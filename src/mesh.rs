use crate::chunk_cache::ChunkCache;
use crate::mesh_send::{
    call_time, BatchSender, Message, ObjectDatagrams, ObjectEncoder, MESSAGES_PER_CALL,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
#[cfg(test)]
use raptorq_datagram_fec::DatagramFecEncoder;
use raptorq_datagram_fec::{
    source_symbol_count, DatagramFecDecoder, DatagramFecError, DatagramFecHeader, HEADER_LEN,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_64;

const FRAME_MAGIC: &[u8; 8] = b"PLMESH1\0";
const FRAME_VERSION: u8 = 2;
const FRAME_VERSION_LEGACY: u8 = 1;
const FRAME_HELLO: u8 = 1;
const FRAME_CHUNK: u8 = 2;
const FRAME_REPLICA_REQUEST: u8 = 3;
const FRAME_INITIALIZATION: u8 = 4;
const FRAME_SUBSCRIBE: u8 = 5;
const FRAME_UNSUBSCRIBE: u8 = 6;
const FRAME_LINK_REPORT: u8 = 7;
/// Packet loss on the path to a peer below which the configured repair is
/// enough: 0.05%.
const LINK_LOSS_FLOOR_PPM: u32 = 500;
/// A loss report is used for this long after it arrived.
const LINK_LOSS_TTL: Duration = Duration::from_secs(5);
/// A receiver measures the loss from each peer over windows of this length.
const LINK_LOSS_WINDOW: Duration = Duration::from_secs(1);
/// Chance that an object needs a repair request, which the repair symbols
/// of a lossy link are chosen for.
const LINK_LOSS_OBJECT_FAILURE: f64 = 0.001;
/// Payload of one IP fragment on an Ethernet link.
const FRAGMENT_PAYLOAD: usize = 1_480;
const DEFAULT_SUBSCRIPTION_LEASE: Duration = Duration::from_secs(5);
const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_secs(2);
/// Longest burst that the warm-up pacer may send in one sync tick.
const BACKFILL_BURST: Duration = Duration::from_millis(100);
/// Source symbols that a large object is divided into when the symbol size
/// may grow with the object.
const ADAPTIVE_SOURCE_SYMBOLS: usize = 64;
/// Source symbols of a large object on a path with loss. Twice as many, so
/// that a symbol is half as many fragments and half as likely to be lost.
const LOSSY_SOURCE_SYMBOLS: usize = 128;
/// Encoded objects that a sender keeps, so that the children of a node get
/// one encoding of a part and not one each.
/// Encoded objects kept so that every child of a pass, and a child reached on
/// a later tick, shares one encoding of a part. At 4,000 parts per second a
/// sync interval holds about 80 of them per stream.
const ENCODED_OBJECT_CACHE: usize = 1_024;
const ENCODED_OBJECT_CACHE_BYTES: usize = 6 * 1024 * 1024;
const DEFAULT_SYMBOL_SIZE: u16 = 1316;
/// Largest object that fitted symbols put in one symbol of its own length.
/// An edge that cuts a mesh block through to UDP clients (av-mesh
/// `edge_udp`) forwards each mesh symbol as a whole number of client
/// symbols of at most 1200 bytes (av-mesh `client_udp::DEFAULT_SYMBOL_SIZE`)
/// and at least 256: a symbol of 1201 to 1315 bytes often has no such
/// divisor (1201 is prime), and its part would not be cut through. A
/// padded 1316-byte symbol always has one (658).
const FITTED_SYMBOL_MAX: usize = 1_200;
const DEFAULT_REPAIR_SYMBOLS: u32 = 1;
const DEFAULT_REPAIR_RATIO: f32 = 0.03;
const DEFAULT_MAX_REPAIR_SYMBOLS: u32 = 32;
const DEFAULT_ANNOUNCE_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(20);
const REQUEST_BLOCK_ID_BASE: u32 = 1 << 30;
const REPLY_BLOCK_ID_BASE: u32 = (1 << 30) + (1 << 29);
const CHUNK_BLOCK_ID_BASE: u32 = 1 << 31;
/// Each lane of block ids starts at its base plus a per-start offset below
/// this bound.
const BLOCK_ID_START_SPAN: u32 = 1 << 28;
const MAX_GOSSIP_PEERS: usize = 256;
const FEC_OBSERVATION_DATAGRAM_WINDOW: u64 = 8_192;
const FEC_COMPLETED_BLOCK_WINDOW: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMeshRole {
    /// Compatibility mode for existing local mesh fixtures.
    Peer,
    /// A regional cache distributor may serve child replicas.
    Distributor,
    /// A leaf playback cache receives from parents and never forwards cache data.
    Edge,
}

impl Default for CacheMeshRole {
    fn default() -> Self {
        Self::Peer
    }
}

impl CacheMeshRole {
    fn wire_value(self) -> u8 {
        match self {
            Self::Peer => 0,
            Self::Distributor => 1,
            Self::Edge => 2,
        }
    }

    fn from_wire(value: u8) -> Result<Self, MeshError> {
        match value {
            0 => Ok(Self::Peer),
            1 => Ok(Self::Distributor),
            2 => Ok(Self::Edge),
            _ => Err(MeshError::InvalidFrame("unknown cache mesh role")),
        }
    }
}

#[derive(Debug)]
pub enum MeshError {
    Io(std::io::Error),
    Fec(DatagramFecError),
    InvalidFrame(&'static str),
    InvalidUtf8(std::str::Utf8Error),
    Cache(&'static str),
}

impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Fec(err) => write!(f, "fec error: {err}"),
            Self::InvalidFrame(msg) => write!(f, "invalid mesh frame: {msg}"),
            Self::InvalidUtf8(err) => write!(f, "invalid utf-8 in mesh frame: {err}"),
            Self::Cache(err) => write!(f, "cache error: {err}"),
        }
    }
}

impl Error for MeshError {}

impl From<std::io::Error> for MeshError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<DatagramFecError> for MeshError {
    fn from(value: DatagramFecError) -> Self {
        Self::Fec(value)
    }
}

impl From<std::str::Utf8Error> for MeshError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8(value)
    }
}

/// How a node that serves children decides which playlists to push.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicationMode {
    /// Push every playlist to every child. This is the original behavior.
    #[default]
    All,
    /// Push a playlist to a child only while the child holds a subscription
    /// lease for it. A child subscribes when a reader asks for the playlist
    /// and lets the lease lapse when the demand is gone.
    Demand,
}

/// One playlist part that the cache mesh wrote into the local cache.
///
/// The times are wall-clock (`CLOCK_REALTIME`) microseconds since the Unix
/// epoch, so that they can be joined with the times that other nodes record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedSlot {
    pub stream_id: u64,
    pub slot_id: usize,
    pub bytes: usize,
    /// When the first datagram of the part's FEC block arrived.
    pub rx_first_us: u64,
    /// When the FEC block decoded.
    pub decoded_us: u64,
    /// When the part was written into the local cache. Exact-part waiters
    /// have been woken by then.
    pub committed_us: u64,
    /// The parent that sent the part.
    pub parent: SocketAddr,
}

/// Called by the sync task each time it hands the first datagram of a live
/// part to the kernel for a child: `(stream_id, slot_id, child, sent_unix_us)`.
///
/// A live part is one published since the sync task last looked at its
/// stream, or any part for a child that is already following the stream
/// (so a part that a child receives late, because of rotation over
/// `max_children` or a failed send, is still reported). Catch-up sends are not
/// reported: the warm-up window of a new lease, and the parts a new child in
/// full replication is sent from the start of the retained window. Their
/// send times measure how old the window is, not the hop.
///
/// It runs inline on the sync task and must be cheap: record and return.
pub type CacheMeshSendObserver = Arc<dyn Fn(u64, u64, SocketAddr, u64) + Send + Sync>;

/// Told of every datagram that the cache mesh receives, before it is decoded,
/// and of every block once it is decoded. A node uses it to forward the
/// source symbols of a part on as they arrive (cut-through), without waiting
/// for the block.
///
/// Both calls run inline on the receive task and must be cheap: no waiting,
/// no blocking send.
pub trait CacheMeshReceiveObserver: Send + Sync {
    /// A datagram from `peer`, exactly as it arrived.
    fn datagram(&self, peer: SocketAddr, datagram: &[u8]);
    /// The datagram that completed a block from `peer`, and the frame that
    /// the block decoded to.
    fn decoded(&self, peer: SocketAddr, datagram: &[u8], frame: &[u8]);
}

/// Holds the optional receive observer so that the configuration stays
/// `Debug`.
#[derive(Clone, Default)]
struct ReceiveObserverSlot(Option<Arc<dyn CacheMeshReceiveObserver>>);

impl fmt::Debug for ReceiveObserverSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() { "Some(..)" } else { "None" })
    }
}

/// The stream and slot of a chunk frame, from its first bytes: the first
/// source symbol of the frame's block is enough. `None` for any other frame,
/// or when `prefix` is too short.
pub fn chunk_frame_slot(prefix: &[u8]) -> Option<(u64, u64)> {
    let header = FRAME_MAGIC.len() + 2;
    if prefix.len() < header + 2 || &prefix[..FRAME_MAGIC.len()] != FRAME_MAGIC {
        return None;
    }
    let version = prefix[FRAME_MAGIC.len()];
    if version != FRAME_VERSION && version != FRAME_VERSION_LEGACY {
        return None;
    }
    if prefix[FRAME_MAGIC.len() + 1] != FRAME_CHUNK {
        return None;
    }
    let node_len = usize::from(u16::from_be_bytes([prefix[header], prefix[header + 1]]));
    let start = header + 2 + node_len;
    let ids = prefix.get(start..start + 16)?;
    Some((
        u64::from_be_bytes(ids[..8].try_into().ok()?),
        u64::from_be_bytes(ids[8..].try_into().ok()?),
    ))
}

/// The slot payload of a decoded chunk frame: what the cache stores for the
/// slot.
pub fn chunk_frame_payload(frame: &[u8]) -> Option<(u64, u64, Bytes)> {
    match MeshFrame::decode(frame).ok()? {
        MeshFrame::Chunk {
            stream_id,
            slot_id,
            payload,
            ..
        } => Some((stream_id, slot_id, payload)),
        _ => None,
    }
}

/// Holds the optional send observer so that the configuration stays `Debug`.
#[derive(Clone, Default)]
struct SendObserverSlot(Option<CacheMeshSendObserver>);

impl fmt::Debug for SendObserverSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() { "Some(..)" } else { "None" })
    }
}

/// Wall-clock time in microseconds since the Unix epoch.
fn unix_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_micros() as u64)
}

/// One playlist subscription lease that this node serves to a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServedSubscription {
    pub child: SocketAddr,
    pub stream_id: u64,
    /// Parts of the warm-up window that are still to be sent.
    pub backfill_parts_pending: usize,
}

#[derive(Clone, Debug)]
pub struct CacheMeshConfig {
    pub node_id: String,
    pub region: String,
    pub bind_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub announce_interval: Duration,
    pub sync_interval: Duration,
    /// Send a part to the children as soon as the local watermark reaches it,
    /// not at the next `sync_interval` tick. The tick still runs: it picks up
    /// new leases and children and paces warm-up windows. Default true.
    pub sync_on_publish: bool,
    pub repair_symbols: u32,
    pub repair_ratio: f32,
    pub max_repair_symbols: u32,
    pub symbol_size: u16,
    /// Largest symbol for a large object. An object that needs more than 64
    /// symbols of `symbol_size` uses a multiple of it, up to this size, so a
    /// 4K video part is tens of datagrams and a 5 ms audio part still fits
    /// small symbols. Zero, or a value at or below `symbol_size`, keeps one
    /// symbol size for every object.
    pub max_symbol_size: u16,
    pub same_region_only: bool,
    pub role: CacheMeshRole,
    /// Maximum number of parent distributors queried by one cache request.
    pub max_parents: usize,
    /// Maximum number of child edges served by one distributor tick.
    pub max_children: usize,
    /// Whether children receive every playlist or only subscribed playlists.
    pub replication: ReplicationMode,
    /// How long a parent keeps pushing a playlist after the last subscribe.
    pub subscription_lease: Duration,
    /// Rate limit for the warm-up window that a new subscription receives, for
    /// each child. Live parts are never limited. Zero sends the window at once.
    pub backfill_bytes_per_sec: u64,
    /// Requested size of the socket receive and send buffers. The kernel can
    /// clamp it. `None` keeps the system default.
    pub socket_buffer_bytes: Option<usize>,
    /// A parent that has been silent for this long is not chosen for a new
    /// subscription or a replica request.
    pub peer_timeout: Duration,
    /// Receives one event for each new playlist part that the mesh writes.
    pub applied_tx: Option<mpsc::UnboundedSender<AppliedSlot>>,
    /// Fault injection for tests: drop this many received datagrams in each
    /// million, as a lossy private network would. Zero drops none.
    pub receive_loss_ppm: u32,
    /// Offset of the first block id of each lane. `start` sets it.
    block_id_offset: u32,
    /// Told when the first datagram of each part leaves for a child.
    send_observer: SendObserverSlot,
    /// Told of every received datagram and decoded block.
    receive_observer: ReceiveObserverSlot,
    /// Hand the datagrams of a sync pass to the kernel in batches, one
    /// `sendmmsg` call for up to 64 messages to a child (Linux; one call
    /// for each datagram elsewhere). False sends each datagram with its own
    /// awaited call, as before. Default true.
    pub batched_send: bool,
    /// With batched sends, let the kernel cut the equal-sized datagrams of
    /// an object from one buffer (UDP generic segmentation offload), where
    /// the kernel has it. While it is in use, symbols do not grow past
    /// `symbol_size` (`max_symbol_size` is not used), so that no datagram is
    /// fragmented. Default true.
    pub gso: bool,
    /// An object smaller than `symbol_size`, and of at most 1200 bytes (the
    /// symbol of an edge's UDP cut-through), goes in one symbol of its own
    /// length, not padded to `symbol_size`: a 2.5 ms Opus part is one
    /// datagram of about 100 bytes and one repair datagram of the same size,
    /// not two of 1352. Receivers take the symbol size from each block's
    /// header. False pads every object to `symbol_size`. Default true.
    pub fitted_symbols: bool,
}

impl CacheMeshConfig {
    pub fn new(
        node_id: impl Into<String>,
        region: impl Into<String>,
        bind_addr: SocketAddr,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            region: region.into(),
            bind_addr,
            peers: Vec::new(),
            announce_interval: DEFAULT_ANNOUNCE_INTERVAL,
            sync_interval: DEFAULT_SYNC_INTERVAL,
            sync_on_publish: true,
            repair_symbols: DEFAULT_REPAIR_SYMBOLS,
            repair_ratio: DEFAULT_REPAIR_RATIO,
            max_repair_symbols: DEFAULT_MAX_REPAIR_SYMBOLS,
            symbol_size: DEFAULT_SYMBOL_SIZE,
            max_symbol_size: 0,
            same_region_only: false,
            role: CacheMeshRole::Peer,
            max_parents: 2,
            max_children: 8,
            replication: ReplicationMode::All,
            subscription_lease: DEFAULT_SUBSCRIPTION_LEASE,
            backfill_bytes_per_sec: 0,
            socket_buffer_bytes: None,
            peer_timeout: DEFAULT_PEER_TIMEOUT,
            applied_tx: None,
            receive_loss_ppm: 0,
            block_id_offset: 0,
            send_observer: SendObserverSlot(None),
            receive_observer: ReceiveObserverSlot(None),
            batched_send: true,
            gso: true,
            fitted_symbols: true,
        }
    }

    pub fn with_peer(mut self, peer: SocketAddr) -> Self {
        self.peers.push(peer);
        self
    }

    pub fn with_peers(mut self, peers: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.peers.extend(peers);
        self
    }

    pub fn with_same_region_only(mut self, same_region_only: bool) -> Self {
        self.same_region_only = same_region_only;
        self
    }

    pub fn with_role(mut self, role: CacheMeshRole) -> Self {
        self.role = role;
        self
    }

    pub fn with_max_parents(mut self, max_parents: usize) -> Self {
        self.max_parents = max_parents.max(1);
        self
    }

    pub fn with_max_children(mut self, max_children: usize) -> Self {
        self.max_children = max_children.max(1);
        self
    }

    pub fn with_max_symbol_size(mut self, max_symbol_size: u16) -> Self {
        self.max_symbol_size = max_symbol_size;
        self
    }

    pub fn with_replication(mut self, replication: ReplicationMode) -> Self {
        self.replication = replication;
        self
    }

    pub fn with_subscription_lease(mut self, lease: Duration) -> Self {
        self.subscription_lease = lease.max(Duration::from_millis(1));
        self
    }

    pub fn with_backfill_bytes_per_sec(mut self, bytes_per_sec: u64) -> Self {
        self.backfill_bytes_per_sec = bytes_per_sec;
        self
    }

    pub fn with_socket_buffer_bytes(mut self, bytes: usize) -> Self {
        self.socket_buffer_bytes = (bytes > 0).then_some(bytes);
        self
    }

    /// How often this node tells its peers that it is there. A peer that
    /// sends media is heard with every datagram; the hello is what a node
    /// hears from a peer that has nothing to send.
    pub fn with_announce_interval(mut self, interval: Duration) -> Self {
        self.announce_interval = interval.max(Duration::from_millis(10));
        self
    }

    pub fn with_peer_timeout(mut self, timeout: Duration) -> Self {
        self.peer_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    pub fn with_receive_loss_ppm(mut self, receive_loss_ppm: u32) -> Self {
        self.receive_loss_ppm = receive_loss_ppm.min(1_000_000);
        self
    }

    pub fn with_applied_tx(mut self, applied_tx: mpsc::UnboundedSender<AppliedSlot>) -> Self {
        self.applied_tx = Some(applied_tx);
        self
    }

    /// How often the sync task runs without being woken by a publication.
    pub fn with_sync_interval(mut self, interval: Duration) -> Self {
        self.sync_interval = interval.max(Duration::from_millis(1));
        self
    }

    /// Whether a part leaves for the children as soon as the local watermark
    /// reaches it (true, the default) or only at the next sync tick (false).
    pub fn with_sync_on_publish(mut self, sync_on_publish: bool) -> Self {
        self.sync_on_publish = sync_on_publish;
        self
    }

    /// Be told `(stream_id, slot_id, child, sent_unix_us)` each time the sync
    /// task hands the first datagram of a part to the kernel for a child.
    pub fn with_send_observer(mut self, observer: CacheMeshSendObserver) -> Self {
        self.send_observer = SendObserverSlot(Some(observer));
        self
    }

    /// Whether a sync pass hands its datagrams to the kernel in batches
    /// (true, the default) or one awaited call per datagram (false).
    pub fn with_batched_send(mut self, batched_send: bool) -> Self {
        self.batched_send = batched_send;
        self
    }

    /// Whether batched sends use UDP generic segmentation offload where the
    /// kernel has it (true, the default).
    pub fn with_gso(mut self, gso: bool) -> Self {
        self.gso = gso;
        self
    }

    /// Whether an object smaller than `symbol_size` goes in one symbol of its
    /// own length (true, the default) or is padded to `symbol_size` (false).
    pub fn with_fitted_symbols(mut self, fitted_symbols: bool) -> Self {
        self.fitted_symbols = fitted_symbols;
        self
    }

    /// Be told of every received datagram, before it is decoded, and of
    /// every decoded block (see [`CacheMeshReceiveObserver`]).
    pub fn with_receive_observer(mut self, observer: Arc<dyn CacheMeshReceiveObserver>) -> Self {
        self.receive_observer = ReceiveObserverSlot(Some(observer));
        self
    }
}

/// A lease that a child holds on one playlist of this node.
#[derive(Debug, Clone, Copy)]
struct Lease {
    expires: Instant,
    /// Set when the lease is new. The sync task takes it and starts the
    /// warm-up window: the first slot wanted and the most parts to send.
    open: Option<(usize, usize)>,
}

/// Warm-up window of one lease, sent newest part first.
#[derive(Debug, Clone, Copy)]
struct Backfill {
    low: usize,
    /// Next slot to send. The window is complete when it is below `low`.
    next: Option<usize>,
}

impl Backfill {
    fn pending(&self) -> usize {
        self.next
            .map(|next| next.saturating_sub(self.low).saturating_add(1))
            .unwrap_or(0)
    }
}

type LeaseTable = Arc<StdMutex<HashMap<(SocketAddr, u64), Lease>>>;
type ServedTable = Arc<StdMutex<Vec<ServedSubscription>>>;
type PeerSeen = Arc<StdMutex<HashMap<SocketAddr, Instant>>>;
type WantedStreams = Arc<StdMutex<HashSet<u64>>>;
/// Packet loss on the path from this node to a peer, in parts per million,
/// as the peer reported it, and when.
type LinkLossTable = Arc<StdMutex<HashMap<SocketAddr, (u32, Instant)>>>;
/// Packet loss on the path from a peer to this node, as this node measures
/// it.
type MeasuredLossTable = Arc<StdMutex<HashMap<SocketAddr, u32>>>;

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Rank the parents of one playlist for one child with rendezvous hashing.
///
/// Each child spreads its playlists over the distributors of the region, and
/// each distributor serves a share of the children. Adding a distributor moves
/// only the playlists that now rank it first.
fn rank_parents(node_id: &str, stream_id: u64, peers: &mut [SocketAddr]) {
    let score = |peer: &SocketAddr| {
        let mut key = Vec::with_capacity(node_id.len() + 40);
        key.extend_from_slice(node_id.as_bytes());
        key.extend_from_slice(&stream_id.to_be_bytes());
        key.extend_from_slice(peer.to_string().as_bytes());
        xxh3_64(&key)
    };
    peers.sort_unstable_by(|left, right| {
        score(right).cmp(&score(left)).then_with(|| left.cmp(right))
    });
}

/// Offset for the first block id of every lane of one mesh start.
///
/// A receiver remembers the block ids that a peer address completed, so that
/// late repair symbols are not decoded again. A node that restarts keeps its
/// address. If it counted its block ids from the same value again, its peers
/// would discard its first frames, among them its subscriptions, as repeats
/// of blocks that the previous process sent.
fn block_id_start_offset(config: &CacheMeshConfig) -> u32 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64);
    let mut seed = Vec::with_capacity(config.node_id.len() + 12);
    seed.extend_from_slice(&nanos.to_be_bytes());
    seed.extend_from_slice(&std::process::id().to_be_bytes());
    seed.extend_from_slice(config.node_id.as_bytes());
    (xxh3_64(&seed) as u32) % BLOCK_ID_START_SPAN
}

fn bind_socket(config: &CacheMeshConfig) -> Result<UdpSocket, MeshError> {
    let std_socket = std::net::UdpSocket::bind(config.bind_addr)?;
    std_socket.set_nonblocking(true)?;
    if let Some(bytes) = config.socket_buffer_bytes {
        let socket = socket2::SockRef::from(&std_socket);
        if let Err(err) = socket.set_recv_buffer_size(bytes) {
            warn!(bytes, error = %err, "cache mesh receive buffer request failed");
        }
        if let Err(err) = socket.set_send_buffer_size(bytes) {
            warn!(bytes, error = %err, "cache mesh send buffer request failed");
        }
        let receive = socket.recv_buffer_size().unwrap_or(0);
        let send = socket.send_buffer_size().unwrap_or(0);
        if receive < bytes {
            warn!(
                requested = bytes,
                receive,
                "cache mesh receive buffer is smaller than requested; raise net.core.rmem_max"
            );
        }
        info!(
            requested = bytes,
            receive, send, "cache mesh socket buffers"
        );
    }
    Ok(UdpSocket::from_std(std_socket)?)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheMeshFecStats {
    pub tx_objects: u64,
    pub tx_protected_bytes: u64,
    pub tx_source_datagrams: u64,
    pub tx_repair_datagrams: u64,
    pub tx_wire_bytes: u64,
    pub tx_errors: u64,
    pub rx_wire_datagrams: u64,
    pub rx_wire_bytes: u64,
    pub rx_source_datagrams: u64,
    pub rx_repair_datagrams: u64,
    pub rx_decoded_objects: u64,
    pub rx_decoded_bytes: u64,
    pub rx_repaired_objects: u64,
    pub rx_repaired_source_datagrams: u64,
    pub rx_late_source_datagrams: u64,
    pub rx_presumed_lost_source_datagrams: u64,
    pub rx_decode_errors: u64,
    pub rx_expired_objects: u64,
    pub rx_inflight_objects: u64,
}

#[derive(Debug, Default)]
struct CacheMeshFecCounters {
    tx_objects: AtomicU64,
    tx_protected_bytes: AtomicU64,
    tx_source_datagrams: AtomicU64,
    tx_repair_datagrams: AtomicU64,
    tx_wire_bytes: AtomicU64,
    tx_errors: AtomicU64,
    rx_wire_datagrams: AtomicU64,
    rx_wire_bytes: AtomicU64,
    rx_source_datagrams: AtomicU64,
    rx_repair_datagrams: AtomicU64,
    rx_decoded_objects: AtomicU64,
    rx_decoded_bytes: AtomicU64,
    rx_repaired_objects: AtomicU64,
    rx_repaired_source_datagrams: AtomicU64,
    rx_late_source_datagrams: AtomicU64,
    rx_presumed_lost_source_datagrams: AtomicU64,
    rx_decode_errors: AtomicU64,
    rx_expired_objects: AtomicU64,
    rx_inflight_objects: AtomicU64,
}

impl CacheMeshFecCounters {
    fn snapshot(&self) -> CacheMeshFecStats {
        CacheMeshFecStats {
            tx_objects: self.tx_objects.load(Ordering::Relaxed),
            tx_protected_bytes: self.tx_protected_bytes.load(Ordering::Relaxed),
            tx_source_datagrams: self.tx_source_datagrams.load(Ordering::Relaxed),
            tx_repair_datagrams: self.tx_repair_datagrams.load(Ordering::Relaxed),
            tx_wire_bytes: self.tx_wire_bytes.load(Ordering::Relaxed),
            tx_errors: self.tx_errors.load(Ordering::Relaxed),
            rx_wire_datagrams: self.rx_wire_datagrams.load(Ordering::Relaxed),
            rx_wire_bytes: self.rx_wire_bytes.load(Ordering::Relaxed),
            rx_source_datagrams: self.rx_source_datagrams.load(Ordering::Relaxed),
            rx_repair_datagrams: self.rx_repair_datagrams.load(Ordering::Relaxed),
            rx_decoded_objects: self.rx_decoded_objects.load(Ordering::Relaxed),
            rx_decoded_bytes: self.rx_decoded_bytes.load(Ordering::Relaxed),
            rx_repaired_objects: self.rx_repaired_objects.load(Ordering::Relaxed),
            rx_repaired_source_datagrams: self.rx_repaired_source_datagrams.load(Ordering::Relaxed),
            rx_late_source_datagrams: self.rx_late_source_datagrams.load(Ordering::Relaxed),
            rx_presumed_lost_source_datagrams: self
                .rx_presumed_lost_source_datagrams
                .load(Ordering::Relaxed),
            rx_decode_errors: self.rx_decode_errors.load(Ordering::Relaxed),
            rx_expired_objects: self.rx_expired_objects.load(Ordering::Relaxed),
            rx_inflight_objects: self.rx_inflight_objects.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct CacheMesh {
    cache: Arc<ChunkCache>,
    config: CacheMeshConfig,
}

impl CacheMesh {
    pub fn new(cache: Arc<ChunkCache>, config: CacheMeshConfig) -> Self {
        Self { cache, config }
    }

    pub async fn start(mut self) -> Result<CacheMeshHandle, MeshError> {
        self.config.block_id_offset = block_id_start_offset(&self.config);
        let socket = Arc::new(bind_socket(&self.config)?);
        let local_addr = socket.local_addr()?;
        let peers = Arc::new(RwLock::new(
            self.config.peers.iter().copied().collect::<HashSet<_>>(),
        ));
        let peer_regions = Arc::new(RwLock::new(HashMap::new()));
        let peer_roles = Arc::new(RwLock::new(HashMap::new()));
        let remote_slots = Arc::new(RwLock::new(HashSet::new()));
        let fec_counters = Arc::new(CacheMeshFecCounters::default());
        let leases: LeaseTable = Arc::new(StdMutex::new(HashMap::new()));
        let served: ServedTable = Arc::new(StdMutex::new(Vec::new()));
        let peer_seen: PeerSeen = Arc::new(StdMutex::new(HashMap::new()));
        let wanted: WantedStreams = Arc::new(StdMutex::new(HashSet::new()));
        let link_loss: LinkLossTable = Arc::new(StdMutex::new(HashMap::new()));
        let measured_loss: MeasuredLossTable = Arc::new(StdMutex::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        tokio::spawn(receive_task(
            Arc::clone(&socket),
            Arc::clone(&self.cache),
            Arc::clone(&peers),
            Arc::clone(&peer_regions),
            Arc::clone(&peer_roles),
            Arc::clone(&remote_slots),
            Arc::clone(&fec_counters),
            MeshShared {
                leases: Arc::clone(&leases),
                peer_seen: Arc::clone(&peer_seen),
                wanted: Arc::clone(&wanted),
                link_loss: Arc::clone(&link_loss),
                measured_loss: Arc::clone(&measured_loss),
            },
            self.config.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(announce_task(
            Arc::clone(&socket),
            Arc::clone(&peers),
            Arc::clone(&peer_regions),
            Arc::clone(&peer_roles),
            Arc::clone(&fec_counters),
            Arc::clone(&link_loss),
            Arc::clone(&measured_loss),
            self.config.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(sync_task(
            Arc::clone(&socket),
            self.cache,
            Arc::clone(&peers),
            Arc::clone(&peer_regions),
            Arc::clone(&peer_roles),
            Arc::clone(&remote_slots),
            Arc::clone(&fec_counters),
            Arc::clone(&leases),
            Arc::clone(&served),
            Arc::clone(&link_loss),
            self.config.clone(),
            shutdown_rx,
        ));

        info!(
            node_id = self.config.node_id,
            region = self.config.region,
            bind = %local_addr,
            "cache mesh started"
        );

        Ok(CacheMeshHandle {
            shutdown_tx,
            peers,
            peer_regions,
            peer_roles,
            socket,
            node_id: self.config.node_id,
            repair_symbols: self.config.repair_symbols,
            repair_ratio: self.config.repair_ratio,
            max_repair_symbols: self.config.max_repair_symbols,
            symbol_size: self.config.symbol_size,
            fec_counters,
            request_block_id: Arc::new(AtomicU32::new(
                REQUEST_BLOCK_ID_BASE + self.config.block_id_offset,
            )),
            local_addr,
            region: self.config.region,
            same_region_only: self.config.same_region_only,
            role: self.config.role,
            max_parents: self.config.max_parents.max(1),
            replication: self.config.replication,
            subscription_lease: self.config.subscription_lease,
            peer_timeout: self.config.peer_timeout,
            served,
            peer_seen,
            wanted,
            link_loss,
            measured_loss,
            remote_slots,
        })
    }
}

#[derive(Clone)]
struct MeshShared {
    leases: LeaseTable,
    peer_seen: PeerSeen,
    wanted: WantedStreams,
    link_loss: LinkLossTable,
    measured_loss: MeasuredLossTable,
}

pub struct CacheMeshHandle {
    shutdown_tx: watch::Sender<()>,
    peers: Arc<RwLock<HashSet<SocketAddr>>>,
    peer_regions: Arc<RwLock<HashMap<SocketAddr, String>>>,
    peer_roles: Arc<RwLock<HashMap<SocketAddr, CacheMeshRole>>>,
    socket: Arc<UdpSocket>,
    node_id: String,
    repair_symbols: u32,
    repair_ratio: f32,
    max_repair_symbols: u32,
    symbol_size: u16,
    fec_counters: Arc<CacheMeshFecCounters>,
    request_block_id: Arc<AtomicU32>,
    local_addr: SocketAddr,
    region: String,
    same_region_only: bool,
    role: CacheMeshRole,
    max_parents: usize,
    replication: ReplicationMode,
    subscription_lease: Duration,
    peer_timeout: Duration,
    served: ServedTable,
    remote_slots: Arc<RwLock<HashSet<(u64, usize)>>>,
    peer_seen: PeerSeen,
    wanted: WantedStreams,
    link_loss: LinkLossTable,
    measured_loss: MeasuredLossTable,
}

/// Packet loss on the links of this node to one peer, in parts per million.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkLoss {
    pub peer: SocketAddr,
    /// Loss from the peer to this node, as this node measures it.
    pub receive_ppm: Option<u32>,
    /// Loss from this node to the peer, as the peer reports it.
    pub send_ppm: Option<u32>,
}

impl CacheMeshHandle {
    /// Packet loss on the links to the peers that this node exchanges parts
    /// with. A node sends more repair symbols, in datagrams that fit one
    /// packet, to a peer that reports loss.
    pub fn link_loss(&self) -> Vec<LinkLoss> {
        let measured = lock(&self.measured_loss).clone();
        let reported = lock(&self.link_loss).clone();
        let now = Instant::now();
        let mut peers = measured
            .keys()
            .chain(reported.keys())
            .copied()
            .collect::<Vec<_>>();
        peers.sort_unstable();
        peers.dedup();
        peers
            .into_iter()
            .map(|peer| LinkLoss {
                peer,
                receive_ppm: measured.get(&peer).copied(),
                send_ppm: reported
                    .get(&peer)
                    .filter(|(_, at)| now.duration_since(*at) <= LINK_LOSS_TTL)
                    .map(|(ppm, _)| *ppm),
            })
            .collect()
    }

    /// Ask the first `parents` parents of a playlist for `count` parts from
    /// `from_slot`. A node that has a hole asks for the hole and nothing
    /// else, and asks one parent first.
    pub async fn request_parts(
        &self,
        stream_id: u64,
        from_slot: usize,
        count: u32,
        parents: usize,
    ) -> Result<usize, MeshError> {
        let mut peers = self.parents_for(stream_id).await;
        peers.truncate(parents.max(1));
        if peers.is_empty() {
            return Ok(0);
        }
        let frame = MeshFrame::ReplicaRequest {
            node_id: self.node_id.clone(),
            stream_id,
            from_slot: from_slot as u64,
            count,
        }
        .encode()?;
        self.send_control(&peers, &frame).await
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn peers(&self) -> Vec<SocketAddr> {
        self.peers.read().await.iter().copied().collect()
    }

    pub async fn add_peer(&self, peer: SocketAddr) -> bool {
        if peer == self.local_addr {
            return false;
        }
        self.peers.write().await.insert(peer)
    }

    pub fn fec_stats(&self) -> CacheMeshFecStats {
        self.fec_counters.snapshot()
    }

    pub async fn request_replica(
        &self,
        stream_id: u64,
        from_slot: usize,
    ) -> Result<usize, MeshError> {
        let peers = self.parents_for(stream_id).await;
        if peers.is_empty() {
            return Ok(0);
        }
        let frame = MeshFrame::replica_request(self.node_id.clone(), stream_id, from_slot as u64)
            .encode()?;
        self.send_control(&peers, &frame).await
    }

    /// Subscribe to one playlist at its parents, or renew the subscription.
    ///
    /// A parent that runs demand replication pushes the playlist only while
    /// the lease is current, so call this again within the lease. A new lease
    /// first receives a warm-up window of at most `backfill_parts` parts that
    /// ends at the newest part and does not start before `from_slot`.
    /// Returns the parents that were asked.
    pub async fn subscribe(
        &self,
        stream_id: u64,
        from_slot: usize,
        backfill_parts: usize,
    ) -> Result<Vec<SocketAddr>, MeshError> {
        lock(&self.wanted).insert(stream_id);
        let peers = self.parents_for(stream_id).await;
        if peers.is_empty() {
            return Ok(peers);
        }
        let lease_ms = u32::try_from(self.subscription_lease.as_millis()).unwrap_or(u32::MAX);
        // Every parent pushes the live parts. Only the first parent sends the
        // warm-up window, so a new copy is not warmed twice over.
        for (index, peer) in peers.iter().enumerate() {
            let backfill_parts = if index == 0 { backfill_parts } else { 0 };
            let frame = MeshFrame::Subscribe {
                node_id: self.node_id.clone(),
                stream_id,
                from_slot: from_slot as u64,
                backfill_parts: u32::try_from(backfill_parts).unwrap_or(u32::MAX),
                lease_ms,
            }
            .encode()?;
            self.send_control(std::slice::from_ref(peer), &frame)
                .await?;
        }
        Ok(peers)
    }

    /// End the subscription to one playlist. Parents stop pushing it, and
    /// this node no longer accepts its parts.
    pub async fn unsubscribe(&self, stream_id: u64) -> Result<usize, MeshError> {
        if !lock(&self.wanted).remove(&stream_id) {
            return Ok(0);
        }
        // Every allowed parent is told, because the parents of the playlist
        // can have changed since the subscription was made.
        let peers = self.allowed_peers().await;
        if peers.is_empty() {
            return Ok(0);
        }
        let frame = MeshFrame::Unsubscribe {
            node_id: self.node_id.clone(),
            stream_id,
        }
        .encode()?;
        self.send_control(&peers, &frame).await
    }

    /// Playlists that this node currently subscribes to.
    pub fn subscribed_streams(&self) -> Vec<u64> {
        let mut streams = lock(&self.wanted).iter().copied().collect::<Vec<_>>();
        streams.sort_unstable();
        streams
    }

    /// Subscription leases that this node currently serves to children.
    pub fn served_subscriptions(&self) -> Vec<ServedSubscription> {
        lock(&self.served).clone()
    }

    pub fn replication(&self) -> ReplicationMode {
        self.replication
    }

    pub fn subscription_lease(&self) -> Duration {
        self.subscription_lease
    }

    /// How long ago this node last heard from any peer that it may take
    /// playlists from. `None` before the first frame of such a peer.
    ///
    /// An edge that has heard no parent for a few hello intervals has lost
    /// its network, or every parent: it cannot say when the next part will
    /// come, and should send its readers to another cache.
    /// Slots that this node took from a peer and still remembers. It is at
    /// most the slots that its cache retains.
    pub async fn remote_slot_count(&self) -> usize {
        self.remote_slots.read().await.len()
    }

    pub async fn parents_silent_for(&self) -> Option<Duration> {
        let peers = self.allowed_peers().await;
        let seen = lock(&self.peer_seen);
        let now = Instant::now();
        peers
            .iter()
            .filter_map(|peer| seen.get(peer))
            .map(|last| now.duration_since(*last))
            .min()
    }

    /// The parents that this node uses for one playlist: allowed, alive, and
    /// ranked for this node and playlist, at most `max_parents` of them.
    pub async fn parents_for(&self, stream_id: u64) -> Vec<SocketAddr> {
        let mut peers = self.allowed_peers().await;
        let alive = {
            let seen = lock(&self.peer_seen);
            let now = Instant::now();
            peers
                .iter()
                .copied()
                .filter(|peer| {
                    seen.get(peer)
                        .is_some_and(|last| now.duration_since(*last) <= self.peer_timeout)
                })
                .collect::<Vec<_>>()
        };
        // Before the first hello arrives no parent is known to be alive. The
        // configured parents are then asked, so a cold start does not wait.
        if !alive.is_empty() {
            peers = alive;
        }
        rank_parents(&self.node_id, stream_id, &mut peers);
        peers.truncate(self.max_parents.max(1));
        peers
    }

    async fn send_control(&self, peers: &[SocketAddr], frame: &[u8]) -> Result<usize, MeshError> {
        let block_id = self.request_block_id.fetch_add(1, Ordering::Relaxed);
        let mut sender = FecSender::with_policy_and_initial_block_id(
            self.repair_symbols,
            self.repair_ratio,
            self.max_repair_symbols,
            self.symbol_size,
            block_id,
            Arc::clone(&self.fec_counters),
        )
        .with_link_loss(Arc::clone(&self.link_loss));
        let mut sent = 0usize;
        for peer in peers {
            sender.send(&self.socket, *peer, frame).await?;
            sent = sent.saturating_add(1);
        }
        Ok(sent)
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    async fn allowed_peers(&self) -> Vec<SocketAddr> {
        let peers = self.peers.read().await.iter().copied().collect::<Vec<_>>();
        let regions = self.peer_regions.read().await;
        let roles = self.peer_roles.read().await;
        let mut peers = peers
            .into_iter()
            .filter(|peer| {
                if self.same_region_only
                    && regions
                        .get(peer)
                        .is_none_or(|region| region != &self.region)
                {
                    return false;
                }
                if self.role == CacheMeshRole::Edge {
                    return roles
                        .get(peer)
                        .is_none_or(|role| *role != CacheMeshRole::Edge);
                }
                if self.role == CacheMeshRole::Distributor {
                    return roles
                        .get(peer)
                        .is_none_or(|role| *role != CacheMeshRole::Edge);
                }
                true
            })
            .collect::<Vec<_>>();
        peers.sort_unstable();
        peers
    }
}

impl Drop for CacheMeshHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MeshFrame {
    Hello {
        node_id: String,
        region: String,
        role: CacheMeshRole,
        peers: Vec<SocketAddr>,
    },
    Chunk {
        node_id: String,
        stream_id: u64,
        slot_id: u64,
        payload: Bytes,
    },
    Initialization {
        node_id: String,
        stream_id: u64,
        payload: Bytes,
    },
    ReplicaRequest {
        node_id: String,
        stream_id: u64,
        from_slot: u64,
        /// Parts wanted from `from_slot`. Zero is every part up to the
        /// newest one.
        count: u32,
    },
    /// Packet loss that the sender of this frame measures on the path from
    /// the receiver of the frame, in parts per million.
    LinkReport {
        node_id: String,
        loss_ppm: u32,
    },
    Subscribe {
        node_id: String,
        stream_id: u64,
        from_slot: u64,
        backfill_parts: u32,
        lease_ms: u32,
    },
    Unsubscribe {
        node_id: String,
        stream_id: u64,
    },
}

impl MeshFrame {
    #[cfg(test)]
    fn hello_with_peers(
        node_id: impl Into<String>,
        region: impl Into<String>,
        peers: Vec<SocketAddr>,
    ) -> Self {
        Self::Hello {
            node_id: node_id.into(),
            region: region.into(),
            role: CacheMeshRole::Peer,
            peers,
        }
    }

    fn hello_with_role(
        node_id: impl Into<String>,
        region: impl Into<String>,
        role: CacheMeshRole,
        peers: Vec<SocketAddr>,
    ) -> Self {
        Self::Hello {
            node_id: node_id.into(),
            region: region.into(),
            role,
            peers,
        }
    }

    fn chunk(node_id: impl Into<String>, stream_id: u64, slot_id: u64, payload: Bytes) -> Self {
        Self::Chunk {
            node_id: node_id.into(),
            stream_id,
            slot_id,
            payload,
        }
    }

    fn replica_request(node_id: impl Into<String>, stream_id: u64, from_slot: u64) -> Self {
        Self::ReplicaRequest {
            node_id: node_id.into(),
            stream_id,
            from_slot,
            count: 0,
        }
    }

    fn initialization(node_id: impl Into<String>, stream_id: u64, payload: Bytes) -> Self {
        Self::Initialization {
            node_id: node_id.into(),
            stream_id,
            payload,
        }
    }

    fn node_id(&self) -> &str {
        match self {
            Self::Hello { node_id, .. }
            | Self::Chunk { node_id, .. }
            | Self::Initialization { node_id, .. }
            | Self::ReplicaRequest { node_id, .. }
            | Self::LinkReport { node_id, .. }
            | Self::Subscribe { node_id, .. }
            | Self::Unsubscribe { node_id, .. } => node_id,
        }
    }

    fn encode(&self) -> Result<Bytes, MeshError> {
        let node_id = self.node_id().as_bytes();
        if node_id.len() > u16::MAX as usize {
            return Err(MeshError::InvalidFrame("node id too long"));
        }

        let mut out = BytesMut::new();
        out.put_slice(FRAME_MAGIC);
        out.put_u8(FRAME_VERSION);
        match self {
            Self::Hello {
                region,
                role,
                peers,
                ..
            } => {
                let region = region.as_bytes();
                if region.len() > u16::MAX as usize {
                    return Err(MeshError::InvalidFrame("region too long"));
                }
                if peers.len() > MAX_GOSSIP_PEERS {
                    return Err(MeshError::InvalidFrame("too many gossip peers"));
                }
                out.put_u8(FRAME_HELLO);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u16(region.len() as u16);
                out.put_slice(region);
                out.put_u16(peers.len() as u16);
                for peer in peers {
                    let peer = peer.to_string();
                    let peer = peer.as_bytes();
                    if peer.len() > u16::MAX as usize {
                        return Err(MeshError::InvalidFrame("gossip peer addr too long"));
                    }
                    out.put_u16(peer.len() as u16);
                    out.put_slice(peer);
                }
                out.put_u8(role.wire_value());
            }
            Self::Chunk {
                stream_id,
                slot_id,
                payload,
                ..
            } => {
                if payload.len() > u32::MAX as usize {
                    return Err(MeshError::InvalidFrame("payload too large"));
                }
                out.put_u8(FRAME_CHUNK);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u64(*stream_id);
                out.put_u64(*slot_id);
                out.put_u32(payload.len() as u32);
                out.put_slice(payload);
            }
            Self::Initialization {
                stream_id, payload, ..
            } => {
                if payload.len() > u32::MAX as usize {
                    return Err(MeshError::InvalidFrame("initialization payload too large"));
                }
                out.put_u8(FRAME_INITIALIZATION);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u64(*stream_id);
                out.put_u32(payload.len() as u32);
                out.put_slice(payload);
            }
            Self::ReplicaRequest {
                stream_id,
                from_slot,
                count,
                ..
            } => {
                out.put_u8(FRAME_REPLICA_REQUEST);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u64(*stream_id);
                out.put_u64(*from_slot);
                // A request without a count is the frame of an older node,
                // which reads exactly 16 bytes here.
                if *count > 0 {
                    out.put_u32(*count);
                }
            }
            Self::LinkReport { loss_ppm, .. } => {
                out.put_u8(FRAME_LINK_REPORT);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u32(*loss_ppm);
            }
            Self::Subscribe {
                stream_id,
                from_slot,
                backfill_parts,
                lease_ms,
                ..
            } => {
                out.put_u8(FRAME_SUBSCRIBE);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u64(*stream_id);
                out.put_u64(*from_slot);
                out.put_u32(*backfill_parts);
                out.put_u32(*lease_ms);
            }
            Self::Unsubscribe { stream_id, .. } => {
                out.put_u8(FRAME_UNSUBSCRIBE);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u64(*stream_id);
            }
        }
        Ok(out.freeze())
    }

    fn decode(bytes: &[u8]) -> Result<Self, MeshError> {
        if bytes.len() < FRAME_MAGIC.len() + 4 {
            return Err(MeshError::InvalidFrame("frame too short"));
        }
        let mut buf = bytes;
        if &buf[..FRAME_MAGIC.len()] != FRAME_MAGIC {
            return Err(MeshError::InvalidFrame("bad magic"));
        }
        buf.advance(FRAME_MAGIC.len());
        let version = buf.get_u8();
        if version != FRAME_VERSION && version != FRAME_VERSION_LEGACY {
            return Err(MeshError::InvalidFrame("unsupported version"));
        }
        let kind = buf.get_u8();
        let node_id = read_string(&mut buf)?;
        match kind {
            FRAME_HELLO => {
                let region = read_string(&mut buf)?;
                let mut peers = Vec::new();
                if buf.has_remaining() {
                    if buf.remaining() < 2 {
                        return Err(MeshError::InvalidFrame("truncated hello peer count"));
                    }
                    let peer_count = buf.get_u16() as usize;
                    if peer_count > MAX_GOSSIP_PEERS {
                        return Err(MeshError::InvalidFrame("too many gossip peers"));
                    }
                    peers.reserve(peer_count);
                    for _ in 0..peer_count {
                        let peer = read_string(&mut buf)?
                            .parse::<SocketAddr>()
                            .map_err(|_| MeshError::InvalidFrame("invalid gossip peer addr"))?;
                        peers.push(peer);
                    }
                    let role = if version == FRAME_VERSION && buf.has_remaining() {
                        CacheMeshRole::from_wire(buf.get_u8())?
                    } else {
                        CacheMeshRole::Peer
                    };
                    if buf.has_remaining() {
                        return Err(MeshError::InvalidFrame("trailing hello bytes"));
                    }
                    return Ok(Self::Hello {
                        node_id,
                        region,
                        role,
                        peers,
                    });
                }
                Ok(Self::Hello {
                    node_id,
                    region,
                    role: CacheMeshRole::Peer,
                    peers,
                })
            }
            FRAME_CHUNK => {
                if buf.remaining() < 20 {
                    return Err(MeshError::InvalidFrame("chunk header too short"));
                }
                let stream_id = buf.get_u64();
                let slot_id = buf.get_u64();
                let payload_len = buf.get_u32() as usize;
                if buf.remaining() != payload_len {
                    return Err(MeshError::InvalidFrame("chunk payload length mismatch"));
                }
                let payload = Bytes::copy_from_slice(&buf[..payload_len]);
                Ok(Self::Chunk {
                    node_id,
                    stream_id,
                    slot_id,
                    payload,
                })
            }
            FRAME_REPLICA_REQUEST => {
                if buf.remaining() != 16 && buf.remaining() != 20 {
                    return Err(MeshError::InvalidFrame("replica request header mismatch"));
                }
                let stream_id = buf.get_u64();
                let from_slot = buf.get_u64();
                let count = if buf.remaining() == 4 {
                    buf.get_u32()
                } else {
                    0
                };
                Ok(Self::ReplicaRequest {
                    node_id,
                    stream_id,
                    from_slot,
                    count,
                })
            }
            FRAME_LINK_REPORT => {
                if buf.remaining() != 4 {
                    return Err(MeshError::InvalidFrame("link report header mismatch"));
                }
                Ok(Self::LinkReport {
                    node_id,
                    loss_ppm: buf.get_u32(),
                })
            }
            FRAME_INITIALIZATION => {
                if buf.remaining() < 12 {
                    return Err(MeshError::InvalidFrame("initialization header too short"));
                }
                let stream_id = buf.get_u64();
                let payload_len = buf.get_u32() as usize;
                if buf.remaining() != payload_len {
                    return Err(MeshError::InvalidFrame(
                        "initialization payload length mismatch",
                    ));
                }
                let payload = Bytes::copy_from_slice(&buf[..payload_len]);
                Ok(Self::Initialization {
                    node_id,
                    stream_id,
                    payload,
                })
            }
            FRAME_SUBSCRIBE => {
                if buf.remaining() != 24 {
                    return Err(MeshError::InvalidFrame("subscribe header mismatch"));
                }
                let stream_id = buf.get_u64();
                let from_slot = buf.get_u64();
                let backfill_parts = buf.get_u32();
                let lease_ms = buf.get_u32();
                Ok(Self::Subscribe {
                    node_id,
                    stream_id,
                    from_slot,
                    backfill_parts,
                    lease_ms,
                })
            }
            FRAME_UNSUBSCRIBE => {
                if buf.remaining() != 8 {
                    return Err(MeshError::InvalidFrame("unsubscribe header mismatch"));
                }
                let stream_id = buf.get_u64();
                Ok(Self::Unsubscribe { node_id, stream_id })
            }
            _ => Err(MeshError::InvalidFrame("unknown frame kind")),
        }
    }
}

fn read_string(buf: &mut &[u8]) -> Result<String, MeshError> {
    if buf.remaining() < 2 {
        return Err(MeshError::InvalidFrame("missing string length"));
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(MeshError::InvalidFrame("truncated string"));
    }
    let value = std::str::from_utf8(&buf[..len])?.to_string();
    buf.advance(len);
    Ok(value)
}

struct FecSender {
    encoder: ObjectEncoder,
    min_repair_symbols: u32,
    repair_ratio: f32,
    max_repair_symbols: u32,
    symbol_size: u16,
    max_symbol_size: u16,
    /// An object smaller than a symbol is one symbol of its own length.
    fitted_symbols: bool,
    stats: Arc<CacheMeshFecCounters>,
    /// Loss that the peers report on the paths from this node.
    link_loss: Option<LinkLossTable>,
    /// The newest encoded objects, by object key and symbol size, and the
    /// order in which they were encoded.
    encoded: HashMap<EncodedKey, EncodedObject>,
    encoded_order: VecDeque<(EncodedKey, u64)>,
    encoded_bytes: usize,
    encoded_serial: u64,
}

/// An object (stream and slot) in one symbol size, and the hash of its
/// payload. A restarted publication numbers its slots from the start again,
/// so a slot id alone could name the old payload.
type EncodedKey = (u64, u64, u16, u64);

/// One object as datagrams: its source symbols, then its repair symbols.
struct EncodedObject {
    packets: ObjectDatagrams,
    /// Which encoding of the key this is, in `encoded_order`.
    serial: u64,
    /// Children that have been sent this encoding. A receiver ignores a
    /// block that it has decoded, so a part that is sent to a child again
    /// needs a new encoding.
    sent_to: Vec<SocketAddr>,
}

/// Fragments that a datagram of `bytes` crosses an Ethernet link in.
fn datagram_fragments(bytes: usize) -> usize {
    (bytes + 8).div_ceil(FRAGMENT_PAYLOAD).max(1)
}

/// Fewest repair symbols with which an object of `source_symbols` survives a
/// link that loses each symbol with probability `symbol_loss`, except once
/// in `1 / failure` objects.
fn repair_symbols_for_loss(source_symbols: u32, symbol_loss: f64, failure: f64, limit: u32) -> u32 {
    let loss = symbol_loss.clamp(0.0, 0.9);
    if loss <= 0.0 {
        return 0;
    }
    let ratio = loss / (1.0 - loss);
    for repair in 0..=limit {
        // The object is lost when more than `repair` of its symbols are.
        let symbols = f64::from(source_symbols + repair);
        let mut term = (1.0 - loss).powf(symbols);
        let mut survives = term;
        for lost in 0..repair {
            term *= (symbols - f64::from(lost)) / f64::from(lost + 1) * ratio;
            survives += term;
        }
        if 1.0 - survives <= failure {
            return repair;
        }
    }
    limit
}

impl FecSender {
    #[cfg(test)]
    fn new(repair_symbols: u32, symbol_size: u16) -> Self {
        Self::with_policy_and_initial_block_id(
            repair_symbols,
            0.0,
            repair_symbols,
            symbol_size,
            0,
            Arc::new(CacheMeshFecCounters::default()),
        )
    }

    fn from_config(config: &CacheMeshConfig, stats: Arc<CacheMeshFecCounters>) -> Self {
        Self::with_policy_and_initial_block_id(
            config.repair_symbols,
            config.repair_ratio,
            config.max_repair_symbols,
            config.symbol_size,
            0,
            stats,
        )
        .with_max_symbol_size(config.max_symbol_size)
    }

    fn with_max_symbol_size(mut self, max_symbol_size: u16) -> Self {
        self.max_symbol_size = max_symbol_size;
        self
    }

    fn with_fitted_symbols(mut self, fitted_symbols: bool) -> Self {
        self.fitted_symbols = fitted_symbols;
        self
    }

    /// Symbol size for one object: the configured size, or a multiple of it
    /// that keeps a large object within about 64 source symbols.
    fn symbol_size_for(&self, frame_len: usize) -> u16 {
        self.symbol_size_within(frame_len, ADAPTIVE_SOURCE_SYMBOLS)
    }

    /// Symbol size that keeps an object within about `source_symbols`.
    fn symbol_size_within(&self, frame_len: usize, source_symbols: usize) -> u16 {
        let base = usize::from(self.symbol_size.max(1));
        if self.fitted_symbols && frame_len < base && frame_len <= FITTED_SYMBOL_MAX {
            return frame_len.max(1) as u16;
        }
        let limit = usize::from(self.max_symbol_size);
        if limit <= base || frame_len <= base * source_symbols {
            return self.symbol_size;
        }
        let multiple = frame_len
            .div_ceil(source_symbols)
            .div_ceil(base)
            .min(limit / base)
            .max(1);
        u16::try_from(base * multiple).unwrap_or(self.symbol_size)
    }

    fn from_config_with_initial_block_id(
        config: &CacheMeshConfig,
        initial_block_id: u32,
        stats: Arc<CacheMeshFecCounters>,
    ) -> Self {
        Self::with_policy_and_initial_block_id(
            config.repair_symbols,
            config.repair_ratio,
            config.max_repair_symbols,
            config.symbol_size,
            initial_block_id,
            stats,
        )
        .with_max_symbol_size(config.max_symbol_size)
        .with_fitted_symbols(config.fitted_symbols)
    }

    fn with_policy_and_initial_block_id(
        min_repair_symbols: u32,
        repair_ratio: f32,
        max_repair_symbols: u32,
        symbol_size: u16,
        initial_block_id: u32,
        stats: Arc<CacheMeshFecCounters>,
    ) -> Self {
        let max_repair_symbols = max_repair_symbols.max(min_repair_symbols);
        let encoder = ObjectEncoder::new(initial_block_id);
        Self {
            encoder,
            min_repair_symbols,
            repair_ratio: repair_ratio.max(0.0),
            max_repair_symbols,
            symbol_size: symbol_size.max(1),
            max_symbol_size: 0,
            fitted_symbols: false,
            stats,
            link_loss: None,
            encoded: HashMap::new(),
            encoded_order: VecDeque::new(),
            encoded_bytes: 0,
            encoded_serial: 0,
        }
    }

    fn with_link_loss(mut self, link_loss: LinkLossTable) -> Self {
        self.link_loss = Some(link_loss);
        self
    }

    /// Packet loss that `peer` reports on the path from this node, in parts
    /// per million. Zero when it has reported none lately.
    fn loss_ppm_to(&self, peer: SocketAddr) -> u32 {
        let Some(table) = &self.link_loss else {
            return 0;
        };
        lock(table)
            .get(&peer)
            .filter(|(_, at)| at.elapsed() <= LINK_LOSS_TTL)
            .map_or(0, |(ppm, _)| *ppm)
    }

    /// Symbol size and repair symbols for one object on a path with the
    /// given packet loss.
    ///
    /// A large object goes in large symbols: fewer datagrams cost a small
    /// node less CPU, and the network fragments each of them. One lost
    /// fragment loses the whole symbol, so the loss of a symbol is the loss
    /// of a packet times its fragments: at 0.5% packet loss a symbol of
    /// eleven fragments is lost 5% of the time, and an object of 25 such
    /// symbols with one repair symbol fails four times in ten.
    ///
    /// A path with loss therefore gets symbols of half the size, and the
    /// repair symbols that keep an object from failing more than once in a
    /// thousand at the loss that its far end measures. The repair is at most
    /// half of the object: beyond that the path is asked to repeat a part.
    /// Symbols that fit one packet would need the least repair, and a small
    /// node cannot send or receive that many datagrams for 4K video.
    fn plan(&self, frame_len: usize, loss_ppm: u32) -> (u16, u32) {
        if loss_ppm < LINK_LOSS_FLOOR_PPM {
            return (
                self.symbol_size_for(frame_len),
                self.repair_symbols_for(frame_len),
            );
        }
        let symbol_size = self.symbol_size_within(frame_len, LOSSY_SOURCE_SYMBOLS);
        let source_symbols = u32::from(source_symbol_count(frame_len, symbol_size));
        // The measurement is a second old and loss comes in bursts: provide
        // for one and a half times the reported loss.
        let packet_loss = (f64::from(loss_ppm) * 1.5 / 1_000_000.0).min(0.5);
        let fragments = datagram_fragments(usize::from(symbol_size) + HEADER_LEN + 4) as f64;
        let symbol_loss = 1.0 - (1.0 - packet_loss).powf(fragments);
        let limit = self
            .max_repair_symbols
            .max(source_symbols.div_ceil(2))
            .min(255);
        let repair =
            repair_symbols_for_loss(source_symbols, symbol_loss, LINK_LOSS_OBJECT_FAILURE, limit);
        let base = (source_symbols as f32 * self.repair_ratio).ceil() as u32;
        (
            symbol_size,
            repair.max(base).max(self.min_repair_symbols).min(limit),
        )
    }

    /// The worst loss that a peer reports at the moment.
    fn worst_loss_ppm(&self) -> u32 {
        let Some(table) = &self.link_loss else {
            return 0;
        };
        lock(table)
            .values()
            .filter(|(_, at)| at.elapsed() <= LINK_LOSS_TTL)
            .map(|(ppm, _)| *ppm)
            .max()
            .unwrap_or(0)
    }

    /// The datagrams of one stored part for a child, and how many of them the
    /// child is sent. The part is encoded once for all the children that
    /// take the same symbol size, with the repair symbols of the child with
    /// the most loss, and each child receives as many repair symbols as its
    /// own path needs. The frame is the concatenation of `frame`; `content`
    /// is the hash of the payload (the cache's slot hash), so that an
    /// encoding is reused only for the same bytes.
    fn prepare_object(
        &mut self,
        peer: SocketAddr,
        key: (u64, u64),
        content: u64,
        frame: &[&[u8]],
    ) -> Result<(ObjectDatagrams, usize), MeshError> {
        let frame_len = frame.iter().map(|piece| piece.len()).sum::<usize>();
        let (symbol_size, repair_symbols) = self.plan(frame_len, self.loss_ppm_to(peer));
        let encoded_key = (key.0, key.1, symbol_size, content);
        let reusable = self.encoded.get(&encoded_key).is_some_and(|object| {
            object.packets.count() >= object.packets.source_symbols + repair_symbols as usize
                && !object.sent_to.contains(&peer)
        });
        if !reusable {
            let (_, worst) = self.plan(frame_len, self.worst_loss_ppm());
            let repair = repair_symbols.max(worst);
            let packets = match self.encoder.encode(frame, symbol_size, repair) {
                Ok(packets) => packets,
                Err(err) => {
                    self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(err.into());
                }
            };
            self.stats.tx_objects.fetch_add(1, Ordering::Relaxed);
            self.stats
                .tx_protected_bytes
                .fetch_add(frame_len as u64, Ordering::Relaxed);
            self.encoded_serial += 1;
            let serial = self.encoded_serial;
            self.encoded_bytes += packets.wire.len();
            // An older encoding of the same part with less repair is of no
            // more use.
            if let Some(old) = self.encoded.insert(
                encoded_key,
                EncodedObject {
                    packets,
                    serial,
                    sent_to: Vec::new(),
                },
            ) {
                self.encoded_bytes -= old.packets.wire.len();
            }
            self.encoded_order.push_back((encoded_key, serial));
            while self.encoded.len() > ENCODED_OBJECT_CACHE
                || (self.encoded_bytes > ENCODED_OBJECT_CACHE_BYTES && self.encoded.len() > 1)
                || self.encoded_order.len() > 2 * ENCODED_OBJECT_CACHE
            {
                let Some((old_key, old_serial)) = self.encoded_order.pop_front() else {
                    break;
                };
                if self
                    .encoded
                    .get(&old_key)
                    .is_some_and(|object| object.serial == old_serial)
                {
                    if let Some(old) = self.encoded.remove(&old_key) {
                        self.encoded_bytes -= old.packets.wire.len();
                    }
                }
            }
        }
        let Some(object) = self.encoded.get_mut(&encoded_key) else {
            return Err(MeshError::InvalidFrame("encoded object evicted"));
        };
        object.sent_to.push(peer);
        let count =
            (object.packets.source_symbols + repair_symbols as usize).min(object.packets.count());
        Ok((object.packets.clone(), count))
    }

    /// Count `count` datagrams of `object` as sent.
    fn count_sent(&self, object: &ObjectDatagrams, count: usize) {
        let source = count.min(object.source_symbols) as u64;
        self.stats
            .tx_source_datagrams
            .fetch_add(source, Ordering::Relaxed);
        self.stats
            .tx_repair_datagrams
            .fetch_add(count as u64 - source, Ordering::Relaxed);
        self.stats
            .tx_wire_bytes
            .fetch_add((count * object.datagram_len) as u64, Ordering::Relaxed);
    }

    /// Send one stored part to a child, a datagram at a time (see
    /// [`Self::prepare_object`]).
    ///
    /// Returns when the first datagram was handed to the kernel, in wall-clock
    /// microseconds since the Unix epoch.
    async fn send_object(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        key: (u64, u64),
        content: u64,
        frame: &[u8],
    ) -> Result<u64, MeshError> {
        let (object, count) = self.prepare_object(peer, key, content, &[frame])?;
        let mut first_sent_us = 0;
        for position in 0..count {
            // Read the clock as the first datagram is handed to the kernel,
            // so that the time cannot be later than its arrival at the child.
            if position == 0 {
                first_sent_us = unix_micros();
            }
            if let Err(err) = socket.send_to(&object.datagram(position), peer).await {
                self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
                self.count_sent(&object, position);
                return Err(err.into());
            }
        }
        self.count_sent(&object, count);
        Ok(first_sent_us)
    }

    fn repair_symbols_for(&self, frame_len: usize) -> u32 {
        let source_symbols = source_symbol_count(frame_len, self.symbol_size_for(frame_len));
        let proportional = (f32::from(source_symbols) * self.repair_ratio).ceil() as u32;
        proportional
            .max(self.min_repair_symbols)
            .min(self.max_repair_symbols)
    }

    async fn send(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        frame: &[u8],
    ) -> Result<(), MeshError> {
        let (symbol_size, repair_symbols) = self.plan(frame.len(), self.loss_ppm_to(peer));
        let object = match self.encoder.encode(&[frame], symbol_size, repair_symbols) {
            Ok(object) => object,
            Err(err) => {
                self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
                return Err(err.into());
            }
        };
        self.stats.tx_objects.fetch_add(1, Ordering::Relaxed);
        self.stats
            .tx_protected_bytes
            .fetch_add(frame.len() as u64, Ordering::Relaxed);

        for index in 0..object.count() {
            if let Err(err) = socket.send_to(&object.datagram(index), peer).await {
                self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
                self.count_sent(&object, index);
                return Err(err.into());
            }
        }
        self.count_sent(&object, object.count());

        Ok(())
    }
}

#[derive(Debug)]
struct FecBlockObservation {
    source_symbols: u16,
    received_source_symbols: HashSet<u32>,
    last_seen_datagram: u64,
    /// When the first datagram of the block arrived, in wall-clock
    /// microseconds since the Unix epoch.
    first_seen_us: u64,
    /// Packets that one symbol of the block crosses the link in.
    fragments: u32,
}

/// Loss on the path from one peer, measured over a short window.
#[derive(Debug)]
struct LossWindow {
    started: Instant,
    /// Packets that the source symbols of the finished blocks came in.
    expected_packets: u64,
    /// Source symbols of those blocks that did not arrive.
    lost_symbols: u64,
    /// The published estimate, in parts per million.
    ppm: u32,
}

#[derive(Debug)]
struct FecRecoveryObservation {
    missing_source_symbols: HashSet<u32>,
    completed_at_datagram: u64,
    /// When the block decoded, and what its loss is counted against once
    /// its late source symbols have had time to arrive.
    completed_at: Instant,
    source_symbols: u32,
    fragments: u32,
}

/// Time that the source symbols of a decoded block have to arrive before
/// those that are still missing count as lost. A path that reorders packets
/// delivers repair symbols before source symbols. Counted at once, each such
/// symbol was a loss: on a link with 8 ms of jitter and no loss at all, the
/// receivers reported so much loss that the senders sent 3.6 repair symbols
/// for each source symbol.
const LATE_SOURCE_GRACE: Duration = Duration::from_millis(250);

/// A frame that a FEC block decoded to.
struct DecodedBlock {
    frame: Bytes,
    /// When the first datagram of the block arrived, in wall-clock
    /// microseconds since the Unix epoch.
    first_seen_us: u64,
}

struct FecReceiver {
    decoders: HashMap<SocketAddr, DatagramFecDecoder>,
    observations: HashMap<(SocketAddr, u32), FecBlockObservation>,
    completed: HashSet<(SocketAddr, u32)>,
    completed_order: VecDeque<(SocketAddr, u32)>,
    recoveries: HashMap<(SocketAddr, u32), FecRecoveryObservation>,
    observed_datagrams: u64,
    stats: Arc<CacheMeshFecCounters>,
    loss: HashMap<SocketAddr, LossWindow>,
    /// Where the loss from each peer is published for the task that reports
    /// it to the peer.
    measured_loss: Option<MeasuredLossTable>,
}

impl FecReceiver {
    fn new(stats: Arc<CacheMeshFecCounters>) -> Self {
        Self {
            decoders: HashMap::new(),
            observations: HashMap::new(),
            completed: HashSet::new(),
            completed_order: VecDeque::new(),
            recoveries: HashMap::new(),
            observed_datagrams: 0,
            stats,
            loss: HashMap::new(),
            measured_loss: None,
        }
    }

    fn with_measured_loss(mut self, measured_loss: MeasuredLossTable) -> Self {
        self.measured_loss = Some(measured_loss);
        self
    }

    /// Count a finished block of a peer: the packets that its source symbols
    /// came in, and the source symbols that did not arrive. One lost packet
    /// loses one symbol, so lost symbols over packets is the packet loss.
    fn record_block(&mut self, peer: SocketAddr, source_symbols: u32, fragments: u32, lost: u32) {
        let now = Instant::now();
        let window = self.loss.entry(peer).or_insert(LossWindow {
            started: now,
            expected_packets: 0,
            lost_symbols: 0,
            ppm: 0,
        });
        window.expected_packets += u64::from(source_symbols) * u64::from(fragments.max(1));
        window.lost_symbols += u64::from(lost);
        if now.duration_since(window.started) < LINK_LOSS_WINDOW || window.expected_packets < 64 {
            return;
        }
        let measured = (window.lost_symbols.saturating_mul(1_000_000) / window.expected_packets)
            .min(1_000_000) as u32;
        // Follow a rise at once and a fall slowly: the next burst is likely.
        window.ppm = if measured >= window.ppm {
            measured
        } else {
            ((u64::from(window.ppm) * 3 + u64::from(measured)) / 4) as u32
        };
        window.started = now;
        window.expected_packets = 0;
        window.lost_symbols = 0;
        if let Some(table) = &self.measured_loss {
            lock(table).insert(peer, window.ppm);
        }
    }

    #[cfg(test)]
    fn push(&mut self, peer: SocketAddr, bytes: &[u8]) -> Result<Option<Bytes>, DatagramFecError> {
        Ok(self
            .push_at(peer, bytes, unix_micros())?
            .map(|decoded| decoded.frame))
    }

    /// Take one datagram that arrived at `received_us` (wall-clock
    /// microseconds). Returns the frame of a block that it completes, with
    /// the arrival time of the block's first datagram.
    fn push_at(
        &mut self,
        peer: SocketAddr,
        bytes: &[u8],
        received_us: u64,
    ) -> Result<Option<DecodedBlock>, DatagramFecError> {
        self.observed_datagrams = self.observed_datagrams.saturating_add(1);
        self.stats.rx_wire_datagrams.fetch_add(1, Ordering::Relaxed);
        self.stats
            .rx_wire_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);

        let header = match DatagramFecHeader::decode(bytes) {
            Ok(header) => header,
            Err(err) => {
                self.record_decode_error();
                return Err(err);
            }
        };
        if bytes.len() < HEADER_LEN + 4 {
            let err = DatagramFecError::PacketTooShort {
                actual: bytes.len(),
            };
            self.record_decode_error();
            return Err(err);
        }
        let encoding_symbol_id = u32::from_be_bytes([
            0,
            bytes[HEADER_LEN + 1],
            bytes[HEADER_LEN + 2],
            bytes[HEADER_LEN + 3],
        ]);
        let is_source = encoding_symbol_id < u32::from(header.source_symbols);
        if is_source {
            self.stats
                .rx_source_datagrams
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .rx_repair_datagrams
                .fetch_add(1, Ordering::Relaxed);
        }

        let key = (peer, header.block_id);
        if self.completed.contains(&key) {
            if let Err(err) = header.payload(bytes) {
                self.record_decode_error();
                return Err(err);
            }
            if is_source {
                let mut resolved = false;
                let mut complete = false;
                if let Some(recovery) = self.recoveries.get_mut(&key) {
                    resolved = recovery.missing_source_symbols.remove(&encoding_symbol_id);
                    complete = recovery.missing_source_symbols.is_empty();
                }
                if resolved {
                    self.stats
                        .rx_late_source_datagrams
                        .fetch_add(1, Ordering::Relaxed);
                }
                if complete {
                    // Every source symbol arrived after all: nothing was lost.
                    self.finalize_recovery(key);
                }
            }
            self.prune_observations();
            self.stats
                .rx_inflight_objects
                .store(self.observations.len() as u64, Ordering::Relaxed);
            return Ok(None);
        }

        let decoder = self.decoders.entry(peer).or_default();
        let decoded = match decoder.push_datagram(bytes) {
            Ok(decoded) => decoded,
            Err(err) => {
                self.record_decode_error();
                return Err(err);
            }
        };

        let observation = self
            .observations
            .entry(key)
            .or_insert_with(|| FecBlockObservation {
                source_symbols: header.source_symbols,
                received_source_symbols: HashSet::new(),
                last_seen_datagram: self.observed_datagrams,
                first_seen_us: received_us,
                fragments: datagram_fragments(bytes.len()) as u32,
            });
        observation.last_seen_datagram = self.observed_datagrams;
        if is_source {
            observation
                .received_source_symbols
                .insert(encoding_symbol_id);
        }

        let decoded = decoded.map(Bytes::from);
        let mut first_seen_us = received_us;
        if let Some(frame) = decoded.as_ref() {
            let observation = self.observations.remove(&key);
            let fragments = observation
                .as_ref()
                .map_or(1, |observation| observation.fragments);
            if let Some(observation) = observation.as_ref() {
                first_seen_us = observation.first_seen_us;
            }
            let missing_source_symbols = observation
                .map(|observation| {
                    (0..u32::from(observation.source_symbols))
                        .filter(|encoding_symbol_id| {
                            !observation
                                .received_source_symbols
                                .contains(encoding_symbol_id)
                        })
                        .collect::<HashSet<_>>()
                })
                .unwrap_or_default();
            if missing_source_symbols.is_empty() {
                self.record_block(peer, u32::from(header.source_symbols), fragments, 0);
            }
            self.stats
                .rx_decoded_objects
                .fetch_add(1, Ordering::Relaxed);
            self.stats
                .rx_decoded_bytes
                .fetch_add(frame.len() as u64, Ordering::Relaxed);
            if !missing_source_symbols.is_empty() {
                self.stats
                    .rx_repaired_objects
                    .fetch_add(1, Ordering::Relaxed);
                self.stats
                    .rx_repaired_source_datagrams
                    .fetch_add(missing_source_symbols.len() as u64, Ordering::Relaxed);
                self.recoveries.insert(
                    key,
                    FecRecoveryObservation {
                        missing_source_symbols,
                        completed_at_datagram: self.observed_datagrams,
                        completed_at: Instant::now(),
                        source_symbols: u32::from(header.source_symbols),
                        fragments,
                    },
                );
            }
            self.remember_completed(key);
        }

        self.prune_observations();
        self.stats
            .rx_inflight_objects
            .store(self.observations.len() as u64, Ordering::Relaxed);
        Ok(decoded.map(|frame| DecodedBlock {
            frame,
            first_seen_us,
        }))
    }

    fn record_decode_error(&self) {
        self.stats.rx_decode_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn remember_completed(&mut self, key: (SocketAddr, u32)) {
        if self.completed.insert(key) {
            self.completed_order.push_back(key);
        }
        while self.completed_order.len() > FEC_COMPLETED_BLOCK_WINDOW {
            if let Some(expired) = self.completed_order.pop_front() {
                self.completed.remove(&expired);
                self.finalize_recovery(expired);
            }
        }
    }

    fn finalize_recovery(&mut self, key: (SocketAddr, u32)) {
        let Some(recovery) = self.recoveries.remove(&key) else {
            return;
        };
        self.stats.rx_presumed_lost_source_datagrams.fetch_add(
            recovery.missing_source_symbols.len() as u64,
            Ordering::Relaxed,
        );
        // The loss of the block is what is still missing now.
        self.record_block(
            key.0,
            recovery.source_symbols,
            recovery.fragments,
            recovery.missing_source_symbols.len() as u32,
        );
    }

    fn prune_observations(&mut self) {
        let cutoff = self
            .observed_datagrams
            .saturating_sub(FEC_OBSERVATION_DATAGRAM_WINDOW);
        let before = self.observations.len();
        // A block that never decoded lost more symbols than it had repair
        // for. Count the source symbols that did not arrive.
        let expired = self
            .observations
            .iter()
            .filter(|(_, observation)| observation.last_seen_datagram < cutoff)
            .map(|((peer, _), observation)| {
                let source = u32::from(observation.source_symbols);
                let received = observation.received_source_symbols.len() as u32;
                (
                    *peer,
                    source,
                    observation.fragments,
                    source.saturating_sub(received),
                )
            })
            .collect::<Vec<_>>();
        self.observations
            .retain(|_, observation| observation.last_seen_datagram >= cutoff);
        for (peer, source, fragments, lost) in expired {
            self.record_block(peer, source, fragments, lost);
        }
        self.stats.rx_expired_objects.fetch_add(
            before.saturating_sub(self.observations.len()) as u64,
            Ordering::Relaxed,
        );

        let expired_recoveries = self
            .recoveries
            .iter()
            .filter(|(_, recovery)| {
                recovery.completed_at_datagram < cutoff
                    || recovery.completed_at.elapsed() >= LATE_SOURCE_GRACE
            })
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in expired_recoveries {
            self.finalize_recovery(key);
        }
    }
}

async fn receive_task(
    socket: Arc<UdpSocket>,
    cache: Arc<ChunkCache>,
    peers: Arc<RwLock<HashSet<SocketAddr>>>,
    peer_regions: Arc<RwLock<HashMap<SocketAddr, String>>>,
    peer_roles: Arc<RwLock<HashMap<SocketAddr, CacheMeshRole>>>,
    remote_slots: Arc<RwLock<HashSet<(u64, usize)>>>,
    stats: Arc<CacheMeshFecCounters>,
    shared: MeshShared,
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut receiver =
        FecReceiver::new(Arc::clone(&stats)).with_measured_loss(Arc::clone(&shared.measured_loss));
    let mut reply_sender = FecSender::from_config_with_initial_block_id(
        &config,
        REPLY_BLOCK_ID_BASE + config.block_id_offset,
        stats,
    )
    .with_link_loss(Arc::clone(&shared.link_loss));
    let mut buf = vec![0u8; 65_536];
    // xorshift64 state for the test-only loss injection.
    let mut loss_state = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0x9e37_79b9_7f4a_7c15, |elapsed| elapsed.as_nanos() as u64)
        | 1;

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("cache mesh receive task shutting down");
                return;
            }
            result = socket.recv_from(&mut buf) => {
                let (len, peer) = match result {
                    Ok(received) => received,
                    Err(err) => {
                        error!(error = %err, "cache mesh UDP receive failed");
                        continue;
                    }
                };
                let received_us = unix_micros();
                if config.receive_loss_ppm > 0 {
                    loss_state ^= loss_state << 13;
                    loss_state ^= loss_state >> 7;
                    loss_state ^= loss_state << 17;
                    if loss_state % 1_000_000 < u64::from(config.receive_loss_ppm) {
                        continue;
                    }
                }

                if let Some(observer) = &config.receive_observer.0 {
                    observer.datagram(peer, &buf[..len]);
                }
                let DecodedBlock { frame: decoded, first_seen_us } =
                    match receiver.push_at(peer, &buf[..len], received_us) {
                        Ok(Some(decoded)) => {
                            if let Some(observer) = &config.receive_observer.0 {
                                observer.decoded(peer, &buf[..len], &decoded.frame);
                            }
                            decoded
                        }
                        Ok(None) => continue,
                        Err(err) => {
                            warn!(peer = %peer, error = %err, "cache mesh FEC decode failed");
                            continue;
                        }
                    };
                let decoded_us = unix_micros();
                match MeshFrame::decode(&decoded) {
                    Ok(frame) => {
                        if frame.node_id() == config.node_id {
                            continue;
                        }
                        handle_frame(
                            frame,
                            peer,
                            FrameHandler {
                                socket: &socket,
                                cache: &cache,
                                peers: &peers,
                                peer_regions: &peer_regions,
                                peer_roles: &peer_roles,
                                remote_slots: &remote_slots,
                                shared: &shared,
                                config: &config,
                                reply_sender: &mut reply_sender,
                                rx_first_us: first_seen_us,
                                decoded_us,
                            },
                        ).await;
                    }
                    Err(err) => warn!(peer = %peer, error = %err, "cache mesh frame decode failed"),
                }
            }
        }
    }
}

struct FrameHandler<'a> {
    socket: &'a Arc<UdpSocket>,
    cache: &'a Arc<ChunkCache>,
    peers: &'a Arc<RwLock<HashSet<SocketAddr>>>,
    peer_regions: &'a Arc<RwLock<HashMap<SocketAddr, String>>>,
    peer_roles: &'a Arc<RwLock<HashMap<SocketAddr, CacheMeshRole>>>,
    remote_slots: &'a Arc<RwLock<HashSet<(u64, usize)>>>,
    shared: &'a MeshShared,
    config: &'a CacheMeshConfig,
    reply_sender: &'a mut FecSender,
    /// When the first datagram of the frame's FEC block arrived.
    rx_first_us: u64,
    /// When the frame's FEC block decoded.
    decoded_us: u64,
}

impl FrameHandler<'_> {
    /// Whether this node accepts parts of the playlist from a parent. An edge
    /// that replicates on demand holds only the playlists it subscribes to.
    fn accepts_stream(&self, stream_id: u64) -> bool {
        self.config.replication != ReplicationMode::Demand
            || self.config.role != CacheMeshRole::Edge
            || lock(&self.shared.wanted).contains(&stream_id)
    }
}

async fn handle_frame(frame: MeshFrame, peer: SocketAddr, handler: FrameHandler<'_>) {
    lock(&handler.shared.peer_seen).insert(peer, Instant::now());
    match frame {
        MeshFrame::Hello {
            node_id,
            region,
            role,
            peers,
        } => {
            if handler.config.same_region_only && region != handler.config.region {
                handler.peers.write().await.remove(&peer);
                handler.peer_regions.write().await.remove(&peer);
                handler.peer_roles.write().await.remove(&peer);
                debug!(node_id, region, peer = %peer, "ignored cache mesh peer from another region");
                return;
            }
            let mut discovered = 0usize;
            let mut advertised = Vec::new();
            {
                let mut peer_set = handler.peers.write().await;
                if peer_set.insert(peer) {
                    discovered = discovered.saturating_add(1);
                }
                for advertised_peer in peers {
                    if advertised_peer == peer || advertised_peer == handler.config.bind_addr {
                        continue;
                    }
                    if peer_set.insert(advertised_peer) {
                        discovered = discovered.saturating_add(1);
                    }
                    advertised.push(advertised_peer);
                }
            }
            let mut peer_regions = handler.peer_regions.write().await;
            peer_regions.insert(peer, region.clone());
            for advertised_peer in advertised {
                peer_regions.insert(advertised_peer, region.clone());
            }
            drop(peer_regions);
            handler.peer_roles.write().await.insert(peer, role);
            debug!(node_id, region, peer = %peer, discovered, "cache mesh peer discovered");
        }
        MeshFrame::Chunk {
            node_id,
            stream_id,
            slot_id,
            payload,
        } => {
            if !peer_is_allowed(peer, &handler).await || !handler.accepts_stream(stream_id) {
                return;
            }
            let Ok(slot_id) = usize::try_from(slot_id) else {
                warn!(
                    node_id,
                    stream_id, slot_id, "cache mesh slot id does not fit usize"
                );
                return;
            };
            // Each parent sends every part. The second copy of a part is not
            // written again, so it costs no copy and wakes no reader.
            if handler
                .cache
                .get_for_stream_id(stream_id, slot_id)
                .await
                .is_some_and(|(existing, _)| existing == payload)
            {
                return;
            }
            let bytes = payload.len();
            // The set tells the copy loop which slots came from a peer, so
            // that it does not send them back. An edge has no copy loop: it
            // must not keep a set that nothing reads and nothing prunes.
            //
            // The slot is marked before the write, and the set stays locked
            // until the write is done: the write advances the watermark,
            // which wakes the copy loop at once, and the loop must see the
            // slot as remote. Holding the lock also keeps a prune, which
            // drops slots above the watermark, from running in between.
            let mut remote_slots = if handler.config.role != CacheMeshRole::Edge {
                Some(handler.remote_slots.write().await)
            } else {
                None
            };
            let marked = remote_slots
                .as_mut()
                .is_some_and(|remote_slots| remote_slots.insert((stream_id, slot_id)));
            let written = handler
                .cache
                .add_for_stream_id(stream_id, slot_id, payload)
                .await;
            if written.is_err() && marked {
                if let Some(remote_slots) = remote_slots.as_mut() {
                    remote_slots.remove(&(stream_id, slot_id));
                }
            }
            drop(remote_slots);
            if let Err(err) = written {
                warn!(
                    node_id,
                    stream_id,
                    slot_id,
                    error = err,
                    "cache mesh write failed"
                );
                return;
            }
            let committed_us = unix_micros();
            if let Some(applied_tx) = &handler.config.applied_tx {
                let _ = applied_tx.send(AppliedSlot {
                    stream_id,
                    slot_id,
                    bytes,
                    rx_first_us: handler.rx_first_us,
                    decoded_us: handler.decoded_us,
                    committed_us,
                    parent: peer,
                });
            }
            debug!(node_id, stream_id, slot_id, "cache mesh slot applied");
        }
        MeshFrame::Initialization {
            node_id,
            stream_id,
            payload,
        } => {
            if !peer_is_allowed(peer, &handler).await || !handler.accepts_stream(stream_id) {
                return;
            }
            let bytes = payload.len();
            if let Err(err) = handler
                .cache
                .set_stream_initialization(stream_id, payload)
                .await
            {
                warn!(
                    node_id,
                    stream_id,
                    error = err,
                    "cache mesh initialization write failed"
                );
                return;
            }
            debug!(
                node_id,
                stream_id, bytes, "cache mesh stream initialization applied"
            );
        }
        MeshFrame::LinkReport { loss_ppm, .. } => {
            if !peer_is_allowed(peer, &handler).await {
                return;
            }
            lock(&handler.shared.link_loss).insert(peer, (loss_ppm, Instant::now()));
        }
        MeshFrame::ReplicaRequest {
            node_id,
            stream_id,
            from_slot,
            count,
        } => {
            if !peer_is_allowed(peer, &handler).await || handler.config.role == CacheMeshRole::Edge
            {
                return;
            }
            handler.peers.write().await.insert(peer);
            if let Err(err) = serve_replica_request(
                handler.socket,
                handler.cache,
                handler.reply_sender,
                handler.config,
                peer,
                stream_id,
                from_slot,
                count,
            )
            .await
            {
                debug!(
                    requester = node_id,
                    peer = %peer,
                    stream_id,
                    error = %err,
                    "cache mesh replica request could not be served"
                );
            }
        }
        MeshFrame::Subscribe {
            node_id,
            stream_id,
            from_slot,
            backfill_parts,
            lease_ms,
        } => {
            if !peer_is_allowed(peer, &handler).await || handler.config.role == CacheMeshRole::Edge
            {
                return;
            }
            handler.peers.write().await.insert(peer);
            let lease = Duration::from_millis(u64::from(lease_ms).max(1)).min(
                handler
                    .config
                    .subscription_lease
                    .max(Duration::from_secs(1))
                    * 4,
            );
            let now = Instant::now();
            let mut leases = lock(&handler.shared.leases);
            let current = leases
                .get_mut(&(peer, stream_id))
                .filter(|lease| lease.expires > now);
            if let Some(current) = current {
                current.expires = now + lease;
            } else {
                let from_slot = usize::try_from(from_slot).unwrap_or(usize::MAX);
                let backfill_parts = usize::try_from(backfill_parts).unwrap_or(usize::MAX);
                leases.insert(
                    (peer, stream_id),
                    Lease {
                        expires: now + lease,
                        open: Some((from_slot, backfill_parts)),
                    },
                );
                debug!(
                    child = node_id,
                    peer = %peer,
                    stream_id,
                    from_slot,
                    backfill_parts,
                    "cache mesh playlist subscription opened"
                );
            }
        }
        MeshFrame::Unsubscribe { node_id, stream_id } => {
            if lock(&handler.shared.leases)
                .remove(&(peer, stream_id))
                .is_some()
            {
                debug!(
                    child = node_id,
                    peer = %peer,
                    stream_id,
                    "cache mesh playlist subscription closed"
                );
            }
        }
    }
}

async fn peer_is_allowed(peer: SocketAddr, handler: &FrameHandler<'_>) -> bool {
    let region_allowed = handler
        .peer_regions
        .read()
        .await
        .get(&peer)
        .map(|region| !handler.config.same_region_only || region == &handler.config.region)
        .unwrap_or(!handler.config.same_region_only);
    if !region_allowed {
        return false;
    }
    if handler.config.role == CacheMeshRole::Edge {
        return handler
            .peer_roles
            .read()
            .await
            .get(&peer)
            .is_none_or(|role| *role != CacheMeshRole::Edge);
    }
    true
}

async fn serve_replica_request(
    socket: &UdpSocket,
    cache: &ChunkCache,
    sender: &mut FecSender,
    config: &CacheMeshConfig,
    peer: SocketAddr,
    stream_id: u64,
    from_slot: u64,
    count: u32,
) -> Result<usize, MeshError> {
    let Some(stream_idx) = cache.get_stream_idx(stream_id).await else {
        return Ok(0);
    };
    let mut sent = 0usize;
    // A request for named parts fills a hole. The requester has the
    // initialization of the stream already.
    if count > 0 {
        let Some(last) = cache.last(stream_idx) else {
            return Ok(0);
        };
        let first = usize::try_from(from_slot).unwrap_or(usize::MAX);
        if first > last {
            return Ok(0);
        }
        let end = first.saturating_add(count as usize - 1).min(last);
        for slot_id in first.max(cache.retained_start(last))..=end {
            let Some((payload, hash)) = cache.get(stream_idx, slot_id).await else {
                continue;
            };
            if hash == 0 && payload.is_empty() {
                continue;
            }
            let frame =
                MeshFrame::chunk(config.node_id.clone(), stream_id, slot_id as u64, payload)
                    .encode()?;
            sender.send(socket, peer, &frame).await?;
            sent = sent.saturating_add(1);
        }
        debug!(peer = %peer, stream_id, from_slot, count, sent, "cache mesh hole filled");
        return Ok(sent);
    }
    if let Some(initialization) = cache.stream_initialization(stream_id) {
        let frame = MeshFrame::initialization(config.node_id.clone(), stream_id, initialization)
            .encode()?;
        sender.send(socket, peer, &frame).await?;
        sent = sent.saturating_add(1);
    }
    let Some(last) = cache.last(stream_idx) else {
        return Ok(sent);
    };
    let from_slot = usize::try_from(from_slot).unwrap_or(usize::MAX);
    if from_slot > last {
        return Ok(sent);
    }
    let from_slot = from_slot.max(cache.retained_start(last));
    for slot_id in from_slot..=last {
        let Some((payload, hash)) = cache.get(stream_idx, slot_id).await else {
            continue;
        };
        if hash == 0 && payload.is_empty() {
            continue;
        }
        let frame = MeshFrame::chunk(config.node_id.clone(), stream_id, slot_id as u64, payload)
            .encode()?;
        sender.send(socket, peer, &frame).await?;
        sent = sent.saturating_add(1);
    }
    debug!(peer = %peer, stream_id, sent, "cache mesh replica request served");
    Ok(sent)
}

async fn announce_task(
    socket: Arc<UdpSocket>,
    peers: Arc<RwLock<HashSet<SocketAddr>>>,
    peer_regions: Arc<RwLock<HashMap<SocketAddr, String>>>,
    _peer_roles: Arc<RwLock<HashMap<SocketAddr, CacheMeshRole>>>,
    stats: Arc<CacheMeshFecCounters>,
    link_loss: LinkLossTable,
    measured_loss: MeasuredLossTable,
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut sender =
        FecSender::from_config_with_initial_block_id(&config, config.block_id_offset, stats)
            .with_link_loss(link_loss);
    let mut reported: HashMap<SocketAddr, (Instant, u32)> = HashMap::new();
    let mut interval = tokio::time::interval(config.announce_interval);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("cache mesh announce task shutting down");
                return;
            }
            _ = interval.tick() => {
                let current_peers = peers.read().await.iter().copied().collect::<Vec<_>>();
                let peer_regions = peer_regions.read().await;
                let advertised_peers = if config.role == CacheMeshRole::Edge {
                    Vec::new()
                } else {
                    current_peers
                        .iter()
                        .copied()
                        .filter(|peer| {
                            !config.same_region_only
                                || peer_regions
                                    .get(peer)
                                    .is_none_or(|region| region == &config.region)
                        })
                        .take(MAX_GOSSIP_PEERS)
                        .collect::<Vec<_>>()
                };
                drop(peer_regions);
                let frame = match MeshFrame::hello_with_role(
                    config.node_id.clone(),
                    config.region.clone(),
                    config.role,
                    advertised_peers,
                )
                .encode() {
                    Ok(frame) => frame,
                    Err(err) => {
                        error!(error = %err, "cache mesh hello encode failed");
                        continue;
                    }
                };
                for peer in &current_peers {
                    if let Err(err) = sender.send(&socket, *peer, &frame).await {
                        debug!(peer = %peer, error = %err, "cache mesh hello send failed");
                    }
                }
                // Tell each peer what this node loses of what the peer
                // sends, so that the peer protects the path accordingly.
                let measured = lock(&measured_loss).clone();
                for (peer, loss_ppm) in measured {
                    if !current_peers.contains(&peer) {
                        continue;
                    }
                    // A report is sent when the loss changes, and once a
                    // second while there is loss, which keeps it current at
                    // the peer. A clean path costs nothing.
                    let due = match reported.get(&peer) {
                        Some((at, last)) => {
                            *last != loss_ppm
                                || (loss_ppm > 0 && at.elapsed() >= LINK_LOSS_WINDOW)
                        }
                        None => loss_ppm > 0,
                    };
                    if !due {
                        continue;
                    }
                    reported.insert(peer, (Instant::now(), loss_ppm));
                    let report = MeshFrame::LinkReport {
                        node_id: config.node_id.clone(),
                        loss_ppm,
                    };
                    match report.encode() {
                        Ok(report) => {
                            if let Err(err) = sender.send(&socket, peer, &report).await {
                                debug!(peer = %peer, error = %err, "cache mesh link report send failed");
                            }
                        }
                        Err(err) => error!(error = %err, "cache mesh link report encode failed"),
                    }
                }
            }
        }
    }
}

/// The watermark of each stream when the sync task last looked at it.
#[derive(Default)]
struct SeenWatermarks(HashMap<(u64, usize), usize>);

impl SeenWatermarks {
    /// Remember the watermark `last` of a stream and return the one seen
    /// before.
    fn record(&mut self, stream_id: u64, stream_idx: usize, last: Option<usize>) -> Option<usize> {
        match last {
            Some(last) => self.0.insert((stream_id, stream_idx), last),
            None => self.0.remove(&(stream_id, stream_idx)),
        }
    }

    /// Remember the current watermark of every stream.
    fn refresh(&mut self, cache: &ChunkCache, stream_ids: &[(u64, usize)]) {
        for (stream_id, stream_idx) in stream_ids {
            self.record(*stream_id, *stream_idx, cache.last(*stream_idx));
        }
        self.retain_streams(stream_ids);
    }

    /// Forget the streams that the cache no longer holds.
    fn retain_streams(&mut self, stream_ids: &[(u64, usize)]) {
        if self.0.len() > stream_ids.len() {
            let current = stream_ids.iter().copied().collect::<HashSet<_>>();
            self.0.retain(|key, _| current.contains(key));
        }
    }
}

/// Whether two byte strings are equal, without comparing the bytes when they
/// are the same buffer (a stored initialization is cloned, not copied).
fn same_bytes(a: &Bytes, b: &Bytes) -> bool {
    (a.as_ptr() == b.as_ptr() && a.len() == b.len()) || a == b
}

/// How often a node without children forgets the remote slots that its
/// cache no longer retains.
const REMOTE_SLOT_PRUNE_INTERVAL: Duration = Duration::from_millis(250);

/// Forget the slots that came from a peer and that the cache no longer
/// retains: slots behind the window of their stream, and slots of a stream
/// that was released.
async fn forget_unretained_remote_slots(
    cache: &ChunkCache,
    remote_slots: &RwLock<HashSet<(u64, usize)>>,
) {
    // The streams and their watermarks are read under the lock. The receive
    // task marks a slot and writes it under the same lock, so a slot that is
    // being received is never seen above the watermark, or in a stream not
    // yet created, here.
    let mut remote_slots = remote_slots.write().await;
    let stream_ids = cache.stream_ids().await;
    let retained_ranges = stream_ids
        .iter()
        .filter_map(|(stream_id, stream_idx)| {
            cache
                .last(*stream_idx)
                .map(|last| (*stream_id, (cache.retained_start(last), last)))
        })
        .collect::<HashMap<_, _>>();
    remote_slots.retain(|(stream_id, slot_id)| {
        retained_ranges
            .get(stream_id)
            .is_some_and(|(start, last)| slot_id >= start && slot_id <= last)
    });
}

/// A chunk frame without its payload: the payload follows it.
fn chunk_frame_header(
    out: &mut Vec<u8>,
    node_id: &str,
    stream_id: u64,
    slot_id: u64,
    payload_len: usize,
) {
    out.clear();
    out.extend_from_slice(FRAME_MAGIC);
    out.push(FRAME_VERSION);
    out.push(FRAME_CHUNK);
    out.extend_from_slice(&(node_id.len() as u16).to_be_bytes());
    out.extend_from_slice(node_id.as_bytes());
    out.extend_from_slice(&stream_id.to_be_bytes());
    out.extend_from_slice(&slot_id.to_be_bytes());
    out.extend_from_slice(&(payload_len as u32).to_be_bytes());
}

/// A part queued for a child in a batched pass.
struct QueuedPart {
    stream_id: u64,
    stream_idx: usize,
    slot_id: usize,
    /// Its messages are `first_message..end_message` of the child's batch;
    /// none for a part that the child is not sent (it came from a peer).
    first_message: usize,
    end_message: usize,
    /// The send observer is told of it.
    live: bool,
    /// A warm-up part: it moves no cursor.
    warm_up: bool,
    object: Option<(ObjectDatagrams, usize)>,
}

/// What a batched pass sends one child.
#[derive(Default)]
struct ChildBatch {
    messages: Vec<Message>,
    parts: Vec<QueuedPart>,
}

impl ChildBatch {
    fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

async fn sync_task(
    socket: Arc<UdpSocket>,
    cache: Arc<ChunkCache>,
    peers: Arc<RwLock<HashSet<SocketAddr>>>,
    peer_regions: Arc<RwLock<HashMap<SocketAddr, String>>>,
    peer_roles: Arc<RwLock<HashMap<SocketAddr, CacheMeshRole>>>,
    remote_slots: Arc<RwLock<HashSet<(u64, usize)>>>,
    stats: Arc<CacheMeshFecCounters>,
    leases: LeaseTable,
    served: ServedTable,
    link_loss: LinkLossTable,
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    if config.role == CacheMeshRole::Edge {
        return;
    }
    let demand = config.replication == ReplicationMode::Demand;
    let mut sender = FecSender::from_config_with_initial_block_id(
        &config,
        CHUNK_BLOCK_ID_BASE + config.block_id_offset,
        stats,
    )
    .with_link_loss(link_loss);
    let batched = config.batched_send;
    let mut batch_sender = BatchSender::new(&socket, batched && config.gso);
    if batch_sender.gso() {
        // The kernel cuts an object into datagrams that fit a packet: keep
        // every symbol within one packet rather than fragment large ones.
        sender = sender.with_max_symbol_size(0);
    }
    // The children's batches of a pass, kept from pass to pass for their
    // capacity: the live parts of the stream that the pass is on, sent when
    // the pass has queued them for every child of the stream (or a child's
    // batch fills a call), and the warm-up parts, sent after the live parts
    // of every stream of the pass.
    let mut batches: HashMap<SocketAddr, ChildBatch> = HashMap::new();
    let mut warm_batches: HashMap<SocketAddr, ChildBatch> = HashMap::new();
    let mut call_times: Vec<(usize, u64)> = Vec::new();
    let mut frame_header: Vec<u8> = Vec::new();
    // The payloads of the stream that the pass is on, by slot.
    let mut stream_payloads: HashMap<usize, Option<(Bytes, u64)>> = HashMap::new();
    // The streams that the cache says advanced, and the stream at each
    // index as the last look found it.
    let moved = cache.subscribe_publications();
    let mut moved_idxs: Vec<usize> = Vec::new();
    let mut stream_at: HashMap<usize, u64> = HashMap::new();
    let mut pass_streams: Vec<(u64, usize)> = Vec::new();
    let mut sent: HashMap<(u64, usize, SocketAddr), usize> = HashMap::new();
    let mut sent_initializations: HashMap<(u64, usize, SocketAddr), Bytes> = HashMap::new();
    // Demand replication state: the leases the last tick saw, the warm-up
    // window of each, and the warm-up byte allowance of each child.
    let mut active: HashSet<(SocketAddr, u64)> = HashSet::new();
    let mut pending_open: HashMap<(SocketAddr, u64), (usize, usize)> = HashMap::new();
    let mut backfills: HashMap<(SocketAddr, u64), Backfill> = HashMap::new();
    let mut backfill_tokens: HashMap<SocketAddr, f64> = HashMap::new();
    let mut last_remote_slot_prune = Instant::now();
    let mut last_tick = Instant::now();
    let mut child_cursor = 0usize;
    let mut interval = tokio::time::interval(config.sync_interval);
    // The children that the last tick chose. A pass woken by a publication
    // sends the new parts to them; only the tick changes the set.
    let mut children: Vec<SocketAddr> = Vec::new();
    // The children of the stream that the pass is on; reused for each stream.
    let mut stream_children: Vec<SocketAddr> = Vec::new();
    // The watermark of each stream when the last pass looked at it. A pass
    // woken by a publication visits only the streams whose watermark moved
    // since, and the parts at or below it were not published since then.
    let mut seen_last = SeenWatermarks::default();
    let publication = cache.publication_notify();
    let mut published = std::pin::pin!(publication.notified());
    published.as_mut().enable();

    loop {
        // A tick does the whole pass: leases, the choice of children, live
        // parts and warm-up windows. A publication sends the live parts at
        // once, so that a part does not wait for the next tick.
        let tick = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                debug!("cache mesh sync task shutting down");
                return;
            }
            _ = interval.tick() => true,
            _ = published.as_mut(), if config.sync_on_publish => false,
        };
        if config.sync_on_publish {
            // Arm the next wait before the watermarks are read, so that a
            // part published while this pass runs wakes the next one.
            published.set(publication.notified());
            published.as_mut().enable();
        }
        if tick {
            let now = Instant::now();
            let elapsed = now.duration_since(last_tick);
            last_tick = now;
            if demand {
                let mut current = HashSet::new();
                {
                    let mut leases = lock(&leases);
                    leases.retain(|_, lease| lease.expires > now);
                    for (key, lease) in leases.iter_mut() {
                        current.insert(*key);
                        if let Some(open) = lease.open.take() {
                            pending_open.insert(*key, open);
                        }
                    }
                }
                for (peer, stream_id) in active.difference(&current) {
                    // The lease ended. Forget the cursors, so that a later
                    // subscription starts with the initialization again.
                    sent.retain(|(sent_stream, _, sent_peer), _| {
                        sent_stream != stream_id || sent_peer != peer
                    });
                    sent_initializations.retain(|(sent_stream, _, sent_peer), _| {
                        sent_stream != stream_id || sent_peer != peer
                    });
                    backfills.remove(&(*peer, *stream_id));
                    pending_open.remove(&(*peer, *stream_id));
                }
                backfill_tokens.retain(|peer, _| current.iter().any(|(child, _)| child == peer));
                active = current;
                if config.backfill_bytes_per_sec > 0 {
                    let rate = config.backfill_bytes_per_sec as f64;
                    let burst = rate * BACKFILL_BURST.as_secs_f64();
                    for (peer, _) in &active {
                        let tokens = backfill_tokens.entry(*peer).or_insert(burst);
                        *tokens = (*tokens + rate * elapsed.as_secs_f64()).min(burst);
                    }
                }
                *lock(&served) = active
                    .iter()
                    .map(|(child, stream_id)| ServedSubscription {
                        child: *child,
                        stream_id: *stream_id,
                        backfill_parts_pending: pending_open
                            .get(&(*child, *stream_id))
                            .map(|(_, parts)| *parts)
                            .or_else(|| backfills.get(&(*child, *stream_id)).map(Backfill::pending))
                            .unwrap_or(0),
                    })
                    .collect();
            }
            children = {
                let regions = peer_regions.read().await;
                let roles = peer_roles.read().await;
                let mut peers = peers
                    .read()
                    .await
                    .iter()
                    .copied()
                    .filter(|peer| !demand || active.iter().any(|(child, _)| child == peer))
                    .filter(|peer| {
                        !config.same_region_only
                            || regions
                                .get(peer)
                                .is_some_and(|region| region == &config.region)
                    })
                    .filter(|peer| {
                        config.role != CacheMeshRole::Distributor
                            || roles
                                .get(peer)
                                .is_none_or(|role| *role == CacheMeshRole::Edge)
                    })
                    .collect::<Vec<_>>();
                peers.sort_unstable();
                let max_children = config.max_children.max(1);
                if peers.len() > max_children {
                    let offset = child_cursor % peers.len();
                    peers.rotate_left(offset);
                    child_cursor = child_cursor.wrapping_add(1);
                    peers.truncate(max_children);
                }
                peers
            };
            if children.is_empty() {
                // A node without children still learns slots from its
                // parents. An edge in demand replication never has a
                // child, and its set grew by one entry for each part
                // that it received, for the life of the process.
                let stream_ids = cache.stream_ids().await;
                // Keep the watermarks current, so that the parts published
                // long ago are not reported as live sends to a child that
                // comes later.
                seen_last.refresh(&cache, &stream_ids);
                if last_remote_slot_prune.elapsed() >= REMOTE_SLOT_PRUNE_INTERVAL {
                    last_remote_slot_prune = Instant::now();
                    forget_unretained_remote_slots(&cache, &remote_slots).await;
                }
                continue;
            }
        } else if children.is_empty() {
            continue;
        }

        // The streams of the pass: every stream on a tick, and on a
        // publication only those that the cache marked as advanced.
        moved_idxs.clear();
        moved.drain(&mut moved_idxs);
        if tick {
            let stream_ids = cache.stream_ids().await;
            if last_remote_slot_prune.elapsed() >= REMOTE_SLOT_PRUNE_INTERVAL {
                last_remote_slot_prune = Instant::now();
                forget_unretained_remote_slots(&cache, &remote_slots).await;
            }
            seen_last.retain_streams(&stream_ids);
            stream_at.clear();
            stream_at.extend(stream_ids.iter().map(|(stream_id, idx)| (*idx, *stream_id)));
            pass_streams = stream_ids;
        } else {
            if moved_idxs.is_empty() {
                continue;
            }
            let current = |stream_at: &HashMap<usize, u64>, idx: &usize| {
                stream_at
                    .get(idx)
                    .filter(|stream_id| cache.stream_idx(**stream_id) == Some(*idx))
                    .copied()
            };
            if moved_idxs
                .iter()
                .any(|idx| current(&stream_at, idx).is_none())
            {
                // A stream that is new since the last look.
                stream_at.clear();
                stream_at.extend(
                    cache
                        .stream_ids()
                        .await
                        .into_iter()
                        .map(|(stream_id, idx)| (idx, stream_id)),
                );
            }
            pass_streams.clear();
            pass_streams.extend(
                moved_idxs
                    .iter()
                    .filter_map(|idx| current(&stream_at, idx).map(|stream_id| (stream_id, *idx))),
            );
        }
        'streams: for &(stream_id, stream_idx) in &pass_streams {
            let last = cache.last(stream_idx);
            let previous = seen_last.record(stream_id, stream_idx, last);
            // A publication woke this pass: only a stream whose watermark
            // moved has a part to send. The tick does everything else.
            if !tick && (last.is_none() || last == previous) {
                continue;
            }
            // In demand replication a child receives a playlist only
            // while it holds a lease for it.
            stream_children.clear();
            if demand {
                stream_children.extend(
                    children
                        .iter()
                        .copied()
                        .filter(|peer| active.contains(&(*peer, stream_id))),
                );
            } else {
                stream_children.extend_from_slice(&children);
            }
            if stream_children.is_empty() {
                continue;
            }
            if let Some(initialization) = cache.stream_initialization(stream_id) {
                // Encoded only when a child lacks this initialization.
                let mut frame = None;
                for peer in &stream_children {
                    let key = (stream_id, stream_idx, *peer);
                    if sent_initializations
                        .get(&key)
                        .is_some_and(|sent| same_bytes(sent, &initialization))
                    {
                        continue;
                    }
                    if batched {
                        // The parts queued before it must not overtake it.
                        flush_child(
                            &socket,
                            &mut sender,
                            &mut batch_sender,
                            *peer,
                            batches.get_mut(peer),
                            &mut call_times,
                            &mut sent,
                            &config,
                        )
                        .await;
                    }
                    let frame = match &mut frame {
                        Some(frame) => frame,
                        None => match MeshFrame::initialization(
                            config.node_id.clone(),
                            stream_id,
                            initialization.clone(),
                        )
                        .encode()
                        {
                            Ok(encoded) => frame.insert(encoded),
                            Err(err) => {
                                warn!(stream_id, error = %err, "cache mesh initialization encode failed");
                                continue 'streams;
                            }
                        },
                    };
                    match sender.send(&socket, *peer, frame).await {
                        Ok(()) => {
                            sent_initializations.insert(key, initialization.clone());
                        }
                        Err(err) => {
                            debug!(peer = %peer, stream_id, error = %err, "cache mesh initialization send failed");
                        }
                    }
                }
            }
            let Some(last) = last else {
                continue;
            };
            // Parts above this slot were published since the last pass. A
            // stream that is new, or that restarted, has only such parts.
            let published_from = match previous {
                Some(previous) if previous <= last => previous + 1,
                _ => 0,
            };
            let retained_start = cache.retained_start(last);
            stream_payloads.clear();
            for peer in &stream_children {
                if let Some((from_slot, backfill_parts)) = pending_open.remove(&(*peer, stream_id))
                {
                    // A new lease starts live at the next part. The
                    // parts already stored are the warm-up window.
                    let low = from_slot
                        .max(retained_start)
                        .max(last.saturating_add(1).saturating_sub(backfill_parts));
                    sent.insert((stream_id, stream_idx, *peer), last);
                    if low <= last && backfill_parts > 0 {
                        backfills.insert(
                            (*peer, stream_id),
                            Backfill {
                                low,
                                next: Some(last),
                            },
                        );
                    }
                }
                // A cursor beyond the newest part means that the
                // stream restarted with a new publication, whose
                // sequence starts again. Send it from its beginning.
                let cursor = sent
                    .get(&(stream_id, stream_idx, *peer))
                    .copied()
                    .filter(|slot| *slot <= last);
                let next = cursor
                    .and_then(|slot| slot.checked_add(1))
                    .unwrap_or(retained_start)
                    .max(retained_start);
                if next > last {
                    continue;
                }
                // A child without a cursor is new, in full replication: the
                // parts that were there before this pass are its catch-up,
                // not live parts, and the send observer does not see them.
                let live_from = if cursor.is_some() {
                    next
                } else {
                    published_from.max(next)
                };

                if batched {
                    // The slots of the range that came from a peer, read
                    // under one lock.
                    let from_peers = if config.role != CacheMeshRole::Distributor {
                        let remote = remote_slots.read().await;
                        if remote.is_empty() {
                            Vec::new()
                        } else {
                            (next..=last)
                                .filter(|slot_id| remote.contains(&(stream_id, *slot_id)))
                                .collect()
                        }
                    } else {
                        Vec::new()
                    };
                    let batch = batches.entry(*peer).or_default();
                    for slot_id in next..=last {
                        if from_peers.binary_search(&slot_id).is_ok() {
                            batch.parts.push(QueuedPart {
                                stream_id,
                                stream_idx,
                                slot_id,
                                first_message: batch.messages.len(),
                                end_message: batch.messages.len(),
                                live: false,
                                warm_up: false,
                                object: None,
                            });
                            continue;
                        }
                        let Some((payload, hash)) =
                            stream_payload(&cache, &mut stream_payloads, stream_idx, slot_id).await
                        else {
                            continue;
                        };
                        chunk_frame_header(
                            &mut frame_header,
                            &config.node_id,
                            stream_id,
                            slot_id as u64,
                            payload.len(),
                        );
                        match sender.prepare_object(
                            *peer,
                            (stream_id, slot_id as u64),
                            hash,
                            &[&frame_header, &payload],
                        ) {
                            Ok((object, count)) => {
                                let first_message = batch.messages.len();
                                batch_sender.push(&mut batch.messages, &object, count);
                                batch.parts.push(QueuedPart {
                                    stream_id,
                                    stream_idx,
                                    slot_id,
                                    first_message,
                                    end_message: batch.messages.len(),
                                    live: slot_id >= live_from,
                                    warm_up: false,
                                    object: Some((object, count)),
                                });
                                if batch.messages.len() >= MESSAGES_PER_CALL {
                                    // A full call: a catch-up of many parts
                                    // does not hold the first of them back.
                                    flush_child(
                                        &socket,
                                        &mut sender,
                                        &mut batch_sender,
                                        *peer,
                                        Some(batch),
                                        &mut call_times,
                                        &mut sent,
                                        &config,
                                    )
                                    .await;
                                }
                            }
                            Err(err) => {
                                warn!(stream_id, slot_id, error = %err, "cache mesh chunk encode failed");
                            }
                        }
                    }
                    continue;
                }

                for slot_id in next..=last {
                    if config.role != CacheMeshRole::Distributor
                        && remote_slots.read().await.contains(&(stream_id, slot_id))
                    {
                        sent.insert((stream_id, stream_idx, *peer), slot_id);
                        continue;
                    }
                    let Some((payload, hash)) = cache.get(stream_idx, slot_id).await else {
                        continue;
                    };
                    if hash == 0 && payload.is_empty() {
                        continue;
                    }
                    let frame = match MeshFrame::chunk(
                        config.node_id.clone(),
                        stream_id,
                        slot_id as u64,
                        payload,
                    )
                    .encode()
                    {
                        Ok(frame) => frame,
                        Err(err) => {
                            warn!(stream_id, slot_id, error = %err, "cache mesh chunk encode failed");
                            continue;
                        }
                    };
                    match sender
                        .send_object(&socket, *peer, (stream_id, slot_id as u64), hash, &frame)
                        .await
                    {
                        Ok(sent_us) => {
                            sent.insert((stream_id, stream_idx, *peer), slot_id);
                            if slot_id >= live_from {
                                if let Some(observer) = &config.send_observer.0 {
                                    observer(stream_id, slot_id as u64, *peer, sent_us);
                                }
                            }
                        }
                        Err(err) => {
                            debug!(peer = %peer, stream_id, slot_id, error = %err, "cache mesh chunk send failed");
                            break;
                        }
                    }
                }
            }

            if batched {
                // The stream's live parts leave now, one call for each
                // child, before the pass encodes the next stream.
                for peer in &stream_children {
                    flush_child(
                        &socket,
                        &mut sender,
                        &mut batch_sender,
                        *peer,
                        batches.get_mut(peer),
                        &mut call_times,
                        &mut sent,
                        &config,
                    )
                    .await;
                }
            }

            // Warm-up windows go after the live parts of the tick,
            // newest part first, within the allowance of each child.
            // Only the tick sends them: it paces them.
            if !tick {
                continue;
            }
            for peer in &stream_children {
                let key = (*peer, stream_id);
                let Some(mut backfill) = backfills.get(&key).copied() else {
                    continue;
                };
                let limited = config.backfill_bytes_per_sec > 0;
                while let Some(slot_id) = backfill.next {
                    if slot_id < backfill.low.max(retained_start) {
                        backfill.next = None;
                        break;
                    }
                    if limited && backfill_tokens.get(peer).copied().unwrap_or(0.0) <= 0.0 {
                        break;
                    }
                    backfill.next = slot_id.checked_sub(1);
                    let Some((payload, hash)) = cache.get(stream_idx, slot_id).await else {
                        continue;
                    };
                    if hash == 0 && payload.is_empty() {
                        continue;
                    }
                    let bytes = payload.len();
                    if batched {
                        // Sent after the live parts of every stream of the
                        // pass.
                        chunk_frame_header(
                            &mut frame_header,
                            &config.node_id,
                            stream_id,
                            slot_id as u64,
                            payload.len(),
                        );
                        match sender.prepare_object(
                            *peer,
                            (stream_id, slot_id as u64),
                            hash,
                            &[&frame_header, &payload],
                        ) {
                            Ok((object, count)) => {
                                let batch = warm_batches.entry(*peer).or_default();
                                let first_message = batch.messages.len();
                                batch_sender.push(&mut batch.messages, &object, count);
                                batch.parts.push(QueuedPart {
                                    stream_id,
                                    stream_idx,
                                    slot_id,
                                    first_message,
                                    end_message: batch.messages.len(),
                                    live: false,
                                    warm_up: true,
                                    object: Some((object, count)),
                                });
                            }
                            Err(err) => {
                                warn!(stream_id, slot_id, error = %err, "cache mesh chunk encode failed");
                                continue;
                            }
                        }
                    } else {
                        let frame = match MeshFrame::chunk(
                            config.node_id.clone(),
                            stream_id,
                            slot_id as u64,
                            payload,
                        )
                        .encode()
                        {
                            Ok(frame) => frame,
                            Err(err) => {
                                warn!(stream_id, slot_id, error = %err, "cache mesh chunk encode failed");
                                continue;
                            }
                        };
                        match sender
                            .send_object(&socket, *peer, (stream_id, slot_id as u64), hash, &frame)
                            .await
                        {
                            // A warm-up part is not a live send: the observer
                            // does not see it.
                            Ok(_) => {}
                            Err(err) => {
                                debug!(peer = %peer, stream_id, slot_id, error = %err, "cache mesh warm-up send failed");
                                break;
                            }
                        }
                    }
                    if limited {
                        if let Some(tokens) = backfill_tokens.get_mut(peer) {
                            *tokens -= bytes as f64;
                        }
                    }
                }
                if backfill.next.is_none() {
                    backfills.remove(&key);
                } else {
                    backfills.insert(key, backfill);
                }
            }
        }

        if batched {
            // The warm-up parts of the pass, after every live part, each
            // child's in as few calls as the kernel takes them. (The live
            // batches are empty here: each stream sent its own.)
            for (peer, batch) in batches.iter_mut().chain(warm_batches.iter_mut()) {
                if batch.is_empty() {
                    continue;
                }
                flush_child(
                    &socket,
                    &mut sender,
                    &mut batch_sender,
                    *peer,
                    Some(batch),
                    &mut call_times,
                    &mut sent,
                    &config,
                )
                .await;
            }
            if batches.len() > children.len().max(1) * 4 {
                batches.retain(|peer, _| children.contains(peer));
            }
            if warm_batches.len() > children.len().max(1) * 4 {
                warm_batches.retain(|peer, _| children.contains(peer));
            }
        }
    }
}

/// The payload of `slot_id` of a stream and its hash, read from the cache
/// once for every child of the pass. `None` for a slot that the cache does
/// not hold.
async fn stream_payload(
    cache: &ChunkCache,
    payloads: &mut HashMap<usize, Option<(Bytes, u64)>>,
    stream_idx: usize,
    slot_id: usize,
) -> Option<(Bytes, u64)> {
    if let Some(payload) = payloads.get(&slot_id) {
        return payload.clone();
    }
    let payload = match cache.get(stream_idx, slot_id).await {
        Some((payload, hash)) if hash != 0 || !payload.is_empty() => Some((payload, hash)),
        _ => None,
    };
    payloads.insert(slot_id, payload.clone());
    payload
}

/// Hand a child's queued datagrams to the kernel, then move its cursors and
/// tell the send observer of each live part, with the time of the call that
/// carried the part's first datagram. A part whose datagrams did not all
/// leave moves no cursor, and neither does any part after it.
#[allow(clippy::too_many_arguments)]
async fn flush_child(
    socket: &UdpSocket,
    sender: &mut FecSender,
    batch_sender: &mut BatchSender,
    peer: SocketAddr,
    batch: Option<&mut ChildBatch>,
    call_times: &mut Vec<(usize, u64)>,
    sent: &mut HashMap<(u64, usize, SocketAddr), usize>,
    config: &CacheMeshConfig,
) {
    let Some(batch) = batch else {
        return;
    };
    if batch.is_empty() {
        return;
    }
    call_times.clear();
    let (delivered, error) = batch_sender
        .send(socket, peer, &batch.messages, call_times)
        .await;
    for part in &batch.parts {
        if part.end_message > delivered {
            break;
        }
        if let Some((object, count)) = &part.object {
            sender.count_sent(object, *count);
        }
        if part.warm_up {
            continue;
        }
        sent.insert((part.stream_id, part.stream_idx, peer), part.slot_id);
        if part.live && part.end_message > part.first_message {
            if let Some(observer) = &config.send_observer.0 {
                observer(
                    part.stream_id,
                    part.slot_id as u64,
                    peer,
                    call_time(call_times, part.first_message),
                );
            }
        }
    }
    if let Some(err) = error {
        sender.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
        debug!(peer = %peer, error = %err, "cache mesh batched send failed");
    }
    batch.messages.clear();
    batch.parts.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Options;
    use tokio::time::{timeout, Instant};

    fn loopback_addr() -> SocketAddr {
        static NEXT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(25_000);

        loop {
            let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            if std::net::UdpSocket::bind(addr).is_ok() {
                return addr;
            }
        }
    }

    #[test]
    fn mesh_frame_roundtrips() {
        let frame = MeshFrame::chunk("uk", 42, 7, Bytes::from_static(b"media"));
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);

        let frame = MeshFrame::initialization("uk", 42, Bytes::from_static(b"ftyp-moov-init"));
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);

        let frame = MeshFrame::hello_with_peers("us", "us-east", Vec::new());
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);

        let peer: SocketAddr = "127.0.0.1:9911".parse().unwrap();
        let frame = MeshFrame::hello_with_peers("us", "us-east", vec![peer]);
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);

        let mut legacy_hello = BytesMut::new();
        legacy_hello.put_slice(FRAME_MAGIC);
        legacy_hello.put_u8(FRAME_VERSION_LEGACY);
        legacy_hello.put_u8(FRAME_HELLO);
        legacy_hello.put_u16(2);
        legacy_hello.put_slice(b"uk");
        legacy_hello.put_u16(7);
        legacy_hello.put_slice(b"uk-west");
        assert_eq!(
            MeshFrame::decode(&legacy_hello).unwrap(),
            MeshFrame::hello_with_peers("uk", "uk-west", Vec::new())
        );

        let frame = MeshFrame::replica_request("jp", 42, 3);
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[tokio::test]
    async fn cache_mesh_syncs_slots_between_two_nodes() {
        let addr_a = loopback_addr();
        let addr_b = loopback_addr();

        let options = Options {
            num_playlists: 4,
            max_segments: 1,
            max_parts_per_segment: 16,
            buffer_size_kb: 4,
            ..Options::default()
        };

        let cache_a = Arc::new(ChunkCache::new(options));
        let cache_b = Arc::new(ChunkCache::new(options));

        let mesh_a = CacheMesh::new(
            Arc::clone(&cache_a),
            CacheMeshConfig::new("uk", "uk", addr_a).with_peer(addr_b),
        )
        .start()
        .await
        .unwrap();
        let mesh_b = CacheMesh::new(
            Arc::clone(&cache_b),
            CacheMeshConfig::new("us", "us", addr_b).with_peer(addr_a),
        )
        .start()
        .await
        .unwrap();

        cache_a
            .set_stream_initialization(99, Bytes::from_static(b"ftyp-moov"))
            .await
            .unwrap();
        cache_a
            .add_for_stream_id(99, 0, Bytes::from_static(b"ts-part-0"))
            .await
            .unwrap();
        cache_a
            .add_for_stream_id(99, 1, Bytes::from_static(b"ts-part-1"))
            .await
            .unwrap();

        timeout(Duration::from_secs(2), async {
            let start = Instant::now();
            loop {
                if let Some((bytes, _hash)) = cache_b.get_for_stream_id(99, 1).await {
                    if bytes == Bytes::from_static(b"ts-part-1")
                        && cache_b.stream_initialization(99).as_deref() == Some(b"ftyp-moov")
                    {
                        break;
                    }
                }
                assert!(start.elapsed() < Duration::from_secs(2));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        assert!(mesh_a.peers().await.contains(&addr_b));
        assert!(mesh_b.peers().await.contains(&addr_a));
        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn regional_distributor_feeds_edge_without_leaf_forwarding() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let options = Options {
            num_playlists: 4,
            max_segments: 1,
            max_parts_per_segment: 16,
            buffer_size_kb: 4,
            ..Options::default()
        };
        let parent_cache = Arc::new(ChunkCache::new(options));
        let edge_cache = Arc::new(ChunkCache::new(options));
        let mut parent_config = CacheMeshConfig::new("parent", "uk", parent_addr)
            .with_peer(edge_addr)
            .with_same_region_only(true)
            .with_role(CacheMeshRole::Distributor);
        parent_config.sync_interval = Duration::from_millis(10);
        let mut edge_config = CacheMeshConfig::new("edge", "uk", edge_addr)
            .with_peer(parent_addr)
            .with_same_region_only(true)
            .with_role(CacheMeshRole::Edge);
        edge_config.sync_interval = Duration::from_millis(10);
        let parent = CacheMesh::new(Arc::clone(&parent_cache), parent_config)
            .start()
            .await
            .unwrap();
        let edge = CacheMesh::new(Arc::clone(&edge_cache), edge_config)
            .start()
            .await
            .unwrap();

        parent_cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"parent-part"))
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if edge_cache.get_for_stream_id(7, 0).await.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        edge_cache
            .add_for_stream_id(8, 0, Bytes::from_static(b"edge-part"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(parent_cache.get_for_stream_id(8, 0).await.is_none());

        parent.shutdown();
        edge.shutdown();
    }

    #[tokio::test]
    async fn distributor_forwards_a_parent_replica_to_edges() {
        let upstream_addr = loopback_addr();
        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let options = Options {
            num_playlists: 4,
            max_segments: 1,
            max_parts_per_segment: 16,
            buffer_size_kb: 4,
            ..Options::default()
        };
        let upstream_cache = Arc::new(ChunkCache::new(options));
        let distributor_cache = Arc::new(ChunkCache::new(options));
        let edge_cache = Arc::new(ChunkCache::new(options));
        let mut upstream_config = CacheMeshConfig::new("upstream", "uk", upstream_addr)
            .with_peer(distributor_addr)
            .with_same_region_only(true)
            .with_role(CacheMeshRole::Distributor);
        upstream_config.sync_interval = Duration::from_millis(10);
        let mut distributor_config = CacheMeshConfig::new("distributor", "uk", distributor_addr)
            .with_peers([upstream_addr, edge_addr])
            .with_same_region_only(true)
            .with_role(CacheMeshRole::Distributor);
        distributor_config.sync_interval = Duration::from_millis(10);
        let mut edge_config = CacheMeshConfig::new("edge", "uk", edge_addr)
            .with_peer(distributor_addr)
            .with_same_region_only(true)
            .with_role(CacheMeshRole::Edge);
        edge_config.sync_interval = Duration::from_millis(10);
        let upstream = CacheMesh::new(Arc::clone(&upstream_cache), upstream_config)
            .start()
            .await
            .unwrap();
        let distributor = CacheMesh::new(Arc::clone(&distributor_cache), distributor_config)
            .start()
            .await
            .unwrap();
        let edge = CacheMesh::new(Arc::clone(&edge_cache), edge_config)
            .start()
            .await
            .unwrap();

        upstream_cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"parent-part"))
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if distributor.peers().await.contains(&upstream_addr) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if distributor.request_replica(7, 0).await.unwrap() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if distributor_cache.get_for_stream_id(7, 0).await.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                if edge_cache.get_for_stream_id(7, 0).await.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        upstream.shutdown();
        distributor.shutdown();
        edge.shutdown();
    }

    #[tokio::test]
    async fn same_region_mesh_rejects_cross_region_leaf() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let options = Options {
            num_playlists: 4,
            max_segments: 1,
            max_parts_per_segment: 16,
            buffer_size_kb: 4,
            ..Options::default()
        };
        let parent_cache = Arc::new(ChunkCache::new(options));
        let edge_cache = Arc::new(ChunkCache::new(options));
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            CacheMeshConfig::new("parent", "uk", parent_addr)
                .with_peer(edge_addr)
                .with_same_region_only(true)
                .with_role(CacheMeshRole::Distributor),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "jp", edge_addr)
                .with_peer(parent_addr)
                .with_same_region_only(true)
                .with_role(CacheMeshRole::Edge),
        )
        .start()
        .await
        .unwrap();
        parent_cache
            .add_for_stream_id(9, 0, Bytes::from_static(b"cross-region"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(edge_cache.get_for_stream_id(9, 0).await.is_none());
        parent.shutdown();
        edge.shutdown();
    }

    #[tokio::test]
    async fn cache_mesh_gossips_peer_addresses_from_seed_node() {
        let addr_a = loopback_addr();
        let addr_b = loopback_addr();
        let addr_c = loopback_addr();

        let options = Options {
            num_playlists: 4,
            max_segments: 1,
            max_parts_per_segment: 16,
            buffer_size_kb: 4,
            ..Options::default()
        };

        let cache_a = Arc::new(ChunkCache::new(options));
        let cache_b = Arc::new(ChunkCache::new(options));
        let cache_c = Arc::new(ChunkCache::new(options));

        let mut config_a = CacheMeshConfig::new("uk", "uk", addr_a).with_peer(addr_b);
        config_a.announce_interval = Duration::from_millis(20);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b = CacheMeshConfig::new("us", "us", addr_b)
            .with_peer(addr_a)
            .with_peer(addr_c);
        config_b.announce_interval = Duration::from_millis(20);
        config_b.sync_interval = Duration::from_secs(60);
        let mut config_c = CacheMeshConfig::new("jp", "jp", addr_c).with_peer(addr_b);
        config_c.announce_interval = Duration::from_millis(20);
        config_c.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = CacheMesh::new(Arc::clone(&cache_b), config_b)
            .start()
            .await
            .unwrap();
        let mesh_c = CacheMesh::new(Arc::clone(&cache_c), config_c)
            .start()
            .await
            .unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                let peers_a = mesh_a.peers().await;
                let peers_c = mesh_c.peers().await;
                if peers_a.contains(&addr_c) && peers_c.contains(&addr_a) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        mesh_a.shutdown();
        mesh_b.shutdown();
        mesh_c.shutdown();
    }

    #[tokio::test]
    async fn cache_mesh_replica_request_fetches_stream_on_demand() {
        let addr_a = loopback_addr();
        let addr_b = loopback_addr();

        let options = Options {
            num_playlists: 4,
            max_segments: 1,
            max_parts_per_segment: 16,
            buffer_size_kb: 4,
            ..Options::default()
        };

        let cache_a = Arc::new(ChunkCache::new(options));
        let cache_b = Arc::new(ChunkCache::new(options));

        let mut config_a = CacheMeshConfig::new("uk", "uk", addr_a).with_peer(addr_b);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b = CacheMeshConfig::new("jp", "jp", addr_b).with_peer(addr_a);
        config_b.sync_interval = Duration::from_secs(60);

        let mesh_a = CacheMesh::new(Arc::clone(&cache_a), config_a)
            .start()
            .await
            .unwrap();
        let mesh_b = CacheMesh::new(Arc::clone(&cache_b), config_b)
            .start()
            .await
            .unwrap();

        cache_a
            .add_for_stream_id(707, 0, Bytes::from_static(b"warm-part-0"))
            .await
            .unwrap();
        cache_a
            .add_for_stream_id(707, 1, Bytes::from_static(b"warm-part-1"))
            .await
            .unwrap();

        let requested = mesh_b.request_replica(707, 0).await.unwrap();
        assert_eq!(requested, 1);

        timeout(Duration::from_secs(2), async {
            let start = Instant::now();
            loop {
                if let Some((bytes, _hash)) = cache_b.get_for_stream_id(707, 1).await {
                    if bytes == Bytes::from_static(b"warm-part-1") {
                        break;
                    }
                }
                assert!(start.elapsed() < Duration::from_secs(2));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        mesh_a.shutdown();
        mesh_b.shutdown();
    }

    #[tokio::test]
    async fn fec_receiver_ignores_completed_block_repairs() {
        let tx = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let rx = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let rx_addr = rx.local_addr().unwrap();
        let mut sender = FecSender::new(2, DEFAULT_SYMBOL_SIZE);
        let receiver_stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::new(Arc::clone(&receiver_stats));

        sender.send(&tx, rx_addr, b"mesh-frame").await.unwrap();

        let mut buf = vec![0u8; 65_536];
        let mut decoded = Vec::new();
        for _ in 0..3 {
            let (len, peer) = rx.recv_from(&mut buf).await.unwrap();
            if let Some(frame) = receiver.push(peer, &buf[..len]).unwrap() {
                decoded.push(frame);
            }
        }

        assert_eq!(decoded, vec![Bytes::from_static(b"mesh-frame")]);
        assert_eq!(sender.stats.snapshot().tx_source_datagrams, 1);
        assert_eq!(sender.stats.snapshot().tx_repair_datagrams, 2);
        assert_eq!(receiver_stats.snapshot().rx_decoded_objects, 1);
        assert_eq!(receiver_stats.snapshot().rx_repaired_objects, 0);
    }

    /// A restarted publication numbers its slots from the start again. A
    /// child that was not sent the old encoding of a slot must get the new
    /// payload, even when it has the same length; the same payload is still
    /// encoded once for every child.
    #[test]
    fn an_encoding_is_reused_only_for_the_same_payload() {
        let mut sender = FecSender::new(1, DEFAULT_SYMBOL_SIZE).with_fitted_symbols(true);
        let [a, b, c]: [SocketAddr; 3] = [
            "127.0.0.1:9001".parse().unwrap(),
            "127.0.0.1:9002".parse().unwrap(),
            "127.0.0.1:9003".parse().unwrap(),
        ];
        let old = vec![1_u8; 96];
        let new = vec![2_u8; 96];
        let (first, _) = sender
            .prepare_object(a, (7, 5), xxh3_64(&old), &[&old])
            .unwrap();
        let (restarted, _) = sender
            .prepare_object(b, (7, 5), xxh3_64(&new), &[&new])
            .unwrap();
        let symbol = |object: &ObjectDatagrams| object.datagram(0)[HEADER_LEN + 4..].to_vec();
        assert_eq!(symbol(&first), old);
        assert_eq!(symbol(&restarted), new);
        let (shared, _) = sender
            .prepare_object(c, (7, 5), xxh3_64(&new), &[&new])
            .unwrap();
        assert_eq!(shared.wire.as_ptr(), restarted.wire.as_ptr());
    }

    #[test]
    fn fec_receiver_reports_source_symbol_recovery() {
        let payload = b"mesh-frame-with-several-symbols";
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(2);
        let datagrams = encoder.encode_object(payload).unwrap();
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::new(Arc::clone(&stats));
        let peer: SocketAddr = "127.0.0.1:9911".parse().unwrap();
        let mut decoded = None;

        for datagram in &datagrams {
            let header = DatagramFecHeader::decode(datagram).unwrap();
            let encoding_symbol_id = u32::from_be_bytes([
                0,
                datagram[HEADER_LEN + 1],
                datagram[HEADER_LEN + 2],
                datagram[HEADER_LEN + 3],
            ]);
            if encoding_symbol_id == 1 && encoding_symbol_id < u32::from(header.source_symbols) {
                continue;
            }
            if let Some(frame) = receiver.push(peer, datagram).unwrap() {
                decoded = Some(frame);
                break;
            }
        }

        assert_eq!(decoded.as_deref(), Some(payload.as_slice()));
        let stats = stats.snapshot();
        assert_eq!(stats.rx_decoded_objects, 1);
        assert_eq!(stats.rx_repaired_objects, 1);
        assert_eq!(stats.rx_repaired_source_datagrams, 1);
        assert_eq!(stats.rx_late_source_datagrams, 0);
        assert_eq!(stats.rx_presumed_lost_source_datagrams, 0);
        assert_eq!(stats.rx_decode_errors, 0);
        assert_eq!(stats.rx_inflight_objects, 0);

        let late_source = datagrams
            .iter()
            .find(|datagram| {
                u32::from_be_bytes([
                    0,
                    datagram[HEADER_LEN + 1],
                    datagram[HEADER_LEN + 2],
                    datagram[HEADER_LEN + 3],
                ]) == 1
            })
            .unwrap();
        assert_eq!(receiver.push(peer, late_source).unwrap(), None);
        assert_eq!(stats.rx_late_source_datagrams, 0);
        assert_eq!(receiver.stats.snapshot().rx_late_source_datagrams, 1);
    }

    #[test]
    fn the_slot_of_a_chunk_frame_is_read_from_its_first_bytes() {
        let frame = MeshFrame::chunk("distributor-a", 3001, 77, Bytes::from_static(b"part"))
            .encode()
            .unwrap();
        assert_eq!(chunk_frame_slot(&frame[..45]), Some((3001, 77)));
        assert_eq!(chunk_frame_slot(&frame), Some((3001, 77)));
        assert_eq!(chunk_frame_slot(&frame[..20]), None);
        assert_eq!(
            chunk_frame_payload(&frame),
            Some((3001, 77, Bytes::from_static(b"part")))
        );
        let hello = MeshFrame::hello_with_role("n", "r", CacheMeshRole::Edge, Vec::new())
            .encode()
            .unwrap();
        assert_eq!(chunk_frame_slot(&hello), None);
    }

    #[test]
    fn a_source_symbol_that_arrives_late_is_not_counted_as_loss() {
        let payload = b"mesh-frame-with-several-symbols";
        let symbol_id = |datagram: &Vec<u8>| {
            u32::from_be_bytes([
                0,
                datagram[HEADER_LEN + 1],
                datagram[HEADER_LEN + 2],
                datagram[HEADER_LEN + 3],
            ])
        };
        let peer: SocketAddr = "127.0.0.1:9913".parse().unwrap();
        // A path that reorders: the repair symbols overtake source symbol 1.
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(2);
        let datagrams = encoder.encode_object(payload).unwrap();
        let mut receiver = FecReceiver::new(Arc::new(CacheMeshFecCounters::default()));
        let mut decoded = false;
        for datagram in datagrams.iter().filter(|datagram| symbol_id(datagram) != 1) {
            if receiver.push(peer, datagram).unwrap().is_some() {
                decoded = true;
                break;
            }
        }
        assert!(decoded);
        // The block is decoded, and its loss is not counted yet.
        assert!(receiver.loss.get(&peer).is_none());
        let late = datagrams
            .iter()
            .find(|datagram| symbol_id(datagram) == 1)
            .unwrap();
        assert_eq!(receiver.push(peer, late).unwrap(), None);
        let window = receiver.loss.get(&peer).expect("the block is counted");
        assert!(window.expected_packets > 0);
        assert_eq!(window.lost_symbols, 0);

        // A path that loses the symbol: it counts once it has had its time.
        let mut receiver = FecReceiver::new(Arc::new(CacheMeshFecCounters::default()));
        for datagram in datagrams.iter().filter(|datagram| symbol_id(datagram) != 1) {
            if receiver.push(peer, datagram).unwrap().is_some() {
                break;
            }
        }
        std::thread::sleep(LATE_SOURCE_GRACE + Duration::from_millis(30));
        receiver.prune_observations();
        let window = receiver.loss.get(&peer).expect("the block is counted");
        assert_eq!(window.lost_symbols, 1);
    }

    #[test]
    fn fec_receiver_ages_repaired_sources_into_presumed_loss() {
        let payload = b"mesh-frame-with-several-symbols";
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(2);
        let datagrams = encoder.encode_object(payload).unwrap();
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::new(Arc::clone(&stats));
        let peer: SocketAddr = "127.0.0.1:9912".parse().unwrap();

        for datagram in &datagrams {
            let header = DatagramFecHeader::decode(datagram).unwrap();
            let encoding_symbol_id = u32::from_be_bytes([
                0,
                datagram[HEADER_LEN + 1],
                datagram[HEADER_LEN + 2],
                datagram[HEADER_LEN + 3],
            ]);
            if encoding_symbol_id == 1 && encoding_symbol_id < u32::from(header.source_symbols) {
                continue;
            }
            if receiver.push(peer, datagram).unwrap().is_some() {
                break;
            }
        }

        receiver.observed_datagrams = receiver
            .observed_datagrams
            .saturating_add(FEC_OBSERVATION_DATAGRAM_WINDOW + 1);
        receiver.prune_observations();
        let stats = stats.snapshot();
        assert_eq!(stats.rx_repaired_source_datagrams, 1);
        assert_eq!(stats.rx_late_source_datagrams, 0);
        assert_eq!(stats.rx_presumed_lost_source_datagrams, 1);
    }

    #[test]
    fn mesh_fec_scales_repair_with_payload_and_caps_overhead() {
        let sender = FecSender::with_policy_and_initial_block_id(
            1,
            0.03,
            16,
            DEFAULT_SYMBOL_SIZE,
            0,
            Arc::new(CacheMeshFecCounters::default()),
        );

        assert_eq!(sender.repair_symbols_for(1), 1);
        assert_eq!(
            sender.repair_symbols_for(50 * usize::from(DEFAULT_SYMBOL_SIZE)),
            2
        );
        assert_eq!(
            sender.repair_symbols_for(500 * usize::from(DEFAULT_SYMBOL_SIZE)),
            15
        );
        assert_eq!(
            sender.repair_symbols_for(1_000 * usize::from(DEFAULT_SYMBOL_SIZE)),
            16
        );
    }

    fn demand_options() -> Options {
        Options {
            num_playlists: 4,
            max_segments: 1,
            max_parts_per_segment: 64,
            buffer_size_kb: 4,
            ..Options::default()
        }
    }

    /// Slots that hold parts of some tens of kilobytes.
    fn large_part_options() -> Options {
        Options {
            max_parts_per_segment: 256,
            buffer_size_kb: 64,
            ..demand_options()
        }
    }

    fn demand_distributor(name: &str, addr: SocketAddr) -> CacheMeshConfig {
        let mut config = CacheMeshConfig::new(name, "uk", addr)
            .with_same_region_only(true)
            .with_role(CacheMeshRole::Distributor)
            .with_replication(ReplicationMode::Demand)
            .with_subscription_lease(Duration::from_millis(600));
        config.sync_interval = Duration::from_millis(10);
        config.announce_interval = Duration::from_millis(100);
        config
    }

    fn demand_edge(name: &str, addr: SocketAddr) -> CacheMeshConfig {
        let mut config = CacheMeshConfig::new(name, "uk", addr)
            .with_same_region_only(true)
            .with_role(CacheMeshRole::Edge)
            .with_replication(ReplicationMode::Demand)
            .with_subscription_lease(Duration::from_millis(600));
        config.sync_interval = Duration::from_millis(10);
        config.announce_interval = Duration::from_millis(100);
        config
    }

    async fn wait_for_slot(cache: &ChunkCache, stream_id: u64, slot_id: usize) {
        timeout(Duration::from_secs(3), async {
            loop {
                if cache.get_for_stream_id(stream_id, slot_id).await.is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("stream {stream_id} slot {slot_id} did not arrive"));
    }

    /// Wait until the child has learned the role and region of its parents.
    async fn wait_for_hello(handle: &CacheMeshHandle, parents: usize) {
        timeout(Duration::from_secs(3), async {
            loop {
                if handle.peer_roles.read().await.len() >= parents {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("parents did not announce themselves");
    }

    #[test]
    fn subscription_frames_roundtrip() {
        let frame = MeshFrame::Subscribe {
            node_id: "edge-1".into(),
            stream_id: 2001,
            from_slot: 77,
            backfill_parts: 32,
            lease_ms: 5_000,
        };
        assert_eq!(MeshFrame::decode(&frame.encode().unwrap()).unwrap(), frame);

        let frame = MeshFrame::Unsubscribe {
            node_id: "edge-1".into(),
            stream_id: 2001,
        };
        assert_eq!(MeshFrame::decode(&frame.encode().unwrap()).unwrap(), frame);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn demand_replication_pushes_only_subscribed_playlists() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr).with_peer(parent_addr),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;

        for slot in 0..4 {
            for stream_id in [7_u64, 8] {
                parent_cache
                    .add_for_stream_id(stream_id, slot, Bytes::from(vec![stream_id as u8; 64]))
                    .await
                    .unwrap();
            }
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            edge_cache.get_stream_idx(7).await.is_none(),
            "a playlist without demand must not be replicated"
        );
        assert!(parent.served_subscriptions().is_empty());

        // Demand for playlist 7 arrives. The edge receives the stored window
        // and then every new part. Playlist 8 stays at the distributor.
        assert_eq!(edge.subscribe(7, 0, 16).await.unwrap(), vec![parent_addr]);
        for slot in 0..4 {
            wait_for_slot(&edge_cache, 7, slot).await;
        }
        parent_cache
            .add_for_stream_id(7, 4, Bytes::from(vec![7_u8; 64]))
            .await
            .unwrap();
        parent_cache
            .add_for_stream_id(8, 4, Bytes::from(vec![8_u8; 64]))
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 4).await;
        assert!(edge_cache.get_stream_idx(8).await.is_none());
        assert_eq!(edge.subscribed_streams(), vec![7]);
        let served = parent.served_subscriptions();
        assert_eq!(served.len(), 1);
        assert_eq!((served[0].child, served[0].stream_id), (edge_addr, 7));

        parent.shutdown();
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn demand_replication_stops_when_the_lease_lapses_or_is_released() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr).with_peer(parent_addr),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;

        parent_cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"part-0"))
            .await
            .unwrap();
        edge.subscribe(7, 0, 16).await.unwrap();
        wait_for_slot(&edge_cache, 7, 0).await;

        // No renewal: the lease of 600 ms lapses and the push stops.
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(parent.served_subscriptions().is_empty());
        parent_cache
            .add_for_stream_id(7, 1, Bytes::from_static(b"part-1"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(edge_cache.get_for_stream_id(7, 1).await.is_none());

        // A new subscription asks from the first missing slot and receives it.
        edge.subscribe(7, 1, 16).await.unwrap();
        wait_for_slot(&edge_cache, 7, 1).await;

        // An explicit release stops the push at once. A part that is already
        // on the wire is not accepted afterwards.
        assert_eq!(edge.unsubscribe(7).await.unwrap(), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(parent.served_subscriptions().is_empty());
        parent_cache
            .add_for_stream_id(7, 2, Bytes::from_static(b"part-2"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(edge_cache.get_for_stream_id(7, 2).await.is_none());
        assert!(edge.subscribed_streams().is_empty());

        parent.shutdown();
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn warm_up_window_is_bounded_paced_and_newest_first() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        // 1,000-byte parts at 20,000 bytes per second: 2,000 bytes of burst,
        // then one part every 50 ms.
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr)
                .with_peer(edge_addr)
                .with_backfill_bytes_per_sec(20_000),
        )
        .start()
        .await
        .unwrap();
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr)
                .with_peer(parent_addr)
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;

        for slot in 0..20 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 1_000]))
                .await
                .unwrap();
        }
        let started = Instant::now();
        // The edge asks for at most eight parts before the newest part.
        edge.subscribe(7, 0, 8).await.unwrap();
        let mut order = Vec::new();
        while order.len() < 8 {
            let applied = timeout(Duration::from_secs(3), applied_rx.recv())
                .await
                .expect("warm-up part did not arrive")
                .unwrap();
            assert_eq!(applied.stream_id, 7);
            assert_eq!(applied.bytes, 1_000);
            order.push(applied.slot_id);
        }
        let elapsed = started.elapsed();
        assert_eq!(order, vec![19, 18, 17, 16, 15, 14, 13, 12]);
        assert!(
            elapsed >= Duration::from_millis(200),
            "eight 1,000-byte parts at 20 kB/s arrived in {elapsed:?}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(edge_cache.get_for_stream_id(7, 11).await.is_none());

        // A live part is not held behind the allowance.
        parent_cache
            .add_for_stream_id(7, 20, Bytes::from(vec![20_u8; 1_000]))
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 20).await;

        parent.shutdown();
        edge.shutdown();
    }

    /// A child's warm-up window goes after the live parts of every stream of
    /// the pass, so a live part never waits behind another stream's
    /// warm-up. (When the pass visits the warm-up stream first, a pass that
    /// queued everything in one batch per child sent the window first.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_live_part_is_not_held_behind_a_warm_up_window() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        // Only the tick sends, so that the live part and the warm-up window
        // share a pass.
        let mut parent_config = demand_distributor("parent", parent_addr)
            .with_peer(edge_addr)
            .with_sync_on_publish(false)
            .with_subscription_lease(Duration::from_secs(5));
        parent_config.sync_interval = Duration::from_millis(200);
        let parent = CacheMesh::new(Arc::clone(&parent_cache), parent_config)
            .start()
            .await
            .unwrap();
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr)
                .with_peer(parent_addr)
                .with_subscription_lease(Duration::from_secs(5))
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;

        // Stream 8 is live at the edge.
        parent_cache
            .add_for_stream_id(8, 0, Bytes::from(vec![8_u8; 100]))
            .await
            .unwrap();
        edge.subscribe(8, 0, 1).await.unwrap();
        wait_for_slot(&edge_cache, 8, 0).await;
        while applied_rx.try_recv().is_ok() {}

        // Stream 7 has forty parts that the edge has not asked for yet.
        for slot in 0..40 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 1_000]))
                .await
                .unwrap();
        }
        // A live part of stream 8, then a lease on stream 7 with its whole
        // window: the next tick has both.
        parent_cache
            .add_for_stream_id(8, 1, Bytes::from(vec![8_u8; 100]))
            .await
            .unwrap();
        edge.subscribe(7, 0, 40).await.unwrap();

        let mut order = Vec::new();
        while order.len() < 41 {
            let applied = timeout(Duration::from_secs(3), applied_rx.recv())
                .await
                .expect("a part did not arrive")
                .unwrap();
            order.push((applied.stream_id, applied.slot_id));
        }
        assert_eq!(order[0], (8, 1), "{order:?}");
        assert!(order[1..].iter().all(|(stream_id, _)| *stream_id == 7));

        parent.shutdown();
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_parent_copy_of_a_part_is_applied_once() {
        let parent_a_addr = loopback_addr();
        let parent_b_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let cache_a = Arc::new(ChunkCache::new(demand_options()));
        let cache_b = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent_a = CacheMesh::new(
            Arc::clone(&cache_a),
            demand_distributor("parent-a", parent_a_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let parent_b = CacheMesh::new(
            Arc::clone(&cache_b),
            demand_distributor("parent-b", parent_b_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr)
                .with_peers([parent_a_addr, parent_b_addr])
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 2).await;

        let mut parents = edge.subscribe(7, 0, 16).await.unwrap();
        parents.sort_unstable();
        let mut expected = vec![parent_a_addr, parent_b_addr];
        expected.sort_unstable();
        assert_eq!(parents, expected);
        tokio::time::sleep(Duration::from_millis(50)).await;
        for slot in 0..5 {
            let part = Bytes::from(vec![slot as u8; 200]);
            cache_a
                .add_for_stream_id(7, slot, part.clone())
                .await
                .unwrap();
            cache_b.add_for_stream_id(7, slot, part).await.unwrap();
        }
        // The parts that were stored when the lease began come as the
        // warm-up window, after the live parts. The edge renews its lease of
        // 600 ms while it waits, as a node with readers does.
        for slot in (0..5).rev() {
            edge.subscribe(7, 0, 16).await.unwrap();
            wait_for_slot(&edge_cache, 7, slot).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut applied = Vec::new();
        while let Ok(event) = applied_rx.try_recv() {
            applied.push(event.slot_id);
        }
        applied.sort_unstable();
        assert_eq!(applied, vec![0, 1, 2, 3, 4]);

        parent_a.shutdown();
        parent_b.shutdown();
        edge.shutdown();
    }

    #[tokio::test]
    async fn an_edge_knows_how_long_its_parents_have_been_silent() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            demand_distributor("parent", parent_addr)
                .with_peer(edge_addr)
                .with_announce_interval(Duration::from_millis(40)),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            demand_edge("edge", edge_addr).with_peer(parent_addr),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        // The parent says hello every 40 ms: it is never silent for long.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let silent = edge
            .parents_silent_for()
            .await
            .expect("the parent was heard");
        assert!(silent < Duration::from_millis(200), "{silent:?}");
        // The parent is gone: the silence grows.
        parent.shutdown();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let silent = edge
            .parents_silent_for()
            .await
            .expect("the parent was heard");
        assert!(silent >= Duration::from_millis(300), "{silent:?}");
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_edge_does_not_remember_the_slots_that_it_receives() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr).with_peer(parent_addr),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        assert_eq!(edge.subscribe(7, 0, 4).await.unwrap(), vec![parent_addr]);

        // The cache retains 64 slots of a stream. The parent publishes 400.
        for slot_id in 0..400_usize {
            parent_cache
                .add_for_stream_id(7, slot_id, Bytes::from(vec![slot_id as u8; 32]))
                .await
                .unwrap();
            if slot_id % 16 == 15 {
                wait_for_slot(&edge_cache, 7, slot_id).await;
                let _ = edge.subscribe(7, slot_id, 4).await;
            }
        }
        wait_for_slot(&edge_cache, 7, 399).await;

        // An edge sends no slot on, so it has no use for the set of slots
        // that came from a peer. It kept one entry for each part that it
        // ever received.
        assert_eq!(edge.remote_slot_count().await, 0);

        parent.shutdown();
        edge.shutdown();
    }

    #[tokio::test]
    async fn remote_slots_that_the_cache_has_dropped_are_forgotten() {
        let cache = ChunkCache::new(demand_options());
        let remote_slots = RwLock::new(HashSet::new());
        for slot_id in 0..400_usize {
            cache
                .add_for_stream_id(7, slot_id, Bytes::from_static(b"part"))
                .await
                .unwrap();
            remote_slots.write().await.insert((7_u64, slot_id));
        }
        // A stream that the cache does not hold.
        remote_slots.write().await.insert((9_u64, 3_usize));

        forget_unretained_remote_slots(&cache, &remote_slots).await;

        // The cache retains the last 64 slots of the stream.
        let left = remote_slots.read().await;
        assert_eq!(left.len(), 64);
        assert!(left
            .iter()
            .all(|(stream_id, slot_id)| *stream_id == 7 && *slot_id >= 336));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn edge_moves_its_subscription_away_from_a_silent_parent() {
        let parent_a_addr = loopback_addr();
        let parent_b_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let cache_a = Arc::new(ChunkCache::new(demand_options()));
        let cache_b = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent_a = CacheMesh::new(
            Arc::clone(&cache_a),
            demand_distributor("parent-a", parent_a_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let parent_b = CacheMesh::new(
            Arc::clone(&cache_b),
            demand_distributor("parent-b", parent_b_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        // One parent for each playlist, so the choice is visible.
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr)
                .with_peers([parent_a_addr, parent_b_addr])
                .with_max_parents(1)
                .with_peer_timeout(Duration::from_millis(400)),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 2).await;

        let first = edge.subscribe(7, 0, 4).await.unwrap();
        assert_eq!(first.len(), 1);
        let (silent, survivor, survivor_cache) = if first[0] == parent_a_addr {
            (&parent_a, parent_b_addr, &cache_b)
        } else {
            (&parent_b, parent_a_addr, &cache_a)
        };
        silent.shutdown();
        tokio::time::sleep(Duration::from_millis(700)).await;

        // The renewal finds the first parent silent and uses the other one.
        assert_eq!(edge.subscribe(7, 0, 4).await.unwrap(), vec![survivor]);
        survivor_cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"from-survivor"))
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 0).await;

        parent_a.shutdown();
        parent_b.shutdown();
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restarted_stream_is_pushed_from_its_new_beginning() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr).with_peer(parent_addr),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        edge.subscribe(7, 0, 16).await.unwrap();
        for slot in 0..6 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![1_u8; 32]))
                .await
                .unwrap();
        }
        wait_for_slot(&edge_cache, 7, 5).await;

        // The source restarts: the parent drops the stream and its sequence
        // starts again below the part that the child was last sent.
        parent_cache.zero_stream_id(7).await;
        for slot in 0..2 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![2_u8; 32]))
                .await
                .unwrap();
        }
        timeout(Duration::from_secs(3), async {
            loop {
                let restarted = edge_cache
                    .get_for_stream_id(7, 1)
                    .await
                    .is_some_and(|(bytes, _)| bytes == Bytes::from(vec![2_u8; 32]));
                if restarted {
                    return;
                }
                edge.subscribe(7, 0, 16).await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the restarted stream did not reach the child");

        parent.shutdown();
        edge.shutdown();
    }

    #[tokio::test]
    async fn symbol_size_grows_with_the_object_and_decodes() {
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut sender =
            FecSender::with_policy_and_initial_block_id(0, 0.03, 32, 900, 7, Arc::clone(&stats))
                .with_max_symbol_size(16_000);
        // A 5 ms audio part keeps the small symbol.
        assert_eq!(sender.symbol_size_for(1_700), 900);
        assert_eq!(sender.symbol_size_for(900 * 64), 900);
        // A 500 kB video part uses a multiple of it: 64 symbols, not 556.
        assert_eq!(sender.symbol_size_for(500_000), 8_100);
        assert_eq!(source_symbol_count(500_000, 8_100), 62);
        // The largest symbol is a multiple of the base at or below the limit.
        assert_eq!(sender.symbol_size_for(4_000_000), 15_300);
        let fixed = FecSender::new(1, 900);
        assert_eq!(fixed.symbol_size_for(500_000), 900);

        let tx = UdpSocket::bind(loopback_addr()).await.unwrap();
        let rx = UdpSocket::bind(loopback_addr()).await.unwrap();
        // The test sends a whole object before it reads, so the receive
        // buffer must hold it.
        socket2::SockRef::from(&rx)
            .set_recv_buffer_size(4 * 1024 * 1024)
            .unwrap();
        let peer = tx.local_addr().unwrap();
        let mut receiver = FecReceiver::new(Arc::new(CacheMeshFecCounters::default()));
        let mut buf = vec![0_u8; 65_536];
        for frame_len in [1_700_usize, 500_000, 2_000] {
            let frame = (0..frame_len)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>();
            let before = stats.snapshot().tx_source_datagrams;
            sender
                .send(&tx, rx.local_addr().unwrap(), &frame)
                .await
                .unwrap();
            let datagrams = stats.snapshot().tx_source_datagrams - before;
            assert!(
                datagrams <= 64,
                "{frame_len} bytes used {datagrams} datagrams"
            );
            let decoded = timeout(Duration::from_secs(3), async {
                loop {
                    let (len, _) = rx.recv_from(&mut buf).await.unwrap();
                    if let Some(decoded) = receiver.push(peer, &buf[..len]).unwrap() {
                        return decoded;
                    }
                }
            })
            .await
            .expect("object was not decoded");
            assert_eq!(decoded.as_ref(), frame.as_slice());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restarted_child_is_heard_by_its_parent_at_once() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        for slot in 0..4 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 64]))
                .await
                .unwrap();
        }
        // The same child, at the same address, starts three times. Each
        // process subscribes and must receive the playlist.
        for start in 0..3 {
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                demand_edge("edge", edge_addr).with_peer(parent_addr),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            edge.subscribe(7, 0, 16).await.unwrap();
            timeout(Duration::from_secs(3), async {
                while edge_cache.get_for_stream_id(7, 3).await.is_none() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("start {start} of the child received nothing"));
            edge.unsubscribe(7).await.unwrap();
            edge.shutdown();
            drop(edge);
            // The socket of the stopped child must be free for the next one.
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        parent.shutdown();
    }

    #[test]
    fn a_lossy_path_gets_unfragmented_symbols_and_repair_for_its_loss() {
        // The lab configuration: 900-byte symbols, up to 16 kB for a large
        // object, 3% repair and at least one repair symbol.
        let table: LinkLossTable = Arc::new(StdMutex::new(HashMap::new()));
        let sender = FecSender::with_policy_and_initial_block_id(
            1,
            0.03,
            32,
            900,
            0,
            Arc::new(CacheMeshFecCounters::default()),
        )
        .with_max_symbol_size(16_000)
        .with_link_loss(Arc::clone(&table));
        let peer = loopback_addr();
        let part_4k = 400_000;

        // A clean path: large symbols, the configured repair.
        assert_eq!(sender.loss_ppm_to(peer), 0);
        let (symbol, repair) = sender.plan(part_4k, 0);
        assert!(symbol > 6_000, "{symbol}");
        assert_eq!(repair, sender.repair_symbols_for(part_4k));

        // The peer reports 0.5% packet loss. The symbols are now half the
        // size, and the repair covers the loss with room to spare.
        lock(&table).insert(peer, (5_000, std::time::Instant::now()));
        let (clean_symbol, clean_repair) = sender.plan(part_4k, 0);
        let (symbol, repair) = sender.plan(part_4k, sender.loss_ppm_to(peer));
        assert!(symbol < clean_symbol && symbol >= 2_700, "{symbol}");
        let source = u32::from(source_symbol_count(part_4k, symbol));
        assert!((100..=160).contains(&source), "{source}");
        assert!(
            repair > clean_repair && repair <= source / 5,
            "{repair} of {source}"
        );
        // With that repair the object fails less than once in a thousand at
        // the loss of a symbol of this size.
        let fragments = datagram_fragments(usize::from(symbol) + HEADER_LEN + 4) as i32;
        let symbol_loss = 1.0 - (1.0_f64 - 0.0075).powi(fragments);
        assert!(repair >= repair_symbols_for_loss(source, symbol_loss, 0.001, 255));

        // 5% loss needs more, and no more than half of the object.
        let (_, heavy) = sender.plan(part_4k, 50_000);
        assert!(
            heavy > repair && heavy <= source.div_ceil(2),
            "{heavy} of {source}"
        );

        // A 5 ms audio part is two symbols. A few repair symbols make its
        // loss rare at 5%.
        let (symbol, repair) = sender.plan(1_200, 50_000);
        assert_eq!(symbol, 900);
        assert!((2..=5).contains(&repair), "{repair}");

        // A report that is not renewed stops counting.
        lock(&table).insert(peer, (5_000, std::time::Instant::now() - LINK_LOSS_TTL * 2));
        assert_eq!(sender.loss_ppm_to(peer), 0);
    }

    #[test]
    fn repair_for_loss_is_the_binomial_tail() {
        // One symbol at 10% loss: both of two symbols are lost 1% of the
        // time, all three of three 0.1% of the time.
        assert_eq!(repair_symbols_for_loss(1, 0.1, 0.02, 32), 1);
        assert_eq!(repair_symbols_for_loss(1, 0.1, 0.002, 32), 2);
        assert_eq!(repair_symbols_for_loss(100, 0.0, 0.001, 32), 0);
        // 300 symbols at 1%: three are lost on average, eleven almost never.
        let repair = repair_symbols_for_loss(300, 0.01, 0.001, 255);
        assert!((9..=12).contains(&repair), "{repair}");
        // The limit holds.
        assert_eq!(repair_symbols_for_loss(300, 0.5, 0.001, 40), 40);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_receiver_measures_loss_and_its_parent_hears_it() {
        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(large_part_options()));
        let edge_cache = ChunkCache::new(large_part_options());
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            demand_distributor("dist", distributor_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        // The edge drops 3% of what it receives.
        let edge = CacheMesh::new(
            Arc::new(edge_cache),
            demand_edge("edge", edge_addr)
                .with_peer(distributor_addr)
                .with_receive_loss_ppm(30_000),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        edge.subscribe(7, 0, 8).await.unwrap();
        // Two seconds of 6 kB parts every 20 ms. The lease lasts 600 ms.
        for slot in 0..120_usize {
            if slot % 10 == 0 {
                edge.subscribe(7, 0, 8).await.unwrap();
            }
            distributor_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 6_000]))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let reported = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let loss = distributor.link_loss();
                if let Some(ppm) = loss
                    .iter()
                    .find(|loss| loss.peer == edge.local_addr())
                    .and_then(|loss| loss.send_ppm)
                    .filter(|ppm| *ppm > 0)
                {
                    break ppm;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the distributor hears the loss of its child");
        assert!((8_000..90_000).contains(&reported), "{reported} ppm");
        let measured = edge
            .link_loss()
            .into_iter()
            .find(|loss| loss.peer == distributor.local_addr())
            .and_then(|loss| loss.receive_ppm)
            .unwrap();
        assert!((8_000..90_000).contains(&measured), "{measured} ppm");
        distributor.shutdown();
        edge.shutdown();
    }

    #[tokio::test]
    async fn a_request_for_named_parts_returns_those_parts_only() {
        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        for slot in 0..10_usize {
            distributor_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 600]))
                .await
                .unwrap();
        }
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            demand_distributor("dist", distributor_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr).with_peer(distributor_addr),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        // The edge accepts parts of the playlist, and asks for parts 4 and 5.
        lock(&edge.wanted).insert(7);
        let before = distributor.fec_stats().tx_objects;
        assert_eq!(edge.request_parts(7, 4, 2, 1).await.unwrap(), 1);
        wait_for_slot(&edge_cache, 7, 5).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stream = edge_cache.get_stream_idx(7).await.unwrap();
        assert!(edge_cache.get(stream, 4).await.is_some());
        assert!(edge_cache.get(stream, 5).await.is_some());
        for slot in [3_usize, 6, 9] {
            assert!(edge_cache.get(stream, slot).await.is_none(), "slot {slot}");
        }
        // Two parts went out, beside a few hellos: not the six parts from
        // the hole to the newest part.
        let sent = distributor.fec_stats().tx_objects - before;
        assert!((2..=7).contains(&sent), "{sent} objects");
        distributor.shutdown();
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_children_of_a_node_share_one_encoding_of_a_part() {
        let distributor_addr = loopback_addr();
        let first_addr = loopback_addr();
        let second_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(large_part_options()));
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            demand_distributor("dist", distributor_addr)
                .with_peer(first_addr)
                .with_peer(second_addr),
        )
        .start()
        .await
        .unwrap();
        let mut edges = Vec::new();
        for (name, addr) in [("edge-a", first_addr), ("edge-b", second_addr)] {
            let cache = Arc::new(ChunkCache::new(large_part_options()));
            let edge = CacheMesh::new(
                Arc::clone(&cache),
                demand_edge(name, addr).with_peer(distributor_addr),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            edge.subscribe(7, 0, 0).await.unwrap();
            edges.push((edge, cache));
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        let before = distributor.fec_stats();
        for slot in 0..40_usize {
            if slot % 10 == 0 {
                // The lease lasts 600 ms.
                for (edge, _) in &edges {
                    edge.subscribe(7, 0, 0).await.unwrap();
                }
            }
            distributor_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 8_000]))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for (_, cache) in &edges {
            wait_for_slot(cache, 7, 39).await;
        }
        let after = distributor.fec_stats();
        // Forty parts were encoded once each, beside the hellos. Each of the
        // two children received every one of them.
        let encoded = after.tx_objects - before.tx_objects;
        assert!((40..=72).contains(&encoded), "{encoded} objects encoded");
        let source = after.tx_source_datagrams - before.tx_source_datagrams;
        assert!(source >= 2 * 40 * 6, "{source} source datagrams");
        distributor.shutdown();
        for (edge, _) in edges {
            edge.shutdown();
        }
    }

    #[test]
    fn parent_ranking_spreads_children_and_is_stable() {
        let distributors = (0..4)
            .map(|index| SocketAddr::from(([10, 0, 0, 10 + index], 9101)))
            .collect::<Vec<_>>();
        let mut first_choice: HashMap<SocketAddr, usize> = HashMap::new();
        for child in 0..200 {
            let node_id = format!("edge-{child}");
            let mut ranked = distributors.clone();
            rank_parents(&node_id, 7, &mut ranked);
            *first_choice.entry(ranked[0]).or_default() += 1;

            // The order does not depend on the order in which peers are known.
            let mut reversed = distributors.iter().rev().copied().collect::<Vec<_>>();
            rank_parents(&node_id, 7, &mut reversed);
            assert_eq!(ranked, reversed);

            // Removing a distributor that is not the first choice keeps it.
            let mut fewer = distributors
                .iter()
                .copied()
                .filter(|peer| *peer != ranked[3])
                .collect::<Vec<_>>();
            rank_parents(&node_id, 7, &mut fewer);
            assert_eq!(fewer[0], ranked[0]);
        }
        assert_eq!(first_choice.len(), 4);
        for (distributor, children) in first_choice {
            assert!(
                (25..=75).contains(&children),
                "{distributor} is the first parent of {children} of 200 children"
            );
        }
    }

    #[test]
    fn fec_receiver_keeps_the_arrival_of_the_first_datagram_of_a_block() {
        let payload = b"mesh-frame-with-several-symbols";
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(1);
        let datagrams = encoder.encode_object(payload).unwrap();
        assert!(datagrams.len() > 2);
        let mut receiver = FecReceiver::new(Arc::new(CacheMeshFecCounters::default()));
        let peer: SocketAddr = "127.0.0.1:9914".parse().unwrap();
        let mut decoded = None;
        for (index, datagram) in datagrams.iter().enumerate() {
            let received_us = 1_000_000 + 100 * index as u64;
            if let Some(block) = receiver.push_at(peer, datagram, received_us).unwrap() {
                decoded = Some((block, received_us));
                break;
            }
        }
        let (block, last_us) = decoded.expect("the block decodes");
        assert_eq!(block.frame.as_ref(), payload.as_slice());
        assert_eq!(block.first_seen_us, 1_000_000);
        assert!(last_us > block.first_seen_us);

        // A block of one datagram arrived when that datagram did.
        let datagrams = encoder.encode_object(b"tiny").unwrap();
        let block = receiver
            .push_at(peer, &datagrams[0], 7_000_000)
            .unwrap()
            .expect("one source symbol decodes");
        assert_eq!(block.first_seen_us, 7_000_000);
    }

    #[test]
    fn sync_on_publish_is_on_by_default_and_can_be_turned_off() {
        let addr: SocketAddr = "127.0.0.1:9915".parse().unwrap();
        let config = CacheMeshConfig::new("node", "uk", addr);
        assert!(config.sync_on_publish);
        assert!(config.send_observer.0.is_none());
        let config = config
            .with_sync_on_publish(false)
            .with_sync_interval(Duration::from_millis(50))
            .with_send_observer(Arc::new(|_, _, _, _| {}));
        assert!(!config.sync_on_publish);
        assert_eq!(config.sync_interval, Duration::from_millis(50));
        assert!(config.send_observer.0.is_some());
        // The configuration stays printable with an observer in it.
        assert!(format!("{config:?}").contains("send_observer: Some(..)"));
    }

    type SentRecord = (u64, u64, SocketAddr, u64);

    /// Start a distributor and an edge with a 50 ms sync interval, then
    /// commit parts at the distributor one at a time, each as soon as the edge
    /// has applied the one before. Returns how long each part took from its
    /// commit to its application, the applied slots, and what the send
    /// observer saw.
    async fn commit_to_apply(
        sync_on_publish: bool,
        batched_send: bool,
        parts: usize,
    ) -> (
        Vec<Duration>,
        Vec<AppliedSlot>,
        Arc<StdMutex<Vec<SentRecord>>>,
        SocketAddr,
        SocketAddr,
    ) {
        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let sent: Arc<StdMutex<Vec<SentRecord>>> = Arc::default();
        let observer = {
            let sent = Arc::clone(&sent);
            Arc::new(move |stream_id, slot_id, child, sent_us| {
                lock(&sent).push((stream_id, slot_id, child, sent_us));
            })
        };
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            CacheMeshConfig::new("distributor", "uk", distributor_addr)
                .with_peer(edge_addr)
                .with_same_region_only(true)
                .with_role(CacheMeshRole::Distributor)
                .with_announce_interval(Duration::from_millis(100))
                .with_sync_interval(Duration::from_millis(50))
                .with_sync_on_publish(sync_on_publish)
                .with_batched_send(batched_send)
                .with_send_observer(observer),
        )
        .start()
        .await
        .unwrap();
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_same_region_only(true)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(100))
                .with_sync_interval(Duration::from_millis(50))
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;

        let mut latencies = Vec::new();
        let mut applied = Vec::new();
        for slot in 0..=parts {
            let committed = Instant::now();
            distributor_cache
                .put_if_absent_contiguous_for_stream_id(
                    7,
                    slot,
                    0,
                    Bytes::from(vec![slot as u8; 300]),
                )
                .await
                .unwrap();
            let event = timeout(Duration::from_secs(2), applied_rx.recv())
                .await
                .expect("the part did not reach the edge")
                .unwrap();
            let elapsed = committed.elapsed();
            assert_eq!((event.stream_id, event.slot_id), (7, slot));
            // The first part only lines the test up with the edge.
            if slot > 0 {
                latencies.push(elapsed);
                applied.push(event);
            }
        }
        // The observer runs after the last datagram of a part is sent, which
        // can be after the edge applied it.
        timeout(Duration::from_secs(1), async {
            while lock(&sent).len() < parts + 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the send observer did not see every part");
        distributor.shutdown();
        edge.shutdown();
        (latencies, applied, sent, distributor_addr, edge_addr)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_committed_part_leaves_at_once_when_sync_on_publish() {
        let (mut latencies, applied, sent, distributor_addr, edge_addr) =
            commit_to_apply(true, true, 19).await;
        latencies.sort_unstable();
        let median = latencies[latencies.len() / 2];
        assert!(
            median < Duration::from_millis(5),
            "commit to edge took {latencies:?} with a 50 ms sync interval"
        );
        // Every part, not only most: a part whose publication did not wake
        // the sender waits for the tick, which is up to 50 ms away and more
        // than 10 ms away four times in five. Over 19 parts a lost wake-up
        // that hits even one part in ten fails here almost surely. The bound
        // is 10 ms rather than 5 so that one part delayed by the scheduler of
        // a busy test host does not fail the test.
        assert!(
            latencies
                .iter()
                .all(|latency| *latency < Duration::from_millis(10)),
            "a part waited for the tick: {latencies:?}"
        );

        // The mesh reports when each hop happened, on one clock.
        let sent = lock(&sent).clone();
        for event in &applied {
            assert_eq!(event.parent, distributor_addr);
            let &(_, _, child, sent_us) = sent
                .iter()
                .find(|(stream_id, slot_id, _, _)| {
                    *stream_id == event.stream_id && *slot_id == event.slot_id as u64
                })
                .expect("the send observer saw the part");
            assert_eq!(child, edge_addr);
            assert!(sent_us > 0);
            assert!(
                sent_us <= event.rx_first_us
                    && event.rx_first_us <= event.decoded_us
                    && event.decoded_us <= event.committed_us,
                "hop times out of order: sent {sent_us} {event:?}"
            );
            assert!(event.committed_us - sent_us < 1_000_000);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_committed_part_waits_for_the_tick_without_sync_on_publish() {
        let (latencies, applied, _sent, distributor_addr, _edge_addr) =
            commit_to_apply(false, true, 3).await;
        // Each part is committed just after a tick delivered the one before,
        // so it waits most of the next 50 ms interval.
        assert!(
            latencies
                .iter()
                .all(|latency| *latency >= Duration::from_millis(20)),
            "a part left before the tick: {latencies:?}"
        );
        assert!(applied.iter().all(|event| event.parent == distributor_addr));
    }
    #[test]
    fn seen_watermarks_tell_a_pass_which_streams_moved() {
        let mut seen = SeenWatermarks::default();
        assert_eq!(seen.record(7, 0, Some(3)), None);
        assert_eq!(seen.record(7, 0, Some(3)), Some(3), "unchanged");
        assert_eq!(seen.record(7, 0, Some(5)), Some(3), "advanced");
        assert_eq!(seen.record(9, 1, None), None);
        // The same slot index holding another stream is another stream.
        assert_eq!(seen.record(8, 0, Some(1)), None);
        seen.retain_streams(&[(8, 0)]);
        assert_eq!(seen.record(7, 0, Some(5)), None, "forgotten");
    }

    #[test]
    fn same_bytes_compares_clones_without_reading_them() {
        let initialization = Bytes::from(vec![1u8; 4096]);
        assert!(same_bytes(&initialization, &initialization.clone()));
        assert!(same_bytes(&initialization, &Bytes::from(vec![1u8; 4096])));
        assert!(!same_bytes(&initialization, &Bytes::from(vec![2u8; 4096])));
        assert!(!same_bytes(&initialization, &initialization.slice(..4095)));
    }

    /// Records, at the moment a publication wakes it, whether the slot was
    /// already marked as remote. The check runs inside the wake-up, so it
    /// sees exactly what the sync task would see if it ran at once.
    struct RemoteAtWake {
        remote_slots: Arc<RwLock<HashSet<(u64, usize)>>>,
        slot: (u64, usize),
        // 0: not woken, 1: marked, 2: not marked, 3: set locked.
        seen: std::sync::atomic::AtomicU8,
    }

    impl std::task::Wake for RemoteAtWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let seen = match self.remote_slots.try_read() {
                Ok(remote_slots) if remote_slots.contains(&self.slot) => 1,
                Ok(_) => 2,
                // The receive task holds the set until the write is done, so
                // the sync task waits for it and then sees the slot.
                Err(_) => 3,
            };
            let _ = self.seen.compare_exchange(
                0,
                seen,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_received_part_is_marked_remote_before_its_publication_wakes_the_sender() {
        let sender_addr = loopback_addr();
        let receiver_addr = loopback_addr();
        let sender_cache = Arc::new(ChunkCache::new(demand_options()));
        let receiver_cache = Arc::new(ChunkCache::new(demand_options()));
        let sender = CacheMesh::new(
            Arc::clone(&sender_cache),
            CacheMeshConfig::new("sender", "uk", sender_addr)
                .with_peer(receiver_addr)
                .with_announce_interval(Duration::from_millis(100))
                .with_sync_interval(Duration::from_millis(5)),
        )
        .start()
        .await
        .unwrap();
        // A peer, not an edge: it has a copy loop that must not send the
        // part back. Its interval is long so that no tick prunes the set.
        let receiver = CacheMesh::new(
            Arc::clone(&receiver_cache),
            CacheMeshConfig::new("receiver", "uk", receiver_addr)
                .with_peer(sender_addr)
                .with_announce_interval(Duration::from_millis(100))
                .with_sync_interval(Duration::from_secs(60)),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&receiver, 1).await;

        let probe = Arc::new(RemoteAtWake {
            remote_slots: Arc::clone(&receiver.remote_slots),
            slot: (7, 0),
            seen: std::sync::atomic::AtomicU8::new(0),
        });
        use std::future::Future;
        let waker = std::task::Waker::from(Arc::clone(&probe));
        let notified = receiver_cache.publication_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        assert!(notified
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending());

        sender_cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"part-0"))
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            while probe.seen.load(std::sync::atomic::Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the part was not published at the receiver");
        assert_ne!(
            probe.seen.load(std::sync::atomic::Ordering::Acquire),
            2,
            "the publication woke the sender before the slot was marked remote"
        );
        assert!(receiver.remote_slots.read().await.contains(&(7, 0)));
        sender.shutdown();
        receiver.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_send_observer_sees_live_parts_and_not_a_new_childs_catch_up() {
        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let sent: Arc<StdMutex<Vec<SentRecord>>> = Arc::default();
        let observer = {
            let sent = Arc::clone(&sent);
            Arc::new(move |stream_id, slot_id, child, sent_us| {
                lock(&sent).push((stream_id, slot_id, child, sent_us));
            })
        };
        // The distributor has no child yet: it learns of the edge from its
        // hello, after these parts are published.
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            CacheMeshConfig::new("distributor", "uk", distributor_addr)
                .with_role(CacheMeshRole::Distributor)
                .with_announce_interval(Duration::from_millis(20))
                .with_sync_interval(Duration::from_millis(10))
                .with_send_observer(observer),
        )
        .start()
        .await
        .unwrap();
        for slot in 0..10 {
            distributor_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 100]))
                .await
                .unwrap();
        }
        // Let a tick see the parts while there is no child.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(20))
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        // Full replication: the new child is sent the whole window. The edge
        // takes parts from its configured parent before it hears its hello.
        let mut caught_up = HashSet::new();
        timeout(Duration::from_secs(3), async {
            while caught_up.len() < 10 {
                let event: AppliedSlot = applied_rx.recv().await.unwrap();
                caught_up.insert(event.slot_id);
            }
        })
        .await
        .expect("the new child did not catch up");
        assert!(
            lock(&sent).is_empty(),
            "catch-up sends were reported as live: {:?}",
            lock(&sent)
        );

        distributor_cache
            .add_for_stream_id(7, 10, Bytes::from(vec![10u8; 100]))
            .await
            .unwrap();
        let event = timeout(Duration::from_secs(2), applied_rx.recv())
            .await
            .expect("the live part did not reach the edge")
            .unwrap();
        assert_eq!(event.slot_id, 10);
        timeout(Duration::from_secs(1), async {
            while lock(&sent).is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the live part was not reported");
        let sent = lock(&sent).clone();
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert_eq!((sent[0].0, sent[0].1, sent[0].2), (7, 10, edge_addr));
        distributor.shutdown();
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_committed_part_leaves_at_once_without_batched_send() {
        let (mut latencies, applied, sent, distributor_addr, edge_addr) =
            commit_to_apply(true, false, 9).await;
        latencies.sort_unstable();
        assert!(
            latencies[latencies.len() / 2] < Duration::from_millis(5),
            "{latencies:?}"
        );
        let sent = lock(&sent).clone();
        for event in &applied {
            assert_eq!(event.parent, distributor_addr);
            let &(_, _, child, sent_us) = sent
                .iter()
                .find(|(_, slot_id, _, _)| *slot_id == event.slot_id as u64)
                .expect("the send observer saw the part");
            assert_eq!(child, edge_addr);
            assert!(sent_us <= event.rx_first_us);
        }
    }

    #[test]
    fn batched_send_and_gso_are_on_by_default_and_can_be_turned_off() {
        let config = CacheMeshConfig::new("node", "uk", "127.0.0.1:9916".parse().unwrap());
        assert!(config.batched_send);
        assert!(config.gso);
        let config = config.with_batched_send(false).with_gso(false);
        assert!(!config.batched_send);
        assert!(!config.gso);
    }

    fn percentile(sorted: &[u64], fraction: f64) -> u64 {
        let index = ((sorted.len() as f64 * fraction).ceil() as usize).clamp(1, sorted.len()) - 1;
        sorted[index]
    }

    /// CPU time of the calling thread in microseconds.
    fn thread_cpu_us() -> u64 {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `time` is a valid timespec for the call to fill.
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
        time.tv_sec as u64 * 1_000_000 + time.tv_nsec as u64 / 1_000
    }

    /// What a test child records: when the first datagram of each part
    /// arrived, by slot.
    type Arrivals = Arc<StdMutex<HashMap<usize, u64>>>;

    /// A child that is only a socket on its own thread: it reads each
    /// datagram as it arrives and notes when the first source symbol of each
    /// part came. It measures the sender alone, with no receive path of its
    /// own to fall behind.
    fn raw_child(
        addr: SocketAddr,
        stop: Arc<std::sync::atomic::AtomicBool>,
    ) -> (Arrivals, std::thread::JoinHandle<()>) {
        let arrivals: Arrivals = Arc::default();
        let socket = std::net::UdpSocket::bind(addr).unwrap();
        let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(4 << 20);
        socket
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        let thread = {
            let arrivals = Arc::clone(&arrivals);
            std::thread::spawn(move || {
                let mut buf = vec![0_u8; 65_536];
                while !stop.load(Ordering::Relaxed) {
                    let Ok((len, _)) = socket.recv_from(&mut buf) else {
                        continue;
                    };
                    let at = unix_micros();
                    let datagram = &buf[..len];
                    // The first source symbol of a block holds the head of
                    // the chunk frame.
                    if len < HEADER_LEN + 4
                        || DatagramFecHeader::decode(datagram).is_err()
                        || datagram[HEADER_LEN + 1..HEADER_LEN + 4] != [0, 0, 0]
                    {
                        continue;
                    }
                    if let Some((_, slot_id)) = chunk_frame_slot(&datagram[HEADER_LEN + 4..]) {
                        lock(&arrivals).entry(slot_id as usize).or_insert(at);
                    }
                }
            })
        };
        (arrivals, thread)
    }

    /// The children of a throughput run.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Children {
        /// Two cache-mesh edges on a runtime of four threads; the time is the
        /// edge's `rx_first_us`.
        MeshEdges,
        /// Two sockets that note the arrival of each part (see [`raw_child`]).
        Sockets,
    }

    /// A distributor alone on one core (a one-thread runtime on its own OS
    /// thread) sends to two children, while a publisher thread commits
    /// `rate` parts of `part_bytes` a second for `seconds` at its cache, each
    /// at its own deadline. Returns, sorted, the microseconds from each
    /// part's commit to the arrival of its first datagram at each child, and
    /// from the commit to the send observer's time; and the parts that a
    /// child never received.
    fn publish_to_two_children(
        children: Children,
        batched_send: bool,
        rate: u64,
        seconds: u64,
        part_bytes: usize,
    ) -> (Vec<u64>, Vec<u64>, usize) {
        // One run at a time: they measure time, and each takes cores.
        static RUNS: StdMutex<()> = StdMutex::new(());
        let _run = lock(&RUNS);
        let parts = (rate * seconds) as usize;
        let sends: Arc<StdMutex<Vec<(u64, u64)>>> = Arc::default();
        let options = Options {
            num_playlists: 2,
            max_segments: 1,
            max_parts_per_segment: parts + 64,
            ..Options::default()
        };
        let distributor_addr = loopback_addr();
        let child_addrs = [loopback_addr(), loopback_addr()];
        let distributor_cache = Arc::new(ChunkCache::new(options));

        // The children: each one's arrivals by slot.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut arrivals: Vec<Arrivals> = Vec::new();
        let mut threads = Vec::new();
        let mut edges = Vec::new();
        for (index, addr) in child_addrs.into_iter().enumerate() {
            if children == Children::Sockets {
                let (child_arrivals, thread) = raw_child(addr, Arc::clone(&stop));
                arrivals.push(child_arrivals);
                threads.push(thread);
                continue;
            }
            let child_arrivals: Arrivals = Arc::default();
            arrivals.push(Arc::clone(&child_arrivals));
            let (applied_tx, mut applied_rx) = mpsc::unbounded_channel::<AppliedSlot>();
            runtime.spawn(async move {
                while let Some(event) = applied_rx.recv().await {
                    lock(&child_arrivals)
                        .entry(event.slot_id)
                        .or_insert(event.rx_first_us);
                }
            });
            edges.push(runtime.block_on(async {
                CacheMesh::new(
                    Arc::new(ChunkCache::new(options)),
                    CacheMeshConfig::new(format!("edge-{index}"), "uk", addr)
                        .with_peer(distributor_addr)
                        .with_role(CacheMeshRole::Edge)
                        .with_socket_buffer_bytes(4 << 20)
                        .with_announce_interval(Duration::from_millis(100))
                        .with_applied_tx(applied_tx),
                )
                .start()
                .await
                .unwrap()
            }));
        }

        // The distributor.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let distributor = {
            let cache = Arc::clone(&distributor_cache);
            let sends = Arc::clone(&sends);
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let cpu_start = thread_cpu_us();
                runtime.block_on(async move {
                    let mesh = CacheMesh::new(
                        cache,
                        CacheMeshConfig::new("distributor", "uk", distributor_addr)
                            .with_peers(child_addrs)
                            .with_role(CacheMeshRole::Distributor)
                            .with_socket_buffer_bytes(4 << 20)
                            .with_announce_interval(Duration::from_millis(100))
                            .with_batched_send(batched_send)
                            .with_send_observer(Arc::new(move |_, slot_id, _, sent_us| {
                                lock(&sends).push((slot_id, sent_us));
                            })),
                    )
                    .start()
                    .await
                    .unwrap();
                    ready_tx.send(()).unwrap();
                    let _ = stop_rx.await;
                    mesh.shutdown();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                });
                thread_cpu_us() - cpu_start
            })
        };
        ready_rx.recv().unwrap();
        runtime.block_on(async {
            for edge in &edges {
                wait_for_hello(edge, 1).await;
            }
            // Slot 0 lines the run up: both children receive parts.
            distributor_cache
                .add_for_stream_id(7, 0, Bytes::from(vec![0; part_bytes]))
                .await
                .unwrap();
            timeout(Duration::from_secs(3), async {
                while !arrivals
                    .iter()
                    .all(|arrivals| lock(arrivals).contains_key(&0))
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the children did not receive the first part");
        });

        // The publisher.
        let publisher = {
            let cache = Arc::clone(&distributor_cache);
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                let period = Duration::from_nanos(1_000_000_000 / rate);
                let payload = Bytes::from(vec![7_u8; part_bytes]);
                let mut published = vec![0_u64; parts + 1];
                let start = std::time::Instant::now();
                for slot in 1..=parts {
                    let deadline = start + period * slot as u32;
                    let now = std::time::Instant::now();
                    if deadline > now + Duration::from_micros(200) {
                        std::thread::sleep(deadline - now - Duration::from_micros(200));
                    }
                    while std::time::Instant::now() < deadline {
                        std::hint::spin_loop();
                    }
                    published[slot] = unix_micros();
                    runtime
                        .block_on(cache.add_for_stream_id(7, slot, payload.clone()))
                        .unwrap();
                }
                published
            })
        };
        let published = publisher.join().unwrap();

        // Wait for the last part, or for the stragglers to be lost.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::time::Instant::now() < deadline
            && !arrivals.iter().all(|arrivals| lock(arrivals).len() > parts)
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        stop.store(true, Ordering::Relaxed);
        let _ = stop_tx.send(());
        let cpu_us = distributor.join().unwrap();
        eprintln!(
            "distributor thread CPU {:.1} us per part ({} parts, batched {batched_send})",
            cpu_us as f64 / parts as f64,
            parts
        );
        for thread in threads {
            thread.join().unwrap();
        }
        runtime.block_on(async {
            for edge in &edges {
                edge.shutdown();
            }
        });

        let mut latencies = Vec::with_capacity(2 * parts);
        let mut missing = 0;
        for arrivals in &arrivals {
            let arrivals = lock(arrivals);
            for slot in 1..=parts {
                match arrivals.get(&slot) {
                    Some(at) => latencies.push(at.saturating_sub(published[slot])),
                    None => missing += 1,
                }
            }
        }
        latencies.sort_unstable();
        let mut sent = lock(&sends)
            .iter()
            .filter(|(slot_id, _)| (1..=parts as u64).contains(slot_id))
            .map(|(slot_id, sent_us)| sent_us.saturating_sub(published[*slot_id as usize]))
            .collect::<Vec<_>>();
        sent.sort_unstable();
        (latencies, sent, missing)
    }

    fn report(label: &str, latencies: &[u64], sent: &[u64], missing: usize) -> (u64, u64) {
        let p50 = percentile(latencies, 0.5);
        let p99 = percentile(latencies, 0.99);
        eprintln!(
            "{label}: publish->child first datagram p50 {p50} us, p99 {p99} us, max {} us, missing {missing}; publish->sent p50 {} us, p99 {} us",
            latencies.last().unwrap(),
            percentile(sent, 0.5),
            percentile(sent, 0.99)
        );
        (p50, p99)
    }

    /// 4,000 parts a second (0.25 ms PCM parts) for two seconds, from a
    /// distributor on one core to two children over loopback: every part
    /// arrives, and 99 in 100 within 2 ms of the commit. The children are
    /// sockets, so that the test measures the sender in any build.
    #[test]
    fn a_distributor_on_one_core_sends_four_thousand_parts_a_second() {
        let (latencies, sent, missing) =
            publish_to_two_children(Children::Sockets, true, 4_000, 2, 72);
        let (_, p99) = report("batched, socket children", &latencies, &sent, missing);
        assert_eq!(missing, 0, "parts did not arrive");
        assert!(p99 < 2_000, "p99 {p99} us");
    }

    /// The same with two cache-mesh edges: the edge's `rx_first_us` within
    /// 2 ms of the commit for 99 parts in 100. The receive path of an
    /// unoptimised build cannot take 8,000 datagrams a second, so this runs
    /// in an optimised build (`cargo test --release`).
    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "needs an optimised build: cargo test --release"
    )]
    fn a_distributor_on_one_core_keeps_two_edges_at_four_thousand_parts_a_second() {
        let (latencies, sent, missing) =
            publish_to_two_children(Children::MeshEdges, true, 4_000, 2, 72);
        let (_, p99) = report("batched, mesh edges", &latencies, &sent, missing);
        assert_eq!(missing, 0, "parts did not arrive");
        assert!(p99 < 2_000, "p99 {p99} us");
    }

    /// Both runs with one awaited send per datagram, for comparison. They are
    /// reported, not judged: it is the behaviour that batching replaces.
    #[test]
    #[ignore = "comparison run: cargo test --release -- --ignored unbatched"]
    fn unbatched_four_thousand_parts_a_second_for_comparison() {
        let (latencies, sent, missing) =
            publish_to_two_children(Children::Sockets, false, 4_000, 2, 72);
        report("unbatched, socket children", &latencies, &sent, missing);
        let (latencies, sent, missing) =
            publish_to_two_children(Children::MeshEdges, false, 4_000, 2, 72);
        report("unbatched, mesh edges", &latencies, &sent, missing);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_gso_a_large_part_keeps_symbols_that_fit_a_packet() {
        for gso in [true, false] {
            let distributor_addr = loopback_addr();
            let edge_addr = loopback_addr();
            let distributor_cache = Arc::new(ChunkCache::new(large_part_options()));
            let distributor = CacheMesh::new(
                Arc::clone(&distributor_cache),
                CacheMeshConfig::new("distributor", "uk", distributor_addr)
                    .with_peer(edge_addr)
                    .with_role(CacheMeshRole::Distributor)
                    .with_max_symbol_size(8_100)
                    .with_announce_interval(Duration::from_millis(100))
                    .with_gso(gso),
            )
            .start()
            .await
            .unwrap();
            let edge_cache = Arc::new(ChunkCache::new(large_part_options()));
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                CacheMeshConfig::new("edge", "uk", edge_addr)
                    .with_peer(distributor_addr)
                    .with_role(CacheMeshRole::Edge)
                    .with_socket_buffer_bytes(4 << 20)
                    .with_announce_interval(Duration::from_millis(100)),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            let gso_active = BatchSender::new(&distributor.socket, gso).gso();
            let before = distributor.fec_stats();
            let part = Bytes::from((0..200_000).map(|index| index as u8).collect::<Vec<_>>());
            distributor_cache
                .add_for_stream_id(7, 0, part.clone())
                .await
                .unwrap();
            wait_for_slot(&edge_cache, 7, 0).await;
            assert_eq!(edge_cache.get_for_stream_id(7, 0).await.unwrap().0, part);
            let after = distributor.fec_stats();
            let source = after.tx_source_datagrams - before.tx_source_datagrams;
            let bytes = after.tx_wire_bytes - before.tx_wire_bytes;
            let datagram = bytes
                / (after.tx_source_datagrams + after.tx_repair_datagrams
                    - before.tx_source_datagrams
                    - before.tx_repair_datagrams);
            if gso_active {
                // 1316-byte symbols: no datagram is fragmented.
                assert!(source >= 152, "{source} source datagrams");
                assert_eq!(datagram, 1_352);
            } else {
                // Symbols grow so that the part is about 64 of them.
                assert!(source <= 64, "{source} source datagrams");
                assert!(datagram > 1_500);
            }
            assert!(gso_active || !gso || !cfg!(target_os = "linux"));
            distributor.shutdown();
            edge.shutdown();
        }
    }

    #[test]
    fn a_small_object_is_one_symbol_of_its_own_length() {
        let stats = Arc::new(CacheMeshFecCounters::default());
        let fitted = FecSender::with_policy_and_initial_block_id(
            1,
            0.03,
            32,
            DEFAULT_SYMBOL_SIZE,
            0,
            Arc::clone(&stats),
        )
        .with_fitted_symbols(true);
        assert_eq!(fitted.symbol_size_for(120), 120);
        assert_eq!(fitted.symbol_size_for(0), 1);
        assert_eq!(fitted.symbol_size_for(1_200), 1_200);
        // Above the cut-through client symbol an object is padded, so that
        // the edge can forward the symbol as two client symbols of 658.
        for frame_len in [1_201, 1_211, 1_213, 1_217, 1_315] {
            assert_eq!(fitted.symbol_size_for(frame_len), 1_316, "{frame_len}");
            assert_eq!(fitted.plan(frame_len, 50_000).0, 1_316, "{frame_len}");
        }
        assert_eq!(fitted.symbol_size_for(1_316), 1_316);
        assert_eq!(fitted.symbol_size_for(5_000), 1_316);
        // A lossy path plans the same way.
        assert_eq!(fitted.plan(120, 50_000).0, 120);
        let padded =
            FecSender::with_policy_and_initial_block_id(1, 0.03, 32, DEFAULT_SYMBOL_SIZE, 0, stats);
        assert_eq!(padded.symbol_size_for(120), 1_316);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_audio_part_goes_in_datagrams_of_its_own_size() {
        for fitted in [true, false] {
            let distributor_addr = loopback_addr();
            let edge_addr = loopback_addr();
            let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
            let distributor = CacheMesh::new(
                Arc::clone(&distributor_cache),
                CacheMeshConfig::new("distributor", "uk", distributor_addr)
                    .with_peer(edge_addr)
                    .with_role(CacheMeshRole::Distributor)
                    .with_announce_interval(Duration::from_millis(100))
                    .with_fitted_symbols(fitted),
            )
            .start()
            .await
            .unwrap();
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                CacheMeshConfig::new("edge", "uk", edge_addr)
                    .with_peer(distributor_addr)
                    .with_role(CacheMeshRole::Edge)
                    .with_announce_interval(Duration::from_millis(100)),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            let before = distributor.fec_stats();
            // A 2.5 ms Opus part of 40 bytes.
            let part = Bytes::from(vec![9_u8; 40]);
            distributor_cache
                .add_for_stream_id(7, 0, part.clone())
                .await
                .unwrap();
            wait_for_slot(&edge_cache, 7, 0).await;
            assert_eq!(edge_cache.get_for_stream_id(7, 0).await.unwrap().0, part);
            let after = distributor.fec_stats();
            let datagrams = after.tx_source_datagrams + after.tx_repair_datagrams
                - before.tx_source_datagrams
                - before.tx_repair_datagrams;
            let bytes = (after.tx_wire_bytes - before.tx_wire_bytes) / datagrams;
            // The chunk frame: 12 bytes of head, the node id, 20 bytes of ids
            // and length, and the part; then the 36 bytes of FEC header.
            let frame = 12 + "distributor".len() + 20 + part.len();
            if fitted {
                assert_eq!(bytes as usize, 36 + frame);
            } else {
                assert_eq!(bytes, 1_352);
            }
            distributor.shutdown();
            edge.shutdown();
        }
    }
}
