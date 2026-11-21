use crate::chunk_cache::ChunkCache;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use raptorq_datagram_fec::{
    source_symbol_count, DatagramFecDecoder, DatagramFecEncoder, DatagramFecError,
    DatagramFecHeader, HEADER_LEN,
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
const ENCODED_OBJECT_CACHE: usize = 24;
const ENCODED_OBJECT_CACHE_BYTES: usize = 6 * 1024 * 1024;
const DEFAULT_SYMBOL_SIZE: u16 = 1316;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedSlot {
    pub stream_id: u64,
    pub slot_id: usize,
    pub bytes: usize,
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
    encoder: DatagramFecEncoder,
    min_repair_symbols: u32,
    repair_ratio: f32,
    max_repair_symbols: u32,
    symbol_size: u16,
    max_symbol_size: u16,
    stats: Arc<CacheMeshFecCounters>,
    /// Loss that the peers report on the paths from this node.
    link_loss: Option<LinkLossTable>,
    /// The newest encoded objects, by object key and symbol size.
    encoded: VecDeque<EncodedObject>,
    encoded_bytes: usize,
}

/// One object as datagrams: its source symbols, then its repair symbols.
struct EncodedObject {
    key: (u64, u64),
    symbol_size: u16,
    source_symbols: usize,
    packets: Vec<Bytes>,
    bytes: usize,
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

    /// Symbol size for one object: the configured size, or a multiple of it
    /// that keeps a large object within about 64 source symbols.
    fn symbol_size_for(&self, frame_len: usize) -> u16 {
        self.symbol_size_within(frame_len, ADAPTIVE_SOURCE_SYMBOLS)
    }

    /// Symbol size that keeps an object within about `source_symbols`.
    fn symbol_size_within(&self, frame_len: usize, source_symbols: usize) -> u16 {
        let base = usize::from(self.symbol_size.max(1));
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
        let encoder = DatagramFecEncoder::new()
            .with_repair_symbols(min_repair_symbols)
            .with_symbol_size(symbol_size)
            .with_initial_block_id(initial_block_id);
        Self {
            encoder,
            min_repair_symbols,
            repair_ratio: repair_ratio.max(0.0),
            max_repair_symbols,
            symbol_size: symbol_size.max(1),
            max_symbol_size: 0,
            stats,
            link_loss: None,
            encoded: VecDeque::new(),
            encoded_bytes: 0,
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

    /// Send one stored part to a child. The part is encoded once for all the
    /// children that take the same symbol size, with the repair symbols of
    /// the child with the most loss, and each child receives as many repair
    /// symbols as its own path needs.
    async fn send_object(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        key: (u64, u64),
        frame: &[u8],
    ) -> Result<(), MeshError> {
        let (symbol_size, repair_symbols) = self.plan(frame.len(), self.loss_ppm_to(peer));
        let cached = self.encoded.iter().position(|object| {
            object.key == key
                && object.symbol_size == symbol_size
                && object.packets.len() >= object.source_symbols + repair_symbols as usize
                && !object.sent_to.contains(&peer)
        });
        let index = match cached {
            Some(index) => index,
            None => {
                let (_, worst) = self.plan(frame.len(), self.worst_loss_ppm());
                let repair = repair_symbols.max(worst);
                self.encoder.set_symbol_size(symbol_size);
                let packets = match self
                    .encoder
                    .encode_object_with_repair_symbols(frame, repair)
                {
                    Ok(packets) => packets.into_iter().map(Bytes::from).collect::<Vec<_>>(),
                    Err(err) => {
                        self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
                        return Err(err.into());
                    }
                };
                self.stats.tx_objects.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .tx_protected_bytes
                    .fetch_add(frame.len() as u64, Ordering::Relaxed);
                let bytes = packets.iter().map(Bytes::len).sum::<usize>();
                // An older encoding of the same part with less repair is of
                // no more use.
                self.encoded
                    .retain(|object| object.key != key || object.symbol_size != symbol_size);
                self.encoded_bytes = self.encoded.iter().map(|object| object.bytes).sum();
                self.encoded.push_back(EncodedObject {
                    key,
                    symbol_size,
                    source_symbols: usize::from(source_symbol_count(frame.len(), symbol_size)),
                    packets,
                    bytes,
                    sent_to: Vec::new(),
                });
                self.encoded_bytes += bytes;
                while self.encoded.len() > ENCODED_OBJECT_CACHE
                    || (self.encoded_bytes > ENCODED_OBJECT_CACHE_BYTES && self.encoded.len() > 1)
                {
                    if let Some(old) = self.encoded.pop_front() {
                        self.encoded_bytes -= old.bytes;
                    }
                }
                self.encoded.len() - 1
            }
        };
        self.encoded[index].sent_to.push(peer);
        let object = &self.encoded[index];
        let source_symbols = object.source_symbols;
        let count = (source_symbols + repair_symbols as usize).min(object.packets.len());
        for (position, packet) in object.packets[..count].iter().enumerate() {
            let sent = match socket.send_to(packet, peer).await {
                Ok(sent) => sent,
                Err(err) => {
                    self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(err.into());
                }
            };
            if position < source_symbols {
                self.stats
                    .tx_source_datagrams
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.stats
                    .tx_repair_datagrams
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.stats
                .tx_wire_bytes
                .fetch_add(sent as u64, Ordering::Relaxed);
        }
        Ok(())
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
        self.encoder.set_symbol_size(symbol_size);
        let source_symbols = usize::from(source_symbol_count(frame.len(), symbol_size));
        let packets = match self
            .encoder
            .encode_object_with_repair_symbols(frame, repair_symbols)
        {
            Ok(packets) => packets,
            Err(err) => {
                self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
                return Err(err.into());
            }
        };
        self.stats.tx_objects.fetch_add(1, Ordering::Relaxed);
        self.stats
            .tx_protected_bytes
            .fetch_add(frame.len() as u64, Ordering::Relaxed);

        for (index, packet) in packets.into_iter().enumerate() {
            let sent = match socket.send_to(&packet, peer).await {
                Ok(sent) => sent,
                Err(err) => {
                    self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(err.into());
                }
            };
            if index < source_symbols {
                self.stats
                    .tx_source_datagrams
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.stats
                    .tx_repair_datagrams
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.stats
                .tx_wire_bytes
                .fetch_add(sent as u64, Ordering::Relaxed);
        }

        Ok(())
    }
}

#[derive(Debug)]
struct FecBlockObservation {
    source_symbols: u16,
    received_source_symbols: HashSet<u32>,
    last_seen_datagram: u64,
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

    fn push(&mut self, peer: SocketAddr, bytes: &[u8]) -> Result<Option<Bytes>, DatagramFecError> {
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
                fragments: datagram_fragments(bytes.len()) as u32,
            });
        observation.last_seen_datagram = self.observed_datagrams;
        if is_source {
            observation
                .received_source_symbols
                .insert(encoding_symbol_id);
        }

        let decoded = decoded.map(Bytes::from);
        if let Some(frame) = decoded.as_ref() {
            let observation = self.observations.remove(&key);
            let fragments = observation
                .as_ref()
                .map_or(1, |observation| observation.fragments);
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
        Ok(decoded)
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
                if config.receive_loss_ppm > 0 {
                    loss_state ^= loss_state << 13;
                    loss_state ^= loss_state >> 7;
                    loss_state ^= loss_state << 17;
                    if loss_state % 1_000_000 < u64::from(config.receive_loss_ppm) {
                        continue;
                    }
                }

                let decoded = match receiver.push(peer, &buf[..len]) {
                    Ok(Some(decoded)) => decoded,
                    Ok(None) => continue,
                    Err(err) => {
                        warn!(peer = %peer, error = %err, "cache mesh FEC decode failed");
                        continue;
                    }
                };
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
            if let Err(err) = handler
                .cache
                .add_for_stream_id(stream_id, slot_id, payload)
                .await
            {
                warn!(
                    node_id,
                    stream_id,
                    slot_id,
                    error = err,
                    "cache mesh write failed"
                );
                return;
            }
            // The set tells the copy loop which slots came from a peer, so
            // that it does not send them back. An edge has no copy loop: it
            // must not keep a set that nothing reads and nothing prunes.
            if handler.config.role != CacheMeshRole::Edge {
                handler
                    .remote_slots
                    .write()
                    .await
                    .insert((stream_id, slot_id));
            }
            if let Some(applied_tx) = &handler.config.applied_tx {
                let _ = applied_tx.send(AppliedSlot {
                    stream_id,
                    slot_id,
                    bytes,
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

/// How often a node without children forgets the remote slots that its
/// cache no longer retains.
const REMOTE_SLOT_PRUNE_INTERVAL: Duration = Duration::from_millis(250);

/// Forget the slots that came from a peer and that the cache no longer
/// retains: slots behind the window of their stream, and slots of a stream
/// that was released.
async fn forget_unretained_remote_slots(
    cache: &ChunkCache,
    stream_ids: &[(u64, usize)],
    remote_slots: &RwLock<HashSet<(u64, usize)>>,
) {
    let retained_ranges = stream_ids
        .iter()
        .filter_map(|(stream_id, stream_idx)| {
            cache
                .last(*stream_idx)
                .map(|last| (*stream_id, (cache.retained_start(last), last)))
        })
        .collect::<HashMap<_, _>>();
    remote_slots.write().await.retain(|(stream_id, slot_id)| {
        retained_ranges
            .get(stream_id)
            .is_some_and(|(start, last)| slot_id >= start && slot_id <= last)
    });
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

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("cache mesh sync task shutting down");
                return;
            }
            _ = interval.tick() => {
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
                                .or_else(|| {
                                    backfills
                                        .get(&(*child, *stream_id))
                                        .map(Backfill::pending)
                                })
                                .unwrap_or(0),
                        })
                        .collect();
                }
                let peers = {
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
                if peers.is_empty() {
                    // A node without children still learns slots from its
                    // parents. An edge in demand replication never has a
                    // child, and its set grew by one entry for each part
                    // that it received, for the life of the process.
                    if last_remote_slot_prune.elapsed() >= REMOTE_SLOT_PRUNE_INTERVAL {
                        last_remote_slot_prune = Instant::now();
                        let stream_ids = cache.stream_ids().await;
                        forget_unretained_remote_slots(&cache, &stream_ids, &remote_slots).await;
                    }
                    continue;
                }

                let stream_ids = cache.stream_ids().await;
                forget_unretained_remote_slots(&cache, &stream_ids, &remote_slots).await;
                for (stream_id, stream_idx) in stream_ids {
                    // In demand replication a child receives a playlist only
                    // while it holds a lease for it.
                    let peers = if demand {
                        peers
                            .iter()
                            .copied()
                            .filter(|peer| active.contains(&(*peer, stream_id)))
                            .collect::<Vec<_>>()
                    } else {
                        peers.clone()
                    };
                    if peers.is_empty() {
                        continue;
                    }
                    if let Some(initialization) = cache.stream_initialization(stream_id) {
                        let frame = match MeshFrame::initialization(
                            config.node_id.clone(),
                            stream_id,
                            initialization.clone(),
                        ).encode() {
                            Ok(frame) => frame,
                            Err(err) => {
                                warn!(stream_id, error = %err, "cache mesh initialization encode failed");
                                continue;
                            }
                        };
                        for peer in &peers {
                            let key = (stream_id, stream_idx, *peer);
                            if sent_initializations.get(&key) == Some(&initialization) {
                                continue;
                            }
                            match sender.send(&socket, *peer, &frame).await {
                                Ok(()) => {
                                    sent_initializations.insert(key, initialization.clone());
                                }
                                Err(err) => {
                                    debug!(peer = %peer, stream_id, error = %err, "cache mesh initialization send failed");
                                }
                            }
                        }
                    }
                    let Some(last) = cache.last(stream_idx) else {
                        continue;
                    };
                    let retained_start = cache.retained_start(last);
                    for peer in &peers {
                        if let Some((from_slot, backfill_parts)) =
                            pending_open.remove(&(*peer, stream_id))
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
                                    Backfill { low, next: Some(last) },
                                );
                            }
                        }
                        // A cursor beyond the newest part means that the
                        // stream restarted with a new publication, whose
                        // sequence starts again. Send it from its beginning.
                        let next = sent
                            .get(&(stream_id, stream_idx, *peer))
                            .copied()
                            .filter(|slot| *slot <= last)
                            .and_then(|slot| slot.checked_add(1))
                            .unwrap_or(retained_start)
                            .max(retained_start);
                        if next > last {
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
                            ).encode() {
                                Ok(frame) => frame,
                                Err(err) => {
                                    warn!(stream_id, slot_id, error = %err, "cache mesh chunk encode failed");
                                    continue;
                                }
                            };
                            match sender
                                .send_object(&socket, *peer, (stream_id, slot_id as u64), &frame)
                                .await
                            {
                                Ok(()) => {
                                    sent.insert((stream_id, stream_idx, *peer), slot_id);
                                }
                                Err(err) => {
                                    debug!(peer = %peer, stream_id, slot_id, error = %err, "cache mesh chunk send failed");
                                    break;
                                }
                            }
                        }
                    }

                    // Warm-up windows go after the live parts of the tick,
                    // newest part first, within the allowance of each child.
                    for peer in &peers {
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
                            let frame = match MeshFrame::chunk(
                                config.node_id.clone(),
                                stream_id,
                                slot_id as u64,
                                payload,
                            ).encode() {
                                Ok(frame) => frame,
                                Err(err) => {
                                    warn!(stream_id, slot_id, error = %err, "cache mesh chunk encode failed");
                                    continue;
                                }
                            };
                            if let Err(err) = sender
                                .send_object(&socket, *peer, (stream_id, slot_id as u64), &frame)
                                .await
                            {
                                debug!(peer = %peer, stream_id, slot_id, error = %err, "cache mesh warm-up send failed");
                                break;
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
            }
        }
    }
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

        let stream_ids = cache.stream_ids().await;
        forget_unretained_remote_slots(&cache, &stream_ids, &remote_slots).await;

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
}
