use crate::chunk_cache::{ChunkCache, RemoteWrite, StreamHandle};
use crate::mesh_send::{
    call_time, BatchSender, Message, ObjectDatagrams, ObjectEncoder, MESSAGES_PER_CALL,
};
use arc_swap::ArcSwap;
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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, error, info, warn};
use xxhash_rust::xxh3::xxh3_64;

const FRAME_MAGIC: &[u8; 8] = b"PLMESH1\0";
const FRAME_VERSION: u8 = 2;
const FRAME_HELLO: u8 = 1;
const FRAME_CHUNK: u8 = 2;
const FRAME_REPLICA_REQUEST: u8 = 3;
const FRAME_INITIALIZATION: u8 = 4;
const FRAME_SUBSCRIBE: u8 = 5;
const FRAME_UNSUBSCRIBE: u8 = 6;
const FRAME_LINK_REPORT: u8 = 7;
/// The version that a node speaking like a later build writes, and the kind
/// of the frame that it sends besides (see
/// `CacheMeshConfig::with_test_future_frames`).
const FUTURE_FRAME_VERSION: u8 = FRAME_VERSION + 1;
const FUTURE_FRAME_KIND: u8 = 0x7e;
/// A hello extension that no build defines, which such a node appends.
const FUTURE_HELLO_EXTENSION: u8 = 0x7f;
/// How often such a node sends each peer a frame of the unknown kind.
const FUTURE_FRAME_INTERVAL: Duration = Duration::from_secs(1);
/// Hello extensions follow the role byte, any number of them, each
/// `tag u8, len u16, value[len]`. The process's start in Unix microseconds
/// (u64): a peer whose boot id changed has restarted.
const HELLO_EXTENSION_BOOT_ID: u8 = 0x01;
/// The sender's build id: UTF-8, at most [`HELLO_BUILD_MAX_BYTES`].
const HELLO_EXTENSION_BUILD: u8 = 0x02;
/// The sender's state, one byte (see `CacheMeshConfig::with_hello_state`).
const HELLO_EXTENSION_STATE: u8 = 0x03;
const HELLO_BUILD_MAX_BYTES: usize = 64;
/// The rank byte of a Subscribe whose sender gives no rank.
const SUBSCRIBE_RANK_NONE: u8 = u8::MAX;
/// One warning of a frame that does not decode, for each peer, in this long.
const INVALID_FRAME_WARN_INTERVAL: Duration = Duration::from_secs(10);
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
/// `SO_PRIORITY` of the audio socket of a mesh split by class: band 0 of the
/// hosts' `prio` qdisc, and the highest priority that needs no
/// `CAP_NET_ADMIN`.
pub const AUDIO_SOCKET_PRIORITY: u32 = 6;
const REQUEST_BLOCK_ID_BASE: u32 = 1 << 30;
const REPLY_BLOCK_ID_BASE: u32 = (1 << 30) + (1 << 29);
const CHUNK_BLOCK_ID_BASE: u32 = 1 << 31;
/// Each lane of block ids starts at its base plus a per-start offset below
/// this bound.
const BLOCK_ID_START_SPAN: u32 = 1 << 28;
const MAX_GOSSIP_PEERS: usize = 256;
/// The top bit of a hello's role byte: the sender is draining. Its children
/// rank it after every live parent that is not (see
/// `CacheMeshConfig::avoid_draining_parents`).
const HELLO_DRAINING_BIT: u8 = 0x80;
/// The low four bits of a hello's role byte: the role. Bits 4 to 6 are
/// reserved and ignored.
const HELLO_ROLE_MASK: u8 = 0x0f;
const FEC_OBSERVATION_DATAGRAM_WINDOW: u64 = 8_192;
const FEC_COMPLETED_BLOCK_WINDOW: usize = 4_096;
const DEFAULT_MAX_PEERS: usize = 256;
const DEFAULT_MAX_INFLIGHT_FRAMES: usize = 64;
const DEFAULT_REPLICA_QUEUE_CAPACITY: usize = 64;
/// Room in a frame beyond its payload: the head of a chunk or
/// initialization frame with any node id, and the largest hello (256 gossip
/// peers). `max_frame_bytes` follows the cache's payload limits plus this.
const FRAME_OVERHEAD_BYTES: usize = 64 * 1024;
/// `max_frame_bytes` when there is no cache to follow (`FecReceiver::new`
/// in tests).
#[cfg(test)]
const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheMeshRole {
    /// A flat mesh of equal nodes: a node sends the parts written on it to
    /// every peer and receives theirs. It does not relay a part that it
    /// received: a part reaches only the peers of the node that wrote it.
    #[default]
    Peer,
    /// A regional cache distributor may serve child replicas.
    Distributor,
    /// A leaf playback cache receives from parents and never forwards cache data.
    Edge,
}

impl CacheMeshRole {
    fn wire_value(self) -> u8 {
        match self {
            Self::Peer => 0,
            Self::Distributor => 1,
            Self::Edge => 2,
        }
    }

    /// The role in a hello's role byte.
    fn from_wire(value: u8) -> WireRole {
        match value & HELLO_ROLE_MASK {
            0 => WireRole::Known(Self::Peer),
            1 => WireRole::Known(Self::Distributor),
            2 => WireRole::Known(Self::Edge),
            other => WireRole::Other(other),
        }
    }
}

/// The role that a peer's hello gives: one of this build's, or one that this
/// build does not know (a later build's). A peer of an unknown role is heard
/// and counted (`rx_unknown_roles`); it is never a parent and never a child,
/// and no node of this build can be given such a role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireRole {
    Known(CacheMeshRole),
    /// The role bits (3 to 15) as they arrived.
    Other(u8),
}

impl WireRole {
    fn wire_value(self) -> u8 {
        match self {
            Self::Known(role) => role.wire_value(),
            Self::Other(value) => value & HELLO_ROLE_MASK,
        }
    }

    /// Whether a node of role `own` takes playlists from a peer of this
    /// role (`None`: its hello has not arrived).
    fn may_be_parent(role: Option<Self>, own: CacheMeshRole) -> bool {
        match role {
            Some(Self::Other(_)) => false,
            // Only a flat mesh of equal nodes takes parts from a leaf.
            Some(Self::Known(CacheMeshRole::Edge)) => own == CacheMeshRole::Peer,
            Some(Self::Known(_)) | None => true,
        }
    }

    /// Whether a node of role `own` sends playlists to a peer of this role
    /// (`None`: its hello has not arrived). A distributor's children are
    /// the leaves.
    fn may_be_child(role: Option<Self>, own: CacheMeshRole) -> bool {
        match role {
            Some(Self::Other(_)) => false,
            Some(Self::Known(role)) => {
                own != CacheMeshRole::Distributor || role == CacheMeshRole::Edge
            }
            None => true,
        }
    }
}

/// How a node writes its frames.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum FrameForm {
    /// As this build does.
    #[default]
    Current,
    /// As a later build might (see
    /// `CacheMeshConfig::with_test_future_frames`): a later version, an
    /// extension on every hello and three bytes on every subscribe that this
    /// build does not know.
    Future,
}

impl FrameForm {
    fn version(self) -> u8 {
        match self {
            Self::Current => FRAME_VERSION,
            Self::Future => FUTURE_FRAME_VERSION,
        }
    }
}

#[derive(Debug)]
pub enum MeshError {
    Io(std::io::Error),
    Fec(DatagramFecError),
    InvalidFrame(&'static str),
    /// A frame of a kind that this build does not know (a later build's).
    /// Its body was not read.
    UnknownKind(u8),
    InvalidUtf8(std::str::Utf8Error),
    Cache(&'static str),
}

impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Fec(err) => write!(f, "fec error: {err}"),
            Self::InvalidFrame(msg) => write!(f, "invalid mesh frame: {msg}"),
            Self::UnknownKind(kind) => write!(f, "unknown mesh frame kind {kind:#04x}"),
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
    /// Push every playlist to every child.
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

/// Decides, for a (child, stream id) pair that the sync task is about to
/// push live parts to, whether to skip the push: the child gets the stream's
/// parts another way (a distributor's fast plane hands its lease children
/// the contributor's symbols as they arrive). A skipped pair keeps its
/// lease, its warm-up window, its cursor (it moves on with the stream, so a
/// later push starts at the next part) and every replica request the child
/// sends (its hole repair). It runs inline on the sync task, once per pair
/// per pass, and must be cheap.
pub type CacheMeshLivePushFilter = Arc<dyn Fn(SocketAddr, u64) -> bool + Send + Sync>;

#[derive(Clone, Default)]
struct LivePushFilterSlot(Option<CacheMeshLivePushFilter>);

impl fmt::Debug for LivePushFilterSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() { "Some(..)" } else { "None" })
    }
}

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

/// The class of a playlist on a cache mesh that is split by class (see
/// [`CacheMeshConfig::with_class_port_offset`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StreamClass {
    /// Audio, and every playlist that the classifier does not call video.
    /// Its parts go on the main socket.
    #[default]
    Audio,
    /// Video: its parts go on the class socket, with their own receive and
    /// sync tasks, so that encoding, sending and decoding them never holds an
    /// audio part back.
    Video,
}

/// Tells the cache mesh the class of a playlist, by stream id. It is called
/// by the sync tasks for every stream of every pass and by the receive task
/// for each replica request, so it must be cheap (a lookup), must not block,
/// and must not call into the mesh.
pub type CacheMeshStreamClassifier = Arc<dyn Fn(u64) -> StreamClass + Send + Sync>;

/// A runtime that the mesh built, which may be dropped anywhere: in an
/// asynchronous context too, where dropping a runtime directly panics.
struct OwnedRuntime(Option<tokio::runtime::Runtime>);

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// Holds the optional classifier so that the configuration stays `Debug`.
#[derive(Clone, Default)]
struct ClassifierSlot(Option<CacheMeshStreamClassifier>);

impl fmt::Debug for ClassifierSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() { "Some(..)" } else { "None" })
    }
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
/// or when `prefix` is too short. A frame of a later version is read as
/// [`MeshFrame::decode`] reads it.
pub fn chunk_frame_slot(prefix: &[u8]) -> Option<(u64, u64)> {
    let header = FRAME_MAGIC.len() + 2;
    if prefix.len() < header + 2 || &prefix[..FRAME_MAGIC.len()] != FRAME_MAGIC {
        return None;
    }
    if prefix[FRAME_MAGIC.len()] < FRAME_VERSION || prefix[FRAME_MAGIC.len() + 1] != FRAME_CHUNK {
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
/// slot. The payload is copied out of `frame`.
pub fn chunk_frame_payload(frame: &[u8]) -> Option<(u64, u64, Bytes)> {
    match MeshFrame::decode(Bytes::copy_from_slice(frame)).ok()? {
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
    /// This node's place in the child's ranking of the playlist's parents
    /// (0: its first parent), from the child's newest Subscribe; None when
    /// the child did not say (a node from before, or `subscribe_rank` off).
    pub rank: Option<u8>,
}

#[derive(Clone, Debug)]
pub struct CacheMeshConfig {
    pub node_id: String,
    pub region: String,
    pub bind_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub announce_interval: Duration,
    /// How often the sync task runs a whole pass: it picks up new leases and
    /// children and paces warm-up windows. A part leaves for the children as
    /// soon as the local watermark reaches it, without waiting for the tick.
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
    /// Addresses authorized to send frames or join through gossip. A
    /// datagram from any other address is dropped before it is decoded.
    /// `with_peer` and `with_peers` authorize the peers they add. On a mesh
    /// split by class, authorizing a peer authorizes its class socket too
    /// (its address at the main port plus `class_port_offset`), on the class
    /// socket only.
    pub allowed_peers: HashSet<SocketAddr>,
    /// Hard limit on known peer addresses and per-peer decoder state.
    pub max_peers: usize,
    /// Hard limit on incomplete FEC objects retained across all peers.
    pub max_inflight_frames: usize,
    /// Largest decoded mesh frame accepted or sent. Zero follows the cache:
    /// its largest part or initialization (`buffer_size_kb`,
    /// `init_size_kb`) plus room for a frame head.
    pub max_frame_bytes: usize,
    /// Hard limit on remembered remote-origin slots. Zero follows the cache:
    /// every slot that it retains, plus the parts of one prune interval.
    pub max_remote_slots: usize,
    /// Replica requests waiting to be served. A request that finds the
    /// queue full is dropped; the requester asks again. On a mesh split by
    /// class the class lane has a queue of this size of its own for the
    /// requests for video playlists.
    pub replica_queue_capacity: usize,
    /// Whether children receive every playlist or only subscribed playlists.
    pub replication: ReplicationMode,
    /// How long a parent keeps pushing a playlist after the last subscribe.
    pub subscription_lease: Duration,
    /// Rate limit for the warm-up window that a new subscription receives, for
    /// each child. Live parts are never limited. Zero sends the window at once.
    pub backfill_bytes_per_sec: u64,
    /// Rate limit for the warm-up windows of all children together, on top
    /// of each child's: when many edges warm at once (edges join, or the
    /// readers of a lost edge move) a distributor sends at most this many
    /// warm-up bytes a second on each lane (burst: 100 ms of it; a large
    /// part takes at most one burst of debt), the children taking turns.
    /// Zero: no total limit.
    pub backfill_total_bytes_per_sec: u64,
    /// The warm-up window of a new lease on a video playlist (see
    /// `with_stream_classifier`) is at most this many parts; `None` (the
    /// default): the window the child asks for, as for audio. A 4K playlist
    /// warmed at an audio window's length is minutes of parts sent at the
    /// warm-up rate, oldest last, each part in one burst (a 4K part of a few
    /// MB fills the child's receive queue at once), long after its readers
    /// started at the live part.
    pub video_backfill_parts: Option<usize>,
    /// Subscribe frames carry the parent's place in this node's ranking of
    /// the playlist's parents (0: first), so a parent knows whether it is
    /// the child's first parent. Off, the rank byte says that the child
    /// gives none.
    pub subscribe_rank: bool,
    /// A parent that left the ranked set of a playlist is released
    /// (Unsubscribe), make before break: it is kept and its lease renewed
    /// until its replacement is confirmed, or for `parent_release_after`
    /// (see [`CacheMeshHandle::release_pending`]). Off: a parent that left is
    /// neither renewed nor released, and its lease lapses.
    pub release_unranked_parents: bool,
    /// Rank a parent whose hello says it is draining after every live
    /// parent that is not.
    pub avoid_draining_parents: bool,
    /// When more live parents than `max_parents` are ranked (a distributor
    /// slot has joined), the second parent is asked for the warm-up window
    /// too, not only the first: a slot ranked first holds a playlist only
    /// from its own subscription on, and its window is nearly empty (wave 8
    /// review). With two parents, only the first, as before.
    pub warm_from_second_parent: bool,
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
    /// What this node's hellos and subscribes give as its boot id: the
    /// start of the process in Unix microseconds (`with_boot_id`). Zero
    /// until `start` sets it to the time of the start.
    boot_id: u64,
    /// The build id in this node's hellos (`with_build`).
    build: Option<String>,
    /// The state in this node's hellos, read for each hello
    /// (`with_hello_state`).
    hello_state: Option<Arc<AtomicU8>>,
    /// How long a parent that left a playlist's ranked set is kept while
    /// its replacement is not confirmed (`with_parent_release_after`).
    /// `None`: two thirds of the subscription lease.
    parent_release_after: Option<Duration>,
    /// The form of the frames that this node writes.
    frame_form: FrameForm,
    /// Told when the first datagram of each part leaves for a child.
    send_observer: SendObserverSlot,
    /// Pairs whose live parts are not pushed (see `with_live_push_filter`).
    live_push_filter: LivePushFilterSlot,
    /// Reads the origin's epoch from a part's bytes (see `with_slot_epoch`).
    slot_epoch: Option<fn(&[u8]) -> Option<u64>>,
    /// Told of every received datagram and decoded block.
    receive_observer: ReceiveObserverSlot,
    /// Tests turn generic segmentation offload off to take the path of a
    /// kernel without it.
    #[cfg(test)]
    gso: bool,
    /// Split the mesh by class: when non-zero, a second socket at the main
    /// port plus this offset carries the parts and initializations of the
    /// video playlists (see `with_stream_classifier`), with its own receive
    /// task, sync task and replica server, and the main socket carries the
    /// audio playlists. Hellos, leases, replica requests and loss reports
    /// stay on the main socket. Every node of a mesh must use the same
    /// offset: a node sends a peer's video parts to the peer's main port plus
    /// the offset, and takes a datagram on its class socket from port `p` as
    /// one from the peer at `p` minus the offset, which must be an
    /// authorized peer (see `allowed_peers`). Zero, the default, keeps one
    /// socket and one set of tasks for every playlist.
    pub class_port_offset: u16,
    /// The class of each playlist. Without one every playlist is audio.
    stream_classifier: ClassifierSlot,
    /// The observer of the class socket's receive task. None (the default)
    /// gives it none: `receive_observer` sees only the main socket.
    class_receive_observer: ReceiveObserverSlot,
    /// Where the class socket's applied parts go on a mesh split by class:
    /// `None` (the default) sends them to `applied_tx` with the audio parts.
    pub class_applied_tx: Option<mpsc::UnboundedSender<AppliedSlot>>,
    /// `SO_PRIORITY` of the main (audio) socket of a mesh split by class, so
    /// that a host's `prio` qdisc puts the audio parts in the band above the
    /// video ones. Default `Some(6)` (the highest a process may set without
    /// `CAP_NET_ADMIN`); `None` leaves the socket's priority alone. A mesh
    /// that is not split never sets it.
    pub main_socket_priority: Option<u32>,
    /// `SO_PRIORITY` of the class (video) socket. Default `None`: left at 0.
    pub class_socket_priority: Option<u32>,
    /// Where the class socket's receive task, sync task and replica server
    /// run. Default [`ClassLaneRuntime::OwnThread`].
    pub class_runtime: ClassLaneRuntime,
}

/// Where the tasks of the class (video) lane of a mesh split by class run.
#[derive(Clone, Debug, Default)]
pub enum ClassLaneRuntime {
    /// A thread of their own with a current-thread runtime (the default): a
    /// 4K encode, send or decode runs there, never on a worker that the audio
    /// tasks need, whatever runtime the caller starts the mesh on.
    #[default]
    OwnThread,
    /// A runtime that the caller chose, for example one pinned to a CPU.
    /// `Handle(tokio::runtime::Handle::current())` runs them with the audio
    /// tasks, on the runtime that starts the mesh.
    Handle(tokio::runtime::Handle),
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
            allowed_peers: HashSet::new(),
            max_peers: DEFAULT_MAX_PEERS,
            max_inflight_frames: DEFAULT_MAX_INFLIGHT_FRAMES,
            max_frame_bytes: 0,
            max_remote_slots: 0,
            replica_queue_capacity: DEFAULT_REPLICA_QUEUE_CAPACITY,
            replication: ReplicationMode::All,
            subscription_lease: DEFAULT_SUBSCRIPTION_LEASE,
            backfill_bytes_per_sec: 0,
            backfill_total_bytes_per_sec: 0,
            video_backfill_parts: None,
            subscribe_rank: true,
            release_unranked_parents: true,
            warm_from_second_parent: true,
            avoid_draining_parents: true,
            socket_buffer_bytes: None,
            peer_timeout: DEFAULT_PEER_TIMEOUT,
            applied_tx: None,
            receive_loss_ppm: 0,
            block_id_offset: 0,
            boot_id: 0,
            build: None,
            hello_state: None,
            parent_release_after: None,
            frame_form: FrameForm::Current,
            send_observer: SendObserverSlot(None),
            live_push_filter: LivePushFilterSlot(None),
            slot_epoch: None,
            receive_observer: ReceiveObserverSlot(None),
            #[cfg(test)]
            gso: true,
            class_port_offset: 0,
            stream_classifier: ClassifierSlot(None),
            class_receive_observer: ReceiveObserverSlot(None),
            class_applied_tx: None,
            main_socket_priority: Some(AUDIO_SOCKET_PRIORITY),
            class_socket_priority: None,
            class_runtime: ClassLaneRuntime::OwnThread,
        }
    }

    /// Add a peer and authorize it.
    pub fn with_peer(mut self, peer: SocketAddr) -> Self {
        self.peers.push(peer);
        self.allowed_peers.insert(peer);
        self
    }

    /// Add peers and authorize them.
    pub fn with_peers(mut self, peers: impl IntoIterator<Item = SocketAddr>) -> Self {
        for peer in peers {
            self.peers.push(peer);
            self.allowed_peers.insert(peer);
        }
        self
    }

    /// Authorize peers that are not configured: children that a distributor
    /// learns from their hello, and peers learned through a trusted seed.
    pub fn with_allowed_peers(mut self, peers: impl IntoIterator<Item = SocketAddr>) -> Self {
        self.allowed_peers.extend(peers);
        self
    }

    pub fn with_max_peers(mut self, max_peers: usize) -> Self {
        self.max_peers = max_peers.max(1);
        self
    }

    pub fn with_max_inflight_frames(mut self, max_inflight_frames: usize) -> Self {
        self.max_inflight_frames = max_inflight_frames.max(1);
        self
    }

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes.max(1);
        self
    }

    pub fn with_max_remote_slots(mut self, max_remote_slots: usize) -> Self {
        self.max_remote_slots = max_remote_slots.max(1);
        self
    }

    pub fn with_replica_queue_capacity(mut self, capacity: usize) -> Self {
        self.replica_queue_capacity = capacity.max(1);
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

    /// The total warm-up allowance of the node (see
    /// [`Self::backfill_total_bytes_per_sec`]).
    pub fn with_backfill_total_bytes_per_sec(mut self, bytes_per_sec: u64) -> Self {
        self.backfill_total_bytes_per_sec = bytes_per_sec;
        self
    }

    /// Bound the warm-up window of video playlists (see
    /// [`Self::video_backfill_parts`]).
    pub fn with_video_backfill_parts(mut self, parts: Option<usize>) -> Self {
        self.video_backfill_parts = parts;
        self
    }

    pub fn with_subscribe_rank(mut self, subscribe_rank: bool) -> Self {
        self.subscribe_rank = subscribe_rank;
        self
    }

    pub fn with_release_unranked_parents(mut self, release: bool) -> Self {
        self.release_unranked_parents = release;
        self
    }

    /// See [`Self::warm_from_second_parent`].
    pub fn with_warm_from_second_parent(mut self, warm: bool) -> Self {
        self.warm_from_second_parent = warm;
        self
    }

    pub fn with_avoid_draining_parents(mut self, avoid: bool) -> Self {
        self.avoid_draining_parents = avoid;
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

    /// The boot id of this node: the start of its process in Unix
    /// microseconds. Its hellos and its subscribes carry it. A parent whose
    /// boot id changes has restarted and holds no lease
    /// ([`CacheMeshHandle::parents_changed`]); a child whose boot id changes
    /// has restarted and gets a new lease, with its warm-up window and the
    /// initialization. Without this call it is the time of
    /// [`CacheMesh::start`]. Zero is not a boot id.
    pub fn with_boot_id(mut self, boot_id: u64) -> Self {
        self.boot_id = boot_id;
        self
    }

    /// The build id that this node's hellos give, for a peer to show
    /// ([`CacheMeshHandle::peer_statuses`]); it changes nothing else. At
    /// most 64 bytes of it are sent.
    pub fn with_build(mut self, build: String) -> Self {
        let mut end = build.len().min(HELLO_BUILD_MAX_BYTES);
        while !build.is_char_boundary(end) {
            end -= 1;
        }
        self.build = Some(build[..end].to_string());
        self
    }

    /// The state that this node's hellos give, read from `state` for each
    /// hello: 0 starting, 1 ready, 2 degraded, 3 draining, 4 stopping. A
    /// peer shows it ([`CacheMeshHandle::peer_statuses`]); ranking reads
    /// only [`CacheMeshHandle::set_draining`].
    pub fn with_hello_state(mut self, state: Arc<AtomicU8>) -> Self {
        self.hello_state = Some(state);
        self
    }

    /// How long a parent that left a playlist's ranked set is kept, its
    /// lease renewed, while its replacement is not confirmed (see
    /// [`CacheMeshHandle::release_pending`]). The default is two thirds of
    /// the subscription lease: two renewal periods of a child that renews
    /// at a third of its lease.
    pub fn with_parent_release_after(mut self, after: Duration) -> Self {
        self.parent_release_after = Some(after);
        self
    }

    /// For the proof that two builds interoperate: make this node speak as a
    /// later build might. It writes the next frame version, appends an
    /// extension that no build defines to every hello and three bytes to
    /// every subscribe, and sends every peer one frame of an unknown kind a
    /// second. A node of this build reads all of it as far as it knows it
    /// and counts the frame of the unknown kind (`rx_unknown_kinds`).
    #[doc(hidden)]
    pub fn with_test_future_frames(mut self, future: bool) -> Self {
        self.frame_form = if future {
            FrameForm::Future
        } else {
            FrameForm::Current
        };
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

    /// Be told `(stream_id, slot_id, child, sent_unix_us)` each time the sync
    /// task hands the first datagram of a part to the kernel for a child.
    /// Skip the live push of the (child, stream) pairs that `filter` names
    /// (see [`CacheMeshLivePushFilter`]). None (the default) pushes every
    /// leased pair.
    pub fn with_live_push_filter(mut self, filter: CacheMeshLivePushFilter) -> Self {
        self.live_push_filter = LivePushFilterSlot(Some(filter));
        self
    }

    /// Read the epoch of the origin that made a part (its session) from the
    /// part's bytes; `None` when the bytes do not say. With it, a part from
    /// a parent is judged against the part that holds its slot: the same
    /// epoch keeps the first copy of an id (`apply_conflicts`) and never
    /// lets an older id replace a newer one, an older epoch is dropped
    /// (`apply_stale`), a newer one replaces. Two origins that publish the
    /// same parts then merge in every cache by first arrival. Without it, or
    /// when either epoch is unknown, the part from a parent replaces what
    /// its slot holds (a restarted origin numbers its parts from the start
    /// again).
    pub fn with_slot_epoch(mut self, epoch: fn(&[u8]) -> Option<u64>) -> Self {
        self.slot_epoch = Some(epoch);
        self
    }

    pub fn with_send_observer(mut self, observer: CacheMeshSendObserver) -> Self {
        self.send_observer = SendObserverSlot(Some(observer));
        self
    }

    /// Send without generic segmentation offload, as on a kernel that does
    /// not have it.
    #[cfg(test)]
    fn without_gso(mut self) -> Self {
        self.gso = false;
        self
    }

    /// Be told of every received datagram, before it is decoded, and of
    /// every decoded block (see [`CacheMeshReceiveObserver`]).
    pub fn with_receive_observer(mut self, observer: Arc<dyn CacheMeshReceiveObserver>) -> Self {
        self.receive_observer = ReceiveObserverSlot(Some(observer));
        self
    }

    /// Split the mesh by class, with the video socket at the main port plus
    /// `offset` (see [`Self::class_port_offset`]). Zero keeps one socket.
    pub fn with_class_port_offset(mut self, offset: u16) -> Self {
        self.class_port_offset = offset;
        self
    }

    /// Tell the mesh the class of each playlist. Used only when the mesh is
    /// split by class.
    pub fn with_stream_classifier(mut self, classifier: CacheMeshStreamClassifier) -> Self {
        self.stream_classifier = ClassifierSlot(Some(classifier));
        self
    }

    /// The observer of the class socket's receive task, which sees only the
    /// datagrams of video playlists. Without this call (or with `None`) the
    /// class socket has no observer: the observer of
    /// [`Self::with_receive_observer`] runs on the main (audio) receive task
    /// only, so that whatever it locks is never taken by the video task. An
    /// observer given to both is called from two tasks at once.
    pub fn with_class_receive_observer(
        mut self,
        observer: Option<Arc<dyn CacheMeshReceiveObserver>>,
    ) -> Self {
        self.class_receive_observer = ReceiveObserverSlot(observer);
        self
    }

    /// Send the applied parts of the class (video) socket to a channel of
    /// their own, so that a consumer can take the audio parts of `applied_tx`
    /// without reading behind video ones.
    pub fn with_class_applied_tx(mut self, applied_tx: mpsc::UnboundedSender<AppliedSlot>) -> Self {
        self.class_applied_tx = Some(applied_tx);
        self
    }

    /// `SO_PRIORITY` of the main (audio) socket of a mesh split by class
    /// (see [`Self::main_socket_priority`]); `None` leaves it alone.
    pub fn with_main_socket_priority(mut self, priority: Option<u32>) -> Self {
        self.main_socket_priority = priority;
        self
    }

    /// `SO_PRIORITY` of the class (video) socket; `None` leaves it alone.
    pub fn with_class_socket_priority(mut self, priority: Option<u32>) -> Self {
        self.class_socket_priority = priority;
        self
    }

    /// Where the tasks of the class lane run (see [`ClassLaneRuntime`]).
    pub fn with_class_runtime(mut self, runtime: ClassLaneRuntime) -> Self {
        self.class_runtime = runtime;
        self
    }

    /// The class of a playlist: what the classifier says, or audio.
    pub fn stream_class(&self, stream_id: u64) -> StreamClass {
        self.stream_classifier
            .0
            .as_ref()
            .map_or(StreamClass::Audio, |classify| classify(stream_id))
    }
}

/// One socket of the cache mesh and the playlists whose parts it carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Lane {
    /// The class of playlists that the lane carries; `None` when the mesh is
    /// not split, and its one lane carries every playlist.
    class: Option<StreamClass>,
    /// How far the lane's port is above the main port, at this node and at
    /// every peer.
    port_offset: u16,
}

impl Lane {
    /// The one lane of a mesh that is not split.
    const ALL: Lane = Lane {
        class: None,
        port_offset: 0,
    };

    fn split(&self) -> bool {
        self.class.is_some()
    }

    /// Whether this is the class (video) lane of a mesh split by class, on
    /// the class socket.
    fn is_class(&self) -> bool {
        self.port_offset > 0
    }

    /// Whether this lane sends the parts of a playlist.
    fn carries(&self, config: &CacheMeshConfig, stream_id: u64) -> bool {
        self.class
            .is_none_or(|class| config.stream_class(stream_id) == class)
    }

    /// The address of the socket of this lane at the peer whose main
    /// address is `peer`. `None` when it would be beyond the last port.
    fn peer_socket(&self, peer: SocketAddr) -> Option<SocketAddr> {
        let port = peer.port().checked_add(self.port_offset)?;
        let mut addr = peer;
        addr.set_port(port);
        Some(addr)
    }

    /// The main address of the peer whose socket of this lane is `from`.
    fn peer_of(&self, from: SocketAddr) -> Option<SocketAddr> {
        let port = from.port().checked_sub(self.port_offset)?;
        let mut addr = from;
        addr.set_port(port);
        Some(addr)
    }

    /// Where to send a peer's datagrams of this lane. A peer without such a
    /// socket is never a child of the lane (see the sync task), so the main
    /// address is only a fallback.
    fn to(&self, peer: SocketAddr) -> SocketAddr {
        self.peer_socket(peer).unwrap_or(peer)
    }
}

/// A lease that a child holds on one playlist of this node.
#[derive(Debug, Clone, Copy)]
struct Lease {
    expires: Instant,
    /// Set when the lease is new. The sync task takes it and starts the
    /// warm-up window: the first slot wanted and the most parts to send.
    open: Option<(usize, usize)>,
    /// This node's place in the child's ranking of the playlist's parents,
    /// from its newest Subscribe (None: the child did not say).
    rank: Option<u8>,
    /// The boot id of the child's process that opened the lease. A
    /// Subscribe with another one comes from a restarted child, and opens
    /// the lease again.
    child_boot: Option<u64>,
    /// Which opening of a lease this is, of all that this node has served:
    /// a sync task that finds another number than it last saw for the child
    /// and playlist forgets what it sent the child before.
    generation: u32,
}

/// What a peer's newest hello said, and when it came.
#[derive(Debug, Clone)]
struct PeerHello {
    boot_id: Option<u64>,
    build: Option<String>,
    state: Option<u8>,
    draining: bool,
    at: Instant,
}

/// The parents of one playlist that this node subscribes to.
#[derive(Debug, Default)]
struct StreamParents {
    /// The ranked parents that the last subscribe asked.
    ranked: Vec<SocketAddr>,
    /// Parents that left the ranked set and are not released yet, and when
    /// each left: they keep feeding this node until their replacement is
    /// confirmed (see [`CacheMeshHandle::release_pending`]).
    pending: Vec<(SocketAddr, Instant)>,
}

/// A peer of this node as its newest hello described it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheMeshPeer {
    pub peer: SocketAddr,
    /// `None` before the peer's first hello, and for a role that this build
    /// does not know.
    pub role: Option<CacheMeshRole>,
    /// The start of the peer's process in Unix microseconds.
    pub boot_id: Option<u64>,
    /// The peer's build id.
    pub build: Option<String>,
    /// The peer's state: 0 starting, 1 ready, 2 degraded, 3 draining,
    /// 4 stopping.
    pub state: Option<u8>,
    /// The peer's hello says that it is draining.
    pub draining: bool,
    /// Whether this node may take playlists from the peer.
    pub parent: bool,
    /// How long ago this node last heard anything from the peer; `None`
    /// when it never did.
    pub heard: Option<Duration>,
}

/// A token bucket of warm-up bytes that refills from the clock it is read
/// at, so that two lanes reading it do not refill it twice. Its burst is
/// `BACKFILL_BURST` of the rate, as for a child's bucket.
#[derive(Debug, Default)]
struct WarmBucket {
    tokens: f64,
    last: Option<Instant>,
}

impl WarmBucket {
    /// The bytes that may go now at `rate` bytes a second (refilled to
    /// `now`; full at first).
    fn available(&mut self, rate: u64, now: Instant) -> f64 {
        let rate = rate as f64;
        let burst = rate * BACKFILL_BURST.as_secs_f64();
        self.tokens = match self.last {
            None => burst,
            Some(last) if now > last => {
                (self.tokens + rate * now.duration_since(last).as_secs_f64()).min(burst)
            }
            Some(_) => self.tokens,
        };
        self.last = Some(now);
        self.tokens
    }

    /// Take `bytes` sent: the debt is bounded at one burst, so that one
    /// large part (a 4K part of several MB) does not hold every later
    /// warm-up back for seconds (wave 8 review).
    fn spend(&mut self, bytes: usize, rate: u64) {
        let burst = rate as f64 * BACKFILL_BURST.as_secs_f64();
        self.tokens = (self.tokens - bytes as f64).max(-burst);
    }
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
/// What a lane knew of a playlist's children when the playlist left it: by
/// child, the last part that the lane sent (its cursor), the initialization
/// that the child has, the warm-up window still to send and a lease's
/// window not started yet.
#[derive(Debug, Default)]
struct LaneStreamState {
    sent: HashMap<SocketAddr, usize>,
    initializations: HashMap<SocketAddr, Bytes>,
    pending_open: HashMap<SocketAddr, (usize, usize)>,
    backfills: HashMap<SocketAddr, Backfill>,
}

impl LaneStreamState {
    fn forget_child(&mut self, peer: &SocketAddr) {
        self.sent.remove(peer);
        self.initializations.remove(peer);
        self.pending_open.remove(peer);
        self.backfills.remove(peer);
    }
}

/// Which lane sends a playlist of a mesh split by class.
#[derive(Debug)]
enum StreamOwner {
    /// The lane of this class sends it, and it alone holds the children's
    /// cursors.
    Lane(StreamClass),
    /// The lane that sent it gave it up, with its state, to whichever lane
    /// carries the playlist next.
    InTransit(LaneStreamState),
}

/// The owner of each playlist (by stream lifetime: its handle) of a mesh
/// split by class. Only the owner sends a playlist's parts, so the two lanes never
/// hold a cursor for the same child and playlist: a playlist that changes
/// class is sent by the lane that it leaves until that lane hands its state
/// over, and from the cursors it handed over after that.
#[derive(Debug, Default)]
struct LaneHandover {
    owners: StdMutex<HashMap<StreamHandle, StreamOwner>>,
    /// Wakes the audio lane when a playlist is handed over for it.
    wake_audio: tokio::sync::Notify,
    /// Wakes the video lane when a playlist is handed over for it.
    wake_video: tokio::sync::Notify,
}

impl LaneHandover {
    fn wake(&self, class: StreamClass) -> &tokio::sync::Notify {
        match class {
            StreamClass::Audio => &self.wake_audio,
            StreamClass::Video => &self.wake_video,
        }
    }
}

/// The per-child state of the playlists that a sync task sends.
#[derive(Debug, Default)]
struct LaneChildren {
    /// The last part sent to each child, by stream lifetime: a stream that
    /// restarted, or whose index was reused, is a new stream.
    sent: HashMap<(StreamHandle, SocketAddr), usize>,
    sent_initializations: HashMap<(StreamHandle, SocketAddr), Bytes>,
    /// The warm-up window of each new lease, before the lane starts it.
    pending_open: HashMap<(SocketAddr, u64), (usize, usize)>,
    backfills: HashMap<(SocketAddr, u64), Backfill>,
}

impl LaneChildren {
    /// The lease of a child on a playlist ended. Forget the cursors, so that
    /// a later subscription starts with the initialization again.
    fn forget_lease(&mut self, peer: SocketAddr, stream_id: u64) {
        self.sent.retain(|(sent_stream, sent_peer), _| {
            sent_stream.stream_id() != stream_id || *sent_peer != peer
        });
        self.sent_initializations
            .retain(|(sent_stream, sent_peer), _| {
                sent_stream.stream_id() != stream_id || *sent_peer != peer
            });
        self.backfills.remove(&(peer, stream_id));
        self.pending_open.remove(&(peer, stream_id));
    }

    /// Take out everything that this lane knows of a playlist's children.
    fn take_stream(&mut self, stream: StreamHandle) -> LaneStreamState {
        let stream_id = stream.stream_id();
        let mut state = LaneStreamState::default();
        self.sent.retain(|&(sent_stream, peer), slot| {
            if sent_stream != stream {
                return true;
            }
            state.sent.insert(peer, *slot);
            false
        });
        self.sent_initializations
            .retain(|&(sent_stream, peer), initialization| {
                if sent_stream != stream {
                    return true;
                }
                state.initializations.insert(peer, initialization.clone());
                false
            });
        self.pending_open.retain(|(peer, open_stream), open| {
            if *open_stream != stream_id {
                return true;
            }
            state.pending_open.insert(*peer, *open);
            false
        });
        self.backfills.retain(|(peer, backfill_stream), backfill| {
            if *backfill_stream != stream_id {
                return true;
            }
            state.backfills.insert(*peer, *backfill);
            false
        });
        state
    }

    /// Take over the state that the other lane handed over. In demand
    /// replication only the children that still hold a lease are kept. A
    /// lease window that this lane took meanwhile is newer and is kept.
    fn put_stream(
        &mut self,
        stream: StreamHandle,
        state: LaneStreamState,
        leases: Option<&LeaseTable>,
    ) {
        let stream_id = stream.stream_id();
        let leased: Option<HashSet<SocketAddr>> = leases.map(|leases| {
            lock(leases)
                .keys()
                .filter(|(_, leased_stream)| *leased_stream == stream_id)
                .map(|(peer, _)| *peer)
                .collect()
        });
        let keep = |peer: &SocketAddr| leased.as_ref().is_none_or(|leased| leased.contains(peer));
        for (peer, slot) in state.sent {
            if keep(&peer) {
                self.sent.insert((stream, peer), slot);
            }
        }
        for (peer, initialization) in state.initializations {
            if keep(&peer) {
                self.sent_initializations
                    .insert((stream, peer), initialization);
            }
        }
        for (peer, open) in state.pending_open {
            if keep(&peer) {
                self.pending_open.entry((peer, stream_id)).or_insert(open);
            }
        }
        for (peer, backfill) in state.backfills {
            if keep(&peer) && !self.pending_open.contains_key(&(peer, stream_id)) {
                self.backfills.insert((peer, stream_id), backfill);
            }
        }
    }
}

/// What a lane of a mesh split by class does with a playlist on a visit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneVisit {
    /// The lane owns the playlist: it sends its parts.
    Send,
    /// Another lane carries the playlist.
    Skip,
    /// The playlist is this lane's class now, but the lane that sent it has
    /// not handed it over yet: send nothing until it does, so that no part
    /// is skipped.
    Wait,
}

/// Decide what lane `class` does with a playlist on this visit, and hand it
/// over or take it over as its class requires. `owns` is the set of
/// playlists this lane owns (so a visit to a playlist that stays where it is
/// takes no lock).
#[allow(clippy::too_many_arguments)]
fn visit_stream(
    class: StreamClass,
    wants: bool,
    key: StreamHandle,
    owns: &mut HashSet<StreamHandle>,
    children: &mut LaneChildren,
    handover: &LaneHandover,
    leases: Option<&LeaseTable>,
) -> LaneVisit {
    match (owns.contains(&key), wants) {
        (true, true) => LaneVisit::Send,
        (false, false) => LaneVisit::Skip,
        (true, false) => {
            hand_over_stream(class, key, owns, children, handover);
            LaneVisit::Skip
        }
        (false, true) => {
            let mut owners = lock(&handover.owners);
            match owners.get_mut(&key) {
                Some(StreamOwner::Lane(owner)) if *owner != class => LaneVisit::Wait,
                Some(StreamOwner::InTransit(state)) => {
                    let state = std::mem::take(state);
                    owners.insert(key, StreamOwner::Lane(class));
                    drop(owners);
                    children.put_stream(key, state, leases);
                    owns.insert(key);
                    LaneVisit::Send
                }
                // A playlist that no lane sent yet.
                _ => {
                    owners.insert(key, StreamOwner::Lane(class));
                    owns.insert(key);
                    LaneVisit::Send
                }
            }
        }
    }
}

/// Give up a playlist that this lane owns, with its children's state, and
/// wake the other lane to take it.
fn hand_over_stream(
    class: StreamClass,
    key: StreamHandle,
    owns: &mut HashSet<StreamHandle>,
    children: &mut LaneChildren,
    handover: &LaneHandover,
) {
    let state = children.take_stream(key);
    owns.remove(&key);
    lock(&handover.owners).insert(key, StreamOwner::InTransit(state));
    let other = match class {
        StreamClass::Audio => StreamClass::Video,
        StreamClass::Video => StreamClass::Audio,
    };
    handover.wake(other).notify_one();
}

/// A replica request for a video playlist, which the main receive task hands
/// to the class lane's replica server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplicaJob {
    peer: SocketAddr,
    stream_id: u64,
    from_slot: u64,
    count: u32,
}

impl ReplicaJob {
    /// Whether serving `self` sends everything that serving `other` would:
    /// the same peer and playlist, and a window (count 0, which runs to the
    /// newest part and sends the initialization) that starts no later, or the
    /// same request.
    fn covers(&self, other: &ReplicaJob) -> bool {
        if (self.peer, self.stream_id) != (other.peer, other.stream_id) {
            return false;
        }
        if self.count == 0 {
            return self.from_slot <= other.from_slot;
        }
        self == other
    }
}

/// The queue of replica requests for the class lane, which is bounded: a
/// request that finds it full is dropped and counted, and the requester asks
/// again, as it does when a socket's receive buffer overflows.
#[derive(Debug, Clone)]
struct ClassReplicas {
    tx: mpsc::Sender<ReplicaJob>,
    dropped: Arc<AtomicU64>,
}

impl ClassReplicas {
    fn offer(&self, job: ReplicaJob) {
        if self.tx.try_send(job).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Take the jobs that are waiting behind `first`, and drop each one that
/// another job of the batch covers, so that a burst of requests for the same
/// window or hole is served once. Order is kept.
fn coalesce_replica_jobs(
    first: ReplicaJob,
    jobs: &mut mpsc::Receiver<ReplicaJob>,
) -> Vec<ReplicaJob> {
    let mut batch = vec![first];
    while let Ok(job) = jobs.try_recv() {
        batch.push(job);
    }
    coalesce_replica_batch(batch)
}

fn coalesce_replica_batch(batch: Vec<ReplicaJob>) -> Vec<ReplicaJob> {
    let mut kept: Vec<ReplicaJob> = Vec::with_capacity(batch.len());
    for job in batch {
        if kept.iter().any(|kept| kept.covers(&job)) {
            continue;
        }
        // A wider request takes the place of the first one that it covers.
        match kept.iter().position(|kept| job.covers(kept)) {
            Some(position) => {
                kept[position] = job;
                let mut index = 0;
                kept.retain(|kept| {
                    let keep = index <= position || !job.covers(kept);
                    index += 1;
                    keep
                });
            }
            None => kept.push(job),
        }
    }
    kept
}

/// The loss measured on both lanes from each peer: the worse of the two.
fn merged_measured_loss(
    measured_loss: &MeasuredLossTable,
    class_measured_loss: Option<&MeasuredLossTable>,
) -> HashMap<SocketAddr, u32> {
    let mut merged = lock(measured_loss).clone();
    if let Some(class) = class_measured_loss {
        for (peer, ppm) in lock(class).iter() {
            let entry = merged.entry(*peer).or_insert(0);
            *entry = (*entry).max(*ppm);
        }
    }
    merged
}

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

/// Set `SO_PRIORITY`, which a `prio` qdisc maps to its bands. Linux only;
/// elsewhere it does nothing.
#[cfg(target_os = "linux")]
fn set_socket_priority(socket: &std::net::UdpSocket, priority: u32) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let value = priority as libc::c_int;
    // SAFETY: the fd is open for the life of `socket`, and the option value
    // is a c_int of the size given.
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PRIORITY,
            (&value as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn set_socket_priority(_socket: &std::net::UdpSocket, _priority: u32) -> std::io::Result<()> {
    Ok(())
}

/// Bind a socket of the mesh at `addr`, with `SO_PRIORITY` `priority` if
/// given. Must run in the context of the runtime whose tasks use it.
fn bind_socket(
    config: &CacheMeshConfig,
    addr: SocketAddr,
    priority: Option<u32>,
) -> Result<UdpSocket, MeshError> {
    let std_socket = std::net::UdpSocket::bind(addr)?;
    std_socket.set_nonblocking(true)?;
    if let Some(priority) = priority {
        match set_socket_priority(&std_socket, priority) {
            Ok(()) => debug!(bind = %addr, priority, "cache mesh socket priority set"),
            // Not fatal: the parts still flow, in the qdisc's default band.
            Err(err) => {
                info!(bind = %addr, priority, error = %err, "cache mesh socket priority not set")
            }
        }
    }
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
    /// Calls that handed datagrams to the kernel: one per `sendmmsg` on the
    /// sync path, one per datagram for control frames and replicas.
    pub tx_send_calls: u64,
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
    pub rx_evicted_objects: u64,
    pub rx_oversized_objects: u64,
    pub rx_unauthorized_datagrams: u64,
    /// Frames of a kind that this build does not know (a later build's):
    /// dropped unread, and not logged.
    pub rx_unknown_kinds: u64,
    /// Hellos whose role this build does not know: the peer is heard, and
    /// is neither a parent nor a child.
    pub rx_unknown_roles: u64,
    /// Frames that did not decode for any other reason.
    pub rx_invalid_frames: u64,
    pub peer_limit_drops: u64,
    pub remote_slot_evictions: u64,
    pub replica_queue_depth: u64,
    pub replica_queue_max_depth: u64,
    pub replica_queue_drops: u64,
    pub decode_nanoseconds: u64,
    pub replica_requests_serviced: u64,
    pub replica_service_errors: u64,
    pub replica_service_nanoseconds: u64,
    /// Parts (and initializations) that replica requests were answered
    /// with: hole fills and replica windows.
    pub replica_parts_served: u64,
    /// Warm-up parts queued for children, and their payload bytes.
    pub warm_parts_sent: u64,
    pub warm_bytes_sent: u64,
    /// The most warm-up payload bytes that one sync tick queued (the
    /// largest warm-up burst of a tick; the larger of the two lanes).
    pub warm_tick_max_bytes: u64,
    /// Ticks in which a child's own warm-up allowance stopped its window.
    pub warm_starved_ticks: u64,
    /// Ticks in which the node's total warm-up allowance stopped a window.
    pub warm_capped_ticks: u64,
    /// Parents released because they left a playlist's ranked set.
    pub parents_released: u64,
    /// Those of them released after `parent_release_after`, their
    /// replacement not confirmed.
    pub parents_released_unconfirmed: u64,
    /// Parts from a parent refused because their slot held other bytes
    /// under the same id and epoch: the first copy stays
    /// (`CacheMeshConfig::with_slot_epoch`).
    pub apply_conflicts: u64,
    /// Parts from a parent refused because they were of an older epoch
    /// than the part in their slot, or an older id of the same epoch.
    pub apply_stale: u64,
}

impl CacheMeshFecStats {
    /// The counts of two lanes together.
    fn plus(self, other: Self) -> Self {
        Self {
            tx_objects: self.tx_objects + other.tx_objects,
            tx_protected_bytes: self.tx_protected_bytes + other.tx_protected_bytes,
            tx_source_datagrams: self.tx_source_datagrams + other.tx_source_datagrams,
            tx_repair_datagrams: self.tx_repair_datagrams + other.tx_repair_datagrams,
            tx_wire_bytes: self.tx_wire_bytes + other.tx_wire_bytes,
            tx_errors: self.tx_errors + other.tx_errors,
            tx_send_calls: self.tx_send_calls + other.tx_send_calls,
            rx_wire_datagrams: self.rx_wire_datagrams + other.rx_wire_datagrams,
            rx_wire_bytes: self.rx_wire_bytes + other.rx_wire_bytes,
            rx_source_datagrams: self.rx_source_datagrams + other.rx_source_datagrams,
            rx_repair_datagrams: self.rx_repair_datagrams + other.rx_repair_datagrams,
            rx_decoded_objects: self.rx_decoded_objects + other.rx_decoded_objects,
            rx_decoded_bytes: self.rx_decoded_bytes + other.rx_decoded_bytes,
            rx_repaired_objects: self.rx_repaired_objects + other.rx_repaired_objects,
            rx_repaired_source_datagrams: self.rx_repaired_source_datagrams
                + other.rx_repaired_source_datagrams,
            rx_late_source_datagrams: self.rx_late_source_datagrams
                + other.rx_late_source_datagrams,
            rx_presumed_lost_source_datagrams: self.rx_presumed_lost_source_datagrams
                + other.rx_presumed_lost_source_datagrams,
            rx_decode_errors: self.rx_decode_errors + other.rx_decode_errors,
            rx_expired_objects: self.rx_expired_objects + other.rx_expired_objects,
            rx_inflight_objects: self.rx_inflight_objects + other.rx_inflight_objects,
            rx_evicted_objects: self.rx_evicted_objects + other.rx_evicted_objects,
            rx_oversized_objects: self.rx_oversized_objects + other.rx_oversized_objects,
            rx_unauthorized_datagrams: self.rx_unauthorized_datagrams
                + other.rx_unauthorized_datagrams,
            rx_unknown_kinds: self.rx_unknown_kinds + other.rx_unknown_kinds,
            rx_unknown_roles: self.rx_unknown_roles + other.rx_unknown_roles,
            rx_invalid_frames: self.rx_invalid_frames + other.rx_invalid_frames,
            peer_limit_drops: self.peer_limit_drops + other.peer_limit_drops,
            remote_slot_evictions: self.remote_slot_evictions + other.remote_slot_evictions,
            replica_queue_depth: self.replica_queue_depth + other.replica_queue_depth,
            // Each lane has its own queue: the deeper of the two.
            replica_queue_max_depth: self
                .replica_queue_max_depth
                .max(other.replica_queue_max_depth),
            replica_queue_drops: self.replica_queue_drops + other.replica_queue_drops,
            decode_nanoseconds: self.decode_nanoseconds + other.decode_nanoseconds,
            replica_requests_serviced: self.replica_requests_serviced
                + other.replica_requests_serviced,
            replica_service_errors: self.replica_service_errors + other.replica_service_errors,
            replica_service_nanoseconds: self.replica_service_nanoseconds
                + other.replica_service_nanoseconds,
            replica_parts_served: self.replica_parts_served + other.replica_parts_served,
            warm_parts_sent: self.warm_parts_sent + other.warm_parts_sent,
            warm_bytes_sent: self.warm_bytes_sent + other.warm_bytes_sent,
            warm_tick_max_bytes: self.warm_tick_max_bytes.max(other.warm_tick_max_bytes),
            warm_starved_ticks: self.warm_starved_ticks + other.warm_starved_ticks,
            warm_capped_ticks: self.warm_capped_ticks + other.warm_capped_ticks,
            parents_released: self.parents_released + other.parents_released,
            parents_released_unconfirmed: self.parents_released_unconfirmed
                + other.parents_released_unconfirmed,
            apply_conflicts: self.apply_conflicts + other.apply_conflicts,
            apply_stale: self.apply_stale + other.apply_stale,
        }
    }
}

#[derive(Debug, Default)]
struct CacheMeshFecCounters {
    tx_objects: AtomicU64,
    tx_protected_bytes: AtomicU64,
    tx_source_datagrams: AtomicU64,
    tx_repair_datagrams: AtomicU64,
    tx_wire_bytes: AtomicU64,
    tx_errors: AtomicU64,
    tx_send_calls: AtomicU64,
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
    rx_evicted_objects: AtomicU64,
    rx_oversized_objects: AtomicU64,
    rx_unauthorized_datagrams: AtomicU64,
    rx_unknown_kinds: AtomicU64,
    rx_unknown_roles: AtomicU64,
    rx_invalid_frames: AtomicU64,
    peer_limit_drops: AtomicU64,
    remote_slot_evictions: AtomicU64,
    replica_queue_depth: AtomicU64,
    replica_queue_max_depth: AtomicU64,
    replica_queue_drops: AtomicU64,
    decode_nanoseconds: AtomicU64,
    replica_requests_serviced: AtomicU64,
    replica_service_errors: AtomicU64,
    replica_service_nanoseconds: AtomicU64,
    replica_parts_served: AtomicU64,
    warm_parts_sent: AtomicU64,
    warm_bytes_sent: AtomicU64,
    warm_tick_max_bytes: AtomicU64,
    warm_starved_ticks: AtomicU64,
    warm_capped_ticks: AtomicU64,
    parents_released: AtomicU64,
    parents_released_unconfirmed: AtomicU64,
    apply_conflicts: AtomicU64,
    apply_stale: AtomicU64,
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
            tx_send_calls: self.tx_send_calls.load(Ordering::Relaxed),
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
            rx_evicted_objects: self.rx_evicted_objects.load(Ordering::Relaxed),
            rx_oversized_objects: self.rx_oversized_objects.load(Ordering::Relaxed),
            rx_unauthorized_datagrams: self.rx_unauthorized_datagrams.load(Ordering::Relaxed),
            rx_unknown_kinds: self.rx_unknown_kinds.load(Ordering::Relaxed),
            rx_unknown_roles: self.rx_unknown_roles.load(Ordering::Relaxed),
            rx_invalid_frames: self.rx_invalid_frames.load(Ordering::Relaxed),
            peer_limit_drops: self.peer_limit_drops.load(Ordering::Relaxed),
            remote_slot_evictions: self.remote_slot_evictions.load(Ordering::Relaxed),
            replica_queue_depth: self.replica_queue_depth.load(Ordering::Relaxed),
            replica_queue_max_depth: self.replica_queue_max_depth.load(Ordering::Relaxed),
            replica_queue_drops: self.replica_queue_drops.load(Ordering::Relaxed),
            decode_nanoseconds: self.decode_nanoseconds.load(Ordering::Relaxed),
            replica_requests_serviced: self.replica_requests_serviced.load(Ordering::Relaxed),
            replica_service_errors: self.replica_service_errors.load(Ordering::Relaxed),
            replica_service_nanoseconds: self.replica_service_nanoseconds.load(Ordering::Relaxed),
            replica_parts_served: self.replica_parts_served.load(Ordering::Relaxed),
            warm_parts_sent: self.warm_parts_sent.load(Ordering::Relaxed),
            warm_bytes_sent: self.warm_bytes_sent.load(Ordering::Relaxed),
            warm_tick_max_bytes: self.warm_tick_max_bytes.load(Ordering::Relaxed),
            warm_starved_ticks: self.warm_starved_ticks.load(Ordering::Relaxed),
            warm_capped_ticks: self.warm_capped_ticks.load(Ordering::Relaxed),
            parents_released: self.parents_released.load(Ordering::Relaxed),
            parents_released_unconfirmed: self.parents_released_unconfirmed.load(Ordering::Relaxed),
            apply_conflicts: self.apply_conflicts.load(Ordering::Relaxed),
            apply_stale: self.apply_stale.load(Ordering::Relaxed),
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
        if self.config.boot_id == 0 {
            self.config.boot_id = unix_micros();
        }
        if self.config.max_frame_bytes == 0 {
            self.config.max_frame_bytes = frame_bytes_for(&self.cache);
        }
        if self.config.max_remote_slots == 0 {
            self.config.max_remote_slots = remote_slots_for(&self.cache);
        }
        let class_offset = self.config.class_port_offset;
        let split = class_offset > 0;
        let socket = Arc::new(bind_socket(
            &self.config,
            self.config.bind_addr,
            if split {
                self.config.main_socket_priority
            } else {
                None
            },
        )?);
        let local_addr = socket.local_addr()?;
        let class_lane = Lane {
            class: Some(StreamClass::Video),
            port_offset: class_offset,
        };
        let main_lane = if split {
            Lane {
                class: Some(StreamClass::Audio),
                port_offset: 0,
            }
        } else {
            Lane::ALL
        };
        // The runtime of the class lane's tasks, and the one this call builds
        // for a thread of their own. The class socket is bound in its
        // context, so that runtime's reactor drives it.
        let (class_runtime, own_class_runtime) = if split {
            match &self.config.class_runtime {
                ClassLaneRuntime::OwnThread => {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .thread_name("cache-mesh-video")
                        .build()?;
                    (
                        Some(runtime.handle().clone()),
                        Some(OwnedRuntime(Some(runtime))),
                    )
                }
                ClassLaneRuntime::Handle(handle) => (Some(handle.clone()), None),
            }
        } else {
            (None, None)
        };
        let class_socket = match &class_runtime {
            Some(class_runtime) => {
                let Some(class_addr) = class_lane.peer_socket(local_addr) else {
                    return Err(MeshError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "cache mesh class port offset {class_offset} puts the class socket of {local_addr} beyond the last port"
                        ),
                    )));
                };
                let _context = class_runtime.enter();
                Some(Arc::new(bind_socket(
                    &self.config,
                    class_addr,
                    self.config.class_socket_priority,
                )?))
            }
            None => None,
        };
        let class_local_addr = match &class_socket {
            Some(class_socket) => Some(class_socket.local_addr()?),
            None => None,
        };
        let max_peers = self.config.max_peers.max(1);
        let initial_peers = self
            .config
            .peers
            .iter()
            .copied()
            .filter(|peer| self.config.allowed_peers.contains(peer))
            .take(max_peers)
            .collect::<HashSet<_>>();
        let shared = Arc::new(MeshShared {
            identity: HelloIdentity {
                node_id: self.config.node_id.clone(),
                region: self.config.region.clone(),
                role: self.config.role,
                same_region_only: self.config.same_region_only,
                boot_id: self.config.boot_id,
                build: self.config.build.clone(),
                state: self.config.hello_state.clone(),
            },
            peers: RwLock::new(initial_peers),
            allowed_peers: ArcSwap::from_pointee(self.config.allowed_peers.clone()),
            peer_regions: RwLock::new(HashMap::new()),
            peer_roles: RwLock::new(HashMap::new()),
            peer_draining: StdMutex::new(HashSet::new()),
            peer_hellos: StdMutex::new(HashMap::new()),
            invalid_frame_warnings: StdMutex::default(),
            parents_changed: watch::channel(0).0,
            draining: AtomicBool::new(false),
            subscribed: StdMutex::new(HashMap::new()),
            subscribe_order: tokio::sync::Mutex::new(()),
            warm_total: Default::default(),
            remote_slots: RwLock::new(HashSet::new()),
            counters: Arc::new(CacheMeshFecCounters::default()),
            leases: Arc::new(StdMutex::new(HashMap::new())),
            lease_generation: AtomicU32::new(0),
            served: Arc::new(StdMutex::new(Vec::new())),
            served_changed: watch::channel(0).0,
            peer_seen: Arc::new(StdMutex::new(HashMap::new())),
            wanted: Arc::new(StdMutex::new(HashSet::new())),
            link_loss: Arc::new(StdMutex::new(HashMap::new())),
            measured_loss: Arc::new(StdMutex::new(HashMap::new())),
            // The class lane keeps its own counters, loss measurements and
            // served leases; the handle adds them to the main lane's.
            class: class_local_addr.map(|local_addr| ClassLaneShared {
                local_addr,
                counters: Arc::new(CacheMeshFecCounters::default()),
                measured_loss: Arc::new(StdMutex::new(HashMap::new())),
                served: Arc::new(StdMutex::new(Vec::new())),
                replicas_dropped: Arc::new(AtomicU64::new(0)),
            }),
            handover: LaneHandover::default(),
            #[cfg(test)]
            cursor_streams: StdMutex::new(HashSet::new()),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let (replica_tx, replica_rx) = mpsc::channel(self.config.replica_queue_capacity.max(1));
        // Replica requests for video playlists, which the main receive task
        // hands to the class lane.
        let (class_replicas, class_replicas_rx) = match &shared.class {
            Some(class) => {
                let (tx, rx) = mpsc::channel(self.config.replica_queue_capacity.max(1));
                (
                    Some(ClassReplicas {
                        tx,
                        dropped: Arc::clone(&class.replicas_dropped),
                    }),
                    Some(rx),
                )
            }
            None => (None, None),
        };

        tokio::spawn(receive_task(
            Arc::clone(&socket),
            Arc::clone(&self.cache),
            Arc::clone(&shared),
            self.config.clone(),
            main_lane,
            Some(replica_tx),
            class_replicas,
            shutdown_rx.clone(),
        ));
        tokio::spawn(replica_task(
            Arc::clone(&socket),
            Arc::clone(&self.cache),
            Arc::clone(&shared),
            self.config.clone(),
            replica_rx,
            shutdown_rx.clone(),
        ));
        tokio::spawn(announce_task(
            Arc::clone(&socket),
            Arc::clone(&shared),
            self.config.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(sync_task(
            Arc::clone(&socket),
            Arc::clone(&self.cache),
            Arc::clone(&shared),
            self.config.clone(),
            main_lane,
            shutdown_rx.clone(),
        ));
        if let (Some(class_runtime), Some(class_socket), Some(class_replicas_rx)) = (
            class_runtime.as_ref(),
            class_socket.as_ref(),
            class_replicas_rx,
        ) {
            // The class receive task sends its applied parts to their own
            // channel, if there is one.
            let mut class_config = self.config.clone();
            if let Some(class_applied_tx) = &self.config.class_applied_tx {
                class_config.applied_tx = Some(class_applied_tx.clone());
            }
            class_runtime.spawn(receive_task(
                Arc::clone(class_socket),
                Arc::clone(&self.cache),
                Arc::clone(&shared),
                class_config,
                class_lane,
                None,
                None,
                shutdown_rx.clone(),
            ));
            class_runtime.spawn(class_replica_task(
                Arc::clone(class_socket),
                Arc::clone(&self.cache),
                Arc::clone(&shared),
                self.config.clone(),
                class_lane,
                class_replicas_rx,
                shutdown_rx.clone(),
            ));
            class_runtime.spawn(sync_task(
                Arc::clone(class_socket),
                Arc::clone(&self.cache),
                Arc::clone(&shared),
                self.config.clone(),
                class_lane,
                shutdown_rx.clone(),
            ));
        }
        if let Some(runtime) = own_class_runtime {
            // The thread drives the class lane's tasks until the mesh shuts
            // down (or its handle is dropped), then drops the runtime.
            let mut shutdown_rx = shutdown_rx.clone();
            std::thread::Builder::new()
                .name("cache-mesh-video".into())
                .spawn(move || {
                    if let Some(runtime) = &runtime.0 {
                        runtime.block_on(async move {
                            let _ = shutdown_rx.changed().await;
                        });
                    }
                })?;
        }
        drop(shutdown_rx);

        info!(
            node_id = self.config.node_id,
            region = self.config.region,
            bind = %local_addr,
            class_bind = ?class_local_addr,
            "cache mesh started"
        );

        Ok(CacheMeshHandle {
            shutdown_tx,
            shared,
            socket,
            node_id: self.config.node_id,
            repair_symbols: self.config.repair_symbols,
            repair_ratio: self.config.repair_ratio,
            max_repair_symbols: self.config.max_repair_symbols,
            symbol_size: self.config.symbol_size,
            request_block_id: Arc::new(AtomicU32::new(
                REQUEST_BLOCK_ID_BASE + self.config.block_id_offset,
            )),
            local_addr,
            region: self.config.region,
            same_region_only: self.config.same_region_only,
            role: self.config.role,
            max_parents: self.config.max_parents.max(1),
            max_peers,
            max_frame_bytes: self.config.max_frame_bytes,
            replication: self.config.replication,
            subscription_lease: self.config.subscription_lease,
            peer_timeout: self.config.peer_timeout,
            subscribe_rank: self.config.subscribe_rank,
            release_unranked_parents: self.config.release_unranked_parents,
            avoid_draining_parents: self.config.avoid_draining_parents,
            warm_from_second_parent: self.config.warm_from_second_parent,
            boot_id: self.config.boot_id,
            parent_release_after: self
                .config
                .parent_release_after
                .unwrap_or(self.config.subscription_lease * 2 / 3),
            frame_form: self.config.frame_form,
        })
    }
}

/// The largest frame that a node with this cache sends or accepts: its
/// largest part or initialization, and the head of the frame.
fn frame_bytes_for(cache: &ChunkCache) -> usize {
    cache
        .options
        .buffer_size_kb
        .max(cache.options.init_size_kb)
        .saturating_mul(1024)
        .saturating_add(FRAME_OVERHEAD_BYTES)
}

/// The remote slots that a node with this cache remembers at most: every
/// slot that the cache retains, and one prune interval of parts at
/// 4,000 parts a second per stream on top.
fn remote_slots_for(cache: &ChunkCache) -> usize {
    let streams = cache.options.num_playlists.max(1);
    let retained = streams
        .saturating_mul(cache.options.max_segments)
        .saturating_mul(cache.options.max_parts_per_segment);
    let per_interval = REMOTE_SLOT_PRUNE_INTERVAL.as_millis() as usize * 4;
    retained
        .saturating_add(streams.saturating_mul(per_interval))
        .max(1)
}

/// What a node's hellos say of it, besides its peers and whether it is
/// draining.
struct HelloIdentity {
    node_id: String,
    region: String,
    role: CacheMeshRole,
    same_region_only: bool,
    boot_id: u64,
    build: Option<String>,
    state: Option<Arc<AtomicU8>>,
}

/// State that the tasks of one mesh node and its handle share.
struct MeshShared {
    identity: HelloIdentity,
    peers: RwLock<HashSet<SocketAddr>>,
    /// Read once for every datagram, so without a lock.
    allowed_peers: ArcSwap<HashSet<SocketAddr>>,
    peer_regions: RwLock<HashMap<SocketAddr, String>>,
    peer_roles: RwLock<HashMap<SocketAddr, WireRole>>,
    /// Peers whose newest hello says that they are draining.
    peer_draining: StdMutex<HashSet<SocketAddr>>,
    /// The newest hello of each peer in the peer table.
    peer_hellos: StdMutex<HashMap<SocketAddr, PeerHello>>,
    /// When each peer was last named for a frame that did not decode, on
    /// either socket: a peer is named once in
    /// [`INVALID_FRAME_WARN_INTERVAL`] however many lanes hear it. Taken
    /// only on a decode error.
    invalid_frame_warnings: StdMutex<InvalidFrameWarnings>,
    /// A count that goes up when a peer that this node may take playlists
    /// from is new, has restarted, returns from a silence or changes
    /// whether it drains (`CacheMeshHandle::parents_changed`).
    parents_changed: watch::Sender<u64>,
    /// This node is draining: its hellos say so (`CacheMeshHandle::set_draining`).
    draining: AtomicBool,
    /// The parents of each playlist that this node subscribes to: those
    /// that its last subscribe ranked, and those waiting to be released.
    subscribed: StdMutex<HashMap<u64, StreamParents>>,
    /// Held by a subscribe, an unsubscribe and a release of pending parents
    /// from their first look at `subscribed` to their last frame, so that
    /// the frames of two of them never cross: a parent that one call ranks
    /// again is not told to stop by another that saw it pending.
    subscribe_order: tokio::sync::Mutex<()>,
    /// The node's total warm-up allowance across its children, one for
    /// each lane (`CacheMeshConfig::backfill_total_bytes_per_sec`): the
    /// video lane's 4K warm-ups do not starve the audio lane's (wave 8
    /// review), indexed by [`Lane::is_class`].
    warm_total: [StdMutex<WarmBucket>; 2],
    /// Slots that came from a peer, by stream lifetime (the handle: stream,
    /// index and generation; a generation alone is per index) and slot.
    remote_slots: RwLock<HashSet<(StreamHandle, usize)>>,
    counters: Arc<CacheMeshFecCounters>,
    leases: LeaseTable,
    /// The generation of the next lease that this node opens.
    lease_generation: AtomicU32,
    served: ServedTable,
    /// A count that goes up whenever the leases that either lane serves
    /// change (a child or a playlist comes or goes), so that a consumer of
    /// the served leases need not poll them.
    served_changed: watch::Sender<u64>,
    peer_seen: PeerSeen,
    wanted: WantedStreams,
    link_loss: LinkLossTable,
    measured_loss: MeasuredLossTable,
    /// The class lane of a mesh split by class.
    class: Option<ClassLaneShared>,
    /// Which lane sends each playlist of a mesh split by class.
    handover: LaneHandover,
    /// The streams that the sync task kept cursors or initializations for
    /// at its last prune.
    #[cfg(test)]
    cursor_streams: StdMutex<HashSet<StreamHandle>>,
}

/// The class socket of a mesh split by class, and what its lane keeps apart
/// from the main lane: counters, loss measurements and served leases. The
/// handle adds them to the main lane's.
struct ClassLaneShared {
    local_addr: SocketAddr,
    counters: Arc<CacheMeshFecCounters>,
    measured_loss: MeasuredLossTable,
    served: ServedTable,
    /// Replica requests for video playlists dropped because the class lane's
    /// queue was full.
    replicas_dropped: Arc<AtomicU64>,
}

impl ClassLaneShared {
    /// The class lane's counts, with the requests that its full queue
    /// dropped among the replica queue drops.
    fn snapshot(&self) -> CacheMeshFecStats {
        let mut stats = self.counters.snapshot();
        stats.replica_queue_drops += self.replicas_dropped.load(Ordering::Relaxed);
        stats
    }
}

impl MeshShared {
    fn is_allowed(&self, peer: &SocketAddr) -> bool {
        self.allowed_peers.load().contains(peer)
    }

    /// This node's hello as it stands now: its identity, the peers that it
    /// gossips (a leaf gossips none) and whether it is draining.
    async fn hello(&self) -> MeshFrame {
        let identity = &self.identity;
        let peers = if identity.role == CacheMeshRole::Edge {
            Vec::new()
        } else {
            // One lock at a time: the sync task takes these two in the
            // other order.
            let mut peers = self.peers.read().await.iter().copied().collect::<Vec<_>>();
            let peer_regions = self.peer_regions.read().await;
            peers.retain(|peer| {
                !identity.same_region_only
                    || peer_regions
                        .get(peer)
                        .is_none_or(|region| region == &identity.region)
            });
            peers.truncate(MAX_GOSSIP_PEERS);
            peers
        };
        MeshFrame::Hello {
            node_id: identity.node_id.clone(),
            region: identity.region.clone(),
            role: WireRole::Known(identity.role),
            peers,
            draining: self.draining.load(Ordering::Relaxed),
            boot_id: Some(identity.boot_id),
            build: identity.build.clone(),
            state: identity
                .state
                .as_ref()
                .map(|state| state.load(Ordering::Relaxed)),
        }
    }

    /// The peer that sent a datagram from `from` to the socket of `lane`, if
    /// it is authorized. A peer's class socket is authorized with the peer:
    /// the allowlist holds main addresses, and a datagram on the class socket
    /// is taken as one from the peer at its port minus the class offset.
    fn authorized_sender(&self, lane: Lane, from: SocketAddr) -> Option<SocketAddr> {
        let peer = lane.peer_of(from)?;
        self.is_allowed(&peer).then_some(peer)
    }

    /// The counters of a lane.
    fn counters_of(&self, lane: Lane) -> &Arc<CacheMeshFecCounters> {
        match &self.class {
            Some(class) if lane.is_class() => &class.counters,
            _ => &self.counters,
        }
    }

    /// The loss that a lane measures from each peer.
    fn measured_loss_of(&self, lane: Lane) -> &MeasuredLossTable {
        match &self.class {
            Some(class) if lane.is_class() => &class.measured_loss,
            _ => &self.measured_loss,
        }
    }

    /// The leases that a lane serves.
    fn served_of(&self, lane: Lane) -> &ServedTable {
        match &self.class {
            Some(class) if lane.is_class() => &class.served,
            _ => &self.served,
        }
    }

    /// The loss measured on both lanes from each peer: the worse of the two.
    fn measured_loss(&self) -> HashMap<SocketAddr, u32> {
        merged_measured_loss(
            &self.measured_loss,
            self.class.as_ref().map(|class| &class.measured_loss),
        )
    }

    /// Authorize a peer. The allowlist only grows.
    fn allow(&self, peer: SocketAddr) {
        if self.is_allowed(&peer) {
            return;
        }
        self.allowed_peers.rcu(|allowed| {
            let mut allowed = HashSet::clone(allowed);
            allowed.insert(peer);
            allowed
        });
    }
}

pub struct CacheMeshHandle {
    shutdown_tx: watch::Sender<()>,
    shared: Arc<MeshShared>,
    socket: Arc<UdpSocket>,
    node_id: String,
    repair_symbols: u32,
    repair_ratio: f32,
    max_repair_symbols: u32,
    symbol_size: u16,
    request_block_id: Arc<AtomicU32>,
    local_addr: SocketAddr,
    region: String,
    same_region_only: bool,
    role: CacheMeshRole,
    max_parents: usize,
    max_peers: usize,
    max_frame_bytes: usize,
    replication: ReplicationMode,
    subscription_lease: Duration,
    peer_timeout: Duration,
    subscribe_rank: bool,
    release_unranked_parents: bool,
    avoid_draining_parents: bool,
    warm_from_second_parent: bool,
    boot_id: u64,
    parent_release_after: Duration,
    frame_form: FrameForm,
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
        let measured = self.shared.measured_loss();
        let reported = lock(&self.shared.link_loss).clone();
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
        .encode_as(self.frame_form)?;
        self.send_control(&peers, &frame).await
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn peers(&self) -> Vec<SocketAddr> {
        self.shared.peers.read().await.iter().copied().collect()
    }

    /// Add and authorize a peer. Refused when the peer table is full; a
    /// refused peer is not authorized.
    pub async fn add_peer(&self, peer: SocketAddr) -> bool {
        if peer == self.local_addr {
            return false;
        }
        let mut peers = self.shared.peers.write().await;
        if !peers.contains(&peer) && peers.len() >= self.max_peers {
            self.shared
                .counters
                .peer_limit_drops
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.shared.allow(peer);
        peers.insert(peer)
    }

    /// The counts of every socket of the mesh together.
    pub fn fec_stats(&self) -> CacheMeshFecStats {
        let main = self.shared.counters.snapshot();
        match &self.shared.class {
            Some(class) => main.plus(class.snapshot()),
            None => main,
        }
    }

    /// The counts of the class socket alone, when the mesh is split by class.
    pub fn class_fec_stats(&self) -> Option<CacheMeshFecStats> {
        self.shared.class.as_ref().map(ClassLaneShared::snapshot)
    }

    /// The address of the class socket, when the mesh is split by class.
    pub fn class_local_addr(&self) -> Option<SocketAddr> {
        self.shared.class.as_ref().map(|class| class.local_addr)
    }

    /// Replica requests for video playlists that were dropped because the
    /// class lane already had a full queue of them to serve.
    pub fn class_replica_requests_dropped(&self) -> u64 {
        self.shared
            .class
            .as_ref()
            .map_or(0, |class| class.replicas_dropped.load(Ordering::Relaxed))
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
            .encode_as(self.frame_form)?;
        self.send_control(&peers, &frame).await
    }

    /// Subscribe to one playlist at its parents, or renew the subscription.
    ///
    /// A parent that runs demand replication pushes the playlist only while
    /// the lease is current, so call this again within the lease. A new lease
    /// first receives a warm-up window of at most `backfill_parts` parts that
    /// ends at the newest part and does not start before `from_slot`.
    /// Returns the ranked parents, which were asked.
    ///
    /// A change of parents is make before break. A parent that has left the
    /// playlist's ranked set (a parent that ranks above it joined, or it is
    /// draining) is not told to stop here: it waits to be released, and
    /// each call renews its lease, without a warm-up window and with a rank
    /// after the ranked parents, so that it keeps feeding this node.
    /// [`Self::release_pending`] releases it. A waiting parent that is
    /// ranked again waits no longer.
    pub async fn subscribe(
        &self,
        stream_id: u64,
        from_slot: usize,
        backfill_parts: usize,
    ) -> Result<Vec<SocketAddr>, MeshError> {
        let _order = self.shared.subscribe_order.lock().await;
        lock(&self.shared.wanted).insert(stream_id);
        let (peers, ranked) = self.parents_ranked(stream_id).await;
        if peers.is_empty() {
            return Ok(peers);
        }
        let lease_ms = u32::try_from(self.subscription_lease.as_millis()).unwrap_or(u32::MAX);
        // Every parent pushes the live parts. Only the first parent sends the
        // warm-up window, so a new copy is not warmed twice over; with more
        // live parents than are asked (a slot joined, and may be the first
        // with a nearly empty window), the second one too.
        let warm_parents = if self.warm_from_second_parent && ranked > peers.len() {
            2
        } else {
            1
        };
        // The parents that left the ranked set wait to be released; those
        // that wait and are not overdue are asked to go on.
        let now = Instant::now();
        let waiting = {
            let mut subscribed = lock(&self.shared.subscribed);
            let parents = subscribed.entry(stream_id).or_default();
            let previous = std::mem::replace(&mut parents.ranked, peers.clone());
            parents.pending.retain(|(peer, _)| !peers.contains(peer));
            if self.release_unranked_parents {
                for peer in previous {
                    if !peers.contains(&peer)
                        && !parents.pending.iter().any(|(waiting, _)| *waiting == peer)
                    {
                        parents.pending.push((peer, now));
                    }
                }
            }
            // An overdue parent is not renewed: if nothing releases it, its
            // lease lapses.
            parents
                .pending
                .iter()
                .filter(|(_, since)| now.duration_since(*since) < self.parent_release_after)
                .map(|(peer, _)| *peer)
                .collect::<Vec<_>>()
        };
        for (index, peer) in peers.iter().chain(&waiting).enumerate() {
            let backfill_parts = if index < warm_parents.min(peers.len()) {
                backfill_parts
            } else {
                0
            };
            let frame = MeshFrame::Subscribe {
                node_id: self.node_id.clone(),
                stream_id,
                from_slot: from_slot as u64,
                backfill_parts: u32::try_from(backfill_parts).unwrap_or(u32::MAX),
                lease_ms,
                rank: self
                    .subscribe_rank
                    .then(|| u8::try_from(index).unwrap_or(SUBSCRIBE_RANK_NONE - 1)),
                boot_id: Some(self.boot_id),
            }
            .encode_as(self.frame_form)?;
            self.send_control(std::slice::from_ref(peer), &frame)
                .await?;
        }
        Ok(peers)
    }

    /// Release the parents that wait for it (see [`Self::subscribe`]): tell
    /// each to stop (Unsubscribe).
    ///
    /// A waiting parent of a playlist is released when `confirmed(stream_id,
    /// parent)` is true of every ranked parent of the playlist (each is
    /// seen delivering it, so the one that left is no longer needed), or
    /// when it has waited for `parent_release_after`
    /// ([`CacheMeshConfig::with_parent_release_after`]): a parent must be
    /// left in the end, whether or not its replacement delivers. Call it
    /// often (every 100 ms); a node that cannot tell whether a parent
    /// delivers passes `&|_, _| false` and gets the timed release.
    ///
    /// Returns the playlist and address of each parent released, and
    /// whether its replacement was confirmed. `parents_released` counts
    /// them all, `parents_released_unconfirmed` those released on time
    /// alone.
    pub async fn release_pending(
        &self,
        confirmed: &(dyn Fn(u64, SocketAddr) -> bool + Send + Sync),
    ) -> Vec<(u64, SocketAddr, bool)> {
        let _order = self.shared.subscribe_order.lock().await;
        // What waits, read under the lock; `confirmed` runs outside it.
        let waiting = {
            let subscribed = lock(&self.shared.subscribed);
            subscribed
                .iter()
                .filter(|(_, parents)| !parents.pending.is_empty())
                .map(|(stream_id, parents)| {
                    (*stream_id, parents.ranked.clone(), parents.pending.clone())
                })
                .collect::<Vec<_>>()
        };
        let now = Instant::now();
        let mut released = Vec::new();
        for (stream_id, ranked, pending) in waiting {
            let replaced = !ranked.is_empty()
                && ranked
                    .iter()
                    .all(|parent| confirmed(stream_id, *parent));
            let due = pending
                .into_iter()
                .filter(|(_, since)| {
                    replaced || now.duration_since(*since) >= self.parent_release_after
                })
                .map(|(parent, _)| parent)
                .collect::<Vec<_>>();
            if due.is_empty() {
                continue;
            }
            let frame = match (MeshFrame::Unsubscribe {
                node_id: self.node_id.clone(),
                stream_id,
            })
            .encode_as(self.frame_form)
            {
                Ok(frame) => frame,
                Err(err) => {
                    debug!(stream_id, error = %err, "cache mesh unsubscribe encode failed");
                    continue;
                }
            };
            for parent in due {
                // A parent that could not be told keeps waiting.
                if let Err(err) = self
                    .send_control(std::slice::from_ref(&parent), &frame)
                    .await
                {
                    debug!(peer = %parent, stream_id, error = %err, "cache mesh parent release failed");
                    continue;
                }
                if let Some(parents) = lock(&self.shared.subscribed).get_mut(&stream_id) {
                    parents.pending.retain(|(waiting, _)| *waiting != parent);
                }
                let counters = &self.shared.counters;
                counters.parents_released.fetch_add(1, Ordering::Relaxed);
                if !replaced {
                    counters
                        .parents_released_unconfirmed
                        .fetch_add(1, Ordering::Relaxed);
                }
                released.push((stream_id, parent, replaced));
            }
        }
        released
    }

    /// The parents that left a playlist's ranked set and are not released
    /// yet: the playlist and the parent's address, sorted.
    pub fn pending_parents(&self) -> Vec<(u64, SocketAddr)> {
        let mut pending = lock(&self.shared.subscribed)
            .iter()
            .flat_map(|(stream_id, parents)| {
                parents.pending.iter().map(|(parent, _)| (*stream_id, *parent))
            })
            .collect::<Vec<_>>();
        pending.sort_unstable();
        pending
    }

    /// A watch that changes when a peer that this node may take playlists
    /// from is new, has restarted (the boot id in its hello changed), is
    /// heard again after more than `peer_timeout` without a hello, or
    /// changes whether it is draining. Such a parent holds no lease of this
    /// node, or should hold none: greet the parents
    /// ([`Self::greet_parents`]) and subscribe every playlist again now,
    /// not at the next renewal. The value is a count with no other
    /// meaning.
    pub fn parents_changed(&self) -> watch::Receiver<u64> {
        self.shared.parents_changed.subscribe()
    }

    /// Send this node's hello now to every peer that it may take playlists
    /// from, and return how many were sent. A parent that has just started
    /// knows no region for this node until a hello of it arrives, and one
    /// that keeps to its region drops a Subscribe from a peer of unknown
    /// region without a word: greet first, then subscribe. The hello goes
    /// out on the socket and in the order of the subscribes that follow.
    pub async fn greet_parents(&self) -> Result<usize, MeshError> {
        let peers = self.allowed_peers().await;
        if peers.is_empty() {
            return Ok(0);
        }
        let frame = self.shared.hello().await.encode_as(self.frame_form)?;
        self.send_control(&peers, &frame).await
    }

    /// The peers of this node, each as its newest hello described it,
    /// sorted by address.
    pub async fn peer_statuses(&self) -> Vec<CacheMeshPeer> {
        let mut peers = self.peers().await;
        peers.sort_unstable();
        let parents = self.allowed_peers().await;
        let roles = self.shared.peer_roles.read().await.clone();
        let hellos = lock(&self.shared.peer_hellos);
        let draining = lock(&self.shared.peer_draining);
        let seen = lock(&self.shared.peer_seen);
        let now = Instant::now();
        peers
            .into_iter()
            .map(|peer| {
                let hello = hellos.get(&peer);
                CacheMeshPeer {
                    peer,
                    role: match roles.get(&peer) {
                        Some(WireRole::Known(role)) => Some(*role),
                        Some(WireRole::Other(_)) | None => None,
                    },
                    boot_id: hello.and_then(|hello| hello.boot_id),
                    build: hello.and_then(|hello| hello.build.clone()),
                    state: hello.and_then(|hello| hello.state),
                    draining: draining.contains(&peer),
                    parent: parents.binary_search(&peer).is_ok(),
                    heard: seen.get(&peer).map(|last| now.duration_since(*last)),
                }
            })
            .collect()
    }

    /// End the subscription to one playlist. Parents stop pushing it, and
    /// this node no longer accepts its parts.
    pub async fn unsubscribe(&self, stream_id: u64) -> Result<usize, MeshError> {
        let _order = self.shared.subscribe_order.lock().await;
        if !lock(&self.shared.wanted).remove(&stream_id) {
            return Ok(0);
        }
        // With it go the parents that waited to be released: every allowed
        // parent is told, because the parents of the playlist can have
        // changed since the subscription was made.
        lock(&self.shared.subscribed).remove(&stream_id);
        let peers = self.allowed_peers().await;
        if peers.is_empty() {
            return Ok(0);
        }
        let frame = MeshFrame::Unsubscribe {
            node_id: self.node_id.clone(),
            stream_id,
        }
        .encode_as(self.frame_form)?;
        self.send_control(&peers, &frame).await
    }

    /// Say in this node's hellos that it is draining (or no longer is): its
    /// children rank it after their other live parents and move their
    /// subscriptions away at their next renewal.
    pub fn set_draining(&self, draining: bool) {
        self.shared.draining.store(draining, Ordering::Relaxed);
    }

    pub fn is_draining(&self) -> bool {
        self.shared.draining.load(Ordering::Relaxed)
    }

    /// The peers whose newest hello says that they are draining.
    pub fn draining_peers(&self) -> Vec<SocketAddr> {
        let mut peers = lock(&self.shared.peer_draining)
            .iter()
            .copied()
            .collect::<Vec<_>>();
        peers.sort_unstable();
        peers
    }

    /// Playlists that this node currently subscribes to.
    pub fn subscribed_streams(&self) -> Vec<u64> {
        let mut streams = lock(&self.shared.wanted)
            .iter()
            .copied()
            .collect::<Vec<_>>();
        streams.sort_unstable();
        streams
    }

    /// Subscription leases that this node currently serves to children.
    pub fn served_subscriptions(&self) -> Vec<ServedSubscription> {
        let mut served = lock(&self.shared.served).clone();
        if let Some(class) = &self.shared.class {
            // Each lane lists the leases of its own class. A playlist that
            // has just changed class can be in both lists for a pass.
            for subscription in lock(&class.served).iter() {
                if !served.iter().any(|main| {
                    (main.child, main.stream_id) == (subscription.child, subscription.stream_id)
                }) {
                    served.push(*subscription);
                }
            }
        }
        served
    }

    /// A watch that changes whenever the leases of
    /// [`Self::served_subscriptions`] change: a child or a playlist comes or
    /// goes, by a new lease, an Unsubscribe or an expiry. The leases are
    /// looked at once a sync tick, so a change is seen within one
    /// `sync_interval`. The value is a count with no other meaning.
    pub fn served_changed(&self) -> watch::Receiver<u64> {
        self.shared.served_changed.subscribe()
    }

    pub fn replication(&self) -> ReplicationMode {
        self.replication
    }

    pub fn subscription_lease(&self) -> Duration {
        self.subscription_lease
    }

    /// Slots that this node took from a peer and still remembers. It is at
    /// most the slots that its cache retains.
    pub async fn remote_slot_count(&self) -> usize {
        self.shared.remote_slots.read().await.len()
    }

    /// How long ago this node last heard from any peer that it may take
    /// playlists from. `None` before the first frame of such a peer.
    ///
    /// An edge that has heard no parent for a few hello intervals has lost
    /// its network, or every parent: it cannot say when the next part will
    /// come, and should send its readers to another cache.
    pub async fn parents_silent_for(&self) -> Option<Duration> {
        let peers = self.allowed_peers().await;
        let seen = lock(&self.shared.peer_seen);
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
        self.parents_ranked(stream_id).await.0
    }

    /// [`Self::parents_for`], and how many live parents were ranked before
    /// the list was cut to `max_parents` (0 before the first hello, when
    /// the configured parents are asked: a slot that has not started is
    /// not counted).
    async fn parents_ranked(&self, stream_id: u64) -> (Vec<SocketAddr>, usize) {
        let mut peers = self.allowed_peers().await;
        let alive = {
            let seen = lock(&self.shared.peer_seen);
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
        let live = !alive.is_empty();
        if live {
            peers = alive;
        }
        rank_parents(&self.node_id, stream_id, &mut peers);
        // A draining parent goes after every parent that is not: its
        // children move away from it while it still serves them.
        if self.avoid_draining_parents {
            let draining = lock(&self.shared.peer_draining);
            if !draining.is_empty() {
                peers.sort_by_key(|peer| draining.contains(peer));
            }
        }
        let ranked = if live { peers.len() } else { 0 };
        peers.truncate(self.max_parents.max(1));
        (peers, ranked)
    }

    async fn send_control(&self, peers: &[SocketAddr], frame: &[u8]) -> Result<usize, MeshError> {
        // The sender numbers one block per peer: reserve them all, so the
        // next call does not send a peer a block id it has already taken.
        let blocks = u32::try_from(peers.len()).unwrap_or(u32::MAX);
        let block_id = self.request_block_id.fetch_add(blocks, Ordering::Relaxed);
        let mut sender = FecSender::with_policy_and_initial_block_id(
            self.repair_symbols,
            self.repair_ratio,
            self.max_repair_symbols,
            self.symbol_size,
            block_id,
            Arc::clone(&self.shared.counters),
        )
        .with_link_loss(Arc::clone(&self.shared.link_loss))
        .with_max_frame_bytes(self.max_frame_bytes);
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
        let peers = self
            .shared
            .peers
            .read()
            .await
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let regions = self.shared.peer_regions.read().await;
        let roles = self.shared.peer_roles.read().await;
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
                WireRole::may_be_parent(roles.get(peer).copied(), self.role)
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
        role: WireRole,
        peers: Vec<SocketAddr>,
        /// The sender is draining (the top bit of the role byte).
        draining: bool,
        /// The start of the sender's process in Unix microseconds
        /// (extension `0x01`).
        boot_id: Option<u64>,
        /// The sender's build id (extension `0x02`).
        build: Option<String>,
        /// The sender's state (extension `0x03`).
        state: Option<u8>,
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
        /// The parent's place in the child's ranking of the playlist's
        /// parents (0: first), when the child says: one byte after the
        /// lease, 255 for none.
        rank: Option<u8>,
        /// The start of the child's process in Unix microseconds, after the
        /// rank: a Subscribe with another boot id than the lease's comes
        /// from a restarted child.
        boot_id: Option<u64>,
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
        Self::hello_with_role(node_id, region, CacheMeshRole::Peer, peers)
    }

    #[cfg(test)]
    fn hello_with_role(
        node_id: impl Into<String>,
        region: impl Into<String>,
        role: CacheMeshRole,
        peers: Vec<SocketAddr>,
    ) -> Self {
        Self::Hello {
            node_id: node_id.into(),
            region: region.into(),
            role: WireRole::Known(role),
            peers,
            draining: false,
            boot_id: None,
            build: None,
            state: None,
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

    /// Whether this build reads frames of `kind`.
    fn knows_kind(kind: u8) -> bool {
        matches!(
            kind,
            FRAME_HELLO
                | FRAME_CHUNK
                | FRAME_REPLICA_REQUEST
                | FRAME_INITIALIZATION
                | FRAME_SUBSCRIBE
                | FRAME_UNSUBSCRIBE
                | FRAME_LINK_REPORT
        )
    }

    /// The frame as this build writes it.
    #[cfg(test)]
    fn encode(&self) -> Result<Bytes, MeshError> {
        self.encode_as(FrameForm::Current)
    }

    fn encode_as(&self, form: FrameForm) -> Result<Bytes, MeshError> {
        let node_id = self.node_id().as_bytes();
        if node_id.len() > u16::MAX as usize {
            return Err(MeshError::InvalidFrame("node id too long"));
        }

        let mut out = BytesMut::new();
        out.put_slice(FRAME_MAGIC);
        out.put_u8(form.version());
        match self {
            Self::Hello {
                region,
                role,
                peers,
                draining,
                boot_id,
                build,
                state,
                ..
            } => {
                let region = region.as_bytes();
                if region.len() > u16::MAX as usize {
                    return Err(MeshError::InvalidFrame("region too long"));
                }
                if peers.len() > MAX_GOSSIP_PEERS {
                    return Err(MeshError::InvalidFrame("too many gossip peers"));
                }
                if build
                    .as_ref()
                    .is_some_and(|build| build.len() > HELLO_BUILD_MAX_BYTES)
                {
                    return Err(MeshError::InvalidFrame("build id too long"));
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
                out.put_u8(role.wire_value() | if *draining { HELLO_DRAINING_BIT } else { 0 });
                if let Some(boot_id) = boot_id {
                    out.put_u8(HELLO_EXTENSION_BOOT_ID);
                    out.put_u16(8);
                    out.put_u64(*boot_id);
                }
                if let Some(build) = build {
                    out.put_u8(HELLO_EXTENSION_BUILD);
                    out.put_u16(build.len() as u16);
                    out.put_slice(build.as_bytes());
                }
                if let Some(state) = state {
                    out.put_u8(HELLO_EXTENSION_STATE);
                    out.put_u16(1);
                    out.put_u8(*state);
                }
                if form == FrameForm::Future {
                    // Five bytes that this build skips.
                    out.put_u8(FUTURE_HELLO_EXTENSION);
                    out.put_u16(2);
                    out.put_u16(0);
                }
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
                out.put_u32(*count);
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
                rank,
                boot_id,
                ..
            } => {
                out.put_u8(FRAME_SUBSCRIBE);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u64(*stream_id);
                out.put_u64(*from_slot);
                out.put_u32(*backfill_parts);
                out.put_u32(*lease_ms);
                // A node always sends both the rank and its boot id. What
                // follows them is unknown to this build, so the three bytes
                // of the later form come only after both.
                let boot_id = match form {
                    FrameForm::Current => *boot_id,
                    FrameForm::Future => Some(boot_id.unwrap_or(0)),
                };
                match (rank, boot_id) {
                    (rank, Some(boot_id)) => {
                        out.put_u8(rank.unwrap_or(SUBSCRIBE_RANK_NONE));
                        out.put_u64(boot_id);
                    }
                    (Some(rank), None) => out.put_u8(*rank),
                    (None, None) => {}
                }
                if form == FrameForm::Future {
                    out.put_slice(&[FUTURE_HELLO_EXTENSION; 3]);
                }
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

    /// A frame of a kind that no build defines, as a node that speaks like a
    /// later build sends it. Its body is not a node id: a receiver that read
    /// it as one would refuse the frame as invalid.
    fn of_unknown_kind() -> Bytes {
        let mut out = BytesMut::with_capacity(FRAME_MAGIC.len() + 10);
        out.put_slice(FRAME_MAGIC);
        out.put_u8(FUTURE_FRAME_VERSION);
        out.put_u8(FUTURE_FRAME_KIND);
        out.put_slice(&[0xff; 8]);
        out.freeze()
    }

    /// Decode a frame. The payload of a chunk or initialization frame is a
    /// slice of `bytes`, not a copy.
    ///
    /// A frame is read as far as this build knows it, so that two builds
    /// interoperate for the length of a roll: any version from this build's
    /// on is accepted (the version is the newest revision that the sender
    /// speaks; an incompatible change takes a new magic or a new kind, never
    /// a version test), every kind reads the fields it knows and ignores
    /// what follows them, and a kind that this build does not know is
    /// [`MeshError::UnknownKind`], found before anything of its body is read.
    fn decode(bytes: Bytes) -> Result<Self, MeshError> {
        if bytes.len() < FRAME_MAGIC.len() + 2 {
            return Err(MeshError::InvalidFrame("frame too short"));
        }
        let mut buf = bytes;
        if &buf[..FRAME_MAGIC.len()] != FRAME_MAGIC {
            return Err(MeshError::InvalidFrame("bad magic"));
        }
        buf.advance(FRAME_MAGIC.len());
        if buf.get_u8() < FRAME_VERSION {
            return Err(MeshError::InvalidFrame("unsupported version"));
        }
        let kind = buf.get_u8();
        if !Self::knows_kind(kind) {
            return Err(MeshError::UnknownKind(kind));
        }
        let node_id = read_string(&mut buf)?;
        match kind {
            FRAME_HELLO => {
                let region = read_string(&mut buf)?;
                if buf.remaining() < 2 {
                    return Err(MeshError::InvalidFrame("truncated hello peer count"));
                }
                let peer_count = buf.get_u16() as usize;
                if peer_count > MAX_GOSSIP_PEERS {
                    return Err(MeshError::InvalidFrame("too many gossip peers"));
                }
                let mut peers = Vec::with_capacity(peer_count);
                for _ in 0..peer_count {
                    let peer = read_string(&mut buf)?
                        .parse::<SocketAddr>()
                        .map_err(|_| MeshError::InvalidFrame("invalid gossip peer addr"))?;
                    peers.push(peer);
                }
                if !buf.has_remaining() {
                    return Err(MeshError::InvalidFrame("hello without its role"));
                }
                let role_byte = buf.get_u8();
                let mut boot_id = None;
                let mut build = None;
                let mut state = None;
                // The extensions: tag, length, value. One that is cut short
                // ends the list; one that this build does not know, or whose
                // value it cannot read, is skipped.
                while buf.remaining() >= 3 {
                    let tag = buf.get_u8();
                    let len = buf.get_u16() as usize;
                    if buf.remaining() < len {
                        break;
                    }
                    let value = buf.split_to(len);
                    match tag {
                        HELLO_EXTENSION_BOOT_ID if len >= 8 => {
                            boot_id = Some((&value[..]).get_u64());
                        }
                        HELLO_EXTENSION_BUILD if len <= HELLO_BUILD_MAX_BYTES => {
                            build = std::str::from_utf8(&value).ok().map(str::to_string);
                        }
                        HELLO_EXTENSION_STATE if len >= 1 => state = Some(value[0]),
                        _ => {}
                    }
                }
                Ok(Self::Hello {
                    node_id,
                    region,
                    role: CacheMeshRole::from_wire(role_byte),
                    peers,
                    draining: role_byte & HELLO_DRAINING_BIT != 0,
                    boot_id,
                    build,
                    state,
                })
            }
            FRAME_CHUNK => {
                if buf.remaining() < 20 {
                    return Err(MeshError::InvalidFrame("chunk header too short"));
                }
                let stream_id = buf.get_u64();
                let slot_id = buf.get_u64();
                let payload_len = buf.get_u32() as usize;
                if buf.remaining() < payload_len {
                    return Err(MeshError::InvalidFrame("chunk payload cut short"));
                }
                let payload = buf.split_to(payload_len);
                Ok(Self::Chunk {
                    node_id,
                    stream_id,
                    slot_id,
                    payload,
                })
            }
            FRAME_REPLICA_REQUEST => {
                if buf.remaining() < 20 {
                    return Err(MeshError::InvalidFrame("replica request too short"));
                }
                let stream_id = buf.get_u64();
                let from_slot = buf.get_u64();
                let count = buf.get_u32();
                Ok(Self::ReplicaRequest {
                    node_id,
                    stream_id,
                    from_slot,
                    count,
                })
            }
            FRAME_LINK_REPORT => {
                if buf.remaining() < 4 {
                    return Err(MeshError::InvalidFrame("link report too short"));
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
                if buf.remaining() < payload_len {
                    return Err(MeshError::InvalidFrame("initialization payload cut short"));
                }
                let payload = buf.split_to(payload_len);
                Ok(Self::Initialization {
                    node_id,
                    stream_id,
                    payload,
                })
            }
            FRAME_SUBSCRIBE => {
                if buf.remaining() < 24 {
                    return Err(MeshError::InvalidFrame("subscribe too short"));
                }
                let has_rank = buf.remaining() >= 25;
                let has_boot_id = buf.remaining() >= 33;
                let stream_id = buf.get_u64();
                let from_slot = buf.get_u64();
                let backfill_parts = buf.get_u32();
                let lease_ms = buf.get_u32();
                let rank = has_rank
                    .then(|| buf.get_u8())
                    .filter(|rank| *rank != SUBSCRIBE_RANK_NONE);
                let boot_id = has_boot_id.then(|| buf.get_u64());
                Ok(Self::Subscribe {
                    node_id,
                    stream_id,
                    from_slot,
                    backfill_parts,
                    lease_ms,
                    rank,
                    boot_id,
                })
            }
            FRAME_UNSUBSCRIBE => {
                if buf.remaining() < 8 {
                    return Err(MeshError::InvalidFrame("unsubscribe too short"));
                }
                let stream_id = buf.get_u64();
                Ok(Self::Unsubscribe { node_id, stream_id })
            }
            _ => Err(MeshError::UnknownKind(kind)),
        }
    }
}

fn read_string(buf: &mut Bytes) -> Result<String, MeshError> {
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
    /// Largest frame that this sender encodes.
    max_frame_bytes: usize,
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

    fn with_max_symbol_size(mut self, max_symbol_size: u16) -> Self {
        self.max_symbol_size = max_symbol_size;
        self
    }

    fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes.max(1);
        self
    }

    /// Refuse a frame larger than the configured limit.
    fn check_frame_len(&self, frame_len: usize) -> Result<(), MeshError> {
        if frame_len > self.max_frame_bytes {
            self.stats.tx_errors.fetch_add(1, Ordering::Relaxed);
            return Err(MeshError::InvalidFrame(
                "mesh frame exceeds configured limit",
            ));
        }
        Ok(())
    }

    /// Symbol size for one object: the configured size, or a multiple of it
    /// that keeps a large object within about 64 source symbols.
    fn symbol_size_for(&self, frame_len: usize) -> u16 {
        self.symbol_size_within(frame_len, ADAPTIVE_SOURCE_SYMBOLS)
    }

    /// Symbol size that keeps an object within about `source_symbols`.
    ///
    /// An object smaller than `symbol_size`, and of at most 1200 bytes (the
    /// symbol of an edge's UDP cut-through), goes in one symbol of its own
    /// length, not padded to `symbol_size`: a 2.5 ms Opus part is one
    /// datagram of about 100 bytes and one repair datagram of the same size,
    /// not two of 1352. Receivers take the symbol size from each block's
    /// header.
    fn symbol_size_within(&self, frame_len: usize, source_symbols: usize) -> u16 {
        let base = usize::from(self.symbol_size.max(1));
        if frame_len < base && frame_len <= FITTED_SYMBOL_MAX {
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
        .with_max_frame_bytes(config.max_frame_bytes)
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
            max_frame_bytes: usize::MAX,
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
        self.check_frame_len(frame_len)?;
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
        self.send_to(socket, peer, peer, frame).await
    }

    /// Send a frame to `peer` at `dest`, its socket of a lane. The repair
    /// follows the loss that `peer` reports.
    async fn send_to(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        dest: SocketAddr,
        frame: &[u8],
    ) -> Result<(), MeshError> {
        self.check_frame_len(frame.len())?;
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
            self.stats.tx_send_calls.fetch_add(1, Ordering::Relaxed);
            if let Err(err) = socket.send_to(&object.datagram(index), dest).await {
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

/// The ranges of block ids that a peer numbers on their own counters:
/// announcements, control requests, replica replies and parts.
const BLOCK_ID_LANES: usize = 4;

/// The range of block ids that `block_id` belongs to.
fn block_id_lane(block_id: u32) -> usize {
    if block_id >= CHUNK_BLOCK_ID_BASE {
        3
    } else if block_id >= REPLY_BLOCK_ID_BASE {
        2
    } else if block_id >= REQUEST_BLOCK_ID_BASE {
        1
    } else {
        0
    }
}

/// The decoders of one peer: one for each range of block ids.
///
/// A decoder that completes a block drops every incomplete block more than
/// `COMPLETED_WINDOW` (64) ids below it. Each range of a peer counts from
/// its own base, so with one decoder a completed part (ids from 2^31) would
/// drop a replica reply (ids from 1.5 x 2^30) that is still arriving, and
/// every symbol of it already received.
#[derive(Default)]
struct PeerDecoders {
    lanes: [DatagramFecDecoder; BLOCK_ID_LANES],
}

impl PeerDecoders {
    fn lane_mut(&mut self, block_id: u32) -> &mut DatagramFecDecoder {
        &mut self.lanes[block_id_lane(block_id)]
    }

    fn expire_block(&mut self, block_id: u32) {
        self.lane_mut(block_id).expire_block(block_id);
    }

    #[cfg(test)]
    fn in_flight_block_count(&self) -> usize {
        self.lanes
            .iter()
            .map(DatagramFecDecoder::in_flight_block_count)
            .sum()
    }
}

struct FecReceiver {
    decoders: HashMap<SocketAddr, PeerDecoders>,
    /// The peers in the order in which they were given a decoder.
    decoder_order: VecDeque<SocketAddr>,
    observations: HashMap<(SocketAddr, u32), FecBlockObservation>,
    /// Incomplete blocks in the order in which they were first seen.
    observation_order: VecDeque<(SocketAddr, u32)>,
    /// Every datagram's block, with the datagram count when it was seen: an
    /// entry is current while the block was last seen then.
    observation_expiries: VecDeque<(u64, (SocketAddr, u32))>,
    completed: HashSet<(SocketAddr, u32)>,
    completed_order: VecDeque<(SocketAddr, u32)>,
    recoveries: HashMap<(SocketAddr, u32), FecRecoveryObservation>,
    /// Decoded blocks with missing source symbols, in the order in which
    /// they decoded, with the datagram count then.
    recovery_expiries: VecDeque<(u64, (SocketAddr, u32))>,
    observed_datagrams: u64,
    max_decoder_peers: usize,
    max_inflight_frames: usize,
    max_frame_bytes: usize,
    stats: Arc<CacheMeshFecCounters>,
    loss: HashMap<SocketAddr, LossWindow>,
    /// Where the loss from each peer is published for the task that reports
    /// it to the peer.
    measured_loss: Option<MeasuredLossTable>,
}

impl FecReceiver {
    #[cfg(test)]
    fn new(stats: Arc<CacheMeshFecCounters>) -> Self {
        Self::with_limits(
            DEFAULT_MAX_PEERS,
            DEFAULT_MAX_INFLIGHT_FRAMES,
            DEFAULT_MAX_FRAME_BYTES,
            stats,
        )
    }

    fn with_limits(
        max_decoder_peers: usize,
        max_inflight_frames: usize,
        max_frame_bytes: usize,
        stats: Arc<CacheMeshFecCounters>,
    ) -> Self {
        Self {
            decoders: HashMap::new(),
            decoder_order: VecDeque::new(),
            observations: HashMap::new(),
            observation_order: VecDeque::new(),
            observation_expiries: VecDeque::new(),
            completed: HashSet::new(),
            completed_order: VecDeque::new(),
            recoveries: HashMap::new(),
            recovery_expiries: VecDeque::new(),
            observed_datagrams: 0,
            max_decoder_peers: max_decoder_peers.max(1),
            max_inflight_frames: max_inflight_frames.max(1),
            max_frame_bytes: max_frame_bytes.max(1),
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
        // An object larger than any frame that this node accepts takes no
        // decoder state.
        if header.transfer_length as usize > self.max_frame_bytes {
            self.stats
                .rx_oversized_objects
                .fetch_add(1, Ordering::Relaxed);
            self.record_decode_error();
            return Err(DatagramFecError::PayloadTooLargeForBlock {
                actual: header.transfer_length as usize,
                max: self.max_frame_bytes,
            });
        }
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

        self.admit_decoder_peer(peer);
        if !self.observations.contains_key(&key) {
            self.make_observation_room();
        }
        let decoder = self
            .decoders
            .get_mut(&peer)
            .expect("admitted decoder peer must exist")
            .lane_mut(header.block_id);
        let decoded = match decoder.push_datagram(bytes) {
            Ok(decoded) => decoded,
            Err(err) => {
                // A block that cannot be decoded takes no more state.
                decoder.expire_block(header.block_id);
                self.observations.remove(&key);
                self.stats
                    .rx_inflight_objects
                    .store(self.observations.len() as u64, Ordering::Relaxed);
                self.record_decode_error();
                return Err(err);
            }
        };

        let observation = self.observations.entry(key).or_insert_with(|| {
            self.observation_order.push_back(key);
            FecBlockObservation {
                source_symbols: header.source_symbols,
                received_source_symbols: HashSet::new(),
                last_seen_datagram: self.observed_datagrams,
                first_seen_us: received_us,
                fragments: datagram_fragments(bytes.len()) as u32,
            }
        });
        observation.last_seen_datagram = self.observed_datagrams;
        self.observation_expiries
            .push_back((self.observed_datagrams, key));
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
                self.recovery_expiries
                    .push_back((self.observed_datagrams, key));
            }
            self.remember_completed(key);
            if self.observation_order.len() > self.max_inflight_frames.saturating_mul(2) {
                self.observation_order
                    .retain(|candidate| self.observations.contains_key(candidate));
            }
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

    /// Give a peer a decoder, first taking the decoder and blocks of the peer
    /// that has had one longest when the table is full.
    fn admit_decoder_peer(&mut self, peer: SocketAddr) {
        if self.decoders.contains_key(&peer) {
            return;
        }
        while self.decoders.len() >= self.max_decoder_peers {
            let Some(expired_peer) = self.decoder_order.pop_front() else {
                break;
            };
            if self.decoders.remove(&expired_peer).is_none() {
                continue;
            }
            let before = self.observations.len();
            self.observations
                .retain(|(candidate, _), _| *candidate != expired_peer);
            self.completed
                .retain(|(candidate, _)| *candidate != expired_peer);
            self.recoveries
                .retain(|(candidate, _), _| *candidate != expired_peer);
            self.loss.remove(&expired_peer);
            self.stats.rx_evicted_objects.fetch_add(
                before.saturating_sub(self.observations.len()) as u64,
                Ordering::Relaxed,
            );
        }
        self.decoders.insert(peer, PeerDecoders::default());
        self.decoder_order.push_back(peer);
    }

    /// Make room for one more incomplete block: drop the oldest ones beyond
    /// `max_inflight_frames`. A dropped block did not decode in time, so its
    /// missing source symbols count as lost.
    fn make_observation_room(&mut self) {
        while self.observations.len() >= self.max_inflight_frames {
            let Some(key) = self.observation_order.pop_front() else {
                break;
            };
            let Some(observation) = self.observations.remove(&key) else {
                continue;
            };
            if let Some(decoder) = self.decoders.get_mut(&key.0) {
                decoder.expire_block(key.1);
            }
            self.record_unfinished_block(key.0, &observation);
            self.stats
                .rx_evicted_objects
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Count the loss of a block that never decoded: it lost more symbols
    /// than it had repair for.
    fn record_unfinished_block(&mut self, peer: SocketAddr, observation: &FecBlockObservation) {
        let source = u32::from(observation.source_symbols);
        let received = observation.received_source_symbols.len() as u32;
        self.record_block(
            peer,
            source,
            observation.fragments,
            source.saturating_sub(received),
        );
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

    /// Expire what the receiver no longer waits for, on every datagram. Both
    /// queues are in order, so this costs only the entries that expire.
    ///
    /// An incomplete block expires when no datagram of it has come in the
    /// last `FEC_OBSERVATION_DATAGRAM_WINDOW` datagrams. A decoded block
    /// that misses source symbols is finalized, and what is still missing
    /// counted as lost, once it falls out of that window or once its late
    /// sources have had `LATE_SOURCE_GRACE` to arrive, whichever is first.
    fn prune_observations(&mut self) {
        let cutoff = self
            .observed_datagrams
            .saturating_sub(FEC_OBSERVATION_DATAGRAM_WINDOW);
        let mut expired = 0_u64;
        while let Some(&(seen_at, key)) = self.observation_expiries.front() {
            if seen_at >= cutoff {
                break;
            }
            self.observation_expiries.pop_front();
            // The block was seen again since, or is no longer incomplete.
            if self
                .observations
                .get(&key)
                .is_none_or(|observation| observation.last_seen_datagram != seen_at)
            {
                continue;
            }
            let Some(observation) = self.observations.remove(&key) else {
                continue;
            };
            if let Some(decoder) = self.decoders.get_mut(&key.0) {
                decoder.expire_block(key.1);
            }
            self.record_unfinished_block(key.0, &observation);
            expired += 1;
        }
        if expired > 0 {
            self.stats
                .rx_expired_objects
                .fetch_add(expired, Ordering::Relaxed);
        }

        if self.recovery_expiries.is_empty() {
            return;
        }
        let now = Instant::now();
        while let Some(&(completed_at_datagram, key)) = self.recovery_expiries.front() {
            let due = match self.recoveries.get(&key) {
                // Finalized already: every late source came, or the block
                // left the completed window.
                None => {
                    self.recovery_expiries.pop_front();
                    continue;
                }
                Some(recovery) if recovery.completed_at_datagram != completed_at_datagram => {
                    self.recovery_expiries.pop_front();
                    continue;
                }
                Some(recovery) => {
                    completed_at_datagram < cutoff
                        || now.saturating_duration_since(recovery.completed_at) >= LATE_SOURCE_GRACE
                }
            };
            if !due {
                break;
            }
            self.recovery_expiries.pop_front();
            self.finalize_recovery(key);
        }
    }
}

/// How often a receive task writes that a peer was heard into the shared
/// map: every datagram would take the map's lock thousands of times a
/// second for a 4K part, and liveness is read in hundreds of milliseconds.
const PEER_HEARD_STAMP_INTERVAL: Duration = Duration::from_millis(5);

/// A receive task's own record of when it last stamped each peer heard, and
/// of the addresses at which this node hears itself.
#[derive(Default)]
struct PeerHeard {
    /// When each peer was last stamped.
    considered: HashMap<SocketAddr, Instant>,
    /// Addresses from which this node's own frames arrived: a node whose
    /// own address is among its peers (gossiped to it while it binds a
    /// wildcard address) sends itself hellos, and is never its own parent.
    own: HashSet<SocketAddr>,
}

impl PeerHeard {
    /// Whether a datagram from `peer` is due to stamp it heard: not this
    /// node, and not stamped within [`PEER_HEARD_STAMP_INTERVAL`]. The
    /// caller asks only for an authorized peer (the allowlist check that
    /// drops every other datagram comes first), so a datagram from anywhere
    /// else never grows the shared map. The record of peers is replaced
    /// when it holds many more than a node has peers.
    fn due(&mut self, peer: SocketAddr, bind_addr: SocketAddr) -> bool {
        if peer == bind_addr || self.own.contains(&peer) {
            return false;
        }
        let now = Instant::now();
        if self
            .considered
            .get(&peer)
            .is_some_and(|last| now.duration_since(*last) < PEER_HEARD_STAMP_INTERVAL)
        {
            return false;
        }
        if self.considered.len() >= 4_096 {
            self.considered.clear();
        }
        self.considered.insert(peer, now);
        true
    }

    /// Stamp `peer` heard now.
    fn stamp(peer_seen: &PeerSeen, peer: SocketAddr) {
        lock(peer_seen).insert(peer, Instant::now());
    }

    /// A frame of this node's own arrived from `peer`: it is never heard on
    /// receipt again, and a stamp that its first datagram made is undone.
    fn own_address(&mut self, peer_seen: &PeerSeen, peer: SocketAddr) {
        if self.own.insert(peer) {
            lock(peer_seen).remove(&peer);
        }
    }
}

/// The mesh's record of when it last warned of a frame from each peer that
/// did not decode, shared by the receive tasks of both lanes. Every such frame is counted (`rx_invalid_frames`);
/// a peer that sends many is named once in
/// [`INVALID_FRAME_WARN_INTERVAL`].
#[derive(Default)]
struct InvalidFrameWarnings(HashMap<SocketAddr, Instant>);

impl InvalidFrameWarnings {
    /// Whether a warning that names `peer` is due. The caller asks only
    /// for an authorized peer; the record is replaced when it holds many
    /// more than a node has peers.
    fn due(&mut self, peer: SocketAddr) -> bool {
        let now = Instant::now();
        if self
            .0
            .get(&peer)
            .is_some_and(|last| now.duration_since(*last) < INVALID_FRAME_WARN_INTERVAL)
        {
            return false;
        }
        if self.0.len() >= 4_096 {
            self.0.clear();
        }
        self.0.insert(peer, now);
        true
    }
}

/// Receives the datagrams of one socket of the mesh: the main socket, or the
/// class socket of a mesh split by class, which takes parts and
/// initializations only. `replica_tx` and `class_replicas` take the replica
/// requests, which come on the main socket.
#[allow(clippy::too_many_arguments)]
async fn receive_task(
    socket: Arc<UdpSocket>,
    cache: Arc<ChunkCache>,
    shared: Arc<MeshShared>,
    config: CacheMeshConfig,
    lane: Lane,
    replica_tx: Option<mpsc::Sender<ReplicaWork>>,
    class_replicas: Option<ClassReplicas>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    // The class socket takes parts and initializations only: everything else
    // (hellos, leases, requests, loss reports) is on the main socket.
    let class_socket = lane.is_class();
    let observer = if class_socket {
        config.class_receive_observer.clone()
    } else {
        config.receive_observer.clone()
    };
    let counters = Arc::clone(shared.counters_of(lane));
    let mut receiver = FecReceiver::with_limits(
        config.max_peers,
        config.max_inflight_frames,
        config.max_frame_bytes,
        Arc::clone(&counters),
    )
    .with_measured_loss(Arc::clone(shared.measured_loss_of(lane)));
    let mut buf = vec![0u8; 65_536];
    let mut heard = PeerHeard::default();
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
                let (len, from) = match result {
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
                // Before anything else reads the datagram: an observer that
                // cuts it through to clients must never see it either. A
                // datagram on the class socket comes from the peer's class
                // socket, and the peer is known by its main address.
                let Some(peer) = shared.authorized_sender(lane, from) else {
                    counters
                        .rx_unauthorized_datagrams
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                };

                // Heard on receipt, before FEC decode: a peer whose blocks
                // are slow to complete is still alive.
                if heard.due(peer, config.bind_addr) {
                    PeerHeard::stamp(&shared.peer_seen, peer);
                }
                if let Some(observer) = &observer.0 {
                    observer.datagram(peer, &buf[..len]);
                }
                let pushed = receiver.push_at(peer, &buf[..len], received_us);
                let decoded_us = unix_micros();
                counters.decode_nanoseconds.fetch_add(
                    decoded_us.saturating_sub(received_us).saturating_mul(1_000),
                    Ordering::Relaxed,
                );
                let DecodedBlock { frame: decoded, first_seen_us } = match pushed {
                    Ok(Some(decoded)) => {
                        if let Some(observer) = &observer.0 {
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
                match MeshFrame::decode(decoded) {
                    Ok(frame) => {
                        if frame.node_id() == config.node_id {
                            heard.own_address(&shared.peer_seen, peer);
                            continue;
                        }
                        if class_socket
                            && !matches!(
                                frame,
                                MeshFrame::Chunk { .. } | MeshFrame::Initialization { .. }
                            )
                        {
                            debug!(peer = %peer, "cache mesh control frame on the class socket ignored");
                            continue;
                        }
                        handle_frame(
                            frame,
                            peer,
                            FrameHandler {
                                cache: &cache,
                                shared: &shared,
                                counters: &counters,
                                config: &config,
                                replica_tx: replica_tx.as_ref(),
                                class_replicas: class_replicas.as_ref(),
                                rx_first_us: first_seen_us,
                                decoded_us,
                            },
                        ).await;
                    }
                    // A frame of a later build: counted, never logged.
                    Err(MeshError::UnknownKind(_)) => {
                        counters.rx_unknown_kinds.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => {
                        counters.rx_invalid_frames.fetch_add(1, Ordering::Relaxed);
                        if lock(&shared.invalid_frame_warnings).due(peer) {
                            warn!(peer = %peer, error = %err, "cache mesh frame decode failed");
                        }
                    }
                }
            }
        }
    }
}

fn elapsed_nanoseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Queue work without waiting: false, and a drop counted, when the queue is
/// full.
fn try_enqueue<T>(
    sender: &mpsc::Sender<T>,
    depth: &AtomicU64,
    max_depth: &AtomicU64,
    drops: &AtomicU64,
    value: T,
) -> bool {
    match sender.try_reserve() {
        Ok(permit) => {
            let current = depth.fetch_add(1, Ordering::Relaxed).saturating_add(1);
            max_depth.fetch_max(current, Ordering::Relaxed);
            permit.send(value);
            true
        }
        Err(_) => {
            drops.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
}

fn decrement_depth(depth: &AtomicU64) {
    let mut current = depth.load(Ordering::Relaxed);
    while current > 0 {
        match depth.compare_exchange_weak(
            current,
            current - 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

/// A replica request, served by the replica task so that a large range does
/// not hold up the receive task.
struct ReplicaWork {
    requester_node_id: String,
    peer: SocketAddr,
    stream_id: u64,
    from_slot: u64,
    count: u32,
}

async fn replica_task(
    socket: Arc<UdpSocket>,
    cache: Arc<ChunkCache>,
    shared: Arc<MeshShared>,
    config: CacheMeshConfig,
    mut replica_rx: mpsc::Receiver<ReplicaWork>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut sender = FecSender::from_config_with_initial_block_id(
        &config,
        REPLY_BLOCK_ID_BASE + config.block_id_offset,
        Arc::clone(&shared.counters),
    )
    .with_link_loss(Arc::clone(&shared.link_loss));
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("cache mesh replica task shutting down");
                return;
            }
            work = replica_rx.recv() => {
                let Some(work) = work else {
                    return;
                };
                decrement_depth(&shared.counters.replica_queue_depth);
                let started = Instant::now();
                let service_result = serve_replica_request(
                    &socket,
                    &cache,
                    &mut sender,
                    &config,
                    work.peer,
                    work.peer,
                    work.stream_id,
                    work.from_slot,
                    work.count,
                    #[cfg(test)]
                    None,
                )
                .await;
                shared
                    .counters
                    .replica_requests_serviced
                    .fetch_add(1, Ordering::Relaxed);
                if let Ok(parts) = &service_result {
                    shared
                        .counters
                        .replica_parts_served
                        .fetch_add(*parts as u64, Ordering::Relaxed);
                }
                if let Err(err) = service_result {
                    shared
                        .counters
                        .replica_service_errors
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(
                        requester = work.requester_node_id,
                        peer = %work.peer,
                        stream_id = work.stream_id,
                        error = %err,
                        "cache mesh replica request could not be served"
                    );
                }
                shared.counters.replica_service_nanoseconds.fetch_add(
                    elapsed_nanoseconds(started),
                    Ordering::Relaxed,
                );
            }
        }
    }
}

struct FrameHandler<'a> {
    cache: &'a Arc<ChunkCache>,
    shared: &'a Arc<MeshShared>,
    config: &'a CacheMeshConfig,
    /// The counters of the lane whose socket the frame came on.
    counters: &'a CacheMeshFecCounters,
    /// Where a replica request goes: the replica task of the main socket.
    /// None on the class socket, which takes no requests.
    replica_tx: Option<&'a mpsc::Sender<ReplicaWork>>,
    /// Where a replica request for a video playlist goes on a mesh split by
    /// class: the class lane serves it on its own socket and task.
    class_replicas: Option<&'a ClassReplicas>,
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
            draining,
            boot_id,
            build,
            state,
        } => {
            if matches!(role, WireRole::Other(_)) {
                handler
                    .counters
                    .rx_unknown_roles
                    .fetch_add(1, Ordering::Relaxed);
            }
            if handler.config.same_region_only && region != handler.config.region {
                handler.shared.peers.write().await.remove(&peer);
                handler.shared.peer_regions.write().await.remove(&peer);
                handler.shared.peer_roles.write().await.remove(&peer);
                lock(&handler.shared.peer_hellos).remove(&peer);
                debug!(node_id, region, peer = %peer, "ignored cache mesh peer from another region");
                return;
            }
            // Only authorized peers join, and only while the table has room.
            let mut discovered = 0usize;
            let mut admitted = Vec::new();
            if let Some(inserted) = admit_peer(handler.shared, peer, handler.config.max_peers).await
            {
                if inserted {
                    discovered = discovered.saturating_add(1);
                }
                admitted.push(peer);
            }
            for advertised_peer in peers {
                if advertised_peer == peer || advertised_peer == handler.config.bind_addr {
                    continue;
                }
                if let Some(inserted) =
                    admit_peer(handler.shared, advertised_peer, handler.config.max_peers).await
                {
                    if inserted {
                        discovered = discovered.saturating_add(1);
                    }
                    admitted.push(advertised_peer);
                }
            }
            let peer_admitted = admitted.contains(&peer);
            let mut peer_regions = handler.shared.peer_regions.write().await;
            for admitted_peer in admitted {
                peer_regions.insert(admitted_peer, region.clone());
            }
            drop(peer_regions);
            if peer_admitted {
                handler.shared.peer_roles.write().await.insert(peer, role);
                {
                    let mut peer_draining = lock(&handler.shared.peer_draining);
                    if draining {
                        peer_draining.insert(peer);
                    } else {
                        peer_draining.remove(&peer);
                    }
                }
                let now = Instant::now();
                let previous = lock(&handler.shared.peer_hellos).insert(
                    peer,
                    PeerHello {
                        boot_id,
                        build,
                        state,
                        draining,
                        at: now,
                    },
                );
                // A parent that is new, has restarted, returns from a
                // silence or changes whether it drains: its children greet
                // it, rank again and ask now, not at their next renewal.
                if WireRole::may_be_parent(Some(role), handler.config.role)
                    && previous.is_none_or(|previous| {
                        previous.boot_id != boot_id
                            || previous.draining != draining
                            || now.duration_since(previous.at) > handler.config.peer_timeout
                    })
                {
                    handler
                        .shared
                        .parents_changed
                        .send_modify(|count| *count = count.wrapping_add(1));
                }
            }
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
            if let Err(err) = handler.cache.validate_write(slot_id, &payload) {
                warn!(
                    node_id,
                    stream_id,
                    slot_id,
                    error = err,
                    "cache mesh chunk rejected"
                );
                return;
            }
            let handle = handler.cache.resolve_or_create_stream(stream_id).await;
            // Each parent sends every part. The second copy of a part is not
            // written again, so it costs no copy and wakes no reader.
            if handler
                .cache
                .get_for_handle(handle, slot_id)
                .await
                .is_some_and(|(existing, _)| existing == payload)
            {
                return;
            }
            let bytes = payload.len();
            let remote_key = (handle, slot_id);
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
                Some(handler.shared.remote_slots.write().await)
            } else {
                None
            };
            let marked = match remote_slots.as_mut() {
                Some(remote_slots) => mark_remote_slot(
                    handler.cache,
                    remote_slots,
                    remote_key,
                    handler.config.max_remote_slots,
                    handler.counters,
                ),
                None => false,
            };
            // With the origin's epoch known on both sides, the part is judged
            // against what holds its slot: the first copy of an id stays, an
            // older epoch or an older id of the epoch is refused. Otherwise a
            // peer's part may replace a newer id of the same generation: a
            // restarted origin numbers its parts from the start again.
            let written = match handler.config.slot_epoch {
                Some(epoch) => {
                    handler
                        .cache
                        .add_remote_by_epoch_for_handle(handle, slot_id, payload, epoch)
                        .await
                }
                None => handler
                    .cache
                    .add_remote_for_handle(handle, slot_id, payload)
                    .await
                    .map(|()| RemoteWrite::Written),
            };
            if marked && !matches!(written, Ok(RemoteWrite::Written)) {
                if let Some(remote_slots) = remote_slots.as_mut() {
                    remote_slots.remove(&remote_key);
                }
            }
            drop(remote_slots);
            match written {
                Ok(RemoteWrite::Written) => {}
                Ok(RemoteWrite::Duplicate) => return,
                Ok(RemoteWrite::Conflict) => {
                    handler.counters.apply_conflicts.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Ok(RemoteWrite::Stale) => {
                    handler.counters.apply_stale.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Err(err) => {
                    warn!(
                        node_id,
                        stream_id,
                        slot_id,
                        error = err,
                        "cache mesh write failed"
                    );
                    return;
                }
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
            // The requester is a child: it learns parts from this node from
            // now on. The reply goes to its address either way.
            let _ = admit_peer(handler.shared, peer, handler.config.max_peers).await;
            if let Some(class_replicas) = handler.class_replicas {
                if handler.config.stream_class(stream_id) == StreamClass::Video {
                    // A video window or hole is sent from the class socket by
                    // its own task, so it never holds the audio replies back.
                    class_replicas.offer(ReplicaJob {
                        peer,
                        stream_id,
                        from_slot,
                        count,
                    });
                    return;
                }
            }
            let Some(replica_tx) = handler.replica_tx else {
                return;
            };
            if !try_enqueue(
                replica_tx,
                &handler.counters.replica_queue_depth,
                &handler.counters.replica_queue_max_depth,
                &handler.counters.replica_queue_drops,
                ReplicaWork {
                    requester_node_id: node_id,
                    peer,
                    stream_id,
                    from_slot,
                    count,
                },
            ) {
                debug!(
                    peer = %peer,
                    stream_id,
                    "cache mesh replica request queue full"
                );
            }
        }
        MeshFrame::Subscribe {
            node_id,
            stream_id,
            from_slot,
            backfill_parts,
            lease_ms,
            rank,
            boot_id,
        } => {
            if !peer_is_allowed(peer, &handler).await || handler.config.role == CacheMeshRole::Edge
            {
                return;
            }
            // A child that the peer table has no room for is not served.
            if admit_peer(handler.shared, peer, handler.config.max_peers)
                .await
                .is_none()
            {
                return;
            }
            let lease = Duration::from_millis(u64::from(lease_ms).max(1)).min(
                handler
                    .config
                    .subscription_lease
                    .max(Duration::from_secs(1))
                    * 4,
            );
            let now = Instant::now();
            let mut leases = lock(&handler.shared.leases);
            // A renewal comes from the process that holds the lease. A
            // Subscribe with another boot id comes from a child that has
            // restarted inside its lease: it holds nothing of the playlist,
            // and its lease is opened again. The boot id is in the
            // Subscribe, not only in the hello, because a Subscribe that
            // overtakes the first hello of the new process would otherwise
            // renew the lease of the dead one.
            let renewed = leases
                .get_mut(&(peer, stream_id))
                .filter(|current| {
                    current.expires > now && (boot_id.is_none() || current.child_boot == boot_id)
                })
                .map(|current| {
                    current.expires = now + lease;
                    current.rank = rank;
                })
                .is_some();
            if !renewed {
                let from_slot = usize::try_from(from_slot).unwrap_or(usize::MAX);
                let backfill_parts = usize::try_from(backfill_parts).unwrap_or(usize::MAX);
                leases.insert(
                    (peer, stream_id),
                    Lease {
                        expires: now + lease,
                        open: Some((from_slot, backfill_parts)),
                        rank,
                        child_boot: boot_id,
                        generation: handler
                            .shared
                            .lease_generation
                            .fetch_add(1, Ordering::Relaxed),
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

/// Whether this node takes cache data and requests from a peer. The sender
/// is authorized already: the receive task drops every other datagram.
async fn peer_is_allowed(peer: SocketAddr, handler: &FrameHandler<'_>) -> bool {
    let region_allowed = handler
        .shared
        .peer_regions
        .read()
        .await
        .get(&peer)
        .map(|region| !handler.config.same_region_only || region == &handler.config.region)
        .unwrap_or(!handler.config.same_region_only);
    if !region_allowed {
        return false;
    }
    match handler.shared.peer_roles.read().await.get(&peer) {
        // A peer of a role that this build does not know is neither a
        // parent nor a child: nothing is taken from it or served to it.
        Some(WireRole::Other(_)) => false,
        Some(WireRole::Known(CacheMeshRole::Edge)) => handler.config.role != CacheMeshRole::Edge,
        Some(WireRole::Known(_)) | None => true,
    }
}

/// Add an authorized peer to the peer table. `None` when the peer is not
/// authorized or the table is full; otherwise whether it is new.
async fn admit_peer(shared: &MeshShared, peer: SocketAddr, max_peers: usize) -> Option<bool> {
    if !shared.is_allowed(&peer) {
        return None;
    }
    let mut peers = shared.peers.write().await;
    if peers.contains(&peer) {
        return Some(false);
    }
    if peers.len() >= max_peers.max(1) {
        shared
            .counters
            .peer_limit_drops
            .fetch_add(1, Ordering::Relaxed);
        return None;
    }
    Some(peers.insert(peer))
}

/// Mark a slot as received from a peer, under the held lock. A full set is
/// first pruned to the slots that the cache retains; it never drops a slot
/// that the cache still holds, which the copy loop would send back. Returns
/// whether the slot was newly marked.
fn mark_remote_slot(
    cache: &ChunkCache,
    remote_slots: &mut HashSet<(StreamHandle, usize)>,
    key: (StreamHandle, usize),
    max_remote_slots: usize,
    counters: &CacheMeshFecCounters,
) -> bool {
    if remote_slots.contains(&key) {
        return false;
    }
    if remote_slots.len() >= max_remote_slots.max(1) {
        retain_retained_remote_slots(cache, remote_slots);
        if remote_slots.len() >= max_remote_slots.max(1) {
            // Only a limit set below the cache's capacity gets here. The part
            // is still stored; the copy loop may send it back once.
            counters
                .remote_slot_evictions
                .fetch_add(1, Ordering::Relaxed);
            return false;
        }
    }
    remote_slots.insert(key)
}

#[cfg(test)]
struct ReplicaServiceHook {
    first_chunk_sent: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
}

/// Send a requester the parts of a stream: `count` parts from `from_slot`
/// (a hole), or, with a `count` of zero, the initialization and every
/// retained part from `from_slot` to the newest. Stops when the stream's
/// index is reused while the parts go out. The datagrams go to `dest`, the
/// requester's socket of the lane that serves the request.
#[allow(clippy::too_many_arguments)]
async fn serve_replica_request(
    socket: &UdpSocket,
    cache: &ChunkCache,
    sender: &mut FecSender,
    config: &CacheMeshConfig,
    peer: SocketAddr,
    dest: SocketAddr,
    stream_id: u64,
    from_slot: u64,
    count: u32,
    #[cfg(test)] hook: Option<&ReplicaServiceHook>,
) -> Result<usize, MeshError> {
    let Some(handle) = cache.resolve_stream(stream_id) else {
        return Ok(0);
    };
    let mut sent = 0usize;
    // A request for named parts fills a hole. The requester has the
    // initialization of the stream already.
    if count == 0 {
        if let Some(initialization) = cache.stream_initialization_for_handle(handle) {
            let frame =
                MeshFrame::initialization(config.node_id.clone(), stream_id, initialization)
                    .encode_as(config.frame_form)?;
            sender.send_to(socket, peer, dest, &frame).await?;
            sent = sent.saturating_add(1);
        }
    }
    let Some(last) = cache.last_for_handle(handle) else {
        return Ok(sent);
    };
    let first = usize::try_from(from_slot).unwrap_or(usize::MAX);
    if first > last {
        return Ok(sent);
    }
    let end = if count > 0 {
        first.saturating_add(count as usize - 1).min(last)
    } else {
        last
    };
    #[cfg(test)]
    let mut chunks_sent = 0usize;
    for slot_id in first.max(cache.retained_start(last))..=end {
        let Some((payload, hash)) = cache.get_for_handle(handle, slot_id).await else {
            if cache.last_for_handle(handle).is_none() {
                // The index now holds another stream.
                break;
            }
            continue;
        };
        if hash == 0 && payload.is_empty() {
            continue;
        }
        let frame = MeshFrame::chunk(config.node_id.clone(), stream_id, slot_id as u64, payload)
            .encode_as(config.frame_form)?;
        sender.send_to(socket, peer, dest, &frame).await?;
        sent = sent.saturating_add(1);
        #[cfg(test)]
        {
            chunks_sent += 1;
            if chunks_sent == 1 {
                if let Some(hook) = hook {
                    hook.first_chunk_sent.wait().await;
                    hook.resume.wait().await;
                }
            }
        }
    }
    debug!(peer = %peer, stream_id, from_slot, count, sent, "cache mesh replica request served");
    Ok(sent)
}

/// Serves the replica requests for video playlists of a mesh split by class,
/// from the class socket, to the requester's class socket.
async fn class_replica_task(
    socket: Arc<UdpSocket>,
    cache: Arc<ChunkCache>,
    shared: Arc<MeshShared>,
    config: CacheMeshConfig,
    lane: Lane,
    mut jobs: mpsc::Receiver<ReplicaJob>,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let counters = Arc::clone(shared.counters_of(lane));
    let mut sender = FecSender::from_config_with_initial_block_id(
        &config,
        REPLY_BLOCK_ID_BASE + config.block_id_offset,
        Arc::clone(&counters),
    )
    .with_link_loss(Arc::clone(&shared.link_loss));
    loop {
        let first = tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("cache mesh class replica task shutting down");
                return;
            }
            job = jobs.recv() => match job {
                Some(job) => job,
                None => return,
            },
        };
        // The requests that came in while the last one was served: a
        // request that another one covers is served once.
        for job in coalesce_replica_jobs(first, &mut jobs) {
            let Some(dest) = lane.peer_socket(job.peer) else {
                continue;
            };
            let started = Instant::now();
            let service_result = serve_replica_request(
                &socket,
                &cache,
                &mut sender,
                &config,
                job.peer,
                dest,
                job.stream_id,
                job.from_slot,
                job.count,
                #[cfg(test)]
                None,
            )
            .await;
            counters
                .replica_requests_serviced
                .fetch_add(1, Ordering::Relaxed);
            if let Ok(parts) = &service_result {
                counters
                    .replica_parts_served
                    .fetch_add(*parts as u64, Ordering::Relaxed);
            }
            if let Err(err) = service_result {
                counters
                    .replica_service_errors
                    .fetch_add(1, Ordering::Relaxed);
                debug!(
                    peer = %job.peer,
                    stream_id = job.stream_id,
                    error = %err,
                    "cache mesh class replica request could not be served"
                );
            }
            counters
                .replica_service_nanoseconds
                .fetch_add(elapsed_nanoseconds(started), Ordering::Relaxed);
        }
    }
}

async fn announce_task(
    socket: Arc<UdpSocket>,
    shared: Arc<MeshShared>,
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut sender = FecSender::from_config_with_initial_block_id(
        &config,
        config.block_id_offset,
        Arc::clone(&shared.counters),
    )
    .with_link_loss(Arc::clone(&shared.link_loss));
    let mut reported: HashMap<SocketAddr, (Instant, u32)> = HashMap::new();
    let mut interval = tokio::time::interval(config.announce_interval);
    // When a node that speaks like a later build last sent its frame of an
    // unknown kind.
    let mut future_frame_sent: Option<Instant> = None;

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("cache mesh announce task shutting down");
                return;
            }
            _ = interval.tick() => {
                let current_peers = shared.peers.read().await.iter().copied().collect::<Vec<_>>();
                let frame = match shared.hello().await.encode_as(config.frame_form) {
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
                if config.frame_form == FrameForm::Future
                    && future_frame_sent.is_none_or(|at| at.elapsed() >= FUTURE_FRAME_INTERVAL)
                {
                    future_frame_sent = Some(Instant::now());
                    let frame = MeshFrame::of_unknown_kind();
                    for peer in &current_peers {
                        if let Err(err) = sender.send(&socket, *peer, &frame).await {
                            debug!(peer = %peer, error = %err, "cache mesh future frame send failed");
                        }
                    }
                }
                // Tell each peer what this node loses of what the peer
                // sends, so that the peer protects the path accordingly.
                // On a mesh split by class, the worse of the two sockets.
                let measured = shared.measured_loss();
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
                    match report.encode_as(config.frame_form) {
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
struct SeenWatermarks(HashMap<StreamHandle, usize>);

impl SeenWatermarks {
    /// Remember the watermark `last` of a stream and return the one seen
    /// before.
    fn record(&mut self, stream: StreamHandle, last: Option<usize>) -> Option<usize> {
        match last {
            Some(last) => self.0.insert(stream, last),
            None => self.0.remove(&stream),
        }
    }

    /// Remember the current watermark of every stream.
    fn refresh(&mut self, cache: &ChunkCache, streams: &[StreamHandle]) {
        for handle in streams {
            self.record(*handle, cache.last_for_handle(*handle));
        }
        self.retain_streams(streams);
    }

    /// Forget the streams that the cache no longer holds.
    fn retain_streams(&mut self, streams: &[StreamHandle]) {
        if self.0.len() > streams.len() {
            let current = streams.iter().copied().collect::<HashSet<_>>();
            self.0.retain(|stream, _| current.contains(stream));
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
/// that was released or whose index was reused.
async fn forget_unretained_remote_slots(
    cache: &ChunkCache,
    remote_slots: &RwLock<HashSet<(StreamHandle, usize)>>,
) {
    // The streams and their watermarks are read under the lock. The receive
    // task marks a slot and writes it under the same lock, so a slot that is
    // being received is never seen above the watermark, or in a stream not
    // yet created, here.
    let mut remote_slots = remote_slots.write().await;
    retain_retained_remote_slots(cache, &mut remote_slots);
}

/// [`forget_unretained_remote_slots`] on the locked set.
fn retain_retained_remote_slots(
    cache: &ChunkCache,
    remote_slots: &mut HashSet<(StreamHandle, usize)>,
) {
    let retained_ranges = cache
        .stream_handles()
        .into_iter()
        .filter_map(|handle| {
            cache
                .last_for_handle(handle)
                .map(|last| (handle, (cache.retained_start(last), last)))
        })
        .collect::<HashMap<_, _>>();
    remote_slots.retain(|(stream, slot_id)| {
        retained_ranges
            .get(stream)
            .is_some_and(|(start, last)| slot_id >= start && slot_id <= last)
    });
}

/// A chunk frame without its payload: the payload follows it.
fn chunk_frame_header(
    out: &mut Vec<u8>,
    form: FrameForm,
    node_id: &str,
    stream_id: u64,
    slot_id: u64,
    payload_len: usize,
) {
    out.clear();
    out.extend_from_slice(FRAME_MAGIC);
    out.push(form.version());
    out.push(FRAME_CHUNK);
    out.extend_from_slice(&(node_id.len() as u16).to_be_bytes());
    out.extend_from_slice(node_id.as_bytes());
    out.extend_from_slice(&stream_id.to_be_bytes());
    out.extend_from_slice(&slot_id.to_be_bytes());
    out.extend_from_slice(&(payload_len as u32).to_be_bytes());
}

/// A part queued for a child in a pass.
struct QueuedPart {
    stream: StreamHandle,
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

/// What a pass sends one child.
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

/// Whether two lists of served leases differ in their (child, playlist)
/// pairs or in a pair's rank. The warm-up count of a lease is not a
/// change.
fn served_keys_differ(old: &[ServedSubscription], new: &[ServedSubscription]) -> bool {
    if old.len() != new.len() {
        return true;
    }
    let keys: HashSet<(SocketAddr, u64, Option<u8>)> = old
        .iter()
        .map(|served| (served.child, served.stream_id, served.rank))
        .collect();
    new.iter()
        .any(|served| !keys.contains(&(served.child, served.stream_id, served.rank)))
}

/// Sends the parts of the cache to the children. A part leaves as soon as
/// the local watermark reaches it: the publication wakes this task, which
/// visits only the streams that moved. Each child's datagrams go to the
/// kernel in batches (`sendmmsg`, up to 64 messages a call), with generic
/// segmentation offload where the kernel has it, and an object smaller than
/// a symbol goes in one symbol of its own length. The tick does the rest:
/// leases, the choice of children, warm-up windows and pruning.
///
/// On a mesh split by class there is one sync task for each lane, and each
/// sends the playlists of its class from its own socket to the children's
/// socket of that lane.
async fn sync_task(
    socket: Arc<UdpSocket>,
    cache: Arc<ChunkCache>,
    shared: Arc<MeshShared>,
    config: CacheMeshConfig,
    lane: Lane,
    mut shutdown_rx: watch::Receiver<()>,
) {
    if config.role == CacheMeshRole::Edge {
        return;
    }
    let demand = config.replication == ReplicationMode::Demand;
    let mut sender = FecSender::from_config_with_initial_block_id(
        &config,
        CHUNK_BLOCK_ID_BASE + config.block_id_offset,
        Arc::clone(shared.counters_of(lane)),
    )
    .with_link_loss(Arc::clone(&shared.link_loss));
    #[cfg(test)]
    let gso = config.gso;
    #[cfg(not(test))]
    let gso = true;
    let mut batch_sender = BatchSender::new(&socket, gso);
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
    // The streams that the cache says advanced.
    let moved = cache.subscribe_publications();
    let mut moved_idxs: Vec<usize> = Vec::new();
    let mut pass_streams: Vec<StreamHandle> = Vec::new();
    // The children's cursors, initializations and warm-up windows.
    let mut lane_children = LaneChildren::default();
    // Demand replication state: the leases the last tick saw, and the
    // warm-up byte allowance of each child.
    let mut active: HashSet<(SocketAddr, u64)> = HashSet::new();
    // The rank that each lease's child gave this node, as the tick read it.
    let mut lease_ranks: HashMap<(SocketAddr, u64), Option<u8>> = HashMap::new();
    // The generation of each lease when this lane last looked.
    let mut lease_generations: HashMap<(SocketAddr, u64), u32> = HashMap::new();
    let mut backfill_tokens: HashMap<SocketAddr, f64> = HashMap::new();
    // Which child of a stream takes warm-up parts first this tick: it turns
    // each tick, so that under the total cap no child is always last.
    let mut warm_rotation = 0usize;
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
    // On a mesh split by class: the playlists that this lane owns (see
    // `LaneHandover`), and those of its class that it waits for the other
    // lane to hand over. The other lane wakes it when it does.
    let handover = &shared.handover;
    let lane_class = lane.class.unwrap_or_default();
    let mut owns: HashSet<StreamHandle> = HashSet::new();
    let mut waiting: HashSet<StreamHandle> = HashSet::new();
    let lease_table = demand.then_some(&shared.leases);
    // Only the main lane prunes the remote slots, which both lanes share.
    let prunes_remote_slots = !lane.is_class();
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
            _ = published.as_mut() => false,
            // A playlist that this lane waits for was handed over.
            _ = handover.wake(lane_class).notified(), if !waiting.is_empty() => false,
        };
        // Arm the next wait before the watermarks are read, so that a part
        // published while this pass runs wakes the next one.
        published.set(publication.notified());
        published.as_mut().enable();
        // The warm-up of this pass: bytes queued, and whether a child's own
        // allowance or the node's total stopped a window.
        let mut pass_warm_bytes = 0u64;
        let mut pass_warm_starved = false;
        let mut pass_warm_capped = false;
        if tick {
            warm_rotation = warm_rotation.wrapping_add(1);
            let now = Instant::now();
            let elapsed = now.duration_since(last_tick);
            last_tick = now;
            if demand {
                let mut current = HashSet::new();
                lease_ranks.clear();
                // Leases opened again since this lane last looked.
                let mut reopened: Vec<(SocketAddr, u64)> = Vec::new();
                {
                    let mut leases = lock(&shared.leases);
                    leases.retain(|_, lease| lease.expires > now);
                    for (key, lease) in leases.iter_mut() {
                        current.insert(*key);
                        lease_ranks.insert(*key, lease.rank);
                        // A lease of another generation is a new lease: its
                        // child restarted, or the lease ended and came back
                        // between two ticks. What the old one was sent is
                        // forgotten before the new window is taken, so the
                        // child gets the initialization and its warm-up
                        // window again.
                        if lease_generations
                            .insert(*key, lease.generation)
                            .is_some_and(|seen| seen != lease.generation)
                        {
                            lane_children.forget_lease(key.0, key.1);
                            reopened.push(*key);
                        }
                        // The lane of the playlist's class starts its
                        // warm-up window.
                        if lease.open.is_some() && lane.carries(&config, key.1) {
                            if let Some(open) = lease.open.take() {
                                lane_children.pending_open.insert(*key, open);
                            }
                        }
                    }
                }
                if lease_generations.len() > current.len() {
                    lease_generations.retain(|key, _| current.contains(key));
                }
                if lane.split() {
                    for (peer, stream_id) in &reopened {
                        forget_child_in_transit(handover, *stream_id, peer);
                    }
                }
                for (peer, stream_id) in active.difference(&current) {
                    // The lease ended. Forget the cursors, so that a later
                    // subscription starts with the initialization again.
                    lane_children.forget_lease(*peer, *stream_id);
                    if lane.split() {
                        forget_child_in_transit(handover, *stream_id, peer);
                    }
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
                let now_served: Vec<ServedSubscription> = active
                    .iter()
                    .filter(|(_, stream_id)| lane.carries(&config, *stream_id))
                    .map(|(child, stream_id)| ServedSubscription {
                        child: *child,
                        stream_id: *stream_id,
                        rank: lease_ranks.get(&(*child, *stream_id)).copied().flatten(),
                        backfill_parts_pending: lane_children
                            .pending_open
                            .get(&(*child, *stream_id))
                            .map(|(_, parts)| *parts)
                            .or_else(|| {
                                lane_children
                                    .backfills
                                    .get(&(*child, *stream_id))
                                    .map(Backfill::pending)
                            })
                            .unwrap_or(0),
                    })
                    .collect();
                let changed = {
                    let mut served = lock(shared.served_of(lane));
                    let changed = served_keys_differ(&served, &now_served);
                    *served = now_served;
                    changed
                };
                if changed {
                    shared
                        .served_changed
                        .send_modify(|version| *version = version.wrapping_add(1));
                }
            }
            children = {
                let regions = shared.peer_regions.read().await;
                let roles = shared.peer_roles.read().await;
                let mut peers = shared
                    .peers
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
                    .filter(|peer| WireRole::may_be_child(roles.get(peer).copied(), config.role))
                    // A peer whose class socket would be beyond the last
                    // port has none.
                    .filter(|peer| !lane.is_class() || lane.peer_socket(*peer).is_some())
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
                let streams = cache.stream_handles();
                // Keep the watermarks current, so that the parts published
                // long ago are not reported as live sends to a child that
                // comes later.
                seen_last.refresh(&cache, &streams);
                if lane.split() {
                    forget_released_streams(
                        &mut owns,
                        &mut waiting,
                        handover,
                        prunes_remote_slots,
                        &streams,
                    );
                    // A lane without children visits no playlist: it hands
                    // over those that are no longer its class here, so the
                    // other lane does not wait for them.
                    let leaving = owns
                        .iter()
                        .copied()
                        .filter(|stream| !lane.carries(&config, stream.stream_id()))
                        .collect::<Vec<_>>();
                    for stream in leaving {
                        hand_over_stream(
                            lane_class,
                            stream,
                            &mut owns,
                            &mut lane_children,
                            handover,
                        );
                    }
                }
                if last_remote_slot_prune.elapsed() >= REMOTE_SLOT_PRUNE_INTERVAL {
                    last_remote_slot_prune = Instant::now();
                    prune_lane(&cache, &shared, lane, &mut lane_children, &streams).await;
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
            let streams = cache.stream_handles();
            if last_remote_slot_prune.elapsed() >= REMOTE_SLOT_PRUNE_INTERVAL {
                last_remote_slot_prune = Instant::now();
                prune_lane(&cache, &shared, lane, &mut lane_children, &streams).await;
            }
            seen_last.retain_streams(&streams);
            if lane.split() {
                forget_released_streams(
                    &mut owns,
                    &mut waiting,
                    handover,
                    prunes_remote_slots,
                    &streams,
                );
            }
            pass_streams = streams;
        } else {
            // A playlist handed over to this lane is visited at once.
            if !waiting.is_empty() {
                moved_idxs.extend(waiting.iter().map(|stream| stream.index()));
                moved_idxs.sort_unstable();
                moved_idxs.dedup();
            }
            if moved_idxs.is_empty() {
                continue;
            }
            // The stream that holds each index now; none while the index is
            // being given to another stream, whose parts a later pass sends.
            pass_streams.clear();
            pass_streams.extend(moved_idxs.iter().filter_map(|idx| cache.handle_at(*idx)));
        }
        'streams: for &handle in &pass_streams {
            let stream_id = handle.stream_id();
            let last = cache.last_for_handle(handle);
            let previous = seen_last.record(handle, last);
            // On a mesh split by class each lane sends the playlists of its
            // own class. A playlist that changed class (audio until its first
            // video) is handed over by the lane that it left, with every
            // child's cursor, and the lane that it came to sends nothing of it
            // until then: the child goes on from the part after the last one
            // that the old lane sent, whichever lane looks first.
            let mut handed_over = false;
            if lane.split() {
                match visit_stream(
                    lane_class,
                    lane.carries(&config, stream_id),
                    handle,
                    &mut owns,
                    &mut lane_children,
                    handover,
                    lease_table,
                ) {
                    LaneVisit::Send => handed_over = waiting.remove(&handle),
                    LaneVisit::Skip => {
                        waiting.remove(&handle);
                        continue;
                    }
                    LaneVisit::Wait => {
                        waiting.insert(handle);
                        continue;
                    }
                }
            }
            // A publication woke this pass: only a stream whose watermark
            // moved has a part to send. The tick does everything else.
            if !tick && !handed_over && (last.is_none() || last == previous) {
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
            if let Some(initialization) = cache.stream_initialization_for_handle(handle) {
                // Encoded only when a child lacks this initialization.
                let mut frame = None;
                for peer in &stream_children {
                    let key = (handle, *peer);
                    if lane_children
                        .sent_initializations
                        .get(&key)
                        .is_some_and(|sent| same_bytes(sent, &initialization))
                    {
                        continue;
                    }
                    // The parts queued before it must not overtake it.
                    flush_child(
                        &socket,
                        &mut sender,
                        &mut batch_sender,
                        *peer,
                        lane.to(*peer),
                        batches.get_mut(peer),
                        &mut call_times,
                        &mut lane_children.sent,
                        &config,
                    )
                    .await;
                    let frame = match &mut frame {
                        Some(frame) => frame,
                        None => match MeshFrame::initialization(
                            config.node_id.clone(),
                            stream_id,
                            initialization.clone(),
                        )
                        .encode_as(config.frame_form)
                        {
                            Ok(encoded) => frame.insert(encoded),
                            Err(err) => {
                                warn!(stream_id, error = %err, "cache mesh initialization encode failed");
                                continue 'streams;
                            }
                        },
                    };
                    match sender.send_to(&socket, *peer, lane.to(*peer), frame).await {
                        Ok(()) => {
                            lane_children
                                .sent_initializations
                                .insert(key, initialization.clone());
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
            // Set when the stream's index is given to another stream during
            // the pass: nothing more of it is sent.
            let mut stale = false;
            for peer in &stream_children {
                if stale {
                    break;
                }
                if let Some((from_slot, backfill_parts)) =
                    lane_children.pending_open.remove(&(*peer, stream_id))
                {
                    let backfill_parts = match config.video_backfill_parts {
                        Some(most) if config.stream_class(stream_id) == StreamClass::Video => {
                            backfill_parts.min(most)
                        }
                        _ => backfill_parts,
                    };
                    // A new lease starts live at the next part. The
                    // parts already stored are the warm-up window.
                    let low = from_slot
                        .max(retained_start)
                        .max(last.saturating_add(1).saturating_sub(backfill_parts));
                    lane_children.sent.insert((handle, *peer), last);
                    if low <= last && backfill_parts > 0 {
                        lane_children.backfills.insert(
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
                let cursor = lane_children
                    .sent
                    .get(&(handle, *peer))
                    .copied()
                    .filter(|slot| *slot <= last);
                let next = cursor
                    .and_then(|slot| slot.checked_add(1))
                    .unwrap_or(retained_start)
                    .max(retained_start);
                if next > last {
                    continue;
                }
                // A pair whose parts reach the child another way (the
                // fast plane) is not pushed; its cursor moves on with the
                // stream, so a push that resumes starts at the next part.
                if config
                    .live_push_filter
                    .0
                    .as_ref()
                    .is_some_and(|filter| filter(*peer, stream_id))
                {
                    lane_children.sent.insert((handle, *peer), last);
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

                // The slots of the range that came from a peer, read under
                // one lock.
                let from_peers = if config.role != CacheMeshRole::Distributor {
                    let remote = shared.remote_slots.read().await;
                    if remote.is_empty() {
                        Vec::new()
                    } else {
                        (next..=last)
                            .filter(|slot_id| remote.contains(&(handle, *slot_id)))
                            .collect()
                    }
                } else {
                    Vec::new()
                };
                let batch = batches.entry(*peer).or_default();
                for slot_id in next..=last {
                    if from_peers.binary_search(&slot_id).is_ok() {
                        batch.parts.push(QueuedPart {
                            stream: handle,
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
                        stream_payload(&cache, &mut stream_payloads, handle, slot_id).await
                    else {
                        if cache.last_for_handle(handle).is_none() {
                            stale = true;
                            break;
                        }
                        continue;
                    };
                    chunk_frame_header(
                        &mut frame_header,
                        config.frame_form,
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
                                stream: handle,
                                slot_id,
                                first_message,
                                end_message: batch.messages.len(),
                                live: slot_id >= live_from,
                                warm_up: false,
                                object: Some((object, count)),
                            });
                            if batch.messages.len() >= MESSAGES_PER_CALL {
                                // A full call: a catch-up of many parts does
                                // not hold the first of them back.
                                flush_child(
                                    &socket,
                                    &mut sender,
                                    &mut batch_sender,
                                    *peer,
                                    lane.to(*peer),
                                    Some(batch),
                                    &mut call_times,
                                    &mut lane_children.sent,
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
            }

            // The stream's live parts leave now, one call for each child,
            // before the pass encodes the next stream.
            for peer in &stream_children {
                flush_child(
                    &socket,
                    &mut sender,
                    &mut batch_sender,
                    *peer,
                    lane.to(*peer),
                    batches.get_mut(peer),
                    &mut call_times,
                    &mut lane_children.sent,
                    &config,
                )
                .await;
            }

            // Warm-up windows go after the live parts of the tick,
            // newest part first, within the allowance of each child.
            // Only the tick sends them: it paces them.
            if !tick || stale {
                continue;
            }
            let rotation = warm_rotation % stream_children.len().max(1);
            let (late, early) = stream_children.split_at(rotation);
            for peer in early.iter().chain(late) {
                let key = (*peer, stream_id);
                let Some(mut backfill) = lane_children.backfills.get(&key).copied() else {
                    continue;
                };
                let limited = config.backfill_bytes_per_sec > 0;
                let total_limited = config.backfill_total_bytes_per_sec > 0;
                while let Some(slot_id) = backfill.next {
                    if slot_id < backfill.low.max(retained_start) {
                        backfill.next = None;
                        break;
                    }
                    if limited && backfill_tokens.get(peer).copied().unwrap_or(0.0) <= 0.0 {
                        pass_warm_starved = true;
                        break;
                    }
                    if total_limited
                        && lock(&shared.warm_total[usize::from(lane.is_class())])
                            .available(config.backfill_total_bytes_per_sec, Instant::now())
                            <= 0.0
                    {
                        pass_warm_capped = true;
                        break;
                    }
                    backfill.next = slot_id.checked_sub(1);
                    let Some((payload, hash)) = cache.get_for_handle(handle, slot_id).await else {
                        if cache.last_for_handle(handle).is_none() {
                            backfill.next = None;
                            break;
                        }
                        continue;
                    };
                    if hash == 0 && payload.is_empty() {
                        continue;
                    }
                    let bytes = payload.len();
                    // Sent after the live parts of every stream of the pass.
                    chunk_frame_header(
                        &mut frame_header,
                        config.frame_form,
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
                                stream: handle,
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
                    if limited {
                        if let Some(tokens) = backfill_tokens.get_mut(peer) {
                            *tokens -= bytes as f64;
                        }
                    }
                    if total_limited {
                        lock(&shared.warm_total[usize::from(lane.is_class())])
                            .spend(bytes, config.backfill_total_bytes_per_sec);
                    }
                    pass_warm_bytes = pass_warm_bytes.saturating_add(bytes as u64);
                    let counters = shared.counters_of(lane);
                    counters.warm_parts_sent.fetch_add(1, Ordering::Relaxed);
                    counters
                        .warm_bytes_sent
                        .fetch_add(bytes as u64, Ordering::Relaxed);
                }
                if backfill.next.is_none() {
                    lane_children.backfills.remove(&key);
                } else {
                    lane_children.backfills.insert(key, backfill);
                }
            }
        }

        if tick {
            let counters = shared.counters_of(lane);
            counters
                .warm_tick_max_bytes
                .fetch_max(pass_warm_bytes, Ordering::Relaxed);
            if pass_warm_starved {
                counters.warm_starved_ticks.fetch_add(1, Ordering::Relaxed);
            }
            if pass_warm_capped {
                counters.warm_capped_ticks.fetch_add(1, Ordering::Relaxed);
            }
        }
        // The warm-up parts of the pass, after every live part, each child's
        // in as few calls as the kernel takes them. (The live batches are
        // empty here: each stream sent its own.)
        for (peer, batch) in batches.iter_mut().chain(warm_batches.iter_mut()) {
            if batch.is_empty() {
                continue;
            }
            flush_child(
                &socket,
                &mut sender,
                &mut batch_sender,
                *peer,
                lane.to(*peer),
                Some(batch),
                &mut call_times,
                &mut lane_children.sent,
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

/// The prune of a lane, every `REMOTE_SLOT_PRUNE_INTERVAL`: the main lane
/// forgets the remote slots that the cache no longer retains (both lanes
/// share them), and each lane forgets the cursors of released streams and
/// of peers that are gone.
async fn prune_lane(
    cache: &ChunkCache,
    shared: &MeshShared,
    lane: Lane,
    lane_children: &mut LaneChildren,
    streams: &[StreamHandle],
) {
    if !lane.is_class() {
        forget_unretained_remote_slots(cache, &shared.remote_slots).await;
    }
    let peers = shared.peers.read().await.clone();
    forget_dead_cursors(
        &mut lane_children.sent,
        &mut lane_children.sent_initializations,
        streams,
        &peers,
    );
    #[cfg(test)]
    if !lane.is_class() {
        record_cursor_streams(
            shared,
            &lane_children.sent,
            &lane_children.sent_initializations,
        );
    }
}

#[cfg(test)]
fn record_cursor_streams(
    shared: &MeshShared,
    sent: &HashMap<(StreamHandle, SocketAddr), usize>,
    sent_initializations: &HashMap<(StreamHandle, SocketAddr), Bytes>,
) {
    *lock(&shared.cursor_streams) = sent
        .keys()
        .chain(sent_initializations.keys())
        .map(|(stream, _)| *stream)
        .collect();
}

/// Forget the cursors and initializations sent for streams that the cache no
/// longer holds in that lifetime, and for peers that are gone.
fn forget_dead_cursors(
    sent: &mut HashMap<(StreamHandle, SocketAddr), usize>,
    sent_initializations: &mut HashMap<(StreamHandle, SocketAddr), Bytes>,
    streams: &[StreamHandle],
    peers: &HashSet<SocketAddr>,
) {
    if sent.is_empty() && sent_initializations.is_empty() {
        return;
    }
    let live = streams.iter().copied().collect::<HashSet<_>>();
    sent.retain(|(stream, peer), _| live.contains(stream) && peers.contains(peer));
    sent_initializations.retain(|(stream, peer), _| live.contains(stream) && peers.contains(peer));
}

/// Forget the streams that the cache no longer holds (in that lifetime):
/// from the playlists that a lane owns or waits for, and (for the main lane,
/// `owners_too`) from the owners of the handover table.
fn forget_released_streams(
    owns: &mut HashSet<StreamHandle>,
    waiting: &mut HashSet<StreamHandle>,
    handover: &LaneHandover,
    owners_too: bool,
    streams: &[StreamHandle],
) {
    let mut owners = owners_too.then(|| lock(&handover.owners));
    if owns.is_empty()
        && waiting.is_empty()
        && owners.as_ref().is_none_or(|owners| owners.is_empty())
    {
        return;
    }
    let current = streams.iter().copied().collect::<HashSet<_>>();
    owns.retain(|stream| current.contains(stream));
    waiting.retain(|stream| current.contains(stream));
    if let Some(owners) = owners.as_mut() {
        owners.retain(|stream, _| current.contains(stream));
    }
}

/// Forget a child whose lease of a playlist ended, from the state that a lane
/// handed over and the other lane has not taken yet.
fn forget_child_in_transit(handover: &LaneHandover, stream_id: u64, peer: &SocketAddr) {
    let mut owners = lock(&handover.owners);
    for (stream, owner) in owners.iter_mut() {
        if stream.stream_id() != stream_id {
            continue;
        }
        if let StreamOwner::InTransit(state) = owner {
            state.forget_child(peer);
        }
    }
}

/// The payload of `slot_id` of a stream and its hash, read from the cache
/// once for every child of the pass. `None` for a slot that the cache does
/// not hold.
async fn stream_payload(
    cache: &ChunkCache,
    payloads: &mut HashMap<usize, Option<(Bytes, u64)>>,
    handle: StreamHandle,
    slot_id: usize,
) -> Option<(Bytes, u64)> {
    if let Some(payload) = payloads.get(&slot_id) {
        return payload.clone();
    }
    let payload = match cache.get_for_handle(handle, slot_id).await {
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
    dest: SocketAddr,
    batch: Option<&mut ChildBatch>,
    call_times: &mut Vec<(usize, u64)>,
    sent: &mut HashMap<(StreamHandle, SocketAddr), usize>,
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
        .send(socket, dest, &batch.messages, call_times)
        .await;
    sender
        .stats
        .tx_send_calls
        .fetch_add(call_times.len() as u64, Ordering::Relaxed);
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
        sent.insert((part.stream, peer), part.slot_id);
        if part.live && part.end_message > part.first_message {
            if let Some(observer) = &config.send_observer.0 {
                observer(
                    part.stream.stream_id(),
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
        let decoded = MeshFrame::decode(encoded).unwrap();
        assert_eq!(decoded, frame);

        let frame = MeshFrame::initialization("uk", 42, Bytes::from_static(b"ftyp-moov-init"));
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(encoded).unwrap();
        assert_eq!(decoded, frame);

        let frame = MeshFrame::hello_with_peers("us", "us-east", Vec::new());
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(encoded).unwrap();
        assert_eq!(decoded, frame);

        let peer: SocketAddr = "127.0.0.1:9911".parse().unwrap();
        let frame = MeshFrame::hello_with_peers("us", "us-east", vec![peer]);
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(encoded).unwrap();
        assert_eq!(decoded, frame);

        let frame = MeshFrame::replica_request("jp", 42, 3);
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    /// One wire format: a version 1 frame, a hello without its role and a
    /// replica request without its count are refused.
    #[test]
    fn only_the_current_wire_format_is_accepted() {
        let mut v1_hello = BytesMut::new();
        v1_hello.put_slice(FRAME_MAGIC);
        v1_hello.put_u8(1);
        v1_hello.put_u8(FRAME_HELLO);
        v1_hello.put_u16(2);
        v1_hello.put_slice(b"uk");
        v1_hello.put_u16(7);
        v1_hello.put_slice(b"uk-west");
        assert!(MeshFrame::decode(v1_hello.freeze()).is_err());

        let mut hello = BytesMut::new();
        hello.put_slice(FRAME_MAGIC);
        hello.put_u8(FRAME_VERSION);
        hello.put_u8(FRAME_HELLO);
        hello.put_u16(2);
        hello.put_slice(b"uk");
        hello.put_u16(7);
        hello.put_slice(b"uk-west");
        hello.put_u16(0);
        assert!(MeshFrame::decode(hello.clone().freeze()).is_err());
        hello.put_u8(CacheMeshRole::Edge.wire_value());
        assert_eq!(
            MeshFrame::decode(hello.freeze()).unwrap(),
            MeshFrame::hello_with_role("uk", "uk-west", CacheMeshRole::Edge, Vec::new())
        );

        let request = MeshFrame::replica_request("jp", 42, 3).encode().unwrap();
        assert_eq!(request.len(), 12 + 2 + 20);
        assert!(MeshFrame::decode(request.slice(..request.len() - 4)).is_err());

        let chunk = MeshFrame::chunk("uk", 42, 7, Bytes::from_static(b"media"))
            .encode()
            .unwrap();
        let mut v1_chunk = chunk.to_vec();
        v1_chunk[FRAME_MAGIC.len()] = 1;
        assert!(MeshFrame::decode(Bytes::from(v1_chunk.clone())).is_err());
        assert_eq!(chunk_frame_slot(&v1_chunk), None);
        assert_eq!(chunk_frame_slot(&chunk), Some((42, 7)));
    }

    /// A decoded payload is a slice of the decoded frame, not a copy.
    #[test]
    fn a_decoded_payload_shares_the_frame_buffer() {
        let encoded = MeshFrame::chunk("uk", 42, 7, Bytes::from(vec![5_u8; 600]))
            .encode()
            .unwrap();
        let range = encoded.as_ptr_range();
        let MeshFrame::Chunk { payload, .. } = MeshFrame::decode(encoded.clone()).unwrap() else {
            panic!("a chunk frame");
        };
        assert!(range.contains(&payload.as_ptr()));
        // The public parser copies: its caller keeps its own buffer.
        let (_, _, copied) = chunk_frame_payload(&encoded).unwrap();
        assert_eq!(copied, payload);
        assert!(!range.contains(&copied.as_ptr()));
    }

    /// `frame` with another version byte.
    fn as_version(frame: &[u8], version: u8) -> Bytes {
        let mut bytes = frame.to_vec();
        bytes[FRAME_MAGIC.len()] = version;
        Bytes::from(bytes)
    }

    /// `frame` with `extra` bytes after it.
    fn followed_by(frame: &[u8], extra: &[u8]) -> Bytes {
        let mut bytes = frame.to_vec();
        bytes.extend_from_slice(extra);
        Bytes::from(bytes)
    }

    /// One frame of every kind, each with every field that this build
    /// writes.
    fn a_frame_of_every_kind() -> Vec<MeshFrame> {
        vec![
            MeshFrame::Hello {
                node_id: "lon-dist-a".into(),
                region: "london".into(),
                role: WireRole::Known(CacheMeshRole::Distributor),
                peers: vec!["10.80.3.21:29101".parse().unwrap()],
                draining: true,
                boot_id: Some(1_763_924_000_123_456),
                build: Some("3f2a9c1d77e0-dirty".into()),
                state: Some(1),
            },
            MeshFrame::chunk("uk", 42, 7, Bytes::from_static(b"media")),
            MeshFrame::initialization("uk", 42, Bytes::from_static(b"ftyp-moov-init")),
            MeshFrame::ReplicaRequest {
                node_id: "jp".into(),
                stream_id: 42,
                from_slot: 3,
                count: 5,
            },
            MeshFrame::LinkReport {
                node_id: "jp".into(),
                loss_ppm: 12_000,
            },
            MeshFrame::Subscribe {
                node_id: "edge-1".into(),
                stream_id: 2001,
                from_slot: 77,
                backfill_parts: 32,
                lease_ms: 5_000,
                rank: Some(1),
                boot_id: Some(1_763_924_000_123_456),
            },
            MeshFrame::Unsubscribe {
                node_id: "edge-1".into(),
                stream_id: 2001,
            },
        ]
    }

    /// The version byte is the newest revision that the sender speaks: a
    /// frame of any later version is read as this build's.
    #[test]
    fn a_frame_of_a_later_version_is_read() {
        for frame in a_frame_of_every_kind() {
            let encoded = frame.encode().unwrap();
            assert_eq!(encoded[FRAME_MAGIC.len()], FRAME_VERSION);
            for version in [FRAME_VERSION + 1, 9, u8::MAX] {
                assert_eq!(
                    MeshFrame::decode(as_version(&encoded, version)).unwrap(),
                    frame,
                    "version {version}"
                );
            }
            // What a node that speaks like a later build writes.
            let future = frame.encode_as(FrameForm::Future).unwrap();
            assert_eq!(future[FRAME_MAGIC.len()], FUTURE_FRAME_VERSION);
            assert!(future.len() >= encoded.len());
            assert_eq!(MeshFrame::decode(future).unwrap(), frame);
        }
    }

    /// Every kind reads the fields that it knows and ignores what a later
    /// build put after them; a frame cut short of those fields is refused.
    #[test]
    fn every_frame_ignores_what_follows_its_fields() {
        for frame in a_frame_of_every_kind() {
            let encoded = frame.encode().unwrap();
            for extra in [1_usize, 2, 3, 7, 40] {
                assert_eq!(
                    MeshFrame::decode(followed_by(&encoded, &vec![0xa5; extra])).unwrap(),
                    frame,
                    "{extra} bytes after {frame:?}"
                );
            }
        }
        // The payload of a chunk and of an initialization is the first
        // `payload_len` bytes.
        let chunk = MeshFrame::chunk("uk", 42, 7, Bytes::from_static(b"media"))
            .encode()
            .unwrap();
        assert_eq!(
            chunk_frame_payload(&followed_by(&chunk, b"later")),
            Some((42, 7, Bytes::from_static(b"media")))
        );
        // Without the rank and the boot id a subscribe is 24 bytes; one byte
        // more is the rank, and nine more the rank and the boot id.
        let subscribe = MeshFrame::Subscribe {
            node_id: "edge-1".into(),
            stream_id: 2001,
            from_slot: 77,
            backfill_parts: 32,
            lease_ms: 5_000,
            rank: None,
            boot_id: None,
        }
        .encode()
        .unwrap();
        let mut tail = vec![0_u8];
        tail.extend_from_slice(&7_u64.to_be_bytes());
        tail.extend_from_slice(b"later");
        assert!(matches!(
            MeshFrame::decode(followed_by(&subscribe, &tail)).unwrap(),
            MeshFrame::Subscribe {
                rank: Some(0),
                boot_id: Some(7),
                ..
            }
        ));
        // Eight bytes after the lease are a rank and not yet a boot id.
        assert!(matches!(
            MeshFrame::decode(followed_by(&subscribe, &tail[..8])).unwrap(),
            MeshFrame::Subscribe {
                rank: Some(0),
                boot_id: None,
                ..
            }
        ));
        // Cut short of the fields that this build reads, each is refused.
        for frame in a_frame_of_every_kind() {
            let encoded = frame.encode().unwrap();
            let fixed = match &frame {
                // The node id, the region and the peer count.
                MeshFrame::Hello { .. } => continue,
                MeshFrame::Chunk { payload, .. } => 20 + payload.len(),
                MeshFrame::Initialization { payload, .. } => 12 + payload.len(),
                MeshFrame::ReplicaRequest { .. } => 20,
                MeshFrame::LinkReport { .. } => 4,
                MeshFrame::Subscribe { .. } => 24,
                MeshFrame::Unsubscribe { .. } => 8,
            };
            let head = FRAME_MAGIC.len() + 2 + 2 + frame.node_id().len();
            assert!(
                MeshFrame::decode(encoded.slice(..head + fixed)).is_ok(),
                "{frame:?} with its fields"
            );
            assert!(
                matches!(
                    MeshFrame::decode(encoded.slice(..head + fixed - 1)),
                    Err(MeshError::InvalidFrame(_))
                ),
                "{frame:?} cut short"
            );
        }
    }

    /// Counts the warnings and errors logged on the thread whose default
    /// subscriber it is.
    struct WarningCounter(Arc<AtomicU64>);

    impl tracing::Subscriber for WarningCounter {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() <= tracing::Level::WARN
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// A frame of a kind that this build does not know is recognised before
    /// its node id is read, counted, and not logged: a later build may send
    /// one a second to every peer for the length of a roll. A frame that
    /// does not decode for another reason is counted too, and its peer is
    /// named in one warning in ten seconds.
    #[tokio::test]
    async fn an_unknown_kind_is_counted_not_logged_and_its_body_is_not_parsed() {
        let unknown = MeshFrame::of_unknown_kind();
        // The body is not a node id: read as one, the frame would be invalid.
        assert!(matches!(
            read_string(&mut unknown.slice(FRAME_MAGIC.len() + 2..)),
            Err(MeshError::InvalidFrame(_))
        ));
        assert!(matches!(
            MeshFrame::decode(unknown.clone()),
            Err(MeshError::UnknownKind(FUTURE_FRAME_KIND))
        ));
        // With no body at all, and in this build's version too.
        for kind in [0_u8, FRAME_LINK_REPORT + 1, FUTURE_FRAME_KIND, u8::MAX] {
            let mut bare = FRAME_MAGIC.to_vec();
            bare.extend_from_slice(&[FRAME_VERSION, kind]);
            assert!(
                matches!(
                    MeshFrame::decode(Bytes::from(bare)),
                    Err(MeshError::UnknownKind(found)) if found == kind
                ),
                "kind {kind}"
            );
        }

        let warnings = Arc::new(AtomicU64::new(0));
        // The mesh's tasks run on this thread: the runtime is current-thread.
        let _subscriber = tracing::subscriber::set_default(WarningCounter(Arc::clone(&warnings)));
        let bind_addr = loopback_addr();
        let peer_addr = loopback_addr();
        let mesh = CacheMesh::new(
            Arc::new(ChunkCache::new(Options::default())),
            CacheMeshConfig::new("node", "uk", bind_addr).with_peer(peer_addr),
        )
        .start()
        .await
        .unwrap();
        let socket = UdpSocket::bind(peer_addr).await.unwrap();
        let mut sender = FecSender::new(0, DEFAULT_SYMBOL_SIZE);
        for _ in 0..3 {
            sender.send(&socket, bind_addr, &unknown).await.unwrap();
        }
        timeout(Duration::from_secs(2), async {
            while mesh.fec_stats().rx_unknown_kinds < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the frames of an unknown kind were not counted");
        assert_eq!(mesh.fec_stats().rx_invalid_frames, 0);
        assert_eq!(warnings.load(Ordering::Relaxed), 0);

        // A hello without its role is invalid.
        let hello = MeshFrame::hello_with_peers("peer", "uk", Vec::new())
            .encode()
            .unwrap();
        let invalid = hello.slice(..hello.len() - 1);
        for _ in 0..3 {
            sender.send(&socket, bind_addr, &invalid).await.unwrap();
        }
        timeout(Duration::from_secs(2), async {
            while mesh.fec_stats().rx_invalid_frames < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the invalid frames were not counted");
        assert_eq!(warnings.load(Ordering::Relaxed), 1);
        assert_eq!(mesh.fec_stats().rx_unknown_kinds, 3);
        mesh.shutdown();
    }

    /// On a mesh split by class, as av-mesh runs it, a peer whose frames do
    /// not decode on both sockets is still named in one warning in ten
    /// seconds: the two receive tasks share one record. The class lane runs
    /// on a thread of its own, so its warning is not seen here; the main
    /// lane's would be (as the test above shows), and is not made when the
    /// class lane has named the peer first.
    #[tokio::test]
    async fn a_peer_s_invalid_frames_on_both_sockets_make_one_warning() {
        let warnings = Arc::new(AtomicU64::new(0));
        // The main lane's tasks run on this thread: the runtime is
        // current-thread.
        let _subscriber = tracing::subscriber::set_default(WarningCounter(Arc::clone(&warnings)));
        let class_of =
            |addr: SocketAddr| SocketAddr::new(addr.ip(), addr.port() + TEST_CLASS_OFFSET);
        let bind_addr = class_split_addr(TEST_CLASS_OFFSET);
        let peer_addr = class_split_addr(TEST_CLASS_OFFSET);
        let mesh = CacheMesh::new(
            Arc::new(ChunkCache::new(Options::default())),
            CacheMeshConfig::new("node", "uk", bind_addr)
                .with_peer(peer_addr)
                .with_class_port_offset(TEST_CLASS_OFFSET),
        )
        .start()
        .await
        .unwrap();
        let main = UdpSocket::bind(peer_addr).await.unwrap();
        let class = UdpSocket::bind(class_of(peer_addr)).await.unwrap();
        let hello = MeshFrame::hello_with_peers("peer", "uk", Vec::new())
            .encode()
            .unwrap();
        let invalid = hello.slice(..hello.len() - 1);
        let mut sender = FecSender::new(0, DEFAULT_SYMBOL_SIZE);
        for _ in 0..3 {
            sender
                .send(&class, class_of(bind_addr), &invalid)
                .await
                .unwrap();
        }
        timeout(Duration::from_secs(2), async {
            while mesh.class_fec_stats().unwrap().rx_invalid_frames < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the class lane did not count the invalid frames");
        for _ in 0..3 {
            sender.send(&main, bind_addr, &invalid).await.unwrap();
        }
        timeout(Duration::from_secs(2), async {
            while mesh.fec_stats().rx_invalid_frames < 6 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the main lane did not count the invalid frames");
        assert_eq!(warnings.load(Ordering::Relaxed), 0);
        assert_eq!(lock(&mesh.shared.invalid_frame_warnings).0.len(), 1);
        mesh.shutdown();
    }

    /// The kind of a frame and, for a chunk, its slot, from the datagram
    /// that carries the first source symbol of the frame's block. `None`
    /// for any other datagram.
    fn frame_start(datagram: &[u8]) -> Option<(u8, Option<u64>)> {
        if datagram.len() < HEADER_LEN + 4 + FRAME_MAGIC.len() + 2
            || DatagramFecHeader::decode(datagram).is_err()
            || datagram[HEADER_LEN + 1..HEADER_LEN + 4] != [0, 0, 0]
        {
            return None;
        }
        let frame = &datagram[HEADER_LEN + 4..];
        (&frame[..FRAME_MAGIC.len()] == FRAME_MAGIC).then(|| {
            (
                frame[FRAME_MAGIC.len() + 1],
                chunk_frame_slot(frame).map(|(_, slot_id)| slot_id),
            )
        })
    }

    /// The frames that start to arrive at `socket` within `wait` (see
    /// [`frame_start`]).
    async fn frames_arriving(socket: &UdpSocket, wait: Duration) -> Vec<(u8, Option<u64>)> {
        let mut frames = Vec::new();
        let mut buf = vec![0_u8; 65_536];
        let end = Instant::now() + wait;
        while let Ok(Ok((len, _))) = tokio::time::timeout_at(end, socket.recv_from(&mut buf)).await
        {
            frames.extend(frame_start(&buf[..len]));
        }
        frames
    }

    /// The slots of the chunk frames that start to arrive at `socket`
    /// within `wait`.
    async fn chunks_arriving(socket: &UdpSocket, wait: Duration) -> Vec<u64> {
        frames_arriving(socket, wait)
            .await
            .into_iter()
            .filter_map(|(_, slot_id)| slot_id)
            .collect()
    }

    /// A peer whose hello gives a role that this build does not know (a
    /// later build's) is heard and counted. It is not a parent: it is not
    /// ranked, and its parts are not applied. It is not a child: its
    /// subscription is not served and no part is pushed to it. Before, the
    /// hello did not decode, the peer's role stayed unknown, and a peer of
    /// unknown role is both.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unknown_role_is_heard_and_is_neither_parent_nor_child() {
        assert_eq!(
            CacheMeshRole::from_wire(HELLO_DRAINING_BIT | 0x72),
            WireRole::Known(CacheMeshRole::Edge),
            "bits 4 to 6 are ignored"
        );
        assert_eq!(CacheMeshRole::from_wire(9), WireRole::Other(9));
        for own in [
            CacheMeshRole::Peer,
            CacheMeshRole::Distributor,
            CacheMeshRole::Edge,
        ] {
            assert!(!WireRole::may_be_parent(Some(WireRole::Other(9)), own));
            assert!(!WireRole::may_be_child(Some(WireRole::Other(9)), own));
        }

        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let flat_addr = loopback_addr();
        let stranger_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            demand_distributor("dist", distributor_addr).with_peers([edge_addr, stranger_addr]),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            demand_edge("edge", edge_addr).with_peers([distributor_addr, stranger_addr]),
        )
        .start()
        .await
        .unwrap();
        // A flat mesh of equal nodes sends every part to every peer and
        // takes parts from every peer.
        let flat_cache = Arc::new(ChunkCache::new(demand_options()));
        let mut flat_config =
            CacheMeshConfig::new("flat", "uk", flat_addr).with_peer(stranger_addr);
        flat_config.sync_interval = Duration::from_millis(10);
        let flat = CacheMesh::new(Arc::clone(&flat_cache), flat_config)
            .start()
            .await
            .unwrap();

        let stranger = UdpSocket::bind(stranger_addr).await.unwrap();
        let mut sender = FecSender::new(0, DEFAULT_SYMBOL_SIZE);
        let hello_as = |role: WireRole| {
            MeshFrame::Hello {
                node_id: "stranger".into(),
                region: "uk".into(),
                role,
                peers: Vec::new(),
                draining: false,
                boot_id: Some(1),
                build: None,
                state: None,
            }
            .encode()
            .unwrap()
        };
        let hello = hello_as(WireRole::Other(9));
        assert_eq!(
            MeshFrame::decode(hello.clone()).unwrap().encode().unwrap(),
            hello
        );
        for node in [distributor_addr, edge_addr, flat_addr] {
            sender.send(&stranger, node, &hello).await.unwrap();
        }
        timeout(Duration::from_secs(2), async {
            while [&distributor, &edge, &flat]
                .iter()
                .any(|node| node.fec_stats().rx_unknown_roles == 0)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the hello of an unknown role was not counted");
        wait_for_hello(&edge, 2).await;

        // Heard: a peer, seen, with no frame refused.
        for node in [&distributor, &edge, &flat] {
            assert!(node.peers().await.contains(&stranger_addr));
            assert!(lock(&node.shared.peer_seen).contains_key(&stranger_addr));
            assert_eq!(node.fec_stats().rx_invalid_frames, 0);
        }

        // Not a parent of the edge, which ranks its one distributor alone.
        assert_eq!(edge.parents_for(7).await, vec![distributor_addr]);
        assert_eq!(
            edge.subscribe(7, 0, 4).await.unwrap(),
            vec![distributor_addr]
        );

        // Not a child of the distributor: its subscription opens no lease.
        let subscribe = MeshFrame::Subscribe {
            node_id: "stranger".into(),
            stream_id: 7,
            from_slot: 0,
            backfill_parts: 4,
            lease_ms: 5_000,
            rank: Some(0),
            boot_id: Some(1),
        }
        .encode()
        .unwrap();
        sender
            .send(&stranger, distributor_addr, &subscribe)
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            while distributor.served_subscriptions().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the edge's lease was not served");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let served = distributor.served_subscriptions();
        assert_eq!(served.len(), 1, "{served:?}");
        assert_eq!(served[0].child, edge_addr);

        // Not a child of the flat node: a part written there is not pushed.
        flat_cache
            .add_for_stream_id(9, 0, Bytes::from(vec![9_u8; 64]))
            .await
            .unwrap();
        assert_eq!(
            chunks_arriving(&stranger, Duration::from_millis(150)).await,
            Vec::<u64>::new()
        );
        // Not a parent of the flat node: its part is not applied.
        let part = MeshFrame::chunk("stranger", 11, 0, Bytes::from(vec![11_u8; 64]))
            .encode()
            .unwrap();
        sender.send(&stranger, flat_addr, &part).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(flat_cache.get_for_stream_id(11, 0).await.is_none());

        // The same peer under a role that this build knows is both.
        sender
            .send(
                &stranger,
                flat_addr,
                &hello_as(WireRole::Known(CacheMeshRole::Peer)),
            )
            .await
            .unwrap();
        assert!(chunks_arriving(&stranger, Duration::from_secs(1))
            .await
            .contains(&0));
        sender.send(&stranger, flat_addr, &part).await.unwrap();
        wait_for_slot(&flat_cache, 11, 0).await;

        distributor.shutdown();
        edge.shutdown();
        flat.shutdown();
    }

    /// The extensions of a hello: the boot id, the build and the state
    /// after the role byte, each as tag, length and value. A tag that this
    /// build does not know is skipped, wherever it stands.
    #[test]
    fn hello_extensions_round_trip_and_unknown_tags_are_skipped() {
        let full = MeshFrame::Hello {
            node_id: "lon-dist-a".into(),
            region: "london".into(),
            role: WireRole::Known(CacheMeshRole::Distributor),
            peers: Vec::new(),
            draining: false,
            boot_id: Some(1_763_924_000_123_456),
            build: Some("3f2a9c1d77e0".into()),
            state: Some(3),
        };
        let plain = MeshFrame::hello_with_role(
            "lon-dist-a",
            "london",
            CacheMeshRole::Distributor,
            Vec::new(),
        );
        let encoded = full.encode().unwrap();
        let bare = plain.encode().unwrap();
        // 3 bytes of tag and length for each, and 8, 12 and 1 of value.
        assert_eq!(encoded.len(), bare.len() + 11 + 15 + 4);
        assert_eq!(MeshFrame::decode(encoded.clone()).unwrap(), full);
        assert_eq!(MeshFrame::decode(bare.clone()).unwrap(), plain);
        // Each on its own.
        for (boot_id, build, state) in [
            (Some(7_u64), None, None),
            (None, Some(String::from("b")), None),
            (None, None, Some(0_u8)),
        ] {
            let mut one = plain.clone();
            if let MeshFrame::Hello {
                boot_id: boot,
                build: named,
                state: said,
                ..
            } = &mut one
            {
                (*boot, *named, *said) = (boot_id, build, state);
            }
            assert_eq!(MeshFrame::decode(one.encode().unwrap()).unwrap(), one);
        }

        // Unknown tags before, between and after the known ones.
        let mut bytes = bare.to_vec();
        bytes.extend_from_slice(&[FUTURE_HELLO_EXTENSION, 0, 3, 1, 2, 3]);
        bytes.extend_from_slice(&[HELLO_EXTENSION_BOOT_ID, 0, 8]);
        bytes.extend_from_slice(&1_763_924_000_123_456_u64.to_be_bytes());
        bytes.extend_from_slice(&[0x50, 0, 0]);
        bytes.extend_from_slice(&[HELLO_EXTENSION_BUILD, 0, 12]);
        bytes.extend_from_slice(b"3f2a9c1d77e0");
        bytes.extend_from_slice(&[HELLO_EXTENSION_STATE, 0, 1, 3]);
        bytes.extend_from_slice(&[0xee, 0, 2, 9, 9]);
        assert_eq!(MeshFrame::decode(Bytes::from(bytes)).unwrap(), full);
        // What a node that speaks like a later build appends is skipped.
        let future = full.encode_as(FrameForm::Future).unwrap();
        assert_eq!(future.len(), encoded.len() + 5);
        assert_eq!(MeshFrame::decode(future).unwrap(), full);

        // A value longer than this build reads: its first bytes.
        let mut bytes = bare.to_vec();
        bytes.extend_from_slice(&[HELLO_EXTENSION_BOOT_ID, 0, 10]);
        bytes.extend_from_slice(&7_u64.to_be_bytes());
        bytes.extend_from_slice(&[1, 2]);
        bytes.extend_from_slice(&[HELLO_EXTENSION_STATE, 0, 2, 4, 9]);
        assert!(matches!(
            MeshFrame::decode(Bytes::from(bytes)).unwrap(),
            MeshFrame::Hello {
                boot_id: Some(7),
                state: Some(4),
                build: None,
                ..
            }
        ));
        // A value that this build cannot read is skipped, and the hello
        // stands: a boot id of four bytes, a build that is not UTF-8 or is
        // longer than 64 bytes.
        let mut bytes = bare.to_vec();
        bytes.extend_from_slice(&[HELLO_EXTENSION_BOOT_ID, 0, 4, 1, 2, 3, 4]);
        bytes.extend_from_slice(&[HELLO_EXTENSION_BUILD, 0, 2, 0xff, 0xfe]);
        bytes.extend_from_slice(&[HELLO_EXTENSION_BUILD, 0, 65]);
        bytes.extend_from_slice(&[b'a'; 65]);
        bytes.extend_from_slice(&[HELLO_EXTENSION_STATE, 0, 1, 2]);
        assert!(matches!(
            MeshFrame::decode(Bytes::from(bytes)).unwrap(),
            MeshFrame::Hello {
                boot_id: None,
                build: None,
                state: Some(2),
                ..
            }
        ));
        // A node does not send a build id beyond the limit.
        let mut long = full.clone();
        if let MeshFrame::Hello { build, .. } = &mut long {
            *build = Some("a".repeat(HELLO_BUILD_MAX_BYTES + 1));
        }
        assert!(long.encode().is_err());
    }

    /// An extension that is cut short ends the list without an error: the
    /// hello stands with what came before it.
    #[test]
    fn a_truncated_extension_ends_the_list() {
        let plain =
            MeshFrame::hello_with_role("dist-c", "uk", CacheMeshRole::Distributor, Vec::new());
        let bare = plain.encode().unwrap();
        let mut booted = bare.to_vec();
        booted.extend_from_slice(&[HELLO_EXTENSION_BOOT_ID, 0, 8]);
        booted.extend_from_slice(&7_u64.to_be_bytes());
        let with_boot_id = |frame: Bytes| {
            let frame = MeshFrame::decode(frame).unwrap();
            assert!(
                matches!(
                    &frame,
                    MeshFrame::Hello {
                        boot_id: Some(7),
                        build: None,
                        state: None,
                        ..
                    }
                ),
                "{frame:?}"
            );
        };
        with_boot_id(Bytes::from(booted.clone()));
        // A tag alone, a tag and half a length, a length beyond the frame.
        with_boot_id(followed_by(&booted, &[HELLO_EXTENSION_STATE]));
        with_boot_id(followed_by(&booted, &[HELLO_EXTENSION_STATE, 0]));
        with_boot_id(followed_by(&booted, &[HELLO_EXTENSION_STATE, 0, 5, 1]));
        // Nothing after the cut is read, even if it looks like an extension.
        with_boot_id(followed_by(
            &booted,
            &[0x50, 0, 9, HELLO_EXTENSION_STATE, 0, 1, 4],
        ));
        // The first extension cut short: the hello of before.
        for cut in 1..11 {
            assert_eq!(
                MeshFrame::decode(Bytes::from(booted[..bare.len() + cut].to_vec())).unwrap(),
                plain,
                "{cut} bytes of the boot id extension"
            );
        }
    }

    /// The prefix reader, which an edge's cut-through and a UDP client use
    /// on the first symbol of a block, follows the same rule as the decoder.
    #[test]
    fn the_chunk_prefix_of_a_later_version_is_read() {
        let payload = Bytes::from_static(b"media");
        let chunk = MeshFrame::chunk("uk", 42, 7, payload.clone())
            .encode()
            .unwrap();
        for version in [FRAME_VERSION, FRAME_VERSION + 1, 9, u8::MAX] {
            let later = as_version(&chunk, version);
            assert_eq!(chunk_frame_slot(&later), Some((42, 7)), "version {version}");
            assert_eq!(
                chunk_frame_payload(&later),
                Some((42, 7, payload.clone())),
                "version {version}"
            );
        }
        // The head that the sync path of a later-speaking node writes.
        let mut head = Vec::new();
        chunk_frame_header(&mut head, FrameForm::Future, "uk", 42, 7, payload.len());
        assert_eq!(head[FRAME_MAGIC.len()], FUTURE_FRAME_VERSION);
        assert_eq!(chunk_frame_slot(&head), Some((42, 7)));
        chunk_frame_header(&mut head, FrameForm::Current, "uk", 42, 7, payload.len());
        assert_eq!(&head[..], &chunk[..head.len()]);
        // An earlier version, and any other kind, are still not chunks.
        assert_eq!(
            chunk_frame_slot(&as_version(&chunk, FRAME_VERSION - 1)),
            None
        );
        let hello = MeshFrame::hello_with_peers("uk", "uk", Vec::new())
            .encode()
            .unwrap();
        assert_eq!(chunk_frame_slot(&as_version(&hello, 9)), None);
        assert_eq!(chunk_frame_slot(&MeshFrame::of_unknown_kind()), None);
    }

    /// The mixed-build proof in one process: a node that speaks like a
    /// later build (`with_test_future_frames`) and a node of this build
    /// serve each other, as parent and as child, and the node of this build
    /// counts the frames of the unknown kind and refuses nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_node_that_speaks_like_a_later_build_is_served_and_serves() {
        for future_parent in [true, false] {
            let parent_addr = loopback_addr();
            let edge_addr = loopback_addr();
            let parent_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            let parent = CacheMesh::new(
                Arc::clone(&parent_cache),
                demand_distributor("parent", parent_addr)
                    .with_peer(edge_addr)
                    .with_test_future_frames(future_parent),
            )
            .start()
            .await
            .unwrap();
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                demand_edge("edge", edge_addr)
                    .with_peer(parent_addr)
                    .with_test_future_frames(!future_parent),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            parent_cache
                .set_stream_initialization(7, Bytes::from_static(b"ftyp-moov"))
                .await
                .unwrap();
            for slot in 0..4 {
                parent_cache
                    .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 64]))
                    .await
                    .unwrap();
            }
            // The warm-up window, the initialization, a live part and a
            // hole repair, each way.
            assert_eq!(edge.subscribe(7, 0, 16).await.unwrap(), vec![parent_addr]);
            for slot in 0..4 {
                wait_for_slot(&edge_cache, 7, slot).await;
            }
            assert_eq!(
                edge_cache.stream_initialization(7).as_deref(),
                Some(&b"ftyp-moov"[..])
            );
            parent_cache
                .add_for_stream_id(7, 4, Bytes::from(vec![4_u8; 64]))
                .await
                .unwrap();
            wait_for_slot(&edge_cache, 7, 4).await;
            assert!(edge_cache.clear_for_stream_id(7, 2).await);
            assert_eq!(edge.request_parts(7, 2, 1, 1).await.unwrap(), 1);
            wait_for_slot(&edge_cache, 7, 2).await;
            let served = parent.served_subscriptions();
            assert_eq!(served.len(), 1);
            assert_eq!(served[0].rank, Some(0));

            let (current, later) = if future_parent {
                (&edge, &parent)
            } else {
                (&parent, &edge)
            };
            timeout(Duration::from_secs(3), async {
                while current.fec_stats().rx_unknown_kinds == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the node of this build counted no frame of an unknown kind");
            assert_eq!(current.fec_stats().rx_invalid_frames, 0);
            assert_eq!(current.fec_stats().rx_unknown_roles, 0);
            assert_eq!(later.fec_stats().rx_invalid_frames, 0);
            assert_eq!(later.fec_stats().rx_unknown_kinds, 0);
            parent.shutdown();
            edge.shutdown();
        }
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
        assert!(edge.shared.remote_slots.read().await.is_empty());

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

        let mut config_a = CacheMeshConfig::new("uk", "uk", addr_a)
            .with_peer(addr_b)
            .with_allowed_peers([addr_c]);
        config_a.announce_interval = Duration::from_millis(20);
        config_a.sync_interval = Duration::from_secs(60);
        let mut config_b = CacheMeshConfig::new("us", "us", addr_b)
            .with_peer(addr_a)
            .with_peer(addr_c);
        config_b.announce_interval = Duration::from_millis(20);
        config_b.sync_interval = Duration::from_secs(60);
        let mut config_c = CacheMeshConfig::new("jp", "jp", addr_c)
            .with_peer(addr_b)
            .with_allowed_peers([addr_a]);
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
        let mut sender = FecSender::new(1, DEFAULT_SYMBOL_SIZE);
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
        assert!(!receiver.loss.contains_key(&peer));
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

    #[tokio::test]
    async fn replica_response_stops_when_its_stream_index_is_reused() {
        let cache = Arc::new(ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        }));
        let old = cache.resolve_or_create_stream(801).await;
        cache
            .add_for_handle(old, 0, Bytes::from_static(b"old-0"))
            .await
            .unwrap();
        cache
            .add_for_handle(old, 1, Bytes::from_static(b"old-1"))
            .await
            .unwrap();

        let sender_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let receiver_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver_socket.local_addr().unwrap();
        let first_chunk_sent = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        let service = {
            let cache = Arc::clone(&cache);
            let sender_socket = Arc::clone(&sender_socket);
            let hook = ReplicaServiceHook {
                first_chunk_sent: Arc::clone(&first_chunk_sent),
                resume: Arc::clone(&resume),
            };
            tokio::spawn(async move {
                let mut sender = FecSender::new(0, DEFAULT_SYMBOL_SIZE);
                let config =
                    CacheMeshConfig::new("source", "uk", sender_socket.local_addr().unwrap());
                serve_replica_request(
                    &sender_socket,
                    &cache,
                    &mut sender,
                    &config,
                    receiver_addr,
                    receiver_addr,
                    801,
                    0,
                    0,
                    Some(&hook),
                )
                .await
            })
        };

        first_chunk_sent.wait().await;
        let current = cache.resolve_or_create_stream(802).await;
        cache
            .add_for_handle(current, 0, Bytes::from_static(b"new-0"))
            .await
            .unwrap();
        cache
            .add_for_handle(current, 1, Bytes::from_static(b"new-1"))
            .await
            .unwrap();
        resume.wait().await;

        assert_eq!(service.await.unwrap().unwrap(), 1);
        let mut datagram = vec![0_u8; 65_536];
        let (len, peer) = timeout(
            Duration::from_millis(100),
            receiver_socket.recv_from(&mut datagram),
        )
        .await
        .unwrap()
        .unwrap();
        let mut receiver = FecReceiver::new(Arc::new(CacheMeshFecCounters::default()));
        let frame = receiver.push(peer, &datagram[..len]).unwrap().unwrap();
        assert_eq!(
            MeshFrame::decode(frame).unwrap(),
            MeshFrame::chunk("source", 801, 0, Bytes::from_static(b"old-0"))
        );
        assert!(timeout(
            Duration::from_millis(20),
            receiver_socket.recv_from(&mut datagram)
        )
        .await
        .is_err());
    }

    #[test]
    fn fec_receiver_rejects_declared_transfer_before_allocating_decoder_state() {
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(1);
        let datagrams = encoder.encode_object(&[7_u8; 128]).unwrap();
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::with_limits(2, 2, 64, Arc::clone(&stats));
        let peer: SocketAddr = "127.0.0.1:9920".parse().unwrap();

        assert!(matches!(
            receiver.push(peer, &datagrams[0]),
            Err(DatagramFecError::PayloadTooLargeForBlock {
                actual: 128,
                max: 64
            })
        ));
        assert!(receiver.decoders.is_empty());
        assert!(receiver.observations.is_empty());
        assert_eq!(stats.snapshot().rx_oversized_objects, 1);
    }

    #[test]
    fn fec_decode_error_expires_partial_decoder_state() {
        let mut first_encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(0)
            .with_initial_block_id(7);
        let mut conflicting_encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(0)
            .with_initial_block_id(7);
        let first = first_encoder.encode_object(&[1_u8; 32]).unwrap();
        let conflicting = conflicting_encoder.encode_object(&[2_u8; 40]).unwrap();
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::with_limits(1, 2, 1024, Arc::clone(&stats));
        let peer: SocketAddr = "127.0.0.1:9921".parse().unwrap();

        assert!(receiver.push(peer, &first[0]).unwrap().is_none());
        assert_eq!(receiver.observations.len(), 1);
        assert!(matches!(
            receiver.push(peer, &conflicting[0]),
            Err(DatagramFecError::InconsistentBlockGeometry { block_id: 7 })
        ));
        assert!(receiver.observations.is_empty());
        assert_eq!(receiver.decoders[&peer].in_flight_block_count(), 0);
        assert_eq!(stats.snapshot().rx_inflight_objects, 0);
    }

    #[test]
    fn fec_receiver_bounds_peers_and_incomplete_objects() {
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::with_limits(2, 3, 1024, Arc::clone(&stats));
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(0);

        for block in 0..10_u16 {
            let payload = [block as u8; 32];
            let datagrams = encoder.encode_object(&payload).unwrap();
            let peer: SocketAddr = format!("127.0.0.1:{}", 10_000 + block).parse().unwrap();
            assert!(receiver.push(peer, &datagrams[0]).unwrap().is_none());
        }

        assert!(receiver.decoders.len() <= 2);
        assert!(receiver.observations.len() <= 3);
        assert!(
            receiver
                .decoders
                .values()
                .map(PeerDecoders::in_flight_block_count)
                .sum::<usize>()
                <= 3
        );
        assert!(stats.snapshot().rx_evicted_objects > 0);
    }

    /// A peer numbers its parts, replica replies, requests and announcements
    /// on counters of their own. A part that completes while a reply of the
    /// same peer is arriving does not drop what the reply has received.
    #[test]
    fn a_completed_part_keeps_a_reply_of_the_same_peer_that_is_arriving() {
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::new(Arc::clone(&stats));
        let peer: SocketAddr = "127.0.0.1:9930".parse().unwrap();
        let mut replies = DatagramFecEncoder::new()
            .with_symbol_size(100)
            .with_repair_symbols(1)
            .with_initial_block_id(REPLY_BLOCK_ID_BASE);
        let mut parts = DatagramFecEncoder::new()
            .with_symbol_size(100)
            .with_repair_symbols(0)
            .with_initial_block_id(CHUNK_BLOCK_ID_BASE);
        let mut requests = DatagramFecEncoder::new()
            .with_symbol_size(100)
            .with_repair_symbols(0)
            .with_initial_block_id(REQUEST_BLOCK_ID_BASE);
        let reply_payload = (0..1_000).map(|i| i as u8).collect::<Vec<_>>();
        let reply = replies.encode_object(&reply_payload).unwrap();
        let request = requests.encode_object(&[3_u8; 300]).unwrap();
        // 10 source symbols and 1 repair: the reply needs 10 of its 11.
        assert_eq!(reply.len(), 11);
        assert_eq!(request.len(), 3);

        assert!(receiver.push(peer, &request[0]).unwrap().is_none());
        for datagram in &reply[..5] {
            assert!(receiver.push(peer, datagram).unwrap().is_none());
        }
        for part in 0..3_u8 {
            let datagrams = parts.encode_object(&[part; 50]).unwrap();
            assert_eq!(
                receiver.push(peer, &datagrams[0]).unwrap().as_deref(),
                Some(&[part; 50][..])
            );
        }
        assert_eq!(receiver.decoders[&peer].in_flight_block_count(), 2);
        let mut decoded = None;
        for datagram in &reply[5..] {
            if let Some(frame) = receiver.push(peer, datagram).unwrap() {
                decoded = Some(frame);
            }
        }
        assert_eq!(decoded.as_deref(), Some(&reply_payload[..]));
        assert_eq!(
            receiver.push(peer, &request[1]).unwrap(),
            None,
            "the request still has one symbol to come"
        );
        assert_eq!(
            receiver.push(peer, &request[2]).unwrap().as_deref(),
            Some(&[3_u8; 300][..])
        );
        assert_eq!(receiver.decoders[&peer].in_flight_block_count(), 0);
    }

    /// Each peer of one control send takes its own block id, and the next
    /// send starts beyond all of them: a peer that comes first in the next
    /// send is not given a block id that it has already decoded, which it
    /// would drop as a duplicate.
    #[tokio::test]
    async fn control_sends_to_several_peers_never_reuse_a_block_id() {
        let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let first_addr = first.local_addr().unwrap();
        let second_addr = second.local_addr().unwrap();
        let cache = Arc::new(ChunkCache::new(demand_options()));
        let edge = CacheMesh::new(
            cache,
            CacheMeshConfig::new("edge", "uk", loopback_addr())
                .with_peers([first_addr, second_addr])
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_secs(60)),
        )
        .start()
        .await
        .unwrap();

        // The order of the peers changes between sends, as the parents of
        // two playlists do.
        edge.send_control(&[first_addr, second_addr], b"one")
            .await
            .unwrap();
        edge.send_control(&[second_addr, first_addr], b"two")
            .await
            .unwrap();
        edge.send_control(&[second_addr], b"three").await.unwrap();

        async fn request_blocks(socket: &UdpSocket, blocks: usize) -> Vec<u32> {
            let mut ids = Vec::new();
            let mut buf = vec![0_u8; 65_536];
            while ids.len() < blocks {
                let (len, _) = timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
                    .await
                    .expect("a control block did not arrive")
                    .unwrap();
                let header = DatagramFecHeader::decode(&buf[..len]).unwrap();
                if block_id_lane(header.block_id) == 1 && !ids.contains(&header.block_id) {
                    ids.push(header.block_id);
                }
            }
            ids
        }
        let to_first = request_blocks(&first, 2).await;
        let to_second = request_blocks(&second, 3).await;
        let mut all = to_first
            .iter()
            .chain(&to_second)
            .copied()
            .collect::<Vec<_>>();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), 5, "first {to_first:?}, second {to_second:?}");
        edge.shutdown();
    }

    /// The receiver prunes on every datagram, so the expiry queue holds no
    /// more than the window.
    #[test]
    fn fec_receiver_expiry_queue_stays_within_the_datagram_window() {
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::with_limits(1, 1, 1024, stats);
        let peer: SocketAddr = "127.0.0.1:9930".parse().unwrap();
        let key = (peer, 1);

        for observed in 1..=FEC_OBSERVATION_DATAGRAM_WINDOW * 3 {
            receiver.observed_datagrams = observed;
            receiver.observations.insert(
                key,
                FecBlockObservation {
                    source_symbols: 1,
                    received_source_symbols: HashSet::new(),
                    last_seen_datagram: observed,
                    first_seen_us: 0,
                    fragments: 1,
                },
            );
            receiver.observation_expiries.push_back((observed, key));
            receiver.prune_observations();
        }

        assert!(
            receiver.observation_expiries.len() <= FEC_OBSERVATION_DATAGRAM_WINDOW as usize + 1
        );
        assert_eq!(receiver.observations.len(), 1);
    }

    /// A decoded block that misses a source symbol is counted as loss once
    /// the late source has had its time, on the next datagram of any block:
    /// the expiry queue keeps the grace of the low-latency receiver.
    #[test]
    fn a_missing_source_counts_as_loss_on_the_first_datagram_after_its_grace() {
        let symbol_id = |datagram: &Vec<u8>| {
            u32::from_be_bytes([
                0,
                datagram[HEADER_LEN + 1],
                datagram[HEADER_LEN + 2],
                datagram[HEADER_LEN + 3],
            ])
        };
        let peer: SocketAddr = "127.0.0.1:9931".parse().unwrap();
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(2);
        let repaired = encoder
            .encode_object(b"mesh-frame-with-several-symbols")
            .unwrap();
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::new(Arc::clone(&stats));
        for datagram in repaired.iter().filter(|datagram| symbol_id(datagram) != 1) {
            if receiver.push(peer, datagram).unwrap().is_some() {
                break;
            }
        }
        assert_eq!(receiver.recoveries.len(), 1);
        // Within the grace another block's datagram leaves it pending.
        let next = encoder.encode_object(b"tiny").unwrap();
        receiver.push(peer, &next[0]).unwrap();
        assert_eq!(receiver.recoveries.len(), 1);
        assert_eq!(stats.snapshot().rx_presumed_lost_source_datagrams, 0);

        std::thread::sleep(LATE_SOURCE_GRACE + Duration::from_millis(30));
        let next = encoder.encode_object(b"tiny").unwrap();
        receiver.push(peer, &next[0]).unwrap();
        assert!(receiver.recoveries.is_empty());
        assert!(receiver.recovery_expiries.is_empty());
        assert_eq!(stats.snapshot().rx_presumed_lost_source_datagrams, 1);
        assert_eq!(receiver.loss[&peer].lost_symbols, 1);
    }

    /// An incomplete block that is dropped to make room never decoded: its
    /// missing source symbols count as loss on the path.
    #[test]
    fn an_evicted_incomplete_block_counts_as_loss() {
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut receiver = FecReceiver::with_limits(4, 1, 1024, Arc::clone(&stats));
        let peer: SocketAddr = "127.0.0.1:9932".parse().unwrap();
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(8)
            .with_repair_symbols(0);
        let first = encoder.encode_object(&[1_u8; 32]).unwrap();
        let second = encoder.encode_object(&[2_u8; 32]).unwrap();
        assert!(receiver.push(peer, &first[0]).unwrap().is_none());
        assert!(receiver.push(peer, &second[0]).unwrap().is_none());
        assert_eq!(stats.snapshot().rx_evicted_objects, 1);
        let window = &receiver.loss[&peer];
        assert_eq!(window.lost_symbols, 3);
        assert!(window.expected_packets >= 4);
    }

    #[test]
    fn bounded_replica_queue_drops_work_when_full() {
        let (sender, mut receiver) = mpsc::channel(1);
        let depth = AtomicU64::new(0);
        let max_depth = AtomicU64::new(0);
        let drops = AtomicU64::new(0);

        assert!(try_enqueue(&sender, &depth, &max_depth, &drops, 1_u8));
        assert!(!try_enqueue(&sender, &depth, &max_depth, &drops, 2_u8));
        assert_eq!(depth.load(Ordering::Relaxed), 1);
        assert_eq!(max_depth.load(Ordering::Relaxed), 1);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.try_recv(), Ok(1));
        decrement_depth(&depth);
        assert_eq!(depth.load(Ordering::Relaxed), 0);
        decrement_depth(&depth);
        assert_eq!(depth.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn mesh_drops_unauthorized_datagrams_before_fec_decode() {
        let bind_addr = loopback_addr();
        let cache = Arc::new(ChunkCache::new(Options::default()));
        let seen: Arc<StdMutex<usize>> = Arc::default();
        struct Seen(Arc<StdMutex<usize>>);
        impl CacheMeshReceiveObserver for Seen {
            fn datagram(&self, _: SocketAddr, _: &[u8]) {
                *lock(&self.0) += 1;
            }
            fn decoded(&self, _: SocketAddr, _: &[u8], _: &[u8]) {}
        }
        let mesh = CacheMesh::new(
            cache,
            CacheMeshConfig::new("closed", "uk", bind_addr)
                .with_receive_observer(Arc::new(Seen(Arc::clone(&seen)))),
        )
        .start()
        .await
        .unwrap();
        let attacker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut sender = FecSender::new(0, DEFAULT_SYMBOL_SIZE);
        sender
            .send(&attacker, mesh.local_addr(), b"unauthorized")
            .await
            .unwrap();

        timeout(Duration::from_secs(1), async {
            loop {
                if mesh.fec_stats().rx_unauthorized_datagrams > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let stats = mesh.fec_stats();
        assert_eq!(stats.rx_wire_datagrams, 0);
        assert_eq!(stats.rx_inflight_objects, 0);
        // A cut-through observer never sees an unauthorized datagram.
        assert_eq!(*lock(&seen), 0);
        mesh.shutdown();
    }

    #[tokio::test]
    async fn rejected_runtime_peer_does_not_expand_the_allowlist() {
        let bind_addr = loopback_addr();
        let first_peer = loopback_addr();
        let rejected_peer = loopback_addr();
        let mesh = CacheMesh::new(
            Arc::new(ChunkCache::new(Options::default())),
            CacheMeshConfig::new("bounded", "uk", bind_addr).with_max_peers(1),
        )
        .start()
        .await
        .unwrap();

        assert!(mesh.add_peer(first_peer).await);
        assert!(!mesh.add_peer(rejected_peer).await);
        assert!(mesh.shared.is_allowed(&first_peer));
        assert!(!mesh.shared.is_allowed(&rejected_peer));
        assert_eq!(mesh.peers().await, vec![first_peer]);
        assert_eq!(mesh.fec_stats().peer_limit_drops, 1);

        mesh.shutdown();
    }

    /// A hello can only bring authorized peers, and no more than the table
    /// holds.
    #[tokio::test]
    async fn a_hello_admits_only_authorized_peers_within_the_limit() {
        let bind_addr = loopback_addr();
        let seed = loopback_addr();
        let [authorized, second, unknown] = [loopback_addr(), loopback_addr(), loopback_addr()];
        let mesh = CacheMesh::new(
            Arc::new(ChunkCache::new(Options::default())),
            CacheMeshConfig::new("node", "uk", bind_addr)
                .with_peer(seed)
                .with_allowed_peers([authorized, second])
                .with_max_peers(2),
        )
        .start()
        .await
        .unwrap();
        let socket = UdpSocket::bind(seed).await.unwrap();
        let hello = MeshFrame::hello_with_peers("seed", "uk", vec![unknown, authorized, second])
            .encode()
            .unwrap();
        FecSender::new(0, DEFAULT_SYMBOL_SIZE)
            .send(&socket, bind_addr, &hello)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            while mesh.fec_stats().peer_limit_drops == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the hello was not handled");
        let mut peers = mesh.peers().await;
        peers.sort_unstable();
        let mut expected = vec![seed, authorized];
        expected.sort_unstable();
        assert_eq!(peers, expected);
        assert!(!mesh.shared.is_allowed(&unknown));
        mesh.shutdown();
    }

    /// The frame limit follows the cache, so a part that fits a slot is never
    /// refused by the mesh; a fixed 1 MiB limit refused 4K parts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_frame_limit_follows_the_cache() {
        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            buffer_size_kb: 2_048,
            ..Options::default()
        };
        let cache = ChunkCache::new(options);
        assert_eq!(frame_bytes_for(&cache), 2_048 * 1024 + FRAME_OVERHEAD_BYTES);
        assert_eq!(
            remote_slots_for(&cache),
            4 + REMOTE_SLOT_PRUNE_INTERVAL.as_millis() as usize * 4
        );

        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(options));
        let edge_cache = Arc::new(ChunkCache::new(options));
        // Large symbols, as for 4K video: the part is about 100 of them, which
        // an unoptimised build encodes in good time.
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            CacheMeshConfig::new("distributor", "uk", distributor_addr)
                .with_peer(edge_addr)
                .with_role(CacheMeshRole::Distributor)
                .with_max_symbol_size(16_000)
                .with_announce_interval(Duration::from_millis(50))
                .without_gso(),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_role(CacheMeshRole::Edge)
                .with_socket_buffer_bytes(8 << 20)
                .with_announce_interval(Duration::from_millis(50)),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        let part = Bytes::from(
            (0..1_536 * 1024)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>(),
        );
        distributor_cache
            .add_for_stream_id(7, 0, part.clone())
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 0).await;
        assert_eq!(edge_cache.get_for_stream_id(7, 0).await.unwrap().0, part);
        assert_eq!(edge.fec_stats().rx_oversized_objects, 0);
        distributor.shutdown();
        edge.shutdown();
    }

    #[tokio::test]
    async fn a_frame_beyond_the_limit_is_not_sent() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stats = Arc::new(CacheMeshFecCounters::default());
        let mut sender = FecSender::with_policy_and_initial_block_id(
            1,
            0.0,
            1,
            DEFAULT_SYMBOL_SIZE,
            0,
            Arc::clone(&stats),
        )
        .with_max_frame_bytes(100);
        let peer = socket.local_addr().unwrap();
        assert!(matches!(
            sender.send(&socket, peer, &[0_u8; 101]).await,
            Err(MeshError::InvalidFrame(_))
        ));
        assert!(matches!(
            sender.prepare_object(peer, (7, 0), 1, &[&[0_u8; 60], &[0_u8; 41]]),
            Err(MeshError::InvalidFrame(_))
        ));
        let stats = stats.snapshot();
        assert_eq!(stats.tx_errors, 2);
        assert_eq!(stats.tx_objects, 0);
        sender.send(&socket, peer, &[0_u8; 100]).await.unwrap();
    }

    /// A full set of remote slots is pruned to what the cache retains; it
    /// never drops a slot that the cache holds, which would be sent back.
    #[tokio::test]
    async fn a_full_remote_slot_set_is_pruned_and_keeps_retained_slots() {
        let cache = ChunkCache::new(demand_options());
        let counters = CacheMeshFecCounters::default();
        let stream = cache.resolve_or_create_stream(7).await;
        let mut remote_slots = HashSet::new();
        for slot_id in 0..100_usize {
            cache
                .add_for_handle(stream, slot_id, Bytes::from_static(b"part"))
                .await
                .unwrap();
            assert!(mark_remote_slot(
                &cache,
                &mut remote_slots,
                (stream, slot_id),
                80,
                &counters,
            ));
        }
        // The set reached 80 and was pruned to the 64 retained slots.
        assert!(remote_slots.len() <= 80);
        assert!((36..100).all(|slot_id| remote_slots.contains(&(stream, slot_id))));
        assert_eq!(counters.snapshot().remote_slot_evictions, 0);
        // A limit below what the cache retains cannot be kept without
        // dropping a live slot: the part is left unmarked and counted.
        let mut small = HashSet::new();
        for slot_id in 36..100_usize {
            mark_remote_slot(&cache, &mut small, (stream, slot_id), 8, &counters);
        }
        assert_eq!(small.len(), 8);
        assert_eq!(counters.snapshot().remote_slot_evictions, 56);
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

    /// Wait until the child has learned the role and region of its parents,
    /// and its own first hello has gone out. A parent that keeps to its
    /// region refuses a child's subscription until it has the child's hello;
    /// sent after it on the same socket, the subscription arrives after it.
    async fn wait_for_hello(handle: &CacheMeshHandle, parents: usize) {
        timeout(Duration::from_secs(3), async {
            loop {
                if handle.shared.peer_roles.read().await.len() >= parents
                    && handle.fec_stats().tx_source_datagrams > 0
                {
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
            rank: None,
            boot_id: None,
        };
        let short = frame.encode().unwrap();
        assert_eq!(MeshFrame::decode(short.clone()).unwrap(), frame);
        // The rank is one byte after the lease, and the boot id eight bytes
        // after the rank; a frame that ends before either decodes without it.
        let ranked = MeshFrame::Subscribe {
            node_id: "edge-1".into(),
            stream_id: 2001,
            from_slot: 77,
            backfill_parts: 32,
            lease_ms: 5_000,
            rank: Some(1),
            boot_id: None,
        };
        let encoded = ranked.encode().unwrap();
        assert_eq!(encoded.len(), short.len() + 1);
        assert_eq!(MeshFrame::decode(encoded).unwrap(), ranked);
        let booted = MeshFrame::Subscribe {
            node_id: "edge-1".into(),
            stream_id: 2001,
            from_slot: 77,
            backfill_parts: 32,
            lease_ms: 5_000,
            rank: Some(1),
            boot_id: Some(1_763_924_000_123_456),
        };
        let encoded = booted.encode().unwrap();
        assert_eq!(encoded.len(), short.len() + 9);
        assert_eq!(MeshFrame::decode(encoded).unwrap(), booted);
        // A child that gives no rank still gives its boot id: the rank byte
        // is 255.
        let unranked = MeshFrame::Subscribe {
            node_id: "edge-1".into(),
            stream_id: 2001,
            from_slot: 77,
            backfill_parts: 32,
            lease_ms: 5_000,
            rank: None,
            boot_id: Some(9),
        };
        let encoded = unranked.encode().unwrap();
        assert_eq!(encoded.len(), short.len() + 9);
        assert_eq!(encoded[short.len()], SUBSCRIBE_RANK_NONE);
        assert_eq!(MeshFrame::decode(encoded).unwrap(), unranked);

        let frame = MeshFrame::Unsubscribe {
            node_id: "edge-1".into(),
            stream_id: 2001,
        };
        assert_eq!(MeshFrame::decode(frame.encode().unwrap()).unwrap(), frame);
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
            edge_cache.resolve_stream(7).is_none(),
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
        assert!(edge_cache.resolve_stream(8).is_none());
        assert_eq!(edge.subscribed_streams(), vec![7]);
        let served = parent.served_subscriptions();
        assert_eq!(served.len(), 1);
        assert_eq!((served[0].child, served[0].stream_id), (edge_addr, 7));

        parent.shutdown();
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_served_leases_watch_changes_when_a_lease_comes_or_goes() {
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
        let mut changed = parent.served_changed();
        changed.mark_unchanged();
        let wait_change = |changed: &mut watch::Receiver<u64>| {
            let mut changed = changed.clone();
            async move {
                timeout(Duration::from_secs(2), changed.changed())
                    .await
                    .expect("the served leases watch changes")
                    .unwrap();
            }
        };

        // A new lease is a change.
        edge.subscribe(7, 0, 0).await.unwrap();
        wait_change(&mut changed).await;
        changed.mark_unchanged();
        assert_eq!(
            parent
                .served_subscriptions()
                .iter()
                .map(|served| (served.child, served.stream_id))
                .collect::<Vec<_>>(),
            vec![(edge_addr, 7)]
        );

        // Renewing it is not.
        edge.subscribe(7, 0, 0).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!changed.has_changed().unwrap(), "a renewal is no change");

        // A second playlist is, and so is the release of the first.
        edge.subscribe(8, 0, 0).await.unwrap();
        wait_change(&mut changed).await;
        changed.mark_unchanged();
        assert_eq!(parent.served_subscriptions().len(), 2);
        edge.unsubscribe(7).await.unwrap();
        wait_change(&mut changed).await;
        changed.mark_unchanged();
        assert_eq!(
            parent
                .served_subscriptions()
                .iter()
                .map(|served| served.stream_id)
                .collect::<Vec<_>>(),
            vec![8]
        );

        // A lease that lapses is a change too.
        wait_change(&mut changed).await;
        assert!(parent.served_subscriptions().is_empty());

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

    /// A distributor's live push filter (wave 7 item 4): a pair it names
    /// keeps its lease and its hole repair but gets no live part; when the
    /// filter lets it go again the push starts at the next part.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_filtered_pair_keeps_its_lease_and_repair_but_gets_no_live_push() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        use std::sync::atomic::{AtomicBool, Ordering};
        let filtered = Arc::new(AtomicBool::new(true));
        let filter_flag = Arc::clone(&filtered);
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr)
                .with_peer(edge_addr)
                .with_live_push_filter(Arc::new(move |peer, stream_id| {
                    peer == edge_addr && stream_id == 7 && filter_flag.load(Ordering::Relaxed)
                })),
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
        // The warm-up window still comes.
        edge.subscribe(7, 0, 16).await.unwrap();
        wait_for_slot(&edge_cache, 7, 0).await;
        for slot in 1..=3 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(format!("part-{slot}")))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(edge_cache.get_for_stream_id(7, 1).await.is_none());
        assert!(edge_cache.get_for_stream_id(7, 3).await.is_none());
        // The lease is served all along.
        assert_eq!(parent.served_subscriptions().len(), 1);
        // A hole is repaired on request.
        edge.request_parts(7, 2, 1, 1).await.unwrap();
        wait_for_slot(&edge_cache, 7, 2).await;

        // Pushed again from the next part on, not the parts it skipped.
        filtered.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(50)).await;
        parent_cache
            .add_for_stream_id(7, 4, Bytes::from_static(b"part-4"))
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 4).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(edge_cache.get_for_stream_id(7, 1).await.is_none());
        assert!(edge_cache.get_for_stream_id(7, 3).await.is_none());

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

    #[test]
    fn a_hello_says_when_its_sender_drains() {
        let mut hello =
            MeshFrame::hello_with_role("dist-c", "uk", CacheMeshRole::Distributor, Vec::new());
        let plain = hello.encode().unwrap();
        assert_eq!(MeshFrame::decode(plain.clone()).unwrap(), hello);
        if let MeshFrame::Hello { draining, .. } = &mut hello {
            *draining = true;
        }
        let encoded = hello.encode().unwrap();
        // The same length: the top bit of the role byte.
        assert_eq!(encoded.len(), plain.len());
        assert_eq!(MeshFrame::decode(encoded).unwrap(), hello);
    }

    /// Wave 8 stage 5 (AM 12): a parent knows its place in each child's
    /// ranking of a playlist's parents (the fast plane feeds the first
    /// parent's children video in full), a draining parent is ranked after
    /// the live ones, and a parent that leaves the ranked set is released
    /// rather than left until its lease lapses: not by the renewal that
    /// ranks it out, which keeps it with a rank after the ranked parents,
    /// but when its replacement is confirmed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parents_learn_their_rank_and_a_draining_parent_is_left() {
        let addrs = [loopback_addr(), loopback_addr(), loopback_addr()];
        let edge_addr = loopback_addr();
        let mut parents = Vec::new();
        for (index, addr) in addrs.iter().enumerate() {
            let cache = Arc::new(ChunkCache::new(demand_options()));
            let parent = CacheMesh::new(
                cache,
                demand_distributor(&format!("dist-{index}"), *addr)
                    .with_peer(edge_addr)
                    .with_subscription_lease(Duration::from_secs(5)),
            )
            .start()
            .await
            .unwrap();
            parents.push(parent);
        }
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge = CacheMesh::new(
            edge_cache,
            demand_edge("edge", edge_addr)
                .with_peers(addrs)
                .with_max_parents(2)
                .with_subscription_lease(Duration::from_secs(5)),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 3).await;
        let served_rank = |index: usize, stream_id: u64| {
            parents[index]
                .served_subscriptions()
                .iter()
                .find(|served| served.stream_id == stream_id)
                .map(|served| served.rank)
        };
        let wait_ranks = |stream_id: u64, want: Vec<Option<Option<u8>>>| {
            let parents = &parents;
            async move {
                timeout(Duration::from_secs(3), async {
                    loop {
                        let ranks = (0..parents.len())
                            .map(|index| {
                                parents[index]
                                    .served_subscriptions()
                                    .iter()
                                    .find(|served| served.stream_id == stream_id)
                                    .map(|served| served.rank)
                            })
                            .collect::<Vec<_>>();
                        if ranks == want {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("ranks of stream {stream_id} never became {want:?}"));
            }
        };
        let index_of = |peer: SocketAddr| addrs.iter().position(|addr| *addr == peer).unwrap();
        let ranked = edge.subscribe(7, 0, 0).await.unwrap();
        assert_eq!(ranked.len(), 2);
        let first = index_of(ranked[0]);
        let second = index_of(ranked[1]);
        let third = 3 - first - second;
        let mut want = vec![None; 3];
        want[first] = Some(Some(0));
        want[second] = Some(Some(1));
        wait_ranks(7, want).await;
        assert_eq!(served_rank(third, 7), None);

        // The first parent drains: once its hello says so, the renewal ranks
        // it out of the set (three parents, two wanted). It is kept, after
        // the two ranked parents, until they are confirmed.
        parents[first].set_draining(true);
        timeout(Duration::from_secs(3), async {
            while !edge.draining_peers().contains(&addrs[first]) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the draining hello did not arrive");
        let ranked = edge.subscribe(7, 0, 0).await.unwrap();
        assert!(!ranked.contains(&addrs[first]), "{ranked:?}");
        assert_eq!(index_of(ranked[0]), second, "the second parent moves up");
        let mut want = vec![None; 3];
        want[first] = Some(Some(2));
        want[second] = Some(Some(0));
        want[third] = Some(Some(1));
        wait_ranks(7, want.clone()).await;
        assert_eq!(edge.pending_parents(), vec![(7, addrs[first])]);
        assert_eq!(edge.fec_stats().parents_released, 0);
        // Its replacement confirmed, it is told to stop.
        assert_eq!(
            edge.release_pending(&|_, _| true).await,
            vec![(7, addrs[first], true)]
        );
        want[first] = None;
        wait_ranks(7, want).await;
        assert_eq!(edge.pending_parents(), Vec::new());
        assert_eq!(edge.fec_stats().parents_released, 1);
        assert_eq!(edge.fec_stats().parents_released_unconfirmed, 0);

        for parent in &parents {
            parent.shutdown();
        }
        edge.shutdown();
    }

    /// Three distributors that replicate on demand, and one edge that asks
    /// two of them for a playlist.
    struct ThreeParents {
        addrs: [SocketAddr; 3],
        caches: Vec<Arc<ChunkCache>>,
        parents: Vec<CacheMeshHandle>,
        edge: CacheMeshHandle,
        edge_cache: Arc<ChunkCache>,
    }

    impl ThreeParents {
        /// Start them, every node with a subscription lease of `lease`.
        /// Every distributor holds part 0 of playlist 7, so that a lease of
        /// it starts live at part 1. (A lease opened on a playlist that a
        /// parent does not hold yet starts after the first part that it
        /// stores.)
        async fn start(lease: Duration) -> Self {
            let addrs = [loopback_addr(), loopback_addr(), loopback_addr()];
            let edge_addr = loopback_addr();
            let mut caches = Vec::new();
            let mut parents = Vec::new();
            for (index, addr) in addrs.iter().enumerate() {
                let cache = Arc::new(ChunkCache::new(demand_options()));
                cache
                    .add_for_stream_id(7, 0, Bytes::from(vec![0_u8; 64]))
                    .await
                    .unwrap();
                let parent = CacheMesh::new(
                    Arc::clone(&cache),
                    demand_distributor(&format!("dist-{index}"), *addr)
                        .with_peer(edge_addr)
                        .with_subscription_lease(lease),
                )
                .start()
                .await
                .unwrap();
                caches.push(cache);
                parents.push(parent);
            }
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                demand_edge("edge", edge_addr)
                    .with_peers(addrs)
                    .with_max_parents(2)
                    .with_subscription_lease(lease),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 3).await;
            Self {
                addrs,
                caches,
                parents,
                edge,
                edge_cache,
            }
        }

        fn index_of(&self, peer: SocketAddr) -> usize {
            self.addrs.iter().position(|addr| *addr == peer).unwrap()
        }

        /// The rank of the edge's lease of a playlist at each distributor;
        /// `None` at one that serves the edge no lease of it.
        fn ranks(&self, stream_id: u64) -> [Option<Option<u8>>; 3] {
            std::array::from_fn(|index| {
                self.parents[index]
                    .served_subscriptions()
                    .iter()
                    .find(|served| served.stream_id == stream_id)
                    .map(|served| served.rank)
            })
        }

        async fn wait_ranks(&self, stream_id: u64, want: [Option<Option<u8>>; 3]) {
            timeout(Duration::from_secs(3), async {
                while self.ranks(stream_id) != want {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "ranks of stream {stream_id} are {:?}, not {want:?}",
                    self.ranks(stream_id)
                )
            });
        }

        /// Make a distributor drain (or stop draining), and wait until the
        /// edge has its hello.
        async fn set_draining(&self, index: usize, draining: bool) {
            self.parents[index].set_draining(draining);
            timeout(Duration::from_secs(3), async {
                while self.edge.draining_peers().contains(&self.addrs[index]) != draining {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("the hello of the distributor did not arrive");
        }

        /// The edge subscribes to playlist 7; its first parent then drains,
        /// and a second subscribe ranks it out of the set (three live
        /// parents, two asked): it waits to be released. Returns the index
        /// of the parent that left, of the two ranked now in their order,
        /// and a time just before the parent left.
        async fn first_parent_leaves(&self) -> (usize, usize, usize, Instant) {
            let ranked = self.edge.subscribe(7, 0, 0).await.unwrap();
            assert_eq!(ranked.len(), 2);
            let left = self.index_of(ranked[0]);
            let second = self.index_of(ranked[1]);
            let third = 3 - left - second;
            let mut want = [None; 3];
            want[left] = Some(Some(0));
            want[second] = Some(Some(1));
            self.wait_ranks(7, want).await;
            self.set_draining(left, true).await;
            let before = Instant::now();
            let ranked = self.edge.subscribe(7, 0, 0).await.unwrap();
            assert_eq!(ranked, vec![self.addrs[second], self.addrs[third]]);
            want[left] = Some(Some(2));
            want[second] = Some(Some(0));
            want[third] = Some(Some(1));
            self.wait_ranks(7, want).await;
            assert_eq!(self.edge.pending_parents(), vec![(7, self.addrs[left])]);
            (left, second, third, before)
        }

        fn shutdown(&self) {
            for parent in &self.parents {
                parent.shutdown();
            }
            self.edge.shutdown();
        }
    }

    /// A node calls `subscribe` for a playlist from several places, so the
    /// call after the one that ranked a parent out can come a millisecond
    /// later. It releases nothing: the parent that left keeps its lease,
    /// keeps feeding, and waits; ranked again, it waits no longer. Before,
    /// the call that ranked a parent out told it to stop at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_second_subscribe_call_does_not_release_a_pending_parent() {
        let mesh = ThreeParents::start(Duration::from_secs(5)).await;
        let (left, second, third, _) = mesh.first_parent_leaves().await;
        for _ in 0..5 {
            assert_eq!(
                mesh.edge.subscribe(7, 0, 0).await.unwrap(),
                vec![mesh.addrs[second], mesh.addrs[third]]
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(mesh.edge.pending_parents(), vec![(7, mesh.addrs[left])]);
        assert_eq!(mesh.edge.fec_stats().parents_released, 0);
        assert_eq!(mesh.ranks(7)[left], Some(Some(2)));
        // It still feeds the edge: a part that it alone holds arrives.
        mesh.caches[left]
            .add_for_stream_id(7, 1, Bytes::from(vec![1_u8; 64]))
            .await
            .unwrap();
        wait_for_slot(&mesh.edge_cache, 7, 1).await;

        // It drains no longer and is ranked first again: it waits no longer,
        // was never told to stop, and the third parent waits in its place.
        mesh.set_draining(left, false).await;
        assert_eq!(
            mesh.edge.subscribe(7, 0, 0).await.unwrap(),
            vec![mesh.addrs[left], mesh.addrs[second]]
        );
        assert_eq!(mesh.edge.pending_parents(), vec![(7, mesh.addrs[third])]);
        let mut want = [None; 3];
        want[left] = Some(Some(0));
        want[second] = Some(Some(1));
        want[third] = Some(Some(2));
        mesh.wait_ranks(7, want).await;
        assert_eq!(mesh.edge.fec_stats().parents_released, 0);
        mesh.shutdown();
    }

    /// A parent that left a playlist's ranked set is told to stop when
    /// every ranked parent of the playlist is confirmed, and not while one
    /// of them is not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_pending_parent_is_released_when_its_replacement_is_confirmed() {
        let mesh = ThreeParents::start(Duration::from_secs(5)).await;
        let (left, second, third, _) = mesh.first_parent_leaves().await;
        // Nothing is confirmed, and the limit (two thirds of 5 s) is far.
        assert_eq!(mesh.edge.release_pending(&|_, _| false).await, Vec::new());
        // One of the two ranked parents delivers: not yet.
        let one = mesh.addrs[second];
        assert_eq!(
            mesh.edge
                .release_pending(&|stream_id, parent| stream_id == 7 && parent == one)
                .await,
            Vec::new()
        );
        assert_eq!(mesh.ranks(7)[left], Some(Some(2)));
        assert_eq!(mesh.edge.fec_stats().parents_released, 0);
        // Both do. It is asked of the ranked parents of the playlist only.
        let asked = StdMutex::new(Vec::new());
        assert_eq!(
            mesh.edge
                .release_pending(&|stream_id, parent| {
                    lock(&asked).push((stream_id, parent));
                    true
                })
                .await,
            vec![(7, mesh.addrs[left], true)]
        );
        let mut asked = asked.into_inner().unwrap();
        asked.sort_unstable();
        let mut ranked = vec![(7, mesh.addrs[second]), (7, mesh.addrs[third])];
        ranked.sort_unstable();
        assert_eq!(asked, ranked);
        let mut want = [None; 3];
        want[second] = Some(Some(0));
        want[third] = Some(Some(1));
        mesh.wait_ranks(7, want).await;
        assert_eq!(mesh.edge.pending_parents(), Vec::new());
        let stats = mesh.edge.fec_stats();
        assert_eq!(
            (stats.parents_released, stats.parents_released_unconfirmed),
            (1, 0)
        );
        // Nothing waits now.
        assert_eq!(mesh.edge.release_pending(&|_, _| true).await, Vec::new());
        mesh.shutdown();
    }

    /// A ranked parent must be left in the end, whether or not its
    /// replacement delivers: a parent that left the ranked set is kept, its
    /// lease renewed, for two renewal periods (two thirds of the lease) and
    /// then told to stop, and that release is counted apart. A parent that
    /// nothing releases is not renewed beyond that time either: its lease
    /// lapses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unconfirmed_replacement_releases_after_two_renewals_and_is_counted() {
        let lease = Duration::from_millis(600);
        let renewal = lease / 3;
        let mesh = ThreeParents::start(lease).await;
        assert_eq!(mesh.edge.parent_release_after, renewal * 2);
        let (left, second, third, left_at) = mesh.first_parent_leaves().await;
        // The edge renews at a third of its lease and looks every 20 ms for
        // a parent to release; no replacement is ever confirmed.
        let mut next_renewal = left_at + renewal;
        let released = loop {
            assert!(left_at.elapsed() < Duration::from_secs(3), "never released");
            tokio::time::sleep(Duration::from_millis(20)).await;
            if Instant::now() >= next_renewal {
                next_renewal += renewal;
                mesh.edge.subscribe(7, 0, 0).await.unwrap();
            }
            let held = mesh.ranks(7)[left];
            let waited = left_at.elapsed();
            let released = mesh.edge.release_pending(&|_, _| false).await;
            if !released.is_empty() {
                break released;
            }
            // While it waits it holds its lease.
            assert_eq!(held, Some(Some(2)), "after {waited:?}");
        };
        let waited = left_at.elapsed();
        assert_eq!(released, vec![(7, mesh.addrs[left], false)]);
        assert!(waited >= renewal * 2, "released after {waited:?}");
        let mut want = [None; 3];
        want[second] = Some(Some(0));
        want[third] = Some(Some(1));
        mesh.wait_ranks(7, want).await;
        let stats = mesh.edge.fec_stats();
        assert_eq!(
            (stats.parents_released, stats.parents_released_unconfirmed),
            (1, 1)
        );

        // The second parent drains too and leaves the set (the first, which
        // drains as well, ranks above it). Nothing looks for a parent to
        // release: the renewals go on, the lease of the parent that left is
        // renewed for the two periods only, and it lapses.
        mesh.set_draining(second, true).await;
        let left_at = Instant::now();
        assert_eq!(
            mesh.edge.subscribe(7, 0, 0).await.unwrap(),
            vec![mesh.addrs[third], mesh.addrs[left]]
        );
        assert_eq!(mesh.edge.pending_parents(), vec![(7, mesh.addrs[second])]);
        while mesh.ranks(7)[second].is_some() {
            assert!(
                left_at.elapsed() < renewal * 2 + lease + Duration::from_secs(2),
                "the lease of a parent that nothing released never lapsed"
            );
            tokio::time::sleep(renewal / 2).await;
            mesh.edge.subscribe(7, 0, 0).await.unwrap();
        }
        assert!(left_at.elapsed() >= lease, "{:?}", left_at.elapsed());
        assert_eq!(mesh.edge.pending_parents(), vec![(7, mesh.addrs[second])]);
        assert_eq!(mesh.edge.fec_stats().parents_released, 1);
        mesh.shutdown();
    }

    /// A parent that starts or stops draining is ranked anew by its
    /// children at once: the change of the bit in its hello ticks
    /// `parents_changed`. A hello that changes nothing does not, and a
    /// child's hello never does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_draining_bit_change_ticks_parents_changed() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let parent = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            demand_distributor("parent", parent_addr).with_peer(edge_addr),
        )
        .start()
        .await
        .unwrap();
        let parent_changed = parent.parents_changed();
        let edge = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            demand_edge("edge", edge_addr).with_peer(parent_addr),
        )
        .start()
        .await
        .unwrap();
        let mut changed = edge.parents_changed();
        // The parent's first hello: a parent is new.
        timeout(Duration::from_secs(2), changed.changed())
            .await
            .expect("the first hello of a parent did not tick")
            .unwrap();
        // Hellos every 100 ms that say the same do not tick.
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(!changed.has_changed().unwrap());
        for draining in [true, false] {
            parent.set_draining(draining);
            timeout(Duration::from_secs(2), changed.changed())
                .await
                .unwrap_or_else(|_| panic!("draining {draining} did not tick"))
                .unwrap();
            assert_eq!(edge.draining_peers().contains(&parent_addr), draining);
            tokio::time::sleep(Duration::from_millis(250)).await;
            assert!(!changed.has_changed().unwrap(), "draining {draining}");
        }
        // An edge is no parent of a distributor: its hellos, and its
        // draining bit, tick nothing there.
        edge.set_draining(true);
        timeout(Duration::from_secs(2), async {
            while !parent.draining_peers().contains(&edge_addr) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the edge's draining hello did not arrive");
        assert!(!parent_changed.has_changed().unwrap());
        let statuses = parent.peer_statuses().await;
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].draining && !statuses[0].parent);
        parent.shutdown();
        edge.shutdown();
    }

    /// A parent that restarts at the same address holds no lease, and its
    /// children knew of it only at their next renewal. Its hello now gives
    /// its boot id: the first hello of the new process ticks
    /// `parents_changed` at each child, which greets it and subscribes
    /// again, so the parent has its leases back within one hello of its
    /// start. Nothing renews the lease in this test: only the tick does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restarted_parent_is_told_within_one_hello() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let lease = Duration::from_secs(5);
        let parent_config = |boot_id: u64| {
            demand_distributor("parent", parent_addr)
                .with_peer(edge_addr)
                .with_subscription_lease(lease)
                .with_boot_id(boot_id)
                .with_build("3f2a9c1d77e0".into())
                .with_hello_state(Arc::new(AtomicU8::new(1)))
        };
        let parent = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            parent_config(1_000),
        )
        .start()
        .await
        .unwrap();
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr)
                .with_peer(parent_addr)
                .with_subscription_lease(lease),
        )
        .start()
        .await
        .unwrap();
        let mut changed = edge.parents_changed();
        wait_for_hello(&edge, 1).await;
        changed.borrow_and_update();
        let served = |parent: &CacheMeshHandle| {
            parent
                .served_subscriptions()
                .iter()
                .any(|served| (served.child, served.stream_id) == (edge_addr, 7))
        };
        edge.subscribe(7, 0, 4).await.unwrap();
        timeout(Duration::from_secs(2), async {
            while !served(&parent) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the lease was not served");
        // What the edge knows of its parent comes from the hello.
        let status = edge.peer_statuses().await;
        assert_eq!(status.len(), 1);
        assert_eq!(
            (
                status[0].peer,
                status[0].role,
                status[0].boot_id,
                status[0].build.as_deref(),
                status[0].state,
                status[0].draining,
                status[0].parent
            ),
            (
                parent_addr,
                Some(CacheMeshRole::Distributor),
                Some(1_000),
                Some("3f2a9c1d77e0"),
                Some(1),
                false,
                true
            )
        );
        assert!(status[0].heard.is_some_and(|heard| heard < Duration::from_secs(1)));

        // The parent is killed and starts again 150 ms later: far inside
        // the two seconds for which a silent parent counts as alive.
        parent.shutdown();
        drop(parent);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!changed.has_changed().unwrap());
        let started = Instant::now();
        let parent = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            parent_config(2_000),
        )
        .start()
        .await
        .unwrap();
        timeout(Duration::from_secs(2), changed.changed())
            .await
            .expect("the restart of the parent was not noticed")
            .unwrap();
        let noticed = started.elapsed();
        assert_eq!(edge.peer_statuses().await[0].boot_id, Some(2_000));
        // What a node does on the tick.
        assert_eq!(edge.greet_parents().await.unwrap(), 1);
        assert_eq!(edge.subscribe(7, 0, 4).await.unwrap(), vec![parent_addr]);
        timeout(Duration::from_secs(2), async {
            while !served(&parent) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the restarted parent did not get its lease back");
        let back = started.elapsed();
        // The first hello goes out as the mesh starts. One hello interval
        // is 100 ms here; the next renewal of a node is up to 2.2 s away.
        assert!(
            noticed < Duration::from_millis(500) && back < Duration::from_millis(700),
            "noticed after {noticed:?}, lease back after {back:?}"
        );
        // The hellos that follow say the same and tick nothing.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!changed.has_changed().unwrap());
        parent.shutdown();
        edge.shutdown();
    }

    /// A parent that keeps to its region takes a Subscribe only from a
    /// peer whose region it knows, and a parent that has just started
    /// knows no peer: a Subscribe sent on its first hello is dropped
    /// without a word, and the child's next hello may be far. The child
    /// greets first: its hello, then its Subscribe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_subscribe_sent_on_a_parents_first_hello_is_not_lost() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let lease = Duration::from_secs(5);
        let parent_config = || {
            demand_distributor("parent", parent_addr)
                .with_peer(edge_addr)
                .with_subscription_lease(lease)
        };
        assert!(parent_config().same_region_only);
        let parent = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            parent_config(),
        )
        .start()
        .await
        .unwrap();
        // The edge's own hellos are a minute apart: after the one that it
        // sends as it starts, none comes by itself in this test.
        let edge = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            demand_edge("edge", edge_addr)
                .with_peer(parent_addr)
                .with_subscription_lease(lease)
                .with_announce_interval(Duration::from_secs(60)),
        )
        .start()
        .await
        .unwrap();
        let mut changed = edge.parents_changed();
        wait_for_hello(&edge, 1).await;
        changed.borrow_and_update();
        parent.shutdown();
        drop(parent);
        tokio::time::sleep(Duration::from_millis(150)).await;

        let parent = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            parent_config(),
        )
        .start()
        .await
        .unwrap();
        timeout(Duration::from_secs(2), changed.changed())
            .await
            .expect("the restart of the parent was not noticed")
            .unwrap();
        // A Subscribe alone, on the parent's first hello, is lost.
        assert_eq!(edge.subscribe(7, 0, 4).await.unwrap(), vec![parent_addr]);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(parent.served_subscriptions(), Vec::new());
        assert_eq!(parent.peer_statuses().await[0].role, None);
        // The hello first, then the Subscribe.
        assert_eq!(edge.greet_parents().await.unwrap(), 1);
        assert_eq!(edge.subscribe(7, 0, 4).await.unwrap(), vec![parent_addr]);
        timeout(Duration::from_secs(2), async {
            while parent.served_subscriptions().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the Subscribe that followed the hello was lost");
        assert_eq!(
            parent.peer_statuses().await[0].role,
            Some(CacheMeshRole::Edge)
        );
        parent.shutdown();
        edge.shutdown();
    }

    /// A child that is killed and starts again inside its lease holds
    /// nothing of the playlist. Its Subscribe was taken for a renewal: no
    /// warm-up window, and no initialization, which the parent had sent
    /// the process before. The Subscribe gives the boot id of its process:
    /// another one than the lease's opens the lease again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restarted_child_gets_a_new_lease_its_window_and_its_initialization() {
        restarted_child_gets_its_window_and_initialization(false).await;
    }

    /// The same on a mesh split by class, as av-mesh runs it: a video
    /// playlist's lease is taken on the main socket and served by the class
    /// lane, whose sync task must see the new generation and send the
    /// window and the initialization again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restarted_child_on_a_split_mesh_gets_its_video_window_again() {
        restarted_child_gets_its_window_and_initialization(true).await;
    }

    /// Two processes of a child, one after the other at one address, each
    /// subscribe to a playlist inside the first one's lease. With `split`,
    /// the mesh is split by class and the playlist is video.
    async fn restarted_child_gets_its_window_and_initialization(split: bool) {
        let (stream_id, split_by_class): (u64, fn(CacheMeshConfig) -> CacheMeshConfig) = if split {
            (9, |config: CacheMeshConfig| {
                config
                    .with_class_port_offset(TEST_CLASS_OFFSET)
                    .with_stream_classifier(video_streams(&[9]))
            })
        } else {
            (7, |config: CacheMeshConfig| config)
        };
        let (parent_addr, edge_addr) = if split {
            (
                class_split_addr(TEST_CLASS_OFFSET),
                class_split_addr(TEST_CLASS_OFFSET),
            )
        } else {
            (loopback_addr(), loopback_addr())
        };
        let lease = Duration::from_secs(5);
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            split_by_class(
                demand_distributor("parent", parent_addr)
                    .with_peer(edge_addr)
                    .with_subscription_lease(lease),
            ),
        )
        .start()
        .await
        .unwrap();
        parent_cache
            .set_stream_initialization(stream_id, Bytes::from_static(b"ftyp-moov"))
            .await
            .unwrap();
        for slot in 0..8 {
            parent_cache
                .add_for_stream_id(stream_id, slot, Bytes::from(vec![slot as u8; 64]))
                .await
                .unwrap();
        }
        // The warm-up parts that the lane of the playlist sent.
        let warm_parts_sent = |parent: &CacheMeshHandle| {
            if split {
                let class = parent.class_fec_stats().unwrap().warm_parts_sent;
                assert_eq!(parent.fec_stats().warm_parts_sent, class);
                class
            } else {
                parent.fec_stats().warm_parts_sent
            }
        };
        // Two processes of the child, at the same address. The first is
        // killed: it sends no Unsubscribe, and its lease of 5 s is current
        // when the second subscribes.
        for process in 1..=2_u64 {
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                split_by_class(
                    demand_edge("edge", edge_addr)
                        .with_peer(parent_addr)
                        .with_subscription_lease(lease)
                        .with_boot_id(process),
                ),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            edge.subscribe(stream_id, 0, 4).await.unwrap();
            for slot in 4..8 {
                wait_for_slot(&edge_cache, stream_id, slot).await;
            }
            timeout(Duration::from_secs(2), async {
                while edge_cache.stream_initialization(stream_id).is_none() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("process {process} did not get the initialization"));
            assert_eq!(warm_parts_sent(&parent), 4 * process);
            if split {
                // Parts and the initialization came on the class socket.
                let class = edge.class_fec_stats().unwrap();
                assert!(class.rx_decoded_objects >= 5, "{class:?}");
            }
            // A renewal by the same process opens nothing.
            edge.subscribe(stream_id, 0, 4).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(warm_parts_sent(&parent), 4 * process);
            assert!(edge_cache.get_for_stream_id(stream_id, 3).await.is_none());
            assert_eq!(parent.served_subscriptions().len(), 1);
            edge.shutdown();
            drop(edge);
            // The sockets of the stopped child must be free for the next one.
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        parent.shutdown();
    }

    /// The boot id is in the Subscribe, not only in the hello: the
    /// Subscribe of a restarted child that overtakes its first hello opens
    /// a new lease all the same. (The parent still knows the child's region
    /// from the process before.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_subscribe_that_overtakes_the_hello_still_opens_a_new_lease() {
        let parent_addr = loopback_addr();
        let child_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr)
                .with_peer(child_addr)
                .with_subscription_lease(Duration::from_secs(5)),
        )
        .start()
        .await
        .unwrap();
        parent_cache
            .set_stream_initialization(7, Bytes::from_static(b"ftyp-moov"))
            .await
            .unwrap();
        for slot in 0..8 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 64]))
                .await
                .unwrap();
        }
        // The child is a socket: the test sends its frames in the order it
        // wants, and sees what the parent sends it.
        let child = UdpSocket::bind(child_addr).await.unwrap();
        let mut sender = FecSender::new(0, DEFAULT_SYMBOL_SIZE);
        let subscribe_of = |boot_id: u64| {
            MeshFrame::Subscribe {
                node_id: "child".into(),
                stream_id: 7,
                from_slot: 0,
                backfill_parts: 4,
                lease_ms: 5_000,
                rank: Some(0),
                boot_id: Some(boot_id),
            }
            .encode()
            .unwrap()
        };
        // What the parent sent of the playlist: its initialization, and
        // the slots of its parts, sorted.
        let sent_of_the_playlist = |frames: Vec<(u8, Option<u64>)>| {
            let initializations = frames
                .iter()
                .filter(|(kind, _)| *kind == FRAME_INITIALIZATION)
                .count();
            let mut slots = frames
                .iter()
                .filter_map(|(_, slot_id)| *slot_id)
                .collect::<Vec<_>>();
            slots.sort_unstable();
            (initializations, slots)
        };
        let hello = MeshFrame::Hello {
            node_id: "child".into(),
            region: "uk".into(),
            role: WireRole::Known(CacheMeshRole::Edge),
            peers: Vec::new(),
            draining: false,
            boot_id: Some(1),
            build: None,
            state: None,
        }
        .encode()
        .unwrap();
        sender.send(&child, parent_addr, &hello).await.unwrap();
        sender
            .send(&child, parent_addr, &subscribe_of(1))
            .await
            .unwrap();
        assert_eq!(
            sent_of_the_playlist(frames_arriving(&child, Duration::from_millis(300)).await),
            (1, vec![4, 5, 6, 7])
        );
        // A renewal of the same process: nothing.
        sender
            .send(&child, parent_addr, &subscribe_of(1))
            .await
            .unwrap();
        assert_eq!(
            sent_of_the_playlist(frames_arriving(&child, Duration::from_millis(150)).await),
            (0, Vec::new())
        );
        // The process after it subscribes before it has said hello.
        sender
            .send(&child, parent_addr, &subscribe_of(2))
            .await
            .unwrap();
        assert_eq!(
            sent_of_the_playlist(frames_arriving(&child, Duration::from_millis(300)).await),
            (1, vec![4, 5, 6, 7])
        );
        assert_eq!(parent.served_subscriptions().len(), 1);
        assert_eq!(parent.fec_stats().warm_parts_sent, 8);
        parent.shutdown();
    }

    /// A rolling restart of the forwarder tier under load. Three
    /// distributors carry one playlist at 1,000 parts a second, each written
    /// by the source as it comes; four edges each take it from two of them.
    /// Each distributor in turn drains (its edges leave it make before
    /// break), is killed, and starts again at its address with a new boot
    /// id and an empty cache. Every part written during the roll reaches
    /// every edge, and no tick of a distributor sends more warm-up bytes
    /// than its total allows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_distributors_drain_kill_and_start_under_load_lose_no_slot() {
        let (released, unconfirmed) = roll_three_distributors_under_load(2, 32).await;
        assert!(
            released > unconfirmed,
            "{released} parents released, {unconfirmed} of them unconfirmed"
        );
    }

    /// The same roll with one parent an edge: each change moves an edge's
    /// whole feed, and the parent that joins is asked for no warm-up
    /// window, so nothing but the parent that leaves covers the parts
    /// written before the joiner's first one. Every edge moves twice (off
    /// its first parent when it drains, back when it starts again), and
    /// each move releases the parent that left only once the joiner is
    /// seen delivering. (With the parent released in the call that asks
    /// its replacement, as before this build, the edges lose parts here.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_roll_that_moves_an_edge_s_only_parent_loses_no_slot() {
        let (released, unconfirmed) = roll_three_distributors_under_load(1, 0).await;
        assert!(
            released - unconfirmed >= 2 * 4,
            "{released} parents released, {unconfirmed} of them unconfirmed"
        );
    }

    /// The roll of the two tests above, with `max_parents` parents an edge
    /// and a warm-up window of `backfill_parts` for a new lease. Each edge
    /// does what av-mesh does: on a tick of `parents_changed` it greets its
    /// parents and subscribes, again 100 and 300 ms later; it renews every
    /// third of its lease; and every 100 ms it releases the parents that
    /// wait, a ranked parent counting as confirmed while the edge has
    /// applied a part from it within the last 100 ms (the fast plane's
    /// view in av-mesh). The source writes each part to the distributors in
    /// turn, a different one first each part, so that every feeding parent
    /// delivers some first copies. Asserts that no part was lost and the
    /// warm-up cap held; returns the parents released and how many of them
    /// unconfirmed.
    async fn roll_three_distributors_under_load(
        max_parents: usize,
        backfill_parts: usize,
    ) -> (u64, u64) {
        const EDGES: usize = 4;
        const PART: usize = 64;
        const WARM_TOTAL: u64 = 64_000;
        const PARTS: usize = 3_000;
        type Distributor = (Arc<ChunkCache>, Arc<CacheMeshHandle>);
        let lease = Duration::from_millis(600);
        let renewal = lease / 3;
        let options = Options {
            num_playlists: 2,
            max_segments: 1,
            max_parts_per_segment: 1_024,
            buffer_size_kb: 1,
            ..Options::default()
        };
        let dist_addrs = [loopback_addr(), loopback_addr(), loopback_addr()];
        let edge_addrs = (0..EDGES).map(|_| loopback_addr()).collect::<Vec<_>>();
        let start_distributor = |index: usize, boot_id: u64| {
            let edge_addrs = edge_addrs.clone();
            async move {
                let cache = Arc::new(ChunkCache::new(options));
                let handle = CacheMesh::new(
                    Arc::clone(&cache),
                    demand_distributor(&format!("dist-{index}"), dist_addrs[index])
                        .with_peers(edge_addrs)
                        .with_max_children(EDGES)
                        .with_subscription_lease(lease)
                        .with_backfill_bytes_per_sec(100_000)
                        .with_backfill_total_bytes_per_sec(WARM_TOTAL)
                        .with_boot_id(boot_id),
                )
                .start()
                .await
                .unwrap();
                (cache, Arc::new(handle))
            }
        };
        let distributors: Arc<StdMutex<Vec<Option<Distributor>>>> = Arc::default();
        for index in 0..3 {
            let started = start_distributor(index, 1 + index as u64).await;
            lock(&distributors).push(Some(started));
        }
        let stop = Arc::new(AtomicBool::new(false));

        // The source: part after part, to every distributor that is up.
        let next_slot = Arc::new(AtomicU64::new(0));
        let source = {
            let (distributors, next_slot, stop) =
                (Arc::clone(&distributors), Arc::clone(&next_slot), Arc::clone(&stop));
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(1));
                while !stop.load(Ordering::Relaxed) {
                    tick.tick().await;
                    let slot = next_slot.load(Ordering::Relaxed);
                    let mut caches = lock(&distributors)
                        .iter()
                        .flatten()
                        .map(|(cache, _)| Arc::clone(cache))
                        .collect::<Vec<_>>();
                    if !caches.is_empty() {
                        let first = slot as usize % caches.len();
                        caches.rotate_left(first);
                    }
                    for cache in caches {
                        cache
                            .add_for_stream_id(7, slot as usize, Bytes::from(vec![slot as u8; PART]))
                            .await
                            .unwrap();
                    }
                    next_slot.store(slot + 1, Ordering::Relaxed);
                }
            })
        };

        // The edges, each with the parts it applied, when it last applied
        // one from each parent, and a driver.
        let mut edges = Vec::new();
        let mut arrived = Vec::new();
        let mut drivers = Vec::new();
        for (index, edge_addr) in edge_addrs.iter().copied().enumerate() {
            let (applied_tx, mut applied_rx) = mpsc::unbounded_channel::<AppliedSlot>();
            let slots: Arc<StdMutex<HashSet<usize>>> = Arc::default();
            let delivered: Arc<StdMutex<HashMap<SocketAddr, Instant>>> = Arc::default();
            {
                let (slots, delivered) = (Arc::clone(&slots), Arc::clone(&delivered));
                tokio::spawn(async move {
                    while let Some(applied) = applied_rx.recv().await {
                        lock(&slots).insert(applied.slot_id);
                        lock(&delivered).insert(applied.parent, Instant::now());
                    }
                });
            }
            let edge = Arc::new(
                CacheMesh::new(
                    Arc::new(ChunkCache::new(options)),
                    demand_edge(&format!("edge-{index}"), edge_addr)
                        .with_peers(dist_addrs)
                        .with_max_parents(max_parents)
                        .with_subscription_lease(lease)
                        .with_applied_tx(applied_tx),
                )
                .start()
                .await
                .unwrap(),
            );
            let confirmed = move |_stream_id: u64, parent: SocketAddr| {
                lock(&delivered)
                    .get(&parent)
                    .is_some_and(|at| at.elapsed() <= Duration::from_millis(100))
            };
            let driver = {
                let (edge, stop) = (Arc::clone(&edge), Arc::clone(&stop));
                tokio::spawn(async move {
                    let mut changed = edge.parents_changed();
                    let mut next_renewal = Instant::now();
                    let mut early: VecDeque<Instant> = VecDeque::new();
                    let mut next_release = Instant::now();
                    while !stop.load(Ordering::Relaxed) {
                        let now = Instant::now();
                        let mut ask = now >= next_renewal;
                        if changed.has_changed().unwrap_or(false) {
                            changed.borrow_and_update();
                            edge.greet_parents().await.unwrap();
                            early = [now + Duration::from_millis(100), now + Duration::from_millis(300)]
                                .into();
                            ask = true;
                        }
                        if early.front().is_some_and(|at| now >= *at) {
                            early.pop_front();
                            ask = true;
                        }
                        if ask {
                            edge.subscribe(7, 0, backfill_parts).await.unwrap();
                            next_renewal = now + renewal;
                        }
                        if now >= next_release {
                            edge.release_pending(&confirmed).await;
                            next_release = now + Duration::from_millis(100);
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
            };
            edges.push(edge);
            arrived.push(slots);
            drivers.push(driver);
        }
        for edge in &edges {
            wait_for_hello(edge, 3).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        let first = next_slot.load(Ordering::Relaxed) as usize;

        let mut warm_tick_max = Vec::new();
        let mut boot_id = 10;
        for index in 0..3 {
            // Drain: every edge leaves it, make before break.
            let draining = Arc::clone(&lock(&distributors)[index].as_ref().unwrap().1);
            draining.set_draining(true);
            timeout(Duration::from_secs(5), async {
                while !draining.served_subscriptions().is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("dist-{index} kept a lease while it drained"));
            // Kill it.
            lock(&distributors)[index] = None;
            warm_tick_max.push(draining.fec_stats().warm_tick_max_bytes);
            draining.shutdown();
            drop(draining);
            tokio::time::sleep(Duration::from_millis(150)).await;
            // Start it again, empty, with a new boot id.
            boot_id += 1;
            let started = start_distributor(index, boot_id).await;
            lock(&distributors)[index] = Some(started);
            timeout(Duration::from_secs(3), async {
                loop {
                    let mut known = 0;
                    for edge in &edges {
                        known += edge.peer_statuses().await.iter().any(|peer| {
                            peer.peer == dist_addrs[index]
                                && peer.boot_id == Some(boot_id)
                                && !peer.draining
                        }) as usize;
                    }
                    if known == EDGES {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the edges did not hear dist-{index} start again"));
            // Its edges settle: nothing waits to be released.
            timeout(Duration::from_secs(3), async {
                while edges.iter().any(|edge| !edge.pending_parents().is_empty()) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("a parent that left was never released");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        // At least PARTS parts written across the roll, at 1,000 a second
        // however long its waits took.
        timeout(Duration::from_secs(10), async {
            while (next_slot.load(Ordering::Relaxed) as usize) <= first + PARTS {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the source stalled");
        let last = next_slot.load(Ordering::Relaxed) as usize - 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.store(true, Ordering::Relaxed);
        source.await.unwrap();
        for driver in drivers {
            driver.await.unwrap();
        }

        for (index, slots) in arrived.iter().enumerate() {
            let slots = lock(slots);
            let missing = (first..=last)
                .filter(|slot| !slots.contains(slot))
                .collect::<Vec<_>>();
            assert!(
                missing.is_empty(),
                "edge-{index} lost {} of {} parts: {:?}",
                missing.len(),
                last - first + 1,
                &missing[..missing.len().min(20)]
            );
        }
        let distributors = std::mem::take(&mut *lock(&distributors));
        for (_, handle) in distributors.iter().flatten() {
            warm_tick_max.push(handle.fec_stats().warm_tick_max_bytes);
            handle.shutdown();
        }
        let burst = (WARM_TOTAL as f64 * BACKFILL_BURST.as_secs_f64()) as u64;
        assert!(
            warm_tick_max.iter().all(|bytes| *bytes <= burst + PART as u64),
            "largest warm-up tick of each distributor {warm_tick_max:?}, the cap {burst}"
        );
        let counts = edges.iter().fold((0, 0), |(all, timed), edge| {
            let stats = edge.fec_stats();
            (
                all + stats.parents_released,
                timed + stats.parents_released_unconfirmed,
            )
        });
        for edge in &edges {
            edge.shutdown();
        }
        counts
    }

    /// The epoch of a test part: its first eight bytes.
    fn test_epoch(bytes: &[u8]) -> Option<u64> {
        Some(u64::from_be_bytes(bytes.get(..8)?.try_into().ok()?))
    }

    /// A test part of `epoch`, 64 bytes.
    fn epoch_part(epoch: u64, fill: u8) -> Bytes {
        let mut bytes = epoch.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[fill; 56]);
        Bytes::from(bytes)
    }

    /// A PL1: two contributors publish the same parts, so two distributors
    /// hold the same ids, each written by its own origin, and an edge takes
    /// the playlist from both. Equal bytes are applied once a part. Unequal
    /// bytes under one id and epoch keep the copy that came first and are
    /// counted (`apply_conflicts`); the edge never flips between the two.
    /// An older id of the epoch, from a parent that lags by the length of
    /// the ring, does not replace the newer id in its slot, and an older
    /// epoch is dropped (`apply_stale`); a newer epoch replaces.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_origins_of_one_epoch_merge_by_the_first_copy_at_an_edge() {
        let dist_addrs = [loopback_addr(), loopback_addr()];
        let edge_addr = loopback_addr();
        let mut caches = Vec::new();
        let mut distributors = Vec::new();
        for (index, addr) in dist_addrs.iter().enumerate() {
            let cache = Arc::new(ChunkCache::new(demand_options()));
            // The playlist exists at both before the lease: it starts live.
            cache.add_for_stream_id(7, 0, epoch_part(5, 0)).await.unwrap();
            distributors.push(
                CacheMesh::new(
                    Arc::clone(&cache),
                    demand_distributor(&format!("dist-{index}"), *addr)
                        .with_peer(edge_addr)
                        .with_subscription_lease(Duration::from_secs(10)),
                )
                .start()
                .await
                .unwrap(),
            );
            caches.push(cache);
        }
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel::<AppliedSlot>();
        let applied: Arc<StdMutex<Vec<usize>>> = Arc::default();
        {
            let applied = Arc::clone(&applied);
            tokio::spawn(async move {
                while let Some(event) = applied_rx.recv().await {
                    lock(&applied).push(event.slot_id);
                }
            });
        }
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            demand_edge("edge", edge_addr)
                .with_peers(dist_addrs)
                .with_max_parents(2)
                .with_subscription_lease(Duration::from_secs(10))
                .with_applied_tx(applied_tx)
                .with_slot_epoch(test_epoch),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 2).await;
        assert_eq!(edge.subscribe(7, 0, 0).await.unwrap().len(), 2);
        timeout(Duration::from_secs(3), async {
            while distributors
                .iter()
                .any(|distributor| distributor.served_subscriptions().is_empty())
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("both distributors serve the edge");
        let wait_stats = |want: fn(&CacheMeshFecStats) -> bool| {
            let edge = &edge;
            async move {
                timeout(Duration::from_secs(3), async {
                    while !want(&edge.fec_stats()) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("stats {:?}", edge.fec_stats()));
            }
        };

        // Equal bytes from both: each part is applied once.
        for slot in 1..=20 {
            for cache in &caches {
                cache
                    .add_for_stream_id(7, slot, epoch_part(5, slot as u8))
                    .await
                    .unwrap();
            }
        }
        wait_for_slot(&edge_cache, 7, 20).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut once = lock(&applied).clone();
        once.sort_unstable();
        assert_eq!(once, (1..=20).collect::<Vec<_>>());

        // Unequal bytes under the same ids and epoch: the first copy stays.
        for slot in 21..=30 {
            caches[0]
                .add_for_stream_id(7, slot, epoch_part(5, 0xa0))
                .await
                .unwrap();
        }
        wait_for_slot(&edge_cache, 7, 30).await;
        for slot in 21..=30 {
            caches[1]
                .add_for_stream_id(7, slot, epoch_part(5, 0xb0))
                .await
                .unwrap();
        }
        wait_stats(|stats| stats.apply_conflicts == 10).await;
        for slot in 21..=30 {
            assert_eq!(
                edge_cache.get_for_stream_id(7, slot).await.unwrap().0,
                epoch_part(5, 0xa0)
            );
        }

        // A parent that lags by the length of the ring (64 parts): its older
        // id of the epoch lands in the slot of the newer one, and is refused.
        caches[0]
            .add_for_stream_id(7, 100, epoch_part(5, 100))
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 100).await;
        caches[1]
            .add_for_stream_id(7, 36, epoch_part(5, 36))
            .await
            .unwrap();
        wait_stats(|stats| stats.apply_stale == 1).await;
        assert_eq!(
            edge_cache.get_for_stream_id(7, 100).await.unwrap().0,
            epoch_part(5, 100)
        );
        assert!(edge_cache.get_for_stream_id(7, 36).await.is_none());

        // An older epoch is dropped; a newer one replaces.
        caches[0]
            .add_for_stream_id(7, 101, epoch_part(6, 1))
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 101).await;
        caches[1]
            .add_for_stream_id(7, 101, epoch_part(4, 1))
            .await
            .unwrap();
        wait_stats(|stats| stats.apply_stale == 2).await;
        assert_eq!(
            edge_cache.get_for_stream_id(7, 101).await.unwrap().0,
            epoch_part(6, 1)
        );
        caches[0]
            .add_for_stream_id(7, 102, epoch_part(5, 2))
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 102).await;
        caches[1]
            .add_for_stream_id(7, 102, epoch_part(7, 2))
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            while edge_cache.get_for_stream_id(7, 102).await.map(|(bytes, _)| bytes)
                != Some(epoch_part(7, 2))
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a part of a newer epoch replaces");
        let stats = edge.fec_stats();
        assert_eq!((stats.apply_conflicts, stats.apply_stale), (10, 2));
        for distributor in &distributors {
            distributor.shutdown();
        }
        edge.shutdown();
    }

    /// Wave 8 review: before any hello (a cold start, the slot not started)
    /// the configured parents are asked (a mesh without the region filter)
    /// but none counts as live, so a new copy is warmed by its first parent
    /// alone.
    #[tokio::test]
    async fn a_cold_start_counts_no_live_parent_for_the_second_warm_up() {
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge = CacheMesh::new(
            edge_cache,
            demand_edge("edge", loopback_addr())
                .with_peers([loopback_addr(), loopback_addr(), loopback_addr()])
                // Without a region filter the configured parents are asked
                // before their first hello.
                .with_same_region_only(false)
                .with_max_parents(2),
        )
        .start()
        .await
        .unwrap();
        let (parents, live) = edge.parents_ranked(7).await;
        assert_eq!(parents.len(), 2);
        assert_eq!(live, 0);
        edge.shutdown();
    }

    /// Wave 8 review: with three live distributors (a slot joined) the
    /// first ranked parent may hold nothing of a new playlist yet: the
    /// second parent sends the warm-up window too. Without the setting
    /// (before) the window stays empty.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_slot_ranked_first_does_not_leave_the_warm_up_window_empty() {
        for warm_second in [true, false] {
            let addrs = [loopback_addr(), loopback_addr(), loopback_addr()];
            let edge_addr = loopback_addr();
            let mut caches = Vec::new();
            let mut parents = Vec::new();
            for (index, addr) in addrs.iter().enumerate() {
                let cache = Arc::new(ChunkCache::new(demand_options()));
                let parent = CacheMesh::new(
                    Arc::clone(&cache),
                    demand_distributor(&format!("dist-{index}"), *addr)
                        .with_peer(edge_addr)
                        .with_subscription_lease(Duration::from_secs(5)),
                )
                .start()
                .await
                .unwrap();
                caches.push(cache);
                parents.push(parent);
            }
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                demand_edge("edge", edge_addr)
                    .with_peers(addrs)
                    .with_max_parents(2)
                    .with_warm_from_second_parent(warm_second)
                    .with_subscription_lease(Duration::from_secs(5)),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 3).await;
            let ranked = edge.parents_for(7).await;
            // The first parent (the slot) holds nothing; the others 0..20.
            for (index, addr) in addrs.iter().enumerate() {
                if *addr == ranked[0] {
                    continue;
                }
                for slot in 0..20 {
                    caches[index]
                        .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 100]))
                        .await
                        .unwrap();
                }
            }
            edge.subscribe(7, 0, 8).await.unwrap();
            let arrived = timeout(Duration::from_secs(2), async {
                loop {
                    let mut have = 0;
                    for slot in 12..20 {
                        if edge_cache.get_for_stream_id(7, slot).await.is_some() {
                            have += 1;
                        }
                    }
                    if have == 8 {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .is_ok();
            assert_eq!(arrived, warm_second, "warm from the second parent: {warm_second}");
            for parent in &parents {
                parent.shutdown();
            }
            edge.shutdown();
        }
    }

    /// Wave 8 review: one large warm-up part (a 4K part of 2 MB) takes at
    /// most one burst of debt from the total bucket, so warm-ups go on
    /// within 200 ms, not seconds later; each lane has its own bucket.
    #[test]
    fn a_large_part_takes_at_most_one_burst_of_warm_up_debt() {
        let rate = 40_000_u64;
        let start = std::time::Instant::now();
        let mut buckets: [WarmBucket; 2] = Default::default();
        assert_eq!(buckets[1].available(rate, start), 4_000.0);
        buckets[1].spend(2_000_000, rate);
        assert_eq!(buckets[1].available(rate, start), -4_000.0);
        assert!(buckets[1].available(rate, start + Duration::from_millis(201)) > 0.0);
        // The audio lane's bucket is untouched.
        assert_eq!(buckets[0].available(rate, start), 4_000.0);
    }

    /// Wave 8 stage 5 (AM 13): eight children that warm at once share the
    /// distributor's total warm-up allowance, over each child's own; the
    /// largest warm-up burst of a tick stays within the total's burst, and
    /// every child gets its window in turn.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eight_children_warm_at_once_under_the_total_cap() {
        const CHILDREN: usize = 8;
        const PART: usize = 1_000;
        const WINDOW: usize = 8;
        let parent_addr = loopback_addr();
        let child_addrs = (0..CHILDREN).map(|_| loopback_addr()).collect::<Vec<_>>();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        // Each child alone could take 100,000 bytes a second; all of them
        // together 40,000 (4,000 bytes of burst a tick).
        let parent = CacheMesh::new(
            Arc::clone(&parent_cache),
            demand_distributor("parent", parent_addr)
                .with_peers(child_addrs.iter().copied())
                .with_max_children(CHILDREN)
                .with_subscription_lease(Duration::from_secs(5))
                .with_backfill_bytes_per_sec(100_000)
                .with_backfill_total_bytes_per_sec(40_000),
        )
        .start()
        .await
        .unwrap();
        for slot in 0..20 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; PART]))
                .await
                .unwrap();
        }
        let mut children = Vec::new();
        let mut caches = Vec::new();
        for (index, addr) in child_addrs.iter().enumerate() {
            let cache = Arc::new(ChunkCache::new(demand_options()));
            let child = CacheMesh::new(
                Arc::clone(&cache),
                demand_edge(&format!("edge-{index}"), *addr)
                    .with_peer(parent_addr)
                    .with_subscription_lease(Duration::from_secs(5)),
            )
            .start()
            .await
            .unwrap();
            children.push(child);
            caches.push(cache);
        }
        for child in &children {
            wait_for_hello(child, 1).await;
        }
        let started = Instant::now();
        for child in &children {
            child.subscribe(7, 0, WINDOW).await.unwrap();
        }
        // Every child gets its eight newest parts.
        timeout(Duration::from_secs(6), async {
            loop {
                let mut whole = 0;
                for cache in &caches {
                    let mut have = 0;
                    for slot in 12..20 {
                        if cache.get_for_stream_id(7, slot).await.is_some() {
                            have += 1;
                        }
                    }
                    if have == WINDOW {
                        whole += 1;
                    }
                }
                if whole == CHILDREN {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every child's warm-up window arrives under the total cap");
        let elapsed = started.elapsed();
        let stats = parent.fec_stats();
        let bytes = (CHILDREN * WINDOW * PART) as u64;
        assert_eq!(stats.warm_parts_sent, (CHILDREN * WINDOW) as u64);
        assert_eq!(stats.warm_bytes_sent, bytes);
        // 64,000 bytes at 40,000 a second, less the first 4,000 of burst:
        // 1.5 s at least (each child's own cap alone would allow 0.08 s).
        assert!(
            elapsed >= Duration::from_millis(1_400),
            "{bytes} warm-up bytes under a 40 kB/s total arrived in {elapsed:?}"
        );
        // A tick sends at most the total's burst, plus the one part that
        // finds the last tokens.
        assert!(
            stats.warm_tick_max_bytes <= (4_000 + PART) as u64,
            "largest warm-up tick {} bytes",
            stats.warm_tick_max_bytes
        );
        assert!(stats.warm_capped_ticks > 0);
        parent.shutdown();
        for child in &children {
            child.shutdown();
        }
    }

    /// A child's warm-up window goes after the live parts of every stream of
    /// the pass, so a live part never waits behind another stream's
    /// warm-up. (When the pass visits the warm-up stream first, a pass that
    /// queued everything in one batch per child sent the window first.)
    ///
    /// A part leaves on its publication to the children of the last tick.
    /// With one child per tick, the parts published during the other
    /// child's turn reach the edge on its next tick: the tick on which its
    /// new window opens, so the pass has both.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_live_part_is_not_held_behind_a_warm_up_window() {
        let parent_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let other_addr = loopback_addr();
        let parent_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let other_cache = Arc::new(ChunkCache::new(demand_options()));
        let mut parent_config = demand_distributor("parent", parent_addr)
            .with_peers([edge_addr, other_addr])
            .with_max_children(1)
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
        let other = CacheMesh::new(
            Arc::clone(&other_cache),
            demand_edge("other", other_addr)
                .with_peer(parent_addr)
                .with_subscription_lease(Duration::from_secs(5)),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        wait_for_hello(&other, 1).await;

        // Stream 8 is live at both children.
        parent_cache
            .add_for_stream_id(8, 0, Bytes::from(vec![8_u8; 100]))
            .await
            .unwrap();
        // A subscription is renewed until its part comes, as a node with
        // readers does: a loaded host can lose a datagram.
        timeout(Duration::from_secs(5), async {
            loop {
                let mut arrived = true;
                for (child, cache) in [(&edge, &edge_cache), (&other, &other_cache)] {
                    if cache.get_for_stream_id(8, 0).await.is_none() {
                        arrived = false;
                        child.subscribe(8, 0, 1).await.unwrap();
                    }
                }
                if arrived {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("stream 8 did not reach both children");

        // Stream 7 has forty parts that the edge has not asked for yet.
        for slot in 0..40 {
            parent_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 1_000]))
                .await
                .unwrap();
        }
        while applied_rx.try_recv().is_ok() {}

        // Stream 8 goes on through both children's turns, and the edge asks
        // for the whole window of stream 7 meanwhile.
        let published: Arc<StdMutex<HashMap<usize, u64>>> = Arc::default();
        let publisher = {
            let cache = Arc::clone(&parent_cache);
            let published = Arc::clone(&published);
            tokio::spawn(async move {
                for slot in 1..=40_usize {
                    lock(&published).insert(slot, unix_micros());
                    cache
                        .add_for_stream_id(8, slot, Bytes::from(vec![8_u8; 100]))
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
        };
        tokio::time::sleep(Duration::from_millis(250)).await;
        edge.subscribe(7, 0, 40).await.unwrap();

        let mut order = Vec::new();
        timeout(Duration::from_secs(5), async {
            while order
                .iter()
                .filter(|event: &&AppliedSlot| event.stream_id == 7)
                .count()
                < 40
                || !order
                    .iter()
                    .any(|event: &AppliedSlot| (event.stream_id, event.slot_id) == (8, 40))
            {
                match timeout(Duration::from_millis(250), applied_rx.recv()).await {
                    Ok(event) => order.push(event.unwrap()),
                    // Nothing for a while: the subscription may be lost.
                    Err(_) => {
                        edge.subscribe(7, 0, 40).await.unwrap();
                    }
                }
            }
        })
        .await
        .expect("a part did not arrive");
        publisher.await.unwrap();
        let published = lock(&published).clone();

        let window = order.iter().position(|event| event.stream_id == 7).unwrap();
        let window_us = order[window].rx_first_us;
        // The window went out on the edge's tick, in one go after the live
        // parts of that tick.
        assert!(
            order[window..window + 40]
                .iter()
                .all(|event| event.stream_id == 7),
            "{order:?}"
        );
        // That tick had live parts: parts published during the other child's
        // turn, which waited for it and arrived just before the window.
        assert!(
            order[..window].iter().any(|event| {
                event.stream_id == 8
                    && window_us.saturating_sub(event.rx_first_us) < 50_000
                    && event.rx_first_us.saturating_sub(published[&event.slot_id]) > 100_000
            }),
            "the window's tick carried no held live part: {order:?}"
        );
        // No live part that was there well before the window came after it.
        for event in &order[window..] {
            if event.stream_id == 8 {
                assert!(
                    published[&event.slot_id] + 20_000 > window_us,
                    "part {} waited behind the window: {order:?}",
                    event.slot_id
                );
            }
        }

        parent.shutdown();
        edge.shutdown();
        other.shutdown();
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

    /// Datagrams of a parent that never complete a block (a 4K part waiting
    /// for repair, a decode behind a burst) still say that the parent is
    /// alive: liveness is stamped on receipt, before FEC decode.
    ///
    /// On a mesh split by class the datagrams go only to the class socket,
    /// from the parent's class socket (its main port plus the offset): a 4K
    /// parent whose video is all on the class lane is heard there. Junk from
    /// a source that is not a peer is refused by the allowlist before it can
    /// be stamped, on either lane.
    #[tokio::test]
    async fn a_parent_is_heard_on_receipt_before_any_block_decodes() {
        /// The parent's main address, how long the edge's parents have been
        /// silent, every address that the edge has stamped heard, and how
        /// many datagrams it refused.
        async fn heard_after_undecodable_datagrams(
            class_offset: u16,
        ) -> (SocketAddr, Option<Duration>, Vec<SocketAddr>, u64) {
            let (parent_addr, parent) = if class_offset == 0 {
                let parent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
                (parent.local_addr().unwrap(), parent)
            } else {
                let parent_addr = class_split_addr(class_offset);
                let class = SocketAddr::from(([127, 0, 0, 1], parent_addr.port() + class_offset));
                (parent_addr, std::net::UdpSocket::bind(class).unwrap())
            };
            let stranger = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let edge_addr = if class_offset == 0 {
                loopback_addr()
            } else {
                class_split_addr(class_offset)
            };
            let edge = CacheMesh::new(
                Arc::new(ChunkCache::new(demand_options())),
                // Not same-region-only: a parent that has not said hello
                // yet is one of the edge's parents.
                CacheMeshConfig::new("edge", "uk", edge_addr)
                    .with_role(CacheMeshRole::Edge)
                    .with_peer(parent_addr)
                    .with_class_port_offset(class_offset)
                    .with_stream_classifier(video_streams(&[9])),
            )
            .start()
            .await
            .unwrap();
            let to = if class_offset == 0 {
                edge_addr
            } else {
                edge.class_local_addr().expect("a class socket")
            };
            assert_eq!(to.port(), edge_addr.port() + class_offset);
            assert!(edge.parents_silent_for().await.is_none());
            // Bytes that no FEC block ever decodes: only receipt hears them.
            for _ in 0..5 {
                parent.send_to(b"not a fec datagram", to).unwrap();
                stranger.send_to(b"not a fec datagram", to).unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            let silent = edge.parents_silent_for().await;
            let stamped = lock(&edge.shared.peer_seen).keys().copied().collect();
            let refused = edge.fec_stats().rx_unauthorized_datagrams;
            edge.shutdown();
            (parent_addr, silent, stamped, refused)
        }
        for class_offset in [0, TEST_CLASS_OFFSET] {
            let (parent_addr, silent, stamped, refused) =
                heard_after_undecodable_datagrams(class_offset).await;
            let silent = silent.expect("the parent was heard on receipt");
            assert!(
                silent < Duration::from_millis(200),
                "{class_offset}: {silent:?}"
            );
            // The parent, under its main address, and not the stranger,
            // whose five datagrams the allowlist refused.
            assert_eq!(stamped, vec![parent_addr], "{class_offset}");
            assert_eq!(refused, 5, "{class_offset}");
        }
    }

    /// A node bound to a wildcard address may be told its own address by a
    /// peer's hello, and then sends hellos to itself. Receipt must not make
    /// it hear itself, or an edge would rank itself a live parent.
    #[tokio::test]
    async fn a_node_that_sends_itself_hellos_is_not_its_own_live_parent() {
        let own = loopback_addr();
        let bind = SocketAddr::from(([0, 0, 0, 0], own.port()));
        let edge = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            CacheMeshConfig::new("edge", "uk", bind)
                .with_role(CacheMeshRole::Edge)
                .with_peer(own)
                .with_announce_interval(Duration::from_millis(10)),
        )
        .start()
        .await
        .unwrap();
        // Its own hellos arrive from `own`, which is not the bind address.
        let mut silent = Vec::new();
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            silent.push(edge.parents_silent_for().await);
        }
        assert!(
            edge.fec_stats().rx_decoded_objects > 0,
            "the node heard its own hellos"
        );
        assert!(silent.iter().all(Option::is_none), "{silent:?}");
        assert!(!lock(&edge.shared.peer_seen).contains_key(&own));
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
        let old = cache.resolve_or_create_stream(7).await;
        cache
            .add_for_handle(old, 0, Bytes::from_static(b"part"))
            .await
            .unwrap();
        remote_slots.write().await.insert((old, 0_usize));
        // Stream 7 again, on another index with the same generation number.
        cache.zero_stream_id(7);
        let stream = cache.resolve_or_create_stream(7).await;
        assert_ne!(stream, old);
        for slot_id in 0..400_usize {
            cache
                .add_for_handle(stream, slot_id, Bytes::from_static(b"part"))
                .await
                .unwrap();
            remote_slots.write().await.insert((stream, slot_id));
        }
        // A stream that the cache does not hold.
        let gone = cache.resolve_or_create_stream(9).await;
        cache.release_stream_id(9).await;
        remote_slots.write().await.insert((gone, 3_usize));

        forget_unretained_remote_slots(&cache, &remote_slots).await;

        // The cache retains the last 64 slots of the stream.
        let left = remote_slots.read().await;
        assert_eq!(left.len(), 64);
        assert!(left
            .iter()
            .all(|(kept, slot_id)| *kept == stream && *slot_id >= 336));
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
        parent_cache.zero_stream_id(7);
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
                    .is_some_and(|(bytes, _)| bytes == vec![2_u8; 32]);
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

    /// A 40-byte part tagged with its publication (`A` or `B`) and its slot:
    /// one fitted symbol.
    fn tagged_part(tag: u8, slot: usize) -> Bytes {
        let mut part = vec![0_u8; 40];
        part[0] = tag;
        part[1..9].copy_from_slice(&(slot as u64).to_le_bytes());
        Bytes::from(part)
    }

    /// A restarted publication reaches an edge whole, in full and in demand
    /// replication, and nothing of the old one shows again: the edge ring
    /// has wrapped, so every restarted part lands on a slot that holds a
    /// newer id of the old publication (C7), and every restarted part has
    /// the same stream, slot and length as an old one (the encoding is
    /// keyed by the payload hash).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_restarted_publication_reaches_edges_whole_and_old_parts_never_reappear() {
        for demand in [false, true] {
            restarted_publication_run(demand).await;
        }
    }

    async fn restarted_publication_run(demand: bool) {
        let options = Options {
            num_playlists: 4,
            max_segments: 2,
            max_parts_per_segment: 32,
            buffer_size_kb: 4,
            ..Options::default()
        };
        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(options));
        let edge_cache = Arc::new(ChunkCache::new(options));
        let (distributor_config, edge_config) = if demand {
            (
                demand_distributor("distributor", distributor_addr).with_peer(edge_addr),
                demand_edge("edge", edge_addr).with_peer(distributor_addr),
            )
        } else {
            (
                CacheMeshConfig::new("distributor", "uk", distributor_addr)
                    .with_peer(edge_addr)
                    .with_role(CacheMeshRole::Distributor)
                    .with_sync_interval(Duration::from_millis(10))
                    .with_announce_interval(Duration::from_millis(50)),
                CacheMeshConfig::new("edge", "uk", edge_addr)
                    .with_peer(distributor_addr)
                    .with_role(CacheMeshRole::Edge)
                    .with_announce_interval(Duration::from_millis(50)),
            )
        };
        let distributor = CacheMesh::new(Arc::clone(&distributor_cache), distributor_config)
            .start()
            .await
            .unwrap();
        let edge = CacheMesh::new(Arc::clone(&edge_cache), edge_config)
            .start()
            .await
            .unwrap();
        wait_for_hello(&edge, 1).await;
        let subscribe = || async {
            if demand {
                edge.subscribe(7, 0, 16).await.unwrap();
            }
        };
        subscribe().await;

        // Phase A: 200 parts, so the edge's ring of 64 wraps three times.
        let init_a = Bytes::from_static(b"init-a");
        let init_b = Bytes::from_static(b"init-b");
        let first = distributor_cache.resolve_or_create_stream(7).await;
        distributor_cache
            .set_stream_initialization_for_handle(first, init_a.clone())
            .unwrap();
        for slot in 0..200 {
            distributor_cache
                .add_for_handle(first, slot, tagged_part(b'A', slot))
                .await
                .unwrap();
            if slot % 50 == 49 {
                subscribe().await;
                wait_for_slot(&edge_cache, 7, slot).await;
            }
        }
        wait_for_slot(&edge_cache, 7, 199).await;
        let old_edge = edge_cache.resolve_stream(7).unwrap();
        assert_eq!(edge_cache.stream_initialization(7), Some(init_a.clone()));

        // A reader polls the edge throughout: once it has read a restarted
        // part or initialization, the old one never shows again.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader = {
            let edge_cache = Arc::clone(&edge_cache);
            let stop = Arc::clone(&stop);
            let (init_a, init_b) = (init_a.clone(), init_b.clone());
            tokio::spawn(async move {
                let mut restarted = [false; 50];
                let mut restarted_init = false;
                loop {
                    // A last pass after the stop, which comes once every
                    // restarted part is there.
                    let stopping = stop.load(Ordering::Relaxed);
                    for (slot, restarted) in restarted.iter_mut().enumerate() {
                        if let Some((bytes, _)) = edge_cache.get_for_stream_id(7, slot).await {
                            assert_eq!(&bytes[1..9], &(slot as u64).to_le_bytes());
                            match bytes[0] {
                                b'B' => *restarted = true,
                                b'A' => assert!(!*restarted, "slot {slot} went back"),
                                tag => panic!("tag {tag}"),
                            }
                        }
                    }
                    match edge_cache.stream_initialization(7) {
                        Some(init) if init == init_b => restarted_init = true,
                        Some(init) if init == init_a => {
                            assert!(!restarted_init, "the old initialization came back")
                        }
                        other => assert!(other.is_none(), "{other:?}"),
                    }
                    if stopping {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                restarted.iter().all(|seen| *seen) && restarted_init
            })
        };

        // The restart.
        let before = distributor.fec_stats();
        distributor_cache.zero_stream_id(7);
        let second = distributor_cache.resolve_or_create_stream(7).await;
        // Another lifetime of the stream; the generation is per index.
        assert_ne!(second, first);
        assert!(distributor_cache.get_for_handle(first, 199).await.is_none());
        assert!(distributor_cache.last_for_handle(first).is_none());
        distributor_cache
            .set_stream_initialization_for_handle(second, init_b.clone())
            .unwrap();
        for slot in 0..50 {
            distributor_cache
                .add_for_handle(second, slot, tagged_part(b'B', slot))
                .await
                .unwrap();
        }
        timeout(Duration::from_secs(5), async {
            loop {
                let mut whole = edge_cache.stream_initialization(7) == Some(init_b.clone());
                for slot in 0..50 {
                    whole &= edge_cache
                        .get_for_stream_id(7, slot)
                        .await
                        .is_some_and(|(bytes, _)| bytes == tagged_part(b'B', slot));
                }
                if whole {
                    return;
                }
                subscribe().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!("the restarted publication did not reach the edge (demand {demand})")
        });
        stop.store(true, Ordering::Relaxed);
        assert!(reader.await.unwrap(), "the reader saw every restarted part");

        // The wire carries no generation: the edge keeps its stream (C7).
        assert_eq!(edge_cache.resolve_stream(7), Some(old_edge));
        // Each restarted part was encoded anew, not taken from the encodings
        // of the old parts with the same stream, slot and length.
        let encoded = distributor.fec_stats().tx_objects - before.tx_objects;
        assert!(encoded >= 50, "{encoded} objects (demand {demand})");
        // The sync task forgets the old generation's cursors at its prune.
        timeout(Duration::from_secs(2), async {
            loop {
                let cursors = lock(&distributor.shared.cursor_streams).clone();
                if cursors.contains(&second) && !cursors.contains(&first) {
                    return;
                }
                subscribe().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the cursors of the old generation were not forgotten");

        distributor.shutdown();
        edge.shutdown();
    }

    async fn next_applied(
        applied_rx: &mut mpsc::UnboundedReceiver<AppliedSlot>,
        slot_id: usize,
    ) -> AppliedSlot {
        let event = timeout(Duration::from_secs(2), applied_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("slot {slot_id} was not applied"))
            .unwrap();
        assert_eq!((event.stream_id, event.slot_id), (7, slot_id));
        event
    }

    /// A part that the cache refuses whatever its stream (the id `u64::MAX`,
    /// the cache's empty marker) is refused before its stream is created: on
    /// a full edge it does not evict a live stream.
    #[tokio::test]
    async fn a_refused_part_id_does_not_evict_a_live_stream() {
        let edge_addr = loopback_addr();
        let raw_addr = loopback_addr();
        let edge_cache = Arc::new(ChunkCache::new(Options {
            num_playlists: 1,
            ..demand_options()
        }));
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_allowed_peers([raw_addr])
                .with_role(CacheMeshRole::Edge)
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        let live = edge_cache.resolve_or_create_stream(7).await;
        edge_cache
            .add_for_handle(live, 0, Bytes::from_static(b"live"))
            .await
            .unwrap();

        let raw = UdpSocket::bind(raw_addr).await.unwrap();
        let mut raw_sender = FecSender::new(1, DEFAULT_SYMBOL_SIZE);
        for (stream_id, slot_id) in [(9, u64::MAX), (7, 1)] {
            let frame = MeshFrame::chunk("raw", stream_id, slot_id, Bytes::from_static(b"part"))
                .encode()
                .unwrap();
            raw_sender.send(&raw, edge_addr, &frame).await.unwrap();
        }
        // One socket's frames are applied in order: part 1 of stream 7
        // comes after the refused part.
        next_applied(&mut applied_rx, 1).await;
        assert_eq!(edge_cache.resolve_stream(7), Some(live));
        assert!(edge_cache.resolve_stream(9).is_none());
        assert_eq!(
            edge_cache.get_for_handle(live, 0).await.unwrap().0,
            Bytes::from_static(b"live")
        );
        assert_eq!(edge.fec_stats().rx_unauthorized_datagrams, 0);
        edge.shutdown();
    }

    /// The readers of an edge, started before anything is published, wake
    /// for parts that arrive over the mesh: an exact-part waiter on a stream
    /// that the edge does not have yet, a lane notifier, and a publication
    /// subscriber. Then a reader that holds a handle of a stream that the
    /// edge released finds it stale and resolves the new one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocking_readers_wake_for_parts_that_arrive_over_the_mesh() {
        let wake_bound_us = if cfg!(debug_assertions) {
            10_000
        } else {
            2_000
        };
        let distributor_addr = loopback_addr();
        let edge_addr = loopback_addr();
        let raw_addr = loopback_addr();
        let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
        let edge_cache = Arc::new(ChunkCache::new(demand_options()));
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            CacheMeshConfig::new("distributor", "uk", distributor_addr)
                .with_peer(edge_addr)
                .with_role(CacheMeshRole::Distributor)
                .with_sync_interval(Duration::from_millis(10))
                .with_announce_interval(Duration::from_millis(50)),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_allowed_peers([raw_addr])
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(50))
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        let playlists = crate::m3u8_cache::M3u8Cache::new(demand_options());

        // 1. An exact-part waiter on a stream that the edge does not have: it
        // is given (C6) and creates no mapping.
        let waiter = edge_cache
            .exact_part_waiter(7, 0)
            .expect("a waiter for a stream that is not mapped yet");
        assert!(edge_cache.resolve_stream(7).is_none());
        let (armed_tx, armed_rx) = tokio::sync::oneshot::channel();
        let exact = {
            let edge_cache = Arc::clone(&edge_cache);
            tokio::spawn(async move {
                let mut armed_tx = Some(armed_tx);
                loop {
                    let notified = waiter.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if edge_cache.get_for_stream_id(7, 0).await.is_some() {
                        break;
                    }
                    if let Some(armed_tx) = armed_tx.take() {
                        let _ = armed_tx.send(());
                    }
                    notified.await;
                }
                let woke_us = unix_micros();
                // The write time is there as soon as the part is: no await.
                let written_us = edge_cache
                    .written_unix_us_for_stream_id(7, 0)
                    .expect("the write time of a part that a waiter was woken for");
                assert!(written_us <= unix_micros());
                woke_us
            })
        };
        armed_rx.await.unwrap();
        // 3. A publication subscriber, before the first part.
        let moved = edge_cache.subscribe_publications();
        let mut drained = Vec::new();

        distributor_cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"part-0"))
            .await
            .unwrap();
        let event = next_applied(&mut applied_rx, 0).await;
        let woke_us = exact.await.unwrap();
        assert!(
            woke_us.saturating_sub(event.committed_us) < wake_bound_us,
            "exact waiter woke {} us after the commit",
            woke_us.saturating_sub(event.committed_us)
        );
        let stream = edge_cache.resolve_stream(7).unwrap();
        drained.clear();
        moved.drain(&mut drained);
        assert_eq!(drained, vec![stream.index()]);
        playlists
            .add_part_us(7, 2_500, true, Some(event.bytes))
            .unwrap();

        // 2. The lane notifier wakes for the next part, and for a new
        // initialization.
        let lane = edge_cache.update_notifier(stream.index()).unwrap();
        let (armed_tx, armed_rx) = tokio::sync::oneshot::channel();
        let lane_reader = {
            let edge_cache = Arc::clone(&edge_cache);
            tokio::spawn(async move {
                let mut armed_tx = Some(armed_tx);
                loop {
                    let notified = lane.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if edge_cache.last_for_handle(stream) == Some(1) {
                        break;
                    }
                    if let Some(armed_tx) = armed_tx.take() {
                        let _ = armed_tx.send(());
                    }
                    notified.await;
                }
                let woke_us = unix_micros();
                loop {
                    let notified = lane.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if edge_cache
                        .stream_initialization_for_handle(stream)
                        .as_deref()
                        == Some(b"init".as_slice())
                    {
                        break;
                    }
                    notified.await;
                }
                woke_us
            })
        };
        armed_rx.await.unwrap();
        distributor_cache
            .add_for_stream_id(7, 1, Bytes::from_static(b"part-1"))
            .await
            .unwrap();
        let event = next_applied(&mut applied_rx, 1).await;
        distributor_cache
            .set_stream_initialization(7, Bytes::from_static(b"init"))
            .await
            .unwrap();
        let woke_us = timeout(Duration::from_secs(2), lane_reader)
            .await
            .expect("the lane notifier did not wake for the initialization")
            .unwrap();
        assert!(
            woke_us.saturating_sub(event.committed_us) < wake_bound_us,
            "lane reader woke {} us after the commit",
            woke_us.saturating_sub(event.committed_us)
        );
        drained.clear();
        moved.drain(&mut drained);
        assert_eq!(drained, vec![stream.index()]);
        playlists
            .add_part_us(7, 2_500, false, Some(event.bytes))
            .unwrap();

        // The publication set is marked exactly when the edge's watermark
        // advances: a part beyond a gap moves it, the part of the gap does
        // not.
        let raw = UdpSocket::bind(raw_addr).await.unwrap();
        let mut raw_sender = FecSender::new(1, DEFAULT_SYMBOL_SIZE);
        for slot in [4_u64, 3] {
            let frame = MeshFrame::chunk("raw", 7, slot, Bytes::from(vec![slot as u8; 20]))
                .encode()
                .unwrap();
            raw_sender.send(&raw, edge_addr, &frame).await.unwrap();
            next_applied(&mut applied_rx, slot as usize).await;
            drained.clear();
            moved.drain(&mut drained);
            if slot == 4 {
                assert_eq!(drained, vec![stream.index()], "the watermark moved to 4");
            } else {
                assert!(drained.is_empty(), "slot 3 is below the watermark");
            }
        }
        assert_eq!(edge_cache.last_for_handle(stream), Some(4));

        // The edge releases the stream: a reader's handle is stale, and the
        // next part makes a new stream that it resolves.
        assert!(edge_cache.release_stream_id(7).await);
        assert!(edge_cache.last_for_handle(stream).is_none());
        assert!(edge_cache.get_for_handle(stream, 1).await.is_none());
        distributor_cache
            .add_for_stream_id(7, 5, Bytes::from_static(b"part-5"))
            .await
            .unwrap();
        next_applied(&mut applied_rx, 5).await;
        let renewed = edge_cache.resolve_stream(7).unwrap();
        assert_ne!(renewed, stream);
        assert_eq!(edge_cache.last_for_handle(renewed), Some(5));
        assert!(edge_cache.last_for_handle(stream).is_none());

        // The playlist side: a reader's notifier for a zeroed playlist stream
        // is gone with it.
        let playlist = playlists.resolve_stream(7).unwrap();
        assert!(playlists.update_notifier_for_handle(playlist).is_some());
        playlists.zero_stream_id(7);
        assert!(playlists.update_notifier_for_handle(playlist).is_none());

        distributor.shutdown();
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
        lock(&edge.shared.wanted).insert(7);
        let before = distributor.fec_stats().tx_objects;
        assert_eq!(edge.request_parts(7, 4, 2, 1).await.unwrap(), 1);
        wait_for_slot(&edge_cache, 7, 5).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stream = edge_cache.resolve_stream(7).unwrap();
        assert!(edge_cache.get_for_handle(stream, 4).await.is_some());
        assert!(edge_cache.get_for_handle(stream, 5).await.is_some());
        for slot in [3_usize, 6, 9] {
            assert!(
                edge_cache.get_for_handle(stream, slot).await.is_none(),
                "slot {slot}"
            );
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
    fn a_config_with_a_send_observer_stays_printable() {
        let addr: SocketAddr = "127.0.0.1:9915".parse().unwrap();
        let config = CacheMeshConfig::new("node", "uk", addr);
        assert!(config.send_observer.0.is_none());
        let config = config
            .with_sync_interval(Duration::from_millis(50))
            .with_send_observer(Arc::new(|_, _, _, _| {}));
        assert_eq!(config.sync_interval, Duration::from_millis(50));
        assert!(config.send_observer.0.is_some());
        // The configuration stays printable with an observer in it.
        assert!(format!("{config:?}").contains("send_observer: Some(..)"));
        let config = config.with_live_push_filter(Arc::new(|_, _| false));
        assert!(format!("{config:?}").contains("live_push_filter: Some(..)"));
    }

    type SentRecord = (u64, u64, SocketAddr, u64);

    /// Start a distributor and an edge with a 50 ms sync interval, then
    /// commit parts at the distributor one at a time, each as soon as the edge
    /// has applied the one before. Returns how long each part took from its
    /// commit to its application, the applied slots, and what the send
    /// observer saw.
    async fn commit_to_apply(
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
    async fn a_committed_part_leaves_at_once() {
        let (mut latencies, applied, sent, distributor_addr, edge_addr) = commit_to_apply(19).await;
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

    #[tokio::test]
    async fn seen_watermarks_tell_a_pass_which_streams_moved() {
        let cache = ChunkCache::new(demand_options());
        let seven = cache.resolve_or_create_stream(7).await;
        let nine = cache.resolve_or_create_stream(9).await;
        let mut seen = SeenWatermarks::default();
        assert_eq!(seen.record(seven, Some(3)), None);
        assert_eq!(seen.record(seven, Some(3)), Some(3), "unchanged");
        assert_eq!(seen.record(seven, Some(5)), Some(3), "advanced");
        assert_eq!(seen.record(nine, None), None);
        // The same stream in another lifetime is another stream, even with
        // the same generation number on another index.
        cache.zero_stream_id(7);
        let restarted = cache.resolve_or_create_stream(7).await;
        assert_eq!(seen.record(restarted, Some(1)), None);
        seen.retain_streams(&[restarted]);
        assert_eq!(seen.record(seven, Some(5)), None, "forgotten");
        assert_eq!(seen.record(restarted, Some(2)), Some(1), "kept");
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
        shared: Arc<MeshShared>,
        /// Stream and slot, in whatever lifetime the receiver gave it.
        slot: (u64, usize),
        // 0: not woken, 1: marked, 2: not marked, 3: set locked.
        seen: std::sync::atomic::AtomicU8,
    }

    impl std::task::Wake for RemoteAtWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let seen = match self.shared.remote_slots.try_read() {
                Ok(remote_slots)
                    if remote_slots
                        .iter()
                        .any(|(stream, slot_id)| (stream.stream_id(), *slot_id) == self.slot) =>
                {
                    1
                }
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
            shared: Arc::clone(&receiver.shared),
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
        let stream = receiver_cache.resolve_stream(7).unwrap();
        assert!(receiver
            .shared
            .remote_slots
            .read()
            .await
            .contains(&(stream, 0)));
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
        // hello, after these parts are published. The edge is authorized.
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            CacheMeshConfig::new("distributor", "uk", distributor_addr)
                .with_allowed_peers([edge_addr])
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

    #[test]
    fn generic_segmentation_offload_is_on_by_default() {
        let config = CacheMeshConfig::new("node", "uk", "127.0.0.1:9916".parse().unwrap());
        assert!(config.gso);
        assert!(!config.without_gso().gso);
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

    /// What else runs during a throughput run.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Load {
        /// Nothing: the run measures the send path alone.
        None,
        /// The caches are in use as in a product: the distributor churns
        /// other streams through its lanes, and each edge has readers that
        /// poll, a reader that blocks on each part, and a playlist fed from
        /// the parts it applies with readers of its own. The edges replicate
        /// on demand, so the churned streams stay at the distributor.
        CachesInUse,
    }

    /// A part of a loaded run: its slot and the generation of its stream in
    /// the first 16 bytes.
    fn load_part(slot: usize, generation: u64, part_bytes: usize) -> Bytes {
        let mut part = vec![7_u8; part_bytes.max(16)];
        part[..8].copy_from_slice(&(slot as u64).to_le_bytes());
        part[8..16].copy_from_slice(&generation.to_le_bytes());
        Bytes::from(part)
    }

    /// What the readers of a loaded run saw.
    #[derive(Default)]
    struct LoadChecks {
        polled: std::sync::atomic::AtomicU64,
        mismatches: std::sync::atomic::AtomicU64,
        /// Parts that each edge's blocking reader read, in order.
        blocking_reads: Vec<std::sync::atomic::AtomicU64>,
        /// Microseconds from each part's commit at an edge to the wake of
        /// its blocking reader.
        blocking_wakes: StdMutex<Vec<u64>>,
        playlist_errors: std::sync::atomic::AtomicU64,
        playlist_reads: std::sync::atomic::AtomicU64,
    }

    /// A distributor alone on one core (a one-thread runtime on its own OS
    /// thread) sends to two children, while a publisher thread commits
    /// `rate` parts of `part_bytes` a second for `seconds` at its cache, each
    /// at its own deadline. Returns, sorted, the microseconds from each
    /// part's commit to the arrival of its first datagram at each child, and
    /// from the commit to the send observer's time; and the parts that a
    /// child never received. With [`Load::CachesInUse`] it also checks what
    /// the readers saw.
    fn publish_to_two_children(
        children: Children,
        load: Load,
        rate: u64,
        seconds: u64,
        part_bytes: usize,
    ) -> (Vec<u64>, Vec<u64>, usize) {
        // One run at a time: they measure time, and each takes cores.
        static RUNS: StdMutex<()> = StdMutex::new(());
        let _run = lock(&RUNS);
        let loaded = load == Load::CachesInUse;
        assert!(!loaded || children == Children::MeshEdges);
        let parts = (rate * seconds) as usize;
        let sends: Arc<StdMutex<Vec<(u64, u64)>>> = Arc::default();
        let options = Options {
            // Lanes for the churned streams beside stream 7.
            num_playlists: if loaded { 8 } else { 2 },
            max_segments: 1,
            max_parts_per_segment: parts + 64,
            ..Options::default()
        };
        // An edge's ring wraps in about a second at 4,000 parts a second.
        let edge_options = if loaded {
            Options {
                num_playlists: 8,
                max_segments: 2,
                max_parts_per_segment: 2_048,
                buffer_size_kb: 64,
                ..Options::default()
            }
        } else {
            options
        };
        let lease = Duration::from_secs(seconds + 10);
        let distributor_addr = loopback_addr();
        let child_addrs = [loopback_addr(), loopback_addr()];
        let distributor_cache = Arc::new(ChunkCache::new(options));
        let checks = Arc::new(LoadChecks {
            blocking_reads: (0..2)
                .map(|_| std::sync::atomic::AtomicU64::new(0))
                .collect(),
            ..LoadChecks::default()
        });

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
        let mut edge_caches = Vec::new();
        let mut playlists = Vec::new();
        for (index, addr) in child_addrs.into_iter().enumerate() {
            if children == Children::Sockets {
                let (child_arrivals, thread) = raw_child(addr, Arc::clone(&stop));
                arrivals.push(child_arrivals);
                threads.push(thread);
                continue;
            }
            let child_arrivals: Arrivals = Arc::default();
            arrivals.push(Arc::clone(&child_arrivals));
            let edge_cache = Arc::new(ChunkCache::new(edge_options));
            let playlist = Arc::new(crate::m3u8_cache::M3u8Cache::new(Options::default()));
            let (applied_tx, mut applied_rx) = mpsc::unbounded_channel::<AppliedSlot>();
            let commits: Arc<StdMutex<HashMap<usize, u64>>> = Arc::default();
            {
                let playlist = Arc::clone(&playlist);
                let checks = Arc::clone(&checks);
                let commits = Arc::clone(&commits);
                runtime.spawn(async move {
                    while let Some(event) = applied_rx.recv().await {
                        lock(&child_arrivals)
                            .entry(event.slot_id)
                            .or_insert(event.rx_first_us);
                        if loaded {
                            lock(&commits).insert(event.slot_id, event.committed_us);
                            let independent = event.slot_id % 400 == 0;
                            if playlist
                                .add_part_us(7, 2_500, independent, Some(event.bytes))
                                .is_err()
                            {
                                checks.playlist_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                });
            }
            let mut config = CacheMeshConfig::new(format!("edge-{index}"), "uk", addr)
                .with_peer(distributor_addr)
                .with_role(CacheMeshRole::Edge)
                .with_socket_buffer_bytes(4 << 20)
                .with_announce_interval(Duration::from_millis(100))
                .with_applied_tx(applied_tx);
            if loaded {
                config = config
                    .with_replication(ReplicationMode::Demand)
                    .with_subscription_lease(lease);
            }
            edges.push(runtime.block_on(async {
                CacheMesh::new(Arc::clone(&edge_cache), config)
                    .start()
                    .await
                    .unwrap()
            }));
            if loaded {
                threads.extend(edge_readers(
                    index,
                    &edge_cache,
                    &playlist,
                    &commits,
                    &checks,
                    &stop,
                    parts,
                ));
            }
            edge_caches.push(edge_cache);
            playlists.push(playlist);
        }

        // The distributor.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let (distributor_applied_tx, mut distributor_applied_rx) = mpsc::unbounded_channel();
        let distributor_stats: Arc<StdMutex<Option<CacheMeshFecStats>>> = Arc::default();
        let distributor = {
            let cache = Arc::clone(&distributor_cache);
            let sends = Arc::clone(&sends);
            let distributor_stats = Arc::clone(&distributor_stats);
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let cpu_start = thread_cpu_us();
                runtime.block_on(async move {
                    let mut config = CacheMeshConfig::new("distributor", "uk", distributor_addr)
                        .with_peers(child_addrs)
                        .with_role(CacheMeshRole::Distributor)
                        .with_socket_buffer_bytes(4 << 20)
                        .with_announce_interval(Duration::from_millis(100))
                        .with_applied_tx(distributor_applied_tx)
                        .with_send_observer(Arc::new(move |_, slot_id, _, sent_us| {
                            lock(&sends).push((slot_id, sent_us));
                        }));
                    if loaded {
                        config = config
                            .with_replication(ReplicationMode::Demand)
                            .with_subscription_lease(lease);
                    }
                    let mesh = CacheMesh::new(cache, config).start().await.unwrap();
                    ready_tx.send(()).unwrap();
                    let _ = stop_rx.await;
                    *lock(&distributor_stats) = Some(mesh.fec_stats());
                    mesh.shutdown();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                });
                thread_cpu_us() - cpu_start
            })
        };
        ready_rx.recv().unwrap();
        let stream = runtime.block_on(async {
            for edge in &edges {
                wait_for_hello(edge, 1).await;
                if loaded {
                    edge.subscribe(7, 0, 1).await.unwrap();
                }
            }
            // Slot 0 lines the run up: both children receive parts.
            let stream = distributor_cache.resolve_or_create_stream(7).await;
            let part = if loaded {
                load_part(0, stream.generation(), part_bytes)
            } else {
                Bytes::from(vec![0; part_bytes])
            };
            distributor_cache
                .add_for_handle(stream, 0, part)
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
            stream
        });

        // Streams that come and go at the distributor, a thousand a second:
        // with the free list they never take stream 7's lane.
        let churn = loaded.then(|| {
            let cache = Arc::clone(&distributor_cache);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                let mut churned = 0_u64;
                while !stop.load(Ordering::Relaxed) {
                    let stream_id = 1_000 + churned;
                    runtime.block_on(async {
                        let handle = cache.resolve_or_create_stream(stream_id).await;
                        cache
                            .add_for_handle(handle, 0, Bytes::from_static(b"churn"))
                            .await
                            .unwrap();
                        assert!(cache.release_stream_id(stream_id).await);
                    });
                    churned += 1;
                    std::thread::sleep(Duration::from_millis(1));
                }
                churned
            })
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
                for (slot, published_us) in published.iter_mut().enumerate().skip(1) {
                    let deadline = start + period * slot as u32;
                    let now = std::time::Instant::now();
                    if deadline > now + Duration::from_micros(200) {
                        std::thread::sleep(deadline - now - Duration::from_micros(200));
                    }
                    while std::time::Instant::now() < deadline {
                        std::hint::spin_loop();
                    }
                    if loaded {
                        let part = load_part(slot, stream.generation(), part_bytes);
                        *published_us = unix_micros();
                        runtime
                            .block_on(cache.add_for_handle(stream, slot, part))
                            .unwrap();
                    } else {
                        // As a product that names the stream on each write.
                        *published_us = unix_micros();
                        runtime
                            .block_on(cache.add_for_stream_id(7, slot, payload.clone()))
                            .unwrap();
                    }
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
        if loaded {
            // The blocking readers have read up to the last part.
            while std::time::Instant::now() < deadline
                && !checks
                    .blocking_reads
                    .iter()
                    .all(|reads| reads.load(Ordering::Relaxed) as usize >= parts)
            {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        stop.store(true, Ordering::Relaxed);
        let _ = stop_tx.send(());
        let cpu_us = distributor.join().unwrap();
        eprintln!(
            "distributor thread CPU {:.1} us per part ({} parts)",
            cpu_us as f64 / parts as f64,
            parts
        );
        for thread in threads {
            thread.join().unwrap();
        }
        if let Some(churn) = churn {
            let churned = churn.join().unwrap();
            eprintln!("{churned} streams churned through the distributor's lanes");
            assert!(churned > 0);
            assert_eq!(distributor_cache.resolve_stream(7), Some(stream));
        }
        runtime.block_on(async {
            for edge in &edges {
                if loaded {
                    let stats = edge.fec_stats();
                    assert_eq!(stats.rx_unauthorized_datagrams, 0, "{stats:?}");
                }
                edge.shutdown();
            }
        });
        if loaded {
            // The edges sent nothing back to the distributor.
            assert!(distributor_applied_rx.try_recv().is_err());
            let stats = lock(&distributor_stats).expect("the distributor's stats");
            assert_eq!(stats.rx_unauthorized_datagrams, 0, "{stats:?}");
            let polled = checks.polled.load(Ordering::Relaxed);
            assert!(polled > 0);
            assert_eq!(
                checks.mismatches.load(Ordering::Relaxed),
                0,
                "a reader read a part that is not the one it asked for"
            );
            for (edge, reads) in checks.blocking_reads.iter().enumerate() {
                assert_eq!(
                    reads.load(Ordering::Relaxed) as usize,
                    parts,
                    "the blocking reader of edge {edge} missed parts"
                );
            }
            let mut wakes = lock(&checks.blocking_wakes).clone();
            wakes.sort_unstable();
            eprintln!(
                "{polled} polled reads; blocking reader commit->wake p50 {} us, p99 {} us; {} playlist reads",
                percentile(&wakes, 0.5),
                percentile(&wakes, 0.99),
                checks.playlist_reads.load(Ordering::Relaxed)
            );
            assert_eq!(checks.playlist_errors.load(Ordering::Relaxed), 0);
            assert!(checks.playlist_reads.load(Ordering::Relaxed) > 0);
            for (edge_cache, playlist) in edge_caches.iter().zip(&playlists) {
                let edge_stream = edge_cache.resolve_stream(7).unwrap();
                assert_eq!(edge_cache.last_for_handle(edge_stream), Some(parts));
                assert!(playlist.last(7).unwrap().is_some());
            }
        }

        let mut latencies = Vec::with_capacity(2 * parts);
        let mut missing = 0;
        for arrivals in &arrivals {
            let arrivals = lock(arrivals);
            for (slot, published_us) in published.iter().enumerate().skip(1) {
                match arrivals.get(&slot) {
                    Some(at) => latencies.push(at.saturating_sub(*published_us)),
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

    /// The readers of one edge in a loaded run, each on its own thread:
    /// eight that poll the newest part, one that blocks on each part in
    /// turn, and two that read the playlist.
    fn edge_readers(
        edge: usize,
        cache: &Arc<ChunkCache>,
        playlist: &Arc<crate::m3u8_cache::M3u8Cache>,
        commits: &Arc<StdMutex<HashMap<usize, u64>>>,
        checks: &Arc<LoadChecks>,
        stop: &Arc<std::sync::atomic::AtomicBool>,
        parts: usize,
    ) -> Vec<std::thread::JoinHandle<()>> {
        let mut threads = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(cache);
            let checks = Arc::clone(checks);
            let stop = Arc::clone(stop);
            threads.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                let mut generation = None;
                runtime.block_on(async {
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_micros(200));
                        let Some(stream) = cache.resolve_stream(7) else {
                            continue;
                        };
                        let Some(last) = cache.last_for_handle(stream) else {
                            continue;
                        };
                        let Some((part, _)) = cache.get_for_handle(stream, last).await else {
                            continue;
                        };
                        checks.polled.fetch_add(1, Ordering::Relaxed);
                        let slot = u64::from_le_bytes(part[..8].try_into().unwrap());
                        let tag = u64::from_le_bytes(part[8..16].try_into().unwrap());
                        let first = *generation.get_or_insert(tag);
                        if slot != last as u64 || tag != first {
                            checks.mismatches.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }));
        }
        {
            let cache = Arc::clone(cache);
            let checks = Arc::clone(checks);
            let stop = Arc::clone(stop);
            let commits = Arc::clone(commits);
            threads.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let mut next = 1;
                    while next <= parts && !stop.load(Ordering::Relaxed) {
                        let waiter = cache
                            .exact_part_waiter(7, next)
                            .expect("an exact-part waiter");
                        let notified = waiter.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        let read = cache.get_for_stream_id(7, next).await;
                        let Some((part, _)) = read else {
                            // Woken by the part, or polled again in a while
                            // so that a stop is seen.
                            let _ = tokio::time::timeout(Duration::from_millis(50), notified).await;
                            if cache.get_for_stream_id(7, next).await.is_none() {
                                continue;
                            }
                            let woke_us = unix_micros();
                            if let Some(committed_us) = lock(&commits).get(&next).copied() {
                                lock(&checks.blocking_wakes)
                                    .push(woke_us.saturating_sub(committed_us));
                            }
                            checks.blocking_reads[edge].fetch_add(1, Ordering::Relaxed);
                            next += 1;
                            continue;
                        };
                        if u64::from_le_bytes(part[..8].try_into().unwrap()) != next as u64 {
                            checks.mismatches.fetch_add(1, Ordering::Relaxed);
                        }
                        checks.blocking_reads[edge].fetch_add(1, Ordering::Relaxed);
                        next += 1;
                    }
                });
            }));
        }
        for delta in [false, true] {
            let playlist = Arc::clone(playlist);
            let checks = Arc::clone(checks);
            let stop = Arc::clone(stop);
            threads.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_micros(200));
                    let Some(stream) = playlist.resolve_stream(7) else {
                        continue;
                    };
                    let read = || {
                        if delta {
                            playlist.last_delta_for_handle(stream)
                        } else {
                            playlist.last_for_handle(stream)
                        }
                    };
                    let before = playlist.version_for_handle(stream);
                    let (Ok(Some((first, _))), Ok(Some((second, _)))) = (read(), read()) else {
                        continue;
                    };
                    let after = playlist.version_for_handle(stream);
                    checks.playlist_reads.fetch_add(2, Ordering::Relaxed);
                    // Two reads of one version share one rendering.
                    if before.is_some() && before == after && first.as_ptr() != second.as_ptr() {
                        checks.mismatches.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        threads
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

    /// The p99 bound of the throughput runs: 2 ms in an optimised build.
    /// An unoptimised build on a busy host (`cargo test` in CI, a laptop on
    /// battery) misses it on scheduler jitter alone, so there it only
    /// checks that the sender keeps up at all.
    const THROUGHPUT_P99_US: u64 = if cfg!(debug_assertions) {
        20_000
    } else {
        2_000
    };

    /// 4,000 parts a second (0.25 ms PCM parts) for two seconds, from a
    /// distributor on one core to two children over loopback: every part
    /// arrives, and 99 in 100 within 2 ms of the commit (20 ms in an
    /// unoptimised build). The children are sockets, so that the test
    /// measures the sender in any build.
    #[test]
    fn a_distributor_on_one_core_sends_four_thousand_parts_a_second() {
        let (latencies, sent, missing) =
            publish_to_two_children(Children::Sockets, Load::None, 4_000, 2, 72);
        let (_, p99) = report("socket children", &latencies, &sent, missing);
        assert_eq!(missing, 0, "parts did not arrive");
        assert!(p99 < THROUGHPUT_P99_US, "p99 {p99} us");
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
            publish_to_two_children(Children::MeshEdges, Load::None, 4_000, 2, 72);
        let (_, p99) = report("mesh edges", &latencies, &sent, missing);
        assert_eq!(missing, 0, "parts did not arrive");
        assert!(p99 < THROUGHPUT_P99_US, "p99 {p99} us");
    }

    /// Two mesh edges at 4,000 parts a second as a product runs them:
    /// streams come and go through the distributor's lanes, and each edge's
    /// cache has readers that poll, a reader that blocks on every part, and
    /// a playlist with readers. Every part arrives, 99 in 100 within the
    /// bound, every reader reads the part it asked for, and nothing goes
    /// back to the distributor. The receive path of an unoptimised build
    /// takes about 1,000 datagrams a second (see above), so there the run
    /// is at 250 parts a second.
    #[test]
    fn a_distributor_and_its_edges_keep_up_while_caches_churn_and_readers_block() {
        let rate = if cfg!(debug_assertions) { 250 } else { 4_000 };
        let (latencies, sent, missing) =
            publish_to_two_children(Children::MeshEdges, Load::CachesInUse, rate, 2, 72);
        let (_, p99) = report("mesh edges, caches in use", &latencies, &sent, missing);
        assert_eq!(missing, 0, "parts did not arrive");
        assert!(p99 < THROUGHPUT_P99_US, "p99 {p99} us");
    }

    /// A pass hands a child's datagrams to the kernel in calls of up to 64.
    #[test]
    fn a_batched_pass_makes_one_call_for_many_datagrams() {
        {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let child_addrs = [loopback_addr(), loopback_addr()];
            let children = child_addrs.map(|addr| raw_child(addr, Arc::clone(&stop)));
            let cache = Arc::new(ChunkCache::new(Options {
                num_playlists: 1,
                max_segments: 1,
                max_parts_per_segment: 128,
                ..Options::default()
            }));
            let stats = runtime.block_on(async {
                // Sixty parts are there before the first pass: two
                // datagrams each (a source and a repair symbol) for each
                // child.
                for slot in 0..60 {
                    cache
                        .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 72]))
                        .await
                        .unwrap();
                }
                let distributor = CacheMesh::new(
                    Arc::clone(&cache),
                    CacheMeshConfig::new("distributor", "uk", loopback_addr())
                        .with_peers(child_addrs)
                        .with_role(CacheMeshRole::Distributor)
                        .with_announce_interval(Duration::from_secs(10)),
                )
                .start()
                .await
                .unwrap();
                timeout(Duration::from_secs(3), async {
                    while !children
                        .iter()
                        .all(|(arrivals, _)| lock(arrivals).len() >= 60)
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("the children did not receive every part");
                // A part is counted when its child's call has returned, which
                // can be after the child has it: wait for the counts.
                let _ = timeout(Duration::from_secs(1), async {
                    loop {
                        let stats = distributor.fec_stats();
                        if stats.tx_source_datagrams + stats.tx_repair_datagrams >= 240 {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await;
                let stats = distributor.fec_stats();
                distributor.shutdown();
                stats
            });
            stop.store(true, Ordering::Relaxed);
            for (_, thread) in children {
                thread.join().unwrap();
            }
            let datagrams = stats.tx_source_datagrams + stats.tx_repair_datagrams;
            assert!(datagrams >= 240, "{stats:?}");
            eprintln!("{} calls for {datagrams} datagrams", stats.tx_send_calls);
            // 120 datagrams for each child in two calls each, and the few
            // hellos of the start, one call per datagram.
            assert!(stats.tx_send_calls * 8 <= datagrams, "{stats:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_gso_a_large_part_keeps_symbols_that_fit_a_packet() {
        // Slots that hold the 200 kB part.
        let options = Options {
            buffer_size_kb: 256,
            ..large_part_options()
        };
        for gso in [true, false] {
            let distributor_addr = loopback_addr();
            let edge_addr = loopback_addr();
            let distributor_cache = Arc::new(ChunkCache::new(options));
            let mut config = CacheMeshConfig::new("distributor", "uk", distributor_addr)
                .with_peer(edge_addr)
                .with_role(CacheMeshRole::Distributor)
                .with_max_symbol_size(8_100)
                .with_announce_interval(Duration::from_millis(100));
            if !gso {
                config = config.without_gso();
            }
            let distributor = CacheMesh::new(Arc::clone(&distributor_cache), config)
                .start()
                .await
                .unwrap();
            let edge_cache = Arc::new(ChunkCache::new(options));
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
        );
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_audio_part_goes_in_datagrams_of_its_own_size() {
        {
            let distributor_addr = loopback_addr();
            let edge_addr = loopback_addr();
            let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
            let distributor = CacheMesh::new(
                Arc::clone(&distributor_cache),
                CacheMeshConfig::new("distributor", "uk", distributor_addr)
                    .with_peer(edge_addr)
                    .with_role(CacheMeshRole::Distributor)
                    .with_announce_interval(Duration::from_millis(100)),
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
            assert_eq!(bytes as usize, 36 + frame);
            distributor.shutdown();
            edge.shutdown();
        }
    }

    /// An address whose port and the port `offset` above it are both free.
    fn class_split_addr(offset: u16) -> SocketAddr {
        loop {
            let addr = loopback_addr();
            let class = SocketAddr::from(([127, 0, 0, 1], addr.port() + offset));
            if std::net::UdpSocket::bind(class).is_ok() {
                return addr;
            }
        }
    }

    /// Video for the given streams, audio for every other.
    fn video_streams(video: &'static [u64]) -> CacheMeshStreamClassifier {
        Arc::new(move |stream_id| {
            if video.contains(&stream_id) {
                StreamClass::Video
            } else {
                StreamClass::Audio
            }
        })
    }

    /// Records the stream and slot of every chunk that a receive task
    /// decodes.
    #[derive(Default)]
    struct DecodedChunks(StdMutex<Vec<(u64, u64)>>);

    impl CacheMeshReceiveObserver for DecodedChunks {
        fn datagram(&self, _peer: SocketAddr, _datagram: &[u8]) {}

        fn decoded(&self, _peer: SocketAddr, _datagram: &[u8], frame: &[u8]) {
            if let Some(slot) = chunk_frame_slot(frame) {
                lock(&self.0).push(slot);
            }
        }
    }

    const TEST_CLASS_OFFSET: u16 = 7_000;

    #[test]
    fn a_mesh_is_split_by_class_only_when_asked() {
        let addr = SocketAddr::from(([10, 0, 0, 1], 29_101));
        let config = CacheMeshConfig::new("node", "uk", addr);
        assert_eq!(config.class_port_offset, 0);
        assert_eq!(config.stream_class(9), StreamClass::Audio);
        let config = config
            .with_class_port_offset(1_000)
            .with_stream_classifier(video_streams(&[9]));
        assert_eq!(config.class_port_offset, 1_000);
        assert_eq!(config.stream_class(9), StreamClass::Video);
        assert_eq!(config.stream_class(7), StreamClass::Audio);
        assert!(format!("{config:?}").contains("stream_classifier: Some(..)"));

        // The one lane of a mesh that is not split carries everything.
        assert!(!Lane::ALL.split());
        assert!(Lane::ALL.carries(&config, 9) && Lane::ALL.carries(&config, 7));
        assert_eq!(Lane::ALL.to(addr), addr);
        let audio = Lane {
            class: Some(StreamClass::Audio),
            port_offset: 0,
        };
        let video = Lane {
            class: Some(StreamClass::Video),
            port_offset: 1_000,
        };
        assert!(audio.carries(&config, 7) && !audio.carries(&config, 9));
        assert!(video.carries(&config, 9) && !video.carries(&config, 7));
        // A peer's class socket is its main port plus the offset, and a
        // datagram from there is the peer's.
        let class_addr = SocketAddr::from(([10, 0, 0, 1], 30_101));
        assert_eq!(video.peer_socket(addr), Some(class_addr));
        assert_eq!(video.to(addr), class_addr);
        assert_eq!(video.peer_of(class_addr), Some(addr));
        assert_eq!(audio.to(addr), addr);
        let high = SocketAddr::from(([10, 0, 0, 1], 65_000));
        assert_eq!(video.peer_socket(high), None);
        assert_eq!(video.peer_of(SocketAddr::from(([10, 0, 0, 1], 999))), None);
    }

    #[tokio::test]
    async fn a_class_socket_beyond_the_last_port_is_refused() {
        let cache = Arc::new(ChunkCache::new(demand_options()));
        let config =
            CacheMeshConfig::new("node", "uk", loopback_addr()).with_class_port_offset(60_000);
        let err = CacheMesh::new(cache, config)
            .start()
            .await
            .err()
            .expect("refused");
        assert!(err.to_string().contains("beyond the last port"), "{err}");
    }

    /// A video playlist's new lease is warmed with at most
    /// `video_backfill_parts` parts, newest first; an audio playlist's with
    /// the window its child asks for; without the bound, video as audio.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_video_warm_up_window_is_bounded_on_its_own() {
        for bound in [Some(2), None] {
            let parent_addr = class_split_addr(TEST_CLASS_OFFSET);
            let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
            let parent_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            let parent = CacheMesh::new(
                Arc::clone(&parent_cache),
                demand_distributor("parent", parent_addr)
                    .with_peer(edge_addr)
                    .with_class_port_offset(TEST_CLASS_OFFSET)
                    .with_stream_classifier(video_streams(&[9]))
                    .with_video_backfill_parts(bound),
            )
            .start()
            .await
            .unwrap();
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                demand_edge("edge", edge_addr)
                    .with_peer(parent_addr)
                    .with_class_port_offset(TEST_CLASS_OFFSET)
                    .with_stream_classifier(video_streams(&[9])),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            for slot in 0..20 {
                for stream in [7, 9] {
                    parent_cache
                        .add_for_stream_id(stream, slot, Bytes::from(vec![slot as u8; 1_000]))
                        .await
                        .unwrap();
                }
            }
            edge.subscribe(7, 0, 8).await.unwrap();
            edge.subscribe(9, 0, 8).await.unwrap();
            wait_for_slot(&edge_cache, 7, 12).await;
            wait_for_slot(&edge_cache, 9, 18).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let mut held = [Vec::new(), Vec::new()];
            for (index, stream) in [7_u64, 9].into_iter().enumerate() {
                for slot in 0..20 {
                    if edge_cache.get_for_stream_id(stream, slot).await.is_some() {
                        held[index].push(slot);
                    }
                }
            }
            assert_eq!(held[0], (12..20).collect::<Vec<_>>(), "audio, bound {bound:?}");
            let video: Vec<usize> = match bound {
                Some(parts) => (20 - parts..20).collect(),
                None => (12..20).collect(),
            };
            assert_eq!(held[1], video, "video, bound {bound:?}");
            parent.shutdown();
            edge.shutdown();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_class_split_mesh_sends_video_parts_on_the_class_socket() {
        let distributor_addr = class_split_addr(TEST_CLASS_OFFSET);
        let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
        let distributor_cache = Arc::new(ChunkCache::new(large_part_options()));
        let edge_cache = Arc::new(ChunkCache::new(large_part_options()));
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
                .with_role(CacheMeshRole::Distributor)
                .with_announce_interval(Duration::from_millis(100))
                .with_socket_buffer_bytes(4 << 20)
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9]))
                .with_send_observer(observer),
        )
        .start()
        .await
        .unwrap();
        assert_eq!(
            distributor.class_local_addr().map(|addr| addr.port()),
            Some(distributor_addr.port() + TEST_CLASS_OFFSET)
        );
        let main_chunks = Arc::new(DecodedChunks::default());
        let class_chunks = Arc::new(DecodedChunks::default());
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(100))
                .with_socket_buffer_bytes(4 << 20)
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9]))
                .with_receive_observer(Arc::clone(&main_chunks) as _)
                .with_class_receive_observer(Some(Arc::clone(&class_chunks) as _))
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;

        let init = Bytes::from_static(b"ftyp-moov-of-the-video");
        distributor_cache
            .set_stream_initialization(9, init.clone())
            .await
            .unwrap();
        for slot in 0..6 {
            distributor_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 120]))
                .await
                .unwrap();
            distributor_cache
                .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 20_000]))
                .await
                .unwrap();
        }
        let mut applied = Vec::new();
        timeout(Duration::from_secs(3), async {
            while applied.len() < 12 {
                applied.push(applied_rx.recv().await.unwrap());
            }
        })
        .await
        .expect("every part reached the edge");
        for slot in 0..6 {
            assert_eq!(
                edge_cache.get_for_stream_id(9, slot).await.unwrap().0,
                Bytes::from(vec![slot as u8; 20_000])
            );
        }
        assert_eq!(edge_cache.stream_initialization(9), Some(init));
        // Parts of both classes come from the parent's main address.
        assert!(applied.iter().all(|event| event.parent == distributor_addr));

        // The video parts and the initialization went on the class socket
        // and nothing else did; the audio parts on the main socket.
        let class_sent = distributor.class_fec_stats().unwrap();
        assert_eq!(class_sent.tx_objects, 7, "{class_sent:?}");
        let class_received = edge.class_fec_stats().unwrap();
        assert_eq!(class_received.rx_decoded_objects, 7, "{class_received:?}");
        assert!(edge.fec_stats().rx_decoded_objects > class_received.rx_decoded_objects);
        let mut main = lock(&main_chunks.0).clone();
        main.sort_unstable();
        assert_eq!(main, (0..6).map(|slot| (7, slot)).collect::<Vec<_>>());
        let mut class = lock(&class_chunks.0).clone();
        class.sort_unstable();
        assert_eq!(class, (0..6).map(|slot| (9, slot)).collect::<Vec<_>>());

        // The send observer names the child by its main address.
        timeout(Duration::from_secs(1), async {
            while lock(&sent).len() < 12 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the send observer saw every part");
        assert!(lock(&sent)
            .iter()
            .all(|(_, _, child, _)| *child == edge_addr));
        distributor.shutdown();
        edge.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_video_replica_request_is_served_from_the_class_socket() {
        let distributor_addr = class_split_addr(TEST_CLASS_OFFSET);
        let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
        let distributor_cache = Arc::new(ChunkCache::new(large_part_options()));
        let edge_cache = Arc::new(ChunkCache::new(large_part_options()));
        // Demand replication without a lease: the distributor pushes
        // nothing, and sends only what it is asked for.
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            demand_distributor("distributor", distributor_addr)
                .with_peer(edge_addr)
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9])),
        )
        .start()
        .await
        .unwrap();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_same_region_only(true)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(100))
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9])),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        wait_for_hello(&distributor, 1).await;
        for slot in 0..4 {
            distributor_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 100]))
                .await
                .unwrap();
            distributor_cache
                .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 10_000]))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(edge_cache.resolve_stream(9).is_none());

        assert_eq!(edge.request_parts(9, 1, 2, 1).await.unwrap(), 1);
        wait_for_slot(&edge_cache, 9, 1).await;
        wait_for_slot(&edge_cache, 9, 2).await;
        assert!(edge_cache.get_for_stream_id(9, 3).await.is_none());
        assert_eq!(edge.request_parts(7, 0, 2, 1).await.unwrap(), 1);
        wait_for_slot(&edge_cache, 7, 0).await;
        wait_for_slot(&edge_cache, 7, 1).await;
        // The request went on the main socket; the video parts came back on
        // the class socket, and the audio parts did not.
        assert_eq!(distributor.class_fec_stats().unwrap().tx_objects, 2);
        assert_eq!(edge.class_fec_stats().unwrap().rx_decoded_objects, 2);
        distributor.shutdown();
        edge.shutdown();
    }

    /// A distributor holds a window of 200 video parts when an edge appears,
    /// and sends it the window in one pass. While that runs, an audio part
    /// is published. Returns how long the audio part took to reach the edge
    /// and how many video parts the distributor had encoded by then.
    async fn audio_part_during_a_video_burst(
        split: bool,
        runtime: ClassLaneRuntime,
    ) -> (Duration, u64) {
        const VIDEO_PARTS: usize = 200;
        let offset = if split { TEST_CLASS_OFFSET } else { 0 };
        let distributor_addr = class_split_addr(TEST_CLASS_OFFSET);
        let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
        let distributor_cache = Arc::new(ChunkCache::new(large_part_options()));
        let edge_cache = Arc::new(ChunkCache::new(large_part_options()));
        for slot in 0..VIDEO_PARTS {
            distributor_cache
                .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 12_000]))
                .await
                .unwrap();
        }
        distributor_cache
            .add_for_stream_id(7, 0, Bytes::from(vec![0u8; 100]))
            .await
            .unwrap();
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            CacheMeshConfig::new("distributor", "uk", distributor_addr)
                // The edge is learned from its hello.
                .with_allowed_peers([edge_addr])
                .with_role(CacheMeshRole::Distributor)
                .with_announce_interval(Duration::from_millis(50))
                .with_sync_interval(Duration::from_millis(10))
                .with_socket_buffer_bytes(4 << 20)
                .with_class_port_offset(offset)
                .with_class_runtime(runtime.clone())
                .with_stream_classifier(video_streams(&[9])),
        )
        .start()
        .await
        .unwrap();
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(50))
                .with_socket_buffer_bytes(4 << 20)
                .with_class_port_offset(offset)
                .with_class_runtime(runtime)
                .with_stream_classifier(video_streams(&[9]))
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        // The video parts encoded so far: on the class socket when the mesh
        // is split, otherwise among every object of the one socket.
        let video_encoded = || match distributor.class_fec_stats() {
            Some(class) => class.tx_objects,
            None => distributor.fec_stats().tx_objects,
        };
        // The distributor hears the edge and sends it the window.
        timeout(Duration::from_secs(5), async {
            while video_encoded() < 20 {
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
        })
        .await
        .expect("the video window did not start");
        let published = Instant::now();
        distributor_cache
            .add_for_stream_id(7, 1, Bytes::from(vec![1u8; 100]))
            .await
            .unwrap();
        let encoded = timeout(Duration::from_secs(10), async {
            loop {
                let event: AppliedSlot = applied_rx.recv().await.unwrap();
                if (event.stream_id, event.slot_id) == (7, 1) {
                    return video_encoded();
                }
            }
        })
        .await
        .expect("the audio part did not reach the edge");
        let latency = published.elapsed();
        distributor.shutdown();
        edge.shutdown();
        eprintln!(
            "audio part during a video burst: split {split}: {} us, after {encoded} video parts",
            latency.as_micros()
        );
        (latency, encoded)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_video_burst_does_not_hold_back_an_audio_part_on_a_split_mesh() {
        let (latency, encoded) =
            audio_part_during_a_video_burst(true, ClassLaneRuntime::OwnThread).await;
        assert!(
            encoded < 200,
            "the audio part waited for the video window: {encoded} video parts first, {latency:?}"
        );
        assert!(latency < Duration::from_millis(10), "{latency:?}");

        // On one socket the audio part leaves after the whole window.
        let (latency, encoded) =
            audio_part_during_a_video_burst(false, ClassLaneRuntime::OwnThread).await;
        assert!(encoded >= 200, "{encoded} video parts first, {latency:?}");
    }

    /// On a runtime of one thread, as av-mesh's media runtime is: the class
    /// lane on a thread of its own keeps the audio part clear of the video
    /// window. (With the class lane on the caller's runtime, as `fe8318c` had
    /// it, the same run measured the audio part after all 200 video parts,
    /// about 31 ms in a debug build.)
    #[tokio::test(flavor = "current_thread")]
    async fn a_video_burst_does_not_hold_back_an_audio_part_on_one_runtime_thread() {
        let (latency, encoded) =
            audio_part_during_a_video_burst(true, ClassLaneRuntime::OwnThread).await;
        assert!(
            encoded < 200,
            "the audio part waited for the video window: {encoded} video parts first, {latency:?}"
        );
        assert!(latency < Duration::from_millis(10), "{latency:?}");
    }

    /// On two workers, as on a 2-CPU node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_video_burst_does_not_hold_back_an_audio_part_on_two_workers() {
        let (latency, encoded) =
            audio_part_during_a_video_burst(true, ClassLaneRuntime::OwnThread).await;
        assert!(
            encoded < 200,
            "the audio part waited for the video window: {encoded} video parts first, {latency:?}"
        );
        assert!(latency < Duration::from_millis(10), "{latency:?}");
    }

    /// An edge receives a 200-part video window from one distributor while
    /// another publishes audio parts, both distributors on a runtime of their
    /// own so that only the edge's receiving is shared. Returns, for each
    /// audio part, how long it took from its publication to its application
    /// at the edge, and the time from its first datagram to its commit.
    async fn audio_parts_while_an_edge_decodes_a_video_window(split: bool) -> Vec<(Duration, u64)> {
        const VIDEO_PARTS: usize = 200;
        const AUDIO_PARTS: usize = 10;
        let offset = if split { TEST_CLASS_OFFSET } else { 0 };
        let senders = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let video_addr = class_split_addr(TEST_CLASS_OFFSET);
        let audio_addr = class_split_addr(TEST_CLASS_OFFSET);
        let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
        let video_cache = Arc::new(ChunkCache::new(large_part_options()));
        let audio_cache = Arc::new(ChunkCache::new(large_part_options()));
        let edge_cache = Arc::new(ChunkCache::new(large_part_options()));
        for slot in 0..VIDEO_PARTS {
            video_cache
                .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 12_000]))
                .await
                .unwrap();
        }
        let distributor = |name: &'static str, addr, cache: &Arc<ChunkCache>| {
            let mesh = CacheMesh::new(
                Arc::clone(cache),
                CacheMeshConfig::new(name, "uk", addr)
                    // The edge is learned from its hello.
                    .with_allowed_peers([edge_addr])
                    .with_role(CacheMeshRole::Distributor)
                    .with_announce_interval(Duration::from_millis(50))
                    .with_sync_interval(Duration::from_millis(10))
                    .with_socket_buffer_bytes(8 << 20)
                    .with_class_port_offset(offset)
                    .with_stream_classifier(video_streams(&[9])),
            );
            senders.spawn(mesh.start())
        };
        let audio = distributor("audio", audio_addr, &audio_cache)
            .await
            .unwrap()
            .unwrap();
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(video_addr)
                .with_peer(audio_addr)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(50))
                .with_socket_buffer_bytes(8 << 20)
                .with_class_port_offset(offset)
                .with_stream_classifier(video_streams(&[9]))
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        // The audio distributor knows the edge before the window starts.
        audio_cache
            .add_for_stream_id(7, 0, Bytes::from(vec![0u8; 100]))
            .await
            .unwrap();
        wait_for_slot(&edge_cache, 7, 0).await;
        // The video distributor starts only now. Started with the audio one,
        // it learned the edge from the same hello, and when its window
        // reached the edge first, part 0 above waited behind all of it: the
        // window was over before the audio parts below were published, and
        // on one socket they did not wait (about 4 runs in 10 of the full
        // suite, after the merge with the August line).
        let video = distributor("video", video_addr, &video_cache)
            .await
            .unwrap()
            .unwrap();
        let decoded_before_window = edge.fec_stats().rx_decoded_objects;
        timeout(Duration::from_secs(5), async {
            while edge.fec_stats().rx_decoded_objects < decoded_before_window + 20 {
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
        })
        .await
        .expect("the video window did not start");
        let mut audio_parts = Vec::new();
        for slot in 1..=AUDIO_PARTS {
            let published = Instant::now();
            audio_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 100]))
                .await
                .unwrap();
            let event = timeout(Duration::from_secs(10), async {
                loop {
                    let event: AppliedSlot = applied_rx.recv().await.unwrap();
                    if (event.stream_id, event.slot_id) == (7, slot) {
                        return event;
                    }
                }
            })
            .await
            .expect("the audio part did not reach the edge");
            audio_parts.push((
                published.elapsed(),
                event.committed_us.saturating_sub(event.rx_first_us),
            ));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        video.shutdown();
        audio.shutdown();
        edge.shutdown();
        senders.shutdown_background();
        let mut latencies = audio_parts
            .iter()
            .map(|(latency, _)| latency.as_micros() as u64)
            .collect::<Vec<_>>();
        latencies.sort_unstable();
        eprintln!(
            "audio parts while an edge decodes a video window: split {split}: publish->applied p50 {} us, max {} us; first datagram->commit max {} us",
            latencies[latencies.len() / 2],
            latencies[latencies.len() - 1],
            audio_parts.iter().map(|(_, rx)| *rx).max().unwrap_or(0)
        );
        audio_parts
    }

    /// Check the audio parts of a split edge against those of an edge with
    /// one socket.
    fn assert_audio_parts_kept_clear(split: &[(Duration, u64)], one: &[(Duration, u64)]) {
        let split_max = split.iter().map(|(latency, _)| *latency).max().unwrap();
        let one_max = one.iter().map(|(latency, _)| *latency).max().unwrap();
        // Split, every audio part is applied at once, and its first datagram
        // to its commit takes microseconds.
        assert!(split_max < Duration::from_millis(10), "split {split:?}");
        assert!(
            split
                .iter()
                .all(|(_, rx_to_commit_us)| *rx_to_commit_us < 5_000),
            "split {split:?}"
        );
        // On one socket the first of them waits behind the window's
        // datagrams, which the one receive task decodes first.
        assert!(
            one_max >= Duration::from_millis(10) && one_max > split_max,
            "split {split:?}, one socket {one:?}"
        );
    }

    /// The edge's side of the split: its video receive task decodes the
    /// window on its own socket and thread, and the audio parts on the main
    /// socket are applied as they come, on a runtime of one thread.
    #[tokio::test(flavor = "current_thread")]
    async fn an_edge_applies_audio_parts_while_it_decodes_a_video_window() {
        let split = audio_parts_while_an_edge_decodes_a_video_window(true).await;
        let one = audio_parts_while_an_edge_decodes_a_video_window(false).await;
        assert_audio_parts_kept_clear(&split, &one);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_edge_applies_audio_parts_while_it_decodes_a_video_window_on_two_workers() {
        let split = audio_parts_while_an_edge_decodes_a_video_window(true).await;
        let one = audio_parts_while_an_edge_decodes_a_video_window(false).await;
        assert_audio_parts_kept_clear(&split, &one);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_playlist_that_becomes_video_moves_to_the_class_socket_from_the_next_part() {
        for replication in [ReplicationMode::All, ReplicationMode::Demand] {
            let distributor_addr = class_split_addr(TEST_CLASS_OFFSET);
            let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
            let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            // Stream 9 is audio until its first video, as a muxed stream is.
            let is_video = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let classifier: CacheMeshStreamClassifier = {
                let is_video = Arc::clone(&is_video);
                Arc::new(move |stream_id| {
                    if stream_id == 9 && is_video.load(std::sync::atomic::Ordering::Acquire) {
                        StreamClass::Video
                    } else {
                        StreamClass::Audio
                    }
                })
            };
            let distributor = CacheMesh::new(
                Arc::clone(&distributor_cache),
                demand_distributor("distributor", distributor_addr)
                    .with_peer(edge_addr)
                    .with_replication(replication)
                    .with_class_port_offset(TEST_CLASS_OFFSET)
                    .with_stream_classifier(Arc::clone(&classifier)),
            )
            .start()
            .await
            .unwrap();
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                demand_edge("edge", edge_addr)
                    .with_peer(distributor_addr)
                    .with_replication(replication)
                    .with_class_port_offset(TEST_CLASS_OFFSET)
                    .with_stream_classifier(classifier),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            wait_for_hello(&distributor, 1).await;
            if replication == ReplicationMode::Demand {
                edge.subscribe(9, 0, 16).await.unwrap();
                timeout(Duration::from_secs(2), async {
                    while distributor.served_subscriptions().is_empty() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .expect("the lease did not open");
            }
            for slot in 0..5 {
                distributor_cache
                    .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 200]))
                    .await
                    .unwrap();
                wait_for_slot(&edge_cache, 9, slot).await;
            }
            assert_eq!(distributor.class_fec_stats().unwrap().tx_objects, 0);

            is_video.store(true, std::sync::atomic::Ordering::Release);
            // Let a tick of each lane see the change, then publish more.
            tokio::time::sleep(Duration::from_millis(30)).await;
            for slot in 5..8 {
                distributor_cache
                    .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 200]))
                    .await
                    .unwrap();
                wait_for_slot(&edge_cache, 9, slot).await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            // Only the new parts went on the class socket: the window that
            // the edge had was not sent again.
            assert_eq!(
                distributor.class_fec_stats().unwrap().tx_objects,
                3,
                "{replication:?}"
            );
            assert_eq!(edge.class_fec_stats().unwrap().rx_decoded_objects, 3);
            if replication == ReplicationMode::Demand {
                // The lease is listed once, by the lane of its class now.
                let served = distributor.served_subscriptions();
                assert_eq!(served.len(), 1, "{served:?}");
                assert_eq!((served[0].child, served[0].stream_id), (edge_addr, 9));
            }
            distributor.shutdown();
            edge.shutdown();
        }
    }

    #[tokio::test]
    async fn a_playlist_changes_lane_only_when_the_lane_that_it_leaves_hands_it_over() {
        use StreamClass::{Audio, Video};
        let child = SocketAddr::from(([127, 0, 0, 1], 9_000));
        let newcomer = SocketAddr::from(([127, 0, 0, 1], 9_001));
        let handover = LaneHandover::default();
        // Each playlist's lifetime, as the sync tasks name it.
        let cache = ChunkCache::new(demand_options());
        let key = cache.resolve_or_create_stream(9).await;
        let other = cache.resolve_or_create_stream(7).await;
        let fresh = cache.resolve_or_create_stream(5).await;
        let mut audio_owns = HashSet::new();
        let mut audio = LaneChildren::default();
        let mut video_owns = HashSet::new();
        let mut video = LaneChildren::default();
        let woken = |class| {
            let handover = &handover;
            async move {
                timeout(Duration::from_millis(20), handover.wake(class).notified())
                    .await
                    .is_ok()
            }
        };

        // The playlist is audio: the audio lane takes it, the video lane
        // leaves it alone.
        let visit = |class, wants, owns: &mut HashSet<_>, children: &mut LaneChildren| {
            visit_stream(class, wants, key, owns, children, &handover, None)
        };
        assert_eq!(
            visit(Audio, true, &mut audio_owns, &mut audio),
            LaneVisit::Send
        );
        assert_eq!(
            visit(Video, false, &mut video_owns, &mut video),
            LaneVisit::Skip
        );
        // The audio lane sent the child parts up to 10, and part 11 did not
        // all leave (EAGAIN, or a partial batch), so its cursor stays at 10.
        // A newcomer's lease window is not started yet, and the child has
        // a warm-up window under way.
        audio.sent.insert((key, child), 10);
        audio.sent.insert((other, child), 3);
        audio
            .sent_initializations
            .insert((key, child), Bytes::from_static(b"init"));
        audio.backfills.insert(
            (child, 9),
            Backfill {
                low: 2,
                next: Some(5),
            },
        );
        audio.pending_open.insert((newcomer, 9), (0, 4));

        // It becomes video, and the video lane looks first: well past part
        // 11, since it follows the watermark too. It waits for the audio
        // lane, and takes no cursor of its own.
        assert_eq!(
            visit(Video, true, &mut video_owns, &mut video),
            LaneVisit::Wait
        );
        assert_eq!(
            visit(Video, true, &mut video_owns, &mut video),
            LaneVisit::Wait
        );
        assert!(video.sent.is_empty());
        assert!(!woken(Video).await);

        // The audio lane hands everything over, and wakes the video lane.
        assert_eq!(
            visit(Audio, false, &mut audio_owns, &mut audio),
            LaneVisit::Skip
        );
        assert_eq!(audio.sent, HashMap::from([((other, child), 3)]));
        assert!(audio.sent_initializations.is_empty());
        assert!(audio.backfills.is_empty() && audio.pending_open.is_empty());
        assert!(woken(Video).await);
        assert_eq!(
            visit(Audio, false, &mut audio_owns, &mut audio),
            LaneVisit::Skip
        );

        // The video lane goes on from the audio lane's cursor, so part 11
        // is sent, with the window and the lease of the newcomer.
        assert_eq!(
            visit(Video, true, &mut video_owns, &mut video),
            LaneVisit::Send
        );
        assert_eq!(video.sent, HashMap::from([((key, child), 10)]));
        assert_eq!(
            video.sent_initializations.get(&(key, child)),
            Some(&Bytes::from_static(b"init"))
        );
        assert_eq!(
            video.backfills.get(&(child, 9)).map(Backfill::pending),
            Some(4)
        );
        assert_eq!(video.pending_open.get(&(newcomer, 9)), Some(&(0, 4)));
        assert_eq!(
            visit(Video, true, &mut video_owns, &mut video),
            LaneVisit::Send
        );

        // Back to audio, and again before the audio lane looks: whichever
        // lane wants the playlist when it looks takes the state, so neither
        // waits for the other for ever.
        assert_eq!(
            visit(Video, false, &mut video_owns, &mut video),
            LaneVisit::Skip
        );
        assert_eq!(
            visit(Video, true, &mut video_owns, &mut video),
            LaneVisit::Send
        );
        assert_eq!(video.sent.get(&(key, child)), Some(&10));

        // A playlist that no lane has sent goes to the lane of its class.
        assert_eq!(
            visit_stream(
                Audio,
                true,
                fresh,
                &mut audio_owns,
                &mut audio,
                &handover,
                None
            ),
            LaneVisit::Send
        );
        assert!(audio_owns.contains(&fresh));

        // In demand replication only the children that still hold a lease
        // are taken over, and a lease that ended meanwhile is forgotten.
        assert_eq!(
            visit(Video, false, &mut video_owns, &mut video),
            LaneVisit::Skip
        );
        forget_child_in_transit(&handover, 9, &newcomer);
        let leases: LeaseTable = Arc::default();
        assert_eq!(
            visit_stream(
                Audio,
                true,
                key,
                &mut audio_owns,
                &mut audio,
                &handover,
                Some(&leases)
            ),
            LaneVisit::Send
        );
        assert!(!audio.sent.contains_key(&(key, child)));
        assert!(audio.pending_open.is_empty());

        // A released playlist is forgotten by both lanes and the table.
        forget_released_streams(
            &mut audio_owns,
            &mut HashSet::new(),
            &handover,
            true,
            &[fresh],
        );
        assert_eq!(audio_owns, HashSet::from([fresh]));
        assert_eq!(lock(&handover.owners).len(), 1);
    }

    #[test]
    fn replica_requests_that_another_covers_are_served_once() {
        let peer = SocketAddr::from(([127, 0, 0, 1], 9_000));
        let other = SocketAddr::from(([127, 0, 0, 1], 9_001));
        let job = |peer, stream_id, from_slot, count| ReplicaJob {
            peer,
            stream_id,
            from_slot,
            count,
        };
        let batch = vec![
            job(peer, 9, 40, 1),
            job(peer, 9, 40, 1),
            job(other, 9, 40, 1),
            job(peer, 9, 41, 2),
            job(peer, 9, 30, 0),
            job(peer, 9, 50, 0),
            job(peer, 7, 50, 0),
        ];
        assert_eq!(
            coalesce_replica_batch(batch),
            vec![
                job(peer, 9, 30, 0),
                job(other, 9, 40, 1),
                job(peer, 7, 50, 0)
            ]
        );
    }

    #[tokio::test]
    async fn a_full_class_replica_queue_drops_and_counts_requests() {
        let (tx, mut rx) = mpsc::channel(2);
        let replicas = ClassReplicas {
            tx,
            dropped: Arc::default(),
        };
        let peer = SocketAddr::from(([127, 0, 0, 1], 9_000));
        for from_slot in 0..5 {
            replicas.offer(ReplicaJob {
                peer,
                stream_id: 9,
                from_slot,
                count: 1,
            });
        }
        assert_eq!(replicas.dropped.load(Ordering::Relaxed), 3);
        let first = rx.recv().await.unwrap();
        let batch = coalesce_replica_jobs(first, &mut rx);
        assert_eq!(
            batch.iter().map(|job| job.from_slot).collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    /// The `SO_PRIORITY` of a socket.
    #[cfg(target_os = "linux")]
    fn socket_priority(socket: &UdpSocket) -> i32 {
        use std::os::fd::AsRawFd;
        let mut value: libc::c_int = -1;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: the fd is open, and value and len are valid for the call.
        let rc = unsafe {
            libc::getsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PRIORITY,
                (&mut value as *mut libc::c_int).cast(),
                &mut len,
            )
        };
        assert_eq!(rc, 0);
        value
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_audio_socket_of_a_split_mesh_has_the_audio_priority() {
        let cache = Arc::new(ChunkCache::new(demand_options()));
        let split = CacheMesh::new(
            Arc::clone(&cache),
            CacheMeshConfig::new("split", "uk", class_split_addr(TEST_CLASS_OFFSET))
                .with_class_port_offset(TEST_CLASS_OFFSET),
        )
        .start()
        .await
        .unwrap();
        assert_eq!(socket_priority(&split.socket), AUDIO_SOCKET_PRIORITY as i32);
        split.shutdown();
        let chosen = CacheMesh::new(
            Arc::clone(&cache),
            CacheMeshConfig::new("chosen", "uk", class_split_addr(TEST_CLASS_OFFSET))
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_main_socket_priority(Some(4)),
        )
        .start()
        .await
        .unwrap();
        assert_eq!(socket_priority(&chosen.socket), 4);
        chosen.shutdown();
        // A mesh that is not split leaves it alone.
        let one = CacheMesh::new(cache, CacheMeshConfig::new("one", "uk", loopback_addr()))
            .start()
            .await
            .unwrap();
        assert_eq!(socket_priority(&one.socket), 0);
        one.shutdown();
        // The class socket gets one only when asked.
        let config = CacheMeshConfig::new("class", "uk", loopback_addr());
        let class = bind_socket(&config, loopback_addr(), config.class_socket_priority).unwrap();
        assert_eq!(socket_priority(&class), 0);
        let class = bind_socket(&config, loopback_addr(), Some(2)).unwrap();
        assert_eq!(socket_priority(&class), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_class_lane_has_its_own_applied_channel_and_no_observer_by_default() {
        let distributor_addr = class_split_addr(TEST_CLASS_OFFSET);
        let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
        let distributor_cache = Arc::new(ChunkCache::new(large_part_options()));
        let edge_cache = Arc::new(ChunkCache::new(large_part_options()));
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            CacheMeshConfig::new("distributor", "uk", distributor_addr)
                // The edge is learned from its hello.
                .with_allowed_peers([edge_addr])
                .with_role(CacheMeshRole::Distributor)
                .with_announce_interval(Duration::from_millis(50))
                .with_socket_buffer_bytes(4 << 20)
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9])),
        )
        .start()
        .await
        .unwrap();
        let observed = Arc::new(DecodedChunks::default());
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let (class_applied_tx, mut class_applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(50))
                .with_socket_buffer_bytes(4 << 20)
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9]))
                .with_receive_observer(Arc::clone(&observed) as _)
                .with_applied_tx(applied_tx)
                .with_class_applied_tx(class_applied_tx),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        for slot in 0..4 {
            distributor_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 100]))
                .await
                .unwrap();
            distributor_cache
                .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 8_000]))
                .await
                .unwrap();
        }
        let collect = |rx: &mut mpsc::UnboundedReceiver<AppliedSlot>| {
            let mut slots = Vec::new();
            while let Ok(event) = rx.try_recv() {
                slots.push((event.stream_id, event.slot_id));
            }
            slots.sort_unstable();
            slots
        };
        timeout(Duration::from_secs(3), async {
            while edge_cache.get_for_stream_id(9, 3).await.is_none()
                || edge_cache.get_for_stream_id(7, 3).await.is_none()
            {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("every part reached the edge");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            collect(&mut applied_rx),
            (0..4).map(|slot| (7, slot)).collect::<Vec<_>>()
        );
        assert_eq!(
            collect(&mut class_applied_rx),
            (0..4).map(|slot| (9, slot)).collect::<Vec<_>>()
        );
        // The observer of the main socket saw the audio parts only: the
        // class socket has none unless it is given one.
        let mut observed = lock(&observed.0).clone();
        observed.sort_unstable();
        assert_eq!(observed, (0..4).map(|slot| (7, slot)).collect::<Vec<_>>());
        distributor.shutdown();
        edge.shutdown();
    }
    /// Stream 9 is audio until `is_video` is set, as a muxed stream is.
    fn flipping_classifier(
        is_video: &Arc<std::sync::atomic::AtomicBool>,
    ) -> CacheMeshStreamClassifier {
        let is_video = Arc::clone(is_video);
        Arc::new(move |stream_id| {
            if stream_id == 9 && is_video.load(std::sync::atomic::Ordering::Acquire) {
                StreamClass::Video
            } else {
                StreamClass::Audio
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_playlist_that_becomes_video_while_the_audio_lane_lags_loses_no_part() {
        use std::sync::atomic::AtomicBool;
        for replication in [ReplicationMode::All, ReplicationMode::Demand] {
            let distributor_addr = class_split_addr(TEST_CLASS_OFFSET);
            let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
            let distributor_cache = Arc::new(ChunkCache::new(demand_options()));
            let edge_cache = Arc::new(ChunkCache::new(demand_options()));
            let is_video = Arc::new(AtomicBool::new(false));
            // The audio lane stalls in the send observer on its first part
            // of stream 7 after `hold` is set, until `release`: it is behind,
            // as a sync task that sends to many children, or whose send did
            // not complete, is behind the watermark.
            let hold = Arc::new(AtomicBool::new(false));
            let holding = Arc::new(AtomicBool::new(false));
            let release = Arc::new(AtomicBool::new(false));
            let observer: CacheMeshSendObserver = {
                let hold = Arc::clone(&hold);
                let holding = Arc::clone(&holding);
                let release = Arc::clone(&release);
                Arc::new(move |stream_id, _, _, _| {
                    if stream_id == 7 && hold.swap(false, Ordering::AcqRel) {
                        holding.store(true, Ordering::Release);
                        // The sync task waits; the rest of the runtime (its
                        // timers too) goes on without this worker.
                        tokio::task::block_in_place(|| {
                            let give_up = std::time::Instant::now() + Duration::from_secs(5);
                            while !release.load(Ordering::Acquire)
                                && std::time::Instant::now() < give_up
                            {
                                std::thread::sleep(Duration::from_millis(1));
                            }
                        });
                    }
                })
            };
            let distributor = CacheMesh::new(
                Arc::clone(&distributor_cache),
                demand_distributor("distributor", distributor_addr)
                    .with_peer(edge_addr)
                    .with_replication(replication)
                    .with_class_port_offset(TEST_CLASS_OFFSET)
                    .with_stream_classifier(flipping_classifier(&is_video))
                    .with_send_observer(observer),
            )
            .start()
            .await
            .unwrap();
            // Every chunk that the edge decodes, on either socket.
            let decoded = Arc::new(DecodedChunks::default());
            let edge = CacheMesh::new(
                Arc::clone(&edge_cache),
                demand_edge("edge", edge_addr)
                    .with_peer(distributor_addr)
                    .with_replication(replication)
                    .with_class_port_offset(TEST_CLASS_OFFSET)
                    .with_stream_classifier(flipping_classifier(&is_video))
                    .with_receive_observer(Arc::clone(&decoded) as _)
                    .with_class_receive_observer(Some(Arc::clone(&decoded) as _)),
            )
            .start()
            .await
            .unwrap();
            wait_for_hello(&edge, 1).await;
            wait_for_hello(&distributor, 1).await;
            if replication == ReplicationMode::Demand {
                edge.subscribe(9, 0, 16).await.unwrap();
                edge.subscribe(7, 0, 16).await.unwrap();
                timeout(Duration::from_secs(2), async {
                    while distributor.served_subscriptions().len() < 2 {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .expect("the leases did not open");
            }
            for slot in 0..5 {
                distributor_cache
                    .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 200]))
                    .await
                    .unwrap();
                wait_for_slot(&edge_cache, 9, slot).await;
            }
            distributor_cache
                .add_for_stream_id(7, 0, Bytes::from_static(b"audio-0"))
                .await
                .unwrap();
            wait_for_slot(&edge_cache, 7, 0).await;

            // The audio lane stalls on the next part of stream 7.
            hold.store(true, Ordering::Release);
            distributor_cache
                .add_for_stream_id(7, 1, Bytes::from_static(b"audio-1"))
                .await
                .unwrap();
            timeout(Duration::from_secs(2), async {
                while !holding.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("the audio lane did not stall");
            // Part 5 is published while stream 9 is still audio; the video
            // lane sees the watermark move past it.
            distributor_cache
                .add_for_stream_id(9, 5, Bytes::from(vec![5u8; 200]))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            // The stream becomes video, and the video lane looks first.
            is_video.store(true, Ordering::Release);
            for slot in 6..8 {
                distributor_cache
                    .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 200]))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            release.store(true, Ordering::Release);
            for slot in 5..8 {
                wait_for_slot(&edge_cache, 9, slot).await;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            // Every part reached the edge once: part 5, which the audio lane
            // had not sent when the stream left it, from the video lane.
            let mut parts = lock(&decoded.0)
                .iter()
                .filter(|(stream_id, _)| *stream_id == 9)
                .map(|(_, slot)| *slot)
                .collect::<Vec<_>>();
            parts.sort_unstable();
            assert_eq!(parts, (0..8).collect::<Vec<_>>(), "{replication:?}");
            assert_eq!(
                distributor.class_fec_stats().unwrap().tx_objects,
                3,
                "{replication:?}"
            );
            distributor.shutdown();
            edge.shutdown();
        }
    }

    /// The peers that a receive task's observer is told of, one for each
    /// datagram.
    #[derive(Default)]
    struct SeenDatagrams(StdMutex<Vec<SocketAddr>>);

    impl CacheMeshReceiveObserver for SeenDatagrams {
        fn datagram(&self, peer: SocketAddr, _datagram: &[u8]) {
            lock(&self.0).push(peer);
        }

        fn decoded(&self, _peer: SocketAddr, _datagram: &[u8], _frame: &[u8]) {}
    }

    /// The class socket takes datagrams only from the class socket of an
    /// authorized peer: from any other address they are dropped before the
    /// observer and the FEC decoder see them. The limits of the main socket
    /// hold on it too.
    #[tokio::test]
    async fn a_class_socket_drops_unauthorized_datagrams_before_decode() {
        let node_addr = class_split_addr(TEST_CLASS_OFFSET);
        let peer_addr = class_split_addr(TEST_CLASS_OFFSET);
        let peer_class_addr =
            SocketAddr::from(([127, 0, 0, 1], peer_addr.port() + TEST_CLASS_OFFSET));
        let cache = Arc::new(ChunkCache::new(large_part_options()));
        let main_seen = Arc::new(SeenDatagrams::default());
        let class_seen = Arc::new(SeenDatagrams::default());
        let mesh = CacheMesh::new(
            Arc::clone(&cache),
            CacheMeshConfig::new("edge", "uk", node_addr)
                .with_peer(peer_addr)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_secs(10))
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9]))
                .with_max_frame_bytes(16 * 1024)
                .with_receive_observer(Arc::clone(&main_seen) as _)
                .with_class_receive_observer(Some(Arc::clone(&class_seen) as _)),
        )
        .start()
        .await
        .unwrap();
        let class_socket = mesh.class_local_addr().unwrap();
        let chunk = MeshFrame::chunk("parent".to_string(), 9, 0, Bytes::from_static(b"video-0"))
            .encode()
            .unwrap();

        // From an address that nobody authorized, and from the peer's main
        // address, which is not its class socket: two datagrams each (a
        // source and a repair symbol).
        let stranger = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_main = UdpSocket::bind(peer_addr).await.unwrap();
        for socket in [&stranger, &peer_main] {
            FecSender::new(1, DEFAULT_SYMBOL_SIZE)
                .send(socket, class_socket, &chunk)
                .await
                .unwrap();
        }
        // The peer's class socket is not authorized on the main socket.
        let peer_class = UdpSocket::bind(peer_class_addr).await.unwrap();
        FecSender::new(1, DEFAULT_SYMBOL_SIZE)
            .send(&peer_class, mesh.local_addr(), &chunk)
            .await
            .unwrap();
        timeout(Duration::from_secs(2), async {
            while mesh.class_fec_stats().unwrap().rx_unauthorized_datagrams < 4
                || mesh.fec_stats().rx_unauthorized_datagrams < 6
            {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("the datagrams were not dropped");
        let class = mesh.class_fec_stats().unwrap();
        assert_eq!(class.rx_unauthorized_datagrams, 4, "{class:?}");
        assert_eq!(class.rx_wire_datagrams, 0, "{class:?}");
        assert_eq!(class.rx_inflight_objects, 0, "{class:?}");
        assert_eq!(class.rx_decoded_objects, 0, "{class:?}");
        assert_eq!(mesh.fec_stats().rx_wire_datagrams, 0);
        // A cut-through observer of either socket never saw them, and
        // nothing was written.
        assert!(lock(&class_seen.0).is_empty());
        assert!(lock(&main_seen.0).is_empty());
        assert!(cache.resolve_stream(9).is_none());

        // From the peer's class socket: a frame beyond the limit is refused
        // before a decoder is made for it, and a part is applied, as the
        // peer's.
        let mut sender = FecSender::new(1, DEFAULT_SYMBOL_SIZE);
        let oversized = MeshFrame::chunk(
            "parent".to_string(),
            9,
            1,
            Bytes::from(vec![1u8; 20 * 1024]),
        )
        .encode()
        .unwrap();
        sender
            .send(&peer_class, class_socket, &oversized)
            .await
            .unwrap();
        sender
            .send(&peer_class, class_socket, &chunk)
            .await
            .unwrap();
        wait_for_slot(&cache, 9, 0).await;
        let class = mesh.class_fec_stats().unwrap();
        assert!(class.rx_oversized_objects >= 1, "{class:?}");
        assert_eq!(class.rx_decoded_objects, 1, "{class:?}");
        assert_eq!(class.rx_unauthorized_datagrams, 4, "{class:?}");
        assert!(cache.get_for_stream_id(9, 1).await.is_none());
        let seen = lock(&class_seen.0).clone();
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|peer| *peer == peer_addr), "{seen:?}");
        mesh.shutdown();
    }

    /// Whatever authorizes a peer authorizes its class socket, on the class
    /// socket only.
    #[tokio::test]
    async fn every_way_of_authorizing_a_peer_authorizes_its_class_socket() {
        let [node, configured, listed, allowed, added, gossiped, stranger] =
            [(); 7].map(|_| class_split_addr(TEST_CLASS_OFFSET));
        let mesh = CacheMesh::new(
            Arc::new(ChunkCache::new(demand_options())),
            CacheMeshConfig::new("node", "uk", node)
                .with_peer(configured)
                .with_peers([listed])
                .with_allowed_peers([allowed, gossiped])
                .with_announce_interval(Duration::from_secs(10))
                .with_class_port_offset(TEST_CLASS_OFFSET),
        )
        .start()
        .await
        .unwrap();
        assert!(mesh.add_peer(added).await);
        // A configured peer gossips an authorized peer and a stranger.
        let seed = UdpSocket::bind(configured).await.unwrap();
        let hello = MeshFrame::hello_with_peers("seed", "uk", vec![gossiped, stranger])
            .encode()
            .unwrap();
        FecSender::new(0, DEFAULT_SYMBOL_SIZE)
            .send(&seed, node, &hello)
            .await
            .unwrap();
        timeout(Duration::from_secs(1), async {
            while !mesh.peers().await.contains(&gossiped) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("the gossiped peer did not join");

        let main_lane = Lane {
            class: Some(StreamClass::Audio),
            port_offset: 0,
        };
        let class_lane = Lane {
            class: Some(StreamClass::Video),
            port_offset: TEST_CLASS_OFFSET,
        };
        let class_socket = |peer: SocketAddr| class_lane.peer_socket(peer).unwrap();
        for peer in [configured, listed, allowed, added, gossiped] {
            assert_eq!(
                mesh.shared
                    .authorized_sender(class_lane, class_socket(peer)),
                Some(peer)
            );
            assert_eq!(mesh.shared.authorized_sender(main_lane, peer), Some(peer));
            // Each address is authorized on its own socket only.
            assert_eq!(
                mesh.shared.authorized_sender(main_lane, class_socket(peer)),
                None
            );
            assert_eq!(mesh.shared.authorized_sender(class_lane, peer), None);
        }
        assert_eq!(
            mesh.shared
                .authorized_sender(class_lane, class_socket(stranger)),
            None
        );
        assert_eq!(mesh.shared.authorized_sender(main_lane, stranger), None);
        assert!(!mesh.peers().await.contains(&stranger));
        // A port below the offset has no main port: nobody's class socket.
        assert_eq!(
            mesh.shared
                .authorized_sender(class_lane, SocketAddr::from(([127, 0, 0, 1], 1))),
            None
        );
        mesh.shutdown();
    }

    /// A distributor that knows its edge only from the allowlist learns it
    /// from its hello, and sends it video on the class socket and audio on
    /// the main socket. No datagram of either lane is refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_class_split_mesh_between_authorized_peers_passes_video_and_audio() {
        let distributor_addr = class_split_addr(TEST_CLASS_OFFSET);
        let edge_addr = class_split_addr(TEST_CLASS_OFFSET);
        let distributor_cache = Arc::new(ChunkCache::new(large_part_options()));
        let edge_cache = Arc::new(ChunkCache::new(large_part_options()));
        let distributor = CacheMesh::new(
            Arc::clone(&distributor_cache),
            CacheMeshConfig::new("distributor", "uk", distributor_addr)
                .with_allowed_peers([edge_addr])
                .with_role(CacheMeshRole::Distributor)
                .with_announce_interval(Duration::from_millis(50))
                .with_socket_buffer_bytes(4 << 20)
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9])),
        )
        .start()
        .await
        .unwrap();
        let (applied_tx, mut applied_rx) = mpsc::unbounded_channel();
        let edge = CacheMesh::new(
            Arc::clone(&edge_cache),
            CacheMeshConfig::new("edge", "uk", edge_addr)
                .with_peer(distributor_addr)
                .with_role(CacheMeshRole::Edge)
                .with_announce_interval(Duration::from_millis(50))
                .with_socket_buffer_bytes(4 << 20)
                .with_class_port_offset(TEST_CLASS_OFFSET)
                .with_stream_classifier(video_streams(&[9]))
                .with_applied_tx(applied_tx),
        )
        .start()
        .await
        .unwrap();
        wait_for_hello(&edge, 1).await;
        timeout(Duration::from_secs(3), async {
            while !distributor.peers().await.contains(&edge_addr) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("the distributor did not learn its edge");

        let init = Bytes::from_static(b"ftyp-moov-of-the-video");
        distributor_cache
            .set_stream_initialization(9, init.clone())
            .await
            .unwrap();
        for slot in 0..6 {
            distributor_cache
                .add_for_stream_id(7, slot, Bytes::from(vec![slot as u8; 120]))
                .await
                .unwrap();
            distributor_cache
                .add_for_stream_id(9, slot, Bytes::from(vec![slot as u8; 20_000]))
                .await
                .unwrap();
        }
        let mut applied = Vec::new();
        timeout(Duration::from_secs(3), async {
            while applied.len() < 12 {
                applied.push(applied_rx.recv().await.unwrap());
            }
        })
        .await
        .expect("every part reached the edge");
        for slot in 0..6 {
            assert_eq!(
                edge_cache.get_for_stream_id(7, slot).await.unwrap().0,
                Bytes::from(vec![slot as u8; 120])
            );
            assert_eq!(
                edge_cache.get_for_stream_id(9, slot).await.unwrap().0,
                Bytes::from(vec![slot as u8; 20_000])
            );
        }
        assert_eq!(edge_cache.stream_initialization(9), Some(init));
        assert!(applied.iter().all(|event| event.parent == distributor_addr));
        // The video parts and the initialization on the class socket, the
        // audio parts on the main one.
        assert_eq!(distributor.class_fec_stats().unwrap().tx_objects, 7);
        assert_eq!(edge.class_fec_stats().unwrap().rx_decoded_objects, 7);
        assert!(edge.fec_stats().rx_decoded_objects >= 7 + 6);
        for node in [&distributor, &edge] {
            let stats = node.fec_stats();
            assert_eq!(stats.rx_unauthorized_datagrams, 0, "{stats:?}");
            assert_eq!(stats.rx_oversized_objects, 0, "{stats:?}");
        }
        distributor.shutdown();
        edge.shutdown();
    }
}
