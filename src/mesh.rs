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
const DEFAULT_SUBSCRIPTION_LEASE: Duration = Duration::from_secs(5);
const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_secs(2);
/// Longest burst that the warm-up pacer may send in one sync tick.
const BACKFILL_BURST: Duration = Duration::from_millis(100);
/// Source symbols that a large object is divided into when the symbol size
/// may grow with the object.
const ADAPTIVE_SOURCE_SYMBOLS: usize = 64;
const DEFAULT_SYMBOL_SIZE: u16 = 1316;
const DEFAULT_REPAIR_SYMBOLS: u32 = 1;
const DEFAULT_REPAIR_RATIO: f32 = 0.03;
const DEFAULT_MAX_REPAIR_SYMBOLS: u32 = 32;
const DEFAULT_ANNOUNCE_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(20);
const REQUEST_BLOCK_ID_BASE: u32 = 1 << 30;
const CHUNK_BLOCK_ID_BASE: u32 = 1 << 31;
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

    pub fn with_peer_timeout(mut self, timeout: Duration) -> Self {
        self.peer_timeout = timeout.max(Duration::from_millis(1));
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

    pub async fn start(self) -> Result<CacheMeshHandle, MeshError> {
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
            self.config.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(sync_task(
            Arc::clone(&socket),
            self.cache,
            Arc::clone(&peers),
            Arc::clone(&peer_regions),
            Arc::clone(&peer_roles),
            remote_slots,
            Arc::clone(&fec_counters),
            Arc::clone(&leases),
            Arc::clone(&served),
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
            request_block_id: Arc::new(AtomicU32::new(REQUEST_BLOCK_ID_BASE)),
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
        })
    }
}

#[derive(Clone)]
struct MeshShared {
    leases: LeaseTable,
    peer_seen: PeerSeen,
    wanted: WantedStreams,
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
    peer_seen: PeerSeen,
    wanted: WantedStreams,
}

impl CacheMeshHandle {
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
        );
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
                ..
            } => {
                out.put_u8(FRAME_REPLICA_REQUEST);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u64(*stream_id);
                out.put_u64(*from_slot);
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
                if buf.remaining() != 16 {
                    return Err(MeshError::InvalidFrame("replica request header mismatch"));
                }
                let stream_id = buf.get_u64();
                let from_slot = buf.get_u64();
                Ok(Self::ReplicaRequest {
                    node_id,
                    stream_id,
                    from_slot,
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
        let base = usize::from(self.symbol_size.max(1));
        let limit = usize::from(self.max_symbol_size);
        if limit <= base || frame_len <= base * ADAPTIVE_SOURCE_SYMBOLS {
            return self.symbol_size;
        }
        let multiple = frame_len
            .div_ceil(ADAPTIVE_SOURCE_SYMBOLS)
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
        }
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
        let repair_symbols = self.repair_symbols_for(frame.len());
        let symbol_size = self.symbol_size_for(frame.len());
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
}

#[derive(Debug)]
struct FecRecoveryObservation {
    missing_source_symbols: HashSet<u32>,
    completed_at_datagram: u64,
}

struct FecReceiver {
    decoders: HashMap<SocketAddr, DatagramFecDecoder>,
    observations: HashMap<(SocketAddr, u32), FecBlockObservation>,
    completed: HashSet<(SocketAddr, u32)>,
    completed_order: VecDeque<(SocketAddr, u32)>,
    recoveries: HashMap<(SocketAddr, u32), FecRecoveryObservation>,
    observed_datagrams: u64,
    stats: Arc<CacheMeshFecCounters>,
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
                    self.recoveries.remove(&key);
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
            });
        observation.last_seen_datagram = self.observed_datagrams;
        if is_source {
            observation
                .received_source_symbols
                .insert(encoding_symbol_id);
        }

        let decoded = decoded.map(Bytes::from);
        if let Some(frame) = decoded.as_ref() {
            let missing_source_symbols = self
                .observations
                .remove(&key)
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
    }

    fn prune_observations(&mut self) {
        let cutoff = self
            .observed_datagrams
            .saturating_sub(FEC_OBSERVATION_DATAGRAM_WINDOW);
        let before = self.observations.len();
        self.observations
            .retain(|_, observation| observation.last_seen_datagram >= cutoff);
        self.stats.rx_expired_objects.fetch_add(
            before.saturating_sub(self.observations.len()) as u64,
            Ordering::Relaxed,
        );

        let expired_recoveries = self
            .recoveries
            .iter()
            .filter(|(_, recovery)| recovery.completed_at_datagram < cutoff)
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
    let mut receiver = FecReceiver::new(Arc::clone(&stats));
    let mut reply_sender = FecSender::from_config_with_initial_block_id(
        &config,
        REQUEST_BLOCK_ID_BASE.saturating_add(1 << 20),
        stats,
    );
    let mut buf = vec![0u8; 65_536];

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
            handler
                .remote_slots
                .write()
                .await
                .insert((stream_id, slot_id));
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
        MeshFrame::ReplicaRequest {
            node_id,
            stream_id,
            from_slot,
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
) -> Result<usize, MeshError> {
    let Some(stream_idx) = cache.get_stream_idx(stream_id).await else {
        return Ok(0);
    };
    let mut sent = 0usize;
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
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut sender = FecSender::from_config(&config, stats);
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
                for peer in current_peers {
                    if let Err(err) = sender.send(&socket, peer, &frame).await {
                        debug!(peer = %peer, error = %err, "cache mesh hello send failed");
                    }
                }
            }
        }
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
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    if config.role == CacheMeshRole::Edge {
        return;
    }
    let demand = config.replication == ReplicationMode::Demand;
    let mut sender =
        FecSender::from_config_with_initial_block_id(&config, CHUNK_BLOCK_ID_BASE, stats);
    let mut sent: HashMap<(u64, usize, SocketAddr), usize> = HashMap::new();
    let mut sent_initializations: HashMap<(u64, usize, SocketAddr), Bytes> = HashMap::new();
    // Demand replication state: the leases the last tick saw, the warm-up
    // window of each, and the warm-up byte allowance of each child.
    let mut active: HashSet<(SocketAddr, u64)> = HashSet::new();
    let mut pending_open: HashMap<(SocketAddr, u64), (usize, usize)> = HashMap::new();
    let mut backfills: HashMap<(SocketAddr, u64), Backfill> = HashMap::new();
    let mut backfill_tokens: HashMap<SocketAddr, f64> = HashMap::new();
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
                    continue;
                }

                let stream_ids = cache.stream_ids().await;
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
                            match sender.send(&socket, *peer, &frame).await {
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
                            if let Err(err) = sender.send(&socket, *peer, &frame).await {
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
        wait_for_slot(&edge_cache, 7, 4).await;
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
