use crate::chunk_cache::ChunkCache;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use raptorq_datagram_fec::{DatagramFecDecoder, DatagramFecEncoder, DatagramFecError};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, warn};

const FRAME_MAGIC: &[u8; 8] = b"PLMESH1\0";
const FRAME_VERSION: u8 = 1;
const FRAME_HELLO: u8 = 1;
const FRAME_CHUNK: u8 = 2;
const FRAME_REPLICA_REQUEST: u8 = 3;
const DEFAULT_SYMBOL_SIZE: u16 = 1316;
const DEFAULT_REPAIR_SYMBOLS: u32 = 1;
const DEFAULT_ANNOUNCE_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(20);
const REQUEST_BLOCK_ID_BASE: u32 = 1 << 30;
const CHUNK_BLOCK_ID_BASE: u32 = 1 << 31;
const MAX_GOSSIP_PEERS: usize = 256;

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

#[derive(Clone, Debug)]
pub struct CacheMeshConfig {
    pub node_id: String,
    pub region: String,
    pub bind_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub announce_interval: Duration,
    pub sync_interval: Duration,
    pub repair_symbols: u32,
    pub symbol_size: u16,
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
            symbol_size: DEFAULT_SYMBOL_SIZE,
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
        let socket = Arc::new(UdpSocket::bind(self.config.bind_addr).await?);
        let local_addr = socket.local_addr()?;
        let peers = Arc::new(RwLock::new(
            self.config.peers.iter().copied().collect::<HashSet<_>>(),
        ));
        let remote_slots = Arc::new(RwLock::new(HashSet::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        tokio::spawn(receive_task(
            Arc::clone(&socket),
            Arc::clone(&self.cache),
            Arc::clone(&peers),
            Arc::clone(&remote_slots),
            self.config.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(announce_task(
            Arc::clone(&socket),
            Arc::clone(&peers),
            self.config.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(sync_task(
            Arc::clone(&socket),
            self.cache,
            Arc::clone(&peers),
            remote_slots,
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
            socket,
            node_id: self.config.node_id,
            repair_symbols: self.config.repair_symbols,
            symbol_size: self.config.symbol_size,
            request_block_id: Arc::new(AtomicU32::new(REQUEST_BLOCK_ID_BASE)),
            local_addr,
        })
    }
}

pub struct CacheMeshHandle {
    shutdown_tx: watch::Sender<()>,
    peers: Arc<RwLock<HashSet<SocketAddr>>>,
    socket: Arc<UdpSocket>,
    node_id: String,
    repair_symbols: u32,
    symbol_size: u16,
    request_block_id: Arc<AtomicU32>,
    local_addr: SocketAddr,
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

    pub async fn request_replica(
        &self,
        stream_id: u64,
        from_slot: usize,
    ) -> Result<usize, MeshError> {
        let peers = self.peers().await;
        if peers.is_empty() {
            return Ok(0);
        }

        let block_id = self.request_block_id.fetch_add(1, Ordering::Relaxed);
        let mut sender =
            FecSender::with_initial_block_id(self.repair_symbols, self.symbol_size, block_id);
        let frame = MeshFrame::replica_request(self.node_id.clone(), stream_id, from_slot as u64)
            .encode()?;
        let mut sent = 0usize;
        for peer in peers {
            sender.send(&self.socket, peer, &frame).await?;
            sent = sent.saturating_add(1);
        }
        Ok(sent)
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
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
        peers: Vec<SocketAddr>,
    },
    Chunk {
        node_id: String,
        stream_id: u64,
        slot_id: u64,
        payload: Bytes,
    },
    ReplicaRequest {
        node_id: String,
        stream_id: u64,
        from_slot: u64,
    },
}

impl MeshFrame {
    fn hello_with_peers(
        node_id: impl Into<String>,
        region: impl Into<String>,
        peers: Vec<SocketAddr>,
    ) -> Self {
        Self::Hello {
            node_id: node_id.into(),
            region: region.into(),
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

    fn node_id(&self) -> &str {
        match self {
            Self::Hello { node_id, .. }
            | Self::Chunk { node_id, .. }
            | Self::ReplicaRequest { node_id, .. } => node_id,
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
            Self::Hello { region, peers, .. } => {
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
        if version != FRAME_VERSION {
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
                    if buf.has_remaining() {
                        return Err(MeshError::InvalidFrame("trailing hello bytes"));
                    }
                }
                Ok(Self::Hello {
                    node_id,
                    region,
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
}

impl FecSender {
    fn new(repair_symbols: u32, symbol_size: u16) -> Self {
        Self::with_initial_block_id(repair_symbols, symbol_size, 0)
    }

    fn with_initial_block_id(repair_symbols: u32, symbol_size: u16, initial_block_id: u32) -> Self {
        let encoder = DatagramFecEncoder::new()
            .with_repair_symbols(repair_symbols)
            .with_symbol_size(symbol_size)
            .with_initial_block_id(initial_block_id);
        Self { encoder }
    }

    async fn send(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        frame: &[u8],
    ) -> Result<(), MeshError> {
        for packet in self.encoder.encode_object(frame)? {
            socket.send_to(&packet, peer).await?;
        }

        Ok(())
    }
}

struct FecReceiver {
    decoders: HashMap<SocketAddr, DatagramFecDecoder>,
}

impl FecReceiver {
    fn new() -> Self {
        Self {
            decoders: HashMap::new(),
        }
    }

    fn push(&mut self, peer: SocketAddr, bytes: &[u8]) -> Option<Bytes> {
        let decoder = self.decoders.entry(peer).or_default();
        decoder.push_datagram(bytes).ok().flatten().map(Bytes::from)
    }
}

async fn receive_task(
    socket: Arc<UdpSocket>,
    cache: Arc<ChunkCache>,
    peers: Arc<RwLock<HashSet<SocketAddr>>>,
    remote_slots: Arc<RwLock<HashSet<(u64, usize)>>>,
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut receiver = FecReceiver::new();
    let mut reply_sender = FecSender::with_initial_block_id(
        config.repair_symbols,
        config.symbol_size,
        REQUEST_BLOCK_ID_BASE.saturating_add(1 << 20),
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

                let Some(decoded) = receiver.push(peer, &buf[..len]) else {
                    continue;
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
                                remote_slots: &remote_slots,
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
    remote_slots: &'a Arc<RwLock<HashSet<(u64, usize)>>>,
    config: &'a CacheMeshConfig,
    reply_sender: &'a mut FecSender,
}

async fn handle_frame(frame: MeshFrame, peer: SocketAddr, handler: FrameHandler<'_>) {
    match frame {
        MeshFrame::Hello {
            node_id,
            region,
            peers,
        } => {
            let mut discovered = 0usize;
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
            let Ok(slot_id) = usize::try_from(slot_id) else {
                warn!(
                    node_id,
                    stream_id, slot_id, "cache mesh slot id does not fit usize"
                );
                return;
            };
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
            debug!(node_id, stream_id, slot_id, "cache mesh slot applied");
        }
        MeshFrame::ReplicaRequest {
            node_id,
            stream_id,
            from_slot,
        } => {
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
    }
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
    let Some(last) = cache.last(stream_idx) else {
        return Ok(0);
    };
    let from_slot = usize::try_from(from_slot).unwrap_or(usize::MAX);
    if from_slot > last {
        return Ok(0);
    }
    let from_slot = from_slot.max(cache.retained_start(last));
    let mut sent = 0usize;
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
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut sender = FecSender::new(config.repair_symbols, config.symbol_size);
    let mut interval = tokio::time::interval(config.announce_interval);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("cache mesh announce task shutting down");
                return;
            }
            _ = interval.tick() => {
                let current_peers = peers.read().await.iter().copied().collect::<Vec<_>>();
                let advertised_peers = current_peers
                    .iter()
                    .copied()
                    .take(MAX_GOSSIP_PEERS)
                    .collect::<Vec<_>>();
                let frame = match MeshFrame::hello_with_peers(
                    config.node_id.clone(),
                    config.region.clone(),
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
    remote_slots: Arc<RwLock<HashSet<(u64, usize)>>>,
    config: CacheMeshConfig,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut sender = FecSender::with_initial_block_id(
        config.repair_symbols,
        config.symbol_size,
        CHUNK_BLOCK_ID_BASE,
    );
    let mut sent: HashMap<(u64, usize), usize> = HashMap::new();
    let mut interval = tokio::time::interval(config.sync_interval);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("cache mesh sync task shutting down");
                return;
            }
            _ = interval.tick() => {
                let peers = peers.read().await.iter().copied().collect::<Vec<_>>();
                if peers.is_empty() {
                    continue;
                }

                for (stream_id, stream_idx) in cache.stream_ids().await {
                    let Some(last) = cache.last(stream_idx) else {
                        continue;
                    };
                    let retained_start = cache.retained_start(last);
                    let next = sent
                        .get(&(stream_id, stream_idx))
                        .copied()
                        .and_then(|slot| slot.checked_add(1))
                        .unwrap_or(retained_start)
                        .max(retained_start);
                    if next > last {
                        continue;
                    }

                    for slot_id in next..=last {
                        if remote_slots.read().await.contains(&(stream_id, slot_id)) {
                            sent.insert((stream_id, stream_idx), slot_id);
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
                        for peer in &peers {
                            if let Err(err) = sender.send(&socket, *peer, &frame).await {
                                debug!(peer = %peer, stream_id, slot_id, error = %err, "cache mesh chunk send failed");
                            }
                        }
                        sent.insert((stream_id, stream_idx), slot_id);
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
        legacy_hello.put_u8(FRAME_VERSION);
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
                    if bytes == Bytes::from_static(b"ts-part-1") {
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
        let mut receiver = FecReceiver::new();

        sender.send(&tx, rx_addr, b"mesh-frame").await.unwrap();

        let mut buf = vec![0u8; 65_536];
        let mut decoded = Vec::new();
        for _ in 0..3 {
            let (len, peer) = rx.recv_from(&mut buf).await.unwrap();
            if let Some(frame) = receiver.push(peer, &buf[..len]) {
                decoded.push(frame);
            }
        }

        assert_eq!(decoded, vec![Bytes::from_static(b"mesh-frame")]);
    }
}
