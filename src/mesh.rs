use crate::chunk_cache::ChunkCache;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, warn};

const FRAME_MAGIC: &[u8; 8] = b"PLMESH1\0";
const FEC_MAGIC: &[u8; 8] = b"PLFEC1\0\0";
const FRAME_VERSION: u8 = 1;
const FRAME_HELLO: u8 = 1;
const FRAME_CHUNK: u8 = 2;
const FEC_HEADER_LEN: usize = 18;
const DEFAULT_SYMBOL_SIZE: u16 = 1316;
const DEFAULT_REPAIR_SYMBOLS: u32 = 1;
const DEFAULT_ANNOUNCE_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug)]
pub enum MeshError {
    Io(std::io::Error),
    InvalidFrame(&'static str),
    InvalidUtf8(std::str::Utf8Error),
    Cache(&'static str),
}

impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
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
            self.config.node_id.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(announce_task(
            Arc::clone(&socket),
            Arc::clone(&peers),
            self.config.clone(),
            shutdown_rx.clone(),
        ));
        tokio::spawn(sync_task(
            socket,
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
            local_addr,
        })
    }
}

pub struct CacheMeshHandle {
    shutdown_tx: watch::Sender<()>,
    peers: Arc<RwLock<HashSet<SocketAddr>>>,
    local_addr: SocketAddr,
}

impl CacheMeshHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn peers(&self) -> Vec<SocketAddr> {
        self.peers.read().await.iter().copied().collect()
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
    },
    Chunk {
        node_id: String,
        stream_id: u64,
        slot_id: u64,
        payload: Bytes,
    },
}

impl MeshFrame {
    fn hello(node_id: impl Into<String>, region: impl Into<String>) -> Self {
        Self::Hello {
            node_id: node_id.into(),
            region: region.into(),
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

    fn node_id(&self) -> &str {
        match self {
            Self::Hello { node_id, .. } | Self::Chunk { node_id, .. } => node_id,
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
            Self::Hello { region, .. } => {
                let region = region.as_bytes();
                if region.len() > u16::MAX as usize {
                    return Err(MeshError::InvalidFrame("region too long"));
                }
                out.put_u8(FRAME_HELLO);
                out.put_u16(node_id.len() as u16);
                out.put_slice(node_id);
                out.put_u16(region.len() as u16);
                out.put_slice(region);
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
                if buf.has_remaining() {
                    return Err(MeshError::InvalidFrame("trailing hello bytes"));
                }
                Ok(Self::Hello { node_id, region })
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

#[derive(Debug, Clone, Copy)]
struct FecHeader {
    block_id: u32,
    transfer_length: u32,
    symbol_size: u16,
}

impl FecHeader {
    fn encode(&self, out: &mut BytesMut) {
        out.put_slice(FEC_MAGIC);
        out.put_u32(self.block_id);
        out.put_u32(self.transfer_length);
        out.put_u16(self.symbol_size);
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < FEC_HEADER_LEN || &bytes[..FEC_MAGIC.len()] != FEC_MAGIC {
            return None;
        }
        let mut buf = &bytes[FEC_MAGIC.len()..FEC_HEADER_LEN];
        Some(Self {
            block_id: buf.get_u32(),
            transfer_length: buf.get_u32(),
            symbol_size: buf.get_u16(),
        })
    }

    fn oti(&self) -> ObjectTransmissionInformation {
        ObjectTransmissionInformation::with_defaults(self.transfer_length as u64, self.symbol_size)
    }
}

struct FecSender {
    next_block_id: u32,
    repair_symbols: u32,
    symbol_size: u16,
}

impl FecSender {
    fn new(repair_symbols: u32, symbol_size: u16) -> Self {
        Self {
            next_block_id: 0,
            repair_symbols,
            symbol_size,
        }
    }

    async fn send(
        &mut self,
        socket: &UdpSocket,
        peer: SocketAddr,
        frame: &[u8],
    ) -> Result<(), MeshError> {
        let encoder = Encoder::with_defaults(frame, self.symbol_size);
        let packets = encoder.get_encoded_packets(self.repair_symbols);
        let header = FecHeader {
            block_id: self.next_block_id,
            transfer_length: frame.len() as u32,
            symbol_size: self.symbol_size,
        };

        for packet in packets {
            let packet = packet.serialize();
            let mut out = BytesMut::with_capacity(FEC_HEADER_LEN + packet.len());
            header.encode(&mut out);
            out.put_slice(&packet);
            socket.send_to(&out, peer).await?;
        }

        self.next_block_id = self.next_block_id.wrapping_add(1);
        Ok(())
    }
}

struct FecReceiver {
    blocks: HashMap<(SocketAddr, u32), Decoder>,
}

impl FecReceiver {
    fn new() -> Self {
        Self {
            blocks: HashMap::new(),
        }
    }

    fn push(&mut self, peer: SocketAddr, bytes: &[u8]) -> Option<Bytes> {
        let header = FecHeader::decode(bytes)?;
        let packet = EncodingPacket::deserialize(&bytes[FEC_HEADER_LEN..]);
        let decoder = self
            .blocks
            .entry((peer, header.block_id))
            .or_insert_with(|| Decoder::new(header.oti()));
        let decoded = decoder.decode(packet)?;
        self.blocks.remove(&(peer, header.block_id));
        self.prune(peer, header.block_id);
        Some(Bytes::from(decoded))
    }

    fn prune(&mut self, peer: SocketAddr, current_block_id: u32) {
        let cutoff = current_block_id.wrapping_sub(32);
        self.blocks
            .retain(|(candidate_peer, block_id), _| *candidate_peer != peer || *block_id >= cutoff);
    }
}

async fn receive_task(
    socket: Arc<UdpSocket>,
    cache: Arc<ChunkCache>,
    peers: Arc<RwLock<HashSet<SocketAddr>>>,
    remote_slots: Arc<RwLock<HashSet<(u64, usize)>>>,
    node_id: String,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let mut receiver = FecReceiver::new();
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
                        if frame.node_id() == node_id {
                            continue;
                        }
                        handle_frame(frame, peer, &cache, &peers, &remote_slots).await;
                    }
                    Err(err) => warn!(peer = %peer, error = %err, "cache mesh frame decode failed"),
                }
            }
        }
    }
}

async fn handle_frame(
    frame: MeshFrame,
    peer: SocketAddr,
    cache: &Arc<ChunkCache>,
    peers: &Arc<RwLock<HashSet<SocketAddr>>>,
    remote_slots: &Arc<RwLock<HashSet<(u64, usize)>>>,
) {
    match frame {
        MeshFrame::Hello { node_id, region } => {
            peers.write().await.insert(peer);
            debug!(node_id, region, peer = %peer, "cache mesh peer discovered");
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
            if let Err(err) = cache.add_for_stream_id(stream_id, slot_id, payload).await {
                warn!(
                    node_id,
                    stream_id,
                    slot_id,
                    error = err,
                    "cache mesh write failed"
                );
                return;
            }
            remote_slots.write().await.insert((stream_id, slot_id));
            debug!(node_id, stream_id, slot_id, "cache mesh slot applied");
        }
    }
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
                let frame = match MeshFrame::hello(config.node_id.clone(), config.region.clone()).encode() {
                    Ok(frame) => frame,
                    Err(err) => {
                        error!(error = %err, "cache mesh hello encode failed");
                        continue;
                    }
                };
                let current_peers = peers.read().await.iter().copied().collect::<Vec<_>>();
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
    let mut sender = FecSender::new(config.repair_symbols, config.symbol_size);
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
                    let next = sent
                        .get(&(stream_id, stream_idx))
                        .copied()
                        .and_then(|slot| slot.checked_add(1))
                        .unwrap_or(0);
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
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    }

    #[test]
    fn mesh_frame_roundtrips() {
        let frame = MeshFrame::chunk("uk", 42, 7, Bytes::from_static(b"media"));
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);

        let frame = MeshFrame::hello("us", "us-east");
        let encoded = frame.encode().unwrap();
        let decoded = MeshFrame::decode(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[tokio::test]
    async fn cache_mesh_syncs_slots_between_two_nodes() {
        let addr_a = loopback_addr();
        let addr_b = loopback_addr();

        let mut options = Options::default();
        options.num_playlists = 4;
        options.max_segments = 1;
        options.max_parts_per_segment = 16;
        options.buffer_size_kb = 4;

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
}
