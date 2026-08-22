mod support;

use bytes::{BufMut, Bytes, BytesMut};
use playlists::{
    chunk_cache::ChunkCache,
    mesh::{CacheMesh, CacheMeshConfig, CacheMeshFecStats, CacheMeshRole},
    Options,
};
use raptorq_datagram_fec::{DatagramFecEncoder, DatagramFecHeader, HEADER_LEN};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::runtime::Builder;
use tokio::sync::Barrier;

use support::{allocation_snapshot, reset_peak_live_bytes};

const FRAME_MAGIC: &[u8; 8] = b"PLMESH1\0";
const FRAME_VERSION: u8 = 2;
const FRAME_CHUNK: u8 = 2;
const LATENCY_SAMPLE_INTERVAL: u64 = 4_093;
const DEADLINE_CHECK_INTERVAL: u64 = 256;
const COOPERATIVE_YIELD_INTERVAL: u64 = 4_096;
const MEDIA_BYTES: usize = 1_024;
const REPLICA_PARTS: usize = 8;

#[derive(Clone, Copy)]
enum Mode {
    Receive,
    Recovery,
    Replica,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Receive => "udp_fec_receive_and_cache_write",
            Self::Recovery => "udp_fec_one_source_recovery_and_cache_write",
            Self::Replica => "replica_request_enqueue_and_range_service",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Usage {
    cpu_seconds: f64,
    max_rss_platform_units: i64,
}

#[derive(Default)]
struct WorkerResult {
    operations: u64,
    failures: u64,
    latency_ns: Vec<u64>,
}

#[derive(Serialize)]
struct Percentiles {
    samples: usize,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
}

#[derive(Serialize)]
struct StepReport {
    workers: usize,
    duration_seconds: f64,
    submitted_operations: u64,
    submission_failures: u64,
    submitted_operations_per_second: f64,
    decoded_objects: u64,
    repaired_objects: u64,
    transmitted_objects: u64,
    frame_queue_depth: u64,
    frame_queue_max_depth: u64,
    frame_queue_drops: u64,
    replica_queue_depth: u64,
    replica_queue_max_depth: u64,
    replica_queue_drops: u64,
    replica_requests_serviced: u64,
    replica_service_errors: u64,
    decode_nanoseconds_per_object: f64,
    replica_service_nanoseconds_per_request: f64,
    cpu_cores_used: f64,
    cpu_nanoseconds_per_submitted_operation: f64,
    max_rss_platform_units: i64,
    allocation_calls: u64,
    reallocation_calls: u64,
    deallocation_calls: u64,
    process_live_allocation_bytes: u64,
    sharded_peak_live_allocation_bytes_upper_bound: u64,
    retained_cache_payload_bytes: usize,
    maximum_cache_payload_bytes: usize,
    sampled_submission_latency: Percentiles,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    mode: &'static str,
    target_os: &'static str,
    target_arch: &'static str,
    available_parallelism: usize,
    media_bytes_per_chunk: usize,
    payload_note: &'static str,
    allocation_note: &'static str,
    steps: Vec<StepReport>,
}

fn main() {
    let (duration, mode) = parse_args();
    let available_parallelism = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    let mut worker_counts = BTreeSet::from([1, 2, 4, available_parallelism]);
    worker_counts.retain(|workers| *workers > 0 && *workers <= available_parallelism);
    let mut steps = Vec::new();
    for workers in worker_counts {
        let runtime = Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .expect("build Tokio runtime");
        steps.push(runtime.block_on(run_step(workers, duration, mode)));
    }

    let report = Report {
        schema: "needletail.playlists.mesh-pipeline.v1",
        mode: mode.label(),
        target_os: env::consts::OS,
        target_arch: env::consts::ARCH,
        available_parallelism,
        media_bytes_per_chunk: MEDIA_BYTES,
        payload_note:
            "submitted payload length; this is not decoded or network goodput",
        allocation_note:
            "uses 64 counter shards; live bytes include runtime and socket state; summed shard peaks are an upper bound",
        steps,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
}

async fn run_step(workers: usize, duration: Duration, mode: Mode) -> StepReport {
    match mode {
        Mode::Receive | Mode::Recovery => run_receive_step(workers, duration, mode).await,
        Mode::Replica => run_replica_step(workers, duration).await,
    }
}

async fn run_receive_step(workers: usize, duration: Duration, mode: Mode) -> StepReport {
    let cache = Arc::new(ChunkCache::new(cache_options(workers, 512)));
    let mut senders = Vec::with_capacity(workers);
    for _ in 0..workers {
        senders.push(Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap()));
    }
    let allowed = senders
        .iter()
        .map(|socket| socket.local_addr().unwrap())
        .collect::<Vec<_>>();
    let mut config = CacheMeshConfig::new("receiver", "local", "127.0.0.1:0".parse().unwrap())
        .with_allowed_peers(allowed)
        .with_max_peers(workers)
        .with_max_inflight_frames(workers.saturating_mul(64))
        .with_frame_queue_capacity(256);
    config.announce_interval = Duration::from_secs(60);
    config.sync_interval = Duration::from_secs(60);
    let mesh = CacheMesh::new(Arc::clone(&cache), config)
        .start()
        .await
        .unwrap();
    let destination = mesh.local_addr();
    let barrier = Arc::new(Barrier::new(workers + 1));
    let payload = Bytes::from(vec![0x5a; MEDIA_BYTES]);
    let mut tasks = Vec::with_capacity(workers);

    for (worker, socket) in senders.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            let mut encoder = DatagramFecEncoder::new()
                .with_symbol_size(256)
                .with_repair_symbols(u32::from(matches!(mode, Mode::Recovery)))
                .with_initial_block_id((worker as u32).wrapping_mul(100_000_000));
            barrier.wait().await;
            let deadline = Instant::now() + duration;
            let mut result = WorkerResult::default();
            loop {
                let sampled =
                    (result.operations + result.failures).is_multiple_of(LATENCY_SAMPLE_INTERVAL);
                let sample_started = sampled.then(Instant::now);
                let frame = encode_chunk_frame(
                    &format!("sender-{worker}"),
                    worker as u64 + 1,
                    result.operations + result.failures,
                    &payload,
                );
                let datagrams = match encoder.encode_object(&frame) {
                    Ok(datagrams) => datagrams,
                    Err(_) => {
                        result.failures += 1;
                        continue;
                    }
                };
                let mut send_failed = false;
                for datagram in datagrams {
                    if matches!(mode, Mode::Recovery) && is_first_source_symbol(&datagram) {
                        continue;
                    }
                    if socket.send_to(&datagram, destination).await.is_err() {
                        send_failed = true;
                        break;
                    }
                }
                if send_failed {
                    result.failures += 1;
                } else {
                    result.operations += 1;
                }
                if let Some(sample_started) = sample_started {
                    result.latency_ns.push(
                        sample_started
                            .elapsed()
                            .as_nanos()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                }
                let completed = result.operations + result.failures;
                if completed.is_multiple_of(DEADLINE_CHECK_INTERVAL) && Instant::now() >= deadline {
                    break;
                }
                if completed.is_multiple_of(COOPERATIVE_YIELD_INTERVAL) {
                    tokio::task::yield_now().await;
                }
            }
            result
        }));
    }

    reset_peak_live_bytes();
    let allocations_before = allocation_snapshot();
    let usage_before = process_usage();
    barrier.wait().await;
    let started = Instant::now();
    let result = collect_workers(tasks).await;
    let elapsed = started.elapsed();
    tokio::time::sleep(Duration::from_millis(25)).await;
    let usage_after = process_usage();
    let allocations = allocation_snapshot().since(allocations_before);
    let stats = mesh.fec_stats();
    let memory = cache.memory_stats().await;
    mesh.shutdown();
    build_step_report(
        workers,
        elapsed,
        result,
        stats,
        usage_before,
        usage_after,
        allocations,
        memory.chunk_bytes + memory.initialization_bytes,
        memory.maximum_payload_bytes,
    )
}

async fn run_replica_step(workers: usize, duration: Duration) -> StepReport {
    let distributor_addr = reserve_loopback_addr();
    let edge_addr = reserve_loopback_addr();
    let distributor_cache = Arc::new(ChunkCache::new(cache_options(workers, REPLICA_PARTS)));
    let edge_cache = Arc::new(ChunkCache::new(cache_options(workers, REPLICA_PARTS)));
    let payload = Bytes::from(vec![0x7b; MEDIA_BYTES]);
    for worker in 0..workers {
        let handle = distributor_cache
            .resolve_or_create_stream(worker as u64 + 1)
            .await;
        for sequence in 0..REPLICA_PARTS {
            distributor_cache
                .add_for_handle(handle, sequence, payload.clone())
                .await
                .unwrap();
        }
    }

    let mut distributor_config = CacheMeshConfig::new("distributor", "local", distributor_addr)
        .with_peer(edge_addr)
        .with_role(CacheMeshRole::Distributor)
        .with_replica_queue_capacity(64);
    distributor_config.announce_interval = Duration::from_secs(60);
    distributor_config.sync_interval = Duration::from_secs(60);
    let mut edge_config = CacheMeshConfig::new("edge", "local", edge_addr)
        .with_peer(distributor_addr)
        .with_role(CacheMeshRole::Edge)
        .with_frame_queue_capacity(256);
    edge_config.announce_interval = Duration::from_secs(60);
    edge_config.sync_interval = Duration::from_secs(60);
    let distributor = CacheMesh::new(Arc::clone(&distributor_cache), distributor_config)
        .start()
        .await
        .unwrap();
    let edge = Arc::new(
        CacheMesh::new(Arc::clone(&edge_cache), edge_config)
            .start()
            .await
            .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut tasks = Vec::with_capacity(workers);
    for worker in 0..workers {
        let edge = Arc::clone(&edge);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let deadline = Instant::now() + duration;
            let mut result = WorkerResult::default();
            loop {
                let sampled =
                    (result.operations + result.failures).is_multiple_of(LATENCY_SAMPLE_INTERVAL);
                let sample_started = sampled.then(Instant::now);
                match edge.request_replica(worker as u64 + 1, 0).await {
                    Ok(1) => result.operations += 1,
                    _ => result.failures += 1,
                }
                if let Some(sample_started) = sample_started {
                    result.latency_ns.push(
                        sample_started
                            .elapsed()
                            .as_nanos()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                }
                let completed = result.operations + result.failures;
                if completed.is_multiple_of(DEADLINE_CHECK_INTERVAL) && Instant::now() >= deadline {
                    break;
                }
                if completed.is_multiple_of(COOPERATIVE_YIELD_INTERVAL) {
                    tokio::task::yield_now().await;
                }
            }
            result
        }));
    }

    reset_peak_live_bytes();
    let allocations_before = allocation_snapshot();
    let usage_before = process_usage();
    barrier.wait().await;
    let started = Instant::now();
    let result = collect_workers(tasks).await;
    let elapsed = started.elapsed();
    tokio::time::sleep(Duration::from_millis(25)).await;
    let usage_after = process_usage();
    let allocations = allocation_snapshot().since(allocations_before);
    let stats = distributor.fec_stats();
    let distributor_memory = distributor_cache.memory_stats().await;
    let edge_memory = edge_cache.memory_stats().await;
    distributor.shutdown();
    edge.shutdown();
    build_step_report(
        workers,
        elapsed,
        result,
        stats,
        usage_before,
        usage_after,
        allocations,
        distributor_memory
            .chunk_bytes
            .saturating_add(distributor_memory.initialization_bytes)
            .saturating_add(edge_memory.chunk_bytes)
            .saturating_add(edge_memory.initialization_bytes),
        distributor_memory
            .maximum_payload_bytes
            .saturating_add(edge_memory.maximum_payload_bytes),
    )
}

async fn collect_workers(tasks: Vec<tokio::task::JoinHandle<WorkerResult>>) -> WorkerResult {
    let mut result = WorkerResult::default();
    for task in tasks {
        let worker = task.await.expect("mesh benchmark worker completed");
        result.operations += worker.operations;
        result.failures += worker.failures;
        result.latency_ns.extend(worker.latency_ns);
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn build_step_report(
    workers: usize,
    elapsed: Duration,
    result: WorkerResult,
    stats: CacheMeshFecStats,
    usage_before: Usage,
    usage_after: Usage,
    allocations: support::AllocationSnapshot,
    retained_cache_payload_bytes: usize,
    maximum_cache_payload_bytes: usize,
) -> StepReport {
    let seconds = elapsed.as_secs_f64();
    let cpu_seconds = (usage_after.cpu_seconds - usage_before.cpu_seconds).max(0.0);
    StepReport {
        workers,
        duration_seconds: seconds,
        submitted_operations: result.operations,
        submission_failures: result.failures,
        submitted_operations_per_second: result.operations as f64 / seconds,
        decoded_objects: stats.rx_decoded_objects,
        repaired_objects: stats.rx_repaired_objects,
        transmitted_objects: stats.tx_objects,
        frame_queue_depth: stats.frame_queue_depth,
        frame_queue_max_depth: stats.frame_queue_max_depth,
        frame_queue_drops: stats.frame_queue_drops,
        replica_queue_depth: stats.replica_queue_depth,
        replica_queue_max_depth: stats.replica_queue_max_depth,
        replica_queue_drops: stats.replica_queue_drops,
        replica_requests_serviced: stats.replica_requests_serviced,
        replica_service_errors: stats.replica_service_errors,
        decode_nanoseconds_per_object: stats.decode_nanoseconds as f64
            / stats.rx_decoded_objects.max(1) as f64,
        replica_service_nanoseconds_per_request: stats.replica_service_nanoseconds as f64
            / stats.replica_requests_serviced.max(1) as f64,
        cpu_cores_used: cpu_seconds / seconds,
        cpu_nanoseconds_per_submitted_operation: cpu_seconds * 1e9
            / result.operations.max(1) as f64,
        max_rss_platform_units: usage_after.max_rss_platform_units,
        allocation_calls: allocations.allocation_calls,
        reallocation_calls: allocations.reallocation_calls,
        deallocation_calls: allocations.deallocation_calls,
        process_live_allocation_bytes: allocations.live_bytes,
        sharded_peak_live_allocation_bytes_upper_bound: allocations.peak_live_bytes,
        retained_cache_payload_bytes,
        maximum_cache_payload_bytes,
        sampled_submission_latency: percentiles(result.latency_ns),
    }
}

fn cache_options(streams: usize, retained_parts: usize) -> Options {
    Options {
        num_playlists: streams,
        max_segments: 1,
        max_parts_per_segment: retained_parts,
        buffer_size_kb: 16,
        ..Options::default()
    }
}

fn encode_chunk_frame(node_id: &str, stream_id: u64, slot_id: u64, payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::with_capacity(
        FRAME_MAGIC.len() + 1 + 1 + 2 + node_id.len() + 8 + 8 + 4 + payload.len(),
    );
    frame.put_slice(FRAME_MAGIC);
    frame.put_u8(FRAME_VERSION);
    frame.put_u8(FRAME_CHUNK);
    frame.put_u16(node_id.len() as u16);
    frame.put_slice(node_id.as_bytes());
    frame.put_u64(stream_id);
    frame.put_u64(slot_id);
    frame.put_u32(payload.len() as u32);
    frame.put_slice(payload);
    frame.freeze()
}

fn is_first_source_symbol(datagram: &[u8]) -> bool {
    let Ok(header) = DatagramFecHeader::decode(datagram) else {
        return false;
    };
    if datagram.len() < HEADER_LEN + 4 {
        return false;
    }
    let encoding_symbol_id = u32::from_be_bytes([
        0,
        datagram[HEADER_LEN + 1],
        datagram[HEADER_LEN + 2],
        datagram[HEADER_LEN + 3],
    ]);
    encoding_symbol_id == 0 && encoding_symbol_id < u32::from(header.source_symbols)
}

fn reserve_loopback_addr() -> SocketAddr {
    static NEXT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(35_000);
    loop {
        let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        if std::net::UdpSocket::bind(address).is_ok() {
            return address;
        }
    }
}

fn parse_args() -> (Duration, Mode) {
    let args = env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    let mut duration = 3.0;
    let mut mode = Mode::Receive;
    let mut index = 0;
    while index < args.len() {
        let value = args
            .get(index + 1)
            .unwrap_or_else(|| panic!("missing value for {}", args[index]));
        match args[index].as_str() {
            "--duration-seconds" => {
                duration = value
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite() && *value >= 0.25 && *value <= 300.0)
                    .expect("--duration-seconds must be between 0.25 and 300");
            }
            "--mode" => {
                mode = match value.as_str() {
                    "receive" => Mode::Receive,
                    "recovery" => Mode::Recovery,
                    "replica" => Mode::Replica,
                    _ => panic!("--mode must be receive, recovery, or replica"),
                };
            }
            flag => panic!("unknown argument {flag}"),
        }
        index += 2;
    }
    (Duration::from_secs_f64(duration), mode)
}

fn percentiles(mut values_ns: Vec<u64>) -> Percentiles {
    values_ns.sort_unstable();
    let at = |percentile: usize| {
        if values_ns.is_empty() {
            return 0.0;
        }
        let rank = values_ns.len().saturating_mul(percentile).div_ceil(100);
        values_ns[rank.clamp(1, values_ns.len()) - 1] as f64 / 1_000.0
    };
    Percentiles {
        samples: values_ns.len(),
        p50_us: at(50),
        p95_us: at(95),
        p99_us: at(99),
        max_us: values_ns.last().copied().unwrap_or(0) as f64 / 1_000.0,
    }
}

fn process_usage() -> Usage {
    let mut raw = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the provided rusage when it returns zero.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, raw.as_mut_ptr()) } != 0 {
        return Usage::default();
    }
    // SAFETY: the successful getrusage call above initialized raw.
    let raw = unsafe { raw.assume_init() };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    Usage {
        cpu_seconds: seconds(raw.ru_utime) + seconds(raw.ru_stime),
        max_rss_platform_units: raw.ru_maxrss,
    }
}
