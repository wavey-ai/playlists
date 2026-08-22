mod support;

use bytes::Bytes;
use playlists::{
    chunk_cache::{ChunkCache, StreamHandle},
    Options,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;
use tokio::sync::Barrier;

use support::{allocation_snapshot, reset_peak_live_bytes};

const PART_BYTES: usize = 5_760;
const RETAINED_PARTS: usize = 512;
const LATENCY_SAMPLE_INTERVAL: u64 = 4_093;
const DEADLINE_CHECK_INTERVAL: u64 = 256;
const COOPERATIVE_YIELD_INTERVAL: u64 = 4_096;

#[derive(Clone, Copy)]
enum Mode {
    WriteRaw,
    WriteMapped,
    WriteHandle,
    MixedHandle,
    Churn,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::WriteRaw => "raw_index_write_independent_streams",
            Self::WriteMapped => "mapped_write_independent_streams",
            Self::WriteHandle => "handle_write_independent_streams",
            Self::MixedHandle => "handle_15_reads_per_write",
            Self::Churn => "stream_create_evict_reuse_with_write",
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
    reads: u64,
    writes: u64,
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
    reads: u64,
    writes: u64,
    failures: u64,
    operations_per_second: f64,
    logical_payload_gbit_per_second: f64,
    cpu_cores_used: f64,
    cpu_nanoseconds_per_operation: f64,
    max_rss_platform_units: i64,
    allocation_calls: u64,
    reallocation_calls: u64,
    deallocation_calls: u64,
    process_live_allocation_bytes: u64,
    sharded_peak_live_allocation_bytes_upper_bound: u64,
    retained_cache_payload_bytes: usize,
    maximum_cache_payload_bytes: usize,
    sampled_operation_latency: Percentiles,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    mode: &'static str,
    target_os: &'static str,
    target_arch: &'static str,
    available_parallelism: usize,
    bytes_per_part: usize,
    retained_parts_per_stream: usize,
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
        schema: "needletail.playlists.chunk-workloads.v1",
        mode: mode.label(),
        target_os: env::consts::OS,
        target_arch: env::consts::ARCH,
        available_parallelism,
        bytes_per_part: PART_BYTES,
        retained_parts_per_stream: RETAINED_PARTS,
        payload_note:
            "logical Bytes length accessed; this is not network throughput or a memory copy",
        allocation_note:
            "uses 64 counter shards; live bytes include runtime state; summed shard peaks are an upper bound",
        steps,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
}

async fn run_step(workers: usize, duration: Duration, mode: Mode) -> StepReport {
    let cache = Arc::new(ChunkCache::new(Options {
        num_playlists: workers,
        max_segments: 1,
        max_parts_per_segment: RETAINED_PARTS,
        buffer_size_kb: 8,
        ..Options::default()
    }));
    let payload = Bytes::from(vec![0x5a; PART_BYTES]);
    let mut streams: Vec<Option<StreamHandle>> = vec![None; workers];
    match mode {
        Mode::WriteRaw => {
            for stream_idx in 0..workers {
                cache
                    .add(stream_idx, 0, payload.clone())
                    .await
                    .expect("seed physical-index stream");
            }
        }
        Mode::Churn => {}
        Mode::WriteMapped | Mode::WriteHandle | Mode::MixedHandle => {
            for (stream_idx, stream) in streams.iter_mut().enumerate() {
                let handle = cache.resolve_or_create_stream(stream_idx as u64 + 1).await;
                cache
                    .add_for_handle(handle, 0, payload.clone())
                    .await
                    .expect("seed mapped stream");
                *stream = Some(handle);
            }
        }
    }

    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut tasks = Vec::with_capacity(workers);
    for (worker, stream) in streams.into_iter().enumerate() {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let deadline = Instant::now() + duration;
            let mut result = WorkerResult::default();
            let mut sequence = 1_usize;
            let mut operation = 0_u64;
            loop {
                let sampled = operation.is_multiple_of(LATENCY_SAMPLE_INTERVAL);
                let sample_started = sampled.then(Instant::now);
                match mode {
                    Mode::WriteRaw => {
                        if cache.add(worker, sequence, payload.clone()).await.is_ok() {
                            result.writes += 1;
                            sequence = sequence.saturating_add(1);
                        } else {
                            result.failures += 1;
                        }
                    }
                    Mode::WriteMapped => {
                        if cache
                            .add_for_stream_id(worker as u64 + 1, sequence, payload.clone())
                            .await
                            .is_ok()
                        {
                            result.writes += 1;
                            sequence = sequence.saturating_add(1);
                        } else {
                            result.failures += 1;
                        }
                    }
                    Mode::WriteHandle => {
                        if cache
                            .add_for_handle(
                                stream.expect("mapped stream handle"),
                                sequence,
                                payload.clone(),
                            )
                            .await
                            .is_ok()
                        {
                            result.writes += 1;
                            sequence = sequence.saturating_add(1);
                        } else {
                            result.failures += 1;
                        }
                    }
                    Mode::MixedHandle if operation.is_multiple_of(16) => {
                        if cache
                            .add_for_handle(
                                stream.expect("mapped stream handle"),
                                sequence,
                                payload.clone(),
                            )
                            .await
                            .is_ok()
                        {
                            result.writes += 1;
                            sequence = sequence.saturating_add(1);
                        } else {
                            result.failures += 1;
                        }
                    }
                    Mode::MixedHandle => {
                        let handle = stream.expect("mapped stream handle");
                        let hit = if let Some(last) = cache.last_for_handle(handle) {
                            cache.get_for_handle(handle, last).await
                        } else {
                            None
                        };
                        match hit {
                            Some((bytes, _)) if bytes.len() == PART_BYTES => result.reads += 1,
                            _ => result.failures += 1,
                        }
                    }
                    Mode::Churn => {
                        let stream_id = operation
                            .saturating_mul(workers as u64)
                            .saturating_add(worker as u64)
                            .saturating_add(1);
                        let handle = cache.resolve_or_create_stream(stream_id).await;
                        if cache
                            .add_for_handle(handle, 0, payload.clone())
                            .await
                            .is_ok()
                        {
                            result.writes += 1;
                        } else {
                            result.failures += 1;
                        }
                    }
                }
                if let Some(sample_started) = sample_started {
                    result.latency_ns.push(
                        sample_started
                            .elapsed()
                            .as_nanos()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                }
                operation += 1;
                if operation.is_multiple_of(DEADLINE_CHECK_INTERVAL) && Instant::now() >= deadline {
                    break;
                }
                if operation.is_multiple_of(COOPERATIVE_YIELD_INTERVAL) {
                    tokio::task::yield_now().await;
                }
            }
            result
        }));
    }

    reset_peak_live_bytes();
    let allocations_before = allocation_snapshot();
    let before = process_usage();
    barrier.wait().await;
    let started = Instant::now();
    let mut result = WorkerResult::default();
    for task in tasks {
        let worker = task.await.expect("workload worker completed");
        result.reads += worker.reads;
        result.writes += worker.writes;
        result.failures += worker.failures;
        result.latency_ns.extend(worker.latency_ns);
    }
    let elapsed = started.elapsed();
    let after = process_usage();
    let allocations = allocation_snapshot().since(allocations_before);
    let memory = cache.memory_stats().await;
    let operations = result.reads + result.writes;
    let seconds = elapsed.as_secs_f64();
    let cpu_seconds = (after.cpu_seconds - before.cpu_seconds).max(0.0);

    StepReport {
        workers,
        duration_seconds: seconds,
        reads: result.reads,
        writes: result.writes,
        failures: result.failures,
        operations_per_second: operations as f64 / seconds,
        logical_payload_gbit_per_second: operations as f64 * PART_BYTES as f64 * 8.0
            / seconds
            / 1e9,
        cpu_cores_used: cpu_seconds / seconds,
        cpu_nanoseconds_per_operation: cpu_seconds * 1e9 / operations.max(1) as f64,
        max_rss_platform_units: after.max_rss_platform_units,
        allocation_calls: allocations.allocation_calls,
        reallocation_calls: allocations.reallocation_calls,
        deallocation_calls: allocations.deallocation_calls,
        process_live_allocation_bytes: allocations.live_bytes,
        sharded_peak_live_allocation_bytes_upper_bound: allocations.peak_live_bytes,
        retained_cache_payload_bytes: memory.chunk_bytes + memory.initialization_bytes,
        maximum_cache_payload_bytes: memory.maximum_payload_bytes,
        sampled_operation_latency: percentiles(result.latency_ns),
    }
}

fn parse_args() -> (Duration, Mode) {
    let args = env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    let mut duration = 3.0;
    let mut mode = Mode::MixedHandle;
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
                    "write" | "write-handle" => Mode::WriteHandle,
                    "write-raw" => Mode::WriteRaw,
                    "write-mapped" => Mode::WriteMapped,
                    "mixed" | "mixed-handle" => Mode::MixedHandle,
                    "churn" => Mode::Churn,
                    _ => panic!(
                        "--mode must be write-raw, write-mapped, write-handle, mixed-handle, or churn"
                    ),
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
