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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::Barrier;

use support::{allocation_snapshot, reset_peak_live_bytes};

const STREAMS: usize = 2;
const RETAINED_PARTS: usize = 512;
const PCM_PART_BYTES: usize = 5_760;
const LATENCY_SAMPLE_INTERVAL: u64 = 4_093;
const DEADLINE_CHECK_INTERVAL: u64 = 256;
const COOPERATIVE_YIELD_INTERVAL: u64 = 4_096;

#[derive(Clone, Copy)]
enum Lookup {
    Raw,
    Mapped,
    Handle,
}

impl Lookup {
    fn label(self) -> &'static str {
        match self {
            Self::Raw => "ChunkCache::get physical index",
            Self::Mapped => "ChunkCache::get_for_stream_id",
            Self::Handle => "ChunkCache::get_for_handle",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Usage {
    cpu_seconds: f64,
    voluntary_context_switches: i64,
    involuntary_context_switches: i64,
    max_rss_platform_units: i64,
}

#[derive(Default)]
struct WorkerResult {
    reads: u64,
    bytes: u64,
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
    failures: u64,
    reads_per_second: f64,
    logical_payload_bytes_per_second: f64,
    logical_payload_gbit_per_second: f64,
    cpu_seconds: f64,
    cpu_cores_used: f64,
    cpu_nanoseconds_per_read: f64,
    voluntary_context_switches: i64,
    involuntary_context_switches: i64,
    max_rss_platform_units: i64,
    allocation_calls: u64,
    reallocation_calls: u64,
    deallocation_calls: u64,
    process_live_allocation_bytes: u64,
    sharded_peak_live_allocation_bytes_upper_bound: u64,
    retained_cache_payload_bytes: usize,
    maximum_cache_payload_bytes: usize,
    sampled_lookup_latency: Percentiles,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    generated_unix_ms: u128,
    crate_name: &'static str,
    crate_version: &'static str,
    boundary: &'static str,
    target_os: &'static str,
    target_arch: &'static str,
    available_parallelism: usize,
    streams: usize,
    retained_parts_per_stream: usize,
    bytes_per_part: usize,
    lookup: &'static str,
    payload_note: &'static str,
    allocation_note: &'static str,
    steps: Vec<StepReport>,
}

fn main() {
    let (duration_seconds, lookup) = parse_args();
    let available_parallelism = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    let mut worker_counts = BTreeSet::from([1, 2, 4, available_parallelism]);
    worker_counts.retain(|workers| *workers <= available_parallelism && *workers > 0);

    let mut steps = Vec::new();
    for workers in worker_counts {
        let runtime = Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .expect("build Tokio runtime");
        steps.push(runtime.block_on(run_step(
            workers,
            Duration::from_secs_f64(duration_seconds),
            lookup,
        )));
    }

    let report = Report {
        schema: "needletail.playlists.distribution-capacity.v1",
        generated_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after Unix epoch")
            .as_millis(),
        crate_name: env!("CARGO_PKG_NAME"),
        crate_version: env!("CARGO_PKG_VERSION"),
        boundary: "B1_chunk_cache_exact_stream_part_hit",
        target_os: env::consts::OS,
        target_arch: env::consts::ARCH,
        available_parallelism,
        streams: STREAMS,
        retained_parts_per_stream: RETAINED_PARTS,
        bytes_per_part: PCM_PART_BYTES,
        lookup: lookup.label(),
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

async fn run_step(workers: usize, duration: Duration, lookup: Lookup) -> StepReport {
    let cache = Arc::new(ChunkCache::new(Options {
        num_playlists: STREAMS,
        max_segments: 1,
        max_parts_per_segment: RETAINED_PARTS,
        buffer_size_kb: 8,
        part_target_ms: 5,
        ..Options::default()
    }));
    let stream_handles = if matches!(lookup, Lookup::Raw) {
        for stream in 0..STREAMS {
            for sequence in 0..RETAINED_PARTS {
                let payload = benchmark_payload(stream, sequence);
                cache
                    .add(stream, sequence, payload)
                    .await
                    .expect("seed exact physical-index PCM part");
            }
        }
        Arc::<[Option<StreamHandle>]>::from(vec![None; STREAMS])
    } else {
        for stream in 0..STREAMS {
            for sequence in 0..RETAINED_PARTS {
                let payload = benchmark_payload(stream, sequence);
                cache
                    .add_for_stream_id(stream as u64 + 1, sequence, payload)
                    .await
                    .expect("seed exact mapped PCM part");
            }
        }
        Arc::<[Option<StreamHandle>]>::from(
            (0..STREAMS)
                .map(|stream| cache.resolve_stream(stream as u64 + 1))
                .collect::<Vec<_>>(),
        )
    };

    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut tasks = Vec::with_capacity(workers);
    for worker in 0..workers {
        let cache = Arc::clone(&cache);
        let stream_handles = Arc::clone(&stream_handles);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let deadline = Instant::now() + duration;
            let mut result = WorkerResult::default();
            let mut sequence = worker % RETAINED_PARTS;
            let mut stream = worker % STREAMS;
            loop {
                let sampled = result.reads.is_multiple_of(LATENCY_SAMPLE_INTERVAL);
                let sample_started = sampled.then(Instant::now);
                let hit = match lookup {
                    Lookup::Raw => cache.get(stream, sequence).await,
                    Lookup::Mapped => cache.get_for_stream_id(stream as u64 + 1, sequence).await,
                    Lookup::Handle => {
                        cache
                            .get_for_handle(
                                stream_handles[stream].expect("mapped stream handle"),
                                sequence,
                            )
                            .await
                    }
                };
                match hit {
                    Some((bytes, _hash)) if bytes.len() == PCM_PART_BYTES => {
                        result.reads += 1;
                        result.bytes += bytes.len() as u64;
                    }
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
                sequence += 1;
                if sequence == RETAINED_PARTS {
                    sequence = 0;
                    stream = (stream + 1) % STREAMS;
                }
                let operations = result.reads + result.failures;
                if operations.is_multiple_of(DEADLINE_CHECK_INTERVAL) && Instant::now() >= deadline
                {
                    break;
                }
                if operations.is_multiple_of(COOPERATIVE_YIELD_INTERVAL) {
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
    let mut result = WorkerResult::default();
    for task in tasks {
        let worker = task.await.expect("capacity worker completed");
        result.reads += worker.reads;
        result.bytes += worker.bytes;
        result.failures += worker.failures;
        result.latency_ns.extend(worker.latency_ns);
    }
    let elapsed = started.elapsed();
    let usage_after = process_usage();
    let allocations = allocation_snapshot().since(allocations_before);
    let memory = cache.memory_stats().await;
    let cpu_seconds = (usage_after.cpu_seconds - usage_before.cpu_seconds).max(0.0);
    let elapsed_seconds = elapsed.as_secs_f64();

    StepReport {
        workers,
        duration_seconds: elapsed_seconds,
        reads: result.reads,
        failures: result.failures,
        reads_per_second: result.reads as f64 / elapsed_seconds,
        logical_payload_bytes_per_second: result.bytes as f64 / elapsed_seconds,
        logical_payload_gbit_per_second: result.bytes as f64 * 8.0 / elapsed_seconds / 1e9,
        cpu_seconds,
        cpu_cores_used: cpu_seconds / elapsed_seconds,
        cpu_nanoseconds_per_read: cpu_seconds * 1e9 / result.reads.max(1) as f64,
        voluntary_context_switches: usage_after.voluntary_context_switches
            - usage_before.voluntary_context_switches,
        involuntary_context_switches: usage_after.involuntary_context_switches
            - usage_before.involuntary_context_switches,
        max_rss_platform_units: usage_after.max_rss_platform_units,
        allocation_calls: allocations.allocation_calls,
        reallocation_calls: allocations.reallocation_calls,
        deallocation_calls: allocations.deallocation_calls,
        process_live_allocation_bytes: allocations.live_bytes,
        sharded_peak_live_allocation_bytes_upper_bound: allocations.peak_live_bytes,
        retained_cache_payload_bytes: memory.chunk_bytes + memory.initialization_bytes,
        maximum_cache_payload_bytes: memory.maximum_payload_bytes,
        sampled_lookup_latency: percentiles(result.latency_ns),
    }
}

fn benchmark_payload(stream: usize, sequence: usize) -> Bytes {
    let mut payload = vec![0_u8; PCM_PART_BYTES];
    payload[..8].copy_from_slice(&(stream as u64).to_le_bytes());
    payload[8..16].copy_from_slice(&(sequence as u64).to_le_bytes());
    Bytes::from(payload)
}

fn parse_args() -> (f64, Lookup) {
    let args = env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    let mut duration_seconds = 3.0;
    let mut lookup = Lookup::Mapped;
    let mut index = 0;
    while index < args.len() {
        let value = args
            .get(index + 1)
            .unwrap_or_else(|| panic!("missing value for {}", args[index]));
        match args[index].as_str() {
            "--duration-seconds" => {
                duration_seconds = value
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite() && *value >= 0.25 && *value <= 300.0)
                    .expect("--duration-seconds must be between 0.25 and 300");
            }
            "--lookup" => {
                lookup = match value.as_str() {
                    "raw" => Lookup::Raw,
                    "mapped" => Lookup::Mapped,
                    "handle" => Lookup::Handle,
                    _ => panic!("--lookup must be raw, mapped, or handle"),
                };
            }
            flag => panic!("unknown argument {flag}"),
        }
        index += 2;
    }
    (duration_seconds, lookup)
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
    let timeval_seconds =
        |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    Usage {
        cpu_seconds: timeval_seconds(raw.ru_utime) + timeval_seconds(raw.ru_stime),
        voluntary_context_switches: raw.ru_nvcsw,
        involuntary_context_switches: raw.ru_nivcsw,
        max_rss_platform_units: raw.ru_maxrss,
    }
}
