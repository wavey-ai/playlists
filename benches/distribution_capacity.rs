use bytes::Bytes;
use playlists::{chunk_cache::ChunkCache, Options};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;
use tokio::sync::Barrier;

const STREAMS: usize = 2;
const RETAINED_PARTS: usize = 512;
const PCM_PART_BYTES: usize = 5_760;
const LATENCY_SAMPLE_INTERVAL: u64 = 4_096;

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
    steps: Vec<StepReport>,
}

fn main() {
    let duration_seconds = parse_duration_seconds();
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
        steps.push(runtime.block_on(run_step(workers, Duration::from_secs_f64(duration_seconds))));
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
        lookup: "ChunkCache::get_for_stream_id",
        payload_note:
            "logical Bytes length accessed; this is not network throughput or a memory copy",
        steps,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
}

async fn run_step(workers: usize, duration: Duration) -> StepReport {
    let cache = Arc::new(ChunkCache::new(Options {
        num_playlists: STREAMS,
        max_segments: 1,
        max_parts_per_segment: RETAINED_PARTS,
        buffer_size_kb: 8,
        part_target_ms: 5,
        ..Options::default()
    }));
    for stream in 0..STREAMS {
        for sequence in 0..RETAINED_PARTS {
            let mut payload = vec![0_u8; PCM_PART_BYTES];
            payload[..8].copy_from_slice(&(stream as u64).to_le_bytes());
            payload[8..16].copy_from_slice(&(sequence as u64).to_le_bytes());
            cache
                .add_for_stream_id(stream as u64 + 1, sequence, Bytes::from(payload))
                .await
                .expect("seed exact PCM part");
        }
    }

    let barrier = Arc::new(Barrier::new(workers + 1));
    let usage_before = process_usage();
    let deadline = Instant::now() + duration;
    let mut tasks = Vec::with_capacity(workers);
    for worker in 0..workers {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut result = WorkerResult::default();
            let mut sequence = worker % RETAINED_PARTS;
            let mut stream = worker % STREAMS;
            loop {
                let sampled = result.reads.is_multiple_of(LATENCY_SAMPLE_INTERVAL);
                let sample_started = sampled.then(Instant::now);
                match cache.get_for_stream_id(stream as u64 + 1, sequence).await {
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
                if (result.reads + result.failures).is_multiple_of(4_096)
                    && Instant::now() >= deadline
                {
                    break;
                }
            }
            result
        }));
    }

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
        sampled_lookup_latency: percentiles(result.latency_ns),
    }
}

fn parse_duration_seconds() -> f64 {
    let args = env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    match args.as_slice() {
        [] => 3.0,
        [flag, value] if flag == "--duration-seconds" => value
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite() && *value >= 0.25 && *value <= 300.0)
            .expect("--duration-seconds must be between 0.25 and 300"),
        _ => panic!("usage: distribution_capacity [--duration-seconds 3]"),
    }
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
