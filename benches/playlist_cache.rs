mod support;

use playlists::{m3u8_cache::M3u8Cache, m3u8_manifest::M3u8Manifest, Options};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use support::{allocation_snapshot, reset_peak_live_bytes};

const LATENCY_SAMPLE_INTERVAL: u64 = 4_093;
const DEADLINE_CHECK_INTERVAL: u64 = 256;
const COOPERATIVE_YIELD_INTERVAL: u64 = 4_096;

#[derive(Clone, Copy)]
enum Mode {
    MappedFull,
    HandleFull,
    MappedDelta,
    HandleDelta,
    HandleDeltaReplicated,
    MappedMiss,
    HandleMiss,
    ReuseRace,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::MappedFull => "mapped_full_hit",
            Self::HandleFull => "handle_full_hit",
            Self::MappedDelta => "mapped_cached_delta_hit",
            Self::HandleDelta => "handle_cached_delta_hit",
            Self::HandleDeltaReplicated => "handle_cached_delta_hit_replicated_per_worker",
            Self::MappedMiss => "mapped_miss",
            Self::HandleMiss => "handle_miss",
            Self::ReuseRace => "index_reuse_resolve_and_write_race",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Usage {
    cpu_seconds: f64,
    max_rss_platform_units: i64,
}

#[derive(Serialize)]
struct StepReport {
    workers: usize,
    latest_read_replicas: usize,
    duration_seconds: f64,
    operations: u64,
    failures: u64,
    operations_per_second: f64,
    cpu_cores_used: f64,
    cpu_nanoseconds_per_operation: f64,
    max_rss_platform_units: i64,
    allocation_calls: u64,
    reallocation_calls: u64,
    deallocation_calls: u64,
    process_live_allocation_bytes: u64,
    sharded_peak_live_allocation_bytes_upper_bound: u64,
    retained_encoded_payload_bytes: usize,
    maximum_cache_payload_bytes: usize,
    sampled_operation_latency: Percentiles,
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
struct Report {
    schema: &'static str,
    mode: &'static str,
    target_os: &'static str,
    target_arch: &'static str,
    available_parallelism: usize,
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
    let steps = worker_counts
        .into_iter()
        .map(|workers| run_step(workers, duration, mode))
        .collect();
    let report = Report {
        schema: "needletail.playlists.playlist-cache.v1",
        mode: mode.label(),
        target_os: env::consts::OS,
        target_arch: env::consts::ARCH,
        available_parallelism,
        allocation_note:
            "uses 64 counter shards; live bytes include thread and cache state; summed shard peaks are an upper bound",
        steps,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
}

fn run_step(workers: usize, duration: Duration, mode: Mode) -> StepReport {
    let options = Options {
        num_playlists: 1,
        max_segments: 32,
        max_parts_per_segment: 128,
        segment_min_ms: 1_000,
        target_duration_ms: 1_000,
        part_target_ms: 1_000,
        ..Options::default()
    };
    let latest_read_replicas = if matches!(mode, Mode::HandleDeltaReplicated) {
        workers
    } else {
        1
    };
    let cache = Arc::new(M3u8Cache::new_with_latest_read_replicas(
        options,
        latest_read_replicas,
    ));
    let mut manifest = M3u8Manifest::new(options);
    let mut latest = None;
    for _ in 0..40 {
        latest = Some(manifest.add_part(1_000, true));
    }
    let (playlist, segment_id, sequence, part_idx, _) = latest.unwrap();
    let reuse_playlist = playlist.clone();
    cache
        .add(1, segment_id, sequence, part_idx, playlist)
        .expect("seed playlist snapshot");
    let handle = cache.resolve_stream(1).expect("seeded stream handle");

    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut threads = Vec::with_capacity(workers);
    for worker in 0..workers {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        let reuse_playlist = reuse_playlist.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            let deadline = Instant::now() + duration;
            let mut operations = 0_u64;
            let mut failures = 0_u64;
            let mut latency_ns = Vec::new();
            loop {
                let sampled = (operations + failures).is_multiple_of(LATENCY_SAMPLE_INTERVAL);
                let sample_started = sampled.then(Instant::now);
                let valid = match mode {
                    Mode::MappedFull => cache.last(1).is_ok_and(|value| value.is_some()),
                    Mode::HandleFull => cache
                        .last_for_handle(handle)
                        .is_ok_and(|value| value.is_some()),
                    Mode::MappedDelta => cache.last_delta(1).is_ok_and(|value| value.is_some()),
                    Mode::HandleDelta => cache
                        .last_delta_for_handle(handle)
                        .is_ok_and(|value| value.is_some()),
                    Mode::HandleDeltaReplicated => cache
                        .last_delta_for_handle(handle)
                        .is_ok_and(|value| value.is_some()),
                    Mode::MappedMiss => cache
                        .get(1, segment_id.saturating_add(1), 0)
                        .is_ok_and(|value| value.is_none()),
                    Mode::HandleMiss => cache
                        .get_for_handle(handle, segment_id.saturating_add(1), 0)
                        .is_ok_and(|value| value.is_none()),
                    Mode::ReuseRace => {
                        let operation = operations.saturating_add(failures);
                        let stream_id = operation
                            .saturating_mul(workers as u64)
                            .saturating_add(worker as u64)
                            .saturating_add(2);
                        cache
                            .resolve_or_create_stream(stream_id)
                            .and_then(|current| {
                                cache.add_for_handle(
                                    current,
                                    1,
                                    operation as usize,
                                    0,
                                    reuse_playlist.clone(),
                                )
                            })
                            .is_ok()
                    }
                };
                if valid {
                    operations += 1;
                } else {
                    failures += 1;
                }
                if let Some(sample_started) = sample_started {
                    latency_ns.push(
                        sample_started
                            .elapsed()
                            .as_nanos()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                }
                let completed = operations + failures;
                if completed.is_multiple_of(DEADLINE_CHECK_INTERVAL) && Instant::now() >= deadline {
                    break;
                }
                if completed.is_multiple_of(COOPERATIVE_YIELD_INTERVAL) {
                    std::thread::yield_now();
                }
            }
            (operations, failures, latency_ns)
        }));
    }
    reset_peak_live_bytes();
    let allocations_before = allocation_snapshot();
    let before = process_usage();
    barrier.wait();
    let started = Instant::now();
    let mut operations = 0_u64;
    let mut failures = 0_u64;
    let mut latency_ns = Vec::new();
    for thread in threads {
        let worker = thread.join().expect("playlist cache worker completed");
        operations += worker.0;
        failures += worker.1;
        latency_ns.extend(worker.2);
    }
    let elapsed = started.elapsed();
    let after = process_usage();
    let allocations = allocation_snapshot().since(allocations_before);
    let memory = cache.memory_stats();
    let seconds = elapsed.as_secs_f64();
    let cpu_seconds = (after.cpu_seconds - before.cpu_seconds).max(0.0);

    StepReport {
        workers,
        latest_read_replicas,
        duration_seconds: seconds,
        operations,
        failures,
        operations_per_second: operations as f64 / seconds,
        cpu_cores_used: cpu_seconds / seconds,
        cpu_nanoseconds_per_operation: cpu_seconds * 1e9 / operations.max(1) as f64,
        max_rss_platform_units: after.max_rss_platform_units,
        allocation_calls: allocations.allocation_calls,
        reallocation_calls: allocations.reallocation_calls,
        deallocation_calls: allocations.deallocation_calls,
        process_live_allocation_bytes: allocations.live_bytes,
        sharded_peak_live_allocation_bytes_upper_bound: allocations.peak_live_bytes,
        retained_encoded_payload_bytes: memory.encoded_playlist_bytes + memory.initialization_bytes,
        maximum_cache_payload_bytes: memory.maximum_payload_bytes,
        sampled_operation_latency: percentiles(latency_ns),
    }
}

fn parse_args() -> (Duration, Mode) {
    let args = env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    let mut duration = 3.0;
    let mut mode = Mode::HandleDelta;
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
                    "mapped-full" => Mode::MappedFull,
                    "handle-full" => Mode::HandleFull,
                    "mapped-delta" => Mode::MappedDelta,
                    "handle-delta" => Mode::HandleDelta,
                    "handle-delta-replicated" => Mode::HandleDeltaReplicated,
                    "mapped-miss" => Mode::MappedMiss,
                    "handle-miss" => Mode::HandleMiss,
                    "reuse-race" => Mode::ReuseRace,
                    _ => panic!(
                        "--mode must be mapped-full, handle-full, mapped-delta, handle-delta, handle-delta-replicated, mapped-miss, handle-miss, or reuse-race"
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
