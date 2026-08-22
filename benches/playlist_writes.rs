mod support;

use access_unit::Fmp4;
use bytes::Bytes;
use playlists::{m3u8_cache::M3u8Cache, m3u8_manifest::M3u8Manifest, Options, Playlists};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use support::{allocation_snapshot, reset_peak_live_bytes};

const LATENCY_SAMPLE_INTERVAL: u64 = 4_093;
const DEADLINE_CHECK_INTERVAL: u64 = 256;
const COOPERATIVE_YIELD_INTERVAL: u64 = 4_096;
const MEDIA_BYTES: usize = 5_760;

#[derive(Clone, Copy)]
enum Mode {
    ManifestRender,
    CacheWrite,
    PlaylistsIndependent,
    PlaylistsHot,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::ManifestRender => "manifest_render_independent_streams",
            Self::CacheWrite => "manifest_render_and_m3u8_cache_write",
            Self::PlaylistsIndependent => "playlists_add_independent_streams",
            Self::PlaylistsHot => "playlists_add_one_hot_stream",
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Usage {
    cpu_seconds: f64,
    max_rss_platform_units: i64,
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
struct Report {
    schema: &'static str,
    mode: &'static str,
    target_os: &'static str,
    target_arch: &'static str,
    available_parallelism: usize,
    media_bytes_per_part: usize,
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
        schema: "needletail.playlists.playlist-writes.v1",
        mode: mode.label(),
        target_os: env::consts::OS,
        target_arch: env::consts::ARCH,
        available_parallelism,
        media_bytes_per_part: MEDIA_BYTES,
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
        num_playlists: workers,
        max_segments: 8,
        max_parts_per_segment: 64,
        segment_min_ms: 5,
        target_duration_ms: 1_000,
        part_target_ms: 5,
        buffer_size_kb: 64,
        ..Options::default()
    };
    let cache = Arc::new(M3u8Cache::new(options));
    let (playlists, _chunks, playlists_cache) = Playlists::new(options);
    let handles = (0..workers)
        .map(|worker| {
            cache
                .resolve_or_create_stream(worker as u64 + 1)
                .expect("create benchmark stream")
        })
        .collect::<Vec<_>>();
    let payload = Bytes::from(vec![0x5a; MEDIA_BYTES]);
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut threads = Vec::with_capacity(workers);

    for (worker, handle) in handles.into_iter().enumerate() {
        let cache = Arc::clone(&cache);
        let playlists = Arc::clone(&playlists);
        let barrier = Arc::clone(&barrier);
        let payload = payload.clone();
        threads.push(std::thread::spawn(move || {
            let mut manifest = M3u8Manifest::new(options);
            barrier.wait();
            let deadline = Instant::now() + duration;
            let mut operations = 0_u64;
            let mut failures = 0_u64;
            let mut latency_ns = Vec::new();

            loop {
                let sampled = (operations + failures).is_multiple_of(LATENCY_SAMPLE_INTERVAL);
                let sample_started = sampled.then(Instant::now);
                let valid = match mode {
                    Mode::ManifestRender => {
                        let (playlist, ..) = manifest.add_part(5, true);
                        black_box(playlist);
                        true
                    }
                    Mode::CacheWrite => {
                        let (playlist, segment_id, sequence, part_idx, _) =
                            manifest.add_part(5, true);
                        cache
                            .add_for_handle(handle, segment_id, sequence, part_idx, playlist)
                            .is_ok()
                    }
                    Mode::PlaylistsIndependent | Mode::PlaylistsHot => {
                        let stream_id = if matches!(mode, Mode::PlaylistsHot) {
                            1
                        } else {
                            worker as u64 + 1
                        };
                        playlists.add(
                            stream_id,
                            Fmp4 {
                                init: None,
                                key: true,
                                data: payload.clone(),
                                duration: 5,
                            },
                        )
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
        let worker = thread.join().expect("playlist write worker completed");
        operations += worker.0;
        failures += worker.1;
        latency_ns.extend(worker.2);
    }
    let elapsed = started.elapsed();
    let after = process_usage();
    let allocations = allocation_snapshot().since(allocations_before);
    let (retained_encoded_payload_bytes, maximum_cache_payload_bytes) =
        if matches!(mode, Mode::CacheWrite) {
            let memory = cache.memory_stats();
            (
                memory.encoded_playlist_bytes + memory.initialization_bytes,
                memory.maximum_payload_bytes,
            )
        } else if matches!(mode, Mode::PlaylistsIndependent | Mode::PlaylistsHot) {
            let memory = playlists_cache.memory_stats();
            (
                memory.encoded_playlist_bytes + memory.initialization_bytes,
                memory.maximum_payload_bytes,
            )
        } else {
            (0, 0)
        };
    let seconds = elapsed.as_secs_f64();
    let cpu_seconds = (after.cpu_seconds - before.cpu_seconds).max(0.0);

    StepReport {
        workers,
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
        retained_encoded_payload_bytes,
        maximum_cache_payload_bytes,
        sampled_operation_latency: percentiles(latency_ns),
    }
}

fn parse_args() -> (Duration, Mode) {
    let args = env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    let mut duration = 3.0;
    let mut mode = Mode::PlaylistsIndependent;
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
                    "manifest-render" => Mode::ManifestRender,
                    "cache-write" => Mode::CacheWrite,
                    "playlists-independent" => Mode::PlaylistsIndependent,
                    "playlists-hot" => Mode::PlaylistsHot,
                    _ => panic!(
                        "--mode must be manifest-render, cache-write, playlists-independent, or playlists-hot"
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
