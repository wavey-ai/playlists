# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased] - 2026-11-21

### Changed

- `Options` holds PART-TARGET in microseconds (`part_target_us`, default 2500 µs); `part_target_ms` is kept and wins when non-zero. New `part_hold_back_ms` (a floor for PART-HOLD-BACK), `window_ms` (default 12 s), `parted_target_durations` (default 3) and `eager_render` (default false). Defaults: 1 s segments, TARGETDURATION 1, ring counts derived from time when zero.
- Playlist durations are rendered exactly from microseconds as decimal seconds (up to six decimals, at least three); the delta-update parser reads them exactly.
- EXT-X-PART lines only for segments within three target durations of the end, and at most `max_parted_segments` when it is set.
- A part is added to a playlist in constant time; `M3u8Cache` renders and compresses a stream's playlist when it is read, once per new part, instead of storing a gzip snapshot for every part. `M3u8Cache::add_part_us`, `M3u8Cache::last_text`, `Playlists::add_us`, `M3u8Manifest::push_part_us` and `m3u8_manifest::{format_seconds_us, parse_seconds_us}` are new.

## [Unreleased] - 2026-06-02

### Fixed

- Allowed contiguous live publication to resume at a real retained object after an unresolved gap has been evicted from the rolling window.
- Rejected stale `ChunkCache` reads after ring-buffer wraparound by validating per-slot logical ids and stream generations.
- Prevented reused stream slots from exposing data from the previous stream in both `ChunkCache` and `M3u8Cache`.
- Made `ChunkCache::append` safe for concurrent writers by reserving logical ids atomically and publishing `last` monotonically.
- Replaced chunk-cache stream clearing with generation invalidation to avoid teardown-time zero-fill spikes.
- Guarded invalid `stream_idx` and zero-valued options from panics.
- Fixed truncated `M3u8Cache` packet validation so malformed packets return an error instead of panicking.
- Bounded mesh replica sync and on-demand replica responses to the retained ring-buffer window.

### Changed

- Updated the massive-read contention benchmark to read valid retained-window slots instead of counting overwritten stale slots as hits.
- Reduced `Playlists::add` global mutex hold time before cache/compression work and made `fin` clear chunk mappings synchronously.
- Documented the `ChunkCache` slot metadata layout.

### Tests

- Added a retained-window regression that holds publication behind a live gap and verifies bounded recovery only after that gap is evicted.
- Added regressions for stale logical ids, stream-slot reuse, invalid stream indices, concurrent append id uniqueness, and truncated playlist packets.
- Verified `cargo test`, `cargo clippy --all-targets -- -D warnings`, and the massive-read contention benchmark.

## [0.2.0] - 2026-01-03

### Changed

- Renamed `fmp4_cache` module to `chunk_cache` for more generic naming
- Renamed `Fmp4Cache` struct to `ChunkCache`
- Renamed `Playlists.fmp4_cache` field to `Playlists.chunk_cache`
- Updated README to reflect new naming

### Migration

Replace all occurrences:
- `playlists::fmp4_cache::Fmp4Cache` → `playlists::chunk_cache::ChunkCache`
- `fmp4_cache` field access → `chunk_cache`

## [0.1.1] - Previous

- Initial release with `Fmp4Cache`, `M3u8Cache`, and `M3u8Manifest`
