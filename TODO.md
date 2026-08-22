# Concurrency and Capacity Work

This document records the concurrency review from 2026-08-22 and its
implementation status.

The crate code tasks are complete. The final section lists downstream migration
work and tests that need a stable host or an HTTP service.

## Terms

- A **logical stream** has a caller-supplied `u64` stream ID.
- A **physical index** is one reusable position in the fixed stream capacity.
- A **generation** identifies one assignment of a physical index.
- A **stream handle** contains a cache identity, stream ID, physical index, and
  generation.
- A **mapped operation** resolves a logical stream ID for each operation.
- The **retained window** contains the segments and parts that the cache can
  still serve.
- RSS is the resident set size that the operating system reports.

## Implementation rules

- Treat the generation as part of every cache identity.
- Do not use a raw physical index as a durable logical identity.
- Put a hard bound on state that accepts churn or network input.
- Do not wait for capacity while you hold a map or slot lock.
- Resolve a stream once for a request or batch.
- Reuse its handle for repeated work.
- Keep `Bytes` payloads zero-copy on cache hits.
- Use deterministic barriers or held locks to test race order.
- Keep timed throughput work out of unit tests.

## P0: Correctness and bounded state

### [x] Isolate initialization data in `M3u8Cache`

The cache now stores `Option<StreamInitialization>` for each physical stream.
Each value contains the stream ID, generation, and `Bytes` payload.

Assignment clears the initialization while it holds the exclusive reuse gate.
Set operations validate a complete `M3u8StreamHandle` while they hold the shared
gate. Get operations validate the generation before and after the read.

The constructor does not allocate zero-filled initialization payloads. A missing
value returns `CacheError::StreamNotFound`.

Tests cover these conditions:

- A new stream has no initialization.
- A reused index does not expose the previous initialization.
- A stale writer cannot replace initialization for a new generation.
- A handle from a different cache cannot access initialization.

The crate has no HTTP layer. The HTTP service must map
`CacheError::StreamNotFound` to its documented missing-resource response.

### [x] Make writes safe across physical-index reuse

`StreamRegistry` owns all logical mappings and reverse ownership state. A map
entry contains one complete `ResolvedStream` value.

Each physical index has an owner `RwLock`. Assignment closes publication, waits
for the exclusive owner lock, advances the generation, and resets cache state.
Writes hold a shared owner lock through slot and publication changes.

`ChunkCache` keeps ID, generation, hash, and payload in one locked `ChunkSlot`.
`M3u8Cache` keeps identity, position, and encoded variants in one
`PlaylistSnapshot`.

Both caches reject a delayed writer from an old generation. They also reject a
delayed older position from the current generation. The latter case returns
`PutIfAbsentResult::Superseded` or `CacheError::Superseded`.

Mapped writes validate sizes and indices before they assign stream capacity.
An invalid write cannot evict a valid stream before it returns an error.

Deterministic tests hold a target slot lock, reassign the physical index, and
then release the old writer. The new generation remains readable.

### [x] Remove the raw-index identity hazard

`StreamHandle` and `M3u8StreamHandle` are the public identities for repeated
operations. Their identity fields are private.

Raw-index methods now operate only on physical indices that have no logical
owner. They reject an index that the registry assigned to a stream.

This behavior is an intentional compatibility restriction. Existing callers
must replace this sequence:

```text
get_or_create_stream_idx(stream_id) -> add/get(raw_index, ...)
```

with this sequence:

```text
resolve_or_create_stream(stream_id) -> add/get_for_handle(handle, ...)
```

Mesh synchronization and replica service use handles. A reassignment stops the
old response. It cannot send new-stream bytes under the old stream ID.

### [x] Publish playlist position as one value

`PlaylistPosition` contains segment, part, and sequence. The generation is in
the tagged snapshot and handle.

The cache publishes a complete snapshot only after it writes the ring slot.
The latest path reads the complete snapshot through `ArcSwapOption`.

Readers can see the complete old position or the complete new position. They
cannot combine fields from different writes.

### [x] Remove the unbounded new-playlist notification channel

The workspace had no user of `take_new_playlists_rx`. The implementation removes
the channel and its receiver API.

`stream_ids` and `stream_handles` are authoritative bounded snapshots. Stream
churn retains at most `num_playlists` mappings.

Exact-part waiters remain because request code uses them. Their keys include the
stream generation. The implementation uses 64 shards with a total limit of
65,536 retained keys.

An unknown-stream waiter does not create a mapping. This rule prevents a read
request from evicting a live stream.

### [x] Bound mesh state and define trust

The mesh now applies these configurable limits:

- known peers
- decoder peers and incomplete FEC objects
- decoded frame bytes
- completed-frame queue entries
- replica-request queue entries
- remote slot hints

The default incomplete-object and frame-queue limits are 64. The default frame
limit is 1 MiB.

Each limit has a drop, expiry, or eviction rule. `fec_stats` reports the related
counters.

The receive path rejects an unauthorized source before FEC state allocation.
It also rejects an oversized declared transfer before decoder allocation.

Configured peers are authorized automatically. Gossip can add only addresses in
`allowed_peers`. Edge nodes do not retain remote-forwarding slot hints.

Tests cover oversized declarations, unauthorized datagrams, peer limits,
incomplete-object limits, remote-slot limits, and ordered-expiry bounds.

## P1: Throughput and CPU

### [x] Remove the global map from repeated chunk reads

Handle reads use two lock-free generation checks and one slot read. They do not
lock the global stream map or the owner gate.

Mapped convenience methods still use one map lookup. `resolve_or_create_stream`
uses one lifecycle operation and O(1) reverse ownership state.

The generation checks use this publication protocol:

1. Assignment stores published generation zero with release ordering.
2. Assignment resets cache state while it holds the exclusive owner lock.
3. Assignment stores the stream ID.
4. Assignment publishes the new generation with release ordering.
5. A reader loads generation, stream ID, and generation with acquire ordering.

The second generation load rejects a transition that starts during the read.

### [x] Remove repeated map locks from `M3u8Cache`

Mapped access resolves one handle. Handle access calculates the physical slot
from that handle.

Explicit historical reads validate position and the tagged ring snapshot. Latest
full and delta reads use one `ArcSwapOption` load.

Tests cover direct hits, mapped misses, stale handles, cross-cache handles, and
index-reuse races. `playlist_cache` supplies the matching benchmark modes.

### [x] Shard playlist creation and rendering by stream

The global map stores `Arc<PlaylistEntry>`. Each entry has its own manifest
mutex and closing flag.

`Playlists::add` releases the map lock before it renders or compresses a
playlist. It keeps the entry lock through the cache commit.

`Playlists::fin` marks the exact entry as closing before it waits for that entry.
It clears cache state before it removes the same `Arc` from the map.

A deterministic test proves that an old `Arc` cannot resurrect a closed stream.
The `playlist_writes` benchmark compares independent streams with one hot stream.

### [x] Cache delta playlists on write

A write builds the full and delta variants from the raw manifest. It compresses
each available variant once and stores both variants in one snapshot.

A delta hit clones cached `Bytes`. It performs no decompression, parse, or
compression work.

Playlist writes use `Compression::fast`. A short local A/B check improved write
throughput by 32% to 48%. Retained encoded bytes increased by 5% to 9%.

These percentages are diagnostic samples. The release host must confirm the
CPU and memory tradeoff.

Equivalence tests cover skip boundaries and the absent-delta case. The
`playlist_cache` benchmark measures full and delta variants separately.

### [x] Bound manifest history and use constant-time base-time updates

`M3u8Manifest` stores completed segments in a bounded `VecDeque`. Each entry has
its logical ID, duration, and start time.

The manifest advances `open_start_time` when it removes an old duration. It does
not scan history to calculate the retained base time.

Sequence, segment, and part counters use `usize`. Duration totals use `u64`.
Saturating operations prevent integer wrap.

An open segment rolls when it reaches `max_parts_per_segment`. Rendition reports
and report URI lengths also have hard limits.

Tests cover 20,000 segment rotations and 10,000 non-independent input parts.

### [x] Re-evaluate nonblocking chunk slot reads

The selected path makes one `try_read` attempt. It waits on the fair Tokio lock
if that attempt fails. It does not spin.

A short local A/B check compared this path with an unconditional fair read. The
`try_read` path was 4.32x, 1.80x, 1.36x, and 1.04x faster at 1, 2, 4, and 8
workers.

The same short run reported 30.11, 48.35, 42.80, and 54.11 million handle reads
per second. These values are diagnostic samples, not release results.

The mixed benchmark also showed progress for readers and writers with no write
failures. A dedicated-host median is still required for a release claim.

## P2: Memory, layout, and mesh pipeline

### [x] Reuse the manifest render buffer

`M3u8Manifest` precomputes both invariant LL-HLS headers. The hot `add_part`
path clears one per-stream scratch buffer and writes changing fields into it.

Integer-backed durations and UTC timestamps write directly into the buffer.
Returned `Bytes` own one copy, so later renders cannot mutate earlier output.

The instrumented benchmark fell from 46 allocations and 10 reallocations per
write to one allocation and effectively no steady-state reallocations.

A one-second local diagnostic improved one-worker throughput by 45%. It
improved eight-worker throughput by 83%. Dedicated-host medians remain required
for release claims.

An exact-output fixture covers closed and open segments, byte ranges, preload
hints, and rendition reports. Timestamp tests compare direct formatting with
Chrono, including extended years and leap seconds.

### [x] Make latest-playlist read replication opt-in

The default latest-read path retains one payload. New cache and `Playlists`
constructors accept one through 64 independent latest-payload replicas.

Runtime threads select stable replicas without a request-time allocation.
Writes copy only the additional latest payloads. Historical ring entries remain
single-copy.

An immediate two-second local A/B compared one hot cached delta playlist. Eight
readers improved from 8.96 million to 38.62 million reads/s with eight replicas.
CPU cost fell from 279.9 to 116.5 ns/read.

Actual encoded payload increased from 911 to 7,304 bytes. The calculated maximum
increased by 5.47 MiB because the configured snapshot limit is 800 KiB.

The matching write diagnostic added nine allocations/write at eight replicas.
Its CPU cost increased by 9%. Keep one replica unless hot-read fan-out justifies
this write and memory cost.

The benchmark adds `handle-delta-replicated` and `cache-write-replicated` modes.
Tests cover exact variants, concurrent readers, memory accounting, and reuse.

### [x] Define and enforce the playlist memory budget

`buffer_size_kb` limits both raw manifest input and combined encoded snapshot
bytes. `init_size_kb` limits one initialization value.

`M3u8Cache::try_new` uses checked multiplication. It returns a configuration
error instead of an overflow panic.

Every snapshot, including segment zero, occupies a tagged ring slot. The latest
pointer shares that slot's `Arc` and does not duplicate payload bytes.

`memory_stats` reports retained encoded bytes and the calculated maximum. Tests
fill multiple ring rotations and verify that payload bytes plateau.

### [x] Remove eager playlist-slot zeroing

Stream reset and segment rollover do not write filler data to every slot. Tagged
stream, generation, segment, and part fields reject stale data.

Tests cover stale reads after stream reuse and ring wrap. The cache replaces a
slot only when a new snapshot uses that slot.

### [x] Group per-stream and per-slot state

`ChunkStreamState` groups last ID, next ID, and version. `PlaylistStreamState`
groups position, segment ranges, and version.

The chunk hash is in `ChunkSlot`. Playlist hashes are in the immutable snapshot.
The implementation removed separate sequentially-consistent slot atomics.

No explicit cache-line alignment was added. Current profiles do not show enough
false-sharing evidence to justify its memory cost.

### [x] Separate mesh receive work from replica service

The datagram task performs source authorization, FEC decode, frame decode, and a
bounded enqueue. It does not await a cache write or a replica range.

A frame task applies discovery and cache work. It sends replica requests to a
separate bounded task.

Both queues use `try_reserve`. Full queues drop new retryable work and increment
drop counters. Counters also record current depth, peak depth, decode time,
service attempts, service errors, and service time.

FEC observation and recovery state use ordered expiry queues. Cleanup runs every
256 datagrams and does not scan all observation maps.

Frame decoding takes ownership of `Bytes`. Chunk and initialization payloads are
zero-copy slices of the decoded frame.

## Benchmark work

### [x] Prevent scheduler phase locking

Latency sampling uses the prime interval 4,093. Cooperative yielding uses 4,096.
Deadline checks use 256 operations.

### [x] Move timed loops out of unit tests

The implementation removes all three timed throughput tests from
`src/chunk_cache.rs`. Deterministic fixed-operation concurrency tests remain.

### [x] Add permanent named scenarios

| Binary | Scenarios |
| --- | --- |
| `distribution_capacity` | raw, mapped, and handle chunk reads |
| `chunk_workloads` | raw, mapped, and handle writes; 15:1 mixed load; churn |
| `playlist_cache` | full and delta hits; misses; reuse race |
| `playlist_writes` | manifest render; cache write; independent and hot `Playlists::add` |
| `mesh_pipeline` | UDP/FEC receive; one-source recovery; replica range service |

Each report contains these measurements:

- wall time and operations per second
- process CPU time and CPU nanoseconds per operation
- p50, p95, p99, and maximum sampled latency
- allocator, reallocator, and deallocator call counts
- process live allocation bytes and the summed per-shard peak upper bound
- retained and maximum cache payload bytes
- platform `ru_maxrss`
- failures and queue drops where applicable

Allocator counters use 64 shards to reduce measurement contention. They still
add instrumentation cost. Use the same instrumentation for both comparisons.

## Maintenance

### [x] Clean strict Clippy findings

`CacheMeshRole` derives `Default`. The mesh pipeline refactor reduces the former
large handler argument sets.

`cargo clippy --all-features --all-targets -- -D warnings` is clean.

### [x] Refresh public performance documentation

The README removes old throughput claims that had incomplete test metadata. It
documents all permanent scenarios and the required result metadata.

Publish a number only after five dedicated-host samples. Include the commit,
Rust version, target, CPU, duration, mode, payload meaning, median, and range.

## Verification status

The local implementation checks have these results:

- [x] All-feature library tests pass: 100 tests.
- [x] The local all-feature, all-target release run passes.
- [x] Strict all-feature, all-target Clippy passes with warnings denied.
- [x] All benchmark targets compile in release mode.
- [x] Formatting and diff whitespace checks pass.
- [x] Deterministic reuse tests cover chunks, playlists, initialization, and
  replica reads.
- [x] Deterministic memory tests show a payload plateau after ring rotations.
- [x] Hostile mesh tests enforce configured state limits.

## External release qualification

These checks do not block this crate implementation. They block a production
performance or integration claim.

- [ ] Run `cargo test --all-features --all-targets --release` on the release
  toolchain and target.
- [ ] Run each applicable benchmark at least five times on a quiet dedicated
  host.
- [ ] Record the median and range for 1, 2, 4, and 8 workers.
- [ ] Confirm that handle throughput does not regress by more than 5%.
- [ ] Confirm that handle reads do not fall from 4 to 8 workers.
- [ ] Record mapped reads as a contended control. Do not use them as the
  high-concurrency release path.
- [ ] Confirm that independent `Playlists::add` does not fall from 4 to 8
  workers.
- [ ] Confirm that cached delta CPU cost is at most twice full-hit CPU cost.
- [ ] Run a long mesh churn and hostile-input RSS soak at configured limits.
- [ ] Run a long cache churn RSS soak after the retained windows are full.
- [ ] Add the downstream HTTP test for missing initialization and its 404
  mapping.
- [ ] Migrate active downstream users from numeric indices to `StreamHandle`.

Use this migration procedure:

1. Update `av-service/av-hls`, `av-contrib`, `io`, `av-mesh`, and
   `av-mesh-ops-telemetry`.
2. Replace `get_or_create_stream_idx` and `add_stream_id` with
   `resolve_or_create_stream`.
3. Replace `get_stream_idx` with `resolve_stream` when the stream must exist.
4. Store `StreamHandle` for a request, subscription, or ingest lifetime.
5. Replace `get`, `get_last`, `last`, and `version` with their `_for_handle`
   forms.
6. Replace `add` and `append` with `add_for_handle` and `append_for_handle`.
7. Use `zero_stream_id` for logical-stream teardown.
8. Keep raw-index operations only for fixed lanes that never use the stream
   registry.
9. Add a capacity-one reuse test to each migrated service.
10. Verify that an old request stops after another stream reuses its index.
