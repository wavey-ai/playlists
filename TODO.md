# Concurrency and Capacity TODO

This document records the concurrency and capacity review from 2026-08-22. It covers correctness, read and write throughput, CPU use, and memory use.

The measured values are local baselines. They are not service-level guarantees. Repeat each benchmark on a dedicated host before you set a release target.

## Terms

- A **logical stream** is the stream that a caller identifies with a `u64` stream ID.
- A **physical index** is a reusable slot in a fixed-size cache.
- A **generation** is the version of one physical index. It changes when the cache assigns that index to another logical stream.
- A **stream handle** is an opaque value that contains a stream ID, a physical index, and a generation.
- A **mapped operation** resolves a logical stream ID for each cache operation.
- An **indexed operation** uses a physical index directly.
- The **retained window** is the configured set of segments and parts that the cache can still serve.
- RSS means resident set size. It is the memory that the operating system reports as resident for the process.

## Implementation rules

- Fix data isolation before throughput.
- Treat the generation as part of every cache identity.
- Do not use a raw physical index as a stable logical identity.
- Put a hard bound on every queue and collection that can grow from stream churn or network input.
- Do not wait for capacity while holding a map or cache-slot lock.
- Resolve a stream once per request or batch. Reuse its handle for the rest of that operation.
- Keep `Bytes` payloads zero-copy on cache hits. A `Bytes` clone copies metadata, not the payload.
- Measure one material change at a time. Record throughput, CPU time, allocation count, and RSS.
- Keep correctness tests deterministic. Use barriers or test hooks to force race order.

## P0: Correctness and bounded state

P0 work blocks a production claim for safe high-concurrency use.

### [ ] Isolate initialization data in `M3u8Cache`

`M3u8Cache::reset_stream_idx` resets position and generation state. It does not reset `inits`. `set_init` and `get_init` store only `Bytes`, so a reused physical index exposes the previous stream's initialization bytes.

A new stream also receives the preallocated zero-filled initialization value. The API cannot distinguish this value from a real initialization.

Implement this change as follows:

1. Replace each `RwLock<Bytes>` entry in `inits` with `RwLock<Option<StreamInitialization>>`.
2. Store `stream_id`, `generation`, and `Bytes` in `StreamInitialization`.
3. Clear the initialization while the physical index is reset.
4. Resolve one generation-bearing handle before `set_init` or `get_init` accesses the slot.
5. Hold the stream reuse guard while `set_init` validates the handle and writes the value.
6. Return `None` or a specific missing-initialization error unless both the stream ID and generation match.
7. Remove the zero-filled initialization allocation from `M3u8Cache::new`.

Add these tests:

- A new stream has no initialization.
- A physical index does not expose initialization from its previous stream.
- An old writer cannot replace the initialization for a new generation.
- The HTTP integration returns `404` or its documented equivalent when initialization is absent.

### [ ] Make cache writes safe across physical-index reuse

`ChunkCache::set_with_generation` writes a slot before `add_with_generation` checks the current generation. A delayed writer for an old generation can therefore replace data from a new generation. The later error does not restore the new data.

`ChunkCache::put_if_absent_with_generation` has the same race. `M3u8Cache::add` also calculates a physical slot before its final generation check, so it can commit stale data after reassignment.

Use this lifecycle design:

1. Add an opaque `StreamHandle` with private `stream_id`, `index`, and `generation` fields.
2. Add `resolve_stream(stream_id)` and `resolve_or_create_stream(stream_id)` methods.
3. Store the generation in the map entry. Return the complete handle from one map lookup.
4. Add a reuse gate for each physical index. A normal operation holds a shared permit. Reassignment holds an exclusive permit.
5. Validate the complete handle after the shared permit is acquired and before a slot changes.
6. Keep the shared permit until the slot write and its publication metadata are complete.
7. Advance the generation and reset the slot while the exclusive permit is held.
8. Update the forward and reverse mappings as one assignment operation.
9. Never hold the global map lock while waiting for an asynchronous permit.

The lock order must be consistent. Assignment should use an assignment lock, then the per-index exclusive reuse permit, and then the short map write. Normal operations should resolve the map, release the map lock, acquire the shared reuse permit, and validate the handle.

Consider one lock-protected slot value with `id`, `generation`, `hash`, and `Bytes`. This layout makes payload and identity changes atomic to readers. Measure it against the current separate atomics before adoption.

Add deterministic tests for both caches. Pause an old writer after handle resolution. Reassign the physical index and complete a new write. Resume the old writer. The old write must fail, and the new bytes must remain readable.

### [ ] Remove the raw-index identity hazard

`ChunkCache::get(stream_idx, id)` loads the current generation. A caller that retained an old physical index can therefore read bytes for a different logical stream after index reuse.

The mesh replica path resolves an index once and then uses the direct indexed read in a loop. Reassignment can make that response contain bytes from a new stream while the response still names the old stream.

Make the stream handle the public identity for repeated cache access. Require `get(handle, id)`, `add(handle, id, bytes)`, and initialization operations to validate the handle. Keep a raw-index API only for internal code that owns the physical index lifecycle. Deprecate the public raw-index API if compatibility permits.

Add a mesh regression test. Reassign an index during a replica response. The response must stop or fail. It must never return data from the new stream under the old stream ID.

### [ ] Publish playlist position as one consistent value

`M3u8Cache` stores `last_seg`, `last_part`, and `last_seq` in separate atomics. A reader can observe fields from different writes.

Store `segment`, `part`, `sequence`, and `generation` in one per-stream state value. Protect the value with the stream lock or use a versioned retry protocol. Publish the state only after the data slot is complete.

Start with the lock-protected design. Use a versioned atomic design only if profiling shows that the lock is material.

Add a test hook between field updates in the old flow. Verify that the new implementation returns either the old complete position or the new complete position.

### [ ] Bound new-playlist notifications

`ChunkCache` owns an unbounded channel for new-playlist notifications. The crate has no production consumer of `take_new_playlists_rx`.

A local churn probe queued every notification. It increased maximum RSS by about 2.13 MB for 100,000 assignments and 20.63 MB for 1,000,000 assignments.

First audit downstream users of the public receiver. Remove the channel if no downstream user needs it.

If the event remains, treat it as a bounded hint:

1. Use a bounded channel.
2. Send after the map lock is released.
3. Use `try_send` so a missing or slow consumer cannot block cache assignment.
4. Set a `rescan_required` flag when the channel is full.
5. Make the consumer reconcile with the authoritative `stream_ids()` snapshot after a gap.
6. Document lag and duplicate-event behavior.

Add a churn test with no consumer and a slow consumer. Queue memory must stay within its configured bound. The consumer must converge after reconciliation.

### [ ] Bound mesh state and validate trust assumptions

The mesh feature is marked as a prototype. It still needs explicit limits before production use.

Known unbounded or stale state includes `remote_slots`, `sent`, `sent_initializations`, peer state, and decoder state. Edge nodes record `remote_slots`, but their sync task returns before the current pruning code.

Implement these changes:

- Do not record `remote_slots` on an Edge node, or prune it in the receive path.
- Prune `sent` and `sent_initializations` against the current stream handles and peer set on every sync tick.
- Limit peers, active decoders, in-flight frames, decoded payload size, and completed-frame queues.
- Define an eviction rule and a metric for every limit.
- Require authentication or an allowlist for production peers. Do not accept arbitrary frames when `same_region_only` is false.

Run churn and hostile-input tests. Memory must plateau at the configured limits.

## P1: Throughput and CPU

### [ ] Remove the global map from the repeated read path

Mapped reads contend on `ChunkCache::offsets`. The contention dominates at higher worker counts. The same map also causes two lookups on the common `add_for_stream_id` path.

The first implementation should use the generation-bearing stream handle from P0:

1. Resolve the stream once at the request or batch boundary.
2. Pass the handle through all repeated reads and writes.
3. Make `resolve_or_create_stream` return a handle from one map operation.
4. Add `Vec<Option<u64>>` reverse ownership state. Remove the current full map scan during physical-index reuse.
5. Keep mapped convenience methods for one-shot calls.

After this change, benchmark the remaining map. Consider a sharded map or a read-optimized map only if handle resolution remains material.

The local read baseline shows the current cost:

| Workers | Mapped reads/s | Indexed reads/s | Indexed/mapped |
| ---: | ---: | ---: | ---: |
| 1 | 6.601 M | 7.880 M | 1.19x |
| 2 | 14.296 M | 36.531 M | 2.56x |
| 4 | 11.266 M | 45.704 M | 4.06x |
| 8 | 3.320 M | 61.168 M | 18.42x |

The test used two streams and 5,760-byte parts. Each hit cloned a `Bytes` value. The byte rate is a logical payload rate, not a memory-copy or network rate.

### [ ] Remove repeated map locks from `M3u8Cache::get`

One hit can acquire the offsets lock several times through `is_included`, position helpers, `offset_and_generation`, and index calculation.

Resolve one handle. Load one consistent position. Calculate the physical slot from the handle's index. Read the slot. Validate its segment, part, generation, and stream identity before return.

Add separate benchmarks for a direct handle hit, a mapped hit, a miss, and an index-reuse race.

### [ ] Shard playlist creation and rendering by stream

`Playlists::add` holds one global `Mutex<BTreeMap<...>>` while it mutates and renders a manifest. Writes to unrelated streams therefore serialize.

Change the map value to an `Arc<PlaylistEntry>`. Give each entry its own manifest lock, generation, and live or closed state.

Use this operation order:

1. Hold the map lock only to find or create an entry and update the active count.
2. Release the map lock.
3. Lock only the selected entry.
4. Confirm that the entry is live.
5. Update and render the manifest.
6. Write the rendered value with the same stream handle.

`fin` must remove the exact entry from the map and mark that entry closed. It must then wait for or reject an in-flight add before cache cleanup. An add that already owns an old `Arc` must not resurrect a closed generation.

Add a benchmark with independent streams and one writer per stream. Add deterministic `add` versus `fin` tests.

### [ ] Cache delta playlists on write

`M3u8Cache::last_delta` decompresses, parses, and recompresses the stored playlist on every read.

In a steady 32-segment probe, a full cached hit reached 11.070 million reads/s. A delta hit reached 967 reads/s. The delta path was about 11,448 times slower.

Build both variants when the manifest changes:

1. Generate the full playlist and its delta from the uncompressed manifest bytes.
2. Compress each available variant once.
3. Store the full bytes, full hash, optional delta bytes, and delta hash in the same versioned slot.
4. Return the cached delta without decompression or parsing.
5. Preserve the current absent result when the playlist cannot use `EXT-X-SKIP`.

Add equivalence tests that compare the cached delta with the current delta generator. Benchmark full and delta hits separately. A cached delta hit should be within two times the CPU cost of a full cached hit.

### [ ] Bound manifest history and make time lookup constant-time

`M3u8Manifest::seg_durs` grows for the lifetime of a stream. `segment_start_time` scans from the start of that vector during every render.

With four retained segments, the measured render cost rose from 4.19 microseconds per part after 2,000 completed segments to 14.53 microseconds after 20,000 completed segments.

Replace `seg_durs` with a bounded `VecDeque` or ring:

1. Store only durations that the retained playlist can reference.
2. Track the logical ID of the first retained segment.
3. Track the program date-time of that first retained segment.
4. Advance the base time when a duration leaves the ring.
5. Make `full_segments` use ring-relative indices.
6. Remove `dur`. The crate increments it but does not read it.

Review `seq`, `seg_id`, and `idx` at the same time. Use checked operations or wider types for long-lived streams. The current unused `u32` duration counter wraps after about 49.7 days of accumulated milliseconds.

Add a long-run test that crosses many retained-window rotations. Assert stable output cost and correct program date-times.

### [ ] Re-evaluate nonblocking slot reads after map work

A temporary `try_read` and `try_write` experiment improved the one-worker read result. It reduced some two-worker and four-worker mapped results. The experiment was reverted.

Do not land this change alone. Remove global map contention first. Then compare fair locking, bounded spin, and `try_read` fallback behavior under read-only and mixed workloads. Include tail latency and writer progress in the decision.

## P2: Memory, layout, and mesh pipeline

### [ ] Define and enforce the playlist memory budget

`M3u8Cache::new` forces `buffer_size_kb` to 5. Stored `Bytes` values are dynamic, so this value does not cap a playlist snapshot.

A probe used 5 ms parts, 100 parts per segment, 32 segments, and 3,400 writes per stream. One stream retained 3,100 gzip snapshots with 19,710,512 payload bytes and added 26,804,224 bytes of RSS. Five streams retained 15,500 snapshots with 98,579,823 payload bytes and added 123,240,448 bytes of RSS.

Choose one explicit policy:

- Treat the option as a maximum encoded playlist size and reject larger values.
- Remove the option and expose a calculated memory budget based on stream count, retained slots, and observed payload sizes.

Use checked multiplication in the constructor. Return a configuration error instead of panicking or overflowing. Add a test that fills at least two complete retained-window rotations. Live payload bytes and RSS growth must plateau after warm-up.

### [ ] Prove whether eager playlist-slot zeroing is necessary

`M3u8Cache::end_segment` locks every part slot for the new ring segment and replaces each value with a shared 5 KB filler. This adds rollover latency and lock traffic.

The packet header already contains segment ID and generation fields. First add tests that show these fields reject every stale slot. If the proof holds, remove eager zeroing and use lazy replacement. Measure rollover latency, peak RSS, and stale-read behavior before and after the change.

### [ ] Group per-stream state and test cache-line alignment

The caches keep per-stream atomics in separate vectors. Concurrent streams can update adjacent atomics on the same cache line.

Group generation, position, sequence, and version state in one `StreamState`. Isolate frequently written fields from read-mostly fields. Test explicit cache-line alignment only after a profile shows false sharing. Report memory overhead with the throughput result.

Move a slot hash into the lock-protected slot value if all readers already hold the slot lock. Remove the extra atomic only when the benchmark shows no regression.

Review `SeqCst` operations after the lifecycle redesign. Use weaker ordering only with a written happens-before argument and a concurrency test.

### [ ] Separate mesh receive work from replica serving

The receive loop awaits complete frame handling. A replica request can serve a complete retained range in that path, so it can delay packet receive and FEC recovery.

Use a bounded pipeline:

1. Keep the datagram loop limited to validation, decode, and bounded enqueue.
2. Send completed frames to a bounded worker queue.
3. Send replica work to a separate bounded queue.
4. Define drop or retry behavior for each full queue.
5. Record queue depth, drops, decode time, and replica service time.

`FecReceiver::prune_observations` scans maps on each datagram. Replace this with periodic pruning and an ordered expiry queue. Consider a bit set for source-symbol observations. Bound the total decoder and peer state before micro-optimizing FEC counters.

Pass `Bytes` into frame decoding and return slices where ownership permits. Avoid copying decoded payloads into new buffers.

## Benchmark work

### [x] Prevent scheduler phase locking in `distribution_capacity`

The old latency sample interval was 4,096 operations. It aligned with Tokio's cooperative scheduling boundary and biased samples toward forced-yield events.

The benchmark now samples every 4,093 operations. It also yields explicitly every 4,096 operations. The explicit yield gives all workers progress and makes the deadline check reliable.

### [ ] Move timed loops out of unit tests

`test_append_and_last` can run on a single-thread Tokio test runtime. Its reader loop does not yield when cache hits complete immediately. A local run produced zero reader operations, delayed the writer, and took about 10 seconds instead of the intended 5 seconds.

Keep correctness assertions in unit tests. Move all timed throughput loops to `benches/`. The Rust test runner can run timed unit tests together and contaminate their results.

### [ ] Add permanent benchmark scenarios

Extend the custom benchmark runner with named scenarios:

- Chunk reads by raw index, stream ID, and stream handle.
- Chunk writes by raw index, stream ID, and stream handle.
- Mixed reads and writes at fixed ratios.
- Stream creation, eviction, and reuse under churn.
- Manifest render and `M3u8Cache` writes.
- Full `Playlists::add` with independent streams and with one hot stream.
- Full and delta playlist reads.
- Mesh receive, FEC recovery, and replica service when the feature is enabled.

Use a barrier to start workers. Use a prime latency-sampling interval. Yield at a separate fixed interval. Record wall time, process CPU time, CPU nanoseconds per operation, latency percentiles, allocation counts, live payload bytes, current RSS where available, and maximum RSS.

Run each scenario at 1, 2, 4, and 8 workers when the host has enough CPUs. Use at least five samples on a quiet dedicated host. Report the median and range. Do not label logical `Bytes` length as memory or network bandwidth.

## Local baseline

The following release-mode probes ran on an Apple M1 with eight logical CPUs. They used reused 5,760-byte `Bytes` values unless the row says otherwise. Temporary probe sources were removed after the review.

| Operation | 1 worker | 2 workers | 4 workers | 8 workers |
| --- | ---: | ---: | ---: | ---: |
| Indexed chunk writes/s | 375,227 | 710,251 | 1,132,002 | 1,418,402 |
| Mapped chunk writes/s | 379,974 | 731,641 | 1,097,372 | 1,078,453 |
| Manifest renders/s | 21,545 | 40,572 | 57,516 | 77,053 |
| Small gzip playlist writes/s | 103,609 | 171,320 | 189,459 | 197,711 |
| Full `Playlists::add` calls/s | 4,877 | 9,049 | 12,890 | 10,990 |

The corrected read benchmark reported these CPU costs:

| Workers | Mapped CPU ns/read | Indexed CPU ns/read |
| ---: | ---: | ---: |
| 1 | 59.59 | 37.04 |
| 2 | 101.82 | 32.84 |
| 4 | 272.68 | 53.13 |
| 8 | 1,992.21 | 88.46 |

## Release acceptance gates

- All tests pass with all features in release mode.
- `cargo check --all-targets --all-features` passes.
- `cargo fmt --check` passes.
- Deterministic tests cover index reuse during chunk, playlist, initialization, and replica writes.
- No queue or collection that accepts churn or network input can grow without a configured bound.
- Live cache memory reaches a stable plateau after the retained window is full.
- Indexed or handle-based throughput does not regress by more than 5% for a change that targets another path. Use the median of at least five dedicated-host runs.
- The mapped or handle-based read path does not lose throughput from four to eight workers after the map redesign.
- The independent-stream `Playlists::add` path does not lose throughput from four to eight workers after playlist sharding.
- A cached delta hit uses no decompression and no compression. Its CPU cost is at most two times the full cached-hit cost.
- Mixed-load benchmarks show forward progress for readers and writers. Report p50, p95, p99, and maximum latency.

Exact numeric release targets need a stable benchmark host and a production workload profile. Record those targets after the permanent scenarios exist.

## Maintenance

### [ ] Clean strict Clippy findings

`cargo clippy --all-targets --all-features -- -D warnings` currently reports three nonfunctional findings. Derive `Default` for `CacheMeshRole` where possible. Refactor or explicitly allow the two mesh functions with many arguments after the mesh pipeline design is stable.

### [ ] Refresh public performance claims

Update README performance examples only after the permanent suite runs on a dedicated host. Include the crate commit, Rust version, target, host CPUs, duration, scenario, and payload semantics with every published result.
