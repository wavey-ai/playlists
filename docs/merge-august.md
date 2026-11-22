# Merging the August line into low-latency: design fit

This record was written before any conflict was resolved.

- Branch `merge-august`, made from `origin/low-latency` at `ff09196`.
- The two lines diverged at `516b838`.
- The August line is `origin/main`, `0fee88e..32c5c81`, 10 commits.
- The low-latency line is `origin/low-latency`, `516b838..ff09196`, 22 commits.

A trial `git merge-tree --write-tree HEAD origin/main` (tree `fd7d8fd`) conflicts in seven files:

| File | Conflict hunks | Notes |
|---|---|---|
| Cargo.toml | 1 | |
| README.md | 3 | |
| src/chunk_cache.rs | 10 | |
| src/lib.rs | 5 | |
| src/m3u8_cache.rs | 16 | |
| src/m3u8_manifest.rs | 11 | |
| src/mesh.rs | 35 | |
| Cargo.lock | none | Auto-merges correctly: bytes 1.11.1, rustls-webpki 0.103.13, aws-lc-sys 0.44.0, aws-lc-rs 1.18.0, arc-swap 1.9.2. The `playlists` entry lists raptorq, socket2 and libc. |
| `TODO.md`, `benches/*` | none | Added cleanly. |
| `src/stream_registry.rs` | none | Added cleanly. |

**Warning about the auto-merged regions.** Several regions merged without a conflict marker, but the result is wrong. Two examples:

- In `src/mesh.rs`, `FecReceiver::prune_observations` took August's queue-based recovery expiry. That silently drops low-latency's `LATE_SOURCE_GRACE` wall-time check, and the observation loop keeps the August cadence of a prune every 256 datagrams.
- `src/chunk_cache.rs` interleaves August's `ChunkSlot` writes with low-latency's per-field atomics.

The plan in section 6 therefore rebuilds each of the five core files from one side and ports the other side's features onto it. It does not resolve hunk by hunk.

Labels used below:

- **LL** is low-latency and **AUG** is August.
- **Provisional** marks a decision that a measurement settles. The measurements are the product session's latency bench (Opus 2.5 ms parts and PCM 1 ms parts, audio only and with 4K on the mesh, p50 and p99, distributor CPU per part) and the in-crate 4,000 parts/s tests.
- The product bench uses `ChunkCache` and the mesh only. `av-mesh-run` builds a `ChunkCache` and never an `M3u8Cache`. Changes to `M3u8Cache` and `M3u8Manifest` are therefore gated by this crate's tests and by the AUG benches once they are ported to render-on-read, not by the product bench.

---

## 1. The two lines

**August (`origin/main`, 22–23 Aug 2026).** August makes the caches safe under concurrent use and stream reuse, then bounds everything.

The plan is `0fee88e` (`TODO.md`). The core is `9a8747d`:

- A crate-private `StreamRegistry` gives every logical stream a `(registry_id, stream_id, index, generation)` identity. Callers hold it as an opaque `StreamHandle` or `M3u8StreamHandle`.
- Generations are u64 and never 0. Writes run under a per-index owner read lock (`with_validated`). Reads check a two-load seqlock (`is_current_fast`) before and after they clone `Bytes`.
- `ChunkCache` slots become one locked `ChunkSlot {id, generation, hash, bytes}`.
- Inits become tagged `Option<StreamInitialization>`.
- `M3u8Cache` stores a gzip full and delta snapshot per part in a bounded ring, plus an `ArcSwapOption` pointer to the latest.
- `Playlists` gets a per-entry lock and a `fin` that cannot resurrect a stream.
- The mesh gets a peer allowlist, bounded decoder, observation, peer and remote-slot state, a three-stage bounded receive pipeline, and zero-copy frame decode.
- Checked-arithmetic `try_new` constructors, `memory_stats()`, five bench binaries and 100 lib tests.

Later commits add the following:

| Commit | Adds |
|---|---|
| `52a34a9` | A migration doc. |
| `06da940`, `c5bb885` | Per-lane `Notify` for blocking readers. |
| `1f35d28` | Generation-tagged segment ranges. |
| `e541002` | Render allocation work, with a direct PROGRAM-DATE-TIME writer. |
| `bf29e48` | Opt-in hot read replicas. |
| `e107101` | Lockfile security bumps. |
| `32c5c81` | GCP results. |

Nothing deployed uses this line. `web-services` has it locked at `e107101`, and `daw-nexus` pins `e107101`.

**Low-latency (`origin/low-latency`, through 21 Nov 2026).** Low-latency makes the smallest parts cheap end to end. This is the line `av-mesh-run` uses and the labs run, and the `product` session still pushes to it.

Playlists (`886e64e`, `c67412e`):

- Time is held in microseconds: `part_target_us` defaults to 2,500, with exact decimal durations and ring sizes derived from `window_ms`.
- `M3u8Cache` keeps one live `M3u8Manifest` per stream. A part is added in O(1). The playlist is rendered and gzipped on read, once per new part, under `render_locks`.
- A segment closes before its EXTINF would round above TARGETDURATION.

`ChunkCache`:

- Keeps a lock-free slot write time (`7260e74`).
- Signals publication only when the watermark advances, through `publication_notify` and `subscribe_publications`/`PublishedStreams` (`978e596`, `973139a`).
- Takes a free index before evicting a stream (`151eaee`).
- Releases single slots and whole streams (`afc3bae`, `151eaee`).

The mesh is rebuilt for 4,000 parts/s on one core:

- Send on publish, with hop times.
- Publication passes that visit only the streams that moved, and no echo of received parts.
- `mesh_send::ObjectEncoder`, with a plan cache, a one-symbol repair table and an encoded-object cache keyed by payload hash.
- Fitted symbols up to 1,200 bytes.
- `sendmmsg` and GSO.
- Live parts sent before warm-up.
- A receive observer for the edge's UDP cut-through.
- Loss-driven repair, with `LinkReport` and late-source grace.
- Demand replication with leases.
- Per-start block-id lanes.

The in-crate gate is `a_distributor_on_one_core_{sends,keeps_two_edges_at}_four_thousand_parts_a_second`: zero missing parts and p99 under 2 ms in release. Measured, that is about 100 µs to sockets and about 180 µs to edges.

---

## 2. How they fit, by area

### 2.1 Caches

#### Stream identity and the registry (`src/stream_registry.rs`, AUG only)

**AUG.**

- `StreamRegistry {registry_id, mappings: RwLock<HashMap<u64, ResolvedStream>>, owners: Vec<OwnerCell{accepting, published_stream_id, published_generation, owner: RwLock<Owner{stream_id: Option<u64>, generation}>}>, lifecycle: Mutex<Lifecycle{next_index}>}`.
- `resolve_or_create` publishes generation 0 and closes the gate. It removes the previous owner's mapping, then takes the owner write lock and advances the generation. It runs the cache reset closure under that lock, then publishes the stream_id and generation, opens the gate, and inserts the mapping last.
- `with_validated` runs handle writes under the owner read lock. `with_generation` runs raw-lane ops only when `owner.stream_id` is `None`.
- New indices are chosen round-robin (`next_index`).

**LL.**

- No registry. `ChunkCache` uses `offsets: StdRwLock<HashMap<u64, usize>>`, `generations: Vec<AtomicUsize>` and a scan for a free index (`151eaee`).
- `M3u8Cache` uses `offsets: RwLock<BTreeMap<u64, usize>>` and `generations: Vec<AtomicUsize>`.

**Compose: yes.** The registry is strictly stronger. The LL behaviours it lacks can all be added inside it:

- Free index first, then rotate.
- Release that frees bytes.
- Index-to-handle lookup for the publication sets.

**Plan.**

- Keep `stream_registry.rs` and extend `Lifecycle` to `{next_index, free: VecDeque<usize>, is_free: Vec<bool>}`.
  - Seed `free` with `0..capacity` in order.
  - `resolve_or_create` pops `free` first. Only when `free` is empty does it take `next_index` and advance it.
  - `remove_stream` and `reset_index` push the index if it is not already free.
  - Assignment stays O(1). LL's scan was O(n²) under the map lock.
  - Check: `stream_churn_retains_only_the_configured_mapping_capacity` (100k streams, 2 lanes) still yields `[(99_998,0),(99_999,1)]`.
- Change `remove_stream` to return `Option<ResolvedStream>`, the released identity, so `release_stream_id` can free exactly that generation's slots.
- Add `pub(crate) fn handle_at(&self, index) -> Option<ResolvedStream>`.
  - It builds the identity from `published_stream_id` and `published_generation` with the same two-load check as `is_current_fast`, and returns `None` while `accepting` is false.
  - It replaces LL's `stream_at` map plus the `cache.stream_idx(id) == Some(idx)` recheck in `sync_task`.

#### `ChunkCache` (`src/chunk_cache.rs`)

**AUG.**

- `buffer: Vec<tokio RwLock<ChunkSlot>>`, `stream_states: Vec<StdRwLock<ChunkStreamState{last, next_id, version}>>`, `stream_initializations`, `registry`, `stream_update_notifiers` (per lane, `c5bb885`), and `exact_part_waiters` keyed `(stream_id, generation, id)`.
- Writes into the same generation reject an older id over a newer one:
  - `add` returns `"Chunk id already evicted"`.
  - `put_if_absent` returns `PutIfAbsentResult::Superseded`.
- Mapped writes validate the payload size (`buffer_size_kb`, `init_size_kb`) before `resolve_or_create`.
- `take_new_playlists_rx` is removed.
- Raw-lane ops refuse registry-owned lanes.

**LL.**

- Parallel atomics: `slot_ids`, `slot_generations`, `slot_hashes`, `slot_written_us`, `idxs`, `next_ids`, `generations` and `versions`.
- `published: Notify` plus `publication_sets: Vec<Weak<PublishedStreams>>`, signalled only by a watermark CAS that advances (`publish_last`, `publish_contiguous_last`).
- `written_unix_us_for_stream_id`: sync, no lock, and readable before any waiter wakes.
- `clear_for_stream_id`, `release_stream_id`, and a sync `stream_idx`.
- Keeps `take_new_playlists_rx`, but nothing calls it: not this crate, `av-mesh-run`, `av-mesh`, `web-services` or `daw-nexus`.

**Compose: yes, with four ports onto the AUG layout.**

1. **Publication.**
   - `update_state_after_slot` and `publish_contiguous_last` return whether `last` strictly advanced (from `None`, or to a larger id).
   - The caller drops the slot and state locks, then calls `signal_publication(index)`: mark every set, then `notify_waiters`. After that come `notify_exact_part_waiters` and `notify_stream_update`.
   - No signal on `AlreadyPresent`, `HashConflict`, `Superseded`, a write beyond a gap, `clear_for_stream_id` or `release_stream_id`.
   - Both notification systems are kept. `publication_notify` is global and fires on watermark advance; the mesh uses it. `update_notifier` is per lane and fires on every write, init change and reset; `web-services` uses it.
2. **Write time (Option A, which wins on latency).**
   - Keep three lock-free atomics per slot next to `ChunkSlot`: `slot_written_us: AtomicU64`, plus an identity mirror `slot_tag_id: AtomicUsize` (EMPTY = `usize::MAX`) and `slot_tag_generation: AtomicU64`.
   - The writer, holding the slot write lock, stores in this order: tag id = EMPTY (Release), `written_us` (Release), tag generation (Release), tag id (Release). It then drops the lock, then notifies.
   - `written_unix_us_for_stream_id` resolves the handle with `is_current_fast`. It loads tag id and generation (Acquire), then the time (Acquire), then the tag again. It returns `None` on any mismatch or on 0.
   - Option B, a field read with `try_read`, can return `None` under write contention. That breaks the LL invariant behind `av-mesh-run` `main.rs:6759`, so it is rejected.
3. **`clear_for_stream_id` and `release_stream_id`.**
   - `clear_for_stream_id(stream_id, id)`: resolve the handle, take the slot write lock, then inside `with_validated`, if `slot.id == Some(id) && slot.generation == gen`, set `id = None`, `bytes = Bytes::new()`, `hash = 0` and clear the tag. The watermark and version stay unchanged.
   - `release_stream_id(stream_id)`: `registry.remove_stream(stream_id, reset_index_state)` returns the released identity. Then, for each slot of that lane, under its write lock, empty it if `slot.generation == released.generation`. This is more exact than LL's "generation ≠ current".
4. **Free index.** Done in the registry (see 2.1, registry).

**Write cost (provisional).** AUG adds an owner `RwLock` read and a `StdRwLock<ChunkStreamState>` write per part where LL had CAS atomics. If the product bench or the 4,000/s tests show p50 or p99 or distributor CPU per part getting worse, keep `last` and `next_id` as atomics updated inside the `with_validated` region instead of the state lock. The generation checks stay as they are.

#### `M3u8Cache` (`src/m3u8_cache.rs`)

**AUG.**

- Write-time rendering. `Playlists::add` renders the manifest on every part. `M3u8Cache::add` gzips full and delta (`prepare_variants`, with a `buffer_size_kb` budget on both raw and encoded size) and stores `Arc<PlaylistSnapshot>` in the per-part ring `buffer: Vec<RwLock<Option<Arc<PlaylistSnapshot>>>>` plus `latest: Vec<ArcSwapOption<PlaylistSnapshot>>`.
- `get(seg, part)` returns that part's own snapshot.
- Also: `PlaylistStreamState {position, segment_ranges, version}`, `update_notifiers`, the registry, tagged inits, `try_new`, `memory_stats` and replicas.

**LL** (`886e64e`).

- `slots: Vec<Mutex<PlaylistSlot{generation, version, manifest: Option<M3u8Manifest>, rendered: Option<Rendered{version,text,gzip,hash}>, delta}>>` and `render_locks: Vec<Mutex<()>>`.
- `add_part_us` pushes a part and bumps the version, with no rendering. The first reader of a new version renders `pieces()` outside the slot lock and gzips with `Compression::fast()`.
- `get(seg, part)` returns the newest playlist once the part is included (RFC 8216bis blocking reload).
- Position is held as three separate atomics (`last_seg`, `last_part`, `last_seq`) plus `seg_parts`.

**Compose: in part.** The storage models are mutually exclusive, and LL's wins (section 3, C1). Everything August built that does not depend on its ring is layered onto LL's slot. The **plan**:

- **Identity.** Replace `offsets`, `offset` and `generations` with `StreamRegistry`, `M3u8StreamHandle` and `*_for_handle` variants. `PlaylistSlot.generation` becomes u64 from the registry.
- **Position and ranges.** Fold AUG's `PlaylistStreamState {position: Option<PlaylistPosition>, segment_ranges: Vec<Option<SegmentRange>>, version}` into `PlaylistSlot`. `add_part_us` publishes the position and the tagged segment range in the same critical section that pushes the part. That gives one atomic position, which LL's three atomics did not.
  - Ranges come from the `PartPosition`: `first = sequence - index`, `end = sequence + 1`.
  - `get_idxs(9,2) == (401, 801)` must hold, from LL's `a_part_is_added_without_rendering_and_read_renders_once`.
- **Version.** `slot.version` continues across reuse via `next_version` and is never 0, so a version or ETag never repeats for a new stream. LL reset it to 0.
- **Validate before create.** `add_part_us` checks the stream capacity rule and init sizes before `resolve_or_create`, so a rejected write never evicts a live stream (`rejected_mapped_writes_do_not_evict_a_live_stream`).
- **Inits.** `inits: Vec<RwLock<Option<StreamInitialization>>>`, tagged. `get_init` on a new or reused stream returns `StreamNotFound`, with no zero-filled default. `set_init` enforces `init_size_kb`.
- **Notifiers.** `update_notifiers: Vec<Arc<Notify>>`, one per lane. `notify_update(idx)` is called from `add_part_us` after the position is stored and the slot lock is dropped, from `set_init`, and from reset or zero. A lazy render does not notify (it does not change the version).
- **Read fast path (provisional).** LL's `render()` takes the slot `Mutex` on every read, even a hit. Under many readers that loses AUG's lock-free ArcSwap hit.
  - Add per slot `published: ArcSwapOption<Rendered>` (tagged with stream_id and generation) and `published_version: AtomicU64`. Writers store `published_version` after the push. A reader loads the version and the ArcSwap and returns the rendering if both match and the handle is current. It takes `render_locks[n]` and the slot lock only to render.
  - Keep this only if the ported `playlist_cache` bench shows a gain and the LL tests still pass, including the pointer equality of two reads.
- **Delta.** Keep LL's `render_delta`, cached per version.
- **Removed:** AUG's ring (`buffer`, `calculate_index`), `prepare_variants` on the write path, `add_prepared_for_handle`, the write-time delta, and the per-snapshot `buffer_size_kb` budget. Also LL's stored-playlist path `M3u8Cache::add` / `add_for_handle` / `end_segment` and the `manifest: None` fallback in `render()` (section 4, item 6).
- **`memory_stats()`** is recomputed over slots:
  - `occupied_slots`: slots with a manifest.
  - `encoded_playlist_bytes`: `rendered.text + rendered.gzip + delta` plus replica copies.
  - `initialization_bytes`.
  - `maximum_payload_bytes`: an options-derived bound on one rendering times streams.

#### Options and `Playlists` (`src/lib.rs`)

**AUG.**

- `PlaylistEntry {closing: AtomicBool, manifest: Mutex<M3u8Manifest>}` in `Mutex<BTreeMap<u64, Arc<PlaylistEntry>>>`.
- `fin`: set `closing`, take the entry lock, zero both caches, then remove the entry only if `Arc::ptr_eq`.
- `add` rejects an init larger than `init_size_kb*1024` and propagates `ensure_stream_id`/`set_init` errors.
- `try_new` and `new_with_latest_read_replicas` / `try_new_with_latest_read_replicas`, `PlaylistCacheBundle`, and `CacheError::{ArithmeticOverflow, StreamNotFound, Superseded}`.
- The old Options defaults (32×128, ms).

**LL.**

- `streams: Mutex<BTreeSet<u64>>`, with the manifest moved into `M3u8Cache`.
- `add` (ms) calls `add_us`, which calls `m3u8_cache.add_part_us`. Init errors are ignored.
- LL's `Options` (µs fields, derived counts, public idempotent `normalized()`).

**Compose: yes.**

- Take LL's `Options` minus `part_target_ms` and `eager_render`.
- Take an entry `{closing: AtomicBool, commit: Mutex<()>}` with no manifest. `add_us` holds `commit` across `set_init` and `add_part_us`. Without it, `add_part_us` would auto-create the stream after `fin` and leave a ghost slot.
- Keep AUG's `fin` protocol, the init-size precheck, error propagation, the `try_*` constructors and `PlaylistCacheBundle`.
- Drop `CacheError::Superseded` if nothing produces it once `M3u8Cache::add` is gone. `ChunkCache` uses `PutIfAbsentResult::Superseded` and `&'static str` errors.

### 2.2 Manifest and rendering (`src/m3u8_manifest.rs`)

**AUG** (`9a8747d`, `e541002`).

- Milliseconds, with five-decimal durations.
- `completed_segments: VecDeque<CompletedSegment{id, duration_ms, start_time}>` with `open_start_time`.
- `RenderHeaders{without_skip, with_skip, can_skip_until_ms}`, precomputed.
- `render_buffer: String`, reused by `render_reusing_buffer`.
- `append_program_date_time`: direct RFC 3339 output with no chrono allocation, covering extended years and leap seconds.
- `open_segment_full` forces a segment close at `max_parts_per_segment`.
- Rendition reports capped at `MAX_RENDITION_REPORTS = 256`, with URIs of at most `MAX_RENDITION_URI_BYTES = 2048`.
- Measured: 46 allocations and 10 reallocations per render fell to 1.

**LL.**

- Microseconds. `ClosedSegment{parted: Option<Arc<str>>, plain: Arc<str>}` is rendered once at close. `OpenSegment.part_lines` grows in O(1) per part.
- `pieces() -> RenderedPieces{head, segments, tail}` is assembled outside the lock.
- `push_seconds` / `format_seconds_us` / `parse_seconds_us`, `push_part_us`, `opens_segment(duration_us, key)`, `version()`, and `parted_target_durations`.
- The head (SERVER-CONTROL, PART-INF and so on) is built per render, and `program_date_time_line` uses chrono `to_rfc3339_opts`.

**Compose: in part.** LL's structure wins, because it renders each segment once and a part costs O(1). That makes `render_buffer` reuse moot: `RenderedPieces::into_bytes` already allocates once. Port from AUG:

- `append_program_date_time` into `program_date_time_line`, used in `close_segment` and in the per-render tail, together with its test.
- Precomputed head pieces: the parts that depend only on Options (VERSION, TARGETDURATION, SERVER-CONTROL with and without `CAN-SKIP-UNTIL`, PART-INF). MEDIA-SEQUENCE and PDT stay per render. This is provisional on the ported `playlist_writes manifest-render` mode showing fewer allocations and no slower render.
- The rendition-report caps (256 entries, 2,048-byte URIs) next to LL's `valid_quoted_string_value` filter, keeping the `version += 1`.

Not ported: AUG's ms fields, `append_milliseconds`, and `open_segment_full` (C5).

### 2.3 Readers

**Blocking notifications.**

- AUG `06da940`: `M3u8Cache::update_notifier_for_handle(handle) -> Option<Arc<Notify>>`. It returns `None` for a stale handle, and `is_current_fast` is checked before and after the clone.
- AUG `c5bb885`: `ChunkCache::update_notifier(stream_idx)`.
- LL: `ChunkCache::publication_notify` plus `PublishedStreams` (global, watermark only), and exact-part waiters.
- No `Notify` stores a permit. The contract is: `enable()`, recheck, then await.
- **Compose: yes.** All three are kept with the firing points from 2.1. The placement rule is that `ChunkCache` notifiers fire after the slot write and after the write-time tag is stored, and `M3u8Cache` notifiers fire on version change (part add, init, reset), never on a lazy render.

**Exact-part waiters.**

- AUG keys them `(stream_id, generation, id)`. It returns `None` for an unknown stream so that a read cannot create a mapping.
- LL keys them `(stream_id, id)` and returns `Some` for a stream that does not exist yet. An edge reader can then wait for the first replicated part.
- The product's blocking reader does `exact_part_waiter(..)?`, as do `web-services/chunks` and `hls`.
- Conflict C6: LL wins.

**Segment ranges.**

- AUG `1f35d28`: `get_idxs_for_handle(handle, seg) -> Result<Option<(usize, usize)>, CacheError>`, half-open `[first, end)`, tagged by `segment_id`. A stale handle gets `Err(StreamNotFound)`; an unknown id gets `Ok(None)`.
- LL: `seg_parts` atomics, written when idx = 0.
- **Compose: yes.** Same numbers; AUG's tagging fixes the stale-boundary case. Feed the ranges from `add_part_us` positions as in 2.1.

**Hot read replicas.**

- AUG `bf29e48`: opt-in through the constructor's replica count (1..=64, default 1). Every publish copied the payload for replicas 1..n-1.
- Measured at write time: 8 readers on one hot delta went from 8.96M to 38.62M reads/s, at +9 allocations and +9% CPU per write.
- Under render-on-read there is no write-time copy.

**Compose: yes, but changed and provisional.**

- `render()` builds `variants: Box<[RenderedVariant{gzip, hash}]>`, copying the gzip for replicas 1..n-1. Delta variants are built in `render_delta` the same way. The result is published through the ArcSwap fast path above.
- A reader picks `LATEST_READ_REPLICA % n`, as AUG did.
- With the default of 1 replica there are no copies, so the default path costs nothing. The copies are a memcpy of a few KB on the first reader per version, which is cheaper than the gzip that reader already does.
- Keep `M3u8Cache::{new,try_new}_with_latest_read_replicas`, `latest_read_replica_count` and the `Playlists` equivalents, but only if the ported `playlist_cache handle-delta-replicated` mode, interleaving `add_part_us` with reads, still shows the gain.
- If it does not, drop the replicas with their three tests and the bench modes, and record the numbers.

### 2.4 Mesh (`src/mesh.rs`, `src/mesh_send.rs`)

#### Send path

**AUG.**

- Tick-only `sync_task`, one awaited `send_to` per datagram, `DatagramFecEncoder`.
- Keys `sent` and `sent_initializations` by `(stream_id, generation, peer)`, pruned each tick.
- Generation-safe reads: `last_for_handle`, `get_for_handle`, and a break when the handle goes stale.
- `FecSender.max_frame_bytes` checked in `send()`.

**LL.** Everything in section 1 (send on publish, `ObjectEncoder`, `BatchSender`, fitted symbols, live before warm-up, observer), keyed by `(stream_id, stream_idx)`.

**Compose: yes, and LL's structure wins wholesale.** Graft the following onto it:

- Handles everywhere:
  - `PublishedStreams::drain` idx → `cache.handle_at(idx)`.
  - `SeenWatermarks`, `sent` and `sent_initializations` keyed `(stream_id, generation[, peer])`.
  - `last_for_handle`, `get_for_handle` (which returns the slot hash for `EncodedKey`) and `stream_initialization_for_handle`.
  - `stream_handles()` on ticks.
- Keep LL's "cursor > last ⇒ restarted" rule as well as the generation key.
- AUG's pruning of `sent` and `sent_initializations` for dead streams and unknown peers, run at LL's 250 ms `REMOTE_SLOT_PRUNE_INTERVAL` cadence, not on every publication pass.
- The `max_frame_bytes` check (`tx_errors`, `InvalidFrame`) in LL's `prepare_object` and `FecSender::send`. `from_config_with_initial_block_id` sets it.

#### Receive path

**AUG.**

- Allowlist check, then FEC push, then `MeshFrame::decode(Bytes)`, then `try_enqueue(FrameWork)` on a bounded queue (64) to `frame_task`, which runs `handle_frame`.
- `ReplicaRequest` goes to a bounded `replica_task` (64) whose sender starts at `REQUEST_BLOCK_ID_BASE + (1<<20)`.
- Chunk apply: `validate_payload`, then `resolve_or_create_stream`, then `add_for_handle`, then insert into remote slots *after* the write, keyed `(stream_id, generation, slot)`, with arbitrary eviction at `max_remote_slots`.

**LL.**

- Inline `handle_frame` on the receive task, with `received_us`, `first_seen_us` and `decoded_us`.
- `receive_observer.datagram` before push and `.decoded` after decode (cut-through).
- `receive_loss_ppm` injection.
- `reply_sender` on `REPLY_BLOCK_ID_BASE + block_id_offset` with `with_link_loss`.
- Chunk apply: `peer_is_allowed`, `accepts_stream`, the second-parent dedupe, then mark the remote slot *before* the write and hold the `remote_slots` write lock across it (unmark on error), then `AppliedSlot`.

**Compose: in part.** Chunk handling stays inline (C3). `ReplicaRequest` moves to AUG's `replica_task`, with:

- LL's sender (`REPLY_BLOCK_ID_BASE + block_id_offset`, `with_link_loss`, `ObjectEncoder`).
- `ReplicaWork { requester_node_id, peer, stream_id, from_slot, count: u32 }`.
- `serve_replica_request(.., count, #[cfg(test)] hook)` using handles, with the stale-handle break in both branches (hole fill and range).
- `reply_sender` is used only for replicas on LL, so the move is clean, and a large replica range no longer blocks live receive.

`MeshFrame::decode` takes `Bytes` (AUG, zero-copy). `chunk_frame_payload(frame: &[u8])` keeps its signature (`av-mesh-run client_udp.rs:567`) and copies inside.

Merged chunk apply order:

1. `peer_is_allowed`
2. `accepts_stream`
3. `validate_payload`
4. `resolve_or_create_stream`
5. dedupe (`get_for_handle == payload`)
6. on non-edge: `remote_slots.write()`, mark `(stream_id, generation, slot)`
7. remote add (see C7)
8. unmark on error
9. drop the lock
10. `AppliedSlot`

#### Hardening, limits and queues

AUG adds:

- `CacheMeshConfig` fields: `allowed_peers`, `max_peers` 256, `max_inflight_frames` 64, `max_frame_bytes` 1 MiB, `max_remote_slots` 65,536, `frame_queue_capacity` 64, `replica_queue_capacity` 64, each with a `with_*` builder.
- `admit_peer`, and `add_peer` refused when the table is full.
- `FecReceiver::with_limits`, `admit_decoder_peer`, `make_observation_room` and `expire_block` on decode error.
- Ordered expiry queues, with pruning every 256 datagrams.
- 15 new `CacheMeshFecStats` fields.
- `Arc<MeshShared>`.

**Compose: yes, apart from the frame queue.**

- **One shared struct.** Merge AUG's `MeshShared{peers, allowed_peers, peer_regions, peer_roles, remote_slots, counters}` and LL's `MeshShared{leases, peer_seen, wanted, link_loss, measured_loss}` into one `Arc<MeshShared>`. `CacheMeshHandle` keeps LL's other fields (`served`, `replication`, `subscription_lease`, `peer_timeout`, ...).
- **Allowlist.**
  - Use `std::sync::RwLock<HashSet<SocketAddr>>` or `ArcSwap<HashSet<SocketAddr>>`, not a tokio lock per datagram.
  - Check it once on the receive task: after `recv_from`, `received_us` and the loss injection, and before `receive_observer.datagram`, so cut-through never forwards unauthorized traffic.
  - LL's `peers.insert` on Subscribe and ReplicaRequest goes through `admit_peer`.
- **FecReceiver.**
  - `with_limits(..).with_measured_loss(..)`. Reject an oversized `transfer_length` before `admit_decoder_peer`.
  - Prune on every datagram, as LL does. The queues make it O(expired).
  - The observation-expiry pop calls LL's `record_block` for blocks that never decoded.
  - The recovery-expiry pop finalizes when the front is past the datagram cutoff *or* past `LATE_SOURCE_GRACE`. The queue is ordered by completion, so both orders hold.
  - Provisional: `make_observation_room` and `admit_decoder_peer` evictions also call `record_block`, so evicted blocks count as loss in `LinkReport`.
- **Limits that must follow the cache.**
  - The `max_frame_bytes` default comes from `cache.options.buffer_size_kb * 1024` plus the frame header, set in `CacheMesh::new`. The product runs `--slot-kb 2048` (2 MiB 4K parts), so a fixed 1 MiB would silently drop large video parts that LL accepts.
  - `max_remote_slots` defaults to the cache's slot capacity (`num_playlists * max_segments * max_parts_per_segment`) plus one prune interval of headroom. On overflow, run `forget_unretained_remote_slots` under the held lock instead of evicting an arbitrary entry: an evicted live slot would be echoed (C8).
  - `max_inflight_frames` 64 is provisional. Check it with 4K video interleaved with 4,000 audio parts/s at `receive_loss_ppm` 30,000, and raise it if `rx_evicted_objects > 0` without real loss.
- **Stats.** `CacheMeshFecStats` is the union: LL's `tx_send_calls` and `rx_late_source_datagrams`, plus AUG's fields minus `frame_queue_*`.

### 2.5 Dependencies and build

| Source | Change |
|---|---|
| AUG `Cargo.toml` | `arc-swap = "1.7"` (kept: used by the M3u8 fast path, the replicas and possibly the allowlist). `[[bench]]` `chunk_workloads`, `playlist_cache`, `playlist_writes`, `mesh_pipeline` (`required-features = ["mesh"]`). Dev-deps `libc`, `serde` and `serde_json` are on both lines. |
| LL `Cargo.toml` | `mesh = ["dep:raptorq-datagram-fec", "dep:raptorq", "dep:socket2", "dep:libc"]`, optional raptorq 2.0.1, socket2 0.6, libc 0.2, and `[profile.dev.package.raptorq] opt-level = 3`. |
| Version | Keep `0.2.0`: 0.1.6 is published and the API differs. It is not mentioned in the changelog (section 4). |
| `Cargo.lock` | Take the merge-tree result (AUG's security bumps plus LL's deps) and confirm with `cargo build --all-features`. LL must not undo `e107101`. |

---

## 3. Real conflicts and which way they go

**C1. M3u8Cache storage: render on write (AUG) or render on read (LL). LL wins.**

- LL's `886e64e` replaced exactly AUG's model: O(window) work and a gzip per part, and O(window²) memory at 400–4,000 parts/s.
- Given up from AUG:
  - Per-part historical snapshots: `get(seg, part)` now returns the newest playlist that contains the part.
  - The write-time delta cache.
  - The O(1) latest hit with no render on the first read after a part. This is partly restored by the ArcSwap fast path (provisional).
  - The per-snapshot byte budget.
  - The "eager slot zeroing is unnecessary" proof, which was about the ring.
- Tests that go with the ring: `initial_segment_snapshot_uses_the_bounded_ring`, `delayed_older_playlist_cannot_overwrite_newer_ring_slot`, `lower_sequence_cannot_overwrite_the_same_playlist_position`, `snapshot_size_limit_is_enforced_before_compression`, `encoded_snapshot_must_also_fit_the_budget`.
- Positions are now produced in order under the slot lock, so the "older write overwrites newer" hazard those tests guarded no longer exists. A concurrent `add_part_us` monotonicity test replaces them (5.4).
- `encoded_payload_memory_plateaus_after_ring_rotations` is rewritten over slot memory.

**C2. Raw-index operations on registry-owned lanes. AUG wins; this is correctness, not latency.**

- On LL, `get_stream_idx` followed by `last(idx)`, `get(idx, id)` or `version(idx)` works for any stream. On AUG these return `None` for owned lanes.
- LL users of the pattern:
  - mesh `serve_replica_request` (`mesh.rs:2725-2771`)
  - the publication pass (`:3243`, `:3462`, `:3541`, `:3668`)
  - LL tests (`mesh.rs:5335`, the LL chunk tests that use `last(stream_idx)`)
  - `av-mesh-run`: 24 × `get_stream_idx`, 11 × `last`, 8 × `version`, 4 × `get`, 2 × `stream_ids`, 1 × `get_or_create_stream_idx`
- The merged mesh uses handles (2.4). The numeric shims are removed (section 4).
- The product must move to `resolve_stream` / `resolve_or_create_stream` plus `*_for_handle`. Tell the product session; this is not a change made here.
- Raw-lane ops stay for lanes the registry never assigns, as in `web-services` proxy and relay queues.

**C3. Receive pipeline: AUG's bounded frame queue or LL's inline handling. LL wins (provisional).**

- The queue adds a task wake per part between `decoded_us` and `committed_us`. Under bursts it drops FEC-recovered live parts (drop-new), which then need a hole fill.
- It would also reopen the mark-before-write ordering across a task hop.
- Given up from AUG: decoupling the frame apply from decode, `frame_task`, `FrameWork`, `frame_queue_capacity` / `with_frame_queue_capacity`, `frame_queue_{depth,max_depth,drops}`, the bench's frame-queue fields, and `bounded_mesh_queue_drops_work_when_full` as a frame-queue test. It is kept as a replica-queue test, since `try_enqueue` stays.
- Measurement that would reverse this: the product hop bench (MeshFirst, MeshDecoded, EdgeCommitted) and `a_distributor_on_one_core_keeps_two_edges_at_four_thousand_parts_a_second`, with a queue variant that is not worse at p99.

**C4. Stream index allocation. LL wins.**

- AUG uses round-robin eviction even when lanes are free. LL takes a free index first; edges add and release a stream per reader.
- Implemented in the registry with an O(1) free list (2.1, registry).
- Nothing is given up.

**C5. Segment part cap. LL wins.**

- AUG's manifest force-closes a segment at `max_parts_per_segment` (`open_segment_full`). LL refuses the part in `M3u8Cache::add_part_us` with `IndexOutOfBounds` and adds nothing.
- Given up: the manifest-level bound when `M3u8Manifest` is used directly with `segment_min_ms = u32::MAX`.
- `non_independent_input_cannot_grow_an_open_segment_without_bound` moves to the cache level: it asserts the refusal. Do not make `opens_segment` return true on capacity, or `a_segment_is_capped_at_its_part_capacity` breaks.

**C6. Exact-part waiter key. LL wins.**

- Key on `(stream_id, id)` and create no mapping. A waiter for a stream that is not mapped yet (an edge before its first part) gets `Some` and is woken by the first write in any generation. The woken reader rechecks with generation-safe reads.
- Given up from AUG: a waiter of generation N is no longer guaranteed not to be woken by generation N+1. A spurious wake costs one recheck; stale data is still impossible.
- `exact_part_waiter_does_not_cross_a_stream_generation` becomes "a waiter woken across a generation finds no data from the previous generation".
- `waiter_for_unknown_stream_does_not_evict_a_live_stream` becomes `Some` plus no eviction and no mapping.

**C7. Older id over newer id in the same generation. AUG for local writes, LL for mesh-applied writes (provisional).**

- AUG rejects an older id at a slot holding a newer id: `"Chunk id already evicted"` or `Superseded`. LL overwrote it.
- Found while planning test 5.2: the wire carries no generation. When the origin restarts a stream (`zero_stream_id`, then ids again from 0) after the edge ring has wrapped, every restarted part on the edge maps to a slot holding a larger old id, and AUG's rule rejects it. LL stored it.
- Resolution for this merge: the mesh apply path uses a crate-private `add_remote_for_handle`. It is generation-checked against local reuse, but does not reject an older id. That matches LL's behaviour exactly.
- Origin-local `add`, `put_if_absent` and `append` keep AUG's protection.
- Open item, the same on both lines: a non-origin node's `last` never moves back after an origin restart, so a relay does not forward restarted parts until the ids pass the old watermark. The proper fix is an origin epoch in CHUNK and INITIALIZATION frames, stored per stream: a receiver that sees a new epoch recreates its stream. That changes the wire format and the cut-through parser, so raise it with the product session. It is not part of this merge.

**C8. Remote-slot marking order and bound. LL wins.**

- Mark before the write, under a lock held across the write (`3317b29`). AUG's insert-after-write reintroduces the echo race that `a_received_part_is_marked_remote_before_its_publication_wakes_the_sender` catches.
- AUG's arbitrary eviction at `max_remote_slots` would also echo. The bound is derived from cache capacity, and overflow triggers the retained-window prune (2.4, hardening).

**C9. Peer admission. AUG wins (security), with a topology risk.**

- LL learned children from Hello, Subscribe and ReplicaRequest from any address. AUG drops datagrams from addresses not in `allowed_peers` before FEC.
- LL tests that rely on open admission get `with_allowed_peers`:
  - `the_send_observer_sees_live_parts_and_not_a_new_childs_catch_up`, whose distributor starts with no peers.
  - `a_restarted_child_is_heard_by_its_parent_at_once`.
  - `cache_mesh_gossips_peer_addresses_from_seed_node`.
- The product builds peers from `--peer` plus discovery `add_peer`, which authorizes. Any distributor whose children are in neither would silently stop serving them; `rx_unauthorized_datagrams` shows it. Tell the product session.

**C10. Versions across reuse. AUG wins.** LL's `PlaylistSlot.version` reset to 0 on reuse. Merged versions never repeat.

**C11. Payload validation. AUG wins.**

- LL never validated sizes. Merged writes reject chunks over `buffer_size_kb` and inits over `init_size_kb` (default 5 KB).
- The mesh's `max_frame_bytes` follows `buffer_size_kb` (2.4).
- The product must confirm that its 4K init segments fit `init_size_kb`, or set it.

**C12. Benches and docs that describe AUG's model.** Remove or re-measure:

- The README "Operation costs" rows "Latest playlist hit O(1) ArcSwap load" and "Playlist write O(retained manifest bytes) incl. full/delta gzip".
- The diagnostic maxima "cached delta handle read 46.6M/s" and "Playlists::add 109,083/s".
- The GCP HTTP section stays: it measures a static payload and does not depend on the crate.

---

## 4. Compatibility code to remove

One way to do each thing. Callers in this crate are updated in the same change.

1. **`Options.part_target_ms`** (`src/lib.rs`).
   - Remove the field (`lib.rs:70` on LL), the alias branch in `Options::part_target_us()`, and the "keep the ms field when exact" step in `normalized()`.
   - Callers to convert to `part_target_us`:
     - LL `m3u8_manifest.rs` tests at about lines 650, 668, 685, 733, 748, 782, 801 and 939; `m3u8_cache.rs:861`.
     - The "legacy" block of `options_derive_ring_sizes_from_time_and_keep_given_counts`: delete it.
     - AUG manifest tests: `render_bytes_remain_exact_across_buffer_reuse` (100 ms), `long_running_manifest_keeps_only_the_retained_timeline`, `non_independent_input_cannot_grow_an_open_segment_without_bound`, and `delta_update_*`, which takes LL's `6.000` form.
     - `benches/distribution_capacity.rs:167` (5 → 5_000), `benches/playlist_cache.rs:124` (1_000 → 1_000_000), `benches/playlist_writes.rs:127` (5 → 5_000).
   - Docs: README `:48` ("the old `part_target_ms` still works and wins when set") and `docs/dm-ll-hls-spec-gaps.md:265` ("or the older `part_target_ms`").
2. **`Options.eager_render`** (`src/lib.rs:90`, default at `:116`). Remove it, the eager branch in `M3u8Cache::add_part_us` (`m3u8_cache.rs:357` on LL), and the test `eager_render_renders_each_part_as_it_is_added`.
3. **`CacheMeshConfig.batched_send` / `with_batched_send(bool)`** (`mesh.rs:350-354, 403, 507-512`).
   - Remove the field, the builder, the `batched` variable (`:3026`) and the unbatched branches of `sync_task`, including the unbatched warm-up branch (`:3455-3499`, `:3584-3611`).
   - `FecSender::send_object` then has no caller: delete it.
   - Tests:
     - Delete `a_committed_part_leaves_at_once_without_batched_send` and `unbatched_four_thousand_parts_a_second_for_comparison`.
     - In `a_batched_pass_makes_one_call_for_many_datagrams`, drop the unbatched half.
     - Rename `batched_send_and_gso_are_on_by_default_and_can_be_turned_off` and drop its batched part.
     - Remove the `batched_send` parameter of `commit_to_apply` and `publish_to_two_children`.
4. **`CacheMeshConfig.sync_on_publish` / `with_sync_on_publish(bool)`.**
   - `978e596` kept it "so the old tick-only behaviour can be chosen at run time". Remove it.
   - Tests:
     - `sync_on_publish_is_on_by_default_and_can_be_turned_off` and `a_committed_part_waits_for_the_tick_without_sync_on_publish` go.
     - `a_live_part_is_not_held_behind_a_warm_up_window` needs another way to force a shared pass. Publish both streams before the edge subscribes, with a 200 ms tick, so that the first tick carries both.
     - `commit_to_apply` loses its `sync_on_publish` parameter.
   - Product: `av-mesh-run` has `--mesh-sync-on-publish` (`main.rs:856, 1685, 1723, 10378`) and a metric `av_mesh_transport_sync_on_publish`. Tell the product session that the flag goes.
5. **`with_fitted_symbols(bool)`** and **`with_gso(bool)`**.
   - These are opt-outs of the low-latency path. Fitted symbols become unconditional on the sync path. The `false` arm of `an_audio_part_goes_in_datagrams_of_its_own_size` is removed.
   - GSO keeps its automatic fallback (no `UDP_SEGMENT`, or the kernel refuses). The public builder goes. A `#[cfg(test)]` switch stays so `with_gso_a_large_part_keeps_symbols_that_fit_a_packet` can exercise the no-GSO path that non-GSO kernels take.
6. **Stored-playlist path in `M3u8Cache`.**
   - Remove `M3u8Cache::add(stream, seg, seq, idx, data)` (LL) and AUG's `add_for_handle` and `add_prepared_for_handle`, `end_segment`, and the `manifest: None` fallback in `render()`.
   - Nothing calls them in production: not `av-mesh-run`, `av-contrib`, `daw-nexus`, or `web-services` production code. Only the `web-services/hls` tests use `M3u8Cache::add` and `M3u8Manifest::add_part*`, and that crate is locked at `e107101`. Record this for whoever bumps it.
   - Tests to port to `add_part_us`: `tracks_last_playlist_position`, `exposes_current_open_segment_part_range`, `open_segment_range_ignores_stale_ring_boundary`, `reused_stream_slot_does_not_expose_previous_playlist_data`, `stale_handle_cannot_replace_new_playlist_or_initialization` (which uses private `prepare_variants`), `rejected_mapped_writes_do_not_evict_a_live_stream`, `stream_handle_is_bound_to_its_cache`, `update_notifier_wakes_for_publication_and_stream_reset`, and `delta_update_*`.
7. **Render-returning ms wrappers in `M3u8Manifest`.**
   - Remove `add_part`, `add_part_with_byte_len`, `add_part_with_byte_range`, `add_part_us` (which returns the tuple) and the private `rendered()`.
   - Callers move to `push_part_us` / `push_part_with_byte_len_us` plus `m3u8()`: LL manifest tests, AUG manifest tests, `benches/playlist_cache.rs:139` and `benches/playlist_writes.rs:170,176`.
8. **Numeric stream-index shims in `ChunkCache`.**
   - Remove `add_stream_id` (documented "Legacy alias"), `get_or_create_stream_idx`, `get_stream_idx`, LL's sync `stream_idx`, and `stream_ids() -> Vec<(u64, usize)>`. Use `stream_handles()`.
   - Also `take_new_playlists_rx` with `new_playlist_tx/rx`, and its LL-kept tests `test_new_playlist_notification_sent`, `test_no_notification_for_existing_stream_id`, `test_take_new_playlists_rx_single_use` and `test_notification_after_zero_stream_id`.
   - Callers:
     - LL mesh, via handles and `handle_at` (2.4).
     - LL chunk tests `a_new_stream_takes_a_free_index_before_it_replaces_a_stream`, `a_released_stream_frees_the_bytes_of_its_parts` (which reads `cache.buffer[idx]`: adapt to `ChunkSlot.bytes`), `a_cleared_slot_is_released_and_can_be_written_again`, `a_part_beyond_a_gap_signals_publication_only_when_the_gap_fills` and `a_publication_set_names_the_streams_that_advanced`. They use `resolve_or_create_stream` and `last_for_handle`.
     - `lib.rs` `test_fin_clears_chunk_cache_entry` (`:273`).
     - LL mesh tests that use `get_stream_idx` and `get(idx, ..)` (`:4538`, `:4558`, `:5335-5339`).
     - AUG `distribution_capacity --lookup raw` keeps raw lanes on unowned lanes only.
   - Raw-lane ops (`set`, `append`, `add`, `get`, `get_last`, `last`, `version`, `reset_stream_idx`, `update_notifier`) stay, for lanes the registry never assigns.
   - LL's long timing tests `test_append_and_last`, `test_concurrent_read_write` and `test_massive_concurrent_reads` were moved to benches on AUG. They stay removed.
9. **Wire compatibility in the mesh.**
   - `FRAME_VERSION_LEGACY = 1`: remove its acceptance in `MeshFrame::decode` and `chunk_frame_slot` (`mesh.rs:25, 243, 1397, 1421-1441`), and the `legacy_hello` case in `mesh_frame_roundtrips`.
   - `ReplicaRequest.count`: always write the u32 and require 20 bytes (`:1350-1354`, `:1461-1478`).
   - All nodes move together: the product moves av-mesh to the merged rev.
10. **Compatibility wording.**
    - `CacheMeshRole::Peer` doc ("Compatibility mode for existing local mesh fixtures") and `ReplicationMode::All` doc ("the original behavior"): reword. The roles themselves stay; tests and the default use them.
11. **`CHANGELOG.md`.**
    - Remove the "Cargo.toml version 0.2.0 ... never published" note, the whole `### Breaking` section, and the mentions of `part_target_ms` "kept and wins", `eager_render`, `with_batched_send`, `with_gso` and `with_fitted_symbols` as defaults with opt-outs.
    - Write one `[Unreleased]` entry that describes the merged design.
12. **README and TODO.md (AUG).**
    - Drop README "Migrate from numeric stream indices" (the old-to-handle table and the fixed-lane caveat; restate fixed lanes as current API).
    - Drop TODO.md "intentional compatibility restriction" wording and the 10-step downstream migration.
    - Rewrite TODO.md to the merged design: its `[x]` items for cache-delta-on-write, the snapshot budget, the ring eager-zeroing proof and replica opt-in are reversed or changed. Alternatively cut it to "External release qualification".
13. **Duplicate sync/async pairs (optional).**
    - Collapse `zero_stream_id` (async) / `zero_stream_id_sync` to one sync `zero_stream_id`. Callers: `Playlists::fin`, and the LL mesh test at `:5035`.
    - Leave `resolve_or_create_stream` async; `web-services` awaits it.

Kept because they are not compatibility code:

- `Playlists::add(stream, Fmp4)`: `Fmp4.duration` is u32 ms in `access-unit`.
- `add_us`.
- The mapped `*_for_stream_id` convenience methods.
- The replica constructors (2.3).
- `FecReceiver::new` and `FecSender::new` as `#[cfg(test)]` helpers.
- LL's lenient `parse_duration_us` f64 fallback, used only for foreign playlists.

---

## 5. Interaction tests to add

Each test is written against the merged API. Mesh tests are `#[cfg(all(test, feature = "mesh"))]` in `src/mesh.rs` and reuse LL's helpers (`publish_to_two_children`, `raw_child`, `wait_for_hello`, `demand_distributor`, `demand_edge`, the `RUNS` mutex, `THROUGHPUT_P99_US`).

### 5.1 Concurrent caches under mesh publication and receive at 4,000 parts/s

`a_distributor_and_its_edges_keep_four_thousand_parts_a_second_while_caches_churn_and_readers_block` runs in any build. The bound is release 2 ms and debug 20 ms, as in LL.

**Setup.**

- Extend `publish_to_two_children` with `Children::MeshEdges` and a `load: Load` argument. Each edge cache uses `Options { num_playlists: 8, max_segments: 2, max_parts_per_segment: 2_048, buffer_size_kb: 64, ..Default::default() }`, which wraps in about 1 s at 4,000/s.
- Every edge config carries `with_allowed_peers([distributor])`, and the distributor config carries the edges.

**Publisher.** On the distributor:

- `let h = cache.resolve_or_create_stream(7).await`.
- At each deadline, `cache.add_for_handle(h, slot, payload(slot, h.generation()))`, where `payload` encodes the slot and generation in its first 16 bytes (72 bytes total, so the parts are fitted).

**Concurrent load on the distributor.**

- *Churn task.* Loop `resolve_or_create_stream(1000 + k)`, `add_for_handle(.., 0, ..)`, `release_stream_id(1000 + k)`. With 8 lanes and the free list, stream 7 must never be evicted: assert `resolve_stream(7) == Some(h)` at the end.

**Concurrent load on each edge.**

- `R = 8` reader tasks. Each loops: `resolve_stream(7)`, then `last_for_handle`, then `get_for_handle(h, last)`. Each asserts that the payload's slot equals the id read and that its generation tag is the one the edge saw first for stream 7.
- One blocking reader per edge: `exact_part_waiter(7, next)`, `notified()`, `enable()`, recheck `get_for_stream_id(7, next)`, await. It records the wake time against `AppliedSlot.committed_us` from `with_applied_tx`.
- One `M3u8Cache` per edge fed from `applied_tx`: `add_part_us(7, 2_500, slot % 400 == 0, Some(len))`. Two readers alternate `last(7)` and `last_delta(7)`, and assert that two reads of the same `version_for_handle` share a pointer.

**Assertions.**

- Zero missing parts at every edge, and p99 publish→`rx_first_us` under `THROUGHPUT_P99_US`, as before.
- No read ever returns a payload whose slot or generation tag mismatches.
- Every blocking reader woke for every part.
- `fec_stats()` on the distributor shows `rx_decoded_objects` from the edges == 0 (no echo).
- `rx_unauthorized_datagrams == 0`.
- Distributor CPU per part (`CLOCK_THREAD_CPUTIME_ID`) is reported alongside the LL baseline from `ff09196` for the same run.

### 5.2 Generation safety across a restarted publication with fitted symbols

`a_restarted_publication_reaches_edges_whole_and_old_parts_never_reappear`.

**Setup.**

- Distributor and edge in `ReplicationMode::All` (and a second run in Demand, with `subscribe(7, 0, 16)` re-sent until restarted bytes arrive, as in LL's `a_restarted_stream_is_pushed_from_its_new_beginning`).
- Edge cache with a small ring (`max_segments: 2, max_parts_per_segment: 32`, capacity 64), so the restart lands on wrapped slots.

**Phase A.**

- Publish 200 parts of 40 bytes, `b'A'` plus the slot, so each is one fitted symbol.
- Wait until the edge has slot 199, then take `old_edge = edge_cache.resolve_stream(7)`.

**Restart.**

- `cache.zero_stream_id(7)`, then `h2 = resolve_or_create_stream(7)`.
- Assert `h2.generation() != h1.generation()`, `cache.get_for_handle(h1, 199)` is `None`, and `cache.last_for_handle(h1)` is `None`.
- Publish 0..50 with the same lengths and contents `b'B'` plus the slot. Same `(stream, slot, symbol_size)` and same length, so this checks that `EncodedKey`'s payload hash (`c316cf9`) forces a new encoding.

**Assertions.**

- For every slot in 0..50, the edge's `get_for_stream_id(7, s)` eventually equals the `B` bytes. Before the C7 resolution this fails: the old ids 192..199 occupy the wrapped slots.
- Never at any time an `A` payload for a slot whose `B` has been read: a reader task polls throughout.
- The distributor's encoded cache produced a distinct encoding for each restarted slot (`tx_objects` grows by at least 50 × children).
- On the distributor, `sent` and `sent_initializations` contain no `(7, h1.generation(), ..)` keys after the next prune interval (expose a `#[cfg(test)]` counter, or check that `served_subscriptions()` shows the new cursor).
- Also exercise `set_stream_initialization_for_handle(h2, init_b)`: the edge's `stream_initialization(7)` becomes `init_b`, never the old init.

### 5.3 Blocking readers woken by mesh-received parts

`blocking_readers_wake_for_parts_that_arrive_over_the_mesh`. Three readers on one edge are started *before* the distributor publishes anything:

1. **Exact-part waiter on a stream the edge does not have yet.** `exact_part_waiter(7, 0)` must be `Some` (C6) and must create no mapping (`resolve_stream(7) == None`). It enables, rechecks and awaits. On wake, `written_unix_us_for_stream_id(7, 0)` must be `Some` at once, with no await (`7260e74` across the receive path), and `≤ now`.
2. **Lane notifier.** After the first part creates the stream, `h = resolve_stream(7)` and `update_notifier(h.index())`. It enables, rechecks `last_for_handle(h)` and awaits. It must wake for part 1, and again after the distributor sends a new initialization.
3. **Publication subscriber.** `subscribe_publications()` plus `publication_notify()`. It enables and drains. The drained indices must contain `h.index()` exactly when the edge's watermark advances. A part beyond a gap (drop slot 3 with `receive_loss_ppm`, or send 4 before 3 through a raw child) must not mark the set until the hole fill (`request_parts`) lands.

Then the stale handle:

- The distributor restarts stream 7 (`zero_stream_id`) and the edge applies the restarted stream (5.2).
- A reader that holds the old edge handle gets `None` from `last_for_handle` and re-resolves.
- The `M3u8Cache` variant: `update_notifier_for_handle(old)` is `None` after the edge's playlist stream is zeroed.

Timing: each wake is within 10 ms of `AppliedSlot.committed_us` in debug and 2 ms in release.

### 5.4 Smaller in-crate interaction tests

- `playlists_fin_racing_add_us_leaves_no_ghost_slot`. Threads loop `Playlists::add_us(7, ..)` while another calls `fin(7)`. After the joins and one final `fin`: `active() == 0`, `m3u8_cache.position(7)` is `None`, `chunk_cache.resolve_stream(7)` is `None`. This adapts `closing_entry_rejects_add_and_cannot_resurrect_cache_state` to the manifest-less entry.
- `concurrent_part_adds_publish_monotonic_positions`. Two threads call `add_part_us` on one stream while readers poll `position_for_handle`. Positions never go backwards and `get_idxs_for_handle` ranges are never torn. This replaces AUG's ring Superseded tests.
- `a_render_on_read_is_not_published_by_notifier_but_the_part_is`. Enable `update_notifier_for_handle`, call `add_part_us`: the waiter wakes, and the slot's `rendered` is still `None` until the first read.

---

## 6. Per-file resolution plan, in order

Rule: for `chunk_cache.rs`, `m3u8_cache.rs`, `m3u8_manifest.rs`, `lib.rs` and `mesh.rs`, start from the side named below. Do not keep any auto-merged interleave. Port the other side by feature, then port its tests. Build and run `cargo test --all-features` (debug) after each file. The release-only 4,000/s edges test runs after `mesh.rs`.

**Step 1. `Cargo.toml`** (1 hunk, `fd7d8fd:39-62`).

- Keep both sides: the LL `[profile.dev.package.raptorq]` block, then AUG's four `[[bench]]` sections.
- The rest auto-merged correctly (arc-swap, the mesh feature, optional deps, version 0.2.0).
- Keep the merge-tree `Cargo.lock`.

**Step 2. `src/stream_registry.rs`** (no conflict).

- Add the free list in `Lifecycle`, `remove_stream -> Option<ResolvedStream>` and `handle_at(index)`.
- New tests: `a_free_index_is_taken_before_any_stream_is_evicted`, and `handle_at_is_none_while_reassignment_is_unpublished` (mirrors `resolve_rejects_an_identity_while_reassignment_is_unpublished`).

**Step 3. `src/lib.rs`** (5 hunks). Start from LL.

- Hunk `15-22` (imports): `BTreeMap`, `AtomicBool`; drop `BTreeSet`.
- `208-212`: take LL's.
- `277-294` and `328-397` (Playlists): take AUG's entry and `fin` protocol with `PlaylistEntry {closing, commit: Mutex<()>}`. `add_us` holds `commit` across `ensure_stream_id`/`set_init`/`add_part_us` and propagates errors. Keep the `try_new`/replica constructors.
- `Options`: remove `part_target_ms` and `eager_render`, and the alias logic in `part_target_us()` and `normalized()`.
- `CacheError`: keep AUG's `ArithmeticOverflow` and `StreamNotFound`. Drop `Superseded` if it has no producer after step 5.
- Tests `466-558`: union. Keep `parts_are_added_in_microseconds`, `test_fin_clears_chunk_cache_entry` (handles), `closing_entry_rejects_add_and_cannot_resurrect_cache_state` (adapted), `playlist_add_reports_an_initialization_budget_failure` and `playlists_constructor_rejects_capacity_arithmetic_overflow`. Add 5.4.

**Step 4. `src/m3u8_manifest.rs`** (11 hunks). Start from LL.

- Hunks `4-16`, `32-38`: LL imports plus the two rendition constants.
- `153-258`, `289-298`: LL structs. No `RenderHeaders` or `render_buffer` fields unless the head precompute goes in, in which case add `head_without_skip` and `head_with_skip` built in `new()`.
- `305-454`: LL rendering, then the port:
  - `append_program_date_time` from AUG, rewritten to take `DateTime<Utc>` or µs and called by `program_date_time_line`.
  - The rendition caps in `set_rendition_reports`.
  - Delete the ms wrappers and `rendered()` (section 4, item 7).
- `503-637`, `775-929`: LL helpers.
- Tests `1155-1626`: keep all LL tests (minus the legacy `part_target_ms` block) and add AUG's, rewritten:
  - `render_bytes_remain_exact_across_buffer_reuse` becomes `render_bytes_are_exact` (microsecond fixture: `0.100`, `3.000`, `0.300`; byte ranges, preload hints, rendition reports). It renders twice and asserts equal bytes.
  - `direct_program_date_time_format_matches_chrono_rfc3339` as is.
  - `long_running_manifest_keeps_only_the_retained_timeline` (20,000 segments) uses `closed` and `open.start_us`.
  - `non_independent_input_cannot_grow_an_open_segment_without_bound` moves to `m3u8_cache` (C5).

**Step 5. `src/m3u8_cache.rs`** (16 hunks). Start from LL.

- Hunks `1-38`: imports from both (registry, `arc_swap`, `Notify`, `Arc`, the replica thread-local) plus LL's `PLAYLIST_GZIP_LEVEL`.
- `46-188` (types): keep LL's `Rendered` and `PlaylistSlot`. Add AUG's `M3u8StreamHandle`, `PlaylistPosition`, `M3u8CacheMemoryStats`, `StreamInitialization` and `SegmentRange`. Fold `position`, `segment_ranges` and `version` into `PlaylistSlot`.
- Drop `CachedPlaylist`, `PlaylistSnapshot` and `LatestPlaylistSnapshot`. If replicas are kept, add `RenderedVariant` and `published: ArcSwapOption<..>` per slot.
- `250-376`, `432-514` (constructors, identity): AUG's `try_new`/`try_new_with_latest_read_replicas`, checked arithmetic, `registry`, `update_notifiers` and tagged `inits`. LL's `slots` and `render_locks`.
- `666-915` (write and read paths):
  - LL's `add_part_us` plus validate-first, publish position and range in the slot lock, `next_version`, then `notify_update`.
  - LL's `render`/`render_delta`, made generation-u64 and handle-checked, plus the fast path and replicas (provisional).
  - AUG's `get_idxs_for_handle`, `position_for_handle`, `version_for_handle`, `get_for_handle`/`get_delta_for_handle`/`last_for_handle`/`last_delta_for_handle` (LL semantics: newest once included), `last_text`, `update_notifier_for_handle` and `memory_stats`.
  - Delete `add`, `add_for_handle`, `add_prepared_for_handle`, `end_segment`, `prepare_variants` and `calculate_index`.
- Unknown-stream semantics: LL. `last`/`last_text`/`last_delta` return `Err(StreamNotFound)`.
- Tests `947-1405`, `1902-1977`:
  - Keep LL's `a_part_is_added_without_rendering_and_read_renders_once`, `a_segment_is_capped_at_its_part_capacity`, `default_options_take_every_frame_of_a_long_gop`, `a_reused_slot_starts_a_new_playlist` and `delta_update_reads_small_part_durations_exactly`.
  - Port AUG's tests per section 4, item 6, and drop the ring tests per C1.
  - Keep the replica tests if the replicas stay, rewritten to render-time copies.
  - Add `a_full_segment_refuses_non_independent_parts` (from C5) and 5.4.

**Step 6. `src/chunk_cache.rs`** (10 hunks). Start from AUG.

- Hunks `59-104`: keep both `PublishedStreams` (LL) and `ChunkCacheMemoryStats` (AUG).
- `139-170` (struct): AUG's fields plus `published: Notify`, `publication_sets`, `slot_written_us`, `slot_tag_id` and `slot_tag_generation`. No `idx`, `offsets` or `new_playlist_*`.
- `185-249`: AUG's `try_new` plus initialization of the LL fields.
- `254-378`: LL's `publication_notify`/`subscribe_publications`/`signal_publication`, and AUG's `resolve_stream`/`resolve_or_create_stream`/`stream_handles`. Delete the numeric shims and `take_new_playlists_rx`. Add `pub(crate) fn handle_at(idx) -> Option<StreamHandle>`.
- `828-888`, `1179-1366` (write and publish paths): AUG's slot writes plus the write-time tag stores. `update_state_after_slot` and `publish_contiguous_last` return `advanced`, and callers signal after dropping locks. Add `pub(crate) add_remote_for_handle` (C7).
- New: `written_unix_us_for_stream_id`, `clear_for_stream_id`, `release_stream_id` (2.1, `ChunkCache`).
- `exact_part_waiter`: LL key and semantics (C6).
- Tests `2481-2684`: union, minus the removed channel and timing tests. Port the 8 LL-only tests to handles and `ChunkSlot`. Rewrite the two AUG waiter tests per C6.
- Gate: run the product bench here (chunk write cost, provisional item in 2.1) before moving on.

**Step 7. `src/mesh.rs`** (35 hunks). Start from LL; `mesh_send.rs` is LL-only and unchanged apart from `max_frame_bytes` if `prepare_object` lives there.

| Hunks | Resolution |
|---|---|
| `16-20`, `331-437` (consts, config) | LL config plus AUG's `allowed_peers`, `max_peers`, `max_inflight_frames`, `max_frame_bytes` (default from the cache), `max_remote_slots` (default from the cache), `replica_queue_capacity` and builders. No `frame_queue_capacity`. Delete `batched_send`, `sync_on_publish` and the `fitted_symbols`/`gso` builders. `CacheMeshRole` derives `Default`. |
| `481-713` (stats, shared state, handle) | Union `CacheMeshFecStats`/`Counters` minus `frame_queue_*`. One `Arc<MeshShared>`. Handle fields as LL, plus `max_peers`; `add_peer` via AUG. |
| `865-1070` (start, tasks) | LL's `bind_socket` and the three LL tasks, plus AUG's `replica_task` (LL sender lane, `count`). No `frame_task`. `start()` filters configured peers through the allowlist and `max_peers`. |
| `1303-1313` (frames) | `MeshFrame::decode(Bytes)`, `read_string(&mut Bytes)`. Remove v1 and the optional count. |
| `1768-1941` (`FecSender`) | LL's, plus the `max_frame_bytes` check in `send` and `prepare_object`. |
| `2162-2180`, `2471-2478`, `2642-2705` (`FecReceiver`) | Merge field sets. `with_limits(..).with_measured_loss(..)`. Prune every datagram with `record_block` on expiry and the grace on recoveries. `expire_block` on decode error. Oversized rejection first. **Fix the auto-merged recovery loop**, which lost `LATE_SOURCE_GRACE`. |
| `2736-2892` (`receive_task`, `FrameHandler`) | LL's inline receive with the allowlist inserted before the observer. `FrameHandler` keeps `rx_first_us`/`decoded_us`, gains `replica_tx`, and loses `reply_sender`. Chunk apply in the order of 2.4. Hello via `admit_peer`. |
| `3045-3546` (`sync_task`) | LL's, re-keyed by handle and generation via `handle_at`, with AUG's `sent` pruning at the 250 ms cadence and the stale-handle break. Unbatched branches deleted. |
| `3751-4202` (tests) | Union. Take AUG's `replica_response_stops_when_its_stream_index_is_reused` (adapted to the `count` signature), the `fec_receiver_*` bound tests, `mesh_drops_unauthorized_datagrams_before_fec_decode` and `rejected_runtime_peer_does_not_expand_the_allowlist`. Add `with_allowed_peers` where C9 requires. Apply the test removals of section 4, items 3–5 and 9. Add 5.1–5.3. |

Gate: `cargo test --release --all-features mesh::` (both 4,000/s tests), then the product bench, audio only and with 4K, p50, p99 and CPU per part, against the `ff09196` baseline.

**Step 8. Benches.**

- `distribution_capacity` / `chunk_workloads` / `mesh_pipeline`: switch to `part_target_us`. In `mesh_pipeline`, remove `with_frame_queue_capacity` and the `frame_queue_*` report fields, and add `with_allowed_peers` where needed. Expect one start-up push of retained parts in replica mode.
- `playlist_cache` / `playlist_writes`: port to `push_part_us`/`add_part_us`, with a read that renders after each write, so the numbers measure render-on-read. These are the authority for the provisional items in 2.1 (`M3u8Cache` read fast path), 2.2 (head precompute) and 2.3 (replicas).

**Step 9. Docs.**

- `README.md` (3 hunks, but resolve as a rewrite): keep AUG's structure, then:
  - Rewrite Concurrency design, Capacity and memory, and Operation costs for render-on-read and one rendering per stream.
  - Fold in LL's "Time and sizes" (no alias) and "Send path" (no toggles).
  - Mention that the mesh's `max_frame_bytes` follows `buffer_size_kb`.
  - Drop the Migrate section and the stale numbers (C12).
- `TODO.md` per section 4, item 12.
- `CHANGELOG.md` per section 4, item 11.
- `docs/dm-ll-hls-spec-gaps.md:265`: drop the `part_target_ms` mention.

**Callers outside this crate.** Tell the product session; do not edit its worktrees.

- **`av-mesh-run`:**
  - Move to handles (C2).
  - `--mesh-sync-on-publish` goes (section 4, item 4).
  - Check that 4K inits fit `init_size_kb` and that `--slot-kb` covers the largest part (C11).
  - Distributors must list or discover every child (C9).
  - `chunk_frame_payload` is unchanged.
  - The mesh wire stays at v2, but v1 hellos and 16-byte replica requests are no longer accepted, so all nodes move together.
- **`av-contrib`:** off crates.io 0.1.6 onto the merged rev. Its `Options` literals need `..Default::default()` and lose `part_target_ms`.
- **`web-services`:** locked at `e107101`. On update, its hls tests move from `M3u8Cache::add` and `M3u8Manifest::add_part*` to `add_part_us`, and `get_for_handle` now returns the newest playlist that contains the part.
- **Open item for the product session:** an origin epoch on the wire, so that relays and edges follow a restarted stream (C7).

---

## 7. Resolution notes

### Steps 1, 2 and 4: Cargo.toml, stream_registry.rs, m3u8_manifest.rs

- **Cargo.toml.** Both sides kept. The `[profile.dev.package.raptorq]` block is placed after all five `[[bench]]` sections, not between them: TOML meaning is the same and the benches stay together. `Cargo.lock` is the merge-tree result, untouched.
- **Registry free list.** As planned: `Lifecycle {next_index, free: VecDeque, is_free}`, seeded `0..capacity`. `remove_stream` and `reset_index` free the index after the reset closure has run. The churn pattern (2 lanes, no removals) still assigns 0, 1, 0, 1, ...
- **`remove_stream`** returns `Option<ResolvedStream>`. Deviation: it now checks the owner before it unpublishes the cell. AUG cleared `accepting` and the published generation first and then returned on an owner mismatch, which would have left the current owner's gate closed. The mismatch cannot happen under the lifecycle lock; the reorder only removes that hazard.
- **`handle_at` and the seqlock.** `handle_at` builds the identity from the published stream id and generation, as `is_current_fast` does. Because `handle_at` *returns* the stream id it read (`is_current_fast` only compares it), the two-load check was made a proper seqlock: a release fence before the writer stores the new stream id, and an acquire fence before the second generation load, in both `handle_at` and `is_current_fast`. On x86 both fences are compiler barriers only.
- **Manifest head precompute: done.** `head_without_skip` and `head_with_skip` are `Arc<str>` built in `new()` from the normalized options. `RenderedPieces` carries `media_sequence: u32`, which `into_bytes` writes into the single exact-length buffer. Checked in a scratch crate against LL's `m3u8_manifest.rs`: byte-identical output over 5 option and part mixes (2.5 ms, 250 µs, 23.976 fps byte ranges, CAN-SKIP, parted limits). Allocations per render (default options, 4,000 parts, a read after each) went from 6.9 to 2.9, and render time went from 11.9 ms to 9.3 ms in total. This is the evidence for the provisional item in 2.2. The ported `playlist_writes` bench (step 8) should still confirm it.
- **Rendition reports.** Deviation: they are rendered into text once, in `set_rendition_reports`, rather than on every render. The caps (256 reports, URIs of at most 2,048 bytes) and LL's quoted-string filter are applied there, before the count cap, so invalid URIs do not use up the 256. The version still increments. New test: `rendition_reports_are_capped_in_count_and_uri_length`.
- **PDT.** AUG's `append_program_date_time` is used directly by `close_segment` and by the open-segment tail. `program_date_time_line` is deleted. `time_at` now uses `checked_add_signed` and saturates at `DateTime::<Utc>::MAX_UTC`; LL's `+` panicked on overflow.
- **Tests.** `render_bytes_are_exact` (also asserts `pieces().len()` equals the rendered length), `direct_program_date_time_format_matches_chrono_rfc3339` (also bounds the line length) and `long_running_manifest_keeps_only_the_retained_timeline` (also checks the open segment's PDT at +19,999 s) are ported. New: `rendered_pieces_know_their_exact_length`. The `part_target_ms` block of `options_derive_ring_sizes_from_time_and_keep_given_counts` is replaced by a check that given counts are kept. All 24 manifest tests and 6 registry tests pass in scratch crates that use the step-3 `Options` (no `part_target_ms` or `eager_render`).
- **Callers still to update in other steps.** Removed `M3u8Manifest::{add_part, add_part_with_byte_len, add_part_with_byte_range, add_part_us}` are still called by AUG `lib.rs` (`add_part_with_byte_len`, step 3) and `benches/playlist_cache.rs`/`playlist_writes.rs` (step 8). `non_independent_input_cannot_grow_an_open_segment_without_bound` belongs to step 5 (C5).

### Step 6: chunk_cache.rs

Built from AUG, with the LL features ported onto it. Checked in a scratch crate (`/tmp/claude-1000/plmerge-scratch/chunk`, the step-3 `Options`, with the merged `stream_registry.rs`): 50 tests pass, in debug and release. Clippy is clean, with and without `mesh`.

- **Stream state is atomic (the provisional "write cost" item in 2.1, done).** AUG's `StdRwLock<ChunkStreamState>` is replaced by `StreamState {last: AtomicUsize (EMPTY = usize::MAX), next_id: AtomicUsize, version: AtomicU64}`.
  - Writers change it only inside `with_validated` or `with_generation`, under the owner read lock. Resets run under the owner write lock, so no value crosses a generation.
  - `raise_last` is a CAS loop that returns `advanced`. `version` never becomes 0.
  - No other lock is taken per part, beyond the slot lock and the owner read lock.
- **Write tag.** The three atomics per slot are one `Vec<SlotTag {id, generation, written_us}>`, not three Vecs. It is stored under the slot lock in the order of 2.1.
  - `written_unix_us_for_handle` and `written_unix_us_for_stream_id` recheck the id, the generation and `is_current_fast` after loading the time.
  - Clear and release reset the tag.
- **Contiguous publication.** It now scans the slot tags, without slot locks or awaits, inside one `with_validated` region that also raises `next_id`. AUG used two regions and an await per slot.
  - A `fence(SeqCst)` comes before the scan. Two writers that fill each other's gap therefore cannot both miss the other's tag.
  - New test: `concurrent_contiguous_writers_publish_every_filled_gap`.
- **Publication.** `signal_publication` fires only when `last` strictly advanced, after the slot lock is dropped, and before the exact-part and lane notifiers.
  - This covers raw-lane `add` and `append` too: LL's `a_publication_wakes_a_task_waiting_for_it` uses `append(0, ..)`.
  - Nothing else signals: repeats, conflicts, `Superseded` writes, rejected writes, raw `set`, inits, clear, release, zero and reuse. New test: `only_an_advancing_watermark_signals_publication`.
  - Deviation: AUG's `put_if_absent` also publishes on `AlreadyPresent`. That is kept, but it signals only if it moved the watermark. LL published on `Inserted` only.
- **`release_stream_id`** uses the identity that `remove_stream` returns. It empties the lane's slots whose generation is `<=` the released generation, not `==` as the plan said. That also frees hidden bytes from earlier `zero_stream_id` calls, as LL's release did, and never touches a stream that has taken the index since, because generations only grow on an index. New test: `a_release_keeps_the_parts_of_a_stream_that_took_the_index_since`.
- **`clear_for_handle`** was added next to `clear_for_stream_id`, and `written_unix_us_for_handle` next to `written_unix_us_for_stream_id`.
- **`handle_at` is `pub`, not `pub(crate)`.** `subscribe_publications` and `PublishedStreams::drain` are public and return indices. Once `stream_idx` and `stream_ids` are removed, `handle_at` is the only way an outside consumer can name those indices.
- **`add_remote_for_handle` (C7)** is `pub(crate)` and has `allow(dead_code)` without `mesh`. Test: `a_remote_write_replaces_a_newer_id_of_the_same_generation`.
- **`zero_stream_id`** is now one sync method (section 4, item 13, done here), matching `M3u8Cache::zero_stream_id`. `zero_stream_id_sync` is gone.
- **Removed:** `add_stream_id`, `get_or_create_stream_idx`, `get_stream_idx`, `stream_idx`, `stream_ids`, `take_new_playlists_rx`/`new_playlist_*` and their four tests, and LL's three timing tests.
- **Raw lanes and ids.** Writes at id `usize::MAX`, the empty marker, are refused with "Chunk id out of range". `update_notifier` stays and also fires for mapped streams; new test `a_lane_notifier_wakes_for_a_mapped_stream`.
- **Hash.** The slot hash moved from `const_xxh3::xxh3_64`, a scalar const evaluator, to the runtime `xxh3::xxh3_64`. The values are identical (`the_slot_hash_is_xxh3_64`), so the mesh's `EncodedKey` is unaffected.
- **Write cost.** Measured with one current-thread writer, `add_for_stream_id` (LL, AUG) versus `resolve_or_create_stream` + `add_for_handle` (merged), with one publication subscriber, best of 5. Scratch bench `/tmp/claude-1000/plmerge-scratch/bench`.

  | Payload | LL | AUG | Merged |
  |---|---|---|---|
  | 72 B | 356 ns | 299 ns | 269 ns |
  | 1,200 B | 1,215 ns | 1,161 ns | 324 ns |
  | 256 KiB | 205 µs | 203 µs | 10.8 µs |

  With the const hash, the merged structure alone costs 363 ns, 1,219 ns and 203 µs: the same as LL. The win comes from the hash. The product bench gate of step 6 was not run here, because the crate does not build until steps 3, 5 and 7 are resolved. It is still owed after `mesh.rs`.
- **Tests.**
  - AUG tests kept, with handles in place of `add_stream_id` and `stream_ids`.
  - LL's two exact-waiter tests run without creating the stream first (C6).
  - AUG's `exact_part_waiter_does_not_cross_a_stream_generation` became `a_waiter_woken_across_a_generation_finds_no_data_from_the_previous_one`, per C6. It asserts the wake, then that neither the old handle nor the stream id reads old-generation data.
  - `waiter_for_unknown_stream_does_not_evict_a_live_stream` now asserts `Some`, per C6.
  - The 8 LL-only tests are ported to handles. `a_released_stream_frees_the_bytes_of_its_parts` checks `ChunkSlot.id`/`bytes`.
  - New: the tests named above, plus `a_write_time_is_bound_to_the_generation_that_wrote_it` and `handle_at_names_the_stream_of_a_drained_index`.

### Step 5: m3u8_cache.rs

Built from LL (render on read, O(1) `add_part_us`, `render_locks`, µs delta parsing), with AUG's identity and reader features ported onto it. Checked in a scratch crate (`/tmp/claude-1000/plmerge-scratch/m3u8`: the merged `m3u8_cache.rs`, `m3u8_manifest.rs` and `stream_registry.rs`, with the step-3 `Options`): 28 cache tests pass in debug and release, the two concurrency tests pass five runs in a row, and clippy and rustfmt are clean.

- **Identity.** `StreamRegistry` plus `M3u8StreamHandle`, with `resolve_stream`, `resolve_or_create_stream`, `ensure_stream_id` and `zero_stream_id` (which frees the slot). `PlaylistSlot.generation` is the registry's u64. The reset runs under the slot lock before the new generation is published, so a handle write checks `is_current_fast` (registry and bounds) and then `slot.generation` under the slot lock, without taking the owner lock per part. `set_init_for_handle` keeps AUG's `with_validated`.
- **One atomic position (deviation in form).** The generation, version and newest part are published per slot as a small sequence lock (`PublishedState`), written only under the slot lock. That keeps LL's lock-free `get`/`last_position` and AUG's single consistent position without an allocation per part, which an `ArcSwap` of the position would cost. Segment ranges are tagged as in AUG and live in the slot, read under its lock (`get_idxs_for_handle`).
- **Versions** continue across reuse (`next_version`, never 0) and advance on parts, initializations and resets (C10). A lazy render does not change the version or notify.
- **Read fast path: kept.** Per slot `rendered: ArcSwapOption<Rendered>`, tagged with generation and version. A hit takes no lock. `Rendered` also holds the delta, made once per version by the first reader that asks, in a `OnceLock` under the render lock; this replaces LL's `slot.delta`. LL's `slot.rendered` field is gone; the 5.4 test checks the `ArcSwap` instead.
- **Replicas: kept, made at render time.** `Variants` copies the gzip of the full and delta playlists once per extra replica when a reader renders them; a reader picks `LATEST_READ_REPLICA % n`. One replica (the default) makes no copies.
- **Scratch measurement** (release, 32 cores, default options, one stream of 4,000 parts of 2.5 ms; `examples/readers.rs` in the scratch crates, LL's cache in `m3u8-ll`). These are scratch numbers; the ported `playlist_cache` bench (step 8) should confirm them.

  | Workload | LL | Merged |
  |---|---|---|
  | `last(1)` hit, 1 reader | 18.7M/s | 13.3M/s |
  | `last(1)` hit, 8 readers | 3.0–3.5M/s | 9.5–11.2M/s |
  | `last_delta(1)` hit, 8 readers | 2.3M/s | 6.2–7.3M/s |
  | `last_delta_for_handle` hit, 8 readers, 1 / 8 replicas | n/a | 10.8M / 92–98M/s |
  | `last_for_handle` hit, 8 readers, 1 / 8 replicas | n/a | 16–17M / 133–136M/s |
  | writer at 4,000 parts/s, 1 `last` reader: `add_part_us` p50 / p99 | 0.56–0.82 / 53–86 µs | 0.67–1.23 / 2.0–4.8 µs |
  | writer at 4,000 parts/s, no reader: p50 / p99 | 0.24–0.27 / 2.1–3.2 µs | 0.27–0.28 / 2.0–5.2 µs |

  Given up: a single uncontended mapped hit costs about 30 ns more (85 ns against LL's 53 ns). Most of that is the registry's `resolve` (about 30 ns, a `RwLock<HashMap>` read); a handle read is 55 ns. With eight mapped readers the shared registry read lock, not the payload, becomes the limit, which is why replicas help handle readers (9×) and not mapped ones.
- **Stored-playlist path removed** (section 4, item 6): LL's `add` and `end_segment`, the `manifest: None` fallback in `render()`, the `eager_render` branch and its test, and all of AUG's ring (`buffer`, `latest`, `calculate_index`, `prepare_variants`, `add_for_handle`, `add_prepared_for_handle`, the snapshot types). The per-snapshot `buffer_size_kb` budget goes with them; `buffer_size_kb` no longer bounds playlists.
- **Added** `add_part_us_for_handle` (AUG's handle write, in LL's form) and `last_text_for_handle`. Unknown streams: `last`, `last_text` and `last_delta` return `StreamNotFound` (LL); `get`, `get_delta` and `get_idxs` return `Ok(None)`; a stale handle gets `StreamNotFound` (AUG). `last_position` is `None` until the first part (AUG; LL gave `(0, 0)`).
- **Validate before create.** `add_part_us` creates the stream only when it is unmapped, and a first part can never be refused, so a refused part never evicts. Reads and `set_init` never create a stream. `rejected_mapped_writes_do_not_evict_a_live_stream` is rewritten to those cases (refused part, oversized init, init for an unknown stream, reads of unknown streams).
- **`memory_stats`** counts slots with a manifest, the rendered text, every gzip replica of the full and delta playlists, and initializations. Deviation: `maximum_payload_bytes` is a per-stream bound from the options (per-line maxima × `max_segments` × `max_parts_per_segment`, plus zlib's compress bound per replica) times `num_playlists`, plus `init_size_kb` per slot. New test `the_rendering_bound_holds_for_the_largest_lines` checks it with the widest numbers.
- **Hash.** Playlist hashes use the runtime `xxh3::xxh3_64`, as `ChunkCache` now does; the values equal `const_xxh3`.
- **Tests.**
  - LL's tests are kept. `eager_render_renders_each_part_as_it_is_added` is deleted. `a_part_is_added_without_rendering_and_read_renders_once` checks the `ArcSwap`. `a_reused_slot_starts_a_new_playlist` also asserts that versions continue across reuse.
  - AUG's tests from section 4 item 6 are ported to `add_part_us`, with the same assertions. The positions and sequences come from the manifest, so the fixtures use 1 s independent parts to place segments, for example `get_idxs(7, 5) == (5, 7)` in place of `(25, 27)`. `open_segment_range_ignores_stale_ring_boundary` now also wraps the range ring and checks that the reused position is not read as the old segment. `delta_update_*` use the cache. `stale_handle_cannot_replace_new_playlist_or_initialization` uses a delayed write with the old handle in place of `prepare_variants`.
  - The replica tests are rewritten to render-time copies. Byte equality across two caches no longer holds, because each manifest has its own PROGRAM-DATE-TIME, so they check equal bytes in distinct allocations, hashes, and the memory accounting. The concurrent replica test now interleaves 2,000 `add_part_us_for_handle` calls with 8 readers.
  - `encoded_payload_memory_plateaus_after_ring_rotations` is rewritten over slot memory (C1). The five ring tests are dropped (C1). `constructor_rejects_capacity_arithmetic_overflow` also covers `init_size_kb`.
  - New tests: `non_independent_input_cannot_grow_an_open_segment_without_bound` (C5, at cache level), `a_full_segment_refuses_non_independent_parts`, `a_segment_range_is_bound_to_its_stream_lifetime`, `concurrent_part_adds_publish_monotonic_positions` (5.4) and `a_render_on_read_is_not_published_by_notifier_but_the_part_is` (5.4).
- **For step 3 (lib.rs).** Nothing in `M3u8Cache` produces `CacheError::Superseded` any more, so drop it.

### Step 3: lib.rs

Built from LL, with AUG's `Playlists` protocol ported onto it. With `lib.rs` resolved the non-mesh crate builds: `nice cargo build`, `nice cargo test` (122 lib tests, debug and release) and `nice cargo clippy --lib --tests --bins -- -D warnings` are clean. `--all-targets` still fails in the benches only (step 8: `part_target_ms`, `M3u8Manifest::add_part`, `M3u8Cache::add`/`add_for_handle`, and a deprecated `fetch_update` in `benches/support/mod.rs`).

- **Options.** LL's, without `part_target_ms` and `eager_render`. `part_target_us()` is `part_target_us`, or the default when it is zero; `normalized()` no longer keeps a millisecond copy. New test `options_normalize_the_part_target_in_microseconds`.
- **`CacheError`.** LL's set, which already had `ArithmeticOverflow` and `StreamNotFound`. `Superseded` is dropped: nothing produces it (`ChunkCache` uses `PutIfAbsentResult::Superseded`).
- **`Playlists`.** AUG's `BTreeMap<u64, Arc<PlaylistEntry>>` with `PlaylistEntry {closing, commit: Mutex<()>}`, AUG's `fin` protocol (closing, commit lock, zero both caches, `ptr_eq` removal), the init-size precheck before an entry is made, `try_new`, the replica constructors and `PlaylistCacheBundle`. `add` (ms) calls `add_us`. Under the commit lock `add_us` resolves the playlist stream once (resolve, else create, as `M3u8Cache::add_part_us` does), then calls `set_init_for_handle` and `add_part_us_for_handle` with that handle, and returns false on any error. The live-stream count uses `self.options.num_playlists`. Both caches' `zero_stream_id` are sync.
- **Tests.** `parts_are_added_in_microseconds` (LL), `test_fin_clears_chunk_cache_entry` (handles; `fin` is now sync, so the 200 ms polling loop became a direct assertion), `closing_entry_rejects_add_and_cannot_resurrect_cache_state` (holds `commit`; also asserts no playlist stream), `playlist_add_reports_an_initialization_budget_failure`, `playlists_constructor_rejects_capacity_arithmetic_overflow`. New: `an_initialization_is_stored_with_its_part`.
- **5.4 `playlists_fin_racing_add_us_leaves_no_ghost_slot`.** Deviation in form: the plan's "threads loop `add_us` while another calls `fin`, then one final `fin`" cannot see a ghost, because AUG's `fin` zeroes the caches even when there is no entry, and a later add makes a new entry that hides it. The test runs 2,000 rounds of one `add_us` against one `fin` behind a barrier, and after each round asserts that a playlist stream exists exactly when its entry does and that `active()` matches. Then a final `fin` must leave `active() == 0`, no entry, `position(7) == None`, `occupied_slots == 0` and no chunk stream (one is created each round). Checked by mutation: with the commit lock removed from `add_us` it failed in 5 of 5 runs, and it passed 10 of 10 runs with the lock. A check that both outcomes of the race occur was tried and dropped: the add after `fin` almost never happened, so it was flaky.
- **Also fixed** in `m3u8_manifest.rs`: clippy's `manual_is_multiple_of` in `push_seconds` (the existing LL warning noted in step 4), which `-D warnings` refuses.
- **Not changed.** `tls-helpers` is an unused dependency on both lines; cargo warns about it (`cargo::unused_dependencies`), which `-D warnings` does not cover. Left for whoever owns `Cargo.toml` cleanup.

### Step 7: mesh.rs, mesh_send.rs

Built from LL (`ff09196`, the merge's HEAD side), with AUG ported by feature. `mesh_send.rs` is unchanged. Gates: `nice cargo build --all-features`, `nice cargo test --all-features` (192 passed, 2 LL-ignored) and `nice cargo test --release --all-features mesh::` (66 passed, both 4,000/s tests included) are green, and `cargo clippy --all-features --lib --tests --bins -- -D warnings` is clean.

- **Send path (LL, unchanged in behaviour).** Send on publish always, `ObjectEncoder` with its caches, `sendmmsg` batches always, GSO with automatic fallback, fitted symbols always.
  - Removed: `sync_on_publish`, `batched_send` with the unbatched branches and `FecSender::send_object`, the `with_gso`/`with_fitted_symbols` builders (and `FecSender::with_fitted_symbols`, `from_config`).
  - GSO's only switch is a `#[cfg(test)]` config field (`without_gso()`).
  - Deviation: fitted symbols now apply to every `FecSender`. The handle's control frames (subscribe, unsubscribe, replica and part requests) were padded on LL. They now go in fitted symbols too.
- **Identity: keyed by the whole `StreamHandle`, not `(stream_id, generation)`.** A generation counts per index. With the free list, a stream recreated after `zero_stream_id` lands on another index with the same generation number; 5.2 hit this (both were 2). AUG's keys had the same ambiguity under round robin.
  - Keyed by handle: `sent`, `sent_initializations`, `SeenWatermarks`, the remote-slot set and `QueuedPart`.
  - A publication pass names each drained index with `handle_at`; the tick uses `stream_handles()`.
  - A stale handle stops the stream: for the rest of the pass, in the warm-up loop and in replica service.
  - `sent`/`sent_initializations` lose dead lifetimes and unknown peers at the 250 ms remote-slot prune (AUG), on ticks with and without children.
- **Hardening.**
  - `allowed_peers` is an `ArcSwap<HashSet<SocketAddr>>`. It is checked once per datagram, after the loss injection and before the receive observer, so cut-through never sees an unauthorized datagram. `peer_is_allowed` no longer reads it.
  - Hello, Subscribe and ReplicaRequest go through `admit_peer`. A Subscribe that the full peer table refuses is ignored; a ReplicaRequest is still served. `add_peer` authorizes only an admitted peer.
  - `max_peers`, `max_inflight_frames` and `replica_queue_capacity` are as AUG. There is no frame queue (C3).
- **Limits that follow the cache.** `max_frame_bytes` and `max_remote_slots` are `usize`, and 0 means "follow the cache"; `CacheMesh::start` fills them in.
  - `max_frame_bytes` = max(`buffer_size_kb`, `init_size_kb`) × 1024 + 64 KiB (`FRAME_OVERHEAD_BYTES`: any node id and the largest hello). The plan said "plus the frame header"; with a header-only margin, a 4 KiB-slot cache would refuse its own 256-peer hello.
  - `max_remote_slots` = the cache's slot capacity + `num_playlists` × 1,000, which is one 250 ms prune interval at 4,000 parts/s.
  - `FecSender` checks the limit in `send` and `prepare_object` (`tx_errors`, `InvalidFrame`). The handle's control sender uses it too.
  - A full remote-slot set is pruned under the held lock (`mark_remote_slot`). If it is still full, which happens only when the limit is set below the cache's capacity, the part is stored unmarked and counted in `remote_slot_evictions`. It may be echoed once. Nothing live is evicted.
- **FecReceiver.** The union of both field sets.
  - An oversized `transfer_length` is rejected first. `admit_decoder_peer` also drops the evicted peer's loss window. A decode error calls `expire_block`.
  - `make_observation_room` counts the dropped block's missing sources as loss in `LinkReport`. This was the provisional item; it is done.
  - Pruning runs on every datagram (LL) over AUG's ordered queues, so it costs only what expires.
  - **The auto-merged recovery loop is fixed.** It finalizes when the front is past the datagram cutoff or past `LATE_SOURCE_GRACE`, and pops stale entries. Observation expiry records loss as LL did.
- **Receive.**
  - Inline, as LL (C3).
  - `ReplicaRequest` goes to the bounded `replica_task`, on LL's reply lane (`REPLY_BLOCK_ID_BASE + offset`, `with_link_loss`, `count`).
  - Chunk apply follows the order in 2.4 and uses `add_remote_for_handle` (C7).
  - `MeshFrame::decode(Bytes)` is zero-copy (`split_to`); `chunk_frame_payload(&[u8])` copies.
  - Deviation: `decode_nanoseconds` is the wall-clock µs from `received_us` to the end of the FEC push, × 1,000. It reuses the clock read that is now `decoded_us`: one extra read per datagram, not AUG's two `Instant`s. It excludes the frame parse, and `decoded_us` is now taken before `receive_observer.decoded`.
- **Wire.**
  - v1 is gone from `decode` and `chunk_frame_slot`.
  - A hello must carry its peer count and role; every v2 encoder on both lines writes them.
  - A replica request is always 20 bytes.
- **Misc.**
  - `try_enqueue` uses `fetch_max` and `decrement_depth` a CAS loop, because AUG's `fetch_update` is deprecated in this toolchain.
  - The stats are the union minus `frame_queue_*`.
  - `CacheMeshRole` derives `Default`, and the "compatibility"/"original behavior" docs are reworded.
- **Tests: removed and renamed** (section 4, items 3–5 and 9).
  - Removed: `a_committed_part_waits_for_the_tick_without_sync_on_publish`, `a_committed_part_leaves_at_once_without_batched_send`, `unbatched_four_thousand_parts_a_second_for_comparison`, the `legacy_hello` case, the unbatched half of `a_batched_pass_makes_one_call_for_many_datagrams`, the `false` arm of `an_audio_part_goes_in_datagrams_of_its_own_size`, and the padded-sender assertion of `a_small_object_is_one_symbol_of_its_own_length`.
  - Renamed: `a_committed_part_leaves_at_once`; `a_config_with_a_send_observer_stays_printable`, which keeps the observer half of the sync-on-publish test; `generic_segmentation_offload_is_on_by_default`.
- **Tests: changed.**
  - `a_live_part_is_not_held_behind_a_warm_up_window`: the plan's recipe cannot put a live part on a tick, because a new lease starts live after the newest part. It now uses `max_children(1)` and two children. The parts published during the other child's turn go out on the edge's tick that opens its 40-part window. It asserts the window is contiguous, that tick carried held live parts, and no part published well before the window comes after it. Subscriptions are renewed while waiting.
  - C9: `with_allowed_peers` was added to `cache_mesh_gossips_peer_addresses_from_seed_node` (AUG's) and `the_send_observer_sees_live_parts_and_not_a_new_childs_catch_up`. `a_restarted_child_is_heard_by_its_parent_at_once` needs none: its parent lists the child.
  - C11: `with_gso_a_large_part_keeps_symbols_that_fit_a_packet` uses `buffer_size_kb: 256` for its 200 kB part.
  - Four lints that the current clippy raises in LL tests and the harness are fixed.
  - LL's `wait_for_hello` also waits for the child's own first hello to go out. A child that had just started could subscribe before its parent had its hello; with `same_region_only` the parent refuses that subscription, and LL's tests subscribe once. This was traced as the cause of the flaky `a_restarted_child_is_heard_by_its_parent_at_once` and `an_edge_does_not_remember_the_slots_that_it_receives` (the Subscribe arrived 1.4 ms before the hello). After the fix the full debug suite passed 20 of 20 runs. `a_committed_part_leaves_at_once` (10 ms debug bound) still fails about once in 15 suite runs under load, as LL's original did on the baseline (1 in 15).
- **Tests: added.**
  - AUG's tests, adapted: `replica_response_stops_when_its_stream_index_is_reused` (with `count`), the three `fec_receiver_*` bound tests, and `fec_receiver_expiry_queue_stays_within_the_datagram_window`, whose bound is now window + 1 because pruning runs per datagram. Also `bounded_replica_queue_drops_work_when_full`, `mesh_drops_unauthorized_datagrams_before_fec_decode` (which also checks the observer never sees the datagram) and `rejected_runtime_peer_does_not_expand_the_allowlist`.
  - New tests: `only_the_current_wire_format_is_accepted`, `a_decoded_payload_shares_the_frame_buffer`, `a_missing_source_counts_as_loss_on_the_first_datagram_after_its_grace`, `an_evicted_incomplete_block_counts_as_loss`, `a_hello_admits_only_authorized_peers_within_the_limit`, `the_frame_limit_follows_the_cache` (a 1.5 MiB part through 2 MiB slots), `a_frame_beyond_the_limit_is_not_sent` and `a_full_remote_slot_set_is_pruned_and_keeps_retained_slots`.
- **5.1** is `a_distributor_and_its_edges_keep_up_while_caches_churn_and_readers_block`. The harness gained `Load`; the no-load gate path is LL's, including the publisher's `add_for_stream_id`. Deviations:
  - It runs in demand replication. In full replication the churned streams reach the edges and evict stream 7 from their 8 lanes.
  - Churn runs at 1,000 streams/s.
  - "No echo" is checked as "no applied event at the distributor", because hellos are decoded objects.
  - The readers run on their own threads and poll every 200 µs.
  - Debug runs at 250 parts/s: a debug edge takes about 1,000 datagrams/s, the same on LL (its `--ignored` edges test in debug: about 5,200 of 8,000 parts missing on both lines).
  - Release, at load average ~5, 5 of 5 passed: p50 101–114 µs, p99 0.70–1.13 ms, blocking wake p99 43–47 µs, distributor 33–35 µs CPU per part (churn wakes it about 1,350 times a second).
  - At load average 11–14 it failed on p99 (3–6.6 ms), as LL's own edges gate did at that time.
- **5.2** is `a_restarted_publication_reaches_edges_whole_and_old_parts_never_reappear`, in both modes, as planned. The cursor check reads a `#[cfg(test)]` snapshot (`MeshShared::cursor_streams`) that is refreshed at each prune.
- **5.3** is `blocking_readers_wake_for_parts_that_arrive_over_the_mesh`. Deviations:
  - The mesh applies with `add`, not contiguous puts, so a part beyond a gap does advance the watermark. The test asserts that the set is marked exactly when the watermark advances: slot 4 marks it, then slot 3 does not.
  - The stale-handle step uses the edge's own `release_stream_id`, because a distributor restart does not restart the edge's stream (the C7 open item).
- **Gate measurements.** Release; distributor alone on one core; 2 × 4,000 parts/s of 72 B for 2 s. LL is `ff09196` in `/tmp/claude-1000/plmerge-baseline`. Runs were back to back and alternating: 28 rounds each, on a host shared with other sessions (load average 4–12). Figures are medians [range].

  | Test | Side | p50 µs | p99 µs | CPU µs/part |
  |---|---|---|---|---|
  | `sends_four_thousand_parts_a_second` | LL | 62.5 [54–71] | 189.5 [98–263] | 25.2 [23–34] |
  | | merged | 61.5 [54–73] | 179.5 [97–264] | 25.3 [22–36] |
  | `keeps_two_edges_at_four_thousand_parts_a_second` | LL | 79.5 [73–114] | 262.5 [151–1,466] | 25.7 [23–37] |
  | | merged | 78.0 [64–113] | 259.5 [159–1,371] | 25.6 [23–37] |

  In the last 4 rounds, on a quieter host, the p99s were LL 106.5/170.5 µs and merged 100.5/171.5 µs. Paired round by round over the first 24, merged was better in 9–14 rounds and worse in 8–13 on every metric, with no consistent direction. No regression was found, so no hot-path change was made. The allowlist check per datagram is an `ArcSwap` load with no lock.
- **Not done here.** The `max_inflight_frames` 64 check (4K video with 4,000 audio parts/s at 30,000 ppm) and the product bench (Opus, PCM, 4K) are owed by the product session.
- **`origin/low-latency` moved during this step.** At 23:59 on 21 Nov the product session pushed `fe8318c` and `89964dc` (the class split: video on its own socket and tasks, audio priority; +2,354 lines in `mesh.rs`). This merge is of `origin/main` into `ff09196` and does not contain them. They need a follow-up merge onto the merged mesh.

### Step 8: benches, and the compatibility sweep

- **Benches.** All five build (`cargo bench --all-features --no-run`). Every mode of every bench ran once for 0.25 s per step without a panic. The only failures they count are by design: the `reuse-race` and `churn` modes lose handles to concurrent reuse.
  - `distribution_capacity`: `part_target_us: 5_000`.
  - `mesh_pipeline`: `with_frame_queue_capacity` is gone. In its place the report has `rx_unauthorized_datagrams`, `rx_decode_errors`, `rx_expired_objects` and `peer_limit_drops` (all 0 in the runs). No `with_allowed_peers` was needed: the receive mode already lists its senders, and the replica mode's `with_peer` authorizes. Schema `mesh-pipeline.v2`.
  - `playlist_writes`: every mode writes and then reads. `manifest-render` does `push_part_us` + `m3u8()`. `cache-write[-replicated]` does `add_part_us_for_handle` + `last_for_handle`, which renders on read. The `playlists-*` modes do `Playlists::add` + `M3u8Cache::last`. Schema `playlist-writes.v2`.
  - `playlist_cache`: seeded with 40 `add_part_us` parts. A writer thread interleaves `add_part_us_for_handle` with the reads at `--parts-per-second` (default 400; 0 means no writer; there is no writer in `reuse-race`). Miss modes ask for a segment 10^9 ahead. `reuse-race` does `resolve_or_create_stream` + `add_part_us_for_handle` + `last_for_handle`. The report gains the writer's parts, failures and latency. Schema `playlist-cache.v2`.
  - `support/mod.rs`: `fetch_update` (deprecated by this toolchain) is replaced with `fetch_max`.
- **2.3 replicas: settled, keep.** `playlist_cache`, release, 2 s per step, 2 runs, load average 8. Delta reads by handle, in M reads/s:

  | Writer | Mode | 1 worker | 2 | 4 | 32 |
  |---|---|---|---|---|---|
  | 0/s | handle-delta | 15.6–16.1 | 12.7–22.1 | 14.0–14.3 | 18.9–21.2 |
  | 0/s | replicated | 16.3–16.4 | 30.4–30.8 | 57.4–60.7 | 281–301 |
  | 400/s | handle-delta | 13.1–14.1 | 11.5–20.7 | 12.0–24.7 | 14.9–17.8 |
  | 400/s | replicated | 12.9–13.9 | 26.6–27.8 | 49.7–49.8 | 156–221 |

  At 400 parts/s, the p99 at 32 workers fell from 3.0–4.2 µs to 0.29–0.32 µs. The cost is more allocations per rendering (about 100k against 26k in 2 s at 32 replicas). With one replica there are no copies.
- **4,000 versions/s.** At a writer of 4,000 parts/s, the delta modes collapse with or without replicas: about 3,700 reads/s, and p99 up to 4 s at 32 workers. Full reads still do 4–10 M/s.
  - The cause is the render, not the merge. One render of the 2.7 KB playlist costs about 130 µs, and a delta read costs about 270 µs (two gzips). On the LL baseline `ff09196` the same probe measures 128/263 µs.
  - Nearly all of it is the gzip. A bare `GzEncoder` (miniz_oxide) of 2.7 KB costs 120 µs at `Compression::fast()`, which is mostly compressor setup.
  - When versions change faster than one render, every reader renders, and the readers queue on `render_locks`. This is recorded as an open item, not fixed in this step.
- **2.2 head precompute: settled, keep.** Probe of `push_part_us` + `m3u8()` with the `playlist_writes` options, single thread, best of 7: merged 1,274 ns and 12.0 allocations per op, LL `ff09196` 1,373 ns and 17.0. The in-crate `manifest-render` mode: 10.0 allocations per op, 669k ops/s on one worker.
- **2.1 read fast path: confirmed.** The same probe, mapped `last(1)` and `last_delta(1)` with a 400/s writer, in M reads/s:
  - Merged against LL, full: 11.4 vs 19.2 at 1 reader, 4.1 vs 3.7 at 4, 4.2 vs 3.0 at 8, 5.0 vs 3.2 at 32.
  - Delta: 10.2 vs 14.1 at 1 reader, 3.9 vs 3.1 at 4, 4.6 vs 2.1 at 8, 4.9 vs 2.0 at 32.
  - The single uncontended mapped read is slower (the registry resolve, as step 5 found). Handle reads avoid it: `handle-full` does about 20 M/s on one worker.
- **Compatibility sweep** of `src`, `benches` (there is no `tests/` or `examples/`). It found no compatibility code left. The only hits were a test comment ("kept for the version") and the test `with_gso_a_large_part_keeps_symbols_that_fit_a_packet`, whose no-GSO arm uses the planned `#[cfg(test)] without_gso`. `send_object`, `zero_stream_id_sync`, `stream_idx`, `frame_queue*`, `FrameWork`, `new_playlist_tx`, the stored-playlist path and `add_part*` wrappers are gone. `CacheError::Superseded` is gone; `PutIfAbsentResult::Superseded` (AUG's older-id protection) stays. Doc hits are left for step 9.
- **Not changed:** `chunk_workloads` accepts `write`/`write-handle` and `mixed`/`mixed-handle` as names for the same modes, as AUG wrote it. These are short names, not old ones.

### Merge of 89964dc (class split)

`origin/low-latency` moved to `89964dc` after the merge above: `fe8318c` (the mesh split by class: video parts on their own socket and tasks) and `89964dc` (review fixes: lossless lane handover, the class lane on its own thread, audio priority). Both change only `src/mesh.rs` (+2,354 lines). It is merged with `git merge --no-ff origin/low-latency` onto `4a3896e`: 41 conflict hunks, all in `mesh.rs`. The merged mesh (handles, `MeshShared`, allowlist, bounded receiver, replica queue) is kept, and the class split is ported onto it by feature.

- **Shared state.** `MeshShared` gains `class: Option<ClassLaneShared>` (the class socket's address, counters, measured loss, served leases and dropped replica requests) and `handover: LaneHandover`. They replace LL's separate `class_counters`, `class_measured_loss`, `class_served`, `class_replicas_dropped` and `Arc<LaneHandover>`. `counters_of(lane)`, `measured_loss_of(lane)` and `served_of(lane)` give a lane its own; `Lane::is_class()` is `port_offset > 0`. `measured_loss()` is the worse of the two lanes, for `link_loss()` and the link reports.
- **Handles.** The lanes key what LL keyed by `(stream_id, index)` by `StreamHandle`, as the merged `sync_task` does:
  - `LaneChildren::{sent, sent_initializations}` by `(StreamHandle, SocketAddr)`;
  - the ownership table, `owns` and `waiting` by `StreamHandle`. `take_stream`, `put_stream`, `visit_stream` and `hand_over_stream` take a handle, and `forget_released_streams` takes `stream_handles()`, so a stream recreated in a new lifetime is a new playlist for the handover too;
  - pending lease windows and backfills stay keyed by `(peer, stream_id)`, as on both lines: leases are by stream ID. `forget_lease` is LL's lease-end cleanup as a method.
  - A playlist handed over to a lane is visited at once: its index joins the pass's drained indices, and `handle_at` names it.
- **Receive.** One `receive_task` serves both sockets, with its `lane`.
  - The allowlist check is `MeshShared::authorized_sender(lane, from)`: the peer is `lane.peer_of(from)` (the address itself on the main socket), and it must be in `allowed_peers`. The allowlist holds main addresses only. So every way of authorizing a peer (`with_peer`, `with_peers`, `with_allowed_peers`, `add_peer`, and a gossiped peer, which joins only if it is allowed) authorizes its class socket, on the class socket only. A peer's class address is not authorized on the main socket, nor its main address on the class socket. Storing the class addresses as entries of their own was the alternative; deriving them keeps one set, with no moment at which a peer is authorized on one socket and not the other.
  - The check runs where it runs on the main socket: after the loss injection, before the observer and FEC decode. A refused class datagram counts in the class lane's `rx_unauthorized_datagrams`. A source port below the offset is nobody's class socket and is refused and counted; LL skipped it uncounted.
  - The class lane's receiver is `FecReceiver::with_limits` (`max_peers`, `max_inflight_frames`, `max_frame_bytes`), so an oversized transfer is refused there before a decoder is made, as on the main socket. LL's class receiver had no bounds.
  - After decode the class socket ignores control frames (LL). Chunks go through the one `handle_frame`: mark-before-write in the one remote-slot set that both lanes share, under the lock held across `add_remote_for_handle`. `FrameHandler::counters` puts `remote_slot_evictions` on the lane's counters.
- **Replica requests.** The main receive task admits the requester (merged), then hands a request for a video playlist to the class lane (`ClassReplicas::offer`), and any other request to the bounded `replica_task`.
  - The class queue is `replica_queue_capacity` long; LL's constant `CLASS_REPLICA_QUEUE` (64, the same as the default) is gone, so there is one setting for both queues.
  - `class_replica_task` coalesces (LL), and also counts the requests served, their errors and time on the class lane's counters. A dropped request counts in `class_replica_requests_dropped` and in the class lane's `replica_queue_drops` (`ClassLaneShared::snapshot`).
  - `serve_replica_request` takes `dest`, and keeps the merged rule that only a window (`count == 0`) sends the initialization.
- **Send.** One `sync_task` per lane, with its `lane`: the merged body with LL's lane logic. A lease window is opened only by the lane that carries the playlist; a lease's end calls `forget_lease` and `forget_child_in_transit`; each lane lists only its own served leases; the class lane skips children without a class port; `visit_stream` runs before the watermark check; the select waits for a handover; and a lane without children hands over on its tick what it no longer carries.
  - `flush_child`, the initialization send and `serve_replica_request` send to `dest` (`lane.to(peer)`). The send observer, the loss lookup and the cursors use the main address.
  - The 250 ms prune is `prune_lane`. Only the main lane prunes the remote slots and the ownership table, which both lanes share. Each lane prunes its own cursors (`forget_dead_cursors`, the merged prune). Only the main lane records the test-only cursor snapshot.
- **Start.** `max_frame_bytes` and `max_remote_slots` are filled in before either lane's configuration is cloned. The class socket is bound in the class runtime's context and its three tasks are spawned there; the own thread runs that runtime until shutdown (LL).
- **Stats.** `CacheMeshFecStats::plus` adds the merged counters too; `replica_queue_max_depth` is the larger of the two lanes', since each lane has its own queue.
- **Removed.** `ClassLaneRuntime::Caller`: it is `fe8318c`'s behaviour, which `89964dc` found holds audio back on av-mesh's one-thread media runtime, and `ClassLaneRuntime::Handle(tokio::runtime::Handle::current())` does the same. From the LL side of the conflicts: the `batched_send`, `gso` and `fitted_symbols` fields, `send_object` (LL gave it a `dest`; the merged mesh has no `send_object`), and `sync_on_publish` in the select. No opt-out or compatibility code is added.
- **Tests: kept.** All 16 of `fe8318c`'s and `89964dc`'s tests are kept, with their assertions. Changes:
  - C9: `with_allowed_peers([edge_addr])` on the distributors that learn their edge from its hello: `audio_part_during_a_video_burst`, `audio_parts_while_an_edge_decodes_a_video_window` and `the_class_lane_has_its_own_applied_channel_and_no_observer_by_default`.
  - `a_playlist_changes_lane_only_when_the_lane_that_it_leaves_hands_it_over` keys by handles that a cache gives (`resolve_or_create_stream`), not `(stream_id, index)` tuples.
  - `a_video_replica_request_is_served_from_the_class_socket`: `resolve_stream(9).is_none()` for `get_stream_idx(9)`.
  - `audio_parts_while_an_edge_decodes_a_video_window` starts the video distributor only after audio part 0 has reached the edge, and waits for 20 more decoded objects from there. Its comment said the audio distributor knows the edge first, but nothing made it so: both distributors learned the edge from the same hello, and when the 200-part window reached the edge first (debug, full suite: 4 runs in 10), part 0 waited behind all of it (0.7–1.1 s), the window was over when the audio parts were published, and on one socket they did not wait. The test's own bound then failed (`one_max >= 10 ms`). LL's runs did not hit this (0 in 10); the order of the two windows is a race on both lines.
  - The burst and receive helpers print their timings, for the comparison below.
- **Tests: merged line.** `a_batched_pass_makes_one_call_for_many_datagrams` failed about once in 25 suite runs with 216 of the 240 datagrams counted: a part is counted when its child's `sendmmsg` call has returned, which can be after the raw child has it. It now waits (at most 1 s) for the distributor's counts to reach 240 before it reads them; the assertions are unchanged.
- **Tests: added.**
  - `a_class_socket_drops_unauthorized_datagrams_before_decode`: a chunk on the class socket from an unknown address and from an authorized peer's main address, and on the main socket from the peer's class address, is dropped and counted (4 class, 2 main), with no wire datagram, decoder, observer call or write. From the peer's class socket, an oversized frame is refused (`rx_oversized_objects`) and a part is applied, the observer naming the peer's main address.
  - `every_way_of_authorizing_a_peer_authorizes_its_class_socket`: `with_peer`, `with_peers`, `with_allowed_peers`, `add_peer` and a gossiped allowed peer; each address on its own socket only; a gossiped stranger and a port below the offset are refused.
  - `a_class_split_mesh_between_authorized_peers_passes_video_and_audio`: a distributor that knows its edge only from `with_allowed_peers` learns it from its hello; video and the initialization go on the class socket and audio on the main one, with no refused or oversized datagram on either node.
  - A mutation that lets any class datagram through (`authorized_sender` skipping the allowlist on the class lane) fails the first two.
- **Gates.** `nice cargo build --all-features`; `nice cargo test --all-features` three times (211 passed, 2 ignored, each time); `nice cargo test --release --all-features mesh::` (85 passed); `cargo clippy --all-features --all-targets -- -D warnings`; `cargo fmt --check`. Before the gate runs, 10 alternating debug suite runs of `origin/low-latency` and the merge had no failure besides the two described above. One release `mesh::` run at load average 8.7 failed `a_distributor_and_its_edges_keep_up_while_caches_churn_and_readers_block` on p99, as recorded for 5.1 above; the next three passed.
- **Performance.** Release, `origin/low-latency` (`89964dc`, in `/tmp/claude-1000/plmerge-baseline`) against the merge, each test in its own process, alternating, on a host shared with other sessions (load average 5–9). Two batches of 14 rounds of every test, and a third batch of 30 rounds of the edges test. Medians [range]:

  | Test | Side | p50 µs | p99 µs | CPU µs/part |
  |---|---|---|---|---|
  | `sends_four_thousand_parts_a_second` (28) | LL | 60.0 [55–77] | 100.0 [82–185] | 25.4 [23–37] |
  | | merged | 61.0 [54–74] | 100.5 [89–152] | 24.9 [23–38] |
  | `keeps_two_edges_at_four_thousand_parts_a_second` (58) | LL | 79.0 [60–117] | 181.5 [147–1,379] | 26.1 [22–34] |
  | | merged | 79.0 [66–112] | 189.0 [145–2,079] | 26.4 [23–34] |

  The edges p99 was worse in 10 of 14 and 9 of 14 paired rounds of the first two batches, and even (15 and 15) in the 30-round third batch; over all 58 rounds, 34 worse and 24 better, +7.5 µs at the median. Every other paired count is within 13–16 of 28. One merged edges round failed its 2 ms bound (p99 2,079 µs, max 5 ms), as both lines do at times under this load. No regression is found beyond noise, so no hot-path change is made: on a mesh that is not split, the merge adds a port subtraction of 0 to the allowlist check and a `None` test in the sync task.

  The class-split tests (28 rounds each; the latency of the audio part, or of the 10 audio parts, in µs):

  | Test | LL | merged |
  |---|---|---|
  | burst, split, 4 workers | 114.5 [79–191] | 110.0 [82–180] |
  | burst, one socket, 4 workers | 48,273 [45,497–69,832] | 47,110 [45,106–70,327] |
  | burst, split, current thread | 47.0 [37–85] | 49.5 [40–85] |
  | burst, split, 2 workers | 111.0 [80–196] | 108.0 [80–209] |
  | edge receive, split, current thread: p50 / max | 112 / 146.5 | 115 / 154 |
  | edge receive, one socket, current thread: max | 34,731 | 34,853 |
  | edge receive, split, 2 workers: p50 / max | 125 / 166.5 | 122.5 / 160 |
  | edge receive, one socket, 2 workers: max | 24,690 | 23,924 |

  The first datagram to the commit of a split audio part was 9–40 µs on both lines.
- **For the product session.**
  - `ClassLaneRuntime::Caller` is gone: use `ClassLaneRuntime::Handle(tokio::runtime::Handle::current())` if av-mesh chose it.
  - C9 holds for the class socket: a class datagram is taken only from an authorized peer's main port plus the offset. Every node needs the same offset, and a NAT or proxy that changes the source port breaks the split.
  - The class lane's receiver is now bounded by `max_inflight_frames` (64 by default). This adds to the owed 4K check in step 7.

### Review fixes

A review of `7e27ac3` raised nine claims, each checked by two verifiers. Each was reread against the code before it was fixed. Every fix has a regression test, and each test was checked to fail with its fix reverted (in a scratch copy).

- **A refused part id evicted a live stream.** The merge refuses id `usize::MAX` (the empty marker) in `slot_index`, which runs after `add_for_stream_id`, `put_if_absent_for_stream_id` and `put_if_absent_contiguous_for_stream_id` have created the stream. On a full cache the refused write evicted a live stream, against AUG's rule that a refused write never does. The mesh's chunk apply had the same order. Fixed: `ChunkCache::validate_write(id, &payload)` (crate-private) refuses the id next to the payload limit, before the stream is created, in the three entry points and in the mesh's chunk apply. Tests: `rejected_mapped_payloads_do_not_evict_a_live_stream` gains the three `usize::MAX` cases; `a_refused_part_id_does_not_evict_a_live_stream` sends a CHUNK with slot id `u64::MAX` to a full edge.
- **A second `fin` of an ended entry cleared a newer stream.** Two `fin`s of one entry both clone it; the second waits on its commit lock while the first clears the caches and removes the entry, an add makes a new entry and stream, and the second then cleared that stream and left the new entry without one. Not new in the merge: AUG's `fin` does the same. Fixed: after taking the commit lock, `fin` checks that the map still holds its entry, and returns without clearing if not. While the map holds it, the entry stays: an add sees it closing, and only a holder of its commit lock removes it. Test: `a_second_fin_of_an_ended_entry_leaves_a_newer_stream_alone` (the test plays the first `fin` under the commit lock that the real second `fin` waits for).
- **One FEC decoder per peer.** A peer numbers announcements, requests, replica replies and parts from separate bases, but the receiver had one `DatagramFecDecoder` per peer. A decoder that completes a block drops every incomplete block more than 64 ids below it (`COMPLETED_WINDOW`), so each completed part (ids from 2^31) dropped the symbols of a replica reply (from 1.5 x 2^30) or request that was still arriving. Pre-existing on both lines. Fixed: `PeerDecoders` holds one decoder per range (`block_id_lane`); admission and eviction stay per peer, and `max_peers` still bounds the peers. Test: `a_completed_part_keeps_a_reply_of_the_same_peer_that_is_arriving`.
- **`send_control` reused block ids.** It reserved one request block id but the sender numbers one block per peer, so the next call gave its first peer an id that peer had already decoded, and the frame was dropped as a duplicate (an unsubscribe to two parents, then a subscribe whose first parent is the second one). Pre-existing on both lines. Fixed: it reserves one id per peer. Test: `control_sends_to_several_peers_never_reuse_a_block_id`.
- **A lagging parent's oldest part can replace the edge's newest (C7).** Real, and the same on LL: a parent one part behind sends a window or catch-up from its own retained start, whose first part lands in the ring slot of the edge's newest part. The part comes back when the lagging parent forwards it. Not fixed: any local check that refuses it also refuses a restarted origin's parts, which C7 keeps. It is added to the origin epoch item in `TODO.md`.
- **Docs.** README: `update_notifier` and `reset_stream_idx` are listed apart from the raw-index methods that reject a logical stream's index (they do not check the owner); the `Peer` role is described as it behaves (it sends its own parts and does not relay received ones), in the README and on `CacheMeshRole::Peer`. CHANGELOG: content steering, `with_max_symbol_size`, `with_socket_buffer_bytes`, `parents_for`, `parents_silent_for`, the per-lane decoders and the id refusal. TODO: the class split merge is marked done, the product bench baseline is `89964dc`, and the class split's deployment requirements are in the integration checks.
- **Performance.** Only the decoder lanes touch a hot path: three compares and an array index per received datagram. Release, each test in its own process, on a host shared with other sessions. `89964dc` (in `/tmp/claude-1000/plmerge-baseline`), `7e27ac3` and the fix, alternating, 10 rounds of both gates twice; then 14 rounds of the edges gate with `7e27ac3` and the fix alternating which runs first. Medians [range], the second batch (load average 5–8):

  | Test | Side | p50 µs | p99 µs | CPU µs/part |
  |---|---|---|---|---|
  | `sends_four_thousand_parts_a_second` | LL | 60.5 [58–62] | 133.0 [85–214] | 25.0 [23.8–26.7] |
  | | `7e27ac3` | 58.5 [55–67] | 123.5 [87–229] | 25.1 [23.5–29.1] |
  | | fix | 60.0 [53–67] | 132.5 [85–227] | 24.9 [23.0–32.4] |
  | `keeps_two_edges_at_four_thousand_parts_a_second` | LL | 76.0 [69–85] | 207.0 [159–275] | 26.1 [23.1–28.7] |
  | | `7e27ac3` | 75.0 [64–79] | 188.5 [153–999] | 25.4 [23.1–26.6] |
  | | fix | 76.5 [69–86] | 227.0 [165–428] | 25.6 [24.1–31.6] |

  The edges p99 was worse than `7e27ac3` in 9 of 10 paired rounds of that batch, where the fix always ran right after `7e27ac3`. In the third batch, which swapped the order each round (load average 7–9), it was worse in 7 of 14 and better in 7: p50 84 against 86.5 µs, p99 377.5 against 563.5 µs, with 1 failed 2 ms bound against 3. In the first batch (load average 3–23) the fix failed the edges bound twice, at load average 20 and 23, where `7e27ac3` reached 1.3 ms. No regression is found beyond noise.
- **Gates.** `nice cargo test --all-features` three times (215 passed, 2 ignored, each time); `nice cargo test --release --all-features mesh::` (88 passed); `cargo clippy --all-features --all-targets -- -D warnings`; `cargo fmt --check`. One earlier debug run at load average 16 failed three timing bounds (`a_committed_part_leaves_at_once`, the two-worker audio-during-video test and the 4,000/s sender's debug bound); 6 alternating debug runs of `7e27ac3` and the fix after it had no failure on either side.
- **Public API.** No signature changes. Behaviour: a write with id `usize::MAX` by stream ID fails as before (`"Chunk id out of range"`) but no longer creates the stream; a `fin` on an entry another `fin` has ended clears nothing.
