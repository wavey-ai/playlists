# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

The August line (`origin/main`, August 2026: stream identity, bounds and mesh
hardening) merged into the low-latency line (November 2026: render on read,
microsecond time and the 4,000 parts/s mesh). There is one way to do each
thing; the switches and aliases of both lines are removed.

### From the low-latency line

- `Options` holds PART-TARGET in microseconds (`part_target_us`, default 2,500). New fields `part_hold_back_ms` (a floor for PART-HOLD-BACK), `window_ms` (default 12 s) and `parted_target_durations` (default 3). Defaults are 1 s segments and TARGETDURATION 1; `max_segments` and `max_parts_per_segment` are derived from time when they are zero. `Options::normalized` is public and idempotent.
- Playlist durations are rendered exactly from microseconds as decimal seconds (up to six decimals, at least three), and the delta-update parser reads them exactly.
- The target duration is at least `segment_min_ms`, and a segment closes before its EXTINF would round above TARGETDURATION, at a part that is not independent if need be. EXT-X-PART lines are kept only for segments within `parted_target_durations` target durations of the end, and at most `max_parted_segments`.
- `M3u8Cache` keeps one live `M3u8Manifest` per stream. A part is added in constant time (`add_part_us`, `Playlists::add_us`, `M3u8Manifest::push_part_us`), and the playlist is rendered and compressed when it is read, once per new part. `M3u8Cache::get` returns the newest playlist once it contains the part. New: `M3u8Cache::last_text`, `M3u8Manifest::opens_segment(duration_us, key)`, `pieces`, `m3u8_manifest::{format_seconds_us, parse_seconds_us}`.
- `ChunkCache` records a lock-free write time per slot (`written_unix_us_for_stream_id`), signals `publication_notify` and `subscribe_publications` sets only when a watermark advances, takes a free index before it replaces a stream, and frees single slots and whole streams (`clear_for_stream_id`, `release_stream_id`). Exact-part waiters are keyed by stream ID and part ID and create no mapping, so a reader can wait for a stream's first part.
- Mesh send path: a part leaves when the watermark reaches it, and a publication pass visits only the streams that moved. `ObjectEncoder` encodes once for all children, keeps the RaptorQ plan of each symbol count, repairs a one-symbol block by byte table, and caches encodings by stream, slot and payload hash. Objects smaller than `symbol_size` (at most 1,200 bytes) go in one symbol of their own length. Datagrams leave in `sendmmsg` batches, with UDP GSO where the kernel has it. Live parts go before warm-up windows. `CacheMeshFecStats::tx_send_calls` counts kernel send calls.
- Mesh receive and topology: a receive observer for edge UDP cut-through, loss-driven repair (`LinkReport`, late-source grace), demand replication with subscription leases, `request_parts`, and block-id lanes per process start. A receiver decodes each peer's lanes (announcements, requests, replica replies, parts) separately, so a completed part does not drop a reply that is still arriving. Parents are ranked per playlist by rendezvous hash (`CacheMeshHandle::parents_for`), and `parents_silent_for` tells an edge how long it has heard no parent. A peer is heard on the receipt of any datagram on either socket, before FEC decode, so a parent whose blocks are slow to complete is not taken for lost; a node never hears itself, at its bind address or at an address from which its own frames arrive.
- `CacheMeshHandle::served_changed`: a watch whose count goes up when the leases that either lane serves change (a new lease, an Unsubscribe or an expiry), so that a forwarder need not poll `served_subscriptions`.
- Mesh sizes: `CacheMeshConfig::with_max_symbol_size` (a large object uses symbols of up to this size, a multiple of `symbol_size`) and `with_socket_buffer_bytes` (the sockets' send and receive buffers).
- Content Steering in multivariant playlists: `ContentSteering`, `MultivariantPlaylist::with_content_steering`, `VariantStream::with_pathway_id` and `is_valid_pathway_id`.
- Mesh split by class (`with_class_port_offset`, `with_stream_classifier`, `StreamClass`): a second socket at the main port plus the offset carries the video playlists' parts and initializations, with its own receive task, sync task and replica server, on a thread of its own with a current-thread runtime by default (`ClassLaneRuntime::{OwnThread, Handle}`, `with_class_runtime`). Hellos, leases, replica requests and loss reports stay on the main socket. The main socket of a split mesh has `SO_PRIORITY` 6 (`AUDIO_SOCKET_PRIORITY`, `with_main_socket_priority`; `with_class_socket_priority`). A playlist that changes class is handed over losslessly, with every child's cursor, initialization, warm-up window and unstarted lease. Replica requests for video playlists are queued for the class lane and coalesced. `with_class_receive_observer` (none by default) and `with_class_applied_tx`; `class_fec_stats`, `class_local_addr` and `class_replica_requests_dropped` on the handle. `fec_stats` is the sum of both lanes, and the measured loss reported to a peer is the worse of the two.

### From the August line

- Stream identity: a crate-private registry gives every logical stream an index and a generation. `StreamHandle` and `M3u8StreamHandle`, `resolve_stream`, `resolve_or_create_stream`, `stream_handles` and the `*_for_handle` methods are the identities for repeated work. A stale or foreign handle cannot read or write a reused index. Raw-index methods work only on lanes that the registry never assigns.
- `ChunkCache` keeps a slot's ID, generation, hash and payload under one lock. An origin-local write of an older ID over a newer one of the same generation is refused (`"Chunk id already evicted"`, `PutIfAbsentResult::Superseded`).
- Initialization data is tagged with the stream and generation. A new or reused stream has none, and `M3u8Cache::get_init` returns `CacheError::StreamNotFound`.
- Writes validate sizes before they assign a stream: chunks against `buffer_size_kb`, initializations against `init_size_kb` (default 5), so a rejected write never evicts a live stream. A part ID of `usize::MAX` (the empty marker) is refused (`"Chunk id out of range"`), also before the stream is assigned, and so is a mesh part with that ID.
- Per-lane notifiers: `ChunkCache::update_notifier` and `M3u8Cache::update_notifier_for_handle`.
- Generation-tagged segment ranges (`get_idxs_for_handle`) and one consistent playlist position (`position_for_handle`).
- `Playlists` gives each stream an entry with its own commit lock, and `fin` cannot let a racing add resurrect a stream.
- Manifest: a direct PROGRAM-DATE-TIME writer, an invariant head built once, and caps on rendition reports (256 entries, 2,048-byte URIs).
- Opt-in playlist read replicas (`new_with_latest_read_replicas`, 1 to 64).
- Mesh hardening: a peer allowlist checked before FEC decode (`with_allowed_peers`; `with_peer`, `with_peers` and `add_peer` authorize), `max_peers`, `max_inflight_frames`, bounded decoder and observation state with ordered expiry queues, a bounded replica-request queue (`replica_queue_capacity`), zero-copy frame decode, and the matching `CacheMeshFecStats` counters.
- `try_new` constructors with checked capacity arithmetic, `memory_stats` on both caches, and the bench binaries `distribution_capacity`, `chunk_workloads`, `playlist_cache`, `playlist_writes` and `mesh_pipeline`.
- Lockfile security updates: `bytes` 1.11.1, `rustls-webpki` 0.103.13, `aws-lc-sys` 0.44.0.

### Merged design

- The registry takes a free index first, in O(1), and `ChunkCache::handle_at` names the stream at an index drained from `PublishedStreams`. `release_stream_id` frees exactly the released lifetime's slots.
- `ChunkCache` stream state is atomic, under the registry's owner read lock, with no other lock per part. The slot hash is the runtime `xxh3_64` (the same values as before), which makes large writes much cheaper.
- An `M3u8Cache` read of a playlist that is already rendered takes no lock (`ArcSwap`). Readers that render take the stream's render lock, so one renders and the others take its result. The delta update is made once per version. Read replicas are copied when a reader renders, not when a part is added. Versions continue across reuse and are never 0.
- `M3u8Cache::add_part_us_for_handle` and `last_text_for_handle`; `zero_stream_id` is one synchronous method on both caches.
- The mesh keys its send state, watermarks and remote slots by `StreamHandle`. A stale handle stops the stream in a pass, in warm-up and in replica service.
- `max_frame_bytes` and `max_remote_slots` follow the cache by default: the larger of `buffer_size_kb` and `init_size_kb` plus 64 KiB, and every retained slot plus one prune interval. A full remote-slot set is pruned to the retained window and never evicts a live slot.
- Received parts are marked remote before the write, under a lock held across it. Mesh-applied writes replace an older ID of the same generation, because the wire carries no generation and a restarted origin's parts must land.
- Replica requests are served from the bounded replica queue on the reply lane, so a large range does not delay live receive.
- A distributor serves only authorized children: list them with `with_peer`, `with_peers` or `with_allowed_peers`, or add them with `add_peer`.
- Receipt liveness and the allowlist are one rule: the stamp that a peer was heard is written after the allowlist check on either lane, so an unauthorized datagram is dropped before it can stamp anything, and a class datagram stamps the peer under its main address. Receipt liveness is the only behaviour; there is no switch for stamping on decode only.
- A walk over many slots of one stream reads write times with `written_unix_us_for_handle`: the stream is resolved once, and each slot costs one lock-free registry check and its tag, with no map lookup. There is no raw-index `written_unix_us`, as with every raw-index read of a registry-owned lane.
- The class split uses the merged mesh throughout: the class lane's cursors, initializations and lane ownership are keyed by `StreamHandle`, and it applies parts with `add_remote_for_handle`, marking them remote before the write in the remote-slot set that both lanes share. The allowlist covers the class socket: a datagram there is taken as the peer's at its port minus the offset, and is dropped before the class observer and FEC decode unless that peer is authorized. Authorizing a peer in any way (`with_peer`, `with_peers`, `with_allowed_peers`, `add_peer`) authorizes its class socket, on the class socket only. The class lane's FEC receiver has the same bounds (`max_peers`, `max_inflight_frames`, `max_frame_bytes`) and its replica queue is `replica_queue_capacity` long; its drops also count in the class lane's `replica_queue_drops`.

### Removed

- `Options::part_target_ms` and `Options::eager_render`.
- `CacheMeshConfig::{batched_send, sync_on_publish}` with `with_batched_send` and `with_sync_on_publish`, the unbatched and tick-only send paths, and `FecSender::send_object`.
- The `with_fitted_symbols` and `with_gso` builders. Fitted symbols are always on; GSO falls back automatically when the kernel lacks it.
- The stored-playlist path: `M3u8Cache::add`, `add_for_handle`, `add_prepared_for_handle` and `end_segment`, the write-time snapshot ring, and the render-returning `M3u8Manifest::{add_part, add_part_with_byte_len, add_part_with_byte_range, add_part_us}`.
- The numeric stream-index methods `add_stream_id`, `get_or_create_stream_idx`, `get_stream_idx`, `stream_idx` and `stream_ids`, `take_new_playlists_rx`, and `zero_stream_id_sync`.
- `CacheError::Superseded`.
- Mesh wire version 1 frames and 16-byte replica requests: every node must run this version.
- `ClassLaneRuntime::Caller` of the low-latency line's class split. `ClassLaneRuntime::Handle(tokio::runtime::Handle::current())` does the same.
- The low-latency line's `CacheMeshConfig::{liveness_on_receipt, with_liveness_on_receipt}` and `ChunkCache::written_unix_us(stream_idx, id)`.

### Given up

- Per-part playlist snapshots: `M3u8Cache::get` answers with the newest playlist that contains the part. Also given up with the snapshot ring: the delta cached at write time, a lock-free hit on the first read after a part (that read now renders), and the per-snapshot byte budget; `buffer_size_kb` no longer bounds playlists.
- The August bounded frame queue between decode and apply. Chunk frames are applied inline on the receive task.
- The manifest's forced segment close at `max_parts_per_segment`. The cache refuses the part instead.
- Generation-keyed exact-part waiters. A waiter can be woken by a write of a newer generation; the woken reader rechecks and never reads stale data.
- The older-ID refusal for mesh-applied writes. A relay or edge does not follow an origin that restarts a stream until the new IDs pass the old watermark.
- A single uncontended mapped playlist hit costs about 30 ns more than on the low-latency line, for the registry lookup. Handle reads avoid it.

## 2026-06-02 (unpublished)

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

## [0.1.1] - Previous

- Initial release with `Fmp4Cache`, `M3u8Cache`, and `M3u8Manifest`
