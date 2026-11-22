# Playlists and Chunk Cache

[![CI](https://github.com/wavey-ai/playlists/actions/workflows/ci.yml/badge.svg)](https://github.com/wavey-ai/playlists/actions/workflows/ci.yml)

## Overview

This crate provides fixed-capacity caches and manifest tools for Low-Latency HLS
(LL-HLS) and other rolling byte streams. It is built for the smallest parts:
a 2.5 ms Opus part, a 1 ms PCM part, and 4,000 parts a second on one stream.

- `ChunkCache` stores `bytes::Bytes` media objects in a fixed ring.
- `M3u8Cache` keeps one live playlist per stream. It renders and compresses the
  playlist when a reader asks for it, once for each new part, and stores the
  stream's initialization section.
- `M3u8Manifest` is a bounded LL-HLS media playlist. A part is added in
  constant time.
- `Playlists` ties the two caches together and tracks active live streams.
- The optional `CacheMesh` feature (`mesh`) replicates chunk data over UDP with
  RaptorQ forward error correction (FEC).

`ChunkCache` uses Tokio locks for media slots. Playlist rendering, gzip work,
and `M3u8Cache` operations are synchronous CPU work. The crate does not perform
file I/O.

## Stream identity

A logical stream ID is not a physical cache index. Physical indices are reused
when the configured stream capacity is full. Each assignment therefore has a
generation.

Resolve a stream once at a request or batch boundary. Reuse the returned opaque
handle for repeated work.

```rust
use bytes::Bytes;
use playlists::{chunk_cache::ChunkCache, Options};

# async fn example() -> Result<(), &'static str> {
let cache = ChunkCache::new(Options::default());
let stream = cache.resolve_or_create_stream(42).await;

cache
    .add_for_handle(stream, 7, Bytes::from_static(b"part-7"))
    .await?;
let part = cache.get_for_handle(stream, 7).await;
assert_eq!(part.unwrap().0, Bytes::from_static(b"part-7"));
# Ok(())
# }
```

The handle contains a cache identity, stream ID, physical index, and generation.
A stale handle cannot read or write a reused index. A handle from another cache
is also rejected. `ChunkCache` uses `StreamHandle` and `M3u8Cache` uses
`M3u8StreamHandle`.

Mapped methods such as `get_for_stream_id` are suitable for one-shot calls.
They resolve the logical ID on each call. Handle methods avoid the global map on
the repeated read path.

A new stream takes a free index first. A live stream is replaced only when
every index is in use. `ChunkCache::release_stream_id` and `zero_stream_id`
end a stream and free its index. `ChunkCache::stream_handles` lists the live
streams, and `ChunkCache::handle_at` names the stream at an index, for example
an index drained from `PublishedStreams`.

The raw-index methods (`set`, `append`, `add`, `get`, `get_last`, `last`,
`version`) are for fixed lanes: code that owns a physical index which the
stream registry never assigns. They reject indices that belong to a logical
stream. A fixed lane must not use `resolve_stream`, `resolve_or_create_stream`,
or another registry method. Two index methods do not check the owner:

- `update_notifier(index)` returns the notifier of any index, a logical
  stream's (`handle.index()`) included.
- `reset_stream_idx(index)` ends whatever stream holds the index, a logical
  stream included, and frees the index. A fixed lane calls it only on its own
  index, before the lane starts a new logical lifetime.

## Time and sizes

`Options` holds PART-TARGET in microseconds: `part_target_us`, default 2,500
(one 2.5 ms Opus frame). Durations are rendered exactly as decimal seconds with
up to six decimals (`0.0025`, `0.001`, `0.00025`, `0.041708`), so the parts of
a segment sum to its EXTINF.

- Only segments that start within `parted_target_durations` (default 3) target
  durations of the end carry EXT-X-PART lines, and at most
  `max_parted_segments` of them when that is set.
- PART-HOLD-BACK is three part targets or `part_hold_back_ms`, whichever is
  larger.
- TARGETDURATION is at least `segment_min_ms`. A segment closes on the first
  independent part at or after `segment_min_ms`, and before its EXTINF would
  round above TARGETDURATION, at a part that is not independent if need be.
  Set `target_duration_ms` to the GOP to keep every segment starting on a key
  frame.
- `max_segments` and `max_parts_per_segment` default to 0, which derives them
  from `window_ms` (default 12 s), `segment_min_ms` (default 1 s) and the part
  target. A non-zero count is kept as given. `Options::normalized` fills in
  the derived values; it is idempotent.
- A part that would not open a new segment, when the open segment already
  holds `max_parts_per_segment` parts, is refused with
  `CacheError::IndexOutOfBounds` and adds nothing.

`Playlists::add` takes the part duration in milliseconds from `Fmp4.duration`.
`Playlists::add_us` and `M3u8Cache::add_part_us` take microseconds.

## Concurrency design

`ChunkCache` stores the slot ID, generation, hash, and payload under one slot
lock. A reader either sees the complete old slot or the complete new slot.
Each stream's last ID, next ID, and content version are atomics. Writers change
them only under the registry's owner read lock for the index, and a reset runs
under its owner write lock, so no value crosses a generation.

Each slot also has a lock-free tag: its ID, generation, and write time.
`written_unix_us_for_handle` and `written_unix_us_for_stream_id` read the write
time without a lock or an await. The time is stored before any reader that the
write wakes can run.

`ChunkCache` has three notification sources. None of them stores a permit:
enable a notification, recheck the cache, and then await it.

- `publication_notify` with `subscribe_publications` fires only when a
  stream's watermark advances. The returned `PublishedStreams` names the
  stream indices that advanced. The mesh send path uses it.
- `exact_part_waiter` wakes for one stream ID and part ID. It creates no
  mapping, so a reader can wait for the first part of a stream that does not
  exist yet. The first write of that identity in any generation wakes it.
  Recheck with a generation-safe read.
- `update_notifier` is one fixed source per lane. It wakes for every write,
  initialization change, and reset.

`M3u8Cache` keeps one live `M3u8Manifest` per stream under the stream's slot
lock. `add_part_us` appends the part, publishes the new position, version, and
segment range together, and wakes `update_notifier_for_handle`. It does not
render or compress anything.

A reader renders. The first reader of a new version takes the stream's render
lock, assembles the playlist from shared pieces outside the slot lock,
compresses it once at the fastest gzip level, and publishes the result through
an `ArcSwap`. Readers that arrive during the render wait for it and take the
same result. Later reads of that version take no lock. The delta update
(EXT-X-SKIP) is made once per version by the first reader that asks for it. A
render does not change the version and does not wake a notifier.

`get(seg, part)` returns the newest playlist once it contains that part, as an
RFC 8216bis blocking reload allows. It does not keep a playlist for each part.

Versions continue across reuse of a slot and are never 0, so an ETag-style
version never repeats for a new stream.

Each manifest builds its invariant LL-HLS head once. A closed segment is
rendered to text once, when it closes. A render copies the retained text into
one exact-length buffer.

Playlist reads share one compressed payload by default. Read-heavy deployments
can use `M3u8Cache::new_with_latest_read_replicas` or the matching `Playlists`
constructor. Each runtime thread selects one replica. The copies are made once
per rendering, by the reader that renders, so a part add never copies. Use
replicas only after measuring the target read fan-out.

Stream reassignment closes a per-index reuse gate, clears published state, and
advances the generation. Read paths validate the published generation before
and after they clone `Bytes`. Write paths keep the reuse gate until slot and
position metadata are complete.

`Playlists` holds the global map lock only while it finds or creates an entry.
Each entry has its own commit lock, held across the initialization and part
writes. Writes to unrelated streams can run in parallel. `fin` marks the exact
entry as closing, waits for its in-flight write, clears both caches, and
removes that entry. An add that races with `fin` either commits before the
caches are cleared or adds nothing.

## Capacity and memory

The main fixed-slot count is:

```text
num_playlists × max_segments × max_parts_per_segment
```

- `buffer_size_kb` is the maximum size of one `ChunkCache` payload. It does
  not bound playlists.
- `init_size_kb` (default 5) is the maximum initialization size in both caches.
- Constructors use checked capacity arithmetic. `try_new` returns
  `CacheError::ArithmeticOverflow` for an invalid calculated capacity.

`M3u8Cache` holds one rendering per stream, not one snapshot per part: the
playlist text, its compressed copy for each read replica, and the compressed
delta update once a reader asks for it. A new part replaces the rendering when
the next reader renders.

Both caches expose `memory_stats`. These snapshots report live payload bytes and
a configured maximum. They do not include lock, map, allocator, or runtime
overhead. For `M3u8Cache`, `maximum_payload_bytes` is a bound derived from the
options: the largest rendered lines, times `max_segments` and
`max_parts_per_segment`, plus gzip's worst case per replica, for every stream,
plus `init_size_kb` per stream.

Manifest history is bounded by the retained segment and part counts. A long
stream does not retain a duration entry for every segment it has produced.
Rendition reports are limited to 256 entries, and each URI is limited to 2,048
bytes.

Exact-part waiter keys use 64 shards with a total limit of 65,536 retained
keys. Registering a waiter for an unknown stream does not create a stream or
evict a live stream.

## Missing initialization data

Initialization data is tagged with the stream ID and generation. A new or reused
stream has no initialization until a caller sets one. `M3u8Cache::get_init`
returns `CacheError::StreamNotFound` when the stream or its initialization is
absent. An HTTP service should map that crate-level result to its documented
missing-resource response, normally HTTP 404.

## Cache mesh

Enable the prototype mesh with the `mesh` feature.

```rust
use playlists::{
    chunk_cache::ChunkCache,
    mesh::{CacheMesh, CacheMeshConfig},
    Options,
};
use std::{net::SocketAddr, sync::Arc};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let cache = Arc::new(ChunkCache::new(Options::default()));
let peer: SocketAddr = "127.0.0.1:9201".parse()?;
let config = CacheMeshConfig::new("uk-1", "uk", "127.0.0.1:9101".parse()?)
    .with_peer(peer);
let mesh = CacheMesh::new(Arc::clone(&cache), config).start().await?;

cache
    .add_for_stream_id(1, 0, "part bytes".into())
    .await
    .expect("cache write");
mesh.shutdown();
# Ok(())
# }
```

A node is a `Peer` (the default: it sends the parts written on it to every
peer and receives theirs, but does not relay a part that it received), a
`Distributor` that serves children and forwards received parts to them, or an
`Edge` that receives from parents and sends no parts. A part written on a
`Peer` reaches only that node's own peers: a chain of peers does not relay. In `ReplicationMode::All` (the default) a distributor pushes every
stream to every child. In `ReplicationMode::Demand` it pushes a stream only
while the child holds a subscription lease for it.

### Peer allowlist

The mesh is closed to unconfigured source addresses. A datagram from an address
that is not allowed is dropped before FEC decode and before the receive
observer, so cut-through never forwards it. `fec_stats` counts it in
`rx_unauthorized_datagrams`.

- `with_peer` and `with_peers` connect to an address and authorize it.
- `with_allowed_peers` authorizes addresses that are not configured peers:
  children that a distributor learns from their hello, and peers that a
  trusted seed advertises through gossip.
- Runtime `CacheMeshHandle::add_peer` authorizes the address, unless the peer
  table is full (`max_peers`).

A distributor's children must be authorized. A child that is in neither its
configured peers nor its allowed peers, and was not added with `add_peer`, is
not served: its hello, subscriptions, and requests are dropped.

This address allowlist is not cryptographic authentication. Do not expose the
prototype mesh to an untrusted network. Use a private authenticated transport or
add message authentication before an Internet deployment.

### Send path

A part leaves for the children as soon as the local watermark reaches it. The
sync task wakes on the streams that the cache marks as published, not on a
scan of every stream. The `sync_interval` tick picks up new leases and children
and paces warm-up windows. Live parts are sent before any warm-up window.

A part is encoded once for all children, and the encoding is cached by stream,
slot, and payload hash. The RaptorQ encoding plan of each symbol count is kept
from part to part. A part of one symbol, which is any audio part, is repaired
by a byte table with no RaptorQ encoding. A part smaller than `symbol_size`,
and of at most 1,200 bytes, is one symbol of its own length: a 2.5 ms Opus
part is two datagrams of about 120 bytes, not two of 1,352. A large object uses
symbols of up to `max_symbol_size`.

Each child's datagrams of a pass go to the kernel in one `sendmmsg` call
(Linux). The equal-sized datagrams of an object are cut from one buffer with
UDP GSO when the kernel has it; without it the mesh falls back to one datagram
per message. Symbols stay within `symbol_size`, so no datagram is fragmented.

FEC uses a repair-symbol floor plus payload-proportional redundancy and a hard
cap. Defaults are one repair symbol, a 3% repair ratio, and at most 32 repair
symbols. Receivers report measured loss to their senders (`LinkReport`), which
raise repair for a lossy link. Configure the policy through
`CacheMeshConfig::{repair_symbols, repair_ratio, max_repair_symbols,
symbol_size, max_symbol_size}`.

The tests `a_distributor_on_one_core_sends_four_thousand_parts_a_second` (any
build) and `a_distributor_on_one_core_keeps_two_edges_at_four_thousand_parts_a_second`
(`cargo test --release`) send 4,000 parts a second for two seconds from a
distributor on one thread to two children. They require every part, and 99 in
100 within 2 ms of the commit.

### Receive path

The receive task checks the source address, runs the FEC decode, decodes the
frame without copying the payload, and applies chunk and initialization frames
inline. A `Peer` does not send parts that it received from another node. It
marks a received part as remote before it writes it, under a lock held across
the write, so the publication that the write causes cannot echo it. A
`Distributor` forwards received parts to its children.

Replica requests go to a separate bounded queue and task, so a large replica
range does not delay live receive. A full replica queue drops the new request;
the requester asks again.

Default network-input limits are:

| Limit | Default |
| --- | ---: |
| Known peers | 256 |
| Incomplete FEC objects | 64 |
| Decoded frame bytes | follows the cache |
| Replica-request queue | 64 |
| Remote slot hints | follows the cache |

- `max_frame_bytes` defaults to the larger of `buffer_size_kb` and
  `init_size_kb`, in bytes, plus 64 KiB for the frame head and the largest
  hello. A frame above the limit is not sent and is not accepted.
- `max_remote_slots` defaults to every slot the cache retains, plus
  `num_playlists` × 1,000 (one 250 ms prune interval at 4,000 parts a second).
  A full set is pruned to the retained window; a live slot is never evicted.
- Set either explicitly with `with_max_frame_bytes` or `with_max_remote_slots`.

The FEC observation path uses ordered expiry queues, pruned on every datagram
at the cost of what expires. Unauthorized and oversized objects are rejected
before decoder allocation. Edge nodes do not retain remote-forwarding hints.

`CacheMeshHandle::fec_stats()` returns a lock-free snapshot of transport
outcomes. It separates:

- source and repair traffic, protected and wire bytes, and kernel send calls
- successful decodes, and objects and source symbols recovered by FEC
- late sources, and repaired sources that remain absent after observation
- incomplete objects that expire or are evicted
- unauthorized datagrams, oversized objects, and peer-limit drops
- replica queue depth, peak depth, drops, service count, errors, and time
- decode time, and encode and decode errors.

### Split by class

`with_class_port_offset(n)` splits the mesh by class; 0, the default, keeps one
socket. A second socket at the main port plus `n` carries the parts and
initializations of the playlists that `with_stream_classifier` calls
`StreamClass::Video`, with its own receive task, sync task and replica server.
The main socket keeps the audio playlists, and the hellos, leases, replica
requests and loss reports of both. A 4K encode, send or decode then never holds
an audio part back. Every node of a mesh uses the same offset.

- The class lane runs on a thread of its own with a current-thread runtime
  (`ClassLaneRuntime::OwnThread`, the default), or on a runtime that you give
  (`ClassLaneRuntime::Handle`, with `with_class_runtime`).
- The main socket of a split mesh has `SO_PRIORITY` 6
  (`AUDIO_SOCKET_PRIORITY`, `with_main_socket_priority`), so a `prio` qdisc
  puts the audio in band 0. `with_class_socket_priority` sets the class
  socket's; it is unset by default. A failure to set it is logged, not fatal.
- A datagram on the class socket from port `p` is the peer's at `p - n`:
  leases, liveness, `AppliedSlot::parent` and the send observer name the peer's
  main address. The class socket takes chunk and initialization frames only.
- The allowlist covers the class socket. Authorizing a peer in any of the ways
  above authorizes its class socket (its address at the main port plus `n`),
  on the class socket only. A class datagram from any other address is dropped
  before the class observer and FEC decode, and counted in the class lane's
  `rx_unauthorized_datagrams`.
- The class lane has its own FEC receiver, with the same limits, its own sender
  and its own counters. `fec_stats()` is the sum of both lanes and
  `class_fec_stats()` the class lane alone. The loss reported to a peer is the
  worse of the two lanes.
- Replica requests for video playlists go to the class lane's own bounded
  queue (`replica_queue_capacity`). A request that a queued window covers is
  served once. Drops are counted in `class_replica_requests_dropped`.
- `with_class_receive_observer` gives the class receive task an observer. It
  has none by default, so the main observer (an edge's cut-through) runs on the
  audio receive task only. `with_class_applied_tx` sends the class lane's
  applied parts to a channel of their own; by default they go to `applied_tx`.
- A playlist that changes class (a muxed stream is audio until its first
  video) is handed over by the lane that it leaves, with each child's cursor,
  initialization, warm-up window and unstarted lease. The lane that it comes to
  sends nothing of it until then, so no part is lost or sent twice. The lanes
  key this, like the rest of the send state, by `StreamHandle`.

With a 200-part window of 12 kB video parts being sent, an audio part published
meanwhile reaches the edge in about 0.1 ms split, and after the whole window
(about 50 ms in a release build) on one socket. An edge that decodes such a
window applies audio parts from another distributor within about 0.2 ms split;
on one socket the first waits about 25–35 ms.

## Operation costs

| Operation | Expected cost |
| --- | --- |
| Chunk handle hit | O(1), two generation checks and one shared slot lock |
| Chunk mapped hit | O(1), plus one stream-map read |
| Chunk write | O(payload bytes) for hashing, plus one slot lock and atomic stream state |
| Chunk write time | O(1), no lock |
| Playlist part add | O(1), no rendering or compression |
| Segment close | O(parts in the segment), rendered once |
| Playlist hit, already rendered | O(1), no lock: a sequence-lock read and one `ArcSwap` load |
| First playlist read of a new version | O(retained playlist bytes): one render and one gzip, under the stream's render lock |
| First delta read of a new version | one more render and gzip of the delta update |
| Stream assignment | O(1), free index first, without clearing the payload ring |

Rendering on read moves the playlist cost from writers to the first reader of
each version. With many readers and a few hundred parts a second, nearly every
read is a hit. When versions change faster than one render, every reader
renders and readers queue on the render lock. The cost is mostly gzip
setup: about 130 µs for a 2.7 KB playlist, and about 270 µs for a delta read.
At 4,000 parts a second on one stream, delta reads fall to a few thousand a
second.

`Bytes` clones are zero-copy payload references. Logical payload bytes in the
benchmarks are not memory-copy or network throughput.

## Benchmarks

The benchmark binaries emit JSON. They run with 1, 2, 4, and all available
workers, when the host has those CPUs.

```sh
# Chunk reads by physical index, logical stream ID, or handle.
cargo bench --bench distribution_capacity -- --duration-seconds 3 --lookup handle

# Chunk raw/mapped/handle writes, 15:1 mixed load, and stream churn.
cargo bench --bench chunk_workloads -- --duration-seconds 3 --mode mixed-handle

# Full/delta hits and misses, with a writer adding parts at 400 parts/s.
cargo bench --bench playlist_cache -- --duration-seconds 3 --mode handle-delta --parts-per-second 400

# Manifest, M3u8Cache, and full Playlists write paths, each followed by a read.
cargo bench --bench playlist_writes -- --duration-seconds 3 --mode playlists-independent

# UDP receive, one-source FEC recovery, and replica range service.
cargo bench --features mesh --bench mesh_pipeline -- --duration-seconds 3 --mode recovery
```

Use these accepted mode values:

| Binary | Modes |
| --- | --- |
| `distribution_capacity` | `--lookup raw`, `mapped`, `handle` |
| `chunk_workloads` | `write-raw`, `write-mapped`, `write-handle`, `mixed-handle`, `churn` |
| `playlist_cache` | `mapped-full`, `handle-full`, `mapped-delta`, `handle-delta`, `handle-delta-replicated`, `mapped-miss`, `handle-miss`, `reuse-race` |
| `playlist_writes` | `manifest-render`, `cache-write`, `cache-write-replicated`, `playlists-independent`, `playlists-hot` |
| `mesh_pipeline` | `receive`, `recovery`, `replica` |

The playlist benches measure render on read:

- `playlist_cache` (schema `playlist-cache.v2`) seeds a stream with 40 parts.
  A writer thread adds parts at `--parts-per-second` (default 400; 0 runs no
  writer) while the workers read, so reads include the render of each new
  version. Miss modes ask for a segment far ahead. `reuse-race` resolves or
  creates a stream, adds a part, and reads it, with no separate writer. The
  report includes the writer's parts, failures, and latency.
- `playlist_writes` (schema `playlist-writes.v2`) follows every write with a
  read. `manifest-render` is `push_part_us` and `m3u8()`. `cache-write` is
  `add_part_us_for_handle` and `last_for_handle`. The `playlists-*` modes are
  `Playlists::add` and `M3u8Cache::last`.
- `mesh_pipeline` (schema `mesh-pipeline.v2`) reports unauthorized datagrams,
  decode errors, expired objects, and peer-limit drops.

Each report includes process CPU time, CPU nanoseconds per operation, allocator
calls, process live allocation bytes, retained cache payload bytes, maximum RSS,
and sampled p50, p95, p99, and maximum latency. The allocator uses 64 counter
shards. It reports the sum of shard peaks as an upper bound. On macOS,
`ru_maxrss` is in bytes. On Linux, it is in KiB. The JSON field therefore uses
the name `max_rss_platform_units`.

Run at least five samples for a release comparison. Use a quiet dedicated host.
Record the crate commit, `rustc --version --verbose`, target, CPU model, duration,
mode, worker count, payload size, median, and range. Do not use a short local run
as a service-level claim.

## Verification

```sh
cargo test --all-features --all-targets --release
cargo check --all-features --all-targets
cargo clippy --all-features --all-targets -- -D warnings
cargo fmt --check
```

## Test results

### GCP HLS-shaped HTTP serving, 2026-08-22

The downstream `web-services` test used two GCP hosts in London
(`europe-west2-b`). The server was an `n2-highcpu-4`. The load generator was a
`c4-highcpu-8`. Traffic used the private network. Each response contained one
fixed 5,760-byte `Bytes` payload. This is the size of 5 ms of 48 kHz, 24-bit PCM
for eight channels.

The one-hour H3 soak used 32 persistent connections and a pipeline depth of
eight. It ran as 12 consecutive five-minute samples against one server process.

| Result | Value |
| --- | ---: |
| Successful responses | 269,074,634 |
| Weighted mean | 74,734 responses/s |
| Five-minute range | 73,675 to 76,232 responses/s |
| Mean payload rate | 3.444 Gbit/s |
| Reported errors | 0 |
| Scheduling backpressure events | 0 |
| Highest sampled p99 latency | 32.55 ms |
| Server CPU average | 3.68 cores |
| Server RSS range | 5.5 to 20.6 MiB |

The server RSS reached about 20 MiB during load and remained near that level.
The short saturation controls reached these maximum observed rates:

| Protocol | Duration | Responses/s | Payload or application rate | Server CPU |
| --- | ---: | ---: | ---: | ---: |
| H3 | 20 s | 76,976 | 3.547 Gbit/s payload | 3.12 cores |
| H2 | 20 s | 134,606 | 743.52 MB/s reported by `h2load` | 3.56 cores |
| H1.1 | 20 s | 145,064 | 846.52 MB/s reported by `h2load` | 3.63 cores |

These are HTTP read-path results, not `playlists` crate benchmarks. The server
returned one static payload. The test did not call this crate and did not test
cache writes, playlist rendering, stream churn, encoding, or storage. It proves
that the downstream HTTP stack can serve an HLS-part-shaped retained payload at
these rates on this topology. It does not prove end-to-end stream capacity.
