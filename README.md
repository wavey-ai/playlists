# Playlists and Chunk Cache

[![CI](https://github.com/wavey-ai/playlists/actions/workflows/ci.yml/badge.svg)](https://github.com/wavey-ai/playlists/actions/workflows/ci.yml)

## Overview

This crate provides fixed-capacity caches and manifest tools for Low-Latency HLS
(LL-HLS) and other rolling byte streams.

- `ChunkCache` stores `bytes::Bytes` media objects in a fixed ring.
- `M3u8Cache` stores gzip-compressed full and delta playlist snapshots.
- `M3u8Manifest` renders a bounded LL-HLS media playlist.
- `Playlists` gives each active stream an independent manifest lock.
- The optional `CacheMesh` feature replicates chunk data over UDP with RaptorQ
  forward error correction (FEC).

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
is also rejected.

Mapped methods such as `get_for_stream_id` are suitable for one-shot calls.
They resolve the logical ID on each call. Handle methods avoid the global map on
the repeated read path.

The raw-index methods are only for code that owns an unassigned physical index.
They reject indices that belong to the logical-stream registry. Do not keep a
raw index from `get_stream_idx` and use it as a durable identity.

## Migrate from numeric stream indices

Older integrations can resolve a logical stream and then retain its physical
index. That index can refer to a different stream after capacity reuse.

Use these replacements for registry-owned streams:

| Old operation | Generation-safe operation |
| --- | --- |
| `get_or_create_stream_idx` or `add_stream_id` | `resolve_or_create_stream` |
| `get_stream_idx` | `resolve_stream` |
| `get` | `get_for_handle` |
| `get_last` | `get_last_for_handle` |
| `last` | `last_for_handle` |
| `version` | `version_for_handle` |
| `add` | `add_for_handle` |
| `append` | `append_for_handle` |

Store one `StreamHandle` for each request, subscription, or ingest lifetime.
Resolve a new handle after teardown or an unsuccessful handle operation. Use
`zero_stream_id` to remove a logical stream.

Raw-index operations remain available for fixed lanes. A fixed lane must not
use `resolve_stream`, `resolve_or_create_stream`, or another registry method.
Call `reset_stream_idx` before the lane starts a new logical lifetime.

## Concurrency design

`ChunkCache` stores the slot ID, generation, hash, and payload under one slot
lock. A reader either sees the complete old slot or the complete new slot.
Each stream has one grouped state value for its last ID, next ID, and content
version.

`M3u8Cache` stores one tagged snapshot with these values:

- stream ID and generation
- segment, part, and sequence
- compressed full playlist and hash
- optional compressed delta playlist and hash

`M3u8Cache::update_notifier_for_handle` exposes one fixed notification source
per physical stream lane. A blocking-reload server can enable a notification,
recheck the cache, and sleep until playlist publication, segment closure, or
stream reset. This avoids a polling task for each waiting client and does not
grow retained state with the number of requests.

`ChunkCache::update_notifier` provides the equivalent fixed notification source
for a deliberately raw lane. It wakes for successful writes, initialization
changes, and resets. Enable a notification, recheck the lane, and then await it
to avoid a missed update. Logical-stream callers should use
`exact_part_waiter` when they know the requested part ID; that path also binds
the wait to the stream generation.

The cache builds and compresses both playlist variants on a write. A latest-full
or latest-delta hit loads one `ArcSwap` snapshot. It does not decompress, parse,
or recompress the playlist. Writes use the speed-optimized gzip level.

Stream reassignment closes a per-index reuse gate, clears published state, and
advances the generation. Read paths validate the published generation before
and after they clone `Bytes`. Write paths keep the reuse gate until slot and
position metadata are complete.

`Playlists` holds the global map lock only while it finds or creates an entry.
Each entry has its own manifest lock. Writes to unrelated streams can run in
parallel. `fin` marks the exact entry as closing, waits for its in-flight write,
clears both caches, and removes that entry.

## Capacity and memory

The main fixed-slot count is:

```text
num_playlists × max_segments × max_parts_per_segment
```

`buffer_size_kb` has two related meanings:

- `ChunkCache` treats it as the maximum size of one chunk payload.
- `M3u8Cache` treats it as the maximum raw manifest size and the maximum combined
  encoded size of one full-plus-delta snapshot.

`init_size_kb` is the maximum initialization-object size. Constructors use
checked capacity arithmetic. `try_new` returns `CacheError::ArithmeticOverflow`
for an invalid calculated capacity.

Both caches expose `memory_stats`. These snapshots report live payload bytes and
the configured maximum payload bytes. They do not include lock, map, allocator,
or runtime overhead.

Manifest history is bounded by the retained segment and part counts. Program
date-time state advances when an old segment leaves the retained window. A long
stream does not retain a duration entry for every segment it has produced.
Rendition reports are limited to 256 entries, and each URI is limited to 2,048
bytes.

Exact-part waiter keys use 64 bounded shards. Registering a waiter for an unknown
stream returns `None`; it does not create a stream or evict a live stream.

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

The mesh is closed to unconfigured source addresses. `with_peer` and
`with_peers` both connect and authorize an address. `with_allowed_peers`
authorizes addresses that a trusted seed can advertise through gossip. Runtime
`add_peer` also authorizes the address.

This address allowlist is not cryptographic authentication. Do not expose the
prototype mesh to an untrusted network. Use a private authenticated transport or
add message authentication before an Internet deployment.

The receive path has three bounded stages:

1. Validate the source, decode FEC, decode the frame, and enqueue it.
2. Apply discovery, chunk, and initialization frames.
3. Serve replica ranges from a separate replica queue.

A full frame or replica queue drops the new retryable work. Cache sync and a
later replica request can retry it. `fec_stats` reports current and peak queue
depth, queue drops, decode time, replica service attempts, service errors, and
service time.

Default network-input limits are:

| Limit | Default |
| --- | ---: |
| Known peers | 256 |
| Incomplete FEC objects | 64 |
| Decoded frame bytes | 1 MiB |
| Completed-frame queue | 64 |
| Replica-request queue | 64 |
| Remote slot hints | 65,536 |

The FEC observation path uses an ordered expiry queue. It does not scan every
in-flight map for each datagram. Unauthorized and oversized objects are rejected
before decoder allocation. Edge nodes do not retain remote-forwarding hints.

## Operation costs

| Operation | Expected cost |
| --- | --- |
| Chunk handle hit | O(1), two generation checks and one shared slot lock |
| Chunk mapped hit | O(1), plus one stream-map read |
| Chunk write | O(payload bytes) for hashing, plus one slot and state update |
| Latest playlist hit | O(1), two generation validations and one `ArcSwap` load |
| Playlist write | O(retained manifest bytes), including full/delta gzip work |
| Manifest render | O(retained segments and parts) |
| Stream assignment | O(1), without clearing the complete payload ring |

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

# Full/delta hits, mapped/handle misses, and index-reuse races.
cargo bench --bench playlist_cache -- --duration-seconds 3 --mode handle-delta

# Manifest, M3u8Cache, and full Playlists write paths.
cargo bench --bench playlist_writes -- --duration-seconds 3 --mode playlists-independent

# UDP receive, one-source FEC recovery, and replica range service.
cargo bench --features mesh --bench mesh_pipeline -- --duration-seconds 3 --mode recovery
```

Use these accepted mode values:

| Binary | Modes |
| --- | --- |
| `distribution_capacity` | `--lookup raw`, `mapped`, `handle` |
| `chunk_workloads` | `write-raw`, `write-mapped`, `write-handle`, `mixed-handle`, `churn` |
| `playlist_cache` | `mapped-full`, `handle-full`, `mapped-delta`, `handle-delta`, `mapped-miss`, `handle-miss`, `reuse-race` |
| `playlist_writes` | `manifest-render`, `cache-write`, `playlists-independent`, `playlists-hot` |
| `mesh_pipeline` | `receive`, `recovery`, `replica` |

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
