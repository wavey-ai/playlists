# Playlists & Chunk Cache

[![CI](https://github.com/wavey-ai/playlists/actions/workflows/ci.yml/badge.svg)](https://github.com/wavey-ai/playlists/actions/workflows/ci.yml)

## Overview

This crate supplies a high-throughput, lock-efficient ring buffer. It supports
Low-Latency HLS (LL-HLS) segments and other binary-packet workloads.

Components

* ChunkCache – fixed-size circular buffer of `bytes::Bytes` slots
* M3u8Cache – companion buffer that stores compressed playlist data and
  initialisation fragments
* M3u8Manifest – manifest builder that rolls segments and parts following the
  LL-HLS draft spec
* CacheMesh – UDP/RaptorQ-FEC peer discovery and cache-slot replication for
  local multi-region HLS prototypes
* Playlists – ties the two caches together and tracks active live streams

All code is asynchronous (Tokio), uses only RwLock plus atomics, and performs no
blocking I/O.

## Architecture

ChunkCache layout
    num_playlists × max_segments × max_parts_per_segment slots
        slot = atomic logical id + atomic generation + atomic hash + payload
        stream = atomic last id + atomic generation + atomic content version

M3u8Cache layout
    identical shape, but payload is gzip-compressed playlist data

• Each playlist gets its own ring buffer. Unrelated playlists never block each other.
• Writes lock exactly one slot. Reads take shared locks and can run in parallel.
• All indices (last_seg, last_part, idxs) are AtomicUsize.
• `ChunkCache::version(stream_idx)` advances after every slot mutation and stream
  reuse, allowing callers to invalidate derived manifests without rescanning
  unchanged slots on every request.

## Generic nature

ChunkCache stores raw `bytes::Bytes`.  The cache never parses or validates the
content, so it can be reused for protobuf blobs, encrypted chunks, telemetry
frames, or any other byte slice.

## Cache mesh prototype

`playlists::mesh` can:

- bind a UDP socket
- discover configured or broadcast peer caches with FEC-protected hello frames
- share known peer addresses
- copy `ChunkCache` slots by stable `stream_id`.

Use this feature for local AV mesh prototypes. One region can ingest media, and
other regions can serve the same HLS parts from their local caches.

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
let mesh = CacheMesh::new(cache.clone(), config).start().await?;

cache.add_for_stream_id(1, 0, "part bytes".into()).await?;
mesh.shutdown();
# Ok(())
# }
```

The current implementation is deliberately simple. Static seed peers or a
caller-supplied private-subnet path start discovery. Mesh `HELLO` frames share
known peer addresses. Nodes do not forward remotely replicated slots again.
This design supports the first regional prototype. It also keeps the cache API
independent of a specific media protocol.

Mesh FEC uses a configurable repair-symbol floor plus payload-proportional
redundancy and a hard cap. Defaults are one repair symbol, a 3% repair ratio,
and at most 32 repair symbols. Small realtime messages retain low overhead,
while larger cache parts receive enough independent repair symbols to tolerate
multiple packet losses without waiting for demand-triggered backfill. Configure
the policy through `CacheMeshConfig::{repair_symbols, repair_ratio,
max_repair_symbols, symbol_size}`.

`CacheMeshHandle::fec_stats()` returns a lock-free bounded snapshot of transport
outcomes. It separates:

- source and repair traffic
- protected and wire bytes
- successful decodes
- objects and missing source symbols recovered by FEC
- late sources and repaired sources that remain absent after observation
- incomplete objects that age out of the observation window
- encode and decode errors.

Recovery accounting reads only fixed datagram and RaptorQ payload-ID headers on
the hot path. Normal decoder validation remains authoritative.

## Performance

| Operation | Complexity | Details |
| --- | --- | --- |
| append / add | O(1) | one atomic operation and one write lock |
| get / last | O(1) | one atomic operation and one read lock |
| playlist rollover | O(S) | zero-fill S = max_segments × max_parts_per_segment |

### Benchmarks (Apple Silicon)

Single-stream sequential writes:
```
Slot size: 64 KB
Throughput: ~1390 MB/s
```

Concurrent multi-stream (8 streams, 1 writer + 2 readers each):
```
Write:  1864 MB/s (29,827 ops/s)
Read:   3966 MB/s (63,461 ops/s)
Combined: 5830 MB/s
```

Massive concurrent reads (1000 tasks, 1 writer):
```
Read:  22.1M ops/s
Write: 22K ops/s (concurrent)
```

Run the realistic 48 kHz S24 PCM part-hit qualification with:

```sh
cargo bench --bench distribution_capacity -- --duration-seconds 3
```

It reports `ChunkCache::get_for_stream_id` throughput for 5,760-byte parts at
1, 2, 4, and all available worker threads as JSON. Logical payload throughput
in that report is the byte length accessed through zero-copy `Bytes`. It is not
network throughput.

ChunkCache sustains millions of lookups per second under concurrent readers
because:

- Pre-allocated ring buffers (no malloc per write)
- Per-slot RwLock (readers only contend on same slot, not globally)
- Lock-free `last()` via atomic load
- Zero-copy reads via `Bytes::slice()`

Lookup operations per second, simultaneous blocked requests, open connections,
and active media customers are separate capacities. This cache benchmark makes
no claim that one edge can deliver millions of active PCM streams. HTTP/3,
encryption, kernel networking, and payload bandwidth are outside this boundary.
