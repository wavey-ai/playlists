# Playlists & Chunk Cache

[![CI](https://github.com/wavey-ai/playlists/actions/workflows/ci.yml/badge.svg)](https://github.com/wavey-ai/playlists/actions/workflows/ci.yml)

## Overview

This crate offers a high-throughput, lock-efficient ring buffer for Low-Latency HLS
(LL-HLS) segments and any other workload that needs to push and read binary packets
with minimal contention.

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

• Each playlist gets its own ring buffer; unrelated playlists never block each other.
• Writes lock exactly one slot; reads take shared locks and can run in parallel.
• All indices (last_seg, last_part, idxs) are AtomicUsize.
• `ChunkCache::version(stream_idx)` advances after every slot mutation and stream
  reuse, allowing callers to invalidate derived manifests without rescanning
  unchanged slots on every request.

## Generic nature

ChunkCache stores raw `bytes::Bytes`.  The cache never parses or validates the
content, so it can be reused for protobuf blobs, encrypted chunks, telemetry
frames, or any other byte slice.

## Cache mesh prototype

`playlists::mesh` can bind a UDP socket, discover configured or broadcast peer
caches with FEC-protected hello frames, gossip known peer addresses, and copy
`ChunkCache` slots by stable `stream_id`. This is intended for local AV mesh
prototyping where one region ingests media and other regions need to serve the
same HLS parts from their own local cache.

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

The current implementation is deliberately simple: static seed peers or a
caller-provided private-subnet discovery path bootstrap discovery, mesh `HELLO`
frames gossip known peer addresses, and remotely replicated slots are not
re-forwarded. That is enough for the first regional prototype and keeps the
cache API independent of any specific media protocol.

Mesh FEC uses a configurable repair-symbol floor plus payload-proportional
redundancy and a hard cap. Defaults are one repair symbol, a 3% repair ratio,
and at most 32 repair symbols. Small realtime messages retain low overhead,
while larger cache parts receive enough independent repair symbols to tolerate
multiple packet losses without waiting for demand-triggered backfill. Configure
the policy through `CacheMeshConfig::{repair_symbols, repair_ratio,
max_repair_symbols, symbol_size}`.

`CacheMeshHandle::fec_stats()` returns a lock-free bounded snapshot of transport
outcomes. It separates source and repair traffic, protected and wire bytes,
successful decodes, objects and missing source symbols actually recovered by
FEC, late source arrivals versus repaired sources still absent after the bounded
observation window, incomplete objects aged out of that window, and
encode/decode errors. Recovery accounting reads only the fixed datagram and
RaptorQ payload-id headers on the hot path; normal decoder validation remains
authoritative.

## Performance

Operation                  Complexity   Details
append / add               O(1)         one atomic + one write-lock
get / last                 O(1)         one atomic + one read-lock
playlist rollover          O(S)         zero-fill S = max_segments × max_parts_per_segment

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

Massive concurrent reads (1000 readers, 1 writer):
```
Read:  22.1M ops/s
Write: 22K ops/s (concurrent)
```

ChunkCache scales to millions of concurrent reads because:

- Pre-allocated ring buffers (no malloc per write)
- Per-slot RwLock (readers only contend on same slot, not globally)
- Lock-free `last()` via atomic load
- Zero-copy reads via `Bytes::slice()`
