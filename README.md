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
* Playlists – ties the two caches together and tracks active live streams

All code is asynchronous (Tokio), uses only RwLock plus atomics, and performs no
blocking I/O.

## Architecture

ChunkCache layout
    num_playlists × max_segments × max_parts_per_segment slots
        slot = size(u32) | hash(u64) | payload

M3u8Cache layout
    identical shape, but payload is gzip-compressed playlist data

• Each playlist gets its own ring buffer; unrelated playlists never block each other.
• Writes lock exactly one slot; reads take shared locks and can run in parallel.
• All indices (last_seg, last_part, idxs) are AtomicUsize.

## Generic nature

ChunkCache stores raw `bytes::Bytes`.  The cache never parses or validates the
content, so it can be reused for protobuf blobs, encrypted chunks, telemetry
frames, or any other byte slice.

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
