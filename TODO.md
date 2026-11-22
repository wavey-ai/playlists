# Open Work

This list covers the merged design: stream handles and bounded state from the
August line, with render on read, microsecond time, and the 4,000 parts/s mesh
from the low-latency line. The record of the merge, with its measurements, is
`docs/merge-august.md`; section 7 records what the merged code does.

## Terms

- A **logical stream** has a caller-supplied `u64` stream ID.
- A **physical index** is one reusable position in the fixed stream capacity.
- A **generation** identifies one assignment of a physical index.
- A **stream handle** contains a cache identity, stream ID, physical index, and
  generation.
- A **version** of a playlist changes with every part, initialization, and
  reset. A reader renders each version at most once.

## Implementation rules

- Treat the generation as part of every cache identity.
- Do not use a raw physical index as a durable logical identity.
- Put a hard bound on state that accepts churn or network input.
- Do not wait for capacity while you hold a map or slot lock.
- Resolve a stream once for a request or batch, and reuse its handle.
- Keep `Bytes` payloads zero-copy on cache hits.
- Use deterministic barriers or held locks to test race order.

## Open items

### [ ] Playlist render cost when versions change faster than a render

At a writer of 4,000 parts/s on one stream, delta reads collapse with or without
read replicas: about 3,700 reads/s, with p99 up to 4 s at 32 workers. Full reads
still do 4 to 10 million reads/s.

- One render of the 2.7 KB playlist costs about 130 µs, and a delta read about
  270 µs (two gzips). The low-latency baseline `ff09196` measures the same
  (128/263 µs), so the merge did not cause it.
- Nearly all of it is gzip. A bare `GzEncoder` (miniz_oxide) of 2.7 KB costs
  about 120 µs at `Compression::fast()`, mostly compressor setup.
- When versions change faster than one render, every reader renders, and the
  readers queue on the stream's render lock.

Measure with `playlist_cache --mode handle-delta --parts-per-second 4000`.

### [ ] Origin epoch on the wire

The mesh wire carries no generation. Mesh-applied writes therefore replace an
older ID of the same generation, so a restarted origin's parts land. But a
non-origin node's watermark never moves back after the origin restarts a stream
(`zero_stream_id`, then IDs from 0 again). A relay does not forward the
restarted parts until their IDs pass the old watermark, and an edge keeps its
old stream.

The fix is an origin epoch in CHUNK and INITIALIZATION frames, stored per
stream. A receiver that sees a new epoch recreates its stream. This changes the
wire format and the edge's cut-through parser, so it is planned with the
product (`av-mesh-run`). The low-latency line has the same gap.

### [ ] `max_inflight_frames` under 4K video with loss

The default of 64 incomplete FEC objects is unmeasured with large parts. Run 4K
video interleaved with 4,000 audio parts/s at `with_receive_loss_ppm(30_000)`.
Raise the default if `rx_evicted_objects` is above 0 without real loss.

### [ ] Product latency bench against the low-latency baseline

Run the product bench (Opus 2.5 ms parts and PCM 1 ms parts, audio only and
with 4K on the mesh) for p50, p99, and distributor CPU per part, against
`ff09196`. The in-crate 4,000 parts/s gates show no regression, but they use
72-byte parts only.

### [ ] Merge the low-latency class split

`origin/low-latency` gained `fe8318c` and `89964dc` after this merge was based:
video on its own socket and tasks, and audio priority. They need a follow-up
merge onto the merged mesh.

### [ ] Remove the unused `tls-helpers` dependency

`tls-helpers` is declared in `Cargo.toml` and used nowhere. Cargo warns
(`cargo::unused_dependencies`); `clippy -D warnings` does not cover it.

### [ ] `a_committed_part_leaves_at_once` under load

The test's 10 ms debug bound fails about once in 15 full suite runs on a loaded
host, as it did on `ff09196`.

## Integration checks

These are properties of the merged design that a deployment must satisfy.

- [ ] A distributor serves only authorized children. Every child must be a
  configured peer, in `with_allowed_peers`, or added with `add_peer`. Watch
  `rx_unauthorized_datagrams`.
- [ ] Initialization segments must fit `init_size_kb` (default 5), and the
  largest part must fit `buffer_size_kb`. Both caches refuse larger writes.
- [ ] Every mesh node runs this version: version 1 frames and 16-byte replica
  requests are refused.
- [ ] Add the downstream HTTP test for missing initialization and its 404
  mapping.

## External release qualification

These checks do not block crate work. They block a production performance or
integration claim.

- [ ] Run `cargo test --all-features --all-targets --release` on the release
  toolchain and target.
- [ ] Run each applicable benchmark at least five times on a quiet dedicated
  host.
- [ ] Record the median and range for 1, 2, 4, and 8 workers.
- [ ] Confirm that handle reads do not fall from 4 to 8 workers.
- [ ] Record mapped reads as a contended control. Do not use them as the
  high-concurrency release path.
- [ ] Confirm that independent `Playlists::add` with a read does not fall from
  4 to 8 workers.
- [ ] Confirm that a cached delta hit costs at most twice the CPU of a full hit.
- [ ] Run a long mesh churn and hostile-input RSS soak at configured limits.
- [ ] Run a long cache churn RSS soak after the retained windows are full.
