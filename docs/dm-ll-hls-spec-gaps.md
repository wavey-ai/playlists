# DM: LL-HLS Specification Gaps

Status: Draft
Date: 2026-06-02
Source reviewed: `draft-pantos-hls-rfc8216bis-22`, dated 2026-05-01, from the user-provided text.
Local HTTP layer reviewed: current working tree of `/Users/jamie/wavey.ai/web-services`, specifically the workspace manifest, `upload-response/Cargo.toml`, `hls/src/lib.rs`, `web-service/src/http_range.rs`, and the OBS LL-HLS example.
Implementation status updated: 2026-06-02 after playlist byte-range, open-parent serving, fixed target-duration, Advance Part Limit, and preload range blocking fixes.

## Context

Low-Latency HLS in this draft is defined by the combined use of Partial Segments, Blocking Playlist Reload, and preload hinting. This review found several areas that are either ambiguous, internally inconsistent, or not specific enough for robust interoperable server and client behavior.

Evidence boundary: this memo only uses the draft text pasted by the user and the local files listed above. It does not assume behavior from upstream repositories, unpublished server code, CDN configuration, or clients.

## High-Impact Gaps

### 1. LL-HLS tags do not have explicit protocol-version requirements

References: Sections 4.4.3.7, 4.4.3.8, 4.4.4.9, 4.4.5.3, 4.4.5.4, and 8.

Section 8 defines version requirements for `EXT-X-SKIP`, but does not define minimum `EXT-X-VERSION` values for `EXT-X-PART-INF`, `EXT-X-SERVER-CONTROL`, `EXT-X-PART`, `EXT-X-PRELOAD-HINT`, or `EXT-X-RENDITION-REPORT`.

Impact: playlist authors have no clear version floor for core LL-HLS features. Implementations may choose different versions, and validators may disagree.

Recommendation: define explicit minimum versions for each LL-HLS tag or state clearly that these tags are intentionally backward-compatible and do not require a higher `EXT-X-VERSION`.

### 2. Client part-selection behavior is underspecified

References: Sections 3.2, 6.3.3, 6.3.5, and 6.3.8.

Section 6.3.5 defines how to choose the next Media Segment, but there is no equivalent normative algorithm for choosing the next Partial Segment by `(Media Sequence Number, Part Index)`. Section 6.3.3 says clients may play Media Segments or Partial Segments in playlist order, but does not specify how to avoid duplicate loading when a client has already loaded all parts of a Parent Segment and the full Parent Segment later appears.

Impact: clients can differ on whether they download full segments after parts, how they resume after a reload, and how they recover when a part disappears from the live edge.

Recommendation: add a "Determining the Next Partial Segment to Load" subsection covering part index tracking, parent segment completion, duplicate suppression, fallback to full segments, and transition from regular-latency to low-latency playback.

### 3. "End of Playlist" is ambiguous for low-latency hold-back

References: Sections 4.4.3.8 and 6.3.3.

`HOLD-BACK` and `PART-HOLD-BACK` are defined as distances from the "end of the Playlist", but the draft does not explicitly define whether the end is the end of the last complete Media Segment, the last listed Partial Segment, the hinted but not yet listed Partial Segment, or the open Parent Segment's inferred duration.

Impact: clients can compute different live edges and therefore play at different latency or stall risk.

Recommendation: define the live edge calculation for playlists with complete segments only, with listed parts, with an open parent, with gaps, and with preload hints.

### 4. Blocking reload behavior is incomplete for stale, impossible, and part-only requests

References: Section 6.2.5.2.

The server must deliver the entire playlist "even if the requested Media Segment is not the last one in the Playlist, and even if it is no longer in the Playlist." It is unclear whether this means the current playlist may omit the requested MSN, whether the server must reconstruct an older playlist containing it, or whether a stale request should return an error.

The behavior is also unclear when:

- `_HLS_part` is provided for a playlist that currently has no parts.
- `_HLS_msn` points to an open parent that only has Partial Segments.
- The requested part index is beyond the final part, but the final part is not known yet.
- The requested MSN is older than the retained window.

Impact: origin and CDN implementations may block, return current state, or error in different ways for the same request.

Recommendation: define request classes and expected responses for stale, current, future, impossible, and part-only blocking reload requests.

### 5. Delta-update merge semantics are not precise enough

References: Sections 4.4.5.2, 6.2.5.1, and 6.3.7.

Clients are told to merge a Playlist Delta Update with the previous playlist, but the merge algorithm is not specified. The most important missing details are:

- Whether stateful tags such as `EXT-X-MAP`, `EXT-X-KEY`, `EXT-X-DISCONTINUITY`, `EXT-X-PROGRAM-DATE-TIME`, and `EXT-X-BITRATE` must be repeated when their original occurrence was before the skipped range but their state still applies to unskipped segments.
- How clients should merge `EXT-X-DATERANGE` updates when tags were skipped or recently removed.
- Whether `_HLS_skip=v2` must be treated as `YES` if `CAN-SKIP-DATERANGES` is absent, or rejected/ignored.
- Whether a delta update with zero skipped segments is valid and what `EXT-X-SKIP:SKIPPED-SEGMENTS=0` means.

Impact: delta playlists can become non-self-describing and clients may fail to reconstruct the active encryption, map, discontinuity, or metadata state.

Recommendation: provide a deterministic merge algorithm and require all state needed by the first unskipped segment or part to be present in the delta response.

### 6. Partial Segment media-format requirements need more detail

References: Sections 3.1, 3.1.2, 3.2, and 4.4.4.9.

The draft says a Partial Segment must be in a supported Media Segment format and gives CMAF chunks as an example, but it does not spell out format-specific requirements for parts. For fMP4, it is not explicit whether every part must be a complete parseable fragment with `moof`/`mdat` and `tfdt`, or whether other chunk shapes are valid. For MPEG-TS, Packed Audio, WebVTT, and IMSC, part boundary and timestamp requirements are not detailed.

Impact: servers may produce parts that are valid for one player but not another.

Recommendation: add format-specific part constraints, especially for fMP4/CMAF chunk structure, TS packet alignment, audio timestamp signaling, and subtitle cue splitting.

### 7. Gap semantics conflict with part/parent sample equivalence

References: Sections 3.2, 4.4.4.9, 6.2.1, and 6.3.3.

Section 3.2 says a Parent Segment and its complete set of Partial Segments must contain the same media samples. Elsewhere, Partial Segments may carry `GAP=YES`, and Media Segments may carry `EXT-X-GAP`, meaning the resource is unavailable and should not be loaded.

Impact: it is unclear whether a gap part contributes no samples, placeholder timeline duration, or samples that are present only in the parent. It is also unclear whether any `GAP=YES` part requires the Parent Segment to be marked `EXT-X-GAP`.

Recommendation: define how gap parts contribute to the parent timeline and whether parent gap marking is required when one or more child parts are gaps.

### 8. `PART-TARGET` mutability is not explicitly constrained

References: Sections 4.4.3.7, 4.4.3.8, 6.2.1, and 6.2.2.

The draft states that `EXT-X-TARGETDURATION` must not change, but does not explicitly say whether `EXT-X-PART-INF:PART-TARGET` can change over the lifetime of a live playlist.

Impact: changing `PART-TARGET` affects `PART-HOLD-BACK`, Advance Part Limit, client reload cadence, and low-latency tuning.

Recommendation: explicitly state whether `PART-TARGET` is immutable for a Media Playlist, and if it can change, define when clients must apply the new value.

### 9. `INDEPENDENT=YES` does not define independence across tracks

References: Sections 4.4.4.9, 6.3.3, and Appendix B.1.

`INDEPENDENT=YES` is defined as indicating that a Partial Segment contains an independent frame. It does not define whether independence applies to all tracks, only the video track, or the whole playable part.

Impact: a client may join or switch at a part marked independent and still need prior audio, metadata, or initialization state.

Recommendation: define `INDEPENDENT=YES` in terms of all samples needed to decode and present the part, or explicitly limit it to video-frame independence.

## Medium-Impact Gaps

### 10. Preload hint range behavior contains a naming inconsistency

References: Sections 4.4.5.3 and 6.2.6.

`EXT-X-PRELOAD-HINT` defines `BYTERANGE-START` and `BYTERANGE-LENGTH`, but Section 6.2.6 refers to `BYTERANGE-OFFSET`, which is not a defined attribute.

Impact: implementers may look for or emit the wrong attribute name.

Recommendation: replace `BYTERANGE-OFFSET` with `BYTERANGE-START` in Section 6.2.6.

### 11. Preload hint HTTP behavior is underspecified

References: Sections 4.4.5.3, 6.2.6, and 6.3.8.

The draft says hinted resources must be available for request when the hint is added, but the server may block until bytes are available. It does not define enough HTTP behavior for unknown-length ranges, 200 vs 206 responses, 416 handling, cache headers, server timeouts, or client retry behavior after a previously hinted resource is abandoned.

Impact: preload behavior can differ across origins and CDNs, especially when byte ranges are used with unknown final length.

Recommendation: add HTTP response requirements and client retry/cancel rules for hinted resources, including byte-range requests with omitted `BYTERANGE-LENGTH`.

### 12. Rendition Report defaults are unclear

References: Section 4.4.5.4 and Appendix B.1.

The draft allows a server to omit even mandatory `EXT-X-RENDITION-REPORT` attributes if the omitted value is the same as "that of the Rendition Report of the Media Playlist to which the tag is being added." This wording is hard to interpret because the containing playlist is not itself a Rendition Report.

Impact: clients may not know which values to infer when attributes are omitted.

Recommendation: clarify that omitted values default to the containing Media Playlist's current last MSN and last part, if that is the intended meaning.

### 13. Program date/time mapping for parts is implicit

References: Sections 4.4.4.6, 4.4.4.9, 6.2.4, 6.3.3, and Appendix B.1.

The low-latency profile requires Media Playlists to have `EXT-X-PROGRAM-DATE-TIME` tags, and Section 4.4.4.9 says the tag applies to the first part when placed before the first `EXT-X-PART` of a Parent Segment. The draft does not define a normative part-level mapping algorithm for later parts in the same parent, open parents, discontinuities, or drift correction.

Impact: rendition synchronization and live-edge estimation can vary by client.

Recommendation: define how to derive program date/time for every part from parent-level PDT plus part durations and media timestamps.

### 14. Low-Latency Server Configuration Profile mixes verifiable and unverifiable requirements

References: Appendix B.1.

The profile tells clients to verify server requirements before playing at less than two Target Durations, but several requirements cannot be verified from a playlist or by a normal HLS client, such as route-wide TCP SACK support, CDN request coalescing, and whether every server offers the full Variant Stream set.

Impact: clients have no clear conformance signal or fallback trigger.

Recommendation: separate client-verifiable playlist requirements from deployment recommendations, and define an explicit profile-advertisement or conformance signal if clients are expected to verify support.

### 15. Delivery Directive query canonicalization is underspecified

References: Sections 6.2.5 and 6.3.2.

Clients must put all query parameters in UTF-8 order, but the draft does not specify how to handle existing non-HLS parameters, duplicate keys, percent-encoding normalization, or whether ordering is by decoded or encoded form.

Impact: cache-key behavior can diverge across clients and CDNs.

Recommendation: define a canonical query serialization rule for Delivery Directives and existing query parameters.

## Lower-Impact Editorial or Consistency Issues

### 16. Low-latency examples depend on hidden required tags

Reference: Section 9.11.

The low-latency playlist example uses ellipses around the area where required tags such as `EXT-X-PART-INF` and likely `EXT-X-SERVER-CONTROL` would appear. This makes the example less useful as a conformance model.

Recommendation: include a complete minimal LL-HLS example with visible `EXT-X-VERSION`, `EXT-X-PART-INF`, `EXT-X-SERVER-CONTROL`, parts, a preload hint, and a rendition report.

### 17. Date-range text contains a likely attribute typo

Reference: Section 4.4.5.1.1.

The SCTE-35 mapping text says `END-TIME` must not be present, but the defined Date Range attribute is `END-DATE`.

Impact: this is not LL-HLS-specific, but it affects delta-update and date-range behavior used by low-latency live streams.

Recommendation: replace `END-TIME` with `END-DATE`.

## Local HTTP Layer Check

### A. `upload-response` does not currently expose an `hls` feature

The current `web-services` workspace has separate `hls` and `upload-response` members (`/Users/jamie/wavey.ai/web-services/Cargo.toml`, lines 1-8). The current `upload-response/Cargo.toml` features are `tcp`, `srt`, `rist`, `rist-pure`, `webrtc`, and `udp-fec`; there is no `hls` feature in that manifest (`/Users/jamie/wavey.ai/web-services/upload-response/Cargo.toml`, lines 6-13).

Verified implication: HLS HTTP behavior should be reviewed through the `hls` crate and `web-service` transport layer, not through a feature flag named `hls` on `upload-response`.

### B. Playlist and byte-range responses are handled differently

The HLS handler returns playlists with `Accept-Ranges: none`, `Vary: accept-encoding`, and optional `content-encoding: gzip` based on `Accept-Encoding`; if a playlist request contains a `Range` header, it serves the decompressed identity playlist instead of a ranged playlist (`hls/src/lib.rs`, lines 257-283).

The shared HTTP range layer skips range processing when a handler has already set `Accept-Ranges: none`, and also skips range processing for non-identity encoded responses (`web-service/src/http_range.rs`, lines 28-40). For other successful buffered responses, it applies single-byte-range slicing and returns `206` or `416` as appropriate (`web-service/src/http_range.rs`, lines 42-83).

Verified implication: playlist range requests are deliberately disabled, while part and segment body responses can inherit byte-range support unless their handler adds headers that disable it.

### C. Blocking Playlist Reload policy is explicit locally, but differs from one draft recommendation

The HLS handler rejects `_HLS_part` without `_HLS_msn` with `400`, and rejects non-numeric `_HLS_msn` or `_HLS_part` with `400` (`hls/src/lib.rs`, lines 184-224).

For stale requests, the handler returns the latest playlist or latest delta rather than reconstructing an older playlist containing the requested MSN (`hls/src/lib.rs`, lines 426-437 and 456-466).

For future requests within the draft's allowed look-ahead, the handler polls for the requested snapshot until it is available or until an 18-second timeout returns `503` (`hls/src/lib.rs`, lines 441-480). Requests with `_HLS_msn` more than the current last MSN plus two now return `400`. Requests with `_HLS_part` beyond the current last part by more than the Advance Part Limit now return `400`; for sub-second parts, the limit is computed as `ceil(3 / PART-TARGET)`.

Verified implication: the local code now follows the draft's SHOULD-level Bad Request recommendation for too-far-ahead MSN and part requests. The remaining policy gap is stale-window behavior: the handler serves the latest playlist rather than reconstructing an older playlist containing an MSN that has already fallen out of the retained window.

### D. Delta-update behavior is narrower than the draft's full surface

The HLS handler treats `_HLS_skip=YES` and `_HLS_skip=v2` identically (`hls/src/lib.rs`, lines 339-340). The `M3u8Cache` delta generator only requires `CAN-SKIP-UNTIL`, inserts one `EXT-X-SKIP`, and retains lines based on parsed media blocks (`src/m3u8_cache.rs`, lines 509-549).

The local delta parser recognizes only `EXT-X-GAP`, `EXT-X-PROGRAM-DATE-TIME`, and `EXT-X-PART` as segment-scoped tags when finding media block boundaries. It does not implement special handling for `EXT-X-KEY`, `EXT-X-MAP`, `EXT-X-DISCONTINUITY`, `EXT-X-BITRATE`, `CAN-SKIP-DATERANGES`, or `RECENTLY-REMOVED-DATERANGES`.

Verified implication: this confirms Gap 5 for the local code. Delta updates can be correct for the simple generated playlists currently tested, but the implementation is not a complete general-purpose delta-update merger/generator for all legal LL-HLS playlists.

### E. Delivery Directive query handling is simple string splitting

The HLS handler builds the directive map with `split('&')` and `split_once('=')` (`hls/src/lib.rs`, lines 512-514). It does not verify UTF-8 query parameter order, percent-decode names or values, or define duplicate-key behavior.

Verified implication: this confirms Gap 15. The draft asks clients to serialize query parameters in UTF-8 order for cache efficiency, but server behavior for unsorted, encoded, or duplicate parameters remains a policy choice locally.

### F. Preload hint support is split across implementations

The `playlists` manifest builder now emits byte-ranged `EXT-X-PART` entries for known fMP4 part sizes, byte-ranged parent segment entries, and a live `EXT-X-PRELOAD-HINT:TYPE=PART` for the current parent segment using `BYTERANGE-START` with no speculative `BYTERANGE-LENGTH`.

The main `hls` handler now detects byte-range requests for a parent segment URI currently advertised by `EXT-X-PRELOAD-HINT:TYPE=PART` and blocks until the requested start offset has complete part bytes available. This prevents the active open parent resource from returning an immediate short body, `416`, or `404` when a client follows a no-length preload hint.

The OBS LL-HLS example also emits `EXT-X-PRELOAD-HINT:TYPE=PART` and serves hinted part paths through a blocking `get_part_blocking` loop that waits until the part is known, too old, too far ahead, or timed out.

Verified implication: the main `playlists` plus `hls` path now has the core preload-hint surface for byte-ranged fMP4 parent segments. Remaining gaps are multi-part unknown-length range streaming semantics and full delta-update state preservation for legal playlists outside our generated subset.

### G. Open parent segment byte ranges must be served before closure

The generated LL-HLS playlist can advertise an open parent segment URI such as `s5.mp4` through `EXT-X-PART:URI="s5.mp4",BYTERANGE="..."` before segment 5 is closed by the next parent segment. The HLS server must therefore be able to assemble `s5.mp4` from the current open part sequence, not only from closed segment boundaries.

Verified implication: the playlist cache now tracks the latest global part sequence separately from the latest part index, and exposes the current open parent range as `[previous_boundary, latest_seq + 1)`. This prevents valid byte-range requests for the live parent resource from returning `404` solely because the parent segment has not closed yet.

### H. Target duration is configured, stable, and no longer derived from the retained window

The manifest builder no longer pads startup playlists with synthetic `EXT-X-GAP` media segments and no longer derives `EXT-X-TARGETDURATION` from whatever segments happen to be retained. It uses the configured target duration for the whole live playlist lifetime.

Verified implication: this aligns with the draft rule that the `EXT-X-TARGETDURATION` value in a Media Playlist MUST NOT change. It also prevents retained-window churn from changing `CAN-SKIP-UNTIL`, `HOLD-BACK`, and player reload cadence.

## Implementation Watch List

For any server or manifest builder targeting this draft:

- Do not assume an `upload-response` feature named `hls`; current local HLS routing lives in the `hls` crate.
- Do not advertise `CAN-BLOCK-RELOAD` unless the playlist endpoint implements `_HLS_msn` and `_HLS_part` blocking semantics.
- Reject `_HLS_msn` values more than the current last MSN plus two, and reject `_HLS_part` values beyond the current last part by more than the Advance Part Limit. This is implemented in `hls`.
- Do not advertise `CAN-SKIP-UNTIL` unless delta updates are implemented and preserve all state needed by unskipped media.
- Do not treat `_HLS_skip=v2` as equivalent to `YES` unless Date Range skipping is intentionally unsupported and the response is known to be safe.
- Emit `EXT-X-PART-INF` whenever `EXT-X-PART` is present.
- Treat `PART-TARGET` as effectively immutable unless the final RFC/draft explicitly allows mutation.
- Include `EXT-X-PRELOAD-HINT` for live playlists with parts if the goal is compliance with the Low-Latency Server Configuration Profile. This is implemented for generated byte-ranged fMP4 playlists.
- Use complete byte-range semantics for parts and hints; avoid omitted offsets unless the previous part range is unambiguous and belongs to the same parent resource. The main `hls` path blocks preload range starts until at least one complete additional part is available.
- Keep `EXT-X-TARGETDURATION` configured and stable; do not derive it from the retained window.
