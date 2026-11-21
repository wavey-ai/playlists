use crate::Options;

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MediaByteRange {
    pub length: u64,
    pub offset: u64,
}

impl MediaByteRange {
    pub fn new(length: u64, offset: u64) -> Option<Self> {
        (length > 0).then_some(Self { length, offset })
    }
}

#[derive(Clone, Copy, Debug)]
struct PartInfo {
    sequence: u64,
    duration_us: u64,
    independent: bool,
    byte_range: Option<MediaByteRange>,
}

/// Where an added part went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartPosition {
    /// The media sequence number of the part's segment.
    pub segment_id: usize,
    /// The part's sequence number, counted from 1 over the whole stream: the
    /// `N` of `pN.mp4`.
    pub sequence: usize,
    /// The part's index within its segment, from 0.
    pub index: usize,
    /// Whether the part opened a new segment.
    pub new_segment: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenditionReport {
    pub uri: String,
    pub last_msn: Option<u32>,
    pub last_part: Option<u32>,
}

impl RenditionReport {
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            last_msn: None,
            last_part: None,
        }
    }

    pub fn with_last_msn(mut self, last_msn: u32) -> Self {
        self.last_msn = Some(last_msn);
        self
    }

    pub fn with_last_part(mut self, last_part: u32) -> Self {
        self.last_part = Some(last_part);
        self
    }
}

/// A closed segment, rendered once when it closed.
#[derive(Clone, Debug)]
struct ClosedSegment {
    id: u32,
    /// Start, in microseconds after the start of the stream.
    start_us: u64,
    duration_us: u64,
    /// PROGRAM-DATE-TIME, the EXT-X-PART lines, EXTINF, BYTERANGE and URI.
    /// Dropped once the segment is too old to carry parts again.
    parted: Option<Arc<str>>,
    /// The same without the parts.
    plain: Arc<str>,
}

/// The segment that parts are being added to.
#[derive(Clone, Debug)]
struct OpenSegment {
    id: u32,
    start_us: u64,
    duration_us: u64,
    /// Bytes of the segment resource that its parts cover.
    byte_len: u64,
    parts: usize,
    last_sequence: u64,
    /// Every part so far has a byte range: the segment has one too.
    ranged: bool,
    range_start: u64,
    range_end: u64,
    /// The EXT-X-PART lines of the parts so far.
    part_lines: String,
}

impl OpenSegment {
    fn new(id: u32, start_us: u64) -> Self {
        Self {
            id,
            start_us,
            duration_us: 0,
            byte_len: 0,
            parts: 0,
            last_sequence: 0,
            ranged: true,
            range_start: 0,
            range_end: 0,
            part_lines: String::new(),
        }
    }

    fn byte_range(&self) -> Option<(u64, u64)> {
        (self.parts > 0 && self.ranged).then(|| {
            (
                self.range_start,
                self.range_end.saturating_sub(self.range_start),
            )
        })
    }
}

/// A live LL-HLS media playlist.
///
/// Adding a part is constant work: the part's line is appended to the open
/// segment, and a segment is rendered once, when it closes. A playlist is
/// rendered only when it is asked for ([`Self::m3u8`]), and then costs one
/// copy of the retained text.
///
/// Durations are microseconds and are rendered exactly, as decimal seconds
/// with up to six decimals (see [`format_seconds_us`]), so that the parts of
/// a segment sum to its EXTINF and a 2.5 ms part is `0.0025`.
pub struct M3u8Manifest {
    /// Part sequence of the newest part.
    seq: u64,
    closed: VecDeque<ClosedSegment>,
    open: OpenSegment,
    start_time: DateTime<Utc>,
    options: Options,
    rendition_reports: Vec<RenditionReport>,
    version: u64,
}

/// A playlist taken apart: the pieces are shared with the manifest, so that
/// the copy is cheap and the text can be put together without a lock.
pub struct RenderedPieces {
    head: String,
    segments: Vec<Arc<str>>,
    tail: String,
}

impl RenderedPieces {
    pub fn len(&self) -> usize {
        self.head.len()
            + self
                .segments
                .iter()
                .map(|segment| segment.len())
                .sum::<usize>()
            + self.tail.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_bytes(self) -> Bytes {
        let mut text = String::with_capacity(self.len());
        text.push_str(&self.head);
        for segment in &self.segments {
            text.push_str(segment);
        }
        text.push_str(&self.tail);
        Bytes::from(text)
    }
}

impl M3u8Manifest {
    pub fn new(options: Options) -> Self {
        let options = options.normalized();
        Self {
            seq: 0,
            closed: VecDeque::new(),
            open: OpenSegment::new(1, 0),
            start_time: Utc::now(),
            options,
            rendition_reports: Vec::new(),
            version: 0,
        }
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Advances with every change to the playlist.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Parts in the open segment.
    pub fn open_parts(&self) -> usize {
        self.open.parts
    }

    /// Whether a part with this independence would open a new segment.
    pub fn opens_segment(&self, key: bool) -> bool {
        key && self.open.duration_us >= u64::from(self.options.segment_min_ms) * 1_000
    }

    pub fn set_rendition_reports(&mut self, reports: Vec<RenditionReport>) {
        self.rendition_reports = reports;
        self.version += 1;
    }

    fn retained_segment_limit(&self) -> usize {
        self.options.max_segments.saturating_sub(1)
    }

    fn target_duration_us(&self) -> u64 {
        u64::from(ms_to_target_duration(self.options.target_duration_ms)) * 1_000_000
    }

    fn end_us(&self) -> u64 {
        self.open.start_us + self.open.duration_us
    }

    /// Whether the closed segment `age` segments before the open one (1 for
    /// the newest closed segment), starting at `start_us`, carries its parts.
    fn carries_parts(&self, age: usize, start_us: u64, end_us: u64) -> bool {
        let max = self.options.max_parted_segments;
        if max > 0 && age >= max {
            return false;
        }
        let durations = u64::from(self.options.parted_target_durations);
        durations == 0 || end_us.saturating_sub(start_us) <= durations * self.target_duration_us()
    }

    fn time_at(&self, offset_us: u64) -> DateTime<Utc> {
        self.start_time + Duration::microseconds(i64::try_from(offset_us).unwrap_or(i64::MAX))
    }

    fn program_date_time_line(&self, offset_us: u64) -> String {
        format!(
            "#EXT-X-PROGRAM-DATE-TIME:{}\n",
            self.time_at(offset_us)
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        )
    }

    fn close_segment(&mut self) {
        let next_id = self.open.id.wrapping_add(1);
        let next_start = self.end_us();
        let open = std::mem::replace(&mut self.open, OpenSegment::new(next_id, next_start));
        let limit = self.retained_segment_limit();
        if limit == 0 {
            return;
        }

        let mut tail = String::with_capacity(64);
        tail.push_str("#EXTINF:");
        push_seconds(&mut tail, open.duration_us);
        tail.push_str(",\n");
        if let Some((offset, length)) = open.byte_range() {
            if length > 0 {
                let _ = writeln!(tail, "#EXT-X-BYTERANGE:{length}@{offset}");
            }
        }
        let _ = writeln!(tail, "s{}.mp4", open.id);
        let date = self.program_date_time_line(open.start_us);
        let plain: Arc<str> = format!("{date}{tail}").into();
        let mut parted = String::with_capacity(date.len() + open.part_lines.len() + tail.len());
        parted.push_str(&date);
        parted.push_str(&open.part_lines);
        parted.push_str(&tail);

        self.closed.push_back(ClosedSegment {
            id: open.id,
            start_us: open.start_us,
            duration_us: open.duration_us,
            parted: Some(parted.into()),
            plain,
        });
        while self.closed.len() > limit {
            self.closed.pop_front();
        }
        // A segment that cannot carry its parts now never will again: the
        // end of the playlist only moves on.
        let end_us = self.end_us();
        let len = self.closed.len();
        for position in 0..len {
            let age = len - position;
            let start_us = self.closed[position].start_us;
            if !self.carries_parts(age, start_us, end_us) {
                self.closed[position].parted = None;
            }
        }
    }

    /// Add a part of `duration_us` microseconds without rendering the
    /// playlist. A part that is independent (`key`) opens a new segment once
    /// the open one is at least `segment_min_ms` long.
    pub fn push_part_us(
        &mut self,
        duration_us: u64,
        key: bool,
        byte_range: Option<MediaByteRange>,
        byte_len: Option<u64>,
    ) -> PartPosition {
        let new_segment = self.opens_segment(key);
        if new_segment {
            self.close_segment();
        }
        let index = self.open.parts;
        self.seq += 1;
        self.version += 1;
        let byte_range = byte_range.or_else(|| MediaByteRange::new(byte_len?, self.open.byte_len));
        let part = PartInfo {
            sequence: self.seq,
            duration_us,
            independent: key,
            byte_range,
        };
        let open = &mut self.open;
        match byte_range {
            Some(range) => {
                open.byte_len = open.byte_len.max(range.offset.saturating_add(range.length));
                if open.parts == 0 {
                    open.range_start = range.offset;
                }
                open.range_end = range.offset.saturating_add(range.length);
            }
            None => open.ranged = false,
        }
        open.parts += 1;
        open.duration_us += duration_us;
        open.last_sequence = self.seq;
        append_part_line(&mut open.part_lines, open.id, &part);

        PartPosition {
            segment_id: self.open.id as usize,
            sequence: self.seq as usize,
            index,
            new_segment,
        }
    }

    /// Add a part whose bytes follow the previous part's in the segment
    /// resource.
    pub fn push_part_with_byte_len_us(
        &mut self,
        duration_us: u64,
        key: bool,
        byte_len: usize,
    ) -> PartPosition {
        self.push_part_us(duration_us, key, None, Some(byte_len as u64))
    }

    fn rendered(&self, position: PartPosition) -> (Bytes, usize, usize, usize, bool) {
        (
            self.m3u8(),
            position.segment_id,
            position.sequence,
            position.index,
            position.new_segment,
        )
    }

    /// Add a part of `duration` milliseconds and render the playlist.
    pub fn add_part(&mut self, duration: u32, key: bool) -> (Bytes, usize, usize, usize, bool) {
        self.add_part_with_byte_range(duration, key, None)
    }

    pub fn add_part_with_byte_len(
        &mut self,
        duration: u32,
        key: bool,
        byte_len: usize,
    ) -> (Bytes, usize, usize, usize, bool) {
        let position = self.push_part_us(
            u64::from(duration) * 1_000,
            key,
            None,
            Some(byte_len as u64),
        );
        self.rendered(position)
    }

    pub fn add_part_with_byte_range(
        &mut self,
        duration: u32,
        key: bool,
        byte_range: Option<MediaByteRange>,
    ) -> (Bytes, usize, usize, usize, bool) {
        let position = self.push_part_us(u64::from(duration) * 1_000, key, byte_range, None);
        self.rendered(position)
    }

    /// Add a part of `duration_us` microseconds and render the playlist.
    pub fn add_part_us(
        &mut self,
        duration_us: u64,
        key: bool,
    ) -> (Bytes, usize, usize, usize, bool) {
        let position = self.push_part_us(duration_us, key, None, None);
        self.rendered(position)
    }

    /// The playlist as it is now.
    pub fn m3u8(&self) -> Bytes {
        self.pieces().into_bytes()
    }

    /// The playlist in pieces that share the retained text. Cheap: put it
    /// together with [`RenderedPieces::into_bytes`] outside any lock.
    pub fn pieces(&self) -> RenderedPieces {
        let end_us = self.end_us();
        let len = self.closed.len();
        let mut segments = Vec::with_capacity(len);
        let mut retained_us = self.open.duration_us;
        for (position, segment) in self.closed.iter().enumerate() {
            let age = len - position;
            let text = match &segment.parted {
                Some(parted) if self.carries_parts(age, segment.start_us, end_us) => parted,
                _ => &segment.plain,
            };
            segments.push(Arc::clone(text));
            retained_us += segment.duration_us;
        }

        let mut tail = String::with_capacity(self.open.part_lines.len() + 128);
        if self.open.parts > 0 {
            tail.push_str(&self.program_date_time_line(self.open.start_us));
            tail.push_str(&self.open.part_lines);
            if let Some((offset, length)) = self.open.byte_range() {
                let _ = writeln!(
                    tail,
                    "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"s{}.mp4\",BYTERANGE-START={}",
                    self.open.id,
                    offset.saturating_add(length)
                );
            } else {
                let _ = writeln!(
                    tail,
                    "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"p{}.mp4\"",
                    self.open.last_sequence.saturating_add(1)
                );
            }
        }
        append_rendition_reports(&mut tail, &self.rendition_reports);

        let target_duration = ms_to_target_duration(self.options.target_duration_ms);
        let target_duration_us = u64::from(target_duration) * 1_000_000;
        let can_skip_until_us = target_duration_us * 6;
        let can_skip = self
            .closed
            .front()
            .is_some_and(|first| retained_us.saturating_sub(first.duration_us) > can_skip_until_us);

        let mut head = String::with_capacity(320);
        head.push_str("#EXTM3U\n#EXT-X-VERSION:9\n");
        let _ = writeln!(head, "#EXT-X-TARGETDURATION:{target_duration}");
        head.push_str("#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=");
        push_seconds(&mut head, target_duration_us * 3);
        head.push_str(",PART-HOLD-BACK=");
        push_seconds(&mut head, self.options.part_hold_back_us());
        if can_skip {
            head.push_str(",CAN-SKIP-UNTIL=");
            push_seconds(&mut head, can_skip_until_us);
        }
        head.push_str("\n#EXT-X-PART-INF:PART-TARGET=");
        push_seconds(&mut head, u64::from(self.options.part_target_us()));
        let media_sequence = self
            .closed
            .front()
            .map_or(self.open.id, |segment| segment.id);
        let _ = writeln!(head, "\n#EXT-X-MEDIA-SEQUENCE:{media_sequence}");
        head.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n");

        RenderedPieces {
            head,
            segments,
            tail,
        }
    }
}

/// Append `us` microseconds as decimal seconds: up to six decimals, trailing
/// zeros trimmed, at least three decimals. 2500 µs is `0.0025`, 1000 µs is
/// `0.001`, 250 µs is `0.00025`, 41708 µs is `0.041708` and 6 s is `6.000`.
pub fn push_seconds(out: &mut String, us: u64) {
    let _ = write!(out, "{}.", us / 1_000_000);
    let mut fraction = us % 1_000_000;
    let mut digits = 6;
    while digits > 3 && fraction % 10 == 0 {
        fraction /= 10;
        digits -= 1;
    }
    let _ = write!(out, "{fraction:0digits$}");
}

/// `us` microseconds as decimal seconds (see [`push_seconds`]).
pub fn format_seconds_us(us: u64) -> String {
    let mut out = String::with_capacity(16);
    push_seconds(&mut out, us);
    out
}

/// Decimal seconds as exact microseconds. Digits beyond the sixth decimal
/// round half up. `None` for anything that is not a non-negative decimal.
pub fn parse_seconds_us(value: &str) -> Option<u64> {
    let value = value.trim();
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let whole = if whole.is_empty() {
        0
    } else {
        whole.parse::<u64>().ok()?
    };
    let mut micros = 0_u64;
    let mut digits = 0;
    let mut round_up = false;
    for (position, byte) in fraction.bytes().enumerate() {
        let digit = u64::from(byte - b'0');
        if position < 6 {
            micros = micros * 10 + digit;
            digits += 1;
        } else {
            round_up = position == 6 && digit >= 5;
            break;
        }
    }
    for _ in digits..6 {
        micros *= 10;
    }
    whole
        .checked_mul(1_000_000)?
        .checked_add(micros)?
        .checked_add(u64::from(round_up))
}

fn append_part_line(playlist: &mut String, segment_id: u32, part: &PartInfo) {
    playlist.push_str("#EXT-X-PART:DURATION=");
    push_seconds(playlist, part.duration_us);
    if let Some(byte_range) = part.byte_range {
        let _ = write!(
            playlist,
            ",URI=\"s{segment_id}.mp4\",BYTERANGE=\"{}@{}\"",
            byte_range.length, byte_range.offset
        );
    } else {
        let _ = write!(playlist, ",URI=\"p{}.mp4\"", part.sequence);
    }
    if part.independent {
        playlist.push_str(",INDEPENDENT=YES");
    }
    playlist.push('\n');
}

fn append_rendition_reports(playlist: &mut String, reports: &[RenditionReport]) {
    for report in reports {
        if !valid_quoted_string_value(&report.uri) {
            continue;
        }

        playlist.push_str("#EXT-X-RENDITION-REPORT:URI=\"");
        playlist.push_str(&report.uri);
        playlist.push('"');

        if let Some(last_msn) = report.last_msn {
            let _ = write!(playlist, ",LAST-MSN={last_msn}");
        }
        if let Some(last_part) = report.last_part {
            let _ = write!(playlist, ",LAST-PART={last_part}");
        }
        playlist.push('\n');
    }
}

fn valid_quoted_string_value(value: &str) -> bool {
    !value
        .bytes()
        .any(|byte| matches!(byte, b'"' | b'\n' | b'\r'))
}

fn ms_to_target_duration(ms: u32) -> u32 {
    u64::from(ms).div_ceil(1000).max(1) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag_line<'a>(playlist: &'a str, prefix: &str) -> &'a str {
        playlist
            .lines()
            .find(|line| line.starts_with(prefix))
            .unwrap_or_else(|| panic!("missing {prefix}"))
    }

    fn attr_f64(line: &str, name: &str) -> f64 {
        line.split_once(':')
            .map(|(_, attrs)| attrs)
            .unwrap_or(line)
            .split(',')
            .find_map(|attr| {
                let (attr_name, value) = attr.split_once('=')?;
                (attr_name == name).then(|| value.trim_matches('"').parse::<f64>().unwrap())
            })
            .unwrap_or_else(|| panic!("missing {name}"))
    }

    #[test]
    fn fresh_live_manifest_has_valid_positive_targets() {
        let manifest = String::from_utf8(M3u8Manifest::new(Options::default()).m3u8().to_vec())
            .expect("manifest utf8");

        assert!(manifest.contains("#EXT-X-TARGETDURATION:1"));
        assert!(manifest.contains("#EXT-X-PART-INF:PART-TARGET=0.0025\n"));
        assert!(manifest.contains("HOLD-BACK=3.000,"));
        assert!(manifest.contains("PART-HOLD-BACK=0.0075"));
        assert!(!manifest.contains("gap.mp4"));
        assert!(!manifest.contains("CAN-SKIP-UNTIL"));
        assert!(!manifest.contains("#EXT-X-PRELOAD-HINT"));
    }

    #[test]
    fn advertises_skip_only_when_window_can_skip() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 10,
            segment_min_ms: 1000,
            target_duration_ms: 1000,
            part_target_ms: 1000,
            ..Options::default()
        });

        for _ in 0..16 {
            manifest.add_part(1000, true);
        }

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).expect("manifest utf8");
        assert!(playlist.contains("CAN-SKIP-UNTIL=6.000"));
    }

    #[test]
    fn does_not_advertise_skip_when_ring_cannot_retain_skip_boundary() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 7,
            segment_min_ms: 1000,
            target_duration_ms: 1000,
            part_target_ms: 1000,
            ..Options::default()
        });

        for _ in 0..16 {
            manifest.add_part(1000, true);
        }

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).expect("manifest utf8");
        assert!(!playlist.contains("CAN-SKIP-UNTIL"));
    }

    #[test]
    fn emitted_ll_hls_controls_are_internally_consistent() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 64,
            segment_min_ms: 1000,
            part_target_ms: 1000,
            ..Options::default()
        });

        for _ in 0..48 {
            manifest.add_part(1000, true);
        }

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).expect("manifest utf8");
        let version = tag_line(&playlist, "#EXT-X-VERSION:")
            .trim_start_matches("#EXT-X-VERSION:")
            .parse::<u32>()
            .unwrap();
        let target_duration = tag_line(&playlist, "#EXT-X-TARGETDURATION:")
            .trim_start_matches("#EXT-X-TARGETDURATION:")
            .parse::<f64>()
            .unwrap();
        let part_target = tag_line(&playlist, "#EXT-X-PART-INF:");
        let part_target = attr_f64(part_target, "PART-TARGET");
        let server_control = tag_line(&playlist, "#EXT-X-SERVER-CONTROL:");
        let hold_back = attr_f64(server_control, "HOLD-BACK");
        let part_hold_back = attr_f64(server_control, "PART-HOLD-BACK");
        let can_skip_until = attr_f64(server_control, "CAN-SKIP-UNTIL");

        assert!(version >= 9);
        assert!(playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""));
        assert!(server_control.contains("CAN-BLOCK-RELOAD=YES"));
        assert!(hold_back >= target_duration * 3.0);
        assert!(part_hold_back >= part_target * 3.0);
        assert!(can_skip_until >= target_duration * 6.0);
        assert!(!server_control.contains("CAN-SKIP-DATERANGES"));
        assert!(playlist.contains("#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"p49.mp4\""));

        for line in playlist
            .lines()
            .filter(|line| line.starts_with("#EXT-X-PART:"))
        {
            assert!(line.contains("DURATION="));
            assert!(line.contains("URI=\"p"));
        }
    }

    #[test]
    fn target_duration_is_configured_and_stable() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 16,
            segment_min_ms: 200,
            target_duration_ms: 6000,
            part_target_ms: 500,
            ..Options::default()
        });

        for duration in [200, 300, 100, 400, 250, 150] {
            let playlist = String::from_utf8(manifest.add_part(duration, true).0.to_vec()).unwrap();
            assert!(playlist.contains("#EXT-X-TARGETDURATION:6"));
        }
    }

    #[test]
    fn part_target_is_configured_and_stable() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 16,
            segment_min_ms: 200,
            part_target_ms: 500,
            ..Options::default()
        });

        for duration in [200, 300, 100, 400, 250, 150] {
            let playlist = String::from_utf8(manifest.add_part(duration, true).0.to_vec()).unwrap();
            assert!(playlist.contains("#EXT-X-PART-INF:PART-TARGET=0.500\n"));
            assert!(playlist.contains("PART-HOLD-BACK=1.500"));
        }
    }

    #[test]
    fn emits_rendition_reports_for_peer_ll_hls_playlists() {
        let mut manifest = M3u8Manifest::new(Options::default());
        manifest.set_rendition_reports(vec![
            RenditionReport::new("720p/stream.m3u8")
                .with_last_msn(42)
                .with_last_part(3),
            RenditionReport::new("audio/stream.m3u8").with_last_msn(41),
        ]);
        manifest.add_part(500, true);

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).expect("manifest utf8");

        assert!(playlist
            .contains("#EXT-X-RENDITION-REPORT:URI=\"720p/stream.m3u8\",LAST-MSN=42,LAST-PART=3"));
        assert!(playlist.contains("#EXT-X-RENDITION-REPORT:URI=\"audio/stream.m3u8\",LAST-MSN=41"));
    }

    #[test]
    fn emits_preload_hint_for_discrete_part_resources() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 10,
            segment_min_ms: 1000,
            part_target_ms: 250,
            ..Options::default()
        });

        manifest.add_part(250, true);
        manifest.add_part(250, false);

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).expect("manifest utf8");

        assert!(playlist.contains("#EXT-X-PART:DURATION=0.250,URI=\"p1.mp4\",INDEPENDENT=YES"));
        assert!(playlist.contains("#EXT-X-PART:DURATION=0.250,URI=\"p2.mp4\""));
        assert!(playlist.contains("#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"p3.mp4\""));
    }

    #[test]
    fn emits_raw_media_byte_ranges_when_part_sizes_are_known() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 10,
            segment_min_ms: 200,
            part_target_ms: 100,
            ..Options::default()
        });

        manifest.add_part_with_byte_len(100, true, 120);
        manifest.add_part_with_byte_len(100, false, 80);
        manifest.add_part_with_byte_len(100, true, 40);

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).expect("manifest utf8");

        assert!(playlist.contains("URI=\"s1.mp4\",BYTERANGE=\"120@0\",INDEPENDENT=YES"));
        assert!(playlist.contains("URI=\"s1.mp4\",BYTERANGE=\"80@120\""));
        assert!(playlist.contains("#EXT-X-BYTERANGE:200@0\ns1.mp4"));
        assert!(playlist.contains("URI=\"s2.mp4\",BYTERANGE=\"40@0\",INDEPENDENT=YES"));
        assert!(
            playlist.contains("#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"s2.mp4\",BYTERANGE-START=40")
        );
    }

    /// Every rule of RFC 8216bis that a playlist of small parts must keep:
    /// each part at most PART-TARGET and, except an independent part or the
    /// last of a segment, at least 85% of it; PART-HOLD-BACK at least two
    /// part targets; the parts of a segment sum to its EXTINF; no EXTINF
    /// rounds above TARGETDURATION. Returns the segments that carry parts.
    fn assert_valid_ll_hls(playlist: &str) -> Vec<u32> {
        let part_target = parse_seconds_us(
            tag_line(playlist, "#EXT-X-PART-INF:")
                .split_once("PART-TARGET=")
                .unwrap()
                .1,
        )
        .unwrap();
        let target_duration = tag_line(playlist, "#EXT-X-TARGETDURATION:")
            .trim_start_matches("#EXT-X-TARGETDURATION:")
            .parse::<u64>()
            .unwrap();
        let server_control = tag_line(playlist, "#EXT-X-SERVER-CONTROL:");
        let part_hold_back = server_control
            .split(',')
            .find_map(|attr| attr.strip_prefix("PART-HOLD-BACK="))
            .and_then(parse_seconds_us)
            .unwrap();
        assert!(part_hold_back >= 2 * part_target, "{server_control}");

        let mut parts: Vec<(u64, bool)> = Vec::new();
        let mut parted = Vec::new();
        let mut extinf = None;
        for line in playlist.lines() {
            if let Some(attrs) = line.strip_prefix("#EXT-X-PART:") {
                let duration = attrs
                    .split(',')
                    .find_map(|attr| attr.strip_prefix("DURATION="))
                    .and_then(parse_seconds_us)
                    .unwrap();
                assert!(duration <= part_target, "{line} exceeds {part_target} µs");
                parts.push((duration, attrs.contains("INDEPENDENT=YES")));
            } else if let Some(value) = line.strip_prefix("#EXTINF:") {
                extinf = Some(parse_seconds_us(value.trim_end_matches(',')).unwrap());
            } else if let Some(uri) = line.strip_prefix('s').filter(|_| !line.starts_with('#')) {
                let duration = extinf.take().unwrap();
                assert!((duration + 500_000) / 1_000_000 <= target_duration);
                if !parts.is_empty() {
                    let sum: u64 = parts.iter().map(|(duration, _)| duration).sum();
                    assert_eq!(sum, duration, "parts of s{uri} do not sum to EXTINF");
                    for (index, (duration, independent)) in parts.iter().enumerate() {
                        let last = index + 1 == parts.len();
                        if !independent && !last {
                            assert!(duration * 100 >= part_target * 85, "short part in s{uri}");
                        }
                    }
                    parted.push(uri.trim_end_matches(".mp4").parse().unwrap());
                }
                parts.clear();
            }
        }
        parted
    }

    #[test]
    fn seconds_are_written_exactly_with_at_least_three_decimals() {
        for (us, text) in [
            (2_500, "0.0025"),
            (1_000, "0.001"),
            (500, "0.0005"),
            (250, "0.00025"),
            (125, "0.000125"),
            (41_708, "0.041708"),
            (16_667, "0.016667"),
            (500_000, "0.500"),
            (1_000_000, "1.000"),
            (6_000_000, "6.000"),
            (1_002_500, "1.0025"),
            (0, "0.000"),
        ] {
            assert_eq!(format_seconds_us(us), text);
            assert_eq!(parse_seconds_us(text), Some(us), "{text}");
        }
        assert_eq!(parse_seconds_us("0.50000"), Some(500_000));
        assert_eq!(parse_seconds_us("6"), Some(6_000_000));
        assert_eq!(parse_seconds_us("0.0000005"), Some(1));
        assert_eq!(parse_seconds_us("0.0000004"), Some(0));
        assert_eq!(parse_seconds_us("-1"), None);
        assert_eq!(parse_seconds_us("1e-3"), None);
        assert_eq!(parse_seconds_us(""), None);
    }

    #[test]
    fn options_derive_ring_sizes_from_time_and_keep_given_counts() {
        let options = Options::default().normalized();
        assert_eq!(options.part_target_us(), 2_500);
        // 1 s segments of 2.5 ms parts: 400 parts, a quarter more and 8.
        assert_eq!(options.max_parts_per_segment, 508);
        // 12 s of 1 s segments and the open one.
        assert_eq!(options.max_segments, 13);
        assert_eq!(options.normalized().max_parts_per_segment, 508);
        assert_eq!(options.normalized().part_target_us(), 2_500);

        let quarter_ms = Options {
            part_target_us: 250,
            ..Options::default()
        }
        .normalized();
        assert_eq!(quarter_ms.max_parts_per_segment, 4_000 + 1_000 + 8);

        // The millisecond field still works, and wins when set.
        let legacy = Options {
            part_target_ms: 500,
            part_target_us: 2_500,
            max_segments: 32,
            max_parts_per_segment: 128,
            ..Options::default()
        }
        .normalized();
        assert_eq!(legacy.part_target_us(), 500_000);
        assert_eq!(legacy.normalized().part_target_us(), 500_000);
        assert_eq!(
            (legacy.max_segments, legacy.max_parts_per_segment),
            (32, 128)
        );

        // PART-HOLD-BACK is three part targets or the configured floor.
        assert_eq!(Options::default().part_hold_back_us(), 7_500);
        let floored = Options {
            part_hold_back_ms: 250,
            ..Options::default()
        };
        assert_eq!(floored.part_hold_back_us(), 250_000);
    }

    #[test]
    fn opus_parts_of_two_and_a_half_ms_make_a_valid_playlist() {
        let mut manifest = M3u8Manifest::new(Options::default());
        // 4.5 s of 120-sample Opus parts at 48 kHz.
        for _ in 0..1_800 {
            manifest.push_part_us(2_500, true, None, None);
        }
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("#EXT-X-PART-INF:PART-TARGET=0.0025\n"));
        assert!(playlist.contains("#EXT-X-PART:DURATION=0.0025,URI=\"p1800.mp4\",INDEPENDENT=YES"));
        assert!(playlist.contains("#EXTINF:1.000,\ns4.mp4"));
        assert!(playlist.contains("#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"p1801.mp4\""));
        // The open segment has 0.5 s; the closed segments that start within
        // three target durations of the end carry their parts, the older
        // one does not.
        let parted = assert_valid_ll_hls(&playlist);
        assert_eq!(parted, vec![3, 4]);
        assert!(!playlist.contains("URI=\"p400.mp4\""));
        assert!(playlist.contains("URI=\"p801.mp4\""));
        assert!(playlist.contains("s1.mp4"));
    }

    #[test]
    fn sub_millisecond_pcm_parts_are_exact() {
        for part_us in [1_000_u32, 500, 250] {
            let mut manifest = M3u8Manifest::new(Options {
                part_target_us: part_us,
                ..Options::default()
            });
            let parts = 2_500_000 / part_us;
            for _ in 0..parts {
                manifest.push_part_us(u64::from(part_us), true, None, None);
            }
            let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
            let target = format!(
                "#EXT-X-PART-INF:PART-TARGET={}\n",
                format_seconds_us(part_us.into())
            );
            assert!(playlist.contains(&target), "{target}");
            assert!(playlist.contains("#EXTINF:1.000,\ns1.mp4"));
            assert!(playlist.contains("#EXTINF:1.000,\ns2.mp4"));
            assert_valid_ll_hls(&playlist);
        }
    }

    #[test]
    fn video_frame_parts_sum_exactly_to_the_segment() {
        // 23.976 fps: frames of 41708 or 41709 µs, a keyframe every 48.
        let mut manifest = M3u8Manifest::new(Options {
            part_target_us: 41_709,
            target_duration_ms: 2_000,
            ..Options::default()
        });
        let mut elapsed = 0_u64;
        for frame in 0..240_u64 {
            let end = (frame + 1) * 1_001_000_000 / 24_000;
            let duration = end - elapsed;
            elapsed = end;
            manifest.push_part_us(duration, frame % 48 == 0, None, None);
        }
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("#EXT-X-PART:DURATION=0.041708,"));
        assert!(playlist.contains("#EXT-X-PART:DURATION=0.041709,"));
        assert!(playlist.contains("#EXTINF:2.002,\ns1.mp4"));
        assert_valid_ll_hls(&playlist);
    }

    #[test]
    fn max_parted_segments_limits_the_segments_with_parts() {
        let mut manifest = M3u8Manifest::new(Options {
            max_parted_segments: 2,
            ..Options::default()
        });
        for _ in 0..1_000 {
            manifest.push_part_us(2_500, true, None, None);
        }
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        // The open segment and one closed segment.
        assert_eq!(assert_valid_ll_hls(&playlist), vec![2]);
        assert!(playlist.contains("URI=\"p1000.mp4\""));

        // Zero target durations keeps the parts of every retained segment.
        let mut manifest = M3u8Manifest::new(Options {
            parted_target_durations: 0,
            ..Options::default()
        });
        for _ in 0..2_000 {
            manifest.push_part_us(2_500, true, None, None);
        }
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert_eq!(assert_valid_ll_hls(&playlist), vec![1, 2, 3, 4]);
    }

    #[test]
    fn part_hold_back_has_a_configured_floor() {
        let mut manifest = M3u8Manifest::new(Options {
            part_hold_back_ms: 200,
            ..Options::default()
        });
        manifest.push_part_us(2_500, true, None, None);
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("PART-HOLD-BACK=0.200\n"));
        assert!(playlist.contains("#EXT-X-PART-INF:PART-TARGET=0.0025\n"));
    }

    #[test]
    fn adding_a_part_does_not_render_and_the_window_stays_bounded() {
        let options = Options::default();
        let mut manifest = M3u8Manifest::new(options);
        for _ in 0..40_000 {
            manifest.push_part_us(2_500, true, None, None);
        }
        // 100 s of parts: only the window is held.
        assert_eq!(manifest.closed.len(), options.normalized().max_segments - 1);
        assert!(
            manifest
                .closed
                .iter()
                .filter(|segment| segment.parted.is_some())
                .count()
                <= 3
        );
        assert_eq!(manifest.version(), 40_000);
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:88\n"));
        assert_valid_ll_hls(&playlist);
    }
}
