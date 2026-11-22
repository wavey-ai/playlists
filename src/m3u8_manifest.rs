use crate::Options;

use bytes::Bytes;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::Arc;

/// At most this many rendition reports are kept.
const MAX_RENDITION_REPORTS: usize = 256;
/// A rendition report with a longer URI is dropped.
const MAX_RENDITION_URI_BYTES: usize = 2_048;
/// Room for one EXT-X-PROGRAM-DATE-TIME line, extended years included.
const PROGRAM_DATE_TIME_LINE_BYTES: usize = 56;
/// Room for the PROGRAM-DATE-TIME line and the PRELOAD-HINT of the open
/// segment, whatever their numbers.
const OPEN_SEGMENT_TAG_BYTES: usize = 160;
/// CAN-SKIP-UNTIL is six target durations (RFC 8216bis 4.4.3.8).
const CAN_SKIP_TARGET_DURATIONS: u64 = 6;
const MEDIA_SEQUENCE_TAG: &str = "#EXT-X-MEDIA-SEQUENCE:";
const MAP_LINE: &str = "\n#EXT-X-MAP:URI=\"init.mp4\"\n";

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
    /// The EXT-X-RENDITION-REPORT lines, rendered when they are set.
    rendition_report_lines: String,
    version: u64,
    /// The tags up to PART-INF, which depend only on the options: without
    /// and with CAN-SKIP-UNTIL. Built once, shared by every rendering.
    head_without_skip: Arc<str>,
    head_with_skip: Arc<str>,
}

/// A playlist taken apart: the pieces are shared with the manifest, so that
/// the copy is cheap and the text can be put together without a lock.
pub struct RenderedPieces {
    /// The tags up to PART-INF, shared with the manifest.
    head: Arc<str>,
    /// EXT-X-MEDIA-SEQUENCE, written straight into the playlist.
    media_sequence: u32,
    segments: Vec<Arc<str>>,
    tail: String,
}

impl RenderedPieces {
    /// The exact length of the playlist in bytes.
    pub fn len(&self) -> usize {
        self.head.len()
            + MEDIA_SEQUENCE_TAG.len()
            + decimal_digits(self.media_sequence)
            + MAP_LINE.len()
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

    /// The playlist, in one allocation of exactly its length.
    pub fn into_bytes(self) -> Bytes {
        let len = self.len();
        let mut text = String::with_capacity(len);
        text.push_str(&self.head);
        text.push_str(MEDIA_SEQUENCE_TAG);
        let _ = write!(text, "{}", self.media_sequence);
        text.push_str(MAP_LINE);
        for segment in &self.segments {
            text.push_str(segment);
        }
        text.push_str(&self.tail);
        debug_assert_eq!(text.len(), len);
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
            rendition_report_lines: String::new(),
            version: 0,
            head_without_skip: render_head(&options, false),
            head_with_skip: render_head(&options, true),
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

    /// Whether a part of `duration_us` with this independence would open a
    /// new segment: an independent part once the open segment is at least
    /// `segment_min_ms` long, or any part that would make the open
    /// segment's EXTINF round above TARGETDURATION.
    pub fn opens_segment(&self, duration_us: u64, key: bool) -> bool {
        if self.open.parts == 0 {
            return false;
        }
        (key && self.open.duration_us >= u64::from(self.options.segment_min_ms) * 1_000)
            || self.open.duration_us.saturating_add(duration_us) >= self.options.segment_limit_us()
    }

    /// Set the EXT-X-RENDITION-REPORT lines. A report whose URI cannot be a
    /// quoted string or is longer than 2,048 bytes is dropped, and at most
    /// 256 reports are kept.
    pub fn set_rendition_reports(&mut self, reports: Vec<RenditionReport>) {
        self.rendition_report_lines.clear();
        for report in reports
            .iter()
            .filter(|report| {
                report.uri.len() <= MAX_RENDITION_URI_BYTES
                    && valid_quoted_string_value(&report.uri)
            })
            .take(MAX_RENDITION_REPORTS)
        {
            append_rendition_report(&mut self.rendition_report_lines, report);
        }
        self.version += 1;
    }

    fn retained_segment_limit(&self) -> usize {
        self.options.max_segments.saturating_sub(1)
    }

    fn target_duration_us(&self) -> u64 {
        u64::from(self.options.target_duration_secs()) * 1_000_000
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
        i64::try_from(offset_us)
            .ok()
            .and_then(|offset_us| {
                self.start_time
                    .checked_add_signed(Duration::microseconds(offset_us))
            })
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
    }

    fn close_segment(&mut self) {
        let next_id = self.open.id.wrapping_add(1);
        let next_start = self.end_us();
        let open = std::mem::replace(&mut self.open, OpenSegment::new(next_id, next_start));
        let limit = self.retained_segment_limit();
        if limit == 0 {
            return;
        }

        let mut date = String::with_capacity(PROGRAM_DATE_TIME_LINE_BYTES);
        append_program_date_time(&mut date, self.time_at(open.start_us));
        let mut tail = String::with_capacity(96);
        tail.push_str("#EXTINF:");
        push_seconds(&mut tail, open.duration_us);
        tail.push_str(",\n");
        if let Some((offset, length)) = open.byte_range() {
            if length > 0 {
                let _ = writeln!(tail, "#EXT-X-BYTERANGE:{length}@{offset}");
            }
        }
        let _ = writeln!(tail, "s{}.mp4", open.id);
        let mut plain = String::with_capacity(date.len() + tail.len());
        plain.push_str(&date);
        plain.push_str(&tail);
        let mut parted = String::with_capacity(date.len() + open.part_lines.len() + tail.len());
        parted.push_str(&date);
        parted.push_str(&open.part_lines);
        parted.push_str(&tail);

        self.closed.push_back(ClosedSegment {
            id: open.id,
            start_us: open.start_us,
            duration_us: open.duration_us,
            parted: Some(parted.into()),
            plain: plain.into(),
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
    /// the open one is at least `segment_min_ms` long; any part opens one
    /// that the open segment cannot take without its EXTINF rounding above
    /// TARGETDURATION.
    pub fn push_part_us(
        &mut self,
        duration_us: u64,
        key: bool,
        byte_range: Option<MediaByteRange>,
        byte_len: Option<u64>,
    ) -> PartPosition {
        let new_segment = self.opens_segment(duration_us, key);
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

        let mut tail = String::with_capacity(
            self.open.part_lines.len() + self.rendition_report_lines.len() + OPEN_SEGMENT_TAG_BYTES,
        );
        if self.open.parts > 0 {
            append_program_date_time(&mut tail, self.time_at(self.open.start_us));
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
        tail.push_str(&self.rendition_report_lines);

        let can_skip_until_us = self.target_duration_us() * CAN_SKIP_TARGET_DURATIONS;
        let can_skip = self
            .closed
            .front()
            .is_some_and(|first| retained_us.saturating_sub(first.duration_us) > can_skip_until_us);
        let head = if can_skip {
            &self.head_with_skip
        } else {
            &self.head_without_skip
        };
        let media_sequence = self
            .closed
            .front()
            .map_or(self.open.id, |segment| segment.id);

        RenderedPieces {
            head: Arc::clone(head),
            media_sequence,
            segments,
            tail,
        }
    }
}

/// The tags from EXTM3U to PART-INF, which depend only on the (normalized)
/// options.
fn render_head(options: &Options, can_skip: bool) -> Arc<str> {
    let target_duration = options.target_duration_secs();
    let target_duration_us = u64::from(target_duration) * 1_000_000;
    let mut head = String::with_capacity(256);
    head.push_str("#EXTM3U\n#EXT-X-VERSION:9\n");
    let _ = writeln!(head, "#EXT-X-TARGETDURATION:{target_duration}");
    head.push_str("#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=");
    push_seconds(&mut head, target_duration_us * 3);
    head.push_str(",PART-HOLD-BACK=");
    push_seconds(&mut head, options.part_hold_back_us());
    if can_skip {
        head.push_str(",CAN-SKIP-UNTIL=");
        push_seconds(&mut head, target_duration_us * CAN_SKIP_TARGET_DURATIONS);
    }
    head.push_str("\n#EXT-X-PART-INF:PART-TARGET=");
    push_seconds(&mut head, u64::from(options.part_target_us()));
    head.push('\n');
    head.into()
}

fn decimal_digits(value: u32) -> usize {
    value
        .checked_ilog10()
        .map_or(1, |digits| digits as usize + 1)
}

/// Append an EXT-X-PROGRAM-DATE-TIME line: RFC 3339 with milliseconds and
/// `Z`, as chrono's `to_rfc3339_opts(SecondsFormat::Millis, true)` writes
/// it (extended years and leap seconds included), without allocating.
fn append_program_date_time(playlist: &mut String, date_time: DateTime<Utc>) {
    playlist.push_str("#EXT-X-PROGRAM-DATE-TIME:");
    let year = date_time.year();
    if (0..=9_999).contains(&year) {
        let _ = write!(playlist, "{year:04}");
    } else {
        let _ = write!(playlist, "{year:+05}");
    }

    let mut second = date_time.second();
    let mut nanosecond = date_time.nanosecond();
    if nanosecond >= 1_000_000_000 {
        second += 1;
        nanosecond -= 1_000_000_000;
    }
    let _ = writeln!(
        playlist,
        "-{:02}-{:02}T{:02}:{:02}:{second:02}.{:03}Z",
        date_time.month(),
        date_time.day(),
        date_time.hour(),
        date_time.minute(),
        nanosecond / 1_000_000
    );
}

/// Append `us` microseconds as decimal seconds: up to six decimals, trailing
/// zeros trimmed, at least three decimals. 2500 µs is `0.0025`, 1000 µs is
/// `0.001`, 250 µs is `0.00025`, 41708 µs is `0.041708` and 6 s is `6.000`.
pub fn push_seconds(out: &mut String, us: u64) {
    let _ = write!(out, "{}.", us / 1_000_000);
    let mut fraction = us % 1_000_000;
    let mut digits = 6;
    while digits > 3 && fraction.is_multiple_of(10) {
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

fn append_rendition_report(playlist: &mut String, report: &RenditionReport) {
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

fn valid_quoted_string_value(value: &str) -> bool {
    !value
        .bytes()
        .any(|byte| matches!(byte, b'"' | b'\n' | b'\r'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, SecondsFormat, TimeZone};

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
            part_target_us: 1_000_000,
            ..Options::default()
        });

        for _ in 0..16 {
            manifest.push_part_us(1_000_000, true, None, None);
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
            part_target_us: 1_000_000,
            ..Options::default()
        });

        for _ in 0..16 {
            manifest.push_part_us(1_000_000, true, None, None);
        }

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).expect("manifest utf8");
        assert!(!playlist.contains("CAN-SKIP-UNTIL"));
    }

    #[test]
    fn emitted_ll_hls_controls_are_internally_consistent() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 64,
            segment_min_ms: 1000,
            part_target_us: 1_000_000,
            ..Options::default()
        });

        for _ in 0..48 {
            manifest.push_part_us(1_000_000, true, None, None);
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
            part_target_us: 500_000,
            ..Options::default()
        });

        for duration_ms in [200_u64, 300, 100, 400, 250, 150] {
            manifest.push_part_us(duration_ms * 1_000, true, None, None);
            let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
            assert!(playlist.contains("#EXT-X-TARGETDURATION:6"));
        }
    }

    #[test]
    fn part_target_is_configured_and_stable() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 16,
            segment_min_ms: 200,
            part_target_us: 500_000,
            ..Options::default()
        });

        for duration_ms in [200_u64, 300, 100, 400, 250, 150] {
            manifest.push_part_us(duration_ms * 1_000, true, None, None);
            let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
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
        manifest.push_part_us(500_000, true, None, None);

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
            part_target_us: 250_000,
            ..Options::default()
        });

        manifest.push_part_us(250_000, true, None, None);
        manifest.push_part_us(250_000, false, None, None);

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
            part_target_us: 100_000,
            ..Options::default()
        });

        manifest.push_part_with_byte_len_us(100_000, true, 120);
        manifest.push_part_with_byte_len_us(100_000, false, 80);
        manifest.push_part_with_byte_len_us(100_000, true, 40);

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
        // A segment is closed before 1.5 s (TARGETDURATION 1): 600 parts of
        // 2.5 ms, a quarter more and 8.
        assert_eq!(options.segment_limit_us(), 1_500_000);
        assert_eq!(options.max_parts_per_segment, 758);
        // 12 s of 1 s segments and the open one.
        assert_eq!(options.max_segments, 13);
        assert_eq!(options.normalized().max_parts_per_segment, 758);
        assert_eq!(options.normalized().part_target_us(), 2_500);

        let quarter_ms = Options {
            part_target_us: 250,
            ..Options::default()
        }
        .normalized();
        assert_eq!(quarter_ms.max_parts_per_segment, 6_000 + 1_500 + 8);

        // The target duration covers the minimum segment.
        let long_segments = Options {
            segment_min_ms: 1_500,
            ..Options::default()
        }
        .normalized();
        assert_eq!(long_segments.target_duration_ms, 1_500);
        assert_eq!(long_segments.target_duration_secs(), 2);
        assert_eq!(long_segments.normalized().target_duration_ms, 1_500);

        // Counts that are given are kept.
        let given = Options {
            max_segments: 32,
            max_parts_per_segment: 128,
            ..Options::default()
        }
        .normalized();
        assert_eq!((given.max_segments, given.max_parts_per_segment), (32, 128));

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

    /// Frames of `fps_num / fps_den` frames a second with a key frame every
    /// `gop` frames, for `frames` frames, into a playlist of default options
    /// but the part target.
    fn video_with_default_options(
        fps_num: u64,
        fps_den: u64,
        gop: u64,
        frames: u64,
        part_target_us: u32,
    ) -> String {
        let mut manifest = M3u8Manifest::new(Options {
            part_target_us,
            ..Options::default()
        });
        let mut elapsed = 0_u64;
        for frame in 0..frames {
            let end = (frame + 1) * 1_000_000 * fps_den / fps_num;
            manifest.push_part_us(end - elapsed, frame % gop == 0, None, None);
            elapsed = end;
        }
        String::from_utf8(manifest.m3u8().to_vec()).unwrap()
    }

    #[test]
    fn a_gop_longer_than_the_target_duration_keeps_the_playlist_valid() {
        // 60 fps, a key frame every 2 s, default options (TARGETDURATION 1).
        let playlist = video_with_default_options(60, 1, 120, 600, 16_667);
        assert!(playlist.contains("#EXT-X-TARGETDURATION:1\n"));
        assert_valid_ll_hls(&playlist);
        // A segment that a key frame does not close is closed before its
        // EXTINF rounds above 1: at 89 frames, 1.483333 s.
        assert!(playlist.contains("#EXTINF:1.483333,\ns1.mp4"), "{playlist}");

        // 23.976 fps, a key frame every 48 frames (2.002 s).
        let playlist = video_with_default_options(24_000, 1_001, 48, 480, 41_709);
        assert_valid_ll_hls(&playlist);

        // With the target duration set to the GOP, every segment starts on
        // a key frame, as before.
        let mut manifest = M3u8Manifest::new(Options {
            part_target_us: 16_667,
            target_duration_ms: 2_000,
            ..Options::default()
        });
        let mut elapsed = 0_u64;
        for frame in 0..600_u64 {
            let end = (frame + 1) * 1_000_000 / 60;
            manifest.push_part_us(end - elapsed, frame % 120 == 0, None, None);
            elapsed = end;
        }
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("#EXTINF:2.000,\ns1.mp4"));
        assert_valid_ll_hls(&playlist);
    }

    #[test]
    fn a_minimum_segment_longer_than_the_target_raises_the_target() {
        let mut manifest = M3u8Manifest::new(Options {
            segment_min_ms: 1_500,
            ..Options::default()
        });
        for _ in 0..2_000 {
            manifest.push_part_us(2_500, true, None, None);
        }
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("#EXT-X-TARGETDURATION:2\n"));
        assert!(playlist.contains("#EXTINF:1.500,\ns1.mp4"));
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

    #[test]
    fn render_bytes_are_exact() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 10,
            segment_min_ms: 200,
            target_duration_ms: 1_000,
            part_target_us: 100_000,
            ..Options::default()
        });
        manifest.start_time = Utc
            .with_ymd_and_hms(2026, 8, 22, 12, 34, 56)
            .single()
            .expect("valid timestamp")
            + Duration::milliseconds(789);
        manifest.set_rendition_reports(vec![RenditionReport::new("peer/stream.m3u8")
            .with_last_msn(8)
            .with_last_part(2)]);

        manifest.push_part_with_byte_len_us(100_000, true, 120);
        let first = manifest.m3u8();
        let first_bytes = first.to_vec();
        manifest.push_part_with_byte_len_us(100_000, false, 80);
        manifest.push_part_with_byte_len_us(100_000, true, 40);
        let pieces = manifest.pieces();
        let len = pieces.len();
        let rendered = pieces.into_bytes();
        let again = manifest.m3u8();

        assert_eq!(first.as_ref(), first_bytes);
        assert_eq!(rendered.len(), len);
        assert_eq!(rendered, again);
        assert_eq!(
            rendered.as_ref(),
            b"#EXTM3U\n\
#EXT-X-VERSION:9\n\
#EXT-X-TARGETDURATION:1\n\
#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=3.000,PART-HOLD-BACK=0.300\n\
#EXT-X-PART-INF:PART-TARGET=0.100\n\
#EXT-X-MEDIA-SEQUENCE:1\n\
#EXT-X-MAP:URI=\"init.mp4\"\n\
#EXT-X-PROGRAM-DATE-TIME:2026-08-22T12:34:56.789Z\n\
#EXT-X-PART:DURATION=0.100,URI=\"s1.mp4\",BYTERANGE=\"120@0\",INDEPENDENT=YES\n\
#EXT-X-PART:DURATION=0.100,URI=\"s1.mp4\",BYTERANGE=\"80@120\"\n\
#EXTINF:0.200,\n\
#EXT-X-BYTERANGE:200@0\n\
s1.mp4\n\
#EXT-X-PROGRAM-DATE-TIME:2026-08-22T12:34:56.989Z\n\
#EXT-X-PART:DURATION=0.100,URI=\"s2.mp4\",BYTERANGE=\"40@0\",INDEPENDENT=YES\n\
#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"s2.mp4\",BYTERANGE-START=40\n\
#EXT-X-RENDITION-REPORT:URI=\"peer/stream.m3u8\",LAST-MSN=8,LAST-PART=2\n"
        );
    }

    #[test]
    fn rendered_pieces_know_their_exact_length() {
        // One-second segments of one part, so that the media sequence
        // crosses digit boundaries and CAN-SKIP-UNTIL comes and stays.
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 8,
            segment_min_ms: 1_000,
            part_target_us: 1_000_000,
            ..Options::default()
        });
        let empty = manifest.pieces();
        assert_eq!(empty.len(), manifest.m3u8().len());
        let mut skipped = false;
        for _ in 0..1_005 {
            manifest.push_part_us(1_000_000, true, None, None);
            let pieces = manifest.pieces();
            let len = pieces.len();
            let playlist = pieces.into_bytes();
            assert_eq!(playlist.len(), len);
            skipped |= playlist
                .windows(b"CAN-SKIP-UNTIL".len())
                .any(|window| window == b"CAN-SKIP-UNTIL");
        }
        assert!(skipped);
        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:998\n#EXT-X-MAP:URI=\"init.mp4\"\n"));
        assert!(playlist.contains("CAN-SKIP-UNTIL=6.000\n#EXT-X-PART-INF:PART-TARGET=1.000\n"));
    }

    #[test]
    fn rendition_reports_are_capped_in_count_and_uri_length() {
        let mut manifest = M3u8Manifest::new(Options::default());
        let mut reports = vec![
            RenditionReport::new("a".repeat(MAX_RENDITION_URI_BYTES + 1)),
            RenditionReport::new("bad\"uri.m3u8"),
            RenditionReport::new("b".repeat(MAX_RENDITION_URI_BYTES)),
        ];
        reports.extend((0..300).map(|index| RenditionReport::new(format!("r{index}.m3u8"))));
        let version = manifest.version();
        manifest.set_rendition_reports(reports);
        assert_eq!(manifest.version(), version + 1);

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        let lines: Vec<_> = playlist
            .lines()
            .filter(|line| line.starts_with("#EXT-X-RENDITION-REPORT:"))
            .collect();
        assert_eq!(lines.len(), MAX_RENDITION_REPORTS);
        assert_eq!(
            lines[0],
            format!(
                "#EXT-X-RENDITION-REPORT:URI=\"{}\"",
                "b".repeat(MAX_RENDITION_URI_BYTES)
            )
        );
        assert_eq!(lines[1], "#EXT-X-RENDITION-REPORT:URI=\"r0.m3u8\"");
        assert_eq!(
            lines[MAX_RENDITION_REPORTS - 1],
            "#EXT-X-RENDITION-REPORT:URI=\"r254.m3u8\""
        );
        assert!(!playlist.contains("bad"));
        assert!(!playlist.contains(&"a".repeat(MAX_RENDITION_URI_BYTES)));

        manifest.set_rendition_reports(Vec::new());
        assert!(!String::from_utf8(manifest.m3u8().to_vec())
            .unwrap()
            .contains("#EXT-X-RENDITION-REPORT"));
    }

    #[test]
    fn direct_program_date_time_format_matches_chrono_rfc3339() {
        let regular = Utc
            .with_ymd_and_hms(2026, 8, 22, 12, 34, 56)
            .single()
            .expect("valid timestamp")
            + Duration::nanoseconds(789_654_321);
        let extended_year = NaiveDate::from_ymd_opt(10_000, 1, 2)
            .expect("valid extended year")
            .and_hms_milli_opt(3, 4, 5, 6)
            .expect("valid time")
            .and_utc();
        let leap_second = NaiveDate::from_ymd_opt(2016, 12, 31)
            .expect("valid leap-second date")
            .and_hms_nano_opt(23, 59, 59, 1_500_000_000)
            .expect("valid leap second")
            .and_utc();

        for date_time in [regular, extended_year, leap_second] {
            let mut actual = String::new();
            append_program_date_time(&mut actual, date_time);
            assert_eq!(
                actual,
                format!(
                    "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                    date_time.to_rfc3339_opts(SecondsFormat::Millis, true)
                )
            );
            assert!(actual.len() <= PROGRAM_DATE_TIME_LINE_BYTES);
        }
    }

    #[test]
    fn long_running_manifest_keeps_only_the_retained_timeline() {
        let options = Options {
            max_segments: 4,
            segment_min_ms: 1000,
            target_duration_ms: 1000,
            part_target_us: 1_000_000,
            ..Options::default()
        };
        let mut manifest = M3u8Manifest::new(options);

        for _ in 0..20_000 {
            manifest.push_part_us(1_000_000, true, None, None);
        }

        assert_eq!(manifest.closed.len(), 3);
        assert_eq!(manifest.closed.front().unwrap().id, 19_997);
        assert_eq!(manifest.closed.back().unwrap().id, 19_999);
        assert_eq!(manifest.open.start_us, 19_999_000_000);

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:19997"));
        assert!(playlist.contains("s19997.mp4"));
        assert!(!playlist.contains("s19996.mp4"));
        let open_date = manifest.start_time + Duration::seconds(19_999);
        assert!(playlist.contains(&format!(
            "#EXT-X-PROGRAM-DATE-TIME:{}\n#EXT-X-PART:DURATION=1.000,URI=\"p20000.mp4\"",
            open_date.to_rfc3339_opts(SecondsFormat::Millis, true)
        )));
        assert!(playlist.len() < 16 * 1024);
    }
}
