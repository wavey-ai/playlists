use crate::Options;

use bytes::Bytes;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use std::collections::VecDeque;
use std::fmt::Write;

const MAX_RENDITION_REPORTS: usize = 256;
const MAX_RENDITION_URI_BYTES: usize = 2_048;

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
    sequence: usize,
    duration_ms: u32,
    independent: bool,
    byte_range: Option<MediaByteRange>,
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

pub struct M3u8Manifest {
    seq: usize,
    seg_dur: u64,
    seg_byte_len: u64,
    seg_id: usize,
    completed_segments: VecDeque<CompletedSegment>,
    seg_parts: Vec<Vec<PartInfo>>,
    open_start_time: DateTime<Utc>,
    idx: usize,
    options: Options,
    rendition_reports: Vec<RenditionReport>,
    render_headers: RenderHeaders,
    render_buffer: String,
}

struct RenderHeaders {
    without_skip: String,
    with_skip: String,
    can_skip_until_ms: u32,
}

#[derive(Clone, Copy, Debug)]
struct CompletedSegment {
    id: usize,
    duration_ms: u64,
    start_time: DateTime<Utc>,
}

impl M3u8Manifest {
    pub fn new(options: Options) -> Self {
        let options = options.normalized();
        let render_headers = RenderHeaders::new(options);
        let seg_parts_size = options.max_segments;
        let mut seg_parts = Vec::with_capacity(seg_parts_size);
        for _ in 0..seg_parts_size {
            seg_parts.push(Vec::new());
        }

        Self {
            seq: 0,
            seg_dur: 0,
            seg_byte_len: 0,
            seg_id: 1,
            completed_segments: VecDeque::with_capacity(options.max_segments.saturating_sub(1)),
            seg_parts,
            open_start_time: Utc::now(),
            idx: 0,
            options,
            rendition_reports: Vec::new(),
            render_headers,
            render_buffer: String::new(),
        }
    }

    pub fn set_rendition_reports(&mut self, reports: Vec<RenditionReport>) {
        self.rendition_reports = reports
            .into_iter()
            .filter(|report| report.uri.len() <= MAX_RENDITION_URI_BYTES)
            .take(MAX_RENDITION_REPORTS)
            .collect();
    }

    fn retained_segment_limit(&self) -> usize {
        self.options.max_segments.saturating_sub(1)
    }

    fn advance_open_start_time(&mut self, duration_ms: u64) {
        let duration_ms = i64::try_from(duration_ms).unwrap_or(i64::MAX);
        if let Some(next) = self
            .open_start_time
            .checked_add_signed(Duration::milliseconds(duration_ms))
        {
            self.open_start_time = next;
        }
    }

    pub fn add_part(&mut self, duration: u32, key: bool) -> (Bytes, usize, usize, usize, bool) {
        self.add_part_with_byte_range(duration, key, None)
    }

    pub fn add_part_with_byte_len(
        &mut self,
        duration: u32,
        key: bool,
        byte_len: usize,
    ) -> (Bytes, usize, usize, usize, bool) {
        self.add_part_inner(duration, key, None, Some(byte_len as u64))
    }

    pub fn add_part_with_byte_range(
        &mut self,
        duration: u32,
        key: bool,
        byte_range: Option<MediaByteRange>,
    ) -> (Bytes, usize, usize, usize, bool) {
        self.add_part_inner(duration, key, byte_range, None)
    }

    fn add_part_inner(
        &mut self,
        duration: u32,
        key: bool,
        byte_range: Option<MediaByteRange>,
        byte_len: Option<u64>,
    ) -> (Bytes, usize, usize, usize, bool) {
        let mut new_seg = false;
        let open_segment_full = self.seg_parts[self.seg_id % self.options.max_segments].len()
            >= self.options.max_parts_per_segment;
        if (key && self.seg_dur >= u64::from(self.options.segment_min_ms)) || open_segment_full {
            self.completed_segments.push_back(CompletedSegment {
                id: self.seg_id,
                duration_ms: self.seg_dur,
                start_time: self.open_start_time,
            });
            while self.completed_segments.len() > self.retained_segment_limit() {
                self.completed_segments.pop_front();
            }
            self.advance_open_start_time(self.seg_dur);
            self.seg_id = self.seg_id.saturating_add(1);
            self.seg_dur = 0;
            self.seg_byte_len = 0;
            self.idx = 0;

            let seg_index = self.seg_id % self.options.max_segments;
            self.seg_parts[seg_index].clear();
            new_seg = true;
        }
        let idx = self.idx;
        self.idx = self.idx.saturating_add(1);
        self.seq = self.seq.saturating_add(1);
        self.seg_dur = self.seg_dur.saturating_add(u64::from(duration));
        let byte_range = byte_range.or_else(|| MediaByteRange::new(byte_len?, self.seg_byte_len));
        if let Some(range) = byte_range {
            self.seg_byte_len = self
                .seg_byte_len
                .max(range.offset.saturating_add(range.length));
        }
        let seg_index = self.seg_id % self.options.max_segments;

        self.seg_parts[seg_index].push(PartInfo {
            sequence: self.seq,
            duration_ms: duration,
            independent: key,
            byte_range,
        });

        (
            self.render_reusing_buffer(),
            self.seg_id,
            self.seq,
            idx,
            new_seg,
        )
    }

    pub fn m3u8(&self) -> Bytes {
        let mut playlist = String::with_capacity(self.render_buffer.capacity());
        self.render_into(&mut playlist);
        playlist.into()
    }

    fn render_reusing_buffer(&mut self) -> Bytes {
        let mut playlist = std::mem::take(&mut self.render_buffer);
        playlist.clear();
        self.render_into(&mut playlist);
        let rendered = Bytes::copy_from_slice(playlist.as_bytes());
        self.render_buffer = playlist;
        rendered
    }

    fn render_into(&self, playlist: &mut String) {
        let can_skip = self
            .completed_segments
            .front()
            .map(|first_segment| {
                let retained_ms = self
                    .completed_segments
                    .iter()
                    .map(|segment| segment.duration_ms)
                    .fold(self.seg_dur, u64::saturating_add);
                retained_ms.saturating_sub(first_segment.duration_ms)
                    > u64::from(self.render_headers.can_skip_until_ms)
            })
            .unwrap_or(false);
        playlist.push_str(if can_skip {
            &self.render_headers.with_skip
        } else {
            &self.render_headers.without_skip
        });

        let seq = self
            .completed_segments
            .front()
            .map(|segment| segment.id)
            .unwrap_or(self.seg_id);
        writeln!(playlist, "#EXT-X-MEDIA-SEQUENCE:{seq}").expect("writing to a String cannot fail");
        playlist.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n");

        let mut pt = self
            .completed_segments
            .front()
            .map_or(self.open_start_time, |segment| segment.start_time);

        for segment in &self.completed_segments {
            let segment_parts = &self.seg_parts[segment.id % self.options.max_segments];
            append_program_date_time(playlist, pt);
            for p in segment_parts {
                append_part_line(playlist, segment.id, p);
            }
            playlist.push_str("#EXTINF:");
            append_milliseconds(playlist, segment.duration_ms);
            playlist.push_str(",\n");
            append_segment_byte_range(playlist, segment_parts);
            writeln!(playlist, "s{}.mp4", segment.id).expect("writing to a String cannot fail");
            let duration_ms = i64::try_from(segment.duration_ms).unwrap_or(i64::MAX);
            if let Some(next) = pt.checked_add_signed(Duration::milliseconds(duration_ms)) {
                pt = next;
            }
        }

        let seg_index = self.seg_id % self.options.max_segments;
        let open_parts = &self.seg_parts[seg_index];
        if !open_parts.is_empty() {
            append_program_date_time(playlist, pt);
            for p in open_parts {
                append_part_line(playlist, self.seg_id, p);
            }
        }
        append_preload_hint(playlist, self.seg_id, open_parts);
        append_rendition_reports(playlist, &self.rendition_reports);
    }
}

impl RenderHeaders {
    fn new(options: Options) -> Self {
        let target_duration = ms_to_target_duration(options.target_duration_ms);
        let can_skip_until = target_duration * 6;
        let can_skip_until_ms = can_skip_until.saturating_mul(1000);
        Self {
            without_skip: render_header(options, target_duration, None),
            with_skip: render_header(options, target_duration, Some(can_skip_until)),
            can_skip_until_ms,
        }
    }
}

fn render_header(options: Options, target_duration: u32, can_skip_until: Option<u32>) -> String {
    let mut header = String::with_capacity(256);
    header.push_str("#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:");
    writeln!(header, "{target_duration}").expect("writing to a String cannot fail");
    header.push_str("#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=");
    append_milliseconds(&mut header, u64::from(target_duration) * 3_000);
    header.push_str(",PART-HOLD-BACK=");
    append_milliseconds(&mut header, u64::from(options.part_target_ms) * 3);
    if let Some(can_skip_until) = can_skip_until {
        header.push_str(",CAN-SKIP-UNTIL=");
        append_milliseconds(&mut header, u64::from(can_skip_until) * 1_000);
    }
    header.push_str("\n#EXT-X-PART-INF:PART-TARGET=");
    append_milliseconds(&mut header, u64::from(options.part_target_ms));
    header.push('\n');
    header
}

fn append_program_date_time(playlist: &mut String, date_time: DateTime<Utc>) {
    playlist.push_str("#EXT-X-PROGRAM-DATE-TIME:");
    let year = date_time.year();
    if (0..=9_999).contains(&year) {
        write!(playlist, "{year:04}").expect("writing to a String cannot fail");
    } else {
        write!(playlist, "{year:+05}").expect("writing to a String cannot fail");
    }

    let mut second = date_time.second();
    let mut nanosecond = date_time.nanosecond();
    if nanosecond >= 1_000_000_000 {
        second += 1;
        nanosecond -= 1_000_000_000;
    }
    writeln!(
        playlist,
        "-{:02}-{:02}T{:02}:{:02}:{second:02}.{:03}Z",
        date_time.month(),
        date_time.day(),
        date_time.hour(),
        date_time.minute(),
        nanosecond / 1_000_000
    )
    .expect("writing to a String cannot fail");
}

fn append_milliseconds(playlist: &mut String, duration_ms: u64) {
    let seconds = duration_ms / 1_000;
    let fractional = duration_ms % 1_000 * 100;
    write!(playlist, "{seconds}.{fractional:05}").expect("writing to a String cannot fail");
}

fn append_part_line(playlist: &mut String, segment_id: usize, part: &PartInfo) {
    playlist.push_str("#EXT-X-PART:DURATION=");
    append_milliseconds(playlist, u64::from(part.duration_ms));
    if let Some(byte_range) = part.byte_range {
        write!(
            playlist,
            ",URI=\"s{segment_id}.mp4\",BYTERANGE=\"{}@{}\"",
            byte_range.length, byte_range.offset
        )
        .expect("writing to a String cannot fail");
    } else {
        write!(playlist, ",URI=\"p{}.mp4\"", part.sequence)
            .expect("writing to a String cannot fail");
    }
    if part.independent {
        playlist.push_str(",INDEPENDENT=YES");
    }
    playlist.push('\n');
}

fn append_segment_byte_range(playlist: &mut String, parts: &[PartInfo]) {
    let Some((offset, length)) = segment_byte_range(parts) else {
        return;
    };
    if length > 0 {
        writeln!(playlist, "#EXT-X-BYTERANGE:{length}@{offset}")
            .expect("writing to a String cannot fail");
    }
}

fn append_preload_hint(playlist: &mut String, segment_id: usize, parts: &[PartInfo]) {
    let Some(last_part) = parts.last() else {
        return;
    };
    if let Some((offset, length)) = segment_byte_range(parts) {
        let start = offset.saturating_add(length);
        writeln!(
            playlist,
            "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"s{segment_id}.mp4\",BYTERANGE-START={start}"
        )
        .expect("writing to a String cannot fail");
    } else {
        let next_part_sequence = last_part.sequence.saturating_add(1);
        writeln!(
            playlist,
            "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"p{next_part_sequence}.mp4\""
        )
        .expect("writing to a String cannot fail");
    }
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
            write!(playlist, ",LAST-MSN={last_msn}").expect("writing to a String cannot fail");
        }
        if let Some(last_part) = report.last_part {
            write!(playlist, ",LAST-PART={last_part}").expect("writing to a String cannot fail");
        }
        playlist.push('\n');
    }
}

fn valid_quoted_string_value(value: &str) -> bool {
    !value
        .bytes()
        .any(|byte| matches!(byte, b'"' | b'\n' | b'\r'))
}

fn segment_byte_range(parts: &[PartInfo]) -> Option<(u64, u64)> {
    let first = parts.first()?.byte_range?;
    let end = parts.iter().try_fold(first.offset, |_, part| {
        let range = part.byte_range?;
        Some(range.offset.saturating_add(range.length))
    })?;
    Some((first.offset, end.saturating_sub(first.offset)))
}

fn ms_to_target_duration(ms: u32) -> u32 {
    u64::from(ms).div_ceil(1000).max(1) as u32
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

        assert!(manifest.contains("#EXT-X-TARGETDURATION:6"));
        assert!(manifest.contains("#EXT-X-PART-INF:PART-TARGET=0.50000"));
        assert!(manifest.contains("HOLD-BACK=18.00000"));
        assert!(manifest.contains("PART-HOLD-BACK=1.50000"));
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
        assert!(playlist.contains("CAN-SKIP-UNTIL=6.00000"));
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
            assert!(playlist.contains("#EXT-X-PART-INF:PART-TARGET=0.50000"));
            assert!(playlist.contains("PART-HOLD-BACK=1.50000"));
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

        assert!(playlist.contains("#EXT-X-PART:DURATION=0.25000,URI=\"p1.mp4\",INDEPENDENT=YES"));
        assert!(playlist.contains("#EXT-X-PART:DURATION=0.25000,URI=\"p2.mp4\""));
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

    #[test]
    fn render_bytes_remain_exact_across_buffer_reuse() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 10,
            segment_min_ms: 200,
            target_duration_ms: 1_000,
            part_target_ms: 100,
            ..Options::default()
        });
        manifest.open_start_time = Utc
            .with_ymd_and_hms(2026, 8, 22, 12, 34, 56)
            .single()
            .expect("valid timestamp")
            + Duration::milliseconds(789);
        manifest.set_rendition_reports(vec![RenditionReport::new("peer/stream.m3u8")
            .with_last_msn(8)
            .with_last_part(2)]);

        let first = manifest.add_part_with_byte_len(100, true, 120).0;
        let first_bytes = first.to_vec();
        manifest.add_part_with_byte_len(100, false, 80);
        let rendered = manifest.add_part_with_byte_len(100, true, 40).0;

        assert_eq!(first.as_ref(), first_bytes);
        assert_eq!(
            rendered.as_ref(),
            b"#EXTM3U\n\
#EXT-X-VERSION:9\n\
#EXT-X-TARGETDURATION:1\n\
#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,HOLD-BACK=3.00000,PART-HOLD-BACK=0.30000\n\
#EXT-X-PART-INF:PART-TARGET=0.10000\n\
#EXT-X-MEDIA-SEQUENCE:1\n\
#EXT-X-MAP:URI=\"init.mp4\"\n\
#EXT-X-PROGRAM-DATE-TIME:2026-08-22T12:34:56.789Z\n\
#EXT-X-PART:DURATION=0.10000,URI=\"s1.mp4\",BYTERANGE=\"120@0\",INDEPENDENT=YES\n\
#EXT-X-PART:DURATION=0.10000,URI=\"s1.mp4\",BYTERANGE=\"80@120\"\n\
#EXTINF:0.20000,\n\
#EXT-X-BYTERANGE:200@0\n\
s1.mp4\n\
#EXT-X-PROGRAM-DATE-TIME:2026-08-22T12:34:56.989Z\n\
#EXT-X-PART:DURATION=0.10000,URI=\"s2.mp4\",BYTERANGE=\"40@0\",INDEPENDENT=YES\n\
#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"s2.mp4\",BYTERANGE-START=40\n\
#EXT-X-RENDITION-REPORT:URI=\"peer/stream.m3u8\",LAST-MSN=8,LAST-PART=2\n"
        );
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
        }
    }

    #[test]
    fn long_running_manifest_keeps_only_the_retained_timeline() {
        let options = Options {
            max_segments: 4,
            segment_min_ms: 1000,
            target_duration_ms: 1000,
            part_target_ms: 1000,
            ..Options::default()
        };
        let mut manifest = M3u8Manifest::new(options);
        let started = manifest.open_start_time;

        for _ in 0..20_000 {
            manifest.add_part(1000, true);
        }

        assert_eq!(manifest.completed_segments.len(), 3);
        assert_eq!(manifest.completed_segments.front().unwrap().id, 19_997);
        assert_eq!(manifest.completed_segments.back().unwrap().id, 19_999);
        assert_eq!(
            manifest
                .open_start_time
                .signed_duration_since(started)
                .num_milliseconds(),
            19_999_000
        );

        let playlist = String::from_utf8(manifest.m3u8().to_vec()).unwrap();
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:19997"));
        assert!(playlist.contains("s19997.mp4"));
        assert!(!playlist.contains("s19996.mp4"));
        assert!(playlist.len() < 16 * 1024);
    }

    #[test]
    fn non_independent_input_cannot_grow_an_open_segment_without_bound() {
        let options = Options {
            max_segments: 4,
            max_parts_per_segment: 4,
            segment_min_ms: u32::MAX,
            ..Options::default()
        };
        let mut manifest = M3u8Manifest::new(options);

        for _ in 0..10_000 {
            manifest.add_part(1, false);
        }

        assert!(manifest
            .seg_parts
            .iter()
            .all(|parts| parts.len() <= options.max_parts_per_segment));
        assert_eq!(manifest.completed_segments.len(), 3);
        assert!(manifest.m3u8().len() < 16 * 1024);
    }
}
