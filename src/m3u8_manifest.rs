use crate::Options;

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};

const GAP_DURATION_MS: u32 = 4_000;

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
    sequence: u32,
    duration_ms: u32,
    independent: bool,
    byte_range: Option<MediaByteRange>,
}

pub struct M3u8Manifest {
    dur: u32,
    seq: u32,
    seg_dur: u32,
    seg_byte_len: u64,
    seg_id: u32,
    seg_durs: Vec<u32>,
    seg_parts: Vec<Vec<PartInfo>>,
    part_target_ms: u32,
    start_time: DateTime<Utc>,
    idx: u32,
    options: Options,
}

impl M3u8Manifest {
    pub fn new(options: Options) -> Self {
        let options = options.normalized();
        let seg_parts_size = options.max_segments;
        let mut seg_parts = Vec::with_capacity(seg_parts_size);
        for _ in 0..seg_parts_size {
            seg_parts.push(Vec::new());
        }

        Self {
            dur: 0,
            seq: 0,
            seg_dur: 0,
            seg_byte_len: 0,
            seg_id: 1,
            seg_durs: Vec::new(),
            seg_parts,
            part_target_ms: 0,
            start_time: Utc::now(),
            idx: 0,
            options,
        }
    }

    fn retained_segment_limit(&self) -> u32 {
        self.options
            .max_segments
            .saturating_sub(1)
            .min(u32::MAX as usize) as u32
    }

    fn full_segments(&self) -> Vec<(u32, u32)> {
        let retained_segments = self.retained_segment_limit();
        if retained_segments == 0 {
            return Vec::new();
        }

        let start = if self.seg_id <= retained_segments {
            1
        } else {
            self.seg_id - retained_segments
        };

        let len = self.seg_id - start;
        let mut res = Vec::with_capacity(len as usize);

        for i in start..self.seg_id {
            res.push((i, self.seg_durs[(i - 1) as usize]));
        }

        res
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
        if key && (self.seg_dur) >= self.options.segment_min_ms {
            self.seg_durs.push(self.seg_dur);
            self.seg_id += 1;
            self.seg_dur = 0;
            self.seg_byte_len = 0;
            self.idx = 0;

            let seg_index = self.seg_id as usize % self.options.max_segments;
            self.seg_parts[seg_index].clear();
            new_seg = true;
        }
        let idx = self.idx;
        self.idx += 1;
        self.seq += 1;
        self.dur += duration;
        self.seg_dur += duration;
        self.part_target_ms = self.part_target_ms.max(duration);
        let byte_range = byte_range.or_else(|| MediaByteRange::new(byte_len?, self.seg_byte_len));
        if let Some(range) = byte_range {
            self.seg_byte_len = self
                .seg_byte_len
                .max(range.offset.saturating_add(range.length));
        }
        let seg_index = self.seg_id as usize % self.options.max_segments;

        self.seg_parts[seg_index].push(PartInfo {
            sequence: self.seq,
            duration_ms: duration,
            independent: key,
            byte_range,
        });

        (
            self.m3u8(),
            self.seg_id as usize,
            self.seq as usize,
            idx as usize,
            new_seg,
        )
    }

    pub fn m3u8(&self) -> Bytes {
        let mut ph = String::new();
        let mut ps = String::new();

        let mut pt = self.start_time;

        let segs = self.full_segments();

        let mut gaps = 0;
        let mut segment_durations = Vec::new();
        if segs.len() < 7 {
            for _ in 0..(7 - segs.len()) {
                gaps += 1;
                let secs = GAP_DURATION_MS as f64 / 1000.0;
                ps.push_str(&format!("#EXT-X-GAP\n#EXTINF:{secs:.5},\ngap.mp4\n"));
                pt += Duration::milliseconds(GAP_DURATION_MS as i64);
                segment_durations.push(GAP_DURATION_MS);
            }
        }

        let mut durs = Vec::new();

        for (i, seg) in segs.iter().enumerate() {
            let segment_parts = &self.seg_parts[seg.0 as usize % self.options.max_segments];
            if gaps + i <= 4 {
                let secs = seg.1 as f64 / 1000.0;
                ps.push_str(&format!("#EXTINF:{secs:.5},\n"));
                append_segment_byte_range(&mut ps, segment_parts);
                ps.push_str(&format!("s{}.mp4\n", seg.0));
                pt += Duration::milliseconds(seg.1 as i64);
                segment_durations.push(seg.1);
            } else {
                ps.push_str(&format!(
                    "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                    pt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                ));
                for p in segment_parts {
                    durs.push(p.duration_ms);
                    append_part_line(&mut ps, seg.0, p);
                }
                let secs = seg.1 as f64 / 1000.0;
                ps.push_str(&format!("#EXTINF:{secs:.5},\n"));
                append_segment_byte_range(&mut ps, segment_parts);
                ps.push_str(&format!("s{}.mp4\n", seg.0));
                pt += Duration::milliseconds(seg.1 as i64);
                segment_durations.push(seg.1);
            }
        }

        let mut open_parent_duration_ms = 0_u32;
        let seg_index = self.seg_id as usize % self.options.max_segments;
        for p in &self.seg_parts[seg_index] {
            durs.push(p.duration_ms);
            open_parent_duration_ms = open_parent_duration_ms.saturating_add(p.duration_ms);
            append_part_line(&mut ps, self.seg_id, p);
        }
        append_preload_hint(&mut ps, self.seg_id, &self.seg_parts[seg_index]);

        let target_duration = segs
            .iter()
            .map(|(_, duration)| ms_to_target_duration(*duration))
            .max()
            .unwrap_or_else(|| ms_to_target_duration(self.options.segment_min_ms))
            .max(if gaps > 0 {
                ms_to_target_duration(GAP_DURATION_MS)
            } else {
                1
            })
            .max(1);

        let max_duration = if self.part_target_ms == 0 {
            self.options.segment_min_ms
        } else {
            self.part_target_ms
        }
        .max(1);
        let part_target = max_duration as f64 / 1000.0;

        let part_hold_back = part_target * 3_f64;
        let can_skip_until = target_duration * 6;
        let can_skip_until_ms = can_skip_until.saturating_mul(1000);
        let can_skip = segment_durations
            .first()
            .map(|first_duration| {
                let retained_ms: u32 = segment_durations
                    .iter()
                    .copied()
                    .fold(open_parent_duration_ms, u32::saturating_add);
                retained_ms.saturating_sub(*first_duration) > can_skip_until_ms
            })
            .unwrap_or(false);

        ph.push_str("#EXTM3U\n");
        ph.push_str("#EXT-X-VERSION:9\n");
        ph.push_str(&format!("#EXT-X-TARGETDURATION:{target_duration}\n"));

        if can_skip {
            ph.push_str(&format!(
                "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.5},CAN-SKIP-UNTIL={:.5}\n",
                part_hold_back, can_skip_until as f64
            ));
        } else {
            ph.push_str(&format!(
                "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.5}\n",
                part_hold_back
            ));
        }

        let mut seq = self.seg_id;
        if self.seg_id > 7 {
            seq = self.seg_id - 7
        }
        ph.push_str(&format!("#EXT-X-PART-INF:PART-TARGET={part_target:.5}\n"));
        ph.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{seq}\n"));
        ph.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n");

        format!("{ph}{ps}").into()
    }
}

fn append_part_line(playlist: &mut String, segment_id: u32, part: &PartInfo) {
    let secs = part.duration_ms as f64 / 1000.0;
    let mut line = if let Some(byte_range) = part.byte_range {
        format!(
            "#EXT-X-PART:DURATION={secs:.5},URI=\"s{segment_id}.mp4\",BYTERANGE=\"{}@{}\"",
            byte_range.length, byte_range.offset
        )
    } else {
        format!(
            "#EXT-X-PART:DURATION={secs:.5},URI=\"p{}.mp4\"",
            part.sequence
        )
    };
    if part.independent {
        line.push_str(",INDEPENDENT=YES");
    }
    line.push('\n');
    playlist.push_str(&line);
}

fn append_segment_byte_range(playlist: &mut String, parts: &[PartInfo]) {
    let Some((offset, length)) = segment_byte_range(parts) else {
        return;
    };
    if length > 0 {
        playlist.push_str(&format!("#EXT-X-BYTERANGE:{length}@{offset}\n"));
    }
}

fn append_preload_hint(playlist: &mut String, segment_id: u32, parts: &[PartInfo]) {
    let Some((offset, length)) = segment_byte_range(parts) else {
        return;
    };
    let start = offset.saturating_add(length);
    playlist.push_str(&format!(
        "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"s{segment_id}.mp4\",BYTERANGE-START={start}\n"
    ));
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

        assert!(manifest.contains("#EXT-X-TARGETDURATION:4"));
        assert!(manifest.contains("#EXT-X-PART-INF:PART-TARGET=1.50000"));
        assert!(manifest.contains("PART-HOLD-BACK=4.50000"));
        assert!(!manifest.contains("CAN-SKIP-UNTIL"));
        assert!(!manifest.contains("#EXT-X-PRELOAD-HINT"));
    }

    #[test]
    fn advertises_skip_only_when_window_can_skip() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 10,
            segment_min_ms: 1000,
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
            max_segments: 10,
            segment_min_ms: 1000,
            ..Options::default()
        });

        for _ in 0..12 {
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
        let part_hold_back = attr_f64(server_control, "PART-HOLD-BACK");
        let can_skip_until = attr_f64(server_control, "CAN-SKIP-UNTIL");

        assert!(version >= 9);
        assert!(playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""));
        assert!(server_control.contains("CAN-BLOCK-RELOAD=YES"));
        assert!(part_hold_back >= part_target * 3.0);
        assert!(can_skip_until >= target_duration * 6.0);
        assert!(!server_control.contains("CAN-SKIP-DATERANGES"));
        assert!(!playlist.contains("#EXT-X-PRELOAD-HINT"));

        for line in playlist
            .lines()
            .filter(|line| line.starts_with("#EXT-X-PART:"))
        {
            assert!(line.contains("DURATION="));
            assert!(line.contains("URI=\"p"));
        }
    }

    #[test]
    fn emits_raw_media_byte_ranges_when_part_sizes_are_known() {
        let mut manifest = M3u8Manifest::new(Options {
            max_segments: 10,
            segment_min_ms: 200,
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
}
