use crate::Options;

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};

pub struct M3u8Manifest {
    dur: u32,
    seq: u32,
    seg_dur: u32,
    seg_id: u32,
    seg_durs: Vec<u32>,
    seg_parts: Vec<Vec<(u32, u32, bool)>>,
    start_time: DateTime<Utc>,
    idx: u32,
    options: Options,
}

impl M3u8Manifest {
    pub fn new(options: Options) -> Self {
        let seg_parts_size = options.max_segments;
        let mut seg_parts = Vec::with_capacity(seg_parts_size);
        for _ in 0..seg_parts_size {
            seg_parts.push(Vec::new());
        }

        Self {
            dur: 0,
            seq: 0,
            seg_dur: 0,
            seg_id: 1,
            seg_durs: Vec::new(),
            seg_parts,
            start_time: Utc::now(),
            idx: 0,
            options,
        }
    }

    fn full_segments(&self) -> Vec<(u32, u32)> {
        let start = if self.seg_id <= 7 { 1 } else { self.seg_id - 7 };

        let len = self.seg_id - start;
        let mut res = Vec::with_capacity(len as usize);

        for i in start..self.seg_id {
            res.push((i, self.seg_durs[(i - 1) as usize]));
        }

        res
    }

    pub fn add_part(&mut self, duration: u32, key: bool) -> (Bytes, usize, usize, usize, bool) {
        let mut new_seg = false;
        dbg!(self.options.segment_min_ms);
        if key && (self.seg_dur) >= self.options.segment_min_ms {
            self.seg_durs.push(self.seg_dur);
            self.seg_id += 1;
            self.seg_dur = 0;
            self.idx = 0;

            let seg_index = (self.seg_id as usize % self.options.max_segments as usize) as usize;
            self.seg_parts[seg_index].clear();
            new_seg = true;
        }
        let idx = self.idx;
        self.idx += 1;
        self.seq += 1;
        self.dur += duration;
        self.seg_dur += duration;
        let seg_index = (self.seg_id as usize % self.options.max_segments as usize) as usize;

        self.seg_parts[seg_index].push((self.seq, duration, key));

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
        if segs.len() < 7 {
            for _ in 0..(7 - segs.len()) {
                gaps += 1;
                ps.push_str("#EXT-X-GAP\n#EXTINF:4.00000,\ngap.mp4\n");
                pt += Duration::milliseconds(1000);
            }
        }

        let mut durs = Vec::new();

        for (i, seg) in segs.iter().enumerate() {
            if gaps + i <= 4 {
                let secs = seg.1 as f64 / 1000.0;
                ps.push_str(&format!("#EXTINF:{:.5},\n", secs));
                ps.push_str(&format!("s{}.mp4\n", seg.0));
                pt += Duration::milliseconds(seg.1 as i64);
            } else {
                ps.push_str(&format!(
                    "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                    pt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                ));
                for p in &self.seg_parts[seg.0 as usize % self.options.max_segments as usize] {
                    durs.push(p.1);
                    let secs = p.1 as f64 / 1000.0;
                    let mut str = format!("#EXT-X-PART:DURATION={:.5},URI=\"p{}.mp4\"", secs, p.0);
                    if p.2 {
                        str += ",INDEPENDENT=YES\n"
                    } else {
                        str += "\n"
                    }
                    ps.push_str(&str);
                }
                let secs = seg.1 as f64 / 1000.0;
                ps.push_str(&format!("#EXTINF:{:.5},\n", secs));
                ps.push_str(&format!("s{}.mp4\n", seg.0));
                pt += Duration::milliseconds(seg.1 as i64);
            }
        }

        let mut id = 0;
        let seg_index = (self.seg_id as usize % self.options.max_segments as usize) as usize;
        for p in &self.seg_parts[seg_index] {
            durs.push(p.1);
            let secs = p.1 as f64 / 1000.0;
            let mut str = format!("#EXT-X-PART:DURATION={:.5},URI=\"p{}.mp4\"", secs, p.0);
            if p.2 {
                str += ",INDEPENDENT=YES\n"
            } else {
                str += "\n"
            }
            ps.push_str(&str);
            id = p.0;
        }

        ps.push_str(&format!(
            "#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"p{}.mp4\"\n",
            id + 1
        ));

        let target_duration = segs
            .iter()
            .map(|(_, duration)| (*duration as f64 / 1000.0).round() as u32)
            .max()
            .unwrap_or(0);

        let mut duration_counts = std::collections::HashMap::new();
        for parts in &self.seg_parts {
            for &(_, duration, _) in parts {
                *duration_counts.entry(duration).or_insert(0) += 1;
            }
        }

        let max_duration = durs.iter().max().cloned().unwrap_or(0);
        let part_target = max_duration as f64 / 1000.0;

        let part_hold_back = part_target * 30 as f64;
        let can_skip_until = target_duration * 6;

        ph.push_str("#EXTM3U\n");
        ph.push_str("#EXT-X-VERSION:9\n");
        ph.push_str(&format!("#EXT-X-TARGETDURATION:{}\n", target_duration));

        ph.push_str(&format!(
            "#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK={:.5},CAN-SKIP-UNTIL={:.5}\n",
            part_hold_back, can_skip_until as f64
        ));

        let mut seq = self.seg_id;
        if self.seg_id > 7 {
            seq = self.seg_id - 7
        }
        ph.push_str(&format!("#EXT-X-PART-INF:PART-TARGET={:.5}\n", part_target));
        ph.push_str(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", seq));
        ph.push_str("#EXT-X-MAP:URI=\"init.mp4\"\n");

        format!("{}{}", ph, ps).into()
    }
}
