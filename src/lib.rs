use access_unit::Fmp4;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Duration, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::BTreeMap;
use std::io::prelude::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    RwLock,
};
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

#[derive(Copy, Clone)]
pub struct Options {
    pub max_segments: usize,
    pub num_playlists: usize,
    pub max_parts_per_segment: usize,
    pub max_parted_segments: usize,
    pub segment_min_ms: u32,
    pub buffer_size_kb: usize,
    pub init_size_kb: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            max_segments: 32,
            num_playlists: 10,
            max_parts_per_segment: 128,
            max_parted_segments: 32,
            segment_min_ms: 1500,
            buffer_size_kb: 5,
            init_size_kb: 5,
        }
    }
}

pub struct Playlists {
    fmp4_cache: Arc<RingBuffer>,
    m3u8_cache: Arc<Store>,
    playlists: Mutex<BTreeMap<u64, Playlist>>,
    active: AtomicUsize,
}

impl Playlists {
    pub fn new(options: Options) -> (Arc<Self>, Arc<RingBuffer>, Arc<Store>) {
        let fmp4_cache = Arc::new(RingBuffer::new(options));
        let m3u8_cache = Arc::new(Store::new(options));

        (
            Arc::new(Self {
                fmp4_cache: Arc::clone(&fmp4_cache),
                m3u8_cache: Arc::clone(&m3u8_cache),
                playlists: Mutex::new(BTreeMap::new()),
                active: AtomicUsize::new(0),
            }),
            Arc::clone(&fmp4_cache),
            Arc::clone(&m3u8_cache),
        )
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub fn fin(&self, id: u64) {
        let mut playlists = self.playlists.lock().unwrap();
        if playlists.remove(&id).is_some() {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        self.m3u8_cache.zero_stream_id(id);
        self.fmp4_cache.zero_stream_id(id);
    }

    pub fn add(&self, stream_id: u64, fmp4: Fmp4) -> bool {
        let mut playlists = self.playlists.lock().unwrap();

        if !playlists.contains_key(&stream_id) {
            if self.active.load(Ordering::SeqCst) >= self.fmp4_cache.options.num_playlists {
                return false;
            }

            let n = self.active.fetch_add(1, Ordering::SeqCst);
            info!("PLAY:NEW active={}", n + 1);
        }

        let (m3u8, seg, seq, idx, new_seg) = {
            let playlist: &mut Playlist = playlists
                .entry(stream_id)
                .or_insert_with(|| Playlist::new(Options::default()));
            playlist.add_part(fmp4.duration, fmp4.key)
        };

        if new_seg {
            info!("PLAY:UP active={}", self.active());
        }

        if let Some(init) = fmp4.init {
            self.m3u8_cache.set_init(stream_id, init);
        }
        self.fmp4_cache.add(stream_id, seq as usize, fmp4.data);
        self.m3u8_cache.add(stream_id, seg, seq, idx, m3u8);

        true
    }
}

pub struct Playlist {
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

impl Playlist {
    pub fn new(options: Options) -> Playlist {
        let seg_parts_size = options.max_segments;
        let mut seg_parts = Vec::with_capacity(seg_parts_size);
        for _ in 0..seg_parts_size {
            seg_parts.push(Vec::new());
        }

        Playlist {
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

        let part_hold_back = part_target * 3 as f64;
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

const NUM_PLAYLISTS: usize = 10;
const MAX_SEGMENTS: usize = 32;
const MAX_PARTS_PER_SEGMENT: usize = 128;
const BUFFER_SIZE_KB: usize = 512;
const BUFFER_SIZE_BYTES: usize = BUFFER_SIZE_KB * 1024;

pub struct Store {
    buffer: Vec<RwLock<Bytes>>,
    seg_parts: Vec<AtomicUsize>,
    last_seg: Vec<AtomicUsize>,
    last_part: Vec<AtomicUsize>,
    inits: Vec<RwLock<Bytes>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    offset: AtomicUsize,
    options: Options,
}

impl Store {
    pub fn new(options: Options) -> Self {
        let buffer_size_bytes = options.buffer_size_kb * 1024;
        let init_size_bytes = options.init_size_kb * 1024;

        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);
        let init_repeat_value = Bytes::from(vec![0u8; init_size_bytes]);

        let buffer_size =
            options.num_playlists * options.max_parts_per_segment * options.max_parted_segments;
        let mut buffer = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            buffer.push(RwLock::new(buffer_repeat_value.clone()));
        }

        let seg_parts_size = options.max_segments * options.num_playlists;
        let mut seg_parts = Vec::with_capacity(seg_parts_size);
        for _ in 0..seg_parts_size {
            seg_parts.push(AtomicUsize::new(0));
        }

        let num_playlists = options.num_playlists;
        let mut last_seg = Vec::with_capacity(num_playlists);
        let mut last_part = Vec::with_capacity(num_playlists);
        let mut initial_seg_id = Vec::with_capacity(num_playlists);
        let mut inits = Vec::with_capacity(num_playlists);
        for _ in 0..num_playlists {
            last_seg.push(AtomicUsize::new(0));
            last_part.push(AtomicUsize::new(0));
            initial_seg_id.push(AtomicUsize::new(0));
            inits.push(RwLock::new(init_repeat_value.clone()));
        }

        Store {
            buffer,
            seg_parts,
            last_seg,
            last_part,
            inits,
            offsets: RwLock::new(BTreeMap::new()),
            offset: AtomicUsize::new(0),
            options,
        }
    }

    fn offset(&self, stream_id: u64) -> Option<usize> {
        let lock = self.offsets.read().unwrap();
        lock.get(&stream_id).copied()
    }

    fn last_seg(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_seg[n].load(Ordering::SeqCst))
    }

    fn last_part(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_part[n].load(Ordering::SeqCst))
    }

    fn add_stream_id(&self, stream_id: u64) {
        let idx = self.offset.load(Ordering::SeqCst);
        let next_offset = (idx + 1) % self.options.num_playlists;
        {
            let mut lock = self.offsets.write().unwrap();
            let stream_id = stream_id.to_owned();
            lock.insert(stream_id, idx);
        }
        self.set_last_seg(idx as u64, 0);
        self.set_last_part(idx as u64, 0);

        let seg_idx = self.options.max_segments * idx;
        for n in seg_idx..(seg_idx + self.options.max_segments) {
            self.seg_parts[n].store(0, Ordering::SeqCst)
        }

        self.offset.store(next_offset, Ordering::SeqCst);
    }

    pub(crate) fn zero_stream_id(&self, stream_id: u64) {
        let mut lock = self.offsets.write().unwrap();
        let stream_id = stream_id.to_owned();
        lock.remove(&stream_id);
    }

    fn set_last_seg(&self, stream_id: u64, id: usize) {
        if let Some(n) = self.offset(stream_id) {
            self.last_seg[n].store(id, Ordering::SeqCst);
        }
    }

    fn set_last_part(&self, stream_id: u64, id: usize) {
        if let Some(n) = self.offset(stream_id) {
            self.last_part[n].store(id, Ordering::SeqCst);
        }
    }

    fn zero_buffer(&self, idx: usize) {
        let buffer_size_bytes = self.options.buffer_size_kb * 1024;
        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);

        if let Some(lock) = self.buffer.get(idx) {
            let mut buf = lock.write().unwrap();
            *buf = buffer_repeat_value;
        }
    }

    pub fn set_init(&self, stream_id: u64, data_bytes: Bytes) {
        if let Some(n) = self.offset(stream_id) {
            let mut inits_lock = self.inits[n].write().unwrap();
            *inits_lock = data_bytes;
        }
    }

    pub fn get_init(&self, stream_id: u64) -> Option<Bytes> {
        if let Some(n) = self.offset(stream_id) {
            let lock = &self.inits[n];
            let data = lock.read().unwrap();
            Some(data.clone())
        } else {
            None
        }
    }

    pub fn add(
        &self,
        stream_id: u64,
        segment_id: usize,
        seq: usize,
        idx: usize,
        data: Bytes,
    ) -> u64 {
        if self.offset(stream_id).is_none() {
            self.add_stream_id(stream_id);
        }

        let h = const_xxh3(&data);
        let gz = self.compress_data(&data).unwrap();
        let b = Bytes::from(gz);

        let mut packet = BytesMut::new();
        packet.put_u32(b.len() as u32);
        packet.put_u64(h);
        packet.put(b);

        if let Some(i) = self.calculate_index(stream_id, segment_id, idx) {
            let mut lock = self.buffer[i].write().unwrap();
            *lock = packet.freeze();
        }

        if let Some(n) = self.last_seg(stream_id as u64) {
            if n + 1 == segment_id {
                self.end_segment(stream_id, segment_id, seq);
            }
        }

        self.set_last_seg(stream_id, segment_id);
        self.set_last_part(stream_id, idx);

        h
    }

    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).map_err(|e| e.to_string())?;
        encoder.finish().map_err(|e| e.to_string())
    }

    fn is_included(&self, stream_id: u64, segment_id: usize, part_idx: usize) -> bool {
        if let (Some(last_seg), Some(last_part)) =
            (self.last_seg(stream_id), self.last_part(stream_id))
        {
            if segment_id < last_seg
                || (segment_id == last_seg && part_idx <= last_part)
                    && (last_seg - segment_id) < self.options.max_segments
            {
                return true;
            }
        }

        false
    }

    fn calculate_seg_index(&self, stream_id: u64, segment_id: usize) -> Option<usize> {
        if let Some(offset) = self.offset(stream_id) {
            let sub_buffer_size = self.options.max_segments;
            let idx = segment_id % sub_buffer_size;
            Some((offset * sub_buffer_size) + idx)
        } else {
            None
        }
    }

    fn calculate_index(&self, stream_id: u64, segment_id: usize, seq: usize) -> Option<usize> {
        if segment_id == 0 {
            return None;
        }

        if let Some(offset) = self.offset(stream_id) {
            let sub_buffer_size =
                self.options.max_parts_per_segment * self.options.max_parted_segments;
            let idx = ((segment_id * self.options.max_parts_per_segment) + seq) % sub_buffer_size;
            Some((offset * sub_buffer_size) + idx)
        } else {
            None
        }
    }

    pub fn end_segment(&self, stream_id: u64, segment_id: usize, part_id: usize) {
        if let Some(idx) = self.calculate_seg_index(stream_id, segment_id - 1) {
            self.seg_parts[idx].store(part_id, Ordering::SeqCst);
        }
        if let Some(idx) = self.calculate_seg_index(stream_id, segment_id) {
            self.zero_buffer(idx);
        }
    }

    pub fn get_idxs(&self, stream_id: u64, segment_id: usize) -> Option<(usize, usize)> {
        if let Some(idx) = self.calculate_seg_index(stream_id, segment_id) {
            let b = self.seg_parts[idx].load(Ordering::SeqCst);
            if let Some(idx) = self.calculate_seg_index(stream_id, segment_id - 1) {
                let a = self.seg_parts[idx].load(Ordering::SeqCst);
                if a < b {
                    return Some((a, b));
                }
            }
        }

        None
    }

    fn get_bytes(&self, data: &Bytes) -> (Bytes, u64) {
        let data_size = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let h = u64::from_be_bytes(data[4..12].try_into().unwrap());
        let payload = data.slice(12..12 + data_size as usize);
        (payload, h)
    }

    pub fn get(&self, stream_id: u64, segment_id: usize, part_idx: usize) -> Option<(Bytes, u64)> {
        if self.is_included(stream_id, segment_id, part_idx) {
            if let Some(idx) = self.calculate_index(stream_id, segment_id, part_idx) {
                let lock = self.buffer[idx].read().unwrap();
                let (payload, h) = self.get_bytes(&lock);
                if h != 0 {
                    Some((payload, h))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn last(&self, stream_id: u64) -> Option<(Bytes, u64)> {
        if let (Some(last_seg), Some(last_part)) =
            (self.last_seg(stream_id), self.last_part(stream_id))
        {
            if let Some(idx) = self.calculate_index(stream_id, last_seg, last_part) {
                let lock = self.buffer[idx].read().unwrap();
                let (payload, h) = self.get_bytes(&lock);
                drop(lock);
                Some((payload, h))
            } else {
                None
            }
        } else {
            None
        }
    }
}

pub struct RingBuffer {
    buffer: Vec<RwLock<Bytes>>,
    idxs: RwLock<BTreeMap<u64, usize>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    idx: AtomicUsize,
    options: Options,
}

impl RingBuffer {
    pub fn new(options: Options) -> Self {
        let buffer_size_bytes = options.buffer_size_kb * 1024;
        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);

        let buffer_size =
            options.num_playlists * options.max_parts_per_segment * options.max_parted_segments;
        let mut buffer = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            buffer.push(RwLock::new(buffer_repeat_value.clone()));
        }

        RingBuffer {
            buffer,
            idxs: RwLock::new(BTreeMap::new()),
            offsets: RwLock::new(BTreeMap::new()),
            idx: AtomicUsize::new(0),
            options,
        }
    }

    pub fn set(&self, stream_id: u64, id: usize, data: Bytes) {
        let h = const_xxh3(&data);
        let b = Bytes::from(data);

        let mut packet = BytesMut::new();
        packet.put_u32(b.len() as u32);
        packet.put_u64(h);
        packet.put(b);

        if let Some(idx) = self.offset(stream_id, id) {
            let mut lock = self.buffer[idx].write().unwrap();
            *lock = packet.freeze();
        }
    }

    fn get_bytes(&self, data: &Bytes) -> (Bytes, u64) {
        let data_size = u32::from_be_bytes(data[0..4].try_into().unwrap());

        let h = u64::from_be_bytes(data[4..12].try_into().unwrap());

        let payload = data.slice(12..12 + data_size as usize);

        (payload, h)
    }

    fn add_stream_id(&self, stream_id: u64) {
        let idx = self.idx.load(Ordering::SeqCst);
        let next_offset = (idx + 1) % self.options.num_playlists;
        {
            let mut lock = self.offsets.write().unwrap();
            let stream_id = stream_id.to_owned();
            lock.insert(stream_id, idx);
        }
        self.idx.store(next_offset, Ordering::SeqCst);
    }

    pub(crate) fn zero_stream_id(&self, stream_id: u64) {
        let mut lock = self.offsets.write().unwrap();
        let stream_id = stream_id.to_owned();
        lock.remove(&stream_id);
    }

    pub fn add(&self, stream_id: u64, id: usize, data_bytes: Bytes) {
        if let Some(idx) = self.offset(stream_id, id) {
            self.set(stream_id, idx, data_bytes);
        } else {
            self.add_stream_id(stream_id);
            if let Some(idx) = self.offset(stream_id, id) {
                self.set(stream_id, idx, data_bytes);
            }
        }

        let mut lock = self.idxs.write().unwrap();
        let stream_id = stream_id.to_owned();
        lock.insert(stream_id, id);
    }

    pub fn get(&self, stream_id: u64, id: usize) -> Option<(Bytes, u64)> {
        {
            let lock = self.idxs.read().unwrap();
            if let Some(last) = lock.get(&stream_id) {
                if &id > last {
                    return None;
                }
            } else {
                return None;
            }
        }

        if let Some(idx) = self.offset(stream_id, id) {
            let lock = self.buffer[idx].read().unwrap();
            let (payload, h) = self.get_bytes(&lock);
            drop(lock);
            Some((payload, h))
        } else {
            None
        }
    }

    fn offset(&self, stream_id: u64, id: usize) -> Option<usize> {
        let lock = self.offsets.read().unwrap();
        if let Some(offset) = lock.get(&stream_id) {
            let sub_buffer_size = MAX_PARTS_PER_SEGMENT * MAX_SEGMENTS;
            Some((offset * sub_buffer_size) + id % sub_buffer_size)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_playlist() {
        let mut opts = Options::default();
        opts.segment_min_ms = 400;
        let mut pl = Playlist::new(opts);

        for i in 0..4 {
            let key = if i % 4 == 0 { true } else { false };
            pl.add_part(100, key);
        }

        let expected = b"#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-TARGETDURATION:0\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=0.30000,CAN-SKIP-UNTIL=0.00000\n#EXT-X-PART-INF:PART-TARGET=0.10000\n#EXT-X-MEDIA-SEQUENCE:1\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXT-X-GAP\n#EXTINF:4.00000,\ngap.mp4\n#EXT-X-GAP\n#EXTINF:4.00000,\ngap.mp4\n#EXT-X-GAP\n#EXTINF:4.00000,\ngap.mp4\n#EXT-X-GAP\n#EXTINF:4.00000,\ngap.mp4\n#EXT-X-GAP\n#EXTINF:4.00000,\ngap.mp4\n#EXT-X-GAP\n#EXTINF:4.00000,\ngap.mp4\n#EXT-X-GAP\n#EXTINF:4.00000,\ngap.mp4\n#EXT-X-PART:DURATION=0.10000,URI=\"p1.mp4\",INDEPENDENT=YES\n#EXT-X-PART:DURATION=0.10000,URI=\"p2.mp4\"\n#EXT-X-PART:DURATION=0.10000,URI=\"p3.mp4\"\n#EXT-X-PART:DURATION=0.10000,URI=\"p4.mp4\"\n#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"p5.mp4\"\n";

        //assert_eq!(&pl.m3u8()[..], expected);

        opts = Options::default();
        opts.segment_min_ms = 400;

        pl = Playlist::new(opts);

        for i in 0..222 {
            let key = if i % 4 == 0 { true } else { false };
            pl.add_part(100, key);
        }

        print!("\n\n{}\n\n", String::from_utf8(pl.m3u8().to_vec()).unwrap());
    }

    #[test]
    fn test_wrapping_ring_buffer() {
        let rb = Store::new(Options::default());

        let mut all_added_data = Vec::new();

        let mut ranges = BTreeMap::new();

        for stream_id in 0..2 {
            let mut part_id = 1;
            rb.add_stream_id(stream_id as u64);
            let mut added_data = Vec::new();
            let mut range = Vec::new();
            for a in 1..14 {
                let ra = part_id;
                for b in 0..4 {
                    let data = {
                        let mut d = BytesMut::new();
                        d.put_u8(a);
                        let bytes = (part_id as u64).to_be_bytes();
                        d.put(&bytes[..]);
                        d.freeze()
                    };

                    let h = rb.add(stream_id as u64, a as usize, part_id, 0, data);
                    added_data.push((a as usize, part_id, h));

                    part_id += 1;
                }
                range.push((ra, part_id));

                rb.end_segment(stream_id as u64, a as usize, part_id);
                let idxs = rb.get_idxs(stream_id as u64, a as usize);
            }
            all_added_data.push(added_data);

            ranges.insert(stream_id, range);
        }

        let ids = rb.get_idxs(1, 13);
        assert_eq!(ids.unwrap_or((0, 0)), (49, 53));

        for (stream_id, added_data) in all_added_data.iter().enumerate() {
            for &(a, b, c) in added_data.iter().rev().take(3).collect::<Vec<_>>().iter() {
                match rb.get(stream_id as u64, *a, *b) {
                    Some((_, h)) => {
                        assert_eq!(*c, h);
                    }
                    None => panic!(
                        "Expected data not found for stream_id: {}, segment ID: {}, part ID: {}",
                        stream_id, a, b
                    ),
                }
            }
        }

        for stream_id in 0..2 {
            let range = ranges.get(&stream_id).unwrap();
            let l = range.len();
            for i in (l - 3)..l {
                if let Some(idxs) = rb.get_idxs(stream_id as u64, i) {
                    let r = range[i - 1];
                    assert_eq!(idxs.0, r.0);
                    assert_eq!(idxs.1, r.1);
                }
            }
        }
    }
}
