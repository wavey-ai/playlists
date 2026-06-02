use crate::Options;

use crate::CacheError;
use bytes::{BufMut, Bytes, BytesMut};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::BTreeMap;
use std::io::prelude::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex, RwLock,
};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

const M3U8_HEADER_LEN: usize = 24;

#[derive(Debug, Clone, Copy)]
struct MediaSegmentBlock {
    start: usize,
    duration_ms: u64,
}

pub struct M3u8Cache {
    buffer: Vec<RwLock<Bytes>>,
    seg_parts: Vec<AtomicUsize>,
    last_seg: Vec<AtomicUsize>,
    last_part: Vec<AtomicUsize>,
    last_seq: Vec<AtomicUsize>,
    generations: Vec<AtomicUsize>,
    inits: Vec<RwLock<Bytes>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    offset: AtomicUsize,
    options: Options,
    stream_id_mutex: Mutex<()>,
}

impl M3u8Cache {
    pub fn new(mut options: Options) -> Self {
        options = options.normalized();
        options.buffer_size_kb = 5;
        let buffer_size_bytes = options.buffer_size_kb * 1024;
        let init_size_bytes = options.init_size_kb * 1024;

        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);
        let init_repeat_value = Bytes::from(vec![0u8; init_size_bytes]);

        let buffer_size =
            options.num_playlists * options.max_parts_per_segment * options.max_segments;
        let buffer = (0..buffer_size)
            .map(|_| RwLock::new(buffer_repeat_value.clone()))
            .collect();

        let seg_parts_size = options.max_segments * options.num_playlists;
        let seg_parts = (0..seg_parts_size).map(|_| AtomicUsize::new(0)).collect();

        let num_playlists = options.num_playlists;
        let last_seg = (0..num_playlists).map(|_| AtomicUsize::new(0)).collect();
        let last_part = (0..num_playlists).map(|_| AtomicUsize::new(0)).collect();
        let last_seq = (0..num_playlists).map(|_| AtomicUsize::new(0)).collect();
        let generations = (0..num_playlists).map(|_| AtomicUsize::new(1)).collect();
        let inits = (0..num_playlists)
            .map(|_| RwLock::new(init_repeat_value.clone()))
            .collect();

        Self {
            buffer,
            seg_parts,
            last_seg,
            last_part,
            last_seq,
            generations,
            inits,
            offsets: RwLock::new(BTreeMap::new()),
            offset: AtomicUsize::new(0),
            options,
            stream_id_mutex: Mutex::new(()),
        }
    }

    fn offset(&self, stream_id: u64) -> Option<usize> {
        let lock = self.offsets.read().unwrap();
        lock.get(&stream_id).copied()
    }

    fn offset_and_generation(&self, stream_id: u64) -> Option<(usize, usize)> {
        let n = self.offset(stream_id)?;
        let generation = self.generations[n].load(Ordering::Acquire);
        Some((n, generation))
    }

    fn last_seg(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_seg[n].load(Ordering::Acquire))
    }

    fn last_part(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_part[n].load(Ordering::Acquire))
    }

    fn last_seq(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_seq[n].load(Ordering::Acquire))
    }

    pub fn last_position(&self, stream_id: u64) -> Option<(usize, usize)> {
        Some((self.last_seg(stream_id)?, self.last_part(stream_id)?))
    }

    fn add_stream_id(&self, stream_id: u64) -> Result<(), CacheError> {
        let _lock = self.stream_id_mutex.lock().unwrap();
        if self.offset(stream_id).is_some() {
            return Ok(());
        }

        let idx = self.offset.load(Ordering::Acquire);
        let next_offset = idx
            .checked_add(1)
            .map(|n| n % self.options.num_playlists)
            .ok_or(CacheError::ArithmeticOverflow)?;

        {
            let mut lock = self.offsets.write().unwrap();
            if let Some(previous_stream_id) = lock
                .iter()
                .find_map(|(candidate, mapped_idx)| (*mapped_idx == idx).then_some(*candidate))
            {
                lock.remove(&previous_stream_id);
            }
            self.reset_stream_idx(idx)?;
            lock.insert(stream_id, idx);
        }

        self.set_last_seg(stream_id, 0)?;
        self.set_last_part(stream_id, 0)?;
        self.set_last_seq(stream_id, 0)?;

        let seg_idx = self
            .options
            .max_segments
            .checked_mul(idx)
            .ok_or(CacheError::ArithmeticOverflow)?;
        for n in seg_idx..(seg_idx + self.options.max_segments) {
            self.seg_parts[n].store(0, Ordering::Release);
        }

        self.offset.store(next_offset, Ordering::Release);
        Ok(())
    }

    pub fn ensure_stream_id(&self, stream_id: u64) -> Result<(), CacheError> {
        self.add_stream_id(stream_id)
    }

    pub fn zero_stream_id(&self, stream_id: u64) {
        let mut lock = self.offsets.write().unwrap();
        if let Some(idx) = lock.remove(&stream_id) {
            let _ = self.reset_stream_idx(idx);
        }
    }

    fn reset_stream_idx(&self, idx: usize) -> Result<(), CacheError> {
        self.last_seg[idx].store(0, Ordering::Release);
        self.last_part[idx].store(0, Ordering::Release);
        self.last_seq[idx].store(0, Ordering::Release);
        let next = self.generations[idx]
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        if next == 0 {
            self.generations[idx].store(1, Ordering::Release);
        }

        let seg_idx = self
            .options
            .max_segments
            .checked_mul(idx)
            .ok_or(CacheError::ArithmeticOverflow)?;
        for n in seg_idx..(seg_idx + self.options.max_segments) {
            self.seg_parts[n].store(0, Ordering::Release);
        }
        Ok(())
    }

    fn set_last_seg(&self, stream_id: u64, id: usize) -> Result<(), CacheError> {
        if let Some(n) = self.offset(stream_id) {
            self.last_seg[n].store(id, Ordering::Release);
            Ok(())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    fn set_last_part(&self, stream_id: u64, id: usize) -> Result<(), CacheError> {
        if let Some(n) = self.offset(stream_id) {
            self.last_part[n].store(id, Ordering::Release);
            Ok(())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    fn set_last_seq(&self, stream_id: u64, id: usize) -> Result<(), CacheError> {
        if let Some(n) = self.offset(stream_id) {
            self.last_seq[n].store(id, Ordering::Release);
            Ok(())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    pub fn set_init(&self, stream_id: u64, data_bytes: Bytes) -> Result<(), CacheError> {
        if let Some(n) = self.offset(stream_id) {
            let mut inits_lock = self.inits[n].write().unwrap();
            *inits_lock = data_bytes;
            Ok(())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    pub fn get_init(&self, stream_id: u64) -> Result<Bytes, CacheError> {
        if let Some(n) = self.offset(stream_id) {
            let lock = &self.inits[n];
            let data = lock.read().unwrap();
            Ok(data.clone())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    pub fn add(
        &self,
        stream_id: u64,
        segment_id: usize,
        seq: usize,
        idx: usize,
        data: Bytes,
    ) -> Result<u64, CacheError> {
        if self.offset(stream_id).is_none() {
            self.add_stream_id(stream_id)?;
        }
        let (_, generation) = self
            .offset_and_generation(stream_id)
            .ok_or(CacheError::StreamNotFound)?;

        if idx == 0 {
            self.end_segment(stream_id, segment_id, seq)?;
        }

        let h = const_xxh3(&data);
        let gz = self.compress_data(&data)?;
        let b = Bytes::from(gz);
        if segment_id > u32::MAX as usize || b.len() > u32::MAX as usize {
            return Err(CacheError::BufferOverflow);
        }

        let mut packet = BytesMut::new();
        packet.put_u32(segment_id as u32);
        packet.put_u32(b.len() as u32);
        packet.put_u64(generation as u64);
        packet.put_u64(h);
        packet.put(b);

        if let Some(i) = self.calculate_index(stream_id, segment_id, idx)? {
            let mut lock = self.buffer[i].write().unwrap();
            *lock = packet.freeze();
        }

        if self
            .offset_and_generation(stream_id)
            .map(|(_, current_generation)| current_generation)
            != Some(generation)
        {
            return Err(CacheError::StreamNotFound);
        }
        self.set_last_seg(stream_id, segment_id)?;
        self.set_last_part(stream_id, idx)?;
        self.set_last_seq(stream_id, seq)?;

        Ok(h)
    }

    pub fn end_segment(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_id: usize,
    ) -> Result<(), CacheError> {
        if let Ok(idx) = self.calculate_seg_index(
            stream_id,
            segment_id
                .checked_sub(1)
                .ok_or(CacheError::ArithmeticOverflow)?,
        ) {
            self.seg_parts[idx].store(part_id, Ordering::Release);
        }
        if let Ok(idx) = self.calculate_seg_index(stream_id, segment_id) {
            self.zero_buffer(idx);
        }
        Ok(())
    }

    fn zero_buffer(&self, idx: usize) {
        let buffer_size_bytes = self.options.buffer_size_kb * 1024;
        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);

        let Some(start) = idx.checked_mul(self.options.max_parts_per_segment) else {
            return;
        };
        if start >= self.buffer.len() {
            return;
        }
        let end = start
            .saturating_add(self.options.max_parts_per_segment)
            .min(self.buffer.len());
        for lock in &self.buffer[start..end] {
            let mut buf = lock.write().unwrap();
            *buf = buffer_repeat_value.clone();
        }
    }

    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>, CacheError> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }

    fn decompress_data(&self, data: &[u8]) -> Result<String, CacheError> {
        let mut decoder = GzDecoder::new(data);
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded)?;
        Ok(decoded)
    }

    fn delta_from_gzip(&self, data: &Bytes) -> Result<Option<(Bytes, u64)>, CacheError> {
        let playlist = self.decompress_data(data)?;
        let Some(delta) = playlist_delta_update(&playlist) else {
            return Ok(None);
        };
        let h = const_xxh3(delta.as_bytes());
        Ok(Some((
            Bytes::from(self.compress_data(delta.as_bytes())?),
            h,
        )))
    }

    fn is_included(&self, stream_id: u64, segment_id: usize, part_idx: usize) -> bool {
        if let (Some(last_seg), Some(last_part)) =
            (self.last_seg(stream_id), self.last_part(stream_id))
        {
            if segment_id < last_seg || (segment_id == last_seg && part_idx <= last_part) {
                return true;
            }
        }

        false
    }

    fn calculate_seg_index(&self, stream_id: u64, segment_id: usize) -> Result<usize, CacheError> {
        if let Some(offset) = self.offset(stream_id) {
            let segments_per_stream = self.options.max_segments;
            let wrapped_segment_idx = segment_id % segments_per_stream;
            offset
                .checked_mul(segments_per_stream)
                .and_then(|result| result.checked_add(wrapped_segment_idx))
                .ok_or(CacheError::ArithmeticOverflow)
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    fn calculate_index(
        &self,
        stream_id: u64,
        segment_id: usize,
        seq: usize,
    ) -> Result<Option<usize>, CacheError> {
        if segment_id == 0 {
            return Ok(None);
        }

        if let Some(offset) = self.offset(stream_id) {
            let parts_per_segment = self.options.max_parts_per_segment;
            let segments_per_stream = self.options.max_segments;

            if seq >= parts_per_segment {
                return Err(CacheError::IndexOutOfBounds);
            }

            // Calculate the wrapped segment index
            let wrapped_segment_idx = (segment_id - 1) % segments_per_stream;

            // Calculate the index within the stream's buffer
            let stream_index = wrapped_segment_idx
                .checked_mul(parts_per_segment)
                .and_then(|result| result.checked_add(seq))
                .ok_or(CacheError::ArithmeticOverflow)?;

            // Calculate the global index
            let stream_capacity = segments_per_stream
                .checked_mul(parts_per_segment)
                .ok_or(CacheError::ArithmeticOverflow)?;
            let global_index = offset
                .checked_mul(stream_capacity)
                .and_then(|result| result.checked_add(stream_index))
                .ok_or(CacheError::ArithmeticOverflow)?;

            // Ensure the global index wraps within the total buffer size
            let total_buffer_size = self.buffer.len();
            Ok(Some(global_index % total_buffer_size))
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    pub fn get_idxs(
        &self,
        stream_id: u64,
        segment_id: usize,
    ) -> Result<Option<(usize, usize)>, CacheError> {
        if let Ok(idx) = self.calculate_seg_index(stream_id, segment_id) {
            let mut b = self.seg_parts[idx].load(Ordering::Acquire);
            if self.last_seg(stream_id) == Some(segment_id) {
                b = self
                    .last_seq(stream_id)
                    .and_then(|last_seq| last_seq.checked_add(1))
                    .ok_or(CacheError::ArithmeticOverflow)?;
            }
            if let Ok(prev_idx) = self.calculate_seg_index(
                stream_id,
                segment_id
                    .checked_sub(1)
                    .ok_or(CacheError::ArithmeticOverflow)?,
            ) {
                let a = self.seg_parts[prev_idx].load(Ordering::Acquire);
                if a < b {
                    return Ok(Some((a, b)));
                }
            }
        }

        Ok(None)
    }

    fn get_bytes(&self, data: &Bytes) -> Result<(Bytes, usize, usize, u64), CacheError> {
        if data.len() < M3U8_HEADER_LEN {
            return Err(CacheError::BufferOverflow);
        }
        let seg_id = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let data_size = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
        let generation = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let h = u64::from_be_bytes(data[16..24].try_into().unwrap());
        let end = M3U8_HEADER_LEN
            .checked_add(data_size)
            .ok_or(CacheError::BufferOverflow)?;
        if data.len() < end || generation > usize::MAX as u64 {
            return Err(CacheError::BufferOverflow);
        }
        let payload = data.slice(M3U8_HEADER_LEN..end);
        Ok((payload, seg_id, generation as usize, h))
    }

    pub fn get(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        if self.is_included(stream_id, segment_id, part_idx) {
            let (_, generation) = self
                .offset_and_generation(stream_id)
                .ok_or(CacheError::StreamNotFound)?;
            if let Some(idx) = self.calculate_index(stream_id, segment_id, part_idx)? {
                let lock = self.buffer[idx].read().unwrap();
                let (payload, seg_id, stored_generation, h) = self.get_bytes(&lock)?;
                if seg_id != segment_id || stored_generation != generation {
                    Ok(None)
                } else if h != 0 {
                    Ok(Some((payload, h)))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_delta(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        let Some((data, _)) = self.get(stream_id, segment_id, part_idx)? else {
            return Ok(None);
        };
        self.delta_from_gzip(&data)
    }

    pub fn last(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, CacheError> {
        if let (Some(last_seg), Some(last_part)) =
            (self.last_seg(stream_id), self.last_part(stream_id))
        {
            let (_, generation) = self
                .offset_and_generation(stream_id)
                .ok_or(CacheError::StreamNotFound)?;
            if let Some(idx) = self.calculate_index(stream_id, last_seg, last_part)? {
                let lock = self.buffer[idx].read().unwrap();
                let (payload, seg_id, stored_generation, h) = self.get_bytes(&lock)?;
                if seg_id == last_seg && stored_generation == generation && h != 0 {
                    Ok(Some((payload, h)))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn last_delta(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, CacheError> {
        let Some((data, _)) = self.last(stream_id)? else {
            return Ok(None);
        };
        self.delta_from_gzip(&data)
    }
}

fn playlist_delta_update(playlist: &str) -> Option<String> {
    if playlist.contains("#EXT-X-ENDLIST") || playlist.contains("#EXT-X-SKIP:") {
        return None;
    }

    let skip_boundary_ms = parse_can_skip_until_ms(playlist)?;
    let lines: Vec<&str> = playlist.lines().collect();
    let (blocks, trailing_start, trailing_part_duration_ms) = parse_media_timeline(&lines);
    let insert_at = blocks
        .first()
        .map(|block| block.start)
        .unwrap_or(trailing_start);

    let total_parent_duration_ms = blocks
        .iter()
        .map(|block| block.duration_ms)
        .fold(trailing_part_duration_ms, u64::saturating_add);

    let mut skipped_segments = 0;
    let mut elapsed_ms = 0_u64;
    for block in &blocks {
        elapsed_ms = elapsed_ms.saturating_add(block.duration_ms);
        if total_parent_duration_ms.saturating_sub(elapsed_ms) > skip_boundary_ms {
            skipped_segments += 1;
        } else {
            break;
        }
    }

    let retained_at = blocks
        .get(skipped_segments)
        .map(|block| block.start)
        .unwrap_or(trailing_start);

    let mut delta = String::new();
    push_lines(&mut delta, &lines[..insert_at]);
    delta.push_str(&format!(
        "#EXT-X-SKIP:SKIPPED-SEGMENTS={skipped_segments}\n"
    ));
    push_lines(&mut delta, &lines[retained_at..]);
    Some(delta)
}

fn parse_can_skip_until_ms(playlist: &str) -> Option<u64> {
    playlist
        .lines()
        .find_map(|line| {
            line.strip_prefix("#EXT-X-SERVER-CONTROL:")
                .and_then(|attributes| parse_attribute_value(attributes, "CAN-SKIP-UNTIL"))
        })
        .and_then(|seconds| seconds.parse::<f64>().ok())
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(|seconds| (seconds * 1000.0).round() as u64)
}

fn parse_media_timeline(lines: &[&str]) -> (Vec<MediaSegmentBlock>, usize, u64) {
    let mut blocks = Vec::new();
    let mut current_start = None;
    let mut current_duration_ms = None;

    for (idx, line) in lines.iter().enumerate() {
        if is_segment_scoped_tag(line) {
            current_start.get_or_insert(idx);
        }

        if let Some(duration_ms) = parse_extinf_duration_ms(line) {
            current_start.get_or_insert(idx);
            current_duration_ms = Some(duration_ms);
        }

        if is_uri_line(line) {
            if let (Some(start), Some(duration_ms)) =
                (current_start.take(), current_duration_ms.take())
            {
                blocks.push(MediaSegmentBlock { start, duration_ms });
            }
        }
    }

    let trailing_start = current_start.unwrap_or(lines.len());
    let trailing_part_duration_ms = lines[trailing_start..]
        .iter()
        .filter_map(|line| parse_part_duration_ms(line))
        .fold(0_u64, u64::saturating_add);

    (blocks, trailing_start, trailing_part_duration_ms)
}

fn is_segment_scoped_tag(line: &str) -> bool {
    line.starts_with("#EXT-X-BITRATE:")
        || line.starts_with("#EXT-X-BYTERANGE:")
        || line.starts_with("#EXT-X-DISCONTINUITY")
        || line.starts_with("#EXT-X-GAP")
        || line.starts_with("#EXT-X-KEY:")
        || line.starts_with("#EXT-X-MAP:")
        || line.starts_with("#EXT-X-PART:")
        || line.starts_with("#EXT-X-PROGRAM-DATE-TIME:")
}

fn is_uri_line(line: &str) -> bool {
    !line.is_empty() && !line.starts_with('#')
}

fn parse_extinf_duration_ms(line: &str) -> Option<u64> {
    line.strip_prefix("#EXTINF:")
        .and_then(|value| value.split_once(',').map(|(duration, _)| duration))
        .and_then(parse_duration_ms)
}

fn parse_part_duration_ms(line: &str) -> Option<u64> {
    line.strip_prefix("#EXT-X-PART:")
        .and_then(|attributes| parse_attribute_value(attributes, "DURATION"))
        .and_then(parse_duration_ms)
}

fn parse_duration_ms(value: &str) -> Option<u64> {
    value
        .parse::<f64>()
        .ok()
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(|seconds| (seconds * 1000.0).round() as u64)
}

fn parse_attribute_value<'a>(attributes: &'a str, name: &str) -> Option<&'a str> {
    attributes.split(',').find_map(|attribute| {
        let (attribute_name, value) = attribute.split_once('=')?;
        (attribute_name == name).then_some(value.trim_matches('"'))
    })
}

fn push_lines(output: &mut String, lines: &[&str]) {
    for line in lines {
        output.push_str(line);
        output.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m3u8_manifest::M3u8Manifest;

    fn decompress(data: &[u8]) -> String {
        let mut decoder = GzDecoder::new(data);
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).unwrap();
        decoded
    }

    #[test]
    fn reused_stream_slot_does_not_expose_previous_playlist_data() {
        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 2,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);

        cache
            .add(1, 1, 1, 0, Bytes::from_static(b"first-playlist"))
            .unwrap();
        assert!(cache.get(1, 1, 0).unwrap().is_some());

        cache.ensure_stream_id(2).unwrap();
        assert!(cache.get(1, 1, 0).unwrap().is_none());
        assert!(cache.get(2, 1, 0).unwrap().is_none());
    }

    #[test]
    fn tracks_last_playlist_position() {
        let cache = M3u8Cache::new(Options::default());

        assert_eq!(cache.last_position(7), None);

        cache
            .add(7, 12, 0, 0, Bytes::from_static(b"first"))
            .unwrap();
        assert_eq!(cache.last_position(7), Some((12, 0)));

        cache
            .add(7, 12, 1, 1, Bytes::from_static(b"second"))
            .unwrap();
        assert_eq!(cache.last_position(7), Some((12, 1)));
    }

    #[test]
    fn exposes_current_open_segment_part_range() {
        let cache = M3u8Cache::new(Options::default());

        cache
            .add(7, 4, 24, 0, Bytes::from_static(b"closed-4"))
            .unwrap();
        cache
            .add(7, 5, 25, 0, Bytes::from_static(b"open-5-a"))
            .unwrap();
        cache
            .add(7, 5, 26, 1, Bytes::from_static(b"open-5-b"))
            .unwrap();

        assert_eq!(cache.get_idxs(7, 5).unwrap(), Some((25, 27)));
    }

    #[test]
    fn open_segment_range_ignores_stale_ring_boundary() {
        let options = Options {
            max_segments: 4,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);

        cache
            .add(7, 1, 1, 0, Bytes::from_static(b"open-1"))
            .unwrap();
        cache
            .add(7, 2, 3, 0, Bytes::from_static(b"open-2"))
            .unwrap();
        cache
            .add(7, 3, 5, 0, Bytes::from_static(b"open-3"))
            .unwrap();
        cache
            .add(7, 4, 7, 0, Bytes::from_static(b"open-4-a"))
            .unwrap();
        cache
            .add(7, 4, 8, 1, Bytes::from_static(b"open-4-b"))
            .unwrap();

        assert_eq!(cache.last_position(7), Some((4, 1)));
        assert_eq!(cache.get_idxs(7, 4).unwrap(), Some((7, 9)));
    }

    #[test]
    fn delta_update_replaces_segments_older_than_skip_boundary() {
        let options = Options {
            max_segments: 10,
            segment_min_ms: 1000,
            target_duration_ms: 1000,
            part_target_ms: 1000,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);
        let mut manifest = M3u8Manifest::new(options);
        let mut latest = None;

        for _ in 0..12 {
            latest = Some(manifest.add_part(1000, true));
        }

        let (playlist, segment_id, seq, idx, _) = latest.unwrap();
        cache.add(1, segment_id, seq, idx, playlist).unwrap();

        let (delta, _) = cache.last_delta(1).unwrap().unwrap();
        let delta = decompress(&delta);

        assert!(delta.contains("#EXT-X-VERSION:9"));
        assert!(delta.contains("#EXT-X-SERVER-CONTROL:"));
        assert!(delta.contains("CAN-SKIP-UNTIL=6.00000"));
        assert!(delta.contains("#EXT-X-SKIP:SKIPPED-SEGMENTS=3"));
        assert_eq!(delta.matches("#EXT-X-SKIP:").count(), 1);
        assert!(!delta.contains("s3.mp4"));
        assert!(!delta.contains("s4.mp4"));
        assert!(!delta.contains("s5.mp4"));
        assert!(delta.contains("s6.mp4"));
        assert!(delta.contains("#EXT-X-PART:"));
    }

    #[test]
    fn delta_update_preserves_state_for_first_retained_segment() {
        let playlist = concat!(
            "#EXTM3U\n",
            "#EXT-X-VERSION:9\n",
            "#EXT-X-TARGETDURATION:1\n",
            "#EXT-X-SERVER-CONTROL:CAN-SKIP-UNTIL=3.00000\n",
            "#EXT-X-MEDIA-SEQUENCE:1\n",
            "#EXTINF:1.00000,\n",
            "s1.mp4\n",
            "#EXTINF:1.00000,\n",
            "s2.mp4\n",
            "#EXTINF:1.00000,\n",
            "s3.mp4\n",
            "#EXTINF:1.00000,\n",
            "s4.mp4\n",
            "#EXT-X-DISCONTINUITY\n",
            "#EXT-X-MAP:URI=\"init5.mp4\"\n",
            "#EXT-X-KEY:METHOD=NONE\n",
            "#EXT-X-BITRATE:800\n",
            "#EXT-X-BYTERANGE:10@40\n",
            "#EXTINF:1.00000,\n",
            "s5.mp4\n",
            "#EXTINF:1.00000,\n",
            "s6.mp4\n",
            "#EXTINF:1.00000,\n",
            "s7.mp4\n",
            "#EXTINF:1.00000,\n",
            "s8.mp4\n",
        );

        let delta = playlist_delta_update(playlist).expect("delta update");

        assert!(delta.contains("#EXT-X-SKIP:SKIPPED-SEGMENTS=4"));
        assert!(!delta.contains("s4.mp4"));
        assert!(delta.contains("#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"init5.mp4\""));
        assert!(delta.contains("#EXT-X-KEY:METHOD=NONE"));
        assert!(delta.contains("#EXT-X-BITRATE:800"));
        assert!(delta.contains("#EXT-X-BYTERANGE:10@40\n#EXTINF:1.00000,\ns5.mp4"));
    }

    #[test]
    fn delta_update_is_not_generated_without_can_skip_until() {
        let cache = M3u8Cache::new(Options::default());
        cache
            .add(
                1,
                1,
                1,
                0,
                Bytes::from_static(
                    b"#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES\n",
                ),
            )
            .unwrap();

        assert!(cache.last_delta(1).unwrap().is_none());
    }

    #[test]
    fn truncated_packet_is_rejected_without_panic() {
        let cache = M3u8Cache::new(Options::default());
        let mut packet = BytesMut::new();
        packet.put_u32(1);
        packet.put_u32(64);
        packet.put_u64(1);
        packet.put_u64(99);

        assert!(matches!(
            cache.get_bytes(&packet.freeze()),
            Err(CacheError::BufferOverflow)
        ));
    }
}
