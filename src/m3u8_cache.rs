use crate::Options;

use crate::CacheError;
use bytes::{BufMut, Bytes, BytesMut};
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

pub struct M3u8Cache {
    buffer: Vec<RwLock<Bytes>>,
    seg_parts: Vec<AtomicUsize>,
    last_seg: Vec<AtomicUsize>,
    last_part: Vec<AtomicUsize>,
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
        let generations = (0..num_playlists).map(|_| AtomicUsize::new(1)).collect();
        let inits = (0..num_playlists)
            .map(|_| RwLock::new(init_repeat_value.clone()))
            .collect();

        Self {
            buffer,
            seg_parts,
            last_seg,
            last_part,
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
            let b = self.seg_parts[idx].load(Ordering::Acquire);
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
