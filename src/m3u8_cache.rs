use crate::Options;

use bytes::{BufMut, Bytes, BytesMut};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::BTreeMap;
use std::io::prelude::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex, RwLock,
};
use thiserror::Error;
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

#[derive(Error, Debug)]
pub enum M3u8CacheError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Index out of bounds")]
    IndexOutOfBounds,
    #[error("Stream not found")]
    StreamNotFound,
    #[error("Buffer overflow")]
    BufferOverflow,
    #[error("Arithmetic overflow")]
    ArithmeticOverflow,
}

pub struct M3u8Cache {
    buffer: Vec<RwLock<Bytes>>,
    seg_parts: Vec<AtomicUsize>,
    last_seg: Vec<AtomicUsize>,
    last_part: Vec<AtomicUsize>,
    inits: Vec<RwLock<Bytes>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    offset: AtomicUsize,
    options: Options,
    stream_id_mutex: Mutex<()>,
}

impl M3u8Cache {
    pub fn new(mut options: Options) -> Self {
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
        let inits = (0..num_playlists)
            .map(|_| RwLock::new(init_repeat_value.clone()))
            .collect();

        Self {
            buffer,
            seg_parts,
            last_seg,
            last_part,
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

    fn last_seg(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_seg[n].load(Ordering::Acquire))
    }

    fn last_part(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_part[n].load(Ordering::Acquire))
    }

    fn add_stream_id(&self, stream_id: u64) -> Result<(), M3u8CacheError> {
        let _lock = self.stream_id_mutex.lock().unwrap();
        let idx = self.offset.load(Ordering::Acquire);
        let next_offset = idx
            .checked_add(1)
            .and_then(|n| Some(n % self.options.num_playlists))
            .ok_or(M3u8CacheError::ArithmeticOverflow)?;

        {
            let mut lock = self.offsets.write().unwrap();
            lock.insert(stream_id, idx);
        }

        self.set_last_seg(stream_id, 0)?;
        self.set_last_part(stream_id, 0)?;

        let seg_idx = self
            .options
            .max_segments
            .checked_mul(idx)
            .ok_or(M3u8CacheError::ArithmeticOverflow)?;
        for n in seg_idx..(seg_idx + self.options.max_segments) {
            self.seg_parts[n].store(0, Ordering::Release);
        }

        self.offset.store(next_offset, Ordering::Release);
        Ok(())
    }

    pub fn zero_stream_id(&self, stream_id: u64) {
        let mut lock = self.offsets.write().unwrap();
        lock.remove(&stream_id);
    }

    fn set_last_seg(&self, stream_id: u64, id: usize) -> Result<(), M3u8CacheError> {
        if let Some(n) = self.offset(stream_id) {
            self.last_seg[n].store(id, Ordering::Release);
            Ok(())
        } else {
            Err(M3u8CacheError::StreamNotFound)
        }
    }

    fn set_last_part(&self, stream_id: u64, id: usize) -> Result<(), M3u8CacheError> {
        if let Some(n) = self.offset(stream_id) {
            self.last_part[n].store(id, Ordering::Release);
            Ok(())
        } else {
            Err(M3u8CacheError::StreamNotFound)
        }
    }

    pub fn set_init(&self, stream_id: u64, data_bytes: Bytes) -> Result<(), M3u8CacheError> {
        if let Some(n) = self.offset(stream_id) {
            let mut inits_lock = self.inits[n].write().unwrap();
            *inits_lock = data_bytes;
            Ok(())
        } else {
            Err(M3u8CacheError::StreamNotFound)
        }
    }

    pub fn get_init(&self, stream_id: u64) -> Result<Bytes, M3u8CacheError> {
        if let Some(n) = self.offset(stream_id) {
            let lock = &self.inits[n];
            let data = lock.read().unwrap();
            Ok(data.clone())
        } else {
            Err(M3u8CacheError::StreamNotFound)
        }
    }

    pub fn add(
        &self,
        stream_id: u64,
        segment_id: usize,
        seq: usize,
        idx: usize,
        data: Bytes,
    ) -> Result<u64, M3u8CacheError> {
        if self.offset(stream_id).is_none() {
            self.add_stream_id(stream_id)?;
        }

        let h = const_xxh3(&data);
        let gz = self.compress_data(&data)?;
        let b = Bytes::from(gz);

        let mut packet = BytesMut::new();
        packet.put_u32(segment_id as u32);
        packet.put_u32(b.len() as u32);
        packet.put_u64(h);
        packet.put(b);

        if let Some(i) = self.calculate_index(stream_id, segment_id, idx)? {
            let mut lock = self.buffer[i].write().unwrap();
            *lock = packet.freeze();
        }

        if idx == 0 {
            self.end_segment(stream_id, segment_id, seq)?;
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
    ) -> Result<(), M3u8CacheError> {
        if let Ok(idx) = self.calculate_seg_index(
            stream_id,
            segment_id
                .checked_sub(1)
                .ok_or(M3u8CacheError::ArithmeticOverflow)?,
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

        if let Some(lock) = self.buffer.get(idx) {
            let mut buf = lock.write().unwrap();
            *buf = buffer_repeat_value;
        }
    }

    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>, M3u8CacheError> {
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

    fn calculate_seg_index(
        &self,
        stream_id: u64,
        segment_id: usize,
    ) -> Result<usize, M3u8CacheError> {
        if let Some(offset) = self.offset(stream_id) {
            let segments_per_stream = self.options.max_segments;
            let wrapped_segment_idx = segment_id % segments_per_stream;
            offset
                .checked_mul(segments_per_stream)
                .and_then(|result| result.checked_add(wrapped_segment_idx))
                .ok_or(M3u8CacheError::ArithmeticOverflow)
        } else {
            Err(M3u8CacheError::StreamNotFound)
        }
    }

    fn calculate_index(
        &self,
        stream_id: u64,
        segment_id: usize,
        seq: usize,
    ) -> Result<Option<usize>, M3u8CacheError> {
        if segment_id == 0 {
            return Ok(None);
        }

        if let Some(offset) = self.offset(stream_id) {
            let parts_per_segment = self.options.max_parts_per_segment;
            let segments_per_stream = self.options.max_segments;

            if seq >= parts_per_segment {
                return Err(M3u8CacheError::IndexOutOfBounds);
            }

            // Calculate the wrapped segment index
            let wrapped_segment_idx = (segment_id - 1) % segments_per_stream;

            // Calculate the index within the stream's buffer
            let stream_index = wrapped_segment_idx
                .checked_mul(parts_per_segment)
                .and_then(|result| result.checked_add(seq))
                .ok_or(M3u8CacheError::ArithmeticOverflow)?;

            // Calculate the global index
            let global_index = offset
                .checked_mul(segments_per_stream * parts_per_segment)
                .and_then(|result| result.checked_add(stream_index))
                .ok_or(M3u8CacheError::ArithmeticOverflow)?;

            // Ensure the global index wraps within the total buffer size
            let total_buffer_size = self.buffer.len();
            Ok(Some(global_index % total_buffer_size))
        } else {
            Err(M3u8CacheError::StreamNotFound)
        }
    }

    pub fn get_idxs(
        &self,
        stream_id: u64,
        segment_id: usize,
    ) -> Result<Option<(usize, usize)>, M3u8CacheError> {
        if let Ok(idx) = self.calculate_seg_index(stream_id, segment_id) {
            let b = self.seg_parts[idx].load(Ordering::Acquire);
            if let Ok(prev_idx) = self.calculate_seg_index(
                stream_id,
                segment_id
                    .checked_sub(1)
                    .ok_or(M3u8CacheError::ArithmeticOverflow)?,
            ) {
                let a = self.seg_parts[prev_idx].load(Ordering::Acquire);
                if a < b {
                    return Ok(Some((a, b)));
                }
            }
        }

        Ok(None)
    }

    fn get_bytes(&self, data: &Bytes) -> Result<(Bytes, usize, u64), M3u8CacheError> {
        if data.len() < 16 {
            return Err(M3u8CacheError::BufferOverflow);
        }
        let seg_id = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
        let data_size = u32::from_be_bytes(data[4..8].try_into().unwrap()) as usize;
        let h = u64::from_be_bytes(data[8..16].try_into().unwrap());
        if data.len() < 12 + data_size {
            return Err(M3u8CacheError::BufferOverflow);
        }
        let payload = data.slice(16..16 + data_size);
        Ok((payload, seg_id, h))
    }

    pub fn get(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<(Bytes, u64)>, M3u8CacheError> {
        if self.is_included(stream_id, segment_id, part_idx) {
            if let Some(idx) = self.calculate_index(stream_id, segment_id, part_idx)? {
                let lock = self.buffer[idx].read().unwrap();
                let (payload, seg_id, h) = self.get_bytes(&lock)?;
                if seg_id != segment_id {
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
    pub fn last(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, M3u8CacheError> {
        if let (Some(last_seg), Some(last_part)) =
            (self.last_seg(stream_id), self.last_part(stream_id))
        {
            if let Some(idx) = self.calculate_index(stream_id, last_seg, last_part)? {
                let lock = self.buffer[idx].read().unwrap();
                let (payload, _, h) = self.get_bytes(&lock)?;
                Ok(Some((payload, h)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
