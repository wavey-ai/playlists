use crate::Options;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    RwLock,
};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

pub struct Fmp4Cache {
    buffer: Vec<RwLock<Bytes>>,
    idxs: RwLock<BTreeMap<u64, usize>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    idx: AtomicUsize,
    pub options: Options,
}

impl Fmp4Cache {
    pub fn new(options: Options) -> Self {
        let buffer_size_bytes = options.buffer_size_kb * 1024;
        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);
        let buffer_size =
            options.num_playlists * options.max_parts_per_segment * options.max_segments;
        let buffer = (0..buffer_size)
            .map(|_| RwLock::new(buffer_repeat_value.clone()))
            .collect();

        Self {
            buffer,
            idxs: RwLock::new(BTreeMap::new()),
            offsets: RwLock::new(BTreeMap::new()),
            idx: AtomicUsize::new(0),
            options,
        }
    }

    pub fn set(&self, stream_id: u64, id: usize, data: Bytes) -> Result<(), &'static str> {
        let h = const_xxh3(&data);
        let mut packet = BytesMut::new();
        packet.put_u32(data.len() as u32);
        packet.put_u64(h);
        packet.put(data);

        if let Some(idx) = self.offset(stream_id, id) {
            let mut lock = self.buffer[idx]
                .write()
                .map_err(|_| "Failed to acquire write lock")?;
            *lock = packet.freeze();
            Ok(())
        } else {
            Err("Invalid stream_id or id")
        }
    }

    fn get_bytes(&self, data: &Bytes) -> Result<(Bytes, u64), &'static str> {
        if data.len() < 12 {
            return Err("Invalid data format");
        }
        let data_size = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let h = u64::from_be_bytes(data[4..12].try_into().unwrap());
        if data.len() < 12 + data_size as usize {
            return Err("Invalid data size");
        }
        let payload = data.slice(12..12 + data_size as usize);
        Ok((payload, h))
    }

    fn add_stream_id(&self, stream_id: u64) {
        let idx = self.idx.fetch_add(1, Ordering::SeqCst) % self.options.num_playlists;
        let mut lock = self.offsets.write().unwrap();
        lock.insert(stream_id, idx);
    }

    pub fn zero_stream_id(&self, stream_id: u64) {
        let mut offsets_lock = self.offsets.write().unwrap();
        if let Some(offset) = offsets_lock.remove(&stream_id) {
            let sub_buffer_size = self.options.max_parts_per_segment * self.options.max_segments;
            let start_idx = offset * sub_buffer_size;
            let end_idx = start_idx + sub_buffer_size;
            drop(offsets_lock); // Release the write lock early

            for idx in start_idx..end_idx {
                if let Ok(mut buffer_lock) = self.buffer[idx].write() {
                    *buffer_lock = Bytes::from(vec![0u8; self.options.buffer_size_kb * 1024]);
                }
            }
        }

        let mut idxs_lock = self.idxs.write().unwrap();
        idxs_lock.remove(&stream_id);
    }

    pub fn add(&self, stream_id: u64, id: usize, data_bytes: Bytes) -> Result<(), &'static str> {
        let offsets_lock = self.offsets.write().unwrap();
        if !offsets_lock.contains_key(&stream_id) {
            drop(offsets_lock);
            self.add_stream_id(stream_id);
        } else {
            drop(offsets_lock);
        }

        self.set(stream_id, id, data_bytes)?;

        let mut lock = self.idxs.write().unwrap();
        lock.insert(stream_id, id);
        Ok(())
    }

    pub fn get(&self, stream_id: u64, id: usize) -> Option<(Bytes, u64)> {
        let idxs_lock = self.idxs.read().unwrap();
        if let Some(last) = idxs_lock.get(&stream_id) {
            if &id > last {
                return None;
            }
        } else {
            return None;
        }
        drop(idxs_lock);

        self.offset(stream_id, id).and_then(|idx| {
            self.buffer[idx]
                .read()
                .ok()
                .and_then(|lock| self.get_bytes(&lock).ok())
        })
    }

    fn offset(&self, stream_id: u64, id: usize) -> Option<usize> {
        let lock = self.offsets.read().unwrap();
        lock.get(&stream_id).map(|&offset| {
            let sub_buffer_size = self.options.max_parts_per_segment * self.options.max_segments;
            offset
                .checked_mul(sub_buffer_size)
                .and_then(|result| result.checked_add(id % sub_buffer_size))
                .unwrap_or(0)
        })
    }
}
