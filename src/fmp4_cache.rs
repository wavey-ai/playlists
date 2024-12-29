use crate::Options;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::watch::{self, Receiver, Sender};
use tokio::sync::RwLock;
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

pub struct Fmp4Cache {
    buffer: Vec<RwLock<Bytes>>,
    idxs: RwLock<BTreeMap<u64, usize>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    idx: AtomicUsize,
    watch_channels: RwLock<HashMap<u64, Sender<usize>>>,
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
            watch_channels: RwLock::new(HashMap::new()),
            options,
        }
    }

    pub async fn watch(&self, stream_id: u64) -> Option<Receiver<usize>> {
        let mut channels = self.watch_channels.write().await;
        if let Some(sender) = channels.get(&stream_id) {
            Some(sender.subscribe())
        } else {
            let (sender, receiver) = watch::channel(0);
            channels.insert(stream_id, sender);
            Some(receiver)
        }
    }

    pub async fn set(&self, stream_id: u64, id: usize, data: Bytes) -> Result<(), &'static str> {
        let h = const_xxh3(&data);
        let mut packet = BytesMut::new();
        packet.put_u32(data.len() as u32);
        packet.put_u64(h);
        packet.put(data);

        if let Some(idx) = self.offset(stream_id, id).await {
            let mut lock = self.buffer[idx].write().await;
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

    async fn add_stream_id(&self, stream_id: u64) {
        let idx = self.idx.fetch_add(1, Ordering::SeqCst) % self.options.num_playlists;
        let mut lock = self.offsets.write().await;
        lock.insert(stream_id, idx);
    }

    pub async fn zero_stream_id(&self, stream_id: u64) {
        let mut offsets_lock = self.offsets.write().await;
        if let Some(offset) = offsets_lock.remove(&stream_id) {
            let sub_buffer_size = self.options.max_parts_per_segment * self.options.max_segments;
            let start_idx = offset * sub_buffer_size;
            let end_idx = start_idx + sub_buffer_size;
            drop(offsets_lock);

            for idx in start_idx..end_idx {
                let mut buffer_lock = self.buffer[idx].write().await;
                *buffer_lock = Bytes::from(vec![0u8; self.options.buffer_size_kb * 1024]);
            }
        }

        let mut idxs_lock = self.idxs.write().await;
        idxs_lock.remove(&stream_id);

        // Clean up watch channel
        self.watch_channels.write().await.remove(&stream_id);
    }

    pub async fn append(&self, stream_id: u64, data_bytes: Bytes) -> Result<(), &'static str> {
        if let Some(idx) = self.last(stream_id).await {
            self.add(stream_id, idx + 1, data_bytes).await
        } else {
            self.add(stream_id, 1, data_bytes).await
        }
    }

    pub async fn add(
        &self,
        stream_id: u64,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<(), &'static str> {
        let offsets_lock = self.offsets.read().await;
        if !offsets_lock.contains_key(&stream_id) {
            drop(offsets_lock);
            self.add_stream_id(stream_id).await;
        } else {
            drop(offsets_lock);
        }

        self.set(stream_id, id, data_bytes).await?;

        let mut lock = self.idxs.write().await;
        lock.insert(stream_id, id);

        // Notify watchers
        if let Some(sender) = self.watch_channels.read().await.get(&stream_id) {
            let _ = sender.send(id);
        }

        Ok(())
    }

    pub async fn last(&self, stream_id: u64) -> Option<usize> {
        self.idxs.read().await.get(&stream_id).copied()
    }

    pub async fn get(&self, stream_id: u64, id: usize) -> Option<(Bytes, u64)> {
        let idxs_lock = self.idxs.read().await;
        if let Some(&last) = idxs_lock.get(&stream_id) {
            if id > last {
                return None;
            }
        } else {
            return None;
        }
        drop(idxs_lock);

        let idx = self.offset(stream_id, id).await?;
        let bytes = self.buffer[idx].read().await;
        self.get_bytes(&bytes).ok()
    }

    async fn offset(&self, stream_id: u64, id: usize) -> Option<usize> {
        let lock = self.offsets.read().await;
        lock.get(&stream_id).copied().map(|offset| {
            let sub_buffer_size = self.options.max_parts_per_segment * self.options.max_segments;
            offset
                .checked_mul(sub_buffer_size)
                .and_then(|result| result.checked_add(id % sub_buffer_size))
                .unwrap_or(0)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::task;

    #[tokio::test]
    async fn test_concurrent_read_write_throughput() {
        // Test configuration
        const TEST_DURATION_SECS: u64 = 50;
        const NUM_READER_THREADS: usize = 8;
        const NUM_WRITER_THREADS: usize = 2;
        const STREAM_ID: u64 = 1;

        // Create cache with test configuration
        let options = Options::default();
        let cache = Arc::new(Fmp4Cache::new(options));

        // Counters for operations
        let read_count = Arc::new(AtomicU64::new(0));
        let write_count = Arc::new(AtomicU64::new(0));

        // Generate some initial data
        let sample_data = Bytes::from(vec![1u8; 1024]);
        cache.add(STREAM_ID, 1, sample_data.clone()).await.unwrap();

        // Spawn reader tasks
        let mut reader_handles = Vec::new();
        for _ in 0..NUM_READER_THREADS {
            let cache_clone = Arc::clone(&cache);
            let read_count_clone = Arc::clone(&read_count);

            let handle = task::spawn(async move {
                let start = Instant::now();
                while start.elapsed().as_secs() < TEST_DURATION_SECS {
                    if let Some(_) = cache_clone.get(STREAM_ID, 1).await {
                        read_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            reader_handles.push(handle);
        }

        // Spawn writer tasks
        let mut writer_handles = Vec::new();
        for _ in 0..NUM_WRITER_THREADS {
            let cache_clone = Arc::clone(&cache);
            let write_count_clone = Arc::clone(&write_count);
            let sample_data = sample_data.clone();

            let handle = task::spawn(async move {
                let start = Instant::now();
                let mut id = 1;
                while start.elapsed().as_secs() < TEST_DURATION_SECS {
                    if cache_clone
                        .add(STREAM_ID, id, sample_data.clone())
                        .await
                        .is_ok()
                    {
                        write_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    id = id.wrapping_add(1);
                    if id >= 10 {
                        id = 1;
                    }
                }
            });
            writer_handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in reader_handles {
            handle.await.unwrap();
        }
        for handle in writer_handles {
            handle.await.unwrap();
        }

        // Report results
        let total_reads = read_count.load(Ordering::Relaxed);
        let total_writes = write_count.load(Ordering::Relaxed);
        let reads_per_sec = total_reads as f64 / TEST_DURATION_SECS as f64;
        let writes_per_sec = total_writes as f64 / TEST_DURATION_SECS as f64;

        println!("=== Contention Test Results ===");
        println!("Test duration: {} seconds", TEST_DURATION_SECS);
        println!("Reader threads: {}", NUM_READER_THREADS);
        println!("Writer threads: {}", NUM_WRITER_THREADS);
        println!("Total reads: {}", total_reads);
        println!("Total writes: {}", total_writes);
        println!("Reads/second: {:.2}", reads_per_sec);
        println!("Writes/second: {:.2}", writes_per_sec);
    }
}
