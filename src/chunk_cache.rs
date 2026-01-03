use crate::Options;
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

pub struct ChunkCache {
    buffer: Vec<RwLock<Bytes>>,
    idxs: Vec<AtomicUsize>,
    offsets: RwLock<HashMap<u64, usize>>,
    idx: AtomicUsize,
    new_playlist_tx: mpsc::UnboundedSender<(u64, usize)>,
    new_playlist_rx: Mutex<Option<mpsc::UnboundedReceiver<(u64, usize)>>>,
    pub options: Options,
}

impl ChunkCache {
    pub fn new(options: Options) -> Self {
        let num_playlists: usize = options.num_playlists;

        let buffer_size_bytes = options.buffer_size_kb * 1024;
        let buffer_repeat_value = Bytes::from(vec![0u8; buffer_size_bytes]);
        let buffer_size =
            options.num_playlists * options.max_parts_per_segment * options.max_segments;
        let buffer = (0..buffer_size)
            .map(|_| RwLock::new(buffer_repeat_value.clone()))
            .collect();
        let idxs = (0..num_playlists).map(|_| AtomicUsize::new(0)).collect();
        let (new_playlist_tx, new_playlist_rx) = mpsc::unbounded_channel();

        Self {
            buffer,
            idxs,
            offsets: RwLock::new(HashMap::new()),
            idx: AtomicUsize::new(0),
            new_playlist_tx,
            new_playlist_rx: Mutex::new(Some(new_playlist_rx)),
            options,
        }
    }

    pub fn take_new_playlists_rx(&self) -> Option<mpsc::UnboundedReceiver<(u64, usize)>> {
        self.new_playlist_rx.lock().unwrap().take()
    }

    pub async fn get_or_create_stream_idx(&self, stream_id: u64) -> usize {
        if let Some(idx) = self.get_stream_idx(stream_id).await {
            idx
        } else {
            self.add_stream_id(stream_id).await
        }
    }

    pub async fn add_stream_id(&self, stream_id: u64) -> usize {
        let mut lock = self.offsets.write().await;
        if let Some(idx) = lock.get(&stream_id).copied() {
            return idx;
        }

        let idx = self.idx.fetch_add(1, Ordering::SeqCst) % self.options.num_playlists;
        lock.insert(stream_id, idx);
        drop(lock);
        let _ = self.new_playlist_tx.send((stream_id, idx));
        idx
    }

    pub async fn get_stream_idx(&self, stream_id: u64) -> Option<usize> {
        let lock = self.offsets.read().await;
        lock.get(&stream_id).copied()
    }

    pub async fn set(&self, stream_idx: usize, id: usize, data: Bytes) -> Result<(), &'static str> {
        let h = const_xxh3(&data);
        let mut packet = BytesMut::new();
        packet.put_u32(data.len() as u32);
        packet.put_u64(h);
        packet.put(data);

        let idx = self.offset(stream_idx, id);
        let mut lock = self.buffer[idx].write().await;
        *lock = packet.freeze();
        Ok(())
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
    }

    pub async fn append(&self, stream_idx: usize, data_bytes: Bytes) -> Result<(), &'static str> {
        if let Some(idx) = self.last(stream_idx) {
            self.add(stream_idx, idx + 1, data_bytes).await
        } else {
            self.add(stream_idx, 1, data_bytes).await
        }
    }

    pub async fn add(
        &self,
        stream_idx: usize,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<(), &'static str> {
        self.set(stream_idx, id, data_bytes).await?;
        self.idxs[stream_idx].store(id, Ordering::Release);

        Ok(())
    }

    pub async fn get_last(&self, stream_idx: usize) -> Option<(usize, Bytes, u64)> {
        if let Some(id) = self.last(stream_idx) {
            if let Some((bytes, h)) = self.get(stream_idx, id).await {
                return Some((id, bytes, h));
            }
        }

        None
    }

    pub fn last(&self, stream_idx: usize) -> Option<usize> {
        let val = self.idxs[stream_idx].load(Ordering::Acquire);

        Some(val)
    }

    pub async fn get(&self, stream_idx: usize, id: usize) -> Option<(Bytes, u64)> {
        if let Some(last) = self.last(stream_idx) {
            if id > last {
                return None;
            }
        } else {
            return None;
        }

        let idx = self.offset(stream_idx, id);
        let bytes = self.buffer[idx].read().await;
        self.get_bytes(&bytes).ok()
    }

    fn offset(&self, stream_idx: usize, id: usize) -> usize {
        let sub_buffer_size = self.options.max_parts_per_segment * self.options.max_segments;
        stream_idx
            .checked_mul(sub_buffer_size)
            .and_then(|result| result.checked_add(id % sub_buffer_size))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::task;
    use tokio::time::{timeout, Duration, Instant};

    #[tokio::test]
    async fn test_append_and_last() {
        const TEST_DURATION_SECS: u64 = 5;
        const NUM_READERS: usize = 1;
        const STREAM_ID: u64 = 1;

        println!(
            "Starting max read test for {}s, {} readers",
            TEST_DURATION_SECS, NUM_READERS
        );

        let options = Options::default();
        let cache = Arc::new(ChunkCache::new(options));
        let read_count = Arc::new(AtomicU64::new(0));
        let write_count = Arc::new(AtomicU64::new(0));

        let stream_idx = cache.get_or_create_stream_idx(STREAM_ID).await;

        let mut reader_handles = Vec::new();
        for _ in 0..NUM_READERS {
            let cache_clone = Arc::clone(&cache);
            let read_count_clone = Arc::clone(&read_count);

            let handle = task::spawn(async move {
                let start = Instant::now();
                while start.elapsed().as_secs() < TEST_DURATION_SECS {
                    if let Some(_) = cache_clone.last(stream_idx) {
                        read_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            reader_handles.push(handle);
        }

        let writer_handle = {
            let cache_clone = Arc::clone(&cache);
            let write_count_clone = Arc::clone(&write_count);

            task::spawn(async move {
                let start = Instant::now();
                let mut id = 0u64;
                while start.elapsed().as_secs() < TEST_DURATION_SECS {
                    id += 1;
                    let mut data = vec![0u8; 64];
                    data[0..8].copy_from_slice(&id.to_le_bytes());
                    if cache_clone
                        .append(stream_idx, Bytes::from(data))
                        .await
                        .is_ok()
                    {
                        write_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        };

        for handle in reader_handles {
            handle.await.unwrap();
        }
        writer_handle.await.unwrap();

        let total_reads = read_count.load(Ordering::Relaxed);
        let total_writes = write_count.load(Ordering::Relaxed);
        let reads_per_sec = total_reads as f64 / TEST_DURATION_SECS as f64;
        let writes_per_sec = total_writes as f64 / TEST_DURATION_SECS as f64;

        println!("\n=== Test Results ===");
        println!("Total reads: {}", total_reads);
        println!("Total writes: {}", total_writes);
        println!("Reads/second: {:.2}", reads_per_sec);
        println!("Writes/second: {:.2}", writes_per_sec);
        println!(
            "Average reads per write: {:.2}",
            total_reads as f64 / (total_writes as f64).max(1.0)
        );
    }

    #[tokio::test]
    async fn test_concurrent_read_write() {
        const TEST_DURATION_SECS: u64 = 5;
        const NUM_STREAMS: usize = 8;

        println!("\n=== Concurrent ChunkCache Benchmark ===");
        println!("Duration: {}s, Streams: {} (1 writer + 2 readers each)", TEST_DURATION_SECS, NUM_STREAMS);

        let mut options = Options::default();
        options.num_playlists = NUM_STREAMS;
        options.buffer_size_kb = 64;
        options.max_parts_per_segment = 10000;
        let cache = Arc::new(ChunkCache::new(options));

        let read_count = Arc::new(AtomicU64::new(0));
        let write_count = Arc::new(AtomicU64::new(0));
        let read_bytes = Arc::new(AtomicU64::new(0));
        let write_bytes = Arc::new(AtomicU64::new(0));

        // Pre-create streams
        for i in 0..NUM_STREAMS {
            cache.get_or_create_stream_idx(i as u64).await;
            // Seed with initial data so readers have something
            cache.append(i, Bytes::from(vec![0u8; 64 * 1024])).await.ok();
        }

        let mut handles = Vec::new();

        // Each stream gets 1 writer
        for stream_idx in 0..NUM_STREAMS {
            let cache_clone = Arc::clone(&cache);
            let write_count_clone = Arc::clone(&write_count);
            let write_bytes_clone = Arc::clone(&write_bytes);

            handles.push(task::spawn(async move {
                let start = Instant::now();
                let data = Bytes::from(vec![0xABu8; 64 * 1024]); // 64KB chunks

                while start.elapsed().as_secs() < TEST_DURATION_SECS {
                    if cache_clone.append(stream_idx, data.clone()).await.is_ok() {
                        write_count_clone.fetch_add(1, Ordering::Relaxed);
                        write_bytes_clone.fetch_add(data.len() as u64, Ordering::Relaxed);
                    }
                }
            }));
        }

        // Each stream gets 2 readers
        for stream_idx in 0..NUM_STREAMS {
            for _ in 0..2 {
                let cache_clone = Arc::clone(&cache);
                let read_count_clone = Arc::clone(&read_count);
                let read_bytes_clone = Arc::clone(&read_bytes);

                handles.push(task::spawn(async move {
                    let start = Instant::now();
                    let mut slot = 1usize;

                    while start.elapsed().as_secs() < TEST_DURATION_SECS {
                        if let Some((bytes, _hash)) = cache_clone.get(stream_idx, slot).await {
                            read_count_clone.fetch_add(1, Ordering::Relaxed);
                            read_bytes_clone.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                            // Move to next slot, wrap around
                            if let Some(last) = cache_clone.last(stream_idx) {
                                slot = if slot >= last { 1 } else { slot + 1 };
                            }
                        }
                    }
                }));
            }
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let total_reads = read_count.load(Ordering::Relaxed);
        let total_writes = write_count.load(Ordering::Relaxed);
        let total_read_bytes = read_bytes.load(Ordering::Relaxed);
        let total_write_bytes = write_bytes.load(Ordering::Relaxed);

        let reads_per_sec = total_reads as f64 / TEST_DURATION_SECS as f64;
        let writes_per_sec = total_writes as f64 / TEST_DURATION_SECS as f64;
        let read_throughput_mb = (total_read_bytes as f64 / 1024.0 / 1024.0) / TEST_DURATION_SECS as f64;
        let write_throughput_mb = (total_write_bytes as f64 / 1024.0 / 1024.0) / TEST_DURATION_SECS as f64;

        println!("\n=== Results ===");
        println!("Writers: {} streams", NUM_STREAMS);
        println!("Readers: {} total ({} per stream)", NUM_STREAMS * 2, 2);
        println!("Write: {:.0}/s ({:.0} MB/s)", writes_per_sec, write_throughput_mb);
        println!("Read:  {:.0}/s ({:.0} MB/s)", reads_per_sec, read_throughput_mb);
        println!("Combined throughput: {:.0} MB/s", read_throughput_mb + write_throughput_mb);
    }

    #[tokio::test]
    async fn test_new_playlist_notification_sent() {
        let options = Options::default();
        let cache = ChunkCache::new(options);
        let mut rx = cache.take_new_playlists_rx().expect("receiver already taken");

        let idx = cache.add_stream_id(42).await;
        let (stream_id, notified_idx) = rx.recv().await.expect("missing notification");

        assert_eq!(stream_id, 42);
        assert_eq!(notified_idx, idx);
    }

    #[tokio::test]
    async fn test_no_notification_for_existing_stream_id() {
        let options = Options::default();
        let cache = ChunkCache::new(options);
        let mut rx = cache.take_new_playlists_rx().expect("receiver already taken");

        let _ = cache.add_stream_id(7).await;
        let _ = rx.recv().await.expect("missing initial notification");

        let _ = cache.add_stream_id(7).await;
        let recv = timeout(Duration::from_millis(50), rx.recv()).await;

        assert!(recv.is_err());
    }

    #[tokio::test]
    async fn test_take_new_playlists_rx_single_use() {
        let options = Options::default();
        let cache = ChunkCache::new(options);
        let mut rx = cache.take_new_playlists_rx().expect("receiver already taken");

        assert!(cache.take_new_playlists_rx().is_none());

        let idx = cache.add_stream_id(13).await;
        let (stream_id, notified_idx) = rx.recv().await.expect("missing notification");

        assert_eq!(stream_id, 13);
        assert_eq!(notified_idx, idx);
    }

    #[tokio::test]
    async fn test_notification_after_zero_stream_id() {
        let options = Options::default();
        let cache = ChunkCache::new(options);
        let mut rx = cache.take_new_playlists_rx().expect("receiver already taken");

        let first_idx = cache.add_stream_id(9).await;
        let (stream_id, notified_idx) = rx.recv().await.expect("missing notification");

        assert_eq!(stream_id, 9);
        assert_eq!(notified_idx, first_idx);

        cache.zero_stream_id(9).await;

        let second_idx = cache.add_stream_id(9).await;
        let (stream_id, notified_idx) =
            rx.recv().await.expect("missing notification after reset");

        assert_eq!(stream_id, 9);
        assert_eq!(notified_idx, second_idx);
    }
}
