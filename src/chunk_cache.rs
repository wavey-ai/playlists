use crate::Options;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex as StdMutex, RwLock as StdRwLock};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

const EMPTY_LAST: usize = usize::MAX;

pub struct ChunkCache {
    buffer: Vec<RwLock<Bytes>>,
    slot_ids: Vec<AtomicUsize>,
    slot_generations: Vec<AtomicUsize>,
    slot_hashes: Vec<AtomicU64>,
    idxs: Vec<AtomicUsize>,
    next_ids: Vec<AtomicUsize>,
    generations: Vec<AtomicUsize>,
    offsets: StdRwLock<HashMap<u64, usize>>,
    idx: AtomicUsize,
    new_playlist_tx: mpsc::UnboundedSender<(u64, usize)>,
    new_playlist_rx: StdMutex<Option<mpsc::UnboundedReceiver<(u64, usize)>>>,
    pub options: Options,
}

impl ChunkCache {
    pub fn new(options: Options) -> Self {
        let options = options.normalized();
        let num_playlists: usize = options.num_playlists;

        let buffer_size = options
            .num_playlists
            .checked_mul(options.max_parts_per_segment)
            .and_then(|n| n.checked_mul(options.max_segments))
            .expect("chunk cache buffer size overflow");
        let buffer = (0..buffer_size)
            .map(|_| RwLock::new(Bytes::new()))
            .collect();
        let slot_ids = (0..buffer_size)
            .map(|_| AtomicUsize::new(EMPTY_LAST))
            .collect();
        let slot_generations = (0..buffer_size).map(|_| AtomicUsize::new(0)).collect();
        let slot_hashes = (0..buffer_size).map(|_| AtomicU64::new(0)).collect();
        let idxs = (0..num_playlists)
            .map(|_| AtomicUsize::new(EMPTY_LAST))
            .collect();
        let next_ids = (0..num_playlists).map(|_| AtomicUsize::new(1)).collect();
        let generations = (0..num_playlists).map(|_| AtomicUsize::new(1)).collect();
        let (new_playlist_tx, new_playlist_rx) = mpsc::unbounded_channel();

        Self {
            buffer,
            slot_ids,
            slot_generations,
            slot_hashes,
            idxs,
            next_ids,
            generations,
            offsets: StdRwLock::new(HashMap::new()),
            idx: AtomicUsize::new(0),
            new_playlist_tx,
            new_playlist_rx: StdMutex::new(Some(new_playlist_rx)),
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
        let mut lock = self.offsets.write().unwrap();
        if let Some(idx) = lock.get(&stream_id).copied() {
            return idx;
        }

        let idx = self.idx.fetch_add(1, Ordering::SeqCst) % self.options.num_playlists;
        if let Some(previous_stream_id) = lock
            .iter()
            .find_map(|(candidate, mapped_idx)| (*mapped_idx == idx).then_some(*candidate))
        {
            lock.remove(&previous_stream_id);
        }
        self.reset_stream_idx(idx);
        lock.insert(stream_id, idx);
        drop(lock);
        let _ = self.new_playlist_tx.send((stream_id, idx));
        idx
    }

    pub async fn get_stream_idx(&self, stream_id: u64) -> Option<usize> {
        let lock = self.offsets.read().unwrap();
        lock.get(&stream_id).copied()
    }

    pub async fn stream_ids(&self) -> Vec<(u64, usize)> {
        let lock = self.offsets.read().unwrap();
        lock.iter()
            .map(|(stream_id, idx)| (*stream_id, *idx))
            .collect()
    }

    pub async fn add_for_stream_id(
        &self,
        stream_id: u64,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<usize, &'static str> {
        let _ = self.get_or_create_stream_idx(stream_id).await;
        let (stream_idx, generation) = self
            .stream_generation_for_id(stream_id)
            .ok_or("Stream not found")?;
        self.add_with_generation(stream_idx, generation, id, data_bytes)
            .await?;
        Ok(stream_idx)
    }

    pub async fn get_for_stream_id(&self, stream_id: u64, id: usize) -> Option<(Bytes, u64)> {
        let (stream_idx, generation) = self.stream_generation_for_id(stream_id)?;
        self.get_with_generation(stream_idx, generation, id).await
    }

    pub async fn set(&self, stream_idx: usize, id: usize, data: Bytes) -> Result<(), &'static str> {
        let generation = self
            .generation(stream_idx)
            .ok_or("Stream index out of bounds")?;
        self.set_with_generation(stream_idx, generation, id, data)
            .await
    }

    async fn set_with_generation(
        &self,
        stream_idx: usize,
        generation: usize,
        id: usize,
        data: Bytes,
    ) -> Result<(), &'static str> {
        let idx = self
            .offset(stream_idx, id)
            .ok_or("Stream index out of bounds")?;
        let h = const_xxh3(&data);

        let mut lock = self.buffer[idx].write().await;
        *lock = data;
        self.slot_ids[idx].store(id, Ordering::Release);
        self.slot_generations[idx].store(generation, Ordering::Release);
        self.slot_hashes[idx].store(h, Ordering::Release);
        Ok(())
    }

    pub async fn zero_stream_id(&self, stream_id: u64) {
        self.zero_stream_id_sync(stream_id);
    }

    pub fn zero_stream_id_sync(&self, stream_id: u64) {
        let mut offsets_lock = self.offsets.write().unwrap();
        if let Some(offset) = offsets_lock.remove(&stream_id) {
            self.reset_stream_idx(offset);
        }
    }

    pub async fn append(&self, stream_idx: usize, data_bytes: Bytes) -> Result<(), &'static str> {
        let generation = self
            .generation(stream_idx)
            .ok_or("Stream index out of bounds")?;
        let next_id = self
            .next_ids
            .get(stream_idx)
            .ok_or("Stream index out of bounds")?;
        let id = next_id.fetch_add(1, Ordering::AcqRel);
        if id == EMPTY_LAST {
            return Err("Stream id overflow");
        }
        self.add_with_generation(stream_idx, generation, id, data_bytes)
            .await
    }

    pub async fn add(
        &self,
        stream_idx: usize,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<(), &'static str> {
        let generation = self
            .generation(stream_idx)
            .ok_or("Stream index out of bounds")?;
        self.add_with_generation(stream_idx, generation, id, data_bytes)
            .await
    }

    async fn add_with_generation(
        &self,
        stream_idx: usize,
        generation: usize,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<(), &'static str> {
        self.set_with_generation(stream_idx, generation, id, data_bytes)
            .await?;
        if self.generation(stream_idx) != Some(generation) {
            return Err("Stream index changed");
        }
        self.advance_next_id(stream_idx, id.saturating_add(1));
        self.publish_last(stream_idx, id);

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
        let val = self.idxs.get(stream_idx)?.load(Ordering::Acquire);
        (val != EMPTY_LAST).then_some(val)
    }

    pub async fn get(&self, stream_idx: usize, id: usize) -> Option<(Bytes, u64)> {
        let generation = self.generation(stream_idx)?;
        self.get_with_generation(stream_idx, generation, id).await
    }

    async fn get_with_generation(
        &self,
        stream_idx: usize,
        generation: usize,
        id: usize,
    ) -> Option<(Bytes, u64)> {
        let idx = self.offset(stream_idx, id)?;
        let bytes = self.buffer[idx].read().await;
        let stored_id = self.slot_ids[idx].load(Ordering::Acquire);
        let stored_generation = self.slot_generations[idx].load(Ordering::Acquire);
        let hash = self.slot_hashes[idx].load(Ordering::Acquire);
        if stored_id == id && stored_generation == generation {
            Some((bytes.clone(), hash))
        } else {
            None
        }
    }

    pub fn retained_start(&self, last: usize) -> usize {
        last.saturating_sub(self.stream_capacity().saturating_sub(1))
    }

    fn stream_capacity(&self) -> usize {
        self.options.max_parts_per_segment * self.options.max_segments
    }

    fn generation(&self, stream_idx: usize) -> Option<usize> {
        self.generations
            .get(stream_idx)
            .map(|generation| generation.load(Ordering::Acquire))
    }

    fn stream_generation_for_id(&self, stream_id: u64) -> Option<(usize, usize)> {
        let lock = self.offsets.read().unwrap();
        let stream_idx = lock.get(&stream_id).copied()?;
        let generation = self.generation(stream_idx)?;
        Some((stream_idx, generation))
    }

    fn reset_stream_idx(&self, stream_idx: usize) {
        if let Some(last) = self.idxs.get(stream_idx) {
            last.store(EMPTY_LAST, Ordering::Release);
        }
        if let Some(next_id) = self.next_ids.get(stream_idx) {
            next_id.store(1, Ordering::Release);
        }
        if let Some(generation) = self.generations.get(stream_idx) {
            let next = generation.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
            if next == 0 {
                generation.store(1, Ordering::Release);
            }
        }
    }

    fn offset(&self, stream_idx: usize, id: usize) -> Option<usize> {
        if stream_idx >= self.options.num_playlists {
            return None;
        }
        let sub_buffer_size = self.options.max_parts_per_segment * self.options.max_segments;
        stream_idx
            .checked_mul(sub_buffer_size)
            .and_then(|result| result.checked_add(id % sub_buffer_size))
            .filter(|idx| *idx < self.buffer.len())
    }

    fn advance_next_id(&self, stream_idx: usize, next: usize) {
        let Some(next_id) = self.next_ids.get(stream_idx) else {
            return;
        };
        let mut current = next_id.load(Ordering::Acquire);
        while current < next {
            match next_id.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn publish_last(&self, stream_idx: usize, id: usize) {
        let Some(last) = self.idxs.get(stream_idx) else {
            return;
        };
        let mut current = last.load(Ordering::Acquire);
        loop {
            if current != EMPTY_LAST && current >= id {
                return;
            }
            match last.compare_exchange_weak(current, id, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
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

        println!("Starting max read test for {TEST_DURATION_SECS}s, {NUM_READERS} readers");

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
                    if cache_clone.last(stream_idx).is_some() {
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
        println!("Total reads: {total_reads}");
        println!("Total writes: {total_writes}");
        println!("Reads/second: {reads_per_sec:.2}");
        println!("Writes/second: {writes_per_sec:.2}");
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
        println!(
            "Duration: {TEST_DURATION_SECS}s, Streams: {NUM_STREAMS} (1 writer + 2 readers each)"
        );

        let options = Options {
            num_playlists: NUM_STREAMS,
            buffer_size_kb: 64,
            max_parts_per_segment: 10000,
            ..Options::default()
        };
        let cache = Arc::new(ChunkCache::new(options));

        let read_count = Arc::new(AtomicU64::new(0));
        let write_count = Arc::new(AtomicU64::new(0));
        let read_bytes = Arc::new(AtomicU64::new(0));
        let write_bytes = Arc::new(AtomicU64::new(0));

        // Pre-create streams
        for i in 0..NUM_STREAMS {
            cache.get_or_create_stream_idx(i as u64).await;
            // Seed with initial data so readers have something
            cache
                .append(i, Bytes::from(vec![0u8; 64 * 1024]))
                .await
                .ok();
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
        let read_throughput_mb =
            (total_read_bytes as f64 / 1024.0 / 1024.0) / TEST_DURATION_SECS as f64;
        let write_throughput_mb =
            (total_write_bytes as f64 / 1024.0 / 1024.0) / TEST_DURATION_SECS as f64;

        println!("\n=== Results ===");
        println!("Writers: {NUM_STREAMS} streams");
        println!("Readers: {} total ({} per stream)", NUM_STREAMS * 2, 2);
        println!("Write: {writes_per_sec:.0}/s ({write_throughput_mb:.0} MB/s)");
        println!("Read:  {reads_per_sec:.0}/s ({read_throughput_mb:.0} MB/s)");
        println!(
            "Combined throughput: {:.0} MB/s",
            read_throughput_mb + write_throughput_mb
        );
    }

    #[tokio::test]
    async fn test_massive_concurrent_reads() {
        const TEST_DURATION_SECS: u64 = 3;
        const NUM_READERS: usize = 1000;
        const STREAM_ID: u64 = 1;

        println!("\n=== Massive Concurrent Reads Benchmark ===");
        println!("Duration: {TEST_DURATION_SECS}s, Readers: {NUM_READERS}, Writers: 1");

        let options = Options::default();
        let cache = Arc::new(ChunkCache::new(options));
        let read_count = Arc::new(AtomicU64::new(0));
        let write_count = Arc::new(AtomicU64::new(0));

        let stream_idx = cache.get_or_create_stream_idx(STREAM_ID).await;
        // Seed with data
        for i in 1..=100 {
            cache
                .add(stream_idx, i, Bytes::from(vec![0xABu8; 1024]))
                .await
                .ok();
        }

        let mut handles = Vec::new();

        // Single writer
        let cache_clone = Arc::clone(&cache);
        let write_count_clone = Arc::clone(&write_count);
        handles.push(task::spawn(async move {
            let start = Instant::now();
            let data = Bytes::from(vec![0xCDu8; 1024]);
            while start.elapsed().as_secs() < TEST_DURATION_SECS {
                cache_clone.append(stream_idx, data.clone()).await.ok();
                write_count_clone.fetch_add(1, Ordering::Relaxed);
            }
        }));

        // Many concurrent readers
        for _ in 0..NUM_READERS {
            let cache_clone = Arc::clone(&cache);
            let read_count_clone = Arc::clone(&read_count);

            handles.push(task::spawn(async move {
                let start = Instant::now();
                let mut slot = 1usize;
                while start.elapsed().as_secs() < TEST_DURATION_SECS {
                    if let Some(last) = cache_clone.last(stream_idx) {
                        let retained_start = cache_clone.retained_start(last);
                        if slot < retained_start || slot > last {
                            slot = retained_start;
                        }
                    }
                    if cache_clone.get(stream_idx, slot).await.is_some() {
                        read_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    slot = slot.saturating_add(1);
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let total_reads = read_count.load(Ordering::Relaxed);
        let total_writes = write_count.load(Ordering::Relaxed);
        let reads_per_sec = total_reads as f64 / TEST_DURATION_SECS as f64;

        println!("\n=== Results ===");
        println!("Concurrent readers: {NUM_READERS}");
        println!(
            "Total reads: {total_reads} ({:.1}M/s)",
            reads_per_sec / 1_000_000.0
        );
        println!("Total writes: {total_writes}");
        println!(
            "Reads per reader: {:.0}",
            total_reads as f64 / NUM_READERS as f64
        );
    }

    #[tokio::test]
    async fn test_new_playlist_notification_sent() {
        let options = Options::default();
        let cache = ChunkCache::new(options);
        let mut rx = cache
            .take_new_playlists_rx()
            .expect("receiver already taken");

        let idx = cache.add_stream_id(42).await;
        let (stream_id, notified_idx) = rx.recv().await.expect("missing notification");

        assert_eq!(stream_id, 42);
        assert_eq!(notified_idx, idx);
    }

    #[tokio::test]
    async fn test_no_notification_for_existing_stream_id() {
        let options = Options::default();
        let cache = ChunkCache::new(options);
        let mut rx = cache
            .take_new_playlists_rx()
            .expect("receiver already taken");

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
        let mut rx = cache
            .take_new_playlists_rx()
            .expect("receiver already taken");

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
        let mut rx = cache
            .take_new_playlists_rx()
            .expect("receiver already taken");

        let first_idx = cache.add_stream_id(9).await;
        let (stream_id, notified_idx) = rx.recv().await.expect("missing notification");

        assert_eq!(stream_id, 9);
        assert_eq!(notified_idx, first_idx);

        cache.zero_stream_id(9).await;

        let second_idx = cache.add_stream_id(9).await;
        let (stream_id, notified_idx) = rx.recv().await.expect("missing notification after reset");

        assert_eq!(stream_id, 9);
        assert_eq!(notified_idx, second_idx);
    }

    #[tokio::test]
    async fn old_logical_ids_do_not_read_overwritten_slots() {
        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 2,
            ..Options::default()
        };
        let cache = ChunkCache::new(options);
        let stream_idx = cache.add_stream_id(1).await;

        cache
            .add(stream_idx, 0, Bytes::from_static(b"slot-0"))
            .await
            .unwrap();
        cache
            .add(stream_idx, 1, Bytes::from_static(b"slot-1"))
            .await
            .unwrap();
        cache
            .add(stream_idx, 2, Bytes::from_static(b"slot-2"))
            .await
            .unwrap();

        assert!(cache.get(stream_idx, 0).await.is_none());
        assert_eq!(
            cache.get(stream_idx, 2).await.unwrap().0,
            Bytes::from_static(b"slot-2")
        );
    }

    #[tokio::test]
    async fn reused_stream_slot_does_not_expose_previous_stream_data() {
        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        };
        let cache = ChunkCache::new(options);

        let first_idx = cache.add_stream_id(1).await;
        cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();

        let second_idx = cache.add_stream_id(2).await;
        assert_eq!(first_idx, second_idx);
        assert!(cache.get_for_stream_id(1, 0).await.is_none());
        assert!(cache.get_for_stream_id(2, 0).await.is_none());

        cache
            .add_for_stream_id(2, 0, Bytes::from_static(b"second"))
            .await
            .unwrap();
        assert_eq!(
            cache.get_for_stream_id(2, 0).await.unwrap().0,
            Bytes::from_static(b"second")
        );
    }

    #[tokio::test]
    async fn invalid_stream_idx_is_rejected_without_panic() {
        let cache = ChunkCache::new(Options::default());

        assert!(cache.last(usize::MAX).is_none());
        assert!(cache.get(usize::MAX, 0).await.is_none());
        assert!(cache
            .add(usize::MAX, 0, Bytes::from_static(b"bad"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn concurrent_appends_publish_unique_monotonic_ids() {
        const WRITERS: usize = 8;
        const WRITES_PER_WRITER: usize = 32;

        let cache = Arc::new(ChunkCache::new(Options::default()));
        let stream_idx = cache.add_stream_id(1).await;
        let mut handles = Vec::new();

        for _ in 0..WRITERS {
            let cache = Arc::clone(&cache);
            handles.push(task::spawn(async move {
                for _ in 0..WRITES_PER_WRITER {
                    cache
                        .append(stream_idx, Bytes::from_static(b"part"))
                        .await
                        .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let expected_last = WRITERS * WRITES_PER_WRITER;
        assert_eq!(cache.last(stream_idx), Some(expected_last));
        for id in 1..=expected_last {
            assert_eq!(
                cache.get(stream_idx, id).await.unwrap().0,
                Bytes::from_static(b"part")
            );
        }
    }
}
