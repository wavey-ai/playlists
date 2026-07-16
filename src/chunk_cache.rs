use crate::Options;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex as StdMutex, RwLock as StdRwLock};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

const EMPTY_LAST: usize = usize::MAX;

/// Outcome of an immutable write at a stream/slot identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutIfAbsentResult {
    /// The slot identity was vacant and now contains the supplied bytes.
    Inserted,
    /// The slot identity already contained the exact same bytes.
    AlreadyPresent,
    /// The slot identity already contained different bytes.
    HashConflict,
}

#[derive(Clone)]
struct StreamInitialization {
    stream_id: u64,
    generation: usize,
    bytes: Bytes,
}

pub struct ChunkCache {
    buffer: Vec<RwLock<Bytes>>,
    slot_ids: Vec<AtomicUsize>,
    slot_generations: Vec<AtomicUsize>,
    slot_hashes: Vec<AtomicU64>,
    idxs: Vec<AtomicUsize>,
    next_ids: Vec<AtomicUsize>,
    generations: Vec<AtomicUsize>,
    versions: Vec<AtomicU64>,
    stream_initializations: Vec<StdRwLock<Option<StreamInitialization>>>,
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
        let versions = (0..num_playlists).map(|_| AtomicU64::new(1)).collect();
        let stream_initializations = (0..num_playlists).map(|_| StdRwLock::new(None)).collect();
        let (new_playlist_tx, new_playlist_rx) = mpsc::unbounded_channel();

        Self {
            buffer,
            slot_ids,
            slot_generations,
            slot_hashes,
            idxs,
            next_ids,
            generations,
            versions,
            stream_initializations,
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

    /// Store immutable bytes for a canonical stream/slot identity.
    ///
    /// Retries with identical bytes are idempotent. A different payload for an
    /// already-retained identity is reported as a conflict and never replaces
    /// the original bytes.
    pub async fn put_if_absent_for_stream_id(
        &self,
        stream_id: u64,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<PutIfAbsentResult, &'static str> {
        let _ = self.get_or_create_stream_idx(stream_id).await;
        let (stream_idx, generation) = self
            .stream_generation_for_id(stream_id)
            .ok_or("Stream not found")?;
        let result = self
            .put_if_absent_with_generation(stream_idx, generation, id, data_bytes)
            .await?;
        if self.generation(stream_idx) != Some(generation) {
            return Err("Stream index changed");
        }
        if result == PutIfAbsentResult::Inserted {
            self.advance_next_id(stream_idx, id.saturating_add(1));
            self.publish_last(stream_idx, id);
        }
        Ok(result)
    }

    /// Store immutable bytes and publish only the contiguous completed prefix.
    ///
    /// `first_expected_id` is supplied by the subscription/catalog boundary.
    /// An object arriving beyond a gap remains readable by identity while
    /// [`Self::last`] stays at the highest gap-free object. Filling the gap
    /// advances publication across every already-buffered contiguous object.
    pub async fn put_if_absent_contiguous_for_stream_id(
        &self,
        stream_id: u64,
        id: usize,
        first_expected_id: usize,
        data_bytes: Bytes,
    ) -> Result<PutIfAbsentResult, &'static str> {
        let _ = self.get_or_create_stream_idx(stream_id).await;
        let (stream_idx, generation) = self
            .stream_generation_for_id(stream_id)
            .ok_or("Stream not found")?;
        let result = self
            .put_if_absent_with_generation(stream_idx, generation, id, data_bytes)
            .await?;
        if self.generation(stream_idx) != Some(generation) {
            return Err("Stream index changed");
        }
        if result == PutIfAbsentResult::Inserted {
            self.advance_next_id(stream_idx, id.saturating_add(1));
            self.publish_contiguous_last(stream_idx, generation, first_expected_id);
        }
        Ok(result)
    }

    pub async fn get_for_stream_id(&self, stream_id: u64, id: usize) -> Option<(Bytes, u64)> {
        let (stream_idx, generation) = self.stream_generation_for_id(stream_id)?;
        self.get_with_generation(stream_idx, generation, id).await
    }

    /// Store the durable initialization object associated with a stream.
    ///
    /// Initialization bytes live beside the rolling media window so a fresh
    /// replica or late-joining player can still initialize after the media
    /// slot that introduced the codec configuration has been evicted.
    pub async fn set_stream_initialization(
        &self,
        stream_id: u64,
        bytes: Bytes,
    ) -> Result<(), &'static str> {
        let stream_idx = self.get_or_create_stream_idx(stream_id).await;
        let generation = self
            .generation(stream_idx)
            .ok_or("Stream index out of bounds")?;
        let slot = self
            .stream_initializations
            .get(stream_idx)
            .ok_or("Stream index out of bounds")?;
        let mut slot = slot.write().map_err(|_| "Stream initialization poisoned")?;
        if slot.as_ref().is_some_and(|initialization| {
            initialization.stream_id == stream_id
                && initialization.generation == generation
                && initialization.bytes == bytes
        }) {
            return Ok(());
        }
        *slot = Some(StreamInitialization {
            stream_id,
            generation,
            bytes,
        });
        if let Some(version) = self.versions.get(stream_idx) {
            version.fetch_add(1, Ordering::Release);
        }
        Ok(())
    }

    pub fn stream_initialization(&self, stream_id: u64) -> Option<Bytes> {
        let (stream_idx, generation) = self.stream_generation_for_id(stream_id)?;
        let slot = self.stream_initializations.get(stream_idx)?.read().ok()?;
        let initialization = slot.as_ref()?;
        (initialization.stream_id == stream_id && initialization.generation == generation)
            .then(|| initialization.bytes.clone())
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
        if let Some(version) = self.versions.get(stream_idx) {
            version.fetch_add(1, Ordering::Release);
        }
        Ok(())
    }

    async fn put_if_absent_with_generation(
        &self,
        stream_idx: usize,
        generation: usize,
        id: usize,
        data: Bytes,
    ) -> Result<PutIfAbsentResult, &'static str> {
        let idx = self
            .offset(stream_idx, id)
            .ok_or("Stream index out of bounds")?;
        let h = const_xxh3(&data);

        let mut lock = self.buffer[idx].write().await;
        let stored_id = self.slot_ids[idx].load(Ordering::Acquire);
        let stored_generation = self.slot_generations[idx].load(Ordering::Acquire);
        if stored_id == id && stored_generation == generation {
            return if lock.as_ref() == data.as_ref() {
                Ok(PutIfAbsentResult::AlreadyPresent)
            } else {
                Ok(PutIfAbsentResult::HashConflict)
            };
        }

        *lock = data;
        self.slot_ids[idx].store(id, Ordering::Release);
        self.slot_generations[idx].store(generation, Ordering::Release);
        self.slot_hashes[idx].store(h, Ordering::Release);
        if let Some(version) = self.versions.get(stream_idx) {
            version.fetch_add(1, Ordering::Release);
        }
        Ok(PutIfAbsentResult::Inserted)
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

    pub fn version(&self, stream_idx: usize) -> Option<u64> {
        self.versions
            .get(stream_idx)
            .map(|version| version.load(Ordering::Acquire))
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

    /// Reset all published state for a physical stream index.
    ///
    /// This is intended for callers that manage their own logical stream IDs
    /// on top of fixed `ChunkCache` indices. It advances the generation so
    /// retained ring-buffer bytes from the previous logical stream are no
    /// longer visible through `get`, `last`, or initialization lookups.
    pub fn reset_stream_idx(&self, stream_idx: usize) {
        if let Some(initialization) = self.stream_initializations.get(stream_idx) {
            if let Ok(mut initialization) = initialization.write() {
                *initialization = None;
            }
        }
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
        if let Some(version) = self.versions.get(stream_idx) {
            version.fetch_add(1, Ordering::Release);
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

    fn publish_contiguous_last(
        &self,
        stream_idx: usize,
        generation: usize,
        first_expected_id: usize,
    ) {
        let Some(last) = self.idxs.get(stream_idx) else {
            return;
        };
        loop {
            if self.generation(stream_idx) != Some(generation) {
                return;
            }
            let current = last.load(Ordering::Acquire);
            let published_next = if current == EMPTY_LAST {
                first_expected_id
            } else {
                current.saturating_add(1)
            };
            let observed_next = self
                .next_ids
                .get(stream_idx)
                .map(|next| next.load(Ordering::Acquire))
                .unwrap_or(published_next);
            let retained_start = observed_next.saturating_sub(self.stream_capacity());
            let mut candidate = published_next.max(first_expected_id).max(retained_start);
            let mut contiguous_last = current;

            // One scan can advance by at most the retained ring capacity. This
            // keeps completion work bounded even for adversarial object IDs.
            // Once an unresolved gap has fallen behind `retained_start`, the
            // rolling live publication may resume at the first still-retained
            // object. The watermark always points at a real stored object; it
            // never fabricates completeness for a slot that is still retained.
            for _ in 0..self.stream_capacity() {
                let Some(slot) = self.offset(stream_idx, candidate) else {
                    return;
                };
                if self.slot_ids[slot].load(Ordering::Acquire) != candidate
                    || self.slot_generations[slot].load(Ordering::Acquire) != generation
                {
                    break;
                }
                contiguous_last = candidate;
                candidate = candidate.saturating_add(1);
            }

            if contiguous_last == current {
                return;
            }
            match last.compare_exchange_weak(
                current,
                contiguous_last,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(_) => continue,
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
    async fn read_heavy_workload_handles_thousands_more_reads_than_writes() {
        const READERS: usize = 64;
        const READS_PER_READER: usize = 4096;
        const WRITES: usize = 8;
        const STREAM_ID: u64 = 1;

        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 64,
            ..Options::default()
        };
        let cache = Arc::new(ChunkCache::new(options));
        let stream_idx = cache.add_stream_id(STREAM_ID).await;
        cache
            .add(stream_idx, 0, Bytes::from_static(b"seed"))
            .await
            .unwrap();

        let read_count = Arc::new(AtomicU64::new(0));
        let write_count = Arc::new(AtomicU64::new(1));
        let mut handles = Vec::new();

        for _ in 0..READERS {
            let cache = Arc::clone(&cache);
            let read_count = Arc::clone(&read_count);
            handles.push(task::spawn(async move {
                for _ in 0..READS_PER_READER {
                    let (bytes, hash) = cache.get(stream_idx, 0).await.expect("seed slot");
                    assert_eq!(bytes, Bytes::from_static(b"seed"));
                    assert_ne!(hash, 0);
                    read_count.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        let writer_cache = Arc::clone(&cache);
        let writer_count = Arc::clone(&write_count);
        handles.push(task::spawn(async move {
            for id in 1..=WRITES {
                writer_cache
                    .add(stream_idx, id, Bytes::from(vec![id as u8; 128]))
                    .await
                    .unwrap();
                writer_count.fetch_add(1, Ordering::Relaxed);
                task::yield_now().await;
            }
        }));

        for handle in handles {
            handle.await.unwrap();
        }

        let total_reads = read_count.load(Ordering::Relaxed);
        let total_writes = write_count.load(Ordering::Relaxed);
        assert_eq!(total_reads, (READERS * READS_PER_READER) as u64);
        assert_eq!(total_writes, (WRITES + 1) as u64);
        assert!(
            total_reads / total_writes >= 10_000,
            "expected at least 10k reads per write, got {total_reads}/{total_writes}"
        );
        assert_eq!(
            cache.get(stream_idx, WRITES).await.unwrap().0,
            Bytes::from(vec![WRITES as u8; 128])
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
    async fn explicit_stream_idx_reset_hides_previous_slot_data() {
        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        };
        let cache = ChunkCache::new(options);
        let stream_idx = 0;

        cache
            .add(stream_idx, 1, Bytes::from_static(b"headers"))
            .await
            .unwrap();
        cache
            .add(stream_idx, 2, Bytes::from_static(b"body"))
            .await
            .unwrap();

        assert_eq!(cache.last(stream_idx), Some(2));
        assert_eq!(
            cache.get(stream_idx, 2).await.unwrap().0,
            Bytes::from_static(b"body")
        );

        cache.reset_stream_idx(stream_idx);

        assert!(cache.last(stream_idx).is_none());
        assert!(cache.get(stream_idx, 1).await.is_none());
        assert!(cache.get(stream_idx, 2).await.is_none());

        cache
            .add(stream_idx, 1, Bytes::from_static(b"next-headers"))
            .await
            .unwrap();
        assert_eq!(cache.last(stream_idx), Some(1));
        assert_eq!(
            cache.get(stream_idx, 1).await.unwrap().0,
            Bytes::from_static(b"next-headers")
        );
        assert!(cache.get(stream_idx, 2).await.is_none());
    }

    #[tokio::test]
    async fn stream_initialization_survives_media_eviction_and_is_cleared_on_slot_reuse() {
        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 2,
            ..Options::default()
        };
        let cache = ChunkCache::new(options);
        cache
            .set_stream_initialization(1, Bytes::from_static(b"ftyp-moov"))
            .await
            .unwrap();

        for sequence in 0..8 {
            cache
                .add_for_stream_id(1, sequence, Bytes::from(sequence.to_be_bytes().to_vec()))
                .await
                .unwrap();
        }

        assert!(cache.get_for_stream_id(1, 0).await.is_none());
        assert_eq!(
            cache.stream_initialization(1).unwrap(),
            Bytes::from_static(b"ftyp-moov")
        );

        cache.add_stream_id(2).await;
        assert!(cache.stream_initialization(1).is_none());
        assert!(cache.stream_initialization(2).is_none());
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

    #[tokio::test]
    async fn stream_version_advances_on_writes_and_slot_reuse() {
        let options = Options {
            num_playlists: 1,
            ..Options::default()
        };
        let cache = ChunkCache::new(options);
        let stream_idx = cache.add_stream_id(1).await;
        let initial = cache.version(stream_idx).unwrap();

        cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        let after_write = cache.version(stream_idx).unwrap();
        assert!(after_write > initial);

        assert_eq!(cache.add_stream_id(2).await, stream_idx);
        assert!(cache.version(stream_idx).unwrap() > after_write);
    }

    #[tokio::test]
    async fn immutable_put_is_idempotent_and_preserves_original_on_conflict() {
        let cache = ChunkCache::new(Options::default());
        let stream_idx = cache.add_stream_id(41).await;
        let version_before = cache.version(stream_idx).unwrap();

        assert_eq!(
            cache
                .put_if_absent_for_stream_id(41, 7, Bytes::from_static(b"canonical"))
                .await
                .unwrap(),
            PutIfAbsentResult::Inserted
        );
        let version_after_insert = cache.version(stream_idx).unwrap();
        assert!(version_after_insert > version_before);

        assert_eq!(
            cache
                .put_if_absent_for_stream_id(41, 7, Bytes::from_static(b"canonical"))
                .await
                .unwrap(),
            PutIfAbsentResult::AlreadyPresent
        );
        assert_eq!(cache.version(stream_idx), Some(version_after_insert));

        assert_eq!(
            cache
                .put_if_absent_for_stream_id(41, 7, Bytes::from_static(b"conflict"))
                .await
                .unwrap(),
            PutIfAbsentResult::HashConflict
        );
        assert_eq!(cache.version(stream_idx), Some(version_after_insert));
        assert_eq!(
            cache.get_for_stream_id(41, 7).await.unwrap().0,
            Bytes::from_static(b"canonical")
        );
        assert_eq!(cache.last(stream_idx), Some(7));
    }

    #[tokio::test]
    async fn concurrent_immutable_puts_allow_exactly_one_identity_value() {
        let cache = Arc::new(ChunkCache::new(Options::default()));
        cache.add_stream_id(52).await;

        let first = {
            let cache = Arc::clone(&cache);
            task::spawn(async move {
                cache
                    .put_if_absent_for_stream_id(52, 9, Bytes::from_static(b"first"))
                    .await
                    .unwrap()
            })
        };
        let second = {
            let cache = Arc::clone(&cache);
            task::spawn(async move {
                cache
                    .put_if_absent_for_stream_id(52, 9, Bytes::from_static(b"second"))
                    .await
                    .unwrap()
            })
        };

        let outcomes = [first.await.unwrap(), second.await.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == PutIfAbsentResult::Inserted)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == PutIfAbsentResult::HashConflict)
                .count(),
            1
        );
        let retained = cache.get_for_stream_id(52, 9).await.unwrap().0;
        assert!(
            retained == Bytes::from_static(b"first") || retained == Bytes::from_static(b"second")
        );
    }

    #[tokio::test]
    async fn contiguous_immutable_publication_holds_objects_behind_a_gap() {
        let cache = ChunkCache::new(Options::default());
        let stream_idx = cache.add_stream_id(61).await;

        assert_eq!(
            cache
                .put_if_absent_contiguous_for_stream_id(61, 1, 0, Bytes::from_static(b"object-1"),)
                .await
                .unwrap(),
            PutIfAbsentResult::Inserted
        );
        assert_eq!(cache.last(stream_idx), None);
        assert_eq!(
            cache.get_for_stream_id(61, 1).await.unwrap().0,
            Bytes::from_static(b"object-1")
        );

        assert_eq!(
            cache
                .put_if_absent_contiguous_for_stream_id(61, 0, 0, Bytes::from_static(b"object-0"),)
                .await
                .unwrap(),
            PutIfAbsentResult::Inserted
        );
        assert_eq!(cache.last(stream_idx), Some(1));
    }

    #[tokio::test]
    async fn contiguous_immutable_publication_uses_an_explicit_subscription_base() {
        let cache = ChunkCache::new(Options::default());
        let stream_idx = cache.add_stream_id(62).await;

        cache
            .put_if_absent_contiguous_for_stream_id(
                62,
                8_000,
                8_000,
                Bytes::from_static(b"late-join-base"),
            )
            .await
            .unwrap();

        assert_eq!(cache.last(stream_idx), Some(8_000));
    }

    #[tokio::test]
    async fn contiguous_publication_resumes_after_a_gap_leaves_the_retained_window() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        });
        let stream_idx = cache.add_stream_id(63).await;

        cache
            .put_if_absent_contiguous_for_stream_id(63, 0, 0, Bytes::from_static(b"object-0"))
            .await
            .unwrap();
        for sequence in 2..=4 {
            cache
                .put_if_absent_contiguous_for_stream_id(
                    63,
                    sequence,
                    0,
                    Bytes::from(sequence.to_be_bytes().to_vec()),
                )
                .await
                .unwrap();
        }

        assert_eq!(cache.last(stream_idx), Some(0));

        cache
            .put_if_absent_contiguous_for_stream_id(63, 5, 0, Bytes::from_static(b"object-5"))
            .await
            .unwrap();

        assert!(cache.get_for_stream_id(63, 1).await.is_none());
        assert_eq!(cache.last(stream_idx), Some(5));
        assert_eq!(
            cache.get_for_stream_id(63, 5).await.unwrap().0,
            Bytes::from_static(b"object-5")
        );
    }
}
