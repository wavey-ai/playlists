use crate::stream_registry::{ResolvedStream, StreamRegistry};
use crate::{CacheError, Options};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock, Weak};
use tokio::sync::{Notify, RwLock};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

const MAX_EXACT_PART_WAITERS: usize = 65_536;
const EXACT_PART_WAITER_SHARDS: usize = 64;
const MAX_EXACT_PART_WAITERS_PER_SHARD: usize =
    MAX_EXACT_PART_WAITERS.div_ceil(EXACT_PART_WAITER_SHARDS);

type ExactPartKey = (u64, u64, usize);
type ExactPartWaiterShard = StdMutex<HashMap<ExactPartKey, Weak<Notify>>>;

/// Cache-owned identity for one logical chunk stream.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StreamHandle(ResolvedStream);

impl StreamHandle {
    pub fn stream_id(self) -> u64 {
        self.0.stream_id()
    }

    pub fn index(self) -> usize {
        self.0.index()
    }

    pub fn generation(self) -> u64 {
        self.0.generation()
    }
}

/// Outcome of an immutable write at a stream/slot identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutIfAbsentResult {
    /// The slot identity was vacant and now contains the supplied bytes.
    Inserted,
    /// The slot identity already contained the exact same bytes.
    AlreadyPresent,
    /// The slot identity already contained different bytes.
    HashConflict,
    /// A newer ring position already occupies the physical slot.
    Superseded,
}

/// Bounded exact-part waiter state retained by the rolling chunk cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactPartWaiterStats {
    /// Stream/sequence identities still represented in the sharded waiter maps.
    pub retained_keys: usize,
    /// Strong waiter handles currently held by blocked requests.
    pub active_registrations: usize,
    /// Hard upper bound across every waiter shard.
    pub capacity: usize,
}

/// Snapshot of payload memory retained by the fixed chunk ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkCacheMemoryStats {
    pub occupied_slots: usize,
    pub chunk_bytes: usize,
    pub initialization_bytes: usize,
    pub maximum_payload_bytes: usize,
}

#[derive(Clone)]
struct StreamInitialization {
    stream_id: u64,
    generation: u64,
    bytes: Bytes,
}

#[derive(Default)]
struct ChunkSlot {
    id: Option<usize>,
    generation: u64,
    hash: u64,
    bytes: Bytes,
}

struct ChunkStreamState {
    last: Option<usize>,
    next_id: usize,
    version: u64,
}

impl Default for ChunkStreamState {
    fn default() -> Self {
        Self {
            last: None,
            next_id: 1,
            version: 1,
        }
    }
}

pub struct ChunkCache {
    buffer: Vec<RwLock<ChunkSlot>>,
    stream_states: Vec<StdRwLock<ChunkStreamState>>,
    stream_initializations: Vec<StdRwLock<Option<StreamInitialization>>>,
    registry: StreamRegistry,
    exact_part_waiters: Vec<ExactPartWaiterShard>,
    pub options: Options,
}

impl ChunkCache {
    pub fn new(options: Options) -> Self {
        Self::try_new(options).expect("valid chunk cache capacity")
    }

    pub fn try_new(options: Options) -> Result<Self, CacheError> {
        let options = options.normalized();
        let buffer_size = options
            .num_playlists
            .checked_mul(options.max_parts_per_segment)
            .and_then(|n| n.checked_mul(options.max_segments))
            .ok_or(CacheError::ArithmeticOverflow)?;
        options
            .buffer_size_kb
            .checked_mul(1024)
            .ok_or(CacheError::ArithmeticOverflow)?;

        Ok(Self {
            buffer: (0..buffer_size)
                .map(|_| RwLock::new(ChunkSlot::default()))
                .collect(),
            stream_states: (0..options.num_playlists)
                .map(|_| StdRwLock::new(ChunkStreamState::default()))
                .collect(),
            stream_initializations: (0..options.num_playlists)
                .map(|_| StdRwLock::new(None))
                .collect(),
            registry: StreamRegistry::new(options.num_playlists),
            exact_part_waiters: (0..EXACT_PART_WAITER_SHARDS)
                .map(|_| StdMutex::new(HashMap::new()))
                .collect(),
            options,
        })
    }

    /// Resolve a logical stream once for repeated cache operations.
    pub fn resolve_stream(&self, stream_id: u64) -> Option<StreamHandle> {
        self.registry.resolve(stream_id).map(StreamHandle)
    }

    /// Resolve a logical stream or atomically assign a reusable physical slot.
    pub async fn resolve_or_create_stream(&self, stream_id: u64) -> StreamHandle {
        self.resolve_or_create_stream_sync(stream_id)
    }

    fn resolve_or_create_stream_sync(&self, stream_id: u64) -> StreamHandle {
        StreamHandle(
            self.registry
                .resolve_or_create(stream_id, |index, generation| {
                    self.reset_index_state(index, generation);
                }),
        )
    }

    /// Return the physical index that is assigned to a logical stream.
    ///
    /// Do not use the returned value with raw-index data operations. Retain the
    /// [`StreamHandle`] from [`Self::resolve_or_create_stream`] for data access.
    pub async fn get_or_create_stream_idx(&self, stream_id: u64) -> usize {
        self.resolve_or_create_stream_sync(stream_id).index()
    }

    /// Legacy alias for [`Self::get_or_create_stream_idx`].
    pub async fn add_stream_id(&self, stream_id: u64) -> usize {
        self.resolve_or_create_stream_sync(stream_id).index()
    }

    pub async fn get_stream_idx(&self, stream_id: u64) -> Option<usize> {
        self.resolve_stream(stream_id).map(StreamHandle::index)
    }

    pub async fn stream_ids(&self) -> Vec<(u64, usize)> {
        let mut streams = self
            .registry
            .streams()
            .into_iter()
            .map(|stream| (stream.stream_id(), stream.index()))
            .collect::<Vec<_>>();
        streams.sort_unstable();
        streams
    }

    pub fn stream_handles(&self) -> Vec<StreamHandle> {
        let mut streams = self
            .registry
            .streams()
            .into_iter()
            .map(StreamHandle)
            .collect::<Vec<_>>();
        streams.sort_unstable_by_key(|stream| stream.stream_id());
        streams
    }

    pub async fn add_for_stream_id(
        &self,
        stream_id: u64,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<usize, &'static str> {
        self.validate_payload(&data_bytes)?;
        let handle = self.resolve_or_create_stream_sync(stream_id);
        self.add_for_handle(handle, id, data_bytes).await?;
        Ok(handle.index())
    }

    pub async fn add_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<(), &'static str> {
        self.add_slot_for_handle(handle, id, data_bytes).await?;
        self.notify_exact_part_waiters(handle, id);
        Ok(())
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
        self.validate_payload(&data_bytes)?;
        let handle = self.resolve_or_create_stream_sync(stream_id);
        self.put_if_absent_for_handle(handle, id, data_bytes).await
    }

    pub async fn put_if_absent_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<PutIfAbsentResult, &'static str> {
        let result = self
            .put_slot_if_absent_for_handle(handle, id, data_bytes, true)
            .await?;
        if matches!(
            result,
            PutIfAbsentResult::Inserted | PutIfAbsentResult::AlreadyPresent
        ) {
            self.notify_exact_part_waiters(handle, id);
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
        self.validate_payload(&data_bytes)?;
        let handle = self.resolve_or_create_stream_sync(stream_id);
        self.put_if_absent_contiguous_for_handle(handle, id, first_expected_id, data_bytes)
            .await
    }

    pub async fn put_if_absent_contiguous_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        first_expected_id: usize,
        data_bytes: Bytes,
    ) -> Result<PutIfAbsentResult, &'static str> {
        let result = self
            .put_slot_if_absent_for_handle(handle, id, data_bytes, false)
            .await?;
        if matches!(
            result,
            PutIfAbsentResult::Inserted | PutIfAbsentResult::AlreadyPresent
        ) {
            self.advance_next_for_handle(handle, id.saturating_add(1))?;
            self.publish_contiguous_last(handle, first_expected_id)
                .await?;
            self.notify_exact_part_waiters(handle, id);
        }
        Ok(result)
    }

    /// Register for one exact stream/part identity becoming readable.
    ///
    /// Callers must register, enable the returned notification, and then
    /// recheck the cache before awaiting it. This closes the lookup/register
    /// race without waking unrelated LL-HLS requests for every cache commit.
    pub fn exact_part_waiter(&self, stream_id: u64, id: usize) -> Option<Arc<Notify>> {
        // A read-side waiter must not create a mapping because an arbitrary
        // unknown stream ID could otherwise evict a live stream.
        let handle = self.resolve_stream(stream_id)?;
        let key = (stream_id, handle.generation(), id);
        let mut waiters = self
            .exact_part_waiters
            .get(Self::exact_part_waiter_shard(stream_id, id))?
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(waiter) = waiters.get(&key).and_then(Weak::upgrade) {
            return Some(waiter);
        }
        // A cancelled request leaves only a dead Weak at this key. Replacing it
        // is O(1); sweeping the shard on every 5 ms registration made live-tail
        // work quadratic in the number of concurrent waiters.
        waiters.remove(&key);
        if waiters.len() >= MAX_EXACT_PART_WAITERS_PER_SHARD {
            waiters.retain(|_, waiter| waiter.strong_count() > 0);
        }
        if waiters.len() >= MAX_EXACT_PART_WAITERS_PER_SHARD {
            return None;
        }
        let waiter = Arc::new(Notify::new());
        waiters.insert(key, Arc::downgrade(&waiter));
        Some(waiter)
    }

    /// Snapshot exact-part waiter occupancy without retaining any waiter.
    ///
    /// Dead weak entries are reported separately from active request handles so
    /// cancellation qualifications can prove both request cleanup and the hard
    /// bound on lazily reclaimed keys.
    pub fn exact_part_waiter_stats(&self) -> ExactPartWaiterStats {
        let mut retained_keys = 0_usize;
        let mut active_registrations = 0_usize;
        for shard in &self.exact_part_waiters {
            let waiters = shard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            retained_keys = retained_keys.saturating_add(waiters.len());
            active_registrations = active_registrations.saturating_add(
                waiters
                    .values()
                    .map(|waiter| waiter.strong_count())
                    .sum::<usize>(),
            );
        }
        ExactPartWaiterStats {
            retained_keys,
            active_registrations,
            capacity: MAX_EXACT_PART_WAITERS,
        }
    }

    /// Scan fixed slots and report retained payload bytes.
    pub async fn memory_stats(&self) -> ChunkCacheMemoryStats {
        let mut occupied_slots = 0_usize;
        let mut chunk_bytes = 0_usize;
        for slot in &self.buffer {
            let slot = slot.read().await;
            if slot.id.is_some() {
                occupied_slots = occupied_slots.saturating_add(1);
                chunk_bytes = chunk_bytes.saturating_add(slot.bytes.len());
            }
        }
        let initialization_bytes = self
            .stream_initializations
            .iter()
            .filter_map(|initialization| initialization.read().ok())
            .filter_map(|initialization| {
                initialization
                    .as_ref()
                    .map(|initialization| initialization.bytes.len())
            })
            .fold(0_usize, usize::saturating_add);
        let per_chunk = self.options.buffer_size_kb.saturating_mul(1024);
        let per_init = self.options.init_size_kb.saturating_mul(1024);
        ChunkCacheMemoryStats {
            occupied_slots,
            chunk_bytes,
            initialization_bytes,
            maximum_payload_bytes: self
                .buffer
                .len()
                .saturating_mul(per_chunk)
                .saturating_add(self.options.num_playlists.saturating_mul(per_init)),
        }
    }

    fn notify_exact_part_waiters(&self, handle: StreamHandle, id: usize) {
        let waiter = self
            .exact_part_waiters
            .get(Self::exact_part_waiter_shard(handle.stream_id(), id))
            .expect("exact-part waiter shard index must be in bounds")
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(handle.stream_id(), handle.generation(), id))
            .and_then(|waiter| waiter.upgrade());
        if let Some(waiter) = waiter {
            waiter.notify_waiters();
        }
    }

    fn exact_part_waiter_shard(stream_id: u64, id: usize) -> usize {
        debug_assert!(EXACT_PART_WAITER_SHARDS.is_power_of_two());
        let mixed =
            stream_id ^ (id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ stream_id.rotate_left(17);
        mixed as usize & (EXACT_PART_WAITER_SHARDS - 1)
    }

    pub async fn get_for_stream_id(&self, stream_id: u64, id: usize) -> Option<(Bytes, u64)> {
        let handle = self.resolve_stream(stream_id)?;
        self.get_for_handle(handle, id).await
    }

    pub async fn get_for_handle(&self, handle: StreamHandle, id: usize) -> Option<(Bytes, u64)> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let idx = self.offset(handle.index(), id)?;
        let slot = if let Ok(slot) = self.buffer[idx].try_read() {
            slot
        } else {
            self.buffer[idx].read().await
        };
        let part = (slot.id == Some(id) && slot.generation == handle.generation())
            .then(|| (slot.bytes.clone(), slot.hash));
        drop(slot);
        self.registry
            .is_current_fast(handle.0)
            .then_some(part)
            .flatten()
    }

    /// Read a contiguous retained range for one logical stream identity.
    ///
    /// The stream mapping and generation are resolved once for the whole
    /// range. Every slot must still contain the requested sequence in that
    /// same generation, and the mapping must remain unchanged through the
    /// read. This makes aggregation all-or-nothing while avoiding one global
    /// stream-map lookup per constituent part.
    pub async fn get_range_for_stream_id(
        &self,
        stream_id: u64,
        first_id: usize,
        count: usize,
    ) -> Option<Vec<(Bytes, u64)>> {
        if count == 0 || count > self.stream_capacity() {
            return None;
        }
        let handle = self.resolve_stream(stream_id)?;
        self.get_range_for_handle(handle, first_id, count).await
    }

    pub async fn get_range_for_handle(
        &self,
        handle: StreamHandle,
        first_id: usize,
        count: usize,
    ) -> Option<Vec<(Bytes, u64)>> {
        if count == 0 || count > self.stream_capacity() {
            return None;
        }
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let last_id = first_id.checked_add(count - 1)?;
        let mut parts = Vec::with_capacity(count);

        for id in first_id..=last_id {
            let idx = self.offset(handle.index(), id)?;
            let slot = if let Ok(slot) = self.buffer[idx].try_read() {
                slot
            } else {
                self.buffer[idx].read().await
            };
            let part = (slot.id == Some(id) && slot.generation == handle.generation())
                .then(|| (slot.bytes.clone(), slot.hash))?;
            parts.push(part);
        }

        self.registry.is_current_fast(handle.0).then_some(parts)
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
        self.validate_initialization_payload(&bytes)?;
        let handle = self.resolve_or_create_stream_sync(stream_id);
        self.set_stream_initialization_for_handle(handle, bytes)
    }

    pub fn set_stream_initialization_for_handle(
        &self,
        handle: StreamHandle,
        bytes: Bytes,
    ) -> Result<(), &'static str> {
        self.validate_initialization_payload(&bytes)?;
        self.registry
            .with_validated(handle.0, || {
                let slot = self
                    .stream_initializations
                    .get(handle.index())
                    .expect("validated stream index must be in bounds");
                let mut slot = slot
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if slot.as_ref().is_some_and(|initialization| {
                    initialization.stream_id == handle.stream_id()
                        && initialization.generation == handle.generation()
                        && initialization.bytes == bytes
                }) {
                    return;
                }
                *slot = Some(StreamInitialization {
                    stream_id: handle.stream_id(),
                    generation: handle.generation(),
                    bytes,
                });
                self.bump_version_locked(handle.index());
            })
            .ok_or("Stale stream handle")
    }

    pub fn stream_initialization(&self, stream_id: u64) -> Option<Bytes> {
        let handle = self.resolve_stream(stream_id)?;
        self.stream_initialization_for_handle(handle)
    }

    pub fn stream_initialization_for_handle(&self, handle: StreamHandle) -> Option<Bytes> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let slot = self
            .stream_initializations
            .get(handle.index())?
            .read()
            .ok()?;
        let initialization = slot.as_ref()?;
        let bytes = (initialization.stream_id == handle.stream_id()
            && initialization.generation == handle.generation())
        .then(|| initialization.bytes.clone());
        drop(slot);
        self.registry
            .is_current_fast(handle.0)
            .then_some(bytes)
            .flatten()
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
        generation: u64,
        id: usize,
        data: Bytes,
    ) -> Result<(), &'static str> {
        self.validate_payload(&data)?;
        let idx = self
            .offset(stream_idx, id)
            .ok_or("Stream index out of bounds")?;
        let h = const_xxh3(&data);

        let mut slot = self.buffer[idx].write().await;
        self.registry
            .with_generation(stream_idx, generation, || {
                if slot.generation == generation && slot.id.is_some_and(|stored| stored > id) {
                    return Err("Chunk id already evicted");
                }
                slot.id = Some(id);
                slot.generation = generation;
                slot.hash = h;
                slot.bytes = data;
                self.bump_version_locked(stream_idx);
                Ok(())
            })
            .ok_or("Stream index changed")?
    }

    pub async fn zero_stream_id(&self, stream_id: u64) {
        self.zero_stream_id_sync(stream_id);
    }

    pub fn zero_stream_id_sync(&self, stream_id: u64) {
        let _ = self.registry.remove_stream(stream_id, |index, generation| {
            self.reset_index_state(index, generation);
        });
    }

    pub async fn append(&self, stream_idx: usize, data_bytes: Bytes) -> Result<(), &'static str> {
        self.validate_payload(&data_bytes)?;
        let generation = self
            .generation(stream_idx)
            .ok_or("Stream index out of bounds")?;
        let id = self.reserve_next_with_generation(stream_idx, generation)?;
        self.add_with_generation(stream_idx, generation, id, data_bytes)
            .await
    }

    pub async fn append_for_handle(
        &self,
        handle: StreamHandle,
        data_bytes: Bytes,
    ) -> Result<usize, &'static str> {
        self.validate_payload(&data_bytes)?;
        let id = self.reserve_next_for_handle(handle)?;
        self.add_for_handle(handle, id, data_bytes).await?;
        Ok(id)
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
        generation: u64,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<(), &'static str> {
        self.validate_payload(&data_bytes)?;
        let idx = self
            .offset(stream_idx, id)
            .ok_or("Stream index out of bounds")?;
        let hash = const_xxh3(&data_bytes);
        let mut slot = self.buffer[idx].write().await;
        self.registry
            .with_generation(stream_idx, generation, || {
                if slot.generation == generation && slot.id.is_some_and(|stored| stored > id) {
                    return Err("Chunk id already evicted");
                }
                slot.id = Some(id);
                slot.generation = generation;
                slot.hash = hash;
                slot.bytes = data_bytes;
                self.update_state_after_slot(stream_idx, id, true, true);
                Ok(())
            })
            .ok_or("Stream index changed")?
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
        let generation = self.generation(stream_idx)?;
        self.registry
            .with_generation(stream_idx, generation, || self.state_last(stream_idx))
            .flatten()
    }

    pub fn version(&self, stream_idx: usize) -> Option<u64> {
        let generation = self.generation(stream_idx)?;
        self.registry
            .with_generation(stream_idx, generation, || self.state_version(stream_idx))
            .flatten()
    }

    pub fn last_for_handle(&self, handle: StreamHandle) -> Option<usize> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let last = self.state_last(handle.index());
        self.registry
            .is_current_fast(handle.0)
            .then_some(last)
            .flatten()
    }

    pub fn version_for_handle(&self, handle: StreamHandle) -> Option<u64> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let version = self.state_version(handle.index());
        self.registry
            .is_current_fast(handle.0)
            .then_some(version)
            .flatten()
    }

    pub async fn get_last_for_handle(&self, handle: StreamHandle) -> Option<(usize, Bytes, u64)> {
        let id = self.last_for_handle(handle)?;
        let (bytes, hash) = self.get_for_handle(handle, id).await?;
        Some((id, bytes, hash))
    }

    pub async fn get(&self, stream_idx: usize, id: usize) -> Option<(Bytes, u64)> {
        let generation = self.generation(stream_idx)?;
        self.get_with_generation(stream_idx, generation, id).await
    }

    async fn get_with_generation(
        &self,
        stream_idx: usize,
        generation: u64,
        id: usize,
    ) -> Option<(Bytes, u64)> {
        let idx = self.offset(stream_idx, id)?;
        let slot = if let Ok(slot) = self.buffer[idx].try_read() {
            slot
        } else {
            self.buffer[idx].read().await
        };
        self.registry
            .with_generation(stream_idx, generation, || {
                (slot.id == Some(id) && slot.generation == generation)
                    .then(|| (slot.bytes.clone(), slot.hash))
            })
            .flatten()
    }

    pub fn retained_start(&self, last: usize) -> usize {
        last.saturating_sub(self.stream_capacity().saturating_sub(1))
    }

    fn stream_capacity(&self) -> usize {
        self.options.max_parts_per_segment * self.options.max_segments
    }

    fn generation(&self, stream_idx: usize) -> Option<u64> {
        self.registry.generation(stream_idx)
    }

    fn state_last(&self, stream_idx: usize) -> Option<usize> {
        self.stream_states
            .get(stream_idx)?
            .read()
            .ok()
            .and_then(|state| state.last)
    }

    fn state_version(&self, stream_idx: usize) -> Option<u64> {
        self.stream_states
            .get(stream_idx)?
            .read()
            .ok()
            .map(|state| state.version)
    }

    /// Reset all published state for a physical stream index.
    ///
    /// This is intended for callers that manage their own logical stream IDs
    /// on top of fixed `ChunkCache` indices. It advances the generation so
    /// retained ring-buffer bytes from the previous logical stream are no
    /// longer visible through `get`, `last`, or initialization lookups.
    pub fn reset_stream_idx(&self, stream_idx: usize) {
        let _ = self.registry.reset_index(stream_idx, |index, generation| {
            self.reset_index_state(index, generation);
        });
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

    fn reset_index_state(&self, stream_idx: usize, _generation: u64) {
        if let Some(initialization) = self.stream_initializations.get(stream_idx) {
            let mut initialization = initialization
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *initialization = None;
        }
        if let Some(state) = self.stream_states.get(stream_idx) {
            let mut state = state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.last = None;
            state.next_id = 1;
            state.version = next_version(state.version);
        }
    }

    pub(crate) fn validate_payload(&self, data: &Bytes) -> Result<(), &'static str> {
        let max_bytes = self
            .options
            .buffer_size_kb
            .checked_mul(1024)
            .ok_or("Chunk payload limit overflow")?;
        (data.len() <= max_bytes)
            .then_some(())
            .ok_or("Chunk payload exceeds configured limit")
    }

    pub(crate) fn validate_initialization_payload(&self, data: &Bytes) -> Result<(), &'static str> {
        let max_bytes = self
            .options
            .init_size_kb
            .checked_mul(1024)
            .ok_or("Initialization payload limit overflow")?;
        (data.len() <= max_bytes)
            .then_some(())
            .ok_or("Initialization payload exceeds configured limit")
    }

    async fn add_slot_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        data: Bytes,
    ) -> Result<(), &'static str> {
        self.validate_payload(&data)?;
        let idx = self
            .offset(handle.index(), id)
            .ok_or("Stream index out of bounds")?;
        let hash = const_xxh3(&data);
        let mut slot = self.buffer[idx].write().await;
        self.registry
            .with_validated(handle.0, || {
                if slot.generation == handle.generation()
                    && slot.id.is_some_and(|stored| stored > id)
                {
                    return Err("Chunk id already evicted");
                }
                slot.id = Some(id);
                slot.generation = handle.generation();
                slot.hash = hash;
                slot.bytes = data;
                self.update_state_after_slot(handle.index(), id, true, true);
                Ok(())
            })
            .ok_or("Stale stream handle")?
    }

    async fn put_slot_if_absent_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        data: Bytes,
        publish: bool,
    ) -> Result<PutIfAbsentResult, &'static str> {
        self.validate_payload(&data)?;
        let idx = self
            .offset(handle.index(), id)
            .ok_or("Stream index out of bounds")?;
        let hash = const_xxh3(&data);
        let mut slot = self.buffer[idx].write().await;
        self.registry
            .with_validated(handle.0, || {
                let result = if slot.id == Some(id) && slot.generation == handle.generation() {
                    if slot.bytes == data {
                        PutIfAbsentResult::AlreadyPresent
                    } else {
                        PutIfAbsentResult::HashConflict
                    }
                } else if slot.generation == handle.generation()
                    && slot.id.is_some_and(|stored| stored > id)
                {
                    PutIfAbsentResult::Superseded
                } else {
                    slot.id = Some(id);
                    slot.generation = handle.generation();
                    slot.hash = hash;
                    slot.bytes = data;
                    PutIfAbsentResult::Inserted
                };
                let inserted = result == PutIfAbsentResult::Inserted;
                let publish = publish
                    && matches!(
                        result,
                        PutIfAbsentResult::Inserted | PutIfAbsentResult::AlreadyPresent
                    );
                if inserted || publish {
                    self.update_state_after_slot(handle.index(), id, inserted, publish);
                }
                result
            })
            .ok_or("Stale stream handle")
    }

    fn update_state_after_slot(
        &self,
        stream_idx: usize,
        id: usize,
        bump_version: bool,
        publish: bool,
    ) {
        let Some(state) = self.stream_states.get(stream_idx) else {
            return;
        };
        let mut state = state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bump_version {
            state.version = next_version(state.version);
        }
        if publish {
            state.next_id = state.next_id.max(id.saturating_add(1));
            state.last = Some(state.last.map_or(id, |last| last.max(id)));
        }
    }

    fn bump_version_locked(&self, stream_idx: usize) {
        if let Some(state) = self.stream_states.get(stream_idx) {
            let mut state = state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.version = next_version(state.version);
        }
    }

    fn reserve_next_for_handle(&self, handle: StreamHandle) -> Result<usize, &'static str> {
        self.registry
            .with_validated(handle.0, || self.reserve_next_locked(handle.index()))
            .ok_or("Stale stream handle")?
    }

    fn reserve_next_with_generation(
        &self,
        stream_idx: usize,
        generation: u64,
    ) -> Result<usize, &'static str> {
        self.registry
            .with_generation(stream_idx, generation, || {
                self.reserve_next_locked(stream_idx)
            })
            .ok_or("Stream index changed")?
    }

    fn reserve_next_locked(&self, stream_idx: usize) -> Result<usize, &'static str> {
        let state = self
            .stream_states
            .get(stream_idx)
            .ok_or("Stream index out of bounds")?;
        let mut state = state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let id = state.next_id;
        state.next_id = state.next_id.checked_add(1).ok_or("Stream id overflow")?;
        Ok(id)
    }

    fn advance_next_for_handle(
        &self,
        handle: StreamHandle,
        next: usize,
    ) -> Result<(), &'static str> {
        self.registry
            .with_validated(handle.0, || self.advance_next_locked(handle.index(), next))
            .ok_or("Stale stream handle")
    }

    fn advance_next_locked(&self, stream_idx: usize, next: usize) {
        if let Some(state) = self.stream_states.get(stream_idx) {
            let mut state = state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.next_id = state.next_id.max(next);
        }
    }

    async fn publish_contiguous_last(
        &self,
        handle: StreamHandle,
        first_expected_id: usize,
    ) -> Result<(), &'static str> {
        let (current, next_id) = self
            .registry
            .with_validated(handle.0, || {
                let state = self.stream_states[handle.index()]
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (state.last, state.next_id)
            })
            .ok_or("Stale stream handle")?;
        let published_next = current
            .and_then(|last| last.checked_add(1))
            .unwrap_or(first_expected_id);
        let retained_start = next_id.saturating_sub(self.stream_capacity());
        let mut candidate = published_next.max(first_expected_id).max(retained_start);
        let mut contiguous_last = current;

        for _ in 0..self.stream_capacity() {
            let slot_idx = self
                .offset(handle.index(), candidate)
                .ok_or("Stream index out of bounds")?;
            let slot = if let Ok(slot) = self.buffer[slot_idx].try_read() {
                slot
            } else {
                self.buffer[slot_idx].read().await
            };
            if slot.id != Some(candidate) || slot.generation != handle.generation() {
                break;
            }
            contiguous_last = Some(candidate);
            let Some(next) = candidate.checked_add(1) else {
                break;
            };
            candidate = next;
        }

        self.registry
            .with_validated(handle.0, || {
                if let Some(contiguous_last) = contiguous_last {
                    let mut state = self.stream_states[handle.index()]
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.last = Some(
                        state
                            .last
                            .map_or(contiguous_last, |last| last.max(contiguous_last)),
                    );
                }
            })
            .ok_or("Stale stream handle")
    }
}

fn next_version(version: u64) -> u64 {
    let next = version.wrapping_add(1);
    if next == 0 {
        1
    } else {
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::task;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn exact_part_waiters_follow_stream_and_sequence_identity() {
        let cache = ChunkCache::new(Options::default());
        cache.resolve_or_create_stream(77).await;
        let first_waiter = cache.exact_part_waiter(77, 0).unwrap();
        let second_waiter = cache.exact_part_waiter(77, 1).unwrap();
        let first = first_waiter.notified();
        let second = second_waiter.notified();
        tokio::pin!(first);
        tokio::pin!(second);
        first.as_mut().enable();
        second.as_mut().enable();
        assert_eq!(cache.exact_part_waiter_stats().retained_keys, 2);
        assert_eq!(cache.exact_part_waiter_stats().active_registrations, 2);

        cache
            .add_for_stream_id(77, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        timeout(Duration::from_millis(100), &mut first)
            .await
            .expect("the exact sequence-zero waiter should wake");
        assert_eq!(cache.exact_part_waiter_stats().retained_keys, 1);
        assert_eq!(cache.exact_part_waiter_stats().active_registrations, 1);
        assert!(timeout(Duration::from_millis(1), &mut second)
            .await
            .is_err());

        cache
            .add_for_stream_id(77, 1, Bytes::from_static(b"second"))
            .await
            .unwrap();
        timeout(Duration::from_millis(100), &mut second)
            .await
            .expect("the exact sequence-one waiter should wake");
        assert_eq!(cache.exact_part_waiter_stats().retained_keys, 0);
        assert_eq!(cache.exact_part_waiter_stats().active_registrations, 0);
    }

    #[test]
    fn exact_part_waiters_share_keys_and_replace_cancelled_registrations() {
        let cache = ChunkCache::new(Options::default());
        cache.resolve_or_create_stream_sync(78);
        let first = cache.exact_part_waiter(78, 12).unwrap();
        let duplicate = cache.exact_part_waiter(78, 12).unwrap();
        assert!(Arc::ptr_eq(&first, &duplicate));
        assert_eq!(
            cache.exact_part_waiter_stats(),
            ExactPartWaiterStats {
                retained_keys: 1,
                active_registrations: 2,
                capacity: MAX_EXACT_PART_WAITERS,
            }
        );

        drop(first);
        drop(duplicate);
        assert_eq!(cache.exact_part_waiter_stats().active_registrations, 0);
        let replacement = cache.exact_part_waiter(78, 12).unwrap();
        let duplicate_replacement = cache.exact_part_waiter(78, 12).unwrap();
        assert!(Arc::ptr_eq(&replacement, &duplicate_replacement));

        let shard = ChunkCache::exact_part_waiter_shard(78, 12);
        assert_eq!(cache.exact_part_waiters[shard].lock().unwrap().len(), 1);
        assert_eq!(
            cache.exact_part_waiter_stats(),
            ExactPartWaiterStats {
                retained_keys: 1,
                active_registrations: 2,
                capacity: MAX_EXACT_PART_WAITERS,
            }
        );
    }

    #[test]
    fn exact_part_waiter_keys_are_distributed_across_shards() {
        let used = (0_u64..256)
            .map(|stream_id| ChunkCache::exact_part_waiter_shard(stream_id, 1_000))
            .collect::<HashSet<_>>();
        assert_eq!(used.len(), EXACT_PART_WAITER_SHARDS);
    }

    #[tokio::test]
    async fn range_read_returns_ordered_slots_across_ring_wrap() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        });

        for id in 2..=5 {
            cache
                .add_for_stream_id(91, id, Bytes::from(format!("part-{id}")))
                .await
                .unwrap();
        }

        let parts = cache.get_range_for_stream_id(91, 2, 4).await.unwrap();
        assert_eq!(
            parts
                .into_iter()
                .map(|(bytes, _hash)| bytes)
                .collect::<Vec<_>>(),
            vec![
                Bytes::from_static(b"part-2"),
                Bytes::from_static(b"part-3"),
                Bytes::from_static(b"part-4"),
                Bytes::from_static(b"part-5"),
            ]
        );
    }

    #[tokio::test]
    async fn range_read_is_all_or_nothing_for_overwritten_or_invalid_ranges() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        });

        for id in 0..=4 {
            cache
                .add_for_stream_id(92, id, Bytes::from(format!("part-{id}")))
                .await
                .unwrap();
        }

        assert!(cache.get_range_for_stream_id(92, 0, 4).await.is_none());
        assert!(cache.get_range_for_stream_id(92, 1, 4).await.is_some());
        assert!(cache.get_range_for_stream_id(92, 1, 0).await.is_none());
        assert!(cache.get_range_for_stream_id(92, 1, 5).await.is_none());
        assert!(cache
            .get_range_for_stream_id(92, usize::MAX, 2)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn range_read_never_crosses_a_reused_stream_generation() {
        let cache = Arc::new(ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        }));
        for id in 0..2 {
            cache
                .add_for_stream_id(93, id, Bytes::from(format!("old-{id}")))
                .await
                .unwrap();
        }

        let blocked_slot = cache.offset(0, 1).unwrap();
        let slot_write = cache.buffer[blocked_slot].write().await;
        let reader = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move { cache.get_range_for_stream_id(93, 0, 2).await })
        };
        tokio::task::yield_now().await;
        cache.add_stream_id(94).await;
        drop(slot_write);

        assert!(reader.await.unwrap().is_none());
        assert!(cache.get_range_for_stream_id(93, 0, 2).await.is_none());

        for id in 0..2 {
            cache
                .add_for_stream_id(94, id, Bytes::from(format!("new-{id}")))
                .await
                .unwrap();
        }
        let new_parts = cache.get_range_for_stream_id(94, 0, 2).await.unwrap();
        assert_eq!(new_parts[0].0, Bytes::from_static(b"new-0"));
        assert_eq!(new_parts[1].0, Bytes::from_static(b"new-1"));
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
        let stream_handle = cache.resolve_or_create_stream(STREAM_ID).await;
        cache
            .add_for_handle(stream_handle, 0, Bytes::from_static(b"seed"))
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
                    let (bytes, hash) = cache
                        .get_for_handle(stream_handle, 0)
                        .await
                        .expect("seed slot");
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
                    .add_for_handle(stream_handle, id, Bytes::from(vec![id as u8; 128]))
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
            cache.get_for_handle(stream_handle, WRITES).await.unwrap().0,
            Bytes::from(vec![WRITES as u8; 128])
        );
    }

    #[tokio::test]
    async fn stream_churn_retains_only_the_configured_mapping_capacity() {
        let cache = ChunkCache::new(Options {
            num_playlists: 2,
            ..Options::default()
        });

        for stream_id in 0..100_000 {
            cache.add_stream_id(stream_id).await;
        }

        let streams = cache.stream_ids().await;
        assert_eq!(streams.len(), 2);
        assert_eq!(streams, vec![(99_998, 0), (99_999, 1)]);
    }

    #[tokio::test]
    async fn old_handle_cannot_write_after_physical_slot_reuse() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 2,
            ..Options::default()
        });
        let old = cache.resolve_or_create_stream(10).await;
        cache
            .add_for_handle(old, 0, Bytes::from_static(b"old"))
            .await
            .unwrap();

        let current = cache.resolve_or_create_stream(11).await;
        cache
            .add_for_handle(current, 0, Bytes::from_static(b"current"))
            .await
            .unwrap();

        assert_eq!(
            cache
                .add_for_handle(old, 0, Bytes::from_static(b"stale"))
                .await,
            Err("Stale stream handle")
        );
        assert_eq!(
            cache.get_for_handle(current, 0).await.unwrap().0,
            Bytes::from_static(b"current")
        );
        assert!(cache.get_for_handle(old, 0).await.is_none());
    }

    #[tokio::test]
    async fn delayed_older_position_cannot_overwrite_newer_ring_slot() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 2,
            ..Options::default()
        });
        let handle = cache.resolve_or_create_stream(12).await;
        cache
            .add_for_handle(handle, 2, Bytes::from_static(b"newer"))
            .await
            .unwrap();

        assert_eq!(
            cache
                .add_for_handle(handle, 0, Bytes::from_static(b"delayed-older"))
                .await,
            Err("Chunk id already evicted")
        );
        assert_eq!(
            cache.get_for_handle(handle, 2).await.unwrap().0,
            Bytes::from_static(b"newer")
        );
        assert_eq!(
            cache
                .put_if_absent_for_handle(handle, 0, Bytes::from_static(b"delayed-older"))
                .await
                .unwrap(),
            PutIfAbsentResult::Superseded
        );
    }

    #[tokio::test]
    async fn payload_memory_plateaus_after_ring_rotations() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            buffer_size_kb: 1,
            ..Options::default()
        });
        let handle = cache.resolve_or_create_stream(14).await;
        for id in 0..100 {
            cache
                .add_for_handle(handle, id, Bytes::from(vec![id as u8; 512]))
                .await
                .unwrap();
        }

        let stats = cache.memory_stats().await;
        assert_eq!(stats.occupied_slots, 4);
        assert_eq!(stats.chunk_bytes, 4 * 512);
        assert!(stats.chunk_bytes + stats.initialization_bytes <= stats.maximum_payload_bytes);
    }

    #[tokio::test]
    async fn exact_part_waiter_does_not_cross_a_stream_generation() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        cache.resolve_or_create_stream(21).await;
        let waiter = cache.exact_part_waiter(21, 3).unwrap();
        let notified = waiter.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        cache.add_stream_id(22).await;
        cache.zero_stream_id(22).await;
        cache
            .add_for_stream_id(21, 3, Bytes::from_static(b"new-generation"))
            .await
            .unwrap();

        assert!(timeout(Duration::from_millis(10), &mut notified)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn waiter_for_unknown_stream_does_not_evict_a_live_stream() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        let live = cache.resolve_or_create_stream(31).await;

        assert!(cache.exact_part_waiter(32, 0).is_none());
        assert_eq!(cache.resolve_stream(31), Some(live));
        assert_eq!(cache.resolve_stream(32), None);
    }

    #[tokio::test]
    async fn rejected_mapped_payloads_do_not_evict_a_live_stream() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            buffer_size_kb: 1,
            init_size_kb: 1,
            ..Options::default()
        });
        let live = cache.resolve_or_create_stream(41).await;
        cache
            .add_for_handle(live, 0, Bytes::from_static(b"live"))
            .await
            .unwrap();

        assert!(cache
            .add_for_stream_id(42, 0, Bytes::from(vec![0_u8; 1025]))
            .await
            .is_err());
        assert!(cache
            .set_stream_initialization(43, Bytes::from(vec![0_u8; 1025]))
            .await
            .is_err());

        assert_eq!(cache.resolve_stream(41), Some(live));
        assert_eq!(cache.resolve_stream(42), None);
        assert_eq!(cache.resolve_stream(43), None);
        assert_eq!(
            cache.get_for_handle(live, 0).await.unwrap().0,
            Bytes::from_static(b"live")
        );
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
        let stream_handle = cache.resolve_or_create_stream(1).await;

        cache
            .add_for_handle(stream_handle, 0, Bytes::from_static(b"slot-0"))
            .await
            .unwrap();
        cache
            .add_for_handle(stream_handle, 1, Bytes::from_static(b"slot-1"))
            .await
            .unwrap();
        cache
            .add_for_handle(stream_handle, 2, Bytes::from_static(b"slot-2"))
            .await
            .unwrap();

        assert!(cache.get_for_handle(stream_handle, 0).await.is_none());
        assert_eq!(
            cache.get_for_handle(stream_handle, 2).await.unwrap().0,
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

    #[test]
    fn constructor_rejects_capacity_arithmetic_overflow() {
        assert!(matches!(
            ChunkCache::try_new(Options {
                num_playlists: usize::MAX,
                max_segments: 2,
                ..Options::default()
            }),
            Err(CacheError::ArithmeticOverflow)
        ));
    }

    #[tokio::test]
    async fn concurrent_appends_publish_unique_monotonic_ids() {
        const WRITERS: usize = 8;
        const WRITES_PER_WRITER: usize = 32;

        let cache = Arc::new(ChunkCache::new(Options::default()));
        let stream_handle = cache.resolve_or_create_stream(1).await;
        let mut handles = Vec::new();

        for _ in 0..WRITERS {
            let cache = Arc::clone(&cache);
            handles.push(task::spawn(async move {
                for _ in 0..WRITES_PER_WRITER {
                    cache
                        .append_for_handle(stream_handle, Bytes::from_static(b"part"))
                        .await
                        .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let expected_last = WRITERS * WRITES_PER_WRITER;
        assert_eq!(cache.last_for_handle(stream_handle), Some(expected_last));
        for id in 1..=expected_last {
            assert_eq!(
                cache.get_for_handle(stream_handle, id).await.unwrap().0,
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
        let first = cache.resolve_or_create_stream(1).await;
        let initial = cache.version_for_handle(first).unwrap();

        cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        let after_write = cache.version_for_handle(first).unwrap();
        assert!(after_write > initial);

        let second = cache.resolve_or_create_stream(2).await;
        assert_eq!(second.index(), first.index());
        assert!(cache.version_for_handle(second).unwrap() > after_write);
    }

    #[tokio::test]
    async fn immutable_put_is_idempotent_and_preserves_original_on_conflict() {
        let cache = ChunkCache::new(Options::default());
        let stream_handle = cache.resolve_or_create_stream(41).await;
        let version_before = cache.version_for_handle(stream_handle).unwrap();

        assert_eq!(
            cache
                .put_if_absent_for_stream_id(41, 7, Bytes::from_static(b"canonical"))
                .await
                .unwrap(),
            PutIfAbsentResult::Inserted
        );
        let version_after_insert = cache.version_for_handle(stream_handle).unwrap();
        assert!(version_after_insert > version_before);

        assert_eq!(
            cache
                .put_if_absent_for_stream_id(41, 7, Bytes::from_static(b"canonical"))
                .await
                .unwrap(),
            PutIfAbsentResult::AlreadyPresent
        );
        assert_eq!(
            cache.version_for_handle(stream_handle),
            Some(version_after_insert)
        );

        assert_eq!(
            cache
                .put_if_absent_for_stream_id(41, 7, Bytes::from_static(b"conflict"))
                .await
                .unwrap(),
            PutIfAbsentResult::HashConflict
        );
        assert_eq!(
            cache.version_for_handle(stream_handle),
            Some(version_after_insert)
        );
        assert_eq!(
            cache.get_for_stream_id(41, 7).await.unwrap().0,
            Bytes::from_static(b"canonical")
        );
        assert_eq!(cache.last_for_handle(stream_handle), Some(7));
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
        let stream_handle = cache.resolve_or_create_stream(61).await;

        assert_eq!(
            cache
                .put_if_absent_contiguous_for_stream_id(61, 1, 0, Bytes::from_static(b"object-1"),)
                .await
                .unwrap(),
            PutIfAbsentResult::Inserted
        );
        assert_eq!(cache.last_for_handle(stream_handle), None);
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
        assert_eq!(cache.last_for_handle(stream_handle), Some(1));
    }

    #[tokio::test]
    async fn contiguous_immutable_publication_uses_an_explicit_subscription_base() {
        let cache = ChunkCache::new(Options::default());
        let stream_handle = cache.resolve_or_create_stream(62).await;

        cache
            .put_if_absent_contiguous_for_stream_id(
                62,
                8_000,
                8_000,
                Bytes::from_static(b"late-join-base"),
            )
            .await
            .unwrap();

        assert_eq!(cache.last_for_handle(stream_handle), Some(8_000));
    }

    #[tokio::test]
    async fn contiguous_publication_resumes_after_a_gap_leaves_the_retained_window() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        });
        let stream_handle = cache.resolve_or_create_stream(63).await;

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

        assert_eq!(cache.last_for_handle(stream_handle), Some(0));

        cache
            .put_if_absent_contiguous_for_stream_id(63, 5, 0, Bytes::from_static(b"object-5"))
            .await
            .unwrap();

        assert!(cache.get_for_stream_id(63, 1).await.is_none());
        assert_eq!(cache.last_for_handle(stream_handle), Some(5));
        assert_eq!(
            cache.get_for_stream_id(63, 5).await.unwrap().0,
            Bytes::from_static(b"object-5")
        );
    }
}
