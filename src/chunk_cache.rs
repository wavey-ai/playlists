use crate::stream_registry::{ResolvedStream, StreamRegistry};
use crate::{CacheError, Options};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock, Weak};
use tokio::sync::{Notify, RwLock};
use xxhash_rust::xxh3::xxh3_64;

/// `last` of a stream with no published part, and the tag of an empty slot.
const EMPTY_ID: usize = usize::MAX;
const MAX_EXACT_PART_WAITERS: usize = 65_536;
const EXACT_PART_WAITER_SHARDS: usize = 64;
const MAX_EXACT_PART_WAITERS_PER_SHARD: usize =
    MAX_EXACT_PART_WAITERS.div_ceil(EXACT_PART_WAITER_SHARDS);

type ExactPartWaiterShard = StdMutex<HashMap<(u64, usize), Weak<Notify>>>;

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

/// The streams whose watermark advanced since a consumer last looked: one
/// bit per stream index. See [`ChunkCache::subscribe_publications`].
pub struct PublishedStreams {
    words: Vec<AtomicU64>,
}

impl PublishedStreams {
    fn new(streams: usize) -> Self {
        Self {
            words: (0..streams.div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect(),
        }
    }

    fn mark(&self, stream_idx: usize) {
        if let Some(word) = self.words.get(stream_idx / 64) {
            word.fetch_or(1 << (stream_idx % 64), Ordering::AcqRel);
        }
    }

    /// Append the stream indexes that advanced since the last call, in
    /// order, and forget them. [`ChunkCache::handle_at`] names the stream
    /// that holds an index now.
    pub fn drain(&self, out: &mut Vec<usize>) {
        for (index, word) in self.words.iter().enumerate() {
            if word.load(Ordering::Acquire) == 0 {
                continue;
            }
            let mut bits = word.swap(0, Ordering::AcqRel);
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                out.push(index * 64 + bit);
                bits &= bits - 1;
            }
        }
    }
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

/// A lock-free mirror of a slot's identity and the time it was written.
///
/// Written only under the slot's write lock, in the order: id = EMPTY, time,
/// generation, id. A reader loads id and generation, then the time, then id
/// and generation again, and trusts the time only if both loads match. The
/// time is therefore readable without a lock, before any reader woken by the
/// write runs.
struct SlotTag {
    id: AtomicUsize,
    generation: AtomicU64,
    written_us: AtomicU64,
}

impl SlotTag {
    fn new() -> Self {
        Self {
            id: AtomicUsize::new(EMPTY_ID),
            generation: AtomicU64::new(0),
            written_us: AtomicU64::new(0),
        }
    }

    fn store(&self, id: usize, generation: u64, written_us: u64) {
        self.id.store(EMPTY_ID, Ordering::Relaxed);
        self.written_us.store(written_us, Ordering::Release);
        self.generation.store(generation, Ordering::Release);
        self.id.store(id, Ordering::Release);
    }

    fn clear(&self) {
        self.id.store(EMPTY_ID, Ordering::Relaxed);
        self.written_us.store(0, Ordering::Release);
    }

    fn holds(&self, id: usize, generation: u64) -> bool {
        id != EMPTY_ID
            && self.id.load(Ordering::Acquire) == id
            && self.generation.load(Ordering::Acquire) == generation
    }
}

/// Per-index publication state. Writers change it only while they hold the
/// registry's owner read lock for the index, and a reset only under its
/// owner write lock, so a value never crosses a generation.
struct StreamState {
    /// The published watermark, or `EMPTY_ID`.
    last: AtomicUsize,
    next_id: AtomicUsize,
    /// Never 0, and never repeats across reuse of the index.
    version: AtomicU64,
}

impl StreamState {
    fn new() -> Self {
        Self {
            last: AtomicUsize::new(EMPTY_ID),
            next_id: AtomicUsize::new(1),
            version: AtomicU64::new(1),
        }
    }

    fn last(&self) -> Option<usize> {
        let last = self.last.load(Ordering::Acquire);
        (last != EMPTY_ID).then_some(last)
    }

    /// Raise the watermark to `id`. Returns whether it strictly advanced.
    fn raise_last(&self, id: usize) -> bool {
        if id == EMPTY_ID {
            return false;
        }
        let mut current = self.last.load(Ordering::Acquire);
        loop {
            if current != EMPTY_ID && current >= id {
                return false;
            }
            match self
                .last
                .compare_exchange_weak(current, id, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn raise_next_id(&self, next: usize) {
        self.next_id.fetch_max(next, Ordering::AcqRel);
    }

    fn bump_version(&self) {
        let mut current = self.version.load(Ordering::Acquire);
        loop {
            match self.version.compare_exchange_weak(
                current,
                next_version(current),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn reserve_next(&self) -> Result<usize, &'static str> {
        let mut current = self.next_id.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(1)
                .filter(|next| *next != EMPTY_ID)
                .ok_or("Stream id overflow")?;
            match self.next_id.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(current),
                Err(observed) => current = observed,
            }
        }
    }

    fn reset(&self) {
        self.last.store(EMPTY_ID, Ordering::Release);
        self.next_id.store(1, Ordering::Release);
        self.bump_version();
    }
}

/// How a slot write treats a newer id of the same generation that already
/// occupies the physical slot.
#[derive(Clone, Copy, Eq, PartialEq)]
enum OlderId {
    /// Origin-local writes: an older id never replaces a newer one.
    Reject,
    /// Mesh-applied writes: the wire carries no generation, so a restarted
    /// origin's parts must replace the old ids they land on.
    Replace,
}

pub struct ChunkCache {
    buffer: Vec<RwLock<ChunkSlot>>,
    slot_tags: Vec<SlotTag>,
    stream_states: Vec<StreamState>,
    stream_initializations: Vec<StdRwLock<Option<StreamInitialization>>>,
    registry: StreamRegistry,
    stream_update_notifiers: Vec<Arc<Notify>>,
    exact_part_waiters: Vec<ExactPartWaiterShard>,
    /// Signalled whenever the watermark of any stream advances. See
    /// [`Self::publication_notify`].
    published: Notify,
    /// Told which stream advanced, before `published` is signalled.
    publication_sets: StdRwLock<Vec<Weak<PublishedStreams>>>,
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
            slot_tags: (0..buffer_size).map(|_| SlotTag::new()).collect(),
            stream_states: (0..options.num_playlists)
                .map(|_| StreamState::new())
                .collect(),
            stream_initializations: (0..options.num_playlists)
                .map(|_| StdRwLock::new(None))
                .collect(),
            registry: StreamRegistry::new(options.num_playlists),
            stream_update_notifiers: (0..options.num_playlists)
                .map(|_| Arc::new(Notify::new()))
                .collect(),
            exact_part_waiters: (0..EXACT_PART_WAITER_SHARDS)
                .map(|_| StdMutex::new(HashMap::new()))
                .collect(),
            published: Notify::new(),
            publication_sets: StdRwLock::new(Vec::new()),
            options,
        })
    }

    /// Signalled with [`Notify::notify_waiters`] each time the watermark of a
    /// stream ([`Self::last_for_handle`]) strictly advances: from `add`,
    /// `append`, `add_for_handle`, `put_if_absent_for_handle`, and from
    /// `put_if_absent_contiguous_for_handle` when the contiguous prefix
    /// grows. A write beyond a gap, a repeat, a conflict, a superseded write,
    /// a cleared slot and a released stream do not signal.
    ///
    /// The signal is sent after the slot and the stream state are unlocked.
    /// No permit is stored. A consumer creates its `notified()` future (and
    /// may `enable()` it) before it reads the watermarks, so that an advance
    /// made while it works wakes its next wait.
    pub fn publication_notify(&self) -> &Notify {
        &self.published
    }

    /// A set of the streams whose watermark advances from now on. Each
    /// advance marks its stream index before [`Self::publication_notify`] is
    /// signalled, so a consumer woken by the signal finds the streams to
    /// visit without scanning every stream. The set lives as long as the
    /// consumer holds it.
    pub fn subscribe_publications(&self) -> Arc<PublishedStreams> {
        let set = Arc::new(PublishedStreams::new(self.options.num_playlists));
        let mut sets = self
            .publication_sets
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        sets.retain(|set| set.strong_count() > 0);
        sets.push(Arc::downgrade(&set));
        set
    }

    /// Mark `stream_idx` in every publication set and wake the consumers.
    fn signal_publication(&self, stream_idx: usize) {
        {
            let sets = self
                .publication_sets
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for set in sets.iter() {
                if let Some(set) = set.upgrade() {
                    set.mark(stream_idx);
                }
            }
        }
        self.published.notify_waiters();
    }

    /// Resolve a logical stream once for repeated cache operations.
    pub fn resolve_stream(&self, stream_id: u64) -> Option<StreamHandle> {
        self.registry.resolve(stream_id).map(StreamHandle)
    }

    /// Resolve a logical stream or atomically assign a reusable physical slot.
    ///
    /// A free index is taken first. Only when every index is in use does a
    /// new stream replace the stream at the next index in turn.
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

    /// The stream that holds `stream_idx` now, without taking a lock.
    ///
    /// This names the indexes that [`PublishedStreams::drain`] returns. It is
    /// `None` for a free index, a raw lane, and while the index is being
    /// reassigned.
    pub fn handle_at(&self, stream_idx: usize) -> Option<StreamHandle> {
        self.registry.handle_at(stream_idx).map(StreamHandle)
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

    /// Return the fixed notification source for writes, initializations and
    /// resets on one lane.
    ///
    /// Enable a `notified` future, recheck the cache, and then await it.
    /// Logical stream callers should prefer [`Self::exact_part_waiter`] when
    /// they know the requested part identity.
    pub fn update_notifier(&self, stream_idx: usize) -> Option<Arc<Notify>> {
        self.stream_update_notifiers.get(stream_idx).cloned()
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
        self.write_for_handle(handle, id, data_bytes, OlderId::Reject)
            .await
    }

    /// Store a part received from a mesh peer.
    ///
    /// Generation-checked against local reuse of the index like
    /// [`Self::add_for_handle`], but an older id replaces a newer one of the
    /// same generation: a restarted origin numbers its parts from the start
    /// again, and the wire carries no generation.
    #[cfg_attr(not(feature = "mesh"), allow(dead_code))]
    pub(crate) async fn add_remote_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        data_bytes: Bytes,
    ) -> Result<(), &'static str> {
        self.write_for_handle(handle, id, data_bytes, OlderId::Replace)
            .await
    }

    async fn write_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        data_bytes: Bytes,
        older: OlderId,
    ) -> Result<(), &'static str> {
        let advanced = self
            .add_slot_for_handle(handle, id, data_bytes, older)
            .await?;
        if advanced {
            self.signal_publication(handle.index());
        }
        self.notify_exact_part_waiters(handle.stream_id(), id);
        self.notify_stream_update(handle.index());
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
        let (result, advanced) = self
            .put_slot_if_absent_for_handle(handle, id, data_bytes, true)
            .await?;
        if advanced {
            self.signal_publication(handle.index());
        }
        if matches!(
            result,
            PutIfAbsentResult::Inserted | PutIfAbsentResult::AlreadyPresent
        ) {
            self.notify_exact_part_waiters(handle.stream_id(), id);
        }
        if result == PutIfAbsentResult::Inserted {
            self.notify_stream_update(handle.index());
        }
        Ok(result)
    }

    /// Store immutable bytes and publish only the contiguous completed prefix.
    ///
    /// `first_expected_id` is supplied by the subscription/catalog boundary.
    /// An object arriving beyond a gap remains readable by identity while
    /// [`Self::last_for_handle`] stays at the highest gap-free object. Filling
    /// the gap advances publication across every already-buffered contiguous
    /// object.
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
        let (result, _) = self
            .put_slot_if_absent_for_handle(handle, id, data_bytes, false)
            .await?;
        if matches!(
            result,
            PutIfAbsentResult::Inserted | PutIfAbsentResult::AlreadyPresent
        ) {
            let advanced = self.publish_contiguous_last(handle, id, first_expected_id)?;
            if advanced {
                self.signal_publication(handle.index());
            }
            self.notify_exact_part_waiters(handle.stream_id(), id);
            self.notify_stream_update(handle.index());
        }
        Ok(result)
    }

    /// Register for one exact stream/part identity becoming readable.
    ///
    /// Callers must register, enable the returned notification, and then
    /// recheck the cache before awaiting it. This closes the lookup/register
    /// race without waking unrelated LL-HLS requests for every cache commit.
    ///
    /// The waiter is keyed by stream ID and part ID only, and registering
    /// creates no mapping: a reader can wait for the first part of a stream
    /// that does not exist yet, and an unknown ID evicts nothing. The first
    /// write of that identity in any generation wakes it, so a woken reader
    /// rechecks with a generation-safe read. `None` only when the waiter
    /// shard is full.
    pub fn exact_part_waiter(&self, stream_id: u64, id: usize) -> Option<Arc<Notify>> {
        let key = (stream_id, id);
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

    fn notify_exact_part_waiters(&self, stream_id: u64, id: usize) {
        let waiter = self
            .exact_part_waiters
            .get(Self::exact_part_waiter_shard(stream_id, id))
            .expect("exact-part waiter shard index must be in bounds")
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&(stream_id, id))
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

    /// When the retained slot of a stream was written into this cache:
    /// CLOCK_REALTIME microseconds since the Unix epoch.
    ///
    /// The time is stored before the slot becomes readable, so a reader woken
    /// by the write (an exact-part waiter, a lane notifier or a watermark
    /// advance) finds it. Takes no lock and does not wait.
    pub fn written_unix_us_for_stream_id(&self, stream_id: u64, id: usize) -> Option<u64> {
        let handle = self.resolve_stream(stream_id)?;
        self.written_unix_us_for_handle(handle, id)
    }

    /// [`Self::written_unix_us_for_stream_id`] for a resolved stream.
    pub fn written_unix_us_for_handle(&self, handle: StreamHandle, id: usize) -> Option<u64> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let tag = self.slot_tags.get(self.offset(handle.index(), id)?)?;
        if !tag.holds(id, handle.generation()) {
            return None;
        }
        let written_us = tag.written_us.load(Ordering::Acquire);
        // A concurrent write of another identity into the slot replaces the
        // time; report nothing rather than its time.
        (written_us != 0
            && tag.holds(id, handle.generation())
            && self.registry.is_current_fast(handle.0))
        .then_some(written_us)
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
        let result = self
            .registry
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
                self.stream_states[handle.index()].bump_version();
            })
            .ok_or("Stale stream handle");
        if result.is_ok() {
            self.notify_stream_update(handle.index());
        }
        result
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

    /// Release the bytes of one retained slot of a stream.
    ///
    /// The ring holds as many slots for every stream as the stream with the
    /// shortest parts needs. A stream with long parts, such as 4K video, uses
    /// this to drop the parts behind its own window instead of keeping the
    /// whole ring of them in memory. The watermark and the version are not
    /// changed and no reader is woken. Returns whether a slot was released.
    pub async fn clear_for_stream_id(&self, stream_id: u64, id: usize) -> bool {
        let Some(handle) = self.resolve_stream(stream_id) else {
            return false;
        };
        self.clear_for_handle(handle, id).await
    }

    /// [`Self::clear_for_stream_id`] for a resolved stream.
    pub async fn clear_for_handle(&self, handle: StreamHandle, id: usize) -> bool {
        let Some(idx) = self.offset(handle.index(), id) else {
            return false;
        };
        let mut slot = self.buffer[idx].write().await;
        self.registry
            .with_validated(handle.0, || {
                if slot.id != Some(id) || slot.generation != handle.generation() {
                    return false;
                }
                self.clear_slot(idx, &mut slot);
                true
            })
            .unwrap_or(false)
    }

    /// Remove a stream and release the bytes of every part that it retained.
    ///
    /// [`Self::zero_stream_id`] only hides the parts: they stay in memory
    /// until another stream overwrites the same slots. A cache that holds a
    /// stream only while it has readers uses this instead, so that a released
    /// 4K stream does not keep its window in memory. The index becomes free
    /// and is taken before any stream is evicted. Returns whether the stream
    /// existed.
    pub async fn release_stream_id(&self, stream_id: u64) -> bool {
        let Some(released) = self.registry.remove_stream(stream_id, |index, generation| {
            self.reset_index_state(index, generation);
        }) else {
            return false;
        };
        self.release_slots_of(released).await;
        true
    }

    /// Empty the lane's slots written by the released generation or an older
    /// one. A stream that took the index since keeps what it wrote:
    /// generations only grow on an index.
    async fn release_slots_of(&self, released: ResolvedStream) {
        for id in 0..self.stream_capacity() {
            let Some(idx) = self.offset(released.index(), id) else {
                break;
            };
            let mut slot = self.buffer[idx].write().await;
            if slot.id.is_some() && slot.generation <= released.generation() {
                self.clear_slot(idx, &mut slot);
            }
        }
    }

    /// Remove a stream. Its parts and initialization are hidden at once and
    /// the index becomes free; the bytes stay until overwritten (see
    /// [`Self::release_stream_id`]).
    pub fn zero_stream_id(&self, stream_id: u64) {
        let _ = self.registry.remove_stream(stream_id, |index, generation| {
            self.reset_index_state(index, generation);
        });
    }

    pub async fn set(&self, stream_idx: usize, id: usize, data: Bytes) -> Result<(), &'static str> {
        let generation = self
            .generation(stream_idx)
            .ok_or("Stream index out of bounds")?;
        self.set_with_generation(stream_idx, generation, id, data)
            .await?;
        self.notify_stream_update(stream_idx);
        Ok(())
    }

    async fn set_with_generation(
        &self,
        stream_idx: usize,
        generation: u64,
        id: usize,
        data: Bytes,
    ) -> Result<(), &'static str> {
        self.validate_payload(&data)?;
        let idx = self.slot_index(stream_idx, id)?;
        let hash = xxh3_64(&data);

        let mut slot = self.buffer[idx].write().await;
        self.registry
            .with_generation(stream_idx, generation, || {
                if slot.generation == generation && slot.id.is_some_and(|stored| stored > id) {
                    return Err("Chunk id already evicted");
                }
                self.store_slot(idx, &mut slot, id, generation, hash, data);
                self.stream_states[stream_idx].bump_version();
                Ok(())
            })
            .ok_or("Stream index changed")?
    }

    pub async fn append(&self, stream_idx: usize, data_bytes: Bytes) -> Result<(), &'static str> {
        self.validate_payload(&data_bytes)?;
        let generation = self
            .generation(stream_idx)
            .ok_or("Stream index out of bounds")?;
        let id = self
            .registry
            .with_generation(stream_idx, generation, || {
                self.stream_states[stream_idx].reserve_next()
            })
            .ok_or("Stream index changed")??;
        self.add_with_generation(stream_idx, generation, id, data_bytes)
            .await
    }

    pub async fn append_for_handle(
        &self,
        handle: StreamHandle,
        data_bytes: Bytes,
    ) -> Result<usize, &'static str> {
        self.validate_payload(&data_bytes)?;
        let id = self
            .registry
            .with_validated(handle.0, || {
                self.stream_states[handle.index()].reserve_next()
            })
            .ok_or("Stale stream handle")??;
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
        let idx = self.slot_index(stream_idx, id)?;
        let hash = xxh3_64(&data_bytes);
        let mut slot = self.buffer[idx].write().await;
        let advanced = self
            .registry
            .with_generation(stream_idx, generation, || {
                if slot.generation == generation && slot.id.is_some_and(|stored| stored > id) {
                    return Err("Chunk id already evicted");
                }
                self.store_slot(idx, &mut slot, id, generation, hash, data_bytes);
                Ok(self.update_state_after_slot(stream_idx, id, true, true))
            })
            .ok_or("Stream index changed")??;
        drop(slot);
        if advanced {
            self.signal_publication(stream_idx);
        }
        self.notify_stream_update(stream_idx);
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
        let generation = self.generation(stream_idx)?;
        self.registry
            .with_generation(stream_idx, generation, || {
                self.stream_states[stream_idx].last()
            })
            .flatten()
    }

    pub fn version(&self, stream_idx: usize) -> Option<u64> {
        let generation = self.generation(stream_idx)?;
        self.registry.with_generation(stream_idx, generation, || {
            self.stream_states[stream_idx]
                .version
                .load(Ordering::Acquire)
        })
    }

    pub fn last_for_handle(&self, handle: StreamHandle) -> Option<usize> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let last = self.stream_states.get(handle.index())?.last();
        self.registry
            .is_current_fast(handle.0)
            .then_some(last)
            .flatten()
    }

    pub fn version_for_handle(&self, handle: StreamHandle) -> Option<u64> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let version = self
            .stream_states
            .get(handle.index())?
            .version
            .load(Ordering::Acquire);
        self.registry.is_current_fast(handle.0).then_some(version)
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

    /// Reset all published state for a physical stream index.
    ///
    /// This is intended for callers that manage their own logical stream IDs
    /// on top of fixed `ChunkCache` indices. It advances the generation so
    /// retained ring-buffer bytes from the previous logical stream are no
    /// longer visible through `get`, `last`, or initialization lookups. A
    /// stream that held the index is removed, and the index becomes free.
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

    /// The ring slot of a part that is about to be written. `usize::MAX` is
    /// the empty marker of the watermark and the slot tags, and is refused.
    fn slot_index(&self, stream_idx: usize, id: usize) -> Result<usize, &'static str> {
        if id == EMPTY_ID {
            return Err("Chunk id out of range");
        }
        self.offset(stream_idx, id)
            .ok_or("Stream index out of bounds")
    }

    /// Runs under the index's owner write lock.
    fn reset_index_state(&self, stream_idx: usize, _generation: u64) {
        if let Some(initialization) = self.stream_initializations.get(stream_idx) {
            let mut initialization = initialization
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *initialization = None;
        }
        if let Some(state) = self.stream_states.get(stream_idx) {
            state.reset();
        }
        self.notify_stream_update(stream_idx);
    }

    fn notify_stream_update(&self, stream_idx: usize) {
        if let Some(notifier) = self.stream_update_notifiers.get(stream_idx) {
            notifier.notify_waiters();
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

    /// Write a slot under its lock: the tag (and its time) first, then the
    /// payload. Readers are woken by the caller after the lock is dropped.
    fn store_slot(
        &self,
        idx: usize,
        slot: &mut ChunkSlot,
        id: usize,
        generation: u64,
        hash: u64,
        bytes: Bytes,
    ) {
        self.slot_tags[idx].store(id, generation, unix_micros());
        slot.id = Some(id);
        slot.generation = generation;
        slot.hash = hash;
        slot.bytes = bytes;
    }

    fn clear_slot(&self, idx: usize, slot: &mut ChunkSlot) {
        self.slot_tags[idx].clear();
        slot.id = None;
        slot.hash = 0;
        slot.bytes = Bytes::new();
    }

    /// Returns whether the watermark advanced.
    async fn add_slot_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        data: Bytes,
        older: OlderId,
    ) -> Result<bool, &'static str> {
        self.validate_payload(&data)?;
        let idx = self.slot_index(handle.index(), id)?;
        let hash = xxh3_64(&data);
        let mut slot = self.buffer[idx].write().await;
        self.registry
            .with_validated(handle.0, || {
                if older == OlderId::Reject
                    && slot.generation == handle.generation()
                    && slot.id.is_some_and(|stored| stored > id)
                {
                    return Err("Chunk id already evicted");
                }
                self.store_slot(idx, &mut slot, id, handle.generation(), hash, data);
                Ok(self.update_state_after_slot(handle.index(), id, true, true))
            })
            .ok_or("Stale stream handle")?
    }

    /// Returns the outcome and whether the watermark advanced.
    async fn put_slot_if_absent_for_handle(
        &self,
        handle: StreamHandle,
        id: usize,
        data: Bytes,
        publish: bool,
    ) -> Result<(PutIfAbsentResult, bool), &'static str> {
        self.validate_payload(&data)?;
        let idx = self.slot_index(handle.index(), id)?;
        let hash = xxh3_64(&data);
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
                    self.store_slot(idx, &mut slot, id, handle.generation(), hash, data);
                    PutIfAbsentResult::Inserted
                };
                let inserted = result == PutIfAbsentResult::Inserted;
                let publish = publish
                    && matches!(
                        result,
                        PutIfAbsentResult::Inserted | PutIfAbsentResult::AlreadyPresent
                    );
                let advanced = (inserted || publish)
                    && self.update_state_after_slot(handle.index(), id, inserted, publish);
                (result, advanced)
            })
            .ok_or("Stale stream handle")
    }

    /// Runs under the index's owner read lock. Returns whether the watermark
    /// strictly advanced.
    fn update_state_after_slot(
        &self,
        stream_idx: usize,
        id: usize,
        bump_version: bool,
        publish: bool,
    ) -> bool {
        let state = &self.stream_states[stream_idx];
        if bump_version {
            state.bump_version();
        }
        if !publish {
            return false;
        }
        state.raise_next_id(id.saturating_add(1));
        state.raise_last(id)
    }

    /// Advance the watermark across every retained contiguous part from the
    /// published one (or `first_expected_id`). Returns whether it advanced.
    fn publish_contiguous_last(
        &self,
        handle: StreamHandle,
        id: usize,
        first_expected_id: usize,
    ) -> Result<bool, &'static str> {
        // Two writers that fill each other's gap each store their tag and
        // then scan for the other's. The fence keeps both from missing the
        // other's tag, which would leave the watermark behind a filled gap.
        fence(Ordering::SeqCst);
        self.registry
            .with_validated(handle.0, || {
                let state = &self.stream_states[handle.index()];
                state.raise_next_id(id.saturating_add(1));
                let current = state.last();
                let next_id = state.next_id.load(Ordering::Acquire);
                let published_next = current
                    .and_then(|last| last.checked_add(1))
                    .unwrap_or(first_expected_id);
                let retained_start = next_id.saturating_sub(self.stream_capacity());
                let mut candidate = published_next.max(first_expected_id).max(retained_start);
                let mut contiguous_last = None;

                // One scan can advance by at most the retained ring capacity.
                // This keeps completion work bounded even for adversarial
                // object IDs. Once an unresolved gap has fallen behind
                // `retained_start`, the rolling live publication may resume at
                // the first still-retained object. The watermark always points
                // at a real stored object; it never fabricates completeness
                // for a slot that is still retained.
                for _ in 0..self.stream_capacity() {
                    let Some(slot) = self.offset(handle.index(), candidate) else {
                        break;
                    };
                    if !self.slot_tags[slot].holds(candidate, handle.generation()) {
                        break;
                    }
                    contiguous_last = Some(candidate);
                    let Some(next) = candidate.checked_add(1) else {
                        break;
                    };
                    candidate = next;
                }
                contiguous_last.is_some_and(|last| state.raise_last(last))
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

/// CLOCK_REALTIME in microseconds since the Unix epoch.
fn unix_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX)
        })
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
        cache.resolve_or_create_stream(94).await;
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
            cache.resolve_or_create_stream(stream_id).await;
        }

        let streams = cache
            .stream_handles()
            .into_iter()
            .map(|handle| (handle.stream_id(), handle.index()))
            .collect::<Vec<_>>();
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
            cache
                .add_remote_for_handle(old, 0, Bytes::from_static(b"stale"))
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

    /// C7: the wire carries no generation, so a restarted origin's parts
    /// reach a receiver in the generation that holds the old ids. A mesh
    /// write replaces them; an origin-local write still may not.
    #[tokio::test]
    async fn a_remote_write_replaces_a_newer_id_of_the_same_generation() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        });
        let handle = cache.resolve_or_create_stream(13).await;
        for id in 0..8 {
            cache
                .add_remote_for_handle(handle, id, Bytes::from(format!("old-{id}")))
                .await
                .unwrap();
        }
        assert_eq!(
            cache
                .add_for_handle(handle, 1, Bytes::from_static(b"local"))
                .await,
            Err("Chunk id already evicted")
        );

        let notified = cache.publication_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        cache
            .add_remote_for_handle(handle, 1, Bytes::from_static(b"restarted-1"))
            .await
            .unwrap();
        assert!(
            !fired(&mut notified),
            "an id below the watermark does not advance it"
        );
        assert_eq!(
            cache.get_for_handle(handle, 1).await.unwrap().0,
            Bytes::from_static(b"restarted-1")
        );
        assert!(cache.get_for_handle(handle, 5).await.is_none());
        assert!(cache.written_unix_us_for_handle(handle, 1).is_some());
        assert!(cache.written_unix_us_for_handle(handle, 5).is_none());
        assert_eq!(cache.last_for_handle(handle), Some(7));
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

    /// C6: a waiter is keyed by stream and part only, so a write in a later
    /// generation of the stream wakes it. What it then reads is never data
    /// of the generation it registered in.
    #[tokio::test]
    async fn a_waiter_woken_across_a_generation_finds_no_data_from_the_previous_one() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        let old = cache.resolve_or_create_stream(21).await;
        cache
            .add_for_handle(old, 2, Bytes::from_static(b"old-generation-2"))
            .await
            .unwrap();
        let waiter = cache.exact_part_waiter(21, 3).unwrap();
        let notified = waiter.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        cache.resolve_or_create_stream(22).await;
        cache.zero_stream_id(22);
        cache
            .add_for_stream_id(21, 3, Bytes::from_static(b"new-generation"))
            .await
            .unwrap();

        timeout(Duration::from_millis(100), &mut notified)
            .await
            .expect("the first write of the identity in any generation wakes the waiter");
        let current = cache.resolve_stream(21).unwrap();
        assert_ne!(current.generation(), old.generation());
        assert!(cache.get_for_handle(old, 3).await.is_none());
        assert!(cache.last_for_handle(old).is_none());
        assert!(cache.get_for_stream_id(21, 2).await.is_none());
        assert_eq!(
            cache.get_for_stream_id(21, 3).await.unwrap().0,
            Bytes::from_static(b"new-generation")
        );
    }

    /// C6: a reader may wait for a stream that is not mapped yet. Waiting
    /// creates no mapping and evicts nothing.
    #[tokio::test]
    async fn waiter_for_unknown_stream_does_not_evict_a_live_stream() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        let live = cache.resolve_or_create_stream(31).await;

        assert!(cache.exact_part_waiter(32, 0).is_some());
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

        let first = cache.resolve_or_create_stream(1).await;
        cache
            .add_for_stream_id(1, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();

        let second = cache.resolve_or_create_stream(2).await;
        assert_eq!(first.index(), second.index());
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

        cache.resolve_or_create_stream(2).await;
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
        assert!(cache.handle_at(usize::MAX).is_none());
        assert!(cache.update_notifier(usize::MAX).is_none());
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
        cache.resolve_or_create_stream(52).await;

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

    /// Writers that fill each other's gaps from many threads leave the
    /// watermark at the last part, never behind a filled gap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_contiguous_writers_publish_every_filled_gap() {
        const PARTS: usize = 4_096;
        const WRITERS: usize = 4;
        for round in 0..8_u64 {
            let cache = Arc::new(ChunkCache::new(Options {
                num_playlists: 1,
                max_segments: 1,
                max_parts_per_segment: PARTS,
                ..Options::default()
            }));
            let handle = cache.resolve_or_create_stream(round).await;
            let writers = (0..WRITERS)
                .map(|writer| {
                    let cache = Arc::clone(&cache);
                    task::spawn(async move {
                        for id in (writer..PARTS).step_by(WRITERS) {
                            cache
                                .put_if_absent_contiguous_for_handle(
                                    handle,
                                    id,
                                    0,
                                    Bytes::from_static(b"part"),
                                )
                                .await
                                .unwrap();
                        }
                    })
                })
                .collect::<Vec<_>>();
            for writer in writers {
                writer.await.unwrap();
            }
            assert_eq!(cache.last_for_handle(handle), Some(PARTS - 1));
        }
    }

    #[tokio::test]
    async fn raw_lane_update_notifier_wakes_for_write_and_reset() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        let notifier = cache.update_notifier(0).unwrap();
        let written = notifier.notified();
        tokio::pin!(written);
        written.as_mut().enable();
        cache
            .add(0, 1, Bytes::from_static(b"raw-lane"))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), written)
            .await
            .expect("raw lane write must wake an enabled waiter");

        let reset = notifier.notified();
        tokio::pin!(reset);
        reset.as_mut().enable();
        cache.reset_stream_idx(0);
        tokio::time::timeout(std::time::Duration::from_secs(1), reset)
            .await
            .expect("raw lane reset must wake an enabled waiter");
    }

    #[tokio::test]
    async fn a_new_stream_takes_a_free_index_before_it_replaces_a_stream() {
        let cache = ChunkCache::new(Options {
            num_playlists: 4,
            max_segments: 1,
            max_parts_per_segment: 8,
            ..Options::default()
        });
        // One stream stays while forty others come and go.
        cache
            .add_for_stream_id(7, 0, Bytes::from_static(b"kept"))
            .await
            .unwrap();
        for stream_id in 100..140_u64 {
            cache
                .add_for_stream_id(stream_id, 0, Bytes::from_static(b"visitor"))
                .await
                .unwrap();
            assert!(cache.release_stream_id(stream_id).await);
            assert_eq!(
                cache.get_for_stream_id(7, 0).await.unwrap().0,
                Bytes::from_static(b"kept"),
                "stream 7 was evicted when stream {stream_id} was added"
            );
        }
        // With every index in use a new stream still gets one.
        for stream_id in [8_u64, 9, 10, 11] {
            cache
                .add_for_stream_id(stream_id, 0, Bytes::from_static(b"full"))
                .await
                .unwrap();
        }
        assert!(cache.get_for_stream_id(11, 0).await.is_some());
    }

    #[tokio::test]
    async fn a_released_stream_frees_the_bytes_of_its_parts() {
        let cache = ChunkCache::new(Options {
            num_playlists: 2,
            max_segments: 1,
            max_parts_per_segment: 8,
            ..Options::default()
        });
        for id in 0..8 {
            cache
                .add_for_stream_id(7, id, Bytes::from(vec![id as u8; 1_024]))
                .await
                .unwrap();
        }
        let stream_idx = cache.resolve_stream(7).unwrap().index();
        assert!(cache.release_stream_id(7).await);
        assert!(!cache.release_stream_id(7).await);
        assert!(cache.resolve_stream(7).is_none());
        for id in 0..8 {
            let idx = cache.offset(stream_idx, id).unwrap();
            let slot = cache.buffer[idx].read().await;
            assert!(
                slot.id.is_none() && slot.bytes.is_empty(),
                "slot {id} kept its bytes"
            );
        }
        // The index is usable again, and a hidden stream keeps its bytes
        // only until they are released.
        cache
            .add_for_stream_id(8, 3, Bytes::from_static(b"next"))
            .await
            .unwrap();
        assert_eq!(
            cache.get_for_stream_id(8, 3).await.unwrap().0,
            Bytes::from_static(b"next")
        );
    }

    /// A release frees the parts of the released generation and of older,
    /// hidden ones, but never what a stream that took the index since wrote.
    #[tokio::test]
    async fn a_release_keeps_the_parts_of_a_stream_that_took_the_index_since() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 8,
            ..Options::default()
        });
        for id in 0..8 {
            cache
                .add_for_stream_id(6, id, Bytes::from_static(b"hidden"))
                .await
                .unwrap();
        }
        cache.zero_stream_id(6);
        for id in 0..4 {
            cache
                .add_for_stream_id(7, id, Bytes::from_static(b"released"))
                .await
                .unwrap();
        }
        let released = cache
            .registry
            .remove_stream(7, |index, generation| {
                cache.reset_index_state(index, generation)
            })
            .unwrap();
        // A new stream takes the index before the released slots are freed.
        let next = cache.resolve_or_create_stream(8).await;
        assert_eq!(next.index(), released.index());
        cache
            .add_for_handle(next, 2, Bytes::from_static(b"next"))
            .await
            .unwrap();
        cache.release_slots_of(released).await;

        assert_eq!(
            cache.get_for_handle(next, 2).await.unwrap().0,
            Bytes::from_static(b"next")
        );
        let stats = cache.memory_stats().await;
        assert_eq!(stats.occupied_slots, 1);
        assert_eq!(stats.chunk_bytes, b"next".len());
    }

    #[tokio::test]
    async fn a_cleared_slot_is_released_and_can_be_written_again() {
        let cache = ChunkCache::new(Options {
            num_playlists: 2,
            max_segments: 1,
            max_parts_per_segment: 8,
            ..Options::default()
        });
        for id in 0..4 {
            cache
                .add_for_stream_id(7, id, Bytes::from(vec![id as u8; 16]))
                .await
                .unwrap();
        }
        assert!(cache.clear_for_stream_id(7, 1).await);
        assert!(cache.get_for_stream_id(7, 1).await.is_none());
        assert!(cache.get_for_stream_id(7, 2).await.is_some());
        // The newest part is still the head of the stream.
        let handle = cache.resolve_stream(7).unwrap();
        assert_eq!(cache.last_for_handle(handle), Some(3));
        // Clearing is idempotent and does not touch other identities.
        assert!(!cache.clear_for_stream_id(7, 1).await);
        assert!(!cache.clear_for_stream_id(7, 9).await);
        assert!(!cache.clear_for_stream_id(8, 2).await);
        cache
            .add_for_stream_id(7, 1, Bytes::from_static(b"again"))
            .await
            .unwrap();
        assert_eq!(
            cache.get_for_stream_id(7, 1).await.unwrap().0,
            Bytes::from_static(b"again")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reader_woken_by_a_write_finds_when_the_slot_was_written() {
        let cache = Arc::new(ChunkCache::new(Options::default()));
        cache.resolve_or_create_stream(9).await;
        assert_eq!(cache.written_unix_us_for_stream_id(9, 3), None);
        let waiter = cache.exact_part_waiter(9, 3).unwrap();
        let notified = waiter.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let writer = {
            let cache = Arc::clone(&cache);
            task::spawn(async move {
                let before = unix_micros();
                cache
                    .put_if_absent_contiguous_for_stream_id(9, 3, 0, Bytes::from_static(b"p3"))
                    .await
                    .unwrap();
                before
            })
        };
        timeout(Duration::from_secs(1), notified).await.unwrap();
        // Read at once on waking, before the writer returns to its caller.
        let written = cache.written_unix_us_for_stream_id(9, 3).unwrap();
        let before = writer.await.unwrap();
        assert!(written >= before && written <= unix_micros());
        // Another identity, or a released slot, has no time.
        assert_eq!(cache.written_unix_us_for_stream_id(9, 4), None);
        assert_eq!(cache.written_unix_us_for_stream_id(10, 3), None);
        assert!(cache.clear_for_stream_id(9, 3).await);
        assert_eq!(cache.written_unix_us_for_stream_id(9, 3), None);
        // add_for_stream_id records the time too.
        cache
            .add_for_stream_id(9, 5, Bytes::from_static(b"p5"))
            .await
            .unwrap();
        assert!(cache.written_unix_us_for_stream_id(9, 5).unwrap() >= written);
    }

    /// The write time of a part never outlives its generation: a reused
    /// index or a released stream reports none for the old identity.
    #[tokio::test]
    async fn a_write_time_is_bound_to_the_generation_that_wrote_it() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        });
        let old = cache.resolve_or_create_stream(1).await;
        cache
            .add_for_handle(old, 0, Bytes::from_static(b"old"))
            .await
            .unwrap();
        assert!(cache.written_unix_us_for_handle(old, 0).is_some());

        let current = cache.resolve_or_create_stream(2).await;
        assert_eq!(cache.written_unix_us_for_handle(old, 0), None);
        assert_eq!(cache.written_unix_us_for_handle(current, 0), None);
        cache
            .add_for_handle(current, 0, Bytes::from_static(b"current"))
            .await
            .unwrap();
        assert!(cache.written_unix_us_for_handle(current, 0).is_some());
        assert_eq!(cache.written_unix_us_for_handle(old, 0), None);

        assert!(cache.release_stream_id(2).await);
        assert_eq!(cache.written_unix_us_for_handle(current, 0), None);
        assert_eq!(cache.written_unix_us_for_stream_id(2, 0), None);
    }

    /// Whether the publication notification has fired since `notified` was
    /// created.
    fn fired(notified: &mut std::pin::Pin<&mut tokio::sync::futures::Notified<'_>>) -> bool {
        use std::future::Future;
        use std::task::{Context, Poll};
        let mut context = Context::from_waker(std::task::Waker::noop());
        matches!(notified.as_mut().poll(&mut context), Poll::Ready(()))
    }

    #[tokio::test]
    async fn an_advancing_watermark_signals_publication() {
        let cache = ChunkCache::new(Options {
            num_playlists: 2,
            max_segments: 1,
            max_parts_per_segment: 16,
            ..Options::default()
        });

        // add_for_stream_id advances the watermark.
        let notified = cache.publication_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        assert!(!fired(&mut notified));
        cache
            .add_for_stream_id(5, 0, Bytes::from_static(b"part-0"))
            .await
            .unwrap();
        assert!(fired(&mut notified));

        // Rewriting an older slot leaves the watermark where it is.
        cache
            .add_for_stream_id(5, 1, Bytes::from_static(b"part-1"))
            .await
            .unwrap();
        let notified = cache.publication_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        cache
            .add_for_stream_id(5, 0, Bytes::from_static(b"part-0-again"))
            .await
            .unwrap();
        assert!(!fired(&mut notified));

        // put_if_absent_for_stream_id signals a new part, not a repeat.
        cache
            .put_if_absent_for_stream_id(5, 2, Bytes::from_static(b"part-2"))
            .await
            .unwrap();
        let notified = cache.publication_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        cache
            .put_if_absent_for_stream_id(5, 2, Bytes::from_static(b"part-2"))
            .await
            .unwrap();
        assert!(!fired(&mut notified));
    }

    /// Writes that leave the watermark where it was, and slot and stream
    /// releases, do not signal publication.
    #[tokio::test]
    async fn only_an_advancing_watermark_signals_publication() {
        let cache = ChunkCache::new(Options {
            num_playlists: 2,
            max_segments: 1,
            max_parts_per_segment: 2,
            ..Options::default()
        });
        let handle = cache.resolve_or_create_stream(5).await;
        cache
            .add_for_handle(handle, 2, Bytes::from_static(b"part-2"))
            .await
            .unwrap();
        cache
            .set_stream_initialization_for_handle(handle, Bytes::from_static(b"init"))
            .unwrap();
        let set = cache.subscribe_publications();
        let notified = cache.publication_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        assert_eq!(
            cache
                .put_if_absent_for_handle(handle, 2, Bytes::from_static(b"other"))
                .await
                .unwrap(),
            PutIfAbsentResult::HashConflict
        );
        assert_eq!(
            cache
                .put_if_absent_for_handle(handle, 0, Bytes::from_static(b"older"))
                .await
                .unwrap(),
            PutIfAbsentResult::Superseded
        );
        assert!(cache
            .add_for_handle(handle, 0, Bytes::from_static(b"older"))
            .await
            .is_err());
        cache
            .set(handle.index(), 2, Bytes::from_static(b"raw-set"))
            .await
            .ok();
        cache
            .set_stream_initialization_for_handle(handle, Bytes::from_static(b"init-2"))
            .unwrap();
        assert!(cache.clear_for_handle(handle, 2).await);
        assert!(cache.release_stream_id(5).await);
        cache.resolve_or_create_stream(6).await;
        cache.zero_stream_id(6);

        assert!(!fired(&mut notified));
        let mut moved = Vec::new();
        set.drain(&mut moved);
        assert!(moved.is_empty());
    }

    #[tokio::test]
    async fn a_part_beyond_a_gap_signals_publication_only_when_the_gap_fills() {
        let cache = ChunkCache::new(Options {
            num_playlists: 2,
            max_segments: 1,
            max_parts_per_segment: 16,
            ..Options::default()
        });
        cache
            .put_if_absent_contiguous_for_stream_id(9, 0, 0, Bytes::from_static(b"object-0"))
            .await
            .unwrap();

        let notified = cache.publication_notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        cache
            .put_if_absent_contiguous_for_stream_id(9, 2, 0, Bytes::from_static(b"object-2"))
            .await
            .unwrap();
        assert!(
            !fired(&mut notified),
            "a part beyond a gap is not published"
        );

        cache
            .put_if_absent_contiguous_for_stream_id(9, 1, 0, Bytes::from_static(b"object-1"))
            .await
            .unwrap();
        assert!(fired(&mut notified), "filling the gap publishes");
        let handle = cache.resolve_stream(9).unwrap();
        assert_eq!(cache.last_for_handle(handle), Some(2));
    }

    #[tokio::test]
    async fn a_publication_wakes_a_task_waiting_for_it() {
        let cache = Arc::new(ChunkCache::new(Options {
            num_playlists: 2,
            max_segments: 1,
            max_parts_per_segment: 16,
            ..Options::default()
        }));
        let waiter = {
            let cache = Arc::clone(&cache);
            task::spawn(async move {
                let notified = cache.publication_notify().notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                notified.await;
            })
        };
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        cache
            .append(0, Bytes::from_static(b"appended"))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("publication did not wake the waiter")
            .unwrap();
    }

    #[tokio::test]
    async fn a_publication_set_names_the_streams_that_advanced() {
        let cache = ChunkCache::new(Options {
            num_playlists: 70,
            max_segments: 1,
            max_parts_per_segment: 8,
            ..Options::default()
        });
        let set = cache.subscribe_publications();
        let mut moved = Vec::new();
        set.drain(&mut moved);
        assert!(moved.is_empty());
        let mut handles = Vec::new();
        for stream_id in 0..70_u64 {
            handles.push(cache.resolve_or_create_stream(stream_id).await);
        }
        cache
            .add_for_stream_id(3, 0, Bytes::from_static(b"a"))
            .await
            .unwrap();
        cache
            .add_for_stream_id(68, 0, Bytes::from_static(b"b"))
            .await
            .unwrap();
        // A write beyond a gap leaves the watermark where it was.
        cache
            .put_if_absent_contiguous_for_stream_id(5, 3, 0, Bytes::from_static(b"c"))
            .await
            .unwrap();
        set.drain(&mut moved);
        let mut expected = vec![handles[3].index(), handles[68].index()];
        expected.sort_unstable();
        assert_eq!(moved, expected);
        moved.clear();
        set.drain(&mut moved);
        assert!(moved.is_empty(), "drained twice");

        // A second consumer has its own set, and a dropped set is forgotten.
        let other = cache.subscribe_publications();
        drop(set);
        cache
            .add_for_stream_id(3, 1, Bytes::from_static(b"d"))
            .await
            .unwrap();
        other.drain(&mut moved);
        assert_eq!(moved, vec![handles[3].index()]);
        let live = cache
            .publication_sets
            .read()
            .unwrap()
            .iter()
            .filter(|set| set.strong_count() > 0)
            .count();
        assert_eq!(live, 1);
        assert_eq!(cache.handle_at(handles[68].index()), Some(handles[68]));
    }

    /// A drained index names its stream through `handle_at`, and stops naming
    /// it once the stream is released or the index is reused.
    #[tokio::test]
    async fn handle_at_names_the_stream_of_a_drained_index() {
        let cache = ChunkCache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 4,
            ..Options::default()
        });
        assert_eq!(cache.handle_at(0), None);
        let set = cache.subscribe_publications();
        cache
            .add_for_stream_id(40, 0, Bytes::from_static(b"a"))
            .await
            .unwrap();
        let mut moved = Vec::new();
        set.drain(&mut moved);
        assert_eq!(moved, vec![0]);
        let first = cache.handle_at(0).unwrap();
        assert_eq!(Some(first), cache.resolve_stream(40));
        assert_eq!(first.stream_id(), 40);

        let second = cache.resolve_or_create_stream(41).await;
        assert_eq!(cache.handle_at(0), Some(second));
        assert!(cache.last_for_handle(first).is_none());

        assert!(cache.release_stream_id(41).await);
        assert_eq!(cache.handle_at(0), None);
        // A raw lane has no stream.
        cache.add(0, 1, Bytes::from_static(b"raw")).await.unwrap();
        assert_eq!(cache.handle_at(0), None);
    }

    /// A lane notifier wakes for a mapped stream's writes and initialization,
    /// not only for raw lanes.
    #[tokio::test]
    async fn a_lane_notifier_wakes_for_a_mapped_stream() {
        let cache = ChunkCache::new(Options::default());
        let handle = cache.resolve_or_create_stream(3).await;
        let notifier = cache.update_notifier(handle.index()).unwrap();

        let written = notifier.notified();
        tokio::pin!(written);
        written.as_mut().enable();
        cache
            .add_for_handle(handle, 0, Bytes::from_static(b"part"))
            .await
            .unwrap();
        timeout(Duration::from_secs(1), written)
            .await
            .expect("a mapped write must wake the lane notifier");

        let initialized = notifier.notified();
        tokio::pin!(initialized);
        initialized.as_mut().enable();
        cache
            .set_stream_initialization_for_handle(handle, Bytes::from_static(b"init"))
            .unwrap();
        timeout(Duration::from_secs(1), initialized)
            .await
            .expect("a new initialization must wake the lane notifier");
    }

    /// The slot hash is the one the const evaluator computes: the mesh keys
    /// its encoded-object cache by it.
    #[test]
    fn the_slot_hash_is_xxh3_64() {
        for len in [0_usize, 1, 16, 17, 128, 129, 240, 241, 1_200, 65_536] {
            let data = (0..len).map(|i| (i * 31 % 251) as u8).collect::<Vec<_>>();
            assert_eq!(
                xxh3_64(&data),
                xxhash_rust::const_xxh3::xxh3_64(&data),
                "{len}"
            );
        }
    }
}
