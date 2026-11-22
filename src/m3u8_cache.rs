use crate::m3u8_manifest::{parse_seconds_us, M3u8Manifest, PartPosition};
use crate::stream_registry::{ResolvedStream, StreamRegistry};
use crate::{CacheError, Options};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::prelude::*;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use tokio::sync::Notify;
use xxhash_rust::xxh3::xxh3_64;

/// Playlists are compressed when a reader asks for them, once for each new
/// part: the fastest level keeps that cheap.
const PLAYLIST_GZIP_LEVEL: Compression = Compression::fast();

const MAX_LATEST_READ_REPLICAS: usize = 64;
static NEXT_LATEST_READ_REPLICA: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static LATEST_READ_REPLICA: usize =
        NEXT_LATEST_READ_REPLICA.fetch_add(1, Ordering::Relaxed);
}

/// Upper bounds on the lines of a playlist that this cache renders, used for
/// [`M3u8CacheMemoryStats::maximum_payload_bytes`]. The tags up to
/// EXT-X-MAP are at most about 300 bytes, the open segment's
/// PROGRAM-DATE-TIME and PRELOAD-HINT at most about 150, a closed segment's
/// PROGRAM-DATE-TIME, EXTINF, BYTERANGE and URI at most about 165, and an
/// EXT-X-PART line at most about 135 (every number at its largest).
const HEAD_BOUND_BYTES: usize = 512;
const OPEN_TAGS_BOUND_BYTES: usize = 256;
const SEGMENT_TAGS_BOUND_BYTES: usize = 192;
const PART_LINE_BOUND_BYTES: usize = 160;
/// The EXT-X-SKIP line a delta update adds.
const SKIP_LINE_BOUND_BYTES: usize = 64;

#[derive(Debug, Clone, Copy)]
struct MediaSegmentBlock {
    start: usize,
    duration_us: u64,
}

/// Cache-owned identity for one logical playlist stream.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct M3u8StreamHandle(ResolvedStream);

impl M3u8StreamHandle {
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

/// One atomically published playlist position: the newest part.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlaylistPosition {
    pub segment_id: usize,
    pub part_idx: usize,
    pub sequence: usize,
}

/// Snapshot of encoded payload memory retained by the playlist cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct M3u8CacheMemoryStats {
    /// Streams with a live playlist.
    pub occupied_slots: usize,
    /// Rendered playlists: the text, its compressed copies (one per read
    /// replica) and the compressed delta updates.
    pub encoded_playlist_bytes: usize,
    pub initialization_bytes: usize,
    /// A bound, derived from the options, on all of the above.
    pub maximum_payload_bytes: usize,
}

#[derive(Clone)]
struct StreamInitialization {
    stream_id: u64,
    generation: u64,
    bytes: Bytes,
}

/// The sequence numbers of one segment's parts, `[first, end)`, tagged with
/// the segment so that a ring position reused by a newer segment is never
/// read as an older one.
#[derive(Clone, Copy)]
struct SegmentRange {
    segment_id: usize,
    first_sequence: usize,
    end_sequence: usize,
}

/// Compressed copies of one playlist text, one for each read replica. Every
/// copy holds the same bytes; readers on different threads take different
/// copies, so that they do not share a reference count.
struct Variants {
    gzip: Box<[Bytes]>,
    hash: u64,
}

impl Variants {
    fn new(gzip: Bytes, hash: u64, replicas: usize) -> Self {
        let gzip = (0..replicas)
            .map(|replica| {
                if replica == 0 {
                    gzip.clone()
                } else {
                    Bytes::copy_from_slice(&gzip)
                }
            })
            .collect();
        Self { gzip, hash }
    }

    fn pick(&self) -> (Bytes, u64) {
        let replica = if self.gzip.len() == 1 {
            0
        } else {
            LATEST_READ_REPLICA.with(|replica| *replica) % self.gzip.len()
        };
        (self.gzip[replica].clone(), self.hash)
    }

    fn bytes(&self) -> usize {
        self.gzip
            .iter()
            .map(Bytes::len)
            .fold(0_usize, usize::saturating_add)
    }
}

/// One rendering of a playlist: the version of the stream it was made from,
/// and its delta update, made by the first reader that asks for it.
struct Rendered {
    generation: u64,
    version: u64,
    text: Bytes,
    full: Variants,
    delta: OnceLock<Option<Variants>>,
}

impl Rendered {
    fn bytes(&self) -> usize {
        let delta = self
            .delta
            .get()
            .and_then(Option::as_ref)
            .map_or(0, Variants::bytes);
        self.text
            .len()
            .saturating_add(self.full.bytes())
            .saturating_add(delta)
    }
}

/// The playlist of one stream slot. Written under its lock; a reset
/// replaces it under the same lock.
struct PlaylistSlot {
    /// Registry generation of the stream that this state belongs to.
    generation: u64,
    /// Advances with every part, initialization and reset. Never repeats
    /// for a slot, so it is never 0 and a new stream never reuses one.
    version: u64,
    /// The live playlist. A playlist is rendered from it only when it is
    /// read.
    manifest: Option<M3u8Manifest>,
    segment_ranges: Box<[Option<SegmentRange>]>,
}

/// What a reader needs to know of a slot without its lock: the generation,
/// version and newest part, published together as a sequence lock. Written
/// only under the slot lock, so there is one writer at a time.
struct PublishedState {
    sequence: AtomicU64,
    generation: AtomicU64,
    version: AtomicU64,
    /// 0 before the first part: segment ids start at 1.
    segment_id: AtomicUsize,
    part_idx: AtomicUsize,
    part_sequence: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StateSnapshot {
    generation: u64,
    version: u64,
    position: Option<PlaylistPosition>,
}

impl PublishedState {
    fn new() -> Self {
        Self {
            sequence: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            version: AtomicU64::new(0),
            segment_id: AtomicUsize::new(0),
            part_idx: AtomicUsize::new(0),
            part_sequence: AtomicUsize::new(0),
        }
    }

    /// Publish `state`. The caller holds the slot lock.
    fn store(&self, state: StateSnapshot) {
        let sequence = self.sequence.load(Ordering::Relaxed);
        self.sequence
            .store(sequence.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        let position = state.position.unwrap_or(PlaylistPosition {
            segment_id: 0,
            part_idx: 0,
            sequence: 0,
        });
        self.generation.store(state.generation, Ordering::Relaxed);
        self.version.store(state.version, Ordering::Relaxed);
        self.segment_id
            .store(position.segment_id, Ordering::Relaxed);
        self.part_idx.store(position.part_idx, Ordering::Relaxed);
        self.part_sequence
            .store(position.sequence, Ordering::Relaxed);
        self.sequence
            .store(sequence.wrapping_add(2), Ordering::Release);
    }

    fn load(&self) -> StateSnapshot {
        let mut spins = 0_u32;
        loop {
            let before = self.sequence.load(Ordering::Acquire);
            if before & 1 == 0 {
                let generation = self.generation.load(Ordering::Relaxed);
                let version = self.version.load(Ordering::Relaxed);
                let segment_id = self.segment_id.load(Ordering::Relaxed);
                let part_idx = self.part_idx.load(Ordering::Relaxed);
                let sequence = self.part_sequence.load(Ordering::Relaxed);
                fence(Ordering::Acquire);
                if self.sequence.load(Ordering::Relaxed) == before {
                    return StateSnapshot {
                        generation,
                        version,
                        position: (segment_id != 0).then_some(PlaylistPosition {
                            segment_id,
                            part_idx,
                            sequence,
                        }),
                    };
                }
            }
            // A writer is between its stores: a few of them, with no lock
            // or allocation in between.
            spins += 1;
            if spins < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

/// Compressed LL-HLS playlists of live streams, and their initialization
/// sections.
///
/// A stream's playlist is kept as a live [`M3u8Manifest`] and rendered when it
/// is read, once for each new part however many readers ask, so adding a part
/// costs no rendering or compression. The memory is one playlist per stream,
/// not one snapshot per part. A reader that asks for a part's playlist is
/// given the newest playlist, which contains that part (RFC 8216bis: a
/// blocking reload is answered with a playlist that contains the part or a
/// later one).
///
/// Streams are named by [`M3u8StreamHandle`]s from a fixed-capacity registry.
/// A handle is bound to one lifetime of a stream: once the stream is zeroed
/// or its slot is given to another stream, every operation on the old handle
/// fails with [`CacheError::StreamNotFound`] and no read returns its data.
///
/// A read of a playlist that is already rendered takes no lock.
pub struct M3u8Cache {
    slots: Vec<Mutex<PlaylistSlot>>,
    /// The generation, version and newest part of each slot, for readers.
    states: Vec<PublishedState>,
    /// The newest rendering of each slot.
    rendered: Vec<ArcSwapOption<Rendered>>,
    /// Readers that render take this, so that one renders and the others
    /// find the rendering. Writers never take it.
    render_locks: Vec<Mutex<()>>,
    inits: Vec<RwLock<Option<StreamInitialization>>>,
    update_notifiers: Vec<Arc<Notify>>,
    registry: StreamRegistry,
    latest_read_replica_count: usize,
    options: Options,
    max_init_bytes: usize,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl M3u8Cache {
    pub fn new(options: Options) -> Self {
        Self::try_new(options).expect("valid playlist cache capacity")
    }

    pub fn try_new(options: Options) -> Result<Self, CacheError> {
        Self::try_new_with_latest_read_replicas(options, 1)
    }

    /// Build a cache with independent compressed playlist payloads for hot
    /// readers.
    ///
    /// One replica keeps the default memory and render costs. With more, the
    /// first reader of each new playlist also copies its compressed bytes
    /// once per extra replica, and readers on different threads take
    /// different copies, so that they do not contend on one reference count.
    /// The count is normalized to 1 through 64.
    pub fn new_with_latest_read_replicas(options: Options, replicas: usize) -> Self {
        Self::try_new_with_latest_read_replicas(options, replicas)
            .expect("valid playlist cache capacity")
    }

    pub fn try_new_with_latest_read_replicas(
        options: Options,
        replicas: usize,
    ) -> Result<Self, CacheError> {
        let options = options.normalized();
        let latest_read_replica_count = replicas.clamp(1, MAX_LATEST_READ_REPLICAS);
        let num_playlists = options.num_playlists;
        num_playlists
            .checked_mul(options.max_segments)
            .ok_or(CacheError::ArithmeticOverflow)?;
        options
            .max_segments
            .checked_mul(options.max_parts_per_segment)
            .ok_or(CacheError::ArithmeticOverflow)?;
        let max_init_bytes = options
            .init_size_kb
            .checked_mul(1024)
            .ok_or(CacheError::ArithmeticOverflow)?;

        Ok(Self {
            slots: (0..num_playlists)
                .map(|_| {
                    Mutex::new(PlaylistSlot {
                        generation: 0,
                        version: 1,
                        manifest: None,
                        segment_ranges: vec![None; options.max_segments].into_boxed_slice(),
                    })
                })
                .collect(),
            states: (0..num_playlists).map(|_| PublishedState::new()).collect(),
            rendered: (0..num_playlists).map(|_| ArcSwapOption::empty()).collect(),
            render_locks: (0..num_playlists).map(|_| Mutex::new(())).collect(),
            inits: (0..num_playlists).map(|_| RwLock::new(None)).collect(),
            update_notifiers: (0..num_playlists)
                .map(|_| Arc::new(Notify::new()))
                .collect(),
            registry: StreamRegistry::new(num_playlists),
            latest_read_replica_count,
            options,
            max_init_bytes,
        })
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    /// The normalized read replica count.
    pub fn latest_read_replica_count(&self) -> usize {
        self.latest_read_replica_count
    }

    pub fn resolve_stream(&self, stream_id: u64) -> Option<M3u8StreamHandle> {
        self.registry.resolve(stream_id).map(M3u8StreamHandle)
    }

    /// The stream's handle, creating the stream when it is new. A new stream
    /// takes a free slot, or replaces the stream in the next slot in turn
    /// when none is free.
    pub fn resolve_or_create_stream(&self, stream_id: u64) -> Result<M3u8StreamHandle, CacheError> {
        Ok(M3u8StreamHandle(
            self.registry
                .resolve_or_create(stream_id, |index, generation| {
                    self.reset_index_state(index, generation)
                }),
        ))
    }

    pub fn ensure_stream_id(&self, stream_id: u64) -> Result<(), CacheError> {
        self.resolve_or_create_stream(stream_id).map(|_| ())
    }

    /// Remove the stream and its playlist and initialization. Its slot
    /// becomes free.
    pub fn zero_stream_id(&self, stream_id: u64) {
        let _ = self.registry.remove_stream(stream_id, |index, generation| {
            self.reset_index_state(index, generation)
        });
    }

    /// The segment and part index of the stream's newest part.
    pub fn last_position(&self, stream_id: u64) -> Option<(usize, usize)> {
        let position = self.position(stream_id)?;
        Some((position.segment_id, position.part_idx))
    }

    pub fn position(&self, stream_id: u64) -> Option<PlaylistPosition> {
        self.position_for_handle(self.resolve_stream(stream_id)?)
    }

    pub fn position_for_handle(&self, handle: M3u8StreamHandle) -> Option<PlaylistPosition> {
        self.checked(handle, |state| Ok(state.position))
            .ok()
            .flatten()
    }

    /// Advances with every part, initialization and reset of the stream.
    pub fn version_for_handle(&self, handle: M3u8StreamHandle) -> Option<u64> {
        self.checked(handle, |state| Ok(state.version)).ok()
    }

    /// The notification source for one stream lifetime.
    ///
    /// Callers must create and enable a `notified` future, then recheck the
    /// cache before they await it. A new part, a new initialization and a
    /// stream reset all wake current waiters; a reader rendering the
    /// playlist does not. The retained handle lets callers detect that a
    /// wake belongs to stream teardown instead of new playlist data.
    pub fn update_notifier_for_handle(&self, handle: M3u8StreamHandle) -> Option<Arc<Notify>> {
        self.checked(handle, |_| {
            Ok(Arc::clone(&self.update_notifiers[handle.index()]))
        })
        .ok()
    }

    pub fn set_init(&self, stream_id: u64, data_bytes: Bytes) -> Result<(), CacheError> {
        let handle = self
            .resolve_stream(stream_id)
            .ok_or(CacheError::StreamNotFound)?;
        self.set_init_for_handle(handle, data_bytes)
    }

    /// Set the stream's initialization section. Fails with
    /// [`CacheError::BufferOverflow`] when it is larger than `init_size_kb`.
    pub fn set_init_for_handle(
        &self,
        handle: M3u8StreamHandle,
        data_bytes: Bytes,
    ) -> Result<(), CacheError> {
        if data_bytes.len() > self.max_init_bytes {
            return Err(CacheError::BufferOverflow);
        }
        let index = handle.index();
        self.registry
            .with_validated(handle.0, || {
                let mut init = self.inits[index]
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *init = Some(StreamInitialization {
                    stream_id: handle.stream_id(),
                    generation: handle.generation(),
                    bytes: data_bytes,
                });
                drop(init);
                let mut slot = lock(&self.slots[index]);
                if slot.generation == handle.generation() {
                    slot.version = next_version(slot.version);
                    let state = StateSnapshot {
                        generation: slot.generation,
                        version: slot.version,
                        position: self.states[index].load().position,
                    };
                    self.states[index].store(state);
                }
            })
            .ok_or(CacheError::StreamNotFound)?;
        self.notify_update(index);
        Ok(())
    }

    /// The stream's initialization section. A stream has none until one is
    /// set: [`CacheError::StreamNotFound`] until then.
    pub fn get_init(&self, stream_id: u64) -> Result<Bytes, CacheError> {
        let handle = self
            .resolve_stream(stream_id)
            .ok_or(CacheError::StreamNotFound)?;
        self.get_init_for_handle(handle)
            .ok_or(CacheError::StreamNotFound)
    }

    pub fn get_init_for_handle(&self, handle: M3u8StreamHandle) -> Option<Bytes> {
        self.checked(handle, |_| {
            let init = self.inits[handle.index()]
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Ok(init
                .as_ref()
                .filter(|init| {
                    init.stream_id == handle.stream_id() && init.generation == handle.generation()
                })
                .map(|init| init.bytes.clone()))
        })
        .ok()
        .flatten()
    }

    /// Add a part of `duration_us` microseconds to the stream's live playlist,
    /// creating the stream when it is new. No playlist is rendered: a reader
    /// renders it.
    ///
    /// Fails with [`CacheError::IndexOutOfBounds`], and adds nothing, when
    /// the part would not open a new segment and the open one already holds
    /// `max_parts_per_segment` parts.
    pub fn add_part_us(
        &self,
        stream_id: u64,
        duration_us: u64,
        key: bool,
        byte_len: Option<usize>,
    ) -> Result<PartPosition, CacheError> {
        let handle = match self.resolve_stream(stream_id) {
            Some(handle) => handle,
            None => self.resolve_or_create_stream(stream_id)?,
        };
        self.add_part_us_for_handle(handle, duration_us, key, byte_len)
    }

    /// [`Self::add_part_us`] for one lifetime of a stream: fails with
    /// [`CacheError::StreamNotFound`] once the stream is gone.
    pub fn add_part_us_for_handle(
        &self,
        handle: M3u8StreamHandle,
        duration_us: u64,
        key: bool,
        byte_len: Option<usize>,
    ) -> Result<PartPosition, CacheError> {
        if !self.registry.is_current_fast(handle.0) {
            return Err(CacheError::StreamNotFound);
        }
        let index = handle.index();
        let position = {
            let mut guard = lock(&self.slots[index]);
            let slot = &mut *guard;
            // A reset replaces the slot under this lock before the new
            // owner's handle is published, so a slot of the handle's
            // generation belongs to the handle's stream.
            if slot.generation != handle.generation() {
                return Err(CacheError::StreamNotFound);
            }
            let options = self.options;
            let manifest = slot
                .manifest
                .get_or_insert_with(|| M3u8Manifest::new(options));
            if !manifest.opens_segment(duration_us, key)
                && manifest.open_parts() >= options.max_parts_per_segment
            {
                return Err(CacheError::IndexOutOfBounds);
            }
            let position =
                manifest.push_part_us(duration_us, key, None, byte_len.map(|len| len as u64));
            slot.version = next_version(slot.version);
            let end_sequence = position.sequence.saturating_add(1);
            match &mut slot.segment_ranges[position.segment_id % options.max_segments] {
                Some(range) if range.segment_id == position.segment_id => {
                    range.end_sequence = end_sequence;
                }
                range => {
                    *range = Some(SegmentRange {
                        segment_id: position.segment_id,
                        first_sequence: position.sequence.saturating_sub(position.index),
                        end_sequence,
                    });
                }
            }
            self.states[index].store(StateSnapshot {
                generation: slot.generation,
                version: slot.version,
                position: Some(PlaylistPosition {
                    segment_id: position.segment_id,
                    part_idx: position.index,
                    sequence: position.sequence,
                }),
            });
            position
        };
        self.notify_update(index);
        Ok(position)
    }

    /// The sequence numbers of the parts of segment `segment_id`,
    /// `[first, end)`, while the segment is retained.
    pub fn get_idxs(
        &self,
        stream_id: u64,
        segment_id: usize,
    ) -> Result<Option<(usize, usize)>, CacheError> {
        let Some(handle) = self.resolve_stream(stream_id) else {
            return Ok(None);
        };
        self.get_idxs_for_handle(handle, segment_id)
    }

    pub fn get_idxs_for_handle(
        &self,
        handle: M3u8StreamHandle,
        segment_id: usize,
    ) -> Result<Option<(usize, usize)>, CacheError> {
        self.checked(handle, |_| {
            let slot = lock(&self.slots[handle.index()]);
            if slot.generation != handle.generation() {
                return Err(CacheError::StreamNotFound);
            }
            Ok(slot.segment_ranges[segment_id % self.options.max_segments]
                .filter(|range| {
                    range.segment_id == segment_id && range.first_sequence < range.end_sequence
                })
                .map(|range| (range.first_sequence, range.end_sequence)))
        })
    }

    /// The compressed playlist that answers a request for part `part_idx` of
    /// segment `segment_id`, and the hash of the playlist text: the newest
    /// playlist, once it contains that part. `None` before then.
    pub fn get(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        let Some(handle) = self.resolve_stream(stream_id) else {
            return Ok(None);
        };
        self.get_for_handle(handle, segment_id, part_idx)
    }

    pub fn get_for_handle(
        &self,
        handle: M3u8StreamHandle,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        self.checked(handle, |state| {
            Ok(if includes(state.position, segment_id, part_idx) {
                self.full(handle, state)
            } else {
                None
            })
        })
    }

    /// The delta update (EXT-X-SKIP) of the playlist that answers a request
    /// for part `part_idx` of segment `segment_id`.
    pub fn get_delta(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        let Some(handle) = self.resolve_stream(stream_id) else {
            return Ok(None);
        };
        self.get_delta_for_handle(handle, segment_id, part_idx)
    }

    pub fn get_delta_for_handle(
        &self,
        handle: M3u8StreamHandle,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        self.checked(handle, |state| {
            if includes(state.position, segment_id, part_idx) {
                self.delta(handle, state)
            } else {
                Ok(None)
            }
        })
    }

    /// The newest compressed playlist and the hash of its text.
    pub fn last(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, CacheError> {
        self.last_for_handle(self.mapped(stream_id)?)
    }

    pub fn last_for_handle(
        &self,
        handle: M3u8StreamHandle,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        self.checked(handle, |state| Ok(self.full(handle, state)))
    }

    /// The newest playlist, uncompressed.
    pub fn last_text(&self, stream_id: u64) -> Result<Option<Bytes>, CacheError> {
        self.last_text_for_handle(self.mapped(stream_id)?)
    }

    pub fn last_text_for_handle(
        &self,
        handle: M3u8StreamHandle,
    ) -> Result<Option<Bytes>, CacheError> {
        self.checked(handle, |state| {
            Ok(self
                .fresh(handle, state, |rendered| rendered.text.clone())
                .or_else(|| self.render(handle).map(|rendered| rendered.text.clone())))
        })
    }

    /// The delta update of the newest playlist.
    pub fn last_delta(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, CacheError> {
        self.last_delta_for_handle(self.mapped(stream_id)?)
    }

    pub fn last_delta_for_handle(
        &self,
        handle: M3u8StreamHandle,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        self.checked(handle, |state| self.delta(handle, state))
    }

    /// Scan the slots and report retained encoded payload bytes.
    pub fn memory_stats(&self) -> M3u8CacheMemoryStats {
        let occupied_slots = self
            .slots
            .iter()
            .filter(|slot| lock(slot).manifest.is_some())
            .count();
        let encoded_playlist_bytes = self
            .rendered
            .iter()
            .filter_map(|rendered| rendered.load().as_ref().map(|rendered| rendered.bytes()))
            .fold(0_usize, usize::saturating_add);
        let initialization_bytes = self
            .inits
            .iter()
            .filter_map(|init| {
                init.read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .map(|init| init.bytes.len())
            })
            .fold(0_usize, usize::saturating_add);
        M3u8CacheMemoryStats {
            occupied_slots,
            encoded_playlist_bytes,
            initialization_bytes,
            maximum_payload_bytes: self
                .options
                .num_playlists
                .saturating_mul(self.max_rendering_bytes())
                .saturating_add(self.inits.len().saturating_mul(self.max_init_bytes)),
        }
    }

    /// A bound on the retained bytes of one stream's rendering: the text,
    /// and the compressed full and delta playlists of every replica.
    fn max_rendering_bytes(&self) -> usize {
        let part_lines = self
            .options
            .max_parts_per_segment
            .saturating_mul(PART_LINE_BOUND_BYTES);
        let text = self
            .options
            .max_segments
            .saturating_mul(part_lines.saturating_add(SEGMENT_TAGS_BOUND_BYTES))
            .saturating_add(HEAD_BOUND_BYTES)
            .saturating_add(OPEN_TAGS_BOUND_BYTES);
        let delta = text.saturating_add(SKIP_LINE_BOUND_BYTES);
        gzip_bound(text)
            .saturating_add(gzip_bound(delta))
            .saturating_mul(self.latest_read_replica_count)
            .saturating_add(text)
    }

    /// The handle of a mapped stream, or [`CacheError::StreamNotFound`].
    fn mapped(&self, stream_id: u64) -> Result<M3u8StreamHandle, CacheError> {
        self.resolve_stream(stream_id)
            .ok_or(CacheError::StreamNotFound)
    }

    /// Run a read of the handle's slot, given its published state. Fails
    /// with [`CacheError::StreamNotFound`] unless the handle is current both
    /// before and after the read.
    fn checked<R>(
        &self,
        handle: M3u8StreamHandle,
        read: impl FnOnce(StateSnapshot) -> Result<R, CacheError>,
    ) -> Result<R, CacheError> {
        if !self.registry.is_current_fast(handle.0) {
            return Err(CacheError::StreamNotFound);
        }
        let state = self.states[handle.index()].load();
        if state.generation != handle.generation() {
            return Err(CacheError::StreamNotFound);
        }
        let result = read(state)?;
        if !self.registry.is_current_fast(handle.0) {
            return Err(CacheError::StreamNotFound);
        }
        Ok(result)
    }

    /// Read the rendering of the slot's version in `state`, if a reader has
    /// made it. Takes no lock.
    fn fresh<R>(
        &self,
        handle: M3u8StreamHandle,
        state: StateSnapshot,
        read: impl FnOnce(&Rendered) -> R,
    ) -> Option<R> {
        let rendered = self.rendered[handle.index()].load();
        rendered
            .as_ref()
            .filter(|rendered| {
                rendered.generation == state.generation && rendered.version == state.version
            })
            .map(|rendered| read(rendered))
    }

    fn full(&self, handle: M3u8StreamHandle, state: StateSnapshot) -> Option<(Bytes, u64)> {
        self.fresh(handle, state, |rendered| rendered.full.pick())
            .or_else(|| self.render(handle).map(|rendered| rendered.full.pick()))
    }

    fn delta(
        &self,
        handle: M3u8StreamHandle,
        state: StateSnapshot,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        if let Some(delta) = self
            .fresh(handle, state, |rendered| {
                rendered
                    .delta
                    .get()
                    .map(|delta| delta.as_ref().map(Variants::pick))
            })
            .flatten()
        {
            return Ok(delta);
        }
        let Some(rendered) = self.render(handle) else {
            return Ok(None);
        };
        if let Some(delta) = rendered.delta.get() {
            return Ok(delta.as_ref().map(Variants::pick));
        }
        let _render = lock(&self.render_locks[handle.index()]);
        if let Some(delta) = rendered.delta.get() {
            return Ok(delta.as_ref().map(Variants::pick));
        }
        let text = std::str::from_utf8(&rendered.text).map_err(|_| {
            CacheError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "playlist is not UTF-8",
            ))
        })?;
        let delta = match playlist_delta_update(text) {
            Some(delta) => Some(Variants::new(
                Bytes::from(compress_data(delta.as_bytes())?),
                xxh3_64(delta.as_bytes()),
                self.latest_read_replica_count,
            )),
            None => None,
        };
        let delta = rendered.delta.get_or_init(|| delta);
        Ok(delta.as_ref().map(Variants::pick))
    }

    /// The newest playlist of the handle's slot, rendered now if no reader
    /// has rendered it since the last change. `None` before the first part,
    /// or when the slot has been given to another stream.
    fn render(&self, handle: M3u8StreamHandle) -> Option<Arc<Rendered>> {
        let index = handle.index();
        let generation = handle.generation();
        let current = |slot: &PlaylistSlot| {
            self.rendered[index].load_full().filter(|rendered| {
                rendered.generation == slot.generation && rendered.version == slot.version
            })
        };
        let _render = lock(&self.render_locks[index]);
        let (version, pieces) = {
            let slot = lock(&self.slots[index]);
            if slot.generation != generation {
                return None;
            }
            if let Some(rendered) = current(&slot) {
                return Some(rendered);
            }
            (slot.version, slot.manifest.as_ref()?.pieces())
        };
        let text = pieces.into_bytes();
        let hash = xxh3_64(&text);
        let gzip = Bytes::from(compress_data(&text).ok()?);
        let rendered = Arc::new(Rendered {
            generation,
            version,
            full: Variants::new(gzip, hash, self.latest_read_replica_count),
            text,
            delta: OnceLock::new(),
        });
        let previous = {
            let slot = lock(&self.slots[index]);
            (slot.generation == generation)
                .then(|| self.rendered[index].swap(Some(Arc::clone(&rendered))))
        };
        drop(previous);
        Some(rendered)
    }

    fn notify_update(&self, index: usize) {
        self.update_notifiers[index].notify_waiters();
    }

    /// Give slot `index` to generation `generation`, with no playlist and no
    /// initialization. Runs under the registry's owner lock, before the
    /// generation is published.
    fn reset_index_state(&self, index: usize, generation: u64) {
        let previous_init = self.inits[index]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let (previous_manifest, previous_rendering) = {
            let mut slot = lock(&self.slots[index]);
            slot.generation = generation;
            slot.version = next_version(slot.version);
            slot.segment_ranges.fill(None);
            self.states[index].store(StateSnapshot {
                generation,
                version: slot.version,
                position: None,
            });
            (slot.manifest.take(), self.rendered[index].swap(None))
        };
        drop((previous_init, previous_manifest, previous_rendering));
        self.notify_update(index);
    }
}

/// Whether a playlist whose newest part is at `position` contains part
/// `part_idx` of segment `segment_id`.
fn includes(position: Option<PlaylistPosition>, segment_id: usize, part_idx: usize) -> bool {
    position.is_some_and(|position| {
        segment_id < position.segment_id
            || (segment_id == position.segment_id && part_idx <= position.part_idx)
    })
}

fn next_version(version: u64) -> u64 {
    let next = version.wrapping_add(1);
    if next == 0 {
        1
    } else {
        next
    }
}

/// A bound on the gzip of `len` bytes (zlib's `compressBound`, with the gzip
/// header and trailer).
fn gzip_bound(len: usize) -> usize {
    len.saturating_add(len >> 12)
        .saturating_add(len >> 14)
        .saturating_add(len >> 25)
        .saturating_add(25)
}

fn compress_data(data: &[u8]) -> Result<Vec<u8>, CacheError> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(data.len() / 4 + 64), PLAYLIST_GZIP_LEVEL);
    encoder.write_all(data)?;
    Ok(encoder.finish()?)
}

#[cfg(test)]
fn decompress_data(data: &[u8]) -> Result<String, CacheError> {
    let mut decoder = flate2::read::GzDecoder::new(data);
    let mut decoded = String::new();
    decoder.read_to_string(&mut decoded)?;
    Ok(decoded)
}

fn playlist_delta_update(playlist: &str) -> Option<String> {
    if playlist.contains("#EXT-X-ENDLIST") || playlist.contains("#EXT-X-SKIP:") {
        return None;
    }

    let skip_boundary_us = parse_can_skip_until_us(playlist)?;
    let lines: Vec<&str> = playlist.lines().collect();
    let (blocks, trailing_start, trailing_part_duration_us) = parse_media_timeline(&lines);
    let insert_at = blocks
        .first()
        .map(|block| block.start)
        .unwrap_or(trailing_start);

    let total_parent_duration_us = blocks
        .iter()
        .map(|block| block.duration_us)
        .fold(trailing_part_duration_us, u64::saturating_add);

    let mut skipped_segments = 0;
    let mut elapsed_us = 0_u64;
    for block in &blocks {
        elapsed_us = elapsed_us.saturating_add(block.duration_us);
        if total_parent_duration_us.saturating_sub(elapsed_us) > skip_boundary_us {
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

fn parse_can_skip_until_us(playlist: &str) -> Option<u64> {
    playlist
        .lines()
        .find_map(|line| {
            line.strip_prefix("#EXT-X-SERVER-CONTROL:")
                .and_then(|attributes| parse_attribute_value(attributes, "CAN-SKIP-UNTIL"))
        })
        .and_then(parse_duration_us)
}

fn parse_media_timeline(lines: &[&str]) -> (Vec<MediaSegmentBlock>, usize, u64) {
    let mut blocks = Vec::new();
    let mut current_start = None;
    let mut current_duration_us = None;

    for (idx, line) in lines.iter().enumerate() {
        if is_segment_scoped_tag(line) {
            current_start.get_or_insert(idx);
        }

        if let Some(duration_us) = parse_extinf_duration_us(line) {
            current_start.get_or_insert(idx);
            current_duration_us = Some(duration_us);
        }

        if is_uri_line(line) {
            if let (Some(start), Some(duration_us)) =
                (current_start.take(), current_duration_us.take())
            {
                blocks.push(MediaSegmentBlock { start, duration_us });
            }
        }
    }

    let trailing_start = current_start.unwrap_or(lines.len());
    let trailing_part_duration_us = lines[trailing_start..]
        .iter()
        .filter_map(|line| parse_part_duration_us(line))
        .fold(0_u64, u64::saturating_add);

    (blocks, trailing_start, trailing_part_duration_us)
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

fn parse_extinf_duration_us(line: &str) -> Option<u64> {
    line.strip_prefix("#EXTINF:")
        .and_then(|value| value.split_once(',').map(|(duration, _)| duration))
        .and_then(parse_duration_us)
}

fn parse_part_duration_us(line: &str) -> Option<u64> {
    line.strip_prefix("#EXT-X-PART:")
        .and_then(|attributes| parse_attribute_value(attributes, "DURATION"))
        .and_then(parse_duration_us)
}

/// Decimal seconds as exact microseconds; a value in another notation (an
/// exponent, say) through floating point.
fn parse_duration_us(value: &str) -> Option<u64> {
    parse_seconds_us(value).or_else(|| {
        value
            .parse::<f64>()
            .ok()
            .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
            .map(|seconds| (seconds * 1_000_000.0).round() as u64)
    })
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
    use std::time::Duration;

    fn decompress(data: &[u8]) -> String {
        decompress_data(data).unwrap()
    }

    /// Options under which every independent part of a second opens a
    /// segment.
    fn one_second_segments(max_segments: usize) -> Options {
        Options {
            max_segments,
            segment_min_ms: 1_000,
            target_duration_ms: 1_000,
            part_target_us: 1_000_000,
            ..Options::default()
        }
    }

    fn rendered_of(cache: &M3u8Cache, handle: M3u8StreamHandle) -> Option<Arc<Rendered>> {
        cache.rendered[handle.index()].load_full()
    }

    #[test]
    fn reused_stream_slot_does_not_expose_previous_playlist_data() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 2,
            ..Options::default()
        });

        cache.add_part_us(1, 2_500, true, None).unwrap();
        assert!(cache.get(1, 1, 0).unwrap().is_some());

        cache.ensure_stream_id(2).unwrap();
        assert!(cache.get(1, 1, 0).unwrap().is_none());
        assert!(cache.get(2, 1, 0).unwrap().is_none());
        assert!(matches!(cache.last(1), Err(CacheError::StreamNotFound)));
        assert_eq!(cache.last(2).unwrap(), None);
        assert_eq!(cache.memory_stats().encoded_playlist_bytes, 0);
    }

    #[test]
    fn latest_read_replica_count_is_bounded() {
        let options = Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 1,
            ..Options::default()
        };

        assert_eq!(
            M3u8Cache::new_with_latest_read_replicas(options, 0).latest_read_replica_count(),
            1
        );
        assert_eq!(
            M3u8Cache::new_with_latest_read_replicas(options, usize::MAX)
                .latest_read_replica_count(),
            MAX_LATEST_READ_REPLICAS
        );
    }

    #[test]
    fn tracks_last_playlist_position() {
        let cache = M3u8Cache::new(Options::default());

        assert_eq!(cache.last_position(7), None);
        cache.ensure_stream_id(7).unwrap();
        // A stream has no position before its first part.
        assert_eq!(cache.last_position(7), None);

        cache.add_part_us(7, 2_500, true, None).unwrap();
        assert_eq!(cache.last_position(7), Some((1, 0)));

        cache.add_part_us(7, 2_500, false, None).unwrap();
        assert_eq!(cache.last_position(7), Some((1, 1)));
        assert_eq!(
            cache.position(7),
            Some(PlaylistPosition {
                segment_id: 1,
                part_idx: 1,
                sequence: 2,
            })
        );
    }

    #[test]
    fn exposes_current_open_segment_part_range() {
        let cache = M3u8Cache::new(one_second_segments(10));

        // Segments 1 to 4 of one part each (sequences 1 to 4), then segment
        // 5, open, with sequences 5 and 6.
        for _ in 0..5 {
            cache.add_part_us(7, 1_000_000, true, None).unwrap();
        }
        cache.add_part_us(7, 100_000, false, None).unwrap();

        assert_eq!(cache.last_position(7), Some((5, 1)));
        assert_eq!(cache.get_idxs(7, 5).unwrap(), Some((5, 7)));
        assert_eq!(cache.get_idxs(7, 4).unwrap(), Some((4, 5)));
        assert_eq!(cache.get_idxs(7, 6).unwrap(), None);
        assert_eq!(cache.get_idxs(8, 5).unwrap(), None);
    }

    #[test]
    fn open_segment_range_ignores_stale_ring_boundary() {
        let cache = M3u8Cache::new(one_second_segments(4));

        // Segments 1 to 3 of one part, then segment 4 of two.
        for _ in 0..4 {
            cache.add_part_us(7, 1_000_000, true, None).unwrap();
        }
        cache.add_part_us(7, 100_000, false, None).unwrap();
        assert_eq!(cache.last_position(7), Some((4, 1)));
        assert_eq!(cache.get_idxs(7, 4).unwrap(), Some((4, 6)));
        assert_eq!(cache.get_idxs(7, 1).unwrap(), Some((1, 2)));

        // Segment 5 takes segment 1's ring position: segment 1 is gone, and
        // its range is never read as segment 5's or the other way round.
        cache.add_part_us(7, 1_000_000, true, None).unwrap();
        assert_eq!(cache.last_position(7), Some((5, 0)));
        assert_eq!(cache.get_idxs(7, 5).unwrap(), Some((6, 7)));
        assert_eq!(cache.get_idxs(7, 1).unwrap(), None);
        assert_eq!(cache.get_idxs(7, 4).unwrap(), Some((4, 6)));
    }

    #[test]
    fn a_segment_range_is_bound_to_its_stream_lifetime() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        let old = cache.resolve_or_create_stream(1).unwrap();
        cache
            .add_part_us_for_handle(old, 2_500, true, None)
            .unwrap();
        assert_eq!(cache.get_idxs_for_handle(old, 1).unwrap(), Some((1, 2)));

        let current = cache.resolve_or_create_stream(2).unwrap();
        assert!(matches!(
            cache.get_idxs_for_handle(old, 1),
            Err(CacheError::StreamNotFound)
        ));
        assert_eq!(cache.get_idxs_for_handle(current, 1).unwrap(), None);
        assert_eq!(cache.position_for_handle(old), None);
        assert_eq!(cache.version_for_handle(old), None);
    }

    #[test]
    fn delta_update_replaces_segments_older_than_skip_boundary() {
        let cache = M3u8Cache::new(one_second_segments(10));
        for _ in 0..12 {
            cache.add_part_us(1, 1_000_000, true, None).unwrap();
        }

        let (delta, hash) = cache.last_delta(1).unwrap().unwrap();
        let delta = decompress(&delta);
        assert_eq!(xxh3_64(delta.as_bytes()), hash);

        assert!(delta.contains("#EXT-X-VERSION:9"));
        assert!(delta.contains("#EXT-X-SERVER-CONTROL:"));
        assert!(delta.contains("CAN-SKIP-UNTIL=6.000"));
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
        // Too short a playlist for CAN-SKIP-UNTIL.
        for _ in 0..3 {
            cache.add_part_us(1, 2_500, true, None).unwrap();
        }
        let playlist = cache.last_text(1).unwrap().unwrap();
        assert!(!std::str::from_utf8(&playlist)
            .unwrap()
            .contains("CAN-SKIP-UNTIL"));

        assert!(cache.last_delta(1).unwrap().is_none());
        assert!(cache.get_delta(1, 1, 2).unwrap().is_none());
    }

    #[test]
    fn latest_read_replicas_preserve_bytes_and_report_their_memory() {
        let options = Options {
            num_playlists: 1,
            buffer_size_kb: 64,
            ..one_second_segments(10)
        };
        let baseline = M3u8Cache::new(options);
        let replicated = M3u8Cache::new_with_latest_read_replicas(options, 4);
        for _ in 0..12 {
            baseline.add_part_us(1, 1_000_000, true, Some(40)).unwrap();
            replicated
                .add_part_us(1, 1_000_000, true, Some(40))
                .unwrap();
        }
        let handle = replicated.resolve_stream(1).expect("replicated handle");

        let (full, full_hash) = replicated.last_for_handle(handle).unwrap().unwrap();
        let (delta, delta_hash) = replicated.last_delta_for_handle(handle).unwrap().unwrap();
        let text = replicated.last_text_for_handle(handle).unwrap().unwrap();
        assert_eq!(decompress(&full).as_bytes(), &text[..]);
        assert_eq!(xxh3_64(&text), full_hash);
        assert_eq!(xxh3_64(decompress(&delta).as_bytes()), delta_hash);

        // Every replica holds the same bytes, in its own allocation.
        let rendered = rendered_of(&replicated, handle).unwrap();
        let replicas = [
            &rendered.full,
            rendered.delta.get().unwrap().as_ref().unwrap(),
        ];
        for variants in replicas {
            assert_eq!(variants.gzip.len(), 4);
            for (n, copy) in variants.gzip.iter().enumerate() {
                assert_eq!(copy, &variants.gzip[0]);
                for other in &variants.gzip[n + 1..] {
                    assert_ne!(copy.as_ptr(), other.as_ptr());
                }
            }
        }
        for _ in 0..128 {
            assert_eq!(
                replicated.last_for_handle(handle).unwrap(),
                Some((full.clone(), full_hash))
            );
            assert_eq!(
                replicated.last_delta_for_handle(handle).unwrap(),
                Some((delta.clone(), delta_hash))
            );
        }

        let memory = replicated.memory_stats();
        assert_eq!(
            memory.encoded_playlist_bytes,
            text.len() + 4 * (full.len() + delta.len())
        );
        let baseline_memory = {
            baseline.last(1).unwrap().unwrap();
            baseline.last_delta(1).unwrap().unwrap();
            baseline.memory_stats()
        };
        assert!(memory.maximum_payload_bytes > baseline_memory.maximum_payload_bytes);
        assert!(memory.encoded_playlist_bytes <= memory.maximum_payload_bytes);
        assert!(
            baseline_memory.encoded_playlist_bytes < memory.encoded_playlist_bytes,
            "replicas are counted"
        );

        replicated.ensure_stream_id(2).expect("reuse stream slot");
        let replacement = replicated.resolve_stream(2).expect("replacement handle");
        assert!(replicated.last_for_handle(handle).is_err());
        assert_eq!(replicated.last_for_handle(replacement).unwrap(), None);
        assert_eq!(replicated.memory_stats().encoded_playlist_bytes, 0);
    }

    #[test]
    fn concurrent_latest_replica_reads_return_complete_variants() {
        let options = Options {
            num_playlists: 1,
            part_target_us: 2_500,
            ..one_second_segments(10)
        };
        let cache = Arc::new(M3u8Cache::new_with_latest_read_replicas(options, 8));
        for _ in 0..12 {
            cache.add_part_us(1, 1_000_000, true, None).unwrap();
        }
        let handle = cache.resolve_stream(1).expect("stream handle");
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Parts keep arriving while the readers read: each read is a whole
        // playlist of one version, in whichever replica the reader takes.
        let writer = {
            let cache = Arc::clone(&cache);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                for part in 0..2_000_u64 {
                    cache
                        .add_part_us_for_handle(handle, 2_500, part % 40 == 0, None)
                        .expect("writer part");
                }
                done.store(true, Ordering::Release);
            })
        };
        let readers = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let done = Arc::clone(&done);
                std::thread::spawn(move || {
                    let mut reads = 0;
                    while !done.load(Ordering::Acquire) || reads < 100 {
                        let (full, hash) = cache.last_for_handle(handle).unwrap().unwrap();
                        let text = decompress(&full);
                        assert_eq!(xxh3_64(text.as_bytes()), hash);
                        assert!(text.starts_with("#EXTM3U\n"));
                        assert!(text.ends_with('\n'));
                        let (delta, delta_hash) =
                            cache.last_delta_for_handle(handle).unwrap().unwrap();
                        let delta = decompress(&delta);
                        assert_eq!(xxh3_64(delta.as_bytes()), delta_hash);
                        assert_eq!(delta.matches("#EXT-X-SKIP:").count(), 1);
                        reads += 1;
                    }
                })
            })
            .collect::<Vec<_>>();
        writer.join().expect("writer completed");
        for reader in readers {
            reader.join().expect("replica reader completed");
        }
    }

    #[test]
    fn rejected_mapped_writes_do_not_evict_a_live_stream() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            max_parts_per_segment: 1,
            init_size_kb: 1,
            ..Options::default()
        });
        cache.add_part_us(1, 2_500, true, None).unwrap();
        let live = cache.resolve_stream(1).unwrap();
        let version = cache.version_for_handle(live).unwrap();

        // A part the open segment cannot take adds nothing.
        assert!(matches!(
            cache.add_part_us(1, 2_500, false, None),
            Err(CacheError::IndexOutOfBounds)
        ));
        // An initialization that is too large is refused before anything
        // is written, and one for an unknown stream creates nothing.
        assert!(matches!(
            cache.set_init(1, Bytes::from(vec![0_u8; 1025])),
            Err(CacheError::BufferOverflow)
        ));
        assert!(matches!(
            cache.set_init(2, Bytes::from_static(b"init")),
            Err(CacheError::StreamNotFound)
        ));
        // Reads never create a stream.
        assert_eq!(cache.get(3, 1, 0).unwrap(), None);
        assert_eq!(cache.get_idxs(3, 1).unwrap(), None);
        assert!(matches!(cache.last(3), Err(CacheError::StreamNotFound)));
        assert!(matches!(cache.get_init(3), Err(CacheError::StreamNotFound)));

        assert_eq!(cache.resolve_stream(1), Some(live));
        assert_eq!(cache.resolve_stream(2), None);
        assert_eq!(cache.resolve_stream(3), None);
        assert_eq!(cache.version_for_handle(live), Some(version));
        assert_eq!(cache.last_position(1), Some((1, 0)));
        let text = cache.last_text_for_handle(live).unwrap().unwrap();
        assert_eq!(
            cache.last_for_handle(live).unwrap().unwrap().1,
            xxh3_64(&text)
        );
    }

    #[test]
    fn constructor_rejects_capacity_arithmetic_overflow() {
        assert!(matches!(
            M3u8Cache::try_new(Options {
                num_playlists: usize::MAX,
                max_segments: 2,
                ..Options::default()
            }),
            Err(CacheError::ArithmeticOverflow)
        ));
        assert!(matches!(
            M3u8Cache::try_new(Options {
                init_size_kb: usize::MAX,
                ..Options::default()
            }),
            Err(CacheError::ArithmeticOverflow)
        ));
    }

    #[test]
    fn encoded_payload_memory_plateaus_after_ring_rotations() {
        let options = Options {
            num_playlists: 2,
            max_segments: 2,
            max_parts_per_segment: 4,
            segment_min_ms: 1,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);
        let mut plateau = None;
        for part in 0..1_000_u64 {
            for stream in 1..=2 {
                cache
                    .add_part_us(stream, 1_000, true, Some(1 << 20))
                    .unwrap();
                cache.last(stream).unwrap().unwrap();
                cache.last_delta(stream).unwrap();
            }
            if part == 100 {
                plateau = Some(cache.memory_stats().encoded_playlist_bytes);
            }
        }

        let stats = cache.memory_stats();
        assert_eq!(stats.occupied_slots, 2);
        assert!(stats.encoded_playlist_bytes > 0);
        // One rendering per stream, however many parts: the bytes stay
        // where they were after the window filled (the numbers grow a
        // digit or two).
        let plateau = plateau.unwrap();
        assert!(stats.encoded_playlist_bytes <= plateau + plateau / 4);
        assert!(
            stats.encoded_playlist_bytes + stats.initialization_bytes
                <= stats.maximum_payload_bytes
        );

        cache.zero_stream_id(1);
        cache.zero_stream_id(2);
        let stats = cache.memory_stats();
        assert_eq!(stats.occupied_slots, 0);
        assert_eq!(stats.encoded_playlist_bytes, 0);
    }

    #[test]
    fn the_rendering_bound_holds_for_the_largest_lines() {
        let options = Options {
            num_playlists: 1,
            max_segments: 3,
            max_parts_per_segment: 8,
            segment_min_ms: 1,
            parted_target_durations: 0,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);
        // Long parts with large byte ranges: every number is wide.
        for part in 0..64_u64 {
            cache
                .add_part_us(1, 999_999, part % 8 == 0, Some(usize::MAX / 4))
                .unwrap();
        }
        cache.last(1).unwrap().unwrap();
        cache.last_delta(1).unwrap();
        let stats = cache.memory_stats();
        assert!(stats.encoded_playlist_bytes <= stats.maximum_payload_bytes);
        let text = cache.last_text(1).unwrap().unwrap();
        let part_lines = text
            .split(|byte| *byte == b'\n')
            .filter(|line| line.starts_with(b"#EXT-X-PART:"))
            .collect::<Vec<_>>();
        assert!(!part_lines.is_empty());
        assert!(part_lines
            .iter()
            .all(|line| line.len() < PART_LINE_BOUND_BYTES));
    }

    #[test]
    fn new_and_reused_streams_have_no_initialization() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        cache.ensure_stream_id(1).unwrap();
        assert!(matches!(cache.get_init(1), Err(CacheError::StreamNotFound)));
        cache
            .set_init(1, Bytes::from_static(b"first-init"))
            .unwrap();
        assert_eq!(
            cache.get_init(1).unwrap(),
            Bytes::from_static(b"first-init")
        );

        cache.ensure_stream_id(2).unwrap();
        assert!(matches!(cache.get_init(1), Err(CacheError::StreamNotFound)));
        assert!(matches!(cache.get_init(2), Err(CacheError::StreamNotFound)));
    }

    #[test]
    fn stale_handle_cannot_replace_new_playlist_or_initialization() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        let old = cache.resolve_or_create_stream(1).unwrap();
        cache
            .add_part_us_for_handle(old, 2_500, true, None)
            .unwrap();
        cache
            .set_init_for_handle(old, Bytes::from_static(b"old-init"))
            .unwrap();

        let current = cache.resolve_or_create_stream(2).unwrap();
        cache
            .add_part_us_for_handle(current, 2_500, true, Some(7))
            .unwrap();
        cache
            .set_init_for_handle(current, Bytes::from_static(b"current-init"))
            .unwrap();

        // Writes delayed past the reuse, with the old handle.
        assert!(matches!(
            cache.add_part_us_for_handle(old, 2_500, true, None),
            Err(CacheError::StreamNotFound)
        ));
        assert!(matches!(
            cache.set_init_for_handle(old, Bytes::from_static(b"stale-init")),
            Err(CacheError::StreamNotFound)
        ));
        assert!(matches!(
            cache.last_for_handle(old),
            Err(CacheError::StreamNotFound)
        ));
        assert_eq!(cache.get_init_for_handle(old), None);

        let (playlist, _) = cache.last_for_handle(current).unwrap().unwrap();
        let playlist = decompress(&playlist);
        assert_eq!(playlist.matches("#EXT-X-PART:").count(), 1);
        assert!(playlist.contains("BYTERANGE=\"7@0\""));
        assert_eq!(cache.last_position(2), Some((1, 0)));
        assert_eq!(
            cache.get_init_for_handle(current).unwrap(),
            Bytes::from_static(b"current-init")
        );
    }

    #[test]
    fn stream_handle_is_bound_to_its_cache() {
        let first = M3u8Cache::new(Options::default());
        let second = M3u8Cache::new(Options::default());
        let handle = first.resolve_or_create_stream(3).unwrap();
        let own = second.resolve_or_create_stream(3).unwrap();
        // The same slot and generation in both caches.
        assert_eq!(
            (handle.index(), handle.generation()),
            (own.index(), own.generation())
        );

        assert!(matches!(
            second.add_part_us_for_handle(handle, 2_500, true, None),
            Err(CacheError::StreamNotFound)
        ));
        assert!(matches!(
            second.last_for_handle(handle),
            Err(CacheError::StreamNotFound)
        ));
        assert_eq!(second.position(3), None);
    }

    #[tokio::test]
    async fn update_notifier_wakes_for_publication_and_stream_reset() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        let handle = cache.resolve_or_create_stream(41).unwrap();
        let notifier = cache.update_notifier_for_handle(handle).unwrap();
        let published = notifier.notified();
        tokio::pin!(published);
        published.as_mut().enable();

        cache
            .add_part_us_for_handle(handle, 2_500, true, None)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), published)
            .await
            .expect("a new part must wake an enabled waiter");

        let initialized = notifier.notified();
        tokio::pin!(initialized);
        initialized.as_mut().enable();
        cache
            .set_init_for_handle(handle, Bytes::from_static(b"init"))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), initialized)
            .await
            .expect("a new initialization must wake an enabled waiter");

        let reset = notifier.notified();
        tokio::pin!(reset);
        reset.as_mut().enable();
        cache.zero_stream_id(41);
        tokio::time::timeout(Duration::from_secs(1), reset)
            .await
            .expect("stream reset must wake an enabled waiter");
        assert!(cache.update_notifier_for_handle(handle).is_none());
    }

    #[tokio::test]
    async fn a_render_on_read_is_not_published_by_notifier_but_the_part_is() {
        let cache = M3u8Cache::new(Options::default());
        let handle = cache.resolve_or_create_stream(5).unwrap();
        let notifier = cache.update_notifier_for_handle(handle).unwrap();
        let before = cache.version_for_handle(handle).unwrap();

        let part = notifier.notified();
        tokio::pin!(part);
        part.as_mut().enable();
        cache
            .add_part_us_for_handle(handle, 2_500, true, None)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), part)
            .await
            .expect("the part wakes the waiter");
        let version = cache.version_for_handle(handle).unwrap();
        assert_ne!(version, before);
        // Nothing was rendered for the part.
        assert!(rendered_of(&cache, handle).is_none());

        let render = notifier.notified();
        tokio::pin!(render);
        render.as_mut().enable();
        cache.last_for_handle(handle).unwrap().unwrap();
        cache.last_delta_for_handle(handle).unwrap();
        assert_eq!(rendered_of(&cache, handle).unwrap().version, version);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), render)
                .await
                .is_err(),
            "a render does not wake waiters"
        );
        assert_eq!(cache.version_for_handle(handle), Some(version));
    }

    #[test]
    fn concurrent_part_adds_publish_monotonic_positions() {
        let cache = Arc::new(M3u8Cache::new(Options {
            num_playlists: 2,
            ..Options::default()
        }));
        let handle = cache.resolve_or_create_stream(7).unwrap();
        let max_parts = cache.options().max_parts_per_segment;
        let max_segments = cache.options().max_segments;
        const PARTS_PER_WRITER: usize = 5_000;
        let done = Arc::new(AtomicUsize::new(0));

        let writers = (0..2)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let done = Arc::clone(&done);
                std::thread::spawn(move || {
                    for part in 0..PARTS_PER_WRITER {
                        cache
                            .add_part_us_for_handle(handle, 2_500, part % 50 == 0, None)
                            .expect("part added");
                    }
                    done.fetch_add(1, Ordering::AcqRel);
                })
            })
            .collect::<Vec<_>>();
        let readers = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let done = Arc::clone(&done);
                std::thread::spawn(move || {
                    let mut last: Option<PlaylistPosition> = None;
                    let mut last_version = 0_u64;
                    let mut firsts = std::collections::HashMap::new();
                    while done.load(Ordering::Acquire) < 2 {
                        let version = cache.version_for_handle(handle).unwrap();
                        assert!(version >= last_version, "version went back");
                        last_version = version;
                        let Some(position) = cache.position_for_handle(handle) else {
                            continue;
                        };
                        if let Some(last) = last {
                            assert!(position.sequence >= last.sequence, "position went back");
                            assert!(position.segment_id >= last.segment_id);
                            if position.segment_id == last.segment_id {
                                assert!(position.part_idx >= last.part_idx);
                            }
                        }
                        last = Some(position);
                        // The range of the newest part's segment is read
                        // after the position: it holds that part, starts
                        // where it always started, and is never torn.
                        let Some((first, end)) = cache
                            .get_idxs_for_handle(handle, position.segment_id)
                            .unwrap()
                        else {
                            // Only a segment whose ring position a newer
                            // one has taken since has no range.
                            let now = cache.position_for_handle(handle).unwrap();
                            assert!(now.segment_id >= position.segment_id + max_segments);
                            continue;
                        };
                        assert_eq!(first, position.sequence - position.part_idx);
                        assert!(position.sequence < end);
                        assert!(end - first <= max_parts);
                        assert_eq!(*firsts.entry(position.segment_id).or_insert(first), first);
                    }
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().expect("writer completed");
        }
        for reader in readers {
            reader.join().expect("reader completed");
        }
        let position = cache.position_for_handle(handle).unwrap();
        assert_eq!(position.sequence, 2 * PARTS_PER_WRITER);
        let (first, end) = cache
            .get_idxs_for_handle(handle, position.segment_id)
            .unwrap()
            .unwrap();
        assert_eq!(end, 2 * PARTS_PER_WRITER + 1);
        assert_eq!(first, position.sequence - position.part_idx);
    }

    #[test]
    fn a_part_is_added_without_rendering_and_read_renders_once() {
        let cache = M3u8Cache::new(Options::default());
        for _ in 0..800 {
            cache.add_part_us(9, 2_500, true, Some(40)).unwrap();
        }
        let handle = cache.resolve_stream(9).unwrap();
        assert!(rendered_of(&cache, handle).is_none(), "rendered on add");
        assert_eq!(cache.last_position(9), Some((2, 399)));

        let (first, hash) = cache.last(9).unwrap().unwrap();
        let (again, again_hash) = cache.last(9).unwrap().unwrap();
        // The second reader is given the same rendering.
        assert_eq!(first.as_ptr(), again.as_ptr());
        assert_eq!(hash, again_hash);
        let playlist = decompress(&first);
        assert!(playlist.contains("#EXT-X-PART-INF:PART-TARGET=0.0025\n"));
        assert!(playlist.contains("#EXTINF:1.000,\n#EXT-X-BYTERANGE:16000@0\ns1.mp4"));
        assert!(playlist.contains("URI=\"s2.mp4\",BYTERANGE=\"40@15960\",INDEPENDENT=YES"));
        assert_eq!(xxh3_64(playlist.as_bytes()), hash);

        // A new part makes a new rendering that contains it.
        cache.add_part_us(9, 2_500, true, Some(40)).unwrap();
        let (next, _) = cache.get(9, 3, 0).unwrap().unwrap();
        assert_ne!(next.as_ptr(), first.as_ptr());
        assert!(decompress(&next).contains("#EXTINF:1.000,\n#EXT-X-BYTERANGE:16000@0\ns2.mp4"));
        // A part that is not there yet has no playlist.
        assert!(cache.get(9, 3, 1).unwrap().is_none());
        // A part that is there is answered with the newest playlist.
        assert_eq!(
            cache.get(9, 2, 17).unwrap().unwrap().0.as_ptr(),
            next.as_ptr()
        );
        assert_eq!(cache.get_idxs(9, 2).unwrap(), Some((401, 801)));
    }

    #[test]
    fn a_segment_is_capped_at_its_part_capacity() {
        let cache = M3u8Cache::new(Options {
            max_parts_per_segment: 4,
            ..Options::default()
        });
        for _ in 0..4 {
            cache.add_part_us(9, 2_500, false, None).unwrap();
        }
        assert!(matches!(
            cache.add_part_us(9, 2_500, false, None),
            Err(CacheError::IndexOutOfBounds)
        ));
        // Nothing was added for the refused part.
        assert_eq!(cache.last_position(9), Some((1, 3)));
    }

    #[test]
    fn non_independent_input_cannot_grow_an_open_segment_without_bound() {
        let options = Options {
            max_segments: 4,
            max_parts_per_segment: 4,
            segment_min_ms: u32::MAX,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);
        let handle = cache.resolve_or_create_stream(1).unwrap();
        let mut refused = 0;
        for _ in 0..10_000 {
            match cache.add_part_us_for_handle(handle, 1_000, false, None) {
                Ok(_) => {}
                Err(CacheError::IndexOutOfBounds) => refused += 1,
                Err(err) => panic!("unexpected {err}"),
            }
        }

        // The open segment took its capacity and refused the rest.
        assert_eq!(refused, 10_000 - options.max_parts_per_segment);
        assert_eq!(cache.last_position(1), Some((1, 3)));
        let playlist = cache.last_text_for_handle(handle).unwrap().unwrap();
        let playlist = std::str::from_utf8(&playlist).unwrap();
        assert_eq!(playlist.matches("#EXT-X-PART:").count(), 4);
        assert!(playlist.len() < 16 * 1024);
    }

    #[test]
    fn a_full_segment_refuses_non_independent_parts() {
        let cache = M3u8Cache::new(Options {
            max_parts_per_segment: 4,
            segment_min_ms: 10,
            ..Options::default()
        });
        for _ in 0..4 {
            cache.add_part_us(9, 3_000, false, None).unwrap();
        }
        // 12 ms of parts: the segment is full and long enough to close.
        assert!(matches!(
            cache.add_part_us(9, 3_000, false, None),
            Err(CacheError::IndexOutOfBounds)
        ));
        assert_eq!(cache.last_position(9), Some((1, 3)));
        // An independent part closes it and opens the next segment.
        let position = cache.add_part_us(9, 3_000, true, None).unwrap();
        assert!(position.new_segment);
        assert_eq!((position.segment_id, position.index), (2, 0));
        assert_eq!(cache.get_idxs(9, 1).unwrap(), Some((1, 5)));
        assert_eq!(cache.get_idxs(9, 2).unwrap(), Some((5, 6)));
    }

    #[test]
    fn default_options_take_every_frame_of_a_long_gop() {
        // 60 fps with a key frame every 2 s, and 23.976 fps with one every 48
        // frames: every frame is taken, for ten seconds.
        for (fps_num, fps_den, gop, part_target_us) in [
            (60_u64, 1_u64, 120_u64, 16_667_u32),
            (24_000, 1_001, 48, 41_709),
        ] {
            let cache = M3u8Cache::new(Options {
                part_target_us,
                ..Options::default()
            });
            let mut elapsed = 0_u64;
            let frames = 10 * fps_num / fps_den;
            for frame in 0..frames {
                let end = (frame + 1) * 1_000_000 * fps_den / fps_num;
                cache
                    .add_part_us(9, end - elapsed, frame % gop == 0, None)
                    .unwrap_or_else(|err| panic!("frame {frame} refused: {err}"));
                elapsed = end;
            }
            let playlist = decompress(&cache.last(9).unwrap().unwrap().0);
            assert!(playlist.contains("#EXT-X-TARGETDURATION:1\n"));
            for extinf in playlist
                .lines()
                .filter_map(|line| line.strip_prefix("#EXTINF:"))
            {
                let seconds: f64 = extinf.trim_end_matches(',').parse().unwrap();
                assert!(seconds.round() <= 1.0, "{extinf}");
            }
        }
    }

    #[test]
    fn a_reused_slot_starts_a_new_playlist() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            ..Options::default()
        });
        cache.add_part_us(1, 2_500, true, None).unwrap();
        cache.add_part_us(1, 2_500, true, None).unwrap();
        let first = cache.resolve_stream(1).unwrap();
        let first_version = cache.version_for_handle(first).unwrap();
        cache.add_part_us(2, 2_500, true, None).unwrap();
        assert!(matches!(cache.last(1), Err(CacheError::StreamNotFound)));
        let playlist = decompress(&cache.last(2).unwrap().unwrap().0);
        assert_eq!(playlist.matches("#EXT-X-PART:").count(), 1);
        assert!(playlist.contains("#EXT-X-PART:DURATION=0.0025,URI=\"p1.mp4\""));
        // Versions go on across the reuse: a new stream never repeats one.
        let second = cache.resolve_stream(2).unwrap();
        assert!(cache.version_for_handle(second).unwrap() > first_version);
    }

    #[test]
    fn delta_update_reads_small_part_durations_exactly() {
        let options = Options {
            target_duration_ms: 1_000,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);
        // 9.5 s of 2.5 ms parts in 1 s segments.
        for _ in 0..3_800 {
            cache.add_part_us(1, 2_500, true, None).unwrap();
        }
        let delta = decompress(&cache.last_delta(1).unwrap().unwrap().0);
        // 9 closed segments and 0.5 s open: segments that end more than 6 s
        // before the end are skipped, s1 to s3.
        assert!(
            delta.contains("#EXT-X-SKIP:SKIPPED-SEGMENTS=3\n"),
            "{delta}"
        );
        assert!(!delta.contains("s3.mp4"));
        assert!(delta.contains("s4.mp4"));
        // The delta is kept for the version it was made from.
        let again = cache.last_delta(1).unwrap().unwrap().0;
        assert_eq!(decompress(&again), delta);
    }
}
