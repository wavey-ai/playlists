use crate::stream_registry::{ResolvedStream, StreamRegistry};
use crate::{CacheError, Options};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::prelude::*;
use std::sync::{Arc, RwLock};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

#[derive(Debug, Clone, Copy)]
struct MediaSegmentBlock {
    start: usize,
    duration_ms: u64,
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

/// One atomically published playlist position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlaylistPosition {
    pub segment_id: usize,
    pub part_idx: usize,
    pub sequence: usize,
}

/// Snapshot of encoded payload memory retained by the playlist cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct M3u8CacheMemoryStats {
    pub occupied_slots: usize,
    pub encoded_playlist_bytes: usize,
    pub initialization_bytes: usize,
    pub maximum_payload_bytes: usize,
}

#[derive(Clone)]
struct CachedPlaylist {
    bytes: Bytes,
    hash: u64,
}

struct PlaylistSnapshot {
    stream_id: u64,
    generation: u64,
    segment_id: usize,
    part_idx: usize,
    sequence: usize,
    full: CachedPlaylist,
    delta: Option<CachedPlaylist>,
}

#[derive(Clone)]
struct StreamInitialization {
    stream_id: u64,
    generation: u64,
    bytes: Bytes,
}

#[derive(Clone, Copy)]
struct SegmentRange {
    segment_id: usize,
    first_sequence: usize,
    end_sequence: usize,
}

struct PlaylistStreamState {
    position: Option<PlaylistPosition>,
    segment_ranges: Vec<Option<SegmentRange>>,
    version: u64,
}

pub struct M3u8Cache {
    buffer: Vec<RwLock<Option<Arc<PlaylistSnapshot>>>>,
    latest: Vec<ArcSwapOption<PlaylistSnapshot>>,
    stream_states: Vec<RwLock<PlaylistStreamState>>,
    inits: Vec<RwLock<Option<StreamInitialization>>>,
    registry: StreamRegistry,
    options: Options,
    max_snapshot_bytes: usize,
    max_init_bytes: usize,
}

impl M3u8Cache {
    pub fn new(options: Options) -> Self {
        Self::try_new(options).expect("valid playlist cache capacity")
    }

    pub fn try_new(options: Options) -> Result<Self, CacheError> {
        let options = options.normalized();
        let stream_capacity = options
            .max_parts_per_segment
            .checked_mul(options.max_segments)
            .ok_or(CacheError::ArithmeticOverflow)?;
        let buffer_size = options
            .num_playlists
            .checked_mul(stream_capacity)
            .ok_or(CacheError::ArithmeticOverflow)?;
        let max_snapshot_bytes = options
            .buffer_size_kb
            .checked_mul(1024)
            .ok_or(CacheError::ArithmeticOverflow)?;
        let max_init_bytes = options
            .init_size_kb
            .checked_mul(1024)
            .ok_or(CacheError::ArithmeticOverflow)?;

        Ok(Self {
            buffer: (0..buffer_size).map(|_| RwLock::new(None)).collect(),
            latest: (0..options.num_playlists)
                .map(|_| ArcSwapOption::empty())
                .collect(),
            stream_states: (0..options.num_playlists)
                .map(|_| {
                    RwLock::new(PlaylistStreamState {
                        position: None,
                        segment_ranges: vec![None; options.max_segments],
                        version: 1,
                    })
                })
                .collect(),
            inits: (0..options.num_playlists)
                .map(|_| RwLock::new(None))
                .collect(),
            registry: StreamRegistry::new(options.num_playlists),
            options,
            max_snapshot_bytes,
            max_init_bytes,
        })
    }

    pub fn resolve_stream(&self, stream_id: u64) -> Option<M3u8StreamHandle> {
        self.registry.resolve(stream_id).map(M3u8StreamHandle)
    }

    pub fn resolve_or_create_stream(&self, stream_id: u64) -> Result<M3u8StreamHandle, CacheError> {
        Ok(M3u8StreamHandle(self.registry.resolve_or_create(
            stream_id,
            |index, generation| {
                self.reset_index_state(index, generation);
            },
        )))
    }

    pub fn last_position(&self, stream_id: u64) -> Option<(usize, usize)> {
        let position = self.position(stream_id)?;
        Some((position.segment_id, position.part_idx))
    }

    pub fn position(&self, stream_id: u64) -> Option<PlaylistPosition> {
        let handle = self.resolve_stream(stream_id)?;
        self.position_for_handle(handle)
    }

    pub fn position_for_handle(&self, handle: M3u8StreamHandle) -> Option<PlaylistPosition> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let snapshot_guard = self.latest.get(handle.index())?.load();
        let snapshot = snapshot_guard.as_ref()?;
        let position = (snapshot.stream_id == handle.stream_id()
            && snapshot.generation == handle.generation())
        .then_some(PlaylistPosition {
            segment_id: snapshot.segment_id,
            part_idx: snapshot.part_idx,
            sequence: snapshot.sequence,
        });
        self.registry
            .is_current_fast(handle.0)
            .then_some(position)
            .flatten()
    }

    pub fn ensure_stream_id(&self, stream_id: u64) -> Result<(), CacheError> {
        self.resolve_or_create_stream(stream_id).map(|_| ())
    }

    pub fn zero_stream_id(&self, stream_id: u64) {
        let _ = self.registry.remove_stream(stream_id, |index, generation| {
            self.reset_index_state(index, generation);
        });
    }

    pub fn set_init(&self, stream_id: u64, data_bytes: Bytes) -> Result<(), CacheError> {
        let handle = self
            .resolve_stream(stream_id)
            .ok_or(CacheError::StreamNotFound)?;
        self.set_init_for_handle(handle, data_bytes)
    }

    pub fn set_init_for_handle(
        &self,
        handle: M3u8StreamHandle,
        data_bytes: Bytes,
    ) -> Result<(), CacheError> {
        if data_bytes.len() > self.max_init_bytes {
            return Err(CacheError::BufferOverflow);
        }
        self.registry
            .with_validated(handle.0, || {
                let mut init = self.inits[handle.index()]
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *init = Some(StreamInitialization {
                    stream_id: handle.stream_id(),
                    generation: handle.generation(),
                    bytes: data_bytes,
                });
                self.bump_version(handle.index());
            })
            .ok_or(CacheError::StreamNotFound)
    }

    pub fn get_init(&self, stream_id: u64) -> Result<Bytes, CacheError> {
        let handle = self
            .resolve_stream(stream_id)
            .ok_or(CacheError::StreamNotFound)?;
        self.get_init_for_handle(handle)
            .ok_or(CacheError::StreamNotFound)
    }

    pub fn get_init_for_handle(&self, handle: M3u8StreamHandle) -> Option<Bytes> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let init_guard = self.inits[handle.index()]
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let init = init_guard.as_ref()?;
        let bytes = (init.stream_id == handle.stream_id()
            && init.generation == handle.generation())
        .then(|| init.bytes.clone());
        drop(init_guard);
        self.registry
            .is_current_fast(handle.0)
            .then_some(bytes)
            .flatten()
    }

    pub fn add(
        &self,
        stream_id: u64,
        segment_id: usize,
        seq: usize,
        idx: usize,
        data: Bytes,
    ) -> Result<u64, CacheError> {
        if idx >= self.options.max_parts_per_segment {
            return Err(CacheError::IndexOutOfBounds);
        }
        let (full, delta) = self.prepare_variants(&data)?;
        let handle = self.resolve_or_create_stream(stream_id)?;
        self.add_prepared_for_handle(handle, segment_id, seq, idx, full, delta)
    }

    pub fn add_for_handle(
        &self,
        handle: M3u8StreamHandle,
        segment_id: usize,
        sequence: usize,
        part_idx: usize,
        data: Bytes,
    ) -> Result<u64, CacheError> {
        if part_idx >= self.options.max_parts_per_segment {
            return Err(CacheError::IndexOutOfBounds);
        }
        if !self.registry.is_current_fast(handle.0) {
            return Err(CacheError::StreamNotFound);
        }
        let (full, delta) = self.prepare_variants(&data)?;

        self.add_prepared_for_handle(handle, segment_id, sequence, part_idx, full, delta)
    }

    fn add_prepared_for_handle(
        &self,
        handle: M3u8StreamHandle,
        segment_id: usize,
        sequence: usize,
        part_idx: usize,
        full: CachedPlaylist,
        delta: Option<CachedPlaylist>,
    ) -> Result<u64, CacheError> {
        let hash = full.hash;
        let slot_idx = self.calculate_index(handle.index(), segment_id, part_idx)?;

        self.registry
            .with_validated(handle.0, || {
                let snapshot = Arc::new(PlaylistSnapshot {
                    stream_id: handle.stream_id(),
                    generation: handle.generation(),
                    segment_id,
                    part_idx,
                    sequence,
                    full,
                    delta,
                });
                if let Some(slot_idx) = slot_idx {
                    let mut slot = self.buffer[slot_idx]
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if slot.as_ref().is_some_and(|stored| {
                        stored.generation == handle.generation()
                            && (stored.sequence, stored.segment_id, stored.part_idx)
                                > (sequence, segment_id, part_idx)
                    }) {
                        return Err(CacheError::Superseded);
                    }
                    *slot = Some(Arc::clone(&snapshot));
                }
                self.publish_position(handle.index(), snapshot);
                Ok(())
            })
            .ok_or(CacheError::StreamNotFound)??;

        Ok(hash)
    }

    pub fn end_segment(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_id: usize,
    ) -> Result<(), CacheError> {
        let handle = self
            .resolve_stream(stream_id)
            .ok_or(CacheError::StreamNotFound)?;
        let previous = segment_id
            .checked_sub(1)
            .ok_or(CacheError::ArithmeticOverflow)?;
        self.registry
            .with_validated(handle.0, || {
                let mut state = self.stream_states[handle.index()]
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let range_idx = previous % self.options.max_segments;
                if let Some(range) = &mut state.segment_ranges[range_idx] {
                    if range.segment_id == previous {
                        range.end_sequence = range.end_sequence.max(part_id);
                    }
                }
            })
            .ok_or(CacheError::StreamNotFound)
    }

    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>, CacheError> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }

    fn prepare_variants(
        &self,
        data: &[u8],
    ) -> Result<(CachedPlaylist, Option<CachedPlaylist>), CacheError> {
        if data.len() > self.max_snapshot_bytes {
            return Err(CacheError::BufferOverflow);
        }
        let full_hash = const_xxh3(data);
        let full_bytes = Bytes::from(self.compress_data(data)?);
        let delta = std::str::from_utf8(data)
            .ok()
            .and_then(playlist_delta_update)
            .map(|delta| {
                let hash = const_xxh3(delta.as_bytes());
                self.compress_data(delta.as_bytes())
                    .map(|bytes| CachedPlaylist {
                        bytes: Bytes::from(bytes),
                        hash,
                    })
            })
            .transpose()?;
        let retained_bytes = full_bytes
            .len()
            .checked_add(delta.as_ref().map_or(0, |delta| delta.bytes.len()))
            .ok_or(CacheError::ArithmeticOverflow)?;
        if retained_bytes > self.max_snapshot_bytes {
            return Err(CacheError::BufferOverflow);
        }
        Ok((
            CachedPlaylist {
                bytes: full_bytes,
                hash: full_hash,
            },
            delta,
        ))
    }

    fn calculate_index(
        &self,
        stream_idx: usize,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<usize>, CacheError> {
        if stream_idx >= self.options.num_playlists {
            return Err(CacheError::IndexOutOfBounds);
        }
        if part_idx >= self.options.max_parts_per_segment {
            return Err(CacheError::IndexOutOfBounds);
        }
        let stream_capacity = self
            .options
            .max_segments
            .checked_mul(self.options.max_parts_per_segment)
            .ok_or(CacheError::ArithmeticOverflow)?;
        // Segment zero shares the first ring position with segment one. It is
        // only used before the first segment closes, and the tagged snapshot
        // prevents either logical segment from reading the other's value.
        let wrapped_segment = segment_id.saturating_sub(1) % self.options.max_segments;
        let stream_slot = wrapped_segment
            .checked_mul(self.options.max_parts_per_segment)
            .and_then(|slot| slot.checked_add(part_idx))
            .ok_or(CacheError::ArithmeticOverflow)?;
        let global_slot = stream_idx
            .checked_mul(stream_capacity)
            .and_then(|slot| slot.checked_add(stream_slot))
            .ok_or(CacheError::ArithmeticOverflow)?;
        (global_slot < self.buffer.len())
            .then_some(Some(global_slot))
            .ok_or(CacheError::IndexOutOfBounds)
    }

    pub fn get_idxs(
        &self,
        stream_id: u64,
        segment_id: usize,
    ) -> Result<Option<(usize, usize)>, CacheError> {
        let Some(handle) = self.resolve_stream(stream_id) else {
            return Ok(None);
        };
        self.registry
            .with_validated(handle.0, || {
                let state = self.stream_states[handle.index()]
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let range = state.segment_ranges[segment_id % self.options.max_segments]?;
                (range.segment_id == segment_id && range.first_sequence < range.end_sequence)
                    .then_some((range.first_sequence, range.end_sequence))
            })
            .ok_or(CacheError::StreamNotFound)
    }

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
        if !self.registry.is_current_fast(handle.0) {
            return Err(CacheError::StreamNotFound);
        }
        let slot_idx = self.calculate_index(handle.index(), segment_id, part_idx)?;
        let position = self.stream_states[handle.index()]
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .position;
        let included = position.is_some_and(|position| {
            segment_id < position.segment_id
                || (segment_id == position.segment_id && part_idx <= position.part_idx)
        });
        let result = if included {
            slot_idx.and_then(|slot_idx| {
                let slot = self.buffer[slot_idx]
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let snapshot = slot.as_ref()?;
                (snapshot.stream_id == handle.stream_id()
                    && snapshot.generation == handle.generation()
                    && snapshot.segment_id == segment_id
                    && snapshot.part_idx == part_idx)
                    .then(|| (snapshot.full.bytes.clone(), snapshot.full.hash))
            })
        } else {
            None
        };
        self.registry
            .is_current_fast(handle.0)
            .then_some(result)
            .ok_or(CacheError::StreamNotFound)
    }

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
        if !self.registry.is_current_fast(handle.0) {
            return Err(CacheError::StreamNotFound);
        }
        let slot_idx = self.calculate_index(handle.index(), segment_id, part_idx)?;
        let position = self.stream_states[handle.index()]
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .position;
        let included = position.is_some_and(|position| {
            segment_id < position.segment_id
                || (segment_id == position.segment_id && part_idx <= position.part_idx)
        });
        let result = if included {
            slot_idx.and_then(|slot_idx| {
                let slot = self.buffer[slot_idx]
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let snapshot = slot.as_ref()?;
                if snapshot.stream_id != handle.stream_id()
                    || snapshot.generation != handle.generation()
                    || snapshot.segment_id != segment_id
                    || snapshot.part_idx != part_idx
                {
                    return None;
                }
                snapshot
                    .delta
                    .as_ref()
                    .map(|delta| (delta.bytes.clone(), delta.hash))
            })
        } else {
            None
        };
        self.registry
            .is_current_fast(handle.0)
            .then_some(result)
            .ok_or(CacheError::StreamNotFound)
    }

    pub fn last(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, CacheError> {
        let Some(handle) = self.resolve_stream(stream_id) else {
            return Ok(None);
        };
        self.last_for_handle(handle)
    }

    pub fn last_for_handle(
        &self,
        handle: M3u8StreamHandle,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        self.last_variant_for_handle(handle, false)
    }

    pub fn last_delta(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, CacheError> {
        let Some(handle) = self.resolve_stream(stream_id) else {
            return Ok(None);
        };
        self.last_delta_for_handle(handle)
    }

    pub fn last_delta_for_handle(
        &self,
        handle: M3u8StreamHandle,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        self.last_variant_for_handle(handle, true)
    }

    fn last_variant_for_handle(
        &self,
        handle: M3u8StreamHandle,
        delta: bool,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        if !self.registry.is_current_fast(handle.0) {
            return Err(CacheError::StreamNotFound);
        }
        let snapshot_guard = self.latest[handle.index()].load();
        let Some(snapshot) = snapshot_guard.as_ref() else {
            return Ok(None);
        };
        let result = if snapshot.stream_id == handle.stream_id()
            && snapshot.generation == handle.generation()
        {
            if delta {
                snapshot
                    .delta
                    .as_ref()
                    .map(|variant| (variant.bytes.clone(), variant.hash))
            } else {
                Some((snapshot.full.bytes.clone(), snapshot.full.hash))
            }
        } else {
            None
        };
        self.registry
            .is_current_fast(handle.0)
            .then_some(result)
            .ok_or(CacheError::StreamNotFound)
    }

    pub fn version_for_handle(&self, handle: M3u8StreamHandle) -> Option<u64> {
        if !self.registry.is_current_fast(handle.0) {
            return None;
        }
        let version = self.stream_states[handle.index()]
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .version;
        self.registry.is_current_fast(handle.0).then_some(version)
    }

    /// Scan fixed slots and report retained encoded payload bytes.
    pub fn memory_stats(&self) -> M3u8CacheMemoryStats {
        let mut occupied_slots = 0_usize;
        let mut encoded_playlist_bytes = 0_usize;
        for slot in &self.buffer {
            let slot = slot
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(snapshot) = slot.as_ref() {
                occupied_slots = occupied_slots.saturating_add(1);
                encoded_playlist_bytes = encoded_playlist_bytes
                    .saturating_add(snapshot.full.bytes.len())
                    .saturating_add(snapshot.delta.as_ref().map_or(0, |delta| delta.bytes.len()));
            }
        }
        let initialization_bytes = self
            .inits
            .iter()
            .filter_map(|init| init.read().ok())
            .filter_map(|init| init.as_ref().map(|init| init.bytes.len()))
            .fold(0_usize, usize::saturating_add);
        M3u8CacheMemoryStats {
            occupied_slots,
            encoded_playlist_bytes,
            initialization_bytes,
            maximum_payload_bytes: self
                .buffer
                .len()
                .saturating_mul(self.max_snapshot_bytes)
                .saturating_add(self.inits.len().saturating_mul(self.max_init_bytes)),
        }
    }

    fn publish_position(&self, stream_idx: usize, snapshot: Arc<PlaylistSnapshot>) {
        let mut state = self.stream_states[stream_idx]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let range_idx = snapshot.segment_id % self.options.max_segments;
        let end_sequence = snapshot.sequence.saturating_add(1);
        match &mut state.segment_ranges[range_idx] {
            Some(range) if range.segment_id == snapshot.segment_id => {
                range.first_sequence = range.first_sequence.min(snapshot.sequence);
                range.end_sequence = range.end_sequence.max(end_sequence);
            }
            Some(range) if range.segment_id > snapshot.segment_id => {}
            range => {
                *range = Some(SegmentRange {
                    segment_id: snapshot.segment_id,
                    first_sequence: snapshot.sequence.saturating_sub(snapshot.part_idx),
                    end_sequence,
                });
            }
        }
        let position = PlaylistPosition {
            segment_id: snapshot.segment_id,
            part_idx: snapshot.part_idx,
            sequence: snapshot.sequence,
        };
        if state.position.is_none_or(|current| {
            (snapshot.sequence, snapshot.segment_id, snapshot.part_idx)
                >= (current.sequence, current.segment_id, current.part_idx)
        }) {
            state.position = Some(position);
            self.latest[stream_idx].store(Some(snapshot));
        }
        state.version = next_version(state.version);
    }

    fn bump_version(&self, stream_idx: usize) {
        let mut state = self.stream_states[stream_idx]
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.version = next_version(state.version);
    }

    fn reset_index_state(&self, stream_idx: usize, _generation: u64) {
        if let Some(init) = self.inits.get(stream_idx) {
            let mut init = init
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *init = None;
        }
        if let Some(state) = self.stream_states.get(stream_idx) {
            let mut state = state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.position = None;
            state.segment_ranges.fill(None);
            state.version = next_version(state.version);
        }
        if let Some(latest) = self.latest.get(stream_idx) {
            latest.store(None);
        }
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

fn playlist_delta_update(playlist: &str) -> Option<String> {
    if playlist.contains("#EXT-X-ENDLIST") || playlist.contains("#EXT-X-SKIP:") {
        return None;
    }

    let skip_boundary_ms = parse_can_skip_until_ms(playlist)?;
    let lines: Vec<&str> = playlist.lines().collect();
    let (blocks, trailing_start, trailing_part_duration_ms) = parse_media_timeline(&lines);
    let insert_at = blocks
        .first()
        .map(|block| block.start)
        .unwrap_or(trailing_start);

    let total_parent_duration_ms = blocks
        .iter()
        .map(|block| block.duration_ms)
        .fold(trailing_part_duration_ms, u64::saturating_add);

    let mut skipped_segments = 0;
    let mut elapsed_ms = 0_u64;
    for block in &blocks {
        elapsed_ms = elapsed_ms.saturating_add(block.duration_ms);
        if total_parent_duration_ms.saturating_sub(elapsed_ms) > skip_boundary_ms {
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

fn parse_can_skip_until_ms(playlist: &str) -> Option<u64> {
    playlist
        .lines()
        .find_map(|line| {
            line.strip_prefix("#EXT-X-SERVER-CONTROL:")
                .and_then(|attributes| parse_attribute_value(attributes, "CAN-SKIP-UNTIL"))
        })
        .and_then(|seconds| seconds.parse::<f64>().ok())
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(|seconds| (seconds * 1000.0).round() as u64)
}

fn parse_media_timeline(lines: &[&str]) -> (Vec<MediaSegmentBlock>, usize, u64) {
    let mut blocks = Vec::new();
    let mut current_start = None;
    let mut current_duration_ms = None;

    for (idx, line) in lines.iter().enumerate() {
        if is_segment_scoped_tag(line) {
            current_start.get_or_insert(idx);
        }

        if let Some(duration_ms) = parse_extinf_duration_ms(line) {
            current_start.get_or_insert(idx);
            current_duration_ms = Some(duration_ms);
        }

        if is_uri_line(line) {
            if let (Some(start), Some(duration_ms)) =
                (current_start.take(), current_duration_ms.take())
            {
                blocks.push(MediaSegmentBlock { start, duration_ms });
            }
        }
    }

    let trailing_start = current_start.unwrap_or(lines.len());
    let trailing_part_duration_ms = lines[trailing_start..]
        .iter()
        .filter_map(|line| parse_part_duration_ms(line))
        .fold(0_u64, u64::saturating_add);

    (blocks, trailing_start, trailing_part_duration_ms)
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

fn parse_extinf_duration_ms(line: &str) -> Option<u64> {
    line.strip_prefix("#EXTINF:")
        .and_then(|value| value.split_once(',').map(|(duration, _)| duration))
        .and_then(parse_duration_ms)
}

fn parse_part_duration_ms(line: &str) -> Option<u64> {
    line.strip_prefix("#EXT-X-PART:")
        .and_then(|attributes| parse_attribute_value(attributes, "DURATION"))
        .and_then(parse_duration_ms)
}

fn parse_duration_ms(value: &str) -> Option<u64> {
    value
        .parse::<f64>()
        .ok()
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(|seconds| (seconds * 1000.0).round() as u64)
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
    use crate::m3u8_manifest::M3u8Manifest;
    use flate2::read::GzDecoder;

    fn decompress(data: &[u8]) -> String {
        let mut decoder = GzDecoder::new(data);
        let mut decoded = String::new();
        decoder.read_to_string(&mut decoded).unwrap();
        decoded
    }

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
    fn tracks_last_playlist_position() {
        let cache = M3u8Cache::new(Options::default());

        assert_eq!(cache.last_position(7), None);

        cache
            .add(7, 12, 0, 0, Bytes::from_static(b"first"))
            .unwrap();
        assert_eq!(cache.last_position(7), Some((12, 0)));

        cache
            .add(7, 12, 1, 1, Bytes::from_static(b"second"))
            .unwrap();
        assert_eq!(cache.last_position(7), Some((12, 1)));
    }

    #[test]
    fn exposes_current_open_segment_part_range() {
        let cache = M3u8Cache::new(Options::default());

        cache
            .add(7, 4, 24, 0, Bytes::from_static(b"closed-4"))
            .unwrap();
        cache
            .add(7, 5, 25, 0, Bytes::from_static(b"open-5-a"))
            .unwrap();
        cache
            .add(7, 5, 26, 1, Bytes::from_static(b"open-5-b"))
            .unwrap();

        assert_eq!(cache.get_idxs(7, 5).unwrap(), Some((25, 27)));
    }

    #[test]
    fn open_segment_range_ignores_stale_ring_boundary() {
        let options = Options {
            max_segments: 4,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);

        cache
            .add(7, 1, 1, 0, Bytes::from_static(b"open-1"))
            .unwrap();
        cache
            .add(7, 2, 3, 0, Bytes::from_static(b"open-2"))
            .unwrap();
        cache
            .add(7, 3, 5, 0, Bytes::from_static(b"open-3"))
            .unwrap();
        cache
            .add(7, 4, 7, 0, Bytes::from_static(b"open-4-a"))
            .unwrap();
        cache
            .add(7, 4, 8, 1, Bytes::from_static(b"open-4-b"))
            .unwrap();

        assert_eq!(cache.last_position(7), Some((4, 1)));
        assert_eq!(cache.get_idxs(7, 4).unwrap(), Some((7, 9)));
    }

    #[test]
    fn delta_update_replaces_segments_older_than_skip_boundary() {
        let options = Options {
            max_segments: 10,
            segment_min_ms: 1000,
            target_duration_ms: 1000,
            part_target_ms: 1000,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);
        let mut manifest = M3u8Manifest::new(options);
        let mut latest = None;

        for _ in 0..12 {
            latest = Some(manifest.add_part(1000, true));
        }

        let (playlist, segment_id, seq, idx, _) = latest.unwrap();
        cache.add(1, segment_id, seq, idx, playlist).unwrap();

        let (delta, _) = cache.last_delta(1).unwrap().unwrap();
        let delta = decompress(&delta);

        assert!(delta.contains("#EXT-X-VERSION:9"));
        assert!(delta.contains("#EXT-X-SERVER-CONTROL:"));
        assert!(delta.contains("CAN-SKIP-UNTIL=6.00000"));
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
        cache
            .add(
                1,
                1,
                1,
                0,
                Bytes::from_static(
                    b"#EXTM3U\n#EXT-X-VERSION:9\n#EXT-X-SERVER-CONTROL:CAN-BLOCK-RELOAD=YES\n",
                ),
            )
            .unwrap();

        assert!(cache.last_delta(1).unwrap().is_none());
    }

    #[test]
    fn snapshot_size_limit_is_enforced_before_compression() {
        let cache = M3u8Cache::new(Options {
            buffer_size_kb: 1,
            ..Options::default()
        });

        assert!(matches!(
            cache.add(1, 1, 1, 0, Bytes::from(vec![b'a'; 1025])),
            Err(CacheError::BufferOverflow)
        ));
    }

    #[test]
    fn encoded_snapshot_must_also_fit_the_budget() {
        let cache = M3u8Cache::new(Options {
            buffer_size_kb: 1,
            ..Options::default()
        });
        let mut state = 0x1234_5678_u32;
        let data = (0..1024)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect::<Vec<_>>();

        assert!(matches!(
            cache.add(1, 1, 1, 0, Bytes::from(data)),
            Err(CacheError::BufferOverflow)
        ));
    }

    #[test]
    fn rejected_mapped_writes_do_not_evict_a_live_stream() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            max_parts_per_segment: 1,
            buffer_size_kb: 1,
            ..Options::default()
        });
        cache.add(1, 1, 1, 0, Bytes::from_static(b"live")).unwrap();
        let live = cache.resolve_stream(1).unwrap();

        assert!(matches!(
            cache.add(2, 1, 1, 1, Bytes::from_static(b"invalid part")),
            Err(CacheError::IndexOutOfBounds)
        ));
        assert!(matches!(
            cache.add(3, 1, 1, 0, Bytes::from(vec![0_u8; 1025])),
            Err(CacheError::BufferOverflow)
        ));

        assert_eq!(cache.resolve_stream(1), Some(live));
        assert_eq!(cache.resolve_stream(2), None);
        assert_eq!(cache.resolve_stream(3), None);
        assert_eq!(
            cache.last_for_handle(live).unwrap().unwrap().1,
            const_xxh3(b"live")
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
    }

    #[test]
    fn encoded_payload_memory_plateaus_after_ring_rotations() {
        let options = Options {
            num_playlists: 1,
            max_segments: 2,
            max_parts_per_segment: 4,
            buffer_size_kb: 4,
            segment_min_ms: 1,
            ..Options::default()
        };
        let cache = M3u8Cache::new(options);
        let mut manifest = M3u8Manifest::new(options);
        for _ in 0..100 {
            let (playlist, segment_id, sequence, part_idx, _) = manifest.add_part(1, true);
            cache
                .add(1, segment_id, sequence, part_idx, playlist)
                .unwrap();
        }

        let stats = cache.memory_stats();
        assert!(stats.occupied_slots <= options.max_segments * options.max_parts_per_segment);
        assert!(stats.encoded_playlist_bytes > 0);
        assert!(
            stats.encoded_playlist_bytes + stats.initialization_bytes
                <= stats.maximum_payload_bytes
        );
    }

    #[test]
    fn initial_segment_snapshot_uses_the_bounded_ring() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 1,
            buffer_size_kb: 1,
            ..Options::default()
        });
        cache
            .add(1, 0, 0, 0, Bytes::from_static(b"initial"))
            .unwrap();

        assert!(cache.get(1, 0, 0).unwrap().is_some());
        assert_eq!(cache.memory_stats().occupied_slots, 1);

        cache.add(1, 1, 1, 0, Bytes::from_static(b"next")).unwrap();
        assert!(cache.get(1, 0, 0).unwrap().is_none());
        assert!(cache.get(1, 1, 0).unwrap().is_some());
        assert_eq!(cache.memory_stats().occupied_slots, 1);
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
            .add_for_handle(old, 1, 1, 0, Bytes::from_static(b"old"))
            .unwrap();
        cache
            .set_init_for_handle(old, Bytes::from_static(b"old-init"))
            .unwrap();
        let (stale_full, stale_delta) = cache
            .prepare_variants(b"stale")
            .expect("prepare delayed old-generation write");

        let current = cache.resolve_or_create_stream(2).unwrap();
        cache
            .add_for_handle(current, 1, 1, 0, Bytes::from_static(b"current"))
            .unwrap();
        cache
            .set_init_for_handle(current, Bytes::from_static(b"current-init"))
            .unwrap();

        assert!(matches!(
            cache.add_prepared_for_handle(old, 1, 1, 0, stale_full, stale_delta),
            Err(CacheError::StreamNotFound)
        ));
        assert!(matches!(
            cache.set_init_for_handle(old, Bytes::from_static(b"stale-init")),
            Err(CacheError::StreamNotFound)
        ));
        let (playlist, _) = cache.last_for_handle(current).unwrap().unwrap();
        assert_eq!(decompress(&playlist), "current");
        assert_eq!(
            cache.get_init_for_handle(current).unwrap(),
            Bytes::from_static(b"current-init")
        );
    }

    #[test]
    fn delayed_older_playlist_cannot_overwrite_newer_ring_slot() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 1,
            ..Options::default()
        });
        let handle = cache.resolve_or_create_stream(8).unwrap();
        cache
            .add_for_handle(handle, 2, 2, 0, Bytes::from_static(b"newer"))
            .unwrap();

        assert!(matches!(
            cache.add_for_handle(handle, 1, 1, 0, Bytes::from_static(b"delayed-older")),
            Err(CacheError::Superseded)
        ));
        let (playlist, _) = cache.last_for_handle(handle).unwrap().unwrap();
        assert_eq!(decompress(&playlist), "newer");
        assert_eq!(
            cache.position_for_handle(handle),
            Some(PlaylistPosition {
                segment_id: 2,
                part_idx: 0,
                sequence: 2,
            })
        );
    }

    #[test]
    fn lower_sequence_cannot_overwrite_the_same_playlist_position() {
        let cache = M3u8Cache::new(Options {
            num_playlists: 1,
            max_segments: 1,
            max_parts_per_segment: 1,
            ..Options::default()
        });
        let handle = cache.resolve_or_create_stream(9).unwrap();
        cache
            .add_for_handle(handle, 2, 3, 0, Bytes::from_static(b"newer-sequence"))
            .unwrap();

        assert!(matches!(
            cache.add_for_handle(handle, 2, 2, 0, Bytes::from_static(b"older-sequence")),
            Err(CacheError::Superseded)
        ));
        let (playlist, _) = cache.get_for_handle(handle, 2, 0).unwrap().unwrap();
        assert_eq!(decompress(&playlist), "newer-sequence");
    }

    #[test]
    fn stream_handle_is_bound_to_its_cache() {
        let first = M3u8Cache::new(Options::default());
        let second = M3u8Cache::new(Options::default());
        let handle = first.resolve_or_create_stream(3).unwrap();
        second.resolve_or_create_stream(3).unwrap();

        assert!(matches!(
            second.add_for_handle(handle, 1, 1, 0, Bytes::from_static(b"wrong-cache")),
            Err(CacheError::StreamNotFound)
        ));
    }
}
