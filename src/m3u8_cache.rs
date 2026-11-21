use crate::m3u8_manifest::{parse_seconds_us, M3u8Manifest, PartPosition};
use crate::Options;

use crate::CacheError;
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::BTreeMap;
use std::io::prelude::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex, MutexGuard, RwLock,
};
use xxhash_rust::const_xxh3::xxh3_64 as const_xxh3;

/// Playlists are compressed when a reader asks for them, once for each new
/// part: the fastest level keeps that cheap.
const PLAYLIST_GZIP_LEVEL: Compression = Compression::fast();

#[derive(Debug, Clone, Copy)]
struct MediaSegmentBlock {
    start: usize,
    duration_us: u64,
}

/// One rendering of a playlist.
#[derive(Clone)]
struct Rendered {
    version: u64,
    text: Bytes,
    gzip: Bytes,
    hash: u64,
}

/// The playlist of one stream slot.
#[derive(Default)]
struct PlaylistSlot {
    /// Generation of the stream slot that this state belongs to.
    generation: usize,
    /// Advances with every part, and with every stored playlist.
    version: u64,
    /// The live playlist, when parts are added with
    /// [`M3u8Cache::add_part_us`]. A playlist is rendered from it only when it
    /// is read.
    manifest: Option<M3u8Manifest>,
    /// The newest rendering, or the playlist stored with [`M3u8Cache::add`].
    rendered: Option<Rendered>,
    /// The delta update of `rendered`.
    delta: Option<(u64, Option<(Bytes, u64)>)>,
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
pub struct M3u8Cache {
    slots: Vec<Mutex<PlaylistSlot>>,
    /// Readers that render take this, so that one renders and the others
    /// find the rendering. Writers never take it.
    render_locks: Vec<Mutex<()>>,
    seg_parts: Vec<AtomicUsize>,
    last_seg: Vec<AtomicUsize>,
    last_part: Vec<AtomicUsize>,
    last_seq: Vec<AtomicUsize>,
    generations: Vec<AtomicUsize>,
    inits: Vec<RwLock<Bytes>>,
    offsets: RwLock<BTreeMap<u64, usize>>,
    offset: AtomicUsize,
    options: Options,
    stream_id_mutex: Mutex<()>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl M3u8Cache {
    pub fn new(mut options: Options) -> Self {
        options = options.normalized();
        let init_size_bytes = options.init_size_kb * 1024;
        let init_repeat_value = Bytes::from(vec![0u8; init_size_bytes]);

        let num_playlists = options.num_playlists;
        let slots = (0..num_playlists)
            .map(|_| {
                Mutex::new(PlaylistSlot {
                    generation: 1,
                    ..PlaylistSlot::default()
                })
            })
            .collect();
        let render_locks = (0..num_playlists).map(|_| Mutex::new(())).collect();

        let seg_parts_size = options.max_segments * num_playlists;
        let seg_parts = (0..seg_parts_size).map(|_| AtomicUsize::new(0)).collect();

        let last_seg = (0..num_playlists).map(|_| AtomicUsize::new(0)).collect();
        let last_part = (0..num_playlists).map(|_| AtomicUsize::new(0)).collect();
        let last_seq = (0..num_playlists).map(|_| AtomicUsize::new(0)).collect();
        let generations = (0..num_playlists).map(|_| AtomicUsize::new(1)).collect();
        let inits = (0..num_playlists)
            .map(|_| RwLock::new(init_repeat_value.clone()))
            .collect();

        Self {
            slots,
            render_locks,
            seg_parts,
            last_seg,
            last_part,
            last_seq,
            generations,
            inits,
            offsets: RwLock::new(BTreeMap::new()),
            offset: AtomicUsize::new(0),
            options,
            stream_id_mutex: Mutex::new(()),
        }
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    fn offset(&self, stream_id: u64) -> Option<usize> {
        let lock = self.offsets.read().unwrap();
        lock.get(&stream_id).copied()
    }

    fn offset_and_generation(&self, stream_id: u64) -> Option<(usize, usize)> {
        let n = self.offset(stream_id)?;
        let generation = self.generations[n].load(Ordering::Acquire);
        Some((n, generation))
    }

    fn last_seg(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_seg[n].load(Ordering::Acquire))
    }

    fn last_part(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_part[n].load(Ordering::Acquire))
    }

    fn last_seq(&self, stream_id: u64) -> Option<usize> {
        self.offset(stream_id)
            .map(|n| self.last_seq[n].load(Ordering::Acquire))
    }

    pub fn last_position(&self, stream_id: u64) -> Option<(usize, usize)> {
        Some((self.last_seg(stream_id)?, self.last_part(stream_id)?))
    }

    fn add_stream_id(&self, stream_id: u64) -> Result<(), CacheError> {
        let _lock = self.stream_id_mutex.lock().unwrap();
        if self.offset(stream_id).is_some() {
            return Ok(());
        }

        let idx = self.offset.load(Ordering::Acquire);
        let next_offset = idx
            .checked_add(1)
            .map(|n| n % self.options.num_playlists)
            .ok_or(CacheError::ArithmeticOverflow)?;

        {
            let mut lock = self.offsets.write().unwrap();
            if let Some(previous_stream_id) = lock
                .iter()
                .find_map(|(candidate, mapped_idx)| (*mapped_idx == idx).then_some(*candidate))
            {
                lock.remove(&previous_stream_id);
            }
            self.reset_stream_idx(idx)?;
            lock.insert(stream_id, idx);
        }

        self.set_last_seg(stream_id, 0)?;
        self.set_last_part(stream_id, 0)?;
        self.set_last_seq(stream_id, 0)?;

        self.offset.store(next_offset, Ordering::Release);
        Ok(())
    }

    pub fn ensure_stream_id(&self, stream_id: u64) -> Result<(), CacheError> {
        self.add_stream_id(stream_id)
    }

    pub fn zero_stream_id(&self, stream_id: u64) {
        let mut lock = self.offsets.write().unwrap();
        if let Some(idx) = lock.remove(&stream_id) {
            let _ = self.reset_stream_idx(idx);
        }
    }

    fn reset_stream_idx(&self, idx: usize) -> Result<(), CacheError> {
        self.last_seg[idx].store(0, Ordering::Release);
        self.last_part[idx].store(0, Ordering::Release);
        self.last_seq[idx].store(0, Ordering::Release);
        let mut next = self.generations[idx]
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        if next == 0 {
            next = 1;
            self.generations[idx].store(1, Ordering::Release);
        }
        *lock(&self.slots[idx]) = PlaylistSlot {
            generation: next,
            ..PlaylistSlot::default()
        };

        let seg_idx = self
            .options
            .max_segments
            .checked_mul(idx)
            .ok_or(CacheError::ArithmeticOverflow)?;
        for n in seg_idx..(seg_idx + self.options.max_segments) {
            self.seg_parts[n].store(0, Ordering::Release);
        }
        Ok(())
    }

    fn set_last_seg(&self, stream_id: u64, id: usize) -> Result<(), CacheError> {
        if let Some(n) = self.offset(stream_id) {
            self.last_seg[n].store(id, Ordering::Release);
            Ok(())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    fn set_last_part(&self, stream_id: u64, id: usize) -> Result<(), CacheError> {
        if let Some(n) = self.offset(stream_id) {
            self.last_part[n].store(id, Ordering::Release);
            Ok(())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    fn set_last_seq(&self, stream_id: u64, id: usize) -> Result<(), CacheError> {
        if let Some(n) = self.offset(stream_id) {
            self.last_seq[n].store(id, Ordering::Release);
            Ok(())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    pub fn set_init(&self, stream_id: u64, data_bytes: Bytes) -> Result<(), CacheError> {
        if let Some(n) = self.offset(stream_id) {
            let mut inits_lock = self.inits[n].write().unwrap();
            *inits_lock = data_bytes;
            Ok(())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    pub fn get_init(&self, stream_id: u64) -> Result<Bytes, CacheError> {
        if let Some(n) = self.offset(stream_id) {
            let lock = &self.inits[n];
            let data = lock.read().unwrap();
            Ok(data.clone())
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    /// The stream's slot and generation, creating the stream when it is new.
    fn stream_slot(&self, stream_id: u64) -> Result<(usize, usize), CacheError> {
        if self.offset(stream_id).is_none() {
            self.add_stream_id(stream_id)?;
        }
        self.offset_and_generation(stream_id)
            .ok_or(CacheError::StreamNotFound)
    }

    /// Record that part `seq` at `idx` of segment `segment_id` is the newest.
    fn advance_position(
        &self,
        n: usize,
        generation: usize,
        segment_id: usize,
        seq: usize,
        idx: usize,
    ) -> Result<(), CacheError> {
        if idx == 0 {
            // The first part of a segment closes the one before it.
            if let Some(previous) = segment_id.checked_sub(1) {
                let wrapped = previous % self.options.max_segments;
                if let Some(slot) = self.seg_parts.get(n * self.options.max_segments + wrapped) {
                    slot.store(seq, Ordering::Release);
                }
            }
        }
        if self.generations[n].load(Ordering::Acquire) != generation {
            return Err(CacheError::StreamNotFound);
        }
        self.last_seg[n].store(segment_id, Ordering::Release);
        self.last_part[n].store(idx, Ordering::Release);
        self.last_seq[n].store(seq, Ordering::Release);
        Ok(())
    }

    /// Add a part of `duration_us` microseconds to the stream's live playlist,
    /// creating the stream when it is new. No playlist is rendered unless the
    /// options ask for eager rendering: a reader renders it.
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
        let (n, generation) = self.stream_slot(stream_id)?;
        let position = {
            let mut slot = lock(&self.slots[n]);
            if slot.generation != generation {
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
            slot.version += 1;
            position
        };
        self.advance_position(
            n,
            generation,
            position.segment_id,
            position.sequence,
            position.index,
        )?;
        if self.options.eager_render {
            let _ = self.render(n, generation);
        }
        Ok(position)
    }

    /// Store a rendered playlist as the stream's newest, for part `seq` at
    /// `idx` of segment `segment_id`. Returns the hash of the playlist.
    pub fn add(
        &self,
        stream_id: u64,
        segment_id: usize,
        seq: usize,
        idx: usize,
        data: Bytes,
    ) -> Result<u64, CacheError> {
        let (n, generation) = self.stream_slot(stream_id)?;
        if segment_id > u32::MAX as usize {
            return Err(CacheError::BufferOverflow);
        }
        let hash = const_xxh3(&data);
        let gzip = Bytes::from(compress_data(&data)?);
        {
            let mut slot = lock(&self.slots[n]);
            if slot.generation != generation {
                return Err(CacheError::StreamNotFound);
            }
            slot.version += 1;
            slot.manifest = None;
            slot.rendered = Some(Rendered {
                version: slot.version,
                text: data,
                gzip,
                hash,
            });
        }
        self.advance_position(n, generation, segment_id, seq, idx)?;
        Ok(hash)
    }

    pub fn end_segment(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_id: usize,
    ) -> Result<(), CacheError> {
        if let Ok(idx) = self.calculate_seg_index(
            stream_id,
            segment_id
                .checked_sub(1)
                .ok_or(CacheError::ArithmeticOverflow)?,
        ) {
            self.seg_parts[idx].store(part_id, Ordering::Release);
        }
        Ok(())
    }

    /// The newest playlist of slot `n`, rendered now if no reader has
    /// rendered it since the last part.
    fn render(&self, n: usize, generation: usize) -> Option<Rendered> {
        let fresh = |slot: &PlaylistSlot| {
            slot.rendered
                .as_ref()
                .filter(|rendered| {
                    slot.generation == generation && rendered.version == slot.version
                })
                .cloned()
        };
        if let Some(rendered) = fresh(&lock(&self.slots[n])) {
            return Some(rendered);
        }
        let _render = lock(&self.render_locks[n]);
        let (version, pieces) = {
            let slot = lock(&self.slots[n]);
            if slot.generation != generation {
                return None;
            }
            if let Some(rendered) = fresh(&slot) {
                return Some(rendered);
            }
            match &slot.manifest {
                Some(manifest) => (slot.version, manifest.pieces()),
                // A stored playlist that a newer one has not replaced.
                None => return slot.rendered.clone(),
            }
        };
        let text = pieces.into_bytes();
        let hash = const_xxh3(&text);
        let gzip = Bytes::from(compress_data(&text).ok()?);
        let rendered = Rendered {
            version,
            text,
            gzip,
            hash,
        };
        let mut slot = lock(&self.slots[n]);
        if slot.generation == generation
            && slot
                .rendered
                .as_ref()
                .is_none_or(|current| current.version < version)
        {
            slot.rendered = Some(rendered.clone());
        }
        Some(rendered)
    }

    /// The delta update of the newest playlist of slot `n`.
    fn render_delta(
        &self,
        n: usize,
        generation: usize,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        let Some(rendered) = self.render(n, generation) else {
            return Ok(None);
        };
        {
            let slot = lock(&self.slots[n]);
            if let Some((version, delta)) = &slot.delta {
                if slot.generation == generation && *version == rendered.version {
                    return Ok(delta.clone());
                }
            }
        }
        let text = std::str::from_utf8(&rendered.text).map_err(|_| {
            CacheError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "playlist is not UTF-8",
            ))
        })?;
        let delta = match playlist_delta_update(text) {
            Some(delta) => {
                let hash = const_xxh3(delta.as_bytes());
                Some((Bytes::from(compress_data(delta.as_bytes())?), hash))
            }
            None => None,
        };
        let mut slot = lock(&self.slots[n]);
        if slot.generation == generation {
            slot.delta = Some((rendered.version, delta.clone()));
        }
        Ok(delta)
    }

    fn is_included(&self, stream_id: u64, segment_id: usize, part_idx: usize) -> bool {
        if let (Some(last_seg), Some(last_part)) =
            (self.last_seg(stream_id), self.last_part(stream_id))
        {
            if last_seg == 0 {
                return false;
            }
            if segment_id < last_seg || (segment_id == last_seg && part_idx <= last_part) {
                return true;
            }
        }

        false
    }

    fn calculate_seg_index(&self, stream_id: u64, segment_id: usize) -> Result<usize, CacheError> {
        if let Some(offset) = self.offset(stream_id) {
            let segments_per_stream = self.options.max_segments;
            let wrapped_segment_idx = segment_id % segments_per_stream;
            offset
                .checked_mul(segments_per_stream)
                .and_then(|result| result.checked_add(wrapped_segment_idx))
                .ok_or(CacheError::ArithmeticOverflow)
        } else {
            Err(CacheError::StreamNotFound)
        }
    }

    pub fn get_idxs(
        &self,
        stream_id: u64,
        segment_id: usize,
    ) -> Result<Option<(usize, usize)>, CacheError> {
        if let Ok(idx) = self.calculate_seg_index(stream_id, segment_id) {
            let mut b = self.seg_parts[idx].load(Ordering::Acquire);
            if self.last_seg(stream_id) == Some(segment_id) {
                b = self
                    .last_seq(stream_id)
                    .and_then(|last_seq| last_seq.checked_add(1))
                    .ok_or(CacheError::ArithmeticOverflow)?;
            }
            if let Ok(prev_idx) = self.calculate_seg_index(
                stream_id,
                segment_id
                    .checked_sub(1)
                    .ok_or(CacheError::ArithmeticOverflow)?,
            ) {
                let a = self.seg_parts[prev_idx].load(Ordering::Acquire);
                if a < b {
                    return Ok(Some((a, b)));
                }
            }
        }

        Ok(None)
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
        if !self.is_included(stream_id, segment_id, part_idx) {
            return Ok(None);
        }
        self.last(stream_id)
    }

    pub fn get_delta(
        &self,
        stream_id: u64,
        segment_id: usize,
        part_idx: usize,
    ) -> Result<Option<(Bytes, u64)>, CacheError> {
        if !self.is_included(stream_id, segment_id, part_idx) {
            return Ok(None);
        }
        self.last_delta(stream_id)
    }

    /// The newest compressed playlist and the hash of its text.
    pub fn last(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, CacheError> {
        let (n, generation) = self
            .offset_and_generation(stream_id)
            .ok_or(CacheError::StreamNotFound)?;
        Ok(self
            .render(n, generation)
            .map(|rendered| (rendered.gzip, rendered.hash)))
    }

    /// The newest playlist, uncompressed.
    pub fn last_text(&self, stream_id: u64) -> Result<Option<Bytes>, CacheError> {
        let (n, generation) = self
            .offset_and_generation(stream_id)
            .ok_or(CacheError::StreamNotFound)?;
        Ok(self.render(n, generation).map(|rendered| rendered.text))
    }

    pub fn last_delta(&self, stream_id: u64) -> Result<Option<(Bytes, u64)>, CacheError> {
        let (n, generation) = self
            .offset_and_generation(stream_id)
            .ok_or(CacheError::StreamNotFound)?;
        self.render_delta(n, generation)
    }
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
    use crate::m3u8_manifest::M3u8Manifest;

    fn decompress(data: &[u8]) -> String {
        decompress_data(data).unwrap()
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
    fn a_part_is_added_without_rendering_and_read_renders_once() {
        let cache = M3u8Cache::new(Options::default());
        for _ in 0..800 {
            cache.add_part_us(9, 2_500, true, Some(40)).unwrap();
        }
        let n = cache.offset(9).unwrap();
        assert!(lock(&cache.slots[n]).rendered.is_none(), "rendered on add");
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
        assert_eq!(const_xxh3(playlist.as_bytes()), hash);

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
    fn eager_render_renders_each_part_as_it_is_added() {
        let cache = M3u8Cache::new(Options {
            eager_render: true,
            ..Options::default()
        });
        cache.add_part_us(9, 2_500, true, None).unwrap();
        let n = cache.offset(9).unwrap();
        let rendered = lock(&cache.slots[n]).rendered.clone().unwrap();
        assert_eq!(rendered.version, 1);
        assert_eq!(
            cache.last(9).unwrap().unwrap().0.as_ptr(),
            rendered.gzip.as_ptr()
        );
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
    fn default_options_take_every_frame_of_a_long_gop() {
        // 60 fps with a key frame every 2 s, and 23.976 fps with one every 48
        // frames: every frame is taken, for ten seconds.
        for (fps_num, fps_den, gop, part_target_us) in
            [(60_u64, 1_u64, 120_u64, 16_667_u32), (24_000, 1_001, 48, 41_709)]
        {
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
            for extinf in playlist.lines().filter_map(|line| line.strip_prefix("#EXTINF:")) {
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
        cache.add_part_us(2, 2_500, true, None).unwrap();
        assert!(matches!(cache.last(1), Err(CacheError::StreamNotFound)));
        let playlist = decompress(&cache.last(2).unwrap().unwrap().0);
        assert_eq!(playlist.matches("#EXT-X-PART:").count(), 1);
        assert!(playlist.contains("#EXT-X-PART:DURATION=0.0025,URI=\"p1.mp4\""));
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
