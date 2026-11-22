pub mod chunk_cache;
pub mod m3u8_cache;
pub mod m3u8_manifest;
#[cfg(feature = "mesh")]
pub mod mesh;
#[cfg(feature = "mesh")]
mod mesh_send;
pub mod multivariant;
mod stream_registry;
pub mod tail_bundle;

use access_unit::Fmp4;
use chunk_cache::ChunkCache;
use m3u8_cache::M3u8Cache;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;
use tracing::info;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Index out of bounds")]
    IndexOutOfBounds,
    #[error("Stream not found")]
    StreamNotFound,
    #[error("Buffer overflow")]
    BufferOverflow,
    #[error("Arithmetic overflow")]
    ArithmeticOverflow,
}

/// Sizes and timing of the playlists, the playlist cache and the chunk cache.
///
/// Time is held in microseconds so that the smallest parts are exact: a 2.5 ms
/// Opus part (120 samples at 48 kHz) is 2500 µs, a 48-sample PCM part is
/// 1000 µs, a 12-sample part is 250 µs and a 23.976 fps frame is 41708 µs.
///
/// The counts (`max_segments`, `max_parts_per_segment`) are derived from time
/// when they are zero, which is the default: the window is `window_ms` and a
/// segment holds as many parts as `segment_min_ms` (or the target duration)
/// needs at `part_target_us`, with a margin. A non-zero count is kept as
/// given, for callers that size the rings themselves.
#[derive(Copy, Clone, Debug)]
pub struct Options {
    /// Segments in the ring, the open one included. Zero derives it from
    /// `window_ms`.
    pub max_segments: usize,
    pub num_playlists: usize,
    /// Parts in one segment. Zero derives it from the segment duration and
    /// the part target.
    pub max_parts_per_segment: usize,
    /// At most this many of the newest segments, the open one included,
    /// carry EXT-X-PART lines. Zero sets no count: only the time rule of
    /// `parted_target_durations` applies.
    pub max_parted_segments: usize,
    pub segment_min_ms: u32,
    /// EXT-X-TARGETDURATION, rounded up to whole seconds. It is at least
    /// `segment_min_ms`. A segment closes on the first independent part at
    /// or after `segment_min_ms`, and before its EXTINF would round above
    /// the target duration even when that part is not independent (RFC
    /// 8216bis 4.4.3.1). Video with a GOP longer than about the target
    /// duration therefore gets segments that do not all start with a key
    /// frame: set the target to the GOP to keep them aligned.
    pub target_duration_ms: u32,
    pub buffer_size_kb: usize,
    pub init_size_kb: usize,
    /// PART-TARGET in microseconds. Default 2500 (2.5 ms). Zero uses the
    /// default.
    pub part_target_us: u32,
    /// A floor for PART-HOLD-BACK in milliseconds. PART-HOLD-BACK is three
    /// part targets, or this floor when it is larger. Zero (the default)
    /// keeps three part targets.
    pub part_hold_back_ms: u32,
    /// The window that the rings hold when their counts are derived, in
    /// milliseconds. Default 12000.
    pub window_ms: u32,
    /// Segments carry EXT-X-PART lines only while they start within this many
    /// target durations of the end of the playlist (RFC 8216bis: parts SHOULD
    /// be removed once they are more than three target durations from the
    /// end). Default 3. Zero keeps the parts of every retained segment.
    pub parted_target_durations: u32,
}

/// Default PART-TARGET: one 2.5 ms Opus frame (120 samples at 48 kHz).
pub const DEFAULT_PART_TARGET_US: u32 = 2_500;
/// Default minimum segment duration.
pub const DEFAULT_SEGMENT_MIN_MS: u32 = 1_000;
/// Default window of the rings.
pub const DEFAULT_WINDOW_MS: u32 = 12_000;

impl Default for Options {
    fn default() -> Self {
        Options {
            max_segments: 0,
            num_playlists: 5,
            max_parts_per_segment: 0,
            max_parted_segments: 0,
            segment_min_ms: DEFAULT_SEGMENT_MIN_MS,
            target_duration_ms: 1_000,
            buffer_size_kb: 800,
            init_size_kb: 5,
            part_target_us: DEFAULT_PART_TARGET_US,
            part_hold_back_ms: 0,
            window_ms: DEFAULT_WINDOW_MS,
            parted_target_durations: 3,
        }
    }
}

impl Options {
    /// PART-TARGET in microseconds: `part_target_us`, or the default when it
    /// is zero.
    pub fn part_target_us(&self) -> u32 {
        if self.part_target_us > 0 {
            self.part_target_us
        } else {
            DEFAULT_PART_TARGET_US
        }
    }

    /// EXT-X-TARGETDURATION in whole seconds: the target duration rounded
    /// up, at least one.
    pub fn target_duration_secs(&self) -> u32 {
        u32::try_from(u64::from(self.target_duration_ms).div_ceil(1_000).max(1)).unwrap_or(u32::MAX)
    }

    /// A segment is closed before it reaches this many microseconds, so that
    /// its EXTINF, rounded to the nearest second, is at most
    /// TARGETDURATION.
    pub fn segment_limit_us(&self) -> u64 {
        u64::from(self.target_duration_secs()) * 1_000_000 + 500_000
    }

    /// PART-HOLD-BACK in microseconds: three part targets, or the configured
    /// floor when it is larger.
    pub fn part_hold_back_us(&self) -> u64 {
        (u64::from(self.part_target_us()) * 3).max(u64::from(self.part_hold_back_ms) * 1_000)
    }

    /// The options with every derived value filled in. Idempotent.
    pub fn normalized(mut self) -> Self {
        let part_us = self.part_target_us();
        self.part_target_us = part_us;
        self.num_playlists = self.num_playlists.max(1);
        self.segment_min_ms = self.segment_min_ms.max(1);
        // A segment is at least `segment_min_ms` long, and its EXTINF must
        // not round above TARGETDURATION.
        self.target_duration_ms = self.target_duration_ms.max(self.segment_min_ms).max(1000);
        if self.window_ms == 0 {
            self.window_ms = DEFAULT_WINDOW_MS;
        }
        if self.max_parts_per_segment == 0 {
            // A segment closes on the first independent part at or after
            // `segment_min_ms`, and in any case before it reaches
            // `segment_limit_us`. A quarter more, plus a few parts, covers
            // parts that are shorter than the part target.
            let segment_us = self.segment_limit_us();
            let parts = segment_us.div_ceil(u64::from(part_us));
            let parts = parts + parts.div_ceil(4) + 8;
            self.max_parts_per_segment = usize::try_from(parts).unwrap_or(usize::MAX);
        }
        if self.max_segments == 0 {
            // The closed segments that cover the window, and the open one.
            let segments = self.window_ms.div_ceil(self.segment_min_ms) as usize + 1;
            self.max_segments = segments.max(2);
        }
        self.max_parts_per_segment = self.max_parts_per_segment.max(1);
        self.max_segments = self.max_segments.max(1);
        self
    }
}

pub struct Playlists {
    pub chunk_cache: Arc<ChunkCache>,
    m3u8_cache: Arc<M3u8Cache>,
    playlists: Mutex<BTreeMap<u64, Arc<PlaylistEntry>>>,
    active: AtomicUsize,
    options: Options,
}

/// One live stream of [`Playlists`]. Its manifest lives in the playlist
/// cache; the entry only orders the writes of the stream against its `fin`.
struct PlaylistEntry {
    /// Set by `fin` before it waits for `commit`. An add that sees it adds
    /// nothing.
    closing: AtomicBool,
    /// Held by an add across its cache writes, and by `fin` across the
    /// teardown of the caches, so that no add of a closed entry can create
    /// the stream again after `fin` removed it.
    commit: Mutex<()>,
}

pub type PlaylistCacheBundle = (Arc<Playlists>, Arc<ChunkCache>, Arc<M3u8Cache>);

impl PlaylistEntry {
    fn new() -> Self {
        Self {
            closing: AtomicBool::new(false),
            commit: Mutex::new(()),
        }
    }

    fn commit(&self) -> MutexGuard<'_, ()> {
        self.commit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Playlists {
    pub fn new(options: Options) -> PlaylistCacheBundle {
        Self::try_new(options).expect("valid playlist capacities")
    }

    pub fn try_new(options: Options) -> Result<PlaylistCacheBundle, CacheError> {
        Self::try_new_with_latest_read_replicas(options, 1)
    }

    /// Build playlist caches whose playlist readers share `replicas` copies
    /// of each compressed playlist (see
    /// [`M3u8Cache::new_with_latest_read_replicas`]).
    ///
    /// Use more than one replica only for read-heavy streams with measured
    /// payload reference-count contention.
    pub fn new_with_latest_read_replicas(options: Options, replicas: usize) -> PlaylistCacheBundle {
        Self::try_new_with_latest_read_replicas(options, replicas)
            .expect("valid playlist capacities")
    }

    pub fn try_new_with_latest_read_replicas(
        options: Options,
        replicas: usize,
    ) -> Result<PlaylistCacheBundle, CacheError> {
        let options = options.normalized();
        let chunk_cache = Arc::new(ChunkCache::try_new(options)?);
        let m3u8_cache = Arc::new(M3u8Cache::try_new_with_latest_read_replicas(
            options, replicas,
        )?);

        Ok((
            Arc::new(Self {
                chunk_cache: Arc::clone(&chunk_cache),
                m3u8_cache: Arc::clone(&m3u8_cache),
                playlists: Mutex::new(BTreeMap::new()),
                active: AtomicUsize::new(0),
                options,
            }),
            Arc::clone(&chunk_cache),
            Arc::clone(&m3u8_cache),
        ))
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    fn entries(&self) -> MutexGuard<'_, BTreeMap<u64, Arc<PlaylistEntry>>> {
        self.playlists
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// End the stream: remove it and its parts from both caches. An add that
    /// races with `fin` either commits before the caches are cleared or adds
    /// nothing.
    pub fn fin(&self, id: u64) {
        let entry = {
            let playlists = self.entries();
            let Some(entry) = playlists.get(&id) else {
                self.m3u8_cache.zero_stream_id(id);
                self.chunk_cache.zero_stream_id(id);
                return;
            };
            entry.closing.store(true, Ordering::Release);
            Arc::clone(entry)
        };

        // An add holds this lock through its cache commit. Once acquired, no
        // add of the old entry can write after the caches are cleared.
        let _commit = entry.commit();
        self.m3u8_cache.zero_stream_id(id);
        self.chunk_cache.zero_stream_id(id);

        let removed = {
            let mut playlists = self.entries();
            if playlists
                .get(&id)
                .is_some_and(|current| Arc::ptr_eq(current, &entry))
            {
                playlists.remove(&id);
                true
            } else {
                false
            }
        };
        if removed {
            self.active.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Add a part whose duration is `fmp4.duration` milliseconds.
    pub fn add(&self, stream_id: u64, fmp4: Fmp4) -> bool {
        let duration_us = u64::from(fmp4.duration) * 1_000;
        self.add_us(stream_id, fmp4, duration_us)
    }

    /// Add a part of `duration_us` microseconds (`fmp4.duration` is not
    /// read). The playlist is not rendered here: a reader of the playlist
    /// cache renders it.
    ///
    /// Returns false, and adds nothing, when the stream is closing, when
    /// `num_playlists` streams are already live, when the initialization is
    /// larger than `init_size_kb`, or when the playlist cache refuses the
    /// part.
    pub fn add_us(&self, stream_id: u64, fmp4: Fmp4, duration_us: u64) -> bool {
        let max_init_bytes = self.options.init_size_kb.saturating_mul(1024);
        if fmp4
            .init
            .as_ref()
            .is_some_and(|init| init.len() > max_init_bytes)
        {
            return false;
        }
        let entry = {
            let mut playlists = self.entries();
            if let Some(entry) = playlists.get(&stream_id) {
                if entry.closing.load(Ordering::Acquire) {
                    return false;
                }
                Arc::clone(entry)
            } else {
                if playlists.len() >= self.options.num_playlists {
                    return false;
                }
                let entry = Arc::new(PlaylistEntry::new());
                playlists.insert(stream_id, Arc::clone(&entry));
                let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
                info!("PLAY:NEW active={active}");
                entry
            }
        };

        let _commit = entry.commit();
        if entry.closing.load(Ordering::Acquire) {
            return false;
        }
        let handle = match self.m3u8_cache.resolve_stream(stream_id) {
            Some(handle) => handle,
            None => match self.m3u8_cache.resolve_or_create_stream(stream_id) {
                Ok(handle) => handle,
                Err(_) => return false,
            },
        };
        if let Some(init) = fmp4.init {
            if self.m3u8_cache.set_init_for_handle(handle, init).is_err() {
                return false;
            }
        }
        match self.m3u8_cache.add_part_us_for_handle(
            handle,
            duration_us,
            fmp4.key,
            Some(fmp4.data.len()),
        ) {
            Ok(position) => {
                if position.new_segment {
                    info!("PLAY:UP active={}", self.active());
                }
                true
            }
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::thread;

    fn part(duration: u32) -> Fmp4 {
        Fmp4 {
            init: None,
            key: true,
            data: Bytes::from_static(b"part"),
            duration,
        }
    }

    #[tokio::test]
    async fn test_fin_clears_chunk_cache_entry() {
        let options = Options::default();
        let (playlists, chunk_cache, _m3u8_cache) = Playlists::new(options);
        let stream_id = 101;

        assert!(playlists.add(stream_id, part(500)));
        assert_eq!(playlists.active(), 1);

        let handle = chunk_cache.resolve_or_create_stream(stream_id).await;
        assert_eq!(chunk_cache.resolve_stream(stream_id), Some(handle));

        playlists.fin(stream_id);
        assert_eq!(playlists.active(), 0);
        assert_eq!(chunk_cache.resolve_stream(stream_id), None);
    }

    #[test]
    fn parts_are_added_in_microseconds() {
        let (playlists, _chunk_cache, m3u8_cache) = Playlists::new(Options::default());
        for _ in 0..3 {
            let fmp4 = Fmp4 {
                init: None,
                key: true,
                data: Bytes::from_static(b"opus"),
                duration: 3,
            };
            assert!(playlists.add_us(5, fmp4, 2_500));
        }
        let playlist = m3u8_cache.last_text(5).unwrap().unwrap();
        let playlist = std::str::from_utf8(&playlist).unwrap();
        assert_eq!(playlist.matches("#EXT-X-PART:DURATION=0.0025,").count(), 3);
        assert!(playlist.contains("BYTERANGE=\"4@8\""));
    }

    #[test]
    fn an_initialization_is_stored_with_its_part() {
        let (playlists, _chunk_cache, m3u8_cache) = Playlists::new(Options::default());
        let init = Bytes::from_static(b"init");
        assert!(playlists.add_us(
            3,
            Fmp4 {
                init: Some(init.clone()),
                ..part(0)
            },
            2_500,
        ));
        assert_eq!(m3u8_cache.get_init(3).unwrap(), init);
        assert_eq!(m3u8_cache.last_position(3), Some((1, 0)));
    }

    #[test]
    fn closing_entry_rejects_add_and_cannot_resurrect_cache_state() {
        let options = Options {
            num_playlists: 1,
            ..Options::default()
        };
        let (playlists, _chunk_cache, m3u8_cache) = Playlists::new(options);
        assert!(playlists.add(7, part(500)));

        let entry = Arc::clone(playlists.entries().get(&7).expect("live entry"));
        let commit_guard = entry.commit();
        let finisher = {
            let playlists = Arc::clone(&playlists);
            thread::spawn(move || playlists.fin(7))
        };
        while !entry.closing.load(Ordering::Acquire) {
            thread::yield_now();
        }

        assert!(!playlists.add(7, part(500)));
        drop(commit_guard);
        finisher.join().unwrap();

        assert_eq!(playlists.active(), 0);
        assert_eq!(m3u8_cache.last_position(7), None);
        assert!(m3u8_cache.resolve_stream(7).is_none());
    }

    #[test]
    fn playlists_fin_racing_add_us_leaves_no_ghost_slot() {
        let options = Options {
            num_playlists: 2,
            ..Options::default()
        };
        let (playlists, chunk_cache, m3u8_cache) = Playlists::new(options);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for round in 0..2_000 {
            // A live stream, with a chunk stream of the same id that `fin`
            // must clear too.
            assert!(playlists.add_us(7, part(0), 2_500), "round {round}");
            runtime.block_on(chunk_cache.resolve_or_create_stream(7));

            // One add against one `fin`: a later add would make a new entry
            // and hide a ghost.
            let start = Arc::new(std::sync::Barrier::new(2));
            let adder = {
                let playlists = Arc::clone(&playlists);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    playlists.add_us(7, part(0), 2_500)
                })
            };
            start.wait();
            playlists.fin(7);
            adder.join().unwrap();

            // The add either committed before `fin` cleared the caches, was
            // refused by the closing entry, or came after `fin` and made a
            // new entry. A stream in the playlist cache without an entry is
            // a ghost that no `fin` of an entry would remove.
            let live = playlists.entries().contains_key(&7);
            assert_eq!(
                m3u8_cache.resolve_stream(7).is_some(),
                live,
                "round {round}: playlist stream without its entry"
            );
            assert_eq!(playlists.active(), usize::from(live), "round {round}");

            playlists.fin(7);
            assert_eq!(playlists.active(), 0, "round {round}");
            assert!(playlists.entries().is_empty(), "round {round}");
            assert_eq!(m3u8_cache.position(7), None, "round {round}");
            assert_eq!(m3u8_cache.memory_stats().occupied_slots, 0, "round {round}");
            assert!(chunk_cache.resolve_stream(7).is_none(), "round {round}");
        }
    }

    #[test]
    fn playlist_add_reports_an_initialization_budget_failure() {
        let options = Options {
            init_size_kb: 1,
            ..Options::default()
        };
        let (playlists, _chunk_cache, m3u8_cache) = Playlists::new(options);
        let added = playlists.add(
            8,
            Fmp4 {
                init: Some(Bytes::from(vec![0_u8; 1025])),
                key: true,
                data: Bytes::from_static(b"part"),
                duration: 500,
            },
        );

        assert!(!added);
        assert!(matches!(
            m3u8_cache.get_init(8),
            Err(CacheError::StreamNotFound)
        ));
        assert_eq!(m3u8_cache.last_position(8), None);
        assert_eq!(playlists.active(), 0);
    }

    #[test]
    fn playlists_constructor_rejects_capacity_arithmetic_overflow() {
        assert!(matches!(
            Playlists::try_new(Options {
                num_playlists: usize::MAX,
                max_segments: 2,
                ..Options::default()
            }),
            Err(CacheError::ArithmeticOverflow)
        ));
    }

    #[test]
    fn options_normalize_the_part_target_in_microseconds() {
        let options = Options {
            part_target_us: 0,
            ..Options::default()
        }
        .normalized();
        assert_eq!(options.part_target_us, DEFAULT_PART_TARGET_US);
        let options = Options {
            part_target_us: 250,
            ..Options::default()
        }
        .normalized();
        assert_eq!(options.part_target_us, 250);
        assert_eq!(options.normalized().part_target_us, 250);
        assert_eq!(options.part_hold_back_us(), 750);
    }
}
