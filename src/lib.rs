pub mod chunk_cache;
pub mod m3u8_cache;
pub mod m3u8_manifest;
#[cfg(feature = "mesh")]
pub mod mesh;
#[cfg(feature = "mesh")]
mod mesh_send;
pub mod multivariant;
pub mod tail_bundle;

use access_unit::Fmp4;
use chunk_cache::ChunkCache;
use m3u8_cache::M3u8Cache;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
/// given, so callers that size the rings themselves are unchanged.
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
    pub target_duration_ms: u32,
    /// PART-TARGET in whole milliseconds. Kept for compatibility: a non-zero
    /// value wins over `part_target_us`. Zero (the default) uses
    /// `part_target_us`.
    pub part_target_ms: u32,
    pub buffer_size_kb: usize,
    pub init_size_kb: usize,
    /// PART-TARGET in microseconds. Default 2500 (2.5 ms).
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
    /// Render and compress a playlist for every part as it is added (true),
    /// or only when a reader asks for it, once for each new part (false, the
    /// default).
    pub eager_render: bool,
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
            part_target_ms: 0,
            buffer_size_kb: 800,
            init_size_kb: 5,
            part_target_us: DEFAULT_PART_TARGET_US,
            part_hold_back_ms: 0,
            window_ms: DEFAULT_WINDOW_MS,
            parted_target_durations: 3,
            eager_render: false,
        }
    }
}

impl Options {
    /// PART-TARGET in microseconds: `part_target_ms` when it is set, else
    /// `part_target_us`, at least 1 µs.
    pub fn part_target_us(&self) -> u32 {
        if self.part_target_ms > 0 {
            self.part_target_ms.saturating_mul(1_000)
        } else if self.part_target_us > 0 {
            self.part_target_us
        } else {
            DEFAULT_PART_TARGET_US
        }
    }

    /// PART-HOLD-BACK in microseconds: three part targets, or the configured
    /// floor when it is larger.
    pub fn part_hold_back_us(&self) -> u64 {
        (u64::from(self.part_target_us()) * 3).max(u64::from(self.part_hold_back_ms) * 1_000)
    }

    /// The options with every derived value filled in. Idempotent.
    pub fn normalized(mut self) -> Self {
        let part_us = self.part_target_us().max(1);
        self.part_target_us = part_us;
        // Keep the millisecond field only when it is exact, so that a second
        // normalization reads the same part target.
        self.part_target_ms = if part_us % 1_000 == 0 {
            part_us / 1_000
        } else {
            0
        };
        self.num_playlists = self.num_playlists.max(1);
        self.segment_min_ms = self.segment_min_ms.max(1);
        self.target_duration_ms = self.target_duration_ms.max(1000);
        if self.window_ms == 0 {
            self.window_ms = DEFAULT_WINDOW_MS;
        }
        if self.max_parts_per_segment == 0 {
            // A segment closes on the first independent part at or after
            // `segment_min_ms`, and is at most the target duration long. A
            // quarter more, plus a few parts, covers a segment that runs long.
            let segment_us = u64::from(self.segment_min_ms.max(self.target_duration_ms)) * 1_000;
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
    streams: Mutex<BTreeSet<u64>>,
    active: AtomicUsize,
}

impl Playlists {
    pub fn new(options: Options) -> (Arc<Self>, Arc<ChunkCache>, Arc<M3u8Cache>) {
        let options = options.normalized();
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));

        (
            Arc::new(Self {
                chunk_cache: Arc::clone(&chunk_cache),
                m3u8_cache: Arc::clone(&m3u8_cache),
                streams: Mutex::new(BTreeSet::new()),
                active: AtomicUsize::new(0),
            }),
            Arc::clone(&chunk_cache),
            Arc::clone(&m3u8_cache),
        )
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub fn fin(&self, id: u64) {
        let removed = {
            let mut streams = self.streams.lock().unwrap();
            streams.remove(&id)
        };
        if removed {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        self.m3u8_cache.zero_stream_id(id);
        self.chunk_cache.zero_stream_id_sync(id);
    }

    /// Add a part whose duration is `fmp4.duration` milliseconds.
    pub fn add(&self, stream_id: u64, fmp4: Fmp4) -> bool {
        let duration_us = u64::from(fmp4.duration) * 1_000;
        self.add_us(stream_id, fmp4, duration_us)
    }

    /// Add a part of `duration_us` microseconds (`fmp4.duration` is not
    /// read). The playlist is not rendered here: a reader of the playlist
    /// cache renders it.
    pub fn add_us(&self, stream_id: u64, fmp4: Fmp4, duration_us: u64) -> bool {
        {
            let mut streams = self.streams.lock().unwrap();
            if !streams.contains(&stream_id) {
                if self.active.load(Ordering::SeqCst) >= self.chunk_cache.options.num_playlists {
                    return false;
                }
                streams.insert(stream_id);
                let n = self.active.fetch_add(1, Ordering::SeqCst);
                info!("PLAY:NEW active={}", n + 1);
            }
        }

        if fmp4.init.is_some() {
            let _ = self.m3u8_cache.ensure_stream_id(stream_id);
        }
        if let Some(init) = fmp4.init {
            let _ = self.m3u8_cache.set_init(stream_id, init);
        }

        match self
            .m3u8_cache
            .add_part_us(stream_id, duration_us, fmp4.key, Some(fmp4.data.len()))
        {
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
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_fin_clears_chunk_cache_entry() {
        let options = Options::default();
        let (playlists, chunk_cache, _m3u8_cache) = Playlists::new(options);
        let stream_id = 101;

        let fmp4 = Fmp4 {
            init: None,
            key: true,
            data: Bytes::from_static(b"test"),
            duration: 500,
        };
        assert!(playlists.add(stream_id, fmp4));
        assert_eq!(playlists.active(), 1);

        let _ = chunk_cache.add_stream_id(stream_id).await;
        assert!(chunk_cache.get_stream_idx(stream_id).await.is_some());

        playlists.fin(stream_id);
        assert_eq!(playlists.active(), 0);

        let cleared = timeout(Duration::from_millis(200), async {
            loop {
                if chunk_cache.get_stream_idx(stream_id).await.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;

        assert!(cleared.is_ok());
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
}
