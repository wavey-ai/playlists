pub mod chunk_cache;
pub mod m3u8_cache;
pub mod m3u8_manifest;
#[cfg(feature = "mesh")]
pub mod mesh;
pub mod multivariant;
mod stream_registry;
pub mod tail_bundle;

use access_unit::Fmp4;
use chunk_cache::ChunkCache;
use m3u8_cache::M3u8Cache;
use m3u8_manifest::M3u8Manifest;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    #[error("Write was superseded by a newer ring position")]
    Superseded,
}

#[derive(Copy, Clone, Debug)]
pub struct Options {
    pub max_segments: usize,
    pub num_playlists: usize,
    pub max_parts_per_segment: usize,
    pub max_parted_segments: usize,
    pub segment_min_ms: u32,
    pub target_duration_ms: u32,
    pub part_target_ms: u32,
    pub buffer_size_kb: usize,
    pub init_size_kb: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            max_segments: 32,
            num_playlists: 5,
            max_parts_per_segment: 128,
            max_parted_segments: 32,
            segment_min_ms: 1500,
            target_duration_ms: 6000,
            part_target_ms: 500,
            buffer_size_kb: 800,
            init_size_kb: 5,
        }
    }
}

impl Options {
    pub(crate) fn normalized(mut self) -> Self {
        self.max_segments = self.max_segments.max(1);
        self.num_playlists = self.num_playlists.max(1);
        self.max_parts_per_segment = self.max_parts_per_segment.max(1);
        self.max_parted_segments = self.max_parted_segments.max(1);
        self.segment_min_ms = self.segment_min_ms.max(1);
        self.target_duration_ms = self.target_duration_ms.max(1000);
        self.part_target_ms = self.part_target_ms.max(1);
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

struct PlaylistEntry {
    closing: AtomicBool,
    manifest: Mutex<M3u8Manifest>,
}

pub type PlaylistCacheBundle = (Arc<Playlists>, Arc<ChunkCache>, Arc<M3u8Cache>);

impl PlaylistEntry {
    fn new(options: Options) -> Self {
        Self {
            closing: AtomicBool::new(false),
            manifest: Mutex::new(M3u8Manifest::new(options)),
        }
    }
}

impl Playlists {
    pub fn new(options: Options) -> PlaylistCacheBundle {
        Self::try_new(options).expect("valid playlist capacities")
    }

    pub fn try_new(options: Options) -> Result<PlaylistCacheBundle, CacheError> {
        let options = options.normalized();
        let chunk_cache = Arc::new(ChunkCache::try_new(options)?);
        let m3u8_cache = Arc::new(M3u8Cache::try_new(options)?);

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

    pub fn fin(&self, id: u64) {
        let entry = {
            let playlists = self
                .playlists
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(entry) = playlists.get(&id) else {
                self.m3u8_cache.zero_stream_id(id);
                self.chunk_cache.zero_stream_id_sync(id);
                return;
            };
            entry.closing.store(true, Ordering::Release);
            Arc::clone(entry)
        };

        // An add holds this lock through its cache commit. Once acquired, no
        // operation using the old entry can publish after cache teardown.
        let _manifest = entry
            .manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.m3u8_cache.zero_stream_id(id);
        self.chunk_cache.zero_stream_id_sync(id);

        let removed = {
            let mut playlists = self
                .playlists
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    pub fn add(&self, stream_id: u64, fmp4: Fmp4) -> bool {
        let max_init_bytes = self.options.init_size_kb * 1024;
        if fmp4
            .init
            .as_ref()
            .is_some_and(|init| init.len() > max_init_bytes)
        {
            return false;
        }
        let entry = {
            let mut playlists = self
                .playlists
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = playlists.get(&stream_id) {
                if entry.closing.load(Ordering::Acquire) {
                    return false;
                }
                Arc::clone(entry)
            } else {
                if playlists.len() >= self.chunk_cache.options.num_playlists {
                    return false;
                }
                let entry = Arc::new(PlaylistEntry::new(self.options));
                playlists.insert(stream_id, Arc::clone(&entry));
                let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
                info!("PLAY:NEW active={active}");
                entry
            }
        };

        let mut playlist = entry
            .manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entry.closing.load(Ordering::Acquire) {
            return false;
        }
        let (m3u8, seg, seq, idx, new_seg) =
            playlist.add_part_with_byte_len(fmp4.duration, fmp4.key, fmp4.data.len());

        if new_seg {
            info!("PLAY:UP active={}", self.active());
        }

        if let Some(init) = fmp4.init {
            if self
                .m3u8_cache
                .ensure_stream_id(stream_id)
                .and_then(|()| self.m3u8_cache.set_init(stream_id, init))
                .is_err()
            {
                return false;
            }
        }
        //self.fmp4_cache.add(stream_id, seq as usize, fmp4.data);
        self.m3u8_cache.add(stream_id, seg, seq, idx, m3u8).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::thread;
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
    fn closing_entry_rejects_add_and_cannot_resurrect_cache_state() {
        let options = Options {
            num_playlists: 1,
            ..Options::default()
        };
        let (playlists, _chunk_cache, m3u8_cache) = Playlists::new(options);
        let part = || Fmp4 {
            init: None,
            key: true,
            data: Bytes::from_static(b"part"),
            duration: 500,
        };
        assert!(playlists.add(7, part()));

        let entry = Arc::clone(
            playlists
                .playlists
                .lock()
                .unwrap()
                .get(&7)
                .expect("live entry"),
        );
        let manifest_guard = entry.manifest.lock().unwrap();
        let finisher = {
            let playlists = Arc::clone(&playlists);
            thread::spawn(move || playlists.fin(7))
        };
        while !entry.closing.load(Ordering::Acquire) {
            thread::yield_now();
        }

        assert!(!playlists.add(7, part()));
        drop(manifest_guard);
        finisher.join().unwrap();

        assert_eq!(playlists.active(), 0);
        assert_eq!(m3u8_cache.last_position(7), None);
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
}
