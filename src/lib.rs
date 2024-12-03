pub mod fmp4_cache;
pub mod m3u8_cache;
pub mod m3u8_manifest;

use access_unit::Fmp4;
use fmp4_cache::Fmp4Cache;
use m3u8_cache::M3u8Cache;
use m3u8_manifest::M3u8Manifest;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

#[derive(Copy, Clone)]
pub struct Options {
    pub max_segments: usize,
    pub num_playlists: usize,
    pub max_parts_per_segment: usize,
    pub max_parted_segments: usize,
    pub segment_min_ms: u32,
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
            segment_min_ms: 10_000,
            buffer_size_kb: 800,
            init_size_kb: 5,
        }
    }
}

pub struct Playlists {
    fmp4_cache: Arc<Fmp4Cache>,
    m3u8_cache: Arc<M3u8Cache>,
    playlists: Mutex<BTreeMap<u64, M3u8Manifest>>,
    active: AtomicUsize,
}

impl Playlists {
    pub fn new(options: Options) -> (Arc<Self>, Arc<Fmp4Cache>, Arc<M3u8Cache>) {
        let fmp4_cache = Arc::new(Fmp4Cache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));

        (
            Arc::new(Self {
                fmp4_cache: Arc::clone(&fmp4_cache),
                m3u8_cache: Arc::clone(&m3u8_cache),
                playlists: Mutex::new(BTreeMap::new()),
                active: AtomicUsize::new(0),
            }),
            Arc::clone(&fmp4_cache),
            Arc::clone(&m3u8_cache),
        )
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub fn fin(&self, id: u64) {
        let mut playlists = self.playlists.lock().unwrap();
        if playlists.remove(&id).is_some() {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        self.m3u8_cache.zero_stream_id(id);
        self.fmp4_cache.zero_stream_id(id);
    }

    pub fn add(&self, stream_id: u64, fmp4: Fmp4) -> bool {
        let mut playlists = self.playlists.lock().unwrap();

        if !playlists.contains_key(&stream_id) {
            if self.active.load(Ordering::SeqCst) >= self.fmp4_cache.options.num_playlists {
                return false;
            }

            let n = self.active.fetch_add(1, Ordering::SeqCst);
            info!("PLAY:NEW active={}", n + 1);
        }

        let (m3u8, seg, seq, idx, new_seg) = {
            let playlist: &mut M3u8Manifest = playlists
                .entry(stream_id)
                .or_insert_with(|| M3u8Manifest::new(Options::default()));
            playlist.add_part(fmp4.duration, fmp4.key)
        };

        if new_seg {
            info!("PLAY:UP active={}", self.active());
        }

        if let Some(init) = fmp4.init {
            self.m3u8_cache.set_init(stream_id, init);
        }
        self.fmp4_cache.add(stream_id, seq as usize, fmp4.data);
        self.m3u8_cache.add(stream_id, seg, seq, idx, m3u8);

        true
    }
}
