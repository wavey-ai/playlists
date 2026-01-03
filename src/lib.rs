pub mod chunk_cache;
pub mod m3u8_cache;
pub mod m3u8_manifest;

use access_unit::Fmp4;
use chunk_cache::ChunkCache;
use m3u8_cache::M3u8Cache;
use m3u8_manifest::M3u8Manifest;
use std::collections::BTreeMap;
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

#[derive(Copy, Clone, Debug)]
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
            segment_min_ms: 1500,
            buffer_size_kb: 800,
            init_size_kb: 5,
        }
    }
}

pub struct Playlists {
    pub chunk_cache: Arc<ChunkCache>,
    m3u8_cache: Arc<M3u8Cache>,
    playlists: Mutex<BTreeMap<u64, M3u8Manifest>>,
    active: AtomicUsize,
    options: Options,
}

impl Playlists {
    pub fn new(options: Options) -> (Arc<Self>, Arc<ChunkCache>, Arc<M3u8Cache>) {
        let chunk_cache = Arc::new(ChunkCache::new(options));
        let m3u8_cache = Arc::new(M3u8Cache::new(options));

        (
            Arc::new(Self {
                chunk_cache: Arc::clone(&chunk_cache),
                m3u8_cache: Arc::clone(&m3u8_cache),
                playlists: Mutex::new(BTreeMap::new()),
                active: AtomicUsize::new(0),
                options,
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
            let mut playlists = self.playlists.lock().unwrap();
            playlists.remove(&id).is_some()
        };
        if removed {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        self.m3u8_cache.zero_stream_id(id);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let chunk_cache = Arc::clone(&self.chunk_cache);
            let _ = handle.spawn(async move {
                chunk_cache.zero_stream_id(id).await;
            });
        } else if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            rt.block_on(self.chunk_cache.zero_stream_id(id));
        }
    }

    pub fn add(&self, stream_id: u64, fmp4: Fmp4) -> bool {
        let mut playlists = self.playlists.lock().unwrap();

        if !playlists.contains_key(&stream_id) {
            if self.active.load(Ordering::SeqCst) >= self.chunk_cache.options.num_playlists {
                return false;
            }

            let n = self.active.fetch_add(1, Ordering::SeqCst);
            info!("PLAY:NEW active={}", n + 1);
        }

        let (m3u8, seg, seq, idx, new_seg) = {
            let playlist: &mut M3u8Manifest = playlists
                .entry(stream_id)
                .or_insert_with(|| M3u8Manifest::new(self.options));
            playlist.add_part(fmp4.duration, fmp4.key)
        };

        if new_seg {
            info!("PLAY:UP active={}", self.active());
        }

        if let Some(init) = fmp4.init {
            let _ = self.m3u8_cache.set_init(stream_id, init);
        }
        //self.fmp4_cache.add(stream_id, seq as usize, fmp4.data);
        let _ = self.m3u8_cache.add(stream_id, seg, seq, idx, m3u8);

        true
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
}
