# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-01-03

### Changed

- Renamed `fmp4_cache` module to `chunk_cache` for more generic naming
- Renamed `Fmp4Cache` struct to `ChunkCache`
- Renamed `Playlists.fmp4_cache` field to `Playlists.chunk_cache`
- Updated README to reflect new naming

### Migration

Replace all occurrences:
- `playlists::fmp4_cache::Fmp4Cache` → `playlists::chunk_cache::ChunkCache`
- `fmp4_cache` field access → `chunk_cache`

## [0.1.1] - Previous

- Initial release with `Fmp4Cache`, `M3u8Cache`, and `M3u8Manifest`
