use bytes::Bytes;
use thiserror::Error;

pub const LL_HLS_MULTIVARIANT_VERSION: u8 = 9;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VariantStream {
    uri: String,
    bandwidth_bps: u64,
}

impl VariantStream {
    pub fn new(uri: impl Into<String>, bandwidth_bps: u64) -> Result<Self, PlaylistError> {
        let uri = uri.into();
        if uri.is_empty() {
            return Err(PlaylistError::EmptyUri);
        }
        if uri.chars().any(char::is_control) {
            return Err(PlaylistError::InvalidUri);
        }
        if bandwidth_bps == 0 {
            return Err(PlaylistError::InvalidBandwidth);
        }
        Ok(Self { uri, bandwidth_bps })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn bandwidth_bps(&self) -> u64 {
        self.bandwidth_bps
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultivariantPlaylist {
    variants: Vec<VariantStream>,
}

impl MultivariantPlaylist {
    pub fn new(variants: Vec<VariantStream>) -> Result<Self, PlaylistError> {
        if variants.is_empty() {
            return Err(PlaylistError::NoVariants);
        }
        Ok(Self { variants })
    }

    pub fn variants(&self) -> &[VariantStream] {
        &self.variants
    }

    pub fn render(&self) -> Bytes {
        let mut playlist = format!("#EXTM3U\n#EXT-X-VERSION:{LL_HLS_MULTIVARIANT_VERSION}\n");
        for variant in &self.variants {
            playlist.push_str(&format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={}\n{}\n",
                variant.bandwidth_bps, variant.uri
            ));
        }
        Bytes::from(playlist)
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PlaylistError {
    #[error("a multivariant playlist requires at least one variant")]
    NoVariants,
    #[error("a variant URI cannot be empty")]
    EmptyUri,
    #[error("a variant URI cannot contain control characters")]
    InvalidUri,
    #[error("variant bandwidth must be greater than zero")]
    InvalidBandwidth,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_duplicate_variants_for_stream_failover() {
        let playlist = MultivariantPlaylist::new(vec![
            VariantStream::new("stream.m3u8", 4_000_000).unwrap(),
            VariantStream::new("https://edge-b.example/live/801/stream.m3u8", 4_000_000).unwrap(),
        ])
        .unwrap();

        assert_eq!(
            playlist.render(),
            Bytes::from_static(
                b"#EXTM3U\n#EXT-X-VERSION:9\n\
#EXT-X-STREAM-INF:BANDWIDTH=4000000\nstream.m3u8\n\
#EXT-X-STREAM-INF:BANDWIDTH=4000000\n\
https://edge-b.example/live/801/stream.m3u8\n"
            )
        );
    }

    #[test]
    fn rejects_invalid_variants() {
        assert_eq!(
            MultivariantPlaylist::new(Vec::new()),
            Err(PlaylistError::NoVariants)
        );
        assert_eq!(VariantStream::new("", 1), Err(PlaylistError::EmptyUri));
        assert_eq!(
            VariantStream::new("stream.m3u8\n#EXT-X-KEY:METHOD=NONE", 1),
            Err(PlaylistError::InvalidUri)
        );
        assert_eq!(
            VariantStream::new("stream.m3u8", 0),
            Err(PlaylistError::InvalidBandwidth)
        );
    }
}
