use bytes::Bytes;
use thiserror::Error;

pub const LL_HLS_MULTIVARIANT_VERSION: u8 = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VariantStream {
    uri: String,
    bandwidth_bps: u64,
    codecs: Option<String>,
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
        Ok(Self {
            uri,
            bandwidth_bps,
            codecs: None,
        })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn bandwidth_bps(&self) -> u64 {
        self.bandwidth_bps
    }

    pub fn with_codecs(mut self, codecs: impl Into<String>) -> Result<Self, PlaylistError> {
        let codecs = codecs.into();
        if codecs.is_empty()
            || !codecs.is_ascii()
            || codecs
                .bytes()
                .any(|byte| byte.is_ascii_control() || matches!(byte, b'"' | b'\\'))
        {
            return Err(PlaylistError::InvalidCodecs);
        }
        self.codecs = Some(codecs);
        Ok(self)
    }

    pub fn codecs(&self) -> Option<&str> {
        self.codecs.as_deref()
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
                "#EXT-X-STREAM-INF:BANDWIDTH={}",
                variant.bandwidth_bps
            ));
            if let Some(codecs) = variant.codecs() {
                playlist.push_str(&format!(",CODECS=\"{codecs}\""));
            }
            playlist.push('\n');
            playlist.push_str(variant.uri());
            playlist.push('\n');
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
    #[error("variant codecs must be a nonempty safe attribute value")]
    InvalidCodecs,
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
                b"#EXTM3U\n#EXT-X-VERSION:10\n\
#EXT-X-STREAM-INF:BANDWIDTH=4000000\nstream.m3u8\n\
#EXT-X-STREAM-INF:BANDWIDTH=4000000\n\
https://edge-b.example/live/801/stream.m3u8\n"
            )
        );
    }

    #[test]
    fn renders_codec_attributes() {
        let playlist =
            MultivariantPlaylist::new(vec![VariantStream::new("stream.m3u8", 4_000_000)
                .unwrap()
                .with_codecs("fLaC")
                .unwrap()])
            .unwrap();

        assert!(String::from_utf8_lossy(&playlist.render())
            .contains("#EXT-X-STREAM-INF:BANDWIDTH=4000000,CODECS=\"fLaC\""));
        assert_eq!(
            VariantStream::new("stream.m3u8", 1)
                .unwrap()
                .with_codecs("bad\"value"),
            Err(PlaylistError::InvalidCodecs)
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
