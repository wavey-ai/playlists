use bytes::Bytes;
use thiserror::Error;

pub const LL_HLS_MULTIVARIANT_VERSION: u8 = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VariantStream {
    uri: String,
    bandwidth_bps: u64,
    average_bandwidth_bps: Option<u64>,
    codecs: Option<String>,
    resolution: Option<(u32, u32)>,
    frame_rate_millihertz: Option<u32>,
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
            average_bandwidth_bps: None,
            codecs: None,
            resolution: None,
            frame_rate_millihertz: None,
        })
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn bandwidth_bps(&self) -> u64 {
        self.bandwidth_bps
    }

    pub fn with_average_bandwidth(
        mut self,
        average_bandwidth_bps: u64,
    ) -> Result<Self, PlaylistError> {
        if average_bandwidth_bps == 0 || average_bandwidth_bps > self.bandwidth_bps {
            return Err(PlaylistError::InvalidAverageBandwidth);
        }
        self.average_bandwidth_bps = Some(average_bandwidth_bps);
        Ok(self)
    }

    pub fn average_bandwidth_bps(&self) -> Option<u64> {
        self.average_bandwidth_bps
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

    pub fn with_resolution(mut self, width: u32, height: u32) -> Result<Self, PlaylistError> {
        if width == 0 || height == 0 {
            return Err(PlaylistError::InvalidResolution);
        }
        self.resolution = Some((width, height));
        Ok(self)
    }

    pub fn resolution(&self) -> Option<(u32, u32)> {
        self.resolution
    }

    /// Advertise the maximum frame rate in thousandths of a frame per second.
    ///
    /// For example, 29.97 fps is represented as `29_970`.
    pub fn with_frame_rate_millihertz(
        mut self,
        frame_rate_millihertz: u32,
    ) -> Result<Self, PlaylistError> {
        if frame_rate_millihertz == 0 {
            return Err(PlaylistError::InvalidFrameRate);
        }
        self.frame_rate_millihertz = Some(frame_rate_millihertz);
        Ok(self)
    }

    pub fn frame_rate_millihertz(&self) -> Option<u32> {
        self.frame_rate_millihertz
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
            if let Some(average_bandwidth_bps) = variant.average_bandwidth_bps() {
                playlist.push_str(&format!(",AVERAGE-BANDWIDTH={average_bandwidth_bps}"));
            }
            if let Some(codecs) = variant.codecs() {
                playlist.push_str(&format!(",CODECS=\"{codecs}\""));
            }
            if let Some((width, height)) = variant.resolution() {
                playlist.push_str(&format!(",RESOLUTION={width}x{height}"));
            }
            if let Some(frame_rate_millihertz) = variant.frame_rate_millihertz() {
                playlist.push_str(&format!(
                    ",FRAME-RATE={}.{:03}",
                    frame_rate_millihertz / 1_000,
                    frame_rate_millihertz % 1_000
                ));
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
    #[error("variant average bandwidth must be positive and no greater than peak bandwidth")]
    InvalidAverageBandwidth,
    #[error("variant codecs must be a nonempty safe attribute value")]
    InvalidCodecs,
    #[error("variant resolution must have nonzero width and height")]
    InvalidResolution,
    #[error("variant frame rate must be greater than zero")]
    InvalidFrameRate,
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
    fn renders_complete_video_variant_attributes() {
        let playlist =
            MultivariantPlaylist::new(vec![VariantStream::new("720p/stream.m3u8", 4_500_000)
                .unwrap()
                .with_average_bandwidth(3_500_000)
                .unwrap()
                .with_codecs("avc1.64001f,mp4a.40.2")
                .unwrap()
                .with_resolution(1280, 720)
                .unwrap()
                .with_frame_rate_millihertz(59_940)
                .unwrap()])
            .unwrap();

        assert!(String::from_utf8_lossy(&playlist.render()).contains(
            "#EXT-X-STREAM-INF:BANDWIDTH=4500000,AVERAGE-BANDWIDTH=3500000,\
CODECS=\"avc1.64001f,mp4a.40.2\",RESOLUTION=1280x720,FRAME-RATE=59.940\n\
720p/stream.m3u8"
        ));
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
        assert_eq!(
            VariantStream::new("stream.m3u8", 1)
                .unwrap()
                .with_average_bandwidth(2),
            Err(PlaylistError::InvalidAverageBandwidth)
        );
        assert_eq!(
            VariantStream::new("stream.m3u8", 1)
                .unwrap()
                .with_resolution(0, 720),
            Err(PlaylistError::InvalidResolution)
        );
        assert_eq!(
            VariantStream::new("stream.m3u8", 1)
                .unwrap()
                .with_frame_rate_millihertz(0),
            Err(PlaylistError::InvalidFrameRate)
        );
    }
}
