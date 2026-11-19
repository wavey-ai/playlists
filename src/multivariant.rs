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
    pathway_id: Option<String>,
}

/// Whether a value is a valid HLS Content Steering Pathway ID: one or more
/// characters from `[a-zA-Z0-9._-]`.
pub fn is_valid_pathway_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn is_safe_quoted_uri(value: &str) -> bool {
    !value.is_empty()
        && !value
            .chars()
            .any(|character| character.is_control() || matches!(character, '"' | '\\'))
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
            pathway_id: None,
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

    /// Name the Content Steering pathway of this variant. Variants that carry
    /// the same content from different caches use one pathway for each cache.
    pub fn with_pathway_id(mut self, pathway_id: impl Into<String>) -> Result<Self, PlaylistError> {
        let pathway_id = pathway_id.into();
        if !is_valid_pathway_id(&pathway_id) {
            return Err(PlaylistError::InvalidPathwayId);
        }
        self.pathway_id = Some(pathway_id);
        Ok(self)
    }

    pub fn pathway_id(&self) -> Option<&str> {
        self.pathway_id.as_deref()
    }
}

/// The `EXT-X-CONTENT-STEERING` tag of a multivariant playlist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentSteering {
    server_uri: String,
    pathway_id: Option<String>,
}

impl ContentSteering {
    pub fn new(server_uri: impl Into<String>) -> Result<Self, PlaylistError> {
        let server_uri = server_uri.into();
        if !is_safe_quoted_uri(&server_uri) {
            return Err(PlaylistError::InvalidUri);
        }
        Ok(Self {
            server_uri,
            pathway_id: None,
        })
    }

    /// The pathway that a client uses until it has loaded a steering manifest.
    pub fn with_initial_pathway(
        mut self,
        pathway_id: impl Into<String>,
    ) -> Result<Self, PlaylistError> {
        let pathway_id = pathway_id.into();
        if !is_valid_pathway_id(&pathway_id) {
            return Err(PlaylistError::InvalidPathwayId);
        }
        self.pathway_id = Some(pathway_id);
        Ok(self)
    }

    pub fn server_uri(&self) -> &str {
        &self.server_uri
    }

    pub fn initial_pathway(&self) -> Option<&str> {
        self.pathway_id.as_deref()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultivariantPlaylist {
    variants: Vec<VariantStream>,
    content_steering: Option<ContentSteering>,
}

impl MultivariantPlaylist {
    pub fn new(variants: Vec<VariantStream>) -> Result<Self, PlaylistError> {
        if variants.is_empty() {
            return Err(PlaylistError::NoVariants);
        }
        Ok(Self {
            variants,
            content_steering: None,
        })
    }

    /// Add Content Steering. Every variant must then name its pathway, and
    /// the initial pathway must be one of them.
    pub fn with_content_steering(
        mut self,
        content_steering: ContentSteering,
    ) -> Result<Self, PlaylistError> {
        if self
            .variants
            .iter()
            .any(|variant| variant.pathway_id().is_none())
        {
            return Err(PlaylistError::MissingPathwayId);
        }
        if let Some(initial) = content_steering.initial_pathway() {
            if !self
                .variants
                .iter()
                .any(|variant| variant.pathway_id() == Some(initial))
            {
                return Err(PlaylistError::UnknownPathwayId);
            }
        }
        self.content_steering = Some(content_steering);
        Ok(self)
    }

    pub fn content_steering(&self) -> Option<&ContentSteering> {
        self.content_steering.as_ref()
    }

    pub fn variants(&self) -> &[VariantStream] {
        &self.variants
    }

    pub fn render(&self) -> Bytes {
        let mut playlist = format!("#EXTM3U\n#EXT-X-VERSION:{LL_HLS_MULTIVARIANT_VERSION}\n");
        if let Some(content_steering) = &self.content_steering {
            playlist.push_str(&format!(
                "#EXT-X-CONTENT-STEERING:SERVER-URI=\"{}\"",
                content_steering.server_uri()
            ));
            if let Some(pathway_id) = content_steering.initial_pathway() {
                playlist.push_str(&format!(",PATHWAY-ID=\"{pathway_id}\""));
            }
            playlist.push('\n');
        }
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
            if let Some(pathway_id) = variant.pathway_id() {
                playlist.push_str(&format!(",PATHWAY-ID=\"{pathway_id}\""));
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
    #[error("a pathway ID must use only letters, digits, '.', '_' and '-'")]
    InvalidPathwayId,
    #[error("content steering requires a pathway ID on every variant")]
    MissingPathwayId,
    #[error("the initial content steering pathway is not a pathway of any variant")]
    UnknownPathwayId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_one_pathway_for_each_cache_with_content_steering() {
        let variant = |cache: &str| {
            VariantStream::new(
                format!("https://{cache}.example/live/7/stream.m3u8"),
                16_000_000,
            )
            .unwrap()
            .with_codecs("avc1.640033,mp4a.40.2")
            .unwrap()
            .with_pathway_id(cache)
            .unwrap()
        };
        let playlist =
            MultivariantPlaylist::new(vec![variant("lon-edge-2"), variant("lon-edge-1")])
                .unwrap()
                .with_content_steering(
                    ContentSteering::new("/live/7/steering?sid=abc")
                        .unwrap()
                        .with_initial_pathway("lon-edge-2")
                        .unwrap(),
                )
                .unwrap();

        assert_eq!(
            playlist.render(),
            Bytes::from_static(
                b"#EXTM3U\n#EXT-X-VERSION:10\n\
#EXT-X-CONTENT-STEERING:SERVER-URI=\"/live/7/steering?sid=abc\",PATHWAY-ID=\"lon-edge-2\"\n\
#EXT-X-STREAM-INF:BANDWIDTH=16000000,CODECS=\"avc1.640033,mp4a.40.2\",PATHWAY-ID=\"lon-edge-2\"\n\
https://lon-edge-2.example/live/7/stream.m3u8\n\
#EXT-X-STREAM-INF:BANDWIDTH=16000000,CODECS=\"avc1.640033,mp4a.40.2\",PATHWAY-ID=\"lon-edge-1\"\n\
https://lon-edge-1.example/live/7/stream.m3u8\n"
            )
        );
    }

    #[test]
    fn content_steering_rejects_unsafe_or_inconsistent_pathways() {
        let plain = VariantStream::new("stream.m3u8", 1_000).unwrap();
        assert_eq!(
            plain.clone().with_pathway_id("edge 1").unwrap_err(),
            PlaylistError::InvalidPathwayId
        );
        assert_eq!(
            ContentSteering::new("steer\"ing").unwrap_err(),
            PlaylistError::InvalidUri
        );
        assert_eq!(
            MultivariantPlaylist::new(vec![plain.clone()])
                .unwrap()
                .with_content_steering(ContentSteering::new("/steering").unwrap())
                .unwrap_err(),
            PlaylistError::MissingPathwayId
        );
        assert_eq!(
            MultivariantPlaylist::new(vec![plain.with_pathway_id("edge-1").unwrap()])
                .unwrap()
                .with_content_steering(
                    ContentSteering::new("/steering")
                        .unwrap()
                        .with_initial_pathway("edge-2")
                        .unwrap()
                )
                .unwrap_err(),
            PlaylistError::UnknownPathwayId
        );
    }

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
