//! Compact relay objects (low-latency wave 7 item 3): a small media object
//! carried as its per-part fields only, against a stream descriptor that a
//! receiver learnt from a whole object of the same stream.
//!
//! A 1 ms audio part is 40 to 300 bytes, but its canonical media-object
//! envelope (`media_object::encode`) is about 700 bytes more: sixteen string
//! metadata entries, three timestamps with their clock ids, the object key
//! with its 32-byte hash. And every RLS1 datagram of it repeats the key, the
//! hash and the tenant, stream and track strings in its own 127-byte header,
//! ahead of a 36-byte RQD2 header. Almost all of that is the same for every
//! part of the stream.
//!
//! - The **descriptor** of an object is everything that does not change from
//!   part to part (its *shape*, [`shape_bytes`]): the key's strings, epoch,
//!   group and version; the kind, keyframe flag and configuration epoch; for
//!   each timestamp, which one it is, its clock id and its confidence; every
//!   metadata key, and the values of all but the [`VARYING_METADATA`]. Its id
//!   ([`descriptor_id`]) is a 32-bit digest of the shape. A receiver learns
//!   a descriptor from any whole object of the stream ([`CompactTemplates`]):
//!   the sender sends a whole object when the shape changes and again every
//!   so often ([`CompactSender`]), so a receiver that joins learns it within
//!   that interval.
//! - The **slim object** ([`encode_slim`]) is the descriptor id, the object
//!   number, the timestamp values, the varying metadata values as numbers,
//!   and the payload: about 30 bytes and the part.
//! - The **compact datagram** (`RLC1`, [`encode_datagram`]) carries one slim
//!   object of a part that fits one datagram, with the fields of an RLS1
//!   header that a relay reads (generation, subscription, deadline, role,
//!   kind, path, stream, epoch, group, object, version, packet sequence,
//!   symbol id) as varints, the tenant and track as a code: about 30 bytes
//!   for what RLS1 and RQD2 spend 163 on. There is no RaptorQ: every copy of
//!   the part (source or repair) carries the same slim object, so any one
//!   decodes it.
//!
//! A receiver rebuilds the exact object from the descriptor and the slim
//! object ([`expand_slim`]), so the envelope it stores, and the LL-HLS part
//! read from it, are byte for byte those of the whole object. The sender
//! checks that on every part before it sends one compact
//! ([`CompactSender::decide`]): a part whose rebuilt envelope would differ
//! goes whole.

use media_object::{
    ClockConfidence, ClockConfidenceLevel, ClockTimestamp, MediaObject, ObjectKey, ObjectKind,
    Stage, StageTimestamp,
};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;

/// The magic of a compact relay datagram.
pub const DATAGRAM_MAGIC: [u8; 4] = *b"RLC1";
/// The wire version of a compact relay datagram.
pub const DATAGRAM_VERSION: u8 = 1;
/// The version byte of a slim object.
pub const SLIM_VERSION: u8 = 1;
/// The metadata whose values change from part to part: carried as numbers
/// in each slim object, left out of the descriptor.
pub const VARYING_METADATA: [&str; 2] = ["hls-msn", "hls-part"];
/// The tenants a compact datagram names by a code (its index).
pub const TENANT_CODES: [&str; 1] = ["default"];
/// The tracks a compact datagram names by a code (its index).
pub const TRACK_CODES: [&str; 3] = ["opaque-audio", "muxed-fmp4", "audio"];
/// The most timestamps a slim object carries.
pub const MAX_TIMESTAMPS: usize = 10;
/// The longest stream id a compact datagram carries.
pub const MAX_STREAM_BYTES: usize = 64;
/// A slim object's in-place view has no published timestamp.
const NO_PUBLISHED: u8 = 0xff;
const DEADLINE_TAG: u8 = 0xd0;
const CAPTURE_TAG: u8 = 0xc0;

/// Why a compact datagram or slim object could not be read or rebuilt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactError(pub &'static str);

impl fmt::Display for CompactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for CompactError {}

type Result<T> = std::result::Result<T, CompactError>;

// ---------------------------------------------------------------- varints

fn put_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

struct Reader<'a> {
    input: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, at: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(len).ok_or(CompactError("truncated"))?;
        let bytes = self
            .input
            .get(self.at..end)
            .ok_or(CompactError("truncated"))?;
        self.at = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            let bits = u64::from(byte & 0x7f);
            if shift == 63 && bits > 1 {
                return Err(CompactError("varint overflow"));
            }
            value |= bits << shift;
            if byte & 0x80 == 0 {
                // One encoding per value: no trailing zero groups.
                if byte == 0 && shift > 0 {
                    return Err(CompactError("varint not minimal"));
                }
                return Ok(value);
            }
        }
        Err(CompactError("varint overflow"))
    }

    fn rest(&mut self) -> &'a [u8] {
        let rest = &self.input[self.at..];
        self.at = self.input.len();
        rest
    }
}

// ------------------------------------------------------------- descriptor

/// The value of a varying metadata entry as a number, when it is written
/// the one way a number is (`u64::to_string`).
fn canonical_decimal(value: &[u8]) -> Option<u64> {
    if value.is_empty() || value.len() > 20 || (value.len() > 1 && value[0] == b'0') {
        return None;
    }
    if !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn is_varying(key: &str) -> bool {
    VARYING_METADATA.contains(&key)
}

fn put_str(output: &mut Vec<u8>, value: &str) {
    put_varint(output, value.len() as u64);
    output.extend_from_slice(value.as_bytes());
}

fn put_clock(output: &mut Vec<u8>, timestamp: &ClockTimestamp) {
    put_str(output, timestamp.clock_id());
    let confidence = timestamp.confidence();
    output.push(confidence.level() as u8);
    match confidence.maximum_error_ns() {
        Some(error) => {
            output.push(1);
            put_varint(output, error);
        }
        None => output.push(0),
    }
}

/// The timestamps of an object in the order a slim object carries them:
/// the deadline, the capture time, then the stage timestamps (sorted, as
/// the object keeps them).
fn timestamps(object: &MediaObject) -> impl Iterator<Item = (u8, &ClockTimestamp)> {
    object
        .deadline()
        .map(|deadline| (DEADLINE_TAG, deadline))
        .into_iter()
        .chain(
            object
                .capture_timestamp()
                .map(|capture| (CAPTURE_TAG, capture)),
        )
        .chain(
            object
                .stage_timestamps()
                .iter()
                .map(|stage| (stage.stage() as u8, stage.timestamp())),
        )
}

/// The shape of an object: everything a slim object does not carry, in one
/// canonical byte string. `None` for an object that is never sent compact:
/// not a media object, one with dependencies, too many timestamps, or a
/// varying metadata value that is not a plain number.
#[must_use]
pub fn shape_bytes(object: &MediaObject) -> Option<Vec<u8>> {
    if object.kind() != ObjectKind::Media || !object.dependencies().is_empty() {
        return None;
    }
    let key = object.key();
    let mut shape = Vec::with_capacity(512);
    shape.extend_from_slice(b"MOBS-shape-1");
    put_str(&mut shape, key.tenant());
    put_str(&mut shape, key.stream());
    put_str(&mut shape, key.track());
    put_varint(&mut shape, key.epoch());
    put_varint(&mut shape, key.group());
    put_varint(&mut shape, u64::from(key.version()));
    shape.push(object.kind() as u8);
    shape.push(u8::from(object.is_keyframe()));
    put_varint(&mut shape, object.configuration_epoch());
    let mut count = 0;
    for (tag, timestamp) in timestamps(object) {
        count += 1;
        shape.push(tag);
        put_clock(&mut shape, timestamp);
    }
    if count > MAX_TIMESTAMPS {
        return None;
    }
    shape.push(0xee);
    put_varint(&mut shape, object.metadata().len() as u64);
    for (name, value) in object.metadata() {
        put_str(&mut shape, name);
        if is_varying(name) {
            canonical_decimal(value)?;
            shape.push(0xff);
        } else {
            shape.push(0);
            put_varint(&mut shape, value.len() as u64);
            shape.extend_from_slice(value);
        }
    }
    Some(shape)
}

/// The descriptor id of a shape.
#[must_use]
pub fn shape_id(shape: &[u8]) -> u32 {
    (xxh3_64(shape) >> 32) as u32
}

/// The descriptor id of an object; `None` when it is never sent compact.
#[must_use]
pub fn descriptor_id(object: &MediaObject) -> Option<u32> {
    shape_bytes(object).map(|shape| shape_id(&shape))
}

// ------------------------------------------------------------ slim object

/// A slim object of `object` under descriptor `descriptor` (the object's
/// own [`descriptor_id`]); `None` when it is never sent compact.
#[must_use]
pub fn encode_slim(descriptor: u32, object: &MediaObject) -> Option<Vec<u8>> {
    let payload = object.payload();
    let mut slim = Vec::with_capacity(48 + payload.len());
    slim.push(SLIM_VERSION);
    slim.extend_from_slice(&descriptor.to_be_bytes());
    put_varint(&mut slim, object.key().object());
    let mut published = NO_PUBLISHED;
    let mut values = [0_i64; MAX_TIMESTAMPS];
    let mut count = 0;
    for (index, (tag, timestamp)) in timestamps(object).enumerate() {
        if index >= MAX_TIMESTAMPS {
            return None;
        }
        if tag == Stage::Published as u8 {
            published = index as u8;
        }
        values[index] = timestamp.unix_time_ns();
        count = index + 1;
    }
    slim.push(count as u8);
    slim.push(published);
    let mut previous = 0_i64;
    for &value in &values[..count] {
        put_varint(&mut slim, zigzag(value.wrapping_sub(previous)));
        previous = value;
    }
    let varying = object
        .metadata()
        .iter()
        .filter(|(name, _)| is_varying(name))
        .map(|(_, value)| canonical_decimal(value))
        .collect::<Option<Vec<_>>>()?;
    slim.push(varying.len() as u8);
    for value in varying {
        put_varint(&mut slim, value);
    }
    slim.extend_from_slice(payload);
    Some(slim)
}

/// A slim object read in place, without its descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlimView<'a> {
    pub descriptor: u32,
    pub object: u64,
    timestamps: [i64; MAX_TIMESTAMPS],
    timestamp_count: usize,
    published: u8,
    varying: [u64; VARYING_METADATA.len()],
    varying_count: usize,
    /// The object's payload (for an opaque or fMP4 part, the bytes of the
    /// LL-HLS part).
    pub payload: &'a [u8],
}

impl<'a> SlimView<'a> {
    /// Read a slim object.
    ///
    /// # Errors
    ///
    /// A truncated or malformed slim object.
    pub fn parse(slim: &'a [u8]) -> Result<Self> {
        let mut reader = Reader::new(slim);
        if reader.u8()? != SLIM_VERSION {
            return Err(CompactError("slim version"));
        }
        let id = reader.take(4)?;
        let descriptor = u32::from_be_bytes([id[0], id[1], id[2], id[3]]);
        let object = reader.varint()?;
        let timestamp_count = usize::from(reader.u8()?);
        if timestamp_count > MAX_TIMESTAMPS {
            return Err(CompactError("timestamp count"));
        }
        let published = reader.u8()?;
        if published != NO_PUBLISHED && usize::from(published) >= timestamp_count {
            return Err(CompactError("published index"));
        }
        let mut timestamps = [0_i64; MAX_TIMESTAMPS];
        let mut previous = 0_i64;
        for value in timestamps.iter_mut().take(timestamp_count) {
            previous = previous.wrapping_add(unzigzag(reader.varint()?));
            *value = previous;
        }
        let varying_count = usize::from(reader.u8()?);
        if varying_count > VARYING_METADATA.len() {
            return Err(CompactError("varying count"));
        }
        let mut varying = [0_u64; VARYING_METADATA.len()];
        for value in varying.iter_mut().take(varying_count) {
            *value = reader.varint()?;
        }
        Ok(Self {
            descriptor,
            object,
            timestamps,
            timestamp_count,
            published,
            varying,
            varying_count,
            payload: reader.rest(),
        })
    }

    /// The timestamp values, in the order of [`encode_slim`].
    #[must_use]
    pub fn timestamps(&self) -> &[i64] {
        &self.timestamps[..self.timestamp_count]
    }

    /// The publication time (Unix ns), if the object has one.
    #[must_use]
    pub fn published_unix_ns(&self) -> Option<i64> {
        (self.published != NO_PUBLISHED).then(|| self.timestamps[usize::from(self.published)])
    }
}

/// Rebuild the object of `slim` from `template`, a whole object of the
/// descriptor that `slim` names.
///
/// # Errors
///
/// A malformed slim object, a template of another descriptor or shape, or
/// an object that does not build.
pub fn expand_slim(template: &MediaObject, slim: &[u8]) -> Result<MediaObject> {
    let view = SlimView::parse(slim)?;
    expand_view(template, &view)
}

fn expand_view(template: &MediaObject, view: &SlimView<'_>) -> Result<MediaObject> {
    let invalid = |_| CompactError("object does not build");
    let key = template.key();
    let rebuilt_key = ObjectKey::for_payload(
        key.tenant(),
        key.stream(),
        key.track(),
        key.epoch(),
        key.group(),
        view.object,
        key.version(),
        view.payload,
    )
    .map_err(invalid)?;
    let mut builder = MediaObject::builder(rebuilt_key, template.kind(), view.payload.to_vec())
        .with_keyframe(template.is_keyframe())
        .with_configuration_epoch(template.configuration_epoch());
    let values = view.timestamps();
    let mut index = 0;
    let mut next = |timestamp: &ClockTimestamp| -> Result<ClockTimestamp> {
        let value = *values.get(index).ok_or(CompactError("timestamp count"))?;
        index += 1;
        ClockTimestamp::new(value, timestamp.clock_id(), timestamp.confidence()).map_err(invalid)
    };
    if let Some(deadline) = template.deadline() {
        builder = builder.with_deadline(next(deadline)?);
    }
    if let Some(capture) = template.capture_timestamp() {
        builder = builder.with_capture_timestamp(next(capture)?);
    }
    for stage in template.stage_timestamps() {
        builder = builder
            .with_stage_timestamp(StageTimestamp::new(stage.stage(), next(stage.timestamp())?));
    }
    if index != values.len() {
        return Err(CompactError("timestamp count"));
    }
    let mut varying = 0;
    for (name, value) in template.metadata() {
        if is_varying(name) {
            if varying >= view.varying_count {
                return Err(CompactError("varying count"));
            }
            builder = builder.with_metadata(name.clone(), view.varying[varying].to_string());
            varying += 1;
        } else {
            builder = builder.with_metadata(name.clone(), value.clone());
        }
    }
    if varying != view.varying_count {
        return Err(CompactError("varying count"));
    }
    builder.build().map_err(invalid)
}

/// A clock confidence from its parts (for tests and tools).
#[must_use]
pub fn confidence(level: ClockConfidenceLevel, maximum_error_ns: u64) -> ClockConfidence {
    match level {
        ClockConfidenceLevel::Unknown => ClockConfidence::unknown(),
        ClockConfidenceLevel::Estimated => ClockConfidence::estimated(maximum_error_ns),
        ClockConfidenceLevel::Synchronized => ClockConfidence::synchronized(maximum_error_ns),
        ClockConfidenceLevel::Traceable => ClockConfidence::traceable(maximum_error_ns),
    }
}

// ------------------------------------------------------- compact datagram

/// The relay fields of a compact datagram. Role, kind and path are the
/// RLS1 tags (role 0 source, 1 repair; kind 0..=5; path 0..=2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactHeader<'a> {
    pub generation: u64,
    pub subscription_id: u64,
    pub deadline_us: u64,
    pub role: u8,
    pub kind: u8,
    pub path: u8,
    pub tenant: &'static str,
    pub stream: &'a str,
    pub track: &'static str,
    pub epoch: u64,
    pub group: u64,
    pub object: u64,
    pub version: u32,
    pub packet_sequence: u32,
    pub esi: u32,
}

/// The code of a tenant or track that a compact datagram can name.
#[must_use]
pub fn code_of(table: &[&'static str], value: &str) -> Option<u8> {
    table
        .iter()
        .position(|candidate| *candidate == value)
        .map(|index| index as u8)
}

/// The `&'static` name of a tenant or track a compact datagram can name.
#[must_use]
pub fn static_name(table: &[&'static str], value: &str) -> Option<&'static str> {
    table.iter().copied().find(|candidate| *candidate == value)
}

fn valid_stream(stream: &str) -> bool {
    !stream.is_empty() && stream.len() <= MAX_STREAM_BYTES && !stream.chars().any(char::is_control)
}

/// The header bytes of a compact datagram; the slim object follows them.
/// `None` for a tenant, track or stream that a compact datagram cannot
/// carry, or a tag out of range.
#[must_use]
pub fn encode_header(header: &CompactHeader<'_>) -> Option<Vec<u8>> {
    let tenant = code_of(&TENANT_CODES, header.tenant)?;
    let track = code_of(&TRACK_CODES, header.track)?;
    if header.role > 1 || header.kind > 5 || header.path > 2 || !valid_stream(header.stream) {
        return None;
    }
    let mut output = Vec::with_capacity(64);
    output.extend_from_slice(&DATAGRAM_MAGIC);
    output.push(DATAGRAM_VERSION);
    output.push(header.role | (header.path << 2) | (header.kind << 4));
    output.push(tenant | (track << 4));
    put_varint(&mut output, header.generation);
    put_varint(&mut output, header.subscription_id);
    output.extend_from_slice(&header.deadline_us.to_be_bytes());
    output.push(header.stream.len() as u8);
    output.extend_from_slice(header.stream.as_bytes());
    put_varint(&mut output, header.epoch);
    put_varint(&mut output, header.group);
    put_varint(&mut output, header.object);
    put_varint(&mut output, u64::from(header.version));
    output.extend_from_slice(&header.packet_sequence.to_be_bytes());
    put_varint(&mut output, u64::from(header.esi));
    Some(output)
}

/// A whole compact datagram: [`encode_header`] and the slim object.
#[must_use]
pub fn encode_datagram(header: &CompactHeader<'_>, slim: &[u8]) -> Option<Vec<u8>> {
    let mut output = encode_header(header)?;
    output.extend_from_slice(slim);
    Some(output)
}

/// Whether a datagram is a compact relay datagram.
#[must_use]
pub fn is_compact_datagram(datagram: &[u8]) -> bool {
    datagram.starts_with(&DATAGRAM_MAGIC)
}

/// Read a compact datagram in place: its header and its slim object (not
/// read here; see [`SlimView::parse`]).
///
/// # Errors
///
/// A datagram that is not a valid compact datagram.
pub fn parse_datagram(datagram: &[u8]) -> Result<(CompactHeader<'_>, &[u8])> {
    let mut reader = Reader::new(datagram);
    if reader.take(4)? != DATAGRAM_MAGIC {
        return Err(CompactError("not RLC1"));
    }
    if reader.u8()? != DATAGRAM_VERSION {
        return Err(CompactError("unsupported version"));
    }
    let tags = reader.u8()?;
    let role = tags & 0x03;
    let path = (tags >> 2) & 0x03;
    let kind = tags >> 4;
    if role > 1 || path > 2 || kind > 5 {
        return Err(CompactError("tags"));
    }
    let codes = reader.u8()?;
    let tenant = *TENANT_CODES
        .get(usize::from(codes & 0x0f))
        .ok_or(CompactError("tenant code"))?;
    let track = *TRACK_CODES
        .get(usize::from(codes >> 4))
        .ok_or(CompactError("track code"))?;
    let generation = reader.varint()?;
    let subscription_id = reader.varint()?;
    let deadline = reader.take(8)?;
    let deadline_us = u64::from_be_bytes(deadline.try_into().expect("eight bytes"));
    let stream_len = usize::from(reader.u8()?);
    let stream = std::str::from_utf8(reader.take(stream_len)?)
        .map_err(|_| CompactError("stream not UTF-8"))?;
    if !valid_stream(stream) {
        return Err(CompactError("stream"));
    }
    let epoch = reader.varint()?;
    let group = reader.varint()?;
    let object = reader.varint()?;
    let version = u32::try_from(reader.varint()?).map_err(|_| CompactError("version"))?;
    let sequence = reader.take(4)?;
    let packet_sequence = u32::from_be_bytes(sequence.try_into().expect("four bytes"));
    let esi = u32::try_from(reader.varint()?).map_err(|_| CompactError("esi"))?;
    let slim = reader.rest();
    if slim.is_empty() {
        return Err(CompactError("no slim object"));
    }
    Ok((
        CompactHeader {
            generation,
            subscription_id,
            deadline_us,
            role,
            kind,
            path,
            tenant,
            stream,
            track,
            epoch,
            group,
            object,
            version,
            packet_sequence,
            esi,
        },
        slim,
    ))
}

// ---------------------------------------------------------------- sender

/// What a sender does with one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactDecision {
    /// Send the whole object (it carries, or changes, the descriptor).
    Whole,
    /// Send this slim object.
    Slim(Vec<u8>),
}

#[derive(Debug, Clone, Copy)]
struct SentDescriptor {
    id: u32,
    whole_at_us: u64,
}

/// Counts of a [`CompactSender`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CompactSenderCounts {
    pub slim: u64,
    pub whole: u64,
    /// Parts whose rebuilt envelope did not match (sent whole).
    pub mismatched: u64,
    /// Parts that are never sent compact.
    pub ineligible: u64,
}

/// The sender's side: which parts go whole (to carry the descriptor) and
/// which go slim. A stream's part goes whole when its shape is new or
/// changed, and once every `whole_every_us` after that, so that a receiver
/// that joins (or lost a whole part) learns the descriptor within it.
#[derive(Debug)]
pub struct CompactSender {
    whole_every_us: u64,
    streams: HashMap<String, SentDescriptor>,
    counts: CompactSenderCounts,
}

impl CompactSender {
    #[must_use]
    pub fn new(whole_every_us: u64) -> Self {
        Self {
            whole_every_us,
            streams: HashMap::new(),
            counts: CompactSenderCounts::default(),
        }
    }

    #[must_use]
    pub const fn counts(&self) -> CompactSenderCounts {
        self.counts
    }

    /// Decide how to send `object`, whose canonical envelope is `envelope`
    /// (`media_object::encode(object)`), at `now_us`. A slim object is
    /// given only when the object rebuilt from it encodes to `envelope`
    /// byte for byte.
    pub fn decide(
        &mut self,
        object: &MediaObject,
        envelope: &[u8],
        now_us: u64,
    ) -> CompactDecision {
        let Some(id) = descriptor_id(object) else {
            self.counts.ineligible += 1;
            return CompactDecision::Whole;
        };
        let stream = object.key().stream();
        let due = match self.streams.get(stream) {
            Some(sent) => {
                sent.id != id || now_us.saturating_sub(sent.whole_at_us) >= self.whole_every_us
            }
            None => true,
        };
        if due {
            self.streams.insert(
                stream.to_owned(),
                SentDescriptor {
                    id,
                    whole_at_us: now_us,
                },
            );
            self.counts.whole += 1;
            return CompactDecision::Whole;
        }
        let Some(slim) = encode_slim(id, object) else {
            self.counts.ineligible += 1;
            return CompactDecision::Whole;
        };
        // The object is its own template here: a receiver's template has
        // the same shape (the id says so), so it rebuilds the same bytes.
        let same = expand_slim(object, &slim)
            .ok()
            .and_then(|rebuilt| media_object::encode(&rebuilt).ok())
            .is_some_and(|rebuilt| rebuilt == envelope);
        if !same {
            self.counts.mismatched += 1;
            self.counts.whole += 1;
            return CompactDecision::Whole;
        }
        self.counts.slim += 1;
        CompactDecision::Slim(slim)
    }

    /// Forget a stream (it ended): its next part goes whole.
    pub fn forget(&mut self, stream: &str) {
        self.streams.remove(stream);
    }
}

// -------------------------------------------------------------- receiver

/// How many descriptors a receiver keeps per stream.
pub const TEMPLATES_PER_STREAM: usize = 4;
/// How many streams a receiver keeps descriptors for.
pub const MAX_TEMPLATE_STREAMS: usize = 4_096;

/// The receiver's side: the descriptors it learnt, by stream, each with the
/// whole object it learnt it from.
#[derive(Debug, Default)]
pub struct CompactTemplates {
    streams: HashMap<String, VecDeque<(u32, Arc<MediaObject>)>>,
    order: VecDeque<String>,
}

impl CompactTemplates {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Learn the descriptor of a whole object. Returns its id, or `None`
    /// when the object is never sent compact.
    pub fn learn(&mut self, object: &MediaObject) -> Option<u32> {
        let id = descriptor_id(object)?;
        let stream = object.key().stream();
        if let Some(templates) = self.streams.get_mut(stream) {
            if let Some(at) = templates.iter().position(|(known, _)| *known == id) {
                if at != 0 {
                    let entry = templates.remove(at).expect("present");
                    templates.push_front(entry);
                }
                return Some(id);
            }
            templates.push_front((id, Arc::new(object.clone())));
            templates.truncate(TEMPLATES_PER_STREAM);
            return Some(id);
        }
        if self.streams.len() >= MAX_TEMPLATE_STREAMS {
            if let Some(oldest) = self.order.pop_front() {
                self.streams.remove(&oldest);
            }
        }
        self.order.push_back(stream.to_owned());
        self.streams.insert(
            stream.to_owned(),
            VecDeque::from([(id, Arc::new(object.clone()))]),
        );
        Some(id)
    }

    /// The whole object of a descriptor of `stream`.
    #[must_use]
    pub fn template(&self, stream: &str, id: u32) -> Option<&Arc<MediaObject>> {
        self.streams
            .get(stream)?
            .iter()
            .find(|(known, _)| *known == id)
            .map(|(_, template)| template)
    }

    /// Whether a descriptor of the stream is known.
    #[must_use]
    pub fn knows(&self, stream: &str, id: u32) -> bool {
        self.template(stream, id).is_some()
    }

    /// Rebuild the object of a slim object of `stream`.
    ///
    /// # Errors
    ///
    /// `unknown descriptor` when no whole object of its descriptor has
    /// arrived yet; otherwise those of [`expand_slim`].
    pub fn expand(&self, stream: &str, slim: &[u8]) -> Result<MediaObject> {
        let view = SlimView::parse(slim)?;
        let template = self
            .template(stream, view.descriptor)
            .ok_or(CompactError("unknown descriptor"))?;
        expand_view(template, &view)
    }

    /// Streams with a descriptor.
    #[must_use]
    pub fn streams(&self) -> usize {
        self.streams.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLOCK: &str = "system:realtime:av-contrib";

    fn stamp(ns: i64) -> ClockTimestamp {
        ClockTimestamp::new(ns, CLOCK, ClockConfidence::estimated(5_000_000)).unwrap()
    }

    /// An opaque 1 ms audio part as the contributor builds it.
    fn part(stream: u64, sequence: u64, payload: &[u8], published_ns: i64) -> MediaObject {
        let key = ObjectKey::for_payload(
            "default",
            stream.to_string(),
            "opaque-audio",
            1_763_812_345_678,
            0,
            sequence,
            1,
            payload,
        )
        .unwrap();
        let msn = sequence / 1_000;
        let part = sequence % 1_000;
        MediaObject::builder(key, ObjectKind::Media, payload.to_vec())
            .with_deadline(stamp(published_ns + 2_000_000_000))
            .with_stage_timestamp(StageTimestamp::new(
                Stage::Packaged,
                stamp(published_ns - 31_250),
            ))
            .with_stage_timestamp(StageTimestamp::new(Stage::Published, stamp(published_ns)))
            .with_metadata("container", b"opaque".to_vec())
            .with_metadata("duration-ms", b"1".to_vec())
            .with_metadata("duration-us", b"1000".to_vec())
            .with_metadata("duration-samples", b"48".to_vec())
            .with_metadata("sample-rate", b"48000".to_vec())
            .with_metadata("payload-format", b"opaque-part-v1".to_vec())
            .with_metadata("scheduler-class", b"audio".to_vec())
            .with_metadata("track-composition", b"audio".to_vec())
            .with_metadata("content-type", b"application/octet-stream".to_vec())
            .with_metadata("file-extension", b"bin".to_vec())
            .with_metadata("hls-msn", msn.to_string().into_bytes())
            .with_metadata("hls-part", part.to_string().into_bytes())
            .with_metadata("hls-part-target-ms", b"1".to_vec())
            .with_metadata("hls-part-target-us", b"1000".to_vec())
            .with_metadata("hls-target-duration-ms", b"1000".to_vec())
            .with_metadata("hls-target-duration-us", b"1000000".to_vec())
            .build()
            .unwrap()
    }

    #[test]
    fn a_slim_part_rebuilds_the_whole_envelope_byte_for_byte() {
        let template = part(3001, 41_999, &[7; 56], 1_763_812_345_000_000_000);
        let payload: Vec<u8> = (0..56).collect();
        // A later part: another segment and part number (other lengths of
        // their decimal values), other timestamps and payload.
        let later = part(3001, 1_000_000, &payload, 1_763_812_346_234_567_891);
        let id = descriptor_id(&template).unwrap();
        assert_eq!(descriptor_id(&later), Some(id));
        let slim = encode_slim(id, &later).unwrap();
        let rebuilt = expand_slim(&template, &slim).unwrap();
        assert_eq!(
            media_object::encode(&rebuilt).unwrap(),
            media_object::encode(&later).unwrap()
        );
        let whole = media_object::encode(&later).unwrap().len();
        assert!(slim.len() <= payload.len() + 36, "slim {} B", slim.len());
        assert!(
            whole > slim.len() + 600,
            "whole {whole} B, slim {} B",
            slim.len()
        );
        let view = SlimView::parse(&slim).unwrap();
        assert_eq!(view.object, 1_000_000);
        assert_eq!(view.payload, payload.as_slice());
        assert_eq!(view.published_unix_ns(), Some(1_763_812_346_234_567_891));
    }

    #[test]
    fn a_change_of_shape_is_a_new_descriptor() {
        let one = part(3001, 5, &[1; 56], 1_000_000_000);
        let other_stream = part(4001, 5, &[1; 56], 1_000_000_000);
        assert_ne!(descriptor_id(&one), descriptor_id(&other_stream));
        let key = one.key().clone();
        let longer = MediaObject::builder(key, ObjectKind::Media, one.payload().to_vec())
            .with_deadline(one.deadline().unwrap().clone())
            .with_metadata("hls-msn", b"0".to_vec())
            .with_metadata("duration-us", b"2500".to_vec())
            .build()
            .unwrap();
        let shorter =
            MediaObject::builder(one.key().clone(), ObjectKind::Media, one.payload().to_vec())
                .with_deadline(one.deadline().unwrap().clone())
                .with_metadata("hls-msn", b"0".to_vec())
                .with_metadata("duration-us", b"1000".to_vec())
                .build()
                .unwrap();
        assert_ne!(descriptor_id(&longer), descriptor_id(&shorter));
        // A varying value that is not a plain number is never compact.
        let odd =
            MediaObject::builder(one.key().clone(), ObjectKind::Media, one.payload().to_vec())
                .with_metadata("hls-msn", b"007".to_vec())
                .build()
                .unwrap();
        assert_eq!(descriptor_id(&odd), None);
    }

    #[test]
    fn a_compact_datagram_reads_back() {
        let header = CompactHeader {
            generation: 3,
            subscription_id: 0x1234_5678_9abc,
            deadline_us: 1_763_812_347_000_000,
            role: 1,
            kind: 3,
            path: 1,
            tenant: "default",
            stream: "3001",
            track: "opaque-audio",
            epoch: 1_763_812_345_678,
            group: 0,
            object: 1_000_000,
            version: 1,
            packet_sequence: 0xfedc_ba98,
            esi: 1,
        };
        let datagram = encode_datagram(&header, b"\x01slim").unwrap();
        assert!(is_compact_datagram(&datagram));
        assert!(datagram.len() <= 50, "{} B", datagram.len());
        let (read, slim) = parse_datagram(&datagram).unwrap();
        assert_eq!(read, header);
        assert_eq!(slim, b"\x01slim");
        assert!(parse_datagram(&datagram[..20]).is_err());
        let unknown_track = CompactHeader {
            track: "video",
            ..header
        };
        assert_eq!(encode_header(&unknown_track), None);
    }

    #[test]
    fn the_sender_sends_the_descriptor_whole_then_slim_and_again_on_change() {
        let mut sender = CompactSender::new(100_000);
        let mut templates = CompactTemplates::new();
        let mut now = 0_u64;
        let mut slim = 0;
        for sequence in 0..250_u64 {
            let object = part(
                3001,
                sequence,
                &[sequence as u8; 56],
                1_000_000 * sequence as i64,
            );
            let envelope = media_object::encode(&object).unwrap();
            match sender.decide(&object, &envelope, now) {
                CompactDecision::Whole => {
                    templates.learn(&object).unwrap();
                }
                CompactDecision::Slim(bytes) => {
                    slim += 1;
                    let rebuilt = templates.expand("3001", &bytes).unwrap();
                    assert_eq!(media_object::encode(&rebuilt).unwrap(), envelope);
                }
            }
            now += 1_000;
        }
        // Whole at 0, 100 and 200 ms.
        assert_eq!(sender.counts().whole, 3);
        assert_eq!(slim, 247);
        // A receiver that joined late knows no descriptor until a whole
        // part.
        let late = CompactTemplates::new();
        let object = part(3001, 999, &[9; 56], 999_000_000);
        let id = descriptor_id(&object).unwrap();
        let bytes = encode_slim(id, &object).unwrap();
        assert_eq!(
            late.expand("3001", &bytes).unwrap_err(),
            CompactError("unknown descriptor")
        );
    }
}
