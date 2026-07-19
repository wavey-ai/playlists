use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashSet;
use thiserror::Error;

pub const TAIL_BUNDLE_CONTENT_TYPE: &str = "application/vnd.needletail.llhls-tail-bundle";
pub const TAIL_BUNDLE_MAGIC: [u8; 4] = *b"NTB1";
pub const MAX_TAIL_BUNDLE_ENTRIES: usize = 128;
pub const MAX_TAIL_BUNDLE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

const BUNDLE_HEADER_BYTES: usize = 8;
const ENTRY_HEADER_BYTES: usize = 36;

/// One opaque LL-HLS stream range carried in a synchronized tail response.
///
/// Payload bytes are never interpreted here. They may contain one 5 ms media
/// unit or several self-delimiting units aggregated over time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TailBundleEntry {
    pub stream_id: u64,
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub available_unix_us: Option<u64>,
    pub payload: Bytes,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TailBundleError {
    #[error("tail bundle must contain at least one stream")]
    Empty,
    #[error("tail bundle contains too many streams")]
    TooManyEntries,
    #[error("tail bundle contains a duplicate stream id")]
    DuplicateStream,
    #[error("tail bundle sequence range is invalid")]
    InvalidSequenceRange,
    #[error("tail bundle stream payload is empty")]
    EmptyPayload,
    #[error("tail bundle payload exceeds its limit")]
    PayloadTooLarge,
    #[error("tail bundle length overflow")]
    LengthOverflow,
    #[error("tail bundle magic is invalid")]
    InvalidMagic,
    #[error("tail bundle reserved field is nonzero")]
    NonzeroReserved,
    #[error("tail bundle is truncated")]
    Truncated,
    #[error("tail bundle has trailing bytes")]
    TrailingBytes,
}

pub fn encode_tail_bundle(entries: &[TailBundleEntry]) -> Result<Bytes, TailBundleError> {
    validate_entry_count(entries.len())?;
    let mut streams = HashSet::with_capacity(entries.len());
    let mut payload_bytes = 0_usize;
    for entry in entries {
        validate_entry(entry, &mut streams)?;
        payload_bytes = payload_bytes
            .checked_add(entry.payload.len())
            .ok_or(TailBundleError::LengthOverflow)?;
        if payload_bytes > MAX_TAIL_BUNDLE_PAYLOAD_BYTES {
            return Err(TailBundleError::PayloadTooLarge);
        }
    }

    let header_bytes = ENTRY_HEADER_BYTES
        .checked_mul(entries.len())
        .and_then(|length| length.checked_add(BUNDLE_HEADER_BYTES))
        .ok_or(TailBundleError::LengthOverflow)?;
    let encoded_len = header_bytes
        .checked_add(payload_bytes)
        .ok_or(TailBundleError::LengthOverflow)?;
    let entry_count = u16::try_from(entries.len()).map_err(|_| TailBundleError::TooManyEntries)?;
    let mut encoded = BytesMut::with_capacity(encoded_len);
    encoded.extend_from_slice(&TAIL_BUNDLE_MAGIC);
    encoded.put_u16(entry_count);
    encoded.put_u16(0);
    for entry in entries {
        let payload_len =
            u32::try_from(entry.payload.len()).map_err(|_| TailBundleError::PayloadTooLarge)?;
        encoded.put_u64(entry.stream_id);
        encoded.put_u64(entry.start_sequence);
        encoded.put_u64(entry.end_sequence);
        encoded.put_u64(entry.available_unix_us.unwrap_or(0));
        encoded.put_u32(payload_len);
        encoded.extend_from_slice(&entry.payload);
    }
    debug_assert_eq!(encoded.len(), encoded_len);
    Ok(encoded.freeze())
}

pub fn decode_tail_bundle(encoded: Bytes) -> Result<Vec<TailBundleEntry>, TailBundleError> {
    if encoded.len() < BUNDLE_HEADER_BYTES {
        return Err(TailBundleError::Truncated);
    }
    if encoded[..4] != TAIL_BUNDLE_MAGIC {
        return Err(TailBundleError::InvalidMagic);
    }
    let entry_count = u16::from_be_bytes([encoded[4], encoded[5]]) as usize;
    validate_entry_count(entry_count)?;
    if u16::from_be_bytes([encoded[6], encoded[7]]) != 0 {
        return Err(TailBundleError::NonzeroReserved);
    }

    let minimum_len = ENTRY_HEADER_BYTES
        .checked_mul(entry_count)
        .and_then(|length| length.checked_add(BUNDLE_HEADER_BYTES))
        .ok_or(TailBundleError::LengthOverflow)?;
    if encoded.len() < minimum_len {
        return Err(TailBundleError::Truncated);
    }

    let mut cursor = BUNDLE_HEADER_BYTES;
    let mut entries = Vec::with_capacity(entry_count);
    let mut streams = HashSet::with_capacity(entry_count);
    let mut payload_bytes = 0_usize;
    for _ in 0..entry_count {
        let stream_id = take_u64(&encoded, &mut cursor)?;
        let start_sequence = take_u64(&encoded, &mut cursor)?;
        let end_sequence = take_u64(&encoded, &mut cursor)?;
        let available_unix_us = match take_u64(&encoded, &mut cursor)? {
            0 => None,
            value => Some(value),
        };
        let payload_len = take_u32(&encoded, &mut cursor)? as usize;
        let payload_end = cursor
            .checked_add(payload_len)
            .ok_or(TailBundleError::LengthOverflow)?;
        if payload_end > encoded.len() {
            return Err(TailBundleError::Truncated);
        }
        payload_bytes = payload_bytes
            .checked_add(payload_len)
            .ok_or(TailBundleError::LengthOverflow)?;
        if payload_bytes > MAX_TAIL_BUNDLE_PAYLOAD_BYTES {
            return Err(TailBundleError::PayloadTooLarge);
        }
        let entry = TailBundleEntry {
            stream_id,
            start_sequence,
            end_sequence,
            available_unix_us,
            payload: encoded.slice(cursor..payload_end),
        };
        validate_entry(&entry, &mut streams)?;
        entries.push(entry);
        cursor = payload_end;
    }
    if cursor != encoded.len() {
        return Err(TailBundleError::TrailingBytes);
    }
    Ok(entries)
}

fn validate_entry_count(entry_count: usize) -> Result<(), TailBundleError> {
    if entry_count == 0 {
        return Err(TailBundleError::Empty);
    }
    if entry_count > MAX_TAIL_BUNDLE_ENTRIES {
        return Err(TailBundleError::TooManyEntries);
    }
    Ok(())
}

fn validate_entry(
    entry: &TailBundleEntry,
    streams: &mut HashSet<u64>,
) -> Result<(), TailBundleError> {
    if !streams.insert(entry.stream_id) {
        return Err(TailBundleError::DuplicateStream);
    }
    if entry.start_sequence > entry.end_sequence {
        return Err(TailBundleError::InvalidSequenceRange);
    }
    if entry.payload.is_empty() {
        return Err(TailBundleError::EmptyPayload);
    }
    if entry.payload.len() > MAX_TAIL_BUNDLE_PAYLOAD_BYTES {
        return Err(TailBundleError::PayloadTooLarge);
    }
    Ok(())
}

fn take_u64(encoded: &[u8], cursor: &mut usize) -> Result<u64, TailBundleError> {
    let end = cursor
        .checked_add(8)
        .ok_or(TailBundleError::LengthOverflow)?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or(TailBundleError::Truncated)?;
    *cursor = end;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("eight-byte slice"),
    ))
}

fn take_u32(encoded: &[u8], cursor: &mut usize) -> Result<u32, TailBundleError> {
    let end = cursor
        .checked_add(4)
        .ok_or(TailBundleError::LengthOverflow)?;
    let bytes = encoded
        .get(*cursor..end)
        .ok_or(TailBundleError::Truncated)?;
    *cursor = end;
    Ok(u32::from_be_bytes(
        bytes.try_into().expect("four-byte slice"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(stream_id: u64, sequence: u64, payload: &'static [u8]) -> TailBundleEntry {
        TailBundleEntry {
            stream_id,
            start_sequence: sequence,
            end_sequence: sequence,
            available_unix_us: Some(1_000_000 + sequence),
            payload: Bytes::from_static(payload),
        }
    }

    #[test]
    fn opaque_stream_entries_round_trip_without_copying_payloads() {
        let entries = vec![entry(11, 7, b"opus-a"), entry(12, 7, b"anything-b")];
        let encoded = encode_tail_bundle(&entries).unwrap();
        let decoded = decode_tail_bundle(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn decoder_rejects_truncation_trailing_bytes_and_reserved_bits() {
        let encoded = encode_tail_bundle(&[entry(11, 7, b"opus")]).unwrap();
        assert_eq!(
            decode_tail_bundle(encoded.slice(..encoded.len() - 1)),
            Err(TailBundleError::Truncated)
        );
        let mut trailing = BytesMut::from(encoded.as_ref());
        trailing.put_u8(0);
        assert_eq!(
            decode_tail_bundle(trailing.freeze()),
            Err(TailBundleError::TrailingBytes)
        );
        let mut reserved = BytesMut::from(encoded.as_ref());
        reserved[7] = 1;
        assert_eq!(
            decode_tail_bundle(reserved.freeze()),
            Err(TailBundleError::NonzeroReserved)
        );
    }

    #[test]
    fn encoder_and_decoder_reject_ambiguous_stream_sets() {
        let duplicate = vec![entry(11, 7, b"a"), entry(11, 7, b"b")];
        assert_eq!(
            encode_tail_bundle(&duplicate),
            Err(TailBundleError::DuplicateStream)
        );

        let mut encoded = encode_tail_bundle(&[entry(11, 7, b"a"), entry(12, 7, b"b")])
            .unwrap()
            .to_vec();
        let second_stream_offset = BUNDLE_HEADER_BYTES + ENTRY_HEADER_BYTES + 1;
        encoded[second_stream_offset..second_stream_offset + 8]
            .copy_from_slice(&11_u64.to_be_bytes());
        assert_eq!(
            decode_tail_bundle(Bytes::from(encoded)),
            Err(TailBundleError::DuplicateStream)
        );
    }

    #[test]
    fn invalid_counts_ranges_and_empty_payloads_are_rejected() {
        assert_eq!(encode_tail_bundle(&[]), Err(TailBundleError::Empty));
        let invalid_range = TailBundleEntry {
            start_sequence: 8,
            end_sequence: 7,
            ..entry(11, 7, b"opus")
        };
        assert_eq!(
            encode_tail_bundle(&[invalid_range]),
            Err(TailBundleError::InvalidSequenceRange)
        );
        let empty_payload = TailBundleEntry {
            payload: Bytes::new(),
            ..entry(11, 7, b"opus")
        };
        assert_eq!(
            encode_tail_bundle(&[empty_payload]),
            Err(TailBundleError::EmptyPayload)
        );
    }
}
