//! Terminated bytes serialization for lexicographically ordered keys.
//!
//! This module provides encoding and decoding for variable-length byte sequences
//! that preserves lexicographic ordering. Keys are escaped and terminated with `0x00`:
//!
//! - `0x00` → `0x01 0x01`
//! - `0x01` → `0x01 0x02`
//! - All other bytes unchanged
//! - Terminated with `0x00` delimiter
//!
//! Using `0x00` as the terminator ensures shorter keys sort before longer
//! keys with the same prefix (e.g., "/foo" < "/foo/bar").
//!
//! For prefix-based range queries, use [`crate::bytes::lex_increment`] to compute
//! the exclusive upper bound.

use bytes::{BufMut, Bytes, BytesMut};

use super::DeserializeError;
use crate::BytesRange;
use crate::bytes::lex_increment;
use std::ops::Bound::{Excluded, Included, Unbounded};

/// Terminator byte for terminated bytes encoding (lowest byte value)
const TERMINATOR_BYTE: u8 = 0x00;

/// Escape character for terminated bytes encoding
const ESCAPE_BYTE: u8 = 0x01;

/// Serializes raw bytes with escape sequences and `0x00` terminator.
///
/// Writes directly to the provided buffer. The encoding:
/// - `0x00` → `0x01 0x01`
/// - `0x01` → `0x01 0x02`
/// - All other bytes unchanged
/// - Terminated with `0x00`
///
/// This encoding preserves lexicographic ordering of the original byte sequences.
pub fn serialize_to_bytes(data: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    serialize(data, &mut buf);
    buf.freeze()
}

/// Serializes raw bytes with escape sequences and `0x00` terminator.
///
/// Writes directly to the provided buffer. The encoding:
/// - `0x00` → `0x01 0x01`
/// - `0x01` → `0x01 0x02`
/// - All other bytes unchanged
/// - Terminated with `0x00`
///
/// This encoding preserves lexicographic ordering of the original byte sequences.
pub fn serialize(data: &[u8], buf: &mut BytesMut) {
    for &byte in data {
        match byte {
            TERMINATOR_BYTE => {
                buf.put_u8(ESCAPE_BYTE);
                buf.put_u8(0x01);
            }
            ESCAPE_BYTE => {
                buf.put_u8(ESCAPE_BYTE);
                buf.put_u8(0x02);
            }
            _ => buf.put_u8(byte),
        }
    }
    buf.put_u8(TERMINATOR_BYTE);
}

/// Deserializes terminated bytes from a buffer, advancing past the terminator.
///
/// Returns the decoded raw bytes. The input buffer is advanced past the
/// terminator byte.
///
/// # Errors
///
/// Returns an error if:
/// - The buffer contains an incomplete escape sequence
/// - The buffer contains an invalid escape sequence
/// - The buffer is missing the terminator byte
pub fn deserialize(buf: &mut &[u8]) -> Result<Bytes, DeserializeError> {
    let mut result = BytesMut::new();
    let mut i = 0;

    while i < buf.len() {
        let byte = buf[i];

        if byte == TERMINATOR_BYTE {
            // Found terminator, consume it and return
            *buf = &buf[i + 1..];
            return Ok(result.freeze());
        }

        if byte == ESCAPE_BYTE {
            // Escape sequence - need next byte
            if i + 1 >= buf.len() {
                return Err(DeserializeError {
                    message: "truncated escape sequence in terminated bytes".to_string(),
                });
            }
            let next = buf[i + 1];
            match next {
                0x01 => result.put_u8(TERMINATOR_BYTE),
                0x02 => result.put_u8(ESCAPE_BYTE),
                _ => {
                    return Err(DeserializeError {
                        message: format!("invalid escape sequence: 0x01 0x{:02x}", next),
                    });
                }
            }
            i += 2;
        } else {
            result.put_u8(byte);
            i += 1;
        }
    }

    Err(DeserializeError {
        message: "unterminated bytes sequence (missing 0x00 terminator)".to_string(),
    })
}

/// Creates a [`BytesRange`] for scanning all keys with the given logical prefix.
///
/// The prefix is first incremented using [`lex_increment`], then both bounds
/// are serialized using [`serialize`].
///
/// # Examples
///
/// ```ignore
/// // Find all keys starting with "user:"
/// let range = prefix_range(b"user:");
/// storage.scan(range).await?;
/// ```
pub fn prefix_range(prefix: &[u8]) -> BytesRange {
    if prefix.is_empty() {
        BytesRange::unbounded()
    } else {
        let start = serialize_to_bytes(prefix);
        match lex_increment(prefix) {
            Some(end) => BytesRange::new(Included(start), Excluded(serialize_to_bytes(&end))),
            None => BytesRange::new(Included(start), Unbounded),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    // Property tests

    proptest! {
        #[test]
        fn should_roundtrip_any_bytes(data: Vec<u8>) {
            let mut buf = BytesMut::new();
            serialize(&data, &mut buf);

            let mut slice = buf.as_ref();
            let decoded = deserialize(&mut slice).unwrap();

            prop_assert_eq!(decoded.as_ref(), data.as_slice());
            prop_assert!(slice.is_empty());
        }

        #[test]
        fn should_preserve_ordering(a: Vec<u8>, b: Vec<u8>) {
            let mut buf_a = BytesMut::new();
            let mut buf_b = BytesMut::new();
            serialize(&a, &mut buf_a);
            serialize(&b, &mut buf_b);

            prop_assert_eq!(
                a.cmp(&b),
                buf_a.as_ref().cmp(buf_b.as_ref()),
                "ordering mismatch: a={:?}, b={:?}, enc_a={:?}, enc_b={:?}",
                a,
                b,
                buf_a.as_ref(),
                buf_b.as_ref()
            );
        }

        #[test]
        fn should_prefix_range_contain_all_prefixed_keys(prefix: Vec<u8>, suffix: Vec<u8>) {
            prop_assume!(!prefix.is_empty());

            let mut full_key = prefix.clone();
            full_key.extend(&suffix);

            let range = prefix_range(&prefix);
            let serialized = serialize_to_bytes(&full_key);

            prop_assert!(
                range.contains(&serialized),
                "prefix_range({:?}) should contain {:?} (serialized: {:?})",
                prefix,
                full_key,
                serialized
            );
        }
    }

    // Concrete encoding tests (for spec verification)

    #[test]
    fn should_encode_special_bytes_correctly() {
        // 0x00 -> 0x01 0x01
        let mut buf = BytesMut::new();
        serialize(&[0x00], &mut buf);
        assert_eq!(buf.as_ref(), &[0x01, 0x01, 0x00]);

        // 0x01 -> 0x01 0x02
        buf.clear();
        serialize(&[0x01], &mut buf);
        assert_eq!(buf.as_ref(), &[0x01, 0x02, 0x00]);

        // 0xFF -> unchanged (just 0xFF + terminator)
        buf.clear();
        serialize(&[0xFF], &mut buf);
        assert_eq!(buf.as_ref(), &[0xFF, 0x00]);

        // empty -> just terminator
        buf.clear();
        serialize(&[], &mut buf);
        assert_eq!(buf.as_ref(), &[0x00]);
    }

    // Error case tests

    #[test]
    fn should_fail_deserialize_without_terminator() {
        let mut slice: &[u8] = &[0x61, 0x62, 0x63];
        assert!(deserialize(&mut slice).is_err());
    }

    #[test]
    fn should_fail_deserialize_with_truncated_escape() {
        let mut slice: &[u8] = &[0x61, 0x01];
        assert!(deserialize(&mut slice).is_err());
    }

    #[test]
    fn should_fail_deserialize_with_invalid_escape_sequence() {
        let mut slice: &[u8] = &[0x61, 0x01, 0x03, 0x00];
        assert!(deserialize(&mut slice).is_err());
    }

    #[test]
    fn should_advance_buffer_past_consumed_bytes() {
        let data = &[0x61, 0x00, 0xDE, 0xAD]; // "a" + terminator + extra
        let mut slice = &data[..];

        let decoded = deserialize(&mut slice).unwrap();

        assert_eq!(decoded.as_ref(), b"a");
        assert_eq!(slice, &[0xDE, 0xAD]);
    }

    #[test]
    fn should_not_have_encoded_prefix_collision() {
        // Encoded "a" should not be a prefix of encoded "ab"
        let mut buf_a = BytesMut::new();
        serialize(b"a", &mut buf_a);
        let mut buf_ab = BytesMut::new();
        serialize(b"ab", &mut buf_ab);

        assert!(!buf_ab.starts_with(&buf_a));
    }
}
