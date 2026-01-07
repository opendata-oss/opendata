//! Terminated bytes serialization for lexicographically ordered keys.
//!
//! This module provides encoding and decoding for variable-length byte sequences
//! that preserves lexicographic ordering. Keys are escaped and terminated with `0x00`:
//!
//! - `0x00` → `0x01 0x01`
//! - `0x01` → `0x01 0x02`
//! - `0xFF` → `0x01 0x03`
//! - All other bytes unchanged
//! - Terminated with `0x00` delimiter
//!
//! Using `0x00` as the terminator ensures shorter keys sort before longer
//! keys with the same prefix (e.g., "/foo" < "/foo/bar"). This simplifies
//! prefix-based range queries: start at `prefix + 0x00`, end at `prefix + 0xFF`.

use bytes::{BufMut, Bytes, BytesMut};

use super::DeserializeError;

/// Terminator byte for terminated bytes encoding (lowest byte value)
const TERMINATOR_BYTE: u8 = 0x00;

/// Escape character for terminated bytes encoding
const ESCAPE_BYTE: u8 = 0x01;

/// Reserved byte for range query upper bounds (highest byte value)
const RANGE_END_BYTE: u8 = 0xFF;

/// Serializes raw bytes with escape sequences and `0x00` terminator.
///
/// Writes directly to the provided buffer. The encoding:
/// - `0x00` → `0x01 0x01`
/// - `0x01` → `0x01 0x02`
/// - `0xFF` → `0x01 0x03`
/// - All other bytes unchanged
/// - Terminated with `0x00`
///
/// Using `0x00` as the terminator ensures shorter keys sort before longer
/// keys with the same prefix, enabling simple prefix-based range queries.
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
            RANGE_END_BYTE => {
                buf.put_u8(ESCAPE_BYTE);
                buf.put_u8(0x03);
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
                0x03 => result.put_u8(RANGE_END_BYTE),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_serialize_and_deserialize_simple_bytes() {
        // given
        let data = b"hello";
        let mut buf = BytesMut::new();

        // when
        serialize(data, &mut buf);
        let mut slice = buf.as_ref();
        let decoded = deserialize(&mut slice).unwrap();

        // then
        assert_eq!(decoded.as_ref(), b"hello");
        assert!(slice.is_empty());
    }

    #[test]
    fn should_serialize_and_deserialize_bytes_with_escape_char() {
        // given - data containing 0x01 (escape char)
        let data = &[0x61, 0x01, 0x62]; // "a" + 0x01 + "b"
        let mut buf = BytesMut::new();

        // when
        serialize(data, &mut buf);

        // then - should be escaped as 0x01 0x02
        assert_eq!(buf.as_ref(), &[0x61, 0x01, 0x02, 0x62, 0x00]);

        // when - deserialize
        let mut slice = buf.as_ref();
        let decoded = deserialize(&mut slice).unwrap();

        // then
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn should_serialize_and_deserialize_bytes_with_terminator_char() {
        // given - data containing 0x00 (terminator char)
        let data = &[0x61, 0x00, 0x62]; // "a" + 0x00 + "b"
        let mut buf = BytesMut::new();

        // when
        serialize(data, &mut buf);

        // then - should be escaped as 0x01 0x01
        assert_eq!(buf.as_ref(), &[0x61, 0x01, 0x01, 0x62, 0x00]);

        // when - deserialize
        let mut slice = buf.as_ref();
        let decoded = deserialize(&mut slice).unwrap();

        // then
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn should_serialize_and_deserialize_bytes_with_all_special_chars() {
        // given - data with all special chars: 0x00, 0x01, 0xFF
        let data = &[0x00, 0x01, 0xFF, 0x00, 0x01, 0xFF];
        let mut buf = BytesMut::new();

        // when
        serialize(data, &mut buf);
        let mut slice = buf.as_ref();
        let decoded = deserialize(&mut slice).unwrap();

        // then
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn should_serialize_empty_bytes() {
        // given
        let data: &[u8] = &[];
        let mut buf = BytesMut::new();

        // when
        serialize(data, &mut buf);

        // then - just the terminator
        assert_eq!(buf.as_ref(), &[0x00]);

        // when - deserialize
        let mut slice = buf.as_ref();
        let decoded = deserialize(&mut slice).unwrap();

        // then
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_fail_deserialize_without_terminator() {
        // given - no terminator
        let data = &[0x61, 0x62, 0x63];

        // when
        let mut slice = &data[..];
        let result = deserialize(&mut slice);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message
                .contains("missing 0x00 terminator")
        );
    }

    #[test]
    fn should_fail_deserialize_with_truncated_escape() {
        // given - escape at end without following byte
        let data = &[0x61, 0x01];

        // when
        let mut slice = &data[..];
        let result = deserialize(&mut slice);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message
                .contains("truncated escape sequence")
        );
    }

    #[test]
    fn should_fail_deserialize_with_invalid_escape_sequence() {
        // given - invalid escape (0x01 followed by 0x04, which is not valid)
        let data = &[0x61, 0x01, 0x04, 0x00];

        // when
        let mut slice = &data[..];
        let result = deserialize(&mut slice);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message
                .contains("invalid escape sequence")
        );
    }

    #[test]
    fn should_preserve_ordering_for_non_prefix_keys() {
        // given - keys where neither is a prefix of the other
        let pairs = [
            (b"apple".as_slice(), b"banana".as_slice()),
            (b"cat", b"dog"),
            (b"x", b"y"),
        ];

        for (key_a, key_b) in pairs {
            // when
            let mut buf_a = BytesMut::new();
            serialize(key_a, &mut buf_a);
            let mut buf_b = BytesMut::new();
            serialize(key_b, &mut buf_b);

            // then - lexicographic ordering preserved
            assert!(
                buf_a.as_ref() < buf_b.as_ref(),
                "Expected {:?} < {:?} after serialization",
                key_a,
                key_b
            );
        }
    }

    #[test]
    fn should_group_prefix_keys_contiguously() {
        // given - keys where one is a prefix of another
        let mut buf_a = BytesMut::new();
        serialize(b"a", &mut buf_a);
        let mut buf_ab = BytesMut::new();
        serialize(b"ab", &mut buf_ab);
        let mut buf_abc = BytesMut::new();
        serialize(b"abc", &mut buf_abc);

        // then - shorter prefixes sort before longer ones
        assert!(buf_a.as_ref() < buf_ab.as_ref());
        assert!(buf_ab.as_ref() < buf_abc.as_ref());
    }

    #[test]
    fn should_not_allow_key_prefix_collision() {
        // given - key "a" should not be a prefix of key "ab" after serialization
        let mut buf_a = BytesMut::new();
        serialize(b"a", &mut buf_a);
        let mut buf_ab = BytesMut::new();
        serialize(b"ab", &mut buf_ab);

        // then - "a" serialization should not be a prefix of "ab" serialization
        assert!(!buf_ab.starts_with(&buf_a));
    }
}
