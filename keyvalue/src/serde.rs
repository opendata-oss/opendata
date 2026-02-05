//! Key encoding and decoding for KeyValue storage.
//!
//! Implements RFC 0001: KeyValue Storage key encoding.
//!
//! ```text
//! SlateDB Key: | version (u8) | record_tag (u8) | user_key (bytes) |
//! ```

use std::ops::{Bound, RangeBounds};

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;

use crate::error::{Error, Result};

/// Key format version.
pub const KEY_VERSION: u8 = 0x01;

/// Record tag: type 0x1 in high 4 bits, reserved 0x0 in low 4 bits.
pub const RECORD_TAG: u8 = 0x10;

/// The next record tag (for range upper bounds).
const NEXT_RECORD_TAG: u8 = 0x11;

/// Encodes a user key by prepending the 2-byte prefix.
pub fn encode_key(user_key: &Bytes) -> Bytes {
    let mut buf = BytesMut::with_capacity(2 + user_key.len());
    buf.put_u8(KEY_VERSION);
    buf.put_u8(RECORD_TAG);
    buf.extend_from_slice(user_key);
    buf.freeze()
}

/// Decodes a storage key by stripping and validating the 2-byte prefix.
///
/// Returns the user key portion.
pub fn decode_key(storage_key: &[u8]) -> Result<Bytes> {
    if storage_key.len() < 2 {
        return Err(Error::Encoding(format!(
            "key too short: expected at least 2 bytes, got {}",
            storage_key.len()
        )));
    }

    if storage_key[0] != KEY_VERSION {
        return Err(Error::Encoding(format!(
            "invalid key version: expected 0x{:02x}, got 0x{:02x}",
            KEY_VERSION, storage_key[0]
        )));
    }

    if storage_key[1] != RECORD_TAG {
        return Err(Error::Encoding(format!(
            "invalid record tag: expected 0x{:02x}, got 0x{:02x}",
            RECORD_TAG, storage_key[1]
        )));
    }

    Ok(Bytes::copy_from_slice(&storage_key[2..]))
}

/// Transforms a user key range to a storage key range.
///
/// This adds the 2-byte prefix to the range bounds so that scans
/// only see keys with our record type.
pub fn encode_key_range(user_range: impl RangeBounds<Bytes>) -> BytesRange {
    let start = match user_range.start_bound() {
        Bound::Included(key) => Bound::Included(encode_key(key)),
        Bound::Excluded(key) => Bound::Excluded(encode_key(key)),
        Bound::Unbounded => {
            // Start from the beginning of our record type
            let prefix = Bytes::from_static(&[KEY_VERSION, RECORD_TAG]);
            Bound::Included(prefix)
        }
    };

    let end = match user_range.end_bound() {
        Bound::Included(key) => Bound::Included(encode_key(key)),
        Bound::Excluded(key) => Bound::Excluded(encode_key(key)),
        Bound::Unbounded => {
            // End at the next record type (exclusive)
            let prefix = Bytes::from_static(&[KEY_VERSION, NEXT_RECORD_TAG]);
            Bound::Excluded(prefix)
        }
    };

    BytesRange::new(start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_key_with_prefix() {
        // given
        let user_key = Bytes::from("my-key");

        // when
        let encoded = encode_key(&user_key);

        // then
        assert_eq!(encoded.len(), 8); // 2 prefix + 6 key
        assert_eq!(encoded[0], KEY_VERSION);
        assert_eq!(encoded[1], RECORD_TAG);
        assert_eq!(&encoded[2..], b"my-key");
    }

    #[test]
    fn should_encode_empty_key() {
        // given
        let user_key = Bytes::new();

        // when
        let encoded = encode_key(&user_key);

        // then
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0], KEY_VERSION);
        assert_eq!(encoded[1], RECORD_TAG);
    }

    #[test]
    fn should_decode_key_stripping_prefix() {
        // given
        let storage_key = vec![KEY_VERSION, RECORD_TAG, b'm', b'y', b'-', b'k', b'e', b'y'];

        // when
        let user_key = decode_key(&storage_key).unwrap();

        // then
        assert_eq!(user_key, Bytes::from("my-key"));
    }

    #[test]
    fn should_decode_empty_user_key() {
        // given
        let storage_key = vec![KEY_VERSION, RECORD_TAG];

        // when
        let user_key = decode_key(&storage_key).unwrap();

        // then
        assert!(user_key.is_empty());
    }

    #[test]
    fn should_roundtrip_encode_decode() {
        // given
        let original = Bytes::from("test-key-123");

        // when
        let encoded = encode_key(&original);
        let decoded = decode_key(&encoded).unwrap();

        // then
        assert_eq!(decoded, original);
    }

    #[test]
    fn should_reject_short_key() {
        // given
        let short_key = vec![KEY_VERSION];

        // when
        let result = decode_key(&short_key);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn should_reject_wrong_version() {
        // given
        let bad_key = vec![0x99, RECORD_TAG, b'k', b'e', b'y'];

        // when
        let result = decode_key(&bad_key);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid key version")
        );
    }

    #[test]
    fn should_reject_wrong_record_tag() {
        // given
        let bad_key = vec![KEY_VERSION, 0x99, b'k', b'e', b'y'];

        // when
        let result = decode_key(&bad_key);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid record tag")
        );
    }

    #[test]
    fn should_encode_bounded_range() {
        // given
        let start = Bytes::from("a");
        let end = Bytes::from("z");

        // when
        let range = encode_key_range(start.clone()..end.clone());

        // then
        match range.start_bound() {
            Bound::Included(k) => {
                assert_eq!(k[0], KEY_VERSION);
                assert_eq!(k[1], RECORD_TAG);
                assert_eq!(&k[2..], b"a");
            }
            _ => panic!("expected Included start bound"),
        }
        match range.end_bound() {
            Bound::Excluded(k) => {
                assert_eq!(k[0], KEY_VERSION);
                assert_eq!(k[1], RECORD_TAG);
                assert_eq!(&k[2..], b"z");
            }
            _ => panic!("expected Excluded end bound"),
        }
    }

    #[test]
    fn should_encode_unbounded_range() {
        // when
        let range = encode_key_range(..);

        // then
        match range.start_bound() {
            Bound::Included(k) => {
                assert_eq!(k.len(), 2);
                assert_eq!(k[0], KEY_VERSION);
                assert_eq!(k[1], RECORD_TAG);
            }
            _ => panic!("expected Included start bound"),
        }
        match range.end_bound() {
            Bound::Excluded(k) => {
                assert_eq!(k.len(), 2);
                assert_eq!(k[0], KEY_VERSION);
                assert_eq!(k[1], NEXT_RECORD_TAG);
            }
            _ => panic!("expected Excluded end bound"),
        }
    }

    #[test]
    fn should_encode_half_open_range_from_start() {
        // given
        let end = Bytes::from("middle");

        // when
        let range = encode_key_range(..end);

        // then
        match range.start_bound() {
            Bound::Included(k) => {
                assert_eq!(k.len(), 2);
                assert_eq!(k[0], KEY_VERSION);
                assert_eq!(k[1], RECORD_TAG);
            }
            _ => panic!("expected Included start bound"),
        }
        match range.end_bound() {
            Bound::Excluded(k) => {
                assert_eq!(&k[2..], b"middle");
            }
            _ => panic!("expected Excluded end bound"),
        }
    }

    #[test]
    fn should_encode_half_open_range_to_end() {
        // given
        let start = Bytes::from("middle");

        // when
        let range = encode_key_range(start..);

        // then
        match range.start_bound() {
            Bound::Included(k) => {
                assert_eq!(&k[2..], b"middle");
            }
            _ => panic!("expected Included start bound"),
        }
        match range.end_bound() {
            Bound::Excluded(k) => {
                assert_eq!(k.len(), 2);
                assert_eq!(k[0], KEY_VERSION);
                assert_eq!(k[1], NEXT_RECORD_TAG);
            }
            _ => panic!("expected Excluded end bound"),
        }
    }
}
