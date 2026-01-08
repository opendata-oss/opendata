// Key structures with big-endian encoding

use super::*;
use crate::model::{BucketSize, BucketStart, RecordTag, SeriesFingerprint, SeriesId};
use bytes::{Bytes, BytesMut};
use common::BytesRange;

/// BucketList key (global-scoped)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketListKey;

impl BucketListKey {
    pub fn encode(&self) -> Bytes {
        Bytes::from(vec![
            KEY_VERSION,
            RecordTag::new_global_scoped(RecordType::BucketList).as_byte(),
        ])
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for BucketListKey".to_string(),
            });
        }
        if buf[0] != KEY_VERSION {
            return Err(EncodingError {
                message: format!(
                    "Invalid key version: expected 0x{:02x}, got 0x{:02x}",
                    KEY_VERSION, buf[0]
                ),
            });
        }
        let record_tag = RecordTag::from_byte(buf[1])?;
        if record_tag.record_type()? != RecordType::BucketList {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected BucketList, got {:?}",
                    record_tag.record_type()?
                ),
            });
        }
        if record_tag.bucket_size().is_some() {
            return Err(EncodingError {
                message: "BucketListKey should be global-scoped (bucket_size should be None)"
                    .to_string(),
            });
        }
        Ok(BucketListKey)
    }
}

/// SeriesDictionary key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesDictionaryKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub series_fingerprint: SeriesFingerprint,
}

impl SeriesDictionaryKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            RecordTag::new_bucket_scoped(RecordType::SeriesDictionary, self.bucket_size).as_byte(),
        ]);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        buf.extend_from_slice(&self.series_fingerprint.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 + 16 {
            return Err(EncodingError {
                message: "Buffer too short for SeriesDictionaryKey".to_string(),
            });
        }
        if buf[0] != KEY_VERSION {
            return Err(EncodingError {
                message: format!(
                    "Invalid key version: expected 0x{:02x}, got 0x{:02x}",
                    KEY_VERSION, buf[0]
                ),
            });
        }
        let record_tag = RecordTag::from_byte(buf[1])?;
        if record_tag.record_type()? != RecordType::SeriesDictionary {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected SeriesDictionary, got {:?}",
                    record_tag.record_type()?
                ),
            });
        }
        let bucket_size = record_tag.bucket_size().ok_or_else(|| EncodingError {
            message: "SeriesDictionaryKey should be bucket-scoped".to_string(),
        })?;

        let time_bucket = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let series_fingerprint = u128::from_be_bytes([
            buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21],
        ]);

        Ok(SeriesDictionaryKey {
            time_bucket,
            series_fingerprint,
            bucket_size,
        })
    }
}

impl RecordKey for SeriesDictionaryKey {
    const RECORD_TYPE: RecordType = RecordType::SeriesDictionary;
}

impl TimeBucketScoped for SeriesDictionaryKey {
    fn bucket(&self) -> crate::model::TimeBucket {
        crate::model::TimeBucket {
            start: self.time_bucket,
            size: self.bucket_size,
        }
    }
}

/// ForwardIndex key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardIndexKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub series_id: SeriesId,
}

impl ForwardIndexKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            RecordTag::new_bucket_scoped(RecordType::ForwardIndex, self.bucket_size).as_byte(),
        ]);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        buf.extend_from_slice(&self.series_id.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for ForwardIndexKey".to_string(),
            });
        }
        if buf[0] != KEY_VERSION {
            return Err(EncodingError {
                message: format!(
                    "Invalid key version: expected 0x{:02x}, got 0x{:02x}",
                    KEY_VERSION, buf[0]
                ),
            });
        }
        let record_tag = RecordTag::from_byte(buf[1])?;
        if record_tag.record_type()? != RecordType::ForwardIndex {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected ForwardIndex, got {:?}",
                    record_tag.record_type()?
                ),
            });
        }
        let bucket_size = record_tag.bucket_size().ok_or_else(|| EncodingError {
            message: "ForwardIndexKey should be bucket-scoped".to_string(),
        })?;

        let time_bucket = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let series_id = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);

        Ok(ForwardIndexKey {
            time_bucket,
            series_id,
            bucket_size,
        })
    }
}

impl RecordKey for ForwardIndexKey {
    const RECORD_TYPE: RecordType = RecordType::ForwardIndex;
}

impl TimeBucketScoped for ForwardIndexKey {
    fn bucket(&self) -> crate::model::TimeBucket {
        crate::model::TimeBucket {
            start: self.time_bucket,
            size: self.bucket_size,
        }
    }
}

/// InvertedIndex key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvertedIndexKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub attribute: String,
    pub value: String,
}

impl InvertedIndexKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            RecordTag::new_bucket_scoped(RecordType::InvertedIndex, self.bucket_size).as_byte(),
        ]);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        encode_utf8(&self.attribute, &mut buf);
        encode_utf8(&self.value, &mut buf);
        buf.freeze()
    }

    /// Create a BytesRange that covers all entries for a specific attribute (label name)
    /// within a given bucket. This allows efficient scanning for all values of a label.
    pub fn attribute_range(bucket: &crate::model::TimeBucket, attribute: &str) -> BytesRange {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            RecordTag::new_bucket_scoped(RecordType::InvertedIndex, bucket.size).as_byte(),
        ]);
        buf.extend_from_slice(&bucket.start.to_be_bytes());
        encode_utf8(attribute, &mut buf);
        BytesRange::prefix(buf.freeze())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for InvertedIndexKey".to_string(),
            });
        }
        if buf[0] != KEY_VERSION {
            return Err(EncodingError {
                message: format!(
                    "Invalid key version: expected 0x{:02x}, got 0x{:02x}",
                    KEY_VERSION, buf[0]
                ),
            });
        }
        let record_tag = RecordTag::from_byte(buf[1])?;
        if record_tag.record_type()? != RecordType::InvertedIndex {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected InvertedIndex, got {:?}",
                    record_tag.record_type()?
                ),
            });
        }
        let bucket_size = record_tag.bucket_size().ok_or_else(|| EncodingError {
            message: "InvertedIndexKey should be bucket-scoped".to_string(),
        })?;

        let mut slice = &buf[2..];
        let time_bucket = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);
        slice = &slice[4..];

        let attribute = decode_utf8(&mut slice)?;
        let value = decode_utf8(&mut slice)?;

        Ok(InvertedIndexKey {
            time_bucket,
            attribute,
            value,
            bucket_size,
        })
    }
}

impl RecordKey for InvertedIndexKey {
    const RECORD_TYPE: RecordType = RecordType::InvertedIndex;
}

impl TimeBucketScoped for InvertedIndexKey {
    fn bucket(&self) -> crate::model::TimeBucket {
        crate::model::TimeBucket {
            start: self.time_bucket,
            size: self.bucket_size,
        }
    }
}

/// TimeSeries key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSeriesKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub series_id: SeriesId,
}

impl TimeSeriesKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            RecordTag::new_bucket_scoped(RecordType::TimeSeries, self.bucket_size).as_byte(),
        ]);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        buf.extend_from_slice(&self.series_id.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for TimeSeriesKey".to_string(),
            });
        }
        if buf[0] != KEY_VERSION {
            return Err(EncodingError {
                message: format!(
                    "Invalid key version: expected 0x{:02x}, got 0x{:02x}",
                    KEY_VERSION, buf[0]
                ),
            });
        }
        let record_tag = RecordTag::from_byte(buf[1])?;
        if record_tag.record_type()? != RecordType::TimeSeries {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected TimeSeries, got {:?}",
                    record_tag.record_type()?
                ),
            });
        }
        let bucket_size = record_tag.bucket_size().ok_or_else(|| EncodingError {
            message: "TimeSeriesKey should be bucket-scoped".to_string(),
        })?;

        let time_bucket = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let series_id = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);

        Ok(TimeSeriesKey {
            time_bucket,
            series_id,
            bucket_size,
        })
    }
}

impl RecordKey for TimeSeriesKey {
    const RECORD_TYPE: RecordType = RecordType::TimeSeries;
}

impl TimeBucketScoped for TimeSeriesKey {
    fn bucket(&self) -> crate::model::TimeBucket {
        crate::model::TimeBucket {
            start: self.time_bucket,
            size: self.bucket_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_bucket_list_key() {
        // given
        let key = BucketListKey;

        // when
        let encoded = key.encode();
        let decoded = BucketListKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_series_dictionary_key() {
        // given
        let key = SeriesDictionaryKey {
            time_bucket: 12345,
            series_fingerprint: 67890,
            bucket_size: 2,
        };

        // when
        let encoded = key.encode();
        let decoded = SeriesDictionaryKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_forward_index_key() {
        // given
        let key = ForwardIndexKey {
            time_bucket: 12345,
            series_id: 42,
            bucket_size: 3,
        };

        // when
        let encoded = key.encode();
        let decoded = ForwardIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_inverted_index_key() {
        // given
        let key = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "server1".to_string(),
            bucket_size: 1,
        };

        // when
        let encoded = key.encode();
        let decoded = InvertedIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_time_series_key() {
        // given
        let key = TimeSeriesKey {
            time_bucket: 12345,
            series_id: 99,
            bucket_size: 4,
        };

        // when
        let encoded = key.encode();
        let decoded = TimeSeriesKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_create_attribute_range_that_matches_same_attribute_keys() {
        // given
        let bucket = crate::model::TimeBucket {
            start: 12345,
            size: 1,
        };
        let key1 = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "server1".to_string(),
            bucket_size: 1,
        };
        let key2 = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "server2".to_string(),
            bucket_size: 1,
        };
        let key3 = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "env".to_string(),
            value: "prod".to_string(),
            bucket_size: 1,
        };

        // when
        let range = InvertedIndexKey::attribute_range(&bucket, "host");

        // then
        assert!(range.contains(&key1.encode()));
        assert!(range.contains(&key2.encode()));
        assert!(!range.contains(&key3.encode()));
    }

    #[test]
    fn should_not_match_shorter_attribute_with_value_that_looks_like_suffix() {
        // given - searching for "hostname" should NOT match a key with
        // attribute "host" and value "name" even though "host" + "name" = "hostname"
        // The length-prefix encoding should prevent this collision.
        let bucket = crate::model::TimeBucket {
            start: 12345,
            size: 1,
        };
        let host_name_key = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "name".to_string(),
            bucket_size: 1,
        };

        // when - search for "hostname"
        let range = InvertedIndexKey::attribute_range(&bucket, "hostname");

        // then - should NOT match the "host":"name" key
        assert!(
            !range.contains(&host_name_key.encode()),
            "attribute_range for 'hostname' should not match key with attribute='host' value='name'. \
             The length-prefix encoding should differentiate them: \
             'hostname' encodes as [len=8, ...] while 'host' encodes as [len=4, ...]"
        );
    }

    #[test]
    fn should_not_match_when_value_bytes_could_mimic_attribute_continuation() {
        // given - test a more contrived case where the value length bytes
        // might numerically match what would be expected for a longer attribute
        let bucket = crate::model::TimeBucket {
            start: 12345,
            size: 1,
        };

        // Key with short attribute and value whose length encoding could be confused
        // attribute "ab" (len=2) with value of length that starts with same byte
        let short_attr_key = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "ab".to_string(),
            value: "cdef".to_string(), // len=4, encoded as [0x04, 0x00] in little-endian
            bucket_size: 1,
        };

        // Search for "abcdef" (len=6)
        // If encoding was naive concatenation, "ab" + "cdef" might look like "abcdef"
        // But with length-prefix: [len=2][ab][len=4][cdef] vs [len=6][abcdef]

        // when
        let range = InvertedIndexKey::attribute_range(&bucket, "abcdef");

        // then
        assert!(
            !range.contains(&short_attr_key.encode()),
            "attribute_range for 'abcdef' should not match key with attribute='ab' value='cdef'"
        );
    }
}
