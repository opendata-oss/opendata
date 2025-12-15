// Key structures with big-endian encoding

use super::*;
use crate::model::{BucketSize, BucketStart, SeriesFingerprint, SeriesId};
use bytes::{Bytes, BytesMut};

/// BucketList key (global-scoped)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketListKey;

impl BucketListKey {
    pub fn encode(&self) -> Bytes {
        Bytes::from(vec![
            KEY_VERSION,
            encode_record_tag(RecordType::BucketList, None),
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
        let (record_type, bucket_size) = decode_record_tag(buf[1])?;
        if record_type != RecordType::BucketList {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected BucketList, got {:?}",
                    record_type
                ),
            });
        }
        if bucket_size.is_some() {
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
    pub series_fingerprint: SeriesFingerprint,
    pub bucket_size: BucketSize,
}

impl SeriesDictionaryKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            encode_record_tag(RecordType::SeriesDictionary, Some(self.bucket_size)),
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
        let (record_type, bucket_size) = decode_record_tag(buf[1])?;
        if record_type != RecordType::SeriesDictionary {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected SeriesDictionary, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size.ok_or_else(|| EncodingError {
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

/// ForwardIndex key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardIndexKey {
    pub time_bucket: BucketStart,
    pub series_id: SeriesId,
    pub bucket_size: BucketSize,
}

impl ForwardIndexKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            encode_record_tag(RecordType::ForwardIndex, Some(self.bucket_size)),
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
        let (record_type, bucket_size) = decode_record_tag(buf[1])?;
        if record_type != RecordType::ForwardIndex {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected ForwardIndex, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size.ok_or_else(|| EncodingError {
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

/// InvertedIndex key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvertedIndexKey {
    pub time_bucket: BucketStart,
    pub attribute: String,
    pub value: String,
    pub bucket_size: BucketSize,
}

impl InvertedIndexKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            encode_record_tag(RecordType::InvertedIndex, Some(self.bucket_size)),
        ]);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        encode_utf8(&self.attribute, &mut buf);
        encode_utf8(&self.value, &mut buf);
        buf.freeze()
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
        let (record_type, bucket_size) = decode_record_tag(buf[1])?;
        if record_type != RecordType::InvertedIndex {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected InvertedIndex, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size.ok_or_else(|| EncodingError {
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

/// TimeSeries key
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeSeriesKey {
    pub time_bucket: BucketStart,
    pub series_id: SeriesId,
    pub bucket_size: BucketSize,
}

impl TimeSeriesKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[
            KEY_VERSION,
            encode_record_tag(RecordType::TimeSeries, Some(self.bucket_size)),
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
        let (record_type, bucket_size) = decode_record_tag(buf[1])?;
        if record_type != RecordType::TimeSeries {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected TimeSeries, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size.ok_or_else(|| EncodingError {
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
}
