// Key structures with big-endian encoding

use super::*;
use crate::model::{BucketSize, BucketStart, SeriesFingerprint, SeriesId};
use bytes::{Bytes, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::KeyPrefix;
use common::serde::terminated_bytes;

/// BucketList key (global-scoped)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketListKey;

impl BucketListKey {
    pub fn encode(&self) -> Bytes {
        RecordType::BucketList.prefix().to_bytes()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::BucketList {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected BucketList, got {:?}",
                    record_type
                ),
            });
        }
        if bucket_size_from_tag(prefix.tag()).is_some() {
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
        RecordType::SeriesDictionary
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut buf);
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
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::SeriesDictionary {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected SeriesDictionary, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
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
        RecordType::ForwardIndex
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut buf);
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
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::ForwardIndex {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected ForwardIndex, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
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
        RecordType::InvertedIndex
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut buf);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        // Attribute uses terminated encoding to delimit from value
        terminated_bytes::serialize(self.attribute.as_bytes(), &mut buf);
        // Value is raw UTF-8 (no terminator needed - end of key acts as delimiter)
        buf.extend_from_slice(self.value.as_bytes());
        buf.freeze()
    }

    /// Create a BytesRange that covers all entries for a specific attribute (label name)
    /// within a given bucket. This allows efficient scanning for all values of a label.
    pub fn attribute_range(bucket: &crate::model::TimeBucket, attribute: &str) -> BytesRange {
        let mut buf = BytesMut::new();
        RecordType::InvertedIndex
            .prefix_with_bucket_size(bucket.size)
            .write_to(&mut buf);
        buf.extend_from_slice(&bucket.start.to_be_bytes());
        terminated_bytes::serialize(attribute.as_bytes(), &mut buf);
        BytesRange::prefix(buf.freeze())
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for InvertedIndexKey".to_string(),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::InvertedIndex {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected InvertedIndex, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
            message: "InvertedIndexKey should be bucket-scoped".to_string(),
        })?;

        let mut slice = &buf[2..];
        let time_bucket = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);
        slice = &slice[4..];

        // Attribute uses terminated encoding
        let attribute_bytes = terminated_bytes::deserialize(&mut slice)?;
        let attribute = String::from_utf8(attribute_bytes.to_vec()).map_err(|e| EncodingError {
            message: format!("Invalid UTF-8 in attribute: {}", e),
        })?;

        // Value is the remaining bytes (raw UTF-8, no terminator)
        let value = String::from_utf8(slice.to_vec()).map_err(|e| EncodingError {
            message: format!("Invalid UTF-8 in value: {}", e),
        })?;

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
        RecordType::TimeSeries
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut buf);
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
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::TimeSeries {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected TimeSeries, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
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

/// Experimental metric-prefixed sample key.
///
/// Key layout: `<record_prefix, time_bucket, metric_name (terminated), series_id>`
///
/// This groups all series for one metric together within a bucket, providing
/// physical locality for single-metric queries. The metric_name uses terminated
/// encoding so prefix scans over all series for a given metric are efficient.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricTimeSeriesKey {
    pub time_bucket: BucketStart,
    pub bucket_size: BucketSize,
    pub metric_name: String,
    pub series_id: SeriesId,
}

impl MetricTimeSeriesKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        RecordType::MetricTimeSeries
            .prefix_with_bucket_size(self.bucket_size)
            .write_to(&mut buf);
        buf.extend_from_slice(&self.time_bucket.to_be_bytes());
        terminated_bytes::serialize(self.metric_name.as_bytes(), &mut buf);
        buf.extend_from_slice(&self.series_id.to_be_bytes());
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for MetricTimeSeriesKey".to_string(),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;
        if record_type != RecordType::MetricTimeSeries {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected MetricTimeSeries, got {:?}",
                    record_type
                ),
            });
        }
        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
            message: "MetricTimeSeriesKey should be bucket-scoped".to_string(),
        })?;

        let time_bucket = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let mut slice = &buf[6..];

        let metric_name_bytes = terminated_bytes::deserialize(&mut slice)?;
        let metric_name =
            String::from_utf8(metric_name_bytes.to_vec()).map_err(|e| EncodingError {
                message: format!("Invalid UTF-8 in metric_name: {}", e),
            })?;

        if slice.len() < 4 {
            return Err(EncodingError {
                message: "Buffer too short for series_id in MetricTimeSeriesKey".to_string(),
            });
        }
        let series_id = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);

        Ok(MetricTimeSeriesKey {
            time_bucket,
            bucket_size,
            metric_name,
            series_id,
        })
    }

    /// Create a BytesRange covering all series for a given metric within a bucket.
    pub fn metric_range(
        bucket: &crate::model::TimeBucket,
        metric_name: &str,
    ) -> BytesRange {
        let mut buf = BytesMut::new();
        RecordType::MetricTimeSeries
            .prefix_with_bucket_size(bucket.size)
            .write_to(&mut buf);
        buf.extend_from_slice(&bucket.start.to_be_bytes());
        terminated_bytes::serialize(metric_name.as_bytes(), &mut buf);
        BytesRange::prefix(buf.freeze())
    }

    /// Create a BytesRange covering a specific series_id range within a metric and bucket.
    /// Both start and end are inclusive: [start_series_id, end_series_id].
    pub fn series_range(
        bucket: &crate::model::TimeBucket,
        metric_name: &str,
        start_series_id: SeriesId,
        end_series_id: SeriesId,
    ) -> BytesRange {
        use std::ops::Bound::Included;

        let encode_key = |series_id: SeriesId| -> Bytes {
            let mut buf = BytesMut::new();
            RecordType::MetricTimeSeries
                .prefix_with_bucket_size(bucket.size)
                .write_to(&mut buf);
            buf.extend_from_slice(&bucket.start.to_be_bytes());
            terminated_bytes::serialize(metric_name.as_bytes(), &mut buf);
            buf.extend_from_slice(&series_id.to_be_bytes());
            buf.freeze()
        };

        let start = encode_key(start_series_id);
        let end = encode_key(end_series_id);
        BytesRange::new(Included(start), Included(end))
    }
}

impl RecordKey for MetricTimeSeriesKey {
    const RECORD_TYPE: RecordType = RecordType::MetricTimeSeries;
}

impl TimeBucketScoped for MetricTimeSeriesKey {
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
        // The tuple-style delimiter encoding should prevent this collision.
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
             The delimiter-based encoding should differentiate them."
        );
    }

    #[test]
    fn should_not_match_when_value_bytes_could_mimic_attribute_continuation() {
        // given - test a more contrived case where naive concatenation
        // might produce a collision
        let bucket = crate::model::TimeBucket {
            start: 12345,
            size: 1,
        };

        // Key with short attribute and value that concatenates to a different attribute
        let short_attr_key = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "ab".to_string(),
            value: "cdef".to_string(),
            bucket_size: 1,
        };

        // Search for "abcdef"
        // If encoding was naive concatenation, "ab" + "cdef" might look like "abcdef"
        // But with tuple-style encoding, each element is delimited separately

        // when
        let range = InvertedIndexKey::attribute_range(&bucket, "abcdef");

        // then
        assert!(
            !range.contains(&short_attr_key.encode()),
            "attribute_range for 'abcdef' should not match key with attribute='ab' value='cdef'"
        );
    }

    #[test]
    fn should_encode_attribute_with_terminator_and_value_without() {
        // This test demonstrates and verifies that:
        // - Only the attribute uses terminated encoding (with 0x00 delimiter)
        // - The value uses raw UTF-8 (no terminator, delimited by end of key)
        //
        // This is sufficient because the attribute terminator separates attribute
        // from value, and the value extends to the end of the key.

        let key = InvertedIndexKey {
            time_bucket: 12345,
            attribute: "host".to_string(),
            value: "server1".to_string(),
            bucket_size: 1,
        };

        let encoded = key.encode();

        // Verify it round-trips correctly
        let decoded = InvertedIndexKey::decode(&encoded).unwrap();
        assert_eq!(decoded.attribute, "host");
        assert_eq!(decoded.value, "server1");

        // Verify the key ends with raw "server1" bytes (no trailing 0x00)
        let value_bytes = b"server1";
        assert!(
            encoded.ends_with(value_bytes),
            "Encoded key should end with raw value bytes (no terminator)"
        );

        // The encoded key should contain the 0x00 terminator after the attribute
        // but NOT after the value. We can verify by checking the byte before "server1"
        // is 0x00 (the attribute terminator).
        let value_start = encoded.len() - value_bytes.len();
        assert_eq!(
            encoded[value_start - 1],
            0x00,
            "Byte before value should be 0x00 (attribute terminator)"
        );
    }

    #[test]
    fn should_encode_and_decode_metric_time_series_key() {
        // given
        let key = MetricTimeSeriesKey {
            time_bucket: 12345,
            bucket_size: 1,
            metric_name: "http_requests_total".to_string(),
            series_id: 42,
        };

        // when
        let encoded = key.encode();
        let decoded = MetricTimeSeriesKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_order_metric_time_series_keys_by_metric_then_series_id() {
        // given - two keys for same metric, different series
        let bucket = crate::model::TimeBucket {
            start: 100,
            size: 1,
        };
        let key_a = MetricTimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            metric_name: "cpu_usage".to_string(),
            series_id: 5,
        };
        let key_b = MetricTimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            metric_name: "cpu_usage".to_string(),
            series_id: 10,
        };
        // different metric
        let key_c = MetricTimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            metric_name: "mem_usage".to_string(),
            series_id: 1,
        };

        // when
        let enc_a = key_a.encode();
        let enc_b = key_b.encode();
        let enc_c = key_c.encode();

        // then - same metric: ordered by series_id
        assert!(enc_a < enc_b, "same metric: lower series_id should sort first");
        // different metric: ordered lexicographically by metric name
        assert!(enc_b < enc_c, "cpu_usage should sort before mem_usage");
    }

    #[test]
    fn should_create_series_range_containing_bounded_series_ids() {
        // given
        let bucket = crate::model::TimeBucket {
            start: 100,
            size: 1,
        };
        let key_inside_low = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 10,
        };
        let key_inside_mid = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 15,
        };
        let key_inside_high = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 20,
        };
        let key_below = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 5,
        };
        let key_above = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 25,
        };
        let key_diff_metric = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "mem_usage".to_string(),
            series_id: 15,
        };

        // when
        let range = MetricTimeSeriesKey::series_range(&bucket, "cpu_usage", 10, 20);

        // then
        assert!(range.contains(&key_inside_low.encode()), "start boundary should be included");
        assert!(range.contains(&key_inside_mid.encode()), "mid-range should be included");
        assert!(range.contains(&key_inside_high.encode()), "end boundary should be included");
        assert!(!range.contains(&key_below.encode()), "below range should be excluded");
        assert!(!range.contains(&key_above.encode()), "above range should be excluded");
        assert!(!range.contains(&key_diff_metric.encode()), "different metric should be excluded");
    }

    #[test]
    fn should_create_series_range_for_single_id() {
        // given
        let bucket = crate::model::TimeBucket {
            start: 100,
            size: 1,
        };
        let key_exact = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 42,
        };
        let key_below = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 41,
        };
        let key_above = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 43,
        };

        // when - start == end
        let range = MetricTimeSeriesKey::series_range(&bucket, "cpu_usage", 42, 42);

        // then
        assert!(range.contains(&key_exact.encode()), "exact ID should be included");
        assert!(!range.contains(&key_below.encode()), "ID below should be excluded");
        assert!(!range.contains(&key_above.encode()), "ID above should be excluded");
    }

    #[test]
    fn should_scan_metric_prefix_range_for_bucket() {
        // given
        let bucket = crate::model::TimeBucket {
            start: 100,
            size: 1,
        };
        let key_match = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "cpu_usage".to_string(),
            series_id: 5,
        };
        let key_diff_metric = MetricTimeSeriesKey {
            time_bucket: 100,
            bucket_size: 1,
            metric_name: "mem_usage".to_string(),
            series_id: 5,
        };

        // when
        let range = MetricTimeSeriesKey::metric_range(&bucket, "cpu_usage");

        // then
        assert!(range.contains(&key_match.encode()));
        assert!(!range.contains(&key_diff_metric.encode()));
    }
}
