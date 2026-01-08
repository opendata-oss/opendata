#![allow(dead_code)]

use blake3::Hasher;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::Error;
use crate::model::Label;

pub use crate::error::Result;
use crate::model::BucketSize;

/// Computes a Blake3 hash of a string, truncated to u64 for use as a fingerprint
/// in dictionary keys (attribute keys and values).
pub(crate) fn fingerprint_string(value: &str) -> u64 {
    let mut hasher = Hasher::new();
    hasher.update(value.as_bytes());
    let digest = hasher.finalize();
    let mut first8 = [0u8; 8];
    first8.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(first8)
}

pub(crate) trait Fingerprint {
    fn fingerprint(&self) -> u128;
}

impl Fingerprint for Vec<Label> {
    fn fingerprint(&self) -> u128 {
        self.as_slice().fingerprint()
    }
}

impl Fingerprint for [Label] {
    fn fingerprint(&self) -> u128 {
        let mut hasher = Hasher::new();
        for label in self {
            hasher.update(label.name.as_bytes());
            hasher.update(label.value.as_bytes());
        }

        let digest = hasher.finalize();
        let mut first16 = [0u8; 16];
        first16.copy_from_slice(&digest.as_bytes()[..16]);

        u128::from_le_bytes(first16)
    }
}

/// Parse a timestamp parameter that can be either RFC3339 or Unix timestamp (float seconds).
/// Returns SystemTime.
pub fn parse_timestamp(s: &str) -> Result<SystemTime> {
    // Try parsing as RFC3339 first
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.into());
    }

    // Try parsing as float Unix timestamp
    match s.parse::<f64>() {
        Ok(secs) => {
            if secs < 0.0 {
                return Err(Error::InvalidInput(format!(
                    "Invalid timestamp: negative value {}",
                    secs
                )));
            }
            Duration::try_from_secs_f64(secs)
                .map_err(|e| Error::InvalidInput(format!("Invalid timestamp: {}", e)))
                .and_then(|duration| {
                    SystemTime::UNIX_EPOCH.checked_add(duration).ok_or_else(|| {
                        Error::InvalidInput("Invalid timestamp: overflow".to_string())
                    })
                })
        }
        Err(e) => Err(Error::InvalidInput(format!(
            "Could not parse timestamp '{}': not RFC3339 or float ({})",
            s, e
        ))),
    }
}

/// Parse a timestamp and return Unix timestamp in seconds (i64).
/// This is a convenience wrapper for cases where i64 is needed instead of SystemTime.
pub fn parse_timestamp_to_seconds(s: &str) -> Result<i64> {
    parse_timestamp(s).and_then(|st| {
        st.duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .map_err(|e| Error::InvalidInput(format!("Invalid timestamp: {}", e)))
    })
}

/// Parse a duration parameter that can be either float seconds or Prometheus format ("1m", "30s", etc.).
/// Returns Duration.
pub fn parse_duration(s: &str) -> Result<Duration> {
    // Try parsing as float (seconds)
    if let Ok(secs) = s.parse::<f64>() {
        if secs < 0.0 {
            return Err(Error::InvalidInput(format!(
                "Invalid duration: negative value {}",
                secs
            )));
        }
        return Duration::try_from_secs_f64(secs)
            .map_err(|e| Error::InvalidInput(format!("Invalid duration: {}", e)));
    }

    // Try parsing Prometheus duration format
    promql_parser::util::parse_duration(s)
        .map_err(|e| Error::InvalidInput(format!("Invalid duration: {}", e)))
}

/// Truncate `time` down to the start of the hour and return the Unix epoch minutes as `u32`.
/// Errors if `time` is before the Unix epoch or beyond `u32::MAX` minutes (~8170 years).
pub fn hour_bucket_in_epoch_minutes(time: SystemTime) -> Result<u32> {
    const HOUR_MINS: u64 = 60;
    let mins = time.duration_since(UNIX_EPOCH)?.as_secs() / 60;
    let bucket = mins - (mins % HOUR_MINS);
    let bucket_u32 = u32::try_from(bucket)?;
    Ok(bucket_u32)
}

pub fn hour_bucket_unix_secs(time: SystemTime) -> Option<u64> {
    let secs = time.duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(secs - (secs % 3600))
}

pub(crate) fn normalize_str(s: &str) -> Option<String> {
    if s.is_empty() {
        return None;
    }
    Some(s.to_string())
}

/// Convert TimeBucketSize to hours
pub fn time_bucket_size_hours(size: BucketSize) -> u32 {
    if size == 0 || size > 15 {
        return 0;
    }
    2u32.pow((size - 1) as u32)
}

#[cfg(test)]
mod tests {
    use super::{
        parse_duration, parse_timestamp, parse_timestamp_to_seconds, time_bucket_size_hours,
    };
    use bytes::{BufMut, Bytes, BytesMut};
    use common::BytesRange;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_bytes_range_new() {
        let start = Included(Bytes::from("start"));
        let end = Excluded(Bytes::from("end"));
        let range = BytesRange::new(start.clone(), end.clone());

        assert_eq!(range.start, start);
        assert_eq!(range.end, end);
    }

    #[test]
    fn test_bytes_range_all() {
        let range = BytesRange::unbounded();
        assert_eq!(range.start, Unbounded);
        assert_eq!(range.end, Unbounded);
    }

    #[test]
    fn test_bytes_range_prefix() {
        let prefix = Bytes::from(vec![1, 2, 3]);
        let range = BytesRange::prefix(prefix.clone());

        assert_eq!(range.start, Included(prefix));
        assert_eq!(range.end, Excluded(Bytes::from(vec![1, 2, 4])));
    }

    #[test]
    fn test_bytes_range_prefix_with_max_byte() {
        // Test prefix ending with 0xFF
        let prefix = Bytes::from(vec![1, 2, 0xFF]);
        let range = BytesRange::prefix(prefix.clone());

        assert_eq!(range.start, Included(prefix));
        assert_eq!(range.end, Excluded(Bytes::from(vec![1, 3])));
    }

    #[test]
    fn test_bytes_range_prefix_with_max_bytes() {
        let prefix = Bytes::from(vec![0xFF, 0xFF]);
        let range = BytesRange::prefix(prefix.clone());

        assert_eq!(range.start, Included(prefix));
        assert_eq!(range.end, Unbounded);
    }

    #[test]
    fn test_bytes_range_prefix_creates_namespace_bucket_range() {
        let namespace_id = 123u32;
        let bucket_start_epoch_min = 456u32;

        let mut buf = BytesMut::new();
        buf.put_u32(namespace_id);
        buf.put_u32(bucket_start_epoch_min);
        let prefix = buf.freeze();

        let range = BytesRange::prefix(prefix.clone());

        let Included(start) = range.start else {
            panic!("unexpected bound type");
        };
        let Excluded(end) = range.end else {
            panic!("unexpected bound type");
        };

        assert_eq!(start.len(), 8);
        assert_eq!(end.len(), 8);

        // Verify the end is one byte higher
        assert_eq!(&start[..7], &end[..7]);
        assert_eq!(start[7] + 1, end[7]);
    }

    #[test]
    fn test_prefix_range_covers_all_with_prefix() {
        let p = Bytes::from_static(b"\x12\xff\xff");
        let r = BytesRange::prefix(p.clone());
        assert!(r.contains(b"\x12\xff\xff"));
        assert!(r.contains(b"\x12\xff\xff\x00\x01"));
        assert!(!r.contains(b"\x13")); // should be just outside
        assert!(!r.contains(b"\x12\xff\xfe")); // wrong prefix
    }

    #[test]
    fn should_parse_rfc3339_timestamp() {
        // given
        let timestamp_str = "2025-10-10T12:39:19.781Z";
        let expected =
            SystemTime::from(chrono::DateTime::parse_from_rfc3339(timestamp_str).unwrap());

        // when
        let result = parse_timestamp(timestamp_str);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn should_parse_unix_timestamp_as_float() {
        // given
        let timestamp_str = "1234567.56";
        let expected = SystemTime::UNIX_EPOCH + Duration::from_secs_f64(1234567.56);

        // when
        let result = parse_timestamp(timestamp_str);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn should_parse_unix_timestamp_as_integer() {
        // given
        let timestamp_str = "1234567890";
        let expected = SystemTime::UNIX_EPOCH + Duration::from_secs(1234567890);

        // when
        let result = parse_timestamp(timestamp_str);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn should_fail_to_parse_negative_timestamp() {
        // given
        let timestamp_str = "-1234567.56";

        // when
        let result = parse_timestamp(timestamp_str);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("negative value"));
    }

    #[test]
    fn should_fail_to_parse_invalid_timestamp() {
        // given
        let timestamp_str = "not-a-timestamp";

        // when
        let result = parse_timestamp(timestamp_str);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Could not parse timestamp")
        );
    }

    #[test]
    fn should_parse_timestamp_to_seconds() {
        // given
        let timestamp_str = "1234567890";
        let expected = 1234567890i64;

        // when
        let result = parse_timestamp_to_seconds(timestamp_str);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn should_parse_float_duration() {
        // given
        let duration_str = "60.5";
        let expected = Duration::from_secs_f64(60.5);

        // when
        let result = parse_duration(duration_str);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn should_parse_integer_duration() {
        // given
        let duration_str = "120";
        let expected = Duration::from_secs(120);

        // when
        let result = parse_duration(duration_str);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn should_parse_prometheus_duration_minutes() {
        // given
        let duration_str = "5m";
        let expected = Duration::from_secs(300);

        // when
        let result = parse_duration(duration_str);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn should_parse_prometheus_duration_hours() {
        // given
        let duration_str = "2h";
        let expected = Duration::from_secs(7200);

        // when
        let result = parse_duration(duration_str);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn should_fail_to_parse_negative_duration() {
        // given
        let duration_str = "-60";

        // when
        let result = parse_duration(duration_str);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("negative value"));
    }

    #[test]
    fn should_fail_to_parse_invalid_duration() {
        // given
        let duration_str = "not-a-duration";

        // when
        let result = parse_duration(duration_str);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid duration"));
    }

    #[test]
    fn should_convert_time_bucket_size_to_hours() {
        assert_eq!(time_bucket_size_hours(1), 1);
        assert_eq!(time_bucket_size_hours(2), 2);
        assert_eq!(time_bucket_size_hours(3), 4);
        assert_eq!(time_bucket_size_hours(4), 8);
        assert_eq!(time_bucket_size_hours(5), 16);
    }
}
