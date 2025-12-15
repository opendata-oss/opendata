use std::time::SystemTime;

use crate::util::{Result, hour_bucket_in_epoch_minutes};

/// Series ID (unique within a time bucket)
pub(crate) type SeriesId = u32;

/// Series fingerprint (hash of label set)
pub(crate) type SeriesFingerprint = u128;

/// Time bucket (minutes since UNIX epoch)
pub(crate) type BucketStart = u32;

/// Time bucket size (1-15, exponential: 1=1h, 2=2h, 3=4h, 4=8h, etc. = 2^(n-1) hours)
pub(crate) type BucketSize = u8;

/// Attribute key-value pair
pub(crate) type Attribute = (String, String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Temporality {
    Cumulative,
    Delta,
    Unspecified,
}

/// Metric type (gauge, sum, histogram, exponential histogram, summary)
#[derive(Clone, Copy, Debug)]
pub(crate) enum MetricType {
    Gauge,
    Sum {
        monotonic: bool,
        temporality: Temporality,
    },
    Histogram {
        temporality: Temporality,
    },
    ExponentialHistogram {
        temporality: Temporality,
    },
    Summary,
}

#[derive(Clone, Debug)]
pub(crate) struct TimeBucket {
    pub(crate) start: BucketStart,
    pub(crate) size: BucketSize,
}

impl TimeBucket {
    pub(crate) fn hour(start: BucketStart) -> Self {
        Self { start, size: 1 }
    }

    pub(crate) fn round_to_hour(time: SystemTime) -> Result<Self> {
        let bucket = hour_bucket_in_epoch_minutes(time)?;
        Ok(Self::hour(bucket))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SeriesSpec {
    pub(crate) metric_unit: Option<String>,
    pub(crate) metric_type: MetricType,
    pub(crate) attributes: Vec<Attribute>,
}

#[derive(Clone, Debug)]
pub(crate) struct Sample {
    pub(crate) timestamp: u64,
    pub(crate) value: f64,
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
    use super::*;

    #[test]
    fn should_convert_time_bucket_size_to_hours() {
        assert_eq!(time_bucket_size_hours(1), 1);
        assert_eq!(time_bucket_size_hours(2), 2);
        assert_eq!(time_bucket_size_hours(3), 4);
        assert_eq!(time_bucket_size_hours(4), 8);
        assert_eq!(time_bucket_size_hours(5), 16);
    }
}
