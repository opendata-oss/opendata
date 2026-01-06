#![allow(dead_code)]

use std::time::SystemTime;

use crate::series::{Label, MetricType, Sample};
use crate::util::{Result, hour_bucket_in_epoch_minutes};

/// Series ID (unique within a time bucket)
pub(crate) type SeriesId = u32;

/// Series fingerprint (hash of label set)
pub(crate) type SeriesFingerprint = u128;

/// Time bucket (minutes since UNIX epoch)
pub(crate) type BucketStart = u32;

/// Time bucket size (1-15, exponential: 1=1h, 2=2h, 3=4h, 4=8h, etc. = 2^(n-1) hours)
pub(crate) type BucketSize = u8;

/// Record tag combining record type and optional bucket size
/// Encoded as a single byte with high 4 bits for type and low 4 bits for bucket size (or reserved).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordTag(pub(crate) u8);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

    pub(crate) fn size_in_mins(&self) -> u32 {
        (self.size.pow(2) * 60) as u32
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SeriesSpec {
    pub(crate) metric_unit: Option<String>,
    pub(crate) metric_type: MetricType,
    pub(crate) labels: Vec<Label>,
}

/// A sample with all its labels, including the metric name
#[derive(Clone, Debug)]
pub(crate) struct SampleWithLabels {
    // TODO: this should be SeriesSample and use SeriesSpec
    pub(crate) labels: Vec<Label>,
    pub(crate) metric_unit: Option<String>,
    pub(crate) metric_type: MetricType,
    pub(crate) sample: Sample,
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
