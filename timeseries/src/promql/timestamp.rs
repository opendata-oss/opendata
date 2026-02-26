use std::fmt::{Display, Formatter};
use std::ops::{Add, Sub};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// PromQL logical timestamp.
///
/// Represents milliseconds since Unix epoch as a signed 64-bit integer.
/// Matches Prometheus's internal representation (int64 milliseconds).
///
/// Unlike `SystemTime`, this:
/// - Supports negative timestamps
/// - Is pure arithmetic
/// - Has no OS clock semantics
/// - Cannot fail
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Create from raw milliseconds.
    pub fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Return underlying milliseconds.
    pub fn as_millis(&self) -> i64 {
        self.0
    }

    /// Convert back to SystemTime.
    ///
    /// Returns None if the timestamp is outside SystemTime's representable range.
    pub fn to_system_time(self) -> Option<SystemTime> {
        if self.0 >= 0 {
            UNIX_EPOCH.checked_add(Duration::from_millis(self.0 as u64))
        } else {
            let abs = self.0.checked_abs()? as u64;
            UNIX_EPOCH.checked_sub(Duration::from_millis(abs))
        }
    }
}

impl From<SystemTime> for Timestamp {
    fn from(t: SystemTime) -> Self {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Timestamp(d.as_millis() as i64),
            Err(e) => Timestamp(-(e.duration().as_millis() as i64)),
        }
    }
}

/* ---------------- Arithmetic ---------------- */

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        Timestamp(self.0 + rhs.as_millis() as i64)
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: Duration) -> Self::Output {
        Timestamp(self.0 - rhs.as_millis() as i64)
    }
}

/// Difference between two timestamps (returns milliseconds)
impl Sub<Timestamp> for Timestamp {
    type Output = i64;

    fn sub(self, rhs: Timestamp) -> Self::Output {
        self.0 - rhs.0
    }
}

/* ---------------- Formatting ---------------- */

impl Display for Timestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}ms", self.0)
    }
}

/* ---------------- Tests ---------------- */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_positive_system_time() {
        let time = UNIX_EPOCH + Duration::from_millis(1000);
        let ts = Timestamp::from(time);
        assert_eq!(ts.as_millis(), 1000);
    }

    #[test]
    fn converts_negative_system_time() {
        let time = UNIX_EPOCH - Duration::from_millis(500);
        let ts = Timestamp::from(time);
        assert_eq!(ts.as_millis(), -500);
    }

    #[test]
    fn supports_duration_arithmetic() {
        let ts = Timestamp::from_millis(1000);

        let result = ts + Duration::from_millis(500);
        assert_eq!(result.as_millis(), 1500);

        let result = ts - Duration::from_millis(300);
        assert_eq!(result.as_millis(), 700);
    }

    #[test]
    fn supports_timestamp_difference() {
        let ts1 = Timestamp::from_millis(1000);
        let ts2 = Timestamp::from_millis(500);

        assert_eq!(ts1 - ts2, 500);
    }

    #[test]
    fn round_trip_system_time_conversion() {
        let ts = Timestamp::from_millis(1000);
        let time = ts.to_system_time().unwrap();
        assert_eq!(time, UNIX_EPOCH + Duration::from_millis(1000));

        let ts = Timestamp::from_millis(-500);
        let time = ts.to_system_time().unwrap();
        assert_eq!(time, UNIX_EPOCH - Duration::from_millis(500));
    }

    #[test]
    fn display_formats_cleanly() {
        let ts = Timestamp::from_millis(1234);
        assert_eq!(format!("{}", ts), "1234ms");
    }

    #[test]
    fn handles_i64_min_edge_case() {
        let ts = Timestamp::from_millis(i64::MIN);
        // i64::MIN cannot be negated, so to_system_time should return None
        assert_eq!(ts.to_system_time(), None);
    }
}
