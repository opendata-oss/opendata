//! Adapter trait + ClickHouse-shaped insert plan types.
//!
//! Adapters consume a drained [`CommitGroupBatch`] and produce a
//! deterministic sequence of [`InsertChunk`]s. Each chunk carries a
//! per-chunk idempotency token of the form
//! `{manifest}:{low}-{high}:{adapter_version}:{chunking_fingerprint}:{chunk_index}`,
//! so a replay of the same Buffer sequence range under the same
//! configuration produces identical tokens (and therefore deduplicates
//! cleanly at the table level).

pub mod logs;

use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::commit_group::CommitGroupBatch;
use crate::error::IngestorResult;

/// One column value in an [`InsertChunk`] row. Aligns with the column
/// types expected by the alpha logs DDL — primitive types, plus
/// `Map(LowCardinality(String), String)` for OTLP attribute maps.
#[derive(Debug, Clone)]
pub enum RowValue {
    String(String),
    LowCardinalityString(String),
    UInt8(u8),
    Int32(i32),
    UInt32(u32),
    UInt64(u64),
    Int64(i64),
    DateTime64Nanos(u64),
    StringMap(BTreeMap<String, String>),
}

impl RowValue {
    /// Serialize this value into the JSONEachRow payload. The alpha
    /// writer uses JSONEachRow to keep `Map` columns straightforward;
    /// switching to `RowBinaryWithNamesAndTypes` is a future
    /// optimization (see RFC 0003 open questions).
    pub fn to_json(&self) -> JsonValue {
        match self {
            RowValue::String(s) | RowValue::LowCardinalityString(s) => JsonValue::String(s.clone()),
            RowValue::UInt8(v) => JsonValue::from(*v),
            RowValue::Int32(v) => JsonValue::from(*v),
            RowValue::UInt32(v) => JsonValue::from(*v),
            RowValue::UInt64(v) => JsonValue::from(*v),
            RowValue::Int64(v) => JsonValue::from(*v),
            // ClickHouse accepts a string literal for DateTime64(9). We
            // emit ISO-8601 with nanosecond precision.
            RowValue::DateTime64Nanos(ns) => JsonValue::String(format_datetime64_ns(*ns)),
            RowValue::StringMap(m) => {
                let mut obj = serde_json::Map::with_capacity(m.len());
                for (k, v) in m {
                    obj.insert(k.clone(), JsonValue::String(v.clone()));
                }
                JsonValue::Object(obj)
            }
        }
    }
}

fn format_datetime64_ns(unix_nanos: u64) -> String {
    // ClickHouse accepts ISO-8601 strings with nanosecond precision when
    // inserting into DateTime64(9). We format as
    // YYYY-MM-DD HH:MM:SS.NNNNNNNNN UTC.
    let secs = (unix_nanos / 1_000_000_000) as i64;
    let nanos = (unix_nanos % 1_000_000_000) as u32;

    chrono_internal::format(secs, nanos)
}

/// Minimal date/time formatting helper. We avoid taking a hard
/// dependency on `chrono` here by hand-rolling the small ISO-8601
/// formatter we need. ClickHouse accepts both
/// `YYYY-MM-DD HH:MM:SS.NNNNNNNNN` and ISO-8601 `T`-separated forms; we
/// use the first because it round-trips through `parseDateTimeBestEffort`.
mod chrono_internal {
    pub fn format(secs: i64, nanos: u32) -> String {
        let (year, month, day, hour, minute, second) = parts(secs);
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}",
            year, month, day, hour, minute, second, nanos
        )
    }

    fn is_leap(year: i64) -> bool {
        (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
    }

    fn days_in_month(year: i64, month: u32) -> u32 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if is_leap(year) {
                    29
                } else {
                    28
                }
            }
            _ => 0,
        }
    }

    fn parts(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
        let day_seconds = 86_400i64;
        let mut days = secs.div_euclid(day_seconds);
        let mut sec_of_day = secs.rem_euclid(day_seconds);
        let hour = (sec_of_day / 3600) as u32;
        sec_of_day %= 3600;
        let minute = (sec_of_day / 60) as u32;
        let second = (sec_of_day % 60) as u32;

        // Days since 1970-01-01.
        let mut year: i64 = 1970;
        loop {
            let dy = if is_leap(year) { 366 } else { 365 };
            if days >= dy {
                days -= dy;
                year += 1;
            } else if days < 0 {
                year -= 1;
                let dy_prev = if is_leap(year) { 366 } else { 365 };
                days += dy_prev;
            } else {
                break;
            }
        }
        let mut month: u32 = 1;
        loop {
            let dim = days_in_month(year, month) as i64;
            if days >= dim {
                days -= dim;
                month += 1;
            } else {
                break;
            }
        }
        let day = (days + 1) as u32;
        (year, month, day, hour, minute, second)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn unix_epoch_is_1970_01_01() {
            assert_eq!(format(0, 0), "1970-01-01 00:00:00.000000000");
        }

        #[test]
        fn known_timestamp() {
            // 2023-11-14 22:13:20 UTC == 1700000000
            assert_eq!(format(1_700_000_000, 0), "2023-11-14 22:13:20.000000000");
        }

        #[test]
        fn leap_year_handling() {
            // 2020-02-29 00:00:00 UTC == 1582934400
            assert_eq!(format(1_582_934_400, 0), "2020-02-29 00:00:00.000000000");
        }
    }
}

/// ClickHouse settings that the writer should set for an insert chunk.
#[derive(Debug, Clone, Default)]
pub struct ClickHouseSettings {
    pub insert_quorum: Option<String>,
    /// `insert_deduplication_token`. Always populated by the adapter
    /// for traceability; the writer applies it to the insert only when
    /// `apply_deduplication_token` is true. ClickHouse historically
    /// only honors the setting on `Replicated*` engines; for a plain
    /// `ReplacingMergeTree` it can be a no-op or, in some versions,
    /// reject the insert silently.
    pub insert_deduplication_token: String,
    /// Whether the writer should set `insert_deduplication_token` on
    /// the request. Default true (keeps the alpha guarantee). Set
    /// false against non-replicated test deployments.
    pub apply_deduplication_token: bool,
}

/// One ClickHouse insert request.
#[derive(Debug, Clone)]
pub struct InsertChunk {
    pub database: String,
    pub table: String,
    pub columns: Vec<&'static str>,
    pub rows: Vec<Vec<RowValue>>,
    pub settings: ClickHouseSettings,
    pub idempotency_token: String,
    pub chunk_index: u32,
    pub observability_labels: Vec<(&'static str, String)>,
}

impl InsertChunk {
    pub fn rows_count(&self) -> usize {
        self.rows.len()
    }
}

pub trait Adapter {
    type Input;

    /// Plan a deterministic sequence of insert chunks from the drained
    /// commit-group batch. Implementations must satisfy:
    ///
    /// 1. Chunking is a pure function of `(records, configured_thresholds)`.
    /// 2. The same `(low_sequence, high_sequence, chunk_index)` tuple
    ///    always produces the same rows on a replay.
    /// 3. Each chunk's `idempotency_token` is unique within the batch and
    ///    follows the format documented in RFC 0003.
    fn plan(&self, batch: CommitGroupBatch<Self::Input>) -> IngestorResult<Vec<InsertChunk>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime64_format_round_trips_for_nanos() {
        let ns: u64 = 1_700_000_000_123_456_789;
        let s = format_datetime64_ns(ns);
        assert_eq!(s, "2023-11-14 22:13:20.123456789");
    }

    #[test]
    fn row_value_stringmap_serializes_to_object() {
        let mut m = BTreeMap::new();
        m.insert("k".to_string(), "v".to_string());
        let json = RowValue::StringMap(m).to_json();
        assert_eq!(json.as_object().unwrap().get("k").unwrap(), "v");
    }
}
