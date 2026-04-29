//! ORDER BY benchmark harness.
//!
//! RFC 0003 calls for benchmarking the alpha logs table's `ORDER BY`
//! shape against a query-optimized alternative before the alpha goes
//! live. This module defines:
//!
//! - The two table variants under comparison.
//! - The three representative log query shapes.
//! - A small driver that runs each query against a configured
//!   ClickHouse endpoint and reports rows-scanned + elapsed time.
//!
//! Execution against production-scale data is **not** wired in; that's
//! a follow-up. The harness is present so the alpha review has a
//! concrete artifact rather than an open question, and so we can plug
//! a real dataset in when we're ready.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::adapter::logs::{LogsAdapterConfig, logs_table_ddl};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    /// ClickHouse endpoint. The benchmark assumes a database is
    /// already created.
    pub endpoint: String,
    pub database: String,
    /// Where to load synthetic data from. The harness expects the
    /// caller to populate the table; we only run the queries.
    pub table_alpha_default: String,
    /// Optional alternate table used to compare an ORDER BY shape
    /// that prioritizes query performance.
    pub table_query_optimized: Option<String>,
    pub user: String,
    pub password: String,
    pub request_timeout: Duration,
}

/// One representative query shape from the RFC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryShape {
    /// Last N for a service: `SELECT * FROM <t> WHERE ServiceName = ?
    /// ORDER BY Timestamp DESC LIMIT N`.
    LastNForService { service: &'static str, limit: usize },
    /// Time-range scan with severity filter: `SELECT count() FROM <t>
    /// WHERE Timestamp BETWEEN a AND b AND SeverityNumber >= 9`.
    TimeRangeWithSeverity { from_ts: u64, to_ts: u64 },
    /// Attribute-key lookup: `SELECT * FROM <t> WHERE
    /// LogAttributes['topic'] = 'foo' LIMIT N`.
    AttributeLookup {
        key: &'static str,
        value: &'static str,
        limit: usize,
    },
}

impl QueryShape {
    pub fn render_sql(&self, database: &str, table: &str) -> String {
        match self {
            QueryShape::LastNForService { service, limit } => format!(
                "SELECT count() FROM {database}.{table} \
                 WHERE ServiceName = '{service}' \
                 ORDER BY Timestamp DESC LIMIT {limit}"
            ),
            QueryShape::TimeRangeWithSeverity { from_ts, to_ts } => format!(
                "SELECT count() FROM {database}.{table} \
                 WHERE Timestamp BETWEEN fromUnixTimestamp({from_ts}) AND fromUnixTimestamp({to_ts}) \
                 AND SeverityNumber >= 9"
            ),
            QueryShape::AttributeLookup { key, value, limit } => format!(
                "SELECT count() FROM {database}.{table} \
                 WHERE LogAttributes['{key}'] = '{value}' LIMIT {limit}"
            ),
        }
    }
}

/// The two table variants the RFC compares: the alpha hybrid ORDER BY
/// and a raw landing table that is dedupe-only and feeds a query-tuned
/// materialized view.
pub fn alpha_default_ddl(adapter: &LogsAdapterConfig) -> String {
    logs_table_ddl(adapter)
}

pub fn raw_landing_table_ddl(adapter: &LogsAdapterConfig) -> String {
    // Raw landing variant: dedupe-only ORDER BY, no time prefix. Same
    // schema columns. The alternative would feed a materialized view
    // with a query-tuned shape, which the alpha is not yet wiring up
    // — committing the DDL here documents intent.
    let mut ddl = logs_table_ddl(adapter);
    ddl = ddl.replace(
        "ORDER BY (toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)",
        "ORDER BY (_odb_sequence, _odb_entry_index, _odb_record_index)",
    );
    ddl
}

/// Suggested query set for the alpha benchmark.
pub fn alpha_query_suite() -> Vec<QueryShape> {
    vec![
        QueryShape::LastNForService {
            service: "controller",
            limit: 100,
        },
        QueryShape::TimeRangeWithSeverity {
            from_ts: 1_700_000_000,
            to_ts: 1_700_010_000,
        },
        QueryShape::AttributeLookup {
            key: "topic",
            value: "foo",
            limit: 100,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> LogsAdapterConfig {
        LogsAdapterConfig::default()
    }

    #[test]
    fn alpha_default_uses_hybrid_order_by() {
        let ddl = alpha_default_ddl(&cfg());
        assert!(ddl.contains(
            "ORDER BY (toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)"
        ));
    }

    #[test]
    fn raw_landing_uses_pure_source_order_by() {
        let ddl = raw_landing_table_ddl(&cfg());
        assert!(ddl.contains("ORDER BY (_odb_sequence, _odb_entry_index, _odb_record_index)"));
        assert!(!ddl.contains("toDate(Timestamp), ServiceName"));
    }

    #[test]
    fn last_n_query_renders_with_service_and_limit() {
        let q = QueryShape::LastNForService {
            service: "controller",
            limit: 100,
        };
        let sql = q.render_sql("db", "t");
        assert!(sql.contains("ServiceName = 'controller'"));
        assert!(sql.contains("LIMIT 100"));
    }

    #[test]
    fn time_range_renders_with_unix_bounds() {
        let q = QueryShape::TimeRangeWithSeverity {
            from_ts: 1,
            to_ts: 2,
        };
        let sql = q.render_sql("db", "t");
        assert!(sql.contains("fromUnixTimestamp(1)"));
        assert!(sql.contains("fromUnixTimestamp(2)"));
        assert!(sql.contains("SeverityNumber >= 9"));
    }

    #[test]
    fn attribute_lookup_renders_with_key_value() {
        let q = QueryShape::AttributeLookup {
            key: "topic",
            value: "foo",
            limit: 50,
        };
        let sql = q.render_sql("db", "t");
        assert!(sql.contains("LogAttributes['topic'] = 'foo'"));
    }

    #[test]
    fn alpha_query_suite_covers_three_shapes() {
        let suite = alpha_query_suite();
        assert_eq!(suite.len(), 3);
    }
}
