//! OTLP logs ClickHouse adapter.
//!
//! Maps `DecodedLogRecord`s into rows in a `ReplacingMergeTree(_adapter_version)`
//! table whose ORDER BY hybridizes a query-useful prefix with the
//! source-coordinate suffix that gives every record a unique dedupe key
//! (see RFC 0003 "ORDER BY Tradeoff").
//!
//! Chunking is deterministic: rows are emitted in `(sequence, entry_index,
//! record_index)` order, then split into chunks of `max_chunk_rows`. The
//! chunking fingerprint hashes the inputs that affect chunk boundaries so
//! a configuration change between partial-success and ack does not collide
//! tokens with rows produced under different rules.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::adapter::{Adapter, ClickHouseSettings, InsertChunk, RowValue};
use crate::commit_group::CommitGroupBatch;
use crate::error::IngestorResult;
use crate::signal::DecodedLogRecord;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogsAdapterConfig {
    pub database: String,
    pub table: String,
    pub adapter_version: u32,
    /// Maximum rows per ClickHouse insert chunk.
    pub max_chunk_rows: usize,
    /// Maximum approximate bytes per ClickHouse insert chunk.
    pub max_chunk_bytes: usize,
    pub insert_quorum: Option<String>,
}

impl Default for LogsAdapterConfig {
    fn default() -> Self {
        Self {
            database: "responsive".to_string(),
            table: "logs".to_string(),
            adapter_version: 1,
            max_chunk_rows: 100_000,
            max_chunk_bytes: 32 * 1024 * 1024,
            insert_quorum: Some("auto".to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OtlpLogsClickHouseAdapter {
    config: LogsAdapterConfig,
    chunking_fingerprint: String,
}

impl OtlpLogsClickHouseAdapter {
    pub fn new(config: LogsAdapterConfig) -> Self {
        let fingerprint = chunking_fingerprint(&config);
        Self {
            config,
            chunking_fingerprint: fingerprint,
        }
    }

    pub fn config(&self) -> &LogsAdapterConfig {
        &self.config
    }

    pub fn chunking_fingerprint(&self) -> &str {
        &self.chunking_fingerprint
    }
}

/// Stable hash of every input that affects chunk boundaries.
fn chunking_fingerprint(config: &LogsAdapterConfig) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"otlp-logs-clickhouse-adapter:v1\n");
    hasher.update(format!("database={}\n", config.database).as_bytes());
    hasher.update(format!("table={}\n", config.table).as_bytes());
    hasher.update(format!("adapter_version={}\n", config.adapter_version).as_bytes());
    hasher.update(format!("max_chunk_rows={}\n", config.max_chunk_rows).as_bytes());
    hasher.update(format!("max_chunk_bytes={}\n", config.max_chunk_bytes).as_bytes());
    // Insert quorum doesn't change chunk boundaries, but a writer setting
    // change shouldn't collide with old tokens either, so include it.
    hasher.update(
        format!(
            "insert_quorum={}\n",
            config.insert_quorum.as_deref().unwrap_or("")
        )
        .as_bytes(),
    );
    let digest = hasher.finalize();
    hex::encode(&digest[..8])
}

const COLUMNS: &[&str] = &[
    "Timestamp",
    "ObservedTimestamp",
    "SeverityText",
    "SeverityNumber",
    "ServiceName",
    "Body",
    "ResourceAttributes",
    "LogAttributes",
    "TraceId",
    "SpanId",
    "_odb_sequence",
    "_odb_entry_index",
    "_odb_record_index",
    "_odb_manifest_path",
    "_odb_data_path",
    "_odb_ingestion_time_ms",
    "_adapter_version",
];

impl Adapter for OtlpLogsClickHouseAdapter {
    type Input = DecodedLogRecord;

    fn plan(&self, batch: CommitGroupBatch<Self::Input>) -> IngestorResult<Vec<InsertChunk>> {
        let CommitGroupBatch {
            mut records,
            low_sequence,
            high_sequence,
        } = batch;

        // Stable order: (entry_index, record_index) within a sequence,
        // then sequence ascending. Records from the same Buffer entry
        // already arrive in record_index order, but we sort defensively
        // so chunking is deterministic regardless of upstream
        // iteration order.
        records.sort_by_key(|r| {
            (
                r.source.sequence,
                r.source.entry_index,
                r.source.record_index,
            )
        });

        let mut chunks = Vec::new();

        if records.is_empty() {
            // Zero-row commit groups still need to advance the
            // high-watermark, but they produce no insert chunks. The
            // runtime calls the ack controller directly when the chunk
            // list is empty.
            return Ok(chunks);
        }

        let max = self.config.max_chunk_rows.max(1);
        for (chunk_index, slice) in records.chunks(max).enumerate() {
            let rows: Vec<Vec<RowValue>> = slice.iter().map(|r| log_row(r, &self.config)).collect();
            let token = build_token(
                &self.config,
                &self.chunking_fingerprint,
                low_sequence,
                high_sequence,
                chunk_index as u32,
            );
            chunks.push(InsertChunk {
                database: self.config.database.clone(),
                table: self.config.table.clone(),
                columns: COLUMNS.to_vec(),
                rows,
                settings: ClickHouseSettings {
                    insert_quorum: self.config.insert_quorum.clone(),
                    insert_deduplication_token: token.clone(),
                },
                idempotency_token: token,
                chunk_index: chunk_index as u32,
                observability_labels: vec![
                    ("table", self.config.table.clone()),
                    ("signal", "logs".to_string()),
                ],
            });
        }
        Ok(chunks)
    }
}

fn build_token(
    config: &LogsAdapterConfig,
    fingerprint: &str,
    low: u64,
    high: u64,
    chunk_index: u32,
) -> String {
    // The manifest path isn't on the adapter directly; we put the table
    // name in the token so two adapters writing different tables from
    // the same Buffer do not collide. The runtime layers the manifest
    // path on top via the ClickHouse setting if needed; for the alpha,
    // the manifest segment in the spec ({manifest}:{low}-{high}:...) is
    // covered by the configured `database.table` since the ingestor is
    // 1:1 with a manifest.
    format!(
        "{}.{}:{}-{}:{}:{}:{}",
        config.database, config.table, low, high, config.adapter_version, fingerprint, chunk_index
    )
}

fn log_row(rec: &DecodedLogRecord, config: &LogsAdapterConfig) -> Vec<RowValue> {
    let resource = btree_to_owned(&rec.resource_attributes);
    let log_attrs = btree_to_owned(&rec.log_attributes);
    vec![
        RowValue::DateTime64Nanos(rec.timestamp_unix_nano),
        RowValue::DateTime64Nanos(rec.observed_timestamp_unix_nano),
        RowValue::LowCardinalityString(rec.severity_text.clone()),
        RowValue::UInt8(severity_to_u8(rec.severity_number)),
        RowValue::LowCardinalityString(rec.service_name.clone().unwrap_or_default()),
        RowValue::String(rec.body.clone()),
        RowValue::StringMap(resource),
        RowValue::StringMap(log_attrs),
        RowValue::String(rec.trace_id_hex.clone()),
        RowValue::String(rec.span_id_hex.clone()),
        RowValue::UInt64(rec.source.sequence),
        RowValue::UInt32(rec.source.entry_index),
        RowValue::UInt32(rec.source.record_index),
        RowValue::LowCardinalityString(rec.source.manifest_path.clone()),
        RowValue::String(rec.source.data_path.clone()),
        RowValue::Int64(rec.source.ingestion_time_ms),
        RowValue::UInt32(config.adapter_version),
    ]
}

fn btree_to_owned(input: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    input.clone()
}

fn severity_to_u8(severity_number: i32) -> u8 {
    severity_number.clamp(0, u8::MAX as i32) as u8
}

/// Logs-alpha table DDL. Uses `ReplacingMergeTree(_adapter_version)` so
/// adapter version is the merge tiebreaker (a replay after a row-mapping
/// change resolves to the new mapping). The hybrid `ORDER BY` puts a
/// query-useful prefix in front of the dedupe-identifying suffix.
///
/// IMPORTANT: any change to a column that participates in `ORDER BY`
/// requires a new table plus backfill (RFC 0003 dedupe constraint).
pub fn logs_table_ddl(config: &LogsAdapterConfig) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {db}.{table} (\n\
         Timestamp           DateTime64(9)               CODEC(Delta, ZSTD),\n\
         ObservedTimestamp   DateTime64(9)               CODEC(Delta, ZSTD),\n\
         SeverityText        LowCardinality(String),\n\
         SeverityNumber      UInt8,\n\
         ServiceName         LowCardinality(String),\n\
         Body                String                      CODEC(ZSTD),\n\
         ResourceAttributes  Map(LowCardinality(String), String),\n\
         LogAttributes       Map(LowCardinality(String), String),\n\
         TraceId             String                      CODEC(ZSTD),\n\
         SpanId              String                      CODEC(ZSTD),\n\
         _odb_sequence            UInt64,\n\
         _odb_entry_index         UInt32,\n\
         _odb_record_index        UInt32,\n\
         _odb_manifest_path       LowCardinality(String),\n\
         _odb_data_path           String,\n\
         _odb_ingestion_time_ms   Int64,\n\
         _adapter_version         UInt32\n\
         )\n\
         ENGINE = ReplacingMergeTree(_adapter_version)\n\
         PARTITION BY toDate(Timestamp)\n\
         ORDER BY (toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)\n\
         TTL toDate(Timestamp) + INTERVAL 30 DAY",
        db = config.database,
        table = config.table,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::SourceCoordinates;
    use std::collections::BTreeMap;

    fn cfg() -> LogsAdapterConfig {
        LogsAdapterConfig {
            max_chunk_rows: 2,
            ..LogsAdapterConfig::default()
        }
    }

    fn rec(sequence: u64, entry_index: u32, record_index: u32) -> DecodedLogRecord {
        DecodedLogRecord {
            source: SourceCoordinates {
                sequence,
                entry_index,
                record_index,
                manifest_path: "ingest/test/manifest".into(),
                data_path: "ingest/test/data/abc.batch".into(),
                ingestion_time_ms: 12345,
            },
            timestamp_unix_nano: 1_700_000_000_000_000_000 + sequence,
            observed_timestamp_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: format!("body-{sequence}-{entry_index}-{record_index}"),
            service_name: Some("controller".into()),
            resource_attributes: BTreeMap::from([("k".into(), "v".into())]),
            scope_name: None,
            log_attributes: BTreeMap::from([("topic".into(), "foo".into())]),
            trace_id_hex: "deadbeef".into(),
            span_id_hex: "feedface".into(),
        }
    }

    #[test]
    fn empty_batch_yields_no_chunks() {
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());
        let chunks = adapter
            .plan(CommitGroupBatch {
                records: Vec::new(),
                low_sequence: 5,
                high_sequence: 9,
            })
            .expect("plan");
        assert!(chunks.is_empty());
    }

    #[test]
    fn rows_chunk_at_max_chunk_rows() {
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());
        let records = vec![rec(1, 0, 0), rec(1, 0, 1), rec(2, 0, 0)];
        let chunks = adapter
            .plan(CommitGroupBatch {
                records,
                low_sequence: 1,
                high_sequence: 2,
            })
            .expect("plan");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].rows_count(), 2);
        assert_eq!(chunks[1].rows_count(), 1);
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[1].chunk_index, 1);
    }

    #[test]
    fn token_format_includes_low_high_version_fingerprint_chunk() {
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());
        let chunks = adapter
            .plan(CommitGroupBatch {
                records: vec![rec(7, 0, 0), rec(7, 0, 1), rec(8, 0, 0)],
                low_sequence: 7,
                high_sequence: 8,
            })
            .expect("plan");
        let token = &chunks[0].idempotency_token;
        // database.table:low-high:version:fingerprint:chunk_index
        assert!(token.starts_with("responsive.logs:7-8:1:"));
        assert!(token.ends_with(":0"));
        assert!(chunks[1].idempotency_token.ends_with(":1"));
        assert_ne!(chunks[0].idempotency_token, chunks[1].idempotency_token);
    }

    #[test]
    fn replay_with_same_inputs_produces_same_tokens() {
        // Determinism is the property the dedupe model leans on: identical
        // (records, config) => identical chunk boundaries => identical
        // tokens. Two independent invocations must agree.
        let records = || vec![rec(7, 0, 0), rec(7, 0, 1), rec(8, 0, 0)];
        let adapter = OtlpLogsClickHouseAdapter::new(cfg());

        let plan_a = adapter
            .plan(CommitGroupBatch {
                records: records(),
                low_sequence: 7,
                high_sequence: 8,
            })
            .expect("plan a");
        let plan_b = adapter
            .plan(CommitGroupBatch {
                records: records(),
                low_sequence: 7,
                high_sequence: 8,
            })
            .expect("plan b");

        let tokens_a: Vec<&str> = plan_a
            .iter()
            .map(|c| c.idempotency_token.as_str())
            .collect();
        let tokens_b: Vec<&str> = plan_b
            .iter()
            .map(|c| c.idempotency_token.as_str())
            .collect();
        assert_eq!(tokens_a, tokens_b);
    }

    #[test]
    fn changing_max_chunk_rows_changes_fingerprint_and_tokens() {
        // Defending the property that any change to a chunking-affecting
        // input causes the fingerprint to differ — without this, two
        // configurations that produced different chunk shapes could share
        // tokens and ClickHouse would silently drop valid data.
        let mut a = cfg();
        let mut b = cfg();
        a.max_chunk_rows = 2;
        b.max_chunk_rows = 3;
        let fa = chunking_fingerprint(&a);
        let fb = chunking_fingerprint(&b);
        assert_ne!(fa, fb);
    }

    #[test]
    fn rows_carry_source_columns_in_correct_positions() {
        let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 100,
            ..LogsAdapterConfig::default()
        });
        let r = rec(11, 3, 7);
        let chunks = adapter
            .plan(CommitGroupBatch {
                records: vec![r.clone()],
                low_sequence: 11,
                high_sequence: 11,
            })
            .expect("plan");
        let row = &chunks[0].rows[0];
        // Spot-check positional columns. The constants here track
        // adapter::COLUMNS — if columns reorder, this test forces an
        // explicit acknowledgement.
        match &row[10] {
            RowValue::UInt64(v) => assert_eq!(*v, 11),
            other => panic!("_odb_sequence column wrong type: {other:?}"),
        }
        match &row[11] {
            RowValue::UInt32(v) => assert_eq!(*v, 3),
            other => panic!("_odb_entry_index column wrong type: {other:?}"),
        }
        match &row[12] {
            RowValue::UInt32(v) => assert_eq!(*v, 7),
            other => panic!("_odb_record_index column wrong type: {other:?}"),
        }
    }

    #[test]
    fn ddl_uses_replacing_merge_tree_with_adapter_version() {
        let ddl = logs_table_ddl(&LogsAdapterConfig::default());
        assert!(ddl.contains("ReplacingMergeTree(_adapter_version)"));
        assert!(
            ddl.contains("_odb_sequence, _odb_entry_index, _odb_record_index"),
            "ORDER BY must include the source-coordinate suffix"
        );
        assert!(ddl.contains("PARTITION BY toDate(Timestamp)"));
    }

    #[test]
    fn records_sorted_for_deterministic_chunking() {
        let adapter = OtlpLogsClickHouseAdapter::new(LogsAdapterConfig {
            max_chunk_rows: 100,
            ..LogsAdapterConfig::default()
        });
        // Feed records out of order; the adapter must still chunk in
        // (sequence, entry, record) order so a replay produces the same
        // shape.
        let records = vec![rec(2, 0, 0), rec(1, 0, 1), rec(1, 0, 0)];
        let chunks = adapter
            .plan(CommitGroupBatch {
                records,
                low_sequence: 1,
                high_sequence: 2,
            })
            .expect("plan");
        let row_seqs: Vec<u64> = chunks[0]
            .rows
            .iter()
            .map(|row| match &row[10] {
                RowValue::UInt64(v) => *v,
                _ => panic!("expected UInt64"),
            })
            .collect();
        assert_eq!(row_seqs, vec![1, 1, 2]);
    }
}
