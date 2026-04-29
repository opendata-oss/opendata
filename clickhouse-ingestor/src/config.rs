//! Figment-based configuration for the ingestor binary.
//!
//! YAML config file (typically mounted via ConfigMap) gets merged with
//! `INGESTOR__` env vars. Credentials only come from env so they never
//! land in the rendered ConfigMap.
//!
//! Layout (excerpt):
//!
//! ```yaml
//! buffer:
//!   manifest_path: ingest/otel/logs/manifest
//!   data_prefix:   ingest/otel/logs/data
//!   object_store:
//!     type: Aws
//!     region: us-west-2
//!     bucket: responsive-prod-opendata-otel-logs-us-west-2
//!
//! clickhouse:
//!   endpoint: https://example.clickhouse.cloud:8443
//!   database: responsive
//!   table: logs
//!   insert_quorum: auto
//!
//! runtime:
//!   dry_run: true
//!   poll_interval_ms: 250
//!   retry_max_attempts: 6
//!   retry_initial_backoff_ms: 100
//!
//! commit_group:
//!   max_rows: 100000
//!   max_bytes: 33554432
//!   max_age_ms: 1000
//!
//! ack:
//!   policy: every_commit_group
//!
//! adapter:
//!   adapter_version: 1
//!   max_chunk_rows: 100000
//!   max_chunk_bytes: 33554432
//! ```

use std::path::Path;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Env, Format, Yaml};
use serde::{Deserialize, Serialize};

use crate::ack::AckFlushPolicy;
use crate::adapter::logs::LogsAdapterConfig;
use crate::commit_group::CommitGroupThresholds;
use crate::error::{IngestorError, IngestorResult};
use crate::writer::WriterConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorConfig {
    pub buffer: BufferSection,
    pub clickhouse: ClickHouseSection,
    pub runtime: RuntimeSection,
    pub commit_group: CommitGroupSection,
    pub ack: AckSection,
    pub adapter: AdapterSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferSection {
    pub manifest_path: String,
    pub data_prefix: String,
    pub object_store: common::ObjectStoreConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseSection {
    pub endpoint: String,
    pub database: String,
    pub table: String,
    #[serde(default)]
    pub insert_quorum: Option<String>,
    /// Optional non-secret user; password and any secret user override
    /// come from env (`INGESTOR__CLICKHOUSE__USER`,
    /// `INGESTOR__CLICKHOUSE__PASSWORD`).
    #[serde(default = "default_user")]
    pub user: String,
    #[serde(default)]
    pub password: String,
}

fn default_user() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSection {
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_retry_attempts")]
    pub retry_max_attempts: u32,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_initial_backoff_ms: u64,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

fn default_dry_run() -> bool {
    true
}
fn default_poll_interval_ms() -> u64 {
    250
}
fn default_retry_attempts() -> u32 {
    6
}
fn default_retry_backoff_ms() -> u64 {
    100
}
fn default_request_timeout_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitGroupSection {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_age_ms: u64,
}

impl Default for CommitGroupSection {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 32 * 1024 * 1024,
            max_age_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckSection {
    pub policy: AckPolicyKind,
    #[serde(default)]
    pub n: Option<u32>,
}

impl Default for AckSection {
    fn default() -> Self {
        Self {
            policy: AckPolicyKind::EveryCommitGroup,
            n: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckPolicyKind {
    EveryCommitGroup,
    EveryN,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterSection {
    pub adapter_version: u32,
    pub max_chunk_rows: usize,
    pub max_chunk_bytes: usize,
    /// Whether to apply `insert_deduplication_token` on inserts. True
    /// for production replicated tables; set false for non-replicated
    /// targets (single-node dev/test).
    #[serde(default = "default_apply_dedup_token")]
    pub apply_deduplication_token: bool,
}

fn default_apply_dedup_token() -> bool {
    true
}

impl Default for AdapterSection {
    fn default() -> Self {
        Self {
            adapter_version: 1,
            max_chunk_rows: 100_000,
            max_chunk_bytes: 32 * 1024 * 1024,
            apply_deduplication_token: true,
        }
    }
}

impl IngestorConfig {
    /// Load config from a YAML file path, merging `INGESTOR__` env vars
    /// (with `__` as the section separator).
    pub fn load(path: impl AsRef<Path>) -> IngestorResult<Self> {
        let cfg: Self = Figment::new()
            .merge(Yaml::file(path))
            .merge(Env::prefixed("INGESTOR__").split("__"))
            .extract()
            .map_err(|e| IngestorError::Config(e.to_string()))?;
        Ok(cfg)
    }

    pub fn commit_group_thresholds(&self) -> CommitGroupThresholds {
        CommitGroupThresholds {
            max_rows: self.commit_group.max_rows,
            max_bytes: self.commit_group.max_bytes,
            max_age: Duration::from_millis(self.commit_group.max_age_ms),
        }
    }

    pub fn ack_flush_policy(&self) -> AckFlushPolicy {
        match self.ack.policy {
            AckPolicyKind::EveryCommitGroup => AckFlushPolicy::EveryCommitGroup,
            AckPolicyKind::EveryN => AckFlushPolicy::EveryN {
                n: self.ack.n.unwrap_or(1).max(1),
            },
        }
    }

    pub fn writer_config(&self) -> WriterConfig {
        WriterConfig {
            endpoint: self.clickhouse.endpoint.clone(),
            user: self.clickhouse.user.clone(),
            password: self.clickhouse.password.clone(),
            request_timeout: Duration::from_secs(self.runtime.request_timeout_secs),
            max_attempts: self.runtime.retry_max_attempts,
            initial_backoff: Duration::from_millis(self.runtime.retry_initial_backoff_ms),
        }
    }

    pub fn logs_adapter_config(&self) -> LogsAdapterConfig {
        LogsAdapterConfig {
            database: self.clickhouse.database.clone(),
            table: self.clickhouse.table.clone(),
            adapter_version: self.adapter.adapter_version,
            max_chunk_rows: self.adapter.max_chunk_rows,
            max_chunk_bytes: self.adapter.max_chunk_bytes,
            insert_quorum: self.clickhouse.insert_quorum.clone(),
            apply_deduplication_token: self.adapter.apply_deduplication_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_render_via_serde() {
        let yaml = r#"
buffer:
  manifest_path: ingest/otel/logs/manifest
  data_prefix: ingest/otel/logs/data
  object_store:
    type: InMemory

clickhouse:
  endpoint: http://localhost:8123
  database: responsive
  table: logs

runtime:
  dry_run: true
  poll_interval_ms: 250
  retry_max_attempts: 6
  retry_initial_backoff_ms: 100
  request_timeout_secs: 30

commit_group:
  max_rows: 100000
  max_bytes: 33554432
  max_age_ms: 1000

ack:
  policy: every_commit_group

adapter:
  adapter_version: 1
  max_chunk_rows: 100000
  max_chunk_bytes: 33554432
"#;
        let cfg: IngestorConfig = serde_yaml::from_str(yaml).expect("parse");
        let thresholds = cfg.commit_group_thresholds();
        assert_eq!(thresholds.max_rows, 100_000);
        assert_eq!(thresholds.max_age, Duration::from_secs(1));

        let writer = cfg.writer_config();
        assert_eq!(writer.endpoint, "http://localhost:8123");
        assert_eq!(writer.max_attempts, 6);

        match cfg.ack_flush_policy() {
            AckFlushPolicy::EveryCommitGroup => {}
            other => panic!("unexpected policy: {other:?}"),
        }

        let adapter = cfg.logs_adapter_config();
        assert_eq!(adapter.database, "responsive");
        assert_eq!(adapter.table, "logs");
        assert_eq!(adapter.adapter_version, 1);
    }

    #[test]
    fn ack_every_n_clamps_zero_to_one() {
        let yaml = r#"
buffer:
  manifest_path: m
  data_prefix: d
  object_store:
    type: InMemory
clickhouse:
  endpoint: http://x:8123
  database: db
  table: t
runtime:
  dry_run: true
  poll_interval_ms: 250
  retry_max_attempts: 6
  retry_initial_backoff_ms: 100
  request_timeout_secs: 30
commit_group:
  max_rows: 1
  max_bytes: 1
  max_age_ms: 1
ack:
  policy: every_n
  n: 0
adapter:
  adapter_version: 1
  max_chunk_rows: 1
  max_chunk_bytes: 1
"#;
        let cfg: IngestorConfig = serde_yaml::from_str(yaml).expect("parse");
        match cfg.ack_flush_policy() {
            AckFlushPolicy::EveryN { n } => assert_eq!(n, 1),
            other => panic!("unexpected: {other:?}"),
        }
    }
}
