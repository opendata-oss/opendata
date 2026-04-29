//! `clickhouse-ingestor` — reusable Rust runtime that consumes OpenData
//! Buffer batches and writes them into ClickHouse.
//!
//! Layered following RFC 0003:
//!
//! ```text
//! BufferConsumerRuntime
//!   -> MetadataEnvelopeDecoder    (per-entry envelope parsing)
//!   -> SignalDecoder              (e.g. OtlpLogsDecoder)
//!   -> CommitGroup                (coalesce records across batches)
//!   -> Adapter                    (e.g. OtlpLogsClickHouseAdapter)
//!      -> Vec<InsertChunk>        (deterministic chunking, per-chunk token)
//!   -> ClickHouseWriter           (sync inserts, classified retry)
//!   -> AckController              (range ack, flush)
//! ```
//!
//! The infra/app boundary is meaningful: the runtime, envelope decoder,
//! commit group, ack controller, and writer are signal- and table-shape-
//! agnostic; the signal decoder and adapter are signal-specific.

pub mod ack;
pub mod adapter;
pub mod bench;
pub mod commit_group;
pub mod config;
pub mod envelope;
pub mod error;
pub mod metrics;
pub mod runtime;
pub mod signal;
pub mod source;
pub mod writer;

pub use ack::{AckController, AckFlushPolicy};
pub use adapter::logs::{LogsAdapterConfig, OtlpLogsClickHouseAdapter, logs_table_ddl};
pub use adapter::{Adapter, ClickHouseSettings, InsertChunk, RowValue};
pub use commit_group::{CommitGroup, CommitGroupBatch, CommitGroupThresholds};
pub use config::IngestorConfig;
pub use envelope::{
    ConfiguredEnvelope, EnvelopeError, MetadataEnvelope, PayloadEncoding, SignalType,
    decode_envelopes, validate_consistent,
};
pub use error::{IngestorError, IngestorResult};
pub use runtime::{BufferConsumerRuntime, RuntimeOptions};
pub use signal::{
    DecodedLogRecord, DecodedLogs, OtlpLogsDecoder, SignalDecoder, SourceCoordinates,
};
pub use source::{RawBufferBatch, RawEntry, split_into_raw_entries};
pub use writer::{ClickHouseWriter, WriterError, WriterErrorClass};
