//! OpenData TimeSeries - A time series database for metrics.
//!
//! OpenData TimeSeries provides a simple, Prometheus-like data model for storing
//! and querying time series metrics. It is designed for high-throughput ingestion
//! and efficient range queries.
//!
//! # Architecture
//!
//! The database is built on SlateDB's LSM tree with time-based bucketing. Each
//! time bucket contains an inverted index for label-based queries and compressed
//! sample storage for efficient retrieval.
//!
//! # Key Concepts
//!
//! - **TimeSeriesDb**: The main entry point providing write operations.
//! - **Series**: A time series identified by labels, containing timestamped samples.
//! - **Label**: A key-value pair that identifies a time series.
//! - **Sample**: A single data point with a timestamp and value.
//!
//! # Example
//!
//! ```
//! # use timeseries::{TimeSeriesDb, Config, Series};
//! # use common::StorageConfig;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
//! let ts = TimeSeriesDb::open(config).await?;
//!
//! let series = Series::builder("http_requests_total")
//!     .label("method", "GET")
//!     .label("status", "200")
//!     .sample_now(1.0)
//!     .build();
//!
//! ts.write(vec![series]).await?;
//! # Ok(())
//! # }
//! ```

// Internal modules are shared with the binary target (main.rs) which
// re-declares them. Suppress dead_code warnings for the lib target.
#![allow(dead_code)]

// Internal modules
mod delta;
mod flusher;
mod index;
mod minitsdb;
mod promql;
mod query;
mod serde;
#[cfg(feature = "http-server")]
mod server;
mod storage;
#[cfg(test)]
mod test_utils;
mod tsdb;
mod util;

#[cfg(feature = "bench-internals")]
mod bench_api;

#[cfg(feature = "otel")]
pub mod otel;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

// Public API modules
mod config;
pub(crate) mod error;
pub(crate) mod model;
mod reader;
mod timeseries;

// Public re-exports
#[cfg(feature = "bench-internals")]
pub use bench_api::SubqueryLabelCacheHarness;
#[cfg(feature = "bench-internals")]
pub use bench_api::WarmRangeQueryHarness;
pub use config::Config;
pub use error::{Error, QueryError, Result};
pub use model::{
    InstantSample, Label, Labels, MetricMetadata, MetricType, QueryOptions, QueryValue,
    RangeSample, STALE_NAN, Sample, Series, SeriesBuilder, Temporality, is_stale_nan,
};
#[cfg(feature = "otel")]
pub use otel::{OtelConfig, OtelConverter};
pub use reader::TimeSeriesDbReader;
pub use timeseries::TimeSeriesDb;
