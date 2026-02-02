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
//! ```ignore
//! use timeseries::{TimeSeriesDb, Config, Series};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let ts = TimeSeriesDb::open(Config::default()).await?;
//!
//!     let series = Series::builder("http_requests_total")
//!         .label("method", "GET")
//!         .label("status", "200")
//!         .sample_now(1.0)
//!         .build();
//!
//!     ts.write(vec![series]).await?;
//!     Ok(())
//! }
//! ```

// Internal modules
mod delta;
mod head;
mod index;
mod minitsdb;
mod promql;
mod query;
mod serde;
mod storage;
#[cfg(test)]
mod test_utils;
mod tsdb;
mod util;

// Public API modules
mod config;
pub(crate) mod error;
pub(crate) mod model;
mod timeseries;

// Public re-exports
pub use config::{Config, WriteOptions};
pub use error::{Error, Result};
pub use model::{Label, MetricType, Sample, Series, SeriesBuilder, Temporality};
pub use timeseries::TimeSeriesDb;
