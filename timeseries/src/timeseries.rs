//! Core TimeSeriesDb implementation with write API.
//!
//! This module provides the [`TimeSeriesDb`] struct, the primary entry point for
//! interacting with OpenData TimeSeries. It exposes write operations for
//! ingesting time series data.

use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common::{StorageRuntime, StorageSemantics, create_storage};

use crate::config::Config;
use crate::error::{QueryError, Result};
use crate::model::{Labels, MetricMetadata, QueryValue, RangeSample, Series};
use crate::storage::merge_operator::OpenTsdbMergeOperator;
use crate::tsdb::{
    Tsdb, TsdbReadEngine, eval_query_range_bounds, find_label_values_in_range,
    find_labels_in_range, find_series_in_range,
};

/// A time series database for storing and querying metrics.
///
/// `TimeSeriesDb` provides a high-level API for ingesting Prometheus-style
/// metrics. It handles internal details like time bucketing, series
/// deduplication, and storage management automatically.
///
/// # Example
///
/// ```
/// # use timeseries::{TimeSeriesDb, Config, Series};
/// # use common::StorageConfig;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
/// let ts = TimeSeriesDb::open(config).await?;
///
/// let series = Series::builder("http_requests_total")
///     .label("method", "GET")
///     .label("status", "200")
///     .sample_now(1.0)
///     .build();
///
/// ts.write(vec![series]).await?;
/// # Ok(())
/// # }
/// ```
pub struct TimeSeriesDb {
    // Internal Tsdb - not exposed
    tsdb: Tsdb,
}

impl TimeSeriesDb {
    /// Opens or creates a time series database with the given configuration.
    ///
    /// This is the primary entry point for creating a `TimeSeriesDb` instance.
    /// The configuration specifies the storage backend and operational parameters.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying storage backend and settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    ///
    /// # Example
    ///
    /// ```
    /// # use timeseries::{TimeSeriesDb, Config};
    /// # use common::StorageConfig;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
    /// let ts = TimeSeriesDb::open(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(config: Config) -> Result<Self> {
        let storage = create_storage(
            &config.storage,
            StorageRuntime::new(),
            StorageSemantics::new().with_merge_operator(Arc::new(OpenTsdbMergeOperator)),
        )
        .await?;
        let tsdb = Tsdb::new(storage);
        Ok(Self { tsdb })
    }

    /// Writes one or more time series.
    ///
    /// This is the primary write method. It accepts a batch of series,
    /// each containing labels and one or more samples. The method returns
    /// when the data has been accepted for ingestion (but not necessarily
    /// flushed to durable storage).
    ///
    /// # Atomicity
    ///
    /// This operation is atomic: either all series in the batch are accepted,
    /// or none are. This matches the behavior of `LogDb::append()`.
    ///
    /// # Series Identification
    ///
    /// Each unique combination of labels identifies a distinct time series.
    /// The label set must include a `__name__` label for the metric name.
    ///
    /// # Ordering
    ///
    /// Samples within a series should be in timestamp order, but out-of-order
    /// samples are accepted. Duplicate timestamps for the same series will
    /// overwrite previous values.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use timeseries::{TimeSeriesDb, Config, Series};
    /// # use common::StorageConfig;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
    /// # let ts = TimeSeriesDb::open(config).await?;
    /// let series = vec![
    ///     Series::builder("cpu_usage")
    ///         .label("host", "server1")
    ///         .sample(1700000000000, 0.75)
    ///         .sample(1700000001000, 0.82)
    ///         .build(),
    ///     Series::builder("cpu_usage")
    ///         .label("host", "server2")
    ///         .sample(1700000000000, 0.45)
    ///         .build(),
    /// ];
    ///
    /// ts.write(series).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(&self, series: Vec<Series>) -> Result<()> {
        self.tsdb.ingest_samples(series).await
    }

    // ── Read / Query API (RFC 0003) ──────────────────────────────────

    /// Evaluates an instant PromQL query at a single point in time.
    ///
    /// If `time` is `None`, the current wall-clock time is used.
    pub async fn query(
        &self,
        query: &str,
        time: Option<SystemTime>,
    ) -> std::result::Result<QueryValue, QueryError> {
        self.tsdb
            .eval_query(query, time, &crate::model::QueryOptions::default())
            .await
    }

    /// Evaluates a range PromQL query over a time interval.
    pub async fn query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime>,
        step: Duration,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        // Route through shared range helpers so bound conversion happens once.
        eval_query_range_bounds(
            &self.tsdb,
            query,
            range,
            step,
            &crate::model::QueryOptions::default(),
        )
        .await
    }

    /// Returns the set of label-sets matching the given series matchers.
    pub async fn series(
        &self,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        find_series_in_range(&self.tsdb, matchers, range).await
    }

    /// Returns the set of label names matching the given matchers.
    pub async fn labels(
        &self,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        find_labels_in_range(&self.tsdb, matchers, range).await
    }

    /// Returns the set of values for a given label name.
    pub async fn label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        find_label_values_in_range(&self.tsdb, label_name, matchers, range).await
    }

    /// Returns metric metadata, optionally filtered to a single metric.
    pub async fn metadata(
        &self,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        self.tsdb.find_metadata(metric).await
    }

    /// Forces flush of all pending data to durable storage.
    ///
    /// Normally data is flushed according to the configured `flush_interval`,
    /// but this method can be used to ensure durability immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails due to storage issues.
    pub async fn flush(&self) -> Result<()> {
        self.tsdb.flush().await
    }

    /// Closes the time series database, flushing any pending data and releasing
    /// resources.
    ///
    /// All written data is flushed to durable storage before the database is
    /// closed. For SlateDB-backed storage, this also releases the database
    /// fence.
    pub async fn close(self) -> Result<()> {
        self.tsdb.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Sample, Series};
    use common::StorageConfig;
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };

    #[tokio::test]
    async fn close_without_explicit_flush_guarantees_durability() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "ts-data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp_dir.path().to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
        });

        // Write a series and close without calling flush()
        {
            let tsdb = TimeSeriesDb::open(Config {
                storage: storage.clone(),
                ..Default::default()
            })
            .await
            .unwrap();

            tsdb.write(vec![Series::new(
                "cpu_usage",
                vec![Label::new("host", "server1")],
                vec![Sample::new(3_900_000, 0.42)],
            )])
            .await
            .unwrap();

            tsdb.close().await.unwrap();
        }

        // Reopen and verify the series survived
        let tsdb = TimeSeriesDb::open(Config {
            storage: storage.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

        let series = tsdb
            .series(&["{__name__=\"cpu_usage\"}"], ..)
            .await
            .unwrap();

        assert!(
            !series.is_empty(),
            "expected series to survive close without explicit flush"
        );
    }
}
