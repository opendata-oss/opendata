use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common::coordinator::{Durability, WriteCoordinator, WriteCoordinatorConfig, WriteError};
use common::storage::StorageSnapshot;
use common::{Storage, StorageRead};

const WRITE_CHANNEL: &str = "write";

use crate::delta::{TsdbContext, TsdbWriteDelta};
use crate::error::Error;
use crate::flusher::TsdbFlusher;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::model::{Label, Sample, Series, SeriesId, TimeBucket};
use crate::query::BucketQueryReader;
use crate::serde::key::TimeSeriesKey;
use crate::serde::timeseries::TimeSeriesIterator;
use crate::storage::OpenTsdbStorageReadExt;
use crate::util::Result;

pub(crate) struct MiniQueryReader {
    bucket: TimeBucket,
    snapshot: Arc<dyn StorageRead>,
}

#[async_trait]
impl BucketQueryReader for MiniQueryReader {
    async fn forward_index(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let forward_index = self
            .snapshot
            .get_forward_index_series(&self.bucket, series_ids)
            .await?;
        Ok(Box::new(forward_index))
    }

    async fn all_forward_index(
        &self,
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let forward_index = self.snapshot.get_forward_index(self.bucket).await?;
        Ok(Box::new(forward_index))
    }

    async fn inverted_index(
        &self,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let inverted_index = self
            .snapshot
            .get_inverted_index_terms(&self.bucket, terms)
            .await?;
        Ok(Box::new(inverted_index))
    }

    async fn all_inverted_index(
        &self,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let inverted_index = self.snapshot.get_inverted_index(self.bucket).await?;
        Ok(Box::new(inverted_index))
    }

    async fn label_values(&self, label_name: &str) -> Result<Vec<String>> {
        self.snapshot
            .get_label_values(&self.bucket, label_name)
            .await
    }

    async fn samples(
        &self,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        let storage_key = TimeSeriesKey {
            time_bucket: self.bucket.start,
            bucket_size: self.bucket.size,
            series_id,
        };
        let record = self.snapshot.get(storage_key.encode()).await?;

        match record {
            Some(record) => {
                let iter = TimeSeriesIterator::new(record.value.as_ref())
                    .ok_or_else(|| Error::Internal("Invalid timeseries data in storage".into()))?;

                let samples: Vec<Sample> = iter
                    .filter_map(|r| r.ok())
                    // Filter by time range: timestamp > start_ms && timestamp <= end_ms
                    // Following PromQL lookback window semantics with exclusive start
                    .filter(|s| s.timestamp_ms > start_ms && s.timestamp_ms <= end_ms)
                    .collect();

                Ok(samples)
            }
            None => Ok(Vec::new()),
        }
    }
}

pub(crate) struct MiniTsdb {
    bucket: TimeBucket,
    write_coordinator: WriteCoordinator<TsdbWriteDelta, TsdbFlusher>,
}

impl MiniTsdb {
    /// Returns a reference to the time bucket
    pub(crate) fn bucket(&self) -> &TimeBucket {
        &self.bucket
    }

    /// Create a query reader for read operations.
    pub(crate) fn query_reader(&self) -> MiniQueryReader {
        let view = self.write_coordinator.view();
        MiniQueryReader {
            bucket: self.bucket,
            snapshot: view.snapshot.clone(),
        }
    }

    pub(crate) async fn load(bucket: TimeBucket, storage: Arc<dyn Storage>) -> Result<Self> {
        let snapshot = storage.snapshot().await?;

        let mut series_dict = HashMap::new();
        let next_series_id = snapshot
            .load_series_dictionary(&bucket, |fingerprint, series_id| {
                series_dict.insert(fingerprint, series_id);
            })
            .await?;

        let context = TsdbContext {
            bucket,
            series_dict: Arc::new(series_dict),
            next_series_id,
        };

        let flusher = TsdbFlusher {
            storage: storage.clone(),
        };

        let initial_snapshot: Arc<dyn StorageSnapshot> = storage
            .snapshot()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut write_coordinator = WriteCoordinator::new(
            WriteCoordinatorConfig::default(),
            vec![WRITE_CHANNEL.to_string()],
            context,
            initial_snapshot,
            flusher,
        );
        write_coordinator.start();

        Ok(Self {
            bucket,
            write_coordinator,
        })
    }

    /// Ingest a batch of series with samples in a single operation.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>()
        )
    )]
    pub(crate) async fn ingest_batch(&self, series_list: &[Series]) -> Result<()> {
        let total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>();

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Starting MiniTsdb batch ingest"
        );

        let handle = self.write_coordinator.handle(WRITE_CHANNEL);
        let mut write_handle = handle
            .write(series_list.to_vec())
            .await
            .map_err(map_write_error)?;

        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(map_write_error)?;

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Completed MiniTsdb batch ingest"
        );

        Ok(())
    }

    /// Ingest a single series with samples.
    pub(crate) async fn ingest(&self, series: &Series) -> Result<()> {
        self.ingest_batch(std::slice::from_ref(series)).await
    }

    /// Flush pending data to storage, making it durable and visible to queries.
    pub(crate) async fn flush(&self) -> Result<()> {
        let handle = self.write_coordinator.handle(WRITE_CHANNEL);
        let mut flush_handle = handle.flush(false).await.map_err(map_write_error)?;

        flush_handle
            .wait(Durability::Flushed)
            .await
            .map_err(map_write_error)?;

        Ok(())
    }

    /// Gracefully stop the write coordinator, flushing pending data.
    pub(crate) async fn stop(self) -> Result<()> {
        self.write_coordinator
            .stop()
            .await
            .map_err(Error::Internal)?;
        Ok(())
    }
}

fn map_write_error(e: WriteError) -> Error {
    match e {
        WriteError::Backpressure => Error::Backpressure,
        WriteError::Shutdown => Error::Internal("Write coordinator shut down".to_string()),
        WriteError::ApplyError(_, msg) => Error::Internal(msg),
        WriteError::FlushError(msg) => Error::Storage(msg),
        WriteError::Internal(msg) => Error::Internal(msg),
    }
}
