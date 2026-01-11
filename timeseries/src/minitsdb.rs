#![allow(dead_code)]

use std::sync::{Arc, atomic::AtomicU32};

use async_trait::async_trait;
use common::{Storage, StorageRead};
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};

use crate::delta::{TsdbDelta, TsdbDeltaBuilder};
use crate::error::Error;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::model::{Label, Sample, Series, SeriesFingerprint, SeriesId, TimeBucket};
use crate::query::QueryReader;
use crate::serde::key::TimeSeriesKey;
use crate::serde::timeseries::TimeSeriesIterator;
use crate::storage::{OpenTsdbStorageExt, OpenTsdbStorageReadExt};
use crate::util::Result;

pub(crate) struct MiniQueryReader<'a> {
    bucket: &'a TimeBucket,
    snapshot: Arc<dyn StorageRead>,
}

#[async_trait]
impl<'a> QueryReader for MiniQueryReader<'a> {
    async fn forward_index(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>> {
        let forward_index = self
            .snapshot
            .get_forward_index_series(self.bucket, series_ids)
            .await?;
        Ok(Box::new(forward_index))
    }

    async fn all_forward_index(&self) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>> {
        let forward_index = self.snapshot.get_forward_index(self.bucket.clone()).await?;
        Ok(Box::new(forward_index))
    }

    async fn inverted_index(
        &self,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>> {
        let inverted_index = self
            .snapshot
            .get_inverted_index_terms(self.bucket, terms)
            .await?;
        Ok(Box::new(inverted_index))
    }

    async fn all_inverted_index(&self) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>> {
        let inverted_index = self
            .snapshot
            .get_inverted_index(self.bucket.clone())
            .await?;
        Ok(Box::new(inverted_index))
    }

    async fn label_values(&self, label_name: &str) -> Result<Vec<String>> {
        self.snapshot
            .get_label_values(self.bucket, label_name)
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
    series_dict: DashMap<SeriesFingerprint, SeriesId>,
    next_series_id: AtomicU32,
    /// Pending delta accumulating ingested data not yet flushed to storage.
    pending_delta: Mutex<TsdbDelta>,
    /// Receiver for waiting for updates to pending delta
    pending_delta_watch_rx: tokio::sync::watch::Receiver<std::time::Instant>,
    /// Sender for updating the pending delta when flushing.
    pending_delta_watch_tx: tokio::sync::watch::Sender<std::time::Instant>,
    /// Storage snapshot used for queries.
    snapshot: RwLock<Arc<dyn StorageRead>>,
    /// Mutex to ensure only one flush operation can run at a time.
    flush_mutex: Arc<Mutex<()>>,
}

impl MiniTsdb {
    /// Returns a reference to the time bucket
    pub(crate) fn bucket(&self) -> &TimeBucket {
        &self.bucket
    }

    /// Create a query reader for read operations.
    pub(crate) async fn query_reader(&self) -> MiniQueryReader<'_> {
        // TODO: right now the data that is not flushed to storage
        // (sitting in the pending delta) is not visible to queries,
        // we should improve this by adding a tiered reader where we
        // can read from in-memory deltas as well

        // this also holds the lock for the duration of the query reader
        // meaning we can't finish a flush operation
        let snapshot = self.snapshot.read().await.clone();
        MiniQueryReader {
            bucket: &self.bucket,
            snapshot,
        }
    }

    pub(crate) async fn load(bucket: TimeBucket, storage: Arc<dyn StorageRead>) -> Result<Self> {
        let series_dict = DashMap::new();
        let next_series_id = storage
            .load_series_dictionary(&bucket, |fingerprint, series_id| {
                series_dict.insert(fingerprint, series_id);
            })
            .await?;

        // Create tokio::watch channel for pending delta
        let (tx, rx) = tokio::sync::watch::channel(std::time::Instant::now());

        Ok(Self {
            bucket: bucket.clone(),
            series_dict,
            next_series_id: AtomicU32::new(next_series_id),
            pending_delta: Mutex::new(TsdbDelta::empty(bucket.clone())),
            pending_delta_watch_tx: tx,
            pending_delta_watch_rx: rx,
            snapshot: RwLock::new(storage),
            flush_mutex: Arc::new(Mutex::new(())),
        })
    }

    /// Ingest a batch of series with samples in a single operation.
    /// This is more efficient than calling ingest() multiple times as it creates only one delta builder.
    /// Note: Ingested data is batched and NOT visible to queries until flush().
    /// Returns an error if any sample timestamp is outside the bucket's time range.
    /// Blocks if the pending delta is older than 2 * flush_interval_secs.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>()
        )
    )]
    pub(crate) async fn ingest_batch(
        &self,
        series_list: &[Series],
        flush_interval_secs: u64,
    ) -> Result<()> {
        let total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>();

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Starting MiniTsdb batch ingest"
        );

        // Block until the pending delta is young enough (not older than 2 * flush_interval_secs)
        let max_age = std::time::Duration::from_secs(2 * flush_interval_secs);
        let mut receiver = self.pending_delta_watch_rx.clone();
        receiver
            .wait_for(|t| t.elapsed() <= max_age)
            .await
            .map_err(|_e| "pending delta watch_rx disconnected")?;

        let mut builder =
            TsdbDeltaBuilder::new(self.bucket.clone(), &self.series_dict, &self.next_series_id);

        // Ingest all series into the same delta builder
        for series in series_list {
            builder.ingest(series)?;
        }

        let delta = builder.build();

        // Accumulate into pending delta
        {
            let mut pending = self.pending_delta.lock().await;
            pending.merge(delta);
        }

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Completed MiniTsdb batch ingest"
        );

        Ok(())
    }

    /// Ingest a single series with samples.
    /// Note: Ingested data is batched and NOT visible to queries until flush().
    /// Returns an error if any sample timestamp is outside the bucket's time range.
    ///
    /// For better performance when ingesting multiple series, use ingest_batch() instead.
    pub(crate) async fn ingest(&self, series: &Series, flush_interval_secs: u64) -> Result<()> {
        // Delegate to batch method with a single series
        self.ingest_batch(std::slice::from_ref(series), flush_interval_secs)
            .await
    }

    /// Flush pending data to storage, making it durable and visible to queries.
    pub(crate) async fn flush(
        &self,
        storage: Arc<dyn Storage>,
        _flush_interval_secs: u64,
    ) -> Result<()> {
        let _flush_guard = self.flush_mutex.lock().await;

        // Take the pending delta (replace with empty) - after this blocking
        // section we can continue accepting ingestion while we flush to
        // storage
        let (delta, created_at) = {
            let mut pending = self.pending_delta.lock().await;
            if pending.is_empty() {
                return Ok(());
            }

            let delta = std::mem::replace(&mut *pending, TsdbDelta::empty(self.bucket.clone()));
            (delta, std::time::Instant::now())
        };
        self.pending_delta_watch_tx.send_if_modified(|current| {
            if created_at > *current {
                *current = created_at;
                true
            } else {
                false
            }
        });

        let mut ops = Vec::new();
        ops.push(storage.merge_bucket_list(self.bucket.clone())?);

        for (fingerprint, series_id) in &delta.series_dict {
            ops.push(storage.insert_series_id(self.bucket.clone(), *fingerprint, *series_id)?);
        }

        for entry in delta.forward_index.series.iter() {
            ops.push(storage.insert_forward_index(
                self.bucket.clone(),
                *entry.key(),
                entry.value().clone(),
            )?);
        }

        for entry in delta.inverted_index.postings.iter() {
            ops.push(storage.merge_inverted_index(
                self.bucket.clone(),
                entry.key().clone(),
                entry.value().clone(),
            )?);
        }

        for (series_id, samples) in delta.samples {
            ops.push(storage.merge_samples(self.bucket.clone(), series_id, samples)?);
        }

        storage.apply(ops).await?;

        // Update snapshot
        let new_snapshot = storage.snapshot().await?;
        let mut snapshot_guard = self.snapshot.write().await;
        *snapshot_guard = new_snapshot;

        Ok(())
    }
}
