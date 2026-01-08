#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::Storage;
use common::storage::StorageSnapshot;
use dashmap::DashMap;

use crate::error::Error;
use crate::model::{Sample, SeriesFingerprint, SeriesId, TimeBucket};
use crate::util::Result;
use crate::{
    delta::TsdbDelta,
    index::{ForwardIndex, InvertedIndex},
    storage::OpenTsdbStorageExt,
};

/// This is a partial in-memory representation of the head of the TSDB.
/// It accepts deltas from the ingestion layer and updates the internal
/// state, which can be used to server queries.
///
/// This buffers multiple deltas together and applies them to the storage
/// layer batched. The more frequent the flush interval, the less memory
/// pressure exists on the system but the overhead is instead shifted
/// to the storage layer which needs to resolve more merge operations.
pub(crate) struct TsdbHead {
    bucket: TimeBucket,
    forward_index: ForwardIndex,
    inverted_index: InvertedIndex,
    series_dict: DashMap<SeriesFingerprint, SeriesId>,
    samples: DashMap<SeriesId, Vec<Sample>>,
    frozen: AtomicBool,
}

impl TsdbHead {
    pub fn new(bucket: TimeBucket) -> Self {
        Self {
            bucket,
            forward_index: ForwardIndex::default(),
            inverted_index: InvertedIndex::default(),
            series_dict: DashMap::new(),
            samples: DashMap::new(),
            frozen: AtomicBool::new(false),
        }
    }

    /// Returns a reference to the series dictionary (contains only NEW series)
    pub fn series_dict(&self) -> &DashMap<SeriesFingerprint, SeriesId> {
        &self.series_dict
    }

    /// Returns a reference to the bucket
    pub fn bucket(&self) -> &TimeBucket {
        &self.bucket
    }

    /// Returns a reference to the forward index
    pub fn forward_index(&self) -> &ForwardIndex {
        &self.forward_index
    }

    /// Returns a reference to the inverted index
    pub fn inverted_index(&self) -> &InvertedIndex {
        &self.inverted_index
    }

    /// Returns a reference to the samples map
    pub(crate) fn samples(&self) -> &DashMap<SeriesId, Vec<Sample>> {
        &self.samples
    }

    pub fn merge(&self, delta: &TsdbDelta) -> Result<()> {
        if self.frozen.load(Ordering::SeqCst) {
            return Err(Error::Internal("TsdbHead is frozen".to_string()));
        }

        self.forward_index.merge(&delta.forward_index);
        self.inverted_index.merge(delta.inverted_index.clone());

        for (fingerprint, series_id) in &delta.series_dict {
            self.series_dict.insert(*fingerprint, *series_id);
        }

        for (series_id, delta_samples) in &delta.samples {
            // TODO(agavra): do we need to sort here?
            self.samples
                .entry(*series_id)
                .or_default()
                .extend(delta_samples.iter().cloned());
        }

        Ok(())
    }

    pub fn freeze(&self) -> bool {
        self.frozen.fetch_or(true, Ordering::SeqCst)
    }

    pub async fn flush(&self, storage: Arc<dyn Storage>) -> Result<Arc<dyn StorageSnapshot>> {
        if !self.frozen.load(Ordering::SeqCst) {
            return Err(Error::Internal(
                "Should only flush frozen TsdbHead".to_string(),
            ));
        }

        // TODO(agavra): avoid all the cloning that happens in this method by
        // refactoring the encoding layer to work with references instead of
        // values
        let mut ops = Vec::new();

        // Merge bucket list
        ops.push(storage.merge_bucket_list(self.bucket.clone())?);

        // Insert series dictionary entries
        for entry in self.series_dict.iter() {
            ops.push(storage.insert_series_id(
                self.bucket.clone(),
                *entry.key(),
                *entry.value(),
            )?);
        }

        // Insert forward index entries
        for entry in self.forward_index.series.iter() {
            ops.push(storage.insert_forward_index(
                self.bucket.clone(),
                *entry.key(),
                entry.value().clone(),
            )?);
        }

        // Merge inverted index entries
        for entry in self.inverted_index.postings.iter() {
            ops.push(storage.merge_inverted_index(
                self.bucket.clone(),
                entry.key().clone(),
                entry.value().clone(),
            )?);
        }

        // Merge samples
        for entry in self.samples.iter() {
            ops.push(storage.merge_samples(
                self.bucket.clone(),
                *entry.key(),
                entry.value().clone(),
            )?);
        }

        storage.apply(ops).await?;
        Ok(storage.snapshot().await?)
    }
}
