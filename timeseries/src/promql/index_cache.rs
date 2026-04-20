//! Query-scoped inverted/forward index cache.
//!
//! One instance per query, held by `QueryReaderSource`. Dedupes index fetches
//! against `(bucket, terms)` / `(bucket, series_ids)` keys (term/id slices are
//! sorted before keying). Concurrent misses on the same key may both fetch;
//! last write wins. Errors are never cached.
//!
//! Scoped narrowly to index dedup: bucket-list memoisation, semaphores, and
//! query stats are handled elsewhere.

use std::sync::Arc;

use dashmap::DashMap;
use roaring::RoaringBitmap;

use crate::error::Result;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup, SeriesSpec};
use crate::model::{Label, SeriesId, TimeBucket};
use crate::query::QueryReader;

type InvertedKey = (TimeBucket, Vec<Label>);
type ForwardKey = (TimeBucket, Vec<SeriesId>);

type InvertedValue = Arc<dyn InvertedIndexLookup + Send + Sync + 'static>;
type ForwardValue = Arc<dyn ForwardIndexLookup + Send + Sync + 'static>;

/// `Arc<Option<_>>` so "not present in bucket" is also cacheable.
type InvertedTermValue = Arc<Option<RoaringBitmap>>;
type ForwardSeriesValue = Arc<Option<SeriesSpec>>;

/// Per-query cache of inverted and forward index lookups. All methods take
/// `&self`; interior concurrency via [`DashMap`].
///
/// Two granularities: batch (`inverted_index` / `forward_index`, used where
/// callers don't own fan-out) and per-key (`inverted_index_term` /
/// `forward_index_one`, used by the source adapter to parallelise at its
/// own layer and keep per-call traces).
pub(crate) struct IndexCache {
    inverted: DashMap<InvertedKey, InvertedValue>,
    forward: DashMap<ForwardKey, ForwardValue>,
    inverted_terms: DashMap<(TimeBucket, Label), InvertedTermValue>,
    forward_series: DashMap<(TimeBucket, SeriesId), ForwardSeriesValue>,
}

impl IndexCache {
    pub(crate) fn new() -> Self {
        Self {
            inverted: DashMap::new(),
            forward: DashMap::new(),
            inverted_terms: DashMap::new(),
            forward_series: DashMap::new(),
        }
    }

    /// `terms` is sorted internally before keying. Errors propagate verbatim
    /// and are **not** cached.
    pub(crate) async fn inverted_index<R: QueryReader + ?Sized>(
        &self,
        reader: &R,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<InvertedValue> {
        let mut sorted_terms = terms.to_vec();
        sorted_terms.sort();
        let key = (*bucket, sorted_terms);

        if let Some(hit) = self.inverted.get(&key) {
            return Ok(hit.clone());
        }

        let fetched = reader.inverted_index(bucket, &key.1).await?;
        let value: InvertedValue = Arc::from(fetched);
        self.inverted.insert(key, value.clone());
        Ok(value)
    }

    /// Mirrors [`Self::inverted_index`]: `series_ids` is sorted internally;
    /// errors are not cached.
    pub(crate) async fn forward_index<R: QueryReader + ?Sized>(
        &self,
        reader: &R,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<ForwardValue> {
        let mut sorted_ids = series_ids.to_vec();
        sorted_ids.sort();
        let key = (*bucket, sorted_ids);

        if let Some(hit) = self.forward.get(&key) {
            return Ok(hit.clone());
        }

        let fetched = reader.forward_index(bucket, &key.1).await?;
        let value: ForwardValue = Arc::from(fetched);
        self.forward.insert(key, value.clone());
        Ok(value)
    }

    /// Cacheable `None` so repeated lookups of a missing term don't re-hit
    /// storage. Errors are not cached.
    pub(crate) async fn inverted_index_term<R: QueryReader + ?Sized>(
        &self,
        reader: &R,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<InvertedTermValue> {
        let key = (*bucket, term.clone());
        if let Some(hit) = self.inverted_terms.get(&key) {
            return Ok(hit.clone());
        }
        let fetched = reader.inverted_index_term(bucket, term).await?;
        let value: InvertedTermValue = Arc::new(fetched);
        self.inverted_terms.insert(key, value.clone());
        Ok(value)
    }

    /// See [`Self::inverted_index_term`] for cached-`None` semantics.
    pub(crate) async fn forward_index_one<R: QueryReader + ?Sized>(
        &self,
        reader: &R,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> Result<ForwardSeriesValue> {
        let key = (*bucket, series_id);
        if let Some(hit) = self.forward_series.get(&key) {
            return Ok(hit.clone());
        }
        let fetched = reader.forward_index_one(bucket, series_id).await?;
        let value: ForwardSeriesValue = Arc::new(fetched);
        self.forward_series.insert(key, value.clone());
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{ForwardIndex, InvertedIndex, SeriesSpec};
    use crate::model::{MetricType, Sample};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingReader {
        inverted_calls: AtomicUsize,
        forward_calls: AtomicUsize,
    }

    impl CountingReader {
        fn new() -> Self {
            Self {
                inverted_calls: AtomicUsize::new(0),
                forward_calls: AtomicUsize::new(0),
            }
        }

        fn inverted_call_count(&self) -> usize {
            self.inverted_calls.load(Ordering::SeqCst)
        }

        fn forward_call_count(&self) -> usize {
            self.forward_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl QueryReader for CountingReader {
        async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
            Ok(vec![])
        }

        async fn forward_index(
            &self,
            _bucket: &TimeBucket,
            _series_ids: &[SeriesId],
        ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
            self.forward_calls.fetch_add(1, Ordering::SeqCst);
            let fi = ForwardIndex::default();
            fi.series.insert(
                1,
                SeriesSpec {
                    unit: None,
                    metric_type: Some(MetricType::Gauge),
                    labels: vec![],
                },
            );
            Ok(Box::new(fi))
        }

        async fn inverted_index(
            &self,
            _bucket: &TimeBucket,
            _terms: &[Label],
        ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
            self.inverted_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(InvertedIndex::default()))
        }

        async fn all_inverted_index(
            &self,
            _bucket: &TimeBucket,
        ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
            Ok(Box::new(InvertedIndex::default()))
        }

        async fn label_values(
            &self,
            _bucket: &TimeBucket,
            _label_name: &str,
        ) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn samples(
            &self,
            _bucket: &TimeBucket,
            _series_id: SeriesId,
            _metric_name: &str,
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Sample>> {
            Ok(vec![])
        }

        async fn forward_index_one(
            &self,
            _bucket: &TimeBucket,
            _series_id: SeriesId,
        ) -> Result<Option<crate::index::SeriesSpec>> {
            self.forward_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(SeriesSpec {
                unit: None,
                metric_type: Some(MetricType::Gauge),
                labels: vec![],
            }))
        }

        async fn inverted_index_term(
            &self,
            _bucket: &TimeBucket,
            _term: &Label,
        ) -> Result<Option<roaring::RoaringBitmap>> {
            self.inverted_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(roaring::RoaringBitmap::new()))
        }
    }

    /// A reader that fails the first N inverted_index calls, then succeeds.
    struct TransientFailureReader {
        inverted_calls: AtomicUsize,
        fail_first_n: usize,
    }

    impl TransientFailureReader {
        fn new(fail_first_n: usize) -> Self {
            Self {
                inverted_calls: AtomicUsize::new(0),
                fail_first_n,
            }
        }

        fn inverted_call_count(&self) -> usize {
            self.inverted_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl QueryReader for TransientFailureReader {
        async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
            Ok(vec![])
        }

        async fn forward_index(
            &self,
            _bucket: &TimeBucket,
            _series_ids: &[SeriesId],
        ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
            Ok(Box::new(ForwardIndex::default()))
        }

        async fn inverted_index(
            &self,
            _bucket: &TimeBucket,
            _terms: &[Label],
        ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
            let n = self.inverted_calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first_n {
                Err(crate::error::Error::Internal("transient".to_string()))
            } else {
                Ok(Box::new(InvertedIndex::default()))
            }
        }

        async fn all_inverted_index(
            &self,
            _bucket: &TimeBucket,
        ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
            Ok(Box::new(InvertedIndex::default()))
        }

        async fn label_values(
            &self,
            _bucket: &TimeBucket,
            _label_name: &str,
        ) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn samples(
            &self,
            _bucket: &TimeBucket,
            _series_id: SeriesId,
            _metric_name: &str,
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Sample>> {
            Ok(vec![])
        }

        async fn forward_index_one(
            &self,
            _bucket: &TimeBucket,
            _series_id: SeriesId,
        ) -> Result<Option<crate::index::SeriesSpec>> {
            Ok(None)
        }

        async fn inverted_index_term(
            &self,
            _bucket: &TimeBucket,
            _term: &Label,
        ) -> Result<Option<roaring::RoaringBitmap>> {
            let n = self.inverted_calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first_n {
                Err(crate::error::Error::Internal("transient".to_string()))
            } else {
                Ok(Some(roaring::RoaringBitmap::new()))
            }
        }
    }

    fn bucket(start: u32) -> TimeBucket {
        TimeBucket { start, size: 1 }
    }

    fn label(name: &str, value: &str) -> Label {
        Label {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    #[tokio::test]
    async fn should_return_same_arc_for_repeated_inverted_index_fetches_with_identical_terms() {
        // given
        let reader = CountingReader::new();
        let cache = IndexCache::new();
        let b = bucket(0);
        let terms = vec![label("__name__", "m")];

        // when
        let first = cache.inverted_index(&reader, &b, &terms).await.unwrap();
        let second = cache.inverted_index(&reader, &b, &terms).await.unwrap();

        // then
        assert_eq!(reader.inverted_call_count(), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn should_normalize_unsorted_terms_when_keying_inverted_index() {
        // given
        let reader = CountingReader::new();
        let cache = IndexCache::new();
        let b = bucket(0);
        let unsorted = vec![label("z", "1"), label("a", "2"), label("m", "3")];
        let sorted = {
            let mut s = unsorted.clone();
            s.sort();
            s
        };

        // when
        let first = cache.inverted_index(&reader, &b, &unsorted).await.unwrap();
        let second = cache.inverted_index(&reader, &b, &sorted).await.unwrap();

        // then
        assert_eq!(reader.inverted_call_count(), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn should_return_same_arc_for_repeated_forward_index_fetches_with_identical_series_ids() {
        // given
        let reader = CountingReader::new();
        let cache = IndexCache::new();
        let b = bucket(0);
        let ids: Vec<SeriesId> = vec![3, 1, 2];
        let ids_other_order: Vec<SeriesId> = vec![1, 2, 3];

        // when
        let first = cache.forward_index(&reader, &b, &ids).await.unwrap();
        let second = cache
            .forward_index(&reader, &b, &ids_other_order)
            .await
            .unwrap();

        // then
        assert_eq!(reader.forward_call_count(), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn should_treat_different_buckets_as_separate_cache_entries() {
        // given
        let reader = CountingReader::new();
        let cache = IndexCache::new();
        let b1 = bucket(0);
        let b2 = bucket(60);
        let terms = vec![label("__name__", "m")];

        // when
        let v1 = cache.inverted_index(&reader, &b1, &terms).await.unwrap();
        let v2 = cache.inverted_index(&reader, &b2, &terms).await.unwrap();

        // then
        assert_eq!(reader.inverted_call_count(), 2);
        assert!(!Arc::ptr_eq(&v1, &v2));
    }

    #[tokio::test]
    async fn should_propagate_reader_errors_without_caching_them() {
        // given: the reader fails the first call, then succeeds
        let reader = TransientFailureReader::new(1);
        let cache = IndexCache::new();
        let b = bucket(0);
        let terms = vec![label("__name__", "m")];

        // when
        let first = cache.inverted_index(&reader, &b, &terms).await;
        let second = cache.inverted_index(&reader, &b, &terms).await;

        // then: the first returns an error, the second retries and succeeds
        assert!(first.is_err());
        assert!(second.is_ok());
        assert_eq!(reader.inverted_call_count(), 2);

        // subsequent success is cached
        let third = cache.inverted_index(&reader, &b, &terms).await.unwrap();
        assert_eq!(reader.inverted_call_count(), 2);
        let second = second.unwrap();
        assert!(Arc::ptr_eq(&second, &third));
    }
}
