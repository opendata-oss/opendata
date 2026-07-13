//! Native SlateDB storage for timeseries.
//!
//! This module replaces timeseries' use of the `common::storage` *handles*
//! (`Storage`/`StorageRead`/`StorageSnapshot`) with a direct SlateDB
//! integration. Everything reusable — value types (`Record`, `RecordOp`,
//! `Ttl`, …), the merge-operator trait, the error type, the config types,
//! the object-store / foyer-cache / metrics-recorder plumbing, and the
//! `Ttl`/options → SlateDB conversions — is reused from `common::storage`
//! rather than duplicated.
//!
//! - [`slate`] — the concrete [`Storage`] writer plus the [`StorageReader`]
//!   and [`StorageSnapshot`] read handles; all three serve reads through the
//!   shared [`StorageRead`] methods.
//! - [`merge_operator`] — the OpenTSDB merge operator.
//! - [`factory`] — the in-memory test storage helper.
pub(crate) mod factory;
pub(crate) mod merge_operator;
pub(crate) mod segment_extractor;
pub(crate) mod slate;

pub(crate) use slate::{
    Storage, StorageRead, StorageReader, StorageSnapshot, Store, insert_forward_index,
    insert_series_id, merge_inverted_index, merge_samples,
};

#[cfg(any(test, feature = "testing"))]
pub(crate) use factory::in_memory_storage;
#[cfg(test)]
pub(crate) use factory::{FailingStorage, SharedInMemoryStorage, in_memory_shared_storage};

#[cfg(test)]
mod tests {
    use crate::model::TimeBucket;
    use crate::serde::key::SeriesDictionaryKey;
    use crate::storage::{Storage, StorageRead, in_memory_storage};
    use common::Record;
    use common::storage::PutRecordOp;
    use std::sync::Arc;

    /// Create a SlateDb storage with the given hour-buckets pre-populated.
    /// Each entry in `bucket_starts` is the bucket start in minutes.
    ///
    /// One sentinel `SeriesDictionaryKey` per bucket is written so the
    /// segment extractor registers each bucket in the segment list.
    async fn storage_with_buckets(bucket_starts: &[u32]) -> Arc<Storage> {
        let storage = Arc::new(in_memory_storage().await);
        let ops: Vec<PutRecordOp> = bucket_starts
            .iter()
            .map(|&start| {
                let key = SeriesDictionaryKey {
                    bucket: TimeBucket { start, size: 1 },
                    series_fingerprint: 0,
                }
                .encode();
                PutRecordOp::new(Record {
                    key,
                    value: bytes::Bytes::new(),
                })
            })
            .collect();
        storage.put(ops).await.unwrap();
        storage
    }

    fn starts(buckets: &[TimeBucket]) -> Vec<u32> {
        buckets.iter().map(|b| b.start).collect()
    }

    // ── get_buckets_for_ranges boundary tests ──────────────────────────

    #[tokio::test]
    async fn ranges_point_at_bucket_start() {
        // Buckets: [0, 3600s), [3600s, 7200s)  (hour buckets at min 0 and 60)
        let s = storage_with_buckets(&[0, 60]).await;
        // Point query exactly at second bucket start
        let buckets = s.get_buckets_for_ranges(&[(3600, 3600)]).await.unwrap();
        // Should match only the bucket starting at 3600s (min 60), not the one ending there
        assert_eq!(starts(&buckets), vec![60]);
    }

    #[tokio::test]
    async fn ranges_point_at_epoch() {
        let s = storage_with_buckets(&[0, 60]).await;
        // Point query at t=0
        let buckets = s.get_buckets_for_ranges(&[(0, 0)]).await.unwrap();
        assert_eq!(starts(&buckets), vec![0]);
    }

    #[tokio::test]
    async fn ranges_spanning_two_buckets() {
        let s = storage_with_buckets(&[0, 60, 120]).await;
        // Range [1800, 5400] spans bucket [0,3600) and [3600,7200)
        let buckets = s.get_buckets_for_ranges(&[(1800, 5400)]).await.unwrap();
        assert_eq!(starts(&buckets), vec![0, 60]);
    }

    #[tokio::test]
    async fn ranges_disjoint_skips_middle() {
        // 3 hour-buckets: [0,3600), [3600,7200), [7200,10800)
        let s = storage_with_buckets(&[0, 60, 120]).await;
        // Two disjoint ranges that skip the middle bucket
        let buckets = s
            .get_buckets_for_ranges(&[(100, 200), (8000, 9000)])
            .await
            .unwrap();
        assert_eq!(starts(&buckets), vec![0, 120]);
    }

    #[tokio::test]
    async fn ranges_empty_returns_nothing() {
        let s = storage_with_buckets(&[0, 60]).await;
        let buckets = s.get_buckets_for_ranges(&[]).await.unwrap();
        assert!(buckets.is_empty());
    }

    #[tokio::test]
    async fn ranges_exact_bucket_boundary_excludes_previous() {
        // Range starts exactly at boundary between two buckets.
        // Bucket [0, 3600s) should NOT match range [3600, 7200].
        let s = storage_with_buckets(&[0, 60]).await;
        let buckets = s.get_buckets_for_ranges(&[(3600, 7200)]).await.unwrap();
        assert_eq!(starts(&buckets), vec![60]);
    }
}
