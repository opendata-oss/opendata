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
pub(crate) mod slate;

pub(crate) use slate::{
    Storage, StorageRead, StorageReader, StorageSnapshot, Store, insert_forward_index,
    insert_series_id, merge_bucket_list, merge_inverted_index, merge_samples,
};

#[cfg(any(test, feature = "testing"))]
pub(crate) use factory::in_memory_storage;
#[cfg(test)]
pub(crate) use factory::{FailingStorage, SharedInMemoryStorage, in_memory_shared_storage};

#[cfg(test)]
mod tests {
    use crate::model::TimeBucket;
    use crate::serde::bucket_list::BucketListValue;
    use crate::serde::key::BucketListKey;
    use crate::storage::{Storage, StorageRead, in_memory_storage, merge_bucket_list};
    use common::Record;
    use common::storage::{PutRecordOp, Ttl};
    use std::sync::Arc;

    /// Create a SlateDb storage with the given hour-buckets pre-populated.
    /// Each entry in `bucket_starts` is the bucket start in minutes.
    async fn storage_with_buckets(bucket_starts: &[u32]) -> Arc<Storage> {
        let storage = Arc::new(in_memory_storage().await);
        let buckets: Vec<(u8, u32)> = bucket_starts.iter().map(|&s| (1u8, s)).collect();
        let key = BucketListKey.encode();
        let value = BucketListValue { buckets }.encode();
        storage
            .put(vec![PutRecordOp::new(Record { key, value })])
            .await
            .unwrap();
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

    // ── bucket_list_contains tests ─────────────────────────────────────

    #[tokio::test]
    async fn should_return_true_when_bucket_in_list() {
        // given
        let storage = storage_with_buckets(&[0, 60, 120]).await;

        // when
        let present = storage
            .bucket_list_contains(TimeBucket { size: 1, start: 60 })
            .await
            .unwrap();

        // then
        assert!(present);
    }

    #[tokio::test]
    async fn should_return_false_when_bucket_absent() {
        // given
        let storage = storage_with_buckets(&[0, 60]).await;

        // when
        let present = storage
            .bucket_list_contains(TimeBucket {
                size: 1,
                start: 180,
            })
            .await
            .unwrap();

        // then
        assert!(!present);
    }

    #[tokio::test]
    async fn should_return_false_when_bucket_list_key_missing() {
        // given
        let storage = Arc::new(in_memory_storage().await);

        // when
        let present = storage
            .bucket_list_contains(TimeBucket { size: 1, start: 0 })
            .await
            .unwrap();

        // then
        assert!(!present);
    }

    #[tokio::test]
    async fn should_distinguish_buckets_with_same_start_but_different_size() {
        // given: only a size=1 bucket at start=60 is present
        let storage = storage_with_buckets(&[60]).await;

        // when
        let size_one = storage
            .bucket_list_contains(TimeBucket { size: 1, start: 60 })
            .await
            .unwrap();
        let size_two = storage
            .bucket_list_contains(TimeBucket { size: 2, start: 60 })
            .await
            .unwrap();

        // then
        assert!(size_one);
        assert!(!size_two);
    }

    // ── coalesce_bucket_list tests ─────────────────────────────────────

    #[tokio::test]
    async fn should_coalesce_bucket_list_preserving_merged_value() {
        // given: several merge operands have accrued on the BucketList key
        let storage = Arc::new(in_memory_storage().await);
        let ops = vec![
            merge_bucket_list(TimeBucket { size: 1, start: 0 }, Ttl::Default).unwrap(),
            merge_bucket_list(TimeBucket { size: 1, start: 60 }, Ttl::Default).unwrap(),
            merge_bucket_list(
                TimeBucket {
                    size: 1,
                    start: 120,
                },
                Ttl::Default,
            )
            .unwrap(),
        ];
        storage.apply(ops).await.unwrap();

        // when
        storage.coalesce_bucket_list().await.unwrap();

        // then: readable value still reflects the full merged list
        let buckets = storage.get_buckets_in_range(None, None).await.unwrap();
        assert_eq!(starts(&buckets), vec![0, 60, 120]);
    }

    #[tokio::test]
    async fn should_be_noop_when_bucket_list_absent() {
        // given
        let storage = Arc::new(in_memory_storage().await);

        // when
        storage.coalesce_bucket_list().await.unwrap();

        // then: key is still absent
        let value = storage.get(BucketListKey.encode()).await.unwrap();
        assert!(value.is_none());
    }
}
