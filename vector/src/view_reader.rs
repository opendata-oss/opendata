use crate::delta::VectorDbWriteDelta;
use crate::serde::posting_list::{PostingList, PostingListValue, merge_decoded_posting_lists};
use crate::storage::VectorDbStorageReadExt;
use anyhow::Result;
use common::coordinator::View;
use std::sync::Arc;

pub(crate) struct ViewReader {
    view: Arc<View<VectorDbWriteDelta>>,
}

impl ViewReader {
    pub(crate) fn new(view: Arc<View<VectorDbWriteDelta>>) -> Self {
        Self { view }
    }

    pub(crate) async fn get_posting_list(
        &self,
        centroid_id: u64,
        dimensions: usize,
    ) -> Result<PostingList> {
        let mut all_postings = Vec::with_capacity(2 + self.view.frozen.len());
        {
            let current = self.view.current.read().expect("lock poisoned");
            if let Some(p) = current.posting_updates.get(&centroid_id) {
                all_postings.push(PostingListValue::from_posting_updates(p.clone())?);
            };
        }
        for frozen in self.view.frozen.iter() {
            if let Some(p) = frozen.val.posting_updates.get(&centroid_id) {
                all_postings.push(PostingListValue::from_posting_updates(p.clone())?);
            }
        }
        all_postings.push(
            self.view
                .snapshot
                .get_posting_list(centroid_id, dimensions)
                .await?,
        );
        Ok(merge_decoded_posting_lists(all_postings).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::VectorDbDeltaView;
    use crate::serde::key::PostingListKey;
    use crate::serde::posting_list::PostingUpdate;
    use common::Record;
    use common::coordinator::EpochStamped;
    use common::storage::Storage;
    use common::storage::in_memory::InMemoryStorage;
    use roaring::RoaringTreemap;
    use std::collections::HashMap;

    /// Build a VectorDbDeltaView with the given posting updates for a centroid.
    fn make_delta_view(centroid_id: u64, updates: Vec<PostingUpdate>) -> VectorDbDeltaView {
        let mut posting_updates = HashMap::new();
        posting_updates.insert(centroid_id, updates);
        VectorDbDeltaView {
            posting_updates,
            deleted_centroids: RoaringTreemap::new(),
        }
    }

    /// Write a posting list to in-memory storage for the given centroid.
    async fn write_snapshot_postings(
        storage: &InMemoryStorage,
        centroid_id: u64,
        updates: Vec<PostingUpdate>,
    ) {
        let key = PostingListKey::new(centroid_id).encode();
        let value = PostingListValue::from_posting_updates(updates)
            .unwrap()
            .encode_to_bytes();
        storage
            .put(vec![Record::new(key, value).into()])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn should_read_postings_from_current_delta() {
        // given - current delta has postings, snapshot is empty
        let centroid_id = 1u64;
        let current_view = make_delta_view(
            centroid_id,
            vec![
                PostingUpdate::append(10, vec![1.0]),
                PostingUpdate::append(11, vec![2.0]),
            ],
        );

        let storage = InMemoryStorage::new();
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(current_view)),
            frozen: vec![],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then
        assert_eq!(postings.len(), 2);
        assert_eq!(postings[0].id(), 10);
        assert_eq!(postings[1].id(), 11);
    }

    #[tokio::test]
    async fn should_read_postings_from_frozen_deltas() {
        // given - current is empty, one frozen delta has postings
        let centroid_id = 1u64;
        let frozen_view = Arc::new(make_delta_view(
            centroid_id,
            vec![
                PostingUpdate::append(20, vec![3.0]),
                PostingUpdate::append(21, vec![4.0]),
            ],
        ));

        let storage = InMemoryStorage::new();
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(VectorDbDeltaView {
                posting_updates: HashMap::new(),
                deleted_centroids: RoaringTreemap::new(),
            })),
            frozen: vec![EpochStamped {
                val: frozen_view,
                epoch_range: 0..1,
            }],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then
        assert_eq!(postings.len(), 2);
        assert_eq!(postings[0].id(), 20);
        assert_eq!(postings[1].id(), 21);
    }

    #[tokio::test]
    async fn should_read_postings_from_snapshot() {
        // given - deltas are empty, snapshot has postings
        let centroid_id = 1u64;
        let storage = InMemoryStorage::new();
        write_snapshot_postings(
            &storage,
            centroid_id,
            vec![
                PostingUpdate::append(30, vec![5.0]),
                PostingUpdate::append(31, vec![6.0]),
            ],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(VectorDbDeltaView {
                posting_updates: HashMap::new(),
                deleted_centroids: RoaringTreemap::new(),
            })),
            frozen: vec![],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then
        assert_eq!(postings.len(), 2);
        assert_eq!(postings[0].id(), 30);
        assert_eq!(postings[1].id(), 31);
    }

    #[tokio::test]
    async fn should_merge_postings_from_current_frozen_and_snapshot() {
        // given - each layer has disjoint postings
        let centroid_id = 1u64;

        let current_view = make_delta_view(centroid_id, vec![PostingUpdate::append(1, vec![10.0])]);
        let frozen_view = Arc::new(make_delta_view(
            centroid_id,
            vec![PostingUpdate::append(2, vec![20.0])],
        ));
        let storage = InMemoryStorage::new();
        write_snapshot_postings(
            &storage,
            centroid_id,
            vec![PostingUpdate::append(3, vec![30.0])],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(current_view)),
            frozen: vec![EpochStamped {
                val: frozen_view,
                epoch_range: 0..1,
            }],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then - all three postings merged
        assert_eq!(postings.len(), 3);
        assert_eq!(postings[0].id(), 1);
        assert_eq!(postings[1].id(), 2);
        assert_eq!(postings[2].id(), 3);
    }

    #[tokio::test]
    async fn should_prefer_current_over_frozen_on_duplicate_id() {
        // given - current and frozen both have ID 5 with different vectors
        let centroid_id = 1u64;

        let current_view =
            make_delta_view(centroid_id, vec![PostingUpdate::append(5, vec![100.0])]);
        let frozen_view = Arc::new(make_delta_view(
            centroid_id,
            vec![PostingUpdate::append(5, vec![50.0])],
        ));

        let storage = InMemoryStorage::new();
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(current_view)),
            frozen: vec![EpochStamped {
                val: frozen_view,
                epoch_range: 0..1,
            }],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then - current's value wins
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].id(), 5);
        assert_eq!(postings[0].vector(), &[100.0]);
    }

    #[tokio::test]
    async fn should_prefer_frozen_over_snapshot_on_duplicate_id() {
        // given - frozen and snapshot both have ID 7 with different vectors
        let centroid_id = 1u64;

        let frozen_view = Arc::new(make_delta_view(
            centroid_id,
            vec![PostingUpdate::append(7, vec![70.0])],
        ));

        let storage = InMemoryStorage::new();
        write_snapshot_postings(
            &storage,
            centroid_id,
            vec![PostingUpdate::append(7, vec![7.0])],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(VectorDbDeltaView {
                posting_updates: HashMap::new(),
                deleted_centroids: RoaringTreemap::new(),
            })),
            frozen: vec![EpochStamped {
                val: frozen_view,
                epoch_range: 0..1,
            }],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then - frozen's value wins over snapshot
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].id(), 7);
        assert_eq!(postings[0].vector(), &[70.0]);
    }

    #[tokio::test]
    async fn should_prefer_current_over_snapshot_on_duplicate_id() {
        // given - current and snapshot both have ID 9 with different vectors
        let centroid_id = 1u64;

        let current_view =
            make_delta_view(centroid_id, vec![PostingUpdate::append(9, vec![900.0])]);

        let storage = InMemoryStorage::new();
        write_snapshot_postings(
            &storage,
            centroid_id,
            vec![PostingUpdate::append(9, vec![9.0])],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(current_view)),
            frozen: vec![],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then - current's value wins over snapshot
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].id(), 9);
        assert_eq!(postings[0].vector(), &[900.0]);
    }

    #[tokio::test]
    async fn should_prefer_current_over_all_on_three_way_duplicate() {
        // given - current, frozen, and snapshot all have ID 3
        let centroid_id = 1u64;

        let current_view =
            make_delta_view(centroid_id, vec![PostingUpdate::append(3, vec![300.0])]);
        let frozen_view = Arc::new(make_delta_view(
            centroid_id,
            vec![PostingUpdate::append(3, vec![30.0])],
        ));
        let storage = InMemoryStorage::new();
        write_snapshot_postings(
            &storage,
            centroid_id,
            vec![PostingUpdate::append(3, vec![3.0])],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(current_view)),
            frozen: vec![EpochStamped {
                val: frozen_view,
                epoch_range: 0..1,
            }],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then - current's value wins over frozen and snapshot
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].id(), 3);
        assert_eq!(postings[0].vector(), &[300.0]);
    }

    #[tokio::test]
    async fn should_prefer_earlier_frozen_over_later_frozen() {
        // given - two frozen deltas with overlapping ID; first frozen is newer
        let centroid_id = 1u64;

        let frozen_newer = Arc::new(make_delta_view(
            centroid_id,
            vec![PostingUpdate::append(4, vec![400.0])],
        ));
        let frozen_older = Arc::new(make_delta_view(
            centroid_id,
            vec![PostingUpdate::append(4, vec![40.0])],
        ));

        let storage = InMemoryStorage::new();
        let snapshot = storage.snapshot().await.unwrap();

        let view = Arc::new(View::<VectorDbWriteDelta> {
            current: Arc::new(std::sync::RwLock::new(VectorDbDeltaView {
                posting_updates: HashMap::new(),
                deleted_centroids: RoaringTreemap::new(),
            })),
            frozen: vec![
                EpochStamped {
                    val: frozen_newer,
                    epoch_range: 1..2,
                },
                EpochStamped {
                    val: frozen_older,
                    epoch_range: 0..1,
                },
            ],
            snapshot,
            last_flushed_delta: None,
        });
        let reader = ViewReader::new(view);

        // when
        let postings = reader.get_posting_list(centroid_id, 1).await.unwrap();

        // then - first frozen (newer) wins
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].id(), 4);
        assert_eq!(postings[0].vector(), &[400.0]);
    }
}
