use crate::Result;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::centroids::SearchResult;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use common::StorageRead;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct SplitRoot {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
}

impl SplitRoot {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            snapshot_epoch,
        }
    }

    pub(crate) async fn execute(
        self,
        delta: &mut VectorIndexDelta,
        state: &VectorIndexState,
    ) -> Result<()> {
        let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);

        if view.root_count() < self.opts.root_threshold_vectors as u64 {
            return Ok(());
        }

        let original_root_postings = view.root_posting_list(self.opts.dimensions).get().await?;
        let root_vecs: Vec<_> = original_root_postings
            .iter()
            .map(|p| (p.id().id(), p.vector()))
            .collect();
        let clustering = crate::math::kmeans::for_metric(self.opts.distance_metric);
        // todo: change from 2-means to kmeans to support efficient splits of large roots
        let new_root_centroids = clustering.two_means(&root_vecs, self.opts.dimensions);
        let new_root_centroids = vec![new_root_centroids.0, new_root_centroids.1];

        drop(view);

        let new_level = delta.search_index.promote_root(new_root_centroids);

        // assign all the existing root centroid vecs to the new centroids
        let updates_for_original_root_postings = {
            let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);
            let centroid_index =
                view.centroid_index(self.opts.dimensions, self.opts.distance_metric);
            let mut updates_for_original_root_postings =
                HashMap::with_capacity(original_root_postings.len());
            for posting in original_root_postings.iter() {
                let mut original_c = view
                    .centroid(posting.id())
                    .expect("unexpected missing centroid")
                    .clone();
                let root_search = centroid_index.search_root(posting.vector(), 1);
                let SearchResult::Ann(root_search) = root_search else {
                    panic!("root should always be in cache")
                };
                let root_c = root_search.first().expect("unexpected missing centroid");
                assert_eq!(root_c.id().level(), new_level.level());
                original_c.parent_vector_id = root_c.id();
                updates_for_original_root_postings.insert(posting.id(), original_c);
            }
            updates_for_original_root_postings
        };

        // connect original root centroids back to new root
        for (original_c_id, entry) in updates_for_original_root_postings {
            delta.search_index.add_to_posting(
                entry.parent_vector_id,
                original_c_id,
                entry.vector.clone(),
            );
            delta.search_index.update_centroid(original_c_id, entry);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MetadataFieldSpec;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::vector_id::VectorId;
    use crate::storage::VectorDbStorageReadExt;
    use crate::write::indexer::tree::posting_list::PostingList;
    use crate::write::indexer::tree::test_utils::{IndexerOpTestHarnessBuilder, TestDataVector};
    use std::collections::HashSet;

    const DIMS: usize = 2;
    const C0: VectorId = VectorId::centroid_id(1, 100);
    const C1: VectorId = VectorId::centroid_id(1, 101);
    const C2: VectorId = VectorId::centroid_id(1, 102);
    const C3: VectorId = VectorId::centroid_id(1, 103);

    fn create_opts(root_threshold_vectors: usize) -> Arc<IndexerOpts> {
        Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            root_threshold_vectors,
            merge_threshold_vectors: 0,
            split_threshold_vectors: usize::MAX,
            split_search_neighbourhood: 4,
            indexed_fields: HashSet::new(),
        })
    }

    async fn make_root_only_tree(
        root_centroids: Vec<(VectorId, Vec<f32>)>,
    ) -> crate::write::indexer::tree::test_utils::IndexerOpTestHarness {
        let mut builder = IndexerOpTestHarnessBuilder::new(Vec::<MetadataFieldSpec>::new());
        for (centroid_id, vector) in root_centroids {
            builder = builder.with_leaf_centroid(centroid_id, vector, Vec::<TestDataVector>::new());
        }
        builder.build(DIMS).await
    }

    #[tokio::test]
    async fn should_not_split_root_when_below_threshold() {
        // given
        let mut h = make_root_only_tree(vec![(C0, vec![1.0, 0.0]), (C1, vec![0.0, 1.0])]).await;
        let opts = create_opts(3);
        let snapshot = h.snapshot().await;
        let original_depth = h.state.centroids_meta().depth;
        let original_root_count = h.state.root_centroid_count();

        // when
        let mut delta = VectorIndexDelta::new(&h.state);
        SplitRoot::new(&opts, &snapshot, 1)
            .execute(&mut delta, &h.state)
            .await
            .unwrap();
        h.apply_delta(delta).await;

        // then
        assert_eq!(h.state.centroids_meta().depth, original_depth);
        assert_eq!(h.state.root_centroid_count(), original_root_count);
        let root =
            PostingList::from_value(h.storage.get_root_posting_list(DIMS).await.unwrap(), false);
        let root_ids: HashSet<_> = root.iter().map(|p| p.id()).collect();
        assert_eq!(root_ids, HashSet::from([C0, C1]));
    }

    #[tokio::test]
    async fn should_split_root_when_above_threshold_and_reparent_old_root_centroids() {
        // given
        let mut h = make_root_only_tree(vec![
            (C0, vec![1.0, 0.0]),
            (C1, vec![0.9, 0.1]),
            (C2, vec![0.0, 1.0]),
            (C3, vec![0.1, 0.9]),
        ])
        .await;
        let opts = create_opts(4);
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);

        // when:
        SplitRoot::new(&opts, &snapshot, 1)
            .execute(&mut delta, &h.state)
            .await
            .unwrap();
        h.apply_delta(delta).await;

        // then
        assert_eq!(h.state.centroids_meta().depth, 4);
        assert_eq!(h.state.root_centroid_count(), 2);
        let root =
            PostingList::from_value(h.storage.get_root_posting_list(DIMS).await.unwrap(), false);
        assert_eq!(root.len(), 2);
        let new_root_ids: Vec<_> = root.iter().map(|p| p.id()).collect();
        assert!(new_root_ids.iter().all(|id| id.level() == 2));
        let mut all_old_children = HashSet::new();
        for new_root_id in &new_root_ids {
            let posting = PostingList::from_value(
                h.storage
                    .get_posting_list(*new_root_id, DIMS)
                    .await
                    .unwrap(),
                false,
            );
            assert!(!posting.is_empty());
            for child in posting.iter() {
                all_old_children.insert(child.id());
            }
        }
        assert_eq!(all_old_children, HashSet::from([C0, C1, C2, C3]));
        for old_root_centroid in [C0, C1, C2, C3] {
            let info = h
                .storage
                .get_centroid_info(old_root_centroid)
                .await
                .unwrap()
                .unwrap();
            assert!(new_root_ids.contains(&info.parent_vector_id));
            assert_eq!(info.level, 1);
        }
    }
}
