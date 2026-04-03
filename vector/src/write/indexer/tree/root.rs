use crate::Result;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use common::StorageRead;
use std::collections::HashMap;
use std::sync::Arc;
use crate::write::indexer::tree::centroids::{SearchResult};

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
                let root_search = centroid_index
                    .search_root(posting.vector(), 1);
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
