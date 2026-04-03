use crate::Error::Internal;
use crate::Result;
use crate::model::VECTOR_FIELD_NAME;
use crate::serde::FieldValue;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::vector_data::VectorDataValue;
use crate::write::delta::VectorWrite;
use crate::write::indexer::drivers::AsyncBatchDriver;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::centroids::{LeveledCentroidIndex, batch_search_centroids, batch_search_centroids_in_level, TreeLevel};
use crate::write::indexer::tree::split::ReassignVector;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use common::StorageRead;
use futures::future::BoxFuture;
use log::debug;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::trace;
use crate::serde::vector_id::VectorId;

/// An upsert where we need to resolve the old vector data from storage.
struct ResolvedUpsert {
    write: VectorWrite,
    old: (VectorId, VectorDataValue),
}

pub(crate) struct WriteVectors {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
    writes: Vec<VectorWrite>,
}

impl WriteVectors {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        writes: Vec<VectorWrite>,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            snapshot_epoch,
            writes,
        }
    }

    /// Returns (inserts, updates) counts.
    pub(crate) async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<(usize, usize)> {
        if self.writes.is_empty() {
            return Ok((0, 0));
        }
        let (inserts, upserts, centroid_assignments) = {
            let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);

            // compact so last write for each external id wins
            let writes = Self::compact_writes(self.writes);

            // Partition into inserts (new vectors) vs upserts (existing external_id).
            // Centroid assignment is computed separately on the blocking pool so it can run
            // in parallel with the upsert storage reads.
            let centroid_index =
                view.centroid_index(self.opts.dimensions, self.opts.distance_metric);
            let assignment_inputs: Vec<_> = writes
                .iter()
                .map(|write| (write.external_id.clone(), write.values.as_slice()))
                .collect();
            let assignments_fut = Self::assign_centroids(assignment_inputs, &centroid_index);

            let mut inserts = Vec::with_capacity(writes.len());
            let mut upsert_futures: Vec<BoxFuture<'static, Result<ResolvedUpsert>>> = Vec::new();

            for w in writes.clone() {
                let Some(old_vector_id) = view.vector_id(&w.external_id) else {
                    inserts.push(w);
                    continue;
                };
                // Upsert — need to read old vector data from storage
                let data_fut = view.vector_data(old_vector_id, self.opts.dimensions);
                upsert_futures.push(Box::pin(async move {
                    let old_data = data_fut.await?.ok_or_else(|| {
                        Internal(format!("missing vector data for id {}", old_vector_id))
                    })?;
                    Ok(ResolvedUpsert {
                        write: w,
                        old: (old_vector_id, old_data),
                    })
                }));
            }

            // Resolve upserts concurrently (bounded by AsyncBatchDriver) while the
            // centroid assignments run on the blocking pool.
            let (centroid_assignments, upsert_results) =
                tokio::join!(assignments_fut, AsyncBatchDriver::execute(upsert_futures));
            let centroid_assignments = centroid_assignments?;
            let mut upserts = Vec::with_capacity(upsert_results.len());
            for result in upsert_results {
                upserts.push(result?);
            }
            (inserts, upserts, centroid_assignments)
        };

        let insert_count = inserts.len();
        let update_count = upserts.len();

        // Apply inserts to delta (no old data to clean up)
        for insert in inserts {
            let centroid = *centroid_assignments
                .get(&insert.external_id)
                .ok_or_else(|| {
                    Internal(format!(
                        "missing centroid assignment for external_id={}",
                        insert.external_id
                    ))
                })?;
            let vector_id = delta
                .forward_index
                .add_vector(&insert.external_id, &insert.attributes);
            delta
                .search_index
                .add_to_posting(centroid, vector_id, insert.values.clone());
            for (attr_name, attr_value) in &insert.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.opts.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta
                    .search_index
                    .add_to_inverted_index(attr_name.clone(), field_value, vector_id);
            }
        }

        // Apply upserts to delta
        for upsert in upserts {
            let centroid = *centroid_assignments
                .get(&upsert.write.external_id)
                .ok_or_else(|| {
                    Internal(format!(
                        "missing centroid assignment for external_id={}",
                        upsert.write.external_id
                    ))
                })?;
            let (old_vector_id, _old_vector_data) = upsert.old;
            delta.forward_index.delete_vector(old_vector_id);
            // todo: delete from old postings and inverted index
            let vector_id = delta
                .forward_index
                .add_vector(&upsert.write.external_id, &upsert.write.attributes);
            delta
                .search_index
                .add_to_posting(centroid, vector_id, upsert.write.values.clone());
            for (attr_name, attr_value) in &upsert.write.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.opts.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta
                    .search_index
                    .add_to_inverted_index(attr_name.clone(), field_value, vector_id);
            }
        }
        Ok((insert_count, update_count))
    }

    async fn assign_centroids(
        writes: Vec<(String, &[f32])>,
        centroid_index: &LeveledCentroidIndex<'_>,
    ) -> Result<HashMap<String, VectorId>> {
        let search_result = batch_search_centroids(centroid_index, 1, writes).await?;
        let mut centroid_assignments = HashMap::with_capacity(search_result.len());
        for (external_id, centroids) in search_result {
            let Some(centroid) = centroids.first() else {
                return Err(Internal("no centroids found".to_string()));
            };
            centroid_assignments.insert(external_id, centroid.id());
        }
        Ok(centroid_assignments)
    }

    fn compact_writes(updates: Vec<VectorWrite>) -> Vec<VectorWrite> {
        let mut compacted = HashMap::with_capacity(updates.len());
        for update in updates {
            compacted.insert(update.external_id.clone(), update);
        }
        compacted.into_values().collect()
    }
}

struct VerifiedVectorReassignment {
    reassignment: ReassignVector,
    new_centroid: VectorId,
}

struct ResolvedVectorReassignment {
    reassignment: ReassignVector,
    data: VectorDataValue,
    new_centroid: VectorId,
}

struct ResolvedCentroidReassignment {
    reassignment: ReassignVector,
    data: CentroidInfoValue,
    new_centroid: VectorId,
}

pub(crate) struct ReassignVectors {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
    reassignments: Vec<ReassignVector>,
    level: TreeLevel,
}

impl ReassignVectors {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        reassignments: Vec<ReassignVector>,
        level: TreeLevel,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            snapshot_epoch,
            reassignments,
            level,
        }
    }

    /// Returns the number of vectors actually reassigned.
    pub(crate) async fn execute(
        mut self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<usize> {
        if self.reassignments.is_empty() {
            return Ok(0);
        }
        // sanity check we don't have any double reassignments
        let ids = self
            .reassignments
            .iter()
            .map(|r| r.vector_id)
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), self.reassignments.len());
        let reassignments = {
            let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);
            let centroid_index =
                view.centroid_index(self.opts.dimensions, self.opts.distance_metric);

            // update current centroid in case centroid was moved as part of a split/merge
            self.reassignments.iter_mut().for_each(|r| {
                assert_eq!(r.vector_id.level() + 1, self.level.level());
                assert_eq!(r.level, self.level);
                if let Some(p) = view.last_written_posting(r.vector_id) {
                    r.current_centroid = p;
                }
            });

            let ann_search_batch: Vec<_> = self
                .reassignments
                .iter()
                .map(|r| (r.vector_id, r.vector.as_slice()))
                .collect();
            let assignments = batch_search_centroids_in_level(
                &centroid_index,
                1,
                ann_search_batch,
                self.level,
            )
                .await?;

            // determine which vectors actually need a new assignment
            let reassignments: Vec<_> = self
                .reassignments
                .into_iter()
                .filter_map(|r| {
                    let closest_centroid = assignments
                        .get(&r.vector_id)
                        .expect("no centroids")
                        .first()
                        .expect("no centroids")
                        .id();
                    if closest_centroid == r.current_centroid {
                        None
                    } else {
                        Some(VerifiedVectorReassignment {
                            reassignment: r,
                            new_centroid: closest_centroid,
                        })
                    }
                })
                .collect();
            reassignments
        };

        if self.level.is_leaf() {
            Self::execute_vector_reassignments(
                &self.opts,
                &self.snapshot,
                self.snapshot_epoch,
                state,
                delta,
                reassignments
            ).await
        } else {
            Self::execute_centroid_reassignments(
                &self.snapshot,
                self.snapshot_epoch,
                state,
                delta,
                reassignments
            ).await
        }
    }

    pub(crate) async fn execute_centroid_reassignments(
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
        reassignments: Vec<VerifiedVectorReassignment>
    ) -> Result<usize> {
        let resolved = {
            let view = VectorIndexView::new(delta, state, snapshot, snapshot_epoch);
            reassignments
                .into_iter()
                .map(|r| ResolvedCentroidReassignment {
                    data: view.centroid(r.reassignment.vector_id).cloned().expect("unexpected missing centroid"),
                    reassignment: r.reassignment,
                    new_centroid: r.new_centroid,
                })
                .collect::<Vec<_>>()
        };
        let nreassigned = resolved.len();
        for mut r in resolved {
            delta.search_index.remove_from_posting(
                r.reassignment.current_centroid,
                r.reassignment.vector_id,
            );
            delta.search_index.add_to_posting(
                r.new_centroid,
                r.reassignment.vector_id,
                r.reassignment.vector,
            );
            r.data.parent_vector_id = r.new_centroid;
            delta
                .search_index
                .update_centroid(r.reassignment.vector_id, r.data);
        }
        Ok(nreassigned)
    }

    pub(crate) async fn execute_vector_reassignments(
        opts: &IndexerOpts,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
        reassignments: Vec<VerifiedVectorReassignment>
    ) -> Result<usize> {
        let resolved = {
            let view = VectorIndexView::new(delta, state, &snapshot, snapshot_epoch);
            // pull the old vector data so we can update inverted indexes
            let mut to_resolve = Vec::with_capacity(reassignments.len());
            for r in reassignments {
                let data_fut = view.vector_data(r.reassignment.vector_id, opts.dimensions);
                to_resolve.push(Box::pin(async move {
                    Ok(ResolvedVectorReassignment {
                        reassignment: r.reassignment,
                        // TODO: this will panic on update - we need to delete vector from old posting
                        data: data_fut.await?.expect("missing vector data"),
                        new_centroid: r.new_centroid,
                    })
                })
                    as BoxFuture<Result<ResolvedVectorReassignment>>);
            }
            let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
            let mut resolved = Vec::with_capacity(resolve_results.len());
            for result in resolve_results {
                resolved.push(result?);
            }
            resolved
        };
        let nreassigned = resolved.len();
        for r in resolved {
            trace!("old data: {:?}", r.data);
            delta.search_index.remove_from_posting(
                r.reassignment.current_centroid,
                r.reassignment.vector_id,
            );
            delta.search_index.add_to_posting(
                r.new_centroid,
                r.reassignment.vector_id,
                r.reassignment.vector,
            );
        }
        Ok(nreassigned)
    }
}
