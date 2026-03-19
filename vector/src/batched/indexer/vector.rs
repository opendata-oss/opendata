use crate::Error::Internal;
use crate::Result;
use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::indexer::IndexerOpts;
use crate::batched::indexer::split::ReassignVector;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::delta::VectorWrite;
use crate::model::VECTOR_FIELD_NAME;
use crate::serde::FieldValue;
use crate::serde::vector_data::VectorDataValue;
use common::StorageRead;
use futures::future::BoxFuture;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;
use tracing::debug;

/// An upsert where we need to resolve the old vector data from storage.
struct ResolvedUpsert {
    write: VectorWrite,
    old: (u64, VectorDataValue),
}

pub(crate) struct WriteVectors {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    writes: Vec<VectorWrite>,
}

impl WriteVectors {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        writes: Vec<VectorWrite>,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            writes,
        }
    }

    pub(crate) async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<()> {
        if self.writes.is_empty() {
            return Ok(());
        }
        let view = VectorIndexView::new(delta, state, self.snapshot.clone());

        // compact so last write for each external id wins
        let writes = Self::compact_writes(self.writes);

        // Partition into inserts (new vectors) vs upserts (existing external_id).
        // Centroid assignment is computed separately on the blocking pool so it can run
        // in parallel with the upsert storage reads.
        let centroid_graph = view.centroid_graph();
        let assignment_inputs: Vec<_> = writes
            .iter()
            .map(|write| (write.external_id.clone(), write.values.clone()))
            .collect();
        let assignment_handle =
            task::spawn_blocking(move || Self::assign_centroids(assignment_inputs, centroid_graph));

        let mut inserts = Vec::with_capacity(writes.len());
        let mut upsert_futures: Vec<BoxFuture<'static, Result<ResolvedUpsert>>> = Vec::new();

        for w in writes {
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
            tokio::join!(assignment_handle, AsyncBatchDriver::execute(upsert_futures));
        let centroid_assignments = centroid_assignments
            .map_err(|e| Internal(format!("centroid assignment task failed: {e}")))??;
        let mut upserts = Vec::with_capacity(upsert_results.len());
        for result in upsert_results {
            upserts.push(result?);
        }
        drop(view);

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
            let vector_id = delta.add_vector(&insert.external_id, &insert.attributes);
            delta.add_to_posting(centroid, vector_id, insert.values.clone());
            for (attr_name, attr_value) in &insert.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.opts.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta.add_to_inverted_index(attr_name.clone(), field_value, vector_id);
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
            delta.delete_vector(old_vector_id);
            // todo: delete from old postings and inverted index
            let vector_id = delta.add_vector(&upsert.write.external_id, &upsert.write.attributes);
            delta.add_to_posting(centroid, vector_id, upsert.write.values.clone());
            for (attr_name, attr_value) in &upsert.write.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.opts.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta.add_to_inverted_index(attr_name.clone(), field_value, vector_id);
            }
        }
        Ok(())
    }

    fn assign_centroids(
        writes: Vec<(String, Vec<f32>)>,
        centroid_graph: Arc<crate::batched::indexer::state::DirtyCentroidGraph>,
    ) -> Result<HashMap<String, u64>> {
        let assignments: Vec<_> = writes
            .into_par_iter()
            .map(|(external_id, values)| {
                centroid_graph
                    .search(&values, 1)
                    .first()
                    .copied()
                    .ok_or_else(|| Internal("no centroids found".to_string()))
                    .map(|centroid| (external_id, centroid))
            })
            .collect();

        let mut centroid_assignments = HashMap::with_capacity(assignments.len());
        for assignment in assignments {
            let (external_id, centroid) = assignment?;
            centroid_assignments.insert(external_id, centroid);
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
    centroid: u64,
}

struct ResolvedVectorReassignment {
    reassignment: ReassignVector,
    data: VectorDataValue,
    centroid: u64,
}

pub(crate) struct ReassignVectors {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    reassignments: Vec<ReassignVector>,
}

impl ReassignVectors {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        reassignments: Vec<ReassignVector>,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            reassignments,
        }
    }

    pub(crate) async fn execute(
        mut self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<()> {
        if self.reassignments.is_empty() {
            return Ok(());
        }
        let view = VectorIndexView::new(delta, state, self.snapshot);
        let centroid_graph = view.centroid_graph();

        // update current centroid in case centroid was moved as part of a split/merge
        self.reassignments.iter_mut().for_each(|r| {
            if let Some(p) = view.last_written_posting(r.vector_id) {
                r.current_centroid = p;
            }
        });

        // determine which vectors actually need a new assignment
        let reassignments: Vec<_> = self
            .reassignments
            .into_par_iter()
            .filter_map(|r| {
                let &closest_centroid = centroid_graph
                    .search(&r.vector, 1)
                    .first()
                    .expect("no centroids");
                if closest_centroid == r.current_centroid {
                    None
                } else {
                    Some(VerifiedVectorReassignment {
                        reassignment: r,
                        centroid: closest_centroid,
                    })
                }
            })
            .collect();

        // pull the old vector data so we can update inverted indexes
        let mut to_resolve = Vec::with_capacity(reassignments.len());
        for r in reassignments {
            let data_fut = view.vector_data(r.reassignment.vector_id, self.opts.dimensions);
            to_resolve.push(Box::pin(async move {
                Ok(ResolvedVectorReassignment {
                    reassignment: r.reassignment,
                    data: data_fut.await?.expect("missing vector data"),
                    centroid: r.centroid,
                })
            })
                as BoxFuture<Result<ResolvedVectorReassignment>>);
        }
        let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
        let mut resolved = Vec::with_capacity(resolve_results.len());
        for result in resolve_results {
            resolved.push(result?);
        }
        drop(view);

        // execute the reassignments
        for r in resolved {
            debug!("old data: {:?}", r.data);
            delta.remove_from_posting(r.reassignment.current_centroid, r.reassignment.vector_id);
            delta.add_to_posting(r.centroid, r.reassignment.vector_id, r.reassignment.vector);
        }
        Ok(())
    }
}
