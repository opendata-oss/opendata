use rayon::iter::ParallelIterator;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use futures::future::BoxFuture;
use rayon::iter::IntoParallelIterator;
use common::StorageRead;
use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::split::ReassignVector;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::delta::VectorWrite;
use crate::Error::Internal;
use crate::model::VECTOR_FIELD_NAME;
use crate::serde::vector_data::VectorDataValue;
use crate::Result;
use crate::serde::FieldValue;

struct ResolvedVectorUpsert {
    write: VectorWrite,
    old: Option<(u64, VectorDataValue)>,
    centroid: u64,
}

struct WriteVectors {
    indexed_fields: HashSet<String>,
    dimensions: usize,
    snapshot: Arc<dyn StorageRead>,
    writes: Vec<VectorWrite>
}

impl WriteVectors {
    async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta
    ) -> Result<()> {
        let view = VectorIndexView::new(
            delta,
            state,
            self.snapshot.clone()
        );

        // compact so last write for each external id wins
        let writes = Self::compact_writes(self.writes);

        // resolve data required for all updates
        let mut to_resolve = Vec::with_capacity(writes.len());
        let centroid_graph = view.centroid_graph();
        for w in writes {
            // this is a hack. we want to do the searches concurrently, but it probably doesn't
            // make sense to do them on the i/o tasks
            let this_centroid_graph = centroid_graph.clone();
            let data_fut = view.vector_data_for_external_id(&w.external_id, self.dimensions);
            to_resolve.push(
                Box::pin(
                    async move {
                        let centroid = this_centroid_graph.search(&w.values, 1);
                        let Some(&centroid) = centroid.first() else {
                            return Err(Internal("no centroids found".to_string()));
                        };
                        Ok(ResolvedVectorUpsert {
                            write: w,
                            old: data_fut.await?,
                            centroid
                        })
                    }
                ) as BoxFuture<Result<ResolvedVectorUpsert>>
            );
        };
        let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
        let mut resolved = Vec::with_capacity(resolve_results.len());
        for result in resolve_results {
            resolved.push(result?);
        }
        drop(view);

        // now do all serial work of updating the delta

        // handle inserts
        for upsert in resolved {
            if let Some((old_vector_id, _old_vector_data)) = upsert.old {
                delta.delete_vector(old_vector_id);
                // todo: delete from old postings and inverted index
            }
            let vector_id = delta.add_vector(
                &upsert.write.external_id,
                &upsert.write.attributes
            );
            delta.add_to_posting(upsert.centroid, vector_id, upsert.write.values.clone());
            for (attr_name, attr_value) in &upsert.write.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta.add_to_inverted_index(attr_name.clone(), field_value, vector_id);
            }
        }
        Ok(())
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

struct ReassignVectors {
    dimensions: usize,
    snapshot: Arc<dyn StorageRead>,
    reassignments: Vec<ReassignVector>
}

impl ReassignVectors {
    async fn execute(
        mut self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta
    ) -> Result<()> {
        let view = VectorIndexView::new(delta, state, self.snapshot);
        let centroid_graph = view.centroid_graph();

        // update current centroid in case centroid was moved as part of a split/merge
        self.reassignments.
            iter_mut()
            .for_each(|r| {
                if let Some(p) = view.last_written_posting(r.vector_id) {
                    r.current_centroid = p;
                }
            });

        // determine which vectors actually need a new assignment
        let reassignments: Vec<_> = self.reassignments
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
            let data_fut = view.vector_data(r.reassignment.vector_id, self.dimensions);
            to_resolve.push(
                Box::pin(
                    async move {
                        Ok(ResolvedVectorReassignment {
                            reassignment: r.reassignment,
                            data: data_fut.await?.expect("missing vector data"),
                            centroid: r.centroid
                        })
                    }
                ) as BoxFuture<Result<ResolvedVectorReassignment>>
            );
        };
        let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
        let mut resolved = Vec::with_capacity(resolve_results.len());
        for result in resolve_results {
            resolved.push(result?);
        }
        drop(view);

        // execute the reassignments
        for r in resolved {
            delta.remove_from_posting(r.reassignment.current_centroid, r.reassignment.vector_id);
            delta.add_to_posting(r.centroid, r.reassignment.vector_id, r.reassignment.vector);
        }
        Ok(())
    }
}