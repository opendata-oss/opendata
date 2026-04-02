use crate::Error;
use crate::Result;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::centroids::CentroidsValue;
use crate::serde::key::{CentroidInfoKey, CentroidStatsKey, CentroidsKey, PostingListKey};
use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use crate::storage::VectorDbStorageReadExt;
use crate::storage::merge_operator::VectorDbMergeOperator;
use crate::write::indexer::tree::centroids::{AllCentroidsCacheWriter, CentroidCache};
use crate::write::indexer::tree::posting_list::{IntoTreePostingList, Posting, PostingList};
use crate::write::indexer::tree::state::VectorIndexState;
use bytes::Bytes;
use common::storage::StorageSnapshot;
use common::storage::in_memory::InMemoryStorage;
use common::{Record, SequenceAllocator, Storage, StorageRead};
use log::info;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

pub(crate) async fn validate(
    snapshot: Arc<dyn StorageSnapshot>,
    state: &VectorIndexState,
    dimensions: usize,
) -> Result<()> {
    let validate_start = Instant::now();
    let centroids_meta = snapshot
        .get_centroids_meta()
        .await?
        .ok_or_else(|| Error::Internal("missing centroid tree metadata".to_string()))?;
    if centroids_meta.depth == 0 {
        return Err(Error::Internal(
            "centroid tree depth must be at least 1".to_string(),
        ));
    }

    let root_posting_list =
        PostingList::from_value(snapshot.get_root_posting_list(dimensions).await?);
    let centroid_info: HashMap<u64, CentroidInfoValue> = snapshot
        .scan_all_centroid_info()
        .await?
        .into_iter()
        .collect();
    let centroid_counts = load_centroid_counts(snapshot.as_ref()).await?;
    let centroid_postings = load_centroid_postings(snapshot.as_ref(), dimensions).await?;

    let mut reachable_centroids = HashSet::new();
    let mut reachable_counts = HashMap::new();
    validate_root(
        &centroids_meta,
        &root_posting_list,
        &centroid_info,
        &centroid_counts,
        &centroid_postings,
        &mut reachable_centroids,
        &mut reachable_counts,
    )?;
    validate_reachability(
        &centroid_info,
        &centroid_counts,
        &centroid_postings,
        &reachable_centroids,
        &reachable_counts,
    )?;
    validate_state_matches_storage(
        state,
        &centroids_meta,
        &root_posting_list,
        &centroid_info,
        &centroid_counts,
        &centroid_postings,
    )?;

    debug!(
        op = "validate_tree_index",
        elapsed_ms = validate_start.elapsed().as_millis() as u64,
        centroid_count = centroid_info.len(),
        stats_count = centroid_counts.len(),
        "completed"
    );
    Ok(())
}

fn validate_root(
    centroids_meta: &CentroidsValue,
    root_posting_list: &PostingList,
    centroid_info: &HashMap<u64, CentroidInfoValue>,
    centroid_counts: &HashMap<(u8, u64), u64>,
    centroid_postings: &HashMap<u64, PostingList>,
    reachable_centroids: &mut HashSet<u64>,
    reachable_counts: &mut HashMap<(u8, u64), u64>,
) -> Result<()> {
    info!("validate root");
    let root_level = centroids_meta.depth.saturating_sub(1);
    let mut root_children = HashSet::new();
    for posting in root_posting_list.iter() {
        if !root_children.insert(posting.id()) {
            return Err(Error::Internal(format!(
                "duplicate centroid {} in root posting list",
                posting.id()
            )));
        }
        let centroid = centroid_info.get(&posting.id()).ok_or_else(|| {
            Error::Internal(format!(
                "root references centroid {} that has no centroid info",
                posting.id()
            ))
        })?;
        validate_centroid_reference(
            posting.id(),
            centroid,
            posting.vector(),
            root_level,
            None,
            reachable_centroids,
        )?;
        validate_centroid_subtree(
            posting.id(),
            centroid,
            centroid_info,
            centroid_counts,
            centroid_postings,
            reachable_centroids,
            reachable_counts,
        )?;
    }
    info!("validated root successfully");
    Ok(())
}

fn validate_centroid_subtree(
    centroid_id: u64,
    centroid: &CentroidInfoValue,
    centroid_info: &HashMap<u64, CentroidInfoValue>,
    centroid_counts: &HashMap<(u8, u64), u64>,
    centroid_postings: &HashMap<u64, PostingList>,
    reachable_centroids: &mut HashSet<u64>,
    reachable_counts: &mut HashMap<(u8, u64), u64>,
) -> Result<()> {
    info!("validate centroid {} subtree", centroid_id);
    let posting_list = centroid_postings
        .get(&centroid_id)
        .cloned()
        .unwrap_or_else(PostingList::empty);
    let actual_count = posting_list.len() as u64;
    let Some(stored_count) = centroid_counts.get(&(centroid.level, centroid_id)) else {
        return Err(Error::Internal(format!(
            "missing centroid stats for level {}/{}",
            centroid.level, centroid_id
        )));
    };
    if *stored_count != actual_count {
        return Err(Error::Internal(format!(
            "centroid stats mismatch for level {}/{}: stats={}, postings={}",
            centroid.level, centroid_id, stored_count, actual_count
        )));
    }
    if reachable_counts
        .insert((centroid.level, centroid_id), actual_count)
        .is_some()
    {
        return Err(Error::Internal(format!(
            "centroid stats visited multiple times for level {}/{}",
            centroid.level, centroid_id
        )));
    }

    if centroid.level == 0 {
        return Ok(());
    }

    let expected_child_level = centroid.level - 1;
    let mut child_ids = HashSet::new();
    for child in posting_list.iter() {
        if !child_ids.insert(child.id()) {
            return Err(Error::Internal(format!(
                "duplicate child centroid {} in posting list for {}/{}",
                child.id(),
                centroid.level,
                centroid_id
            )));
        }
        let child_info = centroid_info.get(&child.id()).ok_or_else(|| {
            Error::Internal(format!(
                "centroid {}/{} references child {} with no centroid info",
                centroid.level,
                centroid_id,
                child.id()
            ))
        })?;
        validate_centroid_reference(
            child.id(),
            child_info,
            child.vector(),
            expected_child_level,
            Some(centroid_id),
            reachable_centroids,
        )?;
        validate_centroid_subtree(
            child.id(),
            child_info,
            centroid_info,
            centroid_counts,
            centroid_postings,
            reachable_centroids,
            reachable_counts,
        )?;
    }
    Ok(())
}

fn validate_centroid_reference(
    centroid_id: u64,
    centroid: &CentroidInfoValue,
    vector: &[f32],
    expected_level: u8,
    expected_parent: Option<u64>,
    reachable_centroids: &mut HashSet<u64>,
) -> Result<()> {
    if centroid.level != expected_level {
        return Err(Error::Internal(format!(
            "centroid {} has level {}, expected {}",
            centroid_id, centroid.level, expected_level
        )));
    }
    if centroid.parent_vector_id != expected_parent {
        return Err(Error::Internal(format!(
            "centroid {}/{} has parent {:?}, expected {:?}",
            centroid.level, centroid_id, centroid.parent_vector_id, expected_parent
        )));
    }
    if centroid.vector.as_slice() != vector {
        return Err(Error::Internal(format!(
            "centroid {}/{} vector does not match parent posting entry",
            centroid.level, centroid_id
        )));
    }
    if !reachable_centroids.insert(centroid_id) {
        return Err(Error::Internal(format!(
            "centroid {}/{} is reachable multiple times",
            centroid.level, centroid_id
        )));
    }
    Ok(())
}

fn validate_reachability(
    centroid_info: &HashMap<u64, CentroidInfoValue>,
    centroid_counts: &HashMap<(u8, u64), u64>,
    centroid_postings: &HashMap<u64, PostingList>,
    reachable_centroids: &HashSet<u64>,
    reachable_counts: &HashMap<(u8, u64), u64>,
) -> Result<()> {
    let stale_centroids = centroid_info
        .keys()
        .filter(|centroid_id| !reachable_centroids.contains(centroid_id))
        .copied()
        .collect::<BTreeSet<_>>();
    if !stale_centroids.is_empty() {
        return Err(Error::Internal(format!(
            "stale centroid info entries found: {:?}",
            stale_centroids
        )));
    }

    let stale_counts = centroid_counts
        .keys()
        .filter(|key| !reachable_counts.contains_key(key))
        .copied()
        .collect::<BTreeSet<_>>();
    if !stale_counts.is_empty() {
        return Err(Error::Internal(format!(
            "stale centroid stats entries found: {:?}",
            stale_counts
        )));
    }

    let stale_postings = centroid_postings
        .keys()
        .filter(|centroid_id| **centroid_id != 0 && !reachable_centroids.contains(centroid_id))
        .copied()
        .collect::<BTreeSet<_>>();
    if !stale_postings.is_empty() {
        return Err(Error::Internal(format!(
            "stale centroid posting lists found: {:?}",
            stale_postings
        )));
    }

    Ok(())
}

fn validate_state_matches_storage(
    state: &VectorIndexState,
    centroids_meta: &CentroidsValue,
    root_posting_list: &PostingList,
    centroid_info: &HashMap<u64, CentroidInfoValue>,
    centroid_counts: &HashMap<(u8, u64), u64>,
    centroid_postings: &HashMap<u64, PostingList>,
) -> Result<()> {
    if state.centroids_meta() != centroids_meta {
        return Err(Error::Internal(format!(
            "in-memory centroid metadata {:?} does not match storage {:?}",
            state.centroids_meta(),
            centroids_meta
        )));
    }
    if state.root_centroid_count() != root_posting_list.len() as u64 {
        return Err(Error::Internal(format!(
            "in-memory root centroid count {} does not match storage {}",
            state.root_centroid_count(),
            root_posting_list.len()
        )));
    }
    if state.centroids() != centroid_info {
        return Err(Error::Internal(
            "in-memory centroid info map does not match storage".to_string(),
        ));
    }

    let state_counts = flatten_state_counts(state.centroid_counts());
    if state_counts != *centroid_counts {
        return Err(Error::Internal(format!(
            "in-memory centroid stats do not match storage: state={:?}, storage={:?}",
            state_counts, centroid_counts
        )));
    }

    let centroid_cache = state.centroid_cache();
    let Some(cached_root) = centroid_cache.root(u64::MAX) else {
        return Err(Error::Internal(
            "centroid cache is missing the root posting list".to_string(),
        ));
    };
    if cached_root.as_ref() != root_posting_list {
        return Err(Error::Internal(
            "cached root posting list does not match storage".to_string(),
        ));
    }

    for (&centroid_id, centroid) in centroid_info {
        let storage_posting = centroid_postings
            .get(&centroid_id)
            .cloned()
            .unwrap_or_else(PostingList::empty);
        if centroid.level == 0 {
            if centroid_cache.posting(centroid_id, u64::MAX).is_some() {
                return Err(Error::Internal(format!(
                    "leaf centroid {}/{} should not be present in centroid cache",
                    centroid.level, centroid_id
                )));
            }
            continue;
        }

        match centroid_cache.posting(centroid_id, u64::MAX) {
            Some(cached_posting) => {
                let cached_posting = cached_posting
                    .iter()
                    .map(|p| p.id())
                    .collect::<HashSet<_>>();
                let storage_posting = storage_posting
                    .iter()
                    .map(|p| p.id())
                    .collect::<HashSet<_>>();
                if cached_posting != storage_posting {
                    return Err(Error::Internal(format!(
                        "cached posting list for centroid {}/{} does not match storage {:?} {:?}",
                        centroid.level, centroid_id, storage_posting, cached_posting
                    )));
                }
            }
            None if storage_posting.is_empty() => {}
            None => {
                return Err(Error::Internal(format!(
                    "centroid cache is missing internal centroid {}/{}",
                    centroid.level, centroid_id
                )));
            }
        }
    }

    Ok(())
}

async fn load_centroid_counts(snapshot: &dyn StorageRead) -> Result<HashMap<(u8, u64), u64>> {
    let mut counts = HashMap::new();
    for ((level, centroid_id), value) in snapshot.scan_all_centroid_stats().await? {
        if value.num_vectors < 0 {
            return Err(Error::Internal(format!(
                "negative centroid count for level {}/{}: {}",
                level, centroid_id, value.num_vectors
            )));
        }
        counts.insert((level, centroid_id), value.num_vectors as u64);
    }
    Ok(counts)
}

async fn load_centroid_postings(
    snapshot: &dyn StorageRead,
    dimensions: usize,
) -> Result<HashMap<u64, PostingList>> {
    Ok(snapshot
        .scan_all_posting_lists(dimensions)
        .await?
        .into_iter()
        .map(|(centroid_id, posting_list)| (centroid_id, PostingList::from_value(posting_list)))
        .collect())
}

fn flatten_state_counts(
    centroid_counts: &HashMap<u16, HashMap<u64, u64>>,
) -> HashMap<(u8, u64), u64> {
    centroid_counts
        .iter()
        .flat_map(|(&level, counts)| {
            counts.iter().map(move |(&centroid_id, &count)| {
                (
                    u8::try_from(level).expect("level should fit in u8"),
                    centroid_id,
                    count,
                )
            })
        })
        .map(|(level, centroid_id, count)| ((level, centroid_id), count))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::collection_meta::DistanceMetric;

    const DIMS: usize = 2;

    #[tokio::test]
    async fn should_validate_consistent_root_only_tree() {
        let storage = create_storage();
        let root = vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])];
        write_tree(
            &storage,
            1,
            root.clone(),
            vec![
                (1, CentroidInfoValue::new(0, vec![1.0, 0.0], None)),
                (2, CentroidInfoValue::new(0, vec![0.0, 1.0], None)),
            ],
            vec![((0, 1), 0), ((0, 2), 0)],
            vec![],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();
        let state = create_state(storage.clone(), snapshot.clone(), 1, root, vec![]).await;

        let result = validate(snapshot, &state, DIMS).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_reject_stale_centroid_info_entries() {
        let storage = create_storage();
        let root = vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])];
        write_tree(
            &storage,
            1,
            root.clone(),
            vec![
                (1, CentroidInfoValue::new(0, vec![1.0, 0.0], None)),
                (2, CentroidInfoValue::new(0, vec![0.0, 1.0], None)),
                (3, CentroidInfoValue::new(0, vec![2.0, 2.0], None)),
            ],
            vec![((0, 1), 0), ((0, 2), 0), ((0, 3), 0)],
            vec![],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();
        let state = create_state(storage.clone(), snapshot.clone(), 1, root, vec![]).await;

        let result = validate(snapshot, &state, DIMS).await;

        assert_eq!(
            result,
            Err(Error::Internal(
                "stale centroid info entries found: {3}".to_string()
            ))
        );
    }

    fn create_storage() -> Arc<dyn Storage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator::new(DIMS),
        )))
    }

    async fn create_state(
        storage: Arc<dyn Storage>,
        snapshot: Arc<dyn StorageSnapshot>,
        depth: u8,
        root: Vec<(u64, Vec<f32>)>,
        centroid_postings: Vec<(u64, Vec<(u64, Vec<f32>)>)>,
    ) -> VectorIndexState {
        let seq_key = Bytes::from_static(&[0x01, 0x02]);
        let id_allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();
        let centroid_seq_key = Bytes::from_static(&[0x01, 0x03]);
        let centroid_id_allocator = SequenceAllocator::load(storage.as_ref(), centroid_seq_key)
            .await
            .unwrap();
        let (seq_block_key, seq_block) = id_allocator.freeze();
        let (centroid_seq_block_key, centroid_seq_block) = centroid_id_allocator.freeze();

        let centroids: HashMap<u64, CentroidInfoValue> = snapshot
            .scan_all_centroid_info()
            .await
            .unwrap()
            .into_iter()
            .collect();
        let centroid_counts = snapshot
            .scan_all_centroid_stats()
            .await
            .unwrap()
            .into_iter()
            .fold(
                HashMap::new(),
                |mut counts, ((level, centroid_id), value)| {
                    counts
                        .entry(level as u16)
                        .or_insert_with(HashMap::new)
                        .insert(centroid_id, value.num_vectors as u64);
                    counts
                },
            );
        let centroid_cache = AllCentroidsCacheWriter::new(
            Arc::new(
                root.into_iter()
                    .map(|(id, vector)| Posting::new(id, vector))
                    .collect::<PostingList>(),
            ) as Arc<dyn IntoTreePostingList>,
            centroid_postings
                .into_iter()
                .map(|(centroid_id, postings)| {
                    (
                        centroid_id,
                        Arc::new(
                            postings
                                .into_iter()
                                .map(|(id, vector)| Posting::new(id, vector))
                                .collect::<PostingList>(),
                        ) as Arc<dyn IntoTreePostingList>,
                    )
                })
                .collect(),
        );

        let _opts = DistanceMetric::L2;
        VectorIndexState::new(
            HashMap::new(),
            CentroidsValue::new(depth),
            snapshot
                .get_root_posting_list(DIMS)
                .await
                .unwrap()
                .postings
                .len() as u64,
            centroids,
            centroid_counts,
            seq_block_key,
            seq_block,
            centroid_seq_block_key,
            centroid_seq_block,
            centroid_cache,
        )
    }

    async fn write_tree(
        storage: &Arc<dyn Storage>,
        depth: u8,
        root: Vec<(u64, Vec<f32>)>,
        centroid_info: Vec<(u64, CentroidInfoValue)>,
        centroid_stats: Vec<((u8, u64), i32)>,
        centroid_postings: Vec<(u64, Vec<(u64, Vec<f32>)>)>,
    ) {
        let mut ops = Vec::new();
        ops.push(
            Record::new(
                CentroidsKey::new().encode(),
                CentroidsValue::new(depth).encode_to_bytes(),
            )
            .into(),
        );
        ops.push(
            Record::new(
                PostingListKey::new(0).encode(),
                posting_list_value(root).encode_to_bytes(),
            )
            .into(),
        );
        for (centroid_id, value) in centroid_info {
            ops.push(
                Record::new(
                    CentroidInfoKey::new(centroid_id).encode(),
                    value.encode_to_bytes(),
                )
                .into(),
            );
        }
        for ((level, centroid_id), count) in centroid_stats {
            ops.push(
                Record::new(
                    CentroidStatsKey::new(level, centroid_id).encode(),
                    CentroidStatsValue::new(count).encode_to_bytes(),
                )
                .into(),
            );
        }
        for (centroid_id, postings) in centroid_postings {
            ops.push(
                Record::new(
                    PostingListKey::new(centroid_id).encode(),
                    posting_list_value(postings).encode_to_bytes(),
                )
                .into(),
            );
        }
        storage.put(ops).await.unwrap();
    }

    fn posting_list_value(postings: Vec<(u64, Vec<f32>)>) -> PostingListValue {
        PostingListValue::from_posting_updates(
            postings
                .into_iter()
                .map(|(id, vector)| PostingUpdate::append(id, vector))
                .collect(),
        )
        .unwrap()
    }
}
