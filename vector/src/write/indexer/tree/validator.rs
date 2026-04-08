use crate::Error;
use crate::Result;
use crate::db::LastAppliedSnapshot;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::centroids::CentroidsValue;
use crate::serde::vector_id::{LEAF_LEVEL, ROOT_VECTOR_ID, VectorId};
use crate::storage::VectorDbStorageReadExt;
use crate::write::indexer::tree::centroids::{CentroidCache, TreeDepth, TreeLevel};
use crate::write::indexer::tree::posting_list::PostingList;
use crate::write::indexer::tree::state::VectorIndexState;
use common::StorageRead;
use common::storage::StorageSnapshot;
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
    let centroid_info: HashMap<VectorId, CentroidInfoValue> = snapshot
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

pub(crate) async fn validate_state_and_storage_consistent(
    last_applied_snapshot: &LastAppliedSnapshot,
    dimensions: usize,
) -> Result<()> {
    let snapshot = last_applied_snapshot.snapshot.clone();
    let _centroids_meta = snapshot
        .get_centroids_meta()
        .await?
        .ok_or_else(|| Error::Internal("missing centroid tree metadata".to_string()))?;
    let storage_root = PostingList::from_value(snapshot.get_root_posting_list(dimensions).await?);
    let cached_root = last_applied_snapshot
        .centroid_cache
        .root(u64::MAX)
        .ok_or_else(|| {
            Error::Internal("centroid cache is missing the root posting list".to_string())
        })?;
    validate_exact_posting_list_match(ROOT_VECTOR_ID, &storage_root, cached_root.as_ref())?;

    let centroid_postings = load_centroid_postings(snapshot.as_ref(), dimensions).await?;
    validate_cached_subtree(
        last_applied_snapshot.centroid_cache.as_ref(),
        &centroid_postings,
        &storage_root,
    )?;
    Ok(())
}

fn validate_root(
    centroids_meta: &CentroidsValue,
    root_posting_list: &PostingList,
    centroid_info: &HashMap<VectorId, CentroidInfoValue>,
    centroid_counts: &HashMap<(u8, VectorId), u64>,
    centroid_postings: &HashMap<VectorId, PostingList>,
    reachable_centroids: &mut HashSet<VectorId>,
    reachable_counts: &mut HashMap<(u8, VectorId), u64>,
) -> Result<()> {
    debug!("validate root");
    let depth = TreeDepth::of(centroids_meta.depth);
    let root_level = TreeLevel::root(depth);
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
            root_level.next_level_down(),
            ROOT_VECTOR_ID,
            reachable_centroids,
        )?;
        validate_centroid_subtree(
            root_level.next_level_down(),
            posting.id(),
            centroid,
            centroid_info,
            centroid_counts,
            centroid_postings,
            reachable_centroids,
            reachable_counts,
        )?;
    }
    debug!("validated root successfully");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_centroid_subtree(
    level: TreeLevel,
    centroid_id: VectorId,
    centroid: &CentroidInfoValue,
    centroid_info: &HashMap<VectorId, CentroidInfoValue>,
    centroid_counts: &HashMap<(u8, VectorId), u64>,
    centroid_postings: &HashMap<VectorId, PostingList>,
    reachable_centroids: &mut HashSet<VectorId>,
    reachable_counts: &mut HashMap<(u8, VectorId), u64>,
) -> Result<()> {
    debug!("validate centroid {} subtree", centroid_id);
    assert_eq!(level.level(), centroid_id.level());
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

    if centroid.level == LEAF_LEVEL {
        return Ok(());
    }

    let expected_child_level = level.next_level_down();
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
            centroid_id,
            reachable_centroids,
        )?;
        validate_centroid_subtree(
            expected_child_level,
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
    centroid_id: VectorId,
    centroid: &CentroidInfoValue,
    vector: &[f32],
    expected_level: TreeLevel,
    expected_parent: VectorId,
    reachable_centroids: &mut HashSet<VectorId>,
) -> Result<()> {
    if centroid.level != expected_level.level() {
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
    centroid_info: &HashMap<VectorId, CentroidInfoValue>,
    centroid_counts: &HashMap<(u8, VectorId), u64>,
    centroid_postings: &HashMap<VectorId, PostingList>,
    reachable_centroids: &HashSet<VectorId>,
    reachable_counts: &HashMap<(u8, VectorId), u64>,
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
        .filter(|centroid_id| {
            **centroid_id != ROOT_VECTOR_ID && !reachable_centroids.contains(centroid_id)
        })
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
    centroid_info: &HashMap<VectorId, CentroidInfoValue>,
    centroid_counts: &HashMap<(u8, VectorId), u64>,
    centroid_postings: &HashMap<VectorId, PostingList>,
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
                assert!(centroid_id.level() > 1);
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
                if centroid_id.level() > 1 {
                    return Err(Error::Internal(format!(
                        "centroid cache is missing internal centroid {}/{}",
                        centroid.level, centroid_id
                    )));
                }
            }
        }
    }

    Ok(())
}

fn validate_cached_subtree(
    centroid_cache: &dyn CentroidCache,
    centroid_postings: &HashMap<VectorId, PostingList>,
    posting_list: &PostingList,
) -> Result<()> {
    for posting in posting_list.iter() {
        let centroid_id = posting.id();
        if centroid_id.level() == LEAF_LEVEL {
            if centroid_cache.posting(centroid_id, u64::MAX).is_some() {
                return Err(Error::Internal(format!(
                    "leaf centroid {} should not be present in centroid cache",
                    centroid_id
                )));
            }
            continue;
        }

        let storage_posting = centroid_postings.get(&centroid_id).ok_or_else(|| {
            Error::Internal(format!(
                "storage is missing posting list for cached centroid {}",
                centroid_id
            ))
        })?;
        let cached_posting = centroid_cache
            .posting(centroid_id, u64::MAX)
            .ok_or_else(|| {
                Error::Internal(format!(
                    "centroid cache is missing internal centroid {}",
                    centroid_id
                ))
            })?;
        validate_exact_posting_list_match(centroid_id, storage_posting, cached_posting.as_ref())?;
        validate_cached_subtree(centroid_cache, centroid_postings, storage_posting)?;
    }
    Ok(())
}

fn validate_exact_posting_list_match(
    centroid_id: VectorId,
    storage_posting: &PostingList,
    cached_posting: &PostingList,
) -> Result<()> {
    debug!(
        "cached: {:?}",
        cached_posting.iter().map(|p| p.id()).collect::<Vec<_>>()
    );
    debug!(
        "root: {:?}",
        storage_posting.iter().map(|p| p.id()).collect::<Vec<_>>()
    );
    if storage_posting.len() != cached_posting.len() {
        return Err(Error::Internal(format!(
            "posting list length mismatch for {}: storage={}, cache={}",
            centroid_id,
            storage_posting.len(),
            cached_posting.len()
        )));
    }

    let mut storage_posting = storage_posting.iter().cloned().collect::<Vec<_>>();
    storage_posting.sort_by_key(|a| a.id());
    let mut cached_posting = cached_posting.iter().cloned().collect::<Vec<_>>();
    cached_posting.sort_by_key(|a| a.id());

    for (storage, cached) in storage_posting.iter().zip(cached_posting.iter()) {
        if storage != cached {
            return Err(Error::Internal(format!(
                "posting list mismatch for {}: storage={:?}, cache={:?}",
                centroid_id, storage, cached
            )));
        }
    }

    Ok(())
}

async fn load_centroid_counts(snapshot: &dyn StorageRead) -> Result<HashMap<(u8, VectorId), u64>> {
    let mut counts = HashMap::new();
    for (centroid_id, value) in snapshot.scan_all_centroid_stats().await? {
        if value.num_vectors < 0 {
            return Err(Error::Internal(format!(
                "negative centroid count for level {}: {}",
                centroid_id, value.num_vectors
            )));
        }
        counts.insert((centroid_id.level(), centroid_id), value.num_vectors as u64);
    }
    Ok(counts)
}

async fn load_centroid_postings(
    snapshot: &dyn StorageRead,
    dimensions: usize,
) -> Result<HashMap<VectorId, PostingList>> {
    Ok(snapshot
        .scan_all_posting_lists(dimensions)
        .await?
        .into_iter()
        .map(|(centroid_id, posting_list)| (centroid_id, PostingList::from_value(posting_list)))
        .collect())
}

fn flatten_state_counts(
    centroid_counts: &HashMap<u8, HashMap<VectorId, u64>>,
) -> HashMap<(u8, VectorId), u64> {
    centroid_counts
        .iter()
        .flat_map(|(&level, counts)| {
            counts
                .iter()
                .map(move |(&centroid_id, &count)| (level, centroid_id, count))
        })
        .map(|(level, centroid_id, count)| ((level, centroid_id), count))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::LastAppliedSnapshot;
    use crate::serde::centroid_stats::CentroidStatsValue;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::key::{CentroidInfoKey, CentroidStatsKey, CentroidsKey, PostingListKey};
    use crate::serde::posting_list::{PostingListValue, PostingUpdate};
    use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use crate::write::indexer::tree::centroids::{
        AllCentroidsCacheWriter, CachedCentroidReader, CentroidCache, LeveledCentroidIndex,
        StoredCentroidReader,
    };
    use crate::write::indexer::tree::posting_list::Posting;
    use bytes::Bytes;
    use common::storage::in_memory::InMemoryStorage;
    use common::{Record, SequenceAllocator, Storage};

    const DIMS: usize = 2;

    fn centroid_id(level: u8, id: u64) -> VectorId {
        VectorId::centroid_id(level, id)
    }

    fn root_posting_id(depth: u8, id: u64) -> VectorId {
        centroid_id(TreeDepth::of(depth).max_inner_level(), id)
    }

    #[tokio::test]
    async fn should_validate_consistent_root_only_tree() {
        let storage = create_storage();
        let root = vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])];
        write_tree(
            &storage,
            3,
            root.clone(),
            vec![
                (1, CentroidInfoValue::new(1, vec![1.0, 0.0], ROOT_VECTOR_ID)),
                (2, CentroidInfoValue::new(1, vec![0.0, 1.0], ROOT_VECTOR_ID)),
            ],
            vec![((1, 1), 0), ((1, 2), 0)],
            vec![],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();
        let state = create_state(storage.clone(), snapshot.clone(), 3, root, vec![]).await;

        let result = validate(snapshot, &state, DIMS).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_reject_stale_centroid_info_entries() {
        let storage = create_storage();
        let root = vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])];
        write_tree(
            &storage,
            3,
            root.clone(),
            vec![
                (1, CentroidInfoValue::new(1, vec![1.0, 0.0], ROOT_VECTOR_ID)),
                (2, CentroidInfoValue::new(1, vec![0.0, 1.0], ROOT_VECTOR_ID)),
                (3, CentroidInfoValue::new(1, vec![2.0, 2.0], ROOT_VECTOR_ID)),
            ],
            vec![((1, 1), 0), ((1, 2), 0), ((1, 3), 0)],
            vec![],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();
        let state = create_state(storage.clone(), snapshot.clone(), 3, root, vec![]).await;

        let result = validate(snapshot, &state, DIMS).await;

        assert_eq!(
            result,
            Err(Error::Internal(
                "stale centroid info entries found: {1:3}".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn should_validate_cached_root_and_internal_centroids_against_storage() {
        let storage = create_storage();
        let root = vec![(10, vec![1.0, 0.0]), (11, vec![0.0, 1.0])];
        write_tree(
            &storage,
            4,
            root.clone(),
            vec![
                (
                    10,
                    CentroidInfoValue::new(2, vec![1.0, 0.0], ROOT_VECTOR_ID),
                ),
                (
                    11,
                    CentroidInfoValue::new(2, vec![0.0, 1.0], ROOT_VECTOR_ID),
                ),
                (
                    20,
                    CentroidInfoValue::new(1, vec![1.0, 1.0], centroid_id(2, 10)),
                ),
                (
                    21,
                    CentroidInfoValue::new(1, vec![1.0, 2.0], centroid_id(2, 10)),
                ),
                (
                    22,
                    CentroidInfoValue::new(1, vec![2.0, 1.0], centroid_id(2, 11)),
                ),
            ],
            vec![
                ((2, 10), 2),
                ((2, 11), 1),
                ((1, 20), 0),
                ((1, 21), 0),
                ((1, 22), 0),
            ],
            vec![
                (10, vec![(20, vec![1.0, 1.0]), (21, vec![1.0, 2.0])]),
                (11, vec![(22, vec![2.0, 1.0])]),
            ],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();
        let cache = build_last_applied_snapshot(
            storage.clone(),
            snapshot.clone(),
            4,
            root,
            vec![
                (10, vec![(20, vec![1.0, 1.0]), (21, vec![1.0, 2.0])]),
                (11, vec![(22, vec![2.0, 1.0])]),
            ],
        )
        .await;

        let result = validate_state_and_storage_consistent(&cache, DIMS).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_reject_when_cached_internal_posting_differs_from_storage() {
        let storage = create_storage();
        let root = vec![(10, vec![1.0, 0.0])];
        write_tree(
            &storage,
            4,
            root.clone(),
            vec![
                (
                    10,
                    CentroidInfoValue::new(2, vec![1.0, 0.0], ROOT_VECTOR_ID),
                ),
                (
                    20,
                    CentroidInfoValue::new(1, vec![1.0, 1.0], centroid_id(2, 10)),
                ),
            ],
            vec![((2, 10), 1), ((1, 20), 0)],
            vec![(10, vec![(20, vec![1.0, 1.0])])],
        )
        .await;
        let snapshot = storage.snapshot().await.unwrap();
        let cache = build_last_applied_snapshot(
            storage.clone(),
            snapshot.clone(),
            4,
            root,
            vec![(10, vec![(21, vec![9.0, 9.0])])],
        )
        .await;

        let result = validate_state_and_storage_consistent(&cache, DIMS).await;

        assert_eq!(
            result,
            Err(Error::Internal(
                "posting list mismatch for 2:10: storage=Posting { id: 1:20, vector: [1.0, 1.0] }, cache=Posting { id: 1:21, vector: [9.0, 9.0] }".to_string()
            ))
        );
    }

    fn create_storage() -> Arc<dyn Storage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator::new(DIMS),
        )))
    }

    #[allow(clippy::type_complexity)]
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

        let centroids: HashMap<VectorId, CentroidInfoValue> = snapshot
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
            .fold(HashMap::new(), |mut counts, (centroid_id, value)| {
                counts
                    .entry(centroid_id.level())
                    .or_insert_with(HashMap::new)
                    .insert(centroid_id, value.num_vectors as u64);
                counts
            });
        let centroid_cache = AllCentroidsCacheWriter::new(
            Arc::new(
                root.into_iter()
                    .map(|(id, vector)| Posting::new(root_posting_id(depth, id), vector))
                    .collect::<PostingList>(),
            ),
            centroid_postings
                .into_iter()
                .map(|(centroid_num, postings)| {
                    (
                        root_posting_id(depth, centroid_num),
                        Arc::new(
                            postings
                                .into_iter()
                                .map(|(id, vector)| {
                                    Posting::new(VectorId::data_vector_id(id), vector)
                                })
                                .collect::<PostingList>(),
                        ),
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

    #[allow(clippy::type_complexity)]
    async fn build_last_applied_snapshot(
        _storage: Arc<dyn Storage>,
        snapshot: Arc<dyn StorageSnapshot>,
        depth: u8,
        root: Vec<(u64, Vec<f32>)>,
        centroid_postings: Vec<(u64, Vec<(u64, Vec<f32>)>)>,
    ) -> LastAppliedSnapshot {
        let centroid_cache = AllCentroidsCacheWriter::new(
            Arc::new(
                root.clone()
                    .into_iter()
                    .map(|(id, vector)| Posting::new(root_posting_id(depth, id), vector))
                    .collect::<PostingList>(),
            ),
            centroid_postings
                .into_iter()
                .map(|(centroid_num, postings)| {
                    (
                        root_posting_id(depth, centroid_num),
                        Arc::new(
                            postings
                                .into_iter()
                                .map(|(id, vector)| Posting::new(centroid_id(1, id), vector))
                                .collect::<PostingList>(),
                        ),
                    )
                })
                .collect(),
        );
        let query_cache = Arc::new(centroid_cache.cache());
        let stored_reader = StoredCentroidReader::new(DIMS, snapshot.clone(), 0);
        let cached_reader = CachedCentroidReader::new(
            &(query_cache.clone() as Arc<dyn CentroidCache>),
            stored_reader,
        );
        let centroid_index = LeveledCentroidIndex::new(
            TreeDepth::of(depth),
            DistanceMetric::L2,
            Arc::new(cached_reader),
        );
        LastAppliedSnapshot {
            snapshot,
            centroid_cache: query_cache,
            centroid_index: Arc::new(centroid_index),
            centroid_count: 0,
        }
    }

    #[allow(clippy::type_complexity)]
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
                PostingListKey::new(ROOT_VECTOR_ID).encode(),
                posting_list_value(TreeDepth::of(depth).max_inner_level(), root).encode_to_bytes(),
            )
            .into(),
        );
        for (centroid_num, value) in centroid_info {
            ops.push(
                Record::new(
                    CentroidInfoKey::new(centroid_id(value.level, centroid_num)).encode(),
                    value.encode_to_bytes(),
                )
                .into(),
            );
        }
        for ((level, centroid_num), count) in centroid_stats {
            ops.push(
                Record::new(
                    CentroidStatsKey::new(centroid_id(level, centroid_num)).encode(),
                    CentroidStatsValue::new(count).encode_to_bytes(),
                )
                .into(),
            );
        }
        for (centroid_id, postings) in centroid_postings {
            ops.push(
                Record::new(
                    PostingListKey::new(root_posting_id(depth, centroid_id)).encode(),
                    posting_list_value(1, postings).encode_to_bytes(),
                )
                .into(),
            );
        }
        storage.put(ops).await.unwrap();
    }

    fn posting_list_value(level: u8, postings: Vec<(u64, Vec<f32>)>) -> PostingListValue {
        PostingListValue::from_posting_updates(
            postings
                .into_iter()
                .map(|(id, vector)| PostingUpdate::append(centroid_id(level, id), vector))
                .collect(),
        )
        .unwrap()
    }
}
