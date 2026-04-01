use crate::Result;
use crate::math::distance::{VectorDistance, compute_distance};
use crate::serde::collection_meta::DistanceMetric;
use crate::serde::posting_list::{PostingList, PostingListValue, PostingUpdate};
use crate::storage::VectorDbStorageReadExt;
use crate::write::indexer::drivers::AsyncBatchDriver;
use common::StorageRead;
use futures::future::BoxFuture;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

pub(crate) enum MaybeCached<T> {
    Value(T),
    Future(BoxFuture<'static, Result<T>>),
}

impl<T> MaybeCached<T> {
    pub(crate) async fn get(self) -> Result<T> {
        match self {
            MaybeCached::Value(value) => Ok(value),
            MaybeCached::Future(future) => future.await,
        }
    }
}

impl<T: 'static> MaybeCached<T> {
    pub(crate) fn map<O>(
        self,
        mapper: impl FnOnce(T) -> O + Send + Sync + 'static,
    ) -> MaybeCached<O> {
        match self {
            MaybeCached::Value(v) => MaybeCached::Value(mapper(v)),
            MaybeCached::Future(fut) => {
                MaybeCached::Future(Box::pin(async move { fut.await.map(|v| mapper(v)) }))
            }
        }
    }
}

impl<T> Into<MaybeCached<T>> for BoxFuture<'static, Result<T>> {
    fn into(self) -> MaybeCached<T> {
        MaybeCached::Future(self)
    }
}

pub(crate) struct BlockedCentroidSearch {
    /// The level the reads are for
    level: u16,
    /// Centroid postings that were already cached at this level
    found: Vec<(u64, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    reads: Vec<(u64, BoxFuture<'static, Result<(u64, Arc<PostingList>)>>)>,
}

impl BlockedCentroidSearch {
    pub(crate) fn new(
        level: u16,
        found: Vec<(u64, Arc<PostingList>)>,
        reads: Vec<(u64, BoxFuture<'static, Result<(u64, Arc<PostingList>)>>)>,
    ) -> Self {
        Self {
            level,
            found,
            reads,
        }
    }

    pub(crate) fn start(
        self,
    ) -> (
        Vec<(u64, BoxFuture<'static, Result<(u64, Arc<PostingList>)>>)>,
        InFlightBlockedCentroidSearch,
    ) {
        let reads = self.reads;
        let ids = reads.iter().map(|(id, _)| *id).collect();
        let in_flight = InFlightBlockedCentroidSearch {
            level: self.level,
            found: self.found,
            reads: ids,
        };
        (reads, in_flight)
    }
}

pub(crate) struct InFlightBlockedCentroidSearch {
    /// The level the reads are for
    level: u16,
    /// Centroid postings that were already cached at this level
    found: Vec<(u64, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    reads: Vec<u64>,
}

impl InFlightBlockedCentroidSearch {
    pub(crate) fn finish(self, read: &HashMap<u64, Arc<PostingList>>) -> ResumableCentroidSearch {
        let read = self
            .reads
            .into_iter()
            .map(|c| (c, read[&c].clone()))
            .collect::<Vec<_>>();
        ResumableCentroidSearch {
            level: self.level,
            found: self.found,
            read,
        }
    }
}

pub(crate) struct ResumableCentroidSearch {
    /// The level the reads are for
    level: u16,
    /// Centroid postings that were already cached at this level
    found: Vec<(u64, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    read: Vec<(u64, Arc<PostingList>)>,
}

impl ResumableCentroidSearch {
    fn all_found(level: u16, found: Vec<(u64, Arc<PostingList>)>) -> Self {
        Self {
            level,
            found,
            read: vec![],
        }
    }
}

pub(crate) enum SearchResult {
    ReadsRequired(BlockedCentroidSearch),
    Ann(Vec<(u64, Vec<f32>)>),
}

pub(crate) trait CentroidIndex {
    fn search(&self, query: &[f32], k: usize) -> SearchResult;

    fn resume_search(
        &self,
        query: &[f32],
        k: usize,
        postings: ResumableCentroidSearch,
    ) -> SearchResult;
}

pub(crate) trait CentroidReader: Send + Sync {
    fn read_root(&self) -> MaybeCached<Arc<PostingList>>;

    fn read_postings(&self, centroid_id: u64) -> MaybeCached<Arc<PostingList>>;
}

pub(crate) trait CentroidCache: Send + Sync {
    fn root(&self, _epoch: u64) -> Option<Arc<PostingList>> {
        None
    }

    fn posting(&self, centroid_id: u64, epoch: u64) -> Option<Arc<PostingList>> {
        self.postings(&[centroid_id], epoch).first().cloned()
    }

    fn postings(&self, _centroid_ids: &[u64], _epoch: u64) -> Vec<Arc<PostingList>> {
        Vec::new()
    }
}

pub(crate) struct LeveledCentroidIndex<'a> {
    depth: u16,
    beam: usize,
    distance_metric: DistanceMetric,
    reader: Arc<dyn CentroidReader + 'a>,
}

impl<'a> LeveledCentroidIndex<'a> {
    pub(crate) fn new(
        depth: u16,
        distance_metric: DistanceMetric,
        reader: Arc<dyn CentroidReader + 'a>,
    ) -> Self {
        Self {
            depth,
            beam: 100,
            distance_metric,
            reader,
        }
    }
}

impl<'a> CentroidIndex for LeveledCentroidIndex<'a> {
    fn search(&self, query: &[f32], k: usize) -> SearchResult {
        self.search_up_to_level(query, k, 0)
    }

    fn resume_search(
        &self,
        query: &[f32],
        k: usize,
        postings: ResumableCentroidSearch,
    ) -> SearchResult {
        self.resume_search_up_to_level(query, k, 0, postings)
    }
}

impl<'a> LeveledCentroidIndex<'a> {
    fn score_and_rank(
        query: &[f32],
        postings: &[Arc<PostingList>],
        k: usize,
        distance_metric: DistanceMetric,
    ) -> Vec<(u64, Vec<f32>)> {
        if k == 0 || postings.is_empty() {
            return Vec::new();
        }

        if k == usize::MAX {
            let mut seen = HashSet::new();
            let mut ranked = Vec::new();
            for posting_list in postings {
                for posting in posting_list.iter() {
                    if !seen.insert(posting.id()) {
                        continue;
                    }
                    ranked.push(RankedPosting {
                        id: posting.id(),
                        vector: posting.vector().to_vec(),
                        distance: compute_distance(query, posting.vector(), distance_metric),
                    });
                }
            }
            ranked.sort_unstable();
            return ranked
                .into_iter()
                .map(|posting| (posting.id, posting.vector))
                .collect();
        }

        let mut ranked = BinaryHeap::with_capacity(k);
        let mut heap_ids = HashSet::with_capacity(k);

        for posting_list in postings {
            for posting in posting_list.iter() {
                let distance = compute_distance(query, posting.vector(), distance_metric);
                let candidate = RankedPosting {
                    id: posting.id(),
                    vector: posting.vector().to_vec(),
                    distance,
                };

                if heap_ids.contains(&candidate.id) {
                    continue;
                }

                if ranked.len() < k {
                    heap_ids.insert(candidate.id);
                    ranked.push(candidate);
                    continue;
                }

                let Some(worst) = ranked.peek() else {
                    continue;
                };
                if candidate < *worst {
                    let removed = ranked.pop().expect("heap should be non-empty");
                    heap_ids.remove(&removed.id);
                    heap_ids.insert(candidate.id);
                    ranked.push(candidate);
                }
            }
        }

        let mut ranked = ranked.into_vec();
        ranked.sort_unstable();
        ranked
            .into_iter()
            .map(|posting| (posting.id, posting.vector))
            .collect()
    }

    pub(crate) fn search_root(&self, query: &[f32], k: usize) -> Vec<(u64, Vec<f32>)> {
        let MaybeCached::Value(root) = self.reader.read_root() else {
            panic!("LeveledCentroidIndex::search_root() couldn't find root");
        };
        Self::score_and_rank(query, &[root], k, self.distance_metric)
    }

    fn search_up_to_level(&self, query: &[f32], k: usize, level: u16) -> SearchResult {
        let this_level = self.depth - 1;
        assert!(level <= this_level);
        if level == this_level {
            return SearchResult::Ann(self.search_root(query, k));
        }
        let beam = self.beam.max(k);
        let next_centroids = self
            .search_root(query, beam)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        self.search_up_to_level_with_centroids_at_inner_level(
            query,
            k,
            level,
            this_level - 1,
            next_centroids,
        )
    }

    fn search_up_to_level_with_centroids_at_inner_level(
        &self,
        query: &[f32],
        k: usize,
        level: u16,
        inner_level: u16,
        centroids: Vec<u64>,
    ) -> SearchResult {
        let posting_futs: Vec<_> = centroids
            .iter()
            .map(|&c| (c, self.reader.read_postings(c)))
            .collect();
        let mut found = Vec::with_capacity(centroids.len());
        let mut reads = Vec::with_capacity(centroids.len());
        for (c, fut) in posting_futs {
            match fut {
                MaybeCached::Value(p) => {
                    found.push((c, p));
                }
                MaybeCached::Future(fut) => {
                    reads.push((
                        c,
                        Box::pin(async move { fut.await.map(|p| (c, p)) }) as BoxFuture<'static, _>,
                    ));
                }
            }
        }
        if reads.is_empty() {
            self.resume_search_up_to_level(
                query,
                k,
                level,
                ResumableCentroidSearch::all_found(inner_level, found),
            )
        } else {
            SearchResult::ReadsRequired(BlockedCentroidSearch::new(inner_level, found, reads))
        }
    }

    fn resume_search_up_to_level(
        &self,
        query: &[f32],
        k: usize,
        level: u16,
        postings: ResumableCentroidSearch,
    ) -> SearchResult {
        let all_postings = postings
            .found
            .iter()
            .map(|(_, p)| p.clone())
            .chain(postings.read.iter().map(|(_, p)| p.clone()))
            .collect::<Vec<_>>();
        if postings.level == level {
            SearchResult::Ann(Self::score_and_rank(
                query,
                &all_postings,
                k,
                self.distance_metric,
            ))
        } else {
            // find the top beam postings at this level and search at the next level
            // then call search_up_to_level_with_centroids_at_inner_level
            let beam = self.beam.max(k);
            let next_centroids =
                Self::score_and_rank(query, &all_postings, beam, self.distance_metric)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect();
            self.search_up_to_level_with_centroids_at_inner_level(
                query,
                k,
                level,
                postings.level - 1,
                next_centroids,
            )
        }
    }
}

#[derive(Clone, Debug)]
struct RankedPosting {
    id: u64,
    vector: Vec<f32>,
    distance: VectorDistance,
}

impl PartialEq for RankedPosting {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.distance == other.distance
    }
}

impl Eq for RankedPosting {}

impl PartialOrd for RankedPosting {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedPosting {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .cmp(&other.distance)
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[derive(Clone)]
pub(crate) struct StoredCentroidReader {
    dimensions: usize,
    epoch: u64,
    snapshot: Arc<dyn StorageRead>,
}

impl StoredCentroidReader {
    pub(crate) fn new(dimensions: usize, snapshot: Arc<dyn StorageRead>, epoch: u64) -> Self {
        Self {
            dimensions,
            epoch,
            snapshot,
        }
    }
}

impl CentroidReader for StoredCentroidReader {
    fn read_root(&self) -> MaybeCached<Arc<PostingList>> {
        let snapshot = self.snapshot.clone();
        let dimensions = self.dimensions;
        MaybeCached::Future(Box::pin(async move {
            Ok(Arc::new(
                snapshot.get_root_posting_list(dimensions).await?.into(),
            ))
        }))
    }

    fn read_postings(&self, centroid_id: u64) -> MaybeCached<Arc<PostingList>> {
        let snapshot = self.snapshot.clone();
        let dimensions = self.dimensions;
        MaybeCached::Future(Box::pin(async move {
            Ok(Arc::new(
                snapshot
                    .get_posting_list(centroid_id, dimensions)
                    .await?
                    .into(),
            ))
        }))
    }
}

#[derive(Clone)]
pub(crate) struct CachedCentroidReader {
    cache: Arc<dyn CentroidCache>,
    inner: StoredCentroidReader,
}

impl<'a> CachedCentroidReader {
    pub(crate) fn new(cache: &Arc<dyn CentroidCache>, inner: StoredCentroidReader) -> Self {
        Self {
            cache: cache.clone(),
            inner,
        }
    }
}

impl CentroidReader for CachedCentroidReader {
    fn read_root(&self) -> MaybeCached<Arc<PostingList>> {
        if let Some(root) = self.cache.root(self.inner.epoch) {
            MaybeCached::Value(root)
        } else {
            self.inner.read_root()
        }
    }

    fn read_postings(&self, centroid_id: u64) -> MaybeCached<Arc<PostingList>> {
        if let Some(postings) = self.cache.posting(centroid_id, self.inner.epoch) {
            MaybeCached::Value(postings)
        } else {
            self.inner.read_postings(centroid_id)
        }
    }
}

#[derive(Clone)]
struct WrittenPostingList {
    posting_list: Arc<PostingList>,
    written_epoch: u64,
}

pub(crate) struct AllCentroidsCache {
    inner: Arc<Mutex<AllCentroidsCacheInner>>,
}

impl CentroidCache for AllCentroidsCache {
    fn root(&self, epoch: u64) -> Option<Arc<PostingList>> {
        let root = self.inner.lock().expect("lock poisoned").root();
        if epoch < root.written_epoch {
            None
        } else {
            Some(root.posting_list.clone())
        }
    }

    fn posting(&self, centroid_id: u64, epoch: u64) -> Option<Arc<PostingList>> {
        self.postings(&[centroid_id], epoch).into_iter().next()
    }

    fn postings(&self, centroid_ids: &[u64], epoch: u64) -> Vec<Arc<PostingList>> {
        self.inner
            .lock()
            .expect("lock poisoned")
            .postings(centroid_ids)
            .into_iter()
            .filter_map(|p| {
                if epoch < p.written_epoch {
                    None
                } else {
                    Some(p.posting_list.clone())
                }
            })
            .collect()
    }
}

pub(crate) struct AllCentroidsCacheWriter {
    inner: Arc<Mutex<AllCentroidsCacheInner>>,
}

impl AllCentroidsCacheWriter {
    pub(crate) fn new(
        root_posting_list: Arc<PostingList>,
        centroid_postings: Vec<(u64, Arc<PostingList>)>,
    ) -> Self {
        let root = WrittenPostingList {
            written_epoch: 0,
            posting_list: root_posting_list,
        };
        let postings = centroid_postings
            .into_iter()
            .map(|(centroid_id, posting_list)| {
                (
                    centroid_id,
                    WrittenPostingList {
                        written_epoch: 0,
                        posting_list,
                    },
                )
            })
            .collect();
        let inner = Arc::new(Mutex::new(AllCentroidsCacheInner { root, postings }));
        Self { inner }
    }

    pub(crate) fn update_postings(
        &self,
        epoch: u64,
        root: Option<Vec<PostingUpdate>>,
        root_updates: Vec<PostingUpdate>,
        centroid_postings: Vec<(u64, Vec<PostingUpdate>)>,
        deleted_centroids: &HashSet<u64>,
    ) {
        fn apply_updates(
            posting_list: Option<&Arc<PostingList>>,
            updates: Vec<PostingUpdate>,
        ) -> Arc<PostingList> {
            let Some(posting_list) = posting_list else {
                return Arc::new(
                    PostingListValue::from_posting_updates(updates)
                        .expect("posting updates should always encode")
                        .into(),
                );
            };

            let mut all_updates = Vec::with_capacity(posting_list.len() + updates.len());
            all_updates.extend(
                posting_list
                    .iter()
                    .map(|p| PostingUpdate::append(p.id(), p.vector().to_vec())),
            );
            all_updates.extend(updates);
            Arc::new(
                PostingListValue::from_posting_updates(all_updates)
                    .expect("posting updates should always encode")
                    .into(),
            )
        }

        let mut inner = self.inner.lock().expect("lock poisoned");

        if root.is_some() || !root_updates.is_empty() {
            let root = if let Some(root) = root {
                apply_updates(None, root)
            } else {
                inner.root.posting_list.clone()
            };
            let root = if root_updates.is_empty() {
                root
            } else {
                apply_updates(Some(&root), root_updates)
            };
            inner.root = WrittenPostingList {
                posting_list: root,
                written_epoch: epoch,
            };
        }

        for (centroid_id, updates) in centroid_postings {
            let posting_list = inner
                .postings
                .get(&centroid_id)
                .map(|p| p.posting_list.clone());
            let posting_list = apply_updates(posting_list.as_ref(), updates);
            inner.postings.insert(
                centroid_id,
                WrittenPostingList {
                    posting_list,
                    written_epoch: epoch,
                },
            );
        }

        for centroid_id in deleted_centroids {
            inner.postings.remove(&centroid_id);
        }
    }

    pub(crate) fn cache(&self) -> AllCentroidsCache {
        AllCentroidsCache {
            inner: self.inner.clone(),
        }
    }
}

struct AllCentroidsCacheInner {
    root: WrittenPostingList,
    postings: HashMap<u64, WrittenPostingList>,
}

impl AllCentroidsCacheInner {
    pub(crate) fn new(
        root: WrittenPostingList,
        postings: HashMap<u64, WrittenPostingList>,
    ) -> Self {
        Self { root, postings }
    }

    pub(crate) fn root(&self) -> WrittenPostingList {
        self.root.clone()
    }

    pub(crate) fn postings(&self, centroids: &[u64]) -> Vec<WrittenPostingList> {
        centroids
            .iter()
            .filter_map(|c| self.postings.get(c))
            .cloned()
            .collect()
    }
}

pub(crate) async fn search_centroids(
    index: &LeveledCentroidIndex<'_>,
    query: &[f32],
    k: usize,
) -> Result<Vec<(u64, Vec<f32>)>> {
    Ok(batch_search_centroids(index, k, vec![(0, query)])
        .await?
        .remove(&0)
        .expect("unreachable"))
}

pub(crate) async fn batch_search_centroids<K: Hash + Eq + Sized + Send + Sync>(
    index: &LeveledCentroidIndex<'_>,
    k: usize,
    queries: Vec<(K, &[f32])>,
) -> Result<HashMap<K, Vec<(u64, Vec<f32>)>>> {
    batch_search_centroids_up_to_level(index, k, queries, 0).await
}

pub(crate) async fn batch_search_centroids_up_to_level<K: Hash + Eq + Sized + Send + Sync>(
    index: &LeveledCentroidIndex<'_>,
    k: usize,
    queries: Vec<(K, &[f32])>,
    level: u16,
) -> Result<HashMap<K, Vec<(u64, Vec<f32>)>>> {
    let mut results = HashMap::with_capacity(queries.len());
    let queries: Vec<(K, &[f32], Option<ResumableCentroidSearch>)> = queries
        .into_iter()
        .map(|(k, queries)| (k, queries, None))
        .collect();
    let mut queries = Some(queries);
    loop {
        let Some(remaining) = queries.take() else {
            break;
        };
        // find results / intermediate results
        let intermediate = remaining
            .into_par_iter()
            .map(|(key, q, ip)| {
                let result = if let Some(ip) = ip {
                    index.resume_search_up_to_level(&q, k, level, ip)
                } else {
                    index.search_up_to_level(&q, k, level)
                };
                (key, q, result)
            })
            .collect::<Vec<_>>();
        let mut posting_reads = HashMap::new();
        let mut pending = Vec::with_capacity(intermediate.len());
        // separate finished results from intermediate posting reads
        for (key, q, result) in intermediate {
            match result {
                SearchResult::ReadsRequired(reads) => {
                    let (reads, in_flight) = reads.start();
                    for (centroid, fut) in reads {
                        posting_reads.insert(centroid, fut);
                    }
                    pending.push((key, q, in_flight))
                }
                SearchResult::Ann(ann) => {
                    results.insert(key, ann);
                }
            }
        }
        if pending.is_empty() {
            break;
        }
        let posting_reads = posting_reads.into_values().collect::<Vec<_>>();
        let posting_reads = AsyncBatchDriver::execute(posting_reads).await;
        let mut postings = HashMap::with_capacity(posting_reads.len());
        for pr in posting_reads {
            let (centroid, pl) = pr?;
            postings.insert(centroid, pl);
        }
        // construct next batch of resumed queries
        let _ = queries.insert(
            pending
                .into_iter()
                .map(|(key, q, in_flight)| (key, q, Some(in_flight.finish(&postings))))
                .collect(),
        );
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::key::PostingListKey;
    use crate::serde::posting_list::Posting;
    use crate::serde::posting_list::{PostingListValue, PostingUpdate};
    use common::storage::in_memory::InMemoryStorage;
    use common::{Record, Storage, StorageRead};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn should_score_and_rank_l2_postings() {
        // given
        let postings = Arc::new(vec![
            Posting::new(10, vec![1.0, 0.0]),
            Posting::new(11, vec![0.0, 1.0]),
            Posting::new(12, vec![3.0, 0.0]),
        ]);

        // when
        let ranked =
            LeveledCentroidIndex::score_and_rank(&[0.9, 0.1], &[postings], 2, DistanceMetric::L2);

        // then
        assert_eq!(ranked_ids(&ranked), vec![10, 11]);
    }

    #[test]
    fn should_deduplicate_ids_and_keep_best_score() {
        // given
        let postings_a = Arc::new(vec![
            Posting::new(10, vec![1.0, 0.0]),
            Posting::new(11, vec![0.0, 1.0]),
        ]);
        let postings_b = Arc::new(vec![Posting::new(10, vec![1.0, 0.0])]);

        // when
        let ranked = LeveledCentroidIndex::score_and_rank(
            &[0.9, 0.1],
            &[postings_a, postings_b],
            2,
            DistanceMetric::L2,
        );

        // then
        assert_eq!(ranked_ids(&ranked), vec![10, 11]);
    }

    const ROOT_POSTING_ID: u64 = 0;
    const DIMS: usize = 2;

    fn posting_list(postings: Vec<(u64, Vec<f32>)>) -> Arc<PostingList> {
        Arc::new(
            postings
                .into_iter()
                .map(|(id, vector)| Posting::new(id, vector))
                .collect(),
        )
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

    fn assert_posting_list_eq(actual: Arc<PostingList>, expected: Vec<(u64, Vec<f32>)>) {
        let expected = posting_list(expected);
        assert_eq!(actual.as_ref(), expected.as_ref());
    }

    fn ranked_ids(ranked: &[(u64, Vec<f32>)]) -> Vec<u64> {
        ranked.iter().map(|(id, _)| *id).collect()
    }

    fn empty_deleted_centroids() -> HashSet<u64> {
        HashSet::new()
    }

    async fn put_posting_list(
        storage: &Arc<dyn Storage>,
        centroid_id: u64,
        postings: Vec<(u64, Vec<f32>)>,
    ) -> Arc<PostingList> {
        let key = PostingListKey::new(centroid_id).encode();
        let value = posting_list_value(postings);
        let posting_list = Arc::new(value.clone().into());
        storage
            .put(vec![Record::new(key, value.encode_to_bytes()).into()])
            .await
            .unwrap();
        posting_list
    }

    fn cached_index(
        depth: u16,
        storage: Arc<dyn Storage>,
        root: Arc<PostingList>,
        postings: HashMap<u64, Arc<PostingList>>,
        distance_metric: DistanceMetric,
    ) -> LeveledCentroidIndex<'static> {
        let cache: Arc<dyn CentroidCache> = Arc::new(AllCentroidsCache {
            inner: Arc::new(Mutex::new(AllCentroidsCacheInner::new(
                WrittenPostingList {
                    posting_list: root,
                    written_epoch: 0,
                },
                postings
                    .into_iter()
                    .map(|(centroid_id, posting_list)| {
                        (
                            centroid_id,
                            WrittenPostingList {
                                posting_list,
                                written_epoch: 0,
                            },
                        )
                    })
                    .collect(),
            ))),
        });
        let stored = StoredCentroidReader::new(DIMS, storage as Arc<dyn StorageRead>, 0);
        let reader = CachedCentroidReader::new(&cache, stored);
        LeveledCentroidIndex::new(depth, distance_metric, Arc::new(reader))
    }

    struct TestTree {
        depth: u16,
        storage: Arc<dyn Storage>,
        root: Arc<PostingList>,
        postings_by_level: HashMap<u16, HashMap<u64, Arc<PostingList>>>,
    }

    impl TestTree {
        fn fully_cached_index(&self) -> LeveledCentroidIndex<'static> {
            self.index_with_cached_postings(self.all_posting_ids())
        }

        fn index_with_cached_postings(
            &self,
            cached_postings: Vec<u64>,
        ) -> LeveledCentroidIndex<'static> {
            let postings = cached_postings
                .into_iter()
                .filter_map(|centroid_id| {
                    self.posting(centroid_id)
                        .map(|posting| (centroid_id, posting))
                })
                .collect();
            cached_index(
                self.depth,
                self.storage.clone(),
                self.root.clone(),
                postings,
                DistanceMetric::L2,
            )
        }

        fn posting(&self, centroid_id: u64) -> Option<Arc<PostingList>> {
            self.postings_by_level
                .values()
                .find_map(|postings| postings.get(&centroid_id).cloned())
        }

        fn all_posting_ids(&self) -> Vec<u64> {
            self.postings_by_level
                .values()
                .flat_map(|postings| postings.keys().copied())
                .collect()
        }

        fn exhaustive_search(&self, query: &[f32], k: usize) -> Vec<(u64, Vec<f32>)> {
            self.exhaustive_search_up_to_level(query, k, 0)
        }

        fn exhaustive_search_up_to_level(
            &self,
            query: &[f32],
            k: usize,
            target_level: u16,
        ) -> Vec<(u64, Vec<f32>)> {
            assert!(target_level < self.depth);

            let mut current_level = self.depth - 1;
            let mut current_postings = vec![self.root.clone()];

            while current_level > target_level {
                let next_level = current_level - 1;
                let level_postings = self
                    .postings_by_level
                    .get(&next_level)
                    .expect("missing posting lists for level");
                let mut next_postings = Vec::new();
                for posting_list in &current_postings {
                    for posting in posting_list.iter() {
                        let posting_list = level_postings
                            .get(&posting.id())
                            .unwrap_or_else(|| {
                                panic!("missing posting list for centroid {}", posting.id())
                            })
                            .clone();
                        next_postings.push(posting_list);
                    }
                }
                current_postings = next_postings;
                current_level = next_level;
            }

            LeveledCentroidIndex::score_and_rank(query, &current_postings, k, DistanceMetric::L2)
        }
    }

    async fn build_tree(
        depth: u16,
        root: Vec<(u64, Vec<f32>)>,
        postings_by_level: Vec<(u16, Vec<(u64, Vec<(u64, Vec<f32>)>)>)>,
    ) -> TestTree {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let root = put_posting_list(&storage, ROOT_POSTING_ID, root).await;
        let mut tree_postings = HashMap::new();
        for (level, postings) in postings_by_level {
            let mut level_postings = HashMap::with_capacity(postings.len());
            for (centroid_id, posting) in postings {
                let posting = put_posting_list(&storage, centroid_id, posting).await;
                level_postings.insert(centroid_id, posting);
            }
            tree_postings.insert(level, level_postings);
        }
        TestTree {
            depth,
            storage,
            root,
            postings_by_level: tree_postings,
        }
    }

    async fn root_only_tree() -> TestTree {
        build_tree(
            1,
            vec![
                (10, vec![1.0, 0.0]),
                (11, vec![0.0, 1.0]),
                (12, vec![3.0, 0.0]),
            ],
            vec![],
        )
        .await
    }

    async fn two_level_tree() -> TestTree {
        build_tree(
            2,
            vec![(100, vec![10.0, 0.0]), (200, vec![0.0, 10.0])],
            vec![(
                0,
                vec![
                    (100, vec![(10, vec![1.0, 0.0]), (11, vec![2.0, 0.0])]),
                    (200, vec![(20, vec![0.0, 1.0]), (21, vec![0.0, 2.0])]),
                ],
            )],
        )
        .await
    }

    async fn three_level_tree() -> TestTree {
        build_tree(
            3,
            vec![(1000, vec![10.0, 0.0]), (2000, vec![0.0, 10.0])],
            vec![
                (
                    1,
                    vec![
                        (1000, vec![(100, vec![5.0, 0.0]), (101, vec![6.0, 0.0])]),
                        (2000, vec![(200, vec![0.0, 5.0]), (201, vec![0.0, 6.0])]),
                    ],
                ),
                (
                    0,
                    vec![
                        (100, vec![(10, vec![1.0, 0.0]), (11, vec![2.0, 0.0])]),
                        (101, vec![(12, vec![3.0, 0.0]), (13, vec![4.0, 0.0])]),
                        (200, vec![(20, vec![0.0, 1.0]), (21, vec![0.0, 2.0])]),
                        (201, vec![(22, vec![0.0, 3.0]), (23, vec![0.0, 4.0])]),
                    ],
                ),
            ],
        )
        .await
    }

    async fn wide_two_level_tree(width: u64) -> TestTree {
        let root = (0..width)
            .map(|offset| (1000 + offset, vec![offset as f32, 0.0]))
            .collect();
        let postings = vec![(
            0,
            (0..width)
                .map(|offset| {
                    let centroid_id = 1000 + offset;
                    let leaf_id = 10_000 + offset;
                    (centroid_id, vec![(leaf_id, vec![offset as f32, 0.0])])
                })
                .collect(),
        )];
        build_tree(2, root, postings).await
    }

    async fn finish_reads(reads: BlockedCentroidSearch) -> ResumableCentroidSearch {
        let (reads, in_flight) = reads.start();
        let mut resolved = HashMap::with_capacity(reads.len());
        for (_, read) in reads {
            let (centroid_id, posting_list) = read.await.unwrap();
            resolved.insert(centroid_id, posting_list);
        }
        in_flight.finish(&resolved)
    }

    #[tokio::test]
    async fn should_search_root_only_tree() {
        // given
        let tree = root_only_tree().await;
        let index = tree.fully_cached_index();
        let expected = tree.exhaustive_search(&[0.9, 0.1], 2);

        // when
        let ranked = search_centroids(&index, &[0.9, 0.1], 2).await.unwrap();

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_search_two_level_tree() {
        // given
        let tree = two_level_tree().await;
        let index = tree.fully_cached_index();
        let expected = tree.exhaustive_search(&[0.9, 0.1], 2);

        // when
        let ranked = search_centroids(&index, &[0.9, 0.1], 2).await.unwrap();

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_search_three_level_tree() {
        // given
        let tree = three_level_tree().await;
        let index = tree.fully_cached_index();
        let expected = tree.exhaustive_search(&[0.9, 0.1], 2);

        // when
        let ranked = search_centroids(&index, &[0.9, 0.1], 2).await.unwrap();

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_search_up_to_root() {
        // given
        let tree = two_level_tree().await;
        let index = tree.fully_cached_index();
        let expected = tree.exhaustive_search_up_to_level(&[0.9, 0.1], 1, 1);

        // when
        let SearchResult::Ann(ranked) = index.search_up_to_level(&[0.9, 0.1], 1, 1) else {
            panic!("search should complete with all centroids cached");
        };

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_search_up_to_level() {
        // given
        let tree = three_level_tree().await;
        let index = tree.fully_cached_index();
        let expected = tree.exhaustive_search_up_to_level(&[5.1, 0.0], 2, 1);

        // when
        let SearchResult::Ann(ranked) = index.search_up_to_level(&[5.1, 0.0], 2, 1) else {
            panic!("search should complete with all centroids cached");
        };

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_resume_search_from_level_when_no_centroids_cached() {
        // given
        let tree = two_level_tree().await;
        let index = tree.index_with_cached_postings(vec![]);
        let expected = tree.exhaustive_search(&[0.9, 0.1], 2);

        // when
        let SearchResult::ReadsRequired(reads) = index.search(&[0.9, 0.1], 2) else {
            panic!("search should require posting reads");
        };
        assert!(reads.found.is_empty());
        assert_eq!(reads.reads.len(), 2);
        let postings = finish_reads(reads).await;
        let SearchResult::Ann(ranked) = index.resume_search(&[0.9, 0.1], 2, postings) else {
            panic!("resume should finish the search");
        };

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_resume_search_from_level_when_some_centroids_cached() {
        // given
        let tree = two_level_tree().await;
        let index = tree.index_with_cached_postings(vec![100]);
        let expected = tree.exhaustive_search(&[0.9, 0.1], 2);

        // when
        let SearchResult::ReadsRequired(reads) = index.search(&[0.9, 0.1], 2) else {
            panic!("search should require posting reads");
        };
        assert_eq!(reads.found.len(), 1);
        assert_eq!(reads.reads.len(), 1);
        assert_eq!(reads.reads[0].0, 200);
        let postings = finish_reads(reads).await;
        let SearchResult::Ann(ranked) = index.resume_search(&[0.9, 0.1], 2, postings) else {
            panic!("resume should finish the search");
        };

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_use_beam_width_when_searching() {
        // given
        let tree = wide_two_level_tree(101).await;
        let index = tree.index_with_cached_postings(vec![1000]);
        let expected = tree.exhaustive_search_up_to_level(&[0.0, 0.0], 100, 1);
        let mut expected = ranked_ids(&expected);
        expected.sort_unstable();

        // when
        let SearchResult::ReadsRequired(reads) = index.search(&[0.0, 0.0], 1) else {
            panic!("search should require posting reads");
        };

        // then
        assert_eq!(reads.found.len(), 1);
        assert_eq!(reads.reads.len(), 99);
        let mut seen = reads
            .found
            .iter()
            .map(|(centroid_id, _)| *centroid_id)
            .collect::<Vec<_>>();
        seen.extend(reads.reads.iter().map(|(centroid_id, _)| *centroid_id));
        seen.sort_unstable();
        assert_eq!(seen, expected);
    }

    #[tokio::test]
    async fn should_batch_search_centroids() {
        // given
        let tree = three_level_tree().await;
        let index = tree.index_with_cached_postings(vec![1000, 100]);
        let q0 = [0.9, 0.1];
        let q1 = [0.1, 0.9];
        let expected_x = tree.exhaustive_search(&q0, 2);
        let expected_y = tree.exhaustive_search(&q1, 2);

        // when
        let results = batch_search_centroids(&index, 2, vec![("x", &q0), ("y", &q1)])
            .await
            .unwrap();

        // then
        assert_eq!(results["x"], expected_x);
        assert_eq!(results["y"], expected_y);
    }

    #[tokio::test]
    async fn should_batch_search_centroids_up_to_level() {
        // given
        let tree = three_level_tree().await;
        let index = tree.fully_cached_index();
        let q0 = [5.1, 0.0];
        let q1 = [0.0, 5.1];
        let expected_x = tree.exhaustive_search_up_to_level(&q0, 2, 1);
        let expected_y = tree.exhaustive_search_up_to_level(&q1, 2, 1);

        // when
        let results =
            batch_search_centroids_up_to_level(&index, 2, vec![("x", &q0), ("y", &q1)], 1)
                .await
                .unwrap();

        // then
        assert_eq!(results["x"], expected_x);
        assert_eq!(results["y"], expected_y);
    }

    #[test]
    fn should_update_cached_root_posting() {
        // given
        let writer = AllCentroidsCacheWriter::new(
            posting_list(vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])]),
            vec![],
        );

        // when
        writer.update_postings(
            5,
            None,
            vec![
                PostingUpdate::delete(1),
                PostingUpdate::append(3, vec![3.0, 0.0]),
            ],
            vec![],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert!(cache.root(4).is_none());
        assert_posting_list_eq(
            cache.root(5).expect("root should be cached"),
            vec![(2, vec![2.0, 0.0]), (3, vec![3.0, 0.0])],
        );
    }

    #[test]
    fn should_overwrite_cached_root_with_new_root() {
        // given
        let writer = AllCentroidsCacheWriter::new(
            posting_list(vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])]),
            vec![],
        );

        // when
        writer.update_postings(
            5,
            Some(vec![
                PostingUpdate::append(10, vec![10.0, 0.0]),
                PostingUpdate::append(11, vec![11.0, 0.0]),
            ]),
            vec![],
            vec![],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache.root(5).expect("root should be cached"),
            vec![(10, vec![10.0, 0.0]), (11, vec![11.0, 0.0])],
        );
    }

    #[test]
    fn should_apply_updates_to_new_root() {
        // given
        let writer = AllCentroidsCacheWriter::new(posting_list(vec![(1, vec![1.0, 0.0])]), vec![]);

        // when
        writer.update_postings(
            5,
            Some(vec![
                PostingUpdate::append(10, vec![10.0, 0.0]),
                PostingUpdate::append(11, vec![11.0, 0.0]),
            ]),
            vec![
                PostingUpdate::delete(10),
                PostingUpdate::append(12, vec![12.0, 0.0]),
            ],
            vec![],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache.root(5).expect("root should be cached"),
            vec![(11, vec![11.0, 0.0]), (12, vec![12.0, 0.0])],
        );
    }

    #[test]
    fn should_add_new_cached_centroid_posting() {
        // given
        let writer = AllCentroidsCacheWriter::new(posting_list(vec![]), vec![]);

        // when
        writer.update_postings(
            5,
            None,
            vec![],
            vec![(
                100,
                vec![
                    PostingUpdate::append(1, vec![1.0, 0.0]),
                    PostingUpdate::append(2, vec![2.0, 0.0]),
                ],
            )],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache.posting(100, 5).expect("centroid should be cached"),
            vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])],
        );
    }

    #[test]
    fn should_apply_updates_to_cached_centroid_posting() {
        // given
        let writer = AllCentroidsCacheWriter::new(
            posting_list(vec![]),
            vec![(
                100,
                posting_list(vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])]),
            )],
        );

        // when
        writer.update_postings(
            5,
            None,
            vec![],
            vec![(
                100,
                vec![
                    PostingUpdate::delete(1),
                    PostingUpdate::append(3, vec![3.0, 0.0]),
                ],
            )],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache.posting(100, 5).expect("centroid should be cached"),
            vec![(2, vec![2.0, 0.0]), (3, vec![3.0, 0.0])],
        );
    }

    #[test]
    fn should_apply_updates_when_adding_new_cached_centroid_posting() {
        // given
        let writer = AllCentroidsCacheWriter::new(posting_list(vec![]), vec![]);

        // when
        writer.update_postings(
            5,
            None,
            vec![],
            vec![(
                100,
                vec![
                    PostingUpdate::append(1, vec![1.0, 0.0]),
                    PostingUpdate::append(2, vec![2.0, 0.0]),
                    PostingUpdate::delete(1),
                    PostingUpdate::append(3, vec![3.0, 0.0]),
                ],
            )],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache.posting(100, 5).expect("centroid should be cached"),
            vec![(2, vec![2.0, 0.0]), (3, vec![3.0, 0.0])],
        );
    }
}
