use crate::Result;
use crate::math::distance::{VectorDistance, compute_distance};
use crate::serde::collection_meta::DistanceMetric;
use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
use crate::storage::VectorDbStorageReadExt;
use crate::write::indexer::drivers::AsyncBatchDriver;
use crate::write::indexer::tree::posting_list::{IntoTreePostingList, Posting, PostingList};
use common::StorageRead;
use futures::future::BoxFuture;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

pub(crate) const ROOT_LEVEL: u8 = 0xFF;
pub(crate) const LEAF_LEVEL: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TreeDepth {
    val: u8,
}

impl Display for TreeDepth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.val)
    }
}

impl TreeDepth {
    pub(crate) fn of(val: u8) -> Self {
        assert!(
            val >= 3,
            "trees always have at least 3 levels (root, leaf, data vectors)"
        );
        Self { val }
    }

    pub(crate) fn val(&self) -> u8 {
        self.val
    }

    pub(crate) fn max_inner_level(&self) -> u8 {
        self.val - 2
    }

    pub(crate) fn promote(&self) -> TreeDepth {
        TreeDepth::of(self.val + 1)
    }
}

/// wrapper type for inner level to prevent direct enum instantiation
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct InnerLevel {
    level: u8,
    depth: TreeDepth,
}

/// wrapper type for root level to prevent direct enum instantiation
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RootLevel {
    depth: TreeDepth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum TreeLevel {
    Root(RootLevel),
    Inner(InnerLevel),
}

impl Display for TreeLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TreeLevel::Root(rl) => write!(f, "{}.root", rl.depth),
            TreeLevel::Inner(l) => write!(f, "{}.{}", l.depth, l.level),
        }
    }
}

impl TreeLevel {
    pub(crate) fn of(level: u8, depth: TreeDepth) -> Self {
        assert!(level >= LEAF_LEVEL);
        if level == ROOT_LEVEL {
            TreeLevel::root(depth)
        } else {
            TreeLevel::inner(level, depth)
        }
    }

    pub(crate) fn root(depth: TreeDepth) -> Self {
        Self::Root(RootLevel { depth })
    }

    pub(crate) fn leaf(depth: TreeDepth) -> Self {
        Self::Inner(InnerLevel {
            level: LEAF_LEVEL,
            depth,
        })
    }

    pub(crate) fn inner(level: u8, depth: TreeDepth) -> Self {
        assert!(level > 0);
        assert!(level <= depth.max_inner_level());
        Self::Inner(InnerLevel { level, depth })
    }

    pub(crate) fn is_inner(&self) -> bool {
        matches!(*self, TreeLevel::Inner(_))
    }

    pub(crate) fn is_leaf(&self) -> bool {
        match self {
            TreeLevel::Root(_) => false,
            TreeLevel::Inner(l) => l.level == LEAF_LEVEL,
        }
    }

    pub(crate) fn is_root(&self) -> bool {
        matches!(*self, TreeLevel::Root(_))
    }

    fn depth(&self) -> TreeDepth {
        match self {
            TreeLevel::Root(rl) => rl.depth,
            TreeLevel::Inner(l) => l.depth,
        }
    }

    pub(crate) fn next_level_up(&self) -> Self {
        match self {
            TreeLevel::Root(_) => {
                panic!("no level up from root")
            }
            TreeLevel::Inner(l) => {
                let max_inner_level = l.depth.max_inner_level();
                assert!(l.level <= max_inner_level);
                if l.level == max_inner_level {
                    Self::root(l.depth)
                } else {
                    Self::inner(l.level + 1, l.depth)
                }
            }
        }
    }

    pub(crate) fn next_level_down(&self) -> Self {
        match self {
            TreeLevel::Root(rl) => Self::inner(rl.depth.max_inner_level(), rl.depth),
            TreeLevel::Inner(l) => {
                assert!(l.level > LEAF_LEVEL);
                Self::inner(l.level - 1, l.depth)
            }
        }
    }

    pub(crate) fn level(&self) -> u8 {
        match self {
            TreeLevel::Root(_) => ROOT_LEVEL,
            TreeLevel::Inner(l) => l.level,
        }
    }
}

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
    level: TreeLevel,
    /// Centroid postings that were already cached at this level
    found: Vec<(VectorId, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    reads: Vec<(
        VectorId,
        BoxFuture<'static, Result<(VectorId, Arc<PostingList>)>>,
    )>,
}

impl BlockedCentroidSearch {
    pub(crate) fn new(
        level: TreeLevel,
        found: Vec<(VectorId, Arc<PostingList>)>,
        reads: Vec<(
            VectorId,
            BoxFuture<'static, Result<(VectorId, Arc<PostingList>)>>,
        )>,
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
        Vec<(
            VectorId,
            BoxFuture<'static, Result<(VectorId, Arc<PostingList>)>>,
        )>,
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
    level: TreeLevel,
    /// Centroid postings that were already cached at this level
    found: Vec<(VectorId, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    reads: Vec<VectorId>,
}

impl InFlightBlockedCentroidSearch {
    pub(crate) fn finish(
        self,
        read: &HashMap<VectorId, Arc<PostingList>>,
    ) -> ResumableCentroidSearch {
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
    level: TreeLevel,
    /// Centroid postings that were already cached at this level
    found: Vec<(VectorId, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    read: Vec<(VectorId, Arc<PostingList>)>,
}

impl ResumableCentroidSearch {
    fn all_found(level: TreeLevel, found: Vec<(VectorId, Arc<PostingList>)>) -> Self {
        for f in &found {
            assert_eq!(level.level(), f.0.level())
        }
        Self {
            level,
            found,
            read: vec![],
        }
    }
}

pub(crate) enum SearchResult {
    ReadsRequired(BlockedCentroidSearch),
    Ann(Vec<Posting>),
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
    // TODO: get rid of this and root() from CentroidCache
    fn read_root(&self) -> MaybeCached<Arc<PostingList>>;

    fn read_postings(&self, centroid_id: VectorId) -> MaybeCached<Arc<PostingList>>;
}

pub(crate) trait CentroidCache: Send + Sync {
    fn root(&self, epoch: u64) -> Option<Arc<PostingList>> {
        self.posting(ROOT_VECTOR_ID, epoch)
    }

    fn posting(&self, centroid_id: VectorId, epoch: u64) -> Option<Arc<PostingList>> {
        self.postings(&[centroid_id], epoch).first().cloned()
    }

    fn postings(&self, _centroid_ids: &[VectorId], _epoch: u64) -> Vec<Arc<PostingList>> {
        Vec::new()
    }
}

pub(crate) struct LeveledCentroidIndex<'a> {
    depth: TreeDepth,
    beam: usize,
    distance_metric: DistanceMetric,
    reader: Arc<dyn CentroidReader + 'a>,
}

impl<'a> LeveledCentroidIndex<'a> {
    pub(crate) fn new(
        depth: TreeDepth,
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
        self.search_in_level(query, k, TreeLevel::leaf(self.depth))
    }

    fn resume_search(
        &self,
        query: &[f32],
        k: usize,
        postings: ResumableCentroidSearch,
    ) -> SearchResult {
        self.resume_search_in_level(query, k, TreeLevel::leaf(self.depth), postings)
    }
}

impl<'a> LeveledCentroidIndex<'a> {
    fn score_and_rank(
        query: &[f32],
        postings: &[Arc<PostingList>],
        k: usize,
        distance_metric: DistanceMetric,
    ) -> Vec<Posting> {
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
                        posting: &posting,
                        distance: compute_distance(query, posting.vector(), distance_metric),
                    });
                }
            }
            ranked.sort_unstable();
            return ranked.into_iter().map(|rp| rp.posting.clone()).collect();
        }

        let mut ranked = BinaryHeap::with_capacity(k);
        let mut heap_ids = HashSet::with_capacity(k);

        for posting_list in postings {
            for posting in posting_list.iter() {
                let distance = compute_distance(query, posting.vector(), distance_metric);
                let candidate = RankedPosting {
                    posting: &posting,
                    distance,
                };

                if heap_ids.contains(&candidate.posting.id()) {
                    continue;
                }

                if ranked.len() < k {
                    heap_ids.insert(candidate.posting.id());
                    ranked.push(candidate);
                    continue;
                }

                let Some(worst) = ranked.peek() else {
                    continue;
                };
                if candidate < *worst {
                    let removed = ranked.pop().expect("heap should be non-empty");
                    heap_ids.remove(&removed.posting.id());
                    heap_ids.insert(candidate.posting.id());
                    ranked.push(candidate);
                }
            }
        }

        let mut ranked = ranked.into_vec();
        ranked.sort_unstable();
        ranked.into_iter().map(|rp| rp.posting.clone()).collect()
    }

    pub(crate) fn search_root(&self, query: &[f32], k: usize) -> SearchResult {
        self.search_in_level(query, k, TreeLevel::root(self.depth).next_level_down())
    }

    fn search_in_level(&self, query: &[f32], k: usize, target_level: TreeLevel) -> SearchResult {
        assert_eq!(target_level.depth(), self.depth);
        let root = self.reader.read_root();
        match root {
            MaybeCached::Value(root) => self.resume_search_in_level(
                query,
                k,
                target_level,
                ResumableCentroidSearch::all_found(
                    TreeLevel::root(self.depth),
                    vec![(ROOT_VECTOR_ID, root)],
                ),
            ),
            MaybeCached::Future(fut) => SearchResult::ReadsRequired(BlockedCentroidSearch::new(
                TreeLevel::root(self.depth),
                vec![],
                vec![(
                    ROOT_VECTOR_ID,
                    Box::pin(async move { fut.await.map(|p| (ROOT_VECTOR_ID, p)) }),
                )],
            )),
        }
    }

    fn search_inner_level(
        &self,
        query: &[f32],
        k: usize,
        target_level: TreeLevel,
        inner_level: TreeLevel,
        centroids: Vec<VectorId>,
    ) -> SearchResult {
        assert_eq!(target_level.depth(), self.depth);
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
            self.resume_search_in_level(
                query,
                k,
                target_level,
                ResumableCentroidSearch::all_found(inner_level, found),
            )
        } else {
            SearchResult::ReadsRequired(BlockedCentroidSearch::new(inner_level, found, reads))
        }
    }

    fn resume_search_in_level(
        &self,
        query: &[f32],
        k: usize,
        target_level: TreeLevel,
        postings: ResumableCentroidSearch,
    ) -> SearchResult {
        assert_eq!(target_level.depth(), self.depth);
        let all_postings = postings
            .found
            .iter()
            .map(|(_, p)| p.clone())
            .chain(postings.read.iter().map(|(_, p)| p.clone()))
            .collect::<Vec<_>>();
        if postings.level.level() == ROOT_LEVEL {
            // root should never be empty
            assert!(!all_postings.is_empty());
        }
        // the postings at a given level hold vector ids for the next level down
        if postings.level.next_level_down() == target_level {
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
                    .map(|posting| posting.id())
                    .collect();
            self.search_inner_level(
                query,
                k,
                target_level,
                postings.level.next_level_down(),
                next_centroids,
            )
        }
    }
}

#[derive(Clone, Debug)]
struct RankedPosting<'a> {
    posting: &'a Posting,
    distance: VectorDistance,
}

impl<'a> PartialEq for RankedPosting<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.posting.id() == other.posting.id() && self.distance == other.distance
    }
}

impl<'a> Eq for RankedPosting<'a> {}

impl<'a> PartialOrd for RankedPosting<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for RankedPosting<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .cmp(&other.distance)
            .then_with(|| self.posting.id().cmp(&other.posting.id()))
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
        self.read_postings(ROOT_VECTOR_ID)
    }

    fn read_postings(&self, centroid_id: VectorId) -> MaybeCached<Arc<PostingList>> {
        let snapshot = self.snapshot.clone();
        let dimensions = self.dimensions;
        MaybeCached::Future(Box::pin(async move {
            Ok(Arc::new(PostingList::from_value(
                snapshot.get_posting_list(centroid_id, dimensions).await?,
            )))
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

    fn read_postings(&self, centroid_id: VectorId) -> MaybeCached<Arc<PostingList>> {
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
    fn posting(&self, centroid_id: VectorId, epoch: u64) -> Option<Arc<PostingList>> {
        self.postings(&[centroid_id], epoch).into_iter().next()
    }

    fn postings(&self, centroid_ids: &[VectorId], epoch: u64) -> Vec<Arc<PostingList>> {
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
        root_posting_list: Arc<dyn IntoTreePostingList>,
        centroid_postings: Vec<(VectorId, Arc<dyn IntoTreePostingList>)>,
    ) -> Self {
        let postings = centroid_postings
            .into_iter()
            .map(|(centroid_id, posting_list)| {
                assert!(centroid_id.level() > 1);
                assert!(centroid_id.is_centroid());
                (
                    centroid_id,
                    WrittenPostingList {
                        written_epoch: 0,
                        posting_list: Arc::new(posting_list.clone_into_tree()),
                    },
                )
            })
            .chain(
                vec![(
                    ROOT_VECTOR_ID,
                    WrittenPostingList {
                        written_epoch: 0,
                        posting_list: Arc::new(root_posting_list.clone_into_tree()),
                    },
                )]
                .into_iter(),
            )
            .collect();
        let inner = Arc::new(Mutex::new(AllCentroidsCacheInner { postings }));
        Self { inner }
    }

    pub(crate) fn update_postings(
        &self,
        epoch: u64,
        root: Option<Vec<PostingUpdate>>,
        root_updates: Vec<PostingUpdate>,
        new_centroids: &HashSet<VectorId>,
        centroid_postings: Vec<(VectorId, Vec<PostingUpdate>)>,
        deleted_centroids: &HashSet<VectorId>,
    ) {
        fn apply_updates(
            posting_list: Option<&Arc<PostingList>>,
            updates: Vec<PostingUpdate>,
        ) -> Arc<PostingList> {
            let Some(posting_list) = posting_list else {
                return Arc::new(PostingList::from_value(
                    PostingListValue::from_posting_updates(updates)
                        .expect("posting updates should always encode"),
                ));
            };
            Arc::new(posting_list.update_and_flatten(updates))
        }

        let mut inner = self.inner.lock().expect("lock poisoned");

        if root.is_some() || !root_updates.is_empty() {
            let root = if let Some(root) = root {
                apply_updates(None, root)
            } else {
                inner
                    .postings
                    .get(&ROOT_VECTOR_ID)
                    .expect("missing root in cache")
                    .posting_list
                    .clone()
            };
            let root = if root_updates.is_empty() {
                root
            } else {
                apply_updates(Some(&root), root_updates)
            };
            inner.postings.insert(
                ROOT_VECTOR_ID,
                WrittenPostingList {
                    posting_list: root,
                    written_epoch: epoch,
                },
            );
        }

        for centroid_id in new_centroids {
            if centroid_id.level() == LEAF_LEVEL {
                // don't cache leaf level centroids
                continue;
            }
            assert!(centroid_id.is_centroid());
            inner.postings.insert(
                *centroid_id,
                WrittenPostingList {
                    posting_list: Arc::new(PostingList::empty()),
                    written_epoch: epoch,
                },
            );
        }

        for (centroid_id, updates) in centroid_postings {
            if centroid_id.level() == LEAF_LEVEL {
                // don't cache leaf level centroids
                continue;
            }
            assert!(centroid_id.is_centroid());
            let posting_list = inner
                .postings
                .get(&centroid_id)
                .expect(&format!("unexpected missing centroid {:?}", centroid_id))
                .posting_list
                .clone();
            let posting_list = apply_updates(Some(&posting_list), updates);
            inner.postings.insert(
                centroid_id,
                WrittenPostingList {
                    posting_list,
                    written_epoch: epoch,
                },
            );
        }

        for centroid_id in deleted_centroids {
            assert!(centroid_id.is_centroid());
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
    postings: HashMap<VectorId, WrittenPostingList>,
}

impl AllCentroidsCacheInner {
    pub(crate) fn new(postings: HashMap<VectorId, WrittenPostingList>) -> Self {
        Self { postings }
    }

    pub(crate) fn postings(&self, centroids: &[VectorId]) -> Vec<WrittenPostingList> {
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
) -> Result<Vec<Posting>> {
    Ok(batch_search_centroids(index, k, vec![(0, query)])
        .await?
        .remove(&0)
        .expect("unreachable"))
}

pub(crate) async fn batch_search_centroids<K: Hash + Eq + Sized + Send + Sync>(
    index: &LeveledCentroidIndex<'_>,
    k: usize,
    queries: Vec<(K, &[f32])>,
) -> Result<HashMap<K, Vec<Posting>>> {
    batch_search_centroids_in_level(index, k, queries, TreeLevel::leaf(index.depth)).await
}

pub(crate) async fn batch_search_centroids_in_level<K: Hash + Eq + Sized + Send + Sync>(
    index: &LeveledCentroidIndex<'_>,
    k: usize,
    queries: Vec<(K, &[f32])>,
    level: TreeLevel,
) -> Result<HashMap<K, Vec<Posting>>> {
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
                    index.resume_search_in_level(&q, k, level, ip)
                } else {
                    index.search_in_level(&q, k, level)
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
    use crate::serde::posting_list::{PostingListValue, PostingUpdate};
    use common::storage::in_memory::InMemoryStorage;
    use common::{Record, Storage, StorageRead};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn should_score_and_rank_l2_postings() {
        // given
        let postings = Arc::new(
            vec![
                Posting::new(vector_id(1, 10), vec![1.0, 0.0]),
                Posting::new(vector_id(1, 11), vec![0.0, 1.0]),
                Posting::new(vector_id(1, 12), vec![3.0, 0.0]),
            ]
            .into_iter()
            .collect(),
        );

        // when
        let ranked =
            LeveledCentroidIndex::score_and_rank(&[0.9, 0.1], &[postings], 2, DistanceMetric::L2);

        // then
        assert_eq!(
            ranked_ids(&ranked),
            vec![vector_id(1, 10), vector_id(1, 11)]
        );
    }

    #[test]
    fn should_deduplicate_ids_and_keep_best_score() {
        // given
        let postings_a = Arc::new(
            vec![
                Posting::new(vector_id(1, 10), vec![1.0, 0.0]),
                Posting::new(vector_id(1, 11), vec![0.0, 1.0]),
            ]
            .into_iter()
            .collect(),
        );
        let postings_b = Arc::new(
            vec![Posting::new(VectorId::centroid_id(1, 10), vec![1.0, 0.0])]
                .into_iter()
                .collect(),
        );

        // when
        let ranked = LeveledCentroidIndex::score_and_rank(
            &[0.9, 0.1],
            &[postings_a, postings_b],
            2,
            DistanceMetric::L2,
        );

        // then
        assert_eq!(
            ranked_ids(&ranked),
            vec![vector_id(1, 10), vector_id(1, 11)]
        );
    }

    const DIMS: usize = 2;

    fn vector_id(level: u8, id: u64) -> VectorId {
        if level == ROOT_LEVEL {
            ROOT_VECTOR_ID
        } else if level > 0 {
            VectorId::centroid_id(level, id)
        } else {
            VectorId::data_vector_id(id)
        }
    }

    fn posting_list(postings: Vec<(u8, u64, Vec<f32>)>) -> Arc<PostingList> {
        Arc::new(
            postings
                .into_iter()
                .map(|(level, id, vector)| Posting::new(vector_id(level, id), vector))
                .collect(),
        )
    }

    fn posting_list_value(postings: Vec<(u8, u64, Vec<f32>)>) -> PostingListValue {
        PostingListValue::from_posting_updates(
            postings
                .into_iter()
                .map(|(level, id, vector)| PostingUpdate::append(vector_id(level, id), vector))
                .collect(),
        )
        .unwrap()
    }

    fn assert_posting_list_eq(actual: Arc<PostingList>, expected: Vec<(u8, u64, Vec<f32>)>) {
        let expected = posting_list(expected);
        assert_eq!(actual.as_ref(), expected.as_ref());
    }

    fn ranked_ids(ranked: &[Posting]) -> Vec<VectorId> {
        ranked.iter().map(|posting| posting.id()).collect()
    }

    fn empty_deleted_centroids() -> HashSet<VectorId> {
        HashSet::new()
    }

    async fn put_posting_list(
        storage: &Arc<dyn Storage>,
        centroid_id: VectorId,
        postings: Vec<(u8, u64, Vec<f32>)>,
    ) -> Arc<PostingList> {
        let key = PostingListKey::new(centroid_id).encode();
        let value = posting_list_value(postings);
        let posting_list = Arc::new(PostingList::from_value(value.clone()));
        storage
            .put(vec![Record::new(key, value.encode_to_bytes()).into()])
            .await
            .unwrap();
        posting_list
    }

    fn cached_index(
        depth: TreeDepth,
        storage: Arc<dyn Storage>,
        root: Arc<PostingList>,
        postings: HashMap<VectorId, Arc<PostingList>>,
        distance_metric: DistanceMetric,
    ) -> LeveledCentroidIndex<'static> {
        let cache: Arc<dyn CentroidCache> = Arc::new(AllCentroidsCache {
            inner: Arc::new(Mutex::new(AllCentroidsCacheInner::new(
                postings
                    .into_iter()
                    .chain(vec![(ROOT_VECTOR_ID, root)].into_iter())
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
        depth: TreeDepth,
        storage: Arc<dyn Storage>,
        root: Arc<PostingList>,
        postings_by_level: HashMap<u8, HashMap<VectorId, Arc<PostingList>>>,
    }

    impl TestTree {
        fn fully_cached_index(&self) -> LeveledCentroidIndex<'static> {
            self.index_with_cached_postings(self.all_posting_ids())
        }

        fn index_with_cached_postings(
            &self,
            cached_postings: Vec<VectorId>,
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

        fn posting(&self, centroid_id: VectorId) -> Option<Arc<PostingList>> {
            self.postings_by_level
                .values()
                .find_map(|postings| postings.get(&centroid_id).cloned())
        }

        fn all_posting_ids(&self) -> Vec<VectorId> {
            self.postings_by_level
                .values()
                .flat_map(|postings| postings.keys().copied())
                .collect()
        }

        fn exhaustive_search(&self, query: &[f32], k: usize) -> Vec<Posting> {
            self.exhaustive_search_in_level(query, k, TreeLevel::leaf(self.depth))
        }

        fn exhaustive_search_in_level(
            &self,
            query: &[f32],
            k: usize,
            target_level: TreeLevel,
        ) -> Vec<Posting> {
            assert!(
                target_level.level() >= 1
                    && target_level.level() <= self.depth.max_inner_level()
            );

            let mut current_level = TreeLevel::root(self.depth);
            let mut current_postings = vec![self.root.clone()];

            loop {
                let next_level = current_level.next_level_down();
                let level_postings = self
                    .postings_by_level
                    .get(&next_level.level())
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
                if next_level == target_level {
                    break;
                }
                current_postings = next_postings;
                current_level = next_level;
            }

            LeveledCentroidIndex::score_and_rank(query, &current_postings, k, DistanceMetric::L2)
        }
    }

    async fn build_tree(
        depth: u8,
        root: Vec<(u8, u64, Vec<f32>)>,
        postings_by_level: Vec<(u8, Vec<((u8, u64), Vec<(u8, u64, Vec<f32>)>)>)>,
    ) -> TestTree {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let root = put_posting_list(&storage, ROOT_VECTOR_ID, root).await;
        let mut tree_postings = HashMap::new();
        for (level, postings) in postings_by_level {
            let mut level_postings = HashMap::with_capacity(postings.len());
            for ((level, cid), posting) in postings {
                let centroid_id = vector_id(level, cid);
                let posting = put_posting_list(&storage, centroid_id, posting).await;
                level_postings.insert(centroid_id, posting);
            }
            tree_postings.insert(level, level_postings);
        }
        TestTree {
            depth: TreeDepth::of(depth),
            storage,
            root,
            postings_by_level: tree_postings,
        }
    }

    async fn one_inner_level_tree() -> TestTree {
        build_tree(
            3,
            vec![(1, 100, vec![10.0, 0.0]), (1, 200, vec![0.0, 10.0])],
            vec![(
                1,
                vec![
                    (
                        (1, 100),
                        vec![(0, 10, vec![1.0, 0.0]), (0, 11, vec![2.0, 0.0])],
                    ),
                    (
                        (1, 200),
                        vec![(0, 20, vec![0.0, 1.0]), (0, 21, vec![0.0, 2.0])],
                    ),
                ],
            )],
        )
        .await
    }

    async fn two_inner_level_tree() -> TestTree {
        build_tree(
            4,
            vec![(2, 1000, vec![10.0, 0.0]), (2, 2000, vec![0.0, 10.0])],
            vec![
                (
                    2,
                    vec![
                        (
                            (2, 1000),
                            vec![(1, 100, vec![5.0, 0.0]), (1, 101, vec![6.0, 0.0])],
                        ),
                        (
                            (2, 2000),
                            vec![(1, 200, vec![0.0, 5.0]), (1, 201, vec![0.0, 6.0])],
                        ),
                    ],
                ),
                (
                    1,
                    vec![
                        (
                            (1, 100),
                            vec![(0, 10, vec![1.0, 0.0]), (0, 11, vec![2.0, 0.0])],
                        ),
                        (
                            (1, 101),
                            vec![(0, 12, vec![3.0, 0.0]), (0, 13, vec![4.0, 0.0])],
                        ),
                        (
                            (1, 200),
                            vec![(0, 20, vec![0.0, 1.0]), (0, 21, vec![0.0, 2.0])],
                        ),
                        (
                            (1, 201),
                            vec![(0, 22, vec![0.0, 3.0]), (0, 23, vec![0.0, 4.0])],
                        ),
                    ],
                ),
            ],
        )
        .await
    }

    async fn wide_two_inner_level_tree(width: u64) -> TestTree {
        let root = (0..width)
            .map(|offset| (2, 1000 + offset, vec![offset as f32, 0.0]))
            .collect();
        let postings = vec![(
            2,
            (0..width)
                .map(|offset| {
                    let centroid_id = 1000 + offset;
                    let leaf_id = 10_000 + offset;
                    (
                        (2, centroid_id),
                        vec![(1, leaf_id, vec![offset as f32, 0.0])],
                    )
                })
                .collect(),
        )];
        build_tree(4, root, postings).await
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
    async fn should_search_one_inner_level_tree() {
        // given
        let tree = one_inner_level_tree().await;
        let index = tree.fully_cached_index();
        let expected = tree.exhaustive_search(&[0.9, 0.1], 2);

        // when
        let ranked = search_centroids(&index, &[0.9, 0.1], 2).await.unwrap();

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_search_two_inner_level_tree() {
        // given
        let tree = two_inner_level_tree().await;
        let index = tree.fully_cached_index();
        let expected = tree.exhaustive_search(&[0.9, 0.1], 2);

        // when
        let ranked = search_centroids(&index, &[0.9, 0.1], 2).await.unwrap();

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_search_root() {
        // given
        let tree = one_inner_level_tree().await;
        let index = tree.fully_cached_index();
        let expected =
            tree.exhaustive_search_in_level(&[0.9, 0.1], 1, TreeLevel::root(tree.depth).next_level_down());

        // when
        let SearchResult::Ann(ranked) =
            index.search_root(&[0.9, 0.1], 1)
        else {
            panic!("search should complete with all centroids cached");
        };

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_search_up_to_level() {
        // given
        let tree = two_inner_level_tree().await;
        let index = tree.fully_cached_index();
        let level = TreeLevel::inner(2, tree.depth);
        let expected = tree.exhaustive_search_in_level(&[5.1, 0.0], 2, level);

        // when
        let SearchResult::Ann(ranked) = index.search_in_level(&[5.1, 0.0], 2, level) else {
            panic!("search should complete with all centroids cached");
        };

        // then
        assert_eq!(ranked, expected);
    }

    #[tokio::test]
    async fn should_resume_search_from_level_when_no_centroids_cached() {
        // given
        let tree = two_inner_level_tree().await;
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
        let tree = two_inner_level_tree().await;
        let index = tree.index_with_cached_postings(vec![vector_id(2, 1000)]);
        let expected = tree.exhaustive_search(&[0.9, 0.1], 2);

        // when
        let SearchResult::ReadsRequired(reads) = index.search(&[0.9, 0.1], 2) else {
            panic!("search should require posting reads");
        };
        assert_eq!(reads.found.len(), 1);
        assert_eq!(reads.reads.len(), 1);
        assert_eq!(reads.reads[0].0, vector_id(2, 2000));
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
        let tree = wide_two_inner_level_tree(101).await;
        let index = tree.index_with_cached_postings(vec![vector_id(2, 1000)]);
        let expected =
            tree.exhaustive_search_in_level(&[0.0, 0.0], 100, TreeLevel::root(tree.depth).next_level_down());
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
        let tree = two_inner_level_tree().await;
        let index = tree.index_with_cached_postings(vec![vector_id(2, 1000), vector_id(1, 100)]);
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
        let tree = two_inner_level_tree().await;
        let index = tree.fully_cached_index();
        let q0 = [5.1, 0.0];
        let q1 = [0.0, 5.1];
        let level = TreeLevel::inner(2, tree.depth);
        let expected_x = tree.exhaustive_search_in_level(&q0, 2, level);
        let expected_y = tree.exhaustive_search_in_level(&q1, 2, level);

        // when
        let results =
            batch_search_centroids_in_level(&index, 2, vec![("x", &q0), ("y", &q1)], level)
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
            posting_list(vec![(1, 1, vec![1.0, 0.0]), (1, 2, vec![2.0, 0.0])]),
            vec![],
        );

        // when
        writer.update_postings(
            5,
            None,
            vec![
                PostingUpdate::delete(vector_id(1, 1)),
                PostingUpdate::append(vector_id(1, 3), vec![3.0, 0.0]),
            ],
            &([vector_id(1, 3)].into_iter().collect()),
            vec![],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert!(cache.root(4).is_none());
        assert_posting_list_eq(
            cache.root(5).expect("root should be cached"),
            vec![(1, 2, vec![2.0, 0.0]), (1, 3, vec![3.0, 0.0])],
        );
    }

    #[test]
    fn should_overwrite_cached_root_with_new_root() {
        // given
        let writer = AllCentroidsCacheWriter::new(
            posting_list(vec![(1, 1, vec![1.0, 0.0]), (1, 2, vec![2.0, 0.0])]),
            vec![],
        );

        // when
        writer.update_postings(
            5,
            Some(vec![
                PostingUpdate::append(vector_id(2, 10), vec![10.0, 0.0]),
                PostingUpdate::append(vector_id(2, 11), vec![11.0, 0.0]),
            ]),
            vec![],
            &HashSet::new(),
            vec![],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache.root(5).expect("root should be cached"),
            vec![(2, 10, vec![10.0, 0.0]), (2, 11, vec![11.0, 0.0])],
        );
    }

    #[test]
    fn should_apply_updates_to_new_root_in_all_centroids_cache() {
        // given
        let writer =
            AllCentroidsCacheWriter::new(posting_list(vec![(1, 1, vec![1.0, 0.0])]), vec![]);

        // when
        writer.update_postings(
            5,
            Some(vec![
                PostingUpdate::append(vector_id(1, 10), vec![10.0, 0.0]),
                PostingUpdate::append(vector_id(1, 11), vec![11.0, 0.0]),
            ]),
            vec![
                PostingUpdate::delete(vector_id(1, 10)),
                PostingUpdate::append(vector_id(1, 12), vec![12.0, 0.0]),
            ],
            &HashSet::new(),
            vec![],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache.root(5).expect("root should be cached"),
            vec![(1, 11, vec![11.0, 0.0]), (1, 12, vec![12.0, 0.0])],
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
            &([vector_id(2, 100)].into_iter().collect()),
            vec![(
                vector_id(2, 100),
                vec![
                    PostingUpdate::append(vector_id(1, 1), vec![1.0, 0.0]),
                    PostingUpdate::append(vector_id(1, 2), vec![2.0, 0.0]),
                ],
            )],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache
                .posting(vector_id(2, 100), 5)
                .expect("centroid should be cached"),
            vec![(1, 1, vec![1.0, 0.0]), (1, 2, vec![2.0, 0.0])],
        );
    }

    #[test]
    fn should_apply_updates_to_cached_centroid_posting() {
        // given
        let writer = AllCentroidsCacheWriter::new(
            posting_list(vec![]),
            vec![(
                vector_id(2, 100),
                posting_list(vec![(1, 1, vec![1.0, 0.0]), (1, 2, vec![2.0, 0.0])]),
            )],
        );

        // when
        writer.update_postings(
            5,
            None,
            vec![],
            &HashSet::new(),
            vec![(
                vector_id(2, 100),
                vec![
                    PostingUpdate::delete(vector_id(1, 1)),
                    PostingUpdate::append(vector_id(1, 3), vec![3.0, 0.0]),
                ],
            )],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache
                .posting(vector_id(2, 100), 5)
                .expect("centroid should be cached"),
            vec![(1, 2, vec![2.0, 0.0]), (1, 3, vec![3.0, 0.0])],
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
            &([vector_id(2, 100)].into_iter().collect()),
            vec![(
                vector_id(2, 100),
                vec![
                    PostingUpdate::append(vector_id(1, 1), vec![1.0, 0.0]),
                    PostingUpdate::append(vector_id(1, 2), vec![2.0, 0.0]),
                    PostingUpdate::delete(vector_id(1, 1)),
                    PostingUpdate::append(vector_id(1, 3), vec![3.0, 0.0]),
                ],
            )],
            &empty_deleted_centroids(),
        );

        // then
        let cache = writer.cache();
        assert_posting_list_eq(
            cache
                .posting(vector_id(2, 100), 5)
                .expect("centroid should be cached"),
            vec![(1, 2, vec![2.0, 0.0]), (1, 3, vec![3.0, 0.0])],
        );
    }
}
