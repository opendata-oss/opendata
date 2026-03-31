use crate::Result;
use crate::serde::posting_list::{PostingList};
use crate::storage::VectorDbStorageReadExt;
use crate::write::indexer::drivers::AsyncBatchDriver;
use common::StorageRead;
use futures::future::BoxFuture;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

pub(crate) enum MaybeCached<T> {
    Value(T),
    Future(BoxFuture<'static, Result<T>>),
}

impl <T> MaybeCached<T> {
    pub(crate) async fn get(self) -> Result<T> {
        match self {
            MaybeCached::Value(value) => Ok(value),
            MaybeCached::Future(future) => future.await,
        }
    }
}

impl <T: 'static> MaybeCached<T> {
    pub(crate) fn map<O>(self, mapper: impl FnOnce(T) -> O + Send + Sync + 'static) -> MaybeCached<O> {
        match self {
            MaybeCached::Value(v) => MaybeCached::Value(mapper(v)),
            MaybeCached::Future(fut) => MaybeCached::Future(
                Box::pin(async move { fut.await.map(|v| mapper(v)) }),
            )
        }
    }
}

impl <T> Into<MaybeCached<T>> for BoxFuture<'static, Result<T>> {
    fn into(self) -> MaybeCached<T> {
        MaybeCached::Future(self)
    }
}

pub(crate) struct IntermediatePostingsRead {
    /// The level the reads are for
    level: u16,
    /// Centroid postings that were already cached at this level
    found: Vec<(u64, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    reads: Vec<(u64, BoxFuture<'static, Result<(u64, Arc<PostingList>)>>)>,
}

impl IntermediatePostingsRead {
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
        InFlightIntermediatePostingsRead,
    ) {
        let reads = self.reads;
        let ids = reads.iter().map(|(id, _)| *id).collect();
        let in_flight = InFlightIntermediatePostingsRead {
            level: self.level,
            found: self.found,
            reads: ids,
        };
        (reads, in_flight)
    }
}

pub(crate) struct InFlightIntermediatePostingsRead {
    /// The level the reads are for
    level: u16,
    /// Centroid postings that were already cached at this level
    found: Vec<(u64, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    reads: Vec<u64>,
}

impl InFlightIntermediatePostingsRead {
    pub(crate) fn finish(self, read: &HashMap<u64, Arc<PostingList>>) -> IntermediatePostings {
        let read = self
            .reads
            .into_iter()
            .map(|c| (c, read[&c].clone()))
            .collect::<Vec<_>>();
        IntermediatePostings {
            level: self.level,
            found: self.found,
            read,
        }
    }
}

pub(crate) struct IntermediatePostings {
    /// The level the reads are for
    level: u16,
    /// Centroid postings that were already cached at this level
    found: Vec<(u64, Arc<PostingList>)>,
    /// Centroid postings that need to be read
    read: Vec<(u64, Arc<PostingList>)>,
}

impl IntermediatePostings {
    fn all_found(level: u16, found: Vec<(u64, Arc<PostingList>)>) -> Self {
        Self {
            level,
            found,
            read: vec![],
        }
    }
}

pub(crate) enum SearchResult {
    PostingReadRequired(IntermediatePostingsRead),
    Ann(Vec<u64>),
}

pub(crate) trait CentroidIndex {
    fn search(&self, query: &[f32], k: usize) -> SearchResult;

    fn resume_search(
        &self,
        query: &[f32],
        k: usize,
        postings: IntermediatePostings,
    ) -> SearchResult;
}

pub(crate) trait CentroidReader: Send + Sync {
    fn read_root(&self) -> MaybeCached<Arc<PostingList>>;

    fn read_postings(
        &self,
        centroid_id: u64,
    ) -> MaybeCached<Arc<PostingList>>;
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
    reader: Arc<dyn CentroidReader + 'a>,
}

impl<'a> LeveledCentroidIndex<'a> {
    pub(crate) fn new(
        depth: u16,
        reader: Arc<dyn CentroidReader + 'a>
    ) -> Self {
        Self {
            depth,
            beam: 100,
            reader
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
        postings: IntermediatePostings,
    ) -> SearchResult {
        self.resume_search_up_to_level(query, k, 0, postings)
    }
}

impl<'a> LeveledCentroidIndex<'a> {
    fn score_and_rank(query: &[f32], postings: &[Arc<PostingList>], k: usize) -> Vec<u64> {
        // compute the distance from query to all vectors in postings
        // return the top k nearest vectors
        todo!()
    }

    pub(crate) fn search_root(&self, query: &[f32], k: usize) -> Vec<u64> {
        let MaybeCached::Value(root) = self.reader.read_root() else {
            panic!("LeveledCentroidIndex::search_root() couldn't find root");
        };
        Self::score_and_rank(query, &[root], k)
    }

    fn search_up_to_level(&self, query: &[f32], k: usize, level: u16) -> SearchResult {
        let this_level = self.depth - 1;
        assert!(level <= this_level);
        assert!(level >= 0);
        if level == this_level {
            return SearchResult::Ann(self.search_root(query, k))
        }
        let next_centroids = self.search_root(query, self.beam);
        self.search_up_to_level_with_centroids_at_inner_level(
            query,
            k,
            level,
            this_level - 1,
            next_centroids
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
        let posting_futs: Vec<_> = centroids.iter().map(|&c| (c, self.reader.read_postings(c))).collect();
        let mut found = Vec::with_capacity(centroids.len());
        let mut reads = Vec::with_capacity(centroids.len());
        for (c, fut) in posting_futs {
            match fut {
                MaybeCached::Value(p) => {
                    found.push((c, p));
                }
                MaybeCached::Future(fut) => {
                    reads.push(
                        (c,
                         Box::pin(
                             async move {
                                 fut.await.map(|p| (c, p))
                             }) as BoxFuture<'static, _>)
                    );
                }
            }
        }
        if reads.is_empty() {
            self.resume_search_up_to_level(
                query,
                k,
                level,
                IntermediatePostings::all_found(inner_level, found),
            )
        } else {
            SearchResult::PostingReadRequired(
                IntermediatePostingsRead::new(inner_level, found, reads)
            )
        }
    }

    fn resume_search_up_to_level(
        &self,
        query: &[f32],
        k: usize,
        level: u16,
        postings: IntermediatePostings,
    ) -> SearchResult {
        let all_postings = postings
            .found
            .iter()
            .map(|(_, p)| p.clone())
            .chain(postings.read.iter().map(|(_, p)| p.clone()))
            .collect::<Vec<_>>();
        if postings.level == level {
            SearchResult::Ann(Self::score_and_rank(query, &all_postings, k))
        } else {
            // find the top beam postings at this level and search at the next level
            // then call search_up_to_level_with_centroids_at_inner_level
            let next_centroids = Self::score_and_rank(query, &all_postings, self.beam);
            self.search_up_to_level_with_centroids_at_inner_level(
                query,
                k,
                level,
                postings.level - 1,
                next_centroids
            )
        }
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
        MaybeCached::Future(
            Box::pin(async move { Ok(Arc::new(snapshot.get_root_posting_list(dimensions).await?.into())) }))
    }

    fn read_postings(
        &self,
        centroid_id: u64,
    ) -> MaybeCached<Arc<PostingList>> {
        let snapshot = self.snapshot.clone();
        let dimensions = self.dimensions;
        MaybeCached::Future(Box::pin(async move {
            Ok(Arc::new(snapshot.get_posting_list(centroid_id, dimensions).await?.into()))
        }))
    }
}

struct CachedCentroidReader {
    cache: Arc<dyn CentroidCache>,
    inner: StoredCentroidReader
}

impl<'a> CachedCentroidReader {
    pub(crate) fn new(
        cache: &Arc<dyn CentroidCache>,
        inner: StoredCentroidReader
    ) -> Self {
        Self {
            cache: cache.clone(),
            inner
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

struct AllCentroidsCache {
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
        self
            .inner
            .lock()
            .expect("lock poisoned")
            .postings(centroid_ids)
            .into_iter()
            .filter_map(|p| if epoch < p.written_epoch { None } else { Some(p.posting_list.clone()) })
            .collect()
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
        Self { root, postings}
    }

    pub(crate) fn root(&self) -> WrittenPostingList {
        self.root.clone()
    }

    pub(crate) fn postings(&self, centroids: &[u64]) -> Vec<WrittenPostingList> {
        centroids.iter().filter_map(|c| self.postings.get(c)).cloned().collect()
    }
}

pub(crate) async fn batch_search_centroids<K: Hash + Eq + Sized + Send + Sync>(
    index: &LeveledCentroidIndex<'_>,
    k: usize,
    queries: Vec<(K, &[f32])>,
) -> Result<HashMap<K, Vec<u64>>> {
    batch_search_centroids_up_to_level(index, k, queries, 0).await
}

pub(crate) async fn batch_search_centroids_up_to_level<K: Hash + Eq + Sized + Send + Sync>(
    index: &LeveledCentroidIndex<'_>,
    k: usize,
    queries: Vec<(K, &[f32])>,
    level: u16,
) -> Result<HashMap<K, Vec<u64>>> {
    let mut results = HashMap::with_capacity(queries.len());
    let queries: Vec<(K, &[f32], Option<IntermediatePostings>)> = queries
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
                SearchResult::PostingReadRequired(reads) => {
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
