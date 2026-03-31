use crate::Result;
use crate::serde::posting_list::{PostingList, PostingListValue};
use crate::storage::VectorDbStorageReadExt;
use crate::write::indexer::drivers::AsyncBatchDriver;
use common::StorageRead;
use futures::future::BoxFuture;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};

pub(crate) enum MaybeBoxFuture<T> {
    Value(T),
    Future(BoxFuture<'static, T>),
}

impl <T> Into<MaybeBoxFuture<T>> for BoxFuture<'static, T> {
    fn into(self) -> MaybeBoxFuture<T> {
        MaybeBoxFuture::Future(self)
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
        postings: Option<IntermediatePostings>,
    ) -> SearchResult;
}

pub(crate) trait CentroidReader: Send + Sync {
    fn read_root(&self) -> MaybeBoxFuture<Result<Arc<PostingList>>>;

    fn read_postings(
        &self,
        centroid_id: u64,
    ) -> Result<MaybeBoxFuture<Result<Arc<PostingList>>>>;
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
    reader: Arc<dyn CentroidReader + 'a>,
}

impl<'a> LeveledCentroidIndex<'a> {
    pub(crate) fn new(
        reader: Arc<dyn CentroidReader + 'a>
    ) -> Self {
        Self {
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
        postings: Option<IntermediatePostings>,
    ) -> SearchResult {
        self.resume_search_up_to_level(query, k, 0, postings)
    }
}

impl<'a> LeveledCentroidIndex<'a> {
    pub(crate) fn search_root(&self, query: &[f32], k: usize) -> Vec<u64> {
        todo!()
    }

    fn search_up_to_level(&self, query: &[f32], k: usize, level: u16) -> SearchResult {
        // start search at root, finding the 100 nearest neighbours, then move all the way down
        // to level 0, expanding out 100 at each level
        todo!()
    }

    fn resume_search_up_to_level(
        &self,
        query: &[f32],
        k: usize,
        level: u16,
        postings: Option<IntermediatePostings>,
    ) -> SearchResult {
        // postings as the centroids required at a given level, resume search at the next level
        todo!()
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
    fn read_root(&self) -> MaybeBoxFuture<Result<Arc<PostingList>>> {
        let snapshot = self.snapshot.clone();
        let dimensions = self.dimensions;
        MaybeBoxFuture::Future(
            Box::pin(async move { Ok(Arc::new(snapshot.get_root_posting_list(dimensions).await?.into())) }))
    }

    fn read_postings(
        &self,
        centroid_id: u64,
    ) -> Result<MaybeBoxFuture<Result<Arc<PostingList>>>> {
        let snapshot = self.snapshot.clone();
        let dimensions = self.dimensions;
        Ok(MaybeBoxFuture::Future(Box::pin(async move {
            Ok(Arc::new(snapshot.get_posting_list(centroid_id, dimensions).await?.into()))
        })))
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
    fn read_root(&self) -> MaybeBoxFuture<Result<Arc<PostingList>>> {
        if let Some(root) = self.cache.root(self.inner.epoch) {
            MaybeBoxFuture::Value(Ok(root))
        } else {
            self.inner.read_root()
        }
    }

    fn read_postings(&self, centroid_id: u64) -> Result<MaybeBoxFuture<Result<Arc<PostingList>>>> {
        if let Some(postings) = self.cache.posting(centroid_id, self.inner.epoch) {
            Ok(MaybeBoxFuture::Value(Ok(postings)))
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
                let result = index.resume_search_up_to_level(&q, k, level, ip);
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
