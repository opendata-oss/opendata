//! A counting wrapper around an [`ObjectStore`] that tallies GET requests and
//! bytes read into process-global atomics.
//!
//! Wrapping happens in [`create_object_store`](super::factory::create_object_store),
//! so every store the process opens funnels through it. The counters are global
//! because benchmarks want the aggregate GET load across all reader/writer
//! handles in the process (the RFC's GETs/poll cost signal). Overhead is one
//! relaxed atomic add per GET; non-GET operations are pure delegation.
//!
//! All single-object reads funnel through `get_opts` (the ergonomic
//! `get`/`get_range`/`head` are extension-trait methods that delegate to it), so
//! overriding `get_opts` counts them while preserving the inner store's request
//! behavior; `get_ranges` is the one separate read path and is counted alongside.
//! HEAD requests ride through `get_opts` with `head = true` and are not counted.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

static GETS: AtomicU64 = AtomicU64::new(0);
static GET_BYTES: AtomicU64 = AtomicU64::new(0);

/// Total object-store GET requests issued by this process so far.
pub fn object_store_gets() -> u64 {
    GETS.load(Ordering::Relaxed)
}

/// Total bytes returned by object-store GET requests so far.
pub fn object_store_get_bytes() -> u64 {
    GET_BYTES.load(Ordering::Relaxed)
}

/// A per-handle GET tally.
///
/// The process-global [`object_store_get_bytes`] aggregates every store the
/// process opens, so when several stores share a process — e.g. a standalone
/// reader co-resident with a writer and its compactor — it can't attribute GETs
/// to one of them. Attach a `GetCounters` to a single store via
/// [`count_object_store_with`] and the caller that holds the (clonable) handle
/// can read just that store's GETs. Stores with a handle feed *both* their
/// handle and the global aggregate, so the global stays a true process total.
#[derive(Clone, Debug, Default)]
pub struct GetCounters(Arc<GetCountersInner>);

#[derive(Debug, Default)]
struct GetCountersInner {
    gets: AtomicU64,
    get_bytes: AtomicU64,
}

impl GetCounters {
    pub fn new() -> Self {
        Self::default()
    }

    /// GET requests counted into this handle so far.
    pub fn gets(&self) -> u64 {
        self.0.gets.load(Ordering::Relaxed)
    }

    /// Bytes returned by GETs counted into this handle so far.
    pub fn get_bytes(&self) -> u64 {
        self.0.get_bytes.load(Ordering::Relaxed)
    }

    fn record(&self, gets: u64, bytes: u64) {
        self.0.gets.fetch_add(gets, Ordering::Relaxed);
        self.0.get_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Wrap `inner` so its GET requests are counted into the process-global tallies.
pub fn count_object_store(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(CountingObjectStore {
        inner,
        handle: None,
    })
}

/// Wrap `inner` so its GET requests feed both the process-global tallies and the
/// given per-handle counter, letting the caller measure this store in isolation.
pub fn count_object_store_with(
    inner: Arc<dyn ObjectStore>,
    handle: GetCounters,
) -> Arc<dyn ObjectStore> {
    Arc::new(CountingObjectStore {
        inner,
        handle: Some(handle),
    })
}

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    handle: Option<GetCounters>,
}

impl CountingObjectStore {
    fn record_get(&self, gets: u64, bytes: u64) {
        GETS.fetch_add(gets, Ordering::Relaxed);
        GET_BYTES.fetch_add(bytes, Ordering::Relaxed);
        if let Some(handle) = &self.handle {
            handle.record(gets, bytes);
        }
    }
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    // --- Reads: `get_opts` is the single-object read path (ergonomic
    //     `get`/`get_range`/`head` delegate to it); `get_ranges` is the other. ---

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        // A HEAD request rides through `get_opts` with `head = true`; don't count
        // it as a GET.
        let is_head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !is_head {
            self.record_get(1, result.range.end.saturating_sub(result.range.start));
        }
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let results = self.inner.get_ranges(location, ranges).await?;
        // One GET per requested range (inner may coalesce on the wire, but each
        // range is a logical fetch).
        let total: usize = results.iter().map(|b| b.len()).sum();
        self.record_get(ranges.len() as u64, total as u64);
        Ok(results)
    }

    // --- Everything else: pure delegation. ---

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
