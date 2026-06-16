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

/// Wrap `inner` so its GET requests are counted into the process-global tallies.
pub fn count_object_store(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(CountingObjectStore { inner })
}

fn record_get(bytes: u64) {
    GETS.fetch_add(1, Ordering::Relaxed);
    GET_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
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
            record_get(result.range.end.saturating_sub(result.range.start));
        }
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let results = self.inner.get_ranges(location, ranges).await?;
        // One GET per requested range (inner may coalesce on the wire, but each
        // range is a logical fetch).
        GETS.fetch_add(ranges.len() as u64, Ordering::Relaxed);
        let total: usize = results.iter().map(|b| b.len()).sum();
        GET_BYTES.fetch_add(total as u64, Ordering::Relaxed);
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
