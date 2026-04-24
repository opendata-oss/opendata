//! Test utilities for storage fault injection.
//!
//! Provides [`FailingObjectStore`], a thin wrapper around any
//! [`ObjectStore`] that lets tests toggle failures on individual
//! operations (`put`, `list`, `get`, `delete`).
//!
//! Gated behind `#[cfg(any(test, feature = "testing"))]` so production
//! builds don't compile this module.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use slatedb::object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

/// Which `object_store::Error` kind to inject when a fail-knob is on.
///
/// slatedb 0.12's `RetryingObjectStore` retries every error *forever* except
/// these five non-retryable kinds, so any injected failure that needs to
/// surface to the caller must use one of these. Default: [`FailKind::Precondition`].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FailKind {
    /// `Error::Precondition` — widest semantic fit; slatedb surfaces it without retry.
    Precondition,
    /// `Error::AlreadyExists` — object-already-there.
    AlreadyExists,
    /// `Error::NotFound` — object missing. Note: reads treat NotFound as Ok(None),
    /// so this is mostly useful to fail writes/deletes.
    NotFound,
    /// `Error::NotImplemented` — backend rejects the operation.
    NotImplemented,
    /// `Error::NotSupported` — backend explicitly does not support the operation.
    NotSupported,
}

impl FailKind {
    fn to_u8(self) -> u8 {
        match self {
            FailKind::Precondition => 0,
            FailKind::AlreadyExists => 1,
            FailKind::NotFound => 2,
            FailKind::NotImplemented => 3,
            FailKind::NotSupported => 4,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => FailKind::Precondition,
            1 => FailKind::AlreadyExists,
            2 => FailKind::NotFound,
            3 => FailKind::NotImplemented,
            4 => FailKind::NotSupported,
            _ => FailKind::Precondition,
        }
    }
}

/// Tracks whether a specific op should fail and which kind of error to inject.
struct FailSlot {
    enabled: AtomicBool,
    kind: AtomicU8,
}

impl FailSlot {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            kind: AtomicU8::new(FailKind::Precondition.to_u8()),
        }
    }

    fn set(&self, fail: bool) {
        self.enabled.store(fail, Ordering::SeqCst);
    }

    fn set_kind(&self, kind: FailKind) {
        self.kind.store(kind.to_u8(), Ordering::SeqCst);
        self.enabled.store(true, Ordering::SeqCst);
    }

    fn take(&self) -> Option<FailKind> {
        if self.enabled.load(Ordering::SeqCst) {
            Some(FailKind::from_u8(self.kind.load(Ordering::SeqCst)))
        } else {
            None
        }
    }
}

/// Wraps an [`ObjectStore`] with per-operation failure toggles.
///
/// Each knob holds a `FailKind`; set it from a test to make the corresponding
/// operation return that non-retryable error. Operations not toggled on are
/// forwarded to `inner`.
pub struct FailingObjectStore {
    inner: Arc<dyn ObjectStore>,
    fail_put: FailSlot,
    fail_list: FailSlot,
    fail_get: FailSlot,
    fail_delete: FailSlot,
}

impl FailingObjectStore {
    /// Wraps `inner` with all failure knobs initially disabled.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fail_put: FailSlot::new(),
            fail_list: FailSlot::new(),
            fail_get: FailSlot::new(),
            fail_delete: FailSlot::new(),
        })
    }

    /// Toggle put failures (affects `put`, `put_opts`, `put_multipart`).
    /// Uses the previously-set kind, or [`FailKind::Precondition`] if none set.
    pub fn set_fail_put(&self, fail: bool) {
        self.fail_put.set(fail);
    }

    /// Toggle put failures with a specific error kind.
    pub fn set_fail_put_kind(&self, kind: FailKind) {
        self.fail_put.set_kind(kind);
    }

    /// Toggle list failures (affects `list`, `list_with_delimiter`).
    pub fn set_fail_list(&self, fail: bool) {
        self.fail_list.set(fail);
    }

    /// Toggle list failures with a specific error kind.
    pub fn set_fail_list_kind(&self, kind: FailKind) {
        self.fail_list.set_kind(kind);
    }

    /// Toggle get failures (affects `get`, `get_opts`, `get_range`, `head`).
    pub fn set_fail_get(&self, fail: bool) {
        self.fail_get.set(fail);
    }

    /// Toggle get failures with a specific error kind.
    pub fn set_fail_get_kind(&self, kind: FailKind) {
        self.fail_get.set_kind(kind);
    }

    /// Toggle delete failures (affects `delete`).
    pub fn set_fail_delete(&self, fail: bool) {
        self.fail_delete.set(fail);
    }

    /// Toggle delete failures with a specific error kind.
    pub fn set_fail_delete_kind(&self, kind: FailKind) {
        self.fail_delete.set_kind(kind);
    }
}

impl fmt::Display for FailingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingObjectStore({})", self.inner)
    }
}

impl fmt::Debug for FailingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FailingObjectStore")
            .field("inner", &self.inner)
            .field("fail_put", &self.fail_put.take())
            .field("fail_list", &self.fail_list.take())
            .field("fail_get", &self.fail_get.take())
            .field("fail_delete", &self.fail_delete.take())
            .finish()
    }
}

/// Builds the injected error. Uses one of slatedb's five non-retryable
/// `object_store::Error` kinds so the retry loop doesn't swallow it.
fn injected(op: &'static str, kind: FailKind) -> slatedb::object_store::Error {
    use slatedb::object_store::Error;
    let msg = format!("injected failure: {op}");
    match kind {
        FailKind::Precondition => Error::Precondition {
            path: op.to_string(),
            source: msg.into(),
        },
        FailKind::AlreadyExists => Error::AlreadyExists {
            path: op.to_string(),
            source: msg.into(),
        },
        FailKind::NotFound => Error::NotFound {
            path: op.to_string(),
            source: msg.into(),
        },
        FailKind::NotImplemented => Error::NotImplemented,
        FailKind::NotSupported => Error::NotSupported { source: msg.into() },
    }
}

#[async_trait]
impl ObjectStore for FailingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        if let Some(kind) = self.fail_put.take() {
            return Err(injected("put", kind));
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        if let Some(kind) = self.fail_put.take() {
            return Err(injected("put_multipart", kind));
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        if let Some(kind) = self.fail_get.take() {
            return Err(injected("get", kind));
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<u64>,
    ) -> slatedb::object_store::Result<Bytes> {
        if let Some(kind) = self.fail_get.take() {
            return Err(injected("get_range", kind));
        }
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> slatedb::object_store::Result<ObjectMeta> {
        if let Some(kind) = self.fail_get.take() {
            return Err(injected("head", kind));
        }
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> slatedb::object_store::Result<()> {
        if let Some(kind) = self.fail_delete.take() {
            return Err(injected("delete", kind));
        }
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        if let Some(kind) = self.fail_list.take() {
            use futures::StreamExt;
            return futures::stream::once(async move { Err(injected("list", kind)) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> slatedb::object_store::Result<ListResult> {
        if let Some(kind) = self.fail_list.take() {
            return Err(injected("list_with_delimiter", kind));
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> slatedb::object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> slatedb::object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn should_pass_through_when_no_failures_set() {
        // given
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let failing = FailingObjectStore::new(inner);
        let path = Path::from("foo");

        // when
        failing
            .put(&path, PutPayload::from_static(b"hello"))
            .await
            .unwrap();

        // then
        let got = failing.get(&path).await.unwrap();
        let bytes = got.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn should_fail_put_when_toggled() {
        // given
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let failing = FailingObjectStore::new(inner);
        failing.set_fail_put(true);

        // when
        let result = failing
            .put(&Path::from("x"), PutPayload::from_static(b"data"))
            .await;

        // then
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_fail_delete_when_toggled() {
        // given
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let failing = FailingObjectStore::new(inner);
        failing
            .put(&Path::from("x"), PutPayload::from_static(b"data"))
            .await
            .unwrap();
        failing.set_fail_delete(true);

        // when
        let result = failing.delete(&Path::from("x")).await;

        // then
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_inject_precondition_by_default() {
        // given
        let failing = FailingObjectStore::new(Arc::new(InMemory::new()));
        failing.set_fail_put(true);

        // when
        let err = failing
            .put(&Path::from("x"), PutPayload::from_static(b"d"))
            .await
            .unwrap_err();

        // then
        assert!(
            matches!(err, slatedb::object_store::Error::Precondition { .. }),
            "expected Precondition, got {err:?}"
        );
    }

    #[tokio::test]
    async fn should_inject_each_fail_kind() {
        use slatedb::object_store::Error;
        for kind in [
            FailKind::Precondition,
            FailKind::AlreadyExists,
            FailKind::NotFound,
            FailKind::NotImplemented,
            FailKind::NotSupported,
        ] {
            // given
            let failing = FailingObjectStore::new(Arc::new(InMemory::new()));
            failing.set_fail_put_kind(kind);

            // when
            let err = failing
                .put(&Path::from("x"), PutPayload::from_static(b"d"))
                .await
                .unwrap_err();

            // then
            let ok = matches!(
                (kind, &err),
                (FailKind::Precondition, Error::Precondition { .. })
                    | (FailKind::AlreadyExists, Error::AlreadyExists { .. })
                    | (FailKind::NotFound, Error::NotFound { .. })
                    | (FailKind::NotImplemented, Error::NotImplemented)
                    | (FailKind::NotSupported, Error::NotSupported { .. })
            );
            assert!(ok, "kind={kind:?} got {err:?}");
        }
    }

    /// Sanity check: `db.flush()` with `fail_put` set on a db with pending
    /// writes surfaces the injected failure promptly. This is the primitive
    /// we lean on in the timeseries / vector flusher tests. Note: `db.flush`
    /// on a quiet db is a no-op and does NOT surface failures, which is why
    /// the log readiness endpoint probes the object store directly rather
    /// than via `db.flush`.
    #[tokio::test]
    async fn should_surface_fail_put_through_flush() {
        use slatedb::DbBuilder;
        use std::time::Duration;

        let failing = FailingObjectStore::new(Arc::new(InMemory::new()));
        let db = DbBuilder::new("t", failing.clone() as Arc<dyn ObjectStore>)
            .build()
            .await
            .unwrap();
        // Write non-durably so there's something to flush, without failing
        // the write itself.
        let mut batch = slatedb::WriteBatch::new();
        batch.put(b"k", b"v");
        db.write_with_options(
            batch,
            &slatedb::config::WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();

        failing.set_fail_put(true);

        let r = tokio::time::timeout(Duration::from_secs(5), db.flush()).await;
        assert!(r.is_ok(), "db.flush hung despite fail_put=Precondition");
        assert!(
            r.unwrap().is_err(),
            "expected flush to fail with fail_put set"
        );

        let _ = db.close().await;
    }

    /// End-to-end check: slatedb on top of FailingObjectStore surfaces the
    /// injected Precondition error without looping forever on retries. Uses a
    /// durable write (not `flush`) because slatedb's async flushers swallow
    /// non-durable errors.
    #[tokio::test]
    async fn should_surface_precondition_through_slatedb_durable_write() {
        use slatedb::DbBuilder;
        use slatedb::config::WriteOptions;
        use std::time::Duration;

        let failing = FailingObjectStore::new(Arc::new(InMemory::new()));
        let db = DbBuilder::new("t", failing.clone() as Arc<dyn ObjectStore>)
            .build()
            .await
            .unwrap();

        failing.set_fail_put_kind(FailKind::Precondition);

        let mut batch = slatedb::WriteBatch::new();
        batch.put(b"k", b"v");
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            db.write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                },
            ),
        )
        .await;
        assert!(
            result.is_ok(),
            "db.write(await_durable=true) hung past 5s despite Precondition"
        );
        assert!(
            result.unwrap().is_err(),
            "expected Err from durable write when put is failing with Precondition"
        );

        let _ = db.close().await;
    }
}
