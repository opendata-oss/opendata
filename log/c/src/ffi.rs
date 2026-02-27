use std::ffi::{CStr, CString, c_char};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::time::Duration;

use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use log::{Config, ReaderConfig, SegmentConfig};
use tokio::runtime::Runtime;

pub struct opendata_log_t {
    pub(crate) log: log::LogDb,
    pub(crate) runtime: Arc<Runtime>,
    // Kept alive so compaction tasks continue running; never read directly.
    pub(crate) _compaction_runtime: Runtime,
}

pub struct opendata_log_reader_t {
    pub(crate) reader: log::LogDbReader,
    pub(crate) runtime: Arc<Runtime>,
}

pub struct opendata_log_iterator_t {
    pub(crate) iter: log::LogIterator,
    pub(crate) runtime: Arc<Runtime>,
}

pub struct opendata_log_key_iterator_t {
    pub(crate) iter: log::LogKeyIterator,
    pub(crate) runtime: Arc<Runtime>,
}

pub struct opendata_log_object_store_t {
    pub(crate) config: ObjectStoreConfig,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum opendata_log_error_kind_t {
    OPENDATA_LOG_OK = 0,
    OPENDATA_LOG_ERROR_STORAGE,
    OPENDATA_LOG_ERROR_ENCODING,
    OPENDATA_LOG_ERROR_INVALID_INPUT,
    OPENDATA_LOG_ERROR_INTERNAL,
    OPENDATA_LOG_ERROR_QUEUE_FULL,
    OPENDATA_LOG_ERROR_TIMEOUT,
    OPENDATA_LOG_ERROR_SHUTDOWN,
    OPENDATA_LOG_ERROR_INVALID_RECORD,
}

#[repr(C)]
pub struct opendata_log_result_t {
    pub kind: opendata_log_error_kind_t,
    pub message: *mut c_char,
}

pub const OPENDATA_LOG_STORAGE_IN_MEMORY: u8 = 0;
pub const OPENDATA_LOG_STORAGE_SLATEDB: u8 = 1;

#[repr(C)]
pub struct opendata_log_config_t {
    pub storage_type: u8,
    pub slatedb_path: *const c_char,
    pub object_store: *const opendata_log_object_store_t,
    pub settings_path: *const c_char,
    pub seal_interval_ms: i64,
}

#[repr(C)]
pub struct opendata_log_reader_config_t {
    pub storage_type: u8,
    pub slatedb_path: *const c_char,
    pub object_store: *const opendata_log_object_store_t,
    pub settings_path: *const c_char,
    pub refresh_interval_ms: i64,
}

pub const OPENDATA_LOG_BOUND_UNBOUNDED: u8 = 0;
pub const OPENDATA_LOG_BOUND_INCLUDED: u8 = 1;
pub const OPENDATA_LOG_BOUND_EXCLUDED: u8 = 2;

#[repr(C)]
pub struct opendata_log_seq_bound_t {
    pub kind: u8,
    pub value: u64,
}

#[repr(C)]
pub struct opendata_log_seq_range_t {
    pub start: opendata_log_seq_bound_t,
    pub end: opendata_log_seq_bound_t,
}

#[repr(C)]
pub struct opendata_log_segment_bound_t {
    pub kind: u8,
    pub value: u32,
}

#[repr(C)]
pub struct opendata_log_segment_range_t {
    pub start: opendata_log_segment_bound_t,
    pub end: opendata_log_segment_bound_t,
}

#[repr(C)]
pub struct opendata_log_count_options_t {
    pub approximate: bool,
}

#[repr(C)]
pub struct opendata_log_segment_t {
    pub id: u32,
    pub start_seq: u64,
    pub start_time_ms: i64,
}

pub(crate) fn success_result() -> opendata_log_result_t {
    opendata_log_result_t {
        kind: opendata_log_error_kind_t::OPENDATA_LOG_OK,
        message: std::ptr::null_mut(),
    }
}

pub(crate) fn error_result(
    kind: opendata_log_error_kind_t,
    message: &str,
) -> opendata_log_result_t {
    let c_message =
        CString::new(message).unwrap_or_else(|_| CString::new("unknown error").unwrap());
    opendata_log_result_t {
        kind,
        message: c_message.into_raw(),
    }
}

pub(crate) fn error_from_log_error(err: &log::Error) -> opendata_log_result_t {
    let kind = match err {
        log::Error::Storage(_) => opendata_log_error_kind_t::OPENDATA_LOG_ERROR_STORAGE,
        log::Error::Encoding(_) => opendata_log_error_kind_t::OPENDATA_LOG_ERROR_ENCODING,
        log::Error::InvalidInput(_) => opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
        log::Error::Internal(_) => opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INTERNAL,
    };
    error_result(kind, &err.to_string())
}

pub(crate) fn error_from_append_error(err: &log::AppendError) -> opendata_log_result_t {
    let kind = match err {
        log::AppendError::QueueFull(_) => opendata_log_error_kind_t::OPENDATA_LOG_ERROR_QUEUE_FULL,
        log::AppendError::Timeout(_) => opendata_log_error_kind_t::OPENDATA_LOG_ERROR_TIMEOUT,
        log::AppendError::Shutdown => opendata_log_error_kind_t::OPENDATA_LOG_ERROR_SHUTDOWN,
        log::AppendError::Storage(_) => opendata_log_error_kind_t::OPENDATA_LOG_ERROR_STORAGE,
    };
    error_result(kind, &err.to_string())
}

pub(crate) fn require_handle<T>(ptr: *const T, name: &str) -> Result<(), opendata_log_result_t> {
    if ptr.is_null() {
        Err(error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            &format!("{name} must not be null"),
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn require_out_ptr<T>(
    ptr: *mut *mut T,
    name: &str,
) -> Result<(), opendata_log_result_t> {
    if ptr.is_null() {
        Err(error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            &format!("{name} must not be null"),
        ))
    } else {
        Ok(())
    }
}

pub(crate) unsafe fn bytes_from_ptr<'a>(
    ptr: *const u8,
    len: usize,
    name: &str,
) -> Result<&'a [u8], opendata_log_result_t> {
    if ptr.is_null() && len > 0 {
        Err(error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            &format!("{name} must not be null when length > 0"),
        ))
    } else if ptr.is_null() {
        Ok(&[])
    } else {
        Ok(std::slice::from_raw_parts(ptr, len))
    }
}

pub(crate) unsafe fn cstr_to_string(
    ptr: *const c_char,
    name: &str,
) -> Result<String, opendata_log_result_t> {
    if ptr.is_null() {
        Err(error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            &format!("{name} must not be null"),
        ))
    } else {
        CStr::from_ptr(ptr)
            .to_str()
            .map(|s| s.to_string())
            .map_err(|e| {
                error_result(
                    opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
                    &format!("{name} is not valid UTF-8: {e}"),
                )
            })
    }
}

pub(crate) fn alloc_bytes(data: &[u8]) -> (*mut u8, usize) {
    let len = data.len();
    if len == 0 {
        return (std::ptr::null_mut(), 0);
    }
    let layout = std::alloc::Layout::from_size_align(len, 1).unwrap();
    unsafe {
        let ptr = std::alloc::alloc(layout);
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, len);
        (ptr, len)
    }
}

pub(crate) fn create_runtime() -> Result<Arc<Runtime>, opendata_log_result_t> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map(Arc::new)
        .map_err(|e| {
            error_result(
                opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INTERNAL,
                &format!("failed to create tokio runtime: {e}"),
            )
        })
}

pub(crate) unsafe fn build_storage_config(
    storage_type: u8,
    slatedb_path: *const c_char,
    object_store: *const opendata_log_object_store_t,
    settings_path: *const c_char,
) -> Result<StorageConfig, opendata_log_result_t> {
    match storage_type {
        OPENDATA_LOG_STORAGE_IN_MEMORY => Ok(StorageConfig::InMemory),
        OPENDATA_LOG_STORAGE_SLATEDB => {
            let path = cstr_to_string(slatedb_path, "slatedb_path")?;
            require_handle(object_store, "object_store")?;
            let os_config = (*object_store).config.clone();
            let settings = if settings_path.is_null() {
                None
            } else {
                Some(cstr_to_string(settings_path, "settings_path")?)
            };
            Ok(StorageConfig::SlateDb(SlateDbStorageConfig {
                path,
                object_store: os_config,
                settings_path: settings,
            }))
        }
        _ => Err(error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            &format!("invalid storage_type: {storage_type}"),
        )),
    }
}

pub(crate) unsafe fn build_config(
    config: &opendata_log_config_t,
) -> Result<Config, opendata_log_result_t> {
    let storage = build_storage_config(
        config.storage_type,
        config.slatedb_path,
        config.object_store,
        config.settings_path,
    )?;
    let seal_interval = if config.seal_interval_ms < 0 {
        None
    } else {
        Some(Duration::from_millis(config.seal_interval_ms as u64))
    };
    Ok(Config {
        storage,
        segmentation: SegmentConfig { seal_interval },
    })
}

pub(crate) unsafe fn build_reader_config(
    config: &opendata_log_reader_config_t,
) -> Result<ReaderConfig, opendata_log_result_t> {
    let storage = build_storage_config(
        config.storage_type,
        config.slatedb_path,
        config.object_store,
        config.settings_path,
    )?;
    let refresh_interval = if config.refresh_interval_ms < 0 {
        Duration::from_secs(1) // default
    } else {
        Duration::from_millis(config.refresh_interval_ms as u64)
    };
    Ok(ReaderConfig {
        storage,
        refresh_interval,
    })
}

/// A concrete range type that implements RangeBounds for use with the LogRead trait.
pub(crate) struct FfiBound<T> {
    pub start: Bound<T>,
    pub end: Bound<T>,
}

impl<T> RangeBounds<T> for FfiBound<T> {
    fn start_bound(&self) -> Bound<&T> {
        match &self.start {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
    fn end_bound(&self) -> Bound<&T> {
        match &self.end {
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

fn convert_u64_bound(kind: u8, value: u64) -> Result<Bound<u64>, opendata_log_result_t> {
    match kind {
        OPENDATA_LOG_BOUND_UNBOUNDED => Ok(Bound::Unbounded),
        OPENDATA_LOG_BOUND_INCLUDED => Ok(Bound::Included(value)),
        OPENDATA_LOG_BOUND_EXCLUDED => Ok(Bound::Excluded(value)),
        _ => Err(error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            &format!("invalid bound kind: {kind}"),
        )),
    }
}

fn convert_u32_bound(kind: u8, value: u32) -> Result<Bound<u32>, opendata_log_result_t> {
    match kind {
        OPENDATA_LOG_BOUND_UNBOUNDED => Ok(Bound::Unbounded),
        OPENDATA_LOG_BOUND_INCLUDED => Ok(Bound::Included(value)),
        OPENDATA_LOG_BOUND_EXCLUDED => Ok(Bound::Excluded(value)),
        _ => Err(error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            &format!("invalid bound kind: {kind}"),
        )),
    }
}

pub(crate) fn convert_seq_range(
    range: &opendata_log_seq_range_t,
) -> Result<FfiBound<u64>, opendata_log_result_t> {
    Ok(FfiBound {
        start: convert_u64_bound(range.start.kind, range.start.value)?,
        end: convert_u64_bound(range.end.kind, range.end.value)?,
    })
}

pub(crate) fn convert_segment_range(
    range: &opendata_log_segment_range_t,
) -> Result<FfiBound<u32>, opendata_log_result_t> {
    Ok(FfiBound {
        start: convert_u32_bound(range.start.kind, range.start.value)?,
        end: convert_u32_bound(range.end.kind, range.end.value)?,
    })
}
