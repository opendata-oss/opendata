use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::ffi::*;

/// Lazily-initialized global `PrometheusHandle`. The inner `Result` distinguishes
/// "we own the global metrics recorder" (`Ok`) from "someone else installed a
/// recorder before us so our handle would render nothing useful" (`Err`).
static HANDLE: OnceLock<Result<PrometheusHandle, ()>> = OnceLock::new();

fn handle() -> &'static Result<PrometheusHandle, ()> {
    HANDLE.get_or_init(|| {
        let recorder = PrometheusBuilder::new().build_recorder();
        let h = recorder.handle();
        match metrics::set_global_recorder(recorder) {
            Ok(()) => Ok(h),
            Err(_) => Err(()),
        }
    })
}

/// Installs a global `metrics-rs` Prometheus recorder that captures SlateDB
/// metrics emitted from any `LogDb` or `LogDbReader` opened in this process.
///
/// **Idempotent and lock-free.** Safe to call any number of times from any
/// thread; only the first call actually installs the recorder, subsequent
/// calls observe the cached result. Safe to call before or after
/// `opendata_log_open` / `opendata_log_reader_open`.
///
/// Returns `OPENDATA_LOG_ERROR_INTERNAL` if a different `metrics-rs` recorder
/// is already installed in the process (e.g. by another embedded Rust
/// component). In that case `opendata_log_render_metrics` will return an
/// empty buffer.
#[unsafe(no_mangle)]
pub extern "C" fn opendata_log_init_telemetry() -> opendata_log_result_t {
    match handle() {
        Ok(_) => success_result(),
        Err(()) => error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INTERNAL,
            "another metrics-rs recorder is already installed in this process",
        ),
    }
}

/// Renders the current SlateDB metrics in Prometheus text-exposition format.
///
/// On success, allocates a heap buffer and writes its address and length to
/// `*out_data` / `*out_len`. The caller owns the buffer and must release it
/// with `opendata_log_bytes_free`.
///
/// Lazily initializes the recorder on first call if `opendata_log_init_telemetry`
/// was not invoked explicitly. If a foreign `metrics-rs` recorder occupies
/// the global slot, returns success with `*out_data = NULL` and `*out_len = 0`
/// (an empty buffer is not an error).
///
/// `out_data` and `out_len` must be non-null. The function never blocks.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_render_metrics(
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> opendata_log_result_t {
    if out_data.is_null() || out_len.is_null() {
        return error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            "out_data and out_len must not be null",
        );
    }

    let (ptr, len) = match handle() {
        Ok(h) => alloc_bytes(h.render().as_bytes()),
        Err(()) => (std::ptr::null_mut(), 0),
    };
    *out_data = ptr;
    *out_len = len;
    success_result()
}
