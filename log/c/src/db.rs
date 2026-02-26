use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use log::{LogDbBuilder, LogRead};

use crate::ffi::*;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_open(
    config: *const opendata_log_config_t,
    out_log: *mut *mut opendata_log_t,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(config, "config") {
        return e;
    }
    if let Err(e) = require_out_ptr(out_log, "out_log") {
        return e;
    }

    let config = &*config;
    let rust_config = match build_config(config) {
        Ok(c) => c,
        Err(e) => return e,
    };

    let runtime = match create_runtime() {
        Ok(r) => r,
        Err(e) => return e,
    };

    // Separate compaction runtime to prevent block_on deadlock
    let compaction_runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("od-compaction")
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            return error_result(
                opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INTERNAL,
                &format!("failed to create compaction runtime: {e}"),
            );
        }
    };

    let storage_runtime =
        common::StorageRuntime::new().with_compaction_runtime(compaction_runtime.handle().clone());

    let log = match runtime.block_on(
        LogDbBuilder::new(rust_config)
            .with_storage_runtime(storage_runtime)
            .build(),
    ) {
        Ok(log) => log,
        Err(e) => return error_from_log_error(&e),
    };

    *out_log = Box::into_raw(Box::new(opendata_log_t {
        log,
        runtime,
        _compaction_runtime: compaction_runtime,
    }));
    success_result()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_flush(log: *mut opendata_log_t) -> opendata_log_result_t {
    if let Err(e) = require_handle(log, "log") {
        return e;
    }

    let handle = &*log;
    match handle.runtime.block_on(handle.log.flush()) {
        Ok(()) => success_result(),
        Err(e) => error_from_log_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_close(log: *mut opendata_log_t) -> opendata_log_result_t {
    if log.is_null() {
        return error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            "log must not be null",
        );
    }

    let handle = Box::from_raw(log);
    let runtime = Arc::clone(&handle.runtime);
    match runtime.block_on(handle.log.close()) {
        Ok(()) => success_result(),
        Err(e) => error_from_log_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_try_append(
    log: *mut opendata_log_t,
    keys: *const *const u8,
    key_lens: *const usize,
    values: *const *const u8,
    value_lens: *const usize,
    record_count: usize,
    out_start_sequence: *mut u64,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(log, "log") {
        return e;
    }

    let records = match build_records(keys, key_lens, values, value_lens, record_count) {
        Ok(r) => r,
        Err(e) => return e,
    };

    let handle = &*log;
    match handle.runtime.block_on(handle.log.try_append(records)) {
        Ok(output) => {
            if !out_start_sequence.is_null() {
                *out_start_sequence = output.start_sequence;
            }
            success_result()
        }
        Err(e) => error_from_append_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_append_timeout(
    log: *mut opendata_log_t,
    keys: *const *const u8,
    key_lens: *const usize,
    values: *const *const u8,
    value_lens: *const usize,
    record_count: usize,
    timeout_ms: u64,
    out_start_sequence: *mut u64,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(log, "log") {
        return e;
    }

    let records = match build_records(keys, key_lens, values, value_lens, record_count) {
        Ok(r) => r,
        Err(e) => return e,
    };

    let handle = &*log;
    let timeout = Duration::from_millis(timeout_ms);
    match handle
        .runtime
        .block_on(handle.log.append_timeout(records, timeout))
    {
        Ok(output) => {
            if !out_start_sequence.is_null() {
                *out_start_sequence = output.start_sequence;
            }
            success_result()
        }
        Err(e) => error_from_append_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_scan(
    log: *const opendata_log_t,
    key: *const u8,
    key_len: usize,
    seq_range: *const opendata_log_seq_range_t,
    out_iterator: *mut *mut opendata_log_iterator_t,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(log, "log") {
        return e;
    }
    if let Err(e) = require_handle(seq_range, "seq_range") {
        return e;
    }
    if let Err(e) = require_out_ptr(out_iterator, "out_iterator") {
        return e;
    }

    let key_bytes = match bytes_from_ptr(key, key_len, "key") {
        Ok(b) => Bytes::copy_from_slice(b),
        Err(e) => return e,
    };

    let range = match convert_seq_range(&*seq_range) {
        Ok(r) => r,
        Err(e) => return e,
    };

    let handle = &*log;
    match handle.runtime.block_on(handle.log.scan(key_bytes, range)) {
        Ok(iter) => {
            *out_iterator = Box::into_raw(Box::new(opendata_log_iterator_t {
                iter,
                runtime: Arc::clone(&handle.runtime),
            }));
            success_result()
        }
        Err(e) => error_from_log_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_count(
    log: *const opendata_log_t,
    key: *const u8,
    key_len: usize,
    seq_range: *const opendata_log_seq_range_t,
    out_count: *mut u64,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(log, "log") {
        return e;
    }
    if let Err(e) = require_handle(seq_range, "seq_range") {
        return e;
    }

    let key_bytes = match bytes_from_ptr(key, key_len, "key") {
        Ok(b) => Bytes::copy_from_slice(b),
        Err(e) => return e,
    };

    let range = match convert_seq_range(&*seq_range) {
        Ok(r) => r,
        Err(e) => return e,
    };

    let handle = &*log;
    match handle.runtime.block_on(handle.log.count(key_bytes, range)) {
        Ok(count) => {
            if !out_count.is_null() {
                *out_count = count;
            }
            success_result()
        }
        Err(e) => error_from_log_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_count_with_options(
    log: *const opendata_log_t,
    key: *const u8,
    key_len: usize,
    seq_range: *const opendata_log_seq_range_t,
    options: *const opendata_log_count_options_t,
    out_count: *mut u64,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(log, "log") {
        return e;
    }
    if let Err(e) = require_handle(seq_range, "seq_range") {
        return e;
    }
    if let Err(e) = require_handle(options, "options") {
        return e;
    }

    let key_bytes = match bytes_from_ptr(key, key_len, "key") {
        Ok(b) => Bytes::copy_from_slice(b),
        Err(e) => return e,
    };

    let range = match convert_seq_range(&*seq_range) {
        Ok(r) => r,
        Err(e) => return e,
    };

    let rust_options = log::CountOptions {
        approximate: (*options).approximate,
    };

    let handle = &*log;
    match handle.runtime.block_on(
        handle
            .log
            .count_with_options(key_bytes, range, rust_options),
    ) {
        Ok(count) => {
            if !out_count.is_null() {
                *out_count = count;
            }
            success_result()
        }
        Err(e) => error_from_log_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_list_keys(
    log: *const opendata_log_t,
    segment_range: *const opendata_log_segment_range_t,
    out_iterator: *mut *mut opendata_log_key_iterator_t,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(log, "log") {
        return e;
    }
    if let Err(e) = require_handle(segment_range, "segment_range") {
        return e;
    }
    if let Err(e) = require_out_ptr(out_iterator, "out_iterator") {
        return e;
    }

    let range = match convert_segment_range(&*segment_range) {
        Ok(r) => r,
        Err(e) => return e,
    };

    let handle = &*log;
    match handle.runtime.block_on(handle.log.list_keys(range)) {
        Ok(iter) => {
            *out_iterator = Box::into_raw(Box::new(opendata_log_key_iterator_t {
                iter,
                runtime: Arc::clone(&handle.runtime),
            }));
            success_result()
        }
        Err(e) => error_from_log_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_list_segments(
    log: *const opendata_log_t,
    seq_range: *const opendata_log_seq_range_t,
    out_segments: *mut *mut opendata_log_segment_t,
    out_count: *mut usize,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(log, "log") {
        return e;
    }
    if let Err(e) = require_handle(seq_range, "seq_range") {
        return e;
    }

    let range = match convert_seq_range(&*seq_range) {
        Ok(r) => r,
        Err(e) => return e,
    };

    let handle = &*log;
    match handle.runtime.block_on(handle.log.list_segments(range)) {
        Ok(segments) => {
            let c_segments: Vec<opendata_log_segment_t> = segments
                .into_iter()
                .map(|s| opendata_log_segment_t {
                    id: s.id,
                    start_seq: s.start_seq,
                    start_time_ms: s.start_time_ms,
                })
                .collect();
            let count = c_segments.len();
            let mut boxed = c_segments.into_boxed_slice();
            let ptr = boxed.as_mut_ptr();
            std::mem::forget(boxed);

            if !out_segments.is_null() {
                *out_segments = ptr;
            }
            if !out_count.is_null() {
                *out_count = count;
            }
            success_result()
        }
        Err(e) => error_from_log_error(&e),
    }
}

unsafe fn build_records(
    keys: *const *const u8,
    key_lens: *const usize,
    values: *const *const u8,
    value_lens: *const usize,
    record_count: usize,
) -> Result<Vec<log::Record>, opendata_log_result_t> {
    if record_count == 0 {
        return Ok(Vec::new());
    }
    if keys.is_null() || key_lens.is_null() || values.is_null() || value_lens.is_null() {
        return Err(error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            "keys, key_lens, values, and value_lens must not be null when record_count > 0",
        ));
    }

    let keys_ptrs = std::slice::from_raw_parts(keys, record_count);
    let key_lengths = std::slice::from_raw_parts(key_lens, record_count);
    let values_ptrs = std::slice::from_raw_parts(values, record_count);
    let value_lengths = std::slice::from_raw_parts(value_lens, record_count);

    let mut records = Vec::with_capacity(record_count);
    for i in 0..record_count {
        let key_data = bytes_from_ptr(keys_ptrs[i], key_lengths[i], "key")?;
        let value_data = bytes_from_ptr(values_ptrs[i], value_lengths[i], "value")?;
        records.push(log::Record {
            key: Bytes::copy_from_slice(key_data),
            value: Bytes::copy_from_slice(value_data),
        });
    }
    Ok(records)
}
