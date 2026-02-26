use std::sync::Arc;

use bytes::Bytes;
use log::LogRead;

use crate::ffi::*;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_reader_open(
    config: *const opendata_log_reader_config_t,
    out_reader: *mut *mut opendata_log_reader_t,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(config, "config") {
        return e;
    }
    if let Err(e) = require_out_ptr(out_reader, "out_reader") {
        return e;
    }

    let config = &*config;
    let rust_config = match build_reader_config(config) {
        Ok(c) => c,
        Err(e) => return e,
    };

    let runtime = match create_runtime() {
        Ok(r) => r,
        Err(e) => return e,
    };

    let reader = match runtime.block_on(log::LogDbReader::open(rust_config)) {
        Ok(r) => r,
        Err(e) => return error_from_log_error(&e),
    };

    *out_reader = Box::into_raw(Box::new(opendata_log_reader_t { reader, runtime }));
    success_result()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_reader_close(
    reader: *mut opendata_log_reader_t,
) -> opendata_log_result_t {
    if reader.is_null() {
        return error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            "reader must not be null",
        );
    }

    let handle = Box::from_raw(reader);
    let runtime = Arc::clone(&handle.runtime);
    runtime.block_on(handle.reader.close());
    success_result()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_reader_scan(
    reader: *const opendata_log_reader_t,
    key: *const u8,
    key_len: usize,
    seq_range: *const opendata_log_seq_range_t,
    out_iterator: *mut *mut opendata_log_iterator_t,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(reader, "reader") {
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

    let handle = &*reader;
    match handle
        .runtime
        .block_on(handle.reader.scan(key_bytes, range))
    {
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
pub unsafe extern "C" fn opendata_log_reader_count(
    reader: *const opendata_log_reader_t,
    key: *const u8,
    key_len: usize,
    seq_range: *const opendata_log_seq_range_t,
    out_count: *mut u64,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(reader, "reader") {
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

    let handle = &*reader;
    match handle
        .runtime
        .block_on(handle.reader.count(key_bytes, range))
    {
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
pub unsafe extern "C" fn opendata_log_reader_count_with_options(
    reader: *const opendata_log_reader_t,
    key: *const u8,
    key_len: usize,
    seq_range: *const opendata_log_seq_range_t,
    options: *const opendata_log_count_options_t,
    out_count: *mut u64,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(reader, "reader") {
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

    let handle = &*reader;
    match handle.runtime.block_on(
        handle
            .reader
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
pub unsafe extern "C" fn opendata_log_reader_list_keys(
    reader: *const opendata_log_reader_t,
    segment_range: *const opendata_log_segment_range_t,
    out_iterator: *mut *mut opendata_log_key_iterator_t,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(reader, "reader") {
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

    let handle = &*reader;
    match handle.runtime.block_on(handle.reader.list_keys(range)) {
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
pub unsafe extern "C" fn opendata_log_reader_list_segments(
    reader: *const opendata_log_reader_t,
    seq_range: *const opendata_log_seq_range_t,
    out_segments: *mut *mut opendata_log_segment_t,
    out_count: *mut usize,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(reader, "reader") {
        return e;
    }
    if let Err(e) = require_handle(seq_range, "seq_range") {
        return e;
    }

    let range = match convert_seq_range(&*seq_range) {
        Ok(r) => r,
        Err(e) => return e,
    };

    let handle = &*reader;
    match handle.runtime.block_on(handle.reader.list_segments(range)) {
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
