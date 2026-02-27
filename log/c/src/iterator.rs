use crate::ffi::*;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_iterator_next(
    iterator: *mut opendata_log_iterator_t,
    out_present: *mut bool,
    out_key: *mut *mut u8,
    out_key_len: *mut usize,
    out_sequence: *mut u64,
    out_value: *mut *mut u8,
    out_value_len: *mut usize,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(iterator, "iterator") {
        return e;
    }

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.next()) {
        Ok(Some(entry)) => {
            if !out_present.is_null() {
                *out_present = true;
            }
            if !out_key.is_null() && !out_key_len.is_null() {
                let (ptr, len) = alloc_bytes(&entry.key);
                *out_key = ptr;
                *out_key_len = len;
            }
            if !out_sequence.is_null() {
                *out_sequence = entry.sequence;
            }
            if !out_value.is_null() && !out_value_len.is_null() {
                let (ptr, len) = alloc_bytes(&entry.value);
                *out_value = ptr;
                *out_value_len = len;
            }
            success_result()
        }
        Ok(None) => {
            if !out_present.is_null() {
                *out_present = false;
            }
            success_result()
        }
        Err(e) => error_from_log_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_iterator_close(
    iterator: *mut opendata_log_iterator_t,
) -> opendata_log_result_t {
    if iterator.is_null() {
        return error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            "iterator must not be null",
        );
    }
    let _ = Box::from_raw(iterator);
    success_result()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_key_iterator_next(
    iterator: *mut opendata_log_key_iterator_t,
    out_present: *mut bool,
    out_key: *mut *mut u8,
    out_key_len: *mut usize,
) -> opendata_log_result_t {
    if let Err(e) = require_handle(iterator, "iterator") {
        return e;
    }

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.next()) {
        Ok(Some(log_key)) => {
            if !out_present.is_null() {
                *out_present = true;
            }
            if !out_key.is_null() && !out_key_len.is_null() {
                let (ptr, len) = alloc_bytes(&log_key.key);
                *out_key = ptr;
                *out_key_len = len;
            }
            success_result()
        }
        Ok(None) => {
            if !out_present.is_null() {
                *out_present = false;
            }
            success_result()
        }
        Err(e) => error_from_log_error(&e),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_key_iterator_close(
    iterator: *mut opendata_log_key_iterator_t,
) -> opendata_log_result_t {
    if iterator.is_null() {
        return error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            "iterator must not be null",
        );
    }
    let _ = Box::from_raw(iterator);
    success_result()
}
