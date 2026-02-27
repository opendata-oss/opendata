use std::ffi::CString;

use crate::ffi::{opendata_log_result_t, opendata_log_segment_t};

#[unsafe(no_mangle)]
pub extern "C" fn opendata_log_result_free(result: opendata_log_result_t) {
    if !result.message.is_null() {
        unsafe {
            let _ = CString::from_raw(result.message);
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_bytes_free(data: *mut u8, len: usize) {
    if !data.is_null() && len > 0 {
        let layout = std::alloc::Layout::from_size_align(len, 1).unwrap();
        std::alloc::dealloc(data, layout);
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_segments_free(
    segments: *mut opendata_log_segment_t,
    count: usize,
) {
    if !segments.is_null() && count > 0 {
        let _ = Vec::from_raw_parts(segments, count, count);
    }
}
