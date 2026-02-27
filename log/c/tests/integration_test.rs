use std::ffi::{CStr, CString};
use std::ptr;

use opendata_log_c::*;

fn assert_ok(result: opendata_log_result_t) {
    let kind = result.kind;
    if kind != opendata_log_error_kind_t::OPENDATA_LOG_OK {
        let message = if result.message.is_null() {
            String::from("(no message)")
        } else {
            unsafe {
                CStr::from_ptr(result.message)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        opendata_log_result_free(result);
        panic!("expected OK, got {kind:?}: {message}");
    }
    opendata_log_result_free(result);
}

fn assert_error(result: opendata_log_result_t, expected: opendata_log_error_kind_t) {
    let kind = result.kind;
    opendata_log_result_free(result);
    assert_eq!(kind, expected);
}

fn open_in_memory_log() -> *mut opendata_log_t {
    let config = opendata_log_config_t {
        storage_type: OPENDATA_LOG_STORAGE_IN_MEMORY,
        slatedb_path: ptr::null(),
        object_store: ptr::null(),
        settings_path: ptr::null(),
        seal_interval_ms: -1,
    };
    let mut log: *mut opendata_log_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_open(&config, &mut log) });
    assert!(!log.is_null());
    log
}

/// Appends `n` records to `key`, returning the start sequence.
unsafe fn append_records(log: *mut opendata_log_t, key: &[u8], n: usize) -> u64 {
    let keys: Vec<*const u8> = vec![key.as_ptr(); n];
    let key_lens: Vec<usize> = vec![key.len(); n];
    let values: Vec<Vec<u8>> = (0..n).map(|i| format!("value-{i}").into_bytes()).collect();
    let value_ptrs: Vec<*const u8> = values.iter().map(|v| v.as_ptr()).collect();
    let value_lens: Vec<usize> = values.iter().map(|v| v.len()).collect();

    let mut start_seq: u64 = 0;
    assert_ok(unsafe {
        opendata_log_try_append(
            log,
            keys.as_ptr(),
            key_lens.as_ptr(),
            value_ptrs.as_ptr(),
            value_lens.as_ptr(),
            n,
            &mut start_seq,
        )
    });
    start_seq
}

fn unbounded_seq_range() -> opendata_log_seq_range_t {
    opendata_log_seq_range_t {
        start: opendata_log_seq_bound_t {
            kind: OPENDATA_LOG_BOUND_UNBOUNDED,
            value: 0,
        },
        end: opendata_log_seq_bound_t {
            kind: OPENDATA_LOG_BOUND_UNBOUNDED,
            value: 0,
        },
    }
}

fn unbounded_segment_range() -> opendata_log_segment_range_t {
    opendata_log_segment_range_t {
        start: opendata_log_segment_bound_t {
            kind: OPENDATA_LOG_BOUND_UNBOUNDED,
            value: 0,
        },
        end: opendata_log_segment_bound_t {
            kind: OPENDATA_LOG_BOUND_UNBOUNDED,
            value: 0,
        },
    }
}

#[test]
fn open_and_close_in_memory() {
    let log = open_in_memory_log();
    assert_ok(unsafe { opendata_log_close(log) });
}

#[test]
fn open_rejects_null_config() {
    let mut log: *mut opendata_log_t = ptr::null_mut();
    assert_error(
        unsafe { opendata_log_open(ptr::null(), &mut log) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn open_rejects_null_out_pointer() {
    let config = opendata_log_config_t {
        storage_type: OPENDATA_LOG_STORAGE_IN_MEMORY,
        slatedb_path: ptr::null(),
        object_store: ptr::null(),
        settings_path: ptr::null(),
        seal_interval_ms: -1,
    };
    assert_error(
        unsafe { opendata_log_open(&config, ptr::null_mut()) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn close_rejects_null() {
    assert_error(
        unsafe { opendata_log_close(ptr::null_mut()) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn open_rejects_invalid_storage_type() {
    let config = opendata_log_config_t {
        storage_type: 99,
        slatedb_path: ptr::null(),
        object_store: ptr::null(),
        settings_path: ptr::null(),
        seal_interval_ms: -1,
    };
    let mut log: *mut opendata_log_t = ptr::null_mut();
    assert_error(
        unsafe { opendata_log_open(&config, &mut log) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn append_and_scan_round_trip() {
    let log = open_in_memory_log();
    let key = b"events";

    // Append 3 records
    let start_seq = unsafe { append_records(log, key, 3) };
    assert_ok(unsafe { opendata_log_flush(log) });

    // Scan all entries
    let range = unbounded_seq_range();
    let mut iter: *mut opendata_log_iterator_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_scan(log, key.as_ptr(), key.len(), &range, &mut iter) });

    let mut count = 0u64;
    loop {
        let mut present = false;
        let mut out_key: *mut u8 = ptr::null_mut();
        let mut out_key_len: usize = 0;
        let mut out_seq: u64 = 0;
        let mut out_val: *mut u8 = ptr::null_mut();
        let mut out_val_len: usize = 0;

        assert_ok(unsafe {
            opendata_log_iterator_next(
                iter,
                &mut present,
                &mut out_key,
                &mut out_key_len,
                &mut out_seq,
                &mut out_val,
                &mut out_val_len,
            )
        });

        if !present {
            break;
        }

        // Verify key matches
        let got_key = unsafe { std::slice::from_raw_parts(out_key, out_key_len) };
        assert_eq!(got_key, key);

        // Verify sequence is monotonically increasing from start
        assert_eq!(out_seq, start_seq + count);

        // Verify value
        let got_val = unsafe { std::slice::from_raw_parts(out_val, out_val_len) };
        let expected_val = format!("value-{count}");
        assert_eq!(got_val, expected_val.as_bytes());

        unsafe {
            opendata_log_bytes_free(out_key, out_key_len);
            opendata_log_bytes_free(out_val, out_val_len);
        }
        count += 1;
    }

    assert_eq!(count, 3);
    assert_ok(unsafe { opendata_log_iterator_close(iter) });
    assert_ok(unsafe { opendata_log_close(log) });
}

#[test]
fn append_empty_batch_succeeds() {
    let log = open_in_memory_log();

    let mut start_seq: u64 = 999;
    assert_ok(unsafe {
        opendata_log_try_append(
            log,
            ptr::null(),
            ptr::null(),
            ptr::null(),
            ptr::null(),
            0,
            &mut start_seq,
        )
    });
    // Empty batch returns start_sequence = 0
    assert_eq!(start_seq, 0);

    assert_ok(unsafe { opendata_log_close(log) });
}

#[test]
fn append_timeout_works() {
    let log = open_in_memory_log();
    let key = b"events";

    let keys: [*const u8; 1] = [key.as_ptr()];
    let key_lens: [usize; 1] = [key.len()];
    let value = b"hello";
    let values: [*const u8; 1] = [value.as_ptr()];
    let value_lens: [usize; 1] = [value.len()];
    let mut start_seq: u64 = 0;

    assert_ok(unsafe {
        opendata_log_append_timeout(
            log,
            keys.as_ptr(),
            key_lens.as_ptr(),
            values.as_ptr(),
            value_lens.as_ptr(),
            1,
            5000, // 5 second timeout
            &mut start_seq,
        )
    });

    assert_ok(unsafe { opendata_log_close(log) });
}

#[test]
fn scan_with_bounded_range() {
    let log = open_in_memory_log();
    let key = b"events";

    // Append 5 records (sequences 0..5)
    unsafe { append_records(log, key, 5) };
    assert_ok(unsafe { opendata_log_flush(log) });

    // Scan only sequences [1, 3) — should get 2 entries
    let range = opendata_log_seq_range_t {
        start: opendata_log_seq_bound_t {
            kind: OPENDATA_LOG_BOUND_INCLUDED,
            value: 1,
        },
        end: opendata_log_seq_bound_t {
            kind: OPENDATA_LOG_BOUND_EXCLUDED,
            value: 3,
        },
    };
    let mut iter: *mut opendata_log_iterator_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_scan(log, key.as_ptr(), key.len(), &range, &mut iter) });

    let mut count = 0;
    loop {
        let mut present = false;
        let mut out_key: *mut u8 = ptr::null_mut();
        let mut out_key_len: usize = 0;
        let mut out_seq: u64 = 0;
        let mut out_val: *mut u8 = ptr::null_mut();
        let mut out_val_len: usize = 0;

        assert_ok(unsafe {
            opendata_log_iterator_next(
                iter,
                &mut present,
                &mut out_key,
                &mut out_key_len,
                &mut out_seq,
                &mut out_val,
                &mut out_val_len,
            )
        });

        if !present {
            break;
        }

        assert!((1..3).contains(&out_seq), "seq {out_seq} out of range");
        unsafe {
            opendata_log_bytes_free(out_key, out_key_len);
            opendata_log_bytes_free(out_val, out_val_len);
        }
        count += 1;
    }

    assert_eq!(count, 2);
    assert_ok(unsafe { opendata_log_iterator_close(iter) });
    assert_ok(unsafe { opendata_log_close(log) });
}

#[test]
fn list_keys_returns_appended_keys() {
    let log = open_in_memory_log();

    // Append to two different keys
    unsafe { append_records(log, b"alpha", 1) };
    unsafe { append_records(log, b"beta", 1) };
    assert_ok(unsafe { opendata_log_flush(log) });

    let range = unbounded_segment_range();
    let mut iter: *mut opendata_log_key_iterator_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_list_keys(log, &range, &mut iter) });

    let mut keys: Vec<Vec<u8>> = Vec::new();
    loop {
        let mut present = false;
        let mut out_key: *mut u8 = ptr::null_mut();
        let mut out_key_len: usize = 0;

        assert_ok(unsafe {
            opendata_log_key_iterator_next(iter, &mut present, &mut out_key, &mut out_key_len)
        });

        if !present {
            break;
        }

        let k = unsafe { std::slice::from_raw_parts(out_key, out_key_len) }.to_vec();
        keys.push(k);
        unsafe { opendata_log_bytes_free(out_key, out_key_len) };
    }

    assert!(keys.contains(&b"alpha".to_vec()));
    assert!(keys.contains(&b"beta".to_vec()));

    assert_ok(unsafe { opendata_log_key_iterator_close(iter) });
    assert_ok(unsafe { opendata_log_close(log) });
}

#[test]
fn list_segments_returns_at_least_one() {
    let log = open_in_memory_log();
    unsafe { append_records(log, b"key", 1) };
    assert_ok(unsafe { opendata_log_flush(log) });

    let range = unbounded_seq_range();
    let mut segments: *mut opendata_log_segment_t = ptr::null_mut();
    let mut count: usize = 0;
    assert_ok(unsafe { opendata_log_list_segments(log, &range, &mut segments, &mut count) });

    assert!(count >= 1, "expected at least 1 segment, got {count}");
    unsafe { opendata_log_segments_free(segments, count) };
    assert_ok(unsafe { opendata_log_close(log) });
}

#[test]
fn object_store_in_memory_lifecycle() {
    let mut store: *mut opendata_log_object_store_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_object_store_in_memory(&mut store) });
    assert!(!store.is_null());
    assert_ok(unsafe { opendata_log_object_store_close(store) });
}

#[test]
fn object_store_local_lifecycle() {
    let path = CString::new("/tmp/opendata-log-c-test").unwrap();
    let mut store: *mut opendata_log_object_store_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_object_store_local(path.as_ptr(), &mut store) });
    assert!(!store.is_null());
    assert_ok(unsafe { opendata_log_object_store_close(store) });
}

#[test]
fn object_store_rejects_null_out_pointer() {
    assert_error(
        unsafe { opendata_log_object_store_in_memory(ptr::null_mut()) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn object_store_close_rejects_null() {
    assert_error(
        unsafe { opendata_log_object_store_close(ptr::null_mut()) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn iterator_close_rejects_null() {
    assert_error(
        unsafe { opendata_log_iterator_close(ptr::null_mut()) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn key_iterator_close_rejects_null() {
    assert_error(
        unsafe { opendata_log_key_iterator_close(ptr::null_mut()) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn result_free_handles_null_message() {
    let result = opendata_log_result_t {
        kind: opendata_log_error_kind_t::OPENDATA_LOG_OK,
        message: ptr::null_mut(),
    };
    // Should not crash
    opendata_log_result_free(result);
}

#[test]
fn reader_open_and_close() {
    let dir = tempfile::tempdir().unwrap();
    let os_path = CString::new(dir.path().to_str().unwrap()).unwrap();
    let db_path = CString::new("test-log").unwrap();

    let mut store: *mut opendata_log_object_store_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_object_store_local(os_path.as_ptr(), &mut store) });

    // Write some data and close
    let writer_config = opendata_log_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        seal_interval_ms: -1,
    };
    let mut log: *mut opendata_log_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_open(&writer_config, &mut log) });
    unsafe { append_records(log, b"key", 1) };
    assert_ok(unsafe { opendata_log_flush(log) });
    assert_ok(unsafe { opendata_log_close(log) });

    // Open reader against same path
    let reader_config = opendata_log_reader_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        refresh_interval_ms: -1,
    };
    let mut reader: *mut opendata_log_reader_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_reader_open(&reader_config, &mut reader) });
    assert!(!reader.is_null());
    assert_ok(unsafe { opendata_log_reader_close(reader) });
    assert_ok(unsafe { opendata_log_object_store_close(store) });
}

#[test]
fn reader_scan_returns_written_data() {
    let dir = tempfile::tempdir().unwrap();
    let os_path = CString::new(dir.path().to_str().unwrap()).unwrap();
    let db_path = CString::new("test-log").unwrap();

    let mut store: *mut opendata_log_object_store_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_object_store_local(os_path.as_ptr(), &mut store) });

    // Write data
    let writer_config = opendata_log_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        seal_interval_ms: -1,
    };
    let mut log: *mut opendata_log_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_open(&writer_config, &mut log) });
    unsafe { append_records(log, b"events", 3) };
    assert_ok(unsafe { opendata_log_flush(log) });
    assert_ok(unsafe { opendata_log_close(log) });

    // Read via reader
    let reader_config = opendata_log_reader_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        refresh_interval_ms: -1,
    };
    let mut reader: *mut opendata_log_reader_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_reader_open(&reader_config, &mut reader) });

    let range = unbounded_seq_range();
    let key = b"events";
    let mut iter: *mut opendata_log_iterator_t = ptr::null_mut();
    assert_ok(unsafe {
        opendata_log_reader_scan(reader, key.as_ptr(), key.len(), &range, &mut iter)
    });

    let mut count = 0u64;
    loop {
        let mut present = false;
        let mut out_key: *mut u8 = ptr::null_mut();
        let mut out_key_len: usize = 0;
        let mut out_seq: u64 = 0;
        let mut out_val: *mut u8 = ptr::null_mut();
        let mut out_val_len: usize = 0;

        assert_ok(unsafe {
            opendata_log_iterator_next(
                iter,
                &mut present,
                &mut out_key,
                &mut out_key_len,
                &mut out_seq,
                &mut out_val,
                &mut out_val_len,
            )
        });

        if !present {
            break;
        }

        let got_key = unsafe { std::slice::from_raw_parts(out_key, out_key_len) };
        assert_eq!(got_key, key);

        let got_val = unsafe { std::slice::from_raw_parts(out_val, out_val_len) };
        let expected_val = format!("value-{count}");
        assert_eq!(got_val, expected_val.as_bytes());

        unsafe {
            opendata_log_bytes_free(out_key, out_key_len);
            opendata_log_bytes_free(out_val, out_val_len);
        }
        count += 1;
    }

    assert_eq!(count, 3);
    assert_ok(unsafe { opendata_log_iterator_close(iter) });
    assert_ok(unsafe { opendata_log_reader_close(reader) });
    assert_ok(unsafe { opendata_log_object_store_close(store) });
}

#[test]
fn reader_list_keys() {
    let dir = tempfile::tempdir().unwrap();
    let os_path = CString::new(dir.path().to_str().unwrap()).unwrap();
    let db_path = CString::new("test-log").unwrap();

    let mut store: *mut opendata_log_object_store_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_object_store_local(os_path.as_ptr(), &mut store) });

    // Write to two keys
    let writer_config = opendata_log_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        seal_interval_ms: -1,
    };
    let mut log: *mut opendata_log_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_open(&writer_config, &mut log) });
    unsafe { append_records(log, b"alpha", 1) };
    unsafe { append_records(log, b"beta", 1) };
    assert_ok(unsafe { opendata_log_flush(log) });
    assert_ok(unsafe { opendata_log_close(log) });

    // List keys via reader
    let reader_config = opendata_log_reader_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        refresh_interval_ms: -1,
    };
    let mut reader: *mut opendata_log_reader_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_reader_open(&reader_config, &mut reader) });

    let range = unbounded_segment_range();
    let mut iter: *mut opendata_log_key_iterator_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_reader_list_keys(reader, &range, &mut iter) });

    let mut keys: Vec<Vec<u8>> = Vec::new();
    loop {
        let mut present = false;
        let mut out_key: *mut u8 = ptr::null_mut();
        let mut out_key_len: usize = 0;

        assert_ok(unsafe {
            opendata_log_key_iterator_next(iter, &mut present, &mut out_key, &mut out_key_len)
        });

        if !present {
            break;
        }

        let k = unsafe { std::slice::from_raw_parts(out_key, out_key_len) }.to_vec();
        keys.push(k);
        unsafe { opendata_log_bytes_free(out_key, out_key_len) };
    }

    assert!(keys.contains(&b"alpha".to_vec()));
    assert!(keys.contains(&b"beta".to_vec()));

    assert_ok(unsafe { opendata_log_key_iterator_close(iter) });
    assert_ok(unsafe { opendata_log_reader_close(reader) });
    assert_ok(unsafe { opendata_log_object_store_close(store) });
}

#[test]
fn reader_list_segments() {
    let dir = tempfile::tempdir().unwrap();
    let os_path = CString::new(dir.path().to_str().unwrap()).unwrap();
    let db_path = CString::new("test-log").unwrap();

    let mut store: *mut opendata_log_object_store_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_object_store_local(os_path.as_ptr(), &mut store) });

    // Write data
    let writer_config = opendata_log_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        seal_interval_ms: -1,
    };
    let mut log: *mut opendata_log_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_open(&writer_config, &mut log) });
    unsafe { append_records(log, b"key", 1) };
    assert_ok(unsafe { opendata_log_flush(log) });
    assert_ok(unsafe { opendata_log_close(log) });

    // List segments via reader
    let reader_config = opendata_log_reader_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        refresh_interval_ms: -1,
    };
    let mut reader: *mut opendata_log_reader_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_reader_open(&reader_config, &mut reader) });

    let range = unbounded_seq_range();
    let mut segments: *mut opendata_log_segment_t = ptr::null_mut();
    let mut count: usize = 0;
    assert_ok(unsafe {
        opendata_log_reader_list_segments(reader, &range, &mut segments, &mut count)
    });

    assert!(count >= 1, "expected at least 1 segment, got {count}");
    unsafe { opendata_log_segments_free(segments, count) };
    assert_ok(unsafe { opendata_log_reader_close(reader) });
    assert_ok(unsafe { opendata_log_object_store_close(store) });
}

#[test]
fn reader_open_rejects_null_config() {
    let mut reader: *mut opendata_log_reader_t = ptr::null_mut();
    assert_error(
        unsafe { opendata_log_reader_open(ptr::null(), &mut reader) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn reader_open_rejects_null_out_pointer() {
    let reader_config = opendata_log_reader_config_t {
        storage_type: OPENDATA_LOG_STORAGE_IN_MEMORY,
        slatedb_path: ptr::null(),
        object_store: ptr::null(),
        settings_path: ptr::null(),
        refresh_interval_ms: -1,
    };
    assert_error(
        unsafe { opendata_log_reader_open(&reader_config, ptr::null_mut()) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn reader_close_rejects_null() {
    assert_error(
        unsafe { opendata_log_reader_close(ptr::null_mut()) },
        opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
    );
}

#[test]
fn open_with_local_slatedb() {
    let dir = tempfile::tempdir().unwrap();
    let os_path = CString::new(dir.path().to_str().unwrap()).unwrap();
    let db_path = CString::new("test-log").unwrap();

    let mut store: *mut opendata_log_object_store_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_object_store_local(os_path.as_ptr(), &mut store) });

    let config = opendata_log_config_t {
        storage_type: OPENDATA_LOG_STORAGE_SLATEDB,
        slatedb_path: db_path.as_ptr(),
        object_store: store,
        settings_path: ptr::null(),
        seal_interval_ms: -1,
    };
    let mut log: *mut opendata_log_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_open(&config, &mut log) });

    // Write and read back
    let key = b"persistent";
    let start_seq = unsafe { append_records(log, key, 1) };
    assert_ok(unsafe { opendata_log_flush(log) });

    let range = unbounded_seq_range();
    let mut iter: *mut opendata_log_iterator_t = ptr::null_mut();
    assert_ok(unsafe { opendata_log_scan(log, key.as_ptr(), key.len(), &range, &mut iter) });

    let mut present = false;
    let mut out_key: *mut u8 = ptr::null_mut();
    let mut out_key_len: usize = 0;
    let mut out_seq: u64 = 0;
    let mut out_val: *mut u8 = ptr::null_mut();
    let mut out_val_len: usize = 0;

    assert_ok(unsafe {
        opendata_log_iterator_next(
            iter,
            &mut present,
            &mut out_key,
            &mut out_key_len,
            &mut out_seq,
            &mut out_val,
            &mut out_val_len,
        )
    });
    assert!(present);
    assert_eq!(out_seq, start_seq);

    unsafe {
        opendata_log_bytes_free(out_key, out_key_len);
        opendata_log_bytes_free(out_val, out_val_len);
    }

    assert_ok(unsafe { opendata_log_iterator_close(iter) });
    assert_ok(unsafe { opendata_log_close(log) });
    assert_ok(unsafe { opendata_log_object_store_close(store) });
}
