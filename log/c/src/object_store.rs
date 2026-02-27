use std::ffi::c_char;

use common::storage::config::{AwsObjectStoreConfig, LocalObjectStoreConfig, ObjectStoreConfig};

use crate::ffi::{
    error_result, opendata_log_error_kind_t, opendata_log_object_store_t, opendata_log_result_t,
    require_out_ptr, success_result,
};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_object_store_in_memory(
    out_store: *mut *mut opendata_log_object_store_t,
) -> opendata_log_result_t {
    if let Err(e) = require_out_ptr(out_store, "out_store") {
        return e;
    }

    *out_store = Box::into_raw(Box::new(opendata_log_object_store_t {
        config: ObjectStoreConfig::InMemory,
    }));
    success_result()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_object_store_local(
    path: *const c_char,
    out_store: *mut *mut opendata_log_object_store_t,
) -> opendata_log_result_t {
    if let Err(e) = require_out_ptr(out_store, "out_store") {
        return e;
    }

    let path_str = match crate::ffi::cstr_to_string(path, "path") {
        Ok(s) => s,
        Err(e) => return e,
    };

    *out_store = Box::into_raw(Box::new(opendata_log_object_store_t {
        config: ObjectStoreConfig::Local(LocalObjectStoreConfig { path: path_str }),
    }));
    success_result()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_object_store_aws(
    region: *const c_char,
    bucket: *const c_char,
    out_store: *mut *mut opendata_log_object_store_t,
) -> opendata_log_result_t {
    if let Err(e) = require_out_ptr(out_store, "out_store") {
        return e;
    }

    let region_str = match crate::ffi::cstr_to_string(region, "region") {
        Ok(s) => s,
        Err(e) => return e,
    };
    let bucket_str = match crate::ffi::cstr_to_string(bucket, "bucket") {
        Ok(s) => s,
        Err(e) => return e,
    };

    *out_store = Box::into_raw(Box::new(opendata_log_object_store_t {
        config: ObjectStoreConfig::Aws(AwsObjectStoreConfig {
            region: region_str,
            bucket: bucket_str,
        }),
    }));
    success_result()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_object_store_close(
    store: *mut opendata_log_object_store_t,
) -> opendata_log_result_t {
    if store.is_null() {
        return error_result(
            opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
            "store must not be null",
        );
    }
    let _ = Box::from_raw(store);
    success_result()
}
