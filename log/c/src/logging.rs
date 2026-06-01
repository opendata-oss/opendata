use std::ffi::c_char;

use tracing_subscriber::EnvFilter;

use crate::ffi::*;

/// Installs a global `tracing` subscriber that writes to stderr.
///
/// `filter` is an `EnvFilter` directive string (same syntax as `RUST_LOG`),
/// e.g. `"info"`, `"slatedb=debug"`, or `"slatedb=debug,opendata_log=info"`.
/// Pass `NULL` to fall back to the `RUST_LOG` environment variable, or
/// `"info"` if `RUST_LOG` is unset.
///
/// Must be called at most once per process, before `opendata_log_open`. The
/// underlying `tracing` global default can only be set once; if a subscriber
/// is already installed (e.g. by another Rust component embedded in the same
/// host), this returns `OPENDATA_LOG_ERROR_INTERNAL`.
///
/// Hosts that already manage their own `tracing` subscriber should not call
/// this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn opendata_log_enable_logging(
    filter: *const c_char,
) -> opendata_log_result_t {
    let directive = if filter.is_null() {
        None
    } else {
        match cstr_to_string(filter, "filter") {
            Ok(s) => Some(s),
            Err(e) => return e,
        }
    };

    let env_filter = match directive {
        Some(s) => match EnvFilter::try_new(&s) {
            Ok(f) => f,
            Err(e) => {
                return error_result(
                    opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INVALID_INPUT,
                    &format!("invalid filter directive {s:?}: {e}"),
                );
            }
        },
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .try_init()
        .map(|()| success_result())
        .unwrap_or_else(|e| {
            error_result(
                opendata_log_error_kind_t::OPENDATA_LOG_ERROR_INTERNAL,
                &format!("failed to install tracing subscriber: {e}"),
            )
        })
}
