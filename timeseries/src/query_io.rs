//! Re-exports the per-query I/O collector from common.
//!
//! The collector implementation lives in `common::storage::query_io` so that
//! both the timeseries storage layer and the object-store wrapper (in common)
//! can record into the same task-local collector.

pub(crate) use common::storage::query_io::*;
