//! C FFI bindings for opendata-log.
//!
//! # Safety
//!
//! All `extern "C"` functions in this crate follow the same safety contract:
//! pointer arguments must be valid (non-null and properly aligned) for the
//! duration of the call, and out-pointers must point to writable memory.
//! Each function validates its inputs and returns an error result on null
//! pointers rather than invoking undefined behavior.
#![allow(non_camel_case_types)]
#![allow(unsafe_op_in_unsafe_fn)]
#![allow(clippy::missing_safety_doc)]

pub mod ffi;

mod db;
mod iterator;
mod memory;
mod object_store;
mod reader;

// Re-export all public FFI types for cbindgen discovery.
pub use ffi::*;

// Re-export all extern "C" functions so they appear in the cdylib.
pub use db::*;
pub use iterator::*;
pub use memory::*;
pub use object_store::*;
pub use reader::*;
