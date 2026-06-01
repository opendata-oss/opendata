//! Vector database benchmark library.
//!
//! Exposes the recall benchmark and ground-truth generation so they can be
//! shared between the `vector-bench` runner (`main.rs`) and auxiliary tools
//! such as the `gen_groundtruth` binary.

pub mod recall;
