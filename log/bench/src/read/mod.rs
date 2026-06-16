//! Shared read-path primitives used by the read-oriented benchmarks.
//!
//! - [`store`] — the narrow `LogStore` interface (cursor, page-bounded drain) and
//!   its LogDb implementation, served either from the writer's handle or a
//!   standalone [`LogDbReader`](log::LogDbReader).
//! - [`lag`] — per-key appended-minus-acknowledged tracking and the lag buckets
//!   the per-poll cost metrics are reported against (RFC 0006).
//!
//! Both the `follow` benchmark (open-loop Poisson sessions over a live arrival
//! stream) and the `burst_read` benchmark (a fixed pool of sessions over a
//! static, burst-written corpus) read against these.

pub mod lag;
pub mod store;
