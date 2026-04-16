//! Per-query memory accounting for the v2 execution engine.
//!
//! Defines:
//! - [`MemoryReservation`]: atomic reserved-bytes counter enforced against a
//!   configurable per-query cap. All allocating calls in the v2 engine must
//!   route through [`MemoryReservation::try_grow`] before allocating and call
//!   [`MemoryReservation::release`] when the buffer is dropped.
//! - [`QueryError`]: v2-local error type carrying structured diagnostics for
//!   memory-limit and cardinality-gate rejections. Kept isolated from the
//!   crate-level [`crate::error::QueryError`] for phase 1; integration with
//!   the outer error surface happens in the phase-5 wiring unit.
//!
//! See RFC 0007 §"Execution Model" (subsection "Memory accounting") and
//! §"Error Handling & Observability".

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Structured error type for the v2 engine.
///
/// Isolated from [`crate::error::QueryError`] so phase 1 doesn't perturb the
/// crate-wide error surface. The phase-5 wiring unit decides how to bridge
/// the two (likely an `impl From<v2::QueryError> for crate::error::QueryError`).
#[derive(Debug, Clone, thiserror::Error)]
pub enum QueryError {
    /// The query exceeded its per-query memory cap while attempting to grow
    /// a reservation. Carries enough context for operator tracing and user
    /// diagnostics.
    #[error(
        "memory limit exceeded: requested {requested} bytes, already reserved \
         {already_reserved} bytes, cap {cap} bytes"
    )]
    MemoryLimit {
        /// Bytes the caller asked to reserve on the `try_grow` call that
        /// failed.
        requested: usize,
        /// Configured per-query cap (bytes).
        cap: usize,
        /// Bytes already reserved at the moment of the failing call
        /// (does not include `requested`).
        already_reserved: usize,
    },

    /// The planner's cardinality gate rejected the query before allocation.
    /// Populated by the phase-4 planner; defined here to keep the error type
    /// stable across units.
    #[error(
        "query too large: estimated {estimated_cells} cells exceeds limit \
         {cell_limit} (series × steps)"
    )]
    TooLarge {
        /// `sum(series × steps)` across selectors.
        estimated_cells: u64,
        /// Configured limit the estimate exceeded.
        cell_limit: u64,
    },

    /// Generic internal / storage-surface error. Used by the Phase 2
    /// storage adapter to carry crate-level `Error` messages without
    /// extending the public v2 surface with a storage-specific variant.
    /// The Phase 5 wiring unit can retire this in favour of a more
    /// structured shape (e.g. distinct `Storage` / `InvalidInput`
    /// variants) once the bridging into `crate::error::QueryError` is
    /// defined.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Per-query memory reservation.
///
/// Tracks bytes reserved against a configurable per-query `cap`. All counters
/// are `AtomicUsize` so the reservation can be cloned into tokio tasks behind
/// a `Concurrent`/`Coalesce` wrapper without locks. Clones share state via
/// an internal `Arc` — dropping one clone does *not* release its reservation;
/// callers must pair [`try_grow`](Self::try_grow) with
/// [`release`](Self::release).
///
/// See RFC 0007 §"Execution Model" (memory accounting).
#[derive(Debug, Clone)]
pub struct MemoryReservation {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    cap: usize,
    reserved: AtomicUsize,
    high_water: AtomicUsize,
}

impl MemoryReservation {
    /// Create a reservation with the given per-query cap in bytes.
    ///
    /// A `cap` of zero is legal and causes every non-zero `try_grow` to
    /// fail; zero-byte reservations always succeed (they never allocate).
    pub fn new(cap: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                cap,
                reserved: AtomicUsize::new(0),
                high_water: AtomicUsize::new(0),
            }),
        }
    }

    /// Configured per-query cap, in bytes.
    #[inline]
    pub fn cap(&self) -> usize {
        self.inner.cap
    }

    /// Currently reserved bytes. Snapshot; concurrent `try_grow`/`release`
    /// calls may observe a different value on the next load.
    #[inline]
    pub fn reserved(&self) -> usize {
        self.inner.reserved.load(Ordering::Acquire)
    }

    /// High-water mark: the largest value [`reserved`](Self::reserved) has
    /// ever reached for this reservation. Monotonically non-decreasing — in
    /// particular, [`release`](Self::release) does not shrink the high-water
    /// mark. Used for the per-query tracing event (RFC §"Error Handling &
    /// Observability").
    #[inline]
    pub fn high_water(&self) -> usize {
        self.inner.high_water.load(Ordering::Acquire)
    }

    /// Attempt to reserve `bytes`. On success, returns `Ok(())` and the
    /// reserved counter is incremented atomically; on failure, returns
    /// [`QueryError::MemoryLimit`] carrying the requested amount, the cap,
    /// and the reservation observed at the moment of rejection.
    ///
    /// Zero-byte requests always succeed without touching state.
    ///
    /// Concurrency: implemented as a CAS loop on the reserved counter so
    /// multiple tokio tasks may call `try_grow` simultaneously; at most one
    /// over-cap request is accepted for a given state transition.
    pub fn try_grow(&self, bytes: usize) -> Result<(), QueryError> {
        if bytes == 0 {
            return Ok(());
        }

        let cap = self.inner.cap;
        let mut current = self.inner.reserved.load(Ordering::Acquire);
        loop {
            let new = match current.checked_add(bytes) {
                Some(n) if n <= cap => n,
                _ => {
                    return Err(QueryError::MemoryLimit {
                        requested: bytes,
                        cap,
                        already_reserved: current,
                    });
                }
            };
            match self.inner.reserved.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.bump_high_water(new);
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Release `bytes` previously reserved via [`try_grow`](Self::try_grow).
    ///
    /// In debug builds, underflow (releasing more than is currently
    /// reserved) panics; in release builds it saturates at zero so a
    /// miscounted operator cannot take down the process. The high-water
    /// mark is not affected.
    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }

        let mut current = self.inner.reserved.load(Ordering::Acquire);
        loop {
            debug_assert!(
                current >= bytes,
                "MemoryReservation::release underflow: releasing {bytes} bytes \
                 but only {current} reserved",
            );
            let new = current.saturating_sub(bytes);
            match self.inner.reserved.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Raise the high-water mark if `candidate` exceeds the current value.
    fn bump_high_water(&self, candidate: usize) {
        let mut current = self.inner.high_water.load(Ordering::Relaxed);
        while candidate > current {
            match self.inner.high_water.compare_exchange_weak(
                current,
                candidate,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn should_accept_grow_within_cap() {
        // given: a reservation with a 1 KiB cap
        let res = MemoryReservation::new(1024);

        // when
        res.try_grow(256).expect("first grow fits");
        res.try_grow(512).expect("second grow fits");

        // then
        assert_eq!(res.reserved(), 768);
        assert_eq!(res.cap(), 1024);
    }

    #[test]
    fn should_reject_grow_exceeding_cap() {
        // given: 100-byte cap with 40 already reserved
        let res = MemoryReservation::new(100);
        res.try_grow(40).unwrap();

        // when: ask for 80 more (total would be 120 > 100)
        let err = res.try_grow(80).unwrap_err();

        // then: error carries diagnostic fields; state is unchanged
        match err {
            QueryError::MemoryLimit {
                requested,
                cap,
                already_reserved,
            } => {
                assert_eq!(requested, 80);
                assert_eq!(cap, 100);
                assert_eq!(already_reserved, 40);
            }
            other => panic!("expected MemoryLimit, got {other:?}"),
        }
        assert_eq!(res.reserved(), 40);
    }

    #[test]
    fn should_release_previously_reserved_bytes() {
        // given
        let res = MemoryReservation::new(1024);
        res.try_grow(300).unwrap();
        res.try_grow(200).unwrap();
        assert_eq!(res.reserved(), 500);

        // when
        res.release(300);

        // then
        assert_eq!(res.reserved(), 200);
    }

    #[test]
    fn should_allow_regrow_after_release() {
        // given: cap fully reserved
        let res = MemoryReservation::new(512);
        res.try_grow(512).unwrap();
        assert!(res.try_grow(1).is_err());

        // when: release half, then regrow into the freed space
        res.release(256);
        res.try_grow(200).expect("regrow fits after release");

        // then
        assert_eq!(res.reserved(), 456);
    }

    #[test]
    fn should_track_high_water_mark() {
        // given
        let res = MemoryReservation::new(1024);

        // when: climb to 800, then release back to 100
        res.try_grow(500).unwrap();
        res.try_grow(300).unwrap();
        assert_eq!(res.reserved(), 800);
        assert_eq!(res.high_water(), 800);

        res.release(700);
        assert_eq!(res.reserved(), 100);

        // then: high-water does not decrease on release
        assert_eq!(res.high_water(), 800);

        // when: grow again below the previous peak
        res.try_grow(200).unwrap();

        // then: still 800
        assert_eq!(res.high_water(), 800);

        // when: grow past the peak
        res.try_grow(500).unwrap();

        // then: high-water tracks the new maximum
        assert_eq!(res.reserved(), 800);
        assert_eq!(res.high_water(), 800);

        res.try_grow(100).unwrap();
        assert_eq!(res.high_water(), 900);
    }

    #[test]
    fn should_handle_concurrent_grows() {
        // given: cap of N * per_thread bytes so all grows must fit
        const THREADS: usize = 16;
        const PER_THREAD: usize = 64;
        let res = MemoryReservation::new(THREADS * PER_THREAD);
        let successes = Arc::new(AtomicUsize::new(0));

        // when: THREADS tasks race to reserve PER_THREAD bytes each
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let res = res.clone();
            let successes = Arc::clone(&successes);
            handles.push(thread::spawn(move || {
                if res.try_grow(PER_THREAD).is_ok() {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // then: every grow fits; total equals sum of parts
        assert_eq!(successes.load(Ordering::Relaxed), THREADS);
        assert_eq!(res.reserved(), THREADS * PER_THREAD);
        assert_eq!(res.high_water(), THREADS * PER_THREAD);
    }

    #[test]
    fn should_fail_cleanly_when_concurrent_grows_exceed_cap() {
        // given: cap holds exactly HALF of the contenders
        const THREADS: usize = 16;
        const PER_THREAD: usize = 64;
        let cap = (THREADS / 2) * PER_THREAD;
        let res = MemoryReservation::new(cap);
        let successes = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));

        // when: all THREADS tasks race
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let res = res.clone();
            let successes = Arc::clone(&successes);
            let failures = Arc::clone(&failures);
            handles.push(thread::spawn(move || match res.try_grow(PER_THREAD) {
                Ok(()) => {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
                Err(QueryError::MemoryLimit { .. }) => {
                    failures.fetch_add(1, Ordering::Relaxed);
                }
                Err(other) => panic!("unexpected error variant: {other:?}"),
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // then: exactly cap-worth of grows succeeded, rest failed cleanly
        let ok = successes.load(Ordering::Relaxed);
        let fail = failures.load(Ordering::Relaxed);
        assert_eq!(ok + fail, THREADS);
        assert_eq!(ok, THREADS / 2);
        assert_eq!(fail, THREADS / 2);
        assert_eq!(res.reserved(), cap);
    }

    #[test]
    fn should_reject_zero_cap_grow() {
        // given: a zero-cap reservation
        let res = MemoryReservation::new(0);

        // when: non-zero grow
        let err = res.try_grow(1).unwrap_err();

        // then
        match err {
            QueryError::MemoryLimit {
                requested,
                cap,
                already_reserved,
            } => {
                assert_eq!(requested, 1);
                assert_eq!(cap, 0);
                assert_eq!(already_reserved, 0);
            }
            other => panic!("expected MemoryLimit, got {other:?}"),
        }

        // and: zero-byte grow still succeeds on a zero cap (no-op)
        res.try_grow(0).expect("zero-byte grow is always ok");
        assert_eq!(res.reserved(), 0);
    }

    #[test]
    fn should_treat_zero_byte_grow_as_noop() {
        // given
        let res = MemoryReservation::new(64);
        res.try_grow(32).unwrap();

        // when
        res.try_grow(0).unwrap();
        res.release(0);

        // then: state untouched
        assert_eq!(res.reserved(), 32);
        assert_eq!(res.high_water(), 32);
    }

    #[test]
    fn should_reject_grow_when_addition_would_overflow() {
        // given: cap at usize::MAX with a near-max reservation
        let res = MemoryReservation::new(usize::MAX);
        res.try_grow(usize::MAX - 10).unwrap();

        // when: request that would overflow usize
        let err = res.try_grow(usize::MAX).unwrap_err();

        // then: reported as MemoryLimit, not a panic
        match err {
            QueryError::MemoryLimit { requested, cap, .. } => {
                assert_eq!(requested, usize::MAX);
                assert_eq!(cap, usize::MAX);
            }
            other => panic!("expected MemoryLimit, got {other:?}"),
        }
    }

    #[test]
    fn should_share_state_between_clones() {
        // given: clone before any grow
        let a = MemoryReservation::new(1024);
        let b = a.clone();

        // when: grow through one clone, observe through the other
        b.try_grow(400).unwrap();

        // then
        assert_eq!(a.reserved(), 400);
        assert_eq!(b.reserved(), 400);
        assert_eq!(a.high_water(), 400);

        // when: release through the other clone
        a.release(150);

        // then
        assert_eq!(b.reserved(), 250);
    }

    #[test]
    fn should_expose_too_large_variant() {
        // given: phase-4 planner surface — defined here so the type is stable
        let err = QueryError::TooLarge {
            estimated_cells: 1_000_000,
            cell_limit: 100_000,
        };

        // when
        let msg = format!("{err}");

        // then
        assert!(msg.contains("1000000"));
        assert!(msg.contains("100000"));
    }
}
