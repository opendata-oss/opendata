//! Fixed-pool read-session driver.
//!
//! `active_sessions` worker tasks, closed-loop. Worker `w` owns the stride-shard
//! of keys `{w, w+P, w+2P, …}` (`P = active_sessions`), so a key is only ever
//! polled by one worker — its persistent cursor needs no claim or hand-off lock.
//!
//! Each worker runs a sequence of duration-bounded **sessions**: bind to the next
//! key in its shard, poll it from the stored cursor until `session_duration`
//! elapses (or forever, within the measure window), then move to the next key.
//! `session_duration` is the churn knob:
//!
//! - long / forever ⇒ a *consistent* set of readers gluing to few keys (deep
//!   per-key consumption);
//! - short ⇒ the active set sweeps the population, touching many keys briefly and
//!   resuming each from its stored cursor on the next visit.
//!
//! Concurrency (occupancy) is exactly `active_sessions` — the capacity surface's
//! independent variable — with no arrival schedule and no emergent occupancy.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::ReadState;
use crate::read::lag::lag_bucket;
use crate::read::store::Cursor;

/// Run one worker until cancelled: cycle through its shard, one session per key.
pub async fn run_worker(
    worker_idx: usize,
    num_workers: usize,
    state: Arc<ReadState>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let n = state.keys.len();
    let my_keys: Vec<usize> = (worker_idx..n).step_by(num_workers).collect();
    if my_keys.is_empty() {
        // More workers than keys: nothing to poll, just wait for shutdown.
        cancel.cancelled().await;
        return Ok(());
    }

    let mut next = 0usize;
    while !cancel.is_cancelled() {
        let id = my_keys[next % my_keys.len()];
        next += 1;
        run_session(id, &state, &cancel).await?;
    }
    Ok(())
}

/// Poll one key from its persistent cursor until the session deadline (or cancel).
async fn run_session(
    id: usize,
    state: &ReadState,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let key = state.keys[id].clone();
    let mut cursor = Cursor(state.cursors[id].load(Ordering::Relaxed));
    // `None` = poll forever (within the measure window); `Some(t)` = stop at/after t.
    let deadline = state.session_duration.map(|d| Instant::now() + d);

    let mut polled_any = false;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        if deadline.is_some_and(|dl| Instant::now() >= dl) {
            break;
        }
        one_poll(id, &key, state, &mut cursor).await?;
        polled_any = true;

        if !state.poll_interval.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(state.poll_interval) => {}
                _ = cancel.cancelled() => break,
            }
        }
    }

    if polled_any && state.recording() {
        state.sessions_completed.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

/// Issue one poll, advance the persistent cursor, ack the drained records, and
/// (when recording) tally per-poll metrics bucketed by lag at poll time.
async fn one_poll(
    id: usize,
    key: &Bytes,
    state: &ReadState,
    cursor: &mut Cursor,
) -> anyhow::Result<()> {
    // Lag at poll time buckets the per-poll cost (RFC 0006's read-cost axis),
    // captured before acking the records this poll drains.
    let poll_bucket = lag_bucket(state.lag.lag(id));

    let svc_start = Instant::now();
    let out = state
        .store
        .poll(key.clone(), *cursor, state.page_size)
        .await?;
    let svc = svc_start.elapsed();

    *cursor = out.cursor;
    state.cursors[id].store(out.cursor.0, Ordering::Relaxed);
    state.lag.record_acked(id, out.n_records as u64);

    if state.recording() {
        state.polls_completed.fetch_add(1, Ordering::Relaxed);
        state.bucket_polls[poll_bucket].fetch_add(1, Ordering::Relaxed);
        state
            .records_consumed
            .fetch_add(out.n_records as u64, Ordering::Relaxed);
        state
            .bytes_consumed
            .fetch_add(out.n_bytes as u64, Ordering::Relaxed);
        // First measure-phase poll on this key counts toward distinct logs touched.
        if !state.touched[id].swap(true, Ordering::Relaxed) {
            state.distinct_keys_touched.fetch_add(1, Ordering::Relaxed);
        }
        state
            .metrics
            .records_consumed
            .increment(out.n_records as u64);
        state.metrics.polls.increment(1);
        let svc_us = svc.as_micros() as f64;
        state.metrics.poll_service_us[poll_bucket].record(svc_us);
        state.service_us[poll_bucket]
            .lock()
            .expect("service latency mutex")
            .add(svc_us);
    }
    Ok(())
}
