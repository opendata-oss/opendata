//! A single follow **session**.
//!
//! Bare bones: a session models one reader waking for one key, polling it on a
//! cursor that persists across sessions (a mailbox remembers where the user left
//! off), and closing when its time is up. The loop is just
//!
//! ```text
//! while within session_duration: poll; wait poll_interval
//! ```
//!
//! There is no catch-up / tail detection — a poll drains up to `page_size` and the
//! session simply keeps polling until `session_duration` elapses (or forever, for
//! continuous following). Behavior beyond this (end-on-catch-up, backoff while
//! idle, deep-catch-up handling) is intentionally left out, to be built back up
//! as the benchmark needs it.
//!
//! Open-loop session *arrivals* (the population waking up) live in
//! [`super::generator`]; this module is what one arrival executes once it holds a
//! pool slot.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::FollowState;
use super::lag::lag_bucket;
use super::store::Cursor;

/// Execute one session: acquire a pool slot (queueing here is session scheduling
/// lag), poll the key until the session duration elapses, then release the slot.
/// `scheduled_due` is the open-loop arrival time the generator assigned, used to
/// measure scheduling lag.
pub async fn run_one_session(
    id: usize,
    scheduled_due: Instant,
    state: Arc<FollowState>,
    semaphore: Arc<Semaphore>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // Acquire a pool slot. The wait here — scheduled arrival to slot acquisition —
    // is the session's scheduling lag; it stays ~0 while the pool has headroom and
    // diverges once offered load exceeds `max_active_sessions`.
    let permit: OwnedSemaphorePermit = tokio::select! {
        p = semaphore.acquire_owned() => p.expect("session semaphore is never closed"),
        _ = cancel.cancelled() => return Ok(()),
    };
    let acquired = Instant::now();
    let sched_lag = acquired.saturating_duration_since(scheduled_due);

    // Occupancy: count this session as active for the pool-occupancy (Little's-law)
    // measurement. `record` is latched at start so a session contributes wholly or
    // not at all to the measure window.
    let active = state.active_sessions.fetch_add(1, Ordering::Relaxed) + 1;
    state.peak_active.fetch_max(active, Ordering::Relaxed);
    let record = state.recording();

    let result = poll_for_duration(id, &state, record, &cancel, sched_lag, acquired).await;

    state.active_sessions.fetch_sub(1, Ordering::Relaxed);
    drop(permit);
    result
}

/// Poll the key until the session deadline (or cancel), then record the session.
async fn poll_for_duration(
    id: usize,
    state: &FollowState,
    record: bool,
    cancel: &CancellationToken,
    sched_lag: Duration,
    acquired: Instant,
) -> anyhow::Result<()> {
    let key = state.keys[id].clone();
    // Backlog the session woke with is the analysis axis for per-session goodput.
    let bucket = lag_bucket(state.lag.lag(id));
    let mut cursor = Cursor(state.cursors[id].load(Ordering::Relaxed));
    // `None` = poll forever (continuous following); `Some(t)` = stop at/after `t`.
    let deadline = state.session_duration.map(|d| acquired + d);

    if record {
        let us = sched_lag.as_micros() as f64;
        state
            .session_sched_lag_us
            .lock()
            .expect("sched lag mutex")
            .add(us);
        state.metrics.session_sched_lag_us.record(us);
    }

    let mut drained_records = 0u64;
    let mut db_service = Duration::ZERO;
    let mut polls = 0u64;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        if deadline.is_some_and(|dl| Instant::now() >= dl) {
            break;
        }

        let (n_records, svc) = one_poll(id, &key, state, &mut cursor, record).await?;
        drained_records += n_records as u64;
        db_service += svc;
        polls += 1;

        if !state.poll_interval.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(state.poll_interval) => {}
                _ = cancel.cancelled() => break,
            }
        }
    }

    if record {
        record_session(
            state,
            bucket,
            drained_records,
            db_service,
            acquired.elapsed(),
            polls,
        );
    }
    Ok(())
}

/// Record one session's progress metrics into the per-backlog-bucket tallies.
fn record_session(
    state: &FollowState,
    bucket: usize,
    drained_records: u64,
    db_service: Duration,
    total_active: Duration,
    polls: u64,
) {
    state.metrics.sessions.increment(1);
    state.sessions_completed.fetch_add(1, Ordering::Relaxed);
    state.bucket_sessions[bucket].fetch_add(1, Ordering::Relaxed);
    state.bucket_session_polls[bucket].fetch_add(polls, Ordering::Relaxed);
    // Occupancy is the time-integral of active sessions over the window; sum each
    // session's full active duration and divide by elapsed later.
    state
        .active_time_us
        .fetch_add(total_active.as_micros() as u64, Ordering::Relaxed);

    // Per-session goodput over DB-poll time (records/s) — the contention signal: as
    // sessions contend, each poll takes longer and this drops. Only meaningful when
    // the session actually drained data.
    if drained_records > 0 {
        let db_secs = db_service.as_secs_f64();
        if db_secs > 0.0 {
            state.db_rate[bucket]
                .lock()
                .expect("db rate mutex")
                .add(drained_records as f64 / db_secs);
        }
    }
}

/// Issue one `poll`, advance the persistent cursor, ack the drained records, and
/// (when recording) tally per-poll metrics bucketed by lag at poll time. Returns
/// the record count and the poll's service time.
async fn one_poll(
    id: usize,
    key: &bytes::Bytes,
    state: &FollowState,
    cursor: &mut Cursor,
    record: bool,
) -> anyhow::Result<(usize, Duration)> {
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

    if record {
        state.polls_completed.fetch_add(1, Ordering::Relaxed);
        state.bucket_polls[poll_bucket].fetch_add(1, Ordering::Relaxed);
        state
            .records_consumed
            .fetch_add(out.n_records as u64, Ordering::Relaxed);
        state
            .bytes_consumed
            .fetch_add(out.n_bytes as u64, Ordering::Relaxed);
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
    Ok((out.n_records, svc))
}
