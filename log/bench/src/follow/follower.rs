//! Closed-loop continuous followers.
//!
//! This models the traditional log-follower: a reader actively tails its key,
//! polling for what is new since it last read. Polling is **closed-loop** — the
//! next poll is issued only after the previous one completes (a real reader waits
//! for its response), so a key is never polled concurrently with itself and there
//! is no notion of a missed or dropped poll.
//!
//! Each runner owns a slice of keys and multiplexes their follow-loops, holding at
//! most one poll in flight at a time — so the number of runners is the read
//! concurrency the database sees. A runner keeps a min-heap of per-key due times:
//!
//! - a poll that returns a **full page** means a backlog remains, so the key is
//!   re-polled immediately (drain as fast as possible);
//! - a poll that returns a **short page** means the reader has caught up to the
//!   tail, so it waits `idle_interval` before checking again.
//!
//! That idle delay is the only polling cadence in the model; it keeps caught-up
//! followers from busy-spinning empty reads. A runner that falls behind (its keys'
//! due times slip because it is busy) shows up as growing lag and scheduling lag —
//! the saturation signal, in place of the old open-loop drop counter.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::FollowState;
use super::lag::lag_bucket;
use super::store::Cursor;

/// Run one follower runner until cancelled.
///
/// Owns `key_ids` and follows each closed-loop. Returns an error if a poll fails,
/// which aborts the benchmark.
pub async fn run_follower(
    key_ids: Vec<usize>,
    state: Arc<FollowState>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // Every key starts eligible immediately; the runner serializes its keys, which
    // naturally staggers their subsequent due times.
    let mut heap: BinaryHeap<Reverse<(Instant, usize)>> = BinaryHeap::with_capacity(key_ids.len());
    for &id in &key_ids {
        heap.push(Reverse((state.start, id)));
    }

    while let Some(&Reverse((due, id))) = heap.peek() {
        tokio::select! {
            _ = tokio::time::sleep_until(due) => {}
            _ = cancel.cancelled() => break,
        }
        heap.pop();

        let service_start = Instant::now();
        let scheduling_lag = service_start.saturating_duration_since(due);
        let cursor = Cursor(state.cursors[id].load(Ordering::Relaxed));
        // Lag faced by this poll, captured before acking the records it drains —
        // "lag at poll time", the axis the read cost is bucketed by.
        let lag_at_poll = state.lag.lag(id);

        let out = state
            .store
            .poll(state.keys[id].clone(), cursor, state.page_size)
            .await?;
        state.cursors[id].store(out.cursor.0, Ordering::Relaxed);
        // Acknowledgement is workload state, tracked in every phase so lag stays
        // correct across the warm-up/measure boundary.
        state.lag.record_acked(id, out.n_records as u64);

        if state.recording() {
            let completed = Instant::now();
            let bucket = lag_bucket(lag_at_poll);
            state.polls_completed.fetch_add(1, Ordering::Relaxed);
            state.bucket_polls[bucket].fetch_add(1, Ordering::Relaxed);
            state
                .metrics
                .records_consumed
                .increment(out.n_records as u64);
            state.metrics.polls.increment(1);
            state.metrics.poll_latency_us[bucket]
                .record(completed.saturating_duration_since(due).as_micros() as f64);
            state.metrics.poll_service_us[bucket].record(
                completed
                    .saturating_duration_since(service_start)
                    .as_micros() as f64,
            );
            state
                .metrics
                .scheduling_lag_us
                .record(scheduling_lag.as_micros() as f64);
        }

        // Closed-loop reschedule: a full page means more remains, so re-poll now;
        // otherwise the reader has caught up and waits `idle_interval`.
        let next_due = if out.n_records >= state.page_size {
            Instant::now()
        } else {
            Instant::now() + state.idle_interval
        };
        heap.push(Reverse((next_due, id)));
    }
    Ok(())
}
