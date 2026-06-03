//! The arrivals writer.
//!
//! A writer appends records to keys at a target rate (RFC 0006 "Workload model").
//! The aggregate write rate is `cardinality x arrival_rate_per_key`, split across
//! `num_writers`; arrival is uniform across keys in the first pass. Arrivals are
//! paced open-loop against a fixed schedule, independent of how long each append
//! takes, so the offered write load is what we configured rather than what the
//! database happened to accept.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use log::Record;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::FollowState;
use super::lag::LagTracker;
use super::store::LogDbStore;

/// Pre-fill every key with `prefill_per_key` records, in parallel across
/// `concurrency` tasks (key ids partitioned by stride). Each key is appended as a
/// single batch, so this scales to large cardinalities instead of one round-trip
/// per record.
pub async fn run_prefill(
    store: Arc<LogDbStore>,
    keys: Arc<Vec<Bytes>>,
    value: Bytes,
    prefill_per_key: usize,
    concurrency: usize,
    lag: Arc<LagTracker>,
) -> anyhow::Result<()> {
    let concurrency = concurrency.max(1);
    let n = keys.len();
    let mut handles = Vec::with_capacity(concurrency);
    for shard in 0..concurrency {
        let (store, keys, value, lag) = (store.clone(), keys.clone(), value.clone(), lag.clone());
        handles.push(tokio::spawn(async move {
            let mut id = shard;
            while id < n {
                let batch: Vec<Record> = (0..prefill_per_key)
                    .map(|_| Record {
                        key: keys[id].clone(),
                        value: value.clone(),
                    })
                    .collect();
                store.append_batch(batch).await?;
                lag.record_appended(id, prefill_per_key as u64);
                id += concurrency;
            }
            Ok::<(), anyhow::Error>(())
        }));
    }
    for h in handles {
        h.await??;
    }
    Ok(())
}

/// Run one arrivals writer until cancelled.
///
/// Writer `writer_idx` owns the keys where `id % num_writers == writer_idx` and
/// cycles through them, emitting `per_writer_rate` records per second in total.
pub async fn run_writer(
    writer_idx: usize,
    num_writers: usize,
    per_writer_rate: f64,
    value: Bytes,
    state: Arc<FollowState>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let my_keys: Vec<usize> = (writer_idx..state.keys.len())
        .step_by(num_writers)
        .collect();
    if my_keys.is_empty() || per_writer_rate <= 0.0 {
        // Nothing to do but wait for shutdown.
        cancel.cancelled().await;
        return Ok(());
    }

    let period = Duration::from_secs_f64(1.0 / per_writer_rate);
    let mut next = Instant::now();
    let mut cursor = 0usize;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next) => {}
            _ = cancel.cancelled() => break,
        }
        let id = my_keys[cursor % my_keys.len()];
        cursor += 1;

        // `LogDbStore::append` absorbs `QueueFull` internally (counted there as the
        // ingest-capacity ceiling); an error here is terminal.
        state
            .store
            .append(state.keys[id].clone(), value.clone())
            .await?;
        // Appends drive lag in every phase (workload state); the arrivals metric is
        // recorded only in the measure phase.
        state.lag.record_appended(id, 1);
        if state.recording() {
            state.metrics.arrivals.increment(1);
            state.arrivals_completed.fetch_add(1, Ordering::Relaxed);
        }

        next += period;
    }
    Ok(())
}
