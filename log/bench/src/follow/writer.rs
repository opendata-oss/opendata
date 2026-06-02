//! The arrivals writer.
//!
//! A writer appends records to keys at a target rate (RFC 0006 "Workload model").
//! The aggregate write rate is `cardinality x arrival_rate_per_key`, split across
//! `num_writers`; arrival is uniform across keys in the first pass. Arrivals are
//! paced open-loop against a fixed schedule, independent of how long each append
//! takes, so the offered write load is what we configured rather than what the
//! database happened to accept.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::FollowState;

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
        state.lag.record_appended(id, 1);
        state.metrics.arrivals.increment(1);

        next += period;
    }
    Ok(())
}
