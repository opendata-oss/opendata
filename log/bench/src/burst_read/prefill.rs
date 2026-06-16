//! Burst-pattern prefill — builds the static read corpus.
//!
//! The follow bench's prefill writes each key as a single dense batch, so a key's
//! records are contiguous in the log and a reader pays near-ideal locality. The
//! burst workload's read cost comes from the opposite shape: a key's records are
//! *scattered* across many segments, because the key bursted at different times
//! interleaved with thousands of others. We reproduce that here by writing in
//! `rounds` passes over the population — `burst_size` records per key per pass —
//! so each key's `rounds * burst_size` records land in `rounds` separate time
//! windows. With `seal_interval` cutting segments throughout the prefill, those
//! windows fall in different segments, giving the reader real segment fan-out to
//! pay for.
//!
//! Looping `rounds` on the *outside* of the key sweep is what produces the
//! scatter: a key gets one burst, then the task writes every other key in its
//! shard before returning to it, so successive bursts for one key are far apart
//! in the global sequence (and across seals).

use std::sync::Arc;

use bytes::Bytes;
use log::Record;

use crate::read::lag::LagTracker;
use crate::read::store::LogDbStore;

/// Write `rounds * burst_size` records to every key, scattered across segments.
///
/// Keys are partitioned across `concurrency` tasks by stride; each task makes
/// `rounds` passes over its keys, appending one `burst_size` batch per key per
/// pass. Returns once the whole corpus is written (the caller flushes).
pub async fn run_prefill(
    store: Arc<LogDbStore>,
    keys: Arc<Vec<Bytes>>,
    value: Bytes,
    rounds: usize,
    burst_size: usize,
    concurrency: usize,
    lag: Arc<LagTracker>,
) -> anyhow::Result<()> {
    let concurrency = concurrency.max(1);
    let n = keys.len();
    let mut handles = Vec::with_capacity(concurrency);
    for shard in 0..concurrency {
        let (store, keys, value, lag) = (store.clone(), keys.clone(), value.clone(), lag.clone());
        handles.push(tokio::spawn(async move {
            for _ in 0..rounds {
                let mut id = shard;
                while id < n {
                    let batch: Vec<Record> = (0..burst_size)
                        .map(|_| Record {
                            key: keys[id].clone(),
                            value: value.clone(),
                        })
                        .collect();
                    store.append_batch(batch).await?;
                    lag.record_appended(id, burst_size as u64);
                    id += concurrency;
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }
    for h in handles {
        h.await??;
    }
    Ok(())
}
