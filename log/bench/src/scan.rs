//! Single-key catch-up scan benchmark for the log database.
//!
//! Measures the cost of `scan(key, cursor..)` — the follow path of RFC 0006 —
//! as a function of the storage access path ([`ScanPath`]) and how far behind
//! the cursor is. The point of the benchmark is to compare scan strategies
//! over identical data:
//!
//! - `scan_path=range` (variant A): bounded range scan; the backend prunes
//!   SSTs/blocks by key-range metadata and starts at the cursor, but consults
//!   no bloom filters.
//! - `scan_path=prefix` (variant B): prefix scan over `(segment, key)`;
//!   prefix bloom filters skip SSTs without the key, but the scan reads the
//!   key's entries from the start of each segment regardless of the cursor.
//!
//! The cost signal is object-store GETs per scan (deterministic for a given
//! data layout; see RFC 0006), measured by diffing the process-global GET
//! counter around each scan — scans run sequentially so the attribution is
//! exact. Latency is recorded as a secondary signal.
//!
//! Prefill appends records to keys drawn from a seeded PRNG, so each flushed
//! SST contains a random subset of the keyspace — the regime where bloom
//! filters can help. (Round-robin arrivals would put every key in every SST
//! and filters could never skip anything.) An explicit flush every
//! `flush_every` records controls how many L0 SSTs the prefill produces.
//! Every scan targets a distinct key, so no scan is served from blocks a
//! previous scan already cached.

use std::collections::HashMap;
use std::time::Instant;

use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use log::{AppendError, Config, LogDb, LogRead, ScanOptions, ScanPath};

const MICROS_PER_SEC: f64 = 1_000_000.0;

/// Create a parameter set for the scan benchmark.
fn make_params(
    num_keys: usize,
    entries_per_key: usize,
    value_size: usize,
    flush_every: usize,
    scans: usize,
    scan_lag: usize,
    scan_path: &str,
) -> Params {
    let mut params = Params::new();
    params.insert("num_keys", num_keys.to_string());
    params.insert("entries_per_key", entries_per_key.to_string());
    params.insert("value_size", value_size.to_string());
    params.insert("flush_every", flush_every.to_string());
    params.insert("scans", scans.to_string());
    params.insert("scan_lag", scan_lag.to_string());
    params.insert("scan_path", scan_path.to_string());
    params
}

/// xorshift64* PRNG — deterministic arrivals without an extra dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

/// Benchmark for single-key catch-up scans.
pub struct ScanBenchmark;

impl ScanBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ScanBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Benchmark for ScanBenchmark {
    fn name(&self) -> &str {
        "scan"
    }

    fn default_params(&self) -> Vec<Params> {
        // Defaults: ~400k records over 20k keys (~110 MB raw), 20 flushes.
        // The cross product that tells the story: each scan path at a
        // shallow lag (read the last 5 of ~20 entries) and at full catch-up
        // (read the key's whole history).
        let mut sets = Vec::new();
        for scan_path in ["range", "prefix"] {
            for scan_lag in [5, usize::MAX] {
                sets.push(make_params(20_000, 20, 256, 20_000, 200, scan_lag, scan_path));
            }
        }
        sets
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let num_keys: usize = bench.spec().params().get_parse("num_keys")?;
        let entries_per_key: usize = bench.spec().params().get_parse("entries_per_key")?;
        let value_size: usize = bench.spec().params().get_parse("value_size")?;
        let flush_every: usize = bench.spec().params().get_parse("flush_every")?;
        let scans: usize = bench.spec().params().get_parse("scans")?;
        let scan_lag: usize = bench.spec().params().get_parse("scan_lag")?;
        let scan_path = match bench.spec().params().get("scan_path") {
            Some("prefix") => ScanPath::Prefix,
            Some("range") => ScanPath::Range,
            other => anyhow::bail!("scan_path must be 'prefix' or 'range', got {:?}", other),
        };
        let total_records = num_keys * entries_per_key;
        anyhow::ensure!(scans <= num_keys, "scans must be <= num_keys");

        // Live metrics - updated during the benchmark
        let scans_counter = bench.counter("scans");
        let scan_latency = bench.histogram("scan_latency_us");
        let scan_gets = bench.histogram("scan_gets");

        let config = Config {
            storage: bench.spec().data().storage.clone(),
            ..Default::default()
        };
        let db = LogDb::open(config.clone()).await?;

        let keys: Vec<Bytes> = (0..num_keys)
            .map(|i| Bytes::from(format!("key-{:08}", i)))
            .collect();

        // Scans target every `stride`-th key — distinct keys, spread across
        // the keyspace, deterministic. Their exact entry sequences are
        // recorded during prefill so a scan can start `scan_lag` entries
        // behind the key's tail.
        let stride = num_keys / scans;
        let sampled: HashMap<usize, usize> = (0..scans)
            .map(|s| (s * stride, s))
            .collect();
        let mut sampled_seqs: Vec<Vec<u64>> = vec![Vec::new(); scans];

        // Prefill: `total_records` appends to PRNG-drawn keys, in awaited
        // batches so assigned sequences are exactly append order. Flushing
        // every `flush_every` records bounds memtable size and spreads the
        // data across multiple SSTs.
        let prefill_start = Instant::now();
        let prefill_gets_base = common::object_store_gets();
        let value = Bytes::from(vec![b'x'; value_size]);
        let mut rng = Rng::new(42);
        let mut next_seq: Option<u64> = None;
        let mut appended = 0usize;
        // Per-key tallies to know each key's entry count (sampled keys also
        // get their sequence positions).
        let mut per_key_counts: Vec<u32> = vec![0; num_keys];
        const BATCH: usize = 1024;
        while appended < total_records {
            let n = BATCH.min(total_records - appended);
            let mut batch_keys = Vec::with_capacity(n);
            for _ in 0..n {
                let key_idx = (rng.next() % num_keys as u64) as usize;
                batch_keys.push(key_idx);
            }
            let records = batch_keys
                .iter()
                .map(|&key_idx| log::Record {
                    key: keys[key_idx].clone(),
                    value: value.clone(),
                })
                .collect();
            let output = append_retrying(&db, records).await?;
            let start_seq = output.start_sequence;
            if let Some(expected) = next_seq {
                anyhow::ensure!(
                    start_seq == expected,
                    "non-contiguous sequences: expected {}, got {}",
                    expected,
                    start_seq
                );
            }
            next_seq = Some(start_seq + n as u64);
            for (i, &key_idx) in batch_keys.iter().enumerate() {
                per_key_counts[key_idx] += 1;
                if let Some(&slot) = sampled.get(&key_idx) {
                    sampled_seqs[slot].push(start_seq + i as u64);
                }
            }
            appended += n;
            if appended % flush_every < n {
                db.flush().await?;
            }
        }
        db.flush().await?;
        let prefill_secs = prefill_start.elapsed().as_secs_f64();
        let prefill_gets = (common::object_store_gets() - prefill_gets_base) as f64;

        // When the object store is durable, reopen the database so the
        // measured scans start from a cold block cache instead of reading
        // back blocks the prefill left in memory. (An in-memory object store
        // is per-handle — reopening would lose the data — so those runs keep
        // the warm handle and measure mostly cache behavior.)
        let durable = matches!(
            &config.storage,
            common::StorageConfig::SlateDb(c)
                if !matches!(c.object_store, common::ObjectStoreConfig::InMemory)
        );
        let db = if durable {
            db.close().await?;
            LogDb::open(config.clone()).await?
        } else {
            db
        };

        // Measure: sequential scans, one distinct key each, diffing the
        // global GET counter around each scan for exact attribution.
        let runner = bench.start();
        let mut latencies_us: Vec<f64> = Vec::with_capacity(scans);
        let mut gets_per_scan: Vec<f64> = Vec::with_capacity(scans);
        let mut entries_read = 0usize;
        let mut completed = 0usize;

        for (slot, seqs) in sampled_seqs.iter().enumerate() {
            if !runner.keep_running() {
                break;
            }
            let key_idx = slot * stride;
            if seqs.is_empty() {
                // The PRNG never drew this key; nothing to scan.
                continue;
            }
            let start_pos = seqs.len().saturating_sub(scan_lag);
            let start_seq = seqs[start_pos];
            let expected = seqs.len() - start_pos;

            let gets_before = common::object_store_gets();
            let scan_start = Instant::now();
            let mut iter = db
                .scan_with_options(
                    keys[key_idx].clone(),
                    start_seq..,
                    ScanOptions { scan_path },
                )
                .await?;
            let mut n = 0usize;
            while let Some(_entry) = iter.next().await? {
                n += 1;
            }
            let elapsed = scan_start.elapsed();
            let gets = common::object_store_gets() - gets_before;

            anyhow::ensure!(
                n == expected,
                "scan of key {} returned {} entries, expected {}",
                key_idx,
                n,
                expected
            );

            scans_counter.increment(1);
            scan_latency.record(elapsed.as_secs_f64() * MICROS_PER_SEC);
            scan_gets.record(gets as f64);
            latencies_us.push(elapsed.as_secs_f64() * MICROS_PER_SEC);
            gets_per_scan.push(gets as f64);
            entries_read += n;
            completed += 1;
        }
        let elapsed_secs = runner.elapsed().as_secs_f64();

        let mut summary = Summary::new()
            .add("prefill_secs", prefill_secs)
            .add("prefill_gets", prefill_gets)
            .add("scans_completed", completed as f64)
            .add("entries_read", entries_read as f64)
            .add("scans_per_sec", completed as f64 / elapsed_secs);
        summary = add_percentiles(summary, "latency_us", &mut latencies_us);
        summary = add_percentiles(summary, "gets", &mut gets_per_scan);
        bench.summarize(summary).await?;

        db.close().await?;
        bench.close().await?;
        Ok(())
    }
}

/// Append a batch, retrying on `QueueFull`/`Timeout` backpressure.
async fn append_retrying(
    db: &LogDb,
    mut records: Vec<log::Record>,
) -> anyhow::Result<log::AppendOutput> {
    loop {
        match db.try_append(records).await {
            Ok(output) => return Ok(output),
            Err(AppendError::QueueFull(returned)) | Err(AppendError::Timeout(returned)) => {
                records = returned;
                tokio::task::yield_now().await;
            }
            Err(terminal) => anyhow::bail!("append failed: {terminal}"),
        }
    }
}

/// Add mean/p50/p90/p99/max of `values` to the summary under `name_*` keys.
fn add_percentiles(summary: Summary, name: &str, values: &mut [f64]) -> Summary {
    if values.is_empty() {
        return summary;
    }
    values.sort_by(|a, b| a.partial_cmp(b).expect("no NaNs"));
    let pct = |p: f64| values[((values.len() - 1) as f64 * p) as usize];
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    summary
        .add(format!("{name}_mean"), mean)
        .add(format!("{name}_p50"), pct(0.50))
        .add(format!("{name}_p90"), pct(0.90))
        .add(format!("{name}_p99"), pct(0.99))
        .add(format!("{name}_max"), *values.last().expect("non-empty"))
}
