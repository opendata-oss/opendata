//! Single-key catch-up scan benchmark for the log database.
//!
//! Measures the cost of `scan(key, cursor..)` — the follow path of RFC 0006 —
//! comparing both storage access paths ([`ScanPath`]) **over the same store**:
//!
//! - `range` (variant A): bounded range scan; the backend prunes SSTs/blocks
//!   by key-range metadata and starts at the cursor, but consults no bloom
//!   filters.
//! - `prefix` (variant B): prefix scan over `(segment, key)`; prefix bloom
//!   filters skip SSTs without the key, but the scan reads the key's entries
//!   from the start of each segment regardless of the cursor.
//!
//! Each cell prefills once and then interleaves the two paths across its
//! scans (even sample slots scan with `range`, odd with `prefix` — disjoint
//! keys, identical tree). Per-cell prefill would not work for an A/B
//! comparison: the LSM shape after prefill is a race between ingest, seal
//! timing, and compaction, and per-scan cost is dominated by the number of
//! sources (L0 SSTs + sorted runs) in the segments the cursor covers — two
//! independently prefilled stores routinely settle into different shapes.
//!
//! The cost signal is object-store GETs per scan, measured by diffing the
//! process-global GET counter around each scan — scans run sequentially so
//! the attribution is exact. Latency is recorded as a secondary signal.
//!
//! Prefill appends records to keys drawn from a seeded PRNG, so each flushed
//! SST contains a random subset of the keyspace — the regime where bloom
//! filters can help. (Round-robin arrivals would put every key in every SST
//! and filters could never skip anything.) Note the absent-key probability is
//! e^(-records_per_source / num_keys): at low cardinality every source
//! contains every key and filters cannot skip anything, so filter benefits
//! only show at high `num_keys`. An explicit flush every `flush_every`
//! records controls how many L0 SSTs the prefill produces. Every scan
//! targets a distinct key, so no scan is served from blocks a previous scan
//! already cached.

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
) -> Params {
    let mut params = Params::new();
    params.insert("num_keys", num_keys.to_string());
    params.insert("entries_per_key", entries_per_key.to_string());
    params.insert("value_size", value_size.to_string());
    params.insert("flush_every", flush_every.to_string());
    params.insert("scans", scans.to_string());
    params.insert("scan_lag", scan_lag.to_string());
    params.insert("settle_secs", "0".to_string());
    params.insert("seal_interval_ms", "0".to_string());
    params
}

/// Per-path accumulators for the interleaved measure loop.
#[derive(Default)]
struct PathStats {
    latencies_us: Vec<f64>,
    gets: Vec<f64>,
    entries_read: usize,
    completed: usize,
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
        // Two data shapes, each scanned by both paths at a shallow lag
        // (read the last 5 entries) and at full catch-up (read the key's
        // whole history):
        //
        // - Shallow histories: 20k keys x 20 entries (~110 MB). A key's
        //   entire history fits in a block or two, so the cost of a scan is
        //   dominated by locating the key, not by how far back it reads.
        // - Deep histories: 2k keys x 1000 entries (~560 MB raw, ~256 KB =
        //   dozens of blocks per key). The cursor position matters: a
        //   prefix scan reads the key's blocks from entry 0 regardless of
        //   the cursor, while a range scan starts at the cursor.
        let mut sets = Vec::new();
        for scan_lag in [5, usize::MAX] {
            sets.push(make_params(20_000, 20, 256, 20_000, 200, scan_lag));
        }
        for scan_lag in [5, usize::MAX] {
            sets.push(make_params(2_000, 1_000, 256, 20_000, 200, scan_lag));
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
        // Optional knobs — config-file param sets replace the defaults
        // wholesale, so absent labels mean "off" rather than an error.
        let settle_secs: u64 = match bench.spec().params().get("settle_secs") {
            Some(v) => v.parse()?,
            None => 0,
        };
        let seal_interval_ms: u64 = match bench.spec().params().get("seal_interval_ms") {
            Some(v) => v.parse()?,
            None => 0,
        };
        let total_records = num_keys * entries_per_key;
        anyhow::ensure!(scans <= num_keys, "scans must be <= num_keys");

        // Live metrics - updated during the benchmark
        let scans_counter = bench.counter("scans");
        let range_latency = bench.histogram("range_scan_latency_us");
        let range_gets = bench.histogram("range_scan_gets");
        let prefix_latency = bench.histogram("prefix_scan_latency_us");
        let prefix_gets = bench.histogram("prefix_scan_gets");

        // Sealing matters for LSM shape: the compaction scheduler only
        // relieves L0 pressure on the active segment (sorted runs pile up
        // and are never merged with each other); full consolidation into a
        // single dense sorted run happens only after a segment seals. A
        // nonzero seal interval lets the prefill produce sealed segments
        // that consolidate during the settle window.
        let seal_interval = (seal_interval_ms > 0)
            .then(|| std::time::Duration::from_millis(seal_interval_ms));
        let config = Config {
            storage: bench.spec().data().storage.clone(),
            segmentation: log::SegmentConfig { seal_interval },
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
        // Compaction runs inside the writer; an aggressive prefill outpaces
        // it and leaves the tree as a pile of small sorted runs. A settle
        // window lets the compactor consolidate before we measure, so the
        // run can target the steady-state LSM shape rather than the
        // mid-ingest one. The per-scan GET counts reveal which shape a run
        // actually measured.
        // Verify key 0 on the warm writer handle at two points: right after
        // prefill (before the settle window's compaction) and again after
        // settling. Together with the post-reopen scan assertions this
        // brackets where entries disappear: write path, settle-window
        // compaction, or reopen/recovery.
        verify_key0(&db, &keys[0], &sampled_seqs[0], ScanPath::Range, "post-prefill").await?;
        if settle_secs > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(settle_secs)).await;
        }
        let prefill_secs = prefill_start.elapsed().as_secs_f64();
        let prefill_gets = (common::object_store_gets() - prefill_gets_base) as f64;
        verify_key0(&db, &keys[0], &sampled_seqs[0], ScanPath::Range, "pre-close").await?;

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
        // global GET counter around each scan for exact attribution. The two
        // paths alternate by sample slot (even = range, odd = prefix) so
        // they run over the identical tree on disjoint keys, with any cache
        // warm-up spread evenly across both.
        let runner = bench.start();
        let mut range_stats = PathStats::default();
        let mut prefix_stats = PathStats::default();

        for (slot, seqs) in sampled_seqs.iter().enumerate() {
            if !runner.keep_running() {
                break;
            }
            let key_idx = slot * stride;
            if seqs.is_empty() {
                // The PRNG never drew this key; nothing to scan.
                continue;
            }
            let scan_path = if slot % 2 == 0 {
                ScanPath::Range
            } else {
                ScanPath::Prefix
            };
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
            let mut returned = Vec::with_capacity(expected);
            while let Some(entry) = iter.next().await? {
                n += 1;
                returned.push(entry.sequence);
            }
            let elapsed = scan_start.elapsed();
            let gets = common::object_store_gets() - gets_before;

            if n != expected {
                let returned_set: std::collections::HashSet<u64> =
                    returned.iter().copied().collect();
                let missing: Vec<u64> = seqs[start_pos..]
                    .iter()
                    .copied()
                    .filter(|s| !returned_set.contains(s))
                    .collect();
                let segments: Vec<(u32, u64)> = db
                    .list_segments(..)
                    .await?
                    .iter()
                    .map(|s| (s.id, s.start_seq))
                    .collect();
                anyhow::bail!(
                    "{:?} scan of key {} returned {} entries, expected {}; \
                     {} missing seqs (first 20: {:?}); expected range {}..={}; \
                     segments (id, start_seq): {:?}",
                    scan_path,
                    key_idx,
                    n,
                    expected,
                    missing.len(),
                    &missing[..missing.len().min(20)],
                    seqs[start_pos],
                    seqs[seqs.len() - 1],
                    segments,
                );
            }

            scans_counter.increment(1);
            let latency_us = elapsed.as_secs_f64() * MICROS_PER_SEC;
            let stats = match scan_path {
                ScanPath::Range => {
                    range_latency.record(latency_us);
                    range_gets.record(gets as f64);
                    &mut range_stats
                }
                ScanPath::Prefix => {
                    prefix_latency.record(latency_us);
                    prefix_gets.record(gets as f64);
                    &mut prefix_stats
                }
            };
            stats.latencies_us.push(latency_us);
            stats.gets.push(gets as f64);
            stats.entries_read += n;
            stats.completed += 1;
        }
        let elapsed_secs = runner.elapsed().as_secs_f64();
        let completed = range_stats.completed + prefix_stats.completed;

        let mut summary = Summary::new()
            .add("prefill_secs", prefill_secs)
            .add("prefill_gets", prefill_gets)
            .add("scans_completed", completed as f64)
            .add(
                "entries_read",
                (range_stats.entries_read + prefix_stats.entries_read) as f64,
            )
            .add("scans_per_sec", completed as f64 / elapsed_secs);
        summary = add_percentiles(summary, "range_latency_us", &mut range_stats.latencies_us);
        summary = add_percentiles(summary, "range_gets", &mut range_stats.gets);
        summary = add_percentiles(summary, "prefix_latency_us", &mut prefix_stats.latencies_us);
        summary = add_percentiles(summary, "prefix_gets", &mut prefix_stats.gets);
        bench.summarize(summary).await?;

        db.close().await?;
        bench.close().await?;
        Ok(())
    }
}

/// Scan all of key 0's entries on the given handle and report how many of
/// the expected set came back. A debugging probe, not a measurement.
async fn verify_key0(
    db: &LogDb,
    key: &Bytes,
    seqs: &[u64],
    scan_path: ScanPath,
    label: &str,
) -> anyhow::Result<()> {
    if seqs.is_empty() {
        return Ok(());
    }
    let mut iter = db
        .scan_with_options(key.clone(), seqs[0].., ScanOptions { scan_path })
        .await?;
    let mut n = 0usize;
    while iter.next().await?.is_some() {
        n += 1;
    }
    println!(
        "    {} verify: key 0 returned {} of {} expected entries",
        label,
        n,
        seqs.len()
    );
    Ok(())
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
