//! keyscale: read/write scalability vs. key population.
//!
//! The thesis: read/write performance tracks the *active* working set, not the
//! total number of keys in the system — i.e. an idle key carries negligible
//! maintenance cost. To show it we never prefill a static corpus. Instead the
//! population grows organically: `W` writer tasks and `R` reader tasks each
//! sample keys independently from one shared Zipf over a fixed address space
//! `[0, N)`. Writers append bursts (size `B`); the set of distinct keys ever
//! written climbs over the run as the Zipf's tail is gradually discovered, while
//! the hot head stays bounded — so the active working set is decoupled from how
//! many cold keys have accumulated.
//!
//! Reads run through a *separate* `LogDbReader` (a decoupled read replica), each
//! read following a cursor from the start of a sampled key's log to its current
//! tail. Because reader and writer sample the same Zipf independently, the
//! overwhelming majority of reads land on already-written keys; a read of a
//! not-yet-visible key is a cheap, filter-pruned empty read, tracked as a rate.
//!
//! Every snapshot interval we log a row keyed by the current distinct-key count,
//! so one run draws the "performance vs. keys-resident" curve. The headline,
//! cache-independent signals are segment scans and object-store GETs per poll: if
//! those stay flat as distinct keys grow, an idle key is genuinely free.

mod zipf;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::bail;
use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use common::StorageConfig;
use common::storage::config::ObjectStoreConfig;
use log::{Config, LogDb, LogDbReader, ReaderConfig, Record};
use metrics_util::Summary as Sketch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::read::store::{Cursor, LogDbStore, LogStore};
use crate::workload;
use zipf::{SplitMix64, Zipf};

pub struct KeyScaleBenchmark;

impl KeyScaleBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for KeyScaleBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

/// The single fast smoke parameter set; full scenario grids come via `--config`.
/// Requires a shared object store (Local/Aws): the read side is a standalone
/// `LogDbReader`, so an in-memory (per-handle) store would hide the writer's data.
fn smoke_params() -> Params {
    let mut p = Params::new();
    // Population: N = address space, s = Zipf skew (shared by readers and writers).
    p.insert("key_population", "100000");
    p.insert("zipf_skew", "1.1");
    p.insert("key_length", "16");
    p.insert("value_size", "128");
    // Concurrency: W writers, R readers.
    p.insert("write_concurrency", "8");
    p.insert("read_concurrency", "16");
    // Target aggregate write rate (MiB/s of record bytes); 0 = unpaced. Held
    // constant across the sweep so write load doesn't confound read scaling.
    p.insert("write_mb_per_sec", "10");
    // Write burst size B (records appended per admission). Constant for now.
    p.insert("burst_size", "8");
    // Records per write-path append call. Several bursts are packed into one
    // `try_append` so the per-call cost doesn't cap throughput (the gap vs. the
    // ingest bench). Decoupled from burst_size, which stays a workload knob.
    p.insert("write_batch_size", "100");
    // Read paging: records per poll while following a key's cursor.
    p.insert("page_size", "32");
    // Backoff after a read that finds nothing visible, so readers don't busy-spin
    // the cold tail or the t=0 empty store.
    p.insert("empty_backoff_ms", "5");
    // Storage / reader.
    p.insert("seal_interval_ms", "500");
    p.insert("refresh_interval_ms", "1000");
    p.insert("block_cache_mb", "256");
    p.insert("meta_cache_mb", "0");
    // Cadence of the distinct-keyed progress rows.
    p.insert("snapshot_interval_ms", "1000");
    p.insert("seed", "1");
    p
}

/// Role tags that decorrelate writer and reader RNG streams sharing a seed.
const ROLE_WRITE: u64 = 0x57; // 'W'
const ROLE_READ: u64 = 0x52; // 'R'

/// Decorrelate a per-worker RNG seed from the run seed, a role tag, and the
/// worker index, so every writer/reader draws an independent Zipf stream.
fn worker_seed(run_seed: u64, role: u64, idx: usize) -> u64 {
    let mixed = run_seed
        ^ role.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (idx as u64).wrapping_mul(0xD1B5_4A32_D192_ED03);
    SplitMix64::new(mixed).next_u64()
}

/// Shared workload state for the writer and reader pools. Tallies are plain
/// atomics summed over the whole run (there is no warm-up phase: growth from an
/// empty store *is* the measurement).
struct Shared {
    store: Arc<dyn LogStore>,
    n: usize,
    skew: f64,
    key_length: usize,
    value: Bytes,
    record_size: u64,
    burst_size: usize,
    /// Records per `try_append` (records, not keys): several bursts are packed
    /// into one append so the per-call cost doesn't cap write throughput.
    write_batch_size: usize,
    /// Per-writer interval between batches that holds the aggregate write rate at
    /// the target; zero means unpaced (closed-loop).
    batch_interval: Duration,
    page_size: usize,
    empty_backoff: Duration,
    seed: u64,

    /// One bit per key: set on the key's first write. Drives `distinct`.
    resident: Vec<AtomicU64>,
    distinct: AtomicU64,

    // Write tallies.
    write_records: AtomicU64,
    write_bytes: AtomicU64,
    queue_full: AtomicU64,
    // Read tallies.
    reads: AtomicU64,
    polls: AtomicU64,
    read_records: AtomicU64,
    read_bytes: AtomicU64,
    empty_reads: AtomicU64,

    // Service-time sketches.
    poll_us: Mutex<Sketch>,
    write_us: Mutex<Sketch>,
}

impl Shared {
    /// Marks key `idx` resident, returning true if this is its first write.
    fn mark_resident(&self, idx: usize) -> bool {
        let word = idx / 64;
        let bit = 1u64 << (idx % 64);
        let prev = self.resident[word].fetch_or(bit, Ordering::Relaxed);
        prev & bit == 0
    }
}

/// Writer task: build a batch by sampling `keys_per_batch` keys from the Zipf and
/// appending a burst of `B` records to each, then offer the whole batch in one
/// `try_append`. Batching across bursts (rather than one append per burst)
/// amortizes the write-path cost — a burst of `B` records per call makes the
/// per-call overhead the throughput ceiling, exactly the gap vs. the ingest
/// bench. Newly sampled keys grow the resident population; a batch rejected for
/// `QueueFull` is dropped and counted (the open-loop saturation signal).
async fn write_worker(
    shared: Arc<Shared>,
    idx: usize,
    pace_start: Instant,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let zipf = Zipf::new(shared.n, shared.skew);
    let mut rng = SplitMix64::new(worker_seed(shared.seed, ROLE_WRITE, idx));
    let keys_per_batch = (shared.write_batch_size / shared.burst_size).max(1);
    let mut batches_done: u32 = 0;
    while !cancel.is_cancelled() {
        // Pack `keys_per_batch` bursts into one append; remember the sampled
        // keys so we only mark them resident if the batch is accepted.
        let mut batch = Vec::with_capacity(keys_per_batch * shared.burst_size);
        let mut sampled = Vec::with_capacity(keys_per_batch);
        for _ in 0..keys_per_batch {
            let key_idx = zipf.sample(&mut rng);
            sampled.push(key_idx);
            let key = workload::key_at(key_idx, shared.key_length);
            for _ in 0..shared.burst_size {
                batch.push(Record {
                    key: key.clone(),
                    value: shared.value.clone(),
                });
            }
        }
        let n_records = batch.len() as u64;

        let t0 = Instant::now();
        let accepted = shared.store.try_offer(batch).await?;
        let us = t0.elapsed().as_micros() as f64;

        if accepted {
            for key_idx in sampled {
                if shared.mark_resident(key_idx) {
                    shared.distinct.fetch_add(1, Ordering::Relaxed);
                }
            }
            shared.write_records.fetch_add(n_records, Ordering::Relaxed);
            shared
                .write_bytes
                .fetch_add(n_records * shared.record_size, Ordering::Relaxed);
            shared.write_us.lock().unwrap().add(us);
        } else {
            shared.queue_full.fetch_add(1, Ordering::Relaxed);
        }

        // Open-loop pacing: hold the aggregate write rate at the target so the
        // write load is constant across the N/R sweep rather than drifting with
        // the system's closed-loop ceiling. `batch_interval == 0` means unpaced.
        batches_done += 1;
        if !shared.batch_interval.is_zero() {
            let next = pace_start + shared.batch_interval * batches_done;
            let now = Instant::now();
            if next > now {
                tokio::time::sleep(next - now).await;
            }
        }
    }
    Ok(())
}

/// Reader task: repeatedly sample a key from the Zipf and follow a cursor from
/// the start of its log to the current tail, paging `page_size` records at a
/// time. A sampled key with nothing visible yet counts as an empty read.
async fn read_worker(
    shared: Arc<Shared>,
    idx: usize,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let zipf = Zipf::new(shared.n, shared.skew);
    let mut rng = SplitMix64::new(worker_seed(shared.seed, ROLE_READ, idx));
    while !cancel.is_cancelled() {
        let key_idx = zipf.sample(&mut rng);
        let key = workload::key_at(key_idx, shared.key_length);
        let mut cursor = Cursor(0);
        let mut got_any = false;
        loop {
            let t0 = Instant::now();
            let out = shared
                .store
                .poll(key.clone(), cursor, shared.page_size)
                .await?;
            let us = t0.elapsed().as_micros() as f64;

            shared.polls.fetch_add(1, Ordering::Relaxed);
            shared
                .read_records
                .fetch_add(out.n_records as u64, Ordering::Relaxed);
            shared
                .read_bytes
                .fetch_add(out.n_bytes as u64, Ordering::Relaxed);
            shared.poll_us.lock().unwrap().add(us);

            if out.n_records == 0 {
                break;
            }
            got_any = true;
            cursor = out.cursor;
            if cancel.is_cancelled() {
                break;
            }
        }
        shared.reads.fetch_add(1, Ordering::Relaxed);
        if !got_any {
            shared.empty_reads.fetch_add(1, Ordering::Relaxed);
            // A real follower doesn't busy-spin a key with nothing visible yet
            // (newly written but not flushed/refreshed, or an untouched tail
            // key). Back off so the t=0 empty store and the cold tail don't
            // dominate the poll counters with no-op reads.
            if !shared.empty_backoff.is_zero() {
                tokio::time::sleep(shared.empty_backoff).await;
            }
        }
    }
    Ok(())
}

/// A point-in-time snapshot of the cumulative counters, used to compute
/// per-interval rates for the progress rows.
#[derive(Clone, Copy, Default)]
struct Counters {
    distinct: u64,
    polls: u64,
    read_records: u64,
    write_records: u64,
    gets: u64,
    get_bytes: u64,
    read_bytes: u64,
    segment_scans: u64,
}

fn snapshot(shared: &Shared) -> Counters {
    Counters {
        distinct: shared.distinct.load(Ordering::Relaxed),
        polls: shared.polls.load(Ordering::Relaxed),
        read_records: shared.read_records.load(Ordering::Relaxed),
        write_records: shared.write_records.load(Ordering::Relaxed),
        gets: common::object_store_gets(),
        get_bytes: common::object_store_get_bytes(),
        read_bytes: shared.read_bytes.load(Ordering::Relaxed),
        segment_scans: log::segment_scans(),
    }
}

#[async_trait::async_trait]
impl Benchmark for KeyScaleBenchmark {
    fn name(&self) -> &str {
        "keyscale"
    }

    fn default_params(&self) -> Vec<Params> {
        vec![smoke_params()]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let params = bench.spec().params();
        let n: usize = params.get_parse::<usize>("key_population")?.max(1);
        let skew: f64 = params.get_parse("zipf_skew")?;
        let key_length: usize = params.get_parse("key_length")?;
        let value_size: usize = params.get_parse("value_size")?;
        let write_concurrency: usize = params.get_parse::<usize>("write_concurrency")?.max(1);
        let read_concurrency: usize = params.get_parse::<usize>("read_concurrency")?.max(1);
        let burst_size: usize = params.get_parse::<usize>("burst_size")?.max(1);
        let write_batch_size: usize = params
            .get_parse::<usize>("write_batch_size")?
            .max(burst_size);
        let write_mb_per_sec: f64 = params.get_parse("write_mb_per_sec")?;
        let page_size: usize = params.get_parse::<usize>("page_size")?.max(1);
        let empty_backoff = Duration::from_millis(params.get_parse("empty_backoff_ms")?);
        let seed: u64 = params.get_parse("seed")?;
        let snapshot_interval_ms: u64 = params.get_parse::<u64>("snapshot_interval_ms")?.max(1);

        let seal_interval = match params.get("seal_interval_ms") {
            Some(v) => match v.parse::<u64>()? {
                0 => None,
                ms => Some(Duration::from_millis(ms)),
            },
            None => None,
        };
        let refresh_interval_ms: u64 = params.get_parse("refresh_interval_ms")?;
        let block_cache_mb: u64 = params.get_parse("block_cache_mb")?;
        let meta_cache_mb: u64 = params.get_parse("meta_cache_mb")?;

        let storage_config = bench.spec().data().storage.clone();
        if !object_store_is_shared(&storage_config) {
            bail!(
                "keyscale requires a shared object store (Local or Aws); the configured store \
                 is in-memory and per-handle, so the standalone LogDbReader (the read side) \
                 would observe none of the writer's data"
            );
        }

        // Writer.
        let mut config = Config {
            storage: storage_config.clone(),
            ..Default::default()
        };
        config.segmentation.seal_interval = seal_interval;
        let writer = LogDb::open(config).await?;

        // Decoupled standalone reader, with optional split data/metadata cache.
        let reader_config = ReaderConfig {
            storage: storage_config,
            refresh_interval: Duration::from_millis(refresh_interval_ms),
        };
        let block_cache = (block_cache_mb > 0)
            .then(|| common::create_in_memory_block_cache(block_cache_mb << 20));
        let meta_cache =
            (meta_cache_mb > 0).then(|| common::create_in_memory_block_cache(meta_cache_mb << 20));
        let reader = if block_cache.is_some() || meta_cache.is_some() {
            LogDbReader::open_with_caches(reader_config, block_cache, meta_cache).await?
        } else {
            LogDbReader::open(reader_config).await?
        };
        let logdb_store = Arc::new(LogDbStore::with_reader(writer, reader));

        // Per-writer interval between batches that holds the aggregate write rate
        // at the target. Zero when unpaced (write_mb_per_sec <= 0). The interval
        // is sized to the actual records per batch (keys_per_batch * burst_size).
        let record_size = workload::record_size(key_length, value_size) as f64;
        let keys_per_batch = (write_batch_size / burst_size).max(1);
        let records_per_batch = keys_per_batch * burst_size;
        let batch_interval = if write_mb_per_sec > 0.0 {
            let target_records_per_sec = write_mb_per_sec * 1_000_000.0 / record_size;
            let per_writer_rate = target_records_per_sec / write_concurrency as f64;
            Duration::from_secs_f64(records_per_batch as f64 / per_writer_rate)
        } else {
            Duration::ZERO
        };

        let resident_words = n.div_ceil(64);
        let shared = Arc::new(Shared {
            store: logdb_store.clone(),
            n,
            skew,
            key_length,
            value: workload::value_template(value_size),
            record_size: workload::record_size(key_length, value_size) as u64,
            burst_size,
            write_batch_size,
            batch_interval,
            page_size,
            empty_backoff,
            seed,
            resident: (0..resident_words).map(|_| AtomicU64::new(0)).collect(),
            distinct: AtomicU64::new(0),
            write_records: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            queue_full: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            polls: AtomicU64::new(0),
            read_records: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            empty_reads: AtomicU64::new(0),
            poll_us: Mutex::new(Sketch::with_defaults()),
            write_us: Mutex::new(Sketch::with_defaults()),
        });

        // Spawn both pools. There is no warm-up: the population grows from empty
        // over the whole run, and that growth is exactly what we measure.
        let cancel = CancellationToken::new();
        let mut handles = Vec::with_capacity(write_concurrency + read_concurrency);
        let pace_start = Instant::now();
        for w in 0..write_concurrency {
            handles.push(tokio::spawn(write_worker(
                shared.clone(),
                w,
                pace_start,
                cancel.clone(),
            )));
        }
        for r in 0..read_concurrency {
            handles.push(tokio::spawn(read_worker(shared.clone(), r, cancel.clone())));
        }

        // Progress rows: per-interval rates and cache-independent per-poll cost,
        // keyed by the current distinct-key count (the growth axis).
        println!(
            "  {:>12} {:>10} {:>12} {:>13} {:>11} {:>13} {:>11}",
            "distinct",
            "polls/s",
            "read_rec/s",
            "write_rec/s",
            "gets/poll",
            "segs/poll",
            "read_amp"
        );

        let runner = bench.start();
        let snapshot_dt = Duration::from_millis(snapshot_interval_ms);
        let mut prev = snapshot(&shared);
        let mut prev_t = Instant::now();
        while runner.keep_running() {
            tokio::time::sleep(snapshot_dt).await;
            let cur = snapshot(&shared);
            let dt = prev_t.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
            let d_polls = cur.polls.saturating_sub(prev.polls);
            let per_poll = |x: u64| {
                if d_polls > 0 {
                    x as f64 / d_polls as f64
                } else {
                    0.0
                }
            };
            let d_read_bytes = cur.read_bytes.saturating_sub(prev.read_bytes);
            let read_amp = if d_read_bytes > 0 {
                cur.get_bytes.saturating_sub(prev.get_bytes) as f64 / d_read_bytes as f64
            } else {
                0.0
            };
            println!(
                "  {:>12} {:>10.0} {:>12.0} {:>13.0} {:>11.3} {:>13.3} {:>11.2}",
                cur.distinct,
                d_polls as f64 / dt,
                cur.read_records.saturating_sub(prev.read_records) as f64 / dt,
                cur.write_records.saturating_sub(prev.write_records) as f64 / dt,
                per_poll(cur.gets.saturating_sub(prev.gets)),
                per_poll(cur.segment_scans.saturating_sub(prev.segment_scans)),
                read_amp,
            );
            prev = cur;
            prev_t = Instant::now();
        }
        let elapsed_secs = runner.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);

        cancel.cancel();
        for h in handles {
            h.await??;
        }

        let summary = build_summary(&shared, elapsed_secs, n, skew);
        bench.summarize(summary).await?;

        drop(shared);
        if let Ok(store) = Arc::try_unwrap(logdb_store) {
            store.close().await?;
        }
        bench.close().await?;
        Ok(())
    }
}

fn build_summary(shared: &Shared, elapsed: f64, n: usize, skew: f64) -> Summary {
    let polls = shared.polls.load(Ordering::Relaxed);
    let reads = shared.reads.load(Ordering::Relaxed);
    let read_records = shared.read_records.load(Ordering::Relaxed);
    let read_bytes = shared.read_bytes.load(Ordering::Relaxed);
    let write_records = shared.write_records.load(Ordering::Relaxed);
    let write_bytes = shared.write_bytes.load(Ordering::Relaxed);
    let empty_reads = shared.empty_reads.load(Ordering::Relaxed);
    let distinct = shared.distinct.load(Ordering::Relaxed);
    let gets = common::object_store_gets();
    let get_bytes = common::object_store_get_bytes();

    let per_sec = |x: u64| x as f64 / elapsed;
    let per = |num: u64, den: u64| {
        if den > 0 {
            num as f64 / den as f64
        } else {
            0.0
        }
    };
    let q = |sketch: &Mutex<Sketch>, quantile: f64| {
        sketch.lock().unwrap().quantile(quantile).unwrap_or(0.0)
    };

    Summary::new()
        // Population shape.
        .add("key_population", n as f64)
        .add("zipf_skew", skew)
        .add("distinct_keys_written", distinct as f64)
        .add("distinct_keys_frac", per(distinct, n as u64))
        // Read throughput / cost.
        .add("polls_per_sec", per_sec(polls))
        .add("reads_per_sec", per_sec(reads))
        .add("read_records_per_sec", per_sec(read_records))
        .add("read_bytes_per_sec", per_sec(read_bytes))
        .add("empty_read_frac", per(empty_reads, reads))
        .add("gets_per_poll", per(gets, polls))
        .add("read_amplification", per(get_bytes, read_bytes))
        .add("poll_service_us_p50", q(&shared.poll_us, 0.5))
        .add("poll_service_us_p99", q(&shared.poll_us, 0.99))
        // Write throughput / cost.
        .add("write_records_per_sec", per_sec(write_records))
        .add(
            "write_mb_per_sec",
            write_bytes as f64 / elapsed / 1_000_000.0,
        )
        .add(
            "write_queue_full",
            shared.queue_full.load(Ordering::Relaxed) as f64,
        )
        .add("write_service_us_p50", q(&shared.write_us, 0.5))
        .add("write_service_us_p99", q(&shared.write_us, 0.99))
        .add("elapsed_ms", elapsed * 1000.0)
}

fn object_store_is_shared(storage: &StorageConfig) -> bool {
    match storage {
        StorageConfig::InMemory => false,
        StorageConfig::SlateDb(c) => !matches!(c.object_store, ObjectStoreConfig::InMemory),
    }
}
