//! proxy: how many concurrent reader sessions can one reader (a "proxy" to the
//! log) sustain, as a function of the key population `N`?
//!
//! Model: users connect to a proxy, consume a log for a while, and leave. The
//! proxy is a single standalone [`LogDbReader`]. Sessions arrive as an open-loop
//! Poisson stream at `session_rate`; each picks a log by sampling a shared Zipf
//! over `[0, N)` **without regard to occupancy** (so popular logs naturally carry
//! many concurrent sessions), and polls it every `poll_interval` for
//! `session_duration`, then leaves. Concurrent sessions ≈ `session_rate ×
//! session_duration` (Little's law), independent of DB speed.
//!
//! Two consume models (per-session cursors):
//! - **live-tail**: start at the log's current tail (the append frontier) and
//!   read only what arrives during the session — a fresh subscriber.
//! - **persistent**: resume from the log's last-saved position, drain whatever
//!   accumulated, then follow — a durable consumer. The saved position persists
//!   across sessions (read at session start, written back on exit).
//!
//! There is no prefill: a warmup phase with live Zipf writers + sessions builds
//! the population organically, then the measure window records.
//!
//! Capacity is the sustainable concurrent-session occupancy at the **scheduling-
//! lag knee** — sweep `session_rate` up at a fixed `N`; where poll scheduling lag
//! (a cadence poll firing late because the reader can't keep up) and session
//! dispatch lag diverge is the saturation point. Reporting that vs. `N` answers
//! "does one proxy hold the same session count as the log population grows?"

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::bail;
use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use common::storage::config::ObjectStoreConfig;
use common::{GetCounters, StorageConfig, StorageReaderRuntime, create_counted_object_store};
use log::{AppendError, Config, LogDb, LogDbReader, ReaderConfig, Record};
use metrics_util::Summary as Sketch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::read::store::{Cursor, LogDbStore, LogStore, PollOutput};
use crate::workload;
use crate::zipf::{SplitMix64, Zipf};

const ROLE_WRITE: u64 = 0x57; // 'W'
const ROLE_ARRIVAL: u64 = 0x41; // 'A'

const PHASE_WARMUP: u8 = 0;
const PHASE_MEASURE: u8 = 1;
const PHASE_DONE: u8 = 2;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConsumeModel {
    /// Start at the current tail; read only records arriving during the session.
    LiveTail,
    /// Resume from the log's last-saved position, drain the gap, then follow.
    Persistent,
}

/// Which half of the workload this process runs. `Both` is the single-process
/// path (writer + reader in one node); `Writer`/`Reader` split them across nodes
/// of a multi-node deployment that share one object store.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Both,
    Writer,
    Reader,
}

/// The storage handle(s) this process holds, per its [`Role`]. A reader node has
/// no `LogDb` writer (the writer is elsewhere); it polls a standalone reader and
/// seeds live-tail from the reader's own [`LogDbReader::frontier`].
enum Backend {
    Writer(LogDb),
    Reader(LogDbReader),
    Both(LogDbStore),
}

impl Backend {
    /// Append a batch; on accept return the new append frontier, `None` on
    /// `QueueFull` (dropped). Writer/Both only.
    async fn try_append_frontier(&self, records: Vec<Record>) -> anyhow::Result<Option<u64>> {
        match self {
            Backend::Both(store) => store.try_append_frontier(records).await,
            Backend::Writer(db) => {
                let n = records.len() as u64;
                match db.try_append(records).await {
                    Ok(out) => Ok(Some(out.start_sequence + n)),
                    Err(AppendError::QueueFull(_)) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            }
            Backend::Reader(_) => unreachable!("reader role does not write"),
        }
    }

    /// Drain up to `max` records for `key` from `cursor`. Reader/Both only.
    async fn poll(&self, key: Bytes, cursor: Cursor, max: usize) -> anyhow::Result<PollOutput> {
        match self {
            Backend::Both(store) => store.poll(key, cursor, max).await,
            Backend::Reader(r) => LogDbStore::drain(r, key, cursor, max).await,
            Backend::Writer(_) => unreachable!("writer role does not read"),
        }
    }

    /// The reader's visibility frontier (reader role), for seeding live-tail.
    async fn reader_frontier(&self) -> u64 {
        match self {
            Backend::Reader(r) => r.frontier().await,
            _ => 0,
        }
    }

    async fn close(self) -> anyhow::Result<()> {
        match self {
            Backend::Both(store) => store.close().await,
            Backend::Writer(db) => {
                db.close().await?;
                Ok(())
            }
            Backend::Reader(r) => {
                r.close().await;
                Ok(())
            }
        }
    }
}

pub struct ProxyBenchmark;

impl ProxyBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ProxyBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

/// The single fast smoke parameter set; full scenario grids come via `--config`.
/// Requires a shared object store (Local/Aws): the proxy is a standalone reader.
fn smoke_params() -> Params {
    let mut p = Params::new();
    // Role: "both" runs writer + reader in one process (single-node, default);
    // "writer"/"reader" split them across nodes sharing one object store.
    p.insert("role", "both");
    // Population.
    p.insert("key_population", "100000");
    p.insert("zipf_skew", "1.1");
    // Key distribution: "zipf" (realistic) or "uniform" (clean for the partition
    // sweep — P-independent, so only the reader's slice changes).
    p.insert("key_distribution", "zipf");
    p.insert("key_length", "16");
    p.insert("value_size", "128");
    // Write load (held constant; builds the population during warmup + measure).
    p.insert("write_concurrency", "8");
    p.insert("write_mb_per_sec", "10");
    p.insert("burst_size", "8");
    p.insert("write_batch_size", "100");
    // Session model. Two ways to drive occupancy:
    //  - closed-loop: `num_followers > 0` holds exactly that many forever-
    //    followers (occupancy = num_followers); `session_rate`/`session_duration`
    //    are ignored. This is the "how many logs can be followed at once" model.
    //  - open-loop: `num_followers = 0` runs a Poisson arrival stream at
    //    `session_rate`, each session living `session_duration_ms` (occupancy ≈
    //    rate × duration). The churn/consumer model.
    p.insert("num_followers", "2000");
    p.insert("session_rate", "500.0");
    p.insert("session_duration_ms", "1000");
    p.insert("poll_interval_ms", "1000");
    p.insert("consume_model", "live_tail"); // or "persistent"
    p.insert("page_size", "32");
    // Partitioning: split the keyspace into offset-Zipf partitions. 1 = global
    // (unpartitioned). reader_partition "all" = random LB; a number = read one.
    p.insert("num_partitions", "1");
    p.insert("reader_partition", "all");
    // Runaway backstop: cap concurrent sessions (0 = unlimited).
    p.insert("max_inflight", "0");
    // Storage / reader.
    p.insert("seal_interval_ms", "500");
    p.insert("refresh_interval_ms", "1000");
    p.insert("block_cache_mb", "256");
    p.insert("meta_cache_mb", "0");
    p.insert("snapshot_interval_ms", "2000");
    p.insert("warmup_secs", "5");
    p.insert("seed", "1");
    p
}

/// Decorrelate a per-task RNG seed from the run seed, a role tag, and an index.
fn task_seed(run_seed: u64, role: u64, idx: usize) -> u64 {
    let mixed = run_seed
        ^ role.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (idx as u64).wrapping_mul(0xD1B5_4A32_D192_ED03);
    SplitMix64::new(mixed).next_u64()
}

/// Local key distribution within the sampled span.
enum KeyDist {
    /// Skewed — a hot head per span (the realistic/default workload).
    Zipf(Zipf),
    /// Every key equally likely. P-independent, so partitioning only changes the
    /// reader's slice (not the write layout) — the clean choice for isolating the
    /// range-partition / cache-locality effect.
    Uniform,
}

/// Samples a key index, optionally within a partition. The key space `[0, n)` is
/// split into `num_partitions` equal sub-ranges. A `fixed_partition` confines
/// sampling to one partition (a range-partitioned reader); `None` picks a
/// partition uniformly each draw (the writer, and the random-load-balanced
/// reader). With `num_partitions == 1` this is a plain distribution over `[0, n)`
/// (no extra RNG draw — preserves the unpartitioned workload byte-for-byte).
struct KeySampler {
    dist: KeyDist,
    span: usize,
    partition_width: usize,
    num_partitions: usize,
    fixed_partition: Option<usize>,
}

impl KeySampler {
    fn new(
        n: usize,
        num_partitions: usize,
        skew: f64,
        uniform: bool,
        fixed_partition: Option<usize>,
    ) -> Self {
        let num_partitions = num_partitions.max(1);
        let partition_width = (n / num_partitions).max(1);
        let span = if num_partitions <= 1 {
            n
        } else {
            partition_width
        };
        let dist = if uniform {
            KeyDist::Uniform
        } else {
            KeyDist::Zipf(Zipf::new(span, skew))
        };
        Self {
            dist,
            span,
            partition_width,
            num_partitions,
            fixed_partition,
        }
    }

    fn sample(&self, rng: &mut SplitMix64) -> usize {
        let local = match &self.dist {
            KeyDist::Zipf(z) => z.sample(rng),
            KeyDist::Uniform => (rng.next_u64() % self.span as u64) as usize,
        };
        if self.num_partitions <= 1 {
            return local;
        }
        let part = self
            .fixed_partition
            .unwrap_or((rng.next_u64() % self.num_partitions as u64) as usize);
        part * self.partition_width + local
    }
}

struct Shared {
    store: Arc<Backend>,
    // Population / workload.
    n: usize,
    skew: f64,
    /// Uniform key distribution instead of Zipf (P-independent; isolates the
    /// range-partition / cache-locality effect).
    uniform_keys: bool,
    key_length: usize,
    value: Bytes,
    record_size: u64,
    // Write side.
    burst_size: usize,
    write_batch_size: usize,
    batch_interval: Duration,
    /// Append frontier (sequence just past the last record written): the
    /// "now" position a live-tail session starts from.
    append_frontier: AtomicU64,
    write_records: AtomicU64,
    write_bytes: AtomicU64,
    queue_full: AtomicU64,
    // Partitioning: split the key space into `num_partitions` offset-Zipf
    // sub-ranges. Writers cover all of them (uniform); the proxy reads
    // `reader_partition` (Some = range-partitioned, None = all = random LB).
    num_partitions: usize,
    reader_partition: Option<usize>,
    // Read / session side.
    consume_model: ConsumeModel,
    /// Persistent per-log saved position (next sequence), read at session start
    /// and advanced (via `fetch_max`) on exit.
    saved_cursors: Vec<AtomicU64>,
    page_size: usize,
    session_duration: Duration,
    poll_interval: Duration,
    max_inflight: u64,
    // Phase.
    phase: AtomicU8,
    // Occupancy.
    active_sessions: AtomicU64,
    peak_active: AtomicU64,
    // Tallies.
    sessions_completed: AtomicU64,
    refused_sessions: AtomicU64,
    polls: AtomicU64,
    records_consumed: AtomicU64,
    bytes_consumed: AtomicU64,
    // Sketches.
    poll_us: Mutex<Sketch>,
    /// Poll scheduling lag (cadence poll firing late) — the reader-saturation knee.
    /// Cumulative over the run, for the summary percentiles.
    poll_sched_lag_us: Mutex<Sketch>,
    /// Same lag, but rotated each snapshot so the progress rows and the stability
    /// verdict see per-interval lag (flat = steady, rising = diverging) rather
    /// than the inherently-drifting cumulative p99.
    poll_sched_lag_interval: Mutex<Sketch>,
    /// Session dispatch lag (arrival → task start) — the pool-saturation backstop.
    dispatch_lag_us: Mutex<Sketch>,
    /// Records a session consumed (per persistent session, the backlog drained).
    session_records: Mutex<Sketch>,
    /// Per-handle GET counter for the reader's object store, when this role opens
    /// one (Both/Reader). Lets read cost be measured apart from the co-resident
    /// writer's compaction, which also feeds the process-global GET counter.
    reader_get_counters: Option<GetCounters>,
    seed: u64,
}

impl Shared {
    fn recording(&self) -> bool {
        self.phase.load(Ordering::Relaxed) == PHASE_MEASURE
    }

    fn bump_peak(&self, active: u64) {
        let mut peak = self.peak_active.load(Ordering::Relaxed);
        while active > peak {
            match self.peak_active.compare_exchange_weak(
                peak,
                active,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }
}

/// Writer task: Zipf-sample keys, append batched bursts paced to the target rate,
/// and advance the shared append frontier so live-tail sessions know "now".
async fn write_worker(
    shared: Arc<Shared>,
    idx: usize,
    pace_start: Instant,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // Writers cover all partitions (fixed_partition = None).
    let sampler = KeySampler::new(
        shared.n,
        shared.num_partitions,
        shared.skew,
        shared.uniform_keys,
        None,
    );
    let mut rng = SplitMix64::new(task_seed(shared.seed_write(), ROLE_WRITE, idx));
    let keys_per_batch = (shared.write_batch_size / shared.burst_size).max(1);
    let mut batches_done: u32 = 0;
    while !cancel.is_cancelled() {
        let mut batch = Vec::with_capacity(keys_per_batch * shared.burst_size);
        for _ in 0..keys_per_batch {
            let key_idx = sampler.sample(&mut rng);
            let key = workload::key_at(key_idx, shared.key_length);
            for _ in 0..shared.burst_size {
                batch.push(Record {
                    key: key.clone(),
                    value: shared.value.clone(),
                });
            }
        }
        let n_records = batch.len() as u64;

        match shared.store.try_append_frontier(batch).await? {
            Some(frontier) => {
                // The frontier must advance in every phase so live-tail sessions
                // see "now"; the byte/record tallies are measure-only so the
                // achieved write rate divides measure bytes by measure time.
                shared
                    .append_frontier
                    .fetch_max(frontier, Ordering::Relaxed);
                if shared.recording() {
                    shared.write_records.fetch_add(n_records, Ordering::Relaxed);
                    shared
                        .write_bytes
                        .fetch_add(n_records * shared.record_size, Ordering::Relaxed);
                }
            }
            None => {
                if shared.recording() {
                    shared.queue_full.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

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

/// One reader session: poll the chosen log every `poll_interval` for
/// `session_duration`, draining up to `page_size` records per poll. The cadence
/// schedule is fixed (`start + k·interval`), so a poll firing late — because the
/// reader can't service the offered poll load — shows up as poll scheduling lag.
async fn run_session(
    shared: Arc<Shared>,
    key_idx: usize,
    phase_offset: Duration,
    cancel: CancellationToken,
) {
    // Stagger the poll schedule by a phase offset so a synchronously-spawned
    // closed-loop pool doesn't poll in lockstep (a thundering herd that would
    // spike scheduling lag); offsets spread the C polls evenly across the cadence.
    if !phase_offset.is_zero() {
        tokio::select! {
            _ = tokio::time::sleep(phase_offset) => {}
            _ = cancel.cancelled() => return,
        }
    }

    let key = workload::key_at(key_idx, shared.key_length);
    let mut cursor = match shared.consume_model {
        ConsumeModel::LiveTail => Cursor(shared.append_frontier.load(Ordering::Relaxed)),
        ConsumeModel::Persistent => Cursor(shared.saved_cursors[key_idx].load(Ordering::Relaxed)),
    };

    let start = Instant::now();
    let mut session_records = 0u64;
    let mut k: u32 = 0;
    loop {
        if start.elapsed() >= shared.session_duration || cancel.is_cancelled() {
            break;
        }
        // Fixed cadence: the k-th poll is due at start + k·interval. Lag past that
        // due time is the saturation signal.
        if !shared.poll_interval.is_zero() {
            let due = start + shared.poll_interval * k;
            let now = Instant::now();
            if due > now {
                tokio::time::sleep(due - now).await;
            }
            if shared.recording() {
                let lag_us = Instant::now().saturating_duration_since(due).as_micros() as f64;
                shared.poll_sched_lag_us.lock().unwrap().add(lag_us);
                shared.poll_sched_lag_interval.lock().unwrap().add(lag_us);
            }
        }

        let t0 = Instant::now();
        let out = match shared
            .store
            .poll(key.clone(), cursor, shared.page_size)
            .await
        {
            Ok(out) => out,
            Err(_) => break,
        };
        let svc = t0.elapsed().as_micros() as f64;

        if shared.recording() {
            shared.polls.fetch_add(1, Ordering::Relaxed);
            shared
                .records_consumed
                .fetch_add(out.n_records as u64, Ordering::Relaxed);
            shared
                .bytes_consumed
                .fetch_add(out.n_bytes as u64, Ordering::Relaxed);
            shared.poll_us.lock().unwrap().add(svc);
        }
        session_records += out.n_records as u64;
        cursor = out.cursor;
        k += 1;
    }

    if shared.consume_model == ConsumeModel::Persistent {
        shared.saved_cursors[key_idx].fetch_max(cursor.0, Ordering::Relaxed);
    }
    if shared.recording() {
        shared
            .session_records
            .lock()
            .unwrap()
            .add(session_records as f64);
        shared.sessions_completed.fetch_add(1, Ordering::Relaxed);
    }
    let active = shared.active_sessions.fetch_sub(1, Ordering::Relaxed) - 1;
    let _ = active;
}

/// Arrival generator: an open-loop Poisson stream at `session_rate`. Each arrival
/// samples a log by Zipf and spawns a session; dispatch lag (scheduled → started)
/// is the pool-saturation backstop.
async fn run_arrivals(shared: Arc<Shared>, session_rate: f64, cancel: CancellationToken) {
    let sampler = KeySampler::new(
        shared.n,
        shared.num_partitions,
        shared.skew,
        shared.uniform_keys,
        shared.reader_partition,
    );
    let mut rng = SplitMix64::new(task_seed(shared.seed_arrival(), ROLE_ARRIVAL, 0));
    let t0 = Instant::now();
    let mut next_secs = 0.0f64;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        next_secs += rng.exp_interval_secs(session_rate);
        let due = t0 + Duration::from_secs_f64(next_secs);
        tokio::select! {
            _ = tokio::time::sleep_until(due) => {}
            _ = cancel.cancelled() => break,
        }

        // Runaway backstop: refuse arrivals beyond the inflight cap.
        let active = shared.active_sessions.load(Ordering::Relaxed);
        if shared.max_inflight > 0 && active >= shared.max_inflight {
            if shared.recording() {
                shared.refused_sessions.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }

        let key_idx = sampler.sample(&mut rng);
        if shared.recording() {
            let lag = Instant::now().saturating_duration_since(due);
            shared
                .dispatch_lag_us
                .lock()
                .unwrap()
                .add(lag.as_micros() as f64);
        }
        let active = shared.active_sessions.fetch_add(1, Ordering::Relaxed) + 1;
        shared.bump_peak(active);
        let sh = shared.clone();
        let c = cancel.clone();
        tokio::spawn(async move {
            // Poisson arrivals are already staggered, so no phase offset.
            run_session(sh, key_idx, Duration::ZERO, c).await;
        });
    }
}

impl Shared {
    // Seeds are derived from one stored run seed; split so writers and arrivals
    // draw independent streams while staying reproducible.
    fn seed_write(&self) -> u64 {
        self.seed
    }
    fn seed_arrival(&self) -> u64 {
        self.seed ^ 0xABCD_EF01_2345_6789
    }
}

#[async_trait::async_trait]
impl Benchmark for ProxyBenchmark {
    fn name(&self) -> &str {
        "proxy"
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
        let burst_size: usize = params.get_parse::<usize>("burst_size")?.max(1);
        let write_batch_size: usize = params
            .get_parse::<usize>("write_batch_size")?
            .max(burst_size);
        let write_mb_per_sec: f64 = params.get_parse("write_mb_per_sec")?;
        let num_followers: usize = params.get_parse("num_followers")?;
        let closed_loop = num_followers > 0;
        let session_rate: f64 = params.get_parse("session_rate")?;
        // Closed-loop followers run for the whole measure window; open-loop
        // sessions live for the configured duration.
        let session_duration = if closed_loop {
            Duration::MAX
        } else {
            Duration::from_millis(params.get_parse::<u64>("session_duration_ms")?.max(1))
        };
        let poll_interval = Duration::from_millis(params.get_parse("poll_interval_ms")?);
        let page_size: usize = params.get_parse::<usize>("page_size")?.max(1);
        let max_inflight: u64 = params.get_parse("max_inflight")?;
        let consume_model = match params.get("consume_model").unwrap_or("live_tail") {
            "live_tail" => ConsumeModel::LiveTail,
            "persistent" => ConsumeModel::Persistent,
            other => {
                bail!("unknown consume_model '{other}' (expected 'live_tail' or 'persistent')")
            }
        };
        let warmup_secs: f64 = params.get_parse("warmup_secs")?;
        let snapshot_interval_ms: u64 = params.get_parse::<u64>("snapshot_interval_ms")?.max(1);
        let seed: u64 = params.get_parse("seed")?;

        let role = match params.get("role").unwrap_or("both") {
            "both" => Role::Both,
            "writer" => Role::Writer,
            "reader" => Role::Reader,
            other => bail!("unknown role '{other}' (expected 'both', 'writer', or 'reader')"),
        };
        let do_write = matches!(role, Role::Both | Role::Writer);
        let do_read = matches!(role, Role::Both | Role::Reader);

        // Key distribution within a span: zipf (default, realistic) or uniform
        // (P-independent — the clean choice for the partition/locality sweep).
        let uniform_keys = match params.get("key_distribution").unwrap_or("zipf") {
            "zipf" => false,
            "uniform" => true,
            other => bail!("unknown key_distribution '{other}' (expected 'zipf' or 'uniform')"),
        };
        // Partitioning. `reader_partition = "all"` (default) reads the whole
        // keyspace (random LB); a number confines the proxy to that partition.
        let num_partitions: usize = params.get_parse::<usize>("num_partitions")?.max(1);
        let reader_partition = match params.get("reader_partition").unwrap_or("all") {
            "all" => None,
            v => {
                let p: usize = v.parse()?;
                if p >= num_partitions {
                    bail!("reader_partition {p} out of range for num_partitions {num_partitions}");
                }
                Some(p)
            }
        };

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
                "proxy requires a shared object store (Local or Aws); the configured store is \
                 in-memory and per-handle, so the standalone reader (the proxy) would observe \
                 none of the writer's data"
            );
        }
        // Safe after the check above: a shared store is always SlateDb-backed.
        let object_store_config = match &storage_config {
            StorageConfig::SlateDb(c) => c.object_store.clone(),
            StorageConfig::InMemory => unreachable!("object_store_is_shared rejected InMemory"),
        };

        // Open only the handle(s) this role needs — a reader node never opens a
        // `LogDb` writer (that would fence the real writer on another node).
        let open_writer = || async {
            let mut config = Config {
                storage: storage_config.clone(),
                ..Default::default()
            };
            config.segmentation.seal_interval = seal_interval;
            LogDb::open(config).await
        };
        // Give the reader its OWN object store carrying a dedicated GET counter,
        // injected via the runtime. `object_store_get_bytes` is process-global,
        // so in `role=both` it also tallies the writer's compaction GETs; this
        // handle measures the reader's object-store reads in isolation (the
        // read_amplification / locality signal).
        let open_reader = || async {
            let reader_config = ReaderConfig {
                storage: storage_config.clone(),
                refresh_interval: Duration::from_millis(refresh_interval_ms),
            };
            let (object_store, get_counters) = create_counted_object_store(&object_store_config)?;
            let mut runtime = StorageReaderRuntime::new().with_object_store(object_store);
            if block_cache_mb > 0 {
                runtime = runtime
                    .with_block_cache(common::create_in_memory_block_cache(block_cache_mb << 20));
            }
            if meta_cache_mb > 0 {
                runtime = runtime
                    .with_meta_cache(common::create_in_memory_block_cache(meta_cache_mb << 20));
            }
            let reader = LogDbReader::open_with_runtime(reader_config, runtime).await?;
            Ok::<_, anyhow::Error>((reader, get_counters))
        };
        let (backend, reader_get_counters) = match role {
            Role::Both => {
                let writer = open_writer().await?;
                let (reader, counters) = open_reader().await?;
                (
                    Backend::Both(LogDbStore::with_reader(writer, reader)),
                    Some(counters),
                )
            }
            Role::Writer => (Backend::Writer(open_writer().await?), None),
            Role::Reader => {
                let (reader, counters) = open_reader().await?;
                (Backend::Reader(reader), Some(counters))
            }
        };
        let store = Arc::new(backend);

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

        let saved_cursors = match consume_model {
            ConsumeModel::Persistent => (0..n).map(|_| AtomicU64::new(0)).collect(),
            ConsumeModel::LiveTail => Vec::new(),
        };

        let shared = Arc::new(Shared {
            store: store.clone(),
            n,
            skew,
            uniform_keys,
            key_length,
            value: workload::value_template(value_size),
            record_size: record_size as u64,
            burst_size,
            write_batch_size,
            batch_interval,
            append_frontier: AtomicU64::new(0),
            write_records: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            queue_full: AtomicU64::new(0),
            num_partitions,
            reader_partition,
            consume_model,
            saved_cursors,
            page_size,
            session_duration,
            poll_interval,
            max_inflight,
            phase: AtomicU8::new(PHASE_WARMUP),
            active_sessions: AtomicU64::new(0),
            peak_active: AtomicU64::new(0),
            sessions_completed: AtomicU64::new(0),
            refused_sessions: AtomicU64::new(0),
            polls: AtomicU64::new(0),
            records_consumed: AtomicU64::new(0),
            bytes_consumed: AtomicU64::new(0),
            poll_us: Mutex::new(Sketch::with_defaults()),
            poll_sched_lag_us: Mutex::new(Sketch::with_defaults()),
            poll_sched_lag_interval: Mutex::new(Sketch::with_defaults()),
            dispatch_lag_us: Mutex::new(Sketch::with_defaults()),
            session_records: Mutex::new(Sketch::with_defaults()),
            reader_get_counters,
            seed,
        });

        // Spawn writers + the arrival generator. Warmup first (metrics discarded)
        // so the population and caches reach steady state, then measure.
        let cancel = CancellationToken::new();
        let pace_start = Instant::now();
        let mut writers = Vec::new();
        if do_write {
            for w in 0..write_concurrency {
                writers.push(tokio::spawn(write_worker(
                    shared.clone(),
                    w,
                    pace_start,
                    cancel.clone(),
                )));
            }
        }
        // Reader node: there is no in-process writer publishing the frontier, so
        // seed live-tail "now" from the reader's own visibility frontier. Warmup
        // then absorbs the (bounded, ~seal-interval) seed gap before measuring.
        if matches!(role, Role::Reader) {
            let f = shared.store.reader_frontier().await;
            shared.append_frontier.store(f, Ordering::Relaxed);
        }
        // Closed-loop: spawn exactly `num_followers` forever-followers, each on a
        // Zipf-sampled log — occupancy is fixed at C, the directly-swept "how many
        // logs followed at once". Open-loop: run the Poisson arrival generator.
        let mut followers = Vec::new();
        let arrivals = if !do_read {
            None
        } else if closed_loop {
            let mut rng = SplitMix64::new(task_seed(shared.seed_arrival(), ROLE_ARRIVAL, 0));
            let sampler = KeySampler::new(
                n,
                shared.num_partitions,
                skew,
                shared.uniform_keys,
                shared.reader_partition,
            );
            for i in 0..num_followers {
                let key_idx = sampler.sample(&mut rng);
                // Even phase offset across the cadence so polls don't herd.
                let phase = poll_interval.mul_f64(i as f64 / num_followers as f64);
                let active = shared.active_sessions.fetch_add(1, Ordering::Relaxed) + 1;
                shared.bump_peak(active);
                let sh = shared.clone();
                let c = cancel.clone();
                followers.push(tokio::spawn(async move {
                    run_session(sh, key_idx, phase, c).await;
                }));
            }
            None
        } else {
            Some(tokio::spawn(run_arrivals(
                shared.clone(),
                session_rate,
                cancel.clone(),
            )))
        };

        tokio::time::sleep(Duration::from_secs_f64(warmup_secs)).await;

        // Measure. Lag columns are per-interval: p50 is the baseline the verdict
        // tracks, p99 the tail (spiky from periodic refresh).
        println!(
            "  {:>9} {:>10} {:>11} {:>11} {:>13} {:>13}",
            "occ", "sess/s", "polls/s", "rec/s", "pollag_p50us", "pollag_p99us"
        );
        shared.phase.store(PHASE_MEASURE, Ordering::Relaxed);
        // Snapshot process-wide cost counters at measure start; the deltas over
        // the window are the read cost. (These are global, so they include the
        // writers' compaction GETs, but the polling followers dominate GET volume.)
        let gets_at = common::object_store_gets();
        let get_bytes_at = common::object_store_get_bytes();
        // Reader-only GET tallies (None for writer-only role; falls back to the
        // global below, which for a writer process is its own GETs anyway).
        let (reader_gets_at, reader_get_bytes_at) = match &shared.reader_get_counters {
            Some(c) => (c.gets(), c.get_bytes()),
            None => (0, 0),
        };
        let segment_scans_at = log::segment_scans();
        let runner = bench.start();
        let snapshot_dt = Duration::from_millis(snapshot_interval_ms);
        let (mut prev_polls, mut prev_sess, mut prev_rec) = (0u64, 0u64, 0u64);
        let mut prev_t = Instant::now();
        // Time-integral of active sessions, so mean occupancy is robust for both
        // closed-loop (forever-followers never "complete") and open-loop.
        let mut occ_integral = 0.0f64;
        let mut total_dt = 0.0f64;
        // Per-snapshot cumulative poll-scheduling-lag p99, for the stability
        // verdict: a steady run holds ~flat, a saturated one keeps climbing.
        let mut lag_traj: Vec<f64> = Vec::new();
        while runner.keep_running() {
            tokio::time::sleep(snapshot_dt).await;
            let dt = prev_t.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
            let active = shared.active_sessions.load(Ordering::Relaxed);
            occ_integral += active as f64 * dt;
            total_dt += dt;
            let polls = shared.polls.load(Ordering::Relaxed);
            let sess = shared.sessions_completed.load(Ordering::Relaxed);
            let rec = shared.records_consumed.load(Ordering::Relaxed);
            // Per-interval lag: read this window's p50 and p99, then reset so the
            // next snapshot reflects only its own interval (no cumulative drift).
            // The verdict tracks p50 (the baseline — flat when healthy, climbs
            // only under real saturation); p99 is shown for tail visibility but is
            // noisy (periodic refresh spikes), so it's a poor divergence signal.
            let (lag_p50, lag_p99) = {
                let mut g = shared.poll_sched_lag_interval.lock().unwrap();
                let p50 = g.quantile(0.5).unwrap_or(0.0);
                let p99 = g.quantile(0.99).unwrap_or(0.0);
                *g = Sketch::with_defaults();
                (p50, p99)
            };
            lag_traj.push(lag_p50);
            println!(
                "  {:>9} {:>10.0} {:>11.0} {:>11.0} {:>13.0} {:>13.0}",
                active,
                (sess - prev_sess) as f64 / dt,
                (polls - prev_polls) as f64 / dt,
                (rec - prev_rec) as f64 / dt,
                lag_p50,
                lag_p99,
            );
            prev_polls = polls;
            prev_sess = sess;
            prev_rec = rec;
            prev_t = Instant::now();
        }
        let elapsed = runner.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
        let gets_total = common::object_store_gets().saturating_sub(gets_at);
        let get_bytes_total = common::object_store_get_bytes().saturating_sub(get_bytes_at);
        // Reader-only GETs over the window; fall back to the process global when
        // there's no dedicated reader handle (writer-only role).
        let (reader_gets_total, reader_get_bytes_total) = match &shared.reader_get_counters {
            Some(c) => (
                c.gets().saturating_sub(reader_gets_at),
                c.get_bytes().saturating_sub(reader_get_bytes_at),
            ),
            None => (gets_total, get_bytes_total),
        };
        let segment_scans_total = log::segment_scans().saturating_sub(segment_scans_at);
        let mean_occupancy = if total_dt > 0.0 {
            occ_integral / total_dt
        } else {
            0.0
        };
        shared.phase.store(PHASE_DONE, Ordering::Relaxed);

        // Stability verdict from the per-interval lag trajectory: compare the
        // lag around the run's midpoint to its end. A steady run holds ~flat
        // (ratio ≈ 1); a diverging one climbs. Both endpoints are averaged over a
        // window to smooth snapshot noise, and the midpoint (not the start) is the
        // baseline so the cold-start ramp is excluded. Emitted as a parseable line
        // so the limit-search runner can classify each point.
        let mean = |s: &[f64]| {
            if s.is_empty() {
                0.0
            } else {
                s.iter().sum::<f64>() / s.len() as f64
            }
        };
        let (lag_mid, lag_end) = if lag_traj.is_empty() {
            (0.0, 0.0)
        } else {
            let n = lag_traj.len();
            let win = (n / 4).max(1);
            // Average a window centered on the midpoint, and one at the tail.
            let mid_lo = (n / 2).saturating_sub(win / 2);
            let mid = mean(&lag_traj[mid_lo..(mid_lo + win).min(n)]);
            let end = mean(&lag_traj[n - win..]);
            (mid, end)
        };
        let lag_ratio = if lag_mid > 0.0 {
            lag_end / lag_mid
        } else {
            0.0
        };
        println!(
            "STABILITY poll_sched_lag_us_mid={:.0} poll_sched_lag_us_end={:.0} ratio={:.2}",
            lag_mid, lag_end, lag_ratio
        );

        cancel.cancel();
        for w in writers {
            w.await??;
        }
        if let Some(arrivals) = arrivals {
            arrivals.await?;
        }
        for f in followers {
            f.await?;
        }

        let summary = build_summary(
            &shared,
            elapsed,
            mean_occupancy,
            lag_mid,
            lag_end,
            get_bytes_total,
            reader_gets_total,
            reader_get_bytes_total,
            segment_scans_total,
            n,
            skew,
            session_rate,
        );
        bench.summarize(summary).await?;

        drop(shared);
        if let Ok(store) = Arc::try_unwrap(store) {
            store.close().await?;
        }
        bench.close().await?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn build_summary(
    shared: &Shared,
    elapsed: f64,
    mean_occupancy: f64,
    lag_mid: f64,
    lag_end: f64,
    get_bytes_total: u64,
    reader_gets_total: u64,
    reader_get_bytes_total: u64,
    segment_scans_total: u64,
    n: usize,
    skew: f64,
    session_rate: f64,
) -> Summary {
    let sessions = shared.sessions_completed.load(Ordering::Relaxed);
    let polls = shared.polls.load(Ordering::Relaxed);
    let records = shared.records_consumed.load(Ordering::Relaxed);
    let bytes = shared.bytes_consumed.load(Ordering::Relaxed);
    let write_bytes = shared.write_bytes.load(Ordering::Relaxed);

    let per_sec = |x: u64| x as f64 / elapsed;
    let q = |s: &Mutex<Sketch>, p: f64| s.lock().unwrap().quantile(p).unwrap_or(0.0);

    let model = match shared.consume_model {
        ConsumeModel::LiveTail => 0.0,
        ConsumeModel::Persistent => 1.0,
    };

    Summary::new()
        .add("key_population", n as f64)
        .add("zipf_skew", skew)
        .add("num_partitions", shared.num_partitions as f64)
        .add(
            "reader_partition",
            shared.reader_partition.map(|p| p as f64).unwrap_or(-1.0),
        )
        .add("consume_model_persistent", model)
        .add("offered_session_rate", session_rate)
        // Occupancy (the headline "concurrent sessions").
        .add("mean_occupancy", mean_occupancy)
        .add(
            "peak_occupancy",
            shared.peak_active.load(Ordering::Relaxed) as f64,
        )
        .add("sessions_per_sec", per_sec(sessions))
        .add(
            "refused_sessions",
            shared.refused_sessions.load(Ordering::Relaxed) as f64,
        )
        // Saturation signals (the knee).
        .add("poll_sched_lag_us_p50", q(&shared.poll_sched_lag_us, 0.5))
        .add("poll_sched_lag_us_p99", q(&shared.poll_sched_lag_us, 0.99))
        // Stability: cumulative lag p99 at the run's midpoint vs. end. A steady
        // run holds the ratio near 1; a diverging one climbs well above it.
        .add("poll_sched_lag_us_mid", lag_mid)
        .add("poll_sched_lag_us_end", lag_end)
        .add("dispatch_lag_us_p50", q(&shared.dispatch_lag_us, 0.5))
        .add("dispatch_lag_us_p99", q(&shared.dispatch_lag_us, 0.99))
        // Read throughput / cost.
        .add("polls_per_sec", per_sec(polls))
        .add("records_per_sec", per_sec(records))
        .add("read_bytes_per_sec", per_sec(bytes))
        .add("poll_service_us_p50", q(&shared.poll_us, 0.5))
        .add("poll_service_us_p99", q(&shared.poll_us, 0.99))
        // Object-store read cost (the price of cardinality). These use the
        // READER's own GET counter, so in role=both they exclude the writer's
        // compaction GETs (object_store_get_bytes is process-global and would
        // otherwise swamp the reader's reads). read_amplification = bytes the
        // reader fetched per byte delivered to followers; gets and segment scans
        // are normalized per poll.
        .add("object_store_get_bytes_per_sec", per_sec(reader_get_bytes_total))
        .add(
            "read_amplification",
            if bytes > 0 {
                reader_get_bytes_total as f64 / bytes as f64
            } else {
                0.0
            },
        )
        .add(
            "gets_per_poll",
            if polls > 0 {
                reader_gets_total as f64 / polls as f64
            } else {
                0.0
            },
        )
        // The rest of the process-global GETs: in role=both this is the writer's
        // compaction; in an isolated reader/writer process it's ~0.
        .add(
            "compaction_get_bytes_per_sec",
            per_sec(get_bytes_total.saturating_sub(reader_get_bytes_total)),
        )
        .add(
            "segments_per_poll",
            if polls > 0 {
                segment_scans_total as f64 / polls as f64
            } else {
                0.0
            },
        )
        // Per-session consumption (persistent: the backlog drained).
        .add("session_records_p50", q(&shared.session_records, 0.5))
        .add("session_records_max", q(&shared.session_records, 1.0))
        // Write load (held constant).
        .add(
            "write_mb_per_sec",
            write_bytes as f64 / elapsed / 1_000_000.0,
        )
        .add(
            "write_queue_full",
            shared.queue_full.load(Ordering::Relaxed) as f64,
        )
        .add("elapsed_ms", elapsed * 1000.0)
}

fn object_store_is_shared(storage: &StorageConfig) -> bool {
    match storage {
        StorageConfig::InMemory => false,
        StorageConfig::SlateDb(c) => !matches!(c.object_store, ObjectStoreConfig::InMemory),
    }
}
