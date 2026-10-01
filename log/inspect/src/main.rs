//! `log-inspect` — a small CLI for inspecting an OpenData log via [`LogDbReader`].
//!
//! The tool opens a read-only view of a log and exposes its segments, keys, and
//! entries for ad-hoc inspection. It never writes, so it is safe to point at a
//! log that another process owns.
//!
//! # Storage selection
//!
//! Point the CLI at storage either with a YAML config file describing a full
//! [`ReaderConfig`] (`--config`), or with the convenience flags that build a
//! SlateDB-on-local-filesystem config (`--data-dir`, `--db-path`).
//!
//! # Examples
//!
//! ```text
//! log-inspect --data-dir ./.data segments
//! log-inspect --data-dir ./.data keys
//! log-inspect --data-dir ./.data tree
//! log-inspect --data-dir ./.data tree --segment 1
//! log-inspect --data-dir ./.data count orders
//! log-inspect --data-dir ./.data inspect orders
//! log-inspect --data-dir ./.data scan orders --limit 20
//! log-inspect --config reader.yaml scan orders --from 100 --to 200 --hex
//! ```

use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use common::StorageConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use log::{Inspection, LogDbReader, LogRead, ReaderConfig, SegmentTree, TreeSummary};

/// Inspect an OpenData log through a read-only [`LogDbReader`].
#[derive(Debug, Parser)]
#[command(name = "log-inspect", version, about)]
struct Cli {
    #[command(flatten)]
    storage: StorageArgs,

    #[command(subcommand)]
    command: Command,
}

/// Flags controlling which log the reader opens.
#[derive(Debug, Args)]
struct StorageArgs {
    /// Path to a YAML file describing a full `ReaderConfig`.
    ///
    /// When set, takes precedence over the convenience flags below.
    #[arg(long, value_name = "FILE", global = true)]
    config: Option<String>,

    /// Local filesystem directory backing the object store.
    ///
    /// Used to build a SlateDB-on-local config when `--config` is absent.
    #[arg(long, value_name = "DIR", default_value = ".data", global = true)]
    data_dir: String,

    /// Path prefix for the log's data within the object store.
    #[arg(long, value_name = "PREFIX", default_value = "data", global = true)]
    db_path: String,

    /// How often (ms) the reader polls storage for newly written data.
    #[arg(long, value_name = "MS", default_value_t = 1000, global = true)]
    refresh_interval_ms: u64,
}

impl StorageArgs {
    /// Resolves these flags into a [`ReaderConfig`].
    fn into_reader_config(self) -> Result<ReaderConfig> {
        if let Some(path) = self.config {
            let contents = std::fs::read_to_string(&path)
                .with_context(|| format!("reading config file {path}"))?;
            let config: ReaderConfig = serde_yaml::from_str(&contents)
                .with_context(|| format!("parsing config file {path}"))?;
            return Ok(config);
        }

        let storage = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: self.db_path,
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: self.data_dir,
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        });

        Ok(ReaderConfig {
            storage,
            refresh_interval: Duration::from_millis(self.refresh_interval_ms),
        })
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List segments overlapping a sequence range (default: all).
    Segments {
        /// Lowest sequence number to include.
        #[arg(long)]
        from: Option<u64>,
        /// Exclusive upper bound on sequence number.
        #[arg(long)]
        to: Option<u64>,
    },
    /// List distinct keys within a segment-id range (default: all).
    Keys {
        /// Lowest segment id to include.
        #[arg(long)]
        segment_start: Option<u32>,
        /// Exclusive upper bound on segment id.
        #[arg(long)]
        segment_end: Option<u32>,
    },
    /// Count entries for a key within a sequence range (default: all).
    Count {
        /// The key whose entries to count.
        key: String,
        /// Lowest sequence number to include.
        #[arg(long)]
        from: Option<u64>,
        /// Exclusive upper bound on sequence number.
        #[arg(long)]
        to: Option<u64>,
    },
    /// Inspect how a key's records are distributed across segments and, within
    /// each segment, across the L0 and sorted-run tiers of its LSM tree.
    ///
    /// Reports the total count plus, per covering segment, the records found in
    /// each tier and how localized they are: SSTs holding data vs total SSTs
    /// consulted, and the number of data blocks those records span. Tier stats
    /// are only available on a persistent slatedb backend; an in-memory log
    /// shows `-` (its count comes from a scan, not an SST walk).
    Inspect {
        /// The key whose entries to inspect.
        key: String,
        /// Lowest sequence number to include.
        #[arg(long)]
        from: Option<u64>,
        /// Exclusive upper bound on sequence number.
        #[arg(long)]
        to: Option<u64>,
    },
    /// Summarize how data is distributed across the SlateDB LSM tree.
    ///
    /// With no flags, prints the manifest header and a per-segment
    /// distribution table. Pass `--segment <ID>` to drill into one LogDb
    /// segment's run-by-run breakdown. Reads only manifest metadata, never
    /// SST files.
    Tree {
        /// Show the run-by-run breakdown for a single LogDb segment id
        /// (`0` is the system segment).
        #[arg(long, value_name = "ID")]
        segment: Option<u32>,
    },
    /// Scan and print entries for a key within a sequence range.
    Scan {
        /// The key whose entries to scan.
        key: String,
        /// Lowest sequence number to include.
        #[arg(long)]
        from: Option<u64>,
        /// Exclusive upper bound on sequence number.
        #[arg(long)]
        to: Option<u64>,
        /// Stop after printing this many entries.
        #[arg(long)]
        limit: Option<u64>,
        /// Render values as hex instead of UTF-8 (lossy).
        #[arg(long)]
        hex: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let config = cli.storage.into_reader_config()?;
    let reader = LogDbReader::open(config)
        .await
        .context("opening LogDbReader")?;

    let result = run(&reader, cli.command).await;

    // Always shut down the background refresh task cleanly, even on error.
    reader.close().await;
    result
}

/// Dispatches a single inspection command against an open reader.
async fn run(reader: &LogDbReader, command: Command) -> Result<()> {
    match command {
        Command::Segments { from, to } => segments(reader, from, to).await,
        Command::Keys {
            segment_start,
            segment_end,
        } => keys(reader, segment_start, segment_end).await,
        Command::Tree { segment } => tree(reader, segment).await,
        Command::Count { key, from, to } => count(reader, key, from, to).await,
        Command::Inspect { key, from, to } => inspect(reader, key, from, to).await,
        Command::Scan {
            key,
            from,
            to,
            limit,
            hex,
        } => scan(reader, key, from, to, limit, hex).await,
    }
}

async fn segments(reader: &LogDbReader, from: Option<u64>, to: Option<u64>) -> Result<()> {
    let segments = reader
        .list_segments(seq_range(from, to))
        .await
        .context("listing segments")?;

    if segments.is_empty() {
        println!("(no segments)");
        return Ok(());
    }

    println!("{:>8}  {:>16}  {:>24}", "ID", "START_SEQ", "CREATED");
    for segment in &segments {
        println!(
            "{:>8}  {:>16}  {:>24}",
            segment.id,
            segment.start_seq,
            format_time(segment.start_time_ms),
        );
    }
    println!("\n{} segment(s)", segments.len());
    Ok(())
}

async fn keys(
    reader: &LogDbReader,
    segment_start: Option<u32>,
    segment_end: Option<u32>,
) -> Result<()> {
    let mut iter = reader
        .list_keys(segment_id_range(segment_start, segment_end))
        .await
        .context("listing keys")?;

    let mut count = 0u64;
    while let Some(key) = iter.next().await.context("reading next key")? {
        println!("{}", render(&key.key, false));
        count += 1;
    }

    if count == 0 {
        println!("(no keys)");
    } else {
        println!("\n{count} key(s)");
    }
    Ok(())
}

async fn tree(reader: &LogDbReader, segment: Option<u32>) -> Result<()> {
    let Some(summary) = reader.tree_summary().await.context("summarizing tree")? else {
        println!("(no manifest; the in-memory backend has no LSM tree)");
        return Ok(());
    };

    print_manifest_header(&summary);
    match segment {
        Some(id) => print_segment_detail(&summary, id),
        None => print_segment_table(&summary),
    }
    Ok(())
}

/// Prints the manifest-level header common to both tree views.
fn print_manifest_header(summary: &TreeSummary) {
    println!(
        "manifest #{}  writer_epoch={}  compactor_epoch={}  last_l0_seq={}",
        summary.manifest_id, summary.writer_epoch, summary.compactor_epoch, summary.last_l0_seq,
    );
    println!(
        "extractor: {}",
        summary.extractor.as_deref().unwrap_or("(none)")
    );
    println!(
        "total estimated size: {}",
        format_bytes(summary.estimated_bytes())
    );
    println!();
}

/// Prints one row per segment: L0 SSTs, sorted-run count, total SSTs, and size.
fn print_segment_table(summary: &TreeSummary) {
    // Top-level tree first (usually empty once the extractor routes writes),
    // then segments ordered by id with any undecodable prefixes last.
    let mut rows: Vec<(String, &SegmentTree)> = Vec::new();
    if !summary.unsegmented.is_empty() {
        rows.push(("(top-level)".to_string(), &summary.unsegmented));
    }
    let mut segments: Vec<&SegmentTree> = summary.segments.iter().collect();
    segments.sort_by(|a, b| match (a.segment_id, b.segment_id) {
        (Some(x), Some(y)) => x.cmp(&y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    rows.extend(segments.into_iter().map(|t| (segment_label(t), t)));

    if rows.is_empty() {
        println!("(empty tree; no SSTs)");
        return;
    }

    println!(
        "{:>12}  {:>8}  {:>6}  {:>10}  {:>12}",
        "SEGMENT", "L0_SSTS", "RUNS", "TOTAL_SSTS", "EST_SIZE",
    );
    for (label, t) in &rows {
        println!(
            "{:>12}  {:>8}  {:>6}  {:>10}  {:>12}",
            label,
            t.l0.num_ssts,
            t.runs.len(),
            t.total_ssts(),
            format_bytes(t.estimated_bytes()),
        );
    }
    println!("\n{} segment(s)", rows.len());
}

/// Prints the run-by-run breakdown for a single segment.
fn print_segment_detail(summary: &TreeSummary, id: u32) {
    let Some(t) = summary.segments.iter().find(|t| t.segment_id == Some(id)) else {
        println!("segment {id} not found in manifest");
        return;
    };

    println!(
        "segment {}  (prefix {})",
        segment_label(t),
        render(&t.prefix, true)
    );
    println!("{:>8}  {:>6}  {:>12}", "LEVEL", "SSTS", "EST_SIZE");
    println!(
        "{:>8}  {:>6}  {:>12}",
        "L0",
        t.l0.num_ssts,
        format_bytes(t.l0.estimated_bytes),
    );
    for (i, run) in t.runs.iter().enumerate() {
        println!(
            "{:>8}  {:>6}  {:>12}",
            format!("run[{i}]"),
            run.num_ssts,
            format_bytes(run.estimated_bytes),
        );
    }
    println!(
        "\ntotal: {} ssts, {}",
        t.total_ssts(),
        format_bytes(t.estimated_bytes()),
    );
}

/// Human label for a segment: `system` for id 0, the id for user segments,
/// or the hex prefix when the routing prefix did not decode.
fn segment_label(t: &SegmentTree) -> String {
    match t.segment_id {
        Some(0) => "system".to_string(),
        Some(id) => id.to_string(),
        None => render(&t.prefix, true),
    }
}

/// Formats a byte count as a human-readable size (binary units).
fn format_bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut size = n as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{size:.1} {}", UNITS[unit])
}

async fn count(
    reader: &LogDbReader,
    key: String,
    from: Option<u64>,
    to: Option<u64>,
) -> Result<()> {
    let total = reader
        .count(Bytes::from(key), seq_range(from, to))
        .await
        .context("counting entries")?;
    println!("{total}");
    Ok(())
}

async fn inspect(
    reader: &LogDbReader,
    key: String,
    from: Option<u64>,
    to: Option<u64>,
) -> Result<()> {
    let inspection = reader
        .inspect(Bytes::from(key), seq_range(from, to))
        .await
        .context("inspecting key")?;
    print_inspection(&inspection);
    Ok(())
}

/// Renders an [`Inspection`]: total count, manifest shape, and the
/// per-segment distribution with read-amplification stats.
fn print_inspection(inspection: &Inspection) {
    // Aggregate persisted-SST distribution across all covering segments. The
    // remainder of `total` (not in any persisted SST) was tail-scanned from
    // the memtable / lagging snapshot.
    let agg = &inspection.reads;
    let persisted = agg.l0.records + agg.sorted_runs.records;
    println!("total records: {}", inspection.total);
    println!(
        "persisted: {persisted} (L0 {}, sorted runs {}),  tail-scanned: {}",
        agg.l0.records,
        agg.sorted_runs.records,
        inspection.total.saturating_sub(persisted),
    );
    println!();

    if inspection.segments.is_empty() {
        println!("(no covering segments)");
        return;
    }

    // Per-segment tier distribution. SSTs are shown as with_data/total; BLKS
    // is the number of data blocks the in-range records span (locality).
    println!(
        "{:>7}  {:>7}  {:>6}   {:>9}  {:>7}  {:>6}   {:>7}  {:>9}  {:>7}  {:>6}",
        "SEGMENT",
        "COUNT",
        "TAIL",
        "L0_SSTS",
        "L0_BLKS",
        "L0_REC",
        "SR_RUNS",
        "SR_SSTS",
        "SR_BLKS",
        "SR_REC",
    );
    for seg in &inspection.segments {
        match &seg.reads {
            // SST-walk path: split the segment's records across the tiers.
            Some(r) => println!(
                "{:>7}  {:>7}  {:>6}   {:>9}  {:>7}  {:>6}   {:>7}  {:>9}  {:>7}  {:>6}",
                seg.segment_id,
                seg.count,
                seg.tail_scanned,
                format!("{}/{}", r.l0.ssts_with_data, r.l0.ssts_total),
                r.l0.blocks_with_data,
                r.l0.records,
                r.sorted_runs.runs,
                format!(
                    "{}/{}",
                    r.sorted_runs.ssts_with_data, r.sorted_runs.ssts_total
                ),
                r.sorted_runs.blocks_with_data,
                r.sorted_runs.records,
            ),
            // Scan-fallback path (in-memory backend): no per-tier detail.
            None => println!(
                "{:>7}  {:>7}  {:>6}   {:>9}  {:>7}  {:>6}   {:>7}  {:>9}  {:>7}  {:>6}",
                seg.segment_id, seg.count, seg.tail_scanned, "-", "-", "-", "-", "-", "-", "-",
            ),
        }
    }
    println!("\n{} covering segment(s)", inspection.segments.len());
    println!("(SSTS = with_data/total; BLKS = data blocks the in-range records span)");
}

async fn scan(
    reader: &LogDbReader,
    key: String,
    from: Option<u64>,
    to: Option<u64>,
    limit: Option<u64>,
    hex: bool,
) -> Result<()> {
    let mut iter = reader
        .scan(Bytes::from(key), seq_range(from, to))
        .await
        .context("opening scan")?;

    let mut printed = 0u64;
    while let Some(entry) = iter.next().await.context("reading next entry")? {
        if limit.is_some_and(|n| printed >= n) {
            break;
        }
        println!("{:>16}  {}", entry.sequence, render(&entry.value, hex));
        printed += 1;
    }

    if printed == 0 {
        println!("(no entries)");
    } else {
        println!("\n{printed} entry/entries");
    }
    Ok(())
}

/// Builds an inclusive-lower, exclusive-upper sequence range from optional bounds.
fn seq_range(from: Option<u64>, to: Option<u64>) -> std::ops::Range<u64> {
    from.unwrap_or(0)..to.unwrap_or(u64::MAX)
}

/// Builds a segment-id range from optional bounds.
fn segment_id_range(start: Option<u32>, end: Option<u32>) -> std::ops::Range<u32> {
    start.unwrap_or(0)..end.unwrap_or(u32::MAX)
}

/// Renders a byte string for display: hex when requested, otherwise UTF-8 lossy.
fn render(bytes: &[u8], hex: bool) -> String {
    if hex {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

/// Formats a millisecond epoch timestamp as a human-readable UTC string.
fn format_time(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| format!("{ms} (ms)"))
}
