//! INGEST phase: stream the synthetic corpus into a fresh `VectorDb`.
//!
//! Opens a fresh `VectorDb` at phase start, generates documents on a
//! background thread in fixed-size chunks, writes them in small batches,
//! then flushes and closes the db. Mirrors the recall benchmark's ingest
//! phase, including a once-per-minute throughput log line.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Local;
use tokio::sync::{mpsc, oneshot};
use vector::{Vector, VectorDb};

use crate::fts::corpus::{CorpusGenerator, Doc};
use crate::fts::{BODY_FIELD, FtsSpec, open_db};
use crate::recall::ingest_default_memory_bytes;

const DOC_CHUNK_SIZE: usize = 10_000;
const INGEST_WRITE_BATCH_SIZE: usize = 10;

/// Metrics produced by the ingest phase.
pub struct IngestSummary {
    pub num_docs: u64,
    pub ingest_secs: f64,
}

pub async fn run(spec: &FtsSpec, config: &vector::Config) -> anyhow::Result<IngestSummary> {
    let db = open_db(config, spec, ingest_default_memory_bytes()).await?;
    let summary = ingest(spec, &db).await?;
    db.close().await?;
    Ok(summary)
}

async fn ingest(spec: &FtsSpec, db: &VectorDb) -> anyhow::Result<IngestSummary> {
    let num_docs = spec.num_docs as u64;
    println!(
        "  Streaming {} synthetic docs (vocab={}, zipf_s={}, avg_len={}, dim={}) in chunks of {}",
        num_docs, spec.vocab_size, spec.zipf_s, spec.avg_doc_len, spec.dimensions, DOC_CHUNK_SIZE
    );
    let (mut stream, gen_thread) = spawn_doc_stream(spec, DOC_CHUNK_SIZE);

    let ingest_start = Instant::now();
    let ingested_counter = Arc::new(AtomicU64::new(0));
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let monitor_handle =
        spawn_ingest_throughput_monitor(ingested_counter.clone(), ingest_start, stop_rx);

    let num_batches = spec.num_docs.div_ceil(INGEST_WRITE_BATCH_SIZE);
    let mut batch_idx = 0usize;
    let mut docs_written = 0usize;
    while let Some(chunk) = stream.recv().await {
        docs_written += chunk.len();
        println!(
            "  Generated chunk: {} docs ({} / {})",
            chunk.len(),
            docs_written,
            num_docs
        );
        let mut batch: Vec<Vector> = Vec::with_capacity(INGEST_WRITE_BATCH_SIZE);
        for doc in chunk {
            batch.push(
                Vector::builder(doc.id, doc.embedding)
                    .attribute(BODY_FIELD, doc.body)
                    .build(),
            );
            if batch.len() == INGEST_WRITE_BATCH_SIZE {
                let written = batch.len() as u64;
                db.write(std::mem::take(&mut batch)).await?;
                ingested_counter.fetch_add(written, Ordering::Relaxed);
                batch_idx += 1;
                if batch_idx.is_multiple_of(10_000) {
                    println!("  Written batch {}/{}", batch_idx, num_batches);
                }
            }
        }
        // Final partial batch of the corpus (chunks are multiples of the
        // batch size except possibly the last).
        if !batch.is_empty() {
            let written = batch.len() as u64;
            db.write(batch).await?;
            ingested_counter.fetch_add(written, Ordering::Relaxed);
            batch_idx += 1;
        }
    }
    gen_thread
        .join()
        .map_err(|_| anyhow::anyhow!("doc generation thread panicked"))?;
    if docs_written != spec.num_docs {
        anyhow::bail!("generated {} docs but expected {}", docs_written, num_docs);
    }
    db.flush().await?;
    let ingest_secs = ingest_start.elapsed().as_secs_f64();

    let _ = stop_tx.send(());
    monitor_handle
        .await
        .map_err(|e| anyhow::anyhow!("ingest throughput monitor panicked: {e}"))?;

    println!(
        "  Ingested {} docs in {:.1}s ({:.0} docs/s)",
        num_docs,
        ingest_secs,
        num_docs as f64 / ingest_secs,
    );

    Ok(IngestSummary {
        num_docs,
        ingest_secs,
    })
}

/// Spawn a thread that generates corpus chunks and streams them through a
/// bounded channel, so generation overlaps with writes without ever
/// materializing the whole corpus.
fn spawn_doc_stream(
    spec: &FtsSpec,
    chunk_size: usize,
) -> (mpsc::Receiver<Vec<Doc>>, thread::JoinHandle<()>) {
    let mut generator = CorpusGenerator::new(spec);
    let (tx, rx) = mpsc::channel(1);

    let handle = thread::spawn(move || {
        while let Some(chunk) = generator.next_chunk(chunk_size) {
            if tx.blocking_send(chunk).is_err() {
                return;
            }
        }
    });

    (rx, handle)
}

/// Spawn a task that samples the ingest counter once per minute and logs
/// the throughput since the previous sample.
fn spawn_ingest_throughput_monitor(
    counter: Arc<AtomicU64>,
    ingest_start: Instant,
    mut stop_rx: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    const SAMPLE_PERIOD: Duration = Duration::from_secs(60);

    tokio::spawn(async move {
        let mut last = (ingest_start, 0u64);
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + SAMPLE_PERIOD, SAMPLE_PERIOD);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Instant::now();
                    let count = counter.load(Ordering::Relaxed);
                    let window_secs = now.duration_since(last.0).as_secs_f64();
                    let delta = count.saturating_sub(last.1);
                    let docs_per_sec = if window_secs > 0.0 {
                        delta as f64 / window_secs
                    } else {
                        0.0
                    };
                    println!(
                        "  [ingest throughput] {} | {:>10.1} docs/s (preceding {:.0}s, {} docs, {} total)",
                        Local::now().format("%Y-%m-%d %H:%M:%S"),
                        docs_per_sec,
                        window_secs,
                        delta,
                        count,
                    );
                    last = (now, count);
                }
                _ = &mut stop_rx => break,
            }
        }
    })
}
