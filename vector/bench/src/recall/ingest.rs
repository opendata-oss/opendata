//! INGEST phase: ingest a dataset's base vectors into a fresh `VectorDb`.
//!
//! Opens a fresh `VectorDb` at phase start, streams the base file in chunks,
//! writes vectors in fixed-size batches, then flushes and closes the db.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use tokio::sync::oneshot;
use vector::{Vector, VectorDb};

use crate::recall::{Dataset, ingest_default_memory_bytes, open_db};

const BASE_VECTOR_CHUNK_SIZE: usize = 15_000;
const INGEST_WRITE_BATCH_SIZE: usize = 10;

/// Metrics produced by the ingest phase.
pub struct IngestSummary {
    pub num_vectors: u64,
    pub ingest_secs: f64,
}

pub async fn run(
    dataset: &Dataset,
    data_dir: &std::path::Path,
    config: &vector::Config,
) -> anyhow::Result<IngestSummary> {
    let db = open_db(config, dataset, ingest_default_memory_bytes()).await?;
    let summary = ingest(dataset, data_dir, &db).await?;
    db.close().await?;
    Ok(summary)
}

async fn ingest(
    dataset: &Dataset,
    data_dir: &std::path::Path,
    db: &VectorDb,
) -> anyhow::Result<IngestSummary> {
    let num_vectors = dataset.estimated_base_vector_count(data_dir)? as u64;
    println!(
        "  Streaming {} base vectors for {} (dim={}) in chunks of {}",
        num_vectors, dataset.name, dataset.dimensions, BASE_VECTOR_CHUNK_SIZE
    );
    let (mut stream, reader_thread) =
        dataset.spawn_base_vector_stream(data_dir, BASE_VECTOR_CHUNK_SIZE)?;
    let Some(first_message) = stream.recv().await else {
        anyhow::bail!("base vector stream closed before yielding any data");
    };
    let Some(mut base_vectors) = first_message? else {
        anyhow::bail!("dataset {} has no base vectors to ingest", dataset.name);
    };

    let ingest_start = Instant::now();
    let ingested_counter = Arc::new(AtomicU64::new(0));
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let monitor_handle =
        spawn_ingest_throughput_monitor(ingested_counter.clone(), ingest_start, stop_rx);

    let num_batches = (num_vectors as usize).div_ceil(INGEST_WRITE_BATCH_SIZE);
    let mut batch_idx = 0usize;
    let mut vector_offset = 0usize;
    loop {
        println!(
            "  Loaded chunk: {} vectors ({} / {})",
            base_vectors.len(),
            vector_offset + base_vectors.len(),
            num_vectors
        );
        for (chunk_idx, chunk) in base_vectors.chunks(INGEST_WRITE_BATCH_SIZE).enumerate() {
            let batch: Vec<Vector> = chunk
                .iter()
                .enumerate()
                .map(|(i, values)| {
                    let index = vector_offset + chunk_idx * INGEST_WRITE_BATCH_SIZE + i;
                    Vector::new(index.to_string(), values.clone())
                })
                .collect();
            let written = batch.len() as u64;
            db.write(batch).await?;
            ingested_counter.fetch_add(written, Ordering::Relaxed);
            batch_idx += 1;
            if batch_idx.is_multiple_of(10_000) {
                println!("  Written batch {}/{}", batch_idx, num_batches);
            }
        }
        vector_offset += base_vectors.len();
        let Some(message) = stream.recv().await else {
            break;
        };
        let Some(next_batch) = message? else {
            break;
        };
        base_vectors = next_batch;
    }
    reader_thread
        .join()
        .map_err(|_| anyhow::anyhow!("base vector streaming thread panicked"))?;
    if vector_offset != num_vectors as usize {
        anyhow::bail!(
            "streamed {} vectors but expected {}",
            vector_offset,
            num_vectors
        );
    }
    db.flush().await?;
    let ingest_secs = ingest_start.elapsed().as_secs_f64();

    let _ = stop_tx.send(());
    let throughput_table = monitor_handle
        .await
        .map_err(|e| anyhow::anyhow!("ingest throughput monitor panicked: {e}"))?;
    print_ingest_throughput_table(&throughput_table);

    println!(
        "  Ingested {} vectors in {:.1}s ({:.0} vec/s)",
        num_vectors,
        ingest_secs,
        num_vectors as f64 / ingest_secs,
    );
    println!("  Num centroids: {}", db.num_centroids());

    Ok(IngestSummary {
        num_vectors,
        ingest_secs,
    })
}

/// One sample produced by the ingest throughput monitor.
struct IngestThroughputSample {
    wall_time: DateTime<Local>,
    vec_per_sec: f64,
    window_secs: f64,
}

/// Spawn a task that samples the ingest counter once per minute and reports
/// the throughput over the preceding 5 minutes (or shorter, if ingest has
/// been running for less than 5 minutes).
fn spawn_ingest_throughput_monitor(
    counter: Arc<AtomicU64>,
    ingest_start: Instant,
    mut stop_rx: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<Vec<IngestThroughputSample>> {
    const SAMPLE_PERIOD: Duration = Duration::from_secs(60);
    const WINDOW: Duration = Duration::from_secs(300);

    tokio::spawn(async move {
        let mut history: Vec<(Instant, u64)> = vec![(ingest_start, 0)];
        let mut samples: Vec<IngestThroughputSample> = Vec::new();
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + SAMPLE_PERIOD, SAMPLE_PERIOD);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Instant::now();
                    let count = counter.load(Ordering::Relaxed);
                    history.push((now, count));

                    let cutoff = now.checked_sub(WINDOW);
                    let baseline = match cutoff {
                        Some(cutoff) => history
                            .iter()
                            .find(|(t, _)| *t >= cutoff)
                            .copied()
                            .unwrap_or(history[0]),
                        None => history[0],
                    };
                    let window_secs = now.duration_since(baseline.0).as_secs_f64();
                    let delta = count.saturating_sub(baseline.1);
                    let vec_per_sec = if window_secs > 0.0 {
                        delta as f64 / window_secs
                    } else {
                        0.0
                    };
                    let wall_time = Local::now();

                    println!(
                        "  [ingest throughput] {} | {:>10.1} vec/s (preceding {:.0}s, {} vectors)",
                        wall_time.format("%Y-%m-%d %H:%M:%S"),
                        vec_per_sec,
                        window_secs,
                        delta,
                    );

                    samples.push(IngestThroughputSample {
                        wall_time,
                        vec_per_sec,
                        window_secs,
                    });

                    if let Some(cutoff) = cutoff {
                        let mut keep_from = 0;
                        for (i, (t, _)) in history.iter().enumerate() {
                            if *t >= cutoff {
                                keep_from = i.saturating_sub(1);
                                break;
                            }
                        }
                        if keep_from > 0 {
                            history.drain(0..keep_from);
                        }
                    }
                }
                _ = &mut stop_rx => break,
            }
        }
        samples
    })
}

fn print_ingest_throughput_table(samples: &[IngestThroughputSample]) {
    if samples.is_empty() {
        println!("\n  Ingest throughput table: (no samples — ingest finished in under 60s)");
        return;
    }
    println!("\n  Ingest throughput (5-minute trailing window, sampled every minute):");
    println!("  | timestamp           | throughput (vectors/sec) | window (s) |");
    println!("  |---------------------|--------------------------|------------|");
    for s in samples {
        println!(
            "  | {} | {:>24.1} | {:>10.0} |",
            s.wall_time.format("%Y-%m-%d %H:%M:%S"),
            s.vec_per_sec,
            s.window_secs,
        );
    }
}
