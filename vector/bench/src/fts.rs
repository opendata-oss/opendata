//! Full-text-search (BM25) latency benchmark for the vector database.
//!
//! Ingests a deterministic synthetic corpus (Zipf-distributed vocabulary,
//! log-normal-ish document lengths) and measures sequential BM25 query
//! latency for each configured scorer (`block_max`, `exhaustive`),
//! cross-checking that the scorers return identical result lists.
//!
//! Splits work into two phases — `INGEST` and `WARM` — configured via the
//! `phases` param. Each phase opens its own fresh `VectorDb` and closes it
//! before the next phase starts, mirroring the recall benchmark.

mod corpus;
mod ingest;
mod warm;

use std::sync::Arc;

use bencher::{Bench, Benchmark, Params, Summary};
use common::StorageBuilder;
use common::storage::factory::{DbCache, FoyerCache, FoyerCacheOptions};
use vector::{Bm25Scorer, Config, DistanceMetric, FieldType, MetadataFieldSpec, VectorDb};

/// Name of the text metadata field that holds each document's body.
pub(crate) const BODY_FIELD: &str = "body";

const DEFAULT_SEED: u64 = 42;
const DEFAULT_NUM_DOCS: usize = 100_000;
const DEFAULT_VOCAB_SIZE: usize = 200_000;
const DEFAULT_ZIPF_S: f64 = 1.07;
const DEFAULT_AVG_DOC_LEN: f64 = 100.0;
const DEFAULT_DIMENSIONS: u16 = 8;
const DEFAULT_NUM_QUERIES: usize = 200;
const DEFAULT_LIMIT: usize = 10;

/// Default phase list when the params don't specify otherwise.
const DEFAULT_PHASES: &[Phase] = &[Phase::Ingest, Phase::Warm];

/// Default scorer list when the params don't specify otherwise.
const DEFAULT_SCORERS: &[Bm25Scorer] = &[Bm25Scorer::BlockMax, Bm25Scorer::Exhaustive];

// -- Phases -------------------------------------------------------------------

/// A single phase of the FTS benchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Ingest the synthetic corpus into a fresh `VectorDb`.
    Ingest,
    /// Run the per-scorer warmup + measured query passes.
    Warm,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::Ingest => "INGEST",
            Phase::Warm => "WARM",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "INGEST" => Phase::Ingest,
            "WARM" => Phase::Warm,
            _ => panic!("unknown phase: {}", s),
        }
    }
}

fn param_to_phases(s: &str) -> Vec<Phase> {
    s.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(Phase::parse)
        .collect()
}

// -- Scorers ------------------------------------------------------------------

pub(crate) fn scorer_name(scorer: Bm25Scorer) -> &'static str {
    match scorer {
        Bm25Scorer::BlockMax => "block_max",
        Bm25Scorer::BlockMaxSparse => "block_max_sparse",
        Bm25Scorer::Exhaustive => "exhaustive",
    }
}

fn parse_scorer(s: &str) -> Bm25Scorer {
    match s {
        "block_max" => Bm25Scorer::BlockMax,
        "block_max_sparse" => Bm25Scorer::BlockMaxSparse,
        "exhaustive" => Bm25Scorer::Exhaustive,
        _ => panic!("unknown scorer: {}", s),
    }
}

fn param_to_scorers(s: &str) -> Vec<Bm25Scorer> {
    s.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(parse_scorer)
        .collect()
}

// -- Spec ---------------------------------------------------------------------

/// FTS benchmark spec, parsed from `[[params.fts]]`. All values are strings
/// in the bencher config and fall back to the defaults above when unset.
#[derive(Clone, Debug)]
pub(crate) struct FtsSpec {
    /// Seed for the deterministic corpus and query generators.
    pub seed: u64,
    /// Number of synthetic documents to ingest.
    pub num_docs: usize,
    /// Vocabulary size (term ranks `t0..t<vocab_size-1>`).
    pub vocab_size: usize,
    /// Zipf exponent for the term-frequency distribution.
    pub zipf_s: f64,
    /// Target mean document length in terms.
    pub avg_doc_len: f64,
    /// Embedding dimensionality (the embeddings are filler; BM25 queries
    /// don't use them, but every vector must carry one).
    pub dimensions: u16,
    /// Number of BM25 queries per scorer pass.
    pub num_queries: usize,
    /// Result limit (top-k) for each query.
    pub limit: usize,
    /// In-memory block-cache size in bytes. Same semantics as the recall
    /// benchmark: `None` = phase default, negative = disabled, `n >= 0` =
    /// exactly `n` bytes.
    pub block_cache_bytes: Option<i64>,
    /// Scorers to measure during the warm phase, in order.
    pub scorers: Vec<Bm25Scorer>,
    /// Phases to run, in order.
    pub phases: Vec<Phase>,
}

impl From<Params> for FtsSpec {
    fn from(p: Params) -> Self {
        FtsSpec {
            seed: p.get_parse("seed").ok().unwrap_or(DEFAULT_SEED),
            num_docs: p.get_parse("num_docs").ok().unwrap_or(DEFAULT_NUM_DOCS),
            vocab_size: p.get_parse("vocab_size").ok().unwrap_or(DEFAULT_VOCAB_SIZE),
            zipf_s: p.get_parse("zipf_s").ok().unwrap_or(DEFAULT_ZIPF_S),
            avg_doc_len: p
                .get_parse("avg_doc_len")
                .ok()
                .unwrap_or(DEFAULT_AVG_DOC_LEN),
            dimensions: p.get_parse("dimensions").ok().unwrap_or(DEFAULT_DIMENSIONS),
            num_queries: p
                .get_parse("num_queries")
                .ok()
                .unwrap_or(DEFAULT_NUM_QUERIES),
            limit: p.get_parse("limit").ok().unwrap_or(DEFAULT_LIMIT),
            block_cache_bytes: p.get_parse::<i64>("block_cache_bytes").ok(),
            scorers: p
                .get("scorers")
                .map(param_to_scorers)
                .unwrap_or_else(|| DEFAULT_SCORERS.to_vec()),
            phases: p
                .get("phases")
                .map(param_to_phases)
                .unwrap_or_else(|| DEFAULT_PHASES.to_vec()),
        }
    }
}

// -- Storage helpers ------------------------------------------------------------

/// Resolve the effective memory-tier byte count for the block cache, with
/// the same semantics as the recall benchmark: `None` → use
/// `default_memory_bytes`, negative → disabled, `n >= 0` → exactly `n`.
fn resolve_block_cache_memory(configured: Option<i64>, default_memory_bytes: u64) -> Option<u64> {
    match configured {
        None => Some(default_memory_bytes),
        Some(n) if n < 0 => None,
        Some(n) => Some(n as u64),
    }
}

/// Open a fresh `VectorDb` for the given `Config`, layering in a memory-only
/// foyer block cache sized by the spec's `block_cache_bytes` (or
/// `default_memory_bytes` when unset — see the recall benchmark's
/// `ingest_default_memory_bytes` / `warm_default_memory_bytes`).
pub(crate) async fn open_db(
    config: &Config,
    spec: &FtsSpec,
    default_memory_bytes: u64,
) -> anyhow::Result<VectorDb> {
    let mut sb = StorageBuilder::new(&config.storage).await?;
    match resolve_block_cache_memory(spec.block_cache_bytes, default_memory_bytes) {
        Some(memory_bytes) if memory_bytes > 0 => {
            println!("  Block cache: memory-only, {} bytes", memory_bytes);
            let cache = FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: memory_bytes,
                ..Default::default()
            });
            let cache = Arc::new(cache) as Arc<dyn DbCache>;
            sb = sb.map_slatedb(move |db| db.with_db_cache(cache));
        }
        _ => println!("  Block cache: disabled"),
    }
    Ok(VectorDb::open_with_storage(config.clone(), sb).await?)
}

// -- Benchmark ----------------------------------------------------------------

pub struct FtsBenchmark;

impl FtsBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FtsBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Benchmark for FtsBenchmark {
    fn name(&self) -> &str {
        "fts"
    }

    fn default_params(&self) -> Vec<Params> {
        // All spec fields have defaults, so an empty param set is a valid run.
        vec![Params::new()]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let spec: FtsSpec = bench.spec().params().clone().into();
        println!("  Spec: {:?}", spec);

        // BM25 queries run against the `body` text field; the field is not
        // indexed for metadata filtering.
        let config = Config {
            storage: bench.spec().data().storage.clone(),
            dimensions: spec.dimensions,
            distance_metric: DistanceMetric::L2,
            metadata_fields: vec![MetadataFieldSpec::new(BODY_FIELD, FieldType::Text, false)],
            ..Default::default()
        };

        // Queries are derived from the seed alone, so WARM-only runs against
        // a previously ingested corpus issue the same queries.
        let queries = corpus::generate_queries(spec.seed, spec.vocab_size, spec.num_queries);
        println!("  Generated {} queries", queries.len());

        // -- Dispatch each configured phase --------------------------------
        let mut summary = Summary::new()
            .add("num_docs", spec.num_docs as f64)
            .add("num_queries", spec.num_queries as f64)
            .add("limit", spec.limit as f64);
        for phase in spec.phases.iter().copied() {
            println!("\n=== phase: {} ===", phase.as_str());
            match phase {
                Phase::Ingest => {
                    let s = ingest::run(&spec, &config).await?;
                    summary = summary
                        .add("ingest_secs", s.ingest_secs)
                        .add("ingest_docs_per_sec", s.num_docs as f64 / s.ingest_secs);
                }
                Phase::Warm => {
                    let s = warm::run(&spec, &config, &queries, &bench).await?;
                    for (name, sc) in &s.scorers {
                        summary = summary
                            .add(format!("{}_p50_latency_us", name), sc.p50)
                            .add(format!("{}_p90_latency_us", name), sc.p90)
                            .add(format!("{}_p99_latency_us", name), sc.p99)
                            .add(format!("{}_mean_latency_us", name), sc.mean)
                            .add(format!("{}_qps", name), sc.qps);
                    }
                    summary = summary.add("result_mismatches", s.result_mismatches as f64);
                }
            }
        }

        bench.summarize(summary).await?;
        bench.close().await?;
        Ok(())
    }
}
