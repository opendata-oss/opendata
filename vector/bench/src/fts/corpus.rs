//! Corpus and query loading for the FTS benchmark.
//!
//! The benchmark supports the original deterministic synthetic corpus and
//! MS MARCO passage-ranking files. Synthetic data is generated from the spec's
//! seed. MS MARCO data is streamed from `collection.tsv`, so large corpora do
//! not need to be materialized in memory before ingest.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::thread;

use anyhow::Context;
use tokio::sync::mpsc;

use crate::fts::{FtsDataset, FtsSpec};

/// Salt XOR'd into the seed for the query generator so queries don't reuse
/// the corpus generator's random stream.
const QUERY_SEED_SALT: u64 = 0x9E3779B97F4A7C15;

/// Minimum / maximum document length in terms.
const MIN_DOC_LEN: usize = 5;
const MAX_DOC_LEN: usize = 2000;

const DEFAULT_MSMARCO_CORPUS_FILE: &str = "msmarco/collection.tsv";
const DEFAULT_MSMARCO_QUERIES_FILE: &str = "msmarco/queries.dev.small.tsv";

/// A generated or loaded document: external id, a filler embedding, and the
/// text body.
pub(crate) struct Doc {
    pub id: String,
    pub embedding: Vec<f32>,
    pub body: String,
}

// -- RNG ----------------------------------------------------------------------

/// splitmix64 (Vigna). Tiny, fast, and good enough for synthetic data.
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// Uniform f64 in [0, 1) with 53 bits of precision.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform f32 in [0, 1) with 24 bits of precision.
    pub fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u32 << 24) as f32
    }

    /// Uniform usize in [lo, hi). Modulo bias is negligible at benchmark
    /// range sizes.
    pub fn next_range(&mut self, lo: usize, hi: usize) -> usize {
        debug_assert!(lo < hi);
        lo + (self.next_u64() % (hi - lo) as u64) as usize
    }
}

// -- Zipf sampling --------------------------------------------------------------

/// Precomputed normalized cumulative Zipf distribution over term ranks.
/// Sampling is a binary search on a uniform f64.
pub(crate) struct ZipfTable {
    cdf: Vec<f64>,
}

impl ZipfTable {
    pub fn new(vocab_size: usize, s: f64) -> Self {
        assert!(vocab_size > 0, "vocab_size must be positive");
        let mut cdf = Vec::with_capacity(vocab_size);
        let mut acc = 0.0;
        for rank in 0..vocab_size {
            acc += 1.0 / ((rank + 1) as f64).powf(s);
            cdf.push(acc);
        }
        let total = acc;
        for v in &mut cdf {
            *v /= total;
        }
        Self { cdf }
    }

    /// Sample a term rank.
    pub fn sample(&self, rng: &mut SplitMix64) -> usize {
        let u = rng.next_f64();
        // First rank whose cumulative probability covers `u`.
        self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1)
    }
}

// -- Corpus generation ------------------------------------------------------------

/// Streaming generator for the configured corpus. Documents are produced
/// sequentially, so chunking affects only how much is materialized at once.
enum CorpusGenerator {
    Synthetic(SyntheticCorpusGenerator),
    MsMarco(MsMarcoCorpusGenerator),
}

impl CorpusGenerator {
    pub fn new(spec: &FtsSpec) -> anyhow::Result<Self> {
        match spec.dataset {
            FtsDataset::Synthetic => Ok(Self::Synthetic(SyntheticCorpusGenerator::new(spec))),
            FtsDataset::MsMarcoPassage => Ok(Self::MsMarco(MsMarcoCorpusGenerator::open(spec)?)),
        }
    }

    /// Generate or load the next chunk of up to `max_docs` documents, or
    /// `None` when the corpus is exhausted.
    pub fn next_chunk(&mut self, max_docs: usize) -> anyhow::Result<Option<Vec<Doc>>> {
        match self {
            Self::Synthetic(generator) => Ok(generator.next_chunk(max_docs)),
            Self::MsMarco(generator) => generator.next_chunk(max_docs),
        }
    }
}

/// Streaming generator for the synthetic corpus.
struct SyntheticCorpusGenerator {
    rng: SplitMix64,
    zipf: ZipfTable,
    avg_doc_len: f64,
    dimensions: usize,
    num_docs: usize,
    next_doc: usize,
}

impl SyntheticCorpusGenerator {
    fn new(spec: &FtsSpec) -> Self {
        Self {
            rng: SplitMix64::new(spec.seed),
            zipf: ZipfTable::new(spec.vocab_size, spec.zipf_s),
            avg_doc_len: spec.avg_doc_len,
            dimensions: spec.dimensions as usize,
            num_docs: spec.num_docs,
            next_doc: 0,
        }
    }

    fn next_chunk(&mut self, max_docs: usize) -> Option<Vec<Doc>> {
        if self.next_doc >= self.num_docs {
            return None;
        }
        let count = max_docs.min(self.num_docs - self.next_doc);
        let mut docs = Vec::with_capacity(count);
        for _ in 0..count {
            docs.push(self.next_doc());
        }
        Some(docs)
    }

    fn next_doc(&mut self) -> Doc {
        let id = format!("d{}", self.next_doc);
        self.next_doc += 1;

        let embedding: Vec<f32> = (0..self.dimensions).map(|_| self.rng.next_f32()).collect();

        use std::fmt::Write;
        let len = self.doc_len();
        let mut body = String::with_capacity(len * 7);
        for i in 0..len {
            if i > 0 {
                body.push(' ');
            }
            let _ = write!(body, "t{}", self.zipf.sample(&mut self.rng));
        }

        Doc {
            id,
            embedding,
            body,
        }
    }

    /// Log-normal-ish document length: `n` approximates a standard normal
    /// via the sum of 12 uniforms minus 6 (Irwin–Hall).
    fn doc_len(&mut self) -> usize {
        let n: f64 = (0..12).map(|_| self.rng.next_f64()).sum::<f64>() - 6.0;
        let len = (self.avg_doc_len * (0.5 * n).exp()).round();
        (len as usize).clamp(MIN_DOC_LEN, MAX_DOC_LEN)
    }
}

struct MsMarcoCorpusGenerator {
    reader: BufReader<File>,
    rng: SplitMix64,
    dimensions: usize,
    remaining: usize,
    line_number: usize,
}

impl MsMarcoCorpusGenerator {
    fn open(spec: &FtsSpec) -> anyhow::Result<Self> {
        let corpus_path = resolve_data_path(
            spec,
            spec.corpus_file.as_deref(),
            DEFAULT_MSMARCO_CORPUS_FILE,
        );
        let file = File::open(&corpus_path)
            .with_context(|| format!("failed to open MS MARCO corpus {}", corpus_path.display()))?;
        Ok(Self {
            reader: BufReader::new(file),
            rng: SplitMix64::new(spec.seed),
            dimensions: spec.dimensions as usize,
            remaining: spec.num_docs,
            line_number: 0,
        })
    }

    fn next_chunk(&mut self, max_docs: usize) -> anyhow::Result<Option<Vec<Doc>>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let count = max_docs.min(self.remaining);
        let mut docs = Vec::with_capacity(count);
        while docs.len() < count {
            let Some(doc) = self.next_doc()? else {
                break;
            };
            docs.push(doc);
        }
        if docs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(docs))
        }
    }

    fn next_doc(&mut self) -> anyhow::Result<Option<Doc>> {
        let mut line = String::new();
        loop {
            line.clear();
            let bytes = self.reader.read_line(&mut line)?;
            if bytes == 0 {
                return Ok(None);
            }
            self.line_number += 1;
            let line = line.trim_end_matches(&['\r', '\n'][..]);
            if line.is_empty() {
                continue;
            }
            let mut parts = line.splitn(2, '\t');
            let id = parts.next().unwrap_or_default();
            let Some(body) = parts.next() else {
                anyhow::bail!(
                    "invalid MS MARCO collection row {}: expected '<pid>\t<passage>'",
                    self.line_number
                );
            };
            self.remaining -= 1;
            let embedding: Vec<f32> = (0..self.dimensions).map(|_| self.rng.next_f32()).collect();
            return Ok(Some(Doc {
                id: id.to_string(),
                embedding,
                body: body.to_string(),
            }));
        }
    }
}

/// Spawn a thread that streams corpus chunks through a bounded channel, so
/// generation/file I/O overlaps with writes without materializing the whole
/// corpus.
pub(crate) fn spawn_doc_stream(
    spec: FtsSpec,
    chunk_size: usize,
) -> (
    mpsc::Receiver<anyhow::Result<Vec<Doc>>>,
    thread::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel(1);
    let handle = thread::spawn(move || {
        let mut generator = match CorpusGenerator::new(&spec) {
            Ok(generator) => generator,
            Err(err) => {
                let _ = tx.blocking_send(Err(err));
                return;
            }
        };
        loop {
            match generator.next_chunk(chunk_size) {
                Ok(Some(chunk)) => {
                    if tx.blocking_send(Ok(chunk)).is_err() {
                        return;
                    }
                }
                Ok(None) => return,
                Err(err) => {
                    let _ = tx.blocking_send(Err(err));
                    return;
                }
            }
        }
    });
    (rx, handle)
}

pub(crate) fn corpus_description(spec: &FtsSpec) -> String {
    match spec.dataset {
        FtsDataset::Synthetic => format!(
            "synthetic docs (vocab={}, zipf_s={}, avg_len={}, dim={})",
            spec.vocab_size, spec.zipf_s, spec.avg_doc_len, spec.dimensions
        ),
        FtsDataset::MsMarcoPassage => {
            let corpus_path = resolve_data_path(
                spec,
                spec.corpus_file.as_deref(),
                DEFAULT_MSMARCO_CORPUS_FILE,
            );
            format!(
                "MS MARCO passage docs from {} (dim={})",
                corpus_path.display(),
                spec.dimensions
            )
        }
    }
}

pub(crate) fn load_queries(spec: &FtsSpec) -> anyhow::Result<Vec<String>> {
    match spec.dataset {
        FtsDataset::Synthetic => Ok(generate_queries(
            spec.seed,
            spec.vocab_size,
            spec.num_queries,
        )),
        FtsDataset::MsMarcoPassage => load_msmarco_queries(spec),
    }
}

fn load_msmarco_queries(spec: &FtsSpec) -> anyhow::Result<Vec<String>> {
    let path = resolve_data_path(
        spec,
        spec.queries_file.as_deref(),
        DEFAULT_MSMARCO_QUERIES_FILE,
    );
    let file = File::open(&path)
        .with_context(|| format!("failed to open MS MARCO queries {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut queries = Vec::with_capacity(spec.num_queries);
    for (line_idx, line) in reader.lines().enumerate() {
        if queries.len() == spec.num_queries {
            break;
        }
        let line = line?;
        let line = line.trim_end_matches(&['\r', '\n'][..]);
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let _qid = parts.next();
        let Some(query) = parts.next() else {
            anyhow::bail!(
                "invalid MS MARCO query row {}: expected '<qid>\t<query>'",
                line_idx + 1
            );
        };
        if !query.trim().is_empty() {
            queries.push(query.to_string());
        }
    }
    if queries.is_empty() {
        anyhow::bail!("MS MARCO query file {} produced no queries", path.display());
    }
    if queries.len() < spec.num_queries {
        println!(
            "  Query file {} has {} usable queries; requested {}",
            path.display(),
            queries.len(),
            spec.num_queries
        );
    }
    Ok(queries)
}

fn resolve_data_path(spec: &FtsSpec, configured: Option<&str>, default_relative: &str) -> PathBuf {
    let path = PathBuf::from(configured.unwrap_or(default_relative));
    if path.is_absolute() {
        path
    } else {
        data_dir(spec).join(path)
    }
}

fn data_dir(spec: &FtsSpec) -> PathBuf {
    spec.data_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("data"))
}

// -- Query generation -------------------------------------------------------------

/// Generate the benchmark's BM25 query strings. Queries are assigned to
/// strata round-robin by index so any prefix keeps roughly the configured
/// 40/30/30 mix:
///
/// - 40%: 2 terms — one head rank [0, 100), one torso rank [1000, 10000)
/// - 30%: 3 terms — head + torso + tail rank [10000, vocab)
/// - 30%: 2 terms — both mid ranks [1000, 50000)
///
/// All stratum bounds are clamped to the vocabulary.
pub(crate) fn generate_queries(seed: u64, vocab_size: usize, num_queries: usize) -> Vec<String> {
    let mut rng = SplitMix64::new(seed ^ QUERY_SEED_SALT);
    (0..num_queries)
        .map(|i| {
            let ranks = match i % 10 {
                0..=3 => vec![
                    rank_in(&mut rng, 0, 100, vocab_size),
                    rank_in(&mut rng, 1000, 10_000, vocab_size),
                ],
                4..=6 => vec![
                    rank_in(&mut rng, 0, 100, vocab_size),
                    rank_in(&mut rng, 1000, 10_000, vocab_size),
                    rank_in(&mut rng, 10_000, vocab_size, vocab_size),
                ],
                _ => vec![
                    rank_in(&mut rng, 1000, 50_000, vocab_size),
                    rank_in(&mut rng, 1000, 50_000, vocab_size),
                ],
            };
            let terms: Vec<String> = ranks.iter().map(|r| format!("t{}", r)).collect();
            terms.join(" ")
        })
        .collect()
}

/// Uniform term rank in [lo, hi), with the stratum clamped to the
/// vocabulary while keeping at least one valid rank.
fn rank_in(rng: &mut SplitMix64, lo: usize, hi: usize, vocab_size: usize) -> usize {
    let hi = hi.min(vocab_size).max(1);
    let lo = lo.min(hi - 1);
    rng.next_range(lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_spec() -> FtsSpec {
        FtsSpec {
            dataset: FtsDataset::Synthetic,
            seed: 42,
            num_docs: 100,
            vocab_size: 20_000,
            zipf_s: 1.07,
            avg_doc_len: 100.0,
            dimensions: 8,
            num_queries: 50,
            limit: 10,
            block_cache_bytes: None,
            data_dir: None,
            corpus_file: None,
            queries_file: None,
            scorers: Vec::new(),
            phases: Vec::new(),
        }
    }

    #[test]
    fn corpus_should_be_deterministic_and_chunking_invariant() {
        let spec = test_spec();
        let mut all: Vec<Doc> = Vec::new();
        let mut generator = CorpusGenerator::new(&spec).unwrap();
        while let Some(chunk) = generator.next_chunk(7).unwrap() {
            all.extend(chunk);
        }
        assert_eq!(all.len(), spec.num_docs);

        let again = CorpusGenerator::new(&spec)
            .unwrap()
            .next_chunk(1000)
            .unwrap()
            .unwrap();
        assert_eq!(again.len(), spec.num_docs);
        for (a, b) in all.iter().zip(again.iter()) {
            assert_eq!(a.id, b.id);
            assert_eq!(a.embedding, b.embedding);
            assert_eq!(a.body, b.body);
        }
    }

    #[test]
    fn doc_lengths_should_be_clamped() {
        let spec = test_spec();
        let mut generator = CorpusGenerator::new(&spec).unwrap();
        for doc in generator.next_chunk(100).unwrap().unwrap() {
            let len = doc.body.split(' ').count();
            assert!((MIN_DOC_LEN..=MAX_DOC_LEN).contains(&len));
        }
    }

    #[test]
    fn queries_should_be_deterministic_and_in_vocab() {
        let a = generate_queries(42, 20_000, 100);
        let b = generate_queries(42, 20_000, 100);
        assert_eq!(a, b);
        for q in &a {
            for term in q.split(' ') {
                let rank: usize = term.strip_prefix('t').unwrap().parse().unwrap();
                assert!(rank < 20_000);
            }
        }
    }

    #[test]
    fn msmarco_should_load_queries_and_docs() {
        // given
        let root =
            std::env::temp_dir().join(format!("opendata-msmarco-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("msmarco")).unwrap();
        std::fs::write(
            root.join("msmarco/collection.tsv"),
            "10\tfirst passage text\n11\tsecond passage text\n",
        )
        .unwrap();
        std::fs::write(
            root.join("msmarco/queries.dev.small.tsv"),
            "1\twhat is first\n2\twhat is second\n",
        )
        .unwrap();
        let mut spec = test_spec();
        spec.dataset = FtsDataset::MsMarcoPassage;
        spec.data_dir = Some(root.to_string_lossy().to_string());
        spec.num_docs = 2;
        spec.num_queries = 2;

        // when
        let queries = load_queries(&spec).unwrap();
        let mut generator = CorpusGenerator::new(&spec).unwrap();
        let docs = generator.next_chunk(10).unwrap().unwrap();

        // then
        assert_eq!(queries, vec!["what is first", "what is second"]);
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, "10");
        assert_eq!(docs[0].body, "first passage text");
        assert_eq!(docs[0].embedding.len(), spec.dimensions as usize);
        assert_eq!(docs[1].id, "11");
        let _ = std::fs::remove_dir_all(&root);
    }
}
