//! Deterministic synthetic corpus and query generation for the FTS
//! benchmark.
//!
//! Everything here is derived from the spec's `seed` with a hand-rolled
//! splitmix64 generator (no RNG dependency), so the same spec always
//! produces the same corpus and queries:
//!
//! - **Terms** follow a Zipf distribution over `vocab_size` ranks with
//!   exponent `zipf_s`; rank `r` renders as the token `t<r>`.
//! - **Document lengths** are log-normal-ish: a standard normal is
//!   approximated by summing 12 uniforms (Irwin–Hall) and the length is
//!   `clamp(round(avg_doc_len * exp(0.5 * n)), 5, 2000)`.
//! - **Queries** mix three strata of head/torso/tail term ranks.

use crate::fts::FtsSpec;

/// Salt XOR'd into the seed for the query generator so queries don't reuse
/// the corpus generator's random stream.
const QUERY_SEED_SALT: u64 = 0x9E3779B97F4A7C15;

/// Minimum / maximum document length in terms.
const MIN_DOC_LEN: usize = 5;
const MAX_DOC_LEN: usize = 2000;

/// A generated document: external id `d<i>`, a filler embedding, and the
/// space-joined term body.
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

/// Streaming generator for the synthetic corpus. Documents are produced
/// sequentially from a single random stream, so chunking does not affect
/// the generated corpus — only how much is materialized at once.
pub(crate) struct CorpusGenerator {
    rng: SplitMix64,
    zipf: ZipfTable,
    avg_doc_len: f64,
    dimensions: usize,
    num_docs: usize,
    next_doc: usize,
}

impl CorpusGenerator {
    pub fn new(spec: &FtsSpec) -> Self {
        Self {
            rng: SplitMix64::new(spec.seed),
            zipf: ZipfTable::new(spec.vocab_size, spec.zipf_s),
            avg_doc_len: spec.avg_doc_len,
            dimensions: spec.dimensions as usize,
            num_docs: spec.num_docs,
            next_doc: 0,
        }
    }

    /// Generate the next chunk of up to `max_docs` documents, or `None`
    /// when the corpus is exhausted.
    pub fn next_chunk(&mut self, max_docs: usize) -> Option<Vec<Doc>> {
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
            seed: 42,
            num_docs: 100,
            vocab_size: 20_000,
            zipf_s: 1.07,
            avg_doc_len: 100.0,
            dimensions: 8,
            num_queries: 50,
            limit: 10,
            block_cache_bytes: None,
            scorers: Vec::new(),
            phases: Vec::new(),
        }
    }

    #[test]
    fn corpus_should_be_deterministic_and_chunking_invariant() {
        let spec = test_spec();
        let mut all: Vec<Doc> = Vec::new();
        let mut generator = CorpusGenerator::new(&spec);
        while let Some(chunk) = generator.next_chunk(7) {
            all.extend(chunk);
        }
        assert_eq!(all.len(), spec.num_docs);

        let again = CorpusGenerator::new(&spec).next_chunk(1000).unwrap();
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
        let mut generator = CorpusGenerator::new(&spec);
        for doc in generator.next_chunk(100).unwrap() {
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
}
