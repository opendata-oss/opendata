//! BM25 scoring primitives (RFC-0006).
//!
//! Provides the IDF formula and the per-document scoring function used by
//! the FTS query path. Kept in `math` alongside the vector distance functions
//! so all scoring helpers live in one place.

use super::norm;

/// Term-frequency saturation parameter (`k1`).
pub(crate) const K1: f32 = 1.2;

/// Document-length normalisation parameter (`b`).
pub(crate) const B: f32 = 0.75;

/// One (term, document) hit contributing to a document's BM25 score.
///
/// The query path collects one entry per query term that occurs in the
/// document, then passes the slice to [`score`]. `norm` is the quantised
/// length byte read straight out of the posting and is decoded inside
/// [`score`] via [`norm::decode_norm`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Bm25TermEntry {
    /// Term frequency: number of times the term occurs in the document.
    pub(crate) freq: u32,
    /// Document length norm (Lucene `intToByte4`-quantised byte).
    pub(crate) norm: u8,
    /// Precomputed IDF for the term against the current corpus.
    pub(crate) idf: f32,
}

/// IDF for a term occurring in `n_t` of `n_docs` documents.
///
/// Uses the RFC-0006 variant `ln(((N - n + 0.5) / (n + 0.5)) + 1)`, which is
/// always non-negative for `n_t <= n_docs`.
pub(crate) fn idf(n_docs: u64, n_t: u64) -> f32 {
    let n_docs = n_docs as f32;
    let n_t = n_t as f32;
    (((n_docs - n_t + 0.5) / (n_t + 0.5)) + 1.0).ln()
}

/// BM25 score for a single document given all its term hits and the corpus
/// average document length. Uses the [`K1`] and [`B`] module constants and
/// decodes the per-entry [`Bm25TermEntry::norm`] back to a document length
/// via [`norm::decode_norm`].
pub(crate) fn score(entries: &[Bm25TermEntry], avgdl: f32) -> f32 {
    let mut sum = 0.0f32;
    for entry in entries {
        let f = entry.freq as f32;
        let dl = norm::decode_norm(entry.norm) as f32;
        let denom = f + K1 * (1.0 - B + B * (dl / avgdl));
        sum += entry.idf * (f * (K1 + 1.0) / denom);
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(freq: u32, norm: u8, idf: f32) -> Bm25TermEntry {
        Bm25TermEntry { freq, norm, idf }
    }

    #[test]
    fn idf_is_non_negative_for_typical_inputs() {
        // given/when
        let v = idf(100, 10);

        // then
        assert!(v > 0.0, "idf should be positive: {}", v);
    }

    #[test]
    fn idf_saturates_when_term_is_rare() {
        // given - rare term in big corpus
        let a = idf(10_000, 1);
        let b = idf(10_000, 5_000);

        // then - rare terms score higher than common ones
        assert!(
            a > b,
            "rare-term idf should exceed common-term idf: {} vs {}",
            a,
            b
        );
    }

    #[test]
    fn score_increases_with_term_frequency() {
        // given
        let avgdl = 10.0;
        let idf_val = 1.0;
        let dl = 10;

        // when
        let one = score(&[entry(1, dl, idf_val)], avgdl);
        let three = score(&[entry(3, dl, idf_val)], avgdl);

        // then
        assert!(three > one);
    }

    #[test]
    fn score_decreases_with_document_length() {
        // given
        let avgdl = 10.0;
        let idf_val = 1.0;
        let tf = 2;

        // when
        let short = score(&[entry(tf, 5, idf_val)], avgdl);
        let long = score(&[entry(tf, 50, idf_val)], avgdl);

        // then - longer docs are penalised
        assert!(short > long);
    }

    #[test]
    fn score_sums_term_contributions() {
        // given - two terms, both with the same frequency/length
        let avgdl = 10.0;
        let single = score(&[entry(2, 8, 1.5)], avgdl);

        // when - same hit duplicated
        let combined = score(&[entry(2, 8, 1.5), entry(2, 8, 1.5)], avgdl);

        // then - score scales linearly across independent term entries
        assert!(
            (combined - 2.0 * single).abs() < 1e-5,
            "expected combined ≈ 2*single, got {} vs {}",
            combined,
            single
        );
    }

    #[test]
    fn score_of_empty_entries_is_zero() {
        // given/when
        let s = score(&[], 10.0);

        // then
        assert_eq!(s, 0.0);
    }
}
