//! BM25 scoring primitives (RFC-0006).
//!
//! Provides the IDF formula and the per-document scoring function used by
//! the FTS query path. Kept in `math` alongside the vector distance functions
//! so all scoring helpers live in one place.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{
    __m256i, _mm256_add_ps, _mm256_cvtepi32_ps, _mm256_div_ps, _mm256_i32gather_ps,
    _mm256_loadu_si256, _mm256_mul_ps, _mm256_set1_ps, _mm256_setr_epi32, _mm256_storeu_ps,
    _mm256_sub_ps,
};

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

/// The reciprocal of the BM25 denominator's document-length component:
/// `1 / c` with `c = K1 * (1 - B + B * dl/avgdl)`.
///
/// Cached inverted (rather than as `c` itself) so [`hit_score`] can use the
/// rounding-monotone `w - w / (1 + f * inv)` form; see [`hit_score`].
#[inline]
fn norm_inverse(norm: u8, avgdl: f32) -> f32 {
    let dl = norm::decode_norm(norm) as f32;
    1.0 / (K1 * (1.0 - B + B * (dl / avgdl)))
}

/// BM25 contribution of a single (term, document) hit, computed as
/// `w - w / (1 + f * inv)` where `w = idf * (K1 + 1)`, `f` is the term
/// frequency, and `inv` comes from [`norm_inverse`] — the same rewrite
/// Lucene's `BM25Similarity` uses.
///
/// This is the textbook BM25 hit `idf * f*(K1+1) / (f + c)` rewritten in
/// terms of `inv = 1/c`:
///
/// ```text
/// w - w/(1 + f/c) = w - w*c/(c + f)        multiply num and denom by c
///                 = w * (1 - c/(f + c))
///                 = w * f/(f + c)
///                 = idf * f*(K1+1) / (f + c)
/// ```
///
/// The two forms are equal in real arithmetic but differ under f32 rounding,
/// and only the rewritten form is *monotone* there. It evaluates as a chain
/// of single correctly-rounded operations,
///
/// ```text
/// x = f * inv    increasing in f (and in inv)
/// y = 1 + x      increasing in x
/// z = w / y      decreasing in y
/// w - z          increasing as z decreases
/// ```
///
/// and rounding-to-nearest preserves ordering per operation, so the computed
/// score never decreases when `f` rises or `dl` falls. The textbook form
/// rounds `f`'s appearance in the numerator and the denominator
/// independently and can drop by an ulp as `f` increases. That matters
/// because BlockMaxScore bounds a block by its dominating `(freq, norm)`
/// impact pairs: dominance only implies a bound if higher freq / lower norm
/// can never score lower *as computed*, otherwise a dominated posting can
/// exceed the block "maximum" and a document near the top-k floor can be
/// wrongly pruned.
///
/// Every scoring path (the exhaustive scorer, BlockMaxScore accumulation,
/// and impact-based bounds) must use this exact arithmetic so that scores and
/// bounds stay bit-consistent with each other.
#[inline(always)]
fn hit_score(idf: f32, freq: u32, inv: f32) -> f32 {
    let w = idf * (K1 + 1.0);
    w - w / (1.0 + freq as f32 * inv)
}

/// BM25 score for a single document given all its term hits and the corpus
/// average document length. Uses the [`K1`] and [`B`] module constants and
/// decodes the per-entry [`Bm25TermEntry::norm`] back to a document length
/// via [`norm::decode_norm`].
///
/// Each hit is computed in f32 ([`hit_score`]) and the hits are accumulated
/// in f64 with one final rounding. An f64 sum of a query's worth of f32 hits
/// is exact, so the result is independent of accumulation order — which is
/// what lets the BlockMaxScore path (which sums the same hits in window
/// order) return bit-identical scores.
pub(crate) fn score(entries: &[Bm25TermEntry], avgdl: f32) -> f32 {
    let mut sum = 0.0f64;
    for entry in entries {
        let inv = norm_inverse(entry.norm, avgdl);
        sum += hit_score(entry.idf, entry.freq, inv) as f64;
    }
    sum as f32
}

/// Per-query scoring context for the BlockMaxScore path.
///
/// Caches [`norm_inverse`] for all 256 norm bytes (the same trick as Lucene's
/// `BM25Similarity` norm cache), so a per-hit score is one lookup and four
/// arithmetic ops — a shape that batches/vectorizes cleanly.
#[derive(Debug, Clone)]
pub(crate) struct ScoreContext {
    /// [`norm_inverse`] for every norm byte.
    norm_cache: [f32; 256],
}

impl ScoreContext {
    pub(crate) fn new(avgdl: f32) -> Self {
        let mut norm_cache = [0f32; 256];
        for (b, slot) in norm_cache.iter_mut().enumerate() {
            *slot = norm_inverse(b as u8, avgdl);
        }
        Self { norm_cache }
    }

    /// BM25 contribution of one (term, document) hit; bit-identical to the
    /// corresponding term of the exhaustive [`score`] sum.
    #[inline(always)]
    pub(crate) fn score_hit(&self, idf: f32, freq: u32, norm: u8) -> f32 {
        hit_score(idf, freq, self.norm_cache[norm as usize])
    }

    /// BM25 contributions for one term's posting run.
    ///
    /// Computes the same per-hit values as [`score_hit`](Self::score_hit),
    /// but batches the independent `(freq, norm)` pairs so x86_64 hosts with
    /// AVX2 can evaluate eight postings per vector instruction group. The
    /// fallback is scalar and preserves the exact hit arithmetic.
    pub(crate) fn score_hits(&self, idf: f32, freqs: &[u32], norms: &[u8], out: &mut [f32]) {
        assert_eq!(freqs.len(), norms.len());
        assert_eq!(freqs.len(), out.len());

        #[cfg(target_arch = "x86_64")]
        {
            if freqs.len() >= 8 && std::is_x86_feature_detected!("avx2") {
                // SAFETY: AVX2 support is checked at runtime. Slice length
                // equality is asserted above, and the AVX2 helper handles
                // chunks and tails within bounds.
                unsafe {
                    score_hits_avx2(&self.norm_cache, idf, freqs, norms, out);
                }
                return;
            }
        }

        self.score_hits_scalar(idf, freqs, norms, out);
    }

    fn score_hits_scalar(&self, idf: f32, freqs: &[u32], norms: &[u8], out: &mut [f32]) {
        for ((slot, &freq), &norm) in out.iter_mut().zip(freqs).zip(norms) {
            *slot = self.score_hit(idf, freq, norm);
        }
    }

    /// Upper bound on any hit's contribution for a term, regardless of
    /// frequency or document length.
    ///
    /// [`hit_score`] is `w - w / (1 + f * inv)` and the subtracted quotient
    /// is non-negative, so the rounded result never exceeds
    /// `w = idf * (K1 + 1)`. Used for blocks written before impacts landed.
    #[inline]
    pub(crate) fn global_bound(&self, idf: f32) -> f32 {
        idf * (K1 + 1.0)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn score_hits_avx2(
    norm_cache: &[f32; 256],
    idf: f32,
    freqs: &[u32],
    norms: &[u8],
    out: &mut [f32],
) {
    debug_assert_eq!(freqs.len(), norms.len());
    debug_assert_eq!(freqs.len(), out.len());

    let mut idx = 0usize;
    let len = freqs.len();
    let one = _mm256_set1_ps(1.0);
    let w_scalar = idf * (K1 + 1.0);
    let w = _mm256_set1_ps(w_scalar);

    while idx + 8 <= len {
        let freq_chunk = &freqs[idx..idx + 8];
        if freq_chunk.iter().all(|&freq| freq <= i32::MAX as u32) {
            // SAFETY: `idx + 8 <= len` guarantees the load and store cover
            // eight initialized `u32` inputs and eight writable `f32` outputs.
            let freq_i32 = unsafe { _mm256_loadu_si256(freqs.as_ptr().add(idx).cast::<__m256i>()) };
            let freq = _mm256_cvtepi32_ps(freq_i32);
            let norm_idx = _mm256_setr_epi32(
                norms[idx] as i32,
                norms[idx + 1] as i32,
                norms[idx + 2] as i32,
                norms[idx + 3] as i32,
                norms[idx + 4] as i32,
                norms[idx + 5] as i32,
                norms[idx + 6] as i32,
                norms[idx + 7] as i32,
            );
            // SAFETY: all gathered norm indices are bytes in 0..=255, and
            // scale 4 addresses `f32` elements in `norm_cache`.
            let inv = unsafe { _mm256_i32gather_ps(norm_cache.as_ptr(), norm_idx, 4) };
            let denom = _mm256_add_ps(one, _mm256_mul_ps(freq, inv));
            let score = _mm256_sub_ps(w, _mm256_div_ps(w, denom));
            // SAFETY: `idx + 8 <= len` guarantees eight writable `f32`s.
            unsafe { _mm256_storeu_ps(out.as_mut_ptr().add(idx), score) };
        } else {
            for lane in idx..idx + 8 {
                out[lane] = hit_score(idf, freqs[lane], norm_cache[norms[lane] as usize]);
            }
        }
        idx += 8;
    }

    for lane in idx..len {
        out[lane] = hit_score(idf, freqs[lane], norm_cache[norms[lane] as usize]);
    }
}

/// Upper bound of the sum of `num_values` non-negative `f32`-valued terms
/// accumulated in `f64`, mirroring Lucene's `MathUtil.sumUpperBound`.
///
/// Summation order affects the rounded result, so bound comparisons against a
/// threshold must allow for the worst-case accumulation error (Higham, "The
/// accuracy of floating point summation", bound 3.5) or documents could be
/// pruned that an exhaustive scorer would have kept.
#[inline]
pub(crate) fn sum_upper_bound(sum: f64, num_values: usize) -> f64 {
    if num_values <= 2 {
        // A sum of two values is the same regardless of order.
        return sum;
    }
    // Relative error bound `b = (n - 1) * u` with unit roundoff `u = 2^-52`
    // (`f64::EPSILON`); two differently-ordered sums are within `2b`.
    let b = (num_values - 1) as f64 * f64::EPSILON;
    sum * (1.0 + 2.0 * b)
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
    fn score_hits_matches_scalar_score_hit() {
        // given
        let ctx = ScoreContext::new(37.0);
        let idf = 1.7;
        let freqs = vec![
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            u32::MAX,
            17,
            18,
            19,
        ];
        let norms = vec![
            0, 1, 3, 7, 15, 31, 63, 127, 255, 200, 150, 100, 50, 25, 12, 6, 4, 2, 1,
        ];
        let mut actual = vec![0.0; freqs.len()];

        // when
        ctx.score_hits(idf, &freqs, &norms, &mut actual);

        // then
        for i in 0..freqs.len() {
            let expected = ctx.score_hit(idf, freqs[i], norms[i]);
            assert_eq!(
                actual[i].to_bits(),
                expected.to_bits(),
                "hit {i}: actual={} expected={}",
                actual[i],
                expected
            );
        }
    }

    #[test]
    fn score_of_empty_entries_is_zero() {
        // given/when
        let s = score(&[], 10.0);

        // then
        assert_eq!(s, 0.0);
    }
}
