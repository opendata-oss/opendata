//! Essential-clause scoring strategies for the BlockMaxScore driver.
//!
//! The driver ([`MaxScoreScorer`](super::block_max::MaxScoreScorer)) owns
//! partitioning, score bounds, filtering, non-essential probing, and
//! collection; an [`EssentialScorer`] owns the two pieces that depend on how
//! candidates are accumulated:
//!
//! - **planning** — where the next outer window should end, and
//! - **scoring** — turning the essential scorers' postings within that
//!   window into a doc-ascending list of partially-scored candidates.
//!
//! Two strategies are provided:
//!
//! - [`DenseWindowScorer`]: Lucene's `MaxScoreBulkScorer` shape. Windows end
//!   at the nearest posting-block boundary; essential postings scatter into
//!   a dense [`ScoreWindow`] indexed by `doc - window_min`. Best when doc
//!   ids are dense — slot utilization is high and cross-term combination is
//!   one array write.
//! - [`MergeScorer`]: windows sized by posting **count** (the id span ends
//!   where each lead scorer is ~[`WINDOW_POSTINGS`] entries past its
//!   cursor); each essential term appends its hits to a per-term list and
//!   the lists are k-way merged. Work and memory scale with postings, not
//!   with id span, so this holds up when the id space is sparse (deletes /
//!   update churn leave permanent holes — ids are never reassigned).
//!
//! Both strategies compute hits with the same [`ScoreContext`] arithmetic
//! and accumulate in `f64`, so they produce bit-identical candidate scores.

use crate::error::Result;
use crate::math::bm25::ScoreContext;

use super::term_scorer::{NO_MORE_DOCS, TermScorer};

/// Size of the dense inner scoring window, in doc ids. Only meaningful for
/// [`DenseWindowScorer`]: a window of ids maps to this many accumulator
/// slots, so utilization degrades as the id space grows sparse.
const INNER_WINDOW_SIZE: u64 = 4096;

/// Target number of postings per window lead for [`MergeScorer`]'s
/// count-based planning. The window ends at the block boundary where the
/// tightest lead is roughly this many entries past its cursor, so per-window
/// essential work is bounded by postings rather than id span.
const WINDOW_POSTINGS: usize = 4096;

/// Per-batch candidate cap for the single-essential-clause fast path. Caps
/// how many candidates accumulate before the driver probes non-essential
/// clauses and collects — keeping the top-k floor fresh.
const SINGLE_CLAUSE_BATCH: usize = 256;

/// Which [`EssentialScorer`] the BlockMax operator drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EssentialStrategy {
    /// [`DenseWindowScorer`]: block-aligned windows + dense accumulator.
    DenseWindow,
    /// [`MergeScorer`]: count-sized windows + sort-merge accumulation.
    SortMerge,
}

/// A [`TermScorer`] plus its partitioning state for the current outer window
/// (Lucene's `DisiWrapper`). The window bound lives here — not on the
/// iterator — because it belongs to the partitioning pass, which rewrites it
/// every window.
pub(super) struct WindowScorer {
    pub(super) scorer: TermScorer,
    /// Max possible contribution within the current outer window.
    pub(super) max_window_score: f32,
}

/// Candidate documents within a window: doc-ascending parallel arrays of ids
/// and partial (essential-only, then progressively completed) scores. This is
/// first populated from the accumulated scores from essential terms, then
/// updated with scores from non-essential terms.
#[derive(Default)]
pub(super) struct Candidates {
    pub(super) docs: Vec<u64>,
    pub(super) scores: Vec<f64>,
}

impl Candidates {
    pub(super) fn clear(&mut self) {
        self.docs.clear();
        self.scores.clear();
    }

    pub(super) fn push(&mut self, doc: u64, score: f64) {
        self.docs.push(doc);
        self.scores.push(score);
    }

    pub(super) fn len(&self) -> usize {
        self.docs.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Drop candidates whose score is below `min_score`, in place.
    pub(super) fn retain_at_least(&mut self, min_score: f64) {
        let mut out = 0usize;
        for i in 0..self.docs.len() {
            if self.scores[i] >= min_score {
                self.docs[out] = self.docs[i];
                self.scores[out] = self.scores[i];
                out += 1;
            }
        }
        self.docs.truncate(out);
        self.scores.truncate(out);
    }

    /// Drop candidates whose doc id fails `keep`, in place.
    pub(super) fn retain_docs(&mut self, mut keep: impl FnMut(u64) -> bool) {
        let mut out = 0usize;
        for i in 0..self.docs.len() {
            if keep(self.docs[i]) {
                self.docs[out] = self.docs[i];
                self.scores[out] = self.scores[i];
                out += 1;
            }
        }
        self.docs.truncate(out);
        self.scores.truncate(out);
    }
}

/// Strategy for planning outer windows and scoring the essential clauses
/// within them.
///
/// `score_window` may consume only a *chunk* of `[top, window_max)` per call
/// (e.g. one inner window, or one candidate batch); the driver keeps calling
/// while the minimum essential doc stays below `window_max`, re-checking the
/// top-k floor between calls. Contract: each call pushes candidates in
/// ascending doc order with no duplicates, covering exactly the postings the
/// essential scorers were advanced past; candidates are *not* filtered — the
/// driver applies the metadata filter and deletions to the buffer afterwards.
pub(super) trait EssentialScorer {
    /// Pick the end (exclusive) of the outer window starting at `window_min`,
    /// given the window-lead scorers. Must return a bound strictly greater
    /// than `window_min` unless every lead is exhausted ([`NO_MORE_DOCS`]).
    fn plan_window(&self, leads: &[WindowScorer], window_min: u64) -> u64;

    /// Score a chunk of the essential scorers' postings in
    /// `[min essential doc, window_max)` into `candidates`, advancing the
    /// scorers past everything scored.
    fn score_window(
        &mut self,
        essential: &mut [WindowScorer],
        window_max: u64,
        ctx: &ScoreContext,
        candidates: &mut Candidates,
    ) -> Result<()>;
}

/// Smallest and second-smallest current doc among `scorers`.
fn top2(scorers: &[WindowScorer]) -> (u64, u64) {
    let mut top = NO_MORE_DOCS;
    let mut top2 = NO_MORE_DOCS;
    for ws in scorers {
        let doc = ws.scorer.doc();
        if doc < top {
            top2 = top;
            top = doc;
        } else if doc < top2 {
            top2 = doc;
        }
    }
    (top, top2)
}

// -- Dense windows (Lucene MaxScoreBulkScorer shape) ----------------------------

/// Dense per-window score accumulator: an `f64` score per slot plus a match
/// bitset over the slots, populated term-at-a-time by
/// [`TermScorer::score_window_into`] and emptied with [`drain`](Self::drain).
pub(super) struct ScoreWindow {
    scores: Vec<f64>,
    match_words: Vec<u64>,
}

impl ScoreWindow {
    pub(super) fn new(slots: usize) -> Self {
        let words = slots.div_ceil(64);
        Self {
            scores: vec![0.0; words * 64],
            match_words: vec![0u64; words],
        }
    }

    /// Accumulate one hit into `slot`.
    #[inline]
    pub(super) fn add(&mut self, slot: usize, hit: f32) {
        self.scores[slot] += hit as f64;
        self.match_words[slot >> 6] |= 1u64 << (slot & 63);
    }

    /// Visit the matched slots in ascending order and reset the window,
    /// touching only slots that were set (no full clear of the buffers).
    pub(super) fn drain(&mut self, mut visit: impl FnMut(usize, f64)) {
        for word_idx in 0..self.match_words.len() {
            let mut word = self.match_words[word_idx];
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                // unsets the right-most set bit
                word &= word - 1;
                let slot = (word_idx << 6) | bit;
                let score = self.scores[slot];
                self.scores[slot] = 0.0;
                visit(slot, score);
            }
            self.match_words[word_idx] = 0;
        }
    }
}

/// Block-aligned windows scored into a dense id-indexed accumulator —
/// Lucene's `MaxScoreBulkScorer` structure. See the module docs for when
/// this wins (dense id spaces) and loses (sparse ones).
pub(super) struct DenseWindowScorer {
    window: ScoreWindow,
}

impl DenseWindowScorer {
    pub(super) fn new() -> Self {
        Self {
            window: ScoreWindow::new(INNER_WINDOW_SIZE as usize),
        }
    }
}

impl EssentialScorer for DenseWindowScorer {
    /// End the window at the nearest posting-block boundary among the leads,
    /// so block impacts bound the window tightly.
    fn plan_window(&self, leads: &[WindowScorer], window_min: u64) -> u64 {
        let mut window_max = NO_MORE_DOCS;
        for ws in leads {
            let upper = ws.scorer.probe_block_boundary(window_min);
            window_max = window_max.min(upper.saturating_add(1));
        }
        // TODO(rfc-0006): block-aligned windows can degenerate to a few docs
        // each when posting lists are poorly aligned, making the scorer spend
        // more time re-planning windows than scoring documents.
        // Lucene counteracts this with a minimum window size that doubles
        // while windows stay unproductive (`MaxScoreBulkScorer`,
        // `minWindowSize`); revisit if window-planning overhead shows up in
        // benchmarks.
        window_max
    }

    fn score_window(
        &mut self,
        essential: &mut [WindowScorer],
        window_max: u64,
        ctx: &ScoreContext,
        candidates: &mut Candidates,
    ) -> Result<()> {
        let (top, top2) = top2(essential);

        // Single-clause fast path: when only one essential clause has docs in
        // the first half-window, stream one candidate batch directly without
        // the dense accumulator.
        if top2 >= top.saturating_add(INNER_WINDOW_SIZE / 2) {
            let up_to = top2.min(window_max);
            let ws = essential
                .iter_mut()
                .min_by_key(|ws| ws.scorer.doc())
                .expect("score_window requires an essential scorer");
            while ws.scorer.doc() < up_to && candidates.len() < SINGLE_CLAUSE_BATCH {
                let hit = ws.scorer.current_hit(ctx);
                candidates.push(ws.scorer.doc(), hit as f64);
                ws.scorer.next()?;
            }
            return Ok(());
        }

        let inner_min = top;
        let inner_max = window_max.min(inner_min.saturating_add(INNER_WINDOW_SIZE));

        // Term-at-a-time: exhaust each essential clause through the whole
        // inner window before touching the next clause.
        for ws in essential.iter_mut() {
            if ws.scorer.doc() < inner_max {
                ws.scorer
                    .score_window_into(inner_min, inner_max, ctx, &mut self.window)?;
            }
        }

        // Flush the accumulator into the doc-sorted candidate list.
        self.window.drain(|slot, score| {
            candidates.push(inner_min + slot as u64, score);
        });
        Ok(())
    }
}

// -- Count-sized windows + sort-merge accumulation ------------------------------

/// One essential term's hits within the current window, appended in
/// ascending doc order by [`TermScorer::collect_hits_into`].
#[derive(Default)]
pub(super) struct HitList {
    pub(super) docs: Vec<u64>,
    pub(super) hits: Vec<f32>,
}

impl HitList {
    fn clear(&mut self) {
        self.docs.clear();
        self.hits.clear();
    }
}

/// Count-sized windows scored by appending each essential term's hits to a
/// sorted per-term list and k-way merging the lists into the candidate
/// buffer. Work and memory scale with the postings actually present, never
/// with the window's id span, so sparsity in the doc id space costs nothing.
/// The price is a comparison per posting during the merge instead of the
/// dense path's O(1) slot addressing — cheap while essential clauses are few.
pub(super) struct MergeScorer {
    /// Per-essential-term hit buffers, reused across windows.
    buffers: Vec<HitList>,
    /// Per-buffer merge cursors, reused across windows.
    cursors: Vec<usize>,
}

impl MergeScorer {
    pub(super) fn new() -> Self {
        Self {
            buffers: Vec::new(),
            cursors: Vec::new(),
        }
    }
}

impl EssentialScorer for MergeScorer {
    /// End the window where the tightest lead is ~[`WINDOW_POSTINGS`]
    /// entries past its cursor (rounded out to that entry's block boundary,
    /// which keeps impact-based window bounds tight).
    fn plan_window(&self, leads: &[WindowScorer], window_min: u64) -> u64 {
        let mut window_max = NO_MORE_DOCS;
        for ws in leads {
            let upper = ws.scorer.probe_count_boundary(window_min, WINDOW_POSTINGS);
            window_max = window_max.min(upper.saturating_add(1));
        }
        window_max
    }

    fn score_window(
        &mut self,
        essential: &mut [WindowScorer],
        window_max: u64,
        ctx: &ScoreContext,
        candidates: &mut Candidates,
    ) -> Result<()> {
        if self.buffers.len() < essential.len() {
            self.buffers.resize_with(essential.len(), HitList::default);
        }

        // Term-at-a-time: append each essential clause's window hits to its
        // own doc-ascending list. Same batch shape as the dense path's
        // accumulation loop (and the same vectorization target), minus the
        // scatter.
        for (ws, buffer) in essential.iter_mut().zip(&mut self.buffers) {
            buffer.clear();
            if ws.scorer.doc() < window_max {
                ws.scorer.collect_hits_into(window_max, ctx, buffer)?;
            }
        }
        let buffers = &self.buffers[..essential.len()];

        // Single-clause fast path: nothing to merge.
        if let [only] = buffers {
            for (&doc, &hit) in only.docs.iter().zip(&only.hits) {
                candidates.push(doc, hit as f64);
            }
            return Ok(());
        }

        // K-way merge: emit each distinct doc once, in ascending order,
        // summing the hits of every list positioned on it. Linear scan over
        // the heads instead of a heap — k is the essential clause count.
        self.cursors.clear();
        self.cursors.resize(buffers.len(), 0);
        loop {
            let mut min_doc = NO_MORE_DOCS;
            for (cursor, buffer) in self.cursors.iter().zip(buffers) {
                if let Some(&doc) = buffer.docs.get(*cursor) {
                    min_doc = min_doc.min(doc);
                }
            }
            if min_doc == NO_MORE_DOCS {
                return Ok(());
            }
            let mut score = 0.0f64;
            for (cursor, buffer) in self.cursors.iter_mut().zip(buffers) {
                if buffer.docs.get(*cursor) == Some(&min_doc) {
                    score += buffer.hits[*cursor] as f64;
                    *cursor += 1;
                }
            }
            candidates.push(min_doc, score);
        }
    }
}
