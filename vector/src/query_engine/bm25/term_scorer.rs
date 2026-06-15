//! Lazily-decoding posting-list iterator for the BlockMaxScore scorer.
//!
//! [`TermScorer`] walks one query term's posting list in ascending doc id
//! order. Blocks are decoded (bit-unpacked) only when the cursor enters
//! them; blocks the algorithm skips are never decoded. Per-block max scores
//! derived from skip-block impacts are memoized per query.

use crate::error::Result;
use crate::math::bm25::ScoreContext;
use crate::query_engine::bm25::essential::{HitList, ScoreWindow};
use crate::serde::term_postings::{DecodedPostingsBlock, PostingListView};

/// Sentinel: no documents at or beyond this id (data-vector ids use the low
/// 56 bits only, so `u64::MAX` is unreachable).
pub(super) const NO_MORE_DOCS: u64 = u64::MAX;

/// One query term's posting list, iterated lazily in ascending doc id order.
pub(super) struct TermScorer {
    view: PostingListView,
    /// IDF for this term in the current corpus.
    idf: f32,
    /// Current doc id; [`NO_MORE_DOCS`] when exhausted.
    doc: u64,
    /// Term frequency / norm of the current doc.
    freq: u32,
    norm: u8,
    /// Index of the block holding the cursor.
    block_idx: usize,
    /// Decoded arrays for `block_idx` (ascending ids).
    decoded: DecodedPostingsBlock,
    /// Cursor position within `decoded`.
    pos: usize,
    /// Memoized per-block max score contribution.
    block_max_scores: Vec<Option<f32>>,
}

impl TermScorer {
    pub(super) fn new(view: PostingListView, idf: f32) -> Result<Self> {
        let block_max_scores = vec![None; view.blocks().len()];
        let mut scorer = Self {
            view,
            idf,
            doc: 0,
            freq: 0,
            norm: 0,
            block_idx: 0,
            decoded: DecodedPostingsBlock::default(),
            pos: 0,
            block_max_scores,
        };
        scorer.load_block(0)?;
        Ok(scorer)
    }

    /// Current doc id; [`NO_MORE_DOCS`] when exhausted.
    #[inline]
    pub(super) fn doc(&self) -> u64 {
        self.doc
    }

    /// BM25 contribution of the current doc's hit on this term.
    #[inline]
    pub(super) fn current_hit(&self, ctx: &ScoreContext) -> f32 {
        ctx.score_hit(self.idf, self.freq, self.norm)
    }

    /// Decode block `idx` and position the cursor on its first posting.
    ///
    /// `parse()` only validates block framing, so a payload corrupted past
    /// its header is first detected here; the error propagates out of the
    /// query like any other storage decode failure.
    fn load_block(&mut self, idx: usize) -> Result<()> {
        if idx >= self.view.blocks().len() {
            self.doc = NO_MORE_DOCS;
            return Ok(());
        }
        self.block_idx = idx;
        self.view.decode_block_into(idx, &mut self.decoded)?;
        self.pos = 0;
        self.set_current();
        Ok(())
    }

    #[inline]
    fn set_current(&mut self) {
        self.doc = self.decoded.ids[self.pos];
        self.freq = self.decoded.freqs[self.pos];
        self.norm = self.decoded.norms[self.pos];
    }

    /// Advance the cursor to the next posting.
    #[inline]
    pub(super) fn next(&mut self) -> Result<()> {
        self.pos += 1;
        if self.pos < self.decoded.ids.len() {
            self.set_current();
            Ok(())
        } else {
            self.load_block(self.block_idx + 1)
        }
    }

    /// Advance to the first posting with `id >= target`. Targets must be
    /// non-decreasing across calls (cursor only moves forward).
    pub(super) fn seek(&mut self, target: u64) -> Result<u64> {
        if self.doc >= target {
            return Ok(self.doc);
        }
        // Skip whole blocks without decoding them.
        if self.view.blocks()[self.block_idx].max_id < target {
            let blocks = self.view.blocks();
            let mut idx = self.block_idx + 1;
            // Galloping is overkill: block directories are small and this
            // walk is monotonic across the whole query.
            while idx < blocks.len() && blocks[idx].max_id < target {
                idx += 1;
            }
            if idx >= blocks.len() {
                self.doc = NO_MORE_DOCS;
                return Ok(self.doc);
            }
            self.load_block(idx)?;
            if self.doc >= target {
                return Ok(self.doc);
            }
        }
        // Target lies within the current decoded block.
        let ids = &self.decoded.ids;
        self.pos += ids[self.pos..].partition_point(|&id| id < target);
        debug_assert!(self.pos < ids.len());
        self.set_current();
        Ok(self.doc)
    }

    /// Inclusive upper bound (`max_id`) of the block containing the cursor
    /// or `doc`, whichever is further along; [`NO_MORE_DOCS`] when
    /// the scorer is exhausted or no block remains. Outer windows align to
    /// these boundaries so block impacts bound them tightly.
    pub(super) fn probe_block_boundary(&self, doc: u64) -> u64 {
        if self.doc == NO_MORE_DOCS {
            return NO_MORE_DOCS;
        }
        let from = self.doc.max(doc);
        let blocks = self.view.blocks();
        let idx = blocks.partition_point(|b| b.max_id < from);
        if idx >= blocks.len() {
            NO_MORE_DOCS
        } else {
            blocks[idx].max_id
        }
    }

    /// Inclusive upper bound (`max_id`) of the block holding the posting
    /// roughly `n` entries past the cursor (or past `doc`, whichever is
    /// further along); [`NO_MORE_DOCS`] when the scorer is exhausted or the
    /// list ends first. Used for count-based window sizing: the returned
    /// boundary covers at least `n` of this scorer's postings (rounded out
    /// to a block edge, so impact-based window bounds stay tight).
    ///
    /// Counting is approximate at the edges — entries of the first block
    /// that precede `doc` are counted when the cursor lags behind it — which
    /// only shrinks the window; correctness never depends on window size.
    pub(super) fn probe_count_boundary(&self, doc: u64, n: usize) -> u64 {
        if self.doc == NO_MORE_DOCS {
            return NO_MORE_DOCS;
        }
        let from = self.doc.max(doc);
        let blocks = self.view.blocks();
        let start = blocks.partition_point(|b| b.max_id < from);
        let mut remaining = n;
        for (idx, block) in blocks.iter().enumerate().skip(start) {
            // Exact within the decoded block the cursor sits in; whole-block
            // counts elsewhere.
            let count = if idx == self.block_idx {
                self.decoded.ids.len() - self.pos
            } else {
                block.count as usize
            };
            if remaining <= count {
                return block.max_id;
            }
            remaining -= count;
        }
        NO_MORE_DOCS
    }

    /// Memoized max score contribution of block `idx`.
    fn block_max_score(&mut self, idx: usize, ctx: &ScoreContext) -> f32 {
        if let Some(cached) = self.block_max_scores[idx] {
            return cached;
        }
        let impacts = self.view.impacts(idx);
        let score = if impacts.is_empty() {
            // Written before impacts landed: fall back to the global bound.
            ctx.global_bound(self.idf)
        } else {
            impacts
                .iter()
                .map(|imp| ctx.score_hit(self.idf, imp.freq, imp.norm))
                .fold(0.0f32, f32::max)
        };
        self.block_max_scores[idx] = Some(score);
        score
    }

    /// Max possible contribution of this term for docs in
    /// `[from, to_inclusive]`, from block impacts.
    pub(super) fn max_score_in_range(
        &mut self,
        from: u64,
        to_inclusive: u64,
        ctx: &ScoreContext,
    ) -> f32 {
        let start = self.view.blocks().partition_point(|b| b.max_id < from);
        let mut max = 0.0f32;
        for idx in start..self.view.blocks().len() {
            if self.view.blocks()[idx].min_id > to_inclusive {
                break;
            }
            max = max.max(self.block_max_score(idx, ctx));
        }
        max
    }

    /// Score every posting in `[self.doc, up_to)` into the window
    /// accumulator, leaving the cursor at the first posting `>= up_to`.
    /// Slot `i` of the window corresponds to doc id `window_min + i`.
    ///
    /// This is the term-at-a-time batch loop: for each decoded block it runs
    /// a tight scalar loop over the block's parallel arrays. Vectorization
    /// (RFC-0006 future work) will replace the loop body, not the structure.
    pub(super) fn score_window_into(
        &mut self,
        window_min: u64,
        up_to: u64,
        ctx: &ScoreContext,
        window: &mut ScoreWindow,
    ) -> Result<()> {
        debug_assert!(self.doc >= window_min);
        while self.doc < up_to {
            let ids = &self.decoded.ids;
            let end = self.pos + ids[self.pos..].partition_point(|&id| id < up_to);
            let run = ids[self.pos..end]
                .iter()
                .zip(&self.decoded.freqs[self.pos..end])
                .zip(&self.decoded.norms[self.pos..end]);
            for ((&id, &freq), &norm) in run {
                let slot = (id - window_min) as usize;
                window.add(slot, ctx.score_hit(self.idf, freq, norm));
            }
            if end < ids.len() {
                self.pos = end;
                self.set_current();
                return Ok(());
            }
            self.load_block(self.block_idx + 1)?;
        }
        Ok(())
    }

    /// Append every posting in `[self.doc, up_to)` to `out` as
    /// `(doc, hit)` pairs in ascending doc order, leaving the cursor at the
    /// first posting `>= up_to`.
    ///
    /// The merge strategy's counterpart to
    /// [`score_window_into`](Self::score_window_into): the same
    /// term-at-a-time batch loop over the block's decoded arrays, but
    /// appending sequentially instead of scattering into id-indexed slots.
    pub(super) fn collect_hits_into(
        &mut self,
        up_to: u64,
        ctx: &ScoreContext,
        out: &mut HitList,
    ) -> Result<()> {
        while self.doc < up_to {
            let ids = &self.decoded.ids;
            let end = self.pos + ids[self.pos..].partition_point(|&id| id < up_to);
            out.docs.extend_from_slice(&ids[self.pos..end]);
            let run = self.decoded.freqs[self.pos..end]
                .iter()
                .zip(&self.decoded.norms[self.pos..end]);
            for (&freq, &norm) in run {
                out.hits.push(ctx.score_hit(self.idf, freq, norm));
            }
            if end < ids.len() {
                self.pos = end;
                self.set_current();
                return Ok(());
            }
            self.load_block(self.block_idx + 1)?;
        }
        Ok(())
    }
}
