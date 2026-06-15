//! FTS compaction filter for the vector subsystem (RFC-0006).
//!
//! Deletes are recorded lazily: a deleted vector's id is unioned into the
//! [`Deletions`](crate::serde::RecordType::Deletions) bitmap, and the actual
//! cleanup of its term postings and statistics is deferred to compaction. This
//! [`CompactionFilter`] performs that cleanup as the FTS segment is compacted.
//!
//! [`VectorCompactionFilterSupplier`] is handed to the storage layer alongside
//! the [`VectorSegmentExtractor`](super::segment_extractor::VectorSegmentExtractor),
//! which routes the FTS records into their own segment so they can be compacted
//! as a unit. SlateDB only exposes compaction filters through its
//! `CompactorBuilder`, so the common storage builder installs it there.
//!
//! # Only at the last sorted run
//!
//! The supplier installs the real filter **only when the compaction's
//! destination is the last (oldest) sorted run**; for every other compaction it
//! installs a [`NoOpCompactionFilter`]. Applying deletes only at the last run
//! guarantees that all of a deleted vector's keys — term postings, term stats,
//! and per-vector field stats — are present together in the compaction, so a
//! delete is observed and applied consistently in a single pass.
//!
//! # What it does (last-run compactions)
//!
//! For an FTS-segment compaction reaching the last sorted run, the filter (in
//! key order):
//!
//! 1. **Accumulates and drops the deletions bitmap.** The `Deletions` record
//!    (tag `0x0c`) sorts before every other FTS record, so by the time any
//!    postings/stats entry is seen the bitmap holds every id deleted in this
//!    compaction. The `Deletions` entries are dropped — every posting they
//!    reference is in this compaction and has been pruned, so the ids can never
//!    resurface.
//! 2. **Prunes term postings.** Deleted ids are removed from each
//!    [`TermPostingsValue`] and the survivors are re-packed into uniform blocks
//!    via [`TermPostingsValue::from_postings`]. The number of removed documents
//!    is recorded for the sibling `TermStats` key.
//! 3. **Applies the term document-frequency delta** to the immediately
//!    following [`TermStatsValue`] (the `TermStats` key for a `(field, term)`
//!    sorts right after its `TermPostings` key — discriminator `0x01` > `0x00`).
//! 4. **Drops per-vector field stats** ([`VectorFieldStatsValue`]) for deleted
//!    vectors, folding a `count -1` / `total_length -len` / `deletes -1` into a
//!    pending [`FieldStatsValue`] delta for each field the vector populated.
//! 5. **Applies the field-stats delta** to each [`FieldStatsValue`]. The indexer
//!    only ever *increments* `FieldStats` (`count`/`total_length` on insert,
//!    `deletes` on delete, using the FTS field set recorded in `VectorIndexData`);
//!    the filter retires deleted documents from all three here. All
//!    `VectorFieldStats` (tag `0x0e`) sort before all `FieldStats` (tag `0x0f`),
//!    so every delta is accumulated before it is applied.
//!
//! For any non-FTS key (the ANN and Default segments, or any malformed key) the
//! filter is a strict no-op.
//!
//! # Merge-operand semantics
//!
//! SlateDB applies the merge operator *below* the compaction filter, but only up
//! to the compaction's snapshot barrier (`retention_min_seq`). Merge operands
//! sequenced above that barrier are passed through to the filter individually
//! and **un-merged**, so the filter can be called multiple times for the same
//! key within one compaction — each call carrying one operand, not the fully
//! merged value. The design tolerates this because every FTS merge is structured
//! so that pruning-then-merging equals merging-then-pruning:
//!
//! - **Postings** merge by byte concatenation in descending-id order. Each
//!   operand is a self-contained, independently decodable `TermPostingsValue`,
//!   so pruning deleted ids from each operand in turn yields the same result as
//!   pruning the concatenation. The document-frequency decrement is accumulated
//!   across every operand for the key into `pending_term_deltas`.
//! - **Term / field stats** are *signed deltas* summed by the merge operator.
//!   The full accumulated decrement is applied to the *first* stats operand seen
//!   for a key (the pending entry is then removed, so later operands pass
//!   through unchanged); because the operator sums them, the merged total is
//!   correct regardless of which operand absorbed the decrement. Postings keys
//!   sort before their sibling stats key, so the decrement is fully accumulated
//!   before any stats operand is reached.
//!
//! # Limitations (RFC-0006 milestone 1)
//!
//! - The slatedb compaction filter cannot insert new keys, so the term/field
//!   stat deltas can only be applied to a `TermStats`/`FieldStats` entry that is
//!   already present in the compaction. Postings and their stats are always
//!   written together, so this holds in practice; if a stats entry is somehow
//!   absent the delta is dropped and the statistic is left slightly stale (BM25
//!   tolerates document-frequency drift — see the RFC).
//! - Block re-alignment uses the fixed 256-entry block size baked into the
//!   posting encoder; the configurable `target_posting_block_size` from the RFC
//!   is not yet wired.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, RowEntry, ValueDeletable,
};

use crate::serde::deletions::DeletionsValue;
use crate::serde::field_stats::FieldStatsValue;
use crate::serde::key::{
    DELETIONS_DISCRIMINATOR, DELETIONS_SENTINEL_DISCRIMINATOR, FieldStatsKey,
    TERM_POSTINGS_DISCRIMINATOR, TERM_STATS_DISCRIMINATOR, VectorFieldStatsKey,
};
use crate::serde::term_postings::TermPostingsValue;
use crate::serde::term_stats::TermStatsValue;
use crate::serde::vector_bitmap::VectorBitmap;
use crate::serde::vector_field_stats::VectorFieldStatsValue;
use crate::serde::{EncodingError, RecordType, Segment, parse_record_tag};

#[derive(Debug, PartialEq, Eq)]
enum FilterPhase {
    Init,
    // transition from Init
    CollectDeletes,
    // transition from CollectDeletes or CollectTermStats with a smaller key
    CollectPostings(Bytes),
    // transition from CollectPostings with same key
    CollectTermStats(Bytes),
    // transition from CollectTermStats
    CollectVectorFieldStats,
    // transition from CollectFieldStats
    CollectFieldStats,
}

/// Applies FTS deletes to postings and statistics during compaction.
///
/// One instance is created per compaction job by
/// [`VectorCompactionFilterSupplier`] and is driven single-threaded over the
/// job's entries in ascending key order.
pub(crate) struct VectorCompactionFilter {
    /// Union of every `Deletions` bitmap seen so far in this compaction; empty
    /// until the (single, merged) bitmap is reached, and empty for the whole
    /// compaction when no deletes were recorded. Only meaningful for
    /// last-sorted-run compactions; see [`filter`](Self::filter).
    deletions: VectorBitmap,
    /// Pending per-term document-frequency decrements, keyed by the term key
    /// with its trailing discriminator byte stripped so a `TermPostings` key and
    /// its sibling `TermStats` key map to the same entry. Each value is the
    /// (positive) number of documents to subtract.
    pending_term_deltas: HashMap<Bytes, i64>,
    /// Pending per-field corpus-stat deltas (`count`, `total_length`, `deletes`),
    /// keyed by field name — the documents this compaction retires from each
    /// field as it applies their deletes.
    pending_field_deltas: HashMap<String, FieldStatsValue>,
    retention_min_seq: Option<u64>,
    phase: FilterPhase,
}

impl VectorCompactionFilter {
    fn new(retention_min_seq: Option<u64>) -> Self {
        Self {
            deletions: VectorBitmap::new(),
            pending_term_deltas: HashMap::new(),
            pending_field_deltas: HashMap::new(),
            retention_min_seq,
            phase: FilterPhase::Init,
        }
    }

    /// Returns `Ok` when the current phase satisfies `predicate`, else a filter
    /// error. The filter advances through a fixed phase sequence as it sees the
    /// FTS keys in sort order; a violation means the keys arrived out of the
    /// expected order — most importantly, a compaction resumed past the sentinel
    /// reaches a non-sentinel key while still in `Init` — so the compaction is
    /// failed and restarts clean rather than applying a partial delete pass.
    fn check_phase(
        &self,
        predicate: impl FnOnce(&FilterPhase) -> bool,
    ) -> Result<(), CompactionFilterError> {
        if predicate(&self.phase) {
            Ok(())
        } else {
            Err(CompactionFilterError::FilterError(
                format!("FTS compaction filter: unexpected phase {:?}", self.phase).into(),
            ))
        }
    }

    /// Advances `Init` -> `CollectDeletes` on the deletions sentinel.
    ///
    /// The sentinel is written on every FTS write batch and sorts before every
    /// other FTS key, so a fresh last-run compaction always observes it first. It
    /// carries no data — it only anchors the start of the key stream — and is
    /// kept untouched.
    fn handle_sentinel(
        &mut self,
        _entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        self.check_phase(|p| matches!(p, FilterPhase::Init))?;
        self.phase = FilterPhase::CollectDeletes;
        Ok(CompactionFilterDecision::Keep)
    }

    /// Unions the (single, merged) deletions bitmap into the running set and
    /// drops it. Runs only after the sentinel, in `CollectDeletes`. The bitmap is
    /// conditional — a batch with no deletes writes none — and the filter only
    /// runs at the last sorted run, so every posting/stat the bitmap references is
    /// present in this compaction and has been pruned; the ids can never
    /// resurface, so the entry is always dropped.
    fn handle_deletions(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        self.check_phase(|p| matches!(p, FilterPhase::CollectDeletes))?;
        if let Some(bytes) = entry.value.as_bytes() {
            let value = DeletionsValue::decode_from_bytes(&bytes).map_err(filter_err)?;
            self.deletions.union_with(&value.0);
        }
        Ok(CompactionFilterDecision::Drop)
    }

    /// Removes deleted ids from a term's postings and records the
    /// document-frequency decrement for the sibling `TermStats` key.
    fn handle_postings(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        // Postings either start the term phase (right after deletions) or advance
        // to a strictly larger `(field, term)` than the term just finished.
        let term_key = term_map_key(&entry.key);
        self.check_phase(|p| {
            matches!(p, FilterPhase::CollectDeletes)
                || matches!(p, FilterPhase::CollectTermStats(k) if term_key > *k)
        })?;
        self.phase = FilterPhase::CollectPostings(term_key);
        // No deletes recorded in this compaction means there is nothing to prune.
        if self.deletions.is_empty() {
            return Ok(CompactionFilterDecision::Keep);
        }
        let Some(bytes) = entry.value.as_bytes() else {
            return Err(CompactionFilterError::FilterError(
                "unexpected empty postings val".into(),
            ));
        };
        let value = TermPostingsValue::decode_from_bytes(&bytes).map_err(filter_err)?;

        let mut retained = Vec::new();
        let mut removed: i64 = 0;
        for posting in value.iter_entries() {
            if self.deletions.contains(posting.id.id()) {
                removed += 1;
            } else {
                retained.push(*posting);
            }
        }
        if removed == 0 {
            return Ok(CompactionFilterDecision::Keep);
        }

        *self
            .pending_term_deltas
            .entry(term_map_key(&entry.key))
            .or_insert(0) += removed;

        if retained.is_empty() {
            // No documents left for this term in this run; drop the operand.
            // The sibling TermStats decrement is still applied when its key is
            // reached.
            return Ok(CompactionFilterDecision::Drop);
        }
        let rewritten = TermPostingsValue::from_postings(retained).encode_to_bytes();
        Ok(replace_value(&entry.value, rewritten))
    }

    /// Applies the pending document-frequency decrement recorded by
    /// [`handle_postings`](Self::handle_postings) to this `TermStats` entry.
    fn handle_term_stats(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        // A term's stats key sorts immediately after its postings key, so it must
        // arrive while collecting postings for the same `(field, term)`.
        let term_key = term_map_key(&entry.key);
        self.check_phase(|p| matches!(p, FilterPhase::CollectPostings(k) if *k == term_key))?;
        self.phase = FilterPhase::CollectTermStats(term_key);
        // A term whose documents were not deleted recorded no pending decrement;
        // pass its stats through unchanged.
        let Some(removed) = self.pending_term_deltas.remove(&term_map_key(&entry.key)) else {
            return Ok(CompactionFilterDecision::Keep);
        };
        let Some(bytes) = entry.value.as_bytes() else {
            return Err(CompactionFilterError::FilterError(
                "malformed term stats".into(),
            ));
        };
        let mut value = TermStatsValue::decode_from_bytes(&bytes).map_err(filter_err)?;
        value.freq = value.freq.saturating_sub(removed);
        Ok(replace_value(&entry.value, value.encode_to_bytes()))
    }

    /// Drops a deleted vector's per-field length record, folding the applied
    /// delete into the pending corpus-stats deltas.
    ///
    /// The indexer increments `FieldStats` only (`count`/`total_length` on insert,
    /// `deletes` on delete). When the filter applies a delete at the last sorted
    /// run it retires that document from all three: `count −1`, `total_length
    /// −len` (the filter alone knows the per-field token length, from this
    /// record), and `deletes −1` (clearing the now-applied delete).
    fn handle_vector_field_stats(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        // Per-vector field stats (tag 0x0e) sort after all term keys, so they
        // follow deletions (when no term keys were present), the term phase, or
        // earlier per-vector records, and repeat across vectors.
        self.check_phase(|p| {
            matches!(
                p,
                FilterPhase::CollectDeletes
                    | FilterPhase::CollectTermStats(_)
                    | FilterPhase::CollectVectorFieldStats
            )
        })?;
        self.phase = FilterPhase::CollectVectorFieldStats;
        if self.deletions.is_empty() {
            return Ok(CompactionFilterDecision::Keep);
        }
        let key = VectorFieldStatsKey::decode(&entry.key).map_err(filter_err)?;
        if !self.deletions.contains(key.vector_id.id()) {
            return Ok(CompactionFilterDecision::Keep);
        }
        if let Some(bytes) = entry.value.as_bytes() {
            let value = VectorFieldStatsValue::decode_from_bytes(&bytes).map_err(filter_err)?;
            for (field, length) in value.lengths {
                let delta = self.pending_field_deltas.entry(field).or_default();
                delta.count = delta.count.saturating_sub(1);
                delta.total_length = delta.total_length.saturating_sub(i64::from(length));
                delta.deletes = delta.deletes.saturating_sub(1);
            }
        }
        Ok(CompactionFilterDecision::Drop)
    }

    /// Applies the accumulated `(count, total_length, deletes)` delta for this
    /// field — the filter retires the deleted documents the indexer had only
    /// ever counted up.
    fn handle_field_stats(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        // Per-field corpus stats (tag 0x0f) sort last, after all per-vector field
        // stats, and repeat across fields. They may also follow the deletions or
        // term phase directly when this compaction retired no vectors.
        self.check_phase(|p| {
            matches!(
                p,
                FilterPhase::CollectDeletes
                    | FilterPhase::CollectTermStats(_)
                    | FilterPhase::CollectVectorFieldStats
                    | FilterPhase::CollectFieldStats
            )
        })?;
        self.phase = FilterPhase::CollectFieldStats;
        let key = FieldStatsKey::decode(&entry.key).map_err(filter_err)?;
        // A field with no retired documents has no pending delta; pass it through.
        let Some(delta) = self.pending_field_deltas.remove(&key.field) else {
            return Ok(CompactionFilterDecision::Keep);
        };
        let Some(bytes) = entry.value.as_bytes() else {
            return Err(CompactionFilterError::FilterError(
                "malformed field stats".into(),
            ));
        };
        let mut value = FieldStatsValue::decode_from_bytes(&bytes).map_err(filter_err)?;
        value.count = value.count.saturating_add(delta.count);
        value.total_length = value.total_length.saturating_add(delta.total_length);
        value.deletes = value.deletes.saturating_add(delta.deletes);
        Ok(replace_value(&entry.value, value.encode_to_bytes()))
    }
}

#[async_trait]
impl CompactionFilter for VectorCompactionFilter {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        // Anything that isn't a well-formed FTS-segment vector key is left
        // untouched — the filter must be a strict no-op for the ANN and Default
        // segments (and for any non-vector key).
        let record_type = match parse_record_tag(&entry.key) {
            Ok(tag) => match RecordType::from_id(tag.record_type()) {
                Ok(record_type) => record_type,
                Err(_) => return Ok(CompactionFilterDecision::Keep),
            },
            Err(_) => return Ok(CompactionFilterDecision::Keep),
        };
        if record_type.segment() != Segment::Fts {
            return Ok(CompactionFilterDecision::Keep);
        }
        if let Some(retention_min_seq) = self.retention_min_seq
            && entry.seq > retention_min_seq
        {
            // any entries with seq higher than retention_min_seq may belong to some
            // active snapshot for which the filter should still preserve snapshot-reads
            return Ok(CompactionFilterDecision::Keep);
        }

        match record_type {
            // The `Deletions` tag covers two records, discriminated by the
            // trailing byte: the always-written sentinel (`0x00`, sorts first)
            // and the conditional deletions bitmap (`0xff`).
            RecordType::Deletions => match entry.key.last().copied() {
                Some(DELETIONS_SENTINEL_DISCRIMINATOR) => self.handle_sentinel(entry),
                Some(DELETIONS_DISCRIMINATOR) => self.handle_deletions(entry),
                _ => Ok(CompactionFilterDecision::Keep),
            },
            RecordType::FtsTerm => match entry.key.last().copied() {
                Some(TERM_POSTINGS_DISCRIMINATOR) => self.handle_postings(entry),
                Some(TERM_STATS_DISCRIMINATOR) => self.handle_term_stats(entry),
                _ => Ok(CompactionFilterDecision::Keep),
            },
            RecordType::FtsVectorFieldStats => self.handle_vector_field_stats(entry),
            RecordType::FtsFieldStats => self.handle_field_stats(entry),
            // parse_record_tag validated the segment is FTS, so no other record
            // type can reach here; keep defensively rather than panicking mid
            // compaction.
            _ => Ok(CompactionFilterDecision::Keep),
        }
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        // No terminal-phase invariant holds. A last-run compaction only sees keys
        // at or below the snapshot barrier (`retention_min_seq`); everything fresher
        // is kept un-pruned without advancing the phase. So a job whose entries are
        // all above the barrier ends in `Init`, one that saw only the sentinel ends
        // in `CollectDeletes`, and so on — every terminal phase is legitimate. The
        // resume guard the sentinel provides is enforced eagerly in the handlers:
        // any non-sentinel FTS key reached while still in `Init` fails `check_phase`
        // (a resumed job that skipped the sentinel), so there is nothing left to
        // assert here.
        Ok(())
    }
}

/// Per-compaction-job factory for [`VectorCompactionFilter`].
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct VectorCompactionFilterSupplier;

impl VectorCompactionFilterSupplier {
    /// Returns a shared `Arc<dyn CompactionFilterSupplier>` for handing to
    /// `slatedb::DbBuilder::with_compaction_filter_supplier`.
    pub(crate) fn shared() -> Arc<dyn CompactionFilterSupplier> {
        Arc::new(Self)
    }
}

#[async_trait]
impl CompactionFilterSupplier for VectorCompactionFilterSupplier {
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        // TODO: extend CompactionJobContext to include job spec and check for fts segment, and
        //       for resumed fts last-sr jobs (and fail these)
        // Apply FTS deletes only when compacting to the last (oldest) sorted run.
        // There every key for a given vector — postings, term stats, per-vector
        // field stats — is guaranteed present, so a delete observes all of a
        // vector's keys in one pass and the cleanup is consistent. For any other
        // compaction the filter is a strict no-op (RFC-0006).
        if context.is_dest_last_run {
            Ok(Box::new(VectorCompactionFilter::new(
                context.retention_min_seq,
            )))
        } else {
            Ok(Box::new(NoOpCompactionFilter))
        }
    }
}

/// No-op compaction filter installed for compactions whose destination is not
/// the last sorted run — the FTS delete-cleanup filter only runs at the last
/// run (see [`VectorCompactionFilterSupplier::create_compaction_filter`]).
struct NoOpCompactionFilter;

#[async_trait]
impl CompactionFilter for NoOpCompactionFilter {
    async fn filter(
        &mut self,
        _entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        Ok(CompactionFilterDecision::Keep)
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        Ok(())
    }
}

/// Strips the trailing discriminator byte from an `FtsTerm` key so a
/// `TermPostings` key (`0x00`) and its sibling `TermStats` key (`0x01`) map to
/// the same entry.
fn term_map_key(key: &Bytes) -> Bytes {
    key.slice(0..key.len() - 1)
}

/// Rebuilds a `Modify` decision with `new_value`, preserving the entry's
/// existing value kind (`Merge` vs `Value`). Tombstones carry no payload, so
/// they are left untouched.
fn replace_value(original: &ValueDeletable, new_value: Bytes) -> CompactionFilterDecision {
    match original {
        ValueDeletable::Merge(_) => {
            CompactionFilterDecision::Modify(ValueDeletable::Merge(new_value))
        }
        ValueDeletable::Value(_) => {
            CompactionFilterDecision::Modify(ValueDeletable::Value(new_value))
        }
        ValueDeletable::Tombstone => panic!("unexpected tombstone"),
    }
}

fn filter_err(e: EncodingError) -> CompactionFilterError {
    CompactionFilterError::FilterError(format!("FTS compaction filter decode error: {e}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::key::{
        DeletionsKey, DeletionsSentinelKey, FieldStatsKey, TermPostingsKey, TermStatsKey,
    };
    use crate::serde::term_postings::PostingEntry;
    use crate::serde::vector_id::VectorId;

    fn merge_entry(key: Bytes, value: Bytes) -> RowEntry {
        RowEntry {
            key,
            value: ValueDeletable::Merge(value),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        }
    }

    fn value_entry(key: Bytes, value: Bytes) -> RowEntry {
        RowEntry {
            key,
            value: ValueDeletable::Value(value),
            seq: 1,
            create_ts: None,
            expire_ts: None,
        }
    }

    fn posting(id: u64, freq: u32, norm: u8) -> PostingEntry {
        PostingEntry {
            id: VectorId::data_vector_id(id),
            freq,
            norm,
        }
    }

    fn deletions_entry(ids: &[u64]) -> RowEntry {
        let mut bitmap = VectorBitmap::new();
        for &id in ids {
            bitmap.insert(id);
        }
        merge_entry(
            DeletionsKey::new().encode(),
            DeletionsValue::new(bitmap).encode_to_bytes().unwrap(),
        )
    }

    /// The always-written sentinel (sorts first); the filter requires it before
    /// any other FTS key.
    fn sentinel_entry() -> RowEntry {
        value_entry(
            DeletionsSentinelKey::new().encode(),
            Bytes::from_static(&[0u8]),
        )
    }

    fn modified_bytes(decision: &CompactionFilterDecision) -> Bytes {
        match decision {
            CompactionFilterDecision::Modify(v) => v.as_bytes().expect("modify carried no bytes"),
            other => panic!("expected Modify, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn should_keep_non_fts_segment_keys() {
        // given - an ANN-segment posting list key
        use crate::serde::key::PostingListKey;
        let mut filter = VectorCompactionFilter::new(None);
        let entry = merge_entry(
            PostingListKey::new(VectorId::centroid_id(1, 7)).encode(),
            Bytes::from_static(b"opaque-ann-bytes"),
        );

        // when
        let decision = filter.filter(&entry).await.unwrap();

        // then - untouched, and the deletions set is never consulted
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert!(filter.deletions.is_empty());
        assert_eq!(filter.phase, FilterPhase::Init);
    }

    #[tokio::test]
    async fn should_accumulate_and_drop_deletions_at_last_run() {
        // given - last-run compaction; the sentinel is seen first
        let mut filter = VectorCompactionFilter::new(None);
        let sentinel_decision = filter.filter(&sentinel_entry()).await.unwrap();
        assert_eq!(sentinel_decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.phase, FilterPhase::CollectDeletes);

        // when - the deletions bitmap follows
        let decision = filter.filter(&deletions_entry(&[1, 2])).await.unwrap();

        // then - the bitmap is accumulated and the record is dropped
        assert_eq!(decision, CompactionFilterDecision::Drop);
        assert!(filter.deletions.contains(1));
        assert!(filter.deletions.contains(2));
        assert_eq!(filter.phase, FilterPhase::CollectDeletes);
    }

    #[tokio::test]
    async fn should_error_when_sentinel_not_seen() {
        // given - a fresh filter that has not seen the sentinel (a compaction
        // resumed past it, or otherwise missing its start marker)
        let mut filter = VectorCompactionFilter::new(None);

        // when - a non-sentinel FTS key is processed while still in Init
        let result = filter.filter(&deletions_entry(&[1, 2])).await;

        // then - the filter fails (forcing a clean restart) rather than applying
        // a partial delete pass
        assert!(result.is_err());
        assert_eq!(filter.phase, FilterPhase::Init);
    }

    #[tokio::test]
    async fn should_keep_everything_on_non_last_run_compactions() {
        // given - the no-op filter the supplier installs when the destination is
        // not the last sorted run (deletes are only applied at the last run)
        let mut filter = NoOpCompactionFilter;

        // when / then - even a Deletions entry is preserved untouched
        assert_eq!(
            filter.filter(&deletions_entry(&[1, 2])).await.unwrap(),
            CompactionFilterDecision::Keep
        );
    }

    #[tokio::test]
    async fn should_prune_postings_and_decrement_term_stats() {
        // given - {1,2} deleted; term "fox" has docs {1,2,3}
        let mut filter = VectorCompactionFilter::new(None);
        filter.filter(&sentinel_entry()).await.unwrap();
        filter.filter(&deletions_entry(&[1, 2])).await.unwrap();

        let postings = TermPostingsValue::from_postings(vec![
            posting(1, 2, 10),
            posting(2, 1, 20),
            posting(3, 3, 30),
        ])
        .encode_to_bytes();
        let postings_entry = merge_entry(TermPostingsKey::new("body", "fox").encode(), postings);

        let stats_entry = merge_entry(
            TermStatsKey::new("body", "fox").encode(),
            TermStatsValue::new(3).encode_to_bytes(),
        );

        // when - postings first (sort before stats), then stats
        let postings_decision = filter.filter(&postings_entry).await.unwrap();
        let stats_decision = filter.filter(&stats_entry).await.unwrap();

        // then - only doc 3 survives in the postings
        let pruned =
            TermPostingsValue::decode_from_bytes(&modified_bytes(&postings_decision)).unwrap();
        let ids: Vec<u64> = pruned.iter_entries().map(|e| e.id.id()).collect();
        assert_eq!(ids, vec![3]);

        // and the document frequency dropped by the 2 removed docs (3 -> 1)
        let stats = TermStatsValue::decode_from_bytes(&modified_bytes(&stats_decision)).unwrap();
        assert_eq!(stats.freq, 1);
    }

    #[tokio::test]
    async fn should_drop_postings_when_all_docs_deleted() {
        // given - both docs of term "fox" are deleted
        let mut filter = VectorCompactionFilter::new(None);
        filter.filter(&sentinel_entry()).await.unwrap();
        filter.filter(&deletions_entry(&[1, 2])).await.unwrap();

        let postings = TermPostingsValue::from_postings(vec![posting(1, 1, 1), posting(2, 1, 1)])
            .encode_to_bytes();
        let postings_entry = merge_entry(TermPostingsKey::new("body", "fox").encode(), postings);
        let stats_entry = merge_entry(
            TermStatsKey::new("body", "fox").encode(),
            TermStatsValue::new(2).encode_to_bytes(),
        );

        // when
        let postings_decision = filter.filter(&postings_entry).await.unwrap();
        let stats_decision = filter.filter(&stats_entry).await.unwrap();

        // then - the empty postings operand is dropped, the stat still decremented to 0
        assert_eq!(postings_decision, CompactionFilterDecision::Drop);
        let stats = TermStatsValue::decode_from_bytes(&modified_bytes(&stats_decision)).unwrap();
        assert_eq!(stats.freq, 0);
    }

    #[tokio::test]
    async fn should_drop_vector_field_stats_and_apply_field_stats_delta() {
        // given - vector 5 deleted, with text fields body(len 10), title(len 2)
        let mut filter = VectorCompactionFilter::new(None);
        filter.filter(&sentinel_entry()).await.unwrap();
        filter.filter(&deletions_entry(&[5])).await.unwrap();

        let vfs = value_entry(
            VectorFieldStatsKey::new(VectorId::data_vector_id(5)).encode(),
            VectorFieldStatsValue::new(vec![("body".to_string(), 10), ("title".to_string(), 2)])
                .encode_to_bytes(),
        );
        // The indexer counted these docs up (count, total_length) on insert and
        // bumped `deletes` on the delete; the filter now retires them. Start each
        // field at 3 docs / 1 pending delete.
        let body_stats = merge_entry(
            FieldStatsKey::new("body").encode(),
            FieldStatsValue::new(3, 30, 1).encode_to_bytes(),
        );
        let title_stats = merge_entry(
            FieldStatsKey::new("title").encode(),
            FieldStatsValue::new(3, 6, 1).encode_to_bytes(),
        );

        // when
        let vfs_decision = filter.filter(&vfs).await.unwrap();
        let body_decision = filter.filter(&body_stats).await.unwrap();
        let title_decision = filter.filter(&title_stats).await.unwrap();

        // then - the per-vector record is dropped
        assert_eq!(vfs_decision, CompactionFilterDecision::Drop);
        // and the filter retires the deleted document from all three stats:
        // count -1, total_length -len, deletes -1.
        let body = FieldStatsValue::decode_from_bytes(&modified_bytes(&body_decision)).unwrap();
        assert_eq!(body, FieldStatsValue::new(2, 20, 0));
        let title = FieldStatsValue::decode_from_bytes(&modified_bytes(&title_decision)).unwrap();
        assert_eq!(title, FieldStatsValue::new(2, 4, 0));
    }

    #[tokio::test]
    async fn should_keep_vector_field_stats_for_live_vectors() {
        // given - vector 5 is deleted, vector 6 is not
        let mut filter = VectorCompactionFilter::new(None);
        filter.filter(&sentinel_entry()).await.unwrap();
        filter.filter(&deletions_entry(&[5])).await.unwrap();
        let live = value_entry(
            VectorFieldStatsKey::new(VectorId::data_vector_id(6)).encode(),
            VectorFieldStatsValue::new(vec![("body".to_string(), 7)]).encode_to_bytes(),
        );

        // when
        let decision = filter.filter(&live).await.unwrap();

        // then - the live vector's record is preserved and no delta accrues
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert!(filter.pending_field_deltas.is_empty());
    }

    #[tokio::test]
    async fn should_keep_entries_above_retention_watermark() {
        // given - a retention watermark at seq 5
        let mut filter = VectorCompactionFilter::new(Some(5));

        // when - a sentinel above the watermark arrives (it may still be visible
        // to an active snapshot, so the filter must preserve it untouched)
        let mut above = sentinel_entry();
        above.seq = 10;
        let decision = filter.filter(&above).await.unwrap();

        // then - kept untouched and not processed: no phase advance
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.phase, FilterPhase::Init);

        // and - the sentinel at the watermark is processed, advancing the phase
        let mut at_watermark = sentinel_entry();
        at_watermark.seq = 5;
        let decision = filter.filter(&at_watermark).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.phase, FilterPhase::CollectDeletes);

        // and - a deletions bitmap above the watermark is preserved (not applied)
        let mut bitmap_above = deletions_entry(&[1, 2]);
        bitmap_above.seq = 10;
        let decision = filter.filter(&bitmap_above).await.unwrap();
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert!(filter.deletions.is_empty());
    }

    #[tokio::test]
    async fn should_advance_phases_through_a_full_compaction() {
        // given - a deleted vector 5 with term "fox" in field "body"
        let mut filter = VectorCompactionFilter::new(None);

        // sentinel -> CollectDeletes
        filter.filter(&sentinel_entry()).await.unwrap();
        assert_eq!(filter.phase, FilterPhase::CollectDeletes);

        // deletions bitmap (stays in CollectDeletes)
        filter.filter(&deletions_entry(&[5])).await.unwrap();
        assert_eq!(filter.phase, FilterPhase::CollectDeletes);

        // term postings -> CollectPostings
        let postings = merge_entry(
            TermPostingsKey::new("body", "fox").encode(),
            TermPostingsValue::from_postings(vec![posting(5, 1, 1)]).encode_to_bytes(),
        );
        filter.filter(&postings).await.unwrap();
        assert!(matches!(filter.phase, FilterPhase::CollectPostings(_)));

        // term stats (same key) -> CollectTermStats
        let stats = merge_entry(
            TermStatsKey::new("body", "fox").encode(),
            TermStatsValue::new(1).encode_to_bytes(),
        );
        filter.filter(&stats).await.unwrap();
        assert!(matches!(filter.phase, FilterPhase::CollectTermStats(_)));

        // per-vector field stats -> CollectVectorFieldStats
        let vfs = value_entry(
            VectorFieldStatsKey::new(VectorId::data_vector_id(5)).encode(),
            VectorFieldStatsValue::new(vec![("body".to_string(), 1)]).encode_to_bytes(),
        );
        filter.filter(&vfs).await.unwrap();
        assert_eq!(filter.phase, FilterPhase::CollectVectorFieldStats);

        // per-field corpus stats -> CollectFieldStats
        let fs = merge_entry(
            FieldStatsKey::new("body").encode(),
            FieldStatsValue::new(1, 1, 1).encode_to_bytes(),
        );
        filter.filter(&fs).await.unwrap();
        assert_eq!(filter.phase, FilterPhase::CollectFieldStats);
    }

    #[tokio::test]
    async fn should_error_on_out_of_order_term_stats() {
        // given - a term stats key whose postings were never seen (e.g. arriving
        // for a different term than the one being collected)
        let mut filter = VectorCompactionFilter::new(None);
        filter.filter(&sentinel_entry()).await.unwrap();
        let postings = merge_entry(
            TermPostingsKey::new("body", "fox").encode(),
            TermPostingsValue::from_postings(vec![posting(1, 1, 1)]).encode_to_bytes(),
        );
        filter.filter(&postings).await.unwrap();

        // when - stats for a *different* term arrive while collecting "fox"
        let mismatched_stats = merge_entry(
            TermStatsKey::new("body", "wolf").encode(),
            TermStatsValue::new(1).encode_to_bytes(),
        );
        let result = filter.filter(&mismatched_stats).await;

        // then - the phase check fails the compaction
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_allow_field_stats_directly_after_terms_when_no_deletes() {
        // given - an insert-only compaction: sentinel, postings, stats, then field
        // stats with no deletions bitmap and no per-vector field stats in between
        let mut filter = VectorCompactionFilter::new(None);
        filter.filter(&sentinel_entry()).await.unwrap();
        let postings = merge_entry(
            TermPostingsKey::new("body", "fox").encode(),
            TermPostingsValue::from_postings(vec![posting(1, 1, 1)]).encode_to_bytes(),
        );
        filter.filter(&postings).await.unwrap();
        let stats = merge_entry(
            TermStatsKey::new("body", "fox").encode(),
            TermStatsValue::new(1).encode_to_bytes(),
        );
        filter.filter(&stats).await.unwrap();

        // when - field stats follow the term phase directly (no vfs)
        let field_stats = merge_entry(
            FieldStatsKey::new("body").encode(),
            FieldStatsValue::new(1, 3, 0).encode_to_bytes(),
        );
        let decision = filter.filter(&field_stats).await.unwrap();

        // then - with no deletes there is no pending delta, so it passes through
        // unchanged, and the phase advances to CollectFieldStats
        assert_eq!(decision, CompactionFilterDecision::Keep);
        assert_eq!(filter.phase, FilterPhase::CollectFieldStats);
    }
}
