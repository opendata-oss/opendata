//! Compaction scheduling for LogDb.
//!
//! Implements the policy described in RFC 0005. The scheduler classifies each
//! SlateDB-level segment in the manifest as system / active / sealed /
//! orphaned and proposes compactions or drains accordingly. See
//! [`propose_compactions`] for the per-state behaviour.
//!
//! The scheduler doesn't read storage. It reads a compactor-facing view (a
//! shared [`ArcSwap`] the writer publishes into) and derives the live
//! user-segment range from the two segment-id watermarks published there.
//! Because retention only deletes from the low end of the log (RFC 0005), the
//! live set is always a contiguous id range, so two integers fully describe it.

use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use slatedb::compactor::{
    CompactionScheduler, CompactionSchedulerSupplier, CompactionSpec, CompactorStateView, SourceId,
};
use slatedb::config::CompactorOptions;
use slatedb::manifest::Segment;
use ulid::Ulid;

use crate::config::LogCompactionOptions;
use crate::model::SegmentId;
use crate::segment_extractor::ROUTING_PREFIX_LEN;
use crate::serde::FIRST_USER_SEGMENT_ID;
/// Compactor-facing view of the user-segment live set.
///
/// `last_segment_id` advances at written latency so active/sealed segment
/// compactions can respond quickly to new segments. `last_deleted_segment_id`
/// advances only once the corresponding writer operation is durable, so drain
/// proposals never run ahead of durable `SegmentMeta` deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompactorView {
    pub last_segment_id: Option<SegmentId>,
    pub last_deleted_segment_id: Option<SegmentId>,
}

/// Cell shared between the builder, the compaction supplier, and the writer.
/// The writer publishes the latest `CompactorView` here on each segment
/// change; the scheduler reads the most recent value. Initialised to an empty
/// live set (`None`/`None`), which `compute_bounds` treats as uninitialized.
pub(crate) type CompactorViewCell = Arc<ArcSwap<CompactorView>>;

/// L0 SST count at or above which the system segment is consolidated.
const SYSTEM_SEGMENT_L0_THRESHOLD: usize = 2;

/// Bounds of the contiguous live user-segment range, `first_id..=last_id`.
/// Contiguity holds because retention only deletes from the low end (RFC 0005).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveSetBounds {
    pub first_id: SegmentId,
    pub last_id: SegmentId,
}

impl LiveSetBounds {
    /// Returns true if `id` is in the live range.
    pub(crate) fn contains(&self, id: SegmentId) -> bool {
        id >= self.first_id && id <= self.last_id
    }
}

/// Hands a [`LogCompactionScheduler`] to SlateDB at compactor construction time.
pub(crate) struct LogCompactionSchedulerSupplier {
    pub(crate) cell: CompactorViewCell,
    pub(crate) options: LogCompactionOptions,
}

impl CompactionSchedulerSupplier for LogCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        _options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(LogCompactionScheduler {
            cell: Arc::clone(&self.cell),
            options: self.options.clone(),
        })
    }
}

/// LogDb's `CompactionScheduler` impl. Reads the writer's live-set watermarks
/// from a shared `ArcSwap` cell; never touches storage.
pub(crate) struct LogCompactionScheduler {
    cell: CompactorViewCell,
    options: LogCompactionOptions,
}

impl CompactionScheduler for LogCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let view = self.cell.load();
        let bounds = compute_bounds(view.last_segment_id, view.last_deleted_segment_id);
        let active = collect_active_compactions(state);
        let segments: Vec<SegmentSnapshot> = state
            .manifest()
            .segments()
            .iter()
            .map(SegmentSnapshot::from_segment)
            .collect();
        propose_compactions(&segments, bounds, &active, &self.options)
    }
}

/// View of the compactor's in-flight work. Used to (1) skip segments that
/// already have a compaction running and (2) pick a destination SR id that
/// doesn't collide with one already in flight.
#[derive(Debug, Default)]
pub(crate) struct ActiveCompactionsView {
    pub segment_prefixes: HashSet<Bytes>,
    pub max_destination: Option<u32>,
}

/// Per-segment fields the policy reads. Decoupled from slatedb's `Segment`
/// so tests can construct one without building a real slatedb manifest.
#[derive(Debug, Clone)]
pub(crate) struct SegmentSnapshot {
    pub prefix: Bytes,
    pub l0_ids: Vec<Ulid>,
    pub sr_ids: Vec<u32>,
}

impl SegmentSnapshot {
    fn from_segment(segment: &Segment) -> Self {
        Self {
            prefix: segment.prefix().clone(),
            l0_ids: segment.l0().iter().map(|sst| sst.id).collect(),
            sr_ids: segment.compacted().iter().map(|sr| sr.id).collect(),
        }
    }

    /// L0 SSTs as `SourceId::SstView`s.
    fn l0_sources(&self) -> impl Iterator<Item = SourceId> + '_ {
        self.l0_ids.iter().copied().map(SourceId::SstView)
    }

    /// Sorted runs as `SourceId::SortedRun`s.
    fn sr_sources(&self) -> impl Iterator<Item = SourceId> + '_ {
        self.sr_ids.iter().copied().map(SourceId::SortedRun)
    }

    /// All sources (L0 SSTs followed by sorted runs).
    fn all_sources(&self) -> impl Iterator<Item = SourceId> + '_ {
        self.l0_sources().chain(self.sr_sources())
    }
}

/// Allocates the current `next_sr` and bumps the counter.
fn alloc_sr_id(next_sr: &mut u32) -> u32 {
    let id = *next_sr;
    *next_sr += 1;
    id
}

/// Collects the segment prefixes with an in-flight compaction and the max
/// destination SR id among in-flight tiered compactions. Drain specs have no
/// destination and contribute nothing.
fn collect_active_compactions(state: &CompactorStateView) -> ActiveCompactionsView {
    let Some(compactions) = state.compactions() else {
        return ActiveCompactionsView::default();
    };
    let mut prefixes = HashSet::new();
    let mut max_dst: Option<u32> = None;
    for compaction in compactions.recent_compactions().filter(|c| c.active()) {
        prefixes.insert(compaction.spec().segment().clone());
        if let Some(dst) = compaction.spec().destination() {
            max_dst = Some(max_dst.map_or(dst, |cur| cur.max(dst)));
        }
    }
    ActiveCompactionsView {
        segment_prefixes: prefixes,
        max_destination: max_dst,
    }
}

/// Decodes a SegmentId from a 6-byte routing prefix
/// (`[SUBSYSTEM, KEY_VERSION, seg_id(4 BE)]`).
fn decode_segment_id(prefix: &[u8]) -> Option<SegmentId> {
    if prefix.len() != ROUTING_PREFIX_LEN {
        return None;
    }
    Some(u32::from_be_bytes([
        prefix[2], prefix[3], prefix[4], prefix[5],
    ]))
}

/// Returns a sorted-run id safe to use as a fresh compaction destination.
/// SR ids are globally unique across trees (SlateDB RFC-0024), so the
/// counter must skip past every committed run and every in-flight destination.
fn next_fresh_sr_id(segments: &[SegmentSnapshot], max_in_flight_dst: Option<u32>) -> u32 {
    let max_committed = segments
        .iter()
        .flat_map(|seg| seg.sr_ids.iter().copied())
        .max();
    [max_committed, max_in_flight_dst]
        .into_iter()
        .flatten()
        .max()
        .map(|id| id.saturating_add(1))
        .unwrap_or(0)
}

/// Pure policy function. See module docs and RFC 0005 for the policy.
///
/// - **System segment** (id 0): consolidate when L0 ≥ 2.
/// - **Active** (largest live id, or largest manifest id when bounds is None):
///   propose an L0-relief compaction when L0 ≥ `min_l0_per_compaction`.
/// - **Sealed** (live, not active): one-shot final consolidation into a single
///   SR unless already in steady state. Suppressed when bounds is `None`.
/// - **Orphaned** (in manifest but not in live set): drain. Suppressed when
///   bounds is `None`.
/// - **Invariant**: panics if a manifest segment has id > max live id (writer
///   protocol violation per RFC 0005).
pub(crate) fn propose_compactions(
    segments: &[SegmentSnapshot],
    bounds: Option<LiveSetBounds>,
    active: &ActiveCompactionsView,
    options: &LogCompactionOptions,
) -> Vec<CompactionSpec> {
    let mut next_sr = next_fresh_sr_id(segments, active.max_destination);
    let mut out = Vec::new();

    // Determine "what id we treat as active" — either the published max live
    // id, or the manifest-derived fallback when bounds is None.
    let manifest_max_user = segments
        .iter()
        .filter_map(|s| decode_segment_id(&s.prefix))
        .filter(|id| *id > 0)
        .max();
    let active_id_for_policy = bounds.map(|b| b.last_id).or(manifest_max_user);

    for segment in segments {
        if active.segment_prefixes.contains(&segment.prefix) {
            continue;
        }
        let Some(id) = decode_segment_id(&segment.prefix) else {
            // `LogSegmentExtractor` only produces 6-byte prefixes, so an
            // undecodable prefix indicates a manifest from a different
            // extractor configuration. Skip it rather than misclassify.
            tracing::warn!(
                "compaction scheduler: undecodable segment prefix (len {}), skipping",
                segment.prefix.len(),
            );
            continue;
        };

        if id == 0 {
            if segment.l0_ids.len() >= SYSTEM_SEGMENT_L0_THRESHOLD
                && let Some(spec) = build_consolidation_spec(segment, &mut next_sr)
            {
                out.push(spec);
            }
            continue;
        }

        if let Some(b) = bounds {
            if id > b.last_id {
                panic!(
                    "LogCompactionScheduler invariant violated: manifest has \
                     segment id {id} > max live id {} (writer protocol broken)",
                    b.last_id
                );
            }
            if !b.contains(id) {
                out.push(build_drain_spec(segment));
                continue;
            }
        }

        match active_id_for_policy {
            Some(active_id) if id == active_id => {
                if segment.l0_ids.len() >= options.min_l0_per_compaction
                    && let Some(spec) =
                        build_active_l0_spec(segment, &mut next_sr, options.max_l0_per_compaction)
                {
                    out.push(spec);
                }
            }
            _ => {
                if bounds.is_some()
                    && !is_single_sr_steady(segment)
                    && let Some(spec) = build_consolidation_spec(segment, &mut next_sr)
                {
                    out.push(spec);
                }
            }
        }
    }
    out
}

/// True when a segment is already in single-SR steady state: empty L0 and at
/// most one compacted SR.
fn is_single_sr_steady(segment: &SegmentSnapshot) -> bool {
    segment.l0_ids.is_empty() && segment.sr_ids.len() <= 1
}

/// Builds an L0-relief compaction spec for an active segment.
fn build_active_l0_spec(
    segment: &SegmentSnapshot,
    next_sr: &mut u32,
    max_l0: usize,
) -> Option<CompactionSpec> {
    let sources: Vec<SourceId> = segment.l0_sources().take(max_l0).collect();
    if sources.is_empty() {
        return None;
    }
    Some(CompactionSpec::for_segment(
        segment.prefix.clone(),
        sources,
        alloc_sr_id(next_sr),
    ))
}

/// Full consolidation: merge L0 + all SRs into the lowest-id SR among them,
/// or a fresh SR if none exist.
fn build_consolidation_spec(
    segment: &SegmentSnapshot,
    next_sr: &mut u32,
) -> Option<CompactionSpec> {
    let sources: Vec<SourceId> = segment.all_sources().collect();
    if sources.is_empty() {
        return None;
    }
    let dst = segment
        .sr_ids
        .iter()
        .copied()
        .min()
        .unwrap_or_else(|| alloc_sr_id(next_sr));
    Some(CompactionSpec::for_segment(
        segment.prefix.clone(),
        sources,
        dst,
    ))
}

/// Drain spec retiring all SSTs/SRs in the segment.
fn build_drain_spec(segment: &SegmentSnapshot) -> CompactionSpec {
    CompactionSpec::drain_segment(segment.prefix.clone(), segment.all_sources().collect())
}

/// Derives the live user-segment range from the two watermarks on
/// `CompactorView`. Returns `None` if no user segment has been created, or if
/// the inferred range is empty.
pub(crate) fn compute_bounds(
    last_segment_id: Option<SegmentId>,
    last_deleted_segment_id: Option<SegmentId>,
) -> Option<LiveSetBounds> {
    let last = last_segment_id?;
    let first = last_deleted_segment_id
        .map(|d| d + 1)
        .unwrap_or(FIRST_USER_SEGMENT_ID);
    if first > last {
        return None;
    }
    Some(LiveSetBounds {
        first_id: first,
        last_id: last,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_includes_endpoints() {
        let bounds = LiveSetBounds {
            first_id: 3,
            last_id: 5,
        };
        assert!(bounds.contains(3));
        assert!(bounds.contains(4));
        assert!(bounds.contains(5));
    }

    #[test]
    fn contains_excludes_outside() {
        let bounds = LiveSetBounds {
            first_id: 3,
            last_id: 5,
        };
        assert!(!bounds.contains(2));
        assert!(!bounds.contains(6));
    }

    #[test]
    fn singleton_range_contains_only_self() {
        let bounds = LiveSetBounds {
            first_id: 7,
            last_id: 7,
        };
        assert!(bounds.contains(7));
        assert!(!bounds.contains(6));
        assert!(!bounds.contains(8));
    }

    #[test]
    fn compute_bounds_returns_none_when_no_segments_created() {
        assert_eq!(compute_bounds(None, None), None);
        assert_eq!(compute_bounds(None, Some(2)), None);
    }

    #[test]
    fn compute_bounds_starts_at_first_user_segment_when_no_deletes() {
        let bounds = compute_bounds(Some(5), None).unwrap();
        assert_eq!(bounds.first_id, FIRST_USER_SEGMENT_ID);
        assert_eq!(bounds.last_id, 5);
    }

    #[test]
    fn compute_bounds_starts_one_past_last_deleted() {
        let bounds = compute_bounds(Some(7), Some(3)).unwrap();
        assert_eq!(bounds.first_id, 4);
        assert_eq!(bounds.last_id, 7);
    }

    #[test]
    fn compute_bounds_returns_none_when_range_is_empty() {
        // Hypothetical: last_segment_id <= last_deleted_segment_id (unreachable
        // under writer protocol since the active segment can't be deleted).
        assert_eq!(compute_bounds(Some(3), Some(3)), None);
        assert_eq!(compute_bounds(Some(3), Some(5)), None);
    }

    // ---- propose_compactions tests ----

    use crate::serde::{KEY_VERSION, SUBSYSTEM};

    fn prefix_for(id: SegmentId) -> Bytes {
        let bytes = id.to_be_bytes();
        Bytes::from(vec![
            SUBSYSTEM,
            KEY_VERSION,
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
        ])
    }

    fn snapshot(id: SegmentId, l0: usize, srs: &[u32]) -> SegmentSnapshot {
        SegmentSnapshot {
            prefix: prefix_for(id),
            l0_ids: (0..l0).map(|_| Ulid::new()).collect(),
            sr_ids: srs.to_vec(),
        }
    }

    fn default_options() -> LogCompactionOptions {
        LogCompactionOptions::default()
    }

    fn bounds(first: SegmentId, last: SegmentId) -> Option<LiveSetBounds> {
        Some(LiveSetBounds {
            first_id: first,
            last_id: last,
        })
    }

    #[test]
    fn propose_is_noop_on_empty_manifest() {
        let specs = propose_compactions(
            &[],
            bounds(1, 1),
            &ActiveCompactionsView::default(),
            &default_options(),
        );
        assert!(specs.is_empty());
    }

    #[test]
    fn propose_consolidates_system_segment_when_l0_threshold_reached() {
        let segments = vec![snapshot(0, 2, &[])];
        let specs = propose_compactions(
            &segments,
            None,
            &ActiveCompactionsView::default(),
            &default_options(),
        );

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].segment(), &prefix_for(0));
        assert_eq!(specs[0].sources().len(), 2);
        assert!(!specs[0].is_drain());
    }

    #[test]
    fn propose_leaves_system_segment_below_threshold_alone() {
        let segments = vec![snapshot(0, 1, &[])];
        let specs = propose_compactions(
            &segments,
            None,
            &ActiveCompactionsView::default(),
            &default_options(),
        );
        assert!(specs.is_empty());
    }

    #[test]
    fn propose_l0_compacts_active_when_threshold_reached() {
        // active = id 3, L0 = min_l0_per_compaction (default 4) → compact.
        // L0 count is below max_l0_per_compaction, so all L0s are folded in.
        let segments = vec![snapshot(3, 4, &[])];
        let opts = default_options();
        let specs = propose_compactions(
            &segments,
            bounds(1, 3),
            &ActiveCompactionsView::default(),
            &opts,
        );

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].segment(), &prefix_for(3));
        assert_eq!(specs[0].sources().len(), 4);
        assert!(!specs[0].is_drain());
    }

    #[test]
    fn propose_l0_caps_at_max_l0_per_compaction() {
        // active = id 3 with more L0s than `max_l0_per_compaction` (default 8).
        // The spec must include exactly the cap.
        let segments = vec![snapshot(3, 12, &[])];
        let opts = default_options();
        let specs = propose_compactions(
            &segments,
            bounds(1, 3),
            &ActiveCompactionsView::default(),
            &opts,
        );

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].sources().len(), opts.max_l0_per_compaction);
    }

    #[test]
    fn propose_skips_active_when_l0_below_threshold() {
        let segments = vec![snapshot(3, 3, &[])]; // 3 < default 4
        let specs = propose_compactions(
            &segments,
            bounds(1, 3),
            &ActiveCompactionsView::default(),
            &default_options(),
        );
        assert!(specs.is_empty());
    }

    #[test]
    fn propose_consolidates_sealed_when_not_in_steady_state() {
        // Sealed id 2, with 1 L0 and 2 SRs → needs consolidation.
        // Active id 3 with no work; we expect only the sealed work emitted.
        let segments = vec![snapshot(2, 1, &[10, 11]), snapshot(3, 0, &[])];
        let specs = propose_compactions(
            &segments,
            bounds(1, 3),
            &ActiveCompactionsView::default(),
            &default_options(),
        );

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].segment(), &prefix_for(2));
        assert!(!specs[0].is_drain());
        // Destination is min existing SR id.
        assert_eq!(specs[0].destination(), Some(10));
    }

    #[test]
    fn propose_skips_sealed_already_in_steady_state() {
        // Sealed id 2 has L0=0 and exactly 1 SR → steady state, no work.
        let segments = vec![snapshot(2, 0, &[5]), snapshot(3, 0, &[])];
        let specs = propose_compactions(
            &segments,
            bounds(1, 3),
            &ActiveCompactionsView::default(),
            &default_options(),
        );
        assert!(specs.is_empty());
    }

    #[test]
    fn propose_drains_orphaned_segment() {
        // Live range [3,4], but manifest also has id 2 → orphaned.
        let segments = vec![
            snapshot(2, 0, &[7]),
            snapshot(3, 0, &[]),
            snapshot(4, 0, &[]),
        ];
        let specs = propose_compactions(
            &segments,
            bounds(3, 4),
            &ActiveCompactionsView::default(),
            &default_options(),
        );

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].segment(), &prefix_for(2));
        assert!(specs[0].is_drain());
    }

    #[test]
    fn propose_suppresses_orphan_drains_when_bounds_uninitialized() {
        // Without bounds, anything that looks orphaned would otherwise be a
        // false positive (we just haven't seen the writer's view yet).
        let segments = vec![snapshot(2, 0, &[]), snapshot(3, 0, &[])];
        let specs = propose_compactions(
            &segments,
            None,
            &ActiveCompactionsView::default(),
            &default_options(),
        );
        assert!(specs.is_empty());
    }

    #[test]
    fn propose_treats_largest_manifest_id_as_active_when_bounds_uninitialized() {
        // Without bounds, fallback: largest manifest id (3) is active, and we
        // only do L0 work — sealed (2) is left alone.
        let segments = vec![snapshot(2, 5, &[]), snapshot(3, 5, &[])];
        let specs = propose_compactions(
            &segments,
            None,
            &ActiveCompactionsView::default(),
            &default_options(),
        );

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].segment(), &prefix_for(3));
    }

    #[test]
    fn propose_skips_segments_with_active_compactions() {
        // Active id 3 needs L0 compaction, but it's already being compacted.
        let segments = vec![snapshot(3, 4, &[])];
        let mut active = ActiveCompactionsView::default();
        active.segment_prefixes.insert(prefix_for(3));
        let specs = propose_compactions(&segments, bounds(1, 3), &active, &default_options());
        assert!(specs.is_empty());
    }

    #[test]
    fn next_fresh_sr_id_skips_past_in_flight_destinations() {
        // Committed SR id 5; in-flight compaction targeting 10. The next
        // fresh id must be 11, not 6.
        let segments = vec![snapshot(2, 0, &[5])];
        let active = ActiveCompactionsView {
            max_destination: Some(10),
            ..ActiveCompactionsView::default()
        };
        assert_eq!(next_fresh_sr_id(&segments, active.max_destination), 11);
    }

    #[test]
    fn next_fresh_sr_id_uses_committed_when_no_in_flight() {
        let segments = vec![snapshot(2, 0, &[5])];
        assert_eq!(next_fresh_sr_id(&segments, None), 6);
    }

    #[test]
    fn next_fresh_sr_id_is_zero_for_empty_state() {
        assert_eq!(next_fresh_sr_id(&[], None), 0);
    }

    #[test]
    fn propose_uses_fresh_destination_above_in_flight() {
        // Active id 3 needs L0 compaction. Committed max SR=5; in-flight
        // compaction targets 10. The new spec must target 11.
        let segments = vec![snapshot(3, 4, &[5])];
        let active = ActiveCompactionsView {
            // Note: only the destination is in flight; the segment isn't, so
            // we still propose work for it.
            max_destination: Some(10),
            ..ActiveCompactionsView::default()
        };
        let specs = propose_compactions(&segments, bounds(1, 3), &active, &default_options());
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].destination(), Some(11));
    }

    #[test]
    #[should_panic(expected = "invariant violated")]
    fn propose_panics_when_manifest_has_id_above_max_live() {
        // Manifest has id 5 but writer's max is 3 → broken writer protocol.
        let segments = vec![snapshot(5, 0, &[])];
        let _ = propose_compactions(
            &segments,
            bounds(1, 3),
            &ActiveCompactionsView::default(),
            &default_options(),
        );
    }

    #[test]
    fn propose_handles_mixed_states_in_one_tick() {
        // System (consolidate) + sealed (consolidate) + orphan (drain) +
        // active (L0 compact, threshold reached).
        let segments = vec![
            snapshot(0, 2, &[]),  // system needs consolidation
            snapshot(1, 0, &[]),  // orphan: not in [2..=4]
            snapshot(2, 1, &[8]), // sealed, not steady-state → consolidate
            snapshot(3, 0, &[9]), // sealed, steady-state (0 L0 + 1 SR) → skip
            snapshot(4, 4, &[]),  // active, L0 at threshold → compact
        ];
        let specs = propose_compactions(
            &segments,
            bounds(2, 4),
            &ActiveCompactionsView::default(),
            &default_options(),
        );

        let kinds: Vec<_> = specs
            .iter()
            .map(|s| (s.segment().clone(), s.is_drain()))
            .collect();
        assert!(kinds.contains(&(prefix_for(0), false)));
        assert!(kinds.contains(&(prefix_for(1), true)));
        assert!(kinds.contains(&(prefix_for(2), false)));
        assert!(kinds.contains(&(prefix_for(4), false)));
        // Sealed id 3 was steady — must not be in the output.
        assert!(!kinds.iter().any(|(p, _)| p == &prefix_for(3)));
        assert_eq!(kinds.len(), 4);
    }
}
