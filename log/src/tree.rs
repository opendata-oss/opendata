//! Read-only summary of how a log's data is distributed across the SlateDB
//! LSM tree.
//!
//! The log installs a [`crate::segment_extractor::LogSegmentExtractor`] on
//! every slatedb-backed writer, so SlateDB routes each LogDb segment's records
//! into its own SlateDB segment — an independent LSM sub-tree with its own L0
//! SSTs and compacted sorted runs. [`TreeSummary`] reads a manifest snapshot
//! and reports that shape per segment: SST counts, sorted-run structure, and
//! estimated sizes.
//!
//! The summary is derived purely from the manifest (SST view counts and
//! [`slatedb::manifest::SsTableView::estimate_size`]) — it never reads SST
//! files — so it is cheap and safe to run against a live log.

use bytes::Bytes;
use slatedb::manifest::{Segment, SortedRun, SsTableView, VersionedManifest};

use crate::model::SegmentId;
use crate::segment_extractor::ROUTING_PREFIX_LEN;
use crate::serde::{KEY_VERSION, SUBSYSTEM};

/// Distribution of a single SST level: either L0 or one compacted sorted run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LevelSummary {
    /// Number of SSTs in this level.
    pub num_ssts: usize,
    /// Sum of per-SST estimated sizes, in bytes.
    pub estimated_bytes: u64,
}

/// LSM shape of one SlateDB segment — the sub-tree holding a single LogDb
/// segment's records (or the unsegmented top-level tree).
#[derive(Clone, Debug)]
pub struct SegmentTree {
    /// LogDb segment id decoded from the routing prefix, when the prefix
    /// matches the log key layout. `Some(0)` is the system segment; `None`
    /// means the prefix did not decode (e.g. the unsegmented top-level tree).
    pub segment_id: Option<SegmentId>,
    /// Raw routing prefix this segment was keyed by. Empty for the top-level
    /// (unsegmented) tree.
    pub prefix: Bytes,
    /// Uncompacted L0 SSTs (the newest writes).
    pub l0: LevelSummary,
    /// Compacted sorted runs, in manifest order. Each run is one "level".
    pub runs: Vec<LevelSummary>,
}

impl SegmentTree {
    /// Total SST count across L0 and every sorted run.
    pub fn total_ssts(&self) -> usize {
        self.l0.num_ssts + self.runs.iter().map(|r| r.num_ssts).sum::<usize>()
    }

    /// Total estimated bytes across L0 and every sorted run.
    pub fn estimated_bytes(&self) -> u64 {
        self.l0.estimated_bytes + self.runs.iter().map(|r| r.estimated_bytes).sum::<u64>()
    }

    /// True when this segment holds no SSTs at all.
    pub fn is_empty(&self) -> bool {
        self.total_ssts() == 0
    }
}

/// Manifest-level facts plus the per-segment data distribution.
#[derive(Clone, Debug)]
pub struct TreeSummary {
    /// Monotonic manifest version id.
    pub manifest_id: u64,
    /// Current writer epoch (fences stale writers).
    pub writer_epoch: u64,
    /// Current compactor epoch (fences stale compactors).
    pub compactor_epoch: u64,
    /// Persisted segment-extractor name, if the database routes to segments.
    pub extractor: Option<String>,
    /// Highest sequence number reflected in a flushed L0 SST.
    pub last_l0_seq: u64,
    /// The unsegmented top-level tree. With the log's segment extractor active
    /// this is normally empty, but transient or un-routed data can land here.
    pub unsegmented: SegmentTree,
    /// One entry per SlateDB segment, in manifest order.
    pub segments: Vec<SegmentTree>,
}

impl TreeSummary {
    /// Builds a summary from a manifest snapshot. Pure: reads no SST files.
    pub(crate) fn from_manifest(manifest: &VersionedManifest) -> Self {
        let unsegmented = SegmentTree {
            segment_id: None,
            prefix: Bytes::new(),
            l0: level_from_views(manifest.l0()),
            runs: manifest.compacted().iter().map(run_level).collect(),
        };

        let segments = manifest.segments().iter().map(segment_tree).collect();

        Self {
            manifest_id: manifest.id(),
            writer_epoch: manifest.writer_epoch(),
            compactor_epoch: manifest.compactor_epoch(),
            extractor: manifest.segment_extractor_name().map(str::to_owned),
            last_l0_seq: manifest.last_l0_seq(),
            unsegmented,
            segments,
        }
    }

    /// Total estimated bytes across the top-level tree and every segment.
    pub fn estimated_bytes(&self) -> u64 {
        self.unsegmented.estimated_bytes()
            + self
                .segments
                .iter()
                .map(SegmentTree::estimated_bytes)
                .sum::<u64>()
    }
}

fn segment_tree(segment: &Segment) -> SegmentTree {
    SegmentTree {
        segment_id: decode_segment_id(segment.prefix()),
        prefix: segment.prefix().clone(),
        l0: level_from_views(segment.l0()),
        runs: segment.compacted().iter().map(run_level).collect(),
    }
}

fn level_from_views<'a>(views: impl IntoIterator<Item = &'a SsTableView>) -> LevelSummary {
    let mut summary = LevelSummary::default();
    for view in views {
        summary.num_ssts += 1;
        summary.estimated_bytes += view.estimate_size();
    }
    summary
}

fn run_level(run: &SortedRun) -> LevelSummary {
    LevelSummary {
        num_ssts: run.sst_views.len(),
        estimated_bytes: run.estimate_size(),
    }
}

/// Decodes a SlateDB segment's routing prefix back into a LogDb [`SegmentId`].
///
/// Log keys begin with `[subsystem, version, segment_id(u32 BE), ...]`, and the
/// extractor routes on the first [`ROUTING_PREFIX_LEN`] bytes. Returns `None`
/// when the prefix does not match that layout.
fn decode_segment_id(prefix: &[u8]) -> Option<SegmentId> {
    if prefix.len() >= ROUTING_PREFIX_LEN && prefix[0] == SUBSYSTEM && prefix[1] == KEY_VERSION {
        Some(u32::from_be_bytes([
            prefix[2], prefix[3], prefix[4], prefix[5],
        ]))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_system_and_user_segment_prefixes() {
        // [SUBSYSTEM, KEY_VERSION, segment_id u32 BE]
        let system = [SUBSYSTEM, KEY_VERSION, 0, 0, 0, 0];
        assert_eq!(decode_segment_id(&system), Some(0));

        let user = [SUBSYSTEM, KEY_VERSION, 0, 0, 0, 7];
        assert_eq!(decode_segment_id(&user), Some(7));

        let big = [SUBSYSTEM, KEY_VERSION, 0x01, 0x02, 0x03, 0x04];
        assert_eq!(decode_segment_id(&big), Some(0x0102_0304));
    }

    #[test]
    fn rejects_foreign_or_short_prefixes() {
        assert_eq!(decode_segment_id(&[]), None);
        assert_eq!(decode_segment_id(&[SUBSYSTEM, KEY_VERSION, 0, 0]), None);
        // Wrong subsystem byte.
        assert_eq!(
            decode_segment_id(&[SUBSYSTEM.wrapping_add(1), KEY_VERSION, 0, 0, 0, 1]),
            None
        );
        // Wrong version byte.
        assert_eq!(
            decode_segment_id(&[SUBSYSTEM, KEY_VERSION.wrapping_add(1), 0, 0, 0, 1]),
            None
        );
    }

    #[test]
    fn segment_tree_totals_sum_l0_and_runs() {
        let tree = SegmentTree {
            segment_id: Some(1),
            prefix: Bytes::new(),
            l0: LevelSummary {
                num_ssts: 2,
                estimated_bytes: 100,
            },
            runs: vec![
                LevelSummary {
                    num_ssts: 3,
                    estimated_bytes: 300,
                },
                LevelSummary {
                    num_ssts: 1,
                    estimated_bytes: 50,
                },
            ],
        };
        assert_eq!(tree.total_ssts(), 6);
        assert_eq!(tree.estimated_bytes(), 450);
        assert!(!tree.is_empty());
    }
}
