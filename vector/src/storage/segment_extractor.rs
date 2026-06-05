//! SlateDB segment extractor for the vector subsystem (RFC-0006).
//!
//! Implements [`slatedb::PrefixExtractor`] over the vector key layout, mapping
//! every record to the 3-byte routing prefix `[subsystem, segment, version]`.
//! When configured on the `slatedb::DbBuilder`, the extractor causes SlateDB to
//! route writes for each vector [`Segment`](crate::serde::Segment) into its own
//! SlateDB segment, so the FTS segment can be compacted (and its accumulated
//! deletes applied) as a unit without rewriting the ANN or default-segment data.
//!
//! See `vector/rfcs/0006-text-search.md` for the key layout this extractor
//! relies on, and `slatedb::prefix_extractor::PrefixExtractor` for the contract
//! the impl satisfies. The `log` crate's `segment_extractor` is the sibling
//! implementation this is modeled on.
//!
//! # Naming
//!
//! The extractor's [`name`](slatedb::PrefixExtractor::name) is persisted in the
//! SlateDB manifest and validated on open. Changes to the routing logic
//! (including any future bump of the vector `KEY_VERSION`) must change the name
//! as well, so a database created under different routing rules is rejected with
//! a `SegmentExtractorMismatch` error rather than silently mis-routed.

use std::sync::Arc;

use common::StorageBuilder;
use slatedb::{PrefixExtractor, PrefixTarget};

use crate::serde::{KEY_VERSION, SUBSYSTEM};

/// Routing prefix length: `[subsystem(1), segment(1), version(1)]`.
///
/// The `record_tag` byte at position 3 deliberately sits *outside* the
/// extracted prefix so all record types within a segment share one routing
/// prefix and land in the same SlateDB segment.
pub(crate) const ROUTING_PREFIX_LEN: usize = 3;

/// Stable, persisted identifier for this extractor's routing rules. Bump
/// whenever the routing logic changes in a way that would re-route existing
/// keys (e.g. a new `KEY_VERSION` with a different prefix shape).
const EXTRACTOR_NAME: &str = "opendata-vector/v1";

/// Prefix extractor routing vector records to per-segment SlateDB segments.
///
/// Every well-formed vector key has the shape `[SUBSYSTEM, segment, KEY_VERSION,
/// record_tag, ...]` by construction, so this extractor returns
/// `Some(3)` for any well-formed `PrefixTarget::Point` and for any 3+ byte
/// `PrefixTarget::Prefix` matching the subsystem and version bytes.
///
/// **Point inputs that don't match are a bug.** SlateDB rejects writes whose
/// segment extractor returns `None` (or `Some(0)`), so a malformed key would
/// surface as a confusing downstream error rather than a clear failure at the
/// origin. We panic on mismatched `Point` inputs to make the bug obvious. Short
/// or non-conforming `Prefix` scan inputs are permitted to return `None` — the
/// trait contract allows it and slatedb simply skips prefix-based filtering.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct VectorSegmentExtractor;

impl VectorSegmentExtractor {
    /// Returns a shared `Arc<dyn PrefixExtractor>` for handing to
    /// `slatedb::DbBuilder::with_segment_extractor`.
    pub(crate) fn shared() -> Arc<dyn PrefixExtractor> {
        Arc::new(Self)
    }
}

impl PrefixExtractor for VectorSegmentExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            // Stored keys and point-read targets must be well-formed vector
            // keys. A mismatch is a VectorDb bug; panic with the offending bytes
            // rather than letting slatedb reject the write later with an opaque
            // error.
            PrefixTarget::Point(b) => {
                let bytes = b.as_ref();
                assert!(
                    bytes.len() >= ROUTING_PREFIX_LEN
                        && bytes[0] == SUBSYSTEM
                        && bytes[2] == KEY_VERSION,
                    "VectorSegmentExtractor: malformed vector key (subsystem/version mismatch \
                     or under {} bytes): {:02x?}",
                    ROUTING_PREFIX_LEN,
                    bytes
                );
                Some(ROUTING_PREFIX_LEN)
            }
            // Scan prefixes are caller-supplied; a too-short or non-matching
            // prefix is benign — return `None` so any prefix-based filter is
            // skipped (no false negatives, per the trait contract).
            PrefixTarget::Prefix(b) => {
                let bytes = b.as_ref();
                if bytes.len() < ROUTING_PREFIX_LEN
                    || bytes[0] != SUBSYSTEM
                    || bytes[2] != KEY_VERSION
                {
                    None
                } else {
                    Some(ROUTING_PREFIX_LEN)
                }
            }
        }
    }
}

/// Installs the [`VectorSegmentExtractor`] on the builder's underlying SlateDB
/// `DbBuilder`, so every vector record is routed to a per-segment SlateDB
/// segment. No-op for the in-memory backend.
///
/// Every SlateDB-backed writer that opens a vector database must install this
/// (the extractor name is persisted in the manifest and validated on open).
pub(crate) fn builder_with_vector_segments(builder: StorageBuilder) -> StorageBuilder {
    builder.map_slatedb(|db| db.with_segment_extractor(VectorSegmentExtractor::shared()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::key::{CollectionMetaKey, DeletionsKey, PostingListKey};
    use crate::serde::vector_id::VectorId;
    use bytes::Bytes;

    fn point(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Point(Bytes::copy_from_slice(bytes))
    }

    fn prefix(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Prefix(Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn should_return_stable_name() {
        // when / then — must match the persisted manifest value
        assert_eq!(VectorSegmentExtractor.name(), "opendata-vector/v1");
    }

    #[test]
    fn should_extract_three_bytes_from_well_formed_key() {
        // given
        let extractor = VectorSegmentExtractor;
        let key = PostingListKey::new(VectorId::centroid_id(1, 7)).encode();

        // when
        let n = extractor.prefix_len(&point(&key));

        // then — routing prefix is [SUBSYSTEM, segment, KEY_VERSION]
        assert_eq!(n, Some(ROUTING_PREFIX_LEN));
        let extracted = &key[..n.unwrap()];
        assert_eq!(extracted[0], SUBSYSTEM);
        assert_eq!(extracted[1], crate::serde::Segment::Ann.as_byte());
        assert_eq!(extracted[2], KEY_VERSION);
    }

    #[test]
    fn should_route_different_segments_to_different_prefixes() {
        // given — a Default-segment key and an FTS-segment key
        let extractor = VectorSegmentExtractor;
        let default_key = CollectionMetaKey.encode();
        let fts_key = DeletionsKey::new().encode();

        // when
        let n1 = extractor.prefix_len(&point(&default_key)).unwrap();
        let n2 = extractor.prefix_len(&point(&fts_key)).unwrap();

        // then — both extract 3-byte prefixes, but the prefixes differ (the
        // segment byte at index 1 differs)
        assert_eq!(n1, ROUTING_PREFIX_LEN);
        assert_eq!(n2, ROUTING_PREFIX_LEN);
        assert_ne!(&default_key[..n1], &fts_key[..n2]);
    }

    #[test]
    fn should_return_some_for_well_formed_scan_prefix() {
        // given — a 3-byte scan prefix targeting the ANN segment
        let extractor = VectorSegmentExtractor;
        let scan_prefix = [SUBSYSTEM, crate::serde::Segment::Ann.as_byte(), KEY_VERSION];

        // when / then
        assert_eq!(
            extractor.prefix_len(&prefix(&scan_prefix)),
            Some(ROUTING_PREFIX_LEN)
        );
    }

    #[test]
    fn should_return_none_for_prefix_shorter_than_routing_prefix() {
        // given — a 2-byte scan prefix
        let extractor = VectorSegmentExtractor;
        let short = [SUBSYSTEM, crate::serde::Segment::Default.as_byte()];

        // when / then — short Prefix returns None (scan-side, benign)
        assert_eq!(extractor.prefix_len(&prefix(&short)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_wrong_subsystem() {
        // given
        let extractor = VectorSegmentExtractor;
        let bad = [0xFF, 0x00, KEY_VERSION, 0x10];

        // when / then — non-vector Prefix returns None
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_wrong_version() {
        // given
        let extractor = VectorSegmentExtractor;
        let bad = [SUBSYSTEM, 0x00, 0xFF, 0x10];

        // when / then — Prefix with wrong version returns None
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    #[should_panic(expected = "malformed vector key")]
    fn should_panic_on_point_shorter_than_routing_prefix() {
        let extractor = VectorSegmentExtractor;
        let short = [SUBSYSTEM, 0x00];
        let _ = extractor.prefix_len(&point(&short));
    }

    #[test]
    #[should_panic(expected = "malformed vector key")]
    fn should_panic_on_point_with_wrong_subsystem() {
        let extractor = VectorSegmentExtractor;
        let bad = [0xFF, 0x00, KEY_VERSION, 0x10];
        let _ = extractor.prefix_len(&point(&bad));
    }

    #[test]
    #[should_panic(expected = "malformed vector key")]
    fn should_panic_on_point_with_wrong_version() {
        let extractor = VectorSegmentExtractor;
        let bad = [SUBSYSTEM, 0x00, 0xFF, 0x10];
        let _ = extractor.prefix_len(&point(&bad));
    }

    #[test]
    fn should_satisfy_prefix_extension_invariant_for_well_formed_prefix() {
        // given — a 3-byte scan prefix that targets the FTS segment
        let extractor = VectorSegmentExtractor;
        let scan_prefix = [SUBSYSTEM, crate::serde::Segment::Fts.as_byte(), KEY_VERSION];

        // when
        let n = extractor.prefix_len(&prefix(&scan_prefix)).unwrap();
        assert_eq!(n, ROUTING_PREFIX_LEN);

        // then — any extension q of scan_prefix must satisfy Point(q) = Some(n)
        // and q[..n] == scan_prefix[..n].
        let extension: Vec<u8> = scan_prefix
            .iter()
            .copied()
            .chain([0xC0, 0x00, 0x01])
            .collect();
        let point_n = extractor.prefix_len(&point(&extension)).unwrap();
        assert_eq!(point_n, n);
        assert_eq!(&extension[..point_n], &scan_prefix[..n]);
    }
}
