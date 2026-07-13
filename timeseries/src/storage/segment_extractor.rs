//! SlateDB segment extractor for the timeseries subsystem.
//!
//! Implements [`slatedb::PrefixExtractor`] over the v2 timeseries key
//! layout, mapping every record to the 7-byte routing prefix
//! `[subsystem, version, time_bucket(4 BE), bucket_size]`. When configured on
//! the `slatedb::DbBuilder`, the extractor causes SlateDB to route writes for
//! each `(time_bucket, bucket_size)` pair into its own SlateDB segment, so
//! per-bucket LSM state can be compacted (and eventually drained) as a unit.
//!
//! The `record_type` byte at position 7 deliberately sits *outside* the
//! extracted prefix so all record types for a given bucket (series
//! dictionary, forward index, inverted index, time series samples) share one
//! routing prefix and land in the same SlateDB segment.
//!
//! See the parent `storage` module for how the segments list is consumed by
//! the bucket-discovery path.
//!
//! # Naming
//!
//! The extractor's [`name`](slatedb::PrefixExtractor::name) is persisted in
//! the SlateDB manifest and validated on open. Changes to the routing logic
//! (including any future bump of the timeseries `KEY_VERSION`) must change
//! the name as well, so a database created under different routing rules is
//! rejected with `SlateDBError::SegmentExtractorMismatch` rather than
//! silently mis-routed.

use std::sync::Arc;

use common::serde::subsystem::TIMESERIES;
use slatedb::{PrefixExtractor, PrefixTarget};

use crate::model::{BucketSize, BucketStart, TimeBucket};

/// Routing prefix length: `[subsystem(1), version(1), time_bucket(4 BE),
/// bucket_size(1)]`.
const ROUTING_PREFIX_LEN: usize = 7;

/// Stable, persisted identifier for this extractor's routing rules. Bump
/// whenever the routing logic changes in a way that would re-route existing
/// keys (e.g. a new `KEY_VERSION` with a different prefix shape).
const EXTRACTOR_NAME: &str = "opendata-timeseries/v2";

/// Version byte the extractor expects in the routing prefix.
///
/// This is kept local to the extractor module until the timeseries serde
/// module bumps its own `KEY_VERSION` to `0x02`. Once aligned, the two
/// constants will collapse into a single import.
const KEY_VERSION_V2: u8 = 0x02;

/// Prefix extractor routing timeseries records to per-bucket SlateDB segments.
///
/// Every well-formed timeseries key has the shape `[SUBSYSTEM, KEY_VERSION,
/// time_bucket(4 BE), bucket_size, record_type, ...]` by construction, so
/// this extractor returns `Some(7)` for any well-formed `PrefixTarget::Point`
/// and for any 7+ byte `PrefixTarget::Prefix` matching the same leading two
/// bytes.
///
/// **Point inputs that don't match are a bug.** SlateDB rejects writes whose
/// segment extractor returns `None` (or `Some(0)`) with
/// `SlateDBError::EmptySegmentPrefix` at write time, so a malformed key would
/// surface as a confusing downstream error rather than a clear failure at
/// the origin. We panic on mismatched `Point` inputs to make the bug
/// obvious. Short or non-conforming `Prefix` scan inputs are permitted to
/// return `None` — the trait contract allows it and slatedb simply skips
/// prefix-based filtering in that case.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct TimeseriesSegmentExtractor;

impl TimeseriesSegmentExtractor {
    /// Returns a shared `Arc<dyn PrefixExtractor>` for handing to
    /// `slatedb::DbBuilder::with_segment_extractor`.
    pub(crate) fn shared() -> Arc<dyn PrefixExtractor> {
        Arc::new(Self)
    }
}

impl PrefixExtractor for TimeseriesSegmentExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            // Stored keys and point-read targets must be well-formed
            // timeseries keys. A mismatch is a writer bug; panic with the
            // offending bytes rather than letting slatedb reject the write
            // later with an opaque EmptySegmentPrefix error.
            PrefixTarget::Point(b) => {
                let bytes = b.as_ref();
                assert!(
                    bytes.len() >= ROUTING_PREFIX_LEN
                        && bytes[0] == TIMESERIES
                        && bytes[1] == KEY_VERSION_V2,
                    "TimeseriesSegmentExtractor: malformed timeseries key (subsystem/version \
                     mismatch or under {} bytes): {:02x?}",
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
                    || bytes[0] != TIMESERIES
                    || bytes[1] != KEY_VERSION_V2
                {
                    None
                } else {
                    Some(ROUTING_PREFIX_LEN)
                }
            }
        }
    }
}

/// Decodes a 7-byte routing prefix back into a `TimeBucket`.
///
/// Used by the bucket-discovery path to project the segments list returned by
/// `StorageRead::list_segments` into the set of buckets visible to a query.
/// Returns `None` for any prefix that isn't a well-formed timeseries routing
/// prefix.
///
/// While the timeseries database is restricted to 1-hour buckets, a routing
/// prefix with `bucket_size != 1` is rejected as malformed; relax this when
/// multi-size buckets land.
pub(crate) fn parse_bucket(prefix: &[u8]) -> Option<TimeBucket> {
    if prefix.len() < ROUTING_PREFIX_LEN || prefix[0] != TIMESERIES || prefix[1] != KEY_VERSION_V2 {
        return None;
    }
    let start = BucketStart::from_be_bytes([prefix[2], prefix[3], prefix[4], prefix[5]]);
    let size: BucketSize = prefix[6];
    if size != 1 {
        return None;
    }
    Some(TimeBucket { start, size })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, Bytes, BytesMut};

    fn make_key(bucket_start: u32, bucket_size: u8, record_type: u8, tail: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(ROUTING_PREFIX_LEN + 1 + tail.len());
        buf.put_u8(TIMESERIES);
        buf.put_u8(KEY_VERSION_V2);
        buf.put_u32(bucket_start);
        buf.put_u8(bucket_size);
        buf.put_u8(record_type);
        buf.extend_from_slice(tail);
        buf.freeze()
    }

    fn point(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Point(Bytes::copy_from_slice(bytes))
    }

    fn prefix(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Prefix(Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn should_return_stable_name() {
        // given
        let extractor = TimeseriesSegmentExtractor;

        // when / then — must match the persisted manifest value
        assert_eq!(extractor.name(), "opendata-timeseries/v2");
    }

    #[test]
    fn should_extract_seven_bytes_from_well_formed_key() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let key = make_key(12345, 1, 0x05, b"trailing");

        // when
        let n = extractor.prefix_len(&PrefixTarget::Point(key.clone()));

        // then
        assert_eq!(n, Some(ROUTING_PREFIX_LEN));
        let extracted = &key[..n.unwrap()];
        assert_eq!(extracted[0], TIMESERIES);
        assert_eq!(extracted[1], KEY_VERSION_V2);
        assert_eq!(&extracted[2..6], &12345u32.to_be_bytes());
        assert_eq!(extracted[6], 1);
    }

    #[test]
    fn should_route_records_for_different_buckets_to_different_prefixes() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let key1 = make_key(100, 1, 0x05, b"k");
        let key2 = make_key(200, 1, 0x05, b"k");

        // when
        let n1 = extractor.prefix_len(&point(&key1)).unwrap();
        let n2 = extractor.prefix_len(&point(&key2)).unwrap();

        // then
        assert_eq!(n1, ROUTING_PREFIX_LEN);
        assert_eq!(n2, ROUTING_PREFIX_LEN);
        assert_ne!(&key1[..n1], &key2[..n2]);
    }

    #[test]
    fn should_route_records_for_same_bucket_to_same_prefix() {
        // given — two different record types in the same (bucket_start, bucket_size)
        let extractor = TimeseriesSegmentExtractor;
        let key_dict = make_key(100, 1, 0x02, b"dict-tail");
        let key_samples = make_key(100, 1, 0x05, b"sample-tail");

        // when
        let n_dict = extractor.prefix_len(&point(&key_dict)).unwrap();
        let n_samples = extractor.prefix_len(&point(&key_samples)).unwrap();

        // then
        assert_eq!(&key_dict[..n_dict], &key_samples[..n_samples]);
    }

    #[test]
    fn should_distinguish_buckets_with_same_start_but_different_size() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let key_size1 = make_key(100, 1, 0x05, b"k");
        let key_size2 = make_key(100, 2, 0x05, b"k");

        // when
        let p1 = &key_size1[..extractor.prefix_len(&point(&key_size1)).unwrap()];
        let p2 = &key_size2[..extractor.prefix_len(&point(&key_size2)).unwrap()];

        // then
        assert_ne!(p1, p2);
    }

    #[test]
    fn should_return_none_for_prefix_shorter_than_routing_prefix() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let short = [TIMESERIES, KEY_VERSION_V2, 0, 0, 0, 0];

        // when / then
        assert_eq!(extractor.prefix_len(&prefix(&short)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_wrong_subsystem() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let bad = [0xFF, KEY_VERSION_V2, 0, 0, 0, 0, 1];

        // when / then
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_wrong_version() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let bad = [TIMESERIES, 0xFF, 0, 0, 0, 0, 1];

        // when / then
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    fn should_return_none_for_empty_prefix() {
        // given
        let extractor = TimeseriesSegmentExtractor;

        // when / then
        assert_eq!(extractor.prefix_len(&prefix(&[])), None);
    }

    #[test]
    #[should_panic(expected = "malformed timeseries key")]
    fn should_panic_on_point_shorter_than_routing_prefix() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let short = [TIMESERIES, KEY_VERSION_V2, 0, 0, 0, 0];

        // when / then
        let _ = extractor.prefix_len(&point(&short));
    }

    #[test]
    #[should_panic(expected = "malformed timeseries key")]
    fn should_panic_on_point_with_wrong_subsystem() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let bad = [0xFF, KEY_VERSION_V2, 0, 0, 0, 0, 1];

        // when / then
        let _ = extractor.prefix_len(&point(&bad));
    }

    #[test]
    #[should_panic(expected = "malformed timeseries key")]
    fn should_panic_on_point_with_wrong_version() {
        // given
        let extractor = TimeseriesSegmentExtractor;
        let bad = [TIMESERIES, 0xFF, 0, 0, 0, 0, 1];

        // when / then
        let _ = extractor.prefix_len(&point(&bad));
    }

    #[test]
    #[should_panic(expected = "malformed timeseries key")]
    fn should_panic_on_empty_point() {
        // given
        let extractor = TimeseriesSegmentExtractor;

        // when / then
        let _ = extractor.prefix_len(&point(&[]));
    }

    #[test]
    fn should_satisfy_prefix_extension_invariant_for_well_formed_prefix() {
        // given — a 7-byte scan prefix that targets bucket (start=42, size=1)
        let extractor = TimeseriesSegmentExtractor;
        let mut scan_prefix = vec![TIMESERIES, KEY_VERSION_V2];
        scan_prefix.extend_from_slice(&42u32.to_be_bytes());
        scan_prefix.push(1);
        assert_eq!(scan_prefix.len(), ROUTING_PREFIX_LEN);

        // when
        let n = extractor.prefix_len(&prefix(&scan_prefix)).unwrap();
        assert_eq!(n, ROUTING_PREFIX_LEN);

        // then — any extension of scan_prefix to a full Point must extract
        // the same `n` bytes.
        let extension: Vec<u8> = scan_prefix
            .iter()
            .copied()
            .chain([0x05, 0xAA, 0xBB])
            .collect();
        let point_n = extractor.prefix_len(&point(&extension)).unwrap();
        assert_eq!(point_n, n);
        assert_eq!(&extension[..point_n], &scan_prefix[..n]);
    }

    #[test]
    fn parse_bucket_decodes_well_formed_prefix() {
        // given
        let p = make_key(7777, 1, 0x05, b"");

        // when
        let bucket = parse_bucket(&p[..ROUTING_PREFIX_LEN]);

        // then
        assert_eq!(
            bucket,
            Some(TimeBucket {
                start: 7777,
                size: 1
            })
        );
    }

    #[test]
    fn parse_bucket_rejects_non_one_bucket_size() {
        // given — bucket_size 2 is not yet supported
        let p = make_key(7777, 2, 0x05, b"");

        // when
        let bucket = parse_bucket(&p[..ROUTING_PREFIX_LEN]);

        // then
        assert_eq!(bucket, None);
    }

    #[test]
    fn parse_bucket_rejects_zero_bucket_size() {
        // given
        let p = make_key(7777, 0, 0x05, b"");

        // when
        let bucket = parse_bucket(&p[..ROUTING_PREFIX_LEN]);

        // then
        assert_eq!(bucket, None);
    }

    #[test]
    fn parse_bucket_rejects_short_prefix() {
        // given — 6 bytes, one short of the routing prefix
        let p = [TIMESERIES, KEY_VERSION_V2, 0, 0, 0, 0];

        // when / then
        assert_eq!(parse_bucket(&p), None);
    }

    #[test]
    fn parse_bucket_rejects_wrong_subsystem() {
        // given
        let p = [0xFF, KEY_VERSION_V2, 0, 0, 0, 0, 1];

        // when / then
        assert_eq!(parse_bucket(&p), None);
    }

    #[test]
    fn parse_bucket_rejects_wrong_version() {
        // given
        let p = [TIMESERIES, 0xFF, 0, 0, 0, 0, 1];

        // when / then
        assert_eq!(parse_bucket(&p), None);
    }
}
