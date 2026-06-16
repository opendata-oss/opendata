//! Bloom-filter prefix extractor for the log key space.
//!
//! Implements [`slatedb::PrefixExtractor`] over the v2 log key layout to
//! drive a prefix-aware [`slatedb::filter_policy::BloomFilterPolicy`]. The
//! filter hashes every LogDb logical key (the `(segment, user_key)` pair)
//! without the trailing `var_u64(relative_seq)`, so that SSTs containing no
//! entries for a queried `(segment, user_key)` can be skipped without a
//! block read. Slatedb dedups consecutive same-prefix hashes during SST
//! construction, so a segment containing N entries for the same user key
//! contributes a single stored hash to the filter.
//!
//! See `slatedb::prefix_extractor::PrefixExtractor` for the contract this
//! impl satisfies, and `log/src/segment_extractor.rs` for the *segment*
//! extractor — a separate slatedb axis that partitions the LSM physically
//! and uses a different (shorter) prefix.
//!
//! # Properties this extractor must satisfy
//!
//! 1. **Length cap.** `prefix_len` returning `Some(n)` requires `n <=
//!    input.len()`. Slatedb asserts this at SST-build time; violation
//!    panics inside the SST builder.
//!
//! 2. **Truncation invariant for `Prefix` targets.** If
//!    `prefix_len(Prefix(p)) = Some(n)`, then for every extension `q` of
//!    `p`, `prefix_len(Point(q)) = Some(n)` and `q[..n] == p[..n]`. When
//!    this cannot be guaranteed (e.g., the terminator hasn't been seen
//!    yet, or the record type has no delimiter at all) we return `None`
//!    so slatedb skips the filter for that scan — correct, just no
//!    skip-benefit. Violating this would surface as a filter false
//!    negative for stored keys.
//!
//! 3. **No false negatives on stored keys.** A corollary of (2): every
//!    key fed to `add_key` must, on a `might_match(Point(k))` probe
//!    afterward, report "might match". With `whole_key_filtering=false`
//!    the probe uses the extracted prefix, so this reduces to "the
//!    extractor returns the same `n` for build-time `Point(k)` and
//!    query-time `Point(k)` calls" — i.e., the extractor is
//!    deterministic on identical inputs. Trivially satisfied here.
//!
//! See [`tests::proptests`] for these properties exercised against random
//! `(segment, user_key, seq)` triples.
//!
//! # Naming
//!
//! The extractor's [`name`](slatedb::PrefixExtractor::name) is encoded into
//! every filter block via the `BloomFilterPolicy`'s composite name. Readers
//! match the persisted name against the configured policy before decoding;
//! a mismatch causes the filter sub-block to be ignored and the SST is
//! scanned without filter help (correct, just slower). Bump the name
//! whenever the LogEntry key shape changes in a way that would alter the
//! extracted bytes.

use std::sync::Arc;

use common::serde::terminated_bytes;
use slatedb::{BloomFilterPolicy, FilterPolicy, PrefixExtractor, PrefixTarget};

use crate::serde::{
    KEY_VERSION, RECORD_TYPE_OFFSET, RecordType, SEGMENT_META_KEY_LEN, SEGMENTED_PREFIX_LEN,
    SUBSYSTEM,
};

/// Stable, persisted identifier for the bytes this extractor produces.
/// Distinct from the segment extractor's `"opendata-log/v2"` because they
/// are independent slatedb features (filter policy vs. segment routing).
const EXTRACTOR_NAME: &str = "opendata-log/v2/key-prefix";

/// Extracts the prefix of a log key up to and including the user key.
///
/// Dispatches on the record-type byte at offset 6:
///
/// - `LogEntry (0x10)` — variable region is `[terminated_user_key,
///   var_u64(relative_seq)]`. Returns the position one past the first
///   unescaped `0x00` (the terminator that closes the user key),
///   excluding the sequence varint.
/// - `SeqBlock (0x20)` — fixed 7-byte key, no variable suffix; whole key.
/// - `SegmentMeta (0x30)` — fixed 11-byte key (segmented prefix + 4-byte
///   described-segment-id suffix); whole key.
/// - `ListingEntry (0x40)` — variable suffix without a terminator. Whole
///   key for `Point`; `None` for `Prefix` (truncation-unsafe — any
///   extension is a longer key with a longer extracted length).
///
/// Unknown or malformed inputs panic for `Point` (LogDb bug) and return
/// `None` for `Prefix` (caller-supplied, benign).
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct LogKeyPrefixExtractor;

impl LogKeyPrefixExtractor {
    /// Returns a shared `Arc<dyn PrefixExtractor>` for handing to
    /// `slatedb::filter_policy::BloomFilterPolicy::with_prefix_extractor`.
    pub(crate) fn shared() -> Arc<dyn PrefixExtractor> {
        Arc::new(Self)
    }
}

/// Returns the filter policy set that must be shared by LogDb writers,
/// compactors, and standalone readers.
pub(crate) fn bloom_filter_policies() -> Vec<Arc<dyn FilterPolicy>> {
    vec![Arc::new(
        BloomFilterPolicy::new(10)
            .with_prefix_extractor(LogKeyPrefixExtractor::shared())
            .with_whole_key_filtering(false),
    )]
}

/// Returns true if the buffer is a syntactically well-formed log key
/// prefix up to (and including) the record-type byte: matches `SUBSYSTEM`,
/// `KEY_VERSION`, and has the segment id + tag bytes present.
fn has_segmented_prefix(bytes: &[u8]) -> bool {
    bytes.len() >= SEGMENTED_PREFIX_LEN && bytes[0] == SUBSYSTEM && bytes[1] == KEY_VERSION
}

/// Decodes the record-type byte at offset 6 into a [`RecordType`]. The high
/// nibble holds the type id; the low nibble is reserved (currently 0 for
/// every log record). Returns `None` if the id is unknown.
fn decode_record_type(tag_byte: u8) -> Option<RecordType> {
    RecordType::from_id((tag_byte & 0xF0) >> 4).ok()
}

/// Computes the prefix length for a `Point` target — a stored key or
/// point-lookup target. Panics on malformed inputs: an SST builder reaching
/// here with a non-log key is a LogDb bug, and surfacing it as a panic
/// gives a useful stack trace at the bug's origin.
fn prefix_len_for_point(bytes: &[u8]) -> usize {
    assert!(
        has_segmented_prefix(bytes),
        "LogKeyPrefixExtractor: malformed log key (subsystem/version mismatch \
         or under {} bytes): {:02x?}",
        SEGMENTED_PREFIX_LEN,
        bytes,
    );

    let tag = bytes[RECORD_TYPE_OFFSET];
    let record_type = decode_record_type(tag).unwrap_or_else(|| {
        panic!(
            "LogKeyPrefixExtractor: unknown record-type tag 0x{:02x} in key: {:02x?}",
            tag, bytes,
        )
    });
    match record_type {
        RecordType::LogEntry => terminated_bytes::find_terminator_end(bytes, SEGMENTED_PREFIX_LEN)
            .expect("LogKeyPrefixExtractor: LogEntry key missing terminated_bytes terminator"),
        RecordType::SeqBlock => SEGMENTED_PREFIX_LEN,
        RecordType::SegmentMeta => {
            assert!(
                bytes.len() >= SEGMENT_META_KEY_LEN,
                "LogKeyPrefixExtractor: SegmentMeta key shorter than {} bytes: {:02x?}",
                SEGMENT_META_KEY_LEN,
                bytes,
            );
            SEGMENT_META_KEY_LEN
        }
        RecordType::ListingEntry => bytes.len(),
    }
}

/// Byte length of the `(segment, user_key)` prefix of a **LogEntry** key or
/// scan-prefix: the bytes through the user-key terminator, excluding any
/// trailing `var_u64(relative_seq)`. Returns `None` for malformed input, a
/// record type other than `LogEntry`, or a LogEntry whose terminator is not
/// present in `bytes`.
///
/// Shared with [`crate::filter_sequence`] so the sequence-range filter hashes
/// exactly the prefix bytes this extractor feeds the bloom — keeping the two
/// filters' per-key identities aligned. Unlike [`prefix_len_for_point`], this
/// never panics: it is fed both stored keys and caller-supplied scan prefixes.
pub(crate) fn log_entry_prefix_len(bytes: &[u8]) -> Option<usize> {
    if !has_segmented_prefix(bytes) {
        return None;
    }
    match decode_record_type(bytes[RECORD_TYPE_OFFSET])? {
        RecordType::LogEntry => terminated_bytes::find_terminator_end(bytes, SEGMENTED_PREFIX_LEN),
        _ => None,
    }
}

/// Computes the prefix length for a caller-supplied scan prefix. Returns
/// `None` when this impl cannot guarantee the truncation invariant
/// described in the module doc.
fn prefix_len_for_prefix(bytes: &[u8]) -> Option<usize> {
    if !has_segmented_prefix(bytes) {
        return None;
    }

    let tag = bytes[RECORD_TYPE_OFFSET];
    match decode_record_type(tag)? {
        // Terminator at a fixed position once seen; the encoding can't
        // move it under extension, so returning `Some(pos+1)` is
        // truncation-safe.
        RecordType::LogEntry => terminated_bytes::find_terminator_end(bytes, SEGMENTED_PREFIX_LEN),
        // Fixed-length keys: only safe to answer once `bytes` already
        // contains the full key, otherwise extensions would extract a
        // longer prefix than what we report here.
        RecordType::SeqBlock => Some(SEGMENTED_PREFIX_LEN),
        RecordType::SegmentMeta => {
            (bytes.len() >= SEGMENT_META_KEY_LEN).then_some(SEGMENT_META_KEY_LEN)
        }
        // Variable suffix with no delimiter: every extension would extract
        // a strictly longer prefix, so no `Some(_)` answer is truncation-safe.
        RecordType::ListingEntry => None,
    }
}

impl PrefixExtractor for LogKeyPrefixExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            PrefixTarget::Point(b) => Some(prefix_len_for_point(b.as_ref())),
            PrefixTarget::Prefix(b) => prefix_len_for_prefix(b.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    use crate::serde::{FIRST_USER_SEGMENT_ID, ListingEntryKey, LogEntryKey, SegmentMetaKey};

    fn point(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Point(Bytes::copy_from_slice(bytes))
    }

    fn prefix(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Prefix(Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn should_return_stable_name() {
        let extractor = LogKeyPrefixExtractor;
        assert_eq!(extractor.name(), "opendata-log/v2/key-prefix");
    }

    #[test]
    fn should_extract_through_terminator_for_log_entry_key() {
        // given
        let extractor = LogKeyPrefixExtractor;
        let user_key = Bytes::from("orders");
        let key = LogEntryKey::new(FIRST_USER_SEGMENT_ID, user_key.clone(), 42).serialize(0);

        // when
        let n = extractor
            .prefix_len(&PrefixTarget::Point(key.clone()))
            .unwrap();

        // then — extracted bytes are [segmented_prefix | terminated_user_key],
        // ending at the terminator. The trailing var_u64(42) is excluded.
        let expected_end = SEGMENTED_PREFIX_LEN + user_key.len() + 1; // +1 terminator
        assert_eq!(n, expected_end);
        assert_eq!(key[n - 1], 0x00); // terminated_bytes terminator
    }

    #[test]
    fn should_dedup_extracted_prefix_across_sequences() {
        // given — same (segment, user_key), different sequence numbers
        let extractor = LogKeyPrefixExtractor;
        let key_a = LogEntryKey::new(2, Bytes::from("k"), 5).serialize(0);
        let key_b = LogEntryKey::new(2, Bytes::from("k"), 999).serialize(0);

        // when
        let n_a = extractor
            .prefix_len(&PrefixTarget::Point(key_a.clone()))
            .unwrap();
        let n_b = extractor
            .prefix_len(&PrefixTarget::Point(key_b.clone()))
            .unwrap();

        // then — both extract the same bytes; this is what lets slatedb's
        // consecutive-prefix dedup collapse them to a single filter hash.
        assert_eq!(n_a, n_b);
        assert_eq!(&key_a[..n_a], &key_b[..n_b]);
    }

    #[test]
    fn should_handle_user_key_containing_escaped_zero_byte() {
        // given — terminated_bytes encodes 0x00 inside the user key as
        // 0x01 0x01, so the extractor must NOT stop at that inner byte.
        let extractor = LogKeyPrefixExtractor;
        let user_key = Bytes::from_static(&[b'a', 0x00, b'b']);
        let key = LogEntryKey::new(1, user_key.clone(), 0).serialize(0);

        // when
        let n = extractor
            .prefix_len(&PrefixTarget::Point(key.clone()))
            .unwrap();

        // then — the encoded user key is 'a' 0x01 0x01 'b', then a 0x00
        // terminator, then the relative_seq varint. Extracted length =
        // SEGMENTED_PREFIX_LEN + 4 (encoded user key) + 1 (terminator) = 12.
        assert_eq!(n, SEGMENTED_PREFIX_LEN + 5);
        assert_eq!(key[n - 1], 0x00); // terminated_bytes terminator
    }

    #[test]
    fn should_handle_user_key_containing_escape_byte() {
        // given — 0x01 inside the user key is encoded as 0x01 0x02. The
        // 0x02 must not be confused with a terminator candidate.
        let extractor = LogKeyPrefixExtractor;
        let user_key = Bytes::from_static(&[0x01, b'x']);
        let key = LogEntryKey::new(1, user_key.clone(), 0).serialize(0);

        // when
        let n = extractor
            .prefix_len(&PrefixTarget::Point(key.clone()))
            .unwrap();

        // then — encoded user key is 0x01 0x02 'x', then a 0x00 terminator.
        assert_eq!(n, SEGMENTED_PREFIX_LEN + 4);
        assert_eq!(key[n - 1], 0x00); // terminated_bytes terminator
    }

    #[test]
    fn should_extract_whole_key_for_seq_block() {
        // given
        let extractor = LogKeyPrefixExtractor;
        let key = Bytes::from_static(&crate::serde::SEQ_BLOCK_KEY);
        assert_eq!(key.len(), SEGMENTED_PREFIX_LEN);

        // when
        let n = extractor
            .prefix_len(&PrefixTarget::Point(key.clone()))
            .unwrap();

        // then
        assert_eq!(n, SEGMENTED_PREFIX_LEN);
    }

    #[test]
    fn should_extract_whole_key_for_segment_meta() {
        // given
        let extractor = LogKeyPrefixExtractor;
        let key = SegmentMetaKey::new(42).serialize();
        assert_eq!(key.len(), SEGMENT_META_KEY_LEN);

        // when
        let n = extractor
            .prefix_len(&PrefixTarget::Point(key.clone()))
            .unwrap();

        // then
        assert_eq!(n, SEGMENT_META_KEY_LEN);
    }

    #[test]
    fn should_extract_whole_key_for_listing_entry_point() {
        // given
        let extractor = LogKeyPrefixExtractor;
        let user_key = Bytes::from("some-key");
        let key = ListingEntryKey::new(1, user_key.clone()).serialize();

        // when
        let n = extractor
            .prefix_len(&PrefixTarget::Point(key.clone()))
            .unwrap();

        // then — listing entries have a raw user-key suffix with no
        // terminator; the whole-key hash is what slatedb stores in the
        // filter (since whole_key_filtering is disabled and we substitute
        // the full bytes here).
        assert_eq!(n, key.len());
    }

    #[test]
    fn should_return_none_for_listing_entry_prefix() {
        // given — even a syntactically valid listing prefix returns None,
        // because the truncation invariant cannot hold for a variable
        // suffix with no delimiter: a one-byte extension would extract a
        // longer prefix than the input.
        let extractor = LogKeyPrefixExtractor;
        let key = ListingEntryKey::new(1, Bytes::from("anything")).serialize();

        // when / then
        assert_eq!(extractor.prefix_len(&PrefixTarget::Prefix(key)), None);
    }

    #[test]
    fn should_return_none_for_log_entry_prefix_without_terminator() {
        // given — a scan prefix that stops mid-user-key, before the
        // terminator. The terminator could land anywhere in an extension,
        // so we cannot promise a stable extracted length.
        let extractor = LogKeyPrefixExtractor;
        let mut buf = Vec::new();
        buf.extend_from_slice(&[SUBSYSTEM, KEY_VERSION]);
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(RecordType::LogEntry.tag().as_byte());
        buf.extend_from_slice(b"par"); // start of user key, no terminator yet

        // when / then
        assert_eq!(extractor.prefix_len(&prefix(&buf)), None);
    }

    #[test]
    fn should_return_none_for_log_entry_prefix_ending_mid_escape() {
        // given — prefix ends on the escape byte itself, so we don't know
        // what byte follows (could be 0x01 → encoded 0x00, or 0x02 →
        // encoded 0x01). No safe answer.
        let extractor = LogKeyPrefixExtractor;
        let mut buf = Vec::new();
        buf.extend_from_slice(&[SUBSYSTEM, KEY_VERSION]);
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.push(RecordType::LogEntry.tag().as_byte());
        buf.extend_from_slice(b"a");
        buf.push(0x01); // terminated_bytes escape byte

        // when / then
        assert_eq!(extractor.prefix_len(&prefix(&buf)), None);
    }

    #[test]
    fn should_return_some_for_log_entry_prefix_with_terminator_present() {
        // given — a scan prefix that includes the full encoded user key
        // plus its terminator (no varint suffix). This is what
        // `LogEntryKey::scan_prefix` produces.
        let extractor = LogKeyPrefixExtractor;
        let segment = crate::segment::LogSegment::new(
            FIRST_USER_SEGMENT_ID,
            crate::serde::SegmentMeta::new(0, 0),
        );
        let scan_prefix = LogEntryKey::scan_prefix(&segment, b"orders");

        // when
        let n = extractor
            .prefix_len(&PrefixTarget::Prefix(scan_prefix.clone()))
            .unwrap();

        // then — the entire scan_prefix is the extracted prefix
        assert_eq!(n, scan_prefix.len());
    }

    #[test]
    fn should_satisfy_truncation_invariant_for_log_entry_prefix() {
        // Trait invariant: if Prefix(p) returns Some(n), every extension q
        // of p satisfies Point(q) = Some(n) and q[..n] == p[..n]. Exercise
        // this on a (segment, user_key) prefix.
        let extractor = LogKeyPrefixExtractor;
        let segment = crate::segment::LogSegment::new(3, crate::serde::SegmentMeta::new(0, 0));
        let scan_prefix = LogEntryKey::scan_prefix(&segment, b"weights");

        let n = extractor
            .prefix_len(&PrefixTarget::Prefix(scan_prefix.clone()))
            .unwrap();

        // Any full LogEntry key with this (segment, user_key) is an
        // extension of scan_prefix; the extractor on Point must agree on n.
        for seq in [0u64, 1, 7, 250, 65_000] {
            let full = LogEntryKey::new(3, Bytes::from_static(b"weights"), seq).serialize(0);
            let point_n = extractor
                .prefix_len(&PrefixTarget::Point(full.clone()))
                .unwrap();
            assert_eq!(
                point_n, n,
                "Point extracted {point_n} but Prefix extracted {n}"
            );
            assert_eq!(&full[..point_n], &scan_prefix[..n]);
        }
    }

    #[test]
    fn should_return_none_for_prefix_shorter_than_segmented_prefix() {
        let extractor = LogKeyPrefixExtractor;
        let short = [SUBSYSTEM, KEY_VERSION, 0, 0, 0, 1];
        assert_eq!(extractor.prefix_len(&prefix(&short)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_wrong_subsystem() {
        let extractor = LogKeyPrefixExtractor;
        let bad = [0xFF, KEY_VERSION, 0, 0, 0, 1, 0x10];
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_wrong_version() {
        let extractor = LogKeyPrefixExtractor;
        let bad = [SUBSYSTEM, 0xFF, 0, 0, 0, 1, 0x10];
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_unknown_record_type() {
        let extractor = LogKeyPrefixExtractor;
        let bad = [SUBSYSTEM, KEY_VERSION, 0, 0, 0, 1, 0x99];
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    fn should_return_none_for_segment_meta_prefix_with_partial_suffix() {
        // given — a scan prefix that's missing the trailing
        // described-segment-id bytes is short of the SegmentMeta key
        // length, so the truncation invariant fails for shorter `n`.
        let extractor = LogKeyPrefixExtractor;
        let mut buf = Vec::new();
        buf.extend_from_slice(&[SUBSYSTEM, KEY_VERSION]);
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.push(RecordType::SegmentMeta.tag().as_byte());
        buf.extend_from_slice(&[0, 0]); // only 2 of the 4 suffix bytes

        // when / then
        assert_eq!(extractor.prefix_len(&prefix(&buf)), None);
    }

    #[test]
    #[should_panic(expected = "malformed log key")]
    fn should_panic_on_point_shorter_than_segmented_prefix() {
        let extractor = LogKeyPrefixExtractor;
        let short = [SUBSYSTEM, KEY_VERSION, 0, 0, 0, 1];
        let _ = extractor.prefix_len(&point(&short));
    }

    #[test]
    #[should_panic(expected = "malformed log key")]
    fn should_panic_on_point_with_wrong_subsystem() {
        let extractor = LogKeyPrefixExtractor;
        let bad = [0xFF, KEY_VERSION, 0, 0, 0, 1, 0x10];
        let _ = extractor.prefix_len(&point(&bad));
    }

    #[test]
    #[should_panic(expected = "unknown record-type tag")]
    fn should_panic_on_point_with_unknown_record_type() {
        let extractor = LogKeyPrefixExtractor;
        let bad = [SUBSYSTEM, KEY_VERSION, 0, 0, 0, 1, 0x99];
        let _ = extractor.prefix_len(&point(&bad));
    }

    /// End-to-end integration test that drives a real SlateDB-backed LogDb
    /// across the bloom-filter path. Verifies that writes large enough to
    /// populate filters in compacted SSTs can be read back after close +
    /// reopen with no data loss, and that scans for an absent
    /// `(segment, user_key)` return empty. Together these cover the two
    /// failure modes a misconfigured prefix extractor would produce: false
    /// negatives that drop live data, or filters that never fire.
    ///
    /// Lives in the crate rather than `tests/` so it can sit next to the
    /// extractor it exercises.
    #[cfg(test)]
    mod integration_tests {
        use bytes::Bytes;
        use common::StorageConfig;
        use common::storage::config::{
            LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
        };
        use tempfile::TempDir;

        use crate::config::Config;
        use crate::log::LogDb;
        use crate::model::Record;
        use crate::reader::LogRead;

        fn slatedb_storage_config(temp_dir: &TempDir) -> StorageConfig {
            StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "log-data".to_string(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: temp_dir.path().to_string_lossy().to_string(),
                }),
                settings_path: None,
                block_cache: None,
                meta_cache: None,
            })
        }

        /// Writes enough entries under one `(segment, user_key)` to cross
        /// slatedb's default `min_filter_keys = 1000` threshold so that
        /// SSTs are emitted with filter blocks. Closes the writer, reopens
        /// LogDb against the same path (re-running the full builder chain
        /// including the bloom policy), and verifies every appended entry
        /// is still readable via `scan`. A misconfigured prefix extractor
        /// — e.g., one that hashes different bytes at build time vs query
        /// time — would manifest here as missing entries.
        #[tokio::test]
        async fn filter_does_not_drop_stored_entries_across_reopen() {
            // given
            let temp_dir = TempDir::new().expect("tempdir");
            let storage = slatedb_storage_config(&temp_dir);
            let key = Bytes::from_static(b"orders");
            // Cross min_filter_keys (1000) twice over so compacted SSTs
            // build filters rather than falling back to the no-filter path.
            let total: usize = 2500;

            // when — append + flush + close
            {
                let log = LogDb::open(Config {
                    storage: storage.clone(),
                    ..Default::default()
                })
                .await
                .expect("open LogDb");
                // Batch the appends so the test runs quickly but the writes
                // cross multiple memtable flushes.
                for chunk in (0..total).collect::<Vec<_>>().chunks(100) {
                    let records: Vec<Record> = chunk
                        .iter()
                        .map(|i| Record {
                            key: key.clone(),
                            value: Bytes::from(format!("v-{i}")),
                        })
                        .collect();
                    log.try_append(records).await.expect("append");
                }
                log.flush().await.expect("flush");
                log.close().await.expect("close");
            }

            // then — reopen and scan all entries back
            let log = LogDb::open(Config {
                storage,
                ..Default::default()
            })
            .await
            .expect("reopen LogDb");

            // Stored entries: scan returns every one of them.
            let mut iter = log.scan(key.clone(), ..).await.expect("scan");
            let mut seen: Vec<Bytes> = Vec::with_capacity(total);
            while let Some(entry) = iter.next().await.expect("read") {
                seen.push(entry.value);
            }
            assert_eq!(
                seen.len(),
                total,
                "filter must not drop stored entries; got {} of {}",
                seen.len(),
                total,
            );
            for (i, value) in seen.iter().enumerate() {
                assert_eq!(value, &Bytes::from(format!("v-{i}")));
            }

            // Absent key: scan returns empty. Confirms the filter
            // doesn't accidentally route reads to data under a different
            // user key.
            let mut iter = log
                .scan(Bytes::from_static(b"absent"), ..)
                .await
                .expect("scan");
            assert!(
                iter.next().await.expect("read").is_none(),
                "scan for absent user key must return empty",
            );

            log.close().await.expect("close");
        }
    }

    /// Property tests for the three contract properties the extractor must
    /// satisfy. Each property is documented at the module level; these
    /// tests turn each one into an executable invariant over arbitrary
    /// `(segment_id, user_key, sequence)` triples.
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        /// Arbitrary user keys, including empty, single-byte, and keys
        /// containing the encoding's special bytes (`0x00`, `0x01`) which
        /// stress the escape-aware terminator scan.
        fn arb_user_key() -> impl Strategy<Value = Vec<u8>> {
            prop::collection::vec(any::<u8>(), 0..32)
        }

        proptest! {
            /// Property 1: length cap. `prefix_len(Point(k))` must not
            /// exceed `k.len()`. Slatedb's `BloomFilterBuilder::add_key`
            /// asserts this; violation would panic at SST build time.
            #[test]
            fn point_extracted_length_never_exceeds_input_length(
                segment_id in 1u32..1000,
                user_key in arb_user_key(),
                sequence in 0u64..u64::MAX,
            ) {
                let extractor = LogKeyPrefixExtractor;
                let serialized = LogEntryKey::new(segment_id, Bytes::from(user_key), sequence)
                    .serialize(0);
                let n = extractor
                    .prefix_len(&PrefixTarget::Point(serialized.clone()))
                    .expect("LogEntry Point must always extract");
                prop_assert!(
                    n <= serialized.len(),
                    "extracted length {} exceeds input length {}",
                    n,
                    serialized.len(),
                );
            }

            /// Property 1 over `Prefix` targets: any `Some(n)` returned
            /// must satisfy `n <= input.len()`. Slatedb's
            /// `BloomFilter::might_match` slices `bytes[..n]` after the
            /// extractor returns; an out-of-bounds `n` would panic.
            #[test]
            fn prefix_extracted_length_never_exceeds_input_length(
                bytes in prop::collection::vec(any::<u8>(), 0..64),
            ) {
                let extractor = LogKeyPrefixExtractor;
                let target = PrefixTarget::Prefix(Bytes::from(bytes.clone()));
                if let Some(n) = extractor.prefix_len(&target) {
                    prop_assert!(
                        n <= bytes.len(),
                        "extracted length {} exceeds input length {}",
                        n,
                        bytes.len(),
                    );
                }
            }

            /// Property 2: truncation invariant. For a `LogEntryKey`'s
            /// scan prefix `p` (which always contains the user-key
            /// terminator), every concrete LogEntry key sharing the same
            /// `(segment, user_key)` is an extension of `p`. The
            /// extractor must report the same length `n` for both, and
            /// the extracted bytes must be identical.
            #[test]
            fn log_entry_prefix_truncation_invariant_holds(
                segment_id in 1u32..1000,
                user_key in arb_user_key(),
                sequence in 0u64..u64::MAX,
            ) {
                let extractor = LogKeyPrefixExtractor;
                let segment = crate::segment::LogSegment::new(
                    segment_id,
                    crate::serde::SegmentMeta::new(0, 0),
                );
                let scan_prefix = LogEntryKey::scan_prefix(&segment, &user_key);
                let full_key = LogEntryKey::new(
                    segment_id,
                    Bytes::copy_from_slice(&user_key),
                    sequence,
                )
                .serialize(0);

                let n_prefix = extractor
                    .prefix_len(&PrefixTarget::Prefix(scan_prefix.clone()))
                    .expect("scan_prefix always contains the terminator");
                let n_point = extractor
                    .prefix_len(&PrefixTarget::Point(full_key.clone()))
                    .expect("LogEntry Point must always extract");

                prop_assert_eq!(
                    n_prefix, n_point,
                    "Prefix extracted {} but Point extracted {}",
                    n_prefix, n_point,
                );
                prop_assert_eq!(
                    &scan_prefix[..n_prefix],
                    &full_key[..n_point],
                    "extracted bytes diverge across Prefix and Point",
                );
            }

            /// Property 3: determinism / no false negatives. Two
            /// `Point(k)` calls with the same input must produce the
            /// same answer; in particular, the extracted prefix used at
            /// build time matches the one used at query time. Combined
            /// with property 2, this means every stored key probes its
            /// own filter hash and survives `might_match`.
            #[test]
            fn point_extraction_is_deterministic(
                segment_id in 1u32..1000,
                user_key in arb_user_key(),
                sequence in 0u64..u64::MAX,
            ) {
                let extractor = LogKeyPrefixExtractor;
                let serialized = LogEntryKey::new(
                    segment_id,
                    Bytes::from(user_key),
                    sequence,
                )
                .serialize(0);
                let n1 = extractor.prefix_len(&PrefixTarget::Point(serialized.clone()));
                let n2 = extractor.prefix_len(&PrefixTarget::Point(serialized.clone()));
                prop_assert_eq!(n1, n2);
            }

            /// All entries sharing a `(segment, user_key)` must extract
            /// to identical bytes regardless of sequence — this is what
            /// lets slatedb's consecutive-prefix dedup collapse them to
            /// a single stored hash in the SST filter.
            #[test]
            fn same_segment_and_key_extract_identical_prefix(
                segment_id in 1u32..1000,
                user_key in arb_user_key(),
                seq_a in 0u64..u64::MAX,
                seq_b in 0u64..u64::MAX,
            ) {
                let extractor = LogKeyPrefixExtractor;
                let key_a = LogEntryKey::new(
                    segment_id,
                    Bytes::copy_from_slice(&user_key),
                    seq_a,
                )
                .serialize(0);
                let key_b = LogEntryKey::new(
                    segment_id,
                    Bytes::copy_from_slice(&user_key),
                    seq_b,
                )
                .serialize(0);
                let n_a = extractor
                    .prefix_len(&PrefixTarget::Point(key_a.clone()))
                    .unwrap();
                let n_b = extractor
                    .prefix_len(&PrefixTarget::Point(key_b.clone()))
                    .unwrap();
                prop_assert_eq!(n_a, n_b);
                prop_assert_eq!(&key_a[..n_a], &key_b[..n_b]);
            }
        }
    }
}
