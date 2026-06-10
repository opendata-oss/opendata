//! SlateDB segment extractor for the log subsystem.
//!
//! Implements [`slatedb::PrefixExtractor`] over the log key layout, mapping
//! every record to the 6-byte routing prefix `[subsystem, version, segment_id]`.
//! When configured on the `slatedb::DbBuilder`, the extractor causes SlateDB
//! to route writes for each LogDb segment into its own SlateDB segment, so the
//! per-LogDb-segment LSM state can be compacted (and eventually drained) as a
//! unit.
//!
//! See `log/rfcs/0002-logical-segmentation.md` for the layout this extractor
//! relies on, and `slatedb::prefix_extractor::PrefixExtractor` for the contract
//! the impl satisfies.
//!
//! # Naming
//!
//! The extractor's [`name`](slatedb::PrefixExtractor::name) is persisted in
//! the SlateDB manifest and validated on open. Changes to the routing logic
//! (including any future bump of the log's `KEY_VERSION`) must change the
//! name as well, so a database created under different routing rules is
//! rejected with `SlateDBError::SegmentExtractorMismatch` rather than silently
//! mis-routed.

use std::sync::Arc;

use slatedb::{PrefixExtractor, PrefixTarget};

use crate::serde::{KEY_VERSION, SUBSYSTEM};

/// Routing prefix length: `[subsystem(1), version(1), segment_id(4 BE)]`.
///
/// The `record_type` byte at position 6 deliberately sits *outside* the
/// extracted prefix so all record types for a given segment (entries,
/// listings, and — for the system segment — metadata and the seq block)
/// share one routing prefix and land in the same SlateDB segment.
pub(crate) const ROUTING_PREFIX_LEN: usize = 6;

/// Stable, persisted identifier for this extractor's routing rules.
/// Bump whenever the routing logic changes in a way that would re-route
/// existing keys (e.g. a new `KEY_VERSION` with a different prefix shape).
const EXTRACTOR_NAME: &str = "opendata-log/v1";

/// Prefix extractor routing log records to per-segment SlateDB segments.
///
/// Every well-formed log key has the shape `[SUBSYSTEM, KEY_VERSION,
/// segment_id(4 BE), ...]` by construction, so this extractor returns
/// `Some(6)` for any well-formed `PrefixTarget::Point` and for any 6+ byte
/// `PrefixTarget::Prefix` matching the same leading two bytes.
///
/// **Point inputs that don't match are a bug.** SlateDB rejects writes whose
/// segment extractor returns `None` (or `Some(0)`) with
/// `SlateDBError::EmptySegmentPrefix` at write time, so a malformed key
/// would surface as a confusing downstream error rather than a clear
/// failure at the origin. We panic on mismatched `Point` inputs to make
/// the bug obvious. Short or non-conforming `Prefix` scan inputs are
/// permitted to return `None` — the trait contract allows it and slatedb
/// simply skips prefix-based filtering in that case.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct LogSegmentExtractor;

impl LogSegmentExtractor {
    /// Returns a shared `Arc<dyn PrefixExtractor>` for handing to
    /// `slatedb::DbBuilder::with_segment_extractor`.
    pub(crate) fn shared() -> Arc<dyn PrefixExtractor> {
        Arc::new(Self)
    }
}

impl PrefixExtractor for LogSegmentExtractor {
    fn name(&self) -> &str {
        EXTRACTOR_NAME
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        match target {
            // Stored keys and point-read targets must be well-formed log
            // keys. A mismatch is a LogDb bug; panic with the offending bytes
            // rather than letting slatedb reject the write later with an
            // opaque EmptySegmentPrefix error.
            PrefixTarget::Point(b) => {
                let bytes = b.as_ref();
                assert!(
                    bytes.len() >= ROUTING_PREFIX_LEN
                        && bytes[0] == SUBSYSTEM
                        && bytes[1] == KEY_VERSION,
                    "LogSegmentExtractor: malformed log key (subsystem/version mismatch \
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
                    || bytes[1] != KEY_VERSION
                {
                    None
                } else {
                    Some(ROUTING_PREFIX_LEN)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    use crate::serde::{FIRST_USER_SEGMENT_ID, LogEntryKey, SEQ_BLOCK_KEY, SegmentMetaKey};

    fn point(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Point(Bytes::copy_from_slice(bytes))
    }

    fn prefix(bytes: &[u8]) -> PrefixTarget {
        PrefixTarget::Prefix(Bytes::copy_from_slice(bytes))
    }

    #[test]
    fn should_return_stable_name() {
        // given
        let extractor = LogSegmentExtractor;

        // when / then — must match the persisted manifest value
        assert_eq!(extractor.name(), "opendata-log/v1");
    }

    #[test]
    fn should_extract_six_bytes_from_well_formed_log_entry_key() {
        // given
        let extractor = LogSegmentExtractor;
        let key = LogEntryKey::new(FIRST_USER_SEGMENT_ID, Bytes::from("user-key"), 0).serialize(0);

        // when
        let n = extractor.prefix_len(&PrefixTarget::Point(key.clone()));

        // then
        assert_eq!(n, Some(6));
        // routing prefix is [SUBSYSTEM, KEY_VERSION, segment_id BE]
        let extracted = &key[..n.unwrap()];
        assert_eq!(extracted[0], SUBSYSTEM);
        assert_eq!(extracted[1], KEY_VERSION);
        assert_eq!(
            &extracted[2..6],
            &FIRST_USER_SEGMENT_ID.to_be_bytes(),
            "bytes 2..6 should encode the segment id"
        );
    }

    #[test]
    fn should_route_seq_block_to_system_segment() {
        // given — SeqBlock is a 7-byte key in the system segment (id 0)
        let extractor = LogSegmentExtractor;
        let key = Bytes::from_static(&SEQ_BLOCK_KEY);

        // when
        let n = extractor.prefix_len(&PrefixTarget::Point(key.clone()));

        // then — extracted prefix has segment_id 0 (the system segment)
        assert_eq!(n, Some(6));
        assert_eq!(&key[2..6], &0u32.to_be_bytes());
    }

    #[test]
    fn should_route_segment_meta_to_system_segment() {
        // given — SegmentMeta records live in the system segment regardless of
        // which user segment they describe
        let extractor = LogSegmentExtractor;
        let described_segment = 42u32;
        let key = SegmentMetaKey::new(described_segment).serialize();

        // when
        let n = extractor.prefix_len(&PrefixTarget::Point(key.clone()));

        // then — extracted prefix routes by storage seg_id (0), NOT the
        // described seg_id which appears later in the key
        assert_eq!(n, Some(6));
        assert_eq!(&key[2..6], &0u32.to_be_bytes());
    }

    #[test]
    fn should_route_keys_for_different_segments_to_different_prefixes() {
        // given
        let extractor = LogSegmentExtractor;
        let key1 = LogEntryKey::new(1, Bytes::from("k"), 0).serialize(0);
        let key2 = LogEntryKey::new(2, Bytes::from("k"), 0).serialize(0);

        // when
        let n1 = extractor
            .prefix_len(&PrefixTarget::Point(key1.clone()))
            .unwrap();
        let n2 = extractor
            .prefix_len(&PrefixTarget::Point(key2.clone()))
            .unwrap();

        // then — both extract 6-byte prefixes, but the prefixes differ
        assert_eq!(n1, 6);
        assert_eq!(n2, 6);
        assert_ne!(&key1[..n1], &key2[..n2]);
    }

    #[test]
    fn should_return_none_for_prefix_shorter_than_routing_prefix() {
        // given — a 5-byte scan prefix
        let extractor = LogSegmentExtractor;
        let short = [SUBSYSTEM, KEY_VERSION, 0, 0, 0];

        // when / then — short Prefix returns None (scan-side, benign)
        assert_eq!(extractor.prefix_len(&prefix(&short)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_wrong_subsystem() {
        // given
        let extractor = LogSegmentExtractor;
        let bad = [0xFF, KEY_VERSION, 0, 0, 0, 1, 0x10];

        // when / then — non-log Prefix returns None
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    fn should_return_none_for_prefix_with_wrong_version() {
        // given
        let extractor = LogSegmentExtractor;
        let bad = [SUBSYSTEM, 0xFF, 0, 0, 0, 1, 0x10];

        // when / then — Prefix with wrong version returns None
        assert_eq!(extractor.prefix_len(&prefix(&bad)), None);
    }

    #[test]
    fn should_return_none_for_empty_prefix() {
        // given
        let extractor = LogSegmentExtractor;

        // when / then — empty Prefix returns None (scan-side, benign)
        assert_eq!(extractor.prefix_len(&prefix(&[])), None);
    }

    #[test]
    #[should_panic(expected = "malformed log key")]
    fn should_panic_on_point_shorter_than_routing_prefix() {
        // given
        let extractor = LogSegmentExtractor;
        let short = [SUBSYSTEM, KEY_VERSION, 0, 0, 0];

        // when / then — short Point is a LogDb bug; panic
        let _ = extractor.prefix_len(&point(&short));
    }

    #[test]
    #[should_panic(expected = "malformed log key")]
    fn should_panic_on_point_with_wrong_subsystem() {
        // given
        let extractor = LogSegmentExtractor;
        let bad = [0xFF, KEY_VERSION, 0, 0, 0, 1, 0x10];

        // when / then
        let _ = extractor.prefix_len(&point(&bad));
    }

    #[test]
    #[should_panic(expected = "malformed log key")]
    fn should_panic_on_point_with_wrong_version() {
        // given
        let extractor = LogSegmentExtractor;
        let bad = [SUBSYSTEM, 0xFF, 0, 0, 0, 1, 0x10];

        // when / then
        let _ = extractor.prefix_len(&point(&bad));
    }

    #[test]
    #[should_panic(expected = "malformed log key")]
    fn should_panic_on_empty_point() {
        // given
        let extractor = LogSegmentExtractor;

        // when / then
        let _ = extractor.prefix_len(&point(&[]));
    }

    #[test]
    fn should_satisfy_prefix_extension_invariant_for_well_formed_prefix() {
        // given — a 6-byte scan prefix that targets segment 1
        let extractor = LogSegmentExtractor;
        let mut scan_prefix = vec![SUBSYSTEM, KEY_VERSION];
        scan_prefix.extend_from_slice(&1u32.to_be_bytes());
        assert_eq!(scan_prefix.len(), 6);

        // when
        let n = extractor.prefix_len(&prefix(&scan_prefix)).unwrap();
        assert_eq!(n, 6);

        // then — any extension q of scan_prefix must satisfy Point(q) = Some(n)
        // and q[..n] == scan_prefix[..n].
        let extension: Vec<u8> = scan_prefix
            .iter()
            .copied()
            .chain([0x10, b'k', 0x00, 0x05])
            .collect();
        let point_n = extractor.prefix_len(&point(&extension)).unwrap();
        assert_eq!(point_n, n);
        assert_eq!(&extension[..point_n], &scan_prefix[..n]);
    }
}

/// End-to-end tests that exercise the extractor against a real SlateDB on a
/// temp dir: open LogDb, write across segments, drop the writer, then open a
/// `DbReader` and inspect the manifest. Lives inside the crate (rather than
/// under `tests/`) because the deterministic variant uses
/// [`crate::log::LogDbBuilder::with_clock`], which is `pub(crate)`.
///
/// Manifest inspection uses `DbReader::manifest()` after the writer drops —
/// SlateDB's writer fence prevents peeking at the live manifest, but the
/// reader sees the durable state.
#[cfg(test)]
mod integration_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use common::StorageConfig;
    use common::clock::MockClock;
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };
    use common::storage::factory::create_object_store;
    use slatedb::DbReader;
    use tempfile::TempDir;

    use crate::config::{Config, SegmentConfig};
    use crate::log::{LogDb, LogDbBuilder};
    use crate::model::Record;
    use crate::reader::LogRead;

    const EXPECTED_EXTRACTOR_NAME: &str = "opendata-log/v1";
    const LOG_SUBSYSTEM: u8 = 0x03;
    const LOG_KEY_VERSION: u8 = 0x01;
    const SYSTEM_SEGMENT_ID: u32 = 0;

    fn slatedb_storage_config(temp_dir: &TempDir) -> (StorageConfig, SlateDbStorageConfig) {
        let slate = SlateDbStorageConfig {
            path: "log-data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: temp_dir.path().to_string_lossy().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        };
        (StorageConfig::SlateDb(slate.clone()), slate)
    }

    fn segment_id_from_prefix(prefix: &[u8]) -> u32 {
        assert_eq!(prefix.len(), 6, "routing prefix should be 6 bytes");
        assert_eq!(prefix[0], LOG_SUBSYSTEM);
        assert_eq!(prefix[1], LOG_KEY_VERSION);
        u32::from_be_bytes([prefix[2], prefix[3], prefix[4], prefix[5]])
    }

    /// Reopens the SlateDB path as a `DbReader` and returns the segment ids
    /// (decoded from each segment's routing prefix) present in the manifest.
    async fn manifest_segment_ids(slate: &SlateDbStorageConfig) -> Vec<u32> {
        let object_store = create_object_store(&slate.object_store).expect("object store");
        let reader = DbReader::builder(slate.path.clone(), object_store)
            .build()
            .await
            .expect("open DbReader");
        let manifest = reader.manifest();

        // Sanity-check that the extractor name was persisted.
        assert_eq!(
            manifest.segment_extractor_name(),
            Some(EXPECTED_EXTRACTOR_NAME),
            "segment_extractor_name should round-trip through the manifest"
        );

        let mut ids: Vec<u32> = manifest
            .segments()
            .iter()
            .map(|seg| segment_id_from_prefix(seg.prefix().as_ref()))
            .collect();
        ids.sort_unstable();
        ids
    }

    #[tokio::test]
    async fn opens_and_persists_extractor_name_in_manifest() {
        // given
        let temp_dir = TempDir::new().expect("tempdir");
        let (storage, slate) = slatedb_storage_config(&temp_dir);

        // when — open + close a LogDb so the manifest is written out
        {
            let log = LogDb::open(Config {
                storage: storage.clone(),
                ..Default::default()
            })
            .await
            .expect("open LogDb");
            log.try_append(vec![Record {
                key: Bytes::from("k"),
                value: Bytes::from("v"),
            }])
            .await
            .expect("append");
            log.flush().await.expect("flush");
            log.close().await.expect("close");
        }

        // then — reopen as a DbReader and verify the extractor name is persisted
        let object_store = create_object_store(&slate.object_store).expect("object store");
        let reader = DbReader::builder(slate.path.clone(), object_store)
            .build()
            .await
            .expect("open DbReader");
        let manifest = reader.manifest();
        assert_eq!(
            manifest.segment_extractor_name(),
            Some(EXPECTED_EXTRACTOR_NAME)
        );
    }

    #[tokio::test]
    async fn routes_each_user_segment_to_its_own_slatedb_segment() {
        // given — LogDb driven by a MockClock so segment rolls are deterministic.
        // Each clock advance past the seal interval forces a new user segment on
        // the next append.
        let temp_dir = TempDir::new().expect("tempdir");
        let (storage, slate) = slatedb_storage_config(&temp_dir);
        let clock = Arc::new(MockClock::new());
        let log = LogDbBuilder::new(Config {
            storage: storage.clone(),
            segmentation: SegmentConfig {
                seal_interval: Some(Duration::from_secs(1)),
            },
            ..Default::default()
        })
        .with_clock(clock.clone())
        .build()
        .await
        .expect("open LogDb");

        // when — three appends, with the mock clock advanced past the seal
        // interval between each. No real-time sleeps.
        for i in 0..3 {
            log.try_append(vec![Record {
                key: Bytes::from(format!("k-{i}")),
                value: Bytes::from(format!("v-{i}")),
            }])
            .await
            .expect("append");
            log.flush().await.expect("flush");
            clock.advance(Duration::from_secs(2));
        }

        // confirm via the LogDb API that three user segments exist
        let segments = log.list_segments(..).await.expect("list_segments");
        assert_eq!(segments.len(), 3, "expected three user segments");
        let user_ids: Vec<u32> = segments.iter().map(|s| s.id).collect();
        assert_eq!(user_ids, vec![1, 2, 3]);

        log.close().await.expect("close");

        // then — manifest contains a SlateDB segment per LogDb segment, plus
        // the system segment for the SeqBlock and SegmentMeta records
        let manifest_ids = manifest_segment_ids(&slate).await;
        assert!(
            manifest_ids.contains(&SYSTEM_SEGMENT_ID),
            "system segment (id 0) should hold SeqBlock + SegmentMeta records; \
             manifest segment ids: {:?}",
            manifest_ids
        );
        for user_id in &user_ids {
            assert!(
                manifest_ids.contains(user_id),
                "user segment {} should have its own SlateDB segment; \
                 manifest segment ids: {:?}",
                user_id,
                manifest_ids
            );
        }
    }

    #[tokio::test]
    async fn reopens_existing_database_with_matching_extractor_name() {
        // given — create a database, write, close
        let temp_dir = TempDir::new().expect("tempdir");
        let (storage, _slate) = slatedb_storage_config(&temp_dir);
        {
            let log = LogDb::open(Config {
                storage: storage.clone(),
                ..Default::default()
            })
            .await
            .expect("open LogDb");
            log.try_append(vec![Record {
                key: Bytes::from("k"),
                value: Bytes::from("v"),
            }])
            .await
            .expect("append");
            log.flush().await.expect("flush");
            log.close().await.expect("close");
        }

        // when / then — reopening with the same config (same extractor name)
        // must succeed. A mismatched name would surface as a
        // SegmentExtractorMismatch SlateDB error during build().
        let log = LogDb::open(Config {
            storage,
            ..Default::default()
        })
        .await
        .expect("reopen LogDb with matching extractor name");

        // Reader should see the previously appended record.
        let mut iter = log.scan(Bytes::from("k"), ..).await.expect("scan");
        let entry = iter.next().await.expect("read").expect("entry present");
        assert_eq!(entry.value, Bytes::from("v"));

        log.close().await.expect("close");
    }
}
