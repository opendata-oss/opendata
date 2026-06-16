//! Global-sequence-range filter policy for the log key space.
//!
//! Implements a custom [`slatedb::FilterPolicy`] that prunes SSTs a tail scan
//! cannot need: those whose data *for the scanned key* lies entirely below the
//! scan's resume cursor. It is the sequence half of RFC 0007 Part 2 — the
//! [`crate::filter_prefix`] bloom drops SSTs that cannot contain the key, then
//! this filter drops survivors whose records for the key are all older than the
//! cursor. See `log/rfcs/0007-efficient-tail-scans.md`.
//!
//! # Relative sequences
//!
//! A log key encodes only `relative_seq` (the entry's offset from its segment's
//! `start_seq`); the *global* sequence is `start_seq + relative_seq`. Rather
//! than resolve `segment_id → start_seq` at build time (the RFC's original
//! scheme), this filter stores **relative** sequences — exactly what the key
//! carries — and relativizes the cursor at query time. That works because every
//! scan is segment-scoped: [`crate::storage::LogStorageRead::scan_entries`]
//! issues a `scan_prefix` over one `(segment, user_key)`, and the reader holds
//! that segment's `start_seq`, so it passes `cursor − start_seq` as the filter
//! context. This removes the resolver, a shared registry, and all build-time
//! state — the policy is stateless, like the bloom policy.
//!
//! Soundness of relative storage:
//!
//! - **Per-key map (the precision case): exact.** Entries are keyed by
//!   `hash(segment_id ‖ user_key)` — the segment id is part of the hashed
//!   prefix, so every entry under one hash shares one segment base and its
//!   stored relative range is internally consistent. A query hashes the same
//!   `(segment, user_key)` prefix and relativizes its cursor identically. Hash
//!   collisions union two ranges: coarser selectivity, never a false negative.
//! - **Table min/max on a multi-segment compacted SST: sound, ≈ as coarse as a
//!   global scheme.** Mixed relative maxes still bound any single-segment query
//!   from above (a matching entry for segment S has `relative_seq ≤
//!   table_max`, so it is never skipped); other segments only inflate the
//!   bound, costing selectivity, never soundness. L0 flushes are
//!   single-segment, so their table range is exact.
//!
//! # Compatibility
//!
//! The blob is tagged with the policy [`name`](slatedb::FilterPolicy::name); a
//! reader that cannot match the name skips the filter entirely (no pruning,
//! still sound). The `version` byte covers compatible evolution — an unknown
//! version decodes to a match-everything filter. Bump the name on an
//! incompatible format change.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::serde::varint::var_u64;
use slatedb::{
    Filter, FilterBuilder, FilterContext, FilterPolicy, FilterQuery, PrefixTarget, RowEntry,
};

use crate::filter_prefix::log_entry_prefix_len;
use crate::model::Sequence;

/// Stable, persisted identifier for this filter's blob format. Distinct from
/// the bloom policy name so a reader applies each filter only where it matches.
const POLICY_NAME: &str = "opendata-log/v2/seq-range";

/// Current blob format version (see module doc for the layout). An unknown
/// version decodes to match-everything.
const FORMAT_VERSION: u8 = 1;

/// `flags` bit set when the per-key map is present in the blob.
const FLAG_PER_KEY: u8 = 0b0000_0001;

/// Byte budget for the per-key map. The map is stored **all-or-nothing**: it is
/// written only when its encoded size fits this budget, otherwise it is omitted
/// and the SST relies on the table-level range alone. Bounds the filter's
/// per-SST footprint regardless of key cardinality. Tunable.
///
/// (Future work, RFC 0007: partial coverage — spend the budget on the
/// lowest-`max` keys rather than dropping the map wholesale.)
const MAX_PER_KEY_BYTES: usize = 4096;

/// Stable 64-bit hash of a `(segment, user_key)` prefix, the identity used by
/// the per-key map. FNV-1a followed by the murmur3 `fmix64` finalizer — fully
/// self-contained and deterministic across builds, which the persisted map
/// requires (the writer hashes prefixes into the blob; a later reader re-hashes
/// query prefixes to look them up, so both must agree forever).
///
/// It need not match slatedb's bloom hash — the per-key map is private to this
/// filter. A 64-bit collision is astronomically unlikely within one SST's
/// budget-bounded map, and benign regardless: colliding keys union their ranges
/// (less selective, never a false negative).
fn prefix_hash(prefix: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = FNV_OFFSET;
    for &b in prefix {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    // fmix64 (murmur3) finalizer for clean avalanche across the full 64 bits.
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// Encodes a relative resume cursor into the inline filter context a scan
/// passes to [`Filter::might_match`]: bytes `0..8` hold the cursor as a
/// big-endian `u64`, the remainder reserved (zero).
pub(crate) fn sequence_filter_context(relative_cursor: Sequence) -> FilterContext {
    let mut payload = [0u8; 64];
    payload[..8].copy_from_slice(&relative_cursor.to_be_bytes());
    FilterContext::Inline(payload)
}

/// Reads the relative cursor from a filter context. Returns `None` when there
/// is no context (a scan that does not parametrize the filter) or the variant
/// is not understood — both meaning "cannot prune", so the filter matches.
fn cursor_from_context(context: Option<&FilterContext>) -> Option<u64> {
    match context? {
        FilterContext::Inline(payload) => Some(u64::from_be_bytes(
            payload[..8].try_into().expect("8-byte prefix"),
        )),
        // `FilterContext` is `#[non_exhaustive]`; an unknown variant cannot be
        // decoded, so we decline to prune.
        _ => None,
    }
}

/// Returns the raw bytes of a query target (a stored point key or a scan
/// prefix); both are hashed through [`log_entry_prefix_len`] the same way.
fn target_bytes(target: &PrefixTarget) -> &[u8] {
    match target {
        PrefixTarget::Point(b) => b.as_ref(),
        PrefixTarget::Prefix(b) => b.as_ref(),
    }
}

/// Pulls the relative sequence and `(segment, user_key)` prefix out of a stored
/// LogEntry key. Returns `None` for any non-LogEntry record (those carry no log
/// sequence to prune on) or a key that fails to parse.
fn relative_seq_and_prefix(key: &[u8]) -> Option<(u64, &[u8])> {
    let end = log_entry_prefix_len(key)?;
    let mut rest = &key[end..];
    let relative = var_u64::deserialize(&mut rest).ok()?;
    Some((relative, &key[..end]))
}

/// The full set of SST filter policies for the log key space, shared by writers,
/// compactors, and standalone readers so all three agree on filter
/// encoding/decoding: the prefix bloom (prunes SSTs that cannot contain the key)
/// plus the sequence-range filter (prunes SSTs whose records for the key are all
/// below the scan cursor). The two layer rather than split by level — see RFC
/// 0007 Part 2.
pub(crate) fn filter_policies() -> Vec<Arc<dyn FilterPolicy>> {
    let mut policies = crate::filter_prefix::bloom_filter_policies();
    policies.push(Arc::new(SequenceRangeFilterPolicy));
    policies
}

/// The custom [`FilterPolicy`]. Stateless: the builder needs nothing beyond the
/// keys it is fed.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct SequenceRangeFilterPolicy;

impl FilterPolicy for SequenceRangeFilterPolicy {
    fn name(&self) -> &str {
        POLICY_NAME
    }

    fn builder(&self) -> Box<dyn FilterBuilder> {
        Box::new(SequenceRangeFilterBuilder::default())
    }

    fn decode(&self, data: &[u8]) -> Arc<dyn Filter> {
        Arc::new(SequenceRangeFilter::decode(data))
    }

    fn estimate_size(&self, num_keys: usize) -> usize {
        // version + flags + two table varints (≤10 each), plus a per-key
        // section bounded by the budget. A hint only.
        let per_key = num_keys.saturating_mul(SequenceRangeFilter::MAX_ENTRY_BYTES);
        2 + 20 + per_key.min(MAX_PER_KEY_BYTES + 10)
    }
}

/// Accumulates per-SST relative ranges as the SST is built.
#[derive(Default)]
struct SequenceRangeFilterBuilder {
    /// Table-level (min, max) relative sequence over every LogEntry seen.
    table: Option<(u64, u64)>,
    /// Per-key `hash → (min, max)` relative ranges. `BTreeMap` keeps it sorted
    /// by hash for the binary-searched on-disk layout.
    per_key: BTreeMap<u64, (u64, u64)>,
}

impl FilterBuilder for SequenceRangeFilterBuilder {
    fn add_entry(&mut self, entry: &RowEntry) {
        let Some((relative, prefix)) = relative_seq_and_prefix(&entry.key) else {
            return; // not a LogEntry — nothing to prune on
        };
        self.table = Some(match self.table {
            Some((lo, hi)) => (lo.min(relative), hi.max(relative)),
            None => (relative, relative),
        });
        self.per_key
            .entry(prefix_hash(prefix))
            .and_modify(|(lo, hi)| {
                *lo = (*lo).min(relative);
                *hi = (*hi).max(relative);
            })
            .or_insert((relative, relative));
    }

    fn build(&mut self) -> Arc<dyn Filter> {
        let (table_min, table_max) = self.table.unwrap_or((0, 0));
        let entries: Vec<PerKeyEntry> = self
            .per_key
            .iter()
            .map(|(&hash, &(min, max))| PerKeyEntry { hash, min, max })
            .collect();
        // All-or-nothing: keep the per-key map only if its encoded size fits
        // the budget; otherwise fall back to the table range alone.
        let per_key = (encoded_per_key_size(&entries) <= MAX_PER_KEY_BYTES).then_some(entries);
        Arc::new(SequenceRangeFilter::new(table_min, table_max, per_key))
    }
}

/// One per-key range in the filter blob. All sequences are relative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PerKeyEntry {
    hash: u64,
    min: u64,
    max: u64,
}

/// The decoded, read-side filter. Holds the parsed ranges plus the encoded blob
/// (so `encode`/`size` are trivial and `clamp_allocated_size` can drop slack).
struct SequenceRangeFilter {
    table_min: u64,
    table_max: u64,
    /// Per-key ranges sorted by `hash`, or `None` when the map was omitted
    /// (budget overflow) or the SST holds no LogEntry records.
    per_key: Option<Vec<PerKeyEntry>>,
    encoded: Bytes,
}

impl SequenceRangeFilter {
    /// Worst-case encoded bytes for one per-key entry: 8-byte hash + two
    /// 10-byte varints.
    const MAX_ENTRY_BYTES: usize = 8 + 10 + 10;

    fn new(table_min: u64, table_max: u64, per_key: Option<Vec<PerKeyEntry>>) -> Self {
        let encoded = encode_blob(table_min, table_max, per_key.as_deref());
        Self {
            table_min,
            table_max,
            per_key,
            encoded,
        }
    }

    /// A filter that matches everything — used when the stored blob carries an
    /// unknown version, so an older format never produces a false negative.
    fn match_everything(data: &[u8]) -> Self {
        Self {
            table_min: 0,
            table_max: u64::MAX,
            per_key: None,
            encoded: Bytes::copy_from_slice(data),
        }
    }

    fn decode(data: &[u8]) -> Self {
        match data.first() {
            Some(&version) if version == FORMAT_VERSION => {}
            // Empty, or an unknown version: decline to prune.
            _ => return Self::match_everything(data),
        }
        match decode_fields(data) {
            Some((table_min, table_max, per_key)) => Self {
                table_min,
                table_max,
                per_key,
                encoded: Bytes::copy_from_slice(data),
            },
            // A truncated/corrupt blob under our own version: decline to prune.
            None => Self::match_everything(data),
        }
    }

    /// The relevant `max` relative sequence for a query: the per-key `max` when
    /// the map is present and the key is found, else the table `max`.
    ///
    /// An absent key falls back to the table `max` rather than skipping the
    /// SST: the layered bloom already rules out absent keys, and the table
    /// fallback stays sound for the future partial-coverage map (where absence
    /// would not imply the key is missing).
    fn max_for_query(&self, query: &FilterQuery) -> u64 {
        let Some(per_key) = &self.per_key else {
            return self.table_max;
        };
        let bytes = target_bytes(&query.target);
        let Some(end) = log_entry_prefix_len(bytes) else {
            return self.table_max;
        };
        let hash = prefix_hash(&bytes[..end]);
        match per_key.binary_search_by_key(&hash, |e| e.hash) {
            Ok(idx) => per_key[idx].max,
            Err(_) => self.table_max,
        }
    }
}

impl Filter for SequenceRangeFilter {
    fn might_match(&self, query: &FilterQuery) -> bool {
        // No (or unrecognized) cursor → cannot prune.
        let Some(relative_cursor) = cursor_from_context(query.context.as_ref()) else {
            return true;
        };
        self.max_for_query(query) >= relative_cursor
    }

    fn encode(&self, writer: &mut dyn BufMut) {
        writer.put_slice(&self.encoded);
    }

    fn size(&self) -> usize {
        self.encoded.len()
    }

    fn clamp_allocated_size(&self) -> Arc<dyn Filter> {
        Arc::new(Self {
            table_min: self.table_min,
            table_max: self.table_max,
            per_key: self.per_key.clone(),
            encoded: Bytes::copy_from_slice(&self.encoded),
        })
    }
}

/// Encoded size of the per-key section (count varint + each entry), used to
/// decide all-or-nothing inclusion against the budget.
fn encoded_per_key_size(entries: &[PerKeyEntry]) -> usize {
    let mut buf = BytesMut::new();
    var_u64::serialize(entries.len() as u64, &mut buf);
    let mut size = buf.len();
    for e in entries {
        size += 8; // hash, big-endian u64
        let mut tmp = BytesMut::new();
        var_u64::serialize(e.min, &mut tmp);
        var_u64::serialize(e.max - e.min, &mut tmp);
        size += tmp.len();
    }
    size
}

/// Serializes the blob (see module doc for the layout).
fn encode_blob(table_min: u64, table_max: u64, per_key: Option<&[PerKeyEntry]>) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(FORMAT_VERSION);
    buf.put_u8(if per_key.is_some() { FLAG_PER_KEY } else { 0 });
    var_u64::serialize(table_min, &mut buf);
    var_u64::serialize(table_max - table_min, &mut buf);
    if let Some(entries) = per_key {
        var_u64::serialize(entries.len() as u64, &mut buf);
        for e in entries {
            buf.put_u64(e.hash);
            var_u64::serialize(e.min, &mut buf);
            var_u64::serialize(e.max - e.min, &mut buf);
        }
    }
    buf.freeze()
}

/// Parses the blob fields. Assumes the caller has already checked the version
/// byte. Returns `None` on truncation.
fn decode_fields(data: &[u8]) -> Option<(u64, u64, Option<Vec<PerKeyEntry>>)> {
    let mut buf = data;
    let _version = read_u8(&mut buf)?;
    let flags = read_u8(&mut buf)?;
    let table_min = var_u64::deserialize(&mut buf).ok()?;
    let table_max = table_min.checked_add(var_u64::deserialize(&mut buf).ok()?)?;
    let per_key = if flags & FLAG_PER_KEY != 0 {
        let count = var_u64::deserialize(&mut buf).ok()?;
        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            if buf.len() < 8 {
                return None;
            }
            let hash = buf.get_u64();
            let min = var_u64::deserialize(&mut buf).ok()?;
            let max = min.checked_add(var_u64::deserialize(&mut buf).ok()?)?;
            entries.push(PerKeyEntry { hash, min, max });
        }
        Some(entries)
    } else {
        None
    };
    Some((table_min, table_max, per_key))
}

fn read_u8(buf: &mut &[u8]) -> Option<u8> {
    let (&first, rest) = buf.split_first()?;
    *buf = rest;
    Some(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::ValueDeletable;

    use crate::segment::LogSegment;
    use crate::serde::{LogEntryKey, SegmentMeta};

    /// Builds a LogEntry `RowEntry` whose stored `relative_seq` is `relative`
    /// (serialize with `start_seq = 0`, so `sequence == relative`).
    fn log_entry(segment_id: u32, key: &str, relative: u64) -> RowEntry {
        let k = LogEntryKey::new(segment_id, Bytes::from(key.to_string()), relative).serialize(0);
        RowEntry {
            key: k,
            value: ValueDeletable::Value(Bytes::from_static(b"v")),
            seq: 0,
            create_ts: None,
            expire_ts: None,
        }
    }

    /// A prefix-scan query over `(segment, key)` carrying a relative cursor —
    /// the exact shape the read path produces.
    fn query(segment_id: u32, key: &str, relative_cursor: u64) -> FilterQuery {
        let segment = LogSegment::new(segment_id, SegmentMeta::new(0, 0));
        let prefix = LogEntryKey::scan_prefix(&segment, key.as_bytes());
        FilterQuery::prefix(prefix).with_context(Some(sequence_filter_context(relative_cursor)))
    }

    fn build(entries: &[RowEntry]) -> Arc<dyn Filter> {
        let policy = SequenceRangeFilterPolicy;
        let mut builder = policy.builder();
        for e in entries {
            builder.add_entry(e);
        }
        builder.build()
    }

    /// Round-trips a filter through encode/decode so tests exercise the
    /// on-disk path the reader takes.
    fn round_trip(filter: &Arc<dyn Filter>) -> Arc<dyn Filter> {
        let mut buf = Vec::new();
        filter.encode(&mut buf);
        SequenceRangeFilterPolicy.decode(&buf)
    }

    #[test]
    fn should_expose_stable_name() {
        assert_eq!(
            SequenceRangeFilterPolicy.name(),
            "opendata-log/v2/seq-range"
        );
    }

    #[test]
    fn should_keep_sst_when_cursor_at_or_below_key_max() {
        let filter = round_trip(&build(&[
            log_entry(1, "orders", 10),
            log_entry(1, "orders", 20),
        ]));
        // cursor 20 == max → keep; cursor 21 > max → skip.
        assert!(filter.might_match(&query(1, "orders", 20)));
        assert!(!filter.might_match(&query(1, "orders", 21)));
    }

    #[test]
    fn should_prune_per_key_independently() {
        // "orders" tops out at relative 20; "shipments" at relative 200.
        let filter = round_trip(&build(&[
            log_entry(1, "orders", 10),
            log_entry(1, "orders", 20),
            log_entry(1, "shipments", 100),
            log_entry(1, "shipments", 200),
        ]));
        // A cursor past orders' max skips for orders...
        assert!(!filter.might_match(&query(1, "orders", 50)));
        // ...but the same cursor still matches shipments (its own max is 200).
        assert!(filter.might_match(&query(1, "shipments", 50)));
    }

    #[test]
    fn should_match_when_no_context() {
        let filter = round_trip(&build(&[log_entry(1, "orders", 10)]));
        let segment = LogSegment::new(1, SegmentMeta::new(0, 0));
        let q = FilterQuery::prefix(LogEntryKey::scan_prefix(&segment, b"orders"));
        assert!(filter.might_match(&q), "no cursor → cannot prune");
    }

    #[test]
    fn should_match_everything_on_unknown_version() {
        let filter = build(&[log_entry(1, "orders", 10)]);
        let mut buf = Vec::new();
        filter.encode(&mut buf);
        buf[0] = 0xFF; // corrupt the version byte
        let decoded = SequenceRangeFilterPolicy.decode(&buf);
        assert!(decoded.might_match(&query(1, "orders", u64::MAX)));
    }

    #[test]
    fn should_ignore_non_log_entry_records() {
        // A SeqBlock key has no relative sequence; feeding it must not panic
        // and contributes nothing, leaving an empty (match-nothing-above-0)
        // table.
        let seq_block = RowEntry {
            key: Bytes::from_static(&crate::serde::SEQ_BLOCK_KEY),
            value: ValueDeletable::Value(Bytes::from_static(b"v")),
            seq: 0,
            create_ts: None,
            expire_ts: None,
        };
        let filter = round_trip(&build(&[seq_block]));
        // Empty table is (0, 0): only a cursor of 0 matches.
        assert!(filter.might_match(&query(1, "orders", 0)));
        assert!(!filter.might_match(&query(1, "orders", 1)));
    }

    #[test]
    fn should_relativize_per_segment_bases() {
        // Same stored relative (10) in two segments. A query for segment 2 with
        // a relative cursor of 10 must consult segment 2's own per-key entry.
        let filter = round_trip(&build(&[log_entry(1, "k", 10), log_entry(2, "k", 10)]));
        assert!(filter.might_match(&query(1, "k", 10)));
        assert!(filter.might_match(&query(2, "k", 10)));
        assert!(!filter.might_match(&query(2, "k", 11)));
    }

    #[test]
    fn should_drop_per_key_map_over_budget_but_stay_sound() {
        // Enough distinct keys that the per-key map exceeds MAX_PER_KEY_BYTES,
        // forcing the table-only fallback. Soundness must hold regardless.
        let mut entries = Vec::new();
        let key_count = (MAX_PER_KEY_BYTES / SequenceRangeFilter::MAX_ENTRY_BYTES) + 200;
        for i in 0..key_count {
            entries.push(log_entry(1, &format!("key-{i}"), i as u64));
        }
        let filter = round_trip(&build(&entries));
        // The map was dropped (no per-key precision), so every stored key must
        // still survive a cursor at its own relative — table_max covers it.
        for i in 0..key_count {
            assert!(
                filter.might_match(&query(1, &format!("key-{i}"), i as u64)),
                "false negative for key-{i} after budget fallback",
            );
        }
    }

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// No false negatives: for any multi-segment set of entries, every
            /// stored `(segment, key)` with `relative >= cursor` survives
            /// `might_match`. Exercises both the per-key map and the
            /// table-level cross-segment bound.
            #[test]
            fn no_false_negatives(
                entries in prop::collection::vec(
                    (1u32..4, "[a-c]{1,3}", 0u64..1000),
                    1..64,
                ),
            ) {
                let rows: Vec<RowEntry> = entries
                    .iter()
                    .map(|(seg, key, rel)| log_entry(*seg, key, *rel))
                    .collect();
                let filter = round_trip(&build(&rows));

                for (seg, key, rel) in &entries {
                    // A cursor at the stored relative must keep the SST (the
                    // record itself qualifies).
                    prop_assert!(
                        filter.might_match(&query(*seg, key, *rel)),
                        "false negative for (seg={seg}, key={key}, rel={rel})",
                    );
                }
            }
        }
    }

    /// End-to-end test that drives a real SlateDB-backed LogDb with the
    /// sequence-range policy registered. Because the read path does not yet
    /// pass a cursor (next commit), the filter is built and persisted but never
    /// prunes — so this verifies the builder/decoder round-trip through real
    /// flush + compaction without dropping data, across **multiple segments**
    /// (so compacted SSTs carry per-key entries from several segment bases — the
    /// case relative storage must get right). A builder that mis-parsed keys, or
    /// a decoder that corrupted ranges, would surface here as missing entries.
    ///
    /// Lives in the crate rather than `tests/` so it sits next to the policy.
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

        /// Appends `count` records for `key`, labelled `v-{offset}..` so the
        /// reader can assert exact ordering, in flush-sized chunks.
        async fn append_run(log: &LogDb, key: &Bytes, offset: usize, count: usize) {
            for chunk in (offset..offset + count).collect::<Vec<_>>().chunks(100) {
                let records: Vec<Record> = chunk
                    .iter()
                    .map(|i| Record {
                        key: key.clone(),
                        value: Bytes::from(format!("v-{i}")),
                    })
                    .collect();
                log.try_append(records).await.expect("append");
            }
        }

        #[tokio::test]
        async fn filter_does_not_drop_entries_across_segments_and_reopen() {
            // given
            let temp_dir = TempDir::new().expect("tempdir");
            let storage = slatedb_storage_config(&temp_dir);
            let keys = [
                Bytes::from_static(b"orders"),
                Bytes::from_static(b"shipments"),
            ];
            // Per (key, segment) run size, and the number of segments. Total
            // entries (2 keys * 600 * 3 = 3600) crosses slatedb's default
            // min_filter_keys (1000) so compacted SSTs build filter blocks.
            let run = 600usize;
            let segments = 3usize;

            // when — write `run` entries per key into each of `segments`
            // segments, sealing between them so each key's data spans multiple
            // segment bases in the compacted tree.
            {
                let log = LogDb::open(Config {
                    storage: storage.clone(),
                    ..Default::default()
                })
                .await
                .expect("open LogDb");
                for seg in 0..segments {
                    if seg > 0 {
                        log.seal_segment().await.expect("seal");
                    }
                    for key in &keys {
                        append_run(&log, key, seg * run, run).await;
                    }
                }
                log.flush().await.expect("flush");
                log.close().await.expect("close");
            }

            // then — reopen and scan each key back in full, in order.
            let log = LogDb::open(Config {
                storage,
                ..Default::default()
            })
            .await
            .expect("reopen LogDb");

            let total = run * segments;
            for key in &keys {
                let mut iter = log.scan(key.clone(), ..).await.expect("scan");
                let mut seen: Vec<Bytes> = Vec::with_capacity(total);
                while let Some(entry) = iter.next().await.expect("read") {
                    seen.push(entry.value);
                }
                assert_eq!(
                    seen.len(),
                    total,
                    "filter must not drop stored entries for {key:?}; got {} of {total}",
                    seen.len(),
                );
                for (i, value) in seen.iter().enumerate() {
                    assert_eq!(value, &Bytes::from(format!("v-{i}")));
                }
            }

            // Absent key: scan returns empty (the registered filters must not
            // route reads to data under a different key).
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

        /// End-to-end check that the sequence-range filter, now consulted with a
        /// relative cursor on the scan path, prunes *soundly*: a `scan(key, N..)`
        /// must return exactly the entries with `sequence >= N`, across reopen,
        /// compaction, and multiple segments. A false-negative bug — a wrong
        /// relativization, or pruning an SST that actually holds matching
        /// records — would drop entries near the cursor and fail here.
        /// (The magnitude of pruning is covered by the `might_match` unit tests;
        /// this guards the safety property that activating it drops nothing.)
        #[tokio::test]
        async fn cursor_scan_returns_exactly_entries_at_or_above_cursor() {
            // given — one key spanning 3 sealed segments, enough to flush and
            // compact (a single key keeps global sequences contiguous, so the
            // ground truth is easy to reason about).
            let temp_dir = TempDir::new().expect("tempdir");
            let storage = slatedb_storage_config(&temp_dir);
            let key = Bytes::from_static(b"orders");
            let run = 800usize;
            let segments = 3usize;
            {
                let log = LogDb::open(Config {
                    storage: storage.clone(),
                    ..Default::default()
                })
                .await
                .expect("open LogDb");
                for seg in 0..segments {
                    if seg > 0 {
                        log.seal_segment().await.expect("seal");
                    }
                    append_run(&log, &key, seg * run, run).await;
                }
                log.flush().await.expect("flush");
                log.close().await.expect("close");
            }

            let log = LogDb::open(Config {
                storage,
                ..Default::default()
            })
            .await
            .expect("reopen LogDb");

            // Ground truth: the full (sequence, value) stream in order.
            let mut truth: Vec<(u64, Bytes)> = Vec::new();
            let mut iter = log.scan(key.clone(), ..).await.expect("scan");
            while let Some(entry) = iter.next().await.expect("read") {
                truth.push((entry.sequence, entry.value));
            }
            assert_eq!(truth.len(), run * segments, "full scan ground truth");

            // For cursors at the start, both segment-boundary regions, an exact
            // entry sequence, the last entry, and just past the end, the
            // cursor-bounded scan must equal the ground truth filtered to
            // `sequence >= cursor`.
            let last_seq = truth.last().unwrap().0;
            let cursors = [
                truth[0].0,
                truth[run - 1].0, // end of segment 0
                truth[run].0,     // start of segment 1
                truth[run * 2].0, // start of segment 2
                truth[truth.len() / 2].0,
                last_seq,
                last_seq + 1, // past the end → empty
            ];
            for cursor in cursors {
                let expected: Vec<(u64, Bytes)> = truth
                    .iter()
                    .filter(|(seq, _)| *seq >= cursor)
                    .cloned()
                    .collect();
                let mut got: Vec<(u64, Bytes)> = Vec::with_capacity(expected.len());
                let mut iter = log.scan(key.clone(), cursor..).await.expect("scan");
                while let Some(entry) = iter.next().await.expect("read") {
                    got.push((entry.sequence, entry.value));
                }
                assert_eq!(
                    got, expected,
                    "scan(key, {cursor}..) must return exactly sequences >= {cursor}",
                );
            }

            log.close().await.expect("close");
        }
    }
}
