//! Buffer-side raw types.
//!
//! [`BufferConsumerRuntime`](crate::BufferConsumerRuntime) materializes
//! per-entry source coordinates here so the rest of the pipeline never has
//! to re-derive ranges from `buffer::ConsumedBatch`.

use bytes::Bytes;

/// One record from the Buffer batch, paired with the metadata envelope
/// that covers its index and the runtime-resolved ingestion time.
#[derive(Debug, Clone)]
pub struct RawEntry {
    /// Index within the batch (the same index the Buffer record block uses).
    pub entry_index: u32,
    /// Opaque entry bytes; the signal decoder is responsible for
    /// interpreting them.
    pub raw_bytes: Bytes,
    /// The metadata payload that applies to this entry. A single Buffer
    /// batch may carry multiple metadata ranges with different envelopes;
    /// here, each entry already gets the envelope that covers its index.
    pub raw_metadata: Bytes,
    /// Wall-clock ingestion time taken from the metadata range that covers
    /// this entry.
    pub ingestion_time_ms: i64,
}

/// A batch of [`RawEntry`]s plus the Buffer source coordinates.
#[derive(Debug, Clone)]
pub struct RawBufferBatch {
    pub sequence: u64,
    pub manifest_path: String,
    pub data_object_path: String,
    pub entries: Vec<RawEntry>,
}

/// Apply the per-range metadata items in a `buffer::ConsumedBatch` to each
/// record index, producing a flat list of [`RawEntry`]s.
///
/// The buffer crate represents metadata as `Vec<Metadata>` where each item
/// has a `start_index`; the entry at index `i` belongs to the metadata
/// range whose `start_index` is the largest value `<= i`. Ranges are
/// emitted in order, so we can scan in lockstep with the entries.
pub fn split_into_raw_entries(
    batch: buffer::ConsumedBatch,
    manifest_path: impl Into<String>,
) -> RawBufferBatch {
    let buffer::ConsumedBatch {
        entries,
        sequence,
        location,
        metadata,
    } = batch;

    let mut raw = Vec::with_capacity(entries.len());
    // Index into `metadata` of the range that currently covers the entry
    // we're about to emit. We bump it forward as `entry_index` crosses the
    // next range's `start_index`.
    let mut range_idx = 0usize;
    for (i, payload) in entries.into_iter().enumerate() {
        let entry_index = i as u32;
        while range_idx + 1 < metadata.len() && metadata[range_idx + 1].start_index <= entry_index {
            range_idx += 1;
        }
        let (raw_metadata, ingestion_time_ms) = match metadata.get(range_idx) {
            Some(m) => (m.payload.clone(), m.ingestion_time_ms),
            None => (Bytes::new(), 0),
        };
        raw.push(RawEntry {
            entry_index,
            raw_bytes: payload,
            raw_metadata,
            ingestion_time_ms,
        });
    }

    RawBufferBatch {
        sequence,
        manifest_path: manifest_path.into(),
        data_object_path: location,
        entries: raw,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn meta(start_index: u32, signal: u8, ts: i64) -> buffer::Metadata {
        buffer::Metadata {
            start_index,
            ingestion_time_ms: ts,
            payload: Bytes::copy_from_slice(&[1, signal, 1, 0]),
        }
    }

    #[test]
    fn single_metadata_range_applies_to_all_entries() {
        let batch = buffer::ConsumedBatch {
            entries: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            sequence: 7,
            location: "loc".into(),
            metadata: vec![meta(0, 1, 1234)],
        };
        let raw = split_into_raw_entries(batch, "manifest");
        assert_eq!(raw.sequence, 7);
        assert_eq!(raw.manifest_path, "manifest");
        assert_eq!(raw.data_object_path, "loc");
        assert_eq!(raw.entries.len(), 2);
        for (i, entry) in raw.entries.iter().enumerate() {
            assert_eq!(entry.entry_index, i as u32);
            assert_eq!(&entry.raw_metadata[..], &[1, 1, 1, 0]);
            assert_eq!(entry.ingestion_time_ms, 1234);
        }
    }

    #[test]
    fn multiple_metadata_ranges_split_at_start_indexes() {
        // Three ranges: [0..2) signal=1, [2..5) signal=2, [5..) signal=1.
        let batch = buffer::ConsumedBatch {
            entries: (0..6)
                .map(|i| Bytes::copy_from_slice(format!("e{i}").as_bytes()))
                .collect(),
            sequence: 9,
            location: "loc".into(),
            metadata: vec![meta(0, 1, 100), meta(2, 2, 200), meta(5, 1, 300)],
        };
        let raw = split_into_raw_entries(batch, "manifest");
        let signals: Vec<u8> = raw.entries.iter().map(|e| e.raw_metadata[1]).collect();
        let times: Vec<i64> = raw.entries.iter().map(|e| e.ingestion_time_ms).collect();
        assert_eq!(signals, vec![1, 1, 2, 2, 2, 1]);
        assert_eq!(times, vec![100, 100, 200, 200, 200, 300]);
    }

    #[test]
    fn empty_metadata_yields_empty_envelopes() {
        let batch = buffer::ConsumedBatch {
            entries: vec![Bytes::from_static(b"x")],
            sequence: 1,
            location: "loc".into(),
            metadata: vec![],
        };
        let raw = split_into_raw_entries(batch, "manifest");
        assert_eq!(raw.entries.len(), 1);
        assert!(raw.entries[0].raw_metadata.is_empty());
        assert_eq!(raw.entries[0].ingestion_time_ms, 0);
    }
}
