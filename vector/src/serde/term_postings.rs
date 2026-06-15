//! `TermPostings` value encoding/decoding for FTS (RFC-0006).
//!
//! A term's posting list is a sequence of typed blocks written one after
//! another to the end of the value buffer. Each `Postings` block holds up to
//! [`TARGET_POSTINGS_PER_BLOCK`] `(id, freq, norm)` triples in strictly
//! descending `id` order. Each `Postings` block is preceded by a `Skip`
//! block that summarises the postings block's last id and serialized byte
//! length, so a future query path can iterate without parsing the full
//! payload. Multiple `(Skip, Postings)` pairs may appear in a single value;
//! they too are stored in descending id order (each pair's ids are strictly
//! lower than all ids in the preceding pair).
//!
//! Milestone 0 does not encode skip-block `impacts` (used for BlockMaxScore);
//! the field is reserved per the RFC shape but always empty.
//!
//! ## Value Layout (little-endian unless noted)
//!
//! ```text
//! TermPostingsValue
//! ┌──────────────────────────────────────────────────────────────────┐
//! │  blocks: { type: u8, len: u32, payload[len bytes] } until EOF    │
//! └──────────────────────────────────────────────────────────────────┘
//!
//! Postings (type = 0x00) payload
//! ┌──────────────────────────────────────────────────────────────────┐
//! │  count:        u32   (1..=256 entries actually used)             │
//! │  base_id:      u64   (highest id in block)                       │
//! │  ids_encoding: u8   (0 = bitpacked gaps, 1 = u64 varint gaps)    │
//! │  -- if ids_encoding == 0:                                        │
//! │     ids_bits:    u8                                              │
//! │     ids_packed:  ids_bits * 32 bytes (BitPacker8x of 256 gaps)   │
//! │  -- if ids_encoding == 1:                                        │
//! │     ids_varint:  count u64 LEB128 unsigned varints               │
//! │  freqs_bits:   u8                                                │
//! │  freqs_packed: freqs_bits * 32 bytes (BitPacker8x of 256 freqs)  │
//! │  norms:        count bytes                                       │
//! └──────────────────────────────────────────────────────────────────┘
//!
//! `ids_*` carries gaps between consecutive ids in descending order:
//! `gap[0] = 0` (the delta from the first id to itself) and
//! `gap[i] = ids[i-1] - ids[i]` for `i > 0`. The encoder builds the
//! gap array first, then picks bitpacked when every gap fits in `u32` and
//! falls back to per-gap LEB128 unsigned varints (`ids_encoding = 1`)
//! otherwise.
//!
//! Skip (type = 0x01) payload
//! ┌──────────────────────────────────────────────────────────────────┐
//! │  last_id:        u64                                              │
//! │  impacts_count:  u16   (always 0 in M0)                           │
//! │  length:         u64   (byte length of associated Postings block) │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Merge Semantics
//!
//! Term postings values are written as merge operands. Each fresh batch
//! produces a single `(Skip, Postings)` pair containing only the newly
//! inserted ids. Vector ids are monotonically allocated, so a newer batch's
//! ids are strictly higher than any previous batch's ids and the merge
//! operator can simply concatenate newer operands' bytes before the existing
//! value's bytes — preserving the global descending-id ordering.
//!
//! TODO(rfc-0006 milestone 1+): once the FTS compaction filter lands, it
//! will rewrite postings into uniform 256-entry `Postings` blocks (each
//! preceded by a refreshed `Skip`) and prune any stale ids removed via the
//! deletions bitmap. The variable-size last block today is a Milestone 0
//! artifact of merge-by-concat with batches of arbitrary size.

use super::EncodingError;
use crate::serde::vector_id::VectorId;
use bitpacking::{BitPacker, BitPacker8x};
use bytes::{BufMut, Bytes, BytesMut};

const BLOCK_LEN: usize = BitPacker8x::BLOCK_LEN;

/// Target number of postings per block. Aligned with [`BitPacker8x::BLOCK_LEN`]
/// so a full block bitpacks in one SIMD call.
pub(crate) const TARGET_POSTINGS_PER_BLOCK: usize = BLOCK_LEN;

const BLOCK_TYPE_POSTINGS: u8 = 0x00;
const BLOCK_TYPE_SKIP: u8 = 0x01;

/// id `gaps` packed via [`BitPacker8x`]. Used when every gap fits in `u32`.
const IDS_ENCODING_BITPACKED: u8 = 0;

/// id `gaps` written one-by-one as unsigned LEB128 `u64` varints. Used
/// when at least one gap exceeds [`u32::MAX`].
const IDS_ENCODING_VARINT: u8 = 1;

/// A single posting entry within a `Postings` block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PostingEntry {
    pub(crate) id: VectorId,
    pub(crate) freq: u32,
    pub(crate) norm: u8,
}

/// A `Postings` block: a run of `PostingEntry` in descending `id` order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PostingsBlock {
    /// Entries, sorted by descending `id`.
    pub(crate) entries: Vec<PostingEntry>,
}

impl PostingsBlock {
    pub(crate) fn from_descending(entries: Vec<PostingEntry>) -> Self {
        debug_assert!(
            entries.windows(2).all(|w| w[0].id > w[1].id),
            "PostingsBlock entries must be strictly descending"
        );
        debug_assert!(entries.len() <= BLOCK_LEN);
        Self { entries }
    }

    pub(crate) fn last_id(&self) -> VectorId {
        self.entries
            .last()
            .map(|e| e.id)
            .unwrap_or(VectorId::data_vector_id(0))
    }

    /// Encode just the payload (no type tag or length prefix).
    fn encode_payload(&self, buf: &mut BytesMut) {
        let count = self.entries.len();
        assert!(count <= BLOCK_LEN);
        buf.put_u32_le(count as u32);
        if count == 0 {
            return;
        }

        let base_id = self.entries[0].id.id();
        buf.put_u64_le(base_id);

        // Build the gap array first so we can choose the id encoding
        // based on the max gap. `gap[0]` is always 0 (the first id is
        // its own delta); subsequent gaps are positive because the block is
        // strictly descending.
        let mut gaps = Vec::with_capacity(count);
        gaps.push(0u64);
        let mut max_gap = 0u64;
        for window in self.entries.windows(2) {
            let g = window[0].id.id() - window[1].id.id();
            max_gap = max_gap.max(g);
            gaps.push(g);
        }

        let bitpacker = BitPacker8x::new();

        if max_gap <= u32::MAX as u64 {
            buf.put_u8(IDS_ENCODING_BITPACKED);
            // Pad the tail with zeros — zero is `<= max_gap`, so this never
            // widens num_bits beyond what the real data requires.
            let mut ids = [0u32; BLOCK_LEN];
            for (i, &g) in gaps.iter().enumerate() {
                ids[i] = g as u32;
            }
            let ids_bits = bitpacker.num_bits(&ids);
            buf.put_u8(ids_bits);
            let ids_bytes = ids_bits as usize * (BLOCK_LEN / 8);
            let mut packed = vec![0u8; ids_bytes];
            let written = bitpacker.compress(&ids, &mut packed, ids_bits);
            debug_assert_eq!(written, ids_bytes);
            buf.extend_from_slice(&packed);
        } else {
            buf.put_u8(IDS_ENCODING_VARINT);
            for &g in &gaps {
                write_u64_varint(buf, g);
            }
        }

        // Freqs: bitpacked u32; pad tail with zeros so num_bits stays tight.
        let mut freqs = [0u32; BLOCK_LEN];
        for (i, entry) in self.entries.iter().enumerate() {
            freqs[i] = entry.freq;
        }
        let freqs_bits = bitpacker.num_bits(&freqs);
        buf.put_u8(freqs_bits);
        let freqs_bytes = freqs_bits as usize * (BLOCK_LEN / 8);
        let mut packed_freqs = vec![0u8; freqs_bytes];
        let written = bitpacker.compress(&freqs, &mut packed_freqs, freqs_bits);
        debug_assert_eq!(written, freqs_bytes);
        buf.extend_from_slice(&packed_freqs);

        // Norms: raw u8 per entry, not padded since `count` is recoverable
        // from the header.
        for entry in &self.entries {
            buf.put_u8(entry.norm);
        }
    }

    fn decode_payload(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut cursor = buf;
        if cursor.len() < 4 {
            return Err(EncodingError {
                message: "postings block payload too short for count".to_string(),
            });
        }
        let count = u32::from_le_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]) as usize;
        cursor = &cursor[4..];
        if count == 0 {
            return Ok(Self::default());
        }
        if count > BLOCK_LEN {
            return Err(EncodingError {
                message: format!(
                    "postings block count {} exceeds BLOCK_LEN {}",
                    count, BLOCK_LEN
                ),
            });
        }

        if cursor.len() < 8 {
            return Err(EncodingError {
                message: "postings block payload too short for base id".to_string(),
            });
        }
        let base_id = u64::from_le_bytes([
            cursor[0], cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6], cursor[7],
        ]);
        cursor = &cursor[8..];

        let bitpacker = BitPacker8x::new();

        // ids_encoding byte
        if cursor.is_empty() {
            return Err(EncodingError {
                message: "postings block payload too short for ids_encoding".to_string(),
            });
        }
        let ids_encoding = cursor[0];
        cursor = &cursor[1..];

        let gaps: Vec<u64> = match ids_encoding {
            IDS_ENCODING_BITPACKED => {
                if cursor.is_empty() {
                    return Err(EncodingError {
                        message: "postings block payload too short for ids_bits".to_string(),
                    });
                }
                let ids_bits = cursor[0];
                cursor = &cursor[1..];
                let ids_bytes = ids_bits as usize * (BLOCK_LEN / 8);
                if cursor.len() < ids_bytes {
                    return Err(EncodingError {
                        message: format!(
                            "postings block truncated within ids section: need {}, have {}",
                            ids_bytes,
                            cursor.len()
                        ),
                    });
                }
                let mut ids = [0u32; BLOCK_LEN];
                let consumed = bitpacker.decompress(&cursor[..ids_bytes], &mut ids, ids_bits);
                debug_assert_eq!(consumed, ids_bytes);
                cursor = &cursor[ids_bytes..];
                ids[..count].iter().map(|&g| g as u64).collect()
            }
            IDS_ENCODING_VARINT => {
                let mut gaps = Vec::with_capacity(count);
                for _ in 0..count {
                    let (g, used) = read_u64_varint(cursor)?;
                    cursor = &cursor[used..];
                    gaps.push(g);
                }
                gaps
            }
            other => {
                return Err(EncodingError {
                    message: format!("unknown postings ids_encoding: 0x{:02x}", other),
                });
            }
        };

        // Reconstruct ids from gaps. `gap[0]` is 0 so ids[0] == base_id.
        let mut ids = Vec::with_capacity(count);
        let mut current = base_id;
        for (i, &g) in gaps.iter().enumerate() {
            if i == 0 {
                ids.push(current);
                continue;
            }
            current = current.checked_sub(g).ok_or_else(|| EncodingError {
                message: format!("gap {} underflows running id {}", g, current),
            })?;
            ids.push(current);
        }

        // freqs
        if cursor.is_empty() {
            return Err(EncodingError {
                message: "postings block payload too short for freqs_bits".to_string(),
            });
        }
        let freqs_bits = cursor[0];
        cursor = &cursor[1..];
        let freqs_bytes = freqs_bits as usize * (BLOCK_LEN / 8);
        if cursor.len() < freqs_bytes {
            return Err(EncodingError {
                message: format!(
                    "postings block truncated within freqs section: need {}, have {}",
                    freqs_bytes,
                    cursor.len()
                ),
            });
        }
        let mut freqs = [0u32; BLOCK_LEN];
        let consumed = bitpacker.decompress(&cursor[..freqs_bytes], &mut freqs, freqs_bits);
        debug_assert_eq!(consumed, freqs_bytes);
        cursor = &cursor[freqs_bytes..];

        // norms
        if cursor.len() < count {
            return Err(EncodingError {
                message: format!(
                    "postings block too short for norms: need {}, have {}",
                    count,
                    cursor.len()
                ),
            });
        }
        let norms = &cursor[..count];

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            entries.push(PostingEntry {
                id: VectorId::from_raw(ids[i]),
                freq: freqs[i],
                norm: norms[i],
            });
        }
        Ok(Self { entries })
    }
}

fn write_u64_varint(buf: &mut BytesMut, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
            buf.put_u8(byte);
        } else {
            buf.put_u8(byte);
            return;
        }
    }
}

fn read_u64_varint(buf: &[u8]) -> Result<(u64, usize), EncodingError> {
    let mut shift: u32 = 0;
    let mut result: u64 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        if shift >= 64 {
            return Err(EncodingError {
                message: "u64 varint overflow".to_string(),
            });
        }
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(EncodingError {
        message: "u64 varint truncated".to_string(),
    })
}

/// A `Skip` block summarising the immediately following `Postings` block.
///
/// For Milestone 0 `impacts` is reserved per the RFC shape but always empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SkipBlock {
    pub(crate) last_id: VectorId,
    pub(crate) length: u64,
}

impl SkipBlock {
    fn encode_payload(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.last_id.id());
        buf.put_u16_le(0); // impacts_count, always 0 in M0
        buf.put_u64_le(self.length);
    }

    fn decode_payload(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 8 + 2 + 8 {
            return Err(EncodingError {
                message: format!(
                    "skip block payload too short: need {} bytes, have {}",
                    8 + 2 + 8,
                    buf.len()
                ),
            });
        }
        let last_id = VectorId::from_raw(u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]));
        let impacts_count = u16::from_le_bytes([buf[8], buf[9]]);
        // Milestone 0 always writes 0 impacts; tolerate but ignore higher counts.
        let mut cursor = &buf[10..];
        for _ in 0..impacts_count {
            if cursor.len() < 5 {
                return Err(EncodingError {
                    message: "skip block payload truncated within impacts".to_string(),
                });
            }
            cursor = &cursor[5..];
        }
        if cursor.len() < 8 {
            return Err(EncodingError {
                message: "skip block payload too short for length".to_string(),
            });
        }
        let length = u64::from_le_bytes([
            cursor[0], cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6], cursor[7],
        ]);
        Ok(Self { last_id, length })
    }
}

/// Decoded block within a `TermPostingsValue`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TermPostingsBlock {
    Skip(SkipBlock),
    Postings(PostingsBlock),
}

/// Decoded view of a term's posting list.
///
/// Blocks appear in descending `id` order: each `Skip` is immediately
/// followed by the `Postings` block it describes, and successive pairs cover
/// strictly lower id ranges.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TermPostingsValue {
    pub(crate) blocks: Vec<TermPostingsBlock>,
}

impl TermPostingsValue {
    /// Build a value from a flat list of postings in arbitrary order.
    ///
    /// Postings are sorted descending and chunked into
    /// [`TARGET_POSTINGS_PER_BLOCK`]-sized `Postings` blocks; each block is
    /// preceded by its `Skip` summary.
    pub(crate) fn from_postings(mut postings: Vec<PostingEntry>) -> Self {
        if postings.is_empty() {
            return Self::default();
        }
        postings.sort_unstable_by_key(|p| std::cmp::Reverse(p.id));
        // De-dup defensively; a single id should appear at most once per term per batch.
        postings.dedup_by_key(|e| e.id);

        let mut blocks = Vec::new();
        for chunk in postings.chunks(TARGET_POSTINGS_PER_BLOCK) {
            let postings_block = PostingsBlock::from_descending(chunk.to_vec());
            let mut payload = BytesMut::new();
            postings_block.encode_payload(&mut payload);
            let skip = SkipBlock {
                last_id: postings_block.last_id(),
                length: payload.len() as u64,
            };
            blocks.push(TermPostingsBlock::Skip(skip));
            blocks.push(TermPostingsBlock::Postings(postings_block));
        }
        Self { blocks }
    }

    /// Iterate all decoded posting entries in descending `id` order across blocks.
    pub(crate) fn iter_entries(&self) -> impl Iterator<Item = &PostingEntry> {
        self.blocks.iter().flat_map(|block| match block {
            TermPostingsBlock::Postings(p) => p.entries.iter(),
            TermPostingsBlock::Skip(_) => [].iter(),
        })
    }

    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        for block in &self.blocks {
            encode_block(block, &mut buf);
        }
        buf.freeze()
    }

    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut cursor = buf;
        let mut blocks = Vec::new();
        while !cursor.is_empty() {
            if cursor.len() < 5 {
                return Err(EncodingError {
                    message: "term postings value truncated at block header".to_string(),
                });
            }
            let block_type = cursor[0];
            let len = u32::from_le_bytes([cursor[1], cursor[2], cursor[3], cursor[4]]) as usize;
            cursor = &cursor[5..];
            if cursor.len() < len {
                return Err(EncodingError {
                    message: format!(
                        "term postings value truncated within block: need {}, have {}",
                        len,
                        cursor.len()
                    ),
                });
            }
            let payload = &cursor[..len];
            cursor = &cursor[len..];
            let block = match block_type {
                BLOCK_TYPE_SKIP => TermPostingsBlock::Skip(SkipBlock::decode_payload(payload)?),
                BLOCK_TYPE_POSTINGS => {
                    TermPostingsBlock::Postings(PostingsBlock::decode_payload(payload)?)
                }
                other => {
                    return Err(EncodingError {
                        message: format!("unknown term postings block type: 0x{:02x}", other),
                    });
                }
            };
            blocks.push(block);
        }
        Ok(Self { blocks })
    }
}

fn encode_block(block: &TermPostingsBlock, buf: &mut BytesMut) {
    let mut payload = BytesMut::new();
    let (tag, payload_buf) = match block {
        TermPostingsBlock::Skip(s) => {
            s.encode_payload(&mut payload);
            (BLOCK_TYPE_SKIP, &payload)
        }
        TermPostingsBlock::Postings(p) => {
            p.encode_payload(&mut payload);
            (BLOCK_TYPE_POSTINGS, &payload)
        }
    };
    buf.put_u8(tag);
    buf.put_u32_le(payload_buf.len() as u32);
    buf.extend_from_slice(payload_buf);
}

/// Merge multiple `TermPostings` values via byte concatenation.
///
/// SlateDB delivers operands oldest-to-newest. Newer batches insert strictly
/// higher vector ids (ids are monotonically allocated and Milestone 0 is
/// insert-only), so reversing the operand list and prepending it to the
/// existing value preserves the global descending-id invariant. No parsing
/// or re-encoding required.
pub(crate) fn merge_batch_term_postings(existing: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    if operands.is_empty() {
        return existing.unwrap_or_default();
    }
    if operands.len() == 1 && existing.is_none() {
        return operands[0].clone();
    }
    let total: usize = existing.as_ref().map(|b| b.len()).unwrap_or(0)
        + operands.iter().map(|b| b.len()).sum::<usize>();
    let mut out = BytesMut::with_capacity(total);
    for operand in operands.iter().rev() {
        out.extend_from_slice(operand);
    }
    if let Some(ex) = existing {
        out.extend_from_slice(&ex);
    }
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: u64, freq: u32, norm: u8) -> PostingEntry {
        PostingEntry {
            id: VectorId::from_raw(id),
            freq,
            norm,
        }
    }

    #[test]
    fn should_roundtrip_empty_value() {
        // given
        let value = TermPostingsValue::default();

        // when
        let encoded = value.encode_to_bytes();
        let decoded = TermPostingsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_roundtrip_single_block() {
        // given
        let value = TermPostingsValue::from_postings(vec![
            entry(5, 1, 10),
            entry(2, 3, 20),
            entry(8, 2, 30),
        ]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = TermPostingsValue::decode_from_bytes(&encoded).unwrap();

        // then - two blocks: Skip + Postings
        assert_eq!(decoded.blocks.len(), 2);
        assert!(matches!(decoded.blocks[0], TermPostingsBlock::Skip(_)));
        assert!(matches!(decoded.blocks[1], TermPostingsBlock::Postings(_)));
        // Entries descending by id with original freq/norm preserved.
        let entries: Vec<_> = decoded.iter_entries().copied().collect();
        assert_eq!(
            entries,
            vec![entry(8, 2, 30), entry(5, 1, 10), entry(2, 3, 20),]
        );
    }

    #[test]
    fn should_chunk_postings_into_blocks_of_target_size() {
        // given - 2.5x target so we get 3 (Skip,Postings) pairs
        let n = TARGET_POSTINGS_PER_BLOCK * 2 + TARGET_POSTINGS_PER_BLOCK / 2;
        let postings: Vec<_> = (0..n as u64)
            .map(|i| entry(i, (i % 7) as u32 + 1, ((i % 11) as u8) + 1))
            .collect();
        let value = TermPostingsValue::from_postings(postings);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = TermPostingsValue::decode_from_bytes(&encoded).unwrap();

        // then - 3 pairs of (Skip, Postings)
        assert_eq!(decoded.blocks.len(), 6);
        let entries: Vec<_> = decoded.iter_entries().copied().collect();
        assert_eq!(entries.len(), n);
        // Strictly descending
        for w in entries.windows(2) {
            assert!(w[0].id > w[1].id);
        }
    }

    #[test]
    fn should_emit_skip_block_for_every_postings_block() {
        // given - enough postings for 2 blocks
        let n = TARGET_POSTINGS_PER_BLOCK + 5;
        let postings: Vec<_> = (0..n as u64).map(|i| entry(i, 1, 1)).collect();
        let value = TermPostingsValue::from_postings(postings);

        // when
        let kinds: Vec<&str> = value
            .blocks
            .iter()
            .map(|b| match b {
                TermPostingsBlock::Skip(_) => "skip",
                TermPostingsBlock::Postings(_) => "postings",
            })
            .collect();

        // then
        assert_eq!(kinds, vec!["skip", "postings", "skip", "postings"]);
    }

    #[test]
    fn should_merge_concatenating_newer_blocks_first() {
        // given - older operand has lower ids, newer operand has higher ids
        let older = TermPostingsValue::from_postings(vec![entry(1, 1, 1), entry(2, 1, 1)])
            .encode_to_bytes();
        let newer = TermPostingsValue::from_postings(vec![entry(10, 1, 1), entry(11, 1, 1)])
            .encode_to_bytes();

        // when - operands ordered oldest -> newest
        let merged = merge_batch_term_postings(None, &[older, newer]);
        let decoded = TermPostingsValue::decode_from_bytes(&merged).unwrap();

        // then - ids are globally descending
        let ids: Vec<u64> = decoded.iter_entries().map(|e| e.id.id()).collect();
        assert_eq!(ids, vec![11, 10, 2, 1]);
    }

    #[test]
    fn should_merge_with_existing_value() {
        // given
        let existing = TermPostingsValue::from_postings(vec![entry(1, 1, 1)]).encode_to_bytes();
        let op = TermPostingsValue::from_postings(vec![entry(5, 1, 1)]).encode_to_bytes();

        // when
        let merged = merge_batch_term_postings(Some(existing), &[op]);
        let decoded = TermPostingsValue::decode_from_bytes(&merged).unwrap();

        // then - ids globally descending
        let ids: Vec<u64> = decoded.iter_entries().map(|e| e.id.id()).collect();
        assert_eq!(ids, vec![5, 1]);
    }

    #[test]
    fn merge_is_pure_byte_concatenation() {
        // given - the encoded forms of two operands
        let older = TermPostingsValue::from_postings(vec![entry(1, 1, 1)]).encode_to_bytes();
        let newer = TermPostingsValue::from_postings(vec![entry(10, 1, 1)]).encode_to_bytes();
        let mut expected = Vec::with_capacity(older.len() + newer.len());
        expected.extend_from_slice(&newer); // newer first
        expected.extend_from_slice(&older);

        // when
        let merged = merge_batch_term_postings(None, &[older, newer]);

        // then
        assert_eq!(merged.as_ref(), expected.as_slice());
    }

    #[test]
    fn should_preserve_skip_last_id_and_length() {
        // given
        let value = TermPostingsValue::from_postings(vec![
            entry(10, 1, 1),
            entry(20, 1, 1),
            entry(30, 1, 1),
        ]);

        // when
        let skip = value
            .blocks
            .iter()
            .find_map(|b| match b {
                TermPostingsBlock::Skip(s) => Some(*s),
                _ => None,
            })
            .unwrap();

        // then - last_id is the lowest id in the following postings block; length matches
        assert_eq!(skip.last_id.id(), 10);
        let postings_payload_len = {
            let postings = value
                .blocks
                .iter()
                .find_map(|b| match b {
                    TermPostingsBlock::Postings(p) => Some(p),
                    _ => None,
                })
                .unwrap();
            let mut buf = BytesMut::new();
            postings.encode_payload(&mut buf);
            buf.len() as u64
        };
        assert_eq!(skip.length, postings_payload_len);
    }

    #[test]
    fn should_use_bitpacked_id_encoding_when_gaps_fit_in_u32() {
        // given
        let value = TermPostingsValue::from_postings(vec![
            entry(100, 1, 1),
            entry(50, 1, 1),
            entry(1, 1, 1),
        ]);

        // when - inspect the raw payload for the ids_encoding byte
        let TermPostingsBlock::Postings(p) = &value.blocks[1] else {
            panic!("expected postings block")
        };
        let mut payload = BytesMut::new();
        p.encode_payload(&mut payload);
        // count(4) + base_id(8) = 12, then ids_encoding byte
        let ids_encoding = payload[12];

        // then
        assert_eq!(ids_encoding, IDS_ENCODING_BITPACKED);

        // and - encode/decode round-trips identical entries
        let encoded = value.encode_to_bytes();
        let decoded = TermPostingsValue::decode_from_bytes(&encoded).unwrap();
        let ids: Vec<u64> = decoded.iter_entries().map(|e| e.id.id()).collect();
        assert_eq!(ids, vec![100, 50, 1]);
    }

    #[test]
    fn should_switch_to_varint_id_encoding_when_gap_exceeds_u32() {
        // given - gap between two consecutive entries exceeds u32::MAX
        let huge = (u32::MAX as u64) + 2; // gap will be 1 too big for u32
        let value = TermPostingsValue::from_postings(vec![entry(huge, 1, 5), entry(0, 2, 7)]);

        // when - inspect ids_encoding byte
        let TermPostingsBlock::Postings(p) = &value.blocks[1] else {
            panic!("expected postings block")
        };
        let mut payload = BytesMut::new();
        p.encode_payload(&mut payload);
        let ids_encoding = payload[12];

        // then
        assert_eq!(ids_encoding, IDS_ENCODING_VARINT);

        // and - encode/decode preserves the original entries (huge id and all)
        let encoded = value.encode_to_bytes();
        let decoded = TermPostingsValue::decode_from_bytes(&encoded).unwrap();
        let entries: Vec<_> = decoded.iter_entries().copied().collect();
        assert_eq!(entries, vec![entry(huge, 1, 5), entry(0, 2, 7)]);
    }

    #[test]
    fn u64_varint_round_trips() {
        // given - assorted u64 values, including u32 boundaries
        for &v in &[
            0u64,
            1,
            127,
            128,
            16_383,
            16_384,
            u32::MAX as u64,
            (u32::MAX as u64) + 1,
            (u32::MAX as u64) * 17,
            u64::MAX,
        ] {
            // when
            let mut buf = BytesMut::new();
            write_u64_varint(&mut buf, v);
            let (decoded, used) = read_u64_varint(&buf).unwrap();

            // then
            assert_eq!(decoded, v);
            assert_eq!(used, buf.len());
        }
    }

    #[test]
    fn should_roundtrip_full_block_of_256_entries() {
        // given - exactly TARGET_POSTINGS_PER_BLOCK postings
        let postings: Vec<_> = (0..TARGET_POSTINGS_PER_BLOCK as u64)
            .map(|i| entry(i, (i as u32 % 5) + 1, ((i % 200) as u8) + 1))
            .collect();
        let value = TermPostingsValue::from_postings(postings.clone());

        // when
        let encoded = value.encode_to_bytes();
        let decoded = TermPostingsValue::decode_from_bytes(&encoded).unwrap();

        // then
        let entries: Vec<_> = decoded.iter_entries().copied().collect();
        assert_eq!(entries.len(), postings.len());
        // Original postings sorted descending should match decoded order.
        let mut expected = postings;
        expected.sort_unstable_by_key(|e| std::cmp::Reverse(e.id));
        assert_eq!(entries, expected);
    }
}
