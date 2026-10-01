//! `TermPostings` value encoding/decoding for FTS (RFC-0006).
//!
//! A term's posting list is a sequence of typed blocks written one after
//! another to the end of the value buffer. Each `Postings` block holds up to
//! [`TARGET_POSTINGS_PER_BLOCK`] `(id, freq, norm)` triples in strictly
//! ascending `id` order. Each `Postings` block is preceded by a `Skip`
//! block that summarises the postings block's last (highest) id, impacts,
//! and serialized byte length, so readers can index and skip blocks without
//! decoding their payloads. Multiple `(Skip, Postings)` pairs may appear in
//! a single value, also in ascending id order (each pair's ids are strictly
//! higher than all ids in the preceding pair).
//!
//! Each skip block carries the `impacts` of its postings block: the dominating
//! `(freq, norm)` pairs of the contained documents (RFC-0006). The query path
//! uses them to compute a tight upper bound on the BM25 contribution of any
//! document in the block without decoding the block (BlockMaxScore).
//!
//! The canonical decoded representation is columnar: [`TermPostingsView`]
//! parses block headers into a directory and decodes payloads on demand into
//! ascending parallel arrays ([`DecodedPostingsBlock`]). Row-oriented readers
//! use [`TermPostingsView::iter_entries`]; [`TermPostingsEncoder`] is the
//! encode-side builder only.
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
//! │  base_id:      u64   (lowest id in block, == ids[0])             │
//! │  ids_encoding: u8   (0 = bitpacked gaps, 1 = varint gaps)       │
//! │  -- if ids_encoding == 0:                                        │
//! │     ids_bits:    u8                                              │
//! │     ids_packed:  ids_bits * 32 bytes (BitPacker8x of 256 gaps)  │
//! │  -- if ids_encoding == 1:                                        │
//! │     ids_varint:  count - 1 u64 LEB128 unsigned varints           │
//! │  freqs_bits:   u8                                                │
//! │  freqs_packed: freqs_bits * 32 bytes (BitPacker8x of 256 freqs)  │
//! │  norms:        count bytes                                       │
//! └──────────────────────────────────────────────────────────────────┘
//!
//! `ids_*` carries the ascending gaps `ids[i] - ids[i-1]` for `i >= 1`
//! (`base_id` itself is stored explicitly). When every gap fits in `u32`
//! they are stored as a plain bitpacked 256-element [`BitPacker8x`] block
//! (tail padded with zeros so it never widens `ids_bits`); otherwise each
//! gap is written as an unsigned LEB128 `u64` varint (`ids_encoding = 1`).
//!
//! Skip (type = 0x01) payload
//! ┌──────────────────────────────────────────────────────────────────┐
//! │  last_id:        u64   (highest id in block, == ids[count - 1])   │
//! │  impacts_count:  u16                                              │
//! │  impacts:        impacts_count * { freq: u32, norm: u8 }          │
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
//! operator can simply concatenate operands' bytes after the existing
//! value's bytes, in delivery (oldest-to-newest) order — preserving the
//! global ascending-id ordering.

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

/// Ascending id gaps packed via [`BitPacker8x`]. Used when every gap fits
/// in `u32`. This format encodes deltas between consecutive ids. One alternative
/// is to encode deltas between each id and the first id in the block. These are
/// strictly sorted and could use [`BitPacker8x#compress_strictly_sorted`]. This
/// was measured to be much slower (1.9us vs 36ns per block)
const IDS_ENCODING_BITPACKED: u8 = 0;

/// Ascending id gaps written one-by-one as unsigned LEB128 `u64` varints.
/// Used when at least one gap exceeds `u32::MAX`.
const IDS_ENCODING_VARINT: u8 = 1;

/// A single posting entry within a `Postings` block. Used for construction
/// and row-oriented/aos iteration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PostingEntry {
    pub(crate) id: VectorId,
    pub(crate) freq: u32,
    pub(crate) norm: u8,
}

/// A `(freq, norm)` pair bounding the BM25 contribution of the documents in a
/// postings block (RFC-0006).
///
/// Impacts have a partial order: a pair with higher `freq` *and* lower `norm`
/// than another is guaranteed to score at least as high under BM25 (the score
/// is non-decreasing in frequency and non-increasing in document length). A
/// skip block stores the *dominating* pairs — those not lower than any other
/// pair in the block — so the max score over the block is the max score over
/// its impacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Impact {
    pub(crate) freq: u32,
    pub(crate) norm: u8,
}

/// Compute the dominating impacts of a postings block.
///
/// Returns the pareto frontier of the entries' `(freq, norm)` pairs, sorted by
/// ascending `norm` (and therefore strictly ascending `freq`). Bounded by 256
/// entries since norms are one byte; in practice much smaller.
fn compute_dominating_impacts(entries: &[PostingEntry]) -> Vec<Impact> {
    // Max freq seen for each norm value.
    let mut max_freq_by_norm = [0u32; 256];
    for entry in entries {
        let slot = &mut max_freq_by_norm[entry.norm as usize];
        // this assumes entry.freq is non-zero, so entry.freq.max(1) _should_ be a no-op
        *slot = (*slot).max(entry.freq.max(1));
    }
    // Sweep norms ascending; a pair survives only when its freq strictly
    // exceeds every pair with a lower (better) norm.
    let mut impacts = Vec::new();
    let mut running_max = 0u32;
    for (norm, &freq) in max_freq_by_norm.iter().enumerate() {
        if freq > running_max {
            impacts.push(Impact {
                freq,
                norm: norm as u8,
            });
            running_max = freq;
        }
    }
    impacts
}

/// A `Postings` block: a run of `PostingEntry` in ascending `id` order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PostingsBlock {
    /// Entries, sorted by ascending `id`.
    pub(crate) entries: Vec<PostingEntry>,
}

impl PostingsBlock {
    pub(crate) fn from_ascending(entries: Vec<PostingEntry>) -> Self {
        debug_assert!(
            entries.windows(2).all(|w| w[0].id < w[1].id),
            "PostingsBlock entries must be strictly ascending"
        );
        debug_assert!(entries.len() <= BLOCK_LEN);
        Self { entries }
    }

    /// Id of the last (highest) entry in the block.
    pub(crate) fn last_id(&self) -> VectorId {
        self.entries
            .last()
            .map(|e| e.id)
            .expect("invalid empty posting block")
    }

    fn encoded_len(&self) -> usize {
        let mut payload = BytesMut::new();
        self.encode_payload(&mut payload);
        payload.len()
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

        let bitpacker = BitPacker8x::new();

        // Gaps between consecutive ids for entries 1.. (the first entry is
        // base_id itself, so only `count - 1` gaps are real).
        let mut max_gap = 0u64;
        for window in self.entries.windows(2) {
            max_gap = max_gap.max(window[1].id.id() - window[0].id.id());
        }

        if max_gap <= u32::MAX as u64 {
            buf.put_u8(IDS_ENCODING_BITPACKED);
            // Pad the tail with zeros — zero is `<= max_gap`, so padding
            // never widens ids_bits beyond what the real data requires.
            let mut gaps = [0u32; BLOCK_LEN];
            for (i, window) in self.entries.windows(2).enumerate() {
                gaps[i] = (window[1].id.id() - window[0].id.id()) as u32;
            }
            let ids_bits = bitpacker.num_bits(&gaps);
            buf.put_u8(ids_bits);
            let ids_bytes = ids_bits as usize * (BLOCK_LEN / 8);
            let mut packed = vec![0u8; ids_bytes];
            let written = bitpacker.compress(&gaps, &mut packed, ids_bits);
            debug_assert_eq!(written, ids_bytes);
            buf.extend_from_slice(&packed);
        } else {
            buf.put_u8(IDS_ENCODING_VARINT);
            for window in self.entries.windows(2) {
                write_u64_varint(buf, window[1].id.id() - window[0].id.id());
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
}

/// Decode a `Postings` block payload into caller-provided stack arrays in
/// ascending id order. Returns the entry count and the borrowed norms
/// slice. Allocation-free so the hot decode path can reuse buffers.
fn decode_postings_block_raw<'a>(
    buf: &'a [u8],
    ids: &mut [u64; BLOCK_LEN],
    freqs: &mut [u32; BLOCK_LEN],
) -> Result<(usize, &'a [u8]), EncodingError> {
    let mut cursor = buf;
    if cursor.len() < 4 {
        return Err(EncodingError {
            message: "postings block payload too short for count".to_string(),
        });
    }
    let count = u32::from_le_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]) as usize;
    cursor = &cursor[4..];
    if count == 0 {
        return Ok((0, &[]));
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

    // The first id is base_id itself; the remaining `count - 1` come from
    // the encoded section.
    ids[0] = base_id;
    match ids_encoding {
        IDS_ENCODING_BITPACKED => {
            if cursor.is_empty() {
                return Err(EncodingError {
                    message: "postings block payload too short for ids_bits".to_string(),
                });
            }
            let ids_bits = cursor[0];
            cursor = &cursor[1..];
            // BitPacker8x asserts (panics) on widths above 32; corrupt data
            // must surface as a recoverable error instead.
            if ids_bits > 32 {
                return Err(EncodingError {
                    message: format!("postings ids_bits {} exceeds 32", ids_bits),
                });
            }
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
            let mut gaps = [0u32; BLOCK_LEN];
            let consumed = bitpacker.decompress(&cursor[..ids_bytes], &mut gaps, ids_bits);
            debug_assert_eq!(consumed, ids_bytes);
            cursor = &cursor[ids_bytes..];
            let mut current = base_id;
            for i in 1..count {
                current = current
                    .checked_add(gaps[i - 1] as u64)
                    .ok_or_else(|| EncodingError {
                        message: format!("gap {} overflows running id {}", gaps[i - 1], current),
                    })?;
                ids[i] = current;
            }
        }
        IDS_ENCODING_VARINT => {
            let mut current = base_id;
            for slot in ids.iter_mut().take(count).skip(1) {
                let (g, used) = read_u64_varint(cursor)?;
                cursor = &cursor[used..];
                current = current.checked_add(g).ok_or_else(|| EncodingError {
                    message: format!("gap {} overflows running id {}", g, current),
                })?;
                *slot = current;
            }
        }
        other => {
            return Err(EncodingError {
                message: format!("unknown postings ids_encoding: 0x{:02x}", other),
            });
        }
    }

    // freqs
    if cursor.is_empty() {
        return Err(EncodingError {
            message: "postings block payload too short for freqs_bits".to_string(),
        });
    }
    let freqs_bits = cursor[0];
    cursor = &cursor[1..];
    if freqs_bits > 32 {
        return Err(EncodingError {
            message: format!("postings freqs_bits {} exceeds 32", freqs_bits),
        });
    }
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
    let consumed = bitpacker.decompress(&cursor[..freqs_bytes], freqs, freqs_bits);
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
    Ok((count, &cursor[..count]))
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
/// `impacts` holds the dominating `(freq, norm)` pairs of the postings block
/// (see [`Impact`]). An empty list is valid on the wire and means "unknown" —
/// readers must fall back to the term's global score bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkipBlock {
    pub(crate) last_id: VectorId,
    pub(crate) impacts: Vec<Impact>,
    pub(crate) length: u64,
}

impl SkipBlock {
    fn encode_payload(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.last_id.id());
        debug_assert!(self.impacts.len() <= u16::MAX as usize);
        buf.put_u16_le(self.impacts.len() as u16);
        for impact in &self.impacts {
            buf.put_u32_le(impact.freq);
            buf.put_u8(impact.norm);
        }
        buf.put_u64_le(self.length);
    }
}

/// Block within a `TermPostingsValue` builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TermPostingsBlock {
    Skip(SkipBlock),
    Postings(PostingsBlock),
}

/// Encode-side builder for a term's posting list. The decoded/canonical
/// representation is [`TermPostingsView`].
///
/// Blocks appear in ascending `id` order: each `Skip` is immediately
/// followed by the `Postings` block it describes, and successive pairs cover
/// strictly higher id ranges.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TermPostingsEncoder {
    pub(crate) blocks: Vec<TermPostingsBlock>,
}

impl TermPostingsEncoder {
    /// Build a value from a flat list of postings in arbitrary order.
    ///
    /// Postings are sorted ascending and chunked into
    /// [`TARGET_POSTINGS_PER_BLOCK`]-sized `Postings` blocks; each block is
    /// preceded by its `Skip` summary.
    pub(crate) fn from_postings(mut postings: Vec<PostingEntry>) -> Self {
        if postings.is_empty() {
            return Self::default();
        }
        postings.sort_unstable_by_key(|p| p.id);
        // De-dup defensively; a single id should appear at most once per term per batch.
        postings.dedup_by_key(|e| e.id);

        let mut blocks = Vec::new();
        for chunk in postings.chunks(TARGET_POSTINGS_PER_BLOCK) {
            let postings_block = PostingsBlock::from_ascending(chunk.to_vec());
            let skip = SkipBlock {
                last_id: postings_block.last_id(),
                impacts: compute_dominating_impacts(&postings_block.entries),
                length: postings_block.encoded_len() as u64,
            };
            blocks.push(TermPostingsBlock::Skip(skip));
            blocks.push(TermPostingsBlock::Postings(postings_block));
        }
        Self { blocks }
    }

    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        for block in &self.blocks {
            Self::encode_block(block, &mut buf);
        }
        buf.freeze()
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
}

/// Directory entry for one `(Skip, Postings)` pair within an encoded
/// `TermPostings` value, recorded by [`TermPostingsView::parse`].
#[derive(Debug, Clone)]
pub(crate) struct PostingBlockMeta {
    /// Lowest doc id in the block (the postings payload's `base_id`).
    pub(crate) min_id: u64,
    /// Highest doc id in the block (the skip block's `last_id`).
    pub(crate) max_id: u64,
    /// Number of postings in the block.
    pub(crate) count: u32,
    /// Range of this block's impacts within the view's impact arena.
    impacts_start: u32,
    impacts_len: u16,
    /// Byte offset of the skip+postings pair's skip-block header within the raw value;
    /// with `payload_start + payload_len` this delimits the whole
    /// `(Skip, Postings)` pair for verbatim copying.
    pair_start: usize,
    /// Byte range of the postings payload within the raw value.
    payload_start: usize,
    payload_len: usize,
}

/// A `Postings` block decoded into parallel arrays in ascending doc id
/// order — the storage order, and the iteration order of the BlockMaxScore
/// query path.
#[derive(Debug, Clone, Default)]
pub(crate) struct DecodedPostingsBlock {
    pub(crate) ids: Vec<u64>,
    pub(crate) freqs: Vec<u32>,
    pub(crate) norms: Vec<u8>,
}

/// A lazily-decoded view over an encoded term postings value.
///
/// [`parse`](Self::parse) scans only the block headers and skip payloads —
/// it never touches the bitpacked postings payloads — and records where each
/// payload lives so [`decode_block_into`](Self::decode_block_into) can decode
/// blocks on demand. This is what lets BlockMaxScore skip whole blocks
/// without paying their decode cost.
///
/// `blocks` is ordered by ascending doc id range (the storage order):
/// `blocks[0]` holds the lowest ids. Ranges of distinct blocks never
/// overlap (merge operands partition the id space). All blocks' impacts live
/// in one arena (`impacts`) rather than per-block `Vec`s so a query on a
/// common term with thousands of blocks doesn't pay one allocation per block.
#[derive(Debug, Clone, Default)]
pub(crate) struct TermPostingsView {
    raw: Bytes,
    blocks: Vec<PostingBlockMeta>,
    impacts: Vec<Impact>,
}

impl TermPostingsView {
    /// Scan the value's block headers into a directory.
    pub(crate) fn parse(raw: Bytes) -> Result<Self, EncodingError> {
        let mut blocks = Vec::new();
        let mut impacts: Vec<Impact> = Vec::new();
        let mut offset = 0usize;
        // (max_id, impacts range, pair start) decoded from the pending skip
        // block.
        let mut pending_skip: Option<(u64, u32, u16, usize)> = None;
        while offset < raw.len() {
            if raw.len() - offset < 5 {
                return Err(EncodingError {
                    message: "term postings value truncated at block header".to_string(),
                });
            }
            let block_type = raw[offset];
            let len = u32::from_le_bytes([
                raw[offset + 1],
                raw[offset + 2],
                raw[offset + 3],
                raw[offset + 4],
            ]) as usize;
            let payload_start = offset + 5;
            if raw.len() - payload_start < len {
                return Err(EncodingError {
                    message: format!(
                        "term postings value truncated within block: need {}, have {}",
                        len,
                        raw.len() - payload_start
                    ),
                });
            }
            let payload = &raw[payload_start..payload_start + len];
            offset = payload_start + len;
            match block_type {
                BLOCK_TYPE_SKIP => {
                    if pending_skip.is_some() {
                        return Err(EncodingError {
                            message: "skip block not followed by postings block".to_string(),
                        });
                    }
                    // Decode the skip payload straight into the arena instead
                    // of through SkipBlock (which allocates per block).
                    if payload.len() < 8 + 2 + 8 {
                        return Err(EncodingError {
                            message: format!(
                                "skip block payload too short: need {} bytes, have {}",
                                8 + 2 + 8,
                                payload.len()
                            ),
                        });
                    }
                    let max_id = u64::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5],
                        payload[6], payload[7],
                    ]);
                    let impacts_count = u16::from_le_bytes([payload[8], payload[9]]) as usize;
                    let impacts_bytes = impacts_count * 5;
                    if payload.len() < 10 + impacts_bytes + 8 {
                        return Err(EncodingError {
                            message: "skip block payload truncated within impacts".to_string(),
                        });
                    }
                    let impacts_start = impacts.len() as u32;
                    for chunk in payload[10..10 + impacts_bytes].chunks_exact(5) {
                        impacts.push(Impact {
                            freq: u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]),
                            norm: chunk[4],
                        });
                    }
                    pending_skip = Some((
                        max_id,
                        impacts_start,
                        impacts_count as u16,
                        payload_start - 5,
                    ));
                }
                BLOCK_TYPE_POSTINGS => {
                    let Some((max_id, impacts_start, impacts_len, pair_start)) =
                        pending_skip.take()
                    else {
                        return Err(EncodingError {
                            message: "postings block without preceding skip block".to_string(),
                        });
                    };
                    // Peek count and base_id from the payload header without
                    // decompressing the block.
                    if len < 4 {
                        return Err(EncodingError {
                            message: "postings block payload too short for count".to_string(),
                        });
                    }
                    let count =
                        u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
                    if count == 0 {
                        // Empty block: drop the (skip, postings) pair.
                        impacts.truncate(impacts_start as usize);
                        continue;
                    }
                    if len < 12 {
                        return Err(EncodingError {
                            message: "postings block payload too short for base id".to_string(),
                        });
                    }
                    let min_id = u64::from_le_bytes([
                        payload[4],
                        payload[5],
                        payload[6],
                        payload[7],
                        payload[8],
                        payload[9],
                        payload[10],
                        payload[11],
                    ]);
                    // The directory invariants every consumer relies on
                    // (block ranges are well-formed, ascending, and
                    // disjoint) come from persisted bytes, so check them
                    // here rather than trusting the writer.
                    if min_id > max_id {
                        return Err(EncodingError {
                            message: format!(
                                "postings block base_id {} exceeds skip block last_id {}",
                                min_id, max_id
                            ),
                        });
                    }
                    if let Some(prev) = blocks.last().map(|b: &PostingBlockMeta| b.max_id)
                        && min_id <= prev
                    {
                        return Err(EncodingError {
                            message: format!(
                                "postings block base_id {} does not ascend past previous block's last id {}",
                                min_id, prev
                            ),
                        });
                    }
                    blocks.push(PostingBlockMeta {
                        min_id,
                        max_id,
                        count,
                        impacts_start,
                        impacts_len,
                        pair_start,
                        payload_start,
                        payload_len: len,
                    });
                }
                other => {
                    return Err(EncodingError {
                        message: format!("unknown term postings block type: 0x{:02x}", other),
                    });
                }
            }
        }
        if pending_skip.is_some() {
            return Err(EncodingError {
                message: "trailing skip block without postings block".to_string(),
            });
        }
        Ok(Self {
            raw,
            blocks,
            impacts,
        })
    }

    /// Directory of blocks in ascending doc id order.
    pub(crate) fn blocks(&self) -> &[PostingBlockMeta] {
        &self.blocks
    }

    /// Impacts of block `idx`. May be empty ("unknown") — callers must fall
    /// back to the term's global score bound.
    pub(crate) fn impacts(&self, idx: usize) -> &[Impact] {
        let meta = &self.blocks[idx];
        &self.impacts[meta.impacts_start as usize..][..meta.impacts_len as usize]
    }

    /// Decode block `idx` into `out` as ascending-id parallel arrays,
    /// reusing `out`'s buffers (capacity stabilizes at one block after the
    /// first decode).
    pub(crate) fn decode_block_into(
        &self,
        idx: usize,
        out: &mut DecodedPostingsBlock,
    ) -> Result<(), EncodingError> {
        let meta = &self.blocks[idx];
        let payload = &self.raw[meta.payload_start..meta.payload_start + meta.payload_len];
        let mut ids = [0u64; BLOCK_LEN];
        let mut freqs = [0u32; BLOCK_LEN];
        let (count, norms) = decode_postings_block_raw(payload, &mut ids, &mut freqs)?;
        out.ids.clear();
        out.ids.extend_from_slice(&ids[..count]);
        out.freqs.clear();
        out.freqs.extend_from_slice(&freqs[..count]);
        out.norms.clear();
        out.norms.extend_from_slice(norms);
        Ok(())
    }

    /// Decode block `idx` into fresh ascending-id parallel arrays.
    #[cfg(test)]
    pub(crate) fn decode_block(&self, idx: usize) -> Result<DecodedPostingsBlock, EncodingError> {
        let mut out = DecodedPostingsBlock::default();
        self.decode_block_into(idx, &mut out)?;
        Ok(out)
    }

    /// The raw bytes of block `idx`'s whole `(Skip, Postings)` pair —
    /// headers included — for copying into a rewritten value verbatim.
    pub(crate) fn pair_bytes(&self, idx: usize) -> &[u8] {
        &self.raw[self.pair_range(idx)]
    }

    /// Byte range of block `idx`'s whole `(Skip, Postings)` pair within the
    /// raw value.
    pub(crate) fn pair_range(&self, idx: usize) -> std::ops::Range<usize> {
        let meta = &self.blocks[idx];
        meta.pair_start..meta.payload_start + meta.payload_len
    }

    /// Iterate every posting in ascending doc id order, decoding blocks
    /// lazily into a reused buffer (memory stays O(one block)). The
    /// row-oriented adapter over the columnar representation: yields an
    /// `Err` once and ends if a block payload fails to decode.
    pub(crate) fn iter_entries(&self) -> PostingEntriesIter<'_> {
        PostingEntriesIter {
            view: self,
            block_idx: 0,
            decoded: DecodedPostingsBlock::default(),
            pos: 0,
            failed: false,
        }
    }
}

/// Row-oriented iterator over a [`TermPostingsView`] that returns `PostingEntry`s; see
/// [`TermPostingsView::iter_entries`].
pub(crate) struct PostingEntriesIter<'a> {
    view: &'a TermPostingsView,
    /// Index of the next block to decode.
    block_idx: usize,
    /// Decoded arrays for the block currently being yielded.
    decoded: DecodedPostingsBlock,
    /// Cursor within `decoded`.
    pos: usize,
    failed: bool,
}

impl Iterator for PostingEntriesIter<'_> {
    type Item = Result<PostingEntry, EncodingError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        while self.pos >= self.decoded.ids.len() {
            if self.block_idx >= self.view.blocks().len() {
                return None;
            }
            if let Err(err) = self
                .view
                .decode_block_into(self.block_idx, &mut self.decoded)
            {
                self.failed = true;
                return Some(Err(err));
            }
            self.block_idx += 1;
            self.pos = 0;
        }
        let i = self.pos;
        self.pos += 1;
        Some(Ok(PostingEntry {
            id: VectorId::from_raw(self.decoded.ids[i]),
            freq: self.decoded.freqs[i],
            norm: self.decoded.norms[i],
        }))
    }
}

/// Merge multiple `TermPostings` values via byte concatenation.
///
/// SlateDB delivers operands oldest-to-newest. Newer batches insert strictly
/// higher vector ids (ids are monotonically allocated; deletes never reuse
/// ids), so appending operands after the existing value, in delivery order,
/// preserves the global ascending-id invariant. No parsing or re-encoding
/// required.
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
    if let Some(ex) = existing {
        out.extend_from_slice(&ex);
    }
    for operand in operands {
        out.extend_from_slice(operand);
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

    /// Decode every posting through the view's row iterator.
    fn all_entries(encoded: Bytes) -> Vec<PostingEntry> {
        TermPostingsView::parse(encoded)
            .unwrap()
            .iter_entries()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    /// Corrupt bytes at the given payload offsets within the `nth`
    /// postings-block payload of an encoded value.
    fn corrupt_nth_postings_payload(encoded: &Bytes, nth: usize, edits: &[(usize, u8)]) -> Bytes {
        let mut bytes = encoded.to_vec();
        let mut offset = 0usize;
        let mut seen = 0usize;
        loop {
            let tag = bytes[offset];
            let len = u32::from_le_bytes([
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
            ]) as usize;
            let payload_start = offset + 5;
            if tag == BLOCK_TYPE_POSTINGS {
                if seen == nth {
                    for &(off, val) in edits {
                        bytes[payload_start + off] = val;
                    }
                    return Bytes::from(bytes);
                }
                seen += 1;
            }
            offset = payload_start + len;
        }
    }

    /// Corrupt bytes within the first postings-block payload.
    fn corrupt_postings_payload(encoded: &Bytes, edits: &[(usize, u8)]) -> Bytes {
        corrupt_nth_postings_payload(encoded, 0, edits)
    }

    /// parse() only validates framing; a payload corrupted past the header
    /// must surface from decode as a recoverable error, never a panic.
    #[test]
    fn corrupt_payload_decodes_to_error_not_panic() {
        let postings = vec![entry(8, 2, 30), entry(2, 3, 20), entry(5, 1, 10)];
        let encoded = TermPostingsEncoder::from_postings(postings).encode_to_bytes();

        // ids_encoding byte is at payload offset 12 (count:4 + base_id:8).
        let bad_encoding = corrupt_postings_payload(&encoded, &[(12, 0xEE)]);
        let view = TermPostingsView::parse(bad_encoding).expect("framing is intact");
        let err = view.decode_block(0).unwrap_err();
        assert!(
            err.message.contains("unknown postings ids_encoding"),
            "{}",
            err.message
        );
        // The row iterator surfaces the same error once, then ends.
        let items: Vec<_> = view.iter_entries().collect();
        assert_eq!(items.len(), 1);
        assert!(items[0].is_err());

        // count is payload bytes 0..4; 300 (0x012C) > BLOCK_LEN (256).
        let bad_count = corrupt_postings_payload(&encoded, &[(0, 0x2C), (1, 0x01)]);
        let view = TermPostingsView::parse(bad_count).expect("framing is intact");
        let err = view.decode_block(0).unwrap_err();
        assert!(err.message.contains("exceeds BLOCK_LEN"), "{}", err.message);
    }

    /// A degenerate count==0 postings block must be skipped by the view,
    /// not turned into an error.
    #[test]
    fn view_should_skip_zero_count_blocks() {
        // given - a valid pair followed by an empty (skip, postings) pair
        let mut buf = BytesMut::new();
        let real = TermPostingsEncoder::from_postings(vec![entry(7, 1, 1), entry(3, 2, 2)]);
        buf.extend_from_slice(&real.encode_to_bytes());
        // skip block for the empty pair
        let mut skip_payload = BytesMut::new();
        skip_payload.put_u64_le(0); // last_id
        skip_payload.put_u16_le(0); // impacts_count
        skip_payload.put_u64_le(4); // length of the empty postings payload
        buf.put_u8(BLOCK_TYPE_SKIP);
        buf.put_u32_le(skip_payload.len() as u32);
        buf.extend_from_slice(&skip_payload);
        // empty postings block: count == 0
        buf.put_u8(BLOCK_TYPE_POSTINGS);
        buf.put_u32_le(4);
        buf.put_u32_le(0);
        let encoded = buf.freeze();

        // when
        let view = TermPostingsView::parse(encoded).unwrap();

        // then - the view skips the empty pair and indexes only the real one
        assert_eq!(view.blocks().len(), 1);
        assert_eq!(view.decode_block(0).unwrap().ids, vec![3, 7]);
    }

    #[test]
    fn should_roundtrip_empty_value() {
        // given
        let value = TermPostingsEncoder::default();

        // when
        let encoded = value.encode_to_bytes();
        let view = TermPostingsView::parse(encoded).unwrap();

        // then
        assert!(view.blocks().is_empty());
    }

    #[test]
    fn should_roundtrip_single_block() {
        // given
        let value = TermPostingsEncoder::from_postings(vec![
            entry(5, 1, 10),
            entry(2, 3, 20),
            entry(8, 2, 30),
        ]);

        // then - one (Skip, Postings) pair in the builder
        assert_eq!(value.blocks.len(), 2);
        assert!(matches!(value.blocks[0], TermPostingsBlock::Skip(_)));
        assert!(matches!(value.blocks[1], TermPostingsBlock::Postings(_)));
        // Entries ascending by id with original freq/norm preserved.
        let entries = all_entries(value.encode_to_bytes());
        assert_eq!(
            entries,
            vec![entry(2, 3, 20), entry(5, 1, 10), entry(8, 2, 30)]
        );
    }

    #[test]
    fn should_chunk_postings_into_blocks_of_target_size() {
        // given - 2.5x target so we get 3 (Skip,Postings) pairs
        let n = TARGET_POSTINGS_PER_BLOCK * 2 + TARGET_POSTINGS_PER_BLOCK / 2;
        let postings: Vec<_> = (0..n as u64)
            .map(|i| entry(i, (i % 7) as u32 + 1, ((i % 11) as u8) + 1))
            .collect();
        let value = TermPostingsEncoder::from_postings(postings);

        // when
        let entries = all_entries(value.encode_to_bytes());

        // then - 3 pairs of (Skip, Postings)
        assert_eq!(value.blocks.len(), 6);
        assert_eq!(entries.len(), n);
        // Strictly ascending
        for w in entries.windows(2) {
            assert!(w[0].id < w[1].id);
        }
    }

    #[test]
    fn should_emit_skip_block_for_every_postings_block() {
        // given - enough postings for 2 blocks
        let n = TARGET_POSTINGS_PER_BLOCK + 5;
        let postings: Vec<_> = (0..n as u64).map(|i| entry(i, 1, 1)).collect();
        let value = TermPostingsEncoder::from_postings(postings);

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
    fn should_merge_appending_newer_blocks_after_older() {
        // given - older operand has lower ids, newer operand has higher ids
        let older = TermPostingsEncoder::from_postings(vec![entry(1, 1, 1), entry(2, 1, 1)])
            .encode_to_bytes();
        let newer = TermPostingsEncoder::from_postings(vec![entry(10, 1, 1), entry(11, 1, 1)])
            .encode_to_bytes();

        // when - operands ordered oldest -> newest
        let merged = merge_batch_term_postings(None, &[older, newer]);

        // then - ids are globally ascending
        let ids: Vec<u64> = all_entries(merged).iter().map(|e| e.id.id()).collect();
        assert_eq!(ids, vec![1, 2, 10, 11]);
    }

    #[test]
    fn should_merge_with_existing_value() {
        // given
        let existing = TermPostingsEncoder::from_postings(vec![entry(1, 1, 1)]).encode_to_bytes();
        let op = TermPostingsEncoder::from_postings(vec![entry(5, 1, 1)]).encode_to_bytes();

        // when
        let merged = merge_batch_term_postings(Some(existing), &[op]);

        // then - ids globally ascending
        let ids: Vec<u64> = all_entries(merged).iter().map(|e| e.id.id()).collect();
        assert_eq!(ids, vec![1, 5]);
    }

    #[test]
    fn merge_is_pure_byte_concatenation() {
        // given - the encoded forms of two operands
        let older = TermPostingsEncoder::from_postings(vec![entry(1, 1, 1)]).encode_to_bytes();
        let newer = TermPostingsEncoder::from_postings(vec![entry(10, 1, 1)]).encode_to_bytes();
        let mut expected = Vec::with_capacity(older.len() + newer.len());
        expected.extend_from_slice(&older); // delivery order: oldest first
        expected.extend_from_slice(&newer);

        // when
        let merged = merge_batch_term_postings(None, &[older, newer]);

        // then
        assert_eq!(merged.as_ref(), expected.as_slice());
    }

    #[test]
    fn should_preserve_skip_last_id_and_length() {
        // given
        let value = TermPostingsEncoder::from_postings(vec![
            entry(10, 1, 1),
            entry(20, 1, 1),
            entry(30, 1, 1),
        ]);

        // when
        let skip = value
            .blocks
            .iter()
            .find_map(|b| match b {
                TermPostingsBlock::Skip(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap();

        // then - last_id is the highest id in the following postings block;
        // length matches
        assert_eq!(skip.last_id.id(), 30);
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
    fn should_use_bitpacked_id_encoding_when_range_fits_u32() {
        // given
        let value = TermPostingsEncoder::from_postings(vec![
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

        // and - encode/decode round-trips identical ids, ascending
        let ids: Vec<u64> = all_entries(value.encode_to_bytes())
            .iter()
            .map(|e| e.id.id())
            .collect();
        assert_eq!(ids, vec![1, 50, 100]);
    }

    #[test]
    fn should_switch_to_varint_id_encoding_when_range_exceeds_u32() {
        // given - the block's id range exceeds what bitpacked offsets hold
        let huge = (u32::MAX as u64) + 2;
        let value = TermPostingsEncoder::from_postings(vec![entry(huge, 1, 5), entry(0, 2, 7)]);

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
        let entries = all_entries(value.encode_to_bytes());
        assert_eq!(entries, vec![entry(0, 2, 7), entry(huge, 1, 5)]);
    }

    /// The bitpacked condition is per-gap: the block's total id range can
    /// exceed `u32::MAX` as long as every individual gap fits (the prefix
    /// sum accumulates in u64).
    #[test]
    fn should_stay_bitpacked_when_gaps_fit_but_range_does_not() {
        let step = 3_000_000_000u64; // < u32::MAX, but two steps exceed it
        let value = TermPostingsEncoder::from_postings(vec![
            entry(0, 1, 1),
            entry(step, 1, 1),
            entry(2 * step, 1, 1),
        ]);

        let TermPostingsBlock::Postings(p) = &value.blocks[1] else {
            panic!("expected postings block")
        };
        let mut payload = BytesMut::new();
        p.encode_payload(&mut payload);
        assert_eq!(payload[12], IDS_ENCODING_BITPACKED);

        let ids: Vec<u64> = all_entries(value.encode_to_bytes())
            .iter()
            .map(|e| e.id.id())
            .collect();
        assert_eq!(ids, vec![0, step, 2 * step]);
    }

    /// Single-entry blocks exercise the all-padding path of the
    /// strictly-sorted codec.
    #[test]
    fn should_roundtrip_single_entry_block() {
        let value = TermPostingsEncoder::from_postings(vec![entry(12345, 7, 3)]);
        let entries = all_entries(value.encode_to_bytes());
        assert_eq!(entries, vec![entry(12345, 7, 3)]);
    }

    /// Dense consecutive ids pack to one-bit gaps.
    #[test]
    fn consecutive_ids_pack_to_one_bit() {
        let postings: Vec<_> = (1000..1256u64).map(|i| entry(i, 1, 1)).collect();
        let value = TermPostingsEncoder::from_postings(postings.clone());

        let TermPostingsBlock::Postings(p) = &value.blocks[1] else {
            panic!("expected postings block")
        };
        let mut payload = BytesMut::new();
        p.encode_payload(&mut payload);
        // count(4) + base_id(8) + encoding(1), then ids_bits.
        assert_eq!(payload[12], IDS_ENCODING_BITPACKED);
        assert_eq!(payload[13], 1, "gaps of 1 need 1 bit");

        assert_eq!(all_entries(value.encode_to_bytes()), postings);
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
    fn should_compute_dominating_impacts_per_rfc_example() {
        // given - the RFC-0006 example impact set
        let entries: Vec<PostingEntry> = [(10, 5), (5, 15), (11, 7), (12, 3), (13, 8), (11, 1)]
            .iter()
            .enumerate()
            .map(|(i, &(freq, norm))| entry(i as u64, freq, norm))
            .collect();

        // when
        let impacts = compute_dominating_impacts(&entries);

        // then - the dominating pairs from the RFC, sorted by ascending norm
        assert_eq!(
            impacts,
            vec![
                Impact { freq: 11, norm: 1 },
                Impact { freq: 12, norm: 3 },
                Impact { freq: 13, norm: 8 },
            ]
        );
    }

    #[test]
    fn dominating_impacts_should_bound_every_entry() {
        // given - a pseudo-random block
        let entries: Vec<PostingEntry> = (0..256u64)
            .map(|i| {
                entry(
                    i,
                    ((i * 2654435761) % 37) as u32 + 1,
                    ((i * 40503) % 251) as u8,
                )
            })
            .collect();

        // when
        let impacts = compute_dominating_impacts(&entries);

        // then - every entry is dominated by (or equal to) some impact
        for e in &entries {
            assert!(
                impacts
                    .iter()
                    .any(|imp| imp.freq >= e.freq && imp.norm <= e.norm),
                "entry (freq {}, norm {}) not covered by impacts {:?}",
                e.freq,
                e.norm,
                impacts
            );
        }
        // and - the frontier is strictly increasing in both norm and freq
        for w in impacts.windows(2) {
            assert!(w[0].norm < w[1].norm);
            assert!(w[0].freq < w[1].freq);
        }
    }

    #[test]
    fn should_roundtrip_skip_impacts() {
        // given
        let value = TermPostingsEncoder::from_postings(vec![
            entry(10, 3, 7),
            entry(20, 1, 2),
            entry(30, 5, 9),
        ]);

        // when
        let view = TermPostingsView::parse(value.encode_to_bytes()).unwrap();

        // then - skip block impacts survive the round trip
        assert_eq!(
            view.impacts(0),
            &[
                Impact { freq: 1, norm: 2 },
                Impact { freq: 3, norm: 7 },
                Impact { freq: 5, norm: 9 }
            ]
        );
    }

    /// A pair whose skip block carries no impacts parses with an empty
    /// impact list (the "unknown — use the global bound" signal).
    #[test]
    fn should_parse_skip_block_without_impacts() {
        // given - a hand-built pair with impacts stripped from the skip
        let postings = PostingsBlock::from_ascending(vec![entry(7, 1, 1), entry(42, 2, 2)]);
        let mut payload = BytesMut::new();
        postings.encode_payload(&mut payload);
        let value = TermPostingsEncoder {
            blocks: vec![
                TermPostingsBlock::Skip(SkipBlock {
                    last_id: postings.last_id(),
                    impacts: Vec::new(),
                    length: payload.len() as u64,
                }),
                TermPostingsBlock::Postings(postings),
            ],
        };

        // when
        let view = TermPostingsView::parse(value.encode_to_bytes()).unwrap();

        // then
        assert_eq!(view.blocks().len(), 1);
        assert_eq!(view.blocks()[0].min_id, 7);
        assert_eq!(view.blocks()[0].max_id, 42);
        assert!(view.impacts(0).is_empty());
    }

    #[test]
    fn should_roundtrip_full_block_of_256_entries() {
        // given - exactly TARGET_POSTINGS_PER_BLOCK postings
        let postings: Vec<_> = (0..TARGET_POSTINGS_PER_BLOCK as u64)
            .map(|i| entry(i, (i as u32 % 5) + 1, ((i % 200) as u8) + 1))
            .collect();
        let value = TermPostingsEncoder::from_postings(postings.clone());

        // when
        let entries = all_entries(value.encode_to_bytes());

        // then - decoded order is the ascending input order
        assert_eq!(entries, postings);
    }

    #[test]
    fn view_should_index_blocks_in_ascending_order() {
        // given - 2.5 blocks worth of postings
        let n = TARGET_POSTINGS_PER_BLOCK * 2 + TARGET_POSTINGS_PER_BLOCK / 2;
        let postings: Vec<_> = (0..n as u64)
            .map(|i| entry(i, (i % 7) as u32 + 1, ((i % 11) as u8) + 1))
            .collect();
        let encoded = TermPostingsEncoder::from_postings(postings).encode_to_bytes();

        // when
        let view = TermPostingsView::parse(encoded).unwrap();

        // then - three blocks, ascending and non-overlapping, counts sum to n
        assert_eq!(view.blocks().len(), 3);
        let total: usize = (0..view.blocks().len())
            .map(|i| view.decode_block(i).unwrap().ids.len())
            .sum();
        assert_eq!(total, n);
        for w in view.blocks().windows(2) {
            assert!(w[0].max_id < w[1].min_id);
        }
        for (idx, block) in view.blocks().iter().enumerate() {
            assert!(block.min_id <= block.max_id);
            assert!(!view.impacts(idx).is_empty());
        }
    }

    #[test]
    fn view_decode_block_should_return_ascending_arrays() {
        // given
        let postings = vec![entry(8, 2, 30), entry(2, 3, 20), entry(5, 1, 10)];
        let encoded = TermPostingsEncoder::from_postings(postings).encode_to_bytes();

        // when
        let view = TermPostingsView::parse(encoded).unwrap();
        let decoded = view.decode_block(0).unwrap();

        // then
        assert_eq!(decoded.ids, vec![2, 5, 8]);
        assert_eq!(decoded.freqs, vec![3, 1, 2]);
        assert_eq!(decoded.norms, vec![20, 10, 30]);
    }

    #[test]
    fn view_should_parse_merged_operands() {
        // given - two merge operands (older = lower ids)
        let older = TermPostingsEncoder::from_postings(
            (0..300u64).map(|i| entry(i, 1, 1)).collect::<Vec<_>>(),
        )
        .encode_to_bytes();
        let newer = TermPostingsEncoder::from_postings(
            (1000..1100u64).map(|i| entry(i, 2, 2)).collect::<Vec<_>>(),
        )
        .encode_to_bytes();
        let merged = merge_batch_term_postings(None, &[older, newer]);

        // when
        let view = TermPostingsView::parse(merged).unwrap();

        // then - blocks ascending across operand boundaries
        let mut prev_max = None;
        for block in view.blocks() {
            if let Some(prev) = prev_max {
                assert!(block.min_id > prev);
            }
            prev_max = Some(block.max_id);
        }
        // and - every posting is reachable, in ascending order overall
        let mut all_ids = Vec::new();
        for idx in 0..view.blocks().len() {
            all_ids.extend(view.decode_block(idx).unwrap().ids);
        }
        let expected: Vec<u64> = (0..300).chain(1000..1100).collect();
        assert_eq!(all_ids, expected);
    }

    #[test]
    fn view_of_empty_value_has_no_blocks() {
        let view = TermPostingsView::parse(Bytes::new()).unwrap();
        assert!(view.blocks().is_empty());
    }

    /// `iter_entries` walks every block lazily and yields the ascending row
    /// stream the compaction filter and exhaustive scorer consume.
    #[test]
    fn iter_entries_yields_all_postings_ascending() {
        let n = TARGET_POSTINGS_PER_BLOCK + 17;
        let postings: Vec<_> = (0..n as u64)
            .map(|i| entry(i * 3, (i % 9) as u32 + 1, (i % 50) as u8 + 1))
            .collect();
        let encoded = TermPostingsEncoder::from_postings(postings.clone()).encode_to_bytes();

        let entries = all_entries(encoded);

        assert_eq!(entries, postings);
    }

    /// The pair byte ranges recorded by parse() tile the raw value exactly,
    /// so verbatim pair copies reconstruct it byte-for-byte.
    #[test]
    fn pair_bytes_tile_the_encoded_value() {
        let n = TARGET_POSTINGS_PER_BLOCK * 2 + 9;
        let postings: Vec<_> = (0..n as u64).map(|i| entry(i, 1, 1)).collect();
        let encoded = TermPostingsEncoder::from_postings(postings).encode_to_bytes();

        let view = TermPostingsView::parse(encoded.clone()).unwrap();
        let mut rebuilt = BytesMut::new();
        for idx in 0..view.blocks().len() {
            rebuilt.extend_from_slice(view.pair_bytes(idx));
        }

        assert_eq!(rebuilt.freeze(), encoded);
    }

    /// Corrupt bit-width bytes must error, not panic inside the bitpacker
    /// (its `num_bits <= 32` assert is active in release builds, and a panic
    /// here would crash-loop the compactor on the same corrupt SST).
    #[test]
    fn corrupt_bit_widths_decode_to_error_not_panic() {
        // A full block with wide freqs gives the corrupted ids_bytes claim
        // room to pass the payload-length check.
        let postings: Vec<_> = (0..BLOCK_LEN as u64)
            .map(|i| entry(1000 + i, u32::MAX, 1))
            .collect();
        let encoded = TermPostingsEncoder::from_postings(postings).encode_to_bytes();

        // ids_bits is at payload offset 13 (count:4 + base_id:8 + encoding:1).
        let bad_ids_bits = corrupt_postings_payload(&encoded, &[(13, 33)]);
        let view = TermPostingsView::parse(bad_ids_bits).expect("framing is intact");
        let err = view.decode_block(0).unwrap_err();
        assert!(err.message.contains("ids_bits"), "{}", err.message);

        // freqs_bits sits after the packed ids section, whose width depends
        // on the encoded ids_bits — read it back rather than hardcoding.
        let ids_bits = {
            // Skip block first: its header is 5 bytes + payload.
            let skip_len =
                u32::from_le_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]) as usize;
            let payload_start = 5 + skip_len + 5;
            encoded[payload_start + 13] as usize
        };
        let freqs_bits_offset = 14 + ids_bits * (BLOCK_LEN / 8);
        let bad_freqs_bits = corrupt_postings_payload(&encoded, &[(freqs_bits_offset, 40)]);
        let view = TermPostingsView::parse(bad_freqs_bits).expect("framing is intact");
        let err = view.decode_block(0).unwrap_err();
        assert!(err.message.contains("freqs_bits"), "{}", err.message);
    }

    /// Inconsistent directory metadata (inverted or non-ascending pairs) is
    /// rejected at parse time rather than trusted by consumers — the
    /// compaction filter's verbatim-copy probe would otherwise silently
    /// resurrect deleted postings.
    #[test]
    fn parse_rejects_inconsistent_pair_metadata() {
        // Inverted: corrupt the skip's last_id below the postings base_id.
        let encoded = TermPostingsEncoder::from_postings(vec![
            entry(10, 1, 1),
            entry(42, 1, 1),
            entry(100, 1, 1),
        ])
        .encode_to_bytes();
        let mut bytes = encoded.to_vec();
        // Skip block payload starts at offset 5; last_id is its first 8 bytes.
        bytes[5..13].copy_from_slice(&5u64.to_le_bytes());
        let err = TermPostingsView::parse(Bytes::from(bytes)).unwrap_err();
        assert!(
            err.message.contains("exceeds skip block"),
            "{}",
            err.message
        );

        // Non-ascending: two pairs whose ranges do not ascend.
        let pair_a = TermPostingsEncoder::from_postings(vec![entry(50, 1, 1)]).encode_to_bytes();
        let pair_b = TermPostingsEncoder::from_postings(vec![entry(20, 1, 1)]).encode_to_bytes();
        let mut merged = BytesMut::new();
        merged.extend_from_slice(&pair_a);
        merged.extend_from_slice(&pair_b);
        let err = TermPostingsView::parse(merged.freeze()).unwrap_err();
        assert!(err.message.contains("does not ascend"), "{}", err.message);
    }

    /// Both sides of the codec-selection seam: the boundary is the largest
    /// single gap, `u32::MAX`.
    #[test]
    fn codec_selection_boundary() {
        let ids_encoding = |value: &TermPostingsEncoder| {
            let TermPostingsBlock::Postings(p) = &value.blocks[1] else {
                panic!("expected postings block")
            };
            let mut payload = BytesMut::new();
            p.encode_payload(&mut payload);
            payload[12]
        };

        // gap == u32::MAX: bitpacked, round-trips.
        let at =
            TermPostingsEncoder::from_postings(vec![entry(0, 1, 1), entry(u32::MAX as u64, 1, 1)]);
        assert_eq!(ids_encoding(&at), IDS_ENCODING_BITPACKED);
        let ids: Vec<u64> = all_entries(at.encode_to_bytes())
            .iter()
            .map(|e| e.id.id())
            .collect();
        assert_eq!(ids, vec![0, u32::MAX as u64]);

        // gap == u32::MAX + 1: varint, round-trips.
        let past = TermPostingsEncoder::from_postings(vec![
            entry(0, 1, 1),
            entry(u32::MAX as u64 + 1, 1, 1),
        ]);
        assert_eq!(ids_encoding(&past), IDS_ENCODING_VARINT);
        let ids: Vec<u64> = all_entries(past.encode_to_bytes())
            .iter()
            .map(|e| e.id.id())
            .collect();
        assert_eq!(ids, vec![0, u32::MAX as u64 + 1]);

        // Full block with one boundary gap: zero padding never widens bits.
        let mut postings: Vec<_> = (0..(BLOCK_LEN as u64 - 1))
            .map(|i| entry(i, 1, 1))
            .collect();
        postings.push(entry(BLOCK_LEN as u64 - 2 + u32::MAX as u64, 1, 1));
        let full = TermPostingsEncoder::from_postings(postings.clone());
        assert_eq!(ids_encoding(&full), IDS_ENCODING_BITPACKED);
        assert_eq!(all_entries(full.encode_to_bytes()), postings);
    }

    /// Mid-stream corruption: the iterator yields every entry of the good
    /// blocks, then exactly one Err, then ends.
    #[test]
    fn iter_entries_yields_good_blocks_then_one_error() {
        let n = TARGET_POSTINGS_PER_BLOCK + 5;
        let postings: Vec<_> = (0..n as u64).map(|i| entry(i, 1, 1)).collect();
        let encoded = TermPostingsEncoder::from_postings(postings.clone()).encode_to_bytes();
        // Corrupt the SECOND postings payload's ids_encoding byte.
        let corrupted = corrupt_nth_postings_payload(&encoded, 1, &[(12, 0xEE)]);

        let view = TermPostingsView::parse(corrupted).expect("framing is intact");
        let mut iter = view.iter_entries();
        for expected in postings.iter().take(TARGET_POSTINGS_PER_BLOCK) {
            assert_eq!(iter.next().unwrap().unwrap(), *expected);
        }
        assert!(iter.next().unwrap().is_err());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }
}
