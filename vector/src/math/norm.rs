//! Lucene-style length norm quantization (RFC-0006).
//!
//! Wraps Lucene's `SmallFloat.intToByte4` / `byte4ToInt`
//! (<https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/SmallFloat.java>)
//! to compress a non-negative document length into one byte using a
//! 5-bit-exponent, 3-bit-mantissa floating-point representation.
//!
//! ## Structure
//!
//! [`encode_length`] asserts inputs use at most [`MAX_INPUT_BITS`] bits, so
//! the natural Lucene FP algorithm never reaches the top of the byte range.
//! We compute [`MAX_CODE`] — the highest byte the unmodified
//! algorithm would emit for any input in our range — and use the resulting
//! [`CODE_SHIFT`] of unused byte codes to extend the lossless prefix at the
//! bottom of the byte range. For 30-bit inputs and a 3-bit mantissa we get
//! 32 free codes, which grows the lossless range from `[0, 15]` to
//! `[0, 63]`.

/// Maximum number of bits inputs to [`encode_length`] may use.
pub(crate) const MAX_INPUT_BITS: u32 = 30;

/// The number of significant bits to retain
const MANTISSA_BITS: u32 = 4;
/// The encoding omits the most significant bit as it is implicitly 1
const MANTISSA_CODE_BITS: u32 = MANTISSA_BITS - 1;
const MANTISSA_MASK: u32 = (1 << MANTISSA_CODE_BITS) - 1; // 0b111

/// Highest byte code the natural Lucene FP algorithm produces for any input
/// fitting in [`MAX_INPUT_BITS`].
///
/// For an input `i` with `nsb` significant bits the natural algorithm emits
/// `((nsb - MANTISSA_BITS + 1) << MANTISSA_CODE_BITS) | low`. The max-bits
/// input has `nsb = MAX_INPUT_BITS` and `low = MANTISSA_MASK`, which folds
/// to `((MAX_INPUT_BITS - MANTISSA_BITS + 1) << MANTISSA_CODE_BITS) |
/// MANTISSA_MASK` — i.e. the `MAX_INPUT_BITS - 3` you see below for our
/// 4-bit mantissa.
const MAX_CODE: u32 = ((MAX_INPUT_BITS - 3) << MANTISSA_CODE_BITS) | MANTISSA_MASK;

/// Byte codes at the top of `[0, 255]` that the natural algorithm never
/// produces for inputs in our range. We shift the FP encoding up by this
/// many bytes so those codes fall off the top, freeing up the same number
/// of byte codes at the bottom for lossless direct encoding.
const CODE_SHIFT: u32 = 255 - MAX_CODE;

/// Inputs strictly less than this are stored directly (`byte == i`).
///
/// Chosen as the smallest power of two `N` where
/// `natural_code(N) + SHIFT == N`, so the boundary between the direct and
/// shifted-FP byte ranges has no unused codes between them. For
/// `MAX_INPUT_BITS = 30`, this works out to `64`.
const LOSSLESS_THRESHOLD: u32 = 64;

// Compile-time consistency checks for the constants above.
const _: () = {
    assert!(MAX_CODE + CODE_SHIFT == 255);
    // Mirror `encode_length`: nsb = number of significant bits of the input.
    let nsb = LOSSLESS_THRESHOLD.trailing_zeros() + 1;
    let shift = nsb - MANTISSA_BITS;
    let natural = (shift + 1) << MANTISSA_CODE_BITS;
    assert!(natural + CODE_SHIFT == LOSSLESS_THRESHOLD);
};

/// Encode a non-negative document length into one byte.
///
/// Lossless for inputs `< LOSSLESS_THRESHOLD` (= 64). Larger inputs are
/// mapped via the shifted Lucene FP encoding with roughly 3 bits of
/// mantissa precision.
///
/// # Panics
/// Panics when `i` requires more than [`MAX_INPUT_BITS`] bits to represent.
pub(crate) fn encode_length(i: u32) -> u8 {
    assert!(
        i < 1u32 << MAX_INPUT_BITS,
        "encode_length: input {} exceeds {} bits",
        i,
        MAX_INPUT_BITS
    );
    if i < LOSSLESS_THRESHOLD {
        return i as u8;
    }
    // the number of significant bits in the value
    let nsb = 32u32 - i.leading_zeros();
    let low = (i >> (nsb - MANTISSA_BITS)) & MANTISSA_MASK;
    let shift = nsb - MANTISSA_BITS;
    let natural = ((shift + 1) << MANTISSA_CODE_BITS) | low;
    (natural + CODE_SHIFT) as u8
}

/// Decode a byte produced by [`encode_length`] back to its (approximate)
/// integer value.
///
/// Exact for bytes `< LOSSLESS_THRESHOLD`; for larger bytes returns the
/// smallest length representable by that byte.
pub(crate) fn decode_norm(b: u8) -> u32 {
    let b = b as u32;
    if b < LOSSLESS_THRESHOLD {
        return b;
    }
    let natural = b - CODE_SHIFT;
    let biased_shift = natural >> MANTISSA_CODE_BITS;
    // biased_shift >= 4 here: b >= LOSSLESS_THRESHOLD == 64 and CODE_SHIFT
    // == 32 imply natural >= 32, so natural >> MANTISSA_CODE_BITS >= 4.
    ((1 << MANTISSA_CODE_BITS) | (natural & MANTISSA_MASK)) << (biased_shift - 1)
}

/// Clamp `length` to the largest value [`encode_length`] accepts, so callers
/// don't have to special-case oversized documents.
pub(crate) fn clamp_length(length: usize) -> u32 {
    let max = (1u32 << MAX_INPUT_BITS) - 1;
    length.min(max as usize) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_lossless_below_threshold() {
        for i in 0u32..LOSSLESS_THRESHOLD {
            // when
            let b = encode_length(i);
            let back = decode_norm(b);

            // then
            assert_eq!(b as u32, i);
            assert_eq!(back, i);
        }
    }

    #[test]
    fn encode_is_monotonic_non_decreasing() {
        // given - sweep across a representative range
        let mut prev = encode_length(0);
        for i in 1u32..=1_000_000 {
            // when
            let cur = encode_length(i);

            // then
            assert!(cur >= prev, "non-monotonic at {}: {} < {}", i, cur, prev);
            prev = cur;
        }
    }

    #[test]
    fn encode_reaches_byte_255_at_max_input() {
        // given - largest valid input
        let max = (1u32 << MAX_INPUT_BITS) - 1;

        // when
        let b = encode_length(max);

        // then - the byte space is fully used; no codes wasted at the top
        assert_eq!(b, 255);
    }

    #[test]
    fn every_byte_is_a_valid_codepoint() {
        // given/when - encode(decode(b)) == b for every byte (no gaps)
        for b in 0u32..=255 {
            let decoded = decode_norm(b as u8);
            let re_encoded = encode_length(decoded);
            assert_eq!(
                re_encoded, b as u8,
                "encode(decode({})) = {} but expected {}",
                b, re_encoded, b
            );
        }
    }

    #[test]
    fn decoded_norm_approximates_input_for_large_values() {
        // given/when - decode never overshoots; mantissa-bit precision limits error
        for i in [64u32, 100, 128, 1024, 1_000_000, (1 << MAX_INPUT_BITS) - 1] {
            let b = encode_length(i);
            let back = decode_norm(b);

            // then
            assert!(back <= i, "decode({}) = {} > {}", i, back, i);
            // 3-bit mantissa => relative error <= 1/8 in the worst case
            let max_err = i / 8 + 1;
            assert!(
                i - back <= max_err,
                "decode error too large for {}: got {} (err {})",
                i,
                back,
                i - back
            );
        }
    }

    #[test]
    #[should_panic(expected = "exceeds")]
    fn encode_panics_when_input_exceeds_max_bits() {
        // when
        let _ = encode_length(1u32 << MAX_INPUT_BITS);
    }

    #[test]
    fn clamp_length_caps_at_max_input() {
        // given/when/then - values within range pass through
        assert_eq!(clamp_length(0), 0);
        assert_eq!(clamp_length(42), 42);
        assert_eq!(
            clamp_length((1usize << MAX_INPUT_BITS) - 1),
            (1u32 << MAX_INPUT_BITS) - 1
        );
        // and values above the cap are clamped to the largest valid input
        assert_eq!(clamp_length(usize::MAX), (1u32 << MAX_INPUT_BITS) - 1);
    }
}
