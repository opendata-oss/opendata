//! Tokenization for full-text search (RFC-0006).
//!
//! Wraps the ICU4X word segmenter so the write path (when building term
//! frequencies for `TextAttributeSummary`) and the query path (when expanding
//! a BM25 query string into terms) tokenize identically.

use icu_segmenter::options::WordBreakInvariantOptions;
use icu_segmenter::{WordSegmenter, WordSegmenterBorrowed};

thread_local! {
    /// Constructing a `WordSegmenter` sets up segmentation models and is too
    /// expensive to do per call on the write path; cache one per thread.
    static SEGMENTER: WordSegmenterBorrowed<'static> =
        WordSegmenter::new_auto(WordBreakInvariantOptions::default());
}

/// Tokenize a UTF-8 string into Unicode-aware, lowercase word terms.
///
/// Uses ICU4X word segmentation to split the input on Unicode word
/// boundaries, drops segments classified as `WordType::None` (whitespace and
/// punctuation), and lowercases each remaining segment with the full Unicode
/// lowering rules. Empty segments are skipped.
///
/// The same function runs at write and query time so a term written under
/// one byte representation will collide on bytewise equality with the
/// corresponding term emitted by a query.
pub(crate) fn tokenize(input: &str) -> Vec<String> {
    if input.is_empty() {
        return Vec::new();
    }
    SEGMENTER.with(|segmenter| {
        let mut tokens = Vec::new();
        let mut iter = segmenter.segment_str(input).iter_with_word_type();
        // The first emitted boundary is the start of input (always 0); we only
        // care about segments lying _between_ consecutive boundaries.
        let Some((mut prev, _)) = iter.next() else {
            return Vec::new();
        };
        for (boundary, word_type) in iter {
            if word_type.is_word_like() {
                let segment = &input[prev..boundary];
                if !segment.is_empty() {
                    tokens.push(segment.to_lowercase());
                }
            }
            prev = boundary;
        }
        tokens
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_split_simple_words() {
        // given
        let input = "the quick brown fox";

        // when
        let tokens = tokenize(input);

        // then
        assert_eq!(tokens, vec!["the", "quick", "brown", "fox"]);
    }

    #[test]
    fn should_lowercase_ascii_input() {
        // given
        let input = "Hello WORLD";

        // when
        let tokens = tokenize(input);

        // then
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn should_drop_punctuation_segments() {
        // given
        let input = "hello, world!";

        // when
        let tokens = tokenize(input);

        // then - punctuation segments classified non-word-like are dropped
        assert_eq!(tokens, vec!["hello", "world"]);
    }

    #[test]
    fn should_handle_empty_input() {
        // given
        let input = "";

        // when
        let tokens = tokenize(input);

        // then
        assert!(tokens.is_empty());
    }

    #[test]
    fn should_be_deterministic() {
        // given
        let input = "foo bar foo";

        // when
        let a = tokenize(input);
        let b = tokenize(input);

        // then
        assert_eq!(a, b);
    }

    #[test]
    fn should_treat_numbers_as_word_like() {
        // given
        let input = "build 42 ok";

        // when
        let tokens = tokenize(input);

        // then
        assert_eq!(tokens, vec!["build", "42", "ok"]);
    }

    #[test]
    fn should_lowercase_unicode_input() {
        // given
        let input = "ÄPFEL";

        // when
        let tokens = tokenize(input);

        // then
        assert_eq!(tokens, vec!["äpfel"]);
    }
}
