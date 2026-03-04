// TimeSeries value structure with Gorilla compression using tsz crate

use crate::model::Sample;

use super::*;
use bytes::Bytes;
use tsz::stream::{BufferedWriter, Error as TszError, Read as TszRead};
use tsz::{Bit, DataPoint, Decode, Encode, StdDecoder, StdEncoder};

/// A reader that implements `tsz::stream::Read` for byte slices without copying.
struct BytesReader<'a> {
    bytes: &'a [u8],
    byte_pos: usize,
    bit_pos: u8, // 0-7, position within current byte
}

impl<'a> BytesReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            byte_pos: 0,
            bit_pos: 0,
        }
    }
}

impl<'a> TszRead for BytesReader<'a> {
    fn read_bit(&mut self) -> std::result::Result<Bit, TszError> {
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }

        if self.byte_pos >= self.bytes.len() {
            return Err(TszError::EOF);
        }

        let byte = self.bytes[self.byte_pos];
        let bit = if byte & 1u8.wrapping_shl(7 - self.bit_pos as u32) == 0 {
            Bit::Zero
        } else {
            Bit::One
        };

        self.bit_pos += 1;

        Ok(bit)
    }

    fn read_byte(&mut self) -> std::result::Result<u8, TszError> {
        // When bit_pos == 0, we're byte-aligned
        if self.bit_pos == 0 {
            if self.byte_pos >= self.bytes.len() {
                return Err(TszError::EOF);
            }
            let byte = self.bytes[self.byte_pos];
            // Set bit_pos to 8 to mark we've consumed this byte
            // The next read operation will increment byte_pos
            self.bit_pos = 8;
            return Ok(byte);
        }

        // When bit_pos == 8, move to next byte
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            if self.byte_pos >= self.bytes.len() {
                return Err(TszError::EOF);
            }
            let byte = self.bytes[self.byte_pos];
            // Keep bit_pos at 8 since we've consumed this byte
            return Ok(byte);
        }

        // When bit_pos is between 1-7, we need to combine parts of two bytes
        if self.byte_pos >= self.bytes.len() {
            return Err(TszError::EOF);
        }

        let mut byte = 0;
        let mut b = self.bytes[self.byte_pos];
        byte |= b.wrapping_shl(self.bit_pos as u32);

        self.byte_pos += 1;
        if self.byte_pos >= self.bytes.len() {
            return Err(TszError::EOF);
        }

        b = self.bytes[self.byte_pos];
        byte |= b.wrapping_shr(8 - self.bit_pos as u32);

        Ok(byte)
    }

    fn read_bits(&mut self, mut num: u32) -> std::result::Result<u64, TszError> {
        if num > 64 {
            num = 64;
        }

        let mut bits: u64 = 0;
        while num >= 8 {
            let byte = self.read_byte().map(u64::from)?;
            bits = bits.wrapping_shl(8) | byte;
            num -= 8;
        }

        while num > 0 {
            self.read_bit()
                .map(|bit| bits = bits.wrapping_shl(1) | bit.to_u64())?;
            num -= 1;
        }

        Ok(bits)
    }

    fn peak_bits(&mut self, num: u32) -> std::result::Result<u64, TszError> {
        let saved_byte_pos = self.byte_pos;
        let saved_bit_pos = self.bit_pos;

        let bits = self.read_bits(num)?;

        self.byte_pos = saved_byte_pos;
        self.bit_pos = saved_bit_pos;

        Ok(bits)
    }
}

/// Iterator over time series samples from Gorilla-compressed data.
///
/// This iterator lazily decodes samples from the compressed format without
/// materializing the full series in memory.
pub(crate) struct TimeSeriesIterator<'a> {
    decoder: StdDecoder<BytesReader<'a>>,
}

impl<'a> TimeSeriesIterator<'a> {
    /// Creates a new iterator from compressed time series bytes.
    ///
    /// Returns None if the bytes represent an empty series.
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        let reader = BytesReader::new(bytes);
        let decoder = StdDecoder::new(reader);

        Some(TimeSeriesIterator { decoder })
    }
}

impl<'a> Iterator for TimeSeriesIterator<'a> {
    type Item = Result<Sample, EncodingError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.decoder.next() {
            Ok(dp) => Some(Ok(Sample {
                timestamp_ms: dp.get_time() as i64,
                value: dp.get_value(),
            })),
            Err(tsz::decode::Error::EndOfStream) => None,
            Err(e) => Some(Err(EncodingError {
                message: format!("Gorilla decoding failed: {}", e),
            })),
        }
    }
}

/// TimeSeries value: Gorilla-compressed stream of (timestamp_ms, value) pairs
#[derive(Debug, Clone, PartialEq)]
pub struct TimeSeriesValue {
    pub points: Vec<Sample>,
}

impl TimeSeriesValue {
    /// Encode time series points using Gorilla compression
    pub fn encode(&self) -> Result<Bytes, EncodingError> {
        // Handle empty case
        if self.points.is_empty() {
            return Ok(Bytes::new());
        }

        // Use Gorilla compression
        let w = BufferedWriter::new();
        let start_time = self.points[0].timestamp_ms as u64;
        let mut encoder = StdEncoder::new(start_time, w);

        for point in &self.points {
            let dp = DataPoint::new(point.timestamp_ms as u64, point.value);
            encoder.encode(dp);
        }

        let compressed = encoder.close();
        Ok(Bytes::from(compressed))
    }

    /// Decode time series points from Gorilla-compressed data
    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.is_empty() {
            return Ok(TimeSeriesValue { points: vec![] });
        }

        // Use the iterator to collect points
        let points = match TimeSeriesIterator::new(buf) {
            None => vec![], // Empty series
            Some(iter) => iter.collect::<Result<Vec<_>, _>>()?,
        };

        Ok(TimeSeriesValue { points })
    }
}

/// Merges a batch of compressed time series byte values into a single compressed value.
///
/// This function performs an efficient sorted merge of Gorilla-compressed time series
/// without fully deserializing them into memory. Samples are merged in timestamp order,
/// with duplicates resolved by keeping the value from the newest operand (last write wins).
///
/// This is designed for use in merge operators during compaction.
///
/// # Arguments
///
/// * `existing` - The existing compressed time series value (if any)
/// * `operands` - A slice of compressed time series operands, ordered oldest to newest
///
/// # Returns
///
/// A new compressed `Bytes` value containing the merged series
pub(crate) fn merge_batch_time_series(
    existing: Option<Bytes>,
    operands: &[Bytes],
) -> Result<Bytes, EncodingError> {
    let mut sources: Vec<&Bytes> = Vec::new();
    if let Some(ref existing) = existing
        && !existing.is_empty()
    {
        sources.push(existing);
    }
    for operand in operands {
        if !operand.is_empty() {
            sources.push(operand);
        }
    }

    // Handle edge cases
    if sources.is_empty() {
        return Ok(Bytes::new());
    }
    if sources.len() == 1 {
        return Ok(sources[0].clone());
    }

    // Decode all sources into (priority, timestamp, value) triples.
    // Priority is the source index: higher index = newer = wins on tie.
    let mut samples: Vec<(usize, i64, f64)> = Vec::new();
    for (priority, source) in sources.iter().enumerate() {
        let iter = TimeSeriesIterator::new(source.as_ref()).expect("Series should not be empty");
        for result in iter {
            let sample = result?;
            samples.push((priority, sample.timestamp_ms, sample.value));
        }
    }
    // If all iterators returned None immediately, treat as empty.
    if samples.is_empty() {
        return Ok(Bytes::new());
    }

    // Sort by timestamp ascending, then by priority descending (newest first)
    samples.sort_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)));
    // Dedup by timestamp, keeping the first (highest priority) entry (last write wins)
    samples.dedup_by(|a, b| a.1 == b.1);

    // Encode the merged result
    let writer = BufferedWriter::new();
    let start_time = samples[0].1 as u64;
    let mut encoder = StdEncoder::new(start_time, writer);

    for &(_, timestamp, value) in &samples {
        encoder.encode(DataPoint::new(timestamp as u64, value));
    }

    // Close encoder to get compressed data
    let compressed = encoder.close();

    // Convert directly to Bytes without copying
    Ok(Bytes::from(compressed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_time_series_value() {
        // given
        let value = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_empty_time_series_value() {
        // given
        let value = TimeSeriesValue { points: vec![] };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_single_point() {
        // given
        let value = TimeSeriesValue {
            points: vec![Sample {
                timestamp_ms: 1609459200,
                value: 42.5,
            }],
        };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_special_float_values() {
        // given
        let value = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: f64::INFINITY,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: f64::NEG_INFINITY,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 0.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: -0.0,
                },
            ],
        };

        // when
        let encoded = value.encode().unwrap();
        let decoded = TimeSeriesValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded.points.len(), 4);
        assert_eq!(decoded.points[0].value, f64::INFINITY);
        assert_eq!(decoded.points[1].value, f64::NEG_INFINITY);
        assert_eq!(decoded.points[2].value, 0.0);
        assert_eq!(decoded.points[3].value, -0.0);
    }

    #[test]
    fn should_merge_time_series_with_deduplication() {
        // given: two time series with overlapping timestamps
        let base = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        };
        let base_bytes = base.encode().unwrap();

        let other = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 200.0, // Should override base's 20.0
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 300.0, // Should override base's 30.0
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 40.0,
                },
            ],
        };
        let other_bytes = other.encode().unwrap();

        // when: merge the series
        let merged_bytes = merge_batch_time_series(Some(base_bytes), &[other_bytes]).unwrap();
        let merged = TimeSeriesValue::decode(merged_bytes.as_ref()).unwrap();

        // then: should have 4 points with duplicates resolved (last write wins)
        assert_eq!(merged.points.len(), 4);
        assert_eq!(merged.points[0].timestamp_ms, 1000);
        assert_eq!(merged.points[0].value, 10.0); // From base
        assert_eq!(merged.points[1].timestamp_ms, 2000);
        assert_eq!(merged.points[1].value, 200.0); // From other (overrides base)
        assert_eq!(merged.points[2].timestamp_ms, 3000);
        assert_eq!(merged.points[2].value, 300.0); // From other (overrides base)
        assert_eq!(merged.points[3].timestamp_ms, 4000);
        assert_eq!(merged.points[3].value, 40.0); // From other
    }

    #[test]
    fn should_merge_time_series_interleaved() {
        // given: two time series with interleaved timestamps
        let base = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
                Sample {
                    timestamp_ms: 5000,
                    value: 50.0,
                },
            ],
        };
        let base_bytes = base.encode().unwrap();

        let other = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 40.0,
                },
                Sample {
                    timestamp_ms: 6000,
                    value: 60.0,
                },
            ],
        };
        let other_bytes = other.encode().unwrap();

        // when: merge the series
        let merged_bytes = merge_batch_time_series(Some(base_bytes), &[other_bytes]).unwrap();
        let merged = TimeSeriesValue::decode(merged_bytes.as_ref()).unwrap();

        // then: should have all 6 points in sorted order
        assert_eq!(merged.points.len(), 6);
        assert_eq!(merged.points[0].timestamp_ms, 1000);
        assert_eq!(merged.points[1].timestamp_ms, 2000);
        assert_eq!(merged.points[2].timestamp_ms, 3000);
        assert_eq!(merged.points[3].timestamp_ms, 4000);
        assert_eq!(merged.points[4].timestamp_ms, 5000);
        assert_eq!(merged.points[5].timestamp_ms, 6000);
    }

    #[test]
    fn should_batch_merge_return_empty_when_no_existing_and_no_operands() {
        // given - nothing

        // when
        let merged = merge_batch_time_series(None, &[]).unwrap();

        // then
        assert!(merged.is_empty());
    }

    #[test]
    fn should_batch_merge_return_existing_when_no_operands() {
        // given
        let existing = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
            ],
        };
        let existing_bytes = existing.encode().unwrap();

        // when
        let merged = merge_batch_time_series(Some(existing_bytes.clone()), &[]).unwrap();

        // then
        assert_eq!(merged, existing_bytes);
    }

    #[test]
    fn should_batch_merge_return_operand_when_no_existing_and_single_operand() {
        // given
        let op = TimeSeriesValue {
            points: vec![Sample {
                timestamp_ms: 1000,
                value: 10.0,
            }],
        };
        let op_bytes = op.encode().unwrap();

        // when
        let merged = merge_batch_time_series(None, std::slice::from_ref(&op_bytes)).unwrap();

        // then
        assert_eq!(merged, op_bytes);
    }

    #[test]
    fn should_batch_merge_skip_empty_operands() {
        // given
        let existing = TimeSeriesValue {
            points: vec![Sample {
                timestamp_ms: 1000,
                value: 10.0,
            }],
        };
        let existing_bytes = existing.encode().unwrap();

        // when
        let merged =
            merge_batch_time_series(Some(existing_bytes.clone()), &[Bytes::new(), Bytes::new()])
                .unwrap();

        // then - only existing remains, single source passthrough
        assert_eq!(merged, existing_bytes);
    }

    #[test]
    fn should_batch_merge_return_empty_when_all_sources_empty() {
        // given - empty existing and empty operands

        // when
        let merged =
            merge_batch_time_series(Some(Bytes::new()), &[Bytes::new(), Bytes::new()]).unwrap();

        // then
        assert!(merged.is_empty());
    }

    #[test]
    fn should_batch_merge_multiple_operands_with_last_write_wins() {
        // given: three operands where later ones override earlier timestamps
        let op0 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op1 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 200.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op2 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 3000,
                    value: 300.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 40.0,
                },
            ],
        }
        .encode()
        .unwrap();

        // when - no existing value
        let merged = merge_batch_time_series(None, &[op0, op1, op2]).unwrap();
        let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();

        // then - timestamp 2000 takes op1's value, timestamp 3000 takes op2's value
        let expected = vec![
            Sample {
                timestamp_ms: 1000,
                value: 10.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 200.0,
            },
            Sample {
                timestamp_ms: 3000,
                value: 300.0,
            },
            Sample {
                timestamp_ms: 4000,
                value: 40.0,
            },
        ];
        assert_eq!(decoded.points, expected);
    }

    #[test]
    fn should_batch_merge_existing_with_multiple_operands() {
        // given
        let existing = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 1.0,
                },
                Sample {
                    timestamp_ms: 2000,
                    value: 2.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op0 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 20.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op1 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 3000,
                    value: 300.0,
                },
                Sample {
                    timestamp_ms: 4000,
                    value: 40.0,
                },
            ],
        }
        .encode()
        .unwrap();

        // when
        let merged = merge_batch_time_series(Some(existing), &[op0, op1]).unwrap();
        let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();

        // then - existing ts=2000 overridden by op0, op0 ts=3000 overridden by op1
        let expected = vec![
            Sample {
                timestamp_ms: 1000,
                value: 1.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 20.0,
            },
            Sample {
                timestamp_ms: 3000,
                value: 300.0,
            },
            Sample {
                timestamp_ms: 4000,
                value: 40.0,
            },
        ];
        assert_eq!(decoded.points, expected);
    }
}
