// ForwardIndex value structure with MetricMeta and LabelBinding

use crate::index::SeriesSpec;
use crate::model::{Label, MetricType, Temporality};

use super::*;
use bytes::{Bytes, BytesMut};

/// MetricMeta: Encodes the series' metric type and auxiliary flags
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricMeta {
    pub metric_type: u8,
    pub flags: u8,
}

impl MetricMeta {
    /// Extract temporality from flags (bits 0-1)
    /// 0=Unspecified, 1=Cumulative, 2=Delta
    pub fn temporality(&self) -> u8 {
        self.flags & 0x03
    }

    /// Extract monotonic flag (bit 2)
    /// Only meaningful when metric_type=2 (Sum)
    pub fn monotonic(&self) -> bool {
        (self.flags & 0x04) != 0
    }
}

impl From<MetricType> for MetricMeta {
    fn from(metric_type: MetricType) -> Self {
        let (metric_type_val, temporality, monotonic) = match metric_type {
            MetricType::Gauge => (1, Temporality::Unspecified, false),
            MetricType::Sum {
                monotonic,
                temporality,
            } => (2, temporality, monotonic),
            MetricType::Histogram { temporality } => (3, temporality, false),
            MetricType::ExponentialHistogram { temporality } => (4, temporality, false),
            MetricType::Summary => (5, Temporality::Unspecified, false),
        };

        // Encode temporality in bits 0-1
        let temporality_bits = match temporality {
            Temporality::Unspecified => 0,
            Temporality::Cumulative => 1,
            Temporality::Delta => 2,
        };

        // Encode monotonic flag in bit 2 (only meaningful for Sum, but we set it for all)
        let monotonic_bit = if monotonic { 0x04 } else { 0x00 };

        // Combine flags: bits 0-1 = temporality, bit 2 = monotonic, bits 3-7 = reserved (must be zero)
        let flags = temporality_bits | monotonic_bit;

        MetricMeta {
            metric_type: metric_type_val,
            flags,
        }
    }
}

impl From<Option<MetricType>> for MetricMeta {
    fn from(metric_type: Option<MetricType>) -> Self {
        match metric_type {
            Some(mt) => MetricMeta::from(mt),
            None => MetricMeta {
                metric_type: 0, // 0 = unknown/unspecified
                flags: 0,
            },
        }
    }
}

impl Encode for MetricMeta {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&[self.metric_type, self.flags]);
    }
}

impl Decode for MetricMeta {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for MetricMeta".to_string(),
            });
        }
        let metric_type = buf[0];
        let flags = buf[1];
        *buf = &buf[2..];
        Ok(MetricMeta { metric_type, flags })
    }
}

impl Encode for Label {
    fn encode(&self, buf: &mut BytesMut) {
        encode_utf8(&self.name, buf);
        encode_utf8(&self.value, buf);
    }
}

impl Decode for Label {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        let name = decode_utf8(buf)?;
        let value = decode_utf8(buf)?;
        Ok(Label { name, value })
    }
}

/// ForwardIndex value
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardIndexValue {
    pub metric_unit: Option<String>,
    pub metric_meta: MetricMeta,
    pub label_count: u16,
    pub labels: Vec<Label>,
}

impl ForwardIndexValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        encode_optional_utf8(self.metric_unit.as_deref(), &mut buf);
        self.metric_meta.encode(&mut buf);
        buf.extend_from_slice(&self.label_count.to_le_bytes());
        encode_array(&self.labels, &mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut slice = buf;
        let metric_unit = decode_optional_utf8(&mut slice)?;
        let metric_meta = MetricMeta::decode(&mut slice)?;

        if slice.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for label_count".to_string(),
            });
        }
        let label_count = u16::from_le_bytes([slice[0], slice[1]]);
        slice = &slice[2..];

        let labels = decode_array::<Label>(&mut slice)?;

        if labels.len() != label_count as usize {
            return Err(EncodingError {
                message: format!(
                    "Label count mismatch: expected {}, got {}",
                    label_count,
                    labels.len()
                ),
            });
        }

        Ok(ForwardIndexValue {
            metric_unit,
            metric_meta,
            label_count,
            labels,
        })
    }
}

impl From<ForwardIndexValue> for SeriesSpec {
    fn from(value: ForwardIndexValue) -> Self {
        let temporality = match value.metric_meta.temporality() {
            0 => Temporality::Unspecified,
            1 => Temporality::Cumulative,
            2 => Temporality::Delta,
            _ => Temporality::Unspecified, // Default fallback
        };

        let metric_type = match value.metric_meta.metric_type {
            0 => None, // Unknown/unspecified
            1 => Some(MetricType::Gauge),
            2 => Some(MetricType::Sum {
                monotonic: value.metric_meta.monotonic(),
                temporality,
            }),
            3 => Some(MetricType::Histogram { temporality }),
            4 => Some(MetricType::ExponentialHistogram { temporality }),
            5 => Some(MetricType::Summary),
            _ => None, // Unknown types map to None
        };

        SeriesSpec {
            unit: value.metric_unit,
            metric_type,
            labels: value.labels,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_forward_index_value() {
        // given
        let value = ForwardIndexValue {
            metric_unit: Some("bytes".to_string()),
            metric_meta: MetricMeta {
                metric_type: 1, // Gauge
                flags: 0x00,
            },
            label_count: 2,
            labels: vec![
                Label {
                    name: "host".to_string(),
                    value: "server1".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
        };

        // when
        let encoded = value.encode();
        let decoded = ForwardIndexValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_forward_index_value_without_metric_unit() {
        // given
        let value = ForwardIndexValue {
            metric_unit: None,
            metric_meta: MetricMeta {
                metric_type: 2, // Sum
                flags: 0x05,    // Cumulative (0x01) | Monotonic (0x04)
            },
            label_count: 1,
            labels: vec![Label {
                name: "service".to_string(),
                value: "api".to_string(),
            }],
        };

        // when
        let encoded = value.encode();
        let decoded = ForwardIndexValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
        assert_eq!(decoded.metric_meta.temporality(), 1);
        assert!(decoded.metric_meta.monotonic());
    }

    #[test]
    fn should_encode_and_decode_forward_index_value_empty_labels() {
        // given
        let value = ForwardIndexValue {
            metric_unit: Some("seconds".to_string()),
            metric_meta: MetricMeta {
                metric_type: 3, // Histogram
                flags: 0x02,    // Delta
            },
            label_count: 0,
            labels: vec![],
        };

        // when
        let encoded = value.encode();
        let decoded = ForwardIndexValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_extract_metric_meta_flags() {
        // given
        let meta = MetricMeta {
            metric_type: 2,
            flags: 0x05, // Cumulative (0x01) | Monotonic (0x04)
        };

        // then
        assert_eq!(meta.temporality(), 1); // Cumulative
        assert!(meta.monotonic());
    }
}
