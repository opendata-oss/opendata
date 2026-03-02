use bytes::{BufMut, Bytes, BytesMut};
use common::serde::sortable::{encode_f64_sortable, encode_i64_sortable};
use common::serde::terminated_bytes;
use grafeo_common::types::Value;

/// Flags for node/edge record values.
pub(crate) const FLAG_DELETED: u16 = 0x0001;

// ---------------------------------------------------------------------------
// NodeRecordValue: flags(2) + label_count(2) + prop_count(2) + reserved(2) = 8 bytes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeRecordValue {
    pub flags: u16,
    pub label_count: u16,
    pub prop_count: u16,
}

impl NodeRecordValue {
    const SIZE: usize = 8;

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        buf.put_u16_le(self.flags);
        buf.put_u16_le(self.label_count);
        buf.put_u16_le(self.prop_count);
        buf.put_u16_le(0); // reserved
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, crate::Error> {
        if data.len() < Self::SIZE {
            return Err(crate::Error::Encoding(format!(
                "NodeRecordValue too short: need {}, got {}",
                Self::SIZE,
                data.len()
            )));
        }
        let flags = u16::from_le_bytes(data[0..2].try_into().unwrap());
        let label_count = u16::from_le_bytes(data[2..4].try_into().unwrap());
        let prop_count = u16::from_le_bytes(data[4..6].try_into().unwrap());
        Ok(Self { flags, label_count, prop_count })
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & FLAG_DELETED != 0
    }
}

// ---------------------------------------------------------------------------
// EdgeRecordValue: src(8) + dst(8) + type_id(4) + flags(2) + prop_count(2) = 24 bytes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeRecordValue {
    pub src: u64,
    pub dst: u64,
    pub type_id: u32,
    pub flags: u16,
    pub prop_count: u16,
}

impl EdgeRecordValue {
    const SIZE: usize = 24;

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        buf.put_u64_le(self.src);
        buf.put_u64_le(self.dst);
        buf.put_u32_le(self.type_id);
        buf.put_u16_le(self.flags);
        buf.put_u16_le(self.prop_count);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, crate::Error> {
        if data.len() < Self::SIZE {
            return Err(crate::Error::Encoding(format!(
                "EdgeRecordValue too short: need {}, got {}",
                Self::SIZE,
                data.len()
            )));
        }
        let src = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let dst = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let type_id = u32::from_le_bytes(data[16..20].try_into().unwrap());
        let flags = u16::from_le_bytes(data[20..22].try_into().unwrap());
        let prop_count = u16::from_le_bytes(data[22..24].try_into().unwrap());
        Ok(Self { src, dst, type_id, flags, prop_count })
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & FLAG_DELETED != 0
    }
}

// ---------------------------------------------------------------------------
// Property value encoding (delegated to grafeo-common bincode)
// ---------------------------------------------------------------------------

/// Encodes a Grafeo Value to bytes for storage.
pub(crate) fn encode_value(value: &Value) -> Result<Bytes, crate::Error> {
    let bytes = value
        .serialize()
        .map_err(|e| crate::Error::Encoding(format!("failed to serialize value: {e}")))?;
    Ok(Bytes::from(bytes))
}

/// Decodes a Grafeo Value from stored bytes.
pub(crate) fn decode_value(data: &[u8]) -> Result<Value, crate::Error> {
    Value::deserialize(data)
        .map_err(|e| crate::Error::Encoding(format!("failed to deserialize value: {e}")))
}

// ---------------------------------------------------------------------------
// Sortable value encoding (for PropertyIndex keys)
// ---------------------------------------------------------------------------

/// Encodes a value for use in PropertyIndex keys, preserving sort order.
///
/// Returns `None` for types that cannot be meaningfully sorted (Null, List, Map, etc.).
pub(crate) fn encode_sortable_value(value: &Value) -> Option<Bytes> {
    match value {
        Value::Bool(b) => {
            let mut buf = BytesMut::with_capacity(1);
            buf.put_u8(if *b { 1 } else { 0 });
            Some(buf.freeze())
        }
        Value::Int64(n) => {
            let mut buf = BytesMut::with_capacity(8);
            buf.put_u64(encode_i64_sortable(*n));
            Some(buf.freeze())
        }
        Value::Float64(f) => {
            let mut buf = BytesMut::with_capacity(8);
            buf.put_u64(encode_f64_sortable(*f));
            Some(buf.freeze())
        }
        Value::String(s) => Some(terminated_bytes::serialize_to_bytes(s.as_bytes())),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_node_record_value() {
        // given
        let val = NodeRecordValue {
            flags: 0,
            label_count: 2,
            prop_count: 5,
        };

        // when
        let encoded = val.encode();
        let decoded = NodeRecordValue::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, val);
    }

    #[test]
    fn should_detect_deleted_node() {
        // given
        let val = NodeRecordValue {
            flags: FLAG_DELETED,
            label_count: 0,
            prop_count: 0,
        };

        // then
        assert!(val.is_deleted());
    }

    #[test]
    fn should_roundtrip_edge_record_value() {
        // given
        let val = EdgeRecordValue {
            src: 10,
            dst: 20,
            type_id: 3,
            flags: 0,
            prop_count: 1,
        };

        // when
        let encoded = val.encode();
        let decoded = EdgeRecordValue::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, val);
    }

    #[test]
    fn should_roundtrip_grafeo_value() {
        // given
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int64(42),
            Value::Float64(3.14),
            Value::String("hello".into()),
        ];

        for value in values {
            // when
            let encoded = encode_value(&value).unwrap();
            let decoded = decode_value(&encoded).unwrap();

            // then
            assert_eq!(decoded, value, "roundtrip failed for {value:?}");
        }
    }

    #[test]
    fn should_encode_sortable_int64_preserving_order() {
        // given
        let values: Vec<i64> = vec![-1000, -1, 0, 1, 1000];

        // when
        let encoded: Vec<Bytes> = values
            .iter()
            .map(|v| encode_sortable_value(&Value::Int64(*v)).unwrap())
            .collect();

        // then: lexicographic order should match numeric order
        for window in encoded.windows(2) {
            assert!(
                window[0] < window[1],
                "sortable ordering violated for int64"
            );
        }
    }

    #[test]
    fn should_encode_sortable_float64_preserving_order() {
        // given
        let values: Vec<f64> = vec![-100.0, -1.0, 0.0, 1.0, 100.0];

        // when
        let encoded: Vec<Bytes> = values
            .iter()
            .map(|v| encode_sortable_value(&Value::Float64(*v)).unwrap())
            .collect();

        // then: lexicographic order should match numeric order
        for window in encoded.windows(2) {
            assert!(
                window[0] < window[1],
                "sortable ordering violated for float64"
            );
        }
    }

    #[test]
    fn should_encode_sortable_string_preserving_order() {
        // given
        let values = vec!["apple", "banana", "cherry"];

        // when
        let encoded: Vec<Bytes> = values
            .iter()
            .map(|v| encode_sortable_value(&Value::String((*v).into())).unwrap())
            .collect();

        // then
        for window in encoded.windows(2) {
            assert!(
                window[0] < window[1],
                "sortable ordering violated for string"
            );
        }
    }

    #[test]
    fn should_return_none_for_unsortable_types() {
        assert!(encode_sortable_value(&Value::Null).is_none());
    }
}
