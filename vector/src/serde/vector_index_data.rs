//! VectorIndexData value encoding/decoding.
//!
//! Stores durable index assignments for a data vector.

use super::{Decode, Encode, EncodingError};
use crate::model::VECTOR_FIELD_NAME;
use crate::serde::vector_data::Field;
use crate::serde::vector_id::VectorId;
use bytes::{BufMut, Bytes, BytesMut};

/// Per-vector index metadata.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorIndexDataValue {
    pub(crate) postings: Vec<VectorId>,
    pub(crate) indexed_fields: Vec<Field>,
    /// Names of the `Text` (FTS) fields this vector populated, sorted ascending.
    /// Read on delete/upsert so the indexer can apply the per-field `FieldStats`
    /// delete deltas without re-tokenizing or reading the postings (RFC-0006).
    pub(crate) fts_fields: Vec<String>,
}

impl VectorIndexDataValue {
    pub(crate) fn new(
        postings: Vec<VectorId>,
        indexed_fields: Vec<Field>,
        fts_fields: Vec<String>,
    ) -> Self {
        for posting in &postings {
            assert!(posting.is_centroid());
        }
        for field in &indexed_fields {
            assert_ne!(field.field_name, VECTOR_FIELD_NAME);
        }
        let mut indexed_fields = indexed_fields;
        indexed_fields.sort_by(|a, b| a.field_name.cmp(&b.field_name));
        let mut fts_fields = fts_fields;
        fts_fields.sort();
        Self {
            postings,
            indexed_fields,
            fts_fields,
        }
    }

    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        buf.freeze()
    }

    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut slice = buf;
        Self::decode(&mut slice)
    }
}

impl Encode for VectorIndexDataValue {
    fn encode(&self, buf: &mut BytesMut) {
        let count = u16::try_from(self.postings.len()).expect("too many postings");
        buf.put_u16_le(count);
        for posting in &self.postings {
            posting.encode(buf);
        }
        let field_count =
            u16::try_from(self.indexed_fields.len()).expect("too many indexed fields");
        buf.put_u16_le(field_count);
        for field in &self.indexed_fields {
            field.encode(buf);
        }
        let fts_count = u16::try_from(self.fts_fields.len()).expect("too many fts fields");
        buf.put_u16_le(fts_count);
        for name in &self.fts_fields {
            let len = u16::try_from(name.len()).expect("fts field name exceeds u16::MAX bytes");
            buf.put_u16_le(len);
            buf.extend_from_slice(name.as_bytes());
        }
    }
}

impl Decode for VectorIndexDataValue {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for VectorIndexDataValue count".to_string(),
            });
        }
        let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        *buf = &buf[2..];

        let mut postings = Vec::with_capacity(count);
        for _ in 0..count {
            let posting = VectorId::decode(buf)?;
            if !posting.is_centroid() {
                return Err(EncodingError {
                    message: format!(
                        "invalid posting {} in VectorIndexDataValue: expected centroid id",
                        posting
                    ),
                });
            }
            postings.push(posting);
        }
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for VectorIndexDataValue field count".to_string(),
            });
        }
        let field_count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        *buf = &buf[2..];
        let mut indexed_fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let field = Field::decode(buf)?;
            if field.field_name == VECTOR_FIELD_NAME {
                return Err(EncodingError {
                    message: "VectorIndexDataValue indexed fields must not contain vector field"
                        .to_string(),
                });
            }
            indexed_fields.push(field);
        }
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for VectorIndexDataValue fts field count".to_string(),
            });
        }
        let fts_count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        *buf = &buf[2..];
        let mut fts_fields = Vec::with_capacity(fts_count);
        for _ in 0..fts_count {
            if buf.len() < 2 {
                return Err(EncodingError {
                    message: "Buffer too short for VectorIndexDataValue fts field length"
                        .to_string(),
                });
            }
            let len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
            *buf = &buf[2..];
            if buf.len() < len {
                return Err(EncodingError {
                    message: "Buffer too short for VectorIndexDataValue fts field name".to_string(),
                });
            }
            let name = std::str::from_utf8(&buf[..len])
                .map_err(|e| EncodingError {
                    message: format!("VectorIndexDataValue fts field is not valid UTF-8: {e}"),
                })?
                .to_string();
            *buf = &buf[len..];
            fts_fields.push(name);
        }
        Ok(Self {
            postings,
            indexed_fields,
            fts_fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_vector_index_data() {
        // given
        let value = VectorIndexDataValue::new(
            vec![VectorId::centroid_id(1, 10), VectorId::centroid_id(1, 11)],
            vec![Field::string("category", "shoes")],
            vec!["title".to_string(), "body".to_string()],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorIndexDataValue::decode_from_bytes(&encoded).unwrap();

        // then - round-trips and fts_fields are sorted
        assert_eq!(decoded, value);
        assert_eq!(
            decoded.fts_fields,
            vec!["body".to_string(), "title".to_string()]
        );
    }

    #[test]
    fn should_reject_non_centroid_postings() {
        // given
        let mut buf = BytesMut::new();
        buf.put_u16_le(1);
        VectorId::data_vector_id(7).encode(&mut buf);

        // when
        let result = VectorIndexDataValue::decode_from_bytes(&buf.freeze());

        // then
        assert!(result.is_err());
    }
}
