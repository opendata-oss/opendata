//! VectorData value encoding/decoding.
//!
//! Stores all data for a single vector: external ID, embedding vector, and metadata.
//!
//! ## Storage Design
//!
//! Vectors are stored individually (one record per vector) rather than batched
//! together. This enables:
//!
//! - **Efficient point lookups**: Load a single vector without reading neighbors
//! - **Partial loading**: During filtered search, only load vectors that pass filters
//! - **Independent updates**: Upsert/delete individual vectors without rewriting batches
//!
//! ## Unified Storage
//!
//! Vector data, metadata, and external ID are stored together in a single record.
//! The vector is stored as a special field with tag 0xff (255) alongside metadata fields.
//!
//! ## Dimensionality
//!
//! The vector length is not stored in the value—it's obtained from `CollectionMeta`.
//! All vectors in a collection must have the same dimensionality.

use super::{Decode, Encode, EncodingError, FieldValue, decode_utf8, encode_utf8};
use crate::FieldType;
use crate::model::VECTOR_FIELD_NAME;
use bytes::{Bytes, BytesMut};

/// A metadata field with name and value.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    /// Field name.
    pub field_name: String,
    /// Field value.
    pub value: FieldValue,
}

impl Field {
    pub fn new(field_name: impl Into<String>, value: FieldValue) -> Self {
        Self {
            field_name: field_name.into(),
            value,
        }
    }

    pub fn string(field_name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(field_name, FieldValue::String(value.into()))
    }

    pub fn int64(field_name: impl Into<String>, value: i64) -> Self {
        Self::new(field_name, FieldValue::Int64(value))
    }

    pub fn float64(field_name: impl Into<String>, value: f64) -> Self {
        Self::new(field_name, FieldValue::Float64(value))
    }

    pub fn bool(field_name: impl Into<String>, value: bool) -> Self {
        Self::new(field_name, FieldValue::Bool(value))
    }

    /// Create a vector field with the reserved name "vector".
    pub fn vector(value: Vec<f32>) -> Self {
        Self::new(VECTOR_FIELD_NAME, FieldValue::Vector(value))
    }
}

impl Encode for Field {
    fn encode(&self, buf: &mut BytesMut) {
        encode_utf8(&self.field_name, buf);
        self.value.encode(buf);
    }
}

impl Decode for Field {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        let field_name = decode_utf8(buf)?;
        let value = FieldValue::decode(buf)?;
        if field_name == VECTOR_FIELD_NAME && value.field_type() != FieldType::Vector {
            return Err(EncodingError {
                message: "vector field must have type vector".to_string(),
            });
        }
        Ok(Field { field_name, value })
    }
}

impl Field {
    /// Decode a MetadataField that may contain a Vector value.
    ///
    /// This is needed because Vector fields require knowing the dimensions.
    fn decode_with_dimensions(buf: &mut &[u8], dimensions: usize) -> Result<Self, EncodingError> {
        let field_name = decode_utf8(buf)?;
        let value = FieldValue::decode_with_dimensions(buf, dimensions)?;
        if field_name == VECTOR_FIELD_NAME && value.field_type() != FieldType::Vector {
            return Err(EncodingError {
                message: "vector field must have type vector".to_string(),
            });
        }
        Ok(Field { field_name, value })
    }
}

/// VectorData value storing the external ID, embedding vector, and metadata.
///
/// The key for this record is `VectorDataKey { vector_id }`, where `vector_id`
/// is the internal u64 ID (not the user-provided external ID).
///
/// ## Value Layout (little-endian)
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │  external_id: Utf8  (max 64 bytes, user-provided identifier)   │
/// │  fields:      Array<Field>                                     │
/// │                                                                │
/// │  Field                                                         │
/// │  ┌──────────────────────────────────────────────────────────┐  │
/// │  │  field_name:  Utf8                                       │  │
/// │  │  value:       FieldValue (tagged union)                  │  │
/// │  └──────────────────────────────────────────────────────────┘  │
/// │                                                                │
/// │  The vector is stored as a special field with name "vector"    │
/// │  and type FieldValue::Vector.                                  │
/// └────────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Field Ordering
///
/// Fields are automatically sorted by `field_name` during construction to ensure
/// consistent encoding.
///
/// ## Common Dimensionalities
///
/// | Model              | Dimensions | Vector Size |
/// |--------------------|------------|-------------|
/// | MiniLM-L6          | 384        | 1.5 KB      |
/// | BERT base          | 768        | 3 KB        |
/// | OpenAI ada-002     | 1536       | 6 KB        |
/// | OpenAI text-3-large| 3072       | 12 KB       |
#[derive(Debug, Clone, PartialEq)]
pub struct VectorDataValue {
    /// User-provided external identifier (max 64 bytes).
    external_id: String,

    /// All fields including metadata and the vector (stored as field "vector").
    /// Sorted by field_name for consistent encoding.
    fields: Vec<Field>,
}

impl VectorDataValue {
    /// Create a new VectorDataValue with external ID and fields.
    ///
    /// The caller should include a field with name `VECTOR_FIELD_NAME` ("vector")
    /// containing a `FieldValue::Vector`. Fields are sorted by name for consistent encoding.
    pub fn new(external_id: impl Into<String>, fields: Vec<Field>) -> Self {
        let mut fields = fields;
        // Sort fields by name for consistent encoding
        fields.sort_by(|a, b| a.field_name.cmp(&b.field_name));
        Self {
            external_id: external_id.into(),
            fields,
        }
    }

    /// Encode to bytes.
    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        encode_utf8(&self.external_id, &mut buf);
        // Encode array manually: count + elements
        let count = self.fields.len();
        if count > u16::MAX as usize {
            panic!("Too many fields: {}", count);
        }
        buf.extend_from_slice(&(count as u16).to_le_bytes());
        for field in &self.fields {
            field.encode(&mut buf);
        }
        buf.freeze()
    }

    /// Decode vector data from bytes.
    ///
    /// Requires dimensions to properly decode the vector field.
    pub fn decode_from_bytes(buf: &[u8], dimensions: usize) -> Result<Self, EncodingError> {
        let mut slice = buf;
        let external_id = decode_utf8(&mut slice)?;

        // Decode array count
        if slice.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for field count".to_string(),
            });
        }
        let count = u16::from_le_bytes([slice[0], slice[1]]) as usize;
        slice = &slice[2..];

        // Decode fields
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            fields.push(Field::decode_with_dimensions(&mut slice, dimensions)?);
        }

        Ok(VectorDataValue {
            external_id,
            fields,
        })
    }

    /// Returns the external ID.
    pub fn external_id(&self) -> &str {
        &self.external_id
    }

    /// Returns an iterator over metadata fields (excludes the vector field).
    pub fn fields(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
    }

    /// Get a metadata field by name.
    pub fn field(&self, name: &str) -> Option<&FieldValue> {
        self.fields
            .iter()
            .find(|f| f.field_name == name)
            .map(|f| &f.value)
    }

    /// Get a string field value.
    pub fn string_field(&self, name: &str) -> Option<&str> {
        match self.field(name) {
            Some(FieldValue::String(s)) => Some(s),
            _ => None,
        }
    }

    /// Get an i64 field value.
    pub fn int64_field(&self, name: &str) -> Option<i64> {
        match self.field(name) {
            Some(FieldValue::Int64(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get an f64 field value.
    pub fn float64_field(&self, name: &str) -> Option<f64> {
        match self.field(name) {
            Some(FieldValue::Float64(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get a bool field value.
    pub fn bool_field(&self, name: &str) -> Option<bool> {
        match self.field(name) {
            Some(FieldValue::Bool(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get vector field value
    pub fn vector_field(&self) -> &[f32] {
        let FieldValue::Vector(v) = self
            .field(VECTOR_FIELD_NAME)
            .expect("vector data must have vector field")
        else {
            panic!("vector field must have type vector")
        };
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_vector_data_value() {
        // given
        let value = VectorDataValue::new(
            "my-vector-id",
            vec![
                Field::vector(vec![1.0, 2.0, 3.0, 4.0]),
                Field::string("category", "shoes"),
                Field::float64("price", 99.99),
                Field::bool("active", true),
            ],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded, 4).unwrap();

        // then
        assert_eq!(decoded.external_id(), "my-vector-id");
        assert_eq!(decoded.string_field("category"), Some("shoes"));
        assert_eq!(decoded.float64_field("price"), Some(99.99));
        assert_eq!(decoded.bool_field("active"), Some(true));
    }

    #[test]
    fn should_handle_vector_only() {
        // given
        let value = VectorDataValue::new("vector-only", vec![Field::vector(vec![1.0, 2.0, 3.0])]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded, 3).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_sort_fields_by_name() {
        // given
        let value = VectorDataValue::new(
            "test",
            vec![
                Field::vector(vec![1.0]),
                Field::string("zebra", "last"),
                Field::string("apple", "first"),
                Field::string("mango", "middle"),
            ],
        );

        // when
        let metadata: Vec<_> = value.fields().collect();

        // then - fields should be sorted alphabetically
        assert_eq!(metadata[0].field_name, "apple");
        assert_eq!(metadata[1].field_name, "mango");
        assert_eq!(metadata[2].field_name, "vector");
        assert_eq!(metadata[3].field_name, "zebra");
    }

    #[test]
    fn should_handle_all_value_types() {
        // given
        let value = VectorDataValue::new(
            "test",
            vec![
                Field::vector(vec![1.0, 2.0]),
                Field::string("s", "hello"),
                Field::int64("i", -42),
                Field::float64("f", 1.23),
                Field::bool("b", false),
            ],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded, 2).unwrap();

        // then
        assert_eq!(decoded.string_field("s"), Some("hello"));
        assert_eq!(decoded.int64_field("i"), Some(-42));
        assert_eq!(decoded.float64_field("f"), Some(1.23));
        assert_eq!(decoded.bool_field("b"), Some(false));
    }

    #[test]
    fn should_handle_high_dimensional_vector() {
        // given
        let vector: Vec<f32> = (0..1536).map(|i| i as f32 * 0.001).collect();
        let value = VectorDataValue::new("high-dim", vec![Field::vector(vector)]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded, 1536).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_handle_unicode_in_metadata() {
        // given
        let value = VectorDataValue::new(
            "unicode-test",
            vec![
                Field::vector(vec![1.0]),
                Field::string("greeting", "Hello, 世界!"),
            ],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded, 1).unwrap();

        // then
        assert_eq!(decoded.string_field("greeting"), Some("Hello, 世界!"));
    }

    #[test]
    fn should_return_none_for_missing_field() {
        // given
        let value = VectorDataValue::new(
            "test",
            vec![Field::vector(vec![1.0]), Field::string("exists", "yes")],
        );

        // when / then
        assert!(value.field("missing").is_none());
        assert!(value.string_field("missing").is_none());
    }

    #[test]
    fn should_return_none_for_wrong_type() {
        // given
        let value = VectorDataValue::new(
            "test",
            vec![Field::vector(vec![1.0]), Field::string("name", "value")],
        );

        // when / then
        assert!(value.int64_field("name").is_none());
        assert!(value.float64_field("name").is_none());
        assert!(value.bool_field("name").is_none());
    }

    #[test]
    fn should_handle_special_float_values_in_vector() {
        // given
        let value = VectorDataValue::new(
            "special-floats",
            vec![Field::vector(vec![
                0.0,
                -0.0,
                f32::INFINITY,
                f32::NEG_INFINITY,
                f32::MIN,
                f32::MAX,
            ])],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded, 6).unwrap();

        // then - verify round-trip worked by checking metadata fields count
        assert_eq!(decoded.external_id(), "special-floats");
        assert_eq!(decoded.fields().count(), 1);
        assert_eq!(decoded.vector_field(), value.vector_field());
    }
}
