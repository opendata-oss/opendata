//! VectorMeta value encoding/decoding.
//!
//! Stores metadata key-value pairs and the external ID for a vector.
//!
//! ## Purpose
//!
//! `VectorMeta` provides:
//!
//! 1. **Reverse ID lookup**: Maps internal u64 ID → external string ID (the forward
//!    mapping is in `IdDictionary`)
//! 2. **Metadata storage**: Key-value pairs attached to the vector
//! 3. **Result formatting**: When returning search results, the external ID and
//!    metadata are fetched from here
//!
//! ## Separation from VectorData
//!
//! Metadata is stored separately from the raw vector bytes because:
//!
//! - **Different access patterns**: Distance computation needs vectors; result
//!   formatting needs metadata
//! - **Size difference**: Vectors are large (KB); metadata is small (bytes to KB)
//! - **Metadata-only operations**: Can scan/filter metadata without loading vectors
//!
//! ## Metadata Schema
//!
//! Field names must match entries in `CollectionMeta.metadata_fields`. Unknown
//! field names are rejected at write time. The schema defines:
//!
//! - Field type (string, int64, float64, bool)
//! - Whether the field is indexed for filtering
//!
//! ## Field Ordering
//!
//! Fields are serialized in lexicographic order by field name for consistent
//! encoding. This ensures identical metadata produces identical bytes.

use super::{
    Decode, Encode, EncodingError, FieldValue, decode_array, decode_utf8, encode_array, encode_utf8,
};
use bytes::{Bytes, BytesMut};

/// A metadata field with name and value.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataField {
    /// Field name.
    pub field_name: String,
    /// Field value.
    pub value: FieldValue,
}

impl MetadataField {
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
}

impl Encode for MetadataField {
    fn encode(&self, buf: &mut BytesMut) {
        encode_utf8(&self.field_name, buf);
        self.value.encode(buf);
    }
}

impl Decode for MetadataField {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        let field_name = decode_utf8(buf)?;
        let value = FieldValue::decode(buf)?;
        Ok(MetadataField { field_name, value })
    }
}

/// VectorMeta value storing external ID and metadata fields.
///
/// The key for this record is `VectorMetaKey { vector_id }`, where `vector_id`
/// is the internal u64 ID. This provides the reverse mapping from internal ID
/// to external ID (the forward mapping is in `IdDictionary`).
///
/// ## Value Layout (little-endian)
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │  external_id: Utf8  (max 64 bytes, user-provided identifier)   │
/// │  fields:      Array<MetadataField>                             │
/// │                                                                │
/// │  MetadataField                                                 │
/// │  ┌──────────────────────────────────────────────────────────┐  │
/// │  │  field_name:  Utf8                                       │  │
/// │  │  value:       FieldValue (tagged union)               │  │
/// │  └──────────────────────────────────────────────────────────┘  │
/// │                                                                │
/// │  FieldValue                                                 │
/// │  ┌──────────────────────────────────────────────────────────┐  │
/// │  │  tag:    u8  (0=string, 1=int64, 2=float64, 3=bool)      │  │
/// │  │  value:  Utf8 | i64 | f64 | u8                           │  │
/// │  └──────────────────────────────────────────────────────────┘  │
/// └────────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Field Ordering
///
/// Fields are automatically sorted by `field_name` during construction to ensure
/// consistent encoding. This is important for deterministic serialization.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorMetaValue {
    /// User-provided external identifier (max 64 bytes).
    ///
    /// This is the string ID users use to reference the vector. The system
    /// assigns internal u64 IDs for efficient storage and indexing.
    pub external_id: String,

    /// Metadata fields (automatically sorted by field_name).
    ///
    /// Field names must match entries in `CollectionMeta.metadata_fields`.
    pub fields: Vec<MetadataField>,
}

impl VectorMetaValue {
    pub fn new(external_id: impl Into<String>, fields: Vec<MetadataField>) -> Self {
        let mut fields = fields;
        // Sort fields by name for consistent encoding
        fields.sort_by(|a, b| a.field_name.cmp(&b.field_name));
        Self {
            external_id: external_id.into(),
            fields,
        }
    }

    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        encode_utf8(&self.external_id, &mut buf);
        encode_array(&self.fields, &mut buf);
        buf.freeze()
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut slice = buf;
        let external_id = decode_utf8(&mut slice)?;
        let fields = decode_array(&mut slice)?;
        Ok(VectorMetaValue {
            external_id,
            fields,
        })
    }

    /// Get a metadata field by name.
    pub fn get_field(&self, name: &str) -> Option<&FieldValue> {
        self.fields
            .iter()
            .find(|f| f.field_name == name)
            .map(|f| &f.value)
    }

    /// Get a string field value.
    pub fn get_string(&self, name: &str) -> Option<&str> {
        match self.get_field(name) {
            Some(FieldValue::String(s)) => Some(s),
            _ => None,
        }
    }

    /// Get an i64 field value.
    pub fn get_int64(&self, name: &str) -> Option<i64> {
        match self.get_field(name) {
            Some(FieldValue::Int64(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get an f64 field value.
    pub fn get_float64(&self, name: &str) -> Option<f64> {
        match self.get_field(name) {
            Some(FieldValue::Float64(v)) => Some(*v),
            _ => None,
        }
    }

    /// Get a bool field value.
    pub fn get_bool(&self, name: &str) -> Option<bool> {
        match self.get_field(name) {
            Some(FieldValue::Bool(v)) => Some(*v),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_vector_meta() {
        // given
        let value = VectorMetaValue::new(
            "my-vector-id",
            vec![
                MetadataField::string("category", "shoes"),
                MetadataField::float64("price", 99.99),
                MetadataField::bool("active", true),
            ],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorMetaValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.external_id, "my-vector-id");
        assert_eq!(decoded.get_string("category"), Some("shoes"));
        assert_eq!(decoded.get_float64("price"), Some(99.99));
        assert_eq!(decoded.get_bool("active"), Some(true));
    }

    #[test]
    fn should_handle_empty_fields() {
        // given
        let value = VectorMetaValue::new("empty-vector", vec![]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorMetaValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.external_id, "empty-vector");
        assert!(decoded.fields.is_empty());
    }

    #[test]
    fn should_sort_fields_by_name() {
        // given
        let value = VectorMetaValue::new(
            "test",
            vec![
                MetadataField::string("zebra", "last"),
                MetadataField::string("apple", "first"),
                MetadataField::string("mango", "middle"),
            ],
        );

        // then
        assert_eq!(value.fields[0].field_name, "apple");
        assert_eq!(value.fields[1].field_name, "mango");
        assert_eq!(value.fields[2].field_name, "zebra");
    }

    #[test]
    fn should_handle_all_value_types() {
        // given
        let value = VectorMetaValue::new(
            "test",
            vec![
                MetadataField::string("s", "hello"),
                MetadataField::int64("i", -42),
                MetadataField::float64("f", 3.14),
                MetadataField::bool("b", false),
            ],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorMetaValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.get_string("s"), Some("hello"));
        assert_eq!(decoded.get_int64("i"), Some(-42));
        assert_eq!(decoded.get_float64("f"), Some(3.14));
        assert_eq!(decoded.get_bool("b"), Some(false));
    }

    #[test]
    fn should_handle_unicode_in_metadata() {
        // given
        let value = VectorMetaValue::new(
            "unicode-test",
            vec![MetadataField::string("greeting", "Hello, 世界!")],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorMetaValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.get_string("greeting"), Some("Hello, 世界!"));
    }

    #[test]
    fn should_return_none_for_missing_field() {
        // given
        let value = VectorMetaValue::new("test", vec![MetadataField::string("exists", "yes")]);

        // when / then
        assert!(value.get_field("missing").is_none());
        assert!(value.get_string("missing").is_none());
    }

    #[test]
    fn should_return_none_for_wrong_type() {
        // given
        let value = VectorMetaValue::new("test", vec![MetadataField::string("name", "value")]);

        // when / then
        assert!(value.get_int64("name").is_none());
        assert!(value.get_float64("name").is_none());
        assert!(value.get_bool("name").is_none());
    }
}
