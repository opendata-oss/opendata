use crate::serde::DeserializeError;

/// Record tag combining record type (high 4 bits) and reserved bits (low 4 bits).
///
/// The record tag is the second byte of the key prefix. It encodes the record
/// type in the high 4 bits, leaving the low 4 bits for subsystem-specific use.
///
/// # Examples
///
/// ```
/// use common::serde::record_tag::RecordTag;
///
/// // Create a tag with type 0x01 and reserved bits 0x00
/// let tag = RecordTag::new(0x01, 0x00);
/// assert_eq!(tag.record_type(), 0x01);
/// assert_eq!(tag.reserved(), 0x00);
///
/// // Create a tag with type 0x05 and reserved bits 0x03
/// let tag = RecordTag::new(0x05, 0x03);
/// assert_eq!(tag.as_byte(), 0x53);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordTag(u8);

impl RecordTag {
    /// Creates a new record tag with the given record type and reserved bits.
    pub fn new(record_type: u8, reserved: u8) -> Self {
        RecordTag((record_type << 4) | reserved)
    }

    /// Creates a record tag from a raw byte value.
    ///
    /// Returns an error if the record type (high 4 bits) is 0.
    pub fn from_byte(byte: u8) -> Result<Self, DeserializeError> {
        let record_type = (byte & 0xF0) >> 4;
        if record_type == 0 {
            return Err(DeserializeError {
                message: format!(
                    "invalid record tag: 0x{:02x} (record type 0 is reserved)",
                    byte
                ),
            });
        }
        Ok(RecordTag(byte))
    }

    /// Returns the record type (high 4 bits).
    pub fn record_type(&self) -> u8 {
        (self.0 & 0xF0) >> 4
    }

    /// Returns the reserved bits (low 4 bits).
    pub fn reserved(&self) -> u8 {
        self.0 & 0x0F
    }

    /// Returns the raw byte representation.
    pub fn as_byte(&self) -> u8 {
        self.0
    }

    /// Returns a new record tag with the same record type but different reserved bits.
    ///
    /// # Panics
    ///
    /// Panics if `reserved` is greater than 15.
    pub fn with_reserved(&self, reserved: u8) -> Self {
        assert!(
            reserved <= 0x0F,
            "reserved bits {} must be in range 0-15",
            reserved
        );
        RecordTag((self.0 & 0xF0) | reserved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_record_tag() {
        // given
        let record_type = 0x05;
        let reserved = 0x03;

        // when
        let tag = RecordTag::new(record_type, reserved);

        // then
        assert_eq!(tag.as_byte(), 0x53);
        assert_eq!(tag.record_type(), 0x05);
        assert_eq!(tag.reserved(), 0x03);
    }

    #[test]
    fn should_create_tag_with_max_values() {
        // given
        let record_type = 0x0F;
        let reserved = 0x0F;

        // when
        let tag = RecordTag::new(record_type, reserved);

        // then
        assert_eq!(tag.as_byte(), 0xFF);
        assert_eq!(tag.record_type(), 0x0F);
        assert_eq!(tag.reserved(), 0x0F);
    }

    #[test]
    fn should_parse_tag_from_byte() {
        // given
        let byte = 0x53;

        // when
        let tag = RecordTag::from_byte(byte).unwrap();

        // then
        assert_eq!(tag.record_type(), 0x05);
        assert_eq!(tag.reserved(), 0x03);
    }

    #[test]
    fn should_reject_zero_record_type_byte() {
        // given
        let byte = 0x0F;

        // when
        let result = RecordTag::from_byte(byte);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message
                .contains("record type 0 is reserved")
        );
    }

    #[test]
    fn should_create_tag_with_different_reserved_bits() {
        // given
        let tag = RecordTag::new(0x05, 0x00);

        // when
        let new_tag = tag.with_reserved(0x0A);

        // then
        assert_eq!(new_tag.record_type(), 0x05);
        assert_eq!(new_tag.reserved(), 0x0A);
        assert_eq!(new_tag.as_byte(), 0x5A);
    }

    #[test]
    #[should_panic(expected = "reserved bits 16 must be in range 0-15")]
    fn should_panic_on_with_reserved_overflow() {
        let tag = RecordTag::new(0x01, 0x00);
        tag.with_reserved(16);
    }
}
