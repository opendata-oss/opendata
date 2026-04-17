use crate::serde::{Decode, Encode};
use bytes::{BufMut, BytesMut};
use common::serde::encoding::EncodingError;
use std::fmt::{Debug, Display, Formatter};

const NUMBER_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;
const LEVEL_MASK: u64 = 0xFF00_0000_0000_0000;
pub(crate) const ROOT_ID_NUM: u64 = 0;
pub(crate) const ROOT_VECTOR_ID: VectorId = VectorId {
    id: 0xFF00_0000_0000_0000,
};
pub(crate) const ROOT_LEVEL: u8 = 0xFF;
pub(crate) const LEAF_LEVEL: u8 = 1;

// TODO: have a separate NodeId, CentroidId, and VectorId
//   have Into fns for each. Conversion to CentroidId and VectorId validates the levels
//   use the right terminology (node vs centroid vs vector in variable names)

/// Internal ID for vectors. Can reference either a data vector, a centroid vector, or the centroid
/// tree root. The ID is a 64-bit identifier where the highest byte specifies the level in the
/// centroid tree that holds the vector, and the lower 56 bits specify a sequence number. Sequence
/// numbers must be unique within a level. Data vectors have level 0. Centroid vectors have level
/// 1-254, with level 1 being referred to as the "leaf". The root uses level 255.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct VectorId {
    id: u64,
}

impl Display for VectorId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}:{}", self.level(), self.number())
    }
}

impl Debug for VectorId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.level(), self.number())
    }
}

impl VectorId {
    pub fn data_vector_id(number: u64) -> Self {
        let vector_id = VectorId { id: number };
        assert_eq!(vector_id.level(), 0);
        vector_id
    }

    pub const fn centroid_id(level: u8, number: u64) -> Self {
        assert!(level != 0);
        assert!(level != 0xFF);
        assert!((number & LEVEL_MASK) == 0);
        let raw = ((level as u64) << (64 - 8)) | number;
        let vector_id = VectorId { id: raw };
        assert!(vector_id.level() == level);
        assert!(vector_id.number() == number);
        assert!(vector_id.number() != ROOT_ID_NUM);
        vector_id
    }

    pub(crate) fn encode_level_prefix(buf: &mut BytesMut, level: u8) {
        buf.put_u8(level);
    }

    pub const fn level(&self) -> u8 {
        (self.id >> (64 - 8)) as u8
    }

    pub fn is_data_vector(&self) -> bool {
        self.level() == 0
    }

    pub fn is_root(&self) -> bool {
        self == &ROOT_VECTOR_ID
    }

    pub fn is_tree_node(&self) -> bool {
        self.is_centroid() || self.is_root()
    }

    pub fn is_centroid(&self) -> bool {
        self.level() > 0 && !self.is_root()
    }

    const fn number(&self) -> u64 {
        self.id & NUMBER_MASK
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

impl Encode for VectorId {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put(self.id.to_be_bytes().as_ref());
    }
}

impl Decode for VectorId {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 8 {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for VectorId: need 8 bytes, have {}",
                    buf.len()
                ),
            });
        }
        let raw = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        *buf = &buf[8..];
        Ok(Self { id: raw })
    }
}

#[cfg(test)]
mod tests {
    use crate::serde::vector_id::VectorId;
    use crate::serde::{Decode, Encode};
    use bytes::BytesMut;

    #[test]
    fn test_id_should_return_level() {
        let id = VectorId::centroid_id(3, 100);
        assert!(id.is_centroid());
        assert_eq!(id.level(), 3);
    }

    #[test]
    fn test_id_should_return_id() {
        let id = VectorId::centroid_id(3, 100);
        assert_eq!(id.number(), 100);
    }

    #[test]
    fn test_should_encode_decode() {
        // given:
        let id = VectorId::centroid_id(3, 0x00DE_ADBE_EF12_3456);

        // when:
        let mut buf = BytesMut::new();
        id.encode(&mut buf);
        let encoded = buf.freeze();
        let mut cursor = encoded.as_ref();
        let decoded = VectorId::decode(&mut cursor).unwrap();

        // then:
        assert_eq!(decoded, id);
        assert_eq!(
            encoded.as_ref(),
            &[0x03, 0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56]
        );
    }

    #[test]
    fn test_should_set_level_0_for_data_vectors() {
        let id = VectorId::data_vector_id(123);

        assert!(id.is_data_vector());
        assert_eq!(id.level(), 0);
        assert_eq!(id.number(), 123);
    }

    #[test]
    #[should_panic]
    fn test_should_fail_data_vector_id_that_sets_level() {
        VectorId::data_vector_id(0x03DE_ADBE_EF12_3456);
    }

    #[test]
    #[should_panic]
    fn test_should_fail_centroid_vector_id_with_data_level() {
        VectorId::centroid_id(0, 100);
    }

    #[test]
    #[should_panic]
    fn test_should_fail_centroid_vector_id_with_root_level() {
        VectorId::centroid_id(0xFF, 100);
    }

    #[test]
    #[should_panic]
    fn test_should_fail_centroid_vector_id_that_sets_level() {
        VectorId::centroid_id(3, 0x03DE_ADBE_EF12_3456);
    }
}
