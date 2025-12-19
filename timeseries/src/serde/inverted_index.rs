// InvertedIndex value structure using RoaringBitmap

use super::*;
use bytes::Bytes;
use roaring::RoaringBitmap;

/// InvertedIndex value: RoaringBitmap<u32> encoding series IDs
#[derive(Debug, Clone, PartialEq)]
pub struct InvertedIndexValue {
    pub postings: RoaringBitmap,
}

impl InvertedIndexValue {
    pub fn encode(&self) -> Result<Bytes, EncodingError> {
        let mut buf = Vec::new();
        self.postings
            .serialize_into(&mut buf)
            .map_err(|e| EncodingError {
                message: format!("Failed to serialize RoaringBitmap: {}", e),
            })?;
        Ok(Bytes::from(buf))
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let bitmap = RoaringBitmap::deserialize_from(buf).map_err(|e| EncodingError {
            message: format!("Failed to deserialize RoaringBitmap: {}", e),
        })?;
        Ok(InvertedIndexValue { postings: bitmap })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_inverted_index_value() {
        // given
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(42);
        bitmap.insert(99);
        let value = InvertedIndexValue { postings: bitmap };

        // when
        let encoded = value.encode().unwrap();
        let decoded = InvertedIndexValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded.postings, value.postings);
    }

    #[test]
    fn should_encode_and_decode_empty_inverted_index_value() {
        // given
        let value = InvertedIndexValue {
            postings: RoaringBitmap::new(),
        };

        // when
        let encoded = value.encode().unwrap();
        let decoded = InvertedIndexValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded.postings, value.postings);
        assert!(decoded.postings.is_empty());
    }

    #[test]
    fn should_encode_and_decode_large_inverted_index_value() {
        // given
        let mut bitmap = RoaringBitmap::new();
        for i in 0..1000 {
            bitmap.insert(i * 10);
        }
        let value = InvertedIndexValue { postings: bitmap };

        // when
        let encoded = value.encode().unwrap();
        let decoded = InvertedIndexValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded.postings, value.postings);
        assert_eq!(decoded.postings.len(), 1000);
    }
}
