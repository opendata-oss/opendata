// SeriesDictionary value structure

use super::*;
use crate::model::SeriesId;
use bytes::{Bytes, BytesMut};

/// SeriesDictionary value: SingleArray<series_id: u32>
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesDictionaryValue {
    pub series_ids: Vec<SeriesId>,
}

impl SeriesDictionaryValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        encode_single_array(&self.series_ids, &mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8], count: u8) -> Result<Self, EncodingError> {
        let mut slice = buf;
        let series_ids = decode_single_array(&mut slice, count as usize)?;
        Ok(SeriesDictionaryValue { series_ids })
    }
}

impl Encode for SeriesId {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for SeriesId {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 4 {
            return Err(EncodingError {
                message: "Buffer too short for SeriesId".to_string(),
            });
        }
        let id = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        *buf = &buf[4..];
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_series_dictionary_value() {
        // given
        let value = SeriesDictionaryValue {
            series_ids: vec![1, 2, 3, 42, 99],
        };

        // when
        let encoded = value.encode();
        let decoded = SeriesDictionaryValue::decode(encoded.as_ref(), 5u8).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_empty_series_dictionary_value() {
        // given
        let value = SeriesDictionaryValue { series_ids: vec![] };

        // when
        let encoded = value.encode();
        let decoded = SeriesDictionaryValue::decode(encoded.as_ref(), 0u8).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_single_series_id() {
        // given
        let value = SeriesDictionaryValue {
            series_ids: vec![12345],
        };

        // when
        let encoded = value.encode();
        let decoded = SeriesDictionaryValue::decode(encoded.as_ref(), 1u8).unwrap();

        // then
        assert_eq!(decoded, value);
    }
}
