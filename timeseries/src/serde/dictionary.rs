// SeriesDictionary value structure

use super::*;
use crate::model::SeriesId;
use bytes::{Bytes, BytesMut};

/// SeriesDictionary value: series_id: u32
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeriesDictionaryValue {
    pub series_id: SeriesId,
}

impl SeriesDictionaryValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.series_id.encode(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut slice = buf;
        let series_id = SeriesId::decode(&mut slice)?;
        Ok(SeriesDictionaryValue { series_id })
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
        let value = SeriesDictionaryValue { series_id: 12345 };

        // when
        let encoded = value.encode();
        let decoded = SeriesDictionaryValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_different_series_ids() {
        // given
        let value = SeriesDictionaryValue { series_id: 42 };

        // when
        let encoded = value.encode();
        let decoded = SeriesDictionaryValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
        assert_eq!(decoded.series_id, 42);
    }
}
