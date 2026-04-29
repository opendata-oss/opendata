//! Per-entry metadata envelope decoding.
//!
//! Producers attach a 4-byte header to each entry's metadata payload. The
//! shape is `{version, signal_type, encoding, reserved}`. RFC 0003 mandates
//! per-entry decoding because a single Buffer batch can carry multiple
//! ranges with different envelopes.
//!
//! For the alpha, an envelope that doesn't match the configured
//! `(signal_type, encoding)` is a fail-closed condition: the runtime halts
//! rather than ack the offending range. A future release may route per
//! entry to per-signal pipelines instead.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::source::RawBufferBatch;

const ENVELOPE_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalType {
    Metrics,
    Logs,
}

impl SignalType {
    pub fn as_byte(self) -> u8 {
        match self {
            SignalType::Metrics => 1,
            SignalType::Logs => 2,
        }
    }

    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(SignalType::Metrics),
            2 => Some(SignalType::Logs),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SignalType::Metrics => "metrics",
            SignalType::Logs => "logs",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadEncoding {
    OtlpProtobuf,
}

impl PayloadEncoding {
    pub fn as_byte(self) -> u8 {
        match self {
            PayloadEncoding::OtlpProtobuf => 1,
        }
    }

    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(PayloadEncoding::OtlpProtobuf),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            PayloadEncoding::OtlpProtobuf => "otlp_protobuf",
        }
    }
}

/// Decoded form of the 4-byte envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataEnvelope {
    pub version: u8,
    pub signal_type: SignalType,
    pub encoding: PayloadEncoding,
    pub reserved: u8,
}

/// What the running ingestor expects every entry's envelope to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfiguredEnvelope {
    pub version: u8,
    pub signal_type: SignalType,
    pub encoding: PayloadEncoding,
}

impl ConfiguredEnvelope {
    pub fn matches(&self, env: &MetadataEnvelope) -> bool {
        self.version == env.version
            && self.signal_type == env.signal_type
            && self.encoding == env.encoding
    }
}

#[derive(Debug, Error)]
pub enum EnvelopeError {
    #[error("entry {entry_index}: envelope is {len} bytes; expected at least {min}")]
    TooShort {
        entry_index: u32,
        len: usize,
        min: usize,
    },
    #[error("entry {entry_index}: unsupported version {version}")]
    UnsupportedVersion { entry_index: u32, version: u8 },
    #[error("entry {entry_index}: unsupported signal_type byte {byte}")]
    UnsupportedSignal { entry_index: u32, byte: u8 },
    #[error("entry {entry_index}: unsupported encoding byte {byte}")]
    UnsupportedEncoding { entry_index: u32, byte: u8 },
    #[error(
        "entry {entry_index}: envelope (v{version}, signal={signal}, encoding={encoding}) does not match configured (v{exp_version}, signal={exp_signal}, encoding={exp_encoding})"
    )]
    Mismatch {
        entry_index: u32,
        version: u8,
        signal: &'static str,
        encoding: &'static str,
        exp_version: u8,
        exp_signal: &'static str,
        exp_encoding: &'static str,
    },
}

/// Decode the envelope for a single entry.
fn decode_one(entry_index: u32, payload: &[u8]) -> Result<MetadataEnvelope, EnvelopeError> {
    if payload.len() < ENVELOPE_LEN {
        return Err(EnvelopeError::TooShort {
            entry_index,
            len: payload.len(),
            min: ENVELOPE_LEN,
        });
    }
    let version = payload[0];
    if version != 1 {
        return Err(EnvelopeError::UnsupportedVersion {
            entry_index,
            version,
        });
    }
    let signal_byte = payload[1];
    let signal_type =
        SignalType::from_byte(signal_byte).ok_or(EnvelopeError::UnsupportedSignal {
            entry_index,
            byte: signal_byte,
        })?;
    let encoding_byte = payload[2];
    let encoding =
        PayloadEncoding::from_byte(encoding_byte).ok_or(EnvelopeError::UnsupportedEncoding {
            entry_index,
            byte: encoding_byte,
        })?;
    Ok(MetadataEnvelope {
        version,
        signal_type,
        encoding,
        reserved: payload[3],
    })
}

/// Decode the envelope for every entry in a batch, in order.
pub fn decode_envelopes(batch: &RawBufferBatch) -> Result<Vec<MetadataEnvelope>, EnvelopeError> {
    batch
        .entries
        .iter()
        .map(|entry| decode_one(entry.entry_index, &entry.raw_metadata))
        .collect()
}

/// Validate that every envelope matches the configured signal/encoding.
pub fn validate_consistent(
    envelopes: &[MetadataEnvelope],
    configured: &ConfiguredEnvelope,
) -> Result<(), EnvelopeError> {
    for (i, env) in envelopes.iter().enumerate() {
        if !configured.matches(env) {
            return Err(EnvelopeError::Mismatch {
                entry_index: i as u32,
                version: env.version,
                signal: env.signal_type.name(),
                encoding: env.encoding.name(),
                exp_version: configured.version,
                exp_signal: configured.signal_type.name(),
                exp_encoding: configured.encoding.name(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::RawEntry;
    use bytes::Bytes;

    fn batch_with_envelopes(envelopes: Vec<&[u8]>) -> RawBufferBatch {
        RawBufferBatch {
            sequence: 1,
            manifest_path: "m".into(),
            data_object_path: "d".into(),
            entries: envelopes
                .into_iter()
                .enumerate()
                .map(|(i, env)| RawEntry {
                    entry_index: i as u32,
                    raw_bytes: Bytes::from_static(b""),
                    raw_metadata: Bytes::copy_from_slice(env),
                    ingestion_time_ms: 0,
                })
                .collect(),
        }
    }

    fn logs_otlp() -> ConfiguredEnvelope {
        ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        }
    }

    #[test]
    fn decodes_per_entry() {
        let batch = batch_with_envelopes(vec![&[1, 2, 1, 0], &[1, 1, 1, 0]]);
        let envs = decode_envelopes(&batch).expect("decode");
        assert_eq!(envs[0].signal_type, SignalType::Logs);
        assert_eq!(envs[1].signal_type, SignalType::Metrics);
    }

    #[test]
    fn rejects_short_envelope() {
        let batch = batch_with_envelopes(vec![&[1, 2]]);
        let err = decode_envelopes(&batch).unwrap_err();
        matches!(err, EnvelopeError::TooShort { .. });
    }

    #[test]
    fn rejects_unsupported_version() {
        let batch = batch_with_envelopes(vec![&[2, 2, 1, 0]]);
        let err = decode_envelopes(&batch).unwrap_err();
        matches!(err, EnvelopeError::UnsupportedVersion { .. });
    }

    #[test]
    fn rejects_unknown_signal() {
        let batch = batch_with_envelopes(vec![&[1, 99, 1, 0]]);
        let err = decode_envelopes(&batch).unwrap_err();
        matches!(err, EnvelopeError::UnsupportedSignal { .. });
    }

    #[test]
    fn rejects_unknown_encoding() {
        let batch = batch_with_envelopes(vec![&[1, 2, 99, 0]]);
        let err = decode_envelopes(&batch).unwrap_err();
        matches!(err, EnvelopeError::UnsupportedEncoding { .. });
    }

    #[test]
    fn validate_passes_when_all_match() {
        let batch = batch_with_envelopes(vec![&[1, 2, 1, 0], &[1, 2, 1, 0]]);
        let envs = decode_envelopes(&batch).expect("decode");
        validate_consistent(&envs, &logs_otlp()).expect("ok");
    }

    #[test]
    fn validate_fails_when_signal_mismatches() {
        let batch = batch_with_envelopes(vec![&[1, 2, 1, 0], &[1, 1, 1, 0]]);
        let envs = decode_envelopes(&batch).expect("decode");
        let err = validate_consistent(&envs, &logs_otlp()).unwrap_err();
        match err {
            EnvelopeError::Mismatch { entry_index, .. } => assert_eq!(entry_index, 1),
            other => panic!("expected mismatch, got {other:?}"),
        }
    }
}
