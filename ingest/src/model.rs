use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValueEntry {
    #[serde_as(as = "Base64")]
    pub key: Bytes,
    #[serde_as(as = "Base64")]
    pub value: Bytes,
}
