//! Benchmark parameter collection.

use std::collections::BTreeMap;

use timeseries::Label;

/// A collection of benchmark parameters with efficient key lookup.
///
/// Provides convenient methods for extracting and parsing parameter values.
/// Parameters are stored as string key-value pairs and can be converted
/// to/from `Vec<Label>` for use with the metrics system.
#[derive(Debug, Clone, Default)]
pub struct Params {
    inner: BTreeMap<String, String>,
}

impl Params {
    /// Create an empty Params collection.
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Get a label value by name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.inner.get(name).map(|s| s.as_str())
    }

    /// Get and parse a label value.
    pub fn get_parse<T>(&self, name: &str) -> anyhow::Result<T>
    where
        T: std::str::FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        let value = self
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("missing label: {}", name))?;
        value
            .parse()
            .map_err(|e: T::Err| anyhow::anyhow!("failed to parse label '{}': {}", name, e))
    }

    /// Insert a label.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.inner.insert(name.into(), value.into());
    }
}

impl From<Vec<Label>> for Params {
    fn from(labels: Vec<Label>) -> Self {
        Self {
            inner: labels.into_iter().map(|l| (l.name, l.value)).collect(),
        }
    }
}

impl From<&[Label]> for Params {
    fn from(labels: &[Label]) -> Self {
        Self {
            inner: labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
        }
    }
}

impl From<Params> for Vec<Label> {
    fn from(labels: Params) -> Self {
        labels
            .inner
            .into_iter()
            .map(|(name, value)| Label::new(name, value))
            .collect()
    }
}
