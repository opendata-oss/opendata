//! Exporter trait and implementations for benchmark metrics.

use timeseries::{Series, TimeSeries};

/// Exporter for benchmark metrics.
///
/// This trait mirrors the `TimeSeries::write` API, allowing metrics to be
/// exported to different backends (CSV, TimeSeries, etc.).
#[async_trait::async_trait]
pub(crate) trait Exporter: Send + Sync {
    /// Write one or more series.
    async fn write(&self, series: Vec<Series>) -> anyhow::Result<()>;

    /// Flush any buffered data to the underlying storage.
    async fn flush(&self) -> anyhow::Result<()>;
}

/// Stub exporter that does nothing (for development).
pub(crate) struct StubExporter;

#[async_trait::async_trait]
impl Exporter for StubExporter {
    async fn write(&self, _series: Vec<Series>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Exporter that writes metrics to a TimeSeries database.
pub(crate) struct TimeSeriesExporter {
    ts: TimeSeries,
}

impl TimeSeriesExporter {
    pub(crate) fn new(ts: TimeSeries) -> Self {
        Self { ts }
    }
}

#[async_trait::async_trait]
impl Exporter for TimeSeriesExporter {
    async fn write(&self, series: Vec<Series>) -> anyhow::Result<()> {
        self.ts.write(series).await?;
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.ts.flush().await?;
        Ok(())
    }
}
