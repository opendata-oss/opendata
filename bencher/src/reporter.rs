//! Reporter trait and implementations for benchmark metrics.

use timeseries::{Series, TimeSeriesDb};

/// Reporter for benchmark metrics.
///
/// Reporters handle two types of metrics:
/// - **Ongoing metrics**: Periodic snapshots of counters, gauges, and histograms
///   recorded via [`write()`](Reporter::write) during benchmark execution.
/// - **Summary metrics**: Final results recorded via [`summarize()`](Reporter::summarize)
///   at the end of a benchmark run.
///
/// The default implementation writes both to the same destination, but reporters
/// can override this behavior (e.g., only report summaries in production mode).
#[async_trait::async_trait]
pub(crate) trait Reporter: Send + Sync {
    /// Write ongoing metric snapshots (counters, gauges, histograms).
    ///
    /// Called periodically during benchmark execution.
    async fn write(&self, series: Vec<Series>) -> anyhow::Result<()>;

    /// Write summary metrics (final results).
    ///
    /// Called at the end of a benchmark run. Default implementation delegates to `write()`.
    async fn summarize(&self, series: Vec<Series>) -> anyhow::Result<()> {
        self.write(series).await
    }

    /// Flush any buffered data to the underlying storage.
    async fn flush(&self) -> anyhow::Result<()>;
}

/// Reporter that writes metrics to a TimeSeriesDb database.
pub(crate) struct TimeSeriesReporter {
    ts: TimeSeriesDb,
}

impl TimeSeriesReporter {
    pub(crate) fn new(ts: TimeSeriesDb) -> Self {
        Self { ts }
    }
}

#[async_trait::async_trait]
impl Reporter for TimeSeriesReporter {
    async fn write(&self, series: Vec<Series>) -> anyhow::Result<()> {
        self.ts.write(series).await?;
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.ts.flush().await?;
        Ok(())
    }
}

/// In-memory reporter for testing.
///
/// Collects all written series for later inspection.
#[cfg(test)]
pub(crate) struct MemoryReporter {
    series: std::sync::Mutex<Vec<Series>>,
}

#[cfg(test)]
impl MemoryReporter {
    pub(crate) fn new() -> Self {
        Self {
            series: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Get all collected series.
    pub(crate) fn series(&self) -> Vec<Series> {
        self.series.lock().unwrap().clone()
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl Reporter for MemoryReporter {
    async fn write(&self, series: Vec<Series>) -> anyhow::Result<()> {
        self.series.lock().unwrap().extend(series);
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
