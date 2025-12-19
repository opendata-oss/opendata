//! Prometheus-style target scraper.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;

use super::config::{PrometheusConfig, ScrapeConfig};
use super::metrics::{Metrics, ScrapeLabels};
use super::openmetrics::parse_openmetrics;
use crate::model::{Attribute, MetricType, Sample, SampleWithAttributes};
use crate::tsdb::Tsdb;
use crate::util::OpenTsdbError;
use crate::util::Result;

/// Scraper that periodically fetches metrics from configured targets.
pub struct Scraper {
    tsdb: Arc<Tsdb>,
    http_client: reqwest::Client,
    config: PrometheusConfig,
    metrics: Arc<Metrics>,
}

impl Scraper {
    /// Create a new scraper with the given TSDB, configuration, and metrics registry.
    pub fn new(tsdb: Arc<Tsdb>, config: PrometheusConfig, metrics: Arc<Metrics>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            tsdb,
            http_client,
            config,
            metrics,
        }
    }

    /// Start scraping all configured targets.
    /// This spawns a background task for each scrape job.
    pub fn run(self: Arc<Self>) {
        for scrape_config in &self.config.scrape_configs {
            let scraper = Arc::clone(&self);
            let job_config = scrape_config.clone();
            let global_config = self.config.global.clone();

            tokio::spawn(async move {
                scraper.run_job(job_config, global_config).await;
            });
        }
    }

    /// Run a single scrape job, scraping all its targets at the configured interval.
    async fn run_job(&self, job_config: ScrapeConfig, global_config: super::config::GlobalConfig) {
        let scrape_interval = job_config.effective_interval(&global_config);
        let job_name = job_config.job_name.clone();

        tracing::info!(
            "Starting scrape job '{}' with interval {:?}",
            job_name,
            scrape_interval
        );

        let mut ticker = interval(scrape_interval);

        loop {
            ticker.tick().await;

            for static_config in &job_config.static_configs {
                for target in &static_config.targets {
                    if let Err(e) = self
                        .scrape_target(&job_name, target, &static_config.labels)
                        .await
                    {
                        tracing::warn!(
                            "Failed to scrape target {} for job {}: {}",
                            target,
                            job_name,
                            e
                        );
                    }
                }
            }
        }
    }

    /// Scrape a single target and ingest the metrics.
    async fn scrape_target(
        &self,
        job_name: &str,
        target: &str,
        extra_labels: &HashMap<String, String>,
    ) -> Result<()> {
        let scrape_labels = ScrapeLabels {
            job: job_name.to_string(),
            instance: target.to_string(),
        };

        let result = self.do_scrape_target(job_name, target, extra_labels).await;

        // Create and ingest the `up` metric (1 = success, 0 = failure)
        let up_value = if result.is_ok() { 1.0 } else { 0.0 };
        let up_sample = self.create_up_sample(job_name, target, up_value);
        if let Err(e) = self.ingest_samples(vec![up_sample]).await {
            tracing::warn!(
                "Failed to ingest up metric for {}/{}: {}",
                job_name,
                target,
                e
            );
        }

        match &result {
            Ok(sample_count) => {
                // Record samples scraped
                self.metrics
                    .scrape_samples_scraped
                    .get_or_create(&scrape_labels)
                    .inc_by(*sample_count as u64);
            }
            Err(_) => {
                // Record failed scrape
                self.metrics
                    .scrape_samples_failed
                    .get_or_create(&scrape_labels)
                    .inc();
            }
        }

        result.map(|_| ())
    }

    /// Create an `up` sample for a target.
    fn create_up_sample(&self, job_name: &str, target: &str, value: f64) -> SampleWithAttributes {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        SampleWithAttributes {
            attributes: vec![
                Attribute {
                    key: "__name__".to_string(),
                    value: "up".to_string(),
                },
                Attribute {
                    key: "job".to_string(),
                    value: job_name.to_string(),
                },
                Attribute {
                    key: "instance".to_string(),
                    value: target.to_string(),
                },
            ],
            metric_unit: None,
            metric_type: MetricType::Gauge,
            sample: Sample {
                timestamp: timestamp_ms,
                value,
            },
        }
    }

    /// Internal scrape implementation that returns sample count on success.
    async fn do_scrape_target(
        &self,
        job_name: &str,
        target: &str,
        extra_labels: &HashMap<String, String>,
    ) -> Result<usize> {
        let url = format!("http://{}/metrics", target);

        tracing::debug!("Scraping {} for job {}", url, job_name);

        let response = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| OpenTsdbError::Internal(format!("HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(OpenTsdbError::Internal(format!(
                "HTTP {} from {}",
                response.status(),
                url
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|e| OpenTsdbError::Internal(format!("Failed to read response body: {}", e)))?;

        // Parse the OpenMetrics/Prometheus format
        let mut samples = parse_openmetrics(&body)?;

        // Add job and instance labels to all samples
        for sample in &mut samples {
            // Add job label
            sample.attributes.push(Attribute {
                key: "job".to_string(),
                value: job_name.to_string(),
            });

            // Add instance label
            sample.attributes.push(Attribute {
                key: "instance".to_string(),
                value: target.to_string(),
            });

            // Add any extra labels from static_config
            for (key, value) in extra_labels {
                sample.attributes.push(Attribute {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
        }

        // Ingest the samples
        let sample_count = samples.len();
        self.ingest_samples(samples).await?;

        tracing::debug!(
            "Successfully scraped {} metrics from {} for job {}",
            sample_count,
            target,
            job_name
        );

        Ok(sample_count)
    }

    /// Ingest samples into the TSDB.
    async fn ingest_samples(&self, samples: Vec<SampleWithAttributes>) -> Result<()> {
        self.tsdb.ingest_samples(samples).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_scraper() {
        // given
        let storage = Arc::new(
            opendata_common::storage::in_memory::InMemoryStorage::with_merge_operator(Arc::new(
                crate::storage::merge_operator::OpenTsdbMergeOperator,
            )),
        );
        let tsdb = Arc::new(Tsdb::new(storage));
        let config = PrometheusConfig::default();
        let metrics = Arc::new(Metrics::new());

        // when
        let scraper = Scraper::new(tsdb, config, metrics);

        // then
        assert!(scraper.config.scrape_configs.is_empty());
    }
}
