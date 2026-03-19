use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ReadLoadConfig;

#[derive(Clone)]
pub(crate) struct ReadLoadCoordinator {
    sample_permits: Arc<Semaphore>,
    metadata_permits: Arc<Semaphore>,
}

impl Default for ReadLoadCoordinator {
    fn default() -> Self {
        Self::from_config(&ReadLoadConfig::default())
    }
}

impl ReadLoadCoordinator {
    pub(crate) fn new(sample_permits: usize, metadata_permits: usize) -> Self {
        Self {
            sample_permits: Arc::new(Semaphore::new(sample_permits)),
            metadata_permits: Arc::new(Semaphore::new(metadata_permits)),
        }
    }

    /// Build from a ReadLoadConfig.
    pub(crate) fn from_config(config: &ReadLoadConfig) -> Self {
        Self::new(config.sample_permits, config.metadata_permits)
    }

    /// Build from environment variables, falling back to defaults.
    pub(crate) fn from_env() -> Self {
        Self::from_config(&ReadLoadConfig::from_env())
    }

    /// Acquire a sample permit. Returns an owned permit (held across .await) and the wait duration.
    pub(crate) async fn acquire_sample(&self) -> (OwnedSemaphorePermit, Duration) {
        let t0 = Instant::now();
        let permit = self.sample_permits.clone().acquire_owned().await.unwrap();
        (permit, t0.elapsed())
    }

    /// Acquire a metadata permit. Returns an owned permit (held across .await) and the wait duration.
    pub(crate) async fn acquire_metadata(&self) -> (OwnedSemaphorePermit, Duration) {
        let t0 = Instant::now();
        let permit = self.metadata_permits.clone().acquire_owned().await.unwrap();
        (permit, t0.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn default_creates_expected_permits() {
        let coord = ReadLoadCoordinator::default();
        assert_eq!(coord.sample_permits.available_permits(), 32);
        assert_eq!(coord.metadata_permits.available_permits(), 16);
    }

    #[test]
    fn custom_permits() {
        let coord = ReadLoadCoordinator::new(4, 2);
        assert_eq!(coord.sample_permits.available_permits(), 4);
        assert_eq!(coord.metadata_permits.available_permits(), 2);
    }

    #[tokio::test]
    async fn acquire_returns_permit_and_releases_on_drop() {
        let coord = ReadLoadCoordinator::new(2, 1);

        let (p1, _wait) = coord.acquire_sample().await;
        assert_eq!(coord.sample_permits.available_permits(), 1);

        let (p2, _wait) = coord.acquire_sample().await;
        assert_eq!(coord.sample_permits.available_permits(), 0);

        drop(p1);
        assert_eq!(coord.sample_permits.available_permits(), 1);

        drop(p2);
        assert_eq!(coord.sample_permits.available_permits(), 2);
    }

    #[tokio::test]
    async fn semaphore_bounds_concurrency() {
        let coord = Arc::new(ReadLoadCoordinator::new(2, 1));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = coord.clone();
            let cur = current.clone();
            let max = max_concurrent.clone();
            handles.push(tokio::spawn(async move {
                let (_permit, _wait) = c.acquire_sample().await;
                let val = cur.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(val, Ordering::SeqCst);
                tokio::task::yield_now().await;
                cur.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert!(max_concurrent.load(Ordering::SeqCst) <= 2);
    }

    #[tokio::test]
    async fn sample_and_metadata_pools_are_independent() {
        let coord = ReadLoadCoordinator::new(1, 1);

        // Exhaust sample pool
        let (_sp, _) = coord.acquire_sample().await;
        assert_eq!(coord.sample_permits.available_permits(), 0);

        // Metadata pool is still available
        let (_mp, _) = coord.acquire_metadata().await;
        assert_eq!(coord.metadata_permits.available_permits(), 0);
    }

    #[test]
    fn clone_shares_semaphores() {
        let coord = ReadLoadCoordinator::new(4, 2);
        let coord2 = coord.clone();
        assert!(Arc::ptr_eq(&coord.sample_permits, &coord2.sample_permits));
        assert!(Arc::ptr_eq(
            &coord.metadata_permits,
            &coord2.metadata_permits
        ));
    }
}
