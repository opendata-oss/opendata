//! HTTP server that exposes `/metrics` (Prometheus text format) and
//! `/-/healthy` for the ingestor binary.
//!
//! Lives in the lib (rather than inline in `bin/`) so tests can drive
//! the same routes the binary serves.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Build the axum router that serves `/metrics` and `/-/healthy`.
///
/// Factored out so tests can drive the same routes the binary serves.
pub fn router(handle: PrometheusHandle) -> Router {
    Router::new()
        .route(
            "/metrics",
            get({
                let handle = handle.clone();
                move || {
                    let handle = handle.clone();
                    async move { handle.render() }
                }
            }),
        )
        .route("/-/healthy", get(|| async { (StatusCode::OK, "OK") }))
}

/// Bind on `addr` and serve until the shutdown token is cancelled.
pub async fn serve(
    handle: PrometheusHandle,
    addr: SocketAddr,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding metrics server to {addr}"))?;
    info!(%addr, "metrics server listening");
    axum::serve(listener, router(handle))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .context("metrics server error")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use axum::http::StatusCode;
    use metrics::counter;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::serve;

    /// Contract test for `counter!() -> handle.render()`. The binary
    /// relies on this round trip for every gauge/counter the runtime
    /// emits, so a regression where the handle and recorder get
    /// disconnected (e.g. by accidentally rebuilding one) needs to fail
    /// the test, not surface in prod as an empty `/metrics` response.
    #[tokio::test]
    async fn handle_renders_recorded_counter_value() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            counter!("ingestor_test_round_trip_total").increment(7);
        });
        let rendered = handle.render();
        assert!(
            rendered.contains("# TYPE ingestor_test_round_trip_total counter"),
            "missing TYPE line: {rendered}",
        );
        assert!(
            rendered.contains("ingestor_test_round_trip_total 7"),
            "missing exact value line: {rendered}",
        );
    }

    /// End-to-end: bind the real `serve` on an ephemeral port, hit
    /// `/metrics` over the wire, and confirm a counter recorded against
    /// the same recorder shows up in the response with the exact value.
    /// `/-/healthy` returns 200/"OK".
    #[tokio::test]
    async fn serves_metrics_and_health_over_http() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        // Record into this recorder before the server starts. The
        // handle is decoupled from the recorder via Arc, so dropping
        // the recorder after this scope still leaves the handle with a
        // live snapshot of the registry.
        metrics::with_local_recorder(&recorder, || {
            counter!("ingestor_test_http_total").increment(42);
        });

        // Pick an ephemeral port. Bind once to claim a free one, then
        // drop so `serve` can rebind. Two-step because axum::serve
        // wants to own the listener.
        let probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr: SocketAddr = probe.local_addr().unwrap();
        drop(probe);

        let shutdown = CancellationToken::new();
        let serve_shutdown = shutdown.clone();
        let server = tokio::spawn(async move { serve(handle, addr, serve_shutdown).await });

        let client = reqwest::Client::new();
        let url_metrics = format!("http://{addr}/metrics");
        let url_health = format!("http://{addr}/-/healthy");

        // Briefly retry the first GET while the listener is coming up.
        let body = timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(resp) = client.get(&url_metrics).send().await
                    && resp.status().is_success()
                {
                    return resp.text().await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("metrics endpoint reachable within 2s");

        assert!(
            body.contains("ingestor_test_http_total 42"),
            "rendered body missing recorded counter: {body}",
        );

        let health = client.get(&url_health).send().await.unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        assert_eq!(health.text().await.unwrap(), "OK");

        shutdown.cancel();
        let _ = timeout(Duration::from_secs(2), server).await;
    }
}
