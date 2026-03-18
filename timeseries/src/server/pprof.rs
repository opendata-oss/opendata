//! CPU profiling endpoint using pprof-rs.
//!
//! Provides `/debug/pprof/profile` which captures a CPU profile for a specified
//! duration and returns a flamegraph SVG.

use axum::extract::Query;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

#[derive(serde::Deserialize)]
pub(crate) struct ProfileParams {
    /// Duration to profile in seconds (default: 30, max: 120)
    #[serde(default = "default_seconds")]
    seconds: u64,
    /// Sampling frequency in Hz (default: 99)
    #[serde(default = "default_frequency")]
    frequency: i32,
}

fn default_seconds() -> u64 {
    30
}

fn default_frequency() -> i32 {
    99
}

/// GET /debug/pprof/profile?seconds=30&frequency=99
///
/// Captures a CPU profile for the specified duration and returns an SVG flamegraph.
/// The request blocks for the profiling duration.
pub(crate) async fn handle_pprof_profile(
    Query(params): Query<ProfileParams>,
) -> Result<Response, (StatusCode, String)> {
    let seconds = params.seconds.clamp(1, 120);
    let frequency = params.frequency.clamp(1, 999);

    tracing::info!(seconds, frequency, "starting CPU profile capture");

    // Start the profiler
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(frequency)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| {
            tracing::error!("failed to start profiler: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to start profiler: {e}"),
            )
        })?;

    // Profile for the requested duration
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;

    // Build the report
    let report = guard.report().build().map_err(|e| {
        tracing::error!("failed to build profile report: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to build report: {e}"),
        )
    })?;

    // Generate flamegraph SVG
    let mut body = Vec::new();
    report.flamegraph(&mut body).map_err(|e| {
        tracing::error!("failed to generate flamegraph: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to generate flamegraph: {e}"),
        )
    })?;

    tracing::info!(seconds, bytes = body.len(), "CPU profile capture complete");

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "image/svg+xml")],
        body,
    )
        .into_response())
}
