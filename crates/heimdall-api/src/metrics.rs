//! Prometheus metrics for Heimdall API.
//!
//! Exposes a `/metrics` endpoint in Prometheus text format.
//! All metrics are in-memory counters/histograms — no persistence.

use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::time::Instant;

use crate::AppState;

/// Install the Prometheus metrics recorder and return the handle
/// used to render `/metrics` output.
pub fn init() -> PrometheusHandle {
    PrometheusBuilder::new()
        .set_buckets(&[
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ])
        .expect("valid buckets")
        .install_recorder()
        .expect("failed to install metrics recorder")
}

/// Record index-level gauges once at startup.
pub fn record_index_info(countries: usize, total_places: usize, total_addresses: usize) {
    gauge!("heimdall_index_countries_loaded").set(countries as f64);
    gauge!("heimdall_index_places_total").set(total_places as f64);
    gauge!("heimdall_index_addresses_total").set(total_addresses as f64);
}

/// Axum middleware that records request metrics.
pub async fn track(req: Request<Body>, next: Next) -> Response {
    let path = req.uri().path().to_owned();

    // Skip metrics endpoint itself to avoid recursion
    if path == "/metrics" {
        return next.run(req).await;
    }

    let endpoint = normalize_endpoint(&path);
    counter!("heimdall_requests_total", "endpoint" => endpoint.clone()).increment(1);
    gauge!("heimdall_requests_in_flight", "endpoint" => endpoint.clone()).increment(1.0);

    let start = Instant::now();
    let response = next.run(req).await;
    let duration = start.elapsed().as_secs_f64();

    gauge!("heimdall_requests_in_flight", "endpoint" => endpoint.clone()).decrement(1.0);

    let status = response.status().as_u16().to_string();
    histogram!("heimdall_request_duration_seconds", "endpoint" => endpoint.clone(), "status" => status.clone())
        .record(duration);

    if response.status().is_server_error() {
        counter!("heimdall_errors_total", "endpoint" => endpoint, "status" => status).increment(1);
    }

    response
}

/// Record the number of results returned by a search/autocomplete/lookup.
pub fn record_result_count(endpoint: &str, count: usize) {
    histogram!("heimdall_search_results", "endpoint" => endpoint.to_owned())
        .record(count as f64);
}

/// Record which country was queried.
pub fn record_country_hit(country_code: &str) {
    counter!("heimdall_country_requests_total", "country" => country_code.to_owned()).increment(1);
}

/// Handler for GET /metrics
pub async fn handler(
    State(state): State<std::sync::Arc<AppState>>,
) -> impl IntoResponse {
    let body = state.metrics_handle.render();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

fn normalize_endpoint(path: &str) -> String {
    match path {
        "/search" => "search".to_owned(),
        "/autocomplete" => "autocomplete".to_owned(),
        "/reverse" => "reverse".to_owned(),
        "/lookup" => "lookup".to_owned(),
        "/status" => "status".to_owned(),
        _ => "other".to_owned(),
    }
}
