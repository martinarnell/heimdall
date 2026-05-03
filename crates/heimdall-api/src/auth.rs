//! Auth wiring for the API binary.
//!
//! Bridges [`heimdall_auth`] into Axum middleware + bootstrapping.
//!
//! Configuration (env):
//! - `HEIMDALL_KEY_PEPPER` — hex-encoded 32-byte HMAC pepper. **If unset,
//!   auth is disabled** and the API behaves identically to today (no key
//!   verification, no rate limiting).
//! - `HEIMDALL_AUTH_DB`   — auth.db path, default `/var/lib/heimdall/auth.db`.
//! - `HEIMDALL_ANON_PER_MINUTE` — anonymous IP limit, default 60.
//! - `HEIMDALL_AUTH_REFRESH_SECS` — cache refresh, default 30.
//! - `HEIMDALL_AUTH_FLUSH_SECS`   — usage flush, default 60.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use heimdall_auth::{
    cache::AuthCache,
    limiter::AnonLimiter,
    service::{AuthDecision, AuthService, AuthVerdict},
    store::KeyStore,
    tier::ANON_RATE_PER_MINUTE,
    usage,
};
use metrics::counter;
use serde_json::json;

pub const DEFAULT_DB_PATH: &str = "/var/lib/heimdall/auth.db";

#[derive(Clone)]
pub struct AuthHandle {
    pub service: Arc<AuthService>,
    /// Held to keep the DB connection alive for the lifetime of the process
    /// and to be the future home of admin-triggered cache reloads.
    #[allow(dead_code)]
    pub store: Arc<KeyStore>,
}

/// Initialize auth from env. Returns `Ok(None)` if disabled (pepper unset).
pub fn init_from_env() -> anyhow::Result<Option<AuthHandle>> {
    let pepper_hex = match std::env::var("HEIMDALL_KEY_PEPPER") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            tracing::warn!(
                "HEIMDALL_KEY_PEPPER is unset — running without authentication. \
                 Set it (and HEIMDALL_AUTH_DB) to enable API key auth."
            );
            return Ok(None);
        }
    };
    let pepper = hex::decode(pepper_hex.trim())
        .map_err(|e| anyhow::anyhow!("HEIMDALL_KEY_PEPPER must be hex: {e}"))?;
    if pepper.len() < 16 {
        anyhow::bail!(
            "HEIMDALL_KEY_PEPPER too short ({} bytes); need ≥ 16",
            pepper.len()
        );
    }

    let db_path: PathBuf = std::env::var("HEIMDALL_AUTH_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_DB_PATH));

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let store = Arc::new(KeyStore::open(&db_path)?);
    let cache = Arc::new(AuthCache::new());

    // Seed the cache synchronously at startup.
    {
        let store_ref = Arc::clone(&store);
        let now = unix_seconds();
        let map = usage::build_cache_map(&store_ref, now)?;
        cache.replace(map);
    }

    let anon_per_minute: u32 = std::env::var("HEIMDALL_ANON_PER_MINUTE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(ANON_RATE_PER_MINUTE);

    let refresh_secs: u64 = std::env::var("HEIMDALL_AUTH_REFRESH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let flush_secs: u64 = std::env::var("HEIMDALL_AUTH_FLUSH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let service = Arc::new(AuthService::new(
        Arc::clone(&cache),
        AnonLimiter::new(anon_per_minute),
        pepper,
    ));

    usage::spawn_background(
        Arc::clone(&cache),
        Arc::clone(&store),
        Duration::from_secs(refresh_secs),
        Duration::from_secs(flush_secs),
    );

    tracing::info!(
        path = %db_path.display(),
        keys = cache.len(),
        anon_per_minute,
        "auth subsystem initialized"
    );

    Ok(Some(AuthHandle { service, store }))
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Axum middleware. Skips `/status`, `/metrics`, `/`. All other endpoints go
/// through the verify path. If auth is disabled (handle `None`), this layer
/// is a no-op — bypass it via `from_fn_with_state` only when `handle` is set.
pub async fn middleware(
    State(handle): State<AuthHandle>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let path = req.uri().path();
    if matches!(path, "/" | "/status" | "/metrics") {
        return next.run(req).await;
    }

    let candidate = extract_key(req.headers(), req.uri().query());
    let ip = client_ip(req.headers(), addr);

    let verdict = handle.service.verify(candidate.as_deref(), ip);

    let outcome = decision_label(verdict.decision);
    counter!("heimdall_auth_requests_total", "outcome" => outcome).increment(1);

    match verdict.decision {
        AuthDecision::KeyOk | AuthDecision::Anonymous => {
            let mut resp = next.run(req).await;
            apply_rate_headers(&mut resp, &verdict);
            // Track 5xx as errors against the key's daily counter.
            if resp.status().is_server_error() {
                if let Some(entry) = &verdict.entry {
                    handle.service.record_error(entry);
                }
            }
            resp
        }
        AuthDecision::UnknownKey => deny(StatusCode::UNAUTHORIZED, "unknown_api_key", "API key not recognized"),
        AuthDecision::Malformed => deny(StatusCode::UNAUTHORIZED, "malformed_api_key", "API key is malformed"),
        AuthDecision::Forbidden => deny(StatusCode::FORBIDDEN, "key_revoked", "API key has been revoked"),
        AuthDecision::RateLimited => {
            let mut resp = deny(StatusCode::TOO_MANY_REQUESTS, "rate_limit", "rate limit exceeded");
            resp.headers_mut().insert("Retry-After", HeaderValue::from_static("1"));
            apply_rate_headers(&mut resp, &verdict);
            resp
        }
        AuthDecision::OverQuota => {
            let mut resp = deny(StatusCode::TOO_MANY_REQUESTS, "quota_exceeded", "monthly quota exceeded");
            apply_rate_headers(&mut resp, &verdict);
            resp
        }
        AuthDecision::AnonThrottled => {
            let mut resp = deny(StatusCode::TOO_MANY_REQUESTS, "anon_rate_limit",
                "anonymous IP rate limit exceeded — get an API key for higher limits");
            resp.headers_mut().insert("Retry-After", HeaderValue::from_static("60"));
            resp
        }
    }
}

fn decision_label(d: AuthDecision) -> &'static str {
    match d {
        AuthDecision::KeyOk => "ok",
        AuthDecision::Anonymous => "anonymous",
        AuthDecision::UnknownKey => "unknown_key",
        AuthDecision::Malformed => "malformed",
        AuthDecision::Forbidden => "forbidden",
        AuthDecision::RateLimited => "rate_limited",
        AuthDecision::OverQuota => "over_quota",
        AuthDecision::AnonThrottled => "anon_throttled",
    }
}

fn deny(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({ "error": code, "message": message })),
    )
        .into_response()
}

fn apply_rate_headers(resp: &mut Response, verdict: &AuthVerdict) {
    if let Some(entry) = &verdict.entry {
        let limit = entry.rate_limit_rps;
        let used = entry.month_used.load(std::sync::atomic::Ordering::Relaxed);
        let remaining = entry.monthly_quota.saturating_sub(used);
        let h = resp.headers_mut();
        let _ = h.insert("X-RateLimit-Limit", HeaderValue::from_str(&limit.to_string()).unwrap());
        let _ = h.insert("X-Quota-Limit",
            HeaderValue::from_str(&entry.monthly_quota.to_string()).unwrap());
        let _ = h.insert("X-Quota-Remaining",
            HeaderValue::from_str(&remaining.to_string()).unwrap());
    }
}

/// Extract `Authorization: Bearer <key>` or `?api_key=<key>` (query string).
/// Headers take precedence; if neither is present, returns `None`.
fn extract_key(headers: &HeaderMap, query: Option<&str>) -> Option<String> {
    if let Some(v) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(s) = v.to_str() {
            if let Some(rest) = s.strip_prefix("Bearer ") {
                return Some(rest.trim().to_string());
            }
        }
    }
    if let Some(v) = headers.get("X-API-Key") {
        if let Ok(s) = v.to_str() {
            return Some(s.trim().to_string());
        }
    }
    if let Some(q) = query {
        for pair in q.split('&') {
            if let Some(rest) = pair.strip_prefix("api_key=") {
                return urldecode(rest);
            }
        }
    }
    None
}

fn urldecode(s: &str) -> Option<String> {
    // We only need to handle %xx escapes for the API key value; keys are
    // alphanumeric+underscore so this is largely defensive.
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && i + 2 < bytes.len() {
            let h = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
            let v = u8::from_str_radix(h, 16).ok()?;
            out.push(v as char);
            i += 3;
        } else if b == b'+' {
            out.push(' ');
            i += 1;
        } else {
            out.push(b as char);
            i += 1;
        }
    }
    Some(out)
}

/// Resolve the originating client IP. Behind Cloudflare → Caddy, the trusted
/// chain is `CF-Connecting-IP` (Cloudflare) → `X-Forwarded-For` (Caddy) →
/// peer socket. We trust `CF-Connecting-IP` first; if absent, the leftmost
/// element of `X-Forwarded-For`; finally fall back to the socket address.
fn client_ip(headers: &HeaderMap, addr: SocketAddr) -> IpAddr {
    if let Some(v) = headers.get("CF-Connecting-IP").and_then(|h| h.to_str().ok()) {
        if let Ok(ip) = v.trim().parse() { return ip; }
    }
    if let Some(v) = headers.get("X-Forwarded-For").and_then(|h| h.to_str().ok()) {
        if let Some(first) = v.split(',').next() {
            if let Ok(ip) = first.trim().parse() { return ip; }
        }
    }
    addr.ip().to_canonical().is_loopback()
        .then(|| IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))
        .unwrap_or(addr.ip())
}
