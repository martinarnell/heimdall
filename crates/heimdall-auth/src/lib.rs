//! Heimdall API key authentication, rate limiting, and usage tracking.
//!
//! See `devops/auth/plan.md` for the full architecture rationale.
//!
//! Public surface:
//! - [`key`] — key format, generation, hashing
//! - [`store`] — SQLite-backed users / keys / usage_daily
//! - [`tier`] — tier defaults
//! - [`cache`] — in-memory verification cache (the hot path)
//! - [`limiter`] — anonymous IP token bucket
//! - [`usage`] — atomic counters + batched flush
//! - [`service`] — top-level AuthService that composes the above

pub mod cache;
pub mod key;
pub mod limiter;
pub mod migrate;
pub mod service;
pub mod store;
pub mod tier;
pub mod usage;

pub use cache::{AuthCache, KeyEntry};
pub use key::{generate_key, hash_key, ApiKey, KeyHash, KEY_PREFIX_LIVE, KEY_PREFIX_TEST};
pub use service::{AuthDecision, AuthError, AuthService, AuthVerdict};
pub use store::{ApiKeyRow, KeyStore, NewKey, UserRow};
pub use tier::Tier;
