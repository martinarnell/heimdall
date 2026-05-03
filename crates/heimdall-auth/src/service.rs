//! Top-level [`AuthService`] — composes the cache, anonymous limiter, and
//! usage tracker into one verdict per request.
//!
//! The hot path:
//! 1. Caller extracts a candidate key string + IP from the request.
//! 2. `AuthService::verify` runs the prefix check, hashes (HMAC), looks up
//!    in the cache, runs the rate limiter, consumes a quota slot, and returns
//!    an [`AuthVerdict`].
//! 3. Caller renders the verdict to an HTTP response.

use std::net::IpAddr;
use std::sync::Arc;

use thiserror::Error;

use crate::cache::{AuthCache, KeyEntry, try_consume_quota};
use crate::key::{KeyHash, hash_key, looks_like_key};
use crate::limiter::AnonLimiter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthDecision {
    /// Authenticated request — caller should serve it and record usage.
    KeyOk,
    /// No key supplied; allowed under the anonymous IP-bucket quota.
    Anonymous,
    /// Key not present in cache (unknown / never existed / fresh revocation
    /// not yet seen).
    UnknownKey,
    /// User is blocked or key is revoked (cache knows).
    Forbidden,
    /// Per-key RPS limit hit.
    RateLimited,
    /// Monthly quota exceeded for the key.
    OverQuota,
    /// Anonymous IP rate limit hit.
    AnonThrottled,
    /// Malformed key string.
    Malformed,
}

#[derive(Clone)]
pub struct AuthVerdict {
    pub decision: AuthDecision,
    /// Cached key entry — set only when `decision == KeyOk` (or when the
    /// decision concerns a known key, e.g. RateLimited / OverQuota / Forbidden).
    pub entry: Option<Arc<KeyEntry>>,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("auth db error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

pub struct AuthService {
    pub cache: Arc<AuthCache>,
    pub anon: AnonLimiter,
    pub pepper: Vec<u8>,
}

impl AuthService {
    pub fn new(cache: Arc<AuthCache>, anon: AnonLimiter, pepper: Vec<u8>) -> Self {
        Self { cache, anon, pepper }
    }

    /// Verify a request. `candidate` is the raw key string from
    /// `Authorization: Bearer …` or `?api_key=…`; pass `None` for
    /// no-key (anonymous) requests.
    pub fn verify(&self, candidate: Option<&str>, ip: IpAddr) -> AuthVerdict {
        match candidate {
            None => self.verify_anonymous(ip),
            Some(s) => self.verify_keyed(s),
        }
    }

    fn verify_anonymous(&self, ip: IpAddr) -> AuthVerdict {
        if self.anon.check(ip).is_err() {
            return AuthVerdict { decision: AuthDecision::AnonThrottled, entry: None };
        }
        AuthVerdict { decision: AuthDecision::Anonymous, entry: None }
    }

    fn verify_keyed(&self, candidate: &str) -> AuthVerdict {
        if !looks_like_key(candidate) {
            return AuthVerdict { decision: AuthDecision::Malformed, entry: None };
        }
        let hash: KeyHash = hash_key(&self.pepper, candidate);
        let entry = match self.cache.lookup(&hash) {
            Some(e) => e,
            None => return AuthVerdict { decision: AuthDecision::UnknownKey, entry: None },
        };
        if entry.blocked.load(std::sync::atomic::Ordering::Relaxed) {
            return AuthVerdict { decision: AuthDecision::Forbidden, entry: Some(entry) };
        }
        if entry.limiter.check().is_err() {
            return AuthVerdict { decision: AuthDecision::RateLimited, entry: Some(entry) };
        }
        if try_consume_quota(&entry).is_err() {
            return AuthVerdict { decision: AuthDecision::OverQuota, entry: Some(entry) };
        }
        AuthVerdict { decision: AuthDecision::KeyOk, entry: Some(entry) }
    }

    /// Record an error for an authenticated request — increments the
    /// `pending_errors` counter so the next flush includes it.
    pub fn record_error(&self, entry: &KeyEntry) {
        entry
            .pending_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::AuthCache;
    use crate::key::generate_key;
    use crate::store::{KeyStore, NewKey};
    use crate::usage::build_cache_map;
    use std::net::Ipv4Addr;

    fn build(quota: u64, rps: u32) -> (AuthService, String) {
        let store = KeyStore::open_in_memory().unwrap();
        let uid = store.upsert_user("a@b.com", "free").unwrap();
        let k = generate_key(b"pep");
        store
            .insert_key(NewKey {
                user_id: uid,
                key_hash: k.hash,
                key_prefix: k.prefix.clone(),
                key_last4: k.last4.clone(),
                name: None,
            })
            .unwrap();
        let kid: i64 = store
            .list_keys_for_user(uid)
            .unwrap()
            .first()
            .unwrap()
            .id;
        store.set_monthly_quota(kid, Some(quota)).unwrap();
        store.set_rate_limit(kid, Some(rps)).unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let map = build_cache_map(&store, now).unwrap();
        let cache = Arc::new(AuthCache::new());
        cache.replace(map);
        let svc = AuthService::new(cache, AnonLimiter::new(60), b"pep".to_vec());
        (svc, k.full)
    }

    #[test]
    fn keyed_request_succeeds() {
        let (svc, key) = build(100, 100);
        let v = svc.verify(Some(&key), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(v.decision, AuthDecision::KeyOk);
    }

    #[test]
    fn unknown_key_rejected() {
        let (svc, _) = build(100, 100);
        let v = svc.verify(
            Some("hk_live_AAAAAAAAAAAAAAAAAAAAAA"),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
        );
        assert_eq!(v.decision, AuthDecision::UnknownKey);
    }

    #[test]
    fn malformed_key_rejected() {
        let (svc, _) = build(100, 100);
        let v = svc.verify(Some("garbage"), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(v.decision, AuthDecision::Malformed);
    }

    #[test]
    fn quota_enforced() {
        let (svc, key) = build(2, 100);
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert_eq!(svc.verify(Some(&key), ip).decision, AuthDecision::KeyOk);
        assert_eq!(svc.verify(Some(&key), ip).decision, AuthDecision::KeyOk);
        assert_eq!(svc.verify(Some(&key), ip).decision, AuthDecision::OverQuota);
    }

    #[test]
    fn anonymous_allowed_then_throttled() {
        let store = KeyStore::open_in_memory().unwrap();
        let _ = store; // unused, just to exercise the no-key path.
        let cache = Arc::new(AuthCache::new());
        let svc = AuthService::new(cache, AnonLimiter::new(2), b"pep".to_vec());
        let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        assert_eq!(svc.verify(None, ip).decision, AuthDecision::Anonymous);
        assert_eq!(svc.verify(None, ip).decision, AuthDecision::Anonymous);
        assert_eq!(svc.verify(None, ip).decision, AuthDecision::AnonThrottled);
    }
}
