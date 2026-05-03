//! In-memory key cache — the hot path.
//!
//! On startup and every refresh, we load all non-revoked keys from the DB,
//! build a `HashMap<KeyHash, Arc<KeyEntry>>`, and atomically swap it in via
//! `arc_swap::ArcSwap`. Verification is then O(1) HashMap lookup against a
//! pre-built snapshot — no locks on the read path.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ahash::AHashMap;
use arc_swap::ArcSwap;
use governor::{Quota, RateLimiter, clock::DefaultClock};
use governor::state::{InMemoryState, NotKeyed};

use crate::key::KeyHash;
use crate::store::ApiKeyRow;
use crate::tier::Tier;

/// Per-key state held in the cache.
pub struct KeyEntry {
    pub id: i64,
    pub user_id: i64,
    pub key_prefix: String,
    pub key_last4: String,
    pub tier: Tier,
    /// Effective rate limit (override or tier default).
    pub rate_limit_rps: u32,
    /// Effective monthly quota (override or tier default).
    pub monthly_quota: u64,
    /// Lock-free GCRA limiter for per-key RPS enforcement.
    pub limiter: RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    /// Month-to-date request count, including in-memory delta not yet flushed
    /// to the DB. Source of truth for quota checks.
    pub month_used: AtomicU64,
    /// Pending request delta since the last flush — drained by the flush task.
    pub pending_requests: AtomicU64,
    pub pending_errors: AtomicU64,
    /// Set when the user is blocked or the key is revoked. We still serve a
    /// 403 from the cached entry until refresh removes it from the map.
    pub blocked: AtomicBool,
}

impl KeyEntry {
    pub fn from_row(row: &ApiKeyRow, month_to_date: u64) -> Arc<Self> {
        let tier: Tier = row.user_tier.parse().unwrap_or(Tier::Free);
        let defaults = tier.defaults();
        let rps = row.rate_limit_rps_override.unwrap_or(defaults.rate_limit_rps);
        let quota = row.monthly_quota_override.unwrap_or(defaults.monthly_quota);

        let rps_nz = NonZeroU32::new(rps.max(1)).unwrap();
        // Allow short bursts up to the per-second rate.
        let limiter = RateLimiter::direct(Quota::per_second(rps_nz).allow_burst(rps_nz));

        Arc::new(Self {
            id: row.id,
            user_id: row.user_id,
            key_prefix: row.key_prefix.clone(),
            key_last4: row.key_last4.clone(),
            tier,
            rate_limit_rps: rps,
            monthly_quota: quota,
            limiter,
            month_used: AtomicU64::new(month_to_date),
            pending_requests: AtomicU64::new(0),
            pending_errors: AtomicU64::new(0),
            blocked: AtomicBool::new(row.user_blocked || row.revoked_at.is_some()),
        })
    }
}

/// Snapshot of all active keys, indexed by their HMAC hash.
pub type CacheMap = AHashMap<KeyHash, Arc<KeyEntry>>;

pub struct AuthCache {
    inner: ArcSwap<CacheMap>,
}

impl AuthCache {
    pub fn new() -> Self {
        Self { inner: ArcSwap::from_pointee(AHashMap::new()) }
    }

    pub fn lookup(&self, hash: &KeyHash) -> Option<Arc<KeyEntry>> {
        self.inner.load().get(hash).cloned()
    }

    pub fn replace(&self, new_map: CacheMap) {
        self.inner.store(Arc::new(new_map));
    }

    pub fn len(&self) -> usize {
        self.inner.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over all current entries — used by the flush task to drain
    /// `pending_requests` counters.
    pub fn snapshot_entries(&self) -> Vec<Arc<KeyEntry>> {
        self.inner
            .load()
            .values()
            .cloned()
            .collect()
    }
}

impl Default for AuthCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Atomically increment month_used and return the new value, OR roll back and
/// return the over-quota value if it would exceed the quota. This avoids
/// "leaking" quota slots if multiple concurrent requests race.
pub fn try_consume_quota(entry: &KeyEntry) -> Result<u64, u64> {
    // Fast path: optimistic increment.
    let prev = entry.month_used.fetch_add(1, Ordering::Relaxed);
    let now = prev + 1;
    if now > entry.monthly_quota {
        // Roll back; report the count we would have hit.
        entry.month_used.fetch_sub(1, Ordering::Relaxed);
        return Err(entry.monthly_quota);
    }
    entry.pending_requests.fetch_add(1, Ordering::Relaxed);
    Ok(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(quota_override: Option<u64>, tier: &str) -> ApiKeyRow {
        ApiKeyRow {
            id: 1,
            user_id: 1,
            key_hash: [0u8; 32],
            key_prefix: "hk_live_aaaa".into(),
            key_last4: "zzzz".into(),
            name: None,
            user_tier: tier.into(),
            user_blocked: false,
            rate_limit_rps_override: None,
            monthly_quota_override: quota_override,
            created_at: 0,
            revoked_at: None,
        }
    }

    #[test]
    fn cache_replace_visible_to_lookup() {
        let cache = AuthCache::new();
        let mut map = AHashMap::new();
        let entry = KeyEntry::from_row(&row(Some(100), "free"), 0);
        let hash = [7u8; 32];
        map.insert(hash, entry);
        cache.replace(map);
        assert!(cache.lookup(&hash).is_some());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn quota_blocks_at_limit() {
        let entry = KeyEntry::from_row(&row(Some(3), "free"), 0);
        assert!(try_consume_quota(&entry).is_ok());
        assert!(try_consume_quota(&entry).is_ok());
        assert!(try_consume_quota(&entry).is_ok());
        assert!(try_consume_quota(&entry).is_err());
        // Failed call rolled back, so used should still be 3.
        assert_eq!(entry.month_used.load(Ordering::Relaxed), 3);
        // pending_requests only incremented for the 3 successful calls.
        assert_eq!(entry.pending_requests.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn enterprise_quota_is_unbounded_in_practice() {
        let entry = KeyEntry::from_row(&row(None, "enterprise"), 0);
        assert!(entry.monthly_quota >= 1_000_000_000);
    }
}
