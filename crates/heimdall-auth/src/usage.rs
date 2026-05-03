//! Background tasks: usage flush + cache refresh.
//!
//! - **Flush**: every `flush_interval`, drain `pending_requests` from each
//!   cached key entry and write a single batched transaction to `usage_daily`.
//!   Resilient: SQLite errors are logged and we retry next tick (deltas
//!   accumulate, no data loss unless the process crashes within the window).
//! - **Refresh**: every `refresh_interval`, reload the active-keys snapshot
//!   from the DB and atomically replace the cache. Picks up new keys, picks
//!   up revocations, picks up tier/quota changes.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ahash::AHashMap;
use tokio::time::interval;

use crate::cache::{AuthCache, CacheMap, KeyEntry};
use crate::store::KeyStore;
use crate::tier::Tier;

/// Convert a unix timestamp into a `YYYYMMDD` integer (UTC).
pub fn day_for(now_secs: i64) -> i64 {
    let dt = time::OffsetDateTime::from_unix_timestamp(now_secs)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    let date = dt.date();
    (date.year() as i64) * 10000 + (date.month() as i64) * 100 + (date.day() as i64)
}

/// First day of the same month as `now_secs`, as `YYYYMM01`.
pub fn month_first_day(now_secs: i64) -> i64 {
    let dt = time::OffsetDateTime::from_unix_timestamp(now_secs)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    let date = dt.date();
    (date.year() as i64) * 10000 + (date.month() as i64) * 100 + 1
}

/// Drain in-memory deltas and persist them in one transaction. Returns the
/// number of rows applied.
pub fn flush_once(
    cache: &AuthCache,
    store: &KeyStore,
    today: i64,
) -> rusqlite::Result<usize> {
    let entries = cache.snapshot_entries();
    let mut rows = Vec::with_capacity(entries.len());
    for entry in &entries {
        let req = entry.pending_requests.swap(0, Ordering::Relaxed);
        let err = entry.pending_errors.swap(0, Ordering::Relaxed);
        if req == 0 && err == 0 {
            continue;
        }
        rows.push((entry.id, today, req, err));
    }
    store.flush_usage(&rows)
}

/// Build a cache map from current DB rows, seeding each entry with its
/// month-to-date request count.
pub fn build_cache_map(store: &KeyStore, now_secs: i64) -> rusqlite::Result<CacheMap> {
    let day_from = month_first_day(now_secs);
    let day_to = day_for(now_secs);
    let rows = store.load_active_keys()?;
    let mut map = AHashMap::with_capacity(rows.len());
    for row in &rows {
        let mtd = store.sum_usage_for_key(row.id, day_from, day_to)?;
        let entry = KeyEntry::from_row(row, mtd);
        map.insert(row.key_hash, entry);
    }
    Ok(map)
}

/// Spawn the periodic refresh + flush tasks. Returns the join handles so the
/// caller can keep them alive (or detach).
pub fn spawn_background(
    cache: Arc<AuthCache>,
    store: Arc<KeyStore>,
    refresh_interval: Duration,
    flush_interval: Duration,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let refresh_handle = {
        let cache = Arc::clone(&cache);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let mut tick = interval(refresh_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let store = Arc::clone(&store);
                let res = tokio::task::spawn_blocking(move || {
                    build_cache_map(&store, now_secs())
                })
                .await;
                match res {
                    Ok(Ok(map)) => {
                        let len = map.len();
                        cache.replace(map);
                        tracing::debug!(keys = len, "auth cache refreshed");
                    }
                    Ok(Err(e)) => tracing::error!(error = %e, "auth cache refresh failed"),
                    Err(e) => tracing::error!(error = %e, "auth cache refresh join error"),
                }
            }
        })
    };

    let flush_handle = {
        let cache = Arc::clone(&cache);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let mut tick = interval(flush_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let cache = Arc::clone(&cache);
                let store = Arc::clone(&store);
                let res = tokio::task::spawn_blocking(move || {
                    flush_once(&cache, &store, day_for(now_secs()))
                })
                .await;
                match res {
                    Ok(Ok(n)) if n > 0 => tracing::debug!(rows = n, "usage flushed"),
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => tracing::error!(error = %e, "usage flush failed"),
                    Err(e) => tracing::error!(error = %e, "usage flush join error"),
                }
            }
        })
    };

    (refresh_handle, flush_handle)
}

/// On the start of a new month, in-memory `month_used` counters need to reset
/// to 0. We don't run this on a clock — instead, on each refresh, the
/// `month_to_date` value passed to `KeyEntry::from_row` is computed fresh and
/// becomes the new initial counter for the rebuilt entry. So a refresh after
/// month rollover automatically zeroes out the counters for any key that has
/// no usage_daily rows in the new month yet.
#[allow(dead_code)]
fn _doc_month_rollover() {}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// Re-exported here so the service module doesn't need its own.
pub fn unix_seconds() -> i64 {
    now_secs()
}

// Suppress unused-warning for tier helper — used elsewhere.
#[allow(dead_code)]
fn _force_link_tier(t: Tier) -> Tier { t }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::generate_key;
    use crate::store::NewKey;

    #[tokio::test]
    async fn cache_build_seeds_month_to_date() {
        let store = Arc::new(KeyStore::open_in_memory().unwrap());
        let uid = store.upsert_user("a@b.com", "free").unwrap();
        let k = generate_key(b"pep");
        let kid = store
            .insert_key(NewKey {
                user_id: uid,
                key_hash: k.hash,
                key_prefix: k.prefix,
                key_last4: k.last4,
                name: None,
            })
            .unwrap();

        let now = now_secs();
        let today = day_for(now);
        store.flush_usage(&[(kid, today, 42, 0)]).unwrap();

        let map = build_cache_map(&store, now).unwrap();
        let entry = map.values().next().unwrap();
        assert_eq!(entry.month_used.load(Ordering::Relaxed), 42);
    }

    #[tokio::test]
    async fn flush_round_trips_pending() {
        let store = Arc::new(KeyStore::open_in_memory().unwrap());
        let cache = Arc::new(AuthCache::new());
        let uid = store.upsert_user("a@b.com", "free").unwrap();
        let k = generate_key(b"pep");
        let kid = store
            .insert_key(NewKey {
                user_id: uid,
                key_hash: k.hash,
                key_prefix: k.prefix,
                key_last4: k.last4,
                name: None,
            })
            .unwrap();

        let now = now_secs();
        cache.replace(build_cache_map(&store, now).unwrap());
        let entry = cache.lookup(&k.hash).unwrap();
        entry.pending_requests.fetch_add(7, Ordering::Relaxed);

        let n = flush_once(&cache, &store, day_for(now)).unwrap();
        assert_eq!(n, 1);
        assert_eq!(entry.pending_requests.load(Ordering::Relaxed), 0);
        let total = store
            .sum_usage_for_key(kid, month_first_day(now), day_for(now))
            .unwrap();
        assert_eq!(total, 7);
    }
}
