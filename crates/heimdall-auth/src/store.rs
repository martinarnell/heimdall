//! SQLite-backed persistence.
//!
//! The DB is shared with the website (Next.js) — both processes open the same
//! file in WAL mode. The Rust API mostly reads from it (cached) and writes
//! batched usage rows; the website does CRUD on users + keys.
//!
//! All blocking SQLite calls are wrapped in `tokio::task::spawn_blocking` at
//! the call site (see `service.rs`); the raw `KeyStore` API is sync.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, params};

use crate::key::KeyHash;
use crate::migrate;

#[derive(Debug, Clone)]
pub struct UserRow {
    pub id: i64,
    pub email: String,
    pub tier: String,
    pub stripe_customer_id: Option<String>,
    pub created_at: i64,
    pub blocked_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ApiKeyRow {
    pub id: i64,
    pub user_id: i64,
    pub key_hash: KeyHash,
    pub key_prefix: String,
    pub key_last4: String,
    pub name: Option<String>,
    pub user_tier: String,
    pub user_blocked: bool,
    pub rate_limit_rps_override: Option<u32>,
    pub monthly_quota_override: Option<u64>,
    pub created_at: i64,
    pub revoked_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct NewKey {
    pub user_id: i64,
    pub key_hash: KeyHash,
    pub key_prefix: String,
    pub key_last4: String,
    pub name: Option<String>,
}

pub struct KeyStore {
    conn: Mutex<Connection>,
}

impl KeyStore {
    /// Open (or create) the auth DB at `path` and run migrations.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        migrate::run(&conn)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// In-memory store, for tests.
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        migrate::run(&conn)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// Idempotent user upsert — returns the user id.
    pub fn upsert_user(&self, email: &str, tier: &str) -> rusqlite::Result<i64> {
        let now = unix_seconds();
        let conn = self.conn.lock().unwrap();
        if let Some(id) = conn
            .query_row(
                "SELECT id FROM users WHERE email = ?",
                [email],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
        {
            return Ok(id);
        }
        conn.execute(
            "INSERT INTO users (email, tier, created_at) VALUES (?, ?, ?)",
            params![email, tier, now],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get_user_by_email(&self, email: &str) -> rusqlite::Result<Option<UserRow>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, email, tier, stripe_customer_id, created_at, blocked_at
             FROM users WHERE email = ?",
            [email],
            |r| {
                Ok(UserRow {
                    id: r.get(0)?,
                    email: r.get(1)?,
                    tier: r.get(2)?,
                    stripe_customer_id: r.get(3)?,
                    created_at: r.get(4)?,
                    blocked_at: r.get(5)?,
                })
            },
        )
        .optional()
    }

    pub fn list_users(&self) -> rusqlite::Result<Vec<UserRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, email, tier, stripe_customer_id, created_at, blocked_at
             FROM users ORDER BY id",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(UserRow {
                    id: r.get(0)?,
                    email: r.get(1)?,
                    tier: r.get(2)?,
                    stripe_customer_id: r.get(3)?,
                    created_at: r.get(4)?,
                    blocked_at: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn insert_key(&self, k: NewKey) -> rusqlite::Result<i64> {
        let now = unix_seconds();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO api_keys
                (user_id, key_hash, key_prefix, key_last4, name, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            params![k.user_id, k.key_hash.as_slice(), k.key_prefix, k.key_last4, k.name, now],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Load all non-revoked keys joined with their user. Used to populate the
    /// in-memory cache on startup and during periodic refresh.
    pub fn load_active_keys(&self) -> rusqlite::Result<Vec<ApiKeyRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT k.id, k.user_id, k.key_hash, k.key_prefix, k.key_last4, k.name,
                    u.tier, u.blocked_at IS NOT NULL,
                    k.rate_limit_rps, k.monthly_quota,
                    k.created_at, k.revoked_at
             FROM api_keys k
             JOIN users u ON u.id = k.user_id
             WHERE k.revoked_at IS NULL",
        )?;
        let rows = stmt
            .query_map([], |r| {
                let blob: Vec<u8> = r.get(2)?;
                let mut hash = [0u8; 32];
                if blob.len() == 32 {
                    hash.copy_from_slice(&blob);
                }
                Ok(ApiKeyRow {
                    id: r.get(0)?,
                    user_id: r.get(1)?,
                    key_hash: hash,
                    key_prefix: r.get(3)?,
                    key_last4: r.get(4)?,
                    name: r.get(5)?,
                    user_tier: r.get(6)?,
                    user_blocked: r.get(7)?,
                    rate_limit_rps_override: r.get::<_, Option<i64>>(8)?.map(|v| v as u32),
                    monthly_quota_override: r.get::<_, Option<i64>>(9)?.map(|v| v as u64),
                    created_at: r.get(10)?,
                    revoked_at: r.get(11)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn list_keys_for_user(&self, user_id: i64) -> rusqlite::Result<Vec<ApiKeyRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT k.id, k.user_id, k.key_hash, k.key_prefix, k.key_last4, k.name,
                    u.tier, u.blocked_at IS NOT NULL,
                    k.rate_limit_rps, k.monthly_quota,
                    k.created_at, k.revoked_at
             FROM api_keys k
             JOIN users u ON u.id = k.user_id
             WHERE k.user_id = ?
             ORDER BY k.id",
        )?;
        let rows = stmt
            .query_map([user_id], |r| {
                let blob: Vec<u8> = r.get(2)?;
                let mut hash = [0u8; 32];
                if blob.len() == 32 {
                    hash.copy_from_slice(&blob);
                }
                Ok(ApiKeyRow {
                    id: r.get(0)?,
                    user_id: r.get(1)?,
                    key_hash: hash,
                    key_prefix: r.get(3)?,
                    key_last4: r.get(4)?,
                    name: r.get(5)?,
                    user_tier: r.get(6)?,
                    user_blocked: r.get(7)?,
                    rate_limit_rps_override: r.get::<_, Option<i64>>(8)?.map(|v| v as u32),
                    monthly_quota_override: r.get::<_, Option<i64>>(9)?.map(|v| v as u64),
                    created_at: r.get(10)?,
                    revoked_at: r.get(11)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Revoke by prefix (e.g. `hk_live_8K3p`). Returns rows affected.
    pub fn revoke_key_by_prefix(&self, prefix: &str) -> rusqlite::Result<usize> {
        let now = unix_seconds();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE api_keys SET revoked_at = ? WHERE key_prefix = ? AND revoked_at IS NULL",
            params![now, prefix],
        )
    }

    /// Set or clear per-key rate-limit override.
    pub fn set_rate_limit(&self, key_id: i64, rps: Option<u32>) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE api_keys SET rate_limit_rps = ? WHERE id = ?",
            params![rps.map(|v| v as i64), key_id],
        )?;
        Ok(())
    }

    /// Set or clear per-key monthly-quota override.
    pub fn set_monthly_quota(&self, key_id: i64, quota: Option<u64>) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE api_keys SET monthly_quota = ? WHERE id = ?",
            params![quota.map(|v| v as i64), key_id],
        )?;
        Ok(())
    }

    pub fn set_user_tier(&self, user_id: i64, tier: &str) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE users SET tier = ? WHERE id = ?",
            params![tier, user_id],
        )?;
        Ok(())
    }

    /// Sum of `requests` across all `usage_daily` rows in [`day_from`..=`day_to`].
    /// Used at cache build time to seed the per-key month-to-date counter.
    pub fn sum_usage_for_key(
        &self,
        key_id: i64,
        day_from: i64,
        day_to: i64,
    ) -> rusqlite::Result<u64> {
        let conn = self.conn.lock().unwrap();
        let v: i64 = conn.query_row(
            "SELECT COALESCE(SUM(requests), 0) FROM usage_daily
             WHERE api_key_id = ? AND day BETWEEN ? AND ?",
            params![key_id, day_from, day_to],
            |r| r.get(0),
        )?;
        Ok(v as u64)
    }

    /// Apply a batch of `(key_id, day, request_delta, error_delta)` rows in
    /// one transaction. Used by the background usage flush task.
    pub fn flush_usage(
        &self,
        rows: &[(i64, i64, u64, u64)],
    ) -> rusqlite::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO usage_daily (api_key_id, day, requests, errors)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(api_key_id, day) DO UPDATE SET
                    requests = requests + excluded.requests,
                    errors = errors + excluded.errors",
            )?;
            for (key_id, day, req, err) in rows {
                stmt.execute(params![key_id, day, *req as i64, *err as i64])?;
            }
        }
        tx.commit()?;
        Ok(rows.len())
    }

    /// Mark `last_used_at = now` for a key. Best-effort; called rarely (we
    /// don't update on every request — the usage_daily table is the source of
    /// truth for activity).
    pub fn touch_last_used(&self, key_id: i64) -> rusqlite::Result<()> {
        let now = unix_seconds();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE api_keys SET last_used_at = ? WHERE id = ?",
            params![now, key_id],
        )?;
        Ok(())
    }
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::generate_key;

    fn fresh() -> KeyStore {
        KeyStore::open_in_memory().unwrap()
    }

    #[test]
    fn roundtrip_user_and_key() {
        let store = fresh();
        let uid = store.upsert_user("a@b.com", "free").unwrap();
        let k = generate_key(b"pep");
        let key_id = store
            .insert_key(NewKey {
                user_id: uid,
                key_hash: k.hash,
                key_prefix: k.prefix.clone(),
                key_last4: k.last4.clone(),
                name: Some("test".into()),
            })
            .unwrap();
        let active = store.load_active_keys().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, key_id);
        assert_eq!(active[0].key_hash, k.hash);
        assert_eq!(active[0].user_tier, "free");
    }

    #[test]
    fn revoke_excludes_from_active() {
        let store = fresh();
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
        let n = store.revoke_key_by_prefix(&k.prefix).unwrap();
        assert_eq!(n, 1);
        assert_eq!(store.load_active_keys().unwrap().len(), 0);
    }

    #[test]
    fn flush_usage_accumulates() {
        let store = fresh();
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
        store.flush_usage(&[(kid, 20260503, 100, 1)]).unwrap();
        store.flush_usage(&[(kid, 20260503, 50, 0)]).unwrap();
        let total = store.sum_usage_for_key(kid, 20260501, 20260531).unwrap();
        assert_eq!(total, 150);
    }
}
