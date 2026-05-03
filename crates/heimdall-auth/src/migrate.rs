//! SQLite schema migrations.
//!
//! Applied at startup by [`KeyStore::open`]. Idempotent — uses a `schema_version`
//! table to track applied migrations.

use rusqlite::Connection;

const MIGRATIONS: &[(i64, &str)] = &[
    (
        1,
        r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            email_verified_at INTEGER,
            stripe_customer_id TEXT UNIQUE,
            tier TEXT NOT NULL DEFAULT 'free',
            created_at INTEGER NOT NULL,
            blocked_at INTEGER
        );
        CREATE TABLE api_keys (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            key_hash BLOB NOT NULL UNIQUE,
            key_prefix TEXT NOT NULL,
            key_last4 TEXT NOT NULL,
            name TEXT,
            rate_limit_rps INTEGER,
            monthly_quota INTEGER,
            created_at INTEGER NOT NULL,
            last_used_at INTEGER,
            revoked_at INTEGER
        );
        CREATE INDEX api_keys_user_active
            ON api_keys(user_id) WHERE revoked_at IS NULL;
        CREATE TABLE usage_daily (
            api_key_id INTEGER NOT NULL REFERENCES api_keys(id),
            day INTEGER NOT NULL,
            requests INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (api_key_id, day)
        ) WITHOUT ROWID;
        "#,
    ),
];

pub fn run(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA foreign_keys=ON;
         CREATE TABLE IF NOT EXISTS schema_version (
             version INTEGER PRIMARY KEY,
             applied_at INTEGER NOT NULL
         );",
    )?;

    let applied: std::collections::HashSet<i64> = conn
        .prepare("SELECT version FROM schema_version")?
        .query_map([], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<_>>()?;

    for (version, sql) in MIGRATIONS {
        if applied.contains(version) {
            continue;
        }
        tracing::info!(version, "applying auth schema migration");
        conn.execute_batch(sql)?;
        let now = unix_seconds();
        conn.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
            (version, now),
        )?;
    }

    Ok(())
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

    #[test]
    fn migrations_are_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        run(&conn).unwrap();
        run(&conn).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, MIGRATIONS.len() as i64);
    }

    #[test]
    fn tables_exist_after_migrate() {
        let conn = Connection::open_in_memory().unwrap();
        run(&conn).unwrap();
        for table in ["users", "api_keys", "usage_daily"] {
            let exists: bool = conn
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    [table],
                    |_| Ok(true),
                )
                .unwrap_or(false);
            assert!(exists, "missing table: {table}");
        }
    }
}
