/// db.rs — SQLite storage for comparison results.

use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;
use rusqlite::{params, Connection};

use crate::types::QueryEntry;

/// Open (or create) the results database and ensure schema is up to date.
pub fn open_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;
    init_or_migrate(&conn)?;
    Ok(conn)
}

fn init_or_migrate(conn: &Connection) -> Result<()> {
    // Check if table exists at all
    let table_exists: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='runs'",
        [],
        |r| r.get(0),
    )?;

    if !table_exists {
        create_schema(conn)?;
        return Ok(());
    }

    // Check if new schema (has query_id column)
    let has_query_id: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM pragma_table_info('runs') WHERE name='query_id'",
        [],
        |r| r.get(0),
    )?;

    if !has_query_id {
        migrate_v1_to_v2(conn)?;
    }

    Ok(())
}

fn create_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id TEXT NOT NULL,
            query TEXT,
            query_type TEXT NOT NULL,
            country TEXT,
            lat REAL,
            lon REAL,
            expected_lat REAL,
            expected_lon REAL,
            heimdall_lat REAL,
            heimdall_lon REAL,
            heimdall_display TEXT,
            heimdall_ms INTEGER,
            nominatim_lat REAL,
            nominatim_lon REAL,
            nominatim_display TEXT,
            nominatim_ms INTEGER,
            distance_m REAL,
            category TEXT NOT NULL,
            variant_of TEXT,
            variant_type TEXT,
            queried_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_query_id ON runs(query_id);
        CREATE INDEX IF NOT EXISTS idx_category ON runs(category);
        CREATE INDEX IF NOT EXISTS idx_country ON runs(country);
        CREATE INDEX IF NOT EXISTS idx_query_type ON runs(query_type);
        CREATE INDEX IF NOT EXISTS idx_queried_at ON runs(queried_at);
    ",
    )?;
    Ok(())
}

/// Migrate old (query, country) schema to new query_id schema.
fn migrate_v1_to_v2(conn: &Connection) -> Result<()> {
    eprintln!("Migrating compare.db from v1 to v2 schema...");
    conn.execute_batch(
        "
        ALTER TABLE runs ADD COLUMN query_id TEXT;
        ALTER TABLE runs ADD COLUMN lat REAL;
        ALTER TABLE runs ADD COLUMN lon REAL;
        ALTER TABLE runs ADD COLUMN expected_lat REAL;
        ALTER TABLE runs ADD COLUMN expected_lon REAL;
        ALTER TABLE runs ADD COLUMN variant_of TEXT;
        ALTER TABLE runs ADD COLUMN variant_type TEXT;
    ",
    )?;
    // Backfill query_id from rowid
    conn.execute("UPDATE runs SET query_id = 'legacy_' || id WHERE query_id IS NULL", [])?;
    // Make query_id NOT NULL going forward (SQLite can't alter NOT NULL, but index enforces uniqueness)
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_query_id ON runs(query_id);",
    )?;
    eprintln!("Migration complete.");
    Ok(())
}

/// Get the set of completed query IDs.
pub fn get_completed_ids(conn: &Connection) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT query_id FROM runs")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut set = HashSet::new();
    for row in rows {
        if let Ok(id) = row {
            set.insert(id);
        }
    }
    Ok(set)
}

/// Insert a comparison result row.
#[allow(clippy::too_many_arguments)]
pub fn insert_result(
    conn: &Connection,
    entry: &QueryEntry,
    heimdall_lat: Option<f64>,
    heimdall_lon: Option<f64>,
    heimdall_display: Option<&str>,
    heimdall_ms: u64,
    nominatim_lat: Option<f64>,
    nominatim_lon: Option<f64>,
    nominatim_display: Option<&str>,
    nominatim_ms: u64,
    distance_m: Option<f64>,
    category: &str,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO runs (
            query_id, query, query_type, country,
            lat, lon, expected_lat, expected_lon,
            heimdall_lat, heimdall_lon, heimdall_display, heimdall_ms,
            nominatim_lat, nominatim_lon, nominatim_display, nominatim_ms,
            distance_m, category, variant_of, variant_type
        ) VALUES (
            ?1, ?2, ?3, ?4,
            ?5, ?6, ?7, ?8,
            ?9, ?10, ?11, ?12,
            ?13, ?14, ?15, ?16,
            ?17, ?18, ?19, ?20
        )",
        params![
            entry.id,
            entry.q,
            entry.category,
            entry.country,
            entry.lat,
            entry.lon,
            entry.expected_lat,
            entry.expected_lon,
            heimdall_lat,
            heimdall_lon,
            heimdall_display,
            heimdall_ms as i64,
            nominatim_lat,
            nominatim_lon,
            nominatim_display,
            nominatim_ms as i64,
            distance_m,
            category,
            entry.variant_of,
            entry.variant_type,
        ],
    )?;
    Ok(())
}

/// Count total rows.
pub fn count_total(conn: &Connection) -> Result<usize> {
    Ok(conn.query_row("SELECT COUNT(*) FROM runs", [], |r| r.get(0))?)
}

/// Count rows matching a category.
pub fn count_category(conn: &Connection, category: &str) -> Result<usize> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM runs WHERE category = ?1",
        params![category],
        |r| r.get(0),
    )?)
}

/// Count rows for a specific country.
pub fn count_country(conn: &Connection, country: &str) -> Result<usize> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM runs WHERE country = ?1",
        params![country],
        |r| r.get(0),
    )?)
}

/// Count rows matching category for a country.
pub fn count_country_category(conn: &Connection, country: &str, category: &str) -> Result<usize> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM runs WHERE country = ?1 AND category = ?2",
        params![country, category],
        |r| r.get(0),
    )?)
}

/// Get distinct countries.
pub fn distinct_countries(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT DISTINCT country FROM runs WHERE country IS NOT NULL ORDER BY country")?;
    let rows = stmt.query_map([], |r| r.get(0))?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// Get all distances (sorted).
pub fn all_distances_sorted(conn: &Connection) -> Result<Vec<f64>> {
    let mut stmt = conn.prepare(
        "SELECT distance_m FROM runs WHERE distance_m IS NOT NULL ORDER BY distance_m",
    )?;
    let rows = stmt.query_map([], |r| r.get(0))?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}
