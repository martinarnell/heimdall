/// lt.rs — Parse Lithuanian address data from govlt/national-boundaries-api SQLite
///
/// Lithuania's national address register is published as a pre-built SQLite database
/// by the govlt/national-boundaries-api project.
///
/// Download: https://github.com/govlt/national-boundaries-api/releases/latest/download/boundaries.sqlite
///
/// The database contains `addresses`, `streets`, and `residential_areas` tables
/// with SpatiaLite geometry in LKS94 (EPSG:3346).

use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use crate::extract::RawAddress;

/// Lithuania bounding box
const LT_LAT_MIN: f64 = 53.90;
const LT_LAT_MAX: f64 = 56.45;
const LT_LON_MIN: f64 = 20.93;
const LT_LON_MAX: f64 = 26.84;

/// Import Lithuanian addresses from a SQLite database.
///
/// Tries the current schema (addresses/streets/residential_areas) first,
/// then falls back to older table names (adresai/gatves/gyvenvietes).
pub fn read_lt_addresses(sqlite_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading Lithuanian addresses from {}", sqlite_path.display());

    let conn = rusqlite::Connection::open_with_flags(
        sqlite_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .with_context(|| format!("failed to open SQLite: {}", sqlite_path.display()))?;

    // Try current schema first, then legacy, then legacy with lat/lon columns
    let result = try_query_current(&conn)
        .or_else(|_| try_query_legacy(&conn))
        .or_else(|_| try_query_legacy_latlon(&conn));

    match result {
        Ok(addresses) => {
            info!("Parsed {} Lithuanian addresses", addresses.len());
            Ok(addresses)
        }
        Err(e) => {
            // Log available tables to help debugging
            if let Ok(mut stmt) = conn.prepare(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            ) {
                if let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(0)) {
                    let tables: Vec<String> = rows.filter_map(|r| r.ok()).collect();
                    info!("  Available tables: {}", tables.join(", "));
                }
            }
            Err(e).context("failed to query Lithuanian addresses — no schema variant matched")
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Current schema: addresses/streets/residential_areas with SpatiaLite geom
// ───────────────────────────────────────────────────────────────────────────

fn try_query_current(conn: &rusqlite::Connection) -> Result<Vec<RawAddress>> {
    let sql = "\
        SELECT s.full_name, \
               a.plot_or_building_number, \
               r.name, \
               a.postal_code, \
               a.geom \
        FROM addresses a \
        JOIN streets s ON a.street_code = s.code \
        JOIN residential_areas r ON a.residential_area_code = r.code \
        WHERE a.geom IS NOT NULL";

    let mut stmt = conn
        .prepare(sql)
        .with_context(|| "current schema query failed to prepare")?;

    let mut addresses = Vec::with_capacity(1_100_000);
    let mut skipped = 0usize;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,           // street
            row.get::<_, String>(1)?,           // number
            row.get::<_, String>(2)?,           // city
            row.get::<_, Option<String>>(3)?,   // postcode
            row.get::<_, Vec<u8>>(4)?,          // geom blob
        ))
    })?;

    for row in rows {
        let (street, number, city, postcode, geom) = match row {
            Ok(r) => r,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        if street.trim().is_empty() || number.trim().is_empty() {
            skipped += 1;
            continue;
        }

        let (lat, lon) = match extract_point_lks94(&geom) {
            Some(coords) => coords,
            None => {
                skipped += 1;
                continue;
            }
        };

        if lat < LT_LAT_MIN || lat > LT_LAT_MAX || lon < LT_LON_MIN || lon > LT_LON_MAX {
            skipped += 1;
            continue;
        }

        addresses.push(RawAddress {
            osm_id: 0,
            street: street.trim().to_string(),
            housenumber: number.trim().to_string(),
            postcode: postcode.map(|p| p.trim().to_string()).filter(|p| !p.is_empty()),
            city: Some(city.trim().to_string()).filter(|c| !c.is_empty()),
            lat,
            lon,
        });
    }

    info!(
        "  current schema — {} addresses ({} skipped)",
        addresses.len(),
        skipped,
    );

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// Legacy schema variant: Lithuanian table names with SpatiaLite geom
// ───────────────────────────────────────────────────────────────────────────

fn try_query_legacy(conn: &rusqlite::Connection) -> Result<Vec<RawAddress>> {
    let sql = "\
        SELECT g.pavadinimas AS street, \
               a.numeris AS number, \
               gv.pavadinimas AS city, \
               a.pasto_kodas AS postcode, \
               a.geom \
        FROM adresai a \
        JOIN gatves g ON a.gatve_id = g.id \
        JOIN gyvenvietes gv ON a.gyvenviete_id = gv.id \
        WHERE a.geom IS NOT NULL";

    let mut stmt = conn
        .prepare(sql)
        .with_context(|| "legacy schema (geom) query failed to prepare")?;

    let mut addresses = Vec::with_capacity(1_000_000);
    let mut skipped = 0usize;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Vec<u8>>(4)?,
        ))
    })?;

    for row in rows {
        let (street, number, city, postcode, geom) = match row {
            Ok(r) => r,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        if street.trim().is_empty() || number.trim().is_empty() {
            skipped += 1;
            continue;
        }

        let (lat, lon) = match extract_point_lks94(&geom) {
            Some(coords) => coords,
            None => {
                skipped += 1;
                continue;
            }
        };

        if lat < LT_LAT_MIN || lat > LT_LAT_MAX || lon < LT_LON_MIN || lon > LT_LON_MAX {
            skipped += 1;
            continue;
        }

        addresses.push(RawAddress {
            osm_id: 0,
            street: street.trim().to_string(),
            housenumber: number.trim().to_string(),
            postcode: postcode.map(|p| p.trim().to_string()).filter(|p| !p.is_empty()),
            city: Some(city.trim().to_string()).filter(|c| !c.is_empty()),
            lat,
            lon,
        });
    }

    info!(
        "  legacy schema (geom) — {} addresses ({} skipped)",
        addresses.len(),
        skipped,
    );

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// Legacy schema variant: Lithuanian table names with lat/lon columns
// ───────────────────────────────────────────────────────────────────────────

fn try_query_legacy_latlon(conn: &rusqlite::Connection) -> Result<Vec<RawAddress>> {
    let sql = "\
        SELECT g.pavadinimas AS street, \
               a.numeris AS number, \
               gv.pavadinimas AS city, \
               a.pasto_kodas AS postcode, \
               a.latitude AS lat, \
               a.longitude AS lon \
        FROM adresai a \
        JOIN gatves g ON a.gatve_id = g.id \
        JOIN gyvenvietes gv ON a.gyvenviete_id = gv.id \
        WHERE a.latitude IS NOT NULL AND a.longitude IS NOT NULL";

    query_addresses_latlon(conn, sql, "legacy schema (lat/lon)")
}

fn query_addresses_latlon(conn: &rusqlite::Connection, sql: &str, schema_name: &str) -> Result<Vec<RawAddress>> {
    let mut stmt = conn
        .prepare(sql)
        .with_context(|| format!("{} query failed to prepare", schema_name))?;

    let mut addresses = Vec::with_capacity(1_000_000);
    let mut skipped = 0usize;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,          // street
            row.get::<_, String>(1)?,          // number
            row.get::<_, String>(2)?,          // city
            row.get::<_, Option<String>>(3)?,  // postcode
            row.get::<_, f64>(4)?,             // lat
            row.get::<_, f64>(5)?,             // lon
        ))
    })?;

    for row in rows {
        let (street, number, city, postcode, lat, lon) = match row {
            Ok(r) => r,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        if street.trim().is_empty() || number.trim().is_empty() {
            skipped += 1;
            continue;
        }

        if lat < LT_LAT_MIN || lat > LT_LAT_MAX || lon < LT_LON_MIN || lon > LT_LON_MAX {
            skipped += 1;
            continue;
        }

        addresses.push(RawAddress {
            osm_id: 0,
            street: street.trim().to_string(),
            housenumber: number.trim().to_string(),
            postcode: postcode.map(|p| p.trim().to_string()).filter(|p| !p.is_empty()),
            city: Some(city.trim().to_string()).filter(|c| !c.is_empty()),
            lat,
            lon,
        });
    }

    info!(
        "  {} — {} addresses ({} skipped)",
        schema_name,
        addresses.len(),
        skipped,
    );

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// SpatiaLite geometry extraction + LKS94 (EPSG:3346) → WGS84 conversion
// ───────────────────────────────────────────────────────────────────────────

/// Extract a point from a SpatiaLite BLOB and convert from LKS94 to WGS84.
///
/// SpatiaLite BLOB format for a Point:
///   [00] start marker (0x00)
///   [01] byte order (01=little-endian, 00=big-endian)
///   [02..06] SRID (i32)
///   [06..14] min_x (f64) — envelope
///   [14..22] min_y (f64)
///   [22..30] max_x (f64)
///   [30..38] max_y (f64)
///   [38] 0x7C marker
///   [39..43] geometry type (i32, 1=Point)
///   [43..51] x (f64)
///   [51..59] y (f64)
///   [59] 0xFE end marker
fn extract_point_lks94(blob: &[u8]) -> Option<(f64, f64)> {
    if blob.len() < 60 || blob[0] != 0x00 || blob[38] != 0x7C {
        return None;
    }

    let le = blob[1] == 1;
    let (x, y) = if le {
        (
            f64::from_le_bytes(blob[43..51].try_into().ok()?),
            f64::from_le_bytes(blob[51..59].try_into().ok()?),
        )
    } else {
        (
            f64::from_be_bytes(blob[43..51].try_into().ok()?),
            f64::from_be_bytes(blob[51..59].try_into().ok()?),
        )
    };

    Some(lks94_to_wgs84(x, y))
}

/// Convert LKS94 (EPSG:3346) coordinates to WGS84 (lat, lon).
///
/// LKS94 is a Transverse Mercator projection on GRS80/ETRS89:
///   - Central meridian: 24°E
///   - Scale factor: 0.9998
///   - False easting: 500,000 m
///   - False northing: 0 m
///   - Ellipsoid: GRS80 (a=6378137, f=1/298.257222101)
///
/// GRS80/ETRS89 ≈ WGS84 (sub-meter difference), so no datum shift needed.
fn lks94_to_wgs84(easting: f64, northing: f64) -> (f64, f64) {
    let a = 6_378_137.0_f64;
    let f = 1.0 / 298.257_222_101;
    let e2: f64 = 2.0 * f - f * f;
    let k0: f64 = 0.9998;
    let lambda0 = 24.0_f64.to_radians();
    let fe = 500_000.0;
    let fn_ = 0.0;

    let x_adj = easting - fe;
    let y_adj = northing - fn_;

    let m = y_adj / k0;

    let e1 = (1.0 - (1.0 - e2).sqrt()) / (1.0 + (1.0 - e2).sqrt());
    let mu = m / (a * (1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2.powi(3) / 256.0));

    let phi1 = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1.powi(3) / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1 * e1 / 16.0 - 55.0 * e1.powi(4) / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1.powi(3) / 96.0) * (6.0 * mu).sin()
        + (1097.0 * e1.powi(4) / 512.0) * (8.0 * mu).sin();

    let sin_phi1 = phi1.sin();
    let cos_phi1 = phi1.cos();
    let tan_phi1 = phi1.tan();
    let n1 = a / (1.0 - e2 * sin_phi1 * sin_phi1).sqrt();
    let t1 = tan_phi1 * tan_phi1;
    let ep2 = e2 / (1.0 - e2);
    let c1 = ep2 * cos_phi1 * cos_phi1;
    let r1 = a * (1.0 - e2) / (1.0 - e2 * sin_phi1 * sin_phi1).powf(1.5);
    let d = x_adj / (n1 * k0);

    let lat = phi1
        - (n1 * tan_phi1 / r1)
            * (d * d / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * ep2) * d.powi(4) / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1 - 252.0 * ep2
                    - 3.0 * c1 * c1)
                    * d.powi(6)
                    / 720.0);

    let lon = lambda0
        + (d - (1.0 + 2.0 * t1 + c1) * d.powi(3) / 6.0
            + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * ep2 + 24.0 * t1 * t1)
                * d.powi(5)
                / 120.0)
            / cos_phi1;

    (lat.to_degrees(), lon.to_degrees())
}
