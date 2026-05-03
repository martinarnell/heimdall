/// abr.rs — Parse Japan ABR (Address Base Registry) addresses from abr-geocoder SQLite
///
/// Japan's national address registry is maintained by the Digital Agency.
/// The abr-geocoder (Node.js tool) downloads ABR data and builds a SQLite database.
/// Heimdall reads from this pre-built SQLite.
///
/// Japan's address system is hierarchical blocks, NOT street-based:
///   Prefecture (都道府県) → City/Ward (市区町村) → Town/District (町字) → Block (街区) → House (号)
///   Example: 東京都千代田区紀尾井町1-3 = Tokyo-to Chiyoda-ku Kioicho 1-3
///
/// For Heimdall's RawAddress mapping:
///   street      = town/district name (machi-aza) — romaji form
///   housenumber = block-house number ("1-3" or "1-3-5")
///   city        = city_name (kanji)
///   postcode    = NULL (ABR SQLite doesn't reliably include postcodes)
///   lat/lon     = rep_lat/rep_lon from position table (WGS84)

use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use crate::extract::RawAddress;

/// Japan bounding box
const JP_LAT_MIN: f64 = 24.0;
const JP_LAT_MAX: f64 = 45.5;
const JP_LON_MIN: f64 = 122.9;
const JP_LON_MAX: f64 = 153.0;

/// Import Japan ABR addresses from a pre-built abr-geocoder SQLite database.
///
/// Tries a multi-table join first (newer abr-geocoder versions), then falls back
/// to a flat `address` table (older versions).
pub fn read_abr_addresses(sqlite_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading Japan ABR addresses from {}", sqlite_path.display());

    let conn = rusqlite::Connection::open_with_flags(
        sqlite_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .with_context(|| format!("failed to open SQLite: {}", sqlite_path.display()))?;

    // Try the multi-table join first, then fall back to simpler schemas
    let result = try_query_joined(&conn)
        .or_else(|e| {
            info!("  Primary query failed ({}), trying flat table...", e);
            try_query_flat(&conn)
        });

    match result {
        Ok(addresses) => {
            info!("Parsed {} Japan ABR addresses", addresses.len());
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
            Err(e).context("failed to query Japan ABR addresses — no compatible schema found")
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Full-width → half-width conversion
// ───────────────────────────────────────────────────────────────────────────

/// Convert full-width digits (０-９) and full-width space to half-width.
fn fullwidth_to_halfwidth(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\u{FF10}'..='\u{FF19}' => {
                // ０(U+FF10) .. ９(U+FF19) → 0..9
                ((c as u32 - 0xFF10) + b'0' as u32) as u8 as char
            }
            '\u{3000}' => ' ', // ideographic space → ASCII space
            _ => c,
        })
        .collect()
}

/// Title-case a string: "kioicho" → "Kioicho", "chiyoda ku" → "Chiyoda Ku"
fn title_case(s: &str) -> String {
    s.split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Format block/house number as "block-house" or "block-house-sub".
/// Returns None if block or house is 0 or missing.
fn format_housenumber(blk_num: Option<&str>, addr_num: Option<&str>, addr_num2: Option<&str>) -> Option<String> {
    let blk = blk_num
        .map(|s| fullwidth_to_halfwidth(s.trim()))
        .unwrap_or_default();
    let addr = addr_num
        .map(|s| fullwidth_to_halfwidth(s.trim()))
        .unwrap_or_default();

    // Parse block — skip if 0 or empty
    let blk_val: i64 = blk.parse().unwrap_or(0);
    if blk_val <= 0 {
        return None;
    }

    // Parse house number — skip if 0 or empty
    let addr_val: i64 = addr.parse().unwrap_or(0);
    if addr_val <= 0 {
        return None;
    }

    let mut result = format!("{}-{}", blk_val, addr_val);

    // Optional sub-number (addr_num2)
    if let Some(sub) = addr_num2 {
        let sub = fullwidth_to_halfwidth(sub.trim());
        if !sub.is_empty() {
            if let Ok(sub_val) = sub.parse::<i64>() {
                if sub_val > 0 {
                    result.push('-');
                    result.push_str(&sub_val.to_string());
                }
            }
        }
    }

    Some(result)
}

// ───────────────────────────────────────────────────────────────────────────
// Schema variant: Multi-table join (newer abr-geocoder)
// ───────────────────────────────────────────────────────────────────────────

fn try_query_joined(conn: &rusqlite::Connection) -> Result<Vec<RawAddress>> {
    let sql = "\
        SELECT \
            t.town_name, t.town_name_roma, \
            p.pref_name, p.pref_name_roma, \
            c.city_name, c.city_name_roma, \
            b.blk_num, \
            r.addr_num, r.addr_num2, \
            pos.rep_lat, pos.rep_lon \
        FROM town t \
        JOIN pref p ON t.pref_code = p.pref_code \
        JOIN city c ON t.city_code = c.city_code \
        LEFT JOIN rsdtdsp_blk b ON t.town_id = b.town_id \
        LEFT JOIN rsdtdsp_rsdt r ON b.rsdtdsp_blk_id = r.rsdtdsp_blk_id \
        LEFT JOIN rsdtdsp_rsdt_pos pos ON r.rsdtdsp_rsdt_id = pos.rsdtdsp_rsdt_id \
        WHERE pos.rep_lat IS NOT NULL AND pos.rep_lon IS NOT NULL";

    let mut stmt = conn
        .prepare(sql)
        .with_context(|| "joined query failed to prepare")?;

    let mut addresses = Vec::with_capacity(20_000_000);
    let mut skipped = 0usize;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,  // town_name (kanji)
            row.get::<_, Option<String>>(1)?,  // town_name_roma
            row.get::<_, Option<String>>(2)?,  // pref_name (kanji)
            row.get::<_, Option<String>>(3)?,  // pref_name_roma
            row.get::<_, Option<String>>(4)?,  // city_name (kanji)
            row.get::<_, Option<String>>(5)?,  // city_name_roma
            row.get::<_, Option<String>>(6)?,  // blk_num
            row.get::<_, Option<String>>(7)?,  // addr_num
            row.get::<_, Option<String>>(8)?,  // addr_num2
            row.get::<_, f64>(9)?,             // rep_lat
            row.get::<_, f64>(10)?,            // rep_lon
        ))
    })?;

    for row in rows {
        let (
            town_name, town_name_roma,
            _pref_name, _pref_name_roma,
            city_name, city_name_roma,
            blk_num, addr_num, addr_num2,
            lat, lon,
        ) = match row {
            Ok(r) => r,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Bbox filter
        if lat < JP_LAT_MIN || lat > JP_LAT_MAX || lon < JP_LON_MIN || lon > JP_LON_MAX {
            skipped += 1;
            continue;
        }

        // Format house number — skip if block/house is 0 or missing
        let housenumber = match format_housenumber(
            blk_num.as_deref(),
            addr_num.as_deref(),
            addr_num2.as_deref(),
        ) {
            Some(h) => h,
            None => {
                skipped += 1;
                continue;
            }
        };

        // Town name is required (this is the "street equivalent")
        let town_kanji = match &town_name {
            Some(t) if !t.trim().is_empty() => t.trim().to_string(),
            _ => {
                skipped += 1;
                continue;
            }
        };

        let city_kanji = city_name
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        // Generate romaji entry (primary — most international users search in romaji)
        let town_roma = town_name_roma
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| title_case(s));

        let city_roma = city_name_roma
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| title_case(s));

        // Entry 1: romaji street + romaji city (for international search)
        if let Some(ref roma) = town_roma {
            addresses.push(RawAddress {
                osm_id: 0,
                street: roma.clone(),
                housenumber: housenumber.clone(),
                postcode: None,
                city: city_roma.clone(),
                state: None,
                lat,
                lon,
            });
        }

        // Entry 2: kanji street + kanji city (for Japanese-language search)
        addresses.push(RawAddress {
            osm_id: 0,
            street: town_kanji,
            housenumber,
            postcode: None,
            city: city_kanji,
            state: None,
            lat,
            lon,
        });
    }

    info!(
        "  Joined query — {} addresses ({} skipped)",
        addresses.len(),
        skipped,
    );

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// Schema variant: Flat address table (older abr-geocoder versions)
// ───────────────────────────────────────────────────────────────────────────

fn try_query_flat(conn: &rusqlite::Connection) -> Result<Vec<RawAddress>> {
    // Some abr-geocoder versions produce a flat `address` table
    let sql = "\
        SELECT * FROM address WHERE latitude IS NOT NULL AND longitude IS NOT NULL \
        LIMIT 1";

    // Probe if the table exists
    conn.prepare(sql)
        .with_context(|| "flat `address` table not found")?;

    // It exists — query for real. Discover column names from pragma.
    let col_info: Vec<String> = {
        let mut stmt = conn.prepare("PRAGMA table_info(address)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        rows.filter_map(|r| r.ok()).collect()
    };

    info!("  Flat address table columns: {}", col_info.join(", "));

    // Build query based on available columns
    let has_col = |name: &str| col_info.iter().any(|c| c == name);

    let town_col = if has_col("town_name") {
        "town_name"
    } else if has_col("machi_name") {
        "machi_name"
    } else {
        "town"
    };

    let city_col = if has_col("city_name") {
        "city_name"
    } else {
        "city"
    };

    let lat_col = if has_col("latitude") {
        "latitude"
    } else {
        "lat"
    };

    let lon_col = if has_col("longitude") {
        "longitude"
    } else {
        "lon"
    };

    let housenumber_col = if has_col("addr_num") {
        "addr_num"
    } else if has_col("house_number") {
        "house_number"
    } else {
        "number"
    };

    let sql = format!(
        "SELECT {town}, {city}, {num}, {lat}, {lon} FROM address \
         WHERE {lat} IS NOT NULL AND {lon} IS NOT NULL",
        town = town_col,
        city = city_col,
        num = housenumber_col,
        lat = lat_col,
        lon = lon_col,
    );

    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| "flat address query failed to prepare")?;

    let mut addresses = Vec::with_capacity(20_000_000);
    let mut skipped = 0usize;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?, // town
            row.get::<_, Option<String>>(1)?, // city
            row.get::<_, Option<String>>(2)?, // housenumber
            row.get::<_, f64>(3)?,            // lat
            row.get::<_, f64>(4)?,            // lon
        ))
    })?;

    for row in rows {
        let (town, city, housenumber, lat, lon) = match row {
            Ok(r) => r,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Bbox filter
        if lat < JP_LAT_MIN || lat > JP_LAT_MAX || lon < JP_LON_MIN || lon > JP_LON_MAX {
            skipped += 1;
            continue;
        }

        let street = match town {
            Some(ref t) if !t.trim().is_empty() => t.trim().to_string(),
            _ => {
                skipped += 1;
                continue;
            }
        };

        let housenumber = match housenumber {
            Some(ref h) if !h.trim().is_empty() => fullwidth_to_halfwidth(h.trim()),
            _ => {
                skipped += 1;
                continue;
            }
        };

        let city = city
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        addresses.push(RawAddress {
            osm_id: 0,
            street,
            housenumber,
            postcode: None,
            city,
            state: None,
            lat,
            lon,
        });
    }

    info!(
        "  Flat query — {} addresses ({} skipped)",
        addresses.len(),
        skipped,
    );

    Ok(addresses)
}
