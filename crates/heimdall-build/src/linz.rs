/// linz.rs — Parse LINZ NZ Addresses (GeoPackage) for New Zealand
///
/// LINZ NZ Addresses is the national authoritative address dataset — 2.3M addresses.
/// Published by Land Information New Zealand (LINZ), CC BY 4.0 license, updated weekly.
///
/// Data format: GeoPackage (SQLite) with a single feature table.
/// Flat schema — no multi-table joins needed (unlike G-NAF).
///
/// Two modes:
///   1. Read a pre-downloaded GeoPackage file
///   2. Download via LINZ Data Service API (requires free account + API token)
///
/// Download: https://data.linz.govt.nz/layer/105689/ (requires free LINZ account + API token)

use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use rusqlite::Connection;
use tracing::info;

use crate::extract::RawAddress;

/// LINZ layer ID for NZ Street Address
const LINZ_LAYER_ID: u32 = 105689;

// ─────────────────────────────────────────────────────────────────────────────
// Download from LINZ Data Service API
// ─────────────────────────────────────────────────────────────────────────────

/// Download the LINZ NZ Addresses GeoPackage via the Data Service export API.
///
/// Process:
///   1. POST to create an export job (layer 105689, GeoPackage format, EPSG:4326)
///   2. Poll until export state is "complete"
///   3. Download the resulting GeoPackage
pub fn download_linz_gpkg(api_key: &str, download_dir: &Path) -> Result<PathBuf> {
    let out_path = download_dir.join("nz-street-address.gpkg");

    // Reuse existing download if present
    if out_path.exists() && std::fs::metadata(&out_path).map(|m| m.len() > 1000).unwrap_or(false) {
        let size_mb = std::fs::metadata(&out_path)?.len() as f64 / 1e6;
        info!("  Reusing existing LINZ download ({:.1} MB): {}", size_mb, out_path.display());
        return Ok(out_path);
    }

    std::fs::create_dir_all(download_dir)?;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(download_linz_async(api_key, &out_path))
}

async fn download_linz_async(api_key: &str, out_path: &Path) -> Result<PathBuf> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()?;

    let base = "https://data.linz.govt.nz";

    // Step 1: Create export job
    info!("  Creating LINZ export job (layer {}, GeoPackage, EPSG:4326)...", LINZ_LAYER_ID);

    let export_body = serde_json::json!({
        "crs": "EPSG:4326",
        "items": [{
            "item": format!("/services/api/v1.x/layers/{}/", LINZ_LAYER_ID)
        }],
        "formats": {
            "vector": "application/x-ogc-gpkg"
        }
    });

    let resp = client
        .post(format!("{}/services/api/v1.x/exports/", base))
        .header("Authorization", format!("key {}", api_key))
        .json(&export_body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!(
            "LINZ export creation failed (HTTP {}): {}",
            status,
            body.chars().take(500).collect::<String>()
        );
    }

    let export_resp: serde_json::Value = resp.json().await?;
    let export_url = export_resp["url"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No 'url' in LINZ export response: {}", export_resp))?
        .to_string();
    let export_id = export_resp["id"]
        .as_u64()
        .unwrap_or(0);

    info!("  Export job created (ID: {}). Polling for completion...", export_id);

    // Step 2: Poll until complete (LINZ typically takes 30s-5min for 2.3M records)
    let poll_url = if export_url.starts_with("http") {
        export_url.clone()
    } else {
        format!("{}{}", base, export_url)
    };

    let mut attempts = 0;
    let download_url = loop {
        attempts += 1;
        if attempts > 120 {
            bail!("LINZ export timed out after 120 poll attempts (~10 minutes)");
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let resp = client
            .get(&poll_url)
            .header("Authorization", format!("key {}", api_key))
            .send()
            .await?;

        let status_json: serde_json::Value = resp.json().await?;
        let state = status_json["state"]
            .as_str()
            .unwrap_or("unknown");

        match state {
            "complete" => {
                let url = status_json["download_url"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("No download_url in complete export"))?
                    .to_string();
                info!("  Export complete after {} polls", attempts);
                break url;
            }
            "processing" | "queued" => {
                if attempts % 6 == 0 {
                    info!("  Still {} ({}s elapsed)...", state, attempts * 5);
                }
            }
            "error" | "cancelled" => {
                let reason = status_json["reason"]
                    .as_str()
                    .unwrap_or("unknown reason");
                bail!("LINZ export failed: {} — {}", state, reason);
            }
            other => {
                if attempts % 12 == 0 {
                    info!("  Export state: {} ({}s elapsed)", other, attempts * 5);
                }
            }
        }
    };

    // Step 3: Download the GeoPackage
    let full_url = if download_url.starts_with("http") {
        download_url
    } else {
        format!("{}{}", base, download_url)
    };

    info!("  Downloading GeoPackage...");
    let resp = client
        .get(&full_url)
        .header("Authorization", format!("key {}", api_key))
        .send()
        .await?;

    if !resp.status().is_success() {
        bail!("LINZ download failed: HTTP {}", resp.status());
    }

    let bytes = resp.bytes().await?;
    std::fs::write(out_path, &bytes)?;
    info!(
        "  Downloaded {:.1} MB to {}",
        bytes.len() as f64 / 1e6,
        out_path.display()
    );

    Ok(out_path.to_path_buf())
}

// ─────────────────────────────────────────────────────────────────────────────
// Read GeoPackage
// ─────────────────────────────────────────────────────────────────────────────

/// Import LINZ addresses from a GeoPackage (.gpkg) file.
///
/// Opens the GeoPackage as SQLite, auto-detects the feature table,
/// and reads all address records into flat RawAddress structs.
pub fn read_linz_addresses(gpkg_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading LINZ addresses from {}", gpkg_path.display());

    let conn = Connection::open(gpkg_path)?;

    // Auto-detect the feature table name from gpkg_contents
    let table_name = detect_feature_table(&conn)?;
    info!("  Feature table: {}", table_name);

    // Verify required columns exist
    let columns = get_table_columns(&conn, &table_name)?;
    let required = ["address_number", "road_name", "gd2000_xcoord", "gd2000_ycoord"];
    for col in &required {
        if !columns.contains(&col.to_string()) {
            bail!("Missing required column '{}' in table '{}'", col, table_name);
        }
    }

    // Build SELECT — include optional columns only if they exist
    let has_suffix = columns.contains(&"address_number_suffix".to_string());
    let has_unit_value = columns.contains(&"unit_value".to_string());
    let has_road_type = columns.contains(&"road_type_name".to_string());
    let has_suburb = columns.contains(&"suburb_locality".to_string());
    let has_town = columns.contains(&"town_city".to_string());
    let has_postcode = columns.contains(&"postcode".to_string());
    let has_high = columns.contains(&"address_number_high".to_string());

    let sql = format!(
        "SELECT address_number, {suffix}, {unit}, road_name, {road_type}, \
         {suburb}, {town}, {postcode}, {high}, \
         gd2000_xcoord, gd2000_ycoord \
         FROM \"{table}\"",
        suffix = if has_suffix { "address_number_suffix" } else { "NULL" },
        unit = if has_unit_value { "unit_value" } else { "NULL" },
        road_type = if has_road_type { "road_type_name" } else { "NULL" },
        suburb = if has_suburb { "suburb_locality" } else { "NULL" },
        town = if has_town { "town_city" } else { "NULL" },
        postcode = if has_postcode { "postcode" } else { "NULL" },
        high = if has_high { "address_number_high" } else { "NULL" },
        table = table_name,
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut addresses = Vec::with_capacity(2_500_000);
    let mut skipped = 0usize;

    let rows = stmt.query_map([], |row| {
        Ok(RawRow {
            address_number: row.get::<_, Option<String>>(0)?,
            address_number_suffix: row.get::<_, Option<String>>(1)?,
            unit_value: row.get::<_, Option<String>>(2)?,
            road_name: row.get::<_, Option<String>>(3)?,
            road_type_name: row.get::<_, Option<String>>(4)?,
            suburb_locality: row.get::<_, Option<String>>(5)?,
            town_city: row.get::<_, Option<String>>(6)?,
            postcode: row.get::<_, Option<String>>(7)?,
            address_number_high: row.get::<_, Option<String>>(8)?,
            lon: row.get::<_, Option<f64>>(9)?,
            lat: row.get::<_, Option<f64>>(10)?,
        })
    })?;

    for row_result in rows {
        let row = match row_result {
            Ok(r) => r,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        let address = match parse_row(row) {
            Some(a) => a,
            None => {
                skipped += 1;
                continue;
            }
        };

        addresses.push(address);
    }

    info!(
        "Parsed {} LINZ addresses ({} skipped)",
        addresses.len(),
        skipped,
    );

    Ok(addresses)
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal types
// ─────────────────────────────────────────────────────────────────────────────

struct RawRow {
    address_number: Option<String>,
    address_number_suffix: Option<String>,
    unit_value: Option<String>,
    road_name: Option<String>,
    road_type_name: Option<String>,
    suburb_locality: Option<String>,
    town_city: Option<String>,
    postcode: Option<String>,
    address_number_high: Option<String>,
    lon: Option<f64>,
    lat: Option<f64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Auto-detect the feature table name from gpkg_contents.
fn detect_feature_table(conn: &Connection) -> Result<String> {
    // Check if gpkg_contents exists (it should in any valid GeoPackage)
    let table_exists: bool = conn.query_row(
        "SELECT count(*) > 0 FROM sqlite_master WHERE type='table' AND name='gpkg_contents'",
        [],
        |row| row.get(0),
    )?;

    if table_exists {
        // Find the first features table
        let name: Option<String> = conn.query_row(
            "SELECT table_name FROM gpkg_contents WHERE data_type='features' LIMIT 1",
            [],
            |row| row.get(0),
        ).ok();

        if let Some(name) = name {
            return Ok(name);
        }
    }

    // Fallback: look for common LINZ table names
    for candidate in &["nz_street_address", "nz_addresses", "nz-street-address"] {
        let exists: bool = conn.query_row(
            "SELECT count(*) > 0 FROM sqlite_master WHERE type='table' AND name=?1",
            [candidate],
            |row| row.get(0),
        )?;
        if exists {
            return Ok(candidate.to_string());
        }
    }

    bail!("Could not detect feature table in GeoPackage. No features table in gpkg_contents and no known LINZ table names found.")
}

/// Get column names for a table.
fn get_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(columns)
}

/// Title-case a string: "MEMORIAL AVENUE" → "Memorial Avenue"
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

/// Parse a raw row into a RawAddress, returning None if essential fields are missing.
fn parse_row(row: RawRow) -> Option<RawAddress> {
    let lat = row.lat?;
    let lon = row.lon?;

    // NZ bbox sanity check: -47 to -34S, 166 to 179E
    if lat < -47.5 || lat > -34.0 || lon < 166.0 || lon > 179.0 {
        return None;
    }

    let address_number = row.address_number.as_deref()?.trim().to_string();
    if address_number.is_empty() {
        return None;
    }

    let road_name = row.road_name.as_deref()?.trim().to_string();
    if road_name.is_empty() {
        return None;
    }

    // Build house number: "45A" or "10-14" or "1/45A"
    let suffix = row
        .address_number_suffix
        .as_deref()
        .unwrap_or("")
        .trim();
    let high = row
        .address_number_high
        .as_deref()
        .unwrap_or("")
        .trim();
    let unit = row.unit_value.as_deref().unwrap_or("").trim();

    let base_number = if !high.is_empty() {
        format!("{}-{}{}", address_number, high, suffix)
    } else {
        format!("{}{}", address_number, suffix)
    };

    let housenumber = if !unit.is_empty() {
        format!("{}/{}", unit, base_number)
    } else {
        base_number
    };

    // Build street: "Memorial Avenue"
    let road_type = row
        .road_type_name
        .as_deref()
        .unwrap_or("")
        .trim();
    let street = if !road_type.is_empty() {
        format!("{} {}", title_case(&road_name), title_case(road_type))
    } else {
        title_case(&road_name)
    };

    // City: prefer suburb_locality, fall back to town_city
    let city = row
        .suburb_locality
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| row.town_city.as_deref().filter(|s| !s.trim().is_empty()))
        .map(|s| title_case(s.trim()));

    // Postcode (NZ uses 4-digit postcodes)
    let postcode = row
        .postcode
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string());

    Some(RawAddress {
        osm_id: 0,
        street,
        housenumber,
        postcode,
        city,
        state: None,
        lat,
        lon,
    })
}
