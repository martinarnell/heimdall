/// lantmateriet.rs — Download and extract Lantmäteriet address data
///
/// Data source: Belägenhetsadress Nedladdning, vektor (GeoPackage)
/// License: CC BY 4.0 (free, requires Lantmäteriet account)
/// CRS: SWEREF99TM (EPSG:3006) → converted to WGS84
///
/// Pipeline:
///   1. Download all 290 municipality GeoPackage files from STAC API
///   2. Extract addresses from each GeoPackage (SQLite)
///   3. Convert SWEREF99TM coordinates to WGS84
///   4. Merge with existing OSM addresses (dedup at 10m)
///   5. Write merged addresses.parquet

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use anyhow::{Result, Context};
use tracing::info;

use crate::extract::RawAddress;

// ---------------------------------------------------------------------------
// SWEREF99TM (EPSG:3006) → WGS84 coordinate transform
// ---------------------------------------------------------------------------
// SWEREF99TM is a Transverse Mercator projection. Parameters:
//   Central meridian: 15°E
//   Scale factor: 0.9996
//   False easting: 500000
//   False northing: 0
//   Ellipsoid: GRS80 (a=6378137, f=1/298.257222101)

/// Convert SWEREF99TM (easting, northing) to WGS84 (lat, lon) in degrees.
/// Uses iterative inverse Transverse Mercator projection.
///
/// Parameters for SWEREF99TM (EPSG:3006):
///   Ellipsoid: GRS80 (a=6378137, 1/f=298.257222101)
///   Central meridian: 15°E
///   Scale factor: 0.9996
///   False easting: 500000, False northing: 0
pub fn sweref99tm_to_wgs84(easting: f64, northing: f64) -> (f64, f64) {
    const A: f64 = 6_378_137.0;        // semi-major axis
    const F: f64 = 1.0 / 298.257_222_101; // flattening
    const LAM0: f64 = 15.0;            // central meridian (degrees)
    const K0: f64 = 0.9996;            // scale factor
    const FE: f64 = 500_000.0;         // false easting
    const FN: f64 = 0.0;              // false northing

    let e2 = F * (2.0 - F);
    let e = e2.sqrt();
    let n = F / (2.0 - F);
    let n2 = n * n;
    let n3 = n2 * n;
    let n4 = n3 * n;

    // Rectifying radius
    let a_hat = A / (1.0 + n) * (1.0 + n2 / 4.0 + n4 / 64.0);

    // Inverse series coefficients (delta) for xi'/eta' → phi_star
    let d1 = n / 2.0 - 2.0 * n2 / 3.0 + 37.0 * n3 / 96.0 - n4 / 360.0;
    let d2 = n2 / 48.0 + n3 / 15.0 - 437.0 * n4 / 1440.0;
    let d3 = 17.0 * n3 / 480.0 - 37.0 * n4 / 840.0;
    let d4 = 4397.0 * n4 / 161_280.0;

    // Footpoint latitude series (from conformal to geodetic)
    let a_star = e2 + e2 * e2 + e2 * e2 * e2 + e2 * e2 * e2 * e2;
    let b_star = -(7.0 * e2 * e2 + 17.0 * e2 * e2 * e2 + 30.0 * e2 * e2 * e2 * e2) / 6.0;
    let c_star = (224.0 * e2 * e2 * e2 + 889.0 * e2 * e2 * e2 * e2) / 120.0;
    let d_star = -(4279.0 * e2 * e2 * e2 * e2) / 1260.0;

    let xi = (northing - FN) / (K0 * a_hat);
    let eta = (easting - FE) / (K0 * a_hat);

    // Remove series terms
    let xi_prim = xi
        - d1 * (2.0 * xi).sin() * (2.0 * eta).cosh()
        - d2 * (4.0 * xi).sin() * (4.0 * eta).cosh()
        - d3 * (6.0 * xi).sin() * (6.0 * eta).cosh()
        - d4 * (8.0 * xi).sin() * (8.0 * eta).cosh();

    let eta_prim = eta
        - d1 * (2.0 * xi).cos() * (2.0 * eta).sinh()
        - d2 * (4.0 * xi).cos() * (4.0 * eta).sinh()
        - d3 * (6.0 * xi).cos() * (6.0 * eta).sinh()
        - d4 * (8.0 * xi).cos() * (8.0 * eta).sinh();

    // Conformal latitude
    let phi_star = (xi_prim.sin() / eta_prim.cosh()).asin();
    let lambda = LAM0.to_radians() + eta_prim.sinh().atan2(xi_prim.cos());

    // Convert conformal latitude to geodetic latitude
    let phi = phi_star
        + (a_star / 2.0) * (2.0 * phi_star).sin()
        + (b_star / 4.0) * (4.0 * phi_star).sin()
        + (c_star / 6.0) * (6.0 * phi_star).sin()
        + (d_star / 8.0) * (8.0 * phi_star).sin();

    (phi.to_degrees(), lambda.to_degrees())
}

// ---------------------------------------------------------------------------
// GeoPackage reader
// ---------------------------------------------------------------------------

/// Extract addresses from a Lantmäteriet GeoPackage file.
pub fn read_geopackage(gpkg_path: &Path) -> Result<Vec<RawAddress>> {
    let conn = rusqlite::Connection::open(gpkg_path)
        .with_context(|| format!("opening {}", gpkg_path.display()))?;

    // The table name is 'belagenhetsadress'
    // Key fields:
    //   adressomrade_faststalltnamn  → street name (address area)
    //   adressplatsnummer            → house number
    //   bokstavstillagg             → letter suffix (e.g., "B")
    //   postnummer                  → postal code
    //   postort                     → postal city
    //   kommunnamn                  → municipality name
    //   geom                        → POINT geometry in SWEREF99TM

    let mut stmt = conn.prepare(
        "SELECT adressomrade_faststalltnamn, adressplatsnummer, bokstavstillagg,
                postnummer, postort, kommunnamn, geom
         FROM belagenhetsadress
         WHERE adressomrade_faststalltnamn IS NOT NULL
           AND adressplatsnummer IS NOT NULL
           AND geom IS NOT NULL"
    )?;

    let mut addresses = Vec::new();
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let street: String = row.get(0)?;
        let number: String = row.get(1)?;
        let suffix: Option<String> = row.get(2)?;
        let postcode: Option<i32> = row.get(3)?;
        let city: Option<String> = row.get(4)?;
        let municipality: Option<String> = row.get(5)?;
        let geom: Vec<u8> = row.get(6)?;

        // Combine number + suffix
        let housenumber = match suffix {
            Some(s) if !s.is_empty() => format!("{}{}", number, s),
            _ => number,
        };

        // Parse GeoPackage geometry (GeoPackage Binary format)
        let (easting, northing) = match parse_gpkg_point(&geom) {
            Some(coords) => coords,
            None => continue,
        };

        // Convert to WGS84
        let (lat, lon) = sweref99tm_to_wgs84(easting, northing);

        // Sanity check
        if lat < 55.0 || lat > 69.5 || lon < 10.5 || lon > 24.5 {
            continue;
        }

        let postcode_str = postcode.map(|p| format!("{:05}", p));

        addresses.push(RawAddress {
            osm_id: 0, // Lantmäteriet addresses don't have OSM IDs
            street,
            housenumber,
            postcode: postcode_str,
            city: city.or(municipality),
            state: None,
            lat,
            lon,
        });
    }

    Ok(addresses)
}

/// Parse a GeoPackage Binary point geometry.
/// Format: [magic(2)][flags(1)][srs_id(4)][envelope...][wkb...]
/// The WKB point is: [byte_order(1)][type(4)][x(8)][y(8)]
fn parse_gpkg_point(geom: &[u8]) -> Option<(f64, f64)> {
    if geom.len() < 13 { return None; }

    // GeoPackage magic: 'GP'
    if geom[0] != b'G' || geom[1] != b'P' {
        return None;
    }

    let flags = geom[3];
    let envelope_type = (flags >> 1) & 0x07;

    // Calculate envelope size
    let envelope_size = match envelope_type {
        0 => 0,       // no envelope
        1 => 32,      // [minx, maxx, miny, maxy]
        2 | 3 => 48,  // + Z or M
        4 => 64,      // + Z and M
        _ => return None,
    };

    let wkb_offset = 8 + envelope_size; // 8 bytes header + envelope
    if geom.len() < wkb_offset + 21 { return None; }

    let wkb = &geom[wkb_offset..];
    let byte_order = wkb[0]; // 0 = big endian, 1 = little endian

    let (x, y) = if byte_order == 1 {
        // Little endian
        let x = f64::from_le_bytes(wkb[5..13].try_into().ok()?);
        let y = f64::from_le_bytes(wkb[13..21].try_into().ok()?);
        (x, y)
    } else {
        // Big endian
        let x = f64::from_be_bytes(wkb[5..13].try_into().ok()?);
        let y = f64::from_be_bytes(wkb[13..21].try_into().ok()?);
        (x, y)
    };

    Some((x, y))
}

// ---------------------------------------------------------------------------
// Download all municipality files
// ---------------------------------------------------------------------------

pub async fn download_all(
    output_dir: &Path,
    username: &str,
    password: &str,
) -> Result<Vec<PathBuf>> {
    info!("Fetching municipality list from STAC API...");
    let client = reqwest::Client::new();

    let resp = client
        .get("https://api.lantmateriet.se/stac-vektor/v1/collections/belagenhetsadresser/items?limit=300")
        .send()
        .await?;

    let data: serde_json::Value = resp.json().await?;
    let items = data["features"].as_array()
        .context("no features in STAC response")?;

    info!("Found {} municipalities to download", items.len());
    std::fs::create_dir_all(output_dir)?;

    let mut paths = Vec::new();
    let total = items.len();

    for (i, item) in items.iter().enumerate() {
        let url = item["assets"]["data"]["href"]
            .as_str()
            .context("no download URL")?;

        let filename = url.rsplit('/').next().unwrap_or("unknown.zip");
        let zip_path = output_dir.join(filename);
        let gpkg_name = filename.replace(".zip", ".gpkg");
        let gpkg_path = output_dir.join(&gpkg_name);

        // Skip if already extracted
        if gpkg_path.exists() {
            paths.push(gpkg_path);
            continue;
        }

        if (i + 1) % 25 == 0 || i == 0 {
            info!("[{}/{}] Downloading {}...", i + 1, total, filename);
        }

        let resp = client
            .get(url)
            .basic_auth(username, Some(password))
            .send()
            .await?;

        if !resp.status().is_success() {
            tracing::warn!("Failed to download {}: HTTP {}", filename, resp.status());
            continue;
        }

        let bytes = resp.bytes().await?;
        std::fs::write(&zip_path, &bytes)?;

        // Extract GeoPackage from zip
        let file = std::fs::File::open(&zip_path)?;
        let mut archive = zip::ZipArchive::new(file)?;

        for j in 0..archive.len() {
            let mut entry = archive.by_index(j)?;
            if entry.name().ends_with(".gpkg") {
                let mut out = std::fs::File::create(&gpkg_path)?;
                std::io::copy(&mut entry, &mut out)?;
                break;
            }
        }

        // Remove zip to save space
        let _ = std::fs::remove_file(&zip_path);

        if gpkg_path.exists() {
            paths.push(gpkg_path);
        }

        // Rate limit: ~2 downloads per second
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    info!("Downloaded {} municipality files", paths.len());
    Ok(paths)
}

// ---------------------------------------------------------------------------
// Merge with OSM addresses (dedup at 10m)
// ---------------------------------------------------------------------------

pub fn merge_addresses(
    osm_addresses: &[RawAddress],
    lm_addresses: &[RawAddress],
) -> Vec<RawAddress> {
    info!(
        "Merging {} OSM + {} new addresses...",
        osm_addresses.len(),
        lm_addresses.len()
    );

    // Build a lightweight dedup index from OSM: key → Vec<(lat, lon)>
    // This is ~40 bytes per entry — much smaller than full RawAddress
    let mut osm_index: HashMap<String, Vec<(f64, f64)>> = HashMap::new();

    for addr in osm_addresses {
        let key = format!("{}:{}", addr.street.to_lowercase(), addr.housenumber.to_lowercase());
        osm_index
            .entry(key)
            .or_default()
            .push((addr.lat, addr.lon));
    }

    // Count non-duplicates first, then build result
    let mut added = 0usize;
    let mut deduped = 0usize;
    let mut new_addrs: Vec<RawAddress> = Vec::new();

    for lm_addr in lm_addresses {
        let key = format!(
            "{}:{}",
            lm_addr.street.to_lowercase(),
            lm_addr.housenumber.to_lowercase()
        );

        let is_dup = if let Some(osm_coords) = osm_index.get(&key) {
            osm_coords.iter().any(|(olat, olon)| {
                approx_distance_m(lm_addr.lat, lm_addr.lon, *olat, *olon) < 10.0
            })
        } else {
            false
        };

        if is_dup {
            deduped += 1;
        } else {
            new_addrs.push(lm_addr.clone());
            added += 1;
        }
    }

    // Drop the dedup index before building the merged result
    drop(osm_index);

    // Build merged: osm (no clone, move/extend) + new
    let osm_count = osm_addresses.len();
    let mut merged = Vec::with_capacity(osm_count + added);
    merged.extend_from_slice(osm_addresses);
    merged.append(&mut new_addrs);

    info!(
        "Merge result: {} total ({} OSM kept, {} LM added, {} LM deduped)",
        merged.len(),
        osm_count,
        added,
        deduped,
    );

    merged
}

/// Fast approximate distance in meters (equirectangular)
fn approx_distance_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians() * ((lat1 + lat2) / 2.0).to_radians().cos();
    ((dlat * dlat + dlon * dlon).sqrt() * 6_371_000.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sweref99tm_to_wgs84_stockholm() {
        // Stockholm City Hall: SWEREF99TM ~(674032, 6580822)
        // Expected WGS84: ~(59.327, 18.054)
        let (lat, lon) = sweref99tm_to_wgs84(674032.0, 6580822.0);
        assert!((lat - 59.327).abs() < 0.01, "lat: {}", lat);
        assert!((lon - 18.054).abs() < 0.01, "lon: {}", lon);
    }

    #[test]
    fn test_sweref99tm_to_wgs84_malmo() {
        // Malmö: SWEREF99TM ~(374415, 6163577)
        // Expected WGS84: ~(55.605, 13.000)
        let (lat, lon) = sweref99tm_to_wgs84(374415.0, 6163577.0);
        assert!((lat - 55.605).abs() < 0.01, "lat: {}", lat);
        assert!((lon - 13.000).abs() < 0.01, "lon: {}", lon);
    }

    #[test]
    fn test_sweref99tm_to_wgs84_goteborg() {
        // Göteborg: SWEREF99TM ~(319262, 6399698)
        // Expected WGS84: ~(57.707, 11.967)
        let (lat, lon) = sweref99tm_to_wgs84(319262.0, 6399698.0);
        assert!((lat - 57.707).abs() < 0.01, "lat: {}", lat);
        assert!((lon - 11.967).abs() < 0.01, "lon: {}", lon);
    }
}
