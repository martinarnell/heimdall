/// bev.rs — Parse BEV Adressregister for Austria
///
/// Austria's authoritative address database (~2.2M addresses), published by
/// the Bundesamt fuer Eich- und Vermessungswesen (BEV).
///
/// Three-table join from a directory of semicolon-delimited CSVs:
///   STRASSE.csv   — (GKZ, SKZ) → street name
///   GEMEINDE.csv  — GKZ → municipality name
///   ADRESSE.csv   — addresses with MGI Gauss-Krueger coordinates (3 zones)
///
/// Coordinates are in MGI Gauss-Krueger (EPSG:31254/31255/31256), converted
/// to WGS84 via inverse Transverse Mercator + Molodensky datum shift.
///
/// Download: https://www.bev.gv.at/Services/Produkte/Adressen.html

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

/// Import BEV addresses from a directory of extracted CSV files.
///
/// Expects `bev_dir` to contain STRASSE.csv, GEMEINDE.csv, and ADRESSE.csv.
pub fn read_bev_addresses(bev_dir: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading BEV addresses from {}", bev_dir.display());

    // Step 1: Load STRASSE.csv → HashMap<(GKZ, SKZ), street_name>
    let streets = load_streets(&bev_dir.join("STRASSE.csv"))?;
    info!("  Loaded {} streets", streets.len());

    // Step 2: Load GEMEINDE.csv → HashMap<GKZ, municipality_name>
    let municipalities = load_municipalities(&bev_dir.join("GEMEINDE.csv"))?;
    info!("  Loaded {} municipalities", municipalities.len());

    // Step 3: Read ADRESSE.csv, join with streets and municipalities
    let addresses = join_addresses(
        &bev_dir.join("ADRESSE.csv"),
        &streets,
        &municipalities,
    )?;
    info!("Parsed {} BEV addresses", addresses.len());

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// CSV parsing helpers
// ───────────────────────────────────────────────────────────────────────────

/// Find column index by name in a semicolon-delimited header.
fn col_index(header: &[&str], name: &str) -> Option<usize> {
    header
        .iter()
        .position(|c| c.trim().trim_matches('"') == name)
}

/// Title-case a string: "WIEN" -> "Wien"
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

/// Strip UTF-8 BOM if present.
fn strip_bom(s: &str) -> &str {
    s.strip_prefix('\u{feff}').unwrap_or(s)
}

// ───────────────────────────────────────────────────────────────────────────
// Coordinate conversion: MGI Gauss-Krueger → WGS84
// ───────────────────────────────────────────────────────────────────────────

/// Zone parameters for Austrian MGI Gauss-Krueger projections.
struct GkZone {
    central_meridian: f64, // radians
    false_easting: f64,    // meters
}

/// Get zone parameters for the given EPSG code.
fn gk_zone(epsg: u32) -> Option<GkZone> {
    match epsg {
        31254 => Some(GkZone {
            central_meridian: (10.0 + 20.0 / 60.0_f64).to_radians(),
            false_easting: 150_000.0,
        }),
        31255 => Some(GkZone {
            central_meridian: (13.0 + 20.0 / 60.0_f64).to_radians(),
            false_easting: 450_000.0,
        }),
        31256 => Some(GkZone {
            central_meridian: (16.0 + 20.0 / 60.0_f64).to_radians(),
            false_easting: 750_000.0,
        }),
        _ => None,
    }
}

// Bessel 1841 ellipsoid parameters
const A_BESSEL: f64 = 6_377_397.155;
const F_BESSEL: f64 = 1.0 / 299.1528128;

/// Inverse Transverse Mercator: projected (easting, northing) → geographic (lat, lon) in radians.
///
/// Uses Bessel 1841 ellipsoid with k0 = 1.0 (Gauss-Krueger convention).
fn inverse_transverse_mercator(
    easting: f64,
    northing: f64,
    lambda0: f64,
    false_easting: f64,
) -> (f64, f64) {
    let a = A_BESSEL;
    let f = F_BESSEL;
    let k0 = 1.0;

    let e2 = 2.0 * f - f * f;
    let e1 = (1.0 - (1.0 - e2).sqrt()) / (1.0 + (1.0 - e2).sqrt());

    // Remove false easting, scale
    let x = (easting - false_easting) / k0;
    let m = northing / k0;

    // Meridian arc → footpoint latitude
    let mu = m / (a * (1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2 * e2 * e2 / 256.0));

    let phi1 = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1 * e1 * e1 / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1 * e1 / 16.0 - 55.0 * e1 * e1 * e1 * e1 / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1 * e1 * e1 / 96.0) * (6.0 * mu).sin()
        + (1097.0 * e1 * e1 * e1 * e1 / 512.0) * (8.0 * mu).sin();

    // Auxiliary values at footpoint latitude
    let sin_phi1 = phi1.sin();
    let cos_phi1 = phi1.cos();
    let tan_phi1 = phi1.tan();

    let ep2 = e2 / (1.0 - e2); // e'^2
    let n1 = a / (1.0 - e2 * sin_phi1 * sin_phi1).sqrt();
    let r1 = a * (1.0 - e2) / (1.0 - e2 * sin_phi1 * sin_phi1).powf(1.5);
    let t1 = tan_phi1 * tan_phi1;
    let c1 = ep2 * cos_phi1 * cos_phi1;
    let d = x / n1;

    // Latitude
    let lat = phi1
        - (n1 * tan_phi1 / r1)
            * (d * d / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * ep2)
                    * d * d * d * d / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1
                    - 252.0 * ep2
                    - 3.0 * c1 * c1)
                    * d * d * d * d * d * d / 720.0);

    // Longitude
    let lon = lambda0
        + (d - (1.0 + 2.0 * t1 + c1) * d * d * d / 6.0
            + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * ep2 + 24.0 * t1 * t1)
                * d * d * d * d * d / 120.0)
            / cos_phi1;

    (lat, lon)
}

/// Molodensky datum transformation: Bessel/MGI → WGS84.
///
/// Shifts geographic coordinates from the MGI datum (Bessel 1841 ellipsoid)
/// to WGS84 (GRS80/WGS84 ellipsoid). Accuracy ~5m, sufficient for geocoding.
fn mgi_to_wgs84(lat_rad: f64, lon_rad: f64) -> (f64, f64) {
    let a = A_BESSEL;
    let f = F_BESSEL;
    let a_wgs84 = 6_378_137.0;
    let f_wgs84 = 1.0 / 298.257223563;

    let da = a_wgs84 - a;
    let df = f_wgs84 - f;

    // MGI → WGS84 Molodensky parameters
    let dx = 577.326_f64;
    let dy = 90.129_f64;
    let dz = 463.919_f64;

    let sin_lat = lat_rad.sin();
    let cos_lat = lat_rad.cos();
    let sin_lon = lon_rad.sin();
    let cos_lon = lon_rad.cos();

    let e2 = 2.0 * f - f * f;
    let b = a * (1.0 - f);

    let rn = a / (1.0 - e2 * sin_lat * sin_lat).sqrt();
    let rm = a * (1.0 - e2) / (1.0 - e2 * sin_lat * sin_lat).powf(1.5);

    // Molodensky formulas (h = 0 for sea-level approximation)
    let dlat = (-dx * sin_lat * cos_lon - dy * sin_lat * sin_lon + dz * cos_lat
        + da * (rn * e2 * sin_lat * cos_lat) / a
        + df * (rm * a / b + rn * b / a) * sin_lat * cos_lat)
        / rm;

    let dlon = (-dx * sin_lon + dy * cos_lon) / (rn * cos_lat);

    (lat_rad + dlat, lon_rad + dlon)
}

/// Convert Austrian MGI Gauss-Krueger coordinates to WGS84 (lat, lon) in degrees.
fn gk_to_wgs84(rw: f64, hw: f64, epsg: u32) -> Option<(f64, f64)> {
    let zone = gk_zone(epsg)?;

    // Step 1: Inverse TM on Bessel ellipsoid → geographic (radians)
    let (lat_bessel, lon_bessel) =
        inverse_transverse_mercator(rw, hw, zone.central_meridian, zone.false_easting);

    // Step 2: Molodensky datum shift → WGS84 (radians)
    let (lat_wgs, lon_wgs) = mgi_to_wgs84(lat_bessel, lon_bessel);

    // Convert to degrees
    Some((lat_wgs.to_degrees(), lon_wgs.to_degrees()))
}

// ───────────────────────────────────────────────────────────────────────────
// Step 1: STRASSE.csv → (GKZ, SKZ) → street name
// ───────────────────────────────────────────────────────────────────────────

fn load_streets(path: &Path) -> Result<HashMap<(String, String), String>> {
    let reader = BufReader::new(std::fs::File::open(path)?);
    let mut lines = reader.lines();

    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty STRASSE.csv"))??;
    let header = strip_bom(&header);
    let cols: Vec<&str> = header.split(';').collect();

    let i_gkz = col_index(&cols, "GKZ")
        .ok_or_else(|| anyhow::anyhow!("missing GKZ column in STRASSE.csv"))?;
    let i_skz = col_index(&cols, "SKZ")
        .ok_or_else(|| anyhow::anyhow!("missing SKZ column in STRASSE.csv"))?;
    let i_name = col_index(&cols, "STRASSENNAME")
        .ok_or_else(|| anyhow::anyhow!("missing STRASSENNAME column in STRASSE.csv"))?;

    let mut map = HashMap::new();

    for line in lines {
        let line = line?;
        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= i_gkz.max(i_skz).max(i_name) {
            continue;
        }

        let gkz = fields[i_gkz].trim().trim_matches('"').to_owned();
        let skz = fields[i_skz].trim().trim_matches('"').to_owned();
        let name = fields[i_name].trim().trim_matches('"');

        if gkz.is_empty() || skz.is_empty() || name.is_empty() {
            continue;
        }

        map.insert((gkz, skz), name.to_owned());
    }

    Ok(map)
}

// ───────────────────────────────────────────────────────────────────────────
// Step 2: GEMEINDE.csv → GKZ → municipality name
// ───────────────────────────────────────────────────────────────────────────

fn load_municipalities(path: &Path) -> Result<HashMap<String, String>> {
    let reader = BufReader::new(std::fs::File::open(path)?);
    let mut lines = reader.lines();

    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty GEMEINDE.csv"))??;
    let header = strip_bom(&header);
    let cols: Vec<&str> = header.split(';').collect();

    let i_gkz = col_index(&cols, "GKZ")
        .ok_or_else(|| anyhow::anyhow!("missing GKZ column in GEMEINDE.csv"))?;
    let i_name = col_index(&cols, "GEMEINDENAME")
        .ok_or_else(|| anyhow::anyhow!("missing GEMEINDENAME column in GEMEINDE.csv"))?;

    let mut map = HashMap::new();

    for line in lines {
        let line = line?;
        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= i_gkz.max(i_name) {
            continue;
        }

        let gkz = fields[i_gkz].trim().trim_matches('"').to_owned();
        let name = fields[i_name].trim().trim_matches('"');

        if gkz.is_empty() || name.is_empty() {
            continue;
        }

        map.insert(gkz, name.to_owned());
    }

    Ok(map)
}

// ───────────────────────────────────────────────────────────────────────────
// Step 3: ADRESSE.csv — join with streets and municipalities → RawAddress
// ───────────────────────────────────────────────────────────────────────────

fn join_addresses(
    path: &Path,
    streets: &HashMap<(String, String), String>,
    municipalities: &HashMap<String, String>,
) -> Result<Vec<RawAddress>> {
    let reader = BufReader::new(std::fs::File::open(path)?);
    let mut lines = reader.lines();

    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty ADRESSE.csv"))??;
    let header = strip_bom(&header);
    let cols: Vec<&str> = header.split(';').collect();

    let i_gkz = col_index(&cols, "GKZ")
        .ok_or_else(|| anyhow::anyhow!("missing GKZ column in ADRESSE.csv"))?;
    let i_skz = col_index(&cols, "SKZ")
        .ok_or_else(|| anyhow::anyhow!("missing SKZ column in ADRESSE.csv"))?;
    let i_plz = col_index(&cols, "PLZ")
        .ok_or_else(|| anyhow::anyhow!("missing PLZ column in ADRESSE.csv"))?;
    let i_hnr1 = col_index(&cols, "HAUSNRZAHL1")
        .ok_or_else(|| anyhow::anyhow!("missing HAUSNRZAHL1 column in ADRESSE.csv"))?;
    let i_hnb1 = col_index(&cols, "HAUSNRBUCHSTABE1");
    let i_hnr2 = col_index(&cols, "HAUSNRZAHL2");
    let i_hnb2 = col_index(&cols, "HAUSNRBUCHSTABE2");
    let i_epsg = col_index(&cols, "EPSG")
        .ok_or_else(|| anyhow::anyhow!("missing EPSG column in ADRESSE.csv"))?;
    let i_rw = col_index(&cols, "RW")
        .ok_or_else(|| anyhow::anyhow!("missing RW column in ADRESSE.csv"))?;
    let i_hw = col_index(&cols, "HW")
        .ok_or_else(|| anyhow::anyhow!("missing HW column in ADRESSE.csv"))?;
    let i_gnr = col_index(&cols, "GNRADRESSE");

    let max_col = *[i_gkz, i_skz, i_plz, i_hnr1, i_epsg, i_rw, i_hw]
        .iter()
        .max()
        .unwrap();

    let mut addresses = Vec::with_capacity(2_500_000);
    let mut skipped = 0usize;
    let mut no_street = 0usize;
    let mut bad_epsg = 0usize;

    for line in lines {
        let line = line?;
        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= max_col {
            skipped += 1;
            continue;
        }

        // Filter: only active addresses (GNRADRESSE = "G")
        if let Some(i_g) = i_gnr {
            if i_g < fields.len() {
                let gnr = fields[i_g].trim().trim_matches('"');
                if gnr != "G" {
                    skipped += 1;
                    continue;
                }
            }
        }

        // Parse EPSG code
        let epsg_str = fields[i_epsg].trim().trim_matches('"');
        if epsg_str.is_empty() || epsg_str == "NULL" {
            skipped += 1;
            continue;
        }
        let epsg: u32 = match epsg_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Parse coordinates
        let rw_str = fields[i_rw].trim().trim_matches('"');
        let hw_str = fields[i_hw].trim().trim_matches('"');
        if rw_str.is_empty() || hw_str.is_empty() || rw_str == "NULL" || hw_str == "NULL" {
            skipped += 1;
            continue;
        }
        let rw: f64 = match rw_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let hw: f64 = match hw_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Convert to WGS84
        let (lat, lon) = match gk_to_wgs84(rw, hw, epsg) {
            Some(coords) => coords,
            None => {
                bad_epsg += 1;
                continue;
            }
        };

        // Austria bbox: 46.37-49.02N, 9.53-17.16E
        if lat < 46.37 || lat > 49.02 || lon < 9.53 || lon > 17.16 {
            skipped += 1;
            continue;
        }

        // Join street name via (GKZ, SKZ)
        let gkz = fields[i_gkz].trim().trim_matches('"').to_owned();
        let skz = fields[i_skz].trim().trim_matches('"').to_owned();
        let street = match streets.get(&(gkz.clone(), skz)) {
            Some(name) => name.clone(),
            None => {
                no_street += 1;
                continue;
            }
        };

        // House number: HAUSNRZAHL1 + HAUSNRBUCHSTABE1 [+ "-" + HAUSNRZAHL2 + HAUSNRBUCHSTABE2]
        let hnr1 = fields[i_hnr1].trim().trim_matches('"');
        if hnr1.is_empty() || hnr1 == "0" {
            skipped += 1;
            continue;
        }
        let hnb1 = i_hnb1
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim().trim_matches('"'))
            .unwrap_or("");
        let hnr2 = i_hnr2
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim().trim_matches('"'))
            .unwrap_or("");
        let hnb2 = i_hnb2
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim().trim_matches('"'))
            .unwrap_or("");

        let mut housenumber = format!("{}{}", hnr1, hnb1);
        if !hnr2.is_empty() && hnr2 != "0" {
            housenumber.push('-');
            housenumber.push_str(hnr2);
            housenumber.push_str(hnb2);
        }

        // Municipality (city) via GKZ
        let city = municipalities.get(&gkz).map(|name| title_case(name));

        // Postcode
        let plz = fields[i_plz].trim().trim_matches('"');
        let postcode = if plz.is_empty() {
            None
        } else {
            Some(plz.to_owned())
        };

        addresses.push(RawAddress {
            osm_id: 0,
            street,
            housenumber,
            postcode,
            city,
            state: None,
            lat,
            lon,
        });
    }

    info!(
        "  Join complete: {} addresses ({} skipped, {} no street match, {} bad EPSG)",
        addresses.len(),
        skipped,
        no_street,
        bad_epsg,
    );

    Ok(addresses)
}
