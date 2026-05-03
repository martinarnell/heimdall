/// vzd.rs — Parse Latvian VZD (Valsts zemes dienests) address CSV
///
/// The State Land Service of Latvia publishes the national address register
/// as CSV exports on data.gov.lv.
///
/// Semicolon-delimited. Coordinates may be in LKS-92 (EPSG:3059, Transverse Mercator)
/// or WGS84. We prefer WGS84 columns if present, otherwise convert from LKS-92.

use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use crate::extract::RawAddress;

/// Latvia bounding box
const LV_LAT_MIN: f64 = 55.67;
const LV_LAT_MAX: f64 = 58.09;
const LV_LON_MIN: f64 = 20.97;
const LV_LON_MAX: f64 = 28.24;

/// Import Latvian VZD addresses from a semicolon-delimited CSV file.
pub fn read_vzd_addresses(csv_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading Latvian VZD addresses from {}", csv_path.display());

    let reader = BufReader::new(
        std::fs::File::open(csv_path)
            .with_context(|| format!("failed to open {}", csv_path.display()))?,
    );
    let mut lines = reader.lines();

    // Parse header
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty VZD CSV file"))??;
    let header = header.trim_start_matches('\u{feff}');
    let cols: Vec<&str> = header.split(';').collect();

    // Detect coordinate columns
    let coord_mode = detect_coord_columns(&cols);
    info!("  Coordinate mode: {:?}", coord_mode);

    // Find address columns — try Latvian and English names
    let i_street = col_index(&cols, "iela")
        .or_else(|| col_index(&cols, "ielas_nosaukums"))
        .or_else(|| col_index(&cols, "street"))
        .or_else(|| col_index(&cols, "nosaukums"));

    let i_number = col_index(&cols, "nr")
        .or_else(|| col_index(&cols, "numurs"))
        .or_else(|| col_index(&cols, "house_number"))
        .or_else(|| col_index(&cols, "maja_nr"));

    let i_city = col_index(&cols, "pilseta")
        .or_else(|| col_index(&cols, "ciems"))
        .or_else(|| col_index(&cols, "city"))
        .or_else(|| col_index(&cols, "apdzivota_vieta"));

    let i_postcode = col_index(&cols, "pasta_indekss")
        .or_else(|| col_index(&cols, "postcode"))
        .or_else(|| col_index(&cols, "postal_code"))
        .or_else(|| col_index(&cols, "atrib_pasta_indekss"));

    if i_street.is_none() {
        info!("  WARNING: no street column found. Available columns: {}", cols.join(", "));
    }
    if i_number.is_none() {
        info!("  WARNING: no house number column found. Available columns: {}", cols.join(", "));
    }

    let mut addresses = Vec::with_capacity(1_000_000);
    let mut skipped = 0usize;
    let mut coord_errors = 0usize;

    for line in lines {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(';').collect();

        // Extract coordinates
        let (lat, lon) = match extract_coords(&fields, &coord_mode) {
            Some(coords) => coords,
            None => {
                coord_errors += 1;
                continue;
            }
        };

        // Bbox filter
        if lat < LV_LAT_MIN || lat > LV_LAT_MAX || lon < LV_LON_MIN || lon > LV_LON_MAX {
            skipped += 1;
            continue;
        }

        // Street
        let street = i_street
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(fields[idx]))
            .unwrap_or_default();
        if street.is_empty() {
            skipped += 1;
            continue;
        }

        // House number
        let number = i_number
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(fields[idx]))
            .unwrap_or_default();
        if number.is_empty() {
            skipped += 1;
            continue;
        }

        // City
        let city = i_city
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(fields[idx]))
            .filter(|s| !s.is_empty());

        // Postcode
        let postcode = i_postcode
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(fields[idx]))
            .filter(|s| !s.is_empty());

        addresses.push(RawAddress {
            osm_id: 0,
            street,
            housenumber: number,
            postcode,
            city,
            state: None,
            lat,
            lon,
        });
    }

    info!(
        "Parsed {} Latvian VZD addresses ({} skipped, {} coordinate errors)",
        addresses.len(),
        skipped,
        coord_errors,
    );

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// Column detection and coordinate handling
// ───────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum CoordMode {
    /// WGS84 latitude/longitude columns
    Wgs84 { lat_col: usize, lon_col: usize },
    /// LKS-92 (EPSG:3059) easting/northing columns
    Lks92 { x_col: usize, y_col: usize },
    /// No coordinate columns found
    None,
}

fn detect_coord_columns(cols: &[&str]) -> CoordMode {
    // Try WGS84 first
    let lat = col_index(cols, "latitude")
        .or_else(|| col_index(cols, "lat"))
        .or_else(|| col_index(cols, "dd_lat"));
    let lon = col_index(cols, "longitude")
        .or_else(|| col_index(cols, "lon"))
        .or_else(|| col_index(cols, "dd_long"));

    if let (Some(lat_col), Some(lon_col)) = (lat, lon) {
        return CoordMode::Wgs84 { lat_col, lon_col };
    }

    // Try LKS-92
    let x = col_index(cols, "koord_x")
        .or_else(|| col_index(cols, "x"))
        .or_else(|| col_index(cols, "lks_x"));
    let y = col_index(cols, "koord_y")
        .or_else(|| col_index(cols, "y"))
        .or_else(|| col_index(cols, "lks_y"));

    if let (Some(x_col), Some(y_col)) = (x, y) {
        return CoordMode::Lks92 { x_col, y_col };
    }

    CoordMode::None
}

fn extract_coords(fields: &[&str], mode: &CoordMode) -> Option<(f64, f64)> {
    match mode {
        CoordMode::Wgs84 { lat_col, lon_col } => {
            let lat: f64 = fields.get(*lat_col)?.trim().trim_matches('"').parse().ok()?;
            let lon: f64 = fields.get(*lon_col)?.trim().trim_matches('"').parse().ok()?;
            Some((lat, lon))
        }
        CoordMode::Lks92 { x_col, y_col } => {
            let x: f64 = fields.get(*x_col)?.trim().trim_matches('"').parse().ok()?;
            let y: f64 = fields.get(*y_col)?.trim().trim_matches('"').parse().ok()?;
            lks92_to_wgs84(x, y)
        }
        CoordMode::None => None,
    }
}

// ───────────────────────────────────────────────────────────────────────────
// LKS-92 (EPSG:3059) → WGS84 inverse Transverse Mercator
// ───────────────────────────────────────────────────────────────────────────
//
// Parameters:
//   Ellipsoid: GRS80 (a=6378137.0, f=1/298.257222101)
//   Central meridian: λ₀=24°, Scale factor: k₀=0.9996
//   False easting: 500000, False northing: 0

fn lks92_to_wgs84(x: f64, y: f64) -> Option<(f64, f64)> {
    let a: f64 = 6_378_137.0;
    let f: f64 = 1.0 / 298.257_222_101;
    let e2 = 2.0 * f - f * f;
    let _e = e2.sqrt();
    let e_prime2 = e2 / (1.0 - e2);

    let k0: f64 = 0.9996;
    let lambda0 = 24.0_f64.to_radians();
    let fe = 500_000.0;
    let fn_ = 0.0;

    // Meridian arc constants
    let e2_2 = e2;
    let e4 = e2_2 * e2_2;
    let e6 = e4 * e2_2;

    let m0 = 0.0; // φ₀ = 0 for fn=0

    // Footpoint latitude from meridian distance
    let big_m = (y - fn_) / k0 + m0;

    let mu = big_m
        / (a * (1.0 - e2_2 / 4.0 - 3.0 * e4 / 64.0 - 5.0 * e6 / 256.0));

    let e1 = (1.0 - (1.0 - e2_2).sqrt()) / (1.0 + (1.0 - e2_2).sqrt());
    let e1_2 = e1 * e1;
    let e1_3 = e1_2 * e1;
    let e1_4 = e1_3 * e1;

    let phi1 = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1_3 / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1_2 / 16.0 - 55.0 * e1_4 / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1_3 / 96.0) * (6.0 * mu).sin()
        + (1097.0 * e1_4 / 512.0) * (8.0 * mu).sin();

    let sin1 = phi1.sin();
    let cos1 = phi1.cos();
    let tan1 = phi1.tan();
    let t1 = tan1 * tan1;
    let c1 = e_prime2 * cos1 * cos1;
    let n1 = a / (1.0 - e2_2 * sin1 * sin1).sqrt();
    let r1 = a * (1.0 - e2_2) / (1.0 - e2_2 * sin1 * sin1).powf(1.5);
    let d = (x - fe) / (n1 * k0);

    let d2 = d * d;
    let d3 = d2 * d;
    let d4 = d3 * d;
    let d5 = d4 * d;
    let d6 = d5 * d;

    let lat = phi1
        - (n1 * tan1 / r1)
            * (d2 / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * e_prime2) * d4 / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1
                    - 252.0 * e_prime2
                    - 3.0 * c1 * c1)
                    * d6
                    / 720.0);

    let lon = lambda0
        + (d - (1.0 + 2.0 * t1 + c1) * d3 / 6.0
            + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * e_prime2 + 24.0 * t1 * t1)
                * d5
                / 120.0)
            / cos1;

    let lat_deg = lat.to_degrees();
    let lon_deg = lon.to_degrees();

    // Sanity check
    if lat_deg < 50.0 || lat_deg > 65.0 || lon_deg < 15.0 || lon_deg > 35.0 {
        return None;
    }

    Some((lat_deg, lon_deg))
}

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────

fn col_index(header: &[&str], name: &str) -> Option<usize> {
    header
        .iter()
        .position(|c| c.trim().trim_matches('"').eq_ignore_ascii_case(name))
}

fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').trim().to_owned()
}
