/// ads.rs — Parse Estonian ADS (Address Data System) CSV export
///
/// The ADS CSV is published by the Estonian Land Board (Maa-amet).
/// Semicolon-delimited, may contain WGS84 lat/lon or L-EST 97 (EPSG:3301) coordinates.
///
/// Strategy: prefer WGS84 columns if present, otherwise convert from L-EST 97.
///
/// L-EST 97 is a Lambert Conformal Conic 2SP projection on GRS80/ETRS89.

use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use crate::extract::RawAddress;

/// Estonia bounding box
const EE_LAT_MIN: f64 = 57.51;
const EE_LAT_MAX: f64 = 59.68;
const EE_LON_MIN: f64 = 21.76;
const EE_LON_MAX: f64 = 28.21;

/// Import Estonian ADS addresses from a semicolon-delimited CSV file.
pub fn read_ads_addresses(csv_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading Estonian ADS addresses from {}", csv_path.display());

    let reader = BufReader::new(
        std::fs::File::open(csv_path)
            .with_context(|| format!("failed to open {}", csv_path.display()))?,
    );
    let mut lines = reader.lines();

    // Parse header
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty ADS CSV file"))??;
    let header = header.trim_start_matches('\u{feff}');
    let cols: Vec<&str> = header.split(';').collect();

    // Find coordinate columns — prefer WGS84
    let coord_mode = detect_coord_columns(&cols);
    info!("  Coordinate mode: {:?}", coord_mode);

    // Find address columns — try multiple naming conventions
    let i_street = col_index(&cols, "liiklus_aadress")
        .or_else(|| col_index(&cols, "taanav"))
        .or_else(|| col_index(&cols, "tanav"))
        .or_else(|| col_index(&cols, "street"))
        .or_else(|| col_index(&cols, "aadress"));

    let i_number = col_index(&cols, "adr_id")
        .and(None) // adr_id is an ID, not house number
        .or_else(|| col_index(&cols, "maja_nr"))
        .or_else(|| col_index(&cols, "nr"))
        .or_else(|| col_index(&cols, "house_number"))
        .or_else(|| col_index(&cols, "number"));

    let i_city = col_index(&cols, "asustusyksus")
        .or_else(|| col_index(&cols, "linn"))
        .or_else(|| col_index(&cols, "city"))
        .or_else(|| col_index(&cols, "koht"));

    let i_postcode = col_index(&cols, "sihtnumber")
        .or_else(|| col_index(&cols, "postiindeks"))
        .or_else(|| col_index(&cols, "postcode"))
        .or_else(|| col_index(&cols, "postal_code"));

    if i_street.is_none() {
        info!("  WARNING: no street column found. Available columns: {}", cols.join(", "));
    }
    if i_number.is_none() {
        info!("  WARNING: no house number column found. Available columns: {}", cols.join(", "));
    }

    let mut addresses = Vec::with_capacity(500_000);
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
        if lat < EE_LAT_MIN || lat > EE_LAT_MAX || lon < EE_LON_MIN || lon > EE_LON_MAX {
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
            lat,
            lon,
        });
    }

    info!(
        "Parsed {} Estonian ADS addresses ({} skipped, {} coordinate errors)",
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
    /// L-EST 97 (EPSG:3301) easting/northing columns
    LEst97 { x_col: usize, y_col: usize },
    /// No coordinate columns found
    None,
}

fn detect_coord_columns(cols: &[&str]) -> CoordMode {
    // Try WGS84 first
    let lat = col_index(cols, "latitude")
        .or_else(|| col_index(cols, "lat"))
        .or_else(|| col_index(cols, "vilestikoordinaat_wgs"));
    let lon = col_index(cols, "longitude")
        .or_else(|| col_index(cols, "lon"))
        .or_else(|| col_index(cols, "idakoordinaat_wgs"));

    if let (Some(lat_col), Some(lon_col)) = (lat, lon) {
        return CoordMode::Wgs84 { lat_col, lon_col };
    }

    // Try L-EST 97
    let x = col_index(cols, "vilestikoordinaat")
        .or_else(|| col_index(cols, "y"))   // note: geographic y = northing
        .or_else(|| col_index(cols, "n"));
    let y = col_index(cols, "idakoordinaat")
        .or_else(|| col_index(cols, "x"))   // note: geographic x = easting
        .or_else(|| col_index(cols, "e"));

    if let (Some(x_col), Some(y_col)) = (x, y) {
        return CoordMode::LEst97 { x_col, y_col };
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
        CoordMode::LEst97 { x_col, y_col } => {
            let northing: f64 = fields.get(*x_col)?.trim().trim_matches('"').parse().ok()?;
            let easting: f64 = fields.get(*y_col)?.trim().trim_matches('"').parse().ok()?;
            lest97_to_wgs84(northing, easting)
        }
        CoordMode::None => None,
    }
}

// ───────────────────────────────────────────────────────────────────────────
// L-EST 97 (EPSG:3301) → WGS84 inverse Lambert Conformal Conic 2SP
// ───────────────────────────────────────────────────────────────────────────
//
// Parameters:
//   Ellipsoid: GRS80 (a=6378137.0, f=1/298.257222101)
//   Standard parallels: φ₁=58°0', φ₂=59°20'
//   Origin: λ₀=24°0', φ₀=57°31'4.0608"
//   False easting: 500000, False northing: 6375000

fn lest97_to_wgs84(northing: f64, easting: f64) -> Option<(f64, f64)> {
    use std::f64::consts::PI;

    let a: f64 = 6_378_137.0;
    let f: f64 = 1.0 / 298.257_222_101;
    let e2 = 2.0 * f - f * f;
    let e = e2.sqrt();

    let phi1 = 58.0_f64.to_radians();
    let phi2 = (59.0_f64 + 20.0 / 60.0).to_radians();
    let phi0 = (57.0_f64 + 31.0 / 60.0 + 4.0608 / 3600.0).to_radians();
    let lambda0 = 24.0_f64.to_radians();
    let fe = 500_000.0;
    let fn_ = 6_375_000.0;

    let m = |phi: f64| -> f64 {
        phi.cos() / (1.0 - e2 * phi.sin().powi(2)).sqrt()
    };

    let t = |phi: f64| -> f64 {
        let es = e * phi.sin();
        ((PI / 4.0 - phi / 2.0).tan()) / ((1.0 - es) / (1.0 + es)).powf(e / 2.0)
    };

    let m1 = m(phi1);
    let m2 = m(phi2);
    let t0 = t(phi0);
    let t1 = t(phi1);
    let t2 = t(phi2);

    let n = (m1.ln() - m2.ln()) / (t1.ln() - t2.ln());
    let ff = m1 / (n * t1.powf(n));
    let rho0 = a * ff * t0.powf(n);

    // Inverse: from E, N to lat, lon
    let x = easting - fe;
    let y = rho0 - (northing - fn_);

    let rho = (x * x + y * y).sqrt() * n.signum();
    let t_inv = (rho / (a * ff)).powf(1.0 / n);
    let theta = x.atan2(y);

    // Iterate for latitude
    let mut phi = PI / 2.0 - 2.0 * t_inv.atan();
    for _ in 0..15 {
        let es = e * phi.sin();
        let phi_new = PI / 2.0 - 2.0 * (t_inv * ((1.0 - es) / (1.0 + es)).powf(e / 2.0)).atan();
        if (phi_new - phi).abs() < 1e-12 {
            break;
        }
        phi = phi_new;
    }

    let lambda = theta / n + lambda0;

    let lat = phi.to_degrees();
    let lon = lambda.to_degrees();

    // Sanity check
    if lat < 50.0 || lat > 65.0 || lon < 15.0 || lon > 35.0 {
        return None;
    }

    Some((lat, lon))
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
