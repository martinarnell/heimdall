/// swisstopo.rs — Parse swisstopo Gebaeudeadressen for Switzerland
///
/// Switzerland's official address register (~2.7M addresses), published by
/// the Federal Office of Topography (swisstopo) as semicolon-delimited CSVs.
///
/// Three-table join:
///   STRASSE.csv  — street ID → street name
///   ORTSCHAFT.csv — locality lookup (municipality + postcode → locality name)
///   ADRESSE.csv  — addresses with LV95 coordinates, joined via ESID
///
/// Coordinates are in Swiss LV95 (EPSG:2056), converted to WGS84 using
/// the official swisstopo approximate formulas.
///
/// Download: https://data.geo.admin.ch/ch.swisstopo.amtliches-gebaeudeadressverzeichnis/

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

/// Import swisstopo addresses from three CSV files.
///
/// - `addr_csv`: path to ADRESSE.csv
/// - `street_csv`: path to STRASSE.csv
/// - `locality_csv`: path to ORTSCHAFT.csv
pub fn read_swisstopo_addresses(
    addr_csv: &Path,
    street_csv: &Path,
    locality_csv: &Path,
) -> Result<Vec<RawAddress>> {
    info!(
        "Reading swisstopo addresses from {}",
        addr_csv.display()
    );

    // Step 1: Load STRASSE.csv → HashMap<ESID, street_name>
    let streets = load_streets(street_csv)?;
    info!("  Loaded {} streets", streets.len());

    // Step 2: Load ORTSCHAFT.csv → HashMap for locality lookup
    let localities = load_localities(locality_csv)?;
    info!("  Loaded {} localities", localities.len());

    // Step 3: Read ADRESSE.csv, join with streets and localities
    let addresses = join_addresses(addr_csv, &streets, &localities)?;
    info!("Parsed {} swisstopo addresses", addresses.len());

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

/// Title-case a string: "BERN" -> "Bern", "ZUG" -> "Zug"
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
// LV95 (EPSG:2056) → WGS84 conversion
// ───────────────────────────────────────────────────────────────────────────

/// Convert Swiss LV95 coordinates (EPSG:2056) to WGS84 (lat, lon).
///
/// Uses the official swisstopo approximate formulas.
/// Accuracy: ~1m, sufficient for geocoding.
fn lv95_to_wgs84(e: f64, n: f64) -> (f64, f64) {
    // Auxiliary values (shift to Bern origin, scale to 1000km units)
    let y_aux = (e - 2_600_000.0) / 1_000_000.0;
    let x_aux = (n - 1_200_000.0) / 1_000_000.0;

    // Latitude in 10000" of arc
    let lat_aux = 16.9023892
        + 3.238272 * x_aux
        - 0.270978 * y_aux * y_aux
        - 0.002528 * x_aux * x_aux
        - 0.0447 * y_aux * y_aux * x_aux
        - 0.0140 * x_aux * x_aux * x_aux;

    // Longitude in 10000" of arc
    let lon_aux = 2.6779094
        + 4.728982 * y_aux
        + 0.791484 * y_aux * x_aux
        + 0.1306 * y_aux * x_aux * x_aux
        - 0.0436 * y_aux * y_aux * y_aux;

    // Convert from 10000" of arc to degrees
    let lat = lat_aux * 100.0 / 36.0;
    let lon = lon_aux * 100.0 / 36.0;
    (lat, lon)
}

// ───────────────────────────────────────────────────────────────────────────
// Step 1: STRASSE.csv → ESID → street name
// ───────────────────────────────────────────────────────────────────────────

fn load_streets(path: &Path) -> Result<HashMap<String, String>> {
    let reader = BufReader::new(std::fs::File::open(path)?);
    let mut lines = reader.lines();

    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty STRASSE.csv"))??;
    let header = strip_bom(&header);
    let cols: Vec<&str> = header.split(';').collect();

    let i_esid = col_index(&cols, "ESID")
        .ok_or_else(|| anyhow::anyhow!("missing ESID column in STRASSE.csv"))?;
    let i_strname = col_index(&cols, "STRNAME")
        .or_else(|| col_index(&cols, "STRNAMK"))
        .ok_or_else(|| anyhow::anyhow!("missing STRNAME/STRNAMK column in STRASSE.csv"))?;

    let mut map = HashMap::new();

    for line in lines {
        let line = line?;
        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= i_esid.max(i_strname) {
            continue;
        }

        let esid = fields[i_esid].trim().trim_matches('"').to_owned();
        let name = fields[i_strname].trim().trim_matches('"');
        if esid.is_empty() || name.is_empty() {
            continue;
        }

        map.insert(esid, name.to_owned());
    }

    Ok(map)
}

// ───────────────────────────────────────────────────────────────────────────
// Step 2: ORTSCHAFT.csv → locality lookup
// ───────────────────────────────────────────────────────────────────────────

struct LocalityInfo {
    name: String,
}

fn load_localities(path: &Path) -> Result<HashMap<(String, String), LocalityInfo>> {
    let reader = BufReader::new(std::fs::File::open(path)?);
    let mut lines = reader.lines();

    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty ORTSCHAFT.csv"))??;
    let header = strip_bom(&header);
    let cols: Vec<&str> = header.split(';').collect();

    let i_gdename = col_index(&cols, "GDENAME")
        .ok_or_else(|| anyhow::anyhow!("missing GDENAME column in ORTSCHAFT.csv"))?;
    let i_dplz4 = col_index(&cols, "DPLZ4")
        .ok_or_else(|| anyhow::anyhow!("missing DPLZ4 column in ORTSCHAFT.csv"))?;
    let i_dplzname = col_index(&cols, "DPLZNAME")
        .ok_or_else(|| anyhow::anyhow!("missing DPLZNAME column in ORTSCHAFT.csv"))?;

    let mut map = HashMap::new();

    for line in lines {
        let line = line?;
        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= i_gdename.max(i_dplz4).max(i_dplzname) {
            continue;
        }

        let gdename = fields[i_gdename].trim().trim_matches('"').to_owned();
        let dplz4 = fields[i_dplz4].trim().trim_matches('"').to_owned();
        let dplzname = fields[i_dplzname].trim().trim_matches('"');

        if gdename.is_empty() || dplz4.is_empty() {
            continue;
        }

        map.insert(
            (gdename, dplz4),
            LocalityInfo {
                name: dplzname.to_owned(),
            },
        );
    }

    Ok(map)
}

// ───────────────────────────────────────────────────────────────────────────
// Step 3: ADRESSE.csv — join with streets and localities → RawAddress
// ───────────────────────────────────────────────────────────────────────────

fn join_addresses(
    path: &Path,
    streets: &HashMap<String, String>,
    _localities: &HashMap<(String, String), LocalityInfo>,
) -> Result<Vec<RawAddress>> {
    let reader = BufReader::new(std::fs::File::open(path)?);
    let mut lines = reader.lines();

    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty ADRESSE.csv"))??;
    let header = strip_bom(&header);
    let cols: Vec<&str> = header.split(';').collect();

    let i_egaid = col_index(&cols, "EGAID")
        .ok_or_else(|| anyhow::anyhow!("missing EGAID column in ADRESSE.csv"))?;
    let i_esid = col_index(&cols, "ESID")
        .ok_or_else(|| anyhow::anyhow!("missing ESID column in ADRESSE.csv"))?;
    let i_dession = col_index(&cols, "DESSION")
        .ok_or_else(|| anyhow::anyhow!("missing DESSION column in ADRESSE.csv"))?;
    let i_suffix = col_index(&cols, "DESSION_SUFFIX");
    let i_dplz4 = col_index(&cols, "DPLZ4")
        .ok_or_else(|| anyhow::anyhow!("missing DPLZ4 column in ADRESSE.csv"))?;
    let i_gdename = col_index(&cols, "GDENAME")
        .ok_or_else(|| anyhow::anyhow!("missing GDENAME column in ADRESSE.csv"))?;
    let i_e = col_index(&cols, "E")
        .ok_or_else(|| anyhow::anyhow!("missing E column in ADRESSE.csv"))?;
    let i_n = col_index(&cols, "N")
        .ok_or_else(|| anyhow::anyhow!("missing N column in ADRESSE.csv"))?;

    let max_col = *[i_egaid, i_esid, i_dession, i_dplz4, i_gdename, i_e, i_n]
        .iter()
        .max()
        .unwrap();

    let mut addresses = Vec::with_capacity(3_000_000);
    let mut skipped = 0usize;
    let mut no_street = 0usize;

    for line in lines {
        let line = line?;
        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= max_col {
            skipped += 1;
            continue;
        }

        // Parse LV95 coordinates
        let e_str = fields[i_e].trim().trim_matches('"');
        let n_str = fields[i_n].trim().trim_matches('"');
        if e_str.is_empty() || n_str.is_empty() || e_str == "NULL" || n_str == "NULL" {
            skipped += 1;
            continue;
        }
        let e: f64 = match e_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let n: f64 = match n_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        let (lat, lon) = lv95_to_wgs84(e, n);

        // Switzerland bbox: 45.82-47.81N, 5.96-10.49E
        if lat < 45.82 || lat > 47.81 || lon < 5.96 || lon > 10.49 {
            skipped += 1;
            continue;
        }

        // Join street name via ESID
        let esid = fields[i_esid].trim().trim_matches('"');
        let street = match streets.get(esid) {
            Some(name) => name.clone(),
            None => {
                no_street += 1;
                continue;
            }
        };

        // House number: DESSION + optional DESSION_SUFFIX
        let dession = fields[i_dession].trim().trim_matches('"');
        if dession.is_empty() || dession == "0" {
            skipped += 1;
            continue;
        }
        let suffix = i_suffix
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim().trim_matches('"'))
            .unwrap_or("");
        let housenumber = if suffix.is_empty() {
            dession.to_owned()
        } else {
            format!("{}{}", dession, suffix)
        };

        // Municipality (city)
        let gdename = fields[i_gdename].trim().trim_matches('"');
        let city = if gdename.is_empty() {
            None
        } else {
            Some(title_case(gdename))
        };

        // Postcode
        let dplz4 = fields[i_dplz4].trim().trim_matches('"');
        let postcode = if dplz4.is_empty() {
            None
        } else {
            Some(dplz4.to_owned())
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
        "  Join complete: {} addresses ({} skipped, {} no street match)",
        addresses.len(),
        skipped,
        no_street,
    );

    Ok(addresses)
}
