/// ban.rs — Parse France BAN (Base Adresse Nationale) per-department CSVs
///
/// BAN contains ~26M addresses for metropolitan France and overseas territories.
/// Published under Licence Ouverte 2.0 (open licence, attribution required).
///
/// Data format: per-department gzipped CSV files (semicolon-delimited).
/// Each file contains addresses with coordinates already in WGS84.
///
/// Download: https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/

use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use tracing::info;

use crate::extract::RawAddress;

/// All French department codes (metropolitan + overseas).
const BAN_DEPARTMENTS: &[&str] = &[
    "01", "02", "03", "04", "05", "06", "07", "08", "09",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "2A", "2B",
    "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95",
    // Overseas departments
    "971", // Guadeloupe
    "972", // Martinique
    "973", // Guyane
    "974", // Réunion
    "976", // Mayotte
];

/// Base URL for BAN per-department CSV downloads.
const BAN_BASE_URL: &str =
    "https://adresse.data.gouv.fr/data/ban/adresses/latest/csv";

// France metropolitan bounding box
const METRO_LAT_MIN: f64 = 41.33;
const METRO_LAT_MAX: f64 = 51.12;
const METRO_LON_MIN: f64 = -5.14;
const METRO_LON_MAX: f64 = 9.56;

// ───────────────────────────────────────────────────────────────────────────
// Public API
// ───────────────────────────────────────────────────────────────────────────

/// Read BAN addresses from pre-downloaded .csv.gz files in a directory.
///
/// Expects files named `adresses-{dept}.csv.gz` in `csv_dir`.
pub fn read_ban_addresses(csv_dir: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading BAN addresses from {}", csv_dir.display());

    let mut addresses = Vec::with_capacity(26_000_000);
    let mut total_skipped = 0usize;

    for dept in BAN_DEPARTMENTS {
        let filename = format!("adresses-{}.csv.gz", dept);
        let path = csv_dir.join(&filename);

        if !path.exists() {
            info!("  Skipping {} (file not found)", filename);
            continue;
        }

        let (mut dept_addrs, skipped) = read_ban_csv_gz(&path, dept)
            .with_context(|| format!("reading {}", filename))?;
        info!("  {} — {} addresses ({} skipped)", dept, dept_addrs.len(), skipped);
        total_skipped += skipped;
        addresses.append(&mut dept_addrs);
    }

    info!(
        "Parsed {} BAN addresses total ({} skipped)",
        addresses.len(),
        total_skipped,
    );
    Ok(addresses)
}

/// Download all BAN department files, then read them.
///
/// Downloads each `adresses-{dept}.csv.gz` to `output_dir`, then parses.
pub fn download_ban_addresses(output_dir: &Path) -> Result<Vec<RawAddress>> {
    info!("Downloading BAN addresses to {}", output_dir.display());
    std::fs::create_dir_all(output_dir)?;

    for (i, dept) in BAN_DEPARTMENTS.iter().enumerate() {
        let filename = format!("adresses-{}.csv.gz", dept);
        let url = format!("{}/{}", BAN_BASE_URL, filename);
        let out_path = output_dir.join(&filename);

        if out_path.exists() {
            info!(
                "  [{}/{}] {} — already exists, skipping",
                i + 1,
                BAN_DEPARTMENTS.len(),
                dept,
            );
            continue;
        }

        info!(
            "  [{}/{}] Downloading {}...",
            i + 1,
            BAN_DEPARTMENTS.len(),
            dept,
        );

        let resp = match ureq::get(&url).call() {
            Ok(r) => r,
            Err(e) => {
                info!("    Failed to download {} — {} — skipping", dept, e);
                continue;
            }
        };

        let mut bytes = Vec::new();
        resp.into_reader()
            .read_to_end(&mut bytes)
            .with_context(|| format!("reading response for {}", dept))?;
        std::fs::write(&out_path, &bytes)?;
        info!("    {} — {} bytes", dept, bytes.len());
    }

    read_ban_addresses(output_dir)
}

// ───────────────────────────────────────────────────────────────────────────
// CSV parsing
// ───────────────────────────────────────────────────────────────────────────

/// Column indices for the BAN CSV schema.
struct BanColumns {
    i_numero: usize,
    i_rep: usize,
    i_nom_voie: usize,
    i_code_postal: usize,
    i_nom_commune: usize,
    i_lat: usize,
    i_lon: usize,
    i_nom_ld: usize,
}

/// Find a column index by name in a semicolon-separated header.
fn col_index(cols: &[&str], name: &str) -> Option<usize> {
    cols.iter().position(|c| c.trim() == name)
}

/// Parse a single .csv.gz file for one department.
fn read_ban_csv_gz(path: &Path, dept: &str) -> Result<(Vec<RawAddress>, usize)> {
    let file = std::fs::File::open(path)?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);
    let mut lines = reader.lines();

    // Parse header
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty BAN file for dept {}", dept))??;
    let header = header.trim_start_matches('\u{feff}');
    let cols: Vec<&str> = header.split(';').collect();

    let schema = BanColumns {
        i_numero: col_index(&cols, "numero")
            .ok_or_else(|| anyhow::anyhow!("missing 'numero' column in dept {}", dept))?,
        i_rep: col_index(&cols, "rep")
            .ok_or_else(|| anyhow::anyhow!("missing 'rep' column in dept {}", dept))?,
        i_nom_voie: col_index(&cols, "nom_voie")
            .ok_or_else(|| anyhow::anyhow!("missing 'nom_voie' column in dept {}", dept))?,
        i_code_postal: col_index(&cols, "code_postal")
            .ok_or_else(|| anyhow::anyhow!("missing 'code_postal' column in dept {}", dept))?,
        i_nom_commune: col_index(&cols, "nom_commune")
            .ok_or_else(|| anyhow::anyhow!("missing 'nom_commune' column in dept {}", dept))?,
        i_lat: col_index(&cols, "lat")
            .ok_or_else(|| anyhow::anyhow!("missing 'lat' column in dept {}", dept))?,
        i_lon: col_index(&cols, "lon")
            .ok_or_else(|| anyhow::anyhow!("missing 'lon' column in dept {}", dept))?,
        i_nom_ld: col_index(&cols, "nom_ld")
            .ok_or_else(|| anyhow::anyhow!("missing 'nom_ld' column in dept {}", dept))?,
    };

    let max_col = [
        schema.i_numero,
        schema.i_rep,
        schema.i_nom_voie,
        schema.i_code_postal,
        schema.i_nom_commune,
        schema.i_lat,
        schema.i_lon,
        schema.i_nom_ld,
    ]
    .into_iter()
    .max()
    .unwrap();

    let is_overseas = matches!(dept, "971" | "972" | "973" | "974" | "976");

    let mut addresses = Vec::new();
    let mut skipped = 0usize;

    for line in lines {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= max_col {
            skipped += 1;
            continue;
        }

        // House number is required
        let numero = fields[schema.i_numero].trim();
        if numero.is_empty() {
            skipped += 1;
            continue;
        }

        // Street name: prefer nom_voie, fall back to nom_ld (lieu-dit)
        let nom_voie = fields[schema.i_nom_voie].trim();
        let nom_ld = fields[schema.i_nom_ld].trim();
        let street = if !nom_voie.is_empty() {
            nom_voie
        } else if !nom_ld.is_empty() {
            nom_ld
        } else {
            skipped += 1;
            continue;
        };

        // Parse coordinates
        let lat: f64 = match fields[schema.i_lat].trim().parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let lon: f64 = match fields[schema.i_lon].trim().parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Bbox check: skip for overseas departments, validate for metro
        if !is_overseas
            && (lat < METRO_LAT_MIN
                || lat > METRO_LAT_MAX
                || lon < METRO_LON_MIN
                || lon > METRO_LON_MAX)
        {
            skipped += 1;
            continue;
        }

        // Build house number: numero + rep suffix ("12" + "bis" → "12bis")
        let rep = fields[schema.i_rep].trim();
        let housenumber = if rep.is_empty() {
            numero.to_string()
        } else {
            format!("{}{}", numero, rep)
        };

        // City
        let commune = fields[schema.i_nom_commune].trim();
        let city = if commune.is_empty() {
            None
        } else {
            Some(commune.to_string())
        };

        // Postcode
        let code_postal = fields[schema.i_code_postal].trim();
        let postcode = if code_postal.is_empty() {
            None
        } else {
            Some(code_postal.to_string())
        };

        addresses.push(RawAddress {
            osm_id: 0,
            street: street.to_string(),
            housenumber,
            postcode,
            city,
            state: None,
            lat,
            lon,
        });
    }

    Ok((addresses, skipped))
}
