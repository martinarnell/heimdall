/// bag.rs — Parse NLExtract BAG (Basisregistratie Adressen en Gebouwen) for the Netherlands
///
/// BAG is the Netherlands' authoritative address and building register.
/// NLExtract provides a pre-processed CSV export (semicolon-delimited, optionally gzipped).
///
/// CSV schema (semicolon-separated):
///   openbareruimte;huisnummer;huisletter;huisnummertoevoeging;postcode;woonplaats;
///   gemeente;provincie;object_id;object_type;nevenadres;x;y;lon;lat
///
/// Coordinates are WGS84 (lon/lat columns).

use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

/// Import BAG addresses from a CSV or CSV.GZ file.
///
/// Reads the NLExtract semicolon-delimited export, filters secondary addresses
/// (nevenadres), and returns flat RawAddress records.
pub fn read_bag_addresses(csv_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading BAG addresses from {}", csv_path.display());

    let file = std::fs::File::open(csv_path)?;

    let is_gz = csv_path
        .to_string_lossy()
        .to_lowercase()
        .ends_with(".gz");

    let reader: Box<dyn BufRead> = if is_gz {
        info!("  Decompressing gzip stream...");
        Box::new(BufReader::new(flate2::read::GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };

    let mut lines = reader.lines();

    // Parse header
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty BAG CSV file"))??;
    let header = header.trim_start_matches('\u{feff}');
    let cols: Vec<&str> = header.split(';').collect();

    let i_street = col_index(&cols, "openbareruimte")
        .ok_or_else(|| anyhow::anyhow!("missing openbareruimte column"))?;
    let i_number = col_index(&cols, "huisnummer")
        .ok_or_else(|| anyhow::anyhow!("missing huisnummer column"))?;
    let i_letter = col_index(&cols, "huisletter");
    let i_toevoeging = col_index(&cols, "huisnummertoevoeging");
    let i_postcode = col_index(&cols, "postcode")
        .ok_or_else(|| anyhow::anyhow!("missing postcode column"))?;
    let i_city = col_index(&cols, "woonplaats")
        .ok_or_else(|| anyhow::anyhow!("missing woonplaats column"))?;
    let i_nevenadres = col_index(&cols, "nevenadres");
    let i_lat = col_index(&cols, "lat")
        .ok_or_else(|| anyhow::anyhow!("missing lat column"))?;
    let i_lon = col_index(&cols, "lon")
        .ok_or_else(|| anyhow::anyhow!("missing lon column"))?;
    let i_object_id = col_index(&cols, "object_id");

    let max_col = [
        i_street, i_number, i_postcode, i_city, i_lat, i_lon,
    ]
    .into_iter()
    .max()
    .unwrap();

    let mut addresses = Vec::with_capacity(10_000_000);
    let mut skipped = 0usize;
    let mut line_count = 0usize;

    for line in lines {
        let line = line?;
        line_count += 1;

        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= max_col {
            skipped += 1;
            continue;
        }

        // Filter secondary addresses (nevenadres)
        if let Some(idx) = i_nevenadres {
            if idx < fields.len() {
                let val = fields[idx].trim().trim_matches('"').to_lowercase();
                if val == "true" || val == "t" {
                    skipped += 1;
                    continue;
                }
            }
        }

        // Parse coordinates
        let lat: f64 = match unquote(fields[i_lat]).parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let lon: f64 = match unquote(fields[i_lon]).parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // NL bbox: 50.75-53.47N, 3.36-7.21E
        if lat < 50.75 || lat > 53.47 || lon < 3.36 || lon > 7.21 {
            skipped += 1;
            continue;
        }

        // Street name
        let street = unquote(fields[i_street]);
        if street.is_empty() {
            skipped += 1;
            continue;
        }

        // House number: huisnummer + huisletter + huisnummertoevoeging
        let number = unquote(fields[i_number]);
        if number.is_empty() {
            skipped += 1;
            continue;
        }

        let letter = i_letter
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(fields[idx]))
            .unwrap_or_default();

        let toevoeging = i_toevoeging
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(fields[idx]))
            .unwrap_or_default();

        let mut housenumber = number;
        if !letter.is_empty() {
            housenumber.push_str(&letter);
        }
        if !toevoeging.is_empty() {
            housenumber.push('-');
            housenumber.push_str(&toevoeging);
        }

        // Postcode (strip spaces: "1234 AB" → "1234AB")
        let postcode = {
            let raw = unquote(fields[i_postcode]);
            let stripped = raw.replace(' ', "");
            if stripped.is_empty() {
                None
            } else {
                Some(stripped)
            }
        };

        // City
        let city = {
            let raw = unquote(fields[i_city]);
            if raw.is_empty() { None } else { Some(raw) }
        };

        // osm_id: use object_id if available, otherwise 0
        let osm_id = i_object_id
            .filter(|&idx| idx < fields.len())
            .and_then(|idx| unquote(fields[idx]).parse::<i64>().ok())
            .unwrap_or(0);

        addresses.push(RawAddress {
            osm_id,
            street,
            housenumber,
            postcode,
            city,
            state: None,
            lat,
            lon,
        });

        if line_count % 2_000_000 == 0 {
            info!("  {} lines processed, {} addresses so far", line_count, addresses.len());
        }
    }

    info!(
        "Parsed {} BAG addresses ({} skipped, {} lines total)",
        addresses.len(),
        skipped,
        line_count,
    );

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────

fn col_index(header: &[&str], name: &str) -> Option<usize> {
    header
        .iter()
        .position(|c| c.trim().trim_matches('"') == name)
}

fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').trim().to_owned()
}
