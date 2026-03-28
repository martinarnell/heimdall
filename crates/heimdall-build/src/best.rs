/// best.rs — Parse BeST (BeST Address) data for Belgium
///
/// BeST aggregates address data from Belgium's three regional registers:
///   - CRAB (Flanders)
///   - URBIS (Brussels)
///   - ICAR (Wallonia)
///
/// CSV schema (comma-separated):
///   EPSG:31370_x, EPSG:31370_y, EPSG:4326_lat, EPSG:4326_lon,
///   address_id, box_number, house_number, municipality_id,
///   municipality_name_de, municipality_name_fr, municipality_name_nl,
///   postcode, postname_fr, postname_nl, street_id,
///   streetname_de, streetname_fr, streetname_nl, region_code, status
///
/// Coordinates in EPSG:4326 columns are WGS84.

use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

/// Import BeST addresses from one or more ZIP files.
///
/// Each ZIP contains CSV files with OpenAddresses-format address data.
/// Returns a combined Vec of all addresses across all input ZIPs.
pub fn read_best_addresses(zip_paths: &[&Path]) -> Result<Vec<RawAddress>> {
    info!("Reading BeST addresses from {} ZIP file(s)", zip_paths.len());

    let mut all_addresses = Vec::with_capacity(5_000_000);

    for zip_path in zip_paths {
        let count = read_single_zip(zip_path, &mut all_addresses)?;
        info!(
            "  {} — {} addresses",
            zip_path.file_name().unwrap_or_default().to_string_lossy(),
            count,
        );
    }

    info!("Parsed {} BeST addresses total", all_addresses.len());
    Ok(all_addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// Single ZIP processing
// ───────────────────────────────────────────────────────────────────────────

fn read_single_zip(zip_path: &Path, out: &mut Vec<RawAddress>) -> Result<usize> {
    let file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut total = 0usize;

    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }

        let full_name = entry.name().to_string();
        if !full_name.to_lowercase().ends_with(".csv") {
            continue;
        }

        let fname = Path::new(&full_name)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let count = parse_csv_entry(entry, &fname, out)?;
        total += count;
    }

    Ok(total)
}

fn parse_csv_entry<R: std::io::Read>(
    reader: R,
    filename: &str,
    out: &mut Vec<RawAddress>,
) -> Result<usize> {
    let buf = BufReader::new(reader);
    let mut lines = buf.lines();

    // Parse header
    let header = match lines.next() {
        Some(Ok(h)) => h,
        Some(Err(e)) => return Err(e.into()),
        None => return Ok(0),
    };
    let header = header.trim_start_matches('\u{feff}');
    let cols: Vec<&str> = header.split(',').collect();

    // Detect format: new BeST native vs old OpenAddresses
    let new_format = col_index(&cols, "EPSG:4326_lon").is_some();

    let i_lon = col_index(&cols, "EPSG:4326_lon")
        .or_else(|| col_index(&cols, "LON"))
        .ok_or_else(|| anyhow::anyhow!("missing lon column in {}", filename))?;
    let i_lat = col_index(&cols, "EPSG:4326_lat")
        .or_else(|| col_index(&cols, "LAT"))
        .ok_or_else(|| anyhow::anyhow!("missing lat column in {}", filename))?;
    let i_number = col_index(&cols, "house_number")
        .or_else(|| col_index(&cols, "NUMBER"))
        .ok_or_else(|| anyhow::anyhow!("missing house_number column in {}", filename))?;
    let i_unit = col_index(&cols, "box_number")
        .or_else(|| col_index(&cols, "UNIT"));
    let i_postcode = col_index(&cols, "postcode")
        .or_else(|| col_index(&cols, "POSTCODE"));
    let i_id = col_index(&cols, "address_id")
        .or_else(|| col_index(&cols, "ID"));

    // Street: new format has per-language columns, old has single STREET
    let i_street = col_index(&cols, "STREET");
    let i_street_nl = col_index(&cols, "streetname_nl");
    let i_street_fr = col_index(&cols, "streetname_fr");
    let i_street_de = col_index(&cols, "streetname_de");

    // City: new format has per-language columns, old has single CITY
    let i_city = col_index(&cols, "CITY");
    let i_city_nl = col_index(&cols, "municipality_name_nl");
    let i_city_fr = col_index(&cols, "municipality_name_fr");
    let i_city_de = col_index(&cols, "municipality_name_de");

    // Region code for language preference (new format)
    let i_region = col_index(&cols, "region_code");

    if !new_format && i_street.is_none() {
        return Err(anyhow::anyhow!("missing street column in {}", filename));
    }
    if !new_format && i_city.is_none() {
        return Err(anyhow::anyhow!("missing city column in {}", filename));
    }

    let mut count = 0usize;
    let mut skipped = 0usize;

    for line in lines {
        let line = line?;
        let fields = parse_csv_line(&line);

        if fields.len() <= i_lon.max(i_lat).max(i_number) {
            skipped += 1;
            continue;
        }

        // Parse coordinates
        let lat: f64 = match unquote(&fields[i_lat]).parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let lon: f64 = match unquote(&fields[i_lon]).parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Belgium bbox: 49.50-51.50N, 2.55-6.41E
        if lat < 49.50 || lat > 51.50 || lon < 2.55 || lon > 6.41 {
            skipped += 1;
            continue;
        }

        // Street name: pick best language based on region
        let street = if new_format {
            let region = i_region
                .filter(|&idx| idx < fields.len())
                .map(|idx| unquote(&fields[idx]))
                .unwrap_or_default();
            pick_lang_value(&fields, &region, i_street_nl, i_street_fr, i_street_de)
        } else {
            unquote(&fields[i_street.unwrap()])
        };
        if street.is_empty() {
            skipped += 1;
            continue;
        }

        // House number + unit
        let number = unquote(&fields[i_number]);
        if number.is_empty() {
            skipped += 1;
            continue;
        }

        let unit = i_unit
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(&fields[idx]))
            .unwrap_or_default();

        let housenumber = if unit.is_empty() {
            number
        } else {
            format!("{}/{}", number, unit)
        };

        // City
        let city = if new_format {
            let region = i_region
                .filter(|&idx| idx < fields.len())
                .map(|idx| unquote(&fields[idx]))
                .unwrap_or_default();
            let v = pick_lang_value(&fields, &region, i_city_nl, i_city_fr, i_city_de);
            if v.is_empty() { None } else { Some(v) }
        } else {
            let raw = unquote(&fields[i_city.unwrap()]);
            if raw.is_empty() { None } else { Some(raw) }
        };

        // Postcode
        let postcode = i_postcode
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(&fields[idx]))
            .filter(|s| !s.is_empty());

        // ID
        let osm_id = i_id
            .filter(|&idx| idx < fields.len())
            .and_then(|idx| unquote(&fields[idx]).parse::<i64>().ok())
            .unwrap_or(0);

        out.push(RawAddress {
            osm_id,
            street,
            housenumber,
            postcode,
            city,
            lat,
            lon,
        });
        count += 1;
    }

    if skipped > 0 {
        info!("    {} — {} addresses ({} skipped)", filename, count, skipped);
    }

    Ok(count)
}

/// Pick the best language value based on Belgian region code.
/// Flanders (VLG) → NL, Wallonia (WAL) → FR, Brussels (BXL) → FR, German-speaking → DE.
fn pick_lang_value(
    fields: &[String],
    region: &str,
    i_nl: Option<usize>,
    i_fr: Option<usize>,
    i_de: Option<usize>,
) -> String {
    let get = |idx: Option<usize>| -> String {
        idx.filter(|&i| i < fields.len())
            .map(|i| unquote(&fields[i]))
            .unwrap_or_default()
    };

    // Pick primary language by region, fall back to any non-empty
    match region.to_uppercase().as_str() {
        "VLG" => {
            let v = get(i_nl);
            if !v.is_empty() { return v; }
        }
        "WAL" => {
            let v = get(i_fr);
            if !v.is_empty() { return v; }
            let v = get(i_de);
            if !v.is_empty() { return v; }
        }
        _ => {} // BXL or unknown — fall through to priority order
    }

    // Default: FR > NL > DE (Brussels is officially bilingual FR/NL)
    let v = get(i_fr);
    if !v.is_empty() { return v; }
    let v = get(i_nl);
    if !v.is_empty() { return v; }
    get(i_de)
}

// ───────────────────────────────────────────────────────────────────────────
// CSV parsing helpers
// ───────────────────────────────────────────────────────────────────────────

fn col_index(header: &[&str], name: &str) -> Option<usize> {
    header
        .iter()
        .position(|c| c.trim().trim_matches('"') == name)
}

/// Parse a CSV line handling quoted fields that may contain commas.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in line.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current);
    fields
}

fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').trim().to_owned()
}
