/// dawa.rs — Parse DAWA (Danish Address Web API) address CSV data
///
/// The Danish national address dataset from dataforsyningen.dk is a
/// comma-separated CSV in WGS84 (srid=4326). No coordinate transform needed.
///
/// Bulk download:
///   curl "https://api.dataforsyningen.dk/adgangsadresser?format=csv&srid=4326" \
///     -H "Accept-Encoding: gzip" -o denmark_addresses.csv.gz
///
/// Key fields: vejnavn (street), husnr (house number), postnr (postcode),
/// postnrnavn (city), kommunekode (municipality), wgs84koordinat_bredde (lat),
/// wgs84koordinat_længde (lon)

use std::path::Path;
use std::io::{BufRead, BufReader};
use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

/// Parse the DAWA address CSV into RawAddress records.
pub fn read_dawa_addresses(csv_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading DAWA addresses from {}", csv_path.display());

    let file = std::fs::File::open(csv_path)?;

    // Support gzip-compressed files
    let is_gz = csv_path.extension().map_or(false, |ext| ext == "gz");

    let addresses = if is_gz {
        let decoder = flate2::read::GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        parse_dawa_csv(reader)?
    } else {
        let reader = BufReader::new(file);
        parse_dawa_csv(reader)?
    };

    Ok(addresses)
}

fn parse_dawa_csv<R: BufRead>(reader: R) -> Result<Vec<RawAddress>> {
    let mut lines = reader.lines();

    // Parse header to find column indices
    let header = lines.next()
        .ok_or_else(|| anyhow::anyhow!("empty CSV"))??;
    // Strip BOM if present
    let header = header.trim_start_matches('\u{feff}');

    // DAWA uses comma-separated CSV with quoted fields
    let cols: Vec<&str> = parse_csv_header(header);

    let idx = |name: &str| -> Option<usize> {
        cols.iter().position(|c| c.trim().trim_matches('"') == name)
    };

    let i_street = idx("vejnavn").ok_or_else(|| anyhow::anyhow!("missing vejnavn column"))?;
    let i_husnr = idx("husnr").ok_or_else(|| anyhow::anyhow!("missing husnr column"))?;
    let i_postcode = idx("postnr");
    let i_city = idx("postnrnavn");
    let i_lat = idx("wgs84koordinat_bredde")
        .ok_or_else(|| anyhow::anyhow!("missing wgs84koordinat_bredde column"))?;
    let i_lon = idx("wgs84koordinat_længde")
        .or_else(|| idx("wgs84koordinat_laengde"))
        .ok_or_else(|| anyhow::anyhow!("missing wgs84koordinat_længde column"))?;

    let max_col = *[i_street, i_husnr, i_lat, i_lon]
        .iter()
        .chain(i_postcode.iter())
        .chain(i_city.iter())
        .max()
        .unwrap();

    let mut addresses = Vec::new();
    let mut skipped = 0usize;

    for line_result in lines {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => { skipped += 1; continue; }
        };

        let fields = parse_csv_line(&line);
        if fields.len() <= max_col {
            skipped += 1;
            continue;
        }

        let street = unquote(&fields[i_street]);
        let husnr = unquote(&fields[i_husnr]);
        if street.is_empty() || husnr.is_empty() {
            skipped += 1;
            continue;
        }

        let lat_str = unquote(&fields[i_lat]);
        let lon_str = unquote(&fields[i_lon]);

        let lat: f64 = match lat_str.parse() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };
        let lon: f64 = match lon_str.parse() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };

        // Sanity check coordinates (Denmark: 54-58N, 8-16E; generous for Bornholm)
        if lat < 54.0 || lat > 58.0 || lon < 7.5 || lon > 16.0 {
            skipped += 1;
            continue;
        }

        let postcode = i_postcode
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(&fields[idx]))
            .filter(|s| !s.is_empty());

        let city = i_city
            .filter(|&idx| idx < fields.len())
            .map(|idx| unquote(&fields[idx]))
            .filter(|s| !s.is_empty());

        addresses.push(RawAddress {
            osm_id: 0, // DAWA addresses don't have OSM IDs
            street: street.to_owned(),
            housenumber: husnr.to_owned(),
            postcode,
            city,
            state: None,
            lat,
            lon,
        });
    }

    info!(
        "Parsed {} addresses ({} skipped)",
        addresses.len(), skipped
    );

    Ok(addresses)
}

/// Parse a CSV header line, handling quoted fields with commas.
fn parse_csv_header(line: &str) -> Vec<&str> {
    // Simple split — header fields shouldn't contain commas
    line.split(',').collect()
}

/// Parse a CSV line, handling quoted fields that may contain commas.
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

/// Remove surrounding quotes and trim whitespace.
fn unquote(s: &str) -> String {
    s.trim().trim_matches('"').trim().to_owned()
}
