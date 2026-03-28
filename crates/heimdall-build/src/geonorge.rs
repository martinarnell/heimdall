/// geonorge.rs — Parse Kartverket/Geonorge address CSV data
///
/// The Norwegian Matrikkelen address dataset is a semicolon-separated CSV
/// in EPSG:4258 (ETRS89 geographic ≈ WGS84). No coordinate transform needed.
///
/// Key fields: adressenavn (street), nummer (number), bokstav (suffix),
/// postnummer (postcode), poststed (city), Nord (lat), Øst (lon)

use std::path::Path;
use std::io::{BufRead, BufReader};
use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

/// Parse the Kartverket address CSV into RawAddress records.
pub fn read_kartverket_addresses(csv_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading Kartverket addresses from {}", csv_path.display());

    let file = std::fs::File::open(csv_path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    // Parse header to find column indices
    let header = lines.next()
        .ok_or_else(|| anyhow::anyhow!("empty CSV"))??;
    // Strip BOM if present
    let header = header.trim_start_matches('\u{feff}');
    let cols: Vec<&str> = header.split(';').collect();

    let idx = |name: &str| -> Option<usize> {
        cols.iter().position(|c| c.trim() == name)
    };

    let i_street = idx("adressenavn").ok_or_else(|| anyhow::anyhow!("missing adressenavn column"))?;
    let i_number = idx("nummer").ok_or_else(|| anyhow::anyhow!("missing nummer column"))?;
    let i_letter = idx("bokstav");
    let i_postcode = idx("postnummer");
    let i_city = idx("poststed");
    let i_lat = idx("Nord").ok_or_else(|| anyhow::anyhow!("missing Nord column"))?;
    let i_lon = idx("Øst").ok_or_else(|| anyhow::anyhow!("missing Øst column"))?;

    let mut addresses = Vec::new();
    let mut skipped = 0usize;

    for line_result in lines {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => { skipped += 1; continue; }
        };

        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= i_lat.max(i_lon) {
            skipped += 1;
            continue;
        }

        let street = fields[i_street].trim();
        let number = fields[i_number].trim();
        if street.is_empty() || number.is_empty() {
            skipped += 1;
            continue;
        }

        let lat: f64 = match fields[i_lat].trim().parse() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };
        let lon: f64 = match fields[i_lon].trim().parse() {
            Ok(v) => v,
            Err(_) => { skipped += 1; continue; }
        };

        // Sanity check coordinates (Norway: 57-81N, 4-32E)
        if lat < 57.0 || lat > 81.0 || lon < 4.0 || lon > 32.0 {
            skipped += 1;
            continue;
        }

        // Combine number + letter suffix
        let housenumber = match i_letter {
            Some(idx) if idx < fields.len() && !fields[idx].trim().is_empty() => {
                format!("{}{}", number, fields[idx].trim())
            }
            _ => number.to_owned(),
        };

        let postcode = i_postcode
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_owned());

        let city = i_city
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_owned());

        addresses.push(RawAddress {
            osm_id: 0, // Kartverket addresses don't have OSM IDs
            street: street.to_owned(),
            housenumber,
            postcode,
            city,
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
