/// nar.rs — Parse Statistics Canada NAR (National Address Register) data
///
/// NAR contains 15.8M civic addresses for Canada, published twice yearly.
/// Available under the Statistics Canada Open Licence (permissive, attribution required).
///
/// Data format: ZIP containing CSV files in two sets:
///   - Address files: civic number, street name, municipality, province, postal code
///   - Location files: lat/lon per building, joined on locationId
///
/// Download: https://open.canada.ca/data/en/dataset/a587c941-261d-4e00-bda2-bdd92cd76e8a

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

/// Import NAR addresses from a ZIP file.
///
/// Extracts CSV files, loads location coordinates, then joins with address data.
pub fn read_nar_addresses(zip_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading NAR addresses from {}", zip_path.display());

    let extract_dir = zip_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("nar_extract");
    std::fs::create_dir_all(&extract_dir)?;

    // Extract CSV files from ZIP
    info!("  Extracting CSV files from ZIP...");
    let extracted = extract_nar_csv(zip_path, &extract_dir)?;
    info!("  Extracted {} files", extracted);

    // Phase 1: Load location files → HashMap<locationId, (lat, lon)>
    let locations = load_locations(&extract_dir)?;
    info!("  Loaded {} locations", locations.len());

    // Phase 2: Read address files, join with locations → RawAddress
    let addresses = join_addresses(&extract_dir, &locations)?;

    // Cleanup
    std::fs::remove_dir_all(&extract_dir).ok();
    info!("Parsed {} NAR addresses", addresses.len());

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// ZIP extraction — extract all CSV files
// ───────────────────────────────────────────────────────────────────────────

fn extract_nar_csv(zip_path: &Path, out_dir: &Path) -> Result<usize> {
    let file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut count = 0;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        let full_name = entry.name().to_string();
        let filename = Path::new(&full_name)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        // Only extract CSV files
        if !filename.to_lowercase().ends_with(".csv") {
            continue;
        }

        let out_path = out_dir.join(&filename);
        let mut out_file = std::fs::File::create(&out_path)?;
        std::io::copy(&mut entry, &mut out_file)?;
        count += 1;
    }

    Ok(count)
}

// ───────────────────────────────────────────────────────────────────────────
// PSV/CSV parsing helpers
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

/// Title-case a string: "NORTH VANCOUVER" → "North Vancouver"
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

// ───────────────────────────────────────────────────────────────────────────
// Phase 1: Location files → locationId → (lat, lon)
// ───────────────────────────────────────────────────────────────────────────

fn load_locations(dir: &Path) -> Result<HashMap<String, (f64, f64)>> {
    let mut map = HashMap::with_capacity(16_000_000);

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let fname = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();

        // Location files contain "location" in the name
        if !fname.contains("location") || !fname.ends_with(".csv") {
            continue;
        }

        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut lines = reader.lines();

        let header = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty location file: {}", fname))??;
        let header = header.trim_start_matches('\u{feff}');
        let cols: Vec<&str> = header.split(',').collect();

        // Try different possible column names
        let i_id = col_index(&cols, "locationId")
            .or_else(|| col_index(&cols, "LOCATION_ID"))
            .or_else(|| col_index(&cols, "location_id"))
            .or_else(|| col_index(&cols, "LOC_GUID"))
            .or_else(|| col_index(&cols, "loc_guid"))
            .ok_or_else(|| anyhow::anyhow!("missing locationId/LOC_GUID column in {}", fname))?;

        let i_lat = col_index(&cols, "LATITUDE")
            .or_else(|| col_index(&cols, "latitude"))
            .or_else(|| col_index(&cols, "lat"))
            .or_else(|| col_index(&cols, "BG_LATITUDE"))
            .or_else(|| col_index(&cols, "bg_latitude"))
            .ok_or_else(|| anyhow::anyhow!("missing LATITUDE/BG_LATITUDE column in {}", fname))?;

        let i_lon = col_index(&cols, "LONGITUDE")
            .or_else(|| col_index(&cols, "longitude"))
            .or_else(|| col_index(&cols, "lon"))
            .or_else(|| col_index(&cols, "BG_LONGITUDE"))
            .or_else(|| col_index(&cols, "bg_longitude"))
            .ok_or_else(|| anyhow::anyhow!("missing LONGITUDE/BG_LONGITUDE column in {}", fname))?;

        let mut file_count = 0usize;

        for line in lines {
            let line = line?;
            let fields = parse_csv_line(&line);
            if fields.len() <= i_id.max(i_lat).max(i_lon) {
                continue;
            }

            let id = unquote(&fields[i_id]);
            if id.is_empty() {
                continue;
            }

            let lat: f64 = match unquote(&fields[i_lat]).parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let lon: f64 = match unquote(&fields[i_lon]).parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Canada bbox: 41.7-83.1N, 141.0W-52.6W
            if lat < 41.0 || lat > 84.0 || lon < -141.5 || lon > -52.0 {
                continue;
            }

            map.insert(id, (lat, lon));
            file_count += 1;
        }

        info!("    {} — {} locations", fname, file_count);
    }

    Ok(map)
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 2: Address files + locations → RawAddress
// ───────────────────────────────────────────────────────────────────────────

fn join_addresses(
    dir: &Path,
    locations: &HashMap<String, (f64, f64)>,
) -> Result<Vec<RawAddress>> {
    let mut addresses = Vec::with_capacity(16_000_000);
    let mut skipped = 0usize;

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let fname = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();

        // Address files: contain "address" but not "location"
        if fname.contains("location") || !fname.contains("address") || !fname.ends_with(".csv") {
            continue;
        }

        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut lines = reader.lines();

        let header = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty address file: {}", fname))??;
        let header = header.trim_start_matches('\u{feff}');
        let cols: Vec<&str> = header.split(',').collect();

        // Find columns — try multiple naming conventions
        let i_id = col_index(&cols, "locationId")
            .or_else(|| col_index(&cols, "LOCATION_ID"))
            .or_else(|| col_index(&cols, "location_id"))
            .or_else(|| col_index(&cols, "LOC_GUID"))
            .or_else(|| col_index(&cols, "loc_guid"))
            .ok_or_else(|| anyhow::anyhow!("missing locationId/LOC_GUID in {}", fname))?;

        let i_civic_num = col_index(&cols, "CIVIC_NUMBER")
            .or_else(|| col_index(&cols, "civic_number"))
            .or_else(|| col_index(&cols, "civicNumber"));

        let i_street_name = col_index(&cols, "STREET_NAME")
            .or_else(|| col_index(&cols, "street_name"))
            .or_else(|| col_index(&cols, "streetName"));

        let i_street_type = col_index(&cols, "STREET_TYPE")
            .or_else(|| col_index(&cols, "street_type"))
            .or_else(|| col_index(&cols, "streetType"));

        let i_street_dir = col_index(&cols, "STREET_DIRECTION")
            .or_else(|| col_index(&cols, "street_direction"))
            .or_else(|| col_index(&cols, "streetDirection"));

        let i_municipality = col_index(&cols, "MUNICIPALITY")
            .or_else(|| col_index(&cols, "municipality"))
            .or_else(|| col_index(&cols, "municipalityName"));

        let i_province = col_index(&cols, "PROVINCE")
            .or_else(|| col_index(&cols, "province"))
            .or_else(|| col_index(&cols, "provinceName"));

        let i_postal = col_index(&cols, "POSTAL_CODE")
            .or_else(|| col_index(&cols, "postal_code"))
            .or_else(|| col_index(&cols, "postalCode"));

        let mut file_count = 0usize;

        for line in lines {
            let line = line?;
            let fields = parse_csv_line(&line);
            if fields.len() <= i_id {
                skipped += 1;
                continue;
            }

            // Lookup coordinates from location data
            let id = unquote(&fields[i_id]);
            let (lat, lon) = match locations.get(&id) {
                Some(coords) => *coords,
                None => {
                    skipped += 1;
                    continue;
                }
            };

            // Civic number (house number)
            let civic_num = i_civic_num
                .filter(|&idx| idx < fields.len())
                .map(|idx| unquote(&fields[idx]))
                .unwrap_or_default();
            if civic_num.is_empty() {
                skipped += 1;
                continue;
            }

            // Build street name: STREET_NAME + STREET_TYPE + STREET_DIRECTION
            let street_name = i_street_name
                .filter(|&idx| idx < fields.len())
                .map(|idx| unquote(&fields[idx]))
                .unwrap_or_default();
            if street_name.is_empty() {
                skipped += 1;
                continue;
            }

            let street_type = i_street_type
                .filter(|&idx| idx < fields.len())
                .map(|idx| unquote(&fields[idx]))
                .unwrap_or_default();
            let street_dir = i_street_dir
                .filter(|&idx| idx < fields.len())
                .map(|idx| unquote(&fields[idx]))
                .unwrap_or_default();

            let mut full_street = title_case(&street_name);
            if !street_type.is_empty() {
                full_street.push(' ');
                full_street.push_str(&title_case(&street_type));
            }
            if !street_dir.is_empty() {
                full_street.push(' ');
                full_street.push_str(&title_case(&street_dir));
            }

            // Municipality (city)
            let city = i_municipality
                .filter(|&idx| idx < fields.len())
                .map(|idx| unquote(&fields[idx]))
                .filter(|s| !s.is_empty())
                .map(|s| title_case(&s));

            // Postal code (A1A 1A1 format)
            let mut postcode = i_postal
                .filter(|&idx| idx < fields.len())
                .map(|idx| unquote(&fields[idx]))
                .filter(|s| !s.is_empty());

            // Add province to city if available
            let city = match (city, i_province) {
                (Some(c), Some(idx)) if idx < fields.len() => {
                    let prov = unquote(&fields[idx]);
                    if prov.is_empty() {
                        Some(c)
                    } else {
                        // Don't append province — keep just municipality
                        Some(c)
                    }
                }
                (c, _) => c,
            };

            // Normalize postal code: ensure space (A1A1A1 → A1A 1A1)
            postcode = postcode.map(|pc| {
                let pc = pc.replace(' ', "");
                if pc.len() == 6 {
                    format!("{} {}", &pc[..3], &pc[3..])
                } else {
                    pc
                }
            });

            addresses.push(RawAddress {
                osm_id: 0,
                street: full_street,
                housenumber: civic_num,
                postcode,
                city,
                lat,
                lon,
            });
            file_count += 1;
        }

        info!(
            "    {} — {} addresses",
            path.file_name().unwrap_or_default().to_string_lossy(),
            file_count
        );
    }

    info!(
        "  Join complete: {} addresses ({} skipped)",
        addresses.len(),
        skipped,
    );

    Ok(addresses)
}
