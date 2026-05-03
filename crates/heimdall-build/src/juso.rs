/// juso.rs — Parse South Korean road-name addresses from juso.go.kr
///
/// Korea's RASS (Road name Address System) provides ~6M road-name addresses.
/// Data is distributed as ZIP files (one per province) containing pipe-delimited
/// text files. A separate building coordinate file provides WGS84 lat/lon.
///
/// Schema (pipe-delimited, 0-indexed columns):
///   2:  시도명 (province)          — e.g., "서울특별시"
///   3:  시군구명 (city/county)     — e.g., "종로구"
///  10:  도로명 (road name)         — e.g., "삼청로"
///  12:  건물본번 (building main #) — e.g., "52"
///  13:  건물부번 (building sub #)  — "0" means none, else "52-3"
///  14:  건물관리번호 (building mgmt #) — join key for coordinates
///  18:  우편번호 (5-digit postcode)
///
/// Coordinate file (건물DB, pipe-delimited):
///   건물관리번호 → 경도 (lon), 위도 (lat) in WGS84
///
/// Download: https://www.juso.go.kr (requires Korean agreement page)

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{bail, Result};
use tracing::{info, warn};

use crate::extract::RawAddress;

/// Read Korean road-name addresses from a directory of juso.go.kr data files.
/// Accepts either a directory of extracted .txt files or ZIP files.
///
/// The directory should contain:
///   - Address files (도로명, 주소, rnaddrkor): pipe-delimited address records
///   - Coordinate files (좌표, coordinate, 건물, jibun_bldg): building coords
///
/// If no coordinate file is found, all addresses are skipped with a warning.
pub fn read_juso_addresses(input_path: &Path) -> Result<Vec<RawAddress>> {
    if !input_path.exists() {
        bail!("Input path does not exist: {}", input_path.display());
    }

    if input_path.is_file() {
        // Single ZIP file — extract first, then process
        return read_juso_from_zip(input_path);
    }

    info!("Reading juso.go.kr addresses from {}", input_path.display());

    // Extract any ZIP files in the directory first
    extract_zips_in_dir(input_path)?;

    // Phase 1: Load coordinate file → HashMap<building_mgmt_no, (lat, lon)>
    let coords = load_coordinates(input_path)?;
    info!("  Loaded {} building coordinates", coords.len());

    if coords.is_empty() {
        warn!(
            "  No coordinate file found in {}. \
             Addresses without coordinates will be skipped. \
             Place files matching '좌표', 'coordinate', '건물', or 'jibun_bldg' in the directory.",
            input_path.display()
        );
    }

    // Phase 2: Read address files, join with coordinates → RawAddress
    let addresses = load_and_join_addresses(input_path, &coords)?;

    info!("Parsed {} juso.go.kr addresses", addresses.len());
    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// ZIP handling
// ───────────────────────────────────────────────────────────────────────────

fn read_juso_from_zip(zip_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading juso.go.kr addresses from ZIP: {}", zip_path.display());

    let extract_dir = zip_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("juso_extract");
    std::fs::create_dir_all(&extract_dir)?;

    let extracted = extract_txt_from_zip(zip_path, &extract_dir)?;
    info!("  Extracted {} files", extracted);

    // Extract nested ZIPs (juso distributes province ZIPs inside an outer ZIP)
    extract_zips_in_dir(&extract_dir)?;

    let coords = load_coordinates(&extract_dir)?;
    info!("  Loaded {} building coordinates", coords.len());

    let addresses = load_and_join_addresses(&extract_dir, &coords)?;

    // Cleanup
    std::fs::remove_dir_all(&extract_dir).ok();
    info!("Parsed {} juso.go.kr addresses", addresses.len());

    Ok(addresses)
}

fn extract_txt_from_zip(zip_path: &Path, out_dir: &Path) -> Result<usize> {
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

        let lower = filename.to_lowercase();
        if !lower.ends_with(".txt") && !lower.ends_with(".zip") {
            continue;
        }

        let out_path = out_dir.join(&filename);
        let mut out_file = std::fs::File::create(&out_path)?;
        std::io::copy(&mut entry, &mut out_file)?;
        count += 1;
    }

    Ok(count)
}

/// Extract any ZIP files found inside a directory (nested province ZIPs).
fn extract_zips_in_dir(dir: &Path) -> Result<()> {
    let zip_files: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .map(|ext| ext.to_ascii_lowercase() == "zip")
                .unwrap_or(false)
        })
        .collect();

    for zip_path in &zip_files {
        let count = extract_txt_from_zip(zip_path, dir)?;
        if count > 0 {
            info!(
                "  Extracted {} files from nested ZIP: {}",
                count,
                zip_path.file_name().unwrap_or_default().to_string_lossy()
            );
        }
    }

    Ok(())
}

// ───────────────────────────────────────────────────────────────────────────
// Pipe-delimited parsing helpers
// ───────────────────────────────────────────────────────────────────────────

/// Check if a filename looks like a coordinate/building file.
fn is_coord_file(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    // Korean names: 좌표 (coordinate), 건물 (building)
    // Romanized: coordinate, jibun_bldg, building, match_build
    lower.contains("좌표")
        || lower.contains("건물")
        || lower.contains("coordinate")
        || lower.contains("jibun_bldg")
        || lower.contains("match_build")
}

/// Check if a filename looks like an address file.
fn is_address_file(filename: &str) -> bool {
    let lower = filename.to_lowercase();
    // Korean: 도로명 (road name), 주소 (address)
    // Romanized: rnaddrkor, rnaddr, addr
    lower.contains("도로명")
        || lower.contains("주소")
        || lower.contains("rnaddrkor")
        || lower.contains("rnaddr")
        || (lower.contains("addr") && !is_coord_file(filename))
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 1: Coordinate file → building_mgmt_no → (lat, lon)
// ───────────────────────────────────────────────────────────────────────────

fn load_coordinates(dir: &Path) -> Result<HashMap<String, (f64, f64)>> {
    let mut map = HashMap::with_capacity(6_000_000);

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            continue;
        }
        let fname = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        if !fname.to_lowercase().ends_with(".txt") {
            continue;
        }
        if !is_coord_file(&fname) {
            continue;
        }

        info!("  Loading coordinate file: {}", fname);
        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut file_count = 0usize;
        let mut line_num = 0usize;

        for line_result in reader.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    if line_num == 0 {
                        warn!(
                            "  Cannot read {} as UTF-8: {}. \
                             Please convert to UTF-8 (iconv -f EUC-KR -t UTF-8).",
                            fname, e
                        );
                        break;
                    }
                    continue;
                }
            };
            line_num += 1;

            // Skip BOM on first line
            let line = if line_num == 1 {
                line.trim_start_matches('\u{feff}')
            } else {
                &line
            };

            // Skip header line
            if line_num == 1 {
                continue;
            }

            let fields: Vec<&str> = line.split('|').collect();
            if fields.len() < 3 {
                continue;
            }

            // Coordinate file format varies, but the building management number
            // is typically the first field. Lat/lon are the last two numeric fields.
            // Try to find the building management number (25-digit key) and coordinates.
            let (mgmt_no, lat, lon) = match parse_coord_line(&fields) {
                Some(v) => v,
                None => continue,
            };

            // South Korea bbox: 33.11-38.62N, 124.60-131.87E
            if lat < 33.0 || lat > 39.0 || lon < 124.0 || lon > 132.0 {
                continue;
            }

            map.insert(mgmt_no, (lat, lon));
            file_count += 1;
        }

        info!("    {} — {} coordinates", fname, file_count);
    }

    Ok(map)
}

/// Parse a coordinate line to extract (building_mgmt_no, lat, lon).
///
/// The building coordinate file has the building management number as the first
/// long numeric field (typically 25 digits), and lat/lon as floating-point fields.
/// We scan for these patterns since column positions vary between file versions.
fn parse_coord_line(fields: &[&str]) -> Option<(String, f64, f64)> {
    // Find the building management number: first field that is a long digit string (>=15 chars)
    let mgmt_no = fields
        .iter()
        .map(|f| f.trim())
        .find(|f| f.len() >= 15 && f.chars().all(|c| c.is_ascii_digit()))?
        .to_string();

    // Find lat and lon: look for floating-point numbers in the Korean geographic range.
    // Latitude: 33-39, Longitude: 124-132
    let mut lat = None;
    let mut lon = None;

    for field in fields {
        let f = field.trim();
        if let Ok(v) = f.parse::<f64>() {
            if v >= 33.0 && v <= 39.0 && lat.is_none() {
                lat = Some(v);
            } else if v >= 124.0 && v <= 132.0 && lon.is_none() {
                lon = Some(v);
            }
        }
    }

    Some((mgmt_no, lat?, lon?))
}

// ───────────────────────────────────────────────────────────────────────────
// Phase 2: Address files + coordinates → RawAddress
// ───────────────────────────────────────────────────────────────────────────

fn load_and_join_addresses(
    dir: &Path,
    coords: &HashMap<String, (f64, f64)>,
) -> Result<Vec<RawAddress>> {
    let mut addresses = Vec::with_capacity(6_000_000);
    let mut skipped_no_coord = 0usize;
    let mut skipped_parse = 0usize;

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            continue;
        }
        let fname = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        if !fname.to_lowercase().ends_with(".txt") {
            continue;
        }
        if !is_address_file(&fname) {
            continue;
        }

        info!("  Reading address file: {}", fname);
        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut file_count = 0usize;
        let mut line_num = 0usize;

        for line_result in reader.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    if line_num == 0 {
                        warn!(
                            "  Cannot read {} as UTF-8: {}. \
                             Please convert to UTF-8 (iconv -f EUC-KR -t UTF-8).",
                            fname, e
                        );
                        break;
                    }
                    continue;
                }
            };
            line_num += 1;

            // Strip BOM
            let line = if line_num == 1 {
                line.trim_start_matches('\u{feff}')
            } else {
                &line
            };

            // Skip header line (contains Korean column names)
            if line_num == 1 && (line.contains("도로명") || line.contains("시도명")) {
                continue;
            }

            let fields: Vec<&str> = line.split('|').collect();

            // Need at least 19 columns for the standard format
            if fields.len() < 19 {
                skipped_parse += 1;
                continue;
            }

            // Column 10: 도로명 (road name)
            let road_name = fields[10].trim();
            if road_name.is_empty() {
                skipped_parse += 1;
                continue;
            }

            // Column 12: 건물본번 (building main number)
            let main_num = fields[12].trim();
            if main_num.is_empty() || main_num == "0" {
                skipped_parse += 1;
                continue;
            }

            // Column 13: 건물부번 (building sub-number)
            let sub_num = fields[13].trim();
            let housenumber = if sub_num.is_empty() || sub_num == "0" {
                main_num.to_owned()
            } else {
                format!("{}-{}", main_num, sub_num)
            };

            // Column 3: 시군구명 (city/county)
            let city_name = fields[3].trim();
            let city = if city_name.is_empty() {
                // Fall back to column 2 (province) if no city
                let province = fields[2].trim();
                if province.is_empty() {
                    None
                } else {
                    Some(province.to_owned())
                }
            } else {
                Some(city_name.to_owned())
            };

            // Column 18: 우편번호 (postcode, 5 digits)
            let postcode_raw = fields[18].trim();
            let postcode = if postcode_raw.len() == 5
                && postcode_raw.chars().all(|c| c.is_ascii_digit())
            {
                Some(postcode_raw.to_owned())
            } else if !postcode_raw.is_empty() {
                // Accept non-standard postcodes too
                Some(postcode_raw.to_owned())
            } else {
                None
            };

            // Column 14: 건물관리번호 (building management number) — join key
            let mgmt_no = fields[14].trim();

            // Lookup coordinates
            let (lat, lon) = match coords.get(mgmt_no) {
                Some(c) => *c,
                None => {
                    skipped_no_coord += 1;
                    continue;
                }
            };

            addresses.push(RawAddress {
                osm_id: 0,
                street: road_name.to_owned(),
                housenumber,
                postcode,
                city,
                state: None,
                lat,
                lon,
            });
            file_count += 1;
        }

        info!(
            "    {} — {} addresses",
            fname, file_count
        );
    }

    if skipped_no_coord > 0 {
        warn!(
            "  {} addresses skipped (no coordinate match)",
            skipped_no_coord
        );
    }
    if skipped_parse > 0 {
        info!("  {} lines skipped (parse/empty)", skipped_parse);
    }
    info!(
        "  Join complete: {} addresses ({} no-coord, {} parse-skip)",
        addresses.len(),
        skipped_no_coord,
        skipped_parse,
    );

    Ok(addresses)
}
