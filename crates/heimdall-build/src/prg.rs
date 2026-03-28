/// prg.rs — Parse Polish PRG (Państwowy Rejestr Granic) address points
///
/// PRG contains ~8M address points for Poland, published by GUGiK (Head Office
/// of Geodesy and Cartography). Available under open government licence.
///
/// Data formats supported:
///   1. CSV (semicolon-delimited): pre-processed flat file with EPSG:2180 coordinates
///   2. GML/ZIP: official PRG export with `prg:PRG_PunktAdresowy` elements
///
/// Coordinates are in PUWG 1992 (EPSG:2180) — Transverse Mercator on GRS80.
/// GRS80/ETRS89 is effectively identical to WGS84 (sub-meter difference),
/// so no datum shift is needed — only the projection math.
///
/// Download: https://dane.gov.pl/pl/dataset/726

use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use crate::extract::RawAddress;

/// Import PRG addresses from a CSV, GML, or ZIP file.
///
/// Detects format by extension:
///   - `.csv` → semicolon-delimited CSV with EPSG:2180 coordinates
///   - `.gml` → GML with `prg:PRG_PunktAdresowy` elements
///   - `.zip` → ZIP containing GML files
pub fn read_prg_addresses(input_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading PRG addresses from {}", input_path.display());

    let ext = input_path
        .extension()
        .unwrap_or_default()
        .to_string_lossy()
        .to_lowercase();

    let addresses = match ext.as_str() {
        "csv" => read_prg_csv(input_path)?,
        "gml" => read_prg_gml(input_path)?,
        "zip" => read_prg_zip(input_path)?,
        other => anyhow::bail!("unsupported PRG file extension: .{}", other),
    };

    info!("Parsed {} PRG addresses", addresses.len());
    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// CSV mode — semicolon-delimited flat file
// ───────────────────────────────────────────────────────────────────────────

fn read_prg_csv(csv_path: &Path) -> Result<Vec<RawAddress>> {
    info!("  Parsing CSV format");

    let file = std::fs::File::open(csv_path)
        .with_context(|| format!("failed to open {}", csv_path.display()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    // Parse header
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty PRG CSV file"))??;
    let header = header.trim_start_matches('\u{feff}');
    let cols: Vec<&str> = header.split(';').collect();

    // Try multiple column naming conventions
    let i_street = col_index(&cols, "ulica")
        .or_else(|| col_index(&cols, "ULICA"))
        .or_else(|| col_index(&cols, "nazwaUlicy"));
    let i_number = col_index(&cols, "numer")
        .or_else(|| col_index(&cols, "NUMER"))
        .or_else(|| col_index(&cols, "numerPorzadkowy"))
        .ok_or_else(|| anyhow::anyhow!("missing house number column (numer/numerPorzadkowy)"))?;
    let i_city = col_index(&cols, "miejscowosc")
        .or_else(|| col_index(&cols, "MIEJSCOWOSC"))
        .or_else(|| col_index(&cols, "miejscowość"));
    let i_postcode = col_index(&cols, "kod_pocztowy")
        .or_else(|| col_index(&cols, "KOD_POCZTOWY"))
        .or_else(|| col_index(&cols, "kodPocztowy"));
    let i_x = col_index(&cols, "x")
        .or_else(|| col_index(&cols, "X"))
        .or_else(|| col_index(&cols, "wspol_x"))
        .ok_or_else(|| anyhow::anyhow!("missing X coordinate column"))?;
    let i_y = col_index(&cols, "y")
        .or_else(|| col_index(&cols, "Y"))
        .or_else(|| col_index(&cols, "wspol_y"))
        .ok_or_else(|| anyhow::anyhow!("missing Y coordinate column"))?;

    let mut addresses = Vec::with_capacity(8_000_000);
    let mut skipped = 0usize;

    for line in lines {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(';').collect();

        // Parse coordinates (EPSG:2180)
        let x_str = fields.get(i_x).map(|s| s.trim()).unwrap_or("");
        let y_str = fields.get(i_y).map(|s| s.trim()).unwrap_or("");
        if x_str.is_empty() || y_str.is_empty() {
            skipped += 1;
            continue;
        }

        let x_2180: f64 = match x_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let y_2180: f64 = match y_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        let (lat, lon) = puwg92_to_wgs84(x_2180, y_2180);

        // Poland bbox sanity check: 49.00-54.84N, 14.12-24.15E
        if lat < 48.8 || lat > 55.0 || lon < 13.9 || lon > 24.3 {
            skipped += 1;
            continue;
        }

        // House number
        let housenumber = fields.get(i_number).map(|s| s.trim()).unwrap_or("");
        if housenumber.is_empty() {
            skipped += 1;
            continue;
        }

        // Street (may be empty for rural addresses)
        let street_raw = i_street
            .and_then(|idx| fields.get(idx))
            .map(|s| s.trim())
            .unwrap_or("");
        // City as fallback when street is empty
        let city_raw = i_city
            .and_then(|idx| fields.get(idx))
            .map(|s| s.trim())
            .unwrap_or("");

        let street = if !street_raw.is_empty() {
            street_raw.to_string()
        } else if !city_raw.is_empty() {
            // Rural addresses without streets — use city name
            city_raw.to_string()
        } else {
            skipped += 1;
            continue;
        };

        let city = if !city_raw.is_empty() {
            Some(city_raw.to_string())
        } else {
            None
        };

        // Postal code: "00-001" format — keep as-is
        let postcode = i_postcode
            .and_then(|idx| fields.get(idx))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        addresses.push(RawAddress {
            osm_id: 0,
            street,
            housenumber: housenumber.to_string(),
            postcode,
            city,
            lat,
            lon,
        });
    }

    info!("  CSV: {} addresses ({} skipped)", addresses.len(), skipped);
    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// GML mode — streaming XML parse of prg:PRG_PunktAdresowy elements
// ───────────────────────────────────────────────────────────────────────────

/// Intermediate state for one GML address element.
#[derive(Debug, Default)]
struct GmlAddress {
    street: String,
    housenumber: String,
    city: String,
    postcode: String,
    x: f64,
    y: f64,
    has_coord: bool,
}

fn read_prg_gml(gml_path: &Path) -> Result<Vec<RawAddress>> {
    info!("  Parsing GML format");

    let file = std::fs::File::open(gml_path)
        .with_context(|| format!("failed to open {}", gml_path.display()))?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    parse_prg_gml_reader(reader)
}

fn read_prg_zip(zip_path: &Path) -> Result<Vec<RawAddress>> {
    info!("  Extracting GML from ZIP");

    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("failed to open {}", zip_path.display()))?;
    let mut archive = zip::ZipArchive::new(file)?;

    let mut all_addresses = Vec::new();

    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        let name = entry.name().to_string();
        if !name.to_lowercase().ends_with(".gml") {
            continue;
        }

        info!("    Parsing {}", name);
        let reader = BufReader::with_capacity(8 * 1024 * 1024, entry);
        let mut addrs = parse_prg_gml_reader(reader)?;
        info!("    {} — {} addresses", name, addrs.len());
        all_addresses.append(&mut addrs);
    }

    Ok(all_addresses)
}

fn parse_prg_gml_reader<R: Read + BufRead>(reader: R) -> Result<Vec<RawAddress>> {
    let mut xml = quick_xml::Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut buf = Vec::with_capacity(1024);
    let mut addresses = Vec::with_capacity(8_000_000);
    let mut skipped = 0usize;

    let mut current: Option<GmlAddress> = None;
    let mut in_element: Option<String> = None;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(ref e)) => {
                let name_bytes = e.name().as_ref().to_vec();
                let local = local_name(&name_bytes);
                match local {
                    "PRG_PunktAdresowy" => {
                        current = Some(GmlAddress::default());
                    }
                    "ulica" | "numerPorzadkowy" | "miejscowosc" | "kodPocztowy" | "pos"
                        if current.is_some() =>
                    {
                        in_element = Some(local.to_string());
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::Text(ref e)) => {
                if let (Some(ref mut addr), Some(ref elem)) = (&mut current, &in_element) {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match elem.as_str() {
                        "ulica" => addr.street = text,
                        "numerPorzadkowy" => addr.housenumber = text,
                        "miejscowosc" => addr.city = text,
                        "kodPocztowy" => addr.postcode = text,
                        "pos" => {
                            // gml:pos contains "x y" in EPSG:2180
                            let parts: Vec<&str> = text.split_whitespace().collect();
                            if parts.len() >= 2 {
                                if let (Ok(x), Ok(y)) =
                                    (parts[0].parse::<f64>(), parts[1].parse::<f64>())
                                {
                                    addr.x = x;
                                    addr.y = y;
                                    addr.has_coord = true;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(quick_xml::events::Event::End(ref e)) => {
                let name_bytes = e.name().as_ref().to_vec();
                let local = local_name(&name_bytes);
                if in_element.as_ref().map_or(false, |el| el == local) {
                    in_element = None;
                }

                if local == "PRG_PunktAdresowy" {
                    if let Some(addr) = current.take() {
                        if !addr.has_coord || addr.housenumber.is_empty() {
                            skipped += 1;
                            continue;
                        }

                        let (lat, lon) = puwg92_to_wgs84(addr.x, addr.y);

                        // Poland bbox sanity check
                        if lat < 48.8 || lat > 55.0 || lon < 13.9 || lon > 24.3 {
                            skipped += 1;
                            continue;
                        }

                        let street = if !addr.street.is_empty() {
                            addr.street
                        } else if !addr.city.is_empty() {
                            addr.city.clone()
                        } else {
                            skipped += 1;
                            continue;
                        };

                        let city = if addr.city.is_empty() {
                            None
                        } else {
                            Some(addr.city)
                        };

                        let postcode = if addr.postcode.is_empty() {
                            None
                        } else {
                            Some(addr.postcode)
                        };

                        addresses.push(RawAddress {
                            osm_id: 0,
                            street,
                            housenumber: addr.housenumber,
                            postcode,
                            city,
                            lat,
                            lon,
                        });
                    }
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => {
                info!("  XML parse error (continuing): {}", e);
            }
            _ => {}
        }
        buf.clear();
    }

    info!("  GML: {} addresses ({} skipped)", addresses.len(), skipped);
    Ok(addresses)
}

/// Extract local name from a possibly-namespaced XML tag.
/// e.g. "prg:ulica" → "ulica", "gml:pos" → "pos"
fn local_name(full: &[u8]) -> &str {
    let s = std::str::from_utf8(full).unwrap_or("");
    match s.rfind(':') {
        Some(i) => &s[i + 1..],
        None => s,
    }
}

// ───────────────────────────────────────────────────────────────────────────
// CSV helpers
// ───────────────────────────────────────────────────────────────────────────

fn col_index(header: &[&str], name: &str) -> Option<usize> {
    header
        .iter()
        .position(|c| c.trim().trim_matches('"') == name)
}

// ───────────────────────────────────────────────────────────────────────────
// PUWG 1992 (EPSG:2180) → WGS84 coordinate conversion
// ───────────────────────────────────────────────────────────────────────────

/// Convert PUWG 1992 (EPSG:2180) coordinates to WGS84 (lat, lon).
///
/// PUWG 1992 is a Transverse Mercator projection on GRS80/ETRS89:
///   - Central meridian: 19°E
///   - Scale factor: 0.9993
///   - False easting: 500,000 m
///   - False northing: -5,300,000 m
///   - Ellipsoid: GRS80 (a=6378137, f=1/298.257222101)
///
/// GRS80/ETRS89 ≈ WGS84 (sub-meter difference), so no datum shift needed.
fn puwg92_to_wgs84(x: f64, y: f64) -> (f64, f64) {
    let a = 6_378_137.0_f64;
    let f = 1.0 / 298.257_222_101;
    let e2: f64 = 2.0 * f - f * f;
    let k0: f64 = 0.9993;
    let lambda0 = 19.0_f64.to_radians();
    let fe = 500_000.0;
    let fn_ = -5_300_000.0;

    // Remove false easting/northing
    let x_adj = x - fe;
    let y_adj = y - fn_;

    // Meridian arc distance
    let m = y_adj / k0;

    // Footpoint latitude via series expansion
    let e1 = (1.0 - (1.0 - e2).sqrt()) / (1.0 + (1.0 - e2).sqrt());
    let mu = m / (a * (1.0 - e2 / 4.0 - 3.0 * e2 * e2 / 64.0 - 5.0 * e2.powi(3) / 256.0));

    let phi1 = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1.powi(3) / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1 * e1 / 16.0 - 55.0 * e1.powi(4) / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1.powi(3) / 96.0) * (6.0 * mu).sin()
        + (1097.0 * e1.powi(4) / 512.0) * (8.0 * mu).sin();

    let sin_phi1 = phi1.sin();
    let cos_phi1 = phi1.cos();
    let tan_phi1 = phi1.tan();
    let n1 = a / (1.0 - e2 * sin_phi1 * sin_phi1).sqrt();
    let t1 = tan_phi1 * tan_phi1;
    let ep2 = e2 / (1.0 - e2);
    let c1 = ep2 * cos_phi1 * cos_phi1;
    let r1 = a * (1.0 - e2) / (1.0 - e2 * sin_phi1 * sin_phi1).powf(1.5);
    let d = x_adj / (n1 * k0);

    let lat = phi1
        - (n1 * tan_phi1 / r1)
            * (d * d / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * ep2) * d.powi(4) / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1 - 252.0 * ep2
                    - 3.0 * c1 * c1)
                    * d.powi(6)
                    / 720.0);

    let lon = lambda0
        + (d - (1.0 + 2.0 * t1 + c1) * d.powi(3) / 6.0
            + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * ep2 + 24.0 * t1 * t1)
                * d.powi(5)
                / 120.0)
            / cos_phi1;

    (lat.to_degrees(), lon.to_degrees())
}
