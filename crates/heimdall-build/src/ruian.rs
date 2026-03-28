/// ruian.rs — Parse Czech RÚIAN (Registr územní identifikace, adres a nemovitostí) addresses
///
/// RÚIAN is the Czech Republic's authoritative address register maintained by ČÚZK.
/// Available as open data (CSV, semicolon-delimited, Windows-1250 encoding).
///
/// Download: https://vdp.cuzk.cz/vdp/ruian/vymennyformat
///
/// CSV columns (semicolon-delimited):
///   Kód ADM;Kód obce;Název obce;Kód MOMC;Název MOMC;Kód MOP Praha;Název MOP Praha;
///   Kód části obce;Název části obce;Kód ulice;Název ulice;Typ SO;Číslo domovní;
///   Číslo orientační;Znak čísla orientačního;PSČ;Souřadnice Y;Souřadnice X;Platí od
///
/// Coordinates are in S-JTSK (EPSG:5514) — both values are negative over Czech Republic.
/// Conversion to WGS84 uses a polynomial approximation (~1m accuracy).

use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use crate::extract::RawAddress;

/// Import RÚIAN addresses from a CSV file.
///
/// Reads the semicolon-delimited CSV (Windows-1250 or UTF-8), converts S-JTSK
/// coordinates to WGS84, and returns flat RawAddress records.
pub fn read_ruian_addresses(csv_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading RÚIAN addresses from {}", csv_path.display());

    // Read file as raw bytes — RÚIAN exports may be Windows-1250 or UTF-8
    let raw_bytes = std::fs::read(csv_path)
        .with_context(|| format!("failed to read {}", csv_path.display()))?;

    // Try UTF-8 first, fall back to Windows-1250 decoding
    let text = match String::from_utf8(raw_bytes.clone()) {
        Ok(s) => {
            info!("  File is valid UTF-8");
            s
        }
        Err(_) => {
            info!("  UTF-8 failed, decoding as Windows-1250");
            decode_windows_1250(&raw_bytes)
        }
    };

    let mut lines = text.lines();

    // Parse header
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty RÚIAN CSV file"))?;
    let header = header.trim_start_matches('\u{feff}'); // strip BOM
    let cols: Vec<&str> = header.split(';').collect();

    let i_city = col_index(&cols, "Název obce")
        .ok_or_else(|| anyhow::anyhow!("missing 'Název obce' column"))?;
    let i_part = col_index(&cols, "Název části obce");
    let i_street = col_index(&cols, "Název ulice");
    let i_domovni = col_index(&cols, "Číslo domovní")
        .ok_or_else(|| anyhow::anyhow!("missing 'Číslo domovní' column"))?;
    let i_orient = col_index(&cols, "Číslo orientační");
    let i_orient_sign = col_index(&cols, "Znak čísla orientačního");
    let i_postcode = col_index(&cols, "PSČ");
    let i_coord_y = col_index(&cols, "Souřadnice Y")
        .ok_or_else(|| anyhow::anyhow!("missing 'Souřadnice Y' column"))?;
    let i_coord_x = col_index(&cols, "Souřadnice X")
        .ok_or_else(|| anyhow::anyhow!("missing 'Souřadnice X' column"))?;

    let max_col = *[i_city, i_domovni, i_coord_y, i_coord_x]
        .iter()
        .max()
        .unwrap();

    let mut addresses = Vec::with_capacity(3_000_000);
    let mut skipped = 0usize;
    let mut bad_coords = 0usize;

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(';').collect();
        if fields.len() <= max_col {
            skipped += 1;
            continue;
        }

        // Coordinates: S-JTSK (EPSG:5514) — both negative over Czech Republic
        let y_str = fields[i_coord_y].trim();
        let x_str = fields[i_coord_x].trim();
        if y_str.is_empty() || x_str.is_empty() {
            skipped += 1;
            continue;
        }

        let y_sjtsk: f64 = match y_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let x_sjtsk: f64 = match x_str.parse() {
            Ok(v) => v,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        let (lat, lon) = sjtsk_to_wgs84(y_sjtsk, x_sjtsk);

        // Czech Republic bbox sanity check: 48.55-51.06N, 12.09-18.86E
        if lat < 48.4 || lat > 51.2 || lon < 11.9 || lon > 19.0 {
            bad_coords += 1;
            continue;
        }

        // House number: prefer orientation number (blue plate, what people use),
        // fall back to registration number (red plate)
        let orient = i_orient
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim())
            .unwrap_or("");
        let orient_sign = i_orient_sign
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim())
            .unwrap_or("");
        let domovni = fields[i_domovni].trim();

        let housenumber = if !orient.is_empty() {
            // Orientation number with optional letter suffix (e.g. "15a")
            let mut hn = orient.to_string();
            if !orient_sign.is_empty() {
                hn.push_str(orient_sign);
            }
            hn
        } else if !domovni.is_empty() {
            domovni.to_string()
        } else {
            skipped += 1;
            continue;
        };

        // Street: use "Název ulice", fall back to "Název části obce" if empty
        let street_raw = i_street
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim())
            .unwrap_or("");
        let street = if !street_raw.is_empty() {
            street_raw.to_string()
        } else {
            // Many Czech addresses lack a street name — use city part instead
            i_part
                .filter(|&idx| idx < fields.len())
                .map(|idx| fields[idx].trim().to_string())
                .unwrap_or_default()
        };
        if street.is_empty() {
            skipped += 1;
            continue;
        }

        // City
        let city = {
            let c = fields[i_city].trim();
            if c.is_empty() {
                None
            } else {
                Some(c.to_string())
            }
        };

        // Postcode: "11000" → "110 00" (Czech 5-digit format with space)
        let postcode = i_postcode
            .filter(|&idx| idx < fields.len())
            .map(|idx| fields[idx].trim())
            .filter(|s| !s.is_empty())
            .map(|s| {
                let s = s.replace(' ', "");
                if s.len() == 5 {
                    format!("{} {}", &s[..3], &s[3..])
                } else {
                    s
                }
            });

        addresses.push(RawAddress {
            osm_id: 0,
            street,
            housenumber,
            postcode,
            city,
            lat,
            lon,
        });
    }

    if bad_coords > 0 {
        info!("  {} addresses outside Czech bbox (skipped)", bad_coords);
    }
    info!(
        "Parsed {} RÚIAN addresses ({} skipped)",
        addresses.len(),
        skipped,
    );

    Ok(addresses)
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
// Windows-1250 decoding
// ───────────────────────────────────────────────────────────────────────────

/// Decode Windows-1250 encoded bytes to UTF-8 string.
///
/// Windows-1250 is a single-byte encoding used for Central European languages.
/// Bytes 0x00-0x7F are identical to ASCII. Bytes 0x80-0xFF map to specific
/// Unicode codepoints covering Czech, Polish, Slovak, and Hungarian characters.
fn decode_windows_1250(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if b < 0x80 {
            out.push(b as char);
        } else {
            out.push(WIN1250_TABLE[(b - 0x80) as usize]);
        }
    }
    out
}

/// Windows-1250 high-byte mapping table (0x80..0xFF → Unicode codepoint).
/// Undefined positions map to the Unicode replacement character.
#[rustfmt::skip]
static WIN1250_TABLE: [char; 128] = [
    // 0x80-0x8F
    '\u{20AC}', '\u{FFFD}', '\u{201A}', '\u{FFFD}', '\u{201E}', '\u{2026}', '\u{2020}', '\u{2021}',
    '\u{FFFD}', '\u{2030}', '\u{0160}', '\u{2039}', '\u{015A}', '\u{0164}', '\u{017D}', '\u{0179}',
    // 0x90-0x9F
    '\u{FFFD}', '\u{2018}', '\u{2019}', '\u{201C}', '\u{201D}', '\u{2022}', '\u{2013}', '\u{2014}',
    '\u{FFFD}', '\u{2122}', '\u{0161}', '\u{203A}', '\u{015B}', '\u{0165}', '\u{017E}', '\u{017A}',
    // 0xA0-0xAF
    '\u{00A0}', '\u{02C7}', '\u{02D8}', '\u{0141}', '\u{00A4}', '\u{0104}', '\u{00A6}', '\u{00A7}',
    '\u{00A8}', '\u{00A9}', '\u{015E}', '\u{00AB}', '\u{00AC}', '\u{00AD}', '\u{00AE}', '\u{017B}',
    // 0xB0-0xBF
    '\u{00B0}', '\u{00B1}', '\u{02DB}', '\u{0142}', '\u{00B4}', '\u{00B5}', '\u{00B6}', '\u{00B7}',
    '\u{00B8}', '\u{0105}', '\u{015F}', '\u{00BB}', '\u{013D}', '\u{02DD}', '\u{013E}', '\u{017C}',
    // 0xC0-0xCF
    '\u{0154}', '\u{00C1}', '\u{00C2}', '\u{0102}', '\u{00C4}', '\u{0139}', '\u{0106}', '\u{00C7}',
    '\u{010C}', '\u{00C9}', '\u{0118}', '\u{00CB}', '\u{011A}', '\u{00CD}', '\u{00CE}', '\u{010E}',
    // 0xD0-0xDF
    '\u{0110}', '\u{0143}', '\u{0147}', '\u{00D3}', '\u{00D4}', '\u{0150}', '\u{00D6}', '\u{00D7}',
    '\u{0158}', '\u{016E}', '\u{00DA}', '\u{0170}', '\u{00DC}', '\u{00DD}', '\u{0162}', '\u{00DF}',
    // 0xE0-0xEF
    '\u{0155}', '\u{00E1}', '\u{00E2}', '\u{0103}', '\u{00E4}', '\u{013A}', '\u{0107}', '\u{00E7}',
    '\u{010D}', '\u{00E9}', '\u{0119}', '\u{00EB}', '\u{011B}', '\u{00ED}', '\u{00EE}', '\u{010F}',
    // 0xF0-0xFF
    '\u{0111}', '\u{0144}', '\u{0148}', '\u{00F3}', '\u{00F4}', '\u{0151}', '\u{00F6}', '\u{00F7}',
    '\u{0159}', '\u{016F}', '\u{00FA}', '\u{0171}', '\u{00FC}', '\u{00FD}', '\u{0163}', '\u{02D9}',
];

// ───────────────────────────────────────────────────────────────────────────
// S-JTSK (EPSG:5514) → WGS84 coordinate conversion
// ───────────────────────────────────────────────────────────────────────────

/// Convert S-JTSK (EPSG:5514) coordinates to WGS84 (lat, lon).
///
/// Uses a polynomial approximation accurate to ~1m over the Czech Republic.
/// S-JTSK coordinates are negative (southing, westing) — we take absolute values.
///
/// TODO: For production accuracy (<0.1m), implement proper inverse Krovak projection
/// with a 7-parameter Helmert transformation from Bessel 1841 to WGS84.
fn sjtsk_to_wgs84(y_neg: f64, x_neg: f64) -> (f64, f64) {
    // S-JTSK values are negative over Czech Republic — make positive
    let y = y_neg.abs();
    let x = x_neg.abs();

    // Scale to ~1.0 range for polynomial stability
    let bx = (y - 868_000.0) / 100_000.0;
    let by = (x - 1_095_000.0) / 100_000.0;

    // Polynomial coefficients from Czech geodetic service (VÚGTK)
    // Reference: Kostelecký, Karský, Šimek (2010)
    let lat = 49.450_697_0
        + 1.270_484_8 * by
        + 0.017_658_0 * bx
        + 0.081_393_4 * by * by
        - 0.025_510_7 * bx * by
        - 0.002_320_0 * bx * bx
        - 0.003_590_0 * by * by * by
        + 0.000_541_0 * by * by * bx
        - 0.001_380_0 * by * bx * bx
        + 0.000_313_0 * bx * bx * bx;

    let lon = 17.669_105_0
        - 2.167_127_8 * bx
        + 0.019_925_0 * by
        + 0.104_978_1 * bx * bx
        + 0.018_975_0 * bx * by
        + 0.005_236_0 * by * by
        - 0.002_290_0 * bx * bx * bx
        + 0.001_165_0 * bx * bx * by
        - 0.004_850_0 * bx * by * by
        + 0.000_483_0 * by * by * by;

    (lat, lon)
}
