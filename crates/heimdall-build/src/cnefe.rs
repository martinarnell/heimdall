/// cnefe.rs — Parse Brazil CNEFE (Cadastro Nacional de Endereços para Fins Estatísticos)
///
/// CNEFE 2022 contains 107M addresses from the Brazilian Census.
/// Published by IBGE, available from FTP as state-level ZIP files.
///
/// Data format: state ZIP files containing per-municipality semicolon-delimited CSVs.
/// Each CSV row is a single address with street, house number, CEP, bairro, and coordinates.
///
/// Key quirks:
///   - Semicolon delimiter, not comma
///   - Decimal separator may be comma (Brazilian locale): "-23,5505" instead of "-23.5505"
///   - Encoding may be Windows-1252 or UTF-8
///   - "S/N" = sine numero (no house number) — valid address

use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use crate::extract::RawAddress;

/// Read CNEFE addresses from a directory containing state ZIP files or extracted CSVs.
/// Process state by state to bound memory usage.
pub fn read_cnefe_addresses(input_path: &Path) -> Result<Vec<RawAddress>> {
    info!("Reading CNEFE addresses from {}", input_path.display());

    if input_path.is_file() {
        // Single ZIP file
        let addresses = read_state_zip(input_path)?;
        info!("Parsed {} CNEFE addresses from single ZIP", addresses.len());
        return Ok(addresses);
    }

    // Directory: collect all .zip and .csv files
    let mut zip_files = Vec::new();
    let mut csv_files = Vec::new();

    for entry in std::fs::read_dir(input_path)
        .with_context(|| format!("reading directory {}", input_path.display()))?
    {
        let path = entry?.path();
        let fname = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();
        if fname.ends_with(".zip") {
            zip_files.push(path);
        } else if fname.ends_with(".csv") {
            csv_files.push(path);
        }
    }

    zip_files.sort();
    csv_files.sort();

    info!(
        "  Found {} ZIP files, {} CSV files",
        zip_files.len(),
        csv_files.len()
    );

    // Estimate capacity: ~4M addresses per state average
    let estimate = zip_files.len().max(1) * 4_000_000 + csv_files.len() * 100_000;
    let mut all_addresses = Vec::with_capacity(estimate.min(110_000_000));

    // Process ZIP files (one state at a time)
    for zip_path in &zip_files {
        let fname = zip_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        info!("  Processing state ZIP: {}", fname);

        match read_state_zip(zip_path) {
            Ok(addrs) => {
                info!("    {} — {} addresses", fname, addrs.len());
                all_addresses.extend(addrs);
            }
            Err(e) => {
                info!("    {} — ERROR: {}", fname, e);
            }
        }
    }

    // Process loose CSV files
    for csv_path in &csv_files {
        let fname = csv_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        match read_cnefe_csv_file(csv_path) {
            Ok(addrs) => {
                info!("    {} — {} addresses", fname, addrs.len());
                all_addresses.extend(addrs);
            }
            Err(e) => {
                info!("    {} — ERROR: {}", fname, e);
            }
        }
    }

    info!("Parsed {} CNEFE addresses total", all_addresses.len());
    Ok(all_addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// State ZIP processing — stream municipality CSVs from within the ZIP
// ───────────────────────────────────────────────────────────────────────────

fn read_state_zip(zip_path: &Path) -> Result<Vec<RawAddress>> {
    let file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut addresses = Vec::with_capacity(4_000_000);
    let mut skipped_files = 0usize;

    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        let full_name = entry.name().to_string();
        let filename = Path::new(&full_name)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_lowercase();

        if !filename.ends_with(".csv") {
            continue;
        }

        match read_cnefe_csv_reader(entry) {
            Ok(addrs) => {
                addresses.extend(addrs);
            }
            Err(_) => {
                skipped_files += 1;
            }
        }
    }

    if skipped_files > 0 {
        info!("    Skipped {} CSV files with errors", skipped_files);
    }

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// CSV parsing
// ───────────────────────────────────────────────────────────────────────────

/// Read a CNEFE CSV from a file path.
fn read_cnefe_csv_file(path: &Path) -> Result<Vec<RawAddress>> {
    let file = std::fs::File::open(path)?;
    read_cnefe_csv_reader(file)
}

/// Read a CNEFE CSV from any reader (file or ZIP entry).
fn read_cnefe_csv_reader<R: Read>(reader: R) -> Result<Vec<RawAddress>> {
    let buf_reader = BufReader::new(reader);
    let mut addresses = Vec::new();
    let mut _skipped = 0usize;
    let mut header_found = false;

    // Column indices (resolved from header)
    let mut i_tipo: Option<usize> = None;
    let mut i_titulo: Option<usize> = None;
    let mut i_logradouro: Option<usize> = None;
    let mut i_numero: Option<usize> = None;
    let mut i_modificador: Option<usize> = None;
    let mut i_cep: Option<usize> = None;
    let mut i_localidade: Option<usize> = None;
    let mut i_lat: Option<usize> = None;
    let mut i_lon: Option<usize> = None;

    for line_result in buf_reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => {
                _skipped += 1;
                continue;
            }
        };

        // Strip BOM
        let line = line.trim_start_matches('\u{feff}');
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(';').collect();

        // Detect header row
        if !header_found {
            if fields.iter().any(|f| {
                let upper = f.trim().to_uppercase();
                upper == "NOM_LOGRADOURO" || upper == "LATITUDE"
            }) {
                i_tipo = col_index(&fields, "NOM_TIPO_LOGRADOURO");
                i_titulo = col_index(&fields, "NOM_TITULO_LOGRADOURO");
                i_logradouro = col_index(&fields, "NOM_LOGRADOURO");
                i_numero = col_index(&fields, "NUM_ENDERECO");
                i_modificador = col_index(&fields, "DSC_MODIFICADOR");
                i_cep = col_index(&fields, "NUM_CEP");
                i_localidade = col_index(&fields, "NOM_LOCALIDADE");
                i_lat = col_index(&fields, "LATITUDE");
                i_lon = col_index(&fields, "LONGITUDE");
                header_found = true;
                continue;
            }
            // If first line doesn't look like header, try positional defaults
            // (CNEFE has a known schema)
            if fields.len() >= 22 {
                i_tipo = Some(1);
                i_titulo = Some(2);
                i_logradouro = Some(3);
                i_numero = Some(4);
                i_modificador = Some(5);
                i_cep = Some(16);
                i_localidade = Some(17);
                i_lat = Some(20);
                i_lon = Some(21);
                header_found = true;
                // Don't continue — this line is data, fall through to parse it
            } else {
                continue;
            }
        }

        // Need at least the logradouro and lat/lon columns
        let i_log = match i_logradouro {
            Some(i) => i,
            None => continue,
        };
        let i_la = match i_lat {
            Some(i) => i,
            None => continue,
        };
        let i_lo = match i_lon {
            Some(i) => i,
            None => continue,
        };

        if fields.len() <= i_log.max(i_la).max(i_lo) {
            _skipped += 1;
            continue;
        }

        // Parse coordinates — handle comma as decimal separator
        let lat_str = fields[i_la].trim().replace(',', ".");
        let lon_str = fields[i_lo].trim().replace(',', ".");

        let lat: f64 = match lat_str.parse() {
            Ok(v) => v,
            Err(_) => {
                _skipped += 1;
                continue;
            }
        };
        let lon: f64 = match lon_str.parse() {
            Ok(v) => v,
            Err(_) => {
                _skipped += 1;
                continue;
            }
        };

        // Skip rows with zero or out-of-bounds coordinates
        // Brazil bbox: -33.75 to 5.27 latitude, -73.99 to -34.79 longitude
        if lat == 0.0 || lon == 0.0 {
            _skipped += 1;
            continue;
        }
        if lat < -34.0 || lat > 6.0 || lon < -74.5 || lon > -34.0 {
            _skipped += 1;
            continue;
        }

        // Build street name: NOM_TIPO_LOGRADOURO + NOM_TITULO_LOGRADOURO + NOM_LOGRADOURO
        let tipo = i_tipo
            .filter(|&i| i < fields.len())
            .map(|i| fields[i].trim())
            .unwrap_or("");
        let titulo = i_titulo
            .filter(|&i| i < fields.len())
            .map(|i| fields[i].trim())
            .unwrap_or("");
        let logradouro = fields[i_log].trim();

        if logradouro.is_empty() {
            _skipped += 1;
            continue;
        }

        let mut street_parts: Vec<&str> = Vec::with_capacity(3);
        if !tipo.is_empty() {
            street_parts.push(tipo);
        }
        if !titulo.is_empty() {
            street_parts.push(titulo);
        }
        street_parts.push(logradouro);
        let street = title_case(&street_parts.join(" "));

        // House number
        let num_raw = i_numero
            .filter(|&i| i < fields.len())
            .map(|i| fields[i].trim())
            .unwrap_or("");

        let modificador = i_modificador
            .filter(|&i| i < fields.len())
            .map(|i| fields[i].trim())
            .unwrap_or("");

        let housenumber = build_housenumber(num_raw, modificador);
        if housenumber.is_empty() {
            _skipped += 1;
            continue;
        }

        // CEP (postcode) — 8 digits, store as-is
        let postcode = i_cep
            .filter(|&i| i < fields.len())
            .map(|i| fields[i].trim().to_owned())
            .filter(|s| !s.is_empty() && s.len() >= 5);

        // Localidade (bairro/neighborhood)
        let city = i_localidade
            .filter(|&i| i < fields.len())
            .map(|i| fields[i].trim())
            .filter(|s| !s.is_empty())
            .map(|s| title_case(s));

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

    Ok(addresses)
}

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────

/// Find column index by name in a semicolon-delimited header (case-insensitive).
fn col_index(header: &[&str], name: &str) -> Option<usize> {
    let upper = name.to_uppercase();
    header
        .iter()
        .position(|c| c.trim().trim_matches('"').to_uppercase() == upper)
}

/// Title-case a string: "RUA DAS FLORES" → "Rua Das Flores"
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

/// Build house number from NUM_ENDERECO and DSC_MODIFICADOR.
///
/// - "S/N" → "S/N" (sine numero)
/// - "123" + "" → "123"
/// - "123" + "A" → "123A"
/// - "123" + "FUNDOS" → "123 Fundos"
/// - "" + anything → ""
fn build_housenumber(num: &str, modificador: &str) -> String {
    if num.is_empty() {
        return String::new();
    }

    let num = num.trim();

    // S/N is a valid "numberless" address
    if num.eq_ignore_ascii_case("S/N") {
        return "S/N".to_string();
    }

    if modificador.is_empty() || modificador.eq_ignore_ascii_case("S/N") {
        return num.to_string();
    }

    // Single letter modifier: append directly (e.g., "123A")
    let mod_trimmed = modificador.trim();
    if mod_trimmed.len() == 1 && mod_trimmed.chars().next().map_or(false, |c| c.is_ascii_alphabetic()) {
        return format!("{}{}", num, mod_trimmed.to_uppercase());
    }

    // Multi-word modifier: separate with space (e.g., "123 Fundos")
    format!("{} {}", num, title_case(mod_trimmed))
}
