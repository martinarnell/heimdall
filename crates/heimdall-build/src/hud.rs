/// hud.rs — US ZIP → city/county crosswalk enrichment
///
/// TIGER's nearest-centroid place assignment silently misattributes urban
/// ZIPs (10001 → Hoboken NJ instead of NYC, 02134 → Cambridge MA instead
/// of Boston, 94102 → Daly City CA instead of SF). The right fix is an
/// address-count-weighted crosswalk: pick the city/county where the most
/// mailable addresses live.
///
/// Source choice: HUD's USPS ZIP-county crosswalk is the canonical
/// dataset, but the public API requires email registration and the XLSX
/// downloads live behind a `/portal/datasets/usps_crosswalk.html` page
/// whose URL pattern shifts each quarter. SimpleMaps' US ZIPs CSV
/// (CC BY 4.0) is functionally equivalent for the "what city does this
/// ZIP belong to" question — derived from USPS + Census + OSM, no auth,
/// stable URL, ~2 MB ZIP. Defaulted here.
///
/// Source: https://simplemaps.com/data/us-zips
/// License: CC BY 4.0
/// Update cadence: ~quarterly
///
/// Inside the ZIP: `uszips.csv`, comma-delimited, header row, fields
/// (per the v1.91 schema):
///   zip,lat,lng,city,state_id,state_name,zcta,parent_zcta,population,
///   density,county_fips,county_name,county_weights,county_names_all,
///   county_fips_all,imprecise,military,timezone

use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use heimdall_core::zip_index::ZipRecord;
use tracing::{info, warn};

/// One row of the simplemaps US ZIPs CSV.
#[allow(dead_code)]
struct UsZipRow {
    zip: String,
    city: String,
    state_id: String,
    county_name: String,
    county_fips: String,
}

/// Read the simplemaps `uszips.csv` (or HUD CSV with the same key columns).
/// Returns a map ZIP → row. Ignores rows lacking a 5-digit ZIP or city.
fn read_simplemaps_csv(csv_path: &Path) -> Result<HashMap<String, UsZipRow>> {
    let text = std::fs::read_to_string(csv_path)
        .with_context(|| format!("read {}", csv_path.display()))?;

    let mut lines = parse_csv(&text);
    let header = match lines.next() {
        Some(h) => h,
        None => bail!("empty CSV: {}", csv_path.display()),
    };

    let find_col = |name: &str| {
        header
            .iter()
            .position(|c| c.trim().eq_ignore_ascii_case(name))
    };

    let zip_idx = find_col("zip").context("CSV missing 'zip' column")?;
    let city_idx = find_col("city").context("CSV missing 'city' column")?;
    let state_idx = find_col("state_id").context("CSV missing 'state_id' column")?;
    // county_name + county_fips are optional — the HUD variant uses
    // different headers, so we degrade gracefully when absent.
    let county_name_idx = find_col("county_name");
    let county_fips_idx = find_col("county_fips");

    let mut out: HashMap<String, UsZipRow> = HashMap::new();

    for fields in lines {
        let get = |i: usize| fields.get(i).map(|s| s.trim()).unwrap_or("");
        let zip = get(zip_idx);
        // Some rows store ZIPs as integers — left-pad to 5 digits.
        let zip = if zip.len() < 5 && !zip.is_empty() {
            format!("{:0>5}", zip)
        } else {
            zip.to_owned()
        };
        if zip.len() != 5 {
            continue;
        }

        let city = get(city_idx).to_owned();
        let state_id = get(state_idx).to_owned();
        if city.is_empty() {
            continue;
        }

        let county_name = county_name_idx
            .map(|i| get(i).to_owned())
            .unwrap_or_default();
        let county_fips = county_fips_idx
            .map(|i| get(i).to_owned())
            .unwrap_or_default();

        out.insert(
            zip.clone(),
            UsZipRow {
                zip,
                city,
                state_id,
                county_name,
                county_fips,
            },
        );
    }

    Ok(out)
}

/// Extract the first .csv from a simplemaps ZIP archive next to the ZIP.
/// Kept public so a future CLI subcommand or alternative driver can
/// consume a pre-downloaded ZIP without going through `tiger.rs`.
#[allow(dead_code)]
pub fn extract_csv_from_zip(zip_path: &Path) -> Result<std::path::PathBuf> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("open {}", zip_path.display()))?;
    let mut archive = zip::ZipArchive::new(file)?;

    let out_dir = zip_path.parent().unwrap_or(Path::new("."));

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if name.to_lowercase().ends_with(".csv") {
            let basename = Path::new(&name)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("uszips.csv");
            let out_path = out_dir.join(basename);
            let mut out = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut out)?;
            info!("  Extracted {} from ZIP", out_path.display());
            return Ok(out_path);
        }
    }

    bail!("no .csv file found in ZIP: {}", zip_path.display())
}

/// Override TIGER's nearest-centroid city assignment with the simplemaps
/// crosswalk where available. Conservative: only override when the
/// crosswalk's primary city differs from TIGER's, OR when TIGER left the
/// city blank. Returns the count of records updated.
///
/// `zip_records` is a flat Vec mirroring the (zip, lat, lon, city, state,
/// county) tuples that TIGER builds before writing the FST.
pub fn enrich_zip_records_with_crosswalk(
    zip_records: &mut [(String, i32, i32, String, String, String)],
    csv_path: &Path,
) -> Result<usize> {
    let crosswalk = read_simplemaps_csv(csv_path)?;
    info!("HUD/simplemaps crosswalk: {} ZIP rows loaded", crosswalk.len());

    let mut updated = 0usize;
    for (zip, _, _, city, state, county) in zip_records.iter_mut() {
        let row = match crosswalk.get(zip) {
            Some(r) => r,
            None => continue,
        };

        let city_changed = !row.city.is_empty()
            && (city.is_empty() || !city.eq_ignore_ascii_case(&row.city));
        let state_changed = !row.state_id.is_empty()
            && (state.is_empty() || !state.eq_ignore_ascii_case(&row.state_id));
        let county_changed = !row.county_name.is_empty()
            && (county.is_empty() || !county.eq_ignore_ascii_case(&row.county_name));

        if city_changed {
            *city = row.city.clone();
        }
        if state_changed {
            *state = row.state_id.clone();
        }
        if county_changed {
            *county = row.county_name.clone();
        }
        let _ = &row.county_fips;
        if city_changed || state_changed || county_changed {
            updated += 1;
        }
    }

    info!(
        "HUD/simplemaps enrichment: updated {} of {} ZIP records",
        updated,
        zip_records.len()
    );
    Ok(updated)
}

/// Best-effort enrichment: warn and continue if the crosswalk file is
/// missing or unparseable. Lets the build complete with TIGER-only ZIPs.
pub fn enrich_zip_records_lossy(
    zip_records: &mut [(String, i32, i32, String, String, String)],
    csv_path: &Path,
) -> usize {
    match enrich_zip_records_with_crosswalk(zip_records, csv_path) {
        Ok(n) => n,
        Err(e) => {
            warn!(
                "HUD/simplemaps enrichment failed ({}); keeping TIGER-only assignments",
                e
            );
            0
        }
    }
}

/// Suppresses dead-code warnings for `ZipRecord` import paths the public
/// API may want later (per-record enrichment from a fully-loaded index).
#[allow(dead_code)]
fn _zip_record_type_anchor(_z: &ZipRecord) {}

// ---------------------------------------------------------------------------
// Tiny RFC-4180 CSV parser (mirrors oa.rs — kept inline to avoid a new dep)
// ---------------------------------------------------------------------------

fn parse_csv(text: &str) -> impl Iterator<Item = Vec<String>> + '_ {
    struct Parser<'a> {
        chars: std::str::Chars<'a>,
        done: bool,
    }
    impl<'a> Iterator for Parser<'a> {
        type Item = Vec<String>;
        fn next(&mut self) -> Option<Vec<String>> {
            if self.done {
                return None;
            }
            let mut fields: Vec<String> = Vec::new();
            let mut cur = String::new();
            let mut in_quotes = false;
            let mut produced_any = false;
            loop {
                let c = match self.chars.next() {
                    Some(c) => c,
                    None => {
                        self.done = true;
                        if !produced_any && cur.is_empty() && fields.is_empty() {
                            return None;
                        }
                        fields.push(std::mem::take(&mut cur));
                        return Some(fields);
                    }
                };
                produced_any = true;
                if in_quotes {
                    if c == '"' {
                        let mut peek = self.chars.clone();
                        if peek.next() == Some('"') {
                            cur.push('"');
                            self.chars.next();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        cur.push(c);
                    }
                } else {
                    match c {
                        '"' if cur.is_empty() => in_quotes = true,
                        ',' => fields.push(std::mem::take(&mut cur)),
                        '\r' => {}
                        '\n' => {
                            fields.push(std::mem::take(&mut cur));
                            return Some(fields);
                        }
                        _ => cur.push(c),
                    }
                }
            }
        }
    }
    Parser {
        chars: text.chars(),
        done: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrichment_overrides_blank_city() {
        let mut records = vec![(
            "10001".to_string(),
            40_750_000_i32,
            -73_990_000_i32,
            String::new(),
            String::new(),
            String::new(),
        )];
        // Synthesize a 1-row crosswalk and call directly.
        let tmp = tempfile_path("hud_test.csv");
        std::fs::write(
            &tmp,
            "zip,city,state_id,county_name,county_fips\n10001,New York,NY,New York,36061\n",
        )
        .unwrap();
        let n = enrich_zip_records_with_crosswalk(&mut records, &tmp).unwrap();
        assert_eq!(n, 1);
        assert_eq!(records[0].3, "New York");
        assert_eq!(records[0].4, "NY");
        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn enrichment_overrides_wrong_city() {
        let mut records = vec![(
            "10001".to_string(),
            0,
            0,
            "Hoboken".to_string(),
            "NJ".to_string(),
            "Hudson".to_string(),
        )];
        let tmp = tempfile_path("hud_test2.csv");
        std::fs::write(
            &tmp,
            "zip,city,state_id,county_name,county_fips\n10001,New York,NY,New York,36061\n",
        )
        .unwrap();
        let n = enrich_zip_records_with_crosswalk(&mut records, &tmp).unwrap();
        assert_eq!(n, 1);
        assert_eq!(records[0].3, "New York");
        std::fs::remove_file(tmp).ok();
    }

    #[test]
    fn missing_zip_is_no_op() {
        let mut records = vec![(
            "99999".to_string(),
            0,
            0,
            "Somewhere".to_string(),
            "AK".to_string(),
            String::new(),
        )];
        let tmp = tempfile_path("hud_test3.csv");
        std::fs::write(
            &tmp,
            "zip,city,state_id,county_name,county_fips\n10001,New York,NY,New York,36061\n",
        )
        .unwrap();
        let n = enrich_zip_records_with_crosswalk(&mut records, &tmp).unwrap();
        assert_eq!(n, 0);
        assert_eq!(records[0].3, "Somewhere");
        std::fs::remove_file(tmp).ok();
    }

    fn tempfile_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("heimdall_{}_{}", std::process::id(), name))
    }
}
