/// tiger.rs — TIGER/Line 2025 shapefile import
///
/// Downloads and processes US Census Bureau TIGER/Line shapefiles:
///   - STATE: 50 states + DC + territories
///   - COUNTY: 3,143 counties
///   - PLACE: incorporated places (per state, by FIPS code)
///   - ZCTA520: ZIP Code Tabulation Areas
///
/// Outputs:
///   - fst_zip.fst + zip_records.bin → ZIP code lookup
///   - states.json → state FIPS → { name, abbreviation, bbox }
///   - admin.bin → admin hierarchy for PIP assignment (states + counties)
///   - admin_map.bin → FIPS → (state_id, county_id) mapping

use std::path::{Path, PathBuf};
use std::io::Cursor;
use std::collections::HashMap;
use anyhow::{bail, Context, Result};
use tracing::info;

use heimdall_core::zip_index::ZipIndexBuilder;

// ---------------------------------------------------------------------------
// US state FIPS codes → abbreviation + name
// ---------------------------------------------------------------------------

pub fn us_states() -> Vec<(u8, &'static str, &'static str)> {
    // (FIPS code, abbreviation, name)
    vec![
        (1, "AL", "Alabama"), (2, "AK", "Alaska"), (4, "AZ", "Arizona"),
        (5, "AR", "Arkansas"), (6, "CA", "California"), (8, "CO", "Colorado"),
        (9, "CT", "Connecticut"), (10, "DE", "Delaware"), (11, "DC", "District of Columbia"),
        (12, "FL", "Florida"), (13, "GA", "Georgia"), (15, "HI", "Hawaii"),
        (16, "ID", "Idaho"), (17, "IL", "Illinois"), (18, "IN", "Indiana"),
        (19, "IA", "Iowa"), (20, "KS", "Kansas"), (21, "KY", "Kentucky"),
        (22, "LA", "Louisiana"), (23, "ME", "Maine"), (24, "MD", "Maryland"),
        (25, "MA", "Massachusetts"), (26, "MI", "Michigan"), (27, "MN", "Minnesota"),
        (28, "MS", "Mississippi"), (29, "MO", "Missouri"), (30, "MT", "Montana"),
        (31, "NE", "Nebraska"), (32, "NV", "Nevada"), (33, "NH", "New Hampshire"),
        (34, "NJ", "New Jersey"), (35, "NM", "New Mexico"), (36, "NY", "New York"),
        (37, "NC", "North Carolina"), (38, "ND", "North Dakota"), (39, "OH", "Ohio"),
        (40, "OK", "Oklahoma"), (41, "OR", "Oregon"), (42, "PA", "Pennsylvania"),
        (44, "RI", "Rhode Island"), (45, "SC", "South Carolina"), (46, "SD", "South Dakota"),
        (47, "TN", "Tennessee"), (48, "TX", "Texas"), (49, "UT", "Utah"),
        (50, "VT", "Vermont"), (51, "VA", "Virginia"), (53, "WA", "Washington"),
        (54, "WV", "West Virginia"), (55, "WI", "Wisconsin"), (56, "WY", "Wyoming"),
        // Territories
        (60, "AS", "American Samoa"), (66, "GU", "Guam"),
        (69, "MP", "Northern Mariana Islands"), (72, "PR", "Puerto Rico"),
        (78, "VI", "U.S. Virgin Islands"),
    ]
}

/// Build a map from state abbreviation → state name
pub fn state_abbrev_to_name() -> HashMap<String, String> {
    us_states().into_iter()
        .map(|(_, abbr, name)| (abbr.to_owned(), name.to_owned()))
        .collect()
}

/// Build a map from state FIPS (2-digit string) → (abbreviation, name)
pub fn fips_to_state() -> HashMap<String, (String, String)> {
    us_states().into_iter()
        .map(|(fips, abbr, name)| (format!("{:02}", fips), (abbr.to_owned(), name.to_owned())))
        .collect()
}

// ---------------------------------------------------------------------------
// Download helpers
// ---------------------------------------------------------------------------

/// Download a file (with progress logging) and return the bytes.
async fn download_bytes(client: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    info!("  Downloading {}...", url);
    let resp = client.get(url).send().await?;

    if !resp.status().is_success() {
        bail!("HTTP {} for {}", resp.status(), url);
    }

    let bytes = resp.bytes().await?;
    info!("  Downloaded {:.1} MB", bytes.len() as f64 / 1e6);
    Ok(bytes.to_vec())
}

/// Download a ZIP file, extract the .shp and .dbf into a temp directory, return the .shp path.
async fn download_and_extract_shapefile(
    client: &reqwest::Client,
    url: &str,
    work_dir: &Path,
    name: &str,
) -> Result<PathBuf> {
    let zip_bytes = download_bytes(client, url).await?;
    let dest_dir = work_dir.join(name);
    std::fs::create_dir_all(&dest_dir)?;

    let reader = Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader)
        .context("Failed to open TIGER ZIP")?;

    let mut shp_path = None;
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = dest_dir.join(file.name());

        if file.is_dir() {
            std::fs::create_dir_all(&outpath)?;
            continue;
        }

        if let Some(parent) = outpath.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut outfile = std::fs::File::create(&outpath)?;
        std::io::copy(&mut file, &mut outfile)?;

        if outpath.extension().map(|e| e == "shp").unwrap_or(false) {
            shp_path = Some(outpath);
        }
    }

    shp_path.ok_or_else(|| anyhow::anyhow!("No .shp file found in {}", url))
}

// ---------------------------------------------------------------------------
// ZCTA (ZIP) processing
// ---------------------------------------------------------------------------

/// Represents a parsed ZCTA record from TIGER.
struct ZctaRecord {
    zcta5: String,    // 5-digit ZIP
    state_fips: String, // 2-digit state FIPS (from STATEFP20 / GEOID20 prefix)
    lat: f64,         // centroid latitude
    lon: f64,         // centroid longitude
}

/// Read ZCTA shapefile and extract ZIP centroids.
/// Carries STATEFP20 (the 2-digit FIPS prefix of GEOID20) so we can pin
/// each ZCTA to its real state without a Euclidean distance scan that
/// flips Hawaii / Alaska / PR onto the nearest mainland centroid.
fn read_zcta_shapefile(shp_path: &Path) -> Result<Vec<ZctaRecord>> {
    let mut reader = shapefile::Reader::from_path(shp_path)
        .context("Failed to open ZCTA shapefile")?;

    let mut records = Vec::new();

    for result in reader.iter_shapes_and_records() {
        let (shape, record) = result.context("Failed to read ZCTA record")?;

        // Get the ZCTA5CE20 or ZCTA5CE field (ZIP code)
        let zcta5 = record.get("ZCTA5CE20")
            .or_else(|| record.get("ZCTA5CE"))
            .and_then(|v| match v {
                shapefile::dbase::FieldValue::Character(Some(s)) => Some(s.trim().to_owned()),
                _ => None,
            });

        let zcta5 = match zcta5 {
            Some(z) if z.len() == 5 => z,
            _ => continue,
        };

        // STATEFP20 is the 2-digit state FIPS prefix of GEOID20. Some older
        // schemas omit it — derive from GEOID20 as a fallback so we still get
        // a reliable state assignment.
        let state_fips = get_string_field(&record, "STATEFP20")
            .or_else(|| get_string_field(&record, "STATEFP"))
            .or_else(|| {
                get_string_field(&record, "GEOID20")
                    .or_else(|| get_string_field(&record, "GEOID"))
                    .and_then(|g| if g.len() >= 2 { Some(g[..2].to_owned()) } else { None })
            })
            .unwrap_or_default();

        // Get centroid from INTPTLAT/INTPTLON fields (string lat/lon)
        let lat = record.get("INTPTLAT")
            .or_else(|| record.get("INTPTLAT20"))
            .and_then(|v| match v {
                shapefile::dbase::FieldValue::Character(Some(s)) => s.trim().parse::<f64>().ok(),
                shapefile::dbase::FieldValue::Float(Some(f)) => Some(*f as f64),
                shapefile::dbase::FieldValue::Numeric(Some(f)) => Some(*f),
                _ => None,
            });

        let lon = record.get("INTPTLON")
            .or_else(|| record.get("INTPTLON20"))
            .and_then(|v| match v {
                shapefile::dbase::FieldValue::Character(Some(s)) => s.trim().parse::<f64>().ok(),
                shapefile::dbase::FieldValue::Float(Some(f)) => Some(*f as f64),
                shapefile::dbase::FieldValue::Numeric(Some(f)) => Some(*f),
                _ => None,
            });

        // If no centroid in attributes, compute from shape geometry (all rings).
        let (lat, lon) = match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => match compute_centroid(&shape) {
                Some(c) => c,
                None => continue,
            },
        };

        records.push(ZctaRecord { zcta5, state_fips, lat, lon });
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// County shapefile processing
// ---------------------------------------------------------------------------

/// Parsed county record with FIPS and centroid.
pub struct CountyRecord {
    pub state_fips: String,
    pub county_fips: String,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    /// Backfilled from the Census Gazetteer (county POP column). 0 when
    /// the Gazetteer download fails or the county is missing from it.
    pub population: u32,
}

fn read_county_shapefile(shp_path: &Path) -> Result<Vec<CountyRecord>> {
    let mut reader = shapefile::Reader::from_path(shp_path)
        .context("Failed to open county shapefile")?;

    let mut records = Vec::new();

    for result in reader.iter_shapes_and_records() {
        let (shape, record) = result.context("Failed to read county record")?;

        let state_fips = get_string_field(&record, "STATEFP")
            .or_else(|| get_string_field(&record, "STATEFP20"))
            .unwrap_or_default();
        let county_fips = get_string_field(&record, "COUNTYFP")
            .or_else(|| get_string_field(&record, "COUNTYFP20"))
            .unwrap_or_default();
        let name = get_string_field(&record, "NAME")
            .or_else(|| get_string_field(&record, "NAMELSAD"))
            .unwrap_or_default();

        let lat = get_numeric_or_string_field(&record, "INTPTLAT")
            .or_else(|| get_numeric_or_string_field(&record, "INTPTLAT20"));
        let lon = get_numeric_or_string_field(&record, "INTPTLON")
            .or_else(|| get_numeric_or_string_field(&record, "INTPTLON20"));

        let (lat, lon) = match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => compute_centroid(&shape).unwrap_or((0.0, 0.0)),
        };

        if state_fips.is_empty() || county_fips.is_empty() { continue; }

        records.push(CountyRecord {
            state_fips, county_fips, name, lat, lon,
            population: 0, // backfilled from Gazetteer in run_tiger_import
        });
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// State shapefile processing
// ---------------------------------------------------------------------------

pub struct StateRecord {
    pub fips: String,
    pub name: String,
    pub abbreviation: String,
    pub lat: f64,
    pub lon: f64,
    pub bbox: (f64, f64, f64, f64), // min_lat, min_lon, max_lat, max_lon
}

fn read_state_shapefile(shp_path: &Path) -> Result<Vec<StateRecord>> {
    let mut reader = shapefile::Reader::from_path(shp_path)
        .context("Failed to open state shapefile")?;

    let fips_map = fips_to_state();
    let mut records = Vec::new();

    for result in reader.iter_shapes_and_records() {
        let (shape, record) = result.context("Failed to read state record")?;

        let fips = get_string_field(&record, "STATEFP")
            .or_else(|| get_string_field(&record, "STATEFP20"))
            .unwrap_or_default();
        let name = get_string_field(&record, "NAME").unwrap_or_default();

        let abbreviation = fips_map.get(&fips)
            .map(|(abbr, _)| abbr.clone())
            .unwrap_or_default();

        let lat = get_numeric_or_string_field(&record, "INTPTLAT")
            .or_else(|| get_numeric_or_string_field(&record, "INTPTLAT20"));
        let lon = get_numeric_or_string_field(&record, "INTPTLON")
            .or_else(|| get_numeric_or_string_field(&record, "INTPTLON20"));

        let (lat, lon) = match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => compute_centroid(&shape).unwrap_or((0.0, 0.0)),
        };

        // Compute bbox from geometry
        let bbox = compute_bbox(&shape).unwrap_or((0.0, 0.0, 0.0, 0.0));

        if fips.is_empty() { continue; }

        records.push(StateRecord {
            fips, name, abbreviation, lat, lon, bbox,
        });
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// Place shapefile processing
// ---------------------------------------------------------------------------

pub struct PlaceRecord {
    pub state_fips: String,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

fn read_place_shapefile(shp_path: &Path) -> Result<Vec<PlaceRecord>> {
    let mut reader = shapefile::Reader::from_path(shp_path)
        .context("Failed to open place shapefile")?;

    let mut records = Vec::new();

    for result in reader.iter_shapes_and_records() {
        let (shape, record) = result.context("Failed to read place record")?;

        let state_fips = get_string_field(&record, "STATEFP")
            .or_else(|| get_string_field(&record, "STATEFP20"))
            .unwrap_or_default();
        let name = get_string_field(&record, "NAME").unwrap_or_default();

        let lat = get_numeric_or_string_field(&record, "INTPTLAT")
            .or_else(|| get_numeric_or_string_field(&record, "INTPTLAT20"));
        let lon = get_numeric_or_string_field(&record, "INTPTLON")
            .or_else(|| get_numeric_or_string_field(&record, "INTPTLON20"));

        let (lat, lon) = match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => compute_centroid(&shape).unwrap_or((0.0, 0.0)),
        };

        if state_fips.is_empty() || name.is_empty() { continue; }

        records.push(PlaceRecord {
            state_fips, name, lat, lon,
        });
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// COUSUB (county subdivisions) shapefile processing
// ---------------------------------------------------------------------------

/// Town / township record. Only meaningful in "strong-MCD" states where
/// county subdivisions are the primary unit of local government (New
/// England, MI, MN, NJ, NY, OH, PA, RI, VT, WI). See
/// `is_strong_mcd_state` for the filter.
#[allow(dead_code)]
pub struct CousubRecord {
    pub state_fips: String,
    pub geoid: String,      // 10-digit STATE+COUNTY+COUSUB FIPS
    pub name: String,
    pub name_lsad: String,  // includes LSAD suffix ("Brookline town")
    pub lat: f64,
    pub lon: f64,
}

fn read_cousub_shapefile(shp_path: &Path, state_fips_hint: &str) -> Result<Vec<CousubRecord>> {
    let mut reader = shapefile::Reader::from_path(shp_path)
        .context("Failed to open cousub shapefile")?;

    let mut records = Vec::new();

    for result in reader.iter_shapes_and_records() {
        let (shape, record) = result.context("Failed to read cousub record")?;

        let state_fips = get_string_field(&record, "STATEFP")
            .or_else(|| get_string_field(&record, "STATEFP20"))
            .unwrap_or_else(|| state_fips_hint.to_string());
        let geoid = get_string_field(&record, "GEOID")
            .or_else(|| get_string_field(&record, "GEOID20"))
            .unwrap_or_default();
        let name = get_string_field(&record, "NAME").unwrap_or_default();
        let name_lsad = get_string_field(&record, "NAMELSAD").unwrap_or_default();

        let lat = get_numeric_or_string_field(&record, "INTPTLAT")
            .or_else(|| get_numeric_or_string_field(&record, "INTPTLAT20"));
        let lon = get_numeric_or_string_field(&record, "INTPTLON")
            .or_else(|| get_numeric_or_string_field(&record, "INTPTLON20"));

        let (lat, lon) = match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => compute_centroid(&shape).unwrap_or((0.0, 0.0)),
        };

        if name.is_empty() { continue; }

        records.push(CousubRecord {
            state_fips, geoid, name, name_lsad, lat, lon,
        });
    }

    Ok(records)
}

/// State FIPS codes where MCDs (county subdivisions) are the primary
/// local-government unit. Per Census `MCD-MAP` plus the MW township
/// states. Outside this set, COUSUB records duplicate the PLACE
/// shapefile and only inflate the admin table.
const STRONG_MCD_STATE_FIPS: &[&str] = &[
    "09", // CT
    "17", // IL
    "18", // IN
    "19", // IA
    "20", // KS
    "23", // ME
    "25", // MA
    "26", // MI
    "27", // MN
    "29", // MO
    "31", // NE
    "33", // NH
    "34", // NJ
    "36", // NY
    "38", // ND
    "39", // OH
    "42", // PA
    "44", // RI
    "46", // SD
    "50", // VT
    "55", // WI
];

fn is_strong_mcd_state(fips: &str) -> bool {
    STRONG_MCD_STATE_FIPS.contains(&fips)
}

// ---------------------------------------------------------------------------
// AIANNH (American Indian / Alaska Native / Native Hawaiian areas)
// ---------------------------------------------------------------------------

/// Federally-recognised tribal area. Single national shapefile, no per-
/// state breakdown — Navajo Nation alone crosses AZ / NM / UT.
#[allow(dead_code)]
pub struct AiannhRecord {
    pub aiannhce: String,   // 4-digit Census code
    pub geoid: String,
    pub name: String,
    pub name_lsad: String,
    pub lsad: String,
    pub classfp: String,
    pub lat: f64,
    pub lon: f64,
}

fn read_aiannh_shapefile(shp_path: &Path) -> Result<Vec<AiannhRecord>> {
    let mut reader = shapefile::Reader::from_path(shp_path)
        .context("Failed to open AIANNH shapefile")?;

    let mut records = Vec::new();

    for result in reader.iter_shapes_and_records() {
        let (shape, record) = result.context("Failed to read AIANNH record")?;

        let aiannhce = get_string_field(&record, "AIANNHCE").unwrap_or_default();
        let geoid = get_string_field(&record, "GEOID").unwrap_or_default();
        let name = get_string_field(&record, "NAME").unwrap_or_default();
        let name_lsad = get_string_field(&record, "NAMELSAD").unwrap_or_default();
        let lsad = get_string_field(&record, "LSAD").unwrap_or_default();
        let classfp = get_string_field(&record, "CLASSFP").unwrap_or_default();

        let lat = get_numeric_or_string_field(&record, "INTPTLAT");
        let lon = get_numeric_or_string_field(&record, "INTPTLON");
        let (lat, lon) = match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => compute_centroid(&shape).unwrap_or((0.0, 0.0)),
        };

        if name.is_empty() { continue; }

        records.push(AiannhRecord {
            aiannhce, geoid, name, name_lsad, lsad, classfp, lat, lon,
        });
    }

    Ok(records)
}

// ---------------------------------------------------------------------------
// Census Gazetteer 2024 — county populations
// ---------------------------------------------------------------------------

/// Download the 2024 Gazetteer counties ZIP, parse the embedded TSV,
/// return GEOID (5-digit STATE+COUNTY) → 2020 decennial population.
///
/// The Gazetteer is the only Census product that bundles county
/// population in a flat tabular file (Vintage estimates ship as
/// separate per-state CSVs that don't carry GEOID). 2020 counts are
/// the latest the Gazetteer publishes — Vintage 2024 estimates would
/// be more current but require a per-county API call.
async fn download_gazetteer_counties(
    client: &reqwest::Client,
    work_dir: &Path,
) -> Result<HashMap<String, u32>> {
    let url = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip";
    info!("  Downloading Gazetteer counties: {}", url);
    let bytes = download_bytes(client, url).await?;

    let dest_dir = work_dir.join("gazetteer_counties");
    std::fs::create_dir_all(&dest_dir)?;

    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .context("Failed to open Gazetteer ZIP")?;

    let mut tsv_path = None;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if name.to_lowercase().ends_with(".txt") {
            let out_path = dest_dir.join(
                Path::new(&name).file_name().unwrap_or(std::ffi::OsStr::new("counties.txt"))
            );
            let mut out = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut out)?;
            tsv_path = Some(out_path);
            break;
        }
    }

    let tsv_path = tsv_path
        .ok_or_else(|| anyhow::anyhow!("no .txt in Gazetteer counties ZIP"))?;
    let text = std::fs::read_to_string(&tsv_path)?;

    let mut out: HashMap<String, u32> = HashMap::new();
    let mut header_seen = false;
    let mut idx_geoid = 1usize;
    let mut idx_pop = 4usize;

    for line in text.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if !header_seen {
            for (i, h) in parts.iter().enumerate() {
                match h.trim().to_ascii_uppercase().as_str() {
                    "GEOID" => idx_geoid = i,
                    "POP" | "POP_2020" | "POP10" => idx_pop = i,
                    _ => {}
                }
            }
            header_seen = true;
            continue;
        }
        if parts.len() <= idx_pop { continue; }
        let geoid = parts[idx_geoid].trim();
        if geoid.len() != 5 { continue; }
        let pop: u32 = match parts[idx_pop].trim().parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        out.insert(geoid.to_owned(), pop);
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Shapefile field helpers
// ---------------------------------------------------------------------------

fn get_string_field(record: &shapefile::dbase::Record, name: &str) -> Option<String> {
    record.get(name).and_then(|v| match v {
        shapefile::dbase::FieldValue::Character(Some(s)) => {
            let s = s.trim();
            if s.is_empty() { None } else { Some(s.to_owned()) }
        }
        _ => None,
    })
}

fn get_numeric_or_string_field(record: &shapefile::dbase::Record, name: &str) -> Option<f64> {
    record.get(name).and_then(|v| match v {
        shapefile::dbase::FieldValue::Character(Some(s)) => s.trim().parse::<f64>().ok(),
        shapefile::dbase::FieldValue::Float(Some(f)) => Some(*f as f64),
        shapefile::dbase::FieldValue::Numeric(Some(f)) => Some(*f),
        _ => None,
    })
}

/// Average all vertices across every ring. The first-ring-only version
/// truncated multi-polygon geometries (Aleutians, Hawaiian island chain,
/// Florida Keys, Louisiana parishes) to whichever ring shapefile happened
/// to emit first. Prefer the TIGER-supplied INTPTLAT/INTPTLON in callers
/// — this is the geometry-only fallback.
fn compute_centroid(shape: &shapefile::Shape) -> Option<(f64, f64)> {
    match shape {
        shapefile::Shape::Polygon(poly) => {
            let mut sx = 0.0f64;
            let mut sy = 0.0f64;
            let mut n = 0usize;
            for ring in poly.rings() {
                let pts: &[shapefile::Point] = ring.as_ref();
                for p in pts {
                    sx += p.x;
                    sy += p.y;
                    n += 1;
                }
            }
            if n == 0 { return None; }
            let n = n as f64;
            Some((sy / n, sx / n))
        }
        _ => None,
    }
}

fn compute_bbox(shape: &shapefile::Shape) -> Option<(f64, f64, f64, f64)> {
    match shape {
        shapefile::Shape::Polygon(poly) => {
            let mut min_lat = f64::MAX;
            let mut min_lon = f64::MAX;
            let mut max_lat = f64::MIN;
            let mut max_lon = f64::MIN;
            let mut any = false;
            for ring in poly.rings() {
                let pts: &[shapefile::Point] = ring.as_ref();
                for p in pts {
                    min_lat = min_lat.min(p.y);
                    min_lon = min_lon.min(p.x);
                    max_lat = max_lat.max(p.y);
                    max_lon = max_lon.max(p.x);
                    any = true;
                }
            }
            if !any { return None; }
            Some((min_lat, min_lon, max_lat, max_lon))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// ZIP → city/state assignment using TIGER places + counties
// ---------------------------------------------------------------------------

/// Assign city and state names to ZCTAs based on nearest TIGER place and county.
fn assign_zip_admin(
    zctas: &[ZctaRecord],
    states: &[StateRecord],
    counties: &[CountyRecord],
    places: &[PlaceRecord],
) -> Vec<(String, i32, i32, String, String, String)> {
    // Build state FIPS → name+abbrev lookup
    let state_map: HashMap<&str, (&str, &str)> = states.iter()
        .map(|s| (s.fips.as_str(), (s.name.as_str(), s.abbreviation.as_str())))
        .collect();

    // Build county lookup: (state_fips, county_fips) → name
    let _county_map: HashMap<(&str, &str), &str> = counties.iter()
        .map(|c| ((c.state_fips.as_str(), c.county_fips.as_str()), c.name.as_str()))
        .collect();

    let mut results = Vec::with_capacity(zctas.len());

    for zcta in zctas {
        let lat_i32 = (zcta.lat * 1e6) as i32;
        let lon_i32 = (zcta.lon * 1e6) as i32;

        // STATEFP20 carries the authoritative state assignment — using
        // squared-degree distance flipped Hawaii / Alaska / PR onto the
        // nearest mainland state.
        let state_abbr = state_map.get(zcta.state_fips.as_str())
            .map(|(_, abbr)| *abbr)
            .unwrap_or("");

        // County still resolved by nearest centroid within the same state —
        // ZCTA→county is many-to-many and TIGER doesn't carry COUNTYFP on
        // the ZCTA shapefile.
        let nearest_county = counties.iter()
            .filter(|c| c.state_fips == zcta.state_fips || zcta.state_fips.is_empty())
            .min_by(|a, b| {
                let da = (a.lat - zcta.lat).powi(2) + (a.lon - zcta.lon).powi(2);
                let db = (b.lat - zcta.lat).powi(2) + (b.lon - zcta.lon).powi(2);
                da.partial_cmp(&db).unwrap()
            });

        // Find nearest incorporated place (within 50km-ish, using degree threshold)
        let nearest_place = places.iter()
            .filter(|p| {
                (p.state_fips == zcta.state_fips || zcta.state_fips.is_empty())
                    && (p.lat - zcta.lat).abs() < 0.5
                    && (p.lon - zcta.lon).abs() < 0.5
            })
            .min_by(|a, b| {
                let da = (a.lat - zcta.lat).powi(2) + (a.lon - zcta.lon).powi(2);
                let db = (b.lat - zcta.lat).powi(2) + (b.lon - zcta.lon).powi(2);
                da.partial_cmp(&db).unwrap()
            });

        let city = nearest_place
            .map(|p| p.name.as_str())
            .unwrap_or("");

        let county = nearest_county
            .map(|c| c.name.as_str())
            .unwrap_or("");

        results.push((
            zcta.zcta5.clone(),
            lat_i32, lon_i32,
            city.to_owned(),
            state_abbr.to_owned(),
            county.to_owned(),
        ));
    }

    results
}

// ---------------------------------------------------------------------------
// Admin hierarchy builder
// ---------------------------------------------------------------------------

/// Write admin.bin for US: states as admin1, counties + cousubs (in
/// strong-MCD states) + AIANNH areas as admin2.
fn write_admin_bin(
    states: &[StateRecord],
    counties: &[CountyRecord],
    cousubs: &[CousubRecord],
    aiannh: &[AiannhRecord],
    output_dir: &Path,
) -> Result<()> {
    use heimdall_core::types::AdminEntry;

    let mut entries = Vec::new();

    // States as admin1 (IDs 0..states.len())
    // Bare state name (no parenthetical abbreviation) — the API's
    // detected_state_admin1 lookup compares against `entry.name == state_name`,
    // which the "Texas (TX)" form misses. AdminEntry has no abbreviation field;
    // states.json carries it for the API to read separately.
    for (i, state) in states.iter().enumerate() {
        entries.push(AdminEntry {
            id: i as u16,
            name: state.name.clone(),
            parent_id: None,
            coord: heimdall_core::types::Coord {
                lat: (state.lat * 1e6) as i32,
                lon: (state.lon * 1e6) as i32,
            },
            place_type: heimdall_core::types::PlaceType::State,
            population: state_pop_lookup(&state.fips),
        });
    }

    // Build state FIPS → admin1 ID mapping
    let state_fips_to_id: HashMap<&str, u16> = states.iter()
        .enumerate()
        .map(|(i, s)| (s.fips.as_str(), i as u16))
        .collect();

    let state_count = states.len() as u16;

    // Counties as admin2 (IDs state_count..state_count+counties.len())
    for (i, county) in counties.iter().enumerate() {
        let parent = state_fips_to_id.get(county.state_fips.as_str())
            .copied()
            .unwrap_or(0);
        entries.push(AdminEntry {
            id: state_count + i as u16,
            name: county.name.clone(),
            parent_id: Some(parent),
            coord: heimdall_core::types::Coord {
                lat: (county.lat * 1e6) as i32,
                lon: (county.lon * 1e6) as i32,
            },
            place_type: heimdall_core::types::PlaceType::County,
            population: county.population,
        });
    }

    // County subdivisions (towns / townships) only matter as primary
    // local government in the strong-MCD states. Including them
    // elsewhere would duplicate PLACE entries and inflate the admin
    // table without disambiguation value. Indexed in admin2 alongside
    // counties — the runtime treats both as "second-level admin".
    let next_id = state_count + counties.len() as u16;
    let mut cousub_added = 0usize;
    for cousub in cousubs.iter() {
        if !is_strong_mcd_state(&cousub.state_fips) { continue; }
        let parent = state_fips_to_id.get(cousub.state_fips.as_str()).copied();
        entries.push(AdminEntry {
            id: next_id + cousub_added as u16,
            name: cousub.name.clone(),
            parent_id: parent,
            coord: heimdall_core::types::Coord {
                lat: (cousub.lat * 1e6) as i32,
                lon: (cousub.lon * 1e6) as i32,
            },
            place_type: heimdall_core::types::PlaceType::County,
            population: 0, // Gazetteer cousubs file is separate; skipped
        });
        cousub_added += 1;
    }

    // AIANNH (tribal areas). Multistate (Navajo crosses AZ/NM/UT) so
    // parent_id is left None — they sit at admin2 level but are not
    // children of a single state. The runtime walker stops climbing at
    // None which is what we want here.
    let next_id = next_id + cousub_added as u16;
    for (i, area) in aiannh.iter().enumerate() {
        entries.push(AdminEntry {
            id: next_id + i as u16,
            name: area.name.clone(),
            parent_id: None,
            coord: heimdall_core::types::Coord {
                lat: (area.lat * 1e6) as i32,
                lon: (area.lon * 1e6) as i32,
            },
            place_type: heimdall_core::types::PlaceType::County,
            population: 0,
        });
    }

    // Postcard matches enrich.rs's serializer — the runtime loader accepts
    // either, but staying consistent keeps `admin.bin` byte-identical
    // regardless of which step wrote it last.
    let data = postcard::to_allocvec(&entries).expect("postcard serialize TIGER admin");
    std::fs::write(output_dir.join("admin.bin"), &data)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// State populations (US Census Bureau 2024 vintage estimates)
// ---------------------------------------------------------------------------

/// 2024 Vintage state population estimates (US Census Bureau, July 2024).
/// Hardcoded — the set is closed, the values change at most once a year, and
/// pulling the Gazetteer ZIP at build time costs a download for ~600 bytes
/// of data. Update this table when the next vintage drops.
fn state_pop_lookup(fips: &str) -> u32 {
    match fips {
        "01" => 5_157_699,  "02" => 740_133,    "04" => 7_582_384,  "05" => 3_088_354,
        "06" => 39_431_263, "08" => 5_957_493,  "09" => 3_675_069,  "10" => 1_051_917,
        "11" => 702_250,    "12" => 23_372_215, "13" => 11_180_878, "15" => 1_446_146,
        "16" => 2_001_619,  "17" => 12_710_158, "18" => 6_924_275,  "19" => 3_241_488,
        "20" => 2_970_606,  "21" => 4_588_372,  "22" => 4_597_740,  "23" => 1_405_012,
        "24" => 6_263_220,  "25" => 7_136_171,  "26" => 10_140_459, "27" => 5_793_151,
        "28" => 2_943_045,  "29" => 6_245_466,  "30" => 1_137_233,  "31" => 2_005_465,
        "32" => 3_267_467,  "33" => 1_409_032,  "34" => 9_500_851,  "35" => 2_130_256,
        "36" => 19_867_248, "37" => 11_046_024, "38" => 796_568,    "39" => 11_883_304,
        "40" => 4_095_393,  "41" => 4_272_371,  "42" => 13_078_751, "44" => 1_112_308,
        "45" => 5_478_831,  "46" => 924_669,    "47" => 7_227_750,  "48" => 31_290_831,
        "49" => 3_503_613,  "50" => 648_493,    "51" => 8_811_195,  "53" => 7_958_180,
        "54" => 1_769_979,  "55" => 5_960_975,  "56" => 587_618,
        // Territories — Census Bureau 2020 decennial counts (Vintage estimates not produced)
        "60" => 49_710,     // American Samoa
        "66" => 153_836,    // Guam
        "69" => 47_329,     // Northern Mariana Islands
        "72" => 3_205_691,  // Puerto Rico
        "78" => 87_146,     // U.S. Virgin Islands
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the TIGER import: download shapefiles, parse, build ZIP FST + admin hierarchy.
pub fn run_tiger_import(output_dir: &Path) -> Result<TigerResult> {
    std::fs::create_dir_all(output_dir)?;

    let work_dir = output_dir.join("tiger_work");
    std::fs::create_dir_all(&work_dir)?;

    let rt = tokio::runtime::Runtime::new()?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()?;

    let base = "https://www2.census.gov/geo/tiger/TIGER2025";

    // Download shapefiles
    info!("=== TIGER Import: Downloading shapefiles ===");

    let state_shp = rt.block_on(download_and_extract_shapefile(
        &client,
        &format!("{}/STATE/tl_2025_us_state.zip", base),
        &work_dir, "state",
    ))?;
    info!("Downloaded state shapefile");

    let county_shp = rt.block_on(download_and_extract_shapefile(
        &client,
        &format!("{}/COUNTY/tl_2025_us_county.zip", base),
        &work_dir, "county",
    ))?;
    info!("Downloaded county shapefile");

    let zcta_shp = rt.block_on(download_and_extract_shapefile(
        &client,
        &format!("{}/ZCTA520/tl_2025_us_zcta520.zip", base),
        &work_dir, "zcta",
    ))?;
    info!("Downloaded ZCTA shapefile");

    // Download place shapefiles per state
    info!("Downloading place shapefiles for all states...");
    let state_fips_codes: Vec<String> = us_states().iter()
        .map(|(fips, _, _)| format!("{:02}", fips))
        .collect();

    let mut all_places = Vec::new();
    for fips in &state_fips_codes {
        let url = format!("{}/PLACE/tl_2025_{}_place.zip", base, fips);
        match rt.block_on(download_and_extract_shapefile(&client, &url, &work_dir, &format!("place_{}", fips))) {
            Ok(shp) => {
                match read_place_shapefile(&shp) {
                    Ok(places) => {
                        info!("  State {}: {} places", fips, places.len());
                        all_places.extend(places);
                    }
                    Err(e) => tracing::warn!("  Failed to read places for state {}: {}", fips, e),
                }
            }
            Err(e) => tracing::warn!("  Failed to download places for state {}: {}", fips, e),
        }
    }
    info!("Total incorporated places: {}", all_places.len());

    // ── Download COUSUB per state (small files, ~25 MB total) ─────────
    // Restricted to strong-MCD states up front — cuts ~30 unnecessary
    // downloads. The shapefile parser stays generic so a future patch
    // can re-enable everywhere by widening the const set.
    info!("Downloading county subdivision shapefiles (strong-MCD states)...");
    let mut all_cousubs: Vec<CousubRecord> = Vec::new();
    for fips in &state_fips_codes {
        if !is_strong_mcd_state(fips) { continue; }
        let url = format!("{}/COUSUB/tl_2025_{}_cousub.zip", base, fips);
        match rt.block_on(download_and_extract_shapefile(&client, &url, &work_dir, &format!("cousub_{}", fips))) {
            Ok(shp) => match read_cousub_shapefile(&shp, fips) {
                Ok(c) => {
                    info!("  State {}: {} cousubs", fips, c.len());
                    all_cousubs.extend(c);
                }
                Err(e) => tracing::warn!("  Failed to read cousubs for state {}: {}", fips, e),
            },
            Err(e) => tracing::warn!("  Failed to download cousubs for state {}: {}", fips, e),
        }
    }
    info!("Total cousubs (towns/townships): {}", all_cousubs.len());

    // ── Download AIANNH (national, single shapefile) ──────────────────
    let aiannh: Vec<AiannhRecord> = match rt.block_on(download_and_extract_shapefile(
        &client,
        &format!("{}/AIANNH/tl_2025_us_aiannh.zip", base),
        &work_dir, "aiannh",
    )) {
        Ok(shp) => match read_aiannh_shapefile(&shp) {
            Ok(a) => { info!("AIANNH (tribal areas): {}", a.len()); a }
            Err(e) => { tracing::warn!("Failed to read AIANNH: {}", e); Vec::new() }
        }
        Err(e) => { tracing::warn!("Failed to download AIANNH: {}", e); Vec::new() }
    };

    // ── Download Gazetteer counties for population backfill ───────────
    let gazetteer_pops = match rt.block_on(download_gazetteer_counties(&client, &work_dir)) {
        Ok(m) => { info!("Gazetteer populations: {} counties", m.len()); m }
        Err(e) => { tracing::warn!("Gazetteer download failed: {}", e); HashMap::new() }
    };

    // Parse shapefiles
    info!("=== Parsing shapefiles ===");

    let states = read_state_shapefile(&state_shp)?;
    info!("States: {}", states.len());

    let mut counties = read_county_shapefile(&county_shp)?;
    info!("Counties: {}", counties.len());

    // Backfill county populations from Gazetteer (5-digit GEOID).
    let mut county_pop_count = 0usize;
    for c in counties.iter_mut() {
        let geoid = format!("{}{}", c.state_fips, c.county_fips);
        if let Some(&pop) = gazetteer_pops.get(&geoid) {
            c.population = pop;
            county_pop_count += 1;
        }
    }
    info!("Backfilled population for {} counties from Gazetteer", county_pop_count);

    let zctas = read_zcta_shapefile(&zcta_shp)?;
    info!("ZCTAs (ZIP codes): {}", zctas.len());

    // Assign city/state/county to each ZIP
    info!("=== Assigning admin hierarchy to ZIP codes ===");
    let mut zip_records = assign_zip_admin(&zctas, &states, &counties, &all_places);
    info!("Assigned {} ZIP records", zip_records.len());

    // ── HUD/simplemaps crosswalk enrichment ───────────────────────────
    // Overrides the nearest-centroid city assignment with the
    // address-count-weighted USPS view. Best-effort: a missing or
    // malformed CSV downgrades silently and leaves TIGER assignments.
    info!("=== Enriching ZIP records with HUD/simplemaps crosswalk ===");
    let hud_updated_count = match rt.block_on(download_simplemaps_uszips(&client, &work_dir)) {
        Ok(csv_path) => crate::hud::enrich_zip_records_lossy(&mut zip_records, &csv_path),
        Err(e) => {
            tracing::warn!("simplemaps US ZIPs download failed: {}; keeping TIGER-only assignments", e);
            0
        }
    };

    // Build ZIP FST
    info!("=== Building ZIP index ===");
    let mut zip_builder = ZipIndexBuilder::new();
    for (zip, lat, lon, city, state, county) in &zip_records {
        zip_builder.add(zip, *lat, *lon, city, state, county);
    }
    zip_builder.write(output_dir)?;
    info!("Written fst_zip.fst + zip_records.bin ({} ZIPs)", zip_builder.len());

    // Write admin.bin
    info!("=== Writing admin hierarchy ===");
    write_admin_bin(&states, &counties, &all_cousubs, &aiannh, output_dir)?;
    info!(
        "Written admin.bin ({} states + {} counties + {} cousubs + {} AIANNH)",
        states.len(), counties.len(), all_cousubs.len(), aiannh.len(),
    );

    // Write states.json
    let states_json: serde_json::Value = states.iter()
        .map(|s| {
            (s.fips.clone(), serde_json::json!({
                "name": s.name,
                "abbreviation": s.abbreviation,
                "bbox": {
                    "min_lat": s.bbox.0,
                    "min_lon": s.bbox.1,
                    "max_lat": s.bbox.2,
                    "max_lon": s.bbox.3,
                }
            }))
        })
        .collect::<serde_json::Map<String, serde_json::Value>>()
        .into();

    std::fs::write(
        output_dir.join("states.json"),
        serde_json::to_string_pretty(&states_json)?,
    )?;
    info!("Written states.json");

    // Cleanup work directory
    std::fs::remove_dir_all(&work_dir).ok();
    info!("Cleaned up work directory");

    Ok(TigerResult {
        state_count: states.len(),
        county_count: counties.len(),
        place_count: all_places.len(),
        zip_count: zip_records.len(),
        cousub_count: all_cousubs.len(),
        aiannh_count: aiannh.len(),
        county_pop_count,
        hud_updated_count,
    })
}

// ---------------------------------------------------------------------------
// Simplemaps US ZIPs CSV download
// ---------------------------------------------------------------------------

/// Download the simplemaps US ZIPs ZIP, extract the CSV into work_dir.
/// URL pinned to v1.91 — the latest free basic build at time of writing.
/// CC BY 4.0; ~2 MB ZIP / ~5 MB CSV.
async fn download_simplemaps_uszips(
    client: &reqwest::Client,
    work_dir: &Path,
) -> Result<PathBuf> {
    let url = "https://simplemaps.com/static/data/us-zips/1.91/basic/simplemaps_uszips_basicv1.91.zip";
    info!("  Downloading simplemaps US ZIPs: {}", url);
    let bytes = download_bytes(client, url).await?;

    let dest_dir = work_dir.join("simplemaps_uszips");
    std::fs::create_dir_all(&dest_dir)?;

    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .context("Failed to open simplemaps ZIP")?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if name.to_lowercase().ends_with(".csv") {
            let basename = Path::new(&name)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("uszips.csv");
            let out_path = dest_dir.join(basename);
            let mut out = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut out)?;
            return Ok(out_path);
        }
    }
    bail!("no .csv in simplemaps ZIP")
}

#[allow(dead_code)]
pub struct TigerResult {
    pub state_count: usize,
    pub county_count: usize,
    pub place_count: usize,
    pub zip_count: usize,
    pub cousub_count: usize,
    pub aiannh_count: usize,
    pub county_pop_count: usize, // counties backfilled from Gazetteer
    pub hud_updated_count: usize, // ZIPs reassigned by HUD/simplemaps
}
