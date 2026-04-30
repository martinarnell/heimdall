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
    zcta5: String,   // 5-digit ZIP
    lat: f64,        // centroid latitude
    lon: f64,        // centroid longitude
}

/// Read ZCTA shapefile and extract ZIP centroids.
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

        // If no centroid in attributes, compute from shape geometry
        let (lat, lon) = match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => {
                // Compute centroid from polygon
                match &shape {
                    shapefile::Shape::Polygon(poly) => {
                        let ring: &[shapefile::Point] = poly.rings()[0].as_ref();
                        let n = ring.len() as f64;
                        if n == 0.0 { continue; }
                        let sum_x: f64 = ring.iter().map(|p| p.x).sum();
                        let sum_y: f64 = ring.iter().map(|p| p.y).sum();
                        (sum_y / n, sum_x / n)
                    }
                    _ => continue,
                }
            }
        };

        records.push(ZctaRecord { zcta5, lat, lon });
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

fn compute_centroid(shape: &shapefile::Shape) -> Option<(f64, f64)> {
    match shape {
        shapefile::Shape::Polygon(poly) => {
            let ring: &[shapefile::Point] = poly.rings().first()?.as_ref();
            let n = ring.len() as f64;
            if n == 0.0 { return None; }
            let sx: f64 = ring.iter().map(|p| p.x).sum();
            let sy: f64 = ring.iter().map(|p| p.y).sum();
            Some((sy / n, sx / n))
        }
        _ => None,
    }
}

fn compute_bbox(shape: &shapefile::Shape) -> Option<(f64, f64, f64, f64)> {
    match shape {
        shapefile::Shape::Polygon(poly) => {
            let ring: &[shapefile::Point] = poly.rings().first()?.as_ref();
            let mut min_lat = f64::MAX;
            let mut min_lon = f64::MAX;
            let mut max_lat = f64::MIN;
            let mut max_lon = f64::MIN;
            for p in ring {
                min_lat = min_lat.min(p.y);
                min_lon = min_lon.min(p.x);
                max_lat = max_lat.max(p.y);
                max_lon = max_lon.max(p.x);
            }
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

        // Find nearest state (by centroid distance — fast heuristic)
        let nearest_state = states.iter()
            .min_by(|a, b| {
                let da = (a.lat - zcta.lat).powi(2) + (a.lon - zcta.lon).powi(2);
                let db = (b.lat - zcta.lat).powi(2) + (b.lon - zcta.lon).powi(2);
                da.partial_cmp(&db).unwrap()
            });

        // Find nearest county
        let nearest_county = counties.iter()
            .min_by(|a, b| {
                let da = (a.lat - zcta.lat).powi(2) + (a.lon - zcta.lon).powi(2);
                let db = (b.lat - zcta.lat).powi(2) + (b.lon - zcta.lon).powi(2);
                da.partial_cmp(&db).unwrap()
            });

        // Find nearest incorporated place (within 50km-ish, using degree threshold)
        let nearest_place = places.iter()
            .filter(|p| (p.lat - zcta.lat).abs() < 0.5 && (p.lon - zcta.lon).abs() < 0.5)
            .min_by(|a, b| {
                let da = (a.lat - zcta.lat).powi(2) + (a.lon - zcta.lon).powi(2);
                let db = (b.lat - zcta.lat).powi(2) + (b.lon - zcta.lon).powi(2);
                da.partial_cmp(&db).unwrap()
            });

        let state_abbr = nearest_state
            .and_then(|s| state_map.get(s.fips.as_str()))
            .map(|(_, abbr)| *abbr)
            .unwrap_or("");

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

/// Write admin.bin for US: states as admin1, counties as admin2.
fn write_admin_bin(
    states: &[StateRecord],
    counties: &[CountyRecord],
    output_dir: &Path,
) -> Result<()> {
    use heimdall_core::types::AdminEntry;

    let mut entries = Vec::new();

    // States as admin1 (IDs 0..states.len())
    for (i, state) in states.iter().enumerate() {
        entries.push(AdminEntry {
            id: i as u16,
            name: format!("{} ({})", state.name, state.abbreviation),
            parent_id: None,
            coord: heimdall_core::types::Coord {
                lat: (state.lat * 1e6) as i32,
                lon: (state.lon * 1e6) as i32,
            },
            place_type: heimdall_core::types::PlaceType::State,
            population: 0, // TIGER doesn't carry state population in this schema
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
            population: 0, // TIGER doesn't carry county population in this schema
        });
    }

    let data = bincode::serialize(&entries)?;
    std::fs::write(output_dir.join("admin.bin"), &data)?;

    Ok(())
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

    // Parse shapefiles
    info!("=== Parsing shapefiles ===");

    let states = read_state_shapefile(&state_shp)?;
    info!("States: {}", states.len());

    let counties = read_county_shapefile(&county_shp)?;
    info!("Counties: {}", counties.len());

    let zctas = read_zcta_shapefile(&zcta_shp)?;
    info!("ZCTAs (ZIP codes): {}", zctas.len());

    // Assign city/state/county to each ZIP
    info!("=== Assigning admin hierarchy to ZIP codes ===");
    let zip_records = assign_zip_admin(&zctas, &states, &counties, &all_places);
    info!("Assigned {} ZIP records", zip_records.len());

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
    write_admin_bin(&states, &counties, output_dir)?;
    info!("Written admin.bin ({} states + {} counties)", states.len(), counties.len());

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
    })
}

pub struct TigerResult {
    pub state_count: usize,
    pub county_count: usize,
    pub place_count: usize,
    pub zip_count: usize,
}
