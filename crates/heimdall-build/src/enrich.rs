/// enrich.rs — resolve admin hierarchy via point-in-polygon assignment
///
/// For each place, test which admin boundary polygon contains it.
/// Falls back to nearest-centroid when no polygon contains the point
/// (boundary gaps, coordinate inaccuracies, missing geometry).
///
/// The polygon geometry comes from `admin_polygons.bin`, produced by
/// the extract step from OSM relation member ways.
///
/// Outputs:
///   admin.bin      — Vec<AdminEntry> (bincode), the admin hierarchy table
///   admin_map.bin  — HashMap<i64, (u16, u16)> (bincode), osm_id → (admin1_id, admin2_id)

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::Result;
use arrow::array::*;
use geo::{Contains, Coord as GeoCoord, LineString, Point, Polygon};
use rayon::prelude::*;
use tracing::debug;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::info;

use crate::extract::AdminPolygon;
use heimdall_core::types::*;

pub struct EnrichResult {
    pub admin_count: usize,
}

/// An admin region extracted from the Parquet (län or kommun)
struct AdminRegion {
    osm_id: i64,
    name: String,
    lat: f64,
    lon: f64,
    _admin_level: u8,
    _population: Option<u32>,
    _wikidata: bool,
}

/// An admin region ring with its polygon geometry for containment testing.
/// Multi-ring admin regions (island municipalities) are flattened into
/// separate entries sharing the same admin_id — each with its own tight
/// bounding box for efficient pre-filtering.
struct AdminWithPolygon {
    admin_id: u16,
    polygon: Polygon<f64>,
    // Bounding box pre-filter — rejects ~95% of polygons without full PIP test
    min_lon: f64,
    max_lon: f64,
    min_lat: f64,
    max_lat: f64,
}

pub fn enrich(parquet_path: &Path, output_dir: &Path) -> Result<EnrichResult> {
    info!("Reading admin regions from Parquet...");

    // -----------------------------------------------------------------------
    // Step 1: Extract admin regions (län and kommun) from Parquet
    // -----------------------------------------------------------------------
    let mut counties: Vec<AdminRegion> = Vec::new();  // admin_level 3-4 = län/region
    let mut municipalities: Vec<AdminRegion> = Vec::new();  // admin_level 5-7 = kommun

    // Also collect all places for admin assignment
    let mut places: Vec<(i64, f64, f64)> = Vec::new(); // (osm_id, lat, lon)

    let file = std::fs::File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    for batch_result in reader {
        let batch = batch_result?;
        let n = batch.num_rows();

        let osm_ids = batch.column_by_name("osm_id").unwrap()
            .as_any().downcast_ref::<Int64Array>().unwrap();
        let names = batch.column_by_name("name").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let lats = batch.column_by_name("lat").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        let lons = batch.column_by_name("lon").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        let admin_levels = batch.column_by_name("admin_level").unwrap()
            .as_any().downcast_ref::<UInt8Array>().unwrap();
        let populations = batch.column_by_name("population").unwrap()
            .as_any().downcast_ref::<UInt32Array>().unwrap();
        let wikidatas = batch.column_by_name("wikidata").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..n {
            let lat = lats.value(i);
            let lon = lons.value(i);
            let osm_id = osm_ids.value(i);
            let name = names.value(i).to_owned();

            // Skip zero-coord records
            if lat == 0.0 && lon == 0.0 { continue; }
            // Skip empty names
            if name.is_empty() { continue; }

            // Collect all places for assignment
            places.push((osm_id, lat, lon));

            // Accept admin regions from any country — is_valid_admin_for_country() handles cross-border filtering
            if !admin_levels.is_null(i) {
                let level = admin_levels.value(i);
                let population = if populations.is_null(i) { None } else { Some(populations.value(i)) };
                let has_wikidata = !wikidatas.is_null(i);

                let region = AdminRegion {
                    osm_id,
                    name,
                    lat,
                    lon,
                    _admin_level: level,
                    _population: population,
                    _wikidata: has_wikidata,
                };

                match level {
                    // admin1: state/region/province/federal subject
                    // Level 3 = federal districts (RU), level 4 = states/regions (universal)
                    3..=4 => counties.push(region),
                    // admin2: district/county/municipality
                    // Level 5-6 = districts/provinces, level 7-8 = municipalities/communes
                    5..=8 => municipalities.push(region),
                    _ => {}
                }
            }
        }
    }

    info!(
        "Found {} counties and {} municipalities from {} places",
        counties.len(), municipalities.len(), places.len()
    );

    // -----------------------------------------------------------------------
    // Step 1b: Load polygon geometry and filter out cross-border admin regions
    // -----------------------------------------------------------------------
    let polygons_path = parquet_path.with_file_name("admin_polygons.bin");
    let raw_polygons: Vec<AdminPolygon> = if polygons_path.exists() {
        let bytes = std::fs::read(&polygons_path)?;
        bincode::deserialize(&bytes)?
    } else {
        info!("No admin_polygons.bin found — falling back to nearest-centroid only");
        vec![]
    };

    // Index polygons by osm_id for quick lookup
    let polygon_map: HashMap<i64, &AdminPolygon> = raw_polygons.iter()
        .map(|p| (p.osm_id, p))
        .collect();

    let has_polygons = !raw_polygons.is_empty();
    if has_polygons {
        info!("Loaded {} admin polygons for point-in-polygon assignment", raw_polygons.len());
    }

    // Filter out cross-border admin regions using country-specific name patterns.
    // Geofabrik PBFs include admin boundary relations from neighboring countries
    // (e.g. Denmark PBF includes Skåne län, Mecklenburg-Vorpommern).
    let dir_name = output_dir.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    let pre_county = counties.len();
    let pre_muni = municipalities.len();

    counties.retain(|r| is_valid_admin_for_country(&r.name, r._admin_level, dir_name));
    municipalities.retain(|r| is_valid_admin_for_country(&r.name, r._admin_level, dir_name));

    let removed_c = pre_county - counties.len();
    let removed_m = pre_muni - municipalities.len();
    if removed_c > 0 || removed_m > 0 {
        info!(
            "Filtered out {} cross-border counties + {} municipalities (country name check)",
            removed_c, removed_m
        );
    }

    // -----------------------------------------------------------------------
    // Step 2: Build AdminEntry list with sequential IDs
    // -----------------------------------------------------------------------
    // Sort for deterministic ID assignment
    counties.sort_by(|a, b| a.name.cmp(&b.name));
    municipalities.sort_by(|a, b| a.name.cmp(&b.name));

    let mut admin_entries: Vec<AdminEntry> = Vec::new();
    let mut county_ids: HashMap<i64, u16> = HashMap::new(); // osm_id → admin entry index

    // Counties first (admin1)
    for county in &counties {
        let id = admin_entries.len() as u16;
        county_ids.insert(county.osm_id, id);
        admin_entries.push(AdminEntry {
            id,
            name: county.name.clone(),
            parent_id: None, // Counties have no parent (country level is implicit)
            coord: Coord::new(county.lat, county.lon),
            place_type: PlaceType::State,
        });
    }

    let county_count = admin_entries.len();

    // Municipalities (admin2), linked to their parent county
    let mut municipality_ids: HashMap<i64, u16> = HashMap::new();

    for muni in &municipalities {
        let id = admin_entries.len() as u16;
        municipality_ids.insert(muni.osm_id, id);

        // Find parent county — use polygon containment if available, else nearest centroid
        let parent_id = if has_polygons {
            find_county_by_polygon(muni.lat, muni.lon, &counties, &county_ids, &polygon_map)
                .unwrap_or_else(|| find_nearest_county(muni.lat, muni.lon, &counties, &county_ids))
        } else {
            find_nearest_county(muni.lat, muni.lon, &counties, &county_ids)
        };

        admin_entries.push(AdminEntry {
            id,
            name: muni.name.clone(),
            parent_id: Some(parent_id),
            coord: Coord::new(muni.lat, muni.lon),
            place_type: PlaceType::County,
        });
    }

    info!(
        "Admin index: {} entries ({} counties + {} municipalities)",
        admin_entries.len(), county_count, admin_entries.len() - county_count
    );

    // -----------------------------------------------------------------------
    // Step 2b: Build geo::Polygon objects for containment testing
    // -----------------------------------------------------------------------
    let county_polygons: Vec<AdminWithPolygon> = if has_polygons {
        build_geo_polygons(&counties, &county_ids, &polygon_map)
    } else {
        vec![]
    };

    let muni_polygons: Vec<AdminWithPolygon> = if has_polygons {
        build_geo_polygons(&municipalities, &municipality_ids, &polygon_map)
    } else {
        vec![]
    };

    if has_polygons {
        info!(
            "Built {} county polygons + {} municipality polygons for containment testing",
            county_polygons.len(), muni_polygons.len()
        );
    }

    // -----------------------------------------------------------------------
    // Step 3: Assign admin1_id + admin2_id to every place
    // -----------------------------------------------------------------------
    let method = if has_polygons { "point-in-polygon (parallel)" } else { "nearest-centroid (parallel)" };
    info!("Assigning admin hierarchy to {} places ({})...", places.len(), method);

    let pip_hits = AtomicU64::new(0);
    let centroid_fallbacks = AtomicU64::new(0);

    let place_assignments: Vec<(i64, (u16, u16))> = places.par_iter()
        .map(|(osm_id, lat, lon)| {
            let (admin1_id, admin2_id) = assign_admin_parallel(
                *lat, *lon,
                &county_polygons, &muni_polygons,
                &counties, &county_ids,
                &municipalities, &municipality_ids,
                &pip_hits, &centroid_fallbacks,
            );
            (*osm_id, (admin1_id, admin2_id))
        })
        .collect();

    let mut admin_map: HashMap<i64, (u16, u16)> = HashMap::with_capacity(place_assignments.len());
    for (osm_id, admin_ids) in place_assignments {
        admin_map.insert(osm_id, admin_ids);
    }

    let pip_hits_val = pip_hits.load(Ordering::Relaxed);
    let centroid_fallbacks_val = centroid_fallbacks.load(Ordering::Relaxed);
    if has_polygons {
        info!(
            "Place assignment: {} polygon hits, {} centroid fallbacks ({:.1}% polygon)",
            pip_hits_val, centroid_fallbacks_val,
            pip_hits_val as f64 / (pip_hits_val + centroid_fallbacks_val).max(1) as f64 * 100.0
        );
    }

    // -----------------------------------------------------------------------
    // Step 3b: Also assign admin to addresses (if addresses.parquet exists)
    // -----------------------------------------------------------------------
    let addr_parquet_path = parquet_path.with_file_name("addresses.parquet");
    if addr_parquet_path.exists() {
        info!("Assigning admin hierarchy to addresses...");

        // Collect all addresses from parquet into a flat vec for parallel processing
        let mut addresses: Vec<(i64, f64, f64)> = Vec::new();
        let file = std::fs::File::open(&addr_parquet_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch_result in reader {
            let batch = batch_result?;
            let osm_ids = batch.column_by_name("osm_id").unwrap()
                .as_any().downcast_ref::<Int64Array>().unwrap();
            let lats = batch.column_by_name("lat").unwrap()
                .as_any().downcast_ref::<Float64Array>().unwrap();
            let lons = batch.column_by_name("lon").unwrap()
                .as_any().downcast_ref::<Float64Array>().unwrap();

            for i in 0..batch.num_rows() {
                let lat = lats.value(i);
                let lon = lons.value(i);
                if lat == 0.0 && lon == 0.0 { continue; }
                addresses.push((osm_ids.value(i), lat, lon));
            }
        }

        let addr_pip = AtomicU64::new(0);
        let addr_fallback = AtomicU64::new(0);

        let addr_assignments: Vec<(i64, (u16, u16))> = addresses.par_iter()
            .map(|(osm_id, lat, lon)| {
                let (admin1_id, admin2_id) = assign_admin_parallel(
                    *lat, *lon,
                    &county_polygons, &muni_polygons,
                    &counties, &county_ids,
                    &municipalities, &municipality_ids,
                    &addr_pip, &addr_fallback,
                );
                (*osm_id, (admin1_id, admin2_id))
            })
            .collect();

        let addr_count = addr_assignments.len();
        for (osm_id, admin_ids) in addr_assignments {
            admin_map.insert(osm_id, admin_ids);
        }

        if has_polygons {
            let addr_pip_val = addr_pip.load(Ordering::Relaxed);
            let addr_fallback_val = addr_fallback.load(Ordering::Relaxed);
            info!(
                "Address assignment: {} polygon hits, {} centroid fallbacks ({:.1}% polygon)",
                addr_pip_val, addr_fallback_val,
                addr_pip_val as f64 / (addr_pip_val + addr_fallback_val).max(1) as f64 * 100.0
            );
        }
        info!("Assigned admin to {} addresses", addr_count);
    }

    // -----------------------------------------------------------------------
    // Step 4: Write outputs
    // -----------------------------------------------------------------------

    // admin.bin — the hierarchy table (loaded at query time)
    let admin_bytes = postcard::to_allocvec(&admin_entries).expect("postcard serialize admin");
    let admin_path = output_dir.join("admin.bin");
    std::fs::write(&admin_path, &admin_bytes)?;
    heimdall_core::compressed_io::compress_file(&admin_path, 19)?;
    info!("admin.bin: {:.1} KB ({} entries)", admin_bytes.len() as f64 / 1024.0, admin_entries.len());

    // admin_map.bin — osm_id → (admin1_id, admin2_id) mapping (used by pack step)
    let map_bytes = bincode::serialize(&admin_map)?;
    std::fs::write(output_dir.join("admin_map.bin"), &map_bytes)?;
    info!("admin_map.bin: {:.1} MB ({} entries)", map_bytes.len() as f64 / 1e6, admin_map.len());

    Ok(EnrichResult {
        admin_count: admin_entries.len(),
    })
}

// ---------------------------------------------------------------------------
// Point-in-polygon assignment
// ---------------------------------------------------------------------------

/// Assign admin1 (county) and admin2 (municipality) to a point — parallel-safe version.
/// Uses polygon containment when available, falls back to nearest centroid.
/// Counters use AtomicU64 for lock-free concurrent updates.
#[allow(clippy::too_many_arguments)]
fn assign_admin_parallel(
    lat: f64,
    lon: f64,
    county_polygons: &[AdminWithPolygon],
    muni_polygons: &[AdminWithPolygon],
    counties: &[AdminRegion],
    county_ids: &HashMap<i64, u16>,
    municipalities: &[AdminRegion],
    municipality_ids: &HashMap<i64, u16>,
    pip_hits: &AtomicU64,
    centroid_fallbacks: &AtomicU64,
) -> (u16, u16) {
    let point = Point::new(lon, lat); // geo crate uses (x=lon, y=lat)

    // Try polygon containment for county
    let admin1_id = match find_containing_polygon(&point, county_polygons) {
        Some(id) => {
            pip_hits.fetch_add(1, Ordering::Relaxed);
            id
        }
        None => {
            centroid_fallbacks.fetch_add(1, Ordering::Relaxed);
            find_nearest_county(lat, lon, counties, county_ids)
        }
    };

    // Try polygon containment for municipality
    let admin2_id = find_containing_polygon(&point, muni_polygons)
        .unwrap_or_else(|| find_nearest_municipality(lat, lon, municipalities, municipality_ids));

    (admin1_id, admin2_id)
}

/// Find which polygon contains the given point. Returns the admin_id if found.
/// Uses bounding box pre-filter to skip most polygons cheaply.
fn find_containing_polygon(point: &Point<f64>, polygons: &[AdminWithPolygon]) -> Option<u16> {
    let (lon, lat) = (point.x(), point.y());
    for awp in polygons {
        // Fast bbox reject — skips ~95% of polygons
        if lon < awp.min_lon || lon > awp.max_lon || lat < awp.min_lat || lat > awp.max_lat {
            continue;
        }
        if awp.polygon.contains(point) {
            return Some(awp.admin_id);
        }
    }
    None
}

/// Build geo::Polygon entries from AdminRegions + raw polygon data.
/// Multi-ring regions (island municipalities) are flattened into separate
/// entries sharing the same admin_id — each ring gets its own tight bbox
/// for efficient spatial pre-filtering.
///
/// Single-ring polygons must pass a self-containment check (centroid inside
/// the ring) to filter out broken cross-border geometry from partial
/// Geofabrik extracts. Multi-ring polygons skip this check since the
/// averaged centroid may fall in the sea between islands.
fn build_geo_polygons(
    regions: &[AdminRegion],
    id_map: &HashMap<i64, u16>,
    polygon_map: &HashMap<i64, &AdminPolygon>,
) -> Vec<AdminWithPolygon> {
    let mut valid_regions = 0usize;
    let mut rejected = 0usize;
    let mut multi_ring = 0usize;
    let mut total_entries = 0usize;

    let mut result: Vec<AdminWithPolygon> = Vec::new();

    for region in regions {
        let admin_id = match id_map.get(&region.osm_id) {
            Some(&id) => id,
            None => continue,
        };
        let raw = match polygon_map.get(&region.osm_id) {
            Some(p) => p,
            None => continue,
        };

        // Convert each ring to a geo::Polygon with per-ring bbox
        let ring_polygons: Vec<(Polygon<f64>, f64, f64, f64, f64)> = raw.rings.iter()
            .filter_map(|ring| {
                let coords: Vec<GeoCoord<f64>> = ring.iter()
                    .map(|(lat, lon)| GeoCoord { x: *lon, y: *lat })
                    .collect();
                if coords.len() < 4 { return None; }

                let (min_lon, max_lon) = ring.iter()
                    .map(|(_, lon)| *lon)
                    .fold((f64::MAX, f64::MIN), |(mn, mx), lon| (mn.min(lon), mx.max(lon)));
                let (min_lat, max_lat) = ring.iter()
                    .map(|(lat, _)| *lat)
                    .fold((f64::MAX, f64::MIN), |(mn, mx), lat| (mn.min(lat), mx.max(lat)));

                Some((
                    Polygon::new(LineString::new(coords), vec![]),
                    min_lon, max_lon, min_lat, max_lat,
                ))
            })
            .collect();

        if ring_polygons.is_empty() { continue; }

        let ring_count = ring_polygons.len();

        // Self-containment check for single-ring polygons
        if ring_count == 1 {
            let centroid_point = Point::new(region.lon, region.lat);
            if !ring_polygons[0].0.contains(&centroid_point) {
                debug!(
                    "Rejected single-ring polygon for '{}' (centroid outside ring)",
                    region.name
                );
                rejected += 1;
                continue;
            }
        }

        if ring_count > 1 {
            multi_ring += 1;
        }

        // Emit one entry per ring, each with its own tight bbox
        for (polygon, min_lon, max_lon, min_lat, max_lat) in ring_polygons {
            result.push(AdminWithPolygon {
                admin_id, polygon,
                min_lon, max_lon, min_lat, max_lat,
            });
            total_entries += 1;
        }

        valid_regions += 1;
    }

    if rejected > 0 || multi_ring > 0 {
        info!(
            "Polygon build: {} regions ({} multi-ring) → {} entries, {} rejected",
            valid_regions, multi_ring, total_entries, rejected
        );
    }

    result
}

// ---------------------------------------------------------------------------
// Country-specific admin filtering
// ---------------------------------------------------------------------------

/// Check if an admin region name matches the expected pattern for the target country.
/// Uses the index directory name to detect which country is being built.
/// This filters out cross-border admin regions from Geofabrik PBF extracts.
fn is_valid_admin_for_country(name: &str, admin_level: u8, dir_name: &str) -> bool {
    if dir_name.contains("denmark") || dir_name.contains("-dk") {
        match admin_level {
            // Danish regions: "Region Hovedstaden", "Region Sjælland", etc.
            3..=4 => name.starts_with("Region "),
            // Danish municipalities: "Københavns Kommune", "Bornholms Regionskommune"
            5..=8 => name.ends_with(" Kommune") || name.ends_with("kommune"),
            _ => false,
        }
    } else if dir_name.contains("sweden") || dir_name.contains("-se") {
        match admin_level {
            // Swedish counties: "Skåne län", "Stockholms län"
            3..=4 => name.ends_with(" län"),
            // Swedish municipalities. Most use the " kommun" suffix; the
            // four metropolitan ones use " Stad"/"stad" instead
            // (Göteborgs Stad, Malmö stad, …) — without these the
            // central Göteborg points fall through to the neighbouring
            // Mölndal polygon.
            5..=8 => {
                name.ends_with(" kommun")
                    || name.ends_with(" Stad")
                    || name.ends_with(" stad")
            }
            _ => false,
        }
    } else if dir_name.contains("finland") || dir_name.contains("-fi") {
        match admin_level {
            // Finnish regions (maakunta): "Uusimaa", "Pirkanmaa", "Ahvenanmaa", etc.
            // Åland is admin_level=3, Finnish regions are admin_level=4
            3..=4 => {
                // Reject Swedish/Norwegian county names that leak in
                !name.ends_with(" län") && !name.ends_with(" fylke")
            }
            // Finnish municipalities: "Helsingin kaupunki", "Tampereen kaupunki", "Espoon kaupunki"
            // or just the name itself without suffix in many cases
            5..=8 => {
                // Reject Swedish/Norwegian municipality names
                !name.ends_with(" kommun") && !name.ends_with(" kommune")
            }
            _ => false,
        }
    } else if dir_name.contains("norway") || dir_name.contains("-no") {
        // Norwegian admin names don't have a consistent suffix, but we can reject
        // known foreign patterns that leak in from Geofabrik cross-border extracts
        let is_foreign = name.ends_with(" kommun")       // Swedish municipality
            || name.ends_with(" län")                     // Swedish county
            || name.contains("seutukunta")                // Finnish sub-region
            || name.starts_with("Region ")                // Danish region
            || name.ends_with(" Kommune")                 // Danish municipality
            || name == "Manner-Suomi"                     // Finnish mainland
            || name == "Lappi";                           // Finnish Lapland
        !is_foreign
    } else if dir_name.contains("germany") || dir_name.contains("-de") {
        // German admin hierarchy:
        //   admin_level 4 = Bundesland (16 states)
        //   admin_level 5 = Regierungsbezirk (some states only)
        //   admin_level 6 = Landkreis/Stadtkreis (~400 districts)
        //   admin_level 8 = Gemeinde (~11,000 municipalities)
        //
        // We use admin_level 4 as admin1 (Bundesland/state) and
        // admin_level 6 as admin2 (Landkreis/district).
        // Filter out neighboring countries' admin regions.
        match admin_level {
            // Bundesländer (states): admin_level 4
            3..=4 => {
                // Reject non-German admin regions that leak from Geofabrik extract
                let is_foreign = name.ends_with(" län")          // Swedish county
                    || name.ends_with(" kommun")                 // Swedish municipality
                    || name.ends_with(" Kommune")                // Danish municipality
                    || name.starts_with("Region ")               // Danish region
                    || name.ends_with(" fylke")                  // Norwegian county
                    || name.starts_with("Provincie ")            // Dutch province
                    || name.starts_with("Province ")             // Belgian/French province
                    || name.starts_with("Canton ")               // Swiss canton
                    || name == "Polska"                          // Poland
                    || name == "Česko";                          // Czechia
                !is_foreign
            }
            // Landkreis/Stadtkreis/Gemeinde (districts + municipalities): admin_level 5-8
            5..=8 => {
                // German Landkreis names typically end with specific patterns
                // but city-states (Berlin, Hamburg, Bremen) are plain names.
                // Reject obvious foreign admin names.
                let is_foreign = name.ends_with(" kommun")       // Swedish
                    || name.ends_with(" Kommune")                // Danish
                    || name.ends_with(" län")                    // Swedish
                    || name.ends_with(" gemeente")               // Dutch
                    || name.starts_with("Arrondissement ");      // French/Belgian
                !is_foreign
            }
            _ => false,
        }
    } else if dir_name.contains("-gb") || dir_name.contains("-uk") || dir_name.contains("britain") {
        // UK admin hierarchy:
        //   admin_level 4 = nation (England, Scotland, Wales, NI)
        //   admin_level 6 = county/council area/borough
        //   admin_level 8 = district/unitary authority
        match admin_level {
            3..=4 => {
                let is_foreign = name.ends_with(" län")
                    || name.ends_with(" kommun")
                    || name.ends_with(" Kommune")
                    || name.starts_with("Region ")
                    || name.starts_with("Provincie ")
                    || name.starts_with("Département ");
                !is_foreign
            }
            5..=8 => {
                let is_foreign = name.ends_with(" kommun")
                    || name.ends_with(" Kommune")
                    || name.ends_with(" län")
                    || name.ends_with(" gemeente");
                !is_foreign
            }
            _ => false,
        }
    } else if dir_name.contains("-us") || dir_name.contains("united-states") || dir_name.contains("america") {
        // US admin hierarchy (from OSM):
        //   admin_level 4 = State (50 states + DC + territories)
        //   admin_level 6 = County (~3,143 counties)
        //   admin_level 8 = City/Town/Village (incorporated places)
        match admin_level {
            3..=4 => {
                // US states — reject obvious foreign admin names
                let is_foreign = name.ends_with(" län")
                    || name.ends_with(" kommun")
                    || name.ends_with(" Kommune")
                    || name.starts_with("Provincia ")
                    || name.starts_with("Municipio ");
                !is_foreign
            }
            5..=8 => {
                // US counties and cities — reject Mexican/Canadian patterns
                let is_foreign = name.starts_with("Municipio ")
                    || name.starts_with("Provincia ");
                !is_foreign
            }
            _ => false,
        }
    } else if dir_name.contains("russia") || dir_name.contains("-ru") {
        // Russian admin hierarchy (from OSM):
        //   admin_level 3 = Federal district (8 macro-regions, e.g. "Приволжский федеральный округ")
        //   admin_level 4 = Federal subject (85 oblasts/republics/krais/autonomous okrugs/federal cities)
        //   admin_level 5 = Administrative district within a subject
        //   admin_level 6 = City/town district
        //   admin_level 8 = Municipality
        //
        // We want admin_level 4 as admin1 (federal subject) — not admin_level 3 (federal district).
        // Federal districts are macro-regions grouping multiple subjects; they're not useful as admin1.
        match admin_level {
            // Federal districts: reject (only 8, too coarse for admin1)
            3 => false,
            // Federal subjects (oblasts, republics, krais, etc.): accept as admin1
            4 => true,
            // Districts and municipalities: accept, reject obvious foreign patterns
            5..=8 => {
                let is_foreign = name.ends_with(" län")       // Swedish county
                    || name.ends_with(" kommun")              // Swedish municipality
                    || name.ends_with(" maakunta");           // Finnish region
                !is_foreign
            }
            _ => false,
        }
    } else {
        true // Unknown country — keep everything
    }
}

// ---------------------------------------------------------------------------
// Nearest-centroid fallback
// ---------------------------------------------------------------------------

/// Find the nearest county centroid to (lat, lon).
/// Returns the admin entry ID (index into admin_entries).
fn find_nearest_county(
    lat: f64,
    lon: f64,
    counties: &[AdminRegion],
    county_ids: &HashMap<i64, u16>,
) -> u16 {
    let mut best_id = 0u16;
    let mut best_dist = f64::MAX;

    for county in counties {
        let dist = approx_distance_sq(lat, lon, county.lat, county.lon);
        if dist < best_dist {
            best_dist = dist;
            best_id = *county_ids.get(&county.osm_id).unwrap_or(&0);
        }
    }

    best_id
}

/// Find the nearest municipality centroid to (lat, lon).
fn find_nearest_municipality(
    lat: f64,
    lon: f64,
    municipalities: &[AdminRegion],
    municipality_ids: &HashMap<i64, u16>,
) -> u16 {
    let mut best_id = 0u16;
    let mut best_dist = f64::MAX;

    for muni in municipalities {
        let dist = approx_distance_sq(lat, lon, muni.lat, muni.lon);
        if dist < best_dist {
            best_dist = dist;
            best_id = *municipality_ids.get(&muni.osm_id).unwrap_or(&0);
        }
    }

    best_id
}

/// Fast approximate squared distance (good enough for nearest-neighbor).
/// Uses equirectangular approximation — fine for Nordic latitude range.
fn approx_distance_sq(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let dlat = lat2 - lat1;
    // Longitude degrees are shorter at high latitudes — scale by cos(lat)
    let cos_lat = ((lat1 + lat2) / 2.0).to_radians().cos();
    let dlon = (lon2 - lon1) * cos_lat;
    dlat * dlat + dlon * dlon
}

/// Find county by polygon containment with self-check filter.
/// Returns None if no valid polygon contains the point.
fn find_county_by_polygon(
    lat: f64,
    lon: f64,
    counties: &[AdminRegion],
    county_ids: &HashMap<i64, u16>,
    polygon_map: &HashMap<i64, &AdminPolygon>,
) -> Option<u16> {
    let point = Point::new(lon, lat);

    for county in counties {
        if let Some(raw) = polygon_map.get(&county.osm_id) {
            for ring in &raw.rings {
                let coords: Vec<GeoCoord<f64>> = ring.iter()
                    .map(|(lat, lon)| GeoCoord { x: *lon, y: *lat })
                    .collect();
                if coords.len() < 4 { continue; }
                let polygon = Polygon::new(LineString::new(coords), vec![]);

                // Self-containment check for single-ring counties
                if raw.rings.len() == 1 {
                    let centroid = Point::new(county.lon, county.lat);
                    if !polygon.contains(&centroid) { continue; }
                }

                if polygon.contains(&point) {
                    return county_ids.get(&county.osm_id).copied();
                }
            }
        }
    }
    None
}
