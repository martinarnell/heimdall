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
    population: Option<u32>,
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
    // Settlement populations — used in Step 3c to backfill admin populations
    // for countries (e.g. Denmark) where most kommune relations have no
    // population tag. Aggregating contained settlements gives the
    // centrality bonus something to work with for cross-country ranking.
    let mut settlement_populations: Vec<(i64, u32)> = Vec::new();

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
        let place_types = batch.column_by_name("place_type").unwrap()
            .as_any().downcast_ref::<UInt8Array>().unwrap();

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

            // Settlement-type place with a population tag — used later to
            // back-fill missing admin populations. Only City/Town/Village
            // counts; Suburb/Hamlet have unreliable OSM population tags.
            if !populations.is_null(i) {
                let pt = place_types.value(i);
                let is_settlement = matches!(pt, 3 | 4 | 5); // City=3, Town=4, Village=5
                if is_settlement {
                    let pop = populations.value(i);
                    if pop > 0 {
                        settlement_populations.push((osm_id, pop));
                    }
                }
            }

            // Accept admin regions from any country — is_valid_admin_for_country() handles cross-border filtering
            //
            // Critical: only admin-typed records qualify. OSM tags
            // admin_level on plenty of non-admin objects (rivers carrying
            // a level number, bus-station polygons, individual buildings
            // like Stadtarchiv / Kriegerdenkmal). Without this gate the
            // centroid pool fills with garbage and nearest-centroid
            // picks "Ruhebank vor dem Haus Kennedyallee" or a Belgian
            // province as a German city's admin2.
            //
            // Country/State/County (place_type 0/1/2) come from extract.rs
            // explicitly. admin_level=8 Gemeinden flow through as Unknown
            // (place_type 255) because the extract.rs admin-level→place_type
            // table only covers 2..=7 — accept those too. Rejected:
            // Locality, River, Suburb, etc. that happen to carry an
            // admin_level tag.
            let pt = place_types.value(i);
            let is_admin_typed = matches!(pt, 0 | 1 | 2 | 255);
            if is_admin_typed && !admin_levels.is_null(i) {
                let level = admin_levels.value(i);
                let population = if populations.is_null(i) { None } else { Some(populations.value(i)) };
                let has_wikidata = !wikidatas.is_null(i);

                let region = AdminRegion {
                    osm_id,
                    name,
                    lat,
                    lon,
                    _admin_level: level,
                    population,
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

    // Belt-and-braces bbox filter: many neighboring-country admin names
    // don't match an obvious foreign prefix (French départements like
    // "Moselle", Polish powiats, Czech kraje). Reject any admin region
    // whose centroid sits outside the target country's bbox so they
    // never get picked as nearest-centroid for a German border city.
    let pre_county_bb = counties.len();
    let pre_muni_bb = municipalities.len();
    counties.retain(|r| is_in_country_bbox(r.lat, r.lon, dir_name));
    municipalities.retain(|r| is_in_country_bbox(r.lat, r.lon, dir_name));
    let removed_cb = pre_county_bb - counties.len();
    let removed_mb = pre_muni_bb - municipalities.len();
    if removed_cb > 0 || removed_mb > 0 {
        info!(
            "Filtered out {} cross-border counties + {} municipalities (country bbox check)",
            removed_cb, removed_mb
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
            population: county.population.unwrap_or(0),
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
            population: muni.population.unwrap_or(0),
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
    // Step 3c: Backfill missing admin populations from contained settlements
    // -----------------------------------------------------------------------
    // Many countries (Denmark, France, Switzerland) tag their kommune /
    // commune relations without a `population` tag — we extract a 0,
    // and the centrality bonus in `compute_importance_inline` never fires.
    // The cross-country effect: a Suburb in Copenhagen with wikidata loses
    // to a same-name Park in Sundsvall whose Sundsvalls kommun *does*
    // have a population tag.
    //
    // Fix: aggregate populations of City/Town/Village settlements that
    // landed inside each admin polygon and use the sum as a backstop when
    // OSM didn't provide one. Pure data fix — no algorithm change.
    {
        let mut agg_admin1: HashMap<u16, u64> = HashMap::new();
        let mut agg_admin2: HashMap<u16, u64> = HashMap::new();
        let mut backfilled_count: usize = 0;

        for (osm_id, pop) in &settlement_populations {
            if let Some(&(a1, a2)) = admin_map.get(osm_id) {
                if a1 != u16::MAX {
                    *agg_admin1.entry(a1).or_insert(0) += *pop as u64;
                }
                if a2 != u16::MAX {
                    *agg_admin2.entry(a2).or_insert(0) += *pop as u64;
                }
            }
        }

        for entry in admin_entries.iter_mut() {
            if entry.population > 0 { continue; }
            let agg = if entry.place_type == PlaceType::State {
                agg_admin1.get(&entry.id).copied().unwrap_or(0)
            } else {
                agg_admin2.get(&entry.id).copied().unwrap_or(0)
            };
            // Cap at u32::MAX to fit AdminEntry.population. A sum overflow
            // at admin1 would mean the parent contains > 4 G people —
            // physically impossible, but the saturating cast keeps us
            // honest if the data is corrupt.
            if agg > 0 {
                entry.population = agg.min(u32::MAX as u64) as u32;
                backfilled_count += 1;
            }
        }

        if backfilled_count > 0 {
            info!(
                "Backfilled population for {} admin entries from contained settlements ({} settlements aggregated)",
                backfilled_count, settlement_populations.len()
            );
        }
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

    let admin_path = output_dir.join("admin.bin");

    // US: tiger.rs already wrote an authoritative admin.bin from Census
    // shapefiles (56 states + 3,143 counties). Overwriting it with the
    // OSM-derived hierarchy throws that work away and yields a flaky
    // nearest-centroid admin near borders. Strategy: keep the TIGER file
    // intact, then remap admin_map.bin entries (currently pointing into
    // our OSM-derived `admin_entries`) onto the TIGER IDs by name match.
    // The OSM PIP / nearest-centroid pipeline still ran above — we only
    // change which IDs the address/place records reference at the end.
    let is_us = dir_name.contains("-us")
        || dir_name.contains("united-states")
        || dir_name.contains("america");
    let preserve_tiger_admin = is_us && admin_path.exists();

    let final_admin_count = if preserve_tiger_admin {
        let tiger_bytes = heimdall_core::compressed_io::read_maybe_compressed(&admin_path)?;
        let tiger_entries: Vec<AdminEntry> = postcard::from_bytes(&tiger_bytes)
            .or_else(|_| bincode::deserialize(&tiger_bytes))
            .map_err(|e| anyhow::anyhow!("TIGER admin.bin deserialize failed: {}", e))?;
        info!(
            "Preserving TIGER admin.bin ({} entries); remapping admin_map.bin via name match",
            tiger_entries.len()
        );

        // Name → id, lowercased. State-level entries (parent_id None) live in
        // admin1_by_name; county-level (parent_id Some) in admin2_by_name.
        // Multi-county-name collisions ("Washington County" exists in 31
        // states) are resolved by the OSM admin's parent name when we have
        // it — the OSM admin1 we matched to TIGER state pins the lookup.
        let mut admin1_by_name: HashMap<String, u16> = HashMap::new();
        // (state_id, county_name_lower) → county_id
        let mut admin2_by_state_name: HashMap<(u16, String), u16> = HashMap::new();
        // Fallback when we don't know the state: county_name_lower → county_id (last write wins)
        let mut admin2_by_name_only: HashMap<String, u16> = HashMap::new();
        for e in &tiger_entries {
            let key = e.name.to_lowercase();
            if e.parent_id.is_none() {
                admin1_by_name.insert(key, e.id);
            } else {
                admin2_by_name_only.insert(key.clone(), e.id);
                if let Some(pid) = e.parent_id {
                    admin2_by_state_name.insert((pid, key), e.id);
                }
            }
        }

        // Build OSM admin id → name lookup so we can map OSM IDs in
        // admin_map → name → TIGER id. Both county and muni entries.
        let osm_id_to_name: HashMap<u16, (String, Option<u16>)> = admin_entries
            .iter()
            .map(|e| (e.id, (e.name.to_lowercase(), e.parent_id)))
            .collect();

        let mut remapped_hits = 0usize;
        let mut remapped_misses = 0usize;
        let mut new_admin_map: HashMap<i64, (u16, u16)> = HashMap::with_capacity(admin_map.len());

        for (osm_id, (osm_a1, osm_a2)) in admin_map.iter() {
            // a1 (state)
            let new_a1 = osm_id_to_name
                .get(osm_a1)
                .and_then(|(name, _)| admin1_by_name.get(name).copied())
                .unwrap_or(u16::MAX);

            // a2 (county) — prefer state-scoped lookup so "Washington
            // County" resolves to the right state's county.
            let new_a2 = if let Some((cname, _)) = osm_id_to_name.get(osm_a2) {
                if new_a1 != u16::MAX {
                    admin2_by_state_name
                        .get(&(new_a1, cname.clone()))
                        .copied()
                        .or_else(|| admin2_by_name_only.get(cname).copied())
                        .unwrap_or(u16::MAX)
                } else {
                    admin2_by_name_only.get(cname).copied().unwrap_or(u16::MAX)
                }
            } else {
                u16::MAX
            };

            if new_a1 != u16::MAX || new_a2 != u16::MAX {
                remapped_hits += 1;
            } else {
                remapped_misses += 1;
            }
            new_admin_map.insert(*osm_id, (new_a1, new_a2));
        }

        info!(
            "TIGER remap: {} records mapped, {} unmappable (kept as u16::MAX)",
            remapped_hits, remapped_misses
        );
        admin_map = new_admin_map;
        tiger_entries.len()
    } else {
        // Non-US (or no TIGER file): write the OSM-derived hierarchy as before.
        let admin_bytes = postcard::to_allocvec(&admin_entries).expect("postcard serialize admin");
        std::fs::write(&admin_path, &admin_bytes)?;
        heimdall_core::compressed_io::compress_file(&admin_path, 19)?;
        info!("admin.bin: {:.1} KB ({} entries)", admin_bytes.len() as f64 / 1024.0, admin_entries.len());
        admin_entries.len()
    };

    // admin_map.bin — osm_id → (admin1_id, admin2_id) mapping (used by pack step)
    let map_bytes = bincode::serialize(&admin_map)?;
    std::fs::write(output_dir.join("admin_map.bin"), &map_bytes)?;
    info!("admin_map.bin: {:.1} MB ({} entries)", map_bytes.len() as f64 / 1e6, admin_map.len());

    Ok(EnrichResult {
        admin_count: final_admin_count,
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

/// Check if an admin region's centroid is within the target country's bbox.
/// Catches neighboring-country admins that slipped past name-pattern filtering.
/// Returns true for unknown country dirs (no bbox = no filter).
fn is_in_country_bbox(lat: f64, lon: f64, dir_name: &str) -> bool {
    // France's bboxes cover metropolitan + outre-mer (Guadeloupe, Martinique,
    // La Réunion, Guyane, Mayotte). Without the overseas bboxes the
    // closed-list régions filter would still keep e.g. "Guadeloupe" in the
    // pool (its name is in is_french_region), but the bbox check would
    // reject it because its centroid sits at 16°N — outside metropolitan
    // France. Mirrors detect_country() in extract.rs.
    if dir_name.contains("france") || has_cc_token(dir_name, "fr") {
        let in_metro = (41.33..=51.12).contains(&lat) && (-5.14..=9.56).contains(&lon);
        let in_guadeloupe = (15.83..=16.53).contains(&lat) && (-61.80..=-61.00).contains(&lon);
        let in_martinique = (14.39..=14.88).contains(&lat) && (-61.23..=-60.81).contains(&lon);
        let in_reunion = (-21.39..=-20.87).contains(&lat) && (55.22..=55.84).contains(&lon);
        let in_guyane = (2.10..=5.80).contains(&lat) && (-54.55..=-51.60).contains(&lon);
        let in_mayotte = (-13.10..=-12.62).contains(&lat) && (45.00..=45.32).contains(&lon);
        return in_metro
            || in_guadeloupe
            || in_martinique
            || in_reunion
            || in_guyane
            || in_mayotte;
    }

    let bb = if dir_name.contains("germany") || has_cc_token(dir_name, "de") {
        // Slightly tighter than the API server bbox (5.866-55.06 / 5.87-15.04)
        // so we don't keep border-edge French/Polish admins clipping in.
        Some((47.27, 55.06, 5.87, 15.04))
    } else if dir_name.contains("denmark") || has_cc_token(dir_name, "dk") {
        Some((54.5, 57.8, 8.0, 15.2))
    } else if dir_name.contains("sweden") || has_cc_token(dir_name, "se") {
        Some((55.3, 69.1, 10.9, 24.2))
    } else if dir_name.contains("norway") || has_cc_token(dir_name, "no") {
        Some((57.5, 71.5, 4.0, 31.5))
    } else if dir_name.contains("finland") || has_cc_token(dir_name, "fi") {
        Some((59.5, 70.2, 19.4, 31.6))
    } else if dir_name.contains("austria") || has_cc_token(dir_name, "at") {
        Some((46.37, 49.02, 9.53, 17.16))
    } else if dir_name.contains("switzerland") || has_cc_token(dir_name, "ch") {
        Some((45.82, 47.81, 5.96, 10.49))
    } else if dir_name.contains("netherlands") || has_cc_token(dir_name, "nl") {
        Some((50.75, 53.47, 3.36, 7.21))
    } else if dir_name.contains("belgium") || has_cc_token(dir_name, "be") {
        Some((49.50, 51.50, 2.55, 6.41))
    } else {
        None
    };
    match bb {
        Some((min_lat, max_lat, min_lon, max_lon)) => {
            lat >= min_lat && lat <= max_lat && lon >= min_lon && lon <= max_lon
        }
        None => true,
    }
}

fn has_cc_token(dir_name: &str, cc: &str) -> bool {
    let token_mid = format!("-{}-", cc);
    let token_suf = format!("-{}", cc);
    dir_name == cc
        || dir_name.starts_with(&format!("{}-", cc))
        || dir_name.contains(&token_mid)
        || dir_name.ends_with(&token_suf)
}

/// The 18 French régions (13 metropolitan + 5 outre-mer). Closed set — last
/// reform was 2016 (Hollande's regional merger) and 2011 (Mayotte); not
/// expected to change in the index's lifetime. Without this filter, foreign
/// L4 entries that fall inside the France bbox (Vlaanderen, Wallonie,
/// Saarland, Rheinland-Pfalz, Liguria, Piemonte, Toscana, Genève, Valais,
/// Vaud, Valle d'Aosta, Euskadi, Navarra, England) get picked as Paris's
/// or Lyon's admin1.
///
/// Names mirror OSM's primary `name=*` tag — including diacritics. Some
/// régions also expose `name:fr=*` variants ("Île-de-France" vs
/// "Ile-de-France"); both are accepted because OSM extractors sometimes
/// pick whichever was first parsed.
fn is_french_region(name: &str) -> bool {
    matches!(
        name,
        "Île-de-France"
            | "Ile-de-France"
            | "Auvergne-Rhône-Alpes"
            | "Auvergne-Rhone-Alpes"
            | "Hauts-de-France"
            | "Nouvelle-Aquitaine"
            | "Occitanie"
            | "Grand Est"
            | "Provence-Alpes-Côte d'Azur"
            | "Provence-Alpes-Cote d'Azur"
            | "Pays de la Loire"
            | "Bretagne"
            | "Normandie"
            | "Bourgogne-Franche-Comté"
            | "Bourgogne-Franche-Comte"
            | "Centre-Val de Loire"
            | "Corse"
            // Outre-mer
            | "Guadeloupe"
            | "Martinique"
            | "La Réunion"
            | "Réunion"
            | "Guyane"
            | "Mayotte"
            // Special status: "France métropolitaine" is the L3 union of
            // metropolitan régions. Useful as a fallback admin1 for places
            // that don't sit inside a régional polygon (very rare, but
            // keeps the assignment from picking a foreign State).
            | "France métropolitaine"
            | "France metropolitaine"
            | "France"
    )
}

/// The 16 German Bundesländer. Closed set — list hasn't changed since
/// 1990 and won't realistically change for the index's lifetime.
fn is_german_bundesland(name: &str) -> bool {
    matches!(
        name,
        "Baden-Württemberg"
            | "Bayern"
            | "Berlin"
            | "Brandenburg"
            | "Bremen"
            | "Freie Hansestadt Bremen"
            | "Hamburg"
            | "Freie und Hansestadt Hamburg"
            | "Hessen"
            | "Mecklenburg-Vorpommern"
            | "Niedersachsen"
            | "Nordrhein-Westfalen"
            | "Rheinland-Pfalz"
            | "Saarland"
            | "Sachsen"
            | "Freistaat Sachsen"
            | "Sachsen-Anhalt"
            | "Schleswig-Holstein"
            | "Thüringen"
            | "Freistaat Thüringen"
            | "Freistaat Bayern"
    )
}

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
    } else if dir_name.contains("france") || dir_name.contains("-fr") {
        // French admin hierarchy:
        //   admin_level 3 = "France métropolitaine" (1 entry)
        //   admin_level 4 = Région (18 régions)
        //   admin_level 5 = Arrondissement de Paris / quasi-régional layers
        //   admin_level 6 = Département (~96 metro + 5 DOM)
        //   admin_level 7 = Arrondissement / EPCI (Métropole, Communauté
        //                   d'agglomération, etc.)
        //   admin_level 8 = Commune (~35,000)
        //   admin_level 9 = Arrondissement municipal (Paris/Lyon/Marseille)
        match admin_level {
            // Régions: closed list of 18 + L3 fallback. Without this,
            // Vlaanderen/Wallonie/Saarland/Liguria/Piemonte etc. (whose
            // centroids sit inside France's bbox) get picked as Lyon or
            // Paris's admin1.
            3..=4 => is_french_region(name),
            // Départements + EPCIs + communes: reject obvious foreign
            // patterns. The remaining French structures are too varied
            // for a closed list (~36k communes), so we filter by
            // exclusion + bbox.
            5..=8 => {
                let is_foreign = name.starts_with("Landkreis ")           // German
                    || name.starts_with("Stadtkreis ")                     // German
                    || name.starts_with("Regierungsbezirk ")               // German
                    || name.starts_with("Verbandsgemeinde ")               // German
                    || name.starts_with("VVG der ")                        // German
                    || name.starts_with("VG ")                             // German
                    || name.starts_with("Bezirk ")                         // German/Swiss
                    || name.starts_with("Kanton ")                         // German/Swiss
                    || name.starts_with("Canton ")                         // (mostly Swiss/Lux)
                    || name.starts_with("Gemeindebezirk ")                 // German
                    || name.starts_with("Provincie ")                      // Dutch/Belgian
                    || name.starts_with("Provincia ")                      // Italian/Spanish
                    || name.starts_with("Comune di ")                      // Italian
                    || name.starts_with("Provincie ")                      // Belgian
                    || name.starts_with("Powiat ")                         // Polish
                    || name.starts_with("powiat ")                         // Polish
                    || name.starts_with("Gmina ")                          // Polish
                    || name.starts_with("Region ")                         // Danish/Belgian
                    || name.ends_with(" Kommune")                          // Danish/Norw
                    || name.ends_with(" kommun")                           // Swedish
                    || name.ends_with(" län")                              // Swedish
                    || name.ends_with(" kraj")                             // Czech
                    || name.ends_with(" gemeente")                         // Dutch
                    || name.ends_with(" Gemeinde")                         // German
                    || name.ends_with(" County")                           // English/Irish
                    || name.starts_with("County ")                         // Irish
                    || name == "Vlaanderen"
                    || name == "Wallonie"
                    || name == "England"
                    || name == "Scotland"
                    || name == "Wales"
                    || name == "Northern Ireland"
                    || name == "Cymru";
                !is_foreign
            }
            _ => false,
        }
    } else if dir_name.contains("germany") || dir_name.contains("-de") {
        // German admin hierarchy:
        //   admin_level 4 = Bundesland (16 states)
        //   admin_level 5 = Regierungsbezirk (some states only)
        //   admin_level 6 = Landkreis/Stadtkreis (~400 districts)
        //   admin_level 8 = Gemeinde (~11,000 municipalities)
        match admin_level {
            // Bundesländer (states): only the 16 known names. Closed list
            // is safe — Bundesländer don't change. Without this, Dutch
            // Limburg (whose centroid sits inside the German bbox just
            // barely past the border) gets picked as Köln's admin1.
            3..=4 => is_german_bundesland(name),
            // Landkreis/Stadtkreis/Gemeinde (districts + municipalities): admin_level 5-8
            5..=8 => {
                // German Landkreis names typically end with specific patterns
                // but city-states (Berlin, Hamburg, Bremen) are plain names.
                // Reject obvious foreign admin names.
                let is_foreign = name.ends_with(" kommun")       // Swedish
                    || name.ends_with(" Kommune")                // Danish
                    || name.ends_with(" län")                    // Swedish
                    || name.ends_with(" gemeente")               // Dutch
                    || name.starts_with("Arrondissement ")       // French/Belgian
                    || name.starts_with("Powiat ")               // Polish
                    || name.starts_with("powiat ")               // Polish (lowercase)
                    || name.ends_with(" kraj")                   // Czech
                    || name.starts_with("Région ")               // French
                    || name.starts_with("Département ")          // French
                    || name == "Vlaanderen"                      // Belgian
                    || name == "Wallonie";                       // Belgian
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
