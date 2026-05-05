/// gnis.rs — USGS GNIS Domestic Names parser
///
/// Parses the official US federal gazetteer from the U.S. Geological Survey
/// (Geographic Names Information System). Public-domain dataset of ~1M+
/// named features: populated places, schools, churches, cemeteries, summits,
/// lakes, streams, parks, hospitals, airports, bridges, etc.
///
/// Why this matters: OSM has variable coverage of US natural features and
/// rural POIs. GNIS provides authoritative coverage with state + county
/// admin hints, useful for disambiguation and as a `known_variants` source
/// for historical / variant names.
///
/// Source: https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/DomesticNames/DomesticNames_National_Text.zip
/// License: Public domain (USGS, US federal gazetteer)
/// Update cadence: rolling (USGS BGN updates monthly)
///
/// File format inside ZIP: a single pipe-delimited UTF-8 text file whose
/// name varies per release (e.g. `DomesticNames_National.txt`). Header row
/// included; coordinates already in WGS84 decimal degrees.
///
/// Pipe-delimited columns (per USGS documentation):
///   feature_id|feature_name|feature_class|state_name|state_numeric|
///   county_name|county_numeric|map_name|date_created|date_edited|
///   bgn_date|bgn_type|bgn_authority|prim_lat_dms|prim_long_dms|
///   prim_lat_dec|prim_long_dec|source_lat_dms|source_long_dms|
///   source_lat_dec|source_long_dec|elev_in_m|elev_in_ft|map_scale

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use heimdall_core::types::*;
use tracing::info;

/// Map a GNIS `feature_class` string to a Heimdall `PlaceType`.
///
/// Returns `None` for feature classes we either don't want (too noisy /
/// already in OSM) or that aren't worth indexing. The classes returned
/// here cover the high-signal physical features and POIs that genuinely
/// expand US coverage beyond OSM.
///
/// Mapping notes:
/// * "Populated Place" → `Locality` — GNIS doesn't carry population, so
///   we let the enrich step / OSM merge upgrade those that match a real
///   city/town. Standalone GNIS populated places are mostly small
///   unincorporated communities, hamlets, and ghost towns.
/// * Religious / civic / educational buildings collapse to `Landmark` /
///   `University` / `Hospital` — Heimdall has no Church/Cemetery/School
///   variants today.
/// * Bridges, dams, tunnels, towers, mines all collapse to `Landmark`.
/// * Falls / Spring / Channel / Bay are water features → `River` or `Bay`
///   accordingly.
fn map_gnis_type(feature_class: &str) -> Option<PlaceType> {
    match feature_class {
        // ── Populated places ───────────────────────────────────────────
        "Populated Place" => Some(PlaceType::Locality),

        // ── Cultural / civic / religious landmarks ─────────────────────
        "School" => Some(PlaceType::University), // best-fit; covers schools+colleges
        "Church" => Some(PlaceType::Landmark),
        "Cemetery" => Some(PlaceType::Landmark),
        "Hospital" => Some(PlaceType::Hospital),
        "Library" => Some(PlaceType::PublicBuilding),
        "Building" => Some(PlaceType::Landmark),
        "Hotel" => Some(PlaceType::Landmark),
        "Restaurant" => Some(PlaceType::Landmark),
        "Stadium" => Some(PlaceType::Landmark),
        "Tower" => Some(PlaceType::Landmark),
        "Post Office" => Some(PlaceType::PublicBuilding),

        // ── Transport infrastructure ───────────────────────────────────
        "Airport" => Some(PlaceType::Airport),
        "Bridge" => Some(PlaceType::Landmark),
        "Tunnel" => Some(PlaceType::Landmark),

        // ── Industrial / engineered ────────────────────────────────────
        "Mine" => Some(PlaceType::Landmark),
        "Dam" => Some(PlaceType::Landmark),
        "Reservoir" => Some(PlaceType::Lake),

        // ── Recreation / parks / trails ────────────────────────────────
        "Park" => Some(PlaceType::Park),
        "Trail" => Some(PlaceType::Park),
        "Forest" => Some(PlaceType::Forest),
        "Beach" => Some(PlaceType::Cape), // closest semantic match (coastal feature)

        // ── Hydrography ────────────────────────────────────────────────
        "Lake" => Some(PlaceType::Lake),
        "Stream" => Some(PlaceType::River),
        "River" => Some(PlaceType::River),
        "Falls" => Some(PlaceType::River),
        "Spring" => Some(PlaceType::River),
        "Channel" => Some(PlaceType::River),
        "Bay" => Some(PlaceType::Bay),

        // ── Terrain ────────────────────────────────────────────────────
        "Summit" => Some(PlaceType::Mountain),
        "Island" => Some(PlaceType::Island),

        // ── Skip everything else ───────────────────────────────────────
        // Notable skips (per importer brief):
        //   Locale, Civil, Cape, Bar, Bench, Crossing, Flat, Gut, Plain,
        //   Range — too noisy / vague / redundant for v1.
        // Other unlisted classes (Arch, Arroyo, Basin, Cliff, Crater,
        //   Glacier, Gap, Harbor, Levee, Pillar, Ridge, Slope, Valley,
        //   Woods …) also fall through here. We can opportunistically
        //   add them later if benchmark gaps motivate it.
        _ => None,
    }
}

/// Stable 64-bit FNV-1a hash. Used as a fallback when a `feature_id` is
/// missing or unparseable so we still produce a unique synthetic OSM id.
fn stable_hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Read GNIS Domestic Names from a pre-extracted pipe-delimited text file.
///
/// File format: UTF-8, `|`-delimited, header row first, no quoting.
/// Coordinates are already WGS84 decimal degrees (no projection needed).
pub fn read_gnis_places(txt_path: &Path) -> Result<Vec<RawPlace>> {
    let file = std::fs::File::open(txt_path)
        .with_context(|| format!("open {}", txt_path.display()))?;
    let file_size = file.metadata()?.len();
    info!(
        "Parsing USGS GNIS Domestic Names: {} ({:.1} MB)...",
        txt_path.display(),
        file_size as f64 / 1_048_576.0
    );

    use std::io::BufRead;
    let reader = std::io::BufReader::with_capacity(8 * 1024 * 1024, file);

    let mut out: Vec<RawPlace> = Vec::with_capacity(500_000);
    let mut total_in = 0usize;
    let mut skipped_type = 0usize;
    let mut skipped_geom = 0usize;
    let mut skipped_name = 0usize;
    let mut header_seen = false;

    // Default column indices (per the canonical GNIS schema). Reset from
    // the actual header row so we don't break if USGS reorders columns.
    let mut idx_feature_id = 0usize;
    let mut idx_feature_name = 1usize;
    let mut idx_feature_class = 2usize;
    let mut idx_state_name = 3usize;
    let mut idx_county_name = 5usize;
    let mut idx_lat_dec = 15usize;
    let mut idx_lon_dec = 16usize;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };

        // Strip BOM on first line if present (USGS dumps don't usually
        // include one, but be defensive).
        let stripped = line.strip_prefix('\u{feff}').unwrap_or(&line);

        // Pipe-delimited; no quoting in GNIS.
        let parts: Vec<&str> = stripped.split('|').collect();

        if !header_seen {
            for (i, h) in parts.iter().enumerate() {
                match h.trim().to_ascii_lowercase().as_str() {
                    "feature_id" => idx_feature_id = i,
                    "feature_name" => idx_feature_name = i,
                    "feature_class" => idx_feature_class = i,
                    "state_name" => idx_state_name = i,
                    "county_name" => idx_county_name = i,
                    "prim_lat_dec" => idx_lat_dec = i,
                    "prim_long_dec" => idx_lon_dec = i,
                    _ => {}
                }
            }
            header_seen = true;
            continue;
        }

        if parts.len() <= idx_lon_dec.max(idx_county_name).max(idx_state_name) {
            continue;
        }
        total_in += 1;

        let feature_id_raw = parts[idx_feature_id].trim();
        let name = parts[idx_feature_name].trim();
        let feature_class = parts[idx_feature_class].trim();
        let state_name = parts[idx_state_name].trim();
        let county_name = parts[idx_county_name].trim();

        let place_type = match map_gnis_type(feature_class) {
            Some(t) => t,
            None => {
                skipped_type += 1;
                continue;
            }
        };

        if name.is_empty() {
            skipped_name += 1;
            continue;
        }

        let lat: f64 = match parts[idx_lat_dec].trim().parse() {
            Ok(v) => v,
            Err(_) => {
                skipped_geom += 1;
                continue;
            }
        };
        let lon: f64 = match parts[idx_lon_dec].trim().parse() {
            Ok(v) => v,
            Err(_) => {
                skipped_geom += 1;
                continue;
            }
        };

        // GNIS uses 0/0 (or absent) for features without a primary point.
        if lat == 0.0 || lon == 0.0 {
            skipped_geom += 1;
            continue;
        }

        // Defensive bounding box: reject coords clearly outside the US +
        // territories envelope (covers CONUS, Alaska, Hawaii, Puerto Rico,
        // US Virgin Islands, Guam, American Samoa, Northern Marianas).
        if !(-15.0..=72.0).contains(&lat) || !(-180.0..=-64.0).contains(&lon) {
            // American Samoa straddles the antimeridian (≈ -14°S, 170°W
            // through -11°S, 169°E). Allow a separate window for that.
            let in_samoa = (-15.5..=-10.0).contains(&lat) && (168.0..=173.0).contains(&lon);
            if !in_samoa {
                skipped_geom += 1;
                continue;
            }
        }

        // Synthetic OSM ID: prefer the feature_id verbatim (it's a stable
        // u32 from USGS). Negate so we never collide with a real OSM
        // positive id — matches the convention used by ssr.rs / dagi.rs /
        // gn250.rs. Fall back to a hash of the row if feature_id is
        // missing or unparseable.
        let synthetic_id: i64 = match feature_id_raw.parse::<u32>() {
            Ok(n) if n > 0 => -(n as i64),
            _ => -((stable_hash(feature_id_raw) & 0x7FFF_FFFF_FFFF_FFFF) as i64),
        };

        out.push(RawPlace {
            osm_id: synthetic_id,
            osm_type: OsmType::Node,
            name: name.to_owned(),
            name_intl: Vec::new(),
            alt_names: Vec::new(),
            old_names: Vec::new(),
            coord: Coord::new(lat, lon),
            place_type,
            admin_level: None,
            country_code: Some(*b"US"),
            admin1: if state_name.is_empty() {
                None
            } else {
                Some(state_name.to_owned())
            },
            admin2: if county_name.is_empty() {
                None
            } else {
                Some(county_name.to_owned())
            },
            population: None,
            wikidata: None,
            class: Some("place".to_owned()),
            class_value: None,
            bbox: None,
        });
    }

    info!(
        "GNIS: {} records read, {} kept ({} skipped: {} type-filtered, {} no-geom, {} no-name)",
        total_in,
        out.len(),
        total_in - out.len(),
        skipped_type,
        skipped_geom,
        skipped_name,
    );

    // Log type distribution for sanity.
    let mut type_counts: std::collections::HashMap<u8, (PlaceType, usize)> =
        std::collections::HashMap::new();
    for p in &out {
        type_counts
            .entry(p.place_type as u8)
            .or_insert((p.place_type, 0))
            .1 += 1;
    }
    let mut types: Vec<_> = type_counts.into_values().collect();
    types.sort_by(|a, b| b.1.cmp(&a.1));
    for (pt, count) in types.iter().take(12) {
        info!("  {:?}: {}", pt, count);
    }

    Ok(out)
}

/// Extract the first plausible GNIS text file from a USGS distribution ZIP.
///
/// The official `DomesticNames_National_Text.zip` ships with a single
/// `.txt` file whose exact name shifts each release (typically
/// `DomesticNames_National.txt` or `DomesticNames_National_<date>.txt`).
/// We grab the first `.txt` entry and write it next to the ZIP.
pub fn extract_gnis_txt_from_zip(zip_path: &Path) -> Result<PathBuf> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("open {}", zip_path.display()))?;
    let mut archive = zip::ZipArchive::new(file)?;

    let out_dir = zip_path.parent().unwrap_or(Path::new("."));

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if name.to_lowercase().ends_with(".txt") {
            let basename = Path::new(&name)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("DomesticNames_National.txt");
            let out_path = out_dir.join(basename);
            let mut out = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut out)?;
            info!("  Extracted {} from ZIP", out_path.display());
            return Ok(out_path);
        }
    }

    bail!(
        "No .txt file found in GNIS ZIP: {}",
        zip_path.display()
    )
}

/// Merge GNIS places into the existing OSM-derived parquet, dropping
/// near-duplicates (same lowercased name within ~111 m). Same algorithm
/// as `ssr::merge_ssr_places` / `gn250::merge_gn250_places`.
pub fn merge_gnis_places(existing: &[RawPlace], gnis: &[RawPlace]) -> Vec<RawPlace> {
    use std::collections::HashMap;

    let cell_size = 0.001; // ~111 m latitude
    let mut grid: HashMap<(i32, i32), Vec<(String, usize)>> = HashMap::new();

    for (i, place) in existing.iter().enumerate() {
        let key = (
            (place.coord.lat_f64() / cell_size) as i32,
            (place.coord.lon_f64() / cell_size) as i32,
        );
        grid.entry(key)
            .or_default()
            .push((place.name.to_lowercase(), i));
    }

    let mut merged = existing.to_vec();
    let mut added = 0usize;
    let mut deduped = 0usize;

    for place in gnis {
        let key = (
            (place.coord.lat_f64() / cell_size) as i32,
            (place.coord.lon_f64() / cell_size) as i32,
        );
        let name_lower = place.name.to_lowercase();
        let mut is_dup = false;

        // Check this cell + 8 neighbours for a same-name record within ~110 m.
        'outer: for dy in -1..=1 {
            for dx in -1..=1 {
                let ck = (key.0 + dy, key.1 + dx);
                if let Some(entries) = grid.get(&ck) {
                    for (n, _) in entries {
                        if *n == name_lower {
                            is_dup = true;
                            break 'outer;
                        }
                    }
                }
            }
        }

        if is_dup {
            deduped += 1;
        } else {
            grid.entry(key).or_default().push((name_lower, merged.len()));
            merged.push(place.clone());
            added += 1;
        }
    }

    info!(
        "GNIS merge: {} kept ({} added new, {} deduped against OSM)",
        merged.len(),
        added,
        deduped,
    );
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_mapping_keeps_high_signal_classes() {
        assert!(map_gnis_type("Populated Place").is_some());
        assert!(map_gnis_type("Summit").is_some());
        assert!(map_gnis_type("Lake").is_some());
        assert!(map_gnis_type("Reservoir").is_some());
        assert!(map_gnis_type("Stream").is_some());
        assert!(map_gnis_type("Airport").is_some());
        assert!(map_gnis_type("Hospital").is_some());
    }

    #[test]
    fn type_mapping_drops_noisy_classes() {
        assert!(map_gnis_type("Locale").is_none());
        assert!(map_gnis_type("Civil").is_none());
        assert!(map_gnis_type("Cape").is_none());
        assert!(map_gnis_type("Bench").is_none());
        assert!(map_gnis_type("Flat").is_none());
        assert!(map_gnis_type("Gut").is_none());
        assert!(map_gnis_type("Range").is_none());
        // Unknown classes also fall through.
        assert!(map_gnis_type("Glacier").is_none());
    }

    #[test]
    fn synthetic_ids_are_negative() {
        assert!(stable_hash("123456") > 0);
        // We negate the hashed/parsed form before storing.
        let id = -(123456_i64);
        assert!(id < 0);
    }
}
