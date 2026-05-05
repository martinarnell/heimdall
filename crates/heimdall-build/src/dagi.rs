/// dagi.rs — Danske Stednavne (DAGI) parser
///
/// Parses the official Danish place-names registry from Dataforsyningen
/// (Styrelsen for Dataforsyning og Infrastruktur). The dataset covers
/// ~140K named features — bydele (city districts), bygninger (buildings:
/// castles, churches, museums, stadiums), bridges, lakes, islands,
/// natural-area features, etc.
///
/// Why this matters: OSM is patchy on Danish landmarks. "Storebæltsbroen"
/// (the Great Belt Bridge), "Charlottenlund Slot", "Klampenborg" (as a
/// proper bydel rather than a place=village), "H.C. Andersens Hus" all
/// have authoritative DAGI records but inconsistent OSM coverage.
///
/// Source: https://api.dataforsyningen.dk/steder?format=json
/// License: Free (SDFI's "frie geografiske data")
///
/// JSON record shape (relevant fields only):
/// ```json
/// {
///   "id": "19e4392e-cf90-5d41-...",
///   "hovedtype": "Bygning",
///   "undertype": "slot",
///   "primærtnavn": "Charlottenlund Slot",
///   "visueltcenter": [12.580, 55.750],
///   "bbox": [...],
///   "kommuner": [{"kode": "0157", "navn": "Gentofte"}],
///   "sekundærenavne": []
/// }
/// ```
///
/// `visueltcenter` is the cartographic display centroid — the right point
/// to geocode to (vs `bbox` centroids which can land off-feature).

use std::path::Path;

use anyhow::{Context, Result};
use heimdall_core::types::*;
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Deserialize)]
struct DagiRecord {
    id: String,
    hovedtype: String,
    undertype: String,
    #[serde(rename = "primærtnavn")]
    primary_name: Option<String>,
    #[serde(rename = "visueltcenter")]
    visual_center: Option<[f64; 2]>,
    #[serde(rename = "sekundærenavne", default)]
    secondary_names: Vec<DagiSecondaryName>,
}

#[derive(Debug, Deserialize)]
struct DagiSecondaryName {
    navn: String,
}

/// Map a DAGI hovedtype/undertype combination to our PlaceType. Returns
/// None when the type isn't worth indexing (too dense, too generic, or
/// already well-covered by OSM addressing).
fn map_dagi_type(hovedtype: &str, undertype: &str) -> Option<PlaceType> {
    match (hovedtype, undertype) {
        // ── Settlements / districts (THE big win — bydele) ────────────
        ("Bebyggelse", "bydel") => Some(PlaceType::Suburb),
        ("Bebyggelse", "by") => Some(PlaceType::Village),
        ("Bebyggelse", "sommerhusområde") => Some(PlaceType::Neighbourhood),
        ("Bebyggelse", "kolonihave") => Some(PlaceType::Neighbourhood),

        // ── Cultural landmarks ─────────────────────────────────────────
        ("Bygning", "slot") => Some(PlaceType::Landmark),         // castles
        ("Bygning", "herregård") => Some(PlaceType::Landmark),    // manor houses
        ("Bygning", "museumSamling") => Some(PlaceType::Landmark), // museums
        ("Bygning", "kirkeProtestantisk") => Some(PlaceType::Landmark),
        ("Bygning", "kirkeKatolsk") => Some(PlaceType::Landmark),
        ("Bygning", "kloster") => Some(PlaceType::Landmark),
        ("Bygning", "domkirke") => Some(PlaceType::Landmark),
        ("Bygning", "ruin") => Some(PlaceType::Landmark),
        ("Seværdighed", _) => Some(PlaceType::Landmark),

        // ── Public buildings ───────────────────────────────────────────
        ("Bygning", "rådhus") => Some(PlaceType::PublicBuilding),
        ("Bygning", "bibliotek") => Some(PlaceType::PublicBuilding),
        ("Bygning", "teater") => Some(PlaceType::PublicBuilding),
        ("Bygning", "operahus") => Some(PlaceType::PublicBuilding),

        // ── Universities / education ───────────────────────────────────
        ("Bygning", "universitet") => Some(PlaceType::University),
        ("Bygning", "højskoleEtc") => Some(PlaceType::University),

        // ── Hospitals ──────────────────────────────────────────────────
        ("Bygning", "hospital") => Some(PlaceType::Hospital),
        ("Bygning", "sygehus") => Some(PlaceType::Hospital),

        // ── Sport ─────────────────────────────────────────────────────
        ("Idraetsanlæg", "stadion") => Some(PlaceType::Landmark), // Parken et al.

        // ── Transport ─────────────────────────────────────────────────
        ("Standsningssted", "tog") => Some(PlaceType::Station),
        ("Standsningssted", _) => Some(PlaceType::Station),
        ("Bygning", "togstation") => Some(PlaceType::Station),
        ("Bygning", "lufthavn") => Some(PlaceType::Airport),

        // ── Bridges (Storebæltsbroen!) ─────────────────────────────────
        ("Andentopografi punkt", "bro") => Some(PlaceType::Landmark),
        ("Vej", "vejbro") => Some(PlaceType::Landmark),  // road bridges
        ("Vej", "vejtunnel") => Some(PlaceType::Landmark),
        ("Jernbane", "jernbanetunnel") => Some(PlaceType::Landmark),
        ("Jernbane", "veteranjernbane") => Some(PlaceType::Landmark),

        // ── Tourist viewpoints + lighthouses ───────────────────────────
        ("Andentopografi punkt", "udsigtspunkt") => Some(PlaceType::Landmark),
        ("Navigationsanlaeg", "fyrtårn") => Some(PlaceType::Landmark),
        ("Bygning", "turistbureau") => Some(PlaceType::Landmark),

        // ── Other church variants ──────────────────────────────────────
        ("Bygning", "kirkeAndenKristen") => Some(PlaceType::Landmark),

        // ── Other public buildings ─────────────────────────────────────
        ("Bygning", "vandrerhjem") => Some(PlaceType::Landmark),  // hostels (often historic)

        // ── Other education ────────────────────────────────────────────
        ("Bygning", "gymnasium") => Some(PlaceType::University),  // upper secondary
        ("Bygning", "folkehøjskole") => Some(PlaceType::University),
        ("Bygning", "uddannelsescenter") => Some(PlaceType::University),

        // ── Harbours (specific) ────────────────────────────────────────
        ("Havnebassin", "trafikhavn") => Some(PlaceType::Locality),
        ("Havnebassin", "lystbådehavn") => Some(PlaceType::Locality),

        // ── Coastal features ───────────────────────────────────────────
        ("Landskabsform", "næs") => Some(PlaceType::Cape),
        ("Landskabsform", "odde") => Some(PlaceType::Cape),

        // ── Historic monuments (only the named ones) ───────────────────
        ("Fortidsminde", "ruin") => Some(PlaceType::Landmark),
        ("Fortidsminde", "skanse") => Some(PlaceType::Landmark),

        // ── Sport/recreation (selective) ───────────────────────────────
        ("Idraetsanlæg", "golfbane") => Some(PlaceType::Locality),

        // ── Natural features ───────────────────────────────────────────
        ("Sø", "sø") => Some(PlaceType::Lake),
        ("Vandløb", "vandløb") => Some(PlaceType::River),
        ("Vandløb", _) => Some(PlaceType::River),
        ("Farvand", "bugt") => Some(PlaceType::Bay),
        ("Farvand", _) => Some(PlaceType::Bay),
        ("Landskabsform", "ø") => Some(PlaceType::Island),
        ("Landskabsform", "halvø") => Some(PlaceType::Island),
        // Skip Landskabsform/bakke — 8.7K records named generically like
        // "Bakken" / "Højen" inflate the per-word index for short common
        // tokens and crowd out properly-tagged landmarks (e.g. the famous
        // Tivoli "Bakken" then loses to a randomly-named hill).
        ("Landskabsform", "dal") => Some(PlaceType::Forest),
        ("Landskabsform", "pynt") => Some(PlaceType::Cape),
        ("Naturareal", "skovPlantage") => Some(PlaceType::Forest),
        ("Naturareal", "parkAnlæg") => Some(PlaceType::Park),
        ("Naturareal", "strand") => Some(PlaceType::Locality), // beaches
        ("Naturareal", "hede") => Some(PlaceType::Forest),

        // ── Harbours ──────────────────────────────────────────────────
        ("Havnebassin", _) => Some(PlaceType::Locality),

        // ── Skip (too dense / too generic) ─────────────────────────────
        // Bygning/gård (48K farms), Bygning/hus (8K houses),
        // Bygning/andenBygning (8K random buildings), spredtBebyggelse,
        // gravhøj (burial mounds), folkeskole (schools), Begravelsesplads,
        // Campingplads, vandmølle, vejrmølle, agerMark, eng, moseSump,
        // grænsesten, sten, rastepladsUdenService, undersøiskGrund.
        _ => None,
    }
}

/// Read DAGI records from the `/steder?format=json` endpoint dump.
///
/// The endpoint returns a flat JSON array — small enough (~95 MB
/// uncompressed) to deserialise in one pass; serde_json parses it
/// streaming via `from_reader` while we filter on the fly.
pub fn read_dagi_places(json_path: &Path) -> Result<Vec<RawPlace>> {
    let file = std::fs::File::open(json_path)
        .with_context(|| format!("open {}", json_path.display()))?;
    let file_size = file.metadata()?.len();
    info!(
        "Parsing DAGI Stednavne: {} ({:.1} MB)...",
        json_path.display(),
        file_size as f64 / 1_048_576.0
    );

    let reader = std::io::BufReader::new(file);
    let records: Vec<DagiRecord> =
        serde_json::from_reader(reader).context("parse DAGI JSON array")?;

    let total_in = records.len();
    let mut out: Vec<RawPlace> = Vec::with_capacity(total_in / 4);
    let mut skipped_type = 0usize;
    let mut skipped_geom = 0usize;
    let mut skipped_name = 0usize;

    for r in records {
        let place_type = match map_dagi_type(&r.hovedtype, &r.undertype) {
            Some(t) => t,
            None => { skipped_type += 1; continue; }
        };

        let center = match r.visual_center {
            Some(c) => c,
            None => { skipped_geom += 1; continue; }
        };
        // visueltcenter is [longitude, latitude] — GeoJSON convention
        let lon = center[0];
        let lat = center[1];
        if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
            skipped_geom += 1;
            continue;
        }

        let name = match r.primary_name {
            Some(n) if !n.trim().is_empty() => n,
            _ => { skipped_name += 1; continue; }
        };

        let alt_names: Vec<String> = r.secondary_names
            .into_iter()
            .map(|s| s.navn)
            .filter(|n| !n.trim().is_empty() && n != &name)
            .collect();

        // Synthetic OSM ID derived from the DAGI UUID. Use a stable
        // negative hash so we never collide with real OSM positives —
        // matches the SSR module's convention.
        let synthetic_id = -((stable_hash(&r.id) & 0x7FFF_FFFF_FFFF_FFFF) as i64);

        out.push(RawPlace {
            osm_id: synthetic_id,
            osm_type: OsmType::Node,
            name,
            name_intl: Vec::new(),
            alt_names,
            old_names: Vec::new(),
            coord: Coord::new(lat, lon),
            place_type,
            admin_level: None,
            country_code: Some(*b"DK"),
            admin1: None,
            admin2: None,
            population: None,
            // DAGI doesn't expose Wikidata IDs directly. Importance comes
            // entirely from PlaceType + admin centrality at pack time.
            wikidata: None,
            class: Some("place".to_owned()),
            class_value: None, // pack synthesises from place_type
            bbox: None,
            extratags: vec![],
        });
    }

    info!(
        "DAGI: {} records read, {} kept ({} skipped: {} type-filtered, {} no-geom, {} no-name)",
        total_in,
        out.len(),
        total_in - out.len(),
        skipped_type,
        skipped_geom,
        skipped_name,
    );

    Ok(out)
}

/// Merge DAGI places into the existing OSM-derived parquet, dropping
/// near-duplicates (same normalised name within ~111 m). Returns the
/// merged vector. Same algorithm as `ssr::merge_ssr_places`.
pub fn merge_dagi_places(existing: &[RawPlace], dagi: &[RawPlace]) -> Vec<RawPlace> {
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

    for place in dagi {
        let key = (
            (place.coord.lat_f64() / cell_size) as i32,
            (place.coord.lon_f64() / cell_size) as i32,
        );
        let name_lower = place.name.to_lowercase();

        let mut is_dup = false;
        'outer: for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(entries) = grid.get(&(key.0 + dx, key.1 + dy)) {
                    for (existing_name, _) in entries {
                        if *existing_name == name_lower {
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
            grid.entry(key)
                .or_default()
                .push((name_lower, merged.len()));
            merged.push(place.clone());
            added += 1;
        }
    }

    info!(
        "DAGI merge: {} total ({} existing + {} new, {} deduped)",
        merged.len(),
        existing.len(),
        added,
        deduped,
    );
    merged
}

/// Stable 64-bit hash over a string (FNV-1a). Used to derive synthetic
/// OSM IDs for DAGI records that aren't backed by real OSM features.
/// Picked over std's DefaultHasher for run-to-run reproducibility — the
/// FNV constants are the same on every host, so two builds on the same
/// input produce the same synthetic IDs.
fn stable_hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}
