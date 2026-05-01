/// gn250.rs — BKG Geographische Namen 1:250 000 parser
///
/// Parses the official German place-names registry from BKG (Bundesamt für
/// Kartographie und Geodäsie). The dataset covers ~170K named features:
/// settlements (Ortslage), water bodies, mountains, forests, landmarks,
/// administrative regions, and more.
///
/// Why this matters: OSM has patchy coverage for German landmarks, smaller
/// settlements, and named natural features. GN250 provides authoritative
/// coverage with English alt-names where present.
///
/// Source: https://daten.gdz.bkg.bund.de/produkte/sonstige/gn250/aktuell/gn250.utm32s.csv.zip
/// License: dl-de/by-2-0 (Datenlizenz Deutschland — Namensnennung 2.0)
/// Update cadence: yearly (Stand 31.12. each year)
///
/// CSV column order (semicolon-delimited, BOM-prefixed):
///   NNID;DATUM;OBA;OBA_WERT;NAME;SPRACHE;GENUS;NAME2;SPRACHE2;GENUS2;
///   ZUSATZ;AGS;ARS;HOEHE;HOEHE_GER;EWZ;EWZ_GER;GEWK;GEMTEIL;VIRTUELL;
///   GEMEINDE;VERWGEM;KREIS;REGBEZIRK;BUNDESLAND;STAAT;RECHTS;HOCH;BOX
///
/// Coordinates are UTM32s (EPSG:25832); we reproject to WGS84 inline.

use std::path::Path;

use anyhow::{Context, Result};
use heimdall_core::types::*;
use tracing::info;

/// Map a GN250 OBA / OBA_WERT pair to our PlaceType. Returns None when
/// the type isn't worth indexing (too dense, too generic, or already
/// well-covered by OSM).
fn map_gn250_type(oba: &str, oba_wert: &str) -> Option<PlaceType> {
    match (oba, oba_wert) {
        // ── Settlements ────────────────────────────────────────────────
        // Ortslage = built-up area / hamlet — 47K records
        ("AX_Ortslage", _) => Some(PlaceType::Locality),
        // Gemeinde = municipality — 11K records (covers OSM gaps for tiny villages)
        ("AX_Gemeinde", _) => Some(PlaceType::Village),

        // ── Cultural / civic landmarks ─────────────────────────────────
        ("AX_BauwerkOderAnlageFuerSportFreizeitUndErholung", "Stadion, Arena") => Some(PlaceType::Landmark),
        ("AX_BauwerkOderAnlageFuerSportFreizeitUndErholung", "Freizeitpark") => Some(PlaceType::Landmark),
        ("AX_BauwerkOderAnlageFuerSportFreizeitUndErholung", "Zoo") => Some(PlaceType::Landmark),
        ("AX_BauwerkOderAnlageFuerSportFreizeitUndErholung", "Safaripark, Wildpark") => Some(PlaceType::Landmark),
        ("AX_BauwerkOderAnlageFuerSportFreizeitUndErholung", "Freilichtbühne") => Some(PlaceType::Landmark),

        // Bridges, tunnels — 2.6K records
        ("AX_BauwerkImVerkehrsbereich", "Brücke") => Some(PlaceType::Landmark),
        ("AX_BauwerkImVerkehrsbereich", "Tunnel, Unterführung") => Some(PlaceType::Landmark),

        // Schlösser / Burgen — fall-through to historic via OBA name
        ("AX_SonstigesBauwerkOderSonstigeEinrichtung", _) => Some(PlaceType::Landmark),

        // Cultural / religious landmarks (palaces, castles, churches, monasteries)
        ("AX_Gebaeude", "Schloss") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Burg, Festung") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Kirche") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Kloster") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Synagoge") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Moschee") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Tempel") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Museum") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Theater, Oper") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Konzertgebäude") => Some(PlaceType::Landmark),
        ("AX_Gebaeude", "Krankenhaus") => Some(PlaceType::Hospital),
        ("AX_Gebaeude", "Gebäude für Bildung und Forschung") => Some(PlaceType::University),
        ("AX_Gebaeude", "Windmühle") => Some(PlaceType::Landmark),

        // Functional areas (universities, hospitals)
        ("AX_FlaecheBesondererFunktionalerPraegung", "Bildung und Forschung") => Some(PlaceType::University),
        ("AX_FlaecheBesondererFunktionalerPraegung", "Gesundheit, Kur") => Some(PlaceType::Hospital),

        // Towers (TV towers, observation towers, lighthouses by category)
        ("AX_Turm", _) => Some(PlaceType::Landmark),

        // ── Transport ──────────────────────────────────────────────────
        ("AX_Bahnverkehrsanlage", "Bahnhof") => Some(PlaceType::Station),
        ("AX_Bahnverkehrsanlage", "Haltepunkt") => Some(PlaceType::Station),
        ("AX_Flugverkehr", "Internationaler Flughafen") => Some(PlaceType::Airport),
        ("AX_Flugverkehr", "Regionalflughafen") => Some(PlaceType::Airport),
        ("AX_Flugverkehr", "Verkehrslandeplatz") => Some(PlaceType::Airport),
        ("AX_Flugverkehrsanlage", _) => Some(PlaceType::Airport),

        // ── Natural features ───────────────────────────────────────────
        ("AX_StehendesGewaesser", _) => Some(PlaceType::Lake),
        ("Gewaesser", _) => Some(PlaceType::River),
        ("AX_Wald", _) => Some(PlaceType::Forest),
        ("AX_Moor", _) => Some(PlaceType::Forest),
        ("Besonderer_Hoehenpunkt", _) => Some(PlaceType::Mountain),
        ("AX_Hoehleneingang", _) => Some(PlaceType::Landmark), // cave entrances
        ("AX_Landschaft", _) => Some(PlaceType::Forest),

        // ── Ports / harbours ───────────────────────────────────────────
        ("AX_Hafen", _) => Some(PlaceType::Locality),
        ("AX_Hafenbecken", _) => Some(PlaceType::Locality),

        // ── Nature reserves ────────────────────────────────────────────
        ("AX_NaturUmweltOderBodenschutzrecht", _) => Some(PlaceType::Park),

        // ── Skip (too generic / too dense / already in OSM) ────────────
        // AX_Gebaeude (20K random buildings),
        // AX_Strassenverkehrsanlage (3K road sections),
        // AX_BauwerkImGewaesserbereich (1.5K dams/locks — kept via Schleuse),
        // AX_Verwaltungsgemeinschaft (4.6K admin associations — duplicate of OSM admins),
        // AX_BauwerkOderAnlageFuerIndustrieUndGewerbe (1.8K industrial sites),
        // AX_TagebauGrubeSteinbruch (840 quarries),
        // AX_Schleuse (564 ship locks),
        // AX_BoeschungKliff, AX_DammWallDeich, AX_EinrichtungenFuerDenSchiffsverkehr,
        // AX_Bundesland (16 entries — already covered by OSM relations).
        _ => None,
    }
}

/// Reproject UTM Zone 32 North (EPSG:25832) → WGS84 (EPSG:4326).
/// Returns (lat, lon).
///
/// Implements Karney's series for the inverse transverse Mercator (4-term).
/// Accuracy: < 1 m within zone bounds (5°E to 11°E for UTM32 — wider in
/// practice for GN250's data which extends to ~15°E).
fn utm32_to_wgs84(easting: f64, northing: f64) -> (f64, f64) {
    // WGS84 ellipsoid
    const A: f64 = 6_378_137.0;
    const F: f64 = 1.0 / 298.257_223_563;
    const K0: f64 = 0.9996; // UTM scale factor
    const FALSE_EASTING: f64 = 500_000.0;
    const FALSE_NORTHING: f64 = 0.0; // northern hemisphere
    const ZONE: u32 = 32;
    let lon0 = ((ZONE as f64) * 6.0 - 183.0).to_radians();

    let n = F / (2.0 - F);
    let n2 = n * n;
    let n3 = n2 * n;
    let n4 = n3 * n;

    // Meridional arc constants
    let big_a = A / (1.0 + n) * (1.0 + n2 / 4.0 + n4 / 64.0);

    // Inverse series coefficients (β1..β4)
    let beta1 = 0.5 * n - (2.0 / 3.0) * n2 + (37.0 / 96.0) * n3 - (1.0 / 360.0) * n4;
    let beta2 = (1.0 / 48.0) * n2 + (1.0 / 15.0) * n3 - (437.0 / 1440.0) * n4;
    let beta3 = (17.0 / 480.0) * n3 - (37.0 / 840.0) * n4;
    let beta4 = (4397.0 / 161_280.0) * n4;

    // Corrected coordinates
    let xi = (northing - FALSE_NORTHING) / (big_a * K0);
    let eta = (easting - FALSE_EASTING) / (big_a * K0);

    // Apply inverse series
    let xi1 = xi
        - beta1 * (2.0 * xi).sin() * (2.0 * eta).cosh()
        - beta2 * (4.0 * xi).sin() * (4.0 * eta).cosh()
        - beta3 * (6.0 * xi).sin() * (6.0 * eta).cosh()
        - beta4 * (8.0 * xi).sin() * (8.0 * eta).cosh();
    let eta1 = eta
        - beta1 * (2.0 * xi).cos() * (2.0 * eta).sinh()
        - beta2 * (4.0 * xi).cos() * (4.0 * eta).sinh()
        - beta3 * (6.0 * xi).cos() * (6.0 * eta).sinh()
        - beta4 * (8.0 * xi).cos() * (8.0 * eta).sinh();

    // Conformal latitude
    let chi = (xi1.sin() / eta1.cosh()).asin();

    // Geodetic latitude via series (3-term, accurate to ~mm at GN250 scale)
    let e2 = 2.0 * F - F * F;
    let e4 = e2 * e2;
    let e6 = e4 * e2;
    let e8 = e6 * e2;
    let lat = chi
        + (e2 / 2.0 + 5.0 * e4 / 24.0 + e6 / 12.0 + 13.0 * e8 / 360.0) * (2.0 * chi).sin()
        + (7.0 * e4 / 48.0 + 29.0 * e6 / 240.0 + 811.0 * e8 / 11_520.0) * (4.0 * chi).sin()
        + (7.0 * e6 / 120.0 + 81.0 * e8 / 1_120.0) * (6.0 * chi).sin()
        + (4279.0 * e8 / 161_280.0) * (8.0 * chi).sin();

    let lon = lon0 + (eta1.sinh()).atan2(xi1.cos());

    (lat.to_degrees(), lon.to_degrees())
}

/// Read GN250 records from the official BKG CSV (UTM32s variant).
///
/// File format: BOM-prefixed UTF-8, semicolon-delimited, header row first.
/// We reproject coordinates inline to WGS84.
pub fn read_gn250_places(csv_path: &Path) -> Result<Vec<RawPlace>> {
    let file = std::fs::File::open(csv_path)
        .with_context(|| format!("open {}", csv_path.display()))?;
    let file_size = file.metadata()?.len();
    info!(
        "Parsing GN250 (BKG Geographische Namen): {} ({:.1} MB)...",
        csv_path.display(),
        file_size as f64 / 1_048_576.0
    );

    let reader = std::io::BufReader::new(file);
    use std::io::BufRead;

    let mut out: Vec<RawPlace> = Vec::with_capacity(50_000);
    let mut total_in = 0usize;
    let mut skipped_type = 0usize;
    let mut skipped_geom = 0usize;
    let mut skipped_name = 0usize;
    let mut header_seen = false;

    // Column indices (set after header row)
    let mut idx_nnid = 0usize;
    let mut idx_oba = 2usize;
    let mut idx_oba_wert = 3usize;
    let mut idx_name = 4usize;
    let mut idx_name2 = 7usize;
    let mut idx_ewz = 15usize;
    let mut idx_rechts = 26usize;
    let mut idx_hoch = 27usize;

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };

        // Strip BOM on first line if present
        let stripped = line.strip_prefix('\u{feff}').unwrap_or(&line);

        // Naive split — GN250 doesn't appear to use quoted fields containing
        // semicolons in any meaningful column. The BOX (POLYGON ...) field
        // is the last column and contains commas/parens but no `;`.
        let parts: Vec<&str> = stripped.split(';').collect();

        if !header_seen {
            for (i, h) in parts.iter().enumerate() {
                match *h {
                    "NNID" => idx_nnid = i,
                    "OBA" => idx_oba = i,
                    "OBA_WERT" => idx_oba_wert = i,
                    "NAME" => idx_name = i,
                    "NAME2" => idx_name2 = i,
                    "EWZ" => idx_ewz = i,
                    "RECHTS" => idx_rechts = i,
                    "HOCH" => idx_hoch = i,
                    _ => {}
                }
            }
            header_seen = true;
            continue;
        }

        if parts.len() <= idx_hoch.max(idx_name2) {
            continue;
        }
        total_in += 1;

        let nnid = parts[idx_nnid].trim();
        let oba = parts[idx_oba].trim();
        let oba_wert = parts[idx_oba_wert].trim();

        let place_type = match map_gn250_type(oba, oba_wert) {
            Some(t) => t,
            None => { skipped_type += 1; continue; }
        };

        let name = parts[idx_name].trim();
        if name.is_empty() {
            skipped_name += 1;
            continue;
        }

        let easting: f64 = match parts[idx_rechts].parse() {
            Ok(v) => v,
            Err(_) => { skipped_geom += 1; continue; }
        };
        let northing: f64 = match parts[idx_hoch].parse() {
            Ok(v) => v,
            Err(_) => { skipped_geom += 1; continue; }
        };

        let (lat, lon) = utm32_to_wgs84(easting, northing);

        // Reject coordinates clearly outside Germany — defensive against
        // malformed rows or projection blowups.
        if !(46.5..=55.5).contains(&lat) || !(5.0..=16.0).contains(&lon) {
            skipped_geom += 1;
            continue;
        }

        // Secondary name (different language form)
        let alt_names: Vec<String> = if parts.len() > idx_name2 {
            let n2 = parts[idx_name2].trim();
            if !n2.is_empty() && n2 != name {
                vec![n2.to_owned()]
            } else { Vec::new() }
        } else { Vec::new() };

        // Population (only filled for some categories — keep when parseable)
        let population: Option<u32> = parts.get(idx_ewz)
            .and_then(|s| s.trim().parse::<u32>().ok())
            .filter(|&p| p > 0);

        // Synthetic OSM ID derived from the NNID. Use a stable negative
        // hash so we never collide with real OSM positives — same
        // convention as ssr.rs and dagi.rs.
        let synthetic_id = -((stable_hash(nnid) & 0x7FFF_FFFF_FFFF_FFFF) as i64);

        out.push(RawPlace {
            osm_id: synthetic_id,
            osm_type: OsmType::Node,
            name: name.to_owned(),
            name_intl: Vec::new(),
            alt_names,
            old_names: Vec::new(),
            coord: Coord::new(lat, lon),
            place_type,
            admin_level: None,
            country_code: Some(*b"DE"),
            admin1: None,
            admin2: None,
            population,
            wikidata: None,
        });
    }

    info!(
        "GN250: {} records read, {} kept ({} skipped: {} type-filtered, {} no-geom, {} no-name)",
        total_in,
        out.len(),
        total_in - out.len(),
        skipped_type,
        skipped_geom,
        skipped_name,
    );

    Ok(out)
}

/// Stable 64-bit hash (FNV-1a) for converting GN250 NNID strings into
/// unique synthetic OSM IDs. Matches the helper used by ssr.rs / dagi.rs.
fn stable_hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Merge GN250 places into the existing OSM-derived parquet, dropping
/// near-duplicates (same normalised name within ~111 m). Same algorithm
/// as ssr::merge_ssr_places / dagi::merge_dagi_places.
pub fn merge_gn250_places(existing: &[RawPlace], gn250: &[RawPlace]) -> Vec<RawPlace> {
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

    for place in gn250 {
        let key = (
            (place.coord.lat_f64() / cell_size) as i32,
            (place.coord.lon_f64() / cell_size) as i32,
        );
        let name_lower = place.name.to_lowercase();
        let mut is_dup = false;

        // Check this cell + 8 neighbors for a same-name record within ~110 m.
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
        "GN250 merge: {} kept ({} added new, {} deduped against OSM)",
        merged.len(),
        added,
        deduped,
    );
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    /// UTM32 → WGS84 sanity: real GN250 row coordinates (city centroids).
    /// From rows in `data/data/downloads/de/gn250/GN250.csv`.
    #[test]
    fn utm32_known_points() {
        // Berlin (capital row, AX_Ortslage):
        //   GN250: RECHTS=798865.13, HOCH=5827743.18 → ~52.51N, 13.40E
        let (lat, lon) = utm32_to_wgs84(798865.13, 5827743.18);
        assert!((lat - 52.51).abs() < 0.05, "Berlin lat={}", lat);
        assert!((lon - 13.40).abs() < 0.05, "Berlin lon={}", lon);

        // Munich (Landeshauptstadt row):
        //   GN250: RECHTS=691439.17, HOCH=5335446.50 → ~48.13N, 11.57E
        let (lat, lon) = utm32_to_wgs84(691439.17, 5335446.50);
        assert!((lat - 48.13).abs() < 0.05, "Munich lat={}", lat);
        assert!((lon - 11.57).abs() < 0.05, "Munich lon={}", lon);
    }
}
