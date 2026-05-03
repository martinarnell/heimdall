/// bdtopo.rs — IGN BD TOPO (France) GeoPackage place-name parser
///
/// BD TOPO is the French national topographic database, published yearly by
/// IGN (Institut national de l'information géographique et forestière) as a
/// stack of GeoPackage files (one per département) in Lambert-93 / EPSG:2154.
///
/// Why this matters: OSM's coverage of French *lieux-dits*, hamlets, named
/// natural features (cours d'eau, plans d'eau, sommets) and historic
/// landmarks (châteaux, abbayes, phares) is patchy and inconsistent.
/// BD TOPO is authoritative.
///
/// Source: https://geoservices.ign.fr/bdtopo
/// License: Etalab Open License 2.0
/// Update cadence: ~quarterly per département
///
/// Layers consumed:
///   - zone_d_habitation        — hamlets, lieux-dits habités, villages   (Locality / Village)
///   - lieu_dit_non_habite      — uninhabited locality names              (Locality)
///   - toponymie_lieux_nommes   — named topographic places                (Locality)
///   - detail_orographique      — peaks, cols, mountains                  (Mountain)
///   - cours_d_eau              — named rivers                            (River)
///   - plan_d_eau               — named lakes                             (Lake)
///   - zone_de_vegetation       — named forests                           (Forest)
///   - construction_ponctuelle  — châteaux/églises/etc. (point form)      (Landmark)
///   - construction_surfacique  — same, surface form                      (Landmark)
///
/// Geometries are decoded from GeoPackage Binary blobs (GPKG header + WKB)
/// and reprojected from Lambert-93 to WGS84 inline.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use heimdall_core::types::PlaceType;
use rusqlite::{Connection, OpenFlags};
use tracing::{info, warn};

// ─────────────────────────────────────────────────────────────────────────────
// Release version + zone catalog (used by the streaming downloader)
// ─────────────────────────────────────────────────────────────────────────────

/// IGN BD TOPO version we target. Bump when IGN ships a new major release.
/// (The on-disk URL pattern bakes the version into both the resource path
/// and the archive filename.)
pub const BDTOPO_VERSION: &str = "3-5";

/// Current quarterly editionDate. IGN republishes per-département snapshots
/// roughly every quarter; bump this to pull a newer release. Per-département
/// state in rebuild-state.json compares against this constant — départements
/// already at this date get skipped (no re-download), départements at older
/// dates (or no record) get pulled.
pub const BDTOPO_EDITION_DATE: &str = "2026-03-15";

/// All 101 BD TOPO zones we ingest. Codes are IGN's `gpf_dl:zone term`
/// values verbatim — they slot directly into the URL templates without
/// further normalisation. 96 metropolitan départements (D001..D095, with
/// Corse split into D02A + D02B) plus 5 outre-mer (D971..D974, D976).
/// Saint-Pierre-et-Miquelon (SPM) and Guyane (D973 vs GUF UTM zone) ship
/// in non-Lambert-93 CRS variants we'd need a separate inverse projection
/// for — we skip them here; their places already come from the OSM
/// per-territory PBFs we merge in the extract step.
pub const BDTOPO_DEPARTEMENTS: &[&str] = &[
    "D001","D002","D003","D004","D005","D006","D007","D008","D009","D010",
    "D011","D012","D013","D014","D015","D016","D017","D018","D019","D021",
    "D022","D023","D024","D025","D026","D027","D028","D029","D030","D031",
    "D032","D033","D034","D035","D036","D037","D038","D039","D040","D041",
    "D042","D043","D044","D045","D046","D047","D048","D049","D050","D051",
    "D052","D053","D054","D055","D056","D057","D058","D059","D060","D061",
    "D062","D063","D064","D065","D066","D067","D068","D069","D070","D071",
    "D072","D073","D074","D075","D076","D077","D078","D079","D080","D081",
    "D082","D083","D084","D085","D086","D087","D088","D089","D090","D091",
    "D092","D093","D094","D095",
    // Corse (split into Haute-Corse + Corse-du-Sud — IGN uses 2A/2B
    // letter suffixes, not numeric codes)
    "D02A","D02B",
    // Outre-mer (Lambert-93 variants only — Réunion uses RGR92 UTM 40S
    // and Guadeloupe/Martinique use UTM 20N; IGN still ships these in a
    // _LAMB93_ archive when France-mainland Lambert-93 boundary covers
    // them. Saint-Martin/Saint-Barthélemy ship under D971 in this catalog.)
    "D971","D972","D974","D976",
    // Note: D973 Guyane and D975 Saint-Pierre-et-Miquelon ship in non-
    // Lambert-93 variants only; we skip them (OSM PBF coverage handles
    // their places via the extra_urls in sources.toml).
];

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

/// One BD TOPO place destined for the Heimdall index.
///
/// Mirrors the shape of GN250's per-record output but trimmed to what
/// BD TOPO actually exposes (no population fields, no Wikidata links,
/// no admin levels for non-administrative features).
#[derive(Debug, Clone)]
pub struct BdtopoPlace {
    /// Synthetic negative ID (real OSM IDs are positive). Generated from a
    /// stable hash of the layer name + IGN `cleabs` identifier so cross-run
    /// deduplication remains stable.
    pub osm_id: i64,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    pub place_type: PlaceType,
    pub population: Option<u32>,
    pub admin_level: Option<u8>,
    pub wikidata: Option<String>,
}

/// Lift a BdtopoPlace into the canonical `RawPlace` shape that the
/// rebuild merge path expects. BD TOPO doesn't publish alt-names,
/// translations, old-names, or admin-hierarchy strings, so those are
/// left empty — admin enrichment runs after the merge anyway.
pub fn to_raw_place(p: BdtopoPlace) -> heimdall_core::types::RawPlace {
    heimdall_core::types::RawPlace {
        osm_id: p.osm_id,
        osm_type: heimdall_core::types::OsmType::Node,
        name: p.name,
        name_intl: vec![],
        alt_names: vec![],
        old_names: vec![],
        coord: heimdall_core::types::Coord::new(p.lat, p.lon),
        place_type: p.place_type,
        admin_level: p.admin_level,
        country_code: Some(*b"FR"),
        admin1: None,
        admin2: None,
        population: p.population,
        wikidata: p.wikidata,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Streaming download orchestrator (per-département, change-detected)
// ─────────────────────────────────────────────────────────────────────────────

/// Build the canonical IGN download URL for one département / editionDate.
///
/// IGN's resource scheme bakes both the version and the editionDate into the
/// path AND the archive filename, so the URL pattern is verbose:
///
/// ```text
/// https://data.geopf.fr/telechargement/download/BDTOPO/
///   BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D075_2026-03-15/
///   BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D075_2026-03-15.7z
/// ```
pub fn bdtopo_archive_url(version: &str, dept: &str, edition_date: &str) -> String {
    format!(
        "https://data.geopf.fr/telechargement/download/BDTOPO/\
         BDTOPO_{ver}_TOUSTHEMES_GPKG_LAMB93_{dept}_{date}/\
         BDTOPO_{ver}_TOUSTHEMES_GPKG_LAMB93_{dept}_{date}.7z",
        ver = version,
        dept = dept,
        date = edition_date,
    )
}

/// Stream-and-delete BD TOPO ingestion across all configured départements.
///
/// For each département in `BDTOPO_DEPARTEMENTS`:
///   1. If `state.zones[dept]` already equals `BDTOPO_EDITION_DATE`, skip
///      (cheap change detection — no network, no I/O).
///   2. Otherwise download the .7z to a per-département temp file,
///      extract just the .gpkg via the system `7z` binary, parse it
///      through `read_bdtopo_places`, then delete both the archive and
///      the extracted directory.
///   3. Update `state.zones[dept]` so a crash partway through the catalog
///      preserves progress for the next run.
///
/// Peak disk: one .7z (~300 MB) + one extracted .gpkg (~2 GB) ≈ 3 GB,
/// regardless of how many départements have already been processed.
///
/// Network: one GET per changed département at process pace (no internal
/// rate limiter — the downloader's politeness comes from IGN's own
/// per-second cap on the CDN). Skipped départements cost zero bytes.
pub fn read_bdtopo_streaming(
    cc: &str,
    state_zones: &mut HashMap<String, String>,
    download_dir: &Path,
    skip_download: bool,
) -> Result<Vec<heimdall_core::types::RawPlace>> {
    let target_date = BDTOPO_EDITION_DATE;
    let version = BDTOPO_VERSION;

    let work_dir = download_dir.join("bdtopo-stream");
    std::fs::create_dir_all(&work_dir)
        .with_context(|| format!("create {}", work_dir.display()))?;

    let mut all: Vec<heimdall_core::types::RawPlace> = Vec::new();
    let mut skipped_unchanged = 0usize;
    let mut downloaded = 0usize;
    let mut failed = 0usize;

    for dept in BDTOPO_DEPARTEMENTS {
        // Cheap change detection: same editionDate on disk → no work.
        if state_zones.get(*dept).map(|s| s.as_str()) == Some(target_date) {
            skipped_unchanged += 1;
            continue;
        }

        let archive_path = work_dir.join(format!("{}.7z", dept));
        let extract_dir = work_dir.join(*dept);

        // Defensive cleanup — leftover from a crashed previous run would
        // pollute the .gpkg discovery walk and confuse 7z.
        if extract_dir.exists() {
            let _ = std::fs::remove_dir_all(&extract_dir);
        }

        if !archive_path.exists() {
            if skip_download {
                info!(
                    "[{}] BD TOPO {}: --skip-download set and archive absent — skipping",
                    cc, dept
                );
                failed += 1;
                continue;
            }
            let url = bdtopo_archive_url(version, dept, target_date);
            info!("[{}] BD TOPO {}: downloading {}", cc, dept, target_date);
            if let Err(e) = download_to_path(&url, &archive_path) {
                warn!("[{}] BD TOPO {}: download failed ({}) — skipping", cc, dept, e);
                let _ = std::fs::remove_file(&archive_path);
                failed += 1;
                continue;
            }
        }

        std::fs::create_dir_all(&extract_dir)
            .with_context(|| format!("create {}", extract_dir.display()))?;

        if let Err(e) = extract_7z(&archive_path, &extract_dir) {
            warn!("[{}] BD TOPO {}: extract failed ({}) — skipping", cc, dept, e);
            let _ = std::fs::remove_dir_all(&extract_dir);
            let _ = std::fs::remove_file(&archive_path);
            failed += 1;
            continue;
        }

        let gpkg = match find_first_gpkg(&extract_dir) {
            Some(p) => p,
            None => {
                warn!("[{}] BD TOPO {}: no .gpkg in archive — skipping", cc, dept);
                let _ = std::fs::remove_dir_all(&extract_dir);
                let _ = std::fs::remove_file(&archive_path);
                failed += 1;
                continue;
            }
        };

        let added = match read_bdtopo_places(&gpkg) {
            Ok(places) => {
                let n = places.len();
                all.extend(places.into_iter().map(to_raw_place));
                n
            }
            Err(e) => {
                warn!("[{}] BD TOPO {}: parse failed ({}) — skipping", cc, dept, e);
                let _ = std::fs::remove_dir_all(&extract_dir);
                let _ = std::fs::remove_file(&archive_path);
                failed += 1;
                continue;
            }
        };

        // Free disk before moving to the next département. Cheap on local
        // FS; sequencing prevents the 96-département catalogue from
        // accumulating ~180 GB of extracted .gpkg files.
        let _ = std::fs::remove_dir_all(&extract_dir);
        let _ = std::fs::remove_file(&archive_path);

        // Persist progress per département so a mid-run crash doesn't
        // throw away everything we already paid to download.
        state_zones.insert((*dept).to_owned(), target_date.to_owned());
        downloaded += 1;
        info!("[{}] BD TOPO {}: +{} places", cc, dept, added);
    }

    // Clean up the work directory itself (empty by now). Leave it on
    // failure so a re-run can resume without re-downloading anything
    // that's still on disk from a crash mid-extract.
    if failed == 0 {
        let _ = std::fs::remove_dir_all(&work_dir);
    }

    info!(
        "[{}] BD TOPO streaming: {} downloaded, {} unchanged (skipped), {} failed → {} places",
        cc, downloaded, skipped_unchanged, failed, all.len()
    );
    Ok(all)
}

/// Streaming download via blocking ureq. Writes directly to disk to keep
/// memory bounded — the larger département archives are 500 MB+. Uses
/// ureq (already a dependency for DVV/BAN) rather than reqwest's blocking
/// feature so we don't pull in a second HTTP runtime.
fn download_to_path(url: &str, dest: &Path) -> Result<()> {
    let tmp = dest.with_extension("7z.partial");
    let resp = ureq::get(url).call()
        .with_context(|| format!("GET {}", url))?;
    if resp.status() < 200 || resp.status() >= 300 {
        anyhow::bail!("HTTP {} for {}", resp.status(), url);
    }
    let mut reader = resp.into_reader();
    let mut file = std::fs::File::create(&tmp)
        .with_context(|| format!("create {}", tmp.display()))?;
    std::io::copy(&mut reader, &mut file)
        .with_context(|| format!("write {}", tmp.display()))?;
    drop(file);
    std::fs::rename(&tmp, dest)
        .with_context(|| format!("rename {} → {}", tmp.display(), dest.display()))?;
    Ok(())
}

/// Shell out to `7z x` for extraction. The .7z LZMA2 format isn't
/// well-supported by the pure-Rust archive crates we already pull in,
/// and IGN ships nothing else for BD TOPO. Mirrors the `osmium merge`
/// shell-out we use for Geofabrik realm builds.
fn extract_7z(archive: &Path, dest_dir: &Path) -> Result<()> {
    let status = std::process::Command::new("7z")
        .arg("x")
        .arg("-y")          // assume "yes" on overwrite prompts
        .arg(format!("-o{}", dest_dir.display()))
        .arg(archive)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .with_context(|| "spawn 7z (is p7zip-full installed?)")?;
    if !status.success() {
        anyhow::bail!("7z exited with status {}", status);
    }
    Ok(())
}

/// Walk a directory recursively looking for the first `.gpkg` file.
/// IGN's archives nest the .gpkg ~5 levels deep behind documentation
/// PDFs and metadata directories.
fn find_first_gpkg(start: &Path) -> Option<PathBuf> {
    fn walk(dir: &Path) -> Option<PathBuf> {
        let entries = std::fs::read_dir(dir).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |e| e.eq_ignore_ascii_case("gpkg")) {
                return Some(path);
            }
            if path.is_dir() {
                if let Some(found) = walk(&path) {
                    return Some(found);
                }
            }
        }
        None
    }
    walk(start)
}

// ─────────────────────────────────────────────────────────────────────────────
// Top-level entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Read all relevant place layers from a BD TOPO GeoPackage.
pub fn read_bdtopo_places(gpkg_path: &Path) -> Result<Vec<BdtopoPlace>> {
    info!("Reading BD TOPO places from {}", gpkg_path.display());

    let conn = Connection::open_with_flags(gpkg_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open GeoPackage {}", gpkg_path.display()))?;

    let mut out: Vec<BdtopoPlace> = Vec::with_capacity(200_000);

    // Plain locality/place layers — name from `nom` family fields, type fixed.
    let locality_layers: &[(&str, PlaceType)] = &[
        ("zone_d_habitation",      PlaceType::Locality),
        ("lieu_dit_non_habite",    PlaceType::Locality),
        ("toponymie_lieux_nommes", PlaceType::Locality),
        ("detail_orographique",    PlaceType::Mountain),
        ("cours_d_eau",            PlaceType::River),
        ("plan_d_eau",             PlaceType::Lake),
        ("zone_de_vegetation",     PlaceType::Forest),
    ];

    for &(layer, pt) in locality_layers {
        match read_layer(&conn, layer, pt, /* filter_constructions = */ false) {
            Ok(rows) => {
                info!("BD TOPO {}: {} places", layer, rows.len());
                out.extend(rows);
            }
            Err(e) => {
                warn!("BD TOPO {}: skipped ({})", layer, e);
            }
        }
    }

    // Construction layers — keep only "interesting" natures (churches,
    // castles, lighthouses, ...). All other rows (generic buildings, sheds,
    // billboards) are dropped.
    for layer in &["construction_ponctuelle", "construction_surfacique"] {
        match read_layer(&conn, layer, PlaceType::Landmark, /* filter_constructions = */ true) {
            Ok(rows) => {
                info!("BD TOPO {}: {} landmarks", layer, rows.len());
                out.extend(rows);
            }
            Err(e) => {
                warn!("BD TOPO {}: skipped ({})", layer, e);
            }
        }
    }

    info!("BD TOPO total: {} places", out.len());
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Layer reader
// ─────────────────────────────────────────────────────────────────────────────

fn read_layer(
    conn: &Connection,
    table: &str,
    place_type: PlaceType,
    filter_constructions: bool,
) -> Result<Vec<BdtopoPlace>> {
    // Skip silently if the table just isn't present in this département file.
    let exists: bool = conn.query_row(
        "SELECT count(*) > 0 FROM sqlite_master WHERE type='table' AND name=?1",
        [table],
        |row| row.get(0),
    )?;
    if !exists {
        return Ok(Vec::new());
    }

    // Look up the geometry column from gpkg_geometry_columns. Fall back to
    // the conventional `geom` if the metadata table is missing or empty.
    let geom_col = conn
        .query_row(
            "SELECT column_name FROM gpkg_geometry_columns WHERE table_name = ?1",
            [table],
            |row| row.get::<_, String>(0),
        )
        .unwrap_or_else(|_| "geom".to_string());

    let columns = get_table_columns(conn, table)?;

    // Pick the best name column available, in preference order.
    let name_candidates = [
        "nom_principal",
        "toponyme",
        "nom",
        "nom_1",
        "nom_officiel",
        "lib",
    ];
    let name_col = name_candidates
        .iter()
        .find(|c| columns.iter().any(|x| x == *c))
        .copied();
    let Some(name_col) = name_col else {
        // No name column — nothing to index from this table.
        return Ok(Vec::new());
    };

    let has_cleabs = columns.iter().any(|c| c == "cleabs");
    let has_nature = columns.iter().any(|c| c == "nature");
    let has_pop = columns.iter().any(|c| c == "population");

    // Build SELECT — quote everything that might shadow keywords.
    let id_expr  = if has_cleabs { "cleabs"  } else { "rowid"  };
    let nat_expr = if has_nature { "nature"  } else { "NULL"   };
    let pop_expr = if has_pop    { "population" } else { "NULL" };

    let sql = format!(
        "SELECT \"{id}\", \"{name}\", {nat}, {pop}, \"{geom}\" FROM \"{tbl}\"",
        id   = id_expr,
        name = name_col,
        nat  = nat_expr,
        pop  = pop_expr,
        geom = geom_col,
        tbl  = table,
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut out: Vec<BdtopoPlace> = Vec::new();
    let mut skipped_name = 0usize;
    let mut skipped_geom = 0usize;
    let mut skipped_nature = 0usize;

    let rows = stmt.query_map([], |row| {
        let id_str: String = match row.get_ref(0)? {
            rusqlite::types::ValueRef::Text(b) => String::from_utf8_lossy(b).into_owned(),
            rusqlite::types::ValueRef::Integer(i) => i.to_string(),
            rusqlite::types::ValueRef::Real(f) => f.to_string(),
            _ => String::new(),
        };
        let name: Option<String> = row.get(1).ok();
        let nature: Option<String> = row.get(2).ok();
        let population: Option<i64> = row.get(3).ok();
        let geom: Option<Vec<u8>> = row.get(4).ok();
        Ok((id_str, name, nature, population, geom))
    })?;

    for r in rows {
        let (id_str, name, nature, population, geom) = match r {
            Ok(v) => v,
            Err(_) => continue,
        };

        let name = match name.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
            Some(n) => n,
            None => { skipped_name += 1; continue; }
        };
        if is_junk_name(&name) {
            skipped_name += 1;
            continue;
        }

        // Construction layers: keep only culturally / geographically notable
        // buildings. Drop anything else.
        let pt = if filter_constructions {
            match nature.as_deref().map(map_construction_nature) {
                Some(Some(t)) => t,
                _ => { skipped_nature += 1; continue; }
            }
        } else {
            // For zone_d_habitation, promote rows whose `nature` indicates
            // an inhabited village rather than a generic locality name.
            match (table, nature.as_deref()) {
                ("zone_d_habitation", Some(n)) if is_village_nature(n) => PlaceType::Village,
                _ => place_type,
            }
        };

        let geom = match geom {
            Some(g) => g,
            None => { skipped_geom += 1; continue; }
        };

        let (x, y) = match parse_gpkg_geom_centroid(&geom) {
            Some(c) => c,
            None => { skipped_geom += 1; continue; }
        };

        let (lat, lon) = lambert93_to_wgs84(x, y);
        // Defensive bounds: metropolitan France + DOM-TOM in Lambert-93 is
        // really only continental France; if a département file is for a
        // territory using a different CRS we'll generate garbage. Reject
        // anything outside a generous mainland-France envelope.
        if !(41.0..=51.5).contains(&lat) || !(-5.5..=10.0).contains(&lon) {
            skipped_geom += 1;
            continue;
        }

        out.push(BdtopoPlace {
            osm_id: synth_id(table, &id_str),
            name,
            lat,
            lon,
            place_type: pt,
            population: population
                .filter(|&p| p > 0 && p <= u32::MAX as i64)
                .map(|p| p as u32),
            admin_level: None,
            wikidata: None,
        });
    }

    if skipped_name + skipped_geom + skipped_nature > 0 {
        info!(
            "BD TOPO {}: skipped {} no-name, {} no-geom, {} unwanted-nature",
            table, skipped_name, skipped_geom, skipped_nature,
        );
    }

    Ok(out)
}

fn get_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{}\")", table))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut cols = Vec::new();
    for r in rows {
        if let Ok(c) = r {
            cols.push(c);
        }
    }
    Ok(cols)
}

// ─────────────────────────────────────────────────────────────────────────────
// Nature filters
// ─────────────────────────────────────────────────────────────────────────────

/// Only keep `construction_*` rows whose `nature` describes a culturally or
/// geographically significant landmark. Returns Some(landmark type) when
/// kept, None to drop.
fn map_construction_nature(nature: &str) -> Option<PlaceType> {
    // Match case-insensitively; IGN stores TitleCase but trim+lowercase the
    // input to avoid surprises with stray whitespace.
    let n = nature.trim();
    let lower = n.to_lowercase();
    match lower.as_str() {
        "château" | "chateau"                            => Some(PlaceType::Landmark),
        "église" | "eglise"                              => Some(PlaceType::Landmark),
        "cathédrale" | "cathedrale"                      => Some(PlaceType::Landmark),
        "abbaye"                                          => Some(PlaceType::Landmark),
        "tour"                                            => Some(PlaceType::Landmark),
        "phare"                                           => Some(PlaceType::Landmark),
        "moulin"                                          => Some(PlaceType::Landmark),
        "chapelle"                                        => Some(PlaceType::Landmark),
        "monastère" | "monastere"                        => Some(PlaceType::Landmark),
        "temple"                                          => Some(PlaceType::Landmark),
        "mosquée" | "mosquee"                            => Some(PlaceType::Landmark),
        "synagogue"                                       => Some(PlaceType::Landmark),
        _ => None,
    }
}

fn is_village_nature(nature: &str) -> bool {
    let lower = nature.trim().to_lowercase();
    matches!(
        lower.as_str(),
        "village" | "bourg" | "ville" | "hameau"
    )
}

fn is_junk_name(name: &str) -> bool {
    // BD TOPO occasionally exports placeholder strings — drop them.
    let n = name.trim();
    if n.is_empty() {
        return true;
    }
    let lower = n.to_lowercase();
    matches!(
        lower.as_str(),
        "sans nom" | "sans toponyme" | "indéterminé" | "indetermine" | "n/a" | "null"
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Synthetic IDs
// ─────────────────────────────────────────────────────────────────────────────

/// Generate a stable negative ID from `(layer, cleabs)`. Uses FNV-1a and
/// masks the high bit so the hashed value fits in i63 before negation.
/// We bias by a fixed offset so IDs don't collide with SSR/DAGI/GN250.
fn synth_id(layer: &str, key: &str) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in layer.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= b':' as u64;
    h = h.wrapping_mul(0x100000001b3);
    for b in key.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let positive = (h & 0x7FFF_FFFF_FFFF_FFFF) as i64;
    // Offset start: -10_000_000 minimum (per task spec). FNV output is large
    // so wrap that into a guaranteed-large negative range.
    let span: i64 = i64::MAX - 10_000_000;
    -(10_000_000 + positive % span)
}

// ─────────────────────────────────────────────────────────────────────────────
// GeoPackage Binary geometry parser
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a GeoPackage Binary blob and return the centroid of its geometry
/// in *source* CRS coordinates (Lambert-93 metres for BD TOPO). Returns
/// None on any malformed / unsupported input.
fn parse_gpkg_geom_centroid(blob: &[u8]) -> Option<(f64, f64)> {
    // GPKG header: 'G','P', version, flags, srs_id (4 bytes), envelope (var)
    if blob.len() < 8 {
        return None;
    }
    if &blob[0..2] != b"GP" {
        return None;
    }
    let _version = blob[2];
    let flags = blob[3];

    // bit 0: envelope endianness for SRS_ID + envelope
    let header_le = (flags & 0x01) != 0;
    // bits 1..=3: envelope type
    let env_type = (flags >> 1) & 0x07;
    let envelope_size = match env_type {
        0 => 0usize,
        1 => 32, // [minx, maxx, miny, maxy]
        2 | 3 => 48, // + min/max z or m
        4 => 64, // + min/max z and m
        _ => return None,
    };
    // bit 5: empty geometry
    let empty = (flags & 0x10) != 0;
    if empty {
        return None;
    }

    let header_len = 8 + envelope_size;
    if blob.len() < header_len + 5 {
        return None;
    }

    // header_le tells us how to interpret SRS_ID, but we don't need it.
    let _srs_id = read_u32(&blob[4..8], header_le);

    let wkb = &blob[header_len..];
    parse_wkb_centroid(wkb)
}

/// Parse a (possibly nested) WKB geometry and return the centroid (mean of
/// vertex coordinates).
fn parse_wkb_centroid(wkb: &[u8]) -> Option<(f64, f64)> {
    let mut sum_x = 0.0f64;
    let mut sum_y = 0.0f64;
    let mut count = 0usize;
    walk_wkb(wkb, &mut sum_x, &mut sum_y, &mut count)?;
    if count == 0 {
        return None;
    }
    Some((sum_x / count as f64, sum_y / count as f64))
}

/// Recurse through a WKB geometry, accumulating coordinate sums and a
/// vertex count. Returns Some(bytes_consumed) on success.
fn walk_wkb(
    wkb: &[u8],
    sum_x: &mut f64,
    sum_y: &mut f64,
    count: &mut usize,
) -> Option<usize> {
    if wkb.len() < 5 {
        return None;
    }
    let endian_byte = wkb[0];
    let le = endian_byte == 1;
    let mut raw_type = read_u32(&wkb[1..5], le);
    // Strip ISO WKB Z/M/ZM bits (2.5d/measure variants), keep base type.
    // EWKB also encodes flags in high bits.
    let has_z = (raw_type & 0x80000000) != 0 || (raw_type / 1000) % 10 == 1 || (raw_type / 1000) % 10 == 3;
    let has_m = (raw_type & 0x40000000) != 0 || (raw_type / 1000) % 10 == 2 || (raw_type / 1000) % 10 == 3;
    raw_type &= 0x0FFFFFFF;
    raw_type %= 1000;
    let dims = 2 + has_z as usize + has_m as usize;

    let mut off = 5usize;
    match raw_type {
        1 => {
            // Point
            if wkb.len() < off + 8 * dims { return None; }
            let x = read_f64(&wkb[off..off + 8], le);
            let y = read_f64(&wkb[off + 8..off + 16], le);
            *sum_x += x;
            *sum_y += y;
            *count += 1;
            off += 8 * dims;
        }
        2 => {
            // LineString
            if wkb.len() < off + 4 { return None; }
            let n = read_u32(&wkb[off..off + 4], le) as usize;
            off += 4;
            if wkb.len() < off + n * 8 * dims { return None; }
            for _ in 0..n {
                let x = read_f64(&wkb[off..off + 8], le);
                let y = read_f64(&wkb[off + 8..off + 16], le);
                *sum_x += x;
                *sum_y += y;
                *count += 1;
                off += 8 * dims;
            }
        }
        3 => {
            // Polygon — sum vertices across all rings.
            if wkb.len() < off + 4 { return None; }
            let n_rings = read_u32(&wkb[off..off + 4], le) as usize;
            off += 4;
            for _ in 0..n_rings {
                if wkb.len() < off + 4 { return None; }
                let n = read_u32(&wkb[off..off + 4], le) as usize;
                off += 4;
                if wkb.len() < off + n * 8 * dims { return None; }
                for _ in 0..n {
                    let x = read_f64(&wkb[off..off + 8], le);
                    let y = read_f64(&wkb[off + 8..off + 16], le);
                    *sum_x += x;
                    *sum_y += y;
                    *count += 1;
                    off += 8 * dims;
                }
            }
        }
        4 | 5 | 6 | 7 => {
            // MultiPoint / MultiLineString / MultiPolygon / GeometryCollection
            if wkb.len() < off + 4 { return None; }
            let n = read_u32(&wkb[off..off + 4], le) as usize;
            off += 4;
            for _ in 0..n {
                let consumed = walk_wkb(&wkb[off..], sum_x, sum_y, count)?;
                off += consumed;
            }
        }
        _ => return None,
    }
    Some(off)
}

#[inline]
fn read_u32(b: &[u8], le: bool) -> u32 {
    let arr = [b[0], b[1], b[2], b[3]];
    if le { u32::from_le_bytes(arr) } else { u32::from_be_bytes(arr) }
}

#[inline]
fn read_f64(b: &[u8], le: bool) -> f64 {
    let arr = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]];
    if le { f64::from_le_bytes(arr) } else { f64::from_be_bytes(arr) }
}

// ─────────────────────────────────────────────────────────────────────────────
// Lambert-93 (EPSG:2154) → WGS84 (EPSG:4326) inverse projection
// ─────────────────────────────────────────────────────────────────────────────

/// Inverse Lambert Conformal Conic with two standard parallels.
/// Snyder eqs. 15-1 .. 15-3, with iterative latitude (eq. 3-4).
/// Returns (lat_deg, lon_deg).
fn lambert93_to_wgs84(x: f64, y: f64) -> (f64, f64) {
    // GRS80 ellipsoid
    const A: f64 = 6_378_137.0;
    const E2: f64 = 0.006_694_380_022_90;
    let e = E2.sqrt();

    // Defining parameters for EPSG:2154 (Lambert-93)
    let lat0 = 46.5_f64.to_radians();
    let lon0 = 3.0_f64.to_radians();
    let lat1 = 44.0_f64.to_radians();
    let lat2 = 49.0_f64.to_radians();
    const FE: f64 = 700_000.0;
    const FN: f64 = 6_600_000.0;

    // t(φ) per Snyder eq. 15-9 — equivalent forms:
    //   tan(π/4 − φ/2) · ((1 + e·sinφ)/(1 − e·sinφ))^(e/2)
    //   = tan(π/4 − φ/2) / ((1 − e·sinφ)/(1 + e·sinφ))^(e/2)
    let t = |phi: f64| -> f64 {
        let s = phi.sin();
        let term = ((1.0 + e * s) / (1.0 - e * s)).powf(e / 2.0);
        ((std::f64::consts::FRAC_PI_4 - phi / 2.0).tan()) * term
    };

    let m = |phi: f64| -> f64 {
        let s = phi.sin();
        phi.cos() / (1.0 - E2 * s * s).sqrt()
    };

    let m1 = m(lat1);
    let m2 = m(lat2);
    let t0 = t(lat0);
    let t1 = t(lat1);
    let t2 = t(lat2);

    let n = (m1.ln() - m2.ln()) / (t1.ln() - t2.ln());
    let big_f = m1 / (n * t1.powf(n));
    let rho0 = A * big_f * t0.powf(n);

    let dx = x - FE;
    let dy = rho0 - (y - FN);
    let rho = dx.hypot(dy).copysign(n);
    let theta = (dx / dy).atan();

    let t_calc = (rho / (A * big_f)).powf(1.0 / n);

    // Iterative latitude (Snyder eq. 3-4)
    let mut phi = std::f64::consts::FRAC_PI_2 - 2.0 * t_calc.atan();
    for _ in 0..30 {
        let s = phi.sin();
        let factor = ((1.0 - e * s) / (1.0 + e * s)).powf(e / 2.0);
        let phi_new = std::f64::consts::FRAC_PI_2 - 2.0 * (t_calc * factor).atan();
        if (phi_new - phi).abs() < 1e-11 {
            phi = phi_new;
            break;
        }
        phi = phi_new;
    }

    let lambda = theta / n + lon0;
    (phi.to_degrees(), lambda.to_degrees())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Lambert-93 → WGS84 sanity: Paris-Notre-Dame.
    /// The point (652000, 6862500) lies a few hundred metres NW of the
    /// cathedral itself (~48.853°N, 2.350°E in WGS84), so we accept a
    /// loose 0.02° tolerance — that's 1 km, well within the "approximate"
    /// reference point. The tighter precision check is `lambert93_origin`
    /// below, which round-trips the projection origin exactly.
    #[test]
    fn lambert93_paris_notre_dame() {
        let (lat, lon) = lambert93_to_wgs84(652_000.0, 6_862_500.0);
        assert!((lat - 48.853).abs() < 0.02, "Paris lat = {} (expected ~48.853)", lat);
        assert!((lon -  2.350).abs() < 0.02, "Paris lon = {} (expected ~2.350)",  lon);
    }

    /// Sanity-check projection scale: stepping 1000 m east in Lambert-93
    /// near Paris should yield ~0.0136° (1000 m / (cos(48.86°) · 111320 m)).
    /// This catches sign / scale errors without needing an external proj
    /// reference value.
    #[test]
    fn lambert93_east_step_scale() {
        let (lat0, lon0) = lambert93_to_wgs84(652_000.0, 6_862_500.0);
        let (lat1, lon1) = lambert93_to_wgs84(653_000.0, 6_862_500.0);
        let dlat = lat1 - lat0;
        let dlon = lon1 - lon0;
        // 1 km east → ~0° change in lat, ~0.0136° change in lon at 48.86°.
        assert!(dlat.abs() < 1e-3, "1 km east shifted lat by {}°", dlat);
        let expected_dlon = 1000.0 / (lat0.to_radians().cos() * 111_320.0);
        assert!(
            (dlon - expected_dlon).abs() < 5e-5,
            "1 km east → dlon={} (expected ~{})", dlon, expected_dlon,
        );
    }

    /// Lambert-93 → WGS84 round-trip on the projection origin: x=FE, y=FN
    /// must produce the reference latitude and longitude (46.5°N, 3.0°E).
    #[test]
    fn lambert93_origin() {
        let (lat, lon) = lambert93_to_wgs84(700_000.0, 6_600_000.0);
        assert!((lat - 46.5).abs() < 1e-6, "origin lat = {}", lat);
        assert!((lon - 3.0).abs() < 1e-6, "origin lon = {}", lon);
    }

    /// Build a tiny GPKG blob that wraps a WKB Point and confirm the
    /// header parser plus WKB walker produce the embedded coordinates.
    #[test]
    fn parse_gpkg_point() {
        let mut blob: Vec<u8> = Vec::new();
        blob.extend_from_slice(b"GP");
        blob.push(0); // version
        blob.push(0x01); // flags: little-endian, no envelope, not empty
        blob.extend_from_slice(&2154u32.to_le_bytes()); // SRS_ID
        // No envelope — env_type=0.
        // WKB Point (LE), type=1, x=652000, y=6862500
        blob.push(0x01);
        blob.extend_from_slice(&1u32.to_le_bytes());
        blob.extend_from_slice(&652_000.0_f64.to_le_bytes());
        blob.extend_from_slice(&6_862_500.0_f64.to_le_bytes());

        let (x, y) = parse_gpkg_geom_centroid(&blob).expect("parse failed");
        assert!((x - 652_000.0).abs() < 1e-6, "x = {}", x);
        assert!((y - 6_862_500.0).abs() < 1e-6, "y = {}", y);
    }

    /// WKB Point parsed directly (no GPKG header).
    #[test]
    fn parse_wkb_point() {
        let mut wkb: Vec<u8> = Vec::new();
        wkb.push(0x01);                                  // little-endian
        wkb.extend_from_slice(&1u32.to_le_bytes());      // geometry type = Point
        wkb.extend_from_slice(&3.0_f64.to_le_bytes());   // x
        wkb.extend_from_slice(&4.0_f64.to_le_bytes());   // y

        let (x, y) = parse_wkb_centroid(&wkb).expect("parse failed");
        assert!((x - 3.0).abs() < 1e-12);
        assert!((y - 4.0).abs() < 1e-12);
    }

    /// WKB Polygon (1 ring, 4 vertices forming a square): centroid is the
    /// mean of the vertices, not the geometric centroid — that's by
    /// design (cheap, good enough for hamlet pinpointing).
    #[test]
    fn parse_wkb_polygon_centroid() {
        let mut wkb: Vec<u8> = Vec::new();
        wkb.push(0x01);
        wkb.extend_from_slice(&3u32.to_le_bytes()); // Polygon
        wkb.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
        wkb.extend_from_slice(&5u32.to_le_bytes()); // 5 points (closed ring)
        // (0,0), (10,0), (10,10), (0,10), (0,0)
        let pts: [(f64, f64); 5] = [
            (0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0),
        ];
        for (x, y) in pts {
            wkb.extend_from_slice(&x.to_le_bytes());
            wkb.extend_from_slice(&y.to_le_bytes());
        }
        let (cx, cy) = parse_wkb_centroid(&wkb).expect("parse failed");
        // Mean of vertices: x = (0+10+10+0+0)/5 = 4, y = (0+0+10+10+0)/5 = 4
        assert!((cx - 4.0).abs() < 1e-9, "cx = {}", cx);
        assert!((cy - 4.0).abs() < 1e-9, "cy = {}", cy);
    }

    /// Construction `nature` filter keeps churches/castles, drops sheds.
    #[test]
    fn nature_filter() {
        assert!(map_construction_nature("Château").is_some());
        assert!(map_construction_nature("Église").is_some());
        assert!(map_construction_nature("eglise").is_some());
        assert!(map_construction_nature("Phare").is_some());
        assert!(map_construction_nature("Hangar").is_none());
        assert!(map_construction_nature("Bâtiment").is_none());
        assert!(map_construction_nature("Réservoir").is_none());
    }

    /// Synthetic IDs are negative, deterministic, distinct across layers.
    #[test]
    fn synth_id_properties() {
        let a = synth_id("zone_d_habitation", "TOPO000123");
        let b = synth_id("zone_d_habitation", "TOPO000123");
        let c = synth_id("lieu_dit_non_habite", "TOPO000123");
        let d = synth_id("zone_d_habitation", "TOPO000124");
        assert!(a < 0);
        assert!(a <= -10_000_000);
        assert_eq!(a, b, "same key/layer must hash equal");
        assert_ne!(a, c, "different layer must hash differently");
        assert_ne!(a, d, "different key must hash differently");
    }

    /// URL builder matches the IGN download path verbatim. Regression
    /// guard — IGN's URLs bake both the version and the date into the
    /// path AND filename, so a typo in one place silently 404s.
    #[test]
    fn archive_url_paris_3_5_2026_03_15() {
        let got = bdtopo_archive_url("3-5", "D075", "2026-03-15");
        let want = "https://data.geopf.fr/telechargement/download/BDTOPO/\
                    BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D075_2026-03-15/\
                    BDTOPO_3-5_TOUSTHEMES_GPKG_LAMB93_D075_2026-03-15.7z";
        assert_eq!(got, want);
    }

    /// Catalog sanity: 101 zones, no duplicates, all distinct codes.
    /// Regression guard for hand-edited insertions.
    #[test]
    fn departement_catalog_invariants() {
        let zones = BDTOPO_DEPARTEMENTS;
        // Catalog is the 96 metro (numeric) + Corse (2A/2B) + 4 outre-mer
        // we currently support in Lambert-93 = 100. Skipping D973 + D975
        // which IGN only ships in non-Lambert-93 CRS variants; their
        // places come from the OSM PBFs we merge in extract.
        // (D020 is also intentionally absent — the Corse renumbering
        // gave us 2A/2B in its place.)
        assert_eq!(
            zones.len(),
            100,
            "BDTOPO catalog drifted from the documented 100-zone shape"
        );
        let mut set: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        for z in zones {
            assert!(z.starts_with('D'), "zone code must start with D: {}", z);
            assert!(
                set.insert(z),
                "duplicate zone code in catalog: {}",
                z
            );
        }
        // Spot-check a handful of well-known codes
        assert!(set.contains("D075"), "Paris missing");
        assert!(set.contains("D013"), "Bouches-du-Rhône missing");
        assert!(set.contains("D02A"), "Corse-du-Sud missing");
        assert!(set.contains("D02B"), "Haute-Corse missing");
        assert!(set.contains("D974"), "La Réunion missing");
    }
}
