/// heimdall — geocoder server with fetch + serve subcommands
///
/// heimdall fetch se no        # download prebuilt indices
/// heimdall fetch nordic       # download Nordic bundle
/// heimdall serve              # start server (auto-discovers indices)
/// heimdall serve --country se,no  # load specific countries

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::path::{Path, PathBuf};
use std::time::Instant;
use clap::{Parser, Subcommand};

use heimdall_core::index::HeimdallIndex;
use heimdall_core::types::{BoundingBox, GeoQuery, GeoResult, MatchType, PlaceType};
use heimdall_core::addr_index::{AddressIndex, parse_address_query, parse_street_query, parse_street_city_freeform};
use heimdall_core::types::Coord;
use heimdall_core::zip_index::ZipIndex;
use heimdall_core::reverse::GeohashIndex;
use heimdall_core::global_index::GlobalIndex;
use heimdall_normalize::Normalizer;

mod fetch;
mod manifest;
mod metrics;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "heimdall", about = "Heimdall geocoder — compact, fast, Nominatim-compatible")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Download prebuilt index data
    Fetch {
        /// Country codes or bundle names (se, no, nordic, europe, world)
        targets: Vec<String>,

        /// List available indices and bundles
        #[arg(long)]
        list: bool,

        /// Check for updates to local indices
        #[arg(long)]
        update: bool,

        /// Base URL for downloads
        #[arg(long, env = "HEIMDALL_MIRROR")]
        mirror: Option<String>,

        /// Index storage directory
        #[arg(long, env = "HEIMDALL_DATA")]
        data_dir: Option<PathBuf>,
    },

    /// Start the geocoder server
    Serve {
        /// Explicit index directory paths (can be repeated)
        #[arg(short, long, num_args = 1..)]
        index: Vec<PathBuf>,

        /// Country codes to load — auto-discovers in data dirs
        #[arg(long, value_delimiter = ',')]
        country: Vec<String>,

        /// Bind address
        #[arg(long, default_value = "127.0.0.1:2399")]
        bind: String,

        /// Additional index search directories
        #[arg(long)]
        data_dir: Vec<PathBuf>,
    },
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

struct CountryIndex {
    code: [u8; 2],
    name: String,
    index: HeimdallIndex,
    addr_index: Option<AddressIndex>,
    zip_index: Option<ZipIndex>,
    geohash_index: Option<GeohashIndex>,
    normalizer: Normalizer,
    #[allow(dead_code)]
    bbox: BoundingBox,
    meta: CountryMeta,
}

struct CountryMeta {
    places: usize,
    addresses: usize,
    index_size_bytes: u64,
    load_time_ms: u64,
}

/// Mapping from (osm_type_char, osm_id) -> (country_index, record_id)
/// Used by /lookup?osm_ids= to resolve OSM references.
type OsmIdMap = HashMap<(char, u32), (usize, u32)>;

struct AppState {
    countries: Vec<CountryIndex>,
    global_index: Option<GlobalIndex>,
    #[allow(dead_code)]
    country_id_map: HashMap<String, usize>,
    osm_id_map: std::sync::OnceLock<OsmIdMap>,
    started_at: std::time::SystemTime,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: Option<String>,
    amenity: Option<String>,
    street: Option<String>,
    city: Option<String>,
    county: Option<String>,
    state: Option<String>,
    country: Option<String>,
    postalcode: Option<String>,
    countrycodes: Option<String>,
    viewbox: Option<String>,
    #[serde(default)]
    bounded: u8,
    #[serde(default = "default_format")]
    #[allow(dead_code)]
    format: String,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    addressdetails: u8,
}

fn default_format() -> String { "json".to_owned() }
fn default_limit() -> usize { 5 }
fn default_autocomplete_limit() -> usize { 5 }

/// Returns true if any structured query parameter (amenity, street, city,
/// county, state, postalcode, country) is present.
fn has_structured_params(p: &SearchParams) -> bool {
    p.amenity.is_some()
        || p.street.is_some()
        || p.city.is_some()
        || p.county.is_some()
        || p.state.is_some()
        || p.postalcode.is_some()
        || p.country.is_some()
}

/// Build a synthetic free-text query from structured parameters.
/// Fields are concatenated in Nominatim order:
///   amenity, street, city, county, state, postalcode, country
/// Returns `None` when no structured params are present.
fn parse_structured_query(p: &SearchParams) -> Option<String> {
    if !has_structured_params(p) {
        return None;
    }

    let parts: Vec<&str> = [
        p.amenity.as_deref(),
        p.street.as_deref(),
        p.city.as_deref(),
        p.county.as_deref(),
        p.state.as_deref(),
        p.postalcode.as_deref(),
        p.country.as_deref(),
    ]
    .iter()
    .filter_map(|o| {
        o.and_then(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() { None } else { Some(trimmed) }
        })
    })
    .collect();

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(", "))
    }
}

/// True when the only meaningful structured param is `postalcode`
/// (optionally accompanied by `country` / `countrycodes`).
fn is_postalcode_only(p: &SearchParams) -> bool {
    p.postalcode.is_some()
        && p.amenity.is_none()
        && p.street.is_none()
        && p.city.is_none()
        && p.county.is_none()
        && p.state.is_none()
}

/// True when `amenity` is set but street and city are both absent.
fn is_amenity_only(p: &SearchParams) -> bool {
    p.amenity.is_some()
        && p.street.is_none()
        && p.city.is_none()
}

#[derive(Debug, Deserialize)]
struct AutocompleteParams {
    q: Option<String>,
    countrycodes: Option<String>,
    #[serde(default = "default_autocomplete_limit")]
    limit: usize,
}

#[derive(Debug, Serialize)]
struct AutocompleteResult {
    name: String,
    display_name: String,
    #[serde(rename = "type")]
    place_type: String,
    lat: String,
    lon: String,
    importance: f64,
}

#[derive(Debug, Deserialize)]
struct LookupParams {
    /// Comma-separated encoded place IDs (e.g., "12345,67890")
    place_ids: Option<String>,
    /// Comma-separated OSM IDs with type prefix (e.g., "R54413,N123456")
    osm_ids: Option<String>,
    #[serde(default = "default_format")]
    #[allow(dead_code)]
    format: String,
    #[serde(default)]
    addressdetails: u8,
}

#[derive(Debug, Serialize)]
struct NominatimResult {
    place_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    osm_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    osm_id: Option<u32>,
    display_name: String,
    lat: String,
    lon: String,
    #[serde(rename = "type")]
    place_type: String,
    importance: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    match_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<AddressDetails>,
}

#[derive(Debug, Serialize)]
struct AddressDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    house_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    road: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suburb: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    town: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    village: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    county: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    postcode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country_code: Option<String>,
}

// ---------------------------------------------------------------------------
// City resolution helper — works in both full and lightweight (global FST) mode
// ---------------------------------------------------------------------------

/// Resolve a city name to (admin2_id, coord) using the per-country FST first,
/// then falling back to the global index when per-country FSTs are empty
/// (lightweight mode).
fn resolve_city(
    city: &str,
    country: &CountryIndex,
    country_idx: usize,
    normalizer: &Normalizer,
    global_index: Option<&GlobalIndex>,
) -> (Option<u16>, Option<Coord>) {
    let city_candidates = normalizer.normalize(city);

    // Try per-country FST first (populated in full mode)
    for candidate in &city_candidates {
        if let Some(record_id) = country.index.exact_lookup(candidate) {
            let muni_id = country.index.record_admin2(record_id).filter(|&a2| a2 > 0);
            let coord = country.index.record_store().get(record_id).ok().map(|r| r.coord);
            if muni_id.is_some() || coord.is_some() {
                return (muni_id, coord);
            }
        }
    }

    // Fallback: use global index postings to find the city, then resolve
    // via the country's record store (which IS loaded even in lightweight mode)
    if let Some(global) = global_index {
        for candidate in &city_candidates {
            let postings = global.exact_lookup(candidate);
            // Postings are sorted by importance desc — pick the best match for this country
            for posting in &postings {
                if posting.country_id as usize == country_idx {
                    let record_id = posting.record_id;
                    let muni_id = country.index.record_admin2(record_id).filter(|&a2| a2 > 0);
                    let coord = country.index.record_store().get(record_id).ok().map(|r| r.coord);
                    return (muni_id, coord);
                }
            }
        }
    }

    (None, None)
}

/// Like `resolve_city`, but also returns the admin1_id of the resolved city.
/// Used to apply a city-context bonus for disambiguation queries like
/// "Slussen, Stockholm" — we want results sharing Stockholm's admin1/admin2
/// or sitting close to Stockholm's coord.
fn resolve_city_full(
    city: &str,
    country: &CountryIndex,
    country_idx: usize,
    normalizer: &Normalizer,
    global_index: Option<&GlobalIndex>,
) -> (Option<u16>, Option<u16>, Option<Coord>) {
    resolve_city_full_filtered(city, country, country_idx, normalizer, global_index, false)
}

/// Like `resolve_city_full` but with `settlement_only` mode: only matches
/// records that are actually settlements (City, Town, Village, Hamlet,
/// Suburb). Used for whitespace-disambiguation queries like "stortorget
/// göteborg" where treating "slottet" as a city would mean any random
/// Locality named "slottet" sets a misleading city context.
fn resolve_city_full_filtered(
    city: &str,
    country: &CountryIndex,
    country_idx: usize,
    normalizer: &Normalizer,
    global_index: Option<&GlobalIndex>,
    settlement_only: bool,
) -> (Option<u16>, Option<u16>, Option<Coord>) {
    let city_candidates = normalizer.normalize(city);
    let is_settlement = |pt: PlaceType| matches!(pt,
        PlaceType::City | PlaceType::Town | PlaceType::Village
            | PlaceType::Hamlet | PlaceType::Suburb | PlaceType::Quarter
            | PlaceType::Neighbourhood
    );

    // Try per-country FST first (populated in full mode)
    for candidate in &city_candidates {
        if let Some(record_id) = country.index.exact_lookup(candidate) {
            if let Ok(record) = country.index.record_store().get(record_id) {
                if settlement_only && !is_settlement(record.place_type) { continue; }
                let a1 = if record.admin1_id > 0 { Some(record.admin1_id) } else { None };
                let a2 = if record.admin2_id > 0 { Some(record.admin2_id) } else { None };
                return (a1, a2, Some(record.coord));
            }
        }
    }

    // Fallback: use global index postings
    if let Some(global) = global_index {
        for candidate in &city_candidates {
            let postings = global.exact_lookup(candidate);
            for posting in &postings {
                if posting.country_id as usize == country_idx {
                    if let Ok(record) = country.index.record_store().get(posting.record_id) {
                        if settlement_only && !is_settlement(record.place_type) { continue; }
                        let a1 = if record.admin1_id > 0 { Some(record.admin1_id) } else { None };
                        let a2 = if record.admin2_id > 0 { Some(record.admin2_id) } else { None };
                        return (a1, a2, Some(record.coord));
                    }
                }
            }
        }
    }

    (None, None, None)
}

/// Split a "X, Y" or "X, Y, Z" query into (head, last_token).
/// `head` is everything before the last comma (e.g. "Slussen" for
/// "Slussen, Stockholm"; "Drottninggatan 1" for
/// "Drottninggatan 1, Stockholm, Sweden").
/// Returns None when the input has no comma or either side is empty,
/// or when the last token starts with a digit (postcode/housenumber).
fn split_disambiguation_query(input: &str) -> Option<(String, String)> {
    let trimmed = input.trim();
    let last_comma = trimmed.rfind(',')?;
    let head = trimmed[..last_comma].trim();
    let last = trimmed[last_comma + 1..].trim();
    if head.is_empty() || last.is_empty() { return None; }
    // Last token must look like a place name (alphabetic start, not a postcode)
    if last.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true) { return None; }
    Some((head.to_owned(), last.to_owned()))
}

/// Compute a city-context tier for a candidate, used to re-rank results
/// when the query has a "X, Y" disambiguation pattern.
///
/// Returns a non-negative integer where higher is better:
///   3 = same admin2 (municipality) — strongest match
///   2 = same admin1 (state/county/län)
///   1 = within 50 km of city coord
///   0 = no relationship
fn city_context_tier(
    cand_admin1: u16,
    cand_admin2: u16,
    cand_coord: Coord,
    ctx_admin1: Option<u16>,
    ctx_admin2: Option<u16>,
    ctx_coord: Option<Coord>,
) -> u8 {
    if let (Some(a2), true) = (ctx_admin2, cand_admin2 > 0) {
        if a2 == cand_admin2 { return 3; }
    }
    if let (Some(a1), true) = (ctx_admin1, cand_admin1 > 0) {
        if a1 == cand_admin1 { return 2; }
    }
    if let Some(c) = ctx_coord {
        if cand_coord.distance_m(&c) <= 50_000.0 {
            return 1;
        }
    }
    0
}

/// Detect a postcode-shaped query. Matches:
///   - 4-7 ASCII digits ("11122", "21100", "75221", "1010", "1234567")
///   - 5-digit Swedish split form "NNN NN" ("11 122", "114 56")
///
/// UK-style alphanumeric postcodes are handled separately by
/// `heimdall_core::addr_index::is_uk_postcode`.
fn is_postcode_shaped(s: &str) -> bool {
    let trimmed = s.trim();
    if trimmed.is_empty() { return false; }
    // Strip a single internal space (Swedish "114 56", DE "12345" w/ stray ws)
    let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
    if compact.len() < 4 || compact.len() > 7 { return false; }
    compact.chars().all(|c| c.is_ascii_digit())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn search(
    Query(params): Query<SearchParams>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<NominatimResult>>, (StatusCode, String)> {
    let structured = has_structured_params(&params);

    // q= and structured params are mutually exclusive
    if params.q.is_some() && structured {
        return Err((
            StatusCode::BAD_REQUEST,
            "Cannot combine q= with structured query parameters".to_owned(),
        ));
    }

    let query_text = if structured {
        match parse_structured_query(&params) {
            Some(q) => q,
            None => return Ok(Json(vec![])),
        }
    } else {
        match &params.q {
            Some(q) => q.clone(),
            None => return Ok(Json(vec![])),
        }
    };

    if query_text.trim().is_empty() {
        return Ok(Json(vec![]));
    }

    // Cap input length to prevent excessive FST automaton build times
    let query_text = if query_text.len() > 100 {
        query_text[..100].to_string()
    } else {
        query_text
    };

    // For postalcode-only structured queries, try postcode FSTs first
    let postalcode_only = structured && is_postalcode_only(&params);
    // amenity without street/city is treated as a place name query — the
    // existing pipeline handles this naturally (parse_address_query and
    // parse_street_query won't match, so it falls through to place name lookup).
    let _amenity_only = structured && is_amenity_only(&params);

    let country_codes: Vec<[u8; 2]> = params
        .countrycodes
        .as_deref()
        .map(|cc| {
            cc.split(',')
                .filter_map(|c| {
                    let b = c.trim().to_uppercase();
                    if b.len() == 2 { Some([b.as_bytes()[0], b.as_bytes()[1]]) } else { None }
                })
                .collect()
        })
        .unwrap_or_default();

    let bbox = params.viewbox.as_deref().and_then(parse_viewbox);
    let limit = params.limit.min(50);

    let target_countries: Vec<(usize, &CountryIndex)> = if country_codes.is_empty() {
        state.countries.iter().enumerate().collect()
    } else {
        state.countries.iter().enumerate().filter(|(_, c)| country_codes.contains(&c.code)).collect()
    };

    // Track which countries are being queried (only when filtered)
    if !country_codes.is_empty() {
        for (_, c) in &target_countries {
            let cc = std::str::from_utf8(&c.code).unwrap_or("??").to_lowercase();
            metrics::record_country_hit(&cc);
        }
    }

    let mut response: Vec<NominatimResult> = Vec::new();

    // Structured postalcode-only: route to postcode FST lookup first
    if postalcode_only {
        let pc = params.postalcode.as_deref().unwrap_or("").trim();
        // Try US ZIP index
        if ZipIndex::is_us_zip(pc) {
            for (_ci, country) in &target_countries {
                if let Some(ref zip_idx) = country.zip_index {
                    if let Some(zr) = zip_idx.lookup(pc) {
                        let display = if zr.city.is_empty() {
                            format!("{}, {}", zr.zip, zr.state)
                        } else {
                            format!("{} — {}, {}", zr.zip, zr.city, zr.state)
                        };
                        response.push(NominatimResult {
                            place_id: 0,
                            osm_type: None,
                            osm_id: None,
                            display_name: display,
                            lat: format!("{:.7}", zr.coord.lat_f64()),
                            lon: format!("{:.7}", zr.coord.lon_f64()),
                            place_type: "postcode".to_owned(),
                            importance: 0.6,
                            match_type: Some("zip".to_owned()),
                            address: if params.addressdetails > 0 {
                                Some(AddressDetails {
                                    house_number: None,
                                    road: None,
                                    suburb: None,
                                    city: if !zr.city.is_empty() { Some(zr.city.clone()) } else { None },
                                    town: None,
                                    village: None,
                                    county: if !zr.county.is_empty() { Some(zr.county.clone()) } else { None },
                                    state: Some(zr.state.clone()),
                                    postcode: Some(zr.zip.clone()),
                                    country: Some("United States".to_owned()),
                                    country_code: Some("us".to_owned()),
                                })
                            } else { None },
                        });
                        return Ok(Json(response));
                    }
                }
            }
        }
        // Try UK / addr_index postcode lookup
        for (_ci, country) in &target_countries {
            if let Some(ref addr_index) = country.addr_index {
                if let Some(r) = addr_index.lookup_postcode(pc) {
                    let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
                    response.push(NominatimResult {
                        place_id: 0,
                        osm_type: None,
                        osm_id: None,
                        display_name: r.street.clone(),
                        lat: format!("{:.7}", r.coord.lat_f64()),
                        lon: format!("{:.7}", r.coord.lon_f64()),
                        place_type: "postcode".to_owned(),
                        importance: 0.6,
                        match_type: Some("postcode".to_owned()),
                        address: if params.addressdetails > 0 {
                            Some(AddressDetails {
                                house_number: None,
                                road: None,
                                suburb: None,
                                city: None,
                                town: None,
                                village: None,
                                county: None,
                                state: None,
                                postcode: Some(pc.to_owned()),
                                country: Some(country.name.clone()),
                                country_code: Some(cc),
                            })
                        } else { None },
                    });
                    return Ok(Json(response));
                }
            }
        }
        // Fall through to the normal pipeline with the synthetic query
    }

    // ZIP code detection
    if ZipIndex::is_us_zip(&query_text) {
        for (_ci, country) in &target_countries {
            if let Some(ref zip_idx) = country.zip_index {
                if let Some(zr) = zip_idx.lookup(query_text.trim()) {
                    let display = if zr.city.is_empty() {
                        format!("{}, {}", zr.zip, zr.state)
                    } else {
                        format!("{} — {}, {}", zr.zip, zr.city, zr.state)
                    };
                    response.push(NominatimResult {
                        place_id: 0,
                        osm_type: None,
                        osm_id: None,
                        display_name: display,
                        lat: format!("{:.7}", zr.coord.lat_f64()),
                        lon: format!("{:.7}", zr.coord.lon_f64()),
                        place_type: "postcode".to_owned(),
                        importance: 0.6,
                        match_type: Some("zip".to_owned()),
                        address: if params.addressdetails > 0 {
                            Some(AddressDetails {
                                house_number: None,
                                road: None,
                                suburb: None,
                                city: if !zr.city.is_empty() { Some(zr.city.clone()) } else { None },
                                town: None,
                                village: None,
                                county: if !zr.county.is_empty() { Some(zr.county.clone()) } else { None },
                                state: Some(zr.state.clone()),
                                postcode: Some(zr.zip.clone()),
                                country: Some("United States".to_owned()),
                                country_code: Some("us".to_owned()),
                            })
                        } else { None },
                    });
                    return Ok(Json(response));
                }
            }
        }
    }

    // Postcode auto-detect for q= (Bug B). Users paste raw zips like "11122"
    // or "114 56" into the search box and expect the same result as
    // ?postalcode=. Try each target country's postcode FST. If a match is
    // found we push it as a `postcode` result; we keep going so place-name
    // results from the global FST below can still be returned alongside.
    let mut postcode_match_found = false;
    if is_postcode_shaped(&query_text) {
        for (_ci, country) in &target_countries {
            if let Some(ref addr_index) = country.addr_index {
                if let Some(r) = addr_index.lookup_postcode(&query_text) {
                    let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
                    let display = if r.street.is_empty() {
                        query_text.trim().to_owned()
                    } else {
                        r.street.clone()
                    };
                    response.push(NominatimResult {
                        place_id: 0,
                        osm_type: None,
                        osm_id: None,
                        display_name: display,
                        lat: format!("{:.7}", r.coord.lat_f64()),
                        lon: format!("{:.7}", r.coord.lon_f64()),
                        place_type: "postcode".to_owned(),
                        importance: 0.6,
                        match_type: Some("postcode".to_owned()),
                        address: if params.addressdetails > 0 {
                            Some(AddressDetails {
                                house_number: None,
                                road: None,
                                suburb: None,
                                city: None,
                                town: None,
                                village: None,
                                county: None,
                                state: None,
                                postcode: Some(query_text.trim().to_owned()),
                                country: Some(country.name.clone()),
                                country_code: Some(cc),
                            })
                        } else { None },
                    });
                    postcode_match_found = true;
                    break;
                }
            }
        }
        // If the input is purely a postcode shape and we found a match,
        // place-name lookups for the same digit string will be useless —
        // short-circuit to keep the postcode result clean.
        if postcode_match_found {
            return Ok(Json(response));
        }
    }

    let query_text = strip_us_unit_designator(&query_text);
    let (query_text, _detected_state) = strip_us_state_suffix(&query_text);

    // ------------------------------------------------------------------
    // City-context disambiguation (Bug A). If the query is "X, Y" we
    // resolve Y to a (admin1, admin2, coord) and use it to re-rank the
    // place-name candidates produced below. Soft re-rank only — we never
    // hide a candidate, just multiply its importance.
    // ------------------------------------------------------------------
    let city_context: Option<(Option<u16>, Option<u16>, Option<Coord>)> = {
        let mut ctx = None;
        // Two patterns produce a city context:
        //   "X, Y"  — explicit comma disambiguation (Slussen, Stockholm).
        //             Y can be any place — "Drottninggatan, Stockholm"
        //             still works even though Stockholm is a City.
        //   "X Y"   — bare whitespace (stortorget göteborg). For these
        //             we *require* the last token to actually be a
        //             settlement. Otherwise queries like "kungliga slottet"
        //             treat "slottet" as a city (matching some random
        //             Locality named "Slottet") and misleadingly hard-
        //             filter the real Royal Palace out.
        let (last_token, settlement_only) = if let Some((_, l)) = split_disambiguation_query(&query_text) {
            (Some(l), false)
        } else {
            let words: Vec<&str> = query_text.split_whitespace().collect();
            if words.len() >= 2
                && words.last().map(|w| w.chars().next().map(|c| c.is_alphabetic()).unwrap_or(false)).unwrap_or(false)
            {
                (Some(words.last().unwrap().to_string()), true)
            } else {
                (None, false)
            }
        };
        if let Some(last) = last_token {
            for &(ci, country) in &target_countries {
                let (a1, a2, coord) = resolve_city_full_filtered(
                    &last,
                    country,
                    ci,
                    &country.normalizer,
                    state.global_index.as_ref(),
                    settlement_only,
                );
                if a1.is_some() || a2.is_some() || coord.is_some() {
                    ctx = Some((a1, a2, coord));
                    break;
                }
            }
        }
        ctx
    };

    // ------------------------------------------------------------------
    // Global FST fast path — single lookup instead of scanning all country FSTs
    // ------------------------------------------------------------------
    let mut used_global = false;
    if let Some(ref global) = state.global_index {
        // Build candidates using the first target country's normalizer (or any
        // loaded normalizer — they all share the same lowercasing/whitespace
        // normalization; the phonetic part is language-specific but the exact
        // normalized form is what matters for the global FST).
        let normalizer = target_countries
            .first()
            .map(|&(_, c)| &c.normalizer);

        // When we have a city context (Bug A), look at a wider candidate pool
        // so the city-context boost can lift a relevant-but-less-important
        // posting above the globally-most-important one.
        let posting_window = if city_context.is_some() {
            (limit * 8).max(32)
        } else {
            limit
        };

        if let Some(norm) = normalizer {
            let candidates = norm.normalize(&query_text);

            // --- Exact lookup via global FST ---
            for candidate in &candidates {
                let postings = global.exact_lookup(candidate);
                let filtered = filter_postings_by_country(&postings, &country_codes, &state.countries);

                for posting in filtered.iter().take(posting_window) {
                    if let Some(result) = posting_to_nominatim(posting, &state.countries, "exact", params.addressdetails > 0) {
                        response.push(result);
                    }
                }
                if !response.is_empty() {
                    used_global = true;
                    break;
                }
            }

            // --- Fuzzy (Levenshtein distance 1) via global FST ---
            if !used_global {
                for candidate in &candidates {
                    let postings = global.fuzzy_lookup(candidate, 1);
                    let filtered = filter_postings_by_country(&postings, &country_codes, &state.countries);

                    for posting in filtered.iter().take(posting_window) {
                        if let Some(result) = posting_to_nominatim(posting, &state.countries, "levenshtein", params.addressdetails > 0) {
                            response.push(result);
                        }
                    }
                    if !response.is_empty() {
                        used_global = true;
                        break;
                    }
                }
            }
        }

    }

    // Per-country pipeline: address/street lookups always run (even when
    // global FST matched a place name) because "kungsgatan, stockholm"
    // needs the address pipeline, not a place name result for "kungsgatan".
    // Address/street results break out of this loop and fall through to the
    // final sort, so they get properly ranked against global FST results.
    for &(ci, country) in &target_countries {
        let candidates = country.normalizer.normalize(&query_text);

        let mut geo_query = GeoQuery::new(&query_text);
        // When a city context is in play, look at a wider candidate pool
        // so the city-context filter can pick the contextually-correct
        // alternate over the globally-most-important one (matches the
        // posting_window expansion used on the global-FST path).
        geo_query.limit = if city_context.is_some() { (limit * 8).max(32) } else { limit };
        geo_query.country_code = country_codes.first().copied();
        geo_query.bbox = bbox;

        // Postcode lookup (UK)
        if let Some(ref addr_index) = country.addr_index {
            if heimdall_core::addr_index::is_uk_postcode(&query_text) {
                if let Some(r) = addr_index.lookup_postcode(&query_text) {
                    let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
                    response.push(NominatimResult {
                        place_id: 0,
                        osm_type: None,
                        osm_id: None,
                        display_name: r.street.clone(),
                        lat: format!("{:.7}", r.coord.lat_f64()),
                        lon: format!("{:.7}", r.coord.lon_f64()),
                        place_type: "postcode".to_owned(),
                        importance: 0.6,
                        match_type: Some("postcode".to_owned()),
                        address: if params.addressdetails > 0 {
                            Some(AddressDetails {
                                house_number: None,
                                road: None,
                                suburb: None,
                                city: None,
                                town: None,
                                village: None,
                                county: None,
                                state: None,
                                postcode: Some(query_text.trim().to_uppercase()),
                                country: Some(country.name.clone()),
                                country_code: Some(cc),
                            })
                        } else { None },
                    });
                    return Ok(Json(response));
                }
            }
        }

        // Address parsing
        if let Some(ref addr_index) = country.addr_index {
            if let Some(addr_query) = parse_address_query(&query_text) {
                let (mut muni_id, mut city_coord) = if let Some(city) = &addr_query.city {
                    resolve_city(city, country, ci, &country.normalizer, state.global_index.as_ref())
                } else {
                    (None, None)
                };

                // When no city given, bias toward the most prominent place with
                // this street name (e.g. "drottninggatan" → Stockholm)
                if city_coord.is_none() {
                    let street_cands = country.normalizer.normalize(&addr_query.street);
                    // Try global index first (has importance-sorted postings)
                    if let Some(ref global) = state.global_index {
                        'outer_global: for sc in &street_cands {
                            let postings = global.exact_lookup(sc);
                            for posting in &postings {
                                if posting.country_id as usize == ci {
                                    if let Ok(record) = country.index.record_store().get(posting.record_id) {
                                        city_coord = Some(record.coord);
                                        let a2 = country.index.record_admin2(posting.record_id).filter(|&a| a > 0);
                                        if a2.is_some() { muni_id = a2; }
                                        break 'outer_global;
                                    }
                                }
                            }
                        }
                    }
                    // Fallback to per-country FST
                    if city_coord.is_none() {
                        for sc in &street_cands {
                            if let Some(record_id) = country.index.exact_lookup(sc) {
                                if let Ok(record) = country.index.record_store().get(record_id) {
                                    city_coord = Some(record.coord);
                                    let a2 = country.index.record_admin2(record_id).filter(|&a| a > 0);
                                    if a2.is_some() { muni_id = a2; }
                                    break;
                                }
                            }
                        }
                    }
                }

                let addr_results = addr_index.lookup(&addr_query, muni_id, city_coord);
                let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
                for (i, r) in addr_results.into_iter().enumerate() {
                    response.push(NominatimResult {
                        place_id: i as u64,
                        osm_type: None,
                        osm_id: None,
                        display_name: format!("{} {}", r.street, r.housenumber),
                        lat: format!("{:.7}", r.coord.lat_f64()),
                        lon: format!("{:.7}", r.coord.lon_f64()),
                        place_type: "address".to_owned(),
                        importance: 0.5,
                        match_type: Some("address".to_owned()),
                        address: if params.addressdetails > 0 {
                            Some(build_address_details_for_addr(
                                &r, &addr_query, country, &cc,
                            ))
                        } else { None },
                    });
                }
                if response.iter().any(|r| r.match_type.as_deref() == Some("address")) {
                    break;
                }
            }

            // Street-only lookup
            if let Some((street, city_str)) = parse_street_query(&query_text) {
                let street_normalized = country.normalizer.normalize(&street);
                let (muni_id, city_coord) = resolve_city(&city_str, country, ci, &country.normalizer, state.global_index.as_ref());

                let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
                for s in &street_normalized {
                    if let Some(result) = addr_index.lookup_street(s, muni_id, city_coord) {
                        let display = if result.street.is_empty() {
                            street.clone()
                        } else {
                            result.street.clone()
                        };
                        response.push(NominatimResult {
                            place_id: 0,
                            osm_type: None,
                            osm_id: None,
                            display_name: display,
                            lat: format!("{:.7}", result.coord.lat_f64()),
                            lon: format!("{:.7}", result.coord.lon_f64()),
                            place_type: "street".to_owned(),
                            importance: 0.4,
                            match_type: Some("street".to_owned()),
                            address: if params.addressdetails > 0 {
                                Some(AddressDetails {
                                    house_number: None,
                                    road: Some(if result.street.is_empty() {
                                        street.clone()
                                    } else {
                                        result.street.clone()
                                    }),
                                    suburb: None,
                                    city: Some(city_str.clone()),
                                    town: None,
                                    village: None,
                                    county: None,
                                    state: None,
                                    postcode: if result.postcode > 0 {
                                        Some(result.postcode.to_string())
                                    } else { None },
                                    country: Some(country.name.clone()),
                                    country_code: Some(cc.clone()),
                                })
                            } else { None },
                        });
                        break;
                    }
                }
                if response.iter().any(|r| r.match_type.as_deref() == Some("street")) {
                    break;
                }
            }

            // Freeform "street city" without comma (e.g. "kungsgatan stockholm")
            if response.is_empty() {
                let global_ref = state.global_index.as_ref();
                let norm = &country.normalizer;
                let idx = &country.index;
                let ci_copy = ci;
                if let Some((street, city_str)) = parse_street_city_freeform(&query_text, |candidate| {
                    // Check per-country FST first, then global index
                    let city_cands = norm.normalize(candidate);
                    for c in &city_cands {
                        if idx.exact_lookup(c).is_some() {
                            return true;
                        }
                    }
                    if let Some(global) = global_ref {
                        for c in &city_cands {
                            let postings = global.exact_lookup(c);
                            if postings.iter().any(|p| p.country_id as usize == ci_copy) {
                                return true;
                            }
                        }
                    }
                    false
                }) {
                    let street_normalized = norm.normalize(&street);
                    let (muni_id, city_coord) = resolve_city(&city_str, country, ci, norm, global_ref);

                    let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
                    for s in &street_normalized {
                        if let Some(result) = addr_index.lookup_street(s, muni_id, city_coord) {
                            let display = if result.street.is_empty() {
                                street.clone()
                            } else {
                                result.street.clone()
                            };
                            response.push(NominatimResult {
                                place_id: 0,
                                osm_type: None,
                                osm_id: None,
                                display_name: display,
                                lat: format!("{:.7}", result.coord.lat_f64()),
                                lon: format!("{:.7}", result.coord.lon_f64()),
                                place_type: "street".to_owned(),
                                importance: 0.4,
                                match_type: Some("street".to_owned()),
                                address: if params.addressdetails > 0 {
                                    Some(AddressDetails {
                                        house_number: None,
                                        road: Some(if result.street.is_empty() {
                                            street.clone()
                                        } else {
                                            result.street.clone()
                                        }),
                                        suburb: None,
                                        city: Some(city_str.clone()),
                                        town: None,
                                        village: None,
                                        county: None,
                                        state: None,
                                        postcode: if result.postcode > 0 {
                                            Some(result.postcode.to_string())
                                        } else { None },
                                        country: Some(country.name.clone()),
                                        country_code: Some(cc.clone()),
                                    })
                                } else { None },
                            });
                            break;
                        }
                    }
                    if response.iter().any(|r| r.match_type.as_deref() == Some("street")) {
                        break;
                    }
                }
            }
        }

        // Place name lookup — skip when global FST already found results
        if !used_global {
            let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
            let mut found_exact = false;
            for candidate in &candidates {
                let results = country.index.geocode_normalized(candidate, &geo_query);
                if !results.is_empty() {
                    for r in results {
                        response.push(to_nominatim_enriched(ci, r, params.addressdetails > 0, &cc, &country.name, country));
                    }
                    found_exact = true;
                    break;
                }
            }

            // Phonetic fallback
            if !found_exact {
                let phonetic = country.normalizer.phonetic_key(&query_text);
                let phonetic_query = GeoQuery::new(&phonetic);
                let results = country.index.geocode_normalized(&phonetic, &phonetic_query);
                for r in results {
                    response.push(to_nominatim_enriched(ci, r, params.addressdetails > 0, &cc, &country.name, country));
                }
            }
        }

        if !response.is_empty() && !country_codes.is_empty() {
            break;
        }
    }

    // -----------------------------------------------------------------------
    // Word-dropping fallback for multi-word queries
    // -----------------------------------------------------------------------
    // If the full query returned no results and has 3+ words, try progressively
    // shorter prefixes. E.g. "Santa Cruz de Tenerife" → "Santa Cruz de" → "Santa Cruz".
    // Also try dropping from the front for qualifier patterns like "Richmond Yorkshire".
    if response.is_empty() {
        let words: Vec<&str> = query_text.split_whitespace().collect();
        if words.len() >= 2 {
            let mut subqueries: Vec<String> = Vec::new();

            // Drop words from the end: "Santa Cruz de Tenerife" → "Santa Cruz de" → "Santa Cruz"
            for end in (2..words.len()).rev() {
                subqueries.push(words[..end].join(" "));
            }
            // Drop words from the front: "Richmond Yorkshire" → "Yorkshire" (not useful alone)
            // Only first word as a standalone (useful for "Pamplona Navarra" → "Pamplona")
            if words.len() >= 2 {
                subqueries.push(words[0].to_string());
            }

            'word_drop: for sub in &subqueries {
                // Try global FST. Use "word_drop" as match_type so the
                // city-context filter (which exempts "exact" matches —
                // verbatim FST hits on the full query — from hard
                // filtering) doesn't accidentally let through results
                // that came from a partial subquery.
                if let Some(ref global) = state.global_index {
                    if let Some(norm) = target_countries.first().map(|&(_, c)| &c.normalizer) {
                        let sub_candidates = norm.normalize(sub);
                        for candidate in &sub_candidates {
                            let postings = global.exact_lookup(candidate);
                            let filtered = filter_postings_by_country(&postings, &country_codes, &state.countries);
                            for posting in filtered.iter().take(limit) {
                                if let Some(result) = posting_to_nominatim(posting, &state.countries, "word_drop", params.addressdetails > 0) {
                                    response.push(result);
                                }
                            }
                            if !response.is_empty() { break 'word_drop; }
                        }
                    }
                }

                // Try per-country FST. Same caveat — relabel to
                // "word_drop" so city-context filtering treats it as a
                // fallback hit, not a verbatim one.
                for &(ci, country) in &target_countries {
                    let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
                    let sub_candidates = country.normalizer.normalize(sub);
                    let mut geo_q = GeoQuery::new(sub);
                    // Mirror the main place-name path: widen the candidate
                    // pool when a city context is set so the city-context
                    // filter can pick the contextually-correct alternate.
                    geo_q.limit = if city_context.is_some() { (limit * 8).max(32) } else { limit };

                    for candidate in &sub_candidates {
                        let results = country.index.geocode_normalized(candidate, &geo_q);
                        for r in results {
                            let mut result = to_nominatim_enriched(ci, r, params.addressdetails > 0, &cc, &country.name, country);
                            result.match_type = Some("word_drop".to_string());
                            response.push(result);
                        }
                        if !response.is_empty() { break 'word_drop; }
                    }
                }
            }
        }
    }

    // Viewbox filtering (bounded=1) or bias (viewbox without bounded)
    if let Some(ref vbox) = bbox {
        if params.bounded == 1 {
            // Hard filter: only keep results inside the viewbox
            response.retain(|r| result_in_bbox(r, vbox));
        } else {
            // Soft bias: boost importance of results inside the viewbox by 1.5x
            for r in &mut response {
                if result_in_bbox(r, vbox) {
                    r.importance *= 1.5;
                }
            }
        }
    }

    // City-context tier (Bug A): when q="X, Y" and Y resolved to a city,
    // compute a per-result tier (0..3) capturing how strongly the candidate
    // belongs to Y's admin hierarchy. The final sort uses this as the
    // primary key. Soft re-rank — never hides a candidate, just promotes
    // contextually-relevant ones. Computed AFTER the viewbox filter so the
    // index alignment stays correct.
    let response_tiers: Vec<u8> = if let Some((ctx_a1, ctx_a2, ctx_coord)) = city_context {
        response.iter().map(|r| {
            // Place-name results have a non-zero place_id encoding
            // (country, record). Address/street results have place_id=0.
            if r.place_id != 0 {
                let (ci, rid) = decode_place_id(r.place_id);
                if let Some(country) = state.countries.get(ci) {
                    if let Ok(rec) = country.index.record_store().get(rid) {
                        return city_context_tier(
                            rec.admin1_id,
                            rec.admin2_id,
                            rec.coord,
                            ctx_a1,
                            ctx_a2,
                            ctx_coord,
                        );
                    }
                }
            }
            // Fallback: distance check using the result's own coord.
            if let Some(c) = ctx_coord {
                if let (Ok(lat), Ok(lon)) = (r.lat.parse::<f64>(), r.lon.parse::<f64>()) {
                    let cand = Coord::new(lat, lon);
                    if cand.distance_m(&c) <= 50_000.0 {
                        return 1;
                    }
                }
            }
            0
        }).collect()
    } else {
        vec![0; response.len()]
    };

    // Rank: city-context tier (highest first) > high-confidence match types
    // (exact/address/street) > importance. The city-context tier dominates
    // when q="X, Y" or q="X Y" with Y a known city — a candidate in Y's
    // municipality outranks a more important candidate elsewhere.
    // Without city context, all tiers are 0 and the sort behaves exactly
    // as before.
    //
    // When city_context resolved, drop results that don't match the
    // city — even if no candidate matches. The user explicitly said
    // "in city X"; returning a same-name result from elsewhere
    // (Stockholm's Stortorget for "stortorget göteborg") is misleading.
    //
    // Exception: "exact" / "address" / "street" match types always
    // pass through. The whole-string FST already matched character-
    // for-character, so the user typed the full name of a real place
    // (Nacka Strand, Skara Sommarland, Höga Kusten) — not "place IN
    // city". The whitespace city-context heuristic shouldn't override
    // a verbatim hit.
    let mut paired: Vec<(NominatimResult, u8)> = response
        .into_iter()
        .zip(response_tiers.into_iter())
        .filter(|(r, t)| {
            if city_context.is_none() { return true; }
            if *t > 0 { return true; }
            matches!(r.match_type.as_deref(), Some("exact" | "address" | "street"))
        })
        .collect();
    paired.sort_by(|(ra, ta), (rb, tb)| {
        let high_confidence = |r: &NominatimResult| {
            matches!(r.match_type.as_deref(), Some("exact" | "address" | "street"))
        };
        let a_hi = high_confidence(ra);
        let b_hi = high_confidence(rb);
        tb.cmp(ta)
            .then(b_hi.cmp(&a_hi))
            .then(rb.importance.partial_cmp(&ra.importance).unwrap_or(std::cmp::Ordering::Equal))
    });
    let mut response: Vec<NominatimResult> = paired.into_iter().map(|(r, _)| r).collect();
    response.truncate(limit);

    metrics::record_result_count("search", response.len());
    Ok(Json(response))
}

async fn autocomplete(
    Query(params): Query<AutocompleteParams>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<AutocompleteResult>>, StatusCode> {
    let prefix = match &params.q {
        Some(q) if !q.trim().is_empty() => q.trim().to_owned(),
        _ => return Ok(Json(vec![])),
    };

    let limit = params.limit.min(20);

    let country_codes: Vec<[u8; 2]> = params
        .countrycodes
        .as_deref()
        .map(|cc| {
            cc.split(',')
                .filter_map(|c| {
                    let b = c.trim().to_uppercase();
                    if b.len() == 2 { Some([b.as_bytes()[0], b.as_bytes()[1]]) } else { None }
                })
                .collect()
        })
        .unwrap_or_default();

    let target_countries: Vec<&CountryIndex> = if country_codes.is_empty() {
        state.countries.iter().collect()
    } else {
        state.countries.iter().filter(|c| country_codes.contains(&c.code)).collect()
    };

    let mut all_results: Vec<AutocompleteResult> = Vec::new();

    // Global index fast path for prefix search
    let mut used_global = false;
    if let Some(ref global) = state.global_index {
        let normalizer = target_countries.first().map(|c| &c.normalizer);
        if let Some(norm) = normalizer {
            let candidates = norm.normalize(&prefix);
            let normalized_prefix = candidates.first().map(|s| s.as_str()).unwrap_or(&prefix);

            let postings = global.prefix_search(normalized_prefix, limit * 2);
            let filtered = filter_postings_by_country(&postings, &country_codes, &state.countries);

            for posting in filtered.iter().take(limit) {
                let country_idx = posting.country_id as usize;
                if let Some(country) = state.countries.get(country_idx) {
                    if let Ok(record) = country.index.record_store().get(posting.record_id) {
                        let name = country.index.record_store().primary_name(&record);
                        let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
                        let admin1 = country.index.admin_entry(record.admin1_id).map(|a| a.name.clone());
                        let admin2 = country.index.admin_entry(record.admin2_id).map(|a| a.name.clone());

                        let display_name = match (&admin2, &admin1) {
                            (Some(a2), Some(a1)) => format!("{}, {}, {}, {}", name, a2, a1, cc.to_uppercase()),
                            (None, Some(a1)) => format!("{}, {}, {}", name, a1, cc.to_uppercase()),
                            _ => format!("{}, {}", name, cc.to_uppercase()),
                        };

                        all_results.push(AutocompleteResult {
                            name,
                            display_name,
                            place_type: format!("{:?}", record.place_type).to_lowercase(),
                            lat: format!("{:.7}", record.coord.lat_f64()),
                            lon: format!("{:.7}", record.coord.lon_f64()),
                            importance: record.importance as f64 / 65535.0,
                        });
                    }
                }
            }
            if !all_results.is_empty() {
                used_global = true;
            }
        }
    }

    // Fall back to per-country prefix search
    if !used_global {
        for country in &target_countries {
            let candidates = country.normalizer.normalize(&prefix);
            let normalized_prefix = candidates.first().map(|s| s.as_str()).unwrap_or(&prefix);

            let results = country.index.prefix_search(normalized_prefix, limit);
            let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();

            for r in results {
                let display_name = match (&r.admin2, &r.admin1) {
                    (Some(a2), Some(a1)) => format!("{}, {}, {}, {}", r.name, a2, a1, cc.to_uppercase()),
                    (None, Some(a1)) => format!("{}, {}, {}", r.name, a1, cc.to_uppercase()),
                    _ => format!("{}, {}", r.name, cc.to_uppercase()),
                };

                all_results.push(AutocompleteResult {
                    name: r.name,
                    display_name,
                    place_type: format!("{:?}", r.place_type).to_lowercase(),
                    lat: format!("{:.7}", r.coord.lat_f64()),
                    lon: format!("{:.7}", r.coord.lon_f64()),
                    importance: r.importance as f64 / 65535.0,
                });
            }
        }
    }

    all_results.sort_by(|a, b| b.importance.partial_cmp(&a.importance).unwrap_or(std::cmp::Ordering::Equal));
    all_results.truncate(limit);

    metrics::record_result_count("autocomplete", all_results.len());
    Ok(Json(all_results))
}

async fn status(
    State(state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let uptime = state.started_at
        .elapsed()
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let countries: Vec<serde_json::Value> = state.countries.iter().map(|c| {
        let cc = std::str::from_utf8(&c.code).unwrap_or("??").to_uppercase();
        serde_json::json!({
            "code": cc,
            "name": c.name,
            "places": c.meta.places,
            "addresses": c.meta.addresses,
            "index_mb": c.meta.index_size_bytes / 1_048_576,
            "load_ms": c.meta.load_time_ms,
        })
    }).collect();

    let total_places: usize = state.countries.iter().map(|c| c.meta.places).sum();
    let total_addrs: usize = state.countries.iter().map(|c| c.meta.addresses).sum();

    Json(serde_json::json!({
        "status": "OK",
        "version": env!("CARGO_PKG_VERSION"),
        "engine": "Heimdall",
        "uptime_seconds": uptime,
        "countries": countries,
        "total_places": total_places,
        "total_addresses": total_addrs,
    }))
}

// ---------------------------------------------------------------------------
// Reverse geocoding
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ReverseParams {
    lat: f64,
    lon: f64,
    #[serde(default = "default_zoom")]
    zoom: u8,
    #[serde(default = "default_format")]
    #[allow(dead_code)]
    format: String,
    #[serde(default)]
    addressdetails: u8,
}

fn default_zoom() -> u8 { 16 }

async fn reverse(
    Query(params): Query<ReverseParams>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let zoom = if params.zoom > 0 { Some(params.zoom) } else { None };

    let mut best: Option<(u32, f64, usize, &CountryIndex)> = None;

    for (ci, country) in state.countries.iter().enumerate() {
        if let Some(ref gi) = country.geohash_index {
            let candidates = gi.nearest(
                params.lat, params.lon,
                country.index.record_store(),
                1, zoom,
            );
            if let Some(&(id, dist)) = candidates.first() {
                if best.is_none() || dist < best.unwrap().1 {
                    best = Some((id, dist, ci, country));
                }
            }
        }
    }

    let (record_id, distance_m, country_index, country) = match best {
        Some(b) => b,
        None => return Ok(Json(serde_json::json!({"error": "no results"}))),
    };

    let record = country.index.record_store().get(record_id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let name = country.index.record_store().primary_name(&record);
    let admin1 = country.index.admin_entry(record.admin1_id).map(|a| a.name.clone());
    let admin2 = country.index.admin_entry(record.admin2_id).map(|a| a.name.clone());

    let display_name = match (&admin2, &admin1) {
        (Some(a2), Some(a1)) => format!("{}, {}, {}", name, a2, a1),
        (None, Some(a1)) => format!("{}, {}", name, a1),
        _ => name.clone(),
    };

    let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
    let encoded_place_id = encode_place_id(country_index, record_id);

    let mut result = serde_json::json!({
        "place_id": encoded_place_id,
        "osm_type": osm_type_from_flags(record.flags),
        "osm_id": if record.osm_id != 0 { Some(record.osm_id) } else { None::<u32> },
        "display_name": display_name,
        "lat": format!("{:.7}", record.coord.lat_f64()),
        "lon": format!("{:.7}", record.coord.lon_f64()),
        "type": format!("{:?}", record.place_type).to_lowercase(),
        "importance": record.importance as f64 / 65535.0,
        "distance_m": format!("{:.0}", distance_m),
    });

    if params.addressdetails > 0 {
        let (city, town, village, suburb) = place_type_to_settlement(&record.place_type, &name);
        let mut addr = serde_json::Map::new();
        if let Some(v) = suburb { addr.insert("suburb".into(), serde_json::Value::String(v)); }
        if let Some(v) = city { addr.insert("city".into(), serde_json::Value::String(v)); }
        if let Some(v) = town { addr.insert("town".into(), serde_json::Value::String(v)); }
        if let Some(v) = village { addr.insert("village".into(), serde_json::Value::String(v)); }
        if let Some(v) = admin2 { addr.insert("county".into(), serde_json::Value::String(v)); }
        if let Some(v) = admin1 { addr.insert("state".into(), serde_json::Value::String(v)); }
        addr.insert("country".into(), serde_json::Value::String(country.name.clone()));
        addr.insert("country_code".into(), serde_json::Value::String(cc.clone()));
        result["address"] = serde_json::Value::Object(addr);
    }

    Ok(Json(result))
}

// ---------------------------------------------------------------------------
// Lookup by place_id / osm_id
// ---------------------------------------------------------------------------

async fn lookup(
    Query(params): Query<LookupParams>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<NominatimResult>>, (StatusCode, String)> {
    if params.place_ids.is_none() && params.osm_ids.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Either place_ids or osm_ids parameter is required".to_owned(),
        ));
    }

    let mut results: Vec<NominatimResult> = Vec::new();

    // --- Lookup by place_ids ---
    if let Some(ref ids_str) = params.place_ids {
        for id_str in ids_str.split(',') {
            let id_str = id_str.trim();
            if id_str.is_empty() {
                continue;
            }
            let place_id: u64 = match id_str.parse() {
                Ok(v) => v,
                Err(_) => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("Invalid place_id: '{}'", id_str),
                    ));
                }
            };

            let (country_index, record_id) = decode_place_id(place_id);

            if country_index >= state.countries.len() {
                continue; // out of range — omit
            }

            let country = &state.countries[country_index];
            let record = match country.index.record_store().get(record_id) {
                Ok(r) => r,
                Err(_) => continue, // out of range — omit
            };

            let name = country.index.record_store().primary_name(&record);
            let admin1 = country.index.admin_entry(record.admin1_id).map(|a| a.name.clone());
            let admin2 = country.index.admin_entry(record.admin2_id).map(|a| a.name.clone());
            let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();

            let display_name = match (&admin2, &admin1) {
                (Some(a2), Some(a1)) => format!("{}, {}, {}", name, a2, a1),
                (None, Some(a1)) => format!("{}, {}", name, a1),
                _ => name.clone(),
            };

            let address = if params.addressdetails > 0 {
                let (city, town, village, suburb) = place_type_to_settlement(&record.place_type, &name);
                Some(AddressDetails {
                    house_number: None,
                    road: None,
                    suburb,
                    city,
                    town,
                    village,
                    county: admin2,
                    state: admin1,
                    postcode: None,
                    country: Some(country.name.clone()),
                    country_code: Some(cc.clone()),
                })
            } else {
                None
            };

            results.push(NominatimResult {
                place_id,
                osm_type: Some(osm_type_from_flags(record.flags).to_owned()),
                osm_id: if record.osm_id != 0 { Some(record.osm_id) } else { None },
                display_name,
                lat: format!("{:.7}", record.coord.lat_f64()),
                lon: format!("{:.7}", record.coord.lon_f64()),
                place_type: format!("{:?}", record.place_type).to_lowercase(),
                importance: record.importance as f64 / 65535.0,
                match_type: None,
                address,
            });
        }
    }

    // --- Lookup by osm_ids ---
    if let Some(ref ids_str) = params.osm_ids {
        for id_str in ids_str.split(',') {
            let id_str = id_str.trim();
            if id_str.len() < 2 {
                continue;
            }

            let type_char = id_str.chars().next().unwrap().to_ascii_uppercase();
            if !matches!(type_char, 'N' | 'W' | 'R') {
                continue;
            }

            let osm_id: u32 = match id_str[1..].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Look up in the OSM ID map (built lazily on first use)
            let osm_map = state.osm_id_map.get_or_init(|| build_osm_id_map(&state.countries));
            let key = (type_char, osm_id);
            if let Some(&(country_index, record_id)) = osm_map.get(&key) {
                let country = &state.countries[country_index];
                let record = match country.index.record_store().get(record_id) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                let name = country.index.record_store().primary_name(&record);
                let admin1 = country.index.admin_entry(record.admin1_id).map(|a| a.name.clone());
                let admin2 = country.index.admin_entry(record.admin2_id).map(|a| a.name.clone());
                let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();

                let display_name = match (&admin2, &admin1) {
                    (Some(a2), Some(a1)) => format!("{}, {}, {}", name, a2, a1),
                    (None, Some(a1)) => format!("{}, {}", name, a1),
                    _ => name.clone(),
                };

                let address = if params.addressdetails > 0 {
                    let (city, town, village, suburb) = place_type_to_settlement(&record.place_type, &name);
                    Some(AddressDetails {
                        house_number: None,
                        road: None,
                        suburb,
                        city,
                        town,
                        village,
                        county: admin2,
                        state: admin1,
                        postcode: None,
                        country: Some(country.name.clone()),
                        country_code: Some(cc.clone()),
                    })
                } else {
                    None
                };

                let place_id = encode_place_id(country_index, record_id);
                results.push(NominatimResult {
                    place_id,
                    osm_type: Some(osm_type_from_flags(record.flags).to_owned()),
                    osm_id: Some(osm_id),
                    display_name,
                    lat: format!("{:.7}", record.coord.lat_f64()),
                    lon: format!("{:.7}", record.coord.lon_f64()),
                    place_type: format!("{:?}", record.place_type).to_lowercase(),
                    importance: record.importance as f64 / 65535.0,
                    match_type: None,
                    address,
                });
            }
            // If not found, omit from results (don't error)
        }
    }

    Ok(Json(results))
}

// ---------------------------------------------------------------------------
// Place ID encoding
// ---------------------------------------------------------------------------

/// Encode a globally unique place_id: country_index in high bits, record_id in low 24 bits.
/// Supports up to 256 countries and ~16M records per country.
fn encode_place_id(country_index: usize, record_id: u32) -> u64 {
    ((country_index as u64) << 24) | (record_id as u64 & 0x00FF_FFFF)
}

/// Decode a place_id into (country_index, local_record_id).
fn decode_place_id(place_id: u64) -> (usize, u32) {
    let country_index = (place_id >> 24) as usize;
    let record_id = (place_id & 0x00FF_FFFF) as u32;
    (country_index, record_id)
}

/// Infer the OSM type string from PlaceRecord flags.
/// flags bit 3 (0x08) = is_relation.
fn osm_type_from_flags(flags: u8) -> &'static str {
    if flags & 0x08 != 0 { "relation" } else { "node" }
}

/// Infer the OSM type char from PlaceRecord flags for OsmIdMap keys.
fn osm_type_char_from_flags(flags: u8) -> char {
    if flags & 0x08 != 0 { 'R' } else { 'N' }
}

/// Build a mapping from (osm_type_char, osm_id) -> (country_index, record_id)
/// by scanning all records in all loaded countries.
fn build_osm_id_map(countries: &[CountryIndex]) -> OsmIdMap {
    let mut map = HashMap::new();
    for (ci, country) in countries.iter().enumerate() {
        let store = country.index.record_store();
        let count = store.len() as u32;
        for rid in 0..count {
            if let Ok(record) = store.get(rid) {
                if record.osm_id != 0 {
                    let type_char = osm_type_char_from_flags(record.flags);
                    map.insert((type_char, record.osm_id), (ci, rid));
                }
            }
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Filter posting entries by country code. When `country_codes` is empty, all
/// entries pass through.
fn filter_postings_by_country<'a>(
    postings: &'a [heimdall_core::global_index::PostingEntry],
    country_codes: &[[u8; 2]],
    countries: &[CountryIndex],
) -> Vec<&'a heimdall_core::global_index::PostingEntry> {
    if country_codes.is_empty() {
        postings.iter().collect()
    } else {
        postings
            .iter()
            .filter(|p| {
                if let Some(country) = countries.get(p.country_id as usize) {
                    country_codes.contains(&country.code)
                } else {
                    false
                }
            })
            .collect()
    }
}

/// Convert a global-index PostingEntry into a NominatimResult by reading the
/// corresponding record from the per-country store.
fn posting_to_nominatim(
    posting: &heimdall_core::global_index::PostingEntry,
    countries: &[CountryIndex],
    match_type_str: &str,
    addressdetails: bool,
) -> Option<NominatimResult> {
    let country_idx = posting.country_id as usize;
    let country = countries.get(country_idx)?;
    let record = country.index.record_store().get(posting.record_id).ok()?;
    let cc = std::str::from_utf8(&country.code).unwrap_or("??").to_lowercase();
    let name = country.index.record_store().primary_name(&record);

    let admin1 = country.index.admin_entry(record.admin1_id).map(|a| a.name.clone());
    let admin2 = country.index.admin_entry(record.admin2_id).map(|a| a.name.clone());

    let display = match (&admin2, &admin1) {
        (Some(a2), Some(a1)) => format!("{}, {}, {}", name, a2, a1),
        (None, Some(a1)) => format!("{}, {}", name, a1),
        _ => name.clone(),
    };

    let place_id = encode_place_id(country_idx, posting.record_id);

    let address = if addressdetails {
        let (city, town, village, suburb) =
            place_type_to_settlement(&record.place_type, &name);
        Some(AddressDetails {
            house_number: None,
            road: None,
            suburb,
            city,
            town,
            village,
            county: admin2.clone(),
            state: admin1.clone(),
            postcode: None,
            country: Some(country.name.clone()),
            country_code: Some(cc.clone()),
        })
    } else {
        None
    };

    Some(NominatimResult {
        place_id,
        osm_type: Some(osm_type_from_flags(record.flags).to_owned()),
        osm_id: if record.osm_id != 0 { Some(record.osm_id) } else { None },
        display_name: display,
        lat: format!("{:.7}", record.coord.lat_f64()),
        lon: format!("{:.7}", record.coord.lon_f64()),
        place_type: format!("{:?}", record.place_type).to_lowercase(),
        importance: record.importance as f64 / 65535.0,
        match_type: Some(match_type_str.to_owned()),
        address,
    })
}

fn to_nominatim(
    country_index: usize,
    r: GeoResult,
    address_details: bool,
    country_code: &str,
    country_name: &str,
) -> NominatimResult {
    let display_name = match (&r.admin2, &r.admin1) {
        (Some(a2), Some(a1)) => format!("{}, {}, {}", r.name, a2, a1),
        (None, Some(a1)) => format!("{}, {}", r.name, a1),
        _ => r.name.clone(),
    };

    let match_type_str = match r.match_type {
        MatchType::Exact => "exact",
        MatchType::Phonetic => "phonetic",
        MatchType::Levenshtein { .. } => "levenshtein",
        MatchType::Neural { .. } => "neural",
        MatchType::NGram { .. } => "ngram",
    };

    let address = if address_details {
        // Map place_type to the appropriate Nominatim field (city/town/village)
        let (city, town, village, suburb) = place_type_to_settlement(&r.place_type, &r.name);

        Some(AddressDetails {
            house_number: None,
            road: None,
            suburb,
            city,
            town,
            village,
            county: r.admin2.clone(),
            state: r.admin1.clone(),
            postcode: None,
            country: Some(country_name.to_owned()),
            country_code: Some(country_code.to_owned()),
        })
    } else {
        None
    };

    let place_id = match r.record_id {
        Some(rid) => encode_place_id(country_index, rid),
        None => 0,
    };

    NominatimResult {
        place_id,
        osm_type: None,
        osm_id: None,
        display_name,
        lat: format!("{:.7}", r.coord.lat_f64()),
        lon: format!("{:.7}", r.coord.lon_f64()),
        place_type: format!("{:?}", r.place_type).to_lowercase(),
        importance: r.importance as f64 / 65535.0,
        match_type: Some(match_type_str.to_owned()),
        address,
    }
}

/// Enriched version of to_nominatim that fills osm_type and osm_id from PlaceRecord.
fn to_nominatim_enriched(
    country_index: usize,
    r: GeoResult,
    address_details: bool,
    country_code: &str,
    country_name: &str,
    country: &CountryIndex,
) -> NominatimResult {
    let mut result = to_nominatim(country_index, r, address_details, country_code, country_name);
    if result.place_id != 0 {
        let (_, local_id) = decode_place_id(result.place_id);
        if let Ok(record) = country.index.record_store().get(local_id) {
            result.osm_type = Some(osm_type_from_flags(record.flags).to_owned());
            if record.osm_id != 0 {
                result.osm_id = Some(record.osm_id);
            }
        }
    }
    result
}

/// Map a PlaceType to the appropriate Nominatim settlement field.
/// Returns (city, town, village, suburb) -- only one will be Some.
fn place_type_to_settlement(pt: &PlaceType, name: &str) -> (Option<String>, Option<String>, Option<String>, Option<String>) {
    match pt {
        PlaceType::City | PlaceType::County | PlaceType::State | PlaceType::Country => {
            (Some(name.to_owned()), None, None, None)
        }
        PlaceType::Town => {
            (None, Some(name.to_owned()), None, None)
        }
        PlaceType::Village | PlaceType::Hamlet | PlaceType::Farm | PlaceType::Locality => {
            (None, None, Some(name.to_owned()), None)
        }
        PlaceType::Suburb | PlaceType::Quarter | PlaceType::Neighbourhood => {
            (None, None, None, Some(name.to_owned()))
        }
        _ => {
            // For natural features, POIs, etc. -- use city as the catch-all
            (Some(name.to_owned()), None, None, None)
        }
    }
}

fn build_address_details_for_addr(
    r: &heimdall_core::addr_index::AddrResult,
    addr_query: &heimdall_core::addr_index::AddressQuery,
    country: &CountryIndex,
    cc: &str,
) -> AddressDetails {
    AddressDetails {
        house_number: if r.housenumber.is_empty() { None } else { Some(r.housenumber.clone()) },
        road: if r.street.is_empty() { None } else { Some(r.street.clone()) },
        suburb: None,
        city: addr_query.city.clone(),
        town: None,
        village: None,
        county: None,
        state: None,
        postcode: if r.postcode > 0 { Some(format!("{}", r.postcode)) } else { addr_query.postcode.clone() },
        country: Some(country.name.clone()),
        country_code: Some(cc.to_owned()),
    }
}

fn parse_viewbox(s: &str) -> Option<BoundingBox> {
    let parts: Vec<f64> = s.split(',')
        .filter_map(|p| p.trim().parse().ok())
        .collect();

    if parts.len() == 4 {
        let (mut x1, mut y1, mut x2, mut y2) = (parts[0], parts[1], parts[2], parts[3]);
        if x1 > x2 { std::mem::swap(&mut x1, &mut x2); }
        if y1 > y2 { std::mem::swap(&mut y1, &mut y2); }
        Some(BoundingBox {
            min_lon: x1,
            min_lat: y1,
            max_lon: x2,
            max_lat: y2,
        })
    } else {
        None
    }
}

/// Check whether a point (given as lat/lon strings from NominatimResult) falls inside a bbox.
fn result_in_bbox(r: &NominatimResult, bbox: &BoundingBox) -> bool {
    let lat: f64 = match r.lat.parse() {
        Ok(v) => v,
        Err(_) => return false,
    };
    let lon: f64 = match r.lon.parse() {
        Ok(v) => v,
        Err(_) => return false,
    };
    lon >= bbox.min_lon && lon <= bbox.max_lon &&
    lat >= bbox.min_lat && lat <= bbox.max_lat
}

// ---------------------------------------------------------------------------
// US query preprocessing
// ---------------------------------------------------------------------------

fn strip_us_unit_designator(s: &str) -> String {
    let lower = s.to_lowercase();
    for pattern in &[" apt ", " apt. ", " ste ", " ste. ", " suite ", " unit ", " #"] {
        if let Some(pos) = lower.find(pattern) {
            return s[..pos].trim().to_owned();
        }
    }
    s.to_owned()
}

fn strip_us_state_suffix(s: &str) -> (String, Option<String>) {
    let trimmed = s.trim();

    if let Some(comma_pos) = trimmed.rfind(',') {
        let before = trimmed[..comma_pos].trim();
        let after = trimmed[comma_pos+1..].trim();
        if !before.is_empty() {
            if let Some(abbr) = match_us_state(after) {
                return (before.to_owned(), Some(abbr));
            }
        }
    }

    let words: Vec<&str> = trimmed.split_whitespace().collect();
    if words.len() >= 2 {
        let last = words[words.len() - 1];
        if last.len() == 2 && last.chars().all(|c| c.is_ascii_alphabetic()) {
            if let Some(abbr) = match_us_state(last) {
                let rest = words[..words.len()-1].join(" ");
                return (rest, Some(abbr));
            }
        }
    }

    (trimmed.to_owned(), None)
}

fn match_us_state(s: &str) -> Option<String> {
    let upper = s.trim().to_uppercase();

    const ABBREVS: &[&str] = &[
        "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN",
        "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
        "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT",
        "VT","VA","WA","WV","WI","WY",
    ];
    if ABBREVS.contains(&upper.as_str()) {
        return Some(upper);
    }

    let lower = s.trim().to_lowercase();
    let mapping: &[(&str, &str)] = &[
        ("alabama","AL"),("alaska","AK"),("arizona","AZ"),("arkansas","AR"),
        ("california","CA"),("colorado","CO"),("connecticut","CT"),("delaware","DE"),
        ("district of columbia","DC"),("florida","FL"),("georgia","GA"),("hawaii","HI"),
        ("idaho","ID"),("illinois","IL"),("indiana","IN"),("iowa","IA"),
        ("kansas","KS"),("kentucky","KY"),("louisiana","LA"),("maine","ME"),
        ("maryland","MD"),("massachusetts","MA"),("michigan","MI"),("minnesota","MN"),
        ("mississippi","MS"),("missouri","MO"),("montana","MT"),("nebraska","NE"),
        ("nevada","NV"),("new hampshire","NH"),("new jersey","NJ"),("new mexico","NM"),
        ("new york","NY"),("north carolina","NC"),("north dakota","ND"),("ohio","OH"),
        ("oklahoma","OK"),("oregon","OR"),("pennsylvania","PA"),("rhode island","RI"),
        ("south carolina","SC"),("south dakota","SD"),("tennessee","TN"),("texas","TX"),
        ("utah","UT"),("vermont","VT"),("virginia","VA"),("washington","WA"),
        ("west virginia","WV"),("wisconsin","WI"),("wyoming","WY"),
    ];
    for (name, abbr) in mapping {
        if lower == *name {
            return Some(abbr.to_string());
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Index loading
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn load_country_index(path: &std::path::Path) -> anyhow::Result<CountryIndex> {
    load_country_index_inner(path, false)
}

fn load_country_index_inner(path: &std::path::Path, lightweight: bool) -> anyhow::Result<CountryIndex> {
    let t0 = Instant::now();

    let index = if lightweight {
        HeimdallIndex::open_lightweight(path)?
    } else {
        HeimdallIndex::open(path)?
    };

    let toml_path = path.join("sv.toml");
    let normalizer = if toml_path.exists() {
        Normalizer::from_config(&toml_path)
    } else {
        Normalizer::swedish()
    };

    let addr_index = AddressIndex::open(path).ok().flatten();

    let geohash_index = {
        let gh_path = path.join("geohash_index.bin");
        if gh_path.exists() {
            GeohashIndex::open(&gh_path).ok()
        } else {
            None
        }
    };

    let dir_name = path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    let (code, bbox, name) = if dir_name.contains("denmark") || dir_name.ends_with("-dk") {
        (*b"DK", BoundingBox { min_lat: 54.5, max_lat: 57.8, min_lon: 8.0, max_lon: 15.2 }, "Denmark")
    } else if dir_name.contains("germany") || dir_name.ends_with("-de") {
        (*b"DE", BoundingBox { min_lat: 47.3, max_lat: 55.1, min_lon: 5.9, max_lon: 15.0 }, "Germany")
    } else if dir_name.contains("norway") || dir_name.ends_with("-no") {
        (*b"NO", BoundingBox { min_lat: 57.5, max_lat: 71.5, min_lon: 4.0, max_lon: 31.5 }, "Norway")
    } else if dir_name.contains("sweden") || dir_name.ends_with("-se") {
        (*b"SE", BoundingBox::sweden(), "Sweden")
    } else if dir_name.contains("finland") || dir_name.ends_with("-fi") {
        (*b"FI", BoundingBox { min_lat: 59.7, max_lat: 70.1, min_lon: 19.5, max_lon: 31.6 }, "Finland")
    } else if dir_name.contains("-gb") || dir_name.contains("-uk") || dir_name.contains("britain") {
        (*b"GB", BoundingBox { min_lat: 49.8, max_lat: 60.9, min_lon: -8.7, max_lon: 1.8 }, "Great Britain")
    } else if dir_name.contains("-us") || dir_name.contains("united-states") || dir_name.contains("america") {
        (*b"US", BoundingBox { min_lat: 24.5, max_lat: 49.4, min_lon: -125.0, max_lon: -66.9 }, "United States")
    } else if dir_name.contains("-au") || dir_name.contains("australia") {
        (*b"AU", BoundingBox { min_lat: -44.0, max_lat: -10.0, min_lon: 112.0, max_lon: 154.0 }, "Australia")
    } else if dir_name.contains("-ca") || dir_name.contains("canada") {
        (*b"CA", BoundingBox { min_lat: 41.7, max_lat: 83.5, min_lon: -141.0, max_lon: -52.0 }, "Canada")
    } else if dir_name.contains("-nz") || dir_name.contains("new-zealand") || dir_name.contains("newzealand") {
        (*b"NZ", BoundingBox { min_lat: -47.5, max_lat: -34.0, min_lon: 166.0, max_lon: 179.0 }, "New Zealand")
    } else if dir_name.contains("-nl") || dir_name.contains("netherlands") {
        (*b"NL", BoundingBox { min_lat: 50.75, max_lat: 53.47, min_lon: 3.36, max_lon: 7.21 }, "Netherlands")
    } else if dir_name.contains("-be") || dir_name.contains("belgium") {
        (*b"BE", BoundingBox { min_lat: 49.50, max_lat: 51.50, min_lon: 2.55, max_lon: 6.41 }, "Belgium")
    } else if dir_name.contains("-fr") || dir_name.contains("france") {
        (*b"FR", BoundingBox { min_lat: 41.33, max_lat: 51.12, min_lon: -5.14, max_lon: 9.56 }, "France")
    } else if dir_name.contains("-ch") || dir_name.contains("switzerland") {
        (*b"CH", BoundingBox { min_lat: 45.82, max_lat: 47.81, min_lon: 5.96, max_lon: 10.49 }, "Switzerland")
    } else if dir_name.contains("-at") || dir_name.contains("austria") {
        (*b"AT", BoundingBox { min_lat: 46.37, max_lat: 49.02, min_lon: 9.53, max_lon: 17.16 }, "Austria")
    } else if dir_name.contains("-cz") || dir_name.contains("czech") {
        (*b"CZ", BoundingBox { min_lat: 48.55, max_lat: 51.06, min_lon: 12.09, max_lon: 18.86 }, "Czech Republic")
    } else if dir_name.contains("-pl") || dir_name.contains("poland") {
        (*b"PL", BoundingBox { min_lat: 49.00, max_lat: 54.84, min_lon: 14.12, max_lon: 24.15 }, "Poland")
    } else if dir_name.contains("-ee") || dir_name.contains("estonia") {
        (*b"EE", BoundingBox { min_lat: 57.51, max_lat: 59.68, min_lon: 21.76, max_lon: 28.21 }, "Estonia")
    } else if dir_name.contains("-lv") || dir_name.contains("latvia") {
        (*b"LV", BoundingBox { min_lat: 55.67, max_lat: 58.09, min_lon: 20.97, max_lon: 28.24 }, "Latvia")
    } else if dir_name.contains("-lt") || dir_name.contains("lithuania") {
        (*b"LT", BoundingBox { min_lat: 53.90, max_lat: 56.45, min_lon: 20.93, max_lon: 26.84 }, "Lithuania")
    } else if dir_name.contains("-jp") || dir_name.contains("japan") {
        (*b"JP", BoundingBox { min_lat: 24.0, max_lat: 45.5, min_lon: 122.9, max_lon: 153.0 }, "Japan")
    } else if dir_name.contains("-kr") || dir_name.contains("korea") {
        (*b"KR", BoundingBox { min_lat: 33.11, max_lat: 38.62, min_lon: 124.60, max_lon: 131.87 }, "South Korea")
    } else if dir_name.contains("-br") || dir_name.contains("brazil") {
        (*b"BR", BoundingBox { min_lat: -33.75, max_lat: 5.27, min_lon: -73.99, max_lon: -34.79 }, "Brazil")
    } else if dir_name.contains("-ad") || dir_name.contains("andorra") {
        (*b"AD", BoundingBox { min_lat: 42.43, max_lat: 42.66, min_lon: 1.41, max_lon: 1.79 }, "Andorra")
    } else if dir_name.contains("-ae") || dir_name.contains("uae") {
        (*b"AE", BoundingBox { min_lat: 22.63, max_lat: 26.08, min_lon: 51.58, max_lon: 56.38 }, "UAE")
    } else if dir_name.contains("-af") || dir_name.contains("afghanistan") {
        (*b"AF", BoundingBox { min_lat: 29.38, max_lat: 38.49, min_lon: 60.50, max_lon: 74.89 }, "Afghanistan")
    } else if dir_name.contains("-ag") || dir_name.contains("antigua-and-barbuda") {
        (*b"AG", BoundingBox { min_lat: 16.94, max_lat: 17.73, min_lon: -62.35, max_lon: -61.66 }, "Antigua and Barbuda")
    } else if dir_name.contains("-al") || dir_name.contains("albania") {
        (*b"AL", BoundingBox { min_lat: 39.64, max_lat: 42.66, min_lon: 19.26, max_lon: 21.06 }, "Albania")
    } else if dir_name.contains("-am") || dir_name.contains("armenia") {
        (*b"AM", BoundingBox { min_lat: 38.84, max_lat: 41.30, min_lon: 43.45, max_lon: 46.63 }, "Armenia")
    } else if dir_name.contains("-ao") || dir_name.contains("angola") {
        (*b"AO", BoundingBox { min_lat: -18.04, max_lat: 5.39, min_lon: 11.64, max_lon: 24.08 }, "Angola")
    } else if dir_name.contains("-ar") || dir_name.contains("argentina") {
        (*b"AR", BoundingBox { min_lat: -55.06, max_lat: -21.78, min_lon: -73.56, max_lon: -53.64 }, "Argentina")
    } else if dir_name.contains("-az") || dir_name.contains("azerbaijan") {
        (*b"AZ", BoundingBox { min_lat: 38.39, max_lat: 41.91, min_lon: 44.77, max_lon: 50.63 }, "Azerbaijan")
    } else if dir_name.contains("-ba") || dir_name.contains("bosnia-and-herzegovina") {
        (*b"BA", BoundingBox { min_lat: 42.56, max_lat: 45.28, min_lon: 15.72, max_lon: 19.62 }, "Bosnia and Herzegovina")
    } else if dir_name.contains("-bb") || dir_name.contains("barbados") {
        (*b"BB", BoundingBox { min_lat: 13.04, max_lat: 13.34, min_lon: -59.65, max_lon: -59.42 }, "Barbados")
    } else if dir_name.contains("-bd") || dir_name.contains("bangladesh") {
        (*b"BD", BoundingBox { min_lat: 20.74, max_lat: 26.63, min_lon: 88.01, max_lon: 92.67 }, "Bangladesh")
    } else if dir_name.contains("-bf") || dir_name.contains("burkina-faso") {
        (*b"BF", BoundingBox { min_lat: 9.39, max_lat: 15.08, min_lon: -5.52, max_lon: 2.41 }, "Burkina Faso")
    } else if dir_name.contains("-bg") || dir_name.contains("bulgaria") {
        (*b"BG", BoundingBox { min_lat: 41.24, max_lat: 44.22, min_lon: 22.36, max_lon: 28.61 }, "Bulgaria")
    } else if dir_name.contains("-bh") || dir_name.contains("bahrain") {
        (*b"BH", BoundingBox { min_lat: 25.79, max_lat: 26.29, min_lon: 50.45, max_lon: 50.82 }, "Bahrain")
    } else if dir_name.contains("-bi") || dir_name.contains("burundi") {
        (*b"BI", BoundingBox { min_lat: -4.47, max_lat: -2.31, min_lon: 28.99, max_lon: 30.85 }, "Burundi")
    } else if dir_name.contains("-bj") || dir_name.contains("benin") {
        (*b"BJ", BoundingBox { min_lat: 6.14, max_lat: 12.41, min_lon: 0.77, max_lon: 3.84 }, "Benin")
    } else if dir_name.contains("-bm") || dir_name.contains("bermuda") {
        (*b"BM", BoundingBox { min_lat: 32.25, max_lat: 32.39, min_lon: -64.89, max_lon: -64.64 }, "Bermuda")
    } else if dir_name.contains("-bo") || dir_name.contains("bolivia") {
        (*b"BO", BoundingBox { min_lat: -22.90, max_lat: -9.68, min_lon: -69.64, max_lon: -57.45 }, "Bolivia")
    } else if dir_name.contains("-bs") || dir_name.contains("bahamas") {
        (*b"BS", BoundingBox { min_lat: 20.91, max_lat: 27.26, min_lon: -80.48, max_lon: -72.71 }, "Bahamas")
    } else if dir_name.contains("-bt") || dir_name.contains("bhutan") {
        (*b"BT", BoundingBox { min_lat: 26.70, max_lat: 28.33, min_lon: 88.75, max_lon: 92.12 }, "Bhutan")
    } else if dir_name.contains("-bw") || dir_name.contains("botswana") {
        (*b"BW", BoundingBox { min_lat: -26.91, max_lat: -17.78, min_lon: 19.99, max_lon: 29.37 }, "Botswana")
    } else if dir_name.contains("-by") || dir_name.contains("belarus") {
        (*b"BY", BoundingBox { min_lat: 51.26, max_lat: 56.17, min_lon: 23.18, max_lon: 32.78 }, "Belarus")
    } else if dir_name.contains("-bz") || dir_name.contains("belize") {
        (*b"BZ", BoundingBox { min_lat: 15.89, max_lat: 18.50, min_lon: -89.22, max_lon: -87.49 }, "Belize")
    } else if dir_name.contains("-cd") || dir_name.contains("dr-congo") {
        (*b"CD", BoundingBox { min_lat: -13.46, max_lat: 5.39, min_lon: 12.18, max_lon: 31.31 }, "DR Congo")
    } else if dir_name.contains("-cf") || dir_name.contains("central-african-rep.") {
        (*b"CF", BoundingBox { min_lat: 2.22, max_lat: 11.01, min_lon: 14.42, max_lon: 27.46 }, "Central African Rep.")
    } else if dir_name.contains("-cg") || dir_name.contains("rep.-congo") {
        (*b"CG", BoundingBox { min_lat: -5.03, max_lat: 3.71, min_lon: 11.20, max_lon: 18.65 }, "Rep. Congo")
    } else if dir_name.contains("-ci") || dir_name.contains("cote-d'ivoire") {
        (*b"CI", BoundingBox { min_lat: 4.36, max_lat: 10.74, min_lon: -8.60, max_lon: -2.49 }, "Cote d'Ivoire")
    } else if dir_name.contains("-cl") || dir_name.contains("chile") {
        (*b"CL", BoundingBox { min_lat: -56.00, max_lat: -17.50, min_lon: -75.70, max_lon: -66.42 }, "Chile")
    } else if dir_name.contains("-cm") || dir_name.contains("cameroon") {
        (*b"CM", BoundingBox { min_lat: 1.65, max_lat: 13.08, min_lon: 8.49, max_lon: 16.19 }, "Cameroon")
    } else if dir_name.contains("-co") || dir_name.contains("colombia") {
        (*b"CO", BoundingBox { min_lat: -4.23, max_lat: 13.39, min_lon: -79.00, max_lon: -66.85 }, "Colombia")
    } else if dir_name.contains("-cr") || dir_name.contains("costa-rica") {
        (*b"CR", BoundingBox { min_lat: 5.50, max_lat: 11.22, min_lon: -85.95, max_lon: -82.55 }, "Costa Rica")
    } else if dir_name.contains("-cu") || dir_name.contains("cuba") {
        (*b"CU", BoundingBox { min_lat: 19.83, max_lat: 23.27, min_lon: -85.00, max_lon: -74.13 }, "Cuba")
    } else if dir_name.contains("-cv") || dir_name.contains("cape-verde") {
        (*b"CV", BoundingBox { min_lat: 14.80, max_lat: 17.21, min_lon: -25.36, max_lon: -22.66 }, "Cape Verde")
    } else if dir_name.contains("-cy") || dir_name.contains("cyprus") {
        (*b"CY", BoundingBox { min_lat: 34.57, max_lat: 35.70, min_lon: 32.27, max_lon: 34.60 }, "Cyprus")
    } else if dir_name.contains("-dj") || dir_name.contains("djibouti") {
        (*b"DJ", BoundingBox { min_lat: 10.93, max_lat: 12.71, min_lon: 41.77, max_lon: 43.42 }, "Djibouti")
    } else if dir_name.contains("-dm") || dir_name.contains("dominica") {
        (*b"DM", BoundingBox { min_lat: 15.20, max_lat: 15.65, min_lon: -61.48, max_lon: -61.24 }, "Dominica")
    } else if dir_name.contains("-do") || dir_name.contains("dominican-republic") {
        (*b"DO", BoundingBox { min_lat: 17.47, max_lat: 19.93, min_lon: -72.01, max_lon: -68.32 }, "Dominican Republic")
    } else if dir_name.contains("-dz") || dir_name.contains("algeria") {
        (*b"DZ", BoundingBox { min_lat: 18.97, max_lat: 37.09, min_lon: -8.67, max_lon: 11.98 }, "Algeria")
    } else if dir_name.contains("-ec") || dir_name.contains("ecuador") {
        (*b"EC", BoundingBox { min_lat: -5.01, max_lat: 1.68, min_lon: -81.08, max_lon: -75.19 }, "Ecuador")
    } else if dir_name.contains("-eg") || dir_name.contains("egypt") {
        (*b"EG", BoundingBox { min_lat: 22.00, max_lat: 31.67, min_lon: 24.70, max_lon: 36.90 }, "Egypt")
    } else if dir_name.contains("-er") || dir_name.contains("eritrea") {
        (*b"ER", BoundingBox { min_lat: 12.36, max_lat: 18.00, min_lon: 36.44, max_lon: 43.13 }, "Eritrea")
    } else if dir_name.contains("-es") || dir_name.contains("spain") {
        (*b"ES", BoundingBox { min_lat: 35.95, max_lat: 43.79, min_lon: -9.30, max_lon: 4.33 }, "Spain")
    } else if dir_name.contains("-et") || dir_name.contains("ethiopia") {
        (*b"ET", BoundingBox { min_lat: 3.40, max_lat: 14.89, min_lon: 32.99, max_lon: 47.99 }, "Ethiopia")
    } else if dir_name.contains("-ga") || dir_name.contains("gabon") {
        (*b"GA", BoundingBox { min_lat: -3.98, max_lat: 2.32, min_lon: 8.70, max_lon: 14.50 }, "Gabon")
    } else if dir_name.contains("-gd") || dir_name.contains("grenada") {
        (*b"GD", BoundingBox { min_lat: 11.99, max_lat: 12.32, min_lon: -61.80, max_lon: -61.58 }, "Grenada")
    } else if dir_name.contains("-ge") || dir_name.contains("georgia") {
        (*b"GE", BoundingBox { min_lat: 41.05, max_lat: 43.59, min_lon: 40.01, max_lon: 46.74 }, "Georgia")
    } else if dir_name.contains("-gh") || dir_name.contains("ghana") {
        (*b"GH", BoundingBox { min_lat: 4.74, max_lat: 11.17, min_lon: -3.26, max_lon: 1.19 }, "Ghana")
    } else if dir_name.contains("-gm") || dir_name.contains("gambia") {
        (*b"GM", BoundingBox { min_lat: 13.06, max_lat: 13.83, min_lon: -16.82, max_lon: -13.80 }, "Gambia")
    } else if dir_name.contains("-gn") || dir_name.contains("guinea") {
        (*b"GN", BoundingBox { min_lat: 7.19, max_lat: 12.67, min_lon: -15.08, max_lon: -7.64 }, "Guinea")
    } else if dir_name.contains("-gq") || dir_name.contains("equatorial-guinea") {
        (*b"GQ", BoundingBox { min_lat: 0.92, max_lat: 3.79, min_lon: 5.62, max_lon: 11.34 }, "Equatorial Guinea")
    } else if dir_name.contains("-gr") || dir_name.contains("greece") {
        (*b"GR", BoundingBox { min_lat: 34.80, max_lat: 41.75, min_lon: 19.37, max_lon: 29.65 }, "Greece")
    } else if dir_name.contains("-gt") || dir_name.contains("guatemala") {
        (*b"GT", BoundingBox { min_lat: 13.74, max_lat: 17.82, min_lon: -92.23, max_lon: -88.22 }, "Guatemala")
    } else if dir_name.contains("-gw") || dir_name.contains("guinea-bissau") {
        (*b"GW", BoundingBox { min_lat: 10.86, max_lat: 12.69, min_lon: -16.71, max_lon: -13.64 }, "Guinea-Bissau")
    } else if dir_name.contains("-gy") || dir_name.contains("guyana") {
        (*b"GY", BoundingBox { min_lat: 1.17, max_lat: 8.56, min_lon: -61.39, max_lon: -56.48 }, "Guyana")
    } else if dir_name.contains("-hn") || dir_name.contains("honduras") {
        (*b"HN", BoundingBox { min_lat: 12.98, max_lat: 16.51, min_lon: -89.35, max_lon: -83.15 }, "Honduras")
    } else if dir_name.contains("-hr") || dir_name.contains("croatia") {
        (*b"HR", BoundingBox { min_lat: 42.39, max_lat: 46.55, min_lon: 13.49, max_lon: 19.45 }, "Croatia")
    } else if dir_name.contains("-ht") || dir_name.contains("haiti") {
        (*b"HT", BoundingBox { min_lat: 18.02, max_lat: 20.09, min_lon: -74.48, max_lon: -71.62 }, "Haiti")
    } else if dir_name.contains("-hu") || dir_name.contains("hungary") {
        (*b"HU", BoundingBox { min_lat: 45.74, max_lat: 48.59, min_lon: 16.11, max_lon: 22.90 }, "Hungary")
    } else if dir_name.contains("-ie") || dir_name.contains("ireland") {
        (*b"IE", BoundingBox { min_lat: 51.42, max_lat: 55.39, min_lon: -10.48, max_lon: -5.99 }, "Ireland")
    } else if dir_name.contains("-il") || dir_name.contains("israel") {
        (*b"IL", BoundingBox { min_lat: 29.49, max_lat: 33.33, min_lon: 34.27, max_lon: 35.90 }, "Israel")
    } else if dir_name.contains("-in") || dir_name.contains("india") {
        (*b"IN", BoundingBox { min_lat: 6.75, max_lat: 35.50, min_lon: 68.19, max_lon: 97.40 }, "India")
    } else if dir_name.contains("-iq") || dir_name.contains("iraq") {
        (*b"IQ", BoundingBox { min_lat: 29.06, max_lat: 37.38, min_lon: 38.79, max_lon: 48.57 }, "Iraq")
    } else if dir_name.contains("-ir") || dir_name.contains("iran") {
        (*b"IR", BoundingBox { min_lat: 25.06, max_lat: 39.78, min_lon: 44.05, max_lon: 63.32 }, "Iran")
    } else if dir_name.contains("-is") || dir_name.contains("iceland") {
        (*b"IS", BoundingBox { min_lat: 63.30, max_lat: 66.56, min_lon: -24.55, max_lon: -13.50 }, "Iceland")
    } else if dir_name.contains("-it") || dir_name.contains("italy") {
        (*b"IT", BoundingBox { min_lat: 35.49, max_lat: 47.09, min_lon: 6.63, max_lon: 18.52 }, "Italy")
    } else if dir_name.contains("-jm") || dir_name.contains("jamaica") {
        (*b"JM", BoundingBox { min_lat: 17.70, max_lat: 18.53, min_lon: -78.37, max_lon: -76.18 }, "Jamaica")
    } else if dir_name.contains("-jo") || dir_name.contains("jordan") {
        (*b"JO", BoundingBox { min_lat: 29.19, max_lat: 33.37, min_lon: 34.96, max_lon: 39.30 }, "Jordan")
    } else if dir_name.contains("-ke") || dir_name.contains("kenya") {
        (*b"KE", BoundingBox { min_lat: -4.68, max_lat: 5.51, min_lon: 33.91, max_lon: 41.91 }, "Kenya")
    } else if dir_name.contains("-km") || dir_name.contains("comoros") {
        (*b"KM", BoundingBox { min_lat: -12.42, max_lat: -11.36, min_lon: 43.22, max_lon: 44.54 }, "Comoros")
    } else if dir_name.contains("-kn") || dir_name.contains("saint-kitts-and-nevis") {
        (*b"KN", BoundingBox { min_lat: 17.09, max_lat: 17.42, min_lon: -62.87, max_lon: -62.54 }, "Saint Kitts and Nevis")
    } else if dir_name.contains("-kw") || dir_name.contains("kuwait") {
        (*b"KW", BoundingBox { min_lat: 28.52, max_lat: 30.10, min_lon: 46.55, max_lon: 48.43 }, "Kuwait")
    } else if dir_name.contains("-lb") || dir_name.contains("lebanon") {
        (*b"LB", BoundingBox { min_lat: 33.05, max_lat: 34.69, min_lon: 35.10, max_lon: 36.63 }, "Lebanon")
    } else if dir_name.contains("-lc") || dir_name.contains("saint-lucia") {
        (*b"LC", BoundingBox { min_lat: 13.71, max_lat: 14.11, min_lon: -61.08, max_lon: -60.87 }, "Saint Lucia")
    } else if dir_name.contains("-lk") || dir_name.contains("sri-lanka") {
        (*b"LK", BoundingBox { min_lat: 5.92, max_lat: 9.84, min_lon: 79.65, max_lon: 81.88 }, "Sri Lanka")
    } else if dir_name.contains("-lr") || dir_name.contains("liberia") {
        (*b"LR", BoundingBox { min_lat: 4.35, max_lat: 8.55, min_lon: -11.49, max_lon: -7.37 }, "Liberia")
    } else if dir_name.contains("-ls") || dir_name.contains("lesotho") {
        (*b"LS", BoundingBox { min_lat: -30.67, max_lat: -28.57, min_lon: 27.01, max_lon: 29.46 }, "Lesotho")
    } else if dir_name.contains("-lu") || dir_name.contains("luxembourg") {
        (*b"LU", BoundingBox { min_lat: 49.45, max_lat: 50.18, min_lon: 5.73, max_lon: 6.53 }, "Luxembourg")
    } else if dir_name.contains("-ly") || dir_name.contains("libya") {
        (*b"LY", BoundingBox { min_lat: 19.50, max_lat: 33.17, min_lon: 9.39, max_lon: 25.15 }, "Libya")
    } else if dir_name.contains("-ma") || dir_name.contains("morocco") {
        (*b"MA", BoundingBox { min_lat: 27.67, max_lat: 35.92, min_lon: -13.17, max_lon: -1.01 }, "Morocco")
    } else if dir_name.contains("-mc") || dir_name.contains("monaco") {
        (*b"MC", BoundingBox { min_lat: 43.72, max_lat: 43.75, min_lon: 7.41, max_lon: 7.44 }, "Monaco")
    } else if dir_name.contains("-md") || dir_name.contains("moldova") {
        (*b"MD", BoundingBox { min_lat: 45.47, max_lat: 48.49, min_lon: 26.62, max_lon: 30.16 }, "Moldova")
    } else if dir_name.contains("-me") || dir_name.contains("montenegro") {
        (*b"ME", BoundingBox { min_lat: 41.85, max_lat: 43.56, min_lon: 18.43, max_lon: 20.36 }, "Montenegro")
    } else if dir_name.contains("-mg") || dir_name.contains("madagascar") {
        (*b"MG", BoundingBox { min_lat: -25.61, max_lat: -11.95, min_lon: 43.19, max_lon: 50.48 }, "Madagascar")
    } else if dir_name.contains("-mk") || dir_name.contains("north-macedonia") {
        (*b"MK", BoundingBox { min_lat: 40.85, max_lat: 42.37, min_lon: 20.45, max_lon: 23.04 }, "North Macedonia")
    } else if dir_name.contains("-ml") || dir_name.contains("mali") {
        (*b"ML", BoundingBox { min_lat: 10.16, max_lat: 25.00, min_lon: -12.24, max_lon: 4.27 }, "Mali")
    } else if dir_name.contains("-mr") || dir_name.contains("mauritania") {
        (*b"MR", BoundingBox { min_lat: 14.72, max_lat: 27.30, min_lon: -17.07, max_lon: -4.83 }, "Mauritania")
    } else if dir_name.contains("-mt") || dir_name.contains("malta") {
        (*b"MT", BoundingBox { min_lat: 35.81, max_lat: 36.08, min_lon: 14.18, max_lon: 14.58 }, "Malta")
    } else if dir_name.contains("-mu") || dir_name.contains("mauritius") {
        (*b"MU", BoundingBox { min_lat: -20.53, max_lat: -19.97, min_lon: 57.30, max_lon: 57.81 }, "Mauritius")
    } else if dir_name.contains("-mv") || dir_name.contains("maldives") {
        (*b"MV", BoundingBox { min_lat: -0.69, max_lat: 7.11, min_lon: 72.64, max_lon: 73.76 }, "Maldives")
    } else if dir_name.contains("-mw") || dir_name.contains("malawi") {
        (*b"MW", BoundingBox { min_lat: -17.13, max_lat: -9.37, min_lon: 32.67, max_lon: 35.92 }, "Malawi")
    } else if dir_name.contains("-mx") || dir_name.contains("mexico") {
        (*b"MX", BoundingBox { min_lat: 14.53, max_lat: 32.72, min_lon: -118.60, max_lon: -86.70 }, "Mexico")
    } else if dir_name.contains("-mz") || dir_name.contains("mozambique") {
        (*b"MZ", BoundingBox { min_lat: -26.87, max_lat: -10.47, min_lon: 30.21, max_lon: 40.84 }, "Mozambique")
    } else if dir_name.contains("-na") || dir_name.contains("namibia") {
        (*b"NA", BoundingBox { min_lat: -28.97, max_lat: -16.96, min_lon: 11.72, max_lon: 25.26 }, "Namibia")
    } else if dir_name.contains("-ne") || dir_name.contains("niger") {
        (*b"NE", BoundingBox { min_lat: 11.69, max_lat: 23.53, min_lon: 0.17, max_lon: 15.99 }, "Niger")
    } else if dir_name.contains("-ng") || dir_name.contains("nigeria") {
        (*b"NG", BoundingBox { min_lat: 4.27, max_lat: 13.89, min_lon: 2.69, max_lon: 14.68 }, "Nigeria")
    } else if dir_name.contains("-ni") || dir_name.contains("nicaragua") {
        (*b"NI", BoundingBox { min_lat: 10.71, max_lat: 15.03, min_lon: -87.69, max_lon: -82.73 }, "Nicaragua")
    } else if dir_name.contains("-np") || dir_name.contains("nepal") {
        (*b"NP", BoundingBox { min_lat: 26.36, max_lat: 30.45, min_lon: 80.06, max_lon: 88.20 }, "Nepal")
    } else if dir_name.contains("-om") || dir_name.contains("oman") {
        (*b"OM", BoundingBox { min_lat: 16.65, max_lat: 26.39, min_lon: 52.00, max_lon: 59.84 }, "Oman")
    } else if dir_name.contains("-pa") || dir_name.contains("panama") {
        (*b"PA", BoundingBox { min_lat: 7.20, max_lat: 9.65, min_lon: -83.05, max_lon: -77.17 }, "Panama")
    } else if dir_name.contains("-pe") || dir_name.contains("peru") {
        (*b"PE", BoundingBox { min_lat: -18.35, max_lat: -0.04, min_lon: -81.33, max_lon: -68.65 }, "Peru")
    } else if dir_name.contains("-pk") || dir_name.contains("pakistan") {
        (*b"PK", BoundingBox { min_lat: 23.69, max_lat: 37.08, min_lon: 60.87, max_lon: 77.84 }, "Pakistan")
    } else if dir_name.contains("-ps") || dir_name.contains("palestine") {
        (*b"PS", BoundingBox { min_lat: 31.22, max_lat: 32.55, min_lon: 34.22, max_lon: 35.57 }, "Palestine")
    } else if dir_name.contains("-pt") || dir_name.contains("portugal") {
        (*b"PT", BoundingBox { min_lat: 36.96, max_lat: 42.15, min_lon: -9.53, max_lon: -6.19 }, "Portugal")
    } else if dir_name.contains("-py") || dir_name.contains("paraguay") {
        (*b"PY", BoundingBox { min_lat: -27.59, max_lat: -19.29, min_lon: -62.65, max_lon: -54.26 }, "Paraguay")
    } else if dir_name.contains("-qa") || dir_name.contains("qatar") {
        (*b"QA", BoundingBox { min_lat: 24.47, max_lat: 26.18, min_lon: 50.75, max_lon: 51.64 }, "Qatar")
    } else if dir_name.contains("-ro") || dir_name.contains("romania") {
        (*b"RO", BoundingBox { min_lat: 43.62, max_lat: 48.27, min_lon: 20.26, max_lon: 29.69 }, "Romania")
    } else if dir_name.contains("-rs") || dir_name.contains("serbia") {
        (*b"RS", BoundingBox { min_lat: 42.23, max_lat: 46.19, min_lon: 18.82, max_lon: 23.01 }, "Serbia")
    } else if dir_name.contains("-ru") || dir_name.contains("russia") {
        (*b"RU", BoundingBox { min_lat: 41.19, max_lat: 81.86, min_lon: 19.64, max_lon: 180.0 }, "Russia")
    } else if dir_name.contains("-rw") || dir_name.contains("rwanda") {
        (*b"RW", BoundingBox { min_lat: -2.84, max_lat: -1.05, min_lon: 28.86, max_lon: 30.90 }, "Rwanda")
    } else if dir_name.contains("-sa") || dir_name.contains("saudi-arabia") {
        (*b"SA", BoundingBox { min_lat: 16.38, max_lat: 32.15, min_lon: 34.57, max_lon: 55.67 }, "Saudi Arabia")
    } else if dir_name.contains("-sc") || dir_name.contains("seychelles") {
        (*b"SC", BoundingBox { min_lat: -9.76, max_lat: -4.28, min_lon: 46.20, max_lon: 56.30 }, "Seychelles")
    } else if dir_name.contains("-sd") || dir_name.contains("sudan") {
        (*b"SD", BoundingBox { min_lat: 8.68, max_lat: 22.23, min_lon: 21.81, max_lon: 38.61 }, "Sudan")
    } else if dir_name.contains("-si") || dir_name.contains("slovenia") {
        (*b"SI", BoundingBox { min_lat: 45.42, max_lat: 46.88, min_lon: 13.38, max_lon: 16.61 }, "Slovenia")
    } else if dir_name.contains("-sk") || dir_name.contains("slovakia") {
        (*b"SK", BoundingBox { min_lat: 47.73, max_lat: 49.61, min_lon: 16.83, max_lon: 22.57 }, "Slovakia")
    } else if dir_name.contains("-sl") || dir_name.contains("sierra-leone") {
        (*b"SL", BoundingBox { min_lat: 6.93, max_lat: 10.00, min_lon: -13.30, max_lon: -10.28 }, "Sierra Leone")
    } else if dir_name.contains("-sm") || dir_name.contains("san-marino") {
        (*b"SM", BoundingBox { min_lat: 43.89, max_lat: 43.99, min_lon: 12.40, max_lon: 12.52 }, "San Marino")
    } else if dir_name.contains("-sn") || dir_name.contains("senegal") {
        (*b"SN", BoundingBox { min_lat: 12.31, max_lat: 16.69, min_lon: -17.54, max_lon: -11.36 }, "Senegal")
    } else if dir_name.contains("-so") || dir_name.contains("somalia") {
        (*b"SO", BoundingBox { min_lat: -1.68, max_lat: 11.99, min_lon: 40.99, max_lon: 51.41 }, "Somalia")
    } else if dir_name.contains("-sr") || dir_name.contains("suriname") {
        (*b"SR", BoundingBox { min_lat: 1.83, max_lat: 6.01, min_lon: -58.07, max_lon: -53.98 }, "Suriname")
    } else if dir_name.contains("-ss") || dir_name.contains("south-sudan") {
        (*b"SS", BoundingBox { min_lat: 3.49, max_lat: 12.24, min_lon: 23.44, max_lon: 35.95 }, "South Sudan")
    } else if dir_name.contains("-st") || dir_name.contains("sao-tome") {
        (*b"ST", BoundingBox { min_lat: 0.02, max_lat: 1.70, min_lon: 6.47, max_lon: 7.47 }, "Sao Tome")
    } else if dir_name.contains("-sv") || dir_name.contains("el-salvador") {
        (*b"SV", BoundingBox { min_lat: 13.15, max_lat: 14.45, min_lon: -90.13, max_lon: -87.68 }, "El Salvador")
    } else if dir_name.contains("-sy") || dir_name.contains("syria") {
        (*b"SY", BoundingBox { min_lat: 32.31, max_lat: 37.32, min_lon: 35.73, max_lon: 42.38 }, "Syria")
    } else if dir_name.contains("-sz") || dir_name.contains("eswatini") {
        (*b"SZ", BoundingBox { min_lat: -27.32, max_lat: -25.72, min_lon: 30.79, max_lon: 32.14 }, "Eswatini")
    } else if dir_name.contains("-td") || dir_name.contains("chad") {
        (*b"TD", BoundingBox { min_lat: 7.44, max_lat: 23.45, min_lon: 13.47, max_lon: 24.00 }, "Chad")
    } else if dir_name.contains("-tg") || dir_name.contains("togo") {
        (*b"TG", BoundingBox { min_lat: 6.10, max_lat: 11.14, min_lon: -0.15, max_lon: 1.81 }, "Togo")
    } else if dir_name.contains("-tn") || dir_name.contains("tunisia") {
        (*b"TN", BoundingBox { min_lat: 30.23, max_lat: 37.54, min_lon: 7.52, max_lon: 11.60 }, "Tunisia")
    } else if dir_name.contains("-tr") || dir_name.contains("turkey") {
        (*b"TR", BoundingBox { min_lat: 35.81, max_lat: 42.11, min_lon: 25.66, max_lon: 44.82 }, "Turkey")
    } else if dir_name.contains("-tt") || dir_name.contains("trinidad-and-tobago") {
        (*b"TT", BoundingBox { min_lat: 10.04, max_lat: 11.36, min_lon: -61.93, max_lon: -60.52 }, "Trinidad and Tobago")
    } else if dir_name.contains("-tz") || dir_name.contains("tanzania") {
        (*b"TZ", BoundingBox { min_lat: -11.75, max_lat: -0.99, min_lon: 29.33, max_lon: 40.44 }, "Tanzania")
    } else if dir_name.contains("-ua") || dir_name.contains("ukraine") {
        (*b"UA", BoundingBox { min_lat: 44.39, max_lat: 52.38, min_lon: 22.13, max_lon: 40.23 }, "Ukraine")
    } else if dir_name.contains("-ug") || dir_name.contains("uganda") {
        (*b"UG", BoundingBox { min_lat: -1.48, max_lat: 4.23, min_lon: 29.57, max_lon: 35.04 }, "Uganda")
    } else if dir_name.contains("-uy") || dir_name.contains("uruguay") {
        (*b"UY", BoundingBox { min_lat: -35.03, max_lat: -30.09, min_lon: -58.44, max_lon: -53.09 }, "Uruguay")
    } else if dir_name.contains("-va") || dir_name.contains("vatican-city") {
        (*b"VA", BoundingBox { min_lat: 41.90, max_lat: 41.91, min_lon: 12.45, max_lon: 12.46 }, "Vatican City")
    } else if dir_name.contains("-vc") || dir_name.contains("saint-vincent-and-the-grenadines") {
        (*b"VC", BoundingBox { min_lat: 12.58, max_lat: 13.38, min_lon: -61.46, max_lon: -61.11 }, "Saint Vincent and the Grenadines")
    } else if dir_name.contains("-ve") || dir_name.contains("venezuela") {
        (*b"VE", BoundingBox { min_lat: 0.65, max_lat: 12.20, min_lon: -73.38, max_lon: -59.80 }, "Venezuela")
    } else if dir_name.contains("-xk") || dir_name.contains("kosovo") {
        (*b"XK", BoundingBox { min_lat: 41.86, max_lat: 43.27, min_lon: 20.01, max_lon: 21.79 }, "Kosovo")
    } else if dir_name.contains("-ye") || dir_name.contains("yemen") {
        (*b"YE", BoundingBox { min_lat: 12.11, max_lat: 19.00, min_lon: 42.55, max_lon: 54.53 }, "Yemen")
    } else if dir_name.contains("-za") || dir_name.contains("south-africa") {
        (*b"ZA", BoundingBox { min_lat: -34.84, max_lat: -22.13, min_lon: 16.45, max_lon: 32.89 }, "South Africa")
    } else if dir_name.contains("-zm") || dir_name.contains("zambia") {
        (*b"ZM", BoundingBox { min_lat: -18.08, max_lat: -8.22, min_lon: 21.99, max_lon: 33.71 }, "Zambia")
    } else if dir_name.contains("-zw") || dir_name.contains("zimbabwe") {
        (*b"ZW", BoundingBox { min_lat: -22.42, max_lat: -15.61, min_lon: 25.24, max_lon: 33.07 }, "Zimbabwe")
    } else if dir_name.contains("-ae") || dir_name.contains("uae") {
        (*b"AE", BoundingBox { min_lat: 22.63, max_lat: 26.08, min_lon: 51.58, max_lon: 56.38 }, "UAE")
    } else if dir_name.contains("-af") || dir_name.contains("afghanistan") {
        (*b"AF", BoundingBox { min_lat: 29.38, max_lat: 38.49, min_lon: 60.50, max_lon: 74.89 }, "Afghanistan")
    } else if dir_name.contains("-bd") || dir_name.contains("bangladesh") {
        (*b"BD", BoundingBox { min_lat: 20.74, max_lat: 26.63, min_lon: 88.01, max_lon: 92.67 }, "Bangladesh")
    } else if dir_name.contains("-bh") || dir_name.contains("bahrain") {
        (*b"BH", BoundingBox { min_lat: 25.79, max_lat: 26.29, min_lon: 50.45, max_lon: 50.82 }, "Bahrain")
    } else if dir_name.contains("-bn") || dir_name.contains("brunei") {
        (*b"BN", BoundingBox { min_lat: 4.00, max_lat: 5.05, min_lon: 114.00, max_lon: 115.37 }, "Brunei")
    } else if dir_name.contains("-bt") || dir_name.contains("bhutan") {
        (*b"BT", BoundingBox { min_lat: 26.70, max_lat: 28.33, min_lon: 88.75, max_lon: 92.12 }, "Bhutan")
    } else if dir_name.contains("-ck") || dir_name.contains("cook-islands") {
        (*b"CK", BoundingBox { min_lat: -21.95, max_lat: -8.95, min_lon: -165.85, max_lon: -157.31 }, "Cook Islands")
    } else if dir_name.contains("-cn") || dir_name.contains("china") {
        (*b"CN", BoundingBox { min_lat: 18.16, max_lat: 53.56, min_lon: 73.50, max_lon: 134.77 }, "China")
    } else if dir_name.contains("-fj") || dir_name.contains("fiji") {
        (*b"FJ", BoundingBox { min_lat: -20.68, max_lat: -12.48, min_lon: 176.00, max_lon: -178.00 }, "Fiji")
    } else if dir_name.contains("-fm") || dir_name.contains("micronesia") {
        (*b"FM", BoundingBox { min_lat: 1.03, max_lat: 10.09, min_lon: 137.33, max_lon: 163.04 }, "Micronesia")
    } else if dir_name.contains("-id") || dir_name.contains("indonesia") {
        (*b"ID", BoundingBox { min_lat: -11.00, max_lat: 5.91, min_lon: 95.01, max_lon: 141.02 }, "Indonesia")
    } else if dir_name.contains("-il") || dir_name.contains("israel") {
        (*b"IL", BoundingBox { min_lat: 29.49, max_lat: 33.33, min_lon: 34.27, max_lon: 35.90 }, "Israel")
    } else if dir_name.contains("-in") || dir_name.contains("india") {
        (*b"IN", BoundingBox { min_lat: 6.75, max_lat: 35.50, min_lon: 68.19, max_lon: 97.40 }, "India")
    } else if dir_name.contains("-iq") || dir_name.contains("iraq") {
        (*b"IQ", BoundingBox { min_lat: 29.06, max_lat: 37.38, min_lon: 38.79, max_lon: 48.57 }, "Iraq")
    } else if dir_name.contains("-ir") || dir_name.contains("iran") {
        (*b"IR", BoundingBox { min_lat: 25.06, max_lat: 39.78, min_lon: 44.05, max_lon: 63.32 }, "Iran")
    } else if dir_name.contains("-jo") || dir_name.contains("jordan") {
        (*b"JO", BoundingBox { min_lat: 29.19, max_lat: 33.37, min_lon: 34.96, max_lon: 39.30 }, "Jordan")
    } else if dir_name.contains("-kh") || dir_name.contains("cambodia") {
        (*b"KH", BoundingBox { min_lat: 10.41, max_lat: 14.69, min_lon: 102.34, max_lon: 107.63 }, "Cambodia")
    } else if dir_name.contains("-ki") || dir_name.contains("kiribati") {
        (*b"KI", BoundingBox { min_lat: -11.45, max_lat: 4.72, min_lon: -174.54, max_lon: 176.85 }, "Kiribati")
    } else if dir_name.contains("-kw") || dir_name.contains("kuwait") {
        (*b"KW", BoundingBox { min_lat: 28.52, max_lat: 30.10, min_lon: 46.55, max_lon: 48.43 }, "Kuwait")
    } else if dir_name.contains("-la") || dir_name.contains("laos") {
        (*b"LA", BoundingBox { min_lat: 13.91, max_lat: 22.50, min_lon: 100.08, max_lon: 107.64 }, "Laos")
    } else if dir_name.contains("-lb") || dir_name.contains("lebanon") {
        (*b"LB", BoundingBox { min_lat: 33.05, max_lat: 34.69, min_lon: 35.10, max_lon: 36.63 }, "Lebanon")
    } else if dir_name.contains("-lk") || dir_name.contains("sri-lanka") {
        (*b"LK", BoundingBox { min_lat: 5.92, max_lat: 9.84, min_lon: 79.65, max_lon: 81.88 }, "Sri Lanka")
    } else if dir_name.contains("-mh") || dir_name.contains("marshall-islands") {
        (*b"MH", BoundingBox { min_lat: 4.57, max_lat: 14.62, min_lon: 160.80, max_lon: 172.17 }, "Marshall Islands")
    } else if dir_name.contains("-mm") || dir_name.contains("myanmar") {
        (*b"MM", BoundingBox { min_lat: 9.78, max_lat: 28.55, min_lon: 92.19, max_lon: 101.17 }, "Myanmar")
    } else if dir_name.contains("-mn") || dir_name.contains("mongolia") {
        (*b"MN", BoundingBox { min_lat: 41.58, max_lat: 52.15, min_lon: 87.75, max_lon: 119.93 }, "Mongolia")
    } else if dir_name.contains("-mp") || dir_name.contains("northern-mariana-islands") {
        (*b"MP", BoundingBox { min_lat: 14.11, max_lat: 20.56, min_lon: 144.89, max_lon: 146.07 }, "Northern Mariana Islands")
    } else if dir_name.contains("-mv") || dir_name.contains("maldives") {
        (*b"MV", BoundingBox { min_lat: -0.69, max_lat: 7.11, min_lon: 72.64, max_lon: 73.76 }, "Maldives")
    } else if dir_name.contains("-my") || dir_name.contains("malaysia") {
        (*b"MY", BoundingBox { min_lat: 0.85, max_lat: 7.36, min_lon: 99.64, max_lon: 119.27 }, "Malaysia")
    } else if dir_name.contains("-np") || dir_name.contains("nepal") {
        (*b"NP", BoundingBox { min_lat: 26.36, max_lat: 30.45, min_lon: 80.06, max_lon: 88.20 }, "Nepal")
    } else if dir_name.contains("-nr") || dir_name.contains("nauru") {
        (*b"NR", BoundingBox { min_lat: -0.56, max_lat: -0.50, min_lon: 166.90, max_lon: 166.96 }, "Nauru")
    } else if dir_name.contains("-nu") || dir_name.contains("niue") {
        (*b"NU", BoundingBox { min_lat: -19.15, max_lat: -18.95, min_lon: -169.95, max_lon: -169.78 }, "Niue")
    } else if dir_name.contains("-om") || dir_name.contains("oman") {
        (*b"OM", BoundingBox { min_lat: 16.65, max_lat: 26.39, min_lon: 52.00, max_lon: 59.84 }, "Oman")
    } else if dir_name.contains("-pg") || dir_name.contains("papua-new-guinea") {
        (*b"PG", BoundingBox { min_lat: -11.66, max_lat: -0.73, min_lon: 140.84, max_lon: 159.50 }, "Papua New Guinea")
    } else if dir_name.contains("-ph") || dir_name.contains("philippines") {
        (*b"PH", BoundingBox { min_lat: 4.59, max_lat: 21.12, min_lon: 116.93, max_lon: 126.60 }, "Philippines")
    } else if dir_name.contains("-pk") || dir_name.contains("pakistan") {
        (*b"PK", BoundingBox { min_lat: 23.69, max_lat: 37.08, min_lon: 60.87, max_lon: 77.84 }, "Pakistan")
    } else if dir_name.contains("-ps") || dir_name.contains("palestine") {
        (*b"PS", BoundingBox { min_lat: 31.22, max_lat: 32.55, min_lon: 34.22, max_lon: 35.57 }, "Palestine")
    } else if dir_name.contains("-pw") || dir_name.contains("palau") {
        (*b"PW", BoundingBox { min_lat: 2.80, max_lat: 8.10, min_lon: 131.12, max_lon: 134.73 }, "Palau")
    } else if dir_name.contains("-qa") || dir_name.contains("qatar") {
        (*b"QA", BoundingBox { min_lat: 24.47, max_lat: 26.18, min_lon: 50.75, max_lon: 51.64 }, "Qatar")
    } else if dir_name.contains("-sa") || dir_name.contains("saudi-arabia") {
        (*b"SA", BoundingBox { min_lat: 16.38, max_lat: 32.15, min_lon: 34.57, max_lon: 55.67 }, "Saudi Arabia")
    } else if dir_name.contains("-sb") || dir_name.contains("solomon-islands") {
        (*b"SB", BoundingBox { min_lat: -12.31, max_lat: -5.10, min_lon: 155.51, max_lon: 170.19 }, "Solomon Islands")
    } else if dir_name.contains("-sg") || dir_name.contains("singapore") {
        (*b"SG", BoundingBox { min_lat: 1.16, max_lat: 1.47, min_lon: 103.60, max_lon: 104.41 }, "Singapore")
    } else if dir_name.contains("-sy") || dir_name.contains("syria") {
        (*b"SY", BoundingBox { min_lat: 32.31, max_lat: 37.32, min_lon: 35.73, max_lon: 42.38 }, "Syria")
    } else if dir_name.contains("-th") || dir_name.contains("thailand") {
        (*b"TH", BoundingBox { min_lat: 5.61, max_lat: 20.46, min_lon: 97.34, max_lon: 105.64 }, "Thailand")
    } else if dir_name.contains("-tl") || dir_name.contains("timor-leste") {
        (*b"TL", BoundingBox { min_lat: -9.50, max_lat: -8.13, min_lon: 124.04, max_lon: 127.34 }, "Timor-Leste")
    } else if dir_name.contains("-to") || dir_name.contains("tonga") {
        (*b"TO", BoundingBox { min_lat: -22.35, max_lat: -15.56, min_lon: -176.22, max_lon: -173.70 }, "Tonga")
    } else if dir_name.contains("-tv") || dir_name.contains("tuvalu") {
        (*b"TV", BoundingBox { min_lat: -10.80, max_lat: -5.64, min_lon: 176.06, max_lon: 179.87 }, "Tuvalu")
    } else if dir_name.contains("-tw") || dir_name.contains("taiwan") {
        (*b"TW", BoundingBox { min_lat: 21.90, max_lat: 25.30, min_lon: 120.00, max_lon: 122.01 }, "Taiwan")
    } else if dir_name.contains("-vn") || dir_name.contains("vietnam") {
        (*b"VN", BoundingBox { min_lat: 8.56, max_lat: 23.39, min_lon: 102.14, max_lon: 109.47 }, "Vietnam")
    } else if dir_name.contains("-vu") || dir_name.contains("vanuatu") {
        (*b"VU", BoundingBox { min_lat: -20.25, max_lat: -13.07, min_lon: 166.52, max_lon: 170.24 }, "Vanuatu")
    } else if dir_name.contains("-ws") || dir_name.contains("samoa") {
        (*b"WS", BoundingBox { min_lat: -14.08, max_lat: -13.43, min_lon: -172.80, max_lon: -171.40 }, "Samoa")
    } else if dir_name.contains("-ye") || dir_name.contains("yemen") {
        (*b"YE", BoundingBox { min_lat: 12.11, max_lat: 19.00, min_lon: 42.55, max_lon: 54.53 }, "Yemen")
    } else {
        (*b"SE", BoundingBox::sweden(), "Unknown")
    };

    let zip_index = ZipIndex::open(path).ok().flatten();

    let places = index.record_count();
    let addresses = addr_index.as_ref().map(|a| a.record_count()).unwrap_or(0);

    // Calculate runtime index size
    let index_size_bytes = runtime_index_size(path);
    let load_time_ms = t0.elapsed().as_millis() as u64;

    Ok(CountryIndex {
        code,
        name: name.to_string(),
        index,
        addr_index,
        zip_index,
        geohash_index,
        normalizer,
        bbox,
        meta: CountryMeta {
            places,
            addresses,
            index_size_bytes,
            load_time_ms,
        },
    })
}

fn runtime_index_size(path: &std::path::Path) -> u64 {
    let runtime_files = [
        "records.bin", "fst_exact.fst", "fst_phonetic.fst", "fst_ngram.fst",
        "admin.bin", "addr_streets.bin", "addr_records.bin", "fst_addr.fst",
        "geohash_index.bin", "fst_postcode.fst", "postcode_centroids.bin",
        "fst_zip.fst", "zip_records.bin", "sv.toml", "meta.json",
    ];
    runtime_files.iter()
        .map(|f| std::fs::metadata(path.join(f)).map(|m| m.len()).unwrap_or(0))
        .sum()
}

/// Discover index directories in standard locations.
fn discover_indices(extra_dirs: &[PathBuf]) -> Vec<PathBuf> {
    let mut dirs = Vec::new();

    let scan = |dir: &Path, out: &mut Vec<PathBuf>| {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if let Some(n) = name.to_str() {
                    if n.starts_with("index-") && entry.path().is_dir() {
                        out.push(entry.path());
                    }
                }
            }
        }
    };

    if extra_dirs.is_empty() {
        // No explicit --data-dir: fall back to ./data and ~/.heimdall/indices.
        scan(Path::new("data"), &mut dirs);
        let default_dir = fetch::default_data_dir();
        if default_dir.exists() {
            scan(&default_dir, &mut dirs);
        }
    } else {
        // Explicit --data-dir is authoritative — ignore implicit locations so
        // stray ./data/index-* dirs can't shift the country vec out of sync
        // with the global FST's country_order.json.
        for dir in extra_dirs {
            scan(dir, &mut dirs);
        }
    }

    // Canonicalize all paths before dedup to prevent double-loading when the
    // same directory appears as both a relative and absolute path (e.g.
    // "data/index-se" and "/root/heimdall/heimdall/data/index-se").
    dirs = dirs
        .into_iter()
        .filter_map(|p| std::fs::canonicalize(&p).ok())
        .collect();

    dirs.sort();
    dirs.dedup();
    dirs
}

fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Fetch {
            targets,
            list,
            update,
            mirror,
            data_dir,
        } => {
            let base_url = mirror.as_deref().unwrap_or(
                "https://github.com/martinarnell/heimdall/releases/latest/download",
            );
            let data_dir = data_dir.unwrap_or_else(fetch::default_data_dir);

            if list {
                fetch::list_available(base_url).await?;
            } else if update {
                fetch::check_updates(&data_dir, base_url).await?;
            } else if targets.is_empty() {
                println!("Usage: heimdall fetch <countries|bundles>");
                println!("       heimdall fetch --list");
                println!("\nExamples:");
                println!("  heimdall fetch se no     # download Sweden and Norway");
                println!("  heimdall fetch nordic    # download Nordic bundle (SE+NO+DK+FI)");
                println!("  heimdall fetch europe    # download all European countries");
                println!("  heimdall fetch world     # download everything");
            } else {
                fetch::fetch_targets(&targets, base_url, &data_dir).await?;
            }
        }

        Commands::Serve {
            index,
            country,
            bind,
            data_dir,
        } => {
            // Resolve index directories
            let index_paths: Vec<PathBuf> = if !index.is_empty() {
                // Explicit --index paths
                index
            } else if !country.is_empty() {
                // --country codes: discover and filter
                let all_dirs = discover_indices(&data_dir);
                let mut paths = Vec::new();
                for cc in &country {
                    let cc_lower = cc.to_lowercase();
                    let matching = all_dirs.iter().find(|d| {
                        let name = d.file_name().and_then(|n| n.to_str()).unwrap_or("");
                        match cc_lower.as_str() {
                            "se" => name.contains("sweden") || name.ends_with("-se"),
                            "no" => name.contains("norway") || name.ends_with("-no"),
                            "dk" => name.contains("denmark") || name.ends_with("-dk"),
                            "fi" => name.contains("finland") || name.ends_with("-fi"),
                            "de" => name.contains("germany") || name.ends_with("-de"),
                            "gb" => name.contains("-gb") || name.contains("-uk") || name.contains("britain"),
                            "us" => name.contains("-us") || name.contains("united-states"),
                            _ => name.contains(&cc_lower),
                        }
                    });
                    match matching {
                        Some(p) => paths.push(p.clone()),
                        None => {
                            eprintln!(
                                "Index for '{}' not found. Run 'heimdall fetch {}' to download it.",
                                cc, cc
                            );
                        }
                    }
                }
                if paths.is_empty() {
                    anyhow::bail!("No matching indices found");
                }
                paths
            } else {
                // Auto-discover
                let dirs = discover_indices(&data_dir);
                if dirs.is_empty() {
                    eprintln!("No index directories found.");
                    eprintln!("  Run 'heimdall fetch <country>' to download indices.");
                    eprintln!("  Or use '--index <path>' to specify an index directory.");
                    std::process::exit(1);
                }
                dirs
            };

            // Check if global name index exists (to enable lightweight loading)
            let has_global_index = {
                let candidates: Vec<PathBuf> = if !index_paths.is_empty() {
                    index_paths.iter()
                        .filter_map(|p| p.parent().map(|pp| pp.join("global")))
                        .collect()
                } else {
                    vec![PathBuf::from("data/global")]
                };
                candidates.iter().any(|d| d.join("fst_exact.fst").exists())
            };

            // Load indices — skip per-country FSTs when global FST will handle search
            let mode = if has_global_index { "lightweight (global FST)" } else { "full" };
            println!("\nLoading indices ({})...\n", mode);
            let mut countries = Vec::new();

            for index_path in &index_paths {
                let ci = load_country_index_inner(index_path, has_global_index)?;
                let code_str = std::str::from_utf8(&ci.code).unwrap_or("??");
                let zip_str = if ci.zip_index.is_some() { " + ZIP" } else { "" };
                println!(
                    "  {}  {:<16} {:>10} places  {:>12} addresses  {:>7.1} MB  [{}ms]{}",
                    code_str,
                    ci.name,
                    format_num(ci.meta.places),
                    format_num(ci.meta.addresses),
                    ci.meta.index_size_bytes as f64 / 1_048_576.0,
                    ci.meta.load_time_ms,
                    zip_str,
                );
                countries.push(ci);
            }

            let total_places: usize = countries.iter().map(|c| c.meta.places).sum();
            let total_addrs: usize = countries.iter().map(|c| c.meta.addresses).sum();
            let total_size: u64 = countries.iter().map(|c| c.meta.index_size_bytes).sum();

            println!();
            println!(
                "  Total: {} countries, {} places, {} addresses, {:.1} MB",
                countries.len(),
                format_num(total_places),
                format_num(total_addrs),
                total_size as f64 / 1_048_576.0,
            );

            // OSM ID map is built lazily on first /lookup request
            let osm_id_map = std::sync::OnceLock::new();

            // Build country code -> index mapping
            let country_id_map: HashMap<String, usize> = {
                let mut map = HashMap::new();
                for (i, c) in countries.iter().enumerate() {
                    let code = std::str::from_utf8(&c.code)
                        .unwrap_or("??")
                        .to_lowercase();
                    map.insert(code, i);
                }
                map
            };

            // Try to load global name index from a `global/` directory next
            // to the index directories.
            let global_index = {
                let candidate_dirs: Vec<PathBuf> = if !index_paths.is_empty() {
                    // Look for global/ next to any index's parent directory
                    let mut candidates: Vec<PathBuf> = index_paths
                        .iter()
                        .filter_map(|p| p.parent().map(|pp| pp.join("global")))
                        .collect();
                    candidates.sort();
                    candidates.dedup();
                    candidates
                } else {
                    vec![PathBuf::from("data/global")]
                };

                let mut loaded = None;
                for dir in &candidate_dirs {
                    match GlobalIndex::try_open(dir) {
                        Ok(Some(gi)) => {
                            println!("  Global name index loaded from {}", dir.display());
                            loaded = Some(gi);
                            break;
                        }
                        Ok(None) => {} // directory/files not present, skip
                        Err(e) => {
                            eprintln!(
                                "  Warning: failed to open global index at {}: {}",
                                dir.display(),
                                e
                            );
                        }
                    }
                }
                loaded
            };

            // Prometheus metrics
            let metrics_handle = metrics::init();
            metrics::record_index_info(
                countries.len(),
                countries.iter().map(|c| c.meta.places).sum(),
                countries.iter().map(|c| c.meta.addresses).sum(),
            );

            let state = Arc::new(AppState {
                countries,
                global_index,
                country_id_map,
                osm_id_map,
                started_at: std::time::SystemTime::now(),
                metrics_handle,
            });

            let app = Router::new()
                .route("/search", get(search))
                .route("/autocomplete", get(autocomplete))
                .route("/reverse", get(reverse))
                .route("/lookup", get(lookup))
                .route("/status", get(status))
                .route("/metrics", get(metrics::handler))
                .layer(axum::middleware::from_fn(metrics::track))
                .with_state(state);

            println!("\n  Listening on http://{}\n", bind);
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            axum::serve(listener, app).await?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helper to build SearchParams with defaults --

    fn default_search_params() -> SearchParams {
        SearchParams {
            q: None,
            amenity: None,
            street: None,
            city: None,
            county: None,
            state: None,
            country: None,
            postalcode: None,
            countrycodes: None,
            viewbox: None,
            bounded: 0,
            format: "json".to_owned(),
            limit: 5,
            addressdetails: 0,
        }
    }

    // -----------------------------------------------------------------------
    // Feature 1: Structured Query — parse_structured_query
    // -----------------------------------------------------------------------

    #[test]
    fn test_structured_no_params() {
        let p = default_search_params();
        assert!(!has_structured_params(&p));
        assert_eq!(parse_structured_query(&p), None);
    }

    #[test]
    fn test_structured_city_only() {
        let mut p = default_search_params();
        p.city = Some("Stockholm".into());
        assert!(has_structured_params(&p));
        assert_eq!(parse_structured_query(&p), Some("Stockholm".into()));
    }

    #[test]
    fn test_structured_street_and_city() {
        let mut p = default_search_params();
        p.street = Some("Kungsgatan 15".into());
        p.city = Some("Stockholm".into());
        assert_eq!(
            parse_structured_query(&p),
            Some("Kungsgatan 15, Stockholm".into())
        );
    }

    #[test]
    fn test_structured_all_fields() {
        let mut p = default_search_params();
        p.amenity = Some("café".into());
        p.street = Some("Kungsgatan".into());
        p.city = Some("Stockholm".into());
        p.county = Some("Stockholm County".into());
        p.state = Some("Stockholms län".into());
        p.postalcode = Some("11156".into());
        p.country = Some("Sweden".into());
        assert_eq!(
            parse_structured_query(&p),
            Some("café, Kungsgatan, Stockholm, Stockholm County, Stockholms län, 11156, Sweden".into())
        );
    }

    #[test]
    fn test_structured_empty_strings_ignored() {
        let mut p = default_search_params();
        p.street = Some("".into());
        p.city = Some("  ".into());
        p.country = Some("Sweden".into());
        // street and city are empty/whitespace, only country should appear
        assert_eq!(parse_structured_query(&p), Some("Sweden".into()));
    }

    #[test]
    fn test_structured_postalcode_only() {
        let mut p = default_search_params();
        p.postalcode = Some("11156".into());
        assert!(is_postalcode_only(&p));
        assert_eq!(parse_structured_query(&p), Some("11156".into()));
    }

    #[test]
    fn test_structured_postalcode_with_city_not_postalcode_only() {
        let mut p = default_search_params();
        p.postalcode = Some("11156".into());
        p.city = Some("Stockholm".into());
        assert!(!is_postalcode_only(&p));
    }

    #[test]
    fn test_structured_amenity_only() {
        let mut p = default_search_params();
        p.amenity = Some("hospital".into());
        assert!(is_amenity_only(&p));
        assert_eq!(parse_structured_query(&p), Some("hospital".into()));
    }

    #[test]
    fn test_structured_amenity_with_city_not_amenity_only() {
        let mut p = default_search_params();
        p.amenity = Some("hospital".into());
        p.city = Some("Berlin".into());
        assert!(!is_amenity_only(&p));
    }

    // -----------------------------------------------------------------------
    // Feature 3: Viewbox — parse_viewbox, result_in_bbox
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_viewbox_valid() {
        let bb = parse_viewbox("18.0,59.2,18.2,59.4").unwrap();
        assert!((bb.min_lon - 18.0).abs() < 1e-9);
        assert!((bb.min_lat - 59.2).abs() < 1e-9);
        assert!((bb.max_lon - 18.2).abs() < 1e-9);
        assert!((bb.max_lat - 59.4).abs() < 1e-9);
    }

    #[test]
    fn test_parse_viewbox_swaps_when_reversed() {
        let bb = parse_viewbox("18.2,59.4,18.0,59.2").unwrap();
        assert!(bb.min_lon <= bb.max_lon);
        assert!(bb.min_lat <= bb.max_lat);
        assert!((bb.min_lon - 18.0).abs() < 1e-9);
        assert!((bb.max_lat - 59.4).abs() < 1e-9);
    }

    #[test]
    fn test_parse_viewbox_invalid() {
        assert!(parse_viewbox("18.0,59.2").is_none());
        assert!(parse_viewbox("").is_none());
        assert!(parse_viewbox("a,b,c,d").is_none());
    }

    #[test]
    fn test_result_in_bbox_inside() {
        let bb = BoundingBox {
            min_lon: 18.0, min_lat: 59.2,
            max_lon: 18.2, max_lat: 59.4,
        };
        let r = NominatimResult {
            place_id: 1,
            osm_type: None,
            osm_id: None,
            display_name: "Test".into(),
            lat: "59.3".into(),
            lon: "18.1".into(),
            place_type: "city".into(),
            importance: 0.5,
            match_type: None,
            address: None,
        };
        assert!(result_in_bbox(&r, &bb));
    }

    #[test]
    fn test_result_in_bbox_outside() {
        let bb = BoundingBox {
            min_lon: 18.0, min_lat: 59.2,
            max_lon: 18.2, max_lat: 59.4,
        };
        let r = NominatimResult {
            place_id: 1,
            osm_type: None,
            osm_id: None,
            display_name: "Test".into(),
            lat: "57.7".into(),  // Gothenburg latitude
            lon: "11.9".into(),  // Gothenburg longitude
            place_type: "city".into(),
            importance: 0.5,
            match_type: None,
            address: None,
        };
        assert!(!result_in_bbox(&r, &bb));
    }

    #[test]
    fn test_result_in_bbox_on_edge() {
        let bb = BoundingBox {
            min_lon: 18.0, min_lat: 59.2,
            max_lon: 18.2, max_lat: 59.4,
        };
        let r = NominatimResult {
            place_id: 1,
            osm_type: None,
            osm_id: None,
            display_name: "Test".into(),
            lat: "59.2".into(),
            lon: "18.0".into(),
            place_type: "city".into(),
            importance: 0.5,
            match_type: None,
            address: None,
        };
        assert!(result_in_bbox(&r, &bb));
    }

    #[test]
    fn test_result_in_bbox_invalid_coords() {
        let bb = BoundingBox {
            min_lon: 18.0, min_lat: 59.2,
            max_lon: 18.2, max_lat: 59.4,
        };
        let r = NominatimResult {
            place_id: 1,
            osm_type: None,
            osm_id: None,
            display_name: "Test".into(),
            lat: "not_a_number".into(),
            lon: "18.1".into(),
            place_type: "city".into(),
            importance: 0.5,
            match_type: None,
            address: None,
        };
        assert!(!result_in_bbox(&r, &bb));
    }

    // -----------------------------------------------------------------------
    // Feature 5: Lookup — encode_place_id / decode_place_id
    // -----------------------------------------------------------------------

    #[test]
    fn test_place_id_roundtrip() {
        let (ci, rid) = (3_usize, 12345_u32);
        let encoded = encode_place_id(ci, rid);
        let (dci, drid) = decode_place_id(encoded);
        assert_eq!(dci, ci);
        assert_eq!(drid, rid);
    }

    #[test]
    fn test_place_id_zero() {
        let encoded = encode_place_id(0, 0);
        assert_eq!(encoded, 0);
        let (ci, rid) = decode_place_id(0);
        assert_eq!(ci, 0);
        assert_eq!(rid, 0);
    }

    #[test]
    fn test_place_id_max_record() {
        // 24-bit max = 16_777_215
        let encoded = encode_place_id(0, 0x00FF_FFFF);
        let (ci, rid) = decode_place_id(encoded);
        assert_eq!(ci, 0);
        assert_eq!(rid, 0x00FF_FFFF);
    }

    #[test]
    fn test_place_id_country_bits() {
        let encoded = encode_place_id(255, 0);
        let (ci, rid) = decode_place_id(encoded);
        assert_eq!(ci, 255);
        assert_eq!(rid, 0);
    }

    #[test]
    fn test_place_id_large_record_truncates() {
        // record_id > 24 bits gets masked
        let encoded = encode_place_id(1, 0x01FF_FFFF);
        let (_, rid) = decode_place_id(encoded);
        assert_eq!(rid, 0x00FF_FFFF); // upper bits masked off
    }

    #[test]
    fn test_osm_type_from_flags() {
        assert_eq!(osm_type_from_flags(0x00), "node");
        assert_eq!(osm_type_from_flags(0x08), "relation");
        assert_eq!(osm_type_from_flags(0x0F), "relation");
        assert_eq!(osm_type_from_flags(0x07), "node");
    }

    #[test]
    fn test_osm_type_char_from_flags() {
        assert_eq!(osm_type_char_from_flags(0x00), 'N');
        assert_eq!(osm_type_char_from_flags(0x08), 'R');
    }

    // -----------------------------------------------------------------------
    // Feature 2: addressdetails — place_type_to_settlement
    // -----------------------------------------------------------------------

    #[test]
    fn test_settlement_city() {
        let (city, town, village, suburb) = place_type_to_settlement(&PlaceType::City, "Stockholm");
        assert_eq!(city.as_deref(), Some("Stockholm"));
        assert!(town.is_none());
        assert!(village.is_none());
        assert!(suburb.is_none());
    }

    #[test]
    fn test_settlement_town() {
        let (city, town, village, suburb) = place_type_to_settlement(&PlaceType::Town, "Enköping");
        assert!(city.is_none());
        assert_eq!(town.as_deref(), Some("Enköping"));
        assert!(village.is_none());
        assert!(suburb.is_none());
    }

    #[test]
    fn test_settlement_village() {
        let (city, town, village, suburb) = place_type_to_settlement(&PlaceType::Village, "Kungsängen");
        assert!(city.is_none());
        assert!(town.is_none());
        assert_eq!(village.as_deref(), Some("Kungsängen"));
        assert!(suburb.is_none());
    }

    #[test]
    fn test_settlement_suburb() {
        let (city, town, village, suburb) = place_type_to_settlement(&PlaceType::Suburb, "Norrmalm");
        assert!(city.is_none());
        assert!(town.is_none());
        assert!(village.is_none());
        assert_eq!(suburb.as_deref(), Some("Norrmalm"));
    }

    #[test]
    fn test_settlement_hamlet() {
        let (city, town, village, _) = place_type_to_settlement(&PlaceType::Hamlet, "Sjöbacken");
        assert!(city.is_none());
        assert!(town.is_none());
        assert_eq!(village.as_deref(), Some("Sjöbacken"));
    }

    #[test]
    fn test_settlement_county_uses_city() {
        let (city, _, _, _) = place_type_to_settlement(&PlaceType::County, "Stockholms län");
        assert_eq!(city.as_deref(), Some("Stockholms län"));
    }

    // -----------------------------------------------------------------------
    // AddressDetails serialization
    // -----------------------------------------------------------------------

    #[test]
    fn test_address_details_skips_none_fields() {
        let ad = AddressDetails {
            house_number: None,
            road: Some("Kungsgatan".into()),
            suburb: None,
            city: Some("Stockholm".into()),
            town: None,
            village: None,
            county: None,
            state: None,
            postcode: None,
            country: Some("Sweden".into()),
            country_code: Some("se".into()),
        };
        let json = serde_json::to_string(&ad).unwrap();
        assert!(!json.contains("house_number"));
        assert!(!json.contains("suburb"));
        assert!(!json.contains("town"));
        assert!(!json.contains("village"));
        assert!(!json.contains("county"));
        assert!(!json.contains("state"));
        assert!(!json.contains("postcode"));
        assert!(json.contains("road"));
        assert!(json.contains("city"));
        assert!(json.contains("country"));
        assert!(json.contains("country_code"));
    }

    #[test]
    fn test_nominatim_result_omits_address_when_none() {
        let r = NominatimResult {
            place_id: 1,
            osm_type: None,
            osm_id: None,
            display_name: "Test".into(),
            lat: "59.3".into(),
            lon: "18.0".into(),
            place_type: "city".into(),
            importance: 0.5,
            match_type: None,
            address: None,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(!json.contains("address"));
        assert!(!json.contains("osm_type"));
        assert!(!json.contains("osm_id"));
        assert!(!json.contains("match_type"));
    }

    #[test]
    fn test_nominatim_result_includes_address_when_some() {
        let r = NominatimResult {
            place_id: 1,
            osm_type: None,
            osm_id: None,
            display_name: "Test".into(),
            lat: "59.3".into(),
            lon: "18.0".into(),
            place_type: "city".into(),
            importance: 0.5,
            match_type: None,
            address: Some(AddressDetails {
                house_number: None,
                road: None,
                suburb: None,
                city: Some("Stockholm".into()),
                town: None,
                village: None,
                county: None,
                state: None,
                postcode: None,
                country: Some("Sweden".into()),
                country_code: Some("se".into()),
            }),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"address\""));
        assert!(json.contains("Stockholm"));
    }

    // -----------------------------------------------------------------------
    // US preprocessing helpers (existing, but not previously tested)
    // -----------------------------------------------------------------------

    #[test]
    fn test_strip_us_unit_designator() {
        assert_eq!(strip_us_unit_designator("123 Main St Apt 4"), "123 Main St");
        assert_eq!(strip_us_unit_designator("123 Main St Suite 200"), "123 Main St");
        assert_eq!(strip_us_unit_designator("123 Main St #4"), "123 Main St");
        assert_eq!(strip_us_unit_designator("123 Main St"), "123 Main St");
    }

    #[test]
    fn test_strip_us_state_suffix() {
        let (rest, state) = strip_us_state_suffix("123 Main St, CA");
        assert_eq!(rest, "123 Main St");
        assert_eq!(state, Some("CA".into()));
    }

    #[test]
    fn test_strip_us_state_suffix_no_state() {
        let (rest, state) = strip_us_state_suffix("Stockholm");
        assert_eq!(rest, "Stockholm");
        assert!(state.is_none());
    }
}
