/// photon.rs — Import Photon JSONL dump into Heimdall index
///
/// Reads a .jsonl.zst Photon dump file and produces:
///   places.parquet    — place records (cities, towns, POIs, etc.)
///   addresses.parquet — street addresses with house numbers
///   admin.bin         — admin hierarchy (state → county)
///   admin_map.bin     — osm_id → (admin1_id, admin2_id) mapping
///
/// After import, the standard pack + pack_addr pipeline builds the final index.
///
/// Usage:
///   heimdall-build photon-import \
///     --input photon-dump-gb-1.0-latest.jsonl.zst \
///     --output data/index-gb

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::ArrowWriter;
use serde::Deserialize;
use tracing::info;

use heimdall_core::types::*;
use crate::extract::RawAddress;

// ---------------------------------------------------------------------------
// Serde types for Photon JSONL
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct PhotonLine {
    #[serde(rename = "type")]
    record_type: String,
    content: Option<serde_json::Value>,
}

/// A single place entry within the content array.
/// Fields follow the Nominatim Dump File Format spec (v0.1.0):
///   - centroid: [lon, lat]           (MUST)
///   - name: { "default": "...", ... } (OPTIONAL)
///   - housenumber: "..."             (top-level, OPTIONAL)
///   - postcode: "..."                (top-level, OPTIONAL)
///   - country_code: "gb"             (top-level, SHOULD)
///   - address: { "state": "...", "county": "...", "city": "...", "street": "..." }
#[derive(Deserialize)]
struct PhotonPlace {
    object_type: Option<String>,
    object_id: Option<i64>,
    osm_key: Option<String>,
    osm_value: Option<String>,
    #[serde(alias = "names")]
    name: Option<HashMap<String, serde_json::Value>>,
    address: Option<PhotonAddress>,
    centroid: Option<Vec<f64>>,
    geometry: Option<PhotonGeometry>,
    rank_address: Option<u8>,
    address_type: Option<String>,
    importance: Option<f64>,
    housenumber: Option<String>,
    postcode: Option<String>,
    country_code: Option<String>,
    /// Some Nominatim dumps (Photon variants) carry the Wikidata Q-id
    /// directly at the top level. Others put it under `extra`. Accept
    /// either via the alias.
    #[serde(alias = "extra_wikidata")]
    wikidata: Option<String>,
    extra: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Deserialize)]
struct PhotonAddress {
    street: Option<serde_json::Value>,
    city: Option<serde_json::Value>,
    county: Option<serde_json::Value>,
    state: Option<serde_json::Value>,
    country: Option<serde_json::Value>,
    district: Option<serde_json::Value>,
    locality: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct PhotonGeometry {
    coordinates: Option<Vec<f64>>,
}

/// Extract a string from a JSON value that may be a string or array of strings.
/// Per spec: "The values in the address field MUST be either a single string or
/// a list of strings. When a list of strings is given, data consumers SHOULD use
/// the first entry as the name to be displayed."
fn json_to_string(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Array(arr) => arr
            .first()
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Import result
// ---------------------------------------------------------------------------

pub struct PhotonImportResult {
    pub place_count: usize,
    pub address_count: usize,
    pub admin_count: usize,
}

/// Lightweight parse result — places + addresses, no admin hierarchy.
/// Used by merge-photon to enrich existing indices.
pub struct PhotonParseResult {
    pub places: Vec<RawPlace>,
    pub addresses: Vec<RawAddress>,
}

// ---------------------------------------------------------------------------
// Parse from Elasticsearch index (Lucene stored fields → RawPlace/RawAddress)
// ---------------------------------------------------------------------------

/// Convert Photon JSON documents (from Lucene `_source` fields) into
/// places and addresses. This is the entry point for reading Graphhopper's
/// tar.bz2 country dumps.
pub fn parse_es_documents(docs: &[serde_json::Value]) -> PhotonParseResult {
    let mut places: Vec<RawPlace> = Vec::new();
    let mut addresses: Vec<RawAddress> = Vec::new();

    for doc in docs {
        let osm_id = doc.get("osm_id").and_then(|v| v.as_i64()).unwrap_or(0);
        let osm_key = doc.get("osm_key").and_then(|v| v.as_str()).unwrap_or("");
        let osm_value = doc.get("osm_value").and_then(|v| v.as_str()).unwrap_or("");

        // Coordinate: {"lat": ..., "lon": ...} or {"coordinates": [lon, lat]}
        let (lat, lon) = if let Some(coord) = doc.get("coordinate") {
            let lat = coord.get("lat").and_then(|v| v.as_f64());
            let lon = coord.get("lon").and_then(|v| v.as_f64());
            match (lat, lon) {
                (Some(la), Some(lo)) => (la, lo),
                _ => continue,
            }
        } else {
            continue;
        };

        if lat.abs() > 90.0 || lon.abs() > 180.0 {
            continue;
        }

        // Name: {"default": "...", "en": "...", ...}
        let name_map = doc.get("name");
        let default_name = name_map
            .and_then(|n| {
                n.get("default")
                    .or_else(|| n.get("name"))
                    .and_then(|v| v.as_str())
                    .or_else(|| {
                        // First non-empty value
                        n.as_object().and_then(|obj| {
                            obj.values().find_map(|v| v.as_str())
                        })
                    })
            })
            .unwrap_or("");

        let housenumber = doc.get("housenumber").and_then(|v| v.as_str()).unwrap_or("");
        let street_name = doc
            .get("street")
            .and_then(|v| v.get("default").or_else(|| v.as_str().map(|_| v)))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let has_housenumber = !housenumber.is_empty();
        let has_street = !street_name.is_empty();

        let is_place = is_place_record(osm_key, osm_value) && !default_name.is_empty();
        let is_address = has_housenumber && has_street;

        if is_place {
            let rank = doc.get("rank_address").and_then(|v| v.as_u64()).map(|v| v as u8);
            let place_type = map_photon_place_type(osm_key, osm_value, rank);
            let admin_level = rank_to_admin_level(rank);

            let mut name_intl: Vec<(String, String)> = Vec::new();
            if let Some(nm) = name_map.and_then(|v| v.as_object()) {
                for (k, v) in nm {
                    if k != "default" && k != "name" {
                        if let Some(s) = v.as_str() {
                            if !s.is_empty() {
                                name_intl.push((k.clone(), s.to_owned()));
                            }
                        }
                    }
                }
            }

            let importance_f = doc.get("importance").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let synthetic_population = if importance_f > 0.5 {
                Some(500_000u32)
            } else if importance_f > 0.3 {
                Some(100_000)
            } else if importance_f > 0.1 {
                Some(10_000)
            } else if importance_f > 0.01 {
                Some(1_000)
            } else {
                None
            };

            let cc = doc
                .get("countrycode")
                .and_then(|v| v.as_str())
                .and_then(|cc| {
                    let b = cc.to_uppercase();
                    if b.len() == 2 {
                        Some([b.as_bytes()[0], b.as_bytes()[1]])
                    } else {
                        None
                    }
                });

            // Photon's `extra` field carries auxiliary OSM tags
            // (wikidata, wikipedia, capital, ele, …). The ones we care
            // about for ranking: wikidata (notability bonus), and any
            // extra alt-name flavours the importer attached. Defensive
            // .get chain — older Photon dumps may lack the field.
            let wikidata = doc
                .get("extra")
                .and_then(|e| e.get("wikidata"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_owned());

            places.push(RawPlace {
                osm_id,
                osm_type: match doc.get("osm_type").and_then(|v| v.as_str()) {
                    Some("W") => OsmType::Way,
                    Some("R") => OsmType::Relation,
                    _ => OsmType::Node,
                },
                name: default_name.to_owned(),
                name_intl,
                alt_names: vec![],
                old_names: vec![],
                coord: Coord::new(lat, lon),
                place_type,
                admin_level,
                country_code: cc,
                admin1: None,
                admin2: None,
                population: synthetic_population,
                wikidata,
                class: (!osm_key.is_empty()).then(|| osm_key.to_owned()),
                class_value: (!osm_value.is_empty()).then(|| osm_value.to_owned()),
                bbox: None,
            });
        }

        if is_address {
            let city = doc
                .get("city")
                .and_then(|v| v.get("default").or_else(|| v.as_str().map(|_| v)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned());
            let postcode = doc
                .get("postcode")
                .and_then(|v| v.as_str())
                .map(|s| s.to_owned());

            addresses.push(RawAddress {
                osm_id,
                street: street_name.to_owned(),
                housenumber: housenumber.to_owned(),
                postcode,
                city,
                state: None,
                lat,
                lon,
            });
        }
    }

    info!(
        "Converted {} Photon docs → {} places + {} addresses",
        docs.len(),
        places.len(),
        addresses.len()
    );

    PhotonParseResult { places, addresses }
}

/// Parse a single Photon/ES document into an optional place and/or address.
/// Used by the streaming pipeline to avoid holding all documents in memory.
pub fn parse_single_es_document(doc: &serde_json::Value) -> (Option<RawPlace>, Option<RawAddress>) {
    let osm_id = doc.get("osm_id").and_then(|v| v.as_i64()).unwrap_or(0);
    let osm_key = doc.get("osm_key").and_then(|v| v.as_str()).unwrap_or("");
    let osm_value = doc.get("osm_value").and_then(|v| v.as_str()).unwrap_or("");

    let (lat, lon) = if let Some(coord) = doc.get("coordinate") {
        let lat = coord.get("lat").and_then(|v| v.as_f64());
        let lon = coord.get("lon").and_then(|v| v.as_f64());
        match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => return (None, None),
        }
    } else {
        return (None, None);
    };

    if lat.abs() > 90.0 || lon.abs() > 180.0 {
        return (None, None);
    }

    let name_map = doc.get("name");
    let default_name = name_map
        .and_then(|n| {
            n.get("default")
                .or_else(|| n.get("name"))
                .and_then(|v| v.as_str())
                .or_else(|| {
                    n.as_object().and_then(|obj| {
                        obj.values().find_map(|v| v.as_str())
                    })
                })
        })
        .unwrap_or("");

    let housenumber = doc.get("housenumber").and_then(|v| v.as_str()).unwrap_or("");
    let street_name = doc
        .get("street")
        .and_then(|v| v.get("default").or_else(|| v.as_str().map(|_| v)))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let is_place = is_place_record(osm_key, osm_value) && !default_name.is_empty();
    let is_address = !housenumber.is_empty() && !street_name.is_empty();

    let place = if is_place {
        let rank = doc.get("rank_address").and_then(|v| v.as_u64()).map(|v| v as u8);
        let place_type = map_photon_place_type(osm_key, osm_value, rank);
        let admin_level = rank_to_admin_level(rank);

        let mut name_intl: Vec<(String, String)> = Vec::new();
        if let Some(nm) = name_map.and_then(|v| v.as_object()) {
            for (k, v) in nm {
                if k != "default" && k != "name" {
                    if let Some(s) = v.as_str() {
                        if !s.is_empty() {
                            name_intl.push((k.clone(), s.to_owned()));
                        }
                    }
                }
            }
        }

        let importance_f = doc.get("importance").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let synthetic_population = if importance_f > 0.5 {
            Some(500_000u32)
        } else if importance_f > 0.3 {
            Some(100_000)
        } else if importance_f > 0.1 {
            Some(10_000)
        } else if importance_f > 0.01 {
            Some(1_000)
        } else {
            None
        };

        let cc = doc
            .get("countrycode")
            .and_then(|v| v.as_str())
            .and_then(|cc| {
                let b = cc.to_uppercase();
                if b.len() == 2 {
                    Some([b.as_bytes()[0], b.as_bytes()[1]])
                } else {
                    None
                }
            });

        let wikidata = doc
            .get("extra")
            .and_then(|e| e.get("wikidata"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_owned());

        Some(RawPlace {
            osm_id,
            osm_type: match doc.get("osm_type").and_then(|v| v.as_str()) {
                Some("W") => OsmType::Way,
                Some("R") => OsmType::Relation,
                _ => OsmType::Node,
            },
            name: default_name.to_owned(),
            name_intl,
            alt_names: vec![],
            old_names: vec![],
            coord: Coord::new(lat, lon),
            place_type,
            admin_level,
            country_code: cc,
            admin1: None,
            admin2: None,
            population: synthetic_population,
            wikidata,
            class: (!osm_key.is_empty()).then(|| osm_key.to_owned()),
            class_value: (!osm_value.is_empty()).then(|| osm_value.to_owned()),
            bbox: None,
        })
    } else {
        None
    };

    let address = if is_address {
        let city = doc
            .get("city")
            .and_then(|v| v.get("default").or_else(|| v.as_str().map(|_| v)))
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());
        let postcode = doc
            .get("postcode")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());

        Some(RawAddress {
            osm_id,
            street: street_name.to_owned(),
            housenumber: housenumber.to_owned(),
            postcode,
            city,
            state: None,
            lat,
            lon,
        })
    } else {
        None
    };

    (place, address)
}

// ---------------------------------------------------------------------------
// Parse-only function (for merge-photon)
// ---------------------------------------------------------------------------

/// Parse a Photon JSONL dump and return places + addresses without building
/// admin hierarchy or writing any files. The enrich step handles admin
/// assignment when the user rebuilds with --skip-extract.
pub fn parse(input: &Path) -> Result<PhotonParseResult> {
    info!("Parsing Photon dump from {}", input.display());

    let file = std::fs::File::open(input)?;
    let decoder = zstd::Decoder::new(file)?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, decoder);

    let mut places: Vec<RawPlace> = Vec::new();
    let mut addresses: Vec<RawAddress> = Vec::new();
    let mut line_count = 0u64;
    let mut skipped = 0u64;

    for line_result in reader.lines() {
        let line = line_result?;
        line_count += 1;

        if line_count % 1_000_000 == 0 {
            info!(
                "  {}M lines ({} places, {} addresses)",
                line_count / 1_000_000,
                places.len(),
                addresses.len()
            );
        }

        if line.is_empty() {
            continue;
        }

        let parsed: PhotonLine = match serde_json::from_str(&line) {
            Ok(p) => p,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        if !parsed.record_type.eq_ignore_ascii_case("place") {
            continue;
        }

        let content = match parsed.content {
            Some(c) => c,
            None => continue,
        };

        let records: Vec<PhotonPlace> = match serde_json::from_value(content) {
            Ok(r) => r,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        let record = match records.into_iter().next() {
            Some(r) => r,
            None => continue,
        };

        let (lon, lat) = if let Some(ref c) = record.centroid {
            if c.len() >= 2 {
                (c[0], c[1])
            } else {
                continue;
            }
        } else if let Some(ref g) = record.geometry {
            match &g.coordinates {
                Some(c) if c.len() >= 2 => (c[0], c[1]),
                _ => continue,
            }
        } else {
            continue;
        };

        if lat.abs() > 90.0 || lon.abs() > 180.0 {
            continue;
        }

        let osm_id = record.object_id.unwrap_or(0);
        let osm_key = record.osm_key.as_deref().unwrap_or("");
        let osm_value = record.osm_value.as_deref().unwrap_or("");

        let names = record.name.as_ref();
        let default_name = names
            .and_then(|n| {
                n.get("default")
                    .or_else(|| n.get("name"))
                    .and_then(json_to_string)
                    .or_else(|| n.values().find_map(json_to_string))
            })
            .unwrap_or_default();

        let address = record.address.as_ref();

        let housenumber_str = record.housenumber.as_deref().unwrap_or("");
        let has_housenumber = !housenumber_str.is_empty();
        let street_str = address
            .and_then(|a| a.street.as_ref())
            .and_then(json_to_string)
            .unwrap_or_default();
        let has_street = !street_str.is_empty();

        let is_place = is_place_record(osm_key, osm_value) && !default_name.is_empty();
        let is_address = has_housenumber && has_street;

        if is_place {
            let place_type = map_photon_place_type(osm_key, osm_value, record.rank_address);
            let admin_level = rank_to_admin_level(record.rank_address);

            let mut name_intl: Vec<(String, String)> = Vec::new();
            if let Some(nm) = names {
                for (k, v) in nm {
                    if k != "default" && k != "name" {
                        if let Some(s) = json_to_string(v) {
                            if !s.is_empty() {
                                name_intl.push((k.clone(), s));
                            }
                        }
                    }
                }
            }

            let importance_f = record.importance.unwrap_or(0.0);
            let synthetic_population = if importance_f > 0.5 {
                Some(500_000u32)
            } else if importance_f > 0.3 {
                Some(100_000)
            } else if importance_f > 0.1 {
                Some(10_000)
            } else if importance_f > 0.01 {
                Some(1_000)
            } else {
                None
            };

            let wikidata = record.wikidata.clone().or_else(|| {
                record.extra.as_ref()
                    .and_then(|e| e.get("wikidata"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_owned())
            });

            places.push(RawPlace {
                osm_id,
                osm_type: match record.object_type.as_deref() {
                    Some("W") => OsmType::Way,
                    Some("R") => OsmType::Relation,
                    _ => OsmType::Node,
                },
                name: default_name.clone(),
                name_intl,
                alt_names: vec![],
                old_names: vec![],
                coord: Coord::new(lat, lon),
                place_type,
                admin_level,
                country_code: record
                    .country_code
                    .as_deref()
                    .and_then(|cc| {
                        let b = cc.to_uppercase();
                        if b.len() == 2 {
                            Some([b.as_bytes()[0], b.as_bytes()[1]])
                        } else {
                            None
                        }
                    }),
                admin1: None, // Resolved by enrich step
                admin2: None,
                population: synthetic_population,
                wikidata,
                class: (!osm_key.is_empty()).then(|| osm_key.to_owned()),
                class_value: (!osm_value.is_empty()).then(|| osm_value.to_owned()),
                bbox: None,
            });
        }

        if is_address {
            let city = address
                .and_then(|a| a.city.as_ref())
                .and_then(json_to_string);
            let postcode = record.postcode.clone();

            addresses.push(RawAddress {
                osm_id,
                street: street_str,
                housenumber: housenumber_str.to_owned(),
                postcode,
                city,
                state: None,
                lat,
                lon,
            });
        }
    }

    info!(
        "Parsed {}M lines: {} places, {} addresses ({} skipped)",
        line_count / 1_000_000,
        places.len(),
        addresses.len(),
        skipped,
    );

    Ok(PhotonParseResult { places, addresses })
}

// ---------------------------------------------------------------------------
// Main import function
// ---------------------------------------------------------------------------

pub fn import(input: &Path, output: &Path) -> Result<PhotonImportResult> {
    std::fs::create_dir_all(output)?;

    info!("Reading Photon dump from {}", input.display());

    let file = std::fs::File::open(input)?;
    let decoder = zstd::Decoder::new(file)?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, decoder);

    let mut places: Vec<RawPlace> = Vec::new();
    let mut addresses: Vec<RawAddress> = Vec::new();

    // Admin hierarchy: collect unique state/county names → assign IDs
    let mut state_names: HashMap<String, (u16, f64, f64)> = HashMap::new();
    let mut county_names: HashMap<String, (u16, f64, f64, u16)> = HashMap::new();
    let mut admin_map: HashMap<i64, (u16, u16)> = HashMap::new();

    let mut next_state_id: u16 = 0;
    let mut next_county_id: u16 = 0;

    let mut line_count = 0u64;
    let mut skipped = 0u64;

    for line_result in reader.lines() {
        let line = line_result?;
        line_count += 1;

        if line_count % 1_000_000 == 0 {
            info!(
                "  {}M lines ({} places, {} addresses)",
                line_count / 1_000_000,
                places.len(),
                addresses.len()
            );
        }

        if line.is_empty() {
            continue;
        }

        let parsed: PhotonLine = match serde_json::from_str(&line) {
            Ok(p) => p,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        // Only process "place" records (skip NominatimDumpFile, CountryInfo)
        if !parsed.record_type.eq_ignore_ascii_case("place") {
            continue;
        }

        let content = match parsed.content {
            Some(c) => c,
            None => continue,
        };

        // Content is an array — take the first element
        let records: Vec<PhotonPlace> = match serde_json::from_value(content) {
            Ok(r) => r,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };

        let record = match records.into_iter().next() {
            Some(r) => r,
            None => continue,
        };

        // Extract coordinates: centroid [lon, lat] (MUST per spec),
        // fallback to geometry.coordinates
        let (lon, lat) = if let Some(ref c) = record.centroid {
            if c.len() >= 2 { (c[0], c[1]) } else { continue }
        } else if let Some(ref g) = record.geometry {
            match &g.coordinates {
                Some(c) if c.len() >= 2 => (c[0], c[1]),
                _ => continue,
            }
        } else {
            continue;
        };

        if lat.abs() > 90.0 || lon.abs() > 180.0 {
            continue;
        }

        let osm_id = record.object_id.unwrap_or(0);
        let osm_key = record.osm_key.as_deref().unwrap_or("");
        let osm_value = record.osm_value.as_deref().unwrap_or("");

        // Extract primary name: try "default" key first (Photon convention),
        // then "name" key (OSM convention), then first available entry
        let names = record.name.as_ref();
        let default_name = names
            .and_then(|n| {
                n.get("default")
                    .or_else(|| n.get("name"))
                    .and_then(json_to_string)
                    .or_else(|| {
                        // Use first non-empty value as fallback
                        n.values().find_map(json_to_string)
                    })
            })
            .unwrap_or_default();

        let address = record.address.as_ref();

        // housenumber: check top-level first (per spec), then address
        let housenumber_str = record
            .housenumber
            .as_deref()
            .or_else(|| None) // address.housenumber not in spec
            .unwrap_or("");
        let has_housenumber = !housenumber_str.is_empty();

        let street_str = address
            .and_then(|a| a.street.as_ref())
            .and_then(json_to_string)
            .unwrap_or_default();
        let has_street = !street_str.is_empty();

        // Resolve admin hierarchy from address fields
        let state_name = address
            .and_then(|a| a.state.as_ref())
            .and_then(json_to_string)
            .unwrap_or_default();
        let county_name = address
            .and_then(|a| a.county.as_ref())
            .and_then(json_to_string)
            .unwrap_or_default();

        let state_id = if !state_name.is_empty() {
            state_names
                .entry(state_name.clone())
                .or_insert_with(|| {
                    let id = next_state_id;
                    next_state_id += 1;
                    (id, lat, lon)
                })
                .0
        } else {
            u16::MAX // sentinel: no state
        };

        let county_raw_id = if !county_name.is_empty() {
            county_names
                .entry(county_name.clone())
                .or_insert_with(|| {
                    let id = next_county_id;
                    next_county_id += 1;
                    (id, lat, lon, state_id)
                })
                .0
        } else {
            u16::MAX // sentinel: no county
        };

        // Determine record routing
        let is_place = is_place_record(osm_key, osm_value) && !default_name.is_empty();
        let is_address = has_housenumber && has_street;

        // Insert admin mapping for this osm_id
        if state_id != u16::MAX || county_raw_id != u16::MAX {
            let a1 = if state_id != u16::MAX { state_id } else { 0 };
            let a2 = county_raw_id; // offset applied after streaming
            admin_map.insert(osm_id, (a1, a2));
        }

        if is_place {
            let place_type = map_photon_place_type(osm_key, osm_value, record.rank_address);
            let admin_level = rank_to_admin_level(record.rank_address);

            // Extract international name variants from names map
            let mut name_intl: Vec<(String, String)> = Vec::new();
            if let Some(nm) = names {
                for (k, v) in nm {
                    if k != "default" && k != "name" {
                        if let Some(s) = json_to_string(v) {
                            if !s.is_empty() {
                                name_intl.push((k.clone(), s));
                            }
                        }
                    }
                }
            }

            // Convert Photon importance (0-1) to synthetic population for ranking
            let importance_f = record.importance.unwrap_or(0.0);
            let synthetic_population = if importance_f > 0.5 {
                Some(500_000u32)
            } else if importance_f > 0.3 {
                Some(100_000)
            } else if importance_f > 0.1 {
                Some(10_000)
            } else if importance_f > 0.01 {
                Some(1_000)
            } else {
                None
            };

            let wikidata = record.wikidata.clone().or_else(|| {
                record.extra.as_ref()
                    .and_then(|e| e.get("wikidata"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_owned())
            });

            places.push(RawPlace {
                osm_id,
                osm_type: match record.object_type.as_deref() {
                    Some("W") => OsmType::Way,
                    Some("R") => OsmType::Relation,
                    _ => OsmType::Node,
                },
                name: default_name.clone(),
                name_intl,
                alt_names: vec![],
                old_names: vec![],
                coord: Coord::new(lat, lon),
                place_type,
                admin_level,
                country_code: record.country_code.as_deref()
                    .and_then(|cc| {
                        let b = cc.to_uppercase();
                        if b.len() == 2 { Some([b.as_bytes()[0], b.as_bytes()[1]]) } else { None }
                    }),
                admin1: if state_name.is_empty() { None } else { Some(state_name.clone()) },
                admin2: if county_name.is_empty() { None } else { Some(county_name.clone()) },
                population: synthetic_population,
                wikidata,
                class: (!osm_key.is_empty()).then(|| osm_key.to_owned()),
                class_value: (!osm_value.is_empty()).then(|| osm_value.to_owned()),
                bbox: None,
            });
        }

        if is_address {
            let city = address
                .and_then(|a| a.city.as_ref())
                .and_then(json_to_string);
            let postcode = record.postcode.clone()
                .or_else(|| address.and_then(|a| a.locality.as_ref()).and_then(json_to_string));

            addresses.push(RawAddress {
                osm_id,
                street: street_str.clone(),
                housenumber: housenumber_str.to_owned(),
                postcode,
                city,
                state: None,
                lat,
                lon,
            });
        }
    }

    info!(
        "Parsed {}M lines: {} places, {} addresses ({} skipped)",
        line_count / 1_000_000,
        places.len(),
        addresses.len(),
        skipped,
    );

    // -----------------------------------------------------------------------
    // Build admin hierarchy
    // -----------------------------------------------------------------------
    let num_states = state_names.len();

    let mut admin_entries: Vec<AdminEntry> =
        Vec::with_capacity(num_states + county_names.len());

    // States first (sorted for deterministic IDs)
    let mut state_list: Vec<(String, u16, f64, f64)> = state_names
        .into_iter()
        .map(|(name, (id, lat, lon))| (name, id, lat, lon))
        .collect();
    state_list.sort_by_key(|(_, id, _, _)| *id);

    for (name, id, lat, lon) in &state_list {
        admin_entries.push(AdminEntry {
            id: *id,
            name: name.clone(),
            parent_id: None,
            coord: Coord::new(*lat, *lon),
            place_type: PlaceType::State,
            population: 0, // Photon extracts don't carry admin population
        });
    }

    // Counties (offset IDs by num_states)
    let mut county_list: Vec<(String, u16, f64, f64, u16)> = county_names
        .into_iter()
        .map(|(name, (id, lat, lon, parent))| (name, id, lat, lon, parent))
        .collect();
    county_list.sort_by_key(|(_, id, _, _, _)| *id);

    for (name, id, lat, lon, parent_state_id) in &county_list {
        let offset_id = *id + num_states as u16;
        admin_entries.push(AdminEntry {
            id: offset_id,
            name: name.clone(),
            parent_id: Some(*parent_state_id),
            coord: Coord::new(*lat, *lon),
            place_type: PlaceType::County,
            population: 0, // Photon extracts don't carry admin population
        });
    }

    // Fix admin_map: offset county IDs by num_states
    for (_, (_, county_id)) in admin_map.iter_mut() {
        if *county_id != u16::MAX {
            *county_id += num_states as u16;
        } else {
            *county_id = 0; // no county → default
        }
    }

    info!(
        "Admin hierarchy: {} states + {} counties = {} entries",
        num_states,
        county_list.len(),
        admin_entries.len()
    );

    // -----------------------------------------------------------------------
    // Write outputs
    // -----------------------------------------------------------------------

    // admin.bin
    let admin_bytes = postcard::to_allocvec(&admin_entries).expect("postcard serialize admin");
    let admin_path = output.join("admin.bin");
    std::fs::write(&admin_path, &admin_bytes)?;
    heimdall_core::compressed_io::compress_file(&admin_path, 19)?;
    info!("admin.bin: {:.1} KB", admin_bytes.len() as f64 / 1024.0);

    // admin_map.bin
    let map_bytes = bincode::serialize(&admin_map)?;
    std::fs::write(output.join("admin_map.bin"), &map_bytes)?;
    info!(
        "admin_map.bin: {:.1} MB ({} entries)",
        map_bytes.len() as f64 / 1e6,
        admin_map.len()
    );

    // places.parquet
    write_places_parquet(&places, &output.join("places.parquet"))?;
    info!("places.parquet: {} records", places.len());

    // addresses.parquet
    write_addresses_parquet(&addresses, &output.join("addresses.parquet"))?;
    info!("addresses.parquet: {} records", addresses.len());

    Ok(PhotonImportResult {
        place_count: places.len(),
        address_count: addresses.len(),
        admin_count: admin_entries.len(),
    })
}

// ---------------------------------------------------------------------------
// Record routing helpers
// ---------------------------------------------------------------------------

/// Determine if a Photon record should be indexed as a place
fn is_place_record(osm_key: &str, osm_value: &str) -> bool {
    matches!(
        (osm_key, osm_value),
        ("place", _)
            | ("boundary", "administrative")
            | ("natural", "water")
            | ("natural", "peak")
            | ("natural", "bay")
            | ("natural", "cape")
            | ("natural", "wood")
            | ("natural", "forest")
            | ("natural", "volcano")
            | ("waterway", "river")
            | ("waterway", "stream")
            | ("railway", "station")
            | ("railway", "halt")
            | ("aeroway", "aerodrome")
            | ("amenity", _)
            | ("tourism", _)
            | ("leisure", _)
            | ("shop", _)
            | ("historic", _)
    )
}

/// Map Photon osm_key/value to Heimdall PlaceType
///
/// Mirrors `extract::place_type_from_tag` so Photon-sourced records get the
/// same classification as OSM-extracted ones. Without this, a Photon record
/// like `tourism=museum` would land as `Unknown` and pack.rs would only keep
/// it if it carried a `wikidata` Q-id — silently dropping museums, libraries,
/// hospitals, and parks that Photon ingested but OSM extract missed (or that
/// got merged in via per-country fallback in regions where the OSM-only
/// pipeline came up short).
fn map_photon_place_type(osm_key: &str, osm_value: &str, rank: Option<u8>) -> PlaceType {
    match (osm_key, osm_value) {
        ("place", v) => PlaceType::from_osm(v),
        ("boundary", "administrative") => match rank {
            Some(r) if r <= 4 => PlaceType::Country,
            Some(r) if r <= 6 => PlaceType::State,
            Some(r) if r <= 8 => PlaceType::County,
            Some(r) if r <= 12 => PlaceType::City,
            Some(r) if r <= 14 => PlaceType::Town,
            Some(r) if r <= 16 => PlaceType::Village,
            _ => PlaceType::Unknown,
        },
        ("natural", "water") | ("natural", "lake") => PlaceType::Lake,
        ("natural", "peak") | ("natural", "volcano")
        | ("natural", "mountain") | ("mountain_pass", _) => PlaceType::Mountain,
        ("natural", "bay") => PlaceType::Bay,
        ("natural", "cape") | ("natural", "peninsula") => PlaceType::Cape,
        ("natural", "wood") | ("natural", "forest") | ("landuse", "forest") => PlaceType::Forest,
        ("natural", "island") => PlaceType::Island,
        ("natural", "islet") => PlaceType::Islet,
        ("waterway", "river") | ("waterway", "stream") | ("waterway", "canal") => PlaceType::River,
        ("railway", "station") | ("railway", "halt") => PlaceType::Station,
        ("public_transport", "station") => PlaceType::Station,
        ("aeroway", "aerodrome") => PlaceType::Airport,

        // Tourism — visitor attractions and museums.
        ("tourism", "attraction") | ("tourism", "museum") | ("tourism", "gallery")
        | ("tourism", "viewpoint") | ("tourism", "theme_park") | ("tourism", "zoo")
        | ("tourism", "aquarium") => PlaceType::Landmark,
        // Historic — castles, monuments, ruins, memorials.
        ("historic", _) => PlaceType::Landmark,
        // Civic — universities, hospitals, libraries, theatres.
        ("amenity", "university") | ("amenity", "college") => PlaceType::University,
        ("amenity", "hospital") => PlaceType::Hospital,
        ("amenity", "townhall") | ("amenity", "library") | ("amenity", "theatre")
        | ("amenity", "arts_centre") | ("amenity", "courthouse")
        | ("amenity", "place_of_worship") => PlaceType::PublicBuilding,
        // Leisure — parks and major venues.
        ("leisure", "park") | ("leisure", "garden") | ("leisure", "nature_reserve") => PlaceType::Park,
        ("leisure", "stadium") | ("leisure", "sports_centre")
        | ("leisure", "ice_rink") => PlaceType::Landmark,
        // Man-made structures.
        ("man_made", "bridge") | ("man_made", "lighthouse")
        | ("man_made", "tower") => PlaceType::Landmark,
        // Squares.
        ("place", "square") => PlaceType::Square,

        _ => PlaceType::Unknown,
    }
}

/// Convert Photon rank_address to OSM-style admin_level
fn rank_to_admin_level(rank: Option<u8>) -> Option<u8> {
    match rank {
        Some(r) if r <= 4 => Some(2),  // country
        Some(r) if r <= 6 => Some(4),  // state/nation
        Some(r) if r <= 8 => Some(6),  // county/district
        Some(r) if r <= 12 => Some(8), // city
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Parquet writers (matching existing extract.rs / main.rs schemas)
// ---------------------------------------------------------------------------

pub fn write_places_parquet(places: &[RawPlace], path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("osm_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lon", DataType::Float64, false),
        Field::new("place_type", DataType::UInt8, false),
        Field::new("admin_level", DataType::UInt8, true),
        Field::new("population", DataType::UInt32, true),
        Field::new("wikidata", DataType::Utf8, true),
        Field::new("alt_names", DataType::Utf8, true),
        Field::new("old_names", DataType::Utf8, true),
        Field::new("name_intl", DataType::Utf8, true),
    ]));

    let chunk_size = 500_000;
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;

    for chunk_start in (0..places.len()).step_by(chunk_size) {
        let chunk = &places[chunk_start..(chunk_start + chunk_size).min(places.len())];

        let osm_ids: Vec<i64> = chunk.iter().map(|p| p.osm_id).collect();
        let names: Vec<&str> = chunk.iter().map(|p| p.name.as_str()).collect();
        let lats: Vec<f64> = chunk.iter().map(|p| p.coord.lat_f64()).collect();
        let lons: Vec<f64> = chunk.iter().map(|p| p.coord.lon_f64()).collect();
        let place_types: Vec<u8> = chunk.iter().map(|p| p.place_type as u8).collect();

        let admin_levels: Vec<Option<u8>> = chunk.iter().map(|p| p.admin_level).collect();
        let populations: Vec<Option<u32>> = chunk.iter().map(|p| p.population).collect();
        let wikidatas: Vec<Option<&str>> = chunk.iter().map(|p| p.wikidata.as_deref()).collect();

        // Stored as semicolon-separated. Empty Vec → None so downstream
        // readers can `.is_null()`-check without splitting an empty string.
        // Earlier this writer was Photon-only and Photon JSON didn't expose
        // alt_names; now extract.rs feeds the same writer with rich OSM data
        // (short_name, loc_name, official_name) — silently dropping it
        // killed cross-name resolution for queries like "AU" → Aarhus
        // Universitet.
        let alt_strings: Vec<Option<String>> = chunk
            .iter()
            .map(|p| {
                if p.alt_names.is_empty() {
                    None
                } else {
                    Some(p.alt_names.join(";"))
                }
            })
            .collect();
        let alt_names: Vec<Option<&str>> = alt_strings.iter().map(|s| s.as_deref()).collect();

        let old_strings: Vec<Option<String>> = chunk
            .iter()
            .map(|p| {
                if p.old_names.is_empty() {
                    None
                } else {
                    Some(p.old_names.join(";"))
                }
            })
            .collect();
        let old_names: Vec<Option<&str>> = old_strings.iter().map(|s| s.as_deref()).collect();

        // Format name_intl as "lang=name;lang=name"
        let name_intl_strings: Vec<Option<String>> = chunk
            .iter()
            .map(|p| {
                if p.name_intl.is_empty() {
                    None
                } else {
                    Some(
                        p.name_intl
                            .iter()
                            .map(|(k, v)| format!("{}={}", k, v))
                            .collect::<Vec<_>>()
                            .join(";"),
                    )
                }
            })
            .collect();
        let name_intl_refs: Vec<Option<&str>> =
            name_intl_strings.iter().map(|s| s.as_deref()).collect();

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(osm_ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(lats)),
                Arc::new(Float64Array::from(lons)),
                Arc::new(UInt8Array::from(place_types)),
                Arc::new(UInt8Array::from(admin_levels)),
                Arc::new(UInt32Array::from(populations)),
                Arc::new(StringArray::from(wikidatas)),
                Arc::new(StringArray::from(alt_names)),
                Arc::new(StringArray::from(old_names)),
                Arc::new(StringArray::from(name_intl_refs)),
            ],
        )?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}

fn write_addresses_parquet(addresses: &[RawAddress], path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("osm_id", DataType::Int64, false),
        Field::new("street", DataType::Utf8, false),
        Field::new("housenumber", DataType::Utf8, false),
        Field::new("postcode", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("lat", DataType::Float64, false),
        Field::new("lon", DataType::Float64, false),
    ]));

    let chunk_size = 500_000;
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;

    for chunk_start in (0..addresses.len()).step_by(chunk_size) {
        let chunk = &addresses[chunk_start..(chunk_start + chunk_size).min(addresses.len())];

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    chunk.iter().map(|a| a.osm_id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    chunk.iter().map(|a| a.street.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    chunk
                        .iter()
                        .map(|a| a.housenumber.as_str())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    chunk
                        .iter()
                        .map(|a| a.postcode.as_deref())
                        .collect::<Vec<Option<&str>>>(),
                )),
                Arc::new(StringArray::from(
                    chunk
                        .iter()
                        .map(|a| a.city.as_deref())
                        .collect::<Vec<Option<&str>>>(),
                )),
                Arc::new(Float64Array::from(
                    chunk.iter().map(|a| a.lat).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    chunk.iter().map(|a| a.lon).collect::<Vec<_>>(),
                )),
            ],
        )?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}
