//! OSM Change (.osc.gz) parser and diff applicator for incremental parquet updates.
//!
//! Geofabrik publishes daily `.osc.gz` diffs containing create/modify/delete operations.
//! This module parses them, filters to geocoding-relevant elements, and applies the
//! changes to existing parquet files — eliminating the need for full PBF re-downloads.

use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{bail, Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use tracing::info;

use heimdall_core::types::{Coord, PlaceType, RawPlace};

use crate::extract::{
    self, parse_tags, place_type_from_tag, RawAddress, MEANINGFUL_NODE_TAGS, MEANINGFUL_WAY_TAGS,
};

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeAction {
    Create,
    Modify,
    Delete,
}

#[derive(Debug)]
pub enum OscChange {
    Place {
        action: ChangeAction,
        osm_id: i64,
        place: Option<RawPlace>,
    },
    Address {
        action: ChangeAction,
        osm_id: i64,
        address: Option<RawAddress>,
    },
}

#[derive(Debug, Default)]
pub struct OscStats {
    pub total_elements: u64,
    pub geocoding_relevant: u64,
    pub place_creates: u64,
    pub place_modifies: u64,
    pub place_deletes: u64,
    pub addr_creates: u64,
    pub addr_modifies: u64,
    pub addr_deletes: u64,
    pub ways_skipped_no_coords: u64,
    pub relations_skipped: u64,
}

#[derive(Debug, Default)]
pub struct DiffApplyStats {
    pub places_before: usize,
    pub places_after: usize,
    pub places_added: usize,
    pub places_modified: usize,
    pub places_deleted: usize,
    pub addresses_before: usize,
    pub addresses_after: usize,
    pub addresses_added: usize,
    pub addresses_modified: usize,
    pub addresses_deleted: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// .osc.gz Parser
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a gzip-compressed OsmChange file, returning only geocoding-relevant changes.
///
/// Nodes: extracted if they have name + meaningful tag, or addr:street + addr:housenumber.
/// Ways: skipped (no coordinates in diffs — centroids retained from previous data).
/// Relations: skipped (rare in geocoding diffs, require multipolygon resolution).
pub fn parse_osc_gz(path: &Path) -> Result<(Vec<OscChange>, OscStats)> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;
    let gz = flate2::read::GzDecoder::new(BufReader::new(file));
    parse_osc_reader(gz)
}

/// Parse OsmChange XML from any reader (for testing with in-memory data).
pub fn parse_osc_reader<R: Read>(reader: R) -> Result<(Vec<OscChange>, OscStats)> {
    let mut xml = Reader::from_reader(BufReader::new(reader));
    xml.config_mut().trim_text(true);

    let mut changes = Vec::new();
    let mut stats = OscStats::default();

    // Parser state
    let mut current_action: Option<ChangeAction> = None;
    let mut current_element: Option<&'static str> = None; // "node", "way", "relation"
    let mut elem_id: i64 = 0;
    let mut elem_lat: f64 = 0.0;
    let mut elem_lon: f64 = 0.0;
    let mut tags: Vec<(String, String)> = Vec::new();

    let mut buf = Vec::new();

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(ref e)) => {
                let name_bytes = e.name();
                let name = std::str::from_utf8(name_bytes.as_ref()).unwrap_or("");
                match name {
                    "create" => current_action = Some(ChangeAction::Create),
                    "modify" => current_action = Some(ChangeAction::Modify),
                    "delete" => current_action = Some(ChangeAction::Delete),
                    "node" | "way" | "relation" => {
                        current_element = Some(match name {
                            "node" => "node",
                            "way" => "way",
                            _ => "relation",
                        });
                        tags.clear();
                        parse_element_attrs(e, &mut elem_id, &mut elem_lat, &mut elem_lon);
                        stats.total_elements += 1;
                    }
                    _ => {}
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name_bytes = e.name();
                let name = std::str::from_utf8(name_bytes.as_ref()).unwrap_or("");
                match name {
                    // Self-closing <tag k="..." v="..."/>
                    "tag" if current_element.is_some() => {
                        parse_tag_element(e, &mut tags);
                    }
                    // Self-closing <nd ref="..."/> — ignored (way node refs)
                    "nd" => {}
                    // Self-closing element like <node id="123" ... /> (delete with no children)
                    "node" | "way" | "relation" => {
                        tags.clear();
                        parse_element_attrs(e, &mut elem_id, &mut elem_lat, &mut elem_lon);
                        stats.total_elements += 1;
                        if let Some(action) = current_action {
                            process_element(
                                name, action, elem_id, elem_lat, elem_lon,
                                &tags, &mut changes, &mut stats,
                            );
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let name_bytes = e.name();
                let name = std::str::from_utf8(name_bytes.as_ref()).unwrap_or("");
                match name {
                    "create" | "modify" | "delete" => {
                        current_action = None;
                    }
                    "node" | "way" | "relation" => {
                        if let Some(action) = current_action {
                            process_element(
                                name, action, elem_id, elem_lat, elem_lon,
                                &tags, &mut changes, &mut stats,
                            );
                        }
                        current_element = None;
                    }
                    _ => {}
                }
            }
            Err(e) => bail!("XML parse error: {}", e),
            _ => {}
        }
        buf.clear();
    }

    Ok((changes, stats))
}

/// Parse id/lat/lon attributes from a node/way/relation element.
fn parse_element_attrs(
    e: &quick_xml::events::BytesStart<'_>,
    id: &mut i64,
    lat: &mut f64,
    lon: &mut f64,
) {
    *id = 0;
    *lat = 0.0;
    *lon = 0.0;
    for attr in e.attributes().flatten() {
        match attr.key.as_ref() {
            b"id" => *id = std::str::from_utf8(&attr.value).unwrap_or("0").parse().unwrap_or(0),
            b"lat" => *lat = std::str::from_utf8(&attr.value).unwrap_or("0").parse().unwrap_or(0.0),
            b"lon" => *lon = std::str::from_utf8(&attr.value).unwrap_or("0").parse().unwrap_or(0.0),
            _ => {}
        }
    }
}

/// Parse a <tag k="..." v="..."/> element.
fn parse_tag_element(e: &quick_xml::events::BytesStart<'_>, tags: &mut Vec<(String, String)>) {
    let mut k = String::new();
    let mut v = String::new();
    for attr in e.attributes().flatten() {
        match attr.key.as_ref() {
            b"k" => k = String::from_utf8_lossy(&attr.value).to_string(),
            b"v" => v = String::from_utf8_lossy(&attr.value).to_string(),
            _ => {}
        }
    }
    if !k.is_empty() {
        tags.push((k, v));
    }
}

/// Process a completed element and emit OscChange if geocoding-relevant.
fn process_element(
    elem_type: &str,
    action: ChangeAction,
    osm_id: i64,
    lat: f64,
    lon: f64,
    tags: &[(String, String)],
    changes: &mut Vec<OscChange>,
    stats: &mut OscStats,
) {
    if osm_id == 0 {
        return;
    }

    // Ways: no coordinates in diffs — skip with counter
    if elem_type == "way" {
        // Check if it has geocoding-relevant tags
        let has_relevant = tags.iter().any(|(k, _)| {
            k == "name" || k == "addr:street"
        });
        if has_relevant {
            stats.ways_skipped_no_coords += 1;
        }
        return;
    }

    // Relations: skip entirely
    if elem_type == "relation" {
        stats.relations_skipped += 1;
        return;
    }

    // Nodes: filter and extract
    assert_eq!(elem_type, "node");

    // For deletes, we don't have tags — emit delete for both place and address
    if action == ChangeAction::Delete {
        // We don't know if this was a place or address, so emit both.
        // apply_diffs_to_parquet will harmlessly skip if the osm_id doesn't exist.
        changes.push(OscChange::Place {
            action: ChangeAction::Delete,
            osm_id,
            place: None,
        });
        changes.push(OscChange::Address {
            action: ChangeAction::Delete,
            osm_id,
            address: None,
        });
        stats.place_deletes += 1;
        stats.addr_deletes += 1;
        stats.geocoding_relevant += 1;
        return;
    }

    // Check for address tags
    let mut street: Option<String> = None;
    let mut housenumber: Option<String> = None;
    let mut postcode: Option<String> = None;
    let mut city: Option<String> = None;

    for (k, v) in tags {
        match k.as_str() {
            "addr:street" => street = Some(v.clone()),
            "addr:housenumber" => housenumber = Some(v.clone()),
            "addr:postcode" => postcode = Some(v.clone()),
            "addr:city" => city = Some(v.clone()),
            _ => {}
        }
    }

    if let (Some(st), Some(hn)) = (street, housenumber) {
        if !hn.is_empty() {
            changes.push(OscChange::Address {
                action,
                osm_id,
                address: Some(RawAddress {
                    osm_id,
                    street: st,
                    housenumber: hn,
                    postcode,
                    city,
                    state: None,
                    lat,
                    lon,
                }),
            });
            stats.geocoding_relevant += 1;
            match action {
                ChangeAction::Create => stats.addr_creates += 1,
                ChangeAction::Modify => stats.addr_modifies += 1,
                ChangeAction::Delete => unreachable!(),
            }
        }
    }

    // Check for place tags
    let parsed = parse_tags(tags.iter().map(|(k, v)| (k.as_str(), v.as_str())));

    if let Some(name) = parsed.name {
        let place_type = if let Some(ref pt) = parsed.place_tag {
            PlaceType::from_osm(pt)
        } else if let Some((ref k, ref v)) = parsed.qualifying_tag {
            place_type_from_tag(k, v)
        } else {
            // Has name but no qualifying tag — not geocoding-relevant
            return;
        };

        changes.push(OscChange::Place {
            action,
            osm_id,
            place: Some({
                let (cls, cls_val) = crate::extract::class_value_from_tags(
                    parsed.place_tag.as_ref(),
                    parsed.qualifying_tag.as_ref(),
                ).map(|(c, v)| (Some(c), Some(v))).unwrap_or((None, None));
                RawPlace {
                    osm_id,
                    osm_type: heimdall_core::types::OsmType::Node,
                    name,
                    name_intl: parsed.name_intl,
                    alt_names: parsed.alt_names,
                    old_names: parsed.old_names,
                    coord: Coord::new(lat, lon),
                    place_type,
                    admin_level: parsed.admin_level,
                    country_code: None,
                    admin1: None,
                    admin2: None,
                    population: parsed.population,
                    wikidata: parsed.wikidata,
                    class: cls,
                    class_value: cls_val,
                    bbox: None,
                    extratags: vec![],
                }
            }),
        });
        stats.geocoding_relevant += 1;
        match action {
            ChangeAction::Create => stats.place_creates += 1,
            ChangeAction::Modify => stats.place_modifies += 1,
            ChangeAction::Delete => unreachable!(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Diff application to parquet
// ─────────────────────────────────────────────────────────────────────────────

/// Apply a list of changes to places.parquet and addresses.parquet.
///
/// Algorithm: read parquet → HashMap<osm_id, _> → apply changes → rewrite parquet.
pub fn apply_diffs_to_parquet(
    places_parquet: &Path,
    addr_parquet: &Path,
    changes: &[OscChange],
) -> Result<DiffApplyStats> {
    let mut stats = DiffApplyStats::default();

    // Load existing data into HashMaps
    let mut places: HashMap<i64, RawPlace> = if places_parquet.exists() {
        let existing = crate::read_osm_places(places_parquet)?;
        stats.places_before = existing.len();
        existing.into_iter().map(|p| (p.osm_id, p)).collect()
    } else {
        HashMap::new()
    };

    let mut addresses: HashMap<i64, RawAddress> = if addr_parquet.exists() {
        let existing = crate::read_osm_addresses(addr_parquet)?;
        stats.addresses_before = existing.len();
        existing.into_iter().map(|a| (a.osm_id, a)).collect()
    } else {
        HashMap::new()
    };

    // Apply changes
    for change in changes {
        match change {
            OscChange::Place { action, osm_id, place } => match action {
                ChangeAction::Create => {
                    if let Some(p) = place {
                        if places.insert(*osm_id, p.clone()).is_none() {
                            stats.places_added += 1;
                        } else {
                            stats.places_modified += 1;
                        }
                    }
                }
                ChangeAction::Modify => {
                    if let Some(p) = place {
                        if places.insert(*osm_id, p.clone()).is_some() {
                            stats.places_modified += 1;
                        } else {
                            stats.places_added += 1;
                        }
                    }
                }
                ChangeAction::Delete => {
                    if places.remove(osm_id).is_some() {
                        stats.places_deleted += 1;
                    }
                }
            },
            OscChange::Address { action, osm_id, address } => match action {
                ChangeAction::Create => {
                    if let Some(a) = address {
                        if addresses.insert(*osm_id, a.clone()).is_none() {
                            stats.addresses_added += 1;
                        } else {
                            stats.addresses_modified += 1;
                        }
                    }
                }
                ChangeAction::Modify => {
                    if let Some(a) = address {
                        if addresses.insert(*osm_id, a.clone()).is_some() {
                            stats.addresses_modified += 1;
                        } else {
                            stats.addresses_added += 1;
                        }
                    }
                }
                ChangeAction::Delete => {
                    if addresses.remove(osm_id).is_some() {
                        stats.addresses_deleted += 1;
                    }
                }
            },
        }
    }

    stats.places_after = places.len();
    stats.addresses_after = addresses.len();

    // Write back to parquet
    let places_vec: Vec<RawPlace> = places.into_values().collect();
    let addr_vec: Vec<RawAddress> = addresses.into_values().collect();

    if !places_vec.is_empty() || places_parquet.exists() {
        crate::photon::write_places_parquet(&places_vec, places_parquet)?;
    }
    if !addr_vec.is_empty() || addr_parquet.exists() {
        crate::write_merged_addresses(&addr_vec, addr_parquet)?;
    }

    Ok(stats)
}

// ─────────────────────────────────────────────────────────────────────────────
// URL helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Derive the updates base URL from a Geofabrik state.txt URL.
/// e.g., "https://download.geofabrik.de/europe/germany-updates/state.txt"
///     → "https://download.geofabrik.de/europe/germany-updates/"
pub fn updates_base_url(state_url: &str) -> Option<String> {
    state_url.strip_suffix("state.txt").map(|s| s.to_string())
}

/// Build the URL for a specific .osc.gz diff file from a sequence number.
/// Geofabrik pattern: {base}{seq/1000000:03d}/{(seq/1000)%1000:03d}/{seq%1000:03d}.osc.gz
pub fn diff_url(base_url: &str, sequence: u64) -> String {
    let a = sequence / 1_000_000;
    let b = (sequence / 1_000) % 1_000;
    let c = sequence % 1_000;
    format!("{}{:03}/{:03}/{:03}.osc.gz", base_url, a, b, c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff_url() {
        let base = "https://download.geofabrik.de/europe/denmark-updates/";
        assert_eq!(
            diff_url(base, 6234),
            "https://download.geofabrik.de/europe/denmark-updates/000/006/234.osc.gz"
        );
        assert_eq!(
            diff_url(base, 1_234_567),
            "https://download.geofabrik.de/europe/denmark-updates/001/234/567.osc.gz"
        );
    }

    #[test]
    fn test_updates_base_url() {
        assert_eq!(
            updates_base_url("https://download.geofabrik.de/europe/germany-updates/state.txt"),
            Some("https://download.geofabrik.de/europe/germany-updates/".to_string())
        );
        assert_eq!(updates_base_url("no-state-suffix"), None);
    }

    #[test]
    fn test_parse_osc_xml() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<osmChange version="0.6">
  <create>
    <node id="12345" lat="55.6761" lon="12.5683" version="1">
      <tag k="name" v="Test Place"/>
      <tag k="place" v="hamlet"/>
      <tag k="population" v="150"/>
    </node>
    <node id="12346" lat="55.6762" lon="12.5684" version="1">
      <tag k="addr:street" v="Testvej"/>
      <tag k="addr:housenumber" v="42"/>
      <tag k="addr:postcode" v="2100"/>
    </node>
    <node id="99999" lat="55.0" lon="12.0" version="1">
      <tag k="highway" v="bus_stop"/>
    </node>
  </create>
  <modify>
    <node id="12345" lat="55.6770" lon="12.5690" version="2">
      <tag k="name" v="Test Place Updated"/>
      <tag k="place" v="village"/>
      <tag k="population" v="200"/>
    </node>
  </modify>
  <delete>
    <node id="12346" version="3"/>
  </delete>
</osmChange>"#;

        let (changes, stats) = parse_osc_reader(&xml[..]).unwrap();

        assert_eq!(stats.total_elements, 5);
        assert_eq!(stats.geocoding_relevant, 4); // place create, addr create, place modify, delete (counts as 1)
        assert_eq!(stats.place_creates, 1);
        assert_eq!(stats.addr_creates, 1);
        assert_eq!(stats.place_modifies, 1);

        // The highway=bus_stop node without name should be filtered out
        assert!(changes.len() >= 4);

        // Check first change is a place create
        match &changes[0] {
            OscChange::Place { action, osm_id, place } => {
                assert_eq!(*action, ChangeAction::Create);
                assert_eq!(*osm_id, 12345);
                let p = place.as_ref().unwrap();
                assert_eq!(p.name, "Test Place");
                assert_eq!(p.population, Some(150));
            }
            _ => panic!("Expected Place create"),
        }

        // Check address create
        match &changes[1] {
            OscChange::Address { action, osm_id, address } => {
                assert_eq!(*action, ChangeAction::Create);
                assert_eq!(*osm_id, 12346);
                let a = address.as_ref().unwrap();
                assert_eq!(a.street, "Testvej");
                assert_eq!(a.housenumber, "42");
            }
            _ => panic!("Expected Address create"),
        }
    }
}
