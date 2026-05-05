/// extract.rs — OSM PBF → raw RawPlace → Parquet
///
/// Three-pass extraction:
///   Pre-pass 1: scan relations → qualifying way IDs (HashSet) + relation node member IDs
///   Pre-pass 2: scan ways → needed node IDs from qualifying ways (sorted Vec)
///   Main pass: filtered node caching + way/relation extraction
///
/// Only nodes referenced by qualifying ways (address, named, relation member)
/// are cached. For Canada, this reduces the node cache from 766M to ~50M entries
/// (11.4 GB to ~0.75 GB disk sort).

use std::collections::HashMap;
use std::path::Path;
use anyhow::Result;
use osmpbf::{ElementReader, Element};
use tracing::info;
use heimdall_core::types::*;
use heimdall_core::node_cache::{NodeCache, MmapNodeCache, SortedFileNodeCache};

// ---------------------------------------------------------------------------
// Address types
// ---------------------------------------------------------------------------

/// A raw address extracted from OSM (node or way centroid)
///
/// `state` is the admin1 / region / state component carried by sources that
/// expose it (OpenAddresses REGION column, NAR PROVINCE, future imports).
/// Stored as Option<String> so OSM-tagged addresses (which generally lack
/// addr:state) and country-scoped sources keep working without ceremony.
/// pack_addr does not yet consume it — see TODO in pack_addr.rs about
/// scoping the FST key by state to avoid cross-state street collisions.
#[derive(Debug, Clone)]
pub struct RawAddress {
    pub osm_id: i64,
    pub street: String,
    pub housenumber: String,
    pub postcode: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub lat: f64,
    pub lon: f64,
}

/// Tags that make a node worth indexing even without place=*
pub(crate) const MEANINGFUL_NODE_TAGS: &[&str] = &[
    "amenity",
    "tourism",
    "historic",
    "railway",
    "aeroway",
    "mountain_pass",
    "public_transport",
    "leisure",
    "man_made",
];

/// OSM tag keys surfaced by the API as Nominatim's `extratags` (Phase 2.3).
/// Curated allowlist — captures the same tags Nominatim emits without
/// dragging the long tail of OSM noise into the index.
///
/// `wikidata` and `population` are also stored in `RawPlace`'s typed fields
/// (used for ranking and `/lookup` resolution); we still echo them into
/// `extratags` so the API response shape exactly matches Nominatim.
pub(crate) const EXTRATAG_KEYS: &[&str] = &[
    "wikidata",
    "wikipedia",
    "population",
    "capital",
    "ele",
    "opening_hours",
    "phone",
    "website",
    "email",
    "wheelchair",
    "iata",
    "icao",
    "ref",
    "operator",
    "brand",
    "cuisine",
    "denomination",
    "religion",
];

pub(crate) fn is_extratag_key(k: &str) -> bool {
    EXTRATAG_KEYS.iter().any(|&t| t == k)
}

/// Tags that make a way worth indexing (with name)
pub(crate) const MEANINGFUL_WAY_TAGS: &[&str] = &[
    "natural",
    "landuse",
    "leisure",
    "waterway",
    "place",
    "amenity",
    "tourism",
    "historic",
    "aeroway",
    "public_transport",
    "man_made",
];

/// Return the OSM `(class, value)` tag pair this object qualified on, in
/// preference order: explicit `place=*` first, then any other qualifying
/// tag (`amenity=*`, `tourism=*`, `historic=*`, `natural=*`, …). Mirrors
/// Nominatim's class/type pair.
pub(crate) fn class_value_from_tags(
    place_tag: Option<&String>,
    qualifying_tag: Option<&(String, String)>,
) -> Option<(String, String)> {
    if let Some(p) = place_tag {
        return Some(("place".to_owned(), p.clone()));
    }
    qualifying_tag.cloned()
}

/// Filter: is this (key, value) a POI worth extracting?
///
/// Additive whitelist: any key in MEANINGFUL_*_TAGS qualifies UNLESS the
/// value is a known-noisy infrastructure tag (vending machines, benches,
/// playgrounds). Specific high-signal values (museum, hospital, square,
/// park, etc.) get dedicated PlaceTypes via place_type_from_tag; the
/// long tail falls back to PlaceType::Locality.
///
/// We rely on the name? requirement at extraction time to filter out the
/// vast majority of throwaway POIs — only ~10% of OSM amenities carry a
/// name, and those are the ones users actually search for ("Grand Hôtel",
/// "Espresso House Götgatan", etc.).
pub(crate) fn is_qualifying_poi(key: &str, value: &str) -> bool {
    match key {
        // Always-qualifying categories (existing behaviour, untouched)
        "natural" | "landuse" | "waterway" | "aeroway" | "mountain_pass" | "place" => true,
        "railway" => matches!(value, "station" | "halt"),

        // Restored to old additive behaviour: any named tourism / amenity /
        // historic POI passes. Specific values get specific PlaceTypes;
        // others fall to Locality. Only truly low-signal infrastructure
        // values are blocked so they don't flood the index.
        "tourism" => !matches!(value, "yes" | "information"),
        "historic" => true,
        "amenity" => !matches!(
            value,
            "yes" | "bench" | "vending_machine" | "waste_basket" | "waste_disposal"
            | "recycling" | "bicycle_parking" | "motorcycle_parking" | "parking_space"
            | "parking_entrance" | "parking" | "charging_station" | "telephone"
            | "post_box" | "drinking_water" | "shower" | "toilets" | "hunting_stand"
            | "clock"
        ),

        // public_transport — only stations (skip stops, platforms, etc.)
        "public_transport" => matches!(value, "station" | "stop_area"),

        // Leisure — parks/gardens/nature_reserves AND major venues
        // (stadiums, sports_centres, ice rinks) — all gated by
        // notability in leisure_needs_notability. Filters out the
        // overwhelming majority of leisure noise (pitches, playgrounds,
        // swimming pools, fitness stations).
        "leisure" => matches!(
            value,
            "park" | "nature_reserve" | "garden"
                | "stadium" | "sports_centre" | "ice_rink"
        ),

        // man_made — bridges, lighthouses, towers (Öresundsbron,
        // Ölandsbron, Tjörnbron, Kaknästornet). Other man_made values
        // (mast, works, pier, etc.) are mostly noise without notability.
        "man_made" => matches!(value, "bridge" | "lighthouse" | "tower"),

        _ => false,
    }
}

/// Does this POI need a notability signal (wikidata) to qualify?
///
/// Leisure values (parks, stadiums, ice rinks) and man_made values
/// (bridges, towers, lighthouses) are extremely numerous in OSM —
/// every pocket-park, every school football pitch, every footbridge,
/// every radio mast. Without a wikidata signal we'd flood the index.
/// Only the regionally-famous names (Liseberg, Ullevi, Ölandsbron,
/// Tjörnbron, Kaknästornet) survive the gate.
pub(crate) fn leisure_needs_notability(key: &str, value: &str) -> bool {
    match key {
        "leisure" => matches!(
            value,
            "park" | "garden" | "nature_reserve"
                | "stadium" | "sports_centre" | "ice_rink"
        ),
        "man_made" => matches!(value, "bridge" | "tower" | "lighthouse"),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Intermediate structs for pass 1 (before we have coordinates)
// ---------------------------------------------------------------------------

/// Way metadata collected in pass 1 (before centroid resolution)
struct PendingWay {
    id: i64,
    name: String,
    name_intl: Vec<(String, String)>,
    alt_names: Vec<String>,
    old_names: Vec<String>,
    population: Option<u32>,
    admin_level: Option<u8>,
    wikidata: Option<String>,
    place_type: PlaceType,
    /// Original OSM (class, value) tag pair the way qualified on (e.g.
    /// `("place","city")`, `("tourism","museum")`). Phase 2.2 — surfaces
    /// in the API as Nominatim-style `class` / `type`.
    osm_class_value: Option<(String, String)>,
    /// Phase 2.3 — Nominatim `extratags`. Curated allowlist captured during
    /// scan; surfaces verbatim in the API when `?extratags=1`.
    extratags: Vec<(String, String)>,
    node_refs: Vec<i64>,
}

/// Relation metadata collected in pass 1
struct PendingRelation {
    id: i64,
    name: String,
    name_intl: Vec<(String, String)>,
    alt_names: Vec<String>,
    old_names: Vec<String>,
    population: Option<u32>,
    admin_level: Option<u8>,
    wikidata: Option<String>,
    place_type: PlaceType,
    osm_class_value: Option<(String, String)>,
    extratags: Vec<(String, String)>,
    node_member_ids: Vec<i64>,
    way_members: Vec<(i64, String)>, // (way_id, role)
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub struct ExtractResult {
    pub place_count: usize,
    pub address_count: usize,
}

pub fn extract_places(
    pbf_path: &Path,
    parquet_path: &Path,
    min_population: u32,
    low_memory: bool,
) -> Result<ExtractResult> {
    // -----------------------------------------------------------------------
    // Pre-pass: scan relations to collect way member IDs (~10s, ~4 MB)
    // This lets us skip buffering millions of candidate ways during extraction.
    // -----------------------------------------------------------------------
    info!("Pre-pass 1: scanning relations for member IDs...");
    let (relation_way_ids, relation_node_ids): (std::collections::HashSet<i64>, Vec<i64>) = {
        let mut way_ids = std::collections::HashSet::new();
        let mut node_ids = Vec::new();
        let reader = ElementReader::from_path(pbf_path)?;
        reader.for_each(|element| {
            if let Element::Relation(relation) = element {
                // Only collect IDs from relations we actually care about:
                // admin boundaries, places, and qualifying multipolygons.
                // Skipping route/public_transport/etc saves millions of way IDs.
                let mut is_admin = false;
                let mut has_place = false;
                let mut is_qualifying_mp = false;
                let mut has_name = false;
                let mut admin_level: Option<u8> = None;
                let mut rel_type: Option<&str> = None;

                for (k, v) in relation.tags() {
                    match k {
                        "name" => has_name = true,
                        "boundary" if v == "administrative" => is_admin = true,
                        "admin_level" => admin_level = v.parse().ok(),
                        "place" => has_place = true,
                        "type" => rel_type = Some(v),
                        _ => {
                            if !is_qualifying_mp {
                                for &mt in MEANINGFUL_WAY_TAGS {
                                    if k == mt && is_qualifying_poi(k, v) {
                                        is_qualifying_mp = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                if !has_name { return; }
                let is_admin = is_admin && admin_level.map_or(false, |l| l >= 2 && l <= 10);
                let is_mp = rel_type == Some("multipolygon") && is_qualifying_mp;
                if !is_admin && !has_place && !is_mp { return; }

                for member in relation.members() {
                    match member.member_type {
                        osmpbf::elements::RelMemberType::Way => {
                            way_ids.insert(member.member_id);
                        }
                        osmpbf::elements::RelMemberType::Node => {
                            node_ids.push(member.member_id);
                        }
                        _ => {}
                    }
                }
            }
        })?;
        info!("  Found {} way IDs + {} node IDs from qualifying relations ({:.1} MB)",
            way_ids.len(), node_ids.len(), way_ids.len() as f64 * 16.0 / 1e6);
        (way_ids, node_ids)
    };

    // -----------------------------------------------------------------------
    // Pre-pass 2: scan ways to collect needed node IDs
    // Only nodes referenced by qualifying ways need to be cached.
    // For Canada this reduces 766M cached nodes to ~50M (~93% reduction).
    // -----------------------------------------------------------------------
    info!("Pre-pass 2: scanning ways for needed node IDs...");
    // Phase 2.5 (audit #30): interpolation ways carry no housenumber on the
    // way itself — endpoints do. Collect endpoint IDs here so pass 1 can
    // grab their `addr:housenumber` while the node is being scanned.
    let mut interpolation_endpoint_ids: std::collections::HashSet<i64> =
        std::collections::HashSet::new();
    let mut interpolation_ways_seen = 0usize;
    let mut needed_node_ids: Option<Vec<i64>> = {
        let reader = ElementReader::from_path(pbf_path)?;
        let mut node_refs: Vec<i64> = relation_node_ids; // start with relation node members
        let mut qualifying_ways = 0usize;
        reader.for_each(|element| {
            if let Element::Way(way) = element {
                let is_interp = is_interpolation_way(&way);
                let dominated = has_addr_tags(&way)
                    || way_qualifies(&way)
                    || relation_way_ids.contains(&way.id())
                    || is_interp;
                if dominated {
                    qualifying_ways += 1;
                    let refs: Vec<i64> = way.refs().collect();
                    if is_interp {
                        interpolation_ways_seen += 1;
                        if let (Some(&first), Some(&last)) = (refs.first(), refs.last()) {
                            interpolation_endpoint_ids.insert(first);
                            interpolation_endpoint_ids.insert(last);
                        }
                    }
                    node_refs.extend(refs);
                }
            }
        })?;
        node_refs.sort_unstable();
        node_refs.dedup();
        info!("  {} unique node IDs from {} qualifying ways ({:.0} MB); {} interpolation ways",
            node_refs.len(), qualifying_ways,
            node_refs.len() as f64 * 8.0 / 1e6,
            interpolation_ways_seen);
        Some(node_refs)
    };

    // -----------------------------------------------------------------------
    // Main pass: nodes → ways → relations
    // -----------------------------------------------------------------------
    info!("Main extraction pass: {}...", pbf_path.display());

    let mut node_cache: Box<dyn NodeCache> = {
        info!("Using sorted-file node cache (external merge sort, ~500 MB RAM)");
        Box::new(SortedFileNodeCache::new().map_err(|e| anyhow::anyhow!("Failed to create sorted-file node cache: {}", e))?)
    };

    // Streaming Parquet writers — flush incrementally instead of buffering all
    // records in memory. Keeps places/addresses Vecs at ≤100K entries.
    let place_schema = make_place_schema();
    let addr_schema = make_addr_schema();
    let addr_parquet_path = parquet_path.with_file_name("addresses.parquet");
    std::fs::create_dir_all(parquet_path.parent().unwrap_or(Path::new(".")))?;

    let parquet_props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();
    let mut place_writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(parquet_path)?, place_schema.clone(), Some(parquet_props.clone()),
    )?;
    let mut addr_writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&addr_parquet_path)?, addr_schema.clone(), Some(parquet_props),
    )?;

    const FLUSH_THRESHOLD: usize = 100_000;
    let mut places: Vec<RawPlace> = Vec::with_capacity(FLUSH_THRESHOLD);
    let mut addresses: Vec<RawAddress> = Vec::with_capacity(FLUSH_THRESHOLD);
    let mut total_places = 0usize;
    let mut total_addresses = 0usize;
    let mut flush_error: Option<String> = None;

    let mut pending_ways: Vec<PendingWay> = Vec::new();
    let mut way_index: HashMap<i64, usize> = HashMap::new();

    // No candidate_way_refs buffer needed — we know relation member IDs from pre-pass.

    let mut pending_relations: Vec<PendingRelation> = Vec::new();

    // Batched way processing: buffer ways and resolve node coords in bulk.
    // One sorted scan of 100K+ node IDs is much faster than 10K separate lookups.
    const WAY_BATCH_SIZE: usize = 10_000;
    struct BufferedWay {
        id: i64,
        ref_start: usize, // index into all_refs
        ref_count: usize,
        kind: BufferedWayKind,
    }
    enum BufferedWayKind {
        Address { street: String, housenumber: String, postcode: Option<String>, city: Option<String> },
        Named(PendingWay),
        RelationMember,
        // Phase 2.5 (audit #30) — interpolation way; endpoint housenumbers
        // are looked up at flush time from `endpoint_housenums`.
        Interpolation { spec: InterpolationSpec, first_node: i64, last_node: i64 },
    }
    let mut way_buf: Vec<BufferedWay> = Vec::with_capacity(WAY_BATCH_SIZE);
    let mut all_refs: Vec<i64> = Vec::with_capacity(WAY_BATCH_SIZE * 10);

    // Phase 2.5: housenumber tag of every interpolation endpoint node.
    // Populated during pass 1 node scan and consumed at way-flush time.
    // Sized at 2 × interpolation ways (typically a few thousand entries
    // per country, sub-MB).
    let mut endpoint_housenums: HashMap<i64, String> = HashMap::new();

    let mut cache_sorted = false;
    let mut needed_cursor: usize = 0;
    let mut nodes_seen = 0usize;
    let mut nodes_cached = 0usize;
    let mut nodes_extracted = 0usize;
    let mut addr_nodes_extracted = 0usize;
    let mut addr_ways_resolved = 0usize;
    let mut interpolated_addresses_emitted = 0usize;
    let mut ways_seen = 0usize;

    let reader = ElementReader::from_path(pbf_path)?;
    reader.for_each(|element| {
        if flush_error.is_some() { return; }
        match element {
            Element::Node(node) => {
                nodes_seen += 1;
                // Only cache nodes needed by qualifying ways (merge-scan on sorted IDs)
                if let Some(ref needed) = needed_node_ids {
                    while needed_cursor < needed.len() && needed[needed_cursor] < node.id() {
                        needed_cursor += 1;
                    }
                    if needed_cursor < needed.len() && needed[needed_cursor] == node.id() {
                        node_cache.insert(node.id(), node.lat(), node.lon());
                        nodes_cached += 1;
                        needed_cursor += 1;
                    }
                } else {
                    node_cache.insert(node.id(), node.lat(), node.lon());
                    nodes_cached += 1;
                }
                if let Some(place) = extract_named_node(
                    node.id(), node.lat(), node.lon(),
                    node.tags(), min_population,
                ) {
                    places.push(place);
                    nodes_extracted += 1;
                }
                // Phase 2.5: capture housenumber for interpolation endpoints
                // before we throw away the tag iterator. Allocates only when
                // the node is actually an endpoint of an interpolation way.
                if interpolation_endpoint_ids.contains(&node.id()) {
                    for (k, v) in node.tags() {
                        if k == "addr:housenumber" && !v.is_empty() {
                            endpoint_housenums.insert(node.id(), v.to_owned());
                            break;
                        }
                    }
                }
                {
                    let (addr, building) = extract_addr_node(
                        node.id(), node.lat(), node.lon(), node.tags(),
                    );
                    if let Some(a) = addr {
                        addresses.push(a);
                        addr_nodes_extracted += 1;
                    }
                    if let Some(b) = building {
                        places.push(b);
                    }
                }
                // Streaming flush
                if addresses.len() >= FLUSH_THRESHOLD {
                    match flush_addr_batch(&mut addr_writer, &mut addresses, &addr_schema) {
                        Ok(n) => total_addresses += n,
                        Err(e) => { flush_error = Some(format!("{}", e)); return; }
                    }
                }
                if places.len() >= FLUSH_THRESHOLD {
                    match flush_place_batch(&mut place_writer, &mut places, &place_schema) {
                        Ok(n) => total_places += n,
                        Err(e) => { flush_error = Some(format!("{}", e)); return; }
                    }
                }
            }
            Element::DenseNode(node) => {
                nodes_seen += 1;
                // Only cache nodes needed by qualifying ways (merge-scan on sorted IDs)
                if let Some(ref needed) = needed_node_ids {
                    while needed_cursor < needed.len() && needed[needed_cursor] < node.id() {
                        needed_cursor += 1;
                    }
                    if needed_cursor < needed.len() && needed[needed_cursor] == node.id() {
                        node_cache.insert(node.id(), node.lat(), node.lon());
                        nodes_cached += 1;
                        needed_cursor += 1;
                    }
                } else {
                    node_cache.insert(node.id(), node.lat(), node.lon());
                    nodes_cached += 1;
                }
                if let Some(place) = extract_named_node(
                    node.id(), node.lat(), node.lon(),
                    node.tags(), min_population,
                ) {
                    places.push(place);
                    nodes_extracted += 1;
                }
                if interpolation_endpoint_ids.contains(&node.id()) {
                    for (k, v) in node.tags() {
                        if k == "addr:housenumber" && !v.is_empty() {
                            endpoint_housenums.insert(node.id(), v.to_owned());
                            break;
                        }
                    }
                }
                {
                    let (addr, building) = extract_addr_node(
                        node.id(), node.lat(), node.lon(), node.tags(),
                    );
                    if let Some(a) = addr {
                        addresses.push(a);
                        addr_nodes_extracted += 1;
                    }
                    if let Some(b) = building {
                        places.push(b);
                    }
                }
                // Streaming flush
                if addresses.len() >= FLUSH_THRESHOLD {
                    match flush_addr_batch(&mut addr_writer, &mut addresses, &addr_schema) {
                        Ok(n) => total_addresses += n,
                        Err(e) => { flush_error = Some(format!("{}", e)); return; }
                    }
                }
                if places.len() >= FLUSH_THRESHOLD {
                    match flush_place_batch(&mut place_writer, &mut places, &place_schema) {
                        Ok(n) => total_places += n,
                        Err(e) => { flush_error = Some(format!("{}", e)); return; }
                    }
                }
            }
            Element::Way(way) => {
                if !cache_sorted {
                    // Free the needed node IDs vec (~400 MB for Canada)
                    needed_node_ids = None;
                    info!(
                        "{} nodes cached (of {} seen). Sorting for binary search...",
                        nodes_cached, nodes_seen,
                    );
                    node_cache.prepare_for_reads();
                    info!("Node cache ready. Processing ways...");
                    cache_sorted = true;
                }
                ways_seen += 1;

                // Buffer way into batch for bulk node resolution
                let way_id = way.id();
                let refs: Vec<i64> = way.refs().collect();
                let ref_start = all_refs.len();
                let ref_count = refs.len();
                all_refs.extend_from_slice(&refs);

                // A way can be ALL THREE: an address (addr:* tags), a named
                // place (tourism=museum, place=square, etc.), AND a member of
                // a relation we're stitching. Skansen is
                // `addr:street=Djurgårdsslätten, tourism=theme_park` — push
                // to every pipeline it qualifies for. They all reuse the
                // same resolved centroid; relation members consume node
                // coords downstream regardless of whether the way is also
                // emitted as a standalone place/address.
                if let Some((street, housenumber, postcode, city)) = scan_addr_way_tags(&way) {
                    way_buf.push(BufferedWay {
                        id: way_id, ref_start, ref_count,
                        kind: BufferedWayKind::Address { street, housenumber, postcode, city },
                    });
                }
                if let Some(pending) = scan_way(&way) {
                    way_buf.push(BufferedWay {
                        id: way_id, ref_start, ref_count,
                        kind: BufferedWayKind::Named(pending),
                    });
                }
                if relation_way_ids.contains(&way_id) {
                    way_buf.push(BufferedWay {
                        id: way_id, ref_start, ref_count,
                        kind: BufferedWayKind::RelationMember,
                    });
                }
                // Phase 2.5 (audit #30) — interpolation way buffered like
                // an Address way; both endpoint node IDs are stashed so the
                // flush path can look up their housenumbers.
                if is_interpolation_way(&way) {
                    if let Some(spec) = scan_interpolation_way(&way) {
                        if let (Some(&first), Some(&last)) =
                            (refs.first(), refs.last())
                        {
                            way_buf.push(BufferedWay {
                                id: way_id, ref_start, ref_count,
                                kind: BufferedWayKind::Interpolation {
                                    spec, first_node: first, last_node: last,
                                },
                            });
                        }
                    }
                }

                // Flush batch when full — one big batch_get for all buffered ways
                if way_buf.len() >= WAY_BATCH_SIZE {
                    // Resolve all node refs in one sorted scan
                    let resolved = node_cache.batch_get(&all_refs);

                    for bw in way_buf.drain(..) {
                        let coords: Vec<Option<(f64, f64)>> = resolved[bw.ref_start..bw.ref_start + bw.ref_count].to_vec();
                        let centroid = centroid_from_resolved(&coords);

                        match bw.kind {
                            BufferedWayKind::Address { street, housenumber, postcode, city } => {
                                if let Some((lat, lon)) = centroid {
                                    addresses.push(RawAddress {
                                        osm_id: bw.id, street, housenumber, postcode, city, state: None, lat, lon,
                                    });
                                    addr_ways_resolved += 1;
                                }
                            }
                            BufferedWayKind::Named(pending) => {
                                if !pending.name.is_empty() {
                                    if let Some((lat, lon)) = centroid {
                                        let bbox = RawBBox::from_coords(
                                            coords.iter().filter_map(|c| *c)
                                        );
                                        let (cls, cls_val) = pending.osm_class_value.clone()
                                            .map(|(c, v)| (Some(c), Some(v)))
                                            .unwrap_or((None, None));
                                        places.push(RawPlace {
                                            osm_id: pending.id,
                                            osm_type: OsmType::Way,
                                            name: pending.name.clone(),
                                            name_intl: pending.name_intl.clone(),
                                            alt_names: pending.alt_names.clone(),
                                            old_names: pending.old_names.clone(),
                                            coord: Coord::new(lat, lon),
                                            place_type: pending.place_type,
                                            admin_level: pending.admin_level,
                                            country_code: detect_country(lat, lon),
                                            admin1: None,
                                            admin2: None,
                                            population: pending.population,
                                            wikidata: pending.wikidata.clone(),
                                            class: cls,
                                            class_value: cls_val,
                                            bbox,
                                            extratags: pending.extratags.clone(),
                                        });
                                    }
                                }
                                if relation_way_ids.contains(&pending.id) {
                                    let idx = pending_ways.len();
                                    way_index.insert(pending.id, idx);
                                    pending_ways.push(pending);
                                }
                            }
                            BufferedWayKind::RelationMember => {
                                let node_refs = all_refs[bw.ref_start..bw.ref_start + bw.ref_count].to_vec();
                                let idx = pending_ways.len();
                                way_index.insert(bw.id, idx);
                                pending_ways.push(PendingWay {
                                    id: bw.id, name: String::new(), name_intl: vec![],
                                    alt_names: vec![], old_names: vec![], population: None,
                                    admin_level: None, wikidata: None, place_type: PlaceType::Unknown,
                                    osm_class_value: None,
                                    extratags: vec![],
                                    node_refs,
                                });
                            }
                            BufferedWayKind::Interpolation { spec, first_node, last_node } => {
                                let resolved_coords: Vec<(f64, f64)> =
                                    coords.iter().filter_map(|c| *c).collect();
                                if let (Some(start_str), Some(end_str)) = (
                                    endpoint_housenums.get(&first_node),
                                    endpoint_housenums.get(&last_node),
                                ) {
                                    if let (Ok(start_n), Ok(end_n)) = (
                                        start_str.parse::<u32>(),
                                        end_str.parse::<u32>(),
                                    ) {
                                        interpolated_addresses_emitted +=
                                            synthesise_interpolated_addresses(
                                                bw.id, &spec, start_n, end_n,
                                                &resolved_coords, &mut addresses,
                                            );
                                    }
                                }
                            }
                        }
                    }
                    all_refs.clear();

                    // Streaming flush
                    if addresses.len() >= FLUSH_THRESHOLD {
                        match flush_addr_batch(&mut addr_writer, &mut addresses, &addr_schema) {
                            Ok(n) => total_addresses += n,
                            Err(e) => { flush_error = Some(format!("{}", e)); return; }
                        }
                    }
                    if places.len() >= FLUSH_THRESHOLD {
                        match flush_place_batch(&mut place_writer, &mut places, &place_schema) {
                            Ok(n) => total_places += n,
                            Err(e) => { flush_error = Some(format!("{}", e)); return; }
                        }
                    }
                }
            }
            Element::Relation(relation) => {
                if !cache_sorted {
                    node_cache.prepare_for_reads();
                    cache_sorted = true;
                }
                if let Some(pending) = scan_relation(&relation) {
                    pending_relations.push(pending);
                }
            }
            _ => {}
        }
    })?;

    // Flush remaining buffered ways
    if !way_buf.is_empty() && flush_error.is_none() {
        let resolved = node_cache.batch_get(&all_refs);
        for bw in way_buf.drain(..) {
            let coords: Vec<Option<(f64, f64)>> = resolved[bw.ref_start..bw.ref_start + bw.ref_count].to_vec();
            let centroid = centroid_from_resolved(&coords);
            match bw.kind {
                BufferedWayKind::Address { street, housenumber, postcode, city } => {
                    if let Some((lat, lon)) = centroid {
                        addresses.push(RawAddress { osm_id: bw.id, street, housenumber, postcode, city, state: None, lat, lon });
                        addr_ways_resolved += 1;
                    }
                }
                BufferedWayKind::Named(pending) => {
                    if !pending.name.is_empty() {
                        if let Some((lat, lon)) = centroid {
                            let bbox = RawBBox::from_coords(coords.iter().filter_map(|c| *c));
                            let (cls, cls_val) = pending.osm_class_value.clone()
                                .map(|(c, v)| (Some(c), Some(v)))
                                .unwrap_or((None, None));
                            places.push(RawPlace {
                                osm_id: pending.id, osm_type: OsmType::Way,
                                name: pending.name.clone(), name_intl: pending.name_intl.clone(),
                                alt_names: pending.alt_names.clone(), old_names: pending.old_names.clone(),
                                coord: Coord::new(lat, lon), place_type: pending.place_type,
                                admin_level: pending.admin_level, country_code: detect_country(lat, lon),
                                admin1: None, admin2: None,
                                population: pending.population, wikidata: pending.wikidata.clone(),
                                class: cls, class_value: cls_val, bbox,
                                extratags: pending.extratags.clone(),
                            });
                        }
                    }
                    if relation_way_ids.contains(&pending.id) {
                        let idx = pending_ways.len();
                        way_index.insert(pending.id, idx);
                        pending_ways.push(pending);
                    }
                }
                BufferedWayKind::RelationMember => {
                    let node_refs = all_refs[bw.ref_start..bw.ref_start + bw.ref_count].to_vec();
                    let idx = pending_ways.len();
                    way_index.insert(bw.id, idx);
                    pending_ways.push(PendingWay {
                        id: bw.id, name: String::new(), name_intl: vec![],
                        alt_names: vec![], old_names: vec![], population: None,
                        admin_level: None, wikidata: None, place_type: PlaceType::Unknown,
                        osm_class_value: None,
                        extratags: vec![],
                        node_refs,
                    });
                }
                BufferedWayKind::Interpolation { spec, first_node, last_node } => {
                    let resolved_coords: Vec<(f64, f64)> =
                        coords.iter().filter_map(|c| *c).collect();
                    if let (Some(start_str), Some(end_str)) = (
                        endpoint_housenums.get(&first_node),
                        endpoint_housenums.get(&last_node),
                    ) {
                        if let (Ok(start_n), Ok(end_n)) = (
                            start_str.parse::<u32>(),
                            end_str.parse::<u32>(),
                        ) {
                            interpolated_addresses_emitted +=
                                synthesise_interpolated_addresses(
                                    bw.id, &spec, start_n, end_n,
                                    &resolved_coords, &mut addresses,
                                );
                        }
                    }
                }
            }
        }
    }

    if let Some(msg) = flush_error {
        anyhow::bail!("Parquet write failed during extraction: {}", msg);
    }

    info!(
        "Single pass complete: {} nodes ({} cached, {} places, {} addrs), {} ways ({} named, {} relation members, {} interpolated addrs), {} relations",
        nodes_seen, nodes_cached, nodes_extracted, addr_nodes_extracted,
        ways_seen, way_index.len(),
        pending_ways.len() - way_index.len(), // relation-only ways
        interpolated_addresses_emitted,
        pending_relations.len(),
    );

    // node_refs are already empty for non-relation ways (cleared during way phase).
    // This is a safety check — should report 0 freed.
    let refs_freed: usize = pending_ways.iter()
        .filter(|pw| !relation_way_ids.contains(&pw.id))
        .map(|pw| pw.node_refs.len())
        .sum();
    if refs_freed > 0 {
        info!("Freed {} stale node_refs ({:.0} MB)", refs_freed, refs_freed as f64 * 8.0 / 1e6);
    }

    // -----------------------------------------------------------------------
    // Resolve relation centroids (way centroids already resolved during scan)
    // -----------------------------------------------------------------------
    info!("Resolving centroids for {} relations...", pending_relations.len());

    let mut relations_resolved = 0usize;
    let mut relations_no_centroid = 0usize;

    for pr in &pending_relations {
        if let Some(coord) = compute_relation_centroid_from_pending(
            pr, &*node_cache, &pending_ways, &way_index,
        ) {
            // Walk member-way coords to derive a bbox. May be None for
            // node-only relations or unresolved members; pack synthesises
            // a small fallback in that case.
            let bbox = compute_relation_bbox_from_pending(
                pr, &*node_cache, &pending_ways, &way_index,
            );
            let (cls, cls_val) = pr.osm_class_value.clone()
                .map(|(c, v)| (Some(c), Some(v)))
                .unwrap_or((None, None));
            places.push(RawPlace {
                osm_id: pr.id,
                osm_type: OsmType::Relation,
                name: pr.name.clone(),
                name_intl: pr.name_intl.clone(),
                alt_names: pr.alt_names.clone(),
                old_names: pr.old_names.clone(),
                coord,
                place_type: pr.place_type,
                admin_level: pr.admin_level,
                country_code: detect_country(coord.lat_f64(), coord.lon_f64()),
                admin1: None,
                admin2: None,
                population: pr.population,
                extratags: pr.extratags.clone(),
                wikidata: pr.wikidata.clone(),
                class: cls,
                class_value: cls_val,
                bbox,
            });
            relations_resolved += 1;
        } else {
            relations_no_centroid += 1;
        }
    }

    info!(
        "Resolved: {} relations ({} failed), {} address ways (inline)",
        relations_resolved, relations_no_centroid,
        addr_ways_resolved,
    );
    let buffered_places = places.len();
    let buffered_addrs = addresses.len();
    info!(
        "Total: {} places, {} addresses ({} addr nodes + {} addr ways)",
        total_places + buffered_places,
        total_addresses + buffered_addrs, addr_nodes_extracted, addr_ways_resolved,
    );

    // Build polygon geometry for admin boundary relations (for point-in-polygon
    // assignment in enrich step). Only keep relations with admin_level 3-7.
    let mut admin_polygons: Vec<AdminPolygon> = Vec::new();
    for pr in &pending_relations {
        let level = match pr.admin_level {
            Some(l) if (3..=7).contains(&l) => l,
            _ => continue,
        };
        if pr.name.is_empty() { continue; }

        // Only use outer/empty-role ways for polygon construction
        let ways: Vec<Vec<(f64, f64)>> = pr.way_members.iter()
            .filter(|(_, role)| role.is_empty() || role == "outer")
            .filter_map(|(wid, _)| way_index.get(wid))
            .map(|&idx| &pending_ways[idx])
            .map(|pw| {
                let coords = node_cache.batch_get(&pw.node_refs);
                coords.into_iter().flatten().collect::<Vec<(f64, f64)>>()
            })
            .filter(|coords| coords.len() >= 2)
            .collect();

        let rings = stitch_ways_to_rings(&ways);
        if !rings.is_empty() {
            admin_polygons.push(AdminPolygon {
                osm_id: pr.id,
                admin_level: level,
                rings,
            });
        }
    }
    info!("Built {} admin polygons for point-in-polygon assignment", admin_polygons.len());

    // Flush remaining buffered records and close Parquet writers
    total_places += flush_place_batch(&mut place_writer, &mut places, &place_schema)?;
    total_addresses += flush_addr_batch(&mut addr_writer, &mut addresses, &addr_schema)?;
    place_writer.close()?;
    addr_writer.close()?;
    info!("Parquet written: {} places, {} addresses (streaming)", total_places, total_addresses);

    // Write admin polygons (build-time only, not part of runtime index)
    let polygons_path = parquet_path.with_file_name("admin_polygons.bin");
    let poly_bytes = bincode::serialize(&admin_polygons)?;
    std::fs::write(&polygons_path, &poly_bytes)?;
    info!("admin_polygons.bin: {:.1} MB ({} polygons)", poly_bytes.len() as f64 / 1e6, admin_polygons.len());

    Ok(ExtractResult {
        place_count: total_places,
        address_count: total_addresses,
    })
}

// ---------------------------------------------------------------------------
// Pass 1 scanners — collect metadata without coordinates
// ---------------------------------------------------------------------------

/// Scan a way for qualifying tags. Returns PendingWay if it's worth extracting.
fn scan_way(way: &osmpbf::Way) -> Option<PendingWay> {
    let mut name: Option<String> = None;
    let mut place_tag: Option<String> = None;
    let mut alt_names: Vec<String> = vec![];
    let mut old_names: Vec<String> = vec![];
    let mut name_intl: Vec<(String, String)> = vec![];
    let mut population: Option<u32> = None;
    let mut admin_level: Option<u8> = None;
    let mut wikidata: Option<String> = None;
    let mut qualifying_tag: Option<(String, String)> = None;
    let mut is_highway = false;
    let mut is_building = false;
    let mut building_value: Option<String> = None;
    let mut has_wikipedia = false;
    let mut highway_value: Option<String> = None;
    let mut is_area = false;
    let mut extratags: Vec<(String, String)> = vec![];

    for (k, v) in way.tags() {
        if is_extratag_key(k) {
            extratags.push((k.to_owned(), v.to_owned()));
        }
        match k {
            "name" => name = Some(v.to_owned()),
            "place" => place_tag = Some(v.to_owned()),
            "alt_name" => alt_names.extend(v.split(';').map(|s| s.trim().to_owned())),
            "loc_name" | "short_name" | "nat_name" | "reg_name" | "official_name" => {
                alt_names.extend(v.split(';').map(|s| s.trim().to_owned()));
            }
            "old_name" => old_names.extend(v.split(';').map(|s| s.trim().to_owned())),
            "population" => population = v.parse().ok(),
            "admin_level" => admin_level = v.parse().ok(),
            "wikidata" => wikidata = Some(v.to_owned()),
            "wikipedia" => has_wikipedia = true,
            "highway" => { is_highway = true; highway_value = Some(v.to_owned()); }
            "area" => { if v == "yes" { is_area = true; } }
            "building" => { is_building = true; building_value = Some(v.to_owned()); }
            k if k.starts_with("name:") => {
                let lang = &k[5..];
                if lang.len() == 2 {
                    name_intl.push((lang.to_owned(), v.to_owned()));
                }
            }
            _ => {
                if qualifying_tag.is_none() {
                    for &mt in MEANINGFUL_WAY_TAGS {
                        if k == mt && is_qualifying_poi(k, v) {
                            qualifying_tag = Some((k.to_owned(), v.to_owned()));
                            break;
                        }
                    }
                }
            }
        }
    }

    // Must have a name.
    let name = name?;
    // Highway notability gate. Without this, every residential street in the
    // PBF gets indexed and floods the FST. We accept four signals:
    //   - wikidata / wikipedia (regionally famous)
    //   - alt_name / loc_name / short_name (`old_name=`-style historic names)
    //   - name_intl (officially bilingual streets — Helsinki / Espoo / Turku
    //     in Finland, Brussels in Belgium, Wales, Catalonia, …). The
    //     `name:sv` (or any `name:<lang>`) tag itself is the notability
    //     signal: someone bothered to translate it because it matters.
    let has_notability = wikidata.is_some() || has_wikipedia
        || !alt_names.is_empty() || !name_intl.is_empty();
    let highway_qualifies = is_highway && has_notability;
    if is_highway && !highway_qualifies {
        return None;
    }
    // Building notability gate (Phase 2.4 / audit #29). A building qualifies
    // as a searchable place if it has another qualifying POI tag (Vasamuseet
    // = building=museum + tourism=museum, the tourism wins) OR carries a
    // notability signal of its own — wikidata/wikipedia (Empire State
    // Building, Sagrada Família), an alt/loc/short_name, or a translated
    // name. Pure `building=yes, name=*` with nothing else stays out so we
    // don't flood the FST with every named warehouse and detached house.
    let building_qualifies = is_building && place_tag.is_none()
        && qualifying_tag.is_none() && has_notability;
    if is_building && qualifying_tag.is_none() && place_tag.is_none()
        && !building_qualifies
    {
        return None;
    }

    // Notability gate: leisure=park / garden / nature_reserve are extremely
    // numerous (every pocket park gets one). Require a wikidata signal so
    // only regionally-known parks (Liseberg, Skansen, Slottsskogen) are kept.
    if place_tag.is_none() {
        if let Some((ref tk, ref tv)) = qualifying_tag {
            if leisure_needs_notability(tk, tv) && wikidata.is_none() {
                return None;
            }
        }
    }

    // Must have a qualifying tag, OR be a notability-gated highway.
    // A pedestrianised plaza without an explicit place=square tag
    // (Stockholm Stortorget = `highway=pedestrian, area=yes`) should
    // still be classified as Square so it ranks against the other named
    // squares in the country.
    let place_type = if let Some(ref pt) = place_tag {
        PlaceType::from_osm(pt)
    } else if let Some((ref tk, ref tv)) = qualifying_tag {
        place_type_from_tag(tk, tv)
    } else if highway_qualifies {
        if is_area && highway_value.as_deref() == Some("pedestrian") {
            PlaceType::Square
        } else {
            PlaceType::Street
        }
    } else if building_qualifies {
        // Notable named building (Empire State Building, Sagrada Família, …).
        // Bucket as Landmark — it ranks alongside other notable POIs and
        // gets the same importance weight at query time.
        PlaceType::Landmark
    } else {
        return None;
    };

    let node_refs: Vec<i64> = way.refs().collect();

    let osm_class_value = class_value_from_tags(place_tag.as_ref(), qualifying_tag.as_ref())
        .or_else(|| {
            // Pedestrianised plaza without an explicit place=* tag —
            // synthesise place=square so the API still emits a sensible class/type.
            if matches!(place_type, PlaceType::Square) {
                Some(("place".to_owned(), "square".to_owned()))
            } else if matches!(place_type, PlaceType::Street) {
                // Notable named highway picked up via the highway-qualifies path.
                highway_value.clone().map(|v| ("highway".to_owned(), v))
            } else if building_qualifies {
                // Notable named building — class=building, type=<value>
                // (e.g. building=yes, building=hotel, building=castle).
                building_value.clone().map(|v| ("building".to_owned(), v))
            } else {
                None
            }
        });

    Some(PendingWay {
        id: way.id(),
        name,
        name_intl,
        alt_names,
        old_names,
        population,
        admin_level,
        wikidata,
        place_type,
        osm_class_value,
        extratags,
        node_refs,
    })
}

/// Scan a relation for qualifying tags. Returns PendingRelation if worth extracting.
fn scan_relation(relation: &osmpbf::Relation) -> Option<PendingRelation> {
    let mut name: Option<String> = None;
    let mut admin_level: Option<u8> = None;
    let mut place_tag: Option<String> = None;
    let mut wikidata: Option<String> = None;
    let mut name_intl: Vec<(String, String)> = vec![];
    let mut alt_names: Vec<String> = vec![];
    let mut old_names: Vec<String> = vec![];
    let mut population: Option<u32> = None;
    let mut boundary_tag: Option<String> = None;
    let mut rel_type: Option<String> = None;
    let mut qualifying_tag: Option<(String, String)> = None;
    let mut extratags: Vec<(String, String)> = vec![];

    for (k, v) in relation.tags() {
        if is_extratag_key(k) {
            extratags.push((k.to_owned(), v.to_owned()));
        }
        match k {
            "name" => name = Some(v.to_owned()),
            "admin_level" => admin_level = v.parse().ok(),
            "place" => place_tag = Some(v.to_owned()),
            "wikidata" => wikidata = Some(v.to_owned()),
            "boundary" => boundary_tag = Some(v.to_owned()),
            "type" => rel_type = Some(v.to_owned()),
            "population" => population = v.parse().ok(),
            "alt_name" => alt_names.extend(v.split(';').map(|s| s.trim().to_owned())),
            "loc_name" | "short_name" | "nat_name" | "reg_name" | "official_name" => {
                alt_names.extend(v.split(';').map(|s| s.trim().to_owned()));
            }
            "old_name" => old_names.extend(v.split(';').map(|s| s.trim().to_owned())),
            k if k.starts_with("name:") => {
                let lang = &k[5..];
                if lang.len() == 2 {
                    name_intl.push((lang.to_owned(), v.to_owned()));
                }
            }
            _ => {
                if qualifying_tag.is_none() {
                    for &mt in MEANINGFUL_WAY_TAGS {
                        if k == mt && is_qualifying_poi(k, v) {
                            qualifying_tag = Some((k.to_owned(), v.to_owned()));
                            break;
                        }
                    }
                }
            }
        }
    }

    let name = name?;

    let is_admin = boundary_tag.as_deref() == Some("administrative") && admin_level.is_some();
    let is_multipolygon = rel_type.as_deref() == Some("multipolygon");
    let has_place = place_tag.is_some();
    let has_qualifying = qualifying_tag.is_some();

    if !is_admin && !has_place && !(is_multipolygon && has_qualifying) {
        return None;
    }

    if is_admin {
        if let Some(level) = admin_level {
            if level < 2 || level > 10 {
                return None;
            }
        }
    }

    // Notability gate for leisure=park / garden / nature_reserve relations
    if !is_admin && place_tag.is_none() {
        if let Some((ref tk, ref tv)) = qualifying_tag {
            if leisure_needs_notability(tk, tv) && wikidata.is_none() {
                return None;
            }
        }
    }

    let place_type = if let Some(ref pt) = place_tag {
        PlaceType::from_osm(pt)
    } else if is_admin {
        match admin_level {
            Some(2) => PlaceType::Country,
            Some(3..=4) => PlaceType::State,
            Some(5..=7) => PlaceType::County,
            _ => PlaceType::Unknown,
        }
    } else if let Some((ref tk, ref tv)) = qualifying_tag {
        place_type_from_tag(tk, tv)
    } else {
        PlaceType::Unknown
    };

    // Collect member IDs by type (with role for way members)
    let mut node_member_ids: Vec<i64> = Vec::new();
    let mut way_members: Vec<(i64, String)> = Vec::new();

    for member in relation.members() {
        match member.member_type {
            osmpbf::elements::RelMemberType::Node => {
                node_member_ids.push(member.member_id);
            }
            osmpbf::elements::RelMemberType::Way => {
                let role = member.role().unwrap_or("").to_owned();
                way_members.push((member.member_id, role));
            }
            _ => {}
        }
    }

    let osm_class_value = class_value_from_tags(place_tag.as_ref(), qualifying_tag.as_ref())
        .or_else(|| {
            // Boundary relations qualify on `boundary=administrative` even
            // when `place=*` is missing — record that as the class/type pair.
            if is_admin {
                Some(("boundary".to_owned(), "administrative".to_owned()))
            } else {
                None
            }
        });

    Some(PendingRelation {
        id: relation.id(),
        name,
        name_intl,
        alt_names,
        old_names,
        population,
        admin_level,
        wikidata,
        place_type,
        osm_class_value,
        extratags,
        node_member_ids,
        way_members,
    })
}

// ---------------------------------------------------------------------------
// Pass 2: Node extraction
// ---------------------------------------------------------------------------

fn extract_named_node<'a>(
    id: i64,
    lat: f64,
    lon: f64,
    tags: impl Iterator<Item = (&'a str, &'a str)>,
    min_population: u32,
) -> Option<RawPlace> {
    let parsed = parse_tags(tags);

    let name = parsed.name?;

    // Notability gate for leisure=park / garden / nature_reserve nodes —
    // require wikidata so neighborhood pocket parks don't flood the index.
    if parsed.place_tag.is_none() {
        if let Some((ref tk, ref tv)) = parsed.qualifying_tag {
            if leisure_needs_notability(tk, tv) && parsed.wikidata.is_none() {
                return None;
            }
        }
    }

    let place_type = if let Some(ref place_tag) = parsed.place_tag {
        PlaceType::from_osm(place_tag)
    } else if let Some((ref tag_key, ref tag_val)) = parsed.qualifying_tag {
        place_type_from_tag(tag_key, tag_val)
    } else {
        return None;
    };

    if parsed.place_tag.is_some() && min_population > 0 {
        if let Some(pop) = parsed.population {
            if pop < min_population {
                return None;
            }
        }
    }

    if lat.abs() > 90.0 || lon.abs() > 180.0 {
        return None;
    }

    let (cls, cls_val) = class_value_from_tags(parsed.place_tag.as_ref(), parsed.qualifying_tag.as_ref())
        .map(|(c, v)| (Some(c), Some(v)))
        .unwrap_or((None, None));

    Some(RawPlace {
        osm_id: id,
        osm_type: OsmType::Node,
        name,
        name_intl: parsed.name_intl,
        alt_names: parsed.alt_names,
        old_names: parsed.old_names,
        coord: Coord::new(lat, lon),
        place_type,
        admin_level: parsed.admin_level,
        country_code: detect_country(lat, lon),
        admin1: None,
        admin2: None,
        population: parsed.population,
        extratags: parsed.extratags,
        wikidata: parsed.wikidata,
        class: cls,
        class_value: cls_val,
        bbox: None,
    })
}

// ---------------------------------------------------------------------------
// Tag parsing helper
// ---------------------------------------------------------------------------

pub(crate) struct ParsedTags {
    pub(crate) name: Option<String>,
    pub(crate) place_tag: Option<String>,
    pub(crate) alt_names: Vec<String>,
    pub(crate) old_names: Vec<String>,
    pub(crate) name_intl: Vec<(String, String)>,
    pub(crate) population: Option<u32>,
    pub(crate) admin_level: Option<u8>,
    pub(crate) wikidata: Option<String>,
    pub(crate) qualifying_tag: Option<(String, String)>,
    pub(crate) extratags: Vec<(String, String)>,
}

pub(crate) fn parse_tags<'a>(tags: impl Iterator<Item = (&'a str, &'a str)>) -> ParsedTags {
    let mut parsed = ParsedTags {
        name: None,
        place_tag: None,
        alt_names: vec![],
        old_names: vec![],
        name_intl: vec![],
        population: None,
        admin_level: None,
        wikidata: None,
        qualifying_tag: None,
        extratags: vec![],
    };

    for (k, v) in tags {
        if is_extratag_key(k) {
            parsed.extratags.push((k.to_owned(), v.to_owned()));
        }
        match k {
            "name" => parsed.name = Some(v.to_owned()),
            "place" => parsed.place_tag = Some(v.to_owned()),
            "alt_name" => {
                parsed.alt_names.extend(v.split(';').map(|s| s.trim().to_owned()));
            }
            // loc_name = local/colloquial name (e.g. Avenyn for Kungsportsavenyen).
            // Treat it as an alt_name so the FST index resolves it.
            "loc_name" | "short_name" | "nat_name" | "reg_name" | "official_name" => {
                parsed.alt_names.extend(v.split(';').map(|s| s.trim().to_owned()));
            }
            "old_name" => {
                parsed.old_names.extend(v.split(';').map(|s| s.trim().to_owned()));
            }
            "population" => parsed.population = v.parse().ok(),
            "admin_level" => parsed.admin_level = v.parse().ok(),
            "wikidata" => parsed.wikidata = Some(v.to_owned()),
            k if k.starts_with("name:") => {
                let lang = &k[5..];
                if lang.len() == 2 {
                    parsed.name_intl.push((lang.to_owned(), v.to_owned()));
                }
            }
            _ => {
                if parsed.qualifying_tag.is_none() {
                    for &mt in MEANINGFUL_NODE_TAGS {
                        if k == mt && is_qualifying_poi(k, v) {
                            parsed.qualifying_tag = Some((k.to_owned(), v.to_owned()));
                            break;
                        }
                    }
                }
            }
        }
    }

    parsed
}

// ---------------------------------------------------------------------------
// Centroid computation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Admin polygon geometry
// ---------------------------------------------------------------------------

/// A serializable admin boundary polygon, extracted at build time.
/// Supports multi-ring geometry (e.g. island municipalities).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AdminPolygon {
    pub osm_id: i64,
    pub admin_level: u8,
    /// Closed rings of (lat, lon) coordinates — one per connected component
    pub rings: Vec<Vec<(f64, f64)>>,
}

/// Stitch unordered ways into closed rings.
/// Each way is a sequence of (lat, lon) coordinates. Ways may be stored
/// in either direction. We greedily connect them by matching endpoints.
/// When no more ways can be connected, start a new ring from the remaining
/// ways. This handles multi-island municipalities (e.g. Danish kommuner
/// spanning Jutland + islands).
fn stitch_ways_to_rings(ways: &[Vec<(f64, f64)>]) -> Vec<Vec<(f64, f64)>> {
    if ways.is_empty() { return vec![]; }

    let mut remaining: Vec<Vec<(f64, f64)>> = ways.to_vec();
    let mut rings: Vec<Vec<(f64, f64)>> = Vec::new();

    while !remaining.is_empty() {
        let mut ring = remaining.remove(0);
        let max_iters = remaining.len() + 1;
        let mut iters = 0;

        while !remaining.is_empty() && iters < max_iters {
            iters += 1;
            let last = match ring.last() {
                Some(l) => *l,
                None => break,
            };

            let next = remaining.iter().position(|way| {
                if way.is_empty() { return false; }
                let start = way[0];
                let end = *way.last().unwrap();
                coords_close(start, last) || coords_close(end, last)
            });

            match next {
                Some(idx) => {
                    let way = remaining.remove(idx);
                    let start = way[0];
                    if coords_close(start, last) {
                        // Forward: skip first point (duplicate of ring end)
                        ring.extend_from_slice(&way[1..]);
                    } else {
                        // Reverse: skip last point (duplicate of ring end)
                        ring.extend(way[..way.len() - 1].iter().rev());
                    }
                }
                None => break, // Gap — this ring is done
            }
        }

        // Close the ring if not already closed
        if ring.len() >= 3 {
            let first = ring[0];
            let last = *ring.last().unwrap();
            if !coords_close(first, last) {
                ring.push(first);
            }
            if ring.len() >= 4 { // Need at least 4 points for a closed ring
                rings.push(ring);
            }
        }
    }

    rings
}

/// Check if two coordinates are approximately equal (within ~1m)
fn coords_close(a: (f64, f64), b: (f64, f64)) -> bool {
    (a.0 - b.0).abs() < 0.00001 && (a.1 - b.1).abs() < 0.00001
}

// ---------------------------------------------------------------------------
// Centroid computation
// ---------------------------------------------------------------------------

/// Compute centroid from pre-resolved coordinates (used by batched way processing).
fn centroid_from_resolved(coords: &[Option<(f64, f64)>]) -> Option<(f64, f64)> {
    let mut sum_lat = 0.0;
    let mut sum_lon = 0.0;
    let mut count = 0u32;
    for coord in coords {
        if let Some((lat, lon)) = coord {
            sum_lat += lat;
            sum_lon += lon;
            count += 1;
        }
    }
    if count == 0 { return None; }
    Some((sum_lat / count as f64, sum_lon / count as f64))
}

fn compute_centroid(
    refs: &[i64],
    cache: &dyn NodeCache,
) -> Option<(f64, f64)> {
    // Use batch_get for sequential mmap access (sorted internally)
    let coords = cache.batch_get(refs);

    let mut sum_lat = 0.0;
    let mut sum_lon = 0.0;
    let mut count = 0u32;

    for coord in &coords {
        if let Some((lat, lon)) = coord {
            sum_lat += lat;
            sum_lon += lon;
            count += 1;
        }
    }

    if count == 0 {
        return None;
    }

    Some((sum_lat / count as f64, sum_lon / count as f64))
}

fn compute_relation_centroid_from_pending(
    pr: &PendingRelation,
    cache: &dyn NodeCache,
    pending_ways: &[PendingWay],
    way_index: &HashMap<i64, usize>,
) -> Option<Coord> {
    // Collect all node IDs we need to resolve (for batch lookup)
    let mut all_ids: Vec<i64> = Vec::new();

    // Direct node members
    all_ids.extend_from_slice(&pr.node_member_ids);

    // Way members — sample nodes from each way
    for &(wid, _) in &pr.way_members {
        if let Some(&idx) = way_index.get(&wid) {
            let way = &pending_ways[idx];
            let step = (way.node_refs.len() / 10).max(1);
            for node_id in way.node_refs.iter().step_by(step) {
                all_ids.push(*node_id);
            }
        }
    }

    if all_ids.is_empty() { return None; }

    // Batch lookup — sorted for sequential mmap access
    let coords = cache.batch_get(&all_ids);

    let mut sum_lat = 0.0;
    let mut sum_lon = 0.0;
    let mut count = 0u32;
    for coord in &coords {
        if let Some((lat, lon)) = coord {
            sum_lat += lat;
            sum_lon += lon;
            count += 1;
        }
    }

    if count == 0 { return None; }
    Some(Coord::new(sum_lat / count as f64, sum_lon / count as f64))
}

/// Compute a relation's bbox from its way members (full set, not sampled).
/// Mirrors `compute_relation_centroid_from_pending` but returns the
/// extent of every resolved member coordinate. None when no member coords
/// could be resolved.
fn compute_relation_bbox_from_pending(
    pr: &PendingRelation,
    cache: &dyn NodeCache,
    pending_ways: &[PendingWay],
    way_index: &HashMap<i64, usize>,
) -> Option<RawBBox> {
    let mut all_ids: Vec<i64> = Vec::new();
    all_ids.extend_from_slice(&pr.node_member_ids);
    for &(wid, _) in &pr.way_members {
        if let Some(&idx) = way_index.get(&wid) {
            let way = &pending_ways[idx];
            all_ids.extend_from_slice(&way.node_refs);
        }
    }
    if all_ids.is_empty() { return None; }

    let coords = cache.batch_get(&all_ids);
    RawBBox::from_coords(coords.into_iter().flatten())
}

// ---------------------------------------------------------------------------
// Place type from non-place tags
// ---------------------------------------------------------------------------

pub(crate) fn place_type_from_tag(key: &str, value: &str) -> PlaceType {
    match (key, value) {
        ("natural", "water") | ("natural", "lake") => PlaceType::Lake,
        ("natural", "bay") => PlaceType::Bay,
        ("natural", "cape") | ("natural", "peninsula") => PlaceType::Cape,
        ("natural", "island") => PlaceType::Island,
        ("natural", "islet") => PlaceType::Islet,
        ("natural", "peak") | ("natural", "mountain") | ("mountain_pass", _) => PlaceType::Mountain,
        ("natural", "wood") | ("natural", "forest") | ("landuse", "forest") => PlaceType::Forest,
        ("waterway", "river") | ("waterway", "stream") | ("waterway", "canal") => PlaceType::River,
        ("aeroway", "aerodrome") => PlaceType::Airport,
        ("railway", "station") | ("railway", "halt") => PlaceType::Station,
        ("public_transport", "station") => PlaceType::Station,
        ("landuse", "residential") | ("place", "suburb") => PlaceType::Suburb,
        ("place", "neighbourhood") | ("place", "neighborhood") => PlaceType::Neighbourhood,
        ("place", "square") => PlaceType::Square,
        ("place", "locality") => PlaceType::Locality,
        ("leisure", "park") | ("leisure", "garden") => PlaceType::Park,
        ("leisure", "nature_reserve") => PlaceType::Park,
        // Major venues — Ullevi, Scandinavium, Avicii Arena, Friends Arena
        ("leisure", "stadium") | ("leisure", "sports_centre")
        | ("leisure", "ice_rink") => PlaceType::Landmark,
        // Famous man-made structures (bridges, lighthouses, towers)
        ("man_made", "bridge") | ("man_made", "lighthouse")
        | ("man_made", "tower") => PlaceType::Landmark,
        // Tourism — mostly visitor-attraction landmarks
        ("tourism", "attraction") | ("tourism", "museum") | ("tourism", "gallery")
        | ("tourism", "viewpoint") | ("tourism", "theme_park") | ("tourism", "zoo")
        | ("tourism", "aquarium") => PlaceType::Landmark,
        // Historic — castles, monuments, ruins, memorials, archaeological sites
        ("historic", _) => PlaceType::Landmark,
        // Civic / cultural amenities
        ("amenity", "university") | ("amenity", "college") => PlaceType::University,
        ("amenity", "hospital") => PlaceType::Hospital,
        ("amenity", "townhall") | ("amenity", "library") | ("amenity", "theatre")
        | ("amenity", "arts_centre") | ("amenity", "courthouse")
        | ("amenity", "place_of_worship") => PlaceType::PublicBuilding,
        _ => PlaceType::Locality,
    }
}

// ---------------------------------------------------------------------------
// Address extraction
// ---------------------------------------------------------------------------

/// Returns true for ways that are never admin/multipolygon relation members.
/// Filters highways, buildings, power lines, etc. to keep the candidate buffer small.
fn is_bulk_geometry(way: &osmpbf::Way) -> bool {
    for (k, _) in way.tags() {
        match k {
            "highway" | "building" | "building:part" | "power" | "railway" | "barrier" => {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Lightweight check: does this way have addr:street + addr:housenumber?
/// No allocations — used in pre-pass 2 for node ID collection.
fn has_addr_tags(way: &osmpbf::Way) -> bool {
    let mut has_street = false;
    let mut has_housenumber = false;
    for (k, _) in way.tags() {
        match k {
            "addr:street" => has_street = true,
            "addr:housenumber" => has_housenumber = true,
            _ => {}
        }
        if has_street && has_housenumber { return true; }
    }
    false
}

/// Lightweight check: does this way have a name + qualifying tag (not building)?
/// Highways are skipped UNLESS they have a notability signal (wikidata, wikipedia,
/// loc_name, short_name) — this preserves Avenyn / Drottninggatan / Sveavägen
/// while filtering out generic residential streets.
/// No allocations — used in pre-pass 2 for node ID collection.
fn way_qualifies(way: &osmpbf::Way) -> bool {
    let mut has_name = false;
    let mut is_highway = false;
    let mut is_building = false;
    let mut has_qualifying = false;
    let mut has_notability = false;

    for (k, v) in way.tags() {
        match k {
            "name" => has_name = true,
            "highway" => is_highway = true,
            "building" => is_building = true,
            "wikidata" | "wikipedia" | "loc_name" | "short_name"
            | "nat_name" | "reg_name" => has_notability = true,
            // `name:<lang>` (e.g. name:sv on Aleksanterinkatu in
            // bilingual Helsinki) is itself a notability signal — keep
            // pre-pass 2 in sync with scan_way's gate.
            k if k.starts_with("name:") && k.len() == 7 => has_notability = true,
            _ => {
                if !has_qualifying {
                    for &mt in MEANINGFUL_WAY_TAGS {
                        if k == mt && is_qualifying_poi(k, v) {
                            has_qualifying = true;
                            break;
                        }
                    }
                }
            }
        }
    }

    if !has_name {
        return false;
    }
    if is_highway {
        // Notable highway only — ~50–500 famous streets per country.
        return has_notability;
    }
    // Buildings qualify when they ALSO have a real POI tag (Vasamuseet =
    // building=museum + tourism=museum, the tourism wins) OR carry a
    // notability signal of their own (wikidata, wikipedia, alt/loc/short
    // name, name:<lang>) — Empire State Building, Sagrada Família, etc.
    // Pure building=yes with just a name stays out.
    if is_building {
        return has_qualifying || has_notability;
    }
    has_qualifying
}

/// Scan a way for addr:street + addr:housenumber tags (building footprints).
/// Check if a way has addr:street + addr:housenumber. Returns (street, housenumber, postcode, city).
fn scan_addr_way_tags(way: &osmpbf::Way) -> Option<(String, String, Option<String>, Option<String>)> {
    let mut street: Option<String> = None;
    let mut housenumber: Option<String> = None;
    let mut postcode: Option<String> = None;
    let mut city: Option<String> = None;

    for (k, v) in way.tags() {
        match k {
            "addr:street" => street = Some(v.to_owned()),
            "addr:housenumber" => housenumber = Some(v.to_owned()),
            "addr:postcode" => postcode = Some(v.to_owned()),
            "addr:city" => city = Some(v.to_owned()),
            _ => {}
        }
    }

    Some((street?, housenumber?, postcode, city))
}

// ---------------------------------------------------------------------------
// Address interpolation (Phase 2.5 / audit #30)
// ---------------------------------------------------------------------------

/// `addr:interpolation` ways encode "houses 1–199 odd along this segment" —
/// common for US/UK/AU rural addresses where individual house nodes aren't
/// mapped. Endpoint nodes carry the bounding `addr:housenumber` values; the
/// way fills in the gaps.
#[derive(Debug, Clone, Copy)]
pub(crate) enum InterpolationKind { Even, Odd, All }

#[derive(Debug, Clone)]
pub(crate) struct InterpolationSpec {
    pub kind: InterpolationKind,
    pub street: String,
    pub postcode: Option<String>,
    pub city: Option<String>,
}

/// Lightweight check for pre-pass 2: does this way carry `addr:interpolation=*`?
fn is_interpolation_way(way: &osmpbf::Way) -> bool {
    way.tags().any(|(k, _)| k == "addr:interpolation")
}

/// Parse the tags of an `addr:interpolation` way into an `InterpolationSpec`.
/// Requires `addr:street` so the synthesised addresses can be FST-keyed;
/// without a street the interpolation has nowhere to live in the address
/// store. Skips `alphabetic` interpolation (1A, 1B, 1C…) — uncommon and
/// not amenable to integer arithmetic.
pub(crate) fn scan_interpolation_way(way: &osmpbf::Way) -> Option<InterpolationSpec> {
    let mut interp: Option<InterpolationKind> = None;
    let mut street: Option<String> = None;
    let mut postcode: Option<String> = None;
    let mut city: Option<String> = None;
    for (k, v) in way.tags() {
        match k {
            "addr:interpolation" => {
                interp = match v {
                    "odd" => Some(InterpolationKind::Odd),
                    "even" => Some(InterpolationKind::Even),
                    "all" => Some(InterpolationKind::All),
                    _ => None, // alphabetic / numeric-special — skip
                };
            }
            "addr:street" => street = Some(v.to_owned()),
            "addr:postcode" => postcode = Some(v.to_owned()),
            "addr:city" => city = Some(v.to_owned()),
            _ => {}
        }
    }
    Some(InterpolationSpec { kind: interp?, street: street?, postcode, city })
}

/// Synthesise interpolated addresses along a way segment.
///
/// Walks the way's polyline using its segment lengths, computes the
/// fractional position for each interpolated housenumber, and emits a
/// `RawAddress` at the corresponding (lat, lon). Endpoints themselves are
/// skipped — they're already in the address pipeline as standalone nodes.
///
/// Capped at `MAX_INTERPOLATIONS` per way as a safety net against
/// pathological tags (e.g. `addr:interpolation=all` between 1 and 9999).
fn synthesise_interpolated_addresses(
    way_id: i64,
    spec: &InterpolationSpec,
    start_num: u32,
    end_num: u32,
    coords: &[(f64, f64)],
    addresses: &mut Vec<RawAddress>,
) -> usize {
    const MAX_INTERPOLATIONS: u32 = 200;

    if coords.len() < 2 || start_num == end_num {
        return 0;
    }
    let (lo_num, hi_num, reversed) = if start_num < end_num {
        (start_num, end_num, false)
    } else {
        (end_num, start_num, true)
    };
    let span = hi_num - lo_num;
    if span < 2 {
        // 1→2 or 1→3 odd has no intermediate housenumbers worth synthesising
        // beyond the endpoints we already have.
        return 0;
    }

    // Cumulative arc lengths along the polyline (cartesian, cos-corrected —
    // good enough for sub-km segments). cum[0] = 0, cum[i] = arc length up
    // to coords[i].
    let mut cum: Vec<f64> = Vec::with_capacity(coords.len());
    cum.push(0.0);
    for w in coords.windows(2) {
        let mid_lat = (w[0].0 + w[1].0) * 0.5;
        let dlat = w[1].0 - w[0].0;
        let dlon = (w[1].1 - w[0].1) * mid_lat.to_radians().cos();
        let seg_len = (dlat * dlat + dlon * dlon).sqrt();
        cum.push(cum.last().unwrap() + seg_len);
    }
    let total_len = *cum.last().unwrap();
    if total_len <= 0.0 {
        return 0;
    }

    // Direction of housenumber traversal vs. the way's node order: if the
    // way goes from end_num to start_num (i.e. reversed), the housenumber
    // increases as we walk the way backwards. We always emit in lo→hi
    // numeric order, so the fractional position needs to be flipped when
    // the way runs in the opposite direction.

    let step: u32 = match spec.kind {
        InterpolationKind::Even | InterpolationKind::Odd => 2,
        InterpolationKind::All => 1,
    };
    // First housenumber strictly between lo and hi that matches the parity
    // gate. We always skip the endpoints since they're real nodes.
    let mut n = match spec.kind {
        InterpolationKind::Even => {
            if (lo_num + 1) % 2 == 0 { lo_num + 1 } else { lo_num + 2 }
        }
        InterpolationKind::Odd => {
            if (lo_num + 1) % 2 == 1 { lo_num + 1 } else { lo_num + 2 }
        }
        InterpolationKind::All => lo_num + 1,
    };

    let mut emitted = 0usize;
    let mut count = 0u32;
    while n < hi_num && count < MAX_INTERPOLATIONS {
        let frac_lo_to_hi = (n - lo_num) as f64 / span as f64;
        let frac = if reversed { 1.0 - frac_lo_to_hi } else { frac_lo_to_hi };
        let target = frac * total_len;
        // Find the segment that contains `target`.
        let mut seg_idx = 0usize;
        for i in 1..cum.len() {
            if cum[i] >= target { seg_idx = i - 1; break; }
            // If we've reached the last segment without breaking, target
            // is at or past the end — pin to the final segment.
            seg_idx = i - 1;
        }
        let seg_start = cum[seg_idx];
        let seg_end = cum[seg_idx + 1];
        let seg_frac = if seg_end > seg_start {
            ((target - seg_start) / (seg_end - seg_start)).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let (lat0, lon0) = coords[seg_idx];
        let (lat1, lon1) = coords[seg_idx + 1];
        let lat = lat0 + (lat1 - lat0) * seg_frac;
        let lon = lon0 + (lon1 - lon0) * seg_frac;
        addresses.push(RawAddress {
            osm_id: way_id,
            street: spec.street.clone(),
            housenumber: n.to_string(),
            postcode: spec.postcode.clone(),
            city: spec.city.clone(),
            state: None,
            lat,
            lon,
        });
        emitted += 1;
        n += step;
        count += 1;
    }
    emitted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(kind: InterpolationKind) -> InterpolationSpec {
        InterpolationSpec {
            kind,
            street: "Main St".to_owned(),
            postcode: Some("12345".to_owned()),
            city: Some("Springfield".to_owned()),
        }
    }

    fn straight_line() -> Vec<(f64, f64)> {
        // 1 km segment going east at 40°N
        vec![(40.0, -100.0), (40.0, -100.0 + 0.0117)]
    }

    #[test]
    fn interpolation_odd_emits_intermediate_only() {
        let mut addrs = Vec::new();
        let n = synthesise_interpolated_addresses(
            42, &spec(InterpolationKind::Odd),
            1, 11, &straight_line(), &mut addrs,
        );
        // 1 → 11 odd: intermediates are 3, 5, 7, 9
        assert_eq!(n, 4);
        let nums: Vec<&str> = addrs.iter().map(|a| a.housenumber.as_str()).collect();
        assert_eq!(nums, vec!["3", "5", "7", "9"]);
        for a in &addrs {
            assert_eq!(a.osm_id, 42);
            assert_eq!(a.street, "Main St");
            assert_eq!(a.postcode.as_deref(), Some("12345"));
            assert_eq!(a.city.as_deref(), Some("Springfield"));
        }
    }

    #[test]
    fn interpolation_even_skips_odd_endpoints() {
        let mut addrs = Vec::new();
        synthesise_interpolated_addresses(
            1, &spec(InterpolationKind::Even),
            2, 10, &straight_line(), &mut addrs,
        );
        let nums: Vec<&str> = addrs.iter().map(|a| a.housenumber.as_str()).collect();
        assert_eq!(nums, vec!["4", "6", "8"]);
    }

    #[test]
    fn interpolation_all_emits_every_intermediate() {
        let mut addrs = Vec::new();
        synthesise_interpolated_addresses(
            1, &spec(InterpolationKind::All),
            10, 14, &straight_line(), &mut addrs,
        );
        let nums: Vec<&str> = addrs.iter().map(|a| a.housenumber.as_str()).collect();
        assert_eq!(nums, vec!["11", "12", "13"]);
    }

    #[test]
    fn interpolation_handles_reversed_way_direction() {
        // start_num > end_num — way drawn from house 11 to house 1.
        let mut addrs = Vec::new();
        synthesise_interpolated_addresses(
            1, &spec(InterpolationKind::Odd),
            11, 1, &straight_line(), &mut addrs,
        );
        let nums: Vec<&str> = addrs.iter().map(|a| a.housenumber.as_str()).collect();
        // Same numeric output, but coords should mirror the way (house 3
        // is now near the END of the polyline, not the start).
        assert_eq!(nums, vec!["3", "5", "7", "9"]);
        // House 3 should be near end of way (largest lon since way goes east).
        let h3 = addrs.iter().find(|a| a.housenumber == "3").unwrap();
        let h9 = addrs.iter().find(|a| a.housenumber == "9").unwrap();
        // Way goes from (40, -100) to (40, -100+0.0117). House 3 is at
        // 0.8 along the way (3 closer to the higher-numbered endpoint at
        // the start, since reversed); house 9 is at 0.2.
        assert!(h3.lon > h9.lon, "h3.lon={}, h9.lon={}", h3.lon, h9.lon);
    }

    #[test]
    fn interpolation_caps_runaway_ranges() {
        let mut addrs = Vec::new();
        // 1 → 9999 all → would emit 9997 entries; cap at 200.
        synthesise_interpolated_addresses(
            1, &spec(InterpolationKind::All),
            1, 9999, &straight_line(), &mut addrs,
        );
        assert_eq!(addrs.len(), 200);
    }

    #[test]
    fn interpolation_skips_adjacent_endpoints() {
        let mut addrs = Vec::new();
        synthesise_interpolated_addresses(
            1, &spec(InterpolationKind::Odd),
            1, 3, &straight_line(), &mut addrs,
        );
        // Span < 2 → no intermediate houses to interpolate.
        assert_eq!(addrs.len(), 0);
    }
}

/// Extract an address from a node with addr:street + addr:housenumber tags.
/// Also returns a named place if addr:housename is present (for UK building names
/// like "Rose Cottage", "Victoria House").
fn extract_addr_node<'a>(
    id: i64,
    lat: f64,
    lon: f64,
    tags: impl Iterator<Item = (&'a str, &'a str)>,
) -> (Option<RawAddress>, Option<RawPlace>) {
    let mut street: Option<String> = None;
    let mut housenumber: Option<String> = None;
    let mut postcode: Option<String> = None;
    let mut city: Option<String> = None;
    let mut housename: Option<String> = None;

    for (k, v) in tags {
        match k {
            "addr:street" => street = Some(v.to_owned()),
            "addr:housenumber" => housenumber = Some(v.to_owned()),
            "addr:postcode" => postcode = Some(v.to_owned()),
            "addr:city" => city = Some(v.to_owned()),
            "addr:housename" => housename = Some(v.to_owned()),
            _ => {}
        }
    }

    if lat.abs() > 90.0 || lon.abs() > 180.0 {
        return (None, None);
    }

    // Named building → place record (searchable by name)
    let building_place = housename
        .filter(|h| !h.is_empty() && street.is_some())
        .map(|name| {
            // Include street + city as alt_name for contextual search
            let mut alt_names = Vec::new();
            if let Some(ref s) = street {
                if let Some(ref c) = city {
                    alt_names.push(format!("{}, {}", name, s));
                    alt_names.push(format!("{}, {}, {}", name, s, c));
                } else {
                    alt_names.push(format!("{}, {}", name, s));
                }
            }
            RawPlace {
                osm_id: id,
                osm_type: OsmType::Node,
                name,
                name_intl: vec![],
                alt_names,
                old_names: vec![],
                coord: Coord::new(lat, lon),
                place_type: PlaceType::Locality,
                admin_level: None,
                country_code: None,
                admin1: None,
                admin2: None,
                population: None,
                extratags: vec![],
                wikidata: None,
                class: Some("building".to_owned()),
                class_value: Some("yes".to_owned()),
                bbox: None,
            }
        });

    // Standard address (requires street + housenumber)
    let address = match (street, housenumber) {
        (Some(street), Some(housenumber)) if !housenumber.is_empty() => {
            Some(RawAddress {
                osm_id: id,
                street,
                housenumber,
                postcode,
                city,
                state: None,
                lat,
                lon,
            })
        }
        _ => None,
    };

    (address, building_place)
}

// ---------------------------------------------------------------------------
// Country detection
// ---------------------------------------------------------------------------

fn detect_country(lat: f64, lon: f64) -> Option<[u8; 2]> {
    // Germany: 47.3-55.1N, 5.9-15.0E
    // Checked before Denmark — northern Germany (Schleswig-Holstein) overlaps with
    // southern Denmark bbox. Germany's bbox is the larger region, so check first
    // and let Denmark's more specific bbox override for actual Danish territory.
    if lat >= 47.3 && lat <= 55.1 && lon >= 5.9 && lon <= 15.0 {
        // But defer to Denmark for the overlap zone above 54.5N if within Danish bbox
        if lat >= 54.5 && lat <= 57.8 && lon >= 8.0 && lon <= 15.2 {
            // This is the DE/DK overlap zone — use 54.8N as the boundary
            // (approximate Flensburg/border latitude)
            if lat >= 54.8 {
                return Some(*b"DK");
            }
        }
        return Some(*b"DE");
    }
    // Denmark mainland: 54.5-57.8N, 8.0-15.2E (includes Bornholm)
    // Checked before Sweden — the bboxes overlap in the Øresund region
    if lat >= 54.5 && lat <= 57.8 && lon >= 8.0 && lon <= 15.2 {
        return Some(*b"DK");
    }
    // Faroe Islands: 61.3-62.4N, -7.7--6.2E
    if lat >= 61.3 && lat <= 62.4 && lon >= -7.7 && lon <= -6.2 {
        return Some(*b"DK");
    }
    // Greenland: 59.7-83.7N, -73.0--11.3E
    if lat >= 59.7 && lat <= 83.7 && lon >= -73.0 && lon <= -11.3 {
        return Some(*b"DK");
    }
    // Finland: 59.7-70.1N, 19.5-31.6E
    // Checked before Sweden/Norway — overlaps with both in northern Fennoscandia
    if lat >= 59.7 && lat <= 70.1 && lon >= 19.5 && lon <= 31.6 {
        return Some(*b"FI");
    }
    // Sweden: 55.0-69.5N, 10.5-24.5E
    if lat >= 55.0 && lat <= 69.5 && lon >= 10.5 && lon <= 24.5 {
        return Some(*b"SE");
    }
    // Norway mainland: 57.5-71.5N, 4.0-31.5E
    if lat >= 57.5 && lat <= 71.5 && lon >= 4.0 && lon <= 31.5 {
        return Some(*b"NO");
    }
    // Svalbard: 74-81N, 10-35E
    if lat >= 74.0 && lat <= 81.0 && lon >= 10.0 && lon <= 35.0 {
        return Some(*b"NO");
    }
    // Netherlands: 50.75-53.47N, 3.36-7.21E
    if lat >= 50.75 && lat <= 53.47 && lon >= 3.36 && lon <= 7.21 {
        return Some(*b"NL");
    }
    // Belgium: 49.50-51.50N, 2.55-6.41E
    if lat >= 49.50 && lat <= 51.50 && lon >= 2.55 && lon <= 6.41 {
        return Some(*b"BE");
    }
    // Switzerland + Liechtenstein: 45.82-47.81N, 5.96-10.49E
    if lat >= 45.82 && lat <= 47.81 && lon >= 5.96 && lon <= 10.49 {
        return Some(*b"CH");
    }
    // Austria: 46.37-49.02N, 9.53-17.16E
    if lat >= 46.37 && lat <= 49.02 && lon >= 9.53 && lon <= 17.16 {
        return Some(*b"AT");
    }
    // Czech Republic: 48.55-51.06N, 12.09-18.86E
    if lat >= 48.55 && lat <= 51.06 && lon >= 12.09 && lon <= 18.86 {
        return Some(*b"CZ");
    }
    // Poland: 49.00-54.84N, 14.12-24.15E
    if lat >= 49.00 && lat <= 54.84 && lon >= 14.12 && lon <= 24.15 {
        return Some(*b"PL");
    }
    // Estonia: 57.51-59.68N, 21.76-28.21E
    if lat >= 57.51 && lat <= 59.68 && lon >= 21.76 && lon <= 28.21 {
        return Some(*b"EE");
    }
    // Latvia: 55.67-58.09N, 20.97-28.24E
    if lat >= 55.67 && lat <= 58.09 && lon >= 20.97 && lon <= 28.24 {
        return Some(*b"LV");
    }
    // Lithuania: 53.90-56.45N, 20.93-26.84E
    if lat >= 53.90 && lat <= 56.45 && lon >= 20.93 && lon <= 26.84 {
        return Some(*b"LT");
    }
    // France metro: 41.33-51.12N, 5.14W-9.56E
    if lat >= 41.33 && lat <= 51.12 && lon >= -5.14 && lon <= 9.56 {
        return Some(*b"FR");
    }
    // France overseas: Guadeloupe
    if lat >= 15.83 && lat <= 16.53 && lon >= -61.80 && lon <= -61.00 {
        return Some(*b"FR");
    }
    // France overseas: Martinique
    if lat >= 14.39 && lat <= 14.88 && lon >= -61.23 && lon <= -60.81 {
        return Some(*b"FR");
    }
    // France overseas: Réunion
    if lat >= -21.39 && lat <= -20.87 && lon >= 55.22 && lon <= 55.84 {
        return Some(*b"FR");
    }
    // Great Britain + Northern Ireland: 49.8-60.9N, 8.7W-1.8E
    if lat >= 49.8 && lat <= 60.9 && lon >= -8.7 && lon <= 1.8 {
        return Some(*b"GB");
    }
    // Japan: 24.0-45.5N, 122.9-153.0E
    if lat >= 24.0 && lat <= 45.5 && lon >= 122.9 && lon <= 153.0 {
        return Some(*b"JP");
    }
    // South Korea: 33.11-38.62N, 124.60-131.87E
    if lat >= 33.11 && lat <= 38.62 && lon >= 124.60 && lon <= 131.87 {
        return Some(*b"KR");
    }
    // Brazil: 33.75S-5.27N, 73.99W-34.79W
    if lat >= -33.75 && lat <= 5.27 && lon >= -73.99 && lon <= -34.79 {
        return Some(*b"BR");
    }
    // United States CONUS: 24.5-49.4N, 125.0W-66.9W
    if lat >= 24.5 && lat <= 49.4 && lon >= -125.0 && lon <= -66.9 {
        return Some(*b"US");
    }
    // Alaska: 51.2-71.4N, 172.4E-129.9W (crosses antimeridian)
    if lat >= 51.2 && lat <= 71.4 && (lon >= 172.4 || lon <= -129.9) {
        return Some(*b"US");
    }
    // Hawaii: 18.9-28.4N, 178.3W-154.8W
    if lat >= 18.9 && lat <= 28.4 && lon >= -178.3 && lon <= -154.8 {
        return Some(*b"US");
    }
    // Puerto Rico + USVI: 17.6-18.6N, 67.3W-64.5W
    if lat >= 17.6 && lat <= 18.6 && lon >= -67.3 && lon <= -64.5 {
        return Some(*b"US");
    }
    // New Zealand: 47S-34S, 166E-179E (North + South Island + Stewart Island)
    if lat >= -47.5 && lat <= -34.0 && lon >= 166.0 && lon <= 179.0 {
        return Some(*b"NZ");
    }
    // Australia mainland + Tasmania: 44S-10S, 112E-154E
    if lat >= -44.0 && lat <= -10.0 && lon >= 112.0 && lon <= 154.0 {
        return Some(*b"AU");
    }
    // Canada: 41.7-83.1N, 141.0W-52.6W
    // Checked after US — the southern border overlaps with northern US.
    // Use 49.0N as boundary (49th parallel) except for southern Ontario/Quebec.
    if lat >= 49.0 && lat <= 83.5 && lon >= -141.0 && lon <= -52.0 {
        return Some(*b"CA");
    }
    // Southern Ontario/Quebec peninsula: 41.7-49.0N, 95.0W-59.0W
    // (below 49th parallel but clearly Canadian territory)
    if lat >= 41.7 && lat <= 49.0 && lon >= -95.0 && lon <= -59.0 {
        // This overlaps with US — Canada only extends south of 49th in Ontario/Quebec
        // and parts of Manitoba. For OSM extraction this mostly doesn't matter
        // since we process by PBF file, not by coordinate.
        // Skip this — let the US claim the overlap zone for OSM extraction.
        // Canadian addresses will come from NAR regardless.
    }
    // ── Batch 4: Photon-only countries ──
    // Andorra: 42.43-42.66N, 1.41-1.79E
    if lat >= 42.43 && lat <= 42.66 && lon >= 1.41 && lon <= 1.79 {
        return Some(*b"AD");
    }
    // UAE: 22.63-26.08N, 51.58-56.38E
    if lat >= 22.63 && lat <= 26.08 && lon >= 51.58 && lon <= 56.38 {
        return Some(*b"AE");
    }
    // Afghanistan: 29.38-38.49N, 60.50-74.89E
    if lat >= 29.38 && lat <= 38.49 && lon >= 60.50 && lon <= 74.89 {
        return Some(*b"AF");
    }
    // Antigua and Barbuda: 16.94-17.73N, -62.35--61.66E
    if lat >= 16.94 && lat <= 17.73 && lon >= -62.35 && lon <= -61.66 {
        return Some(*b"AG");
    }
    // Albania: 39.64-42.66N, 19.26-21.06E
    if lat >= 39.64 && lat <= 42.66 && lon >= 19.26 && lon <= 21.06 {
        return Some(*b"AL");
    }
    // Armenia: 38.84-41.30N, 43.45-46.63E
    if lat >= 38.84 && lat <= 41.30 && lon >= 43.45 && lon <= 46.63 {
        return Some(*b"AM");
    }
    // Angola: -18.04-5.39N, 11.64-24.08E
    if lat >= -18.04 && lat <= 5.39 && lon >= 11.64 && lon <= 24.08 {
        return Some(*b"AO");
    }
    // Argentina: -55.06--21.78N, -73.56--53.64E
    if lat >= -55.06 && lat <= -21.78 && lon >= -73.56 && lon <= -53.64 {
        return Some(*b"AR");
    }
    // Azerbaijan: 38.39-41.91N, 44.77-50.63E
    if lat >= 38.39 && lat <= 41.91 && lon >= 44.77 && lon <= 50.63 {
        return Some(*b"AZ");
    }
    // Bosnia and Herzegovina: 42.56-45.28N, 15.72-19.62E
    if lat >= 42.56 && lat <= 45.28 && lon >= 15.72 && lon <= 19.62 {
        return Some(*b"BA");
    }
    // Barbados: 13.04-13.34N, -59.65--59.42E
    if lat >= 13.04 && lat <= 13.34 && lon >= -59.65 && lon <= -59.42 {
        return Some(*b"BB");
    }
    // Bangladesh: 20.74-26.63N, 88.01-92.67E
    if lat >= 20.74 && lat <= 26.63 && lon >= 88.01 && lon <= 92.67 {
        return Some(*b"BD");
    }
    // Burkina Faso: 9.39-15.08N, -5.52-2.41E
    if lat >= 9.39 && lat <= 15.08 && lon >= -5.52 && lon <= 2.41 {
        return Some(*b"BF");
    }
    // Bulgaria: 41.24-44.22N, 22.36-28.61E
    if lat >= 41.24 && lat <= 44.22 && lon >= 22.36 && lon <= 28.61 {
        return Some(*b"BG");
    }
    // Bahrain: 25.79-26.29N, 50.45-50.82E
    if lat >= 25.79 && lat <= 26.29 && lon >= 50.45 && lon <= 50.82 {
        return Some(*b"BH");
    }
    // Burundi: -4.47--2.31N, 28.99-30.85E
    if lat >= -4.47 && lat <= -2.31 && lon >= 28.99 && lon <= 30.85 {
        return Some(*b"BI");
    }
    // Benin: 6.14-12.41N, 0.77-3.84E
    if lat >= 6.14 && lat <= 12.41 && lon >= 0.77 && lon <= 3.84 {
        return Some(*b"BJ");
    }
    // Bermuda: 32.25-32.39N, -64.89--64.64E
    if lat >= 32.25 && lat <= 32.39 && lon >= -64.89 && lon <= -64.64 {
        return Some(*b"BM");
    }
    // Bolivia: -22.90--9.68N, -69.64--57.45E
    if lat >= -22.90 && lat <= -9.68 && lon >= -69.64 && lon <= -57.45 {
        return Some(*b"BO");
    }
    // Bahamas: 20.91-27.26N, -80.48--72.71E
    if lat >= 20.91 && lat <= 27.26 && lon >= -80.48 && lon <= -72.71 {
        return Some(*b"BS");
    }
    // Bhutan: 26.70-28.33N, 88.75-92.12E
    if lat >= 26.70 && lat <= 28.33 && lon >= 88.75 && lon <= 92.12 {
        return Some(*b"BT");
    }
    // Botswana: -26.91--17.78N, 19.99-29.37E
    if lat >= -26.91 && lat <= -17.78 && lon >= 19.99 && lon <= 29.37 {
        return Some(*b"BW");
    }
    // Belarus: 51.26-56.17N, 23.18-32.78E
    if lat >= 51.26 && lat <= 56.17 && lon >= 23.18 && lon <= 32.78 {
        return Some(*b"BY");
    }
    // Belize: 15.89-18.50N, -89.22--87.49E
    if lat >= 15.89 && lat <= 18.50 && lon >= -89.22 && lon <= -87.49 {
        return Some(*b"BZ");
    }
    // DR Congo: -13.46-5.39N, 12.18-31.31E
    if lat >= -13.46 && lat <= 5.39 && lon >= 12.18 && lon <= 31.31 {
        return Some(*b"CD");
    }
    // Central African Rep.: 2.22-11.01N, 14.42-27.46E
    if lat >= 2.22 && lat <= 11.01 && lon >= 14.42 && lon <= 27.46 {
        return Some(*b"CF");
    }
    // Rep. Congo: -5.03-3.71N, 11.20-18.65E
    if lat >= -5.03 && lat <= 3.71 && lon >= 11.20 && lon <= 18.65 {
        return Some(*b"CG");
    }
    // Cote d'Ivoire: 4.36-10.74N, -8.60--2.49E
    if lat >= 4.36 && lat <= 10.74 && lon >= -8.60 && lon <= -2.49 {
        return Some(*b"CI");
    }
    // Chile: -56.00--17.50N, -75.70--66.42E
    if lat >= -56.00 && lat <= -17.50 && lon >= -75.70 && lon <= -66.42 {
        return Some(*b"CL");
    }
    // Cameroon: 1.65-13.08N, 8.49-16.19E
    if lat >= 1.65 && lat <= 13.08 && lon >= 8.49 && lon <= 16.19 {
        return Some(*b"CM");
    }
    // Colombia: -4.23-13.39N, -79.00--66.85E
    if lat >= -4.23 && lat <= 13.39 && lon >= -79.00 && lon <= -66.85 {
        return Some(*b"CO");
    }
    // Costa Rica: 5.50-11.22N, -85.95--82.55E
    if lat >= 5.50 && lat <= 11.22 && lon >= -85.95 && lon <= -82.55 {
        return Some(*b"CR");
    }
    // Cuba: 19.83-23.27N, -85.00--74.13E
    if lat >= 19.83 && lat <= 23.27 && lon >= -85.00 && lon <= -74.13 {
        return Some(*b"CU");
    }
    // Cape Verde: 14.80-17.21N, -25.36--22.66E
    if lat >= 14.80 && lat <= 17.21 && lon >= -25.36 && lon <= -22.66 {
        return Some(*b"CV");
    }
    // Cyprus: 34.57-35.70N, 32.27-34.60E
    if lat >= 34.57 && lat <= 35.70 && lon >= 32.27 && lon <= 34.60 {
        return Some(*b"CY");
    }
    // Djibouti: 10.93-12.71N, 41.77-43.42E
    if lat >= 10.93 && lat <= 12.71 && lon >= 41.77 && lon <= 43.42 {
        return Some(*b"DJ");
    }
    // Dominica: 15.20-15.65N, -61.48--61.24E
    if lat >= 15.20 && lat <= 15.65 && lon >= -61.48 && lon <= -61.24 {
        return Some(*b"DM");
    }
    // Dominican Republic: 17.47-19.93N, -72.01--68.32E
    if lat >= 17.47 && lat <= 19.93 && lon >= -72.01 && lon <= -68.32 {
        return Some(*b"DO");
    }
    // Algeria: 18.97-37.09N, -8.67-11.98E
    if lat >= 18.97 && lat <= 37.09 && lon >= -8.67 && lon <= 11.98 {
        return Some(*b"DZ");
    }
    // Ecuador: -5.01-1.68N, -81.08--75.19E
    if lat >= -5.01 && lat <= 1.68 && lon >= -81.08 && lon <= -75.19 {
        return Some(*b"EC");
    }
    // Egypt: 22.00-31.67N, 24.70-36.90E
    if lat >= 22.00 && lat <= 31.67 && lon >= 24.70 && lon <= 36.90 {
        return Some(*b"EG");
    }
    // Eritrea: 12.36-18.00N, 36.44-43.13E
    if lat >= 12.36 && lat <= 18.00 && lon >= 36.44 && lon <= 43.13 {
        return Some(*b"ER");
    }
    // Spain: 35.95-43.79N, -9.30-4.33E
    if lat >= 35.95 && lat <= 43.79 && lon >= -9.30 && lon <= 4.33 {
        return Some(*b"ES");
    }
    // Ethiopia: 3.40-14.89N, 32.99-47.99E
    if lat >= 3.40 && lat <= 14.89 && lon >= 32.99 && lon <= 47.99 {
        return Some(*b"ET");
    }
    // Gabon: -3.98-2.32N, 8.70-14.50E
    if lat >= -3.98 && lat <= 2.32 && lon >= 8.70 && lon <= 14.50 {
        return Some(*b"GA");
    }
    // Grenada: 11.99-12.32N, -61.80--61.58E
    if lat >= 11.99 && lat <= 12.32 && lon >= -61.80 && lon <= -61.58 {
        return Some(*b"GD");
    }
    // Georgia: 41.05-43.59N, 40.01-46.74E
    if lat >= 41.05 && lat <= 43.59 && lon >= 40.01 && lon <= 46.74 {
        return Some(*b"GE");
    }
    // Ghana: 4.74-11.17N, -3.26-1.19E
    if lat >= 4.74 && lat <= 11.17 && lon >= -3.26 && lon <= 1.19 {
        return Some(*b"GH");
    }
    // Gambia: 13.06-13.83N, -16.82--13.80E
    if lat >= 13.06 && lat <= 13.83 && lon >= -16.82 && lon <= -13.80 {
        return Some(*b"GM");
    }
    // Guinea: 7.19-12.67N, -15.08--7.64E
    if lat >= 7.19 && lat <= 12.67 && lon >= -15.08 && lon <= -7.64 {
        return Some(*b"GN");
    }
    // Equatorial Guinea: 0.92-3.79N, 5.62-11.34E
    if lat >= 0.92 && lat <= 3.79 && lon >= 5.62 && lon <= 11.34 {
        return Some(*b"GQ");
    }
    // Greece: 34.80-41.75N, 19.37-29.65E
    if lat >= 34.80 && lat <= 41.75 && lon >= 19.37 && lon <= 29.65 {
        return Some(*b"GR");
    }
    // Guatemala: 13.74-17.82N, -92.23--88.22E
    if lat >= 13.74 && lat <= 17.82 && lon >= -92.23 && lon <= -88.22 {
        return Some(*b"GT");
    }
    // Guinea-Bissau: 10.86-12.69N, -16.71--13.64E
    if lat >= 10.86 && lat <= 12.69 && lon >= -16.71 && lon <= -13.64 {
        return Some(*b"GW");
    }
    // Guyana: 1.17-8.56N, -61.39--56.48E
    if lat >= 1.17 && lat <= 8.56 && lon >= -61.39 && lon <= -56.48 {
        return Some(*b"GY");
    }
    // Honduras: 12.98-16.51N, -89.35--83.15E
    if lat >= 12.98 && lat <= 16.51 && lon >= -89.35 && lon <= -83.15 {
        return Some(*b"HN");
    }
    // Croatia: 42.39-46.55N, 13.49-19.45E
    if lat >= 42.39 && lat <= 46.55 && lon >= 13.49 && lon <= 19.45 {
        return Some(*b"HR");
    }
    // Haiti: 18.02-20.09N, -74.48--71.62E
    if lat >= 18.02 && lat <= 20.09 && lon >= -74.48 && lon <= -71.62 {
        return Some(*b"HT");
    }
    // Hungary: 45.74-48.59N, 16.11-22.90E
    if lat >= 45.74 && lat <= 48.59 && lon >= 16.11 && lon <= 22.90 {
        return Some(*b"HU");
    }
    // Ireland: 51.42-55.39N, -10.48--5.99E
    if lat >= 51.42 && lat <= 55.39 && lon >= -10.48 && lon <= -5.99 {
        return Some(*b"IE");
    }
    // Israel: 29.49-33.33N, 34.27-35.90E
    if lat >= 29.49 && lat <= 33.33 && lon >= 34.27 && lon <= 35.90 {
        return Some(*b"IL");
    }
    // India: 6.75-35.50N, 68.19-97.40E
    if lat >= 6.75 && lat <= 35.50 && lon >= 68.19 && lon <= 97.40 {
        return Some(*b"IN");
    }
    // Iraq: 29.06-37.38N, 38.79-48.57E
    if lat >= 29.06 && lat <= 37.38 && lon >= 38.79 && lon <= 48.57 {
        return Some(*b"IQ");
    }
    // Iran: 25.06-39.78N, 44.05-63.32E
    if lat >= 25.06 && lat <= 39.78 && lon >= 44.05 && lon <= 63.32 {
        return Some(*b"IR");
    }
    // Iceland: 63.30-66.56N, -24.55--13.50E
    if lat >= 63.30 && lat <= 66.56 && lon >= -24.55 && lon <= -13.50 {
        return Some(*b"IS");
    }
    // Italy: 35.49-47.09N, 6.63-18.52E
    if lat >= 35.49 && lat <= 47.09 && lon >= 6.63 && lon <= 18.52 {
        return Some(*b"IT");
    }
    // Jamaica: 17.70-18.53N, -78.37--76.18E
    if lat >= 17.70 && lat <= 18.53 && lon >= -78.37 && lon <= -76.18 {
        return Some(*b"JM");
    }
    // Jordan: 29.19-33.37N, 34.96-39.30E
    if lat >= 29.19 && lat <= 33.37 && lon >= 34.96 && lon <= 39.30 {
        return Some(*b"JO");
    }
    // Kenya: -4.68-5.51N, 33.91-41.91E
    if lat >= -4.68 && lat <= 5.51 && lon >= 33.91 && lon <= 41.91 {
        return Some(*b"KE");
    }
    // Comoros: -12.42--11.36N, 43.22-44.54E
    if lat >= -12.42 && lat <= -11.36 && lon >= 43.22 && lon <= 44.54 {
        return Some(*b"KM");
    }
    // Saint Kitts and Nevis: 17.09-17.42N, -62.87--62.54E
    if lat >= 17.09 && lat <= 17.42 && lon >= -62.87 && lon <= -62.54 {
        return Some(*b"KN");
    }
    // Kuwait: 28.52-30.10N, 46.55-48.43E
    if lat >= 28.52 && lat <= 30.10 && lon >= 46.55 && lon <= 48.43 {
        return Some(*b"KW");
    }
    // Lebanon: 33.05-34.69N, 35.10-36.63E
    if lat >= 33.05 && lat <= 34.69 && lon >= 35.10 && lon <= 36.63 {
        return Some(*b"LB");
    }
    // Saint Lucia: 13.71-14.11N, -61.08--60.87E
    if lat >= 13.71 && lat <= 14.11 && lon >= -61.08 && lon <= -60.87 {
        return Some(*b"LC");
    }
    // Sri Lanka: 5.92-9.84N, 79.65-81.88E
    if lat >= 5.92 && lat <= 9.84 && lon >= 79.65 && lon <= 81.88 {
        return Some(*b"LK");
    }
    // Liberia: 4.35-8.55N, -11.49--7.37E
    if lat >= 4.35 && lat <= 8.55 && lon >= -11.49 && lon <= -7.37 {
        return Some(*b"LR");
    }
    // Lesotho: -30.67--28.57N, 27.01-29.46E
    if lat >= -30.67 && lat <= -28.57 && lon >= 27.01 && lon <= 29.46 {
        return Some(*b"LS");
    }
    // Luxembourg: 49.45-50.18N, 5.73-6.53E
    if lat >= 49.45 && lat <= 50.18 && lon >= 5.73 && lon <= 6.53 {
        return Some(*b"LU");
    }
    // Libya: 19.50-33.17N, 9.39-25.15E
    if lat >= 19.50 && lat <= 33.17 && lon >= 9.39 && lon <= 25.15 {
        return Some(*b"LY");
    }
    // Morocco: 27.67-35.92N, -13.17--1.01E
    if lat >= 27.67 && lat <= 35.92 && lon >= -13.17 && lon <= -1.01 {
        return Some(*b"MA");
    }
    // Monaco: 43.72-43.75N, 7.41-7.44E
    if lat >= 43.72 && lat <= 43.75 && lon >= 7.41 && lon <= 7.44 {
        return Some(*b"MC");
    }
    // Moldova: 45.47-48.49N, 26.62-30.16E
    if lat >= 45.47 && lat <= 48.49 && lon >= 26.62 && lon <= 30.16 {
        return Some(*b"MD");
    }
    // Montenegro: 41.85-43.56N, 18.43-20.36E
    if lat >= 41.85 && lat <= 43.56 && lon >= 18.43 && lon <= 20.36 {
        return Some(*b"ME");
    }
    // Madagascar: -25.61--11.95N, 43.19-50.48E
    if lat >= -25.61 && lat <= -11.95 && lon >= 43.19 && lon <= 50.48 {
        return Some(*b"MG");
    }
    // North Macedonia: 40.85-42.37N, 20.45-23.04E
    if lat >= 40.85 && lat <= 42.37 && lon >= 20.45 && lon <= 23.04 {
        return Some(*b"MK");
    }
    // Mali: 10.16-25.00N, -12.24-4.27E
    if lat >= 10.16 && lat <= 25.00 && lon >= -12.24 && lon <= 4.27 {
        return Some(*b"ML");
    }
    // Mauritania: 14.72-27.30N, -17.07--4.83E
    if lat >= 14.72 && lat <= 27.30 && lon >= -17.07 && lon <= -4.83 {
        return Some(*b"MR");
    }
    // Malta: 35.81-36.08N, 14.18-14.58E
    if lat >= 35.81 && lat <= 36.08 && lon >= 14.18 && lon <= 14.58 {
        return Some(*b"MT");
    }
    // Mauritius: -20.53--19.97N, 57.30-57.81E
    if lat >= -20.53 && lat <= -19.97 && lon >= 57.30 && lon <= 57.81 {
        return Some(*b"MU");
    }
    // Maldives: -0.69-7.11N, 72.64-73.76E
    if lat >= -0.69 && lat <= 7.11 && lon >= 72.64 && lon <= 73.76 {
        return Some(*b"MV");
    }
    // Malawi: -17.13--9.37N, 32.67-35.92E
    if lat >= -17.13 && lat <= -9.37 && lon >= 32.67 && lon <= 35.92 {
        return Some(*b"MW");
    }
    // Mexico: 14.53-32.72N, -118.60--86.70E
    if lat >= 14.53 && lat <= 32.72 && lon >= -118.60 && lon <= -86.70 {
        return Some(*b"MX");
    }
    // Mozambique: -26.87--10.47N, 30.21-40.84E
    if lat >= -26.87 && lat <= -10.47 && lon >= 30.21 && lon <= 40.84 {
        return Some(*b"MZ");
    }
    // Namibia: -28.97--16.96N, 11.72-25.26E
    if lat >= -28.97 && lat <= -16.96 && lon >= 11.72 && lon <= 25.26 {
        return Some(*b"NA");
    }
    // Niger: 11.69-23.53N, 0.17-15.99E
    if lat >= 11.69 && lat <= 23.53 && lon >= 0.17 && lon <= 15.99 {
        return Some(*b"NE");
    }
    // Nigeria: 4.27-13.89N, 2.69-14.68E
    if lat >= 4.27 && lat <= 13.89 && lon >= 2.69 && lon <= 14.68 {
        return Some(*b"NG");
    }
    // Nicaragua: 10.71-15.03N, -87.69--82.73E
    if lat >= 10.71 && lat <= 15.03 && lon >= -87.69 && lon <= -82.73 {
        return Some(*b"NI");
    }
    // Nepal: 26.36-30.45N, 80.06-88.20E
    if lat >= 26.36 && lat <= 30.45 && lon >= 80.06 && lon <= 88.20 {
        return Some(*b"NP");
    }
    // Oman: 16.65-26.39N, 52.00-59.84E
    if lat >= 16.65 && lat <= 26.39 && lon >= 52.00 && lon <= 59.84 {
        return Some(*b"OM");
    }
    // Panama: 7.20-9.65N, -83.05--77.17E
    if lat >= 7.20 && lat <= 9.65 && lon >= -83.05 && lon <= -77.17 {
        return Some(*b"PA");
    }
    // Peru: -18.35--0.04N, -81.33--68.65E
    if lat >= -18.35 && lat <= -0.04 && lon >= -81.33 && lon <= -68.65 {
        return Some(*b"PE");
    }
    // Pakistan: 23.69-37.08N, 60.87-77.84E
    if lat >= 23.69 && lat <= 37.08 && lon >= 60.87 && lon <= 77.84 {
        return Some(*b"PK");
    }
    // Palestine: 31.22-32.55N, 34.22-35.57E
    if lat >= 31.22 && lat <= 32.55 && lon >= 34.22 && lon <= 35.57 {
        return Some(*b"PS");
    }
    // Portugal: 36.96-42.15N, -9.53--6.19E
    if lat >= 36.96 && lat <= 42.15 && lon >= -9.53 && lon <= -6.19 {
        return Some(*b"PT");
    }
    // Paraguay: -27.59--19.29N, -62.65--54.26E
    if lat >= -27.59 && lat <= -19.29 && lon >= -62.65 && lon <= -54.26 {
        return Some(*b"PY");
    }
    // Qatar: 24.47-26.18N, 50.75-51.64E
    if lat >= 24.47 && lat <= 26.18 && lon >= 50.75 && lon <= 51.64 {
        return Some(*b"QA");
    }
    // Romania: 43.62-48.27N, 20.26-29.69E
    if lat >= 43.62 && lat <= 48.27 && lon >= 20.26 && lon <= 29.69 {
        return Some(*b"RO");
    }
    // Serbia: 42.23-46.19N, 18.82-23.01E
    if lat >= 42.23 && lat <= 46.19 && lon >= 18.82 && lon <= 23.01 {
        return Some(*b"RS");
    }
    // Russia: 41.19-81.86N, 19.64-180.0E
    if lat >= 41.19 && lat <= 81.86 && lon >= 19.64 && lon <= 180.0 {
        return Some(*b"RU");
    }
    // Rwanda: -2.84--1.05N, 28.86-30.90E
    if lat >= -2.84 && lat <= -1.05 && lon >= 28.86 && lon <= 30.90 {
        return Some(*b"RW");
    }
    // Saudi Arabia: 16.38-32.15N, 34.57-55.67E
    if lat >= 16.38 && lat <= 32.15 && lon >= 34.57 && lon <= 55.67 {
        return Some(*b"SA");
    }
    // Seychelles: -9.76--4.28N, 46.20-56.30E
    if lat >= -9.76 && lat <= -4.28 && lon >= 46.20 && lon <= 56.30 {
        return Some(*b"SC");
    }
    // Sudan: 8.68-22.23N, 21.81-38.61E
    if lat >= 8.68 && lat <= 22.23 && lon >= 21.81 && lon <= 38.61 {
        return Some(*b"SD");
    }
    // Slovenia: 45.42-46.88N, 13.38-16.61E
    if lat >= 45.42 && lat <= 46.88 && lon >= 13.38 && lon <= 16.61 {
        return Some(*b"SI");
    }
    // Slovakia: 47.73-49.61N, 16.83-22.57E
    if lat >= 47.73 && lat <= 49.61 && lon >= 16.83 && lon <= 22.57 {
        return Some(*b"SK");
    }
    // Sierra Leone: 6.93-10.00N, -13.30--10.28E
    if lat >= 6.93 && lat <= 10.00 && lon >= -13.30 && lon <= -10.28 {
        return Some(*b"SL");
    }
    // San Marino: 43.89-43.99N, 12.40-12.52E
    if lat >= 43.89 && lat <= 43.99 && lon >= 12.40 && lon <= 12.52 {
        return Some(*b"SM");
    }
    // Senegal: 12.31-16.69N, -17.54--11.36E
    if lat >= 12.31 && lat <= 16.69 && lon >= -17.54 && lon <= -11.36 {
        return Some(*b"SN");
    }
    // Somalia: -1.68-11.99N, 40.99-51.41E
    if lat >= -1.68 && lat <= 11.99 && lon >= 40.99 && lon <= 51.41 {
        return Some(*b"SO");
    }
    // Suriname: 1.83-6.01N, -58.07--53.98E
    if lat >= 1.83 && lat <= 6.01 && lon >= -58.07 && lon <= -53.98 {
        return Some(*b"SR");
    }
    // South Sudan: 3.49-12.24N, 23.44-35.95E
    if lat >= 3.49 && lat <= 12.24 && lon >= 23.44 && lon <= 35.95 {
        return Some(*b"SS");
    }
    // Sao Tome: 0.02-1.70N, 6.47-7.47E
    if lat >= 0.02 && lat <= 1.70 && lon >= 6.47 && lon <= 7.47 {
        return Some(*b"ST");
    }
    // El Salvador: 13.15-14.45N, -90.13--87.68E
    if lat >= 13.15 && lat <= 14.45 && lon >= -90.13 && lon <= -87.68 {
        return Some(*b"SV");
    }
    // Syria: 32.31-37.32N, 35.73-42.38E
    if lat >= 32.31 && lat <= 37.32 && lon >= 35.73 && lon <= 42.38 {
        return Some(*b"SY");
    }
    // Eswatini: -27.32--25.72N, 30.79-32.14E
    if lat >= -27.32 && lat <= -25.72 && lon >= 30.79 && lon <= 32.14 {
        return Some(*b"SZ");
    }
    // Chad: 7.44-23.45N, 13.47-24.00E
    if lat >= 7.44 && lat <= 23.45 && lon >= 13.47 && lon <= 24.00 {
        return Some(*b"TD");
    }
    // Togo: 6.10-11.14N, -0.15-1.81E
    if lat >= 6.10 && lat <= 11.14 && lon >= -0.15 && lon <= 1.81 {
        return Some(*b"TG");
    }
    // Tunisia: 30.23-37.54N, 7.52-11.60E
    if lat >= 30.23 && lat <= 37.54 && lon >= 7.52 && lon <= 11.60 {
        return Some(*b"TN");
    }
    // Turkey: 35.81-42.11N, 25.66-44.82E
    if lat >= 35.81 && lat <= 42.11 && lon >= 25.66 && lon <= 44.82 {
        return Some(*b"TR");
    }
    // Trinidad and Tobago: 10.04-11.36N, -61.93--60.52E
    if lat >= 10.04 && lat <= 11.36 && lon >= -61.93 && lon <= -60.52 {
        return Some(*b"TT");
    }
    // Tanzania: -11.75--0.99N, 29.33-40.44E
    if lat >= -11.75 && lat <= -0.99 && lon >= 29.33 && lon <= 40.44 {
        return Some(*b"TZ");
    }
    // Ukraine: 44.39-52.38N, 22.13-40.23E
    if lat >= 44.39 && lat <= 52.38 && lon >= 22.13 && lon <= 40.23 {
        return Some(*b"UA");
    }
    // Uganda: -1.48-4.23N, 29.57-35.04E
    if lat >= -1.48 && lat <= 4.23 && lon >= 29.57 && lon <= 35.04 {
        return Some(*b"UG");
    }
    // Uruguay: -35.03--30.09N, -58.44--53.09E
    if lat >= -35.03 && lat <= -30.09 && lon >= -58.44 && lon <= -53.09 {
        return Some(*b"UY");
    }
    // Vatican City: 41.90-41.91N, 12.45-12.46E
    if lat >= 41.90 && lat <= 41.91 && lon >= 12.45 && lon <= 12.46 {
        return Some(*b"VA");
    }
    // Saint Vincent and the Grenadines: 12.58-13.38N, -61.46--61.11E
    if lat >= 12.58 && lat <= 13.38 && lon >= -61.46 && lon <= -61.11 {
        return Some(*b"VC");
    }
    // Venezuela: 0.65-12.20N, -73.38--59.80E
    if lat >= 0.65 && lat <= 12.20 && lon >= -73.38 && lon <= -59.80 {
        return Some(*b"VE");
    }
    // Kosovo: 41.86-43.27N, 20.01-21.79E
    if lat >= 41.86 && lat <= 43.27 && lon >= 20.01 && lon <= 21.79 {
        return Some(*b"XK");
    }
    // Yemen: 12.11-19.00N, 42.55-54.53E
    if lat >= 12.11 && lat <= 19.00 && lon >= 42.55 && lon <= 54.53 {
        return Some(*b"YE");
    }
    // South Africa: -34.84--22.13N, 16.45-32.89E
    if lat >= -34.84 && lat <= -22.13 && lon >= 16.45 && lon <= 32.89 {
        return Some(*b"ZA");
    }
    // Zambia: -18.08--8.22N, 21.99-33.71E
    if lat >= -18.08 && lat <= -8.22 && lon >= 21.99 && lon <= 33.71 {
        return Some(*b"ZM");
    }
    // Zimbabwe: -22.42--15.61N, 25.24-33.07E
    if lat >= -22.42 && lat <= -15.61 && lon >= 25.24 && lon <= 33.07 {
        return Some(*b"ZW");
    }
    // UAE: 22.63-26.08N, 51.58-56.38E
    if lat >= 22.63 && lat <= 26.08 && lon >= 51.58 && lon <= 56.38 {
        return Some(*b"AE");
    }
    // Afghanistan: 29.38-38.49N, 60.50-74.89E
    if lat >= 29.38 && lat <= 38.49 && lon >= 60.50 && lon <= 74.89 {
        return Some(*b"AF");
    }
    // Bangladesh: 20.74-26.63N, 88.01-92.67E
    if lat >= 20.74 && lat <= 26.63 && lon >= 88.01 && lon <= 92.67 {
        return Some(*b"BD");
    }
    // Bahrain: 25.79-26.29N, 50.45-50.82E
    if lat >= 25.79 && lat <= 26.29 && lon >= 50.45 && lon <= 50.82 {
        return Some(*b"BH");
    }
    // Brunei: 4.00-5.05N, 114.00-115.37E
    if lat >= 4.00 && lat <= 5.05 && lon >= 114.00 && lon <= 115.37 {
        return Some(*b"BN");
    }
    // Bhutan: 26.70-28.33N, 88.75-92.12E
    if lat >= 26.70 && lat <= 28.33 && lon >= 88.75 && lon <= 92.12 {
        return Some(*b"BT");
    }
    // Cook Islands: -21.95--8.95N, -165.85--157.31E
    if lat >= -21.95 && lat <= -8.95 && lon >= -165.85 && lon <= -157.31 {
        return Some(*b"CK");
    }
    // China: 18.16-53.56N, 73.50-134.77E
    if lat >= 18.16 && lat <= 53.56 && lon >= 73.50 && lon <= 134.77 {
        return Some(*b"CN");
    }
    // Fiji: -20.68--12.48N, 176.00--178.00E
    if lat >= -20.68 && lat <= -12.48 && lon >= 176.00 && lon <= -178.00 {
        return Some(*b"FJ");
    }
    // Micronesia: 1.03-10.09N, 137.33-163.04E
    if lat >= 1.03 && lat <= 10.09 && lon >= 137.33 && lon <= 163.04 {
        return Some(*b"FM");
    }
    // Indonesia: -11.00-5.91N, 95.01-141.02E
    if lat >= -11.00 && lat <= 5.91 && lon >= 95.01 && lon <= 141.02 {
        return Some(*b"ID");
    }
    // Israel: 29.49-33.33N, 34.27-35.90E
    if lat >= 29.49 && lat <= 33.33 && lon >= 34.27 && lon <= 35.90 {
        return Some(*b"IL");
    }
    // India: 6.75-35.50N, 68.19-97.40E
    if lat >= 6.75 && lat <= 35.50 && lon >= 68.19 && lon <= 97.40 {
        return Some(*b"IN");
    }
    // Iraq: 29.06-37.38N, 38.79-48.57E
    if lat >= 29.06 && lat <= 37.38 && lon >= 38.79 && lon <= 48.57 {
        return Some(*b"IQ");
    }
    // Iran: 25.06-39.78N, 44.05-63.32E
    if lat >= 25.06 && lat <= 39.78 && lon >= 44.05 && lon <= 63.32 {
        return Some(*b"IR");
    }
    // Jordan: 29.19-33.37N, 34.96-39.30E
    if lat >= 29.19 && lat <= 33.37 && lon >= 34.96 && lon <= 39.30 {
        return Some(*b"JO");
    }
    // Cambodia: 10.41-14.69N, 102.34-107.63E
    if lat >= 10.41 && lat <= 14.69 && lon >= 102.34 && lon <= 107.63 {
        return Some(*b"KH");
    }
    // Kiribati: -11.45-4.72N, -174.54-176.85E
    if lat >= -11.45 && lat <= 4.72 && lon >= -174.54 && lon <= 176.85 {
        return Some(*b"KI");
    }
    // Kuwait: 28.52-30.10N, 46.55-48.43E
    if lat >= 28.52 && lat <= 30.10 && lon >= 46.55 && lon <= 48.43 {
        return Some(*b"KW");
    }
    // Laos: 13.91-22.50N, 100.08-107.64E
    if lat >= 13.91 && lat <= 22.50 && lon >= 100.08 && lon <= 107.64 {
        return Some(*b"LA");
    }
    // Lebanon: 33.05-34.69N, 35.10-36.63E
    if lat >= 33.05 && lat <= 34.69 && lon >= 35.10 && lon <= 36.63 {
        return Some(*b"LB");
    }
    // Sri Lanka: 5.92-9.84N, 79.65-81.88E
    if lat >= 5.92 && lat <= 9.84 && lon >= 79.65 && lon <= 81.88 {
        return Some(*b"LK");
    }
    // Marshall Islands: 4.57-14.62N, 160.80-172.17E
    if lat >= 4.57 && lat <= 14.62 && lon >= 160.80 && lon <= 172.17 {
        return Some(*b"MH");
    }
    // Myanmar: 9.78-28.55N, 92.19-101.17E
    if lat >= 9.78 && lat <= 28.55 && lon >= 92.19 && lon <= 101.17 {
        return Some(*b"MM");
    }
    // Mongolia: 41.58-52.15N, 87.75-119.93E
    if lat >= 41.58 && lat <= 52.15 && lon >= 87.75 && lon <= 119.93 {
        return Some(*b"MN");
    }
    // Northern Mariana Islands: 14.11-20.56N, 144.89-146.07E
    if lat >= 14.11 && lat <= 20.56 && lon >= 144.89 && lon <= 146.07 {
        return Some(*b"MP");
    }
    // Maldives: -0.69-7.11N, 72.64-73.76E
    if lat >= -0.69 && lat <= 7.11 && lon >= 72.64 && lon <= 73.76 {
        return Some(*b"MV");
    }
    // Malaysia: 0.85-7.36N, 99.64-119.27E
    if lat >= 0.85 && lat <= 7.36 && lon >= 99.64 && lon <= 119.27 {
        return Some(*b"MY");
    }
    // Nepal: 26.36-30.45N, 80.06-88.20E
    if lat >= 26.36 && lat <= 30.45 && lon >= 80.06 && lon <= 88.20 {
        return Some(*b"NP");
    }
    // Nauru: -0.56--0.50N, 166.90-166.96E
    if lat >= -0.56 && lat <= -0.50 && lon >= 166.90 && lon <= 166.96 {
        return Some(*b"NR");
    }
    // Niue: -19.15--18.95N, -169.95--169.78E
    if lat >= -19.15 && lat <= -18.95 && lon >= -169.95 && lon <= -169.78 {
        return Some(*b"NU");
    }
    // Oman: 16.65-26.39N, 52.00-59.84E
    if lat >= 16.65 && lat <= 26.39 && lon >= 52.00 && lon <= 59.84 {
        return Some(*b"OM");
    }
    // Papua New Guinea: -11.66--0.73N, 140.84-159.50E
    if lat >= -11.66 && lat <= -0.73 && lon >= 140.84 && lon <= 159.50 {
        return Some(*b"PG");
    }
    // Philippines: 4.59-21.12N, 116.93-126.60E
    if lat >= 4.59 && lat <= 21.12 && lon >= 116.93 && lon <= 126.60 {
        return Some(*b"PH");
    }
    // Pakistan: 23.69-37.08N, 60.87-77.84E
    if lat >= 23.69 && lat <= 37.08 && lon >= 60.87 && lon <= 77.84 {
        return Some(*b"PK");
    }
    // Palestine: 31.22-32.55N, 34.22-35.57E
    if lat >= 31.22 && lat <= 32.55 && lon >= 34.22 && lon <= 35.57 {
        return Some(*b"PS");
    }
    // Palau: 2.80-8.10N, 131.12-134.73E
    if lat >= 2.80 && lat <= 8.10 && lon >= 131.12 && lon <= 134.73 {
        return Some(*b"PW");
    }
    // Qatar: 24.47-26.18N, 50.75-51.64E
    if lat >= 24.47 && lat <= 26.18 && lon >= 50.75 && lon <= 51.64 {
        return Some(*b"QA");
    }
    // Saudi Arabia: 16.38-32.15N, 34.57-55.67E
    if lat >= 16.38 && lat <= 32.15 && lon >= 34.57 && lon <= 55.67 {
        return Some(*b"SA");
    }
    // Solomon Islands: -12.31--5.10N, 155.51-170.19E
    if lat >= -12.31 && lat <= -5.10 && lon >= 155.51 && lon <= 170.19 {
        return Some(*b"SB");
    }
    // Singapore: 1.16-1.47N, 103.60-104.41E
    if lat >= 1.16 && lat <= 1.47 && lon >= 103.60 && lon <= 104.41 {
        return Some(*b"SG");
    }
    // Syria: 32.31-37.32N, 35.73-42.38E
    if lat >= 32.31 && lat <= 37.32 && lon >= 35.73 && lon <= 42.38 {
        return Some(*b"SY");
    }
    // Thailand: 5.61-20.46N, 97.34-105.64E
    if lat >= 5.61 && lat <= 20.46 && lon >= 97.34 && lon <= 105.64 {
        return Some(*b"TH");
    }
    // Timor-Leste: -9.50--8.13N, 124.04-127.34E
    if lat >= -9.50 && lat <= -8.13 && lon >= 124.04 && lon <= 127.34 {
        return Some(*b"TL");
    }
    // Tonga: -22.35--15.56N, -176.22--173.70E
    if lat >= -22.35 && lat <= -15.56 && lon >= -176.22 && lon <= -173.70 {
        return Some(*b"TO");
    }
    // Tuvalu: -10.80--5.64N, 176.06-179.87E
    if lat >= -10.80 && lat <= -5.64 && lon >= 176.06 && lon <= 179.87 {
        return Some(*b"TV");
    }
    // Taiwan: 21.90-25.30N, 120.00-122.01E
    if lat >= 21.90 && lat <= 25.30 && lon >= 120.00 && lon <= 122.01 {
        return Some(*b"TW");
    }
    // Vietnam: 8.56-23.39N, 102.14-109.47E
    if lat >= 8.56 && lat <= 23.39 && lon >= 102.14 && lon <= 109.47 {
        return Some(*b"VN");
    }
    // Vanuatu: -20.25--13.07N, 166.52-170.24E
    if lat >= -20.25 && lat <= -13.07 && lon >= 166.52 && lon <= 170.24 {
        return Some(*b"VU");
    }
    // Samoa: -14.08--13.43N, -172.80--171.40E
    if lat >= -14.08 && lat <= -13.43 && lon >= -172.80 && lon <= -171.40 {
        return Some(*b"WS");
    }
    // Yemen: 12.11-19.00N, 42.55-54.53E
    if lat >= 12.11 && lat <= 19.00 && lon >= 42.55 && lon <= 54.53 {
        return Some(*b"YE");
    }
    None
}

// ---------------------------------------------------------------------------
// Streaming Parquet helpers — flush buffered records as row groups
// ---------------------------------------------------------------------------

fn make_place_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::*;
    std::sync::Arc::new(Schema::new(vec![
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
        // Phase 2.2 — original (class, value) tag pair + per-record bbox.
        // All nullable: synthetic / non-OSM sources (BAG, ABR, SSR, …)
        // and node POIs leave them blank.
        Field::new("osm_class", DataType::Utf8, true),
        Field::new("osm_class_value", DataType::Utf8, true),
        Field::new("bbox_south", DataType::Int32, true),
        Field::new("bbox_north", DataType::Int32, true),
        Field::new("bbox_west",  DataType::Int32, true),
        Field::new("bbox_east",  DataType::Int32, true),
        // Phase 2.3 — Nominatim `extratags` allowlist captured at extract.
        // Encoded as semicolon-delimited "key=value;..." strings (same shape
        // as `name_intl`). Pack reads it back, builds the per-record sidecar.
        Field::new("extratags", DataType::Utf8, true),
    ]))
}

fn make_addr_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::*;
    std::sync::Arc::new(Schema::new(vec![
        Field::new("osm_id", DataType::Int64, false),
        Field::new("street", DataType::Utf8, false),
        Field::new("housenumber", DataType::Utf8, false),
        Field::new("postcode", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("lat", DataType::Float64, false),
        Field::new("lon", DataType::Float64, false),
    ]))
}

/// Flush buffered places to a Parquet row group. Clears the buffer, returns count written.
fn flush_place_batch(
    writer: &mut parquet::arrow::ArrowWriter<std::fs::File>,
    places: &mut Vec<RawPlace>,
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
) -> Result<usize> {
    use arrow::array::*;
    use std::sync::Arc;

    if places.is_empty() {
        return Ok(0);
    }
    let count = places.len();

    let mut osm_ids = Vec::with_capacity(count);
    let mut names = Vec::with_capacity(count);
    let mut lats = Vec::with_capacity(count);
    let mut lons = Vec::with_capacity(count);
    let mut place_types = Vec::with_capacity(count);
    let mut admin_levels: Vec<Option<u8>> = Vec::with_capacity(count);
    let mut populations: Vec<Option<u32>> = Vec::with_capacity(count);
    let mut wikidatas: Vec<Option<String>> = Vec::with_capacity(count);
    let mut alt_names_col: Vec<Option<String>> = Vec::with_capacity(count);
    let mut old_names_col: Vec<Option<String>> = Vec::with_capacity(count);
    let mut name_intl_col: Vec<Option<String>> = Vec::with_capacity(count);
    let mut osm_class_col: Vec<Option<String>> = Vec::with_capacity(count);
    let mut osm_class_value_col: Vec<Option<String>> = Vec::with_capacity(count);
    let mut bbox_south_col: Vec<Option<i32>> = Vec::with_capacity(count);
    let mut bbox_north_col: Vec<Option<i32>> = Vec::with_capacity(count);
    let mut bbox_west_col:  Vec<Option<i32>> = Vec::with_capacity(count);
    let mut bbox_east_col:  Vec<Option<i32>> = Vec::with_capacity(count);
    let mut extratags_col: Vec<Option<String>> = Vec::with_capacity(count);

    for p in places.iter() {
        osm_ids.push(p.osm_id);
        names.push(p.name.as_str());
        lats.push(p.coord.lat_f64());
        lons.push(p.coord.lon_f64());
        place_types.push(p.place_type as u8);
        admin_levels.push(p.admin_level);
        populations.push(p.population);
        wikidatas.push(p.wikidata.clone());
        alt_names_col.push(if p.alt_names.is_empty() {
            None
        } else {
            Some(p.alt_names.join(";"))
        });
        old_names_col.push(if p.old_names.is_empty() {
            None
        } else {
            Some(p.old_names.join(";"))
        });
        name_intl_col.push(if p.name_intl.is_empty() {
            None
        } else {
            Some(
                p.name_intl
                    .iter()
                    .map(|(lang, name)| format!("{}={}", lang, name))
                    .collect::<Vec<_>>()
                    .join(";"),
            )
        });
        osm_class_col.push(p.class.clone());
        osm_class_value_col.push(p.class_value.clone());
        let (bs, bn, bw, be) = match p.bbox {
            Some(b) => (Some(b.south), Some(b.north), Some(b.west), Some(b.east)),
            None => (None, None, None, None),
        };
        bbox_south_col.push(bs);
        bbox_north_col.push(bn);
        bbox_west_col.push(bw);
        bbox_east_col.push(be);
        extratags_col.push(if p.extratags.is_empty() {
            None
        } else {
            Some(
                p.extratags
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(";"),
            )
        });
    }

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
            Arc::new(StringArray::from(alt_names_col)),
            Arc::new(StringArray::from(old_names_col)),
            Arc::new(StringArray::from(name_intl_col)),
            Arc::new(StringArray::from(osm_class_col)),
            Arc::new(StringArray::from(osm_class_value_col)),
            Arc::new(Int32Array::from(bbox_south_col)),
            Arc::new(Int32Array::from(bbox_north_col)),
            Arc::new(Int32Array::from(bbox_west_col)),
            Arc::new(Int32Array::from(bbox_east_col)),
            Arc::new(StringArray::from(extratags_col)),
        ],
    )?;

    writer.write(&batch)?;
    places.clear();
    Ok(count)
}

/// Flush buffered addresses to a Parquet row group. Clears the buffer, returns count written.
fn flush_addr_batch(
    writer: &mut parquet::arrow::ArrowWriter<std::fs::File>,
    addresses: &mut Vec<RawAddress>,
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
) -> Result<usize> {
    use arrow::array::*;
    use std::sync::Arc;

    if addresses.is_empty() {
        return Ok(0);
    }
    let count = addresses.len();

    let mut osm_ids = Vec::with_capacity(count);
    let mut streets = Vec::with_capacity(count);
    let mut housenumbers = Vec::with_capacity(count);
    let mut postcodes: Vec<Option<&str>> = Vec::with_capacity(count);
    let mut cities: Vec<Option<&str>> = Vec::with_capacity(count);
    let mut states: Vec<Option<&str>> = Vec::with_capacity(count);
    let mut lats = Vec::with_capacity(count);
    let mut lons = Vec::with_capacity(count);

    for a in addresses.iter() {
        osm_ids.push(a.osm_id);
        streets.push(a.street.as_str());
        housenumbers.push(a.housenumber.as_str());
        postcodes.push(a.postcode.as_deref());
        cities.push(a.city.as_deref());
        states.push(a.state.as_deref());
        lats.push(a.lat);
        lons.push(a.lon);
    }

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(osm_ids)),
            Arc::new(StringArray::from(streets)),
            Arc::new(StringArray::from(housenumbers)),
            Arc::new(StringArray::from(postcodes)),
            Arc::new(StringArray::from(cities)),
            Arc::new(StringArray::from(states)),
            Arc::new(Float64Array::from(lats)),
            Arc::new(Float64Array::from(lons)),
        ],
    )?;

    writer.write(&batch)?;
    addresses.clear();
    Ok(count)
}
