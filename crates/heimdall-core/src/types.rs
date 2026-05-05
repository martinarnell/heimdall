/// Core data types for Heimdall.
///
/// Design principles:
/// - Fixed-size where possible (enables memory-mapped arrays)
/// - No heap allocation in hot query path
/// - Coordinates stored as fixed-point i32 (multiply by 1e-6 for f64)
/// - Admin hierarchy stored as u16 IDs (resolved via AdminIndex)

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Coordinates
// ---------------------------------------------------------------------------

/// Fixed-point coordinate pair.
/// lat/lon stored as integer microdegrees (value * 1_000_000).
/// Avoids float serialization issues, fits in 8 bytes.
///
/// Range: lat ±90.000000, lon ±180.000000
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Coord {
    pub lat: i32, // microdegrees: 59.331_000 → 59_331_000
    pub lon: i32, // microdegrees: 18.068_000 → 18_068_000
}

impl Coord {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self {
            lat: (lat * 1_000_000.0) as i32,
            lon: (lon * 1_000_000.0) as i32,
        }
    }

    pub fn lat_f64(&self) -> f64 {
        self.lat as f64 / 1_000_000.0
    }

    pub fn lon_f64(&self) -> f64 {
        self.lon as f64 / 1_000_000.0
    }

    /// Haversine distance in meters (approximate, fast)
    pub fn distance_m(&self, other: &Coord) -> f64 {
        let lat1 = self.lat_f64().to_radians();
        let lat2 = other.lat_f64().to_radians();
        let dlat = (other.lat_f64() - self.lat_f64()).to_radians();
        let dlon = (other.lon_f64() - self.lon_f64()).to_radians();
        let a = (dlat / 2.0).sin().powi(2)
            + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        6_371_000.0 * 2.0 * a.sqrt().atan2((1.0 - a).sqrt())
    }
}

// ---------------------------------------------------------------------------
// Place type
// ---------------------------------------------------------------------------

/// OSM place=* tag mapped to a compact enum.
/// Stored as u8 in PlaceRecord — max 256 types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum PlaceType {
    // Admin / populated places
    Country = 0,
    State = 1,     // admin_level 4 in Sweden = län
    County = 2,    // admin_level 7 in Sweden = kommun  
    City = 3,
    Town = 4,
    Village = 5,
    Hamlet = 6,
    Farm = 7,      // gård — very common in Swedish OSM
    Locality = 8,  // named place, no population tag

    // Infrastructure
    Suburb = 10,
    Quarter = 11,
    Neighbourhood = 12,
    Island = 13,
    Islet = 14,
    Square = 15,   // place=square (e.g. Sergels torg, Stortorget)
    Street = 16,   // notable named highway (Avenyn, Drottninggatan, Sveavägen)

    // Natural features (often geocoded)
    Lake = 20,
    River = 21,
    Mountain = 22,
    Forest = 23,
    Bay = 24,
    Cape = 25,

    // POI categories
    Airport = 30,
    Station = 31,  // railway/bus / public_transport=station

    // Notable POIs / cultural sights
    Landmark = 32,    // tourism=attraction|museum|gallery|viewpoint|theme_park|zoo|aquarium, historic=*
    University = 33,  // amenity=university|college
    Hospital = 34,    // amenity=hospital
    PublicBuilding = 35, // amenity=townhall|library|theatre|arts_centre
    Park = 36,        // leisure=park (only with notability signal)

    #[default]
    Unknown = 255,
}

impl PlaceType {
    pub fn from_osm(place_tag: &str) -> Self {
        match place_tag {
            "country" => Self::Country,
            "state" => Self::State,
            "county" => Self::County,
            "city" => Self::City,
            "town" => Self::Town,
            "village" => Self::Village,
            "hamlet" => Self::Hamlet,
            "farm" => Self::Farm,
            "locality" => Self::Locality,
            "suburb" => Self::Suburb,
            "quarter" => Self::Quarter,
            "neighbourhood" | "neighborhood" => Self::Neighbourhood,
            "island" => Self::Island,
            "islet" => Self::Islet,
            "square" => Self::Square,
            _ => Self::Unknown,
        }
    }

    /// Higher = more important for ranking homonym resolution
    pub fn importance_weight(&self) -> u8 {
        match self {
            Self::Country => 100,
            Self::State => 90,
            Self::County => 85,
            Self::City => 80,
            Self::Town => 70,
            Self::Village => 60,
            Self::Suburb | Self::Quarter => 55,
            Self::Hamlet | Self::Farm => 40,
            Self::Locality => 35,
            Self::Island => 50,
            Self::Airport | Self::Station => 65,
            Self::Square => 50,
            Self::Street => 48,
            Self::Neighbourhood => 50,
            Self::Landmark => 55,
            Self::University | Self::Hospital | Self::PublicBuilding => 50,
            Self::Park => 35,
            _ => 20,
        }
    }
}

// ---------------------------------------------------------------------------
// Bounding box delta (packed, 8 bytes)
// ---------------------------------------------------------------------------

/// Per-record bbox stored as four signed deltas relative to the record's
/// `coord`, each in 10-microdegree units (i16). Range ±0.32768° (~36 km
/// at the equator) — comfortably covers a city neighbourhood / POI / way
/// extent. Saturates for objects larger than that; admin polygons (which
/// have proper geometry indexed via Phase 2.1) compute their bbox from
/// the polygon at API time when they need a precise one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub struct BBoxDelta {
    /// south-edge delta (lat - coord.lat) / 10
    pub d_south: i16,
    /// north-edge delta (lat - coord.lat) / 10
    pub d_north: i16,
    /// west-edge delta  (lon - coord.lon) / 10
    pub d_west: i16,
    /// east-edge delta  (lon - coord.lon) / 10
    pub d_east: i16,
}

impl BBoxDelta {
    /// Encode an absolute bbox (microdegrees) into a packed delta from `centre`.
    /// Rounds to 10-microdegree units; saturates if the extent exceeds the i16 range.
    pub fn encode(centre: Coord, south: i32, north: i32, west: i32, east: i32) -> Self {
        let pack = |delta: i32| -> i16 {
            let scaled = delta / 10; // 10-microdegree quanta
            scaled.clamp(i16::MIN as i32, i16::MAX as i32) as i16
        };
        Self {
            d_south: pack(south - centre.lat),
            d_north: pack(north - centre.lat),
            d_west:  pack(west  - centre.lon),
            d_east:  pack(east  - centre.lon),
        }
    }

    /// Whether this delta carries any non-zero extent (i.e. the bbox is meaningful).
    pub fn is_zero(&self) -> bool {
        self.d_south == 0 && self.d_north == 0 && self.d_west == 0 && self.d_east == 0
    }

    /// Decode to absolute (south, north, west, east) microdegrees, given the centre.
    pub fn decode(&self, centre: Coord) -> (i32, i32, i32, i32) {
        (
            centre.lat + self.d_south as i32 * 10,
            centre.lat + self.d_north as i32 * 10,
            centre.lon + self.d_west  as i32 * 10,
            centre.lon + self.d_east  as i32 * 10,
        )
    }

    /// Decode to absolute (south, north, west, east) f64 degrees.
    pub fn decode_f64(&self, centre: Coord) -> (f64, f64, f64, f64) {
        let (s, n, w, e) = self.decode(centre);
        (
            s as f64 / 1_000_000.0,
            n as f64 / 1_000_000.0,
            w as f64 / 1_000_000.0,
            e as f64 / 1_000_000.0,
        )
    }
}

// ---------------------------------------------------------------------------
// Place record (fixed-size, 40 bytes — v4 layout)
// The hot path struct — stored in memory-mapped binary array
// ---------------------------------------------------------------------------

/// A single geocodable place.
/// Fixed 40 bytes — entire record store is a flat array, no indirection.
/// Names stored separately in a string pool, referenced by offset.
/// (class, type) pair is interned via `class_types.bin` keyed by `class_type`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(C)]
pub struct PlaceRecord {
    /// Coordinates (8 bytes)
    pub coord: Coord,

    /// Bounding box delta from `coord` (8 bytes — 4× i16 in 10-microdegree units)
    pub bbox: BBoxDelta,

    /// OSM node/way/relation ID — full i64 OSM space (8 bytes)
    pub osm_id: u64,

    /// Index into admin lookup table for county/län (2 bytes)
    pub admin1_id: u16,

    /// Index into admin lookup table for municipality/kommun (2 bytes)
    pub admin2_id: u16,

    /// Importance score 0-65535 (2 bytes)
    /// Derived from: population, place type, OSM way area, link count
    pub importance: u16,

    /// Index into the per-country class_type interning table (2 bytes).
    /// 0 = unknown / not-set. Lookup yields the original OSM
    /// (class, type) tag pair (e.g. `("place", "city")`,
    /// `("amenity", "restaurant")`) for jsonv2 / Nominatim parity.
    pub class_type: u16,

    /// Place type (1 byte) — coarse Heimdall enum
    pub place_type: PlaceType,

    /// Source flags (1 byte)
    /// bit 0: has_population
    /// bit 1: has_alt_name
    /// bit 2: has_old_name
    /// bit 3: is_relation (not a node)
    /// bit 4: is_way      (set when the original element was a way; combined
    ///                     with bit 3 lets `osm_type_from_flags()` distinguish
    ///                     N / W / R)
    /// bit 5: has_bbox    (the bbox field is populated; otherwise zero)
    pub flags: u8,

    /// Byte offset into the names string pool (4 bytes)
    /// Points to: [u8 primary_len][primary_name][u8 n_alts]([u8 len][alt_name])*
    pub name_offset: u32,
}

// Verify size at compile time
const _: () = assert!(std::mem::size_of::<PlaceRecord>() == 40);

// ---------------------------------------------------------------------------
// PlaceRecord v3 — legacy 24-byte layout (read fallback only)
// ---------------------------------------------------------------------------

/// The pre-Phase-2.2 layout of `PlaceRecord` — used **only** as a read-time
/// fallback by `RecordStore::open` when loading an index built before the
/// schema bump. New indices write the v4 (current) layout. Lifted to
/// `PlaceRecord` with bbox = 0, class_type = 0, osm_id widened from u32.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PlaceRecordV3 {
    pub coord: Coord,
    pub admin1_id: u16,
    pub admin2_id: u16,
    pub importance: u16,
    pub place_type: PlaceType,
    pub flags: u8,
    pub name_offset: u32,
    pub osm_id: u32,
}

const _: () = assert!(std::mem::size_of::<PlaceRecordV3>() == 24);

impl From<PlaceRecordV3> for PlaceRecord {
    fn from(v: PlaceRecordV3) -> Self {
        Self {
            coord: v.coord,
            bbox: BBoxDelta::default(),
            osm_id: v.osm_id as u64,
            admin1_id: v.admin1_id,
            admin2_id: v.admin2_id,
            importance: v.importance,
            class_type: 0,
            place_type: v.place_type,
            flags: v.flags,
            name_offset: v.name_offset,
        }
    }
}

// Bit positions for `PlaceRecord::flags`
pub const FLAG_HAS_POPULATION: u8 = 0x01;
pub const FLAG_HAS_ALT_NAME:   u8 = 0x02;
pub const FLAG_HAS_OLD_NAME:   u8 = 0x04;
pub const FLAG_IS_RELATION:    u8 = 0x08;
pub const FLAG_IS_WAY:         u8 = 0x10;
pub const FLAG_HAS_BBOX:       u8 = 0x20;

/// Decode `osm_type` (N / W / R) from the v4 flags byte. Falls back to N
/// when neither IS_WAY nor IS_RELATION is set, matching how Heimdall used
/// to flatten the type before Phase 2.2.
pub fn osm_type_from_flags(flags: u8) -> OsmType {
    if flags & FLAG_IS_RELATION != 0 {
        OsmType::Relation
    } else if flags & FLAG_IS_WAY != 0 {
        OsmType::Way
    } else {
        OsmType::Node
    }
}

// ---------------------------------------------------------------------------
// Raw place — used during build pipeline, before record store packing
// ---------------------------------------------------------------------------

/// Pre-packed representation used in the OSM extraction and Parquet stages.
/// Heap-allocated strings, flexible. Converted to PlaceRecord at index build time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RawPlace {
    pub osm_id: i64,
    pub osm_type: OsmType,

    /// Primary name (OSM name=*)
    pub name: String,

    /// name:en, name:de, etc.
    pub name_intl: Vec<(String, String)>, // (lang_code, name)

    /// alt_name=* (semicolon-split)
    pub alt_names: Vec<String>,

    /// old_name=* — important for historical news references
    pub old_names: Vec<String>,

    pub coord: Coord,
    pub place_type: PlaceType,

    pub admin_level: Option<u8>,
    pub country_code: Option<[u8; 2]>, // ISO 3166-1 alpha-2

    /// Nominatim-style admin hierarchy strings (resolved to IDs at build time)
    pub admin1: Option<String>, // county / state
    pub admin2: Option<String>, // municipality

    pub population: Option<u32>,
    pub wikidata: Option<String>,

    /// Original OSM `(class, type)` tag pair (e.g. `("place","city")`,
    /// `("amenity","restaurant")`, `("tourism","museum")`). Carries
    /// the full granularity that `place_type` collapses, for jsonv2
    /// / Nominatim API parity. Defaults to None for synthetic / non-OSM
    /// sources (ABR addresses, BAG addresses, etc.) — pack derives a
    /// best-effort default from `place_type` when absent.
    #[serde(default)]
    pub class: Option<String>,
    #[serde(default)]
    pub class_value: Option<String>,

    /// Bounding box of the underlying geometry, in microdegrees.
    /// Set for ways/relations where we resolved member coords during
    /// extraction. None for nodes (pack synthesises a small ~50m bbox
    /// from `coord`).
    #[serde(default)]
    pub bbox: Option<RawBBox>,
}

/// Pre-packed bbox (microdegrees) carried through the build pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBBox {
    pub south: i32,
    pub north: i32,
    pub west:  i32,
    pub east:  i32,
}

impl RawBBox {
    pub fn from_f64(s: f64, n: f64, w: f64, e: f64) -> Self {
        Self {
            south: (s * 1_000_000.0) as i32,
            north: (n * 1_000_000.0) as i32,
            west:  (w * 1_000_000.0) as i32,
            east:  (e * 1_000_000.0) as i32,
        }
    }
    /// Compute a bbox from an iterator of (lat, lon) f64 pairs. Returns None if empty.
    pub fn from_coords<I: IntoIterator<Item = (f64, f64)>>(coords: I) -> Option<Self> {
        let mut iter = coords.into_iter();
        let (lat0, lon0) = iter.next()?;
        let mut s = lat0; let mut n = lat0;
        let mut w = lon0; let mut e = lon0;
        for (lat, lon) in iter {
            if lat < s { s = lat; }
            if lat > n { n = lat; }
            if lon < w { w = lon; }
            if lon > e { e = lon; }
        }
        Some(Self::from_f64(s, n, w, e))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum OsmType {
    #[default]
    Node,
    Way,
    Relation,
}

// ---------------------------------------------------------------------------
// Admin index
// ---------------------------------------------------------------------------

/// Compact lookup for admin hierarchy.
/// Stored separately from PlaceRecord — loaded once into memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminEntry {
    pub id: u16,
    pub name: String,
    pub parent_id: Option<u16>,
    pub coord: Coord,
    pub place_type: PlaceType,
    /// Population of the admin entity (kommun/län), if tagged in OSM.
    /// Used by pack.rs as a centrality signal — places inside a populous
    /// parent kommun get a small importance bonus so a hotel in Stockholm
    /// (kommun pop 970K) outranks a same-name hotel in Jönköping (141K).
    /// Note: postcard / bincode-1 do not honour `#[serde(default)]` for
    /// trailing fields, so v2 indices without this field would fail to
    /// deserialise as `AdminEntry`. The runtime reader falls back to
    /// `AdminEntryV2` and lifts those records with population=0; the
    /// build pipeline writes the new shape going forward.
    pub population: u32,
}

/// The pre-centrality v2 layout of `AdminEntry` — everything except
/// `population`. Used **only** as a deserialisation fallback in
/// `HeimdallIndex::open` so the new binary can still load index
/// directories built before this change. New indices always serialise
/// the v3 (current) layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminEntryV2 {
    pub id: u16,
    pub name: String,
    pub parent_id: Option<u16>,
    pub coord: Coord,
    pub place_type: PlaceType,
}

impl From<AdminEntryV2> for AdminEntry {
    fn from(v: AdminEntryV2) -> Self {
        Self {
            id: v.id,
            name: v.name,
            parent_id: v.parent_id,
            coord: v.coord,
            place_type: v.place_type,
            population: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Query types
// ---------------------------------------------------------------------------

/// Input to the geocoder
#[derive(Debug, Clone)]
pub struct GeoQuery {
    pub text: String,

    /// Optional bounding box hint — dramatically improves homonym resolution
    pub bbox: Option<BoundingBox>,

    /// Optional country code hint
    pub country_code: Option<[u8; 2]>,

    /// Max results to return
    pub limit: usize,

    /// Minimum confidence threshold (0.0 - 1.0)
    pub min_confidence: f32,
}

impl GeoQuery {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            bbox: None,
            country_code: None,
            limit: 5,
            min_confidence: 0.0,
        }
    }

    pub fn with_country(mut self, cc: [u8; 2]) -> Self {
        self.country_code = Some(cc);
        self
    }

    pub fn with_bbox(mut self, bbox: BoundingBox) -> Self {
        self.bbox = Some(bbox);
        self
    }
}

/// Output from the geocoder
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoResult {
    pub name: String,
    pub coord: Coord,
    pub place_type: PlaceType,
    pub admin1: Option<String>,
    pub admin2: Option<String>,
    pub country_code: Option<[u8; 2]>,
    pub importance: u16,

    /// How confident we are in this result (0.0 - 1.0)
    pub confidence: f32,

    /// Which lookup path found this result — useful for debugging/logging
    pub match_type: MatchType,

    /// Record ID in the country's record store (for place_id encoding)
    pub record_id: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchType {
    /// Exact string match after normalization
    Exact,
    /// Matched via phonetic encoding (e.g. soundex)
    Phonetic,
    /// Matched via Levenshtein edit distance
    Levenshtein { distance: u8 },
    /// Matched via neural network
    Neural { confidence: u32 }, // confidence * 1000 as u32
    /// Matched via n-gram index
    NGram { score: u32 },
}

// ---------------------------------------------------------------------------
// Bounding box
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BoundingBox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

impl BoundingBox {
    /// Sweden bounding box
    pub fn sweden() -> Self {
        Self {
            min_lat: 55.0,
            max_lat: 69.5,
            min_lon: 10.5,
            max_lon: 24.5,
        }
    }

    pub fn contains(&self, coord: &Coord) -> bool {
        let lat = coord.lat_f64();
        let lon = coord.lon_f64();
        lat >= self.min_lat && lat <= self.max_lat && lon >= self.min_lon && lon <= self.max_lon
    }
}

// ---------------------------------------------------------------------------
// Importance scoring
// ---------------------------------------------------------------------------

/// Compute importance score (0-65535) for a raw place.
/// Used for ranking when multiple places match the same query.
///
/// Population dominates — a city with 100K people always outranks
/// an admin relation with the same name. Admin types (county, state)
/// are reference geometry, not destinations.
pub fn compute_importance(place: &RawPlace) -> u16 {
    let mut score: u32 = 0;

    // Population is king — log-scaled, dominates everything
    // log10(1000) = 3 → 9000, log10(100000) = 5 → 15000, log10(1000000) = 6 → 18000
    if let Some(pop) = place.population {
        if pop > 0 {
            score += ((pop as f64).log10() * 3000.0) as u32;
        }
    }

    // Place type as secondary signal
    // Cities/towns rank HIGH — they're what people search for
    // Admin types rank LOW — they're context, not destinations
    score += match place.place_type {
        PlaceType::City => 2000,
        PlaceType::Town => 1500,
        PlaceType::Village => 1000,
        PlaceType::Suburb | PlaceType::Quarter => 900,
        PlaceType::Neighbourhood => 850,
        PlaceType::Hamlet | PlaceType::Farm => 500,
        PlaceType::Island => 800,
        PlaceType::Airport | PlaceType::Station => 700,
        PlaceType::Square => 750,
        PlaceType::Landmark => 700,
        PlaceType::University | PlaceType::Hospital | PlaceType::PublicBuilding => 600,
        PlaceType::Park => 400,
        PlaceType::Lake | PlaceType::River => 600,
        PlaceType::Mountain | PlaceType::Forest => 500,
        PlaceType::County => 300,
        PlaceType::State => 200,
        PlaceType::Country => 100,
        _ => 200,
    };

    // Wikidata = notable
    if place.wikidata.is_some() {
        score += 1500;
    }

    // International names = well-known
    score += (place.name_intl.len() as u32).min(5) * 300;

    score.min(65535) as u16
}
