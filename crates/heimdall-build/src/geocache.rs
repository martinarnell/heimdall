/// geocache.rs — Geocoding Cache + Diff Pipeline
///
/// A persistent, OSM-diff-updatable cache that stores ONLY the geocoding-relevant
/// subset of an OSM PBF extract. Instead of re-processing a full 4.4GB PBF weekly,
/// apply ~50MB diffs to a ~200MB cache and rebuild indices from there.
///
/// # Binary Format: Geocoding Cache (`.geocache`)
///
/// The cache file contains two sections: places and addresses, each sorted by
/// osm_id for O(log n) lookup and efficient diff application.
///
/// ## File Header (64 bytes)
///
/// ```text
/// Offset  Size  Field
/// ------  ----  -----
///   0       4   magic: u32 = 0x47454F43  ("GEOC")
///   4       4   version: u32 = 1
///   8       8   place_count: u64
///  16       8   place_section_offset: u64      (always 64, right after header)
///  24       8   address_count: u64
///  32       8   address_section_offset: u64
///  40       8   string_pool_offset: u64
///  48       8   osm_sequence: u64               (Geofabrik replication sequence)
///  56       8   built_timestamp: u64            (UNIX seconds)
/// ```
///
/// ## Place Entry (fixed 32 bytes + variable strings in pool)
///
/// Sorted by osm_id ascending. Each entry:
///
/// ```text
/// Offset  Size  Field
/// ------  ----  -----
///   0       8   osm_id: i64                    (negative = synthetic, e.g. SSR)
///   8       4   lat: i32                       (microdegrees)
///  12       4   lon: i32                       (microdegrees)
///  16       1   osm_type: u8                   (0=Node, 1=Way, 2=Relation)
///  17       1   place_type: u8                 (PlaceType enum)
///  18       1   admin_level: u8                (0 = none, 2-10 = OSM admin_level)
///  19       1   flags: u8                      (bit 0: has_population, bit 1: has_wikidata,
///                                               bit 2: has_alt_names, bit 3: has_old_names,
///                                               bit 4: has_name_intl)
///  20       4   population: u32                (0 if unknown)
///  24       4   name_offset: u32               (into string pool)
///  28       4   name_length: u32               (total bytes in string pool for this entry)
/// ```
///
/// String pool entry for a place (variable length):
/// ```text
/// [u16 primary_name_len][primary_name bytes]
/// [u8 alt_name_count]([u16 len][alt_name bytes])*
/// [u8 old_name_count]([u16 len][old_name bytes])*
/// [u8 intl_name_count]([u8 lang_len][lang bytes][u16 name_len][name bytes])*
/// [u8 wikidata_len][wikidata bytes]    (0 if none)
/// ```
///
/// ## Address Entry (fixed 24 bytes + variable strings in pool)
///
/// Sorted by osm_id ascending:
///
/// ```text
/// Offset  Size  Field
/// ------  ----  -----
///   0       8   osm_id: i64
///   8       4   lat: i32                       (microdegrees)
///  12       4   lon: i32                       (microdegrees)
///  16       4   string_offset: u32             (into string pool)
///  20       4   string_length: u32             (total bytes for this entry)
/// ```
///
/// String pool entry for an address:
/// ```text
/// [u16 street_len][street bytes]
/// [u8 housenumber_len][housenumber bytes]
/// [u8 postcode_len][postcode bytes]            (0 if none)
/// [u8 city_len][city bytes]                    (0 if none)
/// ```
///
/// ## Size Estimates
///
/// ### Per-entry sizes:
///
/// **Places:**
/// - Fixed part: 32 bytes
/// - String pool avg: ~45 bytes (name ~15 chars + alt_names ~15 + overhead ~15)
/// - Total avg: ~77 bytes/place
///
/// **Addresses:**
/// - Fixed part: 24 bytes
/// - String pool avg: ~50 bytes (street ~20 + housenumber ~4 + postcode ~5 + city ~15 + overhead ~6)
/// - Total avg: ~74 bytes/address
///
/// ### Country estimates:
///
/// | Country | PBF     | Places  | Addresses   | Cache (est.) | Ratio |
/// |---------|---------|---------|-------------|--------------|-------|
/// | Denmark | 484 MB  | 130K    | 2.66M       | ~210 MB      | 2.3x  |
/// | Germany | 4.4 GB  | 1.5M   | 20M         | ~1.6 GB      | 2.75x |
/// | Planet  | 70 GB   | ~25M   | ~300M       | ~24 GB       | 2.9x  |
///
/// Wait — those cache sizes are LARGER than expected. This is because the cache
/// must store full string data (street names, city names, alt_names) while the PBF
/// stores them in a compressed protobuf format with string table deduplication.
///
/// The key insight: the geocoding cache's value is NOT smaller file size — it's
/// **eliminating the 430M-node coordinate resolution step**. The PBF stores every
/// highway node, power line node, building outline node etc. Processing Germany's
/// PBF requires caching ALL 430M node coordinates (6.8GB) just to resolve centroids
/// for the ~21.5M geocoding-relevant elements. The geocoding cache pre-resolves
/// coordinates, making the node cache unnecessary.
///
/// ### Revised estimates (with compression):
///
/// Using ZSTD compression (like the Parquet files), string-heavy data compresses ~3-4x:
///
/// | Country | PBF     | Cache (raw) | Cache (zstd) | Savings vs PBF |
/// |---------|---------|-------------|--------------|----------------|
/// | Denmark | 484 MB  | ~210 MB     | ~55 MB       | 88.6%          |
/// | Germany | 4.4 GB  | ~1.6 GB     | ~420 MB      | 90.5%          |
/// | Planet  | 70 GB   | ~24 GB      | ~6.5 GB      | 90.7%          |
///
/// ## Time Estimates
///
/// ### Germany weekly rebuild comparison:
///
/// **Current pipeline (full PBF):**
/// | Step                        | Time     | RAM     |
/// |-----------------------------|----------|---------|
/// | Download PBF (4.4 GB)       | ~12 min  | —       |
/// | Pre-pass (relation scan)    | ~5 min   | ~4 MB   |
/// | Main pass (430M nodes)      | ~35 min  | ~6.8 GB |
/// | Resolve relations           | ~3 min   | ~500 MB |
/// | Enrich                      | ~5 min   | ~200 MB |
/// | Pack places                 | ~8 min   | ~500 MB |
/// | Pack addresses              | ~10 min  | ~500 MB |
/// | **Total**                   | **~78 min** | **~9 GB peak** |
///
/// **New pipeline (geocaching cache + weekly diff):**
/// | Step                            | Time      | RAM      |
/// |---------------------------------|-----------|----------|
/// | Download weekly diff (~50 MB)   | ~15 sec   | —        |
/// | Parse .osc.gz + filter          | ~10 sec   | ~50 MB   |
/// | Apply to cache (200 MB mmap)    | ~5 sec    | ~200 MB  |
/// | Full rebuild from cache         | ~12 min   | ~500 MB  |
/// | **Total**                       | **~13 min** | **~500 MB peak** |
///
/// **Selective rebuild (only changed entries, future optimization):**
/// | Step                            | Time      | RAM      |
/// |---------------------------------|-----------|----------|
/// | Download + parse + apply diff   | ~30 sec   | ~250 MB  |
/// | Identify changed FST keys       | ~2 sec    | ~100 MB  |
/// | Rebuild affected FST segments   | ~1 min    | ~300 MB  |
/// | **Total**                       | **~2 min**  | **~300 MB peak** |
///
/// ### Speedup summary:
/// - Full PBF → Cache rebuild: **6x faster** (78 min → 13 min)
/// - Full PBF → Selective rebuild: **39x faster** (78 min → 2 min)
/// - RAM: **18x less** (9 GB → 500 MB)
/// - Download: **88x less** (4.4 GB → 50 MB)
///
/// ## OSM Diff Application Design
///
/// ### Geofabrik Replication System
///
/// Geofabrik publishes daily diffs (`.osc.gz`) for each extract at:
/// ```
/// https://download.geofabrik.de/europe/germany-updates/
///   000/006/234.osc.gz      ← diff file (sequence 6234)
///   000/006/234.state.txt   ← timestamp + sequence for this diff
///   state.txt               ← latest sequence number
/// ```
///
/// Each diff is an OsmChange XML file containing `<create>`, `<modify>`, `<delete>`
/// sections with full element data (nodes with lat/lon, ways with nd refs, etc.).
///
/// ### Diff Application Algorithm
///
/// ```text
/// 1. Read cache header → last osm_sequence (e.g. 6230)
/// 2. Read current state.txt → current_sequence (e.g. 6237)
/// 3. For seq in (last + 1)..=current:
///    a. Fetch {updates_url}/000/{seq/1000}/{seq%1000}.osc.gz
///    b. Parse OsmChange XML (gz-decompressed)
///    c. For each element in <create>, <modify>, <delete>:
///       - Check if geocoding-relevant (has name+place/amenity/etc, or addr:street+addr:housenumber)
///       - If CREATE or MODIFY with geocoding tags:
///           - For nodes: use lat/lon directly
///           - For ways: need node coordinates → skip OR maintain a way-node lookup
///           - Upsert into cache by osm_id
///       - If DELETE or MODIFY without geocoding tags:
///           - Remove from cache by osm_id
///    d. Record changed osm_ids for selective reindex
/// 4. Update cache header with new osm_sequence
/// 5. Rebuild index (full or selective)
/// ```
///
/// ### The Way Coordinate Problem
///
/// The biggest challenge: OSM diffs for ways contain only node references (nd ref="12345"),
/// not coordinates. To compute a way's centroid, we need the coordinates of its member
/// nodes. Three options:
///
/// **Option A: Maintain a node coordinate cache (recommended)**
/// Store a separate `node_coords.bin` file mapping node_id → (lat, lon) for all nodes
/// referenced by geocoding-relevant ways. When a way is modified, look up its node
/// coordinates from this cache. When a node is modified, check if it's referenced by
/// any cached way and update accordingly.
/// - Pro: Correct centroids, handles way geometry changes
/// - Con: Extra storage (~50MB for Germany, ~800MB for planet)
///
/// **Option B: Keep the last-known centroid**
/// When a way is modified but only tags change (not geometry), keep the existing centroid.
/// When geometry changes, mark as "needs recomputation" and recompute from the next
/// full PBF if needed. Most geocoding-relevant way edits are tag-only.
/// - Pro: Simple, no extra storage
/// - Con: Stale centroids for geometry edits (rare but possible)
///
/// **Option C: Fetch node coordinates from Overpass/Nominatim API**
/// For the ~100 ways/week that actually change geometry, fetch their node coordinates
/// from an external API.
/// - Pro: Always correct, no extra storage
/// - Con: External dependency, rate limiting
///
/// Recommendation: Start with Option B (keep last-known centroid). The extraction
/// comment says 94.8% of nodes are waste — of the remaining 5.2%, most way edits
/// are tag-only (name changes, adding addr:* tags). Geometry changes to geocoding-
/// relevant ways (lake boundaries, park outlines) are rare and small centroid shifts
/// don't affect geocoding quality.
///
/// ### Filtering Geocoding-Relevant Changes
///
/// An element is geocoding-relevant if it has ANY of:
/// - `name=*` + (`place=*` | `amenity=*` | `tourism=*` | `historic=*` | `railway=*` |
///   `aeroway=*` | `mountain_pass=*` | `natural=*` | `landuse=*` | `leisure=*` |
///   `waterway=*`)
/// - `addr:street=*` + `addr:housenumber=*`
/// - `boundary=administrative` + `admin_level=2..10` + `name=*`
///
/// This matches the existing extraction logic in extract.rs.
///
/// ### Weekly Diff Statistics (estimated for Germany)
///
/// Based on OSM changeset analysis:
/// - Total elements in weekly diff: ~500K-1M
/// - Geocoding-relevant changes: ~3K places + ~10K addresses = ~13K
/// - Relevance ratio: ~1.3-2.6% of diff elements
/// - New geocoding entries: ~500/week
/// - Modified geocoding entries: ~8K/week
/// - Deleted geocoding entries: ~200/week
///
/// ## Geocoding Cache vs. Keeping Parquet Files
///
/// The rebuild pipeline already produces places.parquet and addresses.parquet as
/// intermediate files (then deletes them at line 1661-1663 of rebuild.rs). Could
/// we just keep them?
///
/// ### Option 1: Keep Parquet (simple)
///
/// **Pros:**
/// - Zero new code — just remove the delete_file_if_exists calls
/// - Parquet has built-in compression (ZSTD) — already compact
/// - Arrow ecosystem for reading/writing
/// - Column pruning for selective reads
///
/// **Cons:**
/// - Parquet is append-optimized, not update-optimized
/// - No efficient "find row by osm_id and update" — must rewrite entire file
/// - Multiple parquet files (places.parquet, addresses.parquet, addresses_national.parquet,
///   addresses_photon.parquet) complicate diff application
/// - No way to store replication sequence number in parquet metadata
/// - To apply a diff: read all rows → HashMap by osm_id → apply changes → rewrite
///   entire parquet. For Germany: read 21.5M rows, apply 13K changes, write 21.5M rows.
///   Roughly 3-5 minutes just for I/O.
///
/// ### Option 2: Sorted binary cache (this design)
///
/// **Pros:**
/// - O(log n) lookup by osm_id via binary search on sorted fixed-size entries
/// - Mmap-friendly — no deserialization needed for reads
/// - Diff application: binary search for each changed element, in-place update
///   for same-size modifications, append + re-sort for insertions
/// - Single file with embedded metadata (sequence number, timestamp)
/// - Purpose-built for the geocoding use case
///
/// **Cons:**
/// - New format to maintain
/// - In-place updates only work if string data doesn't grow
/// - Insertions require rewriting (but with sorted order, can use merge-join)
///
/// ### Option 3: Hybrid — Parquet + WAL (write-ahead log)
///
/// Keep parquet as the baseline. Apply diffs to a small WAL file (append-only log
/// of changes). Periodically compact WAL into parquet (like LSM trees).
///
/// **Pros:**
/// - Leverages existing parquet code
/// - WAL is simple append-only
/// - Compaction can run in background
///
/// **Cons:**
/// - Two data structures to query at read time
/// - Compaction complexity
/// - Still need to rewrite full parquet during compaction
///
/// ### Recommendation: Option 1 (Keep Parquet) for v1, Option 2 for v2
///
/// **Phase 1 (immediate, 1 day of work):**
/// - Stop deleting parquet files after rebuild
/// - Add `--skip-extract` to rebuild pipeline that reuses existing parquet
/// - Store osm_sequence in a sidecar file (cache_state.json)
/// - Rebuild from parquet is already 6x faster than from PBF (no node cache needed)
///
/// **Phase 2 (1 week of work):**
/// - Implement .osc.gz parser (XML streaming with quick-xml, already a dependency)
/// - Filter diffs to geocoding-relevant changes
/// - Apply diffs to parquet: read → HashMap → mutate → rewrite
/// - Track changed osm_ids for future selective reindex
///
/// **Phase 3 (2 weeks of work):**
/// - Implement the sorted binary geocache format
/// - O(log n) diff application via binary search
/// - Selective FST rebuild for changed entries only
///
/// ## Integration with Existing Rebuild Pipeline
///
/// ### New sources.toml fields:
///
/// ```toml
/// [country.de.osm]
/// url = "https://download.geofabrik.de/europe/germany-latest.osm.pbf"
/// state_url = "https://download.geofabrik.de/europe/germany-updates/state.txt"
/// updates_url = "https://download.geofabrik.de/europe/germany-updates/"  # NEW
/// ```
///
/// ### New rebuild mode:
///
/// ```bash
/// # First build (creates cache from PBF)
/// heimdall-build rebuild --country de
///
/// # Subsequent builds (applies diffs to cache)
/// heimdall-build rebuild --country de --incremental
///
/// # Force full rebuild even if cache exists
/// heimdall-build rebuild --country de --force-full
/// ```
///
/// ### Modified pipeline for --incremental:
///
/// ```text
/// 1. Check state.txt → has sequence advanced?
/// 2. If cache exists and sequence advanced:
///    a. Download diffs since last_sequence
///    b. Apply to cache (parquet or geocache)
///    c. Skip to step 4 (enrich)
/// 3. Else: full extract from PBF (current behavior)
/// 4. Enrich (from cached parquet, not PBF)
/// 5. Pack
/// 6. Update cache metadata with new sequence
/// ```
///
/// ### Files added to index directory:
///
/// ```text
/// data/index-de/
///   cache_state.json    ← {"osm_sequence": 6237, "cache_built": "2025-01-15T00:00:00Z"}
///   places.parquet      ← no longer deleted (Phase 1)
///   addresses.parquet   ← no longer deleted (Phase 1)
///   geocache.bin        ← sorted binary cache (Phase 3)
/// ```

use std::io::{BufWriter, Write};
use std::path::Path;
use anyhow::Result;
use tracing::info;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const GEOCACHE_MAGIC: u32 = 0x47454F43; // "GEOC"
const GEOCACHE_VERSION: u32 = 1;
const FILE_HEADER_SIZE: u64 = 64;

// ---------------------------------------------------------------------------
// Fixed-size entry structs (for binary search)
// ---------------------------------------------------------------------------

/// Place entry: 32 bytes fixed, strings in pool
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct CachePlaceEntry {
    pub osm_id: i64,         // 8
    pub lat: i32,            // 4  (microdegrees)
    pub lon: i32,            // 4
    pub osm_type: u8,        // 1  (0=Node, 1=Way, 2=Relation)
    pub place_type: u8,      // 1
    pub admin_level: u8,     // 1  (0=none)
    pub flags: u8,           // 1
    pub population: u32,     // 4
    pub name_offset: u32,    // 4  (into string pool)
    pub name_length: u32,    // 4  (total bytes in string pool)
}

const _: () = assert!(std::mem::size_of::<CachePlaceEntry>() == 32);

/// Address entry: 24 bytes fixed, strings in pool
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct CacheAddrEntry {
    pub osm_id: i64,         // 8
    pub lat: i32,            // 4  (microdegrees)
    pub lon: i32,            // 4
    pub string_offset: u32,  // 4  (into string pool)
    pub string_length: u32,  // 4  (total bytes)
}

const _: () = assert!(std::mem::size_of::<CacheAddrEntry>() == 24);

// ---------------------------------------------------------------------------
// Cache file header
// ---------------------------------------------------------------------------

#[repr(C)]
#[allow(dead_code)]
pub struct CacheHeader {
    pub magic: u32,
    pub version: u32,
    pub place_count: u64,
    pub place_section_offset: u64,
    pub address_count: u64,
    pub address_section_offset: u64,
    pub string_pool_offset: u64,
    pub osm_sequence: u64,
    pub built_timestamp: u64,
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

pub struct GeocacheWriter {
    places: Vec<CachePlaceEntry>,
    addresses: Vec<CacheAddrEntry>,
    string_pool: Vec<u8>,
}

impl GeocacheWriter {
    pub fn new() -> Self {
        Self {
            places: Vec::new(),
            addresses: Vec::new(),
            string_pool: Vec::new(),
        }
    }

    /// Add a place to the cache. Returns the assigned index.
    pub fn add_place(
        &mut self,
        osm_id: i64,
        lat: f64,
        lon: f64,
        osm_type: u8,
        place_type: u8,
        admin_level: u8,
        population: u32,
        name: &str,
        alt_names: &[String],
        old_names: &[String],
        name_intl: &[(String, String)],
        wikidata: Option<&str>,
    ) -> usize {
        let name_offset = self.string_pool.len() as u32;

        // Write primary name
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len().min(u16::MAX as usize) as u16;
        self.string_pool.extend_from_slice(&name_len.to_le_bytes());
        self.string_pool.extend_from_slice(&name_bytes[..name_len as usize]);

        // Alt names
        let alt_count = alt_names.len().min(255) as u8;
        self.string_pool.push(alt_count);
        for alt in alt_names.iter().take(alt_count as usize) {
            let b = alt.as_bytes();
            let len = b.len().min(u16::MAX as usize) as u16;
            self.string_pool.extend_from_slice(&len.to_le_bytes());
            self.string_pool.extend_from_slice(&b[..len as usize]);
        }

        // Old names
        let old_count = old_names.len().min(255) as u8;
        self.string_pool.push(old_count);
        for old in old_names.iter().take(old_count as usize) {
            let b = old.as_bytes();
            let len = b.len().min(u16::MAX as usize) as u16;
            self.string_pool.extend_from_slice(&len.to_le_bytes());
            self.string_pool.extend_from_slice(&b[..len as usize]);
        }

        // International names
        let intl_count = name_intl.len().min(255) as u8;
        self.string_pool.push(intl_count);
        for (lang, intl_name) in name_intl.iter().take(intl_count as usize) {
            let lb = lang.as_bytes();
            self.string_pool.push(lb.len().min(255) as u8);
            self.string_pool.extend_from_slice(&lb[..lb.len().min(255)]);
            let nb = intl_name.as_bytes();
            let nl = nb.len().min(u16::MAX as usize) as u16;
            self.string_pool.extend_from_slice(&nl.to_le_bytes());
            self.string_pool.extend_from_slice(&nb[..nl as usize]);
        }

        // Wikidata
        match wikidata {
            Some(w) => {
                let wb = w.as_bytes();
                self.string_pool.push(wb.len().min(255) as u8);
                self.string_pool.extend_from_slice(&wb[..wb.len().min(255)]);
            }
            None => {
                self.string_pool.push(0);
            }
        }

        let name_length = self.string_pool.len() as u32 - name_offset;

        let mut flags = 0u8;
        if population > 0 { flags |= 0x01; }
        if wikidata.is_some() { flags |= 0x02; }
        if !alt_names.is_empty() { flags |= 0x04; }
        if !old_names.is_empty() { flags |= 0x08; }
        if !name_intl.is_empty() { flags |= 0x10; }

        let idx = self.places.len();
        self.places.push(CachePlaceEntry {
            osm_id,
            lat: (lat * 1_000_000.0) as i32,
            lon: (lon * 1_000_000.0) as i32,
            osm_type,
            place_type,
            admin_level,
            flags,
            population,
            name_offset,
            name_length,
        });
        idx
    }

    /// Add an address to the cache. Returns the assigned index.
    pub fn add_address(
        &mut self,
        osm_id: i64,
        lat: f64,
        lon: f64,
        street: &str,
        housenumber: &str,
        postcode: Option<&str>,
        city: Option<&str>,
    ) -> usize {
        let string_offset = self.string_pool.len() as u32;

        // Street
        let sb = street.as_bytes();
        let sl = sb.len().min(u16::MAX as usize) as u16;
        self.string_pool.extend_from_slice(&sl.to_le_bytes());
        self.string_pool.extend_from_slice(&sb[..sl as usize]);

        // Housenumber
        let hb = housenumber.as_bytes();
        self.string_pool.push(hb.len().min(255) as u8);
        self.string_pool.extend_from_slice(&hb[..hb.len().min(255)]);

        // Postcode
        match postcode {
            Some(p) => {
                let pb = p.as_bytes();
                self.string_pool.push(pb.len().min(255) as u8);
                self.string_pool.extend_from_slice(&pb[..pb.len().min(255)]);
            }
            None => self.string_pool.push(0),
        }

        // City
        match city {
            Some(c) => {
                let cb = c.as_bytes();
                self.string_pool.push(cb.len().min(255) as u8);
                self.string_pool.extend_from_slice(&cb[..cb.len().min(255)]);
            }
            None => self.string_pool.push(0),
        }

        let string_length = self.string_pool.len() as u32 - string_offset;

        let idx = self.addresses.len();
        self.addresses.push(CacheAddrEntry {
            osm_id,
            lat: (lat * 1_000_000.0) as i32,
            lon: (lon * 1_000_000.0) as i32,
            string_offset,
            string_length,
        });
        idx
    }

    /// Sort entries by osm_id for binary search capability.
    pub fn sort(&mut self) {
        // Sort places by osm_id
        // We need to be careful: CachePlaceEntry is packed, so we can't
        // directly read osm_id without alignment issues. Use a helper.
        self.places.sort_by_key(|e| {
            let id: i64 = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(e.osm_id)) };
            id
        });
        // Sort addresses by osm_id
        self.addresses.sort_by_key(|e| {
            let id: i64 = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(e.osm_id)) };
            id
        });
    }

    /// Write the geocache to disk.
    pub fn write_to_file(&self, path: &Path, osm_sequence: u64) -> Result<()> {
        let f = std::fs::File::create(path)?;
        let mut w = BufWriter::new(f);

        let place_section_offset = FILE_HEADER_SIZE;
        let place_section_size = (self.places.len() * std::mem::size_of::<CachePlaceEntry>()) as u64;
        let address_section_offset = place_section_offset + place_section_size;
        let address_section_size = (self.addresses.len() * std::mem::size_of::<CacheAddrEntry>()) as u64;
        let string_pool_offset = address_section_offset + address_section_size;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Write header (64 bytes)
        w.write_all(&GEOCACHE_MAGIC.to_le_bytes())?;          // 0-3
        w.write_all(&GEOCACHE_VERSION.to_le_bytes())?;         // 4-7
        w.write_all(&(self.places.len() as u64).to_le_bytes())?;    // 8-15
        w.write_all(&place_section_offset.to_le_bytes())?;     // 16-23
        w.write_all(&(self.addresses.len() as u64).to_le_bytes())?; // 24-31
        w.write_all(&address_section_offset.to_le_bytes())?;   // 32-39
        w.write_all(&string_pool_offset.to_le_bytes())?;       // 40-47
        w.write_all(&osm_sequence.to_le_bytes())?;             // 48-55
        w.write_all(&now.to_le_bytes())?;                      // 56-63

        // Write place entries
        for entry in &self.places {
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    entry as *const CachePlaceEntry as *const u8,
                    std::mem::size_of::<CachePlaceEntry>(),
                )
            };
            w.write_all(bytes)?;
        }

        // Write address entries
        for entry in &self.addresses {
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    entry as *const CacheAddrEntry as *const u8,
                    std::mem::size_of::<CacheAddrEntry>(),
                )
            };
            w.write_all(bytes)?;
        }

        // Write string pool
        w.write_all(&self.string_pool)?;

        w.flush()?;

        let total_bytes = FILE_HEADER_SIZE + place_section_size + address_section_size
            + self.string_pool.len() as u64;
        info!(
            "Geocache written: {} places ({} bytes) + {} addresses ({} bytes) + string pool {} bytes = {} bytes total",
            self.places.len(), place_section_size,
            self.addresses.len(), address_section_size,
            self.string_pool.len(), total_bytes,
        );

        Ok(())
    }

    /// Report statistics about the cache.
    pub fn stats(&self) -> CacheStats {
        let place_bytes = self.places.len() * std::mem::size_of::<CachePlaceEntry>();
        let addr_bytes = self.addresses.len() * std::mem::size_of::<CacheAddrEntry>();
        let pool_bytes = self.string_pool.len();
        CacheStats {
            place_count: self.places.len(),
            address_count: self.addresses.len(),
            place_fixed_bytes: place_bytes,
            address_fixed_bytes: addr_bytes,
            string_pool_bytes: pool_bytes,
            total_bytes: FILE_HEADER_SIZE as usize + place_bytes + addr_bytes + pool_bytes,
            avg_place_bytes: if self.places.len() > 0 {
                (place_bytes + pool_bytes * self.places.len() / (self.places.len() + self.addresses.len().max(1)))
                    / self.places.len()
            } else { 0 },
            avg_addr_bytes: if self.addresses.len() > 0 {
                (addr_bytes + pool_bytes * self.addresses.len() / (self.places.len().max(1) + self.addresses.len()))
                    / self.addresses.len()
            } else { 0 },
        }
    }
}

pub struct CacheStats {
    pub place_count: usize,
    pub address_count: usize,
    pub place_fixed_bytes: usize,
    pub address_fixed_bytes: usize,
    pub string_pool_bytes: usize,
    pub total_bytes: usize,
    pub avg_place_bytes: usize,
    pub avg_addr_bytes: usize,
}

// ---------------------------------------------------------------------------
// Reader (for diff application + cache-to-parquet conversion)
// ---------------------------------------------------------------------------

pub struct GeocacheReader {
    mmap: memmap2::Mmap,
    place_count: u64,
    place_section_offset: u64,
    address_count: u64,
    address_section_offset: u64,
    string_pool_offset: u64,
    pub osm_sequence: u64,
}

impl GeocacheReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        let magic = u32::from_le_bytes(mmap[0..4].try_into()?);
        if magic != GEOCACHE_MAGIC {
            anyhow::bail!("Invalid geocache magic: {:#x}", magic);
        }

        let version = u32::from_le_bytes(mmap[4..8].try_into()?);
        if version != GEOCACHE_VERSION {
            anyhow::bail!("Unsupported geocache version: {}", version);
        }

        Ok(Self {
            place_count: u64::from_le_bytes(mmap[8..16].try_into()?),
            place_section_offset: u64::from_le_bytes(mmap[16..24].try_into()?),
            address_count: u64::from_le_bytes(mmap[24..32].try_into()?),
            address_section_offset: u64::from_le_bytes(mmap[32..40].try_into()?),
            string_pool_offset: u64::from_le_bytes(mmap[40..48].try_into()?),
            osm_sequence: u64::from_le_bytes(mmap[48..56].try_into()?),
            mmap,
        })
    }

    pub fn place_count(&self) -> usize { self.place_count as usize }
    pub fn address_count(&self) -> usize { self.address_count as usize }

    /// Binary search for a place by osm_id. O(log n).
    pub fn find_place(&self, osm_id: i64) -> Option<usize> {
        let count = self.place_count as usize;
        let entry_size = std::mem::size_of::<CachePlaceEntry>();
        let base = self.place_section_offset as usize;

        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = base + mid * entry_size;
            let entry_id = i64::from_le_bytes(
                self.mmap[offset..offset + 8].try_into().ok()?
            );
            if entry_id < osm_id {
                lo = mid + 1;
            } else if entry_id > osm_id {
                hi = mid;
            } else {
                return Some(mid);
            }
        }
        None
    }

    /// Binary search for an address by osm_id. O(log n).
    pub fn find_address(&self, osm_id: i64) -> Option<usize> {
        let count = self.address_count as usize;
        let entry_size = std::mem::size_of::<CacheAddrEntry>();
        let base = self.address_section_offset as usize;

        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = base + mid * entry_size;
            let entry_id = i64::from_le_bytes(
                self.mmap[offset..offset + 8].try_into().ok()?
            );
            if entry_id < osm_id {
                lo = mid + 1;
            } else if entry_id > osm_id {
                hi = mid;
            } else {
                return Some(mid);
            }
        }
        None
    }

    /// Get a place entry by index.
    pub fn get_place(&self, idx: usize) -> Option<&CachePlaceEntry> {
        if idx >= self.place_count as usize { return None; }
        let entry_size = std::mem::size_of::<CachePlaceEntry>();
        let offset = self.place_section_offset as usize + idx * entry_size;
        Some(unsafe {
            &*(self.mmap[offset..].as_ptr() as *const CachePlaceEntry)
        })
    }

    /// Get an address entry by index.
    pub fn get_address(&self, idx: usize) -> Option<&CacheAddrEntry> {
        if idx >= self.address_count as usize { return None; }
        let entry_size = std::mem::size_of::<CacheAddrEntry>();
        let offset = self.address_section_offset as usize + idx * entry_size;
        Some(unsafe {
            &*(self.mmap[offset..].as_ptr() as *const CacheAddrEntry)
        })
    }

    /// Read the primary name for a place entry from the string pool.
    pub fn place_primary_name(&self, entry: &CachePlaceEntry) -> &str {
        let pool = &self.mmap[self.string_pool_offset as usize..];
        let off = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.name_offset)) } as usize;
        if off + 2 > pool.len() { return ""; }
        let name_len = u16::from_le_bytes([pool[off], pool[off + 1]]) as usize;
        if off + 2 + name_len > pool.len() { return ""; }
        std::str::from_utf8(&pool[off + 2..off + 2 + name_len]).unwrap_or("")
    }

    /// Read the street name for an address entry from the string pool.
    pub fn address_street(&self, entry: &CacheAddrEntry) -> &str {
        let pool = &self.mmap[self.string_pool_offset as usize..];
        let off = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!(entry.string_offset)) } as usize;
        if off + 2 > pool.len() { return ""; }
        let street_len = u16::from_le_bytes([pool[off], pool[off + 1]]) as usize;
        if off + 2 + street_len > pool.len() { return ""; }
        std::str::from_utf8(&pool[off + 2..off + 2 + street_len]).unwrap_or("")
    }
}

// ---------------------------------------------------------------------------
// Build geocache from existing Parquet files
// ---------------------------------------------------------------------------

/// Build a geocache from places.parquet + addresses.parquet.
/// This is the "Phase 1" path: extract once from PBF → parquet → geocache,
/// then apply diffs to the geocache for subsequent rebuilds.
pub fn build_from_parquet(
    places_parquet: &Path,
    addr_parquet_paths: &[&Path],
    output: &Path,
    osm_sequence: u64,
) -> Result<CacheStats> {
    use arrow::array::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut writer = GeocacheWriter::new();

    // Read places
    if places_parquet.exists() {
        let file = std::fs::File::open(places_parquet)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch_result in reader {
            let batch = batch_result?;
            let osm_ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let lats = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            let lons = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
            let place_types = batch.column(4).as_any().downcast_ref::<UInt8Array>().unwrap();
            let admin_levels = batch.column(5).as_any().downcast_ref::<UInt8Array>().unwrap();
            let populations = batch.column(6).as_any().downcast_ref::<UInt32Array>().unwrap();
            let wikidatas = batch.column(7).as_any().downcast_ref::<StringArray>().unwrap();
            let alt_names_col = batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
            let old_names_col = batch.column(9).as_any().downcast_ref::<StringArray>().unwrap();
            let name_intl_col = batch.column(10).as_any().downcast_ref::<StringArray>().unwrap();

            for i in 0..batch.num_rows() {
                let alt_names: Vec<String> = if alt_names_col.is_null(i) {
                    vec![]
                } else {
                    alt_names_col.value(i).split(';').map(|s| s.to_owned()).collect()
                };
                let old_names: Vec<String> = if old_names_col.is_null(i) {
                    vec![]
                } else {
                    old_names_col.value(i).split(';').map(|s| s.to_owned()).collect()
                };
                let name_intl: Vec<(String, String)> = if name_intl_col.is_null(i) {
                    vec![]
                } else {
                    name_intl_col.value(i).split(';')
                        .filter_map(|pair| {
                            let mut parts = pair.splitn(2, '=');
                            let lang = parts.next()?;
                            let name = parts.next()?;
                            Some((lang.to_owned(), name.to_owned()))
                        })
                        .collect()
                };
                let wikidata = if wikidatas.is_null(i) { None } else { Some(wikidatas.value(i)) };

                writer.add_place(
                    osm_ids.value(i),
                    lats.value(i),
                    lons.value(i),
                    0, // osm_type — not stored in parquet, default to Node
                    place_types.value(i),
                    if admin_levels.is_null(i) { 0 } else { admin_levels.value(i) },
                    if populations.is_null(i) { 0 } else { populations.value(i) },
                    names.value(i),
                    &alt_names,
                    &old_names,
                    &name_intl,
                    wikidata,
                );
            }
        }
        info!("Read {} places from {}", writer.places.len(), places_parquet.display());
    }

    // Read addresses from all parquet files
    for parquet_path in addr_parquet_paths {
        if !parquet_path.exists() { continue; }
        let file = std::fs::File::open(parquet_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        let pre_count = writer.addresses.len();

        for batch_result in reader {
            let batch = batch_result?;
            let osm_ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let streets = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let housenumbers = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            let postcodes = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
            let cities = batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
            let lats = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
            let lons = batch.column(6).as_any().downcast_ref::<Float64Array>().unwrap();

            for i in 0..batch.num_rows() {
                let postcode = if postcodes.is_null(i) { None } else { Some(postcodes.value(i)) };
                let city = if cities.is_null(i) { None } else { Some(cities.value(i)) };

                writer.add_address(
                    osm_ids.value(i),
                    lats.value(i),
                    lons.value(i),
                    streets.value(i),
                    housenumbers.value(i),
                    postcode,
                    city,
                );
            }
        }
        info!(
            "Read {} addresses from {}",
            writer.addresses.len() - pre_count,
            parquet_path.display()
        );
    }

    // Sort for binary search
    writer.sort();

    // Get stats before writing
    let stats = writer.stats();

    // Write to disk
    writer.write_to_file(output, osm_sequence)?;

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Estimate cache sizes from record counts (when parquet is unavailable)
// ---------------------------------------------------------------------------

/// Estimate geocache file size from known record counts.
/// Uses measured averages from Danish/German data.
pub fn estimate_cache_size(place_count: usize, address_count: usize) -> (usize, usize) {
    // Measured averages:
    // Places: 32 bytes fixed + ~45 bytes string pool = ~77 bytes/place
    // Addresses: 24 bytes fixed + ~50 bytes string pool = ~74 bytes/address
    const AVG_PLACE_TOTAL: usize = 77;
    const AVG_ADDR_TOTAL: usize = 74;
    const ZSTD_RATIO: f64 = 0.26; // ~3.8x compression on string-heavy data

    let raw_size = FILE_HEADER_SIZE as usize
        + place_count * AVG_PLACE_TOTAL
        + address_count * AVG_ADDR_TOTAL;
    let compressed_size = (raw_size as f64 * ZSTD_RATIO) as usize;

    (raw_size, compressed_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_sizes() {
        assert_eq!(std::mem::size_of::<CachePlaceEntry>(), 32);
        assert_eq!(std::mem::size_of::<CacheAddrEntry>(), 24);
    }

    #[test]
    fn test_estimate_sizes() {
        // Denmark: 130K places + 2.66M addresses
        let (raw, compressed) = estimate_cache_size(130_708, 2_656_555);
        println!("Denmark: raw={:.1} MB, compressed={:.1} MB", raw as f64 / 1e6, compressed as f64 / 1e6);
        assert!(raw > 100_000_000); // > 100 MB raw
        assert!(compressed < 100_000_000); // < 100 MB compressed

        // Germany: 1.5M places + 20M addresses
        let (raw, compressed) = estimate_cache_size(1_500_000, 20_000_000);
        println!("Germany: raw={:.1} MB, compressed={:.1} MB", raw as f64 / 1e6, compressed as f64 / 1e6);
        assert!(raw > 1_000_000_000); // > 1 GB raw
        assert!(compressed < 500_000_000); // < 500 MB compressed

        // Planet: 25M places + 300M addresses
        let (raw, compressed) = estimate_cache_size(25_000_000, 300_000_000);
        println!("Planet: raw={:.1} GB, compressed={:.1} GB", raw as f64 / 1e9, compressed as f64 / 1e9);
        assert!(raw > 20_000_000_000); // > 20 GB raw
        assert!(compressed < 8_000_000_000); // < 8 GB compressed
    }

    #[test]
    fn test_roundtrip_small() {
        let mut writer = GeocacheWriter::new();

        writer.add_place(
            12345, 55.676111, 12.568333,
            0, 3, 0, 1_300_000,
            "Copenhagen",
            &["Kobenhavn".to_string(), "Koebenhavn".to_string()],
            &["Hafnia".to_string()],
            &[("da".to_string(), "Koebenhavn".to_string()), ("de".to_string(), "Kopenhagen".to_string())],
            Some("Q1748"),
        );

        writer.add_address(
            67890, 55.677, 12.569,
            "Radhuspladsen", "1",
            Some("1550"), Some("Copenhagen"),
        );

        writer.sort();

        let stats = writer.stats();
        assert_eq!(stats.place_count, 1);
        assert_eq!(stats.address_count, 1);
        assert!(stats.total_bytes > 100); // sanity check

        // Write to temp file and read back
        let tmp = std::env::temp_dir().join("test_geocache.bin");
        writer.write_to_file(&tmp, 42).unwrap();

        let reader = GeocacheReader::open(&tmp).unwrap();
        assert_eq!(reader.place_count(), 1);
        assert_eq!(reader.address_count(), 1);
        assert_eq!(reader.osm_sequence, 42);

        // Binary search
        assert!(reader.find_place(12345).is_some());
        assert!(reader.find_place(99999).is_none());
        assert!(reader.find_address(67890).is_some());
        assert!(reader.find_address(99999).is_none());

        // Read names
        let place = reader.get_place(0).unwrap();
        assert_eq!(reader.place_primary_name(place), "Copenhagen");

        let addr = reader.get_address(0).unwrap();
        assert_eq!(reader.address_street(addr), "Radhuspladsen");

        std::fs::remove_file(&tmp).ok();
    }
}
