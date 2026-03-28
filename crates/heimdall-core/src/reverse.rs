/// reverse.rs — Reverse geocoding via geohash spatial index
///
/// Compact grouped format: cells stored as a directory + delta-varint record IDs.
///
/// For a given (lat, lon):
///   1. Compute geohash at precision 6 (~1.2km x 0.6km cells)
///   2. Find all records in that cell + 8 neighbors (3x3 grid)
///   3. Compute haversine distance to each candidate
///   4. Return nearest N, filtered by zoom/place_type
///
/// Index format v2 (geohash_index.bin):
///   [u32 magic: 0x47484932]  "GHI2"
///   [u32 cell_count]
///   [u32 total_records]       (for stats/validation)
///   [CellEntry * cell_count]  (sorted by hash)
///     each CellEntry: [u64 hash][u32 data_offset][u16 id_count] = 14 bytes
///   [data region: delta-varint encoded record_ids per cell]
///
/// Delta-varint encoding:
///   Record IDs within each cell are sorted. We store the first ID as a raw varint,
///   then each subsequent ID as (delta from previous) varint-encoded.
///   This is very compact since nearby places often have nearby record IDs.

use std::path::Path;
use memmap2::Mmap;
use std::fs::File;

use crate::types::*;
use crate::record_store::RecordStore;
use crate::error::HeimdallError;

pub const GEOHASH_MAGIC: u32 = 0x47484932; // "GHI2" — version 2
const GEOHASH_MAGIC_V1: u32 = 0x47484958;  // "GHIX" — version 1 (legacy)
pub const GEOHASH_PRECISION: usize = 6;

/// A single entry in the geohash index (used internally during build)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct GeohashEntry {
    /// Geohash encoded as u64 (first 6 chars base-32 packed)
    pub hash: u64,
    /// Record ID in the PlaceRecord store
    pub record_id: u32,
}

/// Cell directory entry in the on-disk format (14 bytes)
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
struct CellEntry {
    hash: u64,
    data_offset: u32,
    id_count: u16,
}

const CELL_ENTRY_SIZE: usize = 14; // u64(8) + u32(4) + u16(2)
const HEADER_SIZE: usize = 12; // magic(4) + cell_count(4) + total_records(4)

// Legacy v1 constants
const V1_HEADER_SIZE: usize = 8;  // magic(4) + count(4)

/// Encode a geohash string (up to 12 chars) as a u64 for fast comparison
pub fn geohash_to_u64(s: &str) -> u64 {
    let mut val: u64 = 0;
    for (i, b) in s.bytes().enumerate() {
        if i >= 8 { break; } // max 8 chars in u64 at 5 bits each = 40 bits
        let c = match b {
            b'0'..=b'9' => b - b'0',
            b'b'..=b'h' => b - b'b' + 10,
            b'j'..=b'n' => b - b'j' + 17,
            b'p'..=b'z' => b - b'p' + 22,
            _ => 0,
        };
        val = (val << 5) | (c as u64);
    }
    // Left-shift remaining bits so shorter hashes sort before longer ones
    let chars = s.len().min(8);
    val <<= (8 - chars) * 5;
    val
}

// ---------------------------------------------------------------------------
// Varint helpers
// ---------------------------------------------------------------------------

/// Encode a u32 as a varint, return number of bytes written.
fn encode_varint(mut val: u32, buf: &mut Vec<u8>) {
    loop {
        if val < 0x80 {
            buf.push(val as u8);
            return;
        }
        buf.push((val as u8 & 0x7F) | 0x80);
        val >>= 7;
    }
}

/// Decode a varint from a byte slice, returning (value, bytes_consumed).
fn decode_varint(data: &[u8]) -> (u32, usize) {
    let mut val: u32 = 0;
    let mut shift = 0u32;
    for (i, &b) in data.iter().enumerate() {
        val |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            return (val, i + 1);
        }
        shift += 7;
        if shift >= 35 {
            // Overflow protection
            return (val, i + 1);
        }
    }
    (val, data.len())
}

// ---------------------------------------------------------------------------
// Index reader — supports both v1 (legacy) and v2 (compact) formats
// ---------------------------------------------------------------------------

enum IndexFormat {
    /// Legacy: flat sorted array of (hash, record_id)
    V1 { count: u32 },
    /// Compact: cell directory + delta-varint data
    V2 { cell_count: u32, total_records: u32 },
}

pub struct GeohashIndex {
    mmap: Mmap,
    format: IndexFormat,
}

impl GeohashIndex {
    pub fn open(path: &Path) -> Result<Self, HeimdallError> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        let magic = u32::from_le_bytes(mmap[0..4].try_into().unwrap());

        let format = match magic {
            GEOHASH_MAGIC => {
                // V2 compact format
                let cell_count = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
                let total_records = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
                IndexFormat::V2 { cell_count, total_records }
            }
            GEOHASH_MAGIC_V1 => {
                // V1 legacy format
                let count = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
                IndexFormat::V1 { count }
            }
            _ => {
                return Err(HeimdallError::Build(format!(
                    "invalid geohash index magic: {:#x}", magic
                )));
            }
        };

        Ok(Self { mmap, format })
    }

    pub fn len(&self) -> usize {
        match &self.format {
            IndexFormat::V1 { count } => *count as usize,
            IndexFormat::V2 { total_records, .. } => *total_records as usize,
        }
    }

    /// Find all record IDs in a given geohash cell.
    fn records_in_cell(&self, cell_hash: u64) -> Vec<u32> {
        match &self.format {
            IndexFormat::V1 { count } => self.records_in_cell_v1(cell_hash, *count),
            IndexFormat::V2 { cell_count, .. } => self.records_in_cell_v2(cell_hash, *cell_count),
        }
    }

    // -- V1 legacy reader --

    fn records_in_cell_v1(&self, cell_hash: u64, count: u32) -> Vec<u32> {
        let entries = self.entries_v1(count);
        let start = entries.partition_point(|e| e.hash < cell_hash);
        let mut ids = Vec::new();
        for i in start..entries.len() {
            if entries[i].hash != cell_hash {
                break;
            }
            ids.push(entries[i].record_id);
        }
        ids
    }

    fn entries_v1(&self, count: u32) -> &[GeohashEntry] {
        let data = &self.mmap[V1_HEADER_SIZE..];
        unsafe {
            std::slice::from_raw_parts(
                data.as_ptr() as *const GeohashEntry,
                count as usize,
            )
        }
    }

    // -- V2 compact reader --

    fn cell_directory(&self, cell_count: u32) -> &[u8] {
        let start = HEADER_SIZE;
        let end = start + cell_count as usize * CELL_ENTRY_SIZE;
        &self.mmap[start..end]
    }

    fn data_region_offset(&self, cell_count: u32) -> usize {
        HEADER_SIZE + cell_count as usize * CELL_ENTRY_SIZE
    }

    fn read_cell_entry(&self, cell_dir: &[u8], idx: usize) -> CellEntry {
        let offset = idx * CELL_ENTRY_SIZE;
        let hash = u64::from_le_bytes(cell_dir[offset..offset+8].try_into().unwrap());
        let data_offset = u32::from_le_bytes(cell_dir[offset+8..offset+12].try_into().unwrap());
        let id_count = u16::from_le_bytes(cell_dir[offset+12..offset+14].try_into().unwrap());
        CellEntry { hash, data_offset, id_count }
    }

    fn records_in_cell_v2(&self, cell_hash: u64, cell_count: u32) -> Vec<u32> {
        let cell_dir = self.cell_directory(cell_count);
        let n = cell_count as usize;

        // Binary search on cell directory
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry = self.read_cell_entry(cell_dir, mid);
            if entry.hash < cell_hash {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if lo >= n {
            return Vec::new();
        }

        let entry = self.read_cell_entry(cell_dir, lo);
        if entry.hash != cell_hash {
            return Vec::new();
        }

        // Decode delta-varint record IDs
        let data_start = self.data_region_offset(cell_count) + entry.data_offset as usize;
        let data = &self.mmap[data_start..];
        let count = entry.id_count as usize;
        let mut ids = Vec::with_capacity(count);
        let mut pos = 0usize;
        let mut prev_id: u32 = 0;

        for i in 0..count {
            let (delta, consumed) = decode_varint(&data[pos..]);
            pos += consumed;
            if i == 0 {
                prev_id = delta;
            } else {
                prev_id = prev_id.wrapping_add(delta);
            }
            ids.push(prev_id);
        }

        ids
    }

    /// Reverse geocode: find nearest places to (lat, lon).
    /// Returns (record_id, distance_m) pairs sorted by distance.
    pub fn nearest(
        &self,
        lat: f64,
        lon: f64,
        records: &RecordStore,
        max_results: usize,
        zoom: Option<u8>,
    ) -> Vec<(u32, f64)> {
        let coord = geo::Coord { x: lon, y: lat };
        let gh = match geohash::encode(coord, GEOHASH_PRECISION) {
            Ok(h) => h,
            Err(_) => return vec![],
        };

        // Get the 3x3 grid of cells
        let center_hash = geohash_to_u64(&gh);
        let mut cell_hashes = vec![center_hash];

        if let Ok(neighbors) = geohash::neighbors(&gh) {
            for n in [
                &neighbors.n, &neighbors.ne, &neighbors.e, &neighbors.se,
                &neighbors.s, &neighbors.sw, &neighbors.w, &neighbors.nw,
            ] {
                cell_hashes.push(geohash_to_u64(n));
            }
        }

        // Collect all candidates from the 3x3 grid
        let query_coord = Coord::new(lat, lon);
        let mut candidates: Vec<(u32, f64, PlaceType)> = Vec::new();

        for cell_hash in &cell_hashes {
            for record_id in self.records_in_cell(*cell_hash) {
                if let Ok(record) = records.get(record_id) {
                    if let Some(z) = zoom {
                        if !matches_zoom(record.place_type, z) {
                            continue;
                        }
                    }
                    let dist = query_coord.distance_m(&record.coord);
                    candidates.push((record_id, dist, record.place_type));
                }
            }
        }

        // Sort by (priority_tier, distance).
        candidates.sort_by(|a, b| {
            let a_pop = is_populated_type(a.2);
            let b_pop = is_populated_type(b.2);

            if zoom.map(|z| z <= 14).unwrap_or(false) && a_pop != b_pop {
                if a_pop && !b_pop {
                    if b.1 + 500.0 < a.1 {
                        return std::cmp::Ordering::Greater;
                    }
                    return std::cmp::Ordering::Less;
                }
                if !a_pop && b_pop {
                    if a.1 + 500.0 < b.1 {
                        return std::cmp::Ordering::Less;
                    }
                    return std::cmp::Ordering::Greater;
                }
            }

            a.1.partial_cmp(&b.1).unwrap()
        });

        candidates.truncate(max_results);
        candidates.into_iter().map(|(id, dist, _)| (id, dist)).collect()
    }
}

/// Is this a populated/built place (vs natural feature)?
fn is_populated_type(pt: PlaceType) -> bool {
    matches!(
        pt,
        PlaceType::Country | PlaceType::State | PlaceType::County
            | PlaceType::City | PlaceType::Town | PlaceType::Village
            | PlaceType::Suburb | PlaceType::Quarter | PlaceType::Neighbourhood
            | PlaceType::Hamlet | PlaceType::Farm
            | PlaceType::Airport | PlaceType::Station
    )
}

/// Does this place_type match the requested zoom level?
fn matches_zoom(pt: PlaceType, zoom: u8) -> bool {
    match zoom {
        0..=3 => matches!(pt, PlaceType::Country),
        4..=6 => matches!(pt, PlaceType::Country | PlaceType::State),
        7..=9 => matches!(pt, PlaceType::State | PlaceType::County | PlaceType::City),
        10..=11 => matches!(
            pt,
            PlaceType::City | PlaceType::Town | PlaceType::Lake | PlaceType::Island
        ),
        12..=13 => matches!(
            pt,
            PlaceType::City | PlaceType::Town | PlaceType::Village
                | PlaceType::Suburb | PlaceType::Lake | PlaceType::Island
        ),
        14..=15 => !matches!(pt, PlaceType::Country | PlaceType::State),
        _ => true, // zoom 16+ = everything
    }
}

// ---------------------------------------------------------------------------
// Builder (used by pack step) — writes compact v2 format
// ---------------------------------------------------------------------------

pub struct GeohashIndexBuilder {
    entries: Vec<GeohashEntry>,
}

impl GeohashIndexBuilder {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn add(&mut self, lat: f64, lon: f64, record_id: u32) {
        let coord = geo::Coord { x: lon, y: lat };
        if let Ok(gh) = geohash::encode(coord, GEOHASH_PRECISION) {
            self.entries.push(GeohashEntry {
                hash: geohash_to_u64(&gh),
                record_id,
            });
        }
    }

    /// Add a raw (hash, record_id) entry — used for converting from V1 format.
    pub fn add_raw(&mut self, hash: u64, record_id: u32) {
        self.entries.push(GeohashEntry { hash, record_id });
    }

    pub fn write(&mut self, path: &Path) -> Result<usize, HeimdallError> {
        use std::io::Write;

        // Sort by (geohash, record_id) for grouping and delta encoding
        self.entries.sort_by_key(|e| (e.hash, e.record_id));

        let total_records = self.entries.len() as u32;

        // Group entries by cell hash
        let mut cells: Vec<(u64, Vec<u32>)> = Vec::new();
        let mut i = 0;
        while i < self.entries.len() {
            let hash = self.entries[i].hash;
            let start = i;
            while i < self.entries.len() && self.entries[i].hash == hash {
                i += 1;
            }
            let ids: Vec<u32> = self.entries[start..i].iter().map(|e| e.record_id).collect();
            cells.push((hash, ids));
        }

        let cell_count = cells.len() as u32;

        // Build data region: delta-varint encode record IDs per cell
        let mut data_region: Vec<u8> = Vec::new();
        let mut cell_entries: Vec<(u64, u32, u16)> = Vec::with_capacity(cells.len());

        for (hash, ids) in &cells {
            let offset = data_region.len() as u32;
            let count = ids.len().min(u16::MAX as usize) as u16;

            // Delta-varint encode
            let mut prev: u32 = 0;
            for (j, &id) in ids.iter().enumerate() {
                if j == 0 {
                    encode_varint(id, &mut data_region);
                    prev = id;
                } else {
                    let delta = id.wrapping_sub(prev);
                    encode_varint(delta, &mut data_region);
                    prev = id;
                }
            }

            cell_entries.push((*hash, offset, count));
        }

        // Write the file
        let mut f = std::io::BufWriter::new(File::create(path)?);

        // Header
        f.write_all(&GEOHASH_MAGIC.to_le_bytes())?;
        f.write_all(&cell_count.to_le_bytes())?;
        f.write_all(&total_records.to_le_bytes())?;

        // Cell directory
        for &(hash, offset, count) in &cell_entries {
            f.write_all(&hash.to_le_bytes())?;
            f.write_all(&offset.to_le_bytes())?;
            f.write_all(&count.to_le_bytes())?;
        }

        // Data region
        f.write_all(&data_region)?;

        let total_bytes = HEADER_SIZE
            + cell_entries.len() * CELL_ENTRY_SIZE
            + data_region.len();

        Ok(total_bytes)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_varint_roundtrip() {
        let values = [0u32, 1, 127, 128, 255, 256, 16383, 16384, 65535, 100000, u32::MAX];
        for &val in &values {
            let mut buf = Vec::new();
            encode_varint(val, &mut buf);
            let (decoded, consumed) = decode_varint(&buf);
            assert_eq!(decoded, val, "roundtrip failed for {}", val);
            assert_eq!(consumed, buf.len(), "consumed mismatch for {}", val);
        }
    }

    #[test]
    fn test_varint_compactness() {
        let mut buf = Vec::new();
        encode_varint(0, &mut buf); assert_eq!(buf.len(), 1); buf.clear();
        encode_varint(127, &mut buf); assert_eq!(buf.len(), 1); buf.clear();
        encode_varint(128, &mut buf); assert_eq!(buf.len(), 2); buf.clear();
        encode_varint(16383, &mut buf); assert_eq!(buf.len(), 2); buf.clear();
        encode_varint(16384, &mut buf); assert_eq!(buf.len(), 3); buf.clear();
    }

    #[test]
    fn test_geohash_index_v2_roundtrip() {
        let tmp = std::env::temp_dir().join(format!(
            "test_geohash_v2_{}.bin", std::process::id()
        ));

        let test_points: Vec<(f64, f64, u32)> = vec![
            (55.6761, 12.5683, 0),   // Copenhagen
            (55.6762, 12.5684, 1),
            (55.6760, 12.5682, 2),
            (56.1629, 10.2039, 10),  // Aarhus
            (56.1630, 10.2040, 11),
            (55.3959, 10.3883, 20),  // Odense
        ];

        let mut builder = GeohashIndexBuilder::new();
        for &(lat, lon, id) in &test_points {
            builder.add(lat, lon, id);
        }

        let size = builder.write(&tmp).unwrap();
        assert!(size > 0);

        let index = GeohashIndex::open(&tmp).unwrap();
        assert_eq!(index.len(), test_points.len());

        for &(lat, lon, id) in &test_points {
            let coord = geo::Coord { x: lon, y: lat };
            let gh = geohash::encode(coord, GEOHASH_PRECISION).unwrap();
            let cell_hash = geohash_to_u64(&gh);
            let ids = index.records_in_cell(cell_hash);
            assert!(ids.contains(&id), "record {} not found in cell", id);
        }

        let mut cell_counts = std::collections::HashMap::new();
        for &(lat, lon, _) in &test_points {
            let coord = geo::Coord { x: lon, y: lat };
            let gh = geohash::encode(coord, GEOHASH_PRECISION).unwrap();
            *cell_counts.entry(geohash_to_u64(&gh)).or_insert(0usize) += 1;
        }
        assert!(cell_counts.values().any(|&c| c >= 2));

        assert!(index.records_in_cell(99999999).is_empty());

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_v2_smaller_than_v1() {
        let tmp_v2 = std::env::temp_dir().join("test_geohash_size_v2.bin");
        let tmp_v1 = std::env::temp_dir().join("test_geohash_size_v1.bin");

        let mut builder = GeohashIndexBuilder::new();
        for i in 0..100u32 {
            let lat = 55.6761 + (i as f64 * 0.0001);
            let lon = 12.5683 + (i as f64 * 0.0001);
            builder.add(lat, lon, i);
        }
        let v2_size = builder.write(&tmp_v2).unwrap();

        let v1_size = {
            let mut entries: Vec<GeohashEntry> = Vec::new();
            for i in 0..100u32 {
                let lat = 55.6761 + (i as f64 * 0.0001);
                let lon = 12.5683 + (i as f64 * 0.0001);
                let coord = geo::Coord { x: lon, y: lat };
                if let Ok(gh) = geohash::encode(coord, GEOHASH_PRECISION) {
                    entries.push(GeohashEntry { hash: geohash_to_u64(&gh), record_id: i });
                }
            }
            entries.sort_by_key(|e| e.hash);
            let count = entries.len() as u32;
            let mut f = std::io::BufWriter::new(File::create(&tmp_v1).unwrap());
            f.write_all(&GEOHASH_MAGIC_V1.to_le_bytes()).unwrap();
            f.write_all(&count.to_le_bytes()).unwrap();
            for entry in &entries {
                let bytes = unsafe {
                    std::slice::from_raw_parts(
                        entry as *const GeohashEntry as *const u8,
                        std::mem::size_of::<GeohashEntry>(),
                    )
                };
                f.write_all(bytes).unwrap();
            }
            drop(f);
            std::fs::metadata(&tmp_v1).unwrap().len() as usize
        };

        assert!(v2_size < v1_size, "V2 ({}) should be smaller than V1 ({})", v2_size, v1_size);

        std::fs::remove_file(&tmp_v2).ok();
        std::fs::remove_file(&tmp_v1).ok();
    }
}
