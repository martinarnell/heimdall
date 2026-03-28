/// zip_index.rs — US ZIP code index
///
/// Binary format: fst_zip.fst + zip_records.bin
///
/// fst_zip.fst maps 5-digit ZIP string → record index (u64)
/// zip_records.bin stores ZipRecord entries with string pool.
///
/// Format v2:
///   [u32 magic: 0x5A495052]   "ZIPR"
///   [u32 count]
///   [u32 string_pool_offset]
///   [ZipEntry * count]        (20 bytes each)
///   [string pool]
///
/// ZipEntry (fixed 20 bytes):
///   lat: i32            — centroid latitude (microdegrees)
///   lon: i32            — centroid longitude
///   city_offset: u32    — byte offset into string pool
///   state_offset: u32   — byte offset into string pool
///   county_offset: u32  — byte offset into string pool
///
/// String pool entries: [u8 len][utf8 bytes]

use std::path::Path;
use std::fs::File;
use std::io::Write;
use memmap2::Mmap;
use fst::Map;

use crate::types::Coord;
use crate::error::HeimdallError;
use crate::compressed_io;

const MAGIC: u32 = 0x5A495052; // "ZIPR"
const ENTRY_SIZE_V2: usize = 20;
const ENTRY_SIZE_V1: usize = 16;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A US ZIP code record with centroid and admin names.
#[derive(Debug, Clone)]
pub struct ZipRecord {
    pub zip: String,
    pub coord: Coord,
    pub city: String,
    pub state: String,
    pub county: String,
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

pub struct ZipIndexBuilder {
    entries: Vec<(String, i32, i32, String, String, String)>, // zip, lat, lon, city, state, county
}

impl ZipIndexBuilder {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn add(&mut self, zip: &str, lat: i32, lon: i32, city: &str, state: &str, county: &str) {
        self.entries.push((
            zip.to_owned(),
            lat, lon,
            city.to_owned(),
            state.to_owned(),
            county.to_owned(),
        ));
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Write fst_zip.fst and zip_records.bin to the given directory.
    pub fn write(&mut self, dir: &Path) -> Result<(), HeimdallError> {
        // Sort by ZIP for FST (must be lexicographic)
        self.entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Deduplicate by ZIP (keep first)
        self.entries.dedup_by(|a, b| a.0 == b.0);

        // Build string pool with interning
        let mut pool = Vec::new();
        let mut intern: std::collections::HashMap<String, u32> = std::collections::HashMap::new();

        let intern_str = |pool: &mut Vec<u8>, s: &str, intern: &mut std::collections::HashMap<String, u32>| -> u32 {
            if let Some(&off) = intern.get(s) {
                return off;
            }
            let off = pool.len() as u32;
            let bytes = s.as_bytes();
            assert!(bytes.len() <= 255, "string too long for zip pool");
            pool.push(bytes.len() as u8);
            pool.extend_from_slice(bytes);
            intern.insert(s.to_owned(), off);
            off
        };

        // Collect entries with u32 offsets (no u16 limit)
        let mut offsets: Vec<(i32, i32, u32, u32, u32)> = Vec::with_capacity(self.entries.len());
        for (_, lat, lon, city, state, county) in &self.entries {
            let city_off = intern_str(&mut pool, city, &mut intern);
            let state_off = intern_str(&mut pool, state, &mut intern);
            let county_off = intern_str(&mut pool, county, &mut intern);
            offsets.push((*lat, *lon, city_off, state_off, county_off));
        }

        // Write zip_records.bin (v2: 20-byte entries with separate u32 offsets)
        let records_path = dir.join("zip_records.bin");
        let mut f = File::create(&records_path)?;

        let count = offsets.len() as u32;
        let header_size = 12u32; // magic + count + string_pool_offset
        let entries_size = (count as usize) * ENTRY_SIZE_V2;
        let string_pool_offset = header_size + entries_size as u32;

        f.write_all(&MAGIC.to_le_bytes())?;
        f.write_all(&count.to_le_bytes())?;
        f.write_all(&string_pool_offset.to_le_bytes())?;

        for &(lat, lon, city_off, state_off, county_off) in &offsets {
            f.write_all(&lat.to_le_bytes())?;
            f.write_all(&lon.to_le_bytes())?;
            f.write_all(&city_off.to_le_bytes())?;
            f.write_all(&state_off.to_le_bytes())?;
            f.write_all(&county_off.to_le_bytes())?;
        }

        // Write string pool
        f.write_all(&pool)?;
        drop(f);

        // Build FST: ZIP string → index
        let fst_path = dir.join("fst_zip.fst");
        let wtr = std::io::BufWriter::new(File::create(&fst_path)?);
        let mut fst_builder = fst::MapBuilder::new(wtr)
            .map_err(HeimdallError::Fst)?;
        for (i, (zip, ..)) in self.entries.iter().enumerate() {
            fst_builder.insert(zip.as_bytes(), i as u64)
                .map_err(HeimdallError::Fst)?;
        }
        fst_builder.finish().map_err(HeimdallError::Fst)?;

        // Compress the FST
        compressed_io::compress_file(&fst_path, 19)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

pub struct ZipIndex {
    fst: Map<Vec<u8>>,
    data: Mmap,
    count: u32,
    string_pool_offset: u32,
    entry_size: usize,
}

impl ZipIndex {
    pub fn open(dir: &Path) -> Result<Option<Self>, HeimdallError> {
        let fst_path = dir.join("fst_zip.fst");
        let data_path = dir.join("zip_records.bin");

        if !fst_path.exists() || !data_path.exists() {
            return Ok(None);
        }

        let fst = {
            let data = compressed_io::read_maybe_compressed(&fst_path)?;
            Map::new(data).map_err(HeimdallError::Fst)?
        };

        let data = {
            let file = File::open(&data_path)?;
            unsafe { Mmap::map(&file)? }
        };

        if data.len() < 12 {
            return Ok(None);
        }

        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Ok(None);
        }

        let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let string_pool_offset = u32::from_le_bytes(data[8..12].try_into().unwrap());

        // Auto-detect v1 (16-byte) vs v2 (20-byte) entries from pool offset
        let expected_v2 = 12 + (count as usize) * ENTRY_SIZE_V2;
        let entry_size = if string_pool_offset as usize == expected_v2 {
            ENTRY_SIZE_V2
        } else {
            ENTRY_SIZE_V1
        };

        Ok(Some(Self { fst, data, count, string_pool_offset, entry_size }))
    }

    /// Look up a 5-digit ZIP code. Returns structured ZipRecord.
    pub fn lookup(&self, zip: &str) -> Option<ZipRecord> {
        let idx = self.fst.get(zip.as_bytes())? as u32;
        if idx >= self.count { return None; }

        let entry_offset = 12 + (idx as usize) * self.entry_size;
        if entry_offset + self.entry_size > self.data.len() { return None; }

        let d = &self.data[entry_offset..];
        let lat = i32::from_le_bytes(d[0..4].try_into().ok()?);
        let lon = i32::from_le_bytes(d[4..8].try_into().ok()?);

        let (city_offset, state_offset, county_offset) = if self.entry_size == ENTRY_SIZE_V2 {
            let city = u32::from_le_bytes(d[8..12].try_into().ok()?);
            let state = u32::from_le_bytes(d[12..16].try_into().ok()?);
            let county = u32::from_le_bytes(d[16..20].try_into().ok()?);
            (city, state, county)
        } else {
            // V1 format: city u32, state_county packed u32
            let city = u32::from_le_bytes(d[8..12].try_into().ok()?);
            let state_county = u32::from_le_bytes(d[12..16].try_into().ok()?);
            let state = state_county & 0xFFFF;
            let county = state_county >> 16;
            (city, state, county)
        };

        let city = self.read_pool_string(city_offset)?;
        let state = self.read_pool_string(state_offset)?;
        let county = self.read_pool_string(county_offset)?;

        Some(ZipRecord {
            zip: zip.to_owned(),
            coord: Coord { lat, lon },
            city,
            state,
            county,
        })
    }

    /// Check if a query looks like a US ZIP code (exactly 5 digits).
    pub fn is_us_zip(s: &str) -> bool {
        let s = s.trim();
        s.len() == 5 && s.chars().all(|c| c.is_ascii_digit())
    }

    fn read_pool_string(&self, offset: u32) -> Option<String> {
        let abs = self.string_pool_offset as usize + offset as usize;
        if abs >= self.data.len() { return None; }
        let len = self.data[abs] as usize;
        if abs + 1 + len > self.data.len() { return None; }
        std::str::from_utf8(&self.data[abs+1..abs+1+len])
            .ok()
            .map(|s| s.to_owned())
    }
}
