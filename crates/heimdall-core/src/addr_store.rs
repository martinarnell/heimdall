/// addr_store.rs — Street-grouped address storage
///
/// Instead of one record per address, stores one record per street segment
/// (street name + municipality). House numbers within a street are stored
/// as delta-encoded coordinates from the street's base point.
///
/// Version 1 (legacy): fixed 7-byte HouseEntry
/// Version 2 (prev):   varint-encoded house entries (~27% smaller than v1)
/// Version 4:          whole-file columnar + single zstd frame (eager decompress)
/// Version 5 (current): block-compressed mmap, decompress on demand, zero heap
///
/// Layout on disk (v1, v2):
///   [u32 magic: 0x53544144]  "STAD" (Street ADdress)
///   [u32 version: 1 or 2]
///   [u32 street_count]
///   [u32 string_pool_offset]
///   [StreetHeader * street_count]
///   [house entries region]
///   [string_pool]
///
/// Layout on disk (v4):
///   [u32 magic]  [u32 version=4]  [u32 street_count]  [u32 total_houses]
///   [u32 compressed_size]  [u32 decompressed_size]
///   [zstd compressed payload containing all columnar data + string pool]
///
/// Layout on disk (v5):
///   Header (48 bytes):
///     [u32 magic]  [u32 version=5]  [u32 street_count]  [u32 total_houses]
///     [u32 street_block_size]  [u32 street_block_count]
///     [u32 house_block_size]   [u32 house_block_count]
///     [u64 street_blocks_offset]  [u64 house_blocks_offset]  [u64 sp_offset]
///   Street Block Directory (street_block_count * 8):
///     [u32 rel_offset, u32 compressed_size] per block
///   House Block Directory (house_block_count * 8):
///     [u32 rel_offset, u32 compressed_size] per block
///   Compressed street blocks (LZ4, each holds sequential StreetHeader structs)
///   Compressed house blocks (LZ4, each holds sequential 7-byte house entries)
///   Uncompressed string pool

use std::path::Path;
use std::fs::File;
use std::sync::Mutex;
use memmap2::Mmap;

use crate::types::Coord;
use crate::error::HeimdallError;

// ---------------------------------------------------------------------------
// Block cache — avoids repeated LZ4 decompression of the same block
// ---------------------------------------------------------------------------

/// Small ring-buffer cache for decompressed V5 blocks.
/// Keyed by (block_type, block_index) where block_type 0=street, 1=house.
const BLOCK_CACHE_SIZE: usize = 8;

struct BlockCacheEntry {
    block_type: u8,
    block_idx: u32,
    data: Vec<u8>,
}

struct BlockCache {
    entries: Vec<BlockCacheEntry>,
    next: usize,
}

impl BlockCache {
    fn new() -> Self {
        Self { entries: Vec::with_capacity(BLOCK_CACHE_SIZE), next: 0 }
    }

    fn get(&self, block_type: u8, block_idx: u32) -> Option<&[u8]> {
        self.entries.iter()
            .find(|e| e.block_type == block_type && e.block_idx == block_idx)
            .map(|e| e.data.as_slice())
    }

    fn insert(&mut self, block_type: u8, block_idx: u32, data: Vec<u8>) {
        if self.entries.len() < BLOCK_CACHE_SIZE {
            self.entries.push(BlockCacheEntry { block_type, block_idx, data });
        } else {
            let slot = self.next % BLOCK_CACHE_SIZE;
            self.entries[slot] = BlockCacheEntry { block_type, block_idx, data };
            self.next = slot + 1;
        }
    }
}

const ADDR_STORE_MAGIC: u32 = 0x53544144;
const HEADER_SIZE: usize = 16;
const HOUSE_ENTRY_SIZE_V1: usize = 7;
const V4_HEADER_SIZE: usize = 24;
const V4_NUM_SECTIONS: usize = 7;
const V5_HEADER_SIZE: usize = 56;
const V5_DEFAULT_BLOCK_SIZE: u32 = 65536;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct StreetHeader {
    pub street_name_offset: u32,
    pub base_lat: i32,
    pub base_lon: i32,
    pub postcode: u16,
    pub house_start: u32,
    pub house_count: u16,
}

const STREET_HEADER_SIZE: usize = std::mem::size_of::<StreetHeader>();

#[derive(Debug, Clone, Copy)]
pub struct HouseEntry {
    pub number: u16,
    pub suffix: u8,
    pub delta_lat: i16,
    pub delta_lon: i16,
}

// ---------------------------------------------------------------------------
// Varint encoding/decoding
// ---------------------------------------------------------------------------

pub fn encode_varint(mut val: u64, buf: &mut Vec<u8>) -> usize {
    let start = buf.len();
    loop {
        if val < 0x80 { buf.push(val as u8); break; }
        buf.push((val as u8 & 0x7F) | 0x80);
        val >>= 7;
    }
    buf.len() - start
}

#[inline]
pub fn decode_varint(data: &[u8], pos: &mut usize) -> u64 {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = data[*pos]; *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 { break; }
        shift += 7;
    }
    result
}

#[inline]
pub fn zigzag_encode(val: i16) -> u16 {
    ((val as i32) << 1 ^ ((val as i32) >> 15)) as u16
}

#[inline]
pub fn zigzag_decode(val: u16) -> i16 {
    ((val >> 1) as i16) ^ -((val & 1) as i16)
}

#[inline]
fn zigzag_encode_i32(val: i32) -> u32 {
    ((val as i64) << 1 ^ ((val as i64) >> 31)) as u32
}

#[inline]
fn zigzag_decode_i32(val: u32) -> i32 {
    ((val >> 1) as i32) ^ -((val & 1) as i32)
}

// ---------------------------------------------------------------------------
// V4 in-memory data (eager decompression — ~1GB heap for US)
// ---------------------------------------------------------------------------

struct V4Data {
    streets: Vec<StreetHeader>,
    numbers: Vec<u16>,
    suffixes: Vec<u8>,
    delta_lats: Vec<i16>,
    delta_lons: Vec<i16>,
    string_pool: Vec<u8>,
}

// ---------------------------------------------------------------------------
// V5 metadata (zero heap — everything stays mmap'd or decompressed on demand)
// ---------------------------------------------------------------------------

struct V5Meta {
    total_houses: u32,
    street_block_size: u32,
    street_block_count: u32,
    house_block_size: u32,
    house_block_count: u32,
    street_dir_offset: usize,
    house_dir_offset: usize,
    street_blocks_offset: u64,
    house_blocks_offset: u64,
    sp_offset: u64,
}

// ---------------------------------------------------------------------------
// Runtime store (reader)
// ---------------------------------------------------------------------------

pub struct AddrStore {
    mmap: Mmap,
    version: u32,
    street_count: u32,
    string_pool_offset: u32,
    house_entries_offset: usize,
    v4_data: Option<V4Data>,
    v5_meta: Option<V5Meta>,
    /// Per-store block cache — avoids repeated LZ4 decompressions during
    /// sequential street lookups (range scans hit the same block many times).
    block_cache: Mutex<BlockCache>,
}

impl AddrStore {
    pub fn open(path: &Path) -> Result<Option<Self>, HeimdallError> {
        if !path.exists() { return Ok(None); }
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let magic = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
        if magic != ADDR_STORE_MAGIC {
            return Err(HeimdallError::Build(format!("invalid addr store magic: {:#x}", magic)));
        }
        let version = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
        if version != 1 && version != 2 && version != 4 && version != 5 {
            return Err(HeimdallError::Build(format!("unsupported addr store version: {}", version)));
        }
        let street_count = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version == 5 {
            // V5: block-compressed mmap, zero heap allocation
            if mmap.len() < V5_HEADER_SIZE {
                return Err(HeimdallError::Build("V5 file too small for header".into()));
            }
            let total_houses = u32::from_le_bytes(mmap[12..16].try_into().unwrap());
            let street_block_size = u32::from_le_bytes(mmap[16..20].try_into().unwrap());
            let street_block_count = u32::from_le_bytes(mmap[20..24].try_into().unwrap());
            let house_block_size = u32::from_le_bytes(mmap[24..28].try_into().unwrap());
            let house_block_count = u32::from_le_bytes(mmap[28..32].try_into().unwrap());
            let street_blocks_offset = u64::from_le_bytes(mmap[32..40].try_into().unwrap());
            let house_blocks_offset = u64::from_le_bytes(mmap[40..48].try_into().unwrap());
            // 8 u32 fields (32 bytes) + 3 u64 fields (24 bytes) = 56 byte header
            let sp_offset = u64::from_le_bytes(mmap[48..56].try_into().unwrap());

            let street_dir_offset = 56; // directories start right after the 56-byte header
            let house_dir_offset = street_dir_offset + (street_block_count as usize) * 8;

            let v5_meta = V5Meta {
                total_houses,
                street_block_size,
                street_block_count,
                house_block_size,
                house_block_count,
                street_dir_offset,
                house_dir_offset,
                street_blocks_offset,
                house_blocks_offset,
                sp_offset,
            };
            Ok(Some(Self {
                mmap, version, street_count,
                string_pool_offset: 0, house_entries_offset: 0,
                v4_data: None, v5_meta: Some(v5_meta),
                block_cache: Mutex::new(BlockCache::new()),
            }))
        } else if version == 4 {
            let total_houses = u32::from_le_bytes(mmap[12..16].try_into().unwrap()) as usize;
            let compressed_size = u32::from_le_bytes(mmap[16..20].try_into().unwrap()) as usize;
            let decompressed_size = u32::from_le_bytes(mmap[20..24].try_into().unwrap()) as usize;
            let payload = zstd::bulk::decompress(
                &mmap[V4_HEADER_SIZE..V4_HEADER_SIZE + compressed_size], decompressed_size,
            ).map_err(|e| HeimdallError::Build(format!("zstd decompress v4: {}", e)))?;
            let v4_data = decode_v4_payload(&payload, street_count as usize, total_houses)?;
            Ok(Some(Self {
                mmap, version, street_count,
                string_pool_offset: 0, house_entries_offset: 0,
                v4_data: Some(v4_data), v5_meta: None,
                block_cache: Mutex::new(BlockCache::new()),
            }))
        } else {
            let string_pool_offset = u32::from_le_bytes(mmap[12..16].try_into().unwrap());
            let house_entries_offset = HEADER_SIZE + (street_count as usize) * STREET_HEADER_SIZE;
            Ok(Some(Self {
                mmap, version, street_count,
                string_pool_offset, house_entries_offset,
                v4_data: None, v5_meta: None,
                block_cache: Mutex::new(BlockCache::new()),
            }))
        }
    }

    pub fn street_count(&self) -> usize { self.street_count as usize }

    pub fn get_street(&self, id: u32) -> Option<StreetHeader> {
        if id >= self.street_count { return None; }
        match self.version {
            5 => self.get_street_v5(id),
            4 => Some(self.v4_data.as_ref().unwrap().streets[id as usize]),
            _ => {
                let offset = HEADER_SIZE + (id as usize) * STREET_HEADER_SIZE;
                Some(unsafe { *(self.mmap[offset..].as_ptr() as *const StreetHeader) })
            }
        }
    }

    fn get_street_v5(&self, id: u32) -> Option<StreetHeader> {
        let meta = self.v5_meta.as_ref().unwrap();
        let byte_offset = id as usize * STREET_HEADER_SIZE;
        let block_idx = byte_offset / meta.street_block_size as usize;
        let local_offset = byte_offset % meta.street_block_size as usize;

        if block_idx >= meta.street_block_count as usize { return None; }

        let decompressed = self.decompress_block_cached(0, block_idx as u32, meta.street_dir_offset, meta.street_blocks_offset)?;

        if local_offset + STREET_HEADER_SIZE > decompressed.len() { return None; }
        let ptr = decompressed[local_offset..].as_ptr() as *const StreetHeader;
        Some(unsafe { *ptr })
    }

    /// Decompress a V5 block, using the cache to avoid repeated decompression.
    /// block_type: 0=street, 1=house
    fn decompress_block_cached(&self, block_type: u8, block_idx: u32, dir_offset: usize, blocks_offset: u64) -> Option<Vec<u8>> {
        // Check cache first
        {
            let cache = self.block_cache.lock().unwrap();
            if let Some(data) = cache.get(block_type, block_idx) {
                return Some(data.to_vec());
            }
        }

        // Cache miss — decompress (outside lock)
        let dir_off = dir_offset + block_idx as usize * 8;
        let rel_off = u32::from_le_bytes(self.mmap[dir_off..dir_off + 4].try_into().unwrap()) as usize;
        let comp_size = u32::from_le_bytes(self.mmap[dir_off + 4..dir_off + 8].try_into().unwrap()) as usize;

        let block_start = blocks_offset as usize + rel_off;
        let compressed = &self.mmap[block_start..block_start + comp_size];
        let decompressed = lz4_flex::decompress_size_prepended(compressed).ok()?;

        // Insert into cache
        let result = decompressed.clone();
        {
            let mut cache = self.block_cache.lock().unwrap();
            cache.insert(block_type, block_idx, decompressed);
        }

        Some(result)
    }

    pub fn street_name(&self, header: &StreetHeader) -> &str {
        if self.version == 5 {
            let sp_off = self.v5_meta.as_ref().unwrap().sp_offset as usize;
            let pool = &self.mmap[sp_off..];
            let off = header.street_name_offset as usize;
            if off >= pool.len() { return ""; }
            let len = pool[off] as usize;
            if off + 1 + len > pool.len() { return ""; }
            std::str::from_utf8(&pool[off + 1..off + 1 + len]).unwrap_or("")
        } else if self.version == 4 {
            let pool = &self.v4_data.as_ref().unwrap().string_pool;
            let off = header.street_name_offset as usize;
            if off >= pool.len() { return ""; }
            let len = pool[off] as usize;
            if off + 1 + len > pool.len() { return ""; }
            std::str::from_utf8(&pool[off + 1..off + 1 + len]).unwrap_or("")
        } else {
            let pool = &self.mmap[self.string_pool_offset as usize..];
            let off = header.street_name_offset as usize;
            let len = pool[off] as usize;
            std::str::from_utf8(&pool[off + 1..off + 1 + len]).unwrap_or("")
        }
    }

    pub fn street_houses(&self, header: &StreetHeader) -> Vec<HouseEntry> {
        if header.house_count == 0 { return vec![]; }
        match self.version {
            1 => self.street_houses_v1(header),
            2 => self.street_houses_v2(header),
            4 => self.street_houses_v4(header),
            5 => self.street_houses_v5(header),
            _ => vec![],
        }
    }

    fn street_houses_v1(&self, header: &StreetHeader) -> Vec<HouseEntry> {
        let mut entries = Vec::with_capacity(header.house_count as usize);
        for i in 0..header.house_count {
            let offset = self.house_entries_offset + (header.house_start + i as u32) as usize * HOUSE_ENTRY_SIZE_V1;
            if offset + HOUSE_ENTRY_SIZE_V1 > self.string_pool_offset as usize { break; }
            let d = &self.mmap[offset..];
            entries.push(HouseEntry {
                number: u16::from_le_bytes([d[0], d[1]]), suffix: d[2],
                delta_lat: i16::from_le_bytes([d[3], d[4]]), delta_lon: i16::from_le_bytes([d[5], d[6]]),
            });
        }
        entries
    }

    fn street_houses_v2(&self, header: &StreetHeader) -> Vec<HouseEntry> {
        let mut entries = Vec::with_capacity(header.house_count as usize);
        let region_start = self.house_entries_offset + header.house_start as usize;
        let region_end = self.string_pool_offset as usize;
        let data = &self.mmap[..region_end];
        let mut pos = region_start;
        for _ in 0..header.house_count {
            if pos >= region_end { break; }
            let number = decode_varint(data, &mut pos) as u16;
            let suffix = data[pos]; pos += 1;
            let delta_lat = zigzag_decode(decode_varint(data, &mut pos) as u16);
            let delta_lon = zigzag_decode(decode_varint(data, &mut pos) as u16);
            entries.push(HouseEntry { number, suffix, delta_lat, delta_lon });
        }
        entries
    }

    fn street_houses_v4(&self, header: &StreetHeader) -> Vec<HouseEntry> {
        let data = self.v4_data.as_ref().unwrap();
        let start = header.house_start as usize;
        let count = header.house_count as usize;
        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let idx = start + i;
            entries.push(HouseEntry {
                number: data.numbers[idx], suffix: data.suffixes[idx],
                delta_lat: data.delta_lats[idx], delta_lon: data.delta_lons[idx],
            });
        }
        entries
    }

    fn street_houses_v5(&self, header: &StreetHeader) -> Vec<HouseEntry> {
        let meta = self.v5_meta.as_ref().unwrap();
        let count = header.house_count as usize;
        let mut entries = Vec::with_capacity(count);

        // House entries are 7 bytes each, sequential starting at house_start
        let first_byte_offset = header.house_start as usize * HOUSE_ENTRY_SIZE_V1;
        let last_byte_offset = first_byte_offset + (count - 1) * HOUSE_ENTRY_SIZE_V1;
        let first_block = first_byte_offset / meta.house_block_size as usize;
        let last_block = last_byte_offset / meta.house_block_size as usize;

        // Decompress each needed block (cached) and read entries from it
        for blk in first_block..=last_block {
            if blk >= meta.house_block_count as usize { break; }

            let decompressed = match self.decompress_block_cached(
                1, blk as u32, meta.house_dir_offset, meta.house_blocks_offset,
            ) {
                Some(d) => d,
                None => break,
            };

            // Determine which entries fall in this block
            let block_byte_start = blk * meta.house_block_size as usize;
            let block_byte_end = block_byte_start + decompressed.len();

            // Entry range within the overall house array that overlaps this block
            let range_start = if first_byte_offset > block_byte_start {
                first_byte_offset
            } else {
                block_byte_start
            };
            let range_end = std::cmp::min(
                first_byte_offset + count * HOUSE_ENTRY_SIZE_V1,
                block_byte_end,
            );

            let mut off = range_start;
            while off + HOUSE_ENTRY_SIZE_V1 <= range_end {
                let local = off - block_byte_start;
                let d = &decompressed[local..];
                entries.push(HouseEntry {
                    number: u16::from_le_bytes([d[0], d[1]]),
                    suffix: d[2],
                    delta_lat: i16::from_le_bytes([d[3], d[4]]),
                    delta_lon: i16::from_le_bytes([d[5], d[6]]),
                });
                off += HOUSE_ENTRY_SIZE_V1;
            }
        }
        entries
    }

    /// Decompress a single V5 house block by index. Returns the raw decompressed bytes.
    #[allow(dead_code)]
    fn decompress_house_block_v5(&self, block_idx: usize) -> Option<Vec<u8>> {
        let meta = self.v5_meta.as_ref()?;
        if block_idx >= meta.house_block_count as usize { return None; }
        let dir_off = meta.house_dir_offset + block_idx * 8;
        let rel_off = u32::from_le_bytes(self.mmap[dir_off..dir_off + 4].try_into().unwrap()) as usize;
        let comp_size = u32::from_le_bytes(self.mmap[dir_off + 4..dir_off + 8].try_into().unwrap()) as usize;
        let block_start = meta.house_blocks_offset as usize + rel_off;
        lz4_flex::decompress_size_prepended(&self.mmap[block_start..block_start + comp_size]).ok()
    }

    pub fn get_house(&self, index: u32) -> Option<HouseEntry> {
        if self.version != 1 { return None; }
        let offset = self.house_entries_offset + (index as usize) * HOUSE_ENTRY_SIZE_V1;
        if offset + HOUSE_ENTRY_SIZE_V1 > self.string_pool_offset as usize { return None; }
        let d = &self.mmap[offset..];
        Some(HouseEntry {
            number: u16::from_le_bytes([d[0], d[1]]), suffix: d[2],
            delta_lat: i16::from_le_bytes([d[3], d[4]]), delta_lon: i16::from_le_bytes([d[5], d[6]]),
        })
    }

    pub fn find_house(&self, street_id: u32, number: u16, suffix: u8) -> Option<Coord> {
        let header = self.get_street(street_id)?;
        if header.house_count == 0 {
            return Some(Coord { lat: header.base_lat, lon: header.base_lon });
        }
        let houses = self.street_houses(&header);

        // Try exact match (number + suffix)
        for e in &houses {
            if e.number == number && (suffix == 0 || e.suffix == suffix) {
                return Some(Coord {
                    lat: header.base_lat + e.delta_lat as i32,
                    lon: header.base_lon + e.delta_lon as i32,
                });
            }
        }

        // Try just number (ignore suffix)
        for e in &houses {
            if e.number == number {
                return Some(Coord {
                    lat: header.base_lat + e.delta_lat as i32,
                    lon: header.base_lon + e.delta_lon as i32,
                });
            }
        }

        // Nearest number fallback
        let mut best: Option<&HouseEntry> = None;
        let mut best_diff = u16::MAX;
        for e in &houses {
            let diff = (e.number as i32 - number as i32).unsigned_abs() as u16;
            if diff < best_diff {
                best_diff = diff;
                best = Some(e);
            }
        }
        best.map(|e| Coord {
            lat: header.base_lat + e.delta_lat as i32,
            lon: header.base_lon + e.delta_lon as i32,
        })
    }

    pub fn total_houses(&self) -> usize {
        match self.version {
            1 => (self.string_pool_offset as usize - self.house_entries_offset) / HOUSE_ENTRY_SIZE_V1,
            4 => self.v4_data.as_ref().unwrap().numbers.len(),
            5 => self.v5_meta.as_ref().unwrap().total_houses as usize,
            _ => (0..self.street_count)
                .filter_map(|id| self.get_street(id))
                .map(|h| h.house_count as usize)
                .sum(),
        }
    }
}

// ---------------------------------------------------------------------------
// V4 payload decoder
// ---------------------------------------------------------------------------

fn decode_v4_payload(buf: &[u8], street_count: usize, total_houses: usize) -> Result<V4Data, HeimdallError> {
    let mut pos = 0;
    // Read 7 section sizes
    let name_offsets_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
    let base_lats_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
    let base_lons_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
    let postcodes_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
    let house_counts_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
    let numbers_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize; pos += 4;
    let coord_lats_size = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize; pos += 4;

    // Section 1: name offsets (delta-varint)
    let s = pos; let mut name_offsets = Vec::with_capacity(street_count);
    let mut p = s; let mut pv: u32 = 0;
    for _ in 0..street_count { let d = decode_varint(buf, &mut p) as u32; pv += d; name_offsets.push(pv); }
    pos = s + name_offsets_size;

    // Section 2: base lats (zigzag delta-varint)
    let s = pos; let mut base_lats = Vec::with_capacity(street_count);
    let mut p = s; let mut pv: i32 = 0;
    for _ in 0..street_count { let d = zigzag_decode_i32(decode_varint(buf, &mut p) as u32); pv += d; base_lats.push(pv); }
    pos = s + base_lats_size;

    // Section 3: base lons (zigzag delta-varint)
    let s = pos; let mut base_lons = Vec::with_capacity(street_count);
    let mut p = s; let mut pv: i32 = 0;
    for _ in 0..street_count { let d = zigzag_decode_i32(decode_varint(buf, &mut p) as u32); pv += d; base_lons.push(pv); }
    pos = s + base_lons_size;

    // Section 4: postcodes (raw u16)
    let s = pos; let mut postcodes = Vec::with_capacity(street_count);
    for i in 0..street_count { let o = s + i * 2; postcodes.push(u16::from_le_bytes(buf[o..o+2].try_into().unwrap())); }
    pos = s + postcodes_size;

    // Section 5: house counts (varint)
    let s = pos; let mut house_counts = Vec::with_capacity(street_count);
    let mut p = s;
    for _ in 0..street_count { house_counts.push(decode_varint(buf, &mut p) as u16); }
    pos = s + house_counts_size;

    // Reconstruct StreetHeader array
    let mut streets = Vec::with_capacity(street_count);
    let mut hidx: u32 = 0;
    for i in 0..street_count {
        streets.push(StreetHeader {
            street_name_offset: name_offsets[i], base_lat: base_lats[i], base_lon: base_lons[i],
            postcode: postcodes[i], house_start: hidx, house_count: house_counts[i],
        });
        hidx += house_counts[i] as u32;
    }

    // Section 6: house numbers (per-street delta-varint)
    let s = pos; let mut numbers = Vec::with_capacity(total_houses);
    let mut p = s;
    for st in &streets {
        let mut pv: u16 = 0;
        for _ in 0..st.house_count {
            let d = decode_varint(buf, &mut p) as u16;
            pv += d;
            numbers.push(pv);
        }
    }
    pos = s + numbers_size;

    // Suffixes: bitmap + sparse values
    let bm_bytes = (total_houses + 7) / 8;
    let bitmap = &buf[pos..pos + bm_bytes]; pos += bm_bytes;
    let sc: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
    let sv = &buf[pos..pos + sc]; pos += sc;
    let mut suffixes = Vec::with_capacity(total_houses);
    let mut vi = 0;
    for i in 0..total_houses {
        if (bitmap[i/8] >> (i%8)) & 1 == 1 { suffixes.push(sv[vi]); vi += 1; }
        else { suffixes.push(0); }
    }

    // Coord deltas: per-street delta-of-delta, byte-split i16
    let s = pos;
    let lat_lo = &buf[s..s + total_houses];
    let lat_hi = &buf[s + total_houses..s + total_houses * 2];
    let mut delta_lats = Vec::with_capacity(total_houses);
    let mut hi = 0;
    for st in &streets {
        let mut prev: i16 = 0;
        for _ in 0..st.house_count {
            let dd = i16::from_le_bytes([lat_lo[hi], lat_hi[hi]]);
            hi += 1;
            prev = prev.wrapping_add(dd);
            delta_lats.push(prev);
        }
    }
    pos = s + coord_lats_size;

    let s = pos;
    let lon_lo = &buf[s..s + total_houses];
    let lon_hi = &buf[s + total_houses..s + total_houses * 2];
    let mut delta_lons = Vec::with_capacity(total_houses);
    let mut hi = 0;
    for st in &streets {
        let mut prev: i16 = 0;
        for _ in 0..st.house_count {
            let dd = i16::from_le_bytes([lon_lo[hi], lon_hi[hi]]);
            hi += 1;
            prev = prev.wrapping_add(dd);
            delta_lons.push(prev);
        }
    }

    let string_pool_start = s + total_houses * 2;
    let string_pool = buf[string_pool_start..].to_vec();
    Ok(V4Data { streets, numbers, suffixes, delta_lats, delta_lons, string_pool })
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

pub struct AddrStoreBuilder {
    streets: Vec<StreetHeader>,
    houses: Vec<HouseEntry>,
    string_pool: Vec<u8>,
}

impl AddrStoreBuilder {
    pub fn new() -> Self {
        Self { streets: Vec::new(), houses: Vec::new(), string_pool: Vec::new() }
    }

    /// Add a street with its house numbers. Returns the street ID.
    pub fn add_street(
        &mut self,
        name: &str,
        base_lat: i32,
        base_lon: i32,
        postcode: u16,
        house_entries: &[(u16, u8, i32, i32)], // (number, suffix, lat, lon)
    ) -> u32 {
        let name_offset = self.string_pool.len() as u32;
        let nb = name.as_bytes();
        assert!(nb.len() < 256);
        self.string_pool.push(nb.len() as u8);
        self.string_pool.extend_from_slice(nb);

        let house_start = self.houses.len() as u32;
        let house_count = house_entries.len() as u16;

        for &(number, suffix, lat, lon) in house_entries {
            let delta_lat = ((lat - base_lat) as i16).max(-32767).min(32767);
            let delta_lon = ((lon - base_lon) as i16).max(-32767).min(32767);
            self.houses.push(HouseEntry { number, suffix, delta_lat, delta_lon });
        }

        let id = self.streets.len() as u32;
        self.streets.push(StreetHeader {
            street_name_offset: name_offset, base_lat, base_lon, postcode,
            house_start, house_count,
        });
        id
    }

    /// Write as v4 (whole-file columnar + single zstd frame).
    pub fn write(&self, path: &Path) -> Result<usize, HeimdallError> {
        use std::io::Write;
        let total_houses = self.houses.len();
        let street_count = self.streets.len();

        // Section 1: name offsets (delta-varint)
        let mut name_offsets_buf = Vec::with_capacity(street_count * 3);
        let mut pv: u32 = 0;
        for s in &self.streets {
            encode_varint((s.street_name_offset - pv) as u64, &mut name_offsets_buf);
            pv = s.street_name_offset;
        }

        // Section 2: base lats (zigzag delta-varint)
        let mut base_lats_buf = Vec::with_capacity(street_count * 4);
        let mut pv: i32 = 0;
        for s in &self.streets {
            encode_varint(zigzag_encode_i32(s.base_lat - pv) as u64, &mut base_lats_buf);
            pv = s.base_lat;
        }

        // Section 3: base lons (zigzag delta-varint)
        let mut base_lons_buf = Vec::with_capacity(street_count * 4);
        let mut pv: i32 = 0;
        for s in &self.streets {
            encode_varint(zigzag_encode_i32(s.base_lon - pv) as u64, &mut base_lons_buf);
            pv = s.base_lon;
        }

        // Section 4: postcodes (raw u16)
        let mut postcodes_buf = Vec::with_capacity(street_count * 2);
        for s in &self.streets { postcodes_buf.extend_from_slice(&s.postcode.to_le_bytes()); }

        // Section 5: house counts (varint)
        let mut house_counts_buf = Vec::with_capacity(street_count * 2);
        for s in &self.streets { encode_varint(s.house_count as u64, &mut house_counts_buf); }

        // Section 6: house numbers (per-street delta-varint)
        let mut numbers_buf = Vec::with_capacity(total_houses * 2);
        for s in &self.streets {
            let (start, end) = (s.house_start as usize, s.house_start as usize + s.house_count as usize);
            let mut pv: u16 = 0;
            for h in &self.houses[start..end] {
                encode_varint(h.number.saturating_sub(pv) as u64, &mut numbers_buf);
                pv = h.number;
            }
        }

        // Suffixes: bitmap (85.5% zero) + sparse values
        let bm_bytes = (total_houses + 7) / 8;
        let mut suffix_bitmap = vec![0u8; bm_bytes];
        let mut suffix_values = Vec::new();
        for (i, h) in self.houses.iter().enumerate() {
            if h.suffix != 0 {
                suffix_bitmap[i/8] |= 1 << (i%8);
                suffix_values.push(h.suffix);
            }
        }

        // Coord deltas: per-street delta-of-delta, byte-split i16
        let mut lat_dd_lo = Vec::with_capacity(total_houses);
        let mut lat_dd_hi = Vec::with_capacity(total_houses);
        let mut lon_dd_lo = Vec::with_capacity(total_houses);
        let mut lon_dd_hi = Vec::with_capacity(total_houses);
        for s in &self.streets {
            let (start, end) = (s.house_start as usize, s.house_start as usize + s.house_count as usize);
            let mut prev_lat: i16 = 0;
            let mut prev_lon: i16 = 0;
            for h in &self.houses[start..end] {
                let dd_lat = h.delta_lat.wrapping_sub(prev_lat);
                let dd_lon = h.delta_lon.wrapping_sub(prev_lon);
                prev_lat = h.delta_lat;
                prev_lon = h.delta_lon;
                let lat_bytes = dd_lat.to_le_bytes();
                let lon_bytes = dd_lon.to_le_bytes();
                lat_dd_lo.push(lat_bytes[0]);
                lat_dd_hi.push(lat_bytes[1]);
                lon_dd_lo.push(lon_bytes[0]);
                lon_dd_hi.push(lon_bytes[1]);
            }
        }
        let mut coord_lats_buf = Vec::with_capacity(total_houses * 2);
        coord_lats_buf.extend_from_slice(&lat_dd_lo);
        coord_lats_buf.extend_from_slice(&lat_dd_hi);
        let mut coord_lons_buf = Vec::with_capacity(total_houses * 2);
        coord_lons_buf.extend_from_slice(&lon_dd_lo);
        coord_lons_buf.extend_from_slice(&lon_dd_hi);

        // Assemble payload: 7 section sizes + all sections + string pool
        let ps = V4_NUM_SECTIONS * 4
            + name_offsets_buf.len() + base_lats_buf.len() + base_lons_buf.len()
            + postcodes_buf.len() + house_counts_buf.len() + numbers_buf.len()
            + suffix_bitmap.len() + suffix_values.len()
            + coord_lats_buf.len() + coord_lons_buf.len() + self.string_pool.len();
        let mut payload = Vec::with_capacity(ps);
        payload.extend_from_slice(&(name_offsets_buf.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(base_lats_buf.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(base_lons_buf.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(postcodes_buf.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(house_counts_buf.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(numbers_buf.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(coord_lats_buf.len() as u32).to_le_bytes());
        payload.extend_from_slice(&name_offsets_buf);
        payload.extend_from_slice(&base_lats_buf);
        payload.extend_from_slice(&base_lons_buf);
        payload.extend_from_slice(&postcodes_buf);
        payload.extend_from_slice(&house_counts_buf);
        payload.extend_from_slice(&numbers_buf);
        payload.extend_from_slice(&suffix_bitmap);
        payload.extend_from_slice(&suffix_values);
        payload.extend_from_slice(&coord_lats_buf);
        payload.extend_from_slice(&coord_lons_buf);
        payload.extend_from_slice(&self.string_pool);
        assert_eq!(payload.len(), ps);

        let compressed = zstd::bulk::compress(&payload, 19)
            .map_err(|e| HeimdallError::Build(format!("zstd compress v4: {}", e)))?;

        let mut f = std::io::BufWriter::new(File::create(path)?);
        f.write_all(&ADDR_STORE_MAGIC.to_le_bytes())?;
        f.write_all(&4u32.to_le_bytes())?;
        f.write_all(&(street_count as u32).to_le_bytes())?;
        f.write_all(&(total_houses as u32).to_le_bytes())?;
        f.write_all(&(compressed.len() as u32).to_le_bytes())?;
        f.write_all(&(payload.len() as u32).to_le_bytes())?;
        f.write_all(&compressed)?;
        Ok(V4_HEADER_SIZE + compressed.len())
    }

    /// Write as v5 (block-compressed mmap, decompress on demand, zero heap on read).
    ///
    /// Street headers are written as raw repr(C) StreetHeader (20 bytes each) in LZ4 blocks.
    /// House entries are written as packed 7-byte records (number:u16, suffix:u8,
    /// delta_lat:i16, delta_lon:i16) in LZ4 blocks.
    /// String pool is written uncompressed at the end (mmap'd directly on read).
    pub fn write_v5(&self, path: &Path) -> Result<usize, HeimdallError> {
        self.write_v5_with_block_size(path, V5_DEFAULT_BLOCK_SIZE)
    }

    /// Write V5 format with a custom (decompressed) block size.
    pub fn write_v5_with_block_size(&self, path: &Path, block_size: u32) -> Result<usize, HeimdallError> {
        use std::io::Write;
        let street_count = self.streets.len();
        let total_houses = self.houses.len();

        // --- Serialize street headers as raw bytes ---
        let street_bytes_total = street_count * STREET_HEADER_SIZE;
        let mut street_raw = Vec::with_capacity(street_bytes_total);
        for s in &self.streets {
            street_raw.extend_from_slice(unsafe {
                std::slice::from_raw_parts(s as *const StreetHeader as *const u8, STREET_HEADER_SIZE)
            });
        }

        // --- Serialize house entries as packed 7-byte records ---
        let house_bytes_total = total_houses * HOUSE_ENTRY_SIZE_V1;
        let mut house_raw = Vec::with_capacity(house_bytes_total);
        for h in &self.houses {
            house_raw.extend_from_slice(&h.number.to_le_bytes());
            house_raw.push(h.suffix);
            house_raw.extend_from_slice(&h.delta_lat.to_le_bytes());
            house_raw.extend_from_slice(&h.delta_lon.to_le_bytes());
        }

        // --- Block-compress streets ---
        let street_block_count = if street_bytes_total == 0 { 0 } else {
            (street_bytes_total + block_size as usize - 1) / block_size as usize
        };
        let mut street_compressed_blocks: Vec<Vec<u8>> = Vec::with_capacity(street_block_count);
        for i in 0..street_block_count {
            let start = i * block_size as usize;
            let end = std::cmp::min(start + block_size as usize, street_bytes_total);
            let compressed = lz4_flex::compress_prepend_size(&street_raw[start..end]);
            street_compressed_blocks.push(compressed);
        }

        // --- Block-compress houses ---
        let house_block_count = if house_bytes_total == 0 { 0 } else {
            (house_bytes_total + block_size as usize - 1) / block_size as usize
        };
        let mut house_compressed_blocks: Vec<Vec<u8>> = Vec::with_capacity(house_block_count);
        for i in 0..house_block_count {
            let start = i * block_size as usize;
            let end = std::cmp::min(start + block_size as usize, house_bytes_total);
            let compressed = lz4_flex::compress_prepend_size(&house_raw[start..end]);
            house_compressed_blocks.push(compressed);
        }

        // --- Calculate offsets ---
        // Header: 56 bytes
        // Street dir: street_block_count * 8
        // House dir: house_block_count * 8
        // Street compressed blocks (sequential)
        // House compressed blocks (sequential)
        // String pool (uncompressed)

        let dirs_size = (street_block_count + house_block_count) * 8;
        let street_blocks_offset = (V5_HEADER_SIZE + dirs_size) as u64;

        let street_blocks_total_size: usize = street_compressed_blocks.iter().map(|b| b.len()).sum();
        let house_blocks_offset = street_blocks_offset + street_blocks_total_size as u64;

        let house_blocks_total_size: usize = house_compressed_blocks.iter().map(|b| b.len()).sum();
        let sp_offset = house_blocks_offset + house_blocks_total_size as u64;

        // --- Build block directories ---
        // Street block dir: (rel_offset: u32, compressed_size: u32)
        let mut street_dir = Vec::with_capacity(street_block_count * 8);
        let mut rel_off: u32 = 0;
        for blk in &street_compressed_blocks {
            street_dir.extend_from_slice(&rel_off.to_le_bytes());
            street_dir.extend_from_slice(&(blk.len() as u32).to_le_bytes());
            rel_off += blk.len() as u32;
        }

        // House block dir
        let mut house_dir = Vec::with_capacity(house_block_count * 8);
        let mut rel_off: u32 = 0;
        for blk in &house_compressed_blocks {
            house_dir.extend_from_slice(&rel_off.to_le_bytes());
            house_dir.extend_from_slice(&(blk.len() as u32).to_le_bytes());
            rel_off += blk.len() as u32;
        }

        // --- Write file ---
        let mut f = std::io::BufWriter::new(File::create(path)?);

        // Header (56 bytes)
        f.write_all(&ADDR_STORE_MAGIC.to_le_bytes())?;      // magic
        f.write_all(&5u32.to_le_bytes())?;                    // version
        f.write_all(&(street_count as u32).to_le_bytes())?;   // street_count
        f.write_all(&(total_houses as u32).to_le_bytes())?;   // total_houses
        f.write_all(&block_size.to_le_bytes())?;               // street_block_size
        f.write_all(&(street_block_count as u32).to_le_bytes())?; // street_block_count
        f.write_all(&block_size.to_le_bytes())?;               // house_block_size
        f.write_all(&(house_block_count as u32).to_le_bytes())?;  // house_block_count
        f.write_all(&street_blocks_offset.to_le_bytes())?;    // street_blocks_offset
        f.write_all(&house_blocks_offset.to_le_bytes())?;     // house_blocks_offset
        f.write_all(&sp_offset.to_le_bytes())?;               // sp_offset

        // Block directories
        f.write_all(&street_dir)?;
        f.write_all(&house_dir)?;

        // Compressed street blocks
        for blk in &street_compressed_blocks {
            f.write_all(blk)?;
        }

        // Compressed house blocks
        for blk in &house_compressed_blocks {
            f.write_all(blk)?;
        }

        // String pool (uncompressed)
        f.write_all(&self.string_pool)?;

        let total_size = sp_offset as usize + self.string_pool.len();
        Ok(total_size)
    }

    /// Write as v2 (varint-encoded house entries, no compression).
    pub fn write_v2(&self, path: &Path) -> Result<usize, HeimdallError> {
        use std::io::Write;
        let mut house_buf: Vec<u8> = Vec::with_capacity(self.houses.len() * 5);
        let mut offsets: Vec<u32> = Vec::with_capacity(self.streets.len());
        for s in &self.streets {
            offsets.push(house_buf.len() as u32);
            for h in &self.houses[s.house_start as usize..s.house_start as usize + s.house_count as usize] {
                encode_varint(h.number as u64, &mut house_buf);
                house_buf.push(h.suffix);
                encode_varint(zigzag_encode(h.delta_lat) as u64, &mut house_buf);
                encode_varint(zigzag_encode(h.delta_lon) as u64, &mut house_buf);
            }
        }
        let mut f = std::io::BufWriter::new(File::create(path)?);
        let sc = self.streets.len() as u32;
        let sb = self.streets.len() * STREET_HEADER_SIZE;
        let spo = (HEADER_SIZE + sb + house_buf.len()) as u32;
        f.write_all(&ADDR_STORE_MAGIC.to_le_bytes())?;
        f.write_all(&2u32.to_le_bytes())?;
        f.write_all(&sc.to_le_bytes())?;
        f.write_all(&spo.to_le_bytes())?;
        for (i, s) in self.streets.iter().enumerate() {
            let u = StreetHeader { house_start: offsets[i], ..*s };
            f.write_all(unsafe {
                std::slice::from_raw_parts(&u as *const StreetHeader as *const u8, STREET_HEADER_SIZE)
            })?;
        }
        f.write_all(&house_buf)?;
        f.write_all(&self.string_pool)?;
        Ok(HEADER_SIZE + sb + house_buf.len() + self.string_pool.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag_roundtrip() {
        for v in [-32767i16, -100, -1, 0, 1, 100, 32767] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    #[test]
    fn test_zigzag_i32_roundtrip() {
        for v in [-2_000_000_000i32, -100, -1, 0, 1, 100, 2_000_000_000] {
            assert_eq!(zigzag_decode_i32(zigzag_encode_i32(v)), v);
        }
    }

    #[test]
    fn test_varint_roundtrip() {
        for &v in &[0u64, 1, 127, 128, 255, 16383, 16384, 65535] {
            let mut b = Vec::new();
            encode_varint(v, &mut b);
            let mut p = 0;
            assert_eq!(decode_varint(&b, &mut p), v);
            assert_eq!(p, b.len());
        }
    }

    #[test]
    fn test_varint_sizes() {
        let mut buf = Vec::new();
        // 0-127: 1 byte
        encode_varint(0, &mut buf); assert_eq!(buf.len(), 1); buf.clear();
        encode_varint(127, &mut buf); assert_eq!(buf.len(), 1); buf.clear();
        // 128-16383: 2 bytes
        encode_varint(128, &mut buf); assert_eq!(buf.len(), 2); buf.clear();
        encode_varint(16383, &mut buf); assert_eq!(buf.len(), 2); buf.clear();
        // 16384-2097151: 3 bytes
        encode_varint(16384, &mut buf); assert_eq!(buf.len(), 3); buf.clear();
    }

    #[test]
    fn test_builder_v4_roundtrip() {
        let dir = std::env::temp_dir().join("heimdall_addr_v4_rt");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("addr_streets.bin");
        let mut b = AddrStoreBuilder::new();
        b.add_street("Testgatan", 59_330_000, 18_068_000, 11432,
            &[(1,0,59_330_050,18_068_100),(3,0,59_330_100,18_068_200),(5,1,59_330_150,18_068_300)]);
        b.add_street("Andra Gatan", 55_600_000, 13_000_000, 21100,
            &[(2,0,55_600_100,13_000_200),(4,2,55_600_200,13_000_300)]);
        b.write(&path).unwrap();
        let s = AddrStore::open(&path).unwrap().unwrap();
        assert_eq!(s.version, 4);
        assert_eq!(s.street_count(), 2);
        assert_eq!(s.total_houses(), 5);
        let h0 = s.get_street(0).unwrap();
        assert_eq!(s.street_name(&h0), "Testgatan");
        assert_eq!(s.find_house(0,1,0).unwrap().lat, 59_330_050);
        assert_eq!(s.find_house(0,5,1).unwrap().lon, 18_068_300);
        let h1 = s.get_street(1).unwrap();
        assert_eq!(s.street_name(&h1), "Andra Gatan");
        assert_eq!(s.find_house(1,4,2).unwrap().lat, 55_600_200);
        assert_eq!(s.street_houses(&h0)[2].suffix, 1);
        assert_eq!(s.street_houses(&h1)[1].suffix, 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_builder_v5_roundtrip() {
        let dir = std::env::temp_dir().join("heimdall_addr_v5_rt");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("addr_streets.bin");
        let mut b = AddrStoreBuilder::new();
        b.add_street("Testgatan", 59_330_000, 18_068_000, 11432,
            &[(1,0,59_330_050,18_068_100),(3,0,59_330_100,18_068_200),(5,1,59_330_150,18_068_300)]);
        b.add_street("Andra Gatan", 55_600_000, 13_000_000, 21100,
            &[(2,0,55_600_100,13_000_200),(4,2,55_600_200,13_000_300)]);
        b.write_v5(&path).unwrap();
        let s = AddrStore::open(&path).unwrap().unwrap();
        assert_eq!(s.version, 5);
        assert_eq!(s.street_count(), 2);
        assert_eq!(s.total_houses(), 5);
        let h0 = s.get_street(0).unwrap();
        assert_eq!(s.street_name(&h0), "Testgatan");
        assert_eq!(h0.base_lat, 59_330_000);
        assert_eq!(h0.base_lon, 18_068_000);
        assert_eq!(h0.postcode, 11432);
        assert_eq!(s.find_house(0,1,0).unwrap().lat, 59_330_050);
        assert_eq!(s.find_house(0,5,1).unwrap().lon, 18_068_300);
        let h1 = s.get_street(1).unwrap();
        assert_eq!(s.street_name(&h1), "Andra Gatan");
        assert_eq!(s.find_house(1,4,2).unwrap().lat, 55_600_200);
        let houses0 = s.street_houses(&h0);
        assert_eq!(houses0.len(), 3);
        assert_eq!(houses0[0].number, 1);
        assert_eq!(houses0[2].suffix, 1);
        let houses1 = s.street_houses(&h1);
        assert_eq!(houses1.len(), 2);
        assert_eq!(houses1[1].suffix, 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_builder_v5_small_blocks() {
        // Test with very small block size to exercise multi-block reads
        let dir = std::env::temp_dir().join("heimdall_addr_v5_small");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("addr_streets.bin");
        let mut b = AddrStoreBuilder::new();
        // Add enough streets to span multiple blocks (20 bytes each, block_size=40 => 2 per block)
        for i in 0..10u32 {
            let name = format!("Street {}", i);
            b.add_street(&name, 59_000_000 + i as i32 * 1000, 18_000_000 + i as i32 * 1000, 10000 + i as u16,
                &[(i as u16 * 2 + 1, 0, 59_000_050 + i as i32 * 1000, 18_000_050 + i as i32 * 1000),
                  (i as u16 * 2 + 2, 0, 59_000_100 + i as i32 * 1000, 18_000_100 + i as i32 * 1000)]);
        }
        // Use block_size=40 (2 street headers per block, ~2 house entries per block)
        b.write_v5_with_block_size(&path, 40).unwrap();
        let s = AddrStore::open(&path).unwrap().unwrap();
        assert_eq!(s.version, 5);
        assert_eq!(s.street_count(), 10);
        assert_eq!(s.total_houses(), 20);

        // Check first and last streets
        let h0 = s.get_street(0).unwrap();
        assert_eq!(s.street_name(&h0), "Street 0");
        assert_eq!(h0.base_lat, 59_000_000);

        let h9 = s.get_street(9).unwrap();
        assert_eq!(s.street_name(&h9), "Street 9");
        assert_eq!(h9.base_lat, 59_009_000);

        // Check houses span blocks correctly
        let houses9 = s.street_houses(&h9);
        assert_eq!(houses9.len(), 2);
        assert_eq!(houses9[0].number, 19);
        assert_eq!(houses9[1].number, 20);

        // Check find_house
        assert_eq!(s.find_house(5, 11, 0).unwrap().lat, 59_005_050);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_builder_v5_empty_houses() {
        // Test street with zero houses
        let dir = std::env::temp_dir().join("heimdall_addr_v5_empty");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("addr_streets.bin");
        let mut b = AddrStoreBuilder::new();
        b.add_street("Empty Street", 59_330_000, 18_068_000, 11432, &[]);
        b.add_street("Has Houses", 55_600_000, 13_000_000, 21100,
            &[(1,0,55_600_100,13_000_200)]);
        b.write_v5(&path).unwrap();
        let s = AddrStore::open(&path).unwrap().unwrap();
        assert_eq!(s.version, 5);
        assert_eq!(s.street_count(), 2);
        assert_eq!(s.total_houses(), 1);
        let h0 = s.get_street(0).unwrap();
        assert_eq!(s.street_name(&h0), "Empty Street");
        assert_eq!(s.street_houses(&h0).len(), 0);
        // find_house on empty street returns base coord
        assert_eq!(s.find_house(0, 1, 0).unwrap().lat, 59_330_000);
        let h1 = s.get_street(1).unwrap();
        assert_eq!(s.street_name(&h1), "Has Houses");
        assert_eq!(s.street_houses(&h1).len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_builder_v2_roundtrip() {
        let dir = std::env::temp_dir().join("heimdall_addr_v2_rt");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("addr_streets.bin");
        let mut b = AddrStoreBuilder::new();
        b.add_street("Testgatan", 59_330_000, 18_068_000, 11432,
            &[(1,0,59_330_050,18_068_100),(3,0,59_330_100,18_068_200),(5,1,59_330_150,18_068_300)]);
        b.write_v2(&path).unwrap();
        let s = AddrStore::open(&path).unwrap().unwrap();
        assert_eq!(s.version, 2);
        assert_eq!(s.total_houses(), 3);
        assert_eq!(s.find_house(0,5,1).unwrap().lat, 59_330_150);
        std::fs::remove_dir_all(&dir).ok();
    }
}
