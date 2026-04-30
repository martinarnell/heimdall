/// Record store — columnar binary file of PlaceRecord fields + a string pool.
///
/// **Version 3** (block-compressed mmap, decompress on demand, zero heap):
///
/// Header (48 bytes):
///   [u32 magic: 0x484D444C]   "HMDL"
///   [u32 version: 3]
///   [u64 record_count]
///   [u32 record_block_size]    decompressed block size in bytes (default 65536)
///   [u32 record_block_count]
///   [u32 sp_block_size]        decompressed string pool block size (default 65536)
///   [u32 sp_block_count]
///   [u64 record_blocks_offset] byte offset to first compressed record block
///   [u64 sp_blocks_offset]     byte offset to first compressed SP block
///
/// Record Block Directory (record_block_count * 8 bytes):
///   Each entry: [u32 offset (relative to record_blocks_offset)][u32 compressed_size]
///
/// String Pool Block Directory (sp_block_count * 8 bytes):
///   Each entry: [u32 offset (relative to sp_blocks_offset)][u32 compressed_size]
///
/// Compressed Record Blocks:
///   Each block is LZ4-compressed (with prepended size). Decompressed, it contains
///   sequential PlaceRecord structs (24 bytes each). Last block may be partial.
///
/// Compressed String Pool Blocks:
///   Each block is LZ4-compressed (with prepended size). Decompressed, it contains
///   sequential string pool bytes.
///
/// On open: just mmap the file, read the 48-byte header. No decompression.
/// On get(): decompress the one record block needed (64KB → ~1-3KB compressed).
/// On primary_name(): decompress the one SP block needed.
///
/// **Version 2** (columnar + zstd, decompresses all on open — legacy):
///
/// [u32 magic: 0x484D444C]   "HMDL"
/// [u32 version: 2]
/// [u64 record_count]
/// [u32 num_columns: 8]
/// Column directory (num_columns entries):
///   [u64 column_offset][u32 compressed_size][u32 raw_size] per column
/// [u64 string_pool_compressed_offset]
/// [u32 string_pool_compressed_size]
/// [u32 string_pool_raw_size]
/// Compressed column data (zstd):
///   0: lat_col      (i32 * N, delta-encoded from previous value)
///   1: lon_col      (i32 * N, delta-encoded from previous value)
///   2: admin1_col   (u16 * N)
///   3: admin2_col   (u16 * N)
///   4: importance_col (u16 * N)
///   5: place_type_col (u8 * N)
///   6: flags_col    (u8 * N)
///   7: name_offset_col (u32 * N)
///   8: osm_id_col   (u32 * N)
/// Compressed string pool (zstd)
///
/// Also supports version 1 (legacy flat format) for backward compatibility.

use std::path::Path;
use std::sync::Mutex;
use memmap2::Mmap;
use std::fs::File;
use crate::types::{PlaceRecord, PlaceType, Coord};
use crate::error::HeimdallError;

// ---------------------------------------------------------------------------
// Block cache — avoids repeated LZ4 decompression of the same block
// ---------------------------------------------------------------------------

const BLOCK_CACHE_SIZE: usize = 4;

struct BlockCacheEntry {
    block_type: u8, // 0=record, 1=string_pool
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

const MAGIC: u32 = 0x484D444C; // "HMDL"
const VERSION_V1: u32 = 1;
const VERSION_V2: u32 = 2;
const VERSION_V3: u32 = 3;
const V1_HEADER_SIZE: usize = 4 + 4 + 8 + 8; // magic + version + count + pool_offset
const NUM_COLUMNS: u32 = 9;
// V2 header: magic(4) + version(4) + record_count(8) + num_columns(4)
//          + column_dir(num_columns * 16) + string_pool info(8+4+4)
const V2_HEADER_FIXED: usize = 4 + 4 + 8 + 4;
const V2_COL_DIR_ENTRY: usize = 8 + 4 + 4; // offset(u64) + compressed(u32) + raw(u32)
const V2_STRING_POOL_INFO: usize = 8 + 4 + 4; // offset(u64) + compressed(u32) + raw(u32)

// V3 header: 48 bytes total
// magic(4) + version(4) + record_count(8)
// + record_block_size(4) + record_block_count(4)
// + sp_block_size(4) + sp_block_count(4)
// + record_blocks_offset(8) + sp_blocks_offset(8)
const V3_HEADER_SIZE: usize = 48;
const V3_DEFAULT_BLOCK_SIZE: u32 = 65536;

pub struct RecordStore {
    /// Decompressed records (V2), mmap'd records (V1), or block metadata (V3)
    records: RecordData,
    record_count: u64,
    /// Decompressed or mmap'd string pool, or block metadata (V3)
    string_pool: StringPoolData,
    /// V3: holds the file mmap; V1/V2: None (mmap is inside RecordData/StringPoolData)
    mmap: Option<Mmap>,
    /// Block cache for V3 — avoids repeated LZ4 decompression
    block_cache: Mutex<BlockCache>,
}

enum RecordData {
    /// Version 1: mmap'd, records accessed via pointer cast
    Mmap { mmap: Mmap },
    /// Version 2: decompressed in memory
    Owned { records: Vec<PlaceRecord> },
    /// Version 3: block-compressed, decompress on demand
    BlockCompressed {
        record_block_size: u32,
        #[allow(dead_code)]
        record_block_count: u32,
        record_block_dir_offset: usize,
        record_blocks_offset: u64,
    },
}

enum StringPoolData {
    /// Version 1: pool is a slice of the mmap
    Mmap { mmap_offset: usize },
    /// Version 2: decompressed in memory
    Owned { pool: Vec<u8> },
    /// Version 3: block-compressed, decompress on demand
    BlockCompressed {
        sp_block_size: u32,
        sp_block_count: u32,
        sp_block_dir_offset: usize,
        sp_blocks_offset: u64,
    },
}

impl RecordStore {
    pub fn open(path: &Path) -> Result<Self, HeimdallError> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        // Validate magic
        let magic = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(HeimdallError::Build(format!(
                "invalid record store magic: {:#x}",
                magic
            )));
        }

        let version = u32::from_le_bytes(mmap[4..8].try_into().unwrap());

        match version {
            VERSION_V1 => Self::open_v1(mmap),
            VERSION_V2 => Self::open_v2(&mmap),
            VERSION_V3 => Self::open_v3(mmap),
            _ => Err(HeimdallError::Build(format!(
                "unsupported record store version: {}",
                version
            ))),
        }
    }

    fn open_v1(mmap: Mmap) -> Result<Self, HeimdallError> {
        let record_count = u64::from_le_bytes(mmap[8..16].try_into().unwrap());
        let string_pool_offset = u64::from_le_bytes(mmap[16..24].try_into().unwrap());

        Ok(Self {
            string_pool: StringPoolData::Mmap { mmap_offset: string_pool_offset as usize },
            records: RecordData::Mmap { mmap },
            record_count,
            mmap: None,
            block_cache: Mutex::new(BlockCache::new()),
        })
    }

    fn open_v2(mmap: &Mmap) -> Result<Self, HeimdallError> {
        let record_count = u64::from_le_bytes(mmap[8..16].try_into().unwrap());
        let num_columns = u32::from_le_bytes(mmap[16..20].try_into().unwrap());
        if num_columns != NUM_COLUMNS {
            return Err(HeimdallError::Build(format!(
                "expected {} columns, got {}", NUM_COLUMNS, num_columns
            )));
        }

        let n = record_count as usize;

        // Read column directory
        let dir_start = V2_HEADER_FIXED;
        let dir_end = dir_start + (num_columns as usize) * V2_COL_DIR_ENTRY;

        let mut col_offset = Vec::with_capacity(num_columns as usize);
        let mut col_compressed = Vec::with_capacity(num_columns as usize);
        let mut col_raw = Vec::with_capacity(num_columns as usize);

        for i in 0..num_columns as usize {
            let base = dir_start + i * V2_COL_DIR_ENTRY;
            let off = u64::from_le_bytes(mmap[base..base+8].try_into().unwrap());
            let comp = u32::from_le_bytes(mmap[base+8..base+12].try_into().unwrap());
            let raw = u32::from_le_bytes(mmap[base+12..base+16].try_into().unwrap());
            col_offset.push(off as usize);
            col_compressed.push(comp as usize);
            col_raw.push(raw as usize);
        }

        // Read string pool info
        let sp_info_start = dir_end;
        let sp_offset = u64::from_le_bytes(
            mmap[sp_info_start..sp_info_start+8].try_into().unwrap()
        ) as usize;
        let sp_compressed = u32::from_le_bytes(
            mmap[sp_info_start+8..sp_info_start+12].try_into().unwrap()
        ) as usize;
        let sp_raw = u32::from_le_bytes(
            mmap[sp_info_start+12..sp_info_start+16].try_into().unwrap()
        ) as usize;

        // Decompress each column
        let decompress = |idx: usize| -> Result<Vec<u8>, HeimdallError> {
            let start = col_offset[idx];
            let end = start + col_compressed[idx];
            let compressed = &mmap[start..end];
            zstd::bulk::decompress(compressed, col_raw[idx])
                .map_err(|e| HeimdallError::Build(format!("zstd decompress col {}: {}", idx, e)))
        };

        let lat_bytes = decompress(0)?;
        let lon_bytes = decompress(1)?;
        let admin1_bytes = decompress(2)?;
        let admin2_bytes = decompress(3)?;
        let importance_bytes = decompress(4)?;
        let place_type_bytes = decompress(5)?;
        let flags_bytes = decompress(6)?;
        let name_offset_bytes = decompress(7)?;
        let osm_id_bytes = decompress(8)?;

        // Decode delta-encoded lat/lon columns
        let lats = decode_delta_i32(&lat_bytes, n);
        let lons = decode_delta_i32(&lon_bytes, n);

        // Decode other columns
        let admin1s = decode_u16(&admin1_bytes, n);
        let admin2s = decode_u16(&admin2_bytes, n);
        let importances = decode_u16(&importance_bytes, n);
        let name_offsets = decode_u32(&name_offset_bytes, n);
        let osm_ids = decode_u32(&osm_id_bytes, n);

        // Reconstruct PlaceRecord array
        let mut records = Vec::with_capacity(n);
        for i in 0..n {
            let place_type = place_type_from_u8(place_type_bytes[i]);
            records.push(PlaceRecord {
                coord: Coord { lat: lats[i], lon: lons[i] },
                admin1_id: admin1s[i],
                admin2_id: admin2s[i],
                importance: importances[i],
                place_type,
                flags: flags_bytes[i],
                name_offset: name_offsets[i],
                osm_id: osm_ids[i],
            });
        }

        // Decompress string pool
        let sp_compressed_data = &mmap[sp_offset..sp_offset + sp_compressed];
        let string_pool = zstd::bulk::decompress(sp_compressed_data, sp_raw)
            .map_err(|e| HeimdallError::Build(format!("zstd decompress string pool: {}", e)))?;

        Ok(Self {
            records: RecordData::Owned { records },
            record_count,
            string_pool: StringPoolData::Owned { pool: string_pool },
            mmap: None,
            block_cache: Mutex::new(BlockCache::new()),
        })
    }

    /// Open V3 block-compressed format. Just reads the 48-byte header — zero decompression,
    /// zero heap allocation beyond the mmap itself.
    fn open_v3(mmap: Mmap) -> Result<Self, HeimdallError> {
        if mmap.len() < V3_HEADER_SIZE {
            return Err(HeimdallError::Build(
                "V3 record store too small for header".into(),
            ));
        }

        let record_count = u64::from_le_bytes(mmap[8..16].try_into().unwrap());
        let record_block_size = u32::from_le_bytes(mmap[16..20].try_into().unwrap());
        let record_block_count = u32::from_le_bytes(mmap[20..24].try_into().unwrap());
        let sp_block_size = u32::from_le_bytes(mmap[24..28].try_into().unwrap());
        let sp_block_count = u32::from_le_bytes(mmap[28..32].try_into().unwrap());
        let record_blocks_offset = u64::from_le_bytes(mmap[32..40].try_into().unwrap());
        let sp_blocks_offset = u64::from_le_bytes(mmap[40..48].try_into().unwrap());

        // Block directories follow immediately after the header
        let record_block_dir_offset = V3_HEADER_SIZE;
        let sp_block_dir_offset =
            record_block_dir_offset + record_block_count as usize * 8;

        // Validate that directory fits in the mmap
        let dirs_end = sp_block_dir_offset + sp_block_count as usize * 8;
        if dirs_end > mmap.len() {
            return Err(HeimdallError::Build(format!(
                "V3 block directories extend past file end ({} > {})",
                dirs_end,
                mmap.len()
            )));
        }

        Ok(Self {
            records: RecordData::BlockCompressed {
                record_block_size,
                record_block_count,
                record_block_dir_offset,
                record_blocks_offset,
            },
            record_count,
            string_pool: StringPoolData::BlockCompressed {
                sp_block_size,
                sp_block_count,
                sp_block_dir_offset,
                sp_blocks_offset,
            },
            mmap: Some(mmap),
            block_cache: Mutex::new(BlockCache::new()),
        })
    }

    pub fn len(&self) -> usize {
        self.record_count as usize
    }

    /// Get a PlaceRecord by its index. Returns by value (PlaceRecord is 24 bytes, Copy).
    /// V1/V2: O(1) with no allocation. V3: decompresses one 64KB block per call.
    pub fn get(&self, id: u32) -> Result<PlaceRecord, HeimdallError> {
        if id as u64 >= self.record_count {
            return Err(HeimdallError::RecordOutOfBounds(id));
        }
        match &self.records {
            RecordData::Mmap { mmap } => {
                let offset = V1_HEADER_SIZE + (id as usize) * std::mem::size_of::<PlaceRecord>();
                let record = unsafe {
                    *(mmap[offset..].as_ptr() as *const PlaceRecord)
                };
                Ok(record)
            }
            RecordData::Owned { records } => {
                Ok(records[id as usize])
            }
            RecordData::BlockCompressed {
                record_block_size,
                record_block_count: _,
                record_block_dir_offset,
                record_blocks_offset,
            } => {
                let record_size = std::mem::size_of::<PlaceRecord>(); // 24
                let byte_offset = id as usize * record_size;
                let block_idx = byte_offset / *record_block_size as usize;
                let local_offset = byte_offset % *record_block_size as usize;

                let decompressed = self.decompress_block_cached(
                    0, block_idx as u32, *record_block_dir_offset, *record_blocks_offset,
                )?;

                if local_offset + record_size > decompressed.len() {
                    return Err(HeimdallError::Build(format!(
                        "V3 record block {} too small: need {}+{}, have {}",
                        block_idx, local_offset, record_size, decompressed.len()
                    )));
                }
                let record = unsafe {
                    *(decompressed[local_offset..].as_ptr() as *const PlaceRecord)
                };
                Ok(record)
            }
        }
    }

    /// Decompress a V3 block, using the cache to avoid repeated decompression.
    fn decompress_block_cached(&self, block_type: u8, block_idx: u32, dir_offset: usize, blocks_offset: u64) -> Result<Vec<u8>, HeimdallError> {
        // Check cache
        {
            let cache = self.block_cache.lock().unwrap();
            if let Some(data) = cache.get(block_type, block_idx) {
                return Ok(data.to_vec());
            }
        }

        // Cache miss — decompress (outside lock)
        let mmap = self.mmap.as_ref().unwrap();
        let dir_off = dir_offset + block_idx as usize * 8;
        let rel_offset = u32::from_le_bytes(
            mmap[dir_off..dir_off + 4].try_into().unwrap(),
        ) as usize;
        let comp_size = u32::from_le_bytes(
            mmap[dir_off + 4..dir_off + 8].try_into().unwrap(),
        ) as usize;

        let block_start = blocks_offset as usize + rel_offset;
        let compressed = &mmap[block_start..block_start + comp_size];
        let decompressed = lz4_flex::decompress_size_prepended(compressed)
            .map_err(|e| HeimdallError::Build(format!("lz4 decompress block {}: {}", block_idx, e)))?;

        let result = decompressed.clone();
        {
            let mut cache = self.block_cache.lock().unwrap();
            cache.insert(block_type, block_idx, decompressed);
        }

        Ok(result)
    }

    /// Resolve the primary name for a record. Returns an owned String.
    /// V1/V2: copies from the pool slice. V3: decompresses one SP block.
    pub fn primary_name(&self, record: &PlaceRecord) -> String {
        match &self.string_pool {
            StringPoolData::Mmap { .. } | StringPoolData::Owned { .. } => {
                let pool = self.string_pool_bytes_v1v2();
                read_primary_name_from_pool(pool, record.name_offset)
            }
            StringPoolData::BlockCompressed {
                sp_block_size,
                sp_block_count: _,
                sp_block_dir_offset,
                sp_blocks_offset,
            } => {
                // Find the block that contains name_offset (may span boundary)
                let offset = record.name_offset as usize;
                let block_idx = offset / *sp_block_size as usize;
                let local_offset = offset % *sp_block_size as usize;

                let decompressed = self.decompress_block_cached(
                    1, block_idx as u32, *sp_block_dir_offset, *sp_blocks_offset,
                );
                match decompressed {
                    Ok(block) => {
                        if local_offset < block.len() {
                            let len = block[local_offset] as usize;
                            if local_offset + 1 + len <= block.len() {
                                return std::str::from_utf8(
                                    &block[local_offset + 1..local_offset + 1 + len],
                                )
                                .unwrap_or("<invalid utf8>")
                                .to_owned();
                            }
                        }
                        // Name spans block boundary or invalid — fall back to
                        // assembling bytes across blocks
                        self.read_sp_bytes_across_blocks(offset, 256)
                            .map(|bytes| read_primary_name_from_pool(&bytes, 0))
                            .unwrap_or_else(|_| "<invalid offset>".to_owned())
                    }
                    Err(_) => "<decompress error>".to_owned(),
                }
            }
        }
    }

    /// Resolve all names (primary + alts) for a record.
    /// Returns owned Strings.
    pub fn all_names(&self, record: &PlaceRecord) -> Vec<String> {
        match &self.string_pool {
            StringPoolData::Mmap { .. } | StringPoolData::Owned { .. } => {
                let pool = self.string_pool_bytes_v1v2();
                read_all_names_from_pool(pool, record.name_offset)
            }
            StringPoolData::BlockCompressed { .. } => {
                // Read a generous chunk of SP bytes starting at name_offset.
                // Max name entry: 1 + 255 + 1 + 255*(1+255) = ~65K, but typical is <500 bytes.
                let bytes = match self.read_sp_bytes_across_blocks(
                    record.name_offset as usize,
                    2048,
                ) {
                    Ok(b) => b,
                    Err(_) => return vec![],
                };
                read_all_names_from_pool(&bytes, 0)
            }
        }
    }

    /// Read `max_len` bytes from the string pool starting at `offset`,
    /// assembling across V3 block boundaries as needed.
    fn read_sp_bytes_across_blocks(
        &self,
        offset: usize,
        max_len: usize,
    ) -> Result<Vec<u8>, HeimdallError> {
        let (sp_block_size, sp_block_count, sp_block_dir_offset, sp_blocks_offset) =
            match &self.string_pool {
                StringPoolData::BlockCompressed {
                    sp_block_size,
                    sp_block_count,
                    sp_block_dir_offset,
                    sp_blocks_offset,
                } => (*sp_block_size, *sp_block_count, *sp_block_dir_offset, *sp_blocks_offset),
                _ => unreachable!("read_sp_bytes_across_blocks called on non-V3"),
            };
        let block_size = sp_block_size as usize;
        let mut result = Vec::with_capacity(max_len);
        let mut pos = offset;

        while result.len() < max_len {
            let block_idx = pos / block_size;
            if block_idx >= sp_block_count as usize {
                break;
            }
            let local = pos % block_size;
            let block = self.decompress_block_cached(
                1, block_idx as u32, sp_block_dir_offset, sp_blocks_offset,
            )?;
            let available = block.len().saturating_sub(local);
            let take = available.min(max_len - result.len());
            result.extend_from_slice(&block[local..local + take]);
            pos += take;
        }

        Ok(result)
    }

    /// Get raw string pool bytes for V1/V2.
    fn string_pool_bytes_v1v2(&self) -> &[u8] {
        match &self.string_pool {
            StringPoolData::Mmap { mmap_offset } => {
                match &self.records {
                    RecordData::Mmap { mmap } => &mmap[*mmap_offset..],
                    _ => unreachable!("v1 string pool with owned records"),
                }
            }
            StringPoolData::Owned { pool } => pool,
            StringPoolData::BlockCompressed { .. } => {
                unreachable!("string_pool_bytes_v1v2 called on V3")
            }
        }
    }
}

/// Decompress a single string pool block from the mmap.
#[allow(dead_code)]
fn decompress_sp_block(
    mmap: &Mmap,
    sp_block_dir_offset: usize,
    sp_blocks_offset: u64,
    block_idx: usize,
) -> Result<Vec<u8>, HeimdallError> {
    let dir_entry_offset = sp_block_dir_offset + block_idx * 8;
    let rel_offset = u32::from_le_bytes(
        mmap[dir_entry_offset..dir_entry_offset + 4]
            .try_into()
            .unwrap(),
    );
    let comp_size = u32::from_le_bytes(
        mmap[dir_entry_offset + 4..dir_entry_offset + 8]
            .try_into()
            .unwrap(),
    );
    let block_start = sp_blocks_offset as usize + rel_offset as usize;
    let compressed = &mmap[block_start..block_start + comp_size as usize];
    lz4_flex::decompress_size_prepended(compressed)
        .map_err(|e| HeimdallError::Build(format!("lz4 decompress SP block {}: {}", block_idx, e)))
}

/// Read primary name from a byte slice representing the string pool.
fn read_primary_name_from_pool(pool: &[u8], name_offset: u32) -> String {
    let offset = name_offset as usize;
    if offset >= pool.len() {
        return "<invalid offset>".to_owned();
    }
    let len = pool[offset] as usize;
    if offset + 1 + len > pool.len() {
        return "<truncated>".to_owned();
    }
    std::str::from_utf8(&pool[offset + 1..offset + 1 + len])
        .unwrap_or("<invalid utf8>")
        .to_owned()
}

/// Read all names (primary + alts) from a byte slice representing the string pool.
fn read_all_names_from_pool(pool: &[u8], name_offset: u32) -> Vec<String> {
    let mut pos = name_offset as usize;
    let mut names = vec![];

    if pos >= pool.len() {
        return names;
    }

    // Primary name
    let primary_len = pool[pos] as usize;
    pos += 1;
    if pos + primary_len > pool.len() {
        return names;
    }
    names.push(
        std::str::from_utf8(&pool[pos..pos + primary_len])
            .unwrap_or("")
            .to_owned(),
    );
    pos += primary_len;

    if pos >= pool.len() {
        return names;
    }

    // Alt names
    let n_alts = pool[pos] as usize;
    pos += 1;
    for _ in 0..n_alts {
        if pos >= pool.len() {
            break;
        }
        let alt_len = pool[pos] as usize;
        pos += 1;
        if pos + alt_len > pool.len() {
            break;
        }
        names.push(
            std::str::from_utf8(&pool[pos..pos + alt_len])
                .unwrap_or("")
                .to_owned(),
        );
        pos += alt_len;
    }

    names
}

// ---------------------------------------------------------------------------
// Column encoding helpers (used by V2 writer)
// ---------------------------------------------------------------------------

/// Delta-encode i32 array: store differences between consecutive values.
/// First value stored as-is. Output as little-endian i32 bytes.
fn encode_delta_i32(values: &[i32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    let mut prev = 0i32;
    for &v in values {
        let delta = v.wrapping_sub(prev);
        out.extend_from_slice(&delta.to_le_bytes());
        prev = v;
    }
    out
}

/// Decode delta-encoded i32 array from little-endian bytes.
fn decode_delta_i32(bytes: &[u8], count: usize) -> Vec<i32> {
    let mut out = Vec::with_capacity(count);
    let mut prev = 0i32;
    for i in 0..count {
        let off = i * 4;
        let delta = i32::from_le_bytes(bytes[off..off+4].try_into().unwrap());
        let val = prev.wrapping_add(delta);
        out.push(val);
        prev = val;
    }
    out
}

fn encode_u16(values: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 2);
    for &v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

fn decode_u16(bytes: &[u8], count: usize) -> Vec<u16> {
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = i * 2;
        out.push(u16::from_le_bytes(bytes[off..off+2].try_into().unwrap()));
    }
    out
}

fn encode_u32(values: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for &v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

fn decode_u32(bytes: &[u8], count: usize) -> Vec<u32> {
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = i * 4;
        out.push(u32::from_le_bytes(bytes[off..off+4].try_into().unwrap()));
    }
    out
}

/// Convert u8 back to PlaceType
fn place_type_from_u8(v: u8) -> PlaceType {
    match v {
        0 => PlaceType::Country,
        1 => PlaceType::State,
        2 => PlaceType::County,
        3 => PlaceType::City,
        4 => PlaceType::Town,
        5 => PlaceType::Village,
        6 => PlaceType::Hamlet,
        7 => PlaceType::Farm,
        8 => PlaceType::Locality,
        10 => PlaceType::Suburb,
        11 => PlaceType::Quarter,
        12 => PlaceType::Neighbourhood,
        13 => PlaceType::Island,
        14 => PlaceType::Islet,
        15 => PlaceType::Square,
        20 => PlaceType::Lake,
        21 => PlaceType::River,
        22 => PlaceType::Mountain,
        23 => PlaceType::Forest,
        24 => PlaceType::Bay,
        25 => PlaceType::Cape,
        30 => PlaceType::Airport,
        31 => PlaceType::Station,
        32 => PlaceType::Landmark,
        33 => PlaceType::University,
        34 => PlaceType::Hospital,
        35 => PlaceType::PublicBuilding,
        36 => PlaceType::Park,
        _ => PlaceType::Unknown,
    }
}

// ---------------------------------------------------------------------------
// Builder — used by heimdall-build crate
// ---------------------------------------------------------------------------

pub struct RecordStoreBuilder {
    records: Vec<PlaceRecord>,
    string_pool: Vec<u8>,
}

impl RecordStoreBuilder {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            string_pool: Vec::new(),
        }
    }

    /// Add a place. Returns the record ID (index into records vec).
    pub fn add(
        &mut self,
        mut record: PlaceRecord,
        primary_name: &str,
        alt_names: &[&str],
    ) -> u32 {
        let offset = self.string_pool.len() as u32;
        record.name_offset = offset;

        // Write primary name: [u8 len][bytes]
        let primary_bytes = primary_name.as_bytes();
        assert!(primary_bytes.len() < 256, "name too long: {}", primary_name);
        self.string_pool.push(primary_bytes.len() as u8);
        self.string_pool.extend_from_slice(primary_bytes);

        // Write alt count + alt names
        assert!(alt_names.len() < 256);
        self.string_pool.push(alt_names.len() as u8);
        for alt in alt_names {
            let alt_bytes = alt.as_bytes();
            assert!(alt_bytes.len() < 256, "alt name too long: {}", alt);
            self.string_pool.push(alt_bytes.len() as u8);
            self.string_pool.extend_from_slice(alt_bytes);
        }

        let id = self.records.len() as u32;
        self.records.push(record);
        id
    }

    /// Write V3 block-compressed format (default).
    /// LZ4-compressed 64KB blocks for both records and string pool.
    pub fn write(&self, path: &Path) -> Result<(), HeimdallError> {
        self.write_v3(path)
    }

    /// Write V2 columnar format with zstd compression (legacy).
    pub fn write_v2(&self, path: &Path) -> Result<(), HeimdallError> {
        use std::io::Write;

        let n = self.records.len();
        let zstd_level = 19; // High compression for index files

        // Extract columns from records
        let mut lats = Vec::with_capacity(n);
        let mut lons = Vec::with_capacity(n);
        let mut admin1s = Vec::with_capacity(n);
        let mut admin2s = Vec::with_capacity(n);
        let mut importances = Vec::with_capacity(n);
        let mut place_types = Vec::with_capacity(n);
        let mut flags = Vec::with_capacity(n);
        let mut name_offsets = Vec::with_capacity(n);
        let mut osm_ids = Vec::with_capacity(n);

        for r in &self.records {
            lats.push(r.coord.lat);
            lons.push(r.coord.lon);
            admin1s.push(r.admin1_id);
            admin2s.push(r.admin2_id);
            importances.push(r.importance);
            place_types.push(r.place_type as u8);
            flags.push(r.flags);
            name_offsets.push(r.name_offset);
            osm_ids.push(r.osm_id);
        }

        // Encode columns
        let raw_cols: Vec<Vec<u8>> = vec![
            encode_delta_i32(&lats),
            encode_delta_i32(&lons),
            encode_u16(&admin1s),
            encode_u16(&admin2s),
            encode_u16(&importances),
            place_types.clone(),
            flags.clone(),
            encode_u32(&name_offsets),
            encode_u32(&osm_ids),
        ];

        // Compress each column
        let col_names = [
            "lat (delta)", "lon (delta)", "admin1_id", "admin2_id",
            "importance", "place_type", "flags", "name_offset", "osm_id",
        ];
        let compressed_cols: Vec<Vec<u8>> = raw_cols.iter()
            .map(|raw| zstd::bulk::compress(raw, zstd_level)
                .expect("zstd compress failed"))
            .collect();

        // Log per-column compression stats
        let mut total_raw = 0usize;
        let mut total_compressed = 0usize;
        for (i, (raw, comp)) in raw_cols.iter().zip(compressed_cols.iter()).enumerate() {
            let name = if i < col_names.len() { col_names[i] } else { "?" };
            tracing::info!(
                "  Column {:2} {:<15}: {:>8} raw -> {:>8} compressed ({:.1}%)",
                i, name, raw.len(), comp.len(),
                comp.len() as f64 / raw.len() as f64 * 100.0
            );
            total_raw += raw.len();
            total_compressed += comp.len();
        }

        // Compress string pool
        let sp_compressed = zstd::bulk::compress(&self.string_pool, zstd_level)
            .map_err(|e| HeimdallError::Build(format!("zstd compress string pool: {}", e)))?;

        tracing::info!(
            "  String pool:           {:>8} raw -> {:>8} compressed ({:.1}%)",
            self.string_pool.len(), sp_compressed.len(),
            sp_compressed.len() as f64 / self.string_pool.len() as f64 * 100.0
        );
        tracing::info!(
            "  Columns total:         {:>8} raw -> {:>8} compressed ({:.1}%)",
            total_raw, total_compressed,
            total_compressed as f64 / total_raw as f64 * 100.0
        );
        let v1_size = 24 + n * 24 + self.string_pool.len(); // header + records + pool
        let v2_data_size = total_compressed + sp_compressed.len();
        tracing::info!(
            "  V1 equivalent:         {:>8} bytes", v1_size
        );
        tracing::info!(
            "  V2 data:               {:>8} bytes ({:.1}% of V1)",
            v2_data_size, v2_data_size as f64 / v1_size as f64 * 100.0
        );

        // Calculate layout
        let header_size = V2_HEADER_FIXED
            + NUM_COLUMNS as usize * V2_COL_DIR_ENTRY
            + V2_STRING_POOL_INFO;

        let mut data_offset = header_size;
        let mut col_offsets = Vec::with_capacity(NUM_COLUMNS as usize);
        for cc in &compressed_cols {
            col_offsets.push(data_offset);
            data_offset += cc.len();
        }
        let sp_offset = data_offset;

        // Write file
        let mut f = std::io::BufWriter::new(File::create(path)?);

        // Header
        f.write_all(&MAGIC.to_le_bytes())?;
        f.write_all(&VERSION_V2.to_le_bytes())?;
        f.write_all(&(n as u64).to_le_bytes())?;
        f.write_all(&NUM_COLUMNS.to_le_bytes())?;

        // Column directory
        for (i, cc) in compressed_cols.iter().enumerate() {
            f.write_all(&(col_offsets[i] as u64).to_le_bytes())?;
            f.write_all(&(cc.len() as u32).to_le_bytes())?;
            f.write_all(&(raw_cols[i].len() as u32).to_le_bytes())?;
        }

        // String pool info
        f.write_all(&(sp_offset as u64).to_le_bytes())?;
        f.write_all(&(sp_compressed.len() as u32).to_le_bytes())?;
        f.write_all(&(self.string_pool.len() as u32).to_le_bytes())?;

        // Column data
        for cc in &compressed_cols {
            f.write_all(cc)?;
        }

        // String pool data
        f.write_all(&sp_compressed)?;

        f.flush()?;

        Ok(())
    }

    /// Write V3 block-compressed format: LZ4-compressed 64KB blocks.
    /// Records stored as flat PlaceRecord array (24 bytes each, repr(C)).
    /// String pool stored as-is, broken into blocks.
    fn write_v3(&self, path: &Path) -> Result<(), HeimdallError> {
        use std::io::Write;

        let n = self.records.len();
        let record_size = std::mem::size_of::<PlaceRecord>(); // 24
        let block_size = V3_DEFAULT_BLOCK_SIZE as usize;

        // --- Serialize records as flat byte array ---
        let records_raw_len = n * record_size;
        let mut records_raw = Vec::with_capacity(records_raw_len);
        for r in &self.records {
            let bytes: &[u8] = unsafe {
                std::slice::from_raw_parts(r as *const PlaceRecord as *const u8, record_size)
            };
            records_raw.extend_from_slice(bytes);
        }

        // --- Break records into blocks and LZ4-compress ---
        let record_block_count = (records_raw_len + block_size - 1) / block_size;
        let mut record_compressed_blocks: Vec<Vec<u8>> = Vec::with_capacity(record_block_count);
        for i in 0..record_block_count {
            let start = i * block_size;
            let end = (start + block_size).min(records_raw_len);
            let block_data = &records_raw[start..end];
            let compressed = lz4_flex::compress_prepend_size(block_data);
            record_compressed_blocks.push(compressed);
        }

        // --- Break string pool into blocks and LZ4-compress ---
        let sp_raw_len = self.string_pool.len();
        let sp_block_count = if sp_raw_len == 0 {
            0
        } else {
            (sp_raw_len + block_size - 1) / block_size
        };
        let mut sp_compressed_blocks: Vec<Vec<u8>> = Vec::with_capacity(sp_block_count);
        for i in 0..sp_block_count {
            let start = i * block_size;
            let end = (start + block_size).min(sp_raw_len);
            let block_data = &self.string_pool[start..end];
            let compressed = lz4_flex::compress_prepend_size(block_data);
            sp_compressed_blocks.push(compressed);
        }

        // --- Calculate layout ---
        // Header: 48 bytes
        // Record block directory: record_block_count * 8
        // SP block directory: sp_block_count * 8
        // Compressed record blocks (sequential)
        // Compressed SP blocks (sequential)

        let record_block_dir_start = V3_HEADER_SIZE;
        let sp_block_dir_start = record_block_dir_start + record_block_count * 8;
        let record_blocks_start = sp_block_dir_start + sp_block_count * 8;

        let mut record_block_dir: Vec<(u32, u32)> = Vec::with_capacity(record_block_count);
        let mut offset: u32 = 0;
        for block in &record_compressed_blocks {
            record_block_dir.push((offset, block.len() as u32));
            offset += block.len() as u32;
        }
        let total_record_blocks_size = offset as usize;

        let sp_blocks_start = record_blocks_start + total_record_blocks_size;

        let mut sp_block_dir: Vec<(u32, u32)> = Vec::with_capacity(sp_block_count);
        let mut sp_offset: u32 = 0;
        for block in &sp_compressed_blocks {
            sp_block_dir.push((sp_offset, block.len() as u32));
            sp_offset += block.len() as u32;
        }
        let total_sp_blocks_size = sp_offset as usize;

        // --- Log stats ---
        let v1_size = 24 + records_raw_len + sp_raw_len;
        let v3_data_size = total_record_blocks_size + total_sp_blocks_size;
        let v3_total = V3_HEADER_SIZE
            + record_block_count * 8
            + sp_block_count * 8
            + v3_data_size;

        tracing::info!(
            "  V3 records: {} blocks, {:>8} raw -> {:>8} compressed ({:.1}%)",
            record_block_count,
            records_raw_len,
            total_record_blocks_size,
            if records_raw_len > 0 {
                total_record_blocks_size as f64 / records_raw_len as f64 * 100.0
            } else {
                0.0
            }
        );
        tracing::info!(
            "  V3 string pool: {} blocks, {:>8} raw -> {:>8} compressed ({:.1}%)",
            sp_block_count,
            sp_raw_len,
            total_sp_blocks_size,
            if sp_raw_len > 0 {
                total_sp_blocks_size as f64 / sp_raw_len as f64 * 100.0
            } else {
                0.0
            }
        );
        tracing::info!(
            "  V1 equivalent:         {:>8} bytes", v1_size
        );
        tracing::info!(
            "  V3 total:              {:>8} bytes ({:.1}% of V1)",
            v3_total,
            if v1_size > 0 {
                v3_total as f64 / v1_size as f64 * 100.0
            } else {
                0.0
            }
        );

        // --- Write file ---
        let mut f = std::io::BufWriter::new(File::create(path)?);

        // Header (48 bytes)
        f.write_all(&MAGIC.to_le_bytes())?;
        f.write_all(&VERSION_V3.to_le_bytes())?;
        f.write_all(&(n as u64).to_le_bytes())?;
        f.write_all(&V3_DEFAULT_BLOCK_SIZE.to_le_bytes())?; // record_block_size
        f.write_all(&(record_block_count as u32).to_le_bytes())?;
        f.write_all(&V3_DEFAULT_BLOCK_SIZE.to_le_bytes())?; // sp_block_size
        f.write_all(&(sp_block_count as u32).to_le_bytes())?;
        f.write_all(&(record_blocks_start as u64).to_le_bytes())?;
        f.write_all(&(sp_blocks_start as u64).to_le_bytes())?;

        // Record block directory
        for (rel_off, comp_sz) in &record_block_dir {
            f.write_all(&rel_off.to_le_bytes())?;
            f.write_all(&comp_sz.to_le_bytes())?;
        }

        // SP block directory
        for (rel_off, comp_sz) in &sp_block_dir {
            f.write_all(&rel_off.to_le_bytes())?;
            f.write_all(&comp_sz.to_le_bytes())?;
        }

        // Record compressed blocks
        for block in &record_compressed_blocks {
            f.write_all(block)?;
        }

        // SP compressed blocks
        for block in &sp_compressed_blocks {
            f.write_all(block)?;
        }

        f.flush()?;

        Ok(())
    }
}
