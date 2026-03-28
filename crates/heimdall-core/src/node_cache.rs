/// Node coordinate cache — trait for resolving OSM node IDs to coordinates.
///
/// Four implementations:
///   InMemoryNodeCache    — HashMap-based, for pre-filtered multi-pass extraction
///   SortedVecNodeCache   — compact sorted vec, for single-pass
///   MmapNodeCache        — sparse mmap'd file, O(1) lookup, ~500MB RAM for any country
///   SortedFileNodeCache  — external merge sort to disk, O(log n) lookup, ~500MB RAM (default)

use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write, Read};

pub trait NodeCache {
    /// Store a node's coordinates. Called during the node-scanning pass.
    fn insert(&mut self, id: i64, lat: f64, lon: f64);

    /// Look up a node's coordinates. Returns None if the node wasn't cached.
    fn get(&self, id: i64) -> Option<(f64, f64)>;

    /// Batch lookup: resolve many node IDs at once. IDs are sorted internally
    /// for sequential memory access, then results returned in original order.
    /// Default implementation falls back to individual get() calls.
    fn batch_get(&self, ids: &[i64]) -> Vec<Option<(f64, f64)>> {
        ids.iter().map(|id| self.get(*id)).collect()
    }

    /// Number of cached entries.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Called once after all inserts, before reads begin.
    /// SortedVecNodeCache uses this to sort; MmapNodeCache switches madvise to random.
    fn prepare_for_reads(&mut self) {}
}

/// In-memory node cache backed by HashMap.
/// Appropriate for country-level extracts (Sweden: ~10M needed nodes ≈ 160MB).
///
/// For planet builds, swap this for MmapNodeCache which uses a sorted
/// on-disk file with binary search and OS page cache.
pub struct InMemoryNodeCache {
    /// Only cache nodes we actually need (pre-filtered by needed_ids)
    needed_ids: HashSet<i64>,
    coords: HashMap<i64, (f64, f64)>,
}

impl InMemoryNodeCache {
    /// Create a cache that will only store coordinates for the given node IDs.
    /// Pass the set of node IDs referenced by ways/relations you want to extract.
    pub fn with_needed_ids(needed_ids: HashSet<i64>) -> Self {
        let capacity = needed_ids.len();
        Self {
            needed_ids,
            coords: HashMap::with_capacity(capacity),
        }
    }

    /// Check if a node ID is in the needed set (useful for the scanning pass
    /// to decide whether to call insert).
    pub fn needs(&self, id: i64) -> bool {
        self.needed_ids.contains(&id)
    }
}

impl NodeCache for InMemoryNodeCache {
    fn insert(&mut self, id: i64, lat: f64, lon: f64) {
        if self.needed_ids.contains(&id) {
            self.coords.insert(id, (lat, lon));
        }
    }

    fn get(&self, id: i64) -> Option<(f64, f64)> {
        self.coords.get(&id).copied()
    }

    fn len(&self) -> usize {
        self.coords.len()
    }
}

// ---------------------------------------------------------------------------
// SortedVecNodeCache — compact, single-pass friendly
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct NodeEntry {
    id: i64,
    lat: i32,  // micro-degrees (lat * 1_000_000)
    lon: i32,  // micro-degrees (lon * 1_000_000)
}

/// Compact sorted-vec node cache for single-pass extraction.
///
/// Appends entries during the node phase, sorts once (in-place), then serves
/// O(log n) binary-search lookups during the way/relation phase.
///
/// Uses i32 micro-degrees for coordinates: 16 bytes per entry vs ~72 for HashMap.
/// For 430M nodes (Germany): ~6.8GB total, in-place sort, no extra allocation.
pub struct SortedVecNodeCache {
    entries: Vec<NodeEntry>,
    sorted: bool,
}

impl SortedVecNodeCache {
    pub fn new() -> Self {
        Self { entries: Vec::new(), sorted: false }
    }

    /// Sort entries by node ID. Call once after all inserts, before any get().
    /// In-place O(n log n), no extra allocation.
    pub fn sort(&mut self) {
        if self.sorted { return; }
        self.entries.sort_unstable_by_key(|e| e.id);
        self.sorted = true;
    }
}

impl NodeCache for SortedVecNodeCache {
    fn insert(&mut self, id: i64, lat: f64, lon: f64) {
        self.entries.push(NodeEntry {
            id,
            lat: (lat * 1_000_000.0) as i32,
            lon: (lon * 1_000_000.0) as i32,
        });
    }

    fn get(&self, id: i64) -> Option<(f64, f64)> {
        self.entries.binary_search_by_key(&id, |e| e.id).ok().map(|i| {
            let e = &self.entries[i];
            (e.lat as f64 / 1_000_000.0, e.lon as f64 / 1_000_000.0)
        })
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn prepare_for_reads(&mut self) {
        self.sort();
    }
}

// ---------------------------------------------------------------------------
// MmapNodeCache — sparse mmap'd file, O(1) direct-indexed by node ID
// ---------------------------------------------------------------------------

/// Disk-backed node cache using a sparse memory-mapped file.
///
/// Uses the node ID as a direct array index: offset = id * 8 bytes.
/// The OS virtual memory system handles paging — only touched pages use
/// physical RAM or disk. No sort step needed; lookups are O(1).
///
/// Coordinates stored as i32 micro-degrees (8 bytes per entry).
/// Latitude is offset by +91_000_000 so that zero (sparse file default)
/// reliably means "not set" — valid encoded latitudes are always > 0.
///
/// Memory usage: ~300-500MB RSS for Germany (430M nodes), regardless of
/// country size. Disk: ~3.4GB sparse temp file (auto-deleted on drop).
pub struct MmapNodeCache {
    mmap: memmap2::MmapMut,
    count: usize,
    _file: std::fs::File,
}

/// Max supported OSM node ID. Current OSM max is ~12 billion.
/// 15 billion gives headroom for growth.
const MMAP_MAX_NODE_ID: u64 = 15_000_000_000;

/// Bytes per coordinate entry: lat_i32 (4) + lon_i32 (4).
const MMAP_ENTRY_BYTES: u64 = 8;

/// Latitude offset so that encoded zero means "not set".
/// Valid latitudes [-90, 90] encode to [1_000_000, 181_000_000] — always > 0.
const LAT_OFFSET: i32 = 91_000_000;

impl MmapNodeCache {
    /// Create a new mmap-backed node cache.
    ///
    /// Creates a sparse temp file in the system temp directory. The file is
    /// unlinked immediately — the mmap keeps data alive via the file descriptor.
    /// When this struct is dropped, the OS reclaims all disk space.
    pub fn new() -> std::io::Result<Self> {
        let temp_path = std::env::temp_dir()
            .join(format!("heimdall-nodecache-{}.tmp", std::process::id()));

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;

        // 15B nodes × 8 bytes = 120GB virtual. Sparse: only written pages use disk.
        let size = MMAP_MAX_NODE_ID * MMAP_ENTRY_BYTES;
        file.set_len(size)?;

        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };

        // Tell OS not to readahead — our access pattern is random by node ID.
        // Without this, macOS eagerly pages in ~12 GB for Germany's scattered node IDs.
        #[cfg(unix)]
        unsafe {
            libc::madvise(
                mmap.as_mut_ptr() as *mut libc::c_void,
                mmap.len(),
                libc::MADV_RANDOM,
            );
        }

        // Unlink file path — the fd (and mmap) keep the inode alive.
        // Disk space is reclaimed when MmapNodeCache is dropped.
        std::fs::remove_file(&temp_path).ok();

        Ok(Self {
            mmap,
            count: 0,
            _file: file,
        })
    }
}

impl NodeCache for MmapNodeCache {
    fn insert(&mut self, id: i64, lat: f64, lon: f64) {
        if id < 0 || id as u64 >= MMAP_MAX_NODE_ID {
            return;
        }
        let offset = id as usize * MMAP_ENTRY_BYTES as usize;
        let lat_enc = (lat * 1_000_000.0) as i32 + LAT_OFFSET;
        let lon_enc = (lon * 1_000_000.0) as i32;
        self.mmap[offset..offset + 4].copy_from_slice(&lat_enc.to_le_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&lon_enc.to_le_bytes());
        self.count += 1;
    }

    fn get(&self, id: i64) -> Option<(f64, f64)> {
        if id < 0 || id as u64 >= MMAP_MAX_NODE_ID {
            return None;
        }
        let offset = id as usize * MMAP_ENTRY_BYTES as usize;
        let lat_enc =
            i32::from_le_bytes(self.mmap[offset..offset + 4].try_into().unwrap());
        if lat_enc == 0 {
            return None; // Not set — sparse file zeros
        }
        let lon_enc =
            i32::from_le_bytes(self.mmap[offset + 4..offset + 8].try_into().unwrap());
        let lat = (lat_enc - LAT_OFFSET) as f64 / 1_000_000.0;
        let lon = lon_enc as f64 / 1_000_000.0;
        Some((lat, lon))
    }

    fn len(&self) -> usize {
        self.count
    }

    /// Batch lookup optimized for mmap: sort IDs for sequential page access,
    /// resolve in sorted order (cache-friendly), then scatter results back.
    /// This turns random mmap page faults into a sequential scan.
    fn batch_get(&self, ids: &[i64]) -> Vec<Option<(f64, f64)>> {
        if ids.len() <= 4 {
            // Too few to benefit from sorting overhead
            return ids.iter().map(|id| self.get(*id)).collect();
        }

        // Build sorted index: (node_id, original_position)
        let mut sorted: Vec<(i64, usize)> = ids.iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
            .collect();
        sorted.sort_unstable_by_key(|&(id, _)| id);

        // Resolve in sorted order — sequential mmap access
        let mut results: Vec<Option<(f64, f64)>> = vec![None; ids.len()];
        for &(id, orig_idx) in &sorted {
            results[orig_idx] = self.get(id);
        }
        results
    }

    fn prepare_for_reads(&mut self) {
        // With batch_get sorting IDs, sequential prefetch actually helps.
        // Switch to normal (let kernel decide) instead of random.
        #[cfg(unix)]
        {
            use memmap2::Advice;
            let _ = self.mmap.advise(Advice::Normal);
        }
    }
}

// ---------------------------------------------------------------------------
// SortedFileNodeCache — external merge sort, O(log n) binary search on mmap
// ---------------------------------------------------------------------------

/// Entry size: i64 id (8) + i32 lat (4) + i32 lon (4) = 16 bytes.
const SORTED_ENTRY_BYTES: usize = 16;

/// Sort chunk size: uses ~25% of available RAM, clamped to 64 MB–500 MB.
/// Auto-detected from cgroup memory limit (Docker) or system total.
fn sort_chunk_entries() -> usize {
    let available = detect_memory_limit();
    let chunk_bytes = (available / 4).clamp(64 * 1024 * 1024, 500 * 1024 * 1024);
    tracing::info!("Sort chunk: {:.0} MB (from {:.0} MB available)",
        chunk_bytes as f64 / 1e6, available as f64 / 1e6);
    chunk_bytes / SORTED_ENTRY_BYTES
}

/// Detect memory limit: cgroup v2 (Docker) → cgroup v1 → system total → default 2 GB.
pub fn detect_memory_limit() -> usize {
    // cgroup v2 (Docker, modern Linux)
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        if let Ok(bytes) = s.trim().parse::<usize>() {
            return bytes;
        }
    }
    // cgroup v1
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(bytes) = s.trim().parse::<usize>() {
            if bytes < 1024 * 1024 * 1024 * 1024 { // ignore "unlimited" (~9 exabytes)
                return bytes;
            }
        }
    }
    // macOS / fallback: use sysctl or default
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok();
        if let Some(out) = output {
            if let Ok(s) = String::from_utf8(out.stdout) {
                if let Ok(bytes) = s.trim().parse::<usize>() {
                    return bytes;
                }
            }
        }
    }
    // Default: 2 GB
    2 * 1024 * 1024 * 1024
}

/// Disk-backed node cache using external merge sort and mmap'd binary search.
///
/// Insert phase: sequentially appends 16-byte entries to a temp file via BufWriter.
/// Uses near-zero RAM during inserts (just the BufWriter buffer).
///
/// prepare_for_reads phase: external merge sort:
///   1. Read ~500MB chunks, sort each in memory by node ID, write to chunk files
///   2. K-way merge sorted chunks into one final sorted file
///   3. Mmap the final sorted file with MADV_RANDOM
///   4. Delete intermediate files
///
/// Read phase: O(log n) binary search on the mmap'd sorted array.
/// batch_get uses merge-scan for sequential access.
///
/// Memory: ~500MB during sort (one chunk in RAM), ~0 during reads (OS page cache).
/// Disk: ~6.8GB for Germany (430M entries × 16 bytes), temporary during sort.
pub struct SortedFileNodeCache {
    /// State machine: Writing during insert, Reading after prepare_for_reads.
    state: SortedFileState,
    count: usize,
    temp_dir: std::path::PathBuf,
}

enum SortedFileState {
    Writing {
        writer: BufWriter<std::fs::File>,
        unsorted_path: std::path::PathBuf,
    },
    Reading {
        mmap: memmap2::Mmap,
        entry_count: usize,
        _file: std::fs::File,
    },
    /// Transitional state during prepare_for_reads.
    Transitioning,
}

impl SortedFileNodeCache {
    pub fn new() -> std::io::Result<Self> {
        let temp_dir = std::env::temp_dir()
            .join(format!("heimdall-sortcache-{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir)?;

        let unsorted_path = temp_dir.join("unsorted.bin");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&unsorted_path)?;
        let writer = BufWriter::with_capacity(8 * 1024 * 1024, file); // 8MB buffer

        Ok(Self {
            state: SortedFileState::Writing { writer, unsorted_path },
            count: 0,
            temp_dir,
        })
    }

    /// Read one entry from a raw byte slice at the given index.
    #[inline]
    fn read_entry(data: &[u8], idx: usize) -> (i64, i32, i32) {
        let off = idx * SORTED_ENTRY_BYTES;
        let id = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        let lat = i32::from_le_bytes(data[off + 8..off + 12].try_into().unwrap());
        let lon = i32::from_le_bytes(data[off + 12..off + 16].try_into().unwrap());
        (id, lat, lon)
    }

    /// Binary search the mmap'd sorted array for a node ID.
    #[inline]
    fn binary_search(data: &[u8], entry_count: usize, target_id: i64) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = entry_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = mid * SORTED_ENTRY_BYTES;
            let mid_id = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            match mid_id.cmp(&target_id) {
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// External merge sort: chunk-sort then k-way merge.
    fn external_sort(&self, unsorted_path: &std::path::Path) -> std::io::Result<(std::fs::File, std::path::PathBuf)> {
        let file_len = std::fs::metadata(unsorted_path)?.len() as usize;
        let total_entries = file_len / SORTED_ENTRY_BYTES;

        if total_entries == 0 {
            let sorted_path = self.temp_dir.join("sorted.bin");
            let file = std::fs::File::create(&sorted_path)?;
            return Ok((file, sorted_path));
        }

        // Phase 1: Read chunks, sort in memory, write sorted chunk files
        let mut chunk_paths: Vec<std::path::PathBuf> = Vec::new();
        let mut reader = std::io::BufReader::with_capacity(
            8 * 1024 * 1024,
            std::fs::File::open(unsorted_path)?,
        );

        let max_chunk = sort_chunk_entries();
        let mut entries_remaining = total_entries;
        let mut chunk_idx = 0u32;

        while entries_remaining > 0 {
            let chunk_size = entries_remaining.min(max_chunk);
            let byte_size = chunk_size * SORTED_ENTRY_BYTES;

            // Read chunk into memory as raw bytes, sort in-place (no extra allocation)
            let mut buf = vec![0u8; byte_size];
            reader.read_exact(&mut buf)?;

            // Sort the raw byte buffer in-place by interpreting 16-byte slices
            // This avoids allocating a separate Vec<(i64, i32, i32)>
            let entries_slice = unsafe {
                std::slice::from_raw_parts_mut(
                    buf.as_mut_ptr() as *mut [u8; SORTED_ENTRY_BYTES],
                    chunk_size,
                )
            };
            entries_slice.sort_unstable_by(|a, b| {
                let id_a = i64::from_le_bytes(a[0..8].try_into().unwrap());
                let id_b = i64::from_le_bytes(b[0..8].try_into().unwrap());
                id_a.cmp(&id_b)
            });

            // Write sorted chunk directly from the sorted buffer
            let chunk_path = self.temp_dir.join(format!("chunk_{:04}.bin", chunk_idx));
            let mut chunk_writer = BufWriter::with_capacity(
                8 * 1024 * 1024,
                std::fs::File::create(&chunk_path)?,
            );
            chunk_writer.write_all(&buf)?;
            chunk_writer.flush()?;
            drop(buf);

            chunk_paths.push(chunk_path);
            entries_remaining -= chunk_size;
            chunk_idx += 1;
        }
        drop(reader);

        // If only one chunk, it's already sorted — just rename it
        let sorted_path = self.temp_dir.join("sorted.bin");
        if chunk_paths.len() == 1 {
            std::fs::rename(&chunk_paths[0], &sorted_path)?;
            let file = std::fs::OpenOptions::new().read(true).open(&sorted_path)?;
            return Ok((file, sorted_path));
        }

        // Phase 2: K-way merge sorted chunks
        let mut sorted_writer = BufWriter::with_capacity(
            8 * 1024 * 1024,
            std::fs::File::create(&sorted_path)?,
        );

        // Open all chunk files with buffered readers and read first entry from each
        struct ChunkReader {
            reader: std::io::BufReader<std::fs::File>,
            remaining: usize,
            current: Option<(i64, i32, i32)>,
        }

        impl ChunkReader {
            fn advance(&mut self) -> std::io::Result<()> {
                if self.remaining == 0 {
                    self.current = None;
                    return Ok(());
                }
                let mut buf = [0u8; SORTED_ENTRY_BYTES];
                self.reader.read_exact(&mut buf)?;
                let id = i64::from_le_bytes(buf[0..8].try_into().unwrap());
                let lat = i32::from_le_bytes(buf[8..12].try_into().unwrap());
                let lon = i32::from_le_bytes(buf[12..16].try_into().unwrap());
                self.current = Some((id, lat, lon));
                self.remaining -= 1;
                Ok(())
            }
        }

        let mut chunk_readers: Vec<ChunkReader> = Vec::with_capacity(chunk_paths.len());
        for path in &chunk_paths {
            let meta = std::fs::metadata(path)?;
            let entries = meta.len() as usize / SORTED_ENTRY_BYTES;
            let reader = std::io::BufReader::with_capacity(
                4 * 1024 * 1024,
                std::fs::File::open(path)?,
            );
            let mut cr = ChunkReader { reader, remaining: entries, current: None };
            cr.advance()?;
            chunk_readers.push(cr);
        }

        // Simple k-way merge: find minimum across all chunk heads.
        // For small k (typically 1-14 chunks for Germany), linear scan is fine.
        loop {
            let mut min_idx = None;
            let mut min_id = i64::MAX;
            for (i, cr) in chunk_readers.iter().enumerate() {
                if let Some((id, _, _)) = cr.current {
                    if id < min_id {
                        min_id = id;
                        min_idx = Some(i);
                    }
                }
            }
            let Some(idx) = min_idx else { break; };
            let (id, lat, lon) = chunk_readers[idx].current.unwrap();
            sorted_writer.write_all(&id.to_le_bytes())?;
            sorted_writer.write_all(&lat.to_le_bytes())?;
            sorted_writer.write_all(&lon.to_le_bytes())?;
            chunk_readers[idx].advance()?;
        }
        sorted_writer.flush()?;
        drop(sorted_writer);
        drop(chunk_readers);

        // Clean up chunk files
        for path in &chunk_paths {
            std::fs::remove_file(path).ok();
        }

        let file = std::fs::OpenOptions::new().read(true).open(&sorted_path)?;
        Ok((file, sorted_path))
    }
}

impl NodeCache for SortedFileNodeCache {
    fn insert(&mut self, id: i64, lat: f64, lon: f64) {
        if let SortedFileState::Writing { ref mut writer, .. } = self.state {
            let lat_i32 = (lat * 1_000_000.0) as i32;
            let lon_i32 = (lon * 1_000_000.0) as i32;
            // Ignore write errors during insert — they'll surface in prepare_for_reads
            let _ = writer.write_all(&id.to_le_bytes());
            let _ = writer.write_all(&lat_i32.to_le_bytes());
            let _ = writer.write_all(&lon_i32.to_le_bytes());
            self.count += 1;
        }
    }

    fn get(&self, id: i64) -> Option<(f64, f64)> {
        if let SortedFileState::Reading { ref mmap, entry_count, .. } = self.state {
            if let Some(idx) = Self::binary_search(mmap, entry_count, id) {
                let (_, lat, lon) = Self::read_entry(mmap, idx);
                return Some((lat as f64 / 1_000_000.0, lon as f64 / 1_000_000.0));
            }
        }
        None
    }

    fn batch_get(&self, ids: &[i64]) -> Vec<Option<(f64, f64)>> {
        let SortedFileState::Reading { ref mmap, entry_count, .. } = self.state else {
            return vec![None; ids.len()];
        };

        if ids.len() <= 4 {
            return ids.iter().map(|id| self.get(*id)).collect();
        }

        // Merge-scan: sort requested IDs, then walk the sorted file sequentially
        let mut indexed: Vec<(i64, usize)> = ids.iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
            .collect();
        indexed.sort_unstable_by_key(|&(id, _)| id);

        let mut results: Vec<Option<(f64, f64)>> = vec![None; ids.len()];
        let data: &[u8] = mmap;
        let mut file_pos = 0usize;

        for &(target_id, orig_idx) in &indexed {
            // Binary search from current file_pos to entry_count
            // (since requests are sorted, we only search forward)
            let mut lo = file_pos;
            let mut hi = entry_count;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let off = mid * SORTED_ENTRY_BYTES;
                let mid_id = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                match mid_id.cmp(&target_id) {
                    std::cmp::Ordering::Equal => {
                        let lat = i32::from_le_bytes(data[off + 8..off + 12].try_into().unwrap());
                        let lon = i32::from_le_bytes(data[off + 12..off + 16].try_into().unwrap());
                        results[orig_idx] = Some((
                            lat as f64 / 1_000_000.0,
                            lon as f64 / 1_000_000.0,
                        ));
                        file_pos = mid;
                        break;
                    }
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                }
            }
        }
        results
    }

    fn len(&self) -> usize {
        self.count
    }

    fn prepare_for_reads(&mut self) {
        // Swap state to Transitioning so we can take ownership of the writer
        let old_state = std::mem::replace(&mut self.state, SortedFileState::Transitioning);
        let SortedFileState::Writing { mut writer, unsorted_path } = old_state else {
            return;
        };

        // Flush buffered writes
        if let Err(e) = writer.flush() {
            tracing::error!("Failed to flush node cache writes: {}", e);
            return;
        }
        drop(writer);

        tracing::info!(
            "External sort: {} entries ({:.1} GB on disk)",
            self.count,
            (self.count * SORTED_ENTRY_BYTES) as f64 / 1_073_741_824.0,
        );

        match self.external_sort(&unsorted_path) {
            Ok((file, _sorted_path)) => {
                // Delete unsorted file
                std::fs::remove_file(&unsorted_path).ok();

                let file_len = file.metadata().map(|m| m.len()).unwrap_or(0) as usize;
                let entry_count = file_len / SORTED_ENTRY_BYTES;

                if file_len == 0 {
                    // Empty cache — nothing to mmap
                    tracing::info!("Node cache empty, no entries to read");
                    return;
                }

                match unsafe { memmap2::Mmap::map(&file) } {
                    Ok(mmap) => {
                        #[cfg(unix)]
                        unsafe {
                            libc::madvise(
                                mmap.as_ptr() as *mut libc::c_void,
                                mmap.len(),
                                libc::MADV_RANDOM,
                            );
                        }
                        tracing::info!(
                            "Node cache ready: {} entries, {:.1} GB mmap'd",
                            entry_count,
                            file_len as f64 / 1_073_741_824.0,
                        );
                        self.state = SortedFileState::Reading {
                            mmap,
                            entry_count,
                            _file: file,
                        };
                    }
                    Err(e) => {
                        tracing::error!("Failed to mmap sorted node cache: {}", e);
                    }
                }
            }
            Err(e) => {
                tracing::error!("External sort failed: {}", e);
            }
        }
    }
}

impl Drop for SortedFileNodeCache {
    fn drop(&mut self) {
        // Clean up temp directory and all files in it
        std::fs::remove_dir_all(&self.temp_dir).ok();
    }
}
