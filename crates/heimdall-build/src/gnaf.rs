/// gnaf.rs — Parse G-NAF (Geocoded National Address File) for Australia
///
/// G-NAF is Australia's authoritative geocoded address database — 15.9M addresses.
/// Published quarterly by Geoscape, CC BY 4.0 license.
///
/// Data format: ZIP containing pipe-separated value (PSV) files organized by state.
/// Requires a 5-table join:
///   ADDRESS_DETAIL → STREET_LOCALITY → LOCALITY → STATE → ADDRESS_DEFAULT_GEOCODE

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Result;
use tracing::info;

use crate::extract::RawAddress;

/// Import G-NAF addresses from a ZIP file.
///
/// Extracts relevant PSV files to a temp directory, performs the 5-table join,
/// and returns flat RawAddress records.
pub fn read_gnaf_addresses(zip_path: &Path) -> Result<Vec<RawAddress>> {
    let mut addresses = Vec::new();
    stream_gnaf_addresses(zip_path, |addr| {
        addresses.push(addr);
    })?;
    Ok(addresses)
}

/// Streaming G-NAF import: calls `emit` for each address instead of collecting.
/// Memory: ~1.2 GB for geocode lookup table + ~20 MB for streets/localities.
/// The output addresses are never held in memory — caller decides what to do.
pub fn stream_gnaf_addresses(
    zip_path: &Path,
    mut emit: impl FnMut(RawAddress),
) -> Result<usize> {
    info!("Reading G-NAF addresses from {}", zip_path.display());

    let extract_dir = zip_path
        .parent()
        .unwrap_or(Path::new("."))
        .join("gnaf_extract");
    std::fs::create_dir_all(&extract_dir)?;

    info!("  Extracting PSV files from ZIP...");
    let extracted = extract_relevant_psv(zip_path, &extract_dir)?;
    info!("  Extracted {} files", extracted);

    let states = load_states(&extract_dir)?;
    info!("  Loaded {} states", states.len());

    let localities = load_localities(&extract_dir)?;
    info!("  Loaded {} localities", localities.len());

    let streets = load_street_localities(&extract_dir)?;
    info!("  Loaded {} street localities", streets.len());

    let geocodes = load_geocodes(&extract_dir)?;

    let count = stream_address_details(
        &extract_dir,
        &states,
        &localities,
        &streets,
        &geocodes,
        &mut emit,
    )?;

    std::fs::remove_dir_all(&extract_dir).ok();
    info!("Streamed {} G-NAF addresses", count);

    Ok(count)
}

// ───────────────────────────────────────────────────────────────────────────
// ZIP extraction — only extract the 5 file types we need
// ───────────────────────────────────────────────────────────────────────────

fn extract_relevant_psv(zip_path: &Path, out_dir: &Path) -> Result<usize> {
    let file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut count = 0;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        let full_name = entry.name().to_string();
        let filename = Path::new(&full_name)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        if !should_extract(&filename) {
            continue;
        }

        let out_path = out_dir.join(&filename);
        let mut out_file = std::fs::File::create(&out_path)?;
        std::io::copy(&mut entry, &mut out_file)?;
        count += 1;
    }

    Ok(count)
}

fn should_extract(filename: &str) -> bool {
    // STATE authority table
    if filename.ends_with("_STATE_psv.psv") {
        return true;
    }
    // LOCALITY (exclude ALIAS, NEIGHBOUR, POINT, and STREET_LOCALITY)
    if filename.ends_with("_LOCALITY_psv.psv")
        && !filename.contains("STREET_LOCALITY")
        && !filename.contains("LOCALITY_ALIAS")
        && !filename.contains("LOCALITY_NEIGHBOUR")
        && !filename.contains("LOCALITY_POINT")
    {
        return true;
    }
    // STREET_LOCALITY (exclude ALIAS, POINT)
    if filename.ends_with("_STREET_LOCALITY_psv.psv")
        && !filename.contains("ALIAS")
        && !filename.contains("POINT")
    {
        return true;
    }
    // ADDRESS_DEFAULT_GEOCODE
    if filename.ends_with("_ADDRESS_DEFAULT_GEOCODE_psv.psv") {
        return true;
    }
    // ADDRESS_DETAIL
    if filename.ends_with("_ADDRESS_DETAIL_psv.psv") {
        return true;
    }
    false
}

// ───────────────────────────────────────────────────────────────────────────
// PSV parsing helpers
// ───────────────────────────────────────────────────────────────────────────

/// Find column index by name in a pipe-delimited header.
fn col_index(header: &[&str], name: &str) -> Option<usize> {
    header.iter().position(|c| c.trim() == name)
}

/// Title-case a string: "NORTH SYDNEY" → "North Sydney"
fn title_case(s: &str) -> String {
    s.split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => {
                    c.to_uppercase().to_string() + &chars.as_str().to_lowercase()
                }
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

// ───────────────────────────────────────────────────────────────────────────
// Step 1: STATE — state_pid → abbreviation
// ───────────────────────────────────────────────────────────────────────────

fn load_states(dir: &Path) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let fname = path.file_name().unwrap_or_default().to_string_lossy();
        if !fname.ends_with("_STATE_psv.psv") {
            continue;
        }

        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut lines = reader.lines();

        let header = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty STATE file"))??;
        let header = header.trim_start_matches('\u{feff}');
        let cols: Vec<&str> = header.split('|').collect();

        let i_pid = col_index(&cols, "STATE_PID")
            .ok_or_else(|| anyhow::anyhow!("missing STATE_PID column"))?;
        let i_abbr = col_index(&cols, "STATE_ABBREVIATION")
            .ok_or_else(|| anyhow::anyhow!("missing STATE_ABBREVIATION column"))?;

        for line in lines {
            let line = line?;
            let fields: Vec<&str> = line.split('|').collect();
            if fields.len() <= i_pid.max(i_abbr) {
                continue;
            }
            map.insert(
                fields[i_pid].trim().to_owned(),
                fields[i_abbr].trim().to_owned(),
            );
        }
    }

    Ok(map)
}

// ───────────────────────────────────────────────────────────────────────────
// Step 2: LOCALITY — locality_pid → (name, postcode, state_pid)
// ───────────────────────────────────────────────────────────────────────────

struct LocalityInfo {
    name: String,
    postcode: String,
    #[allow(dead_code)]
    state_pid: String,
}

fn load_localities(dir: &Path) -> Result<HashMap<String, LocalityInfo>> {
    let mut map = HashMap::new();

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let fname = path.file_name().unwrap_or_default().to_string_lossy();
        if !fname.ends_with("_LOCALITY_psv.psv") || fname.contains("STREET_LOCALITY") {
            continue;
        }

        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut lines = reader.lines();

        let header = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty LOCALITY file: {}", fname))??;
        let header = header.trim_start_matches('\u{feff}');
        let cols: Vec<&str> = header.split('|').collect();

        let i_pid = col_index(&cols, "LOCALITY_PID")
            .ok_or_else(|| anyhow::anyhow!("missing LOCALITY_PID"))?;
        let i_name = col_index(&cols, "LOCALITY_NAME")
            .ok_or_else(|| anyhow::anyhow!("missing LOCALITY_NAME"))?;
        let i_postcode = col_index(&cols, "PRIMARY_POSTCODE");
        let i_state = col_index(&cols, "STATE_PID");

        for line in lines {
            let line = line?;
            let fields: Vec<&str> = line.split('|').collect();
            if fields.len() <= i_pid.max(i_name) {
                continue;
            }

            map.insert(
                fields[i_pid].trim().to_owned(),
                LocalityInfo {
                    name: title_case(fields[i_name].trim()),
                    postcode: i_postcode
                        .filter(|&idx| idx < fields.len())
                        .map(|idx| fields[idx].trim())
                        .unwrap_or("")
                        .to_owned(),
                    state_pid: i_state
                        .filter(|&idx| idx < fields.len())
                        .map(|idx| fields[idx].trim())
                        .unwrap_or("")
                        .to_owned(),
                },
            );
        }
    }

    Ok(map)
}

// ───────────────────────────────────────────────────────────────────────────
// Step 3: STREET_LOCALITY — street_locality_pid → (name, type, suffix, locality_pid)
// ───────────────────────────────────────────────────────────────────────────

struct StreetInfo {
    name: String,
    type_code: String,
    suffix_code: String,
    #[allow(dead_code)]
    locality_pid: String,
}

impl StreetInfo {
    /// Build full street name: "Smith Street North"
    fn full_name(&self) -> String {
        let mut parts = vec![self.name.clone()];
        if !self.type_code.is_empty() {
            parts.push(title_case(&self.type_code));
        }
        if !self.suffix_code.is_empty() {
            parts.push(title_case(&self.suffix_code));
        }
        parts.join(" ")
    }
}

fn load_street_localities(dir: &Path) -> Result<HashMap<String, StreetInfo>> {
    let mut map = HashMap::new();

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let fname = path.file_name().unwrap_or_default().to_string_lossy();
        if !fname.ends_with("_STREET_LOCALITY_psv.psv")
            || fname.contains("ALIAS")
            || fname.contains("POINT")
        {
            continue;
        }

        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut lines = reader.lines();

        let header = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty STREET_LOCALITY file: {}", fname))??;
        let header = header.trim_start_matches('\u{feff}');
        let cols: Vec<&str> = header.split('|').collect();

        let i_pid = col_index(&cols, "STREET_LOCALITY_PID")
            .ok_or_else(|| anyhow::anyhow!("missing STREET_LOCALITY_PID"))?;
        let i_name = col_index(&cols, "STREET_NAME")
            .ok_or_else(|| anyhow::anyhow!("missing STREET_NAME"))?;
        let i_type = col_index(&cols, "STREET_TYPE_CODE");
        let i_suffix = col_index(&cols, "STREET_SUFFIX_CODE");
        let i_locality = col_index(&cols, "LOCALITY_PID");

        for line in lines {
            let line = line?;
            let fields: Vec<&str> = line.split('|').collect();
            if fields.len() <= i_pid.max(i_name) {
                continue;
            }

            map.insert(
                fields[i_pid].trim().to_owned(),
                StreetInfo {
                    name: title_case(fields[i_name].trim()),
                    type_code: i_type
                        .filter(|&idx| idx < fields.len())
                        .map(|idx| fields[idx].trim())
                        .unwrap_or("")
                        .to_owned(),
                    suffix_code: i_suffix
                        .filter(|&idx| idx < fields.len())
                        .map(|idx| fields[idx].trim())
                        .unwrap_or("")
                        .to_owned(),
                    locality_pid: i_locality
                        .filter(|&idx| idx < fields.len())
                        .map(|idx| fields[idx].trim())
                        .unwrap_or("")
                        .to_owned(),
                },
            );
        }
    }

    Ok(map)
}

// ───────────────────────────────────────────────────────────────────────────
// Step 4: ADDRESS_DEFAULT_GEOCODE — address_detail_pid → (lat, lon)
// ───────────────────────────────────────────────────────────────────────────

/// On-disk geocode lookup: sorted (hash, lat_i32, lon_i32) entries.
/// Uses FNV-1a hash of PID string → 16-byte entries, sorted, mmap'd, binary searched.
/// Memory: ~0 (mmap'd file, OS pages only what's accessed).
struct DiskGeocodes {
    mmap: memmap2::Mmap,
    count: usize,
    _file: std::fs::File,
}

const GEO_ENTRY_BYTES: usize = 16; // u64 hash + i32 lat + i32 lon

/// FNV-1a hash for strings — fast, good distribution, deterministic.
fn fnv1a(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

impl DiskGeocodes {
    fn lookup(&self, pid: &str) -> Option<(f64, f64)> {
        let target = fnv1a(pid);
        let data: &[u8] = &self.mmap;
        // Binary search
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = mid * GEO_ENTRY_BYTES;
            let h = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            match h.cmp(&target) {
                std::cmp::Ordering::Equal => {
                    let lat = i32::from_le_bytes(data[off + 8..off + 12].try_into().unwrap());
                    let lon = i32::from_le_bytes(data[off + 12..off + 16].try_into().unwrap());
                    return Some((lat as f64 / 1_000_000.0, lon as f64 / 1_000_000.0));
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }
}

fn load_geocodes(dir: &Path) -> Result<DiskGeocodes> {
    use std::io::Write;

    // Phase 1: Write all geocodes to a temp file as (hash, lat_i32, lon_i32) entries
    let temp_path = dir.join("geocodes_unsorted.bin");
    let mut count = 0usize;
    {
        let mut writer = std::io::BufWriter::with_capacity(
            8 * 1024 * 1024,
            std::fs::File::create(&temp_path)?,
        );

        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            let fname = path.file_name().unwrap_or_default().to_string_lossy();
            if !fname.ends_with("_ADDRESS_DEFAULT_GEOCODE_psv.psv") {
                continue;
            }

            let reader = BufReader::new(std::fs::File::open(&path)?);
            let mut lines = reader.lines();

            let header = lines
                .next()
                .ok_or_else(|| anyhow::anyhow!("empty GEOCODE file: {}", fname))??;
            let header = header.trim_start_matches('\u{feff}');
            let cols: Vec<&str> = header.split('|').collect();

            let i_addr_pid = col_index(&cols, "ADDRESS_DETAIL_PID")
                .ok_or_else(|| anyhow::anyhow!("missing ADDRESS_DETAIL_PID in geocode"))?;
            let i_lon = col_index(&cols, "LONGITUDE")
                .ok_or_else(|| anyhow::anyhow!("missing LONGITUDE"))?;
            let i_lat = col_index(&cols, "LATITUDE")
                .ok_or_else(|| anyhow::anyhow!("missing LATITUDE"))?;

            for line in lines {
                let line = line?;
                let fields: Vec<&str> = line.split('|').collect();
                if fields.len() <= i_addr_pid.max(i_lon).max(i_lat) {
                    continue;
                }

                let lat: f64 = match fields[i_lat].trim().parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let lon: f64 = match fields[i_lon].trim().parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                if lat < -44.0 || lat > -10.0 || lon < 112.0 || lon > 154.0 {
                    continue;
                }

                let hash = fnv1a(fields[i_addr_pid].trim());
                let lat_i32 = (lat * 1_000_000.0) as i32;
                let lon_i32 = (lon * 1_000_000.0) as i32;
                writer.write_all(&hash.to_le_bytes())?;
                writer.write_all(&lat_i32.to_le_bytes())?;
                writer.write_all(&lon_i32.to_le_bytes())?;
                count += 1;
            }

            info!("    {} — {} geocodes so far", fname, count);
        }
        writer.flush()?;
    }

    // Phase 2: Sort by hash (in-place, chunked like SortedFileNodeCache)
    let sorted_path = dir.join("geocodes_sorted.bin");
    {
        let file_len = std::fs::metadata(&temp_path)?.len() as usize;
        let total = file_len / GEO_ENTRY_BYTES;
        let chunk_size = heimdall_core::node_cache::detect_memory_limit() / 4 / GEO_ENTRY_BYTES;
        let chunk_size = chunk_size.clamp(64 * 1024 * 1024 / GEO_ENTRY_BYTES, 500 * 1024 * 1024 / GEO_ENTRY_BYTES);

        let mut reader = std::io::BufReader::with_capacity(
            8 * 1024 * 1024,
            std::fs::File::open(&temp_path)?,
        );
        let mut chunk_paths: Vec<std::path::PathBuf> = Vec::new();
        let mut remaining = total;
        let mut chunk_idx = 0u32;

        while remaining > 0 {
            let n = remaining.min(chunk_size);
            let byte_size = n * GEO_ENTRY_BYTES;
            let mut buf = vec![0u8; byte_size];
            std::io::Read::read_exact(&mut reader, &mut buf)?;

            // Sort in-place by hash (first 8 bytes of each 16-byte entry)
            let entries = unsafe {
                std::slice::from_raw_parts_mut(
                    buf.as_mut_ptr() as *mut [u8; GEO_ENTRY_BYTES],
                    n,
                )
            };
            entries.sort_unstable_by(|a, b| {
                let ha = u64::from_le_bytes(a[0..8].try_into().unwrap());
                let hb = u64::from_le_bytes(b[0..8].try_into().unwrap());
                ha.cmp(&hb)
            });

            let chunk_path = dir.join(format!("geo_chunk_{:04}.bin", chunk_idx));
            let mut w = std::io::BufWriter::new(std::fs::File::create(&chunk_path)?);
            w.write_all(&buf)?;
            w.flush()?;
            chunk_paths.push(chunk_path);
            remaining -= n;
            chunk_idx += 1;
        }
        drop(reader);
        std::fs::remove_file(&temp_path).ok();

        // Merge sorted chunks
        if chunk_paths.len() == 1 {
            std::fs::rename(&chunk_paths[0], &sorted_path)?;
        } else {
            let mut sorted_writer = std::io::BufWriter::new(std::fs::File::create(&sorted_path)?);

            struct ChunkR { reader: std::io::BufReader<std::fs::File>, remaining: usize, current: Option<[u8; GEO_ENTRY_BYTES]> }
            impl ChunkR {
                fn advance(&mut self) -> std::io::Result<()> {
                    if self.remaining == 0 { self.current = None; return Ok(()); }
                    let mut buf = [0u8; GEO_ENTRY_BYTES];
                    std::io::Read::read_exact(&mut self.reader, &mut buf)?;
                    self.current = Some(buf);
                    self.remaining -= 1;
                    Ok(())
                }
            }

            let mut readers: Vec<ChunkR> = Vec::new();
            for p in &chunk_paths {
                let meta = std::fs::metadata(p)?;
                let n = meta.len() as usize / GEO_ENTRY_BYTES;
                let r = std::io::BufReader::new(std::fs::File::open(p)?);
                let mut cr = ChunkR { reader: r, remaining: n, current: None };
                cr.advance()?;
                readers.push(cr);
            }

            loop {
                let mut min_idx = None;
                let mut min_hash = u64::MAX;
                for (i, cr) in readers.iter().enumerate() {
                    if let Some(ref buf) = cr.current {
                        let h = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                        if h < min_hash { min_hash = h; min_idx = Some(i); }
                    }
                }
                let Some(idx) = min_idx else { break; };
                sorted_writer.write_all(readers[idx].current.as_ref().unwrap())?;
                readers[idx].advance()?;
            }
            sorted_writer.flush()?;
            for p in &chunk_paths { std::fs::remove_file(p).ok(); }
        }
    }

    // Phase 3: Mmap the sorted file
    let file = std::fs::OpenOptions::new().read(true).open(&sorted_path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };

    #[cfg(unix)]
    unsafe {
        libc::madvise(mmap.as_ptr() as *mut libc::c_void, mmap.len(), libc::MADV_RANDOM);
    }

    info!("  Geocode index: {} entries, {:.1} MB on disk", count, (count * GEO_ENTRY_BYTES) as f64 / 1e6);

    Ok(DiskGeocodes { mmap, count, _file: file })
}

// ───────────────────────────────────────────────────────────────────────────
// Step 5: ADDRESS_DETAIL — join everything → RawAddress
// ───────────────────────────────────────────────────────────────────────────

/// Streaming version: calls emit() for each joined address instead of collecting.
fn stream_address_details(
    dir: &Path,
    _states: &HashMap<String, String>,
    localities: &HashMap<String, LocalityInfo>,
    streets: &HashMap<String, StreetInfo>,
    geocodes: &DiskGeocodes,
    emit: &mut impl FnMut(RawAddress),
) -> Result<usize> {
    let mut count = 0usize;
    let mut skipped = 0usize;

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let fname = path.file_name().unwrap_or_default().to_string_lossy();
        if !fname.ends_with("_ADDRESS_DETAIL_psv.psv") {
            continue;
        }

        let reader = BufReader::new(std::fs::File::open(&path)?);
        let mut lines = reader.lines();

        let header = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty ADDRESS_DETAIL file: {}", fname))??;
        let header = header.trim_start_matches('\u{feff}');
        let cols: Vec<&str> = header.split('|').collect();

        let i_pid = col_index(&cols, "ADDRESS_DETAIL_PID")
            .ok_or_else(|| anyhow::anyhow!("missing ADDRESS_DETAIL_PID"))?;
        let i_num_first = col_index(&cols, "NUMBER_FIRST");
        let i_num_suffix = col_index(&cols, "NUMBER_FIRST_SUFFIX");
        let i_street_pid = col_index(&cols, "STREET_LOCALITY_PID")
            .ok_or_else(|| anyhow::anyhow!("missing STREET_LOCALITY_PID"))?;
        let i_locality_pid = col_index(&cols, "LOCALITY_PID")
            .ok_or_else(|| anyhow::anyhow!("missing LOCALITY_PID"))?;
        let i_postcode = col_index(&cols, "POSTCODE");
        let i_alias = col_index(&cols, "ALIAS_PRINCIPAL");

        let mut file_count = 0usize;

        for line in lines {
            let line = line?;
            let fields: Vec<&str> = line.split('|').collect();
            if fields.len() <= i_pid.max(i_street_pid).max(i_locality_pid) {
                skipped += 1;
                continue;
            }

            // Skip alias addresses — only keep principal ("P")
            if let Some(i_a) = i_alias {
                if i_a < fields.len() {
                    let alias_val = fields[i_a].trim();
                    if alias_val == "A" {
                        skipped += 1;
                        continue;
                    }
                }
            }

            let pid = fields[i_pid].trim();

            // Lookup geocode (lat/lon) from disk-backed sorted index
            let (lat, lon) = match geocodes.lookup(pid) {
                Some(coords) => coords,
                None => {
                    skipped += 1;
                    continue;
                }
            };

            // Build house number: NUMBER_FIRST + NUMBER_FIRST_SUFFIX
            let number = i_num_first
                .filter(|&idx| idx < fields.len())
                .map(|idx| fields[idx].trim())
                .unwrap_or("");
            if number.is_empty() {
                skipped += 1;
                continue;
            }
            let suffix = i_num_suffix
                .filter(|&idx| idx < fields.len())
                .map(|idx| fields[idx].trim())
                .unwrap_or("");
            let housenumber = if suffix.is_empty() {
                number.to_owned()
            } else {
                format!("{}{}", number, suffix)
            };

            // Lookup street name
            let street_pid = fields[i_street_pid].trim();
            let street_name = match streets.get(street_pid) {
                Some(si) => si.full_name(),
                None => {
                    skipped += 1;
                    continue;
                }
            };

            // Lookup locality (city/suburb)
            let locality_pid = fields[i_locality_pid].trim();
            let (city, fallback_postcode) = match localities.get(locality_pid) {
                Some(li) => (Some(li.name.clone()), &li.postcode),
                None => (None, &String::new()),
            };

            // Postcode: prefer ADDRESS_DETAIL.POSTCODE, fallback to LOCALITY.PRIMARY_POSTCODE
            let postcode = i_postcode
                .filter(|&idx| idx < fields.len())
                .map(|idx| fields[idx].trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_owned())
                .or_else(|| {
                    if fallback_postcode.is_empty() {
                        None
                    } else {
                        Some(fallback_postcode.clone())
                    }
                });

            emit(RawAddress {
                osm_id: 0,
                street: street_name,
                housenumber,
                postcode,
                city,
                lat,
                lon,
            });
            file_count += 1;
            count += 1;
        }

        info!("    {} — {} addresses", fname, file_count);
    }

    info!(
        "  Join complete: {} addresses ({} skipped)",
        count,
        skipped,
    );

    Ok(count)
}
