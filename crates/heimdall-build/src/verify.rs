/// verify.rs — Index verification and comparison harness.
///
/// Provides two subcommands:
///
///   `dump-index`  — Dumps every data structure in an index to deterministic
///                   text files suitable for diff. Also computes a SHA-256
///                   digest of all content.
///
///   `verify`      — Compares two index directories (or two prior dumps) and
///                   reports any differences: missing/added/changed records,
///                   FST entries, addresses, admin entries, geohash entries.
///
/// The dump format is line-oriented, sorted, and fully deterministic so that
/// standard `diff` or `sha256sum` can be used for manual inspection.

use std::path::{Path, PathBuf};
use std::io::{BufWriter, Write, BufRead, BufReader};
use std::fs::File;
use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use fst::{Map, Streamer};
use memmap2::Mmap;
use sha2::{Sha256, Digest};
use tracing::info;

use heimdall_core::record_store::RecordStore;
use heimdall_core::addr_store::AddrStore;
use heimdall_core::types::AdminEntry;
use heimdall_core::reverse::{GeohashEntry, GeohashIndex, GEOHASH_MAGIC};

// ---------------------------------------------------------------------------
// Dump: write every datum from an index directory into canonical text files
// ---------------------------------------------------------------------------

/// Result of a dump operation.
pub struct DumpResult {
    /// Directory where dump files were written
    pub output_dir: PathBuf,
    /// SHA-256 of the concatenated dump content (hex-encoded)
    pub digest: String,
    /// Counts for summary
    pub fst_exact_keys: usize,
    pub fst_phonetic_keys: usize,
    pub fst_addr_keys: usize,
    pub record_count: usize,
    pub admin_count: usize,
    pub street_count: usize,
    pub house_count: usize,
    pub geohash_count: usize,
}

/// Dump the complete contents of an index directory into canonical text files.
///
/// Files created in `output_dir`:
///   fst_exact.txt      — one "key\tvalue" per line, sorted lexicographically
///   fst_phonetic.txt   — same format
///   fst_addr.txt       — same format
///   records.txt        — one record per line: "id\tname\talt_names\tlat\tlon\ttype\tadmin1\tadmin2\timportance\tosm_id\tflags"
///   admin.txt          — one admin entry per line: "id\tname\tparent_id\tlat\tlon\ttype"
///   addresses.txt      — one street per line with all houses expanded
///   geohash.txt        — one entry per line: "hash\trecord_id"
///   DIGEST             — single line with the SHA-256 of all the above
pub fn dump_index(index_dir: &Path, output_dir: &Path) -> Result<DumpResult> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating output dir {}", output_dir.display()))?;

    let mut hasher = Sha256::new();
    let mut result = DumpResult {
        output_dir: output_dir.to_owned(),
        digest: String::new(),
        fst_exact_keys: 0,
        fst_phonetic_keys: 0,
        fst_addr_keys: 0,
        record_count: 0,
        admin_count: 0,
        street_count: 0,
        house_count: 0,
        geohash_count: 0,
    };

    // --- FST exact ---
    let fst_exact_path = index_dir.join("fst_exact.fst");
    if fst_exact_path.exists() {
        info!("Dumping fst_exact.fst ...");
        result.fst_exact_keys = dump_fst(
            &fst_exact_path,
            &output_dir.join("fst_exact.txt"),
            &mut hasher,
        )?;
        info!("  {} keys", result.fst_exact_keys);
    }

    // --- FST phonetic ---
    let fst_phonetic_path = index_dir.join("fst_phonetic.fst");
    if fst_phonetic_path.exists() {
        info!("Dumping fst_phonetic.fst ...");
        result.fst_phonetic_keys = dump_fst(
            &fst_phonetic_path,
            &output_dir.join("fst_phonetic.txt"),
            &mut hasher,
        )?;
        info!("  {} keys", result.fst_phonetic_keys);
    }

    // --- FST addr ---
    let fst_addr_path = index_dir.join("fst_addr.fst");
    if fst_addr_path.exists() {
        info!("Dumping fst_addr.fst ...");
        result.fst_addr_keys = dump_fst(
            &fst_addr_path,
            &output_dir.join("fst_addr.txt"),
            &mut hasher,
        )?;
        info!("  {} keys", result.fst_addr_keys);
    }

    // --- Records ---
    let records_path = index_dir.join("records.bin");
    if records_path.exists() {
        info!("Dumping records.bin ...");
        result.record_count = dump_records(
            &records_path,
            &output_dir.join("records.txt"),
            &mut hasher,
        )?;
        info!("  {} records", result.record_count);
    }

    // --- Admin ---
    let admin_path = index_dir.join("admin.bin");
    if admin_path.exists() {
        info!("Dumping admin.bin ...");
        result.admin_count = dump_admin(
            &admin_path,
            &output_dir.join("admin.txt"),
            &mut hasher,
        )?;
        info!("  {} admin entries", result.admin_count);
    }

    // --- Addresses ---
    let addr_path = index_dir.join("addr_streets.bin");
    if addr_path.exists() {
        info!("Dumping addr_streets.bin ...");
        let (streets, houses) = dump_addresses(
            &addr_path,
            &output_dir.join("addresses.txt"),
            &mut hasher,
        )?;
        result.street_count = streets;
        result.house_count = houses;
        info!("  {} streets, {} houses", streets, houses);
    }

    // --- Geohash index ---
    let geohash_path = index_dir.join("geohash_index.bin");
    if geohash_path.exists() {
        info!("Dumping geohash_index.bin ...");
        result.geohash_count = dump_geohash(
            &geohash_path,
            &output_dir.join("geohash.txt"),
            &mut hasher,
        )?;
        info!("  {} geohash entries", result.geohash_count);
    }

    // --- Finalize digest ---
    let digest_hex = format!("{:x}", hasher.finalize());
    result.digest = digest_hex.clone();

    // Write digest file
    std::fs::write(output_dir.join("DIGEST"), format!("{}\n", digest_hex))?;

    info!("Dump complete → {}", output_dir.display());
    info!("SHA-256 digest: {}", digest_hex);

    Ok(result)
}

// ---------------------------------------------------------------------------
// Dump helpers
// ---------------------------------------------------------------------------

/// Dump an FST map to a text file. Returns the number of keys written.
/// FST keys are already in sorted order (by construction).
fn dump_fst(fst_path: &Path, out_path: &Path, hasher: &mut Sha256) -> Result<usize> {
    let data = heimdall_core::compressed_io::read_maybe_compressed(fst_path)
        .with_context(|| format!("opening {}", fst_path.display()))?;
    let map = Map::new(data).map_err(|e| anyhow::anyhow!("FST error: {}", e))?;

    let mut w = BufWriter::new(File::create(out_path)?);
    let mut stream = map.stream();
    let mut count = 0usize;

    while let Some((key, value)) = stream.next() {
        // Key may contain non-UTF-8 bytes (phonetic codes), so hex-escape them
        let key_str = String::from_utf8_lossy(key);
        let line = format!("{}\t{}\n", key_str, value);
        w.write_all(line.as_bytes())?;
        hasher.update(line.as_bytes());
        count += 1;
    }

    w.flush()?;
    Ok(count)
}

/// Dump all records from a RecordStore to a text file. Returns record count.
fn dump_records(records_path: &Path, out_path: &Path, hasher: &mut Sha256) -> Result<usize> {
    let store = RecordStore::open(records_path)
        .map_err(|e| anyhow::anyhow!("RecordStore: {}", e))?;

    let mut w = BufWriter::new(File::create(out_path)?);
    let count = store.len();

    // Header line
    let header = "id\tprimary_name\talt_names\tlat\tlon\tplace_type\tadmin1_id\tadmin2_id\timportance\tosm_id\tflags\n";
    w.write_all(header.as_bytes())?;
    hasher.update(header.as_bytes());

    for id in 0..count as u32 {
        let record = store.get(id)
            .map_err(|e| anyhow::anyhow!("record {}: {}", id, e))?;

        let names = store.all_names(&record);
        let primary = names.first().map(|s| s.as_str()).unwrap_or("");
        let alts: Vec<&str> = if names.len() > 1 { names[1..].iter().map(|s| s.as_str()).collect() } else { vec![] };
        let alts_str = alts.join("|");

        let place_type_str = format!("{:?}", record.place_type);

        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            id,
            primary,
            alts_str,
            record.coord.lat,
            record.coord.lon,
            place_type_str,
            record.admin1_id,
            record.admin2_id,
            record.importance,
            record.osm_id,
            record.flags,
        );
        w.write_all(line.as_bytes())?;
        hasher.update(line.as_bytes());
    }

    w.flush()?;
    Ok(count)
}

/// Dump admin entries from admin.bin to a text file. Returns entry count.
fn dump_admin(admin_path: &Path, out_path: &Path, hasher: &mut Sha256) -> Result<usize> {
    let bytes = heimdall_core::compressed_io::read_maybe_compressed(admin_path)?;
    let entries: Vec<AdminEntry> = postcard::from_bytes(&bytes)
        .unwrap_or_else(|_| bincode::deserialize(&bytes).expect("admin.bin deserialize"));

    let mut w = BufWriter::new(File::create(out_path)?);

    let header = "id\tname\tparent_id\tlat\tlon\tplace_type\n";
    w.write_all(header.as_bytes())?;
    hasher.update(header.as_bytes());

    for entry in &entries {
        let parent = entry.parent_id.map(|p| p.to_string()).unwrap_or_default();
        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{:?}\n",
            entry.id,
            entry.name,
            parent,
            entry.coord.lat,
            entry.coord.lon,
            entry.place_type,
        );
        w.write_all(line.as_bytes())?;
        hasher.update(line.as_bytes());
    }

    w.flush()?;
    Ok(entries.len())
}

/// Dump all addresses (streets + houses) from addr_streets.bin. Returns (street_count, house_count).
fn dump_addresses(
    addr_path: &Path,
    out_path: &Path,
    hasher: &mut Sha256,
) -> Result<(usize, usize)> {
    let store = AddrStore::open(addr_path)
        .map_err(|e| anyhow::anyhow!("AddrStore: {}", e))?
        .ok_or_else(|| anyhow::anyhow!("AddrStore not found at {}", addr_path.display()))?;

    let mut w = BufWriter::new(File::create(out_path)?);

    let header = "street_id\tstreet_name\tbase_lat\tbase_lon\tpostcode\thouse_number\thouse_suffix\tdelta_lat\tdelta_lon\n";
    w.write_all(header.as_bytes())?;
    hasher.update(header.as_bytes());

    let street_count = store.street_count();
    let mut total_houses = 0usize;

    for sid in 0..street_count as u32 {
        let header_rec = match store.get_street(sid) {
            Some(h) => h,
            None => continue,
        };

        let name = store.street_name(&header_rec);
        let houses = store.street_houses(&header_rec);

        if houses.is_empty() {
            // Still emit the street with no houses
            let line = format!(
                "{}\t{}\t{}\t{}\t{}\t\t\t\t\n",
                sid, name, header_rec.base_lat, header_rec.base_lon, header_rec.postcode,
            );
            w.write_all(line.as_bytes())?;
            hasher.update(line.as_bytes());
        } else {
            for house in &houses {
                let suffix_char = if house.suffix > 0 {
                    format!("{}", (b'A' + house.suffix - 1) as char)
                } else {
                    String::new()
                };
                let line = format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    sid,
                    name,
                    header_rec.base_lat,
                    header_rec.base_lon,
                    header_rec.postcode,
                    house.number,
                    suffix_char,
                    house.delta_lat,
                    house.delta_lon,
                );
                w.write_all(line.as_bytes())?;
                hasher.update(line.as_bytes());
                total_houses += 1;
            }
        }
    }

    w.flush()?;
    Ok((street_count, total_houses))
}

/// Dump the geohash index to a text file. Returns entry count.
fn dump_geohash(geohash_path: &Path, out_path: &Path, hasher: &mut Sha256) -> Result<usize> {
    // Read the raw binary — may be zstd-compressed, may be V1 or V2 format
    let data = heimdall_core::compressed_io::read_maybe_compressed(geohash_path)?;

    let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
    const GEOHASH_MAGIC_V1: u32 = 0x47484958; // "GHIX"
    const GEOHASH_MAGIC_V2: u32 = 0x47484932; // "GHI2"

    let mut w = BufWriter::new(File::create(out_path)?);
    let header_line = "hash\trecord_id\n";
    w.write_all(header_line.as_bytes())?;
    hasher.update(header_line.as_bytes());

    let mut total = 0usize;

    if magic == GEOHASH_MAGIC_V1 {
        // V1: flat sorted array of (u64 hash, u32 record_id) after 8-byte header
        let count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let entry_size = 16usize; // u64(8) + u32(4) + 4 bytes alignment padding
        let data_start = 8usize;
        for i in 0..count {
            let offset = data_start + i * entry_size;
            let hash = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            let record_id = u32::from_le_bytes(data[offset + 8..offset + 12].try_into().unwrap());
            let line = format!("{}\t{}\n", hash, record_id);
            w.write_all(line.as_bytes())?;
            hasher.update(line.as_bytes());
        }
        total = count;
    } else if magic == GEOHASH_MAGIC_V2 {
        // V2: cell directory + delta-varint data region
        let cell_count = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
        let _total_records = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        let header_size = 12usize;
        let cell_entry_size = 14usize; // u64(8) + u32(4) + u16(2)
        let data_region_offset = header_size + cell_count * cell_entry_size;

        for c in 0..cell_count {
            let base = header_size + c * cell_entry_size;
            let cell_hash = u64::from_le_bytes(data[base..base+8].try_into().unwrap());
            let data_offset = u32::from_le_bytes(data[base+8..base+12].try_into().unwrap()) as usize;
            let id_count = u16::from_le_bytes(data[base+12..base+14].try_into().unwrap()) as usize;

            let mut pos = data_region_offset + data_offset;
            let mut prev_id: u32 = 0;
            for j in 0..id_count {
                let (delta, consumed) = decode_varint_simple(&data[pos..]);
                pos += consumed;
                if j == 0 {
                    prev_id = delta;
                } else {
                    prev_id = prev_id.wrapping_add(delta);
                }
                let line = format!("{}\t{}\n", cell_hash, prev_id);
                w.write_all(line.as_bytes())?;
                hasher.update(line.as_bytes());
                total += 1;
            }
        }
    } else {
        bail!("invalid geohash magic: {:#x}", magic);
    }

    w.flush()?;
    Ok(total)
}

fn decode_varint_simple(data: &[u8]) -> (u32, usize) {
    let mut val: u32 = 0;
    let mut shift = 0u32;
    for (i, &b) in data.iter().enumerate() {
        val |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            return (val, i + 1);
        }
        shift += 7;
        if shift >= 35 { return (val, i + 1); }
    }
    (val, data.len())
}

// ---------------------------------------------------------------------------
// Verify: compare two index directories (or two dump directories)
// ---------------------------------------------------------------------------

/// Result of a verification comparison.
#[allow(dead_code)]
pub struct VerifyResult {
    pub identical: bool,
    pub digest_a: String,
    pub digest_b: String,
    pub summary: Vec<String>,
    pub total_differences: usize,
}

/// Compare two index directories by dumping both and diffing the results.
/// If `dump_dir` is provided, dump files are kept there. Otherwise uses temp dirs.
pub fn verify_indices(
    index_a: &Path,
    index_b: &Path,
    dump_dir: Option<&Path>,
) -> Result<VerifyResult> {
    let base_dir = if let Some(d) = dump_dir {
        d.to_owned()
    } else {
        std::env::temp_dir().join("heimdall-verify")
    };
    std::fs::create_dir_all(&base_dir)?;

    let dump_a_dir = base_dir.join("index_a");
    let dump_b_dir = base_dir.join("index_b");

    info!("Dumping index A: {} → {}", index_a.display(), dump_a_dir.display());
    let result_a = dump_index(index_a, &dump_a_dir)?;

    info!("Dumping index B: {} → {}", index_b.display(), dump_b_dir.display());
    let result_b = dump_index(index_b, &dump_b_dir)?;

    info!("Comparing dumps...");
    let mut summary = Vec::new();
    let mut total_diffs = 0usize;

    // Quick digest check
    if result_a.digest == result_b.digest {
        summary.push("IDENTICAL: SHA-256 digests match — indices are byte-for-byte equivalent.".to_string());
        return Ok(VerifyResult {
            identical: true,
            digest_a: result_a.digest,
            digest_b: result_b.digest,
            summary,
            total_differences: 0,
        });
    }

    summary.push(format!("DIFFERENT: digest A = {}", result_a.digest));
    summary.push(format!("           digest B = {}", result_b.digest));
    summary.push(String::new());

    // Compare counts
    let count_checks = [
        ("FST exact keys", result_a.fst_exact_keys, result_b.fst_exact_keys),
        ("FST phonetic keys", result_a.fst_phonetic_keys, result_b.fst_phonetic_keys),
        ("FST addr keys", result_a.fst_addr_keys, result_b.fst_addr_keys),
        ("Records", result_a.record_count, result_b.record_count),
        ("Admin entries", result_a.admin_count, result_b.admin_count),
        ("Streets", result_a.street_count, result_b.street_count),
        ("House entries", result_a.house_count, result_b.house_count),
        ("Geohash entries", result_a.geohash_count, result_b.geohash_count),
    ];

    summary.push("=== Count comparison ===".to_string());
    for (label, a, b) in &count_checks {
        if a != b {
            summary.push(format!("  {} DIFFERS: A={}, B={} (delta={})", label, a, b, *b as i64 - *a as i64));
        } else {
            summary.push(format!("  {} OK: {}", label, a));
        }
    }
    summary.push(String::new());

    // Detailed file-level comparison
    let files_to_compare = [
        ("fst_exact.txt", "FST exact"),
        ("fst_phonetic.txt", "FST phonetic"),
        ("fst_addr.txt", "FST addr"),
        ("records.txt", "Records"),
        ("admin.txt", "Admin"),
        ("addresses.txt", "Addresses"),
        ("geohash.txt", "Geohash"),
    ];

    for (filename, label) in &files_to_compare {
        let path_a = dump_a_dir.join(filename);
        let path_b = dump_b_dir.join(filename);

        if !path_a.exists() && !path_b.exists() {
            continue;
        }
        if !path_a.exists() {
            summary.push(format!("=== {} ===", label));
            summary.push(format!("  MISSING in A (present in B)"));
            total_diffs += 1;
            continue;
        }
        if !path_b.exists() {
            summary.push(format!("=== {} ===", label));
            summary.push(format!("  MISSING in B (present in A)"));
            total_diffs += 1;
            continue;
        }

        let diffs = compare_files(&path_a, &path_b, 20)?;
        if diffs.is_empty() {
            summary.push(format!("=== {} === IDENTICAL", label));
        } else {
            summary.push(format!("=== {} === {} differences", label, diffs.len()));
            for d in &diffs {
                summary.push(format!("  {}", d));
            }
            total_diffs += diffs.len();
        }
    }

    Ok(VerifyResult {
        identical: false,
        digest_a: result_a.digest,
        digest_b: result_b.digest,
        summary,
        total_differences: total_diffs,
    })
}

/// Compare two dumps that have already been generated.
#[allow(dead_code)]
pub fn verify_dumps(dump_a: &Path, dump_b: &Path) -> Result<VerifyResult> {
    // Read digests
    let digest_a = std::fs::read_to_string(dump_a.join("DIGEST"))
        .unwrap_or_default().trim().to_string();
    let digest_b = std::fs::read_to_string(dump_b.join("DIGEST"))
        .unwrap_or_default().trim().to_string();

    let mut summary = Vec::new();
    let mut total_diffs = 0usize;

    if digest_a == digest_b && !digest_a.is_empty() {
        summary.push("IDENTICAL: SHA-256 digests match.".to_string());
        return Ok(VerifyResult {
            identical: true,
            digest_a,
            digest_b,
            summary,
            total_differences: 0,
        });
    }

    if !digest_a.is_empty() && !digest_b.is_empty() {
        summary.push(format!("DIFFERENT: digest A = {}", digest_a));
        summary.push(format!("           digest B = {}", digest_b));
        summary.push(String::new());
    }

    let files_to_compare = [
        ("fst_exact.txt", "FST exact"),
        ("fst_phonetic.txt", "FST phonetic"),
        ("fst_addr.txt", "FST addr"),
        ("records.txt", "Records"),
        ("admin.txt", "Admin"),
        ("addresses.txt", "Addresses"),
        ("geohash.txt", "Geohash"),
    ];

    for (filename, label) in &files_to_compare {
        let path_a = dump_a.join(filename);
        let path_b = dump_b.join(filename);

        if !path_a.exists() && !path_b.exists() {
            continue;
        }
        if !path_a.exists() {
            summary.push(format!("=== {} === MISSING in A", label));
            total_diffs += 1;
            continue;
        }
        if !path_b.exists() {
            summary.push(format!("=== {} === MISSING in B", label));
            total_diffs += 1;
            continue;
        }

        let diffs = compare_files(&path_a, &path_b, 20)?;
        if diffs.is_empty() {
            summary.push(format!("=== {} === IDENTICAL", label));
        } else {
            summary.push(format!("=== {} === {} differences", label, diffs.len()));
            for d in &diffs {
                summary.push(format!("  {}", d));
            }
            total_diffs += diffs.len();
        }
    }

    Ok(VerifyResult {
        identical: total_diffs == 0,
        digest_a,
        digest_b,
        summary,
        total_differences: total_diffs,
    })
}

// ---------------------------------------------------------------------------
// File comparison — key-based diff for tab-separated dump files
// ---------------------------------------------------------------------------

/// Compare two tab-separated dump files line by line.
///
/// Returns a list of human-readable difference descriptions.
/// `max_diffs` limits the number of differences reported (for large indices).
fn compare_files(path_a: &Path, path_b: &Path, max_diffs: usize) -> Result<Vec<String>> {
    let lines_a = read_lines(path_a)?;
    let lines_b = read_lines(path_b)?;

    // Build maps: first column (key) → full line
    // For FST files the key is the FST key, for records it's the ID, etc.
    let map_a = build_line_map(&lines_a);
    let map_b = build_line_map(&lines_b);

    let mut diffs = Vec::new();

    // Find lines only in A (deleted from B)
    for (key, line_a) in &map_a {
        if !map_b.contains_key(key) {
            diffs.push(format!("ONLY_IN_A: {}", truncate(line_a, 120)));
            if diffs.len() >= max_diffs {
                diffs.push(format!("... (truncated at {} differences)", max_diffs));
                return Ok(diffs);
            }
        }
    }

    // Find lines only in B (added)
    for (key, line_b) in &map_b {
        if !map_a.contains_key(key) {
            diffs.push(format!("ONLY_IN_B: {}", truncate(line_b, 120)));
            if diffs.len() >= max_diffs {
                diffs.push(format!("... (truncated at {} differences)", max_diffs));
                return Ok(diffs);
            }
        }
    }

    // Find changed lines (same key, different content)
    for (key, line_a) in &map_a {
        if let Some(line_b) = map_b.get(key) {
            if line_a != line_b {
                diffs.push(format!("CHANGED key={}:", truncate(key, 60)));
                diffs.push(format!("  A: {}", truncate(line_a, 120)));
                diffs.push(format!("  B: {}", truncate(line_b, 120)));
                if diffs.len() >= max_diffs {
                    diffs.push(format!("... (truncated at {} differences)", max_diffs));
                    return Ok(diffs);
                }
            }
        }
    }

    Ok(diffs)
}

fn read_lines(path: &Path) -> Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = Vec::new();
    for line in reader.lines() {
        lines.push(line?);
    }
    Ok(lines)
}

/// Build a map from the first tab-separated column to the full line.
/// If there are duplicate keys (shouldn't happen in a well-formed dump),
/// only the last occurrence is kept.
fn build_line_map(lines: &[String]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for line in lines {
        // Skip header lines
        if line.starts_with("id\t") || line.starts_with("hash\t") || line.starts_with("street_id\t") {
            continue;
        }
        let key = line.split('\t').next().unwrap_or(line).to_string();
        map.insert(key.clone(), line.clone());
    }
    map
}

fn truncate(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len {
        s
    } else {
        &s[..max_len]
    }
}

// ---------------------------------------------------------------------------
// Functional verification: test queries against two indices
// ---------------------------------------------------------------------------

/// Run a set of test queries against two index directories and compare results.
/// Queries are read from a file (one per line) or generated from the index.
pub fn functional_verify(
    index_a: &Path,
    index_b: &Path,
    queries_file: Option<&Path>,
    sample_count: usize,
) -> Result<FunctionalResult> {
    use heimdall_core::index::HeimdallIndex;
    use heimdall_core::types::GeoQuery;

    let idx_a = HeimdallIndex::open(index_a)
        .map_err(|e| anyhow::anyhow!("opening index A: {}", e))?;
    let idx_b = HeimdallIndex::open(index_b)
        .map_err(|e| anyhow::anyhow!("opening index B: {}", e))?;

    // Collect queries
    let queries: Vec<String> = if let Some(qf) = queries_file {
        let file = File::open(qf)?;
        BufReader::new(file).lines().collect::<Result<Vec<_>, _>>()?
    } else {
        // Sample queries from index A's FST
        sample_queries_from_fst(index_a, sample_count)?
    };

    info!("Running {} queries against both indices...", queries.len());

    let mut identical = 0usize;
    let mut different = 0usize;
    let mut only_a = 0usize;
    let mut only_b = 0usize;
    let mut both_miss = 0usize;
    let mut diff_details: Vec<String> = Vec::new();

    for (i, q) in queries.iter().enumerate() {
        let query = GeoQuery::new(q.as_str());

        let results_a = idx_a.geocode(&query);
        let results_b = idx_b.geocode(&query);

        let has_a = !results_a.is_empty();
        let has_b = !results_b.is_empty();

        match (has_a, has_b) {
            (false, false) => {
                both_miss += 1;
            }
            (true, false) => {
                only_a += 1;
                if diff_details.len() < 50 {
                    diff_details.push(format!(
                        "ONLY_A: q=\"{}\" → A={} ({:.6},{:.6})",
                        q,
                        results_a[0].name,
                        results_a[0].coord.lat_f64(),
                        results_a[0].coord.lon_f64(),
                    ));
                }
            }
            (false, true) => {
                only_b += 1;
                if diff_details.len() < 50 {
                    diff_details.push(format!(
                        "ONLY_B: q=\"{}\" → B={} ({:.6},{:.6})",
                        q,
                        results_b[0].name,
                        results_b[0].coord.lat_f64(),
                        results_b[0].coord.lon_f64(),
                    ));
                }
            }
            (true, true) => {
                // Compare first result: same name + same coordinate?
                let a = &results_a[0];
                let b = &results_b[0];
                let same_name = a.name == b.name;
                let same_coord = a.coord.lat == b.coord.lat && a.coord.lon == b.coord.lon;

                if same_name && same_coord {
                    identical += 1;
                } else {
                    different += 1;
                    if diff_details.len() < 50 {
                        let dist = a.coord.distance_m(&b.coord);
                        diff_details.push(format!(
                            "DIFFER: q=\"{}\" → A=\"{}\" ({:.6},{:.6}) vs B=\"{}\" ({:.6},{:.6}) dist={:.0}m",
                            q,
                            a.name, a.coord.lat_f64(), a.coord.lon_f64(),
                            b.name, b.coord.lat_f64(), b.coord.lon_f64(),
                            dist,
                        ));
                    }
                }
            }
        }

        if (i + 1) % 10000 == 0 {
            info!("  {}/{} queries done...", i + 1, queries.len());
        }
    }

    Ok(FunctionalResult {
        total_queries: queries.len(),
        identical,
        different,
        only_a,
        only_b,
        both_miss,
        diff_details,
    })
}

pub struct FunctionalResult {
    pub total_queries: usize,
    pub identical: usize,
    pub different: usize,
    pub only_a: usize,
    pub only_b: usize,
    pub both_miss: usize,
    pub diff_details: Vec<String>,
}

impl FunctionalResult {
    pub fn print_summary(&self) {
        println!("=== Functional Verification Results ===");
        println!("Total queries:       {}", self.total_queries);
        println!("Identical results:   {} ({:.1}%)", self.identical, self.identical as f64 / self.total_queries as f64 * 100.0);
        println!("Different results:   {} ({:.1}%)", self.different, self.different as f64 / self.total_queries as f64 * 100.0);
        println!("Only in A:           {} ({:.1}%)", self.only_a, self.only_a as f64 / self.total_queries as f64 * 100.0);
        println!("Only in B:           {} ({:.1}%)", self.only_b, self.only_b as f64 / self.total_queries as f64 * 100.0);
        println!("Both miss:           {} ({:.1}%)", self.both_miss, self.both_miss as f64 / self.total_queries as f64 * 100.0);

        let data_loss = self.only_a > 0 || self.different > 0;
        if data_loss {
            println!();
            println!("WARNING: Potential data loss detected!");
            println!("  {} queries returned results in A but not B", self.only_a);
            println!("  {} queries returned different results", self.different);
        } else if self.only_b > 0 {
            println!();
            println!("NOTE: B has {} additional results not in A (data gain, not loss)", self.only_b);
        } else {
            println!();
            println!("PASS: No data loss detected.");
        }

        if !self.diff_details.is_empty() {
            println!();
            println!("=== Sample differences ===");
            for d in &self.diff_details {
                println!("  {}", d);
            }
        }
    }
}

/// Sample query strings from an index's exact FST by iterating and picking every Nth key.
fn sample_queries_from_fst(index_dir: &Path, count: usize) -> Result<Vec<String>> {
    let fst_path = index_dir.join("fst_exact.fst");
    let file = File::open(&fst_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let map = Map::new(mmap).map_err(|e| anyhow::anyhow!("FST: {}", e))?;

    // Count total keys first
    let mut total = 0usize;
    {
        let mut stream = map.stream();
        while stream.next().is_some() {
            total += 1;
        }
    }

    if total == 0 {
        return Ok(vec![]);
    }

    let step = if total <= count { 1 } else { total / count };
    let mut queries = Vec::with_capacity(count.min(total));
    let mut stream = map.stream();
    let mut i = 0usize;

    while let Some((key, _value)) = stream.next() {
        if i % step == 0 && queries.len() < count {
            if let Ok(s) = std::str::from_utf8(key) {
                queries.push(s.to_string());
            }
        }
        i += 1;
    }

    Ok(queries)
}
