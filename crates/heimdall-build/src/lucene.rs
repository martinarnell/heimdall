/// lucene.rs — Read stored fields from a Lucene 5.x / Elasticsearch 2-5 index
///
/// Extracts the `_source` JSON from every document in the index.
/// Used to read Photon/Elasticsearch country dumps (tar.bz2 from Graphhopper).
///
/// Format: Lucene50StoredFieldsFormat (big-endian ints, LZ4 compression).
/// Reference: https://lucene.apache.org/core/5_2_0/core/org/apache/lucene/codecs/lucene50/

use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use tracing::info;

// ---------------------------------------------------------------------------
// Primitive readers (big-endian, Lucene 5.x convention)
// ---------------------------------------------------------------------------

fn read_u8(data: &[u8], pos: &mut usize) -> u8 {
    let v = data[*pos];
    *pos += 1;
    v
}

fn read_i32(data: &[u8], pos: &mut usize) -> i32 {
    let v = i32::from_be_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    v
}

fn read_i64(data: &[u8], pos: &mut usize) -> i64 {
    let v = i64::from_be_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    v
}

fn read_u64_be(data: &[u8], pos: &mut usize) -> u64 {
    let v = u64::from_be_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    v
}

fn read_vint(data: &[u8], pos: &mut usize) -> u32 {
    let mut result: u32 = 0;
    let mut shift = 0;
    loop {
        let b = data[*pos];
        *pos += 1;
        result |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn read_vlong(data: &[u8], pos: &mut usize) -> u64 {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let b = data[*pos];
        *pos += 1;
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

fn read_string(data: &[u8], pos: &mut usize) -> String {
    let len = read_vint(data, pos) as usize;
    let s = String::from_utf8_lossy(&data[*pos..*pos + len]).to_string();
    *pos += len;
    s
}

fn skip_string(data: &[u8], pos: &mut usize) {
    let len = read_vint(data, pos) as usize;
    *pos += len;
}

// ---------------------------------------------------------------------------
// Codec header / footer
// ---------------------------------------------------------------------------

const CODEC_MAGIC: i32 = 0x3FD76C17;

/// Parse codec header, return (codec_name, version). Advances pos past the header.
fn read_codec_header(data: &[u8], pos: &mut usize) -> Result<(String, i32)> {
    let magic = read_i32(data, pos);
    if magic != CODEC_MAGIC {
        bail!("Bad codec magic: {:#x} (expected {:#x})", magic, CODEC_MAGIC);
    }
    let codec_name = read_string(data, pos);
    let version = read_i32(data, pos);
    Ok((codec_name, version))
}

/// Parse Lucene 5.x IndexHeader (codec header + objectID + suffix)
fn read_index_header(data: &[u8], pos: &mut usize) -> Result<(String, i32)> {
    let (codec_name, version) = read_codec_header(data, pos)?;
    // ObjectID: 16 bytes
    *pos += 16;
    // Suffix
    let suffix_len = read_u8(data, pos) as usize;
    *pos += suffix_len;
    Ok((codec_name, version))
}

// ---------------------------------------------------------------------------
// segments_N — list segment names
// ---------------------------------------------------------------------------

/// Find and parse the segments_N file in the index directory.
/// Returns segment names (e.g., ["_0", "_1", "_2q"]).
fn read_segments(index_dir: &Path) -> Result<Vec<String>> {
    // Find segments_N file (highest N)
    let mut best: Option<(u64, std::path::PathBuf)> = None;
    for entry in std::fs::read_dir(index_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("segments_") && name != "segments.gen" {
            let gen_str = &name["segments_".len()..];
            // Generation is base-36 encoded
            if let Ok(gen) = u64::from_str_radix(gen_str, 36) {
                if best.is_none() || gen > best.as_ref().unwrap().0 {
                    best = Some((gen, entry.path()));
                }
            }
        }
    }

    let seg_path = match best {
        Some((_, p)) => p,
        None => bail!("No segments_N file found in {}", index_dir.display()),
    };

    let data = std::fs::read(&seg_path)?;
    let mut pos = 0;

    let (_codec, version) = read_index_header(&data, &mut pos)?;

    // Note: the "commit ID" is the ObjectID already read as part of IndexHeader.
    // There is no separate CommitID field after the header.

    // Lucene 5.3+: LuceneVersion (3x VInt)
    if version >= 6 {
        read_vint(&data, &mut pos); // major
        read_vint(&data, &mut pos); // minor
        read_vint(&data, &mut pos); // bugfix
    }

    let _gen = read_i64(&data, &mut pos); // version/generation
    let _name_counter = read_i32(&data, &mut pos);
    let seg_count = read_i32(&data, &mut pos) as usize;

    // MinSegmentLuceneVersion (Lucene 5.3+, only if segCount > 0)
    if version >= 6 && seg_count > 0 {
        read_vint(&data, &mut pos); // major
        read_vint(&data, &mut pos); // minor
        read_vint(&data, &mut pos); // bugfix
    }

    let mut segments = Vec::with_capacity(seg_count);
    for _ in 0..seg_count {
        let seg_name = read_string(&data, &mut pos);
        let has_id = read_u8(&data, &mut pos);
        if has_id == 1 {
            pos += 16; // segmentID
        }
        let _codec = read_string(&data, &mut pos);
        let _del_gen = read_i64(&data, &mut pos);
        let _del_count = read_i32(&data, &mut pos);
        let _fi_gen = read_i64(&data, &mut pos);
        let _dv_gen = read_i64(&data, &mut pos);

        // FieldInfosFiles: Set<String> — uses VInt for count
        let fi_file_count = read_vint(&data, &mut pos) as usize;
        for _ in 0..fi_file_count {
            skip_string(&data, &mut pos);
        }

        // DocValuesUpdatesFiles: Int32 count, per entry: Int32 + Set<String>
        let dvu_count = read_i32(&data, &mut pos) as usize;
        for _ in 0..dvu_count {
            let _field_num = read_i32(&data, &mut pos);
            // Each update file set uses VInt for count
            let set_count = read_vint(&data, &mut pos) as usize;
            for _ in 0..set_count {
                skip_string(&data, &mut pos);
            }
        }

        segments.push(seg_name);
    }

    Ok(segments)
}

// ---------------------------------------------------------------------------
// Compound file (.cfe/.cfs) reader
// ---------------------------------------------------------------------------

struct CompoundFile {
    entries: HashMap<String, (usize, usize)>, // name → (offset, length) in data
    data: Vec<u8>,
}

impl CompoundFile {
    fn open(cfe_path: &Path, cfs_path: &Path) -> Result<Self> {
        let cfe_data = std::fs::read(cfe_path)?;
        let cfs_data = std::fs::read(cfs_path)?;

        let mut pos = 0;
        let _ = read_index_header(&cfe_data, &mut pos)?;

        let file_count = read_vint(&cfe_data, &mut pos) as usize;
        let mut entries = HashMap::new();

        for _ in 0..file_count {
            let name = read_string(&cfe_data, &mut pos);
            let offset = read_i64(&cfe_data, &mut pos) as usize;
            let length = read_i64(&cfe_data, &mut pos) as usize;
            entries.insert(name, (offset, length));
        }

        Ok(Self {
            entries,
            data: cfs_data,
        })
    }

    fn get_file(&self, name: &str) -> Option<&[u8]> {
        self.entries.get(name).map(|&(offset, length)| {
            &self.data[offset..offset + length]
        })
    }
}

// ---------------------------------------------------------------------------
// .fnm — field names
// ---------------------------------------------------------------------------

fn read_field_infos(data: &[u8]) -> Result<HashMap<String, u32>> {
    let mut pos = 0;
    let (codec_name, _version) = read_index_header(data, &mut pos)?;

    // Lucene60FieldInfos adds PointDimensionCount per field
    let is_lucene60 = codec_name.contains("60");

    let field_count = read_vint(data, &mut pos) as usize;
    let mut fields = HashMap::new();

    for _ in 0..field_count {
        let name = read_string(data, &mut pos);
        let number = read_vint(data, &mut pos);
        let _bits = read_u8(data, &mut pos);       // field bits
        let _index_options = read_u8(data, &mut pos);
        let _dv_type = read_u8(data, &mut pos);    // DocValuesType (Lucene60) or DocValuesBits (Lucene50)
        let _dv_gen = read_i64(data, &mut pos);
        // Attributes: Map<String, String>
        let attr_count = read_vint(data, &mut pos) as usize;
        for _ in 0..attr_count {
            skip_string(data, &mut pos); // key
            skip_string(data, &mut pos); // value
        }
        // Lucene 6.0+: point dimensions
        if is_lucene60 {
            let point_dims = read_vint(data, &mut pos);
            if point_dims != 0 {
                let _point_num_bytes = read_vint(data, &mut pos);
            }
        }
        fields.insert(name, number);
    }

    Ok(fields)
}

// ---------------------------------------------------------------------------
// PackedInts — PACKED format reader
// ---------------------------------------------------------------------------

fn read_packed_ints(data: &[u8], pos: &mut usize, count: usize, bits_per_value: usize) -> Vec<u64> {
    if bits_per_value == 0 {
        return vec![0; count];
    }

    // Read exactly ceil(totalBits/8) bytes (byte-aligned, not long-aligned).
    // Lucene's PackedInts writer pads to byte boundary, not long boundary.
    let total_bits = count * bits_per_value;
    let total_bytes = (total_bits + 7) / 8;

    // Convert bytes to big-endian u64 blocks for bit extraction
    let num_longs = (total_bytes + 7) / 8;
    let mut blocks = vec![0u64; num_longs];
    for i in 0..total_bytes {
        let long_idx = i / 8;
        let byte_idx = i % 8;
        blocks[long_idx] |= (data[*pos + i] as u64) << (56 - byte_idx * 8);
    }
    *pos += total_bytes;

    let mask: u64 = if bits_per_value == 64 {
        u64::MAX
    } else {
        (1u64 << bits_per_value) - 1
    };

    let mut values = Vec::with_capacity(count);
    for i in 0..count {
        let bit_pos = i * bits_per_value;
        let long_idx = bit_pos / 64;
        let end_bits = (bit_pos % 64) + bits_per_value;

        let value = if end_bits <= 64 {
            (blocks[long_idx] >> (64 - end_bits)) & mask
        } else {
            let overflow = end_bits - 64;
            ((blocks[long_idx] << overflow) | (blocks[long_idx + 1] >> (64 - overflow))) & mask
        };
        values.push(value);
    }

    values
}

// ---------------------------------------------------------------------------
// LZ4 decompression (standard block format, as used by Lucene)
// ---------------------------------------------------------------------------

fn lz4_decompress(input: &[u8], pos: &mut usize, decompressed_len: usize) -> Result<Vec<u8>> {
    let mut output = vec![0u8; decompressed_len];
    let mut out_pos = 0;

    while out_pos < decompressed_len {
        if *pos >= input.len() {
            bail!("LZ4: input exhausted at out_pos={}/{}", out_pos, decompressed_len);
        }
        let token = input[*pos];
        *pos += 1;

        // Literal length
        let mut lit_len = (token >> 4) as usize;
        if lit_len == 15 {
            loop {
                let b = input[*pos];
                *pos += 1;
                lit_len += b as usize;
                if b != 255 {
                    break;
                }
            }
        }

        // Copy literals
        if lit_len > 0 {
            if *pos + lit_len > input.len() || out_pos + lit_len > decompressed_len {
                bail!("LZ4: literal overflow lit_len={} pos={} out_pos={}", lit_len, *pos, out_pos);
            }
            output[out_pos..out_pos + lit_len].copy_from_slice(&input[*pos..*pos + lit_len]);
            *pos += lit_len;
            out_pos += lit_len;
        }

        if out_pos >= decompressed_len {
            break;
        }

        // Match offset (2 bytes, little-endian)
        let match_offset = input[*pos] as usize | ((input[*pos + 1] as usize) << 8);
        *pos += 2;

        if match_offset == 0 || match_offset > out_pos {
            bail!("LZ4: bad match_offset={} at out_pos={}", match_offset, out_pos);
        }

        // Match length
        let mut match_len = (token & 0x0F) as usize;
        if match_len == 15 {
            loop {
                let b = input[*pos];
                *pos += 1;
                match_len += b as usize;
                if b != 255 {
                    break;
                }
            }
        }
        match_len += 4; // MIN_MATCH

        // Copy match (potentially overlapping — must be byte-by-byte)
        let match_start = out_pos - match_offset;
        for i in 0..match_len {
            output[out_pos + i] = output[match_start + i];
        }
        out_pos += match_len;
    }

    Ok(output)
}

// ---------------------------------------------------------------------------
// .fdt — stored fields data (Lucene50CompressingStoredFieldsFormat)
// ---------------------------------------------------------------------------

/// Extract all `_source` JSON documents from a .fdt stored fields file.
fn read_stored_fields(fdt_data: &[u8], source_field_num: u32) -> Result<Vec<Vec<u8>>> {
    let mut pos = 0;
    let (codec_name, version) = read_index_header(fdt_data, &mut pos)?;

    let is_lz4 = codec_name.contains("Fast");
    let _is_deflate = codec_name.contains("High");

    let _chunk_size = read_vint(fdt_data, &mut pos);
    let _packed_ints_version = read_vint(fdt_data, &mut pos);

    let mut documents: Vec<Vec<u8>> = Vec::new();

    // Read chunks until we hit the footer (last 16 bytes) or stats
    let footer_start = fdt_data.len() - 16; // CodecFooter is 16 bytes
    // If version >= 1 (VERSION_CHUNK_STATS), there's also NumChunks + NumDirtyChunks before footer
    let data_end = if version >= 1 {
        // NumChunks and NumDirtyChunks are VLongs before the footer
        // We need to scan backwards to find them; easier to just stop when pos gets close
        footer_start.saturating_sub(18) // max 9 bytes each for VLong
    } else {
        footer_start
    };

    while pos < data_end {
        // Try to read a chunk; if we can't, we've hit the stats/footer
        if pos + 4 >= fdt_data.len() {
            break;
        }

        let _doc_base = read_vint(fdt_data, &mut pos);
        let token = read_vint(fdt_data, &mut pos);
        let chunk_docs = (token >> 1) as usize;
        let sliced = (token & 1) != 0;

        if chunk_docs == 0 || chunk_docs > 1_000_000 {
            // Likely hit the stats section
            break;
        }

        let is_first = documents.is_empty();

        // NumStoredFields per document
        let num_stored_fields: Vec<u64> = if chunk_docs == 1 {
            vec![read_vint(fdt_data, &mut pos) as u64]
        } else {
            let bits = read_vint(fdt_data, &mut pos) as usize;
            if bits == 0 {
                let common = read_vint(fdt_data, &mut pos) as u64;
                vec![common; chunk_docs]
            } else {
                read_packed_ints(fdt_data, &mut pos, chunk_docs, bits)
            }
        };

        // DocLengths (byte length of each doc's stored data)
        let doc_lengths: Vec<u64> = if chunk_docs == 1 {
            vec![read_vint(fdt_data, &mut pos) as u64]
        } else {
            let bits = read_vint(fdt_data, &mut pos) as usize;
            if bits == 0 {
                let common = read_vint(fdt_data, &mut pos) as u64;
                vec![common; chunk_docs]
            } else {
                read_packed_ints(fdt_data, &mut pos, chunk_docs, bits)
            }
        };

        let total_len: usize = doc_lengths.iter().sum::<u64>() as usize;

        if total_len == 0 {
            continue;
        }

        // Sanity check — a single chunk should never exceed ~100MB
        if total_len > 100_000_000 {
            eprintln!("  Suspiciously large chunk ({}MB, {} docs) — stopping segment", total_len / 1_000_000, chunk_docs);
            break;
        }

        if documents.is_empty() {
            // Debug first chunk
            eprintln!(
                "  First chunk: docs={}, sliced={}, total_len={}, pos={}/{}",
                chunk_docs, sliced, total_len, pos, fdt_data.len()
            );
            eprintln!("    first 5 lengths: {:?}", &doc_lengths[..doc_lengths.len().min(5)]);
            eprintln!("    LZ4 starts with: {:02x?}", &fdt_data[pos..pos+20.min(fdt_data.len()-pos)]);
        }

        // Decompress
        let decompressed = if is_lz4 {
            let result = if sliced {
                // Sliced: multiple LZ4 blocks, each up to chunkSize bytes
                let mut all = Vec::with_capacity(total_len);
                let mut remaining = total_len;
                let mut ok = true;
                while remaining > 0 {
                    let block_len = remaining.min(_chunk_size as usize);
                    match lz4_decompress(fdt_data, &mut pos, block_len) {
                        Ok(block) => {
                            all.extend_from_slice(&block);
                            remaining -= block_len;
                        }
                        Err(e) => {
                            eprintln!("  LZ4 block error (skipping chunk): {}", e);
                            ok = false;
                            break;
                        }
                    }
                }
                if ok { Ok(all) } else { continue }
            } else {
                lz4_decompress(fdt_data, &mut pos, total_len)
            };
            match result {
                Ok(data) => data,
                Err(e) => {
                    eprintln!("  LZ4 error in segment (stopping, got {} docs so far): {}", documents.len(), e);
                    break; // Can't recover position — stop this segment
                }
            }
        } else {
            // DEFLATE: VInt(compressed_length) + deflate bytes
            let compressed_len = read_vint(fdt_data, &mut pos) as usize;
            let compressed = &fdt_data[pos..pos + compressed_len];
            pos += compressed_len;
            let mut decoder = flate2::read::DeflateDecoder::new(compressed);
            let mut decompressed = vec![0u8; total_len];
            std::io::Read::read_exact(&mut decoder, &mut decompressed)?;
            decompressed
        };

        // Parse individual documents from the decompressed buffer
        let mut doc_offset = 0;
        for doc_idx in 0..chunk_docs {
            let doc_len = doc_lengths[doc_idx] as usize;
            let n_fields = num_stored_fields[doc_idx] as usize;
            let doc_end = doc_offset + doc_len;

            let mut dpos = doc_offset;
            for _ in 0..n_fields {
                if dpos >= doc_end {
                    break;
                }
                let field_and_type = read_vlong(&decompressed, &mut dpos);
                let field_type = (field_and_type & 0x07) as u8;
                let field_num = (field_and_type >> 3) as u32;

                match field_type {
                    0 => {
                        // String
                        let len = read_vint(&decompressed, &mut dpos) as usize;
                        if field_num == source_field_num {
                            documents.push(decompressed[dpos..dpos + len].to_vec());
                        }
                        dpos += len;
                    }
                    1 => {
                        // BinaryValue (this is where _source lives)
                        let len = read_vint(&decompressed, &mut dpos) as usize;
                        if field_num == source_field_num {
                            documents.push(decompressed[dpos..dpos + len].to_vec());
                        }
                        dpos += len;
                    }
                    2 => {
                        // Int32
                        if field_num == source_field_num {
                            documents.push(decompressed[dpos..dpos + 4].to_vec());
                        }
                        dpos += 4;
                    }
                    3 => {
                        // Float32
                        dpos += 4;
                    }
                    4 => {
                        // Int64
                        dpos += 8;
                    }
                    5 => {
                        // Float64
                        dpos += 8;
                    }
                    _ => {
                        // Unknown type — skip to end of document
                        dpos = doc_end;
                    }
                }
            }

            doc_offset = doc_end;
        }
    }

    Ok(documents)
}

// ---------------------------------------------------------------------------
// Public API: read all _source documents from an ES/Lucene index directory
// ---------------------------------------------------------------------------

/// Read all `_source` JSON documents from a Lucene/Elasticsearch index.
/// The `index_dir` should contain segments_N, .fdt, .fnm, .cfs/.cfe files.
///
/// Returns raw JSON bytes for each document.
pub fn read_all_sources(index_dir: &Path) -> Result<Vec<Vec<u8>>> {
    let segments = read_segments(index_dir)
        .context("Failed to read segments file")?;

    info!("Found {} segments in {}", segments.len(), index_dir.display());

    let mut all_docs: Vec<Vec<u8>> = Vec::new();

    for seg_name in &segments {
        // Check if this is a compound segment
        let cfs_path = index_dir.join(format!("{}.cfs", seg_name));
        let cfe_path = index_dir.join(format!("{}.cfe", seg_name));
        let fnm_path = index_dir.join(format!("{}.fnm", seg_name));
        let fdt_path = index_dir.join(format!("{}.fdt", seg_name));

        let (fnm_data, fdt_data) = if cfs_path.exists() && cfe_path.exists() {
            // Compound file — extract .fnm and .fdt from within
            let cf = CompoundFile::open(&cfe_path, &cfs_path)
                .with_context(|| format!("Reading compound file for segment {}", seg_name))?;

            // Compound entries may use short names (".fnm") or full names ("_52.fnm")
            let fnm = cf
                .get_file(".fnm")
                .or_else(|| cf.get_file(&format!("{}.fnm", seg_name)))
                .with_context(|| format!("No .fnm in compound file for {}", seg_name))?
                .to_vec();
            let fdt = cf
                .get_file(".fdt")
                .or_else(|| cf.get_file(&format!("{}.fdt", seg_name)))
                .with_context(|| format!("No .fdt in compound file for {}", seg_name))?
                .to_vec();

            (fnm, fdt)
        } else if fnm_path.exists() && fdt_path.exists() {
            // Non-compound — read files directly
            let fnm = std::fs::read(&fnm_path)?;
            let fdt = std::fs::read(&fdt_path)?;
            (fnm, fdt)
        } else {
            // Skip segments without stored fields
            continue;
        };

        let fields = read_field_infos(&fnm_data)
            .with_context(|| format!("Reading field infos for segment {}", seg_name))?;

        let source_field = match fields.get("_source") {
            Some(&n) => n,
            None => {
                info!("  Segment {} has no _source field, skipping", seg_name);
                continue;
            }
        };

        let docs = read_stored_fields(&fdt_data, source_field)
            .with_context(|| format!("Reading stored fields for segment {}", seg_name))?;

        info!("  Segment {}: {} documents", seg_name, docs.len());
        all_docs.extend(docs);
    }

    info!("Total: {} documents from {} segments", all_docs.len(), segments.len());
    Ok(all_docs)
}

/// Convenience: read all _source documents and parse as JSON.
pub fn read_all_json(index_dir: &Path) -> Result<Vec<serde_json::Value>> {
    let sources = read_all_sources(index_dir)?;

    let mut docs = Vec::with_capacity(sources.len());
    let mut parse_errors = 0;

    for raw in &sources {
        match serde_json::from_slice(raw) {
            Ok(v) => docs.push(v),
            Err(_) => parse_errors += 1,
        }
    }

    if parse_errors > 0 {
        info!("{} documents failed JSON parsing", parse_errors);
    }

    Ok(docs)
}

/// Streaming version: read Lucene segments one at a time, parse and convert
/// each document immediately. Memory holds only one segment's raw bytes at a
/// time plus the growing output vectors — NOT all raw bytes simultaneously.
pub fn read_and_parse_streaming(
    index_dir: &Path,
    parse_fn: impl Fn(&serde_json::Value) -> (Option<heimdall_core::types::RawPlace>, Option<crate::extract::RawAddress>),
) -> Result<(Vec<heimdall_core::types::RawPlace>, Vec<crate::extract::RawAddress>)> {
    let segments = read_segments(index_dir)?;
    info!("Streaming {} segments from {}", segments.len(), index_dir.display());

    let mut places: Vec<heimdall_core::types::RawPlace> = Vec::new();
    let mut addresses: Vec<crate::extract::RawAddress> = Vec::new();
    let mut parse_errors = 0usize;
    let mut total_docs = 0usize;

    for seg_name in &segments {
        let cfs_path = index_dir.join(format!("{}.cfs", seg_name));
        let cfe_path = index_dir.join(format!("{}.cfe", seg_name));
        let fnm_path = index_dir.join(format!("{}.fnm", seg_name));
        let fdt_path = index_dir.join(format!("{}.fdt", seg_name));

        let (fnm_data, fdt_data) = if cfs_path.exists() && cfe_path.exists() {
            let cf = CompoundFile::open(&cfe_path, &cfs_path)?;
            let fnm = cf.get_file(".fnm")
                .or_else(|| cf.get_file(&format!("{}.fnm", seg_name)))
                .with_context(|| format!("No .fnm for {}", seg_name))?.to_vec();
            let fdt = cf.get_file(".fdt")
                .or_else(|| cf.get_file(&format!("{}.fdt", seg_name)))
                .with_context(|| format!("No .fdt for {}", seg_name))?.to_vec();
            (fnm, fdt)
        } else if fnm_path.exists() && fdt_path.exists() {
            (std::fs::read(&fnm_path)?, std::fs::read(&fdt_path)?)
        } else {
            continue;
        };

        let fields = read_field_infos(&fnm_data)?;
        let source_field = match fields.get("_source") {
            Some(&n) => n,
            None => continue,
        };

        // Read raw docs for this segment, then immediately parse+convert and drop raw bytes
        let seg_docs = read_stored_fields(&fdt_data, source_field)?;
        drop(fdt_data); // free the raw .fdt bytes
        drop(fnm_data);

        let seg_count = seg_docs.len();
        for raw in seg_docs {
            match serde_json::from_slice::<serde_json::Value>(&raw) {
                Ok(doc) => {
                    let (place, addr) = parse_fn(&doc);
                    if let Some(p) = place { places.push(p); }
                    if let Some(a) = addr { addresses.push(a); }
                }
                Err(_) => parse_errors += 1,
            }
        }
        total_docs += seg_count;
        info!("  Segment {}: {} docs (running: {} places, {} addr)", seg_name, seg_count, places.len(), addresses.len());
    }

    if parse_errors > 0 {
        info!("{} documents failed JSON parsing", parse_errors);
    }
    info!("Total: {} docs → {} places, {} addresses", total_docs, places.len(), addresses.len());

    Ok((places, addresses))
}
