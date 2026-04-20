/// pack.rs — build FSTs and record store from extracted Parquet data
///
/// FST requirements:
///   - Keys must be inserted in lexicographic order
///   - Keys are byte strings (UTF-8)
///   - Values are u64 (we use record_id: u32, cast to u64)
///
/// Strategy:
///   1. Load all places from Parquet
///   2. Generate all normalized name variants (via heimdall-normalize)
///   3. Collect (normalized_key, record_id) pairs
///   4. Sort by key
///   5. Deduplicate — for collisions, keep highest importance record
///   6. Feed sorted pairs into FstMapBuilder

use std::io::Write;
use std::path::Path;
use std::collections::HashMap;
use anyhow::Result;
use fst::MapBuilder;
use tracing::info;

use heimdall_core::types::*;
use heimdall_core::record_store::RecordStoreBuilder;
use heimdall_core::reverse::GeohashIndexBuilder;
use heimdall_normalize::Normalizer;
use crate::enrich::EnrichResult;

pub struct PackStats {
    pub record_count: usize,
    pub fst_exact_bytes: usize,
    pub fst_phonetic_bytes: usize,
    pub fst_ngram_bytes: usize,
    pub record_store_bytes: usize,
}

pub fn pack(
    parquet_path: &Path,
    output_dir: &Path,
    _enriched: &EnrichResult,
) -> Result<PackStats> {
    info!("Streaming places from {}", parquet_path.display());

    // Load admin map (osm_id → (admin1_id, admin2_id)) from enrich step
    let admin_map_path = output_dir.join("admin_map.bin");
    let admin_map: HashMap<i64, (u16, u16)> = if admin_map_path.exists() {
        let bytes = std::fs::read(&admin_map_path)?;
        let map: HashMap<i64, (u16, u16)> = bincode::deserialize(&bytes)?;
        info!("Loaded admin map: {} entries", map.len());
        map
    } else {
        info!("No admin_map.bin found, admin IDs will be 0");
        HashMap::new()
    };

    // Load normalizer config — try sv.toml in output dir first, then detect country
    let mut geohash_builder = GeohashIndexBuilder::new();

    let normalizer = {
        let local = output_dir.join("sv.toml");
        if local.exists() {
            info!("Loading normalizer config from {}", local.display());
            Normalizer::from_config(&local)
        } else {
            // Detect country from directory name
            let dir_name = output_dir.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            let country_toml = if dir_name.contains("germany") || dir_name.contains("-de") {
                Some(std::path::PathBuf::from("data/normalizers/de.toml"))
            } else if dir_name.contains("denmark") || dir_name.contains("-dk") {
                Some(std::path::PathBuf::from("data/normalizers/da.toml"))
            } else if dir_name.contains("finland") || dir_name.contains("-fi") {
                Some(std::path::PathBuf::from("data/normalizers/fi.toml"))
            } else if dir_name.contains("norway") || dir_name.contains("-no") {
                Some(std::path::PathBuf::from("data/normalizers/no.toml"))
            } else {
                None
            };

            match country_toml {
                Some(path) if path.exists() => {
                    info!("Loading normalizer config from {}", path.display());
                    Normalizer::from_config(&path)
                }
                _ => {
                    // Fallback to Swedish defaults
                    let sv = std::path::PathBuf::from("data/normalizers/sv.toml");
                    if sv.exists() {
                        info!("Loading normalizer config from {}", sv.display());
                        Normalizer::from_config(&sv)
                    } else {
                        Normalizer::swedish()
                    }
                }
            }
        }
    };
    let mut record_builder = RecordStoreBuilder::new();

    // key: normalized_name, value: (record_id, importance)
    // For collisions, keep the more important record
    // (record_id, importance, is_populated_place)
    // Disk-backed FST key collection — write to temp files instead of HashMap.
    // Sort externally and build FST from the sorted stream.
    let key_dir = output_dir.join(".fst_keys_tmp");
    std::fs::create_dir_all(&key_dir)?;
    let mut exact_writer = std::io::BufWriter::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::create(key_dir.join("exact.tsv"))?,
    );
    let mut phonetic_writer = std::io::BufWriter::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::create(key_dir.join("phonetic.tsv"))?,
    );
    let mut exact_count = 0usize;
    let mut phonetic_count = 0usize;

    let mut records_added = 0usize;
    let mut skipped_empty = 0usize;
    let mut skipped_unknown = 0usize;

    // Stream parquet batch-by-batch — never holds all RawPlace in memory.
    // Only the key HashMaps + RecordStoreBuilder grow with data.
    {
        use arrow::array::*;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = std::fs::File::open(parquet_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch_result in reader {
            let batch = batch_result?;
            let n = batch.num_rows();

            let osm_ids = batch.column_by_name("osm_id").unwrap()
                .as_any().downcast_ref::<Int64Array>().unwrap();
            let names = batch.column_by_name("name").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();
            let lats = batch.column_by_name("lat").unwrap()
                .as_any().downcast_ref::<Float64Array>().unwrap();
            let lons = batch.column_by_name("lon").unwrap()
                .as_any().downcast_ref::<Float64Array>().unwrap();
            let place_types = batch.column_by_name("place_type").unwrap()
                .as_any().downcast_ref::<UInt8Array>().unwrap();
            let populations = batch.column_by_name("population").unwrap()
                .as_any().downcast_ref::<UInt32Array>().unwrap();
            let wikidatas = batch.column_by_name("wikidata").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();
            let alt_names_arr = batch.column_by_name("alt_names").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();
            let old_names_arr = batch.column_by_name("old_names").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();
            let name_intl_arr = batch.column_by_name("name_intl").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();
            let osm_types = batch.column_by_name("osm_type").and_then(|c|
                c.as_any().downcast_ref::<UInt8Array>().map(|a| a.clone()));

            for i in 0..n {
                let name = names.value(i);
                if name.is_empty() {
                    skipped_empty += 1;
                    continue;
                }
                if name.len() >= 255 { continue; }

                let place_type = place_type_from_u8(place_types.value(i));
                let has_wikidata = !wikidatas.is_null(i) && !wikidatas.value(i).is_empty();
                if place_type == PlaceType::Unknown && !has_wikidata {
                    skipped_unknown += 1;
                    continue;
                }

                let osm_id = osm_ids.value(i);
                let lat = lats.value(i);
                let lon = lons.value(i);
                let population = if populations.is_null(i) { None } else { Some(populations.value(i)) };
                let is_relation = osm_types.as_ref().map_or(false, |t| t.value(i) == 2);

                let importance = compute_importance_inline(place_type, population);

                let mut flags: u8 = 0;
                if population.is_some() { flags |= 0x01; }
                if !alt_names_arr.is_null(i) && !alt_names_arr.value(i).is_empty() { flags |= 0x02; }
                if !old_names_arr.is_null(i) && !old_names_arr.value(i).is_empty() { flags |= 0x04; }
                if is_relation { flags |= 0x08; }

                let (admin1_id, admin2_id) = admin_map.get(&osm_id).copied().unwrap_or((0, 0));
                let coord = Coord::new(lat, lon);

                let record = PlaceRecord {
                    coord,
                    admin1_id,
                    admin2_id,
                    importance,
                    place_type,
                    flags,
                    name_offset: 0,
                    osm_id: osm_id as u32,
                };

                // Parse alt/old/intl names from semicolon-delimited parquet strings
                let alt_strs: Vec<&str> = if alt_names_arr.is_null(i) { vec![] } else {
                    alt_names_arr.value(i).split(';').filter(|s| !s.is_empty() && s.len() < 255).collect()
                };
                let old_strs: Vec<&str> = if old_names_arr.is_null(i) { vec![] } else {
                    old_names_arr.value(i).split(';').filter(|s| !s.is_empty() && s.len() < 255).collect()
                };
                let intl_names: Vec<String> = if name_intl_arr.is_null(i) { vec![] } else {
                    name_intl_arr.value(i).split(';')
                        .filter(|s| !s.is_empty())
                        .filter_map(|s| s.split_once('=').map(|(_, name)| name.to_string()))
                        .filter(|s| !s.is_empty() && s.len() < 255)
                        .collect()
                };

                let mut all_alts: Vec<&str> = Vec::new();
                all_alts.extend(&alt_strs);
                all_alts.extend(&old_strs);
                all_alts.extend(intl_names.iter().map(|s| s.as_str()));

                let id = record_builder.add(record, name, &all_alts);
                records_added += 1;

                geohash_builder.add(lat, lon, id);

                let is_populated = matches!(
                    place_type,
                    PlaceType::City | PlaceType::Town | PlaceType::Village
                        | PlaceType::Suburb | PlaceType::Hamlet
                );
                let collision_score = (is_populated, importance);

                // Write FST keys to disk — collision resolution happens after sort
                let pop_flag: u8 = if is_populated { 1 } else { 0 };

                let primary_lower = name.to_lowercase();
                write!(exact_writer, "{}\t{}\t{}\t{}\n", primary_lower, id, importance, pop_flag)?;
                exact_count += 1;

                // Split compound bilingual names (e.g. "Casteddu/Cagliari", "Bolzano - Bozen")
                for sep in [" / ", " - ", "/"] {
                    if primary_lower.contains(sep) {
                        for part in primary_lower.split(sep) {
                            let part = part.trim();
                            if !part.is_empty() && part != primary_lower {
                                write!(exact_writer, "{}\t{}\t{}\t{}\n", part, id, importance, pop_flag)?;
                                exact_count += 1;
                            }
                        }
                    }
                }

                for alt in &all_alts {
                    let key = alt.to_lowercase();
                    if !key.is_empty() {
                        write!(exact_writer, "{}\t{}\t{}\t{}\n", key, id, importance, pop_flag)?;
                        exact_count += 1;
                    }
                }

                let candidates = normalizer.normalize(name);
                for candidate in &candidates {
                    if !candidate.is_empty() {
                        write!(exact_writer, "{}\t{}\t{}\t{}\n", candidate, id, importance, pop_flag)?;
                        exact_count += 1;
                    }
                }

                let phonetic_key = normalizer.phonetic_key(name);
                if !phonetic_key.is_empty() {
                    write!(phonetic_writer, "{}\t{}\t{}\t{}\n", phonetic_key, id, importance, pop_flag)?;
                    phonetic_count += 1;
                }
            }
        }
    } // parquet reader dropped, Arrow batch buffers freed

    // Write record store
    let record_store_path = output_dir.join("records.bin");
    record_builder.write(&record_store_path)?;
    let record_store_bytes = std::fs::metadata(&record_store_path)?.len() as usize;
    info!("Record store: {:.1} MB", record_store_bytes as f64 / 1e6);

    // Write geohash spatial index (for reverse geocoding)
    let geohash_path = output_dir.join("geohash_index.bin");
    let geohash_bytes = geohash_builder.write(&geohash_path)?;
    info!("Geohash index: {:.1} MB ({} entries)", geohash_bytes as f64 / 1e6, records_added);
    // Compress geohash (delta-varint v2 format compresses well with zstd)
    let (geo_orig, geo_comp) = heimdall_core::compressed_io::compress_file(&geohash_path, 19)?;
    if geo_comp < geo_orig {
        info!("Geohash compressed: {:.1} KB → {:.1} KB", geo_orig as f64 / 1024.0, geo_comp as f64 / 1024.0);
    }

    // Flush key writers
    exact_writer.flush()?;
    phonetic_writer.flush()?;
    drop(exact_writer);
    drop(phonetic_writer);
    info!("FST keys written: {} exact, {} phonetic", exact_count, phonetic_count);

    // Build 3 FSTs in parallel
    let exact_tsv = key_dir.join("exact.tsv");
    let phonetic_tsv = key_dir.join("phonetic.tsv");
    let fst_exact_path = output_dir.join("fst_exact.fst");
    let fst_phonetic_path = output_dir.join("fst_phonetic.fst");
    let fst_ngram_path = output_dir.join("fst_ngram.fst");

    let (res_exact, (res_phonetic, res_ngram)) = rayon::join(
        || -> Result<usize> {
            let bytes = build_fst_from_disk(&exact_tsv, &fst_exact_path)?;
            heimdall_core::compressed_io::compress_file(&fst_exact_path, 19)?;
            Ok(bytes)
        },
        || rayon::join(
            || -> Result<usize> {
                let bytes = build_fst_from_disk(&phonetic_tsv, &fst_phonetic_path)?;
                heimdall_core::compressed_io::compress_file(&fst_phonetic_path, 19)?;
                Ok(bytes)
            },
            || -> Result<usize> {
                let bytes = build_fst(&mut vec![], &fst_ngram_path)?;
                heimdall_core::compressed_io::compress_file(&fst_ngram_path, 19)?;
                Ok(bytes)
            },
        ),
    );
    let fst_exact_bytes = res_exact?;
    let fst_phonetic_bytes = res_phonetic?;
    let fst_ngram_bytes = res_ngram?;
    info!("FSTs built in parallel: exact {:.1} MB, phonetic {:.1} MB",
        fst_exact_bytes as f64 / 1e6, fst_phonetic_bytes as f64 / 1e6);

    // Clean up temp key files
    std::fs::remove_dir_all(&key_dir).ok();

    info!(
        "Packed {} records (skipped {} empty-name, {} unknown-type from {} total)",
        records_added, skipped_empty, skipped_unknown, records_added + skipped_empty + skipped_unknown
    );
    let record_count = records_added;

    Ok(PackStats {
        record_count,
        fst_exact_bytes,
        fst_phonetic_bytes,
        fst_ngram_bytes,
        record_store_bytes,
    })
}

/// Build FST from a disk TSV file: external sort → dedup (keep best) → stream into FST.
/// TSV format: key\trecord_id\timportance\tis_populated
/// Memory: ~sort_chunk_size (auto-tuned) + FST builder streaming buffer.
fn build_fst_from_disk(tsv_path: &Path, fst_path: &Path) -> Result<usize> {
    use std::io::{BufRead, Write};

    if !tsv_path.exists() || std::fs::metadata(tsv_path)?.len() == 0 {
        // Empty — write empty FST
        let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
        let mut builder = MapBuilder::new(file)?;
        builder.finish()?;
        return Ok(std::fs::metadata(fst_path)?.len() as usize);
    }

    // External sort the TSV by key (first column)
    // Use the system `sort` command — it handles external sorting with bounded memory
    let sorted_path = tsv_path.with_extension("sorted.tsv");
    let sort_status = std::process::Command::new("sort")
        .env("LC_ALL", "C")
        .args(["-t", "\t", "-k1,1", "-s", "--buffer-size=128M"])
        .arg(tsv_path)
        .stdout(std::fs::File::create(&sorted_path)?)
        .status()?;
    if !sort_status.success() {
        anyhow::bail!("sort command failed for {}", tsv_path.display());
    }

    // Stream sorted file → dedup → FST
    let reader = std::io::BufReader::new(std::fs::File::open(&sorted_path)?);
    let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
    let mut builder = MapBuilder::new(file)?;

    let mut prev_key = String::new();
    let mut best_id: u32 = 0;
    let mut best_importance: u16 = 0;
    let mut best_is_pop: bool = false;

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 4 { continue; }

        let key = parts[0];
        let id: u32 = parts[1].parse().unwrap_or(0);
        let importance: u16 = parts[2].parse().unwrap_or(0);
        let is_pop = parts[3] == "1";

        if key == prev_key {
            // Collision: keep higher (is_populated, importance)
            if (is_pop, importance) > (best_is_pop, best_importance) {
                best_id = id;
                best_importance = importance;
                best_is_pop = is_pop;
            }
        } else {
            // Emit previous key
            if !prev_key.is_empty() {
                builder.insert(prev_key.as_bytes(), best_id as u64)?;
            }
            prev_key = key.to_owned();
            best_id = id;
            best_importance = importance;
            best_is_pop = is_pop;
        }
    }
    // Emit last key
    if !prev_key.is_empty() {
        builder.insert(prev_key.as_bytes(), best_id as u64)?;
    }

    builder.finish()?;

    // Clean up
    std::fs::remove_file(&sorted_path).ok();
    std::fs::remove_file(tsv_path).ok();

    Ok(std::fs::metadata(fst_path)?.len() as usize)
}

/// Compute importance from individual fields (avoids needing full RawPlace).
fn compute_importance_inline(place_type: PlaceType, population: Option<u32>) -> u16 {
    let mut score: u32 = 0;
    if let Some(pop) = population {
        if pop > 0 {
            score += ((pop as f64).log10() * 3000.0) as u32;
        }
    }
    score += match place_type {
        PlaceType::City => 2000,
        PlaceType::Town => 1500,
        PlaceType::Village => 1000,
        PlaceType::Suburb | PlaceType::Quarter => 900,
        PlaceType::Hamlet | PlaceType::Farm => 500,
        PlaceType::Island => 800,
        PlaceType::Airport | PlaceType::Station => 700,
        PlaceType::Lake | PlaceType::River => 600,
        PlaceType::Mountain | PlaceType::Forest => 500,
        PlaceType::County => 300,
        PlaceType::State => 200,
        PlaceType::Country => 100,
        _ => 200,
    };
    score.min(65535) as u16
}

/// Build an FST from (key, (record_id, importance, is_populated)) tuples.
/// Sorts input, deduplicates, writes to path.
fn build_fst(pairs: &mut Vec<(String, (u32, u16, bool))>, path: &Path) -> Result<usize> {
    // FST requires lexicographically sorted, unique keys
    pairs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    pairs.dedup_by(|a, b| {
        if a.0 == b.0 {
            // Keep: prefer populated place, then higher importance
            let a_score = (a.1 .2, a.1 .1);
            let b_score = (b.1 .2, b.1 .1);
            if a_score > b_score {
                b.1 = a.1;
            }
            true
        } else {
            false
        }
    });

    let file = std::io::BufWriter::new(std::fs::File::create(path)?);
    let mut builder = MapBuilder::new(file)?;

    for (key, (record_id, _importance, _is_pop)) in pairs.iter() {
        builder.insert(key.as_bytes(), *record_id as u64)?;
    }

    builder.finish()?;

    let bytes = std::fs::metadata(path)?.len() as usize;
    Ok(bytes)
}

/// Read places from the Parquet file written by extract.rs
fn read_parquet(path: &Path) -> Result<Vec<RawPlace>> {
    use arrow::array::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut places = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let n = batch.num_rows();

        let osm_ids = batch
            .column_by_name("osm_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lats = batch
            .column_by_name("lat")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let lons = batch
            .column_by_name("lon")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let place_types = batch
            .column_by_name("place_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let admin_levels = batch
            .column_by_name("admin_level")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let populations = batch
            .column_by_name("population")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let wikidatas = batch
            .column_by_name("wikidata")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let alt_names_arr = batch
            .column_by_name("alt_names")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let old_names_arr = batch
            .column_by_name("old_names")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name_intl_arr = batch
            .column_by_name("name_intl")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..n {
            let place_type_u8 = place_types.value(i);
            let place_type = place_type_from_u8(place_type_u8);

            let alt_names: Vec<String> = if alt_names_arr.is_null(i) {
                vec![]
            } else {
                alt_names_arr
                    .value(i)
                    .split(';')
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect()
            };

            let old_names: Vec<String> = if old_names_arr.is_null(i) {
                vec![]
            } else {
                old_names_arr
                    .value(i)
                    .split(';')
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect()
            };

            let name_intl: Vec<(String, String)> = if name_intl_arr.is_null(i) {
                vec![]
            } else {
                name_intl_arr
                    .value(i)
                    .split(';')
                    .filter_map(|entry| {
                        let mut parts = entry.splitn(2, '=');
                        let lang = parts.next()?.trim().to_owned();
                        let name = parts.next()?.trim().to_owned();
                        if lang.is_empty() || name.is_empty() {
                            None
                        } else {
                            Some((lang, name))
                        }
                    })
                    .collect()
            };

            places.push(RawPlace {
                osm_id: osm_ids.value(i),
                osm_type: OsmType::Node, // all stored as nodes for now
                name: names.value(i).to_owned(),
                name_intl,
                alt_names,
                old_names,
                coord: Coord::new(lats.value(i), lons.value(i)),
                place_type,
                admin_level: if admin_levels.is_null(i) {
                    None
                } else {
                    Some(admin_levels.value(i))
                },
                country_code: None, // TODO
                admin1: None,
                admin2: None,
                population: if populations.is_null(i) {
                    None
                } else {
                    Some(populations.value(i))
                },
                wikidata: if wikidatas.is_null(i) {
                    None
                } else {
                    Some(wikidatas.value(i).to_owned())
                },
            });
        }
    }

    Ok(places)
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
        20 => PlaceType::Lake,
        21 => PlaceType::River,
        22 => PlaceType::Mountain,
        23 => PlaceType::Forest,
        24 => PlaceType::Bay,
        25 => PlaceType::Cape,
        30 => PlaceType::Airport,
        31 => PlaceType::Station,
        _ => PlaceType::Unknown,
    }
}
