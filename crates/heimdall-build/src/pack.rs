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

    // Load admin entries to build a (admin_id → population) lookup. Used
    // as a centrality signal in compute_importance_inline — a Locality
    // inside Stockholms kommun (970K pop) outranks the same-name Locality
    // in a small kommun.
    let admin_bin_path = output_dir.join("admin.bin");
    let admin_population: HashMap<u16, u32> = if admin_bin_path.exists() {
        // admin.bin is written by enrich.rs as postcard, then zstd-
        // compressed in place. Match the runtime reader in index.rs:
        // decompress first, then postcard with bincode fallback.
        let bytes = heimdall_core::compressed_io::read_maybe_compressed(&admin_bin_path)
            .unwrap_or_default();
        let entries: Vec<heimdall_core::types::AdminEntry> = postcard::from_bytes(&bytes)
            .or_else(|_| bincode::deserialize::<Vec<heimdall_core::types::AdminEntry>>(&bytes))
            .unwrap_or_default();
        entries.into_iter()
            .filter(|e| e.population > 0)
            .map(|e| (e.id, e.population))
            .collect()
    } else {
        HashMap::new()
    };
    if !admin_population.is_empty() {
        info!("Loaded admin population for {} admin entries (centrality signal)",
            admin_population.len());
    }

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
    // Trigram TSV: same `key\trecord_id\timportance\tpop_flag` format as
    // exact/phonetic so build_fst_from_disk reads it unmodified. Each
    // indexed name expands to ~name_len trigrams, so the file is ~10×
    // bigger than exact.tsv pre-sort.
    let mut ngram_writer = std::io::BufWriter::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::create(key_dir.join("ngram.tsv"))?,
    );
    let mut exact_count = 0usize;
    let mut phonetic_count = 0usize;
    let mut ngram_count = 0usize;

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

                // name_intl translation count is a strong notability proxy.
                // A place with 8 name:* tags (Stockholm Stortorget) is more
                // famous than one with 0 (Växjö Stortorget). Counts as
                // tiebreaker between same-type, same-wikidata places.
                let intl_translation_count = if name_intl_arr.is_null(i) {
                    0
                } else {
                    name_intl_arr.value(i)
                        .split(';')
                        .filter(|s| !s.trim().is_empty())
                        .count()
                };

                let (admin1_id, admin2_id) = admin_map.get(&osm_id).copied().unwrap_or((0, 0));
                // Centrality: how populous is this place's parent admin?
                // Use the larger of admin1 (län) and admin2 (kommun) — kommun
                // is finer-grained and more relevant. A Locality in
                // Stockholms kommun (970K) outranks one in a 5K rural kommun.
                let parent_population = std::cmp::max(
                    admin_population.get(&admin1_id).copied().unwrap_or(0),
                    admin_population.get(&admin2_id).copied().unwrap_or(0),
                );
                let importance = compute_importance_inline(
                    place_type, population, has_wikidata, intl_translation_count,
                    parent_population,
                );

                let mut flags: u8 = 0;
                if population.is_some() { flags |= 0x01; }
                if !alt_names_arr.is_null(i) && !alt_names_arr.value(i).is_empty() { flags |= 0x02; }
                if !old_names_arr.is_null(i) && !old_names_arr.value(i).is_empty() { flags |= 0x04; }
                if is_relation { flags |= 0x08; }
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

                // Per-word indexing: multi-word names also get an FST key
                // for each individual word, with a *demoted* importance so
                // exact full-string matches still beat per-word matches.
                // Combined with the multi-record FST sidecar, "domkyrkan"
                // becomes a key whose posting list contains Uppsala
                // domkyrka, Lunds domkyrka, Linköpings domkyrka — the
                // city-context filter then picks the right one.
                //
                // Skip stopwords (kommun, stad, län, …) and very short
                // tokens (≤ 2 chars) — those would create noisy posting
                // lists where the right record drowns in 1000s of
                // unrelated hits.
                index_per_word_keys(
                    &mut exact_writer, &mut exact_count, &primary_lower,
                    id, importance, pop_flag, normalizer.stopwords(),
                )?;

                // Split compound bilingual names (e.g. "Casteddu/Cagliari", "Bolzano - Bozen")
                for sep in [" / ", " - ", "/"] {
                    if primary_lower.contains(sep) {
                        for part in primary_lower.split(sep) {
                            let part = part.trim();
                            if !part.is_empty() && part != primary_lower {
                                write!(exact_writer, "{}\t{}\t{}\t{}\n", part, id, importance, pop_flag)?;
                                exact_count += 1;
                                // Also write normalized (diacritics-stripped) variants of each split part
                                // so that e.g. "san sebastián" also generates "san sebastian"
                                for norm_part in normalizer.normalize(part) {
                                    if !norm_part.is_empty() && norm_part != part {
                                        write!(exact_writer, "{}\t{}\t{}\t{}\n", norm_part, id, importance, pop_flag)?;
                                        exact_count += 1;
                                    }
                                }
                            }
                        }
                    }
                }

                for alt in &all_alts {
                    let key = alt.to_lowercase();
                    if !key.is_empty() {
                        write!(exact_writer, "{}\t{}\t{}\t{}\n", key, id, importance, pop_flag)?;
                        exact_count += 1;
                        // Per-word entries for the alt too, so individual
                        // words from name:* / old_name / official_name
                        // also resolve.
                        index_per_word_keys(
                            &mut exact_writer, &mut exact_count, &key,
                            id, importance, pop_flag, normalizer.stopwords(),
                        )?;
                        // Also write a stop-word-stripped variant. "ABBA
                        // The Museum" → "abba museum" so the canonical
                        // English query for the place lands on the
                        // record. Cheap — a handful of common articles
                        // in English/Swedish/German.
                        let no_stops = strip_stopwords(&key);
                        if no_stops != key && !no_stops.is_empty() {
                            write!(exact_writer, "{}\t{}\t{}\t{}\n", no_stops, id, importance, pop_flag)?;
                            exact_count += 1;
                        }
                        // Run the alt name through the normalizer too so
                        // diacritic-stripped, abbreviation-expanded variants
                        // also land on the record.
                        for candidate in normalizer.normalize(alt) {
                            let cand_lower = candidate.to_lowercase();
                            if !cand_lower.is_empty() && cand_lower != key {
                                write!(exact_writer, "{}\t{}\t{}\t{}\n", cand_lower, id, importance, pop_flag)?;
                                exact_count += 1;
                            }
                        }
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

                // Trigrams from the lowercased primary name and from each
                // alt/intl name. Demote alt-name trigrams slightly so the
                // primary name's trigrams dominate ranking — but not so
                // much that an English-only name like "Ericsson Globe"
                // can't be reached via "Globen" (its Swedish alt).
                //
                // We deliberately skip the per-word and stop-word stripped
                // variants: trigrams already implicitly handle partial
                // tokens. Adding extra variants blows up the FST without
                // meaningful recall gain.
                let trigrams_emit = |writer: &mut std::io::BufWriter<std::fs::File>,
                                     counter: &mut usize,
                                     text: &str,
                                     imp: u16|
                 -> std::io::Result<()> {
                    if text.is_empty() || text.len() > 80 { return Ok(()); }
                    for tg in heimdall_core::ngram::trigrams(text) {
                        // Skip the boundary-only trigram for very common
                        // 1-char tokens — `^i$`, `^a$` would otherwise
                        // attract every short particle.
                        if tg.len() == 3 && tg.starts_with('^') && tg.ends_with('$') {
                            continue;
                        }
                        write!(writer, "{}\t{}\t{}\t{}\n", tg, id, imp, pop_flag)?;
                        *counter += 1;
                    }
                    Ok(())
                };

                trigrams_emit(&mut ngram_writer, &mut ngram_count,
                              &primary_lower, importance)?;

                // Diacritic-stripped + abbreviation-expanded variants of
                // the primary name. Same demotion as the alt path — these
                // are derived forms.
                for candidate in &candidates {
                    if !candidate.is_empty() && candidate != &primary_lower {
                        trigrams_emit(&mut ngram_writer, &mut ngram_count,
                                      candidate, importance.saturating_sub(50))?;
                    }
                }

                // Alt and intl names — slightly demoted so the primary
                // name takes precedence on ties.
                for alt in &all_alts {
                    let alt_lower = alt.to_lowercase();
                    trigrams_emit(&mut ngram_writer, &mut ngram_count,
                                  &alt_lower, importance.saturating_sub(50))?;
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
    ngram_writer.flush()?;
    drop(exact_writer);
    drop(phonetic_writer);
    drop(ngram_writer);
    info!("FST keys written: {} exact, {} phonetic, {} ngram",
        exact_count, phonetic_count, ngram_count);

    // Build 3 FSTs in parallel
    let exact_tsv = key_dir.join("exact.tsv");
    let phonetic_tsv = key_dir.join("phonetic.tsv");
    let ngram_tsv = key_dir.join("ngram.tsv");
    let fst_exact_path = output_dir.join("fst_exact.fst");
    let fst_phonetic_path = output_dir.join("fst_phonetic.fst");
    let fst_ngram_path = output_dir.join("fst_ngram.fst");
    // Sidecar posting-list files. Hold up to N=8 record_ids per key
    // (sorted by importance desc) so same-name alternates can survive
    // FST collision resolution. The FST value becomes the byte offset
    // into the sidecar; if the sidecar is missing, the FST value is
    // treated as a record_id directly (backwards compatibility).
    let record_lists_exact_path = output_dir.join("record_lists.bin");
    let record_lists_phonetic_path = output_dir.join("record_lists_phonetic.bin");
    let record_lists_ngram_path = output_dir.join("record_lists_ngram.bin");

    let (res_exact, (res_phonetic, res_ngram)) = rayon::join(
        || -> Result<usize> {
            let bytes = build_fst_from_disk(&exact_tsv, &fst_exact_path, Some(&record_lists_exact_path))?;
            heimdall_core::compressed_io::compress_file(&fst_exact_path, 19)?;
            if record_lists_exact_path.exists() {
                heimdall_core::compressed_io::compress_file(&record_lists_exact_path, 19)?;
            }
            Ok(bytes)
        },
        || rayon::join(
            || -> Result<usize> {
                let bytes = build_fst_from_disk(&phonetic_tsv, &fst_phonetic_path, Some(&record_lists_phonetic_path))?;
                heimdall_core::compressed_io::compress_file(&fst_phonetic_path, 19)?;
                if record_lists_phonetic_path.exists() {
                    heimdall_core::compressed_io::compress_file(&record_lists_phonetic_path, 19)?;
                }
                Ok(bytes)
            },
            || -> Result<usize> {
                // Trigram posting lists are *much* longer than exact /
                // phonetic — common letter pairs like `^st` show up in
                // thousands of names. The shared MAX_POSTINGS_PER_KEY=16
                // cap is too tight; use a larger ngram-specific cap so
                // we keep enough candidates per trigram to still find a
                // good intersection while bounding worst-case memory.
                let bytes = build_fst_from_disk_ngram(&ngram_tsv, &fst_ngram_path, &record_lists_ngram_path)?;
                heimdall_core::compressed_io::compress_file(&fst_ngram_path, 19)?;
                if record_lists_ngram_path.exists() {
                    heimdall_core::compressed_io::compress_file(&record_lists_ngram_path, 19)?;
                }
                Ok(bytes)
            },
        ),
    );
    let fst_exact_bytes = res_exact?;
    let fst_phonetic_bytes = res_phonetic?;
    let fst_ngram_bytes = res_ngram?;
    info!("FSTs built in parallel: exact {:.1} MB, phonetic {:.1} MB, ngram {:.1} MB",
        fst_exact_bytes as f64 / 1e6, fst_phonetic_bytes as f64 / 1e6,
        fst_ngram_bytes as f64 / 1e6);

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

/// Maximum number of record_ids stored per posting list. Same-name
/// alternates beyond this are dropped (sorted by importance desc, so the
/// least-important ones go first). 16 leaves room for ~10 full-importance
/// hits on keys that collide between definite-form ("Stadsbiblioteket")
/// and per-word ("X stadsbibliotek") indexing — small enough that the
/// sidecar stays under a few MB.
const MAX_POSTINGS_PER_KEY: usize = 16;

/// Cap on postings per trigram. Common trigrams like `^st` appear in
/// thousands of records — without a cap the worst-case posting list is
/// O(record_count). 4096 keeps the per-trigram cost bounded while still
/// retaining the top several thousand most-important records, which is
/// more than enough headroom for the trigram intersection ranker to find
/// the right needle.
const MAX_NGRAM_POSTINGS_PER_KEY: usize = 4096;

/// Trigram-specific FST builder. Same disk-sort + group-by-key pattern as
/// `build_fst_from_disk` but with a much larger posting cap because
/// common trigrams legitimately appear in thousands of names. Posting
/// list values are u32 record_ids (no importance stored separately —
/// they're already sorted by importance desc when written).
fn build_fst_from_disk_ngram(tsv_path: &Path, fst_path: &Path, sidecar_path: &Path) -> Result<usize> {
    use std::io::{BufRead, Write};

    if !tsv_path.exists() || std::fs::metadata(tsv_path)?.len() == 0 {
        let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
        let mut builder = MapBuilder::new(file)?;
        builder.finish()?;
        std::fs::write(sidecar_path, &[][..])?;
        return Ok(std::fs::metadata(fst_path)?.len() as usize);
    }

    let sorted_path = tsv_path.with_extension("sorted.tsv");
    let sort_status = std::process::Command::new("sort")
        .env("LC_ALL", "C")
        .args(["-t", "\t", "-k1,1", "-s", "--buffer-size=256M"])
        .arg(tsv_path)
        .stdout(std::fs::File::create(&sorted_path)?)
        .status()?;
    if !sort_status.success() {
        anyhow::bail!("sort command failed for {}", tsv_path.display());
    }

    let reader = std::io::BufReader::new(std::fs::File::open(&sorted_path)?);
    let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
    let mut builder = MapBuilder::new(file)?;

    let mut sidecar_writer = std::io::BufWriter::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::create(sidecar_path)?,
    );
    let mut sidecar_offset: u64 = 0;

    let mut prev_key = String::new();
    let mut group: Vec<(u32, u16)> = Vec::new();

    let flush_group = |builder: &mut MapBuilder<std::io::BufWriter<std::fs::File>>,
                       sidecar_writer: &mut std::io::BufWriter<std::fs::File>,
                       sidecar_offset: &mut u64,
                       key: &str,
                       group: &mut Vec<(u32, u16)>|
     -> Result<()> {
        if key.is_empty() || group.is_empty() {
            return Ok(());
        }
        // Same dedup pattern as the exact builder: by record_id keeping
        // highest-importance entry, then re-sort by importance desc.
        group.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        group.dedup_by_key(|(id, _)| *id);
        group.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        if group.len() > MAX_NGRAM_POSTINGS_PER_KEY {
            group.truncate(MAX_NGRAM_POSTINGS_PER_KEY);
        }

        let count = group.len() as u16;
        let offset = *sidecar_offset;
        sidecar_writer.write_all(&count.to_le_bytes())?;
        for &(id, _) in group.iter() {
            sidecar_writer.write_all(&id.to_le_bytes())?;
        }
        *sidecar_offset += 2 + (group.len() as u64) * 4;
        builder.insert(key.as_bytes(), offset)?;

        group.clear();
        Ok(())
    };

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 4 { continue; }

        let key = parts[0];
        let id: u32 = parts[1].parse().unwrap_or(0);
        let importance: u16 = parts[2].parse().unwrap_or(0);

        if key != prev_key {
            flush_group(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;
            prev_key = key.to_owned();
        }
        group.push((id, importance));
    }
    flush_group(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;

    builder.finish()?;
    sidecar_writer.flush()?;

    std::fs::remove_file(&sorted_path).ok();
    std::fs::remove_file(tsv_path).ok();

    Ok(std::fs::metadata(fst_path)?.len() as usize)
}

/// Build FST from a disk TSV file: external sort → group by key → write top-N postings
/// to sidecar → stream offsets into FST.
///
/// TSV format: `key\trecord_id\timportance\tis_populated`
///
/// When `sidecar_path` is `Some`, each FST value is the byte offset of a posting list
/// in the sidecar. Each posting list begins with a `u16` count followed by
/// `count × u32` record_ids, sorted by importance descending and capped at
/// `MAX_POSTINGS_PER_KEY`. When `sidecar_path` is `None`, the legacy single-id
/// format is written (FST value = record_id) — used for the empty-ngram path.
///
/// Memory: ~sort_chunk_size (auto-tuned) + FST builder streaming buffer.
fn build_fst_from_disk(tsv_path: &Path, fst_path: &Path, sidecar_path: Option<&Path>) -> Result<usize> {
    use std::io::{BufRead, Write};

    if !tsv_path.exists() || std::fs::metadata(tsv_path)?.len() == 0 {
        // Empty — write empty FST and (optionally) empty sidecar
        let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
        let mut builder = MapBuilder::new(file)?;
        builder.finish()?;
        if let Some(p) = sidecar_path {
            std::fs::write(p, &[][..])?;
        }
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

    // Stream sorted file → group by key → write postings → FST
    let reader = std::io::BufReader::new(std::fs::File::open(&sorted_path)?);
    let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
    let mut builder = MapBuilder::new(file)?;

    // Sidecar writer (optional). When `None` we emit single-id values
    // (legacy path, used only for the empty ngram FST).
    let mut sidecar_writer: Option<std::io::BufWriter<std::fs::File>> = match sidecar_path {
        Some(p) => Some(std::io::BufWriter::with_capacity(
            4 * 1024 * 1024,
            std::fs::File::create(p)?,
        )),
        None => None,
    };
    let mut sidecar_offset: u64 = 0;

    let mut prev_key = String::new();
    // Buffered postings for the current key: (record_id, importance).
    // Sorted by importance desc on group close, deduped by record_id
    // (highest importance wins), then truncated to MAX_POSTINGS_PER_KEY.
    let mut group: Vec<(u32, u16)> = Vec::new();

    let flush_group = |builder: &mut MapBuilder<std::io::BufWriter<std::fs::File>>,
                       sidecar_writer: &mut Option<std::io::BufWriter<std::fs::File>>,
                       sidecar_offset: &mut u64,
                       key: &str,
                       group: &mut Vec<(u32, u16)>|
     -> Result<()> {
        if key.is_empty() || group.is_empty() {
            return Ok(());
        }
        // Dedup by record_id — same record may appear under multiple
        // importance values if it was inserted via multiple variants
        // (primary name + per-word + alt-name). `Vec::dedup_by_key` only
        // removes *consecutive* duplicates, so we must sort by id first,
        // then collapse keeping the highest importance per id, then
        // re-sort by importance desc for the final posting order.
        group.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        group.dedup_by_key(|(id, _)| *id);
        group.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        if group.len() > MAX_POSTINGS_PER_KEY {
            group.truncate(MAX_POSTINGS_PER_KEY);
        }

        match sidecar_writer.as_mut() {
            Some(w) => {
                let count = group.len() as u16;
                let offset = *sidecar_offset;
                w.write_all(&count.to_le_bytes())?;
                for &(id, _) in group.iter() {
                    w.write_all(&id.to_le_bytes())?;
                }
                *sidecar_offset += 2 + (group.len() as u64) * 4;
                builder.insert(key.as_bytes(), offset)?;
            }
            None => {
                // Legacy single-id path. Pick the first (highest-importance) entry.
                builder.insert(key.as_bytes(), group[0].0 as u64)?;
            }
        }

        group.clear();
        Ok(())
    };

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 4 { continue; }

        let key = parts[0];
        let id: u32 = parts[1].parse().unwrap_or(0);
        let importance: u16 = parts[2].parse().unwrap_or(0);

        if key != prev_key {
            flush_group(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;
            prev_key = key.to_owned();
        }
        group.push((id, importance));
    }
    // Flush the last group
    flush_group(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;

    builder.finish()?;
    if let Some(mut w) = sidecar_writer {
        w.flush()?;
    }

    // Clean up
    std::fs::remove_file(&sorted_path).ok();
    std::fs::remove_file(tsv_path).ok();

    Ok(std::fs::metadata(fst_path)?.len() as usize)
}

/// Compute importance from individual fields (avoids needing full RawPlace).
///
/// Scoring is designed to create large gaps between place types so that
/// cross-country name collisions resolve correctly (e.g. "Pamplona" Spain
/// 200K population should always outrank a Mexican village of the same name).
///
/// Scale: 0-65535 (u16).
/// - Population component: log10(pop) * 4000 (max ~29K for 10M+ cities)
/// - Place type base: City=10000, Town=6000, Village=2000, POI=500
/// - Wikidata bonus: +8000 (notable enough to have a Wikipedia article).
///   Lifts famous-but-tiny places (Gamla stan, Skansen, Drottningholm) above
///   anonymous suburbs/villages of similar size.
/// Per-word indexing for multi-word place names. Writes one TSV line
/// per content-bearing word so the FST has a key for each individual
/// word that appears in the name. Combined with the multi-record FST
/// sidecar this lets `domkyrkan uppsala` find Uppsala domkyrka, and
/// `operan` find Kungliga Operan.
///
/// Demoting via `importance / 4` keeps full-string exact matches above
/// per-word matches so a query that exactly matches a record's primary
/// name still wins on ranking.
///
/// Skipped:
/// - Single-word names (already indexed as the full name).
/// - Stopwords from the language config (kommun, län, stad, sverige …).
/// - Tokens of 2 chars or shorter (i, av, …) — too dense to be useful.
/// - The compound-bilingual sep tokens already handled by the slash
///   loop above are NOT excluded here; per-word indexing is additive.
fn index_per_word_keys<W: std::io::Write>(
    writer: &mut W,
    counter: &mut usize,
    primary_lower: &str,
    record_id: u32,
    importance: u16,
    pop_flag: u8,
    stopwords: &[String],
) -> std::io::Result<()> {
    let words: Vec<&str> = primary_lower.split_whitespace().collect();
    if words.len() < 2 { return Ok(()); }
    // Heavy demotion. Per-word entries must never outrank a full-name
    // exact match — otherwise a query like "Bergen" returns "Vita
    // bergen" (per-word demoted ~2000) instead of any record named
    // *exactly* "Bergen" (full importance ~300). With /128, the demoted
    // score sits below the lowest typical exact-record floor (~300 for
    // Locality without wd) for all but the most-important records, so
    // exact hits dominate the top-8 cap. Tie-breaks within per-word
    // entries (for keys with no exact hits, e.g. "domkyrka" alone)
    // still respect relative importance via the residual division.
    let demoted = ((importance as u32 / 128).max(1)) as u16;
    for word in &words {
        let w = word.trim_matches(|c: char| !c.is_alphanumeric());
        if w.len() <= 2 { continue; }
        if stopwords.iter().any(|sw| sw == w) { continue; }
        write!(writer, "{}\t{}\t{}\t{}\n", w, record_id, demoted, pop_flag)?;
        *counter += 1;
    }
    Ok(())
}

/// Strip common articles/conjunctions so "ABBA The Museum" → "abba museum"
/// and "Universitetet i Stockholm" → "universitetet stockholm".
/// Operates on lowercased input. Conservative list — only words that
/// almost never carry meaning in a place name.
fn strip_stopwords(s: &str) -> String {
    const STOPS: &[&str] = &[
        // English
        "the", "of", "and",
        // Swedish
        "och", "i",
        // German
        "der", "die", "das", "und",
    ];
    s.split_whitespace()
        .filter(|w| !STOPS.contains(w))
        .collect::<Vec<&str>>()
        .join(" ")
}

fn compute_importance_inline(
    place_type: PlaceType,
    population: Option<u32>,
    has_wikidata: bool,
    intl_translations: usize,
    parent_admin_population: u32,
) -> u16 {
    let mut score: u32 = 0;
    // Population bonus: only counts for major settlements (City and
    // Town). Village/Hamlet/Farm population in OSM is wildly unreliable
    // — a 1492-person village called "Slottsskogen" should not outrank
    // a wikidata-tagged park of the same name (the famous Göteborg
    // park). 100-person floor to suppress tiny outliers.
    //
    // Suburb/Quarter/Neighbourhood are subdivisions of cities — their
    // `population` tag is OSM noise.
    let population_eligible = matches!(
        place_type,
        PlaceType::City | PlaceType::Town
    );
    if population_eligible {
        if let Some(pop) = population {
            if pop > 100 {
                score += ((pop as f64).log10() * 4000.0) as u32;
            }
        }
    }
    score += match place_type {
        PlaceType::City => 10000,
        PlaceType::Town => 6000,
        PlaceType::Village => 2000,
        PlaceType::Suburb | PlaceType::Quarter => 1500,
        PlaceType::Neighbourhood => 1400,
        PlaceType::Hamlet | PlaceType::Farm => 500,
        PlaceType::Island => 3000,
        PlaceType::Airport => 1500,
        // Major transit hubs (subway, mainline rail) — bumped from 700 to
        // 1500 so a wikidata-tagged station beats a same-name Hamlet.
        PlaceType::Station => 1500,
        PlaceType::Square => 1500,
        // Famous named streets (Avenyn, Drottninggatan). Set close to
        // Square so they reliably beat random restaurants of the same
        // colloquial name.
        PlaceType::Street => 1500,
        // Notable POIs — bumped to 2500 so a wikidata-tagged landmark
        // (Liseberg theme park = 2500+8000=10500) reliably beats a
        // same-name suburb (Suburb = 1500+8000=9500). Still well below
        // Town/Village so a small village never loses to a same-name POI.
        PlaceType::Landmark => 2500,
        PlaceType::University | PlaceType::Hospital | PlaceType::PublicBuilding => 1000,
        // Park base — same level as Landmark. Beats Village (2000)
        // by a hair (with wd bonus, Park 10500 vs Village 10000) so
        // Slottsskogen-the-park outranks Slottsskogen-the-village,
        // but doesn't dwarf a real Suburb of the same name when the
        // suburb is famous enough to have a `name:*` translation
        // (Stockholm Djurgården).
        PlaceType::Park => 2500,
        PlaceType::Lake | PlaceType::River => 1000,
        PlaceType::Mountain | PlaceType::Forest => 700,
        PlaceType::County => 4000,
        PlaceType::State => 5000,
        PlaceType::Country => 8000,
        _ => 300,
    };
    // Wikidata = notable. A place with a Wikidata entry is at least
    // "notable enough to have a Wikipedia article." Boost it ahead of
    // similar-typed places without a Wikidata tag.
    if has_wikidata {
        score += 8000;
    }
    // Each name:* translation is editorial attention from a foreign-language
    // mapper — a strong proxy for how known the place is internationally.
    // +1500 per translation, capped at 5 (so ≤ +7500 total) — enough to
    // lift famous suburbs (Stockholm Djurgården has name:ru) over an
    // obscure same-name nature reserve, but small enough that a place
    // with 5 translations doesn't suddenly outrank a tier-up category.
    score += (intl_translations as u32).min(5) * 1500;
    // Centrality: places inside a populous parent admin (e.g. a hotel in
    // Stockholms kommun, pop ~970K) get a small bonus over same-name
    // places in tiny rural kommuner. Threshold at 10K population so
    // village-scale parent admins contribute nothing — only real city
    // and metropolitan kommuner activate it.
    //
    //   pop=10K  → 0
    //   pop=50K  → ~1000
    //   pop=200K → ~2000
    //   pop=970K → ~3000 (Stockholms kommun)
    if parent_admin_population > 10_000 {
        let lp = (parent_admin_population as f64).log10();
        score += ((lp - 4.0).max(0.0) * 1500.0) as u32;
    }
    score.min(65535) as u16
}

/// Build an FST from (key, (record_id, importance, is_populated)) tuples.
/// Sorts input, deduplicates, writes to path.
fn build_fst(pairs: &mut Vec<(String, (u32, u16, bool))>, path: &Path) -> Result<usize> {
    // FST requires lexicographically sorted, unique keys
    pairs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    pairs.dedup_by(|a, b| {
        if a.0 == b.0 {
            // Keep the higher-importance record. Importance already
            // factors in population, place type, and wikidata, so we
            // don't need an additional populated-place tiebreak (which
            // used to demote famous landmarks under no-population
            // suburbs of the same name).
            if a.1 .1 > b.1 .1 {
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
        15 => PlaceType::Square,
        16 => PlaceType::Street,
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
