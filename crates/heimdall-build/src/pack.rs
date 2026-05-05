/// pack.rs — build FSTs and record store from extracted Parquet data
///
/// FST requirements:
///   - Keys must be inserted in lexicographic order
///   - Keys are byte strings (UTF-8)
///   - Values are u64 (we use record_id: u32, cast to u64)
///
/// Strategy:
///   1. Stream places from Parquet
///   2. Generate all normalized name variants (via heimdall-normalize)
///   3. Push (normalized_key, (record_id, importance)) pairs into a
///      `SortBuffer` (Phase 5 follow-up: replaces the prior TSV-on-disk +
///      GNU `sort` shell-out)
///   4. `SortBuffer::finish()` merges/sorts; we stream the sorted pairs
///   5. Group by key — for collisions, keep the highest-importance record
///      and write a sidecar posting list (top-N by importance desc)
///   6. Feed sorted (key, posting_offset) pairs into MapBuilder
///
/// Byte-identity contract: the FST output is the same bytes that the old
/// TSV+GNU-sort pipeline emitted. Stable sort by raw key bytes preserves
/// push order between equal keys; the per-key dedup/truncation logic is
/// unchanged. Pinned by the
/// `pack_fst_from_typed_buffer_matches_legacy_tsv_path` unit test.

use std::path::Path;
use std::collections::HashMap;
use anyhow::Result;
use fst::MapBuilder;
use tracing::info;

use heimdall_core::types::*;
use heimdall_core::record_store::RecordStoreBuilder;
use heimdall_core::reverse::GeohashIndexBuilder;
use heimdall_core::class_type::ClassTypeBuilder;
use heimdall_core::sidecar_kv::KvSidecarBuilder;
use heimdall_core::wikidata_index::{WikidataIndexBuilder, normalise_qid};
use heimdall_normalize::Normalizer;
use crate::enrich::EnrichResult;
use crate::sort_buffer::{SortBuffer, PackOptions};

/// Best-effort `(class, type)` defaults synthesised from `PlaceType` for
/// records whose source (Photon snapshots, govt importers, synthetic
/// admin nodes, …) didn't carry the original OSM tag pair. Mirrors
/// Nominatim's two-axis vocabulary: `place=*` for populated/admin
/// hierarchies, `boundary=administrative` for admin relations,
/// `natural=*` for water/forest features, etc.
fn default_class_for_place_type(pt: PlaceType) -> String {
    match pt {
        PlaceType::Country | PlaceType::State | PlaceType::County => "boundary".to_owned(),
        PlaceType::Lake | PlaceType::Mountain | PlaceType::Forest
            | PlaceType::Bay | PlaceType::Cape | PlaceType::Island | PlaceType::Islet => "natural".to_owned(),
        PlaceType::River => "waterway".to_owned(),
        PlaceType::Airport => "aeroway".to_owned(),
        PlaceType::Station => "railway".to_owned(),
        PlaceType::Square => "place".to_owned(),
        PlaceType::Street => "highway".to_owned(),
        PlaceType::Landmark => "tourism".to_owned(),
        PlaceType::University | PlaceType::Hospital | PlaceType::PublicBuilding => "amenity".to_owned(),
        PlaceType::Park => "leisure".to_owned(),
        _ => "place".to_owned(),
    }
}

fn default_type_for_place_type(pt: PlaceType) -> String {
    match pt {
        PlaceType::Country => "administrative".to_owned(),
        PlaceType::State => "administrative".to_owned(),
        PlaceType::County => "administrative".to_owned(),
        PlaceType::City => "city".to_owned(),
        PlaceType::Town => "town".to_owned(),
        PlaceType::Village => "village".to_owned(),
        PlaceType::Hamlet => "hamlet".to_owned(),
        PlaceType::Farm => "farm".to_owned(),
        PlaceType::Locality => "locality".to_owned(),
        PlaceType::Suburb => "suburb".to_owned(),
        PlaceType::Quarter => "quarter".to_owned(),
        PlaceType::Neighbourhood => "neighbourhood".to_owned(),
        PlaceType::Island => "island".to_owned(),
        PlaceType::Islet => "islet".to_owned(),
        PlaceType::Square => "square".to_owned(),
        PlaceType::Street => "primary".to_owned(),
        PlaceType::Lake => "water".to_owned(),
        PlaceType::River => "river".to_owned(),
        PlaceType::Mountain => "peak".to_owned(),
        PlaceType::Forest => "wood".to_owned(),
        PlaceType::Bay => "bay".to_owned(),
        PlaceType::Cape => "cape".to_owned(),
        PlaceType::Airport => "aerodrome".to_owned(),
        PlaceType::Station => "station".to_owned(),
        PlaceType::Landmark => "attraction".to_owned(),
        PlaceType::University => "university".to_owned(),
        PlaceType::Hospital => "hospital".to_owned(),
        PlaceType::PublicBuilding => "townhall".to_owned(),
        PlaceType::Park => "park".to_owned(),
        PlaceType::Unknown => "yes".to_owned(),
    }
}

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

    // FST key collection — push (key, (record_id, importance)) pairs into
    // a typed SortBuffer per FST. SortBuffer's in-memory fast path keeps
    // small countries spill-free; for huge countries (US) it spills to
    // scratch with bounded RAM. Replaces the prior TSV-on-disk + GNU `sort`
    // shell-out (Phase 5 follow-up — see TODO_REBUILD_MODES.md).
    //
    // pop_flag is intentionally NOT carried — the legacy TSV stored it as
    // column 4 but the read side parsed only columns 1-3, so it was dead
    // weight. Dropping it shrinks the per-pair budget by 1 byte plus the
    // postcard tag in spill files.
    let pack_opts = PackOptions::default_for(output_dir);
    std::fs::create_dir_all(&pack_opts.scratch_dir)?;
    // Per-FST scratch sub-dirs so concurrent finish() merges don't hit the
    // same nonce filenames. Cheap (just mkdir).
    let exact_scratch = pack_opts.scratch_dir.join("pack_exact");
    let phonetic_scratch = pack_opts.scratch_dir.join("pack_phonetic");
    let ngram_scratch = pack_opts.scratch_dir.join("pack_ngram");
    let mut exact_buf = SortBuffer::<(u32, u16)>::new(pack_opts.sort_mem, &exact_scratch)?;
    let mut phonetic_buf = SortBuffer::<(u32, u16)>::new(pack_opts.sort_mem, &phonetic_scratch)?;
    // Trigrams expand each name to ~name_len entries, so this buffer
    // reaches ~10× the exact one in a real pack. Same mem_limit applies
    // to each buffer independently — the spill machinery in SortBuffer
    // handles the size growth without changing call-site code.
    let mut ngram_buf = SortBuffer::<(u32, u16)>::new(pack_opts.sort_mem, &ngram_scratch)?;
    let mut exact_count = 0usize;
    let mut phonetic_count = 0usize;
    let mut ngram_count = 0usize;

    let mut records_added = 0usize;
    let mut skipped_empty = 0usize;
    let mut skipped_unknown = 0usize;

    let mut class_type_builder = ClassTypeBuilder::new();

    // Phase 2.3 — per-record sidecars for Nominatim `extratags` and
    // `namedetails`. Sparse: only records with non-empty payloads are
    // stored. Both surface in the API behind `?extratags=1` /
    // `?namedetails=1` query flags.
    let mut extratags_builder = KvSidecarBuilder::new();
    let mut namedetails_builder = KvSidecarBuilder::new();

    // Phase 2.8 — Wikidata QID → record_id reverse index. Built from the
    // `wikidata` parquet column; on QID collision the highest-importance
    // record wins (admin relation usually beats admin node). Surfaces
    // through `/search?q=Q12345` short-circuit.
    let mut wikidata_builder = WikidataIndexBuilder::new();

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
            // Phase 2.2 — new columns. Older parquet files (pre-bump,
            // intermediate from `--skip-extract`) may not carry them yet,
            // so each lookup tolerates a missing column by falling back to
            // None across the batch.
            let osm_class_arr = batch.column_by_name("osm_class")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned());
            let osm_class_value_arr = batch.column_by_name("osm_class_value")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned());
            let bbox_south_arr = batch.column_by_name("bbox_south")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>().cloned());
            let bbox_north_arr = batch.column_by_name("bbox_north")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>().cloned());
            let bbox_west_arr = batch.column_by_name("bbox_west")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>().cloned());
            let bbox_east_arr = batch.column_by_name("bbox_east")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>().cloned());
            // Phase 2.3 — `extratags` column is optional (legacy parquet
            // files predate it; non-OSM importers may not populate it).
            // Missing column → empty extratags across the batch.
            let extratags_arr = batch.column_by_name("extratags")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned());

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

                let is_way = osm_types.as_ref().map_or(false, |t| t.value(i) == 1);

                // Resolve (class, value) — preferred from parquet, otherwise
                // synthesise a best-effort default from `place_type` so the
                // API still emits a sensible Nominatim-style class/type pair
                // for non-OSM sources.
                let class_str = osm_class_arr.as_ref()
                    .filter(|a| !a.is_null(i))
                    .map(|a| a.value(i).to_owned());
                let class_value_str = osm_class_value_arr.as_ref()
                    .filter(|a| !a.is_null(i))
                    .map(|a| a.value(i).to_owned());
                let (class_str, class_value_str) = match (class_str, class_value_str) {
                    (Some(c), Some(v)) => (c, v),
                    (Some(c), None) => (c, default_type_for_place_type(place_type)),
                    (None, Some(v)) => (default_class_for_place_type(place_type), v),
                    (None, None) => (
                        default_class_for_place_type(place_type),
                        default_type_for_place_type(place_type),
                    ),
                };
                let class_type = class_type_builder.intern(&class_str, &class_value_str);

                // Resolve bbox: parquet column wins; otherwise synthesise a
                // small ~50m bbox from the coord so every record carries
                // at least a hint of extent for clients that fit-zoom on it.
                let coord = Coord::new(lat, lon);
                let bbox_from_parquet = match (
                    bbox_south_arr.as_ref().filter(|a| !a.is_null(i)).map(|a| a.value(i)),
                    bbox_north_arr.as_ref().filter(|a| !a.is_null(i)).map(|a| a.value(i)),
                    bbox_west_arr .as_ref().filter(|a| !a.is_null(i)).map(|a| a.value(i)),
                    bbox_east_arr .as_ref().filter(|a| !a.is_null(i)).map(|a| a.value(i)),
                ) {
                    (Some(s), Some(n), Some(w), Some(e)) => Some((s, n, w, e)),
                    _ => None,
                };
                let (bbox, bbox_set) = match bbox_from_parquet {
                    Some((s, n, w, e)) => {
                        (BBoxDelta::encode(coord, s, n, w, e), true)
                    }
                    None => {
                        // ~50m around the centroid (≈ 450 microdegrees of
                        // latitude). Saturates safely if encode is asked
                        // for huge values; here it's tiny.
                        const HALF_EXTENT_UDEG: i32 = 450;
                        (
                            BBoxDelta::encode(
                                coord,
                                coord.lat - HALF_EXTENT_UDEG,
                                coord.lat + HALF_EXTENT_UDEG,
                                coord.lon - HALF_EXTENT_UDEG,
                                coord.lon + HALF_EXTENT_UDEG,
                            ),
                            false,
                        )
                    }
                };

                let mut flags: u8 = 0;
                if population.is_some() { flags |= FLAG_HAS_POPULATION; }
                if !alt_names_arr.is_null(i) && !alt_names_arr.value(i).is_empty() { flags |= FLAG_HAS_ALT_NAME; }
                if !old_names_arr.is_null(i) && !old_names_arr.value(i).is_empty() { flags |= FLAG_HAS_OLD_NAME; }
                if is_relation { flags |= FLAG_IS_RELATION; }
                if is_way { flags |= FLAG_IS_WAY; }
                if bbox_set { flags |= FLAG_HAS_BBOX; }

                let record = PlaceRecord {
                    coord,
                    bbox,
                    osm_id: osm_id as u64,
                    admin1_id,
                    admin2_id,
                    importance,
                    class_type,
                    place_type,
                    flags,
                    name_offset: 0,
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

                // Phase 2.3 — sidecar payloads for this record.
                //
                // `namedetails` always carries `name` plus whichever of the
                // optional name flavours were tagged. `name:xx` keys come
                // from `name_intl`; the merged-into-alt-name flavours
                // (`short_name`, `loc_name`, `official_name`, …) are
                // surfaced under the catch-all `alt_name=…;…` key since
                // we don't preserve the original OSM tag in alt_names.
                let mut namedetails: Vec<(String, String)> =
                    Vec::with_capacity(1 + name_intl_arr.len());
                namedetails.push(("name".to_owned(), name.to_owned()));
                if !name_intl_arr.is_null(i) {
                    for pair in name_intl_arr.value(i).split(';') {
                        if let Some((lang, value)) = pair.split_once('=') {
                            if !lang.is_empty() && !value.is_empty() {
                                namedetails.push((format!("name:{}", lang), value.to_owned()));
                            }
                        }
                    }
                }
                if !alt_strs.is_empty() {
                    namedetails.push(("alt_name".to_owned(), alt_strs.join(";")));
                }
                if !old_strs.is_empty() {
                    namedetails.push(("old_name".to_owned(), old_strs.join(";")));
                }
                namedetails_builder.add(id, namedetails);

                // `extratags` from the parquet column (curated allowlist;
                // see `extract::EXTRATAG_KEYS`). Empty for records sourced
                // from non-OSM importers (Photon JSON, govt feeds) until
                // those grow their own equivalents.
                if let Some(ref col) = extratags_arr {
                    if !col.is_null(i) {
                        let pairs: Vec<(String, String)> = col.value(i)
                            .split(';')
                            .filter_map(|kv| {
                                let mut sp = kv.splitn(2, '=');
                                let k = sp.next()?;
                                let v = sp.next()?;
                                if k.is_empty() || v.is_empty() {
                                    None
                                } else {
                                    Some((k.to_owned(), v.to_owned()))
                                }
                            })
                            .collect();
                        extratags_builder.add(id, pairs);
                    }
                }

                // Phase 2.8 — feed the wikidata reverse index. Any value
                // that doesn't normalise to `Q\d+` (multi-QID strings,
                // typos, country codes accidentally tagged as wikidata)
                // is silently dropped by the builder.
                if !wikidatas.is_null(i) {
                    let v = wikidatas.value(i);
                    if let Some(qid) = normalise_qid(v) {
                        wikidata_builder.add(&qid, id, importance);
                    }
                }

                geohash_builder.add(lat, lon, id);

                // Push FST keys into typed SortBuffers. Collision resolution
                // happens at the FST-build site after a stable sort.
                let primary_lower = name.to_lowercase();
                exact_buf.push(primary_lower.as_bytes().to_vec(), (id, importance))?;
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
                    &mut exact_buf, &mut exact_count, &primary_lower,
                    id, importance, normalizer.stopwords(),
                )?;

                // Split compound bilingual names (e.g. "Casteddu/Cagliari", "Bolzano - Bozen")
                //
                // Demoted via /128 — same demotion as per-word indexing.
                // Without this, German disambiguation suffixes like
                // "Frankenberg/Sachsen" inject the city into the "sachsen"
                // posting list at full town importance, drowning out the
                // actual State record. The bilingual-aliases use case
                // ("Bolzano/Bozen") still works because the city's full
                // base importance even after /128 leaves enough head room
                // to outrank random non-bilingual hits.
                let split_demoted = ((importance as u32 / 128).max(1)) as u16;
                for sep in [" / ", " - ", "/"] {
                    if primary_lower.contains(sep) {
                        for part in primary_lower.split(sep) {
                            let part = part.trim();
                            if !part.is_empty() && part != primary_lower {
                                exact_buf.push(part.as_bytes().to_vec(), (id, split_demoted))?;
                                exact_count += 1;
                                // Also write normalized (diacritics-stripped) variants of each split part
                                // so that e.g. "san sebastián" also generates "san sebastian"
                                for norm_part in normalizer.normalize(part) {
                                    if !norm_part.is_empty() && norm_part != part {
                                        exact_buf.push(norm_part.as_bytes().to_vec(), (id, split_demoted))?;
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
                        exact_buf.push(key.as_bytes().to_vec(), (id, importance))?;
                        exact_count += 1;
                        // Per-word entries for the alt too, so individual
                        // words from name:* / old_name / official_name
                        // also resolve.
                        index_per_word_keys(
                            &mut exact_buf, &mut exact_count, &key,
                            id, importance, normalizer.stopwords(),
                        )?;
                        // Also write a stop-word-stripped variant. "ABBA
                        // The Museum" → "abba museum" so the canonical
                        // English query for the place lands on the
                        // record. Cheap — a handful of common articles
                        // in English/Swedish/German.
                        let no_stops = strip_stopwords(&key);
                        if no_stops != key && !no_stops.is_empty() {
                            exact_buf.push(no_stops.into_bytes(), (id, importance))?;
                            exact_count += 1;
                        }
                        // Run the alt name through the normalizer too so
                        // diacritic-stripped, abbreviation-expanded variants
                        // also land on the record.
                        for candidate in normalizer.normalize(alt) {
                            let cand_lower = candidate.to_lowercase();
                            if !cand_lower.is_empty() && cand_lower != key {
                                exact_buf.push(cand_lower.into_bytes(), (id, importance))?;
                                exact_count += 1;
                            }
                        }
                    }
                }

                let candidates = normalizer.normalize(name);
                for candidate in &candidates {
                    if !candidate.is_empty() {
                        exact_buf.push(candidate.as_bytes().to_vec(), (id, importance))?;
                        exact_count += 1;
                    }
                }

                let phonetic_key = normalizer.phonetic_key(name);
                if !phonetic_key.is_empty() {
                    phonetic_buf.push(phonetic_key.into_bytes(), (id, importance))?;
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
                let mut trigrams_emit = |buf: &mut SortBuffer<(u32, u16)>,
                                         counter: &mut usize,
                                         text: &str,
                                         imp: u16|
                 -> Result<()> {
                    if text.is_empty() || text.len() > 80 { return Ok(()); }
                    for tg in heimdall_core::ngram::trigrams(text) {
                        // Skip the boundary-only trigram for very common
                        // 1-char tokens — `^i$`, `^a$` would otherwise
                        // attract every short particle.
                        if tg.len() == 3 && tg.starts_with('^') && tg.ends_with('$') {
                            continue;
                        }
                        buf.push(tg.into_bytes(), (id, imp))?;
                        *counter += 1;
                    }
                    Ok(())
                };

                trigrams_emit(&mut ngram_buf, &mut ngram_count,
                              &primary_lower, importance)?;

                // Diacritic-stripped + abbreviation-expanded variants of
                // the primary name. Same demotion as the alt path — these
                // are derived forms.
                for candidate in &candidates {
                    if !candidate.is_empty() && candidate != &primary_lower {
                        trigrams_emit(&mut ngram_buf, &mut ngram_count,
                                      candidate, importance.saturating_sub(50))?;
                    }
                }

                // Alt and intl names — slightly demoted so the primary
                // name takes precedence on ties.
                for alt in &all_alts {
                    let alt_lower = alt.to_lowercase();
                    trigrams_emit(&mut ngram_buf, &mut ngram_count,
                                  &alt_lower, importance.saturating_sub(50))?;
                }
            }
        }
    } // parquet reader dropped, Arrow batch buffers freed

    // Write class_types interning table sidecar (Phase 2.2). Loaded by the
    // API at startup to resolve `PlaceRecord::class_type` u16 → (class, type)
    // strings for jsonv2 / Nominatim parity.
    let class_types_path = output_dir.join("class_types.bin");
    class_type_builder.write(&class_types_path)
        .map_err(|e| anyhow::anyhow!("write class_types.bin: {}", e))?;
    info!("class_types.bin: {} interned (class, type) pairs", class_type_builder.len() - 1);

    // Phase 2.3 — per-record extratags + namedetails sidecars. Both files
    // are optional from the API side: a missing file just means the
    // corresponding `?extratags=1` / `?namedetails=1` flag yields no
    // payload, so older indices keep working.
    let extratags_path = output_dir.join("extratags.bin");
    let extratags_count = extratags_builder.len();
    extratags_builder.write(&extratags_path)
        .map_err(|e| anyhow::anyhow!("write extratags.bin: {}", e))?;
    let namedetails_path = output_dir.join("namedetails.bin");
    let namedetails_count = namedetails_builder.len();
    namedetails_builder.write(&namedetails_path)
        .map_err(|e| anyhow::anyhow!("write namedetails.bin: {}", e))?;
    info!(
        "Phase 2.3 sidecars: extratags.bin {} records ({:.1} MB), namedetails.bin {} records ({:.1} MB)",
        extratags_count,
        std::fs::metadata(&extratags_path).map(|m| m.len() as f64 / 1e6).unwrap_or(0.0),
        namedetails_count,
        std::fs::metadata(&namedetails_path).map(|m| m.len() as f64 / 1e6).unwrap_or(0.0),
    );

    // Phase 2.8 — Wikidata QID reverse index. Optional file; missing
    // means `/search?q=Qxxxx` returns no hit for this country.
    let wikidata_path = output_dir.join("wikidata_qids.bin");
    let wikidata_count = wikidata_builder.len();
    wikidata_builder.write(&wikidata_path)
        .map_err(|e| anyhow::anyhow!("write wikidata_qids.bin: {}", e))?;
    info!(
        "wikidata_qids.bin: {} QIDs ({:.1} MB)",
        wikidata_count,
        std::fs::metadata(&wikidata_path).map(|m| m.len() as f64 / 1e6).unwrap_or(0.0),
    );

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

    info!("FST keys collected: {} exact, {} phonetic, {} ngram",
        exact_count, phonetic_count, ngram_count);
    info!(
        "SortBuffer state — exact: {} runs / {} bytes spilled; phonetic: {} runs / {} bytes; ngram: {} runs / {} bytes",
        exact_buf.run_count(), exact_buf.spilled_bytes(),
        phonetic_buf.run_count(), phonetic_buf.spilled_bytes(),
        ngram_buf.run_count(), ngram_buf.spilled_bytes(),
    );

    // Build 3 FSTs in parallel from the SortBuffers' merged streams.
    let fst_exact_path = output_dir.join("fst_exact.fst");
    let fst_phonetic_path = output_dir.join("fst_phonetic.fst");
    let fst_ngram_path = output_dir.join("fst_ngram.fst");
    // Sidecar posting-list files. Hold up to N=16 record_ids per key
    // (sorted by importance desc) so same-name alternates can survive
    // FST collision resolution. The FST value becomes the byte offset
    // into the sidecar; if the sidecar is missing, the FST value is
    // treated as a record_id directly (backwards compatibility).
    let record_lists_exact_path = output_dir.join("record_lists.bin");
    let record_lists_phonetic_path = output_dir.join("record_lists_phonetic.bin");
    let record_lists_ngram_path = output_dir.join("record_lists_ngram.bin");

    let (res_exact, (res_phonetic, res_ngram)) = rayon::join(
        || -> Result<usize> {
            let bytes = build_fst_from_buf(exact_buf, &fst_exact_path, Some(&record_lists_exact_path))?;
            heimdall_core::compressed_io::compress_file(&fst_exact_path, 19)?;
            if record_lists_exact_path.exists() {
                heimdall_core::compressed_io::compress_file(&record_lists_exact_path, 19)?;
            }
            Ok(bytes)
        },
        || rayon::join(
            || -> Result<usize> {
                let bytes = build_fst_from_buf(phonetic_buf, &fst_phonetic_path, Some(&record_lists_phonetic_path))?;
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
                let bytes = build_fst_from_buf_ngram(ngram_buf, &fst_ngram_path, &record_lists_ngram_path)?;
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

    // Clean up per-FST scratch sub-dirs (now empty — MergedIter::Drop
    // already removed any spill files). Best-effort: a leftover dir
    // doesn't break the index.
    std::fs::remove_dir_all(&exact_scratch).ok();
    std::fs::remove_dir_all(&phonetic_scratch).ok();
    std::fs::remove_dir_all(&ngram_scratch).ok();
    // Also try to remove the parent .scratch/ dir if empty (it'll be
    // empty unless something else co-occupies it, which shouldn't
    // happen for pack.rs invocations).
    std::fs::remove_dir(&pack_opts.scratch_dir).ok();

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

/// Build the trigram FST from a `SortBuffer<(u32, u16)>` of
/// `(key, (record_id, importance))` pairs.
///
/// Same group-by-key + top-N posting-list pattern as `build_fst_from_buf`
/// but with `MAX_NGRAM_POSTINGS_PER_KEY` (4096) instead of the smaller
/// `MAX_POSTINGS_PER_KEY` (16) — common letter pairs like `^st` appear
/// in thousands of records and need a much larger cap.
fn build_fst_from_buf_ngram(
    buf: SortBuffer<(u32, u16)>,
    fst_path: &Path,
    sidecar_path: &Path,
) -> Result<usize> {
    use std::io::Write;

    if buf.is_empty() {
        let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
        let mut builder = MapBuilder::new(file)?;
        builder.finish()?;
        std::fs::write(sidecar_path, &[][..])?;
        return Ok(std::fs::metadata(fst_path)?.len() as usize);
    }

    let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
    let mut builder = MapBuilder::new(file)?;

    let mut sidecar_writer = std::io::BufWriter::with_capacity(
        4 * 1024 * 1024,
        std::fs::File::create(sidecar_path)?,
    );
    let mut sidecar_offset: u64 = 0;

    let mut prev_key: Vec<u8> = Vec::new();
    let mut group: Vec<(u32, u16)> = Vec::new();
    let mut have_prev = false;

    fn flush_ngram_group(
        builder: &mut MapBuilder<std::io::BufWriter<std::fs::File>>,
        sidecar_writer: &mut std::io::BufWriter<std::fs::File>,
        sidecar_offset: &mut u64,
        key: &[u8],
        group: &mut Vec<(u32, u16)>,
    ) -> Result<()> {
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
        builder.insert(key, offset)?;

        group.clear();
        Ok(())
    }

    for entry in buf.finish()? {
        let (key, (id, importance)) = entry?;
        if !have_prev {
            prev_key = key;
            have_prev = true;
        } else if key != prev_key {
            flush_ngram_group(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;
            prev_key = key;
        }
        group.push((id, importance));
    }
    if have_prev {
        flush_ngram_group(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;
    }

    builder.finish()?;
    sidecar_writer.flush()?;

    Ok(std::fs::metadata(fst_path)?.len() as usize)
}

/// Build the exact / phonetic FST from a `SortBuffer<(u32, u16)>` of
/// `(key, (record_id, importance))` pairs.
///
/// When `sidecar_path` is `Some`, each FST value is the byte offset of a posting list
/// in the sidecar. Each posting list begins with a `u16` count followed by
/// `count × u32` record_ids, sorted by importance descending and capped at
/// `MAX_POSTINGS_PER_KEY`. When `sidecar_path` is `None`, the legacy single-id
/// format is written (FST value = record_id) — used for the empty path.
///
/// Memory: SortBuffer's mem_limit (default 256 MB) + the FST builder
/// streaming buffer. SortBuffer spills to disk when the in-memory batch
/// crosses the budget; small countries stay entirely in RAM.
fn build_fst_from_buf(
    buf: SortBuffer<(u32, u16)>,
    fst_path: &Path,
    sidecar_path: Option<&Path>,
) -> Result<usize> {
    use std::io::Write;

    if buf.is_empty() {
        // Empty — write empty FST and (optionally) empty sidecar
        let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
        let mut builder = MapBuilder::new(file)?;
        builder.finish()?;
        if let Some(p) = sidecar_path {
            std::fs::write(p, &[][..])?;
        }
        return Ok(std::fs::metadata(fst_path)?.len() as usize);
    }

    let file = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
    let mut builder = MapBuilder::new(file)?;

    // Sidecar writer (optional). When `None` we emit single-id values
    // (legacy path, used only for the empty ngram fallback).
    let mut sidecar_writer: Option<std::io::BufWriter<std::fs::File>> = match sidecar_path {
        Some(p) => Some(std::io::BufWriter::with_capacity(
            4 * 1024 * 1024,
            std::fs::File::create(p)?,
        )),
        None => None,
    };
    let mut sidecar_offset: u64 = 0;

    let mut prev_key: Vec<u8> = Vec::new();
    let mut have_prev = false;
    // Buffered postings for the current key: (record_id, importance).
    // Sorted by importance desc on group close, deduped by record_id
    // (highest importance wins), then truncated to MAX_POSTINGS_PER_KEY.
    let mut group: Vec<(u32, u16)> = Vec::new();

    fn flush_group(
        builder: &mut MapBuilder<std::io::BufWriter<std::fs::File>>,
        sidecar_writer: &mut Option<std::io::BufWriter<std::fs::File>>,
        sidecar_offset: &mut u64,
        key: &[u8],
        group: &mut Vec<(u32, u16)>,
    ) -> Result<()> {
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
                builder.insert(key, offset)?;
            }
            None => {
                // Legacy single-id path. Pick the first (highest-importance) entry.
                builder.insert(key, group[0].0 as u64)?;
            }
        }

        group.clear();
        Ok(())
    }

    for entry in buf.finish()? {
        let (key, (id, importance)) = entry?;
        if !have_prev {
            prev_key = key;
            have_prev = true;
        } else if key != prev_key {
            flush_group(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;
            prev_key = key;
        }
        group.push((id, importance));
    }
    // Flush the last group
    if have_prev {
        flush_group(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;
    }

    builder.finish()?;
    if let Some(mut w) = sidecar_writer {
        w.flush()?;
    }

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
fn index_per_word_keys(
    buf: &mut SortBuffer<(u32, u16)>,
    counter: &mut usize,
    primary_lower: &str,
    record_id: u32,
    importance: u16,
    stopwords: &[String],
) -> Result<()> {
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
        buf.push(w.as_bytes().to_vec(), (record_id, demoted))?;
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

// NOTE (Phase 5 follow-up — TODO_REBUILD_MODES.md):
// The in-memory `build_fst` helper that used to live here was removed in
// Phase 2 in favour of a TSV-on-disk + GNU `sort` shell-out. That has
// since been replaced by `build_fst_from_buf` / `build_fst_from_buf_ngram`
// (above) which consume a typed `SortBuffer<(u32, u16)>` populated
// directly from the parquet streaming loop — no TSV intermediate, no
// shell-out, and bounded RAM via SortBuffer's spill machinery. Byte-
// identity with the old TSV path is pinned by
// `pack_fst_from_typed_buffer_matches_legacy_tsv_path` below.

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
                class: None,
                class_value: None,
                bbox: None,
                extratags: vec![],
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

#[cfg(test)]
mod tests {
    //! Byte-identity tests pinning the Phase-5 typed-buffer migration:
    //! the FST + sidecar bytes produced by `build_fst_from_buf` /
    //! `build_fst_from_buf_ngram` must match the bytes the previous
    //! GNU-`sort`-on-TSV pipeline produced for the same inputs.
    //!
    //! If a future change reorders FST keys at the boundary (e.g. by
    //! changing collation, dedup tie-breakers, or post-flush ordering),
    //! one of these tests will fail — flagging that the next 192-country
    //! rebuild will produce different bytes on disk.
    use super::*;
    use std::io::Write as _;

    /// Reference TSV+GNU-sort builder. This is a verbatim copy of the
    /// pre-Phase-5 code path, kept here as a test-only oracle so the new
    /// `SortBuffer`-based builder can be byte-compared to it.
    fn build_fst_via_legacy_tsv_path(
        inputs: &[(Vec<u8>, u32, u16)],
        fst_path: &Path,
        sidecar_path: Option<&Path>,
    ) -> Result<()> {
        use std::io::BufRead;

        if inputs.is_empty() {
            let f = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
            let mut b = MapBuilder::new(f)?;
            b.finish()?;
            if let Some(p) = sidecar_path { std::fs::write(p, &[][..])?; }
            return Ok(());
        }

        let dir = fst_path.parent().unwrap().to_path_buf();
        let tsv_path = dir.join(format!(
            "{}.tsv",
            fst_path.file_stem().unwrap().to_string_lossy(),
        ));
        {
            let f = std::fs::File::create(&tsv_path)?;
            let mut w = std::io::BufWriter::new(f);
            for (key, id, importance) in inputs {
                // Same TSV layout as pre-Phase-5: `key\tid\timportance\t<unused-pop-flag>`.
                // Pop-flag value is irrelevant — column 4 was never read.
                w.write_all(key)?;
                writeln!(w, "\t{}\t{}\t0", id, importance)?;
            }
        }
        let sorted_path = tsv_path.with_extension("sorted.tsv");
        let status = std::process::Command::new("sort")
            .env("LC_ALL", "C")
            .args(["-t", "\t", "-k1,1", "-s", "--buffer-size=64M"])
            .arg(&tsv_path)
            .stdout(std::fs::File::create(&sorted_path)?)
            .status()?;
        anyhow::ensure!(status.success(), "GNU sort failed");

        let reader = std::io::BufReader::new(std::fs::File::open(&sorted_path)?);
        let f = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
        let mut builder = MapBuilder::new(f)?;
        let mut sidecar_writer: Option<std::io::BufWriter<std::fs::File>> = match sidecar_path {
            Some(p) => Some(std::io::BufWriter::new(std::fs::File::create(p)?)),
            None => None,
        };
        let mut sidecar_offset: u64 = 0;
        let mut prev_key = String::new();
        let mut group: Vec<(u32, u16)> = Vec::new();

        let mut flush = |
            builder: &mut MapBuilder<std::io::BufWriter<std::fs::File>>,
            sidecar_writer: &mut Option<std::io::BufWriter<std::fs::File>>,
            sidecar_offset: &mut u64,
            key: &str,
            group: &mut Vec<(u32, u16)>,
        | -> Result<()> {
            if key.is_empty() || group.is_empty() { return Ok(()); }
            group.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            group.dedup_by_key(|(id, _)| *id);
            group.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            if group.len() > MAX_POSTINGS_PER_KEY { group.truncate(MAX_POSTINGS_PER_KEY); }
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
                flush(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;
                prev_key = key.to_owned();
            }
            group.push((id, importance));
        }
        flush(&mut builder, &mut sidecar_writer, &mut sidecar_offset, &prev_key, &mut group)?;
        builder.finish()?;
        if let Some(mut w) = sidecar_writer { w.flush()?; }
        std::fs::remove_file(&sorted_path).ok();
        std::fs::remove_file(&tsv_path).ok();
        Ok(())
    }

    fn synthetic_pack_inputs(seed: u64) -> Vec<(Vec<u8>, u32, u16)> {
        // 2000 entries with a healthy duplicate-key rate. Mixes:
        //   - per-record primary keys (unique)
        //   - per-word keys ("stockholm", "domkyrka") shared across many records
        //   - phonetic-style normalised keys
        // The duplicate key behaviour is what stresses the dedup +
        // top-N posting logic, which is where ordering bugs would show.
        let mut out: Vec<(Vec<u8>, u32, u16)> = Vec::new();
        let mut x = seed.wrapping_mul(2862933555777941757).wrapping_add(3037000493);
        let mut next = || {
            x = x.wrapping_mul(2862933555777941757).wrapping_add(3037000493);
            x
        };
        let words = [
            "stockholm", "domkyrkan", "lund", "kyrka", "globen",
            "central", "station", "park", "gata", "torget",
        ];
        for i in 0..2000u32 {
            let primary = format!("place-{:04}-{}", i % 500, words[(i as usize) % words.len()]);
            let imp = ((next() % 60000) as u16).max(1);
            out.push((primary.clone().into_bytes(), i, imp));
            // Demoted per-word entry — `imp / 128`, like the real pack.
            let demoted = ((imp as u32 / 128).max(1)) as u16;
            for w in primary.split('-') {
                if w.len() > 2 {
                    out.push((w.as_bytes().to_vec(), i, demoted));
                }
            }
            // Phonetic (synthetic): folded primary
            let phonetic_lite: String = primary.chars().filter(|c| c.is_alphabetic()).collect();
            out.push((phonetic_lite.into_bytes(), i, imp));
        }
        out
    }

    /// Pin: the FST and sidecar bytes from `build_fst_from_buf` match
    /// the legacy GNU-sort + TSV pipeline byte-for-byte. If this fails,
    /// the change forces a 192-country reindex.
    #[test]
    fn pack_fst_from_typed_buffer_matches_legacy_tsv_path() {
        let inputs = synthetic_pack_inputs(0xDEADBEEF);

        let dir_legacy = tempfile::tempdir().unwrap();
        let dir_buf = tempfile::tempdir().unwrap();

        let legacy_fst = dir_legacy.path().join("legacy.fst");
        let legacy_sc = dir_legacy.path().join("legacy.sidecar");
        build_fst_via_legacy_tsv_path(&inputs, &legacy_fst, Some(&legacy_sc)).unwrap();

        let buf_fst = dir_buf.path().join("buf.fst");
        let buf_sc = dir_buf.path().join("buf.sidecar");
        let scratch = dir_buf.path().join("scratch");
        let mut sb = SortBuffer::<(u32, u16)>::new(64 * 1024 * 1024, &scratch).unwrap();
        for (k, id, imp) in &inputs {
            sb.push(k.clone(), (*id, *imp)).unwrap();
        }
        build_fst_from_buf(sb, &buf_fst, Some(&buf_sc)).unwrap();

        let a_fst = std::fs::read(&legacy_fst).unwrap();
        let b_fst = std::fs::read(&buf_fst).unwrap();
        assert_eq!(
            a_fst, b_fst,
            "FST bytes diverge — legacy path: {} bytes, SortBuffer path: {} bytes",
            a_fst.len(), b_fst.len(),
        );
        let a_sc = std::fs::read(&legacy_sc).unwrap();
        let b_sc = std::fs::read(&buf_sc).unwrap();
        assert_eq!(
            a_sc, b_sc,
            "sidecar bytes diverge — legacy: {} bytes, SortBuffer: {} bytes",
            a_sc.len(), b_sc.len(),
        );
    }

    /// Same byte-identity contract under SortBuffer's spill path. A tiny
    /// `mem_limit` forces multiple spills + k-way merge, so the
    /// post-merge order has to match GNU sort's stable-by-bytes order
    /// even across spill boundaries.
    #[test]
    fn pack_fst_from_typed_buffer_byte_identical_under_spill() {
        let inputs = synthetic_pack_inputs(0xCAFE_BABE);

        let dir_legacy = tempfile::tempdir().unwrap();
        let dir_buf = tempfile::tempdir().unwrap();

        let legacy_fst = dir_legacy.path().join("legacy.fst");
        let legacy_sc = dir_legacy.path().join("legacy.sidecar");
        build_fst_via_legacy_tsv_path(&inputs, &legacy_fst, Some(&legacy_sc)).unwrap();

        let buf_fst = dir_buf.path().join("buf.fst");
        let buf_sc = dir_buf.path().join("buf.sidecar");
        let scratch = dir_buf.path().join("scratch");
        // 8 KB ceiling forces aggressive spilling on a 2000-entry input.
        let mut sb = SortBuffer::<(u32, u16)>::new(8 * 1024, &scratch).unwrap();
        for (k, id, imp) in &inputs {
            sb.push(k.clone(), (*id, *imp)).unwrap();
        }
        // Sanity: we actually exercised the spill path.
        assert!(sb.run_count() >= 2, "expected ≥ 2 spills, got {}", sb.run_count());
        build_fst_from_buf(sb, &buf_fst, Some(&buf_sc)).unwrap();

        let a_fst = std::fs::read(&legacy_fst).unwrap();
        let b_fst = std::fs::read(&buf_fst).unwrap();
        assert_eq!(a_fst, b_fst, "FST bytes diverge under spill path");
        let a_sc = std::fs::read(&legacy_sc).unwrap();
        let b_sc = std::fs::read(&buf_sc).unwrap();
        assert_eq!(a_sc, b_sc, "sidecar bytes diverge under spill path");
    }

    /// The ngram builder uses the same group/dedup logic but with a
    /// larger posting cap. Pin the FST + sidecar bytes against the
    /// legacy path under that cap.
    #[test]
    fn pack_fst_ngram_from_typed_buffer_matches_legacy_tsv_path() {
        let mut inputs = synthetic_pack_inputs(0x1234_5678);
        // Inject a hot-key cluster ("^st") to stress dedup with
        // many id collisions.
        for i in 0..200u32 {
            inputs.push((b"^st".to_vec(), i, ((i % 60000) as u16).max(1)));
            inputs.push((b"sto".to_vec(), i, 100));
            inputs.push((b"tor".to_vec(), i, 100));
        }

        // Test-only oracle that mirrors `build_fst_from_buf_ngram`
        // running over a TSV+GNU-sort intermediate (cap 4096).
        fn build_ngram_legacy(
            inputs: &[(Vec<u8>, u32, u16)],
            fst_path: &Path,
            sidecar_path: &Path,
        ) -> Result<()> {
            use std::io::BufRead;
            if inputs.is_empty() {
                let f = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
                let mut b = MapBuilder::new(f)?;
                b.finish()?;
                std::fs::write(sidecar_path, &[][..])?;
                return Ok(());
            }
            let dir = fst_path.parent().unwrap();
            let tsv = dir.join("ngram.tsv");
            {
                let mut w = std::io::BufWriter::new(std::fs::File::create(&tsv)?);
                for (k, id, imp) in inputs {
                    w.write_all(k)?;
                    writeln!(w, "\t{}\t{}\t0", id, imp)?;
                }
            }
            let sorted = tsv.with_extension("sorted.tsv");
            let status = std::process::Command::new("sort")
                .env("LC_ALL", "C")
                .args(["-t", "\t", "-k1,1", "-s", "--buffer-size=64M"])
                .arg(&tsv)
                .stdout(std::fs::File::create(&sorted)?)
                .status()?;
            anyhow::ensure!(status.success(), "GNU sort failed");

            let reader = std::io::BufReader::new(std::fs::File::open(&sorted)?);
            let f = std::io::BufWriter::new(std::fs::File::create(fst_path)?);
            let mut builder = MapBuilder::new(f)?;
            let mut sidecar = std::io::BufWriter::new(std::fs::File::create(sidecar_path)?);
            let mut offset: u64 = 0;
            let mut prev_key = String::new();
            let mut group: Vec<(u32, u16)> = Vec::new();
            let mut flush = |
                builder: &mut MapBuilder<std::io::BufWriter<std::fs::File>>,
                sidecar: &mut std::io::BufWriter<std::fs::File>,
                offset: &mut u64,
                key: &str,
                group: &mut Vec<(u32, u16)>,
            | -> Result<()> {
                if key.is_empty() || group.is_empty() { return Ok(()); }
                group.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
                group.dedup_by_key(|(id, _)| *id);
                group.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
                if group.len() > MAX_NGRAM_POSTINGS_PER_KEY {
                    group.truncate(MAX_NGRAM_POSTINGS_PER_KEY);
                }
                let count = group.len() as u16;
                let off = *offset;
                sidecar.write_all(&count.to_le_bytes())?;
                for &(id, _) in group.iter() {
                    sidecar.write_all(&id.to_le_bytes())?;
                }
                *offset += 2 + (group.len() as u64) * 4;
                builder.insert(key.as_bytes(), off)?;
                group.clear();
                Ok(())
            };
            for line in reader.lines() {
                let line = line?;
                let parts: Vec<&str> = line.split('\t').collect();
                if parts.len() < 4 { continue; }
                let key = parts[0];
                let id: u32 = parts[1].parse().unwrap_or(0);
                let imp: u16 = parts[2].parse().unwrap_or(0);
                if key != prev_key {
                    flush(&mut builder, &mut sidecar, &mut offset, &prev_key, &mut group)?;
                    prev_key = key.to_owned();
                }
                group.push((id, imp));
            }
            flush(&mut builder, &mut sidecar, &mut offset, &prev_key, &mut group)?;
            builder.finish()?;
            sidecar.flush()?;
            std::fs::remove_file(&sorted).ok();
            std::fs::remove_file(&tsv).ok();
            Ok(())
        }

        let dir_legacy = tempfile::tempdir().unwrap();
        let dir_buf = tempfile::tempdir().unwrap();
        let legacy_fst = dir_legacy.path().join("legacy.fst");
        let legacy_sc = dir_legacy.path().join("legacy.sidecar");
        build_ngram_legacy(&inputs, &legacy_fst, &legacy_sc).unwrap();

        let buf_fst = dir_buf.path().join("buf.fst");
        let buf_sc = dir_buf.path().join("buf.sidecar");
        let scratch = dir_buf.path().join("scratch");
        let mut sb = SortBuffer::<(u32, u16)>::new(64 * 1024 * 1024, &scratch).unwrap();
        for (k, id, imp) in &inputs {
            sb.push(k.clone(), (*id, *imp)).unwrap();
        }
        build_fst_from_buf_ngram(sb, &buf_fst, &buf_sc).unwrap();

        assert_eq!(
            std::fs::read(&legacy_fst).unwrap(),
            std::fs::read(&buf_fst).unwrap(),
            "ngram FST bytes diverge",
        );
        assert_eq!(
            std::fs::read(&legacy_sc).unwrap(),
            std::fs::read(&buf_sc).unwrap(),
            "ngram sidecar bytes diverge",
        );
    }

    /// Empty-input behaviour matches the legacy path: empty FST + empty sidecar.
    #[test]
    fn pack_fst_from_typed_buffer_empty_matches_legacy() {
        let dir_legacy = tempfile::tempdir().unwrap();
        let dir_buf = tempfile::tempdir().unwrap();
        let legacy_fst = dir_legacy.path().join("legacy.fst");
        let legacy_sc = dir_legacy.path().join("legacy.sidecar");
        build_fst_via_legacy_tsv_path(&[], &legacy_fst, Some(&legacy_sc)).unwrap();

        let buf_fst = dir_buf.path().join("buf.fst");
        let buf_sc = dir_buf.path().join("buf.sidecar");
        let scratch = dir_buf.path().join("scratch");
        let sb = SortBuffer::<(u32, u16)>::new(1024, &scratch).unwrap();
        build_fst_from_buf(sb, &buf_fst, Some(&buf_sc)).unwrap();

        assert_eq!(std::fs::read(&legacy_fst).unwrap(), std::fs::read(&buf_fst).unwrap());
        assert_eq!(std::fs::read(&legacy_sc).unwrap(), std::fs::read(&buf_sc).unwrap());
    }
}
