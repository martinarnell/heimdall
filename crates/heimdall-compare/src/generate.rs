/// generate.rs — Generate benchmark query sets from loaded indices.
///
/// Produces a JSONL file with 5 query categories:
///   40% address round-trips (population-weighted)
///   30% place names + variants (importance-weighted)
///   15% fuzzy/typo queries
///   10% reverse geocoding
///    5% ambiguous (multi-country)

use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use rand::prelude::*;

use heimdall_core::types::{Coord, PlaceType};

use crate::sampling::{
    self, build_reverse_variants, compute_weights, detect_country_code, find_ambiguous_names,
    find_english_alias, mutate_name, nearest_city_name, strip_diacritics,
    CountryData,
};
use crate::types::{
    CategoryCounts, CountryWeight, MetaInner, QueryEntry, QueryFileMeta,
};

pub fn generate_queries(
    index_paths: &[PathBuf],
    total: usize,
    seed: u64,
    output: &Path,
) -> Result<()> {
    if index_paths.is_empty() {
        bail!("No index directories specified. Use --index <path>");
    }

    let mut rng = StdRng::seed_from_u64(seed);

    // Budget allocation
    let n_addr = (total as f64 * 0.40) as usize;
    let n_place = (total as f64 * 0.30) as usize;
    let n_fuzzy = (total as f64 * 0.15) as usize;
    let n_reverse = (total as f64 * 0.10) as usize;
    let n_ambig = total - n_addr - n_place - n_fuzzy - n_reverse;

    // Detect country codes and compute weights
    let country_codes: Vec<String> = index_paths
        .iter()
        .map(|p| {
            let dir = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            detect_country_code(dir)
        })
        .collect();

    let weights = compute_weights(&country_codes);

    // Per-country query budgets (round, not ceil, to avoid overshoot)
    let country_budgets: Vec<(usize, usize, usize, usize)> = weights
        .iter()
        .map(|(_, _, w)| {
            let addr = (n_addr as f64 * w).round().max(1.0) as usize;
            let place = (n_place as f64 * w).round().max(1.0) as usize;
            let fuzzy = (n_fuzzy as f64 * w).round().max(1.0) as usize;
            let reverse = (n_reverse as f64 * w).round().max(1.0) as usize;
            (addr, place, fuzzy, reverse)
        })
        .collect();

    let mut all_queries: Vec<QueryEntry> = Vec::new();
    let mut variant_count: usize = 0;

    // We also need RecordStores loaded for ambiguous detection
    // Load all indices into memory for ambiguous check (just RecordStores, which are mmap'd)
    let mut country_records_for_ambig: Vec<(String, heimdall_core::record_store::RecordStore)> = Vec::new();

    // Process each country
    for (i, index_path) in index_paths.iter().enumerate() {
        let cc = &country_codes[i];
        let (budget_addr, budget_place, _budget_fuzzy, budget_reverse) = country_budgets[i];

        eprintln!("Loading {}...", index_path.display());
        let data = sampling::load_country(index_path)?;
        eprintln!(
            "  {} -- {} places, {} addresses",
            data.code,
            data.records.len(),
            data.addr_store.as_ref().map(|a| a.total_houses()).unwrap_or(0),
        );

        // Load normalizer for known_variants
        let normalizer_path = index_path.join("sv.toml");
        let known_variants_reverse = if normalizer_path.exists() {
            let normalizer = heimdall_normalize::Normalizer::from_config(&normalizer_path);
            build_reverse_variants(normalizer.known_variants())
        } else {
            eprintln!("  Warning: no sv.toml normalizer found, English alias variants disabled");
            HashMap::new()
        };

        // --- Address queries ---
        sample_addresses(&data, cc, budget_addr, &mut rng, &mut all_queries);

        // --- Place queries + variants ---
        sample_places(
            &data,
            cc,
            budget_place,
            &mut rng,
            &known_variants_reverse,
            &mut all_queries,
            &mut variant_count,
        );

        // --- Reverse queries ---
        sample_reverse(&data, cc, budget_reverse, &mut rng, &mut all_queries);

        // Keep RecordStore for ambiguous detection (it's mmap'd, lightweight)
        country_records_for_ambig.push((cc.clone(), data.records));
        // data.addr_store is dropped here, freeing resources
    }

    // --- Fuzzy queries ---
    let fuzzy_sources: Vec<QueryEntry> = all_queries
        .iter()
        .filter(|q| q.category == "place" && q.variant_of.is_none())
        .filter(|q| q.q.as_ref().map(|s| s.chars().count() > 4).unwrap_or(false))
        .cloned()
        .collect();

    let n_fuzzy_actual = n_fuzzy.min(fuzzy_sources.len());
    let fuzzy_indices: Vec<usize> = (0..fuzzy_sources.len()).collect();
    let selected: Vec<usize> = fuzzy_indices
        .choose_multiple(&mut rng, n_fuzzy_actual)
        .copied()
        .collect();

    for (fi, &src_idx) in selected.iter().enumerate() {
        let source = &fuzzy_sources[src_idx];
        if let Some(ref q) = source.q {
            let mutated = mutate_name(q, &mut rng);
            let cc_lower = source.country.as_deref().unwrap_or("xx").to_lowercase();
            all_queries.push(QueryEntry {
                id: format!("fuzzy_{}_{:06}", cc_lower, fi),
                q: Some(mutated),
                category: "fuzzy".into(),
                country: source.country.clone(),
                lat: None,
                lon: None,
                expected_lat: source.expected_lat,
                expected_lon: source.expected_lon,
                variant_of: None,
                variant_type: None,
            });
        }
    }

    // --- Ambiguous queries ---
    let ambiguous_names = find_ambiguous_names(&country_records_for_ambig, n_ambig * 2);
    let n_ambig_actual = n_ambig.min(ambiguous_names.len());
    let selected_ambig: Vec<&String> = ambiguous_names
        .choose_multiple(&mut rng, n_ambig_actual)
        .collect();

    for (ai, name) in selected_ambig.iter().enumerate() {
        all_queries.push(QueryEntry {
            id: format!("ambig_{:06}", ai),
            q: Some(name.to_string()),
            category: "ambiguous".into(),
            country: None,
            lat: None,
            lon: None,
            expected_lat: None,
            expected_lon: None,
            variant_of: None,
            variant_type: None,
        });
    }

    // Shuffle all queries
    all_queries.shuffle(&mut rng);

    // Count categories
    let counts = CategoryCounts {
        address: all_queries.iter().filter(|q| q.category == "address").count(),
        place: all_queries.iter().filter(|q| q.category == "place" && q.variant_of.is_none()).count(),
        fuzzy: all_queries.iter().filter(|q| q.category == "fuzzy").count(),
        reverse: all_queries.iter().filter(|q| q.category == "reverse").count(),
        ambiguous: all_queries.iter().filter(|q| q.category == "ambiguous").count(),
        variants: variant_count,
    };

    // Build metadata
    let meta = QueryFileMeta {
        _meta: MetaInner {
            version: 1,
            seed,
            total_queries: all_queries.len(),
            categories: counts,
            population_weights: weights
                .iter()
                .zip(country_budgets.iter())
                .map(|((cc, pop, w), (a, p, f, r))| CountryWeight {
                    code: cc.clone(),
                    population_millions: *pop,
                    weight: *w,
                    query_count: a + p + f + r,
                })
                .collect(),
            generated_at: chrono::Utc::now().to_rfc3339(),
        },
    };

    // Write JSONL
    let file = std::fs::File::create(output)?;
    let mut writer = BufWriter::new(file);

    // First line: metadata
    writeln!(writer, "{}", serde_json::to_string(&meta)?)?;

    // Query lines
    for entry in &all_queries {
        writeln!(writer, "{}", serde_json::to_string(entry)?)?;
    }

    writer.flush()?;

    eprintln!(
        "Generated {} queries -> {}",
        all_queries.len(),
        output.display()
    );
    eprintln!(
        "  {} address, {} place ({} variants), {} fuzzy, {} reverse, {} ambiguous",
        meta._meta.categories.address,
        meta._meta.categories.place,
        meta._meta.categories.variants,
        meta._meta.categories.fuzzy,
        meta._meta.categories.reverse,
        meta._meta.categories.ambiguous,
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Per-category samplers
// ---------------------------------------------------------------------------

fn sample_addresses(
    data: &CountryData,
    cc: &str,
    budget: usize,
    rng: &mut StdRng,
    queries: &mut Vec<QueryEntry>,
) {
    let addr_store = match &data.addr_store {
        Some(a) => a,
        None => return,
    };

    let street_count = addr_store.street_count();
    if street_count == 0 {
        return;
    }

    let cc_lower = cc.to_lowercase();
    let mut sampled = 0;
    let mut attempts = 0;

    while sampled < budget && attempts < budget * 3 {
        attempts += 1;
        let street_id = rng.gen_range(0..street_count as u32);
        let header = match addr_store.get_street(street_id) {
            Some(h) => h,
            None => continue,
        };
        let street_name = addr_store.street_name(&header);
        if street_name.is_empty() {
            continue;
        }

        let houses = addr_store.street_houses(&header);
        if houses.is_empty() {
            continue;
        }

        let house = houses.choose(rng).unwrap();
        let base_coord = Coord {
            lat: header.base_lat,
            lon: header.base_lon,
        };
        let city = nearest_city_name(base_coord, &data.records);

        let number_str = if house.suffix > 0 && house.suffix <= 26 {
            format!("{}{}", house.number, (b'A' + house.suffix - 1) as char)
        } else {
            house.number.to_string()
        };

        let query = match &city {
            Some(c) => format!("{} {}, {}", street_name, number_str, c),
            None => format!("{} {}", street_name, number_str),
        };

        // Compute expected coordinate from house delta
        let expected_lat = base_coord.lat_f64() + (house.delta_lat as f64 * 1e-6);
        let expected_lon = base_coord.lon_f64() + (house.delta_lon as f64 * 1e-6);

        queries.push(QueryEntry {
            id: format!("addr_{}_{:06}", cc_lower, sampled),
            q: Some(query),
            category: "address".into(),
            country: Some(cc.to_uppercase()),
            lat: None,
            lon: None,
            expected_lat: Some(expected_lat),
            expected_lon: Some(expected_lon),
            variant_of: None,
            variant_type: None,
        });
        sampled += 1;
    }

    if sampled > 0 {
        eprintln!("  {} address queries sampled", sampled);
    }
}

fn sample_places(
    data: &CountryData,
    cc: &str,
    budget: usize,
    rng: &mut StdRng,
    known_variants_reverse: &HashMap<String, Vec<String>>,
    queries: &mut Vec<QueryEntry>,
    variant_count: &mut usize,
) {
    let count = data.records.len();
    if count == 0 {
        return;
    }

    let cc_lower = cc.to_lowercase();

    // Find max importance for rejection sampling
    let mut max_imp: u16 = 1;
    let step = if count > 100_000 { count / 100_000 } else { 1 };
    for id in (0..count).step_by(step) {
        if let Ok(record) = data.records.get(id as u32) {
            if record.importance > max_imp {
                max_imp = record.importance;
            }
        }
    }

    let mut sampled = 0;
    let mut attempts = 0;

    // Compute expected acceptance rate to size the attempt budget appropriately.
    // With max_imp=65535, median records have ~0.15% acceptance, so we need many attempts.
    let expected_accept = (100.0 + max_imp as f64 / 4.0) / (max_imp as f64 + 100.0);
    let attempts_per_query = (1.0 / expected_accept).ceil() as usize;
    let max_attempts = (budget * attempts_per_query * 3).max(budget * 50);

    while sampled < budget && attempts < max_attempts {
        attempts += 1;
        let id = rng.gen_range(0..count as u32);
        let record = match data.records.get(id) {
            Ok(r) => r,
            Err(_) => continue,
        };

        // Rejection sampling: higher importance → higher acceptance
        let accept_prob = (record.importance as f64 + 100.0) / (max_imp as f64 + 100.0);
        if rng.gen::<f64>() > accept_prob {
            continue;
        }

        let name = data.records.primary_name(&record);
        if name.is_empty() || name.len() < 2 {
            continue;
        }

        let base_id = format!("place_{}_{:06}", cc_lower, sampled);

        // Canonical query
        queries.push(QueryEntry {
            id: base_id.clone(),
            q: Some(name.clone()),
            category: "place".into(),
            country: Some(cc.to_uppercase()),
            lat: None,
            lon: None,
            expected_lat: Some(record.coord.lat_f64()),
            expected_lon: Some(record.coord.lon_f64()),
            variant_of: None,
            variant_type: None,
        });

        // Diacritic-free variant
        if let Some(diac_free) = strip_diacritics(&name) {
            if diac_free.to_lowercase() != name.to_lowercase() {
                queries.push(QueryEntry {
                    id: format!("{}_diacfree", base_id),
                    q: Some(diac_free),
                    category: "place".into(),
                    country: Some(cc.to_uppercase()),
                    lat: None,
                    lon: None,
                    expected_lat: Some(record.coord.lat_f64()),
                    expected_lon: Some(record.coord.lon_f64()),
                    variant_of: Some(base_id.clone()),
                    variant_type: Some("diacritic_free".into()),
                });
                *variant_count += 1;
            }
        }

        // English alias variant
        if let Some(english) = find_english_alias(
            &name,
            &record,
            &data.records,
            known_variants_reverse,
        ) {
            queries.push(QueryEntry {
                id: format!("{}_en", base_id),
                q: Some(english),
                category: "place".into(),
                country: Some(cc.to_uppercase()),
                lat: None,
                lon: None,
                expected_lat: Some(record.coord.lat_f64()),
                expected_lon: Some(record.coord.lon_f64()),
                variant_of: Some(base_id.clone()),
                variant_type: Some("english_alias".into()),
            });
            *variant_count += 1;
        }

        sampled += 1;
    }

    if sampled > 0 {
        eprintln!("  {} place queries sampled", sampled);
    }
}

fn sample_reverse(
    data: &CountryData,
    cc: &str,
    budget: usize,
    rng: &mut StdRng,
    queries: &mut Vec<QueryEntry>,
) {
    let count = data.records.len();
    if count == 0 {
        return;
    }

    let cc_lower = cc.to_lowercase();
    let mut sampled = 0;
    let mut attempts = 0;

    // Populated places (City/Town/Village/Suburb) are typically ~5-10% of records,
    // so we need many more attempts than the budget to hit enough of them.
    while sampled < budget && attempts < budget * 50 {
        attempts += 1;
        let id = rng.gen_range(0..count as u32);
        let record = match data.records.get(id) {
            Ok(r) => r,
            Err(_) => continue,
        };

        // Only use populated places for reverse geocoding
        match record.place_type {
            PlaceType::City | PlaceType::Town | PlaceType::Village | PlaceType::Suburb => {}
            _ => continue,
        }

        let expected_lat = record.coord.lat_f64();
        let expected_lon = record.coord.lon_f64();

        // Add random offset: 10-100 meters in a random direction
        let distance_m = rng.gen_range(10.0..100.0);
        let bearing_rad = rng.gen_range(0.0..std::f64::consts::TAU);
        let (offset_lat, offset_lon) =
            offset_coord(expected_lat, expected_lon, distance_m, bearing_rad);

        queries.push(QueryEntry {
            id: format!("reverse_{}_{:06}", cc_lower, sampled),
            q: None,
            category: "reverse".into(),
            country: Some(cc.to_uppercase()),
            lat: Some(offset_lat),
            lon: Some(offset_lon),
            expected_lat: Some(expected_lat),
            expected_lon: Some(expected_lon),
            variant_of: None,
            variant_type: None,
        });
        sampled += 1;
    }

    if sampled > 0 {
        eprintln!("  {} reverse queries sampled", sampled);
    }
}

/// Offset a coordinate by a distance (meters) and bearing (radians).
fn offset_coord(lat: f64, lon: f64, distance_m: f64, bearing_rad: f64) -> (f64, f64) {
    let r = 6_371_000.0; // Earth radius in meters
    let d = distance_m / r;
    let lat_rad = lat.to_radians();
    let lon_rad = lon.to_radians();

    let new_lat = (lat_rad.sin() * d.cos() + lat_rad.cos() * d.sin() * bearing_rad.cos()).asin();
    let new_lon = lon_rad
        + (bearing_rad.sin() * d.sin() * lat_rad.cos())
            .atan2(d.cos() - lat_rad.sin() * new_lat.sin());

    (new_lat.to_degrees(), new_lon.to_degrees())
}
