/// continuous.rs — Long-running continuous comparison mode.
/// Loads indices, samples queries on-the-fly, and runs indefinitely.
/// This is the refactored version of the original `--run` mode.

use std::path::{Path, PathBuf};

use anyhow::bail;
use rand::prelude::*;

use crate::db;
use crate::http::GeocoderClient;
use crate::sampling::{load_country, mutate_name, nearest_city_name, CountryData};
use crate::types::{categorize, truncate, Category};

use heimdall_core::types::Coord;

pub async fn run_continuous(
    index_paths: &[PathBuf],
    heimdall_url: &str,
    nominatim_url: &str,
    rps: f64,
    pool_size: usize,
    db_path: &Path,
) -> anyhow::Result<()> {
    if index_paths.is_empty() {
        bail!("No index directories specified. Use --index <path>");
    }

    // Load indices
    println!("Loading indices...");
    let mut countries: Vec<CountryData> = Vec::new();
    for path in index_paths {
        println!("  Loading {}...", path.display());
        let data = load_country(path)?;
        println!(
            "    {} -- {} places, {} addresses",
            data.code,
            data.records.len(),
            data.addr_store.as_ref().map(|a| a.total_houses()).unwrap_or(0),
        );
        countries.push(data);
    }

    // Open/create SQLite DB
    let conn = db::open_db(db_path)?;

    // Load or generate query pool
    let pool_path = db_path.with_extension("pool.json");
    let pool: Vec<SampledQuery> = if pool_path.exists() {
        println!("Loading existing query pool from {}...", pool_path.display());
        let data = std::fs::read_to_string(&pool_path)?;
        serde_json::from_str(&data)?
    } else {
        println!("Sampling {} queries...", pool_size);
        let pool = sample_queries_legacy(&countries, pool_size);
        println!(
            "  Sampled: {} address, {} place, {} fuzzy",
            pool.iter().filter(|q| q.query_type == "address").count(),
            pool.iter().filter(|q| q.query_type == "place").count(),
            pool.iter().filter(|q| q.query_type == "fuzzy").count(),
        );
        std::fs::write(&pool_path, serde_json::to_string_pretty(&pool)?)?;
        println!("  Saved to {}", pool_path.display());
        pool
    };

    // Filter already-queried (use query_id based on index)
    let completed = db::get_completed_ids(&conn)?;
    let remaining_indices: Vec<usize> = (0..pool.len())
        .filter(|&i| !completed.contains(&format!("continuous_{}", i)))
        .collect();

    let total_pool = pool.len();
    let done_count = completed.len();
    println!(
        "{} total queries, {} already done, {} remaining",
        total_pool,
        done_count,
        remaining_indices.len()
    );

    if remaining_indices.is_empty() {
        println!("All queries completed! Run `report` to see results.");
        return Ok(());
    }

    // Create HTTP client
    let mut client = GeocoderClient::new(heimdall_url, nominatim_url, rps)?;

    // Track running stats
    let mut agree_count: usize = db::count_category(&conn, "AGREE").unwrap_or(0);
    let mut total_queried: usize = done_count;

    for &idx in &remaining_indices {
        let sq = &pool[idx];
        let query_id = format!("continuous_{}", idx);

        // Query Heimdall
        let h = client
            .search_heimdall(&sq.query, Some(&sq.country))
            .await;

        // Query Nominatim (rate limited)
        let n = client
            .search_nominatim(&sq.query, Some(&sq.country))
            .await;

        // Categorize
        let (category, distance) = if h.is_error {
            (Category::HeimdallError, None)
        } else if n.is_error {
            (Category::NominatimError, None)
        } else {
            let h_coord = h.lat.zip(h.lon);
            let n_coord = n.lat.zip(n.lon);
            categorize(h_coord, n_coord)
        };

        if matches!(category, Category::Agree) {
            agree_count += 1;
        }
        total_queried += 1;

        let agree_pct = if total_queried > 0 {
            agree_count as f64 / total_queried as f64 * 100.0
        } else {
            0.0
        };

        // Build a QueryEntry for db::insert_result
        let entry = crate::types::QueryEntry {
            id: query_id,
            q: Some(sq.query.clone()),
            category: sq.query_type.clone(),
            country: Some(sq.country.clone()),
            lat: None,
            lon: None,
            expected_lat: None,
            expected_lon: None,
            variant_of: None,
            variant_type: None,
        };

        let cat_str = category.as_str();
        db::insert_result(
            &conn,
            &entry,
            h.lat,
            h.lon,
            h.display_name.as_deref(),
            h.latency_ms,
            n.lat,
            n.lon,
            n.display_name.as_deref(),
            n.latency_ms,
            distance,
            cat_str,
        )?;

        // Progress
        let dist_str = distance
            .map(|d| format!("{:.0}m", d))
            .unwrap_or_else(|| "-".into());
        let type_short = match sq.query_type.as_str() {
            "address" => "addr",
            "place" => "place",
            "fuzzy" => "fuzzy",
            other => other,
        };
        println!(
            "[{}/{}] {} {} \"{}\" -> {} {}  ({:.1}% agree)",
            total_queried,
            total_pool,
            sq.country,
            type_short,
            truncate(&sq.query, 40),
            cat_str,
            dist_str,
            agree_pct,
        );

        if total_queried % 1000 == 0 {
            println!(
                "\n--- {} queries completed ({:.1}% agree) ---\n",
                total_queried, agree_pct
            );
        }
    }

    println!(
        "\nComparison complete! {} queries total. Run `report` to see results.",
        total_queried
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Legacy query sampling (preserves old behavior for continuous mode)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SampledQuery {
    query: String,
    query_type: String,
    country: String,
}

fn sample_queries_legacy(countries: &[CountryData], total: usize) -> Vec<SampledQuery> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut queries: Vec<SampledQuery> = Vec::new();

    let n_addr = (total as f64 * 0.6) as usize;
    let n_place = (total as f64 * 0.3) as usize;
    let n_fuzzy = total - n_addr - n_place;

    // Address queries (60%)
    let total_houses: usize = countries
        .iter()
        .filter_map(|c| c.addr_store.as_ref())
        .map(|a| a.total_houses())
        .sum();

    if total_houses > 0 {
        for country in countries {
            if let Some(ref addr_store) = country.addr_store {
                let country_houses = addr_store.total_houses();
                if country_houses == 0 {
                    continue;
                }
                let n = (n_addr as f64 * country_houses as f64 / total_houses as f64).ceil() as usize;
                let street_count = addr_store.street_count();
                if street_count == 0 {
                    continue;
                }

                let mut sampled = 0;
                let mut attempts = 0;
                while sampled < n && attempts < n * 3 {
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
                    let house = houses.choose(&mut rng).unwrap();
                    let city = nearest_city_name(
                        Coord { lat: header.base_lat, lon: header.base_lon },
                        &country.records,
                    );
                    let number_str = if house.suffix > 0 && house.suffix <= 26 {
                        format!("{}{}", house.number, (b'A' + house.suffix - 1) as char)
                    } else {
                        house.number.to_string()
                    };
                    let query = match &city {
                        Some(c) => format!("{} {}, {}", street_name, number_str, c),
                        None => format!("{} {}", street_name, number_str),
                    };
                    queries.push(SampledQuery {
                        query,
                        query_type: "address".into(),
                        country: country.code.clone(),
                    });
                    sampled += 1;
                }
            }
        }
    }

    // Place queries (30%)
    let total_records: usize = countries.iter().map(|c| c.records.len()).sum();
    if total_records > 0 {
        for country in countries {
            let count = country.records.len();
            if count == 0 {
                continue;
            }
            let n = (n_place as f64 * count as f64 / total_records as f64).ceil() as usize;
            let mut max_imp: u16 = 1;
            for id in 0..count {
                if let Ok(record) = country.records.get(id as u32) {
                    if record.importance > max_imp {
                        max_imp = record.importance;
                    }
                }
            }
            let expected_accept = (100.0 + max_imp as f64 / 4.0) / (max_imp as f64 + 100.0);
            let attempts_per = (1.0 / expected_accept).ceil() as usize;
            let max_attempts = (n * attempts_per * 3).max(n * 50);
            let mut sampled = 0;
            let mut attempts = 0;
            while sampled < n && attempts < max_attempts {
                attempts += 1;
                let id = rng.gen_range(0..count as u32);
                let record = match country.records.get(id) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let accept_prob = (record.importance as f64 + 100.0) / (max_imp as f64 + 100.0);
                if rng.gen::<f64>() > accept_prob {
                    continue;
                }
                let name = country.records.primary_name(&record);
                if name.is_empty() || name.len() < 2 {
                    continue;
                }
                queries.push(SampledQuery {
                    query: name,
                    query_type: "place".into(),
                    country: country.code.clone(),
                });
                sampled += 1;
            }
        }
    }

    // Fuzzy queries (10%)
    let place_for_fuzzy: Vec<SampledQuery> = queries
        .iter()
        .filter(|q| q.query_type == "place" && q.query.chars().count() > 4)
        .cloned()
        .collect();
    let n_fuzzy_actual = n_fuzzy.min(place_for_fuzzy.len());
    let fuzzy_sources: Vec<&SampledQuery> = place_for_fuzzy
        .choose_multiple(&mut rng, n_fuzzy_actual)
        .collect();
    for source in fuzzy_sources {
        let mutated = mutate_name(&source.query, &mut rng);
        queries.push(SampledQuery {
            query: mutated,
            query_type: "fuzzy".into(),
            country: source.country.clone(),
        });
    }

    queries.shuffle(&mut rng);
    queries
}
