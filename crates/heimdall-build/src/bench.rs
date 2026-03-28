/// bench.rs — Accuracy benchmark: Heimdall vs Nominatim
///
/// Runs a set of queries against both the local Heimdall server and
/// the public Nominatim API, comparing top-result coordinates.
///
/// Nominatim rate limit: 1 request/second (per their usage policy).

use std::path::Path;
use std::time::Duration;
use anyhow::Result;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Query set generation — sample diverse queries from Parquet
// ---------------------------------------------------------------------------

pub fn generate_queries(parquet_path: &Path, output_path: &Path, count: usize) -> Result<()> {
    use arrow::array::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rand::seq::SliceRandom;

    let file = std::fs::File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    // Collect all named places grouped by type
    let mut cities: Vec<String> = Vec::new();
    let mut towns: Vec<String> = Vec::new();
    let mut villages: Vec<String> = Vec::new();
    let mut lakes: Vec<String> = Vec::new();
    let mut rivers: Vec<String> = Vec::new();
    let mut islands: Vec<String> = Vec::new();
    let mut forests: Vec<String> = Vec::new();
    let mut other: Vec<String> = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let names = batch.column_by_name("name").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let place_types = batch.column_by_name("place_type").unwrap()
            .as_any().downcast_ref::<UInt8Array>().unwrap();

        for i in 0..batch.num_rows() {
            let name = names.value(i).to_owned();
            if name.is_empty() { continue; }

            match place_types.value(i) {
                3 => cities.push(name),       // City
                4 => towns.push(name),        // Town
                5 => villages.push(name),     // Village
                20 => lakes.push(name),       // Lake
                21 => rivers.push(name),      // River
                13 => islands.push(name),     // Island
                23 => forests.push(name),     // Forest
                255 => {}                     // Skip Unknown
                _ => other.push(name),
            }
        }
    }

    let mut rng = rand::thread_rng();

    // Build a balanced query set
    let mut queries: Vec<String> = Vec::new();

    // All cities (there are only ~32)
    queries.extend(cities.iter().cloned());

    // Sample from each category
    let sample = |pool: &mut Vec<String>, n: usize, rng: &mut rand::rngs::ThreadRng| -> Vec<String> {
        pool.shuffle(rng);
        pool.iter().take(n).cloned().collect()
    };

    queries.extend(sample(&mut towns, 30, &mut rng));
    queries.extend(sample(&mut villages, 30, &mut rng));
    queries.extend(sample(&mut lakes, 30, &mut rng));
    queries.extend(sample(&mut rivers, 20, &mut rng));
    queries.extend(sample(&mut islands, 15, &mut rng));
    queries.extend(sample(&mut forests, 15, &mut rng));
    queries.extend(sample(&mut other, 20, &mut rng));

    // Add known tricky queries
    let tricky = vec![
        // English names
        "Gothenburg", "Stockholm, Sweden", "Malmoe",
        // Typos
        "Stokholm", "Uppsla", "Kirruna",
        // Abbreviations
        "Gbg",
        // Diacritic-free
        "Goteborg", "Ostersund", "Norrkoping",
        // Ambiguous
        "Berg", "Näs", "Lund", "Vik",
    ];
    for t in tricky {
        if !queries.contains(&t.to_owned()) {
            queries.push(t.to_owned());
        }
    }

    // Deduplicate
    queries.sort();
    queries.dedup();

    // Cap at requested count
    if queries.len() > count {
        queries.shuffle(&mut rng);
        queries.truncate(count);
    }

    // Write to file
    let content = queries.join("\n");
    std::fs::write(output_path, &content)?;
    println!("Generated {} queries → {}", queries.len(), output_path.display());
    println!("  Cities: {}, Towns: {}, Villages: {}", cities.len(), towns.len(), villages.len());
    println!("  Lakes: {}, Rivers: {}, Islands: {}", lakes.len(), rivers.len(), islands.len());

    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmark runner
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct NominatimResult {
    lat: String,
    lon: String,
    display_name: Option<String>,
    #[serde(rename = "type")]
    place_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HeimdallResult {
    lat: String,
    lon: String,
    display_name: Option<String>,
    match_type: Option<String>,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    place_type: Option<String>,
}

struct BenchResult {
    query: String,
    heimdall_found: bool,
    nominatim_found: bool,
    heimdall_name: Option<String>,
    nominatim_name: Option<String>,
    distance_m: Option<f64>,
    match_type: Option<String>,
}

pub async fn run_benchmark(
    queries_path: &Path,
    heimdall_url: &str,
    output_path: Option<&Path>,
) -> Result<()> {
    let queries: Vec<String> = std::fs::read_to_string(queries_path)?
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.trim().to_owned())
        .collect();

    println!("Running benchmark: {} queries", queries.len());
    println!("Heimdall: {}", heimdall_url);
    println!("Nominatim: https://nominatim.openstreetmap.org");
    println!();

    let client = reqwest::Client::builder()
        .user_agent("Heimdall-Benchmark/0.1 (geocoder accuracy test)")
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut results: Vec<BenchResult> = Vec::new();
    let total = queries.len();

    for (i, query) in queries.iter().enumerate() {
        if (i + 1) % 25 == 0 || i == 0 {
            println!("[{}/{}] Processing...", i + 1, total);
        }

        // Query Heimdall (local, no rate limit needed)
        let heimdall = query_heimdall(&client, heimdall_url, query).await;

        // Query Nominatim (rate limited: 1 req/sec)
        let nominatim = query_nominatim(&client, query).await;

        // Compare
        let distance_m = match (&heimdall, &nominatim) {
            (Some((hlat, hlon, _, _)), Some((nlat, nlon, _, _))) => {
                Some(haversine_m(*hlat, *hlon, *nlat, *nlon))
            }
            _ => None,
        };

        results.push(BenchResult {
            query: query.clone(),
            heimdall_found: heimdall.is_some(),
            nominatim_found: nominatim.is_some(),
            heimdall_name: heimdall.as_ref().map(|h| h.2.clone()),
            nominatim_name: nominatim.as_ref().map(|n| n.2.clone()),
            distance_m,
            match_type: heimdall.as_ref().map(|h| h.3.clone()),
        });

        // Rate limit for Nominatim
        tokio::time::sleep(Duration::from_millis(1100)).await;
    }

    // Print report
    print_report(&results);

    // Write CSV if requested
    if let Some(path) = output_path {
        write_csv(&results, path)?;
        println!("\nDetailed results written to {}", path.display());
    }

    Ok(())
}

async fn query_heimdall(
    client: &reqwest::Client,
    base_url: &str,
    query: &str,
) -> Option<(f64, f64, String, String)> {
    let url = format!(
        "{}/search?q={}&format=json&limit=1&countrycodes=se",
        base_url,
        urlencoding(query),
    );

    let resp = client.get(&url).send().await.ok()?;
    let results: Vec<HeimdallResult> = resp.json().await.ok()?;
    let r = results.into_iter().next()?;

    let lat: f64 = r.lat.parse().ok()?;
    let lon: f64 = r.lon.parse().ok()?;
    let name = r.display_name.unwrap_or_default();
    let match_type = r.match_type.unwrap_or_else(|| "unknown".to_owned());

    Some((lat, lon, name, match_type))
}

async fn query_nominatim(
    client: &reqwest::Client,
    query: &str,
) -> Option<(f64, f64, String, String)> {
    let url = format!(
        "https://nominatim.openstreetmap.org/search?q={}&format=json&limit=1&countrycodes=se",
        urlencoding(query),
    );

    let resp = client.get(&url).send().await.ok()?;
    let results: Vec<NominatimResult> = resp.json().await.ok()?;
    let r = results.into_iter().next()?;

    let lat: f64 = r.lat.parse().ok()?;
    let lon: f64 = r.lon.parse().ok()?;
    let name = r.display_name.unwrap_or_default();
    let place_type = r.place_type.unwrap_or_default();

    Some((lat, lon, name, place_type))
}

fn urlencoding(s: &str) -> String {
    let mut result = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(b as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", b));
            }
        }
    }
    result
}

fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    r * 2.0 * a.sqrt().atan2((1.0 - a).sqrt())
}

fn print_report(results: &[BenchResult]) {
    let total = results.len();
    let heimdall_found = results.iter().filter(|r| r.heimdall_found).count();
    let nominatim_found = results.iter().filter(|r| r.nominatim_found).count();
    let both_found = results.iter().filter(|r| r.heimdall_found && r.nominatim_found).count();

    println!();
    println!("====================================================");
    println!("  HEIMDALL vs NOMINATIM — ACCURACY BENCHMARK");
    println!("====================================================");
    println!();
    println!("Total queries:        {}", total);
    println!("Heimdall found:       {} ({:.1}%)", heimdall_found, heimdall_found as f64 / total as f64 * 100.0);
    println!("Nominatim found:      {} ({:.1}%)", nominatim_found, nominatim_found as f64 / total as f64 * 100.0);
    println!("Both found:           {} ({:.1}%)", both_found, both_found as f64 / total as f64 * 100.0);
    println!();

    // Distance distribution
    let distances: Vec<f64> = results.iter()
        .filter_map(|r| r.distance_m)
        .collect();

    if !distances.is_empty() {
        let under_100m = distances.iter().filter(|d| **d < 100.0).count();
        let under_1km = distances.iter().filter(|d| **d >= 100.0 && **d < 1_000.0).count();
        let under_10km = distances.iter().filter(|d| **d >= 1_000.0 && **d < 10_000.0).count();
        let over_10km = distances.iter().filter(|d| **d >= 10_000.0).count();

        println!("Distance distribution (where both found, n={}):", distances.len());
        println!("  < 100m:     {:>4} ({:.1}%)   — essentially identical",
            under_100m, under_100m as f64 / distances.len() as f64 * 100.0);
        println!("  100m-1km:   {:>4} ({:.1}%)   — close enough",
            under_1km, under_1km as f64 / distances.len() as f64 * 100.0);
        println!("  1km-10km:   {:>4} ({:.1}%)   — wrong area, fixable",
            under_10km, under_10km as f64 / distances.len() as f64 * 100.0);
        println!("  > 10km:     {:>4} ({:.1}%)   — genuine misses",
            over_10km, over_10km as f64 / distances.len() as f64 * 100.0);

        // Median distance
        let mut sorted = distances.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];
        let p95 = sorted[(sorted.len() as f64 * 0.95) as usize];
        println!();
        println!("  Median distance:  {:.0}m", median);
        println!("  P95 distance:     {:.0}m", p95);
    }

    // Match type breakdown
    println!();
    println!("Heimdall match type breakdown:");
    let mut match_types: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for r in results {
        let mt = r.match_type.as_deref().unwrap_or("no result");
        *match_types.entry(mt.to_owned()).or_insert(0) += 1;
    }
    let mut mt_sorted: Vec<_> = match_types.into_iter().collect();
    mt_sorted.sort_by(|a, b| b.1.cmp(&a.1));
    for (mt, count) in &mt_sorted {
        println!("  {:15} {:>4}", mt, count);
    }

    // Heimdall misses (queries where Nominatim found but Heimdall didn't)
    let misses: Vec<&BenchResult> = results.iter()
        .filter(|r| !r.heimdall_found && r.nominatim_found)
        .collect();
    if !misses.is_empty() {
        println!();
        println!("Heimdall misses ({} queries Nominatim found but Heimdall didn't):", misses.len());
        for r in misses.iter().take(20) {
            println!("  \"{}\" → Nominatim: {}",
                r.query,
                r.nominatim_name.as_deref().unwrap_or("?"),
            );
        }
    }

    // Big distance outliers
    let outliers: Vec<&BenchResult> = results.iter()
        .filter(|r| r.distance_m.map(|d| d > 10_000.0).unwrap_or(false))
        .collect();
    if !outliers.is_empty() {
        println!();
        println!(">10km outliers (investigate these):");
        for r in outliers.iter().take(20) {
            println!("  \"{}\" → H: {} | N: {} | {:.0}km",
                r.query,
                r.heimdall_name.as_deref().unwrap_or("?"),
                r.nominatim_name.as_deref().unwrap_or("?"),
                r.distance_m.unwrap_or(0.0) / 1000.0,
            );
        }
    }
}

fn write_csv(results: &[BenchResult], path: &Path) -> Result<()> {
    let mut out = String::new();
    out.push_str("query,heimdall_found,nominatim_found,distance_m,match_type,heimdall_name,nominatim_name\n");

    for r in results {
        out.push_str(&format!(
            "\"{}\",{},{},{},{},\"{}\",\"{}\"\n",
            r.query.replace('"', "\"\""),
            r.heimdall_found,
            r.nominatim_found,
            r.distance_m.map(|d| format!("{:.1}", d)).unwrap_or_default(),
            r.match_type.as_deref().unwrap_or(""),
            r.heimdall_name.as_deref().unwrap_or("").replace('"', "\"\""),
            r.nominatim_name.as_deref().unwrap_or("").replace('"', "\"\""),
        ));
    }

    std::fs::write(path, out)?;
    Ok(())
}
