/// runner.rs — Batch benchmark runner: reads JSONL, queries both geocoders,
/// writes results to SQLite. Resumable on restart.

use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::Result;

use crate::db;
use crate::http::GeocoderClient;
use crate::types::{categorize, categorize_ambiguous, format_num, truncate, Category, QueryEntry};

pub async fn run_benchmark(
    queries_path: &Path,
    heimdall_url: &str,
    nominatim_url: &str,
    rps: f64,
    output_path: &Path,
) -> Result<()> {
    // Open/create SQLite
    let conn = db::open_db(output_path)?;

    // Load completed IDs
    let completed = db::get_completed_ids(&conn)?;
    eprintln!("{} queries already completed in {}", completed.len(), output_path.display());

    // Count total queries in file (skip metadata line)
    let file = std::fs::File::open(queries_path)?;
    let reader = BufReader::new(file);
    let mut total_queries = 0;
    let mut first_line = true;
    for line in reader.lines() {
        let line = line?;
        if first_line {
            first_line = false;
            if line.contains("\"_meta\"") {
                continue; // Skip metadata header
            }
        }
        if !line.trim().is_empty() {
            total_queries += 1;
        }
    }

    let remaining = total_queries - completed.len();
    eprintln!(
        "{} total queries, {} remaining",
        format_num(total_queries),
        format_num(remaining),
    );

    if remaining == 0 {
        eprintln!("All queries completed! Run `report` to see results.");
        return Ok(());
    }

    // Create HTTP client
    let mut client = GeocoderClient::new(heimdall_url, nominatim_url, rps)?;

    // Track running stats
    let mut agree_count: usize = db::count_category(&conn, "AGREE").unwrap_or(0);
    let mut total_queried: usize = completed.len();

    // Stream queries
    let file = std::fs::File::open(queries_path)?;
    let reader = BufReader::new(file);
    let mut first_line = true;

    for line in reader.lines() {
        let line = line?;
        if first_line {
            first_line = false;
            if line.contains("\"_meta\"") {
                continue;
            }
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let entry: QueryEntry = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(err) => {
                eprintln!("Skipping malformed line: {}", err);
                continue;
            }
        };

        // Skip if already completed
        if completed.contains(&entry.id) {
            continue;
        }

        // Dispatch based on category
        let (h, n) = match entry.category.as_str() {
            "reverse" => {
                let lat = entry.lat.unwrap_or(0.0);
                let lon = entry.lon.unwrap_or(0.0);
                let h = client.reverse_heimdall(lat, lon).await;
                let n = client.reverse_nominatim(lat, lon).await;
                (h, n)
            }
            _ => {
                let q = match &entry.q {
                    Some(q) => q.as_str(),
                    None => {
                        eprintln!("Skipping entry {} with no query string", entry.id);
                        continue;
                    }
                };

                // Ambiguous queries: no country filter
                let country = if entry.category == "ambiguous" {
                    None
                } else {
                    entry.country.as_deref()
                };

                let h = client.search_heimdall(q, country).await;
                let n = client.search_nominatim(q, country).await;
                (h, n)
            }
        };

        // Categorize
        let (category, distance) = if h.is_error {
            (Category::HeimdallError, None)
        } else if n.is_error {
            (Category::NominatimError, None)
        } else {
            let h_coord = h.lat.zip(h.lon);
            let n_coord = n.lat.zip(n.lon);
            if entry.category == "ambiguous" {
                categorize_ambiguous(h_coord, n_coord)
            } else {
                categorize(h_coord, n_coord)
            }
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

        let cat_str = category.as_str();

        // Insert into DB
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

        // Progress line
        let dist_str = distance
            .map(|d| format!("{:.0}m", d))
            .unwrap_or_else(|| "-".into());
        let query_short = entry
            .q
            .as_deref()
            .map(|q| truncate(q, 35))
            .unwrap_or_else(|| {
                format!(
                    "reverse({:.4},{:.4})",
                    entry.lat.unwrap_or(0.0),
                    entry.lon.unwrap_or(0.0)
                )
            });
        let cc = entry.country.as_deref().unwrap_or("--");

        println!(
            "[{}/{}] {} {} \"{}\" -> {} {}  ({:.1}% agree)",
            format_num(total_queried),
            format_num(total_queries),
            cc,
            &entry.category[..entry.category.len().min(5)],
            query_short,
            cat_str,
            dist_str,
            agree_pct,
        );

        // Summary every 1000 queries
        if total_queried % 1000 == 0 {
            println!(
                "\n--- {} queries completed ({:.1}% agree) ---\n",
                format_num(total_queried),
                agree_pct,
            );
        }
    }

    println!(
        "\nBenchmark complete! {} queries total. Run `report` for detailed stats.",
        format_num(total_queried),
    );
    Ok(())
}
