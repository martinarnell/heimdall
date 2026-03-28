/// report.rs — Generate benchmark reports from results database.
/// Outputs to console or markdown file.

use std::io::Write;
use std::path::Path;

use anyhow::Result;
use rusqlite::params;

use crate::db;
use crate::types::{format_num, percentile, Category};

pub fn generate_report(db_path: &Path, output_path: Option<&Path>) -> Result<()> {
    if !db_path.exists() {
        eprintln!("No database found at {}. Run `run` first.", db_path.display());
        return Ok(());
    }

    let conn = db::open_db(db_path)?;
    let total = db::count_total(&conn)?;
    if total == 0 {
        eprintln!("No data yet. Run `run` first.");
        return Ok(());
    }

    let mut buf = String::new();

    // Determine output mode
    let is_markdown = output_path.is_some();

    // Header
    let countries = db::distinct_countries(&conn)?;
    let agree_total = db::count_category(&conn, "AGREE")?;

    // Timestamps for accumulation rate
    let first_queried: Option<String> = conn
        .query_row("SELECT MIN(queried_at) FROM runs", [], |r| r.get(0))
        .ok();
    let last_queried: Option<String> = conn
        .query_row("SELECT MAX(queried_at) FROM runs", [], |r| r.get(0))
        .ok();

    if is_markdown {
        buf.push_str("# Heimdall vs Nominatim — Accuracy Report\n\n");
        buf.push_str(&format!(
            "**{} queries** across **{} countries**\n\n",
            format_num(total),
            countries.len(),
        ));
        if let (Some(ref first), Some(ref last)) = (&first_queried, &last_queried) {
            buf.push_str(&format!(
                "Period: {} to {}\n\n",
                &first[..10.min(first.len())],
                &last[..10.min(last.len())],
            ));
        }
    } else {
        buf.push('\n');
        if let Some(ref first) = first_queried {
            buf.push_str(&format!(
                "Validated against {} Nominatim queries across {} countries (since {})\n",
                format_num(total),
                countries.len(),
                &first[..10.min(first.len())],
            ));
        }
        // Accumulation rate
        if let (Some(ref first), Some(ref last)) = (&first_queried, &last_queried) {
            if let (Ok(first_dt), Ok(last_dt)) = (
                chrono::NaiveDateTime::parse_from_str(first, "%Y-%m-%d %H:%M:%S"),
                chrono::NaiveDateTime::parse_from_str(last, "%Y-%m-%d %H:%M:%S"),
            ) {
                let elapsed_secs = (last_dt - first_dt).num_seconds().max(1);
                let rate = total as f64 / elapsed_secs as f64;
                let rate_str = if rate >= 0.8 {
                    format!("{:.1} rps", rate)
                } else {
                    format!("{:.0} queries/hour", rate * 3600.0)
                };
                buf.push_str(&format!("Accumulating at ~{}\n", rate_str));
            }
        }
        buf.push_str(&format!("{}\n", "━".repeat(60)));
    }

    // Overall agreement
    let agree_pct = agree_total as f64 / total as f64 * 100.0;
    if is_markdown {
        buf.push_str(&format!(
            "## Overall Agreement: {:.1}% (<200m)\n\n",
            agree_pct,
        ));
    } else {
        buf.push_str(&format!(
            "Overall agreement rate: {:.1}% (<200m)\n\n",
            agree_pct,
        ));
    }

    // Category breakdown
    if is_markdown {
        buf.push_str("## Results by Category\n\n");
        buf.push_str("| Category | Count | % |\n");
        buf.push_str("|----------|------:|---:|\n");
    } else {
        buf.push_str("By category:\n");
    }

    for &cat in Category::ALL {
        let count = db::count_category(&conn, cat)?;
        if count == 0 {
            continue;
        }
        let pct = count as f64 / total as f64 * 100.0;

        if is_markdown {
            buf.push_str(&format!(
                "| {} | {} | {:.1}% |\n",
                cat,
                format_num(count),
                pct,
            ));
        } else {
            let bar_len = (pct / 5.0).round() as usize;
            let bar: String = "\u{2588}".repeat(bar_len);
            buf.push_str(&format!(
                "  {:<17} {:>7}  ({:>5.1}%)  {}\n",
                cat,
                format_num(count),
                pct,
                bar,
            ));
        }
    }

    // By country
    if is_markdown {
        buf.push_str("\n## Results by Country\n\n");
        buf.push_str("| Country | Queries | Agree % | MISS_H % |\n");
        buf.push_str("|---------|--------:|--------:|---------:|\n");
    } else {
        buf.push_str("\nBy country:\n");
    }

    for country in &countries {
        let count = db::count_country(&conn, country)?;
        let agree = db::count_country_category(&conn, country, "AGREE")?;
        let miss_h = db::count_country_category(&conn, country, "MISS_H")?;
        let agree_pct = if count > 0 { agree as f64 / count as f64 * 100.0 } else { 0.0 };
        let miss_pct = if count > 0 { miss_h as f64 / count as f64 * 100.0 } else { 0.0 };

        if is_markdown {
            buf.push_str(&format!(
                "| {} | {} | {:.1}% | {:.1}% |\n",
                country,
                format_num(count),
                agree_pct,
                miss_pct,
            ));
        } else {
            buf.push_str(&format!(
                "  {}  {:>7} queries  -- {:.1}% agree  {:.1}% MISS_H\n",
                country,
                format_num(count),
                agree_pct,
                miss_pct,
            ));
        }
    }

    // By query type
    if is_markdown {
        buf.push_str("\n## Results by Query Type\n\n");
        buf.push_str("| Type | Queries | Agree % |\n");
        buf.push_str("|------|--------:|--------:|\n");
    } else {
        buf.push_str("\nBy query type:\n");
    }

    for qt in &["address", "place", "fuzzy", "reverse", "ambiguous"] {
        let count: usize = conn.query_row(
            "SELECT COUNT(*) FROM runs WHERE query_type = ?1",
            params![qt],
            |r| r.get(0),
        )?;
        if count == 0 {
            continue;
        }
        let agree: usize = conn.query_row(
            "SELECT COUNT(*) FROM runs WHERE query_type = ?1 AND category = 'AGREE'",
            params![qt],
            |r| r.get(0),
        )?;
        let pct = if count > 0 { agree as f64 / count as f64 * 100.0 } else { 0.0 };

        if is_markdown {
            buf.push_str(&format!(
                "| {} | {} | {:.1}% |\n",
                qt,
                format_num(count),
                pct,
            ));
        } else {
            buf.push_str(&format!(
                "  {:<10} {:>7}  -- {:.1}% agree\n",
                qt,
                format_num(count),
                pct,
            ));
        }
    }

    // Variant resolution analysis
    let variant_count: usize = conn.query_row(
        "SELECT COUNT(*) FROM runs WHERE variant_of IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    if variant_count > 0 {
        if is_markdown {
            buf.push_str("\n## Variant Resolution\n\n");
            buf.push_str("| Variant Type | Count | Agree % |\n");
            buf.push_str("|-------------|------:|--------:|\n");
        } else {
            buf.push_str("\nVariant resolution:\n");
        }

        for vt in &["diacritic_free", "english_alias"] {
            let vcount: usize = conn.query_row(
                "SELECT COUNT(*) FROM runs WHERE variant_type = ?1",
                params![vt],
                |r| r.get(0),
            )?;
            if vcount == 0 {
                continue;
            }
            let vagree: usize = conn.query_row(
                "SELECT COUNT(*) FROM runs WHERE variant_type = ?1 AND category = 'AGREE'",
                params![vt],
                |r| r.get(0),
            )?;
            let vpct = if vcount > 0 { vagree as f64 / vcount as f64 * 100.0 } else { 0.0 };

            if is_markdown {
                buf.push_str(&format!(
                    "| {} | {} | {:.1}% |\n",
                    vt, format_num(vcount), vpct,
                ));
            } else {
                buf.push_str(&format!(
                    "  {:<17} {:>5}  -- {:.1}% agree\n",
                    vt, format_num(vcount), vpct,
                ));
            }
        }
    }

    // Distance percentiles
    let distances = db::all_distances_sorted(&conn)?;
    if !distances.is_empty() {
        let median = percentile(&distances, 50.0);
        let p90 = percentile(&distances, 90.0);
        let p99 = percentile(&distances, 99.0);

        if is_markdown {
            buf.push_str("\n## Distance Percentiles\n\n");
            buf.push_str(&format!("- Median: {:.0}m\n", median));
            buf.push_str(&format!("- P90: {:.0}m\n", p90));
            buf.push_str(&format!("- P99: {:.0}m\n", p99));
        } else {
            buf.push_str(&format!("\nMedian distance: {:.0}m\n", median));
            buf.push_str(&format!("p90 distance:    {:.0}m\n", p90));
            buf.push_str(&format!("p99 distance:    {:.0}m\n", p99));
        }
    }

    // Trend: last 1000 vs overall
    let recent_agree: usize = conn.query_row(
        "SELECT COUNT(*) FROM (
            SELECT category FROM runs ORDER BY id DESC LIMIT 1000
        ) WHERE category = 'AGREE'",
        [],
        |r| r.get(0),
    )?;
    let recent_total: usize = conn.query_row(
        "SELECT COUNT(*) FROM (SELECT id FROM runs ORDER BY id DESC LIMIT 1000)",
        [],
        |r| r.get(0),
    )?;

    if recent_total > 0 {
        let recent_pct = recent_agree as f64 / recent_total as f64 * 100.0;
        let overall_pct = agree_total as f64 / total as f64 * 100.0;
        let trend = if (recent_pct - overall_pct).abs() < 0.5 {
            "stable"
        } else if recent_pct > overall_pct {
            "improving"
        } else {
            "declining"
        };

        if is_markdown {
            buf.push_str(&format!(
                "\n**Recent 1K trend:** {:.1}% agree ({})\n",
                recent_pct, trend,
            ));
        } else {
            buf.push_str(&format!(
                "\nRecent 1K trend: {:.1}% agree ({})\n",
                recent_pct, trend,
            ));
        }
    }

    // MISS_H attention flags
    let mut attention = Vec::new();
    for country in &countries {
        let count = db::count_country(&conn, country)?;
        let miss_h = db::count_country_category(&conn, country, "MISS_H")?;
        if count > 0 && (miss_h as f64 / count as f64) > 0.02 {
            attention.push(format!(
                "  {} -- {:.1}% MISS_H ({}/{})",
                country,
                miss_h as f64 / count as f64 * 100.0,
                miss_h,
                count,
            ));
        }
    }

    if !attention.is_empty() {
        if is_markdown {
            buf.push_str("\n## Attention: MISS_H > 2%\n\n");
        } else {
            buf.push_str("\nATTENTION -- MISS_H > 2%:\n");
        }
        for line in &attention {
            buf.push_str(line);
            buf.push('\n');
        }
    }

    // Top conflicts
    let mut stmt = conn.prepare(
        "SELECT query, query_id, country, heimdall_display, nominatim_display, distance_m
         FROM runs WHERE category = 'CONFLICT'
         ORDER BY distance_m DESC LIMIT 20",
    )?;
    #[allow(clippy::type_complexity)]
    let conflicts: Vec<(Option<String>, String, Option<String>, Option<String>, Option<String>, Option<f64>)> = stmt
        .query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    if !conflicts.is_empty() {
        if is_markdown {
            buf.push_str(&format!(
                "\n## Top {} Conflicts (>20km)\n\n",
                conflicts.len(),
            ));
        } else {
            buf.push_str(&format!(
                "\nTop {} CONFLICT cases:\n",
                conflicts.len(),
            ));
        }

        for (i, (query, _id, country, h_disp, n_disp, dist)) in conflicts.iter().enumerate() {
            let dist_str = dist
                .map(|d| format!("{:.0}km", d / 1000.0))
                .unwrap_or_else(|| "-".into());
            let query_str = query.as_deref().unwrap_or("(reverse)");
            let cc = country.as_deref().unwrap_or("--");

            if is_markdown {
                buf.push_str(&format!(
                    "{}. **[{}] \"{}\"** -- {}\n",
                    i + 1, cc, query_str, dist_str,
                ));
                buf.push_str(&format!(
                    "   - Heimdall: {}\n",
                    h_disp.as_deref().unwrap_or("(no result)"),
                ));
                buf.push_str(&format!(
                    "   - Nominatim: {}\n\n",
                    n_disp.as_deref().unwrap_or("(no result)"),
                ));
            } else {
                buf.push_str(&format!(
                    "  {}. [{}] \"{}\" -- {} apart\n",
                    i + 1,
                    cc,
                    crate::types::truncate(query_str, 50),
                    dist_str,
                ));
                buf.push_str(&format!(
                    "     Heimdall:  {}\n",
                    h_disp.as_deref().unwrap_or("(no result)"),
                ));
                buf.push_str(&format!(
                    "     Nominatim: {}\n",
                    n_disp.as_deref().unwrap_or("(no result)"),
                ));
            }
        }
    }

    buf.push('\n');

    // Output
    if let Some(path) = output_path {
        let mut file = std::fs::File::create(path)?;
        file.write_all(buf.as_bytes())?;
        eprintln!("Report written to {}", path.display());
    } else {
        print!("{}", buf);
    }

    Ok(())
}
