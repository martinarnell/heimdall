/// conflicts.rs — Browse and export conflict/diverge cases from results database.

use std::io::Write;
use std::path::Path;

use anyhow::Result;
use rusqlite::params;

use crate::db;
use crate::types::{format_num, truncate};

pub fn show_conflicts(
    db_path: &Path,
    country: Option<&str>,
    min_distance: f64,
    export_csv: Option<&Path>,
) -> Result<()> {
    if !db_path.exists() {
        eprintln!("No database found at {}. Run `run` first.", db_path.display());
        return Ok(());
    }

    let conn = db::open_db(db_path)?;

    // Build query with optional filters
    let mut sql = String::from(
        "SELECT query, query_id, query_type, country,
                heimdall_display, heimdall_lat, heimdall_lon,
                nominatim_display, nominatim_lat, nominatim_lon,
                distance_m, category
         FROM runs
         WHERE distance_m >= ?1
           AND category IN ('CONFLICT', 'DIVERGE')",
    );

    if country.is_some() {
        sql.push_str(" AND country = ?2");
    }
    sql.push_str(" ORDER BY distance_m DESC");

    let mut stmt = conn.prepare(&sql)?;

    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        Option<String>, String, String, Option<String>,
        Option<String>, Option<f64>, Option<f64>,
        Option<String>, Option<f64>, Option<f64>,
        Option<f64>, String,
    )> = if let Some(cc) = country {
        stmt.query_map(params![min_distance, cc.to_uppercase()], |r| {
            Ok((
                r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?,
                r.get(4)?, r.get(5)?, r.get(6)?,
                r.get(7)?, r.get(8)?, r.get(9)?,
                r.get(10)?, r.get(11)?,
            ))
        })?.filter_map(|r| r.ok()).collect()
    } else {
        stmt.query_map(params![min_distance], |r| {
            Ok((
                r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?,
                r.get(4)?, r.get(5)?, r.get(6)?,
                r.get(7)?, r.get(8)?, r.get(9)?,
                r.get(10)?, r.get(11)?,
            ))
        })?.filter_map(|r| r.ok()).collect()
    };

    if rows.is_empty() {
        println!(
            "No conflicts found with distance >= {:.0}m{}.",
            min_distance,
            country.map(|c| format!(" in {}", c.to_uppercase())).unwrap_or_default(),
        );
        return Ok(());
    }

    println!(
        "{} conflicts (>={:.0}m){}:\n",
        format_num(rows.len()),
        min_distance,
        country.map(|c| format!(" in {}", c.to_uppercase())).unwrap_or_default(),
    );

    // Console output
    for (i, row) in rows.iter().enumerate() {
        let (query, _id, qtype, cc, h_disp, h_lat, h_lon, n_disp, n_lat, n_lon, dist, cat) = row;
        let dist_str = dist
            .map(|d| {
                if d >= 1000.0 {
                    format!("{:.1}km", d / 1000.0)
                } else {
                    format!("{:.0}m", d)
                }
            })
            .unwrap_or_else(|| "-".into());

        let query_str = query.as_deref().unwrap_or("(reverse)");
        let cc_str = cc.as_deref().unwrap_or("--");

        println!(
            "#{} [{}] {} \"{}\"  {} {}",
            i + 1,
            cc_str,
            qtype,
            truncate(query_str, 50),
            cat,
            dist_str,
        );
        println!(
            "  Heimdall:  {} ({:.5}, {:.5})",
            h_disp.as_deref().unwrap_or("(no result)"),
            h_lat.unwrap_or(0.0),
            h_lon.unwrap_or(0.0),
        );
        println!(
            "  Nominatim: {} ({:.5}, {:.5})",
            n_disp.as_deref().unwrap_or("(no result)"),
            n_lat.unwrap_or(0.0),
            n_lon.unwrap_or(0.0),
        );
        println!();

        // Stop at 50 for console
        if i >= 49 && export_csv.is_none() {
            println!("... showing first 50 of {}. Use --export-csv for full list.", format_num(rows.len()));
            break;
        }
    }

    // CSV export
    if let Some(csv_path) = export_csv {
        let mut file = std::fs::File::create(csv_path)?;
        writeln!(
            file,
            "query,query_id,query_type,country,heimdall_result,heimdall_lat,heimdall_lon,nominatim_result,nominatim_lat,nominatim_lon,distance_m,category"
        )?;

        for row in &rows {
            let (query, id, qtype, cc, h_disp, h_lat, h_lon, n_disp, n_lat, n_lon, dist, cat) = row;
            writeln!(
                file,
                "\"{}\",\"{}\",{},{},\"{}\",{},{},\"{}\",{},{},{},{}",
                query.as_deref().unwrap_or("").replace('"', "\"\""),
                id,
                qtype,
                cc.as_deref().unwrap_or(""),
                h_disp.as_deref().unwrap_or("").replace('"', "\"\""),
                h_lat.map(|v| format!("{:.7}", v)).unwrap_or_default(),
                h_lon.map(|v| format!("{:.7}", v)).unwrap_or_default(),
                n_disp.as_deref().unwrap_or("").replace('"', "\"\""),
                n_lat.map(|v| format!("{:.7}", v)).unwrap_or_default(),
                n_lon.map(|v| format!("{:.7}", v)).unwrap_or_default(),
                dist.map(|v| format!("{:.1}", v)).unwrap_or_default(),
                cat,
            )?;
        }

        println!("Exported {} conflicts to {}", format_num(rows.len()), csv_path.display());
    }

    Ok(())
}
