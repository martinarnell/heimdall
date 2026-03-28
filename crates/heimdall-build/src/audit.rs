/// audit.rs — Parquet data quality checks
///
/// Reads the places.parquet and reports:
///   - Records outside Sweden bbox
///   - Records with zero coordinates
///   - Place type distribution
///   - Importance score distribution

use std::path::Path;
use anyhow::Result;
use arrow::array::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub fn audit_parquet(path: &Path) -> Result<()> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut total = 0usize;
    let mut zero_coords = 0usize;
    let mut outside_bbox = 0usize;
    let mut place_type_counts: [usize; 256] = [0; 256];
    let mut outside_examples: Vec<(String, f64, f64, u8)> = Vec::new();
    let mut zero_examples: Vec<(String, u8)> = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let n = batch.num_rows();

        let names = batch.column_by_name("name").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let lats = batch.column_by_name("lat").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        let lons = batch.column_by_name("lon").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        let place_types = batch.column_by_name("place_type").unwrap()
            .as_any().downcast_ref::<UInt8Array>().unwrap();

        for i in 0..n {
            total += 1;
            let lat = lats.value(i);
            let lon = lons.value(i);
            let pt = place_types.value(i);
            let name = names.value(i);

            place_type_counts[pt as usize] += 1;

            if lat == 0.0 && lon == 0.0 {
                zero_coords += 1;
                if zero_examples.len() < 10 {
                    zero_examples.push((name.to_owned(), pt));
                }
            } else if !(lat >= 55.0 && lat <= 69.5 && lon >= 10.5 && lon <= 24.5) {
                outside_bbox += 1;
                if outside_examples.len() < 20 {
                    outside_examples.push((name.to_owned(), lat, lon, pt));
                }
            }
        }
    }

    println!("=== Parquet Audit ===");
    println!("Total records:       {}", total);
    println!("Zero coordinates:    {}", zero_coords);
    println!("Outside Sweden bbox: {}", outside_bbox);
    println!();

    if !zero_examples.is_empty() {
        println!("Zero-coord examples:");
        for (name, pt) in &zero_examples {
            println!("  {} (type={})", name, place_type_name(*pt));
        }
        println!();
    }

    if !outside_examples.is_empty() {
        println!("Outside-bbox examples:");
        for (name, lat, lon, pt) in &outside_examples {
            println!("  {} ({:.4}, {:.4}) type={}", name, lat, lon, place_type_name(*pt));
        }
        println!();
    }

    println!("Place type distribution:");
    let type_names = [
        (0, "Country"), (1, "State"), (2, "County"), (3, "City"),
        (4, "Town"), (5, "Village"), (6, "Hamlet"), (7, "Farm"),
        (8, "Locality"), (10, "Suburb"), (11, "Quarter"), (12, "Neighbourhood"),
        (13, "Island"), (14, "Islet"), (20, "Lake"), (21, "River"),
        (22, "Mountain"), (23, "Forest"), (24, "Bay"), (25, "Cape"),
        (30, "Airport"), (31, "Station"), (255, "Unknown"),
    ];
    for (id, label) in &type_names {
        let count = place_type_counts[*id as usize];
        if count > 0 {
            println!("  {:15} {:>8}", label, count);
        }
    }

    Ok(())
}

fn place_type_name(pt: u8) -> &'static str {
    match pt {
        0 => "Country", 1 => "State", 2 => "County", 3 => "City",
        4 => "Town", 5 => "Village", 6 => "Hamlet", 7 => "Farm",
        8 => "Locality", 10 => "Suburb", 11 => "Quarter",
        12 => "Neighbourhood", 13 => "Island", 14 => "Islet",
        20 => "Lake", 21 => "River", 22 => "Mountain", 23 => "Forest",
        24 => "Bay", 25 => "Cape", 30 => "Airport", 31 => "Station",
        255 => "Unknown", _ => "Other",
    }
}
