/// pack_addr.rs — Build street-grouped address index
///
/// Groups addresses by (normalized_street, municipality_id), stores
/// house numbers as delta-encoded coordinates within each street.
///
/// FST key: {normalized_street}:{municipality_id} → street_record_id
/// Also wildcard: {normalized_street}:0 → street_record_id (for no-city queries)

use std::collections::HashMap;
use std::path::Path;
use anyhow::Result;
use arrow::array::*;
use fst::MapBuilder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::info;

use heimdall_core::addr_store::AddrStoreBuilder;
use heimdall_normalize::Normalizer;

pub struct AddrPackStats {
    pub address_count: usize,
    pub street_count: usize,
    pub fst_bytes: usize,
    pub record_bytes: usize,
}

/// A collected house number for grouping
struct HouseNum {
    number: u16,
    suffix: u8,
    lat: i32, // microdegrees
    lon: i32,
}

/// Key for grouping: normalized_street + municipality_id
#[derive(Hash, Eq, PartialEq, Clone)]
struct StreetKey {
    norm_street: String,
    muni_id: u16,
}

/// A grouped street with all its house numbers
struct StreetGroup {
    display_name: String, // original casing
    muni_id: u16,
    postcode: u16,
    houses: Vec<HouseNum>,
}

pub fn pack_addresses(
    parquet_paths: &[&Path],
    output_dir: &Path,
    admin_map_path: &Path,
    normalizer: &Normalizer,
) -> Result<AddrPackStats> {
    let admin_map: HashMap<i64, (u16, u16)> = if admin_map_path.exists() {
        let bytes = std::fs::read(admin_map_path)?;
        bincode::deserialize(&bytes)?
    } else {
        HashMap::new()
    };

    let existing_paths: Vec<&&Path> = parquet_paths.iter().filter(|p| p.exists()).collect();
    if existing_paths.is_empty() {
        return Ok(AddrPackStats { address_count: 0, street_count: 0, fst_bytes: 0, record_bytes: 0 });
    }

    for p in &existing_paths {
        info!("Reading addresses from {}", p.display());
    }

    // -----------------------------------------------------------------------
    // Step 1: Group addresses by (normalized_street, municipality_id)
    // -----------------------------------------------------------------------
    let mut groups: HashMap<StreetKey, StreetGroup> = HashMap::new();
    let mut total_addresses = 0usize;
    let mut postcode_coords: HashMap<String, Vec<(i32, i32)>> = HashMap::new();
    // Per-postcode city tally so the postcode response can render
    // "75001 Paris, France" instead of just the bare digits. We pick
    // the most common city across all addresses sharing that postcode —
    // robust to OSM-tagged outlier addresses that mention a neighbouring
    // commune. Counts are kept tight (max ~8 distinct cities per postcode
    // is typical) so memory stays modest even on large countries.
    let mut postcode_cities: HashMap<String, HashMap<String, u32>> = HashMap::new();

  for parquet_path in &existing_paths {
    let file = std::fs::File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    for batch_result in reader {
        let batch = batch_result?;
        let n = batch.num_rows();

        let osm_ids = batch.column_by_name("osm_id").unwrap()
            .as_any().downcast_ref::<Int64Array>().unwrap();
        let streets = batch.column_by_name("street").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let housenumbers = batch.column_by_name("housenumber").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let lats = batch.column_by_name("lat").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        let lons = batch.column_by_name("lon").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        let postcodes_col = batch.column_by_name("postcode").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let cities_col = batch.column_by_name("city").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..n {
            let osm_id = osm_ids.value(i);
            let street = streets.value(i);
            let hn_str = housenumbers.value(i);
            let lat = lats.value(i);
            let lon = lons.value(i);

            if street.is_empty() || hn_str.is_empty() { continue; }

            let (_admin1_id, admin2_id) = admin_map.get(&osm_id).copied().unwrap_or((0, 0));

            // If admin_map didn't resolve a municipality, fall back to hashing the
            // city name. This is essential for non-OSM sources (OpenAddresses, etc.)
            // where osm_id=0 but city is known from the source data.
            let admin2_id = if admin2_id == 0 && !cities_col.is_null(i) {
                let city = cities_col.value(i).trim();
                if !city.is_empty() {
                    city_name_to_muni_id(city)
                } else {
                    0
                }
            } else {
                admin2_id
            };

            // Parse house number: "15B" → (15, 'B'), "4-6" → (4, 0)
            let (number, suffix) = parse_housenumber(hn_str);
            if number == 0 { continue; }

            let lat_micro = (lat * 1_000_000.0) as i32;
            let lon_micro = (lon * 1_000_000.0) as i32;

            let norm_street = normalizer.normalize(street)
                .into_iter().next().unwrap_or_default();
            if norm_street.is_empty() { continue; }

            let postcode: u16 = if postcodes_col.is_null(i) {
                0
            } else {
                postcodes_col.value(i).trim().parse().unwrap_or(0)
            };

            let key = StreetKey { norm_street: norm_street.clone(), muni_id: admin2_id };

            let group = groups.entry(key).or_insert_with(|| StreetGroup {
                display_name: street.to_owned(),
                muni_id: admin2_id,
                postcode,
                houses: Vec::new(),
            });

            group.houses.push(HouseNum {
                number,
                suffix,
                lat: lat_micro,
                lon: lon_micro,
            });

            // Collect postcode data for centroid index (avoids second parquet read)
            if !postcodes_col.is_null(i) {
                let pc = postcodes_col.value(i).trim();
                if !pc.is_empty() {
                    let norm_pc: String = pc.chars()
                        .filter(|c| !c.is_whitespace())
                        .flat_map(|c| c.to_lowercase())
                        .collect();
                    if norm_pc.len() >= 3 {
                        postcode_coords.entry(norm_pc.clone()).or_default().push((lat_micro, lon_micro));
                        if !cities_col.is_null(i) {
                            let city = cities_col.value(i).trim();
                            if !city.is_empty() {
                                *postcode_cities
                                    .entry(norm_pc)
                                    .or_default()
                                    .entry(city.to_owned())
                                    .or_insert(0) += 1;
                            }
                        }
                    }
                }
            }

            total_addresses += 1;
        }
    }
  } // end for parquet_path

    info!("Grouped {} addresses into {} street segments", total_addresses, groups.len());

    // -----------------------------------------------------------------------
    // Step 2: Build the street-grouped store
    // -----------------------------------------------------------------------
    // Sort groups by median latitude for better delta-of-delta compression
    let mut sorted_groups: Vec<(StreetKey, StreetGroup)> = groups.into_iter().collect();
    sorted_groups.sort_by(|a, b| {
        let lat_a = a.1.houses.get(a.1.houses.len() / 2).map(|h| h.lat).unwrap_or(0);
        let lat_b = b.1.houses.get(b.1.houses.len() / 2).map(|h| h.lat).unwrap_or(0);
        lat_a.cmp(&lat_b)
    });

    let mut store_builder = AddrStoreBuilder::new();
    let mut fst_keys: Vec<(String, u32)> = Vec::new();
    let street_count = sorted_groups.len();

    for (key, mut group) in sorted_groups {
        if group.houses.is_empty() { continue; }
        if group.display_name.len() >= 255 { continue; }

        // Sort houses by number for binary search at query time
        group.houses.sort_by_key(|h| (h.number, h.suffix));

        // Use median coordinate as base (minimizes delta range)
        let mid = group.houses.len() / 2;
        let base_lat = group.houses[mid].lat;
        let base_lon = group.houses[mid].lon;

        let house_entries: Vec<(u16, u8, i32, i32)> = group.houses.iter()
            .map(|h| (h.number, h.suffix, h.lat, h.lon))
            .collect();

        let street_id = store_builder.add_street(
            &group.display_name,
            base_lat,
            base_lon,
            group.postcode,
            &house_entries,
        );

        // Municipality-specific key
        let fst_key = format!("{}:{}", key.norm_street, group.muni_id);
        fst_keys.push((fst_key, street_id));

        // Wildcard key (muni_id=0) — for no-city queries
        let wildcard_key = format!("{}:0", key.norm_street);
        fst_keys.push((wildcard_key, street_id));
    }

    // Write the street-grouped store
    let store_path = output_dir.join("addr_streets.bin");
    let record_bytes = store_builder.write(&store_path)?;
    info!(
        "Street store: {:.1} MB ({} streets, {} houses)",
        record_bytes as f64 / 1e6,
        street_count,
        total_addresses,
    );

    // Build FST (sort + dedup)
    fst_keys.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    fst_keys.dedup_by(|a, b| {
        if a.0 == b.0 { true } else { false }
    });

    let fst_path = output_dir.join("fst_addr.fst");
    let fst_file = std::io::BufWriter::new(std::fs::File::create(&fst_path)?);
    let mut fst_builder = MapBuilder::new(fst_file)?;
    for (key, id) in &fst_keys {
        fst_builder.insert(key.as_bytes(), *id as u64)?;
    }
    fst_builder.finish()?;
    let fst_bytes = std::fs::metadata(&fst_path)?.len() as usize;
    info!("Address FST: {:.1} MB ({} keys)", fst_bytes as f64 / 1e6, fst_keys.len());
    heimdall_core::compressed_io::compress_file(&fst_path, 19)?;

    // -----------------------------------------------------------------------
    // Step 3: Build postcode index (data already collected during step 1)
    // -----------------------------------------------------------------------
    let postcode_count = postcode_coords.len();
    if postcode_count > 0 {
        // Build postcode centroids + FST
        let mut pc_entries: Vec<(String, i32, i32, String)> = postcode_coords.into_iter()
            .map(|(norm_pc, coords)| {
                let n = coords.len() as i64;
                let avg_lat = (coords.iter().map(|c| c.0 as i64).sum::<i64>() / n) as i32;
                let avg_lon = (coords.iter().map(|c| c.1 as i64).sum::<i64>() / n) as i32;
                // Postcode display = "{canonical_postcode}[ {dominant_city}]".
                // UK postcodes are split before the last 3 chars
                // ("SW1A2AA" → "SW1A 2AA"); every other country uses the
                // canonical compact form (French "75001", German "10115").
                let canonical = if heimdall_core::addr_index::is_uk_postcode(&norm_pc) {
                    let upper = norm_pc.to_uppercase();
                    let split = upper.len() - 3;
                    format!("{} {}", &upper[..split], &upper[split..])
                } else {
                    norm_pc.to_uppercase()
                };
                // Pick the most-common city seen on addresses with this
                // postcode so the bare-postcode response renders something
                // human ("75001 Paris", "10115 Berlin"). Tie-broken by
                // alphabetical order for determinism. Capped at 64 chars
                // — no real-world city name is longer, but defensive in
                // case a malformed feed drops a long string.
                let dominant_city = postcode_cities.get(&norm_pc).and_then(|tally| {
                    tally.iter()
                        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
                        .map(|(name, _)| name.clone())
                });
                let display = match dominant_city {
                    Some(city) if city.len() <= 64 => format!("{} {}", canonical, city),
                    _ => canonical,
                };
                (norm_pc, avg_lat, avg_lon, display)
            })
            .collect();
        pc_entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

        // Write postcode_centroids.bin: [u32 count][entries: lat_i32, lon_i32, display_len_u8, display_bytes]
        let pc_path = output_dir.join("postcode_centroids.bin");
        {
            use std::io::Write;
            let mut f = std::io::BufWriter::new(std::fs::File::create(&pc_path)?);
            f.write_all(&(pc_entries.len() as u32).to_le_bytes())?;
            for (_, lat, lon, display) in &pc_entries {
                f.write_all(&lat.to_le_bytes())?;
                f.write_all(&lon.to_le_bytes())?;
                let db = display.as_bytes();
                f.write_all(&[db.len() as u8])?;
                f.write_all(db)?;
            }
        }

        // Build FST: normalized_postcode → index into centroids array
        let pc_fst_path = output_dir.join("fst_postcode.fst");
        let pc_fst_file = std::io::BufWriter::new(std::fs::File::create(&pc_fst_path)?);
        let mut pc_fst_builder = MapBuilder::new(pc_fst_file)?;
        for (i, (norm_pc, _, _, _)) in pc_entries.iter().enumerate() {
            pc_fst_builder.insert(norm_pc.as_bytes(), i as u64)?;
        }
        pc_fst_builder.finish()?;
        let pc_fst_bytes = std::fs::metadata(&pc_fst_path)?.len() as usize;
        heimdall_core::compressed_io::compress_file(&pc_fst_path, 19)?;
        let pc_path = output_dir.join("postcode_centroids.bin");
        if pc_path.exists() {
            heimdall_core::compressed_io::compress_file(&pc_path, 19)?;
        }
        info!("Postcode index: {} postcodes, FST {:.1} MB", postcode_count, pc_fst_bytes as f64 / 1e6);
    }

    Ok(AddrPackStats {
        address_count: total_addresses,
        street_count,
        fst_bytes,
        record_bytes,
    })
}

/// Re-export city hash from core for consistency.
fn city_name_to_muni_id(city: &str) -> u16 {
    heimdall_core::addr_index::city_name_to_muni_id(city)
}

/// Parse "15B" → (15, 2), "4-6" → (4, 0), "123" → (123, 0)
fn parse_housenumber(s: &str) -> (u16, u8) {
    let s = s.trim();
    // Take first number-like part
    let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if num_end == 0 { return (0, 0); }

    let number: u16 = s[..num_end].parse().unwrap_or(0);

    // Check for letter suffix immediately after number
    let suffix = if num_end < s.len() {
        let c = s.as_bytes()[num_end];
        if c.is_ascii_alphabetic() {
            c.to_ascii_uppercase() - b'A' + 1 // A=1, B=2, etc.
        } else {
            0
        }
    } else {
        0
    };

    (number, suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_housenumber() {
        assert_eq!(parse_housenumber("10"), (10, 0));
        assert_eq!(parse_housenumber("15B"), (15, 2)); // B=2
        assert_eq!(parse_housenumber("4-6"), (4, 0));
        assert_eq!(parse_housenumber("1A"), (1, 1));
        assert_eq!(parse_housenumber(""), (0, 0));
    }
}
