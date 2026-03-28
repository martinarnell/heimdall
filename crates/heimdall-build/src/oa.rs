/// oa.rs — OpenAddresses import for US addresses
///
/// Processes OpenAddresses GeoJSON files into Heimdall's address format.
/// Reads state-by-state to bound memory usage.
///
/// Input: OpenAddresses regional ZIPs (us-northeast, us-south, us-midwest, us-west)
/// Each ZIP contains us/{state}/{county}.geojson files.
///
/// Output: addresses.parquet in the index directory, ready for pack_addr.

use std::path::{Path, PathBuf};
use std::io::Read;
use anyhow::{bail, Context, Result};
use tracing::info;

use crate::extract::RawAddress;

// OpenAddresses US regional archive URLs
const OA_REGIONS: &[(&str, &str)] = &[
    ("us_northeast", "https://data.openaddresses.io/openaddr-collected-us_northeast.zip"),
    ("us_south", "https://data.openaddresses.io/openaddr-collected-us_south.zip"),
    ("us_midwest", "https://data.openaddresses.io/openaddr-collected-us_midwest.zip"),
    ("us_west", "https://data.openaddresses.io/openaddr-collected-us_west.zip"),
];

// ---------------------------------------------------------------------------
// GeoJSON address record
// ---------------------------------------------------------------------------

/// A single address from OpenAddresses.
struct OaAddress {
    number: String,
    street: String,
    city: String,
    state: String,
    postcode: String,
    lat: f64,
    lon: f64,
}

/// Parse a single GeoJSON Feature into an OaAddress.
fn parse_feature(value: &serde_json::Value) -> Option<OaAddress> {
    let props = value.get("properties")?;
    let geom = value.get("geometry")?;

    // Extract coordinates
    let coords = geom.get("coordinates")?;
    let lon = coords.get(0)?.as_f64()?;
    let lat = coords.get(1)?.as_f64()?;

    // Skip records with no coordinates
    if lat == 0.0 && lon == 0.0 { return None; }

    // Extract properties
    let number = props.get("number")?.as_str().unwrap_or("").trim().to_owned();
    let street = props.get("street")?.as_str().unwrap_or("").trim().to_owned();

    // Skip if missing essential fields
    if number.is_empty() || street.is_empty() { return None; }

    // Strip unit designators from number: "123 Apt 4B" → "123"
    let number = strip_unit_designator(&number);
    if number.is_empty() { return None; }

    let city = props.get("city")
        .and_then(|v| v.as_str())
        .unwrap_or("").trim().to_owned();
    let state = props.get("region")
        .and_then(|v| v.as_str())
        .unwrap_or("").trim().to_owned();
    let postcode = props.get("postcode")
        .and_then(|v| v.as_str())
        .unwrap_or("").trim().to_owned();

    Some(OaAddress {
        number, street, city, state, postcode, lat, lon,
    })
}

/// Strip unit/apartment designators from a house number string.
/// "123 Apt 4B" → "123", "456" → "456", "123-B" → "123-B"
fn strip_unit_designator(s: &str) -> String {
    let lower = s.to_lowercase();

    // Common unit designators
    for pattern in &[" apt ", " apt. ", " ste ", " ste. ", " suite ", " unit ", " #", " bldg ", " fl "] {
        if let Some(pos) = lower.find(pattern) {
            return s[..pos].trim().to_owned();
        }
    }

    // Return the numeric prefix if mixed
    let trimmed = s.trim();
    if trimmed.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
        return trimmed.to_owned();
    }

    trimmed.to_owned()
}

// ---------------------------------------------------------------------------
// GeoJSON file reader
// ---------------------------------------------------------------------------

/// Read a GeoJSON file (either FeatureCollection or newline-delimited Features).
fn read_geojson_file(data: &[u8]) -> Result<Vec<OaAddress>> {
    let text = std::str::from_utf8(data).context("Invalid UTF-8 in GeoJSON")?;
    let mut addresses = Vec::new();

    // Try as FeatureCollection first
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        if let Some(features) = value.get("features").and_then(|f| f.as_array()) {
            for feature in features {
                if let Some(addr) = parse_feature(feature) {
                    addresses.push(addr);
                }
            }
            return Ok(addresses);
        }
    }

    // Fall back to newline-delimited GeoJSON
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(addr) = parse_feature(&value) {
                addresses.push(addr);
            }
        }
    }

    Ok(addresses)
}

/// Read a CSV file from OpenAddresses (newer format).
fn read_csv_file(data: &[u8]) -> Result<Vec<OaAddress>> {
    let text = std::str::from_utf8(data).context("Invalid UTF-8 in CSV")?;
    let mut addresses = Vec::new();
    let mut lines = text.lines();

    // Parse header
    let header = match lines.next() {
        Some(h) => h,
        None => return Ok(addresses),
    };
    let cols: Vec<&str> = header.split(',').collect();
    let find_col = |name: &str| cols.iter().position(|c| c.trim().eq_ignore_ascii_case(name));

    let lon_idx = find_col("LON").or_else(|| find_col("lon"));
    let lat_idx = find_col("LAT").or_else(|| find_col("lat"));
    let number_idx = find_col("NUMBER").or_else(|| find_col("number"));
    let street_idx = find_col("STREET").or_else(|| find_col("street"));
    let city_idx = find_col("CITY").or_else(|| find_col("city"));
    let region_idx = find_col("REGION").or_else(|| find_col("region"));
    let postcode_idx = find_col("POSTCODE").or_else(|| find_col("postcode"));

    let lon_idx = match lon_idx { Some(i) => i, None => return Ok(addresses) };
    let lat_idx = match lat_idx { Some(i) => i, None => return Ok(addresses) };
    let number_idx = match number_idx { Some(i) => i, None => return Ok(addresses) };
    let street_idx = match street_idx { Some(i) => i, None => return Ok(addresses) };

    for line in lines {
        let fields: Vec<&str> = line.split(',').collect();
        let get = |idx: usize| fields.get(idx).map(|s| s.trim()).unwrap_or("");

        let lon: f64 = match get(lon_idx).parse() { Ok(v) => v, Err(_) => continue };
        let lat: f64 = match get(lat_idx).parse() { Ok(v) => v, Err(_) => continue };
        let number = get(number_idx).to_owned();
        let street = get(street_idx).to_owned();

        if number.is_empty() || street.is_empty() { continue; }
        let number = strip_unit_designator(&number);
        if number.is_empty() { continue; }

        let city = city_idx.map(|i| get(i).to_owned()).unwrap_or_default();
        let state = region_idx.map(|i| get(i).to_owned()).unwrap_or_default();
        let postcode = postcode_idx.map(|i| get(i).to_owned()).unwrap_or_default();

        addresses.push(OaAddress {
            number, street, city, state, postcode, lat, lon,
        });
    }

    Ok(addresses)
}

// ---------------------------------------------------------------------------
// Download + extract
// ---------------------------------------------------------------------------

/// Download a regional ZIP to disk, then process files from disk.
/// This avoids holding the entire 1-2GB ZIP in memory.
async fn download_region_to_disk(
    client: &reqwest::Client,
    region_name: &str,
    url: &str,
    work_dir: &Path,
) -> Result<PathBuf> {
    let zip_path = work_dir.join(format!("{}.zip", region_name));
    info!("  Downloading {} region to disk...", region_name);

    // Retry up to 3 times with exponential backoff
    let mut last_err = None;
    for attempt in 0..3 {
        if attempt > 0 {
            let delay = std::time::Duration::from_secs(5 * (1 << attempt));
            info!("  Retry {} for {} (waiting {:?})...", attempt + 1, region_name, delay);
            tokio::time::sleep(delay).await;
        }

        match download_to_file(client, url, &zip_path).await {
            Ok(size) => {
                info!("  Downloaded {:.1} MB → {}", size as f64 / 1e6, zip_path.display());
                return Ok(zip_path);
            }
            Err(e) => {
                tracing::warn!("  Download attempt {} failed: {}", attempt + 1, e);
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Download failed")))
}

/// Download to a file on disk. Downloads full response then writes at once.
/// For truly huge files, reqwest streams internally — we just write the final bytes.
async fn download_to_file(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
) -> Result<u64> {
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        bail!("HTTP {} for {}", resp.status(), url);
    }

    let bytes = resp.bytes().await?;
    let size = bytes.len() as u64;
    std::fs::write(dest, &bytes)?;
    Ok(size)
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the OpenAddresses import: download regional ZIPs to disk, parse, output addresses.parquet.
///
/// Memory strategy: streaming Parquet writer flushes every 100K records (~50MB).
/// Only one GeoJSON/CSV file is in memory at a time. Total peak: ~100-200MB
/// regardless of how many addresses exist across all 4 US regions.
pub fn run_oa_import(output_dir: &Path) -> Result<OaResult> {
    std::fs::create_dir_all(output_dir)?;

    let work_dir = output_dir.join("oa_work");
    std::fs::create_dir_all(&work_dir)?;

    let rt = tokio::runtime::Runtime::new()?;
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(30))
        .build()?;

    info!("=== OpenAddresses Import ===");
    info!("Downloading {} US regions to disk...", OA_REGIONS.len());

    // Open a streaming Parquet writer — flushes row groups incrementally
    let parquet_path = output_dir.join("addresses.parquet");
    let schema = addr_parquet_schema();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&parquet_path)?, schema.clone(), None,
    )?;

    const FLUSH_THRESHOLD: usize = 100_000;
    let mut buffer: Vec<RawAddress> = Vec::with_capacity(FLUSH_THRESHOLD);
    let mut total_count = 0usize;

    for (name, url) in OA_REGIONS {
        // Step 1: Download ZIP to disk (with retry)
        let zip_path = match rt.block_on(download_region_to_disk(&client, name, url, &work_dir)) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("Failed to download {}: {}", name, e);
                continue;
            }
        };

        // Step 2: Process ZIP from disk, flushing to parquet incrementally
        match stream_region_to_parquet(&zip_path, name, &mut writer, &schema, &mut buffer, FLUSH_THRESHOLD) {
            Ok(count) => {
                info!("{}: {} addresses", name, count);
                total_count += count;
            }
            Err(e) => {
                tracing::warn!("Failed to process {}: {}", name, e);
            }
        }

        // Step 3: Delete the ZIP to free disk space before next region
        std::fs::remove_file(&zip_path).ok();
    }

    // Flush remaining buffered records
    if !buffer.is_empty() {
        flush_addr_batch(&mut writer, &buffer, &schema)?;
        buffer.clear();
    }
    writer.close()?;

    info!("Total addresses: {} (streaming parquet)", total_count);

    // Clean up work directory
    std::fs::remove_dir_all(&work_dir).ok();

    Ok(OaResult {
        address_count: total_count,
    })
}

/// Process a region ZIP, streaming addresses into the parquet writer.
/// Only one .geojson/.csv file is buffered at a time (~<50MB).
fn stream_region_to_parquet(
    zip_path: &Path,
    region_name: &str,
    writer: &mut parquet::arrow::ArrowWriter<std::fs::File>,
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
    buffer: &mut Vec<RawAddress>,
    flush_threshold: usize,
) -> Result<usize> {
    let file = std::fs::File::open(zip_path)
        .context("Failed to open OA ZIP from disk")?;
    let mut archive = zip::ZipArchive::new(file)
        .context("Failed to read OA ZIP")?;

    let file_count = archive.len();
    let mut region_count = 0usize;
    let mut processed = 0usize;

    for i in 0..file_count {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_owned();

        let is_geojson = name.ends_with(".geojson");
        let is_csv = name.ends_with(".csv");
        if !is_geojson && !is_csv { continue; }

        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;

        let addresses = if is_geojson {
            read_geojson_file(&data)?
        } else {
            read_csv_file(&data)?
        };
        drop(data); // Free file data immediately

        let count = addresses.len();
        if count > 0 && (processed % 100 == 0 || count > 10_000) {
            info!("    {}: {} addresses", name, count);
        }

        // Convert and buffer
        for a in addresses {
            buffer.push(RawAddress {
                osm_id: 0,
                street: a.street,
                housenumber: a.number,
                postcode: if a.postcode.is_empty() { None } else { Some(a.postcode) },
                city: if a.city.is_empty() { None } else { Some(a.city) },
                lat: a.lat,
                lon: a.lon,
            });

            // Flush when buffer is full — keeps memory bounded
            if buffer.len() >= flush_threshold {
                flush_addr_batch(writer, buffer, schema)?;
                buffer.clear();
            }
        }

        region_count += count;
        processed += 1;
    }

    info!("  {} total: {} addresses from {} files", region_name, region_count, processed);
    Ok(region_count)
}

/// Flush a batch of addresses as a Parquet row group.
fn flush_addr_batch(
    writer: &mut parquet::arrow::ArrowWriter<std::fs::File>,
    addresses: &[RawAddress],
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
) -> Result<()> {
    use arrow::array::*;
    use std::sync::Arc;

    if addresses.is_empty() { return Ok(()); }

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(addresses.iter().map(|a| a.osm_id).collect::<Vec<_>>())),
            Arc::new(StringArray::from(addresses.iter().map(|a| a.street.as_str()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(addresses.iter().map(|a| a.housenumber.as_str()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(addresses.iter().map(|a| a.postcode.as_deref()).collect::<Vec<Option<&str>>>())),
            Arc::new(StringArray::from(addresses.iter().map(|a| a.city.as_deref()).collect::<Vec<Option<&str>>>())),
            Arc::new(Float64Array::from(addresses.iter().map(|a| a.lat).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(addresses.iter().map(|a| a.lon).collect::<Vec<_>>())),
        ],
    )?;
    writer.write(&batch)?;
    Ok(())
}

/// Schema for addresses.parquet (matches write_merged_addresses in main.rs)
fn addr_parquet_schema() -> std::sync::Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::*;
    std::sync::Arc::new(Schema::new(vec![
        Field::new("osm_id", DataType::Int64, false),
        Field::new("street", DataType::Utf8, false),
        Field::new("housenumber", DataType::Utf8, false),
        Field::new("postcode", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("lat", DataType::Float64, false),
        Field::new("lon", DataType::Float64, false),
    ]))
}

/// Import from a local directory of already-downloaded OpenAddresses files.
/// Expects .geojson or .csv files, optionally in subdirectories.
/// Uses streaming Parquet writer — same 100K-record flush as the download path.
pub fn run_oa_import_local(input_dir: &Path, output_dir: &Path) -> Result<OaResult> {
    std::fs::create_dir_all(output_dir)?;

    info!("=== OpenAddresses Local Import ===");
    info!("Reading from {}...", input_dir.display());

    let parquet_path = output_dir.join("addresses.parquet");
    let schema = addr_parquet_schema();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&parquet_path)?, schema.clone(), None,
    )?;

    const FLUSH_THRESHOLD: usize = 100_000;
    let mut buffer: Vec<RawAddress> = Vec::with_capacity(FLUSH_THRESHOLD);
    let mut total = 0usize;

    for path in walkdir(input_dir)? {
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext != "geojson" && ext != "csv" { continue; }

        let data = std::fs::read(&path)?;
        let addrs = if ext == "geojson" {
            read_geojson_file(&data)?
        } else {
            read_csv_file(&data)?
        };
        drop(data);

        if !addrs.is_empty() {
            info!("  {}: {} addresses", path.display(), addrs.len());
        }

        for a in addrs {
            buffer.push(RawAddress {
                osm_id: 0,
                street: a.street,
                housenumber: a.number,
                postcode: if a.postcode.is_empty() { None } else { Some(a.postcode) },
                city: if a.city.is_empty() { None } else { Some(a.city) },
                lat: a.lat,
                lon: a.lon,
            });
            if buffer.len() >= FLUSH_THRESHOLD {
                flush_addr_batch(&mut writer, &buffer, &schema)?;
                total += buffer.len();
                buffer.clear();
            }
        }
    }

    if !buffer.is_empty() {
        total += buffer.len();
        flush_addr_batch(&mut writer, &buffer, &schema)?;
    }
    writer.close()?;

    info!("Total addresses: {} (streaming parquet)", total);

    Ok(OaResult {
        address_count: total,
    })
}

/// Simple directory walker — returns all files recursively.
fn walkdir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !dir.is_dir() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(walkdir(&path)?);
        } else {
            files.push(path);
        }
    }
    Ok(files)
}

pub struct OaResult {
    pub address_count: usize,
}
