//! Package built indices into distributable .tar.zst tarballs with manifest.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tracing::info;

/// Files to include in runtime packages (everything else is build-time only).
const RUNTIME_PATTERNS: &[&str] = &[
    "records.bin",
    "fst_exact.fst",
    "fst_phonetic.fst",
    "fst_ngram.fst",
    "admin.bin",
    "addr_streets.bin",
    "addr_records.bin",
    "fst_addr.fst",
    "geohash_index.bin",
    "fst_postcode.fst",
    "postcode_centroids.bin",
    "fst_zip.fst",
    "zip_records.bin",
    "sv.toml",
    "meta.json",
];

/// Bundle definitions: name → country codes.
pub fn bundle_definitions() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        ("nordic", vec!["se", "no", "dk", "fi"]),
        ("europe", vec!["se", "no", "dk", "fi", "de", "gb"]),
    ]
    // "world" is handled dynamically — includes all countries found
}

#[derive(Debug, serde::Serialize)]
pub struct CountryPackage {
    pub file: String,
    pub sha256: String,
    pub size: u64,
    pub places: usize,
    pub addresses: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct BundlePackage {
    pub file: String,
    pub sha256: String,
    pub size: u64,
    pub countries: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct Manifest {
    pub version: String,
    pub date: String,
    pub base_url: String,
    pub countries: HashMap<String, CountryPackage>,
    pub bundles: HashMap<String, BundlePackage>,
}

/// Detect country code from index directory name.
pub fn detect_country_code(dir_name: &str) -> &str {
    // Order matters: "denmark" contains "de" so must be checked before germany
    if dir_name.contains("denmark") || dir_name.ends_with("-dk") {
        "dk"
    } else if dir_name.contains("germany") || dir_name.ends_with("-de") {
        "de"
    } else if dir_name.contains("norway") || dir_name.ends_with("-no") {
        "no"
    } else if dir_name.contains("sweden") || dir_name.ends_with("-se") {
        "se"
    } else if dir_name.contains("finland") || dir_name.ends_with("-fi") {
        "fi"
    } else if dir_name.contains("-gb") || dir_name.contains("-uk") || dir_name.contains("britain") {
        "gb"
    } else if dir_name.contains("-us") || dir_name.contains("united-states") || dir_name.contains("america") {
        "us"
    } else {
        "unknown"
    }
}

fn country_name(code: &str) -> &str {
    match code {
        "se" => "Sweden",
        "no" => "Norway",
        "dk" => "Denmark",
        "fi" => "Finland",
        "de" => "Germany",
        "gb" => "Great Britain",
        "us" => "United States",
        _ => "Unknown",
    }
}

/// Collect runtime files from an index directory.
fn collect_runtime_files(index_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for pattern in RUNTIME_PATTERNS {
        let path = index_dir.join(pattern);
        if path.exists() {
            files.push(path);
        }
    }
    if files.is_empty() {
        anyhow::bail!("No runtime files found in {}", index_dir.display());
    }
    Ok(files)
}

/// Read places/addresses counts from meta.json.
fn read_meta(index_dir: &Path) -> (usize, usize) {
    let meta_path = index_dir.join("meta.json");
    if let Ok(data) = fs::read_to_string(&meta_path) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) {
            let places = v["record_count"].as_u64().unwrap_or(0) as usize;
            let addresses = v["address_count"].as_u64().unwrap_or(0) as usize;
            return (places, addresses);
        }
    }
    (0, 0)
}

/// Create a .tar.zst package from an index directory. Returns (tarball_path, sha256, size).
fn create_tarball(
    index_dir: &Path,
    output_dir: &Path,
    filename: &str,
) -> Result<(PathBuf, String, u64)> {
    let tarball_path = output_dir.join(filename);
    let file = fs::File::create(&tarball_path)
        .with_context(|| format!("creating {}", tarball_path.display()))?;
    let writer = BufWriter::new(file);
    let encoder = zstd::Encoder::new(writer, 3)?;
    let mut tar_builder = tar::Builder::new(encoder);

    // Use the directory name as the tarball root (e.g. "index-se/records.bin")
    let dir_name = index_dir.file_name().unwrap().to_str().unwrap();
    let runtime_files = collect_runtime_files(index_dir)?;

    for file_path in &runtime_files {
        let entry_name = format!(
            "{}/{}",
            dir_name,
            file_path.file_name().unwrap().to_str().unwrap()
        );
        tar_builder
            .append_path_with_name(file_path, &entry_name)
            .with_context(|| format!("adding {} to tarball", file_path.display()))?;
    }

    let encoder = tar_builder.into_inner()?;
    encoder.finish()?;

    // Compute SHA256
    let sha256 = sha256_file(&tarball_path)?;
    let size = fs::metadata(&tarball_path)?.len();

    Ok((tarball_path, sha256, size))
}

/// Create a bundle tarball from multiple index directories.
fn create_bundle_tarball(
    index_dirs: &[&Path],
    output_dir: &Path,
    filename: &str,
) -> Result<(PathBuf, String, u64)> {
    let tarball_path = output_dir.join(filename);
    let file = fs::File::create(&tarball_path)?;
    let writer = BufWriter::new(file);
    let encoder = zstd::Encoder::new(writer, 3)?;
    let mut tar_builder = tar::Builder::new(encoder);

    for index_dir in index_dirs {
        let dir_name = index_dir.file_name().unwrap().to_str().unwrap();
        let runtime_files = collect_runtime_files(index_dir)?;

        for file_path in &runtime_files {
            let entry_name = format!(
                "{}/{}",
                dir_name,
                file_path.file_name().unwrap().to_str().unwrap()
            );
            tar_builder.append_path_with_name(file_path, &entry_name)?;
        }
    }

    let encoder = tar_builder.into_inner()?;
    encoder.finish()?;

    let sha256 = sha256_file(&tarball_path)?;
    let size = fs::metadata(&tarball_path)?.len();

    Ok((tarball_path, sha256, size))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Package all index directories into distributable tarballs + manifest.
pub fn package(
    index_dirs: &[PathBuf],
    output_dir: &Path,
    version: &str,
    create_bundles: bool,
    base_url: &str,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;

    info!("Packaging {} indices into {}", index_dirs.len(), output_dir.display());

    // Phase 1: per-country tarballs
    let mut country_packages: HashMap<String, CountryPackage> = HashMap::new();
    let mut country_dirs: HashMap<String, PathBuf> = HashMap::new();

    for index_dir in index_dirs {
        let dir_name = index_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        let cc = detect_country_code(dir_name).to_string();
        let (places, addresses) = read_meta(index_dir);

        let filename = format!("heimdall-{}-{}.tar.zst", cc, version);
        info!(
            "  {} ({}) — {} places, {} addresses",
            cc.to_uppercase(),
            country_name(&cc),
            places,
            addresses
        );

        let (tarball_path, sha256, size) = create_tarball(index_dir, output_dir, &filename)?;

        info!(
            "    → {} ({:.1} MB)",
            tarball_path.file_name().unwrap().to_str().unwrap(),
            size as f64 / 1_048_576.0,
        );

        country_packages.insert(
            cc.clone(),
            CountryPackage {
                file: filename,
                sha256,
                size,
                places,
                addresses,
            },
        );
        country_dirs.insert(cc, index_dir.clone());
    }

    // Phase 2: bundles
    let mut bundle_packages: HashMap<String, BundlePackage> = HashMap::new();

    if create_bundles {
        let mut defs = bundle_definitions();
        // Add "world" dynamically with all available countries
        let all_codes: Vec<&str> = country_dirs.keys().map(|s| s.as_str()).collect();
        defs.push(("world", all_codes));

        for (bundle_name, codes) in &defs {
            // Only create bundle if we have all the countries
            let available: Vec<&Path> = codes
                .iter()
                .filter_map(|cc| country_dirs.get(*cc).map(|p| p.as_path()))
                .collect();

            if available.len() != codes.len() {
                let missing: Vec<&&str> = codes
                    .iter()
                    .filter(|cc| !country_dirs.contains_key(**cc))
                    .collect();
                info!("  Skipping {} bundle — missing: {:?}", bundle_name, missing);
                continue;
            }

            let filename = format!("heimdall-{}-{}.tar.zst", bundle_name, version);
            info!("  Creating {} bundle ({} countries)...", bundle_name, codes.len());

            let (tarball_path, sha256, size) =
                create_bundle_tarball(&available, output_dir, &filename)?;

            info!(
                "    → {} ({:.1} MB)",
                tarball_path.file_name().unwrap().to_str().unwrap(),
                size as f64 / 1_048_576.0,
            );

            bundle_packages.insert(
                bundle_name.to_string(),
                BundlePackage {
                    file: filename,
                    sha256,
                    size,
                    countries: codes.iter().map(|s| s.to_string()).collect(),
                },
            );
        }
    }

    // Phase 3: manifest
    let manifest = Manifest {
        version: version.to_string(),
        date: chrono_date(),
        base_url: base_url.to_string(),
        countries: country_packages,
        bundles: bundle_packages,
    };

    let manifest_path = output_dir.join("manifest.json");
    let json = serde_json::to_string_pretty(&manifest)?;
    fs::write(&manifest_path, &json)?;
    info!("Manifest written to {}", manifest_path.display());

    // Summary
    let total_places: usize = manifest.countries.values().map(|c| c.places).sum();
    let total_addrs: usize = manifest.countries.values().map(|c| c.addresses).sum();
    info!(
        "Done: {} countries, {} places, {} addresses",
        manifest.countries.len(),
        total_places,
        total_addrs,
    );

    Ok(())
}

fn chrono_date() -> String {
    // Simple ISO date without chrono dependency
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let days = now / 86400;
    // Approximate — good enough for a build date
    let years = days / 365;
    let year = 1970 + years;
    let rem_days = days - years * 365 - (years + 1) / 4; // leap year correction
    let month = rem_days / 30 + 1;
    let day = rem_days % 30 + 1;
    format!("{:04}-{:02}-{:02}", year, month.min(12), day.min(31))
}
