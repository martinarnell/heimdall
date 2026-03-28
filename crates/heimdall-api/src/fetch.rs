//! Download prebuilt Heimdall indices from GitHub Releases.

use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tracing::info;

use crate::manifest::{self, Manifest};

const DEFAULT_BASE_URL: &str =
    "https://github.com/martinarnell/heimdall/releases/latest/download";

/// Resolve the default data directory: ~/.heimdall/indices
pub fn default_data_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".heimdall")
        .join("indices")
}

/// Fetch the manifest.json from the release.
pub async fn fetch_manifest(base_url: &str) -> Result<Manifest> {
    let url = format!("{}/manifest.json", base_url.trim_end_matches('/'));
    info!("Fetching manifest from {}", url);

    let resp = reqwest::get(&url)
        .await
        .with_context(|| format!("downloading manifest from {}", url))?;

    if !resp.status().is_success() {
        bail!(
            "Failed to fetch manifest: HTTP {} from {}",
            resp.status(),
            url
        );
    }

    let manifest: Manifest = resp
        .json()
        .await
        .context("parsing manifest.json")?;

    info!(
        "Manifest v{}: {} countries, {} bundles",
        manifest.version,
        manifest.countries.len(),
        manifest.bundles.len()
    );

    Ok(manifest)
}

/// List available indices from the manifest.
pub async fn list_available(base_url: &str) -> Result<()> {
    let manifest = fetch_manifest(base_url).await?;

    println!("\nAvailable indices (v{}, {}):\n", manifest.version, manifest.date);

    println!("  {:<6} {:<16} {:>10} {:>12} {:>10}", "Code", "Country", "Places", "Addresses", "Size");
    println!("  {}", "-".repeat(60));

    let mut codes: Vec<&String> = manifest.countries.keys().collect();
    codes.sort();

    let mut total_places = 0usize;
    let mut total_addrs = 0usize;

    for code in &codes {
        let pkg = &manifest.countries[*code];
        total_places += pkg.places;
        total_addrs += pkg.addresses;
        println!(
            "  {:<6} {:<16} {:>10} {:>12} {:>7.1} MB",
            code.to_uppercase(),
            manifest::country_name(code),
            format_num(pkg.places),
            format_num(pkg.addresses),
            pkg.size as f64 / 1_048_576.0,
        );
    }

    println!("  {}", "-".repeat(60));
    println!(
        "  {:<6} {:<16} {:>10} {:>12}",
        "",
        "Total",
        format_num(total_places),
        format_num(total_addrs),
    );

    if !manifest.bundles.is_empty() {
        println!("\n  Bundles:");
        for (name, bundle) in &manifest.bundles {
            println!(
                "    {:<12} {} ({:.1} MB)",
                name,
                bundle.countries.join(", ").to_uppercase(),
                bundle.size as f64 / 1_048_576.0,
            );
        }
    }

    println!("\n  Usage: heimdall fetch se no       # individual countries");
    println!("         heimdall fetch nordic      # bundle");
    println!("         heimdall fetch world        # everything");

    Ok(())
}

/// Download and extract indices for the given targets.
pub async fn fetch_targets(
    targets: &[String],
    base_url: &str,
    data_dir: &Path,
) -> Result<()> {
    let manifest = fetch_manifest(base_url).await?;

    // Resolve targets to country codes
    let mut country_codes: Vec<String> = Vec::new();

    for target in targets {
        let t = target.to_lowercase();
        if manifest::is_bundle(&t) {
            // Bundle: expand to individual countries
            if t == "world" || t == "planet" {
                // All countries in manifest
                for code in manifest.countries.keys() {
                    if !country_codes.contains(code) {
                        country_codes.push(code.clone());
                    }
                }
            } else if let Some(codes) = manifest::bundle_countries(&t) {
                for code in codes {
                    let s = code.to_string();
                    if !country_codes.contains(&s) {
                        country_codes.push(s);
                    }
                }
            }
        } else if manifest.countries.contains_key(&t) {
            if !country_codes.contains(&t) {
                country_codes.push(t);
            }
        } else {
            bail!(
                "Unknown target '{}'. Use 'heimdall fetch --list' to see available indices.",
                target
            );
        }
    }

    if country_codes.is_empty() {
        bail!("No indices to download. Use 'heimdall fetch --list' to see available indices.");
    }

    country_codes.sort();

    // Check which are already present
    let mut to_download: Vec<String> = Vec::new();
    for code in &country_codes {
        let index_dir = find_index_dir(data_dir, code);
        if index_dir.exists() && index_dir.join("meta.json").exists() {
            info!("  {} already exists at {}", code.to_uppercase(), index_dir.display());
        } else {
            to_download.push(code.clone());
        }
    }

    if to_download.is_empty() {
        println!("All requested indices already present in {}", data_dir.display());
        println!("Use 'heimdall fetch --update' to check for newer versions.");
        return Ok(());
    }

    println!(
        "\nDownloading {} indices to {}:\n",
        to_download.len(),
        data_dir.display()
    );

    std::fs::create_dir_all(data_dir)?;

    for code in &to_download {
        let pkg = &manifest.countries[code];
        println!(
            "  {} ({}) — {} places, {} addresses, {:.1} MB",
            code.to_uppercase(),
            manifest::country_name(code),
            format_num(pkg.places),
            format_num(pkg.addresses),
            pkg.size as f64 / 1_048_576.0,
        );

        let url = format!(
            "{}/{}",
            manifest.base_url.trim_end_matches('/'),
            pkg.file
        );

        download_and_extract(&url, &pkg.sha256, pkg.size, data_dir).await?;
    }

    println!("\nDone. Start the server with:");
    let index_flags: Vec<String> = country_codes
        .iter()
        .map(|cc| {
            let dir = find_index_dir(data_dir, cc);
            format!("  --index {}", dir.display())
        })
        .collect();
    println!("  heimdall serve {}", index_flags.join(" \\\n"));

    Ok(())
}

/// Check for updates against the manifest.
pub async fn check_updates(data_dir: &Path, base_url: &str) -> Result<()> {
    let manifest = fetch_manifest(base_url).await?;

    println!("\nChecking for updates (manifest v{}):\n", manifest.version);

    let mut found_any = false;

    for (code, pkg) in &manifest.countries {
        let index_dir = find_index_dir(data_dir, code);
        if !index_dir.exists() {
            continue;
        }
        found_any = true;

        let meta_path = index_dir.join("meta.json");
        let local_version = if meta_path.exists() {
            let data = std::fs::read_to_string(&meta_path).unwrap_or_default();
            let v: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
            v["built_at"].as_u64().unwrap_or(0).to_string()
        } else {
            "unknown".to_string()
        };

        println!(
            "  {} — local: {}, remote: v{}",
            code.to_uppercase(),
            local_version,
            manifest.version,
        );
    }

    if !found_any {
        println!("  No local indices found in {}", data_dir.display());
    }

    Ok(())
}

/// Download a file, verify SHA256, and extract the tar.zst to dest_dir.
async fn download_and_extract(
    url: &str,
    expected_sha256: &str,
    expected_size: u64,
    dest_dir: &Path,
) -> Result<()> {
    let resp = reqwest::get(url)
        .await
        .with_context(|| format!("downloading {}", url))?;

    if !resp.status().is_success() {
        bail!("Download failed: HTTP {} from {}", resp.status(), url);
    }

    let total_size = resp.content_length().unwrap_or(expected_size);

    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::with_template(
            "    [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})"
        )
        .unwrap()
        .progress_chars("=> "),
    );

    // Download to temp file while computing SHA256
    let temp_path = dest_dir.join(".download.tmp");
    let mut file = std::fs::File::create(&temp_path)?;
    let mut hasher = Sha256::new();
    let mut stream = resp.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("reading download stream")?;
        hasher.update(&chunk);
        std::io::Write::write_all(&mut file, &chunk)?;
        pb.inc(chunk.len() as u64);
    }

    pb.finish_and_clear();
    drop(file);

    // Verify checksum
    let actual_sha256 = format!("{:x}", hasher.finalize());
    if actual_sha256 != expected_sha256 {
        std::fs::remove_file(&temp_path).ok();
        bail!(
            "SHA256 mismatch: expected {}, got {}",
            expected_sha256,
            actual_sha256
        );
    }

    // Extract tar.zst
    let file = std::fs::File::open(&temp_path)?;
    let decoder = zstd::Decoder::new(file)?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(dest_dir)?;

    std::fs::remove_file(&temp_path).ok();

    Ok(())
}

/// Find or construct the index directory path for a country code.
fn find_index_dir(data_dir: &Path, code: &str) -> PathBuf {
    data_dir.join(format!("index-{}", code))
}

fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
