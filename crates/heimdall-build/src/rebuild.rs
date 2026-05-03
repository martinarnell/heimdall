//! Automated rebuild pipeline for Heimdall indices.
//!
//! Downloads updated sources, detects changes, and rebuilds affected country
//! indices with configurable parallelism (minimal/standard/fast modes).
//!
//! Usage: `heimdall-build rebuild [OPTIONS]`

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tracing::info;

// ───────────────────────────────────────────────────────────────────────────
// Config types (deserialized from sources.toml)
// ───────────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(crate) struct SourcesConfig {
    pub defaults: Defaults,
    pub country: HashMap<String, CountryConfig>,
}

#[derive(Deserialize)]
pub(crate) struct Defaults {
    pub download_dir: String,
    pub index_dir: String,
}

#[derive(Deserialize)]
pub(crate) struct CountryConfig {
    #[allow(dead_code)]
    pub name: String,
    pub index_name: String,
    pub normalizer: String,
    #[allow(dead_code)]
    pub ram_gb: u32,
    pub osm: Option<OsmSource>,
    pub photon: Option<PhotonSource>,
    pub national: Option<NationalSource>,
    pub places_source: Option<PlacesSource>,
}

#[derive(Deserialize)]
pub(crate) struct OsmSource {
    pub url: String,
    pub state_url: Option<String>,
    /// Optional supplementary PBF URLs merged into the main extract before
    /// the per-country pipeline runs. Used to bring overseas territories
    /// (Greenland, Faroe Islands) into Denmark's index in one shot. The
    /// `osmium merge` binary must be installed on the host.
    #[serde(default)]
    pub extra_urls: Vec<String>,
}

#[derive(Deserialize)]
pub(crate) struct PhotonSource {
    pub url: String,
    pub md5_url: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct NationalSource {
    #[serde(rename = "type")]
    pub source_type: Option<String>,
    pub url: Option<String>,
    pub sequence_url: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct PlacesSource {
    #[serde(rename = "type")]
    pub source_type: String,
    pub url: Option<String>,
    /// Local path to already-downloaded file (skip download)
    pub local_path: Option<String>,
}

// ───────────────────────────────────────────────────────────────────────────
// State types (persisted to rebuild-state.json)
// ───────────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Default)]
struct RebuildState {
    countries: HashMap<String, CountryState>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct CountryState {
    osm: Option<SourceState>,
    photon: Option<SourceState>,
    national: Option<SourceState>,
    places_source: Option<SourceState>,
    last_built: Option<u64>,
    /// Number of incremental diffs applied since last full PBF extract.
    #[serde(skip_serializing_if = "Option::is_none")]
    diff_count_since_full: Option<u32>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct SourceState {
    #[serde(skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    osm_sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    md5: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sequence_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_path: Option<String>,
    /// Per-zone version tags for sources that ship as N independent files
    /// (BD TOPO publishes one .7z per département with its own editionDate).
    /// Keyed by IGN zone code (e.g. "D075", "D971"), value is the last
    /// successfully ingested editionDate ("2026-03-15"). Allows streaming
    /// rebuilds that skip unchanged départements without re-downloading.
    #[serde(skip_serializing_if = "Option::is_none")]
    zones: Option<std::collections::HashMap<String, String>>,
}

// ───────────────────────────────────────────────────────────────────────────
// Run mode
// ───────────────────────────────────────────────────────────────────────────

// ───────────────────────────────────────────────────────────────────────────
// Build report
// ───────────────────────────────────────────────────────────────────────────

struct BuildReport {
    started_at: u64,
    mode: String,
    countries_requested: Vec<String>,
    steps: Vec<StepEntry>,
    download_bytes: u64,
    download_secs: f64,
}

struct StepEntry {
    country: String,
    step: String,
    duration_secs: f64,
    peak_ram_mb: Option<u64>,
    cpu_user_secs: Option<f64>,
    cpu_sys_secs: Option<f64>,
    disk_usage_mb: Option<u64>,
    details: String,
}

impl BuildReport {
    fn new(mode: &str, countries: &[String]) -> Self {
        Self {
            started_at: unix_now(),
            mode: mode.to_string(),
            countries_requested: countries.to_vec(),
            steps: Vec::new(),
            download_bytes: 0,
            download_secs: 0.0,
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────────

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Format a UNIX timestamp as ISO 8601 UTC.
fn format_utc(secs: u64) -> String {
    // Howard Hinnant's civil_from_days algorithm
    let days = (secs / 86400) as i32;
    let time = secs % 86400;
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m,
        d,
        time / 3600,
        (time % 3600) / 60,
        time % 60,
    )
}

/// Extract a filename from a URL, stripping query parameters.
fn url_filename(url: &str) -> String {
    let path = url.split('?').next().unwrap_or(url);
    let name = path.rsplit('/').next().unwrap_or("download");
    if name.is_empty() {
        "download".to_string()
    } else {
        name.to_string()
    }
}

/// Get CPU user+sys time in seconds for the current process.
#[cfg(unix)]
fn get_cpu_times() -> Option<(f64, f64)> {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    if ret != 0 { return None; }
    let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0;
    let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0;
    Some((user, sys))
}

#[cfg(not(unix))]
fn get_cpu_times() -> Option<(f64, f64)> { None }

/// Get disk usage of a directory in MB.
fn get_dir_size_mb(path: &Path) -> Option<u64> {
    let mut total: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                }
            }
        }
    }
    Some(total / (1024 * 1024))
}

/// Snapshot current process RSS in megabytes.
fn get_rss_mb() -> Option<u64> {
    let pid = std::process::id();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&output.stdout);
    let kb: u64 = s.trim().parse().ok()?;
    Some(kb / 1024)
}

fn load_config(path: &Path) -> Result<SourcesConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config: {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("Failed to parse config: {}", path.display()))
}

fn load_state(path: &Path) -> Result<RebuildState> {
    if !path.exists() {
        return Ok(RebuildState::default());
    }
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read state: {}", path.display()))?;
    serde_json::from_str(&text)
        .with_context(|| format!("Failed to parse state: {}", path.display()))
}

fn save_state(state: &RebuildState, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(state)?;
    std::fs::write(path, json)?;
    Ok(())
}

/// Sample RSS in a background thread, return the peak observed.
fn sample_peak_rss(stop: Arc<std::sync::atomic::AtomicBool>) -> std::thread::JoinHandle<u64> {
    std::thread::spawn(move || {
        let mut peak: u64 = 0;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            if let Some(rss) = get_rss_mb() {
                peak = peak.max(rss);
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        // One final sample
        if let Some(rss) = get_rss_mb() {
            peak = peak.max(rss);
        }
        peak
    })
}

/// Time a build step, capturing wall clock, true peak RSS, CPU time, and disk usage.
fn time_step(
    cc: &str,
    step_name: &str,
    f: impl FnOnce() -> Result<String>,
) -> Result<StepEntry> {
    let cpu_before = get_cpu_times();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = sample_peak_rss(Arc::clone(&stop));
    let start = Instant::now();
    info!("[{}] {}...", cc, step_name);

    let details = f()?;

    let duration = start.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let peak_rss = sampler.join().unwrap_or(0);
    let cpu_after = get_cpu_times();

    let (cpu_user, cpu_sys) = match (cpu_before, cpu_after) {
        (Some((u0, s0)), Some((u1, s1))) => (Some(u1 - u0), Some(s1 - s0)),
        _ => (None, None),
    };

    let cpu_str = cpu_user.map(|u| format!(", cpu: {:.1}s user + {:.1}s sys", u, cpu_sys.unwrap_or(0.0))).unwrap_or_default();
    info!(
        "[{}] {} done in {:.1}s, peak {:.1} GB{} — {}",
        cc,
        step_name,
        duration.as_secs_f64(),
        peak_rss as f64 / 1024.0,
        cpu_str,
        details,
    );

    Ok(StepEntry {
        country: cc.to_string(),
        step: step_name.to_string(),
        duration_secs: duration.as_secs_f64(),
        peak_ram_mb: Some(peak_rss),
        cpu_user_secs: cpu_user,
        cpu_sys_secs: cpu_sys,
        disk_usage_mb: None,
        details,
    })
}

/// Time a build step that returns a value alongside its description.
fn time_step_with<T>(
    cc: &str,
    step_name: &str,
    f: impl FnOnce() -> Result<(T, String)>,
) -> Result<(StepEntry, T)> {
    let cpu_before = get_cpu_times();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = sample_peak_rss(Arc::clone(&stop));
    let start = Instant::now();
    info!("[{}] {}...", cc, step_name);

    let (value, details) = f()?;

    let duration = start.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let peak_rss = sampler.join().unwrap_or(0);
    let cpu_after = get_cpu_times();

    let (cpu_user, cpu_sys) = match (cpu_before, cpu_after) {
        (Some((u0, s0)), Some((u1, s1))) => (Some(u1 - u0), Some(s1 - s0)),
        _ => (None, None),
    };

    let cpu_str = cpu_user.map(|u| format!(", cpu: {:.1}s user + {:.1}s sys", u, cpu_sys.unwrap_or(0.0))).unwrap_or_default();
    info!(
        "[{}] {} done in {:.1}s, peak {:.1} GB{} — {}",
        cc,
        step_name,
        duration.as_secs_f64(),
        peak_rss as f64 / 1024.0,
        cpu_str,
        details,
    );

    Ok((
        StepEntry {
            country: cc.to_string(),
            step: step_name.to_string(),
            duration_secs: duration.as_secs_f64(),
            peak_ram_mb: Some(peak_rss),
            cpu_user_secs: cpu_user,
            cpu_sys_secs: cpu_sys,
            disk_usage_mb: None,
            details,
        },
        value,
    ))
}

// ───────────────────────────────────────────────────────────────────────────
// Change detection (async)
// ───────────────────────────────────────────────────────────────────────────

/// Check if OSM PBF has a newer sequence number than what we last saw.
/// Returns (changed, new_sequence).
async fn check_osm_changed(
    client: &reqwest::Client,
    osm: &OsmSource,
    prev: Option<&SourceState>,
) -> (bool, Option<u64>) {
    let state_url = match osm.state_url {
        Some(ref u) => u,
        None => return (true, None),
    };

    let resp = match client.get(state_url).send().await {
        Ok(r) => r,
        Err(_) => return (true, None), // Network error → assume changed
    };

    let text = match resp.text().await {
        Ok(t) => t,
        Err(_) => return (true, None),
    };

    for line in text.lines() {
        if let Some(seq_str) = line.strip_prefix("sequenceNumber=") {
            if let Ok(seq) = seq_str.trim().parse::<u64>() {
                let changed = match prev {
                    Some(s) => s.osm_sequence.map_or(true, |prev_seq| seq > prev_seq),
                    None => true,
                };
                return (changed, Some(seq));
            }
        }
    }

    (true, None) // Couldn't parse → assume changed
}

/// Check if Photon MD5 hash has changed.
/// Returns (changed, new_md5).
async fn check_photon_changed(
    client: &reqwest::Client,
    photon: &PhotonSource,
    prev: Option<&SourceState>,
) -> (bool, Option<String>) {
    let md5_url = match photon.md5_url {
        Some(ref u) => u,
        None => return (true, None),
    };

    // Try the configured URL first; if 404 and contains -latest, resolve from directory
    let actual_url = if let Some(resolved) = resolve_photon_url(client, md5_url).await {
        resolved
    } else {
        md5_url.clone()
    };

    let resp = match client.get(&actual_url).send().await {
        Ok(r) if r.status().is_success() => r,
        _ => return (true, None),
    };

    let text = match resp.text().await {
        Ok(t) => t,
        Err(_) => return (true, None),
    };

    let md5 = text.split_whitespace().next().unwrap_or("").to_string();
    if md5.is_empty() {
        return (true, None);
    }

    let changed = match prev {
        Some(s) => s.md5.as_deref().map_or(true, |prev_md5| md5 != prev_md5),
        None => true,
    };
    (changed, Some(md5))
}

/// Check if a national data source has changed.
/// Returns (changed, new_sequence_or_etag).
async fn check_national_changed(
    client: &reqwest::Client,
    national: &NationalSource,
    prev: Option<&SourceState>,
) -> (bool, Option<u64>, Option<String>) {
    let source_type = national.source_type.as_deref().unwrap_or("");
    match source_type {
        "dawa" => {
            if let Some(ref seq_url) = national.sequence_url {
                let resp = match client.get(seq_url).send().await {
                    Ok(r) => r,
                    Err(_) => return (true, None, None),
                };
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(seq) = json.get("sekvensnummer").and_then(|v| v.as_u64()) {
                        let changed = match prev {
                            Some(s) => {
                                s.sequence_number.map_or(true, |prev_seq| seq > prev_seq)
                            }
                            None => true,
                        };
                        return (changed, Some(seq), None);
                    }
                }
            }
            (true, None, None)
        }
        "kartverket" => {
            if let Some(ref url) = national.url {
                let resp = match client.head(url).send().await {
                    Ok(r) => r,
                    Err(_) => return (true, None, None),
                };
                if let Some(etag) = resp.headers().get("etag") {
                    if let Ok(etag_str) = etag.to_str() {
                        let changed = match prev {
                            Some(s) => {
                                s.etag.as_deref().map_or(true, |prev_etag| etag_str != prev_etag)
                            }
                            None => true,
                        };
                        return (changed, None, Some(etag_str.to_string()));
                    }
                }
            }
            (true, None, None)
        }
        "dvv" => (true, None, None), // Always rebuild — no version endpoint
        "gnaf" => {
            // G-NAF: HEAD request + ETag comparison (same as kartverket)
            if let Some(ref url) = national.url {
                let resp = match client.head(url).send().await {
                    Ok(r) => r,
                    Err(_) => return (true, None, None),
                };
                if let Some(etag) = resp.headers().get("etag") {
                    if let Ok(etag_str) = etag.to_str() {
                        let changed = match prev {
                            Some(s) => {
                                s.etag.as_deref().map_or(true, |prev_etag| etag_str != prev_etag)
                            }
                            None => true,
                        };
                        return (changed, None, Some(etag_str.to_string()));
                    }
                }
            }
            (true, None, None)
        }
        "nar" => {
            // NAR: HEAD request + ETag comparison
            if let Some(ref url) = national.url {
                let resp = match client.head(url).send().await {
                    Ok(r) => r,
                    Err(_) => return (true, None, None),
                };
                if let Some(etag) = resp.headers().get("etag") {
                    if let Ok(etag_str) = etag.to_str() {
                        let changed = match prev {
                            Some(s) => {
                                s.etag.as_deref().map_or(true, |prev_etag| etag_str != prev_etag)
                            }
                            None => true,
                        };
                        return (changed, None, Some(etag_str.to_string()));
                    }
                }
            }
            (true, None, None)
        }
        "linz" => {
            // LINZ: manual download required (API token needed) — always rebuild if file present
            (true, None, None)
        }
        // Official sources — always rebuild (manual download or simple URL check)
        "bag" | "ban" | "swisstopo" | "bev" | "best" | "ruian" | "prg" | "ads" | "vzd" | "lt"
        | "abr" | "juso" | "cnefe" => {
            (true, None, None)
        }
        _ => (true, None, None),
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Download (async, streaming)
// ───────────────────────────────────────────────────────────────────────────

/// Streaming download with conditional GET support.
/// Returns (response_etag, bytes_downloaded).
async fn download_file(
    client: &reqwest::Client,
    url: &str,
    local_path: &Path,
    prev_etag: Option<&str>,
) -> Result<(Option<String>, u64)> {
    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut req = client.get(url);
    if let Some(etag) = prev_etag {
        req = req.header("If-None-Match", etag);
    }

    let resp = req
        .send()
        .await
        .with_context(|| format!("Failed to request {}", url))?;

    if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
        info!("  {} → 304 Not Modified", url_filename(url));
        return Ok((prev_etag.map(|s| s.to_string()), 0));
    }

    if !resp.status().is_success() {
        bail!("HTTP {} for {}", resp.status(), url);
    }

    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let total_size = resp.content_length();
    let mut file = std::fs::File::create(local_path)?;
    let mut downloaded: u64 = 0;

    // Stream response body chunk-by-chunk
    let mut resp = resp;
    while let Some(chunk) = resp.chunk().await? {
        file.write_all(&chunk)?;
        let prev = downloaded;
        downloaded += chunk.len() as u64;
        // Log progress every ~50 MB
        if downloaded / (50 * 1024 * 1024) > prev / (50 * 1024 * 1024) {
            if let Some(total) = total_size {
                info!(
                    "  {} → {:.0}/{:.0} MB ({:.0}%)",
                    url_filename(url),
                    downloaded as f64 / 1e6,
                    total as f64 / 1e6,
                    downloaded as f64 / total as f64 * 100.0,
                );
            } else {
                info!(
                    "  {} → {:.0} MB",
                    url_filename(url),
                    downloaded as f64 / 1e6,
                );
            }
        }
    }

    info!(
        "  {} → {:.1} MB downloaded",
        url_filename(url),
        downloaded as f64 / 1e6,
    );
    Ok((etag, downloaded))
}

// ───────────────────────────────────────────────────────────────────────────
// Change detection (check only, no download)
// ───────────────────────────────────────────────────────────────────────────

async fn check_country_changed(
    client: &reqwest::Client,
    cc: &str,
    country_config: &CountryConfig,
    cs: &mut CountryState,
    force: bool,
) -> bool {
    let mut any_changed = false;

    if let Some(ref osm) = country_config.osm {
        let (changed, new_seq) = if force {
            (true, None)
        } else {
            check_osm_changed(client, osm, cs.osm.as_ref()).await
        };
        if let Some(seq) = new_seq {
            cs.osm.get_or_insert_with(Default::default).osm_sequence = Some(seq);
        }
        if changed { any_changed = true; }
        info!(
            "[{}] osm: {}{}",
            cc,
            if changed { "changed" } else { "unchanged" },
            new_seq.map_or(String::new(), |s| format!(" (seq {})", s)),
        );
    }

    if let Some(ref photon) = country_config.photon {
        let (changed, new_md5) = if force {
            (true, None)
        } else {
            check_photon_changed(client, photon, cs.photon.as_ref()).await
        };
        if let Some(ref md5) = new_md5 {
            cs.photon.get_or_insert_with(Default::default).md5 = Some(md5.clone());
        }
        if changed { any_changed = true; }
        info!("[{}] photon: {}", cc, if changed { "changed" } else { "unchanged" });
    }

    if let Some(ref national) = country_config.national {
        let (changed, new_seq, new_etag) = if force {
            (true, None, None)
        } else {
            check_national_changed(client, national, cs.national.as_ref()).await
        };
        if let Some(seq) = new_seq {
            cs.national.get_or_insert_with(Default::default).sequence_number = Some(seq);
        }
        if let Some(ref etag) = new_etag {
            cs.national.get_or_insert_with(Default::default).etag = Some(etag.clone());
        }
        if changed { any_changed = true; }
        let type_str = national.source_type.as_deref().unwrap_or("unknown");
        info!("[{}] national ({}): {}", cc, type_str, if changed { "changed" } else { "unchanged" });
    }

    any_changed
}

// ───────────────────────────────────────────────────────────────────────────
// Per-source download helpers
// ───────────────────────────────────────────────────────────────────────────

/// Download a single source file, update state with local path + etag.
/// If the URL returns 404 and contains "-latest", try to resolve the actual
/// dated filename from the directory listing (Graphhopper broke -latest symlinks).
/// Automatically reuses existing local files if they pass size validation.
async fn download_source(
    client: &reqwest::Client,
    url: &str,
    download_dir: &Path,
    ss: &mut SourceState,
    skip: bool,
) -> Result<PathBuf> {
    if skip {
        let filename = url_filename(url);
        let local = download_dir.join(&filename);
        if local.exists() {
            info!("  Reusing existing: {}", local.display());
            ss.local_path = Some(local.to_string_lossy().to_string());
            return Ok(local);
        }
        // Also check for a file with the base name pattern (without -latest)
        if let Ok(entries) = std::fs::read_dir(download_dir) {
            let base = filename.split("-latest").next().unwrap_or(&filename);
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with(base) && !name_str.contains("-latest") && !name_str.ends_with(".md5") {
                    info!("  Reusing existing: {}", entry.path().display());
                    ss.local_path = Some(entry.path().to_string_lossy().to_string());
                    return Ok(entry.path());
                }
            }
        }
        bail!("--skip-download: no existing file found for {} in {}", filename, download_dir.display());
    }

    let resolved_url = resolve_photon_url(client, url).await;
    let actual_url = resolved_url.as_deref().unwrap_or(url);
    let local = download_dir.join(url_filename(actual_url));

    // If file already exists, check size against server to detect truncation
    if local.exists() {
        let local_size = std::fs::metadata(&local).map(|m| m.len()).unwrap_or(0);
        if local_size > 0 {
            // HEAD request to check expected size (detect truncated downloads)
            let size_ok = match client.head(actual_url).send().await {
                Ok(resp) => match resp.content_length() {
                    Some(expected) if expected > 0 => {
                        if local_size >= expected {
                            true
                        } else {
                            info!("  {} is truncated ({:.1} MB local vs {:.1} MB expected), re-downloading",
                                local.file_name().unwrap().to_string_lossy(),
                                local_size as f64 / 1e6, expected as f64 / 1e6);
                            false
                        }
                    }
                    _ => true, // Server didn't report size or returned 0, trust local file
                },
                Err(_) => true, // Can't reach server, use what we have
            };
            if size_ok {
                info!("  Reusing existing: {} ({:.1} MB)", local.file_name().unwrap().to_string_lossy(), local_size as f64 / 1e6);
                ss.local_path = Some(local.to_string_lossy().to_string());
                return Ok(local);
            }
        }
    }

    let (etag, bytes) = download_file(client, actual_url, &local, ss.etag.as_deref()).await?;
    // If server returned 304 but local file doesn't exist, re-download without ETag
    if bytes == 0 && !local.exists() {
        info!("  304 but local file missing — re-downloading {}", url_filename(actual_url));
        let (etag, _) = download_file(client, actual_url, &local, None).await?;
        if let Some(etag) = etag {
            ss.etag = Some(etag);
        }
    } else if let Some(etag) = etag {
        ss.etag = Some(etag);
    }
    ss.local_path = Some(local.to_string_lossy().to_string());
    Ok(local)
}

/// If a Photon URL with "-latest" is 404, scrape the directory listing
/// for the actual dated filename (e.g. photon-db-se-250720.tar.bz2).
async fn resolve_photon_url(client: &reqwest::Client, url: &str) -> Option<String> {
    if !url.contains("-latest") {
        return None;
    }

    // Quick HEAD check — if 200, no resolution needed
    if let Ok(resp) = client.head(url).send().await {
        if resp.status().is_success() {
            return None;
        }
    }

    // Derive parent directory URL
    let parent = url.rsplit_once('/').map(|(parent, _)| format!("{}/", parent))?;

    // Fetch directory listing
    let listing = client.get(&parent).send().await.ok()?.text().await.ok()?;

    // Extract the base pattern: "photon-db-XX" from "photon-db-XX-latest.tar.bz2"
    let filename = url.rsplit_once('/')?.1;
    let base = filename.split("-latest").next()?; // e.g. "photon-db-se"

    // Find dated files matching the base pattern (not -latest, not .md5)
    let mut candidates: Vec<&str> = listing
        .split('"')
        .filter(|s| s.starts_with(base) && s.ends_with(".tar.bz2") && !s.contains("-latest") && !s.ends_with(".md5"))
        .collect();

    // Sort descending to get the most recent dated version
    candidates.sort();
    candidates.reverse();

    if let Some(dated_file) = candidates.first() {
        let resolved = format!("{}{}", parent, dated_file);
        info!("  Photon -latest is 404, resolved to: {}", dated_file);
        Some(resolved)
    } else {
        None
    }
}

/// Delete a file if it exists, log the freed space.
fn delete_file_if_exists(path: &Path, label: &str, keep: bool) {
    if !path.exists() { return; }
    if keep || path.is_symlink() {
        info!("  Keeping {} (no --cleanup)", label);
        return;
    }
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    match std::fs::remove_file(path) {
        Ok(()) => info!("  Deleted {} ({:.1} MB freed)", label, size as f64 / 1e6),
        Err(e) => tracing::warn!("  Failed to delete {}: {}", label, e),
    }
}

// ───────────────────────────────────────────────────────────────────────────
// National source merge
// ───────────────────────────────────────────────────────────────────────────

fn merge_national(
    cc: &str,
    source_type: &str,
    local_path: Option<&Path>,
    addr_parquet: &Path,
) -> Result<String> {
    let national_addresses = match source_type {
        "dawa" => {
            let path =
                local_path.ok_or_else(|| anyhow::anyhow!("[{}] No DAWA file downloaded", cc))?;
            crate::dawa::read_dawa_addresses(path)?
        }
        "kartverket" => {
            let zip_path = local_path
                .ok_or_else(|| anyhow::anyhow!("[{}] No Kartverket file downloaded", cc))?;
            // Extract CSV from ZIP if needed
            let csv_path = if zip_path.extension().map_or(false, |e| e == "zip") {
                extract_kartverket_csv(zip_path)?
            } else {
                zip_path.to_path_buf()
            };
            crate::geonorge::read_kartverket_addresses(&csv_path)?
        }
        "dvv" => {
            // DVV downloads at build time via OGC API
            crate::dvv::download_dvv_addresses()?
        }
        "gnaf" => {
            // G-NAF: 15.9M Australian addresses — stream directly through dedup to parquet
            // This special path avoids holding all 15.9M addresses in memory (~1.9 GB)
            let path =
                local_path.ok_or_else(|| anyhow::anyhow!("[{}] No G-NAF file downloaded", cc))?;
            return merge_national_streaming(cc, path, addr_parquet, |zip_path, emit| {
                crate::gnaf::stream_gnaf_addresses(zip_path, emit)
            });
        }
        "nar" => {
            // NAR: 15.8M Canadian addresses from ZIP
            let path =
                local_path.ok_or_else(|| anyhow::anyhow!("[{}] No NAR file downloaded", cc))?;
            crate::nar::read_nar_addresses(path)?
        }
        "linz" => {
            // LINZ: 2.3M New Zealand addresses from GeoPackage
            let path =
                local_path.ok_or_else(|| anyhow::anyhow!("[{}] No LINZ GeoPackage downloaded (requires LINZ API token)", cc))?;
            crate::linz::read_linz_addresses(path)?
        }
        "bag" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No BAG CSV downloaded", cc))?;
            crate::bag::read_bag_addresses(path)?
        }
        "ban" => {
            // BAN downloads per-département files — use the download dir
            let dir = local_path.map(|p| p.parent().unwrap_or(p).to_path_buf())
                .unwrap_or_else(|| std::path::PathBuf::from("data/downloads/ban"));
            crate::ban::download_ban_addresses(&dir)?
        }
        "best" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No BeST ZIP downloaded", cc))?;
            crate::best::read_best_addresses(&[path])?
        }
        "swisstopo" => {
            // swisstopo needs 3 CSV files — look for them in the download dir
            let dir = local_path.map(|p| p.parent().unwrap_or(p))
                .ok_or_else(|| anyhow::anyhow!("[{}] No swisstopo files found", cc))?;
            let addr = dir.join("ADRESSE.csv");
            let street = dir.join("STRASSE.csv");
            let locality = dir.join("ORTSCHAFT.csv");
            crate::swisstopo::read_swisstopo_addresses(&addr, &street, &locality)?
        }
        "bev" => {
            let dir = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No BEV directory found", cc))?;
            crate::bev::read_bev_addresses(dir)?
        }
        "ruian" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No RÚIAN CSV downloaded", cc))?;
            crate::ruian::read_ruian_addresses(path)?
        }
        "prg" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No PRG data downloaded", cc))?;
            crate::prg::read_prg_addresses(path)?
        }
        "ads" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No ADS CSV downloaded", cc))?;
            crate::ads::read_ads_addresses(path)?
        }
        "vzd" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No VZD CSV downloaded", cc))?;
            crate::vzd::read_vzd_addresses(path)?
        }
        "lt" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No Lithuanian SQLite downloaded", cc))?;
            crate::lt::read_lt_addresses(path)?
        }
        "abr" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No ABR SQLite found (run `npx @digital-go-jp/abr-geocoder download` first)", cc))?;
            crate::abr::read_abr_addresses(path)?
        }
        "juso" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No juso.go.kr data downloaded (register at business.juso.go.kr)", cc))?;
            crate::juso::read_juso_addresses(path)?
        }
        "cnefe" => {
            let path = local_path.ok_or_else(|| anyhow::anyhow!("[{}] No CNEFE data downloaded", cc))?;
            crate::cnefe::read_cnefe_addresses(path)?
        }
        other => bail!("[{}] Unknown national source type: {}", cc, other),
    };

    // Step A: Build lightweight dedup index by streaming existing parquet
    // Only keys + coords in memory — NOT the full RawAddress structs
    let mut osm_index: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
    let mut osm_count = 0usize;

    if addr_parquet.exists() {
        use arrow::array::*;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = std::fs::File::open(addr_parquet)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        for batch_result in reader {
            let batch = batch_result?;
            let streets = batch.column_by_name("street").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();
            let hns = batch.column_by_name("housenumber").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();
            let lats = batch.column_by_name("lat").unwrap()
                .as_any().downcast_ref::<Float64Array>().unwrap();
            let lons = batch.column_by_name("lon").unwrap()
                .as_any().downcast_ref::<Float64Array>().unwrap();

            for i in 0..batch.num_rows() {
                let street = streets.value(i);
                let hn = hns.value(i);
                if !street.is_empty() && !hn.is_empty() {
                    let key = format!("{}:{}", street.to_lowercase(), hn.to_lowercase());
                    osm_index.entry(key).or_default().push((lats.value(i), lons.value(i)));
                    osm_count += 1;
                }
            }
        }
    }
    info!("[{}] Dedup index: {} OSM addresses ({:.0} MB)", cc, osm_count, osm_index.len() as f64 * 50.0 / 1e6);

    // Step B: Filter national addresses through dedup, write survivors directly
    // to parquet in 500K batches — never holds all survivors in memory.
    let national_parquet = addr_parquet.parent().unwrap_or(Path::new(".")).join("addresses_national.parquet");
    let mut added = 0usize;
    let mut deduped = 0usize;

    {
        use arrow::array::*;
        use arrow::datatypes::*;
        use parquet::arrow::ArrowWriter;

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("osm_id", DataType::Int64, false),
            Field::new("street", DataType::Utf8, false),
            Field::new("housenumber", DataType::Utf8, false),
            Field::new("postcode", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
            Field::new("state", DataType::Utf8, true),
            Field::new("lat", DataType::Float64, false),
            Field::new("lon", DataType::Float64, false),
        ]));

        let file = std::fs::File::create(&national_parquet)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
        let mut batch_buf: Vec<crate::extract::RawAddress> = Vec::with_capacity(500_000);

        for addr in national_addresses {
            let key = format!("{}:{}", addr.street.to_lowercase(), addr.housenumber.to_lowercase());
            let is_dup = if let Some(coords) = osm_index.get(&key) {
                coords.iter().any(|(olat, olon)| {
                    let dlat = (addr.lat - olat) * 111_000.0;
                    let dlon = (addr.lon - olon) * 111_000.0 * addr.lat.to_radians().cos();
                    (dlat * dlat + dlon * dlon).sqrt() < 10.0
                })
            } else {
                false
            };

            if is_dup {
                deduped += 1;
            } else {
                batch_buf.push(addr);
                added += 1;
                if batch_buf.len() >= 500_000 {
                    let batch = arrow::record_batch::RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            std::sync::Arc::new(Int64Array::from(batch_buf.iter().map(|a| a.osm_id).collect::<Vec<_>>())),
                            std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.street.as_str()).collect::<Vec<_>>())),
                            std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.housenumber.as_str()).collect::<Vec<_>>())),
                            std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.postcode.as_deref()).collect::<Vec<Option<&str>>>())),
                            std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.city.as_deref()).collect::<Vec<Option<&str>>>())),
                            std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.state.as_deref()).collect::<Vec<Option<&str>>>())),
                            std::sync::Arc::new(Float64Array::from(batch_buf.iter().map(|a| a.lat).collect::<Vec<_>>())),
                            std::sync::Arc::new(Float64Array::from(batch_buf.iter().map(|a| a.lon).collect::<Vec<_>>())),
                        ],
                    )?;
                    writer.write(&batch)?;
                    batch_buf.clear();
                }
            }
        }
        // Flush remaining
        if !batch_buf.is_empty() {
            let batch = arrow::record_batch::RecordBatch::try_new(
                schema.clone(),
                vec![
                    std::sync::Arc::new(Int64Array::from(batch_buf.iter().map(|a| a.osm_id).collect::<Vec<_>>())),
                    std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.street.as_str()).collect::<Vec<_>>())),
                    std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.housenumber.as_str()).collect::<Vec<_>>())),
                    std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.postcode.as_deref()).collect::<Vec<Option<&str>>>())),
                    std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.city.as_deref()).collect::<Vec<Option<&str>>>())),
                    std::sync::Arc::new(StringArray::from(batch_buf.iter().map(|a| a.state.as_deref()).collect::<Vec<Option<&str>>>())),
                    std::sync::Arc::new(Float64Array::from(batch_buf.iter().map(|a| a.lat).collect::<Vec<_>>())),
                    std::sync::Arc::new(Float64Array::from(batch_buf.iter().map(|a| a.lon).collect::<Vec<_>>())),
                ],
            )?;
            writer.write(&batch)?;
        }
        writer.close()?;
    } // writer + batch_buf + osm_index dropped

    if added == 0 {
        std::fs::remove_file(&national_parquet).ok();
    }

    info!("[{}] National merge: {} added, {} deduped", cc, added, deduped);
    let total = osm_count + added;
    Ok(format!("+{} addr ({} total)", added, total))
}

/// Streaming merge for large national sources (G-NAF, CNEFE, etc.).
/// Builds dedup index from existing parquet, then streams national addresses
/// one-at-a-time through the dedup filter and writes to parquet in 500K batches.
/// Peak memory: dedup index (~200 MB for 4M OSM) + geocode lookup (~1.2 GB for G-NAF) + batch buffer (60 MB).
fn merge_national_streaming(
    cc: &str,
    source_path: &Path,
    addr_parquet: &Path,
    stream_fn: impl FnOnce(&Path, &mut dyn FnMut(crate::extract::RawAddress)) -> anyhow::Result<usize>,
) -> Result<String> {
    use arrow::array::*;
    use arrow::datatypes::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;

    // Build dedup index by streaming existing parquet
    let mut osm_index: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
    let mut osm_count = 0usize;

    if addr_parquet.exists() {
        let file = std::fs::File::open(addr_parquet)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        for batch in builder.build()? {
            let batch = batch?;
            let streets = batch.column_by_name("street").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let hns = batch.column_by_name("housenumber").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
            let lats = batch.column_by_name("lat").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
            let lons = batch.column_by_name("lon").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..batch.num_rows() {
                let s = streets.value(i);
                let h = hns.value(i);
                if !s.is_empty() && !h.is_empty() {
                    osm_index.entry(format!("{}:{}", s.to_lowercase(), h.to_lowercase()))
                        .or_default().push((lats.value(i), lons.value(i)));
                    osm_count += 1;
                }
            }
        }
    }
    info!("[{}] Dedup index: {} existing addresses", cc, osm_count);

    // Open parquet writer for survivors
    let national_parquet = addr_parquet.parent().unwrap_or(Path::new(".")).join("addresses_national.parquet");
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("osm_id", DataType::Int64, false),
        Field::new("street", DataType::Utf8, false),
        Field::new("housenumber", DataType::Utf8, false),
        Field::new("postcode", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("lat", DataType::Float64, false),
        Field::new("lon", DataType::Float64, false),
    ]));
    let file = std::fs::File::create(&national_parquet)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
    let mut batch_buf: Vec<crate::extract::RawAddress> = Vec::with_capacity(500_000);
    let mut added = 0usize;
    let mut deduped = 0usize;

    // Flush helper
    let flush = |buf: &mut Vec<crate::extract::RawAddress>, w: &mut ArrowWriter<std::fs::File>, s: &std::sync::Arc<Schema>| -> Result<()> {
        if buf.is_empty() { return Ok(()); }
        let batch = arrow::record_batch::RecordBatch::try_new(
            s.clone(),
            vec![
                std::sync::Arc::new(Int64Array::from(buf.iter().map(|a| a.osm_id).collect::<Vec<_>>())),
                std::sync::Arc::new(StringArray::from(buf.iter().map(|a| a.street.as_str()).collect::<Vec<_>>())),
                std::sync::Arc::new(StringArray::from(buf.iter().map(|a| a.housenumber.as_str()).collect::<Vec<_>>())),
                std::sync::Arc::new(StringArray::from(buf.iter().map(|a| a.postcode.as_deref()).collect::<Vec<Option<&str>>>())),
                std::sync::Arc::new(StringArray::from(buf.iter().map(|a| a.city.as_deref()).collect::<Vec<Option<&str>>>())),
                std::sync::Arc::new(StringArray::from(buf.iter().map(|a| a.state.as_deref()).collect::<Vec<Option<&str>>>())),
                std::sync::Arc::new(Float64Array::from(buf.iter().map(|a| a.lat).collect::<Vec<_>>())),
                std::sync::Arc::new(Float64Array::from(buf.iter().map(|a| a.lon).collect::<Vec<_>>())),
            ],
        )?;
        w.write(&batch)?;
        buf.clear();
        Ok(())
    };

    // Stream national addresses through dedup filter
    stream_fn(source_path, &mut |addr: crate::extract::RawAddress| {
        let key = format!("{}:{}", addr.street.to_lowercase(), addr.housenumber.to_lowercase());
        let is_dup = if let Some(coords) = osm_index.get(&key) {
            coords.iter().any(|(olat, olon)| {
                let dlat = (addr.lat - olat) * 111_000.0;
                let dlon = (addr.lon - olon) * 111_000.0 * addr.lat.to_radians().cos();
                (dlat * dlat + dlon * dlon).sqrt() < 10.0
            })
        } else {
            false
        };
        if is_dup {
            deduped += 1;
        } else {
            batch_buf.push(addr);
            added += 1;
            if batch_buf.len() >= 500_000 {
                let _ = flush(&mut batch_buf, &mut writer, &schema);
            }
        }
    })?;

    flush(&mut batch_buf, &mut writer, &schema)?;
    writer.close()?;

    if added == 0 {
        std::fs::remove_file(&national_parquet).ok();
    }

    info!("[{}] Streaming merge: {} added, {} deduped", cc, added, deduped);
    let total = osm_count + added;
    Ok(format!("+{} addr ({} total)", added, total))
}

/// US-specific: run TIGER import (admin boundaries + ZIP codes) then OA import (addresses).
/// This replaces the typical merge_national flow for the US.
fn merge_national_us(
    cc: &str,
    index_dir: &Path,
    _addr_parquet: &Path,
) -> Result<String> {
    // Step A: TIGER import → admin.bin, fst_zip.fst, zip_records.bin, states.json
    // Now also pulls COUSUB (towns/townships in strong-MCD states),
    // AIANNH (tribal areas), Census Gazetteer (county populations) and
    // the HUD/simplemaps crosswalk (ZIP → city). All of these are
    // best-effort: a failed download warns and the build continues.
    info!("[{}] Running TIGER import (admin boundaries + ZIP codes + COUSUB + AIANNH + Gazetteer + HUD)...", cc);
    let tiger_result = crate::tiger::run_tiger_import(index_dir)?;
    info!(
        "[{}] TIGER: {} states, {} counties ({} pop), {} places, {} ZIPs ({} HUD-fixed), {} cousubs, {} AIANNH",
        cc, tiger_result.state_count, tiger_result.county_count,
        tiger_result.county_pop_count, tiger_result.place_count,
        tiger_result.zip_count, tiger_result.hud_updated_count,
        tiger_result.cousub_count, tiger_result.aiannh_count,
    );

    // Step B: OpenAddresses import → addresses.parquet
    info!("[{}] Running OpenAddresses import...", cc);
    let oa_result = crate::oa::run_oa_import(index_dir)?;

    Ok(format!(
        "TIGER: {} states + {} counties + {} ZIPs + {} cousubs + {} AIANNH, OA: {} addr",
        tiger_result.state_count, tiger_result.county_count,
        tiger_result.zip_count, tiger_result.cousub_count,
        tiger_result.aiannh_count, oa_result.address_count,
    ))
}

/// Extract the first CSV file from a Kartverket ZIP archive.
fn extract_kartverket_csv(zip_path: &Path) -> Result<PathBuf> {
    let file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    let out_dir = zip_path.parent().unwrap_or(Path::new("."));

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if name.ends_with(".csv") {
            let out_path = out_dir.join(
                Path::new(&name)
                    .file_name()
                    .unwrap_or(std::ffi::OsStr::new("kartverket.csv")),
            );
            let mut out = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut out)?;
            info!("  Extracted {} from ZIP", out_path.display());
            return Ok(out_path);
        }
    }

    bail!(
        "No CSV file found in Kartverket ZIP: {}",
        zip_path.display()
    )
}

// ───────────────────────────────────────────────────────────────────────────
// Places source merge (SSR, etc.)
// ───────────────────────────────────────────────────────────────────────────

fn merge_places_source(
    cc: &str,
    source_type: &str,
    path: &Path,
    places_parquet: &Path,
) -> Result<String> {
    let new_places = match source_type {
        "ssr" => {
            // SSR GML: might be a .zip or a .gml
            let gml_path = if path.extension().map_or(false, |e| e == "zip") {
                extract_gml_from_zip(path)?
            } else {
                path.to_path_buf()
            };
            crate::ssr::read_ssr_places(&gml_path)?
        }
        "dagi" => crate::dagi::read_dagi_places(path)?,
        "gn250" => {
            // GN250: a .zip containing GN250.csv (we want that one only)
            let csv_path = if path.extension().map_or(false, |e| e == "zip") {
                extract_gn250_csv_from_zip(path)?
            } else {
                path.to_path_buf()
            };
            crate::gn250::read_gn250_places(&csv_path)?
        }
        "gnis" => {
            // GNIS: USGS distribution ZIP containing a single .txt file
            // (DomesticNames_National.txt or similar). Pipe-delimited,
            // header row, WGS84 decimal-degree coords.
            let txt_path = if path.extension().map_or(false, |e| e == "zip") {
                crate::gnis::extract_gnis_txt_from_zip(path)?
            } else {
                path.to_path_buf()
            };
            crate::gnis::read_gnis_places(&txt_path)?
        }
        // "bdtopo" is intentionally absent here — it's handled by the
        // streaming downloader in the caller (read_bdtopo_streaming),
        // which manages its own per-département state and disk lifecycle.
        // Routing it through merge_places_source's single-path interface
        // would require dumping all 100+ extracted .gpkgs to disk at
        // once (~180 GB). The caller should never invoke this branch.
        other => bail!("[{}] Unknown places source type: {}", cc, other),
    };

    let existing = if places_parquet.exists() {
        super::read_osm_places(places_parquet)?
    } else {
        vec![]
    };

    let merged = match source_type {
        "dagi" => crate::dagi::merge_dagi_places(&existing, &new_places),
        "gn250" => crate::gn250::merge_gn250_places(&existing, &new_places),
        "gnis" => crate::gnis::merge_gnis_places(&existing, &new_places),
        // Default — SSR's spatial+name dedup works for any geometry-rich
        // place source, so reuse it.
        _ => crate::ssr::merge_ssr_places(&existing, &new_places),
    };
    crate::photon::write_places_parquet(&merged, places_parquet)?;

    let label = match source_type {
        "dagi" => "DAGI",
        "gn250" => "GN250",
        "gnis" => "GNIS",
        _ => "SSR",
    };
    Ok(format!(
        "+{} places ({} {} total)",
        merged.len() - existing.len(),
        new_places.len(),
        label,
    ))
}

/// Extract the first .gml file from a ZIP archive.
fn extract_gml_from_zip(zip_path: &Path) -> Result<PathBuf> {
    let file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    let out_dir = zip_path.parent().unwrap_or(Path::new("."));

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if name.to_lowercase().ends_with(".gml") {
            let out_path = out_dir.join(
                Path::new(&name)
                    .file_name()
                    .unwrap_or(std::ffi::OsStr::new("ssr.gml")),
            );
            let mut out = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut out)?;
            info!("  Extracted {} from ZIP", out_path.display());
            return Ok(out_path);
        }
    }

    bail!(
        "No GML file found in ZIP: {}",
        zip_path.display()
    )
}

/// Extract GN250.csv (the place-names table) from the BKG zip.
///
/// The zip contains GN250.csv (the names), GN_DLMLink.csv (links to DLM50),
/// GN_VORWAHL.csv (phone-area-code reference), plus PDF documentation.
/// We only need GN250.csv.
fn extract_gn250_csv_from_zip(zip_path: &Path) -> Result<PathBuf> {
    let file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    let out_dir = zip_path.parent().unwrap_or(Path::new("."));

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        let basename = Path::new(&name)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        if basename.eq_ignore_ascii_case("GN250.csv") {
            let out_path = out_dir.join("GN250.csv");
            let mut out = std::fs::File::create(&out_path)?;
            std::io::copy(&mut entry, &mut out)?;
            info!("  Extracted {} from ZIP", out_path.display());
            return Ok(out_path);
        }
    }

    bail!(
        "GN250.csv not found in ZIP: {}",
        zip_path.display()
    )
}

// ───────────────────────────────────────────────────────────────────────────
// Photon merge
// ───────────────────────────────────────────────────────────────────────────

/// Hash street + housenumber (case-insensitive) for spatial dedup.
fn addr_name_hash(street: &str, hn: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    street.to_lowercase().hash(&mut hasher);
    hn.to_lowercase().hash(&mut hasher);
    hasher.finish()
}

/// Pack name hash + lat/lon into a u64 spatial dedup cell.
/// Layout: [21 bits name | 21 bits lat | 22 bits lon]. Cell ≈ 11m × 11m.
fn pack_dedup_cell(name_hash: u64, lat: f64, lon: f64) -> u64 {
    let lat_cell = ((lat + 90.0) * 10_000.0).floor() as u32;
    let lon_cell = ((lon + 180.0) * 10_000.0).floor() as u32;
    ((name_hash >> 43) << 43)
        | (((lat_cell & 0x1F_FFFF) as u64) << 22)
        | ((lon_cell & 0x3F_FFFF) as u64)
}

/// Check 3×3 grid of cells around a point for dedup hits (~22m effective radius).
fn is_addr_dup(set: &HashSet<u64>, name_hash: u64, lat: f64, lon: f64) -> bool {
    let lat_cell = ((lat + 90.0) * 10_000.0).floor() as i32;
    let lon_cell = ((lon + 180.0) * 10_000.0).floor() as i32;
    let name_bits = (name_hash >> 43) << 43;
    for dlat in -1..=1_i32 {
        for dlon in -1..=1_i32 {
            let lc = (lat_cell + dlat) as u32;
            let nc = (lon_cell + dlon) as u32;
            let key = name_bits
                | (((lc & 0x1F_FFFF) as u64) << 22)
                | ((nc & 0x3F_FFFF) as u64);
            if set.contains(&key) {
                return true;
            }
        }
    }
    false
}

fn merge_photon(
    cc: &str,
    photon_path: &Path,
    places_parquet: &Path,
    addr_parquet: &Path,
    index_dir: &Path,
) -> Result<String> {
    let input_str = photon_path.to_string_lossy();

    let photon_data = if input_str.ends_with(".tar.bz2") || input_str.ends_with(".tar") {
        // Elasticsearch/Lucene dump from Graphhopper
        let extract_dir = index_dir.join("photon_extract");
        std::fs::create_dir_all(&extract_dir)?;

        super::extract_tar_bz2(photon_path, &extract_dir)
            .with_context(|| format!("[{}] tar extraction failed for {}", cc, photon_path.display()))?;

        let lucene_dir = super::find_lucene_index_dir(&extract_dir)?;
        info!("[{}] Reading Lucene index from {} (streaming)", cc, lucene_dir.display());

        // Streaming: read one segment at a time, parse each doc immediately, drop raw bytes
        let (places, addresses) = crate::lucene::read_and_parse_streaming(
            &lucene_dir,
            crate::photon::parse_single_es_document,
        )?;

        std::fs::remove_dir_all(&extract_dir).ok();
        crate::photon::PhotonParseResult { places, addresses }
    } else {
        crate::photon::parse(photon_path)?
    };

    // Destructure to separate places and addresses
    let crate::photon::PhotonParseResult { places: photon_places, addresses: photon_addresses } = photon_data;

    // Addresses: build dedup index from ALL existing parquets (OSM + national),
    // then write non-duplicate Photon addresses to addresses_photon.parquet
    let addr_delta = {
        use arrow::array::*;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // Spatial dedup: HashSet<u64> with ~11m grid cells + name hash.
        // ~8 bytes per address vs ~120 bytes with HashMap<String, Vec<(f64,f64)>>.
        // US (35M addresses): ~500 MB vs ~4 GB.
        let mut dedup_cells: HashSet<u64> = HashSet::new();

        // Stream all existing address parquets to build dedup index
        let national_parquet = addr_parquet.parent().unwrap_or(Path::new(".")).join("addresses_national.parquet");
        for pq in &[addr_parquet, &national_parquet] {
            if !pq.exists() { continue; }
            let file = std::fs::File::open(pq)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            for batch in builder.build()? {
                let batch = batch?;
                let streets = batch.column_by_name("street").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                let hns = batch.column_by_name("housenumber").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
                let lats = batch.column_by_name("lat").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
                let lons = batch.column_by_name("lon").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..batch.num_rows() {
                    let s = streets.value(i);
                    let h = hns.value(i);
                    if !s.is_empty() && !h.is_empty() {
                        let nh = addr_name_hash(s, h);
                        dedup_cells.insert(pack_dedup_cell(nh, lats.value(i), lons.value(i)));
                    }
                }
            }
        }

        info!("[{}] Address dedup index: {} cells ({:.1} MB)", cc,
              dedup_cells.len(), dedup_cells.len() as f64 * 8.0 / 1_000_000.0);

        // Filter Photon addresses, write survivors to separate parquet
        let mut survivors: Vec<crate::extract::RawAddress> = Vec::new();
        let mut deduped = 0usize;
        for addr in photon_addresses {
            let nh = addr_name_hash(&addr.street, &addr.housenumber);
            if is_addr_dup(&dedup_cells, nh, addr.lat, addr.lon) {
                deduped += 1;
            } else {
                survivors.push(addr);
            }
        }
        drop(dedup_cells);

        let delta = survivors.len();
        if !survivors.is_empty() {
            let photon_parquet = addr_parquet.parent().unwrap_or(Path::new(".")).join("addresses_photon.parquet");
            super::write_merged_addresses(&survivors, &photon_parquet)?;
        }
        delta
    };

    // Places: same dedup pattern but simpler (places are smaller, full load is OK)
    let place_delta = {
        let osm_places = if places_parquet.exists() {
            super::read_osm_places(places_parquet)?
        } else {
            vec![]
        };
        let osm_count = osm_places.len();
        let merged = super::merge_places(&osm_places, &photon_places);
        drop(osm_places);
        let delta = merged.len() - osm_count;
        crate::photon::write_places_parquet(&merged, places_parquet)?;
        delta
    };

    Ok(format!("+{} places  +{} addr", place_delta, addr_delta))
}

// ───────────────────────────────────────────────────────────────────────────
// Per-country build pipeline
// ───────────────────────────────────────────────────────────────────────────

fn load_normalizer(index_dir: &Path) -> heimdall_normalize::Normalizer {
    let toml_path = super::find_normalizer_toml(index_dir);
    if toml_path.exists() {
        heimdall_normalize::Normalizer::from_config(&toml_path)
    } else {
        heimdall_normalize::Normalizer::swedish()
    }
}

/// Maximum number of incremental diffs before forcing a full PBF extract.
/// ~30 daily diffs ≈ 1 month. Beyond this, way centroid drift and relation
/// changes accumulate enough to warrant a fresh extract.
#[allow(dead_code)]
const MAX_INCREMENTAL_DIFFS: u32 = 30;

/// Full per-country pipeline: staggered downloads → extract → cleanup PBF → merge → enrich → pack → cleanup parquet.
/// Downloads are interleaved with build steps to minimize peak disk usage.
fn build_country_pipeline(
    cc: &str,
    config: &CountryConfig,
    cs: &mut CountryState,
    defaults: &Defaults,
    min_population: u32,
    skip_download: bool,
    cleanup: bool,
    rt: &tokio::runtime::Runtime,
    client: &reqwest::Client,
) -> Result<Vec<StepEntry>> {
    let index_dir = PathBuf::from(&defaults.index_dir).join(&config.index_name);
    let download_dir = PathBuf::from(&defaults.download_dir).join(cc);
    std::fs::create_dir_all(&index_dir)?;

    // Copy normalizer TOML — always overwrite, not just when missing.
    // Mid-rebuild edits to data/normalizers/{cc}.toml (e.g. tweaking
    // a known_variants entry to fix a ranking bug) would otherwise
    // silently land in the source TOML but not in the served sv.toml,
    // producing a confusing "I edited the file but the API didn't
    // see the change" failure mode.
    let dest_toml = index_dir.join("sv.toml");
    let source = PathBuf::from(&config.normalizer);
    if source.exists() {
        std::fs::copy(&source, &dest_toml)?;
        info!("[{}] Copied normalizer {} → sv.toml", cc, source.display());
    }

    let photon_only = config.osm.is_none();
    let mut steps = Vec::new();
    let places_parquet = index_dir.join("places.parquet");
    let addr_parquet = index_dir.join("addresses.parquet");

    if photon_only {
        // ── Photon-only pipeline (e.g., GB) ──────────────────────────────

        // Download Photon (only download needed)
        if let Some(ref photon) = config.photon {
            let ss = cs.photon.get_or_insert_with(Default::default);
            rt.block_on(download_source(client, &photon.url, &download_dir, ss, skip_download))?;
        }
        let photon_path = cs.photon.as_ref()
            .and_then(|s| s.local_path.as_ref()).map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("[{}] No Photon download for photon-only country", cc))?;

        let step = time_step(cc, "photon-extract", || {
            merge_photon(cc, &photon_path, &places_parquet, &addr_parquet, &index_dir)
        })?;
        steps.push(step);

        // Delete Photon download — no longer needed
        delete_file_if_exists(&photon_path, "photon download", !cleanup);

        let (step, enriched) = time_step_with(cc, "enrich", || {
            let r = crate::enrich::enrich(&places_parquet, &index_dir)?;
            let desc = format!("{} admin regions", r.admin_count);
            Ok((r, desc))
        })?;
        steps.push(step);

        pack_indices(cc, &index_dir, &places_parquet, &addr_parquet, &enriched, &mut steps)?;
        write_build_meta(&index_dir, &photon_path, &steps)?;
    } else {
        // ── Full OSM pipeline (staggered downloads) ──────────────────────

        // Step 1: Download PBF → extract → delete PBF
        if let Some(ref osm) = config.osm {
            let ss = cs.osm.get_or_insert_with(Default::default);
            let pbf_path = rt.block_on(download_source(client, &osm.url, &download_dir, ss, skip_download))?;

            // Optional supplementary PBFs (e.g. Greenland + Faroe Islands
            // for Denmark). Downloaded fresh each rebuild — no per-source
            // state tracking, since they're tiny and change slowly.
            let mut extra_paths: Vec<PathBuf> = Vec::new();
            for extra_url in &osm.extra_urls {
                let mut tmp_state = SourceState::default();
                let p = rt.block_on(download_source(client, extra_url, &download_dir, &mut tmp_state, skip_download))?;
                extra_paths.push(p);
            }

            let merged_pbf_path: PathBuf = if extra_paths.is_empty() {
                pbf_path.clone()
            } else {
                // Merge with osmium so the extract pipeline sees one stream.
                // osmium ships in most Geofabrik-friendly environments; we
                // surface a clear error if it's missing.
                let merged = download_dir.join(format!("{}-realm.osm.pbf", cc));
                let mut cmd = std::process::Command::new("osmium");
                cmd.arg("merge").arg("--overwrite").arg(&pbf_path);
                for ep in &extra_paths { cmd.arg(ep); }
                cmd.arg("-o").arg(&merged);
                let status = cmd.status().map_err(|e| anyhow::anyhow!(
                    "[{}] osmium merge failed (is osmium-tool installed?): {}", cc, e))?;
                if !status.success() {
                    anyhow::bail!("[{}] osmium merge exited with status {}", cc, status);
                }
                merged
            };

            let pp = places_parquet.clone();
            let step = time_step(cc, "extract", || {
                let r = crate::extract::extract_places(&merged_pbf_path, &pp, min_population, true)?;
                Ok(format!("{} places  {} addr", r.place_count, r.address_count))
            })?;
            steps.push(step);

            // Cleanup PBF + extras + merged file (all served their purpose)
            delete_file_if_exists(&pbf_path, "PBF download", !cleanup);
            for ep in &extra_paths { delete_file_if_exists(ep, "extra PBF", !cleanup); }
            if merged_pbf_path != pbf_path {
                delete_file_if_exists(&merged_pbf_path, "merged PBF", !cleanup);
            }
        }

        // Step 2: Download national source → merge → delete download
        if let Some(ref national) = config.national {
            let source_type = national.source_type.as_deref().unwrap_or("");

            if source_type == "tiger_oa" {
                let id = index_dir.clone();
                let ap = addr_parquet.clone();
                let step = time_step(cc, "tiger-oa-import", || {
                    merge_national_us(cc, &id, &ap)
                })?;
                steps.push(step);
            } else if source_type == "dvv" {
                if skip_download {
                    info!("[{}] Skipping DVV merge (--skip-download, requires API access)", cc);
                } else {
                    // DVV downloads at build time via OGC API
                    let ap = addr_parquet.clone();
                    let step = time_step(cc, "merge-national", || {
                        merge_national(cc, source_type, None, &ap)
                    })?;
                    steps.push(step);
                }
            } else if source_type == "linz" {
                // LINZ: download via API if LINZ_API_KEY is set, otherwise skip
                match std::env::var("LINZ_API_KEY") {
                    Ok(api_key) if !api_key.is_empty() => {
                        let dl = download_dir.clone();
                        let ap = addr_parquet.clone();
                        let step = time_step(cc, "linz-download+merge", || {
                            let gpkg_path = crate::linz::download_linz_gpkg(&api_key, &dl)?;
                            merge_national(cc, source_type, Some(&gpkg_path), &ap)
                        })?;
                        steps.push(step);
                    }
                    _ => {
                        // Check for a pre-downloaded GeoPackage in the download dir
                        let gpkg_path = download_dir.join("nz-street-address.gpkg");
                        if gpkg_path.exists() {
                            info!("[{}] Using pre-downloaded LINZ GeoPackage: {}", cc, gpkg_path.display());
                            let ap = addr_parquet.clone();
                            let step = time_step(cc, "merge-national", || {
                                merge_national(cc, source_type, Some(&gpkg_path), &ap)
                            })?;
                            steps.push(step);
                        } else {
                            info!("[{}] Skipping LINZ addresses (set LINZ_API_KEY to download, or place nz-street-address.gpkg in {})", cc, download_dir.display());
                        }
                    }
                }
            } else if source_type == "ban" {
                // BAN (Base Adresse Nationale): the merge step downloads
                // 100+ per-département CSVs from adresse.data.gouv.fr.
                // No central URL to pre-download, so the merge function
                // pulls them on first call and caches in the download dir.
                // Skipping this branch left France without its 26M
                // addresses — the rebuild silently dropped national
                // coverage.
                if skip_download {
                    info!("[{}] Skipping BAN merge (--skip-download, requires network access)", cc);
                } else {
                    let ban_dir = download_dir.join("ban");
                    let dummy_path = ban_dir.join("adresses-ban.csv.gz");
                    let ap = addr_parquet.clone();
                    let step = time_step(cc, "merge-national", || {
                        merge_national(cc, source_type, Some(&dummy_path), &ap)
                    })?;
                    steps.push(step);
                }
            } else if let Some(ref url) = national.url {
                let ss = cs.national.get_or_insert_with(Default::default);
                let nat_path = rt.block_on(download_source(client, url, &download_dir, ss, skip_download))?;

                let ap = addr_parquet.clone();
                let step = time_step(cc, "merge-national", || {
                    merge_national(cc, source_type, Some(&nat_path), &ap)
                })?;
                steps.push(step);

                delete_file_if_exists(&nat_path, "national download", !cleanup);
            }
        }

        // Step 2b: Places source (SSR / DAGI / GN250 / BD TOPO)
        // → merge into places.parquet.
        if let Some(ref ps) = config.places_source {
            let source_type = ps.source_type.clone();
            let pp = places_parquet.clone();
            let ss = cs.places_source.get_or_insert_with(Default::default);

            if source_type == "bdtopo" {
                // BD TOPO ships per-département (101 separate .7z archives,
                // 200–500 MB each, ~2 GB extracted). Streaming the catalog
                // one département at a time keeps peak disk usage to ~3 GB
                // regardless of how many we ingest, and lets the per-zone
                // editionDate in state.zones drive change detection.
                // Mirrors the per-département flow used by BAN for addresses.
                let dl_dir = download_dir.clone();
                let cc_owned = cc.to_string();
                let zones_in: std::collections::HashMap<String, String> =
                    ss.zones.clone().unwrap_or_default();
                let pp_for_step = pp.clone();
                let (step, zones_out) = time_step_with(cc, "merge-places-source", || {
                    let mut zones = zones_in;
                    let new_places = crate::bdtopo::read_bdtopo_streaming(
                        &cc_owned,
                        &mut zones,
                        &dl_dir,
                        skip_download,
                    )?;
                    let existing = if pp_for_step.exists() {
                        super::read_osm_places(&pp_for_step)?
                    } else {
                        vec![]
                    };
                    // SSR's spatial+name dedup is generic over any
                    // geometry-rich place source — reuse it.
                    let merged = crate::ssr::merge_ssr_places(&existing, &new_places);
                    crate::photon::write_places_parquet(&merged, &pp_for_step)?;
                    let summary = format!(
                        "+{} places ({} BD TOPO total)",
                        merged.len() - existing.len(),
                        new_places.len(),
                    );
                    Ok((zones, summary))
                })?;
                ss.zones = Some(zones_out);
                steps.push(step);
            } else {
                // Single-file flow: SSR (one GML zip), DAGI (one JSON),
                // GN250 (one CSV zip). Download once → merge once.
                let gml_path = if let Some(ref local) = ps.local_path {
                    PathBuf::from(local)
                } else if let Some(ref url) = ps.url {
                    rt.block_on(download_source(client, url, &download_dir, ss, skip_download))?
                } else {
                    bail!("[{}] places_source has no url or local_path", cc);
                };

                let step = time_step(cc, "merge-places-source", || {
                    merge_places_source(cc, &source_type, &gml_path, &pp)
                })?;
                steps.push(step);

                // Only delete if it was a download (not a local path)
                if ps.local_path.is_none() {
                    delete_file_if_exists(&gml_path, "places source download", !cleanup);
                }
            }
        }

        // Step 3: Download Photon → merge → delete download
        if let Some(ref photon) = config.photon {
            let ss = cs.photon.get_or_insert_with(Default::default);
            let photon_path = rt.block_on(download_source(client, &photon.url, &download_dir, ss, skip_download))?;

            let pp = places_parquet.clone();
            let ap = addr_parquet.clone();
            let id = index_dir.clone();
            let step = time_step(cc, "merge-photon", || {
                merge_photon(cc, &photon_path, &pp, &ap, &id)
            })?;
            steps.push(step);

            delete_file_if_exists(&photon_path, "photon download", !cleanup);
        }

        // Step 4: Enrich
        let (step, enriched) = time_step_with(cc, "enrich", || {
            let r = crate::enrich::enrich(&places_parquet, &index_dir)?;
            let desc = format!("{} admin regions", r.admin_count);
            Ok((r, desc))
        })?;
        steps.push(step);

        // Step 5+6: Pack
        pack_indices(cc, &index_dir, &places_parquet, &addr_parquet, &enriched, &mut steps)?;

        let source_label = cs.osm.as_ref()
            .and_then(|s| s.local_path.as_ref())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("unknown"));
        write_build_meta(&index_dir, &source_label, &steps)?;
    }

    // Clean up build-only files not needed for serving.
    // admin_polygons.bin is used by enrich (point-in-polygon), admin_map.bin by pack_addr —
    // both are consumed during the build and not referenced by the API at runtime.
    for build_only in &["admin_polygons.bin", "admin_map.bin"] {
        let path = index_dir.join(build_only);
        if path.exists() {
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            match std::fs::remove_file(&path) {
                Ok(()) => info!("[{}] Deleted {} ({:.1} MB freed)", cc, build_only, size as f64 / 1e6),
                Err(e) => tracing::warn!("[{}] Failed to delete {}: {}", cc, build_only, e),
            }
        }
    }

    // Keep intermediate parquet files for incremental rebuilds (geocaching).
    // The rebuild pipeline can reuse these via --skip-extract, and the future
    // geocache diff pipeline needs them to apply OSM .osc.gz diffs.
    // To reclaim disk space, use `rebuild --cleanup`.

    // Only clean up downloads if user explicitly asked for it
    if cleanup {
        cleanup_downloads(defaults, cc);
    }

    Ok(steps)
}

/// Pack places and address indices, possibly in parallel.
fn pack_indices(
    cc: &str,
    index_dir: &Path,
    places_parquet: &Path,
    addr_parquet: &Path,
    enriched: &crate::enrich::EnrichResult,
    steps: &mut Vec<StepEntry>,
) -> Result<()> {
    let normalizer = load_normalizer(index_dir);
    let admin_map = index_dir.join("admin_map.bin");

    // Collect all address parquet files (main + national + photon extras)
    let addr_national = index_dir.join("addresses_national.parquet");
    let addr_photon = index_dir.join("addresses_photon.parquet");
    let mut addr_paths: Vec<&Path> = vec![addr_parquet];
    if addr_national.exists() { addr_paths.push(&addr_national); }
    if addr_photon.exists() { addr_paths.push(&addr_photon); }

    // Parallel pack when RAM budget allows (> 1 GB), sequential otherwise.
    // Both steps use ~200-500 MB each; combined ~700 MB which fits in > 1 GB.
    let ram = heimdall_core::node_cache::detect_memory_limit();
    let parallel = ram > 1024 * 1024 * 1024;

    if parallel {
        info!("[{}] Pack: parallel ({}MB available)", cc, ram / (1024 * 1024));
        let (pack_result, addr_result) = std::thread::scope(|s| {
            let pack_handle = s.spawn(|| {
                time_step(cc, "pack-places", || {
                    let r = crate::pack::pack(places_parquet, index_dir, enriched)?;
                    Ok(format!("{} records  fst={:.1} MB", r.record_count,
                        (r.fst_exact_bytes + r.fst_phonetic_bytes) as f64 / 1e6))
                })
            });
            let addr_handle = s.spawn(|| {
                time_step(cc, "pack-addr", || {
                    let r = crate::pack_addr::pack_addresses(
                        &addr_paths, index_dir, &admin_map, &normalizer,
                    )?;
                    Ok(format!("{} streets  fst={:.1} MB", r.street_count, r.fst_bytes as f64 / 1e6))
                })
            });
            (
                pack_handle.join().unwrap_or_else(|_| Err(anyhow::anyhow!("pack panicked"))),
                addr_handle.join().unwrap_or_else(|_| Err(anyhow::anyhow!("pack-addr panicked"))),
            )
        });
        steps.push(pack_result?);
        steps.push(addr_result?);
    } else {
        info!("[{}] Pack: sequential ({}MB available)", cc, ram / (1024 * 1024));
        let pack_step = time_step(cc, "pack-places", || {
            let r = crate::pack::pack(places_parquet, index_dir, enriched)?;
            Ok(format!("{} records  fst={:.1} MB", r.record_count,
                (r.fst_exact_bytes + r.fst_phonetic_bytes) as f64 / 1e6))
        })?;
        steps.push(pack_step);

        let addr_step = time_step(cc, "pack-addr", || {
            let r = crate::pack_addr::pack_addresses(
                &addr_paths, index_dir, &admin_map, &normalizer,
            )?;
            Ok(format!("{} streets  fst={:.1} MB", r.street_count, r.fst_bytes as f64 / 1e6))
        })?;
        steps.push(addr_step);
    }

    Ok(())
}

/// Write meta.json for a built index.
fn write_build_meta(index_dir: &Path, source: &Path, steps: &[StepEntry]) -> Result<()> {
    let total_secs: f64 = steps.iter().map(|s| s.duration_secs).sum();
    let meta = serde_json::json!({
        "version": 2,
        "source": source.display().to_string(),
        "built_at": unix_now(),
        "build_duration_secs": total_secs,
        "built_by": "rebuild",
    });
    std::fs::write(
        index_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta)?,
    )?;
    Ok(())
}

// ───────────────────────────────────────────────────────────────────────────
// Wave scheduling (first-fit-decreasing bin packing)
// ───────────────────────────────────────────────────────────────────────────

// ───────────────────────────────────────────────────────────────────────────
// Report writer
// ───────────────────────────────────────────────────────────────────────────

fn write_report(report: &BuildReport, output_dir: &str) -> Result<PathBuf> {
    let path = PathBuf::from(output_dir).join(format!(
        "rebuild-report-{}.log",
        report.started_at,
    ));

    let mut out = String::new();

    out.push_str("========================================\n");
    out.push_str(&format!(
        "REBUILD REPORT — {}\n",
        format_utc(report.started_at),
    ));
    out.push_str(&format!(
        "Mode: {} | Countries: {}\n",
        report.mode,
        report.countries_requested.join(", "),
    ));
    out.push_str("========================================\n\n");

    // Phase 1 summary
    if report.download_bytes > 0 {
        out.push_str(&format!(
            "PHASE 1: DOWNLOAD\n  Total download: {:.1} GB in {:.0}s\n\n",
            report.download_bytes as f64 / 1e9,
            report.download_secs,
        ));
    }

    // Phase 2: Build steps grouped by country
    out.push_str("PHASE 2: BUILD\n");

    let mut by_country: Vec<(String, Vec<&StepEntry>)> = Vec::new();
    for step in &report.steps {
        if let Some(entry) = by_country.last_mut() {
            if entry.0 == step.country {
                entry.1.push(step);
                continue;
            }
        }
        by_country.push((step.country.clone(), vec![step]));
    }

    for (cc, cc_steps) in &by_country {
        out.push('\n');
        let mut total_secs = 0.0;
        let mut peak_ram: u64 = 0;

        let mut total_cpu_user = 0.0f64;
        let mut total_cpu_sys = 0.0f64;

        for step in cc_steps {
            let ram_str = step
                .peak_ram_mb
                .map(|mb| format!("{:.1} GB", mb as f64 / 1024.0))
                .unwrap_or_else(|| "—".to_string());
            let cpu_str = match (step.cpu_user_secs, step.cpu_sys_secs) {
                (Some(u), Some(s)) => {
                    total_cpu_user += u;
                    total_cpu_sys += s;
                    format!("{:.0}s+{:.0}s", u, s)
                }
                _ => "—".to_string(),
            };
            out.push_str(&format!(
                "  [{}] {:<20} {:>5.0}s  {:>8} RSS  {:>10} cpu   {}\n",
                cc, step.step, step.duration_secs, ram_str, cpu_str, step.details,
            ));
            total_secs += step.duration_secs;
            if let Some(mb) = step.peak_ram_mb {
                peak_ram = peak_ram.max(mb);
            }
        }

        // Get final index disk size
        let disk_str = cc_steps.last()
            .and_then(|s| s.disk_usage_mb)
            .map(|mb| format!("{} MB on disk", mb))
            .unwrap_or_default();

        out.push_str(&format!(
            "  [{}] TOTAL              {:>5.0}s  {:>8} peak  {:>5.0}s+{:.0}s cpu   {}\n",
            cc,
            total_secs,
            format!("{:.1} GB", peak_ram as f64 / 1024.0),
            total_cpu_user,
            total_cpu_sys,
            disk_str,
        ));
    }

    // Summary
    let total_duration: f64 = report.steps.iter().map(|s| s.duration_secs).sum();
    let global_peak: u64 = report
        .steps
        .iter()
        .filter_map(|s| s.peak_ram_mb)
        .max()
        .unwrap_or(0);

    out.push_str(&format!(
        "\nSUMMARY\n  Total build time: {:.0}s\n  Peak RAM: {:.1} GB\n",
        total_duration,
        global_peak as f64 / 1024.0,
    ));

    std::fs::write(&path, &out)?;
    info!("Report written to {}", path.display());
    Ok(path)
}

// ───────────────────────────────────────────────────────────────────────────
// Cleanup
// ───────────────────────────────────────────────────────────────────────────

fn cleanup_downloads(defaults: &Defaults, cc: &str) {
    let download_dir = PathBuf::from(&defaults.download_dir).join(cc);
    if download_dir.exists() {
        match std::fs::remove_dir_all(&download_dir) {
            Ok(()) => info!("[{}] Cleaned up downloads: {}", cc, download_dir.display()),
            Err(e) => tracing::warn!("[{}] Failed to clean up {}: {}", cc, download_dir.display(), e),
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Download-only entry point
// ───────────────────────────────────────────────────────────────────────────

/// Download all sources for the requested countries without building.
/// Downloads run concurrently (up to 4 at a time). Run `rebuild` afterwards
/// to build — it will automatically reuse pre-fetched files.
pub(crate) fn run_download(
    config_path: &Path,
    state_file: &Path,
    country_filter: Option<&str>,
    redownload: bool,
    dry_run: bool,
) -> Result<()> {
    let config = load_config(config_path)?;
    let mut state = load_state(state_file)?;

    let mut countries: Vec<String> = if let Some(filter) = country_filter {
        filter.split(',').map(|s| s.trim().to_lowercase()).collect()
    } else {
        let mut all: Vec<String> = config.country.keys().cloned().collect();
        all.sort();
        all
    };

    for cc in &countries {
        if !config.country.contains_key(cc) {
            bail!(
                "Unknown country '{}'. Available: {:?}",
                cc,
                config.country.keys().collect::<Vec<_>>(),
            );
        }
    }

    countries.sort();

    let rt = tokio::runtime::Runtime::new()?;
    let client = rt.block_on(async {
        reqwest::Client::builder()
            .user_agent("heimdall-build/0.1")
            .timeout(std::time::Duration::from_secs(3600))
            .build()
    })?;

    // ── Change detection ────────────────────────────────────────────────
    let mut to_download: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();

    info!("Checking for changes...");
    for cc in &countries {
        let country_config = &config.country[cc];
        let cs = state.countries.entry(cc.clone()).or_default();
        let changed = rt.block_on(check_country_changed(&client, cc, country_config, cs, redownload));
        if changed {
            to_download.push(cc.clone());
        } else {
            skipped.push(cc.clone());
        }
    }

    save_state(&state, state_file)?;

    if !skipped.is_empty() {
        info!("Skipping (unchanged): {}", skipped.join(", "));
    }

    if to_download.is_empty() {
        info!("Nothing to download — all sources unchanged.");
        return Ok(());
    }

    // ── Collect all download tasks ──────────────────────────────────────
    // Each task is (country_code, source_kind, url, download_dir)
    let mut tasks: Vec<(String, String, String, PathBuf)> = Vec::new();

    for cc in &to_download {
        let country_config = &config.country[cc];
        let download_dir = PathBuf::from(&config.defaults.download_dir).join(cc);

        if let Some(ref osm) = country_config.osm {
            tasks.push((cc.clone(), "osm".into(), osm.url.clone(), download_dir.clone()));
        }

        if let Some(ref photon) = country_config.photon {
            tasks.push((cc.clone(), "photon".into(), photon.url.clone(), download_dir.clone()));
        }

        if let Some(ref national) = country_config.national {
            let source_type = national.source_type.as_deref().unwrap_or("");
            // Skip sources that download at build time (DVV, LINZ, TIGER, BAN)
            match source_type {
                "dvv" | "linz" | "tiger_oa" | "ban" => {
                    info!("[{}] Skipping {} download (fetched at build time)", cc, source_type);
                }
                _ => {
                    if let Some(ref url) = national.url {
                        tasks.push((cc.clone(), "national".into(), url.clone(), download_dir.clone()));
                    }
                }
            }
        }

        if let Some(ref ps) = country_config.places_source {
            if ps.local_path.is_some() {
                info!("[{}] Skipping places_source download (local_path configured)", cc);
            } else if let Some(ref url) = ps.url {
                tasks.push((cc.clone(), "places_source".into(), url.clone(), download_dir.clone()));
            }
        }
    }

    if dry_run {
        info!("Dry run — would download {} files for: {}", tasks.len(), to_download.join(", "));
        for (cc, kind, url, _) in &tasks {
            info!("  [{}] {} → {}", cc, kind, url);
        }
        return Ok(());
    }

    info!("Downloading {} files for {} countries (up to 4 concurrent)...", tasks.len(), to_download.len());

    // ── Run downloads concurrently ──────────────────────────────────────
    let start = Instant::now();
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(4));
    let client = std::sync::Arc::new(client);

    let results: Vec<Result<(String, String, PathBuf, u64), (String, String, String)>> = rt.block_on(async {
        let mut handles = Vec::new();

        for (cc, kind, url, download_dir) in tasks {
            let sem = semaphore.clone();
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                info!("[{}] Downloading {}...", cc, kind);

                // Create a temporary SourceState for the download
                let mut ss = SourceState::default();
                match download_source(&client, &url, &download_dir, &mut ss, false).await {
                    Ok(path) => {
                        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                        info!("[{}] {} done ({:.1} MB)", cc, kind, size as f64 / 1e6);
                        Ok((cc, kind, path, size))
                    }
                    Err(e) => {
                        tracing::error!("[{}] {} FAILED: {:#}", cc, kind, e);
                        Err((cc, kind, format!("{:#}", e)))
                    }
                }
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        results
    });

    // ── Update state with download paths ────────────────────────────────
    let mut total_bytes: u64 = 0;
    let mut errors: Vec<(String, String, String)> = Vec::new();
    let mut ok_count = 0;

    for result in results {
        match result {
            Ok((cc, kind, path, size)) => {
                total_bytes += size;
                ok_count += 1;
                let cs = state.countries.entry(cc).or_default();
                let local = path.to_string_lossy().to_string();
                match kind.as_str() {
                    "osm" => { cs.osm.get_or_insert_with(Default::default).local_path = Some(local); }
                    "photon" => { cs.photon.get_or_insert_with(Default::default).local_path = Some(local); }
                    "national" => { cs.national.get_or_insert_with(Default::default).local_path = Some(local); }
                    "places_source" => { cs.places_source.get_or_insert_with(Default::default).local_path = Some(local); }
                    _ => {}
                }
            }
            Err((cc, kind, msg)) => {
                errors.push((cc, kind, msg));
            }
        }
    }

    save_state(&state, state_file)?;

    let elapsed = start.elapsed();
    info!("========================================");
    info!(
        "DOWNLOAD COMPLETE: {} files ({:.1} GB) in {:.0}s, {} failed",
        ok_count,
        total_bytes as f64 / 1e9,
        elapsed.as_secs_f64(),
        errors.len(),
    );

    if !errors.is_empty() {
        for (cc, kind, msg) in &errors {
            tracing::error!("  [{}] {}: {}", cc, kind, msg);
        }
        bail!("{} download(s) failed", errors.len());
    }

    Ok(())
}

// ───────────────────────────────────────────────────────────────────────────
// Rebuild entry point
// ───────────────────────────────────────────────────────────────────────────

pub(crate) fn run_rebuild(
    config_path: &Path,
    state_file: &Path,
    country_filter: Option<&str>,
    redownload: bool,
    dry_run: bool,
    skip_download: bool,
    cleanup: bool,
    min_population: u32,
    jobs: usize,
    ram_budget: u64,
) -> Result<()> {
    let config = load_config(config_path)?;
    let mut state = load_state(state_file)?;

    // Filter countries
    let mut countries: Vec<String> = if let Some(filter) = country_filter {
        filter
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .collect()
    } else {
        let mut all: Vec<String> = config.country.keys().cloned().collect();
        all.sort();
        all
    };

    // Validate
    for cc in &countries {
        if !config.country.contains_key(cc) {
            bail!(
                "Unknown country '{}'. Available: {:?}",
                cc,
                config.country.keys().collect::<Vec<_>>(),
            );
        }
    }

    countries.sort();
    info!("Rebuild: countries=[{}]", countries.join(", "));

    let rt = tokio::runtime::Runtime::new()?;
    let client = rt.block_on(async {
        reqwest::Client::builder()
            .user_agent("heimdall-build/0.1")
            .timeout(std::time::Duration::from_secs(3600))
            .build()
    })?;

    let mut report = BuildReport::new("default", &countries);

    // ── Check changes for all countries (cheap HTTP, no downloads) ────────

    let mut to_build: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();

    if skip_download {
        // --skip-download: no network at all, build everything requested
        info!("Skipping change detection (--skip-download)");
        to_build = countries.clone();
    } else {
        info!("Checking for changes...");
        for cc in &countries {
            let country_config = &config.country[cc];
            let cs = state.countries.entry(cc.clone()).or_default();
            let changed = rt.block_on(check_country_changed(&client, cc, country_config, cs, redownload));

            // Also rebuild if index is missing or incomplete (no meta.json = failed/corrupt build)
            let index_dir = PathBuf::from(&config.defaults.index_dir).join(&country_config.index_name);
            let index_complete = index_dir.join("meta.json").exists();

            if changed || !index_complete {
                if !changed && !index_complete {
                    info!("[{}] sources unchanged but index incomplete (no meta.json) — rebuilding", cc);
                }
                to_build.push(cc.clone());
            } else {
                skipped.push(cc.clone());
            }
        }
    }

    if !dry_run {
        save_state(&state, state_file)?;
    }

    if !skipped.is_empty() {
        info!("Skipping (unchanged): {}", skipped.join(", "));
    }

    if to_build.is_empty() {
        info!("Nothing to rebuild — all sources unchanged.");
        return Ok(());
    }

    if dry_run {
        info!("Dry run — would rebuild: {}", to_build.join(", "));
        return Ok(());
    }

    // ── Pre-download phase: download all sources in parallel ────────────
    // Downloads are I/O bound, not RAM bound. Run them all concurrently
    // so builds can start immediately from local files.
    if !skip_download {
        info!("Downloading sources for {} countries in parallel...", to_build.len());
        let download_start = Instant::now();
        let mut download_futures: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>> = Vec::new();

        for cc in &to_build {
            let country_config = &config.country[cc];
            let download_dir = PathBuf::from(&config.defaults.download_dir).join(cc);
            std::fs::create_dir_all(&download_dir).ok();

            // Collect all URLs for this country
            if let Some(ref osm) = country_config.osm {
                let url = osm.url.clone();
                let dir = download_dir.clone();
                let client = client.clone();
                let cc2 = cc.clone();
                let mut ss = state.countries.entry(cc.clone()).or_default()
                    .osm.clone().unwrap_or_default();
                download_futures.push(Box::pin(async move {
                    match download_source(&client, &url, &dir, &mut ss, false).await {
                        Ok(p) => info!("[{}] OSM: {}", cc2, p.file_name().unwrap().to_string_lossy()),
                        Err(e) => tracing::warn!("[{}] OSM download failed: {}", cc2, e),
                    }
                }));
            }
            if let Some(ref photon) = country_config.photon {
                let url = photon.url.clone();
                let dir = download_dir.clone();
                let client = client.clone();
                let cc2 = cc.clone();
                let mut ss = state.countries.entry(cc.clone()).or_default()
                    .photon.clone().unwrap_or_default();
                download_futures.push(Box::pin(async move {
                    match download_source(&client, &url, &dir, &mut ss, false).await {
                        Ok(p) => info!("[{}] Photon: {}", cc2, p.file_name().unwrap().to_string_lossy()),
                        Err(e) => tracing::warn!("[{}] Photon download failed: {}", cc2, e),
                    }
                }));
            }
            if let Some(ref national) = country_config.national {
                if let Some(ref url) = national.url {
                    let url = url.clone();
                    let dir = download_dir.clone();
                    let client = client.clone();
                    let cc2 = cc.clone();
                    let mut ss = state.countries.entry(cc.clone()).or_default()
                        .national.clone().unwrap_or_default();
                    download_futures.push(Box::pin(async move {
                        match download_source(&client, &url, &dir, &mut ss, false).await {
                            Ok(p) => info!("[{}] National: {}", cc2, p.file_name().unwrap().to_string_lossy()),
                            Err(e) => tracing::warn!("[{}] National download failed: {}", cc2, e),
                        }
                    }));
                }
            }
        }

        // Run all downloads concurrently (bounded by reqwest connection pool)
        let total_downloads = download_futures.len();
        rt.block_on(async {
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(8)); // 8 concurrent
            let mut handles = Vec::new();
            for fut in download_futures {
                let sem = semaphore.clone();
                handles.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    fut.await;
                }));
            }
            for h in handles {
                let _ = h.await;
            }
        });

        let dl_secs = download_start.elapsed().as_secs_f64();
        info!("Downloads complete: {} sources in {:.0}s", total_downloads, dl_secs);
        report.download_secs = dl_secs;
    }

    // ── Build each country (downloads already local) ─────────────────────

    let actual_jobs = jobs.max(1).min(to_build.len());
    info!("Building {} countries with {} parallel job(s){}...",
        to_build.len(), actual_jobs,
        if ram_budget > 0 { format!(", RAM budget {}GB", ram_budget) } else { String::new() }
    );

    let errors: Vec<(String, String)>;

    if actual_jobs <= 1 {
        // ── Sequential build (original behavior) ──
        let mut errs = Vec::new();
        for (i, cc) in to_build.iter().enumerate() {
            info!("────── [{}/{}] {} ──────", i + 1, to_build.len(), cc.to_uppercase());
            let country_config = &config.country[cc];
            let cs = state.countries.entry(cc.clone()).or_default();
            match build_country_pipeline(
                cc, country_config, cs, &config.defaults,
                min_population, skip_download, cleanup, &rt, &client,
            ) {
                Ok(mut step_entries) => {
                    let idx_dir = PathBuf::from(&config.defaults.index_dir).join(&country_config.index_name);
                    if let Some(last) = step_entries.last_mut() {
                        last.disk_usage_mb = get_dir_size_mb(&idx_dir);
                    }
                    info!("[{}] Build complete!", cc);
                    report.steps.extend(step_entries);
                    cs.last_built = Some(unix_now());
                }
                Err(e) => {
                    tracing::error!("[{}] Build FAILED: {:#}", cc, e);
                    errs.push((cc.clone(), format!("{:#}", e)));
                }
            }
            save_state(&state, state_file)?;
        }
        errors = errs;
    } else {
        // ── Parallel build ──
        // Sort by estimated RAM descending (greedy bin-packing: start biggest first)
        let mut sorted_build: Vec<String> = to_build.clone();
        sorted_build.sort_by(|a, b| {
            let ram_a = config.country.get(a).map(|c| c.ram_gb).unwrap_or(1);
            let ram_b = config.country.get(b).map(|c| c.ram_gb).unwrap_or(1);
            ram_b.cmp(&ram_a)
        });

        let queue = std::sync::Mutex::new(sorted_build.into_iter().enumerate());
        let state_mutex = std::sync::Mutex::new(&mut state);
        let report_mutex = std::sync::Mutex::new(&mut report);
        let error_mutex = std::sync::Mutex::new(Vec::<(String, String)>::new());
        let active_ram = std::sync::atomic::AtomicU64::new(0);
        let total = to_build.len();

        std::thread::scope(|s| {
            for _ in 0..actual_jobs {
                s.spawn(|| {
                    loop {
                        let (i, cc) = {
                            let mut q = queue.lock().unwrap();
                            match q.next() {
                                Some(item) => item,
                                None => return,
                            }
                        };

                        // RAM budget check: wait if over budget
                        let country_config = &config.country[&cc];
                        let est_ram_mb = (country_config.ram_gb as u64) * 1024;
                        if ram_budget > 0 {
                            let budget_mb = ram_budget * 1024;
                            loop {
                                let current = active_ram.load(std::sync::atomic::Ordering::Relaxed);
                                if current + est_ram_mb <= budget_mb || current == 0 {
                                    active_ram.fetch_add(est_ram_mb, std::sync::atomic::Ordering::Relaxed);
                                    break;
                                }
                                std::thread::sleep(std::time::Duration::from_secs(2));
                            }
                        } else {
                            active_ram.fetch_add(est_ram_mb, std::sync::atomic::Ordering::Relaxed);
                        }

                        info!("────── [{}/{}] {} (parallel slot) ──────", i + 1, total, cc.to_uppercase());

                        let mut cs = CountryState::default();
                        let result = build_country_pipeline(
                            &cc, country_config, &mut cs, &config.defaults,
                            min_population, skip_download, cleanup, &rt, &client,
                        );

                        active_ram.fetch_sub(est_ram_mb, std::sync::atomic::Ordering::Relaxed);

                        match result {
                            Ok(mut step_entries) => {
                                let idx_dir = PathBuf::from(&config.defaults.index_dir).join(&country_config.index_name);
                                if let Some(last) = step_entries.last_mut() {
                                    last.disk_usage_mb = get_dir_size_mb(&idx_dir);
                                }
                                info!("[{}] Build complete!", cc);
                                {
                                    let mut rpt = report_mutex.lock().unwrap();
                                    rpt.steps.extend(step_entries);
                                }
                                {
                                    let mut st = state_mutex.lock().unwrap();
                                    let entry = st.countries.entry(cc.clone()).or_default();
                                    *entry = cs;
                                    entry.last_built = Some(unix_now());
                                    save_state(&**st, state_file).ok();
                                }
                            }
                            Err(e) => {
                                tracing::error!("[{}] Build FAILED: {:#}", cc, e);
                                error_mutex.lock().unwrap().push((cc.clone(), format!("{:#}", e)));
                            }
                        }
                    }
                });
            }
        });

        errors = error_mutex.into_inner().unwrap();
    }

    // ── Refresh global FST so the API's fast path stays consistent ──────
    //
    // The global FST stores posting lists keyed to per-country record IDs.
    // Re-packing any country renumbers its records, so a partial rebuild
    // leaves the global FST holding stale offsets — the API hands back
    // garbage results until the global FST catches up. This bit us in
    // production after a `--country dk,no,se,fi` run: prod returned
    // "Stockholm → Kåseberga, Ystads kommun" until the global was rebuilt.
    //
    // Skipped only on dry runs (which return earlier anyway) and when
    // every country failed (no point — global would be inconsistent
    // with the still-old per-country indices).
    let built_count = to_build.len() - errors.len();
    if built_count > 0 {
        let data_dir = PathBuf::from(&config.defaults.index_dir);
        let global_dir = data_dir.join("global");
        info!("Refreshing global FST → {}", global_dir.display());
        let t0 = Instant::now();
        match crate::build_global_fst(&data_dir, &global_dir) {
            Ok(()) => info!(
                "Global FST refreshed in {:.0}s",
                t0.elapsed().as_secs_f64()
            ),
            Err(e) => tracing::error!(
                "Global FST refresh failed: {} — \
                 prod will return stale results until rebuilt manually",
                e
            ),
        }
    }

    // ── Summary ──────────────────────────────────────────────────────────

    write_report(&report, &config.defaults.index_dir)?;

    info!("========================================");
    info!(
        "SUMMARY: {} rebuilt, {} skipped, {} failed",
        built_count,
        skipped.len(),
        errors.len(),
    );

    if !errors.is_empty() {
        for (cc, msg) in &errors {
            tracing::error!("  [{}] {}", cc, msg);
        }
        bail!("{} country build(s) failed", errors.len());
    }

    Ok(())
}
