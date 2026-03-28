//! Manifest types for index package distribution.
//! Matches the schema produced by `heimdall-build package`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub version: String,
    pub date: String,
    pub base_url: String,
    pub countries: HashMap<String, CountryPackage>,
    pub bundles: HashMap<String, BundlePackage>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CountryPackage {
    pub file: String,
    pub sha256: String,
    pub size: u64,
    pub places: usize,
    pub addresses: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BundlePackage {
    pub file: String,
    pub sha256: String,
    pub size: u64,
    pub countries: Vec<String>,
}

/// Known bundles and their country codes (must match heimdall-build package.rs).
pub fn bundle_countries(name: &str) -> Option<Vec<&'static str>> {
    match name {
        "nordic" => Some(vec!["se", "no", "dk", "fi"]),
        "europe" => Some(vec!["se", "no", "dk", "fi", "de", "gb"]),
        "world" => None, // dynamic — all countries in manifest
        _ => None,
    }
}

pub fn country_name(code: &str) -> &str {
    match code {
        "se" => "Sweden",
        "no" => "Norway",
        "dk" => "Denmark",
        "fi" => "Finland",
        "de" => "Germany",
        "gb" => "Great Britain",
        "us" => "United States",
        _ => code,
    }
}

/// Check if a string is a known bundle name.
pub fn is_bundle(name: &str) -> bool {
    matches!(name, "nordic" | "europe" | "world")
}
