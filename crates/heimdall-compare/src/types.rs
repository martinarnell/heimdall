/// types.rs — Shared types for the heimdall-compare benchmark framework.

use heimdall_core::types::Coord;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// JSONL query entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEntry {
    pub id: String,
    /// The query string (null for reverse queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    pub category: String,
    /// Country code (null for ambiguous queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    /// For reverse queries: the query latitude
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lat: Option<f64>,
    /// For reverse queries: the query longitude
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lon: Option<f64>,
    /// Ground truth latitude (null for ambiguous queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_lat: Option<f64>,
    /// Ground truth longitude (null for ambiguous queries)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_lon: Option<f64>,
    /// Parent query ID for variant queries (diacritic-free, English alias)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant_of: Option<String>,
    /// Type of variant: "diacritic_free", "english_alias"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant_type: Option<String>,
}

// ---------------------------------------------------------------------------
// JSONL metadata header (first line of query file)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFileMeta {
    pub _meta: MetaInner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaInner {
    pub version: u32,
    pub seed: u64,
    pub total_queries: usize,
    pub categories: CategoryCounts,
    pub population_weights: Vec<CountryWeight>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryCounts {
    pub address: usize,
    pub place: usize,
    pub fuzzy: usize,
    pub reverse: usize,
    pub ambiguous: usize,
    pub variants: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountryWeight {
    pub code: String,
    pub population_millions: f64,
    pub weight: f64,
    pub query_count: usize,
}

// ---------------------------------------------------------------------------
// Query result from a geocoder
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct QueryResult {
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub display_name: Option<String>,
    pub latency_ms: u64,
    pub is_error: bool,
}

// ---------------------------------------------------------------------------
// Category enum — comparison result classification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Agree,
    Close,
    Diverge,
    Conflict,
    MissH,
    MissN,
    BothMiss,
    HeimdallError,
    NominatimError,
}

impl Category {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Agree => "AGREE",
            Self::Close => "CLOSE",
            Self::Diverge => "DIVERGE",
            Self::Conflict => "CONFLICT",
            Self::MissH => "MISS_H",
            Self::MissN => "MISS_N",
            Self::BothMiss => "BOTH_MISS",
            Self::HeimdallError => "HEIMDALL_ERROR",
            Self::NominatimError => "NOMINATIM_ERROR",
        }
    }

    pub const ALL: &'static [&'static str] = &[
        "AGREE",
        "CLOSE",
        "DIVERGE",
        "CONFLICT",
        "MISS_H",
        "MISS_N",
        "BOTH_MISS",
        "HEIMDALL_ERROR",
        "NOMINATIM_ERROR",
    ];
}

/// Classify comparison result by distance between Heimdall and Nominatim coordinates.
pub fn categorize(
    heimdall: Option<(f64, f64)>,
    nominatim: Option<(f64, f64)>,
) -> (Category, Option<f64>) {
    match (heimdall, nominatim) {
        (None, None) => (Category::BothMiss, None),
        (None, Some(_)) => (Category::MissH, None),
        (Some(_), None) => (Category::MissN, None),
        (Some(h), Some(n)) => {
            let hc = Coord::new(h.0, h.1);
            let nc = Coord::new(n.0, n.1);
            let d = hc.distance_m(&nc);
            let cat = if d < 200.0 {
                Category::Agree
            } else if d < 2000.0 {
                Category::Close
            } else if d < 20000.0 {
                Category::Diverge
            } else {
                Category::Conflict
            };
            (cat, Some(d))
        }
    }
}

/// Classify ambiguous queries: compare H vs N agreement only (no ground truth).
/// For ambiguous queries there is no "correct" answer — we only measure whether
/// both geocoders return the same place. AGREE means they agree, CONFLICT means
/// they chose different interpretations. Neither is wrong.
pub fn categorize_ambiguous(
    heimdall: Option<(f64, f64)>,
    nominatim: Option<(f64, f64)>,
) -> (Category, Option<f64>) {
    categorize(heimdall, nominatim)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn format_num(n: usize) -> String {
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

pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!("{}...", s.chars().take(max - 3).collect::<String>())
    }
}
