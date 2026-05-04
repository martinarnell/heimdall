/// sampling.rs — Query sampling utilities: population weighting, name variants,
/// fuzzy mutation, city resolution, ambiguous name detection.

use std::collections::HashMap;

use heimdall_core::addr_store::AddrStore;
use heimdall_core::record_store::RecordStore;
use heimdall_core::types::{Coord, PlaceType};
use rand::prelude::*;

// ---------------------------------------------------------------------------
// Population weights (approximate, in millions)
// ---------------------------------------------------------------------------

/// Returns approximate population in millions for population-weighted sampling.
/// Countries with national address data get real weights; Photon-only countries
/// default to 1.0.
pub fn population_millions(cc: &str) -> f64 {
    match cc.to_uppercase().as_str() {
        "US" => 331.0,
        "BR" => 214.0,
        "JP" => 125.0,
        "DE" => 83.0,
        "GB" => 67.0,
        "FR" => 67.0,
        "IT" => 60.0,
        "KR" => 52.0,
        "ES" => 47.0,
        "PL" => 38.0,
        "CA" => 38.0,
        "AU" => 26.0,
        "NL" => 17.5,
        "BE" => 11.5,
        "CZ" => 10.7,
        "SE" => 10.4,
        "AT" => 9.0,
        "CH" => 8.8,
        "NO" => 5.5,
        "DK" => 5.8,
        "FI" => 5.5,
        "NZ" => 5.1,
        "EE" => 1.3,
        "LV" => 1.9,
        "LT" => 2.8,
        _ => 1.0,
    }
}

/// Compute normalized weights for a set of country codes.
pub fn compute_weights(countries: &[String]) -> Vec<(String, f64, f64)> {
    let raw: Vec<(String, f64)> = countries
        .iter()
        .map(|cc| (cc.clone(), population_millions(cc)))
        .collect();
    let total: f64 = raw.iter().map(|(_, p)| p).sum();
    if total <= 0.0 {
        return Vec::new();
    }
    raw.into_iter()
        .map(|(cc, pop)| {
            let weight = pop / total;
            (cc, pop, weight)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Diacritic stripping
// ---------------------------------------------------------------------------

/// Strip combining marks (Unicode Mn category) to produce a diacritic-free variant.
/// Returns None if the result is identical to the input.
pub fn strip_diacritics(name: &str) -> Option<String> {
    // NFD decompose, then strip Mn (combining marks)
    let nfd: String = unicode_nfd(name);
    let stripped: String = nfd
        .chars()
        .filter(|c| !is_combining_mark(*c))
        .collect();
    if stripped == name {
        None
    } else {
        Some(stripped)
    }
}

/// Simple NFD decomposition for Latin characters.
fn unicode_nfd(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        // Common Latin decompositions
        match c {
            'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' => { result.push('a'); result.push(combining_for(c)); }
            'è' | 'é' | 'ê' | 'ë' => { result.push('e'); result.push(combining_for(c)); }
            'ì' | 'í' | 'î' | 'ï' => { result.push('i'); result.push(combining_for(c)); }
            'ò' | 'ó' | 'ô' | 'õ' | 'ö' => { result.push('o'); result.push(combining_for(c)); }
            'ù' | 'ú' | 'û' | 'ü' => { result.push('u'); result.push(combining_for(c)); }
            'ý' | 'ÿ' => { result.push('y'); result.push(combining_for(c)); }
            'ñ' => { result.push('n'); result.push('\u{0303}'); }
            'ç' => { result.push('c'); result.push('\u{0327}'); }
            'ø' => result.push_str("o"),  // ø doesn't decompose in NFD, just strip
            'æ' => result.push_str("ae"),
            'ð' => result.push('d'),
            'þ' => result.push_str("th"),
            'ß' => result.push_str("ss"),
            'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' => { result.push('A'); result.push(combining_for(c)); }
            'È' | 'É' | 'Ê' | 'Ë' => { result.push('E'); result.push(combining_for(c)); }
            'Ì' | 'Í' | 'Î' | 'Ï' => { result.push('I'); result.push(combining_for(c)); }
            'Ò' | 'Ó' | 'Ô' | 'Õ' | 'Ö' => { result.push('O'); result.push(combining_for(c)); }
            'Ù' | 'Ú' | 'Û' | 'Ü' => { result.push('U'); result.push(combining_for(c)); }
            'Ý' => { result.push('Y'); result.push('\u{0301}'); }
            'Ñ' => { result.push('N'); result.push('\u{0303}'); }
            'Ç' => { result.push('C'); result.push('\u{0327}'); }
            'Ø' => result.push_str("O"),
            'Æ' => result.push_str("AE"),
            'Ð' => result.push('D'),
            'Þ' => result.push_str("TH"),
            _ => result.push(c),
        }
    }
    result
}

fn combining_for(c: char) -> char {
    match c {
        'à' | 'è' | 'ì' | 'ò' | 'ù' | 'À' | 'È' | 'Ì' | 'Ò' | 'Ù' => '\u{0300}', // grave
        'á' | 'é' | 'í' | 'ó' | 'ú' | 'ý' | 'Á' | 'É' | 'Í' | 'Ó' | 'Ú' | 'Ý' => '\u{0301}', // acute
        'â' | 'ê' | 'î' | 'ô' | 'û' | 'Â' | 'Ê' | 'Î' | 'Ô' | 'Û' => '\u{0302}', // circumflex
        'ã' | 'õ' | 'Ã' | 'Õ' => '\u{0303}', // tilde
        'ä' | 'ë' | 'ï' | 'ö' | 'ü' | 'ÿ' | 'Ä' | 'Ë' | 'Ï' | 'Ö' | 'Ü' => '\u{0308}', // diaeresis
        'å' | 'Å' => '\u{030A}', // ring above
        _ => '\u{0000}',
    }
}

fn is_combining_mark(c: char) -> bool {
    let cp = c as u32;
    // Unicode combining diacritical marks block: U+0300..U+036F
    (0x0300..=0x036F).contains(&cp)
        // Combining diacritical marks extended: U+1AB0..U+1AFF
        || (0x1AB0..=0x1AFF).contains(&cp)
        // Combining diacritical marks supplement: U+1DC0..U+1DFF
        || (0x1DC0..=0x1DFF).contains(&cp)
}

// ---------------------------------------------------------------------------
// English alias lookup
// ---------------------------------------------------------------------------

/// Find an English alias for a place from its alt names or known_variants.
/// Returns None if no English name found or it's the same as primary.
pub fn find_english_alias(
    primary_name: &str,
    record: &heimdall_core::types::PlaceRecord,
    records: &RecordStore,
    known_variants_reverse: &HashMap<String, Vec<String>>,
) -> Option<String> {
    // Strategy 1: Check alt_names from RecordStore (includes name:en from OSM)
    let all_names = records.all_names(record);
    for alt in &all_names[1..] {
        // Heuristic: if the alt name is ASCII-only, differs from primary,
        // and doesn't contain language prefixes (e.g. "de=", "sv="), it's
        // likely an English/international name.
        if alt.as_str() != primary_name
            && alt.is_ascii()
            && alt.len() >= 3
            && !alt.contains('=')
            && !alt.contains(':')
        {
            return Some(alt.clone());
        }
    }

    // Strategy 2: Check reverse known_variants map
    // known_variants maps english→canonical, so reverse maps canonical→[english_names]
    let lower = primary_name.to_lowercase();
    if let Some(english_names) = known_variants_reverse.get(&lower) {
        for en in english_names {
            if en != &lower {
                return Some(en.clone());
            }
        }
    }

    None
}

/// Build a reverse map: canonical → [variant_names] from known_variants.
pub fn build_reverse_variants(known_variants: &HashMap<String, String>) -> HashMap<String, Vec<String>> {
    let mut reverse: HashMap<String, Vec<String>> = HashMap::new();
    for (variant, canonical) in known_variants {
        reverse
            .entry(canonical.clone())
            .or_default()
            .push(variant.clone());
    }
    reverse
}

// ---------------------------------------------------------------------------
// Fuzzy mutation
// ---------------------------------------------------------------------------

/// Apply a random typo mutation to a name.
pub fn mutate_name(name: &str, rng: &mut StdRng) -> String {
    let chars: Vec<char> = name.chars().collect();
    if chars.len() <= 1 {
        return name.to_string();
    }

    match rng.gen_range(0..3) {
        0 => {
            // Delete a random char
            let idx = rng.gen_range(0..chars.len());
            chars
                .iter()
                .enumerate()
                .filter(|&(i, _)| i != idx)
                .map(|(_, &c)| c)
                .collect()
        }
        1 => {
            // Swap two adjacent chars
            let mut result = chars;
            let idx = rng.gen_range(0..result.len() - 1);
            result.swap(idx, idx + 1);
            result.into_iter().collect()
        }
        _ => {
            // Substitute a random char
            let idx = rng.gen_range(0..chars.len());
            let replacement = (b'a' + rng.gen_range(0..26u8)) as char;
            let mut result = chars;
            result[idx] = replacement;
            result.into_iter().collect()
        }
    }
}

// ---------------------------------------------------------------------------
// City resolution
// ---------------------------------------------------------------------------

/// Find the nearest city/town name for use in address queries.
pub fn nearest_city_name(coord: Coord, records: &RecordStore) -> Option<String> {
    let mut best_name: Option<String> = None;
    let mut best_dist = f64::MAX;
    let mut best_priority = 0u8;

    let len = records.len();
    let step = if len > 50_000 { len / 50_000 } else { 1 };

    for i in (0..len).step_by(step) {
        let record = match records.get(i as u32) {
            Ok(r) => r,
            Err(_) => continue,
        };

        let priority = match record.place_type {
            PlaceType::City => 4,
            PlaceType::Town => 3,
            PlaceType::Village => 2,
            PlaceType::Suburb => 1,
            _ => continue,
        };

        let dist = coord.distance_m(&record.coord);

        if dist > 30_000.0 {
            continue;
        }

        if priority > best_priority || (priority == best_priority && dist < best_dist) {
            best_priority = priority;
            best_dist = dist;
            best_name = Some(records.primary_name(&record));
        }
    }

    best_name
}

// ---------------------------------------------------------------------------
// Ambiguous name detection
// ---------------------------------------------------------------------------

/// Well-known globally ambiguous place names (supplement auto-detected ones).
const HARDCODED_AMBIGUOUS: &[&str] = &[
    "Springfield", "Richmond", "Victoria", "Alexandria", "Georgetown",
    "Santiago", "San Jose", "Portland", "Columbus", "Hamilton",
    "Kingston", "Cambridge", "Oxford", "Manchester", "Birmingham",
    "Newcastle", "Bergen", "Lund", "Vik", "Freiburg",
    "Frankfurt", "Cordoba", "Valencia", "Leon", "Merida",
    "San Fernando", "Santa Cruz", "Monterey", "Dublin", "Perth",
    "Windsor", "Chester", "Lincoln", "Wellington", "Nelson",
];

/// Find place names that appear in 2+ countries (City/Town/Village only).
/// Returns up to `max` names.
pub fn find_ambiguous_names(
    country_records: &[(String, RecordStore)],
    max: usize,
) -> Vec<String> {
    // name → set of countries where it appears
    let mut name_countries: HashMap<String, Vec<String>> = HashMap::new();

    for (cc, records) in country_records {
        let len = records.len();
        let step = if len > 100_000 { len / 100_000 } else { 1 };

        for i in (0..len).step_by(step) {
            let record = match records.get(i as u32) {
                Ok(r) => r,
                Err(_) => continue,
            };

            match record.place_type {
                PlaceType::City | PlaceType::Town | PlaceType::Village => {}
                _ => continue,
            }

            let name = records.primary_name(&record);
            if name.len() < 3 {
                continue;
            }

            let entry = name_countries
                .entry(name)
                .or_default();
            if !entry.contains(cc) {
                entry.push(cc.clone());
            }
        }
    }

    // Collect names appearing in 2+ countries, sorted by number of countries (desc)
    let mut ambiguous: Vec<(String, usize)> = name_countries
        .into_iter()
        .filter(|(_, countries)| countries.len() >= 2)
        .map(|(name, countries)| (name, countries.len()))
        .collect();
    ambiguous.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    // Add hardcoded fallbacks that might not be in loaded indices
    let mut result: Vec<String> = ambiguous.into_iter().map(|(n, _)| n).collect();
    for &name in HARDCODED_AMBIGUOUS {
        if !result.iter().any(|n| n.eq_ignore_ascii_case(name)) {
            result.push(name.to_owned());
        }
    }

    result.truncate(max);
    result
}

// ---------------------------------------------------------------------------
// Country data (for query sampling)
// ---------------------------------------------------------------------------

pub struct CountryData {
    pub code: String,
    pub records: RecordStore,
    pub addr_store: Option<AddrStore>,
}

/// Detect country code from index directory name.
pub fn detect_country_code(dir_name: &str) -> String {
    // Try to extract 2-letter code from "index-XX" pattern, allowing optional
    // suffixes like "-dev", "-ngram", "-realm" used by dev/benchmark indices.
    if let Some(suffix) = dir_name.strip_prefix("index-") {
        let head = suffix.split('-').next().unwrap_or("");
        if head.len() == 2 && head.chars().all(|c| c.is_ascii_lowercase()) {
            return head.to_uppercase();
        }
    }
    // Fallback patterns
    if dir_name.contains("denmark") || dir_name.ends_with("-dk") {
        "DK".into()
    } else if dir_name.contains("germany") || dir_name.ends_with("-de") {
        "DE".into()
    } else if dir_name.contains("norway") || dir_name.ends_with("-no") {
        "NO".into()
    } else if dir_name.contains("sweden") || dir_name.ends_with("-se") {
        "SE".into()
    } else if dir_name.contains("finland") || dir_name.ends_with("-fi") {
        "FI".into()
    } else if dir_name.contains("britain") || dir_name.ends_with("-gb") || dir_name.ends_with("-uk") {
        "GB".into()
    } else if dir_name.contains("australia") || dir_name.ends_with("-au") {
        "AU".into()
    } else if dir_name.contains("canada") || dir_name.ends_with("-ca") {
        "CA".into()
    } else if dir_name.contains("france") || dir_name.ends_with("-fr") {
        "FR".into()
    } else if dir_name.contains("netherlands") || dir_name.ends_with("-nl") {
        "NL".into()
    } else if dir_name.contains("japan") || dir_name.ends_with("-jp") {
        "JP".into()
    } else if dir_name.contains("brazil") || dir_name.ends_with("-br") {
        "BR".into()
    } else if dir_name.contains("united-states") || dir_name.ends_with("-us") {
        "US".into()
    } else {
        "XX".into()
    }
}

/// Load a country's data from an index directory.
pub fn load_country(index_path: &std::path::Path) -> anyhow::Result<CountryData> {
    let records = RecordStore::open(&index_path.join("records.bin"))?;
    let addr_store = AddrStore::open(&index_path.join("addr_streets.bin"))?;

    let dir_name = index_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    let code = detect_country_code(dir_name);

    Ok(CountryData {
        code,
        records,
        addr_store,
    })
}
