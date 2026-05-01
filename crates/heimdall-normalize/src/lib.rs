/// heimdall-normalize
///
/// Converts a raw input query into one or more normalized candidate strings
/// that should be tried against the FST.
///
/// The normalizer is the first line of "fuzzy" handling — many apparent
/// typos and variations are handled here before the FST ever sees the input.
///
/// Configuration is loaded from a per-country TOML (sv.toml, de.toml, etc).
/// Falls back to hardcoded defaults if the file isn't found.

use std::collections::HashMap;
use std::path::Path;
use unicode_normalization::UnicodeNormalization;
use rphonetic::{Encoder, Cologne};

// ---------------------------------------------------------------------------
// Hardcoded defaults (used when sv.toml not available)
// ---------------------------------------------------------------------------

static DEFAULT_ABBREVIATIONS_SV: &[(&str, &str)] = &[
    ("st.", "sankta"),
    ("s:t", "sankta"),
    ("s:ta", "sankta"),
    ("kga", "kyrka"),
    ("k:a", "kyrka"),
    ("hd", "härad"),
    ("sn", "socken"),
    ("by.", "by"),
    ("gd", "gård"),
];

static DEFAULT_STOPWORDS_SV: &[&str] = &[
    "stad", "kommun", "municipality", "län", "county",
    "sverige", "sweden", "landskap", "province",
];

// ---------------------------------------------------------------------------
// Phonetic engine selection
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PhoneticEngine {
    /// Custom Swedish Metaphone (default for Nordic languages)
    SwedishMetaphone,
    /// Kölner Phonetik (for German)
    Cologne,
    /// No phonetic encoding (for US English — rely on exact + Levenshtein only)
    None,
}

impl Default for PhoneticEngine {
    fn default() -> Self {
        PhoneticEngine::SwedishMetaphone
    }
}

// ---------------------------------------------------------------------------
// TOML config structure
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
struct NormConfig {
    #[serde(default)]
    phonetic: PhoneticConfig,
    #[serde(default)]
    abbreviations: HashMap<String, String>,
    #[serde(default)]
    stopwords: StopwordsConfig,
    #[serde(default)]
    known_variants: HashMap<String, String>,
    #[serde(default)]
    case_suffixes: CaseSuffixConfig,
    #[serde(default)]
    definite_suffixes: DefiniteSuffixConfig,
    #[serde(default)]
    diacritics: HashMap<String, String>,
    #[serde(default)]
    char_equivalences: HashMap<String, String>,
}

#[derive(serde::Deserialize, Default)]
struct PhoneticConfig {
    #[serde(default)]
    engine: Option<String>,
    /// Strip Unicode combining marks (Arabic tashkeel, Hebrew niqqud, Thai tone marks, etc.)
    #[serde(default)]
    strip_combining_marks: bool,
    /// Convert full-width ASCII (０-９, Ａ-Ｚ) to half-width (0-9, A-Z) — for CJK
    #[serde(default)]
    fullwidth_to_halfwidth: bool,
}

#[derive(serde::Deserialize, Default)]
struct StopwordsConfig {
    #[serde(default)]
    words: Vec<String>,
}

#[derive(serde::Deserialize, Default)]
struct CaseSuffixConfig {
    /// Finnish grammatical case suffixes to strip, ordered longest-first
    #[serde(default)]
    suffixes: Vec<String>,
}

#[derive(serde::Deserialize, Default)]
struct DefiniteSuffixConfig {
    /// Definite-article suffixes that turn nouns into their "the" form,
    /// applied per-word in multi-word queries. For Swedish: et, en, arna,
    /// orna ("torget" ↔ "torg", "domkyrkan" ↔ "domkyrka",
    /// "stadsbiblioteket" ↔ "stadsbibliotek"). Order longest-first so the
    /// stripper picks the best match.
    #[serde(default)]
    suffixes: Vec<String>,
}

// ---------------------------------------------------------------------------
// Normalizer
// ---------------------------------------------------------------------------

pub struct Normalizer {
    abbreviations: Vec<(String, String)>,
    stopwords: Vec<String>,
    /// known_variant → canonical_name (lowercased keys)
    known_variants: HashMap<String, String>,
    /// Finnish case suffixes to strip (empty for non-Finnish normalizers)
    case_suffixes: Vec<String>,
    /// Swedish/Norwegian definite-article suffixes (et/en/arna/…) applied
    /// per-word so a query "domkyrkan uppsala" generates a candidate
    /// "domkyrka uppsala" matching the indefinite-form OSM names.
    definite_suffixes: Vec<String>,
    /// Which phonetic encoder to use
    phonetic_engine: PhoneticEngine,
    /// Diacritic replacements (e.g. ä→ae for German, ä→a for Nordic)
    /// If empty, falls back to to_ascii_nordic()
    diacritics: Vec<(String, String)>,
    /// Character equivalences (e.g. ß↔ss) — generates extra candidates
    char_equivalences: Vec<(String, String)>,
    /// Strip Unicode combining marks (Mn category) before processing.
    /// Removes Arabic tashkeel, Hebrew niqqud, Thai tone marks, etc.
    strip_combining_marks: bool,
    /// Convert full-width ASCII to half-width (for CJK: ０→0, Ａ→A)
    fullwidth_to_halfwidth: bool,
}

impl Normalizer {
    /// Create a Swedish normalizer with hardcoded defaults.
    pub fn swedish() -> Self {
        Self {
            abbreviations: DEFAULT_ABBREVIATIONS_SV
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
            stopwords: DEFAULT_STOPWORDS_SV.iter().map(|s| s.to_string()).collect(),
            known_variants: HashMap::new(),
            case_suffixes: Vec::new(),
            // Swedish definite-article suffixes default. Longest first.
            definite_suffixes: vec![
                "arna".to_string(),
                "orna".to_string(),
                "et".to_string(),
                "en".to_string(),
            ],
            phonetic_engine: PhoneticEngine::SwedishMetaphone,
            diacritics: Vec::new(),
            char_equivalences: Vec::new(),
            strip_combining_marks: false,
            fullwidth_to_halfwidth: false,
        }
    }

    /// Load normalizer from a TOML config file.
    /// Works for any language — the config structure is country-agnostic.
    /// Falls back to hardcoded Swedish defaults if the file doesn't exist or can't be parsed.
    pub fn from_config(toml_path: &Path) -> Self {
        match std::fs::read_to_string(toml_path) {
            Ok(contents) => match toml::from_str::<NormConfig>(&contents) {
                Ok(config) => Self::from_parsed_config(config),
                Err(_) => Self::swedish(),
            },
            Err(_) => Self::swedish(),
        }
    }

    /// Backwards-compatible alias for from_config.
    pub fn swedish_from_config(toml_path: &Path) -> Self {
        Self::from_config(toml_path)
    }

    /// Access the known_variants map (variant → canonical, all lowercased).
    pub fn known_variants(&self) -> &HashMap<String, String> {
        &self.known_variants
    }

    /// Access the configured stopwords (lowercased). Used by the build
    /// pipeline's per-word indexing to skip filler tokens like "kommun"
    /// that would otherwise create dense, noisy posting lists.
    pub fn stopwords(&self) -> &[String] {
        &self.stopwords
    }

    fn from_parsed_config(config: NormConfig) -> Self {
        let abbreviations: Vec<(String, String)> = if config.abbreviations.is_empty() {
            DEFAULT_ABBREVIATIONS_SV
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect()
        } else {
            config.abbreviations.into_iter().collect()
        };

        let stopwords = if config.stopwords.words.is_empty() {
            DEFAULT_STOPWORDS_SV.iter().map(|s| s.to_string()).collect()
        } else {
            config.stopwords.words
        };

        // Lowercase all variant keys for matching
        let known_variants: HashMap<String, String> = config
            .known_variants
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v.to_lowercase()))
            .collect();

        // Sort suffixes by length descending for longest-first matching
        let mut case_suffixes = config.case_suffixes.suffixes;
        case_suffixes.sort_by(|a, b| b.len().cmp(&a.len()));

        let mut definite_suffixes = config.definite_suffixes.suffixes;
        definite_suffixes.sort_by(|a, b| b.len().cmp(&a.len()));

        // Phonetic engine selection
        let phonetic_engine = match config.phonetic.engine.as_deref() {
            Some("cologne") | Some("koelner") => PhoneticEngine::Cologne,
            Some("none") => PhoneticEngine::None,
            _ => PhoneticEngine::SwedishMetaphone,
        };

        // Diacritic map (sorted longest-first for multi-char keys like "ß")
        let mut diacritics: Vec<(String, String)> = config.diacritics
            .into_iter()
            .collect();
        diacritics.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

        // Character equivalences (e.g. ß↔ss)
        let char_equivalences: Vec<(String, String)> = config.char_equivalences
            .into_iter()
            .collect();

        let strip_combining_marks = config.phonetic.strip_combining_marks;
        let fullwidth_to_halfwidth = config.phonetic.fullwidth_to_halfwidth;

        Self {
            abbreviations,
            stopwords,
            known_variants,
            case_suffixes,
            definite_suffixes,
            phonetic_engine,
            diacritics,
            char_equivalences,
            strip_combining_marks,
            fullwidth_to_halfwidth,
        }
    }

    /// Normalize a query string into candidate lookup keys.
    ///
    /// Returns a Vec because one input may produce multiple candidates to try.
    /// The FST query layer tries all of them in order — first hit wins.
    ///
    /// Example:
    ///   "Göteborg, Sweden" → ["göteborg", "goteborg", ...]
    ///   "Malmoe"           → ["malmö", "malmoe", ...]  (via known_variants)
    ///   "Straße"           → ["straße", "strasse", ...]  (via char_equivalences)
    pub fn normalize(&self, input: &str) -> Vec<String> {
        let mut candidates = vec![];

        // Step 0: check known_variants FIRST — if the whole input is a known
        // variant, the canonical form should be the first candidate tried
        let lower_input = input.to_lowercase().trim().to_owned();
        if let Some(canonical) = self.known_variants.get(&lower_input) {
            candidates.push(canonical.clone());
        }

        // Step 1: base normalization
        let base = self.base_normalize(input);
        if !candidates.contains(&base) {
            candidates.push(base.clone());
        }

        // Step 2: strip stopwords variant
        let stripped = self.strip_stopwords(&base);
        if stripped != base && !candidates.contains(&stripped) {
            candidates.push(stripped.clone());
        }

        // Step 3: abbreviation expansion
        let expanded = self.expand_abbreviations(&base);
        if expanded != base && !candidates.contains(&expanded) {
            candidates.push(expanded.clone());
            let expanded_stripped = self.strip_stopwords(&expanded);
            if expanded_stripped != expanded && !candidates.contains(&expanded_stripped) {
                candidates.push(expanded_stripped);
            }
        }

        // Step 4: handle "X, country" and "X (region)" patterns
        if let Some(core) = extract_core_name(input) {
            let core_norm = self.base_normalize(&core);
            if !candidates.contains(&core_norm) {
                candidates.push(core_norm.clone());
            }
            // Also check if the core name is a known variant
            if let Some(canonical) = self.known_variants.get(&core_norm) {
                if !candidates.contains(canonical) {
                    candidates.push(canonical.clone());
                }
            }
        }

        // Step 5: Finnish case suffix stripping (e.g. "Helsingissä" → "helsinki")
        if !self.case_suffixes.is_empty() {
            let suffix_stripped = self.strip_case_suffix(&base);
            if suffix_stripped != base && !candidates.contains(&suffix_stripped) {
                // Also check if the stripped form is a known variant
                if let Some(canonical) = self.known_variants.get(&suffix_stripped) {
                    if !candidates.contains(canonical) {
                        candidates.push(canonical.clone());
                    }
                }
                if !candidates.contains(&suffix_stripped) {
                    candidates.push(suffix_stripped);
                }
            }
        }

        // Step 6: character equivalence variants (e.g. ß↔ss for German)
        if !self.char_equivalences.is_empty() {
            let equiv_candidates: Vec<String> = candidates.clone();
            for candidate in &equiv_candidates {
                for (from, to) in &self.char_equivalences {
                    // Both directions: "straße" → "strasse" AND "strasse" → "straße"
                    if candidate.contains(from.as_str()) {
                        let variant = candidate.replace(from.as_str(), to.as_str());
                        if !candidates.contains(&variant) {
                            candidates.push(variant);
                        }
                    }
                    if candidate.contains(to.as_str()) {
                        let variant = candidate.replace(to.as_str(), from.as_str());
                        if !candidates.contains(&variant) {
                            candidates.push(variant);
                        }
                    }
                }
            }
        }

        // Step 7: diacritic variants
        if !self.diacritics.is_empty() {
            // Use config-defined diacritic mappings (e.g. German ä→ae, ö→oe, ü→ue)
            let ascii_variant = self.apply_diacritics(&base);
            if ascii_variant != base && !candidates.contains(&ascii_variant) {
                candidates.push(ascii_variant);
            }
        } else {
            // Nordic default: å/ä/ö/æ/ø → a/a/o/a/o
            let ascii_variant = to_ascii_nordic(&base);
            if ascii_variant != base && !candidates.contains(&ascii_variant) {
                candidates.push(ascii_variant);
            }
        }

        // Swedish definite-article stripping is intentionally NOT applied
        // here — that would pollute the FST at *index* time, indexing
        // every "Stadsbiblioteket" record under the "stadsbibliotek" key
        // at full importance, drowning out the per-word entries. The
        // stripping is exposed via `normalize_for_query` which the API
        // layer calls when interpreting user input only.

        // Step 8: universal ASCII fallback for Latin-script names. The
        // config diacritic map and the Nordic table only cover their
        // language family — but OSM data carries borrowings (Stockholm
        // "Grand Hôtel", "Café Opera") whose users still type the
        // ASCII form. The Latin-only gate avoids stripping combining
        // marks from Devanagari/Thai/etc. where they carry vowels.
        if let Some(universal) = to_ascii_universal(&base) {
            if universal != base && !candidates.contains(&universal) {
                candidates.push(universal);
            }
        }

        candidates.into_iter().filter(|s| !s.is_empty()).collect()
    }

    /// Produce the phonetic key for phonetic FST lookup.
    /// Uses the configured phonetic engine (Swedish Metaphone or Kölner Phonetik).
    pub fn phonetic_key(&self, input: &str) -> String {
        let base = self.base_normalize(input);
        match self.phonetic_engine {
            PhoneticEngine::SwedishMetaphone => swedish_metaphone(&base),
            PhoneticEngine::Cologne => cologne_phonetic(&base),
            PhoneticEngine::None => base,
        }
    }

    fn base_normalize(&self, input: &str) -> String {
        let mut s: String = input.nfc().collect();

        // Strip Unicode combining marks BEFORE lowercasing
        // (Arabic tashkeel, Hebrew niqqud, Thai tone marks, etc.)
        if self.strip_combining_marks {
            s = strip_combining_marks(&s);
        }

        // Full-width → half-width ASCII (CJK: ０→0, Ａ→A, ａ→a)
        if self.fullwidth_to_halfwidth {
            s = fullwidth_to_halfwidth(&s);
        }

        let lower = s.to_lowercase();
        lower.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn strip_stopwords(&self, input: &str) -> String {
        let words: Vec<&str> = input
            .split_whitespace()
            .filter(|w| !self.stopwords.iter().any(|s| s == w))
            .collect();
        words.join(" ")
    }

    /// Like `normalize`, plus per-word definite-article suffix stripping.
    /// Used by the API for user-input candidates; the build pipeline uses
    /// the plain `normalize` so the FST doesn't get an inflected variant
    /// at full importance for every "Xet" record.
    pub fn normalize_for_query(&self, input: &str) -> Vec<String> {
        let mut candidates = self.normalize(input);

        // Trailing single-letter token strip — Nordic postal-district
        // suffixes ("København K", "Aarhus C", "Aalborg Ø") are
        // universally formatted as "<city> <one-letter>". The letter
        // carries no place-name signal but anchors phonetic noise; add
        // a candidate without it so the underlying city resolves.
        // Generic across languages — anything that looks like a
        // 1-letter trailing token is unlikely to be the principal
        // identifier of a place worth indexing.
        if let Some(last) = input.split_whitespace().last() {
            let alphas = last.chars().filter(|c| c.is_alphabetic()).count();
            if alphas == 1 {
                let words: Vec<&str> = input.split_whitespace().collect();
                if words.len() >= 2 {
                    let head = words[..words.len() - 1].join(" ");
                    let head_norm = self.normalize(&head);
                    for v in head_norm {
                        if !candidates.contains(&v) {
                            candidates.push(v);
                        }
                    }
                }
            }
        }

        // Definite-suffix-stripped variants (Norwegian "domkirken" →
        // "domkirke"; Swedish "stadsbiblioteket" → "stadsbibliotek").
        if !self.definite_suffixes.is_empty() {
            let mut new_variants: Vec<String> = Vec::new();
            for c in &candidates {
                for v in self.strip_definite_variants(c) {
                    if &v != c && !candidates.contains(&v) && !new_variants.contains(&v) {
                        new_variants.push(v);
                    }
                }
            }
            candidates.extend(new_variants);
        }

        // Word-boundary variants: hyphen ↔ space and adjacent-word
        // concatenation. Generic — applies universally — because OSM
        // canonicalisation diverges from how users type compound names.
        // "Ny Ålesund" ↔ "Ny-Ålesund", "Kristian Sand" → "Kristiansand",
        // "Sør Trøndelag" ↔ "Sør-Trøndelag". One change per variant to
        // bound the candidate explosion to ~3N for an N-token query.
        let mut boundary_variants: Vec<String> = Vec::new();
        for c in &candidates {
            for v in word_boundary_variants(c) {
                if &v != c && !candidates.contains(&v) && !boundary_variants.contains(&v) {
                    boundary_variants.push(v);
                }
            }
        }
        // Cap to keep downstream FST lookup count reasonable.
        boundary_variants.truncate(16);
        candidates.extend(boundary_variants);

        // Two-token reverse-order variant. FST keys are word-order sensitive —
        // "Roskilde Domkirke" indexes under "roskilde domkirke", and a
        // user-typed "Domkirken Roskilde" misses even after definite-strip
        // ("domkirke roskilde" ≠ "roskilde domkirke"). Generating the
        // reversed form lets either order resolve to the canonical key.
        // Restricted to two tokens to bound candidate growth — for 3+ tokens
        // the per-word index handles long-form queries adequately.
        let mut reverse_variants: Vec<String> = Vec::new();
        for c in &candidates {
            let words: Vec<&str> = c.split_whitespace().collect();
            if words.len() == 2 {
                let reversed = format!("{} {}", words[1], words[0]);
                if !candidates.contains(&reversed) && !reverse_variants.contains(&reversed) {
                    reverse_variants.push(reversed);
                }
            }
        }
        candidates.extend(reverse_variants);

        candidates
    }

    /// All valid definite-suffix-stripped forms of a single word. A word may
    /// match multiple suffixes (Norwegian "domkirken" → "domkirk" via "en"
    /// AND "domkirke" via "n"); both stems are useful candidates because the
    /// real OSM record could be indexed under either depending on whether
    /// the indefinite stem ends in a vowel.
    fn strip_definite_one_all(&self, word: &str) -> Vec<String> {
        let mut out = Vec::new();
        for suffix in &self.definite_suffixes {
            if word.ends_with(suffix.as_str())
                && word.len() - suffix.len() >= 4
            {
                let stem = word[..word.len() - suffix.len()].to_owned();
                if !out.contains(&stem) {
                    out.push(stem);
                }
            }
        }
        out
    }

    /// Generate all definite-stripped variants of a multi-word phrase.
    /// Strips one word at a time (cartesian product across the strip
    /// options of that word) — full cartesian across all words would
    /// blow up for long queries. In practice ≥99% of definite-form
    /// queries have only one strippable token.
    fn strip_definite_variants(&self, input: &str) -> Vec<String> {
        let words: Vec<&str> = input.split_whitespace().collect();
        if words.is_empty() {
            return Vec::new();
        }
        let mut out: Vec<String> = Vec::new();
        for (i, w) in words.iter().enumerate() {
            for stem in self.strip_definite_one_all(w) {
                let parts: Vec<String> = words.iter().enumerate()
                    .map(|(j, ww)| if j == i { stem.clone() } else { (*ww).to_string() })
                    .collect();
                let joined = parts.join(" ");
                if !out.contains(&joined) {
                    out.push(joined);
                }
            }
        }
        out
    }

    /// Strip Finnish grammatical case suffixes from a word.
    /// Returns the original if no suffix matched or result would be too short.
    fn strip_case_suffix(&self, input: &str) -> String {
        // Only strip from single words (place names, not multi-word queries)
        let words: Vec<&str> = input.split_whitespace().collect();
        if words.len() != 1 {
            return input.to_owned();
        }

        for suffix in &self.case_suffixes {
            if input.ends_with(suffix.as_str()) && input.len() - suffix.len() >= 3 {
                return input[..input.len() - suffix.len()].to_owned();
            }
        }
        input.to_owned()
    }

    fn expand_abbreviations(&self, input: &str) -> String {
        // Token-by-token replacement — splits on whitespace, expands any
        // exact word matches. This avoids the previous start-of-string
        // hazard ("ki" would have eaten the leading two chars of "kista"
        // and turned it into "karolinska institutet sta"). Now an
        // abbreviation only expands when it stands alone as a token —
        // "ki solna" → "karolinska institutet solna", "kista" stays put.
        input.split_whitespace()
            .map(|tok| {
                let lower = tok.to_lowercase();
                self.abbreviations.iter()
                    .find(|(a, _)| a == &lower)
                    .map(|(_, e)| e.clone())
                    .unwrap_or_else(|| tok.to_owned())
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Apply configured diacritic replacements (multi-char aware).
    fn apply_diacritics(&self, input: &str) -> String {
        let mut result = String::with_capacity(input.len());
        let chars: Vec<char> = input.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            let mut matched = false;
            // Try each diacritic replacement (sorted longest-first)
            for (from, to) in &self.diacritics {
                let from_chars: Vec<char> = from.chars().collect();
                if i + from_chars.len() <= chars.len()
                    && chars[i..i + from_chars.len()] == from_chars[..]
                {
                    result.push_str(to);
                    i += from_chars.len();
                    matched = true;
                    break;
                }
            }
            if !matched {
                result.push(chars[i]);
                i += 1;
            }
        }
        result
    }
}

/// Generate word-boundary variants of a query string.
///
/// Language-agnostic. Users type compound names with separators that don't
/// always match how OSM canonicalises them. For each adjacent-token gap we
/// emit two variants:
///
///   - **Swap separator** (space ↔ hyphen). `"Ny Ålesund"` → `"Ny-Ålesund"`,
///     `"Sør-Trøndelag"` → `"Sør Trøndelag"`.
///   - **Concatenate** (drop the separator). `"Kristian Sand"` →
///     `"Kristiansand"`, `"Lille Hammer"` → `"Lillehammer"`.
///
/// Exactly one boundary changes per variant — never the cartesian product
/// across all gaps — so an N-token input yields at most `2 * (N - 1)`
/// variants. The original input is **not** included in the output; callers
/// should try it first and treat these as fallbacks.
pub fn word_boundary_variants(input: &str) -> Vec<String> {
    // Tokenise on whitespace and hyphens, recording the separator that
    // ended each token. Empty tokens (from runs like "  ") are skipped.
    let mut tokens: Vec<&str> = Vec::new();
    let mut seps: Vec<char> = Vec::new();
    let mut start = 0usize;
    for (i, c) in input.char_indices() {
        if c.is_whitespace() || c == '-' {
            if i > start {
                tokens.push(&input[start..i]);
                seps.push(c);
            }
            start = i + c.len_utf8();
        }
    }
    if start < input.len() {
        tokens.push(&input[start..]);
    }
    if tokens.len() < 2 {
        return Vec::new();
    }
    // We have tokens[0..N] and seps[0..N-1] (one separator per gap;
    // if `seps.len() < tokens.len() - 1` the input ended in a separator
    // run, which we treat the same as a normal trailing token).
    while seps.len() < tokens.len().saturating_sub(1) {
        seps.push(' ');
    }

    // Helper: rebuild input with the gap at `sep_idx` either swapped to
    // the alternate separator or concatenated.
    let rebuild = |sep_idx: usize, target: Option<char>| -> String {
        let mut s = String::with_capacity(input.len() + 1);
        for (i, t) in tokens.iter().enumerate() {
            if i > 0 {
                let c = if i - 1 == sep_idx {
                    target
                } else {
                    Some(seps[i - 1])
                };
                if let Some(c) = c {
                    s.push(c);
                }
            }
            s.push_str(t);
        }
        s
    };

    let mut out: Vec<String> = Vec::with_capacity(seps.len() * 2);
    for sep_idx in 0..seps.len() {
        let alt = if seps[sep_idx] == '-' { ' ' } else { '-' };
        let swapped = rebuild(sep_idx, Some(alt));
        if swapped != input && !out.contains(&swapped) {
            out.push(swapped);
        }
        let concatenated = rebuild(sep_idx, None);
        if concatenated != input && !out.contains(&concatenated) {
            out.push(concatenated);
        }
    }
    out
}

/// Strip everything after comma or parenthesis.
fn extract_core_name(input: &str) -> Option<String> {
    if let Some(pos) = input.find(',') {
        Some(input[..pos].trim().to_owned())
    } else if let Some(pos) = input.find('(') {
        Some(input[..pos].trim().to_owned())
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Unicode script support
// ---------------------------------------------------------------------------

/// Strip Unicode combining marks (category Mn / Nonspacing Mark).
/// Removes Arabic tashkeel (fatha/damma/kasra/shadda/sukun), Hebrew niqqud,
/// Thai tone marks, generic combining diacritical marks, etc.
///
/// Uses NFD decomposition → filter combining marks → NFC recomposition.
/// This is the correct Unicode way to strip diacritics and vowel marks.
fn strip_combining_marks(s: &str) -> String {
    use unicode_normalization::UnicodeNormalization;

    // NFD decompose (splits combined characters into base + combining marks)
    // then filter out combining marks, then NFC recompose
    s.nfd()
        .filter(|c| {
            let cp = *c as u32;
            !is_combining_mark(cp)
        })
        .nfc()
        .collect()
}

/// Check if a Unicode codepoint is a combining mark that should be stripped.
/// Covers: general combining diacriticals, Arabic, Hebrew, Thai, Lao,
/// Tibetan, Myanmar, and other common ranges.
#[inline]
fn is_combining_mark(cp: u32) -> bool {
    matches!(cp,
        // Combining Diacritical Marks (general accents, tone marks)
        0x0300..=0x036F |
        // Combining Diacritical Marks Extended
        0x1AB0..=0x1AFF |
        // Combining Diacritical Marks Supplement
        0x1DC0..=0x1DFF |
        // Hebrew cantillation marks + niqqud
        0x0591..=0x05BD | 0x05BF | 0x05C1..=0x05C2 | 0x05C4..=0x05C5 | 0x05C7 |
        // Arabic tashkeel, Quranic marks, extended Arabic marks
        0x0610..=0x061A | 0x064B..=0x065F | 0x0670 |
        0x06D6..=0x06DC | 0x06DF..=0x06E4 | 0x06E7..=0x06E8 | 0x06EA..=0x06ED |
        // Syriac
        0x0730..=0x074A |
        // Thai (above/below vowels + tone marks)
        0x0E31 | 0x0E34..=0x0E3A | 0x0E47..=0x0E4E |
        // Lao
        0x0EB1 | 0x0EB4..=0x0EBC | 0x0EC8..=0x0ECD |
        // Tibetan
        0x0F18..=0x0F19 | 0x0F35 | 0x0F37 | 0x0F39 | 0x0F71..=0x0F7E |
        0x0F80..=0x0F84 | 0x0F86..=0x0F87 |
        // Myanmar
        0x102D..=0x1030 | 0x1032..=0x1037 | 0x1039..=0x103A | 0x103D..=0x103E |
        0x1058..=0x1059 | 0x105E..=0x1060 |
        // Devanagari (virama, nukta, anusvara etc.)
        0x0901..=0x0903 | 0x093A..=0x094F | 0x0951..=0x0957 | 0x0962..=0x0963 |
        // Bengali
        0x0981..=0x0983 | 0x09BC | 0x09BE..=0x09CD | 0x09D7 | 0x09E2..=0x09E3 |
        // Combining Half Marks
        0xFE20..=0xFE2F
    )
}

/// Convert full-width ASCII characters (U+FF01–U+FF5E) to half-width (U+0021–U+007E).
/// Also converts ideographic space (U+3000) and full-width space (U+FF00) to regular space.
/// Essential for CJK text where digits and Latin characters may be full-width.
fn fullwidth_to_halfwidth(s: &str) -> String {
    s.chars()
        .map(|c| {
            let cp = c as u32;
            if (0xFF01..=0xFF5E).contains(&cp) {
                // Full-width ASCII variants → normal ASCII
                char::from_u32(cp - 0xFF01 + 0x0021).unwrap_or(c)
            } else if cp == 0x3000 || cp == 0xFF00 {
                // Ideographic space or full-width space → regular space
                ' '
            } else {
                c
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Nordic diacritics
// ---------------------------------------------------------------------------

/// Replace Nordic diacritics with ASCII equivalents.
/// Handles Swedish (ä, ö, å) and Norwegian/Danish (æ, ø, å).
pub fn to_ascii_nordic(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'å' | 'ä' | 'æ' => 'a',
            'ö' | 'ø' => 'o',
            'Å' | 'Ä' | 'Æ' => 'A',
            'Ö' | 'Ø' => 'O',
            c => c,
        })
        .collect()
}

/// Strip Latin diacritics (å→a, ô→o, é→e, ç→c, ü→u, ñ→n …) — broader
/// than `to_ascii_nordic`. Lets foreign-character-bearing names like
/// "Grand Hôtel" → "grand hotel" land in the FST under the ASCII-only key
/// users actually type. Uses Unicode NFD decomposition + drop combining
/// marks, with a few extra rules for non-decomposable Latin characters.
///
/// Returns `None` when the input contains characters from non-Latin
/// scripts (Devanagari, Thai, Arabic, CJK, …). Their combining marks
/// carry semantic vowels and tones — stripping them yields garbage like
/// "नई दिल्ली" → "नई दलल". The script-specific normalizers
/// (`strip_combining_marks`, etc.) handle those separately.
pub fn to_ascii_universal(s: &str) -> Option<String> {
    if !is_latin_script(s) {
        return None;
    }
    use unicode_normalization::UnicodeNormalization;
    let decomposed: String = s.nfd().collect();
    let result: String = decomposed.chars()
        .filter_map(|c| match c {
            // Skip combining marks (the "˝", "´", "¸" parts of decomposed
            // characters)
            c if unicode_normalization::char::is_combining_mark(c) => None,
            // Special cases that don't decompose: ð/Ð/þ/Þ/æ/œ/ø/ß/etc.
            'æ' | 'Æ' => Some('a'),
            'œ' | 'Œ' => Some('o'),
            'ø' | 'Ø' => Some('o'),
            'ß' => Some('s'),
            'ð' | 'Ð' => Some('d'),
            'þ' | 'Þ' => Some('t'),
            'ł' | 'Ł' => Some('l'),
            'đ' | 'Đ' => Some('d'),
            c => Some(c),
        })
        .collect();
    Some(result)
}

/// Returns true iff every alphabetic character in `s` belongs to the
/// Latin script. Non-alphabetic characters (digits, whitespace,
/// punctuation, common CJK punctuation) are ignored. Used to gate
/// `to_ascii_universal` so it doesn't mangle Devanagari/Thai/etc.
fn is_latin_script(s: &str) -> bool {
    s.chars().all(|c| {
        if !c.is_alphabetic() { return true; }
        // Latin Unicode blocks — Basic Latin, Latin-1 Supplement,
        // Latin Extended-A/B/C/D/E, IPA Extensions, Latin Extended
        // Additional. Anything outside these is a non-Latin script.
        matches!(c as u32,
            0x0041..=0x005A     // A-Z
            | 0x0061..=0x007A   // a-z
            | 0x00C0..=0x024F   // Latin-1 Supplement + Latin Extended-A/B
            | 0x1E00..=0x1EFF   // Latin Extended Additional
            | 0x2C60..=0x2C7F   // Latin Extended-C
            | 0xA720..=0xA7FF   // Latin Extended-D
            | 0xAB30..=0xAB6F)  // Latin Extended-E
    })
}

/// Alias for backwards compatibility
pub fn to_ascii_swedish(s: &str) -> String {
    to_ascii_nordic(s)
}

/// Simplified Swedish Metaphone
pub fn swedish_metaphone(s: &str) -> String {
    let s = to_ascii_swedish(s);
    let chars: Vec<char> = s.chars().collect();
    let mut result = String::new();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();

        match c {
            'a' | 'e' | 'i' | 'o' | 'u' | 'y' => {
                if result.is_empty() {
                    result.push('A');
                }
            }
            'k' if next == Some('j') => { result.push('X'); i += 1; }
            't' if next == Some('j') => { result.push('X'); i += 1; }
            's' if next == Some('j') => { result.push('X'); i += 1; }
            'c' if matches!(next, Some('e') | Some('i') | Some('y')) => result.push('S'),
            'c' => result.push('K'),
            'g' if matches!(next, Some('e') | Some('i') | Some('y')) => result.push('J'),
            'g' => result.push('K'),
            'z' => result.push('S'),
            'w' => result.push('V'),
            'h' => {}
            c if Some(c) == next && !matches!(c, 'a' | 'e' | 'i' | 'o' | 'u') => {
                result.push(c.to_ascii_uppercase());
                i += 1;
            }
            c if c.is_alphabetic() => result.push(c.to_ascii_uppercase()),
            ' ' => result.push(' '),
            _ => {}
        }
        i += 1;
    }

    result
}

/// Kölner Phonetik (Cologne Phonetics) — purpose-built for German.
/// Uses the rphonetic crate's Cologne encoder.
pub fn cologne_phonetic(s: &str) -> String {
    let encoder = Cologne;
    // Cologne encoder works on individual words; join multi-word results
    s.split_whitespace()
        .map(|word| encoder.encode(word))
        .collect::<Vec<_>>()
        .join(" ")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_normalization() {
        let n = Normalizer::swedish();
        let candidates = n.normalize("Stockholm");
        assert!(candidates.contains(&"stockholm".to_owned()));
    }

    #[test]
    fn test_word_boundary_variants_concatenate() {
        // Case preservation is intentional — normalize() lowercases later
        // when it builds FST lookup keys. We just rejoin tokens as-is.
        let v = word_boundary_variants("Kristian Sand");
        assert!(v.contains(&"KristianSand".to_owned()), "got {:?}", v);
        let v2 = word_boundary_variants("kristian sand");
        assert!(v2.contains(&"kristiansand".to_owned()), "got {:?}", v2);
    }

    #[test]
    fn test_word_boundary_variants_swap() {
        let v = word_boundary_variants("Ny Ålesund");
        assert!(v.contains(&"Ny-Ålesund".to_owned()), "got {:?}", v);
        let v2 = word_boundary_variants("Sør-Trøndelag");
        assert!(v2.contains(&"Sør Trøndelag".to_owned()), "got {:?}", v2);
    }

    #[test]
    fn test_word_boundary_variants_three_token() {
        let v = word_boundary_variants("A B C");
        // Each gap gets a swap and a concat → 4 variants total
        assert!(v.contains(&"A-B C".to_owned()), "got {:?}", v);
        assert!(v.contains(&"AB C".to_owned()), "got {:?}", v);
        assert!(v.contains(&"A B-C".to_owned()), "got {:?}", v);
        assert!(v.contains(&"A BC".to_owned()), "got {:?}", v);
    }

    #[test]
    fn test_word_boundary_variants_no_change_single_token() {
        let v = word_boundary_variants("Stockholm");
        assert!(v.is_empty(), "single token should produce no variants, got {:?}", v);
    }

    #[test]
    fn test_normalize_for_query_includes_boundary_variants() {
        // Default (Swedish) normalizer should still produce a concatenated
        // candidate from user input "Kristian Sand" via the language-agnostic
        // word-boundary variant path.
        let n = Normalizer::swedish();
        let cands = n.normalize_for_query("Kristian Sand");
        assert!(cands.iter().any(|c| c == "kristiansand"),
                "no kristiansand candidate, got {:?}", cands);
    }

    #[test]
    fn test_stopword_stripping() {
        let n = Normalizer::swedish();
        let candidates = n.normalize("Stockholms stad");
        assert!(candidates.contains(&"stockholms".to_owned())
            || candidates.contains(&"stockholm".to_owned()));
    }

    #[test]
    fn test_comma_extraction() {
        let n = Normalizer::swedish();
        let candidates = n.normalize("Göteborg, Sweden");
        assert!(candidates.iter().any(|c| c.contains("göteborg") || c.contains("goteborg")));
    }

    #[test]
    fn test_diacritic_variant() {
        let n = Normalizer::swedish();
        let candidates = n.normalize("Göteborg");
        assert!(candidates.contains(&"göteborg".to_owned()));
        assert!(candidates.contains(&"goteborg".to_owned()));
    }

    #[test]
    fn test_known_variants() {
        let mut n = Normalizer::swedish();
        n.known_variants.insert("malmoe".to_owned(), "malmö".to_owned());
        n.known_variants.insert("gbg".to_owned(), "göteborg".to_owned());

        let candidates = n.normalize("Malmoe");
        assert_eq!(candidates[0], "malmö", "canonical form should be first candidate");

        let candidates = n.normalize("Gbg");
        assert_eq!(candidates[0], "göteborg", "abbreviation should resolve");
    }

    #[test]
    fn test_phonetic() {
        let n = Normalizer::swedish();
        let p1 = n.phonetic_key("Göteborg");
        let p2 = n.phonetic_key("Gothenburg");
        println!("Göteborg phonetic: {}", p1);
        println!("Gothenburg phonetic: {}", p2);
    }

    #[test]
    fn test_cologne_phonetic() {
        // Kölner Phonetik: "Müller" and "Mueller" should produce the same code
        let c1 = cologne_phonetic("Müller");
        let c2 = cologne_phonetic("Mueller");
        assert_eq!(c1, c2, "Müller and Mueller should have same Cologne code");

        // "Köln" and "Koeln" should match
        let c3 = cologne_phonetic("Köln");
        let c4 = cologne_phonetic("Koeln");
        println!("Köln: {}, Koeln: {}", c3, c4);
    }

    #[test]
    fn test_german_char_equivalences() {
        let mut n = Normalizer::swedish();
        n.char_equivalences = vec![("ß".to_owned(), "ss".to_owned())];

        let candidates = n.normalize("Straße");
        assert!(candidates.contains(&"straße".to_owned()), "should contain original");
        assert!(candidates.contains(&"strasse".to_owned()), "should contain ss variant");
    }

    #[test]
    fn test_german_diacritics() {
        let mut n = Normalizer::swedish();
        n.diacritics = vec![
            ("ä".to_owned(), "ae".to_owned()),
            ("ö".to_owned(), "oe".to_owned()),
            ("ü".to_owned(), "ue".to_owned()),
        ];

        let candidates = n.normalize("München");
        assert!(candidates.contains(&"münchen".to_owned()));
        assert!(candidates.contains(&"muenchen".to_owned()), "should contain ae/oe/ue variant");
    }

    #[test]
    fn test_phonetic_engine_comparison() {
        // Side-by-side: Swedish Metaphone vs Kölner Phonetik on German names
        let names = vec![
            ("Müller", "Mueller"),
            ("Schröder", "Schroeder"),
            ("Bayern", "Baiern"),
            ("Württemberg", "Wuerttemberg"),
            ("München", "Muenchen"),
            ("Köln", "Koeln"),
            ("Düsseldorf", "Duesseldorf"),
            ("Straße", "Strasse"),
        ];

        println!("\n{:<16} {:<16} {:<10} {:<10} {:<10} {:<10}",
            "Original", "Variant", "SW_orig", "SW_var", "CO_orig", "CO_var");
        println!("{}", "-".repeat(72));

        let mut cologne_matches = 0;
        let mut swedish_matches = 0;

        for (orig, variant) in &names {
            let lo = orig.to_lowercase();
            let lv = variant.to_lowercase();
            let sw_o = swedish_metaphone(&lo);
            let sw_v = swedish_metaphone(&lv);
            let co_o = cologne_phonetic(&lo);
            let co_v = cologne_phonetic(&lv);

            if co_o == co_v { cologne_matches += 1; }
            if sw_o == sw_v { swedish_matches += 1; }

            println!("{:<16} {:<16} {:<10} {:<10} {:<10} {:<10} {}",
                orig, variant, sw_o, sw_v, co_o, co_v,
                if co_o == co_v && sw_o != sw_v { "← Cologne wins" }
                else if sw_o == sw_v && co_o != co_v { "← Swedish wins" }
                else if co_o == co_v && sw_o == sw_v { "both match" }
                else { "neither matches" }
            );
        }

        println!("\nCologne matched {}/{} pairs, Swedish matched {}/{}",
            cologne_matches, names.len(), swedish_matches, names.len());

        // Cologne should match more German umlaut pairs than Swedish Metaphone
        assert!(cologne_matches >= swedish_matches,
            "Cologne should match at least as many German name pairs as Swedish Metaphone");
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_load_sv_toml() {
        let path = Path::new("../../data/normalizers/sv.toml");
        if !path.exists() {
            eprintln!("sv.toml not found at {:?}, skipping", path);
            return;
        }
        let n = Normalizer::swedish_from_config(path);
        eprintln!("known_variants count: {}", n.known_variants.len());
        for (k, v) in &n.known_variants {
            eprintln!("  {} -> {}", k, v);
        }
        assert!(n.known_variants.len() > 0, "known_variants should not be empty");
        assert_eq!(n.known_variants.get("malmoe"), Some(&"malmö".to_owned()));

        let candidates = n.normalize("Malmoe");
        eprintln!("Malmoe candidates: {:?}", candidates);
        assert_eq!(candidates[0], "malmö");
    }

    #[test]
    fn test_load_de_toml() {
        let path = Path::new("../../data/normalizers/de.toml");
        if !path.exists() {
            eprintln!("de.toml not found at {:?}, skipping", path);
            return;
        }
        let n = Normalizer::from_config(path);

        // Phonetic engine should be Cologne
        assert_eq!(n.phonetic_engine, PhoneticEngine::Cologne);

        // Known variants
        assert_eq!(n.known_variants.get("munich"), Some(&"münchen".to_owned()));
        assert_eq!(n.known_variants.get("cologne"), Some(&"köln".to_owned()));

        // ß/ss equivalence
        let candidates = n.normalize("Hauptstraße");
        eprintln!("Hauptstraße candidates: {:?}", candidates);
        assert!(candidates.contains(&"hauptstraße".to_owned()));
        assert!(candidates.contains(&"hauptstrasse".to_owned()));

        // German diacritic expansion
        let candidates = n.normalize("München");
        eprintln!("München candidates: {:?}", candidates);
        assert!(candidates.contains(&"münchen".to_owned()));
        assert!(candidates.contains(&"muenchen".to_owned()));

        // Cologne phonetic key
        let key = n.phonetic_key("München");
        eprintln!("München phonetic: {}", key);
        assert!(!key.is_empty());
    }

    #[test]
    fn test_strip_combining_marks_arabic() {
        // "مَدْرَسَة" (madrasa with tashkeel) → "مدرسة" (without)
        let input = "م\u{064E}د\u{0652}ر\u{064E}س\u{064E}ة";
        let stripped = strip_combining_marks(input);
        assert_eq!(stripped, "مدرسة");
    }

    #[test]
    fn test_strip_combining_marks_hebrew() {
        // Hebrew with niqqud → base consonants
        let input = "יְרוּשָׁלַיִם"; // Yerushalayim with niqqud
        let stripped = strip_combining_marks(input);
        // Should have base consonants only, no vowel points
        assert!(!stripped.contains('\u{05B0}')); // sheva
        assert!(!stripped.contains('\u{05B8}')); // qamats
    }

    #[test]
    fn test_fullwidth_to_halfwidth() {
        assert_eq!(fullwidth_to_halfwidth("１２３"), "123");
        assert_eq!(fullwidth_to_halfwidth("Ａ Ｂ Ｃ"), "A B C");
        assert_eq!(fullwidth_to_halfwidth("東京１丁目"), "東京1丁目");
        // Ideographic space
        assert_eq!(fullwidth_to_halfwidth("東京\u{3000}都"), "東京 都");
    }

    #[test]
    fn test_cyrillic_diacritics() {
        let mut n = Normalizer::swedish();
        n.diacritics = vec![
            ("м".to_owned(), "m".to_owned()),
            ("о".to_owned(), "o".to_owned()),
            ("с".to_owned(), "s".to_owned()),
            ("к".to_owned(), "k".to_owned()),
            ("в".to_owned(), "v".to_owned()),
            ("а".to_owned(), "a".to_owned()),
        ];
        let result = n.apply_diacritics("москва");
        assert_eq!(result, "moskva");
    }

    #[test]
    fn test_normalizer_with_combining_marks() {
        let mut n = Normalizer::swedish();
        n.strip_combining_marks = true;
        n.diacritics = vec![
            ("ش".to_owned(), "sh".to_owned()),
            ("ا".to_owned(), "a".to_owned()),
            ("ر".to_owned(), "r".to_owned()),
            ("ع".to_owned(), "a".to_owned()),
        ];
        // Arabic with tashkeel — combining marks should be stripped first,
        // then diacritics applied to the base characters
        let input = "شَارِع"; // sharia with tashkeel
        let candidates = n.normalize(input);
        // Should contain a transliterated form
        assert!(candidates.iter().any(|c| c.contains("sh")),
            "Expected transliterated form, got: {:?}", candidates);
    }
}
