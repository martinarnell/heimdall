/// The main query index — wraps all FSTs and the record store.
///
/// Three FST layers queried in order:
///   1. fst_exact    — normalized string → record_id
///   2. fst_phonetic — soundex code → record_id  
///   3. fst_ngram    — trigram keys → record_id (prefix search / partial)
///
/// Fuzzy (Levenshtein) and NN layers plugged in via trait objects.

use std::path::{Path, PathBuf};
use fst::{Automaton, Map, IntoStreamer, Streamer};
use fst::automaton::{Levenshtein, Str};
use crate::types::*;
use crate::record_store::RecordStore;
use crate::error::HeimdallError;
use crate::compressed_io;

// ---------------------------------------------------------------------------
// Fuzzy fallback trait — implemented by Levenshtein layer and NN layer
// ---------------------------------------------------------------------------

pub trait FuzzyGeocoder: Send + Sync {
    fn query(&self, text: &str, context: &GeoQuery) -> Vec<GeoResult>;
    fn name(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// Index directory layout
// ---------------------------------------------------------------------------

/// All files live in a single directory.
/// Pass the directory path to HeimdallIndex::open().
///
/// heimdall-sweden/
///   records.bin      — PlaceRecord array + string pool
///   fst_exact.fst    — normalized name → record_id
///   fst_phonetic.fst — soundex codes → record_id
///   fst_ngram.fst    — trigram index → record_id
///   admin.bin        — AdminEntry array (serialized with bincode)
///   meta.json        — build metadata (date, source, record count)
pub struct IndexPaths {
    pub dir: PathBuf,
}

impl IndexPaths {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self { dir: dir.as_ref().to_owned() }
    }
    pub fn records(&self)  -> PathBuf { self.dir.join("records.bin") }
    pub fn fst_exact(&self) -> PathBuf { self.dir.join("fst_exact.fst") }
    pub fn fst_phonetic(&self) -> PathBuf { self.dir.join("fst_phonetic.fst") }
    pub fn fst_ngram(&self) -> PathBuf { self.dir.join("fst_ngram.fst") }
    pub fn admin(&self) -> PathBuf { self.dir.join("admin.bin") }
    pub fn meta(&self) -> PathBuf { self.dir.join("meta.json") }
}

// ---------------------------------------------------------------------------
// Main index
// ---------------------------------------------------------------------------

pub struct HeimdallIndex {
    records: RecordStore,
    fst_exact: Map<compressed_io::MmapOrVec>,
    fst_phonetic: Map<compressed_io::MmapOrVec>,
    _fst_ngram: Map<compressed_io::MmapOrVec>,
    admin: Vec<AdminEntry>,

    /// Optional pluggable fuzzy layers, tried in order after FST misses
    fuzzy_layers: Vec<Box<dyn FuzzyGeocoder>>,
}

impl HeimdallIndex {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, HeimdallError> {
        Self::open_inner(dir, false)
    }

    /// Open with only record store + admin — skip per-country FSTs (used when global FST handles search)
    pub fn open_lightweight(dir: impl AsRef<Path>) -> Result<Self, HeimdallError> {
        Self::open_inner(dir, true)
    }

    fn open_inner(dir: impl AsRef<Path>, skip_fsts: bool) -> Result<Self, HeimdallError> {
        let paths = IndexPaths::new(dir);

        let records = RecordStore::open(&paths.records())?;

        let make_empty_fst = || -> Map<compressed_io::MmapOrVec> {
            Map::new(compressed_io::MmapOrVec::Vec(
                fst::MapBuilder::memory().into_inner().unwrap()
            )).unwrap()
        };

        let fst_exact = if skip_fsts { make_empty_fst() } else {
            let data = compressed_io::mmap_or_decompress(&paths.fst_exact())?;
            Map::new(data).map_err(HeimdallError::Fst)?
        };

        let fst_phonetic = if skip_fsts { make_empty_fst() } else {
            let data = compressed_io::mmap_or_decompress(&paths.fst_phonetic())?;
            Map::new(data).map_err(HeimdallError::Fst)?
        };

        let fst_ngram = if skip_fsts { make_empty_fst() } else {
            let data = compressed_io::mmap_or_decompress(&paths.fst_ngram())?;
            Map::new(data).map_err(HeimdallError::Fst)?
        };

        let admin: Vec<AdminEntry> = {
            let bytes = compressed_io::read_maybe_compressed(&paths.admin())?;
            postcard::from_bytes(&bytes)
                .unwrap_or_else(|_| bincode::deserialize(&bytes).expect("admin.bin deserialize failed"))
        };

        Ok(Self {
            records,
            fst_exact,
            fst_phonetic,
            _fst_ngram: fst_ngram,
            admin,
            fuzzy_layers: vec![],
        })
    }

    /// Attach a fuzzy layer (Levenshtein or NN). Called during setup.
    pub fn with_fuzzy(mut self, layer: Box<dyn FuzzyGeocoder>) -> Self {
        self.fuzzy_layers.push(layer);
        self
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    pub fn geocode(&self, query: &GeoQuery) -> Vec<GeoResult> {
        // Normalizer produces multiple candidate strings to try
        // (imported from heimdall-normalize, called by the API layer)
        // Here we accept pre-normalized candidates for the core index.
        self.geocode_normalized(&query.text, query)
    }

    pub fn geocode_normalized(&self, normalized: &str, query: &GeoQuery) -> Vec<GeoResult> {
        // 1. Exact FST lookup
        if let Some(id) = self.fst_exact.get(normalized.as_bytes()) {
            if let Ok(results) = self.resolve_id(id as u32, MatchType::Exact, query) {
                if !results.is_empty() {
                    return results;
                }
            }
        }

        // 2. Phonetic FST lookup
        // (phonetic encoding applied upstream, passed as normalized string)
        if let Some(id) = self.fst_phonetic.get(normalized.as_bytes()) {
            if let Ok(results) = self.resolve_id(id as u32, MatchType::Phonetic, query) {
                if !results.is_empty() {
                    return results;
                }
            }
        }

        // 3. Levenshtein edit-1
        if let Ok(results) = self.levenshtein_lookup(normalized, 1, query) {
            if !results.is_empty() {
                return results;
            }
        }

        // 4. Levenshtein edit-2
        if let Ok(results) = self.levenshtein_lookup(normalized, 2, query) {
            if !results.is_empty() {
                return results;
            }
        }

        // 5. Pluggable fuzzy layers (NN etc.)
        for layer in &self.fuzzy_layers {
            let results = layer.query(normalized, query);
            if !results.is_empty() {
                tracing::debug!("fuzzy layer '{}' matched '{}'", layer.name(), normalized);
                return results;
            }
        }

        vec![]
    }

    // -----------------------------------------------------------------------
    // Levenshtein search over exact FST
    // -----------------------------------------------------------------------

    fn levenshtein_lookup(
        &self,
        query: &str,
        distance: u8,
        context: &GeoQuery,
    ) -> Result<Vec<GeoResult>, HeimdallError> {
        let lev = Levenshtein::new(query, distance as u32)
            .map_err(|e| HeimdallError::Build(e.to_string()))?;

        let mut stream = self.fst_exact.search_with_state(lev).into_stream();
        let mut seen = std::collections::HashSet::new();
        let mut candidates: Vec<(u32, u8)> = vec![]; // (record_id, actual_distance)

        while let Some((_key, val, _state)) = stream.next() {
            let id = val as u32;
            if seen.insert(id) {
                candidates.push((id, distance));
            }
        }

        if candidates.is_empty() {
            return Ok(vec![]);
        }

        let mut results = vec![];
        for (id, dist) in candidates {
            if let Ok(record) = self.records.get(id) {
                if let Some(result) = self.record_to_result(
                    id,
                    record,
                    MatchType::Levenshtein { distance: dist },
                    context,
                ) {
                    results.push(result);
                }
            }
        }

        // Rank by importance, filtered by context
        self.rank_and_filter(&mut results, context);
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn resolve_id(
        &self,
        id: u32,
        match_type: MatchType,
        context: &GeoQuery,
    ) -> Result<Vec<GeoResult>, HeimdallError> {
        let record = self.records.get(id)?;
        let mut results = vec![];

        if let Some(result) = self.record_to_result(id, record, match_type, context) {
            results.push(result);
        }

        Ok(results)
    }

    fn record_to_result(
        &self,
        id: u32,
        record: PlaceRecord,
        match_type: MatchType,
        context: &GeoQuery,
    ) -> Option<GeoResult> {
        // Apply bounding box filter if provided
        if let Some(bbox) = &context.bbox {
            if !bbox.contains(&record.coord) {
                return None;
            }
        }

        // Apply country code filter
        // (country_code stored in admin hierarchy — check admin1)
        // TODO: add country_code directly to PlaceRecord in next schema iteration

        let name = self.records.primary_name(&record);
        let admin1 = self.admin.get(record.admin1_id as usize).map(|a| a.name.clone());
        let admin2 = self.admin.get(record.admin2_id as usize).map(|a| a.name.clone());

        let confidence = match &match_type {
            MatchType::Exact => 0.99,
            MatchType::Phonetic => 0.85,
            MatchType::Levenshtein { distance } => match distance {
                1 => 0.75,
                2 => 0.55,
                _ => 0.35,
            },
            MatchType::Neural { confidence } => *confidence as f32 / 1000.0,
            MatchType::NGram { .. } => 0.50,
        };

        Some(GeoResult {
            name,
            coord: record.coord,
            place_type: record.place_type,
            admin1,
            admin2,
            country_code: None, // TODO
            importance: record.importance,
            confidence,
            match_type,
            record_id: Some(id),
        })
    }

    fn rank_and_filter(&self, results: &mut Vec<GeoResult>, context: &GeoQuery) {
        // Filter by min confidence
        results.retain(|r| r.confidence >= context.min_confidence);

        // Sort: confidence first, then importance
        results.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap()
                .then(b.importance.cmp(&a.importance))
        });

        // Limit
        results.truncate(context.limit);
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn admin_entry(&self, id: u16) -> Option<&AdminEntry> {
        self.admin.get(id as usize)
    }

    /// Access the underlying record store (for reverse geocoding).
    pub fn record_store(&self) -> &RecordStore {
        &self.records
    }

    /// Prefix search over the exact FST — for autocomplete.
    ///
    /// Collects ALL matching record IDs (cheap — just u32 values), deduplicates,
    /// pre-sorts by importance using the record store (O(1) per record, no alloc),
    /// then resolves only the top N to full GeoResults.
    pub fn prefix_search(&self, prefix: &str, limit: usize) -> Vec<GeoResult> {
        let automaton = Str::new(prefix).starts_with();
        let mut stream = self.fst_exact.search(automaton).into_stream();

        // Collect all matching record IDs — FST traversal is fast, IDs are u32
        let mut candidate_ids: Vec<u32> = Vec::new();
        while let Some((_key, value)) = stream.next() {
            candidate_ids.push(value as u32);
        }

        // Dedup (multiple normalized forms may point to the same record)
        candidate_ids.sort_unstable();
        candidate_ids.dedup();

        // Pre-sort by importance using just the record store (mmap, no string alloc)
        candidate_ids.sort_by(|&a, &b| {
            let imp_a = self.records.get(a).map(|r| r.importance).unwrap_or(0);
            let imp_b = self.records.get(b).map(|r| r.importance).unwrap_or(0);
            imp_b.cmp(&imp_a)
        });

        // Only resolve the top N to full GeoResults
        candidate_ids
            .iter()
            .take(limit)
            .filter_map(|&id| {
                let record = self.records.get(id).ok()?;
                self.record_to_result(id, record, MatchType::Exact, &GeoQuery::new(""))
            })
            .collect()
    }

    /// Direct FST lookup — returns record_id if found.
    /// Used by address geocoding to resolve city names to municipality IDs.
    pub fn exact_lookup(&self, normalized: &str) -> Option<u32> {
        self.fst_exact.get(normalized.as_bytes()).map(|v| v as u32)
    }

    /// Get admin2_id (municipality) for a record.
    pub fn record_admin2(&self, id: u32) -> Option<u16> {
        self.records.get(id).ok().map(|r| r.admin2_id)
    }
}
