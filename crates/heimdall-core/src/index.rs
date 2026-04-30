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
///   records.bin                  — PlaceRecord array + string pool
///   fst_exact.fst                — normalized name → posting offset (or record_id, see below)
///   fst_phonetic.fst             — soundex codes → posting offset
///   fst_ngram.fst                — trigram index → record_id (placeholder)
///   record_lists.bin             — sidecar posting lists for fst_exact (optional)
///   record_lists_phonetic.bin    — sidecar posting lists for fst_phonetic (optional)
///   admin.bin                    — AdminEntry array (serialized with bincode)
///   meta.json                    — build metadata (date, source, record count)
///
/// Sidecar format: at each offset, `[u16 count][u32 rec_id_1][u32 rec_id_2]...`
/// where rec_ids are sorted by importance descending (best first). When the
/// sidecar is absent, the FST values are interpreted as raw record_ids
/// (backwards compatibility with v2 indices).
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
    pub fn record_lists_exact(&self) -> PathBuf { self.dir.join("record_lists.bin") }
    pub fn record_lists_phonetic(&self) -> PathBuf { self.dir.join("record_lists_phonetic.bin") }
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
    /// Optional posting-list sidecars. When present the FST values are
    /// byte offsets into these blobs; when absent the FST values are
    /// raw record_ids (legacy v2 layout).
    record_lists_exact: Option<compressed_io::MmapOrVec>,
    record_lists_phonetic: Option<compressed_io::MmapOrVec>,
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

        // Optional posting-list sidecars. Older v2 indices were built
        // without these — in that case the FST values are interpreted as
        // raw record_ids. When present, FST values become byte offsets
        // into the sidecar.
        let record_lists_exact = if skip_fsts {
            None
        } else {
            let p = paths.record_lists_exact();
            if p.exists() && std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0) > 0 {
                Some(compressed_io::mmap_or_decompress(&p)?)
            } else {
                None
            }
        };
        let record_lists_phonetic = if skip_fsts {
            None
        } else {
            let p = paths.record_lists_phonetic();
            if p.exists() && std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0) > 0 {
                Some(compressed_io::mmap_or_decompress(&p)?)
            } else {
                None
            }
        };

        let admin: Vec<AdminEntry> = {
            let bytes = compressed_io::read_maybe_compressed(&paths.admin())?;
            // Try the current schema first (postcard, then bincode). If
            // both fail it's most likely a pre-population v2 index that
            // doesn't carry the trailing field — try the legacy schema
            // and lift entries into the new shape with population=0.
            // Without this fallback the new binary would panic on every
            // unrebuilt v2 index already deployed in production.
            postcard::from_bytes::<Vec<AdminEntry>>(&bytes)
                .or_else(|_| bincode::deserialize::<Vec<AdminEntry>>(&bytes))
                .or_else(|_| {
                    postcard::from_bytes::<Vec<AdminEntryV2>>(&bytes)
                        .or_else(|_| bincode::deserialize::<Vec<AdminEntryV2>>(&bytes))
                        .map(|v2| v2.into_iter().map(AdminEntry::from).collect())
                })
                .expect("admin.bin deserialize failed (neither v3 nor v2 schema matched)")
        };

        Ok(Self {
            records,
            fst_exact,
            fst_phonetic,
            _fst_ngram: fst_ngram,
            record_lists_exact,
            record_lists_phonetic,
            admin,
            fuzzy_layers: vec![],
        })
    }

    /// Attach a fuzzy layer (Levenshtein or NN). Called during setup.
    pub fn with_fuzzy(mut self, layer: Box<dyn FuzzyGeocoder>) -> Self {
        self.fuzzy_layers.push(layer);
        self
    }

    /// Decode a posting list at `value` from `sidecar` (if any).
    /// Sidecar format: `[u16 count][u32 rec_id]*count` at every offset.
    /// Returns rec_ids in importance-desc order (the order they were written).
    /// Falls back to a single-element vec containing `value` cast to u32 when
    /// the sidecar is absent — preserves backwards compatibility with v2
    /// indices that store record_ids directly in FST values.
    fn decode_posting_list(
        sidecar: Option<&compressed_io::MmapOrVec>,
        value: u64,
    ) -> Vec<u32> {
        let bytes = match sidecar {
            Some(s) => s.as_ref(),
            None => return vec![value as u32],
        };
        let off = value as usize;
        if off + 2 > bytes.len() { return vec![]; }
        // Cap the count to the build-time MAX_POSTINGS_PER_KEY (8) — a
        // sane builder will never write more, and clamping prevents
        // unbounded Vec allocation if the sidecar were ever corrupted.
        const MAX_POSTINGS_PER_KEY: usize = 8;
        let count = (u16::from_le_bytes([bytes[off], bytes[off + 1]]) as usize)
            .min(MAX_POSTINGS_PER_KEY);
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let p = off + 2 + i * 4;
            if p + 4 > bytes.len() { break; }
            out.push(u32::from_le_bytes([bytes[p], bytes[p + 1], bytes[p + 2], bytes[p + 3]]));
        }
        out
    }

    /// Look up a normalized key in the exact FST and return ALL record_ids
    /// (sorted by importance desc, capped at the build-time `MAX_POSTINGS_PER_KEY`).
    /// Empty when the key is not present.
    pub fn exact_lookup_all(&self, normalized: &str) -> Vec<u32> {
        match self.fst_exact.get(normalized.as_bytes()) {
            Some(v) => Self::decode_posting_list(self.record_lists_exact.as_ref(), v),
            None => vec![],
        }
    }

    /// Look up a phonetic key and return ALL record_ids (sorted by importance desc).
    pub fn phonetic_lookup_all(&self, phonetic: &str) -> Vec<u32> {
        match self.fst_phonetic.get(phonetic.as_bytes()) {
            Some(v) => Self::decode_posting_list(self.record_lists_phonetic.as_ref(), v),
            None => vec![],
        }
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
        // 1. Exact FST lookup — returns ALL same-name candidates (sorted by importance).
        let exact_ids = self.exact_lookup_all(normalized);
        if !exact_ids.is_empty() {
            let mut results = Vec::new();
            for id in &exact_ids {
                if let Ok(record) = self.records.get(*id) {
                    if let Some(r) = self.record_to_result(*id, record, MatchType::Exact, query) {
                        results.push(r);
                    }
                }
            }
            self.rank_and_filter(&mut results, query);
            if !results.is_empty() { return results; }
        }

        // 2. Phonetic FST lookup
        // (phonetic encoding applied upstream, passed as normalized string)
        let phonetic_ids = self.phonetic_lookup_all(normalized);
        if !phonetic_ids.is_empty() {
            let mut results = Vec::new();
            for id in &phonetic_ids {
                if let Ok(record) = self.records.get(*id) {
                    if let Some(r) = self.record_to_result(*id, record, MatchType::Phonetic, query) {
                        results.push(r);
                    }
                }
            }
            self.rank_and_filter(&mut results, query);
            if !results.is_empty() { return results; }
        }

        // 3. Levenshtein edit-1 — catches typos ("upsala" → Uppsala,
        // "stockholms central" → "Stockholm Central").
        if let Ok(results) = self.levenshtein_lookup(normalized, 1, query) {
            if !results.is_empty() {
                return results;
            }
        }

        // 4. Levenshtein edit-2 — only for short single-word queries.
        // Lev-2 on "stockholm sweden" cheerfully matches "stockholmsheden",
        // and even Lev-1 can be misleading on long multi-word strings.
        // Restrict to single tokens so the per-character edit budget
        // stays meaningful.
        let is_multi_word = normalized.split_whitespace().count() >= 2;
        if !is_multi_word {
            if let Ok(results) = self.levenshtein_lookup(normalized, 2, query) {
                if !results.is_empty() {
                    return results;
                }
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
        // Guard: the fst crate's Levenshtein DFA builder can panic on short
        // multi-byte UTF-8 inputs (e.g. 3-char Cyrillic = 6 bytes, distance 1).
        // Require enough bytes so the DFA stays within its internal limits.
        let min_bytes = (distance as usize + 1) * 4;
        if query.len() < min_bytes {
            return Ok(vec![]);
        }

        let lev = Levenshtein::new(query, distance as u32)
            .map_err(|e| HeimdallError::Build(e.to_string()))?;

        let mut stream = self.fst_exact.search_with_state(lev).into_stream();
        let mut seen = std::collections::HashSet::new();
        let mut candidates: Vec<(u32, u8)> = vec![]; // (record_id, actual_distance)

        while let Some((_key, val, _state)) = stream.next() {
            // Each FST value is a posting offset (or a raw record_id when
            // the sidecar is absent). Expand to the full posting list so
            // same-name alternates aren't lost on a fuzzy match.
            for id in Self::decode_posting_list(self.record_lists_exact.as_ref(), val) {
                if seen.insert(id) {
                    candidates.push((id, distance));
                }
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

    #[allow(dead_code)]
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

        // Collect all matching record IDs — FST traversal is fast, IDs are u32.
        // Expand each FST value through the posting-list sidecar so same-name
        // alternates are visible to autocomplete.
        let mut candidate_ids: Vec<u32> = Vec::new();
        while let Some((_key, value)) = stream.next() {
            for id in Self::decode_posting_list(self.record_lists_exact.as_ref(), value) {
                candidate_ids.push(id);
            }
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

    /// Direct FST lookup — returns the highest-importance record_id if found.
    /// Used by address geocoding to resolve city names to municipality IDs.
    /// When the posting-list sidecar exists, this is the first entry of the
    /// posting list (best match by importance). Otherwise the FST value is
    /// itself the record_id (legacy v2 layout).
    pub fn exact_lookup(&self, normalized: &str) -> Option<u32> {
        let v = self.fst_exact.get(normalized.as_bytes())?;
        let ids = Self::decode_posting_list(self.record_lists_exact.as_ref(), v);
        ids.first().copied()
    }

    /// Get admin2_id (municipality) for a record.
    pub fn record_admin2(&self, id: u32) -> Option<u16> {
        self.records.get(id).ok().map(|r| r.admin2_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `decode_posting_list` reads `[u16 count][u32 rec_id]*count` at the given
    /// offset, returning rec_ids in their stored order. Verifies that a
    /// missing sidecar falls through to single-id mode (legacy v2).
    #[test]
    fn test_decode_posting_list() {
        // Layout: at offset 0 → [count=2][7][13]; at offset 10 → [count=1][42]
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&7u32.to_le_bytes());
        buf.extend_from_slice(&13u32.to_le_bytes());
        // 2 + 8 = 10 bytes consumed
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&42u32.to_le_bytes());
        let sidecar = compressed_io::MmapOrVec::Vec(buf);

        // With sidecar: posting at offset 0
        let ids = HeimdallIndex::decode_posting_list(Some(&sidecar), 0);
        assert_eq!(ids, vec![7, 13]);

        // With sidecar: posting at offset 10
        let ids = HeimdallIndex::decode_posting_list(Some(&sidecar), 10);
        assert_eq!(ids, vec![42]);

        // Truncated/invalid offset returns empty
        let ids = HeimdallIndex::decode_posting_list(Some(&sidecar), 999);
        assert!(ids.is_empty());

        // No sidecar → value is a raw record_id (v2 backwards compat)
        let ids = HeimdallIndex::decode_posting_list(None, 99);
        assert_eq!(ids, vec![99]);
    }
}
