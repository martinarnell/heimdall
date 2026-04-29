/// global_index.rs — Global name index: one FST for all countries.
///
/// Replaces the O(192) sequential country FST scan with a single O(1) global
/// FST lookup. Two FSTs (exact + phonetic) map normalized names to offsets
/// into postings files. Each posting list contains (country_id, record_id,
/// importance) tuples sorted by importance descending.
///
/// Files on disk (stored in a `global/` directory):
///
/// ```text
/// global/
///   fst_exact.fst        - Exact FST: normalized_name -> postings_offset (u64)
///   fst_phonetic.fst     - Phonetic FST: phonetic_key -> postings_offset (u64)
///   postings.bin         - Postings lists for the exact FST
///   postings_phonetic.bin - Postings lists for the phonetic FST
/// ```
///
/// Postings format (`postings.bin` / `postings_phonetic.bin`):
///
/// ```text
/// At each offset:
///   count: u16               (number of entries)
///   entries: [PostingEntry; count]
///
/// PostingEntry (8 bytes):
///   country_id:  u16         (index into the countries array)
///   record_id:   u32         (record ID within that country)
///   importance:  u16         (scaled 0–65535, for sorting)
/// ```

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use fst::automaton::{Levenshtein, Str};
use fst::{Automaton, IntoStreamer, Map, Streamer};
use memmap2::Mmap;

use crate::compressed_io;
use crate::error::HeimdallError;

// ---------------------------------------------------------------------------
// PostingEntry
// ---------------------------------------------------------------------------

/// A single entry in a postings list.
#[derive(Debug, Clone)]
pub struct PostingEntry {
    pub country_id: u16,
    pub record_id: u32,
    pub importance: u16,
}

/// Size of a serialized PostingEntry in bytes.
const POSTING_ENTRY_SIZE: usize = 8; // u16 + u32 + u16

// ---------------------------------------------------------------------------
// GlobalIndex — query-time reader
// ---------------------------------------------------------------------------

pub struct GlobalIndex {
    dir: PathBuf,
    fst_exact: Map<compressed_io::MmapOrVec>,
    fst_phonetic: Map<compressed_io::MmapOrVec>,
    postings_exact_mmap: Mmap,
    postings_phonetic_mmap: Mmap,
}

impl GlobalIndex {
    /// Open a global index from a directory containing fst_exact.fst,
    /// fst_phonetic.fst, postings.bin, and postings_phonetic.bin.
    pub fn open(dir: &Path) -> Result<Self, HeimdallError> {
        let fst_exact = {
            let data = compressed_io::mmap_or_decompress(&dir.join("fst_exact.fst"))?;
            Map::new(data).map_err(HeimdallError::Fst)?
        };

        let fst_phonetic = {
            let data = compressed_io::mmap_or_decompress(&dir.join("fst_phonetic.fst"))?;
            Map::new(data).map_err(HeimdallError::Fst)?
        };

        let postings_exact_mmap = {
            let file = std::fs::File::open(dir.join("postings.bin"))?;
            unsafe { Mmap::map(&file)? }
        };

        let postings_phonetic_mmap = {
            let file = std::fs::File::open(dir.join("postings_phonetic.bin"))?;
            unsafe { Mmap::map(&file)? }
        };

        Ok(Self {
            dir: dir.to_owned(),
            fst_exact,
            fst_phonetic,
            postings_exact_mmap,
            postings_phonetic_mmap,
        })
    }

    /// Try to open a global index. Returns `None` if the directory or any
    /// required file does not exist. Returns an error only for genuine I/O
    /// or format problems.
    pub fn try_open(dir: &Path) -> Result<Option<Self>, HeimdallError> {
        if !dir.is_dir() {
            return Ok(None);
        }
        // Check that all four files exist before attempting a full open.
        let required = [
            "fst_exact.fst",
            "fst_phonetic.fst",
            "postings.bin",
            "postings_phonetic.bin",
        ];
        for name in &required {
            if !dir.join(name).exists() {
                return Ok(None);
            }
        }
        Self::open(dir).map(Some)
    }

    /// The directory this global index was loaded from.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // -----------------------------------------------------------------------
    // Lookups
    // -----------------------------------------------------------------------

    /// Exact lookup: returns posting entries sorted by importance (highest first).
    pub fn exact_lookup(&self, normalized: &str) -> Vec<PostingEntry> {
        match self.fst_exact.get(normalized.as_bytes()) {
            Some(offset) => self.read_postings(&self.postings_exact_mmap, offset),
            None => vec![],
        }
    }

    /// Phonetic lookup: returns posting entries sorted by importance (highest first).
    pub fn phonetic_lookup(&self, phonetic_key: &str) -> Vec<PostingEntry> {
        match self.fst_phonetic.get(phonetic_key.as_bytes()) {
            Some(offset) => self.read_postings(&self.postings_phonetic_mmap, offset),
            None => vec![],
        }
    }

    /// Levenshtein fuzzy lookup over the exact FST.
    ///
    /// Returns all matching entries deduplicated by (country_id, record_id),
    /// sorted by importance descending.
    pub fn fuzzy_lookup(&self, query: &str, max_distance: u32) -> Vec<PostingEntry> {
        // Guard: the fst crate's Levenshtein DFA builder can panic on short
        // multi-byte UTF-8 inputs (e.g. 3-char Cyrillic = 6 bytes, distance 1).
        // Require enough bytes so the DFA stays within its internal limits.
        let min_bytes = (max_distance as usize + 1) * 4;
        if query.len() < min_bytes {
            return vec![];
        }

        let lev = match Levenshtein::new(query, max_distance) {
            Ok(l) => l,
            Err(_) => return vec![],
        };
        let mut stream = self.fst_exact.search(lev).into_stream();
        let mut all_entries: Vec<PostingEntry> = Vec::new();
        let mut seen: HashSet<(u16, u32)> = HashSet::new();

        while let Some((_key, offset)) = stream.next() {
            let postings = self.read_postings(&self.postings_exact_mmap, offset);
            for entry in postings {
                let key = (entry.country_id, entry.record_id);
                if seen.insert(key) {
                    all_entries.push(entry);
                }
            }
        }

        all_entries.sort_by(|a, b| b.importance.cmp(&a.importance));
        all_entries
    }

    /// Prefix search for autocomplete over the exact FST.
    ///
    /// Returns entries deduplicated by (country_id, record_id), sorted by
    /// importance descending, truncated to `limit`.
    pub fn prefix_search(&self, prefix: &str, limit: usize) -> Vec<PostingEntry> {
        let automaton = Str::new(prefix).starts_with();
        let mut stream = self.fst_exact.search(automaton).into_stream();
        let mut all_entries: Vec<PostingEntry> = Vec::new();
        let mut seen: HashSet<(u16, u32)> = HashSet::new();

        while let Some((_key, offset)) = stream.next() {
            let postings = self.read_postings(&self.postings_exact_mmap, offset);
            for entry in postings {
                let key = (entry.country_id, entry.record_id);
                if seen.insert(key) {
                    all_entries.push(entry);
                }
            }
        }

        all_entries.sort_by(|a, b| b.importance.cmp(&a.importance));
        all_entries.truncate(limit);
        all_entries
    }

    // -----------------------------------------------------------------------
    // Internal: postings reader
    // -----------------------------------------------------------------------

    /// Read a postings list from the given mmap at the given byte offset.
    ///
    /// Layout: `count: u16` followed by `count` × 8-byte entries.
    fn read_postings(&self, mmap: &Mmap, offset: u64) -> Vec<PostingEntry> {
        let data: &[u8] = mmap.as_ref();
        let off = offset as usize;

        // Need at least 2 bytes for the count.
        if off + 2 > data.len() {
            return vec![];
        }

        let count =
            u16::from_le_bytes(data[off..off + 2].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(count);
        let mut pos = off + 2;

        for _ in 0..count {
            if pos + POSTING_ENTRY_SIZE > data.len() {
                break;
            }
            let country_id =
                u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
            let record_id =
                u32::from_le_bytes(data[pos + 2..pos + 6].try_into().unwrap());
            let importance =
                u16::from_le_bytes(data[pos + 6..pos + 8].try_into().unwrap());
            entries.push(PostingEntry {
                country_id,
                record_id,
                importance,
            });
            pos += POSTING_ENTRY_SIZE;
        }

        entries
    }
}

// ---------------------------------------------------------------------------
// GlobalIndexBuilder — build-time writer
// ---------------------------------------------------------------------------

/// Builder for creating the global index at build time.
///
/// Workflow:
///   1. Create a builder with `new()`.
///   2. For each country, iterate its exact and phonetic FSTs and call
///      `add_exact` / `add_phonetic` for every entry.
///   3. Call `write(dir)` to produce the four on-disk files.
pub struct GlobalIndexBuilder {
    entries_exact: Vec<(String, u16, u32, u16)>,
    entries_phonetic: Vec<(String, u16, u32, u16)>,
}

impl GlobalIndexBuilder {
    pub fn new() -> Self {
        Self {
            entries_exact: Vec::new(),
            entries_phonetic: Vec::new(),
        }
    }

    /// Add an entry to the exact FST.
    ///
    /// * `name`        — normalized place name (the FST key)
    /// * `country_id`  — index into the countries array
    /// * `record_id`   — record ID within that country's RecordStore
    /// * `importance`  — scaled 0–65535
    pub fn add_exact(
        &mut self,
        name: String,
        country_id: u16,
        record_id: u32,
        importance: u16,
    ) {
        self.entries_exact
            .push((name, country_id, record_id, importance));
    }

    /// Add an entry to the phonetic FST.
    pub fn add_phonetic(
        &mut self,
        name: String,
        country_id: u16,
        record_id: u32,
        importance: u16,
    ) {
        self.entries_phonetic
            .push((name, country_id, record_id, importance));
    }

    /// Number of exact entries accumulated so far.
    pub fn exact_count(&self) -> usize {
        self.entries_exact.len()
    }

    /// Number of phonetic entries accumulated so far.
    pub fn phonetic_count(&self) -> usize {
        self.entries_phonetic.len()
    }

    /// Build and write the global index to `dir`.
    ///
    /// Creates the directory if it does not exist. Produces:
    ///   - `fst_exact.fst`
    ///   - `postings.bin`
    ///   - `fst_phonetic.fst`
    ///   - `postings_phonetic.bin`
    pub fn write(&mut self, dir: &Path) -> Result<(), HeimdallError> {
        std::fs::create_dir_all(dir)?;

        // --- Exact ---
        self.entries_exact.sort_by(|a, b| a.0.cmp(&b.0));
        let (exact_fst_data, exact_postings) =
            Self::build_fst_and_postings(&self.entries_exact)?;
        std::fs::write(dir.join("postings.bin"), &exact_postings)?;
        std::fs::write(dir.join("fst_exact.fst"), &exact_fst_data)?;

        // --- Phonetic ---
        self.entries_phonetic.sort_by(|a, b| a.0.cmp(&b.0));
        let (phonetic_fst_data, phonetic_postings) =
            Self::build_fst_and_postings(&self.entries_phonetic)?;
        std::fs::write(
            dir.join("postings_phonetic.bin"),
            &phonetic_postings,
        )?;
        std::fs::write(dir.join("fst_phonetic.fst"), &phonetic_fst_data)?;

        Ok(())
    }

    /// Build an FST and its postings file from a sorted list of entries.
    ///
    /// The entries **must** be sorted by name (ascending, lexicographic) before
    /// calling this function — `write()` ensures that.
    ///
    /// Returns `(fst_bytes, postings_bytes)`.
    fn build_fst_and_postings(
        entries: &[(String, u16, u32, u16)],
    ) -> Result<(Vec<u8>, Vec<u8>), HeimdallError> {
        let mut postings_buf: Vec<u8> = Vec::new();
        let mut fst_builder = fst::MapBuilder::new(Vec::new())
            .map_err(|e| HeimdallError::Build(format!("FST builder init: {}", e)))?;

        let mut i = 0;
        while i < entries.len() {
            let name = &entries[i].0;
            let offset = postings_buf.len() as u64;

            // Collect all entries that share this name.
            let mut group: Vec<(u16, u32, u16)> = Vec::new();
            while i < entries.len() && entries[i].0 == *name {
                group.push((entries[i].1, entries[i].2, entries[i].3));
                i += 1;
            }

            // Sort by importance descending so the most important hits come
            // first when reading.
            group.sort_by(|a, b| b.2.cmp(&a.2));

            // Deduplicate by (country_id, record_id) — keep the entry with
            // the highest importance (which comes first after sorting).
            group.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

            // Write the posting list.
            let count = group.len().min(u16::MAX as usize);
            postings_buf
                .extend_from_slice(&(count as u16).to_le_bytes());
            for &(country_id, record_id, importance) in
                group.iter().take(count)
            {
                postings_buf
                    .extend_from_slice(&country_id.to_le_bytes());
                postings_buf
                    .extend_from_slice(&record_id.to_le_bytes());
                postings_buf
                    .extend_from_slice(&importance.to_le_bytes());
            }

            fst_builder
                .insert(name.as_bytes(), offset)
                .map_err(|e| {
                    HeimdallError::Build(format!("FST insert: {}", e))
                })?;
        }

        let fst_data = fst_builder.into_inner().map_err(|e| {
            HeimdallError::Build(format!("FST build: {}", e))
        })?;

        Ok((fst_data, postings_buf))
    }
}

impl Default for GlobalIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip: build a small global index and query it.
    #[test]
    fn test_round_trip() {
        let tmp = std::env::temp_dir().join("heimdall_global_index_test");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut builder = GlobalIndexBuilder::new();

        // Country 0: "stockholm" (importance 100), "gothenburg" (importance 80)
        builder.add_exact("stockholm".into(), 0, 0, 100);
        builder.add_exact("gothenburg".into(), 0, 1, 80);

        // Country 1: "stockholm" again — different country, same name
        builder.add_exact("stockholm".into(), 1, 42, 90);

        // Phonetic
        builder.add_phonetic("STKL".into(), 0, 0, 100);
        builder.add_phonetic("STKL".into(), 1, 42, 90);
        builder.add_phonetic("GTNB".into(), 0, 1, 80);

        builder.write(&tmp).expect("write failed");

        let idx = GlobalIndex::open(&tmp).expect("open failed");

        // Exact lookup — "stockholm" should return 2 entries
        let hits = idx.exact_lookup("stockholm");
        assert_eq!(hits.len(), 2);
        // Sorted by importance descending: country 0 (100) then country 1 (90)
        assert_eq!(hits[0].country_id, 0);
        assert_eq!(hits[0].record_id, 0);
        assert_eq!(hits[0].importance, 100);
        assert_eq!(hits[1].country_id, 1);
        assert_eq!(hits[1].record_id, 42);
        assert_eq!(hits[1].importance, 90);

        // Miss
        let hits = idx.exact_lookup("oslo");
        assert!(hits.is_empty());

        // Phonetic lookup
        let hits = idx.phonetic_lookup("STKL");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].importance, 100);

        // Prefix search — "stock" matches "stockholm" which exists in 2 countries
        let hits = idx.prefix_search("stock", 10);
        assert_eq!(hits.len(), 2);
        // Sorted by importance: country 0 (100) first, then country 1 (90)
        assert_eq!(hits[0].importance, 100);
        assert_eq!(hits[0].record_id, 0);

        // Prefix search — "g" matches "gothenburg"
        let hits = idx.prefix_search("g", 10);
        assert_eq!(hits.len(), 1);

        // Fuzzy lookup — "stokholm" → "stockholm" at distance 1
        let hits = idx.fuzzy_lookup("stokholm", 1);
        assert_eq!(hits.len(), 2);

        // try_open on existing dir
        let maybe = GlobalIndex::try_open(&tmp).expect("try_open failed");
        assert!(maybe.is_some());

        // try_open on missing dir
        let missing =
            GlobalIndex::try_open(Path::new("/tmp/no_such_global_index"))
                .expect("try_open should not error on missing dir");
        assert!(missing.is_none());

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// Deduplication: same (country_id, record_id) with different importance
    /// should keep only the highest.
    #[test]
    fn test_dedup() {
        let tmp =
            std::env::temp_dir().join("heimdall_global_index_dedup_test");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut builder = GlobalIndexBuilder::new();
        // Same name, same country+record, different importance.
        builder.add_exact("oslo".into(), 0, 5, 50);
        builder.add_exact("oslo".into(), 0, 5, 90);
        builder.add_phonetic("OSL".into(), 0, 5, 50);

        builder.write(&tmp).expect("write failed");
        let idx = GlobalIndex::open(&tmp).expect("open failed");

        let hits = idx.exact_lookup("oslo");
        assert_eq!(hits.len(), 1, "should be deduplicated");
        assert_eq!(hits[0].importance, 90, "should keep highest importance");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    /// Empty builder produces valid (empty) files that can be opened.
    #[test]
    fn test_empty_index() {
        let tmp =
            std::env::temp_dir().join("heimdall_global_index_empty_test");
        let _ = std::fs::remove_dir_all(&tmp);

        let mut builder = GlobalIndexBuilder::new();
        builder.write(&tmp).expect("write empty failed");

        let idx = GlobalIndex::open(&tmp).expect("open empty failed");
        assert!(idx.exact_lookup("anything").is_empty());
        assert!(idx.phonetic_lookup("anything").is_empty());
        assert!(idx.fuzzy_lookup("anything", 1).is_empty());
        assert!(idx.prefix_search("a", 10).is_empty());

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
