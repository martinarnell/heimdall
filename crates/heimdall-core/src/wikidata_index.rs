//! Wikidata QID → record_id index sidecar (Phase 2.8 of the Nominatim parity audit).
//!
//! Postcard-encoded `Vec<(String, u32)>` sorted by QID. Loaded once at API
//! startup; binary-searched at query time. One entry per QID — when the
//! build pipeline sees more than one record with the same `wikidata=` tag
//! (e.g. admin relation + admin node), the highest-importance record wins.
//!
//! Exposed through `/search?q=Q12345`: the API short-circuits to a direct
//! lookup across every loaded country and returns matching records as
//! standard NominatimResult instances.

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::HeimdallError;

/// Read-side handle. Sorted by QID, binary-searchable in O(log n).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WikidataIndex {
    entries: Vec<(String, u32)>,
}

impl WikidataIndex {
    pub fn new() -> Self { Self::default() }

    pub fn load(path: &Path) -> Result<Self, HeimdallError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes = std::fs::read(path)
            .map_err(|e| HeimdallError::Build(format!("read {}: {}", path.display(), e)))?;
        let table: Self = postcard::from_bytes(&bytes)
            .map_err(|e| HeimdallError::Build(format!("decode {}: {}", path.display(), e)))?;
        Ok(table)
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// Look up the record_id for a QID (case-insensitive on the leading `Q`).
    /// Returns `None` for an unknown or malformed QID.
    pub fn get(&self, qid: &str) -> Option<u32> {
        let normalised = normalise_qid(qid)?;
        self.entries
            .binary_search_by(|(k, _)| k.as_str().cmp(normalised.as_str()))
            .ok()
            .map(|idx| self.entries[idx].1)
    }
}

/// Build-time accumulator. Caller pushes (QID, record_id, importance)
/// triples; on collision we keep the highest-importance record.
#[derive(Debug, Default)]
pub struct WikidataIndexBuilder {
    /// `qid → (record_id, importance)`. Importance breaks ties when more
    /// than one record carries the same `wikidata=` tag.
    by_qid: std::collections::HashMap<String, (u32, u16)>,
}

impl WikidataIndexBuilder {
    pub fn new() -> Self { Self::default() }

    /// Add a `(qid, record_id, importance)` triple. Empty / malformed QIDs
    /// are ignored. On collision, the higher-importance record wins (tie
    /// broken by lower record_id for determinism).
    pub fn add(&mut self, qid: &str, record_id: u32, importance: u16) {
        let q = match normalise_qid(qid) {
            Some(q) => q,
            None => return,
        };
        match self.by_qid.get_mut(&q) {
            Some(existing) => {
                let (rid, imp) = *existing;
                let better = importance > imp || (importance == imp && record_id < rid);
                if better {
                    *existing = (record_id, importance);
                }
            }
            None => {
                self.by_qid.insert(q, (record_id, importance));
            }
        }
    }

    pub fn len(&self) -> usize { self.by_qid.len() }
    pub fn is_empty(&self) -> bool { self.by_qid.is_empty() }

    pub fn write(self, path: &Path) -> Result<(), HeimdallError> {
        let mut entries: Vec<(String, u32)> = self.by_qid
            .into_iter()
            .map(|(q, (rid, _))| (q, rid))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let table = WikidataIndex { entries };
        let bytes = postcard::to_allocvec(&table)
            .map_err(|e| HeimdallError::Build(format!("encode {}: {}", path.display(), e)))?;
        std::fs::write(path, &bytes)
            .map_err(|e| HeimdallError::Build(format!("write {}: {}", path.display(), e)))?;
        Ok(())
    }
}

/// Validate and uppercase-normalise a QID. Accepts `Q\d+` only; returns
/// `None` on anything else (handles upstream tag noise like `q12345`,
/// stray whitespace, multi-QID values, …).
pub fn normalise_qid(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.len() < 2 {
        return None;
    }
    let bytes = trimmed.as_bytes();
    if !(bytes[0] == b'Q' || bytes[0] == b'q') {
        return None;
    }
    if !bytes[1..].iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut out = String::with_capacity(trimmed.len());
    out.push('Q');
    out.push_str(&trimmed[1..]);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wikidata_qids.bin");

        let mut b = WikidataIndexBuilder::new();
        b.add("Q1486", 7, 10000);
        b.add("Q1428", 3, 9000);
        b.add("invalid", 99, 1);
        b.write(&path).unwrap();

        let t = WikidataIndex::load(&path).unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(t.get("Q1486"), Some(7));
        assert_eq!(t.get("Q1428"), Some(3));
        assert_eq!(t.get("Q9999"), None);
    }

    #[test]
    fn collision_prefers_higher_importance() {
        let mut b = WikidataIndexBuilder::new();
        b.add("Q1486", 100, 5000);
        b.add("Q1486", 200, 9000); // wins on importance
        b.add("Q1486", 300, 9000); // ties on importance, loses on record_id
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wikidata_qids.bin");
        b.write(&path).unwrap();
        let t = WikidataIndex::load(&path).unwrap();
        assert_eq!(t.get("Q1486"), Some(200));
    }

    #[test]
    fn case_insensitive_get() {
        let mut b = WikidataIndexBuilder::new();
        b.add("Q42", 1, 100);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wikidata_qids.bin");
        b.write(&path).unwrap();
        let t = WikidataIndex::load(&path).unwrap();
        assert_eq!(t.get("q42"), Some(1));
        assert_eq!(t.get("  Q42  "), Some(1));
    }

    #[test]
    fn missing_file_yields_empty_index() {
        let t = WikidataIndex::load(Path::new("/nonexistent/wikidata_qids.bin")).unwrap();
        assert!(t.is_empty());
        assert!(t.get("Q1").is_none());
    }

    #[test]
    fn normalise_rejects_garbage() {
        assert_eq!(normalise_qid("Q42"), Some("Q42".into()));
        assert_eq!(normalise_qid("q42"), Some("Q42".into()));
        assert_eq!(normalise_qid(""), None);
        assert_eq!(normalise_qid("Q"), None);
        assert_eq!(normalise_qid("Q42a"), None);
        assert_eq!(normalise_qid("42"), None);
        assert_eq!(normalise_qid("Q42;Q43"), None);
    }
}
