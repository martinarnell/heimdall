//! Sparse per-record key/value sidecars — `extratags.bin` and `namedetails.bin`.
//!
//! Phase 2.3 of the Nominatim parity audit. Stored on disk as a postcard-encoded
//! `Vec<(u32, Vec<(String, String)>)>` sorted by record_id. Querying does a
//! binary search; missing record_ids return `None`.
//!
//! Both files are optional — pre-Phase-2.3 indices simply lack them and the
//! API responds with `None` for `?extratags=1` / `?namedetails=1`.

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::HeimdallError;

/// Read-side handle. Sparse: only record_ids with non-empty payloads are stored.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KvSidecar {
    /// Sorted by `record_id` — binary-searchable in O(log n).
    entries: Vec<(u32, Vec<(String, String)>)>,
}

impl KvSidecar {
    pub fn new() -> Self { Self::default() }

    /// Load from disk. Returns an empty sidecar when the file is missing —
    /// older indices predate this layer and clients still see `None`.
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

    /// Look up the payload for `record_id`. `None` when absent or empty.
    pub fn get(&self, record_id: u32) -> Option<&[(String, String)]> {
        match self.entries.binary_search_by_key(&record_id, |(id, _)| *id) {
            Ok(idx) => Some(self.entries[idx].1.as_slice()),
            Err(_) => None,
        }
    }
}

/// Build-time accumulator. Callers push entries in arbitrary order; `write`
/// sorts them once before serialising.
#[derive(Debug, Default)]
pub struct KvSidecarBuilder {
    entries: Vec<(u32, Vec<(String, String)>)>,
}

impl KvSidecarBuilder {
    pub fn new() -> Self { Self::default() }

    /// Add a payload for `record_id`. Empty `pairs` are silently dropped so
    /// the sidecar stays sparse.
    pub fn add(&mut self, record_id: u32, pairs: Vec<(String, String)>) {
        if pairs.is_empty() { return; }
        self.entries.push((record_id, pairs));
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// Serialise to `path` as a postcard-encoded `KvSidecar`. Sorts by
    /// `record_id` first so the on-disk shape matches the read-side
    /// invariant.
    pub fn write(mut self, path: &Path) -> Result<(), HeimdallError> {
        self.entries.sort_by_key(|(id, _)| *id);
        let table = KvSidecar { entries: self.entries };
        let bytes = postcard::to_allocvec(&table)
            .map_err(|e| HeimdallError::Build(format!("encode {}: {}", path.display(), e)))?;
        std::fs::write(path, &bytes)
            .map_err(|e| HeimdallError::Build(format!("write {}: {}", path.display(), e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("extratags.bin");

        let mut b = KvSidecarBuilder::new();
        b.add(0, vec![("population".into(), "1500000".into())]);
        b.add(7, vec![
            ("wikidata".into(), "Q1486".into()),
            ("wikipedia".into(), "en:Stockholm".into()),
        ]);
        b.add(3, vec![("ele".into(), "12".into())]);
        b.write(&path).unwrap();

        let t = KvSidecar::load(&path).unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get(0).unwrap()[0].1, "1500000");
        assert_eq!(t.get(3).unwrap()[0].1, "12");
        assert_eq!(t.get(7).unwrap().len(), 2);
        assert!(t.get(99).is_none());
    }

    #[test]
    fn missing_file_yields_empty_sidecar() {
        let t = KvSidecar::load(Path::new("/nonexistent/extratags.bin")).unwrap();
        assert!(t.is_empty());
        assert!(t.get(0).is_none());
    }

    #[test]
    fn empty_payloads_are_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("namedetails.bin");

        let mut b = KvSidecarBuilder::new();
        b.add(1, vec![]);
        b.add(2, vec![("name".into(), "Stockholm".into())]);
        b.write(&path).unwrap();

        let t = KvSidecar::load(&path).unwrap();
        assert_eq!(t.len(), 1);
        assert!(t.get(1).is_none());
        assert_eq!(t.get(2).unwrap()[0].1, "Stockholm");
    }
}
