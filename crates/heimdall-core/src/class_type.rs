//! Class/type interning sidecar — `class_types.bin` per index.
//!
//! `PlaceRecord::class_type` is a `u16` index into a per-country table of
//! `(class, value)` string pairs reflecting the original OSM tag pair
//! (e.g. `("place", "city")`, `("amenity", "restaurant")`, `("tourism",
//! "museum")`). The audit's `class_type: u16` field (Phase 2.2) trades
//! per-record string overhead for a tiny shared table — typical countries
//! interned ~200-400 unique pairs out of millions of records.
//!
//! Index 0 is reserved for "unknown / not set" so a default-zeroed
//! `PlaceRecord::class_type` reads back as `None`.

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::HeimdallError;

/// Unknown / not-set sentinel.
pub const CLASS_TYPE_UNKNOWN: u16 = 0;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClassTypeTable {
    /// pairs[0] is always ("", "") — the unknown sentinel.
    pairs: Vec<(String, String)>,
}

impl ClassTypeTable {
    pub fn new() -> Self {
        Self {
            pairs: vec![(String::new(), String::new())],
        }
    }

    /// Look up a pair by interned id. Returns None for the unknown sentinel
    /// or out-of-range ids.
    pub fn get(&self, id: u16) -> Option<(&str, &str)> {
        if id == CLASS_TYPE_UNKNOWN {
            return None;
        }
        self.pairs
            .get(id as usize)
            .map(|(c, v)| (c.as_str(), v.as_str()))
    }

    pub fn len(&self) -> usize { self.pairs.len() }
    pub fn is_empty(&self) -> bool { self.pairs.len() <= 1 }

    /// Load from `class_types.bin` (postcard). Returns an empty table if
    /// the file does not exist — older indices predate this sidecar.
    pub fn load(path: &Path) -> Result<Self, HeimdallError> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let bytes = std::fs::read(path)
            .map_err(|e| HeimdallError::Build(format!("read {}: {}", path.display(), e)))?;
        let table: Self = postcard::from_bytes(&bytes)
            .map_err(|e| HeimdallError::Build(format!("decode class_types.bin: {}", e)))?;
        Ok(table)
    }
}

/// Build-time interning helper. Shares the on-disk format with `ClassTypeTable`
/// — the build pipeline writes a `Builder`, the runtime loads a `Table`.
pub struct ClassTypeBuilder {
    pairs: Vec<(String, String)>,
    index: std::collections::HashMap<(String, String), u16>,
}

impl ClassTypeBuilder {
    pub fn new() -> Self {
        Self {
            pairs: vec![(String::new(), String::new())],
            index: std::collections::HashMap::new(),
        }
    }

    /// Intern `(class, value)`. Empty input returns the unknown sentinel.
    /// Capped at u16::MAX entries (~65k unique pairs — comfortably above
    /// any country's actual diversity).
    pub fn intern(&mut self, class: &str, value: &str) -> u16 {
        if class.is_empty() && value.is_empty() {
            return CLASS_TYPE_UNKNOWN;
        }
        let key = (class.to_owned(), value.to_owned());
        if let Some(&id) = self.index.get(&key) {
            return id;
        }
        if self.pairs.len() >= u16::MAX as usize {
            return CLASS_TYPE_UNKNOWN;
        }
        let id = self.pairs.len() as u16;
        self.pairs.push(key.clone());
        self.index.insert(key, id);
        id
    }

    pub fn write(&self, path: &Path) -> Result<(), HeimdallError> {
        let table = ClassTypeTable {
            pairs: self.pairs.clone(),
        };
        let bytes = postcard::to_allocvec(&table)
            .map_err(|e| HeimdallError::Build(format!("encode class_types.bin: {}", e)))?;
        std::fs::write(path, &bytes)
            .map_err(|e| HeimdallError::Build(format!("write {}: {}", path.display(), e)))?;
        Ok(())
    }

    pub fn len(&self) -> usize { self.pairs.len() }
}

impl Default for ClassTypeBuilder {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_dedupes() {
        let mut b = ClassTypeBuilder::new();
        let a = b.intern("place", "city");
        let c = b.intern("place", "city");
        let d = b.intern("amenity", "restaurant");
        assert_eq!(a, c);
        assert_ne!(a, d);
        assert_ne!(a, CLASS_TYPE_UNKNOWN);
        assert_eq!(b.len(), 3); // sentinel + 2
    }

    #[test]
    fn empty_intern_returns_sentinel() {
        let mut b = ClassTypeBuilder::new();
        assert_eq!(b.intern("", ""), CLASS_TYPE_UNKNOWN);
    }

    #[test]
    fn round_trip_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("class_types.bin");

        let mut b = ClassTypeBuilder::new();
        let id1 = b.intern("place", "city");
        let id2 = b.intern("amenity", "restaurant");
        b.write(&path).unwrap();

        let t = ClassTypeTable::load(&path).unwrap();
        assert_eq!(t.get(id1), Some(("place", "city")));
        assert_eq!(t.get(id2), Some(("amenity", "restaurant")));
        assert_eq!(t.get(CLASS_TYPE_UNKNOWN), None);
    }

    #[test]
    fn missing_file_yields_empty_table() {
        let t = ClassTypeTable::load(Path::new("/nonexistent/class_types.bin")).unwrap();
        assert!(t.is_empty());
    }
}
