/// Runtime admin-polygon index for point-in-polygon queries
///
/// At build time `enrich.rs` already does PiP to assign admin1/admin2
/// to every place record. This module mirrors that capability at *query*
/// time so the API can override stored admin assignments with the
/// authoritative polygon containment when the index ships with a
/// `runtime_polygons.bin` sidecar.
///
/// Format on disk (bincode):
/// ```text
/// AdminPolygonFile {
///     version: u8 = 1,
///     admin1: Vec<RuntimePolygon>,   // counties / states (admin_level 3-4)
///     admin2: Vec<RuntimePolygon>,   // municipalities (admin_level 5-7)
/// }
/// ```
///
/// Each `RuntimePolygon` is a single closed ring with its tight bbox
/// and the `admin_id` (index into `admin.bin`). Multi-ring admin
/// regions (island municipalities) appear as multiple entries sharing
/// the same `admin_id`.
///
/// Lookup: linear scan with bbox pre-filter. ~400 polygons per typical
/// country (Sweden = 290 munis + 25 län), so the bbox check rejects
/// ~95 % cheaply and the full PiP test runs on a handful — sub-ms per
/// query without an R-tree.

use std::path::Path;

use geo::{Contains, Coord as GeoCoord, LineString, Point, Polygon};
use serde::{Deserialize, Serialize};

use crate::error::HeimdallError;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimePolygon {
    pub admin_id: u16,
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
    /// Closed ring of (lat, lon) coordinates.
    pub ring: Vec<(f64, f64)>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AdminPolygonFile {
    pub version: u8,
    pub admin1: Vec<RuntimePolygon>,
    pub admin2: Vec<RuntimePolygon>,
}

const RUNTIME_POLYGON_VERSION: u8 = 1;

/// In-memory PiP index. Polygons are pre-built into `geo::Polygon`
/// alongside their bbox so each query just scans the vec.
pub struct AdminPolygonIndex {
    admin1: Vec<IndexedPolygon>,
    admin2: Vec<IndexedPolygon>,
}

struct IndexedPolygon {
    admin_id: u16,
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
    polygon: Polygon<f64>,
}

impl AdminPolygonIndex {
    /// Construct an empty index — used when the sidecar is absent.
    pub fn empty() -> Self {
        Self { admin1: Vec::new(), admin2: Vec::new() }
    }

    /// Load `runtime_polygons.bin` from disk. Returns `Ok(None)` when
    /// the file doesn't exist (graceful degradation for old indices).
    pub fn open(path: &Path) -> Result<Option<Self>, HeimdallError> {
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(path)
            .map_err(|e| HeimdallError::IndexLoad(format!("read runtime_polygons.bin: {e}")))?;
        let file: AdminPolygonFile = bincode::deserialize(&bytes)
            .map_err(|e| HeimdallError::IndexLoad(format!("deserialize runtime_polygons.bin: {e}")))?;
        if file.version != RUNTIME_POLYGON_VERSION {
            return Err(HeimdallError::IndexLoad(format!(
                "runtime_polygons.bin version mismatch: got {}, expected {}",
                file.version, RUNTIME_POLYGON_VERSION
            )));
        }
        Ok(Some(Self {
            admin1: file.admin1.into_iter().map(into_indexed).collect(),
            admin2: file.admin2.into_iter().map(into_indexed).collect(),
        }))
    }

    /// True when the index has at least one polygon — distinguishes
    /// "no sidecar" from "sidecar present but empty".
    pub fn has_polygons(&self) -> bool {
        !self.admin1.is_empty() || !self.admin2.is_empty()
    }

    pub fn admin1_count(&self) -> usize { self.admin1.len() }
    pub fn admin2_count(&self) -> usize { self.admin2.len() }

    /// PiP for the admin1 (county/state) tier. `None` when no polygon
    /// contains the point (e.g. coordinate inaccuracy, missing
    /// geometry near the coast).
    pub fn admin1_at(&self, lat: f64, lon: f64) -> Option<u16> {
        find_containing(lat, lon, &self.admin1)
    }

    /// PiP for the admin2 (municipality) tier.
    pub fn admin2_at(&self, lat: f64, lon: f64) -> Option<u16> {
        find_containing(lat, lon, &self.admin2)
    }

    /// Combined: returns whichever tiers contain the point. Both are
    /// optional — admin1 might hit while admin2 misses (point in a
    /// county but in a coverage gap between munis), or vice versa.
    pub fn containing(&self, lat: f64, lon: f64) -> (Option<u16>, Option<u16>) {
        (self.admin1_at(lat, lon), self.admin2_at(lat, lon))
    }
}

fn into_indexed(p: RuntimePolygon) -> IndexedPolygon {
    let coords: Vec<GeoCoord<f64>> = p.ring.iter()
        .map(|(lat, lon)| GeoCoord { x: *lon, y: *lat })
        .collect();
    IndexedPolygon {
        admin_id: p.admin_id,
        min_lat: p.min_lat,
        max_lat: p.max_lat,
        min_lon: p.min_lon,
        max_lon: p.max_lon,
        polygon: Polygon::new(LineString::new(coords), vec![]),
    }
}

fn find_containing(lat: f64, lon: f64, polys: &[IndexedPolygon]) -> Option<u16> {
    let pt = Point::new(lon, lat);
    for p in polys {
        if lon < p.min_lon || lon > p.max_lon || lat < p.min_lat || lat > p.max_lat {
            continue;
        }
        if p.polygon.contains(&pt) {
            return Some(p.admin_id);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn axis_aligned_box(
        admin_id: u16,
        min_lat: f64, min_lon: f64,
        max_lat: f64, max_lon: f64,
    ) -> RuntimePolygon {
        // Closed ring (last == first) — required by `geo::Polygon`.
        let ring = vec![
            (min_lat, min_lon),
            (min_lat, max_lon),
            (max_lat, max_lon),
            (max_lat, min_lon),
            (min_lat, min_lon),
        ];
        RuntimePolygon { admin_id, min_lat, max_lat, min_lon, max_lon, ring }
    }

    fn unit_square(admin_id: u16, min_lat: f64, min_lon: f64) -> RuntimePolygon {
        axis_aligned_box(admin_id, min_lat, min_lon, min_lat + 1.0, min_lon + 1.0)
    }

    fn idx_from(admin1: Vec<RuntimePolygon>, admin2: Vec<RuntimePolygon>) -> AdminPolygonIndex {
        AdminPolygonIndex {
            admin1: admin1.into_iter().map(into_indexed).collect(),
            admin2: admin2.into_iter().map(into_indexed).collect(),
        }
    }

    #[test]
    fn pip_inside_returns_admin_id() {
        let idx = idx_from(vec![unit_square(7, 0.0, 0.0)], vec![]);
        assert_eq!(idx.admin1_at(0.5, 0.5), Some(7));
    }

    #[test]
    fn pip_outside_returns_none() {
        let idx = idx_from(vec![unit_square(7, 0.0, 0.0)], vec![]);
        assert_eq!(idx.admin1_at(2.0, 2.0), None);
    }

    #[test]
    fn pip_bbox_prefilter_skips_far_polygon() {
        // Two squares — only the first is relevant. Confirms the bbox
        // pre-filter is the cheap path for clearly-outside points.
        let idx = idx_from(
            vec![unit_square(1, 0.0, 0.0), unit_square(2, 50.0, 50.0)],
            vec![],
        );
        assert_eq!(idx.admin1_at(50.5, 50.5), Some(2));
        assert_eq!(idx.admin1_at(0.5, 0.5), Some(1));
    }

    #[test]
    fn pip_admin1_and_admin2_independent() {
        // County: 10x10 square covering origin. Muni: 2x2 square strictly
        // inside the county. Tests two cases: point in both, point in
        // county only.
        let idx = idx_from(
            vec![axis_aligned_box(1, 0.0, 0.0, 10.0, 10.0)],
            vec![axis_aligned_box(20, 2.0, 2.0, 4.0, 4.0)],
        );
        let (a1, a2) = idx.containing(3.0, 3.0);
        assert_eq!(a1, Some(1), "point inside county");
        assert_eq!(a2, Some(20), "point inside muni");

        // Outside muni but inside county
        let (a1, a2) = idx.containing(7.0, 7.0);
        assert_eq!(a1, Some(1));
        assert_eq!(a2, None);
    }

    #[test]
    fn open_returns_none_for_missing_file() {
        let p = std::path::Path::new("/tmp/heimdall-nonexistent-runtime-polygons.bin");
        assert!(matches!(AdminPolygonIndex::open(p), Ok(None)));
    }

    #[test]
    fn round_trip_serialize_deserialize() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime_polygons.bin");
        let file = AdminPolygonFile {
            version: RUNTIME_POLYGON_VERSION,
            admin1: vec![unit_square(1, 0.0, 0.0)],
            admin2: vec![unit_square(20, 0.2, 0.2)],
        };
        std::fs::write(&path, bincode::serialize(&file).unwrap()).unwrap();
        let idx = AdminPolygonIndex::open(&path).unwrap().expect("file present");
        assert_eq!(idx.admin1_count(), 1);
        assert_eq!(idx.admin2_count(), 1);
        assert_eq!(idx.admin1_at(0.5, 0.5), Some(1));
        assert_eq!(idx.admin2_at(0.5, 0.5), Some(20));
    }

    #[test]
    fn version_mismatch_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime_polygons.bin");
        let file = AdminPolygonFile {
            version: 99,
            admin1: vec![],
            admin2: vec![],
        };
        std::fs::write(&path, bincode::serialize(&file).unwrap()).unwrap();
        assert!(AdminPolygonIndex::open(&path).is_err());
    }
}
