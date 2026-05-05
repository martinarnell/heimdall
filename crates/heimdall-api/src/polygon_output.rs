/// Polygon output format converters (Phase 3.1 / audit #17).
///
/// Renders a list of rings as GeoJSON / KML / SVG / WKT, mirroring
/// Nominatim's `polygon_geojson` / `polygon_kml` / `polygon_svg` /
/// `polygon_text` parameters.
///
/// Input rings are `(lat, lon)` pairs — matching the on-disk admin
/// polygon format. All output formats use `(lon, lat)` ordering as is
/// standard for geographic data.
///
/// One ring renders as a single-feature payload (`Polygon` for GeoJSON /
/// KML, `POLYGON((...))` for WKT). Multiple rings — typical of
/// archipelago municipalities (Stockholms kommun, Tromsø kommune, …)
/// or multi-island states — render as `MultiPolygon` / multi-WKT.
///
/// The API also accepts polygon flags on synthetic results (postcodes,
/// raw address rows). Those have no real geometry, so the caller falls
/// back to the record's bbox rendered as a degenerate four-corner
/// polygon. That fallback is composed at the call site rather than in
/// this module.

use serde::ser::SerializeMap;
use serde_json::{json, Value};

use heimdall_core::admin_polygons::AdminTier;
use heimdall_core::types::PlaceType;

/// Parsed `polygon_*` query flags. Constructed from the per-endpoint
/// param structs (Search/Reverse/Lookup/Details) so all four endpoints
/// share the same response logic.
#[derive(Debug, Clone, Copy, Default)]
pub struct PolygonRequest {
    pub geojson: bool,
    pub kml: bool,
    pub svg: bool,
    pub text: bool,
    /// Douglas-Peucker epsilon in degrees. `0.0` returns the original ring.
    pub threshold: f64,
}

impl PolygonRequest {
    /// Build from raw query params. Negative / NaN thresholds are clamped
    /// to 0.0 (no simplification) — matches Nominatim's loose handling.
    pub fn from_flags(
        geojson: u8,
        kml: u8,
        svg: u8,
        text: u8,
        threshold: Option<f64>,
    ) -> Self {
        let t = threshold.unwrap_or(0.0);
        let t = if t.is_finite() && t > 0.0 { t } else { 0.0 };
        Self {
            geojson: geojson != 0,
            kml: kml != 0,
            svg: svg != 0,
            text: text != 0,
            threshold: t,
        }
    }

    /// True iff at least one polygon format is requested.
    pub fn any(&self) -> bool {
        self.geojson || self.kml || self.svg || self.text
    }
}

/// Pre-rendered polygon payloads, one per requested format. Populated
/// lazily — fields are `None` when the corresponding flag was off.
#[derive(Debug, Default, Clone)]
pub struct PolygonOutputs {
    pub geojson: Option<Value>,
    pub kml: Option<String>,
    pub svg: Option<String>,
    pub text: Option<String>,
}

impl PolygonOutputs {
    /// Render the requested formats from the supplied rings. Empty
    /// rings collapse to all-`None` (caller decides whether to elide
    /// the fields).
    pub fn render(rings: &[Vec<(f64, f64)>], req: PolygonRequest) -> Self {
        if rings.is_empty() || !req.any() {
            return Self::default();
        }
        Self {
            geojson: req.geojson.then(|| rings_to_geojson(rings)),
            kml: req.kml.then(|| rings_to_kml(rings)),
            svg: req.svg.then(|| rings_to_svg(rings)),
            text: req.text.then(|| rings_to_wkt(rings)),
        }
    }

    /// Emit the populated formats into the active serde map. Field
    /// names mirror Nominatim: `geojson` (object), `geokml` (string),
    /// `svg` (string), `geotext` (WKT string).
    pub fn serialize_into<S: serde::ser::SerializeMap>(
        &self,
        m: &mut S,
    ) -> Result<(), S::Error> {
        if let Some(ref v) = self.geojson { m.serialize_entry("geojson", v)?; }
        if let Some(ref v) = self.kml      { m.serialize_entry("geokml", v)?;  }
        if let Some(ref v) = self.svg      { m.serialize_entry("svg", v)?;     }
        if let Some(ref v) = self.text     { m.serialize_entry("geotext", v)?; }
        Ok(())
    }
}

// Make `SerializeMap` from the trait above usable without the unused
// import warning even if a downstream caller doesn't use it directly.
#[allow(dead_code)]
fn _serialize_into_marker<M: SerializeMap>(_: &M) {}

/// Map a place_type to the admin tier whose polygon should serve as the
/// record's geometry. Returns `None` for non-admin records (POIs,
/// landmarks, suburbs, neighbourhoods) — those fall back to bbox-as-ring.
///
/// The mapping relies on the build-time PiP rule: an admin relation's
/// centroid lands inside its own polygon, so `record.admin1_id` /
/// `admin2_id` resolves to the record's own admin entry. For
/// place=city/town nodes that aren't standalone relations, the
/// containing muni's polygon is still typically what the user expects
/// (Stockholm city node → Stockholms kommun polygon), so we accept that
/// approximation.
pub fn place_type_to_admin_tier(pt: PlaceType) -> Option<AdminTier> {
    match pt {
        PlaceType::Country | PlaceType::State => Some(AdminTier::Admin1),
        PlaceType::County | PlaceType::City | PlaceType::Town
            => Some(AdminTier::Admin2),
        // Suburb / Quarter / Neighbourhood / Village / Hamlet — these
        // typically aren't admin entities of their own, so the
        // containing-admin polygon would mislead. Bbox fallback.
        _ => None,
    }
}

/// Render rings as GeoJSON. One ring → `Polygon`; multiple → `MultiPolygon`.
/// Empty input returns `null` so the JSON field can be elided.
pub fn rings_to_geojson(rings: &[Vec<(f64, f64)>]) -> Value {
    if rings.is_empty() {
        return Value::Null;
    }
    if rings.len() == 1 {
        let ring = &rings[0];
        let coords: Vec<[f64; 2]> = ring.iter().map(|&(lat, lon)| [lon, lat]).collect();
        json!({
            "type": "Polygon",
            "coordinates": [coords],
        })
    } else {
        let polys: Vec<Vec<Vec<[f64; 2]>>> = rings.iter()
            .map(|r| vec![r.iter().map(|&(lat, lon)| [lon, lat]).collect()])
            .collect();
        json!({
            "type": "MultiPolygon",
            "coordinates": polys,
        })
    }
}

/// Render rings as a Well-Known Text geometry. Single ring →
/// `POLYGON((lon lat, …))`; multiple rings → `MULTIPOLYGON(((…)),((…)))`.
/// Empty input returns an empty string.
pub fn rings_to_wkt(rings: &[Vec<(f64, f64)>]) -> String {
    if rings.is_empty() {
        return String::new();
    }
    let mut buf = String::new();
    let format_ring = |buf: &mut String, ring: &[(f64, f64)]| {
        buf.push('(');
        for (i, &(lat, lon)) in ring.iter().enumerate() {
            if i > 0 { buf.push(','); }
            // 7 decimals matches Nominatim's coordinate precision.
            use std::fmt::Write;
            let _ = write!(buf, "{:.7} {:.7}", lon, lat);
        }
        buf.push(')');
    };
    if rings.len() == 1 {
        buf.push_str("POLYGON(");
        format_ring(&mut buf, &rings[0]);
        buf.push(')');
    } else {
        buf.push_str("MULTIPOLYGON(");
        for (i, ring) in rings.iter().enumerate() {
            if i > 0 { buf.push(','); }
            buf.push('(');
            format_ring(&mut buf, ring);
            buf.push(')');
        }
        buf.push(')');
    }
    buf
}

/// Render rings as a KML `<Polygon>` (single ring) or `<MultiGeometry>`
/// of `<Polygon>` elements (multiple rings). KML coordinates are
/// `lon,lat` (no space inside a coordinate, separated by spaces between
/// vertices) and live inside `<outerBoundaryIs><LinearRing>`.
pub fn rings_to_kml(rings: &[Vec<(f64, f64)>]) -> String {
    if rings.is_empty() {
        return String::new();
    }
    use std::fmt::Write;
    let format_polygon = |buf: &mut String, ring: &[(f64, f64)]| {
        buf.push_str("<Polygon><outerBoundaryIs><LinearRing><coordinates>");
        for (i, &(lat, lon)) in ring.iter().enumerate() {
            if i > 0 { buf.push(' '); }
            let _ = write!(buf, "{:.7},{:.7}", lon, lat);
        }
        buf.push_str("</coordinates></LinearRing></outerBoundaryIs></Polygon>");
    };
    if rings.len() == 1 {
        let mut buf = String::new();
        format_polygon(&mut buf, &rings[0]);
        buf
    } else {
        let mut buf = String::from("<MultiGeometry>");
        for ring in rings {
            format_polygon(&mut buf, ring);
        }
        buf.push_str("</MultiGeometry>");
        buf
    }
}

/// Render rings as an SVG `path` `d` attribute. Each ring becomes a
/// `M lon lat L lon lat … Z` subpath; multiple rings concatenate into a
/// single path string. Coordinates are unscaled — the consumer is
/// expected to apply its own viewport / transform.
///
/// SVG's y-axis points down, so callers flipping for screen rendering
/// should multiply lat by -1 in their transform; we keep the geographic
/// orientation here so the path round-trips through any tool that
/// expects `(lon, lat)` ordering.
pub fn rings_to_svg(rings: &[Vec<(f64, f64)>]) -> String {
    if rings.is_empty() {
        return String::new();
    }
    use std::fmt::Write;
    let mut buf = String::new();
    for ring in rings {
        if ring.is_empty() { continue; }
        for (i, &(lat, lon)) in ring.iter().enumerate() {
            let cmd = if i == 0 { 'M' } else { 'L' };
            if !buf.is_empty() { buf.push(' '); }
            let _ = write!(buf, "{} {:.7} {:.7}", cmd, lon, lat);
        }
        buf.push_str(" Z");
    }
    buf
}

/// Render the record's bbox as a four-corner polygon, suitable as a
/// fallback for non-admin records that don't have a stored polygon.
/// Output is `(lat, lon)`-ordered — feed straight into the format
/// converters above. Always returns a single closed ring.
pub fn bbox_as_ring(south: f64, north: f64, west: f64, east: f64) -> Vec<(f64, f64)> {
    vec![
        (south, west),
        (south, east),
        (north, east),
        (north, west),
        (south, west),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit_square_ring() -> Vec<(f64, f64)> {
        vec![
            (0.0, 0.0),
            (0.0, 1.0),
            (1.0, 1.0),
            (1.0, 0.0),
            (0.0, 0.0),
        ]
    }

    #[test]
    fn geojson_single_ring_polygon() {
        let v = rings_to_geojson(&[unit_square_ring()]);
        assert_eq!(v["type"], "Polygon");
        let coords = v["coordinates"][0].as_array().unwrap();
        assert_eq!(coords.len(), 5);
        // GeoJSON is (lon, lat) — first ring vertex was (lat=0, lon=0).
        assert_eq!(coords[0][0], 0.0);
        assert_eq!(coords[0][1], 0.0);
    }

    #[test]
    fn geojson_multi_ring_multipolygon() {
        let v = rings_to_geojson(&[unit_square_ring(), unit_square_ring()]);
        assert_eq!(v["type"], "MultiPolygon");
        assert_eq!(v["coordinates"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn geojson_empty_returns_null() {
        let v = rings_to_geojson(&[]);
        assert!(v.is_null());
    }

    #[test]
    fn wkt_single_ring_polygon() {
        let s = rings_to_wkt(&[unit_square_ring()]);
        assert!(s.starts_with("POLYGON(("));
        assert!(s.ends_with("))"));
        assert!(s.contains("0.0000000 0.0000000"));
        assert!(s.contains("1.0000000 1.0000000"));
    }

    #[test]
    fn wkt_multi_ring_multipolygon() {
        let s = rings_to_wkt(&[unit_square_ring(), unit_square_ring()]);
        assert!(s.starts_with("MULTIPOLYGON((("));
        assert!(s.ends_with(")))"));
    }

    #[test]
    fn kml_single_ring_polygon() {
        let s = rings_to_kml(&[unit_square_ring()]);
        assert!(s.starts_with("<Polygon><outerBoundaryIs><LinearRing><coordinates>"));
        assert!(s.contains("0.0000000,0.0000000"));
        assert!(s.contains("1.0000000,0.0000000"));
        assert!(s.ends_with("</coordinates></LinearRing></outerBoundaryIs></Polygon>"));
    }

    #[test]
    fn kml_multi_ring_multigeometry() {
        let s = rings_to_kml(&[unit_square_ring(), unit_square_ring()]);
        assert!(s.starts_with("<MultiGeometry>"));
        assert!(s.ends_with("</MultiGeometry>"));
        // Two <Polygon> elements inside.
        assert_eq!(s.matches("<Polygon>").count(), 2);
    }

    #[test]
    fn svg_single_ring_path() {
        let s = rings_to_svg(&[unit_square_ring()]);
        assert!(s.starts_with("M 0.0000000 0.0000000"));
        assert!(s.ends_with("Z"));
        // Should contain four 'L' commands (one per non-start vertex).
        assert_eq!(s.matches('L').count(), 4);
    }

    #[test]
    fn svg_multi_ring_concatenates() {
        let s = rings_to_svg(&[unit_square_ring(), unit_square_ring()]);
        assert_eq!(s.matches('M').count(), 2);
        assert_eq!(s.matches('Z').count(), 2);
    }

    #[test]
    fn bbox_as_ring_closes_ring() {
        let r = bbox_as_ring(0.0, 1.0, 0.0, 1.0);
        assert_eq!(r.len(), 5);
        assert_eq!(r[0], r[4]);
    }
}
