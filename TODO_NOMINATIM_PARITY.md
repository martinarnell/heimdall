# Heimdall — Nominatim Parity Gap Audit

Audit captured 2026-05-04 in response to a user reverse-geocoding bug
(taps on Sergels Torg returned "Norrmalm 200m away" instead of
"Sergelfontänen, Hamngatan, 111 51"). Triggered a fuller compatibility
audit against Nominatim's published API. Counterpart to
`docs/nominatim-compat-features.md`, which documents what we *do*
support — this file is the inverse.

Code references are line-accurate as of commit `7e3a10d`.

Items are grouped by user-visible impact, then ordered roughly by ROI.
Effort estimates are rough; verify scope before scheduling.

---

## Tier 1 — Wrong/missing in every response

These are response-shape gaps that affect every result the API returns.
Closing them is mostly mechanical and unlocks compatibility with
Nominatim-targeted clients (iD, Leaflet plugins, OSM-tooling, MapTiler
SDK, `geopy`, etc.).

### 1. No `boundingbox` field
- **Symptom**: Nominatim returns `"boundingbox": ["s","n","w","e"]` on
  every result. We never emit it.
- **Why it matters**: Many clients use the bbox to fit-zoom the map.
  Without it Leaflet falls back to a hard-coded zoom level.
- **Fix**: Persist a per-record bbox during `pack.rs`. For nodes,
  derive a small bbox from coord (~50–100 m). For ways/relations, keep
  the geometry's bbox (we already compute these during extraction —
  currently dropped). Schema bump on `PlaceRecord` (24 → 32 bytes), or
  store in a sidecar to keep the hot path tight.
- **Effort**: 1–2 days incl. the schema migration.

### 2. No `class` field (only `type`)
- **Symptom**: Nominatim emits `"class": "place", "type": "city"`. We
  emit `"type": "city"` only.
- **Why it matters**: Clients use `class` to choose icons (place vs.
  amenity vs. shop) and to filter category results.
- **Fix**: Store the OSM tag pair on each record. Today
  `PlaceType::Landmark` collapses dozens of `tourism=*`/`historic=*`
  values into one bucket — losing the original key:value loses
  information we already had.
- **Effort**: 1–2 days; couples with #28 (POI granularity).

### 3. No `place_rank` / `address_rank`
- **Symptom**: Nominatim's universal sort/dedupe key (0=globe, 4=country,
  8=state, 16=city, 26=street, 30=building). We don't expose anything
  comparable.
- **Why it matters**: Without rank, clients can't dedupe a city result
  against a county result of the same name, and can't sort cross-tier
  result sets.
- **Fix**: Derive from `PlaceType` at serialise time (table lookup).
  Doesn't require a schema bump.
- **Effort**: <½ day.

### 4. `display_name` is too short
- **Symptom**: We compose `name, county, state` in `reverse()` and
  `lookup()` (main.rs:2360, 2449, 2522). Nominatim builds the full
  chain: `road, house_no, suburb, city, county, state, postcode, country`.
- **Why it matters**: User-facing label that everyone reads. Even when
  we have the parts (e.g. on an address hit) we don't string them in.
- **Fix**: Extract a `compose_display_name()` helper that takes the full
  address pyramid and renders it. Reuse on every endpoint.
- **Effort**: <½ day, dependent on #6 (address pyramid composition).

### 5. `osm_id` is u32-truncated
- **Symptom**: `PlaceRecord::osm_id: u32` (types.rs:193). The comment at
  types.rs:191 acknowledges the limitation. Real OSM IDs are i64; nodes
  past ~4.29B (most of OSM since 2021) collide.
- **Why it matters**: `/lookup?osm_ids=N12345678901` silently fails or
  returns the wrong record. Round-trips between `/search` and `/lookup`
  are unreliable for any modern node.
- **Fix**: Widen to u64 (or i64 to match OSM convention). Schema bump
  on `PlaceRecord` (24 → 32 bytes). Same migration window as #1 and #28.
- **Effort**: 1 day.

### 6. No `licence` string
- **Symptom**: Nominatim always emits `"licence": "Data © OpenStreetMap
  contributors, ODbL 1.0. https://osm.org/copyright"`. We don't.
- **Why it matters**: Some clients lint for it. Trivial omission.
- **Fix**: Two lines per response.
- **Effort**: <1 hour.

---

## Tier 2 — Reverse geocoding correctness

The original bug, expanded. Reverse today is "nearest place node within
the 3×3 geohash grid" — never returns roads, never returns house
numbers, never returns postcodes.

### 7. No spatial index over streets or house points
- **Symptom**: `reverse()` (main.rs:2326) only iterates
  `country.geohash_index`. The geohash builder
  (`pack.rs:284`) only adds entries for *places*. The address store
  (`addr_streets.bin` + `fst_addr.fst`) is never spatially queryable.
- **Why it matters**: Reverse can never return road or house_number.
  The address data is in the index and unreachable.
- **Fix**: Build a second geohash index in `pack_addr.rs` over (a)
  street segment midpoints, (b) house-number points. Add a
  `nearest_address()` method on `GeohashIndex`. Plumb into `reverse()`
  with a "prefer address within Nm of query, fall back to place"
  ranking rule.
- **Effort**: 3–4 days. Largest single ROI item in this document.

### 8. No postcode in reverse
- **Symptom**: Postcodes are stored on address records but never reach
  the reverse response. `place_type_to_settlement()` (main.rs:2786)
  doesn't even take a postcode argument.
- **Why it matters**: "Sergels Torg" → `111 51` is a real expectation.
- **Fix**: Surface `AddrResult.postcode` from the address spatial hit
  in #7.
- **Effort**: included in #7.

### 9. `place_type_to_settlement()` collapses too aggressively
- **Symptom**: Function returns one of `(city, town, village, suburb)`
  based on a single `PlaceType`. Nominatim populates the *full* address
  pyramid: `house_number, road, neighbourhood, suburb, hamlet, village,
  town, city, municipality, county, state, region, country, postcode`.
- **Why it matters**: A reverse hit on a building should fill `road`,
  `house_number`, `suburb`, *and* `city` simultaneously. Our model
  returns only the deepest level.
- **Fix**: Replace with a builder that walks the admin chain and
  appends the address record. Requires polygon containment (#10) for
  full correctness.
- **Effort**: 2 days.

### 10. Nearest-centroid admin assignment leaks across borders
- **Symptom**: `enrich.rs` assigns `admin1_id`/`admin2_id` by nearest
  centroid. Lund→Staffanstorp, etc. — already a known issue per
  `CLAUDE.md`. Same root cause hits address muni-id resolution
  (`addr_index` falls back to FST range scan + city-coord distance to
  paper over it).
- **Why it matters**: Wrong city in display_name; wrong city in the
  reverse response; wrong postcode in some cases.
- **Fix**: Persist admin polygons during pack (we extract them, then
  drop them). Point-in-polygon at query time. R-tree of admin
  geometries.
- **Effort**: 1 week. Removes two known-issue bullets from `CLAUDE.md`.

### 11. `zoom` parameter only filters `PlaceType`, not granularity
- **Symptom**: `matches_zoom()` (reverse.rs:368) filters places by
  type. At z18 we still return the same nearest place node — there's
  nothing finer in the candidate pool.
- **Why it matters**: The user observation that "z16 vs z18 returns
  the same place_id". Nominatim genuinely walks down to building/
  address at z18.
- **Fix**: Once #7 lands, z17+ should prefer address hits over place
  hits.
- **Effort**: included in #7.

---

## Tier 3 — Output formats

We claim `format=json` and support exactly one shape.

### 12. No `format=jsonv2`
- **Symptom**: Nominatim's v2 JSON adds `place_rank`, `category`,
  `addresstype` fields and slightly different field ordering.
- **Why it matters**: Most clients default to v2.
- **Fix**: Format wrapper around existing data; couples with #2 and #3.
- **Effort**: ½ day.

### 13. No `format=geojson`
- **Symptom**: GeoJSON FeatureCollection output. Standard for any
  mapping client.
- **Effort**: ½ day. <100 LOC.

### 14. No `format=geocodejson`
- **Symptom**: The IETF-style standardised geocoding JSON. Lower
  priority but expected by some Pelias-targeted clients.
- **Effort**: ½ day.

### 15. No `format=xml`
- **Symptom**: Older clients still ask for XML.
- **Effort**: ½ day. Optional.

---

## Tier 4 — Query parameters Nominatim accepts that we ignore

### 16. No `Accept-Language` / `accept-language=`
- **Symptom**: We extract `name:en`, `name:de`, etc. into
  `RawPlace.name_intl` (types.rs:214) but never serve them. The header
  is dropped on the floor.
- **Why it matters**: A German client tapping a Swedish place expects
  "Stockholm" not "Stockholms stad".
- **Fix**: Surface `name_intl` to the record store; pick the matching
  locale in `primary_name()`. Falls back to `name=*` if locale missing.
- **Effort**: 1 day. We already have the data.

### 17. No polygon outputs
- **Params**: `polygon_geojson`, `polygon_kml`, `polygon_svg`,
  `polygon_text`, `polygon_threshold`.
- **Symptom**: We have no polygon data on records at all.
- **Why it matters**: Editors (iD, JOSM) require polygons for admin
  boundaries and large features.
- **Fix**: Persist way/relation geometries during extract — currently
  used to compute centroids and discarded. Sidecar polygon store.
- **Effort**: 1 week. Largest single data-model lift.

### 18. No `extratags=1`
- **Symptom**: `extratags` exposes population, wikidata, wikipedia,
  capital, opening_hours, ele, etc. We extract `population` and
  `wikidata` (RawPlace) and discard them.
- **Fix**: Sidecar map of `record_id → extratags`. ~2–4 KB / record.
- **Effort**: 2 days.

### 19. No `namedetails=1`
- **Symptom**: `namedetails` exposes `name`, `name:en`, `alt_name`,
  `official_name`, `short_name`. We have most of this internally.
- **Fix**: Surface `name_intl`, `alt_names`, `old_names` from
  `RawPlace`.
- **Effort**: 1 day. Couples with #16.

### 20. No `dedupe=1`
- **Symptom**: Nominatim merges results sharing
  `(lat, lon, type, name)` — common for districts duplicated as
  admin + place. We don't.
- **Fix**: Hashset post-filter in the search response builder.
- **Effort**: ½ day.

### 21. No `exclude_place_ids=`
- **Symptom**: Nominatim's pagination idiom — "I've already shown
  these, give me the next batch". Useful for "load more" UIs.
- **Fix**: Drop-list filter in the search response builder.
- **Effort**: ½ day.

### 22. No `featuretype=` filter
- **Symptom**: `featuretype=city|state|country|settlement` constraints.
  Partial overlap with our structured query.
- **Fix**: Map onto a `PlaceType` allowlist.
- **Effort**: ½ day.

### 23. No `email=` parameter
- **Symptom**: Nominatim's polite-rate-limit hint. Trivial.
- **Fix**: Accept and ignore (or log).
- **Effort**: <1 hour.

### 24. No `debug=1` page
- **Symptom**: Nominatim returns an HTML page showing the parsed query
  and considered candidates. Not strictly an API feature but very
  valuable for parity diagnosis.
- **Fix**: Optional. Nice-to-have.
- **Effort**: 2 days.

---

## Tier 5 — Endpoints

### 25. No `/details` endpoint
- **Symptom**: Nominatim's introspection endpoint — given a place_id,
  return the parent admin chain, all OSM tags, polygons, search rank,
  linked Wikidata, etc.
- **Why it matters**: iD editor and several Wikidata-aware tools rely
  on `/details`. Without it, integration breaks.
- **Fix**: Materially blocked on #2 (class), #17 (polygons), #18
  (extratags). Trivial once those land.
- **Effort**: 2 days *after* dependencies.

### 26. `/lookup` is osm_id-truncated
- **Symptom**: Same root cause as #5.
- **Fix**: Goes away when #5 lands.

### 27. `/status` shape mismatch
- **Symptom**: Nominatim's `/status` returns a plain `OK` (or a JSON
  with `status: 0`). Some health-check clients literal-string-match.
  Ours returns rich diagnostic JSON, which is *better* but not
  compatible.
- **Fix**: Honor `?format=json` for the rich shape, default to
  Nominatim's plain output.
- **Effort**: <1 hour.

---

## Tier 6 — Data we should be indexing but aren't

### 28. POI granularity collapsed
- **Symptom**: `PlaceType` has ~30 buckets, collapsing thousands of
  OSM `amenity`/`shop`/`tourism`/`leisure`/`craft`/`historic`/`office`
  values. `Landmark` is the catch-all for tourism+historic. We can't
  distinguish a cafe from a restaurant from a bakery.
- **Why it matters**: "cafe near me", "atm near me", category
  filtering — entire classes of query that Nominatim handles and we
  don't.
- **Fix**: Keep the original `(class, type)` tag pair on every record.
  Schema-bump candidate alongside #1 and #5.
- **Effort**: 3 days incl. extractor changes.

### 29. Buildings not indexed as searchable objects
- **Symptom**: Nominatim indexes `building=*` ways with `addr:*` tags
  as searchable places. We harvest `addr:*` for the address store but
  the building itself isn't a returnable result.
- **Why it matters**: Reverse on a building polygon should return the
  building. Forward search by name (e.g. "Empire State Building")
  should find it via the building tag, not just via `tourism=*`.
- **Fix**: Add building extraction pass in `extract.rs`.
- **Effort**: 2–3 days. Index-size impact: meaningful (~10–20% growth).

### 30. No address interpolation
- **Symptom**: OSM `addr:interpolation` ways encode "houses 1–199 odd
  along this segment". Common for US/UK rural addresses where
  individual nodes don't exist.
- **Why it matters**: Can't geocode "123 Main St" if only "100" and
  "200" are mapped explicitly.
- **Fix**: Parse interpolation ways during address extract; emit
  synthetic house points along the segment.
- **Effort**: 3 days. Big quality bump for US/UK/AU rural coverage.

### 31. Postcodes not searchable as standalone places
- **Symptom**: Searching `"111 51"` or `"SW1A"` should return the
  postcode area as its own object with a centroid. Today postcodes
  are joined to addresses as a sidecar field.
- **Fix**: Synthetic postcode records during pack — one per unique
  postcode, centered on the centroid of all member addresses.
- **Effort**: 2 days. Couples with #17 if we want postcode polygons.

### 32. Relation-typed results collapsed to nodes
- **Symptom**: Searching "Stockholm" Nominatim returns
  `osm_type=relation, type=administrative` (the city polygon). We
  collapse relations to a representative node.
- **Why it matters**: `osm_type` always reads `node` for our admin
  hits — the user's report flagged this. Correctness *and*
  expectation mismatch.
- **Fix**: Preserve the original OSM type. Currently flattened in
  `flags` (types.rs:184) but `osm_type_from_flags()` only emits
  N/W/R based on a single bit.
- **Effort**: 2 days.

### 33. No Wikidata QID lookup
- **Symptom**: Nominatim accepts `q=Q1428` and resolves to the place.
  We extract Wikidata QIDs but don't index them.
- **Fix**: Sidecar `wikidata_id → record_id` FST.
- **Effort**: 1 day.

---

## Tier 7 — Search behaviour & ranking

### 34. No native amenity-near-place
- **Symptom**: "restaurants in Berlin", "atm near Centralstation". Our
  amenity handling is fuzzy keyword over names. Nominatim does true
  category search.
- **Fix**: Requires #28 (proper class:type). Then category FST per
  country.
- **Effort**: 1 week incl. dependencies.

### 35. Free-form parsing shallower than Nominatim
- **Symptom**: Nominatim has heuristics for "X in Y", "Y near X",
  postcode-prefix detection, country-code detection, etc. We have a
  parser but it's tuned per pipeline.
- **Fix**: Iterative; benchmark-driven. Use `heimdall-compare` to
  identify miss patterns.
- **Effort**: ongoing.

### 36. Cross-tier sorting weak without `place_rank`
- **Symptom**: Nominatim sorts mixed result sets by
  `(rank desc, importance desc)`. Without a rank we lean on
  importance only, which mis-orders e.g. a small POI named "Stockholm"
  vs the city of Stockholm.
- **Fix**: Comes for free once #3 lands.

---

## Cross-cutting: schema migration window

Items #1, #2, #5, #28 all imply a `PlaceRecord` schema bump. They
should land together so we pay one migration cost (full reindex of all
192 countries) rather than four.

A unified v3 record proposal:

```rust
pub struct PlaceRecord {
    pub coord: Coord,        //  8 bytes
    pub bbox: BBoxDelta,     //  8 bytes  (NEW — packed Δ from coord, see #1)
    pub admin1_id: u16,      //  2
    pub admin2_id: u16,      //  2
    pub importance: u16,     //  2
    pub place_type: PlaceType, // 1
    pub flags: u8,           //  1
    pub name_offset: u32,    //  4
    pub osm_id: u64,         //  8 bytes  (was u32 — see #5)
    pub class_type: u16,     //  2 bytes  (NEW — interned (class,type) pair, see #2 #28)
}
// 38 bytes — pad to 40 for alignment.
```

`extratags` and `namedetails` (#18, #19) live in sidecar files keyed by
`record_id`, so they don't bloat the hot path.

---

## Suggested attack order (by ROI)

If the goal is "be the best geocoder" while staying compatible:

| # | Item | Effort | Unlocks |
|---|------|--------|---------|
| 1 | Reverse with addresses (#7–11) | 1 wk | The user's reported bug; closes the biggest visible gap |
| 2 | Tier 1 response shape (#1–6) | 1 wk | All Nominatim clients start working |
| 3 | `Accept-Language` (#16) | 1 day | Free i18n win — data already exists |
| 4 | Polygon containment for admin (#10) | 1 wk | Removes two CLAUDE.md known issues |
| 5 | `jsonv2` + `geojson` (#12, #13) | 1 day | Most-used output formats |
| 6 | Schema-bump batch (#28, #5, #2 together) | 3 days | One reindex, three big features |
| 7 | `extratags`, `namedetails`, `dedupe`, `exclude_place_ids` (#18–21) | 2 days | Cheap, additive |
| 8 | Postcodes as places + interpolation (#30, #31) | 1 wk | US/UK/AU quality |
| 9 | Polygon outputs + `/details` (#17, #25) | 2 wks | Editor compatibility (iD, JOSM) |
| 10 | Category search (#28+#34) | 1 wk after #28 | Whole new query class |

**Total to "Nominatim parity for the 80% case"**: ~3 weeks of focused
work (items 1–5).

**Total to "best-in-class geocoder"**: ~2–3 months for everything.

---

## Honest framing

Today Heimdall is a fast forward-geocoder for places + addresses, with
a reverse endpoint that's really "nearest place name." Closing items
1–5 alone would put us at Nominatim parity for the 80% case; items
6–10 are what separates parity from best-in-class.

The reverse-geocoding bug isn't a one-off — it's the visible tip of
"we built a forward index and bolted on a minimal reverse endpoint."
Done right, reverse uses the same address index forward search uses,
plus polygon containment for admin attribution.
