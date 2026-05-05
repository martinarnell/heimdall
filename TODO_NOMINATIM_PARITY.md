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

## Strategic framing — "Nominatim-class API on potato infrastructure"

**Heimdall's value proposition is not "lean but feature-poor." It is
"Nominatim-class features without Nominatim-class infrastructure."**

The reason Nominatim is heavy isn't intrinsic to geocoding — polygons
aren't heavy, POI tags aren't heavy, address interpolation isn't
heavy. Nominatim is heavy because Postgres+PostGIS is heavy and the
import pipeline assumes a relational model. Our architecture
(mmap'd FSTs + record store + sidecar files) can deliver the same
capabilities at a fraction of the storage and runtime cost.

Cost re-check on the items that look "expensive" but aren't:

| Feature | Naïve Nominatim cost | Heimdall cost (mmap) |
|---|---|---|
| Admin polygons (192 countries) | ~50–100 GB in PostGIS | ~1 GB total, sub-ms PiP |
| Polygon outputs | full polygon table query | direct mmap read |
| POI granularity (full class:type) | text columns, indexed | 2 bytes/record (~100 MB global) |
| Building extraction | full row per building | ~10–20% index growth (~100–200 MB/country) |
| Address interpolation | synthetic table rows | synthetic FST entries, same store |
| `extratags`, `namedetails` | jsonb columns | sidecar files, opt-in load |

The earlier framing in this doc ("push all of these and you've
reinvented Nominatim") is wrong — or rather, it's literally true but
misses the point. Reinventing Nominatim *on potato infrastructure* is
the moat. That's the product.

The earlier "skip items 9–10" advice (polygon outputs, `/details`,
category search) is **revoked**. We should pursue them — see the
revised attack order at the bottom.

---

## The genuine 20% — caveats we own

These are explicit non-goals: things that conflict with the
architecture, the audience, or both. Documenting them here so we don't
silently drift into trying to be everything.

### C1. Live OSM minutely diff streams
Nominatim consumes OSM's minutely replication feed; every edit on the
planet propagates within minutes. Our model is "rebuild a country
periodically with change detection." Implementing diff-application
would require transactional mutability of the FSTs, which fights
mmap-immutability.
- **Caveat**: if you need <1-hour freshness on user edits anywhere on
  the planet, Heimdall isn't for you. (~95% of geocoding use cases
  don't need this.)
- **Workaround for users**: trigger a per-country rebuild on demand;
  takes minutes-to-hours per country.

### C2. Mutation / admin endpoints
Nominatim has SQL-level access for marking duplicates, setting
Wikidata links, deleting bad records, ingesting custom data. Our
index is built ahead of time and immutable at runtime.
- **Caveat**: data corrections happen at rebuild time via source data
  or normalizer config, not via API.

### C3. OSM-editor backend integration (iD, JOSM)
Editors expect a very specific Nominatim contract — full original tag
preservation, polygon download for boundary editing, exact `place_id`
lifecycle across rebuilds. Editor users are a different audience from
app developers; bridging the gap costs more than it returns.
- **Caveat**: don't use Heimdall as the geocoding backend for an OSM
  editor. Audience mismatch.

### C4. Full original OSM tag preservation
Nominatim keeps every tag on every object. Our extraction is curated
— we keep the tags that matter for geocoding (name, place, amenity,
class, type, addr:*, population, wikidata, etc.) and drop the rest
(heritage codes, surface materials, lit=yes, ref:*).
- **Caveat**: not a tag-store. Use Overpass or osmium-tool for that.

### C5. Quality-tracking endpoints (`/deletable`, `/polygons`)
Used by OSM data-quality teams to find self-reported deletions and
broken polygons. Pure niche, audience mismatch.
- **Caveat**: not a data-quality tool.

### C6. RDF / linked-data outputs
Nominatim has an RDF form for linked-data consumers. Genuinely niche.
- **Caveat**: JSON / JSONv2 / GeoJSON only. (We *will* add GeoJSON; see
  #13.)

### C7. `format=xml`
Older clients still ask for it. Audience mismatch with our target
users (app developers).
- **Caveat**: optional; not in our roadmap.

---

## The pitch (positioning statement)

> *Heimdall implements the Nominatim API surface for forward search,
> reverse geocoding, autocomplete, and structured query — with full
> address-level reverse, polygon containment, polygon output, and
> category search. It runs as a single binary on potato hardware (1–2
> GB RAM, hundreds of MB to low GB of disk per country). It is **not**
> a live OSM mirror, **not** an editor backend, and **not** a
> data-quality tool. Updates land at country-rebuild cadence (hours),
> not minutely. If you're geocoding for an app, this is what you want;
> if you're operating an OSM editor or a Wikidata bot, run Nominatim.*

That's the elevator pitch. The seven caveats above fit on a sticker.

---

## Tier 0 — Stability concerns to address early

### S1. `place_id` stability across rebuilds
- **Symptom**: `encode_place_id(country_index, record_id)`
  (main.rs:2574) packs the record's array position into the ID.
  Whenever upstream data reorders records (a country rebuild, a
  Photon refresh, an OSM PBF update with new node IDs) the same
  physical place gets a different `place_id`.
- **Why it matters**: many clients persist `place_id` to mark
  favorites, build address books, deduplicate user history, etc.
  Nominatim place_ids are mostly stable across imports; ours aren't.
  This is a *silent data corruption* class of bug from a user's
  perspective.
- **Fix**: stable-ID layer. Options:
  - Hash `(osm_type, osm_id, class, type)` to a u64 — stable as long
    as the OSM object exists, breaks if the object is deleted.
  - Sidecar `stable_id ↔ record_id` map persisted across rebuilds —
    new records get new IDs, deleted IDs are tombstoned.
- **Effort**: 2–3 days for the hash approach (recommended);
  1 week for the durable-map approach.
- **Priority**: ship before any large user adopts us, even if the
  feature audit is incomplete. Once users persist place_ids, breaking
  them is a hostile event.

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

## Suggested attack order (by ROI) — revised under the lean-reinvention thesis

The goal is full Nominatim API parity (excepting the seven caveats
above), delivered on potato infrastructure. Earlier guidance to "skip
items 9–10" is **revoked** — those features fit the architecture and
are part of the moat. Only `format=xml` (#15) and `debug=1` HTML (#24)
stay genuinely punted.

Phased, by impact and dependency:

### Phase 0 — Don't ship more users until this lands (1 week)

| # | Item | Effort | Why first |
|---|------|--------|-----------|
| 0.1 | Stable `place_id` (S1) | 2–3 days | Silent corruption class. Once users persist IDs, breaking them is hostile. |

### Phase 1 — Drop-in compatibility for app developers (~3 weeks)

Closes the user-reported bug and gets the API shape clients expect.

| # | Item | Effort | Unlocks |
|---|------|--------|---------|
| 1.1 | Reverse with addresses (#7–11) | 1 wk | The original bug. Largest visible gap. |
| 1.2 | Tier 1 response shape (#1–6: `boundingbox`, `class`, `place_rank`, full `display_name`, `osm_id` u64, `licence`) | 1 wk | Every Nominatim client starts working. |
| 1.3 | `Accept-Language` (#16) | 1 day | Free i18n — data already in `name_intl`. |
| 1.4 | `jsonv2` + `geojson` outputs (#12, #13) | 1 day | Most-requested formats. |
| 1.5 | `dedupe`, `exclude_place_ids`, `featuretype`, `email` (#20–23) | 2 days | Cheap additive params. |

**End of Phase 1**: Heimdall is a credible Nominatim drop-in for app
use cases. Map taps return addresses. Search results have correct
shape. Localised names work.

### Phase 2 — The lean-reinvention (under-promised, doable) (~3 weeks)

Items I previously hedged on. They fit the architecture; the cost
re-check above shows they're affordable.

| # | Item | Effort | Unlocks |
|---|------|--------|---------|
| 2.1 | Polygon containment for admin (#10) | 1 wk | Two CLAUDE.md known issues; correct city/county on every reverse. |
| 2.2 | Schema-bump batch (#28 POI granularity, #5 osm_id u64 if not yet, #2 class field) | 3 days | One reindex pays for three big features. |
| 2.3 | `extratags` + `namedetails` (#18, #19) | 2 days | Sidecar-loaded; zero hot-path cost. |
| 2.4 | Buildings as places (#29) | 2–3 days | "Empire State Building" works as a forward query, building polygons as reverse hits. |
| 2.5 | Address interpolation (#30) | 3 days | US/UK/AU rural coverage. |
| 2.6 | Postcodes as places (#31) | 2 days | `q="111 51"` returns the postcode area. |
| 2.7 | Relation-typed results (#32) | 2 days | Stockholm returns as `osm_type=R, type=administrative`. |
| 2.8 | Wikidata QID lookup (#33) | 1 day | `q=Q1428` resolves. |

**End of Phase 2**: Heimdall has feature-superset overlap with
Nominatim's app-tier API. Polygon containment, full POI tagging,
buildings, interpolation — all on potato infra.

### Phase 3 — Editor-adjacent without becoming an editor (~2 weeks)

Polygon outputs and `/details` look like "editor features" but they're
also useful for app developers (admin boundary highlighting, place
introspection). Worth doing; *but* they don't make us an editor
backend (see caveat C3).

| # | Item | Effort | Unlocks |
|---|------|--------|---------|
| 3.1 | Polygon outputs (#17: `polygon_geojson`, `polygon_kml`, `polygon_svg`, `polygon_text`, `polygon_threshold`) | 1 wk | Admin highlighting, geofencing UI. |
| 3.2 | `/details` endpoint (#25) | 2 days | Place introspection. Trivial after #2, #17, #18 land. |
| 3.3 | Category search (#34) | 1 wk after #28 | "cafes near me", "atm in Berlin". New query class. |
| 3.4 | `/status` Nominatim-shaped output (#27) | <1 hr | Health-check parity. |

### Phase 4 — Long-tail polish

| # | Item | Effort | Notes |
|---|------|--------|-------|
| 4.1 | Free-form parser improvements (#35) | ongoing | Benchmark-driven via `heimdall-compare`. |
| 4.2 | `format=geocodejson` (#14) | ½ day | Pelias-targeted clients. |
| 4.3 | `format=xml` (#15) | ½ day | Optional — caveat C7. |
| 4.4 | `debug=1` HTML page (#24) | 2 days | Diagnostic UX. Optional. |

### Totals

- **Phase 0 + 1** (drop-in for apps): ~4 weeks. Single engineer.
- **Phase 0 + 1 + 2** (lean-reinvention complete): ~7 weeks.
- **Through Phase 3**: ~9–10 weeks. Full app+adjacent feature set.
- **Phase 4**: ongoing.

This is the honest "weeks-to-Nominatim-parity-on-potato" budget.

---

## Honest framing — revised

Today Heimdall is a fast forward-geocoder for places + addresses, with
a reverse endpoint that's really "nearest place name." That's the
80%-case product, and Phase 1 above closes the visible gap to
Nominatim for that case.

The strategic insight from this audit: the items I previously called
"reinventing Nominatim" (polygon containment, polygon outputs, full
POI tagging, buildings, /details, category search) all fit our
architecture at a small fraction of Nominatim's storage and ops cost.
Pursuing them isn't mission creep — *it's the moat*. Heimdall delivers
Nominatim-class features without the Postgres+PostGIS tax. That's the
product.

The seven caveats (C1–C7) define what we explicitly don't try to be:
not a live OSM mirror, not an editor backend, not a data-quality tool,
not a tag store, not an RDF/XML server. Audience mismatch on all of
them; bridging the gap costs more than it returns.

Phase 0's stable-`place_id` work is the one thing we should ship
**before** any meaningful user adoption. Persisted IDs are a contract
we can't silently break.

The reverse-geocoding bug that triggered this audit isn't a one-off —
it's the visible tip of "we built a forward index and bolted on a
minimal reverse endpoint." Done right, reverse uses the same address
index forward search uses, plus polygon containment for admin
attribution. Phase 1 lands that.

---

## Plan review — 2026-05-04

External read of the audit + proposed phase order. Framing is sound;
seven caveats draw the line cleanly. Three sequencing nits and one
cross-roadmap interaction worth flagging before Phase 0 starts.

### 1. Consolidate the schema migration

The cross-cutting note above (lines 502–505) says items #1, #2, #5,
#28 should land together to pay one 192-country reindex tax. The
phase table then splits them: #1 and #5 sit in Phase 1.2, while #2
and #28 are in Phase 2.2. Pick one phase. Recommendation: pull #2
(`class` field) and #28 (POI granularity) **forward** into Phase 1.2.

- `class` is already on Phase 1.2's critical path because `format=jsonv2`
  (#12) requires it.
- `class_type: u16` is 2 bytes; folding it in now is cheaper than
  reindexing twice.
- The failed-US-rebuild memory is still on the books — minimising
  reindex cycles matters operationally.

### 2. #9 (display_name pyramid) has an undocumented dep on #10

Phase 1's #9 says "requires polygon containment (#10) for full
correctness", but #10 lives in Phase 2.1. Make the choice explicit:

- **(a) Accept partial correctness in Phase 1**: build the pyramid
  from nearest-centroid admin. Wrong near borders (Lund →
  Staffanstorp), no worse than today. Ship the visible bug fix now.
- **(b) Pull #10 forward**: 1 week, but removes two `CLAUDE.md`
  known-issue bullets and the address-FST muni-id collision
  workaround.

Default recommendation: (a). Ship the visible bug fix in Phase 1,
make #10 its own discrete Phase 2 milestone.

### 3. Pin a regression baseline before Phase 1 starts

Every Phase 1 item changes response shape, reverse ranking, or the
`PlaceRecord` schema. Without a frozen baseline, "Sergels Torg now
works" ships silently with regressions elsewhere (forward recall on
edge queries, reverse-radius drift, admin attribution near borders).

Concrete: before Phase 0 lands, run

```
heimdall-compare run --queries <190-query US corpus> \
  --rps 1 --output baseline-pre-parity.sqlite
```

against current main and pin the SQLite as a git-tracked baseline.
Every Phase-1 PR re-runs and posts a delta in the PR description.
Repeat for SE/NO/DK corpora once they exist.

### 4. Cross-roadmap interaction with rebuild-modes

`TODO_REBUILD_MODES.md` Phase 5 first-slice landed in PR #12
(2026-05-04, 9933585). The remaining Phase-5 follow-ups are multi-day
refactors with FST byte-content breaks (idempotent merges in the
address pipeline, pre-sort `places.parquet`, typed key-buffer for
`pack.rs`'s TSV path). Phase 2.2 of *this* doc also forces a
192-country reindex. Cleanest sequencing:

1. Parity Phase 0 + 1 (no schema break — only 2.2 breaks it).
2. Rebuild-modes Phase-5 follow-ups.
3. Parity Phase 2 schema bump — one combined reindex picks up
   both byte-content changes.

Costs ~1 week of calendar time; saves ~2 full US reindex cycles
(~24 h each).

### Concrete first PR

Phase 0 stable `place_id` via the hash approach
(`hash(osm_type, osm_id, class, type) → u64`). Narrow scope, no
schema impact, 2–3 days. Ship before any meaningful user adoption.

---

## Phase 2.2 — shipped (schema-bump batch)

Landed 2026-05-05 on branch `feat/parity-phase-2-2-schema-bump`. Single
PR, single 192-country reindex tax. Items #1, #2, #5, #28, #32 (osm_type
distinguishing way vs relation) all paid for in one go.

### What's on disk

* `PlaceRecord` is now 40 bytes (was 24): adds packed `bbox` delta
  (4 × i16, 10-µdeg quanta — ±0.327° range, ~36 km), widens `osm_id`
  u32 → u64, adds `class_type` u16 interning index. Two new flag bits
  distinguish way vs relation (`FLAG_IS_WAY = 0x10`) and signal the
  presence of a real bbox (`FLAG_HAS_BBOX = 0x20`).
* `records.bin` bumps to format v4: same block-compressed layout as v3
  but record_block_size aligns to a multiple of 40 (default 65520 B)
  so records never straddle a block boundary on read.
* New per-index sidecar `class_types.bin` — postcard-encoded
  `Vec<(class, value)>` indexed by `class_type`. Loaded once at API
  startup; missing-file fallback yields an empty table and the runtime
  synthesises class/type from `place_type` (compatible with pre-2.2
  indices via the v3 → v4 read fallback in `record_store.rs`).
* Parquet `places.parquet` schema gains six nullable columns:
  `osm_class`, `osm_class_value`, and `bbox_{south,north,west,east}`
  (Int32 microdegrees). Older parquet files load fine — pack falls
  through to default class/type and synthesises a small node-bbox
  from the centroid.

### What's on the wire

Every record-backed result (search hits, /reverse place branch,
/lookup) now carries:

* `boundingbox`: `[south, north, west, east]` strings, formatted to
  seven decimals (Nominatim shape).
* `class`: the OSM tag key (`place`, `amenity`, `tourism`, …).
* `type`: the OSM tag value (`city`, `restaurant`, `museum`, …) —
  upgraded from the previous `format!("{:?}", PlaceType)` collapse so
  `tourism=museum` no longer flattens to `"landmark"`.
* `osm_id` widened to u64 in the JSON payload.
* `osm_type` distinguishes `node` / `way` / `relation` (was always
  `node` or `relation`).

The /reverse address branch and synthetic /search results (postcode /
zip lookups) still emit `class: "place"` plus a synthetic ~50 m bbox
when not backed by a real record.

### What's still open from the audit

* **Items #18 / #19** (extratags + namedetails sidecars) — Phase 2.3.
  Don't need a schema break; build a sidecar map keyed by `record_id`.
* **Item #29** (buildings as places), **#30** (address interpolation),
  **#31** (postcode synthetic places), **#33** (Wikidata QID lookup) —
  Phases 2.4–2.8.
* The plan-review's "consolidate the schema migration" recommendation
  is now closed: #1, #2, #5, #28 all rode a single reindex.

### Migration consequences

* `stable_place_id` mixes the full u64 osm_id, so persisted
  pre-Phase-2.2 IDs are invalidated — flagged by the unit test
  `stable_place_id_known_vector`. The audit always called this out
  as a one-shot migration paid by the schema bump (line ~509).
* The pinned 190-query US baseline (`benchmarks/baseline-pre-parity.sqlite`)
  pre-dates this PR's response shape; expect drift on the `class` /
  `type` axes when the next compare run lands.
