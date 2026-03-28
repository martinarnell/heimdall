# Nominatim Compatibility Features

Implemented 2026-03-23. Four features that close the biggest integration gaps with Nominatim-compatible clients.

## Feature 1: Structured Query Mode

**Endpoint:** `/search`

Supports structured address queries where components are separate parameters instead of a single `q=` string.

### Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `amenity` | POI name or type | `café`, `hospital` |
| `street` | House number + street name | `15 Kungsgatan` |
| `city` | City/town name | `Stockholm` |
| `county` | County/district | `Stockholm County` |
| `state` | State/province/region | `Stockholms län` |
| `country` | Country name (freeform) | `Sweden` |
| `postalcode` | Postcode/ZIP | `11156` |

All parameters are optional. **Cannot combine with `q=`** — returns HTTP 400 if both are present.

### How it works

Present fields are concatenated in order (amenity → street → city → county → state → postalcode → country) separated by `, ` and fed into the existing search pipeline as a synthetic query string.

### Special cases

- **postalcode only** (with optional country/countrycodes): routes to postcode FST lookup first
- **amenity only** (no street/city): treated as a place name query

### Examples

```
/search?city=Tromsø&countrycodes=no
/search?street=Kungsgatan+15&city=Stockholm&countrycodes=se
/search?postalcode=1011AB&countrycodes=nl
/search?street=Storgatan&city=Uppsala&q=something  → HTTP 400
```

### Implementation

- `parse_structured_query()` in `heimdall-api/src/main.rs` (~line 167)
- `has_structured_params()`, `is_postalcode_only()`, `is_amenity_only()` helpers
- Conflict detection at top of `search()` handler

---

## Feature 2: addressdetails=1

**Endpoints:** `/search`, `/reverse`, `/lookup`

When `addressdetails=1` is passed, each result includes an `address` object with broken-down address components.

### Response format

```json
{
  "place_id": 12345,
  "display_name": "Stockholm, Stockholms län",
  "address": {
    "city": "Stockholm",
    "state": "Stockholms län",
    "country": "Sweden",
    "country_code": "se"
  }
}
```

### Address fields

| Field | Source | When present |
|-------|--------|--------------|
| `house_number` | Address record | Address results only |
| `road` | Street name from addr_store | Address/street results |
| `suburb` | PlaceType::Suburb/Quarter/Neighbourhood | Place results |
| `city` | PlaceType::City/County/State/Country | Place results |
| `town` | PlaceType::Town | Place results |
| `village` | PlaceType::Village/Hamlet/Farm/Locality | Place results |
| `county` | admin2 from admin.bin | When available |
| `state` | admin1 from admin.bin | When available |
| `postcode` | Street header or query | When available |
| `country` | Country name from index config | Always |
| `country_code` | ISO 3166-1 alpha-2 | Always |

All fields use `skip_serializing_if = "Option::is_none"` — absent fields are omitted from JSON.

### Default

`addressdetails=0` — the `address` field is omitted entirely (backward compatible).

### Implementation

- `AddressDetails` struct (~line 266)
- `place_type_to_settlement()` maps PlaceType → city/town/village/suburb
- `build_address_details_for_addr()` for address results
- Populated at every result construction site in search/reverse/lookup handlers

---

## Feature 3: Viewbox Filter and Bias

**Endpoint:** `/search`

### Parameters

| Parameter | Description | Format |
|-----------|-------------|--------|
| `viewbox` | Bounding box | `minLon,minLat,maxLon,maxLat` |
| `bounded` | Hard filter mode | `0` (default) or `1` |

### Behavior

**`viewbox` only (bounded=0):** Results inside the viewbox get a 1.5x importance score boost. Results outside are still returned but ranked lower.

**`viewbox` + `bounded=1`:** Hard filter — only results inside the bbox are returned. May return empty array.

Coordinates are auto-swapped if reversed (x1 > x2 or y1 > y2).

### Examples

```
# Bias toward Stockholm area, still show other results
/search?q=Storgatan&countrycodes=se&viewbox=17.9,59.8,18.2,59.9

# Only Stockholm area results
/search?q=Storgatan&countrycodes=se&viewbox=17.9,59.8,18.2,59.9&bounded=1

# Hard filter — Paris not in Berlin's bbox → empty result
/search?q=Paris&viewbox=13.0,52.3,13.8,52.7&bounded=1
```

### Implementation

- `parse_viewbox()` parses the comma-separated string (~line 1228)
- `result_in_bbox()` checks point-in-rectangle (~line 1249)
- Post-processing applied after FST search, before final sort (~line 667)
- No index changes needed

### Limitations

- Does not handle antimeridian wrapping (viewbox crossing ±180° longitude)

---

## Feature 4: /lookup Endpoint

**Endpoint:** `/lookup`

Retrieve place details by Heimdall place ID or OSM ID without re-running search.

### Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `place_ids` | Comma-separated encoded place IDs | `50343944,16777216` |
| `osm_ids` | Comma-separated OSM IDs with type prefix | `R54413,N123456` |
| `format` | Response format | `json` (default) |
| `addressdetails` | Include address breakdown | `0` (default) or `1` |

At least one of `place_ids` or `osm_ids` is required (HTTP 400 otherwise).

### Place ID encoding

Place IDs are globally unique across all loaded countries:

```
place_id = (country_index << 24) | local_record_id
```

- Supports up to 256 countries and ~16M records per country
- Place IDs are **not stable across rebuilds** — only valid for the current loaded index
- O(1) lookup via direct record store array access

### OSM ID format

Prefix + numeric ID: `N` = node, `W` = way, `R` = relation.

An in-memory HashMap is built at startup by scanning all records. Unrecognized IDs are silently omitted from results.

### Error handling

| Condition | Response |
|-----------|----------|
| No parameters | HTTP 400 |
| Invalid place_id format (not a number) | HTTP 400 |
| place_id out of range | Omitted from results |
| osm_id not found | Omitted from results |
| No results found | Empty array `[]`, HTTP 200 |

### Examples

```
# Get place by encoded ID (from previous /search response)
/lookup?place_ids=50343944

# Get multiple places
/lookup?place_ids=50343944,16777216

# Get by OSM relation ID
/lookup?osm_ids=R54413

# With address breakdown
/lookup?place_ids=50343944&addressdetails=1
```

### Implementation

- `lookup()` handler (~line 885)
- `encode_place_id()` / `decode_place_id()` (~line 1056)
- `build_osm_id_map()` scans all records at startup (~line 1080)
- Route registered at `/lookup` alongside `/search`, `/reverse`, `/status`

---

## Test Coverage

Unit tests in `heimdall-api/src/main.rs` (`#[cfg(test)] mod tests`):

| Area | Tests |
|------|-------|
| Structured query parsing | 9 tests (no params, city only, street+city, all fields, empty strings, postalcode-only, amenity-only) |
| Viewbox parsing | 3 tests (valid, swapped coords, invalid input) |
| Viewbox point-in-bbox | 4 tests (inside, outside, edge, invalid coords) |
| Place ID encoding | 5 tests (roundtrip, zero, max record, country bits, truncation) |
| OSM type flags | 2 tests (node vs relation for both string and char variants) |
| PlaceType → settlement | 6 tests (city, town, village, suburb, hamlet, county) |
| AddressDetails serialization | 3 tests (skip-none fields, omit when None, include when Some) |
| US preprocessing | 2 tests (unit designator stripping, state suffix) |

### Integration testing

These features require a running server with loaded indices for full integration testing:

```bash
# Start server
cargo run --release -p heimdall-api -- serve --index data/index-se --index data/index-no

# Structured query
curl "http://localhost:2399/search?city=Stockholm&countrycodes=se"
curl "http://localhost:2399/search?street=Kungsgatan+15&city=Stockholm&countrycodes=se"
curl "http://localhost:2399/search?postalcode=11156&countrycodes=se"
curl "http://localhost:2399/search?city=Stockholm&q=test"  # expect 400

# addressdetails
curl "http://localhost:2399/search?q=Stockholm&addressdetails=1"
curl "http://localhost:2399/reverse?lat=59.33&lon=18.07&addressdetails=1"

# Viewbox
curl "http://localhost:2399/search?q=Storgatan&countrycodes=se&viewbox=17.9,59.8,18.2,59.9&bounded=1"
curl "http://localhost:2399/search?q=Storgatan&countrycodes=se&viewbox=17.9,59.8,18.2,59.9"

# Lookup (use place_id from a search result)
curl "http://localhost:2399/search?q=Stockholm&countrycodes=se" | jq '.[0].place_id'
curl "http://localhost:2399/lookup?place_ids=<id_from_above>"
curl "http://localhost:2399/lookup?place_ids=<id>&addressdetails=1"
curl "http://localhost:2399/lookup?osm_ids=R54391"  # Stockholm relation
```
