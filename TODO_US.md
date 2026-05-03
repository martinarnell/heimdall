# US Geocoding — Deferred Sources & Future Work

Status of US data sources we've evaluated. Top section = currently wired
(needs only a US rebuild to take effect). Bottom = deferred, with the
reason and what it would buy us.

## Currently wired (rebuild to activate)

| Source | What | Size | Why we picked it |
|---|---|---|---|
| OSM PBF | Places + addr:* (~20 M addresses) | 12 GB | Baseline coverage, free, weekly refresh |
| Photon US extract | 905 K places, 4.6 M addresses, Wikidata tags | 1 GB | Pre-tagged places + landmarks |
| TIGER 2025 STATE/COUNTY/PLACE/ZCTA | Authoritative admin polygons + ZIP boundaries | 3 GB | US federal authoritative source |
| TIGER 2025 COUSUB | County subdivisions (towns/townships) for 21 strong-MCD states | 25 MB | New England + MW use towns, not cities, as primary local govt |
| TIGER 2025 AIANNH | American Indian / Alaska Native / Native Hawaiian areas | 30 MB | Tribal areas don't appear in STATE/PLACE (Navajo Nation, Cherokee Nation, …) |
| Census Gazetteer 2024 (counties) | County populations | 250 KB | Backfills `county.population` for centrality ranking |
| simplemaps US ZIPs | ZIP → city/county crosswalk (CC BY 4.0) | 2 MB | Fixes wrong-city assignment (10001 → NYC instead of Hoboken) |
| OpenAddresses (4 regions) | ~36 M validated addresses | 2 GB streamed | Fills OSM addr:* gaps |
| USGS GNIS Domestic Names | ~1 M+ federally-named features (parks, summits, lakes, schools, churches) | 50 MB | Public-domain US gazetteer, analogue of GN250 (DE) / SSR (NO) |

## Deferred

### National Address Database (NAD) v9 — DEFERRED, disk-hostile

- **What:** USDOT-coordinated authoritative address DB. ~80 M validated addresses
  from 43 participating states + DC + tribal partners. Released Mar 2026.
- **URL:** https://catalog.data.gov/dataset/national-address-database-nad-text-file
- **Size:** ~32 GB CSV (live download).
- **Why defer:** Doesn't fit current dev disk (~16 GB free). On a beefier
  rebuild host this would *strictly improve* coverage where states
  participate — better than OpenAddresses for ~half the country.
- **When to revisit:** When we have ≥ 80 GB free disk for the rebuild,
  OR when we want to do a one-off seed rebuild on cloud infra. The
  importer would mirror `oa.rs` (streaming CSV → Parquet). Likely
  obsoletes most of OA where NAD is present; OA stays as fallback.

### NYC PLUTO / MapPLUTO

- **What:** 860 K NYC tax-lot addresses w/ building footprint centroids.
- **URL:** https://www.nyc.gov/site/planning/data-maps/open-data/dwn-pluto-mappluto.page
- **Cadence:** Semiannual.
- **Why defer:** NAD covers NYC, but PLUTO has *building-footprint precision*
  OA lacks. Add only if NYC accuracy specifically matters (real-estate use
  cases) or NAD is missing/stale for NYC.

### MassGIS Master Address Data

- **What:** ~3 M MA addresses with **weekly** refresh.
- **URL:** https://www.mass.gov/info-details/massgis-data-master-address-data-statewide-address-points-for-geocoding
- **Why defer:** MA isn't a current NAD participant (verify before adding).
  When confirmed, this is the freshest and most accurate MA source —
  weekly beats anyone. Add as `[country.us.massgis]` source.

### State authoritative datasets — case-by-case

- California (CalTrans), Texas (TNRIS), Florida (FGDL), Washington (WSDOT) —
  each has free authoritative state-level address/POI feeds. NAD subsumes
  most. Only worth adding when a specific state's gap is observed (and NAD
  doesn't fix it).

### HUD USPS ZIP Crosswalk (the API version)

- **What:** Quarterly ZIP → county/tract/CBSA with residential/business
  address-count ratios. Free with email registration.
- **URL:** https://www.huduser.gov/portal/datasets/usps_crosswalk.html
- **Status:** **simplemaps US ZIPs** is wired instead (no auth, simpler CSV,
  CC BY 4.0). Same city-assignment outcome. Switch to HUD only if we want
  ratios (residential vs business address counts per ZIP) for ranking.

### TIGER POINTLM (point landmarks)

- **What:** Schools, hospitals, prisons, etc. from federal sources.
- **Size:** ~250 MB SHP.
- **Why defer:** GNIS already covers schools/hospitals/churches with similar
  granularity. POINTLM adds maybe 10-20% more long-tail POIs but
  duplicates a lot. Add only if specific gap surfaces.

### National Park Service Boundaries (NPS IRMA)

- **What:** Park unit boundaries (parks, monuments, historic sites).
- **URL:** https://public-nps.opendata.arcgis.com/
- **Status:** GNIS covers the *names* (with point coords). Boundary polygons
  would only improve reverse geocoding for park interiors. Low priority.

### National Hydrography Dataset (NHD)

- **What:** Rivers, lakes, reservoirs, streams (full geometry).
- **Status:** Names already in GNIS via shared Feature_ID. Polygon geometry
  isn't useful unless we add named-waterbody reverse geocoding. **Skip.**

### Census Gazetteer (states/places)

- **What:** State and place population/centroid files.
- **Status:** State pop currently hardcoded in `tiger.rs::state_pop_lookup`
  (fine — only 56 entries, 2024 vintage). Counties wired (above). Places
  pop would replace OSM-population for US PLACE entries — minor win, defer.

### USPS ZIP+4 file

- **Status:** **Skip.** Paid licence ($20K+/yr from PostalPro). ZCTA + the
  simplemaps crosswalk cover ~98% of geographic ZIPs, which is what users
  actually search.

### NWS / NOAA station lists, TIGER EDGES, TIGER SLDU/SLDL

- **Skip.** Nobody geocodes "KORD" (METAR station). EDGES duplicates OSM
  road geometry (~30 GB). Legislative districts aren't user-search targets.

## Open code TODOs (non-data)

- **`pack_addr.rs`**: `RawAddress.state` field is now populated by `oa.rs` and
  available to all importers, but `StreetKey` still keys on `(street, muni_id)`
  only. Widening to `(street, state_fips, muni_id)` would prevent cross-state
  "Main St" collisions. Requires careful migration of existing FST keys —
  consider doing this in a v6 addr_streets format alongside other changes.
- **`tiger.rs`**: state populations hardcoded for 2024 vintage. Wire the
  Gazetteer state file when we want yearly auto-updates.
- **`gnis.rs`**: doesn't ingest the `DomesticNames_AllNames` companion file
  (~150 MB) which has historical/variant names. Would feed `known_variants`
  for "Fort Pitt → Pittsburgh" style lookups.
- **`oa.rs`**: NAD integration would slot in here as a higher-priority
  address source per state. Schema is similar but state-aware.
- **PlaceType**: GNIS importer collapses Cemetery/School/Church/Bridge/Tower
  onto `Landmark` because no dedicated variants exist. Adding them would
  improve type-based ranking and reverse geocoding zoom filtering.

## Out-of-scope but noted

- **Address verification API** (USPS Address Verification, Smarty): paid
  services that "fix" misspelled addresses. We don't do this — Heimdall is
  a geocoder, not an address validator.
- **Reverse-geocode coverage beyond addresses**: parcels, building
  footprints. NYC PLUTO would help; broader is a separate project.
