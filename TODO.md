# Heimdall — France work, things left to do

Captured at the close of the BD TOPO streaming + BAN + DROM-COM PR (#6).
Sorted by ROI within each section. File paths and effort estimates are
the author's best guess — verify before scheduling.

## Worth doing

### 1. Mont Blanc / alt-name dominance bug
**Symptom**: query `Mont Blanc` resolves to the Gascon village "Monblanc"
(43.46, 0.99) instead of the actual Mont Blanc landmark (45.83, 6.86).

**Root cause**: Monblanc carries `alt_name=Mont Blanc`. The pack-time
alt-name index pass writes "mont blanc" → Monblanc-record at *full*
importance (`crates/heimdall-build/src/pack.rs:351`). The actual
mountain's record is named "Mont Blanc / Monte Bianco"; its slash-split
entry for "mont blanc" is `/128`-demoted (`pack.rs:327`), so it's
buried in the posting list. A `known_variants` redirect to the canonical
slash-form was tried but the API's `normalize_punctuation` strips `/`
before FST lookup (`crates/heimdall-api/src/main.rs`).

**Fix options**:
- Demote alt-name index entries (e.g. `/4` or `/8`) so they outrank
  random fuzzy matches but lose to a same-key landmark's primary or
  slash-split entry. Cleanest. ~50 LOC in `pack.rs`.
- Or: stop stripping `/` in `normalize_punctuation`. Smaller change but
  may have broader effects on slash-split queries.

**Scope**: not France-only — same shape applies anywhere a famous
landmark shares its name with an obscure place that adopted it as
`alt_name`. ~1 hour.

### 2. BD TOPO `editionDate` auto-discovery
Currently a hardcoded constant in `crates/heimdall-build/src/bdtopo.rs`:

```rust
pub const BDTOPO_EDITION_DATE: &str = "2026-03-15";
```

IGN publishes new quarterly snapshots at unpredictable cadences. Bumping
this requires manual edits + recompile.

**Fix**: a `bdtopo update-check` subcommand that scans the parent atom
feed at `https://data.geopf.fr/telechargement/resource/BDTOPO?page=N`,
extracts `<gpf_dl:editionDate>` per zone, finds the latest per
département, and either:
- auto-bumps the constant (writes back to the source file), or
- surfaces a "new release available" warning

Cost: ~2 hours. Removes one manual maintenance step per quarter.

### 3. DROM-COM BD TOPO via UTM reprojection
4 outre-mer zones currently 404 in the streaming downloader because
IGN ships them in non-Lambert-93 CRS:

| Zone | Territory | CRS |
|---|---|---|
| D971 | Guadeloupe | UTM 20N (RGAF09) |
| D972 | Martinique | UTM 20N (RGAF09) |
| D974 | La Réunion | UTM 40S (RGR92) |
| D976 | Mayotte | UTM 38S (RGR92) |

Plus D973 Guyane (UTM 22N, RGFG95) and D975 Saint-Pierre-et-Miquelon
(UTM 21N) which we don't even include in the catalog.

Adding inverse projections (~150 LOC each — same shape as the
existing UTM 32N reprojection in `gn250.rs`) would round out coverage
where OSM is sparsest. Half a day.

Currently we accept the 404s silently and rely on OSM PBFs (the
DROM-COM extras in `sources.toml` `extra_urls`) for those territories.
That's fine, but BD TOPO would add lieux-dits and named natural
features OSM misses.

## Maybe worth doing

### 4. Investigate the lone `MISS_H`
Bench v3 had 1 query (0.2%) where Nominatim found something and
Heimdall didn't. Worth a 15-min look — could be a ranking bug worth
fixing or just a fuzzy-typo edge case worth ignoring.

Run: `target/release/heimdall-compare conflicts --db data/benchmarks/fr-streaming-v3.sqlite --filter MISS_H`

### 5. Per-département parallelism in BD TOPO streaming
Streaming is serial (~10 s per zone, 96 zones = ~14 min). Four parallel
downloads would cut that to ~4 min. IGN's CDN can handle it.
~50 LOC change in `bdtopo::read_bdtopo_streaming`.

Marginal in absolute terms — most rebuild time is in PBF extract
(6 min) and global FST refresh (26 min), not BD TOPO.

## Pipeline-level work, France-adjacent but generic

### 6. Global FST partial-load mismatch
**Bug**: when the API loads only N of M countries that the global FST
was built across, posting `country_id` values desynchronize against
`state.countries[i]`, and lookups silently return empty results.

Bit us twice in the BD TOPO session — workaround was hiding
`data/global/` to force `full` (per-country FST) loading mode.

**Fix**: include the `country_order.json` country code in postings
(or build a fresh per-load remap from country_order.json codes →
loaded country indices). ~1 day. Generic infra fix.

### 7. Incremental global FST refresh
Currently a single-country change rebuilds the entire global FST from
all loaded country indices (26 min for 10 countries). Could be made
incremental — only re-encode the changed country's contribution to
`postings.bin` and merge into the existing FST. Multi-day refactor.
Worth it only if rebuilds happen multiple times a day.

## Probably not worth doing

### 8. Per-record hash diffing within BD TOPO
EditionDate is currently per-département. Even if one record in a
département changed, we re-merge the whole 30K-record file. Hashing
`(cleabs, geom, name)` per record could enable record-level diffing.
But the merge step is cheap (~5 s per département), so the win is
small.

### 9. Self-hosted Nominatim comparison
Public Nominatim's free-text search is meaningfully worse than what
a self-hosted instance can do. Standing up a self-hosted Nominatim
with the same OSM extract would give a fairer head-to-head on
ranking quality (rather than coverage). Big infra setup, marginal
insight beyond "we ingest BD TOPO + BAN, they don't."

### 10. Real-traffic query weighting
The benchmark samples places at random from the index. Real user
query distribution is heavier on cities and famous places (where
both engines tie) and lighter on rural lieux-dits (where we win
big). A weighted benchmark would be more honest, but requires
access to production query logs we don't have.

## Quick reference — where to look

| Concern | File |
|---|---|
| BD TOPO parser + streaming | `crates/heimdall-build/src/bdtopo.rs` |
| BD TOPO version constant | `crates/heimdall-build/src/bdtopo.rs:43` |
| BD TOPO 100-zone catalog | `crates/heimdall-build/src/bdtopo.rs:54` |
| Streaming dispatch in rebuild | `crates/heimdall-build/src/rebuild.rs` (places_source branch) |
| France admin filter | `crates/heimdall-build/src/enrich.rs` (`is_french_region`) |
| Number-first address parser | `crates/heimdall-core/src/addr_index.rs` (`split_street_number`) |
| Postcode display + city baking | `crates/heimdall-build/src/pack_addr.rs` |
| French normalizer | `data/normalizers/fr.toml` |
| Source config (BAN, BDTOPO, PBFs) | `data/sources.toml` (`[country.fr.*]`) |
