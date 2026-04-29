# Global Testing Findings — PBF Extraction for 192 Countries

Date: 2026-04-20

## Context

After adding PBF extraction for all 192 countries (replacing Photon as primary source) and fixing the admin enrichment bbox filter, we stress-tested 5 countries (GB, ES, IT, MX, RU) with ~296 queries.

Overall pass rate: 214/296 (72%). Most failures are pre-existing issues now visible with broader country coverage.

## Test Results Summary

| Country | Pass | Wrong | Missing | Bad Admin | Total | Rate |
|---------|------|-------|---------|-----------|-------|------|
| GB | 54 | 15 | 8 | -- | 77 | 70% |
| ES | 34 | 16 | 3 | 5 | 60 | 57% |
| IT | 52 | 14 | -- | -- | 66 | 79% |
| MX | 39 | 4 | -- | 4 | 47 | 83% |
| RU | 35 | 1 | -- | 10 | 46 | 76% |

---

## Issue 1: 3-char Cyrillic crash (CRITICAL)

**Severity:** Critical — server drops connection
**Affected:** Any 3-character Cyrillic query (e.g. "Уфа", population 1.1M)
**Root cause:** `Levenshtein::new()` from the `fst` crate panics when given a 6-byte UTF-8 string (3 Cyrillic chars) with edit distance 1. The DFA builder exceeds internal limits. No panic handler in the Axum search handler.
**Fix:** Add byte-length guard before Levenshtein construction in `global_index.rs:152` and `index.rs:191`. Also consider adding `catch_unwind` in the API handler as defense in depth.
**Files:** `heimdall-core/src/global_index.rs`, `heimdall-core/src/index.rs`
**Effort:** 10 min

## Issue 2: RU federal districts shown instead of oblasts (HIGH)

**Severity:** High — 10+ Russian cities show wrong admin1
**Affected:** Казань shows "Приволжский ФО" instead of "Республика Татарстан", etc.
**Root cause:** `enrich.rs` maps admin_level 3..=4 → counties (admin1). For Russia, level 3 = federal districts (8 macro-regions), level 4 = federal subjects (85 oblasts/republics). Both lumped together; federal districts often win polygon containment. Also: `is_valid_admin_for_country()` has no Russia-specific pattern, so federal districts pass through unfiltered.
**Fix:** Add Russia block to `is_valid_admin_for_country()` that rejects `admin_level == 3` (federal districts). Only accept level 4 (federal subjects) as admin1.
**File:** `heimdall-build/src/enrich.rs`
**Effort:** 15 min

## Issue 3: Bilingual compound names not split for indexing (HIGH)

**Severity:** High — 8+ Italian cities unfindable (Cagliari, Bolzano, Merano, etc.)
**Affected:** Cities with compound bilingual OSM names like "Casteddu/Cagliari", "Bolzano - Bozen"
**Root cause (two parts):**
1. `name_intl` values (e.g. `"de=Bozen"`, `"it=Cagliari"`) are stored in parquet but NOT indexed in the FST. The `pack.rs` code parses them but stores as `"lang=name"` strings without extracting the name component.
2. Compound primary names with `/` or ` - ` separators are indexed as a single string. Searching for just one component fails.

**Fix:**
- In `pack.rs`: Parse `name_intl` field, extract name after `=`, add as FST key
- In `pack.rs`: Split primary names on `/` and ` - `, index each component separately
**File:** `heimdall-build/src/pack.rs`
**Effort:** 1-2 hours

## Issue 4: Cross-country name collisions (HIGH)

**Severity:** High — major cities lose to tiny same-name places in other countries
**Affected:** Pamplona (ES 200K) → Mexican village, Salamanca (ES) → MX city, Belfast (GB 340K) → Spanish bar
**Root cause:** Per-country FSTs store only one record per key. When searching across loaded indices, results are sorted by match_type first, then importance. But importance scores don't differentiate well: City base=2000 vs Village=1000 is only 2x. Population-based scoring is logarithmic but many OSM places lack population tags.
**Fix options:**
- Short-term: Boost City/Town place_type weights significantly (City=5000+)
- Medium-term: When multiple countries return exact matches for same name, prefer the result with higher population/importance
- Long-term: Store multiple postings per FST key
**Files:** `heimdall-build/src/pack.rs` (importance), `heimdall-api/src/main.rs` (ranking)
**Effort:** 2-4 hours

## Issue 5: Multi-word queries fail (MEDIUM)

**Severity:** Medium — multi-word place names with prepositions return no results
**Affected:** "Santa Cruz de Tenerife", "Las Palmas de Gran Canaria", "Reggio Emilia"
**Root cause:** FST keys are full multi-word strings. No tokenization or n-gram generation. Levenshtein distance 1-2 can't bridge word-level differences. Prepositions ("de", "di", "del") are not in stopword lists.
**Fix options:**
- Short-term: Add known_variants to normalizer configs for major cities
- Medium-term: Generate n-gram keys in pack.rs
- Long-term: Token-based search alongside FST
**Files:** `heimdall-build/src/pack.rs`, normalizer TOMLs
**Effort:** 4-8 hours

## Issue 6: Reverse geocoding returns POIs not cities (MEDIUM)

**Severity:** Medium — reverse geocode at city center returns cafe/hotel/shop
**Affected:** 4/5 GB, 4/5 MX, 1/6 RU reverse geocode queries
**Root cause:** Pure nearest-neighbor. A cafe 50m away beats city center 2km away. The 500m populated-place bias (at zoom<=14) is too weak.
**Fix:** Increase bias to 2-5km for low zoom. Add importance as tiebreaker. Prefer City/Town types when zoomed out.
**File:** `heimdall-core/src/reverse.rs`
**Effort:** 1-2 hours

## Issue 7: Canary Islands bad admin hierarchy (ES-specific)

**Severity:** Medium — Tenerife, Las Palmas show Portuguese admin labels
**Affected:** All Canary Islands queries
**Root cause:** Islands far from mainland Spain. Admin polygon coverage may not include Canary Islands provinces, causing centroid fallback to snap to nearest mainland/Portuguese admin.
**Fix:** Verify Canary Islands admin polygons are in the PBF extract. If missing, check if extraction bbox or admin_level filtering excludes them.
**Effort:** Investigation + 1-2 hours

## Issue 8: ES alternate spellings not indexed (LOW)

**Severity:** Low — Castilian alternate names for Catalan/Galician cities fail
**Affected:** "Gerona" (for Girona), "Lérida" (for Lleida), "La Coruña" (for A Coruña)
**Root cause:** These alternate spellings are not in the normalizer `known_variants` for Spanish.
**Fix:** Add to `data/normalizers/es.toml` known_variants section.
**File:** `data/normalizers/es.toml`
**Effort:** 15 min

## Issue 9: Postcode search not supported (LOW)

**Severity:** Low — "SW1A 1AA" returns garbage via phonetic fallback
**Affected:** All postcode queries
**Root cause:** No postcode FST exists. Postcode queries fall through to phonetic matching.
**Fix:** Build postcode FST (planned for Phase 6 in data source plan).
**Effort:** Significant (separate project)
