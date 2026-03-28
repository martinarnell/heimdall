# heimdall-compare — Development Guide

## What this is

Benchmarking framework for comparing Heimdall geocoder accuracy against Nominatim. Produces reproducible, publishable accuracy reports with downloadable raw data. Designed for credibility: deterministic query generation, resumable execution, and full result transparency.

## Architecture

```
generate-queries:  Load indices → sample 5 categories → write JSONL
run:               Read JSONL → query H + N → categorize → write SQLite
report:            Read SQLite → compute stats → output console/markdown
conflicts:         Read SQLite → filter DIVERGE/CONFLICT → display/CSV
continuous:        Load indices → sample on-the-fly → query + store (legacy)
```

## Module structure

```
src/
  main.rs          CLI dispatcher (clap subcommands)
  lib.rs           Module declarations
  types.rs         QueryEntry, QueryResult, Category enum, categorize(), helpers
  db.rs            SQLite schema, migration v1→v2, CRUD operations
  http.rs          GeocoderClient: forward/reverse for both geocoders, rate limiter
  sampling.rs      Population weights, diacritic stripping, English alias lookup,
                   fuzzy mutation, city resolution, ambiguous name detection
  generate.rs      Query set generation (5 categories, JSONL output)
  runner.rs        Batch benchmark: stream JSONL → query → SQLite (resumable)
  report.rs        Report generation: console tables + markdown
  conflicts.rs     Conflict browser with filtering and CSV export
  continuous.rs    Legacy long-running mode (loads indices, samples live)
```

## Subcommands

```bash
# Generate benchmark query set from loaded indices
heimdall-compare generate-queries \
  --index data/index-se --index data/index-no \
  --count 1000 --seed 42 --output queries.jsonl

# Run benchmark against both geocoders
heimdall-compare run \
  --queries queries.jsonl \
  --nominatim-url https://nominatim.openstreetmap.org \
  --rps 1 --output results.sqlite

# Generate report
heimdall-compare report --db results.sqlite
heimdall-compare report --db results.sqlite --output report.md

# Browse conflicts
heimdall-compare conflicts --db results.sqlite --min-distance 5000
heimdall-compare conflicts --db results.sqlite --country DE --export-csv conflicts.csv

# Legacy continuous mode (samples from indices, runs indefinitely)
heimdall-compare continuous \
  --index data/index-se --rps 1 --db compare.db
```

## Query categories

| Category | % | Method |
|----------|---|--------|
| address | 40% | Sample from AddrStore, format as "street number, city". Population-weighted. |
| place | 30% | Importance-weighted rejection sampling. Generates diacritic-free + English alias variants as bonus queries. |
| fuzzy | 15% | Mutate place names: delete char, swap adjacent, substitute. |
| reverse | 10% | Sample populated-place coordinate, add 10-100m random offset. Uses /reverse endpoint. |
| ambiguous | 5% | Names in 2+ countries (auto-detected + hardcoded). No country filter. |

## JSONL format

First line is metadata header (population weights, seed, counts). Subsequent lines are query entries:

```jsonl
{"_meta":{"version":1,"seed":42,...,"population_weights":[{"code":"SE","population_millions":10.4,...}]}}
{"id":"addr_se_000001","q":"Kungsgatan 15, Stockholm","category":"address","country":"SE","expected_lat":59.334,"expected_lon":18.065}
{"id":"place_de_000001","q":"München","category":"place","country":"DE","expected_lat":48.135,"expected_lon":11.582}
{"id":"place_de_000001_diacfree","q":"Munchen","category":"place","country":"DE","expected_lat":48.135,"expected_lon":11.582,"variant_of":"place_de_000001","variant_type":"diacritic_free"}
{"id":"reverse_au_000001","category":"reverse","country":"AU","lat":-33.8001,"lon":151.2002,"expected_lat":-33.8,"expected_lon":151.2}
{"id":"ambig_000001","q":"Bergen","category":"ambiguous"}
```

## SQLite schema

```sql
CREATE TABLE runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id TEXT NOT NULL,        -- from JSONL "id" (unique, stable)
    query TEXT,                    -- null for reverse queries
    query_type TEXT NOT NULL,      -- address/place/fuzzy/reverse/ambiguous
    country TEXT,                  -- null for ambiguous
    lat REAL, lon REAL,            -- reverse query coordinates
    expected_lat REAL, expected_lon REAL,
    heimdall_lat REAL, heimdall_lon REAL, heimdall_display TEXT, heimdall_ms INTEGER,
    nominatim_lat REAL, nominatim_lon REAL, nominatim_display TEXT, nominatim_ms INTEGER,
    distance_m REAL,               -- H vs N haversine distance
    category TEXT NOT NULL,        -- AGREE/CLOSE/DIVERGE/CONFLICT/MISS_H/MISS_N/...
    variant_of TEXT,               -- parent query_id for variants
    variant_type TEXT,             -- diacritic_free / english_alias
    queried_at TEXT
);
```

## Result categories

| Category | Distance | Meaning |
|----------|----------|---------|
| AGREE | <200m | Both return essentially the same location |
| CLOSE | 200m-2km | Same area, minor coordinate difference |
| DIVERGE | 2km-20km | Different area, possibly wrong admin assignment |
| CONFLICT | >20km | Major disagreement, investigate manually |
| MISS_H | - | Heimdall found nothing, Nominatim did |
| MISS_N | - | Heimdall found it, Nominatim didn't |
| BOTH_MISS | - | Neither found a result |

For ambiguous queries: AGREE/CONFLICT measure whether H and N chose the same interpretation, not correctness (no ground truth).

## Key design decisions

- **Deterministic generation.** Fixed RNG seed → same queries every time. Published query file is the reproducibility guarantee.
- **Population-weighted sampling.** Countries weighted by population, not address count. Germany (83M) gets more queries than Estonia (1.3M).
- **Resumable execution.** SQLite unique index on `query_id`. Stop/restart skips completed queries. Single-writer model (no concurrent benchmarks on same DB).
- **Decoupled pipeline.** Generate queries once, run benchmarks many times (different Nominatim versions, different hardware). JSONL file is the stable interface.
- **Variants as bonus queries.** Diacritic-free and English aliases don't count toward the 30% place budget. They're extra queries tagged with `variant_of` for separate analysis.

## Dependencies

- `heimdall-core` — RecordStore, AddrStore, Coord for index access
- `heimdall-normalize` — Normalizer.known_variants() for English alias reverse-lookup
- `rusqlite` — SQLite storage (bundled, no system dependency)
- `reqwest` — HTTP client for geocoder APIs
- `rand` — Deterministic sampling (StdRng with seed)
- `chrono` — Timestamps for reports
- `toml` — Parse normalizer configs from index directories

## Common development tasks

```bash
cargo check -p heimdall-compare    # Fast check
cargo build --release -p heimdall-compare  # Release build
cargo test -p heimdall-compare     # Run tests (when added)
```
