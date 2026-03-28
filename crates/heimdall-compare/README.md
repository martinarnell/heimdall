# heimdall-compare

Benchmark Heimdall geocoder accuracy against Nominatim. Produces reproducible, publishable accuracy reports.

## Quick start

```bash
# 1. Generate queries from your indices
heimdall-compare generate-queries \
  --index data/index-se --index data/index-no --index data/index-de \
  --count 10000 --seed 42 --output queries.jsonl

# 2. Start Heimdall server (in another terminal)
cargo run --release -p heimdall-api -- --index data/index-se --index data/index-no --index data/index-de

# 3. Run benchmark (1 rps against public Nominatim)
heimdall-compare run \
  --queries queries.jsonl \
  --rps 1 \
  --output results.sqlite

# 4. Generate report
heimdall-compare report --db results.sqlite

# 5. Export to markdown
heimdall-compare report --db results.sqlite --output accuracy.md
```

## Query categories

The benchmark tests five distinct capabilities:

| Category | Share | What it tests |
|----------|-------|---------------|
| **Address round-trips** | 40% | Sample addresses from the index, format as natural queries. Tests whether what was indexed can be found. |
| **Place names** | 30% | Importance-weighted place name lookup. Also generates diacritic-free ("Munchen") and English alias ("Munich") variants. |
| **Fuzzy/typo** | 15% | Deliberately mutated names: missing chars, swapped chars, wrong chars. Tests the fuzzy matching layer. |
| **Reverse geocoding** | 10% | Known coordinates with 10-100m random offset. Tests the spatial index. |
| **Ambiguous** | 5% | Names that exist in multiple countries ("Bergen", "Springfield") with no country filter. Tests ranking. |

## Resumable execution

The benchmark is designed for long runs against public Nominatim (1 rps = ~28 hours for 100K queries). You can stop and restart at any time — completed queries are tracked by ID in SQLite and skipped on resume.

```bash
# Start a benchmark
heimdall-compare run --queries queries.jsonl --output results.sqlite

# Stop with Ctrl+C at any point

# Resume — picks up where it left off
heimdall-compare run --queries queries.jsonl --output results.sqlite
```

## Result categories

Each query is classified by the distance between Heimdall's and Nominatim's top result:

| Category | Distance | Interpretation |
|----------|----------|----------------|
| AGREE | <200m | Both geocoders agree |
| CLOSE | 200m-2km | Same area, minor difference |
| DIVERGE | 2km-20km | Different area |
| CONFLICT | >20km | Major disagreement |
| MISS_H | — | Nominatim found it, Heimdall didn't |
| MISS_N | — | Heimdall found it, Nominatim didn't |

## Investigating conflicts

```bash
# Show all conflicts >2km
heimdall-compare conflicts --db results.sqlite

# Filter by country
heimdall-compare conflicts --db results.sqlite --country DE

# Export for manual review
heimdall-compare conflicts --db results.sqlite --min-distance 5000 --export-csv conflicts.csv
```

## Verify the results yourself

The canonical query file and raw results database are designed to be shared:

```bash
# Download the canonical benchmark
wget https://geoheim.com/benchmark/queries-1m-seed42-v1.jsonl

# Run against your own Nominatim instance
heimdall-compare run \
  --queries queries-1m-seed42-v1.jsonl \
  --nominatim-url http://YOUR_NOMINATIM \
  --rps 50 \
  --output my-results.sqlite

# Generate report
heimdall-compare report --db my-results.sqlite
```

Same queries, different Nominatim version — directly comparable results.

## Self-hosted Nominatim

For faster benchmarks, run Nominatim locally and increase the request rate:

```bash
heimdall-compare run \
  --queries queries.jsonl \
  --nominatim-url http://localhost:8080 \
  --rps 50 \
  --output results.sqlite
```

## Continuous mode

For long-running accuracy monitoring (samples queries on-the-fly from loaded indices):

```bash
heimdall-compare continuous \
  --index data/index-se --index data/index-no \
  --rps 1 \
  --db compare.db
```
