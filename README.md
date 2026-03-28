# Heimdall

Compact geocoder for 192 countries. Single Rust binary, no runtime dependencies.

24 countries with official government address data (~280M addresses total). 163 countries via Photon extracts. 5 via OSM-only (Pacific micro-states). 66 language-specific normalizers covering Latin, Cyrillic, Arabic, Hebrew, CJK, Thai, Devanagari, and Ethiopic scripts.

Built on [Finite State Transducers](https://blog.burntsushi.net/transducers/) and memory-mapped files. Serves a Nominatim-compatible search and reverse geocoding API so existing integrations work as a drop-in replacement.

## Countries with official address data

| Country | Addresses | Source | Import command |
|---------|----------:|--------|----------------|
| Sweden | 903K | OSM addr:* | `build` |
| Norway | 2.6M | Kartverket Matrikkelen | `merge-addresses` |
| Denmark | 2.5M | DAWA | `merge-dawa` |
| Finland | 3.7M | DVV (OGC API) | `merge-dvv` |
| Germany | 20M | OSM addr:* | `build` |
| Great Britain | 4.6M | Photon | `photon-import` |
| United States | 146M | OpenAddresses + TIGER | `oa-import` / `tiger-import` |
| Australia | 15.9M | G-NAF | `gnaf-import` |
| Canada | 15.8M | NAR (StatCan) | `nar-import` |
| New Zealand | 2.3M | LINZ | `linz-import` |
| Netherlands | 9.5M | BAG (NLExtract) | `bag-import` |
| France | 26M | BAN (per-département) | `ban-import` |
| Switzerland | 2.2M | swisstopo | `swisstopo-import` |
| Austria | 2.8M | BEV Adressregister | `bev-import` |
| Belgium | 4.5M | BeST | `best-import` |
| Czech Republic | 2.9M | RÚIAN | `ruian-import` |
| Poland | 8M | PRG | `prg-import` |
| Estonia | 400K | ADS | `ads-import` |
| Latvia | 600K | VZD | `vzd-import` |
| Lithuania | 700K | govlt SQLite | `lt-import` |
| Japan | ~50M | ABR (abr-geocoder) | `abr-import` |
| South Korea | ~6M | juso.go.kr | `juso-import` |
| Brazil | 107M | CNEFE 2022 | `cnefe-import` |

Plus **163 countries** via Photon extracts and **5 OSM-only** (Kiribati, Nauru, Niue, Palau, Tuvalu) using the automated `rebuild` pipeline.

## Quick start

### Option 1: Automated rebuild (recommended)

```bash
git clone https://github.com/martinarnell/heimdall.git
cd heimdall
cargo build --release

# Build all configured countries (downloads sources, detects changes, builds indices)
cargo run --release -p heimdall-build -- rebuild

# Or build specific countries
cargo run --release -p heimdall-build -- rebuild --country se,no,dk,fi

# Preview what would change
cargo run --release -p heimdall-build -- rebuild --dry-run

# Two-step: download all sources first (4 concurrent), then build
cargo run --release -p heimdall-build -- download
cargo run --release -p heimdall-build -- rebuild --skip-download
```

### Option 2: Build a single country manually

```bash
# Download OSM data
wget -O data/osm/sweden-latest.osm.pbf \
  https://download.geofabrik.de/europe/sweden-latest.osm.pbf

# Build the index
cargo run --release -p heimdall-build -- build \
  --input data/osm/sweden-latest.osm.pbf \
  --output data/index-se

# Start the server
cargo run --release -p heimdall-api -- --index data/index-se
```

### Run the server

```bash
# Load multiple countries
cargo run --release -p heimdall-api -- \
  --index data/index-se \
  --index data/index-no \
  --index data/index-fr \
  --index data/index-jp
```

Default: `http://127.0.0.1:2399`

### Try some queries

```bash
# Places
curl "http://localhost:2399/search?q=Stockholm"
curl "http://localhost:2399/search?q=München"
curl "http://localhost:2399/search?q=東京"           # Tokyo (kanji)
curl "http://localhost:2399/search?q=서울"            # Seoul (Hangul)

# Addresses
curl "http://localhost:2399/search?q=Kungsgatan+15,+Stockholm"
curl "http://localhost:2399/search?q=10+Downing+Street,+London"
curl "http://localhost:2399/search?q=Rue+de+Rivoli,+Paris"

# Fuzzy / diacritic-free
curl "http://localhost:2399/search?q=Tromso"         # → Tromsø
curl "http://localhost:2399/search?q=Koeln"          # → Köln
curl "http://localhost:2399/search?q=Sao+Paulo"      # → São Paulo

# Script-transparent
curl "http://localhost:2399/search?q=Moscow"         # → Москва (via known_variants)
curl "http://localhost:2399/search?q=Kyiv"           # → Київ

# Country filter
curl "http://localhost:2399/search?q=Bergen&countrycodes=no"

# Reverse geocoding
curl "http://localhost:2399/reverse?lat=48.86&lon=2.35"    # → Paris
curl "http://localhost:2399/reverse?lat=35.68&lon=139.69"  # → Tokyo
```

## Script & language support

| Script | Languages | Features |
|--------|-----------|----------|
| Latin | EN, FR, DE, ES, PT, NL, IT, PL, CZ, ... | Diacritic folding, abbreviations, phonetic encoding |
| Cyrillic | RU, UA, BY | BGN/PCGN transliteration, ё↔е equivalence |
| Arabic | SA, AE, EG, MA, ... (14 countries) | Tashkeel stripping, 28-letter transliteration, "ال"→"al" |
| Hebrew | IL | Niqqud stripping, final form handling |
| CJK | CN, TW, JP | Full-width→half-width, pinyin diacritics, kanji+romaji dual indexing |
| Korean | KR | Hangul + romanized forms, historical McCune-Reischauer |
| Thai | TH | Tone mark stripping, romanized variant bridging |
| Devanagari | IN, NP | Matra stripping, consonant transliteration |
| Ethiopic | ET, ER | Known-variant bridging (syllabary too large for char mapping) |

Normalizer configs are in `data/normalizers/` (66 files). Each config defines abbreviations, diacritics, known variants, character equivalences, and phonetic encoding.

## How it works

### The FST approach

Traditional geocoders (Nominatim, Pelias) use PostgreSQL or Elasticsearch — heavy infrastructure. Heimdall uses:

1. **Finite State Transducers** — map normalized names to record IDs. FSTs compress shared prefixes efficiently.
2. **Memory-mapped record store** — flat binary array of 24-byte records. No deserialization on the hot path.
3. **Street-grouped address storage** — one record per street segment, not per address. 70% compression.
4. **Multi-layer query pipeline** — exact → phonetic → Levenshtein edit-1 → edit-2.
5. **Per-language normalization** — 66 normalizer configs with script-aware processing.

### Query pipeline

```
Input: "Tromso"
  ↓
Normalizer → ["tromsø", "tromso"]        known variant + diacritic
  ↓
FST exact  → hit on "tromsø" → record 8231
  ↓
Record store → Coord(69.649, 18.955), PlaceType::City
  ↓
Admin index → "Tromsø, Troms"
  ↓
Response: {"display_name": "Tromsø, Tromsø, Troms", ...}
```

## Rebuild pipeline

The `rebuild` command reads `data/sources.toml` (194 countries configured) and:

1. Checks for upstream changes (Geofabrik sequence numbers, Photon MD5 hashes, ETags)
2. Downloads only what changed
3. Extracts places + addresses from OSM PBF
4. Merges official address data where available (BAG, BAN, G-NAF, NAR, etc.)
5. Enriches with admin hierarchy
6. Packs into FST indices
7. Saves state for incremental updates

```bash
# Rebuild specific countries
cargo run --release -p heimdall-build -- rebuild --country nl,fr,de

# Force redownload
cargo run --release -p heimdall-build -- rebuild --redownload

# Import official address data for a specific country
cargo run --release -p heimdall-build -- bag-import --index data/index-nl --bag-csv bagadres-full.csv.gz
cargo run --release -p heimdall-build -- ban-import --index data/index-fr
cargo run --release -p heimdall-build -- gnaf-import --index data/index-au --gnaf-zip g-naf.zip
```

## API

Nominatim-compatible search and reverse geocoding. Supported endpoints:

```
GET /search?q=Stockholm&format=json&limit=5
GET /search?q=Kungsgatan+15,+Stockholm&format=json
GET /search?q=Bergen&countrycodes=no&format=json
GET /reverse?lat=59.33&lon=18.07&format=json
GET /status
```

Not yet implemented: `/lookup`, `/details`, POI category search.

## Project structure

```
crates/
  heimdall-core/       FST index, record store, address store, reverse geocoding
  heimdall-build/      OSM extraction, enrichment, 21 country importers, rebuild pipeline
  heimdall-normalize/  Per-language normalization, phonetic encoding, script support
  heimdall-api/        Axum HTTP server, multi-country routing
data/
  normalizers/         66 per-language normalizer configs
  sources.toml         194-country rebuild pipeline configuration
```

## Comparison

| | Heimdall | Nominatim | Pelias |
|---|---|---|---|
| **Dependencies** | None (single binary) | PostgreSQL + Apache | Elasticsearch + 6 Docker containers |
| **Disk (planet)** | ~40 GB (all 194 countries) | ~900 GB | ~50 GB |
| **RAM** | 1–2 GB per country build, mmap at runtime | 64 GB recommended | 8+ GB for Elasticsearch alone |
| **Setup** | `cargo build && rebuild` | Multi-hour planet import | Docker Compose, multiple data downloads |
| **Countries** | 194 | Global | Global |
| **Official address data** | 24 countries (~280M addr) | Full planet | Full planet |
| **Script support** | 9 script families | Full Unicode | Full Unicode |
| **Autocomplete** | Not yet | Yes | Yes |

### Pricing: hosted vs alternatives

| | Heimdall (hosted) | Nominatim (self-hosted) | OpenCage |
|---|---|---|---|
| **Infrastructure** | None (API) | 64 GB RAM server | None (API) |
| **Self-host option** | Yes, free | Yes, free | No |
| **Results storage** | Forever | N/A | Forever |
| **Price per 1K requests** | **$0.029** | ~$0.05 (server cost) | $0.50 |

**17x cheaper than OpenCage** at the Indie tier. Same Nominatim-compatible API format, so switching is a one-line URL change.

## License

The Heimdall software is MIT licensed.

Indices built from OpenStreetMap data are [ODbL](https://opendatacommons.org/licenses/odbl/). You can use the pre-built indices in commercial products without open-sourcing your application — API query results are insubstantial extracts under ODbL, and the build pipeline is open source, satisfying the derived database requirement. Indices that include national address data carry their respective source licenses (see the country table above).
