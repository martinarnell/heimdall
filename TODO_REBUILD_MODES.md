# Rebuild Modes — Adaptive Resource-Bounded Builds

> **End goal:** anyone with a $200 used laptop should be able to rebuild any
> Heimdall country (or the entire planet, slowly) without OOM, without
> filling their disk, and without buying cloud capacity. Slower is fine —
> "won't run at all" is not.
>
> Code-named **`--budget potato`**.

This document is the design + implementation roadmap for resource-bounded
rebuild modes. Self-contained: a fresh chat should be able to pick this up
cold, read the cited code, and start implementing Phase 1.

---

## TL;DR — What we're shipping

Five phased tickets, each ships independently:

| Phase | What | Effort | Win |
|---|---|---|---|
| **1** | Cleanup flags (delete sources + intermediate parquet on success) | ~1 day | US peak disk ~37 GB → ~12 GB |
| **2** | Bounded-RAM pack (external sort, tiered FST build) | ~1 week | US peak RAM ~6 GB → ~512 MB |
| **3** | Adaptive resource budget (`--budget potato/laptop/server`, never-OOM observer) | ~1 week | "1 GB RAM, 10 GB disk" target hit |
| **4** | Per-step diff (only re-run phases whose inputs changed) | ~1 week | US incremental rebuild 75 min → 10-15 min |
| **5** | Pre-sort upstream of pack (sort during enrich, drop pack's SortBuffer) | ~3-5 days | Pack wall time -30-50% on every rebuild; cleaner streaming pack |

Phases 1+2 alone unlock "rebuild on a laptop". Phase 3 hits the potato goal.
Phase 4 is the perf killer for ongoing rebuilds. Phase 5 is the common-path
perf win — every rebuild benefits, not just the rare incremental ones.

**Status (May 2026):** Phases 1, 2, 3, 4 implemented (Phases 1-3 merged on
main; Phase 4 on this branch — see "Phase 4 — shipped" section). Phase 5
not started.

---

## Today (May 2026) — what's already in the codebase

### What works well (don't break)

- **Streaming sources with delete**: `oa.rs` downloads each of the 4 OA US
  regional ZIPs in turn → parses → flushes → deletes. Peak download disk
  for US OA is ~2 GB even though the cumulative size is ~8 GB. **This is
  the reference pattern for streaming importers.**
- **MmapNodeCache + SortedFileNodeCache**: `crates/heimdall-core/src/node_cache.rs`
  defines the `NodeCache` trait with multiple implementations. The OSM
  extract uses **external merge sort backed by a temp file** — see the log
  line `"Using sorted-file node cache (external merge sort, ~500 MB RAM)"`.
  This is the pattern Phase 2 will reuse for FST construction.
- **Photon extract auto-cleanup**: `rebuild.rs:1486-1500` — extracts the
  Photon tar.bz2 into a tmp dir, parses, then `remove_dir_all` on the way
  out. Already correct.
- **Diff-based rebuild**: change detection per source via ETag /
  sequence number / MD5. `rebuild.rs` uses `data/rebuild-state.json` as
  the source-of-truth. If a source is unchanged, no download.
- **`--cleanup` flag (partial)**: `main.rs:514` exposes `cleanup: bool` to
  the rebuild subcommand; `rebuild.rs:1613, 2370` consumes it. Currently
  it removes some intermediate files but coverage is incomplete — see
  Phase 1 below.
- **`keep_backup` flag**: `repack` subcommand at `main.rs:633` opt-in
  preserves `*.bin.v2.bak` / `*.bin.v4.bak` artefacts. Phase 1 should
  flip the default to `false`.
- **Per-country gate**: `--country us` already implemented and works.

### What's NOT optimised (the targets)

| Bottleneck | Code location | Today | After roadmap |
|---|---|---|---|
| Downloads kept indefinitely | `data/downloads/<cc>/` | ~26 GB for US, accumulates per country | Phase 1: `--keep-downloads=N` |
| Intermediate parquet kept | `data/index-<cc>/*.parquet` | 4-5 GB for US | Phase 1: `--cleanup` covers it |
| In-memory sort during pack | `pack_addr.rs:259` (`fst_keys.sort_by`), `pack.rs:974` | All FST keys in `Vec<(String, u32)>`. US = ~30M × 50 B = 1.5 GB just for keys | Phase 2: external merge sort on disk |
| In-memory FST builder buffer | `pack_addr.rs:273-277`, `pack.rs:577, 596` | `MapBuilder::new(file)` buffers in RAM until `finish()` | Phase 2: pre-sort + stream insert in order; bounded RAM |
| Static RAM budget per country | `data/sources.toml` `ram_gb = 9` (DE), `16` (US) | Hint for parallel scheduling, no actual enforcement | Phase 3: real RSS observer |
| All-or-nothing per country | `rebuild.rs` per-country pipeline | Any source change → whole country rebuilds (extract + merge + enrich + pack) | Phase 4: per-step `.done` sentinels |

---

## Phase 1 — Cleanup flags (1 day, biggest disk win)

### Goal

After a successful rebuild, peak disk should drop from ~37 GB (US) to
~12 GB. Default behaviour for the weekly rebuild stays unchanged
(it can keep everything for change-detection); the new flags are opt-in
for resource-constrained builds.

### Implementation

#### 1.1 Strengthen the existing `--cleanup` flag

`rebuild.rs:1613` already accepts `cleanup: bool` and `:1921` notes
"to reclaim disk space, use `rebuild --cleanup`". Audit what it
currently removes vs. what it should:

```
Should remove on success when --cleanup is set:
  ☐ data/index-<cc>/places.parquet              (intermediate)
  ☐ data/index-<cc>/addresses.parquet           (intermediate)
  ☐ data/index-<cc>/addresses_photon.parquet    (intermediate)
  ☐ data/index-<cc>/addresses_national.parquet  (intermediate)
  ☐ data/index-<cc>/admin_map.bin               (build-time only, per CLAUDE.md)
  ☐ data/index-<cc>/photon_extract/             (likely already done — verify)
  ☐ data/downloads/<cc>/photon-*.tar.bz2        (after Photon merged)
  ☐ data/downloads/<cc>/<tiger>/*-state.zip     (after each state's PLACE parsed)
  ☐ data/downloads/<cc>/oa-*.zip                (after each OA region merged — verify oa.rs)
  ☐ Any "*.bak" artefacts older than the current build (older format
    versions, if a repack happened mid-rebuild)

Should NOT remove (still needed):
  ✗ data/index-<cc>/*.fst, *.bin                (the actual index!)
  ✗ data/index-<cc>/sv.toml, meta.json, states.json  (runtime config)
  ✗ data/downloads/<cc>/<source>.etag           (change detection)
  ✗ data/rebuild-state.json                     (change detection)
```

#### 1.2 New flag: `--keep-downloads=N`

```
--keep-downloads=0    # delete every source after use (zero cache)
--keep-downloads=1    # keep current download for change-detection (default for weekly)
--keep-downloads=2    # keep last 2 (allows quick rollback)
```

Implementation: per-source ETag/sequence files stay regardless. The
actual data files (PBF, ZIP, tar.bz2) get rotated.

#### 1.3 New flag: `--keep-intermediates=N`

```
--keep-intermediates=0    # delete *.parquet after pack (default for --cleanup)
--keep-intermediates=1    # keep current build's parquet (debugging, default today)
```

#### 1.4 Default `keep_backup=false` for repack

`main.rs:633`: flip the default from `true` to `false`. The .bak files we
saw consuming ~400 MB on prod (`addr_streets.bin.v4.bak`,
`records.bin.v2.bak`) shouldn't accumulate by default — they're a
debugging convenience.

#### 1.5 Logging

Print a "disk reclaimed" line at the end of each country:

```
[us] cleanup: deleted 4.4 GB intermediate parquet, 12 GB cached PBF
[us] index: 5.8 GB (no change)
[us] peak disk during build: 18.2 GB
```

Add a `--dry-run-cleanup` for users who want to preview before deleting.

### Files to touch

- `crates/heimdall-build/src/main.rs` — extend the rebuild subcommand args
- `crates/heimdall-build/src/rebuild.rs` — wire flags into per-country loop, add cleanup helpers
- `crates/heimdall-build/src/oa.rs` — verify per-region cleanup works as expected
- `crates/heimdall-build/src/tiger.rs` — verify per-state PLACE ZIP cleanup
- `data/sources.toml` — optional `[defaults] cleanup_after = true` for production

### Acceptance

- `heimdall-build rebuild --country us --skip-download --cleanup --keep-downloads=0`
  finishes with `data/index-us/` containing only the 6 GB of actual
  index files (no `*.parquet`, no downloads cache for US).
- Default behaviour (no flags) unchanged — weekly rebuild still keeps
  everything for change detection.
- New unit tests in `rebuild.rs` for the cleanup helpers (no full rebuild needed).

---

## Phase 2 — Bounded-RAM pack (1 week, the actual potato unlock)

### Goal

US rebuild's peak RAM should drop from ~6 GB to ≤ 512 MB. Mechanism:
external merge sort for FST keys; tiered build for the country FST.

The pattern already exists for OSM nodes (`SortedFileNodeCache`). We're
applying it to the address/place FST construction.

### Why it matters

The `fst` crate's `MapBuilder` requires inserts in **lexicographically
sorted order**. Today we collect all keys into a `Vec`, sort in memory,
then stream into the builder. For US that's ~30M `(String, u32)` pairs ≈
1.5 GB just for keys, plus the value vector, plus FST internal buffers.

External merge sort solves this with a configurable RAM budget:
1. Read N records into RAM up to `--sort-mem` bytes
2. Sort that batch, write to a temp run file
3. Repeat until input exhausted
4. K-way merge the run files via min-heap, streaming results into MapBuilder

Bounded RAM, ~2× more disk I/O, slower by maybe 30%.

### Implementation

#### 2.1 Add `crates/heimdall-build/src/sort_buffer.rs`

A reusable external-sort utility:

```rust
pub struct SortBuffer<K, V> {
    mem_limit: usize,
    runs: Vec<PathBuf>,
    current: Vec<(K, V)>,
    current_bytes: usize,
}

impl<K: Ord + Encode, V: Encode> SortBuffer<K, V> {
    pub fn new(mem_limit_bytes: usize, scratch_dir: &Path) -> Self;
    pub fn push(&mut self, k: K, v: V);          // spills to disk if over limit
    pub fn finish(self) -> impl Iterator<Item = (K, V)>;  // k-way merge
}
```

Ground rules:
- Encode/decode keys+values via `bincode` or `postcard` for compact runs
- Default `mem_limit = 256 MB`
- Spill files go in `<scratch_dir>/sort-<uuid>/` and are deleted on drop
- Use `binary_heap::BinaryHeap<Reverse<(K, RunIdx)>>` for the merge

#### 2.2 Replace in-memory sort in `pack_addr.rs:259`

Today:
```rust
let mut fst_keys: Vec<(String, u32)> = Vec::new();
// ... push 30M items ...
fst_keys.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
fst_keys.dedup_by(|a, b| a.0 == b.0);
let mut fst_builder = MapBuilder::new(fst_file)?;
for (key, id) in &fst_keys {
    fst_builder.insert(key.as_bytes(), *id as u64)?;
}
```

After:
```rust
let mut buf = SortBuffer::<String, u32>::new(sort_mem, &scratch_dir);
// ... buf.push(key, id) for each item ...
let mut fst_builder = MapBuilder::new(fst_file)?;
let mut prev_key: Option<String> = None;
for (key, id) in buf.finish() {
    if Some(&key) == prev_key.as_ref() { continue; } // dedup inline
    fst_builder.insert(key.as_bytes(), id as u64)?;
    prev_key = Some(key);
}
```

#### 2.3 Same swap in `pack.rs:974` (place FST)

#### 2.4 Tiered build option for the country FST (optional)

For very large countries (US, RU, DE, FR), instead of one giant sort:
- Build per-state FSTs first (each fits in 256 MB sort buffer comfortably)
- Stream-merge state FSTs into the country FST as a final pass

The `fst` crate has `OpBuilder::union()` which streams-merges multiple
FSTs without materialising them. Each per-state FST is bounded by the
state's address count.

This is optional; Phase 2 ships fine with just the external sort.

#### 2.5 New flags

```
--sort-mem=256M        # bytes per sort-buffer batch (default 256 MB)
--scratch-dir=/tmp     # where spill files go (default $TMPDIR)
--pack-mode=tiered     # opt into per-state tiered build (US/RU only)
```

### Files to touch

- New: `crates/heimdall-build/src/sort_buffer.rs`
- `crates/heimdall-build/src/pack.rs` — swap sort
- `crates/heimdall-build/src/pack_addr.rs` — swap sort
- `crates/heimdall-build/src/main.rs` — flags
- `crates/heimdall-build/src/rebuild.rs` — plumbing

### Acceptance

- `heimdall-build rebuild --country us --sort-mem=128M` builds with peak
  RSS ≤ 1 GB (vs. ~6 GB today)
- Output FST is byte-identical to the in-memory-sort version
- Build time ≤ 2× the in-memory baseline (acceptable trade)
- New benchmark in `crates/heimdall-build/benches/` measuring sort-buffer
  scaling vs. in-memory

---

## Phase 3 — Adaptive resource budget (1 week, the "never OOM" promise)

### Goal

Heimdall rebuilds NEVER OOM. If the user says `--budget potato`, the
rebuild adapts mid-flight: if RSS approaches the limit, parallelism
drops, sort buffer flushes early, and the next phase waits.

### Implementation

#### 3.1 Resource observer

A small module `crates/heimdall-build/src/resource_monitor.rs`:

```rust
pub struct ResourceBudget {
    pub max_ram_bytes: u64,
    pub max_disk_bytes: u64,
    pub max_threads: usize,
    pub scratch_dir: PathBuf,
}

pub struct ResourceMonitor {
    budget: ResourceBudget,
    rss_signal: watch::Sender<u64>,  // tokio::sync::watch
}

impl ResourceMonitor {
    pub fn spawn(budget: ResourceBudget) -> Self;  // background task polls /proc/self/status:VmRSS every 1s
    pub fn pressure(&self) -> Pressure;            // None | Soft (>70%) | Hard (>90%)
    pub fn signal(&self) -> watch::Receiver<u64>;
}

pub enum Pressure { None, Soft, Hard }
```

Plumbed into the sort buffer (Phase 2):
```rust
// In SortBuffer::push, before adding to current batch:
if monitor.pressure() == Pressure::Hard {
    self.spill_current_run();  // flush early instead of growing
}
```

And into the rebuild scheduler:
```rust
// In rebuild.rs per-country loop:
let parallel_jobs = match monitor.pressure() {
    Pressure::None => budget.max_threads,
    Pressure::Soft => (budget.max_threads / 2).max(1),
    Pressure::Hard => 1,
};
```

#### 3.2 Disk-budget tracking

Walk `data/index-<cc>/` and `data/downloads/<cc>/` periodically;
if total approaches `--max-disk`, force-trigger Phase 1 cleanup of
already-completed steps' intermediates.

#### 3.3 Step-level checkpointing (foundation for Phase 4)

After each phase writes its output (parquet, FSTs, etc.), drop a
`.done` sentinel in `data/index-<cc>/checkpoints/`:

```
data/index-<cc>/checkpoints/
  extract.done       (places.parquet + addresses.parquet ready)
  national.done      (national merge complete)
  places_source.done (e.g. GNIS merged in)
  photon.done        (Photon merged)
  enrich.done        (admin enrichment done)
  pack.done          (final FSTs written)
```

Each sentinel records `{started_at, finished_at, input_hash, output_size}`
in JSON. On rebuild, the planner reads checkpoints and the change-detection
state to decide which phases need re-running (foundation for Phase 4).

After OOM/crash + restart, the rebuild can resume from the most recent
sentinel instead of starting from scratch.

#### 3.4 Presets

```
--budget potato       # 1 GB RAM, 10 GB disk, 1 thread, --cleanup, --sort-mem=64M, --keep-downloads=0
--budget laptop       # 4 GB RAM, 30 GB disk, 2 threads, --cleanup, --sort-mem=512M
--budget workstation  # 16 GB RAM, 100 GB disk, n cores, default cleanup behaviour (current default)
--budget server       # 64 GB RAM, 1 TB disk, all cores, --keep-everything (current weekly rebuild)
```

Presets are sugar over the explicit flags; users can override individual
fields:

```
heimdall-build rebuild --budget potato --max-ram=2G   # potato preset but with 2 GB RAM
```

#### 3.5 Pre-flight check

Before starting a rebuild, check available system resources and warn:

```
$ heimdall-build rebuild --country us --budget laptop
=== pre-flight ==========================================
  Available RAM:    3.8 GB / 4.0 GB requested  ⚠ TIGHT
  Available disk:   12 GB / 30 GB requested    ✗ INSUFFICIENT
  CPU cores:        8 / 2 requested            ✓ OK
  Network:          (skipped, --skip-download)
=========================================================
ERROR: Insufficient disk space. Either:
  - Free 18 GB more disk
  - Use --budget potato (10 GB needed)
  - Use --keep-downloads=0 to recycle aggressively
```

### Files to touch

- New: `crates/heimdall-build/src/resource_monitor.rs`
- New: `crates/heimdall-build/src/budget.rs` (preset definitions)
- `crates/heimdall-build/src/rebuild.rs` — plumb monitor + checkpoints
- `crates/heimdall-build/src/main.rs` — `--budget`, `--max-ram`, `--max-disk` flags + pre-flight
- `crates/heimdall-build/src/sort_buffer.rs` — accept monitor signal

### Acceptance

- `heimdall-build rebuild --country us --budget potato` completes
  successfully on a 2 GB RAM, 15 GB disk VM (cloud micro instance)
- Pre-flight catches insufficient disk before writing anything
- Mid-build OOM never happens — verify with `valgrind --tool=massif`
  or just `time` + `--max-ram=512M` on a country that previously OOMed
- Resume-from-checkpoint works: `kill -9` mid-build then re-run; should
  pick up at the most recent sentinel

---

## Phase 4 — Per-step diff (1 week, ongoing rebuild speed)

### Goal

US rebuild incremental time: 75 min → 10-15 min when only one source changed.

Builds on Phase 3's checkpoints. Now the planner doesn't just resume
after crash — it looks at which sources actually changed and only re-runs
the phases downstream of changed inputs.

### Dependency graph

```
extract (PBF)
    │
    ├──> national merge (DAWA / TIGER / OA / NAD / ...)
    │       │
    │       └──> places_source merge (GNIS / GN250 / SSR / DAGI)
    │               │
    │               └──> photon merge
    │                       │
    │                       └──> enrich (admin)
    │                               │
    │                               └──> pack (FSTs)
    │
    └──> (also feeds enrich)
```

If only Photon changed: skip extract, national, places_source. Re-run
photon → enrich → pack. ~20 min instead of 75.

If only GNIS changed: skip extract, national. Re-run places_source →
photon → enrich → pack. ~30 min.

### Implementation

#### 4.1 Input-hash per checkpoint

Each `.done` sentinel records a hash of its inputs:

```json
{
  "phase": "extract",
  "started_at": "2026-05-03T14:00:00Z",
  "finished_at": "2026-05-03T14:42:00Z",
  "inputs": {
    "us-latest.osm.pbf": "sha256:abc...",
    "config_version": "1.0"
  },
  "outputs": {
    "places.parquet": "size:187M",
    "addresses.parquet": "size:4.4G"
  }
}
```

#### 4.2 Planner

Before running each phase, compare current input hashes vs. the
sentinel's recorded inputs:

```rust
fn should_run(phase: Phase, inputs: &Inputs, ckpt: Option<&Checkpoint>) -> bool {
    match ckpt {
        None => true,                              // never built
        Some(c) if c.inputs != inputs => true,    // inputs changed
        Some(_) => false,                          // up to date
    }
}
```

A downstream phase always runs if any upstream phase ran (cascade).

#### 4.3 New flag: `--force=phase1,phase2`

Force re-run of specific phases regardless of hashes (escape hatch for
debugging):

```
heimdall-build rebuild --country us --force=enrich,pack
```

#### 4.4 New flag: `--show-plan`

Dry-run the planner and show what would actually re-run:

```
$ heimdall-build rebuild --country us --show-plan
=== rebuild plan: us ====================================
  extract       SKIP (PBF unchanged, sentinel at 2026-05-03T14:42)
  national      SKIP (TIGER unchanged)
  places_source SKIP (GNIS unchanged)
  photon        RUN  (Photon ETag changed: e1c2...→f3a4...)
  enrich        RUN  (cascade from photon)
  pack          RUN  (cascade from enrich)
=========================================================
estimated time: ~18 min
```

### Files to touch

- New: `crates/heimdall-build/src/checkpoint.rs`
- `crates/heimdall-build/src/rebuild.rs` — planner integration
- Each phase function: read inputs hash, write sentinel on success
- `crates/heimdall-build/src/main.rs` — `--force`, `--show-plan` flags

### Acceptance

- A second rebuild run immediately after the first does almost nothing
  (`<30 s`, all phases skip)
- After replacing the Photon ETag in `rebuild-state.json`, only photon →
  enrich → pack run, in ~20 min for US
- `--show-plan` accurately predicts what will run
- Force flag works as escape hatch

---

## Phase 4 — shipped (notes for future tweaks)

What landed differs from the original sketch in two notable ways. Both
are pragmatic narrowings, not regressions; flagging them here so anyone
returning to the file knows the as-built shape.

1. **Cascade is linear, not graph-aware.** The merge phases
   (extract → national → places_source → photon) all mutate the same
   `places.parquet` / `addresses.parquet` pair. They're not independent
   nodes — re-running national on a parquet that's already been mutated
   by photon would double-merge addresses. So Phase 4 cascades downstream
   only: any earlier phase RUN forces every later phase RUN with reason
   `"cascade from upstream"`. The sketch's "if only Photon changed,
   skip extract+national" example is partially true today: photon's
   own checkpoint can SKIP if photon.md5 is unchanged, but if e.g.
   national.etag changed, we re-run from national onward (national →
   photon → enrich → pack), not just national.

   The address merge is dedup-aware (spatial cell + name hash), so
   re-running photon on a post-photon parquet is roughly idempotent in
   practice — but we don't lean on that yet. Future work could refactor
   the merge functions to be strictly idempotent and unlock finer-grained
   cascades.

2. **Enrich has no on-disk sentinel.** Enrich produces an in-memory
   `Enriched` value that pack consumes directly. The two phases must run
   as a unit (skipping enrich would leave pack with nothing to read), so
   the planner welds `enrich.decision = pack.decision` and only writes
   `pack.done`. `Phase::Enrich` still appears in `--show-plan` output
   (informationally) and in the cascade order, but it's never persisted.

### What the sentinels store

Each `<phase>.done` is JSON of shape:

```json
{
  "phase": "photon",
  "started_at": 1777886616, "finished_at": 1777886617,
  "outputs": [{"name": "addresses_photon.parquet", "size_bytes": 524288}, ...],
  "inputs": {
    "_format": "v4",
    "photon.md5": "d92b2770896e2a5fbe0a6e182e0ea6a2"
  },
  "summary": "+1144 places  +519 addr"
}
```

The `_format` marker distinguishes Phase-4 sentinels from Phase-3-era
ones (which had no `inputs` field). When the planner sees a legacy
sentinel it falls back to "skip if outputs present" rather than forcing
a full rebuild — so first-upgrade is a no-op for users with healthy
Phase-3 sentinels.

### Per-phase fingerprint keys

| Phase         | Keys |
|---------------|------|
| extract       | `osm.sequence`, `osm.etag`, `osm.extra_urls` (sorted, comma-joined) |
| national      | `national.type`, `national.etag`, `national.sequence` |
| places_source | `places_source.type`, `places_source.etag`, `places_source.zones` (canonical `D075=2026-03-15,D971=…`) |
| photon        | `photon.md5`, `photon.etag` |
| enrich        | (welded to pack — no fingerprint, no sentinel) |
| pack          | (cascade-only — empty inputs map besides `_format`) |

The `places_source.zones` encoding deliberately sorts before joining so
that BD TOPO's per-département map is stable across runs even though
Rust's `HashMap` iteration is randomised.

### Verification

* Unit tests: `crates/heimdall-build/src/checkpoint.rs` (15 tests on
  diff/cascade/legacy-fallback) and `rebuild::plan_tests` (7 tests on
  cascade ordering, missing-output invalidation, fingerprint-key
  encoding).
* End-to-end test: `crates/heimdall-build/tests/phase4_e2e.sh` builds
  Luxembourg from scratch (~30 s on a beefy box, ≤2 min on a potato),
  asserts all sentinels land with `_format=v4`, then re-runs and
  verifies all-SKIP, then mutates `photon.done`'s recorded md5 and
  verifies only photon → enrich → pack RUN.
* `Dockerfile.test-phase4` runs the same script in a clean container —
  smoke test for ops scripts that pin a specific Rust toolchain.

### Files touched

- `crates/heimdall-build/src/checkpoint.rs` — `inputs: BTreeMap`,
  `Decision::{Run{reason},Skip}`, `diff()`, `cascade_decide()`, format
  marker.
- `crates/heimdall-build/src/rebuild.rs` — `compute_country_plan()`,
  `extract_inputs/national_inputs/places_source_inputs/photon_inputs()`,
  `run_phase` and `write_phase_done` updated to take inputs and a
  `Decision`. `--show-plan` rewired to use the planner.
- `crates/heimdall-build/src/main.rs` — help text refresh on
  `--show-plan`.
- `Dockerfile.test-phase4`, `crates/heimdall-build/tests/phase4_e2e.sh`,
  `.dockerignore` — e2e harness.

### Open Phase-4 follow-ups (not blocking)

* Idempotent merges → finer-grained cascade (only re-run the changed
  source's branch). Probably 1-2 weeks of merge-function refactoring.
* `--force=all` is currently identical to wiping `checkpoints/`; no
  change needed unless we extend forces to also wipe specific outputs.
* The cascade reason is currently flat ("cascade from upstream"); a
  nicer string would be "cascade from photon (input changed)" so users
  can trace the trigger one step up. Trivial follow-up.

---

## Phase 5 — Pre-sort upstream of pack (3-5 days, the common-path win)

### Goal

Pack becomes a pure streaming pass: read parquet, encode (key, value),
insert into MapBuilder. No in-memory sort, no SortBuffer, no spill files.
The sort moves *upstream* into the step that already has the data in RAM
to do its own work — `enrich`.

This is the "common path" optimisation: every full rebuild benefits, not
just the rare "only photon changed" incremental rebuilds that Phase 4's
finer cascade would target.

### Why "upstream" not "during extract"

The original sketch in TODO question #3 said "pre-sort during extract".
That framing was slightly off: the FST keys for both place and address
indices include admin info (`admin1_id`, `admin2_id`, `municipality_id`)
that **extract doesn't know yet** — admin assignment happens in `enrich`
via point-in-polygon. So extract can't sort by the FST key directly.

The right place is `enrich`. By the time enrich finishes, every place and
every address has its admin IDs. Sort the in-memory `Vec<RawPlace>` and
`Vec<RawAddress>` by FST key before writing the parquet back, and pack
no longer needs to sort.

### Why it matters

After Phase 2's SortBuffer, pack is bounded-RAM but still does a real
amount of work:

- Push 30M `(String, u32)` pairs to SortBuffer (US): ~1.5 GB written to
  scratch
- K-way merge the spill files: another sequential pass over those 1.5 GB
- *Then* the actual FST construction (which is what we actually want)

Pre-sort eliminates the first two. Rough extrapolation from Luxembourg's
pack times (1.3 s places + 1.5 s addresses, 22 K + 170 K rows):

| Country | Pack today (est.) | Pack after | Saved/run |
|---|---|---|---|
| Luxembourg | ~3 s | ~2 s | ~1 s |
| Germany | ~90 s | ~50 s | ~40 s |
| US | ~6 min | ~3 min | ~3 min |
| Planet | ~30 min | ~15 min | ~15 min |

Numbers are rough — we'll measure on the first DK + DE + US runs and
update. The "saved per run" applies to **every** rebuild, full or
incremental.

Secondary wins:

- **Pack code simplifies.** Pure streaming function: read row, build
  key, insert. No SortBuffer state, no scratch dir, no pressure signal
  threading.
- **Less /tmp churn.** SortBuffer's spill files were ~2× the key
  payload size. On potato hardware that was ~3 GB scratch on US;
  Phase 5 drops it to zero.
- **Pack peak RAM drops further** (already low after Phase 2). Pure
  streaming = O(1) RAM regardless of country size.

### Implementation

#### 5.1 Sort-and-rewrite at the end of `enrich()`

`crates/heimdall-build/src/enrich.rs` currently:

```rust
fn enrich(places_parquet: &Path, output_dir: &Path) -> Result<EnrichResult> {
    // 1. Read places.parquet into Vec<RawPlace>
    // 2. Build admin polygons, point-in-polygon assign
    // 3. Read addresses.parquet into Vec<RawAddress>, assign admin
    // 4. Write admin.bin + admin_map.bin
    // 5. Return EnrichResult
}
```

Phase 5 adds step 4b before step 4: sort the two vecs by their FST key,
then rewrite the parquets. The keys are computed using the same
normaliser pack uses, so the ordering matches what pack would have
produced via SortBuffer.

For laptop+ (≥4 GB RAM), single in-memory sort is fine — US 30M rows ×
~80 B = ~2.4 GB, which fits. For potato (1 GB RAM), reuse the existing
SortBuffer with the pressure signal so it spills correctly. Wrapping
the sort with `if monitor.pressure() == Pressure::Hard { external_sort }
else { in_place_sort }` is a few lines.

#### 5.2 Pack drops SortBuffer

`pack.rs` and `pack_addr.rs` lose their SortBuffer wiring. Read parquet
rows in order, insert into MapBuilder. Add an invariant assertion:
`debug_assert!(prev_key <= current_key)` so a misordered upstream is
caught loud + early rather than producing a broken FST silently.

#### 5.3 FST byte-for-byte parity check

Pack is the most byte-stable part of the build (any reorder produces a
different FST). To guard against silent regression, add a CI smoke test
that builds DK both ways (current path with SortBuffer, Phase-5 path
with pre-sort) and asserts `cmp` of `fst_exact.fst` and `fst_addr.fst`.
First run is opt-in (`--phase5-parity-check`); after we trust it, drop
the legacy path.

#### 5.4 Optional: sort key tweak for US

US currently picks up implicit state-grouping via TIGER's per-state
ingestion. Phase 5 makes it explicit: address sort key
`(state_fips, muni_id, normalized_street, housenumber)` — same FST
output, but better IO locality during enrichment iteration. Probably
not measurably faster; flag for later if benchmarks show otherwise.

### Files to touch

- `crates/heimdall-build/src/enrich.rs` — add the sort + rewrite step,
  using SortBuffer when pressure is Hard.
- `crates/heimdall-build/src/pack.rs` — drop SortBuffer; add ordering
  assertion.
- `crates/heimdall-build/src/pack_addr.rs` — same.
- `crates/heimdall-build/src/sort_buffer.rs` — possibly factor the
  sort path into something `enrich` can call too. Otherwise unchanged.
- New: `crates/heimdall-build/benches/pack_pre_sort.rs` — micro-bench
  comparing pack with/without pre-sort on a sample country.

### Risks

- **Silent FST regression.** A subtle key-encoding mismatch between
  enrich and pack would produce different FST bytes. Mitigation: §5.3
  parity check.
- **Memory pressure migrates from pack to enrich.** Enrich already
  loads everything in RAM for admin assignment, so the marginal cost
  is just the sort. Verify on potato that DK + LU + DE still complete
  within the 1 GB cap.
- **Phase 4 invalidation interaction.** Pre-sorting changes the parquet
  output of enrich; if a Phase-4 sentinel from before the upgrade
  records the parquet size, the next run sees different bytes and
  invalidates. Acceptable — happens once on upgrade.

### Acceptance

- Pack wall time drops measurably on a real country (DK or DE: -30%
  target).
- Output FST is byte-identical to the prior path on DK + LU + DE
  (verified by §5.3 parity check).
- `--budget potato` build of DK still completes within the 1 GB cap.
- New micro-benchmark in `crates/heimdall-build/benches/`.

---

## Open questions / decisions needed

These are flagged for whoever picks this up, NOT decided yet:

1. **Sort buffer encoding**: bincode (faster) vs. postcard (more compact)?
   Default to postcard for smaller spill files; benchmark on US data.

2. **Scratch dir default**: `$TMPDIR` (often tmpfs, RAM-backed → defeats
   the purpose) or `data/scratch/`? Probably the latter, but make it
   easy to override.

3. ~~**Pre-sort during extract**~~ → resolved as **Phase 5** (sort during
   enrich, not extract — extract doesn't know admin IDs). See "Phase 5"
   above.

4. **Memory observer cadence**: 1 s polling burns CPU for nothing on
   short builds. Maybe 1 s default but bump to 5 s after the first
   minute? Or scale by build duration estimate.

5. **Tiered FST merge for the country pack**: deferred from Phase 2 to
   Phase 2.5 if bench numbers say it's worth it. The k-way streaming
   merge inside SortBuffer might be enough alone.

6. **Adaptive parallelism in extract**: extract is currently single-
   threaded (parses PBF sequentially). Parallelisation is its own
   project; out of scope.

7. **Checkpoint file format**: JSON for human-readability, or postcard
   for compactness? JSON; checkpoints are tiny.

8. **What to do when `--budget potato` user runs `--country planet`**?
   The whole-planet build genuinely needs ~50 GB peak even with all
   optimisations. Pre-flight should refuse with a clear "use a bigger
   machine" message that lists which countries fit and which don't.

---

## Test infrastructure (build alongside Phase 1)

- `crates/heimdall-build/tests/budget_potato.sh` — shell script that
  spins up a Docker container with cgroups limited to 1 GB RAM + 10 GB
  disk, runs `rebuild --country dk --budget potato`, asserts success +
  resource bounds.
- Same for `--country se` (medium), `--country fr` (large), `--country us`
  (huge).
- CI matrix: run the potato test on every PR that touches `rebuild.rs`,
  `pack.rs`, `pack_addr.rs`, `sort_buffer.rs`, or `resource_monitor.rs`.

This gives us a regression bar: "you broke the potato build" is a clear
failure signal.

---

## Quick reference — key code locations as of May 2026

```
crates/heimdall-build/src/
  main.rs                  — CLI (rebuild subcommand at :487, repack at :633)
  rebuild.rs               — pipeline orchestrator (per-country loop, cleanup at :859)
  extract.rs               — OSM PBF extraction; uses SortedFileNodeCache today
  enrich.rs                — admin hierarchy + admin_map.bin
  pack.rs                  — places FST (sort at :974)
  pack_addr.rs             — addresses FST (sort at :259, MapBuilder at :273)
  oa.rs                    — reference: streaming-with-delete pattern (US OA, ~600 lines)
  photon.rs                — reference: streaming JSON parse
  tiger.rs                 — TIGER importer (now with COUSUB/AIANNH/Gazetteer/HUD)
  hud.rs                   — simplemaps US ZIPs crosswalk
  gnis.rs                  — USGS Geographic Names

crates/heimdall-core/src/
  node_cache.rs            — NodeCache trait + 4 implementations including SortedFileNodeCache
                             (the proven external-merge-sort pattern to replicate in pack)
  index.rs                 — query-time index reader
  addr_index.rs            — address index reader
  global_index.rs          — global FST reader

data/sources.toml          — country definitions, ram_gb hints
data/rebuild-state.json    — gitignored, change-detection state (ETags etc.)
TODO_US.md                 — US-specific data-source TODOs (NAD, NYC PLUTO, etc.)
TODO_REBUILD_MODES.md      — this file
```

---

## Recommended starting point for the next chat

```
Context: Read CLAUDE.md, then TODO_REBUILD_MODES.md.
Goal:    Ship Phase 5 (pre-sort upstream of pack) as a single PR.
Steps:
  1. Read enrich.rs end-to-end to understand the in-memory data flow
     and where places/addresses live by the time admin is assigned.
  2. Read pack.rs and pack_addr.rs around the SortBuffer integration
     to see exactly what key encoding pack uses today (must match).
  3. Sketch the FST key encoding once and reuse the same function in
     both enrich (sort) and pack (assertion).
  4. Implement §5.1: sort + rewrite places.parquet and addresses.parquet
     at the end of enrich(). Use SortBuffer iff pressure is Hard.
  5. Implement §5.2: drop SortBuffer from pack; add the ordering
     assertion.
  6. Implement §5.3: byte-for-byte parity check against the pre-Phase-5
     path on DK. Land that test FIRST so the rest is just "make it pass".
  7. Bench on DE and US; record before/after numbers in the PR.
  8. Open PR, request review.

Earlier-phase pickup notes:
  * Phase 1, 2, 3 are merged on main (PRs #9 + #10).
  * Phase 4 is on the current branch — see "Phase 4 — shipped" section
    above. Don't re-implement; build on top.
```

---

*Last updated: May 2026 (Phase 4 implementation session). Status:
Phases 1-3 merged, Phase 4 implemented this branch, Phase 5 designed
but not started. Phase 5 estimated at 3-5 days.*
