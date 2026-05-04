//! External merge sort with bounded RAM, used by pack.rs / pack_addr.rs to
//! feed the FST builder in lex-sorted order without materialising every
//! `(key, value)` pair in memory.
//!
//! Why this exists
//! ───────────────
//! The `fst` crate's `MapBuilder` requires inserts in strictly increasing
//! lexicographic order. Today both pack sites collect every key+value into a
//! `Vec` and call `.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()))`. For
//! the United States that's ~30M `(String, u32)` tuples ≈ 1.5 GB just for
//! the keys, before any FST overhead. On a 1 GB-RAM "potato" laptop this
//! OOMs immediately.
//!
//! `SortBuffer` mirrors the proven pattern from
//! `heimdall_core::node_cache::SortedFileNodeCache` (which the OSM extract
//! already relies on for ~500 MB ceiling on the planet build). The
//! algorithm is the textbook external merge sort:
//!
//!   1. Push pairs into an in-memory `Vec` until the encoded bytes exceed
//!      `mem_limit`. (Encoded bytes ≈ what the spill file will weigh; we
//!      use that as the budget unit instead of guessing struct sizes.)
//!   2. Sort the batch by raw key bytes, then write a "run" file:
//!      `<u32 LE key_len><key bytes><u32 LE val_len><postcard value>` per
//!      entry.
//!   3. Repeat until the input is exhausted, then `finish()` returns a
//!      streaming iterator that k-way-merges the run files via a min-heap.
//!      A `Reverse<(key, run_idx)>` heap pops the smallest-key entry; the
//!      caller deduplicates inline (we don't bake dedup in because pack.rs
//!      and pack_addr.rs need different merge policies).
//!
//! The fast path: if the input fits in `mem_limit` and never spills, we
//! sort the in-memory `Vec` and stream from it directly — no disk I/O,
//! no encoding/decoding overhead. Small countries (DK, FI, NZ) stay
//! exactly as fast as the pre-Phase-2 code.
//!
//! Encoding choice: postcard per Open Question #1 in TODO_REBUILD_MODES.md
//! ("more compact than bincode, fast enough"). The key is written raw
//! (not postcard-encoded) so the heap can compare keys directly without
//! decoding values.
//!
//! Scratch dir: caller picks. The rebuild pipeline points it at
//! `data/scratch/` rather than `$TMPDIR` because `$TMPDIR` is often
//! tmpfs-backed (in-RAM) on Linux servers, which would defeat the
//! whole point of spilling to disk.

use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::resource_monitor::PressureSignal;

// ───────────────────────────────────────────────────────────────────────────
// Pack-time options (shared by pack.rs and pack_addr.rs)
// ───────────────────────────────────────────────────────────────────────────

/// Knobs that control how pack.rs / pack_addr.rs build their FSTs.
/// Plumbed from the `--sort-mem` and `--scratch-dir` CLI flags via
/// `rebuild.rs` and `main.rs`. Both call sites accept this struct so a
/// single `--budget potato` flow (Phase 3) can flip the default once
/// without re-touching every pack signature.
#[derive(Clone, Debug)]
pub struct PackOptions {
    /// Soft RAM ceiling for one SortBuffer batch before it spills.
    /// Default 256 MB. Tested practical floor: ~32 MB (anything smaller
    /// thrashes the heap merge).
    pub sort_mem: usize,
    /// Where spill files go. Default: `<index_dir>/.scratch/` (same
    /// filesystem as the FST output, no cross-device copies, clearly
    /// scoped per-country so concurrent builds don't fight). Override
    /// with `--scratch-dir` if you want a faster disk (NVMe scratch
    /// for a network-mounted index dir).
    pub scratch_dir: PathBuf,
    /// Optional global RSS pressure signal (Phase 3). Defaults to the
    /// `disabled()` no-op: present so call sites can pass it
    /// unconditionally without paying for the Arc when no monitor is
    /// running.
    pub pressure: PressureSignal,
}

impl PackOptions {
    /// Sensible default for an index dir: 256 MB sort budget, scratch
    /// at `<index_dir>/.scratch/`, no pressure monitoring.
    pub fn default_for(index_dir: &Path) -> Self {
        Self {
            sort_mem: 256 * 1024 * 1024,
            scratch_dir: index_dir.join(".scratch"),
            pressure: PressureSignal::disabled(),
        }
    }

    pub fn with_pressure(mut self, signal: PressureSignal) -> Self {
        self.pressure = signal;
        self
    }
}

/// External merge sort buffer. Generic over value type V; keys are raw
/// bytes (what the FST builder ultimately wants).
pub struct SortBuffer<V> {
    mem_limit: usize,
    scratch_dir: PathBuf,
    /// Unique nonce per buffer instance, used to namespace spill files so
    /// concurrent SortBuffer instances (parallel pack steps) don't collide.
    nonce: String,
    /// Paths of spilled run files. Empty until the first spill.
    runs: Vec<PathBuf>,
    /// In-memory batch waiting to be sorted/spilled.
    current: Vec<(Vec<u8>, V)>,
    /// Approximate encoded byte cost of `current`. Used to decide spill.
    current_bytes: usize,
    /// Tally of pushed pairs (informational, exposed by `len`).
    total_pushed: u64,
    /// Tally of bytes spilled to disk so far (informational).
    total_spilled_bytes: u64,
    /// Optional pressure-aware early-spill signal (Phase 3). When the
    /// global RSS observer reports `Hard` pressure (>90% of budget), the
    /// next push spills regardless of `mem_limit`. Defaults to disabled
    /// (always-`None` pressure) so existing call sites and tests behave
    /// identically.
    pressure: PressureSignal,
    /// Number of times we spilled early because of `Hard` pressure rather
    /// than because the in-memory batch hit `mem_limit`. Exposed via
    /// `pressure_spills()` for build-report logging.
    pressure_spills: u64,
    _phantom: PhantomData<V>,
}

impl<V> SortBuffer<V>
where
    V: Serialize + DeserializeOwned,
{
    /// Create a new buffer. `mem_limit` is the soft byte ceiling for the
    /// in-memory batch — when crossed, the next `push` spills.
    /// `scratch_dir` must exist (we create it if it doesn't).
    pub fn new(mem_limit: usize, scratch_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(scratch_dir)
            .with_context(|| format!("creating scratch dir {}", scratch_dir.display()))?;
        // PID + monotonic-ish nanos for uniqueness across concurrent
        // pack-places / pack-addr threads. Not security-critical — just
        // needs to avoid filename collisions in the same scratch dir.
        let nonce = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        Ok(Self {
            // No floor: callers (rebuild.rs) pass a sensible default
            // (256 MB); tests deliberately set tiny values to exercise
            // the spill path.
            mem_limit,
            scratch_dir: scratch_dir.to_path_buf(),
            nonce,
            runs: Vec::new(),
            current: Vec::new(),
            current_bytes: 0,
            total_pushed: 0,
            total_spilled_bytes: 0,
            pressure: PressureSignal::disabled(),
            pressure_spills: 0,
            _phantom: PhantomData,
        })
    }

    /// Attach a global RSS pressure signal. When the monitor reports `Hard`
    /// pressure, the next `push` will force-spill instead of letting the
    /// in-memory batch grow further. Idempotent — call once per buffer.
    pub fn with_pressure(mut self, signal: PressureSignal) -> Self {
        self.pressure = signal;
        self
    }

    /// How many times we spilled early because of `Hard` pressure rather
    /// than the local `mem_limit`. Useful for build reports.
    pub fn pressure_spills(&self) -> u64 {
        self.pressure_spills
    }

    /// Add one `(key, value)` to the buffer. Spills the in-memory batch to
    /// a sorted run file when the byte ceiling is crossed *or* when the
    /// resource monitor signals `Hard` pressure.
    pub fn push(&mut self, key: Vec<u8>, value: V) -> Result<()> {
        // Approximate per-pair byte cost: 4 (key_len) + key + 4 (val_len)
        // + sizeof(V) (the postcard encoding will be smaller for compound
        // types, but this is a defensive upper bound).
        let approx = 8 + key.len() + std::mem::size_of::<V>();
        self.current_bytes += approx;
        self.current.push((key, value));
        self.total_pushed += 1;

        // Local-budget spill: the existing Phase 2 trigger.
        if self.current_bytes >= self.mem_limit {
            self.spill_current()?;
            return Ok(());
        }

        // Global-pressure spill: Phase 3. Only check on a coarse cadence
        // (every 4096 pushes) so the atomic load doesn't show up in the
        // hot path. Even at 1 µs per check this would cost real time on a
        // 30M-row US pack — the rebuild loop pushes ~3M keys/s.
        //
        // We also require the batch to have at least *some* content
        // (`current_bytes > 1 MB`) before spilling, otherwise a sustained
        // Hard signal could spawn thousands of tiny run files which then
        // make the k-way merge heap thrash worse than the original RAM
        // pressure. The 1 MB floor is empirical: matches the BufWriter's
        // capacity so each spill produces a tidy run file.
        if self.total_pushed % 4096 == 0
            && self.current_bytes >= 1024 * 1024
            && self.pressure.is_hard()
        {
            self.pressure_spills += 1;
            self.spill_current()?;
        }
        Ok(())
    }

    /// Sort and write out the in-memory batch to a new run file, then
    /// reset the in-memory buffer.
    fn spill_current(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }
        // Stable sort by raw key bytes — same order as the FST builder
        // expects later. Stable so that when we *don't* spill (fast
        // path), order between equal keys mirrors push order, matching
        // the pre-Phase-2 in-memory `Vec::sort_by` behaviour.
        self.current.sort_by(|a, b| a.0.cmp(&b.0));

        let path = self.scratch_dir.join(format!(
            "sortbuf-{}-{:04}.run",
            self.nonce,
            self.runs.len(),
        ));
        let file = File::create(&path)
            .with_context(|| format!("creating spill file {}", path.display()))?;
        let mut w = BufWriter::with_capacity(1024 * 1024, file);
        let mut bytes_out: u64 = 0;
        for (k, v) in self.current.drain(..) {
            let val_bytes = postcard::to_allocvec(&v)
                .context("postcard encoding spill value")?;
            let kl = k.len() as u32;
            let vl = val_bytes.len() as u32;
            w.write_all(&kl.to_le_bytes())?;
            w.write_all(&k)?;
            w.write_all(&vl.to_le_bytes())?;
            w.write_all(&val_bytes)?;
            bytes_out += 8 + kl as u64 + vl as u64;
        }
        w.flush()?;
        drop(w);

        self.total_spilled_bytes += bytes_out;
        self.current_bytes = 0;
        self.runs.push(path);
        Ok(())
    }

    /// Number of pairs pushed so far (regardless of whether they're in
    /// RAM or already spilled).
    pub fn len(&self) -> u64 {
        self.total_pushed
    }

    /// True if no pairs have been pushed.
    pub fn is_empty(&self) -> bool {
        self.total_pushed == 0
    }

    /// Number of run files spilled. Useful for tests / logging
    /// ("spilled into N runs of M MB each").
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    /// Total bytes written to disk for spills. Useful for diagnostics
    /// like "external sort wrote X GB to scratch".
    pub fn spilled_bytes(&self) -> u64 {
        self.total_spilled_bytes
    }

    /// Consume the buffer and return a streaming iterator over all
    /// (key, value) pairs in sorted-by-key order.
    ///
    /// Fast path: if no spill ever happened, sort the in-memory Vec and
    /// stream from it. No disk I/O, no encoding overhead — small
    /// countries pay nothing for the new abstraction.
    ///
    /// Slow path: spill the residual in-memory batch, then k-way merge
    /// all runs via a min-heap.
    pub fn finish(mut self) -> Result<MergedIter<V>> {
        if self.runs.is_empty() {
            // Pure-RAM path. Sort and hand back an iterator over the Vec.
            // Use std::mem::take to move the Vec out of `self` despite
            // Drop being implemented (otherwise Rust forbids the move).
            self.current.sort_by(|a, b| a.0.cmp(&b.0));
            let owned = std::mem::take(&mut self.current);
            return Ok(MergedIter {
                kind: MergedIterKind::InMemory(owned.into_iter()),
                _scratch_owned: Vec::new(),
                run_count: 0,
                spilled_bytes: 0,
            });
        }
        // Mixed path: flush the residual batch.
        self.spill_current()?;

        // Open every run, prime the heap with the first key from each.
        let mut readers: Vec<RunReader<V>> = Vec::with_capacity(self.runs.len());
        let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
        for (idx, path) in self.runs.iter().enumerate() {
            let mut reader = RunReader::<V>::open(path)?;
            if let Some(first_key) = reader.peek_key()? {
                heap.push(Reverse((first_key.to_vec(), idx)));
            }
            readers.push(reader);
        }

        let run_count = self.runs.len();
        let spilled_bytes = self.total_spilled_bytes;
        Ok(MergedIter {
            kind: MergedIterKind::Spilled(SpilledMerge { readers, heap }),
            _scratch_owned: std::mem::take(&mut self.runs),
            run_count,
            spilled_bytes,
        })
    }
}

impl<V> Drop for SortBuffer<V> {
    fn drop(&mut self) {
        // Defensive: if `finish()` was never called (e.g. early error
        // return from the caller), still tidy the spill files. Once
        // `finish()` returns, ownership of the run paths transfers to
        // MergedIter which cleans them on its own drop.
        for p in &self.runs {
            let _ = std::fs::remove_file(p);
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Run reader
// ───────────────────────────────────────────────────────────────────────────

struct RunReader<V> {
    inner: BufReader<File>,
    /// One-entry lookahead: the next decoded pair, decoded eagerly so the
    /// heap can compare keys without re-reading. None at EOF.
    next: Option<(Vec<u8>, V)>,
    _phantom: PhantomData<V>,
}

impl<V> RunReader<V>
where
    V: DeserializeOwned,
{
    fn open(path: &Path) -> Result<Self> {
        let f = File::open(path)
            .with_context(|| format!("opening spill file {}", path.display()))?;
        let mut r = Self {
            inner: BufReader::with_capacity(1024 * 1024, f),
            next: None,
            _phantom: PhantomData,
        };
        r.advance()?;
        Ok(r)
    }

    /// Read one record from disk into `self.next`. Sets `next = None` at EOF.
    fn advance(&mut self) -> Result<()> {
        let mut len_buf = [0u8; 4];
        match self.inner.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.next = None;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
        let key_len = u32::from_le_bytes(len_buf) as usize;
        let mut key = vec![0u8; key_len];
        self.inner.read_exact(&mut key)?;

        self.inner.read_exact(&mut len_buf)?;
        let val_len = u32::from_le_bytes(len_buf) as usize;
        let mut val_bytes = vec![0u8; val_len];
        self.inner.read_exact(&mut val_bytes)?;
        let value: V = postcard::from_bytes(&val_bytes)
            .context("postcard decoding spill value")?;
        self.next = Some((key, value));
        Ok(())
    }

    fn peek_key(&mut self) -> Result<Option<&[u8]>> {
        Ok(self.next.as_ref().map(|(k, _)| k.as_slice()))
    }

    /// Take the current head, advance to the next entry.
    fn pop(&mut self) -> Result<Option<(Vec<u8>, V)>> {
        let out = self.next.take();
        self.advance()?;
        Ok(out)
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Merged iterator
// ───────────────────────────────────────────────────────────────────────────

/// Iterator returned by `SortBuffer::finish`. Holds either an in-memory
/// drain (fast path) or a k-way merge across spill files (slow path).
pub struct MergedIter<V> {
    kind: MergedIterKind<V>,
    /// Run files we own. Deleted when the iterator drops.
    _scratch_owned: Vec<PathBuf>,
    /// Final run count (after `finish()` flushed any residual batch).
    /// Exposed so callers can log "spilled into N runs" without having
    /// to query the SortBuffer before its consuming `finish()` call.
    run_count: usize,
    /// Final spilled-bytes total (after the residual flush).
    spilled_bytes: u64,
}

impl<V> MergedIter<V> {
    /// Number of run files this iterator merges across. 0 means the
    /// data fit entirely in RAM and no disk I/O occurred.
    pub fn run_count(&self) -> usize { self.run_count }
    /// Total bytes spilled to scratch dir during the sort. 0 in the
    /// in-RAM fast path.
    pub fn spilled_bytes(&self) -> u64 { self.spilled_bytes }
}

enum MergedIterKind<V> {
    InMemory(std::vec::IntoIter<(Vec<u8>, V)>),
    Spilled(SpilledMerge<V>),
}

struct SpilledMerge<V> {
    readers: Vec<RunReader<V>>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
}

impl<V> Iterator for MergedIter<V>
where
    V: DeserializeOwned,
{
    type Item = Result<(Vec<u8>, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.kind {
            MergedIterKind::InMemory(it) => it.next().map(Ok),
            MergedIterKind::Spilled(merge) => {
                let Reverse((_, run_idx)) = merge.heap.pop()?;
                let pair = match merge.readers[run_idx].pop() {
                    Ok(Some(p)) => p,
                    Ok(None) => {
                        // Heap said this run had something but the reader
                        // disagreed — would mean we've drifted out of
                        // sync with the lookahead. Surface as an error
                        // rather than silently dropping data.
                        return Some(Err(anyhow::anyhow!(
                            "sort_buffer internal: run {} unexpectedly empty",
                            run_idx,
                        )));
                    }
                    Err(e) => return Some(Err(e)),
                };
                // Re-prime the heap with this run's next key, if any.
                match merge.readers[run_idx].peek_key() {
                    Ok(Some(next_key)) => {
                        merge.heap.push(Reverse((next_key.to_vec(), run_idx)));
                    }
                    Ok(None) => { /* run exhausted */ }
                    Err(e) => return Some(Err(e)),
                }
                Some(Ok(pair))
            }
        }
    }
}

impl<V> Drop for MergedIter<V> {
    fn drop(&mut self) {
        for p in &self._scratch_owned {
            let _ = std::fs::remove_file(p);
        }
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Pure-RAM path: tiny mem_limit but only a handful of items so we
    /// never spill. Output must match an in-memory `sort_by(key)`.
    #[test]
    fn in_memory_sort_matches_vec_sort() {
        let dir = tempfile::tempdir().unwrap();
        let mut buf = SortBuffer::<u32>::new(64 * 1024 * 1024, dir.path()).unwrap();
        let inputs: Vec<(&[u8], u32)> = vec![
            (b"banana", 1),
            (b"apple", 2),
            (b"cherry", 3),
            (b"apricot", 4),
        ];
        for (k, v) in &inputs {
            buf.push(k.to_vec(), *v).unwrap();
        }
        assert_eq!(buf.run_count(), 0, "must stay in memory");
        let out: Vec<(Vec<u8>, u32)> =
            buf.finish().unwrap().map(|r| r.unwrap()).collect();
        let keys: Vec<&[u8]> = out.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![&b"apple"[..], b"apricot", b"banana", b"cherry"]);
    }

    /// Spill path: tiny mem_limit forces multiple spills, then merge.
    /// Output must be globally sorted by key.
    #[test]
    fn spill_path_produces_globally_sorted_output() {
        let dir = tempfile::tempdir().unwrap();
        // 1 KB ceiling → spills aggressively even for ~50 entries.
        let mut buf = SortBuffer::<u32>::new(1024, dir.path()).unwrap();

        // Deterministic shuffled sequence of 500 distinct keys.
        let mut keys: Vec<u32> = (0..500u32).collect();
        // Reverse + interleave to force out-of-order pushes.
        keys.reverse();
        let mut shuffled = Vec::new();
        let mid = keys.len() / 2;
        for i in 0..mid {
            shuffled.push(keys[i]);
            shuffled.push(keys[mid + i]);
        }

        for &k in &shuffled {
            // Use big-endian so lex order matches numeric order.
            buf.push(k.to_be_bytes().to_vec(), k).unwrap();
        }
        assert!(buf.run_count() >= 2, "expected at least two spills");

        let out: Vec<(Vec<u8>, u32)> =
            buf.finish().unwrap().map(|r| r.unwrap()).collect();
        let nums: Vec<u32> = out.iter().map(|(_, v)| *v).collect();
        let mut expected: Vec<u32> = (0..500u32).collect();
        expected.sort();
        assert_eq!(nums, expected);
    }

    /// Compound value type — mirrors pack.rs:972 `(u32, u16, bool)`.
    #[test]
    fn compound_value_round_trips_through_spill() {
        let dir = tempfile::tempdir().unwrap();
        let mut buf =
            SortBuffer::<(u32, u16, bool)>::new(512, dir.path()).unwrap();

        let mut expected: HashMap<Vec<u8>, (u32, u16, bool)> = HashMap::new();
        for i in 0..200u32 {
            let key = format!("key-{:05}", i).into_bytes();
            let val = (i, (i % 1000) as u16, i % 2 == 0);
            expected.insert(key.clone(), val);
            buf.push(key, val).unwrap();
        }
        assert!(buf.run_count() > 0, "expected spills with 512-byte ceiling");

        let mut prev: Option<Vec<u8>> = None;
        let mut seen = 0;
        for r in buf.finish().unwrap() {
            let (k, v) = r.unwrap();
            if let Some(ref p) = prev {
                assert!(p.as_slice() <= k.as_slice(), "merge produced out-of-order keys");
            }
            assert_eq!(expected[&k], v, "value corrupted across spill");
            prev = Some(k);
            seen += 1;
        }
        assert_eq!(seen, 200);
    }

    /// Duplicate keys are returned to the caller; SortBuffer does NOT
    /// dedup. (pack.rs and pack_addr.rs apply different dedup policies.)
    #[test]
    fn duplicate_keys_pass_through_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut buf = SortBuffer::<u32>::new(64 * 1024 * 1024, dir.path()).unwrap();
        buf.push(b"k".to_vec(), 1).unwrap();
        buf.push(b"k".to_vec(), 2).unwrap();
        buf.push(b"k".to_vec(), 3).unwrap();
        let out: Vec<u32> = buf
            .finish()
            .unwrap()
            .map(|r| r.unwrap().1)
            .collect();
        assert_eq!(out.len(), 3);
        // Stable sort means push order is preserved within equal keys.
        assert_eq!(out, vec![1, 2, 3]);
    }

    /// Spill files must be removed when the iterator is dropped.
    #[test]
    fn spill_files_cleaned_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().to_path_buf();
        {
            let mut buf = SortBuffer::<u32>::new(128, &scratch).unwrap();
            for i in 0..200u32 {
                buf.push(format!("{:05}", i).into_bytes(), i).unwrap();
            }
            assert!(buf.run_count() > 0);
            // Drain fully — moves ownership of run paths into the iterator.
            let _: Vec<_> = buf.finish().unwrap().collect();
        }
        // After the iterator drops, no .run files should remain.
        let leftovers: Vec<_> = std::fs::read_dir(&scratch)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("run"))
            .collect();
        assert!(leftovers.is_empty(), "found leftover spill files: {:?}", leftovers);
    }

    /// Empty buffer finishes cleanly with no output.
    #[test]
    fn empty_finish_returns_empty_iterator() {
        let dir = tempfile::tempdir().unwrap();
        let buf = SortBuffer::<u32>::new(1024, dir.path()).unwrap();
        let out: Vec<_> = buf.finish().unwrap().map(|r| r.unwrap()).collect();
        assert!(out.is_empty());
    }

    /// Phase-2 acceptance: an FST built from SortBuffer-merged keys is
    /// byte-identical to one built from `Vec::sort_by + dedup_by`.
    /// This is the contract pack_addr.rs relies on — operationally, the
    /// served `fst_addr.fst` must be unchanged across the migration.
    #[test]
    fn fst_built_from_sort_buffer_matches_in_memory_sort_byte_for_byte() {
        use fst::MapBuilder;

        // 1000 inputs with a healthy duplicate rate (street appearing
        // under multiple muni_id keys collides on the wildcard ":0" key).
        let mut inputs: Vec<(Vec<u8>, u64)> = Vec::new();
        for i in 0..1000u32 {
            let muni = i % 50;          // 50 munis
            let street = format!("street-{:04}", i % 200); // 200 distinct streets
            inputs.push((format!("{}:{}", street, muni).into_bytes(), i as u64));
            inputs.push((format!("{}:0", street).into_bytes(), i as u64)); // wildcard
        }

        // Old path: in-memory sort + dedup-keep-first → MapBuilder.
        let in_mem_path = {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("inmem.fst");
            let mut keys = inputs.clone();
            keys.sort_by(|a, b| a.0.cmp(&b.0));
            keys.dedup_by(|a, b| a.0 == b.0);
            let f = std::fs::File::create(&path).unwrap();
            let mut b = MapBuilder::new(std::io::BufWriter::new(f)).unwrap();
            for (k, v) in &keys {
                b.insert(k, *v).unwrap();
            }
            b.finish().unwrap();
            // Move the FST out of the temp dir so we can compare after
            // the dir drops.
            let target = std::env::temp_dir()
                .join(format!("inmem-{}.fst", std::process::id()));
            std::fs::copy(&path, &target).unwrap();
            target
        };

        // New path: SortBuffer with a tiny mem_limit (forces spills) +
        // streaming dedup → MapBuilder.
        let sb_path = {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("sb.fst");
            let mut buf = SortBuffer::<u64>::new(2048, dir.path()).unwrap();
            for (k, v) in &inputs {
                buf.push(k.clone(), *v).unwrap();
            }
            let f = std::fs::File::create(&path).unwrap();
            let mut b = MapBuilder::new(std::io::BufWriter::new(f)).unwrap();
            let mut prev: Option<Vec<u8>> = None;
            for entry in buf.finish().unwrap() {
                let (k, v) = entry.unwrap();
                if prev.as_deref() == Some(k.as_slice()) {
                    continue;
                }
                b.insert(&k, v).unwrap();
                prev = Some(k);
            }
            b.finish().unwrap();
            let target = std::env::temp_dir()
                .join(format!("sb-{}.fst", std::process::id()));
            std::fs::copy(&path, &target).unwrap();
            target
        };

        let a = std::fs::read(&in_mem_path).unwrap();
        let b = std::fs::read(&sb_path).unwrap();
        std::fs::remove_file(&in_mem_path).ok();
        std::fs::remove_file(&sb_path).ok();
        assert_eq!(
            a, b,
            "SortBuffer-built FST diverges from in-memory FST ({} vs {} bytes)",
            a.len(), b.len(),
        );
    }
}
