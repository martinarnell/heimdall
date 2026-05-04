//! Step-level checkpoints (TODO_REBUILD_MODES Phases 3 + 4).
//!
//! After each rebuild step succeeds we drop a `<phase>.done` file in
//! `<index>/checkpoints/`. On restart, the planner consults these to decide
//! whether the step needs to run again.
//!
//! Phase 3 added the sentinels (resume after crash). Phase 4 adds the
//! **input fingerprint** stored on each sentinel: the planner now diffs
//! the current source state (PBF sequence number, Photon md5, national
//! ETag, …) against what the prior run consumed, and re-runs the phase
//! if anything moved.
//!
//! ### Decision rules
//!
//! For phase X with current input fingerprint `cur`:
//!
//! 1. No sentinel on disk → `Run("no checkpoint")`.
//! 2. Sentinel exists but a recorded output is missing or zero-length →
//!    `Run("output missing: <name>")`. Catches the `--cleanup` case where
//!    intermediate parquets were deleted between runs.
//! 3. Sentinel exists and is **Phase-3-format** (no `_format` marker in
//!    inputs) → `Skip`. We trust the legacy sentinel rather than forcing a
//!    full rebuild on first upgrade to Phase 4.
//! 4. Sentinel exists and is Phase-4-format → strict diff. Any key whose
//!    value changed (or appeared/disappeared) returns `Run("inputs changed:
//!    <key> <old>→<new>")`.
//! 5. Cascade: callers track whether any earlier phase decided `Run`. If
//!    so, the current phase is forced `Run("cascade")` regardless of its
//!    own diff. The cascade is linear because all merge phases mutate
//!    shared parquet files; safe re-runs require restarting from extract
//!    if extract changed, etc.
//!
//! ### Format marker
//!
//! Phase 4 sentinels always include `inputs["_format"] = "v4"`. This lets
//! the planner distinguish a freshly-written empty-inputs sentinel (some
//! phases like enrich legitimately have no external inputs to fingerprint)
//! from a legacy Phase-3 sentinel (`inputs` field absent or empty).
//!
//! ### Other invariants
//!
//! * `--cleanup` and `--keep-intermediates=0` delete the parquet files that
//!   most checkpoints record as outputs. When the planner notices a missing
//!   output, the checkpoint is considered stale → the step re-runs.
//! * `meta.json` lives next to the checkpoints dir and remains the source
//!   of truth for "this index is servable". Checkpoints don't replace it.
//! * On any error mid-step, no `.done` is written → next run re-tries.
//!
//! Format intentionally JSON: tiny files (a few hundred bytes), human
//! debuggable, and the `_format` marker makes future schema bumps cheap.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    Extract,
    National,
    PlacesSource,
    Photon,
    Enrich,
    Pack,
}

impl Phase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Phase::Extract => "extract",
            Phase::National => "national",
            Phase::PlacesSource => "places_source",
            Phase::Photon => "photon",
            Phase::Enrich => "enrich",
            Phase::Pack => "pack",
        }
    }
}

/// Sentinel format marker. Bumped if the inputs schema changes in a
/// breaking way (extra mandatory keys, etc.). Stored under the special
/// key `_format` inside `inputs`.
pub const FORMAT_MARKER_KEY: &str = "_format";
pub const FORMAT_MARKER_VALUE: &str = "v4";

/// What the planner tells the runner about a phase. The reason is the
/// human-readable explanation surfaced by `--show-plan` and the per-step
/// log line ("[us] photon RUN: input changed photon.md5 a1b2…→c3d4…").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Run { reason: String },
    Skip,
}

impl Decision {
    pub fn is_run(&self) -> bool {
        matches!(self, Decision::Run { .. })
    }
    pub fn reason(&self) -> &str {
        match self {
            Decision::Run { reason } => reason,
            Decision::Skip => "checkpoint up to date",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Checkpoint {
    pub phase: String,
    pub started_at: u64,
    pub finished_at: u64,
    /// Filenames (relative to index dir) that this phase produced. The
    /// planner verifies they still exist before trusting the checkpoint.
    pub outputs: Vec<OutputEntry>,
    /// Phase 4: input fingerprints, keyed by stable identifier
    /// (`osm.sequence`, `photon.md5`, `national.etag`, …). Always carries
    /// `_format = v4` for Phase-4-written sentinels so legacy Phase-3
    /// sentinels (missing field or empty map) can be distinguished.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inputs: BTreeMap<String, String>,
    /// Free-form note from the step ("123456 places, 4567 addr").
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
}

impl Checkpoint {
    /// True iff this sentinel was written by Phase-4 code (carries the
    /// format marker). Older sentinels are `false` here and get the
    /// "trust outputs only" fallback in the planner.
    pub fn is_phase4_format(&self) -> bool {
        self.inputs.get(FORMAT_MARKER_KEY).map(|v| v.as_str()) == Some(FORMAT_MARKER_VALUE)
    }

    /// Inputs minus the format marker — what the user thinks of as "the
    /// fingerprint". Used for human-readable diff output.
    pub fn user_inputs(&self) -> BTreeMap<String, String> {
        self.inputs
            .iter()
            .filter(|(k, _)| k.as_str() != FORMAT_MARKER_KEY)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OutputEntry {
    pub name: String,
    pub size_bytes: u64,
}

pub struct PhaseTimer {
    phase: Phase,
    started_at: u64,
}

impl PhaseTimer {
    pub fn start(phase: Phase) -> Self {
        Self { phase, started_at: unix_now() }
    }

    /// Write the `<phase>.done` sentinel. `outputs` lists files (relative
    /// to `index_dir`) the phase produced — used by `is_done` to verify the
    /// checkpoint hasn't been invalidated by a later cleanup. `inputs` is
    /// the fingerprint of the sources this phase consumed; the planner
    /// diffs it against the next run's inputs.
    pub fn finish(
        self,
        index_dir: &Path,
        outputs: &[&str],
        inputs: BTreeMap<String, String>,
        summary: impl Into<String>,
    ) -> Result<()> {
        let dir = checkpoints_dir(index_dir);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating checkpoints dir {}", dir.display()))?;
        let entries: Vec<OutputEntry> = outputs.iter().map(|name| {
            let p = index_dir.join(name);
            let size = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            OutputEntry { name: (*name).into(), size_bytes: size }
        }).collect();
        // Always tag with the format marker so the planner can tell us
        // apart from Phase-3 sentinels (which had no marker).
        let mut full_inputs = inputs;
        full_inputs.insert(FORMAT_MARKER_KEY.into(), FORMAT_MARKER_VALUE.into());
        let ckpt = Checkpoint {
            phase: self.phase.as_str().into(),
            started_at: self.started_at,
            finished_at: unix_now(),
            outputs: entries,
            inputs: full_inputs,
            summary: summary.into(),
        };
        let path = sentinel_path(index_dir, self.phase);
        // Atomic replace — write to .tmp then rename. Avoids leaving a half-
        // written .done if the process is killed mid-write.
        let tmp = path.with_extension("tmp");
        let f = std::fs::File::create(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        serde_json::to_writer_pretty(f, &ckpt)
            .with_context(|| "serializing checkpoint")?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("renaming {} → {}", tmp.display(), path.display()))?;
        Ok(())
    }
}

pub fn checkpoints_dir(index_dir: &Path) -> PathBuf {
    index_dir.join("checkpoints")
}

pub fn sentinel_path(index_dir: &Path, phase: Phase) -> PathBuf {
    checkpoints_dir(index_dir).join(format!("{}.done", phase.as_str()))
}

/// True iff the sentinel exists AND every recorded output is still on disk
/// at non-zero size. Output-only check; ignores input fingerprints. Kept
/// as a public escape hatch for callers (and tests) that don't need the
/// full Phase-4 diff — production code paths use `diff` / `cascade_decide`.
#[allow(dead_code)]
pub fn is_done(index_dir: &Path, phase: Phase) -> bool {
    let Some(ckpt) = read(index_dir, phase) else { return false; };
    first_missing_output(index_dir, &ckpt).is_none()
}

/// Phase-4 planner: decide whether `phase` needs to run, given the
/// current input fingerprint. The reason inside `Decision::Run` is meant
/// for direct display in `--show-plan` and per-step logs.
///
/// Note: callers are responsible for the cascade (if any earlier phase
/// returns `Run`, this one is forced `Run` regardless). See `cascade_decide`.
pub fn diff(
    index_dir: &Path,
    phase: Phase,
    current_inputs: &BTreeMap<String, String>,
) -> Decision {
    let Some(prev) = read(index_dir, phase) else {
        return Decision::Run { reason: "no checkpoint".into() };
    };
    if let Some(missing) = first_missing_output(index_dir, &prev) {
        return Decision::Run { reason: format!("output missing: {}", missing) };
    }
    if !prev.is_phase4_format() {
        // Legacy Phase-3 sentinel: no fingerprint to diff against. Trust
        // the outputs-OK check we just passed and skip.
        return Decision::Skip;
    }
    if let Some(reason) = first_input_diff(&prev.user_inputs(), current_inputs) {
        return Decision::Run { reason: format!("input changed: {}", reason) };
    }
    Decision::Skip
}

/// Apply cascade: if `cascading` is already true (any upstream RUN), this
/// phase is forced RUN; otherwise we delegate to `diff`. Returns the
/// decision, plus the new cascade state for the caller to thread into the
/// next phase.
pub fn cascade_decide(
    index_dir: &Path,
    phase: Phase,
    current_inputs: &BTreeMap<String, String>,
    cascading: bool,
) -> (Decision, bool) {
    if cascading {
        return (Decision::Run { reason: "cascade from upstream".into() }, true);
    }
    let d = diff(index_dir, phase, current_inputs);
    let now_cascading = d.is_run();
    (d, now_cascading)
}

fn first_missing_output(index_dir: &Path, ckpt: &Checkpoint) -> Option<String> {
    for o in &ckpt.outputs {
        let p = index_dir.join(&o.name);
        match std::fs::metadata(&p) {
            Ok(m) if m.len() > 0 => continue,
            _ => return Some(o.name.clone()),
        }
    }
    None
}

/// First key whose value differs (added, removed, or changed). Returns a
/// human-readable "key: old→new" string for log output, or `None` if the
/// two maps are equal.
fn first_input_diff(
    prev: &BTreeMap<String, String>,
    cur: &BTreeMap<String, String>,
) -> Option<String> {
    // Check current ⊇ prev: any added/changed key.
    for (k, v) in cur {
        match prev.get(k) {
            Some(pv) if pv == v => {}
            Some(pv) => return Some(format!("{} {}→{}", k, truncate(pv), truncate(v))),
            None => return Some(format!("{} (added: {})", k, truncate(v))),
        }
    }
    // Check prev ⊇ cur: any removed key.
    for (k, v) in prev {
        if !cur.contains_key(k) {
            return Some(format!("{} (removed: {})", k, truncate(v)));
        }
    }
    None
}

/// Trim long fingerprint values for log output. Keeps short etags
/// readable and clips multi-hundred-char zone manifests to a stub.
fn truncate(s: &str) -> String {
    if s.len() <= 32 {
        s.to_string()
    } else {
        format!("{}…", &s[..30])
    }
}

pub fn read(index_dir: &Path, phase: Phase) -> Option<Checkpoint> {
    let f = std::fs::File::open(sentinel_path(index_dir, phase)).ok()?;
    serde_json::from_reader(f).ok()
}

/// Wipe the entire `checkpoints/` directory. Public escape hatch (handy
/// for tests and ops scripts) — the rebuild pipeline uses `clear_phase`
/// to honour selective `--force=phase1,phase2` invalidation.
#[allow(dead_code)]
pub fn clear_all(index_dir: &Path) -> Result<()> {
    let dir = checkpoints_dir(index_dir);
    if dir.exists() {
        std::fs::remove_dir_all(&dir)
            .with_context(|| format!("clearing {}", dir.display()))?;
    }
    Ok(())
}

/// Wipe a single phase's sentinel — `--force=pack` etc.
pub fn clear_phase(index_dir: &Path, phase: Phase) -> Result<()> {
    let p = sentinel_path(index_dir, phase);
    if p.exists() {
        std::fs::remove_file(&p)
            .with_context(|| format!("clearing {}", p.display()))?;
    }
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn inputs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect()
    }

    fn write_ckpt(idx: &Path, phase: Phase, outputs: &[&str], inp: BTreeMap<String, String>) {
        for o in outputs {
            std::fs::write(idx.join(o), b"x").unwrap();
        }
        PhaseTimer::start(phase).finish(idx, outputs, inp, "").unwrap();
    }

    #[test]
    fn write_then_is_done() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Extract, &["places.parquet"], inputs(&[]));
        assert!(is_done(idx, Phase::Extract));
        assert!(!is_done(idx, Phase::Pack));
    }

    #[test]
    fn missing_output_invalidates() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Extract, &["places.parquet"], inputs(&[]));
        assert!(is_done(idx, Phase::Extract));
        std::fs::remove_file(idx.join("places.parquet")).unwrap();
        assert!(!is_done(idx, Phase::Extract));
    }

    #[test]
    fn clear_all_drops_dir() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Pack, &["a"], inputs(&[]));
        assert!(is_done(idx, Phase::Pack));
        clear_all(idx).unwrap();
        assert!(!is_done(idx, Phase::Pack));
    }

    #[test]
    fn clear_phase_keeps_others() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Extract, &["a"], inputs(&[]));
        write_ckpt(idx, Phase::Pack, &["b"], inputs(&[]));
        assert!(is_done(idx, Phase::Extract));
        assert!(is_done(idx, Phase::Pack));
        clear_phase(idx, Phase::Pack).unwrap();
        assert!(is_done(idx, Phase::Extract));
        assert!(!is_done(idx, Phase::Pack));
    }

    #[test]
    fn diff_no_checkpoint_runs() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        let d = diff(idx, Phase::Extract, &inputs(&[("osm.sequence", "1")]));
        assert!(d.is_run());
        assert!(d.reason().contains("no checkpoint"));
    }

    #[test]
    fn diff_outputs_missing_runs() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Extract, &["p.parquet"], inputs(&[("osm.sequence", "1")]));
        std::fs::remove_file(idx.join("p.parquet")).unwrap();
        let d = diff(idx, Phase::Extract, &inputs(&[("osm.sequence", "1")]));
        assert!(d.is_run());
        assert!(d.reason().contains("output missing"));
    }

    #[test]
    fn diff_inputs_match_skips() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Photon, &["p"], inputs(&[("photon.md5", "abc")]));
        let d = diff(idx, Phase::Photon, &inputs(&[("photon.md5", "abc")]));
        assert_eq!(d, Decision::Skip);
    }

    #[test]
    fn diff_input_changed_runs() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Photon, &["p"], inputs(&[("photon.md5", "abc")]));
        let d = diff(idx, Phase::Photon, &inputs(&[("photon.md5", "xyz")]));
        assert!(d.is_run());
        assert!(d.reason().contains("photon.md5"));
        assert!(d.reason().contains("abc"));
        assert!(d.reason().contains("xyz"));
    }

    #[test]
    fn diff_input_added_runs() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::National, &["a"], inputs(&[]));
        let d = diff(idx, Phase::National, &inputs(&[("national.etag", "e1")]));
        assert!(d.is_run());
        assert!(d.reason().contains("added"));
    }

    #[test]
    fn diff_input_removed_runs() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::National, &["a"], inputs(&[("national.etag", "e1")]));
        let d = diff(idx, Phase::National, &inputs(&[]));
        assert!(d.is_run());
        assert!(d.reason().contains("removed"));
    }

    #[test]
    fn legacy_phase3_sentinel_skips_when_outputs_ok() {
        // Hand-write a sentinel without the format marker (mimicking
        // what Phase 3 wrote pre-upgrade).
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        std::fs::write(idx.join("p"), b"x").unwrap();
        std::fs::create_dir_all(checkpoints_dir(idx)).unwrap();
        let legacy = serde_json::json!({
            "phase": "extract",
            "started_at": 1,
            "finished_at": 2,
            "outputs": [{"name": "p", "size_bytes": 1}],
        });
        std::fs::write(
            sentinel_path(idx, Phase::Extract),
            serde_json::to_string_pretty(&legacy).unwrap(),
        ).unwrap();
        // Even with a non-empty current_inputs, legacy sentinel skips.
        let d = diff(idx, Phase::Extract, &inputs(&[("osm.sequence", "999")]));
        assert_eq!(d, Decision::Skip);
        let parsed = read(idx, Phase::Extract).unwrap();
        assert!(!parsed.is_phase4_format());
    }

    #[test]
    fn cascade_forces_run() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Pack, &["p"], inputs(&[]));
        // Without cascade, pack would skip.
        let (d_no, after_no) = cascade_decide(idx, Phase::Pack, &inputs(&[]), false);
        assert_eq!(d_no, Decision::Skip);
        assert!(!after_no);
        // With cascade, pack runs regardless.
        let (d_yes, after_yes) = cascade_decide(idx, Phase::Pack, &inputs(&[]), true);
        assert!(d_yes.is_run());
        assert!(d_yes.reason().contains("cascade"));
        assert!(after_yes);
    }

    #[test]
    fn cascade_propagates_from_run() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        write_ckpt(idx, Phase::Photon, &["p"], inputs(&[("photon.md5", "abc")]));
        // Photon source moved → photon RUN, cascade flips on.
        let (d, after) = cascade_decide(
            idx, Phase::Photon, &inputs(&[("photon.md5", "xyz")]), false,
        );
        assert!(d.is_run());
        assert!(after);
    }

    #[test]
    fn round_trip_ckpt_struct() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        std::fs::write(idx.join("places.parquet"), b"hello").unwrap();
        PhaseTimer::start(Phase::Extract).finish(
            idx,
            &["places.parquet"],
            inputs(&[("osm.sequence", "12345")]),
            "5 places",
        ).unwrap();
        let read = read(idx, Phase::Extract).expect("read back ckpt");
        assert_eq!(read.phase, "extract");
        assert_eq!(read.outputs.len(), 1);
        assert_eq!(read.outputs[0].name, "places.parquet");
        assert_eq!(read.outputs[0].size_bytes, 5);
        assert_eq!(read.summary, "5 places");
        assert!(read.is_phase4_format());
        let user = read.user_inputs();
        assert_eq!(user.get("osm.sequence").map(String::as_str), Some("12345"));
        assert!(!user.contains_key(FORMAT_MARKER_KEY));
    }

    #[test]
    fn truncate_clips_long() {
        assert_eq!(truncate("short"), "short");
        let long = "a".repeat(40);
        let t = truncate(&long);
        assert!(t.len() < long.len());
        assert!(t.ends_with('…'));
    }
}
