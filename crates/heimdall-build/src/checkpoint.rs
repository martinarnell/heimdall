//! Step-level checkpoints (TODO_REBUILD_MODES Phase 3.3).
//!
//! After each rebuild step succeeds we drop a `<phase>.done` file in
//! `<index>/checkpoints/`. On restart, if a checkpoint exists *and* its
//! recorded outputs are still on disk, the planner skips the step.
//!
//! Phase 3 only uses checkpoints for **resume after crash** — the input-hash
//! diff planner that decides which downstream phases to invalidate when a
//! source changes is Phase 4. We deliberately leave the inputs hash blank
//! (or a placeholder) here so Phase 4 can fill it in without renaming the
//! file format.
//!
//! ### Invalidation rules
//!
//! * `--cleanup` and `--keep-intermediates=0` delete the parquet files that
//!   most checkpoints record as outputs. When the planner notices a missing
//!   output, the checkpoint is considered stale — the step re-runs.
//! * `meta.json` lives next to the checkpoints dir and remains the source
//!   of truth for "this index is servable". Checkpoints don't replace it.
//! * On any error mid-step, no `.done` is written → next run re-tries.
//!
//! Format intentionally JSON: tiny files (a few hundred bytes), human
//! debuggable, easy to extend (Phase 4 adds an `input_hash` field).

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Checkpoint {
    pub phase: String,
    pub started_at: u64,
    pub finished_at: u64,
    /// Filenames (relative to index dir) that this phase produced. The
    /// planner verifies they still exist before trusting the checkpoint.
    pub outputs: Vec<OutputEntry>,
    /// Phase 4 will populate this from input file hashes / etags. Phase 3
    /// leaves it empty so reading old checkpoints stays forward-compatible.
    #[serde(default)]
    pub input_hash: String,
    /// Free-form note from the step ("123456 places, 4567 addr").
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
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
    /// checkpoint hasn't been invalidated by a later cleanup.
    pub fn finish(
        self,
        index_dir: &Path,
        outputs: &[&str],
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
        let ckpt = Checkpoint {
            phase: self.phase.as_str().into(),
            started_at: self.started_at,
            finished_at: unix_now(),
            outputs: entries,
            input_hash: String::new(),
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
/// at non-zero size. Used by the planner to skip a phase that completed in
/// a prior run.
pub fn is_done(index_dir: &Path, phase: Phase) -> bool {
    let path = sentinel_path(index_dir, phase);
    let f = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(_) => return false,
    };
    let ckpt: Checkpoint = match serde_json::from_reader(f) {
        Ok(c) => c,
        Err(_) => return false,  // corrupt → treat as not done
    };
    for o in &ckpt.outputs {
        let p = index_dir.join(&o.name);
        match std::fs::metadata(&p) {
            Ok(m) if m.len() > 0 => continue,
            _ => return false,
        }
    }
    true
}

pub fn read(index_dir: &Path, phase: Phase) -> Option<Checkpoint> {
    let f = std::fs::File::open(sentinel_path(index_dir, phase)).ok()?;
    serde_json::from_reader(f).ok()
}

/// Wipe the entire `checkpoints/` directory. Called when the user passes
/// `--force=all`, when meta.json is gone (corrupt build), or when
/// `--cleanup` deletes the intermediates the checkpoints depend on.
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

    #[test]
    fn write_then_is_done() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        // Create a fake output file
        std::fs::write(idx.join("places.parquet"), b"x").unwrap();
        let t = PhaseTimer::start(Phase::Extract);
        t.finish(idx, &["places.parquet"], "1 place").unwrap();
        assert!(is_done(idx, Phase::Extract));
        assert!(!is_done(idx, Phase::Pack));
    }

    #[test]
    fn missing_output_invalidates() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        std::fs::write(idx.join("places.parquet"), b"x").unwrap();
        PhaseTimer::start(Phase::Extract).finish(idx, &["places.parquet"], "").unwrap();
        assert!(is_done(idx, Phase::Extract));
        // Cleanup removed the parquet → checkpoint is stale.
        std::fs::remove_file(idx.join("places.parquet")).unwrap();
        assert!(!is_done(idx, Phase::Extract));
    }

    #[test]
    fn clear_all_drops_dir() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        std::fs::write(idx.join("a"), b"x").unwrap();
        PhaseTimer::start(Phase::Pack).finish(idx, &["a"], "").unwrap();
        assert!(is_done(idx, Phase::Pack));
        clear_all(idx).unwrap();
        assert!(!is_done(idx, Phase::Pack));
    }

    #[test]
    fn clear_phase_keeps_others() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        std::fs::write(idx.join("a"), b"x").unwrap();
        std::fs::write(idx.join("b"), b"y").unwrap();
        PhaseTimer::start(Phase::Extract).finish(idx, &["a"], "").unwrap();
        PhaseTimer::start(Phase::Pack).finish(idx, &["b"], "").unwrap();
        assert!(is_done(idx, Phase::Extract));
        assert!(is_done(idx, Phase::Pack));
        clear_phase(idx, Phase::Pack).unwrap();
        assert!(is_done(idx, Phase::Extract));
        assert!(!is_done(idx, Phase::Pack));
    }

    #[test]
    fn round_trip_ckpt_struct() {
        let dir = TempDir::new().unwrap();
        let idx = dir.path();
        std::fs::write(idx.join("places.parquet"), b"hello").unwrap();
        PhaseTimer::start(Phase::Extract).finish(idx, &["places.parquet"], "5 places").unwrap();
        let read = read(idx, Phase::Extract).expect("read back ckpt");
        assert_eq!(read.phase, "extract");
        assert_eq!(read.outputs.len(), 1);
        assert_eq!(read.outputs[0].name, "places.parquet");
        assert_eq!(read.outputs[0].size_bytes, 5);
        assert_eq!(read.summary, "5 places");
    }
}
