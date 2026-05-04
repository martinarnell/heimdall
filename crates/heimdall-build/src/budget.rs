//! Resource-budget presets (TODO_REBUILD_MODES Phase 3.4).
//!
//! Each preset is sugar over the explicit `--max-ram` / `--max-disk` /
//! `--sort-mem` / `--keep-downloads` flags. Picking a preset *and* an
//! explicit flag means the explicit flag wins — see `Budget::resolve`.
//!
//! The four presets correspond to plausible deployment shapes:
//!
//! | Preset       | RAM   | Disk   | Threads | sort_mem | keep_dl | Use case |
//! |---           |---    |---     |---      |---       |---      |---       |
//! | `potato`     | 1 GB  | 10 GB  | 1       | 64 MB    | 0       | Cloud micro / cheap laptop |
//! | `laptop`     | 4 GB  | 30 GB  | 2       | 512 MB   | 0       | Dev machine, single country |
//! | `workstation`| 16 GB | 100 GB | n cores | 256 MB   | 1       | Today's default |
//! | `server`     | 64 GB | 1 TB   | n cores | 256 MB   | 1       | Weekly cron, all countries |
//!
//! `--budget potato --max-ram=2G` is valid: the preset sets a 1 GB cap and
//! the override bumps it to 2 GB. Same pattern for every other knob.

use std::path::PathBuf;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Preset {
    Potato,
    Laptop,
    Workstation,
    Server,
}

impl FromStr for Preset {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "potato" => Ok(Preset::Potato),
            "laptop" => Ok(Preset::Laptop),
            "workstation" | "default" => Ok(Preset::Workstation),
            "server" => Ok(Preset::Server),
            other => Err(format!(
                "unknown budget preset '{}'; valid: potato, laptop, workstation, server",
                other,
            )),
        }
    }
}

/// Materialised budget. Every field is concrete — defaults flowed through
/// from the preset, and any user `--max-*` / `--sort-mem` overrides have
/// already been applied.
#[derive(Debug, Clone)]
pub struct ResolvedBudget {
    pub preset: Option<Preset>,
    pub max_ram_bytes: u64,
    pub max_disk_bytes: u64,
    pub max_threads: usize,
    pub sort_mem_bytes: usize,
    pub keep_downloads: u32,
    pub keep_intermediates: u32,
    /// Where SortBuffer spills + checkpoints live when the per-index
    /// default isn't desired. `None` means "use `<index>/.scratch/`".
    pub scratch_dir: Option<PathBuf>,
}

impl ResolvedBudget {
    /// Default when no `--budget` flag was passed: unbounded, current
    /// behaviour. Equivalent to `workstation` minus the explicit caps.
    pub fn unbounded() -> Self {
        Self {
            preset: None,
            max_ram_bytes: 0,
            max_disk_bytes: 0,
            max_threads: 0,
            sort_mem_bytes: 256 * 1024 * 1024,
            keep_downloads: 1,
            keep_intermediates: 1,
            scratch_dir: None,
        }
    }
}

const GB: u64 = 1024 * 1024 * 1024;
const MB: u64 = 1024 * 1024;

fn preset_defaults(p: Preset) -> ResolvedBudget {
    match p {
        Preset::Potato => ResolvedBudget {
            preset: Some(Preset::Potato),
            max_ram_bytes: 1 * GB,
            max_disk_bytes: 10 * GB,
            max_threads: 1,
            sort_mem_bytes: 64 * MB as usize,
            keep_downloads: 0,
            keep_intermediates: 0,
            scratch_dir: None,
        },
        Preset::Laptop => ResolvedBudget {
            preset: Some(Preset::Laptop),
            max_ram_bytes: 4 * GB,
            max_disk_bytes: 30 * GB,
            max_threads: 2,
            sort_mem_bytes: 512 * MB as usize,
            keep_downloads: 0,
            keep_intermediates: 0,
            scratch_dir: None,
        },
        Preset::Workstation => ResolvedBudget {
            preset: Some(Preset::Workstation),
            max_ram_bytes: 16 * GB,
            max_disk_bytes: 100 * GB,
            max_threads: num_cpus(),
            sort_mem_bytes: 256 * MB as usize,
            keep_downloads: 1,
            keep_intermediates: 1,
            scratch_dir: None,
        },
        Preset::Server => ResolvedBudget {
            preset: Some(Preset::Server),
            // server is "trust the operator" — caps are advisory only.
            max_ram_bytes: 64 * GB,
            max_disk_bytes: 1024 * GB,
            max_threads: num_cpus(),
            sort_mem_bytes: 256 * MB as usize,
            keep_downloads: 1,
            keep_intermediates: 1,
            scratch_dir: None,
        },
    }
}

/// Apply preset + per-flag overrides in priority order. Explicit user
/// settings always beat the preset.
pub struct BudgetOverrides {
    pub preset: Option<Preset>,
    pub max_ram_bytes: Option<u64>,
    pub max_disk_bytes: Option<u64>,
    pub max_threads: Option<usize>,
    pub sort_mem_bytes: Option<usize>,
    pub keep_downloads: Option<u32>,
    pub keep_intermediates: Option<u32>,
    pub scratch_dir: Option<PathBuf>,
}

impl BudgetOverrides {
    pub fn resolve(self) -> ResolvedBudget {
        let mut b = match self.preset {
            Some(p) => preset_defaults(p),
            None => ResolvedBudget::unbounded(),
        };
        if let Some(v) = self.max_ram_bytes { b.max_ram_bytes = v; }
        if let Some(v) = self.max_disk_bytes { b.max_disk_bytes = v; }
        if let Some(v) = self.max_threads { b.max_threads = v; }
        if let Some(v) = self.sort_mem_bytes { b.sort_mem_bytes = v; }
        if let Some(v) = self.keep_downloads { b.keep_downloads = v; }
        if let Some(v) = self.keep_intermediates { b.keep_intermediates = v; }
        if let Some(v) = self.scratch_dir { b.scratch_dir = Some(v); }
        b
    }
}

fn num_cpus() -> usize {
    // std::thread::available_parallelism is the right primitive; if it
    // fails (containers without /proc, etc.) we conservatively use 1.
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_preset() {
        assert_eq!("potato".parse::<Preset>().unwrap(), Preset::Potato);
        assert_eq!("LAPTOP".parse::<Preset>().unwrap(), Preset::Laptop);
        assert_eq!(" Server ".parse::<Preset>().unwrap(), Preset::Server);
        assert!("garbage".parse::<Preset>().is_err());
    }

    #[test]
    fn potato_defaults() {
        let b = BudgetOverrides {
            preset: Some(Preset::Potato),
            max_ram_bytes: None,
            max_disk_bytes: None,
            max_threads: None,
            sort_mem_bytes: None,
            keep_downloads: None,
            keep_intermediates: None,
            scratch_dir: None,
        }.resolve();
        assert_eq!(b.max_ram_bytes, 1 * GB);
        assert_eq!(b.max_threads, 1);
        assert_eq!(b.sort_mem_bytes, 64 * MB as usize);
        assert_eq!(b.keep_downloads, 0);
    }

    #[test]
    fn explicit_override_beats_preset() {
        // potato gives 1GB RAM, but --max-ram=2G should win.
        let b = BudgetOverrides {
            preset: Some(Preset::Potato),
            max_ram_bytes: Some(2 * GB),
            max_disk_bytes: None,
            max_threads: None,
            sort_mem_bytes: None,
            keep_downloads: None,
            keep_intermediates: None,
            scratch_dir: None,
        }.resolve();
        assert_eq!(b.max_ram_bytes, 2 * GB);
        assert_eq!(b.max_threads, 1, "preset's other fields should still apply");
    }

    #[test]
    fn no_preset_means_unbounded() {
        let b = BudgetOverrides {
            preset: None,
            max_ram_bytes: None,
            max_disk_bytes: None,
            max_threads: None,
            sort_mem_bytes: None,
            keep_downloads: None,
            keep_intermediates: None,
            scratch_dir: None,
        }.resolve();
        assert_eq!(b.max_ram_bytes, 0);
        assert_eq!(b.max_disk_bytes, 0);
        assert_eq!(b.keep_downloads, 1, "default is keep current download");
    }
}
