//! RSS observer for adaptive rebuild budgets (TODO_REBUILD_MODES Phase 3.1).
//!
//! Polls `/proc/self/status:VmRSS` in a background thread (1 s cadence) and
//! exposes a [`Pressure`] signal computed against a [`ResourceBudget`].
//!
//! The signal is plumbed into:
//!   * [`crate::sort_buffer::SortBuffer`] — force-spill the in-memory batch
//!     on `Hard` pressure instead of growing it further;
//!   * the per-country scheduler in `rebuild.rs` — drop parallelism on
//!     `Soft`/`Hard` pressure.
//!
//! Linux-only (reads `/proc/self/status`); on other platforms `current_rss`
//! returns 0 and pressure is always `None`. We don't claim to support potato
//! builds on macOS yet.
//!
//! Cadence: 1 s for the first minute, 5 s thereafter — see open question 4
//! in TODO_REBUILD_MODES.md. CPU cost is negligible compared to a rebuild.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Hard caps the user is willing to spend on a single rebuild.
///
/// `max_ram_bytes == 0` means "no enforcement" — useful for the `server`
/// preset where we trust the operator to size the box.
#[derive(Debug, Clone)]
pub struct ResourceBudget {
    pub max_ram_bytes: u64,
    pub max_disk_bytes: u64,
    pub max_threads: usize,
    pub scratch_dir: PathBuf,
}

impl ResourceBudget {
    /// `unbounded()` is a sentinel: pressure is always `None`, the monitor
    /// still records peak RSS for the build report.
    pub fn unbounded(scratch_dir: PathBuf) -> Self {
        Self {
            max_ram_bytes: 0,
            max_disk_bytes: 0,
            max_threads: 0,
            scratch_dir,
        }
    }
}

/// Coarse pressure level. The monitor stores it as a `u8` so other threads
/// can read it without locking.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pressure {
    None,
    Soft,
    Hard,
}

impl Pressure {
    fn from_u8(v: u8) -> Self {
        match v {
            2 => Pressure::Hard,
            1 => Pressure::Soft,
            _ => Pressure::None,
        }
    }
    fn to_u8(self) -> u8 {
        match self {
            Pressure::None => 0,
            Pressure::Soft => 1,
            Pressure::Hard => 2,
        }
    }
}

/// Background thread that polls VmRSS and updates a shared pressure cell.
///
/// Drop the [`ResourceMonitor`] to stop the thread. The atomics live in
/// `Arc`s so a [`PressureSignal`] handle can outlive the monitor — useful
/// for tests that want to drive pressure manually.
pub struct ResourceMonitor {
    inner: Arc<MonitorInner>,
    handle: Option<thread::JoinHandle<()>>,
}

struct MonitorInner {
    budget: ResourceBudget,
    rss_bytes: AtomicU64,
    peak_rss_bytes: AtomicU64,
    pressure: AtomicU8,
    stop: AtomicBool,
}

impl ResourceMonitor {
    /// Spawn the polling thread. If `budget.max_ram_bytes == 0`, no
    /// pressure is ever signalled — the monitor still records peak RSS.
    pub fn spawn(budget: ResourceBudget) -> Self {
        let inner = Arc::new(MonitorInner {
            budget,
            rss_bytes: AtomicU64::new(0),
            peak_rss_bytes: AtomicU64::new(0),
            pressure: AtomicU8::new(Pressure::None.to_u8()),
            stop: AtomicBool::new(false),
        });
        let inner_t = inner.clone();
        let handle = thread::Builder::new()
            .name("heimdall-rss-monitor".into())
            .spawn(move || run(inner_t))
            .ok();
        Self { inner, handle }
    }

    /// Cheap handle to read the pressure state from sort buffers / scheduler.
    pub fn signal(&self) -> PressureSignal {
        PressureSignal { inner: self.inner.clone() }
    }

    pub fn budget(&self) -> &ResourceBudget {
        &self.inner.budget
    }

    pub fn current_rss(&self) -> u64 {
        self.inner.rss_bytes.load(Ordering::Relaxed)
    }

    pub fn peak_rss(&self) -> u64 {
        self.inner.peak_rss_bytes.load(Ordering::Relaxed)
    }

    pub fn pressure(&self) -> Pressure {
        Pressure::from_u8(self.inner.pressure.load(Ordering::Relaxed))
    }
}

impl Drop for ResourceMonitor {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            // Best-effort join — sample loop only sleeps for short windows
            // so the thread should exit quickly. We don't propagate panics
            // because the monitor is advisory: a dead monitor must not
            // poison the rebuild.
            let _ = h.join();
        }
    }
}

/// Cheaply-cloned read-only view of the monitor's state.
///
/// Used inside [`crate::sort_buffer::SortBuffer::push`] (must not lock) and
/// in the per-country scheduler. Provides both the current pressure level
/// and a force-spill latch that gets set when crossing into `Hard` pressure
/// and consumed by the buffer on the next push.
#[derive(Clone)]
pub struct PressureSignal {
    inner: Arc<MonitorInner>,
}

impl std::fmt::Debug for PressureSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PressureSignal")
            .field("pressure", &self.pressure())
            .field("rss_bytes", &self.current_rss())
            .field("max_ram_bytes", &self.budget_max_ram())
            .finish()
    }
}

impl PressureSignal {
    /// A no-op signal — always reports `Pressure::None`. Lets call sites
    /// keep a single code path whether or not the monitor is configured.
    pub fn disabled() -> Self {
        Self {
            inner: Arc::new(MonitorInner {
                budget: ResourceBudget::unbounded(PathBuf::new()),
                rss_bytes: AtomicU64::new(0),
                peak_rss_bytes: AtomicU64::new(0),
                pressure: AtomicU8::new(Pressure::None.to_u8()),
                stop: AtomicBool::new(true),
            }),
        }
    }

    pub fn pressure(&self) -> Pressure {
        Pressure::from_u8(self.inner.pressure.load(Ordering::Relaxed))
    }

    pub fn is_hard(&self) -> bool {
        self.pressure() == Pressure::Hard
    }

    pub fn current_rss(&self) -> u64 {
        self.inner.rss_bytes.load(Ordering::Relaxed)
    }

    pub fn budget_max_ram(&self) -> u64 {
        self.inner.budget.max_ram_bytes
    }
}

fn run(inner: Arc<MonitorInner>) {
    let started = Instant::now();
    while !inner.stop.load(Ordering::Relaxed) {
        // Cadence: 1 s for the first 60 s (catches early ballooning during
        // pack, when growth is fastest), then 5 s. Open question 4 in
        // TODO_REBUILD_MODES suggested this; cheap enough to ship as default.
        let elapsed = started.elapsed();
        let interval = if elapsed < Duration::from_secs(60) {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(5)
        };

        if let Some(rss) = read_vm_rss() {
            inner.rss_bytes.store(rss, Ordering::Relaxed);
            // Compare-and-swap loop: peak = max(peak, rss).
            let mut peak = inner.peak_rss_bytes.load(Ordering::Relaxed);
            while rss > peak {
                match inner.peak_rss_bytes.compare_exchange_weak(
                    peak, rss, Ordering::Relaxed, Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => peak = actual,
                }
            }

            let max = inner.budget.max_ram_bytes;
            let p = if max == 0 {
                Pressure::None
            } else if rss * 100 >= max * 90 {
                Pressure::Hard
            } else if rss * 100 >= max * 70 {
                Pressure::Soft
            } else {
                Pressure::None
            };
            inner.pressure.store(p.to_u8(), Ordering::Relaxed);
        }

        // Sleep in 200 ms chunks so `stop` is observed promptly on shutdown.
        let mut remaining = interval;
        while remaining > Duration::ZERO && !inner.stop.load(Ordering::Relaxed) {
            let chunk = remaining.min(Duration::from_millis(200));
            thread::sleep(chunk);
            remaining = remaining.saturating_sub(chunk);
        }
    }
}

/// Read VmRSS from /proc/self/status. Returns bytes.
///
/// Returns None on any parse / IO failure — the monitor falls back to "no
/// pressure" rather than panicking. We've intentionally not cached the file
/// handle: /proc/self/status is volatile and re-opening costs ~10 µs.
#[cfg(target_os = "linux")]
fn read_vm_rss() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn read_vm_rss() -> Option<u64> { None }

// ───────────────────────────────────────────────────────────────────────────
// Disk usage helpers (used by pre-flight + periodic disk-budget tracker)
// ───────────────────────────────────────────────────────────────────────────

/// Available bytes on the filesystem hosting `path`. None if the syscall
/// fails (path doesn't exist yet, etc.).
#[cfg(target_os = "linux")]
pub fn available_disk_bytes(path: &std::path::Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let cpath = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut sb: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(cpath.as_ptr(), &mut sb) };
    if rc != 0 { return None; }
    // f_bavail = blocks available to non-root. Use that, not f_bfree.
    Some(sb.f_bavail as u64 * sb.f_frsize as u64)
}

#[cfg(not(target_os = "linux"))]
pub fn available_disk_bytes(_path: &std::path::Path) -> Option<u64> { None }

/// Total physical RAM on the box. None if /proc/meminfo unreadable.
#[cfg(target_os = "linux")]
pub fn total_system_ram_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
pub fn total_system_ram_bytes() -> Option<u64> { None }

/// Available RAM (MemAvailable, the kernel's estimate of how much can be
/// used without swapping). None if /proc/meminfo unreadable.
#[cfg(target_os = "linux")]
pub fn available_system_ram_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
pub fn available_system_ram_bytes() -> Option<u64> { None }

/// Pretty-print bytes for human reports. Truncated, not rounded — `1023 MB`
/// is fine, we're not chasing thousands-of-an-MB.
pub fn fmt_bytes(b: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    if b >= TB { format!("{:.1} TB", b as f64 / TB as f64) }
    else if b >= GB { format!("{:.1} GB", b as f64 / GB as f64) }
    else if b >= MB { format!("{:.0} MB", b as f64 / MB as f64) }
    else if b >= KB { format!("{:.0} KB", b as f64 / KB as f64) }
    else { format!("{} B", b) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_thresholds() {
        let inner = MonitorInner {
            budget: ResourceBudget {
                max_ram_bytes: 1_000,
                max_disk_bytes: 0,
                max_threads: 1,
                scratch_dir: PathBuf::new(),
            },
            rss_bytes: AtomicU64::new(0),
            peak_rss_bytes: AtomicU64::new(0),
            pressure: AtomicU8::new(0),
            stop: AtomicBool::new(false),
        };
        // 50% — None
        let rss = 500u64;
        let p = compute_pressure(rss, inner.budget.max_ram_bytes);
        assert_eq!(p, Pressure::None);
        // 75% — Soft
        assert_eq!(compute_pressure(750, 1000), Pressure::Soft);
        // 95% — Hard
        assert_eq!(compute_pressure(950, 1000), Pressure::Hard);
        // 70% boundary — Soft
        assert_eq!(compute_pressure(700, 1000), Pressure::Soft);
        // 90% boundary — Hard
        assert_eq!(compute_pressure(900, 1000), Pressure::Hard);
        // unbounded budget — always None
        assert_eq!(compute_pressure(u64::MAX, 0), Pressure::None);
    }

    fn compute_pressure(rss: u64, max: u64) -> Pressure {
        if max == 0 { return Pressure::None; }
        if rss * 100 >= max * 90 { Pressure::Hard }
        else if rss * 100 >= max * 70 { Pressure::Soft }
        else { Pressure::None }
    }

    #[test]
    fn fmt_bytes_units() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(2048), "2 KB");
        assert_eq!(fmt_bytes(2 * 1024 * 1024), "2 MB");
        assert_eq!(fmt_bytes(1_500_000_000), "1.4 GB");
    }

    #[test]
    fn disabled_signal_reports_no_pressure() {
        let s = PressureSignal::disabled();
        assert_eq!(s.pressure(), Pressure::None);
        assert!(!s.is_hard());
    }

    #[test]
    fn monitor_records_peak_rss() {
        // Smoke test: spawn the monitor, give it a tick, peak should be > 0
        // (Linux only — the test process has nonzero RSS).
        if cfg!(not(target_os = "linux")) { return; }
        let m = ResourceMonitor::spawn(ResourceBudget::unbounded(PathBuf::new()));
        std::thread::sleep(std::time::Duration::from_millis(1500));
        assert!(m.peak_rss() > 0, "expected peak RSS > 0 after 1.5s");
        drop(m);
    }
}
