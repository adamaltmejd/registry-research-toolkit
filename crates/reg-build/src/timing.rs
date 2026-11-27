//! Per-phase wall time and memory on stderr, so a real-seed build shows where its time
//! and memory go (#2196). Memory is read from `/proc/self/status` (`unsafe_code` is
//! forbidden, so no `getrusage`); where that file is missing (macOS) the line carries
//! time only: sample the process from outside, e.g. `/usr/bin/time -l`.

use std::sync::OnceLock;
use std::time::Instant;

static START: OnceLock<Instant> = OnceLock::new();

/// Marks process start; later `done` lines report time since then.
pub(crate) fn start() {
    START.get_or_init(Instant::now);
}

/// Prints `reg-build: timing <phase> <seconds>s (at <seconds since start>s[, rss <GiB> GiB,
/// peak <GiB> GiB])`: resident memory now and its high-water mark since start.
pub(crate) fn done(phase: &str, since: Instant) {
    let at = START.get().map_or(0.0, |s| s.elapsed().as_secs_f64());
    eprintln!(
        "reg-build: timing {phase} {:.1}s (at {at:.1}s{})",
        since.elapsed().as_secs_f64(),
        memory().unwrap_or_default()
    );
}

fn memory() -> Option<String> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb = |key: &str| -> Option<f64> {
        let line = status.lines().find(|l| l.starts_with(key))?;
        line[key.len()..]
            .trim()
            .strip_suffix("kB")?
            .trim()
            .parse()
            .ok()
    };
    let gb = |kb: f64| kb / 1_048_576.0;
    Some(format!(
        ", rss {:.2} GiB, peak {:.2} GiB",
        gb(kb("VmRSS:")?),
        gb(kb("VmHWM:")?)
    ))
}
