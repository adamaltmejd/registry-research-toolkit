//! Per-phase wall time on stderr, so a real-seed build shows where its time goes (#2196).
//! Peak memory is not read here (`unsafe_code` is forbidden, so no `getrusage`): sample the
//! process from outside, e.g. `/usr/bin/time -l` or a `ps` loop, and match it to these lines.

use std::sync::OnceLock;
use std::time::Instant;

static START: OnceLock<Instant> = OnceLock::new();

/// Marks process start; later `done` lines report time since then.
pub(crate) fn start() {
    START.get_or_init(Instant::now);
}

/// Prints `reg-build: timing <phase> <seconds>s (at <seconds since start>s)`.
pub(crate) fn done(phase: &str, since: Instant) {
    let at = START.get().map_or(0.0, |s| s.elapsed().as_secs_f64());
    eprintln!(
        "reg-build: timing {phase} {:.1}s (at {at:.1}s)",
        since.elapsed().as_secs_f64()
    );
}
