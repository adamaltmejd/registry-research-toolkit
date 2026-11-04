//! The harness shared by the conformance surfaces: the server pool, the HTTP client,
//! the corpus runner and the HTTP case steps.

pub mod client;
pub mod http_case;
pub mod server;
pub mod steward;

use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::Value;

/// A JSON file.
pub fn read_json(path: &Path) -> Result<Value, String> {
    let text = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// A TOML file.
pub fn read_toml(path: &Path) -> Result<toml::Table, String> {
    let text = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    text.parse().map_err(|e| format!("{}: {e}", path.display()))
}

/// Each finding of the list `findings` without its null members: the oracles spell an
/// absent coordinate by omission.
pub fn without_nulls(findings: &Value) -> Vec<Value> {
    findings
        .as_array()
        .into_iter()
        .flatten()
        .map(|finding| match finding {
            Value::Object(map) => Value::Object(
                map.iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
            other => other.clone(),
        })
        .collect()
}

/// The `level: "error"` entries of the list `findings`.
pub fn error_level(findings: &Value) -> Vec<Value> {
    findings
        .as_array()
        .into_iter()
        .flatten()
        .filter(|finding| finding["level"] == "error")
        .cloned()
        .collect()
}

/// The case directories of a surface, sorted, narrowed by `REG_CONFORMANCE_CASE` (a
/// substring of the directory name).
pub fn case_dirs(surface: &Path) -> Vec<PathBuf> {
    let filter = std::env::var("REG_CONFORMANCE_CASE").unwrap_or_default();
    let mut cases: Vec<PathBuf> = fs::read_dir(surface)
        .unwrap_or_else(|e| panic!("{}: {e}", surface.display()))
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .filter(|path| {
            path.file_name()
                .is_some_and(|n| n.to_string_lossy().contains(&filter))
        })
        .collect();
    cases.sort();
    assert!(
        !cases.is_empty(),
        "{}: no case matches {filter:?}",
        surface.display()
    );
    cases
}

/// Run `check` on every case on worker threads, then fail once with every failing
/// case as `<case>: <first mismatch>`. A failure never stops the other cases.
pub fn run_cases(
    surface: &str,
    cases: &[PathBuf],
    check: impl Fn(&Path) -> Result<(), String> + Sync,
) {
    let next = AtomicUsize::new(0);
    let failures = Mutex::new(Vec::new());
    let workers = std::thread::available_parallelism()
        .map_or(4, usize::from)
        .min(cases.len());
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                while let Some(case) = cases.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let outcome = catch_unwind(AssertUnwindSafe(|| check(case)))
                        .unwrap_or_else(|panic| Err(panic_message(&*panic)));
                    if let Err(why) = outcome {
                        let name = case.file_name().unwrap_or_default().to_string_lossy();
                        failures
                            .lock()
                            .expect("failures")
                            .push(format!("{name}: {why}"));
                    }
                }
            });
        }
    });
    let mut failures = failures.into_inner().expect("failures");
    failures.sort();
    eprintln!(
        "{surface}: {} cases, {} failed",
        cases.len(),
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{surface}: {} of {} cases failed:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_else(|| "panicked".to_owned())
}
