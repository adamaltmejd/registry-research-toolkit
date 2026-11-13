//! `reg-meta serve` processes, one per artifact, shared by every case of the run.
//!
//! A server is started under a watchdog `sh` that kills it when this process's end of
//! its stdin pipe closes, which happens however the test process exits (pass, panic,
//! Ctrl-C, SIGKILL): Rust tests have no session-end hook, so nothing else would stop
//! it.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::client;

/// How long a server may take to answer `READY_PATH`. The release skill raises it in
/// a reviewed PR, not in place.
pub const SERVER_READY_SECONDS: u64 = 120;
/// Static, so polling it stacks no work on a server still on its cold first read.
const READY_PATH: &str = "/openapi.json";

/// `reg-meta`: `$REG_META_BIN`, else beside this test in its cargo target.
pub fn reg_meta() -> PathBuf {
    reg_conformance::binary("reg-meta", "REG_META_BIN")
}

/// The `serve` argv for the artifact directory `db` on a kernel-picked port. The
/// write limit is raised: the suite replays many projects from one address.
/// `REG_META_SERVE_ARGS` is appended, split on whitespace (`--catalog swecov`).
pub fn serve_argv(db: &Path) -> Vec<String> {
    let mut argv = vec![
        "serve".to_owned(),
        "--db".to_owned(),
        db.display().to_string(),
        "--port".to_owned(),
        "0".to_owned(),
        "--write-limit".to_owned(),
        "100000".to_owned(),
    ];
    if let Ok(extra) = std::env::var("REG_META_SERVE_ARGS") {
        argv.extend(extra.split_whitespace().map(str::to_owned));
    }
    argv
}

/// A running server. Its watchdog holds the other end of `_stdin`, so the field
/// must live as long as the server should.
#[derive(Debug)]
pub struct Server {
    pub port: u16,
    _child: Child,
    _stdin: ChildStdin,
}

type Slot = Arc<OnceLock<Result<Arc<Server>, String>>>;
/// An artifact directory and the catalog it is served as (`None`: its own).
type PoolKey = (PathBuf, Option<String>);

/// The server of the artifact directory `db`, started on first use. A failed start
/// is remembered, so later cases on the artifact fail at once instead of waiting out
/// the readiness deadline again.
///
/// Every case shares it, `/mcp` included: `serve_argv`'s raised write limit keeps the
/// one bucket of this address from refusing the suite's own requests.
pub fn server(db: &Path) -> Result<Arc<Server>, String> {
    pooled(db, None)
}

/// The paired server of the steward directory `db` (`reg_meta.db` beside
/// `reg_meta_<catalog>.db`), served as `--catalog catalog`; shared like [`server`].
pub fn steward_server(db: &Path, catalog: &str) -> Result<Arc<Server>, String> {
    pooled(db, Some(catalog))
}

fn pooled(db: &Path, catalog: Option<&str>) -> Result<Arc<Server>, String> {
    static POOL: OnceLock<Mutex<HashMap<PoolKey, Slot>>> = OnceLock::new();
    let slot = POOL
        .get_or_init(Mutex::default)
        .lock()
        .expect("the server pool")
        .entry((db.to_path_buf(), catalog.map(str::to_owned)))
        .or_default()
        .clone();
    slot.get_or_init(|| {
        let mut argv = serve_argv(db);
        if let Some(catalog) = catalog {
            argv.extend(["--catalog".to_owned(), catalog.to_owned()]);
        }
        start(db, &argv, &[]).map(Arc::new)
    })
    .clone()
}

/// This run's directory in the cargo target's temp dir, for the server logs. The
/// session-built stewards are elsewhere (`steward::run_dir`).
pub fn log_dir() -> PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("conformance-{}", std::process::id()));
        // A process id repeats across runs (CI restores the target dir from cache on a
        // fresh runner), so an earlier run's directory is emptied: this run's logs are
        // not mixed with a stale run's.
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::create_dir_all(&dir);
        dir
    })
    .clone()
}

/// A server of its own on `db`, from the `serve` argv `argv` (see `serve_argv`) with
/// `env` added; it stops when the returned value drops.
pub fn start(db: &Path, argv: &[String], env: &[(&str, &str)]) -> Result<Server, String> {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let log = log_dir().join(format!("server-{n}.log"));
    let output = fs::File::create(&log).map_err(|e| format!("{}: {e}", log.display()))?;
    // fd 3 carries the pipe to the watchdog: an asynchronous list's stdin is
    // /dev/null. `wait $s` makes sh exit with the server's status, so an early exit
    // is still seen.
    let mut child = Command::new("sh")
        .args([
            "-c",
            r#"exec 3<&0; "$@" 3<&- </dev/null & s=$!; { read _ <&3; kill $s; } & wait $s"#,
            "sh",
        ])
        .arg(reg_meta())
        .args(argv)
        .current_dir(reg_conformance::repo())
        .env("REG_META_DB", db)
        .envs(env.iter().copied())
        .stdin(Stdio::piped())
        .stdout(output.try_clone().map_err(|e| e.to_string())?)
        .stderr(output)
        .spawn()
        .map_err(|e| format!("sh: {e}"))?;
    let stdin = child.stdin.take().expect("piped stdin");
    let failed = |child: &mut Child, why: &str| {
        let _ = child.kill();
        let text = fs::read_to_string(&log).unwrap_or_default();
        let tail: String = text
            .chars()
            .rev()
            .take(4000)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!(
            "server {why}: {} {}\n{tail}",
            reg_meta().display(),
            argv.join(" ")
        )
    };
    let deadline = Instant::now() + Duration::from_secs(SERVER_READY_SECONDS);
    let mut port = None;
    while Instant::now() < deadline {
        if port.is_none() {
            port = listening_port(&log);
        }
        if let Some(port) = port {
            let answered =
                client::request(port, "GET", READY_PATH, &[], None, Duration::from_secs(1)).is_ok();
            // An exited server's released port can be answered by another.
            if answered && matches!(child.try_wait(), Ok(None)) {
                return Ok(Server {
                    port,
                    _child: child,
                    _stdin: stdin,
                });
            }
        }
        if !matches!(child.try_wait(), Ok(None)) {
            return Err(failed(&mut child, "exited before it answered"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(failed(&mut child, &format!("never answered {READY_PATH}")))
}

/// The port `reg-meta serve` names on its `listening on ADDR:PORT` line once bound.
fn listening_port(log: &Path) -> Option<u16> {
    let text = fs::read_to_string(log).ok()?;
    text.split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .find_map(|line| line.strip_prefix("listening on "))
        .and_then(|address| address.trim_end().rsplit_once(':'))
        .and_then(|(_, port)| port.parse().ok())
}
