//! `reg-build` runs as the builder-CLI surfaces observe them: exit code and both
//! streams.

use std::ffi::OsStr;
use std::path::Path;
use std::process::{Command, Stdio};

/// A finished command.
#[derive(Debug)]
pub struct Ran {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl Ran {
    /// Panics with stderr unless the command exited 0.
    #[track_caller]
    pub fn ok(self) -> Self {
        assert_eq!(self.code, Some(0), "{}", self.stderr);
        self
    }

    /// Panics unless the command exited 1 with every one of `located` in stderr (a
    /// refusal's `<location>: <code>` and any other located part it must name).
    #[track_caller]
    pub fn refused(self, located: &[&str]) -> Self {
        assert_eq!(self.code, Some(1), "{}", self.stderr);
        for needle in located {
            assert!(
                self.stderr.contains(needle),
                "no {needle:?} in: {}",
                self.stderr
            );
        }
        self
    }
}

/// Run `command` from the repository root with no stdin.
pub fn exec(command: &mut Command) -> Ran {
    let output = command
        .current_dir(reg_conformance::repo())
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|e| panic!("{}: {e}", command.get_program().display()));
    Ran {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// `reg-build <args>`.
pub fn reg_build<I: IntoIterator<Item = S>, S: AsRef<OsStr>>(args: I) -> Ran {
    exec(Command::new(reg_conformance::reg_build()).args(args))
}

/// `reg-build <command> --inputs <inputs> --curation <curation>`, for the caller to
/// extend and [`exec`].
pub fn curate(command: &str, inputs: &Path, curation: &Path) -> Command {
    let mut built = Command::new(reg_conformance::reg_build());
    built
        .arg(command)
        .arg("--inputs")
        .arg(inputs)
        .arg("--curation")
        .arg(curation);
    built
}

/// [`curate`], run.
pub fn curated(command: &str, inputs: &Path, curation: &Path, extra: &[&OsStr]) -> Ran {
    exec(curate(command, inputs, curation).args(extra))
}

/// `reg-build verify <db> --catalog <base> --steward-repo <root>`.
pub fn verify_holdings(db: &Path, base: &Path, root: &Path) -> Ran {
    reg_build([
        "verify".as_ref(),
        db.as_os_str(),
        "--catalog".as_ref(),
        base.as_os_str(),
        "--steward-repo".as_ref(),
        root.as_os_str(),
    ])
}

/// One column of every row of `sql` on the SQLite file `db`, as text.
pub fn column(db: &Path, sql: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open(db).unwrap_or_else(|e| panic!("{}: {e}", db.display()));
    let mut statement = conn.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    statement
        .query_map([], |row| {
            Ok(match row.get_ref(0)? {
                rusqlite::types::ValueRef::Integer(n) => n.to_string(),
                rusqlite::types::ValueRef::Real(x) => x.to_string(),
                rusqlite::types::ValueRef::Text(t) | rusqlite::types::ValueRef::Blob(t) => {
                    String::from_utf8_lossy(t).into_owned()
                }
                rusqlite::types::ValueRef::Null => String::new(),
            })
        })
        .and_then(Iterator::collect)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// Run the SQL script `sql` on the SQLite file `db`, foreign keys unenforced: the
/// damage cases break them on purpose, for verify to refuse (rusqlite's bundled SQLite
/// enforces them by default).
pub fn execute(db: &Path, sql: &str) {
    rusqlite::Connection::open(db)
        .and_then(|conn| conn.execute_batch(&format!("PRAGMA foreign_keys = OFF;\n{sql}")))
        .unwrap_or_else(|e| panic!("{}: {sql}: {e}", db.display()));
}
