//! Build a conformance fixture for a consumer outside the suite (`dev.sh`, the gate).
//!
//!     reg-fixture <fixture>        build `conformance/cases/fixtures/<fixture>` into
//!                                  the fixture cache; print its `reg_meta.db`
//!     reg-fixture --paired <out>   build the synthetic holdings pair into <out>;
//!                                  print its catalog selector (`fixture`)
//!
//! Both need this checkout's compiled `target/debug/reg-build` and the second also
//! git. A cached path stays valid for 6 hours after its last lookup, so look it up
//! again rather than keeping it.

use std::path::Path;
use std::process::ExitCode;

const USAGE: &str = "usage: reg-fixture <fixture> | reg-fixture --paired <out-dir>";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.as_slice() {
        [flag, out] if flag == "--paired" => {
            reg_conformance::build_paired(Path::new(out)).map(|()| "fixture".to_owned())
        }
        [name] if !name.starts_with('-') => {
            reg_conformance::cached_reader_artifact(&reg_conformance::fixture(name)).map(|path| {
                eprintln!("valid for 6 hours after this lookup");
                path.display().to_string()
            })
        }
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(line) => {
            println!("{line}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("reg-fixture: {error}");
            ExitCode::FAILURE
        }
    }
}
