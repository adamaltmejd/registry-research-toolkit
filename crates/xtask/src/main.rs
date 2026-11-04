//! Repository tooling, run as `cargo xtask <command>` (alias in `.cargo/config.toml`).

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match args.split_first() {
        Some((cmd, rest)) if cmd == "gh-issue" => xtask::gh_issue::main(rest),
        _ => {
            eprintln!("{}", xtask::USAGE);
            xtask::gh_issue::EXIT_USAGE
        }
    };
    ExitCode::from(code)
}
