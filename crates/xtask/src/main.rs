//! Repository tooling, run as `cargo xtask <command>` (alias in `.cargo/config.toml`).

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match args.split_first() {
        Some((cmd, rest)) if cmd == "gh-issue" => xtask::gh_issue::main(rest),
        Some((cmd, rest)) if cmd == "scope-check" => xtask::scope_check::main(rest),
        Some((cmd, rest)) if cmd == "release-base-reuse" => xtask::release_base_reuse::main(rest),
        Some((cmd, rest)) if cmd == "gen-case-folding" => xtask::gen_case_folding::main(rest),
        Some((cmd, rest)) if cmd == "search-eval" => xtask::search_eval::main(rest),
        Some((cmd, rest)) if cmd == "usage" => xtask::usage::main(rest),
        _ => {
            eprintln!("{}", xtask::USAGE);
            xtask::gh_issue::EXIT_USAGE
        }
    };
    ExitCode::from(code)
}
