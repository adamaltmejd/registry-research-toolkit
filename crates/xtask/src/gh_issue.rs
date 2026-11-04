//! Maintainer-author trust gate for the automation ingestion path.
//!
//! This repo is PUBLIC. A stranger can open an issue, comment on one, or file a fork PR,
//! and that free text is untrusted: an agent or skill that reads it into a model opens a
//! prompt-injection surface. This command is the choke point: it only ever surfaces issue
//! text authored by the single trusted maintainer (repo owner, or
//! `REGISTRY_MAINTAINER_LOGIN`), so a non-maintainer's content is never read by a model.
//!
//! It is deliberately an **allowlist helper, not a transparent `gh` shim.** A shim that
//! forwarded arbitrary `gh` invocations could not soundly filter: `gh api`, `gh search`,
//! GraphQL and future subcommands return author-bearing payloads in shapes this gate
//! can't enumerate. Instead it exposes exactly the one ingestion read the automation
//! needs (`view <n> [--comments]`, a single issue), gated on maintainer authorship.
//!
//! **Fail-closed** everywhere: an issue or comment with a missing, null, empty or
//! non-maintainer author login is DROPPED, never surfaced. A refusal is reported on
//! stderr (the gate never discards silently), but the untrusted content is never
//! printed.
//!
//! The stdout shape is Python's `json.dumps(..., ensure_ascii=False)`, kept
//! byte-identical with the script this replaced.

use std::io::{self, Write};
use std::process::{Command, Output};

use crate::json::Json;

const EXIT_OK: u8 = 0;
pub const EXIT_USAGE: u8 = 2;
const EXIT_REFUSED: u8 = 3;

/// `gh-issue view <n> [--comments]`; prints the gated JSON or refuses.
#[must_use]
pub fn main(args: &[String]) -> u8 {
    let Some((number, comments)) = parse(args) else {
        eprintln!("{}", crate::USAGE);
        return EXIT_USAGE;
    };
    // A failed or unparsable `gh` call exits 2 before any content is read.
    let fetched =
        maintainer_login().and_then(|maintainer| Ok((maintainer, fetch_issue(number, comments)?)));
    let (maintainer, payload) = match fetched {
        Ok(fetched) => fetched,
        Err(message) => {
            eprintln!("{}", message.trim_end());
            return EXIT_USAGE;
        }
    };
    let Some(out) = gate(payload.as_ref(), &maintainer, comments) else {
        eprintln!(
            "gh_issue: issue #{number} is not maintainer-authored; \
             refusing to surface untrusted content"
        );
        return EXIT_REFUSED;
    };
    // A closed stdout (`| head`) is not a gate failure: the verdict was already made.
    let _ = writeln!(io::stdout(), "{}", out.dumps());
    EXIT_OK
}

fn parse(args: &[String]) -> Option<(i64, bool)> {
    let (sub, rest) = args.split_first()?;
    if sub != "view" {
        return None;
    }
    let mut number = None;
    let mut comments = false;
    for arg in rest {
        if arg == "--comments" {
            comments = true;
        } else if number.is_none() {
            number = Some(arg.trim().parse().ok()?);
        } else {
            return None;
        }
    }
    Some((number?, comments))
}

/// The single trusted author: `REGISTRY_MAINTAINER_LOGIN`, else the repo owner.
///
/// The override lets a test or a fork name a different owner without a live
/// `gh repo view`. An empty override is not a login, so it falls back.
fn maintainer_login() -> Result<String, String> {
    if let Some(login) = env_nonempty("REGISTRY_MAINTAINER_LOGIN") {
        return Ok(login);
    }
    let slug = env_nonempty("GITHUB_REPOSITORY").map_or_else(gh_repo_slug, Ok)?;
    let (owner, _name) = slug
        .split_once('/')
        .ok_or_else(|| format!("repository {slug:?} is not owner/name"))?;
    Ok(owner.to_owned())
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn gh_repo_slug() -> Result<String, String> {
    let argv = ["repo", "view", "--json", "nameWithOwner"];
    let out = gh(&argv)?;
    if !out.status.success() {
        return Err(format!(
            "command failed: gh {}\n{}",
            argv.join(" "),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    parse_json(&argv, &out.stdout)?
        .get("nameWithOwner")
        .and_then(Json::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("gh {}: no nameWithOwner", argv.join(" ")))
}

/// The raw `gh issue view` payload, or `None` when `gh` exits non-zero (a missing number).
///
/// A zero exit is not proof of an issue: `gh issue view` resolves a PR number too. That is
/// harmless, because the trust boundary is the author check in [`gate`], not the kind.
fn fetch_issue(number: i64, comments: bool) -> Result<Option<Json>, String> {
    let fields = if comments {
        "number,title,state,body,author,comments"
    } else {
        "number,title,state,body,author"
    };
    let number = number.to_string();
    let argv = ["issue", "view", &number, "--json", fields];
    let out = gh(&argv)?;
    if !out.status.success() {
        return Ok(None);
    }
    parse_json(&argv, &out.stdout).map(Some)
}

fn gh(argv: &[&str]) -> Result<Output, String> {
    Command::new("gh")
        .args(argv)
        .output()
        .map_err(|e| format!("command failed: gh {}\n{e}", argv.join(" ")))
}

fn parse_json(argv: &[&str], stdout: &[u8]) -> Result<Json, String> {
    serde_json::from_slice(stdout).map_err(|e| format!("gh {}: invalid JSON: {e}", argv.join(" ")))
}

/// The gated output object, or `None` to refuse.
///
/// Refuses when the payload is missing or not maintainer-authored. Otherwise returns
/// `{number,title,state,body[,comments]}`, with `comments` (only when requested) filtered
/// to maintainer-authored entries, each passed through unchanged.
fn gate(payload: Option<&Json>, maintainer: &str, comments: bool) -> Option<Json> {
    let data = payload.filter(|d| is_maintainer(d, maintainer))?;
    let mut out: Vec<(String, Json)> = ["number", "title", "state", "body"]
        .into_iter()
        .map(|key| (key.to_owned(), data.get(key).cloned().unwrap_or(Json::Null)))
        .collect();
    if comments {
        let kept = match data.get("comments") {
            Some(Json::Array(raw)) => raw
                .iter()
                .filter(|c| is_maintainer(c, maintainer))
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        out.push(("comments".to_owned(), Json::Array(kept)));
    }
    Some(Json::Object(out))
}

/// The author login of an issue or comment, or `None` (the fail-closed sentinel) when the
/// author or its login is missing, null, not a string or empty.
fn login_of(obj: &Json) -> Option<&str> {
    obj.get("author")?
        .get("login")?
        .as_str()
        .filter(|login| !login.is_empty())
}

/// GitHub logins are ASCII, so an ASCII case-insensitive match is the whole comparison.
fn is_maintainer(obj: &Json, maintainer: &str) -> bool {
    login_of(obj).is_some_and(|login| login.eq_ignore_ascii_case(maintainer))
}
