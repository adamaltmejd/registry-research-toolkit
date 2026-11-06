//! `cargo xtask search-eval [--url URL] [--limit N]`: measure search relevance against
//! the eval set (`crates/xtask/search_eval.toml`, #393 item 10).
//!
//! The eval set is steward-authored `(query -> intended result)` pairs. For each case
//! this asks a running `reg-meta serve` for the first page of the case's arm
//! (`GET /api/search?q=&type=&limit=`) and reports whether the case's `intended` FQID
//! appears in it, and at what rank.
//!
//! This is a maintainer tool, not a test: it needs a server over a real catalog (the
//! synthetic fixtures don't carry catalog-scale content). Start the server first, for
//! example `reg-meta serve --db <dir> --port 8001`. Each request shells out to
//! `curl`, so this crate needs no HTTP client.
//!
//! `expect`:
//! - `hit`: the intended result should rank well today (a regression guard).
//! - `gap`: a confirmed-correct intended that does not surface today: a curation or
//!   concept-group target. A `gap` that becomes a hit is progress (`closed!`), not a
//!   failure.
//!
//! Exit 2 on a usage error or when the server does not answer; 1 on a malformed eval set.

use std::path::Path;
use std::process::Command;

use serde::Deserialize;
use serde_json::Value;

const EXIT_FAIL: u8 = 1;
const EXIT_USAGE: u8 = 2;

const USAGE: &str = "usage: cargo xtask search-eval [--url URL] [--limit N]";

/// A case's `group` is the arm it searches. `value` is deliberately absent: a code hit
/// has no FQID, so a `value` case could never match. The actionable target is always
/// the owning entity (register / variable).
const GROUPS: [&str; 2] = ["register", "variable"];

#[derive(Deserialize)]
struct EvalSet {
    case: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    query: String,
    group: String,
    intended: String,
    expect: String,
}

#[must_use]
pub fn main(args: &[String]) -> u8 {
    let mut url = "http://127.0.0.1:8001".to_owned();
    let mut limit: u32 = 10;
    let mut rest = args.iter();
    while let Some(flag) = rest.next() {
        match (flag.as_str(), rest.next()) {
            ("--url", Some(v)) => v.clone_into(&mut url),
            ("--limit", Some(v)) if let Ok(n) = v.parse() => limit = n,
            _ => {
                eprintln!("{USAGE}");
                return EXIT_USAGE;
            }
        }
    }
    match run(&url, limit) {
        Ok(()) => 0,
        Err((code, message)) => {
            eprintln!("error: {message}");
            code
        }
    }
}

/// Whether a 422 body is `invalid_parameter` on `type`.
fn refused_type(body: &str) -> bool {
    serde_json::from_str::<Value>(body).is_ok_and(|v| {
        v["error"]["code"] == "invalid_parameter" && v["error"]["fields"]["parameter"] == "type"
    })
}

/// The first page of `group`'s arm for `query`: `{items, next_cursor}`.
/// A 422 naming the `type` parameter (a type the API refuses) is a miss, an empty page;
/// any other non-2xx (including a 422 on `limit`) fails the run, so a server error is
/// not read as a ranking regression.
fn page(url: &str, query: &str, group: &str, limit: u32) -> Result<Value, (u8, String)> {
    let out = Command::new("curl")
        .args(["-sS", "--get", "-w", "\n%{http_code}"])
        .args(["--data-urlencode", &format!("q={query}")])
        .args(["--data-urlencode", &format!("type={group}")])
        .args(["--data-urlencode", &format!("limit={limit}")])
        .arg(format!("{url}/api/search"))
        .output()
        .map_err(|e| (EXIT_USAGE, format!("cannot run curl: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err((
            EXIT_USAGE,
            format!("no search answer from {url}: {}", stderr.trim()),
        ));
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let (body, status) = text.rsplit_once('\n').unwrap_or(("", ""));
    if status == "422" && refused_type(body) {
        return Ok(Value::Null);
    }
    if !status.starts_with('2') {
        return Err((
            EXIT_FAIL,
            format!(
                "search answer from {url} was HTTP {status}: {}",
                body.trim()
            ),
        ));
    }
    let mut body: Value = serde_json::from_str(body).map_err(|e| {
        (
            EXIT_FAIL,
            format!("search answer from {url} is not JSON: {e}"),
        )
    })?;
    Ok(body["data"].take())
}

fn run(url: &str, limit: u32) -> Result<(), (u8, String)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("search_eval.toml");
    let text = std::fs::read_to_string(&path)
        .map_err(|e| (EXIT_FAIL, format!("cannot read {}: {e}", path.display())))?;
    let eval: EvalSet =
        toml::from_str(&text).map_err(|e| (EXIT_FAIL, format!("{}: {e}", path.display())))?;

    let mut rows: Vec<[String; 8]> = Vec::new();
    let (mut hit_total, mut hit_found, mut gap_total, mut gap_closed) = (0, 0, 0, 0);
    for c in &eval.case {
        if !GROUPS.contains(&c.group.as_str()) {
            return Err((
                EXIT_FAIL,
                format!(
                    "unsupported eval group {:?}; supported: {}",
                    c.group,
                    GROUPS.join(" | ")
                ),
            ));
        }
        let page = page(url, &c.query, &c.group, limit)?;
        let items = page["items"].as_array().map_or(&[][..], Vec::as_slice);
        let rank = items
            .iter()
            .position(|item| item["fqid"] == c.intended.as_str())
            .map(|i| i + 1);
        let status = match (c.expect.as_str(), rank.is_some()) {
            ("hit", found) => {
                hit_total += 1;
                hit_found += usize::from(found);
                if found { "ok" } else { "MISS" }
            }
            (_, found) => {
                gap_total += 1;
                gap_closed += usize::from(found);
                if found { "closed!" } else { "gap" }
            }
        };
        rows.push([
            c.query.clone(),
            c.group.clone(),
            c.intended.clone(),
            c.expect.clone(),
            rank.map_or_else(|| "-".to_owned(), |r| r.to_string()),
            items.len().to_string(),
            (!page["next_cursor"].is_null()).to_string(),
            status.to_owned(),
        ]);
    }

    let header = [
        "query", "group", "intended", "expect", "rank", "returned", "has_more", "status",
    ]
    .map(str::to_owned);
    // Widths in characters, not bytes: the Swedish queries carry å, ä and ö.
    let width = |i: usize| {
        std::iter::once(&header)
            .chain(&rows)
            .map(|r| r[i].chars().count())
            .max()
            .unwrap_or(0)
    };
    let widths: Vec<usize> = (0..header.len()).map(width).collect();
    let line = |row: &[String; 8]| {
        let cells: Vec<String> = row
            .iter()
            .zip(&widths)
            .map(|(v, &w)| format!("{v:<w$}"))
            .collect();
        cells.join("  ")
    };
    println!("{}", line(&header));
    let dashes: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();
    println!("{}", dashes.join("  "));
    for row in &rows {
        println!("{}", line(row));
    }

    println!();
    println!("hit cases:  {hit_found}/{hit_total} intended result found in top {limit}");
    println!("gap cases:  {gap_closed}/{gap_total} now surfaced (closed)");
    if hit_total > 0 && hit_found < hit_total {
        println!(
            "\nMISS on an `expect=hit` case = a likely relevance REGRESSION (the intended \
             is steward-confirmed) — investigate the ranking change that dropped it."
        );
    }
    Ok(())
}
