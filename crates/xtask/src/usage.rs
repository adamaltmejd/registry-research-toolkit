//! `cargo xtask usage <report>`: the usage statistics the edge workers record in
//! Workers Analytics Engine (#1323, `reg_webapp/DESIGN.md` → "Usage recording"), read
//! with its SQL API. For the maintainer only.
//!
//! ```text
//! cargo xtask usage volume|top|zero|perf [--days N] [--steward S] [--include-ci] [--sql]
//! cargo xtask usage rollup --month YYYY-MM [--sql]
//! ```
//!
//! - `volume`: requests per day, steward, surface and operation.
//! - `top`: the most opened (`show`) and picked variable ids.
//! - `zero`: search text that found nothing, with how often.
//! - `perf`: per operation, the request count, the edge cache hit rate, edge and app
//!   p50/p95 in ms, then refusals and 5xx answers by code.
//! - `rollup`: one month's counts as JSON on stdout: requests per steward, surface and
//!   operation, and the 200 most opened or picked ids. No search text. This is what the
//!   monthly job in the private inputs repository commits, since Analytics Engine keeps
//!   only 3 months.
//!
//! `--days` defaults to 7 (Analytics Engine keeps 90). CI smoke traffic (client `ci`)
//! is left out unless `--include-ci`. `--sql` prints the queries instead of running
//! them. The account and token come from `CF_ACCOUNT_ID` and `CF_ANALYTICS_TOKEN` (an
//! API token with Account Analytics Read). The query goes through `curl`, with the token
//! in its config on stdin, never in an argument.
//!
//! Every count is `SUM(_sample_interval)`: Analytics Engine samples, and a row stands
//! for that many requests.
//!
//! The columns, in the order `reg_webapp/edge/src/usage.ts` writes them: blob1
//! steward, blob2 surface, blob3 kind (`req` or `pick`), blob4 op, blob5 fqids, blob6
//! q, blob7 err, blob8 cache status, blob9 client, blob10 country, blob11 status;
//! double1 edge ms, double2 app ms, double3 bytes, double4 hits (`-1`: not measured).

use std::fmt::Write as _;
use std::io::Write as _;
use std::process::{Command, Stdio};

use serde_json::Value;

const USAGE: &str =
    "usage: cargo xtask usage volume|top|zero|perf [--days N] [--steward S] [--include-ci] [--sql]
       cargo xtask usage rollup --month YYYY-MM [--sql]";
const EXIT_USAGE: u8 = 2;
const EXIT_FAILED: u8 = 1;
const DATASET: &str = "reg_meta_usage";
const MAX_DAYS: u32 = 90;

struct Args {
    report: String,
    days: u32,
    steward: Option<String>,
    include_ci: bool,
    month: Option<(u32, u32)>,
    sql: bool,
}

/// # Panics
///
/// Never on input: a rollup always runs its two queries.
#[must_use]
pub fn main(args: &[String]) -> u8 {
    let args = match parse(args) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}\n{USAGE}");
            return EXIT_USAGE;
        }
    };
    let queries = match queries(&args) {
        Ok(queries) => queries,
        Err(message) => {
            eprintln!("{message}\n{USAGE}");
            return EXIT_USAGE;
        }
    };
    if args.sql {
        for (title, sql) in &queries {
            println!("-- {title}\n{sql};\n");
        }
        return 0;
    }
    let mut results = Vec::new();
    for (title, sql) in &queries {
        match run(sql) {
            Ok(rows) => results.push((title, rows)),
            Err(message) => {
                eprintln!("usage {}: {message}", args.report);
                return EXIT_FAILED;
            }
        }
    }
    if let Some((year, month)) = args.month {
        let [(_, requests), (_, top)] = &results[..] else {
            unreachable!("a rollup runs two queries")
        };
        // An empty month is past retention or not recorded yet; committing it would
        // read as a month without use.
        if requests.is_empty() {
            eprintln!("usage rollup: no requests recorded for that month");
            return EXIT_FAILED;
        }
        let rollup = serde_json::json!({
            "month": format!("{year:04}-{month:02}"),
            "requests": requests,
            "top": top,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&rollup).expect("JSON serializes")
        );
    } else {
        for (title, rows) in &results {
            println!("{title}");
            print_table(rows);
            println!();
        }
    }
    0
}

fn parse(args: &[String]) -> Result<Args, String> {
    let (report, rest) = args.split_first().ok_or("no report named")?;
    let mut parsed = Args {
        report: report.clone(),
        days: 7,
        steward: None,
        include_ci: false,
        month: None,
        sql: false,
    };
    let mut rest = rest.iter();
    while let Some(flag) = rest.next() {
        let mut value = || rest.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--days" => {
                parsed.days = value()?
                    .parse()
                    .ok()
                    .filter(|days| (1..=MAX_DAYS).contains(days))
                    .ok_or(format!("--days is 1 to {MAX_DAYS}"))?;
            }
            // Spliced into SQL, so only the steward slug's characters.
            "--steward" => {
                let steward = value()?;
                if steward.is_empty()
                    || !steward
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                {
                    return Err(format!("--steward {steward:?} is not a steward slug"));
                }
                parsed.steward = Some(steward.clone());
            }
            "--include-ci" => parsed.include_ci = true,
            "--month" => parsed.month = Some(month(value()?)?),
            "--sql" => parsed.sql = true,
            _ => return Err(format!("unknown argument {flag:?}")),
        }
    }
    Ok(parsed)
}

/// `YYYY-MM` as (year, month).
fn month(text: &str) -> Result<(u32, u32), String> {
    let refused = || format!("--month {text:?} is not YYYY-MM");
    let (year, month) = text.split_once('-').ok_or_else(refused)?;
    if year.len() != 4 || month.len() != 2 {
        return Err(refused());
    }
    let year: u32 = year.parse().map_err(|_| refused())?;
    let month: u32 = month.parse().map_err(|_| refused())?;
    if !(1..=12).contains(&month) {
        return Err(refused());
    }
    Ok((year, month))
}

/// The report's queries, each with its title.
fn queries(args: &Args) -> Result<Vec<(&'static str, String)>, String> {
    let mut filter = match args.month {
        Some((year, month)) => {
            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            format!(
                "timestamp >= toDateTime('{year:04}-{month:02}-01 00:00:00') \
                 AND timestamp < toDateTime('{next_year:04}-{next_month:02}-01 00:00:00')"
            )
        }
        None => format!("timestamp > NOW() - INTERVAL '{}' DAY", args.days),
    };
    if !args.include_ci {
        filter.push_str(" AND blob9 != 'ci'");
    }
    if let Some(steward) = &args.steward {
        write!(filter, " AND blob1 = '{steward}'").expect("writing to a String");
    }
    let picked = "blob5 != '' AND (blob3 = 'pick' OR (blob3 = 'req' AND blob4 = 'show'))";
    let top = |limit: u32| {
        format!(
            "SELECT blob5 AS fqid, if(blob3 = 'pick', 'pick', 'open') AS kind, blob2 AS surface, \
             SUM(_sample_interval) AS count FROM {DATASET} WHERE {filter} AND {picked} \
             GROUP BY fqid, kind, surface ORDER BY count DESC, fqid, kind, surface LIMIT {limit}"
        )
    };
    let requests = |by_day: bool| {
        let day = if by_day {
            "toStartOfDay(timestamp) AS day, "
        } else {
            ""
        };
        let day_key = if by_day { "day, " } else { "" };
        format!(
            "SELECT {day}blob1 AS steward, blob2 AS surface, blob4 AS op, \
             SUM(_sample_interval) AS requests FROM {DATASET} WHERE {filter} AND blob3 = 'req' \
             GROUP BY {day_key}steward, surface, op \
             ORDER BY {day_key}requests DESC, steward, surface, op"
        )
    };
    if args.month.is_some() != (args.report == "rollup") {
        return Err("--month goes with rollup, and rollup needs --month".to_owned());
    }
    Ok(match args.report.as_str() {
        "volume" => vec![("Requests per day", requests(true))],
        "top" => vec![("Most opened and picked", top(50))],
        "zero" => vec![(
            "Searches that found nothing",
            format!(
                "SELECT blob6 AS q, blob2 AS surface, SUM(_sample_interval) AS searches \
                 FROM {DATASET} WHERE {filter} AND blob3 = 'req' AND double4 = 0 AND blob6 != '' \
                 GROUP BY q, surface ORDER BY searches DESC, q LIMIT 100"
            ),
        )],
        "perf" => vec![
            (
                "Requests, cache hit rate and edge ms",
                format!(
                    "SELECT blob1 AS steward, blob4 AS op, SUM(_sample_interval) AS requests, \
                     sumIf(_sample_interval, blob8 = 'HIT') / SUM(_sample_interval) AS hit_rate, \
                     quantileExactWeighted(0.5)(double1, _sample_interval) AS edge_p50, \
                     quantileExactWeighted(0.95)(double1, _sample_interval) AS edge_p95 \
                     FROM {DATASET} WHERE {filter} AND blob3 = 'req' \
                     GROUP BY steward, op ORDER BY requests DESC"
                ),
            ),
            (
                "App ms (answered by the origin)",
                format!(
                    "SELECT blob1 AS steward, blob4 AS op, SUM(_sample_interval) AS requests, \
                     quantileExactWeighted(0.5)(double2, _sample_interval) AS app_p50, \
                     quantileExactWeighted(0.95)(double2, _sample_interval) AS app_p95 \
                     FROM {DATASET} WHERE {filter} AND blob3 = 'req' AND double2 >= 0 \
                     GROUP BY steward, op ORDER BY requests DESC"
                ),
            ),
            (
                "Refusals and 5xx",
                format!(
                    "SELECT blob1 AS steward, blob4 AS op, blob7 AS err, blob11 AS status, \
                     SUM(_sample_interval) AS requests FROM {DATASET} \
                     WHERE {filter} AND blob3 = 'req' AND (blob7 != '' OR blob11 LIKE '5%') \
                     GROUP BY steward, op, err, status ORDER BY requests DESC"
                ),
            ),
        ],
        "rollup" => vec![("requests", requests(false)), ("top", top(200))],
        other => return Err(format!("unknown report {other:?}")),
    })
}

/// Run `sql` on the Analytics Engine SQL API: the `data` rows.
fn run(sql: &str) -> Result<Vec<Value>, String> {
    let env = |name: &str| std::env::var(name).map_err(|_| format!("{name} is not set"));
    let account = env("CF_ACCOUNT_ID")?;
    let token = env("CF_ANALYTICS_TOKEN")?;
    let token = token.trim();
    if account.is_empty() || !account.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("CF_ACCOUNT_ID is not an account id".to_owned());
    }
    if token.is_empty() || token.chars().any(char::is_control) {
        return Err("CF_ANALYTICS_TOKEN is empty or holds a control character".to_owned());
    }
    let quote = |text: &str| text.replace('\\', "\\\\").replace('"', "\\\"");
    let config = format!(
        "header = \"Authorization: Bearer {}\"\ndata-binary = \"{} FORMAT JSON\"\n",
        quote(token),
        quote(&sql.replace('\n', " "))
    );
    let mut child = Command::new("curl")
        .args(["-sS", "--fail-with-body", "-K", "-"])
        .arg(format!(
            "https://api.cloudflare.com/client/v4/accounts/{account}/analytics_engine/sql"
        ))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("curl: {e}"))?;
    // A curl that exits early closes its stdin; its own message below says why.
    let _ = child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(config.as_bytes());
    let out = child.wait_with_output().map_err(|e| format!("curl: {e}"))?;
    let body = String::from_utf8_lossy(&out.stdout);
    if !out.status.success() {
        return Err(format!(
            "{} {}",
            String::from_utf8_lossy(&out.stderr).trim(),
            body.trim()
        ));
    }
    let document: Value =
        serde_json::from_str(&body).map_err(|e| format!("not JSON ({e}): {body}"))?;
    document["data"]
        .as_array()
        .cloned()
        .ok_or_else(|| format!("no data rows: {body}"))
}

/// `rows` as an aligned table, columns in the order of the first row.
fn print_table(rows: &[Value]) {
    let Some(first) = rows.first().and_then(Value::as_object) else {
        println!("  (none)");
        return;
    };
    let columns: Vec<&String> = first.keys().collect();
    let cell = |row: &Value, column: &str| match &row[column] {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    };
    let widths: Vec<usize> = columns
        .iter()
        .map(|column| {
            rows.iter()
                .map(|row| cell(row, column).chars().count())
                .chain([column.chars().count()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let line = |cells: Vec<String>| {
        let padded: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(text, width)| format!("{text:<width$}"))
            .collect();
        println!("  {}", padded.join("  ").trim_end());
    };
    line(columns.iter().map(|c| (*c).clone()).collect());
    for row in rows {
        line(columns.iter().map(|column| cell(row, column)).collect());
    }
}
