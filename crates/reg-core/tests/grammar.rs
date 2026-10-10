//! The FQID and period grammars against `conformance/cases/grammar/` (see its README),
//! plus one seeded round-trip loop per grammar.

mod common;

use std::fs;
use std::path::PathBuf;

use common::Rng;
use reg_core::project::SourcePeriod;
use reg_core::{
    Fqid, GrammarError, Period, PeriodToken, Term, next_iso_day, period_token_for_bounds,
    prev_iso_day, render,
};
use serde_json::{Value, json};

fn read_cases(file: &str) -> Vec<Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/cases/grammar")
        .join(file);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    text.lines()
        .enumerate()
        .map(|(i, line)| {
            serde_json::from_str(line).unwrap_or_else(|e| panic!("{file}:{}: {e}", i + 1))
        })
        .collect()
}

fn fqid_json(f: &Fqid) -> Value {
    match f {
        Fqid::Provider { provider } => json!({"kind": "provider", "provider": provider}),
        Fqid::Register { provider, register } => {
            json!({"kind": "register", "provider": provider, "register": register})
        }
        Fqid::Variable {
            provider,
            register,
            variable,
        } => json!({
            "kind": "variable", "provider": provider, "register": register, "variable": variable,
        }),
        Fqid::Classification { classification } => {
            json!({"kind": "classification", "classification": classification})
        }
    }
}

fn token_json(t: PeriodToken) -> Value {
    match t {
        PeriodToken::Year(year) => json!({"kind": "year", "year": year}),
        PeriodToken::Month { year, month } => {
            json!({"kind": "month", "year": year, "month": month})
        }
        PeriodToken::Day { year, month, day } => {
            json!({"kind": "day", "year": year, "month": month, "day": day})
        }
        PeriodToken::Term { term, year } => {
            let term = match term {
                Term::Ht => "HT",
                Term::Vt => "VT",
            };
            json!({"kind": "term", "term": term, "year": year})
        }
        PeriodToken::SchoolYear(year) => json!({"kind": "school_year", "year": year}),
        PeriodToken::Quarter { year, quarter } => {
            json!({"kind": "quarter", "year": year, "quarter": quarter})
        }
        PeriodToken::Half { year, half } => json!({"kind": "half", "year": year, "half": half}),
    }
}

fn period_json(p: Period) -> Value {
    match p {
        Period::Token(t) => token_json(t),
        Period::Range { from, to } => {
            json!({"kind": "range", "from": token_json(from), "to": token_json(to)})
        }
    }
}

/// Each case's parse against its expected value or error code, and each accepted
/// string's `Display` against the input. `extra` adds per-grammar fields to compare.
fn assert_corpus<T: std::str::FromStr<Err = GrammarError> + ToString>(
    file: &str,
    to_json: impl Fn(&T) -> Value,
    extra: impl Fn(&T, &Value) -> Option<String>,
) {
    let cases = read_cases(file);
    let failures: Vec<String> = cases
        .iter()
        .enumerate()
        .filter_map(|(i, case)| {
            let input = case["in"].as_str().expect("`in` is a string");
            let problem = match (input.parse::<T>(), case.get("error")) {
                (Ok(v), None) if to_json(&v) != case["out"] => Some(format!("got {}", to_json(&v))),
                (Ok(v), None) if v.to_string() != input => {
                    Some(format!("displays as {:?}", v.to_string()))
                }
                (Ok(v), None) => extra(&v, case),
                (Ok(v), Some(_)) => Some(format!("accepted as {}", to_json(&v))),
                (Err(e), Some(code)) if code == e.code() => None,
                (Err(e), _) => Some(format!("refused with {}", e.code())),
            };
            problem.map(|p| format!("{file}:{} {input:?}: {p}", i + 1))
        })
        .collect();
    assert!(cases.len() > 30, "{file}: {} cases", cases.len());
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// Fails if a slug rule, the segment counts or the `class/` discriminator changes, or if
// any accepted FQID has a second spelling.
#[test]
fn fqid_matches_corpus() {
    assert_corpus::<Fqid>("fqid.jsonl", fqid_json, |_, _| None);
}

// Fails if a token form, a bound (year, month, day, quarter, half), the range order rule,
// the year span (`LA2019` touches 2020) or the days a period covers (`2019-02` ends on
// the 28th) change.
#[test]
fn period_matches_corpus() {
    assert_corpus::<Period>(
        "period.jsonl",
        |p| period_json(*p),
        |p, case| {
            let (lo, hi) = p.years();
            let (first, last) = p.iso_bounds();
            let got = json!({"years": [lo, hi], "bounds": [first, last]});
            let expected = json!({"years": case["years"], "bounds": case["bounds"]});
            (got != expected).then(|| format!("{got}"))
        },
    );
}

impl Rng {
    fn small(&mut self, lo: u8, hi: u8) -> u8 {
        u8::try_from(self.range(lo.into(), hi.into())).expect("u8 range")
    }

    fn pick<'a>(&mut self, items: &'a [u8]) -> &'a u8 {
        &items[usize::from(self.range(0, u16::try_from(items.len() - 1).expect("short")))]
    }

    /// A slug: alphanumeric runs joined by single hyphens, starting with a letter.
    fn slug(&mut self) -> String {
        const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
        const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        loop {
            let mut s = String::from(char::from(*self.pick(LETTERS)));
            for _ in 0..self.range(0, 10) {
                if self.range(0, 4) == 0 {
                    s.push('-');
                }
                s.push(char::from(*self.pick(ALNUM)));
            }
            // Reserved: `class` everywhere, `group` as a first segment.
            if s != "class" && s != "group" {
                return s;
            }
        }
    }

    fn token(&mut self, year: u16) -> PeriodToken {
        match self.range(0, 6) {
            0 => PeriodToken::Year(year),
            1 => PeriodToken::Month {
                year,
                month: self.small(1, 12),
            },
            2 => {
                let month = self.small(1, 12);
                let leap = year.is_multiple_of(4)
                    && (!year.is_multiple_of(100) || year.is_multiple_of(400));
                let last = match month {
                    2 if leap => 29,
                    2 => 28,
                    4 | 6 | 9 | 11 => 30,
                    _ => 31,
                };
                PeriodToken::Day {
                    year,
                    month,
                    day: self.small(1, last),
                }
            }
            3 => PeriodToken::Term {
                term: if self.range(0, 1) == 0 {
                    Term::Ht
                } else {
                    Term::Vt
                },
                year,
            },
            4 => PeriodToken::SchoolYear(year),
            5 => PeriodToken::Quarter {
                year,
                quarter: self.small(1, 4),
            },
            _ => PeriodToken::Half {
                year,
                half: self.small(1, 2),
            },
        }
    }
}

const ROUND_TRIPS: usize = 20_000;

// Fails if `Display` and `FromStr` disagree on any generated FQID: a slug the parser
// refuses, or a kind rendered with the wrong segments.
#[test]
fn fqid_round_trips() {
    let mut rng = Rng(0x5EED_F01D);
    for _ in 0..ROUND_TRIPS {
        let fqid = match rng.range(0, 3) {
            0 => Fqid::Provider {
                provider: rng.slug(),
            },
            1 => Fqid::Register {
                provider: rng.slug(),
                register: rng.slug(),
            },
            2 => Fqid::Variable {
                provider: rng.slug(),
                register: rng.slug(),
                variable: rng.slug(),
            },
            _ => Fqid::Classification {
                classification: rng.slug(),
            },
        };
        assert_eq!(fqid.to_string().parse(), Ok(fqid.clone()), "{fqid}");
    }
}

// Fails if `Display` and `FromStr` disagree on any generated period: a token rendered
// without zero padding, a calendar day refused, or an ordered range refused.
#[test]
fn period_round_trips() {
    let mut rng = Rng(0x5EED_DA7E);
    for _ in 0..ROUND_TRIPS {
        let period = if rng.range(0, 1) == 0 {
            let year = rng.range(1900, 2099);
            Period::Token(rng.token(year))
        } else {
            // A `from` year before the `to` year orders the range for every token form.
            let from_year = rng.range(1900, 2098);
            let to_year = rng.range(from_year + 1, 2099);
            Period::Range {
                from: rng.token(from_year),
                to: rng.token(to_year),
            }
        };
        assert_eq!(period.to_string().parse(), Ok(period), "{period}");
    }
}

// Fails if `period_token_for_bounds` stops inverting `iso_bounds`: a token whose own
// bounds render as something else (a half year renders as its term, the one spelling
// the reader emits).
#[test]
fn period_token_inverts_bounds() {
    let mut rng = Rng(0x7012_E4B5);
    for _ in 0..ROUND_TRIPS {
        let year = rng.range(1900, 2099);
        let token = rng.token(year);
        let (lo, hi) = Period::Token(token).iso_bounds();
        let expected = match token {
            PeriodToken::Half { year, half: 1 } => format!("VT{year}"),
            PeriodToken::Half { year, .. } => format!("HT{year}"),
            other => other.to_string(),
        };
        assert_eq!(period_token_for_bounds(&lo, &hi), expected, "{token}");
    }
}

// Fails if a window no token spans is rounded to one, the day after a month's end
// (or the open-ended sentinel) is miscounted, or a calendar-impossible day
// (`2018-02-29`) is read as a date: it ends no token and has no next or previous day.
#[test]
fn period_token_edges_and_next_day() {
    for (lo, hi, token) in [
        ("2018-02-01", "2018-02-28", "2018-02"),
        ("2018-02-01", "2018-02-29", "2018-02-01..2018-02-29"),
        ("2020-02-01", "2020-02-29", "2020-02"),
        ("2018-03-01", "2018-04-15", "2018-03-01..2018-04-15"),
        ("2018-01-01", "2019-12-31", "2018-01-01..2019-12-31"),
        ("1850-01-01", "1850-12-31", "1850"),
        ("1850-03-04", "1850-03-04", "1850-03-04..1850-03-04"),
    ] {
        assert_eq!(period_token_for_bounds(lo, hi), token, "{lo}..{hi}");
    }
    for (day, next) in [
        ("2018-02-28", "2018-03-01"),
        ("2018-02-29", "2018-02-29"),
        ("2020-02-28", "2020-02-29"),
        ("2018-12-31", "2019-01-01"),
        ("9999-12-31", "9999-12-31"),
    ] {
        assert_eq!(next_iso_day(day), next, "{day}");
    }
    assert_eq!(prev_iso_day("2018-02-29"), "2018-02-29");
}

// Fails if the wire shaping (`from_wire`, `to_wire`), the one-period structural check,
// the days a source period requests, its year spans or their render change: a list
// member read as a range, a grammar year left a string, an unsorted list accepted,
// day-adjacent members left apart.
#[test]
fn source_period_matches_corpus() {
    let cases = read_cases("source_period.jsonl");
    let failures: Vec<String> = cases
        .iter()
        .enumerate()
        .filter_map(|(i, case)| {
            let period = &case["period"];
            let mut got = serde_json::Map::new();
            got.insert("period".into(), period.clone());
            if let Some(wire) = case.get("from_wire") {
                let shaped = SourcePeriod::from_wire(wire.as_str().expect("a wire string"));
                got.insert("from_wire".into(), json!(shaped));
            }
            let wire = serde_json::from_value::<SourcePeriod>(period.clone())
                .ok()
                .and_then(|p| p.to_wire());
            got.insert("wire".into(), json!(wire));
            match SourcePeriod::from_value(period).map(|p| (p.intervals(), p.year_spans())) {
                Ok((Ok(intervals), years)) => {
                    if let Some(days) = &intervals {
                        got.insert("render".into(), json!(render(days)));
                    }
                    got.insert("intervals".into(), json!(intervals));
                    got.insert("years".into(), json!(years));
                }
                Err(_) | Ok((Err(_), _)) => {
                    got.insert("error".into(), json!("invalid_period"));
                }
            }
            // `from_wire` is the wire that shapes into `period`.
            let mut expected = case.as_object().expect("a case object").clone();
            expected.remove("note");
            if let Some(wire) = expected.get_mut("from_wire") {
                *wire = period.clone();
            }
            let got = Value::Object(got);
            (got != Value::Object(expected)).then(|| format!("line {}: got {got}", i + 1))
        })
        .collect();
    assert!(cases.len() > 30, "{} cases", cases.len());
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
