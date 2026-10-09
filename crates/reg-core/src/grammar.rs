//! The FQID and period grammars (`RUST_RUNTIME_SPEC.md` sections 5 and 7).
//!
//! FQIDs follow `reg_meta/DESIGN.md` "FQID grammar"; periods take the seven token forms
//! listed there plus a `from..to` range. Each grammar has exactly one spelling per
//! value, so `Display` gives back the parsed string. The oracle is
//! `conformance/cases/grammar/`.

use std::fmt;
use std::str::FromStr;

/// A grammar refusal, carrying the error catalog code (`conformance/api/errors.toml`)
/// the caller reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrammarError {
    InvalidRef,
    InvalidPeriod,
}

impl GrammarError {
    /// The error catalog code: `invalid_ref` or `invalid_period`.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidRef => "invalid_ref",
            Self::InvalidPeriod => "invalid_period",
        }
    }
}

impl fmt::Display for GrammarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRef => {
                "not an FQID: provider, provider/register, provider/register/variable or \
                 class/classification, each slug lowercase ASCII kebab-case"
            }
            Self::InvalidPeriod => {
                "not a period: YYYY, YYYY-MM, YYYY-MM-DD, HTYYYY, VTYYYY, LAYYYY, YYYY-Q[1-4], \
                 YYYY-H[12] (years 1900-2099), or from..to with from not after to"
            }
        })
    }
}

impl std::error::Error for GrammarError {}

/// The discriminator of classification FQIDs, reserved as a slug everywhere.
const CLASSIFICATION_PREFIX: &str = "class";

/// The first segment of group refs (`conformance/api/operations.toml`, `show`), so no
/// provider may take it.
const GROUP_PREFIX: &str = "group";

/// A fully qualified identifier. The kind follows from the segment count and the
/// leading `class/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fqid {
    Provider {
        provider: String,
    },
    Register {
        provider: String,
        register: String,
    },
    Variable {
        provider: String,
        register: String,
        variable: String,
    },
    Classification {
        classification: String,
    },
}

/// The slug grammar: `^[a-z](?:-?[a-z0-9])*$` and not `class`.
#[must_use]
pub fn is_slug(s: &str) -> bool {
    let b = s.as_bytes();
    s != CLASSIFICATION_PREFIX
        && b.first().is_some_and(u8::is_ascii_lowercase)
        && b.last() != Some(&b'-')
        && !s.contains("--")
        && b.iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}

impl FromStr for Fqid {
    type Err = GrammarError;

    fn from_str(s: &str) -> Result<Self, GrammarError> {
        let segs: Vec<&str> = s.split('/').collect();
        if segs[0] == CLASSIFICATION_PREFIX {
            return match segs[..] {
                [_, slug] if is_slug(slug) => Ok(Self::Classification {
                    classification: slug.to_owned(),
                }),
                _ => Err(GrammarError::InvalidRef),
            };
        }
        if segs[0] == GROUP_PREFIX || !segs.iter().all(|seg| is_slug(seg)) {
            return Err(GrammarError::InvalidRef);
        }
        match segs[..] {
            [p] => Ok(Self::Provider {
                provider: p.to_owned(),
            }),
            [p, r] => Ok(Self::Register {
                provider: p.to_owned(),
                register: r.to_owned(),
            }),
            [p, r, v] => Ok(Self::Variable {
                provider: p.to_owned(),
                register: r.to_owned(),
                variable: v.to_owned(),
            }),
            _ => Err(GrammarError::InvalidRef),
        }
    }
}

impl fmt::Display for Fqid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider { provider } => write!(f, "{provider}"),
            Self::Register { provider, register } => write!(f, "{provider}/{register}"),
            Self::Variable {
                provider,
                register,
                variable,
            } => write!(f, "{provider}/{register}/{variable}"),
            Self::Classification { classification } => {
                write!(f, "{CLASSIFICATION_PREFIX}/{classification}")
            }
        }
    }
}

/// A Swedish school term: `HT` (autumn, July to December) or `VT` (spring, January to
/// June).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Term {
    Ht,
    Vt,
}

/// One period token. Years are 1900..=2099; months, quarters, halves and days are
/// calendar-valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodToken {
    /// `YYYY`
    Year(u16),
    /// `YYYY-MM`
    Month { year: u16, month: u8 },
    /// `YYYY-MM-DD`
    Day { year: u16, month: u8, day: u8 },
    /// `HTYYYY`, `VTYYYY`
    Term { term: Term, year: u16 },
    /// `LAYYYY`: July `YYYY` through June `YYYY+1`.
    SchoolYear(u16),
    /// `YYYY-Q[1-4]`
    Quarter { year: u16, quarter: u8 },
    /// `YYYY-H[12]`
    Half { year: u16, half: u8 },
}

/// A `period` parameter: one token, or an inclusive `from..to` range whose start is
/// not after its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Period {
    Token(PeriodToken),
    Range { from: PeriodToken, to: PeriodToken },
}

pub(crate) type Date = (u16, u8, u8);

fn last_day(year: u16, month: u8) -> u8 {
    match month {
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Exactly `n` ASCII digits.
fn digits(s: &str, n: usize) -> Option<u16> {
    (s.len() == n && s.bytes().all(|b| b.is_ascii_digit()))
        .then(|| s.parse().ok())
        .flatten()
}

fn year(s: &str) -> Option<u16> {
    digits(s, 4).filter(|y| (1900..=2099).contains(y))
}

/// Exactly `n` ASCII digits with a value in `1..=max`.
fn ordinal(s: &str, n: usize, max: u8) -> Option<u8> {
    digits(s, n)
        .and_then(|v| u8::try_from(v).ok())
        .filter(|v| (1..=max).contains(v))
}

impl PeriodToken {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        if let Some(y) = s.strip_prefix("LA") {
            return year(y).map(Self::SchoolYear);
        }
        for (prefix, term) in [("HT", Term::Ht), ("VT", Term::Vt)] {
            if let Some(y) = s.strip_prefix(prefix) {
                return year(y).map(|year| Self::Term { term, year });
            }
        }
        let y = year(s.get(..4)?)?;
        let rest = &s[4..];
        if rest.is_empty() {
            return Some(Self::Year(y));
        }
        let rest = rest.strip_prefix('-')?;
        if let Some(q) = rest.strip_prefix('Q') {
            return ordinal(q, 1, 4).map(|quarter| Self::Quarter { year: y, quarter });
        }
        if let Some(h) = rest.strip_prefix('H') {
            return ordinal(h, 1, 2).map(|half| Self::Half { year: y, half });
        }
        let month = ordinal(rest.get(..2)?, 2, 12)?;
        match &rest[2..] {
            "" => Some(Self::Month { year: y, month }),
            d => {
                let day = ordinal(d.strip_prefix('-')?, 2, last_day(y, month))?;
                Some(Self::Day {
                    year: y,
                    month,
                    day,
                })
            }
        }
    }

    /// The first and last day the token covers.
    pub(crate) fn bounds(self) -> (Date, Date) {
        let months = |y: u16, lo: u8, hi: u8| ((y, lo, 1), (y, hi, last_day(y, hi)));
        match self {
            Self::Year(y) => months(y, 1, 12),
            Self::Month { year, month } => months(year, month, month),
            Self::Day { year, month, day } => ((year, month, day), (year, month, day)),
            Self::Term {
                term: Term::Vt,
                year,
            } => months(year, 1, 6),
            Self::Term {
                term: Term::Ht,
                year,
            } => months(year, 7, 12),
            Self::SchoolYear(y) => ((y, 7, 1), (y + 1, 6, 30)),
            Self::Quarter { year, quarter } => months(year, 3 * quarter - 2, 3 * quarter),
            Self::Half { year, half } => months(year, 6 * half - 5, 6 * half),
        }
    }
}

impl Period {
    /// The calendar years the period touches, inclusive: `LA2019` is `(2019, 2020)`, a
    /// month or day covers its own year.
    #[must_use]
    pub fn years(self) -> (u16, u16) {
        let (from, to) = match self {
            Self::Token(t) => (t, t),
            Self::Range { from, to } => (from, to),
        };
        (from.bounds().0.0, to.bounds().1.0)
    }

    /// The first and last day the period covers, as ISO dates (`2019-02` is
    /// `2019-02-01` to `2019-02-28`).
    #[must_use]
    pub fn iso_bounds(self) -> (String, String) {
        let (from, to) = match self {
            Self::Token(t) => (t, t),
            Self::Range { from, to } => (from, to),
        };
        (iso(from.bounds().0), iso(to.bounds().1))
    }
}

fn iso((y, m, d): Date) -> String {
    format!("{y:04}-{m:02}-{d:02}")
}

/// `YYYY-MM-DD` as a date, without checking the day against its month (stored
/// bounds may carry a synthesized `YYYY-02-29`).
fn iso_date(text: &str) -> Option<Date> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year = digits(&text[..4], 4)?;
    let month = u8::try_from(digits(&text[5..7], 2)?).ok()?;
    let day = u8::try_from(digits(&text[8..], 2)?).ok()?;
    Some((year, month, day))
}

/// The day after an inclusive ISO upper bound; a day past its month's end (a
/// synthesized non-leap `YYYY-02-29`) counts as that month's end. The open-ended
/// `9999-12-31` and an unreadable date are returned as is. Today's
/// `reg_meta.inventory._next_day`.
#[must_use]
pub fn next_iso_day(s: &str) -> String {
    match iso_date(s) {
        Some((9999, 12, 31)) | None => s.to_owned(),
        Some((y, 12, d)) if d >= 31 => iso((y + 1, 1, 1)),
        Some((y, m, d)) if d >= last_day(y, m) => iso((y, m + 1, 1)),
        Some((y, m, d)) => iso((y, m, d + 1)),
    }
}

/// The day before an ISO date, snapped first ([`snap_month_end`]); an unreadable
/// date is returned as is. Today's `reg_meta.order._prev_day`.
#[must_use]
pub fn prev_iso_day(s: &str) -> String {
    match iso_date(&snap_month_end(s)) {
        None => s.to_owned(),
        Some((y, 1, 1)) => iso((y.saturating_sub(1), 12, 31)),
        Some((y, m, 1)) => iso((y, m - 1, last_day(y, m - 1))),
        Some((y, m, d)) => iso((y, m, d - 1)),
    }
}

/// An ISO date past its month's end (a stored, synthesized non-leap `YYYY-02-29`) as
/// that month's last day; any other string as is. Today's
/// `reg_meta.fqid.snap_to_real_month_end`.
#[must_use]
pub fn snap_month_end(s: &str) -> String {
    match iso_date(s) {
        Some((y, m, d)) if (1..=12).contains(&m) && d > last_day(y, m) => {
            iso((y, m, last_day(y, m)))
        }
        _ => s.to_owned(),
    }
}

/// The coarsest period token whose bounds are exactly `lo..hi` (ISO dates), else the
/// explicit `lo..hi`; today's `reg_meta.fqid.period_token_for_bounds`. A term wins
/// over the half-year it equals. As there, only the school year and the day are
/// held to the grammar's years; a synthesized non-leap `YYYY-02-29` end counts as
/// February's end.
///
/// Today's reader ends every February on the 29th, so it renders a non-leap
/// February window as `YYYY-02-01..YYYY-02-28`; this renders `YYYY-02`, the token
/// the grammar expands to exactly those bounds (a Rust-only fix, stage 3b–3e
/// decision 6).
#[must_use]
pub fn period_token_for_bounds(lo: &str, hi: &str) -> String {
    let explicit = || format!("{lo}..{hi}");
    let (Some(l), Some(h)) = (iso_date(lo), iso_date(hi)) else {
        return explicit();
    };
    let h = (h.0, h.1, h.2.min(last_day(h.0, h.1.clamp(1, 12))));
    let (y, in_grammar) = (l.0, (1900..=2099).contains(&l.0));
    let mut candidates = Vec::new();
    if in_grammar {
        candidates.push(PeriodToken::SchoolYear(y));
    }
    if h.0 == y && (1..=12).contains(&l.1) {
        candidates.extend([
            PeriodToken::Year(y),
            PeriodToken::Term {
                term: Term::Vt,
                year: y,
            },
            PeriodToken::Term {
                term: Term::Ht,
                year: y,
            },
            PeriodToken::Month {
                year: y,
                month: l.1,
            },
        ]);
        candidates.extend((1..=4).map(|quarter| PeriodToken::Quarter { year: y, quarter }));
    }
    if l == h && in_grammar {
        candidates.push(PeriodToken::Day {
            year: y,
            month: l.1,
            day: l.2,
        });
    }
    candidates
        .into_iter()
        .find(|t| t.bounds() == (l, h))
        .map_or_else(explicit, |t| t.to_string())
}

impl FromStr for Period {
    type Err = GrammarError;

    fn from_str(s: &str) -> Result<Self, GrammarError> {
        let token = |t: &str| PeriodToken::parse(t).ok_or(GrammarError::InvalidPeriod);
        let Some((from, to)) = s.split_once("..") else {
            return token(s).map(Self::Token);
        };
        let (from, to) = (token(from)?, token(to)?);
        if from.bounds().0 > to.bounds().1 {
            return Err(GrammarError::InvalidPeriod);
        }
        Ok(Self::Range { from, to })
    }
}

impl fmt::Display for PeriodToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Year(y) => write!(f, "{y}"),
            Self::Month { year, month } => write!(f, "{year}-{month:02}"),
            Self::Day { year, month, day } => write!(f, "{year}-{month:02}-{day:02}"),
            Self::Term {
                term: Term::Ht,
                year,
            } => write!(f, "HT{year}"),
            Self::Term {
                term: Term::Vt,
                year,
            } => write!(f, "VT{year}"),
            Self::SchoolYear(y) => write!(f, "LA{y}"),
            Self::Quarter { year, quarter } => write!(f, "{year}-Q{quarter}"),
            Self::Half { year, half } => write!(f, "{year}-H{half}"),
        }
    }
}

impl fmt::Display for Period {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token(t) => write!(f, "{t}"),
            Self::Range { from, to } => write!(f, "{from}..{to}"),
        }
    }
}
