//! The delivery-name grammar: a stated delivery name (`2019, slutlig version`,
//! `Läsåret 2013/2014`, `2024-2070 huvudalternativ`) parsed into a period, an edition
//! and a free-text qualifier (`crates/DESIGN.md`, "Time"). The vocabulary and the
//! forms are reader config; the code is a tokenizer and a form matcher.
//!
//! A name is tokenized into numbers, words and separators (`-`, `/`, `,`, `:`, `(`,
//! `)`, `.`; dash runs and en or em dashes are one `-`), case folded. A trailing edition
//! phrase is taken off first. The rest must match one form exactly, token for token;
//! failing that, a form followed by words (`2019, sociala ersättningar`) or preceded by
//! words without a number (`Strandlinje 2024`) leaves those words as the qualifier.
//! Qualifier words must be separated from the period by a space or a separator, so
//! `fob1960` and `1990_old` stay unparsed. A name no form places is unparsed.
//!
//! A form is a pattern of literal tokens and slots: `{year}`, `{year_to}` (four digits),
//! `{yy}` (two digits), `{mm}`, `{mm_to}` (a month number), `{dd}`, `{dd_to}` (a day of
//! the month), `{q}`, `{q_to}` (a quarter), `{month}` (a month word) and `{term}`,
//! `{term_to}` (a term word), and the kind of period it states.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::month_end;

/// The grammar's config: `[names]` in a reader's config file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grammar {
    /// Trailing phrase → `final` or `preliminary`.
    editions: BTreeMap<String, String>,
    /// Month word → month number.
    months: BTreeMap<String, u32>,
    /// Term word → `spring`, `summer` or `autumn`.
    terms: BTreeMap<String, String>,
    forms: Vec<Form>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Form {
    pattern: String,
    period: Kind,
}

/// What a form states, read from its slots.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Kind {
    /// `{year}`.
    Year,
    /// `{year}` to `{year_to}`.
    Years,
    /// `{year}`, `{mm}` or `{month}`, `{dd}`.
    Day,
    /// From `{year}-{mm}-{dd}` to `{year_to}-{mm_to}-{dd_to}`.
    Days,
    /// `{year}`, `{mm}` or `{month}`.
    Month,
    /// From `{year}-{mm}` to `{year_to}-{mm_to}`.
    Months,
    /// `{year}` with `{yy}` or `{year_to}` the next year: 1 July to 30 June.
    SchoolYear,
    /// `{term}` of `{year}`.
    Term,
    /// From `{term}` of `{year}` to `{term_to}` of `{year_to}`.
    Terms,
    /// January to June of `{year}`.
    FirstHalf,
    /// July to December of `{year}`.
    SecondHalf,
    /// `{q}` of `{year}`, through `{q_to}` when stated.
    Quarter,
}

/// A parsed name. `period` is `None` when the grammar cannot place the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed {
    /// Inclusive ISO days and the display grain.
    pub period: Option<(String, String, &'static str)>,
    pub edition: &'static str,
    pub qualifier: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Num,
    Word,
    Sep,
}

#[derive(Debug, Clone)]
struct Token {
    text: String,
    class: Class,
    /// A space or a separator stands before it.
    spaced: bool,
    /// Byte span in the original name.
    span: (usize, usize),
}

fn tokenize(name: &str) -> Vec<Token> {
    let mut out: Vec<Token> = Vec::new();
    let mut spaced = true;
    for (i, c) in name.char_indices() {
        let end = i + c.len_utf8();
        if c.is_whitespace() {
            spaced = true;
            continue;
        }
        let (class, c) = match c {
            '-' | '–' | '—' => (Class::Sep, '-'),
            '/' | ',' | ':' | '(' | ')' | '.' => (Class::Sep, c),
            c if c.is_ascii_digit() => (Class::Num, c),
            c => (Class::Word, c),
        };
        match out.last_mut() {
            Some(last)
                if last.class == class
                    && last.span.1 == i
                    && (class != Class::Sep || (c == '-' && last.text == "-")) =>
            {
                if class != Class::Sep {
                    last.text.extend(c.to_lowercase());
                }
                last.span.1 = end;
            }
            _ => {
                let before = out.last().is_none_or(|t| t.class == Class::Sep) || spaced;
                out.push(Token {
                    text: c.to_lowercase().collect(),
                    class,
                    spaced: before,
                    span: (i, end),
                });
            }
        }
        spaced = class == Class::Sep;
    }
    out
}

impl Grammar {
    /// Parse `name`.
    #[must_use]
    pub fn parse(&self, name: &str) -> Parsed {
        let mut tokens = tokenize(name);
        let edition = self.edition(&mut tokens);
        let unparsed = Parsed {
            period: None,
            edition,
            qualifier: None,
        };
        if tokens.is_empty() {
            return unparsed;
        }
        if let Some(period) = self.period(&tokens) {
            return Parsed {
                period: Some(period),
                edition,
                qualifier: None,
            };
        }
        let qualifier = |from: usize, to: usize| {
            let text =
                name[from..to].trim_matches(|c: char| c.is_whitespace() || ",:-".contains(c));
            (!text.is_empty()).then(|| text.to_owned())
        };
        // A period, then words.
        for split in (1..tokens.len()).rev() {
            let (left, right) = tokens.split_at(split);
            let left = trim_seps(left);
            let starts_word = right.iter().find(|t| t.class != Class::Sep);
            if right[0].spaced
                && starts_word.is_some_and(|t| t.class == Class::Word)
                && !left.is_empty()
                && let Some(period) = self.period(left)
            {
                return Parsed {
                    period: Some(period),
                    edition,
                    qualifier: qualifier(right[0].span.0, right[right.len() - 1].span.1),
                };
            }
        }
        // Words without a number, then a period.
        for split in 1..tokens.len() {
            let (left, right) = tokens.split_at(split);
            if left.iter().any(|t| t.class == Class::Num) {
                break;
            }
            let right = trim_seps(right);
            if !right.is_empty()
                && right[0].spaced
                && let Some(period) = self.period(right)
            {
                return Parsed {
                    period: Some(period),
                    edition,
                    qualifier: qualifier(left[0].span.0, right[0].span.0),
                };
            }
        }
        unparsed
    }

    /// Take a trailing edition phrase (after an optional `,`) off `tokens`.
    fn edition(&self, tokens: &mut Vec<Token>) -> &'static str {
        for (phrase, edition) in &self.editions {
            let words: Vec<Token> = tokenize(phrase);
            let n = words.len();
            if tokens.len() > n
                && tokens[tokens.len() - n].spaced
                && tokens[tokens.len() - n..]
                    .iter()
                    .zip(&words)
                    .all(|(a, b)| a.text == b.text)
            {
                tokens.truncate(tokens.len() - n);
                while tokens.last().is_some_and(|t| t.text == ",") {
                    tokens.pop();
                }
                return if edition == "final" {
                    "final"
                } else {
                    "preliminary"
                };
            }
        }
        "unstated"
    }

    /// The period of the first form `tokens` match exactly.
    fn period(&self, tokens: &[Token]) -> Option<(String, String, &'static str)> {
        self.forms.iter().find_map(|form| {
            let slots = self.bind(&tokenize(&form.pattern), tokens)?;
            build(form.period, &slots)
        })
    }

    /// The slot values when `tokens` match `pattern` token for token. A slot is a
    /// pattern word in braces.
    fn bind(&self, pattern: &[Token], tokens: &[Token]) -> Option<BTreeMap<String, u32>> {
        let mut slots = BTreeMap::new();
        let mut p = 0;
        let mut t = 0;
        while p < pattern.len() {
            let token = tokens.get(t)?;
            if let Some(name) = pattern[p]
                .text
                .strip_prefix('{')
                .and_then(|n| n.strip_suffix('}'))
            {
                slots.insert(name.to_owned(), self.slot(name, token)?);
                p += 1;
            } else if pattern[p].text == token.text {
                p += 1;
            } else {
                return None;
            }
            t += 1;
        }
        (t == tokens.len()).then_some(slots)
    }

    fn slot(&self, name: &str, token: &Token) -> Option<u32> {
        let number = || {
            (token.class == Class::Num)
                .then(|| token.text.parse::<u32>().ok())
                .flatten()
        };
        let digits = token.text.len();
        match name.trim_end_matches("_to") {
            "year" => number().filter(|_| digits == 4),
            "yy" => number().filter(|_| digits == 2),
            "mm" => number().filter(|m| digits == 2 && (1..=12).contains(m)),
            "dd" => number().filter(|d| digits <= 2 && (1..=31).contains(d)),
            "q" => number().filter(|q| digits == 1 && (1..=4).contains(q)),
            "month" => self.months.get(&token.text).copied(),
            "term" => match self.terms.get(&token.text)?.as_str() {
                "spring" => Some(1),
                "summer" => Some(2),
                "autumn" => Some(3),
                _ => None,
            },
            _ => None,
        }
    }
}

fn trim_seps(tokens: &[Token]) -> &[Token] {
    let start = tokens
        .iter()
        .position(|t| t.class != Class::Sep)
        .unwrap_or(tokens.len());
    let end = tokens
        .iter()
        .rposition(|t| t.class != Class::Sep)
        .map_or(start, |i| i + 1);
    &tokens[start..end]
}

fn day(y: u32, m: u32, d: u32) -> Option<String> {
    let iso = format!("{y:04}-{m:02}-{d:02}");
    reg_core::iso_day_number(&iso).map(|_| iso)
}

/// The inclusive days and grain a form states from its slots.
fn build(kind: Kind, s: &BTreeMap<String, u32>) -> Option<(String, String, &'static str)> {
    let get = |name: &str| s.get(name).copied();
    let year = get("year")?;
    let month = get("mm").or_else(|| get("month"));
    let first = |y: u32, m: u32| day(y, m, 1);
    let span = |a: Option<String>, b: Option<String>, grain| Some((a?, b?, grain));
    match kind {
        Kind::Year => span(first(year, 1), day(year, 12, 31), "year"),
        Kind::Years => {
            let to = get("year_to")?;
            (year < to).then_some(())?;
            span(first(year, 1), day(to, 12, 31), "span")
        }
        Kind::Day => {
            let d = day(year, month?, get("dd")?);
            span(d.clone(), d, "day")
        }
        Kind::Days => {
            let a = day(year, get("mm")?, get("dd")?)?;
            let b = day(get("year_to")?, get("mm_to")?, get("dd_to")?)?;
            (a < b).then_some(())?;
            span(Some(a), Some(b), "span")
        }
        Kind::Month => span(first(year, month?), month_end(year, month?), "month"),
        Kind::Months => {
            let (m, y2, m2) = (get("mm")?, get("year_to")?, get("mm_to")?);
            ((year, m) < (y2, m2)).then_some(())?;
            span(first(year, m), month_end(y2, m2), "span")
        }
        Kind::SchoolYear => {
            let next = match (get("year_to"), get("yy")) {
                (Some(to), _) => to == year + 1,
                (None, Some(yy)) => yy == (year + 1) % 100,
                (None, None) => false,
            };
            next.then_some(())?;
            span(day(year, 7, 1), day(year + 1, 6, 30), "span")
        }
        Kind::Term => term(year, get("term")?),
        Kind::Terms => {
            let (a, _, _) = term(year, get("term")?)?;
            let (_, b, _) = term(get("year_to")?, get("term_to")?)?;
            (a < b).then_some(())?;
            Some((a, b, "span"))
        }
        Kind::FirstHalf => span(first(year, 1), day(year, 6, 30), "half"),
        Kind::SecondHalf => span(first(year, 7), day(year, 12, 31), "half"),
        Kind::Quarter => {
            let q = get("q")?;
            let q2 = get("q_to").unwrap_or(q);
            (q <= q2).then_some(())?;
            let grain = if q == q2 { "quarter" } else { "span" };
            span(first(year, 3 * q - 2), month_end(year, 3 * q2), grain)
        }
    }
}

/// Spring is January to June, autumn July to December, summer June to August.
fn term(year: u32, term: u32) -> Option<(String, String, &'static str)> {
    match term {
        1 => Some((day(year, 1, 1)?, day(year, 6, 30)?, "half")),
        2 => Some((day(year, 6, 1)?, day(year, 8, 31)?, "span")),
        3 => Some((day(year, 7, 1)?, day(year, 12, 31)?, "half")),
        _ => None,
    }
}
