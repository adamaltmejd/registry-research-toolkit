//! Pure curation addresses. Resolution against source frames belongs to resolve.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

const RELEASE_SELECTOR: &str =
    "unsupported :edition or /qualifier address syntax; use where release = '<handle>'";

/// A normalized union of reg-core periods, optionally open at either end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeriodSet(Vec<String>);

impl FromStr for PeriodSet {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut periods = Vec::new();
        for part in text.split(',').map(str::trim) {
            if part.contains([':', '/']) {
                return Err(RELEASE_SELECTOR.to_owned());
            }
            let canonical = if let Some(end) = part.strip_prefix("..") {
                let period: reg_core::Period = end.parse().map_err(|e| format!("{e}"))?;
                if !matches!(period, reg_core::Period::Token(_)) {
                    return Err("an open period needs one endpoint".to_owned());
                }
                format!("..{period}")
            } else if let Some(start) = part.strip_suffix("..") {
                let period: reg_core::Period = start.parse().map_err(|e| format!("{e}"))?;
                if !matches!(period, reg_core::Period::Token(_)) {
                    return Err("an open period needs one endpoint".to_owned());
                }
                format!("{period}..")
            } else {
                part.parse::<reg_core::Period>()
                    .map_err(|e| format!("{e}"))?
                    .to_string()
            };
            periods.push(canonical);
        }
        periods.sort();
        periods.dedup();
        Ok(Self(periods))
    }
}

impl fmt::Display for PeriodSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.join(","))
    }
}

/// Source fields allowed in guards; keys and release handles are opaque equality values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GuardField {
    Label,
    Type,
    Unit,
    Key,
    Release,
}

impl GuardField {
    fn name(self) -> &'static str {
        match self {
            Self::Label => "label",
            Self::Type => "type",
            Self::Unit => "unit",
            Self::Key => "key",
            Self::Release => "release",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Comparison {
    Equal,
    Prefix,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Guard {
    pub field: GuardField,
    pub comparison: Comparison,
    pub value: String,
}

/// A source occurrence, a public ledger entity, or a table's opaque release handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    Occurrence {
        table: String,
        spellings: Vec<String>,
        periods: Option<PeriodSet>,
        guards: Vec<Guard>,
    },
    Variable(String),
    Table(String),
    Release {
        table: String,
        handle: String,
    },
}

impl FromStr for Address {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        // A release handle is opaque, including characters that delimit occurrence
        // scopes or guards. Parse it before interpreting any of those characters.
        if let Some(release) = text.trim_start().strip_prefix("release:") {
            let (table, handle) = release
                .split_once('/')
                .ok_or("a release address needs release:<table-slug>/<handle>")?;
            if !reg_core::is_slug(table) || handle.is_empty() {
                return Err(
                    "a release address needs a catalog table slug and nonempty source handle"
                        .to_owned(),
                );
            }
            return Ok(Self::Release {
                table: table.to_owned(),
                handle: handle.to_owned(),
            });
        }
        let text = text.trim();
        let (head, guards) = match split_keyword(text, "where") {
            Some((_, guards)) if guards.trim().is_empty() => {
                return Err("where needs a guard".to_owned());
            }
            Some(parts) => parts,
            None => (text, ""),
        };
        let guards = parse_guards(guards)?;
        let (head, periods) = match head.split_once('@') {
            Some((head, periods)) => (head.trim(), Some(periods.trim().parse()?)),
            None => (head.trim(), None),
        };
        let (table, spelling) = head
            .split_once(':')
            .ok_or("an address needs table: spelling, var:<id>, table:<slug> or release:<table-slug>/<handle>")?;
        let (table, spelling) = (table.trim(), spelling.trim());
        if spelling.contains([':', '/']) {
            return Err(RELEASE_SELECTOR.to_owned());
        }
        if table == "var" || table == "table" {
            if !reg_core::is_slug(spelling) || periods.is_some() || !guards.is_empty() {
                return Err("var: and table: need a public slug without scope or guards".to_owned());
            }
            return Ok(if table == "var" {
                Self::Variable(spelling.to_owned())
            } else {
                Self::Table(spelling.to_owned())
            });
        }
        if !reg_core::is_slug(table) {
            return Err("an occurrence address needs a catalog table slug".to_owned());
        }
        let mut spellings = expand(spelling)?;
        for spelling in &mut spellings {
            if spelling != "<none>" {
                *spelling = reg_core::fold_identity(spelling);
            }
        }
        spellings.sort();
        spellings.dedup();
        Ok(Self::Occurrence {
            table: table.to_owned(),
            spellings,
            periods,
            guards,
        })
    }
}

fn split_keyword<'a>(text: &'a str, keyword: &str) -> Option<(&'a str, &'a str)> {
    text.match_indices(keyword).find_map(|(at, _)| {
        let (before, after) = (&text[..at], &text[at + keyword.len()..]);
        (before.ends_with(char::is_whitespace)
            && (after.is_empty() || after.starts_with(char::is_whitespace)))
        .then_some((before.trim_end(), after.trim_start()))
    })
}

fn parse_guards(mut text: &str) -> Result<Vec<Guard>, String> {
    let mut guards = Vec::new();
    while !text.is_empty() {
        let (field, rest) = text.split_once('=').ok_or("a guard needs = or ^=")?;
        let prefix = field.trim_end().ends_with('^');
        let field = field.trim();
        let field = field.strip_suffix('^').unwrap_or(field).trim();
        let field = match field {
            "label" => GuardField::Label,
            "type" => GuardField::Type,
            "unit" => GuardField::Unit,
            "key" => GuardField::Key,
            "release" => GuardField::Release,
            _ => return Err("guards allow only label, type, unit, key and release".to_owned()),
        };
        if prefix && matches!(field, GuardField::Key | GuardField::Release) {
            return Err("where key and where release allow exact equality only".to_owned());
        }
        let quoted = rest
            .trim_start()
            .strip_prefix('\'')
            .ok_or("a guard value needs single quotes")?;
        let (value, rest) = quoted.split_once('\'').ok_or("unterminated guard value")?;
        if value.contains(['\n', '\r']) {
            return Err("a guard value must fit on one line".to_owned());
        }
        guards.push(Guard {
            field,
            comparison: if prefix {
                Comparison::Prefix
            } else {
                Comparison::Equal
            },
            value: value.to_owned(),
        });
        let rest = rest.trim();
        text = if rest.is_empty() {
            ""
        } else {
            let next = rest
                .strip_prefix("and")
                .filter(|tail| tail.starts_with(char::is_whitespace))
                .ok_or("guards must be separated by and")?
                .trim_start();
            if next.is_empty() {
                return Err("and needs another guard".to_owned());
            }
            next
        };
    }
    guards.sort();
    guards.dedup();
    Ok(guards)
}

fn expand(text: &str) -> Result<Vec<String>, String> {
    let Some((prefix, tail)) = text.split_once('{') else {
        if text.is_empty()
            || text != text.trim()
            || text.contains(char::is_whitespace)
            || text.contains([
                '}', '*', '+', '?', '[', ']', '(', ')', '|', '^', '$', '\\', '@', ':', '/', ',',
            ])
            || (text.contains(['<', '>']) && text != "<none>")
        {
            return Err("a spelling must be literal, a brace list, or <none>; regular expressions are not allowed".to_owned());
        }
        return Ok(vec![text.to_owned()]);
    };
    let (body, suffix) = tail.split_once('}').ok_or("unterminated brace list")?;
    if body.contains('{') {
        return Err("nested brace lists are not allowed".to_owned());
    }
    let choices = if let Some((start, end)) = body.split_once("..") {
        if start.is_empty()
            || end.is_empty()
            || !start.bytes().all(|b| b.is_ascii_digit())
            || !end.bytes().all(|b| b.is_ascii_digit())
        {
            return Err("a brace range needs unsigned integer endpoints".to_owned());
        }
        let lo = start.parse::<u32>().map_err(|e| e.to_string())?;
        let hi = end.parse::<u32>().map_err(|e| e.to_string())?;
        if lo > hi || hi - lo >= 10_000 {
            return Err(
                "a brace range must ascend and expand to at most 10000 spellings".to_owned(),
            );
        }
        let width = if start.starts_with('0') {
            start.len()
        } else {
            0
        };
        (lo..=hi)
            .map(|n| format!("{n:0width$}"))
            .collect::<Vec<_>>()
    } else {
        if body.split(',').any(str::is_empty) {
            return Err("brace list items must not be empty".to_owned());
        }
        body.split(',').map(str::to_owned).collect()
    };
    let mut result = Vec::new();
    for choice in choices {
        for expanded in expand(&format!("{prefix}{choice}{suffix}"))? {
            result.push(expanded);
            if result.len() > 10_000 {
                return Err("a brace list expands to more than 10000 spellings".to_owned());
            }
        }
    }
    Ok(result)
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Variable(id) => write!(f, "var:{id}"),
            Self::Table(id) => write!(f, "table:{id}"),
            Self::Release { table, handle } => write!(f, "release:{table}/{handle}"),
            Self::Occurrence {
                table,
                spellings,
                periods,
                guards,
            } => {
                write!(f, "{table}: ")?;
                if spellings.len() == 1 {
                    f.write_str(&spellings[0])?;
                } else {
                    write!(f, "{{{}}}", spellings.join(","))?;
                }
                if let Some(periods) = periods {
                    write!(f, " @{periods}")?;
                }
                for (i, guard) in guards.iter().enumerate() {
                    let join = if i == 0 { " where " } else { " and " };
                    let op = if guard.comparison == Comparison::Equal {
                        "="
                    } else {
                        "^="
                    };
                    write!(f, "{join}{} {op} '{}'", guard.field.name(), guard.value)?;
                }
                Ok(())
            }
        }
    }
}

macro_rules! string_serde {
    ($ty:ty) => {
        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                String::deserialize(deserializer)?
                    .parse()
                    .map_err(serde::de::Error::custom)
            }
        }
    };
}
string_serde!(Address);
string_serde!(PeriodSet);
