//! Pure curation addresses. Resolution against source frames belongs to resolve.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

const RELEASE_SELECTOR: &str =
    "unsupported :edition or /qualifier address syntax; use where release = '<handle>'";

/// A normalized union of reg-core periods, optionally open at either end: each
/// canonical period with its inclusive ISO interval, sorted and deduplicated by the
/// canonical text (the text feeds statement digests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeriodSet(Vec<(String, reg_core::Interval)>);

impl PeriodSet {
    /// The periods as inclusive ISO date intervals, an open end at `OPEN_START` or
    /// `OPEN_END`.
    #[must_use]
    pub fn intervals(&self) -> Vec<reg_core::Interval> {
        self.0
            .iter()
            .map(|(_, interval)| interval.clone())
            .collect()
    }
}

impl FromStr for PeriodSet {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let endpoint = |text: &str| {
            let period: reg_core::Period = text.parse().map_err(|e| format!("{e}"))?;
            if !matches!(period, reg_core::Period::Token(_)) {
                return Err("an open period needs one endpoint".to_owned());
            }
            Ok(period)
        };
        let mut periods = Vec::new();
        for part in text.split(',').map(str::trim) {
            if part.contains([':', '/']) {
                return Err(RELEASE_SELECTOR.to_owned());
            }
            periods.push(if let Some(end) = part.strip_prefix("..") {
                let period = endpoint(end)?;
                let to = period.iso_bounds().1;
                (format!("..{period}"), (crate::OPEN_START.to_owned(), to))
            } else if let Some(start) = part.strip_suffix("..") {
                let period = endpoint(start)?;
                let from = period.iso_bounds().0;
                (format!("{period}.."), (from, crate::OPEN_END.to_owned()))
            } else {
                let period = part
                    .parse::<reg_core::Period>()
                    .map_err(|e| format!("{e}"))?;
                (period.to_string(), period.iso_bounds())
            });
        }
        periods.sort();
        periods.dedup();
        Ok(Self(periods))
    }
}

impl fmt::Display for PeriodSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let texts: Vec<&str> = self.0.iter().map(|(text, _)| text.as_str()).collect();
        f.write_str(&texts.join(","))
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
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Label => "label",
            Self::Type => "type",
            Self::Unit => "unit",
            Self::Key => "key",
            Self::Release => "release",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Guard {
    pub field: GuardField,
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
    Set(String),
    Table(String),
    Release {
        table: String,
        handle: String,
    },
}

impl FromStr for Address {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (table, rest) = text
            .trim_start()
            .split_once(':')
            .ok_or("an address needs table: spelling, var:<id>, table:<slug> or release:<table-slug>/<handle>")?;
        let table = table.trim();
        let rest = rest.trim_start();
        // A release handle is opaque, including characters that delimit occurrence
        // scopes or guards. Parse it before interpreting any of those characters.
        if table == "release" {
            let (table, handle) = rest
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
        let rest = rest.trim_end();
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '@')
            .unwrap_or(rest.len());
        let (spelling, suffix) = rest.split_at(end);
        let (scope, guards) = match split_keyword(suffix, "where") {
            Some((_, guards)) if guards.trim().is_empty() => {
                return Err("where needs a guard".to_owned());
            }
            Some(parts) => parts,
            None => (suffix, ""),
        };
        let guards = parse_guards(guards)?;
        let periods = match scope.trim() {
            "" => None,
            scope => Some(
                scope
                    .strip_prefix('@')
                    .ok_or("after the spelling, expected @period-set or where guards")?
                    .trim()
                    .parse()?,
            ),
        };
        if spelling.contains([':', '/']) {
            return Err(RELEASE_SELECTOR.to_owned());
        }
        if matches!(table, "var" | "table" | "set") {
            if !reg_core::is_slug(spelling) || periods.is_some() || !guards.is_empty() {
                return Err(
                    "var:, table: and set: need a public slug without scope or guards".to_owned(),
                );
            }
            return Ok(if table == "var" {
                Self::Variable(spelling.to_owned())
            } else if table == "set" {
                Self::Set(spelling.to_owned())
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
        let (field, rest) = text.split_once('=').ok_or("a guard needs =")?;
        let field = match field.trim() {
            "label" => GuardField::Label,
            "type" => GuardField::Type,
            "unit" => GuardField::Unit,
            "key" => GuardField::Key,
            "release" => GuardField::Release,
            _ => return Err("guards allow only label, type, unit, key and release".to_owned()),
        };
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
    // Otherwise an asserted occurrence would invent the spelling `x01..12`.
    if body.contains("..") {
        return Err("brace ranges are not supported; list the spellings".to_owned());
    }
    let choices = if body.contains(',') {
        if body.split(',').any(str::is_empty) {
            return Err("brace list items must not be empty".to_owned());
        }
        body.split(',').map(str::to_owned).collect()
    } else {
        if body.is_empty() {
            return Err("brace list items must not be empty".to_owned());
        }
        vec![body.to_owned()]
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
            Self::Set(id) => write!(f, "set:{id}"),
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
                    write!(f, "{join}{} = '{}'", guard.field.name(), guard.value)?;
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
