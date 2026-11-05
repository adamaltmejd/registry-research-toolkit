//! The located finding (`shape.Issue`) and the result period (`shape.Period`) that the
//! order check, `validate` and the MONA order file ([`crate::manifest`]) share
//! (crates/DESIGN.md, "The logical order" and "MONA order file").

use serde::{Deserialize, Serialize};

use crate::{IssueLevel, ValidationIssue};

/// A half-open `[start, end)` of ISO days, `null` for an open end.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Period {
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub start: Option<String>,
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub end: Option<String>,
}

impl Period {
    /// The period of the day numbers `[start, end)`; an end equal to its `open`
    /// sentinel (`meta` `open_start`, `open_end`) is `null`.
    #[must_use]
    pub fn new(open: (i64, i64), start: i64, end: i64) -> Self {
        Self {
            start: (start != open.0).then(|| iso_day(start)),
            end: (end != open.1).then(|| iso_day(end)),
        }
    }

    /// The day numbers `[start, end)`, an open end read as its `open` sentinel.
    ///
    /// # Panics
    ///
    /// When a bound is not an ISO day, which [`Period::new`] never writes.
    #[must_use]
    pub fn days(&self, open: (i64, i64)) -> (i64, i64) {
        let day = |iso: &Option<String>, sentinel| {
            iso.as_deref()
                .map_or(sentinel, |d| crate::iso_day_number(d).expect("an ISO day"))
        };
        (day(&self.start, open.0), day(&self.end, open.1))
    }
}

/// The ISO date of a catalog day number (days from 1970-01-01): the inverse of
/// [`crate::iso_day_number`], Howard Hinnant's `civil_from_days`.
#[must_use]
pub fn iso_day(day: i64) -> String {
    let z = day + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

/// A project issue or an order finding, located in the project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Issue {
    pub level: IssueLevel,
    pub code: String,
    /// An RFC 6901 pointer into the project; empty for the whole document.
    pub path: String,
    pub message: String,
    /// The project source's name.
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub source: Option<String>,
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub variable: Option<String>,
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub table: Option<String>,
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub period: Option<Period>,
    /// The successor a `variable_replaced` finding names, or the successor table of an
    /// undated succession (`table_succession_undated`).
    #[cfg_attr(feature = "openapi", schema(required = true))]
    pub successor_fqid: Option<String>,
}

impl Issue {
    /// An issue with no source, variable, table, period or successor; set those with
    /// struct update (`Issue { source: .., ..Issue::new(..) }`).
    #[must_use]
    pub fn new(
        level: IssueLevel,
        code: &str,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            level,
            code: code.to_owned(),
            path: path.into(),
            message: message.into(),
            source: None,
            variable: None,
            table: None,
            period: None,
            successor_fqid: None,
        }
    }

    /// Whether the issue blocks: an `error`.
    #[must_use]
    pub fn blocks(&self) -> bool {
        self.level == IssueLevel::Error
    }

    /// The ordering within one anchor (a source, a binding, a panel): code, then period
    /// start.
    #[must_use]
    pub fn key(&self) -> (String, Option<String>) {
        (
            self.code.clone(),
            self.period.as_ref().and_then(|p| p.start.clone()),
        )
    }
}

/// A structural issue, whose located fields are `null`.
impl From<ValidationIssue> for Issue {
    fn from(issue: ValidationIssue) -> Self {
        Self {
            successor_fqid: issue.successor_fqid,
            ..Self::new(issue.level, issue.code, issue.path, issue.message)
        }
    }
}
