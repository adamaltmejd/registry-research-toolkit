//! `values`: a classification's codes, or the value set of one state of a variable
//! (today's `/api/value-sets/{id}/codes` and `get classification --codes`), with a
//! declared book's partitions of a state's coding. Rows are ordered by code and
//! label, `q` keeps the rows whose code or label contains it under `fold_search`
//! (today's `matches_filter`), and `total` counts them before the page. Paging runs
//! to the end of the set: the code panel and G1 walk sets of 40k codes, so there
//! is no depth cap.

use reg_core::fold_search;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use super::refs::{self, Target};
use super::states::{alias_evidence, emitted};
use super::{Params, Server, cursor};
use crate::{Code, Error, Scope, hex};

/// `partition`'s members; the default is the first.
pub const PARTITIONS: &[&str] = &["source_extensions", "canonical", "nonstandard", "sentinels"];
/// A state's parameters, which a classification ref does not take.
const STATE_PARAMS: &[&str] = &[
    "state",
    "partition",
    "classification",
    "column",
    "alias_window_from",
];

/// One delivered (code, label) pair: today's `ValueSetMember`.
#[derive(Serialize, ToSchema)]
pub struct ValueSetMember {
    code: String,
    label: String,
}

/// A delivered pair outside its declared book: today's
/// `ClassificationExtensionMember`.
#[derive(Serialize, ToSchema)]
pub struct ClassificationExtensionMember {
    code: String,
    label: String,
    /// `nonstandard` or `sentinel`.
    member_kind: String,
    /// The book's curated meaning of a sentinel code.
    sentinel_meaning: Option<String>,
    /// The certificates that make a source-local code a sentinel.
    scoped_sentinels: Vec<ScopedSentinel>,
}

/// A build-time certificate of a checked, finite source sentinel decision
/// (today's `ScopedSentinelEvidence`).
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ScopedSentinel {
    valid_from: Option<String>,
    valid_to: Option<String>,
    delivery_column_name: String,
    classification_sha256: String,
    source_fingerprints: Vec<String>,
    #[schema(value_type = Vec<Vec<String>>)]
    members: Vec<(String, String)>,
    provenance: String,
}

/// One code of a classification edition: today's `ClassificationCode`.
#[derive(Serialize, ToSchema)]
pub struct ClassificationCode {
    code: String,
    label: String,
    /// The hierarchy depth; null when the classification is flat.
    level: Option<i64>,
    /// True for a canonical code, null when no canonical list exists.
    is_valid: Option<bool>,
}

/// A row of `values`: a classification's code, a state's delivered pair, or a
/// delivered pair outside a declared book.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub enum ValueRow {
    Code(ClassificationCode),
    Extension(ClassificationExtensionMember),
    Member(ValueSetMember),
}

impl ValueRow {
    fn code_label(&self) -> (&str, &str) {
        match self {
            Self::Code(c) => (&c.code, &c.label),
            Self::Extension(m) => (&m.code, &m.label),
            Self::Member(m) => (&m.code, &m.label),
        }
    }
}

#[derive(Serialize, ToSchema)]
pub struct ValuesPage {
    items: Vec<ValueRow>,
    next_cursor: Option<String>,
    /// The rows matching `q` in the whole set.
    total: usize,
}

pub fn values(server: &Server, scope: Scope, params: &Params) -> Result<serde_json::Value, Error> {
    let catalog = &server.catalog;
    let limit = super::limit(params)?;
    let q = super::q(params)?;
    let state = super::storage_id(params, "state")?;
    let partition = params.get("partition").copied();
    if partition.is_some_and(|p| !PARTITIONS.contains(&p)) {
        return Err(Error::invalid_parameter("partition"));
    }
    let window = match (
        params.get("column").copied(),
        params.get("alias_window_from").copied(),
    ) {
        (Some(column), Some(from)) => Some((column, from)),
        (None, None) => None,
        (Some(_), None) => return Err(Error::invalid_parameter("alias_window_from")),
        (None, Some(_)) => return Err(Error::invalid_parameter("column")),
    };
    let conn = catalog.connect()?;
    let reference = params["ref"];
    let mut rows = match refs::resolve(&conn, scope, Some(reference))? {
        Target::Classification { id, .. } => {
            if let Some(name) = STATE_PARAMS.iter().find(|n| params.get(n).is_some()) {
                return Err(Error::invalid_parameter(name));
            }
            classification_codes(&conn, id)?
        }
        Target::Variable { id } => {
            let state = state.ok_or_else(|| Error::invalid_parameter("state"))?;
            let book = params
                .get("classification")
                .map(|value| book(&conn, scope, value))
                .transpose()?;
            if book.is_none() && partition.is_some() {
                return Err(Error::invalid_parameter("partition"));
            }
            let coding = Coding {
                reference,
                variable_id: id,
                state_id: state,
                window,
            };
            coding.rows(&conn, scope, book, partition.unwrap_or(PARTITIONS[0]))?
        }
        _ => {
            return Err(Error::new(
                Code::InvalidRef,
                format!("{reference:?} names neither a classification nor a variable."),
                vec![reference.into()],
            ));
        }
    };
    let needle = fold_search(q);
    // simplify: folds every row per request (pinned v0.43.0: 74 ms over the 44k-member
    // set, 128 ms over icd-10-se, against 167 and 351 ms for today's read and filter);
    // store folded code and label in derive if code filtering is reported slow or a
    // set passes about 100k members.
    if !needle.is_empty() {
        rows.retain(|row| {
            let (code, label) = row.code_label();
            fold_search(code).contains(&needle) || fold_search(label).contains(&needle)
        });
    }
    let context = json!([
        reference,
        state,
        partition,
        params.get("classification"),
        window,
        q,
        scope
    ]);
    let context = hex(&Sha256::digest(context.to_string().as_bytes()));
    let position = |row: &ValueRow| row.code_label().0.to_owned();
    let offset = match params.get("cursor") {
        None => 0,
        Some(c) => {
            let (offset, after) = cursor::decode(c, catalog.generation(), &context, usize::MAX)?;
            if offset > rows.len() || position(&rows[offset - 1]) != after {
                return Err(cursor::invalid(
                    "Cursor no longer matches the result ordering.",
                ));
            }
            offset
        }
    };
    let total = rows.len();
    let end = (offset + limit).min(total);
    let next_cursor = (end < total).then(|| {
        cursor::encode(
            catalog.generation(),
            &context,
            end,
            &position(&rows[end - 1]),
        )
    });
    let items = rows.drain(offset..end).collect();
    let page = ValuesPage {
        items,
        next_cursor,
        total,
    };
    Ok(serde_json::to_value(page).expect("ValuesPage serializes"))
}

/// A declared book: its id, slug and the ref the request named it by.
type Book<'a> = (i64, String, &'a str);

/// The classification `value` names (a ref).
fn book<'a>(conn: &Connection, scope: Scope, value: &'a str) -> Result<Book<'a>, Error> {
    match refs::resolve(conn, scope, Some(value))? {
        Target::Classification { id, slug } => Ok((id, slug, value)),
        _ => Err(Error::new(
            Code::InvalidRef,
            format!("{value:?} does not name a classification."),
            vec![value.into()],
        )),
    }
}

fn classification_codes(conn: &Connection, id: i64) -> Result<Vec<ValueRow>, Error> {
    let mut stmt = conn.prepare_cached(
        "SELECT vc.code, vc.label, cc.level, cc.is_valid FROM classification_code cc \
         JOIN value_code vc ON vc.code_id = cc.code_id WHERE cc.classification_id = ? \
         ORDER BY vc.code, vc.label",
    )?;
    let rows = stmt
        .query_map([id], |row| {
            Ok(ValueRow::Code(ClassificationCode {
                code: row.get(0)?,
                label: row.get(1)?,
                level: row.get(2)?,
                is_valid: row.get(3)?,
            }))
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

fn members(
    conn: &Connection,
    value_set_id: i64,
    canonical_in: Option<i64>,
) -> Result<Vec<ValueRow>, Error> {
    // The delivered pairs, or with a book only those whose literal code it holds.
    let mut stmt = conn.prepare_cached(
        "SELECT vc.code, vc.label FROM value_set_member vsm JOIN value_code vc USING(code_id) \
         WHERE vsm.value_set_id = ?1 AND (?2 IS NULL OR EXISTS (SELECT 1 \
         FROM classification_code cc JOIN value_code canonical ON canonical.code_id = cc.code_id \
         WHERE cc.classification_id = ?2 AND canonical.code = vc.code)) \
         ORDER BY vc.code, vc.label",
    )?;
    let rows = stmt
        .query_map(params![value_set_id, canonical_in], |row| {
            Ok(ValueRow::Member(ValueSetMember {
                code: row.get(0)?,
                label: row.get(1)?,
            }))
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

/// A variable state's coding a request names: the state's own, or with `window`
/// (`column`, `alias_window_from`) the coded alias window's.
struct Coding<'a> {
    reference: &'a str,
    variable_id: i64,
    state_id: i64,
    window: Option<(&'a str, &'a str)>,
}

impl Coding<'_> {
    fn missing(&self, what: &str) -> Error {
        let window = self
            .window
            .map(|(column, from)| format!(" in column {column:?} from {from}"))
            .unwrap_or_default();
        Error::new(
            Code::NotFound,
            format!(
                "{:?} has no state {}{window} with {what} in this scope.",
                self.reference, self.state_id
            ),
            // The identifier not found is the state (errors.toml: "the requested ref
            // or identifier"); the message names the variable and the window.
            vec![self.state_id.to_string().into()],
        )
    }

    /// The rows: the coding's value set, or with `book` its partition of that
    /// book's stored conformance. A state or window `states` does not emit in
    /// `scope`, a coding without a value set and a book without conformance are
    /// `not_found`.
    fn rows(
        &self,
        conn: &Connection,
        scope: Scope,
        book: Option<Book>,
        partition: &str,
    ) -> Result<Vec<ValueRow>, Error> {
        // simplify: the variable's whole emitted history per request, as `states`
        // reads it (pinned v0.43.0, warm: about 2 ms for skolkod's 94 rows, 3 ms for
        // scb/rtb/kon's 521, the most of any variable); select the one state's rows if
        // a variable passes a few thousand `expanded_state` rows.
        let visible = emitted(conn, scope, self.variable_id, None, None)?
            .iter()
            .any(|e| {
                e.state_id == self.state_id
                    && self.window.is_none_or(|(column, from)| {
                        e.delivery_column_name.as_deref() == Some(column)
                            && e.window_valid_from.as_deref() == Some(from)
                    })
            });
        if !visible {
            return Err(self.missing("that coding"));
        }
        let value_set_id: Option<i64> = match self.window {
            None => conn.query_row(
                "SELECT value_set_id FROM variable_state WHERE state_id = ?",
                [self.state_id],
                |row| row.get(0),
            )?,
            Some((column, from)) => conn
                .query_row(
                    "SELECT w.value_set_id FROM variable_alias_window w JOIN variable_state s \
                     ON s.variable_id = w.variable_id AND s.register_variant_id = w.register_variant_id \
                     WHERE s.state_id = ? AND w.delivery_column_name = ? AND w.valid_from = ? \
                     AND w.coding_metadata = 'per_column'",
                    params![self.state_id, column, from],
                    |row| row.get(0),
                )
                .optional()?
                .flatten(),
        };
        let value_set_id = value_set_id.ok_or_else(|| self.missing("a value set"))?;
        let Some((book_id, slug, requested)) = book else {
            return members(conn, value_set_id, None);
        };
        let extensions = match self.window {
            None => self.state_extensions(conn, book_id)?,
            Some((column, from)) => self.window_extensions(conn, book_id, &slug, column, from)?,
        };
        // A book the coding does not declare: not found under the name it was asked by.
        let extensions = extensions.ok_or_else(|| {
            Error::new(
                Code::NotFound,
                format!(
                    "State {} of {:?} declares no conformance to {requested:?}.",
                    self.state_id, self.reference
                ),
                vec![requested.into()],
            )
        })?;
        let kind = match partition {
            "canonical" => return members(conn, value_set_id, Some(book_id)),
            "nonstandard" => Some("nonstandard"),
            "sentinels" => Some("sentinel"),
            _ => None,
        };
        Ok(extensions
            .into_iter()
            .filter(|m| kind.is_none_or(|k| m.member_kind == k))
            .map(ValueRow::Extension)
            .collect())
    }

    /// The state's stored extensions of a book it links with a conformance, or
    /// `None`.
    fn state_extensions(
        &self,
        conn: &Connection,
        book_id: i64,
    ) -> Result<Option<Vec<ClassificationExtensionMember>>, Error> {
        let declared: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM state_classification sc \
             JOIN classification_conformance cf ON cf.state_id = sc.state_id \
             AND cf.declared_classification_id = sc.classification_id \
             WHERE sc.state_id = ? AND sc.classification_id = ?)",
            params![self.state_id, book_id],
            |row| row.get(0),
        )?;
        if !declared {
            return Ok(None);
        }
        let mut stmt = conn.prepare_cached(
            "SELECT vc.code, vc.label, ccc.member_kind, ccc.sentinel_meaning, ccc.scoped_sentinels \
             FROM classification_conformance_code ccc JOIN value_code vc USING(code_id) \
             WHERE ccc.state_id = ? AND ccc.declared_classification_id = ? ORDER BY vc.code, vc.label",
        )?;
        let rows: Vec<(String, String, String, Option<String>, String)> = stmt
            .query_map(params![self.state_id, book_id], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;
        rows.into_iter()
            .map(|(code, label, member_kind, sentinel_meaning, scoped)| {
                let scoped_sentinels = serde_json::from_str(&scoped).map_err(|err| {
                    Error::new(
                        Code::InternalError,
                        format!(
                            "Unreadable scoped sentinels of state {}: {err}",
                            self.state_id
                        ),
                        vec![],
                    )
                })?;
                Ok(ClassificationExtensionMember {
                    code,
                    label,
                    member_kind,
                    sentinel_meaning,
                    scoped_sentinels,
                })
            })
            .collect::<Result<_, Error>>()
            .map(Some)
    }

    /// The coded window's extensions of a book it links with a conformance, from
    /// its stored evidence, or `None` (today's `_alias_classifications`).
    fn window_extensions(
        &self,
        conn: &Connection,
        book_id: i64,
        slug: &str,
        column: &str,
        from: &str,
    ) -> Result<Option<Vec<ClassificationExtensionMember>>, Error> {
        let evidence: Option<String> = conn
            .query_row(
                "SELECT ac.conformance FROM alias_window_classification ac \
                 JOIN variable_state s ON s.variable_id = ac.variable_id \
                 AND s.register_variant_id = ac.register_variant_id \
                 WHERE s.state_id = ? AND ac.delivery_column_name = ? AND ac.valid_from = ? \
                 AND ac.classification_id = ?",
                params![self.state_id, column, from, book_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let Some(evidence) = evidence else {
            return Ok(None);
        };
        let evidence = alias_evidence(slug, &evidence)?;
        let certificates = evidence.scoped_sentinels;
        let member = |(code, label): (String, String), kind: &str| {
            let scoped_sentinels = if kind == "sentinel" {
                certificates
                    .iter()
                    .filter(|c| c.members.iter().any(|m| m.0 == code && m.1 == label))
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            ClassificationExtensionMember {
                code,
                label,
                member_kind: kind.to_owned(),
                sentinel_meaning: None,
                scoped_sentinels,
            }
        };
        let mut out: Vec<ClassificationExtensionMember> = evidence
            .nonconforming_members
            .into_iter()
            .map(|pair| member(pair, "nonstandard"))
            .chain(
                evidence
                    .sentinel_members
                    .into_iter()
                    .map(|pair| member(pair, "sentinel")),
            )
            .collect();
        // Stable, as today's `sorted`: a pair in both lists keeps its kind order.
        out.sort_by(|a, b| (&a.code, &a.label).cmp(&(&b.code, &b.label)));
        Ok(Some(out))
    }
}
