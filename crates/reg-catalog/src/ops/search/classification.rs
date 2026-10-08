//! The classification arm: name full-text hits and, for a code-shaped query, the
//! classifications that contain a matching code; edition chains then fold into one
//! succession row and classification groups into one group row (today's
//! `_search_classifications`, `_search_classifications_by_code`,
//! `_fold_classification_succession` and `_fold_concept_groups`). Classifications
//! keep reference semantics in every scope.

use std::collections::{BTreeMap, BTreeSet};

use reg_core::{fold_search, py_strip};
use rusqlite::{Connection, OptionalExtension, Row, params_from_iter};
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;

use super::{HORIZON, Hit, Member, Request, fold, fqid, is_code_shaped, like_escape};
use crate::{Error, Scope};

/// Code-containment hits rank after every name hit (bm25 is negative).
const CODE_RANK_BASE: f64 = 1000.0;
/// The classification is not pinned for the query (`?` binds `fold_search(q)`).
const UNPINNED: &str = "NOT EXISTS (SELECT 1 FROM search_pin sp WHERE sp.key = ? \
     AND sp.type = 'classification' AND sp.entity = 'class/' || c.slug)";

pub(super) struct ClassificationHit {
    pub id: i64,
    slug: Option<String>,
    pub fqid: Option<String>,
    pub short_name: Option<String>,
    pub name: Option<String>,
    pub rank: f64,
    /// An old edition's current one, when its chain does not fold.
    pub terminal_fqid: Option<String>,
}

/// A chain of editions folded onto its terminal (current) edition.
pub(super) struct SuccessionHit {
    pub id: Option<i64>,
    pub fqid: Option<String>,
    pub short_name: Option<String>,
    pub name: Option<String>,
    pub editions: Vec<Edition>,
    pub matched_count: usize,
    pub rank: f64,
}

#[derive(Serialize, ToSchema)]
pub struct Edition {
    pub slug: String,
    pub fqid: Option<String>,
    pub name: Option<String>,
    pub effective_year: Option<i64>,
}

/// Columns `c.id, c.short_name, c.name, c.slug` as a hit.
fn read(row: &Row, rank: f64) -> rusqlite::Result<ClassificationHit> {
    let slug: Option<String> = row.get(3)?;
    Ok(ClassificationHit {
        id: row.get(0)?,
        fqid: fqid(&[Some("class".to_owned()), slug.clone()]),
        slug,
        short_name: row.get(1)?,
        name: row.get(2)?,
        rank,
        terminal_fqid: None,
    })
}

/// The classifications pinned for the query, in pin order.
pub(super) fn pins(conn: &Connection, q: &str) -> Result<Vec<Hit>, Error> {
    let mut stmt = conn.prepare(
        "SELECT c.id, c.short_name, c.name, c.slug FROM search_pin sp \
         JOIN classification c ON sp.entity = 'class/' || c.slug \
         WHERE sp.key = ? AND sp.type = 'classification' ORDER BY sp.position",
    )?;
    let pins = stmt
        .query_map([fold_search(q)], |row| {
            Ok(Hit::Classification(read(row, 0.0)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(pins)
}

/// The arm's rows, unpinned, before ranking.
pub(super) fn arm(
    conn: &Connection,
    scope: Scope,
    request: &Request,
    fts: &str,
) -> Result<Vec<Hit>, Error> {
    let key = fold_search(request.q);
    let mut stmt = conn.prepare(&format!(
        "SELECT c.id, c.short_name, c.name, c.slug, cf.rank FROM classification_fts cf \
         JOIN classification c ON c.id = cf.rowid WHERE classification_fts MATCH ? \
         AND {UNPINNED} ORDER BY cf.rank, c.id LIMIT {HORIZON}"
    ))?;
    let mut hits: Vec<ClassificationHit> = stmt
        .query_map((fts, &key), |row| read(row, row.get(4)?))?
        .collect::<rusqlite::Result<_>>()?;
    if is_code_shaped(request.q) {
        // Classifications containing the code (exact first), once each: a name hit
        // or a pin is not repeated.
        let q = py_strip(request.q);
        let named: Vec<i64> = hits.iter().map(|h| h.id).collect();
        let mut stmt = conn.prepare(&format!(
            "SELECT c.id, c.short_name, c.name, c.slug, \
             MAX(CASE WHEN vc.code = ?1 COLLATE NOCASE THEN 1 ELSE 0 END) AS has_exact \
             FROM value_code vc JOIN classification_code cc ON cc.code_id = vc.code_id \
             JOIN classification c ON c.id = cc.classification_id \
             WHERE (vc.code = ?1 OR vc.code LIKE ?2 ESCAPE '\\') \
             AND c.id NOT IN (SELECT value FROM json_each(?3)) AND {} \
             GROUP BY c.id, c.short_name, c.name, c.slug \
             ORDER BY has_exact DESC, c.short_name, c.id LIMIT {HORIZON}",
            UNPINNED.replace('?', "?4")
        ))?;
        let rows: Vec<ClassificationHit> = stmt
            .query_map(
                (
                    q,
                    format!("{}%", like_escape(q)),
                    json!(named).to_string(),
                    &key,
                ),
                |row| read(row, 0.0),
            )?
            .collect::<rusqlite::Result<_>>()?;
        let mut rank = CODE_RANK_BASE;
        for mut hit in rows {
            hit.rank = rank;
            rank += 1.0;
            hits.push(hit);
        }
    }
    fold(
        conn,
        scope,
        request,
        "classification",
        fold_succession(conn, hits)?,
        |group| members(conn, group.id),
    )
}

/// A classification group's members (today's `list_classification_groups`), by
/// facet value and slug.
fn members(conn: &Connection, group: i64) -> Result<Vec<Member>, Error> {
    let mut stmt = conn.prepare(
        "SELECT c.slug, c.name, m.facet_value, m.facet_label \
         FROM concept_group_classification m JOIN classification c ON c.id = m.classification_id \
         WHERE m.group_id = ? AND c.slug IS NOT NULL ORDER BY m.facet_value, c.slug",
    )?;
    let members = stmt
        .query_map([group], |row| {
            Ok(Member {
                fqid: format!("class/{}", row.get::<_, String>(0)?),
                name: row.get(1)?,
                delivery_column: None,
                facets: vec![(row.get(2)?, row.get(3)?)],
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(members)
}

/// Fold the editions of one succession chain: where two distinct editions of a
/// chain are hits, they become one row for the chain's terminal edition, at its
/// first hit's place. A lone old edition stays a hit that names its terminal.
fn fold_succession(conn: &Connection, hits: Vec<ClassificationHit>) -> Result<Vec<Hit>, Error> {
    let terminals: Vec<Option<String>> = hits
        .iter()
        .map(|hit| hit.slug.as_deref().map(|s| terminal(conn, s)).transpose())
        .collect::<Result<_, _>>()?;
    let mut chains: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (hit, terminal) in hits.iter().zip(&terminals) {
        if let (Some(slug), Some(terminal)) = (&hit.slug, terminal) {
            chains.entry(terminal).or_default().insert(slug);
        }
    }
    let folded: BTreeSet<String> = chains
        .into_iter()
        .filter(|(_, slugs)| slugs.len() >= 2)
        .map(|(terminal, _)| terminal.to_owned())
        .collect();
    let mut matched: BTreeMap<&str, (BTreeSet<i64>, f64)> = BTreeMap::new();
    for (hit, terminal) in hits.iter().zip(&terminals) {
        if let Some(terminal) = terminal.as_deref().filter(|t| folded.contains(*t)) {
            let (ids, rank) = matched
                .entry(terminal)
                .or_insert((BTreeSet::new(), hit.rank));
            ids.insert(hit.id);
            *rank = rank.min(hit.rank);
        }
    }
    let mut out = Vec::new();
    let mut emitted = BTreeSet::new();
    for (mut hit, terminal) in hits.into_iter().zip(terminals.iter()) {
        match terminal {
            Some(t) if folded.contains(t) => {
                if emitted.insert(t) {
                    let (ids, rank) = &matched[t.as_str()];
                    out.push(Hit::Succession(succession(conn, t, ids.len(), *rank)?));
                }
            }
            _ => {
                // A split root or a current edition is its own terminal: no link.
                if let Some(t) = terminal
                    .as_deref()
                    .filter(|t| Some(*t) != hit.slug.as_deref())
                {
                    hit.terminal_fqid = fqid(&[Some("class".to_owned()), Some(t.to_owned())]);
                }
                out.push(Hit::Classification(hit));
            }
        }
    }
    Ok(out)
}

/// Today's `_terminal_classification_slug`, compiled: the terminal at the policy
/// year (an edition at a split is its own), from `succession_terminal`.
fn terminal(conn: &Connection, slug: &str) -> Result<String, Error> {
    let terminal: Option<String> = conn
        .query_row(
            "SELECT terminal_fqid FROM succession_terminal WHERE fqid = 'class/' || ?",
            [slug],
            |row| row.get(0),
        )
        .optional()?;
    Ok(terminal.map_or_else(
        || slug.to_owned(),
        |t| t.trim_start_matches("class/").to_owned(),
    ))
}

/// The succession row of the chain ending at `terminal`.
fn succession(
    conn: &Connection,
    terminal: &str,
    matched_count: usize,
    rank: f64,
) -> Result<SuccessionHit, Error> {
    let row: Option<(i64, Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT id, short_name, name FROM classification WHERE slug = ?",
            [terminal],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map(Some)
        .or_else(|err| match err {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            err => Err(err),
        })?;
    let (id, short_name, name) = match row {
        Some((id, short_name, name)) => (Some(id), short_name, name),
        None => (None, None, None),
    };
    Ok(SuccessionHit {
        id,
        fqid: fqid(&[Some("class".to_owned()), Some(terminal.to_owned())]),
        short_name,
        name,
        editions: editions(conn, terminal)?,
        matched_count,
        rank,
    })
}

/// Today's `_classification_editions`: the terminal and every predecessor, ordered
/// by distance from the terminal, then slug. An edition's year is that of the edge
/// that first reaches it. A read-time walk, not `classification_chain`: an
/// anchored chain walks back through one predecessor, so it drops a merge's other
/// branch (`api/search-classification-succession` has one).
fn editions(conn: &Connection, terminal: &str) -> Result<Vec<Edition>, Error> {
    let mut found: BTreeMap<String, (usize, Option<i64>)> =
        BTreeMap::from([(terminal.to_owned(), (0, None))]);
    let mut frontier = vec![terminal.to_owned()];
    let mut depth = 0;
    while !frontier.is_empty() {
        depth += 1;
        // Placeholders, as today, so SQLite visits the edges in today's order.
        let mut stmt = conn.prepare(&format!(
            "SELECT predecessor_slug, effective_year FROM classification_replaced_by \
             WHERE successor_slug IN ({})",
            vec!["?"; frontier.len()].join(",")
        ))?;
        let edges: Vec<(String, Option<i64>)> = stmt
            .query_map(params_from_iter(&frontier), |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
        frontier.clear();
        for (slug, year) in edges {
            if !found.contains_key(&slug) {
                found.insert(slug.clone(), (depth, year));
                frontier.push(slug);
            }
        }
    }
    let mut stmt = conn.prepare(&format!(
        "SELECT slug, name FROM classification WHERE slug IN ({})",
        vec!["?"; found.len()].join(",")
    ))?;
    let names: BTreeMap<String, Option<String>> = stmt
        .query_map(params_from_iter(found.keys()), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut editions: Vec<(usize, Edition)> = found
        .into_iter()
        .map(|(slug, (depth, effective_year))| {
            (
                depth,
                Edition {
                    fqid: fqid(&[Some("class".to_owned()), Some(slug.clone())]),
                    name: names.get(&slug).cloned().flatten(),
                    slug,
                    effective_year,
                },
            )
        })
        .collect();
    editions.sort_by(|a, b| (a.0, &a.1.slug).cmp(&(b.0, &b.1.slug)));
    Ok(editions.into_iter().map(|(_, e)| e).collect())
}
