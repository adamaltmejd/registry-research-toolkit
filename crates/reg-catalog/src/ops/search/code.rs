//! The code arms: value labels by full text and, for a code-shaped query, codes by
//! exact or prefix match, split by owner into classification codes and register-local
//! value sets (today's `_search_values_fts` with `code_owner_scope`), with at most
//! five owners of each kind on a shown code (`_code_owner_annotations_batch`). Codes
//! keep reference semantics in every scope and carry no validity window.

use std::cmp::Reverse;
use std::collections::BTreeMap;

use reg_core::py_strip;
use rusqlite::types::Value as Sql;
use rusqlite::{Connection, Row, params_from_iter};
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;

use super::{HORIZON, Hit, Request, SearchHit, fqid, is_code_shaped, like_escape};
use crate::Error;

/// Owners shown per code (today's `_CODE_OWNERS_PER_HIT`).
const OWNERS: usize = 5;
/// Code-shaped matches rank ahead of every label hit.
const CODE_RANK_BASE: f64 = -1_000_000.0;

pub(super) struct CodeHit {
    pub id: i64,
    pub code: String,
    pub label: String,
    mapping_count: i64,
    pub code_system: Option<String>,
    pub rank: f64,
    owners: Owners,
}

/// A code's owners, filled on the shown page only.
#[derive(Default)]
struct Owners {
    variable_count: i64,
    variables: Vec<CodeVariable>,
    classification_count: i64,
    classifications: Vec<CodeClassification>,
}

#[derive(Serialize, ToSchema)]
pub struct CodeVariable {
    fqid: Option<String>,
    name: Option<String>,
    register_name: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct CodeClassification {
    fqid: Option<String>,
    short_name: Option<String>,
    name: Option<String>,
}

impl Hit {
    /// Today's `_rank_codes` key of a code, descending: classification-owned first,
    /// then more classifications, then more variables.
    pub(super) fn owner_rank(&self) -> Option<Reverse<(bool, i64, i64)>> {
        let Self::Code(code) = self else {
            return None;
        };
        let o = &code.owners;
        Some(Reverse((
            o.classification_count > 0,
            o.classification_count,
            o.variable_count,
        )))
    }
}

impl CodeHit {
    pub(super) fn into_item(self) -> SearchHit {
        SearchHit::Code {
            code: self.code,
            label: self.label,
            code_system: self.code_system,
            variable_count: self.owners.variable_count,
            variables: self.owners.variables,
            classification_count: self.owners.classification_count,
            classifications: self.owners.classifications,
        }
    }
}

/// The arm's rows before ranking: classification-owned codes, or register-local
/// ones (owned by a variable and no classification), narrowed to `register`'s
/// owners when given.
pub(super) fn arm(
    conn: &Connection,
    request: &Request,
    fts: &str,
    classification_owned: bool,
) -> Result<Vec<Hit>, Error> {
    let classified = "EXISTS (SELECT 1 FROM classification_code cc WHERE cc.code_id = vc.code_id)";
    let mut owner = if classification_owned {
        format!(" AND {classified}")
    } else {
        format!(" AND vc.mapping_count > 0 AND NOT {classified}")
    };
    let mut owner_args: Vec<Sql> = Vec::new();
    if let Some(register) = request.register {
        owner += " AND EXISTS (SELECT 1 FROM code_variable_map cvm JOIN variable v_scope \
             ON v_scope.variable_id = cvm.variable_id \
             WHERE cvm.code_id = vc.code_id AND v_scope.register_id = ?)";
        owner_args.push(register.into());
    }
    // The first owning classification's short name, by short name. A fact about the
    // code, so a register scope keeps it.
    let code_system = "(SELECT COALESCE(NULLIF(c.short_name, ''), c.name) \
         FROM classification_code cc JOIN classification c ON c.id = cc.classification_id \
         WHERE cc.code_id = vc.code_id ORDER BY c.short_name LIMIT 1)";
    let read = |row: &Row, rank: f64| -> rusqlite::Result<CodeHit> {
        Ok(CodeHit {
            id: row.get(0)?,
            code: row.get(1)?,
            label: row.get(2)?,
            mapping_count: row.get(3)?,
            code_system: row.get(4)?,
            rank,
            owners: Owners::default(),
        })
    };
    let mut hits: BTreeMap<i64, CodeHit> = BTreeMap::new();
    // Bound on the published rank (bm25 plus a rarity penalty), so the prefix holds
    // exactly the best rows.
    let mut args: Vec<Sql> = vec![fts.to_owned().into()];
    args.extend(owner_args.iter().cloned());
    let mut stmt = conn.prepare(&format!(
        "SELECT vc.code_id, vc.code, vc.label, vc.mapping_count, {code_system}, \
         bm25(value_code_fts) + ln(1 + vc.mapping_count) * 0.5 AS rank \
         FROM value_code_fts JOIN value_code vc ON vc.code_id = value_code_fts.rowid \
         WHERE value_code_fts MATCH ?{owner} \
         ORDER BY rank, vc.mapping_count, vc.code_id LIMIT {HORIZON}"
    ))?;
    for hit in stmt.query_map(params_from_iter(&args), |row| read(row, row.get(5)?))? {
        let hit = hit?;
        hits.insert(hit.id, hit);
    }
    if is_code_shaped(request.q) {
        let q = py_strip(request.q);
        let mut args: Vec<Sql> = vec![format!("{}%", like_escape(q)).into()];
        args.extend(owner_args);
        args.push(q.to_owned().into());
        // The prefix LIKE also holds every exact (NOCASE) match: both fold ASCII
        // case only. A lone LIKE term lets idx_value_code_code_nocase serve it.
        let mut stmt = conn.prepare(&format!(
            "SELECT vc.code_id, vc.code, vc.label, vc.mapping_count, {code_system} \
             FROM value_code vc WHERE vc.code LIKE ? ESCAPE '\\' \
             AND (vc.mapping_count > 0 OR {classified}){owner} \
             ORDER BY (vc.code = ? COLLATE NOCASE) DESC, length(vc.code), vc.code, vc.code_id \
             LIMIT {HORIZON}"
        ))?;
        let rows: Vec<CodeHit> = stmt
            .query_map(params_from_iter(&args), |row| read(row, 0.0))?
            .collect::<rusqlite::Result<_>>()?;
        let mut rank = CODE_RANK_BASE;
        for mut hit in rows {
            hit.rank = rank;
            rank += 1.0;
            if hits.get(&hit.id).is_none_or(|h| hit.rank < h.rank) {
                hits.insert(hit.id, hit);
            }
        }
    }
    let mut hits: Vec<CodeHit> = hits.into_values().collect();
    hits.sort_by(|a, b| {
        a.rank
            .total_cmp(&b.rank)
            .then_with(|| (&a.code, a.id).cmp(&(&b.code, b.id)))
    });
    hits.truncate(HORIZON);
    Ok(hits.into_iter().map(Hit::Code).collect())
}

/// Fill the owners of the shown codes: the variable owners (in `register` only, when
/// given) by their own code count ascending, the classification owners by short
/// name, at most five of each, with full counts. A register scope narrows only the
/// variable owners: which classifications own a code is a fact about the code.
pub(super) fn annotate(
    conn: &Connection,
    register: Option<i64>,
    shown: &mut [Hit],
) -> Result<(), Error> {
    let mut codes: BTreeMap<i64, &mut CodeHit> = shown
        .iter_mut()
        .filter_map(|hit| match hit {
            Hit::Code(c) => Some((c.id, c)),
            _ => None,
        })
        .collect();
    if codes.is_empty() {
        return Ok(());
    }
    let ids = json!(codes.keys().collect::<Vec<_>>()).to_string();
    let (in_register, mut args) = match register {
        Some(register) => (" AND v.register_id = ?", vec![Sql::from(register)]),
        None => ("", Vec::new()),
    };
    args.insert(0, ids.clone().into());
    // `mapping_count` is the unscoped variable count; a register scope counts its own.
    let mut counts: BTreeMap<i64, i64> = BTreeMap::new();
    if register.is_some() {
        let mut stmt = conn.prepare(&format!(
            "SELECT cvm.code_id, COUNT(*) FROM code_variable_map cvm \
             JOIN variable v ON cvm.variable_id = v.variable_id \
             WHERE cvm.code_id IN (SELECT value FROM json_each(?)){in_register} \
             GROUP BY cvm.code_id"
        ))?;
        counts = stmt
            .query_map(params_from_iter(&args), |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    for (id, code) in &mut codes {
        code.owners.variable_count = match register {
            Some(_) => counts.get(id).copied().unwrap_or(0),
            None => code.mapping_count,
        };
    }
    let mut stmt = conn.prepare(&format!(
        "WITH owners AS (SELECT cvm.code_id, v.variable_id, v.name, v.slug AS variable_slug, \
         r.name AS register_name, r.slug AS register_slug, p.slug AS provider_slug, \
         (SELECT COUNT(*) FROM code_variable_map c2 WHERE c2.variable_id = v.variable_id) AS n \
         FROM code_variable_map cvm JOIN variable v ON cvm.variable_id = v.variable_id \
         JOIN register r ON v.register_id = r.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         WHERE cvm.code_id IN (SELECT value FROM json_each(?)){in_register}), \
         ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY code_id ORDER BY n, \
         variable_slug, provider_slug, register_slug, variable_id) AS rn FROM owners) \
         SELECT code_id, provider_slug, register_slug, variable_slug, name, register_name \
         FROM ranked WHERE rn <= {OWNERS} ORDER BY code_id, rn"
    ))?;
    for row in stmt.query_map(params_from_iter(&args), |row| {
        Ok((
            row.get::<_, i64>(0)?,
            CodeVariable {
                fqid: fqid(&[row.get(1)?, row.get(2)?, row.get(3)?]),
                name: row.get(4)?,
                register_name: row.get(5)?,
            },
        ))
    })? {
        let (id, owner) = row?;
        codes
            .get_mut(&id)
            .expect("a shown code")
            .owners
            .variables
            .push(owner);
    }
    let mut stmt = conn.prepare(&format!(
        "WITH owners AS (SELECT cc.code_id, c.short_name, c.name, c.slug, \
         COUNT(*) OVER (PARTITION BY cc.code_id) AS n, \
         ROW_NUMBER() OVER (PARTITION BY cc.code_id ORDER BY c.short_name) AS rn \
         FROM classification_code cc JOIN classification c ON c.id = cc.classification_id \
         WHERE cc.code_id IN (SELECT value FROM json_each(?))) \
         SELECT code_id, n, slug, short_name, name FROM owners WHERE rn <= {OWNERS} \
         ORDER BY code_id, rn"
    ))?;
    for row in stmt.query_map([ids], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            CodeClassification {
                fqid: fqid(&[Some("class".to_owned()), row.get(2)?]),
                short_name: row.get(3)?,
                name: row.get(4)?,
            },
        ))
    })? {
        let (id, count, owner) = row?;
        let owners = &mut codes.get_mut(&id).expect("a shown code").owners;
        owners.classification_count = count;
        owners.classifications.push(owner);
    }
    Ok(())
}
