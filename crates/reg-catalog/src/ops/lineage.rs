//! `lineage`: a variable's consumer-side lineage edges and build warnings (today's
//! `Catalog.lineage` and `/lineage_warnings`) and the per-register provenance of
//! its name (today's `get lineage`).

use reg_core::py_strip;
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use super::refs::{self, Target, fqid};
use super::show::rows;
use super::states::storage_id;
use super::{Params, Server};
use crate::held::{self, Narrow};
use crate::{Code, Error, Scope};

#[derive(Serialize, ToSchema)]
pub struct Lineage {
    edges: Vec<LineageEdge>,
    warnings: Vec<LineageWarning>,
    /// Each variable in scope with the variable's name (compared case-insensitively
    /// in ASCII), across registers: whether its register is the variable's source.
    registers: Vec<Provenance>,
}

/// A state of the variable fed by a state of a source variable over the two
/// states' intersection.
#[derive(Serialize, ToSchema)]
pub struct LineageEdge {
    #[serde(serialize_with = "storage_id")]
    #[schema(value_type = String)]
    consumer_state_id: i64,
    #[serde(serialize_with = "storage_id")]
    #[schema(value_type = String)]
    source_state_id: i64,
    valid_from: String,
    valid_to: String,
    /// The source state's variable.
    source_fqid: Option<String>,
}

/// A build-time warning on one of the variable's states: `no_source_state` or
/// `ambiguous_source_variant`.
#[derive(Serialize, ToSchema)]
pub struct LineageWarning {
    #[serde(serialize_with = "storage_id")]
    #[schema(value_type = String)]
    consumer_state_id: i64,
    warning_kind: String,
    message: String,
}

/// A variable's provenance in its register.
#[derive(Serialize, ToSchema)]
pub struct Provenance {
    register: Option<String>,
    register_name: Option<String>,
    variable: Option<String>,
    /// `source` when the register is its own source, `consumer` when it names
    /// another, `unknown` without a source text.
    role: &'static str,
    source_register_text: String,
    /// The register the source text was resolved to.
    source_register: Option<String>,
    /// Its states.
    instance_count: i64,
    /// The first and last year its states span (an open end counts its start
    /// year); empty without a dated state.
    year_range: Vec<i64>,
}

pub fn lineage(server: &Server, scope: Scope, params: &Params) -> Result<Value, Error> {
    let conn = server.catalog.connect()?;
    let value = params.get("ref").copied().unwrap_or_default();
    let Target::Variable { id } = refs::resolve(&conn, scope, Some(value))? else {
        return Err(Error::new(
            Code::InvalidRef,
            format!("{value:?} is not a variable ref."),
            vec![value.into()],
        ));
    };
    let edges = rows(
        &conn,
        "SELECT l.consumer_state_id, l.source_state_id, l.valid_from, l.valid_to, \
         sp.slug, sr.slug, sv.slug FROM variable_state_lineage l \
         JOIN variable_state cs ON l.consumer_state_id = cs.state_id \
         JOIN variable_state ss ON l.source_state_id = ss.state_id \
         JOIN variable sv ON ss.variable_id = sv.variable_id \
         JOIN register sr ON sv.register_id = sr.register_id \
         JOIN provider sp ON sr.provider_id = sp.provider_id \
         WHERE cs.variable_id = ? ORDER BY l.consumer_state_id, l.source_state_id",
        [id],
        |row| {
            Ok(LineageEdge {
                consumer_state_id: row.get(0)?,
                source_state_id: row.get(1)?,
                valid_from: row.get(2)?,
                valid_to: row.get(3)?,
                source_fqid: fqid(&[row.get(4)?, row.get(5)?, row.get(6)?]),
            })
        },
    )?;
    let warnings = rows(
        &conn,
        "SELECT w.consumer_state_id, w.warning_kind, w.message \
         FROM variable_state_lineage_warning w \
         JOIN variable_state cs ON w.consumer_state_id = cs.state_id \
         WHERE cs.variable_id = ? ORDER BY w.consumer_state_id, w.warning_kind",
        [id],
        |row| {
            Ok(LineageWarning {
                consumer_state_id: row.get(0)?,
                warning_kind: row.get(1)?,
                message: row.get(2)?,
            })
        },
    )?;
    // Today's `get lineage <name>`, whose match is SQLite's ASCII `LOWER`; a
    // variable without a name matches only itself.
    let sql = format!(
        "SELECT p.slug, r.slug, r.name, v.slug, v.source_register_text, \
         v.source_register_id = v.register_id, sp.slug, sr.slug, \
         (SELECT COUNT(*) FROM variable_state s WHERE s.variable_id = v.variable_id), \
         (SELECT MIN(CAST(substr(s.valid_from, 1, 4) AS INTEGER)) FROM variable_state s \
          WHERE s.variable_id = v.variable_id AND s.valid_from IS NOT NULL AND s.valid_to IS NOT NULL), \
         (SELECT MAX(CAST(substr(CASE WHEN s.valid_to >= '9999' THEN s.valid_from \
          ELSE s.valid_to END, 1, 4) AS INTEGER)) FROM variable_state s \
 WHERE s.variable_id = v.variable_id AND s.valid_from IS NOT NULL AND s.valid_to IS NOT NULL) \
         FROM variable v JOIN register r ON r.register_id = v.register_id \
         JOIN provider p ON p.provider_id = r.provider_id \
         LEFT JOIN register sr ON sr.register_id = v.source_register_id \
         LEFT JOIN provider sp ON sp.provider_id = sr.provider_id \
         WHERE (v.variable_id = ?1 OR LOWER(v.name) = (SELECT LOWER(name) FROM variable \
         WHERE variable_id = ?1)) AND {} ORDER BY p.slug, r.slug, v.slug",
        held::variable(scope, "v.variable_id", Narrow::default())
    );
    let registers = rows(&conn, &sql, [id], |row| {
        let provider: Option<String> = row.get(0)?;
        let register: Option<String> = row.get(1)?;
        let text = py_strip(&row.get::<_, Option<String>>(4)?.unwrap_or_default()).to_owned();
        let own_source: Option<bool> = row.get(5)?;
        let role = if text.is_empty() {
            "unknown"
        } else if own_source == Some(true) {
            "source"
        } else {
            "consumer"
        };
        let years: [Option<i64>; 2] = [row.get(9)?, row.get(10)?];
        Ok(Provenance {
            register: fqid(&[provider.clone(), register.clone()]),
            register_name: row.get(2)?,
            variable: fqid(&[provider, register, row.get(3)?]),
            role,
            source_register_text: text,
            source_register: fqid(&[row.get(6)?, row.get(7)?]),
            instance_count: row.get(8)?,
            year_range: match years {
                [Some(lo), Some(hi)] => vec![lo, hi],
                _ => Vec::new(),
            },
        })
    })?;
    Ok(serde_json::to_value(Lineage {
        edges,
        warnings,
        registers,
    })
    .expect("Lineage serializes"))
}
