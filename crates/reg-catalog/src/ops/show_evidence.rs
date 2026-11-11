//! Variable source evidence, separate from the default curated summary.

use std::collections::BTreeMap;

use rusqlite::{Connection, Row};
use serde::Serialize;
use utoipa::ToSchema;

use super::docs::{self, RelatedDocument};
use super::refs::Entity;
use super::segments::StatementLocation;
use super::{Period, Server};
use crate::{Error, rows};

/// The provider's texts and the curated values behind them.
#[derive(Serialize, ToSchema)]
pub struct VariableEvidence {
    texts: Vec<ProviderText>,
    source_only: Vec<ProviderTextSource>,
    spellings: Vec<EvidenceSpelling>,
    statements: Vec<CurationStatement>,
    documents: Vec<RelatedDocument>,
}

/// One provider text, the years it covers and the deliveries it comes from.
#[derive(Serialize, ToSchema)]
pub struct ProviderText {
    text: String,
    periods: Vec<Period>,
    sources: Vec<ProviderTextSource>,
}

#[derive(Clone, Serialize, ToSchema)]
pub struct ProviderTextSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    field: Option<&'static str>,
    table: String,
    period: Period,
    status: String,
    source_key: String,
    #[schema(required = true)]
    spelling: Option<String>,
    #[schema(required = true)]
    label: Option<String>,
    #[schema(required = true)]
    unit: Option<String>,
}

/// A column name the variable is known by (case ignored).
#[derive(Serialize, ToSchema)]
pub struct EvidenceSpelling {
    spelling: String,
    kind: String,
}

/// A curated statement that supplied a field of this variable.
#[derive(Serialize, ToSchema)]
pub struct CurationStatement {
    field: String,
    #[schema(required = true)]
    value: Option<String>,
    statement: StatementLocation,
    #[schema(required = true)]
    caveat: Option<String>,
}

// An override row distinguishes a silent source (NULL) from no override. COALESCE
// would incorrectly present newly curated wording as the provider's original text.
const SOURCE_ROWS: &str = "SELECT o.id, t.pid AS table_pid, o.start, o.end, o.status,
    o.source_key,
    CASE WHEN spx.subject_id IS NULL THEN sp.exact ELSE spx.source_value END AS spelling,
    CASE WHEN lx.subject_id IS NULL THEN l.body ELSE lx.source_value END AS label,
    CASE WHEN dx.subject_id IS NULL THEN d.body ELSE dx.source_value END AS definition,
    CASE WHEN ox.subject_id IS NULL THEN op.body ELSE ox.source_value END AS operational_definition,
    CASE WHEN ux.subject_id IS NULL THEN u.body ELSE ux.source_value END AS unit
 FROM occurrence o
   JOIN tbl t ON t.id = o.table_id
   LEFT JOIN spelling sp ON sp.id = o.spelling_id
   LEFT JOIN text l ON l.id = o.label_text
   LEFT JOIN text d ON d.id = o.definition_text
   LEFT JOIN text op ON op.id = o.opdef_text
   LEFT JOIN text u ON u.id = o.unit_text
   LEFT JOIN override spx ON spx.subject_kind = 'occurrence' AND spx.subject_id = o.id
     AND spx.field = 'spelling'
   LEFT JOIN override lx ON lx.subject_kind = 'occurrence' AND lx.subject_id = o.id
     AND lx.field = 'label'
   LEFT JOIN override dx ON dx.subject_kind = 'occurrence' AND dx.subject_id = o.id
     AND dx.field = 'definition'
   LEFT JOIN override ox ON ox.subject_kind = 'occurrence' AND ox.subject_id = o.id
     AND ox.field = 'operational_definition'
   LEFT JOIN override ux ON ux.subject_kind = 'occurrence' AND ux.subject_id = o.id
     AND ux.field = 'unit'
 WHERE o.variable_id = ?1";

struct SourceRow {
    start: i64,
    end: i64,
    definition: Option<String>,
    operational_definition: Option<String>,
    source: ProviderTextSource,
}

fn source_row(server: &Server, row: &Row<'_>) -> rusqlite::Result<SourceRow> {
    Ok(SourceRow {
        start: row.get(2)?,
        end: row.get(3)?,
        definition: row.get(8)?,
        operational_definition: row.get(9)?,
        source: ProviderTextSource {
            field: None,
            table: row.get(1)?,
            period: server.period(row.get(2)?, row.get(3)?),
            status: row.get(4)?,
            source_key: row.get(5)?,
            spelling: row.get(6)?,
            label: row.get(7)?,
            unit: row.get(10)?,
        },
    })
}

/// A used occurrence's provider wording: its table, `[start, end)` and its distinct
/// source texts (definition, operational definition), one per line; `None` when the
/// source is silent. Source values, never a curated correction (design §2.4).
pub(super) struct Wording {
    pub table: String,
    pub start: i64,
    pub end: i64,
    pub text: Option<String>,
}

/// The variable's used occurrences' wordings, latest first, then in source order.
pub(super) fn wordings(conn: &Connection, variable: i64) -> Result<Vec<Wording>, Error> {
    Ok(rows(
        conn,
        &format!(
            "{SOURCE_ROWS} AND o.status = 'used'
             ORDER BY o.start DESC, o.end DESC, t.pid, o.source_key, spelling, o.id"
        ),
        [variable],
        |row| {
            let definition: Option<String> = row.get(8)?;
            let operational: Option<String> = row.get(9)?;
            Ok(Wording {
                table: row.get(1)?,
                start: row.get(2)?,
                end: row.get(3)?,
                text: reg_core::provider_wording(definition.as_deref(), operational.as_deref()),
            })
        },
    )?)
}

/// All source occurrences, including shadowed evidence, in table/period/key order.
pub(super) fn load(
    server: &Server,
    conn: &Connection,
    entity: &Entity,
) -> Result<VariableEvidence, Error> {
    let rows = rows(
        conn,
        &format!("{SOURCE_ROWS} ORDER BY t.pid, o.start, o.end, o.source_key, spelling, o.id"),
        [entity.id],
        |row| source_row(server, row),
    )?;
    let mut text_indices = BTreeMap::new();
    let mut grouped: Vec<(ProviderText, Vec<reg_core::days::Span>)> = Vec::new();
    let mut source_only = Vec::new();
    for row in rows {
        let texts = reg_core::provider_texts(
            row.definition.as_deref(),
            row.operational_definition.as_deref(),
        );
        if texts.is_empty() {
            source_only.push(row.source);
            continue;
        }
        for (field, text) in texts {
            let index = *text_indices
                .entry(reg_core::wording_key(text))
                .or_insert_with(|| {
                    grouped.push((
                        ProviderText {
                            text: text.to_owned(),
                            periods: Vec::new(),
                            sources: Vec::new(),
                        },
                        Vec::new(),
                    ));
                    grouped.len() - 1
                });
            let (group, spans) = &mut grouped[index];
            let mut source = row.source.clone();
            source.field = Some(field);
            group.sources.push(source);
            spans.push((row.start, row.end));
        }
    }
    let texts = grouped
        .into_iter()
        .map(|(mut text, spans)| {
            text.periods = reg_core::days::merge(spans)
                .into_iter()
                .map(|(start, end)| server.period(start, end))
                .collect();
            text
        })
        .collect();
    let spellings = crate::rows(
        conn,
        "SELECT f.folded, a.kind FROM alias a JOIN name_fold f ON f.id = a.fold_id
             WHERE a.variable_id = ? ORDER BY f.folded",
        [entity.id],
        |row| {
            Ok(EvidenceSpelling {
                spelling: row.get(0)?,
                kind: row.get(1)?,
            })
        },
    )?;
    Ok(VariableEvidence {
        texts,
        source_only,
        spellings,
        statements: statements(conn, entity.id)?,
        documents: docs::related_documents(
            conn,
            &entity.kind,
            rusqlite::types::Value::Integer(entity.id),
        )?,
    })
}

fn statements(conn: &Connection, variable: i64) -> Result<Vec<CurationStatement>, Error> {
    Ok(rows(
        conn,
        "WITH decisions AS (
               SELECT field, value, statement_id FROM override
               WHERE subject_kind = 'variable' AND subject_id = ?1
               UNION
               SELECT x.field, x.value, x.statement_id FROM occurrence o
                 JOIN override x ON x.subject_kind = 'occurrence' AND x.subject_id = o.id
               WHERE o.variable_id = ?1)
             SELECT x.field, x.value, s.file, s.line, c.body
             FROM decisions x JOIN statement s ON s.id = x.statement_id
               LEFT JOIN text c ON c.id = s.caveat_text
             ORDER BY s.file, s.line, x.field, x.value",
        [variable],
        |row| {
            Ok(CurationStatement {
                field: row.get(0)?,
                value: row.get(1)?,
                statement: StatementLocation {
                    file: row.get(2)?,
                    line: row.get(3)?,
                },
                caveat: row.get(4)?,
            })
        },
    )?)
}
