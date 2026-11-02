//! Variable source evidence, separate from the default curated summary.

use rusqlite::{Connection, OptionalExtension, Row};
use serde::Serialize;
use utoipa::ToSchema;

use super::docs::{self, RelatedDocument};
use super::refs::Entity;
use super::segments::{Locator, StatementLocation};
use super::{Period, Server};
use crate::Error;

/// Provider text and curation provenance, returned only when evidence is requested.
#[derive(Serialize, ToSchema)]
pub struct VariableEvidence {
    texts: Vec<ProviderText>,
    spellings: Vec<EvidenceSpelling>,
    statements: Vec<CurationStatement>,
    documents: Vec<RelatedDocument>,
}

/// One documented occurrence; source values remain distinct from curated winners.
#[derive(Serialize, ToSchema)]
pub struct ProviderText {
    table: String,
    period: Period,
    status: String,
    source_key: String,
    #[schema(required = true)]
    spelling: Option<String>,
    #[schema(required = true)]
    label: Option<String>,
    #[schema(required = true)]
    definition: Option<String>,
    #[schema(required = true)]
    operational_definition: Option<String>,
    #[schema(required = true)]
    unit: Option<String>,
    locators: Vec<Locator>,
}

/// A delivered or curated folded alias; exact source spellings are in `texts`.
#[derive(Serialize, ToSchema)]
pub struct EvidenceSpelling {
    spelling: String,
    kind: String,
}

/// An authored curation statement that contributed a field of this variable.
#[derive(Serialize, ToSchema)]
pub struct CurationStatement {
    field: String,
    #[schema(required = true)]
    value: Option<String>,
    statement: StatementLocation,
    #[schema(required = true)]
    evidence: Option<String>,
    #[schema(required = true)]
    caveat: Option<String>,
}

/// Latest provider definition pair, used only when no curated description exists.
#[derive(Serialize, ToSchema)]
pub struct SourceDefinition {
    origin: &'static str,
    period: Period,
    #[schema(required = true)]
    definition: Option<String>,
    #[schema(required = true)]
    operational_definition: Option<String>,
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

fn provider_text(server: &Server, row: &Row<'_>) -> rusqlite::Result<(i64, ProviderText)> {
    Ok((
        row.get(0)?,
        ProviderText {
            table: row.get(1)?,
            period: server.period(row.get(2)?, row.get(3)?),
            status: row.get(4)?,
            source_key: row.get(5)?,
            spelling: row.get(6)?,
            label: row.get(7)?,
            definition: row.get(8)?,
            operational_definition: row.get(9)?,
            unit: row.get(10)?,
            locators: Vec::new(),
        },
    ))
}

/// All source occurrences, including shadowed evidence, in table/period/key order.
pub(super) fn load(
    server: &Server,
    conn: &Connection,
    entity: &Entity,
) -> Result<VariableEvidence, Error> {
    let mut texts = conn
        .prepare(&format!(
            "{SOURCE_ROWS} ORDER BY t.pid, o.start, o.end, o.source_key, spelling, o.id"
        ))?
        .query_map([entity.id], |row| provider_text(server, row))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut locators = conn.prepare(
        "SELECT i.name, l.locator
         FROM occurrence_locator l JOIN input i ON i.id = l.input_id
         WHERE l.occurrence_id = ? ORDER BY i.name, l.locator",
    )?;
    for (id, text) in &mut texts {
        text.locators = locators
            .query_map([*id], |row| {
                Ok(Locator {
                    input: row.get(0)?,
                    locator: row.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    let spellings = conn
        .prepare(
            "SELECT f.folded, a.kind FROM alias a JOIN name_fold f ON f.id = a.fold_id
             WHERE a.variable_id = ? ORDER BY f.folded",
        )?
        .query_map([entity.id], |row| {
            Ok(EvidenceSpelling {
                spelling: row.get(0)?,
                kind: row.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(VariableEvidence {
        texts: texts.into_iter().map(|(_, text)| text).collect(),
        spellings,
        statements: statements(conn, entity.id)?,
        documents: docs::related_documents(conn, entity)?,
    })
}

fn statements(conn: &Connection, variable: i64) -> Result<Vec<CurationStatement>, Error> {
    Ok(conn
        .prepare(
            "WITH decisions AS (
               SELECT field, value, statement_id FROM override
               WHERE subject_kind = 'variable' AND subject_id = ?1
               UNION
               SELECT x.field, x.value, x.statement_id FROM occurrence o
                 JOIN override x ON x.subject_kind = 'occurrence' AND x.subject_id = o.id
               WHERE o.variable_id = ?1)
             SELECT x.field, x.value, s.file, s.line, e.body, c.body
             FROM decisions x JOIN statement s ON s.id = x.statement_id
               LEFT JOIN text e ON e.id = s.evidence_text
               LEFT JOIN text c ON c.id = s.caveat_text
             ORDER BY s.file, s.line, x.field, x.value",
        )?
        .query_map([variable], |row| {
            Ok(CurationStatement {
                field: row.get(0)?,
                value: row.get(1)?,
                statement: StatementLocation {
                    file: row.get(2)?,
                    line: row.get(3)?,
                },
                evidence: row.get(4)?,
                caveat: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

/// The two provider text fields from the same latest used occurrence; never a pair
/// assembled from different periods. An entirely silent source yields no fallback.
pub(super) fn latest_source_text(
    server: &Server,
    conn: &Connection,
    variable: i64,
) -> Result<Option<SourceDefinition>, Error> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT start, end, definition, operational_definition FROM ({SOURCE_ROWS})
                 WHERE status = 'used' AND
                   (NULLIF(TRIM(definition), '') IS NOT NULL
                    OR NULLIF(TRIM(operational_definition), '') IS NOT NULL)
                 ORDER BY start DESC, end DESC, table_pid, source_key, spelling, id LIMIT 1"
            ),
            [variable],
            |row| {
                Ok(SourceDefinition {
                    origin: "provider",
                    period: server.period(row.get(0)?, row.get(1)?),
                    definition: row.get(2)?,
                    operational_definition: row.get(3)?,
                })
            },
        )
        .optional()?)
}
