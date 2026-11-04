//! Variable source evidence, separate from the default curated summary.

use std::collections::BTreeMap;

use rusqlite::{Connection, Row};
use serde::Serialize;
use utoipa::ToSchema;

use super::docs::{self, RelatedDocument};
use super::refs::Entity;
use super::segments::{Locator, StatementLocation};
use super::{Period, Server};
use crate::{Error, rows};

/// Provider text and curation provenance, returned only when evidence is requested.
#[derive(Serialize, ToSchema)]
pub struct VariableEvidence {
    texts: Vec<ProviderText>,
    source_only: Vec<ProviderTextSource>,
    spellings: Vec<EvidenceSpelling>,
    statements: Vec<CurationStatement>,
    documents: Vec<RelatedDocument>,
}

/// One provider text, with its coverage and every contributing source occurrence.
#[derive(Serialize, ToSchema)]
pub struct ProviderText {
    text: String,
    periods: Vec<Period>,
    sources: Vec<ProviderTextSource>,
}

/// Source values remain distinct from curated winners, including silent sources.
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
    locators: Vec<Locator>,
}

/// A delivered or curated folded alias; source occurrences retain exact spellings.
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

/// Latest provider text, used only when no curated description exists.
#[derive(Serialize, ToSchema)]
pub struct SourceDefinition {
    origin: &'static str,
    period: Period,
    text: String,
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
    id: i64,
    start: i64,
    end: i64,
    definition: Option<String>,
    operational_definition: Option<String>,
    source: ProviderTextSource,
}

fn source_row(server: &Server, row: &Row<'_>) -> rusqlite::Result<SourceRow> {
    Ok(SourceRow {
        id: row.get(0)?,
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
            locators: Vec::new(),
        },
    })
}

fn normalized_text(text: &str) -> String {
    reg_core::normalized_search_query(text)
        .trim_end_matches(|c: char| c.is_whitespace() || reg_core::is_punctuation(c))
        .to_owned()
}

// Compare normalized wording but retain the provider's original display strings.
fn provider_texts<'a>(
    definition: Option<&'a str>,
    operational: Option<&'a str>,
) -> Vec<(&'static str, &'a str)> {
    let definition = definition.filter(|text| !text.trim().is_empty());
    let operational = operational.filter(|text| !text.trim().is_empty());
    match (definition, operational) {
        (Some(definition), Some(operational)) => {
            let d = normalized_text(definition);
            let o = normalized_text(operational);
            if d.contains(&o) {
                vec![("definition", definition)]
            } else if o.contains(&d) {
                vec![("operational_definition", operational)]
            } else {
                vec![
                    ("definition", definition),
                    ("operational_definition", operational),
                ]
            }
        }
        (Some(text), None) => vec![("definition", text)],
        (None, Some(text)) => vec![("operational_definition", text)],
        (None, None) => Vec::new(),
    }
}

/// All source occurrences, including shadowed evidence, in table/period/key order.
pub(super) fn load(
    server: &Server,
    conn: &Connection,
    entity: &Entity,
) -> Result<VariableEvidence, Error> {
    let mut rows = rows(
        conn,
        &format!("{SOURCE_ROWS} ORDER BY t.pid, o.start, o.end, o.source_key, spelling, o.id"),
        [entity.id],
        |row| source_row(server, row),
    )?;
    let mut locators = conn.prepare(
        "SELECT i.name, l.locator
         FROM occurrence_locator l JOIN input i ON i.id = l.input_id
         WHERE l.occurrence_id = ? ORDER BY i.name, l.locator",
    )?;
    for row in &mut rows {
        row.source.locators = locators
            .query_map([row.id], |row| {
                Ok(Locator {
                    input: row.get(0)?,
                    locator: row.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    let mut text_indices = BTreeMap::new();
    let mut grouped: Vec<(ProviderText, Vec<reg_core::Interval>)> = Vec::new();
    let mut source_only = Vec::new();
    for row in rows {
        let texts = provider_texts(
            row.definition.as_deref(),
            row.operational_definition.as_deref(),
        );
        if texts.is_empty() {
            source_only.push(row.source);
            continue;
        }
        for (field, text) in texts {
            let index = *text_indices
                .entry(normalized_text(text))
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
            // The shared interval algebra uses inclusive ISO endpoints; catalog
            // periods are half-open, including the open-end sentinel.
            spans.push((super::iso_day(row.start), super::iso_day(row.end - 1)));
        }
    }
    let texts = grouped
        .into_iter()
        .map(|(mut text, spans)| {
            text.periods = reg_core::merge(spans)
                .into_iter()
                .map(|(start, end)| {
                    server.period(
                        reg_core::iso_day_number(&start).expect("catalog ISO start"),
                        reg_core::iso_day_number(&end).expect("catalog ISO end") + 1,
                    )
                })
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
        documents: docs::related_documents(conn, entity)?,
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
             SELECT x.field, x.value, s.file, s.line, e.body, c.body
             FROM decisions x JOIN statement s ON s.id = x.statement_id
               LEFT JOIN text e ON e.id = s.evidence_text
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
                evidence: row.get(4)?,
                caveat: row.get(5)?,
            })
        },
    )?)
}

/// The two provider text fields from the same latest used occurrence; never a pair
/// assembled from different periods. An entirely silent source yields no fallback.
pub(super) fn latest_source_text(
    server: &Server,
    conn: &Connection,
    variable: i64,
) -> Result<Option<SourceDefinition>, Error> {
    let mut statement = conn.prepare(&format!(
        "{SOURCE_ROWS} AND o.status = 'used'
         ORDER BY o.start DESC, o.end DESC, t.pid, o.source_key, spelling, o.id"
    ))?;
    let mut rows = statement.query([variable])?;
    while let Some(row) = rows.next()? {
        let definition: Option<String> = row.get(8)?;
        let operational: Option<String> = row.get(9)?;
        let texts = provider_texts(definition.as_deref(), operational.as_deref());
        if !texts.is_empty() {
            return Ok(Some(SourceDefinition {
                origin: "provider",
                period: server.period(row.get(2)?, row.get(3)?),
                text: texts
                    .into_iter()
                    .map(|(_, text)| text)
                    .collect::<Vec<_>>()
                    .join("\n"),
            }));
        }
    }
    Ok(None)
}
