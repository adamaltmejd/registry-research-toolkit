//! Linkage spaces: which census schemas share one person key (`crates/DESIGN.md`,
//! "Linkage spaces"). Compile and verify resolve them the same way, from the admitted
//! config and the census.
use std::collections::BTreeMap;
use std::path::Path;

use reg_readers::holdings as census;

use super::input;
use crate::error::Result;

/// The steward's spaces in config order, as (id, name), and each table's space.
pub(super) struct Spaces {
    pub list: Vec<(String, String)>,
    /// Census schema to space id; empty for the one implicit space.
    by_schema: BTreeMap<String, String>,
}

impl Spaces {
    /// The space of a census table. `resolve` mapped every census schema.
    pub fn of(&self, table: &census::Table) -> &str {
        match table.schema.as_ref() {
            Some(schema) if !self.by_schema.is_empty() => &self.by_schema[schema],
            _ => &self.list[0].0,
        }
    }
}

/// The spaces `config` declares over `tables`. With no `[[space]]` there is one implicit
/// space: the owner's id and the presentation's `name`.
pub(super) fn resolve(
    root: &Path,
    config: &input::Config,
    tables: &[census::Table],
) -> Result<Spaces> {
    if config.space.is_empty() {
        let presentation =
            std::fs::read_to_string(input::path(root, &config.presentation.steward)?)?;
        let name = serde_json::from_str::<serde_json::Value>(&presentation)
            .ok()
            .and_then(|value| value.get("name")?.as_str().map(str::to_owned))
            .ok_or("holdings_presentation_invalid: steward needs a string name")?;
        return Ok(Spaces {
            list: vec![(config.owner.clone(), name)],
            by_schema: BTreeMap::new(),
        });
    }
    if !super::compile::schema_keyed(tables) {
        return Err("config:space=1: holdings_space_invalid: spaces need a schema-keyed census (census_schema)".into());
    }
    let mut by_schema = BTreeMap::<String, String>::new();
    let mut list = Vec::new();
    for (n, space) in (1..).zip(&config.space) {
        let refuse = |why: String| -> crate::error::Error {
            format!("config:space={n}: holdings_space_invalid: {why}").into()
        };
        if !reg_core::is_slug(&space.id) {
            return Err(refuse("id must be a slug".into()));
        }
        if space.name.trim().is_empty() || space.schemas.is_empty() {
            return Err(refuse(
                "a space needs a name and at least one schema".into(),
            ));
        }
        if let Some(m) = (1..)
            .zip(&config.space)
            .take(n - 1)
            .find(|(_, other)| other.id == space.id || other.name == space.name)
            .map(|(m, _)| m)
        {
            return Err(refuse(format!("id or name repeats space={m}")));
        }
        for schema in &space.schemas {
            if !tables.iter().any(|t| t.schema.as_ref() == Some(schema)) {
                return Err(refuse(format!("schema {schema:?} is not in the census")));
            }
            if let Some(other) = by_schema.insert(schema.clone(), space.id.clone()) {
                return Err(refuse(format!(
                    "schema {schema:?} is also in space {other:?}"
                )));
            }
        }
        list.push((space.id.clone(), space.name.clone()));
    }
    if let Some(table) = tables
        .iter()
        .find(|t| t.schema.as_ref().is_none_or(|s| !by_schema.contains_key(s)))
    {
        return Err(format!(
            "census:row={}: holdings_space_unmapped: schema {:?} is in no space",
            table.source_row,
            table.schema.as_deref().unwrap_or("")
        )
        .into());
    }
    Ok(Spaces { list, by_schema })
}
