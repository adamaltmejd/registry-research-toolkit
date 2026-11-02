//! The shared thematic vocabulary and final register-local picking units.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use reg_frame::statement::Statement;
use reg_readers::curation::LocatedStatement;
use serde::Deserialize;

/// Stable build-side membership refusal codes (the runtime API has its own registry).
enum MembershipCode {
    MultipleGroups,
    TooSmall,
}

impl MembershipCode {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::MultipleGroups => "variable-multiple-groups",
            Self::TooSmall => "set-too-small",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tag {
    pub id: String,
    pub label: String,
    pub definition: String,
}

pub fn vocabulary(root: &Path) -> Result<BTreeMap<String, Tag>, String> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Vocabulary {
        #[serde(default)]
        tag: Vec<toml::Spanned<toml::Table>>,
    }
    let path = root.join("tag-vocabulary.toml");
    // Synthetic trees with no tag statements need no vocabulary.
    if !path
        .try_exists()
        .map_err(|e| format!("{}: {e}", path.display()))?
    {
        return Ok(BTreeMap::new());
    }
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let line = |offset| text[..offset].bytes().filter(|&b| b == b'\n').count() + 1;
    let parsed: Vocabulary = toml::from_str(&text).map_err(|e: toml::de::Error| {
        format!(
            "{}:{}: tag-vocabulary: {}",
            path.display(),
            line(e.span().map_or(0, |s| s.start)),
            e.message()
        )
    })?;
    let mut tags = BTreeMap::new();
    for raw in parsed.tag {
        let at = format!(
            "{}:{}: tag-vocabulary",
            path.display(),
            line(raw.span().start)
        );
        let tag: Tag = toml::Value::Table(raw.into_inner())
            .try_into()
            .map_err(|e: toml::de::Error| format!("{at}: {}", e.message()))?;
        if !reg_core::is_slug(&tag.id) {
            return Err(format!("{at}: invalid tag slug {:?}", tag.id));
        }
        if tag.label.trim().is_empty() || tag.definition.trim().is_empty() {
            return Err(format!(
                "{at}: Swedish label and definition must not be blank"
            ));
        }
        let id = tag.id.clone();
        if tags.insert(id.clone(), tag).is_some() {
            return Err(format!("{at}: duplicate tag {id:?}"));
        }
    }
    Ok(tags)
}

pub fn statements(
    tags: &BTreeMap<String, Tag>,
    statements: &[LocatedStatement],
) -> Result<(), String> {
    for located in statements {
        let Statement::Set { fields, .. } = &located.statement else {
            continue;
        };
        for tag in fields.tags.iter().flatten() {
            if !tags.contains_key(tag) {
                return Err(format!(
                    "{}:{}: unknown-tag: tag {tag:?} is not in tag-vocabulary.toml",
                    located.path.display(),
                    located.line
                ));
            }
        }
    }
    Ok(())
}

pub fn final_members(
    registers: &[reg_resolve::Resolved],
    statements: &[LocatedStatement],
) -> Result<(), String> {
    for register in registers {
        let mut sets = BTreeMap::<&str, BTreeSet<usize>>::new();
        for (index, variable) in register.variables.iter().enumerate() {
            for set in &variable.sets {
                sets.entry(set).or_default().insert(index);
            }
        }
        for (name, members) in sets {
            if members.len() < 2 {
                let statement = statements.iter().find(|s| {
                    s.provider == register.register.provider && s.register == register.register.register
                        && matches!(&s.statement, Statement::Set { fields, .. } if fields.sets.as_ref().is_some_and(|sets| sets.iter().any(|s| s == name)))
                }).ok_or_else(|| format!("{}: set {name:?} has no curation location", MembershipCode::TooSmall.as_str()))?;
                let pid = format!(
                    "set/{}/{}/{name}",
                    register.register.provider, register.public
                );
                return Err(format!(
                    "{}:{}: {}: set {pid:?} needs at least 2 distinct final members",
                    statement.path.display(),
                    statement.line,
                    MembershipCode::TooSmall.as_str()
                ));
            }
        }
    }
    Ok(())
}

/// Check resolved targets before setters can overwrite each other's memberships.
pub fn memberships<'a>(
    provider: &str,
    register: &str,
    variables: &BTreeMap<reg_resolve::OccurrenceKey, String>,
    metadata: impl Iterator<
        Item = (
            &'a LocatedStatement,
            &'a reg_resolve::curation::apply::Metadata,
        ),
    >,
) -> Result<(), String> {
    let mut memberships = BTreeMap::<&str, BTreeMap<&str, BTreeSet<String>>>::new();
    for (statement, metadata) in metadata {
        let reg_resolve::curation::apply::MetadataValue::Sets(sets) = &metadata.value else {
            continue;
        };
        let location = format!("{}:{}", statement.path.display(), statement.line);
        let variable = variables
            .get(&metadata.key)
            .ok_or_else(|| format!("{location}: set membership has no resolved variable"))?;
        for set in sets {
            memberships
                .entry(variable)
                .or_default()
                .entry(set)
                .or_default()
                .insert(location.clone());
        }
    }
    let mut refusals = Vec::new();
    for (variable, groups) in memberships {
        if groups.len() > 1 {
            let location = groups
                .values()
                .flatten()
                .min()
                .cloned()
                .expect("group origin");
            let groups = groups
                .into_iter()
                .map(|(group, locations)| {
                    format!(
                        "set/{provider}/{register}/{group} at {}",
                        locations.into_iter().collect::<Vec<_>>().join(", ")
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            refusals.push(format!(
                "{location}: {}: variable {provider}/{register}/{variable} belongs to multiple groups: {groups}",
                MembershipCode::MultipleGroups.as_str()
            ));
        }
    }
    if refusals.is_empty() {
        Ok(())
    } else {
        Err(refusals.join("\n"))
    }
}
