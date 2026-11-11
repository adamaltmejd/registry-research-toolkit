//! Curation statements: typed content only, without matching, precedence or locks.

use serde::{Deserialize, Serialize};

use crate::address::{Address, PeriodSet};

/// One statement. Its identity is derived from content, never supplied by a curator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Statement {
    Assert {
        subject: Address,
        fact: AssertFact,
        evidence: String,
        caveat: Option<String>,
    },
    Set {
        subject: Address,
        #[serde(default)]
        fields: Box<SetFields>,
        evidence: String,
        caveat: Option<String>,
    },
    Link {
        subject: Address,
        relation: Relation,
        object: String,
        at: Option<PeriodSet>,
        evidence: String,
        caveat: Option<String>,
    },
}

/// A curator may state facts without introducing opaque provider coordinates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "entity", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AssertFact {
    Occurrence {
        fields: Box<AssertFields>,
    },
    Release {
        name: String,
        period: PeriodSet,
    },
    Table {
        fields: Box<AssertFields>,
    },
    ValueSet {
        items: Vec<crate::Item>,
    },
    ClassificationVersion {
        family: String,
        version: String,
        items: Vec<crate::Item>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Relation {
    Continues,
    DerivedFrom,
    LabelOf,
}

/// The closed schema of fields curation can override. Names describe catalog
/// facts; source handles and keys deliberately have no slot here.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetFields {
    pub table: Option<String>,
    pub variable: Option<String>,
    pub label: Option<String>,
    pub definition: Option<String>,
    pub operational_definition: Option<String>,
    pub unit: Option<String>,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub dtype: Option<String>,
    pub format: Option<String>,
    pub spelling: Option<String>,
    pub period: Option<PeriodSet>,
    pub value_set: Option<String>,
    pub coded: Option<bool>,
    pub sentinels: Option<Vec<String>>,
    pub classification: Option<String>,
    pub coverage: Option<PeriodSet>,
    pub notice: Option<String>,
    pub sensitive: Option<bool>,
    pub identifier: Option<bool>,
    pub role: Option<String>,
    pub sets: Option<Vec<String>>,
    pub tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub axes: Option<Vec<SetAxis>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<std::collections::BTreeMap<String, MemberFacet>>,
}

/// Ordered presentation dimensions declared by a register-local set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetAxis {
    pub id: String,
    pub label: String,
}

/// One coordinate in a set's declared axis. The value is the lexical sort key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberFacet {
    pub value: String,
    pub label: Option<String>,
}

/// Source-shaped facts for assertions, separate from field overrides.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssertFields {
    pub name: Option<String>,
    pub description: Option<String>,
    pub coverage: Option<PeriodSet>,
    pub label: Option<String>,
    pub definition: Option<String>,
    pub unit: Option<String>,
    #[serde(rename = "type")]
    pub dtype: Option<String>,
    pub length: Option<u64>,
    pub sensitive: Option<bool>,
    pub identifier: Option<bool>,
}

impl Statement {
    /// Validate content that serde's closed schemas cannot express.
    ///
    /// # Errors
    /// Missing evidence, an empty operation or an invalid catalog reference.
    pub fn validate(&self) -> Result<(), String> {
        let subject = match self {
            Self::Assert { subject, .. }
            | Self::Set { subject, .. }
            | Self::Link { subject, .. } => subject,
        };
        if matches!(subject, Address::Set(_)) && !matches!(self, Self::Set { .. }) {
            return Err("set-subject: set: subjects support only set statements".into());
        }
        let (Self::Assert { evidence, .. }
        | Self::Set { evidence, .. }
        | Self::Link { evidence, .. }) = self;
        if evidence.trim().is_empty() {
            return Err("evidence must not be blank".into());
        }
        match self {
            Self::Set {
                subject, fields, ..
            } => {
                validate_fields(fields)?;
                published_type(fields.dtype.as_deref())?;
                fields.validate_references()?;
                fields.validate_subject(subject)?;
            }
            Self::Assert { fact, .. } => match fact {
                AssertFact::Occurrence { fields } | AssertFact::Table { fields } => {
                    validate_fields(fields)?;
                    if matches!(fact, AssertFact::Occurrence { .. }) && fields.description.is_some()
                    {
                        return Err("description-subject: an occurrence cannot assert a description; use set description on var:<id>".into());
                    }
                }
                AssertFact::Release { name, .. } if name.trim().is_empty() => {
                    return Err("asserted release name must not be blank".into());
                }
                AssertFact::ValueSet { items }
                | AssertFact::ClassificationVersion { items, .. } => {
                    let mut codes = std::collections::BTreeSet::new();
                    if items.iter().any(|item| !codes.insert(&item.code)) {
                        return Err("asserted value set contains a duplicate code".into());
                    }
                    if let AssertFact::ClassificationVersion {
                        family, version, ..
                    } = fact
                    {
                        format!("class/{family}/{version}")
                            .parse::<reg_core::Fqid>()
                            .map_err(|e| e.to_string())?;
                    }
                }
                AssertFact::Release { .. } => {}
            },
            Self::Link { object, .. } => {
                if matches!(object.parse::<Address>(), Ok(Address::Set(_)))
                    || matches!(
                        object.parse::<reg_core::Fqid>(),
                        Ok(reg_core::Fqid::Set { .. })
                    )
                {
                    return Err(
                        "set-subject: sets are presentation only and cannot be relation endpoints"
                            .into(),
                    );
                }
                if let Err(error) = object.parse::<Address>()
                    && object.parse::<reg_core::Fqid>().is_err()
                {
                    return Err(format!("invalid link object: {error}"));
                }
            }
        }
        Ok(())
    }

    /// Publication and synthetic source identities include the owning register.
    /// Local premise locks still use [`Self::identity`] within their register file.
    #[must_use]
    pub fn scoped_identity(&self, provider: &str, register: &str) -> String {
        reg_core::sha256_hex(crate::canonical_line(&(
            provider,
            register,
            self.identity(),
        )))
    }

    /// SHA-256 of normalized typed content, independent of TOML order and layout.
    /// `evidence` is left out: it is commentary on the statement, not a premise, so
    /// editing it never moves a lock key, an asserted occurrence's `stmt:` source key or
    /// an id anchor (maintainer ruling, 2026-11-04).
    ///
    /// # Panics
    /// Only if the typed model gains content that JSON cannot serialize.
    #[must_use]
    pub fn identity(&self) -> String {
        let mut normalized = self.clone();
        match &mut normalized {
            Self::Set { fields, .. } => fields.normalize(),
            Self::Assert { fact, .. } => match fact {
                AssertFact::ValueSet { items }
                | AssertFact::ClassificationVersion { items, .. } => {
                    items.sort();
                }
                AssertFact::Occurrence { .. }
                | AssertFact::Table { .. }
                | AssertFact::Release { .. } => {}
            },
            Self::Link { object, .. } => {
                if let Ok(address) = object.parse::<Address>() {
                    *object = address.to_string();
                }
            }
        }
        let mut tree = serde_json::to_value(&normalized).expect("statements serialize");
        tree.as_object_mut()
            .expect("a statement serializes as an object")
            .remove("evidence");
        reg_core::sha256_hex(crate::canonical_line(&tree))
    }
}

impl SetFields {
    fn validate_subject(&self, subject: &Address) -> Result<(), String> {
        if matches!(subject, Address::Set(_)) {
            let stated = serde_json::to_value(self).map_err(|e| e.to_string())?;
            if stated
                .as_object()
                .into_iter()
                .flatten()
                .any(|(key, value)| {
                    !value.is_null() && !matches!(key.as_str(), "label" | "description" | "axes")
                })
            {
                return Err(
                    "set-fields: a set: subject accepts only label, description and axes".into(),
                );
            }
            // Label, description and axes may each come from their own statement, with
            // its own evidence; a set without a curated label is named by its slug.
            if self.label.as_ref().is_some_and(|v| v.trim().is_empty()) {
                return Err("set-label: a set label must not be blank".into());
            }
        } else if self.axes.is_some() {
            return Err("set-axes-subject: axes require a set: subject".into());
        }
        if self.facets.is_some() && !matches!(subject, Address::Variable(_)) {
            return Err("set-facet-subject: facets require a var: subject".into());
        }
        if self.description.is_some() && matches!(subject, Address::Occurrence { .. }) {
            return Err("description-subject: set description requires a var:, set:, table: or release: subject; occurrence descriptions are not supported".into());
        }
        if matches!(subject, Address::Variable(_) | Address::Set(_))
            && self
                .description
                .as_ref()
                .is_some_and(|text| text.trim().is_empty())
        {
            return Err(
                "description-empty: a variable or set description must not be blank".into(),
            );
        }
        Ok(())
    }

    fn validate_references(&self) -> Result<(), String> {
        if let Some(table) = &self.table {
            slug(table)?;
        }
        if let Some(variable) = &self.variable
            && variable != "none"
        {
            slug(variable)?;
        }
        for value in self.sets.iter().chain(&self.tags).flatten() {
            slug(value)?;
        }
        if let Some(reference) = &self.classification {
            match reference.parse::<reg_core::Fqid>() {
                Ok(reg_core::Fqid::ClassVersion { .. }) => {}
                _ => {
                    return Err(
                        "classification must be a class/<family>/<version> reference".into(),
                    );
                }
            }
        }
        let mut axes = std::collections::BTreeSet::new();
        for axis in self.axes.iter().flatten() {
            slug(&axis.id)?;
            if axis.label.trim().is_empty() || !axes.insert(&axis.id) {
                return Err("set-axis-invalid: axes require unique ids and nonblank labels".into());
            }
        }
        for (axis, facet) in self.facets.iter().flatten() {
            slug(axis)?;
            if facet.value.trim().is_empty()
                || facet.label.as_ref().is_some_and(|v| v.trim().is_empty())
            {
                return Err("set-facet-invalid: facet values and labels must not be blank".into());
            }
        }
        value_set_reference(self.value_set.as_deref())
    }

    fn normalize(&mut self) {
        for values in [&mut self.sentinels, &mut self.sets, &mut self.tags]
            .into_iter()
            .flatten()
        {
            values.sort();
            values.dedup();
        }
    }
}

/// The frame fields curation names differently, as (curation, frame); every other
/// field has one name in both.
pub const RENAMED_FIELDS: [(&str, &str); 3] = [
    ("type", "dtype"),
    ("spelling", "column"),
    ("value_set", "valueset"),
];

/// The frame field a curation field name sets.
#[must_use]
pub fn frame_field(name: &str) -> &str {
    RENAMED_FIELDS
        .iter()
        .find(|(curation, _)| *curation == name)
        .map_or(name, |(_, frame)| frame)
}

/// The curation name of a frame field.
#[must_use]
pub fn curation_field(name: &str) -> &str {
    RENAMED_FIELDS
        .iter()
        .find(|(_, frame)| *frame == name)
        .map_or(name, |(curation, _)| curation)
}

fn validate_fields(fields: &impl Serialize) -> Result<(), String> {
    let value = serde_json::to_value(fields).expect("field values serialize without floats");
    if value
        .as_object()
        .expect("fields are an object")
        .values()
        .all(serde_json::Value::is_null)
    {
        return Err("fields must state at least one field".into());
    }
    Ok(())
}

/// A `set` states a type under its published name only (`integer`, `text`, ...), so an
/// override never reads as a reader alias such as `char`, which the integer-code rule
/// rewrites (#2071). An `assert` transcribes a delivery in reader vocabulary, so the rule
/// reads its `char` like a source record's.
fn published_type(dtype: Option<&str>) -> Result<(), String> {
    match dtype {
        Some(t) if crate::dtype_info(t).and_then(|d| d.published) != Some(t) => Err(format!(
            "type-name: type {t:?} is not a published type name (integer, decimal, text, date, datetime, boolean, geometry, uuid, binary, identifier)"
        )),
        _ => Ok(()),
    }
}

fn value_set_reference(hash: Option<&str>) -> Result<(), String> {
    if let Some(hash) = hash
        && !reg_core::is_sha256_hex(hash)
    {
        return Err("value_set must be a lowercase SHA-256 content hash".into());
    }
    Ok(())
}

fn slug(value: &str) -> Result<(), String> {
    if reg_core::is_slug(value) {
        Ok(())
    } else {
        Err(format!("{value:?} is not a catalog slug"))
    }
}
