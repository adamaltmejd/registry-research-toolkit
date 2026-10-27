//! Curation statements: typed content only, without matching, precedence or locks.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::address::{Address, PeriodSet};

/// One statement. Its identity is derived from content, never supplied by a curator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Statement {
    Bind {
        source: Option<String>,
        subject: Option<Address>,
        target: BindTarget,
        period: Option<PeriodSet>,
        priority: Option<i64>,
        evidence: String,
        caveat: Option<String>,
    },
    Assert {
        subject: Address,
        fact: AssertFact,
        evidence: String,
        caveat: Option<String>,
    },
    Assign {
        subject: Address,
        variable: String,
        evidence: String,
        caveat: Option<String>,
    },
    Set {
        subject: Address,
        #[serde(default)]
        fields: SetFields,
        #[serde(default)]
        reject: bool,
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
    Ack {
        subject: Address,
        finding: String,
        severity: Severity,
        evidence: Option<String>,
        caveat: Option<String>,
    },
}

/// A bind names a catalog register or table, rather than another source coordinate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "entity", rename_all = "kebab-case", deny_unknown_fields)]
pub enum BindTarget {
    Register { id: String },
    Table { id: String },
}

/// A curator may state facts without introducing opaque provider coordinates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "entity", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AssertFact {
    Occurrence {
        fields: SetFields,
    },
    Release {
        name: String,
        period: PeriodSet,
    },
    Table {
        fields: SetFields,
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
    Equivalent,
    LabelOf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Notice,
    Review,
}

/// The one closed schema of fields curation can override. Names describe catalog
/// facts; source handles and keys deliberately have no slot here.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetFields {
    pub name: Option<String>,
    pub label: Option<String>,
    pub definition: Option<String>,
    pub unit: Option<String>,
    pub description: Option<String>,
    pub synonyms: Option<Vec<String>>,
    #[serde(rename = "type")]
    pub dtype: Option<String>,
    pub length: Option<u64>,
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
    pub temporality: Option<String>,
    pub unit_type: Option<String>,
    pub sets: Option<Vec<String>>,
    pub tags: Option<Vec<String>>,
}

impl Statement {
    /// Validate content that serde's closed schemas cannot express.
    ///
    /// # Errors
    /// Missing evidence, an empty operation or an invalid catalog reference.
    pub fn validate(&self) -> Result<(), String> {
        let evidence = match self {
            Self::Bind { evidence, .. }
            | Self::Assert { evidence, .. }
            | Self::Assign { evidence, .. }
            | Self::Set { evidence, .. }
            | Self::Link { evidence, .. } => Some(evidence),
            Self::Ack {
                evidence, severity, ..
            } => {
                if *severity != Severity::Notice && evidence.is_none() {
                    return Err("evidence is required except for an ack of a notice".into());
                }
                evidence.as_ref()
            }
        };
        if evidence.is_some_and(|value| value.trim().is_empty()) {
            return Err("evidence must not be blank".into());
        }
        match self {
            Self::Bind {
                source,
                subject,
                target,
                ..
            } => {
                if source.is_some() == subject.is_some() {
                    return Err("bind requires exactly one of source or subject".into());
                }
                if source.as_ref().is_some_and(|s| s.trim().is_empty()) {
                    return Err("bind source must not be blank".into());
                }
                let (BindTarget::Register { id } | BindTarget::Table { id }) = target;
                slug(id)?;
            }
            Self::Assign { variable, .. } => {
                if variable != "none" {
                    slug(variable)?;
                }
            }
            Self::Set { fields, reject, .. } => {
                if !reject {
                    fields.validate()?;
                }
                fields.validate_references()?;
            }
            Self::Assert { fact, .. } => match fact {
                AssertFact::Occurrence { fields } | AssertFact::Table { fields } => {
                    fields.validate()?;
                    fields.validate_references()?;
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
                if let Err(error) = object.parse::<Address>()
                    && object.parse::<reg_core::Fqid>().is_err()
                {
                    return Err(format!("invalid link object: {error}"));
                }
            }
            Self::Ack { finding, .. } if finding.trim().is_empty() => {
                return Err("ack finding must not be blank".into());
            }
            Self::Ack { .. } => {}
        }
        Ok(())
    }

    /// SHA-256 of normalized typed content, independent of TOML order and layout.
    ///
    /// # Panics
    /// Only if the typed model gains content that JSON cannot serialize.
    #[must_use]
    pub fn identity(&self) -> String {
        let mut normalized = self.clone();
        match &mut normalized {
            Self::Set { fields, .. } => fields.normalize(),
            Self::Assert { fact, .. } => match fact {
                AssertFact::Occurrence { fields } | AssertFact::Table { fields } => {
                    fields.normalize();
                }
                AssertFact::ValueSet { items }
                | AssertFact::ClassificationVersion { items, .. } => {
                    items.sort();
                }
                AssertFact::Release { .. } => {}
            },
            Self::Link { object, .. } => {
                if let Ok(address) = object.parse::<Address>() {
                    *object = address.to_string();
                }
            }
            _ => {}
        }
        crate::hex(&Sha256::digest(crate::canonical_line(&normalized)))
    }
}

impl SetFields {
    fn validate_references(&self) -> Result<(), String> {
        if let Some(reference) = &self.classification {
            match reference.parse::<reg_core::Fqid>() {
                Ok(reg_core::Fqid::Classification { .. } | reg_core::Fqid::ClassVersion { .. }) => {
                }
                _ => {
                    return Err(
                        "classification must be a class/<family>[/<version>] reference".into(),
                    );
                }
            }
        }
        if let Some(hash) = &self.value_set
            && (hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
        {
            return Err("value_set must be a lowercase SHA-256 content hash".into());
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), String> {
        let value = serde_json::to_value(self).expect("field values serialize without floats");
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

    fn normalize(&mut self) {
        for values in [
            &mut self.synonyms,
            &mut self.sentinels,
            &mut self.sets,
            &mut self.tags,
        ]
        .into_iter()
        .flatten()
        {
            values.sort();
            values.dedup();
        }
    }
}

fn slug(value: &str) -> Result<(), String> {
    if reg_core::is_slug(value) {
        Ok(())
    } else {
        Err(format!("{value:?} is not a catalog slug"))
    }
}
