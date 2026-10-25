//! The neutral common form every adapter translates into (`reg_meta_build/DESIGN.md`,
//! "Target design: the common form"). Rules and curation see only these types.
//!
//! Nothing here names a provider's own coordinate. SCB's `VarID`, `RegVarID` and `RegVerID`,
//! `SoS`'s Deldatamängd token and a thin provider's authored key reach the model only as
//! keys normalize derives from them, as an opaque [`Claim`], or inside [`Evidence`].
//! There is no `NativeCoordinates` equivalent.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::Evidence;

macro_rules! key {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub String);

        impl $name {
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

key!(
    /// The agency: `scb`, `sos`, `fk`, ... It is data in FQID assembly, never a branch.
    ProviderKey
);
key!(
    /// A register within its provider: the FQID's register slug.
    RegisterKey
);
key!(
    /// A table within its register: the variant slug (SCB registervariant, `SoS`
    /// Deldatamängd, a thin provider's authored variant).
    TableKey
);
key!(
    /// A delivery within its table: SCB's edition name (the edition parent's
    /// Registerversionnamn, whitespace-folded). The `RegVerID` stays in evidence.
    DeliveryKey
);
key!(
    /// A delivered column's physical name, exactly as delivered.
    ColumnName
);
key!(
    /// A variable's builder-internal pin key: fixed at first pin, never re-derived from
    /// links (D3). A fold keeps the owner's existing key (D2).
    VariableKey
);
key!(
    /// A legacy provenance head (`errata:<class>`, `curated:<table>`,
    /// `curated:alias-window`), written verbatim until the post-cutover rename.
    ProvenanceHead
);

/// A one-valued stated fact, as `FieldScalar`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(untagged)]
pub enum Scalar {
    Bool(bool),
    Int(i64),
    Str(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldStatus {
    /// The source states a value.
    Value,
    /// The source has the field but its value cannot be read.
    Unknown,
    /// The source states the absence (only `column_name`: an undelivered column).
    Negative,
}

/// One stated field: the normalized value and the raw value as delivered.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatedField {
    pub status: FieldStatus,
    #[serde(default)]
    pub value: Option<Scalar>,
    #[serde(default)]
    pub raw_value: Option<Scalar>,
}

/// The fields a source states for one column, keyed by a
/// [`crate::prepared::FIELD_NAMES`] name. An absent name is unstated.
pub type StatedFields = BTreeMap<String, StatedField>;

/// A closed or open ISO date interval; a `None` end is open-ended, never unknown.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Interval {
    pub start: String,
    pub end: Option<String>,
}

/// The one coverage slot. Each adapter's period grammar is parsed before this:
/// normalize fills it from SCB's delivery period, `SoS`'s Data från/till
/// (`edition_scope`) or a thin provider's `edition_period_scope`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Coverage {
    NotApplicable,
    /// The source states a period that no grammar reads; the label is kept.
    Unknown {
        label: String,
    },
    YearIndependent,
    /// Ordered, disjoint intervals.
    Intervals(Vec<Interval>),
    /// A pooled period: resolvable only with its whole-range bounds.
    Pooled {
        label: String,
        range: Option<Interval>,
    },
}

/// A delivery's release status. Today only SCB's edition names state it (phase-1 b2
/// moves the parse into the SCB adapter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReleaseStatus {
    Preliminary,
    Final,
    Unstated,
}

/// A table named by its register and its own key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableRef {
    pub register: RegisterKey,
    pub table: TableKey,
}

/// Where a fact comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Provenance {
    /// A source states it, in these records.
    Source { evidence: Vec<Evidence> },
    /// A neutral rule derived it (the default link, supersession by release status).
    Rule { rule: &'static str },
    /// A curation entry states it. `head` is the entry's legacy provenance head.
    Curated {
        head: ProvenanceHead,
        entry: EntryRef,
    },
}

/// A curation entry: its file relative to the curation root, its verb table and its
/// position among that file's entries of the verb.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryRef {
    pub file: String,
    pub verb: String,
    pub index: u32,
}

/// A register: name and description as its provider states them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Register {
    pub key: RegisterKey,
    pub provider: ProviderKey,
    pub name: String,
    pub description: Option<String>,
    pub provenance: Provenance,
}

/// A table: one delivered dataset series of a register.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub key: TableRef,
    pub name: Option<String>,
    /// What one row describes (person, firm, ...).
    pub unit_type: Option<String>,
    /// Which units are included (16+ vs 15+); distinct from the unit type.
    pub population: Option<String>,
    /// The columns that key a row: entity, then time.
    pub row_keys: Vec<ColumnName>,
    /// Other tables this one refers to (today `variant_references`).
    pub references: Vec<TableRef>,
    pub provenance: Provenance,
}

/// A delivery: one release of a table. Only sources that document deliveries have
/// them; a table without deliveries carries coverage on its columns instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub table: TableRef,
    pub key: DeliveryKey,
    /// The day-precision period parsed from the delivery's name.
    pub period: Coverage,
    /// Its year projection. Today's guards and errata read this form on its own
    /// (`scope_payload` for SCB), so both are kept.
    pub period_years: Coverage,
    pub release_status: ReleaseStatus,
    pub version_label: Option<String>,
    /// The populations and object types the source states for this delivery. SCB
    /// scopes them to the edition, not the table.
    pub populations: Vec<String>,
    pub object_types: Vec<String>,
    pub provenance: Provenance,
}

/// One delivered column: a register × table × (delivery or coverage) × literal under one
/// link target. Several records can state it (SCB: one per population and object type
/// of the delivery), and they may disagree on a field; each claim is kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub table: TableRef,
    /// `None` where the source documents no deliveries.
    pub delivery: Option<DeliveryKey>,
    pub coverage: Coverage,
    pub name: ColumnName,
    /// Each stating record's fields, in record order.
    pub claims: Vec<(StatedFields, Evidence)>,
    /// The token that joins this column to its value associations (SCB's `CVID`, a
    /// workbook value-list pointer). It never addresses curation.
    pub value_join: Option<String>,
    pub link: Link,
}

/// A source's own identity statement for a column, such as SCB's `VarID`. Opaque: it
/// says which columns the source joins, never what the variable is called or keyed by.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Claim(String);

impl Claim {
    /// Normalize mints claims; nothing after it can.
    #[expect(dead_code, reason = "normalize mints claims (phase 3, slice 2)")]
    pub(crate) fn new(token: String) -> Self {
        Self(token)
    }
}

/// What a column → variable link points at before formation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LinkTarget {
    /// The columns a source claim joins, within the register.
    Stated { register: RegisterKey, claim: Claim },
    /// The default rule: the same column literal within the register, compared exactly
    /// after `normalize_token`, with no case fold.
    Default {
        register: RegisterKey,
        column: ColumnName,
    },
    /// A curated owner (`identity.assign`).
    Variable(VariableKey),
    /// Curated `owner = false`: the column belongs to no variable.
    Unassigned,
}

/// The column → variable link, with its source. Formation groups columns by target:
/// a stated target never merges with a default one, so a stated link wins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub target: LinkTarget,
    pub provenance: Provenance,
}
