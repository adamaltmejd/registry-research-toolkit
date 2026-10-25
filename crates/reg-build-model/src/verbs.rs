//! The neutral curation verbs as serde types: the register curation file's verb tables
//! after the port (`reg_meta_build/DESIGN.md`, "The neutral verb set"; the schema and the
//! per-verb guard table are in `crates/reg-build-model/DESIGN.md`, "Curation verbs").
//!
//! An entry addresses records only through [`Member`]: a table slug, a column literal, a
//! delivery name or a period, and stated field values. No address is a provider integer.
//! Native ids survive only inside guards, as what the source stated ([`Guard`]).
//!
//! Every entry is converted one-to-one from one entry of today's tree and carries that
//! entry's [`Legacy`] case id, which the build writes into the artifact verbatim. Naming
//! pins (`[register]`, `[[variant]]`, `[variable]`, `*.auto.toml`) and the verbs the port
//! does not change (enrichment, groups, code/label pairs, acknowledgements,
//! classification timelines, documentary bindings) are not in this module.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer};

use crate::model::Scalar;
use crate::prepared::Scope;

/// A verbatim nested projection: what the source stated at an occurrence
/// (`RecordExpectation`, `PreparedCodingAuthority`, documented members), carried unchanged
/// from today's tree. Phase 3 types each one when a slice first evaluates it.
pub type Projection = toml::Table;

/// The register curation file's verb tables.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Verbs {
    #[serde(default)]
    pub identity: Identity,
    #[serde(default)]
    pub table: TableVerbs,
    #[serde(default)]
    pub errata: Errata,
    #[serde(default)]
    pub representation: Representation,
    #[serde(default)]
    pub coding: Coding,
}

/// The entry of today's tree this entry was converted from.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Legacy {
    /// The old case id root, `curation/registers/<provider>/<file>.toml#/<table>/<n>`.
    /// The build writes it (with any `/period/<k>` suffix) where Python writes the case
    /// id today.
    #[serde(rename = "ref")]
    pub case_id: String,
    /// The provenance head the old entry's facts publish: `errata:<class>`,
    /// `curated:<table>` or `curated:alias-window`.
    pub head: String,
}

/// The member selector `M`: the records an entry acts on. Every given coordinate must
/// match; an absent one does not restrict. It is literal-scoped, not link-scoped: the
/// converter writes the loosest selector that selects exactly the old entry's records in
/// the pinned prepared root.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    /// A table (variant) slug of this register, after `table.assign`.
    pub table: Option<String>,
    /// A delivered column literal, exactly.
    pub column: Option<String>,
    /// Delivery names (whitespace-folded edition names), only where the source has
    /// deliveries. Excludes `period`.
    pub deliveries: Option<Vec<String>>,
    /// The occurrence's coverage, where the source has no deliveries. Excludes
    /// `deliveries`.
    pub period: Option<Scope>,
    /// Stated field values the record must carry: the field's normalized `value` with
    /// status `value`.
    #[serde(rename = "where", default)]
    pub where_fields: BTreeMap<String, Scalar>,
}

/// `FieldExpectation`: one stated field as the guard pins it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldExpectation {
    pub name: String,
    pub status: ExpectedStatus,
    pub value: Option<Scalar>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedStatus {
    Absent,
    Value,
    Unknown,
    Negative,
}

/// Every pin today's entries carry over what the source states, carried verbatim; the
/// per-verb table in `DESIGN.md` names the ones each verb uses. A guard that no longer
/// holds makes its entry stale, never silently inert.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Guard {
    /// `evidence_sha256` digests of the selected records (the multiset form for a family).
    pub expected_evidence_sha256: Option<String>,
    /// `field_projection_sha256` of each selected record's complete field projection.
    pub expected_fields_sha256: Option<String>,
    pub expected_fields: Option<Vec<FieldExpectation>>,
    /// Complete original projections (`RecordExpectation`), native ids included.
    pub expected_records: Option<Vec<Projection>>,
    pub authority_records: Option<Vec<Projection>>,
    /// The year projection and the day-precision period of the occurrence.
    pub expected_scope: Option<Scope>,
    pub expected_period: Option<Scope>,
    /// The source's period text per delivery name.
    pub expected_period_text: Option<BTreeMap<String, String>>,
    /// The full delivery list of the source table (`edition_split`'s complement guard).
    pub expected_deliveries: Option<Vec<String>>,
    /// The stated data type, representation, reference and description of the original
    /// (`errata.data_type`, `errata.classification_reference`).
    pub expected_type: Option<String>,
    pub expected_representation: Option<String>,
    pub expected_reference: Option<String>,
    pub expected_description: Option<String>,
    /// Month definitions per member (`representation.period_family`).
    pub expected_definitions: Option<BTreeMap<String, String>>,
    /// The CIS matrix selector's provider ids, as the source states them.
    pub expected_selector: Option<Projection>,
}

/// A link's target: a variable of this register by slug, or `false`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    Variable(String),
    /// `owner = false`: the members belong to no variable. It refuses any other entry
    /// that selects the same records (the competition rule).
    Unassigned,
}

impl<'de> Deserialize<'de> for Owner {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Slug(String),
            Flag(bool),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Slug(slug) => Ok(Self::Variable(slug)),
            Raw::Flag(false) => Ok(Self::Unassigned),
            Raw::Flag(true) => Err(serde::de::Error::custom(
                "owner is a variable slug or false",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    #[serde(default)]
    pub assign: Vec<IdentityAssign>,
}

/// `identity.assign`: these member columns link to an owner, or to none. One entry holds
/// every link of the old entry, because its guards span all of them (a partition's family
/// digest covers every owner and every unassigned literal).
///
/// A source link that any assign entry touches must be fully reassigned: every column the
/// link joins is selected by some assign entry, as a partition's exact literal coverage
/// requires today.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityAssign {
    pub legacy: Legacy,
    pub link: Vec<AssignLink>,
    #[serde(default)]
    pub guard: Guard,
    /// The source reference the decision cites (`columns_ref`, `ref`).
    #[serde(rename = "ref")]
    pub reference: Option<String>,
    pub data_warning: Option<String>,
    pub evidence: Option<String>,
    pub noted: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignLink {
    pub owner: Owner,
    pub members: Vec<Member>,
    /// A member effect: the selected records' delivered column is this literal
    /// (`identity.rename`). It applies to the member itself, never on top of another
    /// correction.
    pub column_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableVerbs {
    #[serde(default)]
    pub assign: Vec<TableAssign>,
}

/// `table.assign`: these records or deliveries belong to that table. A split table takes
/// the named deliveries of its source table; an ambiguous source token routes to the
/// listed tables.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableAssign {
    pub legacy: Legacy,
    /// The target table slugs: one for a split, every candidate for a routed token.
    pub tables: Vec<String>,
    #[serde(default)]
    pub members: Vec<Member>,
    /// The source's own table token, for a route.
    pub source_token: Option<String>,
    #[serde(default)]
    pub guard: Guard,
    pub evidence: Option<String>,
    pub noted: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Errata {
    #[serde(default)]
    pub table: Vec<ErrataTable>,
    #[serde(default)]
    pub field: Vec<ErrataField>,
    #[serde(default)]
    pub period: Vec<ErrataPeriod>,
    #[serde(default)]
    pub delivered: Vec<ErrataDelivered>,
    #[serde(default)]
    pub delivery: Vec<ErrataDelivery>,
    #[serde(default)]
    pub column: Vec<ErrataColumn>,
    #[serde(default)]
    pub support: Vec<ErrataSupport>,
}

/// `errata.table`: a table exists that the source does not list.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrataTable {
    pub table: String,
    pub name: String,
}

/// `errata.field`: stated fields of one occurrence are wrong; set or clear them.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrataField {
    pub legacy: Legacy,
    pub member: Member,
    #[serde(default)]
    pub set: BTreeMap<String, Scalar>,
    #[serde(default)]
    pub clear: Vec<String>,
    #[serde(default)]
    pub guard: Guard,
    pub evidence: String,
    pub noted: String,
}

/// `errata.period`: the source's coverage for this occurrence was read wrongly.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrataPeriod {
    pub legacy: Legacy,
    pub member: Member,
    /// The corrected year projection and day-precision period.
    pub scope: Scope,
    pub period: Scope,
    #[serde(default)]
    pub authority: Vec<Projection>,
    #[serde(default)]
    pub guard: Guard,
    pub evidence: String,
    pub noted: String,
}

/// `errata.delivered`: the same column with the same meaning was also delivered here.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrataDelivered {
    pub legacy: Legacy,
    /// Where it was also delivered: a table and column with the added deliveries or
    /// period.
    pub member: Member,
    /// The documented occurrences it carries, where they are not the member's own table
    /// and column.
    pub donor: Option<Member>,
    #[serde(default)]
    pub donor_coding: bool,
    pub data_type: Option<String>,
    pub measurement_unit: Option<String>,
    pub data_warning: Option<String>,
    pub upstream: Option<String>,
    #[serde(default)]
    pub guard: Guard,
    pub evidence: String,
    pub noted: String,
}

/// `errata.delivery`: the table delivered a delivery the source does not document.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrataDelivery {
    pub legacy: Legacy,
    pub table: String,
    pub delivery: String,
    pub evidence: String,
    pub noted: String,
}

/// `errata.column`: an undocumented column was delivered; mint a variable.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrataColumn {
    pub legacy: Legacy,
    /// Table and column, with the deliveries or the period it was delivered in.
    pub member: Member,
    /// Every delivery of the table (`all_versions`).
    #[serde(default)]
    pub all_deliveries: bool,
    pub name: String,
    pub definition: String,
    pub data_type: Option<String>,
    pub measurement_unit: Option<String>,
    pub operational_definition: Option<String>,
    pub classification: Option<String>,
    pub is_identifier: Option<bool>,
    pub is_sensitive: Option<bool>,
    pub data_warning: Option<String>,
    pub source: String,
    pub evidence: String,
    pub noted: String,
}

/// `errata.support`: this occurrence's assertion is a documented error; keep it as
/// support.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrataSupport {
    pub legacy: Legacy,
    pub member: Member,
    pub kind: String,
    #[serde(default)]
    pub guard: Guard,
    /// The occurrence whose statement is the authority, with its own guards.
    pub authority: Option<SupportAuthority>,
    /// The owner the carried replacement belongs to.
    pub replacement: Option<String>,
    pub evidence: String,
    pub noted: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupportAuthority {
    pub member: Member,
    #[serde(default)]
    pub guard: Guard,
    pub evidence: String,
    pub noted: String,
}

#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Representation {
    #[serde(default)]
    pub parallel: Vec<RepresentationParallel>,
    #[serde(default)]
    pub period_family: Vec<PeriodFamily>,
    #[serde(default)]
    pub delivery_metadata: Vec<DeliveryMetadata>,
    #[serde(default)]
    pub matrix: Vec<Matrix>,
}

/// `representation.parallel`: several columns are forms of one variable over a window.
/// An alias window (`alias_window = true`, one column) says that in these deliveries this
/// column spelling is the owner's representation.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepresentationParallel {
    pub legacy: Legacy,
    pub owner: String,
    pub table: String,
    pub columns: Vec<ParallelColumn>,
    #[serde(default)]
    pub alias_window: bool,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    #[serde(default)]
    pub co_delivered: bool,
    #[serde(default)]
    pub case_aliases: bool,
    pub column_metadata: Option<String>,
    pub coding_metadata: Option<String>,
    pub evidence: String,
    pub noted: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelColumn {
    pub column: String,
    pub deliveries: Vec<String>,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    pub expected_codings: Option<Vec<String>>,
}

/// `representation.period_family`: columns sharing a stem are one variable, one member
/// per month.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeriodFamily {
    pub legacy: Legacy,
    pub family_stem: String,
    pub label: String,
    pub slug: String,
    #[serde(default)]
    pub guard: Guard,
}

/// `representation.delivery_metadata`: literal metadata may vary under one owner.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryMetadata {
    pub legacy: Legacy,
    pub owner: String,
    pub fields: Vec<String>,
    pub columns: Vec<DeliveryMetadataColumn>,
    #[serde(default)]
    pub support: Vec<Projection>,
    #[serde(default)]
    pub guard: Guard,
    pub evidence: String,
    pub noted: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryMetadataColumn {
    pub member: Member,
    pub valid_from: String,
    pub valid_to: String,
    pub expected_codings: Option<Vec<String>>,
}

/// `representation.matrix`: a CIS evidence file answers this occurrence.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Matrix {
    pub legacy: Legacy,
    pub member: Member,
    pub source_mode: String,
    pub evidence_file: String,
    #[serde(default)]
    pub guard: Guard,
}

/// The `coding.*` verbs, re-addressed to [`Member`]. Each keeps its own payload and
/// guards verbatim; `periods` keep their order, because a period's position is part of
/// the case id the artifact stores (`<legacy>/period/<k>`).
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Coding {
    #[serde(default)]
    pub warning: Vec<CodingEntry>,
    #[serde(default)]
    pub choice: Vec<CodingEntry>,
    #[serde(default)]
    pub uncoded: Vec<CodingEntry>,
    #[serde(default)]
    pub omit: Vec<CodingEntry>,
    #[serde(default)]
    pub extend: Vec<CodingEntry>,
    #[serde(default)]
    pub documented: Vec<CodingEntry>,
    #[serde(default)]
    pub sentinel: Vec<CodingEntry>,
    #[serde(default)]
    pub support: Vec<CodingEntry>,
}

/// One coding entry: the member, the finite periods, and the verb's own payload
/// (`keep`, `over`, `list`, `witness`, `members`, `source_authority`, ...) carried
/// verbatim.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodingEntry {
    pub legacy: Legacy,
    pub member: Member,
    /// The owner, where the member alone does not say which variable the column codes.
    pub owner: Option<String>,
    #[serde(default)]
    pub periods: Vec<Vec<String>>,
    pub reason: String,
    pub source: String,
    #[serde(default)]
    pub payload: Projection,
}
