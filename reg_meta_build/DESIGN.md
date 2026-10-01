# Design: reg_meta_build

`reg_meta_build` prepares machine-readable register metadata, applies checked curation,
and builds the SQLite catalogs queried by `reg_meta`. It also builds the separate
`reg_meta_docs.db` document index. It is maintainer tooling, not a runtime dependency of
the query package.

The dependency direction is `reg_meta_build → reg_meta`. The query package owns the
public catalog/schema constants and read models. The builder owns input handling,
curation, materialization and validation. Cross-package constraints live in
[../ARCHITECTURE.md](../ARCHITECTURE.md).

The four-step pipeline below is the only `build-db` implementation. A diagnostic
database is incomplete and cannot be activated by builder publication. Input declaration
loaders and read-only worklists do not constitute an alternative build route.

Where the code has not caught up with this design, the text marks the current state as
*transitional*; [../REFACTOR_SPEC.md](../REFACTOR_SPEC.md) orders the work that removes
it.

## Responsibilities

The design serves five maintainer tasks:

1. Rebuild deterministically from exact accepted inputs and decisions.
2. Prepare and inspect an occasional source update before accepting it.
3. Resolve an exact discrepancy, or explicitly acknowledge a bounded unresolved one.
4. Explain affected identities, periods, coding and dependent catalog content.
5. Repair a source adapter when an actual delivered format changes.

The builder has four steps:

  | Step        | Responsibility                                                                                                              | Boundary                                                                                                                                                                                      |
  | ----------- | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | 1. Condense | Provider adapters decode raw deliveries into one common, compact form with minimal interpretation.                          | No catalog FQIDs, identity merging, source winner, PDF interpretation or LLM calls.                                                                                                           |
  | 2. Rules    | Provider-generic automatic processing of identities, fields, periods, coding, classification binding, groups and relations. | No provider, register, variant or variable ids and no special cases in rule code. A regularity that holds across the corpus is a rule applied on every build, never a stored per-member case. |
  | 3. Curation | Discretionary, tracked decisions for whatever the rules leave unresolved.                                                   | Tracked TOML naming literal source coordinates; no per-register Python and no decision based on another correction's output.                                                                  |
  | 4. Build    | Assign storage IDs, write resolved rows, derive search indexes, validate and publish atomically.                            | Errors on anything unresolved until curation resolves or explicitly acknowledges it. No independent semantic choices or post-write correction passes.                                         |

Cleaning and input storage below describe step 1. Common curation describes steps 2 and
3 together. Strict and diagnostic builds describes step 4.

The maintained call path is `pipeline.build_catalog` →
`prepared_catalog.open_prepared_catalog_sources` → `curation_compile.compile_curation` →
`source_scope.resolve_source_scope` for each complete source/register scope → catalog
dependency and lineage resolution → `resolved_catalog.write_resolved_catalog`.

  | Step | Module family                                                                              | Role                                                                                              |
  | ---- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
  | 1    | `sources/*_records.py`, `sources/*_values.py`, source reference readers                    | Actual-format decoding.                                                                           |
  | 1    | `source_records.py`, `source_values.py`, `source_reference_records.py`, `normalization.py` | Common compact observations, values, literal reference declarations and mechanical normalization. |
  | 1    | `input_snapshot.py`, `prepared_sources.py`, `prepared_values.py`, `prepared_catalog.py`    | Capture, validation, compact storage and pinned prepared-input access.                            |
  | 2    | `source_intervals.py`, `source_coding.py`, `source_formation.py`                           | Exact periods, membership reconciliation and ordinary variable formation.                         |
  | 2    | `catalog_dependencies.py`, `catalog_lineage.py`, `source_event_resolution.py`              | Supported catalog relationships and exact dependent omissions.                                    |
  | 2, 3 | classification binding modules                                                             | The label binding rule and per-classification overrides.                                          |
  | 3    | `source_curation.py`, `source_effects.py`, `source_naming.py`                              | Applicability, original-evidence corrections and checked naming.                                  |
  | 3    | `source_annotations.py`, `source_representations.py`                                       | Checked aliases and parallel columns.                                                             |
  | 3    | `curation_tree.py`, `curation_compile.py`                                                  | Validate tracked entries and compile scoped decisions in memory.                                  |
  | 4    | `resolved_catalog.py`, `resolved_metadata.py`, `db.py`                                     | Direct materialization, SQL schema, indexes and atomic publication.                               |
  | 4    | `validate.py`, `semantic_diff.py`, `dbdiff.py`                                             | Structural/corpus verification and comparison.                                                    |
  | —    | `extend_db.py`, `sources/curated.py`, `ir/`                                                | Separate steward extension over a released global catalog.                                        |
  | —    | `doc_db.py`                                                                                | Document indexing; independent of source fact resolution.                                         |

Transitional: builder code still names specific registers or variants in three places.
They are debt to remove, not precedent:

- `sources/scb_records.py` `_PROJECTION_REGISTERS`, which reads one forecast register's
  edition names as vintages;
- the LISA code-membership anchor in `validate.py`;
- the LISA variant pins in `source_inspection.py`.

## Source observations and authority

An observation retains its logical source and exact revision, original artifact,
semantic key, physical locators and original cells. Source-local coordinates identify
register, variant, edition, variable/question, member and delivery column where
supplied. Sparse typed fields preserve names, definitions, type/length, flags,
availability, classification declarations, source attribution and reference-period text.
Code-list references point to separately stored evidence rather than expanding every
code into every record.

Missing optional metadata, an absent occurrence, a source-defined unknown and an
explicit negative are different. Absence does not establish unavailability. An explicit
negative has meaning only within the format and scope that supplies it. Original
contradictory observations and physical duplicates remain inspectable.

The semantic record identity excludes revision and physical location. Original payloads
remain evidence, so a raw whitespace edit may still change an observation identity.
Curation therefore compares the relevant normalized projections and scoped membership,
not raw record-ID equality. Moving spreadsheet rows or changing unrelated fields does
not invalidate an otherwise identical decision.

Authority depends on the fact and scope, rather than a universal file ordering:

- Machine metadata establishes the native occurrences and fields it contains.
- A LISA workbook establishes what its documented populations, columns and editions
  state. It does not automatically establish historical meaning or coding.
- A canonical classification supplies its own codebook and editions. It does not prove
  that a variable uses that classification.
- Once exact source documentation establishes a named classification version, its code
  membership may be assumed stable across the documented periods unless other supplied
  evidence contradicts that assumption. Missing inline lists can use an explicitly
  checked domain extension. This does not establish physical storage width,
  missing-value encoding or availability; retain those omissions and the assumption's
  provenance.
- SWECOV holdings establish possession by that project. Multi-year tables do not
  establish per-year column availability.
- SWECOV's MONA storage-schema export supplies type evidence for steward-held columns
  absent from SCB documentation and for delivered errata. Each register selects its
  tables by literal prefixes; folded column names join waves. For delivered errata,
  storage classes widen with documented Datatyp from the same column and variant.
  Integer widens to decimal, and any text wave widens the variable to text. Date-only
  waves yield date; date mixed with a numeric class leaves the type unresolved with the
  evidence in the diagnostic. Conflicting documented Datatyp values on overlapping
  editions also widen by that lattice. When every SWECOV wave in the register's
  prefix-selected tables stores the column numerically, that numeric evidence caps a
  widened text declaration to the numeric classes. Storage metadata never establishes
  per-year availability or row content.
- Agents can investigate official handbooks separately and author structured curation
  with document/page references. Neither preparation nor a build extracts or interprets
  PDFs or calls an LLM.

A source update can contradict an earlier decision. The build must then expose the
changed evidence and block its reuse; it must not refresh the decision's expectations.

## Cleaning

Adapters implement the formats actually delivered. A changed or ambiguous layout fails
with retained evidence so the adapter can be repaired. There is no speculative parser
for hypothetical future formats and no custom adapter per SCB register.

`normalization.py` holds the shared mechanical rules. Normalization is idempotent and
never replaces the original cells:

- Single-line labels use Unicode NFC, normalized line endings and spaces, trimmed edges
  and collapsed whitespace. Case, diacritics, words and punctuation remain.
- Paragraph text normalizes NFC, line endings, nonbreaking spaces and trailing padding.
  Internal indentation, tabs and paragraph breaks remain. Arbitrary invisible-character
  removal, line-wrap guessing, HTML decoding and mojibake repair are not generic rules.
- Codes and column tokens use NFC and outer trimming only. Leading zeros, internal
  spaces, case and punctuation remain significant. Numeric-looking codes stay strings.
- Known SQL integer, text, decimal and date aliases normalize mechanically. The original
  declaration and width remain evidence. A text-to-integer change requires curation. SCB
  nonnegative integer length spellings normalize to decimal spelling; other length forms
  remain. SCB `Mattenhet` `Okänd` is a source-defined unknown.
- Exact supplied dates and recognized same-year ranges retain their endpoints. A school
  year `Läsåret YYYY/YYYY+1` — with or without the `Läsåret` prefix — is one period 1
  July of the first year to 30 June of the second, as is a
  `Höstterminen YYYY - Vårterminen YYYY+1` term range; a `YYYY-MM - YYYY-MM` month range
  is one period from the first day of the first month to the last day of the second. A
  `Läsåren A/A+1 - C/C+1` school-year hull pools over 1 July of the first year to 30
  June of the last. A `Deklarationsår YYYY (beskattningsår ZZZZ)` with ZZZZ one below
  YYYY is the income year 1 January to 31 December. An exact `Kvartal 1-3 fr.o.m. YYYY`
  or `Kvartal 1-3 fr.o.m YYYY` label is an open quarterly delivery line from YYYY-01-01.
  This interval is its hull: it includes Q4 even though that line only delivers Q1–Q3.
  `source_scopes` alone applies this reading; `edition_bounds` and `edition_claims`
  still read these labels as one year's Q1–Q3 for inventory coverage and errata era
  order. Other multi-year periods remain pooled. The edition label and a variable's
  declared measurement/reference period are separate observations. An SCB
  `YYYY, slutlig version` edition supersedes the same variant's
  `YYYY, preliminär version` per native variable, retaining its records as support
  evidence. A pooled scope carries its whole-range bounds (Y-202): the exact interval's
  endpoints for a multi-year exact range, else the first claim's start through the last
  claim's end — but only when every claim is a whole calendar year. Other term edges
  (Komvux HT/VT) and forecast-register ranges carry no range. A pooled scope without
  both bounds stays unresolvable downstream — the range is carried evidence, never
  re-parsed from the label.
- Value-set content may deduplicate identical code/label pairs for storage and
  comparison. Equal codes with different labels remain distinct. Deduplication does not
  infer list identity, membership, variable identity or authority.

Year-independent delivery is a separate scope, never an unbounded calendar interval or
pooled delivery. Checked source authority may replace an exact occurrence's unknown
scope with `year_independent`; raw unknown scopes remain evidence. Formation emits
NULL-date states only from agreeing independent occurrence and complete coding claims.
Unknown or dated competitors and supplied member-validity restrictions fail closed.
Literal descriptive-text markers remain nonmembership evidence. Independent inventory
coverage proves exact physical columns and shapes without claiming calendar coverage;
dated classification and parallel-column decisions cannot assign synthetic dates to
these states.

SCB records retain native IDs and joined parent assertions. Population descriptions
attached to an edition do not assign its variable occurrence to a particular population.
Registerinformation, summary flags, identifiers, source-schema metadata and time-series
events remain separate source evidence. The source format can declare a precise support
join, such as native variable ID or the documented register/variant/variable/column key.
Common binding checks the complete target cardinality. A join may also declare a
discriminator for the keys that name more than one known native variable, such as the
summary's own version endpoints against each candidate's observed edition names: a row
binds only where exactly one candidate carries every endpoint, and no endpoint is
ordered, parsed as a year or read as coverage of the years between. Ambiguous joins
supply no flags; conflicting flags are not combined with Boolean OR.

SCB value preparation keeps descriptor/value dictionaries, ordered CVID/ItemId
associations, validity declarations, disconnected identifiers and duplicates. Missing,
empty and zero stay distinct. A descriptor label is not a global code-list ID. Exact
validity dates are not truncated to years or silently repaired. An edition's explicit
item associations establish membership across its finite scope when known global item
dates would leave gaps. This is the accepted continuity assumption when no independent
period information is supplied. Original item dates remain evidence and every widened
association emits a warning. Supplied and section windows still restrict membership;
unknown or conflicting validity, ambiguous joins and competing lists still withhold.

The exact `Tal` and `Beskrivande text` rows whose code, version and level agree are type
declarations, not enumerated codes. Cleaning records that distinction on the descriptor;
the common binder retains their associations without creating a code-list claim or
requiring item validity. Other members in the same descriptor remain ordinary codes.
Unknown code-equals-version shapes fail preparation rather than silently extending this
interpretation. Changed interpretation requires fresh prepared artifacts; old prepared
format versions cannot be reused.

Socialstyrelsen workbook cleaning retains register/subset/variable assertions, language,
headers, annotations, preambles, code-list sections and unparsed rows. Excel cell types,
number formats and hyperlinks remain evidence; displayed code spelling is distinct from
stored cell value. A blank Data till on a row with a supplied Data från reads as an open
end; other missing dates remain unknown. Cleaning does not inherit dates, synthesize a
subset, group variables by name, bind code lists or mint catalog identities. Known
workbook layouts have focused tests; unknown list shapes preserve their original rows
without fabricated members. `Länk kodverk` values that exactly name a sheet in the same
workbook, with an optional `!` or A1 cell anchor, are sheet pointers rather than
classification names when the sheet names a variable or has no named variable and no
cell anchor. An unnamed list can bind through an unanchored variable pointer; a pointer
to a sheet naming another variable adds no binding. An anchored pointer to an unnamed
list stays unresolved. Inline `Värdemängd` assignments also separate at a run of spaces
before a valid `code =` token; malformed lists remain unresolved. SOS contact cells stay
as delivered evidence without a parent metadata field.

An explicit `Värdemängd` representation such as `Se Kodlista_Civil` can bind that exact
sheet in the same workbook. One supplied list header may differ from the physical column
only in case; multiple headers, contrary row references or a different workbook prevent
the binding. Original pointers, headers and codes remain unchanged. This route does not
relax support-record coordinate joins.

The identifier flag does not suppress an exact inline finite enumeration. Missing labels
still produce an incomplete-membership diagnostic, while genuine open keys retain their
existing treatment. A linkage marker cannot silently discard supplied code assertions.

Authored thin-provider TOMLs use `sources/curated_records.py`. Their prepared input role
selects the maintained-declaration compiler, including canonical SCB TOMLs. The provider
name does not suppress this contract or apply it to SCB machine exports. Checked
compilation combines declared parent and variable bounds; absent start bounds still fail
without an inferred date. They are machine-readable source declarations with publisher
and transcription provenance. Reading them does not apply parent defaults, invent false
flags or make the old final-catalog adapter part of preparation. An omitted
thin-provider flag is a deliberate false claim (Y-169). SOS workbook variables carry the
same explicit-flag contract from the delivered template (Y-193): the Kopplingsvariabel
cell is the identifier claim — a non-blank linkage marker reads as explicit true, a
delivered blank as explicit false — and every workbook variable is explicitly sensitive,
because all SOS deliveries are health and social-services microdata. Classification
CSVs, authored code lists and the LISA workbook similarly supply source evidence for
common resolution. The SOS register name is the DCAT-AP Titel; the general sheet's
Datamängd cell is kept as `dataset_label` evidence and never competes with the name
(their spellings disagree for LSS, HSL and SOL). A workbook without a DCAT sheet keeps
Datamängd as its name. When a subset token occurs more than once, its semantic key and
native variant coordinate include the row's Deldatamängdsetikett as well as
Deldatamängdsnamn. Unique tokens retain their existing keys. An authored SOS route names
the chosen labeled row exactly; renaming or removing it makes the route stale. LOVA's
`A_LOVA` routes to its Huvudtabell row and `A_LOVA_LISA` to its ekonomi/arbetsmarknad
row. The prepared source-records schema is 15; older stores must be re-prepared.

A missing SOS subset sheet does not establish that its variable rows describe one table.
An authored named-variant topology binds the explicit native subset coordinates on those
rows and carries their literal names into parent resolution. Missing declared tables
make naming stale. The separately authored single `_default` topology remains an
explicit curation choice. BU uses eleven native tables, keeping their differing storage
types and coverage windows separate without altering the source records.

## Input storage and preparation

Input capture and catalog building are separate operations. The host-local input Git
repository has no remote. A branch name is not a pin: selection names the exact accepted
commit and manifest digest. Preparing a candidate never commits it, replaces accepted
inputs or changes the active catalog.

Raw archives can remain compressed outside Git. The lossless compact SCB snapshot keeps
original dictionaries, associations and ordering without versioning the wasteful
expanded CSV representation. Other selected machine-readable artifacts are preserved in
the input bundle with complete inventory and provenance. Reconstructing from cold
storage and preparing a source update are explicit operations.

`prepare-sources` cleans and validates once. `prepared_sources.py` interns repeated
fields, original cells, coordinates and locators in an indexed SQLite store beside a
small manifest. Source order, physical duplicates and revision membership remain intact.
`prepared_catalog.py` composes this store with compact value sources and literal support
metadata. Preparation validates identities and writes a new candidate directory
atomically; existing output directories are not overwritten.

Warm builds check the accepted Git identity, manifest pin, inventory, file sizes, index
flags and storage version. They do not rehash the entire prepared database, recompute
source record identities, parse original workbooks, expand raw archives or repeat
cleaning. Full verification belongs at preparation/acceptance. The accepted immutable
revision is the trust boundary; decoding uses bounded caches and indexed semantic-key
lookups.

Support target cardinality uses a distinct native-coordinate projection before attaching
facts. It avoids hydrating unrelated prose and cells but includes unknown/ambiguous
native targets. The main resolution pass still accounts for every physical occurrence.
Value binding shares immutable claims only when native membership and effective scope
agree. Bounded caches reuse period intersections without dropping original association
or validity evidence. Different content under one claim ID is a fatal contract error.

A source update follows this sequence:

1. Capture the new actual-format delivery in a new input candidate.
2. Clean and fully validate it; inspect format failures rather than accepting partial
   parsing.
3. Pin the candidate prepared artifact and the existing checked decisions.
4. Inspect applicability, strict errors, source and catalog impact, and semantic DB
   differences.
5. Resolve new discrepancies through separately reviewed curation, or acknowledge an
   exact unresolved issue whose output stays withheld.
6. Accept the reviewed input/decision revision explicitly, then build and publish
   strictly.

A decision cannot authorize its own changed input. A repair that supplies a formerly
missing row also invalidates an omission claim covering that row.

## Common curation

### Curation source and layout

Tracked TOML in this repository is the only curation input. The build reads it directly.
It is not part of the input bundle, and no preparation depends on it, so a curation edit
never forces a re-prepare. Curation compiles in-process on every build; there is no
stored selection. The build summary records a hash of the curation tree. An optional
decision dump serves inspection.

The target layout:

- `curation/registers/<provider>/<register-slug>.toml` holds one register, its
  `[register]` slug, `[[variant]]` and `[[variable]]` naming, and the other register
  curation. Generated variable pins live beside it in `<register-slug>.auto.toml`. An
  optional directory holds a register family, e.g. Komvux 248/249/250.
- `curation/classifications/<short>.toml` holds one classification: its metadata,
  sentinels, the label list behind the binding rule, and overrides.
- Global files keep the cross-register curation: relations and `same_as`, tags, lineage,
  successions spanning registers, and slug freeze state.
- `worklists/concept_groups.auto.toml` is generated from a built DB for the worklist
  tool, not curation. Enrichment is register-scoped curation in each register file. The
  concept-group generator treats a matching literal register `[[group]]` as an already
  accepted family; accepted families have one record, in their register file.
- SWECOV steward holdings belong to the steward layer, not to global SCB errata.

The register tree is loaded once as strict Pydantic models. A register's provider, slug,
and native id agree with its path. Unknown tables, unknown fields, duplicate
declarations, and entries scoped to a different register fail with the source file and
entry index.

SCB errata records omitted rows and explicit periods for existing editions whose names
are topics rather than dates. The register file implies the register; each entry names a
variant and literal SCB column/version coordinates. A curated
`[[identity.edition_split]]` moves exact named editions of one native variant to a
declared split variant. Its `editions` and `source_editions` lists account for every
native edition name; a new or missing name makes the entry stale and withholds the
split. Their edition and population parents move with them; edition name shape never
implies a split. An errata addition naming a moved edition is stale until additions can
be rebound under the split variant; corrections to native rows still move with the
split. RTB's eight `Kvartal 1-3 fr.o.m.` editions use this surface to keep quarterly
delivery lines separate from their annual parent variants.

An SCB `[[identity.column_owner]]` binds a literal column to an accepted owner within
explicit source editions. Optional `expected_fields` pin reviewed source facts using the
existing field-expectation contract. Guarded entries require a finite edition list and
unique known fields. Changed facts refuse fresh compilation; the compiled complete
family also guards every supplied field during replay. This supports a reused column
whose explicitly documented amount or rate role changes between editions without
normalizing units or borrowing another owner's meaning. Physical source multiplicity
remains governed by the prepared-source pins and ordered inventory.

SOS `[[identity.split]]` can partition a complete native family by literal supplied
`data_type`, `deldatamangd`, `name`, or `description`. Supplied names distinguish reused
columns whose clinical code, flag, or event-file meaning differs despite equal data
types. Exact supplied descriptions distinguish observation times when the literal column
and name are reused, such as year-end residence versus residence at a care event. They
do not authorize rewriting the description or merging different events. Every observed
discriminator must be declared, and every declared discriminator must occur; new or
missing values make the whole split stale. Multiple literals may share a declared owner.
Name-conditioned effects keep distinct originals at the same semantic source coordinate
separate. This establishes concept ownership; it does not infer delivery
interchangeability or fill missing file-role coordinates.

Several `identity.edition_split` entries may partition one native variant into disjoint
reviewed delivery roles. A source edition may have only one target, and each entry lists
the complete native edition inventory through selected editions and its complement.
Explicit source edition labels establish the role; unknown population definitions remain
unknown. Frozen cases capture complete literal fields, parent declarations and coding
references. Splitting delivery context changes neither variable ownership nor source
periods or code domains.

`[[errata.version]]` carries `variant`, `name`, `evidence`, and `noted`; it adds an
undocumented register edition and requires a year-bearing SCB version name. Version era
order follows the maximum year the edition claims, so adding a historical edition cannot
replace a newer documented edition's latest spelling or type.
`[[errata.edition_period]]` names one existing native edition by variant and SCB's
literal edition name. For a name the period parser cannot interpret, its documented
`valid_from` and `valid_to` are the exact inclusive occurrence interval; `evidence` and
`noted` record the source of that claim. A missing or renamed edition, or a name the
parser can interpret, makes the entry stale. No register coverage or nearby edition
supplies an inferred period, and other unparseable names remain unsupported.
`[[errata.delivered]]` carries `variant`, literal `column`, existing `versions`,
`evidence`, `noted`, and optional `upstream` and `native_variable_id`; it adds omitted
rows for a column documented elsewhere on the variant and names edition tokens verbatim.
The native-variable anchor selects one documented identity when the same column literal
belongs to multiple variables in the variant's history. An anchor must occur under that
column; without one, the column must identify exactly one variable across the complete
variant history. `[[errata.column]]` carries the variable identity (`name` and
`definition`), a source and evidence, plus either named versions, bounded
`holdings_period`, or the legacy undated `all_versions = true`; it mints a variable for
a column SCB documents nowhere on that variant. Its identity is `(register, column)`, so
two variants of one register use the same variable identity. `source` records the
evidence class; it does not change the materialized rows.

`[[errata.delivered]]` and `[[errata.column]]` split exactly one question: does SCB
document this column anywhere on this variant? One column/variant omission is exactly
one kind. Delivered entries clone a real row and therefore cannot describe a column with
no row; column entries mint a row and therefore cannot describe a column that already
has one. If SCB ships an omitted row, the build fails with `scb_errata_now_present`.
Retire only the fixed edition from `versions`, or delete the entry when that was its
last edition; do not edit it to keep a failing build green. Git history preserves the
upstream-error record.

SCB omitted-row converters share one immutable, validated `ErrataVariantContext` per
source variant during compilation. It validates complete native coordinates and edition
references before indexing literal folded columns, native variables and semantic refs.
Ordered physical duplicates remain in each index. Every entry still checks its exact
source peers and facts; the context changes lookup cost, not evidence or guard behavior.
Complete field, parent and coding projections are captured once per immutable context;
selecting refs retains all physical alternatives. Applicability checks likewise reuse
actual projections only within one immutable `SourceEvidence`, keyed by semantic ref and
every selected projection coordinate. Expected projection tokens use a separate cache in
that same context, keyed by the entire immutable validated projection value. Different
expected values, masks and native coordinate types remain independent. New evidence
requires a fresh context. No cached source state survives a build.

Compiled-scope JSON reads also share identical immutable `RecordProjection` and
`RecordExpectation` objects within that read. Every nested value, projection shape and
alternative is validated before interning. Keys contain the exact model class and
complete normalized JSON value, including native coordinates, scopes, parents and coding
references. Separate scopes receive separate contexts. This restores sharing lost by
serialization without bypassing contract validation or changing garbage-collector
settings; it avoids retaining a full duplicate guard graph for each reviewed case. Scope
serialization passes existing model objects directly to Pydantic's JSON adapter,
avoiding an intermediate nested Python dictionary tree. Serialization warnings are
errors, and nonfinite raw numbers remain explicit tokens for strict rejection rather
than becoming null. Final JSON validation still rejects unexpected subclass fields.

Serialized scope contracts still validate all nested inputs. Field expectations reuse
the source model's single-field type guard instead of constructing every absent field
again. A singleton record alternative needs no uniqueness comparison; its nested
projection still receives full validation. Multiple alternatives retain shape and
canonical-token checks. An added delivery retains its own literal donor and negative
native-base records as quantity evidence. Complete edition or variant support still
guards the correction, but unrelated variables cannot become contributors to that
quantity's metadata or coding authority.

An explicitly reviewed `upstream = "additional-physical-column-in-version"` entry
requires a native-variable anchor and adds the exact physical column alongside the
edition's existing columns. It retains positive siblings and negative native bases
unchanged. Complete native-family and edition peers guard the addition, including
siblings outside the target period. An already supplied own literal refuses the entry.
This mode takes storage type only from that physical column's storage evidence; it does
not borrow predecessor metadata, flags, operation or coding. An optional
`storage_column` names a reviewed physical header when it differs from the catalog
literal. It is permitted only in this anchored mode and must have a supported storage
declaration under the inspected table prefixes. Missing or unsupported headers refuse
the addition; header suffixes never infer a mapping.

An exact `errata.field` column-name correction can replace a positively supplied old
label when complete holdings headers and documentation establish the same native
quantity's physical replacement. It requires all source fields, exact edition and scope,
complete family peers, parents and coding. Original cells remain intact; the correction
does not assert simultaneous delivery of the old and replacement columns.

SOS `[[errata.data_type]]` corrects one existing original record's interpreted
`data_type` without changing its delivered cells. It names the original Deldatamängd
token, variable native id, and literal column, checks the original type and
representation, and records evidence and a canonical noted date. The entry must find
exactly one physical record in its owning selected native register; missing or changed
evidence is stale, and multiple records are over broad. A checked case guards all
original register/variable/Deldatamängd peers regardless of column or type. A second
peer blocks fresh compilation, and a late-added peer invalidates the guard. The only
replacement is `data_type`; routing, identity, coverage, coding, and source evidence
remain separate. LOVA's `DESLEG_DATUM` uses this surface because its workbook declares
`YYYY-MM-DD` while its source type cell says `Decimal`.

`is_identifier` and `is_sensitive` are strict booleans. Set `is_identifier` only when a
sibling row for the same variable already carries that flag; this is a PII guard, not a
way to infer identifiers from a label. HSL was checked for monthly delivery families and
has none; it intentionally has no period-family declaration.

Register `[[group]]` entries are literal, opt-in presentation folds; member selection
uses no name patterns. They include materialized families that formerly referenced
auto-generated candidates; the candidate generator treats a family whose matching group
is already in the register tree as accepted, so accepted families have one record.
Candidate output is worklist data and is never build curation. A group lists exact
variables or delivery-column members. One-axis groups may use a flat value/label
coordinate; multiple axes name their labels and each member's coordinates; axis-less
groups are explicit umbrellas. They alter presentation, not identity or bindings.
`[[code_label_pair]]` is also register-local: both FQIDs belong to the same register,
and the pair only folds matching code and label concepts.

Delivery descriptions are enrichment, not source facts: they fill an empty description
only when an exact delivery-column match grounds the text. The worklist generator drops
generic helper codes, version-axis variables handled by the vintage fold, overloaded
grade/participation columns, and conflicting descriptions across vintages. It normalizes
whitespace and removes trailing footnote markers. Alias enrichment is separately limited
to an exact variable/column pair.

A period family folds period-named columns into one annual variable with a state for
each delivery year and a matching representation window for each period column. It runs
before slug assignment and names parallel period columns, not non-overlapping era
renames; those are represented by succession edges. The tracked families are the four
LISA monthly families plus SCB `bas/jobbink`, `ekonomiskt-bistand/ibel`,
`ekonomiskt-bistand/sbel`, and `rams/lonfink`, the other bounded twelve-month families
present in the corpus. HSL was checked and has no monthly family. Alias-window
declarations name exact source editions for a representation the identity already owns;
they add no variable, state, alias identity, or coverage in a neighboring edition.

### Entries and pins

A curation entry carries no pin by default. It names literal source coordinates:
register, variant, column and edition labels. The compile checks every entry against the
complete source scope; an entry that matches nothing, or more than it names, is stale,
which is an error. Unrelated later editions, layout changes and deliveries elsewhere
therefore never stale it. Compiled `CodingDecision` cases capture checked evidence in
memory for the current build.

Coding register entries name finite ISO `periods = [[from, to], ...]` when a decision is
window-grained. The compiler checks each window against that column's complete source
lists; optional `keep_members` and `list_members` are literal `[code, label]` pairs when
one list label has multiple meanings. It captures coding fingerprints in memory. Neither
the fingerprints nor source-member pins are stored in tracked coding tables.

`source_curation.py` evaluates cases compiled in memory from tracked coordinates. Each
case checks exact members, finite periods, fields and expected facts (`expected_*`),
with peer guards that check completeness without selecting targets. Changed relevant
facts, new intersecting evidence, lost support, changed membership or changed
cardinality make it stale.

### Decisions and formation

Cases can coordinate identity, occurrence, field, period, coding and representation
changes. They are not restricted to one atom per field. Equal assignments compose;
contradictory assignments withhold the disputed aspect. No case reads another
correction's output as its supporting source. Application order cannot choose a winner.

Register `[[errata.field]]` entries correct reviewed source-backed text in `name`,
`description`, `definition` or `measurement_unit` at exact original
variable/variant/column coordinates. Unit corrections additionally pin the supplied unit
and both coverage fields. They reconcile literal wording only when the source definition
positively establishes the same quantity; they do not convert values, merge differing
scales or infer absent units. `[[errata.occurrence_period]]` entries require one exact
native source edition for SCB. SOS rows lack that coordinate and instead require both
literal supplied coverage fields alongside the exact native variable, subset and column.
Both entry types guard the original edition text, both original scopes and all four
supplied prose fields, including absent or unknown values. Complete native-family
support and peer guards retain unrelated editions and coding references. Period entries
carry explicit replacement `TemporalScope` values; omitting an interval's `end` in TOML
means an open end, while unknown and pooled scopes retain their labels and bounds. These
entries emit existing checked field/period effects and never rewrite the original source
records. A finite scoped name correction selects exact original source scopes and
carries those same scopes into its conditional replay effect. A monthly row sharing an
annual row's semantic reference cannot inherit that name correction.

Period entries may cite exact supplied subset metadata through `authority`. Compilation
checks its full source projection, register and routed variant, positive coverage and
containment of the replacement delivery window. Complete metadata peer guards and source
expectations remain in the case for replay; changed, missing or new conflicting
authority refuses the correction. These decisions change effective delivery coverage
without changing original coverage cells or historical dates stored as values.

`[[errata.support]]` retains one documented erroneous assertion as support rather than
catalog data. Its positive `authority` must name another native variable in the same
source-local variant, literal column and exact edition, with matching original scopes
and edition text. Both selectors guard all four prose fields. Complete target and
authority families supply checked projections and peer membership; a pinned combined
fingerprint checks their source code lists and validity during compilation. The emitted
`CheckedSourceUse` carries a literal field condition, so a sibling column sharing the
semantic source reference keeps its catalog role. Originals and coding remain evidence;
this decision neither merges identities nor corrects unsupported physical roles.

The `nonphysical_projection` kind handles an explicitly negative column assertion. It
requires an empty literal column and every original `SourceFields` value, with the
column's status guarded as negative. Its positive witness must be the same native
variable and variant with matching supplied name and definition in an exact finite
edition. Complete target, witness and peer projections and coding fingerprints guard
compilation and replay. The witness establishes the quantity, not delivery in the
negative-column edition. Only the checked negative projection becomes support; all
original facts remain available and positive physical occurrences retain their role.

Ordinary variables form from an established native identity. Their names bind that exact
source coordinate; additional deliveries do not require handwritten whole-variable
cases. A complete unchecked native family retains catalog identity across renamed
columns when every original supplies the same positive exported name and definition
(after whitespace normalization), physical columns, types and periods are explicit, and
distinct columns never overlap within a native variant. Literal delivery metadata remain
separate; this establishes source identity, not statistical equivalence. Explicit
checked ownership takes precedence. Missing semantic or physical evidence and concurrent
columns still require curation. Other case or diacritic twins use the shared
column-identity fallback only when no edition co-delivers both spellings. An accepted
name alone cannot establish a partition across ambiguous columns. A partition or
parallel-column family requires checked ownership and complete relevant membership. An
implicit split suffix binds case or diacritic twin literals only when their folded
column key matches and no variant edition co-delivers them. A one-owner partition may
list a spelling that recurs after an intervening rename (A→B→A). An explicitly reviewed
complete literal map may retain the base source-native key when its named quantity and
definition agree across deliveries. Such a map cannot mix split owners or unassigned
literals. It preserves existing naming, guards all original fields, parent facts, coding
references and complete family membership, and does not claim statistical equivalence
across units, calculation methods or delivery levels. Exact source type spellings
`numerisk`, `alfanumerisk` and `Character` classify as decimal, text and text during
resolution, including already accepted prepared records. Original type declarations
remain evidence; classification never uses substring guessing. Blank column records
remain original evidence without manufacturing a delivered column; a literal claimed by
two native variables in the same edition needs a split or `native_variable_id`, never a
fold into one owner. Related but different variables remain connected through groups; a
shared stem or suffix is not evidence that they are one variable.

Variant-scoped column owners may select exact source edition labels when the source
documents a change of measurement basis within one column. Every selected label must
exist; competing owners or a stale selector withhold the whole ownership decision.
Unselected editions retain their native identity. These selectors never infer dates or
extend delivery or coding validity.

Audited naming ambiguities can connect an existing catalog name to an exact unresolved
source identity. These are error attributions, not new identities or waivers. They must
still match the original family and actual unresolved result. Missing conversion
mappings or stale attribution bridges are implementation failures.

Parent metadata reconciles every observation of one native parent with no positional
selection: conflicting fields stay unknown, and a conflict on the name withholds the
parent and its dependents. When an applicable checked naming declaration for that
register or register_variant pins exactly one of the observed names, the declared name
establishes the parent while the conflict diagnostic withholds only the remaining
fields. No declaration, several pinned names, a pinned name outside the observations, or
a stale declaration leaves the parent withheld exactly as without it.

An added delivery names an existing identity, variant, edition and supplied period with
checked evidence. It is a declaration, not a fabricated physical source row. Supplying
column presence derives type by widening SWECOV storage with documented Datatyp on the
same column and variant; an unclassifiable documented type or date mixed with numeric
leaves it unresolved. Flags and coding are not copied from another edition. Any donor
metadata or membership must be explicitly selected and guarded. Undated `all_versions`
holdings preserve unknown coverage; pooled editions cannot create annual states.
Delivery correction guards capture every supplied source field, parent fact and coding
reference from both donor and target evidence. Changed meaning or bindings makes the
correction stale, even when the physical column and storage type remain unchanged.

`[[representation.matrix]]` activates a reviewed answer partition in its owning register
file. It names a source mode, a normalized evidence JSON path inside the curation tree
and the exact expected native selector. Missing evidence, an escaped path or a selector
that disagrees with either the JSON or the owning register fails configuration.
Undeclared evidence files do not activate processing. The existing meaning,
source-member and coding guards still govern each conversion.

The CIS cooperation-matrix declarations bind reviewed answer meanings to their exact
source partitions. CIS 2014 has a blank-column source member, so each added answer
carries a reviewed `decimal` type from the SWECOV CIS2014 storage catalog and explicit
identifier and sensitivity flags from SCB's adjacent 2014–2016 wave declarations. The
evidence JSON records both sources; type and flags are authored for every answer, with
none of those facts copied from the blank donor. CIS 2016 answers retain their named
source rows and support flags.

A steward holding dated at dataset grain (`holdings_period`) converts to exactly one
pooled-range occurrence over that range (Y-212): the delivery list says the column is
held somewhere inside the range, never that it exists in every wave, so the entry is
never expanded into per-edition claims. The legacy undated `all_versions` form keeps its
unknown scope until the curation rewrites those entries with their ranges.

Copied coding pins the original bound donor evidence at the declared occurrence scope
with explicit `expected_codings`. Missing fingerprints are a contract error; changed
membership or validity makes the entire occurrence case stale before effects run.
Unknown and pooled scopes retain their original validity constraints in the fingerprint
without acquiring dates. Scope fingerprints hash semantic content only — the (kind,
label, intervals, pooled_start, pooled_end) tuple, with the pooled bounds kept when set
— never the model dump, so a new optional scope field left as None leaves every existing
fingerprint unchanged. A fingerprint is captured from prepared evidence in memory during
each build. Checked value-list field corrections bind using the effective declaration
while retaining the original source record as evidence.

An explicitly open upper bound differs from an unknown period. Resolution can retain a
known start and explicit open end; the writer uses `9999-12-31` as the storage sentinel.
A literal source year 9999 is not dated evidence.

### Occurrences and coding

Occurrence reconciliation forms exact nonoverlapping column intervals. Conflicting
optional fields become unknown only over their overlap. Conflicting availability or
population withholds the unsafe segment. Missing periods/columns remain explicit issues.
A delivered literally blank column (raw `""`) is not a missing fact: SCB states the
member has no physical column (aggregate-statistics registers), so the reader carries it
as an explicit negative column claim and reconciliation omits the occurrence on purpose,
reporting it once as an `omitted_columnless_occurrence` warning. A variable whose every
occurrence is columnless is not materialized; its `no_supported_states` outcome is a
warning for this cause alone. A columnless-only remainder left after checked splits
likewise warns and is omitted. An undelivered cell, or a delivered cell that is not
literally blank, stays unknown and keeps the error path. A gap between supported periods
stays a gap.

Pooled multi-year editions form one marked state (Y-202). A pooled scope carries its
whole-range bounds from cleaning — the edition's exact interval or its whole-year claim
hull (see Cleaning) — and occurrence resolution places it as exactly ONE interval over
that range: never one state per year, never inferred annual availability inside the
range. A span is marked pooled only where NO explicit (annual/precise) occurrence covers
it: where an explicit occurrence overlaps, it wins and the span resolves as an ordinary
state, with pooled evidence excluded from field reconciliation on that span, so a pooled
span never overlaps an explicit span on the same (variable, variant, column). A pooled
scope that carries no range stays unresolvable (an unsupported occurrence, as before).
Coding membership on a pooled edition is bound over the whole pooled range (Y-207). The
marker persists as `variable_state.pooled` (INTEGER NOT NULL DEFAULT 0, schema 6.10.0)
through `ResolvedState`/`IRVariableState` into the DB, and `validate_built_db` fails a
build whose pooled-marked window overlaps an unmarked window on one column. Adjacent
pooled segments on one column whose reconciled state-grain facts, population, and coding
evidence agree merge into one pooled state over their combined window (Y-209).

Operational definitions and source references are state-grain. Separate resolved periods
and variants each keep their own exact text and provenance, so differing texts across
one native family are not a variable-level conflict. Their variable-grain summaries stay
populated only when ordinary reconciliation yields one stable value. Disagreement on one
overlapping column period likewise resolves that field to unknown without an occurrence
conflict diagnostic; `reference_period` is also absorbed this way because it has no
published output. The underlying occurrences and provenance remain inspectable. For
`measurement_unit`, only the closed exact-pair list in `source_intervals.py` selects a
published spelling; all other disagreements remain conflicts. Distinct canonical
nonnegative integer `data_length` values reconcile to their maximum only when every
length-supplying observation declares the same resolved `data_type`. When distinct
documented data types widen within one class, canonical nonnegative lengths use the same
maximum rule. A widening across classes, including a storage-capped text result, has no
length and no length conflict. Unmapped or incompatible types keep their conflicts.
Other conflicting fields still raise occurrence diagnostics.

Code-list bindings identify the exact source members and their period constraints before
membership resolution. Concurrent complete lists must agree; neither the largest list,
latest row, descriptor label nor union of competing members chooses a winner. Empty
active membership is distinct from an explicit accepted uncoded period.

Checked coding decisions pin all competing observations in a finite column interval.
They may select an existing complete list, accept an uncoded period or omit a state. A
separately checked witness can supply a constant complete coding over another period
only when an existing decision explicitly authorizes that extension. Changed target or
witness evidence invalidates it. Omitted states retain their evidence and do not become
negative availability claims.

Under the reviewed explicit-link policy, a checked choice can select a supplied complete
superset when every positively linked code is retained and shared code labels agree.
This includes explicitly supplied routing and nonresponse codes. The choice requires
complete source-row authority and coding guards; changed, missing or additional source
assertions invalidate it. Contrary meanings for a shared code remain unresolved.

A checked `coding.extend` entry can use the same complete prepared-row authority to
carry an accepted quantity's own positively bound source list into an earlier missing
window. Complete physical originals, parent facts and ordered coding associations guard
the target and witness; projections sharing a source reference are checked separately. A
holding anchor for another quantity cannot supply its coding. Contrary historical
members or labels remain unresolved under the explicit-link policy. A literal empty
source-book label is accepted only with explicit nonempty members and complete prepared
source authority, including raw fingerprints; it never receives an invented name.

`coding.documented` supplies an independently reviewed finite list for an exact native
owner, variant, physical column and finite occurrence window when source coding cannot
establish membership. Its document URL, SHA256 and page references are tracked
provenance; the build never fetches that document. An alternative authority is the exact
prepared workbook rows themselves, when their prose supplies the meanings of already
supplied finite code tokens. This form pins the source revision, physical locators,
complete original fields, scopes, parent facts and all coding fingerprints. Changed,
missing or new rows and changed incomplete lists invalidate compilation and application.
Source rows cannot be mixed with PDF authority. A finite source-row decision may carry
explicit reviewed label-equivalence certificates for unchanged code keys, such as
`Landsting` and `Region` for the same county-government sector. Each certificate names
all exact observed labels and selects one label already supplied by the source. The
exact authored version label identifies the positive book; complete original and raw
coding guards still cover other historical books. Compilation and replay require the
observed label set and code keys to match exactly. An optional finite `witness` on a
label certificate selects the quantity's own complete source book when one version label
recurs with different missing-value tokens across years. Every witness claim must supply
the exact code keys and reviewed labels throughout that window. Full raw coding and
original guards still cover all years. Compilation and replay refuse any contrary
positive target domain; the witness cannot replace its keys or meanings. Historical and
later domains remain separate, without backfilling later NULL or blank tokens. This
exception permits semantic label normalization of a supplied list, not algorithm
equivalence or code reassignment. Missing, new or changed labels, books or associations
invalidate the decision. Original labels remain in source evidence; no fuzzy matching or
code reassignment occurs. PDF decisions retain the finite window requirement. A
source-row decision instead may name its exact supplied `TemporalScope`, mutually
exclusive with authored finite periods. That scope must match every effective occurrence
and list claim exactly, including an open `end`. The authored scope and originals retain
`end = None`; only the existing internal interval normalization reaches the maximum
date. Complete original and list guards invalidate any source-scope change. This does
not authorize future extrapolation or relax finite bounds for other coding decisions.
Exact code strings, including an explicitly documented empty string, are preserved. The
compiler captures original fields, scopes, coding references and complete column peers;
changed finite claims invalidate application, and any supplied complete list in the
window makes an ordinary documented entry stale. The guarded witness label certificate
additionally requires every positive target domain to match after exact reviewed label
normalization. Contradictory documented assignments withhold only their overlap.
Original claims and nonmembership associations stay in source accounting. Raw
type-marker whitespace is not a finite-membership guard: those associations cannot
establish a list and are retained in the documentary audit.

A finite source-row authority can also certify an explicit complete enumeration in a
named prose field. It records the exact ASCII decimal-code-and-label lines and requires
them to equal every numbered line under that syntax in each guarded original. Complete
recognized type-marker bindings are fingerprinted separately, including their ordered
raw associations; absent, changed or competing finite bindings invalidate the decision.
The original type markers remain evidence. This route supplies only the exact stored
codes and labels explicitly enumerated in the source, with all later coding unchanged.
The separate own-name syntax accepts only a complete `Kategori` assignment of literal
alphabetic codes to meanings separated by commas or `eller`. Exact incomplete source
list tokens must agree with that enumeration. Complete original and raw-list guards
remain required; arbitrary prose, negations and unlisted code tokens cannot supply a
domain. This own-name route may use an exact supplied open source scope; the decimal
marker route retains its finite-period requirement.

`coding.support` retains one proven erroneous physical association as documentary
support while excluding only that association from effective membership in a finite
column window. It pins the complete ordered source claims and the exact target and
positive authority associations in the same bound list. The authority must establish
membership over the whole window. Changed, new or missing rows invalidate fresh
compilation and application. Raw claims, codes, labels, periods and ordered associations
remain unchanged; effective segments carry case provenance and an explicit warning. This
cannot supply a missing code, choose a temporal cut, or suppress an entire list.

Coding decisions require complete checked effective column delivery over their finite
window. Original field and scope guards still capture the supplied records, including
anchors for accepted deliveries; a changed delivery invalidates the decision.

### Classifications and representations

Canonical classification membership comes from the selected prepared codebook.
Conflicting or incomplete canonical members remain issues; source labels are never
rewritten to fit canonical labels. A literal source-reference dictionary or checked
declaration establishes a variable binding. Code overlap percentages, URL guesses and
name similarity cannot.

The label binding is a rule. A state whose value-set version label exactly matches an
entry on a classification's label list binds to that classification. The binding is
re-derived on every build with no codebook-hash pin, so no codebook or builder change
requires regenerating curation; a classification's overrides are curation.

Conformance compares exact code strings. Noncanonical members preserve the original list
and the known source-declared classification link. They emit a warning and are recorded
as source extensions beside matching official codes (`extended`); an exact match is
`conforming`. The official codebook is never expanded from source data.

A classification may curate an exact-string sentinel list
(`sentinel_codes = [{code, meaning}]` per `[classification]` in
`curation/classifications/<short>.toml`) for bulk/missing tokens. These are local source
extensions with an additional warning naming their curated meaning. Codes match exactly
(`"00000"` never equals `"0"`); there are no patterns or cross-classification lists.
Unknown keys, duplicate codes and overlap with canonical members fail fast. Sentinel
curation does not mutate the official codebook or stale a binding. Original coding
issues remain visible. Unknown or ambiguous classification identities remain unresolved.

Where one literal code has substantive meanings in other source lists, finite
`coding.sentinel` entries name exact source code-label pairs at one accepted owner,
variant and column. They compile through the existing coding compiler into checked
classification decisions. Complete original fields, scopes, coding, effective peers,
effective delivery and the selected codebook are guarded. Changed evidence withholds the
scoped decision. Only matching pairs inside the reviewed windows become local sentinels;
source lists and global codebooks remain unchanged. Missing memberships, competing lists
remain errors; other noncanonical codes are retained as source extensions.

The resolver carries each accepted local exception to the writer as a checked
certificate: literal column, finite state window, exact code-label pairs, selected
codebook hash, complete source-coding hashes and case provenance. The writer checks that
certificate against its actual state and book before validating membership. Missing,
changed or out-of-window certificates cannot broaden conformance. The certificate is
build-time evidence; it adds no canonical codes or public sentinel policy.

A parallel-column decision names each literal column and its finite delivery window. It
reconciles sibling metadata and coding before forming shared states. Conflicting facts
stay unknown; missing members or contradictory representation choices withhold the
unsafe state. Monthly alias windows do not split annual metadata into monthly states.
The stored representative column is chosen deterministically from participating columns
after reconciliation; it never selects a metadata donor.

`[[representation.parallel]]` authoring binds an already curated variable and variant to
exact literal columns, source edition labels and full supplied source windows. Its
metadata window must equal their intersection. Compilation verifies complete overlapping
peers and captures their original fields, periods and coding references, then emits the
existing parallel-column decision only for that intersection. Original pooled source
windows remain unchanged, including the outer periods owned by each column. This surface
accepts one literal column per source edition; same-edition competing columns require
separate evidence and are not inferred from a shared native identifier. Later source
drift stales the checked case. Metadata and coding conflicts still pass through the
existing reconciliation diagnostics.

When an existing checked omission correction supplies the effective column or edition
scope, parallel compilation verifies that correction first and includes its complete
source authority. It checks the exact corrected intersection while retaining the raw
original fields and outer windows. A stale correction cannot supply a representation.

An explicit `column_metadata = "per_column"` retains physical type, width, operational
definition and source attribution on each checked representation window. Each literal
column is reconciled independently; a conflict within that column remains unknown and
diagnosed. The shared state retains only agreed facts and never selects a metadata
donor. The existing `variable_alias_window` carries the mode and these literal fields,
and selected-column reads project them, including nulls, onto that representation. An
absent source attribution never inherits a sibling's questionnaire reference. Coverage
checks compare the source claim with the same column's written window. Names retain the
shared reconciliation contract; literal definitions require the guarded authoring below.
Units are delivery facts with a common summary only when they agree; coding has its
independent mode below. The default `shared` mode retains its existing behavior. Source
SQL widths and precision remain literal metadata; this mode neither converts them nor
asserts comparability.

A calendar-month period family may supply an exact `expected_definitions` map for all
months `01` through `12`. Every original definition must match its month before any
identity change is emitted. A mismatch invalidates the whole projected-definition
family, including its other years. Only these fully checked period families permit a
varying common definition: every effective contributor must be covered by an applicable
guarded case. The common variable definition then stays NULL when monthly texts differ,
with a rule-defined warning. Ordinary variable-definition conflicts and disagreements
within one column remain errors. Annual source and metadata scopes stay annual; alias
windows carry the exact calendar month. State merging, delivery coverage and search
aggregation retain these literal definitions. No generic prose replacement or scale
conversion is inferred.

Common metadata expresses semantic agreement after cleaning and checked curation.
Equivalent wording or unit notation can be normalized through guarded `errata.field`
entries before formation: for example, `kr` and `SEK` can name the same currency unit.
Every raw source assertion remains intact. Curation must establish that the quantity and
scale agree; percentage and proportion, different currencies or differing substantive
qualifiers cannot be collapsed from spelling alone. The builder does not guess semantic
equivalence from arbitrary prose. When checked normalization establishes one common
value, it replaces the need for a delivery-metadata permission for that field.

Measurement units are ordinary delivery/state facts. Every physical state retains its
literal unit or supplied absence. A common variable unit is populated only when all
physical contributors have a positive, semantically agreed unit. Different scales,
currencies or supplied absence leave that summary empty and emit a data warning when
some positive unit is known. The builder never converts values or transfers a donor's
unit to a delivery. Contradictory units within the same physical column interval remain
errors. Blank-column originals stay raw evidence and do not supply a physical unit
window. Unit-only curation permissions are therefore unnecessary and rejected.

A checked `representation.delivery_metadata` decision permits only its explicitly listed
text fields (`name`, `description`) to vary for one exact reviewed owner. Compilation
captures every original field, source scope, parent and coding association, including
sibling support. Checked preliminary-source dispositions participate in the compiler's
ownership context, so their exact support rows cannot be mistaken for catalog
contributors. Formation requires complete coverage of the owner's effective contributors
before suppressing a disagreement in a permitted field. Each state and column window
retains its literal text; a common value is written only when it agrees. Unknown names
still withhold the quantity unless complete checked positive state names are available.

Ordinary delivery-metadata windows remain finite. An explicitly authored `source_scope`
may retain an open supplied delivery scope only when it exactly matches every effective
contributor and its derived window. The original open bound remains unchanged; existing
normalized output bounds do not license inferred availability. Complete original
snapshots guard raw source facts, while permitted fields must also match their effective
literal values. Unrelated checked field corrections remain effective. This does not
convert values, infer currency, choose winning wording or fill absent components.

An independent `coding_metadata = "per_column"` mode retains each checked column's
complete finite response domain on its alias windows. The shared quantity state claims
no common domain when they differ. Original fields, scopes, complete peers and coding
associations remain guarded; missing or contradictory within-column coding is withheld.
Windows intersect the existing coding boundaries and cannot bridge a gap. Each window
stores an existing value-set reference and its literal native version label; readers
project that domain without falling back to shared coding. Shared mode carries no
override. Initially this mode requires unclassified domains; classified domains need an
explicit per-window conformance contract before acceptance. No codes are unioned,
dropped or recoded, and no version names are invented.

Search aliases add discovery spellings without adding states or coverage. An alias
window makes an already owned spelling orderable only within supported state coverage
for its variable and variant. Gaps or competing owners withhold the affected window.
Equal window declarations retain all provenance; conflicting decisions withhold only
their intersection. One alias decision cannot establish another's ownership.

### Delivery coverage accounting

Resolution keeps what each supported occurrence actually claims: one obligation per
finite positive effective claim, on its exact variant, physical column and period, less
the periods an explicit outcome takes back. A checked coding omission takes back its own
slice. A representation decision takes back the periods it reports unresolved and the
periods it assigns to a sibling column — what it does deliver stays a claim, so the
shared state and the alias windows it promises are themselves checked. Obligations
travel with the scope result and are checked against the final resolved variables
immediately before the database is written — in both modes, and whatever else the ledger
already holds. The strict build stops before any output is placed. The diagnostic build
records each unexplained fact change and each lost window as an error diagnostic on its
obligation and completes with a nonpublishable database, so one family's defect no
longer hides the rest of the cycle.

Delivery is established by a final state or a declared representation window on the same
variable, variant and column. Another column, another variant and a search alias without
windows deliver nothing here. Each obligation also carries the exact delivery facts its
member-to-window segment claims — type/length texts and member correction attributions.
Every overlapping final state on the same coordinate, and every shared state behind an
alias window for that coordinate, must keep each claimed fact and contain every claimed
attribution as an exact provenance element. Each fact travels as tri-state: a value
claim the written state must equal, a negative claim the written state must leave
absent, or no claim, which is never compared. When an alias window delivers an
obligation's window, the shared states behind the alias must cover that overlap; a slice
with no written state behind it is refused. A conflicting representation fact clears the
claim to none. Genuine source gaps, negative availability, unknown scopes, and pooled
scopes without a carried range claim no delivery in the first place. A range-carrying
pooled scope claims its whole range as one obligation, discharged by its one marked
state. A variable — or one of its variants — that the dependency ledger withholds
outright, with source evidence, answers for its own claim through that entry, whichever
stage recorded it, and for nothing past that exact coordinate: an exact source-linked
blocker stays a curation blocker without covering a sibling. Anything else missing is an
engineering defect, not a curation question: the build names the source records and the
exact missing window. A strict build stops before any output is placed; a diagnostic
build records the miss as an error diagnostic and still completes with a nonpublishable
database. A missing optional field or an unrelated diagnostic is no permission to drop
the state it belongs to.

### Groups, relations and dependent output

Catalog dependencies distinguish a source-backed omission from a missing implementation.
Supported members survive; a group with fewer than two supported members is withheld.
Relations with unavailable endpoints are withheld with exact reasons. Invalid
declarations, cycles, duplicate edges and conflicting group ownership remain fatal even
if some endpoints would later be omitted.

Groups can attach variables or literal delivery columns and declare multiple facet axes.
Members must supply every declared facet. A variable cannot mix whole-variable and
representation membership in one group. Column membership is case-sensitive evidence;
the separate case-insensitive succession lookup does not alter it.

Existing code/label and checked split-sibling relationships share one component rule. A
code/label pair requires supported coding on its code endpoint, no coding on its label
endpoint, and shared variant coverage. Failed guards retain both variables but withhold
the pair. Explicit group membership takes precedence before components form. Month
groups use the finite accepted month vocabulary, minimum three distinct months and
shared-label guard; ambiguous collisions remain separate and diagnosed.

Classification succession combines explicit edges with the accepted guarded edition
rule. Only adjacent recognized editions with matching year-stripped names qualify.
Variable succession lifts adjacent classification editions only within an unambiguous
register/name and slug stream; variables spanning multiple editions do not qualify.
Explicit edges retain their provenance. The combined graph is validated before
materialization.

`event_sources` explicitly binds each native event namespace to its occurrence dataset.
Source succession events use already resolved native register, variant, variable or
member identities. Missing or ambiguous endpoints produce errors; leading zeros are not
guessed away. Reciprocal declarations coalesce. Conflicting descriptions remain unknown
while an agreed edge survives. Literal event metadata remains separately inspectable.

Source attribution matches register names and explicit abbreviations within a provider.
Ambiguous matches are errors; unmatched external labels stay text. State lineage
requires an accepted `same_as` path into that source register and exact endpoint period
intersections. Equal slugs are insufficient. Explicit source-variant defaults resolve
genuinely multiple variants and are checked even when unused. Missing evidence produces
retained lineage warnings and blocking diagnostics: no supported source state withholds
the edge as a warning; ambiguity is an error. Register attribution alone does not
establish a variable endpoint. Missing accepted endpoints retain that warning even for
year-independent deliveries. Only positively linked endpoints reach the
dated-intersection guard; independent endpoints then require an explicit
independent-edge contract and currently remain errors. No date hull is inferred from the
source register's other deliveries.

Panel keys require supported states in the exact variant. Losing one composite-key
member makes that entire key unknown; it never manufactures a shorter key. Other axes,
time grain and independently supported parent metadata survive. A reviewed support
occurrence excludes delivery states, not its independently supplied register, variant,
edition, population or object-type facts within already checked catalog parent topology.
Support-only lookup variants without admitted parent naming remain raw evidence and
support accounting; they do not create public variants or orphan edition children.
Parent resolution reconciles admitted facts with the same naming, language, coordinate
and conflict guards as catalog occurrences; `support_only_refs` continues to account for
the excluded deliveries. Superseded preliminary editions therefore retain their own
parent prose without recreating preliminary states.

## Persistent data warnings

The pipeline captures source diagnostics after acknowledgement settlement. Every
acknowledged limitation and a finite set of diagnostics affecting data interpretation
are persisted; editorial metadata projection notices are not. Diagnostic warnings carry
finite code-specific explanations and SHA-256 digests of the exact original diagnostic
text. The completed report ledger retains that full text and source facts remain intact,
without serializing entire value-set diagnostics into public warning payloads. Actual
corrected source references establish ownership. Ambiguous ownership falls back to the
actual resolved register, and an owner omitted from the final catalog cannot retain a
variable coordinate.

Reviewed identity maps, coding selections/extensions, and missing-column errata can
carry an explicit `data_warning` summary. Omitted annotations are silent. Only
applicable guarded decisions emit assumption warnings. Identity annotations retain the
annotated source members; coding annotations use their exact native column and authored
window; errata annotations follow the actual added occurrence, not its supporting native
anchor. The writer validates each complete warning payload and stores it in an indexed
`data_warning` table. No assumption is discovered by parsing evidence prose.

## Strict and diagnostic builds

Strict publication is the default. Every unhandled source discrepancy requiring a
decision is an actionable structured error until curation resolves or acknowledges it.
Optional unspecified metadata may remain unknown with a diagnostic; an unsupported
parser or missing implementation cannot be reclassified as curation. Unknown
sensitivity/identifier flags cannot be represented faithfully in the current Boolean DB
contract, so the variable and dependent output are withheld instead of substituting
false. Sensitivity follows a disclosure-control ratchet across declarations: any
sensitive or sometimes-sensitive claim makes `is_sensitive` true, even when another
range says false. It is false only when the supplied claims say false and none says
sometimes sensitive; without a claim it stays unknown except for source-specific
defaults. A checked sensitivity correction takes precedence over source claims. The
original records remain attached so a curator can audit every contributing declaration.

A curation `[[acknowledge]]` entry names one exact issue code, its subject, refs, fields
and period as the diagnostic carries them, plus a reason and evidence. Once its scope is
resolved, the one matching error is re-emitted as a warning that keeps its code and
names the acknowledging entry (`acknowledged_by`). The affected output stays withheld,
and output withheld through it inherits the warning. The build summary counts
acknowledgements per code. An entry that matches no error is stale, and one that matches
more than one is over-broad; both are errors. Strict publication accepts acknowledged
issues. A warning is either defined by a rule, such as `omitted_columnless_occurrence`,
or a counted acknowledgement. The compiled `AcknowledgeDecision` reaches issues raised
while its source scope resolves.

Diagnostic mode resolves exactly the same facts and issues. It scans the complete
prepared scope set and writes only independently supported output to a separate new
path. It never aborts on a per-variable or per-family inconsistency: it withholds the
affected output and reports it. It retains strict severity, source locators, fields,
periods, catalog identities, reasons and withheld output. Competing evidence stays in
the pinned prepared artifact. Every source occurrence has a disposition, including
duplicates, support-only rows and omitted output. Existing curation accounting is
separate: accounted does not imply applied or materialized.

A completed diagnostic artifact is marked incomplete and nonpublishable in its manifest.
Its CLI status is exit 10, distinct from publication readiness. Builder publication,
including steward extension, rejects it.

Of the checks that stop a strict build, a diagnostic build reports each one it can
attribute to a variable or family as an error diagnostic carrying the strict text:

- Per-column state windows. The per-column window checks in `validate_built_db` are
  distinct value sets, code-less against code-bearing, and pooled against explicit.
  Formation runs them on the variable's resolved states. A failing variable and its
  dependents are withheld.
- Delivery coverage. `check_delivery_coverage` reports one error per obligation and
  kind. The variable is still written.

Everything else stays fatal in both modes:

- an explicitly unconverted required surface;
- prepared scope, pin and curation contracts;
- cross-scope definition conflicts;
- ledger accounting;
- resolver invariants that hold by construction, and other programming errors;
- writer preflight;
- references that dependency resolution cannot explain by a withheld cause;
- metadata graph duplicates and cycles;
- the remaining global `validate_built_db` checks;
- manifest, output path and SQLite integrity failures.

The writer validates resolved references before writing, assigns deterministic storage
IDs, and writes each final row once. It preserves separately resolved parent metadata
even when variables are withheld. Search indexes derive from those rows. No
SQL-to-IR-to-SQL round trip or provider semantic pass follows materialization.

Structural validation is required for a diagnostic DB. Corpus volume guards remain
unchanged and are separately reported against incomplete output; their failures require
an explained omission audit. Strict builds require both before publication. A failed
build leaves the active catalog intact. Output, backup and summary paths cannot alias
selected inputs, including through hard links or symlinks. A summary-writing failure
reports any already completed artifact separately rather than implying publication never
happened.

`build-db --registers ...` builds a register subset, strict or diagnostic, for fast
verification. It forms only the named scopes and decodes only their records; shared
inputs (classifications, successions, support, identifier and event sources, metadata)
stay complete. Occurrence accounting compares against the selected scopes' prepared
counts. Strict mode still reports the selected registers' errors. A scoped build does
not police curation for unselected registers: a curation entry whose every register
reference lies in an unselected register, or in one no scope declares, is skipped
without any existence proof, emits nothing and is counted once per entry as
`skipped_curation`; such a tag is omitted entirely rather than written with an empty
member list. An entry with no register reference (classifications and source columns
only) is evaluated as in the complete build. An entry that touches the selected
registers never hides a reference error the complete build would report, since a typo
there would silently drop a real edge. It defers a reference only when the other end
provably exists but was not selected, known without forming it: the unselected scope
files' naming declares those registers, variants and variables (a naming-ambiguity name
is a declared variable whose ownership is unresolved, which the complete build withholds
with a cause), and the prepared store holds their observed register names and native
IDs. A representation or state is proven as far as its declared variable and variant;
the complete build checks its column and period. Such a reference (a `same_as` or
succession edge, a group or tag member, a code/label pair, a source event or a source
label) is withheld as one warning code, `deferred_out_of_slice_reference`;
`deferred_references` counts distinct references. A target no scope declares, including
an undeclared variable in an existing unselected register, stays the complete build's
error, and a source label matching no known register stays a literal label. Source
labels naming exactly one unselected register are likewise deferred. A source succession
event may be deferred when at least one endpoint is positively observed in an unselected
register and no endpoint has selected-source references or targets. Missing counterparts
remain explicitly unknown; deferral creates neither ownership nor an edge. Selected
endpoints, including withheld or ambiguous candidates and references without resolved
targets, retain hard errors. Events with no outside anchor and complete builds retain
missing-endpoint errors. A source event with every endpoint observed outside is skipped
as `skipped_curation` only when no endpoint has selected-source evidence. Native IDs
occurring in both scopes, and unselected occurrences with no supported target, need the
complete occurrence census and are resolved only by the full build. A scoped pass is
therefore necessary but not sufficient: it may pass where the complete build fails on
curation outside the slice, and the complete build stays the check on everything,
including every skipped entry and deferred reference. Literal crosswalks and derivations
from a known prepared occurrence source wholly outside the selection retain their
evidence and defer endpoint resolution with the same warning; selected or unknown
sources still report unbound relationships as errors.

Source-wide support and value-list diagnostics retain their complete raw events and
physical accounting. A scoped build routes an issue only through declared source joins
and positively observed, unambiguous register coordinates. Selected-register issues
remain errors and use the existing exact acknowledgment mechanism; proven unselected
register issues become explicit out-of-slice warnings. Unknown or ambiguous contexts
remain errors, and complete builds never use this slice deferral. Unbound list subjects
include the source revision, complete descriptor digest, literal lookup tokens and
physical association count, so an empty target reference cannot become a general future
acknowledgment. Explicit source-diagnostic acknowledgments require equality with the
positively observed delivery register; ordinary occurrence-reference ownership guards
remain unchanged. No support flags, coding domains or missing variable targets are
inferred.

The scoped output is create-only and marked incomplete and nonpublishable in both modes.
The summary records the register list, `publication_ready` false and `corpus_validation`
`not_applicable`; structural validation still runs.

`check-curation --registers ...` is local editing feedback for complete selected
register scopes. It shares prepared-input verification, full curation-tree validation,
support and source resolution with `build-db`, then stops after occurrence accounting,
late coding/classification checks and decision dumps. The source-linked report marks
catalog dependencies (including checks within selected registers), final delivery
coverage, SQLite structural validation and corpus validation as not run. It makes no
deferred-reference proof or final `skipped_curation` accounting and writes no database.
The full `build-db` remains the approval proof.

Publication uses staged output and atomic replacement, with the previous generation
retained as `.prev`. No source preparation, decision refresh, network fetch or LLM call
occurs during a build.

## Inspection and verification

Source inspection and building share readers and applicability rules. Source-target
impact lists exact records, fields, codes and periods. Catalog impact additionally
covers identity, coverage, coding and dependent navigation/search content. A source-only
preview must say so. A real input update requires both impacts and an explained full
semantic comparison.

`dbdiff.py` compares schema and unordered row multisets, preserving multiplicity. It
ignores only documented volatile metadata and FTS shadow contents; base content and
index schemas remain checked. It is read-only and usable independently:

```sh
python -m reg_meta_build.dbdiff OLD.db NEW.db
```

`semantic_diff.py` supports semantic comparison when storage IDs change. Full refactor
verification also maps foreign keys through catalog identities, compares exact cleaned
code membership and interval unions, and preserves real gaps. A raw count delta or
diagnostic completion is insufficient. Major discrepancy categories need representative
source-backed evidence distinguishing data decisions from implementation defects.
Investigate unexplained engineering changes; retain exact unresolved diagnostics for
later curation. The baseline is comparison evidence, never an authority that supplies
missing facts.

Tests cover source layout/normalization, curation invalidation, overlapping decisions,
optional unknowns, exact accounting, dependency withholding, deterministic repeated
builds, and failure without activation. Synthetic tests do not substitute for a full
maintained-input run. Performance evidence includes cold preparation, warm builds,
memory, accepted Git storage, raw archives, prepared stores, outputs, caches and
temporary peak. Tiny fixture measurements cannot establish full-run savings.

## Commands

```text
reg-meta-build prepare-input-bundle --input-dir DIR --scb-snapshot DIR ...
reg-meta-build verify-input-bundle --input-bundle DIR ...
reg-meta-build prepare-sources --input-bundle DIR --input-commit SHA --input-manifest-sha256 SHA256 --output-dir NEW_DIR
reg-meta-build build-db --prepared DIR --input-commit SHA --input-manifest-sha256 SHA256 --report-dir DIR [--curation-dir DIR]
reg-meta-build build-db --prepared DIR --input-commit SHA --input-manifest-sha256 SHA256 --report-dir DIR --diagnostic --diagnostic-db-path NEW.db
reg-meta-build --db NEW_DIR build-db --prepared DIR --input-commit SHA --input-manifest-sha256 SHA256 --report-dir DIR --registers SPEC[,SPEC...]
reg-meta-build check-curation --prepared DIR --input-commit SHA --input-manifest-sha256 SHA256 --registers SPEC[,SPEC...] --report-dir NEW_DIR
reg-meta-build inspect-source-records --input-bundle DIR ...
reg-meta-build extend-db --base-db DB --providers-dir DIR --steward NAME ...
reg-meta-build build-docs ...
```

`--db DIR` and other global output flags precede the subcommand. Per-command `--help`
describes paths and pins. A `--registers` SPEC names a prepared scope: an SCB register
by its register id (`258`), or an SOS workbook or thin-provider register by its register
name (`Patientregistret`, `aktivitetsstod`). A name or id naming scopes in more than one
source is refused; `SOURCE:ID` (`scb-registerinformation:258`) picks one. It also
combines with `--diagnostic --diagnostic-db-path NEW.db`. A strict subset needs an
explicit new `--db` directory: it never replaces the active catalog. Naming,
classification, group, succession, same-as, split-sibling and document-coverage worklist
commands produce review material; they do not approve or apply new curation during a
build.

## Steward extension

`extend-db` copies a released global database and adds steward-private providers and
their registers, variables and states. Facts about an existing global provider belong in
common global curation. Steward extension does not mutate the base or resolve global
source conflicts.

`sources/curated.py` is steward-only. Its `<provider>.toml` requires a `[provider]` name
and source label, explicit variable keys and nonempty state arrays. Repeated variable
keys can pool distinct variant deliveries only when their variable metadata agrees.
Known periods accept ISO year/month/day tokens; omitted boundaries remain open/unknown
under this authored extension contract. Co-delivered aliases have their own windows
within one state. Malformed or inverted bounds, repeated columns, duplicate state keys
and unknown fields fail early. The extension cannot introduce code sets or
classification linkage.

Steward IDs use explicit prefixed inputs to `id.mint`, including provider and native
keys. Register/variant slug pins come from the steward slug directory. Variable slugging
is incremental and preserves every existing global slug. The retained `ir/` models serve
this extension graph, not the global source-cleaning boundary.

The overlay rebuilds register/variable search indexes; it retains the copied value-code
index because no values were added. Structural validation and the steward holdings gate
run before atomic publication. The holdings gate checks single-period editions only;
pooled ranges/lists are explicitly unassessed and never infer annual column
availability. A missing delivery inventory fails configuration unless the explicit skip
flag is selected. Provider regeneration and input acceptance remain separate maintainer
operations.

SWECOV source routing lives in the tracked `source_policy.toml` beside its inventory
overlay. Category/detail routes select catalog variants or split selectors; flavor
entries carry explicit provider/register/variant metadata and any exact table selectors.
The generator validates this policy once and derives its runtime indexes. Repeated
selectors, conflicting metadata and unknown fields fail configuration. Physical
table/column overrides remain in `inventory_overlay.toml`: they have a different scope
from category routing. Both files are curation inputs, not generated outputs. The
generator neither invents routes nor changes their declarations.

Inventory generation retains accepted catalog placement bounds and resolves each finite
table edition through positively covering declared owners. Missing coverage or
simultaneous owners produces a worklist before the inventory can be replaced. Each
literal representation must cover the edition; sibling spellings cannot supply its
dates. Source-backed annual stock-file overrides may name the exact year-end snapshot in
the inventory while retaining the original catalog state windows.

An exact `[[mapping]]` may declare `select_owner = true` when the physical delivery
documents which source question or survey wave owns that column. This selects one
already-declared owner for that table, edition, variant and literal representation; it
does not extend dates or merge variables. The selected owner must independently cover
the complete edition. Without this explicit curation, simultaneous owners still fail
closed.

## Document database

`build-docs` indexes curated markdown and registered related-document binaries. It
parses frontmatter, cleans markdown for FTS, resolves source URLs/titles from
`doc_sources.toml`, and records document provenance. Unknown source mappings are
warnings rather than invented links. Related binaries are checked against tracked hashes
and sizes before being stored verbatim; license metadata determines whether
redistribution is permitted. Missing required local binaries and unregistered PDFs are
reported according to the document-build contract. This indexing does not interpret
documents to change catalog facts.

`doc_db.py` owns the build and FTS creation. Read schema constants and helpers remain in
`reg_meta`, so querying the document database does not import maintainer tooling.

### Literal documentary relationships

`documentary.binding` binds a supplied crosswalk or derivation row to its exact catalog
owner. The complete declaration, physical table peers and endpoint native families have
authored payload digests. Fresh source-native naming must agree with each owner and
operand reference. Missing, duplicate or changed source evidence withholds the
relationship. Outside-slice evidence retains its deferred disposition.

These are `owner_bound_literal` metadata. Their typed declarations preserve every
supplied clause, operand, period, delivered cell and locator. Explicit variable
references record literal source names; no expression evaluation, join, equivalence,
recoding date, classification-edition inference or availability extension follows.
Unresolved input namespaces and clause operands retain exact coordinates and warnings.
They do not become fully bound or executable merely because the owner exists. Dependency
resolution withholds a relation if its exact catalog endpoints are unsupported. The two
source relationship tables preserve the literal document and ordered catalog references
without adding state or coding edges.

The shared evidence primitives and crosswalk/derivation declarations live in
`reg_meta.source_evidence` and `reg_meta.documentary`. Build ingestion and catalog
reading use the same strict models and validators. Source occurrence, temporal and
reconciliation models remain build-only. This keeps the consumer independent of the
builder while retaining identical serialized source evidence.
