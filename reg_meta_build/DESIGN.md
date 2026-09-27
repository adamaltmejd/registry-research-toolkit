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

Transitional: builder code still names specific registers or variants in four places.
They are debt to remove, not precedent:

- `sources/scb.py` `_PROJECTION_REGISTERS`, which reads one forecast register's edition
  names as vintages;
- the LISA code-membership anchor in `validate.py`;
- the CIS 2014 register/variant selector in `cis2016_matrix.py`;
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
- SWECOV holdings establish possession by that project. Multi-year tables do not
  establish per-year column availability.
- SWECOV's MONA storage-schema export supplies type evidence for steward-held columns
  absent from SCB documentation. Each register selects its tables by literal prefixes;
  folded column names join waves. Integer widens to decimal, and any text wave widens
  the variable to text. Date-only waves yield date; date mixed with a numeric class
  leaves the type unresolved with the per-table storage types in the diagnostic. Storage
  metadata never establishes per-year availability or row content.
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
- Known SQL integer/text aliases normalize mechanically. The original declaration and
  width remain evidence. A text-to-integer change requires curation. SCB nonnegative
  integer length spellings normalize to decimal spelling; other length forms remain.
- Exact supplied dates and recognized same-year ranges retain their endpoints. A school
  year `Läsåret YYYY/YYYY+1` — with or without the `Läsåret` prefix — is one period 1
  July of the first year to 30 June of the second, as is a
  `Höstterminen YYYY - Vårterminen YYYY+1` term range; a `YYYY-MM - YYYY-MM` month range
  is one period from the first day of the first month to the last day of the second. A
  `Läsåren A/A+1 - C/C+1` school-year hull pools over 1 July of the first year to 30
  June of the last. A `Deklarationsår YYYY (beskattningsår ZZZZ)` with ZZZZ one below
  YYYY is the income year 1 January to 31 December. Other multi-year periods remain
  pooled. The edition label and a variable's declared measurement/reference period are
  separate observations. A pooled scope carries its whole-range bounds (Y-202): the
  exact interval's endpoints for a multi-year exact range, else the first claim's start
  through the last claim's end — but only when every claim is a whole calendar year.
  Other term edges (Komvux HT/VT) and forecast-register ranges carry no range. A pooled
  scope without both bounds stays unresolvable downstream — the range is carried
  evidence, never re-parsed from the label.
- Value-set content may deduplicate identical code/label pairs for storage and
  comparison. Equal codes with different labels remain distinct. Deduplication does not
  infer list identity, membership, variable identity or authority.

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
validity dates are not truncated to years or silently repaired.

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
without fabricated members.

Authored thin-provider TOMLs use `sources/curated_records.py`. They are machine-readable
source declarations with publisher and transcription provenance. Reading them does not
apply parent defaults, invent false flags or make the old final-catalog adapter part of
preparation. An omitted thin-provider flag is a deliberate false claim (Y-169). SOS
workbook variables carry the same explicit-flag contract from the delivered template
(Y-193): the Kopplingsvariabel cell is the identifier claim — a non-blank linkage marker
reads as explicit true, a delivered blank as explicit false — and every workbook
variable is explicitly sensitive, because all SOS deliveries are health and
social-services microdata. Classification CSVs, authored code lists and the LISA
workbook similarly supply source evidence for common resolution. The SOS register name
is the DCAT-AP Titel; the general sheet's Datamängd cell is kept as `dataset_label`
evidence and never competes with the name (their spellings disagree for LSS, HSL and
SOL). A workbook without a DCAT sheet keeps Datamängd as its name. When a subset token
occurs more than once, its semantic key and native variant coordinate include the row's
Deldatamängdsetikett as well as Deldatamängdsnamn. Unique tokens retain their existing
keys. An authored SOS route names the chosen labeled row exactly; renaming or removing
it makes the route stale. LOVA's `A_LOVA` routes to its Huvudtabell row and
`A_LOVA_LISA` to its ekonomi/arbetsmarknad row. The prepared source-records schema is
12; older stores must be re-prepared.

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
variant and literal SCB column/version coordinates. `[[errata.version]]` carries
`variant`, `name`, `evidence`, and `noted`; it adds an undocumented register edition and
requires a year-bearing SCB version name. Version era order follows the maximum year the
edition claims, so adding a historical edition cannot replace a newer documented
edition's latest spelling or type. `[[errata.edition_period]]` names one existing native
edition by variant and SCB's literal edition name. For a name the period parser cannot
interpret, its documented `valid_from` and `valid_to` are the exact inclusive occurrence
interval; `evidence` and `noted` record the source of that claim. A missing or renamed
edition, or a name the parser can interpret, makes the entry stale. No register coverage
or nearby edition supplies an inferred period, and other unparseable names remain
unsupported. `[[errata.delivered]]` carries `variant`, literal `column`, existing
`versions`, `evidence`, `noted`, and optional `upstream` and `native_variable_id`; it
adds omitted rows for a column documented elsewhere on the variant and names edition
tokens verbatim. The native-variable anchor selects one documented identity when the
same column literal belongs to multiple variables in the variant's history. An anchor
must occur under that column; without one, the column must identify exactly one variable
across the complete variant history. `[[errata.column]]` carries the variable identity
(`name` and `definition`), a source and evidence, plus either named versions, bounded
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

Ordinary variables form from an established native identity. Their names bind that exact
source coordinate; additional deliveries do not require handwritten whole-variable
cases. Column spellings that differ only by case or diacritics fold to one column by the
shared column-identity key and need no partition or alias decision. Two spellings
delivered side by side in one edition of one variant are two columns, so the fold
applies only when no single edition co-delivers two spellings of one fold key. An
accepted name alone cannot establish a partition across ambiguous column spellings. A
partition, rename or parallel-column family requires checked ownership and complete
relevant membership. Related but different variables remain connected through groups; a
shared stem or suffix is not evidence that they are one variable.

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
column presence does not copy type, flags or coding from another edition. Any donor
metadata or membership must be explicitly selected and guarded. Undated `all_versions`
holdings preserve unknown coverage; pooled editions cannot create annual states.

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
warning for this cause alone. An undelivered cell, or a delivered cell that is not
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
conflict diagnostic; the underlying occurrences and provenance remain inspectable. Other
conflicting fields still raise occurrence diagnostics.

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
and declared binding as evidence, withhold the state classification link and emit an
error. There is no global sentinel waiver. A classification may instead curate its own
exact-string sentinel list (`sentinel_codes = [{code, meaning}]` per `[classification]`
in `curation/classifications/<short>.toml`) for bulk/missing tokens the source emits for
uncoded members. An observed code on that list keeps the state binding (`kept`), stays a
variable-local member of the state's value set — never a `classification_code` row — and
is reported once per state as a warning naming the code and its curated meaning. Codes
match exactly (`"00000"` never equals `"0"`); there are no patterns and no
cross-classification lists, and unknown keys or duplicate codes fail the load fast. Any
other noncanonical code still severs the binding with the existing error, which lists
only the non-sentinel codes. A sentinel must not be a canonical code; the load refuses
the overlap. The sentinel list is conformance curation, not codebook content, so
curating a sentinel never stales a binding. Original coding issues remain visible.

A parallel-column decision names each literal column and its finite delivery window. It
reconciles sibling metadata and coding before forming shared states. Conflicting facts
stay unknown; missing members or contradictory representation choices withhold the
unsafe state. Monthly alias windows do not split annual metadata into monthly states.
The stored representative column is chosen deterministically from participating columns
after reconciliation; it never selects a metadata donor.

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
the edge as a warning; ambiguity is an error.

Panel keys require supported states in the exact variant. Losing one composite-key
member makes that entire key unknown; it never manufactures a shorter key. Other axes,
time grain and independently supported parent metadata survive.

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

A curation `[[acknowledge]]` entry names one exact issue code, its target coordinates
(the subject and refs the diagnostic carries), a reason and evidence. Once its scope is
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
error, and a source label matching no known register stays a literal label.
Succession-event endpoints observed only in unselected registers, and source labels
naming exactly one unselected register, are likewise deferred, and a source event with
every endpoint there is skipped as `skipped_curation`; native IDs occurring in both
scopes, and unselected occurrences with no supported target, need the complete
occurrence census and are resolved only by the full build. A scoped pass is therefore
necessary but not sufficient: it may pass where the complete build fails on curation
outside the slice, and the complete build stays the check on everything, including every
skipped entry and deferred reference. Shared-input diagnostics are still reported
corpus-wide. The scoped output is create-only and marked incomplete and nonpublishable
in both modes. The summary records the register list, `publication_ready` false and
`corpus_validation` `not_applicable`; structural validation still runs.

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
