# Design: reg_meta_build

`reg_meta_build` prepares machine-readable register metadata, applies checked curation,
and builds the SQLite catalogs queried by `reg_meta`. It also builds the separate
`reg_meta_docs.db` document index. It is maintainer tooling, not a runtime dependency of
the query package.

The dependency direction is `reg_meta_build → reg_meta`. The query package owns the
public catalog/schema constants and read models. The builder owns input handling,
curation, materialization and validation. Cross-package constraints live in
[../ARCHITECTURE.md](../ARCHITECTURE.md).

The three-stage pipeline is the only `build-db` implementation. A diagnostic database is
incomplete and cannot be activated by builder publication. Input declaration loaders and
read-only worklists do not constitute an alternative build route.

## Responsibilities

The design serves five maintainer tasks:

1. Rebuild deterministically from exact accepted inputs and decisions.
2. Prepare and inspect an occasional source update before accepting it.
3. Resolve an exact discrepancy, or explicitly acknowledge a bounded unresolved one.
4. Explain affected identities, periods, coding and dependent catalog content.
5. Repair a source adapter when an actual delivered format changes.

There are three stages inside one builder:

  | Stage    | Responsibility                                                                                            | Boundary                                                                                                                     |
  | -------- | --------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
  | Cleaning | Decode actual provider formats into source-faithful observations and compact value dictionaries.          | No catalog FQIDs, identity merging, source winner, PDF interpretation or LLM calls.                                          |
  | Curation | Resolve identities, fields, availability, coding, groups and relations through one common implementation. | Exact source scope and checked dependencies; no per-register Python rules or decisions based on another correction's output. |
  | Build    | Assign storage IDs, write resolved rows, derive search indexes, validate and publish atomically.          | No independent semantic choices or post-write correction passes.                                                             |

The maintained call path is `pipeline.build_selected_catalog` →
`prepared_catalog.open_prepared_catalog_sources` → `source_scope.resolve_source_scope`
for each complete source/register scope → catalog dependency and lineage resolution →
`resolved_catalog.write_resolved_catalog`.

  | Module family                                                                           | Role                                                                      |
  | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
  | `sources/*_records.py`, `sources/*_values.py`, source reference readers                 | Actual-format cleaning.                                                   |
  | `source_records.py`, `source_values.py`, `source_reference_records.py`                  | Common source observations, values and literal reference declarations.    |
  | `input_snapshot.py`, `prepared_sources.py`, `prepared_values.py`, `prepared_catalog.py` | Capture, validation, compact storage and pinned prepared-input access.    |
  | `source_curation.py`, `source_effects.py`, `source_naming.py`                           | Applicability, original-evidence corrections and checked naming.          |
  | `source_intervals.py`, `source_coding.py`, `source_formation.py`                        | Exact periods, membership reconciliation and ordinary variable formation. |
  | `source_annotations.py`, `source_representations.py`, classification binding modules    | Checked aliases, parallel columns and classification decisions.           |
  | `catalog_dependencies.py`, `catalog_lineage.py`, `source_event_resolution.py`           | Supported catalog relationships and exact dependent omissions.            |
  | `resolved_catalog.py`, `resolved_metadata.py`, `db.py`                                  | Direct materialization, SQL schema, indexes and atomic publication.       |
  | `validate.py`, `semantic_diff.py`, `dbdiff.py`                                          | Structural/corpus verification and comparison.                            |
  | `extend_db.py`, `sources/curated.py`, `ir/`                                             | Separate steward extension over a released global catalog.                |
  | `doc_db.py`                                                                             | Document indexing; independent of source fact resolution.                 |

The selection contains data, never executable conversion hooks. Scope files load one
register at a time. The runtime does not import one-time conversion scripts or use the
legacy database as evidence.

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
- Exact supplied dates and recognized same-year ranges retain their endpoints. Pooled
  multi-year periods remain pooled. The edition label and a variable's declared
  measurement/reference period are separate observations.
- Value-set content may deduplicate identical code/label pairs for storage and
  comparison. Equal codes with different labels remain distinct. Deduplication does not
  infer list identity, membership, variable identity or authority.

SCB records retain native IDs and joined parent assertions. Population descriptions
attached to an edition do not assign its variable occurrence to a particular population.
Registerinformation, summary flags, identifiers, source-schema metadata and time-series
events remain separate source evidence. The source format can declare a precise support
join, such as native variable ID or the documented register/variant/variable/column key.
Common binding checks the complete target cardinality. Ambiguous joins supply no flags;
conflicting flags are not combined with Boolean OR.

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
stored cell value. Missing dates remain unknown. Cleaning does not inherit dates,
synthesize a subset, group variables by name, bind code lists or mint catalog
identities. Known workbook layouts have focused tests; unknown list shapes preserve
their original rows without fabricated members.

Authored thin-provider TOMLs use `sources/curated_records.py`. They are machine-readable
source declarations with publisher and transcription provenance. Reading them does not
apply parent defaults, invent false flags or make the old final-catalog adapter part of
preparation. Classification CSVs, authored code lists and the LISA workbook similarly
supply source evidence for common resolution.

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
5. Resolve new discrepancies through separately reviewed curation, or preserve exact
   bounded unresolved decisions where safe output is justified.
6. Accept the reviewed input/decision revision explicitly, then build and publish
   strictly.

A decision cannot authorize its own changed input. A repair that supplies a formerly
missing row also invalidates an omission claim covering that row.

## Common curation

`source_curation.py` evaluates cases against the original complete source scope. A case
names exact members, finite periods, fields and expected facts; supporting evidence and
peer guards bound its applicability. Peer guards check completeness but do not select
additional targets. Changed relevant facts, new intersecting evidence, lost support,
changed membership or changed cardinality make the case stale. Unrelated later editions
and layout changes remain applicable when their scoped projections are unchanged.

Cases can coordinate identity, occurrence, field, period, coding and representation
changes. They are not restricted to one atom per field. Equal assignments compose;
contradictory assignments withhold the disputed aspect. No case reads another
correction's output as its supporting source. Application order cannot choose a winner.

Ordinary variables form from an established native identity. Their names bind that exact
source coordinate; additional deliveries do not require handwritten whole-variable
cases. An accepted name alone cannot establish a partition across ambiguous column
spellings. A partition, rename or parallel-column family requires checked ownership and
complete relevant membership. Related but different variables remain connected through
groups; a shared stem or suffix is not evidence that they are one variable.

Audited naming ambiguities can connect an existing catalog name to an exact unresolved
source identity. These are error attributions, not new identities or waivers. They must
still match the original family and actual unresolved result. Missing conversion
mappings or stale attribution bridges are implementation failures.

An added delivery names an existing identity, variant, edition and supplied period with
checked evidence. It is a declaration, not a fabricated physical source row. Supplying
column presence does not copy type, flags or coding from another edition. Any donor
metadata or membership must be explicitly selected and guarded. Undated `all_versions`
holdings preserve unknown coverage; pooled editions cannot create annual states.

Copied coding pins the original bound donor evidence at the declared occurrence scope
with explicit `expected_codings`. Missing fingerprints are a contract error; changed
membership or validity makes the entire occurrence case stale before effects run.
Unknown and pooled scopes retain their original validity constraints in the fingerprint
without acquiring dates. Fingerprints are captured during offline conversion, never
generated or refreshed by a build. Checked value-list field corrections bind using the
effective declaration while retaining the original source record as evidence.

An explicitly open upper bound differs from an unknown period. Resolution can retain a
known start and explicit open end; the writer uses `9999-12-31` as the storage sentinel.
A literal source year 9999 is not dated evidence.

### Occurrences and coding

Occurrence reconciliation forms exact nonoverlapping column intervals. Conflicting
optional fields become unknown only over their overlap. Conflicting availability or
population withholds the unsafe segment. Missing periods/columns remain explicit issues.
A gap between supported periods stays a gap.

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

Conformance compares exact code strings. Noncanonical members preserve the original list
and declared binding as evidence, withhold the state classification link and emit an
error. There is no global sentinel waiver. Original coding issues remain visible.

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
retained lineage warnings and blocking diagnostics.

Panel keys require supported states in the exact variant. Losing one composite-key
member makes that entire key unknown; it never manufactures a shorter key. Other axes,
time grain and independently supported parent metadata survive.

## Strict and diagnostic builds

Strict publication is the default. Every unhandled source discrepancy requiring a
decision is an actionable structured error. Optional unspecified metadata may remain
unknown with a diagnostic; an unsupported parser or missing implementation cannot be
reclassified as curation. Unknown sensitivity/identifier flags cannot be represented
faithfully in the current Boolean DB contract, so the variable and dependent output are
withheld instead of substituting false.

Diagnostic mode resolves exactly the same facts and issues. It scans the complete
selection and writes only independently supported output to a separate new path. It
retains strict severity, source locators, fields, periods, catalog identities, reasons
and withheld output. Competing evidence stays in the pinned prepared artifact. Every
source occurrence has a disposition, including duplicates, support-only rows and omitted
output. Existing curation accounting is separate: accounted does not imply applied or
materialized.

A completed diagnostic artifact is marked incomplete and nonpublishable in its manifest.
Its CLI status is exit 10, distinct from publication readiness. Builder publication,
including steward extension, rejects it. Contract/pin failures, invalid references,
structural corruption and programming errors remain fatal in both modes. An explicitly
unconverted required surface is also fatal.

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
reg-meta-build build-db --selection FILE --report-dir DIR
reg-meta-build build-db --selection FILE --report-dir DIR --diagnostic --diagnostic-db-path NEW.db
reg-meta-build inspect-source-records --input-bundle DIR ...
reg-meta-build extend-db --base-db DB --providers-dir DIR --steward NAME ...
reg-meta-build build-docs ...
```

`--db DIR` and other global output flags precede the subcommand. Per-command `--help`
describes paths and pins. Naming, classification, group, succession, same-as,
split-sibling and document-coverage worklist commands produce review material; they do
not approve or apply new curation during a build.

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
