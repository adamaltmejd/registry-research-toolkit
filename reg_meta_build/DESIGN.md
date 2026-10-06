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

Builder code still names specific registers or variants in three places. These are
bounded exceptions to remove, not precedent for new source interpretation:

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

An authored thin-provider variant may declare `period_scope = "pooled"` with two
explicit, ordered ISO date bounds. The reader retains this declaration and the
maintained-declaration compiler emits bounded pooled occurrences, including when a
variable narrows that range. It never expands the table's observed span into annual
column availability. Register inception may remain unknown when a named delivery
supplies the bounds. An optional nonblank variable `data_warning` stays in the original
cells and becomes a warning scoped to its exact variable, variant, column and period
through the existing checked-occurrence warning contract. These declarations distinguish
maintainer transcription and sensitivity policy from verified provider meanings.

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
lookups. The decoded-record cache keeps at most 8,192 recent originals, including during
compilation; callers still receive complete ordered batches and retain their own
records. Eviction reconstructs the same pinned originals and does not change guards,
source fingerprints or output. Phase-end clearing additionally releases the remaining
cache. Register batches use an exact temporary ordinal relation for uncached originals
and seek the existing locator/cell primary keys. This avoids rescanning the source for
each register, preserves original ordinal/position order and multiplicity, and writes
nothing to accepted prepared files. Native-family reads keep their existing indexed
path; no persistent cache or prepared-format change is required.

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
delivery lines separate from their annual parent variants. The same checked split can
preserve a source-labelled range separately from annual editions, with a scoped
`data_warning` when aggregation or physical delivery semantics are unverified; it never
assigns annual availability to the range.

An SCB `[[identity.partition]]` may use `expected_evidence_sha256` to pin the entire
reviewed native family, including blank columns, every source field, parent fact, coding
reference and physical multiplicity. Its native key selects the complete family; a
second projection selector is unnecessary. Fresh compilation and replay refuse changed,
added or missing originals. A finite map can retain the native owner and public key
without introducing an artificial split.

An SCB `[[identity.column_owner]]` binds a literal column to an accepted owner within
explicit source editions. Optional `expected_fields` pin reviewed source facts using the
existing field-expectation contract. An entry may instead pin the complete selected
originals with `expected_evidence_sha256`, optionally retaining exact `expected_records`
projections. Digest-only entries require finite editions and no field guards; fresh
compilation hydrates full originals and replay checks every original fact. This admits
multiple original prose claims for one exact physical column and edition under one owner
without selecting or correcting either claim. The two guard forms are exclusive. Guarded
entries require a finite edition list. Complete-record guards retain all fields, parent
facts, coding references and original multiplicity; missing, added or changed originals
refuse both compilation and replay. Field guards require unique known fields, and the
compiled complete family also guards every supplied field during replay. This supports a
reused column whose explicitly documented amount or rate role changes between editions
without normalizing units or borrowing another owner's meaning. Physical source
multiplicity remains governed by the prepared-source pins and ordered inventory. A
complete native default column map may retain that owner alongside exact guarded scoped
overrides; only matched source members take a split owner, and all declared owners
remain covered.

An SCB `[[identity.unassigned]]` withholds an entire native family's catalog ownership
when none of its physical fields has a supported owner. It requires the complete
original-family `expected_evidence_sha256` and an evidence reference, and cannot coexist
with partition ownership or split naming. Original records and coding are still
evaluated and retained. It does not acknowledge any error: the resulting source identity
diagnostic requires a separate exact acknowledgement. Changed, added or missing
originals invalidate the declaration. Outside-slice naming also excludes this family.

Finite `[[representation.parallel]]` declarations may use `case_aliases = true` for
case-equivalent column spellings under one source-native identity with equal nonblank
names and definitions. This requires per-column metadata and makes no co-delivery claim;
source members may export either spelling. Literal metadata, source coding, windows and
replay membership remain guarded. Other literal changes need their own identity
evidence.

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

Compiled scopes and globals are strict Pydantic models built directly from the compiled
declarations; they are never serialized and re-read in memory, so the guard graph each
case retains is the compiled one, not a duplicate. Decision dumps serialize the globals
only when requested. After a scope model is built, the raw compiled mappings drop their
references to it. Coding guards still check the complete register before variable
formation; afterward, raw coding claims are consumed per variable rather than retaining
every processed column through scope end.

Field expectations reuse the source model's single-field type guard instead of
constructing every absent field again. A singleton record alternative needs no
uniqueness comparison; its nested projection still receives full validation. Multiple
alternatives retain shape and canonical-token checks. An added delivery retains its own
literal donor and negative native-base records as quantity evidence. Complete edition or
variant support still guards the correction, but unrelated variables cannot become
contributors to that quantity's metadata or coding authority.

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
one list label has multiple meanings. `coding.choice.keep_members_sha256` can select the
same complete source domain by the canonical hash of its sorted code-label pairs,
instead of repeating a large list; it is exclusive with `keep_members`. Neither form
creates codes, chooses the largest list or supplies missing coverage.

Checked choices and extensions may pin `expected_evidence_sha256` instead of verbose
source-row authority. This digest covers every full original in the selected literal's
complete history and all bound raw coding assertions, including out-of-window evidence
and multiplicity. It is exclusive with `source_authority`. Compilation still captures
full field, parent, coding and peer guards for runtime application. Source review and
the decision's reason explain the meaning; the digest only guards that evidence. Raw
coding fingerprints are shared within one application call, never cached across inputs.
Physical coding fingerprints serialize the supplied dataclass/model graph directly
through the installed JSON adapter before canonical hashing. This retains every ordered
assertion and raw validity value without a deep copy or an encode/parse roundtrip;
serialization warnings remain errors.

`coding.uncoded` may explicitly declare `stored_role = "label"` or `"free_text"` when
reviewed source metadata identifies a stored text component whose attached numeric books
describe a related code field. This requires the same complete original and own coding
digest or prepared-row authority, plus a data warning. The effective text field has no
inferred finite response domain; every attached claim and declared classification
remains source evidence. Ordinary uncoded entries still reject a complete nonempty list.

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
`description`, `definition`, `representation` or `measurement_unit` at exact original
variable/variant/column coordinates. Unit corrections additionally pin the supplied unit
and both coverage fields. They reconcile literal wording only when the source definition
positively establishes the same quantity; they do not convert values, merge differing
scales or infer absent units. A `source_attribution` correction requires an exact
edition, all supplied fields and complete original-record expectations, including
parents and coding references. It can normalize an abbreviation explicitly defined in
the source to the existing variant name; it does not introduce inferred aliases. The
catalog stores the interpreted attribution while immutable source evidence retains the
original text. `[[errata.occurrence_period]]` entries require one exact native source
edition for SCB. SOS rows lack that coordinate and instead require both literal supplied
coverage fields alongside the exact native variable, subset and column. Both entry types
guard the original edition text, both original scopes and all four supplied prose
fields, including absent or unknown values. Complete native-family support and peer
guards retain unrelated editions and coding references. Period entries carry explicit
replacement `TemporalScope` values; omitting an interval's `end` in TOML means an open
end, while unknown and pooled scopes retain their labels and bounds. These entries emit
existing checked field/period effects and never rewrite the original source records. A
finite scoped name correction selects exact original source scopes and carries those
same scopes into its conditional replay effect. A monthly row sharing an annual row's
semantic reference cannot inherit that name correction.

Field entries may pin duplicate observations with `expected_records`, using complete
original `RecordExpectation` alternatives. Except for the source-defined attribution
normalization above, their replacement must already occur in a guarded source
alternative. Exact fields, subjects, scopes, parents and coding references remain
checked at compilation and replay; conditional field effects change only the matching
original interpretation. This permits punctuation and reference-prose aliases without
discarding either physical original or inventing a replacement value.

A name correction may instead cite `authority_records` from the same native family. The
replacement must occur literally in those complete remaining source peers. Full target
and authority facts, parents and coding references, plus a combined raw-evidence
fingerprint, are checked at compilation and application. The fingerprint retains
physical multiplicity; removing an otherwise identical row invalidates the case. Guarded
SOS identity splits use the same complete-family boundary and may emit an exact
source-role warning while retaining each original quantity assertion and response list.

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

The same finite authority can certify an already complete own source domain when code
labels establish a reviewed role. Exact raw and copied fingerprints, complete original
rows, unchanged code/label membership and existing coverage are required; this form
returns the original domain without replacement or extension.

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
not authorize future extrapolation or relax finite bounds for other coding decisions. An
opt-in `source_authority.period_block` identifies the physical first row of a positively
dated workbook code block. Complete original and raw coding guards cover the whole
claim. Each member must have exactly one physical association on the same sheet;
subsequent blank period cells belong to that block until the next positive period
anchor. The selected code-label pairs must match the complete block exactly. Its
applicability intersects that positive period with the already known delivery scope.
Compilation and application share this check; missing, new or changed rows, anchors,
periods or associations invalidate the decision. This does not infer blank variable
availability or expand documentary range and blank tokens into stored codes. An optional
explicit `data_warning` records an unverified storage interpretation while retaining
those exact source tokens and all original coding evidence. Exact code strings,
including an explicitly documented empty string, are preserved. The compiler captures
original fields, scopes, coding references and complete column peers; changed finite
claims invalidate application, and any supplied complete list in the window makes an
ordinary documented entry stale. The guarded witness label certificate additionally
requires every positive target domain to match after exact reviewed label normalization.
Contradictory documented assignments withhold only their overlap. Original claims and
nonmembership associations stay in source accounting. Raw type-marker whitespace is not
a finite-membership guard: those associations cannot establish a list and are retained
in the documentary audit.

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

An explicit decimal comma-assignment certificate can supply a complete source cell
rejected by conservative generic parsing. It permits only the guarded literal decimal
code-label pairs and requires no existing source coding claims. Changed source text,
added codes or newly bound source lists invalidate it. The generic parser and original
unresolved descriptor remain unchanged; any accepted descriptor limitation is
acknowledged separately with its full-original fingerprint. Its `assignment_separators`
may explicitly include comma and semicolon; this changes only the reviewed certificate,
not generic SOS parsing. Own-source enumeration may use the existing exact
`source_scope` mode, including a source-explicit open end. Source and application scopes
must remain identical; authored finite historical windows cannot use a fabricated
year-9999 bound.

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
changed or out-of-window certificates cannot broaden conformance. State conformance
stores the exact scoped certificates alongside each affected member, just as alias
conformance retains them. Extension members distinguish substantive nonstandard codes
from known sentinels; global sentinel meanings are retained where declared. These local
facts add no canonical codes or global sentinel policy.

A state retains every positively named classification book as an independent
association, with its declaration provenance and, when an effective response domain
exists, a complete conformance check against that book. Simultaneous annual book claims
do not choose a winning edition or assert equivalence between books. Each book's
verified overlap and source-local extensions are stored separately. Coding still
requires exact independent source-domain agreement: competing codes or labels remain
unavailable with their original errors, rather than becoming a union. The public
associations may therefore be known even when their conformance is unavailable.

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

Same-edition co-delivery remains refused by default. A reviewed `co_delivered = true`
entry requires per-column metadata, one positive stable native question name and
definition, agreeing declared identifier roles, and complete same-member witnesses for
every literal column. Existing exact source-window, field, coding-reference and family
membership guards still apply. Each annual source edition needs its own exact window; a
synthetic continuous window is not inferred. This admits separate physical forms without
folding their names, converting their types or asserting equal values.

When an existing checked omission correction supplies the effective column or edition
scope, parallel compilation verifies that correction first and includes its complete
source authority. It checks the exact corrected intersection while retaining the raw
original fields and outer windows. A stale correction cannot supply a representation.
Checked owner or variant changes also select the effective projection, so sibling
originals stay fully guarded without being treated as representations of the selected
owner.

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
Windows intersect the existing coding boundaries and cannot bridge a gap. Same-literal
source editions may have nested bounds only when their exact union covers the declared
physical window without gaps or widening and their overlapping metadata agrees under the
existing source reconciliation rules. Their original scopes remain guarded. Each window
stores its value-set reference, literal native version label and independently checked
classification links. `alias_window_classification` attaches each declared book and
nullable conformance JSON to the exact composite physical-window key. Conformance checks
the entire local domain and its overlap with that book, including nonstandard codes and
scoped sentinels. The shared state carries no codes or books, and no window inherits a
sibling's associations. Shared mode carries no override. No codes are unioned, dropped
or recoded, and no version names are invented.

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
intersections. Equal slugs are insufficient. A literal `register : variant` attribution
selects only that exact admitted variant, matching its resolved source name. An unknown
or ambiguous explicit variant cannot fall back to a register default or another
variant's states. Explicit source-variant defaults resolve unqualified claims with
genuinely multiple variants and are checked even when unused. Missing evidence produces
retained lineage warnings and blocking diagnostics: no supported source state withholds
the edge as a warning; ambiguity between identity-linked source variants is an error.
Without an accepted variable identity path, even an ambiguous source-variant label
retains a no-source-state warning and cannot create an edge. Register attribution alone
does not establish a variable endpoint. Missing accepted endpoints retain that warning
even for year-independent deliveries. Only positively linked endpoints reach the
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
finite code-specific explanations (or the explicit reviewed acknowledgement reason) and
SHA-256 digests of the exact original diagnostic text. The completed report ledger
retains that full text and source facts remain intact, without serializing entire
value-set diagnostics into public warning payloads. Actual corrected source references
establish ownership. Ambiguous ownership falls back to the actual resolved register, and
an owner omitted from the final catalog cannot retain a variable coordinate.

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
more than one distinct complete diagnostic is over-broad; both are errors. Identical
ledger copies remain counted and persisted. An optional `expected_diagnostic_sha256`
pins the complete original diagnostic when otherwise identical coordinates carry
different details or withheld outputs. Strict publication accepts acknowledged issues. A
warning comes from a rule such as `omitted_columnless_occurrence`, a checked explicit
annotation, or a counted acknowledgement. The compiled `AcknowledgeDecision` reaches
issues raised while its source scope resolves. An optional `expected_evidence_sha256`
pins the full original records named by its refs and their bound physical coding
assertions. The canonical fingerprint ignores content ordering but retains multiplicity,
source fields, parent facts, physical delivered cells, coding references, and raw coding
associations and validity evidence. A changed fingerprint leaves the original error
intact and adds a stale acknowledgement error. Unguarded entries retain exact issue
matching.

A `[[coding.warning]]` declaration records a reviewed source metadata conflict while
retaining its type and response domain unchanged. It uses the coding compiler's exact
column, finite window, full source-evidence digest and original coding expectations.
Changed evidence is an error; a warning is emitted only for applicable guarded cases.
This is separate from an acknowledgement: it makes a supported contradiction visible
without asserting a repair or changing which facts are available. Its consumer warning
code is `source_metadata_conflict`, with exact affected fields, source references and
period. Review prose explains the decision; the checked TOML is executable authority.

Source-event endpoint acknowledgements use the same register-local declarations and
exact matcher at the global event boundary, after all selected scopes are observed. Both
evidence and diagnostic fingerprints are mandatory there. The evidence retains complete
event declarations and every physical endpoint occurrence, including duplicates; the
observed endpoint owner must match the declaring register. Missing successors remain
unavailable, and source events remain documentary evidence without fabricated entities.

Exact lineage-ambiguity acknowledgements settle at the same guarded global boundary.
Their fingerprints pin the consumer originals, complete identity-linked source
variables, every variant matching each literal source label, and connected identity
declarations. Different source origins or labels on one consumer retain separate
candidate contexts. Attribution text and ambiguous lineage rows survive; no source
variant is selected. Positively unselected sources defer these acknowledgements only in
scoped builds; full builds still refuse missing or changed evidence. Settled global
warnings use the same catalog warning projection and writer as scoped limitations.

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

**Compiled-holdings contract (2026-10-04).** `extend-db` copies an exact validated
global database built under schema 9, adds steward-private providers and their
registers, variables and states, then compiles accepted physical holdings into the same
SQLite artifact. A base built under an older schema is rebuilt, never migrated. Rebuild
the schema-9 public base once and reuse it for steward compilation; do not rerun public
preparation for each compiler edit. Facts about an existing global provider belong in
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
keys. Steward slug pins remain tracked in `fqid_slugs/<steward>/`, under the existing
snapshot and freeze checks. Publishable builds read those pins from the clean builder
checkout recorded by `builder_commit` and require each consumed slug file to match its
committed bytes. Reject `--slug-dir` overrides, `--skip-slugs` and untracked or ignored
slug supplements. Private candidate slug copies may remain immutable acceptance
evidence, but never supply build naming. Variable slugging is incremental and preserves
every existing global slug. The retained `ir/` models serve this extension graph, not
the global source-cleaning boundary.

The extension rebuilds register/variable search indexes; it retains the copied
value-code index because no values were added. Structural, mapping and accounting gates
run before atomic publication of one steward artifact. Strict steward publication
requires accepted provider overlays, inventory and policy plus committed steward slug
pins; no skip flag bypasses compilation or accounting. Provider regeneration and input
acceptance remain separate maintainer operations. Diagnostic output remains explicitly
nonpublishable and cannot replace the active artifact.

Compile the four relations specified in `reg_meta/DESIGN.md` → "Compiled holdings
relations and read scope". Reuse inventory models, edition/interval primitives, catalog
IDs and the common atomic writer. Preserve exact physical identifiers, authored edition
shapes, partitions, unmapped columns/reasons and pinned input evidence locators. Include
retained unknown-scope census tables with their authored reason, no physical periods and
no logical mappings. Year-independent scope is explicit, not a fabricated interval. No
segment relation, findings relation, excluded rows or semantic state resolution.

Require every mapping's representation. Resolve authored variable/variant coordinates to
existing IDs, validate register ownership, then fold the literal with the shared Python
`str.lower()`/`py_lower` rule. The folded literal must name exactly one delivery column
in the universe `Catalog._expand_state_windows` emits for that variable/variant over the
whole history (states plus participating alias windows); no match fails. Store the
literal and the representative spelling `representative_columns(states, windows)` gives
that column (ratified 2026-10-04; the states-only draft was stricter than the resolver
and would have rejected legitimate alias-only columns). Do not apply NFC or Unicode
casefold. Rejected accepted mappings require separate review, never silent compiler
fallback. After canonicalization, pass canonical spellings to the shared pure placement
validator extracted from `DeliveryInventory._check_one_to_one_resolution` for temporal
ambiguity per cell per partition, including labelled/unlabelled conflicts. Reject
duplicate canonical triples within a physical column with actionable input locators
before insertion; SQL UNIQUE remains the final guard. Do not infer aliases'
applicability or clip physical holdings to state windows during compilation.

Compilation does not retain the former Y-115 build assertion that a held single-year
column has a catalog window in that same year. A physical holding is preserved even when
semantic applicability is absent for its edition; the whole-history fold gate
establishes its binding identity only. Query-time resolution and ordering must still
require applicable column windows and reject uncovered requests. The compiler reads the
delivery universe through the reader's public `Catalog.delivery_columns`; there is no
second expansion.

The existing coverage assessment keeps range/list editions "temporally unassessed";
reuse `data_warning` for that disposition. Single-period assessment's flat union is
source accounting, not `Catalog` semantic resolution. The latter remains query-time,
including per-column storage/coding intersections, shared-window containment,
replacement participation/fallback and additive curated windows. The compiled
`holding_table` evidence is the sole authority for unknown scopes; the former
`unknown_holding_edition` / `unknown_source_validity` holding warnings are retired.

The accounting gate proves the disjoint union dated ∪ year-independent ∪
retained-unknown ∪ excluded ∪ lookup equals the raw table/column census from accepted
inventory, policy, overlay and CSV. A missing, duplicate or contradictory disposition
fails publication. Runtime relations contain only dated, year-independent and
retained-unknown facts; exclusions and lookups remain accepted build evidence. Store
counts and the accounting projection digest in the manifest. Strict gates and existing
disclosure confinement remain mandatory; no raw individual-level content enters
artifacts.

### Artifact manifest and deterministic generation

Extend existing `import_manifest` key/value rows, not a new identity table. Preserve
`schema_version`, `prepared_commit`, `prepared_manifest_sha256`, `curation_tree_sha256`,
`catalog_publishable`, `catalog_completeness`, classification succession metadata and
other existing keys. `import_date` remains optional display/provenance data, never a
cursor or order generation discriminator. Publishable `catalog_artifact_kind` is
`catalog` or `steward`; existing `diagnostic` output remains nonpublishable and rejected
by runtime readers. Add `builder_commit` and `generation_id` to publishable artifacts.

A steward also requires `steward`, `base_db_sha256`, `base_generation_id`,
`holdings_input_commit`, `holdings_manifest_sha256`, `holdings_policy_sha256`,
`holdings_accounting_counts` and `holdings_accounting_sha256`. These keys are absent on
a global catalog, whose holdings relations are empty. `base_db_sha256` hashes the exact
new-schema base file before extension and remains byte-level provenance only.
`base_generation_id` records that validated catalog artifact's semantic generation. The
compiler requires both; the file digest does not enter `generation_id`. Holdings pins
identify the accepted input Git commit and manifest; no private payload is embedded in
identity rows.

Scoped and diagnostic artifacts omit active `generation_id` and `builder_commit`;
diagnostic extension preserves the copied base identity only as `base_generation_id` and
`base_db_sha256`. These incomplete artifacts never share an active generation with a
publishable catalog. Publishable builds require clean tracked builder sources in a
source checkout. Installed wheels are not a source-revision authority and fail with an
actionable checkout requirement. Capture the revision before compilation and verify it
again before placement; uncommitted code or a changed revision cannot claim that pin.
Clean means no tracked changes, with the loaded builder sources equal to their HEAD
blobs. Unrelated untracked local configuration, reports and generated output do not
change that source identity. Accepted input repositories still require a fully clean
checkout, including untracked files; committed steward slug directories require their
exact committed file set and bytes. Recheck tracked source cleanliness and revision
before placement. Existing input/output separation and atomic placement rules apply.

The accepted candidate's manifest covers every consumed provider TOML, inventory,
policy, source document and review-evidence payload. Provider and policy directories
must come from that clean pinned candidate. External provider/policy overrides and
tracked policy defaults cannot supplement a publishable build. Changed provider bytes
require fresh acceptance and are covered by `holdings_manifest_sha256`. Verification
materializes the exact committed manifest members into a temporary build-owned snapshot;
all provider, inventory, policy and census reads consume that snapshot. Later edits to
the accepted working tree cannot change the consumed bytes. Steward slug identity is
covered separately by `builder_commit`; changing tracked slug pins requires public
review and the existing slug checks, not fresh acceptance merely to copy them privately.
Census accounting runs on the verified snapshot before copying the base DB. Compilation
reuses that accounting result; direct compiler calls perform the same gate themselves.
Diagnostic library extensions may supply metadata warnings and a publication test hook;
strict extensions reject these unpinned controls. CLI provider and inventory paths are
derived only from the accepted candidate. All accepted mapping coordinates must still
resolve under the selected committed pins; incompatibility fails the build rather than
falling back to a private slug copy.

Define `holdings_policy_sha256` as SHA-256 of a sorted relative-policy-path → SHA-256
object covering `source_policy.toml`, `inventory_overlay.toml` and
`holdings_policy.toml` selected from the accepted candidate. Never substitute tracked
defaults. `holdings_accounting_counts` is canonical JSON giving table/physical-column
counts for `raw`, `dated`, `year_independent`, `retained_unknown`, `excluded`, `lookup`.
The accounting digest hashes an array of objects with exactly `table` (exact physical
identifier), `column` (null for the table-grain record, literal name otherwise),
`disposition` (`dated`, `year_independent`, `retained_unknown`, `excluded`, `lookup`)
and `evidence` (sorted relative pinned-input/record locators). Every raw table has
exactly one table-grain record and every raw column exactly one column-grain record with
its table's disposition; unmapping does not exclude a held column. Sort records by exact
table in binary order, null column before named columns in binary order, then
disposition; sort evidence strings in binary order. Only the digest enters the manifest,
not private identifiers. Authored retained-unknown register and census-row digest remain
in the pinned policy evidence located by `source_ref`; they are not new logical
mappings.

Canonical digest encoding is UTF-8 JSON with sorted object keys, compact separators,
`ensure_ascii=false`, no floats and no trailing newline. `generation_id` hashes an
object containing `schema_version`, `builder_commit`, `catalog_artifact_kind`,
`prepared_commit`, `prepared_manifest_sha256`, `curation_tree_sha256`; steward adds
`steward`, `base_generation_id`, `holdings_input_commit`, `holdings_manifest_sha256`,
`holdings_policy_sha256` and `holdings_accounting_sha256`. Exclude `base_db_sha256` and
the count summary (covered by accounting digest). Never include `generation_id` itself,
finished output bytes, `import_date`, absolute paths, logs or volatile timings. Publish
final artifact SHA-256 separately, outside that database. Replays use the exact semantic
pins; do not promise unmeasured build durations.

SWECOV source routing lives in the authored `source_policy.toml` beside its inventory
overlay. Category/detail routes select catalog variants or split selectors; flavor
entries carry explicit provider/register/variant metadata and any exact table selectors.
The generator validates this policy once and derives its runtime indexes. Repeated
selectors, conflicting metadata and unknown fields fail configuration. Physical
table/column overrides remain in `inventory_overlay.toml`: they have a different scope
from category routing. Both files stay tracked as generator defaults, not generated
outputs. The generator neither invents routes nor changes their declarations. The
runtime uses only compiled holdings; accepted holdings remain private builder inputs and
never become loose runtime files.

The complete private input acceptance retains these policies, source documents,
generated inventory and review evidence in a clean local-only Git repository. Its
manifest covers every payload. Regeneration explicitly selects the accepted candidate's
policies; a tracked default policy is not evidence that a later private candidate uses
identical dispositions. Changed policy or inventory bytes require a fresh acceptance.
The public prepared sources and builder code remain separately pinned.

Inventory generation retains accepted catalog placement bounds and resolves each finite
table edition through positively covering declared owners. Missing coverage or
simultaneous owners produces a worklist before the inventory can be replaced. Each
literal representation must cover the edition; sibling spellings cannot supply its
dates. Source-backed stock-file overrides may name an exact snapshot date, including
October school censuses, while retaining the original catalog state windows. A source
snapshot date alone does not establish the delivery date of a derived geocoded extract.

Unsupported physical fields remain in the inventory with unavailable mappings and
explicit reasons. Exact auxiliary fields can also retain a known owner and meaning in
their evidence while the maintained primary holding supplies the ordering coordinate.
This avoids duplicate logical coordinates without claiming that the auxiliary field's
meaning is unknown or that one delivery supersedes another.

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

`documentary.retained` is the exact guarded disposition for a supplied declaration whose
catalog endpoints are not established. Full declaration and physical-table digests
retain raw cells, provenance, periods and row multiplicity. An admitted source/register
scope is required, but no variable owner or code namespace is invented. The builder-only
resolved record persists under `retained_unattached` with a null owner and no endpoint
rows; its reviewed reason becomes a register data warning. Missing, changed or
duplicated evidence still fails compilation. Builder schema `8.0.0` admits this explicit
status; reader models remain unchanged.

The shared evidence primitives and crosswalk/derivation declarations live in
`reg_meta.source_evidence` and `reg_meta.documentary`. Build ingestion and catalog
reading use the same strict models and validators. Source occurrence, temporal and
reconciliation models remain build-only. This keeps the consumer independent of the
builder while retaining identical serialized source evidence.
