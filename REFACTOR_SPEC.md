# Registry Research Toolkit — Refactor Spec

Working document for the cross-package refactor that turns today's
tooling (`regmeta` + `mock_data_wizard`) into a multi-deployment system
spanning most of the register-research pipeline. Temporary — once
stable, sections of this doc migrate into per-package `DESIGN.md` files
(existing and new), and this file is deleted. Not frozen; edit freely
as decisions change.

This is a deliberate deviation from the policy in `CLAUDE.md` ("no
frozen specs or implementation trackers"). Justification: the refactor
cuts across multiple packages, several of which don't exist yet, so
there is no package-scoped `DESIGN.md` that can hold the cross-cutting
design during the design phase. The doc dissolves at implementation
time (§15).

---

## 1. Background

### The domain

Swedish register research uses administrative microdata produced by
Statistics Sweden (SCB) and other government agencies (Socialstyrelsen,
Försäkringskassan, etc.). Each agency publishes **registers** — large
administrative datasets covering the entire Swedish population —
identified by short names like LISA (labour market), RTB (population),
PAR (patient registry), STATIV (immigrant statistics), FRIDA (firms).

A register contains **variables** (columns), each with a stable
identifier, a definition, a data type, and (for categorical variables)
an enumerated set of **value codes** like Kön ∈ {1=Man, 2=Kvinna}.
Variables often have versioned value sets — SUN2000 vs SUN2020 for
education levels, for example — and registers themselves have yearly
versions because column definitions drift.

A research project requests a **variable list** specifying which
register × variable × year combinations it needs, plus a population
definition (the cohort of individuals or firms studied). After ethical
approval and SCB processing, the project receives a delivery of data
tables. Person identifiers are pseudonymized as project-specific
`LopNr` (running numbers): `P1105_LopNr_PersonNr` means project 1105's
internal id for an individual. Many registers share these identifiers
so records can be linked across them.

### MONA and the agent-incompatibility problem

Data delivery is *not* a download. SCB hosts the data inside **MONA**
(Microdata Online Access), a remote-desktop environment researchers
log into via VPN. The data files and database views live inside MONA;
analysis code runs there; only aggregate statistics may leave (export
requests are reviewed). This is the core regulatory boundary.

Three consequences shape the toolkit:

- **PII may not leave MONA.** Only aggregate, disclosure-controlled
  outputs are exported. This isn't a nice-to-have — it's the
  contractual basis for getting data access.
- **No internet on MONA.** Code that runs there must be self-contained.
  Dependencies come pre-installed (WinPython-31700, ~955 packages
  including duckdb, pyodbc, numpy) or bundled into the uploaded script.
- **LLM agents are not allowed inside MONA.** This is the operational
  constraint that drove `mock_data_wizard` in the first place:
  researchers want to use coding agents for analysis development, so
  they need realistic mock data *outside* MONA that matches the shape
  of the real data inside.

### Data stewards

Researchers don't always order from SCB directly. Some research
organizations maintain their own warehouses of registry data they've
already acquired — they re-license access to internal researchers
without each project going through SCB's full ordering process:

- **IFAU** (Institute for Evaluation of Labour Market and Education
  Policy) — the author's employer. Maintains a long-standing warehouse
  spanning a defined set of registers and years.
- **SWECOV** — a research program (the author runs it) with a smaller,
  more targeted catalog of registers, primarily covid-19-relevant.

When a researcher orders through IFAU or SWECOV, they're constrained
to what's already in that organization's catalog. When they order
directly from SCB, the constraint is whatever SCB publishes.

This is the central multi-tenancy axis: same toolkit, three
**data steward** views — global (the full multi-agency catalog
regmeta indexes), IFAU's subset, SWECOV's subset.

## 2. Current state of the toolkit

Two Python packages today, both in this repo:

### `regmeta`

Searchable database of Swedish registry metadata, indexing ~100M
value-code rows across hundreds of registers. Built by parsing SCB
CSV exports and Socialstyrelsen Excel deliveries (`maintain
build-db`); queried via CLI (`regmeta search`, `regmeta get`,
`regmeta resolve`, `regmeta docs ...`) and as a Python library.
Two SQLite databases: the main metadata DB (`regmeta.db`,
~520 MB) and a separate documentation DB (`regmeta_docs.db`,
~3 MB) built from parsed SCB PDFs. FTS5 indexes on both. Stable JSON output, structured errors,
meaningful exit codes — designed primarily for agent consumption.

This is the **authoritative knowledge layer**: "what variables exist
in LISA in 2018, with what value codes." Currently used by
`mock_data_wizard`'s classifier and enricher; also by humans/agents
searching for variables during research-question exploration.

(Post-refactor: renamed to `reg_meta` per §4. The current names are
preserved in this section to describe today's state.)

### `mock_data_wizard` (CLI: `mock-data-wizard`, colloquially `mdw`)

Generates mock CSVs that match the shape of real MONA data, so agents
and researchers can develop analysis code outside MONA before delivery
arrives or while iterating. Workflow today involves **three MONA
round-trips**:

1. **Discover** — upload a Python bundle, run it on MONA, write
   `mock_data_discovery.json` (column names, SQL types, row counts;
   no values).
2. **Configure** locally — author `mock_data_config.json` declaring
   per-column types (id / categorical / numeric / opaque / date).
   This is the bit a Svelte UI currently helps with.
3. **Extract** — re-upload the bundle in extract mode, run aggregation
   queries on MONA, write `mock_data_stats.json` (min/max/quantiles
   for numerics, frequency tables for categoricals, length stats
   for opaque strings, etc.). PII-scanned before export.

Then `mdw generate` runs locally to produce mock CSVs from the stats.

The MONA-uploaded artifact is a single amalgamated `.py` file (the
"bundle"), because MONA's upload UI penalizes multi-file workflows
and the `.py`-allowed whitelist is permissive enough in practice.

### Current friction points

- **Configure-step UI does too much.** Started as "set column types"
  and grew into a full project-metadata browser (sources, registers,
  panels, reg_meta-driven classification). It's really a project-data-
  management tool wearing a reg-mockdata label.
- **No coverage of steps 1–4 of the research pipeline.** The variable
  list, the data order, the ethical-approval paperwork — researchers
  still maintain these in ad-hoc spreadsheets, often rewriting the
  same information by hand.
- **Discover is wasted work when an order exists.** The user already
  knows what variables they ordered. Probing MONA to find out is
  redundant; the spec should drive the workflow, not be derived from
  the data.
- **Each research org reinvents catalog management.** No shared
  substrate.
- **Tooling is local-only.** CLI + a local HTTP server for the UI.
  Distributing it to non-engineer researchers is friction.

## 3. What this becomes

### Product, in one paragraph

A web application — deployed in three steward-scoped flavours
(global, IFAU, SWECOV) off one codebase — that lets Swedish register
researchers browse a registry catalog, author a per-project variable
list against it, export that list as a data order for their chosen
channel (SCB direct, IFAU warehouse, SWECOV program), and later use
the same authored file to drive realistic mock-data generation
outside MONA so they can develop analysis code with LLM agents
before (or in parallel with) data arriving inside MONA. Backed by
Python packages that are also usable standalone via CLI for
researchers who prefer the terminal.

### Concretely, what a user does

A researcher visits the web app at, say, `swecov.example.org` (the
SWECOV-scoped instance). They:

1. Browse what's in SWECOV's catalog by register, full-text search,
   or by following links from existing register documentation.
2. Build a variable list for their project — clicking variables into
   a "selected" panel, choosing years per register, naming the
   project.
3. Export the order as a CSV (later: as a steward-specific spreadsheet
   or PDF). They submit this to SWECOV's ordering process out-of-band.
4. Download a `project_data.json` snapshot of their selections, commit
   it to their project's git repo alongside their R/Python analysis
   code.
5. While waiting for data: download a Python bundle generated from
   the same `project_data.json`. The bundle is a single `.py` file
   for MONA upload.
6. After delivery on MONA: upload the bundle there, run it to verify
   the spec matches what was delivered (realign), then run it again
   to extract aggregate stats. Download the stats; the web app turns
   them into a "generation kit." Run `reg-mockdata generate` locally to
   produce mock CSVs. Hand the mock CSVs to their LLM agent for
   code development.

Steps 1–3 are the new capability; steps 4–6 are today's
`mock_data_wizard` flow reshaped around the same artifact.

### What the toolkit *is*, structurally

- **A web application** (FastAPI backend + Svelte SPA), hosted by the
  author at three URLs corresponding to the three stewards. No user
  accounts; project state lives in the browser plus the researcher's
  git repo. The web app is the main user surface.
- **A Python CLI toolkit** for the parts that can't or shouldn't run
  in the browser: catalog query (`reg_meta` / CLI `reg-meta`), local
  mock generation (`reg_mockdata` / CLI `reg-mockdata`). The web
  app's backend also imports `reg_meta`, `reg_schema`, and
  `reg_monabundle` as libraries.
- **A shared file format** (`project_data.json`) describing the data
  a project uses, written by the web app and consumed by everything
  downstream — the MONA bundle, the mock-data generator, future
  exporters. The unifying artifact across the whole pipeline.

### Pipeline coverage

Extends the toolkit from "mock data after data arrived" to most of
the register-research pipeline:

1. Explore metadata and documentation
2. Define the research population *(orthogonal — out of scope)*
3. Author a variable list (register × variable × years)
4. Get ethical approval and order data
5. Receive data and do research; mock-data bootstrap before delivery

Steps 1, 3, 4, and the mock-data bootstrap inside step 5 are in scope.
Step 2 (population definition) is a predicate over base registers,
executed only inside MONA; modelling it here would overfit early use
cases. Deferred indefinitely.

### Steward model — three URLs, one deployment

- **global** — full catalog spanning every agency reg_meta indexes
  (SCB, Socialstyrelsen, Försäkringskassan, …); orders go to the
  relevant agency directly.
- **ifau** — subset of registers available through IFAU's warehouse.
- **swecov** — subset available through the SWECOV research program.

All three offer the same user experience; only the catalog they expose
and the order export they produce differ. Same FastAPI binary, same
Svelte build, all three steward configs in one image; three hostnames
point at the same container and the active steward is selected per
request from the `Host` header (§9.1). Hosted by the author (no
self-hosting requirement; IFAU and SWECOV are not expected to operate
the service themselves). No user accounts, no server-side project
storage — projects live in the browser plus the researcher's git repo.

## 4. Target package layout

Monorepo, five Python packages + one webapp. All packages share
the `reg_*` family prefix to signal they're part of one toolkit.
CLI binary names match their package names with hyphens
(`reg-meta`, `reg-meta-build`, `reg-mockdata`); no short aliases
for v1 — they can be added later if testers ask.

```text
registry-research-toolkit/
  reg_meta/         # catalog query lib + CLI (binary: reg-meta)
  reg_meta_build/   # catalog DB builder (binary: reg-meta-build)
  reg_schema/       # project_data.json schema + structural validator
  reg_mockdata/     # local mock CSV generation + compare (binary: reg-mockdata)
  reg_monabundle/   # MONA bundle build + bundle runtime + PII scanner + type compat map
  reg_webapp/
    backend/        # FastAPI; depends on reg_meta + reg_schema + reg_monabundle
    frontend/       # Svelte + Vite (bun)
    stewards/
      global/       # steward.toml only (no catalog; full universe)
      ifau/         # steward.toml + steward.project_data.json
      swecov/       # steward.toml + steward.project_data.json
```

All steward configs live in this monorepo (§9.1). One Docker image
contains all three; runtime hostname dispatch picks the active
steward per request. Per-steward repo autonomy is deferred (§14).

Dependency graph (no cycles):

```text
reg_meta_build → reg_meta
reg_webapp     → reg_meta, reg_schema, reg_monabundle
reg_monabundle → reg_schema
reg_mockdata   → reg_schema
reg_schema     → (none)
```

Each Python package releases to PyPI on its own tag (`reg_meta/v*`,
`reg_meta_build/v*`, etc.). The webapp ships as a container image
on `reg_webapp/v*` tags.

### Why this split

- **reg_meta vs reg_meta_build**: different deps (query needs the
  sqlite3 stdlib; build needs CSV/Excel parsers), different release
  cadence, different operators. The built SQLite DBs (`regmeta.db`,
  `regmeta_docs.db`) are too large to ship inside the wheel (~520 MB
  uncompressed for the main DB; ~120 MB compressed) and are
  distributed as `.zst`-compressed **GitHub release artifacts** on
  `reg_meta/v*` tags. `reg_meta`
  ships a `reg-meta maintain update` command that fetches the
  matching version into `$XDG_DATA_HOME/regmeta/`; the webapp
  Dockerfile runs this at image-build time so the DB ends up in
  an image layer. Mirrors the build/runtime separation needed for a future
  Go/Rust port of the query layer; also enables a future
  offline-bundle scenario directly.
- **reg_schema as a standalone package**: the `project_data.json`
  schema has multiple consumers (`reg_webapp` authors it,
  `reg_monabundle` validates it inside the bundle on MONA,
  `reg_mockdata` reads it at generate time, future exporters read
  it). Tiny, focused, no `reg_meta` dep — schema uses string IDs,
  resolution is the consumer's job.
- **reg_monabundle vs reg_mockdata — separate packages**: the MONA
  bundle (built by `reg_webapp`, runs on MONA) and the local mock
  generator (run by researchers on their laptops) are two different
  workflow tools with different deps and different audiences.
  Splitting them clarifies ownership and lets `reg_webapp` skip the
  mock-generation code path entirely.
  - `reg_monabundle` lives in two halves: a *lightweight* surface
    (`reg_monabundle.build`, `.scan`, `.types`) imported by
    `reg_webapp` at container build time, and a *runtime* surface
    (`reg_monabundle.runtime.*`) that gets amalgamated into the
    bundle as source files and only executes on MONA. The runtime
    half pulls duckdb/pyodbc; the lightweight half is pure-python.
  - `reg_mockdata` is a researcher's local CLI. It owns
    `reg-mockdata generate` (mock CSV generation from a kit) and
    `reg-mockdata compare` (validates locally-stored mocks against the
    spec). Numpy/duckdb live behind a `runtime` extras group.
- **reg_meta is gone from the bundle and from `reg_mockdata`**:
  under the new flow, types come from the spec (authored against
  `reg_meta` in `reg_webapp`), so neither `reg_monabundle` nor
  `reg_mockdata` needs `reg_meta` in-process. MONA-side code is
  `reg_meta`-free entirely.
- **`reg_webapp` does not depend on `reg_mockdata`**: kit-build
  (`POST /api/kit`) is just file packaging — spec + codes + stats +
  README — no Python-package logic involved. `reg_mockdata` consumes
  the kit later, locally, on the researcher's machine.

#### Heavy-deps isolation

Both `reg_monabundle` and `reg_mockdata` declare their heavy deps
as optional, behind a `runtime` extras group:

```toml
# reg_monabundle/pyproject.toml
[project]
dependencies = ["reg_schema"]   # pure-python: build, scan, types

[project.optional-dependencies]
runtime = ["duckdb", "pyodbc"]  # only loaded by reg_monabundle.runtime.*
```

```toml
# reg_mockdata/pyproject.toml
[project]
dependencies = ["reg_schema"]

[project.optional-dependencies]
runtime = ["numpy", "duckdb"]   # `reg-mockdata generate` and `reg-mockdata compare`
```

- `reg_webapp` pins `reg-monabundle` with no extras. The container
  never installs duckdb/pyodbc. Import paths used by the webapp
  must be lazy enough that `import reg_monabundle.build` doesn't
  transitively pull `reg_monabundle.runtime.*`.
- Local researchers install `reg-mockdata[runtime]`.
- On MONA the bundle is self-contained and brings its own deps
  (duckdb/pyodbc are pre-installed in WinPython; no install step).
- CI tests both install shapes for both packages to catch
  accidental imports of runtime-only modules from the lightweight
  paths.

## 5. reg_meta as the substrate

The refactor starts with reg_meta. Every downstream artifact — the
`project_data.json` schema, the webapp's `/api/catalog/*` endpoints,
the generation kit consumed by `reg_mockdata` — references reg_meta
entities (registers,
variables, value codes). The contract between them is only as stable
as reg_meta's identifier scheme. Today that scheme rests on the
empirical observation that "kolumnnamn doesn't change much," which
is not a reg_meta-side invariant. Committed `project_data.json` files
in researchers' git repos depend on this contract, and breakage
manifests as silent rot, not loud errors.

This section defines reg_meta's object model and the **FQID grammar**
that every downstream consumer uses to reference reg_meta entities.
The grammar is anchored to the underlying providers' (SCB,
Socialstyrelsen) numeric IDs for stability, overlaid with curated
human-readable slugs for ergonomics.

### 5.1 Object model

Mostly unchanged from today's `reg_meta/STRUCTURE.md`, with
**provider** promoted to first-class for multi-agency coverage:

| Concept | reg_meta term | Notes |
|---|---|---|
| Data publisher | `provider` (NEW) | `scb`, `sos`, `forsakringskassan`, … |
| Statistical register | `register` | LISA, RTB, PAR. |
| Sub-table within a register | `register_variant` | LISA/Individer 15+, RTB/Folkbokförda. The "table" concept — does not nest further. Socialstyrelsen-side `Deldatamängd` rows map to this slot. |
| Periodic release of a variant | `register_version` | LISA/Individer 15+/2018. |
| Variable concept (register-scoped) | `variable` | "Kön in LISA" and "Kön in RTB" are different variables. Cross-register concept-merging is curation, not identity. |
| Variable in a specific version | `variable_instance` | CVID-bound. |
| Column header(s) | `variable_alias` | One instance can have multiple aliases. |
| Code list attached to an instance | `value_set` | Content-hashed for dedup. Internal to reg_meta; never exposed by FQID. |
| Named versioned vocabulary | `classification` | SUN2020, SSYK2012, LKF — provider-independent. |

Population and `object_type` remain orthogonal context layers on
`register_version`; they do not participate in the FQID.

Registers without a sub-decomposition (Socialstyrelsen's LSS, BU,
SOL) get a synthetic `_default` variant so the schema stays regular.
The rule is mechanical and synthesized at FQID-resolve time, not
persisted: when the requested FQID is `<provider>/<register>/_default`
and the register has zero `register_variant` source rows, the
register_variant resolver returns a virtual placeholder with
`regvar_id = None` (`catalog.py:_synthesize_default_variant`).
Persistence stays clean — every `register_variant` row in the DB is
a real source row, so a curated `slug = "_default"` on a real
single-variant register (the "name-mirror" case) round-trips
unambiguously through `seed-slugs`. The FQID emitter may elide
`/_default/` for display. `_default` is a reserved slug (§5.3).

Deeper FQIDs against variant-less registers (`.../_default/<period>`
and `.../_default/<period>/<variable>`) are accepted by the grammar
(§5.2 shows `sos/lss/_default/2022` as an example) but don't resolve
today: `register_version.regvar_id` is `NOT NULL` with an FK to
`register_variant`, so no version row can attach to a non-existent
variant. Reachability for those kinds lands together with the SOS
ingestion path that needs them — either by making the FK nullable
or by extending `_resolve_version` / `_resolve_binding` to
synthesize the variant slot the way `_resolve_variant` does.

### 5.2 FQID grammar

Every reg_meta entity has a Fully Qualified Identifier — a stable,
`/`-separated string with strict positional grammar. The kind of
an FQID is determined entirely by its segment count plus the
`class/` discriminator prefix; no out-of-band lookup is needed to
identify which kind of entity a string addresses.

| Segments | Form | Kind |
|---|---|---|
| 1 | `<provider>` | provider |
| 2 | `<provider>/<register>` | register |
| 3 | `<provider>/<register>/<variant>` | register_variant |
| 4 | `<provider>/<register>/<variant>/<period>` | register_version |
| 5 | `<provider>/<register>/<variant>/<period>/<variable>` | variable binding |
| 3, leading `class/` | `class/<classification>/<version>` | classification |

Examples:

```text
scb                                                  provider
scb/lisa                                             register
scb/lisa/individer-15plus                            register_variant
scb/lisa/individer-15plus/2018                       register_version
scb/lisa/individer-15plus/2018/kon                   variable binding
sos/lss/_default/2022                                version of a variant-less register
class/sun/2020                                       classification (provider-independent namespace)
class/lkf/2012                                       classification
```

Period segments accept integer year (`2018`) or string period
(`2018-01`, `HT2020`, `2018-Q3`) — the same forms `time_key` accepts in
`project_data.json`. Year is constrained to `(?:19|20)\d{2}`
(1900-2099, SCB-realistic) and month to `(?:0[1-9]|1[0-2])`; the
reserved-slug regexes below share these bounds.

**Variables have no concept FQID.** Variables are addressable only
via 5-segment binding FQIDs. A 3-segment "variable concept" form
(e.g. `scb/lisa/kon`) would collide with the variant form; the
grammar would lose its segment-count discriminator. Cross-edition
operations ("availability of Kön across LISA editions", "all
bindings of `kon` under LISA") are catalog traversals via
`Catalog.editions(...)` (§5.8), not serializable identifiers.
Catalog UI variable pages have URL shapes that embed the same
query parameters but are not FQIDs (§9.5).

**Stored FQIDs never elide.** The `_default` placeholder for
variant-less registers (§5.1) is always written explicitly in
`project_data.json`, slug TOMLs, and any FQID accepted by API
input. Display surfaces (catalog UI, CLI output) may elide
`_default/` for readability; resolvers reject elided input. This
preserves the segment-count discriminator: `sos/lss/2022` is
always a register_variant slugged `2022`, never an elided
register_version.

**Slug grammar.** Every slug must match
`^[a-z][a-z0-9-]*[a-z0-9]$` (lowercase ASCII, kebab-case, must
start with a letter, must end with a letter or digit, single
hyphens only). Single-character slugs match `^[a-z]$`.
Disallowed: leading/trailing hyphens, double hyphens, underscores,
uppercase, non-ASCII. Additional positional constraints below.

**Reserved and disallowed slugs.** Build rejects any slug entry
hitting one of these:

- `class` — reserved in any slot (keeps the leading-`class/`
  discriminator unambiguous; collision in the provider slot is the
  load-bearing case but the literal token is reserved everywhere).
- `_default` (register_variant slot only) — synthesized at
  FQID-resolve time for registers without a sub-decomposition
  (§5.1); curators may also pin this slug onto a real single-variant
  register where the lone variant just restates the register name.
  The underscore prefix is outside the slug grammar, so no other
  slug can collide with it.
- Period-shaped slugs in non-period slots (provider / register /
  variant / variable): a slug that matches the period grammar
  (`^\d{4}$`, `^\d{4}-\d{2}$`, `^[HV]T\d{4}$`, `^\d{4}-Q[1-4]$`)
  is rejected outside the period slot, because it would make the
  segment-count discriminator ambiguous to humans reading the
  FQID. The grammar still parses unambiguously by position; the
  ban is for legibility.

All four are enforced via the same precheck that runs against the
slug TOMLs (§5.3) at build time.

**"Slug" disambiguation.** "Slug" in §5 means an **FQID slug** —
a curated token that forms a segment of an FQID. §10 separately
uses "variable-slug stem" to refer to a small hardcoded subset
of variable slugs that `reg_mockdata` treats as person-invariant
(the population spine). The token type is the same; the use is
different.

**Value sets get no FQID.** Two cases only for declaring a column's
codes:

- A named classification → reference by classification FQID
  (`class/sun/2020`).
- Anything else → inline the codes in `project_data.codes.json`,
  keyed by the **binding FQID** of the column. The content-hashed
  `value_set` row inside reg_meta is a dedup optimization that
  consumers never see.

If a non-standard value set ever needs to be named (e.g. a
SWECOV-specific coding shared across columns), promote it to a
classification. The classification concept doesn't require SCB
blessing — it just means "a named, versioned code list."

### 5.3 Slug curation

Slugs are **curated, never derived** from human-readable Swedish
names (those drift). They are anchored to the underlying provider's
ID system — SCB's `RegisterId` / `RegvarId` / `VarID`,
Socialstyrelsen's identifier scheme — and stored in per-provider
TOML files at `reg_meta_build/fqid_slugs/`. Slugs are build-time
inputs: `reg_meta_build` reads them, compiles slug columns into
the DB asset, and `reg_meta`'s query side only ever sees them
through the DB. The TOMLs are committed to this monorepo
alongside the rest of `reg_meta_build`.

TOML keys for source IDs are **always quoted strings**, regardless
of whether the underlying ID looks integer-shaped. This keeps the
key type uniform across providers (SCB's dot-bearing
`register.variable` keys must be quoted; SCB's bare `register` keys
*could* be bare but uniformly are not, for one canonical form).

```toml
# reg_meta_build/fqid_slugs/scb.toml
[register."34"]
slug = "lisa"

[register_variant."34.153"]
slug = "individer-15plus"
display_group = "Individer"      # optional, presentation-only

[register_variant."34.1335"]
slug = "individer-16plus"
display_group = "Individer"

[variable."34.4"]                # (register_id.var_id)
slug = "kon"
```

```toml
# reg_meta_build/fqid_slugs/classifications.toml
[classification."SUN2020"]
slug = "sun"
version = "2020"
```

The build pipeline reads slug TOMLs alongside source CSVs/workbooks,
populates `slug` columns on `register`, `register_variant`, and
`classification`, and refuses to compile the DB if any source ID is
missing a slug entry ("RegisterId 99 in Registerinformation.csv but
no slug in reg_meta_build/fqid_slugs/scb.toml"). A precheck step
lists missing slugs without trying the full build, for cleaner
failure mode.

**Variables are auto-slugged.** The build derives a slug from the
latest kolumnnamn alias (lowercased, kebab-cased), and the TOML is
used only for exceptions — collisions or cross-edition continuity
claims after a SCB rename:

```toml
[variable."34.137"]
slug = "civilstand"               # explicit override
same_as = [                       # slug-anchored, inline-table form (§5.3 field ref)
  { provider = "scb", register = "lisa", variable_slug = "civilstand-legacy" },
]
```

A one-time `reg-meta-build seed-slugs` command reads the current
data and emits starter TOMLs for hand-review. Estimated bootstrap
size: ~5 providers, ~100 registers, ~300 variants, ~20
classifications — ~400 curated entries plus thousands of
auto-derived variable slugs. Steady-state maintenance: a handful
of new entries per year as agencies add registers.

#### Slug TOML field reference

This subsection is the **single source of truth** for the slug TOML
schema. §5.4 (immutability) and §5.5 (variable identity) reference these
fields rather than redefining them.

Every entity (register, register_variant, variable, classification) uses
the same TOML row shape: a table keyed by the provider's source ID
(e.g. `"34"`, `"34.153"`, `"34.4"`, `"SUN2020"`), with the fields below.

**Slug uniqueness scope.** Slugs must be unique within the smallest scope
that the FQID grammar (§5.2) uses to address the entity — never broader.
Concretely: `register` slugs are unique within a provider; `register_variant`
and `variable` slugs are unique within their parent register (the
`<register>` slot in the FQID already disambiguates them); `classification`
entries are unique by `(slug, version)` pair within `classifications.toml`,
so the same slug stem may appear across versions (e.g. `sun` with
`version = "2000"` and `version = "2020"`). Two registers may both have an
`individer` variant or a `kon` variable without colliding.

| Field           | Type                | Applies to        | Required | Description |
|-----------------|---------------------|-------------------|:--------:|-------------|
| `slug`          | string              | all               | yes      | Curated stem. Immutable once published (§5.4). Variables are auto-slugged; TOML entry only needed for overrides, `same_as`, or `deprecated` / `replaced_by`. |
| `version`       | string              | classification    | yes      | Version stem of the classification FQID (`SUN/2020` → `version = "2020"`). |
| `display_group` | string              | register_variant  | no       | Presentation-only grouping label. Drift-tolerant; can change. |
| `deprecated`    | bool (default false)| all               | no       | Source ID dropped from current deliveries (a register retired, a variable removed). The entry is retained forever — resolution succeeds but emits a warning (§6.8.3). |
| `replaced_by`   | string              | all               | no       | TOML key of the replacement entry. Used to correct a published typo by adding a new row and pointing the old one at it (§5.4); resolution follows the link transitively. Cycles rejected at build. |
| `same_as`       | array               | variable, classification | no | Curated equivalence — "two slugs name the same concept". Always slug-anchored, never source-ID-anchored, so the link survives even if the underlying provider ID changes form. List of inline tables: for variables, `[{provider = "scb", register = "rtb", variable_slug = "kon"}]` (or `register_variant`/`period` if the equivalence is narrower); for classifications, `[{provider = "scb", classification_slug = "sun-v1"}]`. Provider-internal links still spell out the provider for one canonical form. Resolution traverses `same_as` transitively (§5.5, §6.7); cycles rejected at build. |

Worked examples covering every field:

```toml
# reg_meta_build/fqid_slugs/scb.toml — registers and register variants

[register."34"]
slug = "lisa"

[register."99"]
slug = "old-register"
deprecated = true                       # retired delivery; slug retained forever

[register_variant."34.153"]
slug = "individer-15plus"
display_group = "Individer"

# Typo correction (§5.4): never edit in place. Add a new row and link the
# old one via replaced_by. The old row stays in the TOML; resolution
# transparently follows the link.
[register."40"]
slug = "rams-typo"
replaced_by = "40b"                     # TOML key of the corrected entry

[register."40b"]
slug = "rams"

# Variables. Auto-slugged in the build; explicit entries appear only for
# overrides or curated cross-edition links.
[variable."34.137"]
slug = "civilstand"
same_as = [                             # always slug-anchored, inline-table form
  { provider = "scb", register = "lisa", variable_slug = "civilstand-legacy" },
]

# Cross-register same_as (rare). Slug-anchored; slugs are immutable so the
# tuple cannot rot.
[variable."40.91"]
slug = "kon"
same_as = [
  { provider = "scb", register = "rtb", variable_slug = "kon" },
]
```

```toml
# reg_meta_build/fqid_slugs/classifications.toml

[classification."SUN2020"]
slug = "sun"
version = "2020"

[classification."OLDSUN"]
slug = "sun-old"
version = "1996"
deprecated = true
replaced_by = "SUN2020"                 # TOML key — typo-correction link;
                                        # cross-classification equivalence
                                        # uses slug-anchored same_as instead
```

### 5.4 Slug immutability

Once a slug is published, it can never change. Committed
`project_data.json` files reference slugs; renaming a slug rots
every project that references it. Concrete rules:

- The slug TOML is **grow-only**: entries are added; entries are
  never deleted or renamed.
- Removed source IDs (a register dropped from a future delivery)
  are flagged `deprecated = true` but retain their slug forever
  (see `deprecated` in §5.3 field reference).
- A typo in a slug is corrected by adding a new entry with a
  `replaced_by` link on the old one — never by editing in place
  (see `replaced_by` in §5.3 field reference).
- CI enforces these via a snapshot test comparing the current TOML
  to the last committed state; non-additive changes fail the build.

**Activation.** The rules above bind only after the first tagged
release of the refactored system (the first `regmeta` version that
emits FQIDs into a `project_data.json` consumers can commit).
Until that release no external artifact references these slugs, so
the *rule* does not yet protect anything; maintainers may rename,
remove, or restructure entries as the hand-review progresses. The
*tooling*, however, ships fully wired: `reg-meta-build
precheck-slugs --update-snapshot` and the CI snapshot test reject
non-additive changes today, by design — the goal during 1c/1d is
to exercise the mechanism under realistic load. During the pre-v1
curation window a maintainer making a non-additive change works
around the gate by regenerating `.snapshot.json` from the curated
TOMLs (either by hand or via a future `--allow-rename` flag, to be
added only if iteration friction warrants it). When v1 is cut, the
snapshot at that commit becomes the immutable baseline and the
gate transitions from "exercise mechanism" to "protect external
artifacts" — at that point the workaround above is no longer
available.

The same rule applies to every slug-bearing entity: register,
register_variant, variable, classification. Implication: every
FQID is safe to embed in `project_data.json` and in webapp-emitted
order artifacts indefinitely.

### 5.5 Variable identity: bindings only

Variables are addressable only via **binding FQIDs** —
`<provider>/<register>/<variant>/<period>/<variable>`. There is no
concept FQID form (§5.2 explains why). `project_data.json` columns
reference bindings — researchers ordered a specific column from a
specific year, and the spec records what they ordered.

**Cross-edition operations.** "What years is Kön available in
LISA?", "show all bindings of this variable", and similar queries
are catalog traversals through `Catalog.editions(...)` (§5.8), not
serialized identifiers. The webapp's variable-list authoring UI
calls these to populate edition pickers and lineage views.

**Cross-rename equivalence (`same_as`).** When SCB renames a column
between editions and the curator wants to claim "same concept", an
explicit `same_as` field in the variable's slug entry links the
renamed forms (TOML field defined in §5.3). Default behaviour is
conservative: a rename creates a new concept; equivalence is curated
case-by-case. This avoids silent "same name = same concept" bugs
when SCB redefines a variable without renaming.

Resolution follows `same_as` transitively (cycles are rejected at
build), so an old binding FQID continues to resolve after a SCB
rename. The presence of a `same_as` traversal during resolution is
reported as info, not warning — it's expected behaviour.

### 5.6 Consumer-side binding materialization

LISA, RAMS, and other composite registers don't define their own
variables — they aggregate variables sourced from base registers
(RTB, FTB, ...). Today's reg_meta records the source link via
`variable.source_register_id`, which puts the canonical
`variable_instance` row under the *source* register, not the
*consumer* register. This conflicts with the user's mental model
when authoring a spec: a researcher who ordered LISA expects to
address columns under LISA's bindings, not under RTB's.

To reconcile, reg_meta's build emits **consumer-side binding rows**:
for every variable delivered through a composite register, an
additional binding row is created under the consumer's
`register_version`, linked back to the canonical source instance
via a new `via_source_id` edge.

Concretely: a `Kön` variable canonically defined under `scb/rtb`
that flows into LISA also gets a binding row at
`scb/lisa/individer-15plus/2018/kon` (and equivalents for every
LISA edition that pulls it from RTB). The consumer-side binding
inherits its definition, value codes, and types from the source
instance; the catalog UI surfaces the lineage when browsing under
LISA (§9.5 binding endpoint).

Implications:

- The user's spec records the binding under whichever register they
  actually ordered; resolution is unambiguous and matches their
  delivery.
- The consumer-side binding's slug equals the source-side slug
  (both derived from the same canonical kolumnnamn). Cross-register
  consistency is automatic — no `same_as` curation needed for the
  consumer↔source pair.
- Build-time deduplication: one canonical instance per (provider,
  variable_slug) cluster; consumer-side rows reference, never copy,
  the value sets and type info.
- Validator (§6.8.3) treats consumer-side bindings as first-class —
  no warning, no info — because from the user's perspective they
  are the bindings.

The `via_source_id` edge is the LISA composite-source problem
fully resolved at the data layer. §14's open issue on this topic
is reduced to a UI presentation question (how to surface lineage
in the catalog browser).

### 5.7 What this means for project_data.json

Descriptive; the schema-level changes land in §6 when this substrate
is finalised. Headline points:

- Column `name` becomes a **binding FQID**. `display_name` still
  carries the delivered SQL column header.
- Source `register` + `year` collapse into a single
  `register_version` FQID under the source.
- `value_set_version` strings (`Kon@2023`, `SUN@2020`) are replaced
  by:
  - For classifications: a `class/<...>` FQID.
  - For everything else: inline codes in `project_data.codes.json`,
    keyed by binding FQID — no synthetic value-set ID needed.
- The LISA composite-source gap is resolved at the data layer:
  consumer-side binding rows are materialized at build time under
  the consumer register (§5.6), so a binding FQID under LISA always
  resolves directly, no fallback or traversal needed. What remains
  is a UI presentation question — how to surface the underlying
  RTB/FTB lineage when browsing — tracked in §14.

### 5.8 Library API surface

The webapp's `/api/catalog/*` endpoints (§9.5) are thin wrappers
around a stable in-process reg_meta API:

```python
from reg_meta import Catalog

catalog = Catalog.open()                 # mmap'd SQLite

# FQID resolution — single entry point for any FQID kind
entity = catalog.resolve("scb/lisa/individer-15plus/2018/kon")
# returns a typed VariableBinding with .definition, .codes, .lineage

# Cross-edition traversal — replaces the variable-concept FQID
editions = catalog.editions(
    provider="scb", register="lisa", variable="kon"
)
# returns list[VariableBinding] across all (variant, period) combos
# where the variable slug exists, including consumer-side bindings
# from §5.6
```

Exact API design is reg_meta's concern and belongs in
`reg_meta/DESIGN.md` once the rebuild starts. The contract from
this spec's perspective: every FQID resolves through one entry
point; cross-edition browsing has one entry point; the webapp
imports nothing else from reg_meta's internals.

### 5.9 Glossary of identity terms

Used across §5–§10. Each term is either an **entity** (a reg_meta
row / a node in the FQID hierarchy) or a **string** (a slug, an
FQID, an alias). Most confusion in earlier drafts came from
collapsing the two — the entity vs. the token that names it.

| Term | Kind | Definition |
|---|---|---|
| variable concept | entity | The `variable` row in reg_meta: "Kön in LISA" as a register-scoped concept. **Has no FQID** (§5.2); addressed indirectly via its bindings. |
| variable_instance | entity | A `variable concept × register_version` pair: "Kön in LISA/Individer 15+/2018". CVID-bound. |
| binding | entity | A `variable_instance` viewed from the consumer side. The addressable unit. |
| variable_alias | entity | A SQL column header attached to a `variable_instance` (`Kon`, `Kön`). Multiple per instance possible. |
| classification | entity | A named versioned vocabulary (SUN2020). Provider-independent; addressed via the `class/` prefix. |
| value_set | entity | A code list attached to a `variable_instance`. Internal to reg_meta; **never exposed via FQID** (§5.2). |
| slug | string | A curated, immutable identifier token (`lisa`, `kon`, `_default`). Lives in `reg_meta_build/fqid_slugs/*.toml` (§5.3). |
| variable-slug stem | string | The last segment of a binding FQID — the slug naming the variable concept. Used by spine matching (§10) and code lookups (§6.6). |
| concept-slug | string | Older term, synonym for variable-slug stem. **Avoid** in new prose; use "variable-slug stem". |
| binding FQID | string | 5-segment: `<provider>/<register>/<register_variant>/<period>/<variable>` (§5.2). |
| register_version FQID | string | 4-segment: binding FQID minus the variable. |
| classification FQID | string | 3-segment: `class/<name>/<version>` (§5.2). |

The rule of thumb: **entities are nouns the system reasons about**
(rows, nodes, edges); **strings are how those nouns are spoken**
(addresses, slugs, headers). The validator emits errors against
strings; resolution turns strings back into entities.

## 6. The shared schema: `project_data.json`

The load-bearing artifact connecting every step. Built on top of
the reg_meta FQID grammar (§5). Single file, owns:

- Sources, each pinned to a `register_version` FQID
- Columns per source, each pinned to a **binding FQID** (reg_meta
  identity) plus a `display_name` (SQL column header in delivered
  data)
- Panels (entity_key + member time_keys), keyed on `display_name`
  strings
- Per-column tunables under a namespaced `reg_monabundle` block
- Value codes via classification FQID references or held inline in
  sibling `project_data.codes.json`

### 6.1 Top-level shape

| Field             | Type           | Required | Description |
|-------------------|----------------|:--------:|-------------|
| `schema_version`  | string         | yes | Semantic version of the schema (e.g. `"1.0.0"`). |
| `steward`         | string enum    | yes | `"global"` / `"ifau"` / `"swecov"`. Identifies which deployment authored the file. |
| `reg_meta_version` | string         | yes | Release tag of the reg_meta DB asset used during authoring (e.g. `reg_meta/v0.8.0`). Best-effort drift detection on later resolves. |
| `name`            | string         | yes | Project identifier (human-readable). |
| `sources`         | `array<Source>`  | yes | List of data sources (tables) in the project. |
| `panels`          | `array<Panel>`   | no  | Panel definitions over sources. Default `[]`. |
| `reg_monabundle`  | object         | no  | Namespaced block for `reg_monabundle` settings (per-column extract tunables; see §6.5). |

No other top-level fields are allowed; future additions must be
namespaced (e.g. `reg_monabundle:`, `reg_mockdata:`, `swecov:`). A
`value_sets` top-level block from earlier drafts is removed —
codes live in sibling `project_data.codes.json` (§6.9), and
classifications are referenced inline on each Column.

### 6.2 Source

```json
{
  "name": "lisa_2018",
  "register_version": "scb/lisa/individer-15plus/2018",
  "columns": [ /* Column objects */ ]
}
```

| Field              | Type             | Required | Description |
|--------------------|------------------|:--------:|-------------|
| `name`             | string           | yes | Internal source handle (e.g. `lisa_2018`); unique within the spec. Referenced by panel members. |
| `register_version` | string (FQID)    | yes | reg_meta register_version FQID: `<provider>/<register>/<variant>/<period>` (§5.2). |
| `columns`          | `array<Column>`    | yes | Columns to include. At least one. |

The Source replaces the v2 `(register, year)` pair with a single
FQID. Provider, register, variant, and period are all encoded —
no inference needed at validation time.

`where` (per-table SQL predicate) is **not** in the v1 baseline
schema. Cohort filtering inside the MONA bundle still happens at
the `sql_table` / `file_source` level in `reg_monabundle`'s `configure()`,
where it belongs: different tables in one source have different
filter columns (LISA's `AR`, PAR's `INDATUM`), and the filter is
operationally a property of the MONA-side runner, not of the order
spec. Some stewards (notably SWECOV, which delivers narrowed
subsets of large SQL panels) will want to record a per-source
filter in the spec for audit/repro reasons; that is added under
the steward's own namespaced block (e.g.
`"swecov": { "filters": { "lisa_2018": "AR > 2015" } }`) rather
than in the v1 baseline.

### 6.3 Column

```json
{
  "name": "scb/lisa/individer-15plus/2018/kon",
  "display_name": "Kon",
  "type": "categorical",
  "value_set": "class/sun/2020"
}
```

| Field             | Type           | Required | Description |
|-------------------|----------------|:--------:|-------------|
| `name`            | string (FQID)  | yes | Binding FQID: `<provider>/<register>/<variant>/<period>/<variable>` (§5.2). The first four segments must equal the source's `register_version`. |
| `display_name`    | string         | no  | Actual column header in the delivered data. Optional in the schema because at authoring time the value is just an echo of reg_meta's `variable_alias.kolumnnamn` for the binding — when absent, **reg_meta-backed consumers** (webapp, kit-build, semantic validator) resolve the default from reg_meta. Becomes meaningfully distinct from the default at realign time (§7) when project prefixes are applied (e.g. `LopNr` → `P1105_LopNr_PersonNr`) or at order time when a user renames a column. Steward catalogs (§9.1) typically omit it. Reg_meta-free consumers (the bundle on MONA, `reg-mockdata` against a kit) **never** see unresolved `display_name`: bundle build (§7) and kit build (§8) materialize defaults from reg_meta before emitting their artifacts. |
| `type`            | enum           | yes | One of `id`, `categorical`, `numeric`, `date`, `datetime`, `opaque`. |
| `id_subtype`      | enum           | no  | For `id` type: `integer` or `string`. Auto-detected from the data when omitted. |
| `numeric_subtype` | enum           | no  | For `numeric` type: `integer` or `double`. |
| `date_format`     | string         | no  | For `date` type: Python `strptime` pattern. Carries granularity — `"%Y"` is year-only, `"%Y-%m"` is month, `"%Y%m%d"` is day. Default `"%Y-%m-%d"`. |
| `datetime_format` | string         | no  | For `datetime` type: `strptime` pattern with time. Default `"%Y-%m-%d %H:%M:%S"`. Time zones are out of scope for v1. |
| `value_set`       | string (FQID)  | no  | For `categorical` type: a classification FQID (`class/<name>/<version>`). When absent, codes live in `project_data.codes.json` keyed by this column's binding FQID (§6.6). |

Two identifiers, two purposes: `name` (the binding FQID) is the
**reg_meta identity** — used for validation, code-set lookup, and
cross-edition continuity. `display_name` is the **runtime data
column header** — what panel keys reference, what realign matches
against, what `reg_monabundle`'s extract queries read. When
`display_name` is omitted, consumers resolve the default from
reg_meta's `variable_alias.kolumnnamn` for the binding; the moment
the user (or realign) sets an explicit value, it overrides. This
default-resolve rule is what makes the field optional in the
schema without forcing every consumer to special-case absence.

**Alias resolution.** A `variable_instance` may carry several
`variable_alias` rows (renames across editions, parallel headers
for the same column). The default `display_name` for a binding is
the alias whose `variable_instance.period` matches the source's
`register_version` period; if multiple aliases match that period,
prefer the most recently asserted one; if that's still ambiguous,
alphabetical on the alias string is the final deterministic
tie-break. This is a query rule, not a schema rule — it lives in
`reg_meta`'s `Catalog.resolve()` and is what every reg_meta-backed
consumer ends up calling when `display_name` is absent.

**Display-name collisions.** Two columns on the same source
resolving to the same `display_name` (either both explicitly, or
one explicit + one resolving to the same value) produces a
`display_name_collision` validation error (§6.8.0). Remediation:
the user sets an explicit `display_name` on one of the columns,
typically using the project-prefixed form delivered by SCB.

#### The type set

Five general-purpose types plus `datetime`, deliberately compact:

- **`id`** — pseudonymized person/firm/family identifier (`LopNr`,
  `PeOrgNr`). Generation draws from a shared pool keyed by the
  binding FQID's variable-slug stem (§10, population spine).
- **`categorical`** — enumerated value set. Mock generation samples
  from observed frequencies and (when codes are present) extends
  to unobserved codes from the value set.
- **`numeric`** — integer or floating-point measure.
- **`date`** — calendar date with no time component. Granularity
  (year / quarter / month / day) is carried by `date_format`.
- **`datetime`** — date plus time. Rare in SCB data but present in
  some Socialstyrelsen feeds.
- **`opaque`** — free text, codes with no enumerated value set, or
  anything we explicitly choose not to model. Length stats only;
  generation emits `val_000001` placeholders.

Non-standard period codes (`HT2020` = Hösttermin 2020, `VT2021`)
are modelled as `categorical` — either with a `value_set`
pointing at a curated classification (e.g.
`class/swecov-terms/v1`) or with codes inline in
`project_data.codes.json`. Not as `date`.

Explicitly **not** in the type set: `string` (use `opaque`),
`boolean` (use `categorical` with a 2-code value set — SCB encodes
booleans as J/N or 0/1 categoricals anyway), `uuid` (use `id` with
`id_subtype: string`).

### 6.4 Panel

Panel logic operates on delivered data, not on reg_meta entities.
**Panel `entity_key` and bare-string `time_key` references resolve
against columns' `display_name` strings**, not binding FQIDs — the
join is over actual SQL column headers in the delivered tables.

| Field         | Type                          | Required | Description |
|---------------|-------------------------------|:--------:|-------------|
| `panel_id`    | string                        | yes | Unique panel identifier within the spec. |
| `entity_key`  | EntityKey                     | no  | Panel-level default entity-key column(s). At least one member must end up with an effective `entity_key` (default or override) for the panel to validate. |
| `time_key`    | TimeKey                       | no  | Panel-level default time-key. Same rule: each member must end up with an effective `time_key`. |
| `members`     | `array<string \| PanelMember>` | yes | Member sources, optionally with per-member overrides. |
| `comment`     | string                        | no  | Free-text description. |

Type aliases:

```text
EntityKey = string | string[]                       // always column refs
TimePoint = int | string | {"period": int | string} // see below
TimeKey   = TimePoint | TimePoint[]
```

A `TimePoint` is one of:

- **`int`** — literal integer period (e.g. `2018`).
- **`string`** — column name on the source whose values carry the
  period for each row (e.g. `"AR"`). Bare strings are *always*
  column refs; never literal periods.
- **`{"period": int | string}`** — explicit literal period. Used
  for non-integer literals (`{"period": "2018-01"}`,
  `{"period": "HT2018"}`, `{"period": "2019-Q1"}`). The object form
  is the only way to express a string-shaped literal period; this
  removes any "is `"2018"` a column or a year?" ambiguity at the
  schema level.

A panel member is either:

- **A plain string** — the source name. Both panel-level
  `entity_key` and `time_key` defaults must be set.
- **A `PanelMember` object**:
  - `source` (required) — source name.
  - `entity_key` (optional) — overrides the panel-level default
    for this member.
  - `time_key` (optional when the panel-level default is set;
    otherwise required).

Composite-key rules:

- `entity_key` and `time_key` arrays are **ordered**;
  `["LopNr_PeOrgNr", "LopNr_CFAR"]` and `["LopNr_CFAR",
  "LopNr_PeOrgNr"]` are different tuple identities. The validator
  enforces consistent ordering across every member of a panel.
- Within a `TimeKey` array (composite time_key on a single member),
  all elements must be the **same kind** — all column refs, or all
  literals (`int` and/or `{"period": ...}`). Mixed arrays (one
  column ref + one literal) are rejected. The column-ref form
  covers the rare mixed cases by reading both signals as columns.
- Across members of a panel, time_keys *may* mix kinds — one
  member with literal `time_key: 2018` and another with column-ref
  `time_key: "AR"` is permitted. At runtime, `reg_monabundle` models a literal
  time_key as a synthetic constant-value column on the member's
  source, so merge / group-by logic is uniform across the panel.
  Concretely, given a panel with column-ref `time_key: "AR"` and a
  member `{"source": "lisa_2018", "time_key": 2018}` (literal),
  the extract emits

  ```sql
  SELECT *, 2018 AS AR FROM lisa_2018
  ```

  so the downstream `GROUP BY AR` (and shared-column joins) work
  identically across members regardless of how each got its period
  value. This is rare in practice but well-defined.

Source-collision rule: a source participates in **at most one
panel**.

Worked examples:

```json
// Yearly file-members (common case)
{
  "panel_id": "lisa",
  "entity_key": "LopNr_PersonNr",
  "members": [
    {"source": "lisa_2018", "time_key": 2018},
    {"source": "lisa_2019", "time_key": 2019}
  ]
}

// Merged longitudinal — both defaults set; members are bare strings
{
  "panel_id": "sv",
  "entity_key": "LopNr_PersonNr",
  "time_key": "AR",
  "members": ["sos_sv"]
}

// Composite entity_key (workplace = firm × workplace)
{
  "panel_id": "workplace",
  "entity_key": ["LopNr_PeOrgNr", "LopNr_CFAR"],
  "time_key": "AR",
  "members": ["rams_2018", "rams_2019"]
}

// Composite time_key (year × quarter columns on a single file)
{
  "panel_id": "quarterly",
  "entity_key": "LopNr_PersonNr",
  "time_key": ["AR", "KVARTAL"],
  "members": ["sv_quarterly"]
}

// Monthly files with object-form literal periods
{
  "panel_id": "monthly",
  "entity_key": "LopNr_PersonNr",
  "members": [
    {"source": "data_201801", "time_key": {"period": "2018-01"}},
    {"source": "data_201802", "time_key": {"period": "2018-02"}}
  ]
}

// Heterogeneous (rare) — per-member entity_key override
{
  "panel_id": "patient_history",
  "entity_key": "LopNr_PersonNr",
  "time_key": "INDATUM",
  "members": [
    "sos_sv",
    {"source": "par_legacy", "entity_key": "LopNr", "time_key": "AdmDate"}
  ]
}
```

At extract time, `project_data.stats.json`'s `by_period.period`
values **collapse the discriminator**: an authored
`{"period": "2018-01"}` lands as the string `"2018-01"` in the
stats; an integer `2018` stays an integer. The stats consumer only
needs the period label, not how it was authored. The richer
authoring form is preserved in `project_data.json` for future use
(see §14 — chronological semantics).

### 6.5 The `reg_monabundle` namespaced block

Settings consumed by the MONA bundle at extract time (and, for
some keys, by `reg_mockdata` at generate time). **`reg_schema`
treats this block as opaque** — its structural validator checks
only that `reg_monabundle` (if present) is an object.
`reg_monabundle` owns validation of the block's contents, both at
bundle-build time (in `reg_webapp`'s container, via
`reg_monabundle.validate_block`) and at bundle load time on MONA
(via the same code amalgamated into the bundle). Other tools call
`reg_monabundle.validate_block` when they need to check it. This
is the standard pattern for namespaced blocks; future
steward-namespaced blocks (e.g. `swecov`) are owned by their
respective packages the same way.

```json
"reg_monabundle": {
  "column_options": {
    "scb/lisa/individer-15plus/2018/salary": {"suppress_k": 20}
  }
}
```

`column_options[<binding_fqid>]` is a dict of per-column tunables,
keyed by the column's binding FQID (§5.2). Survives realign renames
automatically — the binding FQID is immutable; `display_name` is
not, so keying by display_name would silently drop overrides on
rename. Known keys:

- `suppress_k` (int) — disclosure-control threshold for this column's
  frequency table. **Raise-only**: at runtime the effective value
  is `effective_suppress_k = max(SUPPRESS_K, override)`, so a
  typo'd low value is silently floored to the library default
  rather than weakening disclosure control. `reg_monabundle.validate_block`
  emits an info-level `ValidationIssue` when the override is below
  the floor so the user notices the typo, but the runtime never
  applies a value below the floor.

`reg_monabundle` ships fixed library defaults for disclosure-control
parameters; steward config does not override them. This keeps the
spec freestanding: bundle behavior is determined by the spec +
`reg_monabundle`'s release version, with no out-of-band steward
configuration influencing runtime. v1 defaults:

- `SUPPRESS_K = 10` — frequency-table suppression threshold.
- `SMALL_POP_MULT = 20` — small-population warning trigger
  (`n_rows < SMALL_POP_MULT * SUPPRESS_K`).

Both values are constants in `reg_monabundle.runtime` and are
exported for tests to pin against.

Future per-column option keys are added here as the need arises
(e.g. length caps for opaque generation, override of
auto-detected id_subtype). The block is open-ended;
`reg_monabundle.validate_block` accepts known keys strictly —
unknown keys raise.

### 6.6 Value codes

Codes live in sibling `project_data.codes.json` — never in
`project_data.json` itself. The file is a flat, FQID-keyed object
with two keyspaces sharing one dictionary:

- **Classification FQIDs** (e.g. `class/sun/2020`): the canonical
  code list for that classification, dereferenced from reg_meta at
  kit-build time. Shared across every column that references the
  classification via `value_set`.
- **Binding FQIDs** (e.g. `scb/lisa/individer-15plus/2018/civilstand`):
  the codes for a single column whose `value_set` field is absent —
  i.e. the codes are not part of a named classification.

```json
{
  "class/sun/2020": {
    "codes": [ /* SUN2020 full code list */ ]
  },
  "scb/lisa/individer-15plus/2018/civilstand": {
    "codes": [
      {"code": "G", "label": "Gift"},
      {"code": "OG", "label": "Ogift"}
    ]
  }
}
```

A categorical column's `value_set` field selects which entry
`reg_mockdata` generation reads:

- `value_set: "class/<…>"` → reads `codes[value_set]`.
- (`value_set` absent, type=categorical) → reads
  `codes[<column binding FQID>]`.

The webapp populates `project_data.codes.json` at kit-build time
by dereferencing every classification referenced anywhere in the
spec and every ad-hoc binding (via reg_meta). After kit-build the
project is **freestanding from reg_meta**: a researcher who checks
`project_data.json` + `project_data.codes.json` +
`project_data.stats.json` into git can regenerate mock data years
later regardless of how reg_meta evolves steward-side.

**Codes during authoring.** Before kit-build, classification
references are stored only as FQIDs on the column (no inline
codes — those get dereferenced from reg_meta at kit-build).
Ad-hoc inline codes (a categorical column without a classification
FQID) need an authoring-time home: the SPA stores them in
IndexedDB alongside the in-browser project state, in the same
record but logically separate from `project_data.json` proper —
keyed by binding FQID, same shape as the post-kit
`project_data.codes.json` entries. On "Download
`project_data.json`" the SPA also offers a companion
`project_data.codes.json` download containing only the ad-hoc
entries (no classifications dereferenced yet); this is the form
committed to git pre-kit, and kit-build expands it later. The
SPA's "Open from file" flow accepts the pair.

Kit-build errors loudly when a referenced FQID no longer resolves
in the current reg_meta — "FQID `class/foo/2010` not found; closest
match: `class/foo/2012`." Deprecated entries (slug TOML
`deprecated: true`) resolve normally and emit a warning.

### 6.7 Resolution and the source-link graph

With FQIDs, column resolution against reg_meta is direct: a binding
FQID either maps to a binding row (canonical or consumer-side; §5.6)
or it doesn't. No alias lookup, no fallback chain, no warning
machinery.

The LISA composite-source problem (many LISA variables documented
under their source registers RTB, RAMS, …) is resolved at the data
layer by consumer-side binding materialization (§5.6): a LISA-side
binding row exists for every variable that flows into LISA from a
base register. The webapp's variable-list authoring UI surfaces the
underlying lineage as a presentation detail (§14, deferred), but
the FQID the user picks resolves uniformly under the register they
actually ordered.

The validator therefore:

1. Checks that `name` is a structurally well-formed binding FQID.
2. Checks that its first four segments equal the source's
   `register_version` FQID.
3. Checks that the FQID resolves to a known `variable_instance` in
   reg_meta (hard error on miss).

Where reg_meta records a `same_as` link between two variable
concepts (curated cross-rename equivalence; §5.5), resolution
follows the link so that an old binding FQID continues to resolve
after a SCB rename. The presence of a `same_as` traversal is
reported as info, not warning — it's expected behaviour.

### 6.8 Validation rules

Validation is split across three layers by what data each rule
needs to run. The contract matters because the spec is validated in
three execution contexts (browser SPA, webapp backend, MONA bundle)
with very different dependency availability — only the webapp
backend has reg_meta; only the bundle runs on MONA.

#### 6.8.0 Return shape

All three layers (and any composition of them) return a single
typed shape, defined in `reg_schema`:

```python
@dataclass(frozen=True)
class ValidationIssue:
    level: Literal["error", "warning", "info"]
    code: str         # stable identifier, e.g. "fqid_outside_steward_catalog"
    path: str         # JSON pointer into project_data.json, e.g. "/sources/lisa_2018/columns/3/name"
    message: str      # human-readable

@dataclass(frozen=True)
class ValidationResult:
    issues: tuple[ValidationIssue, ...]
    @property
    def ok(self) -> bool:
        return not any(i.level == "error" for i in self.issues)
```

- **`level`** — `error` blocks downstream actions (bundle build,
  kit build); `warning` surfaces in the SPA but doesn't block;
  `info` is purely advisory (e.g. drift, deprecated traversal).
  `ok = True` means no `error`-level issues — it does **not** mean
  the result was complete. At catalog-load time (§6.8.3 caller
  context), unresolved FQIDs are downgraded to `warning`; affected
  bindings are dropped from the in-memory index but `ok` stays
  `True`. Consumers that need completeness must inspect the
  warnings list, not just `ok`.
- **`code`** — namespaced, stable across releases. Tests pin codes;
  the SPA maps codes to UI affordances. New codes are additive.
  Examples: `fqid_unresolved`, `fqid_outside_steward_catalog`,
  `display_name_collision`, `composite_key_inconsistent`,
  `value_set_missing`, `deprecated_traversal`.
- **`path`** — RFC 6901 JSON pointer relative to
  `project_data.json` root. Empty string for whole-document
  issues. The SPA uses this to jump to the offending field.
- **`message`** — human-readable; safe to localize later, but v1
  is English.

The shape is shared by `reg_schema` (Python), `reg_monabundle`'s
bundle-load validator (Python, amalgamated), and the SPA
(TypeScript, codegen'd from OpenAPI). Composition just concatenates
`issues` from each layer.

#### 6.8.1 Structural rules — `reg_schema`

Enforced everywhere. Pure-Python; no external state needed. The
bundle amalgamates this layer to validate the embedded JSON on
MONA; the SPA mirrors it (TypeScript or compiled-to-JS) for
in-browser editing feedback.

- Presence and type of all required fields.
- `type` ∈ the enum defined in §6.3; subtype/format fields are
  valid only on the matching type.
- Every source's `register_version` is a structurally well-formed
  register_version FQID (4 segments: provider/register/variant/period).
- Every column's `name` is a structurally well-formed binding FQID
  (5 segments). The first 4 segments must equal the source's
  `register_version` — enforced as a structural check, no reg_meta
  needed.
- `value_set` (when present) is a structurally well-formed
  classification FQID: `class/<name>/<version>` (3 segments,
  leading `class/`).
- Every panel member has an effective `entity_key` and effective
  `time_key` (inherited from panel-level defaults or member-level
  overrides).
- For each member, every column referenced by `entity_key` (or
  its array elements) exists on the member's source — matched
  against columns' `display_name` strings.
- For each member, every bare-string `time_key` element resolves
  to some column's `display_name` on the member's source. (Bare
  strings are always column refs; literal periods use the
  `{"period": ...}` form or an int.)
- Composite `entity_key` / `time_key` arrays are ordered
  consistently across every member of a panel.
- Composite `TimeKey` arrays are homogeneous: all column refs, or
  all literals (`int` / `{"period": ...}`); mixed kinds are
  rejected.
- Member-level composite `time_key` overrides must use the **same
  kind** as the panel-level composite `time_key` (both column-ref,
  or both literal). Scalar mixing across members is still
  permitted (§6.4); composite mixing is not, to keep the
  synthetic-constant materialization rule (§6.4) tractable on
  tuples.
- Each file-member's literal `time_key` is unique within the panel.
- A source name is referenced by at most one panel.
- No unknown top-level fields except namespaced ones
  (`reg_monabundle`, `reg_mockdata`, `swecov`, etc.); namespaced
  blocks (if present) must be objects. Their contents are not
  inspected at this layer.

#### 6.8.2 Namespaced-block rules — owning package

Each namespaced block is validated by its owner.

- The `reg_monabundle` block is validated by `reg_monabundle` —
  known option keys, value types, raise-only invariants like
  `suppress_k ≥ library default`. Runs at bundle-build time in
  `reg_webapp` and at bundle load time on MONA (same code,
  amalgamated into the bundle).
- Future steward-namespaced blocks follow the same pattern; e.g. a
  `swecov` block would be validated by the package or module that
  owns the SWECOV deployment's extensions.

`reg_webapp` invokes the owning validators alongside structural
validation in `POST /api/project/validate`. `reg_schema` itself
does not import the owning packages — orchestration is the
webapp's job (and any local CLI's, if it cares).

#### 6.8.3 Semantic rules — reg_meta-backed

Enforced where a reg_meta database is available — the webapp
backend and any local tool that has loaded reg_meta. **Not enforced
inside the MONA bundle** (no reg_meta on MONA, no network).

- Every source's `register_version` FQID resolves to a known
  `register_version` row in reg_meta. Code: `fqid_unresolved`,
  level `error`.
- Every column's `name` (binding FQID) resolves to a known
  `variable_instance` (following `same_as` curated links if
  present; §6.7). Code: `fqid_unresolved`, level `error`.
- Every `value_set` (classification FQID) resolves to a known
  `classification`. Code: `value_set_missing`, level `error`.
- Deprecated entries (slug TOML `deprecated: true`) resolve
  normally but emit `deprecated_traversal` at level `warning`.
- For each FQID in the spec, the steward's catalog admits it.
  When a project's binding or `register_version` lies outside
  the loaded steward catalog, emit `fqid_outside_steward_catalog`
  at level `warning` (not error — this is also the deliberate
  feature for "what would my project look like under steward X?":
  a researcher can load their spec against another steward's
  deployment and the warnings enumerate exactly which columns
  would be unavailable). The SPA surfaces these warnings with a
  one-click "drop out-of-scope columns" remediation. The `global`
  deployment never emits this code (no filter).

**Drift detection.** Validation also compares the spec's
`reg_meta_version` against the running reg_meta release tag. FQIDs
are stable, so this is best-effort: a mismatch is reported as
info, not error, and any deprecation warning includes a note that
the deprecation was introduced after the spec's recorded version.

**Caller context — researcher-project vs steward-catalog.** The
levels listed above are for the **researcher-project** path
(`POST /api/project/validate`): unresolved FQIDs are blocking
errors because the researcher needs to fix them before extract.

The **steward-catalog load** path (FastAPI startup, §9.1) uses
the same validator but **downgrades `fqid_unresolved` and
`value_set_missing` from `error` to `warning`** so the deployment
doesn't fail to start when reg_meta evolves out from under a
steward's committed catalog. Affected bindings are removed from
the in-memory index (they can't be authored against until the
steward updates the catalog) and the warnings are exposed via
`/api/context` so the SPA can surface a "catalog drift" banner.

The split lives in `reg_schema`'s caller surface — same
`ValidationIssue` codes, different `level` mapping by caller —
not in the rule definitions themselves.

**Onboarding new variables.** Stewards declare a subset of what
reg_meta knows. Data not in reg_meta has no FQID and cannot be
authored into a catalog — there is no `{display_name + type, no
FQID}` escape hatch in v1. New variables, registers, or
classifications onboard via slug-TOML PRs against `reg_meta_build`
(§5.3); once the next reg_meta release lands, the steward can add
them to their catalog through the webapp.

The browser SPA reaches semantic validation via
`POST /api/project/validate`. The MONA bundle reaches it only
indirectly: kit-build (`POST /api/kit`) re-runs semantic
validation on the spec before dereferencing value sets, so any
drift introduced by a MONA-side inline edit (§7) is caught at
kit-build time at the latest.

### 6.9 Sibling files

- `project_data.codes.json` — value codes for every categorical
  column, FQID-keyed (classification FQIDs and binding FQIDs in
  one keyspace; §6.6). Written by the webapp at kit-build time;
  committable to keep the project freestanding from reg_meta.
- `project_data.stats.json` — output of MONA extract; aggregate
  statistics only; PII-scanned.
- `project_data.realign.json` — small JSON patch produced by the
  bundle's realign pass (§7).

### 6.10 Storage

Project files live in the browser (IndexedDB) during a session and
as JSON in the user's project git repo for durability. The
in-browser record holds the spec plus any ad-hoc inline codes
authored against it (§6.6 "Codes during authoring"); downloading
emits `project_data.json` plus the companion
`project_data.codes.json` (pre-kit form: only ad-hoc entries).
**No server-side storage** — git is the durable store,
email/git-sharing handles collaboration. Server-side projects are
a possible v2 feature, not v1.

## 7. MONA workflow

### Single-file bundle, embedded config

`reg_webapp` builds a single `.py` file per upload via
`reg_monabundle.build`, containing both the bundle runtime code
(amalgamated from `reg_monabundle.runtime.*` source files) **and**
the `project_data.json` spec embedded as a JSON string literal near
the top.

**Pre-resolution at build time.** Before embedding the JSON,
`reg_monabundle.build` resolves every absent `display_name` from
reg_meta (§6.3 alias resolution) and writes the result back into
the spec — so the embedded JSON always has `display_name` on every
column. The bundle on MONA has no reg_meta and never needs one for
this lookup. Same applies to any other reg_meta-derived defaults
the spec may grow in future: pre-resolve at build, embed the
resolved form.

```python
# === EMBEDDED PROJECT CONFIG ===
_PROJECT_DATA_JSON = r"""
{ "name": "swecov-education", "sources": [...], ... }
"""
# === END CONFIG ===
PROJECT_DATA = json.loads(_PROJECT_DATA_JSON)
```

The bundle has one default invocation plus two flag-toggled
variants:

```text
python project_bundle.py            # realign-then-extract; happy path → stats.json
python project_bundle.py --check    # realign phase only; never extract
python project_bundle.py --force    # skip realign; extract regardless
```

Generation is a **local** step (`reg-mockdata generate`), not a bundle mode —
it runs against the generation kit (§8) outside MONA.

### Two phases, one invocation

The default invocation runs in two phases:

1. **Realign phase** — pulls `INFORMATION_SCHEMA.COLUMNS` +
   `COUNT(*)` only (no row-level data; runs in seconds). Verifies
   that every spec column's `display_name` exists in the data and
   that the declared `type` is compatible with the observed
   `sql_type` (via `reg_monabundle.types.is_compatible`; see below).
2. **Extract phase** — runs the aggregation queries (potentially
   hours). Only entered if realign finds zero diffs.

If realign finds any diffs the bundle writes
`project_data.realign.json`, exits non-zero, and never touches the
extract phase. This makes "I forgot to realign" structurally
impossible — a multi-hour misaligned extract cannot be launched by
accident.

### Flow

```text
[Webapp]                                  [MONA batch host]
build bundle  ────────upload───────────►  python bundle.py
                                          │
                                          ├─ realign phase (seconds)
                                          │
                                          ├─ clean → extract phase
                                          │            ↓
consume stats ◄───────download─────────   │          stats.json
emit generation kit (offline)             │
                                          └─ diffs  → exit 1, write
review patch  ◄───────download─────────              realign.json
update spec, rebuild bundle ──────────►   python bundle.py (re-run)
```

Happy case is **one MONA round-trip**. Misalignment costs a second
round-trip after reconciliation.

### No discover trip

Pre-refactor, the toolkit did three MONA round-trips (discover →
configure → extract). The first one (discover) goes away: the spec
is authored from the order, not derived from the data. **Realign**
replaces discover as a small pre-extract verification:

- Pulls `INFORMATION_SCHEMA.COLUMNS` + `COUNT(*)` only.
- Verifies each spec column's `display_name` exists in the data.
- Verifies each column's declared `type` is compatible with the
  observed `sql_type` (via `reg_monabundle.types.is_compatible`; see below).
- Emits a patch listing only genuine diffs — no false positives
  from MONA's "everything is VARCHAR" reality, no rename
  detection, no automatic spec mutation. The bundle judges
  compatibility but never mutates the spec; reconciliation is the
  webapp's job.

`project_data.realign.json` (written only when there are diffs):

```json
{
  "schema_version": "1.0.0",
  "project": "swecov-education",
  "sources": {
    "lisa_2018": {
      "row_count": 8492768,
      "missing_in_data": [
        {"binding": "scb/lisa/individer-15plus/2018/lopnr", "display_name": "LopNr"},
        {"binding": "scb/lisa/individer-15plus/2018/fobostat", "display_name": "FoBoStat"}
      ],
      "extra_in_data": ["P1105_LopNr_PersonNr", "UnexpectedCol"],
      "type_mismatches": [
        {"binding": "scb/lisa/individer-15plus/2018/birthdate",
         "display_name": "BirthDate",
         "spec_type": "date",
         "sql_type": "INTEGER"}
      ]
    }
  }
}
```

`missing_in_data` lists columns by their binding FQID plus the
`display_name` the bundle queried for. `extra_in_data` lists SQL
column names found in the data but not queried. `type_mismatches`
lists columns whose declared `type` is incompatible with the
observed `sql_type` per `reg_monabundle.types.is_compatible`.

### Type compatibility lives in `reg_monabundle`

The SQL↔spec-type machinery is owned by `reg_monabundle` (its
pure-python lightweight side, amalgamated into the bundle and
imported by `reg_webapp`). Two functions:

- `reg_monabundle.types.is_compatible(spec_type, sql_type) -> bool`
  — encodes "what `reg_monabundle`'s extract code can ingest":
  e.g. `numeric` is compatible with `VARCHAR`, `INTEGER`,
  `DECIMAL`, `DOUBLE` (the bundle probes and promotes); `date` is
  compatible with `VARCHAR`, `DATE`, `DATETIME` but not `INTEGER`.
  Drives realign-time mismatch detection.
- `reg_monabundle.types.suggest_spec_type(sql_type) -> SpecType`
  — the inverse mapping: given an observed SQL type, what spec
  type would a user most plausibly declare for it? `VARCHAR` →
  `opaque`, `INTEGER` → `numeric` (subtype `integer`), `DATE` →
  `date`, etc. Used by the realign-review UI to pre-fill the
  "accept SQL type into spec" affordance (§ Reconciling the patch
  below). Always returns *some* spec type so the user has a
  starting point; the user can override before applying.

Living in `reg_monabundle` rather than `reg_webapp` keeps the
durable artifact durable: the spec, the codes, the stats, and the
rules that govern how they're interpreted are all in packages that
ship to the researcher and into the bundle on MONA. `reg_webapp`
is one consumer of those rules, not the owner. If `reg_monabundle`
learns a new cast, the realign check learns it the same release.

### Reconciling the patch

The spec is authoritative. The webapp loads the patch into the
in-browser project state and walks the user through one screen
where every discrepancy is resolved. A "rename" is conceptually a
paired missing-and-extra, so reconciling both at once keeps the
UX coherent. Four kinds of action:

- **Pair as rename.** Link a `missing_in_data` entry to an
  `extra_in_data` entry; the webapp updates `display_name` on the
  matching column to the SQL header. `name` (the binding FQID) is
  never modified — reg_meta identity is stable across the project
  lifecycle. The UI may suggest pairings heuristically (e.g. when
  one is a project-prefixed form of the other), but the user
  confirms.
- **Remove from spec.** A remaining `missing_in_data` entry is
  truly absent; drop the column from its source.
- **Add to spec.** A remaining `extra_in_data` entry is a real
  new column; the UI prompts for a binding FQID (chosen against
  reg_meta via catalog search) and a `type`, and stores the
  delivered SQL string as `display_name`.
- **Resolve type mismatch.** For every entry in `type_mismatches`,
  either accept the SQL type into the spec (changes the column's
  `type` / `numeric_subtype` / etc.) or remove the column from the
  spec. Overriding (keep the spec type and let `reg_monabundle` cast anyway) is
  not a reconciliation option — the bundle already judged the
  combination unsupported, and `--force` is the only way to extract
  past the diff.

The realign-review UI is client-side only. After reconciliation,
the in-browser spec is updated and the next bundle download
embeds the corrected version. No server endpoint applies the
patch.

The bundle structurally enforces realign-before-extract by running
both phases in a single invocation. The `--check` flag stops after
realign (useful for "verify my spec without committing to the long
run"); `--force` skips realign entirely (escape hatch for the rare
case where the user knows about the diff and wants to extract the
matching columns anyway). Default behavior — no flag — is the
safest: realign runs, extract follows iff clean.

**`--force` extract semantics.** With realign skipped, extract
proceeds column-by-column. For each column:

- If the column's `display_name` is absent from the data: emit a
  warning to stderr, skip the column, continue with the rest of
  the source.
- If the column's `display_name` is present but its declared
  `type` is not `is_compatible` with the observed `sql_type`: try
  the cast; on cast failure, emit a warning and skip the column.
  On cast success (e.g. a `VARCHAR` declared as `numeric` that
  parses cleanly), proceed normally.
- The resulting `project_data.stats.json` records `null_count`
  and `n_distinct` only for columns that successfully extracted;
  skipped columns are absent from the per-source `columns` map.

`--force` is **the only path** that produces a partial extract;
the default and `--check` paths never emit a partial stats file.
Researchers using `--force` should treat the resulting kit as
provisional.

### Permissive embedded JSON

The JSON literal in the bundle is editable inline on MONA if needed
(e.g. a tweak under time pressure). **No checksum lock, no integrity
hash.** The threat model is: there is no adversary. The user is the
sole editor; if they corrupt the spec they get a clear parse error
from the bundle's load-time validator. The webapp is the
*recommended* authoring surface, not the *only* one.

Bundle parser validates on load and errors clearly on malformed
JSON or schema violations. **Scope**: only the §6.8.1 structural
rules plus the §6.8.2 `reg_monabundle` namespaced-block rules run
at load — no semantic validation (no `fqid_unresolved`, no
`value_set_missing`, no `deprecated_traversal`), because reg_meta
isn't on MONA. Drift introduced by an inline edit is caught at the
next webapp round-trip (kit-build re-runs semantic validation per
§6.8.3).

## 8. Value codes and the generation kit

### Codes live alongside the spec

See §6.6. `project_data.json` carries `value_set` references
(classification FQIDs) and binding FQIDs on every categorical
column; the actual code lists live in sibling
`project_data.codes.json`, FQID-keyed. After kit-build the trio
`project_data.json` + `project_data.codes.json` +
`project_data.stats.json` is **freestanding from reg_meta** — a
project committed to git regenerates the same mock data years
later, regardless of how reg_meta evolves steward-side.

### `project_data.stats.json` schema (v1)

Produced by `reg_monabundle`'s extract phase on MONA, downloaded by the
researcher, then consumed by `reg_webapp` (kit-build) and `reg_mockdata`
(generation). The file is the durable record of the population's shape; PII
is already removed by aggregation, and `null_count` is suppression-aware
(§6.5 `suppress_k`).

The schema is expected to grow as `reg_mockdata` learns to model more
patterns; v1 fixes the column-keyspace (binding FQIDs), the period encoding
(scalar or composite tuple), and the four sections below.

```json
{
  "schema_version": "1.0.0",
  "project": "swecov-education",
  "generated_at": "2026-03-04T10:30:00Z",
  "reg_meta_version": "reg_meta/v0.8.0",

  "sources": {
    "lisa_2018": {
      "row_count": 8492768,
      "columns": {
        "scb/lisa/individer-15plus/2018/kon": {
          "display_name": "Kon",
          "nullable": false,
          "null_count": 0,
          "null_rate": 0.0,
          "n_distinct": 2,
          "stats": {
            "frequencies": {"1": 4231000, "2": 4261768},
            "suppressed_below_k": 5
          }
        },
        "scb/lisa/individer-15plus/2018/ink": {
          "display_name": "Ink",
          "nullable": true,
          "null_count": 152340,
          "null_rate": 0.0179,
          "n_distinct": 982341,
          "stats": {
            "min": 0, "max": 5234100,
            "mean": 425000, "sd": 312000,
            "quantiles": {"p01": 0, "p05": 12000, "p25": 180000,
                          "p50": 380000, "p75": 580000, "p95": 1200000,
                          "p99": 2500000}
          }
        }
      }
    }
  },

  "shared_columns": [
    {"binding": "scb/lisa/individer-15plus/2018/kon",
     "sources": ["lisa_2018", "lisa_2019"],
     "max_n_distinct": 2}
  ],

  "panels": [
    {
      "panel_id": "lisa",
      "entity_key": "LopNr_PersonNr",
      "members": [
        {"source": "lisa_2018", "time_key": 2018},
        {"source": "lisa_2019", "time_key": 2019}
      ],
      "by_period": [
        {"period": 2018, "source": "lisa_2018",
         "n_rows": 8492768, "n_entity_ids": 8392104},
        {"period": 2019, "source": "lisa_2019",
         "n_rows": 8512345, "n_entity_ids": 8401234}
      ]
    },
    {
      "panel_id": "quarterly",
      "entity_key": "LopNr_PersonNr",
      "members": [{"source": "sv_quarterly", "time_key": ["AR", "KVARTAL"]}],
      "by_period": [
        {"period": [2018, 1], "source": "sv_quarterly",
         "n_rows": 100000, "n_entity_ids": 50000},
        {"period": [2018, 2], "source": "sv_quarterly",
         "n_rows": 105000, "n_entity_ids": 51000}
      ]
    }
  ]
}
```

**Root fields.**

| Field             | Type            | Required | Description |
|-------------------|-----------------|:--------:|-------------|
| `schema_version`  | string (semver) | yes | This document. Bumped on breaking changes. v1 = `1.x.x`. |
| `project`         | string          | yes | Echo of `project_data.json`'s `name`. |
| `generated_at`    | string (ISO-8601) | yes | UTC timestamp the extract finished. |
| `reg_meta_version`| string          | yes | Echo of the reg_meta release tag recorded in the spec at extract time. Drift detection only; not enforced (§6.8.3). |
| `sources`         | object          | yes | Map: source name → per-source stats. |
| `shared_columns`  | array           | yes | Pool-sizing hints for columns appearing on more than one source; may be empty. |
| `panels`          | array           | yes | One entry per panel in the spec; may be empty if the spec has no panels. |

**Per-source (`sources.<name>`).**

| Field         | Type    | Required | Description |
|---------------|---------|:--------:|-------------|
| `row_count`   | int     | yes | Total rows in the delivered source. |
| `columns`     | object  | yes | Map: **binding FQID** → per-column stats. Keyspace is FQIDs (not display names) so cross-edition rename does not silently break consumers. |

**Per-column (`sources.<name>.columns.<binding_fqid>`).**

| Field          | Type     | Required | Description |
|----------------|----------|:--------:|-------------|
| `display_name` | string   | yes | Delivered SQL column header at extract time. Echo of the spec's `display_name` (§6.3); included for human readability and for tools that prefer header-keyed reads. |
| `nullable`     | bool     | yes | True iff any null was observed (or null_count was suppressed). |
| `null_count`   | int      | no  | Total nulls. **Suppressed** when `0 < null_count < suppress_k` (omitted from the object); consumers must treat absence as "small unknown ≥ 1". |
| `null_rate`    | number   | no  | Same suppression as `null_count`. |
| `n_distinct`   | int      | yes | Distinct value count (perturbed if suppression rules require). |
| `stats`        | object   | yes | Type-specific aggregates. Shape depends on the spec's declared `type` (§6.3) for this binding: |

Type-specific `stats` shapes:

- **`id`** — `{}` (only `n_distinct` is meaningful; pool size is the signal).
- **`categorical`** — `{frequencies: {code: count, ...}, suppressed_below_k: int}`. Codes with counts below `suppressed_below_k` are folded into the cell or dropped; consumers treat missing codes as "small unknown".
- **`numeric`** — `{min, max, mean, sd, quantiles: {p01, p05, p25, p50, p75, p95, p99}}`. All values perturbed deterministically; min ≤ max and quantiles are monotonic.
- **`date`** / **`datetime`** — `{min, max}` in the column's `date_format` / `datetime_format`.
- **`opaque`** — `{min_length, max_length, mean_length}`. No content statistics; generation emits placeholders.

**`shared_columns[]`.**

| Field            | Type     | Description |
|------------------|----------|-------------|
| `binding`        | string   | Binding FQID. |
| `sources`        | string[] | Source names where this binding appears. |
| `max_n_distinct` | int      | Largest `n_distinct` across listed sources. Used by `reg_mockdata` to size the shared id/categorical pool. |

**`panels[]`.**

| Field        | Type                          | Description |
|--------------|-------------------------------|-------------|
| `panel_id`   | string                        | Echo of the spec. |
| `entity_key` | string \| string[]            | Echo of the (resolved per-panel) entity key. Composite tuples preserve spec order. |
| `members`    | `{source, time_key}[]`        | Each member's effective `time_key`. For composite column-ref time_keys, `time_key` is an array of column names. |
| `by_period`  | `PeriodStat[]`                | One row per (member, observed period) pair. |

`PeriodStat`:

| Field          | Type                                | Description |
|----------------|-------------------------------------|-------------|
| `period`       | int \| string \| (int \| string)[]  | Scalar for single time_key; tuple for composite time_key (order matches `members[i].time_key`). String-form literal `{"period": "2018Q1"}` collapses to the bare string at extract time (§6.4). |
| `source`       | string                              | Source the row came from. |
| `n_rows`       | int                                 | Rows in this (source, period) cell. |
| `n_entity_ids` | int                                 | Distinct entity-key values in this cell. For composite entity keys, the distinct **tuple** count. |

**Disclosure-control invariants.** Producer-side: the bundle never emits a `null_count` in `(0, suppress_k)`; category frequencies < `suppress_k` are suppressed; small-count perturbation may move `n_distinct`. Consumer-side: `reg_mockdata` and `reg_webapp` treat absent fields as "small unknown ≥ 1", not zero.

**Forward compatibility.** Consumers must tolerate unknown keys in any object and unknown keys inside `stats`. New `type`-specific shapes can be added by minor-version bumps; renames or removals require a major-version bump.

### Generation kit

When the user is ready to generate mocks, the webapp emits a
**generation kit**: a downloadable bundle containing

- `project_data.json` — the spec with FQID references.
- `project_data.codes.json` — dereferenced codes, FQID-keyed.
- `project_data.stats.json` — extract output (uploaded earlier).
- A README and a ready-to-run command.

The user downloads the kit and runs `reg-mockdata generate` locally against
it. `reg_mockdata` consumes JSON only — no reg_meta dep, fully offline.

**Post-kit invariant:** every categorical column's codes live in
`project_data.codes.json` regardless of how they were declared in
the spec. Classification FQIDs (`class/sun/2020`) are
**dereferenced** at kit-build into inline entries; ad-hoc inline
sets are passed through unchanged. The same lookup path
(`codes_by_fqid[<binding-or-classification-fqid>]`) covers both
post-kit. This is what makes `reg_mockdata` reg_meta-free and the
kit reproducible years later.

### Reproducibility

Same spec + same codes + same stats → same generation kit → same
mock data. Regmeta version drift is inert because every code list
has been dereferenced into `project_data.codes.json` at kit-build
time.

## 9. Webapp architecture

### Multi-steward, single deployment

One shared `reg_webapp` binary; **all steward configurations live
in this monorepo** (§4) and ship inside a single Docker image.
Runtime dispatch by `Host` header: a request to
`global.example.org` selects the `global` steward config,
`ifau.example.org` selects `ifau`, and so on. The SPA fetches
`/api/context` on boot to learn its identity and branding;
Cloudflare keys cache by (host, path) so steward responses stay
separated. Per-steward repo autonomy (stewards versioning their
own catalogs in their own repos) is deferred (§14).

```text
reg_webapp/stewards/
  global/
    steward.toml                       # identity only; no catalog (full universe)
  ifau/
    steward.toml
    steward.project_data.json
    order_template.j2                  # optional; omit to inherit the default CSV (§9.5)
  swecov/
    steward.toml
    steward.project_data.json
```

### 9.1 Steward configuration

A steward is configured by two files in `reg_webapp/stewards/<id>/`:

1. **`steward.toml`** — identity and branding (small, ~10 lines).
2. **`steward.project_data.json`** — the catalog of what's
   available through this steward, expressed as a regular
   `project_data.json` instance (§6). The steward enumerates
   sources and columns; the FQIDs on those columns *are* the
   catalog. No separate catalog schema.

Adding a new steward is a monorepo PR: drop a directory in
`stewards/`, register the hostname in DNS/Cloudflare, rebuild the
image. Catalog updates are PRs against the existing directory.

**`steward.toml`:**

```toml
id = "ifau"
name = "IFAU"
long_name = "Institute for Evaluation of Labour Market and Education Policy"
hostname = "ifau.example.org"
order_template = "order_template.j2"   # path relative to this steward dir;
                                       # omit to inherit the default CSV (§9.5)
```

The `id` matches the directory name and is used as the URL slug
for any future steward-namespaced routes. `hostname` is the
canonical hostname the runtime matches against the request's
`Host` header; multiple hostnames per steward (e.g. an apex +
www variant) are out of scope for v1 — front Cloudflare with a
redirect instead.

**`steward.project_data.json`:** structurally identical to a
researcher's project (same `reg_schema` validator). It has many
`sources` and no `panels` — panels are project-level extract
decisions, not steward-level offerings. `display_name` is
typically omitted (the catalog doesn't know any researcher's
delivered SQL header; tools resolve `variable_alias.kolumnnamn`
defaults from reg_meta, §6.3). `value_set` references on
categorical columns are preserved (the steward is declaring which
code lists they support). The webapp's catalog-authoring UI does
not expose panel creation or `reg_monabundle` configuration, and
the catalog-import path (loading an existing
`steward.project_data.json`) rejects either with an error — these
fields belong to researcher projects, not catalogs.

**Why reuse `project_data.json`?** Authoring a steward catalog is
exactly the same UX as authoring a project: browse the global
catalog, pick registers/variants/years/variables, save. So the
steward can just run the webapp against the **`global`** deployment
(which has no filter — it's reg_meta's full universe), build their
catalog in the SPA, and download the resulting JSON as their
`steward.project_data.json`. Maintenance is the same flow: open
the existing catalog in the webapp against `global`, add or remove
columns, re-download.

This also unifies validation: the same structural+semantic
validators (§6.8) check both a researcher's project and a steward's
catalog, so consistency comes for free.

**The `global` deployment** is special: it ships **no
`steward.project_data.json`** — no filter, reg_meta's full universe
is offered. `steward.toml` exists with `id = "global"`. The catalog
endpoints check for the presence of a steward project file at
startup and dispatch accordingly: filter on, or pass-through.

**FastAPI startup** reads both files, validates the steward
project against the loaded reg_meta, and builds the in-memory
index: `register_version FQID → set<binding FQID>` and `register
FQID → year-range`, derived directly from `sources[].register_version`
and `sources[].columns[].name`. The validate endpoint and the
variable-list authoring endpoints consult this index. The
`fqid_outside_steward_catalog` warning (§6.8.3) fires when a
researcher's project references an FQID not in the index.

**Maintenance.** When reg_meta gains new entries (a new variant, a
new variable on an existing version), nothing happens
automatically — the steward decides whether to admit them by
opening their catalog in the webapp against `global` and adding
the new columns, then committing the result back into
`reg_webapp/stewards/<id>/steward.project_data.json`. Optional
CLI: `reg-meta-build steward-diff
<steward.project_data.json>` lists FQIDs in reg_meta that aren't
in the steward catalog, plus FQIDs in the catalog whose
`register_version` reg_meta no longer knows about (rare — slugs
are immutable, but a previously-active register could be
`deprecated = true` upstream). Stewards who prefer a non-UI
workflow can run this and edit JSON directly; the webapp loop is
the recommended path.

**SPA mode disambiguation.** The catalog-authoring UI (used to
build a `steward.project_data.json`) and the project-authoring UI
(used to build a researcher's `project_data.json`) share the same
underlying machinery but expose different controls. Mode is
selected by URL route: `/catalog` for steward-catalog authoring
(panels and `reg_monabundle` block hidden, `display_name` editor
collapsed), `/` (default) for project authoring (everything
visible). Loading a file with `panels` or `reg_monabundle` set in
`/catalog` mode is rejected with a clear error; loading a file
without them in `/` mode is fine (researchers may have an empty
panels list).

Per-steward `extensions` (e.g. SWECOV enabling a
`swecov.filters` namespaced block on projects) are deferred to
§14; the v1 surface is steward.toml + steward.project_data.json,
nothing more.

### 9.2 Stack

- **Backend**: FastAPI + Pydantic + REST.
- **Frontend**: Svelte 5 + Vite + TypeScript (bun-managed).
- **Server**: stateless; reg_meta SQLite mmap'd; in-memory catalog
  index.
- **OpenAPI** auto-generated by FastAPI; committed `openapi.json`
  snapshot-tested in CI.
- **TypeScript types** code-generated from `/openapi.json` via
  `openapi-typescript`; committed alongside the SPA source.
- Every route declares `response_model=` — enforced by lint.

### 9.3 API style: REST, not GraphQL or tRPC

REST has stable URLs cacheable at the CDN edge (Cloudflare), which
is the main cost-control lever. GraphQL POSTs aren't edge-cacheable;
tRPC requires TS on both ends. Neither fits.

### 9.4 No auth, cost protection instead

The data is public-ish registry metadata. There's no user-private
state on the server (project files live in the browser). "Auth"
here is really cost protection:

- Cloudflare in front (free tier): edge caching, DDoS shielding,
  per-IP rate limits.
- Aggressive HTTP cache headers on all read endpoints (reg_meta
  only changes when rebuilt). Specifically:
  - `/api/catalog/*` and `/api/context`: `ETag:
    "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"` plus
    `Cache-Control: public, max-age=86400, must-revalidate`. The
    body-hash component is what makes `If-None-Match` per-URL
    coherent: every URL gets its own ETag value. The
    `reg_meta_version` + `steward_id` prefix isn't strictly needed
    for correctness (the hash already disambiguates) but it keeps
    ETags human-debuggable when inspecting cache headers, and
    invalidates the whole keyspace on either axis when one
    changes (mass body-hash churn does the same job).
  - `/api/docs/*`: same scheme.
  - Write endpoints (`/api/project/*`, `/api/bundle`, `/api/kit`)
    do not set ETag; `/api/bundle` and `/api/kit` are pure
    functions of input and could be content-addressed (deferred).
- Per-endpoint rate limit on actual-work endpoints (`/api/bundle`,
  `/api/kit`, `/api/project/validate`, `/api/project/order`),
  capped at e.g. 30 req/min/IP at the FastAPI layer — separate
  from and stricter than the edge-cached catalog reads.
- **Body-size cap on write endpoints.** Cloudflare drops requests
  with body > 1 MB at the edge; FastAPI enforces the same cap on
  any that slip through (e.g. direct origin hits). 1 MB matches
  the bundle-output budget (§12) and is comfortably larger than
  any plausible `project_data.json` + companion codes file (the
  200-column load-test fixture in §12 lands at tens of KB).

Rate-limit bucketing is **IP-only** in v1. A localStorage session
token would let us bucket per-browser (helpful behind NAT) but
adds a fingerprinting surface for what is currently anonymous
public data; if a steward later needs finer-grained limits, layer
it in then.

Real auth is a v2+ concern, layered on if a steward ever needs
private data.

### 9.5 API surface (v1)

All endpoints are versioned under `/api/`. Read endpoints are
edge-cacheable; write endpoints (`POST` for the file-builders) are
not. Catalog browse paths use FQID segments directly — the
hierarchy embeds in the URL rather than passing FQIDs as encoded
query strings.

**Context / deployment**

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/context` | Deployment identity, branding, build info. Called once on SPA boot. |

**Catalog** (read; cacheable). The FQID hierarchy is the URL
hierarchy: one endpoint serves every level, dispatching on
segment count. Partial paths return the entity at that level plus
its immediate children; the leaf returns the terminal entity with
no `children`. Search stays separate (it's a distinct operation).

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/catalog` | Top-level: lists every provider exposed by the steward catalog. `{kind: "root", children: [{kind: "provider", slug: "scb"}, ..., {kind: "classification-root", slug: "class"}]}`. |
| GET | `/api/catalog/{fqid:path}` | Single endpoint covering every node in the hierarchy. Response shape: `{kind, entity, children?}`. The `kind` discriminates: `provider` (1 seg), `register` (2 seg), `register_variant` (3 seg), `register_version` (4 seg), `binding` (5 seg, leaf), `classification-root` (`class`, 1 seg), `classification-name` (`class/<name>`, 2 seg), `classification` (`class/<name>/<version>`, 3 seg, leaf). `children` is omitted on leaves. Subsumes the previous per-kind detail endpoints and `/api/catalog/resolve`. |
| GET | `/api/catalog-search?q={query}&kind={register\|variable}` | FTS across registers and variables (delegates to reg_meta's FTS5 indexes). Separate path so the catalog endpoint stays single-purpose. |

**Documentation** (reg_meta docs DB; read; cacheable)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/docs/search?q={query}` | FTS over parsed register-documentation markdown. |
| GET | `/api/docs/get/{provider}/{register}` | Register-level documentation. |
| GET | `/api/docs/get/{provider}/{register}/{variable}` | Variable-concept-level documentation. |

**Project** (write; no rate-limit-free cache)

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/project/validate` | Validates a `project_data.json` document; returns structural, namespaced-block, and semantic errors/warnings (§6.8). |
| POST | `/api/project/order` | Renders the steward's order export. Default template (v1): CSV with columns `provider,register,variant,period,variable,display_name`, one row per spec column. `period` carries whatever the source's `register_version` declares (`2018`, `2018-01`, `HT2020`, …). Stewards that don't ship an `order_template` inherit this default. Custom templates (IFAU spreadsheets, SWECOV PDFs) replace the body via `stewards.order_template`. |
| POST | `/api/bundle` | Builds the Python MONA bundle embedding the supplied `project_data.json`. Pure function of the input (no server-side state, no steward config injection); response cacheable by content-hash. Returns `.py` as `application/octet-stream`. |
| POST | `/api/kit` | Builds a generation kit (zip) from `project_data.json` + `project_data.stats.json`. Dereferences classification FQIDs and binding FQIDs into a sibling `project_data.codes.json` (§6.6). |

Realign-patch application is **client-side only** — the SPA reads
`project_data.realign.json` against the in-browser project state.
No `/api/realign/apply` endpoint.

The OpenAPI spec is the canonical contract; the table above is
prose for the spec doc, not the source of truth.

### 9.6 Domain types vs Pydantic models

- `reg_meta`, `reg_schema`, `reg_monabundle`, and `reg_mockdata` all
  use plain dataclasses on their library surface. No Pydantic dep on
  the library side — keeps them importable from any context (Jupyter,
  scripts, the bundle on MONA).
- `reg_webapp/backend/` defines Pydantic *response models* that wrap
  or project the domain types. The mapping is explicit, one function
  per endpoint.

### 9.7 Project file persistence (SPA)

Projects live in the browser. The SPA owns the load/save UX; the
backend has no project store. There is no multi-user experience —
no sharing, no presence, no realtime sync. Projects move between
users as `project_data.json` files exchanged out-of-band (email,
git, Slack).

The flows:

- **Autosave to IndexedDB.** Every change to in-browser project
  state is debounced (e.g. 500 ms) and written to IndexedDB under
  a per-project key. On reload, the SPA restores the most recently
  opened project. IndexedDB is local to the browser profile;
  clearing site data wipes it.
- **Multi-project list.** The SPA's home screen lists all
  IndexedDB projects with name + last-modified timestamp. New /
  open / duplicate / delete actions are all client-side.
- **Download to file.** Explicit "Download `project_data.json`"
  button writes the current spec to the user's filesystem. This
  is the durable copy — the user commits it to their project repo.
- **Open from file.** Dropzone or file-picker accepts a
  `project_data.json`; it's parsed, validated structurally, and
  loaded into IndexedDB as a new project (or overwrites an
  existing one with the same `name`, with a confirm dialog).
- **Unsaved-changes warning.** If the in-browser state has
  diverged from the last downloaded file, the SPA shows a
  persistent "unsaved changes since last download" indicator in
  the header and a `beforeunload` warning on window/tab close.
  Closing the tab with autosaved-but-not-downloaded state is
  recoverable from IndexedDB on the same browser; closing with
  cleared site data is not.

Server-side projects, shareable URLs, and realtime collaboration
are all v2+ concerns layered on if a steward ever needs them.

## 10. Post-refactor allocation of current mdw machinery

The refactor pulls today's `mock_data_wizard` apart into two
packages — `reg_monabundle` (MONA-bound bundle build + runtime)
and `reg_mockdata` (local mock generation). Each existing piece
of machinery needs to land somewhere — `reg_monabundle`,
`reg_mockdata`, `reg_webapp`, `reg_schema`, or deleted. This
section is the design's most operationally critical and deserves
the most scrutiny.

### Lands in `reg_monabundle` (MONA bundle, build + runtime)

- **Bundle builder** (`reg_monabundle.build`). Pure-python file
  concatenation + JSON embed; called by `reg_webapp` from
  `POST /api/bundle`.
- **Bundle runtime** (`reg_monabundle.runtime.realign`,
  `reg_monabundle.runtime.extract`). The realign and extract
  phases that run on MONA (§7). Heavy deps (duckdb, pyodbc) live
  here behind the `runtime` extras group.
- **PII scanner** (`reg_monabundle.scan`, replacing today's
  `scan.write_export`). Runs on MONA before the bundle writes
  anything to disk. Defense-in-depth on top of
  summarize-by-construction. Non-negotiable safety net; lives where
  the bundle is. Pure-python; usable from `reg_webapp` if needed,
  also exposed as `python -m reg_monabundle.scan <path>` for
  ad-hoc audits.
- **SQL→spec-type compatibility map** (`reg_monabundle.types`).
  Drives realign-time mismatch detection (§7); also imported by
  `reg_webapp` for realign-review UI display.
- **Disclosure-control thresholds** (v1: `SUPPRESS_K`,
  `SMALL_POP_MULT`; see §6.5 for values). Apply at extract time
  in the bundle. Library defaults are `reg_monabundle`'s;
  per-column overrides come from the spec's
  `reg_monabundle.column_options`. Steward config does not
  influence these — the spec is the sole source of run-relevant
  configuration. Future thresholds (date jitter, numeric noise)
  go in the same place once they exist.
- **CSV typing machinery** (all-varchar reads, semantic → DuckDB
  cast map, opaque auto-promotion, materialisation threshold).
  Operational detail of the extract runner. Documented in
  `reg_monabundle/DESIGN.md`; the spec doesn't need to repeat it.
- **`reg_monabundle.validate_block`** — validator for the
  `reg_monabundle` namespaced block (§6.5, §6.8.2). Same code
  runs in `reg_webapp` at bundle-build time and inside the bundle
  on MONA at load time.

### Lands in `reg_mockdata` (researcher's local CLI)

- **`reg-mockdata generate`** — local CSV generation from the kit. Reads
  `project_data.json` + `project_data.codes.json` +
  `project_data.stats.json`; writes mock CSVs. Uses numpy + duckdb
  via the `runtime` extras group.
- **`reg-mockdata compare`** — validates locally-stored mock CSVs against
  the spec's column lists. Rewired to read `project_data.json`
  instead of the legacy manifest.
- **Population spine** logic (see below). The hardcoded
  `SPINE_VARIABLE_SLUGS` set is consulted at generate time.

### Moves to `reg_webapp` / `reg_schema`

- **Value-code drift warnings** (codes in stats not present in the
  pinned value set). Run at kit-build time in `reg_webapp`,
  surfaced as a warning block in the kit's README and in the SPA.
  Also re-run defensively by `reg-mockdata generate` against the kit's
  codes; warnings go to stderr.
- **Classifier chain** (id-name / reg_meta evidence / sql_type /
  fallthrough). Replaced by `reg_webapp`'s variable-list authoring —
  the user picks types deliberately against reg_meta's data, and
  the spec carries them. No automated classification at extract
  time.
- **Editor API** (`mock_data_wizard.editor`) — concurrency tokens,
  group views, panel detection, etc. Replaced by `reg_webapp`'s
  in-browser project state. The Python editor module is deleted
  at §15 step 7.
- **Local HTTP server** (`mock_data_wizard.server`, current
  `mock-data-wizard ui`). Deleted; `reg_webapp` covers it.

The current **CVID picker** (tiered year/name/code-set scoring
across multiple cvids per variable) becomes unnecessary under
FQIDs: a binding FQID resolves to exactly one `variable_instance`
row, which directly carries the cvid via reg_meta. Picking is
replaced by deterministic resolution. The picker module is
deleted, not moved (§15 step 7).

### Composite entity_key and time_key support

The panel schema (§6.4) allows composite `entity_key` (firm ×
workplace, household × person) and composite `time_key` (year ×
quarter). This is meaningful new behaviour split across both
packages:

- **`reg_monabundle.runtime.extract`** —
  `COUNT(DISTINCT entity_key)` becomes
  `COUNT(DISTINCT (col_1, col_2, ...))`; `GROUP BY <time_key>`
  becomes `GROUP BY (...)`. Tuple periods replace scalar periods
  in `by_period`.
- **stats schema** (emitted by extract) — `n_entity_ids` semantics
  unchanged but the underlying distinct-tuple count is a
  composite. `period` becomes `int | string | (int | string)[]`.
- **`reg_mockdata` generate** — the shared id pool is keyed by
  tuple rather than scalar; the deterministic shuffle generates
  shuffled tuples. Per-row column generation for column-member
  time_keys produces composite period values consistently.

Single-key panels keep working unchanged — the polymorphism in the
schema means a `string` value for `entity_key` and a scalar
`time_key` continue to be valid inputs. Composite is additive.

### Population spine

Today the toolkit maintains a **population spine**: birth-invariant
attributes (Kön, Födelseår, Födelselän, Födelseland) are generated
once per individual and reused across files, so the same person
doesn't have different sex in different files. Spine-eligible
variables today are a hardcoded set of reg_meta `var_id`s; without
reg_meta, the spine is empty.

Post-refactor: spine eligibility lives in `reg_mockdata` as a
hardcoded set of **variable-slug stems** — the last segment of a
binding FQID:

```python
# reg_mockdata.spine
SPINE_VARIABLE_SLUGS = {"kon", "fodelse-ar", "fodelse-lan", "fodelse-land"}
```

At generate time, `reg_mockdata` inspects each column's `name`
(binding FQID) and applies spine semantics when the trailing
variable slug matches. This works across providers and registers
automatically: `scb/lisa/individer-15plus/2018/kon`,
`scb/rtb/folkbokforda/2019/kon`, and `sos/par/sv/2020/kon` are all
the same concept for spine purposes. Cross-rename equivalence (the
curated `same_as` graph in reg_meta; §5.5) is *not* consulted at
generate time — the kit is freestanding from reg_meta — but
`reg_webapp` can use it to verify that all the project's "Kön"
columns share a canonical concept before kit-build. Whether the
kit-build step should *emit* a warning when `same_as` would have
grouped spine-eligible bindings that the slug-stem rule doesn't
(or vice versa) is tracked under §14's "`same_as` rendering at
generate time" open issue.

If a future project needs to extend the spine (e.g. a migration
study adding Migrationsår), the override would go in a future
`reg_mockdata` namespaced block in the spec, not in steward
config. Schema-design of that override is deferred until a
concrete project requires it.

**Cross-provider concept linkage** is the same kind of deferred
problem: v1's slug-stem-only rule (`/kon` matches `/kon` across
all providers) requires that concept-equivalent variables share a
stem across providers. That's fine for the v1 norm — SCB-only
projects, or projects against SCB-curated stewards. The day a
project mixes SCB's `kon` with another provider's `sex` for the
same person, the generator will silently produce inconsistent
draws. The remediation will live in the same future `reg_mockdata`
namespaced block (e.g. `spine_groups`: explicit lists of bindings
or stems to treat as person-equivalent). The slug-stem rule remains
the default; the override is the escape hatch.

### `mdw update`

`maintain update` (downloads the latest reg_meta DB and docs DB
assets) is a `reg_meta` concern. After the refactor neither
`reg_monabundle` nor `reg_mockdata` has a reg_meta dep, so
`mdw update` is deleted; users run `reg-meta maintain update` to
keep their local reg_meta current.

### Removed wholesale

- `mock_data_discovery.json` — no discover trip.
- `manual_columns` (in old `mock_data_config.json`) — webapp owns
  authoring, no need for a "user manually overrode this cell" side
  table.
- `discover_hash` — same reason.
- mutator concurrency (snapshot tokens, fcntl locks) — webapp owns
  authoring in IndexedDB.
- `mdw classify` / `mdw enrich` modules — webapp drives type
  selection at authoring time.
- `mdw scan` standalone CLI — replaced by
  `python -m reg_monabundle.scan <path>` for the rare ad-hoc
  audit case (PII scanning is otherwise automatic inside the
  bundle).

## 11. What changes from today

| Module / behavior | Fate |
|---|---|
| reg_meta object model | Provider promoted to first-class; FQID grammar introduced; `slug` columns added to register/register_variant/classification; synthetic `_default` variant for variant-less registers (§5) |
| reg_meta slug curation | New `reg_meta_build/fqid_slugs/*.toml` files committed to repo; grow-only; CI-enforced immutability (§5.4) |
| reg_meta library API | New typed `Catalog.resolve(fqid)` entry point; webapp imports through this surface only (§5.8) |
| `mock_data_wizard` Python package | Split into `reg_monabundle` (MONA bundle build + runtime + scan + types) and `reg_mockdata` (local mock generation + compare). See §4 and §10. |
| `mock_data_wizard/server.py` (local HTTP server) | Deleted after migration to `reg_webapp` |
| `mock_data_wizard/editor.py` (mutator API) | Deleted; `reg_webapp` owns authoring (§15 step 7) |
| `mock_data_wizard/classify.py` (classifier chain) | Deleted at §15 step 7 (alongside `editor` / `server`) — once the webapp owns type selection, the classifier is dead code |
| `mock_data_wizard/enrich.py` (reg_meta lookups at generate) | Codes come from `project_data.codes.json` instead |
| `mock_data_wizard/registers.py`, `cli.py` reg_meta cmds | Deleted (equivalents live in `reg_meta`) |
| `mock_data_wizard/web/` (Svelte UI) | Migrates to `reg_webapp/frontend/`, then deleted |
| `mock_data_discovery.json` | Deleted; replaced by realign patch |
| `mock_data_config.json` | Renamed to `project_data.json`; schema owned by `reg_schema`; column `name` becomes a binding FQID; source `register`+`year` collapse into `register_version` FQID |
| `mock_data_stats.json` | Renamed to `project_data.stats.json` |
| `mdw` namespaced block in spec | Renamed to `reg_monabundle`; owner is `reg_monabundle.validate_block` (§6.5) |
| `value_set_version` strings (`Kon@2023`, `SUN@2020`) | Replaced by classification FQIDs (`class/sun/2020`) on the column; ad-hoc codes inlined in `project_data.codes.json` by binding FQID (§6.6) |
| Categorical codes at generate time | After kit-build all codes live inline in `project_data.codes.json` regardless of source — classification FQIDs are dereferenced at kit-build, ad-hoc inline sets pass through; one lookup path post-kit (§8) |
| `regmeta maintain build-db` subcommand | Moves to new `reg-meta-build` CLI (binary; package `reg_meta_build`) |
| `reg-meta maintain update` | Stays in `reg_meta`; the canonical "freshen everything" command |
| `mdw update` | Deleted; users run `reg-meta maintain update` |
| Population spine | Lives in `reg_mockdata`; matches binding FQIDs by variable-slug stem (§10) |
| CVID picker | **Deleted.** FQID resolution replaces tiered scoring (§10) |
| PII scanner | Lives in `reg_monabundle.scan`; runs inside the bundle on MONA |
| Standalone `mdw scan` CLI | Deleted; replaced by `python -m reg_monabundle.scan <path>` for ad-hoc audits |
| `mdw compare` | Kept in `reg_mockdata` as `reg-mockdata compare`; rewired to read `project_data.json` |

`reg_mockdata` post-refactor: `reg-mockdata generate` + `reg-mockdata compare` only.
`reg_monabundle` post-refactor: bundle build + bundle runtime
(realign + extract) + PII scanner + type compatibility map. Both
substantially smaller than today's `mock_data_wizard`.

## 12. Future-proofing constraints

Not preemptive — just hygiene that's good anyway and keeps options
open:

- **OpenAPI as canonical contract.** `reg_webapp/backend/openapi.json`
  is committed; CI runs `make openapi-refresh-check` which
  regenerates the file and `git diff --exit-code`s against the
  committed copy. A pre-commit hook runs `make openapi-refresh` and
  stages the result, so devs touching API code don't repeatedly hit
  a red CI for forgetting to refresh; the CI check stays as the
  safety net for anyone who skipped the hook. Intentional API
  changes are inspected in the PR diff like any other code change.
  The TypeScript types (`openapi-typescript`-generated) are kept in
  sync the same way (codegen step in the SPA build, snapshot-tested
  in CI), so any drift surfaces in the same PR. A future Go/Rust
  port of the query API reproduces the spec; clients are
  unaffected.
- **Build / runtime cleanly separated.** `reg_meta` (query) is small
  and pure; `reg_meta_build` is operator-side. A future port
  replaces query only; build stays Python.
- **Server is stateless.** No process-local caches that change
  behavior across requests, no Python-specific tricks.
- **No Pydantic creep into core libraries.** Already covered above;
  reinforced here.
- **Performance budget (v1 targets).** Starting points, revised
  after the first load-test pass:
  - `/api/catalog/*` (read, edge-cacheable): **p95 ≤ 200 ms** at
    the FastAPI layer (cache miss); edge-cached responses are
    sub-ms.
  - `/api/project/validate`, `/api/project/order`: **p95 ≤ 1 s**.
  - `/api/bundle`, `/api/kit`: **p95 ≤ 5 s** (file packaging +
    amalgamation; larger projects dominate).
  Load-test corpus: a synthetic 200-column project_data.json
  fixture (committed under `reg_schema/test_corpus/`) plus the
  SWECOV reference catalog. Initial pass before §15 step 12;
  regressions fail CI thereafter.
- **Bundle size budget (v1 target).** The MONA bundle's emitted
  `.py` is capped at **1 MB** in v1. The bundle is uploaded
  through MONA's GUI per round-trip; budget keeps the upload
  responsive and forces deliberation before adding heavy
  amalgamated code. Baseline measured at §15 step 5; verified on
  a real MONA test the same step; regressions fail the bundle-build
  test thereafter.
- **Cross-package version compatibility.** Five Python packages
  (`reg_meta`, `reg_meta_build`, `reg_schema`, `reg_monabundle`,
  `reg_mockdata`) plus the webapp. Coordination rules:
  - `reg_webapp` **pins exact versions** of its runtime
    dependencies (`reg_meta`, `reg_schema`, `reg_monabundle`).
    These three move together — the webapp container is the
    integration point.
  - `reg_meta_build` releases independently; no runtime consumer
    pins it (it produces the DB asset that `reg_meta` fetches via
    `reg-meta maintain update`).
  - `reg_mockdata` floor-pins `reg_schema` (lowest compatible
    minor) so kits authored against newer webapp versions still
    generate on a slightly-older local install.
  - Schema breakage is signaled by `project_data.json`'s
    `schema_version`; per §13, v1 doesn't ship migration shims —
    `reg_mockdata` refuses to load an incompatible kit with a
    clear error.

## 13. Migration as policy

Per CLAUDE.md, this repo's tools are **early-stage with a small
group of testers**; breaking changes are acceptable and migration
shims are explicitly out of scope. `schema_version` exists on
`project_data.json` so future selves *can* migrate, but the
current iteration does **not** require migration scripts,
deprecation wrappers, or backwards-compatibility shims. Breaking
changes are clean breaks; testers re-author affected projects.
The migration question is revisited only when the toolkit
graduates to a wider user base.

## 14. Open / deferred decisions

- **LISA composite-source presentation in the catalog UI.** §5.6
  resolves the data-layer problem (consumer-side bindings are
  materialized at build time with `via_source_id` edges back to
  the canonical source instance). What remains is a UI question:
  when a user is authoring a LISA variable list and the catalog
  knows the variable originates in RTB, how is the lineage
  surfaced — as a hover tooltip, an inline note, a "see also"
  panel? Deferred to webapp authoring-UI design. See memory
  `project_reg_meta_lisa_columns_gap`.
- **Order export grammar — steward-specific templates.** v1's
  default CSV is decided (§9.5 `/api/project/order`); pluggable
  steward templates (IFAU spreadsheets, SWECOV PDFs) need concrete
  protocol definition (input contract, template language, output
  MIME type) before stewards 2 and 3 go live.
- **Chronological semantics on literal periods.** The
  `{"period": ...}` object form (§6.4) is a natural extension
  point for a future `kind` field (`"year_month"`,
  `"academic_term"`, `"quarter"`) so the generator can impose
  chronological ordering and inter-period continuity at mock-data
  time. Not designed now; the schema is forward-compatible.
- **Variable slug source on rename.** §5.3 picks "latest
  kolumnnamn alias" as the auto-derived slug. If SCB renames a
  column between editions and the curator hasn't yet added a
  `same_as` link, the auto-rule will produce a new slug for the
  later editions while the earlier ones keep the old slug.
  Behaviour during the gap is fine in principle (rename = new
  concept by default) but the operational rhythm — how often
  curators review newly-shipped renames — is undecided.
- **Per-steward repo autonomy.** v1 hosts every steward's config
  in this monorepo (§9.1). Stewards versioning their own catalogs
  in their own repos — useful if IFAU/SWECOV ever operate their
  own deployments — would reintroduce the external-repo build
  wiring (build-arg manifest, pinned commits) that v1 sheds. Not
  needed until a steward asks for it.
- **`same_as` rendering at generate time.** §10's population spine
  doesn't consult `same_as`; the webapp does, at authoring time.
  Whether `reg-mockdata generate` should also normalize via `same_as`
  (using a snapshot shipped in the kit) — i.e. should two
  cross-rename-equivalent columns share the spine row? — is a
  v1.x question, not v1.
- **Realign patch lifecycle.** Whether the webapp's realign-review
  UI should write the accepted patch back into git automatically
  (via a Download-and-replace flow) or just produce a new
  `project_data.json` the user manually replaces.
- **Path-based CI filters.** Specifically what triggers webapp
  container builds vs Python package CI.
- **Steward `extensions` toggles.** §9.1 mentions per-steward
  feature flags (e.g. SWECOV's `swecov.filters` namespaced block).
  Concrete shape deferred until SWECOV onboarding.

## 15. Migration order (sketch)

Not a checklist. A narrative of what blocks what, intended to
inform sequencing decisions. The order below is the load-bearing
dependency story, not the day-by-day plan.

**Preconditions for step 1.** The §5 FQID grammar, the §5.6
consumer-side binding materialization for composite registers, and
the §6.5 `column_options` keying are all design-locked in this
document. Step 1 can start.

1. **reg_meta identifier rebuild (§5).** Split into five sub-steps
   that can be reviewed/merged independently:
   - **1a — Schema additions.** Provider table promoted to first-class;
     `slug` columns added to `register`, `register_variant`,
     `classification`; synthetic `_default` variant emission for
     variant-less registers (§5.1, §5.2 reserved-slug rule). Existing
     query commands continue to work — the new columns are nullable
     until 1c populates them.
   - **1b — FQID grammar + emission + resolve.** FQID parser/emitter
     and `Catalog.resolve()` land; existing query commands gain FQID
     output alongside their legacy formats.
   - **1c — Slug TOML bootstrap.** `reg-meta-build seed-slugs` emits
     starter TOMLs; **time-boxed to 1 week** of hand-review;
     `reg-meta-build precheck-slugs` lists any source IDs missing a
     slug entry (cleaner failure mode than a full build attempt).
   - **1d — CI immutability snapshot.** Slug-grow-only snapshot test
     (§5.4) enabled **after 1c merges** so the mechanism is wired in
     and exercised. The CLI and CI gate reject non-additive snapshot
     changes from day one — see §5.4 *Activation* for why this is
     "exercise the mechanism" rather than "protect external
     artifacts" before v1, and for the manual snapshot-regeneration
     workaround available pre-v1. The gate's protective semantics
     start binding at the first v1 release, when the in-tree
     snapshot becomes the permanent baseline external consumers
     depend on.
   - **1e — Consumer-side binding materialization (§5.6).** LISA-via-RTB
     and other composite-source bindings get materialized at build
     time with `via_source_id` edges. Closes the data-layer side of
     the §14 LISA presentation issue.

2. **`reg_meta_build` carved out of `reg_meta`.** Pure mechanical
   split; both packages keep working. Releases independently. Done
   alongside (1) because the rebuild touches build code most.
3. **`reg_schema` package created** with the `project_data.json`
   schema + structural validator, referencing reg_meta FQIDs
   throughout (column `name` = binding FQID, `value_set` =
   `class/...` FQID or absent + codes inlined in
   `project_data.codes.json`). Composite key support in the schema
   from day one; runtime support follows in step 10b. Importable
   by `mock_data_wizard`. No webapp yet.
4. **`mock_data_wizard` adopts the `project_data.json` shape and
   the new bundle layout.**
   - Config file is renamed; consumes `reg_schema`.
   - Single-file bundle with embedded config (the modes are still
     the old `--mode=...` shapes here; merged-mode lands in 10a).
   - Discover step still exists; classifier still exists.
   - **Test-fixture rewrites are part of this step**: every
     `mock_data_config.json` fixture is rewritten to
     `project_data.json`, including the binding-FQID columns. The
     fixture corpus is non-trivial and should be in the step's
     estimate.
5. **Carve `reg_monabundle` out of `mock_data_wizard`.** Bundle
   builder + bundle runtime + PII scanner + type compatibility map
   move into the new package. `mock_data_wizard` keeps generate +
   compare + (for now) classifier. `reg_webapp` doesn't exist yet
   so the bundle-builder is invoked from the mock_data_wizard CLI;
   it just lives in a different module.
   - **Verification gate:** measure the bundle's output `.py`
     size against the 1 MB v1 budget (§12) on a real MONA test;
     fail the step if exceeded so the budget bites before it
     compounds.

**Step 5.5 — Shared validator test corpus.** `reg_schema/test_corpus/`
created with golden `(input.json, expected_ValidationResult.json)`
pairs. Three consumers wired up: `reg_schema`'s Python tests read
the corpus directly; the bundle build amalgamates a corpus-runner
self-test that runs on MONA load; the SPA's TS test suite reads
the same JSON. This is the single artifact that makes §6.8.0's
`ValidationResult` shape coherent across the three runtimes.

<!-- markdownlint-disable-next-line MD029 -->
6. **Webapp scaffolds: backend + frontend skeleton.** Empty UI,
   OpenAPI plumbing, the `global` steward dir wired up, reads
   reg_meta through the FQID-keyed catalog API. `reg_webapp`
   imports `reg_meta`, `reg_schema`, `reg_monabundle`. Steward
   configuration shape (`steward.toml` + reused
   `project_data.json` for the catalog; §9.1) is finalised here
   so step 11's catalog authoring is unblocked.

**Step 6.5 — Containerize, Cloudflare, `global` deployment up.**

- `reg_webapp` Dockerfile runs `reg-meta maintain update` at
  image build time to bake the matching reg_meta release's DB
  into the image layer (§4).
- Cloudflare configured in front: edge caching with the §9.4 ETag
  scheme, per-IP rate limits.
- **Edge-cache validation gate:** run a small load through
  Cloudflare to confirm slash-bearing FQID paths round-trip
  cleanly through the edge cache before publishing the OpenAPI.
  If the edge cache mangles them, fall back to a query-string
  form *before* the OpenAPI is committed —
  `/api/catalog/{fqid:path}` is the chosen form (§9.5), and
  changing it post-publish is expensive.
- `global` deployment goes live serving `/api/catalog`,
  `/api/context`, and the bare-bones SPA — no authoring UI yet.

<!-- markdownlint-disable-next-line MD029 -->
7. **Webapp authoring of `project_data.json`.** Hard cut from
   `mock_data_wizard.editor` / `mock-data-wizard ui` to webapp
   authoring. `mock_data_wizard.editor`, `mock_data_wizard.server`,
   **and `mock_data_wizard.classify`** are all deleted the same day
   — once the webapp owns type selection the classifier is dead
   code. No parallel run, no shim, per CLAUDE.md and §13. Testers
   re-author affected projects.

**Step 7.5 — `global` dogfood (2 weeks).** Testers exercise the
full author → bundle → realign → re-author loop against the
`global` deployment before kit-build piles on top. Paired with
the 200-column load-test fixture (§12) for realign-UX stress.
No new code lands in step 8 until this window's findings are
addressed. The `global` deployment is the staging environment;
no separate staging tier.

<!-- markdownlint-disable-next-line MD029 -->
8. **Webapp kit-build** (`POST /api/kit`) — value-set
   dereferencing: classifications resolve to inline `codes` keyed
   by classification FQID; ad-hoc categorical columns get inline
   codes keyed by binding FQID. Generation kit format finalised
   here.
9. **`mock_data_wizard` renamed to `reg_mockdata`; reg_meta dep
   deleted.** What remains is purely `reg-mockdata generate` +
   `reg-mockdata compare`. `reg_mockdata` reads pinned codes from
   the kit. Population spine ships as a hardcoded set of binding
   FQID stems matched at generate time. The webapp does NOT depend
   on `reg_mockdata`. (Classifier deletion already happened at
   step 7.)
10. **Bundle merged-mode and composite-key support in
    `reg_monabundle`.**
    - **10a:** merged-mode (realign-then-extract; §7 redesign);
      discover deleted; type-mismatch detection via
      `reg_monabundle.types.is_compatible`. **`reg_monabundle.scan`
      updated for the new realign output and stats schemas** — without
      this the PII scanner runs against schemas it doesn't understand.
    - **10b:** composite entity_key/time_key support lands in
      `reg_monabundle.runtime` + `reg_mockdata` generate. Schema
      already accepted them at step 3; `reg_monabundle` rejected
      them at bundle-build until now.
11. **Steward catalogs** (ifau, swecov) authored: each steward's
    `steward.project_data.json` is built against the `global`
    deployment and committed into
    `reg_webapp/stewards/<id>/steward.project_data.json` in this
    monorepo. The existing Docker image rebuild picks them up; new
    hostnames are wired up at Cloudflare. Order export exists in
    CSV form (default template) for all three stewards.
12. **Per-steward order templates** and `extensions` toggles
    layered on as steward-specific grammar requirements emerge.

Around step 9-10, `REFACTOR_SPEC.md` dissolves: §5 moves to
`reg_meta/DESIGN.md`; §6 moves to `reg_schema/DESIGN.md`; §7 and
§10's bundle pieces move to `reg_monabundle/DESIGN.md`; §8 and
§10's generate pieces move to `reg_mockdata/DESIGN.md`; §9 moves
to `reg_webapp/DESIGN.md`; §1, §2, §4, §11, §12, §16 distill into
a slimmer `ARCHITECTURE.md` at the repo root.

## 16. Testing strategy

Testing is mentioned piecemeal across the spec; this section is
the consolidated view. Eight load-bearing test categories — none
are negotiable for v1.

- **Shared validator corpus** (§15 step 5.5). `reg_schema/test_corpus/`
  ships golden `(input.json, expected_ValidationResult.json)` pairs.
  **Scope: structural rules only** (§6.8.1) — these are the rules
  every runtime executes and the only ones that must stay aligned
  across Python and TS. Namespaced-block rules (§6.8.2,
  e.g. `reg_monabundle.validate_block`) are tested per owner,
  inside the owning package, since each owner has a single
  canonical Python implementation; semantic rules (§6.8.3) are
  tested inside `reg_webapp` since only the backend has reg_meta.
  Three consumers run the structural corpus:
  - `reg_schema` Python unit tests (the canonical implementation).
  - The bundle build amalgamates a corpus-runner that asserts at
    bundle load on MONA — catches drift between the durable Python
    code and the amalgamated copy.
  - The SPA's TS test suite reads the same JSON and asserts via the
    codegen'd types.
  The corpus is the single artifact that makes §6.8.0's
  `ValidationResult` shape coherent across runtimes.
- **FQID property tests.** Round-trip (parse → emit → parse equals
  identity); segment-count discrimination; reserved-slug rejection
  (`class`, `_default`); `same_as` traversal terminates (cycle
  detection); slug-immutability snapshot vs. last committed TOML
  state.
- **Bundle determinism.** `reg_monabundle.build(spec)` is a pure
  function of its input: building the same spec twice yields a
  byte-identical `.py`. Test in CI on a fixed-content fixture.
- **Kit reproducibility.** Same spec + same `project_data.codes.json`
  + same `project_data.stats.json` → same generation kit zip
  (deterministic ordering, no embedded timestamps). Test via
  hash comparison.
- **Steward catalog filtering.** Researcher project authored
  against `global` and validated against a narrower steward (e.g.
  `ifau`) emits the expected set of
  `fqid_outside_steward_catalog` warnings — codes + paths, not
  message strings. Pins the §6.8.3 caller-context contract.
- **MONA-shape integration.** A `sqlserver-linux` Docker container
  hosts a synthetic MSSQL instance with INFORMATION_SCHEMA-shaped
  fixtures; the bundle's realign and extract queries run against
  it end-to-end. Catches "everything is VARCHAR" assumptions, dialect
  quirks, and `pyodbc` driver issues without needing real MONA.
- **Per-deploy smoke tests.** Every deployment (`global`, `ifau`,
  `swecov`, …) has a golden response for `/api/context` and a
  shallow `/api/catalog` walk. Run on every container start; a
  failing smoke test halts the deploy.
- **PII scanner regression corpus.** Synthetic fixtures with
  embedded PII shapes (personnummer patterns, address-like text,
  free-text comment fields) that `reg_monabundle.scan` **must
  flag**. Grow-only — a missed flag in production becomes a new
  fixture row. The scanner change in §15 step 10a runs against
  this corpus to verify no regressions.
