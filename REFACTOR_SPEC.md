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

Searchable database of Swedish registry metadata. ~1.6 GB SQLite file
indexing ~100M value-code rows across hundreds of registers. Built by
parsing SCB CSV exports and Socialstyrelsen Excel deliveries
(`maintain build-db`); queried via CLI (`regmeta search`, `regmeta
get`, `regmeta resolve`, `regmeta docs ...`) and as a Python library.
Two SQLite databases: the main metadata DB (`regmeta.db`) and a
separate documentation DB (`regmeta_docs.db`) built from parsed SCB
PDFs. FTS5 indexes on both. Stable JSON output, structured errors,
meaningful exit codes — designed primarily for agent consumption.

This is the **authoritative knowledge layer**: "what variables exist
in LISA in 2018, with what value codes." Currently used by mdw's
classifier and enricher; also by humans/agents searching for variables
during research-question exploration.

### `mock_data_wizard` (mdw)

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
  panels, regmeta-driven classification). It's really a project-data-
  management tool wearing a mock-data-wizard label.
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
   them into a "generation kit." Run `mdw generate` locally to
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
  in the browser: searching registry metadata (`regmeta`), generating
  mock data from stats (`mock_data_wizard`). The web app's backend
  also imports these as libraries.
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

### Steward model — three deployments off one codebase

- **global** — full catalog spanning every agency regmeta indexes
  (SCB, Socialstyrelsen, Försäkringskassan, …); orders go to the
  relevant agency directly.
- **ifau** — subset of registers available through IFAU's warehouse.
- **swecov** — subset available through the SWECOV research program.

All three offer the same user experience; only the catalog they expose
and the order export they produce differ. Same FastAPI binary, same
Svelte build, three steward configs picked by URL. Hosted by the
author for now (no self-hosting requirement; IFAU and SWECOV are not
expected to operate the service themselves). No user accounts, no
server-side project storage — projects live in the browser plus the
researcher's git repo.

## 4. Target package layout

Monorepo, four Python packages + one webapp:

```text
registry-research-toolkit/
  regmeta/          # query lib + CLI + schema types (lean deps)
  regmeta_build/    # parsers, source modules, DB construction
  regproject/       # project_data.json schema, validators, diff
  mock_data_wizard/ # CLI-only: bundle on MONA + generate locally
  webapp/
    backend/        # FastAPI; depends on regmeta + regproject
    frontend/       # Svelte + Vite (bun)
    stewards/       # global.yaml, ifau.yaml, swecov.yaml + order templates
```

Dependency graph (no cycles):

```text
regmeta_build → regmeta
webapp        → regmeta, regproject, mdw
mdw           → regproject
regproject    → (none)
```

Each Python package releases to PyPI on its own tag (`regmeta/v*`,
`regmeta_build/v*`, etc.). Webapp ships as a container image on
`webapp/v*` tags.

### Why this split

- **regmeta vs regmeta_build**: different deps (query needs the
  sqlite3 stdlib; build needs CSV/Excel parsers), different release
  cadence, different operators. Enables a future offline-bundle
  scenario (`regmeta` + prebuilt SQLite, no build code shipped).
  Mirrors the build/runtime separation needed for a future Go/Rust
  port of the query layer.
- **regproject**: the `project_data.json` schema is bigger than mdw
  and shouldn't live there. Multiple consumers (webapp authors it,
  mdw reads it, future exporters read it). Tiny, focused, no regmeta
  dep — schema uses string IDs, resolution is the consumer's job.
- **mdw drops regmeta**: under the new flow, types come from the spec
  (authored against regmeta in the webapp), so mdw doesn't need
  regmeta in-process. MONA-side code becomes regmeta-free entirely.
- **webapp**: new home for everything UI. Replaces the current
  `mock_data_wizard/web/`, which lives until the webapp covers it.
- **webapp depends on mdw**: `POST /api/bundle` calls
  `mdw.bundle.build`, so the webapp imports mdw as a library. The
  edge is lightweight — mdw's bundle builder concatenates its
  runtime source files as data (string formatting + JSON embed),
  not as imported modules — so the webapp's container does not pull
  in mdw's heavy runtime deps (duckdb, pyodbc, numpy). Those only
  execute inside the bundle on MONA.

## 5. regmeta as the substrate

The refactor starts with regmeta. Every downstream artifact — the
`project_data.json` schema, the webapp's `/api/catalog/*` endpoints,
mdw's generation kit — references regmeta entities (registers,
variables, value codes). The contract between them is only as stable
as regmeta's identifier scheme. Today that scheme rests on the
empirical observation that "kolumnnamn doesn't change much," which
is not a regmeta-side invariant. Committed `project_data.json` files
in researchers' git repos depend on this contract, and breakage
manifests as silent rot, not loud errors.

This section defines regmeta's object model and the **FQID grammar**
that every downstream consumer uses to reference regmeta entities.
The grammar is anchored to the underlying providers' (SCB,
Socialstyrelsen) numeric IDs for stability, overlaid with curated
human-readable slugs for ergonomics.

### 5.1 Object model

Mostly unchanged from today's `regmeta/STRUCTURE.md`, with
**provider** promoted to first-class for multi-agency coverage:

| Concept | regmeta term | Notes |
|---|---|---|
| Data publisher | `provider` (NEW) | `scb`, `sos`, `forsakringskassan`, … |
| Statistical register | `register` | LISA, RTB, PAR. |
| Sub-table within a register | `register_variant` | LISA/Individer 15+, RTB/Folkbokförda. The "table" concept — does not nest further. Socialstyrelsen-side `Deldatamängd` rows map to this slot. |
| Periodic release of a variant | `register_version` | LISA/Individer 15+/2018. |
| Variable concept (register-scoped) | `variable` | "Kön in LISA" and "Kön in RTB" are different variables. Cross-register concept-merging is curation, not identity. |
| Variable in a specific version | `variable_instance` | CVID-bound. |
| Column header(s) | `variable_alias` | One instance can have multiple aliases. |
| Code list attached to an instance | `value_set` | Content-hashed for dedup. Internal to regmeta; never exposed by FQID. |
| Named versioned vocabulary | `classification` | SUN2020, SSYK2012, LKF — provider-independent. |

Population and `object_type` remain orthogonal context layers on
`register_version`; they do not participate in the FQID.

Registers without a sub-decomposition (Socialstyrelsen's LSS, BU,
SOL) get a synthetic `_default` variant during build so the schema
stays regular. The FQID emitter may elide `/_default/` for display.

### 5.2 FQID grammar

Every regmeta entity has a Fully Qualified Identifier — a stable,
`/`-separated string with strict positional grammar:

| Form | Meaning |
|---|---|
| `<provider>` | provider |
| `<provider>/<register>` | register |
| `<provider>/<register>/<variant>` | register_variant |
| `<provider>/<register>/<variant>/<period>` | register_version |
| `<provider>/<register>/<variable>` | variable concept |
| `<provider>/<register>/<variant>/<period>/<variable>` | variable binding |
| `class/<classification>/<version>` | classification |

Examples:

```text
scb                                                  provider
scb/lisa                                             register
scb/lisa/individer-15plus                            register_variant
scb/lisa/individer-15plus/2018                       register_version
scb/lisa/kon                                         variable concept
scb/lisa/individer-15plus/2018/kon                   variable binding
sos/lss/_default/2022                                version of variant-less register (elidable to sos/lss/2022)
class/sun/2020                                       classification (provider-independent namespace)
class/lkf/2012                                       classification
```

Period segments accept integer year (`2018`) or string period
(`2018-01`, `HT2020`) — the same forms `time_key` accepts in
`project_data.json`.

**Value sets get no FQID.** Two cases only for declaring a column's
codes:

- A named classification → reference by classification FQID
  (`class/sun/2020`).
- Anything else → inline the codes in `project_data.codes.json`,
  keyed by the **binding FQID** of the column. The content-hashed
  `value_set` row inside regmeta is a dedup optimization that
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
TOML files committed to the repo:

```toml
# regmeta/slugs/scb.toml
[register.34]
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
# regmeta/slugs/classifications.toml
[classification."SUN2020"]
slug = "sun"
version = "2020"
```

The build pipeline reads slug TOMLs alongside source CSVs/workbooks,
populates `slug` columns on `register`, `register_variant`, and
`classification`, and refuses to compile the DB if any source ID is
missing a slug entry ("RegisterId 99 in Registerinformation.csv but
no slug in regmeta/slugs/scb.toml"). A precheck step lists missing
slugs without trying the full build, for cleaner failure mode.

**Variables are auto-slugged.** The build derives a slug from the
latest kolumnnamn alias (lowercased, kebab-cased), and the TOML is
used only for exceptions — collisions or cross-edition continuity
claims after a SCB rename:

```toml
[variable."34.137"]
slug = "civilstand"               # explicit override
same_as = ["34.2999"]             # curated rename equivalence within LISA
```

A one-time `regmeta-build seed-slugs` command reads the current
data and emits starter TOMLs for hand-review. Estimated bootstrap
size: ~5 providers, ~100 registers, ~300 variants, ~20
classifications — ~400 curated entries plus thousands of
auto-derived variable slugs. Steady-state maintenance: a handful
of new entries per year as agencies add registers.

### 5.4 Slug immutability

Once a slug is published, it can never change. Committed
`project_data.json` files reference slugs; renaming a slug rots
every project that references it. Concrete rules:

- The slug TOML is **grow-only**: entries are added; entries are
  never deleted or renamed.
- Removed source IDs (a register dropped from a future delivery)
  are flagged `deprecated = true` but retain their slug forever.
- A typo in a slug is corrected by adding a new entry with a
  `replaced_by` link on the old one — never by editing in place.
- CI enforces these via a snapshot test comparing the current TOML
  to the last committed state; non-additive changes fail the build.

The same rule applies to every slug-bearing entity: register,
register_variant, variable, classification. Implication: every
FQID is safe to embed in `project_data.json` and in webapp-emitted
order artifacts indefinitely.

### 5.5 Variable identity: concept vs binding

The FQID grammar exposes two variable forms:

- **Concept FQID** (`scb/lisa/kon`) — register-scoped; refers to
  the same conceptual column across editions.
- **Binding FQID** (`scb/lisa/individer-15plus/2018/kon`) —
  edition-scoped; refers to the specific column-in-table-version.

`project_data.json` columns reference **bindings** by default —
researchers ordered a specific column from a specific year. The
concept form is available for cross-edition queries ("availability
of Kön across LISA years") and for catalog browsing in the SPA.

**Cross-rename equivalence.** When SCB renames a column between
editions and the curator wants to claim "same concept", an explicit
`same_as` field in the variable's slug entry links the renamed
forms. Default behaviour is conservative: a rename creates a new
concept; equivalence is curated case-by-case. This avoids silent
"same name = same concept" bugs when SCB redefines a variable
without renaming.

### 5.6 What this means for project_data.json

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
- The LISA composite-source gap (§14) is partly addressed: binding
  FQIDs resolve through regmeta's existing source-link edges
  (`variable.source_register_id`), so "variable documented under
  source register, not under LISA" becomes traversable rather than
  a validator warning.

### 5.7 Library API surface

The webapp's `/api/catalog/*` endpoints (§9.5) are thin wrappers
around a stable in-process regmeta API:

```python
from regmeta import Catalog

catalog = Catalog.open()                 # mmap'd SQLite
entity = catalog.resolve("scb/lisa/individer-15plus/2018/kon")
# returns a typed VariableBinding with .concept, .codes, .definition
```

Exact API design is regmeta's concern and belongs in
`regmeta/DESIGN.md` once the rebuild starts. The contract from
this spec's perspective: every FQID resolves through one entry
point; the webapp imports nothing else from regmeta's internals.

## 6. The shared schema: `project_data.json`

The load-bearing artifact connecting every step. Built on top of
the regmeta FQID grammar (§5). Single file, owns:

- Sources, each pinned to a `register_version` FQID
- Columns per source, each pinned to a **binding FQID** (regmeta
  identity) plus a `display_name` (SQL column header in delivered
  data)
- Panels (entity_key + member time_keys), keyed on `display_name`
  strings
- Per-column tunables under a namespaced `mdw` block
- Value codes via classification FQID references or held inline in
  sibling `project_data.codes.json`

### 6.1 Top-level shape

| Field             | Type           | Required | Description |
|-------------------|----------------|:--------:|-------------|
| `schema_version`  | string         | yes | Semantic version of the schema (e.g. `"1.0.0"`). |
| `steward`         | string enum    | yes | `"global"` / `"ifau"` / `"swecov"`. Identifies which deployment authored the file. |
| `regmeta_version` | string         | yes | Build hash of the regmeta DB used during authoring. Best-effort drift detection on later resolves. |
| `name`            | string         | yes | Project identifier (human-readable). |
| `sources`         | array<Source>  | yes | List of data sources (tables) in the project. |
| `panels`          | array<Panel>   | no  | Panel definitions over sources. Default `[]`. |
| `mdw`             | object         | no  | Namespaced block for `mock_data_wizard` settings. |

No other top-level fields are allowed; future additions must be
namespaced (e.g. `mdw:`, `swecov:`). A `value_sets` top-level
block from earlier drafts is removed — codes live in sibling
`project_data.codes.json` (§6.9), and classifications are
referenced inline on each Column.

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
| `register_version` | string (FQID)    | yes | regmeta register_version FQID: `<provider>/<register>/<variant>/<period>` (§5.2). |
| `columns`          | array<Column>    | yes | Columns to include. At least one. |

The Source replaces the v2 `(register, year)` pair with a single
FQID. Provider, register, variant, and period are all encoded —
no inference needed at validation time.

`where` (per-table SQL predicate) is **not** in the v1 baseline
schema. Cohort filtering inside the MONA bundle still happens at
the `sql_table` / `file_source` level in mdw's `configure()`,
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
| `display_name`    | string         | yes | Actual column header in the delivered data. Initially populated by the webapp from regmeta's `variable_alias.kolumnnamn`; modified by realign (§7) when project prefixes are applied (e.g. `LopNr` → `P1105_LopNr_PersonNr`) or by a user who renamed a column at order time. |
| `type`            | enum           | yes | One of `id`, `categorical`, `numeric`, `date`, `datetime`, `opaque`. |
| `id_subtype`      | enum           | no  | For `id` type: `integer` or `string`. Auto-detected from the data when omitted. |
| `numeric_subtype` | enum           | no  | For `numeric` type: `integer` or `double`. |
| `date_format`     | string         | no  | For `date` type: Python `strptime` pattern. Carries granularity — `"%Y"` is year-only, `"%Y-%m"` is month, `"%Y%m%d"` is day. Default `"%Y-%m-%d"`. |
| `datetime_format` | string         | no  | For `datetime` type: `strptime` pattern with time. Default `"%Y-%m-%d %H:%M:%S"`. Time zones are out of scope for v1. |
| `value_set`       | string (FQID)  | no  | For `categorical` type: a classification FQID (`class/<name>/<version>`). When absent, codes live in `project_data.codes.json` keyed by this column's binding FQID (§6.6). |

Two identifiers, two purposes: `name` (the binding FQID) is the
**regmeta identity** — used for validation, code-set lookup, and
cross-edition continuity. `display_name` is the **runtime data
column header** — what panel keys reference, what realign matches
against, what mdw's extract queries read.

#### The type set

Five general-purpose types plus `datetime`, deliberately compact:

- **`id`** — pseudonymized person/firm/family identifier (`LopNr`,
  `PeOrgNr`). Generation draws from a shared pool keyed by binding
  FQID concept-stem (§10, population spine).
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

Panel logic operates on delivered data, not on regmeta entities.
**Panel `entity_key` and bare-string `time_key` references resolve
against columns' `display_name` strings**, not binding FQIDs — the
join is over actual SQL column headers in the delivered tables.

| Field         | Type                          | Required | Description |
|---------------|-------------------------------|:--------:|-------------|
| `panel_id`    | string                        | yes | Unique panel identifier within the spec. |
| `entity_key`  | EntityKey                     | no  | Panel-level default entity-key column(s). At least one member must end up with an effective `entity_key` (default or override) for the panel to validate. |
| `time_key`    | TimeKey                       | no  | Panel-level default time-key. Same rule: each member must end up with an effective `time_key`. |
| `members`     | array<string \| PanelMember>  | yes | Member sources, optionally with per-member overrides. |
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
  `time_key: "AR"` is permitted. At runtime, mdw models a literal
  time_key as a synthetic constant-value column on the member's
  source, so merge / group-by logic is uniform across the panel.
  This is rare in practice but well-defined.

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

### 6.5 The `mdw` namespaced block

Settings consumed only by `mock_data_wizard`. **`regproject` treats
this block as opaque** — its structural validator checks only that
`mdw` (if present) is an object. mdw owns validation of the block's
contents, both at bundle-build time and at bundle load time on
MONA. Other tools (the webapp, future exporters) call mdw's
validator when they need to check it. This is the standard pattern
for namespaced blocks; future steward-namespaced blocks (e.g.
`swecov`) are owned by their respective packages the same way.

```json
"mdw": {
  "column_options": {
    "lisa_2018": {
      "Salary": {"suppress_k": 20}
    }
  }
}
```

`column_options[<source_name>][<display_name>]` is a dict of
per-column tunables, keyed by the source's internal handle and the
column's `display_name` (SQL header in delivered data). Known keys:

- `suppress_k` (int) — disclosure-control threshold for this column's
  frequency table. **Raise-only**: the per-column override may
  increase the threshold above mdw's library default (`SUPPRESS_K`)
  but never lower it. A typo'd low value would silently weaken
  disclosure control.

mdw uses a single library default for `SUPPRESS_K` and other
disclosure-control parameters; steward config does not override
them. This keeps the spec freestanding: bundle behavior is
determined by the spec + mdw's release version, with no out-of-band
steward configuration influencing runtime.

Future per-column option keys are added here as the need arises
(e.g. length caps for opaque generation, override of mdw's
auto-detected id_subtype). The block is open-ended; mdw validates
known keys strictly — unknown keys raise.

### 6.6 Value codes

Codes live in sibling `project_data.codes.json` — never in
`project_data.json` itself. The file is a flat, FQID-keyed object
with two keyspaces sharing one dictionary:

- **Classification FQIDs** (e.g. `class/sun/2020`): the canonical
  code list for that classification, dereferenced from regmeta at
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

A categorical column's `value_set` field selects which entry mdw
generation reads:

- `value_set: "class/<…>"` → reads `codes[value_set]`.
- (`value_set` absent, type=categorical) → reads
  `codes[<column binding FQID>]`.

The webapp populates `project_data.codes.json` at kit-build time
by dereferencing every classification referenced anywhere in the
spec and every ad-hoc binding (via regmeta). After kit-build the
project is **freestanding from regmeta**: a researcher who checks
`project_data.json` + `project_data.codes.json` +
`project_data.stats.json` into git can regenerate mock data years
later regardless of how regmeta evolves steward-side.

Kit-build errors loudly when a referenced FQID no longer resolves
in the current regmeta — "FQID `class/foo/2010` not found; closest
match: `class/foo/2012`." Deprecated entries (slug TOML
`deprecated: true`) resolve normally and emit a warning.

### 6.7 Resolution and the source-link graph

With FQIDs, column resolution against regmeta is direct: a binding
FQID either maps to a `variable_instance` or it doesn't. No alias
lookup, no fallback chain, no warning machinery.

The LISA composite-source problem (many LISA variables documented
under their source registers RTB, RAMS, …) moves from
*validation-time* to *catalog-browse-time*: regmeta exposes
source-link edges (`variable.source_register_id` and friends), and
the webapp's variable-list authoring UI surfaces lineage when the
user is choosing a column ("This variable in LISA originates from
RTB"). The user picks the binding under the register they actually
ordered — LISA-side if they ordered LISA, RTB-side if they ordered
RTB — and the binding FQID written to the spec is unambiguous.

The validator therefore:

1. Checks that `name` is a structurally well-formed binding FQID.
2. Checks that its first four segments equal the source's
   `register_version` FQID.
3. Checks that the FQID resolves to a known `variable_instance` in
   regmeta (hard error on miss).

Where regmeta records a `same_as` link between two variable
concepts (curated cross-rename equivalence; §5.5), resolution
follows the link so that an old binding FQID continues to resolve
after a SCB rename. The presence of a `same_as` traversal is
reported as info, not warning — it's expected behaviour.

### 6.8 Validation rules

Validation is split across three layers by what data each rule
needs to run. The contract matters because the spec is validated in
three execution contexts (browser SPA, webapp backend, MONA bundle)
with very different dependency availability — only the webapp
backend has regmeta; only the bundle runs on MONA.

#### 6.8.1 Structural rules — `regproject`

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
  `register_version` — enforced as a structural check, no regmeta
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
- Each file-member's literal `time_key` is unique within the panel.
- A single-member panel (`members` has length 1) requires a
  column-ref `time_key`. A literal `time_key` on the lone member
  is meaningless and rejected.
- A source name is referenced by at most one panel.
- No unknown top-level fields except namespaced ones (`mdw`,
  `swecov`, etc.); namespaced blocks (if present) must be objects.
  Their contents are not inspected at this layer.

#### 6.8.2 Namespaced-block rules — owning package

Each namespaced block is validated by its owner.

- The `mdw` block is validated by mdw — known option keys, value
  types, raise-only invariants like `suppress_k ≥ mdw's library
  default`. Runs at bundle-build time in the builder and at bundle
  load time on MONA.
- Future steward-namespaced blocks follow the same pattern; e.g. a
  `swecov` block would be validated by the package or module that
  owns the SWECOV deployment's extensions.

The webapp invokes the owning validators alongside structural
validation in `POST /api/project/validate`. regproject itself does
not import the owning packages — orchestration is the webapp's job
(and any local CLI's, if it cares).

#### 6.8.3 Semantic rules — regmeta-backed

Enforced where a regmeta database is available — the webapp
backend and any local tool that has loaded regmeta. **Not enforced
inside the MONA bundle** (no regmeta on MONA, no network).

- Every source's `register_version` FQID resolves to a known
  `register_version` row in regmeta.
- Every column's `name` (binding FQID) resolves to a known
  `variable_instance` (following `same_as` curated links if
  present; §6.7). Hard error on miss.
- Every `value_set` (classification FQID) resolves to a known
  `classification`. Hard error on miss.
- Deprecated entries (slug TOML `deprecated: true`) resolve
  normally but emit a warning so the user can review.

**Drift detection.** Validation also compares the spec's
`regmeta_version` against the running regmeta build hash. FQIDs
are stable, so this is best-effort: a mismatch is reported as
info, not error, and any deprecation warning includes a note that
the deprecation was introduced after the spec's recorded version.

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
  committable to keep the project freestanding from regmeta.
- `project_data.stats.json` — output of MONA extract; aggregate
  statistics only; PII-scanned.
- `project_data.realign.json` — small JSON patch produced by the
  bundle's realign pass (§7).

### 6.10 Storage

Project files live in the browser (IndexedDB) during a session and
as JSON in the user's project git repo for durability. **No
server-side storage** — git is the durable store, email/git-sharing
handles collaboration. Server-side projects are a possible v2
feature, not v1.

## 7. MONA workflow

### Single-file bundle, embedded config

The webapp builds a single `.py` file per upload, containing both
the mdw runner code (amalgamated as today) **and** the
`project_data.json` spec embedded as a JSON string literal near the
top:

```python
# === EMBEDDED PROJECT CONFIG ===
_PROJECT_DATA_JSON = r"""
{ "name": "swecov-education", "sources": [...], ... }
"""
# === END CONFIG ===
PROJECT_DATA = json.loads(_PROJECT_DATA_JSON)
```

The bundle has exactly two modes:

```text
python project_bundle.py --mode=realign  → project_data.realign.json
python project_bundle.py --mode=extract  → project_data.stats.json
```

Generation is a **local** step (`mdw generate`), not a bundle mode —
it runs against the generation kit (§8) outside MONA.

### Flow

```text
[Webapp]                                  [MONA batch host]
build bundle  ────────upload───────────►  python bundle.py --mode=realign
review patch  ◄───────download─────────   project_data.realign.json
update spec, rebuild bundle ──────────►   python bundle.py --mode=extract
                                          ↓
consume stats ◄───────download─────────   project_data.stats.json
emit generation kit (offline)
```

### No discover trip

Pre-refactor, mdw did three MONA round-trips (discover → configure
→ extract). The first one (discover) goes away: the spec is
authored from the order, not derived from the data. **Realign**
replaces discover as a small post-delivery diff:

- Pulls `INFORMATION_SCHEMA.COLUMNS` + `COUNT(*)` only (no
  row-level data).
- Compares each spec column's `display_name` against the SQL
  columns present.
- Emits a patch listing raw observations only — no rename
  detection, no automatic spec mutation. The bundle stays dumb;
  the webapp UI handles all reconciliation decisions.

`project_data.realign.json`:

```json
{
  "schema_version": "1.0.0",
  "source": "lisa_2018",
  "row_count": 8492768,
  "missing_in_data": [
    {"binding": "scb/lisa/individer-15plus/2018/lopnr", "display_name": "LopNr"},
    {"binding": "scb/lisa/individer-15plus/2018/fobostat", "display_name": "FoBoStat"}
  ],
  "extra_in_data": ["P1105_LopNr_PersonNr", "UnexpectedCol"],
  "type_mismatches": [
    {"binding": "scb/lisa/individer-15plus/2018/salary", "display_name": "Salary", "expected": "numeric", "sql_type": "VARCHAR"}
  ]
}
```

`missing_in_data` lists columns by their binding FQID plus the
`display_name` the bundle queried for. `extra_in_data` lists SQL
column names found in the data but not queried.

### Reconciling the patch

The spec is authoritative. The webapp loads the patch into the
in-browser project state and walks the user through one screen
where every discrepancy is resolved. A "rename" is conceptually a
paired missing-and-extra, so reconciling both at once keeps the
UX coherent. Four kinds of action:

- **Pair as rename.** Link a `missing_in_data` entry to an
  `extra_in_data` entry; the webapp updates `display_name` on the
  matching column to the SQL header. `name` (the binding FQID) is
  never modified — regmeta identity is stable across the project
  lifecycle. The UI may suggest pairings heuristically (e.g. when
  one is a project-prefixed form of the other), but the user
  confirms.
- **Remove from spec.** A remaining `missing_in_data` entry is
  truly absent; drop the column from its source.
- **Add to spec.** A remaining `extra_in_data` entry is a real
  new column; the UI prompts for a binding FQID (chosen against
  regmeta via catalog search) and a `type`, and stores the
  delivered SQL string as `display_name`.
- **Resolve type mismatch.** Accept the SQL type into the spec
  (changes the column's `type` / `numeric_subtype` / etc.) or
  override (keep the spec type and let mdw cast at extract time;
  may fail loudly at extract).

The realign-review UI is client-side only. After reconciliation,
the in-browser spec is updated and the next bundle download
embeds the corrected version. No server endpoint applies the
patch.

The two modes are independent invocations; the default flow runs
realign first, then extract. A user confident their spec already
matches reality may skip realign and run extract directly. There
is no combined mode — keeping the modes separate ensures a 20-hour
misaligned extract cannot be launched by accident.

### Permissive embedded JSON

The JSON literal in the bundle is editable inline on MONA if needed
(e.g. a tweak under time pressure). **No checksum lock, no integrity
hash.** The threat model is: there is no adversary. The user is the
sole editor; if they corrupt the spec they get a clear parse error
from the bundle's load-time validator. The webapp is the
*recommended* authoring surface, not the *only* one.

Bundle parser validates on load and errors clearly on malformed
JSON or schema violations.

## 8. Value codes and the generation kit

### Codes live alongside the spec

See §6.6. `project_data.json` carries `value_set` references
(classification FQIDs) and binding FQIDs on every categorical
column; the actual code lists live in sibling
`project_data.codes.json`, FQID-keyed. After kit-build the trio
`project_data.json` + `project_data.codes.json` +
`project_data.stats.json` is **freestanding from regmeta** — a
project committed to git regenerates the same mock data years
later, regardless of how regmeta evolves steward-side.

### Generation kit

When the user is ready to generate mocks, the webapp emits a
**generation kit**: a downloadable bundle containing

- `project_data.json` — the spec with FQID references.
- `project_data.codes.json` — dereferenced codes, FQID-keyed.
- `project_data.stats.json` — extract output (uploaded earlier).
- A README and a ready-to-run command.

The user downloads the kit and runs `mdw generate` locally against
it. mdw consumes JSON only — no regmeta dep, fully offline.

### Reproducibility

Same spec + same codes + same stats → same generation kit → same
mock data. Regmeta version drift is inert because every code list
has been dereferenced into `project_data.codes.json` at kit-build
time.

## 9. Webapp architecture

### Three deployments off one codebase

```text
stewards/
  global.yaml       # full catalog (no register filter)
  ifau.yaml         # IFAU's subset
  swecov.yaml       # SWECOV's subset
  global/order_template.{j2,py}    # SCB direct order export
  ifau/order_template.{j2,py}      # IFAU internal grammar (later)
  swecov/order_template.{j2,py}    # SWECOV grammar (later)
```

Deployment dispatch: hostname-per-deployment, same FastAPI binary,
`STEWARD` env var picks the catalog. SPA fetches `/api/context` at
boot to learn its identity and branding.

### 9.1 Steward catalog schema

A steward catalog declares what's available through that channel,
at the **register × year × variable** level (matching how
researchers actually constrain orders). The yaml lives in
`stewards/<steward>.yaml` and uses regmeta FQID slugs throughout
(§5):

```yaml
steward:
  id: ifau
  name: IFAU
  long_name: Institute for Evaluation of Labour Market and Education Policy
  hostname: ifau.example.org
  order_template: ifau/order_template.j2

catalog:
  - register: scb/lisa
    years: [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020]
    variants: null         # null/omitted = all variants of this register
    variables: null        # null/omitted = all variables regmeta knows
  - register: scb/rtb
    years: { from: 1990, to: 2022 }
    variants: [folkbokforda-personer, doda, fodda]   # subset of variants
    variables: [kon, fodelse-ar, lan, kommun]        # variable-slug stems
  - register: sos/par
    years: { from: 1987, to: 2022 }
```

Field shapes:

- `register` — register FQID (`<provider>/<register>`), required.
- `years` — explicit list or `{from: X, to: Y}` range (inclusive
  both ends). Open-ended ranges (`{from: X}`) mean "from X to the
  latest year regmeta knows about."
- `variants` — explicit list of variant slugs (without the
  register prefix), `null`/omitted means every variant of the
  register.
- `variables` — explicit list of variable-concept slugs (the last
  segment of the variable's concept FQID), `null`/omitted means
  every variable regmeta knows about for the listed years and
  variants. Globs are not supported in v1 — the explicit list is
  the audit surface.

The `global` catalog is special: `catalog: all` (sentinel) means
"the entire regmeta universe with no filter." Used by the
SCB-direct deployment.

Loading the catalog at FastAPI startup builds an in-memory index:
`register_version FQID → set<binding FQID>` plus
`register FQID → year-range`. The validate endpoint and the
variable-list authoring endpoints consult this index.

A future schema addition: per-steward `extensions` that toggle
optional spec features (e.g. SWECOV enables the `swecov.filters`
namespaced block). Not in v1.

### 9.2 Stack

- **Backend**: FastAPI + Pydantic + REST.
- **Frontend**: Svelte 5 + Vite + TypeScript (bun-managed).
- **Server**: stateless; regmeta SQLite mmap'd; in-memory catalog
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
- Aggressive HTTP cache headers on all read endpoints (regmeta only
  changes when rebuilt).
- Per-endpoint rate limit on actual-work endpoints (`/api/bundle`,
  `/api/kit`, `/api/project/validate`, `/api/project/order`),
  capped at e.g. 30 req/min/IP at the FastAPI layer — separate
  from and stricter than the edge-cached catalog reads.
- Optional session token in localStorage for rate-limit bucketing
  (not auth — just per-browser identification).

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

**Catalog browsing** (read; cacheable)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/catalog/providers` | List providers exposed by the steward catalog. |
| GET | `/api/catalog/providers/{provider}/registers` | List registers under a provider. |
| GET | `/api/catalog/registers/{provider}/{register}` | Register details with variants and year range. |
| GET | `/api/catalog/registers/{provider}/{register}/variants` | List variants of a register. |
| GET | `/api/catalog/variants/{provider}/{register}/{variant}` | Variant details (description, year range, table-like shape). |
| GET | `/api/catalog/variants/{provider}/{register}/{variant}/versions` | List register_versions of a variant. |
| GET | `/api/catalog/versions/{provider}/{register}/{variant}/{period}` | register_version details, including bindings (variables in that version). |
| GET | `/api/catalog/bindings/{provider}/{register}/{variant}/{period}/{variable}` | Variable binding details: definition, type hint, value-codes shape, source-link graph. |
| GET | `/api/catalog/classifications` | List classifications. |
| GET | `/api/catalog/classifications/{classification}/{version}` | Classification details and full code list. |
| GET | `/api/catalog/resolve?fqid={fqid}` | Resolve any FQID (register, variant, version, binding, concept, classification) to its entity. Used by realign-review and paste-imports. |
| GET | `/api/catalog/search?q={query}&kind={register\|variable}` | FTS across registers and variables (delegates to regmeta's FTS5 indexes). |

**Documentation** (regmeta docs DB; read; cacheable)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/docs/search?q={query}` | FTS over parsed register-documentation markdown. |
| GET | `/api/docs/get/{provider}/{register}` | Register-level documentation. |
| GET | `/api/docs/get/{provider}/{register}/{variable}` | Variable-concept-level documentation. |

**Project** (write; no rate-limit-free cache)

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/project/validate` | Validates a `project_data.json` document; returns structural, namespaced-block, and semantic errors/warnings (§6.8). |
| POST | `/api/project/order` | Renders the steward's order export (CSV in v1; template-driven). |
| POST | `/api/bundle` | Builds the Python MONA bundle embedding the supplied `project_data.json`. Pure function of the input (no server-side state, no steward config injection); response cacheable by content-hash. Returns `.py` as `application/octet-stream`. |
| POST | `/api/kit` | Builds a generation kit (zip) from `project_data.json` + `project_data.stats.json`. Dereferences classification FQIDs and binding FQIDs into a sibling `project_data.codes.json` (§6.6). |

Realign-patch application is **client-side only** — the SPA reads
`project_data.realign.json` against the in-browser project state.
No `/api/realign/apply` endpoint.

The OpenAPI spec is the canonical contract; the table above is
prose for the spec doc, not the source of truth.

### 9.6 Domain types vs Pydantic models

- `regmeta` and `regproject` use plain dataclasses. No Pydantic
  dep on the library side — keeps them importable from any context
  (Jupyter, scripts, mdw on MONA).
- `webapp/backend/` defines Pydantic *response models* that wrap or
  project the domain types. The mapping is explicit, one function
  per endpoint.

## 10. Post-refactor allocation of current mdw machinery

The refactor pulls mdw apart. Each existing piece of machinery needs
to land somewhere — webapp, mdw (slimmed), regproject, or deleted.
This section is the design's most operationally critical and
deserves the most scrutiny.

### Stays in mdw (the slim CLI)

- **PII scanner** (`scan.write_export`). Runs on MONA before the
  bundle writes anything to disk. Defense-in-depth on top of
  summarize-by-construction. Non-negotiable safety net; lives where
  the bundle is.
- **Disclosure-control thresholds** (`SUPPRESS_K`, small-population
  warning, date jitter, numeric noise). Apply at extract time in
  the bundle. Library defaults are mdw's; per-column overrides come
  from the spec's `mdw.column_options`. Steward config does not
  influence these — the spec is the sole source of run-relevant
  configuration.
- **CSV typing machinery** (all-varchar reads, semantic → DuckDB
  cast map, opaque auto-promotion, materialisation threshold).
  Operational detail of `mdw bundle build` + the extract runner.
  Documented in `mock_data_wizard/DESIGN.md`; the spec doesn't need
  to repeat it.
- **`mdw generate`** — local CSV generation from the kit.
- **`mdw scan`** — standalone PII scanner against an existing JSON
  file. Kept; useful for ad-hoc audits.
- **`mdw compare`** — validates locally-stored mock CSVs against
  the spec's column lists. Rewired to read `project_data.json`
  instead of the legacy manifest, but the command stays.

### Moves to the webapp / `regproject`

- **Value-code drift warnings** (codes in stats not present in the
  pinned value set). Run at kit-build time, surfaced as a warning
  block in the kit's README and in the SPA. Also re-run
  defensively by `mdw generate` against the kit's codes; warnings
  go to stderr.
- **Classifier chain** (id-name / regmeta evidence / sql_type /
  fallthrough). Replaced by the webapp's variable-list authoring —
  the user picks types deliberately against regmeta's data, and
  the spec carries them. No automated classification at extract
  time.
- **Editor API** (`mock_data_wizard.editor`) — concurrency tokens,
  group views, panel detection, etc. Replaced by the webapp's
  in-browser project state. The Python editor module is deleted.
- **Local HTTP server** (`mock_data_wizard.server`, current
  `mock-data-wizard ui`). Deleted; webapp covers it.

The current **CVID picker** (tiered year/name/code-set scoring
across multiple cvids per variable) becomes unnecessary under
FQIDs: a binding FQID resolves to exactly one
`variable_instance` row, which directly carries the cvid via
regmeta. Picking is replaced by deterministic resolution. The
picker module is deleted, not moved.

### Composite entity_key and time_key support

The panel schema (§6.4) allows composite `entity_key` (firm ×
workplace, household × person) and composite `time_key` (year ×
quarter). This is meaningful new behaviour in mdw:

- **`extract.py`** — `COUNT(DISTINCT entity_key)` becomes
  `COUNT(DISTINCT (col_1, col_2, ...))`; `GROUP BY <time_key>`
  becomes `GROUP BY (...)`. Tuple periods replace scalar periods
  in `by_period`.
- **`stats.py`** — `n_entity_ids` semantics unchanged but the
  underlying distinct-tuple count is a composite. `period` becomes
  `int | string | (int | string)[]`.
- **`generate.py`** — the shared id pool is keyed by tuple rather
  than scalar; the deterministic shuffle generates shuffled tuples.
  Per-row column generation for column-member time_keys produces
  composite period values consistently.

Single-key panels keep working unchanged — the polymorphism in the
schema means a `string` value for `entity_key` and a scalar
`time_key` continue to be valid inputs. Composite is additive.

### Population spine

Current mdw maintains a **population spine**: birth-invariant
attributes (Kön, Födelseår, Födelselän, Födelseland) are generated
once per individual and reused across files, so the same person
doesn't have different sex in different files. Spine-eligible
variables today are a hardcoded set of regmeta `var_id`s; without
regmeta, the spine is empty.

Post-refactor: spine eligibility becomes a hardcoded set of
**variable concept-slug stems** inside mdw — the last segment of
a binding FQID:

```python
SPINE_CONCEPT_SLUGS = {"kon", "fodelse-ar", "fodelse-lan", "fodelse-land"}
```

At generate time, mdw inspects each column's `name` (binding FQID)
and applies spine semantics when the trailing concept slug
matches. This works across providers and registers automatically:
`scb/lisa/individer-15plus/2018/kon`, `scb/rtb/folkbokforda/2019/kon`,
and `sos/par/sv/2020/kon` are all the same concept for spine
purposes. Cross-rename equivalence (the curated `same_as` graph in
regmeta; §5.5) is *not* consulted at generate time — the kit is
freestanding from regmeta — but the webapp can use it to verify
that all the project's "Kön" columns share a canonical concept
before kit-build.

If a future project needs to extend the spine (e.g. a migration
study adding Migrationsår), the override would go in the spec's
`mdw` block, not in steward config. Schema-design of that override
is deferred until a concrete project requires it.

### `mdw update`

`maintain update` (downloads the latest regmeta DB and docs DB
assets) is a regmeta concern, not an mdw concern. After mdw drops
its regmeta dep, `mdw update` is deleted; users use
`regmeta maintain update` to keep their local regmeta current.

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

## 11. What changes from today

| Module / behavior | Fate |
|---|---|
| regmeta object model | Provider promoted to first-class; FQID grammar introduced; `slug` columns added to register/register_variant/classification; synthetic `_default` variant for variant-less registers (§5) |
| regmeta slug curation | New `regmeta/slugs/*.toml` files committed to repo; grow-only; CI-enforced immutability (§5.4) |
| regmeta library API | New typed `Catalog.resolve(fqid)` entry point; webapp imports through this surface only (§5.7) |
| `mdw/web/` (Svelte UI) | Migrates to `webapp/frontend/`, then deleted |
| `mdw/server.py` (local HTTP server) | Deleted after migration |
| `mdw/editor.py` (mutator API) | Deleted; webapp owns authoring |
| `mdw/classify.py` (classifier chain) | Deleted; types come from the spec |
| `mdw/enrich.py` (regmeta lookups at generate) | Codes come from `project_data.codes.json` instead |
| `mdw/registers.py`, `mdw/cli.py` regmeta cmds | Deleted (equivalents live in `regmeta`) |
| `mock_data_discovery.json` | Deleted; replaced by realign patch |
| `mock_data_config.json` | Renamed to `project_data.json`; schema owned by `regproject`; column `name` becomes a binding FQID; source `register`+`year` collapse into `register_version` FQID |
| `mock_data_stats.json` | Renamed to `project_data.stats.json` |
| `value_set_version` strings (`Kon@2023`, `SUN@2020`) | Replaced by classification FQIDs (`class/sun/2020`) on the column; ad-hoc codes inlined in `project_data.codes.json` by binding FQID (§6.6) |
| `regmeta maintain build-db` subcommand | Moves to new `regmeta-build` CLI |
| `regmeta maintain update` | Stays in `regmeta`; the canonical "freshen everything" command |
| `mdw update` | Deleted; users run `regmeta maintain update` |
| Population spine | Stays in mdw; matches binding FQIDs by concept-slug stem (§10) |
| CVID picker | **Deleted.** FQID resolution replaces tiered scoring (§10) |
| PII scanner | Stays in mdw (in the bundle, MONA-side) |
| `mdw compare` / `mdw scan` | Kept; `compare` rewired to read `project_data.json` |

mdw post-refactor: `bundle build` + bundle runtime + `generate` +
`compare` + `scan`. The Python package shrinks substantially; CLI
surface narrows to the mock-data workflow.

## 12. Future-proofing constraints

Not preemptive — just hygiene that's good anyway and keeps options
open:

- **OpenAPI as canonical contract.** Snapshot-tested in CI. A
  future Go/Rust port of the query API reproduces the spec; clients
  are unaffected.
- **Build / runtime cleanly separated.** `regmeta` (query) is small
  and pure; `regmeta_build` is operator-side. A future port
  replaces query only; build stays Python.
- **Server is stateless.** No process-local caches that change
  behavior across requests, no Python-specific tricks.
- **No Pydantic creep into core libraries.** Already covered above;
  reinforced here.

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
  and §6.7 dissolve the validation-time problem (binding FQIDs
  resolve directly; source-link edges become catalog-browse-time
  hints). What remains is a UI question: when a user is authoring
  a LISA variable list and the catalog knows the variable
  originates in RTB, how is the lineage surfaced — as a hover
  tooltip, an inline note, a "see also" panel? Deferred to webapp
  authoring-UI design. See memory
  `project_regmeta_lisa_columns_gap`.
- **Order export grammar.** Simple CSV (register × variable ×
  year) is the agreed v1; the pluggable seam for steward-specific
  exporters (IFAU spreadsheets, SWECOV PDFs) needs concrete
  protocol definition before stewards 2 and 3 go live.
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
- **FQID encoding in URLs.** §9.5 puts FQID segments directly in
  URL paths (`/api/catalog/bindings/scb/lisa/individer-15plus/2018/kon`).
  FastAPI handles slash-bearing path params via `:path` converters,
  but the URL form is unusual and worth sanity-checking with
  Cloudflare's edge-cache key behaviour before locking it in.
- **`same_as` rendering at generate time.** §10's population spine
  doesn't consult `same_as`; the webapp does, at authoring time.
  Whether `mdw generate` should also normalize via `same_as`
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
inform sequencing decisions.

1. **regmeta identifier rebuild (§5).** Provider promoted to
   first-class; FQID grammar implemented; `slug` columns added to
   `register`, `register_variant`, `classification`; synthetic
   `_default` variant emission for variant-less registers; slug
   TOMLs bootstrapped via `regmeta-build seed-slugs` and
   hand-edited; CI snapshot test on slug immutability. Existing
   query commands continue to work; FQID emission and `resolve` are
   added alongside.
2. **`regmeta_build` carved out of `regmeta`.** Pure mechanical
   split; both packages keep working. Releases independently. Done
   alongside (1) because the rebuild touches build code most.
3. **`regproject` package created** with the `project_data.json`
   schema + validators, referencing regmeta FQIDs throughout
   (column `name` = binding FQID, `value_set` = `class/...` FQID
   or absent + codes inlined in `project_data.codes.json`).
   Importable by mdw. No webapp yet.
4. **mdw config renamed to `project_data.json` shape**; mdw
   consumes the new schema. Classifier still exists but operates
   on the new shape. mdw still imports regmeta at this point.
5. **Single-file bundle with embedded config.** mdw bundle now
   carries the spec inline. Discover step still exists; it's the
   next thing to retire.
6. **Webapp scaffolds: backend + frontend skeleton.** Empty UI,
   OpenAPI plumbing, one steward (global) wired up, reads regmeta
   through the FQID-keyed catalog API.
7. **Webapp authoring of `project_data.json`.** Replaces mdw's
   editor for new projects; mdw editor stays for legacy projects
   in parallel.
8. **Webapp kit-build** (`POST /api/kit`) — value-set
   dereferencing: classifications resolve to inline `codes` keyed
   by classification FQID; ad-hoc categorical columns get inline
   codes keyed by binding FQID. Generation kit format finalised
   here.
9. **mdw drops regmeta dep.** Classifier deleted, enrich rewired
   to read pinned codes from the kit. mdw editor and server
   deleted. Population spine ships as a hardcoded set of binding
   FQID stems matched at generate time.
10. **Composite entity_key / time_key support in mdw.** Schema-side
    (regproject) is purely additive at step 3; the mdw-side
    implementation in `extract.py` / `stats.py` / `generate.py`
    (see §10) lands here so the first composite-key panels can be
    authored end-to-end. Single-key panels are unaffected.
11. **Realign mode in the bundle.** Discover deleted from mdw.
12. **Steward catalogs** (ifau, swecov) added once the architecture
    is stable. Order export exists in CSV form for all three.
13. **Per-steward order templates** and `extensions` toggles
    layered on as steward-specific grammar requirements emerge.

At step 9 or 10, `REFACTOR_SPEC.md` dissolves: §5 moves to
`regmeta/DESIGN.md`; §6, §7, §8 move to `regproject/DESIGN.md` and
`mock_data_wizard/DESIGN.md`; §9 moves to `webapp/DESIGN.md`; §1,
§2, §4, §11, §12 distill into a slimmer `ARCHITECTURE.md` at the
repo root.
