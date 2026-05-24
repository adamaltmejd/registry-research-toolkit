# Registry Research Toolkit — Refactor Spec

Working document for the cross-package refactor that turns today's
tooling (`reg_meta` + `mock_data_wizard`) into a multi-deployment system
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
reg_meta indexes), IFAU's subset, SWECOV's subset.

## 2. Current state of the toolkit

Two Python packages today, both in this repo:

### `reg_meta`

Searchable database of Swedish registry metadata, indexing ~100M
value-code rows across hundreds of registers. Built by parsing SCB
CSV exports and Socialstyrelsen Excel deliveries (`maintain
build-db`); queried via CLI (`reg-meta search`, `reg-meta get`,
`reg-meta resolve`, `reg-meta docs ...`) and as a Python library.
Two SQLite databases: the main metadata DB (`reg_meta.db`,
~520 MB) and a separate documentation DB (`reg_meta_docs.db`,
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
  cadence, different operators. The built SQLite DBs (`reg_meta.db`,
  `reg_meta_docs.db`) are too large to ship inside the wheel (~520 MB
  uncompressed for the main DB; ~120 MB compressed) and are
  distributed as `.zst`-compressed **GitHub release artifacts** on
  `reg_meta/v*` tags. `reg_meta`
  ships a `reg-meta update` command that fetches the
  matching version into `$XDG_DATA_HOME/reg_meta/`; the webapp
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

### 4.4 IR + adapter architecture (build-side)

`reg_meta_build` is restructured around a provider-neutral
intermediate representation (IR) and per-provider adapters that
emit it. This isolates provider-specific source parsing from the
universal materializer.

**Three layers:**

```text
   per-provider adapter (build-time)
       ↓ emits IR (Pydantic dataclasses)
   universal materializer
       ↓ writes
   universal SQLite schema (English column names, provider-agnostic)
       + sibling provenance DB for maintainer-only debug data
       + reg-meta-docs sidecar for variant-level narrative metadata
```

**Adapter** lives at `reg_meta_build/sources/<provider>.py`. Reads
the provider's native source format (SCB CSV exports, SOS xlsx
workbooks, future: FK XML, Skatteverket JSON, etc.) and emits a
stream of IR objects. Handles provider-specific quirks:

- SCB: build-time triage for same-year collisions; auto-derive
  sibling slugs and `variable_related_to` edges.
- SOS: variant synthesis for variant-less registers (LSS, BU, SOL);
  kodlista era parsing; entity-registry vs value-set-drift heuristic
  for kodlistor with many tidsperiod ranges.
- Future provider: implements `IRAdapter` protocol, declares any
  provider-specific extension tables.

**IR** lives at `reg_meta_build/ir/`. Pydantic v2 dataclasses
defining the contract every adapter speaks. Build-time only — never
imported by `reg_meta` runtime, by `reg_monabundle.runtime`, by the
MONA bundle, by the webapp. Validators (Pydantic model-level) catch
builder bugs at construction (e.g. "state validity range crosses
zero", "variable references non-existent variant").

Concrete shapes (sketch — full DDL in `reg_meta_build/DESIGN.md`):

```python
class IRRegister(BaseModel):
    register_id: int                    # universal ID (=SCB RegisterId or hash-minted SOS)
    provider: str                       # 'scb', 'sos', ...
    slug: str
    name: str                           # canonical native title
    description: str | None
    purpose: str | None                 # short prose for catalog browse cards

class IRVariant(BaseModel):
    variant_id: int
    register_id: int
    slug: str                           # '_default' for variant-less registers
    name: str
    description: str | None
    synthesized: bool = False           # True when adapter invented from var.deldatamangd
    # Natural panel structure for this variant (§5.3 panel_template):
    panel_entity_key: str | tuple[str, ...] | None = None     # variable slug(s)
    panel_time_key: str | None = None                         # "period" sentinel OR variable slug
    panel_time_grain: Literal["delivery", "row"] | None = None

class IRVariable(BaseModel):
    variable_id: int
    register_id: int
    variant_id: int                     # variant-scoped identity
    slug: str
    name: str
    definition: str | None
    description: str | None             # includes inlined operational_definition when present
    measurement_unit: str | None        # NULL when source was "Okänd"
    is_sensitive: bool = False
    is_identifier: bool = False
    source_register_id: int | None
    source_register_text: str | None    # human-readable attribution (when source not resolved or for display)

class IRVariableState(BaseModel):
    state_id: int
    variable_id: int                    # variant-scope is implied via variable FK
    valid_from: str | None              # ISO 8601 ('YYYY' | 'YYYY-MM' | 'YYYY-MM-DD')
    valid_to: str | None                # ISO 8601; None = open-ended
    data_type: str                      # normalized lowercase canonical set
    data_length: int | None
    value_set_id: int | None
    value_set_version_label: str | None # overlap discriminator (rare; multi-vintage)

class IRValueSet(BaseModel):
    value_set_id: int
    content_hash: str                   # hash of normalized code list; dedup key
    classification_id: int | None       # set when this value_set is a (possibly year-projected) subset of a named classification
    codes: tuple[IRValueCode, ...]

class IRValueCode(BaseModel):
    value_set_id: int
    code: str
    label: str
    valid_from: str | None              # per-code temporal validity (ISO 8601)
    valid_to: str | None

class IRClassification(BaseModel):
    classification_id: int
    slug: str                           # version baked in: 'sun2020', 'icd10', 'lkf2007'
    name: str
    publisher: str | None
    version: str | None
    provider: str | None                # NULL for cross-provider classifications

class IRLineageEdge(BaseModel):
    consumer_state_id: int
    source_state_id: int
    valid_from: str                     # ISO 8601 (intersection of consumer + source validity)
    valid_to: str

class IRReplacedByEdge(BaseModel):
    predecessor_variable_id: int
    successor_variable_id: int
    effective_year: int | None
    note: str | None

class IRRelatedToEdge(BaseModel):
    a_variable_id: int
    b_variable_id: int
    relation_kind: str
    note: str | None

class IRWarning(BaseModel):
    entity_kind: str
    entity_id: int
    code: str
    detail: str | None = None

class IRDeliveryProvenance(BaseModel):
    """Goes to provenance DB only, not to the published catalog."""
    register_id: int
    source_file: str
    delivery_version: str | None
    delivery_date: date | None
    template_version: str | None
    approval_dates: dict[str, str] | None = None  # for SCB: maps period_token → last_approved_date
```

**Materializer** lives in `reg_meta_build/db.py`. Provider-blind:
takes IR, writes the universal SQLite catalog. Owns:

- Schema creation (universal model only; no provider-specific tables)
- ID validation (no collisions; FKs resolve)
- Slug curation reads from `fqid_slugs/<provider>.toml`
- Auto-derive `variable_replaced_by` from SCB `timeseries_event` (only SCB adapter populates that input)
- `variable_state_lineage` interval-overlap join (§5.6)
- FTS index population
- Sibling provenance DB writes
- Sibling reg-meta-docs writes for adapter-emitted prose (mätinformation, quality narratives, etc.) routed to variant-level paths

The materializer enforces invariants at build time. Notable:

- `(variable_id, valid_from)` is unique across `variable_state` unless explicitly marked multi-vintage via `value_set_version_label`.
- Every `IRVariable.source_register_id`, when set, resolves to an `IRRegister`.
- `IRClassification.slug` is globally unique.
- `IRValueSet.classification_id`, when set, resolves to an `IRClassification`.
- `IRVariableState.valid_from` / `valid_to` are valid ISO 8601 strings; period tokens (`HT2020`, `2020-Q3`) get mapped to ISO ranges at adapter time, not in the IR layer.

**Provenance DB sibling artifact** at
`<db_dir>/reg_meta.provenance.db`. Maintainer-only; not shipped to
consumers. Schema:

- `build_manifest(schema_version, universal_db_path, universal_db_sha256, build_date)` — ties provenance to specific universal DB.
- Per-provider tables holding source-ID linkage for debugging
  (e.g. `scb_register_id_map(register_id, scb_registernamn,
  scb_imported_at)`).
- Adapter parse warnings (per-provider error log).
- Source checksums + row counts (was in `import_manifest` table; moved out of published DB).

The provenance DB rotates: `reg_meta.db` → `reg_meta.db.prev` and
`reg_meta.provenance.db` → `reg_meta.provenance.db.prev` on
rebuild. One generation, no auto-cleanup. Maintainers can `mv` to
long-term archive if needed.

**Deterministic ID minting.** SCB universal IDs reuse the source
integer IDs verbatim (`RegisterId`, `RegVarID`, `RegVerID`, `VarId`,
`CVID`). This means SCB rebuilds produce byte-identical universal
IDs given identical CSV inputs.

SOS IDs are minted deterministically via BLAKE2b, namespaced into
the provider band via bit 62 (top bit 63 reserved for SQLite's
signed-int compatibility):

```python
def mint(*parts: str) -> int:
    """Deterministic 63-bit ID, namespaced into [2^62, 2^63) via bit 62."""
    h = blake2b(
        "/".join(parts).encode("utf-8"),
        digest_size=8,
        person=b"regmeta-id",
    ).digest()
    # Take low 62 bits from hash, set bit 62, leave bit 63 clear.
    return (int.from_bytes(h, "big") & ((1 << 62) - 1)) | (1 << 62)
```

SCB IDs occupy `[0, 2^32)`; SOS IDs occupy `[2^62, 2^63)`. Visual
diagnostic + clean namespace. Future providers get their own
namespace bits (FK could set bit 61 → `[2^61, 2^62)`, etc.).
Bit 63 is left clear throughout so every ID fits in SQLite's signed
`INTEGER` without sign-bit weirdness.

**Why this architecture pays off.** Adding a new provider becomes:
write an adapter at `reg_meta_build/sources/<provider>.py`, declare
any extension tables, add `<provider>.toml` to `fqid_slugs/`. Zero
changes to the materializer; zero changes to the universal schema;
zero changes to `reg_meta`'s read side. The IR is the contract.

## 5. reg_meta as the substrate

The refactor centres on reg_meta. Every downstream artifact — the
`project_data.json` schema, the webapp's `/api/catalog/*` endpoints,
the generation kit consumed by `reg_mockdata` — references reg_meta
entities (registers, variants, variables, value codes). The contract
between them is only as stable as reg_meta's identifier scheme. The
v0.11 scheme worked but baked SCB's CSV vocabulary and yearly
publication cadence into the universal model; adding a second
provider (Socialstyrelsen) and a third (Försäkringskassan, ETA
post-v1) made the cracks visible.

This section defines the post-v0.12 substrate: a **provider-neutral
object model** with English-named columns carrying native data, a
**4-segment FQID grammar** without a period slot, **state-on-variable
modeling** that captures definition changes as `variable_state` rows
with explicit validity ranges, **slug-anchored edge tables** for
succession (`replaced_by`), grain-sibling (`related_to`), and
cross-rename equivalence (`same_as`), and a **build-time triage**
pass that normalizes provider-specific oddities into the universal
shape. The universal schema carries **no provider-specific tables**
(no `scb_*`, no `sos_*`): provider variation is captured via
fill-rate on the universal columns (§5.8); prose and narrative
metadata go to reg-meta-docs at variant level; maintainer-only
debug data goes to the sibling provenance DB.

The grammar is anchored to the underlying providers' source IDs
(SCB's `RegisterId` / `RegVarID` / `VarID` / `CVID` are reused as
universal IDs; SOS IDs are deterministically minted) and overlaid
with curated human-readable slugs for ergonomics. A separate
**provenance DB** sibling artifact (maintainer-only, not shipped to
consumers) tracks adapter-specific debug data — build manifests,
source checksums, raw provider-side IDs.

### 5.0 Empirical basis

The Model A shape is calibrated against the production SCB
`reg_meta.db` (sampled at the 2026-05-22 design lock, schema v0.11.x)
and the 13 Socialstyrelsen workbooks current at that time. The numbers below
are what made each Model A choice load-bearing; they are recorded
here so future contributors can re-run the analysis if the data
drifts.

| Question | Finding | Implication |
|---|---|---|
| How much per-edition metadata drift do current variables show? | 43% of multi-version `(register, variant, variable)` triples carry drift across editions; 74% for variables with ≥20 editions. | State-on-variable captures real signal — not just bookkeeping. |
| Does state-on-variable actually shrink the row count? | 515K `variable_instance` rows → ~104K `variable_state` rows (≈ 5× shrink). | Coalescing-by-shape is the right primitive. |
| What share of variables are actually multi-state? | 65% of triples collapse to a single `variable_state`. | Single-state is the common case; the longitudinal API must not penalize it. |
| How year-coupled is current lineage? | 100% of v0.11 `via_source_id` edges have `src_year == dst_year`. | The §5.6 interval-overlap join is mechanically simpler than the slug-keyed equality join and 3.5× smaller (53,635 → ~15,187 edges). |
| Does build-time triage scale? | ~99% of same-year multi-state collisions resolve automatically with the kolumnnamn-primary discriminator (§5.7). | ~200–300 manual TOML overrides expected — a one-time curation backlog, not an ongoing tax. |
| Are SOS workbooks compatible with the universal shape? | 13 workbooks → ~2,300 IR rows under the §4.4 adapter contract. Variant synthesis (LSS/BU/SOL) and per-row tidsperiod ranges handled by the adapter, not by extension tables. | The "no provider-specific tables" rule holds for SOS. |

These numbers belong here, not just in the PR description that
introduced Model A — once the PR ships, the body is effectively
undiscoverable, and a year-from-now contributor questioning the
shape needs the anchor.

### 5.1 Object model

The universal model has six core entities. **No provider-specific
tables.** Provider variation is captured via fill-rate on the
universal columns (§5.8); prose/narrative metadata lives in
reg-meta-docs; build artifacts live in the provenance DB.

| Entity | Universal columns | Notes |
|---|---|---|
| `provider` | `provider_id`, `slug`, `name` | `scb`, `sos`, `fk` (Försäkringskassan), `skv` (Skatteverket), ... |
| `register` | `register_id`, `provider_id`, `slug`, `name`, `description`, `purpose` | LISA, RTB, PAR. Slug-identified per-provider. Lean column set; provider-specific metadata that doesn't fit lives in docs (§5.8). |
| `variant` | `variant_id`, `register_id`, `slug`, `name`, `description`, `panel_entity_key`, `panel_time_key`, `panel_time_grain` | LISA/individer-15plus, RTB/folkbokforda-personer, SOS/par/sluten-vard. SCB `registervariant`, SOS `deldatamängd`. Doesn't nest further. `_default` slug for variant-less registers (SOS LSS, BU, SOL). Panel-template columns declare natural panel structure (§5.3). |
| `variable` | `(register_id, variant_id, variable_id)` PK, `slug`, `name`, `definition`, `description`, `measurement_unit`, `is_sensitive`, `is_identifier`, `source_register_id`, `source_register_text` | Slug-identified, **variant-scoped**. Identity is `(provider, register, variant, variable_slug)`. "Kön in LISA/individer-15plus" and "Kön in LISA/individer-16plus" are different variables; the empirical equivalence (same SCB `var_id`) is captured via auto-emitted `variable_same_as` edges (§5.5). Cross-register concept-merging is curation via the same mechanism. Operational definitions inline into `description` at ingest when distinct. |
| `variable_state` | `state_id`, FK to variable, `valid_from`, `valid_to` (ISO 8601 `YYYY-MM-DD` TEXT, both NOT NULL), `data_type`, `data_length`, `value_set_id`, `value_set_version_label` | Per-era shape; a variable has 1..N states. Non-overlapping by default; overlap permitted only with explicit `value_set_version_label` discrimination (LKF-shape multi-vintage). Storage is always full-date `YYYY-MM-DD`. Coarser inputs are expanded at ingest into ranges: SCB year `2018` → `valid_from = '2018-01-01'`, `valid_to = '2018-12-31'`; SCB year-month `2018-03` → `valid_from = '2018-03-01'`, `valid_to = '2018-03-31'`; period tokens like `HT2020` → the corresponding ISO date range. Open-ended states use the sentinel `valid_to = '9999-12-31'` (never NULL). Because every stored value is a full date, lexical string comparison is chronologically correct and intersection uses plain `max`/`min`. |
| `value_set` | `value_set_id`, `member_hash`, `classification_id` (nullable) | Content-addressed dedup of code memberships. When `classification_id` is set, the value_set is a (possibly year-projected) subset of a named classification. When NULL, the value_set is anonymous (ad-hoc codes for this variable). |
| `classification` | `classification_id`, `slug`, `name`, `publisher`, `version`, etc. | Named versioned vocabulary: SUN2020, ICD10, ICD11, LKF. Provider-independent. Version baked into the slug (`sun2020`, `icd10`, `lkf2007`), not a separate FQID slot. |

Plus three orthogonal relationship tables:

| Edge | Direction | Semantics |
|---|---|---|
| `variable_same_as` | Symmetric | Cross-rename equivalence: "this variable in register A is the same concept as that variable in register B". Curated. |
| `variable_replaced_by` | Directional | Succession: variable X retired, replaced by variable Y. Auto-derived from SCB `timeseries_event` (`handelse IN ('Ersatt av', 'Ersätter')`) plus TOML curation for cross-provider edges. |
| `variable_related_to` | Symmetric | Grain/position/coding siblings: `utbildningsinriktning-3pos` ↔ `utbildningsinriktning-4pos`. Auto-emitted by build-time triage splits; TOML adds cross-variable edges manually. |

Plus the lineage edge (§5.6) materializing composite-source bindings:

| Edge | Direction | Semantics |
|---|---|---|
| `variable_state_lineage` | Directional | Build-materialized: LISA's `kön` state X was sourced from RTB's `kön` state Y during validity-range intersection [from, to]. Replaces v0.11's per-cvid `via_source_id`. |

`population` and `object_type` remain orthogonal context layers,
attached to variable states (or to variants where appropriate); they
do not participate in the FQID.

**Universal English column names with native data.** Universal
column names (`name`, `title`, `description`, `data_type`, ...) carry
provider-native string values exactly as published. The SCB
`registernamn` "Longitudinell integrationsdatabas för sjukförsäkrings-
och arbetsmarknadsstudier (LISA)" stays verbatim under
`register.name`; the SCB `Variabelnamn` "Kön" stays verbatim under
`variable.name`. Order generation reads these strings verbatim —
they're what the provider's intake system expects on the order form.

**Variant-less registers (`_default`).** Socialstyrelsen LSS, BU,
SOL workbooks ship variables without a `Deldatamängder` sheet.
Adapters synthesise a single `_default` variant row at adapter time
(persisted as a real row, not synthesised at resolve time). The
`_default` slug is reserved (§5.3). Single-variant registers where
the lone variant just restates the register name may also use
`_default` as a curator override. The FQID emitter never elides
`_default`; canonical FQIDs always include the segment.

**Provider-specific publication metadata** (SCB mätinformation,
approval dates, doc_status; SOS DCAT-AP English variants, quality
sheets, dataset_version/date) **does not live in the universal
schema**. Methodology prose and quality narratives go to reg-meta-docs
at variant level (§5.8); build artifacts (approval dates, delivery
versions, source checksums) go to the provenance DB. The universal
model stays uniform across all providers.

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
| 3 | `<provider>/<register>/<variant>` | variant |
| 4 | `<provider>/<register>/<variant>/<variable>` | variable binding |
| 2, leading `class/` | `class/<classification>` | classification |

Examples:

```text
scb                                                  provider
scb/lisa                                             register
scb/lisa/individer-15plus                            variant
scb/lisa/individer-15plus/kon                        variable binding
scb/lisa/individer-avlidna                           variant (singleton aux table)
sos/lss/_default                                     variant (variant-less register)
sos/lss/_default/insatstyp                           variable binding
scb/arbetskraftsbarometern/_default/kon              variable binding (variant-less)
class/sun2020                                        classification (version in slug)
class/icd10                                          classification
class/icd11                                          classification (successor)
class/lkf2007                                        classification (vintage in slug)
```

**No period slot.** Year/period is not part of the FQID grammar.
The same variable can have different definitions in different years
— that drift is captured by `variable_state` rows with explicit
validity ranges, not by per-year FQIDs. Consumers needing
year-specific resolution supply year via `project_data.json`'s
`Source.period` field or via the Catalog's `resolve_at(fqid,
period)` method (§5.10). Time is data context, not identity.

**Classification version is part of the slug, not a separate slot.**
SUN2020 is `class/sun2020`, not `class/sun?version=2020`. SUN1996 is
`class/sun1996`. ICD-10 and ICD-11 are distinct classifications
with distinct slugs. This matches how researchers actually think
about classifications: each vintage is its own normative document.

**Variables have no concept FQID.** A 3-segment "variable concept"
form (e.g. `scb/lisa/kon`) would collide with the variant form; the
grammar would lose its segment-count discriminator. Cross-variant
operations ("Kön across LISA variants", "all bindings of this
variable concept") are catalog traversals via `Catalog.states(...)`
and `Catalog.related(...)` (§5.10), not serializable identifiers.

**Slug grammar.** Every slug must match
`^[a-z](?:[a-z0-9]|-[a-z0-9])*$` (lowercase ASCII, kebab-case, must
start with a letter, must end with a letter or digit, hyphens only
appear singly between alphanumerics). Single-character slugs match
`^[a-z]$`. Disallowed: leading/trailing hyphens, consecutive hyphens
(`--`), underscores, uppercase, non-ASCII.

**Reserved and disallowed slugs.** Build rejects any slug entry
hitting one of these:

- `class` — reserved in any slot (keeps the leading-`class/`
  discriminator unambiguous).
- `_default` (variant slot only) — persisted variant slug for
  variant-less registers (§5.1). Reserved everywhere else.

`_default` is the **one literal exception** to the slug grammar
regex above — it starts with `_` and the underscore otherwise
violates the rule. Slug validators must short-circuit on the literal
string `_default` (in the variant slot) before applying the regex.
Every other slug in the system matches the regex without exception.

Note: under Model A there is no period slot, so no period-shaped
slug ban is needed. The v0.11 rules forbidding year-shaped slugs in
non-period positions disappear with the period slot.

**No variant elision.** Canonical FQIDs always carry the variant
segment explicitly. For variant-less registers the variant is
literally `_default`. Display surfaces (CLI output, catalog UI) may
elide `_default/` for readability, but parsers accept only the
canonical form.

### 5.3 Slug curation

Slugs are **curated, never derived** from human-readable Swedish
names (those drift). They are anchored to the underlying provider's
source ID system — SCB's `RegisterId` / `RegvarId` / `VarID`,
Socialstyrelsen's `dataset_name` + deldatamängd identifiers — and
stored in per-provider TOML files at
`reg_meta_build/fqid_slugs/`:

- `reg_meta_build/fqid_slugs/scb.toml` — SCB-provider slugs
- `reg_meta_build/fqid_slugs/sos.toml` — Socialstyrelsen-provider slugs
- `reg_meta_build/fqid_slugs/classifications.toml` — cross-provider classifications

Slugs are build-time inputs: `reg_meta_build` reads them, compiles
slug columns into the DB asset, and `reg_meta`'s query side only
ever sees them through the DB. The TOMLs are committed to this
monorepo alongside the rest of `reg_meta_build`.

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
display_group = "Individer"

[register_variant."34.1335"]
slug = "individer-16plus"
display_group = "Individer"

[variable."34.4"]                       # register_id.var_id
slug = "kon"
```

```toml
# reg_meta_build/fqid_slugs/sos.toml
[register."patientregistret"]            # keyed by SOS dataset_name
slug = "par"

[register_variant."patientregistret.sluten_vard"]
slug = "sluten-vard"

[variable."patientregistret.sluten_vard.DIAGNOS"]
slug = "diagnos"
```

```toml
# reg_meta_build/fqid_slugs/classifications.toml
[classification."SUN2020"]
slug = "sun2020"

[classification."ICD10"]
slug = "icd10"
```

The build pipeline reads slug TOMLs alongside source CSVs/workbooks,
populates `slug` columns on `register`, `variant`, `variable`, and
`classification`, and refuses to compile the DB if any source ID is
missing a slug entry ("RegisterId 99 in Registerinformation.csv but
no slug in reg_meta_build/fqid_slugs/scb.toml"). A precheck step
lists missing slugs without trying the full build, for cleaner
failure mode.

**No `register_version` slug rules.** Model A drops the
`register_version` FQID kind; the ~1,264 v0.11 `register_version`
slug entries are removed. The `register_version` table is dropped
entirely; per-edition publication metadata (mätinformation,
approval dates) routes to reg-meta-docs (variant-level) and the
provenance DB respectively (§5.8). Period tokens that researchers
need are carried by `Source.period` in project_data.json and by
`variable_state.valid_from`/`valid_to` in the catalog.

**Variables are auto-slugged from kolumnnamn.** The build derives a
variable slug from the latest delivery column name (kolumnnamn,
lowercased + kebab-cased). The TOML is used only for exceptions:
collisions, build-time-triage splits, or cross-rename continuity
claims after a provider rename.

**Build-time triage may auto-split a variable into siblings.** When
the source data has multiple states for one (variable, year) that
the heuristic identifies as parallel deliverables (different
positions of a SUN code, different grain levels, etc.), the build
splits into N sibling variables with auto-derived slugs (§5.7).
Sibling slugs are stable across rebuilds.

#### Slug TOML field reference

Every entity (register, variant, variable, classification) uses
the same TOML row shape: a table keyed by the provider's source ID
(`"34"`, `"34.153"`, `"34.4"`, `"SUN2020"`), with the fields below.

**Slug uniqueness scope.** Slugs must be unique within the smallest
scope that the FQID grammar (§5.2) uses to address the entity:

- `register` slugs are unique within a provider.
- `variant` and `variable` slugs are unique within their parent register.
- `classification` slugs are globally unique (provider-independent namespace).

**Common fields (every entity type):**

| Field | Type | Required | Notes |
|---|---|:--:|---|
| `slug` | string | yes | FQID slug. Must match the slug grammar. |
| `display_group` | string | no | Variant only. Presentation grouping label. |
| `deprecated` | bool | no | True if the source ID has been retired (no longer in current deliveries). Slug remains reserved per §5.4. |

**Variant-specific fields (panel structure):**

| Field | Type | Required | Notes |
|---|---|:--:|---|
| `panel_entity_key` | string \| array | no | Variable slug(s) declaring the entity identifier(s) for this variant's natural panel. Single slug for simple keys (`"personnummer"`); array for composite keys (`["peorgnr", "cfar"]`). NULL when the variant has no natural panel (snapshot aux tables, registers without an inherent entity identity). |
| `panel_time_key` | string | no | Either the sentinel `"period"` (use source.period as the time literal — for delivery-aligned variants like LISA) or a variable slug (use the variable's display_name as a row-level column ref — for event/sub-yearly variants like PAR's sluten-vard with INDATUM). NULL when the variant has no natural time dimension (snapshot aux tables). |
| `panel_time_grain` | enum | no | `"delivery"` when time lives in source.period; `"row"` when time lives in a row-level column. Derived signal — could be omitted in favor of inspecting `panel_time_key` shape, but explicit is clearer. NULL when no natural time. |

Example variant TOML:

```toml
[register_variant."34.153"]               # LISA/individer-15plus
slug = "individer-15plus"
display_group = "Individer"
panel_entity_key = "personnummer"
panel_time_key = "period"                 # delivery-aligned
panel_time_grain = "delivery"

[register_variant."25.763"]               # IoT/bostadshushall
slug = "bostadshushall"
panel_entity_key = "bostadshushallsnummer"
panel_time_key = "period"
panel_time_grain = "delivery"

[register_variant."<sos-par-sluten-vard>"]
slug = "sluten-vard"
panel_entity_key = "personnummer"
panel_time_key = "indatum"                # row-level column ref
panel_time_grain = "row"

[register_variant."<lisa-individer-avlidna>"]
slug = "individer-avlidna"
# No panel_entity_key / panel_time_key — snapshot aux table, no natural panel
```

**Bootstrap.** `reg-meta-build seed-slugs` proposes
`panel_entity_key` and `panel_time_key` defaults by reading SCB's
`Tabelldefinitioner.sql` PRIMARY KEY declarations (those identify
the entity-key column per table) and SCB's `Identifierare.csv`
(entity-type semantics). For SOS, the parser reads
`SosVariable.is_join_variable` and `Variabelnamn` patterns. Curators
confirm or override the suggestions during the hand-review pass —
same workflow as for register/variant/variable slugs.

**Edge fields on variables (slug-anchored, inline-table form):**

```toml
[variable."34.137"]
slug = "civilstand"
replaced_by = [
  { provider = "scb", register = "lisa", variant = "individer-15plus",
    variable = "civilstand-v2", note = "Renamed 2019; same concept" }
]
same_as = [
  { provider = "scb", register = "rtb", variable = "civilstand",
    note = "Cross-register concept" }
]
related_to = [
  { provider = "scb", register = "lisa", variant = "individer-15plus",
    variable = "civilstand-detalj", relation_kind = "same_concept_different_grain" }
]
```

- `replaced_by` — directional: this variable was retired and replaced
  by the listed variable(s). At resolve time, an FQID for this
  variable carries a structured "replaced_by" hint surfacing the
  successor. Auto-emitted from SCB `timeseries_event`; TOML curates
  cross-provider edges.
- `same_as` — symmetric: cross-rename equivalence. Resolution
  follows `same_as` transitively (cycles rejected at build).
- `related_to` — symmetric: grain/position/coding siblings.
  `relation_kind` ∈ `same_concept_different_grain` /
  `same_definition_different_column` / `code_vs_label_pair` /
  `import_bug_suspect`. Auto-emitted by build-time triage; TOML adds
  manual edges (typically cross-variable rather than within one
  variable's split).

**Lineage curation (TOML, not a SQL table).** Source-variant pinning
for §5.6 lineage materialization lives in the slug TOMLs as
`[lineage_defaults]` and `[lineage."<consumer_register>.<variable_slug>"]`
blocks. The build reads these directly when running the
interval-overlap join — there is **no `variable_source_lineage`
table** in the universal schema.

```toml
# Defaults per source register live in one small block.
[lineage_defaults]
rtb = "folkbokforda-personer"
iot = "bostadshushall"

# Per-(consumer_register, variable_slug) overrides override the default.
[lineage."lisa.kon"]
source_register = "rtb"
source_variant = "folkbokforda-personer"
```

See §5.6 for the algorithm that consumes these blocks.

### 5.4 Slug immutability

Once a slug is published, it can never change. Committed
`project_data.json` files reference slugs; renaming a slug rots
every project that references it. Concrete rules:

- The slug TOML is **grow-only**: entries are added; entries are
  never deleted or renamed.
- Removed source IDs (a register dropped from a future delivery)
  are flagged `deprecated = true` but retain their slug forever.
- A typo in a slug is corrected by adding a new entry and emitting
  a `replaced_by` edge from the typo'd slug to the corrected one —
  never by editing in place.
- CI enforces these via a snapshot test comparing the current TOML
  to the last committed state; non-additive changes fail the build.

**Surfaces protected.** Under Model A there are exactly four slug
surfaces:

- `register.slug` (per-provider unique)
- `variant.slug` (per-register unique)
- `variable.slug` (per-register unique)
- `classification.slug` (globally unique)

This is one fewer surface than v0.11 (the `register_version.slug`
surface is gone with the period slot).

**Activation.** The rules above bind only after the first tagged
release of the refactored system (the first reg_meta version that
emits FQIDs into a `project_data.json` consumers can commit).
Until that release no external artifact references these slugs, so
the *rule* does not yet protect anything; maintainers may rename,
remove, or restructure entries as the hand-review progresses.

**Pre-v1 escape hatch — the `UNFROZEN` sentinel.** While the file
`reg_meta_build/fqid_slugs/UNFROZEN` exists in the slug directory, the
grow-only refusal is lifted in both directions:

- `reg-meta-build precheck-slugs --update-snapshot` writes
  rename and removal diffs through to `.snapshot.json` instead of
  refusing. Diffs are still reported in the JSON envelope so a
  reviewer sees what drifted.
- The `test_no_removed_or_renamed_slugs` CI test skips its rename
  guard (the parse and addition-coverage tests stay active).

The sentinel is intentional friction-removal: pre-v1 the right
move is to encourage curators to fix typos, normalize conventions,
and reshape sibling groups before any external artifact pins these
FQIDs. Per-rename ceremony in that window discourages exactly the
hygiene we want. At v1 release the sentinel is **deleted in the
same commit that cuts the release tag** — the snapshot at that
commit becomes the immutable baseline and the grow-only gate
re-arms.

The Model A migration itself happens with UNFROZEN in place,
allowing the ~1,264 register_version slug purge and the auto-derived
sibling slugs from build-time triage to land cleanly.

### 5.5 Edge tables

Three edge tables capture variable relationships that the FQID
grammar can't express by position alone.

#### variable_same_as

Symmetric equivalence: "this variable is the same concept as that
variable, across a rename or a cross-register lookup." Slug-anchored
on both endpoints. Stored as bidirectional rows (A→B and B→A) so
the resolver does a single forward lookup.

```sql
CREATE TABLE variable_same_as (
    a_provider TEXT NOT NULL,
    a_register TEXT NOT NULL,
    a_variant  TEXT NOT NULL,
    a_variable TEXT NOT NULL,
    b_provider TEXT NOT NULL,
    b_register TEXT NOT NULL,
    b_variant  TEXT NOT NULL,
    b_variable TEXT NOT NULL,
    note       TEXT,
    PRIMARY KEY (a_provider, a_register, a_variant, a_variable,
                 b_provider, b_register, b_variant, b_variable)
) WITHOUT ROWID;
```

Edges are slug-only — no period/state qualifier. Validity range is
implicit in both endpoints' state histories. Resolution follows
`same_as` transitively (cycles rejected at build via BFS).

The `*_period` columns from v0.11's schema are dropped — period was
never load-bearing for same-as semantics and becomes meaningless
under Model A.

**Auto-derived from SCB `var_id` matching.** Variables are
variant-scoped (§5.1), so the empirical fact that "kon" under
LISA/individer-15plus and "kon" under LISA/individer-16plus is the
same concept is captured by SCB's reuse of `var_id=4` across both
variants. The build emits `(N choose 2)` `variable_same_as` edges
between every variant where SCB declares the same `(register_id,
var_id)` pair. Mechanical; idempotent across rebuilds; populated
from existing SCB source data. For SCB this materializes on the
order of tens of thousands of edges — none of which require
curation. Provenance: `note = "auto:var_id_match"` distinguishes
from TOML-curated cross-register edges.

**TOML curation for cross-register and cross-provider equivalence.**
The same_as TOML form (§5.3) carries equivalences the auto-derive
can't see: cross-register (SCB's `kon` in LISA and RTB), cross-edition
renames within one register, cross-provider links (SOS DORS variable
matched to an SCB Dödsorsaksregistret equivalent if one existed).

#### variable_replaced_by

Directional: variable A was retired and replaced by variable B.

```sql
CREATE TABLE variable_replaced_by (
    predecessor_provider TEXT NOT NULL,
    predecessor_register TEXT NOT NULL,
    predecessor_variant  TEXT NOT NULL,
    predecessor_variable TEXT NOT NULL,
    successor_provider   TEXT NOT NULL,
    successor_register   TEXT NOT NULL,
    successor_variant    TEXT NOT NULL,
    successor_variable   TEXT NOT NULL,
    effective_year       INTEGER,
    note                 TEXT,
    PRIMARY KEY (predecessor_provider, predecessor_register, predecessor_variant, predecessor_variable,
                 successor_provider, successor_register, successor_variant, successor_variable)
) WITHOUT ROWID;
```

Parallel tables at the register and variant grain — each table's PK
matches the grain of the entities it relates, no empty-string
sentinels:

```sql
CREATE TABLE register_replaced_by (
    predecessor_provider TEXT NOT NULL,
    predecessor_register TEXT NOT NULL,
    successor_provider   TEXT NOT NULL,
    successor_register   TEXT NOT NULL,
    effective_year       INTEGER,
    note                 TEXT,
    PRIMARY KEY (predecessor_provider, predecessor_register,
                 successor_provider, successor_register)
) WITHOUT ROWID;

CREATE TABLE variant_replaced_by (
    predecessor_provider TEXT NOT NULL,
    predecessor_register TEXT NOT NULL,
    predecessor_variant  TEXT NOT NULL,
    successor_provider   TEXT NOT NULL,
    successor_register   TEXT NOT NULL,
    successor_variant    TEXT NOT NULL,
    effective_year       INTEGER,
    note                 TEXT,
    PRIMARY KEY (predecessor_provider, predecessor_register, predecessor_variant,
                 successor_provider, successor_register, successor_variant)
) WITHOUT ROWID;
```

**Auto-derived from SCB `timeseries_event`.** SCB records succession
in `timeseries_event` rows with `handelse IN ('Ersatt av',
'Ersätter')` on `entitet IN ('AktuellVariabel', 'Variabel',
'RegisterVariant', 'Register')`. Build pulls these into the matching
table by entity grain — `Register` rows feed `register_replaced_by`,
`RegisterVariant` rows feed `variant_replaced_by`, the variable-grain
rows feed `variable_replaced_by`. The `id1` / `id2` fields identify
predecessor and successor; `handelse = 'Ersätter'` is the inverse
direction, collapsed during the join.

**TOML curation for cross-provider edges.** SOS→SCB succession (or
SCB→SOS) won't appear in SCB's `timeseries_event`. The slug TOML
form (§5.3) carries these as inline-table rows.

**Resolution-time hint.** `Catalog.resolve("scb/lisa/individer-15plus/sysstat")`
returns a `ResolvedVariable` with a structured `replaced_by` hint
when the variable has a successor edge (e.g. `sysstat` → `sysstat11`
effective 2011). Consumers (webapp, mdw) surface this as "this
variable was replaced by X; did you mean…?"

#### variable_related_to

Symmetric: grain/position/coding siblings. Distinguishes from
`same_as` in that related variables share semantic meaning but are
**not substitutable** — they're parallel deliverables that a
researcher would order as different columns.

```sql
CREATE TABLE variable_related_to (
    a_provider TEXT NOT NULL,
    a_register TEXT NOT NULL,
    a_variant  TEXT NOT NULL,
    a_variable TEXT NOT NULL,
    b_provider TEXT NOT NULL,
    b_register TEXT NOT NULL,
    b_variant  TEXT NOT NULL,
    b_variable TEXT NOT NULL,
    relation_kind TEXT NOT NULL,    -- see below
    note          TEXT,
    PRIMARY KEY (a_provider, a_register, a_variant, a_variable,
                 b_provider, b_register, b_variant, b_variable, relation_kind)
) WITHOUT ROWID;
```

`relation_kind` enum:

- `same_concept_different_grain` — SUN at 1-pos / 2-pos / 3-pos / 4-pos
- `same_definition_different_column` — `Hemkommun` / `Skolkommun` under the same SCB variable concept
- `code_vs_label_pair` — `Lid` (3-char country code) ↔ `LNamn` (40-char country name)
- `import_bug_suspect` — flagged by triage as a likely upstream issue (e.g. `int` and `smalldatetime` for same variable)
- `cross_register_same_concept` — manual TOML curation only

**Auto-emitted by build-time triage.** When the build splits one
variable into N siblings (§5.7), `(N choose 2)` `related_to` edges
fire with `relation_kind` derived from the split reason. Provenance
tag (`note = "auto:triage"`) lets curators distinguish derived edges
from manual ones; full overwrite on rebuild for auto edges.

**TOML curation.** Manual `related_to` edges live in slug TOMLs
(§5.3) and persist across rebuilds. Use for cross-register or
cross-variable relationships the algorithm can't see.

### 5.6 Consumer-side binding lineage

LISA, RAMS, and other composite registers don't define their own
variables — they aggregate variables sourced from base registers
(RTB, FTB, ...). Today's reg_meta records the source link via
`variable.source_register_id`, which under v0.11 produced per-cvid
slug-keyed edges via `via_source_id`. Under Model A this becomes a
**state-pair interval-overlap join** materialized as the
`variable_state_lineage` table.

```sql
CREATE TABLE variable_state_lineage (
    consumer_state_id INTEGER NOT NULL REFERENCES variable_state(state_id),
    source_state_id   INTEGER NOT NULL REFERENCES variable_state(state_id),
    valid_from        TEXT    NOT NULL,    -- ISO 8601 'YYYY-MM-DD', inclusive start of intersection
    valid_to          TEXT    NOT NULL,    -- ISO 8601 'YYYY-MM-DD', inclusive end of intersection ('9999-12-31' for open-ended)
    PRIMARY KEY (consumer_state_id, source_state_id)
);
CREATE INDEX idx_variable_state_lineage_consumer ON variable_state_lineage(consumer_state_id);
CREATE INDEX idx_variable_state_lineage_source ON variable_state_lineage(source_state_id);

CREATE TABLE variable_state_lineage_warning (
    consumer_state_id INTEGER NOT NULL REFERENCES variable_state(state_id),
    warning_kind      TEXT    NOT NULL,    -- 'no_source_state', 'ambiguous_source_variant'
    message           TEXT    NOT NULL,
    PRIMARY KEY (consumer_state_id, warning_kind)
);
CREATE INDEX idx_variable_state_lineage_warning_consumer ON variable_state_lineage_warning(consumer_state_id);
```

The lineage validity range uses the same `YYYY-MM-DD` form and
sentinel as `variable_state`, so an edge's `(valid_from, valid_to)`
is directly comparable to the states it joins.

**Algorithm.** For every consumer variable_state with a populated
`variable.source_register_id`, look up the source variable's states
in the same lineage-candidate group, and emit one lineage edge per
state pair whose validity ranges intersect. Edge's `(valid_from,
valid_to)` is the intersection of the two states' validity ranges.

Pseudocode in adapter terms:

```python
for c_state in consumer_states:
    src_states = lookup_source_states(
        c_state.variable.source_register_id,
        preferred_source_variant_id(c_state),  # see below
        c_state.variable.slug,
    )
    for s_state in src_states:
        lo = max(c_state.valid_from, s_state.valid_from)
        hi = min(c_state.valid_to, s_state.valid_to)
        if lo <= hi:
            emit(consumer_state=c_state, source_state=s_state,
                 valid_from=lo, valid_to=hi)
```

**Source-variant resolution.** A source register like RTB has many
variants (folkbokforda-personer, grund-bosattning, inrikes-flyttningar,
...); LISA's Kön comes specifically from RTB's folkbokforda-personer,
not from any of the others. v0.11 picked non-deterministically by
`MIN(cvid)` — the wrong answer, masked by the fact that all SCB
variants ship the same metadata. Under Model A, the lineage linker
uses:

1. **Heuristic default per source register** — `[lineage_defaults]`
   block in the source register's slug TOML:

   ```toml
   [lineage_defaults]
   rtb = "folkbokforda-personer"
   iot = "bostadshushall"
   ```

2. **Per-(consumer_register, variable_slug) override** — when the
   default is wrong for a specific consumer variable:

   ```toml
   [lineage."lisa.inkomst_pension"]
   source_register = "rams"
   source_variant = "individregister"
   ```

3. **Register-level fallback** — when no curated rule applies, the
   linker emits edges to all matching slugs across all source-side
   variants, plus a `variable_state_lineage_warning` row with
   `warning_kind = 'ambiguous_source_variant'` and a message naming
   the candidate source variants. When the linker finds no source
   state at all, it emits `warning_kind = 'no_source_state'`.
   Warnings are queryable via `Catalog.lineage_warnings(fqid)` (a
   thin SELECT on this table); the build pipeline also surfaces them
   in the build log for curator attention.

The default + override pattern handles the 90% case in ~20 lines of
config plus an estimated ~50 manual overrides. The warning
mechanism surfaces gaps for curator attention.

**Why interval-overlap instead of slug-keyed equality.** Empirically,
100% of v0.11's `via_source_id` edges have `src_year == dst_year`
because v0.11's slug grammar baked period into the slug. Under Model
A the linkage is on `(register, variant, variable_slug)` plus
validity intersection — which produces the same result for the
trivial year-equal case but also surfaces real cross-state lineage
(e.g. LISA 2010-2014 sourcing from RTB's pre-rename state, then
2015+ from RTB's renamed state). The interval-overlap shape is
strictly more expressive at no runtime cost.

**Cross-provider lineage.** The algorithm is provider-blind; SOS
registers sourcing from SCB are handled by the same code path. The
`variable.source_register_id` FK already crosses provider boundaries
(it's a global integer ID into `register`).

**Edges removed under Model A.** v0.11's `variable_instance.via_source_id`
column is dropped when `variable_state_lineage` lands. Edge count
drops from 53,635 per-cvid edges to ~15,187 per-state edges (3.5×
shrink). The prelim/slutlig curation tax in v0.11's §5.6 (forcing
canonical-sibling promotion in the source register's slugs) is also
removed — Model A's join doesn't depend on period-slug alignment.

### 5.7 Build-time triage

When the source data has multiple `variable_state` candidates for a
single `(variable, year)` — empirically 2.7% of (triple, year)
buckets in the SCB DB, ~11,945 buckets across 3,281 distinct
variable triples — the build applies a triage algorithm. The
algorithm enforces the universal invariant: **one state per
variable per year, unless explicitly marked as overlapping
multi-vintage**.

**Discriminator precedence (kolumnnamn-primary).** The single most
important signal is the delivery column name — if two candidate
states map to different physical columns in the SCB CSV, they
cannot be the same variable.

1. **Drop truly-empty stubs.** Candidates with no `data_type` and
   no `kolumnnamn` are import artifacts; drop them.
2. **Group remaining candidates by kolumnnamn intersection.**
   Variable aliases are many-to-many; a state may map to multiple
   delivery column names. Use set intersection: states A and B
   share a column iff their kolumnnamn sets intersect.
3. **Multiple distinct kolumnnamn groups → split.** Each group
   becomes a separate variable with an auto-derived sibling slug
   (next subsection). Emit `variable_related_to` edges with
   `relation_kind = same_definition_different_column`.
4. **Within one kolumnnamn group, apply secondary rules.** The
   triage inspects SCB-source fields (`vardemangdsniva` for grain,
   `datatyp` for type, etc.) that exist transiently in the
   pre-triage coalesced IR rows; these source fields don't carry
   into the final `variable_state` schema (grain becomes part of
   the variable slug when a split fires):
   - `data_type` or `data_length` differs → collapse to one state
     (metadata drift within one column; pick the latest values).
   - One pre-triage row has empty `vardemangdsniva`, others have
     populated → drop the empty (metadata stub).
   - Rows differ in `value_set_version_label` only → **keep
     overlapping states** (true multi-vintage classification, e.g.
     LKF 2006 + LKF 2007 in HREG 2007). The `value_set_version_label`
     is the resolver-time discriminator and survives onto
     `variable_state`.
   - Rows differ in `classification_id` only → **keep overlapping
     states** (multi-classification on one column).
   - Only `value_set_id` differs → collapse to one state (code-list
     drift in re-deliveries; pick the latest).

**Sibling slug derivation.** When the algorithm splits one variable
into N siblings, slugs derive deterministically. All suffix
delimiters use `-` to stay inside the §5.2 slug grammar (no
underscores in slugs):

1. **kolumnnamn-derived** when kolumnnamn distinguishes columns:
   `Hemkommun` / `Skolkommun` → `kommun-hem` / `kommun-skol`. Strip
   common prefixes (`Utbild_St` / `Utbild_Sl` → suffixes `-start` /
   `-slut`, appended to the shared stem).
2. **niva-pattern** when only `vardemangdsniva` (the SCB grain label) distinguishes:
   - `\b(\d+)\s*position(er)?\b` → suffix `-{N}pos`
   - `\bnivaold\b` → `-old`
   - `\bgrov(?:\s+gruppering)?\b` → `-grov`
   - `\bdetalj(?:grupp(er)?)?\b` → `-detalj`
   - `\b(alfa|alpha)\b` → `-alfa`
   - `\bhuvudgrupp\b` → `-huvud`
   - `\bavdelning\b` → `-avd`
   - `\bundergrupp\b` → `-under`
3. **datalangd-derived** for code/label pairs: `Lid`/`LNamn` →
   suffixes `-id` / `-namn`.
4. **BLAKE2b hash fallback** when no pattern matches: suffix
   `-x<6 hex>`.

When two siblings derive the same slug, append `-a` / `-b` in cvid
order and emit a build warning.

**Auto-emitted `variable_related_to` edges.** Build splits emit
`(N choose 2)` edges with provenance `note = "auto:triage"`. The
`relation_kind` reflects the split reason. Curators can override
or add edges in TOML.

**Cross-provider considerations.** Each adapter implements its own
version of build-time triage suited to its source format. SCB's
adapter uses the above. SOS's adapter typically doesn't need triage
— SOS workbooks publish one state per (deldatamängd, variable) with
explicit kodlista tidsperiod ranges. Future adapters may need
provider-specific triage rules; the IR contract (§4.4) is what
travels uniformly.

**Curation backlog.** The algorithm auto-resolves ~99% of
collisions. An estimated 200–300 cases need manual TOML curation —
mostly kolumnnamn-derived suffix ambiguities (`Utbild_St` → `-start`
vs `-st`) or genuinely-ambiguous splits the algorithm can't resolve
from data alone. Each manual entry is a 1–2 line TOML override.

### 5.8 What's not in the catalog (docs and provenance)

The universal model is deliberately lean. **No provider-specific
extension tables.** Provider variation is captured by fill-rate on
the universal columns (SCB may populate fewer register-level fields
than SOS; SOS may populate fewer variable-level fields than SCB),
not by parallel `scb_*` / `sos_*` tables. This keeps one consistent
mental model for consumers across all providers.

Three categories of metadata don't live in reg_meta:

**reg-meta-docs (variant-level prose).** Free-form prose and
narrative metadata. Catalog answers "what variables exist and what
shape they have"; docs answer "how to understand them." Per-variant
documents at paths like
`reg-meta-docs/<provider>/<register>/<variant>/<topic>.md`. When
content drifts over time (methodology changes mid-life), the doc
uses chronological Markdown sections. The catalog doesn't try to
model the chronology at the prose level; that's docs' job.

Material that lives in docs:

- **Mätinformation** (SCB per-edition methodology) — variant-level
  doc, with sections per methodology era when methodology drifts.
  Build pipeline deduplicates: if SCB's `registerversionmatinformation`
  is identical across consecutive editions, one section covers the
  combined span.
- **Quality narratives** (SOS LMED Kvalitet_* sheets) — variant-level
  doc, raw rows rendered as Markdown tables.
- **Conceptual time-series breaks** ("we changed surveying methodology
  in 2015") — variant doc with chronological sections.
- **Per-edition release notes** (SCB `registerversionbeskrivning`
  substantive cases beyond boilerplate) — variant doc with per-edition
  Markdown subsections.
- **Long-form register descriptions** beyond what fits in `register.description`.
- **Legal text** (statute citations) — variant or register doc.

**Provenance DB (build artifacts).** Maintainer-only sibling SQLite
artifact (§4.4). Doesn't ship to consumers; available via ATTACH for
debugging.

Material that lives in provenance:

- SCB approval dates (`registerversion_forstagodkannandedatum`,
  `_senastgodkanddatum`).
- SOS workbook delivery metadata (`dataset_version`, `dataset_date`,
  `template_version`, `contact_email`) — until/unless SOS delivery
  history accumulates and warrants a universal model.
- Source file checksums, build manifests, import warnings.
- Raw provider-side IDs when not reused as universal IDs.

**Localization (deferred to v2+).** Translations of register /
variant / variable text into other languages are out of scope for
v1. The catalog carries one canonical text per field (typically the
provider's native language — Swedish for both SCB and SOS). When SOS
DCAT-AP provides `*_sv` and `*_en` variants, the build maps `*_sv`
to the universal columns and drops `*_en` for now. Future translation
support lands as a separate table or sidecar DB.

**Structural sensitivity flags** stay in the catalog as universal
`variable` columns (`is_sensitive`, `is_identifier`) — these are
MONA-critical and apply to every variable regardless of provider.
The original `unika_summary` table is dropped after **A2.1's
coalescer** consumes `version_forsta` / `version_sista` to derive
`variable_state.valid_from` / `valid_to`; the sensitivity-flag lift
in A1.2 alone is not sufficient to retire the table.

**`is_identifier` downstream semantics.** A variable with
`is_identifier=true` will be pseudonymized at delivery. For SCB
this means SCB prefixes the column header with `LopNr_` (or a
project-specific prefix like `P1105_LopNr_PersonNr`). The flag is
broad: not just the subject identifier (`PersonNr` / variable
slug `personnummer`) but also all related identity columns
(`PersonNrMor`, `PersonNrFar`, `PersonNrAdMor`, `PersonNrSambo`,
`PersonNrVard1`, etc. — every column that identifies a person and
will be pseudonymized). The narrower question "which identifier is
the *subject* of this variant?" is answered by
`variant.panel_entity_key` (§5.3), not by `is_identifier`. The two
fields serve different purposes:

- `variable.is_identifier`: per-variable; "this column gets
  pseudonymized at delivery." Used by:
  - **SPA authoring**: default `display_name` for the binding becomes
    `LopNr_<delivery_column_name>` (researcher can override to a
    project-specific prefix).
  - **Validator** (info-level): emits
    `identifier_without_pseudonymization_prefix` if the binding's
    `display_name` doesn't contain a recognized pseudonymization
    prefix (`LopNr` or pattern matching project prefixes); not
    blocking — researcher might be deliberate.
  - **MONA bundle PII scanner**: treats these columns as PII at
    extract time regardless of the delivered display_name.
  - **Order export**: optionally annotates identifier columns in the
    order template for audit; SCB intake also knows from their side.
- `variant.panel_entity_key`: per-variant; "this single column slug
  is the canonical subject identifier for joining." Used only by
  panel-default inheritance (§6.4).

### 5.9 What this means for project_data.json

Descriptive; the schema-level changes land in §6. Headline points:

- Source.`columns` is renamed `bindings`; each binding's `name` field
  is renamed `variable`. A binding's `variable` is the **4-segment
  binding FQID** `<provider>/<register>/<variant>/<variable>`;
  `display_name` still carries the delivered SQL column header.
- Source `register_version` (4-seg) is replaced by `register_variant`
  (3-seg) plus an explicit `period` field. `period` is polymorphic:
  int (year), period-token string (`"2018-Q1"`, `"HT2020"`, etc.),
  range object (`{"from": ..., "to": ...}`), or the snapshot sentinel
  `"_default"`. Handles yearly, sub-yearly, event-range, and snapshot
  shapes in one field.
- `period` is **always required**. For snapshot aux tables the value
  is `"_default"` (or a known snapshot date when meaningful). The
  structural validator has zero reg_meta dependency.
- Panel `entity_key` / `time_key` inherit from `variant.panel_template`
  (declared in slug TOML; §5.3) when omitted in project_data. Explicit
  panel-level or member-level values override. The default is "this
  variant's natural panel structure" — researchers don't repeat
  themselves for the common case.
- `value_set_version` strings (`Kon@2023`, `SUN@2020`) are replaced by:
  - For classifications: a `class/<...>` FQID (version baked in slug).
  - For ad-hoc value sets: inline codes in `project_data.codes.json`,
    keyed by binding FQID — no synthetic value-set ID needed.
- The LISA composite-source gap is resolved at the data layer via
  `variable_state_lineage` (§5.6). UI presentation question (how to
  surface RTB/IoT lineage when browsing under LISA) is §9 webapp work.
- Replaced-by hints from resolution surface in the SPA: "this
  variable was replaced by `sysstat11` effective 2011; redirect?"

### 5.10 Library API surface

The webapp's `/api/catalog/*` endpoints (§9.5) are thin wrappers
around a stable in-process reg_meta API. Method signatures (normative
— `/api/catalog/*` shape derives from these):

```python
from reg_meta import Catalog, Period  # Period = int | str | dict | None

class Catalog:
    @classmethod
    def open(cls, path: str | None = None) -> "Catalog": ...

    # Longitudinal resolution — full state history + edges.
    def resolve(self, fqid: str) -> ResolvedVariable: ...

    # Resolution at a specific period — one VariableState.
    # `period` accepts the same forms as Source.period (int, period-token, range,
    # "_default" snapshot sentinel). Required.
    # `value_set_version` disambiguates the rare LKF-shape multi-vintage case.
    def resolve_at(
        self,
        fqid: str,
        period: Period,
        *,
        value_set_version: str | None = None,
    ) -> VariableState: ...

    # State history (convenience: equivalent to resolve(fqid).states).
    def states(self, fqid: str) -> list[VariableState]: ...

    # Succession edges, list-returning for symmetry with the other accessors.
    def predecessors(self, fqid: str) -> list[VariableRef]: ...
    def successors(self, fqid: str) -> list[VariableRef]: ...

    # Grain/coding siblings.
    def related(self, fqid: str) -> list[RelatedRef]: ...

    # Composite-source lineage edges (consumer-side variables only).
    def lineage(self, fqid: str) -> list[LineageEdge]: ...

    # Build-time lineage warnings for a binding (no_source_state, ambiguous_source_variant).
    def lineage_warnings(self, fqid: str) -> list[LineageWarning]: ...
```

Example usage:

```python
catalog = Catalog.open()                 # mmap'd SQLite

variable = catalog.resolve("scb/lisa/individer-15plus/kon")
# variable.states = [VariableState(...), VariableState(...)]
# variable.replaced_by = [VariableRef(...)] (successors only on this attribute)
# variable.related_to  = [RelatedRef(...)]
# variable.same_as     = [VariableRef(...)]
# variable.lineage     = [LineageEdge(...)] (consumer-side)

state = catalog.resolve_at("scb/lisa/individer-15plus/kon", period=2018)
# state.value_set, state.data_type, state.delivery_column_name (alias), etc.

# Succession: two separate accessors, each list-returning.
predecessors = catalog.predecessors("scb/lisa/individer-15plus/sysstat11")
# → [VariableRef(.../sysstat)]
successors   = catalog.successors("scb/lisa/individer-15plus/sysstat")
# → [VariableRef(.../sysstat11)]

# Grain siblings.
siblings = catalog.related("scb/lisa/individer-15plus/utbildningsinriktning-3pos")
# → [RelatedRef(.../utbildningsinriktning-1pos, relation_kind="same_concept_different_grain"),
#    RelatedRef(.../utbildningsinriktning-4pos, relation_kind="same_concept_different_grain"), ...]
```

Exact dataclass shapes (`ResolvedVariable`, `VariableState`,
`VariableRef`, `RelatedRef`, `LineageEdge`, `LineageWarning`,
`Period`) live in [reg_meta/DESIGN.md](reg_meta/DESIGN.md). The
contract from this spec's perspective: every FQID resolves through
one entry point; period-specific resolution has one entry point;
cross-variable relationship traversal has one list-returning entry
point per edge type.

**Why two methods for succession.** `predecessors` and `successors`
are split (rather than a single `replaced` returning a dict) so every
edge-traversal accessor returns `list[...]` uniformly. The
longitudinal `resolve(fqid).replaced_by` attribute carries the
**outbound** edges (successors) since "X was replaced by Y" is the
natural directional read; inbound (predecessor) traversal is the
explicit two-step `predecessors(fqid)` call.

**Ambiguity handling.** `resolve_at(fqid, period)` returns a single
state in the unambiguous case (99% of variables, after build-time
triage). For the rare multi-vintage case (LKF-shape, ~0.05%), it
raises `VariableStateAmbiguous` carrying candidate states keyed by
`value_set_version_label`; consumers supply the label to disambiguate
via the `value_set_version=` keyword argument.

### 5.11 Glossary

| Term | Kind | Definition |
|---|---|---|
| variable | entity | A `variable` row: a slug-identified, variant-scoped concept. Has 1..N states across time. |
| variable state | entity | A `variable_state` row: per-era shape of a variable, with `(valid_from, valid_to)` (ISO 8601) and the data type, length, value set, version label for that era. Canonical unit of resolution at a specific period. |
| binding | entity | A 4-segment FQID referencing a variable: `<provider>/<register>/<variant>/<variable>`. Resolves to a `ResolvedVariable` (longitudinal) or single `VariableState` (when period context supplied). Also the project_data.json object that declares "include this variable in this source's extract" (§6.3). |
| variable_alias | entity | A delivery column name (`PersonNr`, `Kon`, `LopNr_PersonNr`) attached to a `variable_state`. SCB pseudonymizes identifier columns at delivery by prefixing `LopNr_`; the metadata stores the un-prefixed name. Multiple aliases per state possible. |
| classification | entity | A named versioned vocabulary (SUN2020, ICD10). Provider-independent; addressed via `class/<slug>` (version in slug). |
| value_set | entity | A code list attached to a `variable_state`. Content-addressed via `member_hash` for dedup. Carries an optional FK to `classification` when the value_set is a (possibly year-projected) subset of a named classification. Never exposed via FQID. |
| slug | string | A curated, immutable identifier token (`lisa`, `kon`, `_default`). Lives in `reg_meta_build/fqid_slugs/*.toml` (§5.3). |
| variable-slug stem | string | The final segment of a binding FQID — the slug naming the variable concept. Used by spine matching (§10) and code lookups (§6.6). |
| binding FQID | string | 4-segment: `<provider>/<register>/<variant>/<variable>` (§5.2). |
| variant FQID | string | 3-segment: binding FQID minus the variable. |
| classification FQID | string | 2-segment: `class/<slug>` (§5.2). |
| state validity range | tuple | `(valid_from, valid_to)` on a `variable_state`. ISO 8601 TEXT (year, year-month, or date precision). `valid_to` NULL = open-ended. |
| period | field | `Source.period` in project_data.json. Always required; polymorphic (int / period-token / range / snapshot sentinel). Drives `resolve_at` for the source's bindings. See §6.2. |
| panel_template | metadata | On `variant`: declares the natural panel structure as `(panel_entity_key, panel_time_key, panel_time_grain)`. Inherited by Panel members in project_data when not overridden. |
| value_set_version_label | column | On `variable_state`. Carries the SCB `vardemangdsversion` label (e.g. "LKF 2006-01-01") when meaningful as a state discriminator (rare; multi-vintage classification case). NULL otherwise. |

**Universal English ↔ SCB Swedish vocabulary** (for the column-rename
pass at stage A1):

| SCB Swedish | Universal English | Lives on |
|---|---|---|
| registernamn | name | register |
| registerrubrik | (dropped) | — (redundant with `name`) |
| registersyfte | purpose | register |
| registervariantnamn | name | variant |
| registervariantrubrik | (dropped) | — (redundant with `name`) |
| registervariantbeskrivning | description | variant |
| registervariantsekretess | (dropped → docs) | — (legal text; reg-meta-docs) |
| registerversion_* | (dropped → docs/provenance) | — (mätinformation → docs; approval dates → provenance) |
| variabelnamn | name | variable |
| variabeldefinition | definition | variable |
| variabelbeskrivning | description | variable |
| variabeloperationell_definition | (merged into `description`) | — (inlined at ingest when distinct + non-empty) |
| variabelreferenstid | (dropped) | — (variant's panel_time_key captures this) |
| variabelhamtadfran | (dropped) | — |
| variabelregister_kalla | source_register_text | variable (human attribution) |
| variabelextern_kommentar | (dropped → docs) | — (editor notes; covered by states + docs) |
| mattenhet | measurement_unit | variable (NULL when source was "Okänd") |
| datatyp | data_type | variable_state (normalized lowercase canonical set) |
| datalangd | data_length | variable_state (INTEGER) |
| vardemangdsversion | value_set_version_label | variable_state (overlap discriminator) |
| vardemangdsniva | (dropped post-triage) | — (becomes part of variable slug when triage splits; see §5.7) |
| värdekod | code | code (`value_code` table) |
| värdebenämning | label | code |
| kolumnnamn | delivery_column_name | variable_alias |
| populationnamn | name | population (variant-scoped) |
| populationdefinition | definition | population |
| populationkommentar | comment | population |
| populationdatum | date_range | population (free-text, often a range) |
| objekttypnamn | name | object_type (variant-scoped) |
| objekttypdefinition | definition | object_type |
| kanslig_variabel / kanslig_variabel_ibland | is_sensitive | variable (boolean; both source values fold into one flag) |
| identitetsvariabel | is_identifier | variable (boolean; implies LopNr-pseudonymization at delivery) |
| version_forsta / version_sista | valid_from / valid_to | variable_state (mapped to ISO 8601 at ingest) |
| Ersatt av / Ersätter | replaced_by | `variable_replaced_by` edge source |
| Tidsseriebrott | (dropped → docs) | — (conceptual breaks captured in variant docs; structural breaks captured by state transitions + replaced_by edges) |

**SOS DCAT-AP field disposition** (drop or map to universal):

| SOS DCAT-AP field | Universal English | Lives on |
|---|---|---|
| title_sv | name | register |
| title_en | (dropped — localization deferred to v2) | — |
| description_sv | description | register |
| description_en | (dropped) | — |
| temporal_coverage_sv | (dropped) | — (derivable from variable_state valid_from/valid_to) |
| population_sv | definition | population (variant-scoped) |
| population_en | (dropped) | — |
| update_frequency_sv | (dropped → docs) | — |
| publisher_sv | name | provider (already universal) |
| access_rights_sv | (dropped) | — (single-value column in source) |
| legislation_sv | (dropped → docs) | — |
| dataset_version / dataset_date / template_version / template_date / contact_email | (dropped → provenance) | — (build artifact / workbook delivery context) |
| Kvalitet_* sheets | (dropped → docs) | — (LMED quality narrative; variant doc with rendered Markdown tables) |

The rule of thumb: **column names are universal English; column
values are provider-native verbatim**. The validator emits errors
against strings; resolution turns strings back into entities.

## 6. The shared schema: `project_data.json`

The load-bearing artifact connecting every step. Built on top of
the reg_meta FQID grammar (§5). Single file, owns:

- Sources, each pinned to a `register_variant` FQID plus an explicit `period`
- Bindings per source, each pinned to a **binding FQID** (reg_meta
  identity) plus a `display_name` (SQL column header in delivered
  data)
- Panels (entity_key + member time_keys), keyed on `display_name`
  strings; default keys inherited from `variant.panel_template` (§5.3)
- Per-binding tunables under a namespaced `reg_monabundle` block
- Value codes via classification FQID references or held inline in
  sibling `project_data.codes.json`

### 6.1 Top-level shape

Bump `schema_version` semantics: `"2.0.0"` is the Model A contract.

| Field | Type | Required | Description |
|---|---|:--:|---|
| `schema_version` | string | yes | Semantic version of the schema. Model A files use `"2.0.0"`. v0.x files (`"1.x.x"`) are explicitly rejected by SPA load and CLI; pre-v1 policy is hard-reject, no migration code. |
| `steward` | string enum | yes | `"global"` / `"ifau"` / `"swecov"`. Identifies which deployment authored the file. |
| `reg_meta_version` | string | yes | Release tag of the reg_meta DB asset used during authoring (e.g. `reg_meta/v1.0.0`). Model A files require a v1.x reg_meta release tag. Best-effort drift detection on later resolves. |
| `name` | string | yes | Project identifier (human-readable). |
| `sources` | `array<Source>` | yes | List of data sources (tables) in the project. |
| `panels` | `array<Panel>` | no | Panel definitions over sources. Default `[]`. |
| `reg_monabundle` | object | no | Namespaced block for `reg_monabundle` settings (per-binding extract tunables; see §6.5). Block keys are 4-segment binding FQIDs. |

No other top-level fields are allowed; future additions must be
namespaced (e.g. `reg_monabundle:`, `reg_mockdata:`, `swecov:`). A
`value_sets` top-level block from earlier drafts is removed —
codes live in sibling `project_data.codes.json` (§6.9), and
classifications are referenced inline on each binding.

### 6.2 Source

```json
{
  "name": "lisa_2018",
  "register_variant": "scb/lisa/individer-15plus",
  "period": 2018,
  "bindings": [ /* Binding objects */ ]
}
```

| Field | Type | Required | Description |
|---|---|:--:|---|
| `name` | string | yes | Internal source handle (e.g. `lisa_2018`); unique within the spec. Referenced by panel members. |
| `register_variant` | string (FQID) | yes | reg_meta variant FQID: `<provider>/<register>/<variant>` (3 segments, §5.2). |
| `period` | int \| string \| object | **yes** | The period of data this source represents. Polymorphic; see "Period forms" below. The resolver uses `period` to pick the matching `variable_state` for each binding. |
| `bindings` | `array<Binding>` | yes | Variable bindings to include. At least one. |

The Source's v0.11 `register_version` field (4-segment FQID embedding
period in slot 4) is replaced by `register_variant` (3-segment) +
explicit `period`. Provider, register, and variant are encoded in the
FQID; data context (which period) lives in `period`. Resolution
combines both: the variant's `variable_state` rows are filtered by
`valid_from <= period <= valid_to` to pick the binding's state.

#### Period forms

The `period` field accepts the union of:

- **`int`** — bare year (`2018`). Most common.
- **`string` — period token** — `"2018-01"` (month), `"2018-12-31"` (date), `"HT2020"` (Swedish autumn term), `"VT2020"` (spring term), `"2018-Q1"` (quarter), `"2018-H1"` (half-year). Same forms `time_key` accepts.
- **`object` — explicit range** — `{"from": ..., "to": ...}` for sub-yearly or event data spanning a range. Endpoints follow the same int/string forms. Example: `{"from": "2018-01-01", "to": "2020-06-30"}` for a single delivery covering 2.5 years of event data.
- **`string` — snapshot sentinel** — `"_default"` for variant-less snapshot tables where the variant has one open-validity state and no natural period token.

The polymorphic shape handles every register shape uniformly: yearly
LISA editions, sub-yearly events, snapshot aux tables, multi-year
range deliveries. No separate snapshot/time-indexed branch in the
validator; one period field carries the semantics.

**Period and panel time_key.** When a source is referenced by a Panel
member without an explicit `time_key`, the member's effective time_key
is derived from the source's `period` (when the variant's
`panel_time_key` metadata is set to `"period"`; see §6.4). For sources
referenced from panels where time lives in a row-level column
(`panel_time_key` is a variable slug), the source's `period` declares
the source's scope, and the panel's row-level time_key drives the join.

**`Source.period` ↔ `TimePoint` shape mapping.** `Source.period`'s
range form is the bare object `{"from": ..., "to": ...}`, while
`TimePoint` ranges use the discriminated wrapper
`{"range": {"from": ..., "to": ...}}`. When a Panel member inherits
its `time_key` from a source whose `period` is a range, the value is
wrapped in `{"range": ...}` before being treated as a `TimePoint`.
Equivalently:

| `Source.period` value          | Resulting inherited `TimePoint`           |
|--------------------------------|-------------------------------------------|
| `2018` (int)                   | `{"period": 2018}`                        |
| `"2018-Q1"` (string)           | `{"period": "2018-Q1"}`                   |
| `{"from": "...", "to": "..."}` | `{"range": {"from": "...", "to": "..."}}` |
| `"_default"` (snapshot)        | `{"period": "_default"}`                  |

The bare `{"from", "to"}` object is **only** legal as a `Source.period`
value; it is rejected wherever a `TimePoint` is expected. This keeps
`TimePoint`'s discriminated-union grammar unambiguous.

`where` (per-table SQL predicate) is **not** in the v1 baseline
schema. Cohort filtering inside the MONA bundle still happens at
the `sql_table` / `file_source` level in `reg_monabundle`'s
`configure()`, where it belongs: different tables in one source
have different filter columns (LISA's `AR`, PAR's `INDATUM`), and
the filter is operationally a property of the MONA-side runner,
not of the order spec. Some stewards (notably SWECOV, which
delivers narrowed subsets of large SQL panels) will want to record
a per-source filter in the spec for audit/repro reasons; that is
added under the steward's own namespaced block (e.g.
`"swecov": { "filters": { "lisa_2018": "AR > 2015" } }`) rather
than in the v1 baseline.

### 6.3 Binding

```json
{
  "variable": "scb/lisa/individer-15plus/personnummer",
  "display_name": "LopNr_PersonNr",
  "type": "id",
  "id_subtype": "string"
}
```

The container is `bindings` (renamed from v0.11 `columns`); the
identifying field is `variable` (renamed from v0.11 `name`). Each
binding declares one variable to include in the source's extract.

| Field             | Type           | Required | Description |
|-------------------|----------------|:--------:|-------------|
| `variable`        | string (FQID)  | yes | Binding FQID: `<provider>/<register>/<variant>/<variable>` (4 segments, §5.2). The first three segments must equal the source's `register_variant`. |
| `display_name`    | string         | no  | Actual column header in the delivered data. Optional in the schema because at authoring time the value is just an echo of reg_meta's `variable_alias.delivery_column_name` for the binding at the source's `period` — when absent, **reg_meta-backed consumers** (webapp, kit-build, semantic validator) resolve the default from reg_meta. Becomes meaningfully distinct from the default at realign time (§7) when project prefixes are applied (e.g. `PersonNr` → `LopNr_PersonNr` → `P1105_LopNr_PersonNr`) or at order time when a user renames a column. Steward catalogs (§9.1) typically omit it. Reg_meta-free consumers (the bundle on MONA, `reg-mockdata` against a kit) **never** see unresolved `display_name`: bundle build (§7) and kit build (§8) materialize defaults from reg_meta before emitting their artifacts. |
| `type`            | enum           | yes | One of `id`, `categorical`, `numeric`, `date`, `datetime`, `opaque`. |
| `id_subtype`      | enum           | no  | For `id` type: `integer` or `string`. Auto-detected from the data when omitted. |
| `numeric_subtype` | enum           | no  | For `numeric` type: `integer` or `double`. |
| `date_format`     | string         | no  | For `date` type: Python `strptime` pattern. Carries granularity — `"%Y"` is year-only, `"%Y-%m"` is month, `"%Y%m%d"` is day. Default `"%Y-%m-%d"`. |
| `datetime_format` | string         | no  | For `datetime` type: `strptime` pattern with time. Default `"%Y-%m-%d %H:%M:%S"`. Time zones are out of scope for v1. |
| `value_set`       | string (FQID)  | no  | For `categorical` type: a classification FQID (`class/<slug>` — 2 segments, version baked into slug; §5.2). When absent, codes live in `project_data.codes.json` keyed by this binding's FQID (§6.6). |

Two identifiers, two purposes: `variable` (the binding FQID) is
the **reg_meta identity** — used for validation, code-set lookup,
and cross-edition continuity. `display_name` is the **runtime data
column header** — what panel keys reference, what realign matches
against, what `reg_monabundle`'s extract queries read. When
`display_name` is omitted, consumers resolve the default from
reg_meta's `variable_alias.delivery_column_name` for the binding
at the source's `period`; the moment the user (or realign) sets an
explicit value, it overrides. This default-resolve rule is what
makes the field optional in the schema without forcing every
consumer to special-case absence.

**Alias resolution at `period`.** A variable may carry several
`variable_alias` rows (renames across editions, parallel headers
for the same column). The default `display_name` for a binding is
the alias on the `variable_state` matching the source's `period`;
if multiple aliases match, prefer the most-recently-asserted; if
that's still ambiguous, alphabetical on the alias string is the
final deterministic tie-break. This is a query rule, not a schema
rule — it lives in `reg_meta`'s `Catalog.resolve_at()` and is what
every reg_meta-backed consumer ends up calling when `display_name`
is absent.

**Identifier columns and the `LopNr_` prefix.** For variables flagged
`is_identifier=true` in reg_meta (§5.8), SCB pseudonymizes at
delivery by prefixing the column header with `LopNr_` (or with a
project-specific prefix like `P1105_LopNr_PersonNr`). Reg_meta
stores the un-prefixed metadata (`PersonNr`, `CFAR-Nummer`, etc.).
Researchers' delivered data carries the prefixed form — they set
`display_name` to the actual delivered header. Default `display_name`
resolution falls back to the un-prefixed metadata name when no
override is set.

**Display-name collisions.** Two bindings on the same source
resolving to the same `display_name` (either both explicitly, or
one explicit + one resolving to the same value) produces a
`display_name_collision` validation error (§6.8.0). Remediation:
the user sets an explicit `display_name` on one of the bindings,
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
`class/swecov-terms-v1`) or with codes inline in
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

Under Model A, panel `entity_key` and `time_key` can be omitted at
the panel level: when absent, they're inherited from
`variant.panel_template` (§5.3) on the member's source's variant.
The variant's natural panel structure (curated in slug TOML)
provides the default; explicit panel-level or member-level values
override.

| Field         | Type                          | Required | Description |
|---------------|-------------------------------|:--------:|-------------|
| `panel_id`    | string                        | yes | Unique panel identifier within the spec. |
| `entity_key`  | EntityKey                     | no  | Panel-level default entity-key column(s). When omitted, inherited from each member's source's `variant.panel_entity_key`. At least one member must end up with an effective `entity_key` (inherited, default, or override) for the panel to validate. |
| `time_key`    | TimeKey                       | no  | Panel-level default time-key. When omitted, inherited from each member's source's `variant.panel_time_key` — which may be the sentinel `"period"` (use the source's `period` as a literal) or a variable slug (use the variable's `display_name` as a column ref). |
| `members`     | `array<string \| PanelMember>` | yes | Member sources, optionally with per-member overrides. |
| `comment`     | string                        | no  | Free-text description. |

Type aliases:

```text
EntityKey = string | string[]                                  // always column refs
TimePoint = int                                                // literal year
          | string                                             // always a column ref (never a literal period — use {"period": ...} for that)
          | {"period": int | string}                           // literal period
          | {"range": {"from": int|string, "to": int|string}}  // range (sub-yearly / event)
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
- **`{"range": {"from": ..., "to": ...}}`** — explicit period
  range. Used for sub-yearly / event data where a single source
  delivery spans multiple periods (e.g. PAR events covering
  `{"range": {"from": "2018-01-01", "to": "2020-06-30"}}`).
  Endpoints follow the same int/string forms as bare periods.

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
  "binding_options": {
    "scb/lisa/individer-15plus/salary": {"suppress_k": 20}
  }
}
```

`binding_options[<binding_fqid>]` is a dict of per-binding tunables,
keyed by the binding's FQID (§5.2). Survives realign renames
automatically — the binding FQID is immutable; `display_name` is
not, so keying by display_name would silently drop overrides on
rename. Known keys:

- `suppress_k` (int) — disclosure-control threshold for this binding's
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

Future per-binding option keys are added here as the need arises
(e.g. length caps for opaque generation, override of
auto-detected id_subtype). The block is open-ended;
`reg_monabundle.validate_block` accepts known keys strictly —
unknown keys raise.

### 6.6 Value codes

Codes live in sibling `project_data.codes.json` — never in
`project_data.json` itself. The file is a flat, FQID-keyed object
with two keyspaces sharing one dictionary:

- **Classification FQIDs** (e.g. `class/sun2020`): the canonical
  code list for that classification, dereferenced from reg_meta at
  kit-build time. Shared across every column that references the
  classification via `value_set`.
- **Binding FQIDs** (e.g. `scb/lisa/individer-15plus/civilstand`):
  the codes for a single column whose `value_set` field is absent —
  i.e. the codes are not part of a named classification.

```json
{
  "class/sun2020": {
    "codes": [ /* SUN2020 full code list */ ]
  },
  "scb/lisa/individer-15plus/civilstand": {
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
references are stored only as FQIDs on the binding (no inline
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
in the current reg_meta — "FQID `class/foo2010` not found; closest
match: `class/foo2012`." Deprecated entries (slug TOML
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

1. Checks that `variable` is a structurally well-formed binding FQID.
2. Checks that its first three segments equal the source's
   `register_variant` FQID.
3. Checks that the FQID resolves to a known `variable` in reg_meta,
   then to a `variable_state` whose validity covers the source's
   `period` (hard error on miss).

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
- Every source's `register_variant` is a structurally well-formed
  variant FQID (3 segments: `<provider>/<register>/<variant>`).
- Every source has a `period` field of type `int | str | object`. If
  string, it must match a period grammar form (`YYYY`, `YYYY-MM`,
  `YYYY-MM-DD`, `HTYYYY`, `VTYYYY`, `YYYY-Q[1-4]`, `YYYY-H[12]`)
  or the snapshot sentinel `"_default"`. If object, must match
  `{"from": ..., "to": ...}` with valid endpoints in the same grammar.
- Every binding's `variable` is a structurally well-formed binding
  FQID (4 segments). The first **3** segments must equal the source's
  `register_variant` — enforced as a structural check, no reg_meta
  needed.
- `value_set` (when present) is a structurally well-formed
  classification FQID: `class/<slug>` (2 segments, leading `class/`;
  version baked into the slug, §5.2).
- Every panel member has an effective `entity_key` and effective
  `time_key` (inherited from variant's `panel_template`, from
  panel-level defaults, or from member-level overrides; §6.4).
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

- Every source's `register_variant` FQID resolves to a known
  variant row in reg_meta. Code: `fqid_unresolved`, level `error`.
- Every binding's `variable` (binding FQID) resolves to a known
  `variable` (following `same_as` curated links if present; §5.5).
  Code: `fqid_unresolved`, level `error`.
- Every binding resolves to a `variable_state` at the source's
  `period`. If no state covers that period, code: **new**
  `period_outside_state_validity`, level `error`. For range
  periods that cross a state transition, emit **new**
  `binding_state_drifts_within_period`, level `info` (the resolver
  returns per-state subsets at extract time).
- When the binding has a `variable_replaced_by` edge whose
  `effective_year` is at or before the source's `period`, emit
  **new** `variable_replaced`, level `info`, with a structured
  hint pointing at the successor binding.
- For the rare multi-vintage case (LKF-shape), when multiple
  `variable_state` rows match `(binding, period)`, emit **new**
  `binding_state_ambiguous`, level `warning`, listing candidate
  `value_set_version_label` values. The resolver picks
  deterministically (most-recently-asserted); the warning surfaces
  the ambiguity.
- Every `value_set` (classification FQID) resolves to a known
  `classification`. Code: `value_set_missing`, level `error`.
- Deprecated entries (slug TOML `deprecated: true`) resolve
  normally but emit `deprecated_traversal` at level `warning`.
- For each FQID in the spec, the steward's catalog admits it.
  When a project's binding or `register_variant` lies outside the
  loaded steward catalog, emit `fqid_outside_steward_catalog` at
  level `warning` (not error — this is also the deliberate
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
the same validator but **downgrades `fqid_unresolved`,
`value_set_missing`, and `period_outside_state_validity` from
`error` to `warning`** so the deployment doesn't fail to start
when reg_meta evolves out from under a steward's committed catalog. Affected bindings are removed from
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
binding. The bundle on MONA has no reg_meta and never needs one for
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
        {"binding": "scb/lisa/individer-15plus/lopnr", "display_name": "LopNr"},
        {"binding": "scb/lisa/individer-15plus/fobostat", "display_name": "FoBoStat"}
      ],
      "extra_in_data": ["P1105_LopNr_PersonNr", "UnexpectedCol"],
      "type_mismatches": [
        {"binding": "scb/lisa/individer-15plus/birthdate",
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
  matching binding to the SQL header. `variable` (the binding FQID)
  is never modified — reg_meta identity is stable across the project
  lifecycle. The UI may suggest pairings heuristically (e.g. when
  one is a project-prefixed form of the other), but the user
  confirms.
- **Remove from spec.** A remaining `missing_in_data` entry is
  truly absent; drop the binding from its source.
- **Add to spec.** A remaining `extra_in_data` entry is a real
  new delivered column; the UI prompts for a binding FQID (chosen
  against reg_meta via catalog search) and a `type`, and stores the
  delivered SQL string as `display_name`.
- **Resolve type mismatch.** For every entry in `type_mismatches`,
  either accept the SQL type into the spec (changes the binding's
  `type` / `numeric_subtype` / etc.) or remove the binding from the
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
  "schema_version": "2.0.0",
  "project": "swecov-education",
  "generated_at": "2026-03-04T10:30:00Z",
  "reg_meta_version": "reg_meta/v1.0.0",

  "sources": {
    "lisa_2018": {
      "row_count": 8492768,
      "bindings": {
        "scb/lisa/individer-15plus/kon": {
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
        "scb/lisa/individer-15plus/ink": {
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
    {"binding": "scb/lisa/individer-15plus/kon",
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
| `bindings`    | object  | yes | Map: **binding FQID** → per-binding stats. Keyspace is FQIDs (not display names) so cross-edition rename does not silently break consumers. |

**Per-binding (`sources.<name>.bindings.<binding_fqid>`).**

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
the spec. Classification FQIDs (`class/sun2020`) are
**dereferenced** at kit-build into inline entries; ad-hoc inline
sets are passed through unchanged. The same lookup path
(`codes_by_fqid[<binding-or-classification-fqid>]`) covers both
post-kit. This is what makes `reg_mockdata` reg_meta-free and the
kit reproducible years later.

### Reproducibility

Same spec + same codes + same stats → same generation kit → same
mock data. RegMeta version drift is inert because every code list
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
index: `register_variant FQID → set<binding FQID>` and `register
FQID → period-range`, derived directly from `sources[].register_variant`,
`sources[].period`, and `sources[].bindings[].variable`. The validate
endpoint and the variable-list authoring endpoints consult this
index. The `fqid_outside_steward_catalog` warning (§6.8.3) fires when
a researcher's project references an FQID not in the index.

**Maintenance.** When reg_meta gains new entries (a new variant, a
new variable on an existing version), nothing happens
automatically — the steward decides whether to admit them by
opening their catalog in the webapp against `global` and adding
the new columns, then committing the result back into
`reg_webapp/stewards/<id>/steward.project_data.json`. Optional
CLI: `reg-meta-build steward-diff
<steward.project_data.json>` lists FQIDs in reg_meta that aren't
in the steward catalog, plus FQIDs in the catalog whose
`register_variant` reg_meta no longer knows about (rare — slugs
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

All endpoints versioned under `/api/`. Read endpoints edge-cacheable;
write endpoints (`POST`) not. Catalog browse paths use FQID segments
directly.

**Context / deployment** (unchanged)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/context` | Deployment identity, branding, build info. |

**Catalog** — single canonical endpoint plus sub-endpoints for the
Model A relationship surface.

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/catalog` | Top-level: every provider exposed by the steward catalog. `{kind: "root", children: [{kind: "provider", slug: "scb"}, {kind: "provider", slug: "sos"}, {kind: "classification-root", slug: "class"}]}`. |
| GET | `/api/catalog/{fqid:path}` | Single endpoint covering every node in the hierarchy. Response shape: `{kind, entity, children?, ...}`. The `kind` discriminates by segment count + `class/` prefix: `provider` (1 seg), `register` (2 seg), `variant` (3 seg), `binding` (4 seg, leaf), `classification-root` (`class`, 1 seg), `classification` (`class/<slug>`, 2 seg, leaf). On `binding` leaves, the response embeds the variable's full longitudinal record (all states with their validity ranges, value sets, aliases, `replaced_by`/`related_to`/`same_as`/`lineage` edges). |
| GET | `/api/catalog/{fqid:path}?period=...` | Same canonical endpoint, with a `period` query string. Accepts the same forms as `Source.period` (int year, period-token, range, snapshot sentinel). On `binding` leaves, the response narrows to a single `variable_state` matching the period (for range periods, returns the matching state set). Returns 404 if no state covers the period; returns 409 (`binding_state_ambiguous`) with candidate `value_set_version_label`s if multiple states match (rare LKF-shape case). The `period` query is otherwise ignored on non-binding kinds. |
| GET | `/api/catalog/{fqid:path}/states` | Full state history for a binding. Returns `{binding, states: [{valid_from, valid_to, data_type, value_set?, delivery_column_name?, value_set_version_label?}, ...]}`. SPA's variable detail UI uses this to render an edition picker / period axis. |
| GET | `/api/catalog/{fqid:path}/predecessors` | Returns `{binding, predecessors: [VariableRef, ...]}` via inbound `variable_replaced_by` edges. Maps 1:1 to `Catalog.predecessors(fqid)`. |
| GET | `/api/catalog/{fqid:path}/successors` | Returns `{binding, successors: [VariableRef, ...]}` via outbound `variable_replaced_by` edges. Maps 1:1 to `Catalog.successors(fqid)`. Used by SPA's "this variable was replaced" remediation flow. |
| GET | `/api/catalog/{fqid:path}/related` | Returns `{related: [{ref: VariableRef, relation_kind: str}, ...]}` via `variable_related_to` edges. Used by SPA's "did you mean this grain level?" picker. |
| GET | `/api/catalog/{fqid:path}/lineage` | First-class v1 endpoint. Returns `{binding, lineage_edges: [{source_state: {state_id, binding, valid_from, valid_to, value_set_id?, value_set_version_label?, delivery_column_name?}, valid_from, valid_to}, ...]}` — edges materialized from `variable_state_lineage` (§5.6). For consumer-side variables (LISA's Kön sourcing from RTB's Kön), enumerates every source-side state that contributed during validity intersection. For canonical-source variables (no inbound lineage), returns empty `lineage_edges`. Embedded `lineage` field on canonical leaf response (a thin reference list); standalone endpoint provides full state context per source. |
| GET | `/api/catalog-search?q={query}&kind={register\|variable}` | FTS across registers and variables (delegates to reg_meta's FTS5 indexes). Separate path so the catalog endpoint stays single-purpose. |

**URL routing notes.** `/api/catalog/{fqid:path}/states` parses
cleanly through FastAPI: the path converter greedy-consumes
slash-bearing FQID before the literal `/states` suffix. Same for
`/predecessors`, `/successors`, `/related`, `/lineage`.

FastAPI / Starlette route matching is **order-sensitive** when a
`{fqid:path}` catch-all is in play. The suffixed routes (`/states`,
`/predecessors`, `/successors`, `/related`, `/lineage`) MUST be
declared *before* the catch-all `/api/catalog/{fqid:path}` in the
FastAPI router — otherwise the catch-all greedy-consumes the suffix
into `fqid` and the suffix handler never fires. CI enforces the
ordering via a router-introspection test
(`assert routes_declared_before(...)`).

**Documentation** (unchanged from v0.11 — `reg-meta-docs` backed)

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/docs/search?q={query}` | FTS over parsed register-documentation markdown. |
| GET | `/api/docs/get/{provider}/{register}` | Register-level documentation. |
| GET | `/api/docs/get/{provider}/{register}/{variable}` | Variable-concept-level documentation. |

**Project** — write endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/project/validate` | Validates a `project_data.json` document; returns structural, namespaced-block, and semantic errors/warnings (§6.8). |
| POST | `/api/project/order` | Renders the steward's order export. **Default template (v1): CSV with columns `provider,register,variant,variable,period,display_name`** — one row per spec binding, `period` from the source's `Source.period` (range periods serialize as `"<from>..<to>"`; snapshot sentinel as literal `"_default"`). Stewards inherit the default unless they ship an `order_template`. |
| POST | `/api/bundle` | Builds the Python MONA bundle embedding the supplied `project_data.json`. Pure function of input. Response cacheable by content-hash. Returns `.py` as `application/octet-stream`. |
| POST | `/api/kit` | Builds a generation kit (zip) from `project_data.json` + `project_data.stats.json`. Dereferences classification FQIDs and binding FQIDs into a sibling `project_data.codes.json` (§6.6). |

Realign-patch application stays client-side only.

**ETag scheme.** Catalog responses carry
`ETag: "<reg_meta_version>-<steward_id>-<sha256(body)[:16]>"`. The
reg_meta version is the v1.x.x Model A release; cache invalidates on
the boundary naturally. The `?period` query is part of the URL and
therefore part of the cache key — different periods are different
cache entries. The `/states`, `/predecessors`, `/successors`,
`/related`, `/lineage` sub-endpoints have their own ETags computed
from their response bodies; no manual invalidation needed.

**Cloudflare edge-cache validation gate (still in effect).** Per
§6.5 of §15, run a small load through Cloudflare to confirm
slash-bearing FQID paths round-trip cleanly before publishing
OpenAPI. Under Model A, FQIDs are one segment shorter, which is
strictly better for edge-cache compatibility (shorter URLs,
fewer slash-encoding edge cases).

The OpenAPI spec is the canonical contract.

### 9.6 Domain types and Pydantic models

Revised under D5: layered Pydantic boundary.

- **`reg_schema`**: Pydantic models. JSON schema codegen for the SPA's
  TypeScript types. FastAPI response models for project_data-related
  endpoints **are** `reg_schema` models directly — no separate 1:1
  Pydantic wrapper layer in `reg_webapp/backend/`. Eliminates the
  drift surface that wrapper layers introduce.
- **`reg_meta_build/ir/`**: Pydantic models. Build-time only. Used by
  adapters to emit IR and by the materializer to validate before
  writing the DB. Not shipped to any consumer.
- **`reg_meta`** (library): plain dataclasses on the library surface.
  Notebook/script import lightness matters. Pydantic adds 100-200ms
  import time + pydantic-core binary; not worth it for the read-side
  query surface. `reg_webapp/backend/` defines Pydantic *response
  wrappers* for reg_meta domain types (one function per endpoint);
  this is the only place a 1:1 wrapper remains.
- **`reg_monabundle.runtime`**: plain dataclasses. Bundle's 1 MB v1
  budget rules out Pydantic-core (multi-MB binary). The bundle
  amalgamator never includes Pydantic. `reg_monabundle.runtime.spec`
  holds a `LoadedSpec` dataclass; bundle-build converts Pydantic
  `Source` → `LoadedSpec` once at amalgamation time, runtime never
  sees Pydantic.
- **`mock_data_wizard`** / **`reg_mockdata`**: dataclasses. CLI tools;
  no Pydantic dep.

The `LoadedSpec` conversion at bundle-build is the explicit
boundary between the Pydantic side (authoring, validation, FastAPI,
SPA codegen) and the dataclass side (bundle runtime, MONA
execution). No amalgamator magic; the boundary is in code, in one
place.

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
  **Hard reject v0.x files.** When the file has
  `schema_version: "1.x.x"` or `reg_meta_version: "reg_meta/v0.x.y"`,
  the SPA surfaces a blocking error: "this project predates Model A
  (v1.0). Please re-author against the current schema." No
  migration code; pre-v1 policy.
- **IndexedDB schema versioning.** The SPA stores its own schema
  version alongside each project; on reload, mismatched schema
  versions get the same hard reject. IndexedDB content otherwise
  carries forward unchanged.
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
  per-binding overrides come from the spec's
  `reg_monabundle.binding_options`. Steward config does not
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
Model A: a binding FQID resolves to exactly one `variable` row,
which has 1..N `variable_state` rows attached. Given a `period`,
`Catalog.resolve_at(fqid, period)` picks a single state
deterministically (or surfaces a structured ambiguity error in
the rare LKF-shape case). Heuristic picking is replaced by
explicit state resolution. The picker module is deleted, not
moved (§15 step 7).

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
automatically: `scb/lisa/individer-15plus/kon`,
`scb/rtb/folkbokforda-personer/kon`, and `sos/par/sluten-vard/kon`
are all the same concept for spine purposes. Cross-rename equivalence (the
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

`reg-meta update` (downloads the latest reg_meta DB and docs DB
assets) is a `reg_meta` concern. After the refactor neither
`reg_monabundle` nor `reg_mockdata` has a reg_meta dep, so
`mdw update` is deleted; users run `reg-meta update` to
keep their local reg_meta current. (Post-step-2 the `maintain`
subgroup has dissolved — `update` and `info` are top-level on
`reg_meta`.)

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
| reg_meta library API | New typed `Catalog.resolve(fqid)` returning a longitudinal variable record, plus `resolve_at(fqid, period)`, `states`, `replaced`, `related`, `lineage` for state-level traversal (§5.10); webapp imports through this surface only |
| `mock_data_wizard` Python package | Split into `reg_monabundle` (MONA bundle build + runtime + scan + types) and `reg_mockdata` (local mock generation + compare). See §4 and §10. |
| `mock_data_wizard/server.py` (local HTTP server) | Deleted after migration to `reg_webapp` |
| `mock_data_wizard/editor.py` (mutator API) | Deleted; `reg_webapp` owns authoring (§15 step 7) |
| `mock_data_wizard/classify.py` (classifier chain) | Deleted at §15 step 7 (alongside `editor` / `server`) — once the webapp owns type selection, the classifier is dead code |
| `mock_data_wizard/enrich.py` (reg_meta lookups at generate) | Codes come from `project_data.codes.json` instead |
| `mock_data_wizard/registers.py`, `cli.py` reg_meta cmds | Deleted (equivalents live in `reg_meta`) |
| `mock_data_wizard/web/` (Svelte UI) | Migrates to `reg_webapp/frontend/`, then deleted |
| `mock_data_discovery.json` | Deleted; replaced by realign patch |
| `mock_data_config.json` | Renamed to `project_data.json`; schema owned by `reg_schema`; `columns` → `bindings`, with each binding's `variable` field carrying a 4-segment binding FQID; source `register`+`year` becomes `register_variant` (3-seg FQID) + explicit `period` (Model A) |
| `mock_data_stats.json` | Renamed to `project_data.stats.json` |
| `mdw` namespaced block in spec | Renamed to `reg_monabundle`; owner is `reg_monabundle.validate_block` (§6.5) |
| `value_set_version` strings (`Kon@2023`, `SUN@2020`) | Replaced by classification FQIDs (`class/sun2020` — 2-seg, version baked into slug) on the binding; ad-hoc codes inlined in `project_data.codes.json` by binding FQID (§6.6) |
| Categorical codes at generate time | After kit-build all codes live inline in `project_data.codes.json` regardless of source — classification FQIDs are dereferenced at kit-build, ad-hoc inline sets pass through; one lookup path post-kit (§8) |
| `reg-meta maintain build-db` subcommand | ✅ Moved to `reg-meta-build build-db` (binary `reg-meta-build`; package `reg_meta_build`). Done in §15 step 2. |
| `reg-meta maintain update` / `info` | ✅ Promoted to top-level `reg-meta update` / `reg-meta info`; `maintain` subgroup dissolved. Done in §15 step 2. |
| `mdw update` | Deleted; users run `reg-meta update` |
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
    `reg-meta update`).
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
  materialized at build time as `variable_state_lineage` interval-
  overlap edges back to the canonical source state). What remains
  is a UI question:
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

## 15. Migration order

Not a checklist. A narrative of what blocks what, intended to inform
sequencing decisions. The order below is the load-bearing dependency
story, not the day-by-day plan.

For the **PR-sized chunking with checkboxes and the v0.x → Model A
rework map**, see [`MIGRATION_PLAN.md`](MIGRATION_PLAN.md) at the
repo root. That file is the live tracker; this section is the
design-level narrative.

### Shipped (v0.x)

1. **reg_meta identifier rebuild (§5 v0.11).** ✅ Shipped 2026-05 across PRs [#78]–[#82], [#85]–[#87], [#89], [#104], [#112]. Tagged `reg_meta v0.11.x`. Implemented period-bearing 5-segment binding FQID + slug-anchored same_as. Now superseded by Model A (§5 v1.0); slugs from this work survive the rewrite (provider/register/variant/variable/classification slugs are forward-compatible; register_version slugs are dropped).

2. **`reg_meta_build` carve-out.** ✅ Shipped 2026-05-19 across PRs [#103], [#105], [#108]. Pure mechanical split.

3. **`reg_schema` v0.x package.** ✅ Shipped 2026-05-19 across PRs [#110], [#111], [#115]. Phase 1 ValidationResult contract; Phase 2 dataclasses; Phase 3 `validate_structural`. **Will be rewritten in stage A3 to Model A's Pydantic shape + Source schema break.**

4. **`mock_data_wizard` adopts `project_data.json`.** ✅ Shipped 2026-05-20 in PR [#116]. Config rename, fixture corpus rewrite. **Will need updates in stage A3** when Source schema breaks.

5.5. **Shared validator test corpus.** ✅ Shipped 2026-05-19 in PR [#113]. **Will need corpus rewrites in stage A3** when Source schema breaks.

### Stage A0 — Finish reg_monabundle carve-out (v0.x grammar) — ✅ shipped

In-flight reg_monabundle step 5 work landed on the v0.x FQID grammar
ahead of Model A's grammar change, avoiding interleaving test failures.
A0 is complete; Model A migration begins on a stable base.

- **A0.1**: Step 5 phase 2b — `mdw/scan.py` → `reg_monabundle/scan.py`. ✅ PR [#122] (commit c0fa0cb).
- **A0.2**: Step 5 phase 2c — runtime modules (`classify`, `sql_emit`, `sources`, `summarize`, `spec`, `extract`) → `reg_monabundle/runtime/`. `LoadedSpec` lives in `reg_monabundle/runtime/spec.py`. ✅ PR [#123] (commit 21543f1).
- **A0.3**: Step 5 phase 3 — 1 MB bundle-size budget gate with 200-column load-test fixture, byte-counted in CI. ✅ PR [#124] (commit cbd4d84) + follow-ups in PR [#125] (commit 9afbc6d).

**Gate cleared:** the 1 MB budget gate verified the amalgamation
strategy works end-to-end before adding Model A's complexity. Stage
A1 (Universal English column rename + IR scaffolding) is now unblocked.

### Stage A1 — Universal renames + IR scaffolding (Model A-prep, A/B-independent)

Lay the foundations for Model A without yet flipping the schema.
Three independent PRs.

- **A1.1 Universal English column rename.** Renames every SCB-Swedish column in the DDL to English (per §5.11 vocabulary glossary). Data values unchanged (provider-native strings preserved). Touches `reg_meta_build/src/reg_meta_build/db.py`, every query in `reg_meta/`, all test fixtures. ~30 query callsites in `reg_meta`; ~50 test fixture files. Heavy but mechanical.
- **A1.2 Lift sensitivity flags.** `unika_summary.{kanslig_variabel, kanslig_variabel_ibland}` both fold into one `variable.is_sensitive` boolean (the 22 sometimes-sensitive edge cases get treated as sensitive — not worth a separate column). `unika_summary.identitetsvariabel` lifted to `variable.is_identifier`. `unika_summary.{version_forsta, version_sista}` are **retained** through A1.2 — A2.1's coalescer consumes them to derive `variable_state.valid_from`/`valid_to` (mapped to ISO 8601). The `unika_summary` table is dropped after A2.1 lands, not after A1.2.
- **A1.3 IR module + adapter scaffolding.** Create `reg_meta_build/ir/` with Pydantic IR dataclasses (`IRRegister`, `IRVariant`, `IRVariable`, `IRVariableState`, `IRValueSet`, `IRValueCode`, `IRClassification`, `IRLineageEdge`, `IRWarning`, `IRDeliveryProvenance`). Create `reg_meta_build/sources/` adapter directory with an unused `IRAdapter` protocol. SCB ingest doesn't move yet — just defines the contract. Provenance DB sibling artifact created as empty placeholder.

**Gate:** A1 can run in parallel; no internal ordering. A1 must complete before A2.

### Stage A2 — Model A schema (load-bearing gate)

The largest stage. Seven PRs.

- **A2.1 `variable_state` table + coalescing.** Add `variable_state` alongside `variable_instance`. Build pipeline writes both in parallel; resolver still uses `variable_instance` (no behavior change yet). The coalescer reads SCB CSV → `variable_instance` rows, groups by `(register, variant, variable, datatyp, datalangd, value_set_id, value_set_version_label, vardemangdsniva)` — `vardemangdsniva` is included in the group key so multi-grain rows stay distinct for A2.2 triage; the final `variable_state` schema doesn't carry grain. Derives `(valid_from, valid_to)` from `unika_summary.version_forsta/sista` (mapped to ISO 8601), writes one `variable_state` row per coalesced group.
- **A2.2 Build-time triage.** Per §5.7. Kolumnnamn-primary discriminator. Splits multi-grain variables; auto-derives sibling slugs; emits `variable_related_to` edges. Catches the ~7,500 same-year collisions in current SCB data; ~200-300 cases need manual TOML curation (slug TOML overrides for ambiguous suffixes).
- **A2.3 Auto-derive `variable_replaced_by`.** Read SCB `timeseries_event` rows with `handelse IN ('Ersatt av', 'Ersätter')`. Materialize into `variable_replaced_by` table (per-variable + parallel register- and variant-level tables). TOML curation for cross-provider edges (empty in A2; populated in A4 for SOS).
- **A2.4 `variable_state_lineage` interval-overlap join.** Per §5.6. Replace `link_consumer_side_bindings` with the new linker. Add `variable_state_lineage` + `variable_state_lineage_warning` tables to DDL. Source-variant pinning is **TOML-only** — `[lineage_defaults]` and `[lineage."<consumer>.<variable>"]` blocks in the source register's slug TOML, no `variable_source_lineage` SQL table. Emit `variable_state_lineage_warning` rows for `no_source_state` / `ambiguous_source_variant` cases. Old `via_source_id` column populated in parallel for transition.
- **A2.5 Catalog API shift.** `Catalog.resolve(fqid)` **flips in place** to the §5.10 longitudinal semantics (returns `ResolvedVariable`). The v0.x per-cvid behavior is **deleted**, not deprecated — pre-v1 policy allows the break, and a grace-period alias would just keep `via_source_id` alive for no real consumer benefit. Add `resolve_at(fqid, period, *, value_set_version=None)`, `states`, `predecessors`, `successors`, `related`, `lineage`, `lineage_warnings` per §5.10. Webapp endpoints in §9.5 still on v0.x grammar (binding leaves still 5-seg); flip in A2.6.
- **A2.6 Drop period from FQID grammar.** The big bang. Update parser, emitter, slug-loading code. Drop ~1,264 `register_version` slug entries from `scb.toml`. **Drop the `register_version` table entirely** — per-edition prose (mätinformation, edition descriptions, time-series break notes) routes to reg-meta-docs at variant level with chronological Markdown sections; per-edition build artifacts (approval dates, doc_status) route to provenance DB. **No `scb_register_edition` extension table.** Webapp catalog endpoints flip to 4-seg binding leaves + new sub-endpoints. v0.x reg_meta clients break; pre-v1 policy says no shim. UNFROZEN sentinel is active so the slug TOML rewrite is a regular commit.
- **A2.7 Cleanup.** Drop `variable_instance` table and the `via_source_id` column (both kept alive through A2.5–A2.6 only so the build pipeline can dual-write while the new tables stabilize). All consumers now on `variable_state`. Bump `reg_meta` to v1.0.0 (with snapshot reset; UNFROZEN still active for the curation polish that follows).

**Gates:**

- A2.1 → A2.2 (triage needs `variable_state` to exist).
- A2.2 → A2.4 (lineage joins on coalesced states, post-triage).
- A2.5 can run parallel to A2.3/A2.4.
- A2.6 must follow A2.5 (FQID grammar flip touches the new API).
- A2.7 must follow A2.6 (cleanup removes legacy code paths).

### Stage A3 — Consumer migration

Four PRs. Lands after A2.

- **A3.1 `reg_schema` v2.0.0** — Pydantic migration + Source schema break. `Source.register_variant` (3-seg) + `Source.period` (always required; polymorphic int/string/range/snapshot-sentinel). `Source.columns` → `Source.bindings`. Binding `name` → `variable` (4-seg). Panel `entity_key`/`time_key` inherit from `variant.panel_template` when omitted. New issue codes (`invalid_period`, `period_outside_state_validity`, `binding_state_drifts_within_period`, `binding_state_ambiguous`, `variable_replaced`). Test corpus rewrite for all 5 cases. Bump pinned `reg_meta_version` in steward catalogs to `reg_meta/v1.0.0`.
- **A3.2 `mock_data_wizard/spec.py`** — adopt new Source shape. `_build_source` rewrite. Fixture sweep for all `mock_data_config.json` → `project_data.json` files that carry the old 4-seg Source FQID.
- **A3.3 `reg_monabundle/validate.py`** — one-line FQID segment count update (5 → 4). Tests follow.
- **A3.4 Bundle amalgamator update.** Bundle-build converts Pydantic `Source` → `LoadedSpec` (the conversion boundary per §9.6). Amalgamator's `_AMALGAMATED_PACKAGE_PREFIXES` tuple ensures `reg_schema` Pydantic models aren't pulled into the bundle.

**Gate:** A3 starts after A2 completes. A3 PRs can land in any order modulo `reg_schema` v2 going first (A3.1 → A3.2/A3.3/A3.4 in parallel).

### Stage A4 — Adapter refactor + SOS

Five PRs. Can run in parallel with A3 after A2.6 lands.

- **A4.1 SCB adapter refactor.** Move SCB ingest from `db.py`'s `_import_*` functions into `reg_meta_build/sources/scb.py` as the `SCBAdapter` implementing the `IRAdapter` protocol. Materializer in `db.py` becomes provider-blind, consumes IR. Test SCB rebuild produces byte-identical output.
- **A4.2 Deterministic IDs + provenance DB.** Switch SCB universal IDs to reuse source IDs verbatim (RegisterId → register_id, etc.). SOS IDs minted via BLAKE2b with top-bit namespace. Provenance DB written by adapters; .prev rotation enabled.
- **A4.3 SOS adapter.** `reg_meta_build/sources/sos.py` `SOSAdapter` consumes the 13 SOS workbooks via the existing parser, emits IR. Handles variant synthesis for LSS/BU/SOL (`_default`). Kodlista state-era parsing per §5.7. MFR IVF_klinik entity-registry heuristic. ~2,300 IR rows.
- **A4.4 SOS slug TOML + panel_template curation.** Create `reg_meta_build/fqid_slugs/sos.toml` with curated register/variant/variable slugs for 13 registers. Initial 3-letter register slugs (`par`, `mfr`, `dors`, etc.). Auto-slug for variables with TOML overrides where needed. **Per-variant `panel_entity_key` / `panel_time_key` / `panel_time_grain` curation** for every variant — seed-slugs suggests defaults from SCB `Tabelldefinitioner.sql` PK declarations and from SOS `is_join_variable` annotations; curator confirms.
- **A4.5 First combined SCB+SOS build.** CI pipeline produces a single `reg_meta.db` containing both providers. Tests verify cross-provider FTS, cross-provider lineage (if any), no ID collisions, no spurious same-name same_as edges.

**Gates:**

- A4.1 → A4.2 (deterministic IDs need adapter pattern to be live).
- A4.2 → A4.3 (SOS adapter mints SOS IDs).
- A4.3 → A4.4 (slug curation against real ingested data).
- A4.5 is the integration gate.

### Stage A5 — Webapp + SPA

Four PRs. Lands after A2 + A3. A4 not required (SCB-only deployment can ship first).

- **A5.1 `reg_webapp` Pydantic models.** Update FastAPI endpoints to use `reg_schema` models directly (no separate wrapper layer). `reg_meta` library types still wrapped 1:1 for catalog responses (Pydantic boundary).
- **A5.2 New API endpoints.** Implement `?period=...` query (polymorphic per §6.2, not year-only) and `/states`, `/predecessors`, `/successors`, `/related`, `/lineage` sub-endpoints per §9.5 — `/lineage` is a first-class v1 endpoint, not deferred. Suffixed routes must be declared before the `/api/catalog/{fqid:path}` catch-all in the FastAPI router (router ordering test enforces this). Cloudflare edge-cache validation gate.
- **A5.3 SPA TypeScript regen.** OpenAPI codegen against new Pydantic models. SPA components updated for 4-seg FQIDs, new sub-endpoints, ambiguity (409) handling.
- **A5.4 SPA IndexedDB hard-reject for v0.x project files.** Per §9.7. Blocking error with clear message on load.

**Gate:** Webapp scaffold (REFACTOR_SPEC v0.x's §15 step 6) already includes this; A5 is the Model A overlay. Can ship `global` deployment immediately after A5.4.

### Renumbered later steps (post-A5)

The original §15 steps 6 onward carry forward to Model A unchanged
in structure, with §10's bundle work now operating against Model A's
`variable_state` shape.

6. **Webapp scaffolds: backend + frontend skeleton.** Empty UI,
   OpenAPI plumbing, the `global` steward dir wired up, reads
   reg_meta through the FQID-keyed catalog API. `reg_webapp`
   imports `reg_meta`, `reg_schema`, `reg_monabundle`. Steward
   configuration shape (`steward.toml` + reused `project_data.json`
   for the catalog; §9.1) is finalised here so step 11's catalog
   authoring is unblocked. **Under Model A**: webapp consumes the
   simplified 4-segment binding FQID + new sub-endpoints
   (`/states`, `/predecessors`, `/successors`, `/related`,
   `/lineage`; §9.5).

**Step 6.5 — Containerize, Cloudflare, `global` deployment up.**

- `reg_webapp` Dockerfile runs `reg-meta update` at image build
  time to bake the matching reg_meta release's DB into the image
  layer (§4).
- Cloudflare configured in front: edge caching with the §9.4 ETag
  scheme, per-IP rate limits.
- **Edge-cache validation gate:** run a small load through
  Cloudflare to confirm slash-bearing FQID paths round-trip
  cleanly through the edge cache before publishing the OpenAPI.
  Under Model A the binding FQID is 4-seg (one shorter than v0.11)
  which is strictly better for edge-cache compatibility. If the
  edge cache mangles them, fall back to a query-string form
  *before* the OpenAPI is committed — `/api/catalog/{fqid:path}`
  is the chosen form (§9.5), and changing it post-publish is
  expensive.
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
   by classification FQID; ad-hoc categorical bindings get inline
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
      updated for the new realign output and stats schemas** —
      without this the PII scanner runs against schemas it doesn't
      understand. Under Model A, `is_compatible` operates against
      `variable_state` shape (data_type + data_length + value_set
      drawn from the relevant state).
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
