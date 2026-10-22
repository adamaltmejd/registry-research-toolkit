# Architecture

Cross-cutting design for the Registry Research Toolkit — the topology, dependency graph,
and repo-wide invariants that no single package owns. Package-local rationale lives in
each `<package>/DESIGN.md`; remaining (not-yet-built) work lives in GitHub issues.

## Domain

Swedish register research uses administrative microdata produced by Statistics Sweden
(SCB) and other agencies (Socialstyrelsen, Försäkringskassan, …). Each agency publishes
**registers** — population-scale administrative datasets named LISA (labour market), RTB
(population), PAR (patient registry), FRIDA (firms), etc. A register holds **variables**
(columns), each with a stable identifier, definition, data type, and — for categorical
variables — an enumerated **value set** (`Kön ∈ {1=Man, 2=Kvinna}`). Value sets are
versioned (SUN2000 vs SUN2020) and registers themselves drift edition to edition.

A project requests a **variable list** (register × variable × period) plus a population
definition, and — after ethical approval and SCB processing — receives data inside MONA.
Person identifiers are pseudonymized as project-specific `LopNr` running numbers, shared
across registers so records link.

### The MONA constraint

Data delivery is not a download. SCB hosts the data inside **MONA** (Microdata Online
Access), a remote-desktop environment. Three consequences shape the whole toolkit:

- **PII may not leave MONA.** Only aggregate, disclosure-controlled outputs are
  exported. This is the contractual basis for data access, not a nice-to-have.
- **No internet on MONA.** Code that runs there is self-contained: dependencies are
  pre-installed (WinPython, incl. duckdb/pyodbc/numpy) or uploaded with the code.
- **LLM agents are not allowed inside MONA.** Researchers develop analysis code with
  coding agents *outside* MONA. The tooling for that (the MONA bundle and local mock
  data) is archived pending a from-scratch rebuild (#707).

### Data stewards

The central multi-tenancy axis. Some research organizations re-license registry data
from their own warehouses instead of each project going through SCB directly:

- **global** — the full multi-agency catalog, served over MCP only (#1709). Its `order`
  answers in `global_fallback` mode (canonical columns, no steward table) to describe
  data a researcher already holds; no channel receives it.
- **ifau** — the subset in IFAU's warehouse.
- **swecov** — the subset in the SWECOV research program.

One codebase, steward-scoped views: same UX, different holdings. A steward's order is an
extract from its own holdings: the researcher sends `order.json` through the steward's
request channel and a data manager runs the MONA extract runner (`reg_mona_runner/`)
inside MONA. The compiled-holdings contract below gives each steward one
self-identifying SQLite artifact containing full reference metadata plus its physical
holdings. TOML inventory and policy are accepted builder inputs; readers do not load
them. SWECOV proves the first cut; IFAU delivery authoring remains outside it. See
[`reg_meta_build/DESIGN.md`](reg_meta_build/DESIGN.md) § "Steward extension" and
[`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md) for the build and runtime boundaries.

## What the toolkit is

A web application (Rust server + React SPA), designed for three steward-scoped flavours
off one image, that lets researchers browse a catalog, author a per-project variable
list, and export it as a data order. The HTTP API (which the SPA uses) and the MCP tools
are equal surfaces over the same `reg-catalog` operations. The SPA validates the draft
automatically on every edit.

The unifying research-intent artifact is **`project_data.json`** — written by the webapp
and consumed by the shared materializer above, whose manifest the MONA extract runner
reads. Its schema and structural validator are `reg-core`'s
([`crates/reg-core/DESIGN.md`](crates/reg-core/DESIGN.md)). It deliberately does not
encode physical filenames or SQL tables. Compiled physical holdings
(`table + edition → literal columns → zero-or-more explicit logical mappings`) join a
project and query-time catalog resolution to produce one normalized, versioned JSON
order manifest for web and MCP, with no per-steward export template. The materializer
reads the selected compiled artifact for `global`/`swecov`; inventory remains a builder
input.

Coverage is browse/search the catalog → choose variables and periods → automatic
validation → the versioned JSON order manifest. Population definition (a predicate over
base registers, executed only inside MONA) is deliberately out of scope.

## Package layout

Monorepo: one Python build package, the Rust runtime crates and one webapp. CLI binaries
are `reg-meta` (the runtime) and `reg-meta-build` (the builder); no short aliases for
v1.

```text
registry-research-toolkit/
  reg_meta_build/   # catalog DB builder, Python (binary: reg-meta-build)
  crates/
    reg-core/       # project_data.json contract + structural validator, grammars
    reg-catalog/    # catalog reader and its operations
    reg-meta/       # binary: `serve` (HTTP API + /mcp) and `mcp` (stdio)
    reg-core-py/    # PyO3 bindings of reg-core for the builder
    reg-core-wasm/  # WebAssembly bindings of reg-core for the SPA
  reg_mona_runner/  # MONA extract runner: order.json -> CSVs inside MONA (R)
  conformance/      # contract corpus, run against the Rust server
  plugins/          # the microdata-tools-se agent plugin (hosted MCP)
  reg_webapp/
    frontend/       # React 19 + React Aria Components + Vite (bun)
    edge/           # edge workers: global is MCP-only, swecov also serves the SPA
    stewards/
      global/       # steward.json only (full universe)
      swecov/       # steward.json (compiled holdings artifact)
```

Dependency graph (acyclic):

```text
reg-meta       → reg-catalog → reg-core
reg-core-py    → reg-core
reg-core-wasm  → reg-core
reg_meta_build → reg-core-py
reg_webapp     → reg-meta (HTTP, through the codegen'd OpenAPI types)
reg_webapp     → reg-core-wasm (built into the SPA by `bun run gen:wasm`)
reg_mona_runner → the order manifest (a file, offline inside MONA; no code dependency)
```

Nothing is published to PyPI. The runtime releases on `reg_meta/v*` tags: the `reg-meta`
crate version, the DB assets and the `reg-meta` binaries for macOS arm64 and Linux
x86_64. `reg_meta_build` is tagged (`reg_meta_build/v*`) but not published: it depends
on the workspace-only `reg-core-py` and runs from a maintainer checkout. The webapp
ships as a container image built from the newest `reg_meta/v*` release's assets.

### Why this split

- **Runtime vs builder** — the Rust runtime reads; the Python builder writes. They have
  different deps, cadence and operators, and share only the artifact contract and
  `reg-core` (through `reg-core-py`). The built SQLite DBs (`reg_meta.db` plus the
  smaller `reg_meta_docs.db`) are distributed as `.zst`-compressed **GitHub release
  artifacts** on `reg_meta/v*` tags, fetched with `curl`, SHA-256 verification and
  `zstd`. See [`crates/DESIGN.md`](crates/DESIGN.md) for the runtime and
  [`crates/reg-core/DESIGN.md`](crates/reg-core/DESIGN.md) for the project contract.
- **Compile, don't resolve.** The artifact is immutable and rebuilt on every release, so
  anything that is a pure function of it is computed once by the build's derive step
  instead of on every query. The resolver has one home, in the build; the reader keeps
  only request-dependent rules. Measured query cost on the old reader was query design,
  not language, so this is what makes the reader fast and small
  ([`reg_meta_build/DESIGN.md`](reg_meta_build/DESIGN.md), "Base and derive").
- **One implementation per contract.** Each contract that crosses a language or package
  boundary (grammars, folds, the project validator, result types) has one home, reached
  from the build through `reg-core-py` and from the SPA through `reg-core-wasm`. The
  table is in [`crates/DESIGN.md`](crates/DESIGN.md), "One implementation per contract".
- **The runtime is Rust; the build stays Python.** The server, MCP tools and SPA share
  one validator, one grammar and one set of result types, and local MCP is one binary.
  The build is the most frequently changed code and stays Python until its curation
  rules stabilize ([`reg_meta_build/DESIGN.md`](reg_meta_build/DESIGN.md), "Build
  performance").
- **Agents and the SPA are the users.** One operation set, exposed over HTTP and MCP
  (hosted at reg-meta.vqv.uk and data.swecov.se, and local `reg-meta mcp`); there is no
  query CLI and no human-oriented output. The interface rules are in
  [`crates/DESIGN.md`](crates/DESIGN.md), "Agent and web interface".

### Accepted source revisions and catalog generations

`reg_meta_build` follows a four-step contract: provider adapters condense raw deliveries
into one common compact form; provider-generic rules process it on every build; tracked
curation decides what the rules leave unresolved; and the build writes the SQLite
catalog, erroring on anything unresolved until curation resolves or acknowledges it.
Machine-readable inputs are selected by the exact commit and manifest of a host-local
input repository; curation is tracked TOML in this repository. Builds reuse validated
preparation; they never call LLMs or extract PDFs. The catalog embeds its
prepared-source pins, and every build records a digest of the curation it used. See
`reg_meta_build/DESIGN.md` for the contracts, the update workflow and what is still
transitional.

The runtime and `reg_webapp` consume only the activated SQLite generation; they never
read the input repository or recompute authority. Diagnostic builds write a separate
nonpublishable database with unresolved discrepancies and withheld output. They cannot
replace the active catalog. When correction detail is exposed, it remains collapsed into
the existing catalog metadata rather than creating a second public evidence API.

The accepted input branch and active catalog are related but distinct local states. A
candidate is reconciled, built, validated, and compared before its input commit is
accepted. Catalog publication then atomically activates the artifact and its embedded
pins. A failed build leaves both states unchanged; a publication failure after input
acceptance may leave the input branch ahead while the active catalog retains its prior
embedded pins. Runtime readers must never combine those two generations implicitly.

The document index and steward inventories retain separate authority. Indexing or
preserving a handbook/workbook does not make it a catalog source, and a steward's
holdings establish possession only for that steward. The LISA supplemental reader enters
through `reg_meta_build`'s machine-readable source-record boundary; PDF extraction stays
upstream. None of these inputs cross the MONA boundary, and no source reconciliation
changes the rule that PII stays in MONA and only aggregate, disclosure-controlled
results leave it.

Shared literal source evidence types live in `reg_meta_build` (`source_evidence.py`);
the Rust reader reads the documentary relationships from the artifact. Documentary
owner/operand links are metadata, not state availability, equivalence, lineage or
executable transformation edges.

### Real-seed output cache

Real-seed `prepare-sources` and `build-db` runs take one to two hours each, so
`scripts/real_seed_cache.py` reuses their outputs by input key. It is tooling over the
builder's existing reports and checks, not builder code. Each key covers only what its
step reads, so a resolution change reuses the preparation; the module docstring lists
the fields. The invariant is that a key leaving out a real input would return stale
output silently, so every key errs generous, a hit re-checks what it returns, only
completed runs are stored, and `build --verify` reruns uncached and compares bytes. The
digest, staging and eviction helpers it shares with the G1 derive cache and the
synthetic fixture cache live in `scripts/keyed_cache.py`. The builder persists its
resolve phase, so the cache reuses a resolved bundle and reruns only materialization.

### Catalog artifact identity and read scope

**Compiled-holdings contract.** A publishable `reg_meta.db` identifies itself through
`import_manifest.catalog_artifact_kind`: `catalog` for a global reference artifact,
`steward` for reference metadata plus that steward's compiled holdings. Both use the
same schema. A steward installation needs no separate global DB to inspect unheld
metadata. The optional sibling `reg_meta_docs.db` remains separately paired; full
document content is outside the single-file guarantee. Diagnostic/incomplete artifacts
remain nonpublishable and fail runtime admission.

Identity stays in `import_manifest`, including `generation_id`; a steward also records
`steward`, `base_db_sha256`, `base_generation_id`, `holdings_input_commit`,
`holdings_manifest_sha256` and `steward_spec_sha256` (the committed
`reg_webapp/stewards/<id>/` spec the holdings were decided by). Generation identity
hashes canonical semantic inputs and the schema version, not output bytes, timestamps or
host paths. Builder revision, accepted public pins and curation digest participate;
steward identity additionally includes its base generation, accepted holdings pins, spec
and accounting digest. The exact base file digest is byte-level provenance only and does
not enter generation identity. Publish the final file SHA-256 separately. The builder
design specifies the digest encoding. Search cursors and `OrderProvenance` use
`generation_id` instead of `import_date`; HTTP read identities include generation and
scope. Import time may remain display data.

Read scope defaults to `holdings` on a steward artifact and `reference` on a catalog
artifact; explicit `holdings` on a catalog artifact errors. The holdings predicate
applies to provider, register and variable binding/state nodes, using the same compiled
mapping membership before counts, groups or pagination. Classifications, value sets,
documents and lineage stay reference metadata. Query-time resolution intersects physical
periods with applicable states; no resolved segments are compiled. Unknown-scope
holdings remain physical evidence, cannot admit logical nodes and cannot be ordered. The
public-surface rules and exceptions live in `crates/DESIGN.md` → "Read scope and
artifact admission".

The reader and webapp open the selected artifact read-only and validate its identity.
`REG_WEBAPP_STEWARD` must equal its manifest steward; unset/global selects the `catalog`
kind, not an implicit fallback for a named steward. Orders use `order`
(`crates/reg-catalog/src/ops/order.rs`) and the artifact's kind, independent of browse
scope. Accepted inventory/policy/raw-census accounting is a build gate with counts and
digests in the manifest; exclusions and lookups are not runtime holdings. Failures leave
prior artifacts and accepted inputs unchanged.

## Export gate

Not yet built; tracked in #1803. This section records the settled design.

The export gate is the toolkit's disclosure checker and transfer. It is the only path by
which results computed on microdata reach an agent. It fills the checker slot of the
register-research scaffold (to move into this repo), which treats the disclosure checker
as an external executable it consumes. The domain invariant holds throughout: PII never
leaves the data side, only aggregate statistics do, and disclosure control is a
load-bearing guard.

### Components

- **Gate binary.** One static Rust binary, a new crate in `crates/`, built and released
  next to `reg-meta`. It runs the whole disclosure check, stages the checked bytes for
  human review and delivers approved bytes to the agent. An agent calls it; it never
  reimplements it.
- **Schemas.** Fixed, versioned files supplied by the toolkit. Choosing or adding a
  schema is a deliberate user configuration step, kept outside the project repository.
  An agent never writes or edits a schema.
- **MONA check file.** One fixed, version-pinned R file of check functions, for MONA,
  where only `.R` and `.py` uploads are accepted (see "MONA mode").

### Trust boundaries

Three parties, which may share one host:

- **Agent VM.** Code, mock data and approved aggregates only. It never sees unreviewed
  output, check reports, logs or the review UI.
- **Data VM.** Microdata, the pipeline and the gate binary.
- **Human.** Reviews the checked output in the review UI and presses Export.

The review UI is unreachable from the agent VM, by `curl` or any other client. That is
enforced by the VM network setup, not by instructions to the agent. After Export,
approved bytes reach the agent VM automatically. The gate records each file's SHA-256 at
check time and verifies it on delivery. A changed byte voids the approval and needs a
new check.

The prior art is the education-returns `server.sh` (adamaltmejd/education-returns#18).
It stops three times: before code reaches the data server, before checked output is
transferred, and before results are published to git. It stages output with SHA-256
checksums. The gate takes over the second stop.

### Schemas

A schema declares exactly the files an export may hold. Per file: its name, its columns
and each column's type. Disclosure rules, such as which columns count persons and the
smallest count allowed, live in the schema or in a separate policy (open question 10).
The toolkit asserts no universal threshold. The schema in force is identified by name,
version and content hash, and every check records them.

Schemas live outside the project because the code under check must not also define what
the check accepts. In education-returns each script brought its own schema
(`schemas/<script>.R`) and its checks were project R (`src/export.R`, `EXPORT_COLUMNS`).
An agent writing the analysis could widen the gate in the same commit.

### The check

The check runs as the pipeline's last step, over the complete candidate output in a
staging directory. A partial run is never checked into a releasable one. In order:

1. **Inventory.** The staging directory holds exactly the schema's files: none missing,
   none extra, no links or hidden files.
2. **Structure.** Each file's columns and types match the schema.
3. **Disclosure.** Each file passes its schema's rules (person counts, minimum cell
   counts, suppression).
4. **Backstop scan.** String values are scanned for identifier patterns such as
   personnummer. This is a backstop only: a pattern scan cannot establish safety (#20),
   and a misclassified column can pass a count threshold (#17).

Only when every file passes do the files move, atomically, to the output folder. On any
refusal the output folder stays empty. Refusals have stable codes by category: setup
(schema missing, unreadable or of the wrong version), inventory, structure, disclosure,
backstop, and delivery integrity (a hash mismatch). A report names the file, column and
rule, never a value, as in the MONA extract runner
([`reg_mona_runner/DESIGN.md`](reg_mona_runner/DESIGN.md), "No row values in logs").
Reports, logs and diagnostics stay on the data side and never enter the output folder.

### Output format

One Parquet file per schema file, readable from Rust, R and Python and queryable with
DuckDB. Column types come from the schema. A suppressed cell is null, never zero.

### MONA mode

No standalone MONA system and no web UI. The toolkit ships one fixed, version-pinned R
file of check functions; the scaffold copies it into the project, and the pipeline's
last step calls it with the chosen schema. The output-folder model is the same: the
folder holds exactly the schema's files, which appear only if the whole check passes.
Logs, error messages and diagnostics never go in output files. The user reviews the
folder, zips it and sends it out through MONA's export.

The Rust binary and the R file are two implementations of one check, an exception to
"one implementation per contract". One case corpus (schema and candidate folder →
expected pass or refusal code) runs against both, so their agreement is the oracle. Each
R file version is pinned to a binary release.

### Out of scope

- Level-1 mock data (metadata-only, generated from a draft `project_data.json`): epic
  #707.
- Moving the scaffold into this repo.
- The disclosure policy values of any institution.
- Changes to the MONA extract runner.

### Open questions

To be resolved before this design merges:

1. Schema file format. It must be readable from Rust and from R on MONA without a new R
   package (the runner already uses jsonlite).
2. The crate and binary name.
3. Writing Parquet on MONA: the `arrow` or `duckdb` R package, and whether either is
   installed there. Is DuckDB only a reader, or are `.duckdb` files also outputs?
4. How approved bytes reach the agent VM (a shared volume, or a push), and whether the
   binary also carries code into the data VM (the first stop).
5. A check receipt (gate version, schema id, file hashes): an output file, or a sidecar
   outside the output folder? The scaffold wants the receipt exported; diagnostics must
   stay out of output files.
6. Where a project's schema choice is configured, outside the project repo.
7. What the agent learns from a failed check: nothing, or the refusal category code.
8. Whether disclosure rules span releases (differencing against earlier approved
   exports), or each export is checked alone.
9. Whether the review UI is part of the gate crate or a separate surface.
10. Whether disclosure thresholds live in the schema or in a separate policy file. The
    scaffold names a configured project policy beside the schema; with fixed schemas,
    thresholds in the schema mean a new schema for each threshold.

## Repo-wide invariants

These are hygiene that keeps options open, enforced in CI where noted. Package-local
mechanisms are documented in the owning DESIGN.md and only summarized here.

- **Build / runtime cleanly separated.** The Rust runtime (`crates/`) only reads the
  artifact; `reg_meta_build` is operator-side, stays Python and is the only writer.
- **Latest tooling.** Newest stable versions of languages, crates, packages and SDKs,
  and modern methods; no compatibility work for older toolchains. Windows is served by
  hosted MCP unless a local binary builds without special effort.
- **Stateless server.** No process-local caches that change behavior across requests;
  the catalog DB is opened read-only.
- **OpenAPI is the canonical contract.** `crates/reg-meta/openapi.json` is committed and
  regenerated by `scripts/gate.py regen`. CI guards drift with a **snapshot test**
  (`cargo test -p reg-meta --test openapi_snapshot`) that asserts the committed file
  equals the server's rendered document, and the frontend CI job runs
  `bun run gen:types` + `git diff --exit-code` so the codegen'd TS types stay in sync.
  (There is no `make` target — drift is a failing test, not a Makefile step.)
- **Performance budget (v1 targets, not yet CI-enforced).** Starting points: the catalog
  reads the Rust server answers (`/api/catalog/*` and the facets
  `/api/{states,warnings,values,graph,lineage}/*`) p95 ≤ 200 ms (cache miss);
  `/api/project/validate` and `/api/project/order` p95 ≤ 1 s; representative broad
  all-scope `/api/search` cache-miss p95 ≤ 500 ms and browser-cold search LCP < 2.5 s.
  Search correctness and latency are origin properties: edge hits do not substitute for
  cold-origin evidence. Classification/value-set initial responses must be bounded by
  bucket/page limits rather than total code cardinality, and cold/repeat rendered routes
  target CLS < 0.1. The 200-column load-test fixture is committed
  (`crates/reg-core/tests/project/corpus/load_test_200col/`), but the load-test harness
  and CI perf gate are remaining work (#1455).
- **Project schema version.** Schema breakage is signalled by `project_data.json`'s
  `schema_version`. `reg-core` accepts exactly its `SCHEMA_VERSION` and refuses any
  other with an `unsupported_schema_version` issue rather than migrating it
  ([`crates/reg-core/DESIGN.md`](crates/reg-core/DESIGN.md)). Pre-v1 there are no
  migration scripts, deprecation wrappers or compatibility shims: breaking changes are
  clean breaks and testers re-author affected projects (`AGENTS.md` → "Maturity and
  compatibility").

## API style

The webapp API is **REST**, not GraphQL or tRPC: edge-cacheability is the primary cost
lever (the Rust server's catalog reads, `/api/catalog/*` and its facets, carry
`Cache-Control` and ETags so Cloudflare absorbs repeat traffic), and a stable resource
grammar is the thing a future port must reproduce. See
[`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md).

## Testing strategy

The binding rules are in `AGENTS.md` → "Testing policy". This section names the
boundaries tests assert at, the oracle each one uses, and the three cost tiers tests run
in.

### Boundaries and oracles

This is the complete list of boundaries a test may assert at.

  | Boundary                                 | Oracle (data, not code)                                                                                            |
  | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
  | Prepared inputs + curation → artifact    | synthetic source fixtures → `validate_built_db` + content snapshot                                                 |
  | Builder CLI → JSON                       | request → expected JSON cases in `reg_meta_build/tests/cases/cli/`                                                 |
  | Artifact → HTTP and MCP                  | `openapi.json` snapshot + request → response goldens run against the Rust server over the fixture DB               |
  | `project_data.json` → validation result  | `crates/reg-core/tests/project/corpus/` run by the Rust and TS consumers                                           |
  | Project + artifact → order manifest      | byte-identical `order.json` goldens, `order` data identical to its download                                        |
  | Order manifest → MONA extract            | order goldens + fixture SQL rows → expected CSVs and `run_log.csv`; a refusal's exit code and `error_log.csv` code |
  | Curation TOML → load or located failure  | committed TOML must load; malformed cases name the locator                                                         |
  | Curation + steward storage → findings    | `derived/storage_type_findings.tsv`: fixture → `expected.tsv` in `reg_meta_build/tests/cases/steward_storage/`     |
  | FQID / period grammars, interval algebra | Hypothesis properties + round-trip snapshots                                                                       |
  | Real artifact ↔ accepted inputs          | conformance artifact checks and explicit accepted-input table/cell census, spec and authored mappings              |
  | Frontend                                 | rendered DOM, the accessibility tree and the codegen'd API types                                                   |
  | Repository tooling (`scripts/tests`)     | the tool's own contract: skill discovery, a lint's verdict                                                         |

Compiled holdings admission, scoped browse/search and selection guards are pinned
through source-built request/expected corpora in `conformance/cases/`.

### Tiers

1. **Package suite (budgeted, every change).** Contract tests over synthetic fixtures
   built from readable source, property tests, golden corpora: the default `pytest`,
   `cargo test --workspace` and `bun run test`. Run narrowed by package while iterating.
   Each package stays inside its budget below.
2. **Push / CI.** `ci.yml` runs the Python suites as a `test` matrix, one leg per root
   `testpaths` entry, each with `timeout-minutes` at its CI budget below. Conformance is
   not one: it requires the Rust server (`--server-cmd`). The `rust` job's 6-minute
   timeout covers `crates/` and the gate's `rust` and `release` steps
   (`scripts/gate.py`: the workspace build, the whole conformance suite against the Rust
   server, then only the `release`-marked admission checks on the synthetic steward
   artifact), with room for a cold rust-cache and its save. The `lint` job runs
   `gate.py lint` (ruff, panache, ty). The `reg-webapp-frontend` job has a 6-minute
   timeout and includes the codegen drift check; the OpenAPI snapshot is a
   `crates/reg-meta` test. A job that exceeds its budget fails. The browser drivers
   (`dev.sh smoke` on agent-browser, the gate's `flows` step on Playwright) are local
   checks, not CI jobs.
3. **Artifact (maintainer or release gate).** Run
   `pytest conformance -m release --run-release --artifact-dir=/path/to/catalog --server-cmd=...`
   after `cargo build --workspace` (the search traversal runs against the Rust server;
   see `conformance/README.md` for the template). The reader admits the selected
   artifact before execution; incompatible or non-publishable artifacts fail, never
   silently skip. The artifact checks compare manifest accounting, sampled
   browse/search/validate agreement, repeated search/order bytes, order/download
   identity and located refusal. CI runs them against both published global catalog and
   SWECOV steward assets in independent `integration.yml` jobs. Published schema drift
   is a release-drift failure. Accepted-private-input census additionally requires
   explicit `--holdings-input=/path/to/accepted-candidate`; mismatched commit/manifest,
   dirty input, missing members and wrong paths fail admission. Full HTTP variable-node
   admission comparison and deterministic stratified artifact/HTTP binding agreement
   derive requests from the artifact. Pinned historical baseline acceptance, exhaustive
   representative resolution, performance and rendered checks remain separate maintainer
   work.

Budgets are what a lean suite for the package needs, derived from its boundaries, not
from the suite's current size. Local is wall time with `pytest <tree> -n auto -q` (or
`bun run test`, or the pytest line of `gate.py rust`) on a 10-core developer Mac; CI is
the job's `timeout-minutes` in `ci.yml` on `ubuntu-latest`, including setup. A suite
over budget is a finding for the `test-audit` skill, not a reason to raise the number.

  | Suite                                | Local | CI job | Rationale                                                                                                                            |
  | ------------------------------------ | ----- | ------ | ------------------------------------------------------------------------------------------------------------------------------------ |
  | `reg_meta_build/tests`               | 45 s  | 6 min  | The compiler: a few hundred source-fixture → artifact cases at about 1 s CPU each, plus one session load of the committed curation.  |
  | `scripts/tests`                      | 20 s  | 2 min  | Repository tooling contracts (skill discovery, lints); subprocess runs of the scripts dominate, `test_real_seed_cache` most.         |
  | `crates/` (`cargo test --workspace`) | 10 s  | 6 min  | The Rust runtime's unit and property tests; G0 always runs them.                                                                     |
  | conformance (`scripts/gate.py rust`) | 30 s  | 6 min  | Session-built artifacts, then data-driven HTTP, MCP, order and validate cases against the Rust server; shares the `rust` job.        |
  | frontend (`bun run test`)            | 15 s  | 6 min  | Rendered DOM and accessibility tree for the user flows, jsdom for grammars. The CI job also installs, type-checks, lints and builds. |
  | `reg_mona_runner` (`gate.py runner`) | 10 s  | 6 min  | One `Rscript` run per refusal locally; CI adds one SQL Server run of every extract case, after installing R and the ODBC driver.     |

Three verification gates cut across the tiers. They are named G0–G2 so they are not
confused with the tiers; `conformance/README.md` ("Verification gates") has the G1
procedure and the pin.

  | Gate | What runs                                                                                                                                  | Budget                           | When                                |
  | ---- | ------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------- | ----------------------------------- |
  | G0   | `scripts/gate.py g0` (lint, `cargo test --workspace` and the touched packages) and `scripts/gate.py rust` (conformance on the Rust server) | under 60 s, plus 30 s for `rust` | every change                        |
  | G1   | `scripts/gate.py g1`: derive on the pinned real artifacts, then the pinned release's server against the checkout's, as raw bytes           | under 5 min warm                 | reader and derive changes, releases |
  | G2   | a full base build, derive, and G1 on the result                                                                                            | about 1 h                        | maintainer, at releases             |

`crates/` takes 10 s of G0's 60 s, so any change touching the runtime fits it;
conformance runs in `rust`, with its own 30 s row. G1 and G2 run on real artifacts in
tier 3 and are not package budgets. A slow gate is a defect to fix, not a reason to skip
it.

### Reader and builder tracks

The reader (`crates/`) and the builder (`reg_meta_build`) ship on different cadences.
These rules keep them coupled only through released artifacts.

1. **Reader work develops against the pinned release.** G0 and the `rust` step run on
   synthetic fixtures; G1 runs against the latest release asset. Reader work needs no
   real-seed build. A reader-only change deploys on push to main: `container-build.yml`
   bakes the newest release's assets.
2. **Derived tables and schema.** A derived-table change is proved through G1, which
   derives on the pinned asset, with no build. It ships only with the next release,
   because deploys bake released databases without deriving. Core-DDL changes the reader
   needs are batched into the next builder content release. The reader gate (`SCHEMA` in
   `crates/reg-catalog/src/lib.rs`) never runs ahead of a released asset outside that
   batch.
3. **`reg-core` edits** invalidate the builder's prepare, build and resolve keys and the
   G1 derive cache. Batch them just before a planned real-seed build. A prepare-key miss
   never forces a re-prepare: an accepted prepared root can be reused.
4. **Releases follow builder batches**, derive-only changes included, not reader
   changes. Each release runs G2 (strict build, derive, then G1 on the result) before
   publishing, and re-pins G1 after.

### Conformance suite

Every tier runs `conformance/` at the repository root, always against the Rust server
(`--server-cmd` is required). Thin loaders run its readable cases against it: HTTP and
MCP responses, order manifests, server startup and project validation. Cases assert
domain outputs or located errors, not object internals or query implementation. Product
adapters are not added solely for testing. Fixture-bound cases always use their named
source-built synthetic artifacts, including in a tier 3 run. Artifact-parametrized
checks default to session-built catalog/steward artifacts and use exactly
`--artifact-dir` under tier 3. They derive expectations from artifact content or
artifact/HTTP agreement, never real-identifier goldens.

Order comparisons across routes and repeated runs use raw bytes, including provenance;
no volatile fields are removed, and key/list order remains observable. Existing scope
accounting covers tables and columns; no manifest count contract yet exists for
mappings, periods or unmapped reasons. Catalog artifacts permit global fallback orders;
steward reference visibility does not grant orderability. The existing
`crates/reg-core/tests/project/corpus/` serves its Rust and TS consumers.

Private imports, internal patches and oversized test modules are gated by repo lints
with no exemptions or file allowlists. The Rust reader runs against these oracles.

Coverage is measured as boundaries with a corpus, not as line percentage; a boundary
without a corpus is the gap.
