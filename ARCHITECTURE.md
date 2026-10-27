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

Some research organizations re-license registry data from their own warehouses instead
of each project going through SCB directly. SWECOV's hosted catalog stays on the last
old-schema release until milestone E ([REBUILD.md](REBUILD.md) §C). The global server,
served over MCP only (#1709), has no holdings: its `order` returns the logical order of
a project, and nothing receives it.

## What the toolkit is

A Rust server and MCP tools that let researchers browse a catalog, validate a
per-project variable list, and export it as a logical data order. The physical manifest
and the runner's input return with milestone E. HTTP and MCP are equal surfaces over the
same `reg-catalog` operations. The SPA is archived on `archive/reg-webapp-spa` and
returns in milestone E.

The unifying research-intent artifact is **`project_data.json`** — authored by
researchers or agents and checked against the catalog by the order check. Its schema and
structural validator are `reg-core`'s
([`crates/reg-core/DESIGN.md`](crates/reg-core/DESIGN.md)). It deliberately does not
encode physical filenames or SQL tables: the order check turns a project and the catalog
into the logical order (per source, the table, period and spelling), which names no
files.

Coverage is browse/search the catalog → choose variables and periods → automatic
validation → the logical order. Population definition (a predicate over base registers,
executed only inside MONA) is deliberately out of scope.

## Package layout

Monorepo: the Rust builder and runtime crates, server deployment, and the legacy Python
builder retained until milestone F. CLI binaries are `reg-meta` (the runtime),
`reg-build` (the catalog builder) and `reg-meta-build` (the legacy builder).

```text
registry-research-toolkit/
  reg_meta_build/   # legacy Python builder, retained until F (reg-meta-build)
  crates/
    reg-core/       # project_data.json contract + structural validator, grammars
    reg-catalog/    # catalog reader and its operations
    reg-meta/       # binary: `serve` (HTTP API + /mcp) and `mcp` (stdio)
    reg-core-py/    # PyO3 bindings of reg-core for the builder
    reg-frame/      # the new builder (REBUILD.md): source frame records
    reg-readers/    #   one reader per input format
    reg-resolve/    #   per-register resolution
    reg-build/      #   link, emit and the CLI (binary: reg-build)
  curation/         # the new builder's reader config and id ledgers
  reg_mona_runner/  # MONA extract runner: order.json -> CSVs inside MONA (R)
  reg_export_gate/  # export gate: MONA check of outputs before export (R), schemas
  conformance/      # contract corpus, run against the Rust server
  plugins/          # the microdata-tools-se agent plugin (hosted MCP, skills incl. the scaffold)
  reg_webapp/
    edge/           # global hosted MCP edge worker
    stewards/
      swecov/       # the old builder's inventory, retained until milestone E
```

Dependency graph (acyclic):

```text
reg-meta       → reg-catalog → reg-core
reg-core-py    → reg-core
reg-build      → reg-resolve, reg-readers → reg-frame, reg-core
reg-frame      → reg-core
reg_meta_build → reg-core-py
reg_webapp     → reg-meta (server container and hosted MCP edge)
reg_mona_runner → the order manifest (a file, offline inside MONA; no code dependency)
reg_export_gate → its schemas and policies (files, offline inside MONA; no code dependency)
```

Nothing is published to PyPI. The runtime releases on `reg_meta/v*` tags: the `reg-meta`
crate version, the catalog asset and the `reg-meta` binaries for macOS arm64 and Linux
x86_64. `reg_meta_build` is tagged (`reg_meta_build/v*`) but not published: it depends
on the workspace-only `reg-core-py` and runs from a maintainer checkout. The webapp
ships as a container image built from the newest `reg_meta/v*` release's assets.

### Why this split

- **Runtime vs builder** — `reg-catalog` reads; `reg-build` writes. They share the
  artifact contract and `reg-core`. The built SQLite catalog (`reg_meta.db`, documents
  included) is distributed as one `reg_meta.db.zst` **GitHub release artifact** on
  `reg_meta/v*` tags, fetched with `curl`, SHA-256 verification and `zstd`. See
  [`crates/DESIGN.md`](crates/DESIGN.md) for the runtime and
  [`crates/reg-core/DESIGN.md`](crates/reg-core/DESIGN.md) for the project contract.
- **Compile, don't resolve.** The artifact is immutable and rebuilt on every release, so
  anything that is a pure function of it is computed once by `reg-build` instead of on
  every query. Resolution belongs to `reg-resolve` and the link stage; the reader keeps
  only request-dependent rules.
- **One implementation per contract.** Each contract that crosses a language or package
  boundary (grammars, folds, the project validator, result types) has one home, reached
  directly from the Rust builder and runtime, and through `reg-core-py` from the legacy
  Python builder. The table is in [`crates/DESIGN.md`](crates/DESIGN.md), "One
  implementation per contract".
- **Builder and runtime are Rust.** The server and MCP tools share one validator, one
  grammar and one set of result types, and local MCP is one binary. The Python builder
  and its tests remain until milestone F; SWECOV uses its old-schema release until E.
- **One operation set for clients.** Exposed over HTTP and MCP (hosted at
  reg-meta.vqv.uk and data.swecov.se, and local `reg-meta mcp`); there is no query CLI
  and no human-oriented output. The interface rules are in
  [`crates/DESIGN.md`](crates/DESIGN.md), "Agent and web interface".

### Accepted source revisions and catalog generations

`reg-build` reads raw inputs through `reg-readers`, resolves each register with
`reg-resolve`, links registers and emits the SQLite catalog, including search and
documents. There is no prepared stage. `curation/inputs.toml` pins the input repository
and commit; its manifest pins input hashes and release assets. Reader config and id
ledgers live under `curation/`; curation statements arrive in milestone D. The builder
design is in [`crates/DESIGN.md`](crates/DESIGN.md).

The runtime and `reg_webapp` consume only the published SQLite generation. They never
read the input repository or combine its current state with an older artifact. A failed
build leaves the published artifact unchanged. Publication verifies the artifact and its
checksum before deployment.

Documents and catalog metadata share the artifact, but indexing a document does not make
it a source of variable availability or executable transformations. Holdings arrive as a
separate layer in E. None of these inputs change the MONA constraint: PII stays inside
MONA and only aggregate, disclosure-controlled results leave it.

### Real-seed output cache

The legacy Python builder, retained until F, uses prepared inputs and a real-seed cache.
Its `prepare-sources` and `build-db` runs take one to two hours each, so
`scripts/real_seed_cache.py` reuses their outputs by input key. It is tooling over the
builder's existing reports and checks, not builder code. Each key covers only what its
step reads, so a resolution change reuses the preparation; the module docstring lists
the fields. The invariant is that a key leaving out a real input would return stale
output silently, so every key errs generous, a hit re-checks what it returns, only
completed runs are stored, and `build --verify` reruns uncached and compares bytes. The
digest, staging and eviction helpers live in `scripts/keyed_cache.py`. The legacy
builder persists its resolve phase, so the cache reuses a resolved bundle and reruns
only materialization. This preparation and cache contract does not apply to `reg-build`.

### Catalog artifact identity

A publishable `reg_meta.db` is one file: the catalog and its documents. The reader
admits it exactly: `meta.schema_version` equals `reg_core::CATALOG_SCHEMA_VERSION`,
every required `meta` key is present and `generation_id` is 64 lowercase hex digits;
anything else is `schema_incompatible` and fails before serving
([`crates/DESIGN.md`](crates/DESIGN.md) → "Artifact admission"). There is no read scope,
no second database and no `REG_WEBAPP_STEWARD`.

Generation identity hashes canonical semantic inputs and the schema version, not output
bytes, timestamps or host paths; builder revision, accepted public pins and the curation
digest participate. Publish the final file SHA-256 separately. Search cursors and the
logical order use `generation_id`, and HTTP read identities include it. The reader opens
the file read-only. Failures leave prior artifacts and accepted inputs unchanged.

## Export gate

The export gate is the toolkit's disclosure checker and transfer: the only path by which
results computed on microdata reach an agent. Its design (components, trust boundaries,
schemas and policy, the check, the receipt, MONA mode and the shared case corpus) is in
[`reg_export_gate/DESIGN.md`](reg_export_gate/DESIGN.md). The MONA check
(`reg_export_gate/export_gate.R`) is built; the `reg-export-gate` Rust binary is not
(#1803).

## Repo-wide invariants

These are hygiene that keeps options open, enforced in CI where noted. Package-local
mechanisms are documented in the owning DESIGN.md and only summarized here.

- **Build / runtime cleanly separated.** `reg-build` is the catalog writer;
  `reg-catalog` and `reg-meta` read the artifact. The legacy Python builder remains
  operator-side until F and writes only its old-schema artifacts.
- **Latest tooling.** Newest stable versions of languages, crates, packages and SDKs,
  and modern methods; no compatibility work for older toolchains. Windows is served by
  hosted MCP unless a local binary builds without special effort.
- **Stateless server.** No process-local caches that change behavior across requests;
  the catalog DB is opened read-only.
- **OpenAPI is the canonical contract.** `crates/reg-meta/openapi.json` is committed and
  regenerated by `scripts/gate.py regen`. CI guards drift with a **snapshot test**
  (`cargo test -p reg-meta --test openapi_snapshot`) that asserts the committed file
  equals the server's rendered document. (There is no `make` target — drift is a failing
  test, not a Makefile step.) The plugin project schema is extracted by
  `scripts/gen_project_schema.py`; `regen` refreshes it and `lint` checks its bytes.
- **Performance budget (v1 targets, not yet CI-enforced).** Starting points: the catalog
  reads the Rust server answers (`/api/catalog/*` and the facets
  `/api/{segments,findings,values,graph}/*`) p95 ≤ 200 ms (cache miss);
  `/api/project/validate` and `/api/project/order` p95 ≤ 1 s; representative broad
  `/api/search` cache-miss p95 ≤ 500 ms. Search correctness and latency are origin
  properties: edge hits do not substitute for cold-origin evidence.
  Classification/value-set initial responses must be bounded by bucket/page limits
  rather than total code cardinality, and cold/repeat rendered routes target CLS < 0.1.
  The 200-column load-test fixture is committed
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

  | Boundary                                     | Oracle (data, not code)                                                                                              |
  | -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
  | Raw inputs + curation → catalog artifact     | synthetic source fixtures → `reg-build verify` + repeated build bytes                                                |
  | Legacy prepared inputs + curation → artifact | synthetic source fixtures → `validate_built_db` + content snapshot                                                   |
  | Builder CLI → JSON                           | request → expected JSON cases in `reg_meta_build/tests/cases/cli/`                                                   |
  | Artifact → HTTP and MCP                      | `openapi.json` snapshot + request → response goldens run against the Rust server over the fixture DB                 |
  | `project_data.json` → validation result      | `crates/reg-core/tests/project/corpus/` run by the Rust consumers                                                    |
  | Project + artifact → logical order           | the order cases in `conformance/cases/order/`: project + fixture → expected logical order or `order_blocked`         |
  | Candidate output → export gate result        | `reg_export_gate/tests/cases/`: schema, policy and candidate folder → pass and receipt, or refusal code and findings |
  | Curation TOML → load or located failure      | committed TOML must load; malformed cases name the locator                                                           |
  | FQID / period grammars, interval algebra     | Hypothesis properties + round-trip snapshots                                                                         |
  | Real artifact ↔ accepted inputs              | conformance artifact checks and explicit accepted-input table/cell census, spec and authored mappings                |
  | Repository tooling (`scripts/tests`)         | the tool's own contract: skill discovery, a lint's verdict                                                           |

### Tiers

1. **Package suite (budgeted, every change).** Contract tests over synthetic fixtures
   built from readable source, property tests, golden corpora: the default `pytest`,
   `cargo test --workspace`. Run narrowed by package while iterating. Each package stays
   inside its budget below.
2. **Push / CI.** `ci.yml` runs the Python suites as a `test` matrix, one leg per root
   `testpaths` entry, each with `timeout-minutes` at its CI budget below. Conformance is
   not one: it requires the Rust server (`--server-cmd`). The `rust` job's 6-minute
   timeout covers `crates/` and the gate's `rust` step (`scripts/gate.py`: the workspace
   build and the whole conformance suite against the Rust server), with room for a cold
   rust-cache and its save. The legacy builder's matrix leg runs `gate.py release`: its
   full Python suite, then the `release`-marked admission checks on the synthetic
   catalog artifact. The `lint` job runs `gate.py lint` (ruff, panache, ty, plugin
   schema drift and provider vocabulary). The OpenAPI snapshot is a `crates/reg-meta`
   test. A job that exceeds its budget fails.
3. **Artifact (maintainer or release gate).** Run
   `pytest conformance -m release --run-release --artifact-dir=/path/to/catalog --server-cmd=...`
   after `cargo build --workspace` (the search traversal runs against the Rust server;
   see `conformance/README.md` for the template). The reader admits the selected
   artifact before execution; incompatible or non-publishable artifacts fail, never
   silently skip. The artifact checks run `reg-build verify`, check indexed search kinds
   and paging depth, and compare sampled logical orders and validation against an
   artifact census. Repeated search and successful order responses must have identical
   raw bytes; HTTP and MCP orders must agree as JSON. CI runs these checks against the
   single published catalog asset in `integration.yml`; the SWECOV integration job is
   deleted until milestone E, and there is no separate docs asset. Published schema
   drift is a release-drift failure. Pinned historical baseline acceptance, exhaustive
   representative resolution, performance and rendered checks remain separate maintainer
   work.

Budgets are what a lean suite for the package needs, derived from its boundaries, not
from the suite's current size. Local is wall time with `pytest <tree> -n auto -q` (or
the pytest line of `gate.py rust`) on a 10-core developer Mac; CI is the job's
`timeout-minutes` in `ci.yml` on `ubuntu-latest`, including setup. A suite over budget
is a finding for the `test-audit` skill, not a reason to raise the number. The R
packages (`reg_mona_runner`, `reg_export_gate`) have no tier: their testthat suites are
run by hand, not by the gate or CI.

  | Suite                                | Local | CI job | Rationale                                                                                                                           |
  | ------------------------------------ | ----- | ------ | ----------------------------------------------------------------------------------------------------------------------------------- |
  | `reg_meta_build/tests`               | 45 s  | 6 min  | The compiler: a few hundred source-fixture → artifact cases at about 1 s CPU each, plus one session load of the committed curation. |
  | `scripts/tests`                      | 20 s  | 2 min  | Repository tooling contracts (skill discovery, lints); subprocess runs of the scripts dominate, `test_real_seed_cache` most.        |
  | `crates/` (`cargo test --workspace`) | 10 s  | 6 min  | The Rust runtime's unit and property tests; G0 always runs them.                                                                    |
  | conformance (`scripts/gate.py rust`) | 30 s  | 6 min  | Session-built artifacts, then data-driven HTTP, MCP, order and validate cases against the Rust server; shares the `rust` job.       |

One verification gate cuts across the tiers. It is named G0 so it is not confused with
the tiers. G1 and G2 (the byte-for-byte differential harness against the previous
release) are retired on the rebuild branch: they cannot compare across the schema break,
and each release is checked by `reg-build diff` against the previous one.

  | Gate | What runs                                                                                                                                  | Budget                           | When         |
  | ---- | ------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------- | ------------ |
  | G0   | `scripts/gate.py g0` (lint, `cargo test --workspace` and the touched packages) and `scripts/gate.py rust` (conformance on the Rust server) | under 60 s, plus 30 s for `rust` | every change |

Local G0 defaults to `scripts/tests`. Until cutover F, the legacy `reg_meta_build` suite
runs in `gate.py release` and remains mandatory on every PR in its CI matrix leg. It is
also available through `g0 --packages reg_meta_build` for release fixes; plain `pytest`
still runs both Python suites. No legacy cases are removed by this tier split.

`crates/` takes 10 s of G0's 60 s, so any change touching the runtime fits it;
conformance runs in `rust`, with its own 30 s row. A slow gate is a defect to fix, not a
reason to skip it.

### Reader and builder tracks

The reader (`reg-catalog`, `reg-meta`) and the builder (`reg-build`) ship on different
cadences. These rules keep them coupled only through released artifacts.

1. **Reader work develops against the pinned release.** G0 and the `rust` step run on
   synthetic fixtures. Reader work needs no real-seed build. A reader-only change
   deploys on push to main: `container-build.yml` bakes the newest release's assets.
2. **Derived tables and schema.** A derived-table change ships only with the next
   release, because deploys bake released databases without deriving. Core-DDL changes
   the reader needs are batched into the next builder content release. The reader admits
   only `reg_core::CATALOG_SCHEMA_VERSION` exactly. The deploy guard compares that
   constant with the newest release's and fails closed on a mismatch or absence. After a
   schema bump merges, deployments wait until the maintainer publishes its matching
   release; a gap is allowed.
3. **Legacy preparation** applies only to `reg_meta_build`, retained until F. `reg-core`
   edits invalidate its prepare, build and resolve keys. An accepted prepared root can
   be reused after a prepare-key miss. `reg-build` reads raw inputs directly.
4. **Releases follow builder batches**, including schema and derived-index changes.

### Conformance suite

Every tier runs `conformance/` at the repository root, always against the Rust server
(`--server-cmd` is required). Thin loaders run its readable cases against it: HTTP and
MCP responses, logical orders, server startup and project validation. Cases assert
domain outputs or located errors, not object internals or query implementation. Product
adapters are not added solely for testing. Fixture-bound cases always use their named
source-built synthetic artifacts, including in a tier 3 run. Artifact-parametrized
checks default to session-built catalog artifacts and use exactly `--artifact-dir` under
tier 3. They derive expectations from artifact content or artifact/HTTP agreement, never
real-identifier goldens.

Repeated search and successful order comparisons use raw bytes; no fields are removed,
and key/list order remains observable. HTTP/MCP order agreement compares JSON documents.
The existing `crates/reg-core/tests/project/corpus/` serves its Rust consumers.

Private imports, internal patches and oversized test modules are gated by repo lints
with no exemptions or file allowlists. The Rust reader runs against these oracles.

Coverage is measured as boundaries with a corpus, not as line percentage; a boundary
without a corpus is the gap.
