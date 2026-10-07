# Rust runtime and a compiled catalog

**Status: accepted 2026-10-07; root-level refactor tracker.** The maintainer took the
decisions in section 13 on 2026-10-07. This file is a scoped, self-deleting tracker
under the governance exception in `CLAUDE.md`/`AGENTS.md`, alongside `REFACTOR_SPEC.md`.
As each stage ships, its design rationale moves into `ARCHITECTURE.md` and the package
`DESIGN.md` files and its section here shrinks. **Completion gate: deleted when stage 5
(SPA on WASM) ships.**

The question asked: should `reg_meta` be ported to Rust, both for speed and to rebuild
the CLI cleanly? The answer is yes, but not as a port of `reg_meta` alone. This is a
redesign of everything that runs after the artifact is built, with the build itself
staying in Python.

## 1. What was measured

All timings: v0.41.0 release catalog (schema 9.0.0, 1.23 GB `reg_meta.db`), warm page
cache, Apple Silicon, median of three runs of the installed `reg-meta` CLI.

  | Command                                         | Wall time  | Where the time goes                                                                    |
  | ----------------------------------------------- | ---------- | -------------------------------------------------------------------------------------- |
  | `--version`, `get register`, `get varinfo`, ... | 220–300 ms | Python start-up. `import reg_meta.cli` alone is ~190 ms, mostly Pydantic model build.  |
  | `search --query kön`                            | ~650 ms    | 70% inside SQLite. 1,646 `execute` calls. 139k calls into the Python `py_lower` UDF.   |
  | `search --query 0115`                           | ~860 ms    | 93% inside SQLite. 1,221 `execute` calls.                                              |
  | `get schema --register LISA --summary`          | ~420 ms    | Mostly Python: slug validation, storage-ID JSON rewriting.                             |
  | `get coded-variables`                           | **~72 s**  | One SQL statement (COUNT DISTINCT over 5.9M value-set members). Bad plan; fixed #1175. |

What this says:

- **Start-up stops mattering.** Python start-up costs ~250 ms per CLI call, but the
  runtime now runs as long-lived processes (`serve`, `mcp`), so start-up is paid once.
  The case for Rust rests on one implementation across build, server and browser
  (sections 5 and 6), memory use and typed contracts, not on start-up.
- **Search and the slow commands are query-design problems.** A Rust reader runs the
  same SQLite engine. Ported line by line, it would still issue 1,600 queries per search
  and still scan 140k rows through a folding function.
- **The fix for query cost is to move work into the build** (section 3). That fix is
  also what makes a Rust reader small enough to be worth writing.

## 2. What `reg_meta` actually is today

`reg_meta` is three things sharing one package:

1. **Contracts the build depends on.** `reg_meta_build` subclasses `source_evidence` and
   `documentary` Pydantic models, calls `canonical_sha256` ~60 times (it feeds
   generation identity), mints slugs with `fqid`, uses the inventory interval helpers
   (including private `_merge`, `_render`, `_intersect`), and builds its own CLI on
   `reg_meta.cli_common`. `holdings_compile` imports the reader's `Catalog` to
   canonicalize representations, so the compiler depends on the reader.
2. **A read library.** The webapp builds a `Catalog` per request and calls ~35 of its
   methods. About 30 `reg_meta` Pydantic models are FastAPI response models, so they
   shape `openapi.json` and the frontend's generated types. The webapp also runs raw SQL
   against the catalog schema.
3. **The CLI.** Its main users are agents, through the `register-metadata-search` skill.
   The Docker build and the publish workflow also call it.

Duplication found along the way:

- **Two query layers.** `queries.py` serves the CLI and `Catalog` serves the webapp.
  Terminal succession, classification editions, classification codes, lineage and
  register resolution each exist twice.
- **Two search rankings.** The webapp calls `search()` five times per request, then
  applies its own best-bets ranking and golden pins (`routes/search.py`, `golden.py`).
  CLI users and agents never see that ranking.
- **Four period grammars.** `reg_meta/fqid.py`, `reg_schema/structural.py`, and the
  frontend's hand-written `period.ts` (1,115 lines) and `validation.ts`. A parity test
  keeps two of them aligned.
- **Four text folds.** Python `str.lower` (`py_lower`), `str.casefold`,
  NFKD-strip-casefold, and SQLite's ASCII-only `NOCASE`/`LOWER`, mixed without a stated
  rule.

Smaller defects (not blocking, listed so they are not ported):

- Row-order-dependent results: the `same_as` BFS and the classification editions year
  map read rows without `ORDER BY`.
- CLI inconsistencies: register is positional in some commands and `--register` in
  others (defined 11 times); years are `--years`, `--year` or `--from/--to`; pagination
  is `--cursor` or `--offset`; `data` changes shape with result cardinality;
  `--summary`/`--flat` are silently ignored in JSON; `order` bypasses the envelope and
  the dispatch table; exit 17 means both "no match" and "order blocked"; exit 20 is
  defined and unused.
- `update` crashes as `internal_error` on non-tty stdin without `--yes`; malformed
  `resolve` stdin exits 30 instead of 2.
- Downloads are not checksum-verified. Legacy bare `v*` release tags are still accepted.
- Import cycles (`catalog` ↔ `graph`, `catalog` → `doc_queries` → `queries` → `catalog`)
  hidden by lazy imports.

## 3. Principle 1: compile, don't resolve

`reg_meta/DESIGN.md` currently says: "No state/window resolution is compiled." Decision
1 reverses that rule; the text changes when the derive slice for states ships.

The artifact is immutable and rebuilt from scratch on every release. Anything that is a
pure function of the artifact can be computed once instead of on every query. An audit
of the reader's hotspots classified them:

- **A — pure function of the artifact.** Becomes a derived table.
- **B — query-dependent, but served by a precomputed column or index.**
- **C — genuinely request-dependent.** Stays in the reader.

About 70% of the hotspots are A or B. The main ones:

  | Read-time work today                                                                                                     | Class | Compiled replacement                                                    |
  | ------------------------------------------------------------------------------------------------------------------------ | ----- | ----------------------------------------------------------------------- |
  | `py_lower(col) LIKE py_lower(?)` scans (alias 82k rows, variable 54k)                                                    | B     | `*_folded` columns + index; trigram FTS5 for substring match            |
  | `py_catalog_column` UDF, `representative_columns`, spelling caches                                                       | A     | `canonical_column` stored on states, aliases, windows, warnings         |
  | `_expand_state_windows`, alias-window participation rules                                                                | A + C | `state_projection` relation (see below); request fallback stays         |
  | `register_variable_deliveries`, `_fuse_windows`, coverage                                                                | A     | `browse_delivery`, `delivery_window` and `resolver_column` (see below)  |
  | `_fuse_provider_held_deliveries`, `scope_predicate` string splicing                                                      | A     | Per-scope rows (`scope = 'holdings'`) in steward artifacts              |
  | `_state_warning_ids` (one `data_warnings` call per state)                                                                | A     | `state_warning` link table                                              |
  | Classification/variable chains, terminal successors, editions, families                                                  | A     | `succession_terminal`, `classification_chain`, `classification_family`  |
  | `same_as` BFS                                                                                                            | A     | `same_as_resolution` table                                              |
  | Concept-group tag N+1, group member assembly                                                                             | A     | `concept_group_tag`, pre-ordered member rows                            |
  | `get coded-variables` (72 s)                                                                                             | A     | `coded_variable_stats` per scope; reader applies only filters and limit |
  | `variable_fts_content` view (correlated `group_concat` for 43k variables)                                                | A     | Materialized FTS content table                                          |
  | Search arm merge, scoring, cursor, fold decisions; period intersection with a request; `get diff`; order materialization | C     | Stays in the reader                                                     |

Consequences:

- **The resolver gets one home, in the build** (the derive step, section 4). The
  alias-window, replacement and coding rules are implemented once, in Python, where
  validation can check them. The reader never re-implements them. This is the largest
  reduction in port risk: the hardest semantics never need a second implementation.
- **Window participation is only partly compiled.** Which windows a state has, and their
  representation metadata, are artifact facts. Whether a window *replaces* its base
  state depends on the requested period: a period that no window overlaps falls back to
  the base state (`_participating_windows` in `catalog.py`; pinned by
  `test_gap_year_month_falls_back_to_annual_state`). That fallback stays in the reader
  (class C), as a small rule over compiled rows. Gap, partial-overlap and spanning cases
  are pinned as `api` cases before the moved code is reduced.
- **Projected states are their own relation.** `variable_state.state_id` is a primary
  key, and a year's month windows share their annual state's `state_id`, so projected
  states cannot be rows of `variable_state` without changing its grain under the Python
  reader. `state_projection` has its own identity, references the source state, and
  links warnings and lineage through it.
- **Browse and resolver eligibility are different contracts.** Browse deliberately keeps
  alias windows that no state contains (`register_variable_deliveries`); holdings
  accepts only resolver-emitted columns (`holdings_compile.py`). `browse_delivery`
  serves browse; `resolver_column` is the only relation that authorizes a holdings
  mapping. Each has its own invariants.
- **The compiler stops importing the reader.** `holdings_compile` canonicalizes against
  the derived `resolver_column` relation.
- **Scope becomes data.** Only two scopes exist (`reference`, `holdings`), and holdings
  exists only in steward artifacts. Precomputing per-scope rows replaces the
  string-spliced predicates in ~25 queries.
- **Reader size.** Of ~10.9k lines in `catalog.py` + `queries.py`, an estimated 35–40%
  of lines and 10–15% of the algorithmic logic remain. The remainder is mostly SQL plus
  sort/merge.
- **Artifact size.** Roughly +70–100 MB (6–8%) before `state_projection`, which slice 3b
  re-estimates. Keep it a narrow relation referencing base states, not a copy of their
  content (+200 MB). Do not precompute code owners (up to 3.9M rows).
- **New invariants the derive validator must own:** browse delivery windows disjoint;
  `resolver_column` equal to the resolver's emitted columns; one canonical spelling per
  (variable, variant, fold); chain/terminal tables acyclic and consistent with
  `*_replaced_by` at the manifest year; `state_warning` equal to the attribution
  predicate; held-\* tables equal to holdings facts; aggregate tables equal to a
  recomputation; fixed insertion order for byte-identical output.
- **Lost test seam.** The `classification_as_of_year` override goes away. Tests build
  artifacts with a different manifest year instead.

## 4. Working structure: decouple the refactor from the slow build

A full base build takes about an hour (section 11). If every step of this refactor
needed one, the refactor would crawl. It does not need one: almost all of its work sits
downstream of curation.

**Split the build at a stable boundary.**

- **Base** — today's curation-driven build (`build-db`, `extend-db`). It produces the
  core tables. Only curation and source changes touch it. It stays slow until the build
  track (section 11) speeds it up.
- **Derive** — a new step, `reg-meta-build derive`, that reads a built database and
  writes the derived tables of section 3. It is a pure function of the base artifact,
  deterministic, and has its own validator. Steward artifacts interleave it with
  `extend-db`, because holdings must see the steward's own metadata (`extend_db.py`
  inserts the steward core graph before compiling holdings): insert the steward core →
  derive the reference tables over base plus overlay → compile holdings → derive the
  holdings tables.

**The derived artifact has its own identity contract** (stage 2). Base identity,
derivation identity (derive code and schema minor) and the admitted schema versions all
feed the served generation, so a re-derived artifact can never answer under a cursor
issued for the previous one. Derive reads an immutable base, publishes atomically,
leaves base bytes unchanged, rejects a stale derivation, and is byte-identical on
repeat. Today the builder requires exact schema equality (`db.py`) and the generation
records one schema version and builder commit (`artifact_identity.py`); both change in
stage 2.

**Bootstrap derive by moving, not rewriting.** The first implementation of each derived
table calls today's reader functions (`Catalog.states`, the delivery fusing, the chain
walks), moved into `reg_meta_build`. Its output then equals what the current reader
returns, by construction, and the risky resolver logic moves without semantic change.
Measured cost: `Catalog.states` over a 300-variable sample takes 6 ms per variable, so
all 54k variables take ~5 min on one core, about 1 min in parallel. Replace a moved
function with set-based SQL only when the tier-1 budget below requires it.

**Ship derived tables additively.** The schema gate accepts a database whose minor
version is at least the code's. Derived tables land as schema 9.x minor bumps, so the
current Python reader and webapp keep working on every intermediate release. There is no
big-bang artifact cutover; the major bump comes only when the Python reader is deleted.

**Develop against pinned artifacts.** Refactor work uses a pinned release base (0.41.0
today, moved forward only at agreed checkpoints) plus the synthetic conformance
fixtures. Curation work keeps changing the base; the refactor changes derive and the
reader. Neither waits for the other.

**Three verification tiers, with budgets.**

  | Tier | What runs                                                                                                                                                                                      | Budget      | When                                         |
  | ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | -------------------------------------------- |
  | 0    | Unit tests and conformance on synthetic artifacts, Python and Rust. Today 215 conformance cases take 12 s, synthetic builds included.                                                          | under 30 s  | every change                                 |
  | 1    | Derive on the pinned real base, then a differential run of the baseline reader against derived tables (from stage 2) and the Rust reader (per operation, as it lands), on both artifact kinds. | under 5 min | every PR touching derive or the reader       |
  | 2    | Full base build plus derive and tier 1 on the result.                                                                                                                                          | ~1 h today  | batch checkpoints and releases, never per PR |

The tier-1 baseline is the Python reader **at a pinned commit, installed in its own
environment**, never the checkout under change, so a regression moved into derive cannot
validate itself. It runs on both artifact kinds (the global catalog and the SWECOV
steward artifact) and both scopes, over generated queries (every register, seeded
variable samples, holdings-specific strata, the search-eval corpus terms). Each accepted
difference is recorded as a named semantic exception in the harness.

A slow tier is a defect to fix, not a reason to skip the tier. If derive exceeds its
budget, make the slow table set-based before adding more tables.

**Work in vertical slices.** Each slice adds derived tables, the Rust reader functions
that read them, and their conformance and differential cases, and is verified at tiers 0
and 1 only. Slices share the pinned base, so independent slices can be built in parallel
by separate agents. Slice order is set by risk: admission + search first, order last.

**Execution protocol** *(decision 14)*. Agents build the refactor, one work package per
PR. These rules keep them on target between maintainer checkpoints without a human in
the loop:

- **Packages live in this file** (section 10), so every worktree sees the same plan. A
  package names the sections it implements, the paths it may change, what is out of
  scope, its acceptance commands and case IDs, and what it depends on. A package that
  does not fit in about ten lines is split. A stage's packages are written when the
  stage starts, against the merged state, never further ahead.
- **Done is mechanical.** A PR is done when its acceptance commands pass, tier 0 (and,
  from stage 2, tier 1) is green within budget, and a fresh agent that did not write it
  has reviewed it against its package and this file. The implementing agent never
  declares its own work done.
- **Escalate, don't decide.** An agent stops and reports instead of changing any of: the
  operation table or error catalog (section 7), a decision in section 13, the meaning of
  an existing golden expected file, the schema major version, a tier budget, or the
  dependency list this file names. A golden change that only converts a shape as the
  approved operation table prescribes is not an escalation; it is reviewed like any
  diff. Everything else the agent decides, and records in the PR.
- **Small PRs, squash-merged to main.** Everything before stage 4 is additive (schema
  9.x minor bumps, a new server beside FastAPI), so main stays deployable and there is
  no long-lived integration branch.
- **At most three packages in flight.** Files every slice would edit (route and tool
  registration, the derived-table list, conformance indexes) are split into one file per
  slice. Schema minor bumps merge one at a time.
- **Tier 2 never runs inside a package.** The maintainer runs it at checkpoints, in the
  background.
- **Four maintainer checkpoints:** (1) end of stage 1: approve the operation table,
  error catalog, surface inventory and fold spec, and decide whether indexed text is
  pre-folded (section 5); (2) after slice 3a: approve the pattern the other slices copy; (3)
  before stage 4: go or no-go on the cutover, with hosted MCP already live; (4) stage 5
  done, and this file deleted.

## 5. Principle 2: one implementation per contract

Each contract that crosses a language or package boundary has exactly one
implementation:

  | Contract                              | Single home                           | Reached from                                         |
  | ------------------------------------- | ------------------------------------- | ---------------------------------------------------- |
  | Artifact schema (DDL, manifest)       | `reg_meta_build` (Python)             | Rust reader reads it; `SCHEMA_VERSION` gate          |
  | Resolver semantics                    | `reg_meta_build` derive (Python)      | Compiled into the artifact                           |
  | FQID and period grammar               | Rust core crate                       | Build via Python bindings; SPA via WASM              |
  | Text folds                            | Rust core crate                       | Build via Python bindings (fills `*_folded` columns) |
  | Canonical JSON + SHA-256              | Rust core crate                       | Build via Python bindings                            |
  | Project schema + structural validator | Rust core crate (replaces reg_schema) | Server, MCP; SPA via WASM; JSON Schema export        |
  | Result types                          | Rust structs (serde + schemars)       | HTTP OpenAPI, MCP tool schemas, frontend TS types    |

Notes:

- **Text folds get a written spec.** Two named folds:
  - `fold_identity` — Unicode lowercase with the Final_Sigma context rule, no
    normalization. This is today's `py_lower` (Python `str.lower()`, which does apply
    Final_Sigma) and the column identity rule. In Rust it is `str::to_lowercase`.
  - `fold_search` — strip, full case folding, NFKD, drop characters with a nonzero
    canonical combining class, collapse whitespace. This is today's search-text fold and
    is *not* what FTS5 `unicode61` does to indexed text: `unicode61` folds neither ß nor
    ligatures nor full-width forms (`straße`/`strasse`, `ﬁlm`/`film`, `ＡＢＣ`/`abc`
    match under `fold_search` but not under `unicode61`). Whether indexed text is
    pre-folded with `fold_search` or stays raw is decided at checkpoint 1, with the
    distinguishing cases in the fold corpus.
  - Helper classes follow Python, not Rust's std: whitespace is `White_Space` plus
    U+001C..U+001F; "alphanumeric" (`[^\W_]`) is general category L\* or N\*, not Rust's
    `Alphabetic` property.

  The build computes folded columns with the same code the reader uses for the query
  string, through the bindings. Pin one Unicode version for all of `reg-core` (stage 0
  found Rust std and `unicode-normalization` on 17.0, `caseless` on 16.0, Python on
  16.0). Keep the stage-0 parity sweep as a property test.

- **Canonical JSON** has no floats today. Keep it that way. A float in a hashed payload
  is a build error.

- **Messages are structured.** Error and blocked-order findings carry a code and fields.
  Prose is rendered from templates. Goldens assert codes and fields, so no Python `repr`
  quoting (`'other'`, `None`) appears in any contract.

## 6. Target architecture

```
                 reg_meta_build (Python, maintainer-only)
                   base: adapters → IR → curation → core tables      (slow)
                   derive: core tables → derived tables              (fast)
                   uses reg_core (Python bindings) for FQID, folds, canonical hash
                                   │
                                   ▼
                    reg_meta.db (+ docs DB), immutable
                                   │
                      reg-catalog: one operation set
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
             reg-meta serve                 reg-meta mcp
             HTTP API (SPA)                 MCP over stdio
             + remote MCP endpoint          (local catalog)
             (hosted agents)
                    │
          SPA (Svelte) + reg_core WASM
          for period grammar and project validation
```

There is no query CLI (decision 11). Every operation is defined once in `reg-catalog`
and exposed over HTTP and MCP.

Rust workspace (`crates/`):

- `reg-core` — no IO. FQID and period grammar, folds, interval algebra, canonical
  JSON/hash, project-data types and structural validator, error codes. Compiles to
  native, Python extension (PyO3, via maturin) and WASM.
- `reg-catalog` — the reader. Opens and admits an artifact (read-only, `immutable=1`,
  schema gate, identity checks), owns scope as reader state, and defines the operation
  set: one function per operation, typed parameters and results with JSON Schemas.
  Includes search (one ranking, including the webapp's best-bets and golden pins, which
  become curated build input), order materialization and project semantic validation.
- `reg-meta` — the binary, with run modes only:
  - `serve` — the HTTP API for the SPA (axum + utoipa; ETag, body-size limit and rate
    limit as tower layers) and a remote MCP endpoint over streamable HTTP for hosted
    agents.
  - `mcp` — MCP over stdio against a local catalog, for offline use and private steward
    catalogs.
  - `fetch` — downloads a catalog release, verifies its SHA-256 and atomically activates
    it, for local `mcp` installs.
- `reg-core-py` — the PyO3 module the build imports.

What is deleted: the Python `reg_meta` package including its CLI, `reg_schema`, the
FastAPI backend, and the frontend's hand-written grammar and validation mirrors. Modules
the build shares with the Python reader (`source_evidence`, `documentary`, `cli_common`,
inventory TOML loading) move into `reg_meta_build` at the stage-4 cutover.

Why the HTTP server moves to Rust rather than FastAPI calling Rust through bindings:

- With bindings, every result type exists twice: as a Rust struct and as a Pydantic
  model for OpenAPI. That is the drift this design removes.
- The backend is ~6k lines. About 3k of them are catalog logic (node assembly, search
  ranking, semantic validation) that belongs in the operation set anyway, so agents get
  it too. The rest is HTTP glue that axum and tower handle directly.
- The deploy image becomes one static binary plus the baked DBs. Keep the 1 GB Fly VM
  for the page cache.

## 7. Agent and web interface

**Users are agents and the webapp only** *(settled, decision 10)*. Agents use MCP; the
SPA uses HTTP. Interactive human use is out of scope while building: no feature, flag or
output exists for it. It is re-evaluated after stage 5.

**No query CLI** *(settled, decision 11)*. Every former CLI user moves:

  | Former CLI user                          | Replacement                                                               |
  | ---------------------------------------- | ------------------------------------------------------------------------- |
  | Agent skill (`register-metadata-search`) | MCP tools: the hosted endpoint by default, `reg-meta mcp` locally         |
  | Docker bake (`reg-meta update`)          | `curl` the release asset, verify its SHA-256, decompress with `zstd`      |
  | Publish workflow smoke test              | Start `serve` in CI and probe endpoints; conformance against the artifact |
  | Conformance CLI cases                    | HTTP request cases                                                        |
  | `reg-meta order project.json`            | `POST /api/project/order` and an MCP tool                                 |

**One operation set, two transports.** Each operation in `reg-catalog` has a name, a
typed parameter object and a typed result, all with JSON Schemas. The HTTP routes and
the MCP tools are generated from those definitions, so the two transports cannot drift.
OpenAPI and MCP `tools/list` are the self-description; there is no separate `describe`.

Rules (settled 2026-10-07; first agreed for the CLI, carried over to the API):

- **JSON only, `{data, meta}` on every response**, except raw-bytes downloads (document
  PDFs, the order manifest file), which carry their media type and have a JSON operation
  for their metadata. `data` is the result; `meta` holds the contract version, catalog
  generation and scope. Agents always know which catalog answered.
- **Deterministic responses.** `meta` carries no timing, so the same request on the same
  catalog returns the same bytes and goldens compare raw output. Performance is measured
  by the tier-1 harness.
- **A ref is an FQID or a bare name.** Any entity is a ref: `provider`,
  `provider/register`, `provider/register/slug`, `class/slug`, group keys, or a bare
  name. A unique name resolves; an ambiguous one returns the candidates with their FQIDs
  and an `ambiguous_ref` error, so the next call can be exact.
- **`show` plus facet operations.** `show(ref)` returns a summary for any kind
  (register, variable, classification, group). Separate operations fetch the heavy
  parts: states, values, lineage, graph, coverage.
- **One time filter: `period`.** Every operation that filters by time takes `period`,
  parsed by the FQID/project period grammar in `reg-core` (`2019`, `2015..2019`,
  `LA2019`, `2019-03`, `2019-01-01..2019-06-30`). `diff` takes two periods, `from` and
  `to`, in the same grammar.
- **Cursor paging everywhere.** Every list takes `limit` (one default, 50) and `cursor`,
  and returns `next_cursor`. Cursors are bound to the catalog generation, so a stale
  cursor fails loudly instead of skipping rows. No offsets.
- **One parameter per concept, the same everywhere.** `register` and `scope` mean the
  same on every operation; `scope` is valid on every read because scope is reader state.
- **One result shape per operation.** The shape does not depend on result count. Every
  list is `{"items": [...], "next_cursor": ...}` inside `data`.
- **Errors** are `{code, class, message, remediation, fields}`, with a stable `code`
  catalog. HTTP maps each class to a status (usage 400/422, not found 404, ambiguous ref
  or no match 409, order blocked 422, catalog unavailable 503, internal 500); MCP
  returns the same document as a tool error.
- **Order manifests keep their bytes.** The order operation returns
  `{data: manifest, meta}`; the HTTP download route serves the exact manifest bytes,
  which is what the byte-identity contract and the SPA download use.
- **Few, coarse MCP tools.** Every tool's schema occupies agent context, so the tool set
  stays around ten: roughly search, show, states, values, lineage/graph, coverage,
  schema/diff, resolve, order and docs. Fine-grained HTTP routes for the SPA can map
  onto the same operations.
- **Catalog selection is configuration.** A server or MCP process serves one catalog,
  chosen at start (`--catalog NAME` or `--db DIR`). Users normally pick one catalog and
  keep it.

**Hosted MCP** *(settled, decision 12)*. `serve` exposes the remote MCP endpoint at
catalog.swecov.se, so agents need no install and no 1.2 GB download. Its rate limits are
sized for tool calls, separately from the SPA's. Private steward catalogs are never
served there; they stay on local `reg-meta mcp`.

Still open for the stage-1 API spec: the exact operation and tool names, and each
operation's parameter and result schemas.

## 8. Distribution

- **Server image:** one static binary plus the baked DBs, fetched in the Dockerfile with
  `curl`, SHA-256 verification and `zstd`.
- **Local binary** for macOS and Linux on each `reg_meta/v*` release, built with
  cargo-dist or a plain matrix workflow, with SHA-256 checksums. PyPI keeps
  `uv tool install reg-meta` working through maturin with `bindings = "bin"`. It is used
  only for `reg-meta mcp` and `reg-meta fetch`.
- **Windows** is a future target, not a build constraint now. Windows agents use the
  hosted MCP endpoint; a local Windows binary is added only if it builds and runs
  without special effort.
- **Agent plugin:** the `microdata-tools-se` plugin declares the MCP server: the hosted
  endpoint by default, the local binary as an alternative. The skill text documents the
  tools, not shell commands.
- **Release DB assets** gain a checksum file. `fetch` verifies before activation.
- **reg_meta_build** depends on `reg-core-py` as a workspace member built by maturin.
  `uv sync` builds it, which needs a Rust toolchain on the maintainer machine and in CI.

## 9. Verification

The conformance corpus is the acceptance gate, and HTTP is its single transport. Today
every runner calls Python in-process, so it must become implementation-neutral first:

- Add a runner seam: HTTP cases run against a server process started by a command
  template, so the same corpus runs against FastAPI today and `reg-meta serve` later.
  Servers are reused per artifact **and** configuration (search settings differ between
  cases).
- Process-boundary cases stay process-level: startup admission failure (a server that
  never listens), catalog selection and `fetch`.
- Rewrite the CLI argv cases (`cli_scope`, `selection`), the 42 `logical` cases and the
  5 `coverage` cases as HTTP request cases against the new API. Write them against the
  new API directly, not against today's surface.
- MCP is checked by an equivalence suite: for every operation, a representative success
  and each applicable domain error (including scope, generation and stale-cursor
  errors), the MCP tool call and the HTTP request return the same `data`, `meta` and
  error document. Transport-level parse errors are specified separately per transport.
- Order cases compare against committed `order.json` bytes, not against Python's own
  output.
- `fetch` cases use a local HTTP fixture server and a URL override
  (`REG_META_RELEASES_URL`) instead of patching `urlopen`.
- Fixtures: a script builds each synthetic (fixture, kind) artifact once through the
  real Python pipeline, cached by a hash of every transitive build input. Entries are
  immutable; a case that mutates an artifact copies it first. Rust tests and conformance
  read the same built files. Determinism makes caching safe.
- The tier-1 differential harness (section 4) maps new API results back to the current
  reader's results where the shapes differ; the mapping is part of the harness, not the
  product.
- Expected outputs change where the new API changes the surface. Those diffs are
  reviewed as content decisions, per the testing policy.

## 10. Staging

There are no users, so each surface switches over in one step, with no compatibility
layers. The derived tables ship additively (section 4), so the deployed webapp keeps
running on every intermediate release until stage 4 replaces it.

0. **Prove (days).** A throwaway Rust spike: open with schema gate plus `search` against
   the pinned base, served over HTTP and MCP. Measure search latency, check fold parity
   with a property corpus, and confirm maturin + PyO3 + uv workspace ergonomics. Adjust
   the plan if anything is worse than expected.
1. **Groundwork (packages below).** The operation table and error catalog, the fold spec
   in `reg-core`, the fixture cache, the out-of-process HTTP runner and the tier-1
   differential harness. Its PRs are gated by tier 0 and review only, because the tier-1
   harness is one of its deliverables. Ends at checkpoint 1.
2. **Derive framework.** `reg-meta-build derive` with its validator, the pattern for
   registering a derived table, the derived-artifact identity contract and the steward
   ordering (section 4), shipped as a schema 9.x minor. Its acceptance proves the tier-1
   harness on a trivial derived table; real tables arrive with the slices. The derived
   tables themselves are built in the stage-3 slice that reads them, so no table shape
   is designed before something consumes it.
3. **Rust operation slices.** Each slice is its `api` cases (written red from the
   approved operation table), its derived tables (a schema 9.x minor), then its Rust
   operations over HTTP and MCP, in one to three PRs: (a) admission + search, (b) show /
   states / values, (c) schema / diff / coverage / coded, (d) chains and graph, (e)
   order and project validation. Slice (a) runs alone: it creates `reg-catalog`, the
   `reg-meta` binary, the envelope, error, paging and MCP wiring, and `reg-core-py`,
   plus a deployment package (edge routing for `/mcp`, separate rate limits, the new
   server beside FastAPI, a public-host MCP smoke test). It ends at checkpoint 2. Then
   (b), (c) and (d) run in parallel; (e) follows (b). Slice (b) owns `state_projection`,
   `browse_delivery` and `resolver_column`, and switches `holdings_compile` to
   `resolver_column`. Slice (e) starts by porting the project types and structural
   validator from `reg_schema` into `reg-core`, keeping its raw-input, accumulated
   diagnostics. Docs, context, stats and `fetch` are assigned to slices by the surface
   inventory in 1.1. The new API runs beside FastAPI and is not yet used by the SPA. The
   build switches to `reg-core-py` for FQID, folds and hashing. Gated by tiers 0 and 1.
   The hosted MCP endpoint goes live once slice (a) passes.
4. **Cutover.** Starts at checkpoint 3. The SPA moves to the Rust server's API
   (regenerate types, adapt calls); the agent plugin moves to MCP; the Dockerfile and
   publish workflow drop the CLI. The modules the build still imports from `reg_meta`
   (`source_evidence`, `documentary`, `inventory`, `cli_common`) move into
   `reg_meta_build`; they cannot move earlier because the Python reader imports them
   too. Delete the FastAPI backend, the Python `reg_meta` package with its CLI, and
   `reg_schema`. Bump the schema major.
5. **SPA on WASM.** Replace `period.ts`, `validation.ts` and the hand-written
   `project_data.ts` with `reg-core` compiled to WASM plus generated types. Ends at
   checkpoint 4.

The build track (section 11) runs alongside, independent of these stages.

### Stage 1 packages

Each package follows the execution protocol (section 4). Order: 1.0 first; then 1.1, 1.2
and 1.3 in parallel; then 1.1b, 1.4 (after 1.1 and 1.3) and 1.5 (after 1.2).

**1.0 Land this tracker.** PR for `claude/reg-meta-rust-port-d3d5bc` (this file, the
governance exception, `spike/stage0/`), reviewed and merged. Every later package
branches from main.

**1.1 Operation table and error catalog.** Implements section 7.

- Changes: section 7 gains the operation table (operation, HTTP method and route, MCP
  tool, parameters, result shape, error codes) and the error catalog (code, class, HTTP
  status). The `api` case format is documented in `conformance/README.md`; cases for
  slice (a) (admission errors, search) go under `conformance/cases/api/`.
- Expected values may start from the current Python reader's output mapped to the new
  shape; each is reviewed as content.
- Paths: this file, `conformance/README.md`, `conformance/cases/api/`, one test module
  under `conformance/`.
- Out of scope: any runner or implementation.
- Acceptance: a tier-0 test checks that every `api` case parses, names an operation in
  the table and references an existing fixture.
- Ends at checkpoint 1, together with 1.2.

**1.1b Surface inventory.** Implements section 7, "No query CLI".

- Changes: a table in section 7 of every current HTTP route, CLI command and public
  library entry point, marked retained, replaced or removed, with its owning slice and
  acceptance case. Nothing the cutover needs may be unowned (docs, context, stats and
  `fetch` included).
- Paths: this file only.
- Acceptance: every route in `reg_webapp/backend/src/reg_webapp/routes/` and every
  `reg-meta` subcommand appears in the table.
- Ends at checkpoint 1.

**1.2 `reg-core` with the folds.** Implements the fold spec (section 5).

- Changes: the `crates/` workspace (edition 2024, resolver 3, workspace lints) and
  `crates/reg-core` with `fold_identity`, `fold_search`, the query normalizer and the
  FTS query builder, ported from the spike. `caseless` is replaced by a case-folding
  table generated from Unicode 17 `CaseFolding.txt`, with the generator and its output
  committed.
- Adds a fold golden corpus as data under `conformance/cases/folds/`. Characters whose
  properties differ between Unicode 16 and 17 are verified by hand, not generated from
  Python.
- Adds a CI job (`cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`) and a
  pre-commit `cargo fmt` hook.
- Paths: `crates/`, root `Cargo.toml` and `Cargo.lock`, `conformance/cases/folds/`,
  `.github/workflows/`, `.pre-commit-config.yaml`.
- Out of scope: Python bindings (slice 3a), the FQID and period grammars.
- Acceptance: `cargo test` passes the corpus and a sweep over every scalar value; CI is
  green.

**1.3 Fixture cache.** Implements section 9, "Fixtures".

- Changes: a builder that makes each synthetic (fixture, kind) artifact once through the
  real pipeline into a cache directory. The key hashes every transitive build input:
  fixture sources, the `reg_meta_build`, `reg_meta` and `reg_schema` sources (the
  fixture builder in `reg_meta/tests/reader_artifacts.py` imports from `reg_meta`),
  builder options and `uv.lock`. Entries are immutable; cases that mutate an artifact
  copy it. Conformance session fixtures read from the cache.
- Paths: `conformance/conftest.py`, `conformance/` helpers,
  `reg_meta/tests/reader_artifacts.py`, a cache builder script.
- Out of scope: changing any fixture or golden.
- Acceptance: `uv run python -m pytest conformance -q` twice, and the second run builds
  nothing; a cache hit is byte-identical to a fresh build; touching each input class
  invalidates its entries (tested); total conformance time does not regress.

**1.4 Out-of-process HTTP runner.** Implements section 9, "runner seam".

- Changes: conformance takes `--server-cmd` (a template with `{db}` and `{port}`),
  starts one server per cached fixture artifact and server configuration, and runs HTTP
  cases over a real socket. The `api` corpus is collected only with this option.
  Startup-failure cases stay process-level.
- Paths: `conformance/conftest.py`, `conformance/http_cases.py`,
  `conformance/README.md`.
- Out of scope: rewriting existing cases.
- Acceptance: the existing HTTP and validate cases pass with `--server-cmd` starting the
  FastAPI app under uvicorn; the default in-process run is unchanged.

**1.5 Tier-1 differential harness.** Implements section 4, tier 1.

- Changes: the harness under `conformance/differential/`. It pins the latest release's
  global and SWECOV artifacts (tag and SHA-256, recorded in section 4), fetched to a
  cache directory, and the baseline reader commit, installed in its own environment. Its
  query generator covers every register, seeded variable samples, holdings strata and
  the search-eval corpus terms, in both scopes. It is seeded from
  `spike/stage0/scripts/parity_fts.py`.
- Deletes `spike/stage0/`; the fold parity sweep lives on in 1.2.
- Paths: `conformance/differential/`, section 4 of this file, `spike/stage0/` (deleted).
- Out of scope: mappings for the new API's shapes, which each slice adds.
- Acceptance: a baseline-against-checkout run on unchanged main reports zero differences
  in under 5 minutes, and a deliberately perturbed checkout reader is reported.

### Stage 0 results (2026-10-07)

The spike lives in `spike/stage0/` (deleted by stage-1 package 1.5): a `core` crate with
the folds and the FTS query builder plus PyO3 bindings, and a `server` binary with run
modes `serve` (HTTP `/api/search` and MCP at `/mcp`) and `mcp` (stdio). Its one
operation is the variable full-text arm of today's search, in reference scope, with
today's SQL. All measurements are on the pinned 0.41.0 catalog, warm.

**Verdict: proceed.** Nothing was worse than expected; two findings change stage 1.

Correctness:

- **Search parity.** 338 queries (the search-eval corpus, 300 sampled variable-name
  words, edge cases): identical result order and FQIDs, and identical bm25 scores to
  1e-9, between Rust and Python running the same arm. The bundled SQLite (3.53.2) and
  Python's (3.53.4) agree.
- **Fold parity.** Every Unicode scalar value (1,112,064, bare and in four contexts) and
  1.09M distinct catalog strings, for `fold_identity`, `fold_search`, the query
  normalizer, the FTS query builder and the two character classes. Zero mismatches on
  the corpus. The sweep's residual mismatches are all characters that differ between
  Unicode 16 (Python) and 17 (Rust); none occur in the catalog.
- **Found and fixed:** Rust's `char::is_alphanumeric` admits `Other_Alphabetic` marks
  and symbols that Python's `[^\W_]` rejects, so the FTS query builder emitted terms
  Python drops. The fix is a general-category rule (§5).
- **Corrected assumption:** Python's `str.lower()` applies Final_Sigma, so
  `fold_identity` is plain `str::to_lowercase` (§5).

Performance (warm, in-process for Python):

  | Measurement                                           | Median | p95      |
  | ----------------------------------------------------- | ------ | -------- |
  | Rust HTTP `GET /api/search`                           | 2.0 ms | 39 ms    |
  | Rust MCP over HTTP, `tools/call search`               | 8.4 ms | 38 ms    |
  | Rust MCP over stdio, `tools/call search`              | 6.1 ms | 40 ms    |
  | Python, the same arm's SQL only                       | 0.4 ms | 19 ms    |
  | Python `search(field="description", type="variable")` | 17 ms  | 179 ms   |
  | Python `search()` default (all arms)                  | 485 ms | 1,408 ms |

- The arm's SQL costs the same in both languages. The gap to Python's `search()` is its
  Python post-processing and query count, which section 3 removes; Rust adds speed on
  top of that only by dropping per-row Python work.
- `reg-meta mcp` spawns and completes MCP initialize in 12 ms. `serve` used 57 MB RSS
  after the load test; the release binary is 6.8 MB.

Toolchain:

- Builds: cold release 51 s; incremental debug ~1 s; incremental release ~35 s (thin
  LTO, benchmarks only). Clippy and rustfmt clean. rusqlite (bundled, FTS5), axum and
  rmcp 3.5 worked without workarounds.
- PyO3 via maturin: `uv run --with ./spike/stage0/core` builds the extension with no
  manual maturin install (needs cargo on PATH). First build 12.5 s, rebuild after a Rust
  edit 4.6 s, 670 KB extension.
- **uv does not see Rust edits by default.** It caches path builds on `pyproject.toml`
  only; the package needs `[tool.uv] cache-keys` covering `Cargo.toml`, `src/**/*.rs`
  and `Cargo.lock`. "Editable" installs are full rebuilds for a Rust extension.
- Plain `cargo build --features python` does not link on macOS; build the extension
  through maturin and use cargo for check, clippy and tests.
- MCP interop papercut: the Python MCP SDK 2.3 logs "Session termination failed: 202"
  when rmcp answers the session DELETE with 202. Harmless; recheck at stage 3.

Stage-1 consequences:

1. Pin one Unicode version across `reg-core`: the latest, which Rust std (17.0) sets;
   replace or regenerate the case-folding table that `caseless` pins at 16.0. Python's
   older UCD only matters to the transitional differential harness, which tolerates
   characters unassigned there.
2. Give `reg-core-py` `cache-keys` from the start, and keep the parity sweep and the
   search-parity harness as the seed of the tier-1 harness.

## 11. The build: faster, incremental, still Python

The build stays Python. That was checked against measurements, not assumed.

**Where the build's time goes.** The 0.41.0 full strict build took 66 min on one core
(the host has 10 cores and 24 GB):

  | Phase                                  | Time   | Note                                             |
  | -------------------------------------- | ------ | ------------------------------------------------ |
  | Per-register resolve, 290 registers    | 41 min | Six SCB registers take 23 min; the largest 320 s |
  | Compile selected curation              | 19 min |                                                  |
  | Database materialization, dependencies | 3 min  |                                                  |
  | Load inputs, reference metadata        | 1 min  |                                                  |

A cProfile of a one-register subset build (`--registers 102`; 23 min wall, 8.6 GB peak
RSS) showed:

- **Algorithmic waste dominates.** `_partition_originals` asks for one native family,
  but `iter_native_families` scans and decodes the whole register and discards the other
  families in Python. 1,912 calls cost 423 s. Elsewhere: 8.4M SQLite `execute` calls
  (159 s), `record_ref` called 52M times for 7,943 records, and `coding_source_sha256`
  114 s over 6,092 calls.
- **Python object overhead is the second layer.** 3.2 billion function calls in total.
  Pydantic `validate_python` runs 68M times (186 s cumulative), `model_construct` 8.4M
  times, JSON encoding 72 s, and small key helpers 50–160M times each.
- **Out-of-slice work.** A one-register build spent 15.5 min on deferred naming for
  registers outside the slice.

**Why not Polars.** None of the hot spots is a column-shaped computation. They are
per-record rule logic with source locators, located errors, preserved order and
multiplicity, and byte-identical output. Polars would add a third data model next to the
Pydantic IR and SQLite, convert at every boundary, and needs `maintain_order` discipline
everywhere to stay deterministic. The parts that are column-shaped (value binding,
coverage, the derived tables) are better written as SQL in the SQLite stores the build
already uses.

**Why not Rust yet.** Rust would remove the second layer, likely by a large factor, and
cut memory. It would also make the stack one language and remove the bindings layer. But
the build is 71k lines and the most frequently changed code in the repo: rule and
curation tickets land weekly and the prepared schema has passed version 15. A rewrite
must reproduce byte-identical artifacts and the same diagnostic baseline while both are
moving. The first layer is cheap to fix in Python.

**Order of build work:**

1. Fix the algorithmic waste: the family scan, N+1 queries, repeated `record_ref` and
   hashing. Byte-identical, verified with dbdiff. The family scan, repeated `record_ref`
   and guarded-claim hashing were fixed in #1181.
2. Run per-register resolve in parallel processes (decision 7). `resolve_source_scope`
   is close to a pure map over one register's records plus read-only shared context. Its
   results are merged in a fixed order, so output stays deterministic. The shared
   accumulators (`matched_labels`, `duplicate_overrides`, the coding callback) become
   per-scope outputs merged afterwards. The floor is the largest register (~5 min).
3. Cache per-register resolve results by content: the register's prepared-record digest,
   the curation entries that reach it, and a hash of the build code. A curation-only fix
   then re-resolves only the registers it affects. Determinism makes the cache safe; a
   periodic uncached build, byte-compared with the cached one, catches a key that misses
   an input.
4. Keep Pydantic validation at trust boundaries only (prepared-input reads, curation
   TOML). Internal hot-path objects already validated upstream do not revalidate.
5. Re-profile. If Python object overhead still dominates, move specific kernels into
   `reg-core` through the same bindings the build already uses for grammars.
6. Reconsider a Rust build only after the runtime port, once the curation rules
   stabilize. Then do it stage by stage behind the same artifact-equality gate, never as
   one rewrite.

## 12. Risks

- **Maintenance surface.** Two languages instead of one (Python for the build, Rust for
  the runtime). Rust build times and toolchain pinning in CI.
- **Self-validating oracle.** If tier 1 compared against the checkout's own reader, a
  regression moved into derive would validate itself. Mitigated by the pinned,
  separately installed baseline reader and recorded semantic exceptions.
- **Fold parity.** The main source of silent drift. Mitigated by computing folded
  columns through the same code via bindings, and a property corpus.
- **Native extension in the build.** The build needs a compiled `reg-core-py`. That is
  only for the maintainer and CI, but it is a new prerequisite.
- **Derive speed.** Derive is fast only while it stays set-based or parallel. The tier-1
  budget is the early warning.
- **Cache keys in the incremental build.** A key that misses an input yields a stale,
  wrong artifact. Mitigated by the periodic uncached byte-compare.
- **Frontend churn in stages 4–5.** Type regeneration will move many generated names.

## 13. Decisions (2026-10-07)

  | #   | Question                               | Decision                                                                                                                                                                                                                                                                                                   |
  | --- | -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | 1   | Where are catalog facts resolved? (§3) | **Compiled in the build**, in the derive step (§4). Reverses "No state/window resolution is compiled" in `reg_meta/DESIGN.md`.                                                                                                                                                                             |
  | 2   | What serves the webapp API? (§6)       | **Rust server** (`reg-meta serve`). The FastAPI backend is deleted in stage 4.                                                                                                                                                                                                                             |
  | 3   | `reg_schema`? (§5)                     | **Merged into `reg-core`.** The Python package is deleted in stage 4.                                                                                                                                                                                                                                      |
  | 4   | CLI v4 surface                         | **Superseded by decision 11.** Its settled rules carry over to the API (§7).                                                                                                                                                                                                                               |
  | 5   | MCP server mode                        | **Yes. Now the primary agent interface** (decision 11), built with each operation slice in stage 3.                                                                                                                                                                                                        |
  | 6   | WASM in the SPA                        | **Yes, as the last stage** (stage 5).                                                                                                                                                                                                                                                                      |
  | 7   | Parallel per-register resolve (§11)    | **Yes**, after the family-scan fix (landed in #1181).                                                                                                                                                                                                                                                      |
  | 8   | Where this plan lives                  | **Its own root tracker**, with the governance rule amended to allow one tracker per concurrent refactor.                                                                                                                                                                                                   |
  | 9   | How to avoid full rebuilds per step    | **Base/derive split, pinned artifacts, three tiers with budgets, incremental base build** (§4, §11).                                                                                                                                                                                                       |
  | 10  | Who the runtime is designed for        | **Agents and the webapp only.** No human-oriented features (text output, prompts, progress, notebook import) while building; re-evaluated after stage 5 (§7).                                                                                                                                              |
  | 11  | Query CLI?                             | **None.** One operation set exposed over HTTP and MCP; the binary has run modes only (`serve`, `mcp`, `fetch`) (§6, §7).                                                                                                                                                                                   |
  | 12  | Where agents reach MCP                 | **Hosted and local.** Remote MCP endpoint on `serve` at catalog.swecov.se; `reg-meta mcp` over stdio for offline use and private steward catalogs (§7).                                                                                                                                                    |
  | 13  | Tooling and versions                   | **Latest everywhere.** Newest stable versions of languages, crates, packages and SDKs, and modern methods; no compatibility work for older toolchains. Windows later, via hosted MCP unless a local binary is effortless (§8).                                                                             |
  | 14  | How agents execute it                  | **Execution protocol (§4).** Work packages in this file, written per stage; mechanical done (acceptance + tiers + fresh-agent review); escalate-don't-decide list; small squash PRs to main; ≤3 in flight; four maintainer checkpoints. Stage 2 builds only the derive framework; slices own their tables. |
