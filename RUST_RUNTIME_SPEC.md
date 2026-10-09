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
  frontend's hand-written `period.ts` (1,173 lines) and `validation.ts`. A parity test
  keeps two of them aligned.
- **Four text folds.** Python `str.lower` (`py_lower`), `str.casefold`,
  NFKD-strip-casefold, and SQLite's ASCII-only `NOCASE`/`LOWER`, mixed without a stated
  rule.

Smaller defects (not blocking, listed so they are not ported):

- Row-order-dependent results: the `same_as` BFS and the classification editions year
  map read rows without `ORDER BY`. The BFS is also unreachable: the writer requires
  both endpoints of every `same_as` edge to be live (`resolved_metadata.py`), so a
  direct lookup never misses on a `same_as` source (3d.1).
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
  | `_expand_state_windows`, alias-window participation rules                                                                | A + C | `expanded_state` relation (see below); request fallback stays           |
  | `register_variable_deliveries`, `_fuse_windows`, coverage                                                                | A     | `browse_delivery`, `delivery_window` and `resolver_column` (see below)  |
  | `_fuse_provider_held_deliveries`, `scope_predicate` string splicing                                                      | A     | Per-scope rows (`scope = 'holdings'`) in steward artifacts              |
  | `_state_warning_ids` (one `data_warnings` call per state)                                                                | C     | One indexed `data_warning` read per variable over `canonical_column`    |
  | Classification/variable chains, terminal successors, editions, families                                                  | A     | `succession_terminal`, `classification_chain`, `classification_family`  |
  | `same_as` BFS                                                                                                            | A     | none: unreachable, not ported (3d.1)                                    |
  | Concept-group tag N+1, group member assembly                                                                             | A     | `concept_group_tag`, pre-ordered member rows                            |
  | `get coded-variables` (72 s; 5.5 s unfiltered since #1175)                                                               | A     | `coded_variable_stats` per scope; reader only orders and pages the rows |
  | `variable_search_text` view (correlated `group_concat` for 43k variables)                                                | A     | Materialized table, same name and columns (#1305, schema 9.7)           |
  | Search arm merge, scoring, cursor, fold decisions; period intersection with a request; `get diff`; order materialization | C     | Stays in the reader                                                     |

Consequences:

- **The resolver gets one home, in the build** (the derive step, section 4). The
  alias-window, replacement and coding rules are implemented once, in Python, where
  validation can check them. The reader keeps only two small request-dependent rules
  over compiled rows: the window fallback and warning clipping (below). This is the
  largest reduction in port risk: the hardest semantics never need a second
  implementation.
- **Window participation is only partly compiled.** Which windows a state has, and their
  representation metadata, are artifact facts. Whether a window *replaces* its base
  state depends on the requested period (`_applicable_alias_windows` in `catalog.py`;
  pinned by `test_gap_year_month_falls_back_to_annual_state`). The rule: windows replace
  the base state only when some source (non-curated) window is spelled like the base
  column and some source window overlaps the request (two separate tests in the code);
  curated windows are always additive; a period no source window overlaps falls back to
  the base state. That rule stays in the reader (class C). Gap, partial-overlap and
  spanning cases are pinned as `api` cases before the moved code is reduced.
- **Projected states are their own relation.** `variable_state.state_id` is a primary
  key, and a year's month windows share their annual state's `state_id`, so projected
  states cannot be rows of `variable_state` without changing its grain under the Python
  reader. `expanded_state` has its own identity and references the source state; lineage
  keeps the source state's identity.
- **Warning attribution is C over compiled `canonical_column`** (corrected by 3b.1,
  stage 3b–3e decision 2). Warning ids are computed per expanded window today, then
  again after clipping to held and requested periods (`_state_warning_ids` calls in
  `catalog.py`; oracle `conformance/cases/api/states-held`). The attribution reduces to
  a plain predicate over `data_warning` and each emitted representation's clipped bounds
  and `canonical_column` (written out in `reg_meta/DESIGN.md`, "Compiled states and
  browse deliveries"; it matched the reader on all 2.28M links of the pinned global
  artifact). A compiled `state_warning` link table measured 2.28M rows keyed by
  64-character ids (about 160 MB), while one variable's warnings read in about 1 ms at
  worst (420 warnings over 405 states), so the reader evaluates the predicate per
  request. A join through the source state alone would leak or drop warnings.
- **Browse and resolver eligibility are different contracts.** Browse deliberately keeps
  alias windows that no state contains (`register_variable_deliveries`); holdings
  accepts only resolver-emitted columns (`holdings_compile.py`). `browse_delivery`
  serves browse; `resolver_column` is the only relation that authorizes a holdings
  mapping. It is the whole-history reference universe of resolver-emitted columns, not
  applicability to one holding edition. Each has its own invariants.
- **The compiler stops importing the reader.** `holdings_compile` canonicalizes against
  the derived `resolver_column` relation.
- **Scope becomes data.** Only two scopes exist (`reference`, `holdings`), and holdings
  exists only in steward artifacts. Precomputing per-scope rows replaces the
  string-spliced predicates in ~25 queries.
- **Reader size.** Of ~10.9k lines in `catalog.py` + `queries.py`, an estimated 35–40%
  of lines and 10–15% of the algorithmic logic remain. The remainder is mostly SQL plus
  sort/merge.
- **Artifact size.** Roughly +70–100 MB (6–8%) before `expanded_state`. Measured by 3b.1
  on the pinned v0.42.0 copies: `expanded_state` (481k rows, 53 MB with its index),
  `browse_delivery` and `delivery_window` (82k and 157k rows, 12 MB) add 65.5 MB (5.1%)
  to the global copy and 66.9 MB to SWECOV; derive takes 51 s and 54 s including
  validation. Keep `expanded_state` a narrow relation referencing base states, not a
  copy of their content (+200 MB). Do not precompute code owners (up to 3.9M rows).
- **New invariants `validate_built_db` must own** (one validator, extended; not a second
  one): browse delivery windows disjoint; `resolver_column` equal to the resolver's
  emitted columns; one canonical spelling per (variable, variant, fold); chain/terminal
  tables acyclic and consistent with `*_replaced_by` at the manifest year; held-\*
  tables equal to holdings facts; aggregate tables equal to a recomputation; fixed
  insertion order for byte-identical output.
- **Lost test seam.** The `classification_as_of_year` override goes away. Tests build
  artifacts with a different manifest year instead. The build's own validator also uses
  it (`classification_succession_as_of_year` in `reg_meta_build/validate.py`), so its
  replacement lands with the chains slice (3d).

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
  deterministic, and its checks join `validate_built_db`. Steward artifacts interleave
  it with `extend-db`, because holdings must see the steward's own metadata
  (`extend_db.py` inserts the steward core graph before compiling holdings): insert the
  steward core (graph, slugs and warnings) → derive the reference tables over base plus
  overlay → compile holdings → derive the holdings tables.

**The derived artifact has its own identity contract** (stage 2). Base identity,
derivation identity (derive code and schema minor) and the admitted schema versions all
feed the served generation. Changed derivation inputs change the generation, so no
cursor outlives them; identical inputs give an identical generation and identical bytes.
Derive reads an immutable base, publishes atomically, leaves base bytes unchanged and
rejects a stale derivation. Since package 2.2, `derive` admits a base of the same major
and a minor up to the builder's (`open_built_db(older_minor=True)` in `db.py`); every
other builder input, `extend-db`'s base included, stays exact, so an older base is
derived before it is extended. A derived generation hashes `derived_from_generation_id`
(`artifact_identity.py`), which built artifacts lack, so their ids are unchanged.

**Bootstrap derive by calling the reader, not rewriting it.** The first implementation
of each derived table calls today's reader functions (`Catalog.states`, the delivery
fusing, the chain walks) in place: `derive` imports `reg_meta.catalog.Catalog`, and the
functions move into `reg_meta_build` in stage 4 (corrected 2026-10-08; earlier text said
they were moved). Its output then equals what the current reader returns, by
construction, and the risky resolver logic needs no second implementation. Where the
frozen reader is wrong (stage 3b–3e decision 6), derive gets the correct logic instead.
Measured cost: `Catalog.states` over a 300-variable sample takes 6 ms per variable, so
all 54k variables take ~5 min on one core, about 1 min in parallel. Replace a moved
function with set-based SQL only when the G1 budget below requires it.

**Ship derived tables additively.** The schema gate accepts a database whose minor
version is at least the code's. Derived tables land as schema 9.x minor bumps, so the
current Python reader and webapp keep working on every intermediate release. There is no
big-bang artifact cutover; the major bump comes only when the Python reader is deleted.

**Develop against pinned artifacts.** Refactor work uses a pinned release (recorded by
package 1.5) plus the synthetic conformance fixtures. Curation work keeps changing the
base; the refactor changes derive and the reader. Neither waits for the other. A
**re-pin package** moves the pin to a newer release, and the baseline reader commit with
it when the new release needs a newer reader: it runs G1 on the old pin (with its
baseline) and the new pin (with its baseline) and records any difference. Re-pin at
least at every maintainer checkpoint.

**Current pin** (package 4.1; the data lives in `conformance/differential/config.toml`):

- Release `reg_meta/v0.45.0` (schema 9.7.0, docs schema 1.3.0). Asset SHA-256s: global
  `reg_meta.db.zst` `7424797d7b94b15f9d28eb323fa6f0811145d8b6220228ad3ea32edde5cce111`;
  SWECOV `reg_meta_swecov.db.zst`
  `ff68bc7dceb0bb024f2d5b5c90870de6af1b25cfe2c167100ac149fd46c7870d`; docs
  `reg_meta_docs.db.zst`
  `85a1a6c883fca2ded5203c60c33d8657a438344c39ab088c0b01299a47721f57` (read by `search`
  and `docs`).
- Baseline: the release tag's own code, from a detached worktree of the tag: its
  `reg-meta` binary (`cargo build --release`); 4.9a deleted the Python CLI arms and the
  fold sweep. It reads the release originals directly. Both arms run the same server, so
  requests go to both unchanged and responses compare as raw bytes; `derived-generation`
  and `reader-version` are the only exceptions.

**Three verification gates, with budgets.** They are named G0–G2 so they are not
confused with the test tiers 1–3 in `ARCHITECTURE.md`.

  | Gate | What runs                                                                                                                                                                                                                                                                   | Budget                                                 | When                                                                                                                                                                                                                   |
  | ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | G0   | `uv run python -m pytest conformance <touched packages> -n auto -q`, `cargo test --workspace` and, from slice 3a, the Rust HTTP run (section 10), all on synthetic artifacts. Conformance alone took 23 s serially (299 test items, 2026-10-07).                            | under 60 s, plus 30 s for the Rust HTTP run            | every change                                                                                                                                                                                                           |
  | G1   | Derive on the pinned real artifacts, then the differential harness: the pinned release's reader (from 4.1 its Rust server) on the originals against the checkout's derived tables and server, on both artifact kinds. Runs locally from a shared artifact cache, not in CI. | under 5 min warm; a cold run is reported, not budgeted | every PR touching derive or the docs build, and once per slice before its cutover (stage 3b–3e decision 7; earlier: every PR touching derive or the reader); after package 4.11, at releases only (stage 4 decision 4) |
  | G2   | Full base build plus derive and G1 on the result.                                                                                                                                                                                                                           | ~1 h today                                             | checkpoints and releases, never per PR                                                                                                                                                                                 |

The G1 baseline is the reader **at the pinned release's tag, built in its own
environment** (the Python reader until stage 3; from package 4.1 the tag's `reg-meta`
binary), never the checkout under change, so a regression moved into derive cannot
validate itself. It reads the pinned release originals, never a copy derived by the code
under change; the implementations under test read derived copies of them. It runs on
both artifact kinds (the global catalog and the SWECOV steward artifact) and both
scopes, over generated queries (every register, seeded variable samples,
holdings-specific strata, the search-eval corpus terms). Each accepted difference is
recorded as a named semantic exception in the harness. The Python runtime is frozen from
stage 3b (decision 6 of the stage 3b–3e decisions): a Rust-only fix that diverges from
the baseline gets a narrow exception linked to the `api` regression case that pins it,
and keeps it until stage 4.

A slow gate is a defect to fix, not a reason to skip the gate. If derive exceeds its
budget, make the slow table set-based before adding more tables.

**Work in vertical slices.** Each slice adds derived tables, the Rust operations that
read them, their conformance and differential cases, and the SPA's switch to them, and
is verified at G0 and G1 only. Slices share the pinned artifacts, so independent slices
can be built in parallel by separate agents. Slice order is set by risk: admission +
search first, order last.

**Execution protocol** *(decision 14)*. Agents build the refactor, one work package per
PR. These rules keep them on target between maintainer checkpoints without a human in
the loop:

- **Packages live in this file** (section 10), so every worktree sees the same plan. A
  package names the sections it implements, the paths it may change, what is out of
  scope, its acceptance commands and case IDs, and what it depends on. A package that
  does not fit in about ten lines is split. A stage's packages are written when the
  stage starts, against the merged state, never further ahead.
- **Done is mechanical.** A PR is done when its acceptance commands pass, G0 (and, from
  stage 2, G1 where the package requires it; G1 cadence above) is green within budget,
  and a fresh agent that did not write it has reviewed it. The review is a PR review
  that lists each acceptance command it reran with its result, and checks the diff
  against the package's paths and out-of-scope list. The implementing agent never
  declares its own work done.
- **Build only what is needed.** A package builds what its stated behavior requires and
  nothing more: no speculative options, retries for unlikely races, extra layers, or
  tests beyond about one per stated behavior. Load-bearing guards (CLAUDE.md) and
  regression cases are never extra, and an existing capability is extended before
  anything new is added. Every PR ends with a simplification pass over its own diff,
  listed in a "Simplification" section of the PR body. Reviewers report defects in the
  stated behavior, rule violations and simplifications; an idea that adds scope goes in
  one line under "Not requested", and the orchestrating session declines it unless it
  fixes a defect.
- **Tests are end-to-end or integration by default, at a public boundary**: the built
  artifact, HTTP, MCP, order bytes, `project_data.json` validation. Unit tests are the
  exception: one is allowed only where it pins behavior a boundary test cannot reach
  well, never an implementation shape that makes the code harder to change. One test per
  guarantee, at its hardest case, with expected values from outside the code under test
  and a comment naming the change that would make it fail. A bug fix extends its
  guarantee's test rather than adding one. G0 stays fast enough to run on every change.
- **Escalate, don't decide.** An agent stops instead of changing any of: the operation
  table or error catalog (`conformance/api/`), a decision in section 13, the meaning of
  an existing golden expected file, the schema major version, a gate budget, or a new
  runtime dependency that this file does not name. It records the question as a PR
  comment, marks the package blocked, and the orchestrating session takes it to the
  maintainer. A golden change that only converts a shape as the approved operation table
  prescribes is not an escalation; it is reviewed like any diff. Everything else the
  agent decides, and records in the PR.
- **Tracker corrections are not escalations.** A PR that fixes a verifiable code fact in
  this file (a wrong name, count or path) goes through like any other. A correction that
  changes a decision, a package's scope or its acceptance is an escalation.
- **Who merges.** The orchestrating session squash-merges a PR once CI is green and the
  review passes, and keeps the in-flight list. Packages do not edit this file except
  where their package says so.
- **Small PRs, squash-merged to main.** Everything before stage 4 is additive (schema
  9.x minor bumps) except the slice cutovers and 3a.9's switch of production to the Rust
  server alone; main stays deployable and there is no long-lived integration branch. The
  one exception is stage 4's schema-major package 4.10, whose PRs collect on one
  integration branch (stage 4 decision 2).
- **At most three packages in flight.** From stage 3b: at most three concurrent slice
  sessions, one package each, with the orchestrator the only merger (stage 3b–3e
  decision 9). Files every slice would edit (route and tool registration, the
  derived-table list, conformance indexes) are split into one file per slice. Schema
  minor bumps merge one at a time.
- **G2 never runs inside a package.** The maintainer runs it at checkpoints, in the
  background.
- **Four maintainer checkpoints:** (1) end of stage 1: approve the surface inventory,
  operation table, error catalog and fold spec, and decide whether indexed text is
  pre-folded (section 5) — passed 2026-10-07; (2) after slice 3a: approve the pattern
  the other slices copy; (3) before stage 4: go or no-go on retiring the Python runtime
  — passed 2026-10-09 (go; "Stage 4 decisions"); (4) stage 5 done, and this file
  deleted.

## 5. Principle 2: one implementation per contract

Each contract that crosses a language or package boundary has exactly one
implementation:

  | Contract                                | Single home                                       | Reached from                                          |
  | --------------------------------------- | ------------------------------------------------- | ----------------------------------------------------- |
  | Artifact schema (DDL, manifest)         | `reg_meta_build` (Python)                         | Rust reader reads it; `SCHEMA_VERSION` gate           |
  | Resolver semantics                      | `reg_meta_build` derive (Python)                  | Compiled into the artifact                            |
  | FQID and period grammar                 | Rust core crate                                   | Build via Python bindings; SPA via WASM               |
  | Text folds                              | Rust core crate                                   | Build via Python bindings (fills `*_folded` columns)  |
  | Canonical JSON + SHA-256 (build hashes) | `reg_meta_build` (Python; package 4.4 assumption) | Build only; no Rust consumer recomputes a build hash  |
  | Project schema + structural validator   | Rust core crate (replaces reg_schema)             | Server, MCP; SPA via WASM; JSON Schema export         |
  | Result types                            | Rust structs (serde + utoipa)                     | OpenAPI; MCP tool schemas and TS types derive from it |

Notes:

- **Text folds get a written spec.** Two named folds:
  - `fold_identity` — Unicode lowercase with the Final_Sigma context rule, no
    normalization. This is today's `py_lower` (Python `str.lower()`, which does apply
    Final_Sigma) and the column identity rule. In Rust it is `str::to_lowercase`.
  - `fold_search` — full case folding, NFKD, drop characters with a nonzero canonical
    combining class, repeated until the text stops changing; then split on whitespace
    and join with single spaces. It is idempotent. Each pass maps characters
    independently, so a pass bound that holds for every scalar holds for every string:
    two passes change any text at most and the third confirms; reaching the cap is a
    bug. The fold before package 1.2 (one pass, strip first) was not idempotent on 712
    scalars: NFKD re-introduced capitals after the case fold (U+1D2C MODIFIER LETTER
    CAPITAL A → `A`), and a spacing mark decomposed to a space after the strip (U+00A8
    `¨` → ` `). This is `reg_meta.queries.fold_search`, the reader's search-text fold,
    and is *not* what FTS5 `unicode61` does to indexed text: `unicode61` folds neither ß
    nor ligatures nor full-width forms (`straße`/`strasse`, `ﬁlm`/`film`, `ＡＢＣ`/`abc`
    match under `fold_search` but not under `unicode61`). Catalog indexed text is
    pre-folded with `fold_search` *(decision 16)*, so one fold definition serves the
    build and the reader; slice 3a applies it, and `doc_fts` folds with the docs slice
    (checkpoint 2).
  - Helper classes follow Python, not Rust's std: whitespace is `White_Space` plus
    U+001C..U+001F; "alphanumeric" (`[^\W_]`) is general category L\* or N\*, not Rust's
    `Alphabetic` property.

  The build computes folded columns with the same code the reader uses for the query
  string, through the bindings. Pin one Unicode version for all of `reg-core` (stage 0
  found Rust std and `unicode-normalization` on 17.0, `caseless` on 16.0, Python on
  16.0). The stage-0 parity sweep becomes a G1 check (Python against Rust) from slice
  3a; `reg-core`'s own tests carry a sampled corpus and property checks (package 1.2).

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
  Includes search (one ranking, including the webapp's best-bets scoring; its golden
  pins become curated build input in slice 3a), order materialization and project
  semantic validation.
- `reg-meta` — the binary, with run modes only:
  - `serve` — the HTTP API for the SPA (axum + utoipa; ETag, body-size limit and rate
    limit as tower layers) and a remote MCP endpoint over streamable HTTP for hosted
    agents.
  - `mcp` — MCP over stdio against a local catalog, for offline use and private steward
    catalogs.
- `reg-core-py` — the PyO3 module the build imports.

Runtime crates are the ones this file names, plus tokio and sha2 (implied by axum/rmcp
and the SHA-256 contract; ratified at checkpoint 2).

What is deleted: the Python `reg_meta` package including its CLI, `reg_schema`, the
FastAPI backend, and the frontend's hand-written grammar and validation mirrors. The
build imports ten `reg_meta` modules today (`source_evidence`, `fqid`, `errors`,
`inventory`, `db`, `catalog`, `documentary`, `doc_db`, `queries`, `cli_common`); the
surface inventory (package 1.1a) gives each a disposition: into `reg_meta_build`, or
replaced by `reg-core-py`. The moves happen in stage 4.

Why the HTTP server moves to Rust rather than FastAPI calling Rust through bindings:

- With bindings, every result type exists twice: as a Rust struct and as a Pydantic
  model for OpenAPI. That is the drift this design removes.
- The backend is ~6k lines. About 3k of them are catalog logic (node assembly, search
  ranking, semantic validation) that belongs in the operation set anyway, so agents get
  it too. The rest is HTTP glue that axum and tower handle directly.
- The deploy image becomes one static binary plus the baked DBs. Keep the 1 GB Fly VM
  for the page cache.
- Local MCP needs no Python environment: agents install one binary. The server, the MCP
  tools and the SPA (through WASM) share one project validator and one grammar.

Performance is not the reason: section 1 shows query cost is a design problem that
derive fixes in either language. The alternative considered was `reg-core` alone
(bindings plus WASM) with the server staying Python over derived tables. It was rejected
because it keeps two definitions of every result type and a Python install for local
MCP.

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

- **JSON only, `{data, meta}` on every successful response**, except raw-bytes downloads
  (document PDFs, the order manifest file), which carry their media type and have a JSON
  operation for their metadata. `data` is the result; `meta` holds the contract version,
  catalog generation and scope. Agents always know which catalog answered.
- **Deterministic responses.** `meta` carries no timing, so the same request on the same
  catalog returns the same bytes and goldens compare raw output. Performance is measured
  by the G1 harness.
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
- **Cursor paging for open-ended lists.** Every such list takes `limit` (one default,
  50) and `cursor`, and returns `next_cursor`. A list bounded by the request or the
      entity (a register's documents, a variable's variants) is a plain array. Cursors
      are bound to the catalog generation, so a stale cursor fails loudly instead of
      skipping rows. No offsets.
- **One parameter per concept, the same everywhere.** `register` and `scope` mean the
  same on every operation; `scope` is valid on every read because scope is reader state.
- **One result shape per operation.** The shape does not depend on result count. Every
  paged list is `{"items": [...], "next_cursor": ...}` inside `data`.
- **Errors** are `{error, meta}`, where `error` is
  `{code, class, message, remediation, fields}` with a stable `code` catalog.
  `conformance/api/errors.toml` maps each code to its class and HTTP status; MCP returns
  the same document as a tool error.
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

The operation and tool names, parameters, result shapes and routes are in
`conformance/api/operations.toml`; the error codes with their classes and statuses are
in `conformance/api/errors.toml` (package 1.1).

## 8. Distribution

- **Server image:** one static binary plus the baked DBs, fetched in the Dockerfile with
  `curl`, SHA-256 verification and `zstd`.
- **Local binary** for macOS arm64 and Linux x86_64 on each `reg_meta/v*` release, as
  GitHub release assets with SHA-256 checksums (package 4.12). It is used only for
  `reg-meta mcp`; the catalog is downloaded with `curl` and `zstd`. There is no PyPI
  package and no `fetch` run mode (stage 4 decision 3). Hosted MCP is the default.
- **Windows** is a future target, not a build constraint now. Windows agents use the
  hosted MCP endpoint; a local Windows binary is added only if it builds and runs
  without special effort.
- **Agent plugin:** the `microdata-tools-se` plugin declares the MCP server: the hosted
  endpoint by default, the local binary as an alternative. The skill text documents the
  tools, not shell commands.
- **Release DB assets** are verified against the SHA-256 digests GitHub records for each
  asset (the Dockerfile bake, package 4.5).
- **reg_meta_build** depends on `reg-core-py` as a workspace member built by maturin.
  `uv sync` builds it, which needs a Rust toolchain on the maintainer machine and in CI.

## 9. Verification

The conformance corpus is the acceptance gate, and HTTP is its single transport. Today
every runner calls Python in-process, so it must become implementation-neutral first:

- Add a runner seam: HTTP cases run against a server process started by a command
  template, so the same corpus runs against FastAPI today and `reg-meta serve` later.
  Servers are reused per artifact. Search pins are build input (package 3a.2), so every
  HTTP case can run out of process.
- Process-boundary cases stay process-level: startup admission failure (a server that
  never listens) and catalog selection.
- Rewrite the CLI argv cases (`cli_scope`), the 30 `logical` cases (corrected
  2026-10-08; earlier text said 49) and the 5 `coverage` cases as HTTP request cases
  against the new API. Write them against the new API directly, not against today's
  surface.
- MCP is checked by an equivalence suite: for every operation, a representative success
  and each applicable domain error (including scope, generation and stale-cursor
  errors), the MCP tool call and the HTTP request return the same `data`, `meta` and
  error document. Transport-level parse errors are specified separately per transport.
  Raw-bytes downloads are HTTP-only: their cases compare exact bytes, media type and the
  required headers, and agreement with the JSON metadata operation. The current runner
  decodes non-JSON bodies as text and drops headers (`http_cases.py`), so 1.4 extends
  it.
- Order cases compare against committed `order.json` bytes, not against Python's own
  output.
- Fixtures: a script builds each synthetic (fixture, kind) artifact once through the
  real Python pipeline, cached by a hash of every transitive build input. Entries are
  immutable; a case that mutates an artifact copies it first. Rust tests and conformance
  read the same built files. Determinism makes caching safe.
- The G1 differential harness (section 4) mapped new API results back to the pinned
  baseline reader's results where the shapes differed, until package 4.1 made the
  baseline the pinned release's Rust server, whose responses compare as raw bytes.
- Expected outputs change where the new API changes the surface. Those diffs are
  reviewed as content decisions, per the testing policy.

## 10. Staging

There are no users, so each surface switches over in one step, with no compatibility
layers. The derived tables ship additively (section 4). From package 3a.9 production
runs the Rust server alone; a FastAPI path is unavailable in production until the slice
that ports it ships *(decision 15, checkpoint 2)*.

0. **Prove (days).** A throwaway Rust spike: open with schema gate plus `search` against
   the pinned base, served over HTTP and MCP. Measure search latency, check fold parity
   with a property corpus, and confirm maturin + PyO3 + uv workspace ergonomics. Adjust
   the plan if anything is worse than expected.
1. **Groundwork (shipped 2026-10-07).** The surface inventory, operation table and error
   catalog (`conformance/api/`), `reg-core` with the folds, the fixture cache, the
   out-of-process HTTP runner and the G1 differential harness. Checkpoint 1 passed.
2. **Derive framework (shipped 2026-10-07).** The derive step with its checks in
   `validate_built_db`, the steward ordering, `resolver_column` (read by
   `holdings_compile`) and the standalone `reg-meta-build derive` with the
   derived-artifact identity contract (section 4), as builder schema 9.1.0; G1 runs on
   derived copies.
3. **Rust operation slices.** Each slice is its `api` cases (written red from the
   approved operation table), its derived tables (a schema 9.x minor), its Rust
   operations over HTTP and MCP, and finally the SPA's switch: regenerate the SPA types
   for its routes and delete the replaced FastAPI routes. One to four PRs per slice (3b
   has seven, sharing the catalog cutover C with 3d; see "Stage 3b–3e packages").
   - **(a) admission + search** runs alone. It creates `reg-catalog`, the `reg-meta`
     binary, the envelope, error, paging and MCP wiring, and `reg-core-py`, with a G0
     test that the generated OpenAPI and MCP `tools/list` match
     `conformance/api/operations.toml`; turns the webapp's search pins into curated
     build input (best-bets is ranking code and ports into `reg-catalog`); a pin that
     does not resolve becomes a build error (today it is a runtime 500:
     `http_search/golden-classification` and `golden-stale-register` expect one);
     applies the checkpoint-1 indexing decision; and has a deployment package (the Rust
     server alone in the image, edge routing for `/mcp`, separate MCP rate limits, a
     public-host MCP smoke test). The exhaustive Python-against-Rust fold sweep joins G1
     here. Hosted MCP goes live when (a) passes. Ends at checkpoint 2.
   - **(b) show / states / values** starts with one PR for the `expanded_state`,
     `browse_delivery` schema, derivation and validator checks (`state_warning` was
     added 2026-10-08 and dropped by 3b.1 under decision 2). That PR merges before
     anything in (c) consumes expanded states (schema, diff and held coverage all do).
   - Then the rest of (b), **(c) schema / diff / coverage / coded** and **(d) chains and
     graph** run in parallel. **(e) order and project validation** follows (b). It
     starts by porting the project types and structural validator from `reg_schema` into
     `reg-core`, keeping its raw-input, accumulated diagnostics, and pins `serde_json`
     output to the two encodings its three consumers use: the order manifest
     (`order.py`) and the validation result (`semantic.validation_json`) write sorted
     keys, `indent=2`, non-ASCII as is and a trailing newline; the project hash
     (`order._project_hash`) writes sorted keys, compact separators and non-ASCII as is
     (corrected 2026-10-08).
   - Docs, context and stats go to the slices the surface inventory assigns. The build
     switches to `reg-core-py` for folds in slice 3a; FQID and hashing move with the
     build's other `reg_meta` imports in stage 4. Gated by G0 and G1.
4. **Retire the Python runtime.** Checkpoint 3 passed 2026-10-09. The agent plugin moves
   to MCP; the Dockerfile and publish workflow drop the CLI; the build's `reg_meta`
   imports move into `reg_meta_build` or `reg-core-py`; the Python `reg_meta` package
   and `reg_schema` are deleted; schema major 10.0.0 carries #1296's catalog rework.
   Packages, order and decisions: "Stage 4 packages".
5. **SPA on WASM.** Replace `period.ts`, `validation.ts` and the hand-written
   `project_data.ts` with `reg-core` compiled to WASM plus generated types. Ends at
   checkpoint 4.

The build track (section 11) runs alongside, independent of these stages.

### Stage 3a packages

Each package follows the execution protocol (section 4). Written against `origin/main`
at `8a232636`, reviewed by Astra (codex `gpt-6-astra`) and revised. Package numbers 3a.8
(G1 wiring, folded into each operation package) and 3a.12 (`fetch`, moved to stage 4 at
checkpoint 2) are withdrawn.

Order and parallelism (at most three in flight):

- Wave 1: **3a.1a**, **3a.3**, **3a.4**. 3a.1a and 3a.4 share `Cargo.lock` and `ci.yml`;
  the later one rebases.
- Then **3a.1b** (needs 3a.1a) and **3a.2** (needs 3a.1a; merges after 3a.1b). Only
  3a.1b (9.2.0) and 3a.2 (9.3.0) bump the artifact schema, in that order.
- **3a.5** (needs 3a.1b, 3a.3, 3a.4); then **3a.6** (needs 3a.2, 3a.5) and **3a.7**
  (needs 3a.5) in parallel. **3a.2a** (needs 3a.2) builds alongside and merges after
  3a.6 (checkpoint-2 decision 7), deleting the provisional, named G1 exceptions 3a.5 and
  3a.6 carry.
- **3a.10** (needs 3a.4) and **3a.11** (needs 3a.6, 3a.10), then **3a.9** (needs 3a.7,
  3a.10, 3a.11), then **3a.13** (needs 3a.9 and the 9.3.0 release), then checkpoint 2.
  From the 3a.1b merge, deploys pause until the maintainer releases at 9.3.0
  (container-build schema guard).

Deployment rule (checkpoint 2): production switches from FastAPI to the Rust server
alone in 3a.9, with no proxy and no per-path edge routing. The SPA cutovers (3a.10,
3a.11) land first, so that when 3a.9 switches production the landing page and search
work; every other page is unavailable in production until its slice ships. Locally, the
vite dev proxy sends ported paths to the Rust server and the rest to uvicorn.

Shared definitions:

- G0: `uv run python -m pytest conformance <touched packages> -n auto -q` and
  `cargo test --workspace`.
- The Rust HTTP run (from 3a.4, part of G0 and CI): `cargo build --workspace`, then
  `uv run python -m pytest conformance -q -k '<selection>' --server-cmd='<template>'`;
  3a.4 documents the template. Each package names a selection that passes; 3a.6 and
  later run all of `[api/`.
- G1: `uv run python -m conformance.differential` on a committed tree. An operation
  joins G1 in the package that implements it.
- A package that changes fixture generations updates every expected `generation` (`api`
  cases included) as a reviewed metadata update.

**3a.1a `reg-core-py` and the build toolchain.** Implements sections 6 (`reg-core-py`),
8 (the build's native dependency) and the section 5 fold sweep.

- Changes: `crates/reg-core-py`, a PyO3 module built by maturin and exposing
  `fold_search` only, is a Cargo and uv workspace member; `reg_meta_build` depends on
  it. PyO3's `extension-module` feature is maturin-only, so `cargo test --workspace`
  links. uv `cache-keys` cover `crates/reg-core/**`, `crates/reg-core-py/**`,
  `Cargo.toml` and `Cargo.lock`; the G1 derive key (`DERIVE_SOURCES`) and the fixture
  cache key (`build_inputs_digest`) hash the same paths.
- CI and image: Rust toolchain and cache in every job that runs `uv sync`; `maturin`
  joins the root dev dependencies; the Dockerfile skeleton copies the member's
  `pyproject.toml`. G1 gains the exhaustive fold sweep (every scalar bare and in the
  stage-0 contexts, plus every distinct indexed string of the pinned artifacts),
  baseline `fold_search` against `reg-core-py`; the only exception is scalars unassigned
  in the baseline's UCD.
- Paths: `crates/reg-core-py/`, `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`,
  `reg_meta_build/pyproject.toml`, `reg_meta/tests/reader_artifacts.py`,
  `conformance/differential/`, `.github/workflows/ci.yml`, `reg_webapp/Dockerfile`,
  `reg_meta_build/DESIGN.md`.
- Acceptance: G0 on a fresh `uv sync`; an edit in `crates/reg-core` changes the next
  `uv run`'s extension, fixture key and derive key; G1 and the sweep report 0
  differences outside the exception. PyPI publishing stops (decision 4).

**3a.1b Pre-folded full-text indexes.** Implements section 5 (decision 16).

- Changes: derive drops and rebuilds `register_fts`, `variable_fts`,
  `classification_fts` and `value_code_fts` as regular FTS5 tables storing `fold_search`
  text (via `reg-core-py`), with the key columns (`register_id`, ...) `UNINDEXED` so
  every existing join holds. The tokenizer is `unicode61 remove_diacritics 0`, so
  `fold_search` is the only fold. `_populate_fts` leaves `write_resolved_catalog` and
  `extend_db` (`simplify:` always rebuilds `value_code_fts`; skip the inherited one if
  derive misses its budget). The stoplist and owner filter stay.
- Python reader: `MATCH` from `fold_search(q)`; the ASCII-strip `_fold_fts_text`/
  `py_fts_term` becomes `fold_search`; display text from base tables. Builder and
  `reg_meta` `SCHEMA_VERSION` 9.2.0.
- `validate_built_db`: one check per table that its stored text equals `fold_search` of
  the source and its rowids equal the indexed source set (today `classification_fts` has
  a rowid check and `value_code_fts` only a count check).
- Paths:
  `reg_meta_build/src/reg_meta_build/{derive,db,resolved_catalog,extend_db,validate}.py`,
  `reg_meta/src/reg_meta/{queries,db}.py`, `conformance/differential/config.toml`,
  `reg_meta/DESIGN.md` (FTS5), the tests and goldens they touch, this file (timings).
- Goldens: each result difference is a content decision with its reason; a fixed defect
  extends its case (a `ø` step in `http_search/column-chips`). Out of scope: `doc_fts`
  (decision 3).
- Acceptance: G0; byte-identical rebuild; G1 differences only from folding, each a named
  exception with its reason (for example `register_id` no longer matching as a token);
  the PR records derive time, G1 time (under 5 min) and the artifact size delta.
- Recorded (2026-10-08, pinned `reg_meta/v0.42.0`): `derive` takes 118.5 s on the global
  artifact and 93.7 s on SWECOV, of which the index step is 4.2 s and its validator
  check 4.9 s (the rest is the stage-2 resolver and validation). G1 takes 282.4 s with a
  re-derive (151.0 s of it deriving) and 126.1 s without. The index bytes grow from 67.8
  to 120.2 MB (+52.4 MB); the derived global file grows 56.6 MB and SWECOV 55.2 MB.

**3a.2 Search pins as curated build input.** Implements sections 6 and 10 (pins are
curated input; an unresolved pin is a build error).

- Changes: `search_golden.toml` moves to `reg_meta_build/curation/search_pins.toml`
  (Pydantic model in `_curation.py`). `pipeline.py` reads it from the selected
  `--curation-dir` and passes it explicitly to `write_resolved_catalog`, which writes a
  `search_pin` table (key `fold_search(query)`, type, position, entity) and a manifest
  key `search_pins_sha256` that joins `GENERATION_KEYS`. A complete build fails with a
  located error on an unresolved pin; a scoped (`--registers`) or diagnostic build
  writes no pins and the empty-pins hash. Derive writes the empty table and the
  empty-pins `search_pins_sha256` on an older base. FastAPI's `golden.py` and
  `run_search_eval.py` read the table. Builder and `reg_meta` schema 9.3.0.
- Fixtures: no default pins; a request key names a case-local pins file, passed to the
  build, so its content enters the generation. The in-process `golden_config` path goes.
- Paths: `reg_meta_build/curation/search_pins.toml`,
  `reg_meta_build/src/reg_meta_build/{_curation,pipeline,resolved_catalog,derive,db,validate,artifact_identity}.py`,
  `reg_meta/src/reg_meta/db.py`,
  `reg_webapp/backend/src/reg_webapp/{golden.py,search_golden.toml}`,
  `reg_webapp/backend/{scripts/run_search_eval.py,tests/test_search_golden_config.py}`,
  `reg_meta/tests/reader_artifacts.py`, `conformance/{http_cases.py,README.md}`,
  `conformance/cases/reader/fixture/identity.json`,
  `conformance/cases/http_search/golden-*`,
  `reg_meta_build/tests/test_curation_toml_load_boundary.py`, the DESIGN.md files.
- Goldens: ten cases. Seven of the eight `golden_config` cases keep their expected
  files; `golden-pin` gets a case-local pin file; `golden-classification` drops its
  `stalebook` pin and two 500 steps, keeping both `choicebook` `type=classification`
  steps; `golden-stale-register` goes. Both 500s become one located build-failure case.
- Acceptance: G0; the failure case names file, entry and FQID; every `golden-*` case
  passes out of process.

**3a.3 FQID and period grammar in `reg-core`.** Implements section 5 (grammar, single
home) for the `register` ref and the `period` parameter.

- Changes: `reg-core` parses FQIDs and periods (`2019`, `2015..2019`, `LA2019`,
  `2019-03`, `2019-01-01..2019-06-30`) to typed values and to errors that map to
  `invalid_ref` and `invalid_period`.
- Oracle: a small hand-written file of accept and reject examples taken from the tracker
  grammar (`conformance/cases/grammar/`), plus one round-trip property per grammar as a
  seeded loop (no property-testing crate). A disagreement with today's Python is
  recorded in the PR, not fixed.
- Paths: `crates/reg-core/`, `conformance/cases/grammar/`. Out of scope: build bindings
  (stage 4), project-schema periods (3e), frontend grammars (stage 5).
- Acceptance: G0.

**3a.4 `reg-catalog` and `reg-meta serve`: admission, envelope, errors, `context`.**
Implements sections 6 and 7 (admission, `{data, meta}`, errors, catalog selection) and
the `context` operation.

- Changes: `reg-catalog` opens read-only with `immutable=1`, gates the schema (same
  major, minor at least 9.1), admits publishable identity and checks `--catalog NAME`; a
  refusal prints the error document and exits with its code. Operations register in one
  file per slice (`ops/slice_3a.rs`). `reg-meta serve` (axum, utoipa; arguments parsed
  with std) serves `/openapi.json`, `{data, meta}`, `{error, meta}` and today's ETag and
  `Cache-Control` policy. `context` follows `shape.Context`; steward branding moves from
  `steward.toml` to `steward.json` (the Python loader follows), so no runtime TOML.
  `reg_meta_version` is the crate version, kept equal to `reg_meta`'s by
  `scripts/check_versions.sh`. Runtime crates: the named ones plus tokio and sha2.
- G0: the generated OpenAPI matches `operations.toml` for shipped slices, and the error
  enum equals `errors.toml` (`toml` dev-dependency). Runner: startup cases under their
  `[api/<case>]` ids and a `{catalog}` placeholder. G1 plumbing: the harness builds
  `reg-meta`, serves each derived copy, and installs the baseline commit's `reg_webapp`
  as the oracle; `context` maps to its `/api/context` plus `/api/stats`.
- Cases: `api/context-*` twins of `http_context/*`, `http_scope/stats` and
  `http_scope/stats-catalog`, plus `scope=holdings` on a catalog (`scope_unavailable`)
  and an invalid `scope` (`invalid_parameter`), written red first. `context` gains
  `errors = ["scope_unavailable"]` in `operations.toml`, which section 7 already implies
  (`scope` is valid on every read).
- Paths: `crates/reg-catalog/`, `crates/reg-meta/`, `Cargo.*`,
  `conformance/{http_cases,conftest,test_http}.py`, `conformance/README.md`,
  `conformance/cases/api/`, `conformance/api/operations.toml` (`context`'s errors only),
  `conformance/differential/`, `reg_webapp/stewards/`,
  `reg_webapp/backend/src/reg_webapp/{stewards,models}.py` with `backend/openapi.json`
  and `frontend/src/lib/api-types.ts` (docstring drift), `reg_webapp/DESIGN.md`,
  `conformance/test_boot.py`, `.github/workflows/ci.yml`, `scripts/check_versions.sh`,
  `ARCHITECTURE.md`, this file.
- Acceptance: G0 with `-k '[api/admission or [api/context'`; G1 reports 0 differences
  for `context`.

**3a.5 Rust `search`: parameters, paging and the variable arm.** Implements section 7
(paging, one parameter per concept) and `search` with `type=variable`.

- Parameters: `q` at most 200 characters, no NUL; no letter or digit gives no items.
  `register` resolves as a FQID first, then a bare name (`not_found`, `ambiguous_ref`
  with candidates, `invalid_ref`). The cursor is hex, bound to generation, parameters
  and scope (`invalid_cursor`, `stale_cursor`); depth stops at 1000. FTS gets
  `fts_match_query(fold_search(q))`.
- Period: a period's years \[lo, hi\] (a month or day covers its year), overlapped as
  today's `_year_scope_filter` does: a variable by its own states, a register by any of
  its variables', a concept group by its members'; applied before each arm's limit.
- The variable arm ports today's behavior: exact identity first, concept-group hits with
  `matched_count` and members, delivery-column chips, scope. The Rust minimum schema
  becomes 9.2.
- Cases: `column-chips`, `pagination`, `group-members` and `cursor-scope` already have
  twins (`api/search-scope`, `search-paging`, `search-group-hit`, `cursor-invalid`).
  New, red first: register resolution (FQID, unique bare name, ambiguous, unknown,
  malformed); period (variable overlap, group by members, `invalid_period`;
  register-wide is 3a.6's, with the register arm); a 9.1 manifest refused. The PR names
  which cases cover `cli_scope/search-holdings-1` and `search-reference-2`. A G1 mapping
  compares typed pages with the baseline webapp's variable group at the same limit and
  cursor depth.
- Paths: `crates/reg-catalog/`, `crates/reg-meta/`, `conformance/http_cases.py`
  (`artifacts`), `conformance/cases/api/`, `conformance/differential/`.
- Acceptance: G0 with a selection joining `[api/meta]`, `[api/cursor-`,
  `[api/invalid-parameters]`, `[api/scope-unavailable]`, `[api/search-paging]`,
  `[api/search-scope]`, `[api/search-group-hit]`, the 3a.4 cases and the new ones; G1
  differences only as named exceptions.

**3a.6 Rust `search`: the other arms, pins and one ranked list.** Implements decision 17
and the single ranking of section 6.

- Arms: `register`, `classification` (with succession and classification-group hits),
  `classification_code` and `register_value`, with today's code ordering and at most 5
  owners. Classification arms are off under `register` or `period`; code arms ignore
  `period`. Pins lead their type's list without duplicates and obey the same scope,
  register and period filters (a pinned classification drops when its arm is off).
- Untyped order: each arm's first 1000 rows, deduplicated by today's candidate key and
  with grouped variable members hidden, sorted in a stable total order: pinned first
  (pin order), then best-bets score descending, then arm order, then the arm's own
  position, then the hit's FQID or key. The cursor is a keyset position in that order.
- The Rust minimum schema becomes 9.3.
- Cases: existing twins are `api/search-no-token` (`punctuation-only`),
  `search-code-owners` (`bounded-code-owners`), `search-all-types`
  (`top-results-exact-leaf`) and `invalid-parameters` (`limit-clamp`). New, red first:
  twins of `golden-*`, the other `top-results-*`, `codes-reference` and
  `code-owner-ranking`; an untyped search followed across three pages; period and
  register filters on the classification and code arms; a register-wide period; a 9.2
  manifest refused. G1 maps each typed page to the baseline webapp's group; untyped
  pages have no baseline equivalent (decision 17 ranks one list across arms), so the
  `api` corpus alone pins them.
- Paths: `crates/reg-catalog/`, `conformance/cases/api/`, `conformance/differential/`.
- Acceptance: G0 with all of `[api/`; G1 differences only as named exceptions.

**3a.7 MCP: `reg-meta mcp` and `/mcp`.** Implements sections 6, 7 (two transports) and 9
(MCP equivalence).

- Changes: rmcp serves the registered operations as tools over stdio (`reg-meta mcp`)
  and streamable HTTP (`/mcp` on `serve`). A domain error is the same error document as
  a tool error. `/mcp` has axum's `DefaultBodyLimit` (`payload_too_large`) and a
  hand-written per-client token bucket (`rate_limited`), separate from any SPA limit.
- G0: `tools/list` is built from the OpenAPI document, matches `operations.toml`'s tool
  names and equals its golden (`conformance/cases/mcp/tools-list.json`).
  `conformance/test_mcp.py` (raw JSON-RPC over `httpx2`) checks that `search`'s tool
  call and HTTP request return the same `data`, `meta` or error for a success and each
  of `invalid_parameter`, `invalid_ref`, `ambiguous_ref`, `not_found`, `invalid_period`,
  `scope_unavailable`, `invalid_cursor` and `stale_cursor`, plus one stdio session.
- Paths: `crates/reg-meta/`, `crates/reg-catalog/src/ops/{mod,slice_3a}.rs` (an
  operation's tool and description; branding optional for `mcp`), `Cargo.lock`,
  `.github/workflows/ci.yml`,
  `conformance/{test_mcp.py,conftest.py,http_cases.py,README.md}` (`case_clients`
  hoisted), `conformance/cases/mcp/`.
- Acceptance: G0; the PR rechecks the stage-0 202-on-DELETE papercut.

**3a.9 Deployment: the Rust server alone, hosted MCP.** Implements decisions 12 and 15
(deployment) and section 8 (server image).

- Changes: a pinned Rust build stage builds `reg-meta`; the runtime stage runs
  `reg-meta serve` alone (no uvicorn, no Python runtime) with the baked DBs and steward
  branding, and the entrypoint's smoke gate probes `context`, `search` and `/mcp` with
  `curl` from the runtime base. The DB bake keeps its current fetch until stage 4.
  Deletes: `reg_webapp/backend/src/reg_webapp/smoke.py` and its test.
- Edge: the global worker's `ORIGIN_PATHS` (`index.ts`) and `run_worker_first`
  (`wrangler.jsonc`) gain `/mcp` together, in the global config only, and drop `/docs`,
  which the Rust server does not serve. The HTTP adapter strips `__edge_v` before
  validation. `/mcp` keys its rate limit on the edge-supplied client address only for
  requests that provably came through the edge, and rmcp's allowed hosts admit the
  public host. The schema guard reads the Rust minimum.
- Smoke: after the edge deploy, `container-build.yml` sends JSON-RPC `initialize`,
  `tools/list` and one `search` to `catalog.swecov.se/mcp` with `curl`. Edge MCP rate
  rules are a maintainer step listed in the PR.
- Paths: `reg_webapp/{Dockerfile,docker-entrypoint.sh,fly.toml,fly.swecov.toml}`,
  `reg_webapp/edge/`, `reg_webapp/backend/src/reg_webapp/smoke.py`,
  `reg_webapp/backend/tests/test_smoke.py`, `.github/workflows/container-build.yml`,
  `scripts/schema_pending_bump.py`, `crates/reg-meta/`, `reg_webapp/DESIGN.md`
  (Deployment).
- Acceptance: G0; the image builds and boots against a fixture catalog and answers
  `context`, `search` and `/mcp`; after the 9.3.0 release, the deployed landing page and
  search are answered by the Rust server, the public smoke step is green and a burst
  gets `rate_limited`.

**3a.10 SPA cutover: `context`.** Implements decision 15 for `/api/context` and
`/api/stats`, atomically.

- Changes: a committed Rust OpenAPI snapshot, kept equal by a `cargo test`; `gen:types`
  writes a second module from it, and the Dockerfile's frontend stage copies it. The
  vite dev proxy sends `/api/context` to the Rust server. `api.ts` reads `{data, meta}`
  and `{error, meta}`; `App` threads `sizes` to `Home`, which stops fetching
  `/api/stats`. `smoke.py` drops its `/api/context` readiness wait and check and keeps
  the `/api/catalog` walk (`test_smoke.py` follows), so deploys boot until 3a.9.
- Deletes: FastAPI `routes/context.py`, `routes/stats.py`, their models and tests,
  `http_context/*`, `http_scope/stats`, `http_scope/stats-catalog`; in `surface.toml`,
  the deleted routes' rows go and surviving rows' `covered_by` and `used_by` are
  updated.
- Paths: `crates/reg-meta/`, `reg_webapp/frontend/`, `reg_webapp/backend/`,
  `reg_webapp/Dockerfile`, `reg_webapp/.claude/skills/run-reg-webapp/`,
  `.github/workflows/ci.yml`, `conformance/cases/http_context/`,
  `conformance/cases/http_scope/`, `conformance/test_http.py`,
  `conformance/api/surface.toml`, `reg_webapp/DESIGN.md`. The `/api/stats` steps in
  `http_scope/catalog-refuses-holdings` and `invalid-scope` go; 3a.4's `api/context-*`
  scope cases pin that behavior.
- Acceptance: G0; `bun run check`, `lint`, `test` and `gen:types` with no diff; the dev
  setup renders Home and the footer from the Rust server.

**3a.11 SPA cutover: `search`.** Implements decisions 15 and 17 for `/api/search`,
atomically.

- Changes: the vite dev proxy sends `/api/search` to the Rust server. `SearchView` makes
  one untyped call (`limit=5`) for the top-results strip and one call per type
  (`limit=3`), each continued by its own cursor; `SearchOmnibox` follows.
  `run_search_eval.py` calls the Rust server over HTTP.
- Deletes: FastAPI `routes/search.py`, `golden.py`, `query_input.py` if unused, the
  search models and tests, `http_search/*`; `surface.toml` rows and references as in
  3a.10.
- Paths: `crates/reg-meta/`, `reg_webapp/frontend/src/lib/` (search files and tests),
  `reg_webapp/frontend/vite.config.ts`, `reg_webapp/backend/`,
  `conformance/cases/http_search/`, `conformance/cases/http_scope/`,
  `conformance/test_http.py`,
  `conformance/{artifact_requests,test_acceptance_agreement,test_artifact_sample}.py`,
  `conformance/api/surface.toml`, `reg_webapp/DESIGN.md`. The in-process `/api/search`
  callers move to the Rust server through `--server-cmd` (the release admission
  traversal included), and the remaining `/api/search` steps in `http_scope/` go to
  their `api` twins. Out of scope: the CLI `search` (stage 4; the G1 baseline uses it).
- Acceptance: G0; the frontend gates as in 3a.10; G1 green; the dev setup's search page
  is answered by the Rust server.

**3a.2a Reference derivation for the G1 baseline (approved at checkpoint 2).**
Implements section 4 (G1 independence).

- Changes: the harness pins a reference builder commit (main after 3a.2, `553ea622`) and
  runs it from a detached `git worktree` of that commit with its own locked environment
  (maturin builds `reg-core-py`), since `builder_commit()` needs a clean source
  checkout; the baseline reader moves to the same commit. It derives reference copies of
  the pinned originals once, cached apart from candidate copies (keyed by base sha256
  and reference commit, with output digests), never from the checkout. The baseline CLI
  and webapp read the reference copies; the checkout arm keeps reading candidate-derived
  copies. `fold-search-matches`, `folded-index-bm25` and the now-empty
  `derived-schema-version` go; `derived-generation-cursor` and
  `derived-generation-order` stay (the arms' `builder_commit` differs). Candidate copies
  of the two catalogs derive in parallel (the G1 budget). Pins stay unexercised by G1
  until 3a.13 (the 9.0 originals have none); G0's pin cases cover them.
- Paths: `conformance/differential/`, this file (section 4, "Current pin").
- Acceptance: G1 reports 0 differences with neither folding exception; a deliberately
  broken candidate index (for example `value_code_fts` indexing `label` without
  `fold_search`, not committed; the owner filter excludes no row on the 0.42.0 pin)
  shows differences; G1 under 5 min with a candidate re-derive; the PR records the
  reference derive time. Depends on: 3a.2, 3a.6 (merge order).

**3a.13 Re-pin at checkpoint 2.** Implements section 4 (re-pin).

- The artifact pin moves to the 9.3.0 release. With 3a.2a, the baseline is already at
  main after 3a.2 and the 9.3.0 originals already carry folded indexes and pins, so the
  baseline reads the release directly and the reference derive is retired.
- G1 runs on the old pin with its baseline and on the new pin with the new baseline, and
  records each difference; the PR updates section 4's "Current pin" and
  `conformance/differential/config.toml`.
- Acceptance: G1 on both pins, under 5 min each. Depends on: 3a.9 and the release.

#### Checkpoint 2 decisions (maintainer, 2026-10-08)

1. **Deployment:** production runs the Rust server alone from 3a.9; unported pages are
   unavailable in production until their slice ships (no users). No proxy, no per-path
   edge routing, no `hyper-util`. Decision 15 is reworded accordingly.
2. **Runtime crates:** tokio and sha2 are ratified.
3. **Decision 16** covers the catalog indexes; `doc_fts` gets its folding in the slice
   that ports docs.
4. **`reg_meta_build` stops publishing to PyPI** (`publish_reg_meta_build.yml` and the
   release skill's step go); the builder runs from a checkout.
5. **`fetch` moves to stage 4** with local binary distribution (`surface.toml` owner);
   3a.12 is withdrawn.
6. **Release:** the maintainer cuts `reg_meta` at schema 9.3.0 now (G2 and release).
7. **G1 baseline:** package 3a.2a merges; the baseline reads reference copies derived by
   a pinned builder.

Resolved by the orchestrator:

- One schema source: utoipa's OpenAPI document, from which the MCP tool schemas are
  built (no schemars derivation, so the transports cannot drift; 3a.7 pins `tools/list`
  with a golden); best-bets is ranking code, not data (tracker correction);
  `surface.toml` row upkeep on deletion is mechanical.
- `context` lists `scope_unavailable` (section 7 makes `scope` valid on every read; the
  operation table had omitted it). The Rust HTTP run joins G0 from 3a.4; the HTTP
  adapter strips `__edge_v`; no MCP disable flag, the SWECOV worker does not route
  `/mcp`.
- Rate limits: fix a demonstrated defect only and keep direct-origin protection; no
  clap, base64, tower-http or runtime `toml` (`toml` is dev-only).

### Stage 3b–3e packages

Each package follows the execution protocol (section 4) and the package and review
protocol below. Written against `origin/main` at `7b645b4e` (3a.9 merged), assuming
3a.13, checkpoint 2 and the tooling package (`scripts/gate.py`, branch
`claude/rr-full-gate`) have merged. The maintainer's decisions for these slices are
recorded after the packages ("Stage 3b–3e decisions"); Dn below refers to them.

Order and parallelism. "Depends on" lists what must be on main before a package's PR
opens. Preparatory work (red `api` cases, a `served` mapping, a derive family module)
may start earlier in a session that has no other package in flight; anything that reads
a new table or a new transport feature waits for it to merge.

- **Wave 0:** **3b.1** (Python: the 3b derived tables) and **3b.2** (Rust: transport
  prerequisites, exercised by the docs metadata operations) have no dependency on each
  other and run as two sessions (the wave-0 exception to one package per slice session).
  **3e.1** (project types in `reg-core`) has no catalog dependency and may take the
  third.
- **Wave 1 (three slice sessions):**
  - 3b session: **3b.3** (`show` and refs), then **3b.4**, **3b.5** and **3b.6**.
  - 3c session: **3c.1**, then **3c.2** and **3c.3**. The slice has no SPA step (its
    surface is CLI-only) and ends at 3c.3; the session then takes 3e.
  - 3d session: **3d.1**, then **3d.2**.
  - Schema PRs (3c.1, 3d.1, the doc-schema bump in 3b.6) merge one at a time and take
    the next free minor in merge order.
- **Wave 2:** **C**, the catalog-page cutover for 3b and 3d (D3), once 3b.4, 3b.5 and
  3d.2 have merged. The 3b slice ends when C merges.
- **3e:** **3e.2** needs 3e.1 and 3b.3 (refs over expanded states, the resolver and
  holdings), not docs or C; then **3e.3** and **3e.4**.
- **F** deletes the empty FastAPI app once C, 3b.6 and 3e.4 have merged (D4). **R**
  re-pins at checkpoint 3.

3b runs to seven PRs against the tracker's estimate of one to four: it owns 27 of the 54
surface rows plus the transport every slice shares, and C is shared with 3d.

Shared definitions:

- **Full gate**: `scripts/gate.py all` (steps `g0`, `rust`, `release`, `flows`,
  `frontend`). `rust` runs the whole conformance suite with `--server-cmd` and
  `--mcp-cmd`, so a test moved out of process needs no CI selection edit; `release`
  keeps release admission's `--server-cmd`. Every package runs it; "Acceptance" lists
  only what it does not cover.
- **Regenerate**: `scripts/gate.py regen` rewrites the OpenAPI snapshot, the MCP
  `tools/list` golden, `api-types-rust.ts`, the backend `openapi.json` and
  `api-types.ts`. A PR that changes a Rust route, parameter, result type or tool
  description runs it, and again after every rebase over another such change.
- **G1** (`scripts/gate.py g1`, on a committed tree; D7) is **required** on every PR
  that touches derive or the docs build (3b.1, 3b.6, 3c.1, 3d.1) and **once per slice
  before its cutover** (3b and 3d in C, 3b's docs in 3b.6, 3c in 3c.3, 3e in 3e.4).
  Other PRs rely on the `api` corpus. An operation package still writes its `served`
  mapping; differences the slice run finds are fixed in the package that owns the
  operation, before the cutover merges. Budget: under 5 min on a warm run; a cold run
  (re-deriving after a derive-source change) is reported in the PR but does not fail the
  budget (maintainer, 2026-10-08); each derive PR records derive time, G1 time and the
  artifact size delta. G1 on a table-only PR protects existing behavior only; a new
  table's semantics rest on `validate_built_db` and the `api` corpus until the slice's
  served comparison reads it.
- **Frozen Python runtime** (D6). The Python runtime (the `reg_meta` reader and CLI,
  `reg_schema`) is frozen: defect fixes go in Rust only. The build stays Python (section
  11), so a derived table gets the correct logic in derive, even where the frozen reader
  computes the same fact wrongly. Every such divergence is pinned by an `api` case once
  a Rust operation serves it, and gets a narrow named G1 exception (`case` glob and
  `paths` as tight as the diff allows) whose `reason` starts `rust-only fix:` and names
  that `api` case. It lives until stage 4 (D1).
- **G1 entry of an operation.** A module under `conformance/differential/served/` (3b.2
  splits `served.py`) maps the Rust result onto the baseline's shape. Two baseline
  kinds:
  - *Webapp baseline*: the baseline commit's `reg_webapp`, as in 3a (show, states,
    warnings, values, docs, graph).
  - *CLI baseline*: the baseline CLI result of a case the CLI arm already generates
    (`cases.py`), reused by case id instead of a second baseline run (coverage, schema,
    diff, coded_variables, resolve, lineage, validate, order). The checkout's CLI arm
    keeps running: it is the compatibility check on candidate-derived copies.
  - Requests with no baseline equivalent are not compared, and the `api` corpus alone
    pins them: bare-name and ambiguous refs, cursors past the first page where the
    baseline pages by offset, filters the baseline lacks (`states` `variant` without
    `period`, `values` `partition`), and orderings the operation table changes.
- **G1 independence.** The baseline reader computes states, deliveries, held intervals,
  schema, coverage and succession at read time from base tables, so it never reads a
  table 3b–3d add; only candidate copies, derived by the checkout, carry them. No
  intermediate re-pin is needed because a slice adds tables. The independent release
  reference (section 4) stays, and R re-pins at checkpoint 3.
- **MCP parity.** Every operation package adds its operation to `test_mcp.py`'s
  equivalence: one representative success and each applicable domain error from its
  `errors` list, tool call against HTTP request (section 9). `warnings` and the two
  downloads are HTTP-only.
- **Schema minors and deploys.** A schema PR bumps the builder's and `reg_meta`'s
  `SCHEMA_VERSION` (the frozen Python reader is not changed, as 3a.2); 3b.6 bumps
  `DOC_SCHEMA_VERSION`. `reg_catalog::SCHEMA` moves in the first Rust package that reads
  the new table. The container-build guard checks all three axes, so production deploys
  pause from the first merged bump (3b.1) until a release carries every merged minor. A
  PR that changes fixture generations updates every expected `generation` as a reviewed
  metadata update.
- **Release coordination** (D5). The maintainer cuts each release from main with every
  merged schema minor at once: the catalog and SWECOV assets at the builder's
  `SCHEMA_VERSION` and the docs asset at `DOC_SCHEMA_VERSION`. Each cutover package (C,
  3b.6, 3e.4) adds its routes to `container-build.yml`'s post-deploy public smoke and
  lists, as a maintainer step, the deployed check after the next release: catalog pages
  (C), docs (3b.6), project validate and order (3e.4).
- **Compiling §3 rows** (D2). A class-A row of section 3 is compiled when the Rust read
  would otherwise repeat resolver, closure or aggregate work per request (the resolver,
  `same_as` closure, chains, `coded_variable_stats`). A row the PR reads directly
  instead needs a measured request time, from its `api` case or a timing on the pinned
  artifact, recorded in the PR.
- **Cross-slice data.** Where a `show` field would need another slice's table, the field
  belongs to that slice's facet (operation table, `shape.Show` note), not to a
  cross-slice dependency.
- **CLI-era cases.** A package deletes a `cli_scope`, `logical`, `coverage` or `reader`
  case only when the PR shows that a named `api` case pins the same behavior. Every
  other such case stays until stage 4.

Shared files (rebase conflicts expected; keep both sides, then regenerate). Every
package's paths implicitly include these files; a package lists only its other paths.

- `reg_meta_build/src/reg_meta_build/db.py` (`SCHEMA_VERSION`, `DERIVED_DDL`),
  `reg_meta/src/reg_meta/db.py` (`SCHEMA_VERSION`, bump history),
  `reg_meta/src/reg_meta/doc_db.py` (`DOC_SCHEMA_VERSION`),
  `crates/reg-catalog/src/lib.rs` (`SCHEMA`).
- `reg_meta_build/src/reg_meta_build/derive/__init__.py` (the `derive()` call list) and
  `validate.py` (the `validate_built_db` check-call list). 3b.1 makes `derive.py` a
  package with one module per table family (`states.py`, `search_index.py`, then 3c's
  `schema.py` and 3d's `chains.py`), named by contract, not slice, so they survive stage 4.
  New validator checks live in the family's module, called from `validate.py`.
- `crates/reg-catalog/src/ops/mod.rs` (one `mod` line and one `all()` entry per slice
  file); operation bodies live in their own modules.
- `crates/reg-meta/src/mcp.rs` and `conformance/test_mcp.py` (3b.2 makes both per-tool;
  later packages add rows, not code).
- `conformance/differential/served/__init__.py` (the family list) and `config.toml`
  (`[[exception]]`, append-only).
- `conformance/api/surface.toml` (row upkeep on deletion is mechanical, checkpoint 2)
  and `operations.toml` (no edits without escalation).
- The generated files (regenerate), `reg_webapp/frontend/vite.config.ts` (proxy map),
  `conformance/test_http.py` (`SURFACES`), `.github/workflows/container-build.yml`
  (post-deploy smoke), `reg_webapp/backend/scripts/fixture_db.py` and the skill's
  `catalog_fixture_db.py` (3b.1 makes them call the whole of `derive()`).
- 3d.2 edits 3a's `crates/reg-catalog/src/ops/search/classification.rs` and 3b.3's refs
  module.

Merge rule: the orchestrator merges one PR at a time. Before each merge the author
rebases on main, takes the next free schema minor if the PR bumps one, re-runs the
affected `api` cases and adds one mechanical commit with the output of
`scripts/gate.py regen`. A PR's G1 result stands across such a rebase unless the rebase
changed derive or the docs build.

#### Package and review protocol

Author (one per package, in its own worktree):

- `git fetch origin && git checkout -B <branch> origin/main`. Read `CLAUDE.md`, section
  4 "Execution protocol", this section's preamble and your package, and every section it
  references. Rebase on `origin/main` before review; never force-push someone else's
  branch.
- Change only the paths the package lists. A path outside them, or anything on the
  escalation list (operation table or error catalog, a section-13 or checkpoint
  decision, the meaning of an existing golden, schema major, a gate budget, a runtime
  dependency this file does not name), stops the package and goes to the orchestrator. A
  wrong code fact in this file (name, count, path) is fixed in the same PR and named in
  the body.
- Latest stable tools and crates; no compatibility work.
- Testing policy: assert at public boundaries, no private-name imports, oracles as data,
  about one test per stated behavior at its hardest case, each with a comment naming the
  change that makes it fail. Unit tests only for grammars, interval algebra and pure
  folds. Every piece of machinery a PR adds is exercised by a case in that PR.
- Simplicity binds as hard as correctness: build only what the stated behavior needs.
  End with a simplification pass over the diff and list it under "Simplification" in the
  PR body.
- Conventional commits (`<type>(<package-or-cross-package>): <summary>`), each ending
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never bypass hooks. Push
  with `GIT_WORK_TREE=$PWD git push -u origin HEAD` (a repo hook hijacks `GIT_DIR`).
  Prefix scratch files with the package id.
- Run the full gate, and `scripts/gate.py g1` only when the package's own G1 line says
  G1 runs in this package (a line reading "G1 (run in X)" means X runs it). Run other
  heavy commands (a parallel pytest, cargo, a derive or a probe over the pinned
  artifacts) through `scripts/gate.py heavy -- CMD`, so they share the heavy-job lock.
  Open the PR with `gh pr create`: package id, what changed, each acceptance item with
  its result, decisions made, anything deferred; end with
  `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Do not merge.
- When the review arrives, the author applies the reviewer's report directly and re-runs
  what the fix touches. The orchestrator settles only scope questions and disagreements
  between author and reviewer.

Reviewer (a fresh agent that did not write the PR):

- `git fetch origin && git checkout -B review-<pkg> origin/<branch>`; read the same
  sections and the PR body (`gh pr view <n>`, body only). Never push, commit to the
  branch, comment on GitHub or merge; the output is a report.
- Check the diff (`git diff origin/main...HEAD`) against the package: stated behavior,
  listed paths (an out-of-path edit must be justified in the body), escalation items;
  correctness, determinism and edge cases; machinery no case exercises; tests that pin
  implementation, import private names, use non-data oracles, twin another test or
  cannot fail; dead code, speculative options and duplicated leaf helpers (CLAUDE.md
  "Reuse first").
- Re-run the full gate and every acceptance item except G1; list each with its result.
  When the author reports a G1 run at the reviewed head, or at a head whose derive,
  differential and pin are unchanged since, read the G1 mapping, sampling and exceptions
  instead of rerunning it; rerun G1 only when one of those changed after that run.
- Report only defects in the stated behavior, rule violations and simplifications. An
  idea that adds scope goes in one line under "Not requested (YAGNI)", unranked.
- Report: verdict (approve / changes needed), findings ranked blocker / major / minor
  with `file:line` evidence and a concrete fix, commands run with results.

Sessions and merging: at most three concurrent sessions, counted as sessions, not
packages; each session runs one package at a time in its own worktree. The one exception
is wave 0, where 3b.1 and 3b.2 run as two sessions because 3e.1 is the only other wave-0
work. A session deletes each worktree when its package merges or is withdrawn. The
orchestrator is the only one who merges to main (merge rule above) and keeps the
in-flight list.

#### Transitional inventory

  | Dual structure                                             | Deleted in                                                                                          |
  | ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
  | `derived-generation-*` G1 exceptions                       | while the baseline reads the release originals and the checkout its derived copies (all of stage 3) |
  | `reader-version` G1 exception (the arms' release versions) | stage 4                                                                                             |
  | `rust-only fix:` G1 exceptions                             | stage 4 (D1)                                                                                        |

#### Packages

**3b.1 Expanded states and browse deliveries.** Implements section 3 (`expanded_state`,
`browse_delivery`, `canonical_column`, scope as data) and §13 decision 1.

- Changes: derive emits `expanded_state` from the pass `resolver_columns` already makes:
  one whole-history call per (variable, variant) on the same worker pool of the
  participation rule inside `_expand_state_windows` (`_applicable_alias_windows`, since
  `_expand_state_windows` drops a replaced base), so derive runs the resolver once. Each
  row references its source state and carries its own id, kind (base fallback, source
  window, curated window, coded window), bounds, `canonical_column` and the
  representation fields the reader needs for the window fallback (section 3).
  `resolver_column` becomes a SQL projection of `expanded_state`. `browse_delivery`
  holds `register_variable_deliveries` per scope (`scope = 'holdings'` rows in steward
  artifacts). Warning attribution is not compiled (decision 2; section 3):
  `reg_meta/DESIGN.md` records its exact predicate. Builder and `reg_meta`
  `SCHEMA_VERSION` 9.4.0; `derive.py` becomes the `derive/` package.
- `validate_built_db`: `expanded_state` equals one recomputation and `resolver_column`
  equals its projection (replacing today's second resolver pass); browse windows
  disjoint; one canonical spelling per (variable, variant, fold); fixed insertion order.
- `reg_meta/DESIGN.md` drops "No state/window resolution is compiled"; section 3's size
  estimate becomes the measured delta.
- Paths: `reg_meta_build/src/reg_meta_build/{derive/,db,validate,extend_db}.py`,
  `reg_meta/src/reg_meta/db.py` (version only),
  `reg_webapp/backend/scripts/fixture_db.py`,
  `reg_webapp/.claude/skills/run-reg-webapp/catalog_fixture_db.py`, the tests and
  fixture generations they touch, `reg_meta/DESIGN.md`, `reg_meta_build/DESIGN.md`, this
  file (section 3, timings).
- Acceptance: full gate; byte-identical rebuild; a synthetic artifact with perturbed
  alias windows fails the new checks with located messages; G1 0 differences; derive and
  G1 times and size delta recorded. If the re-derive path exceeds 5 min, the slow table
  goes set-based before merge (section 4).
- Depends on: checkpoint 2. Deploys pause from this merge (preamble).

**3b.2 Transport prerequisites with `docs_get` and `docs_related`.** Implements the
transport pieces 3b–3e share, each exercised here, and two docs operations.

- Changes:
  - `{ref}` path parameters spanning segments, a final `{filename}` segment after one,
    and more than one route per operation; `api_spec.rs` accepts path parameters.
  - Parameter schemas from the operation table's types, declared per `Param`;
    `param_schema` stops matching names.
  - Cache tier declared per operation, replacing `cache_control`'s path prefixes.
  - The register ref and the cursor encoder move out of `search.rs` into shared modules
    (search's `api` cases exercise both unchanged).
  - A tool exposing several operations takes an `operation` argument: the `docs` tool
    with `docs_get` and `docs_related`; `mcp.rs` and `test_mcp.py` become per-tool
    (equivalence rows per operation, `tools/list` without the one-operation-per-tool
    assumption).
  - A raw-bytes response for the PDF download (bytes, media type, headers).
  - `served.py` split into `served/` per operation family; a docs family maps `docs_get`
    and `docs_related`.
- Operations: `docs_get` and `docs_related` over the docs DB, with `docs_unavailable`
  replacing `ingested: false`. No catalog table is read, so the Rust minimum stays.
- Cases, red first: twins of `test_docs.py`'s get, related and file behaviors; MCP
  equivalence for both operations' success and errors; a download whose bytes and
  headers match `docs_related`'s `sha256` and `byte_size`.
- Paths: `crates/reg-catalog/`, `crates/reg-meta/`, `conformance/cases/api/`,
  `conformance/{http_cases,test_mcp}.py`, `conformance/cases/mcp/`,
  `conformance/differential/`, the generated files.
- Acceptance: full gate; search's `api` cases unchanged. Depends on: checkpoint 2.

**3b.3 Refs and `show`.** Implements section 7 (refs, `show`, retired FQIDs).

- Changes: the refs module covers every FQID kind, group refs (`group/<p>/<r>/<key>`,
  `group/class/<key>`), bare names (unique, or `ambiguous_ref` with candidates) and a
  retired register or variable FQID resolved to its terminal successor from 3d.1's
  `succession_terminal` (a split is `ambiguous_ref`; done here, not in 3d.2). `show`
  serves every kind of `shape.Show`, a register's variants and a classification's owning
  variables unpaged; Rust `SCHEMA` 9.6 (main's schema at merge; `browse_delivery` needs
  9.4).
- Cases, red first: the fields of Show per kind; twins of
  `http_catalog/{admission,concept-group-admission,group-coverage,redirects,unheld-group,provider-pages-follow-scope,provider-register-coverage,live-unheld-register-successor}`
  and `cli_scope/{register,groups,varinfo,classification-variables}-*`; MCP equivalence
  for `show`'s success and each listed error.
- G1 (run in C): webapp baseline `/api/catalog`, `/api/catalog/{fqid}`, both group
  routes and `/variants` for every register, the variable samples and every
  classification; a retired FQID compares the baseline's 301 target with `fqid`; CLI
  baseline `get classification --variables` for owning variables.
- Paths: `crates/reg-catalog/`, `crates/reg-meta/`, `conformance/cases/api/`,
  `conformance/test_mcp.py`, `conformance/differential/served/`, the generated files.
- Acceptance: full gate. Depends on: 3b.1, 3b.2.

**3b.4 `states` and `warnings`.** Implements the class-C rules of section 3 (window
fallback, warning clipping).

- Changes: `states` reads `expanded_state` and applies the request-dependent fallback
  (windows replace the base only when a source window is spelled like the base column
  and a source window overlaps the request; curated windows are additive; with no
  overlapping source window the base state stands). `warnings` applies the attribution
  predicate (`reg_meta/DESIGN.md`) over `data_warning` and clips to held and requested
  periods; it has no MCP tool (operation table).
- Cases, red first: gap, partial-overlap and spanning periods; twins of
  `logical/{narrowed-state-token,narrowed-state-warnings,canonical-case-twin-state-warnings,warnings-*}`
  and `http_catalog/{states-and-deliveries,warnings}`; `variant` and `value_set_version`
  without `period`; MCP equivalence for `states`.
- G1 (run in C): webapp baseline: the states the variable node embeds and the catch-all
  `?period` subset for the variable samples and sampled periods (not `/states`: same
  rows and order, but full hydration, while `states` serves the node's light one);
  `/data_warnings` with each filter.
- Paths: `crates/reg-catalog/src/ops/{slice_3b.rs,states.rs,warnings.rs}`,
  `conformance/cases/api/`, `conformance/test_mcp.py`,
  `conformance/differential/served/`, the generated files.
- Acceptance: full gate. Depends on: 3b.3.

**3b.5 `values`.** Implements `values` (a classification's codes and a state's value
set).

- Changes: cursor paging with `total`, `partition` (default `source_extensions`),
  `classification`, `column` with `alias_window_from`, and `q` matched with
  `fold_search`. If a scan cannot meet the request, a folded column joins 3b.1's family
  module as its own minor (and the PR then runs G1).
- Cases: twins of `cli_scope/values-*`,
  `logical/{values-holdings,saturated-reference-values}` and the codes of
  `get classification --codes`; a page boundary inside a `q` match; MCP equivalence.
- G1 (run in C): webapp baseline `/api/value-sets/{id}/codes` (offset pages concatenated
  against cursor pages) and CLI baseline `get classification --codes`.
- Paths: as 3b.4 with `values.rs`; `reg_meta_build/src/reg_meta_build/derive/` and its
  tests if the folded column is needed. Acceptance: full gate. Depends on: 3b.3.

**3b.6 `docs_search`, docs folding and the docs cutover.** Implements `docs_search` and
checkpoint-2 decision 3 (`doc_fts` folding).

- Changes: the docs build (`doc_db.py`, the step that fills `doc_fts` today) stores
  `fold_search` text, with `DOC_SCHEMA_VERSION` 1.3.0; a derive path for the docs DB is
  added only if that step's measured cost requires it. G1's cache makes a candidate docs
  copy by running that step over a copy of the pinned docs DB, instead of symlinking it.
  Folding differences against the unfolded baseline are named exceptions with their
  reason, as in 3a.1b. `docs_search` joins the `docs` tool.
- Cutover (atomic, as 3a.10): the vite proxy sends `/api/docs` to Rust; `DocView`,
  `DocMentionsPanel` and the related-documents panel read `{data, meta}`. Deletes
  `routes/docs.py`, its models and `test_docs.py`, the docs route rows of `surface.toml`
  and the `doc_queries`/`doc_db` import rows whose last importer goes. Adds docs to the
  post-deploy smoke.
- G1 (this package runs it): CLI baseline `docs list|get|search`; webapp baseline
  `/api/docs/for-variable` and `/related`.
- Paths: `crates/`, `reg_meta_build/src/reg_meta_build/doc_db.py`,
  `reg_meta/src/reg_meta/doc_db.py` (version only), `reg_webapp/frontend/`,
  `reg_webapp/backend/`, `conformance/`, `.github/workflows/container-build.yml`, the
  generated files, `reg_webapp/DESIGN.md`, `reg_meta_build/DESIGN.md`.
- Acceptance: full gate; G1 0 differences outside named exceptions; the dev setup's docs
  pages answered by Rust. Maintainer step: after the next release, the deployed docs
  search, document and download answer. Depends on: 3b.2.

**3c.1 Schema, coverage and coded-variable tables.** Implements the 3c rows of section 3.

- Changes: `coded_variable_stats` per scope (`get coded-variables`, 5.5 s unfiltered),
  and the per-scope delivery windows `schema`, `diff` and `coverage` read
  (`delivery_window`), from `expanded_state` and `browse_delivery`; derive family module
  `schema.py`; next free schema minor.
- `validate_built_db`: each table equals a recomputation; windows disjoint per scope.
- Paths: `reg_meta_build/src/reg_meta_build/{derive/,db,validate}.py`,
  `reg_meta/src/reg_meta/db.py` (version), the fixture scripts as in 3b.1, tests and
  generations touched, `reg_meta_build/DESIGN.md`.
- Acceptance: full gate; byte-identical rebuild; G1 0 differences; times and size delta
  recorded. Depends on: 3b.1.

**3c.2 `schema` and `diff`.** Implements `schema` (register and variable, absorbing
`get datacolumns`) and `diff`, both on the `schema` tool.

- Cases, red first: twins of `cli_scope/{schema,datacolumns,diff}-*` and
  `logical/{alias-case-twin-datacolumn,alias-diff-holdings,datacolumns-holdings,diff-holdings,get_datacolumns-unheld-holdings}`;
  a page boundary inside one register variant; MCP equivalence for both operations.
- G1 (run in 3c.3): CLI baseline `get schema` (summary and a sampled year),
  `get datacolumns` and `get diff`, mapped to the operation's rows; `--columns-like`
  cases are not compared.
- Paths: `crates/reg-catalog/src/ops/{slice_3c.rs,schema.rs}`,
  `crates/reg-catalog/src/lib.rs` (`SCHEMA`), `conformance/cases/api/`,
  `conformance/test_mcp.py`, `conformance/differential/served/`, the generated files.
- Acceptance: full gate. Depends on: 3b.3, 3c.1.

**3c.3 `coverage`, `coded_variables` and `resolve`, closing 3c.** Implements the three
operations.

- `resolve` takes `columns` as `string[]`: repeated query keys over HTTP (array-typed
  parameters only; others still refuse a repeat) and a JSON array over MCP, at most 200;
  each row has a status. That transport is built here, its first consumer.
  `coded_variables` is ordered by distinct codes.
- Cases: twins of `cli_scope/{availability,coded-variables,resolve}-*`, `coverage/*` and
  `logical/{coded-*,alias-case-twin-resolve,resolve-holdings}`; a repeated `columns` key
  and a 201-name request; MCP equivalence for the three, including an array argument.
- G1 (this package runs the slice's): CLI baseline `get availability` (without
  `target`/`target_type`), `get coded-variables` (excluded where the baseline orders
  differently) and `resolve`.
- Owes 3c.2's deferred `rust-only fix:` exception for `schema-alias-windows`. The frozen
  reference `get schema` expands only variables with a per-column window, so G1 reports
  `*/reference/get-schema-*` for registers whose variables have only shared alias
  windows. On the v0.43.0 pin these are
  `scb/{innovation-foretag,it-anvandning,ekonomiskt-bistand,rams,lisa,hreg,bas}`, and in
  SWECOV also `swedbank/konsumtion`, `inera/{bestallda-prover,samtal}`,
  `tillvaxtverket/korttidsarbete` and `swecov/population`. Its `case` globs come from
  the cases G1 actually reports.
- Closes the slice: `covered_by` of the 3c command rows points at the `api` twins;
  proven twins among the CLI-era cases go (preamble).
- Paths: as 3c.2 with `coverage.rs`, `coded.rs`, `resolve.rs`;
  `crates/reg-catalog/src/ops/mod.rs`, `crates/reg-meta/src/mcp.rs`,
  `crates/reg-catalog/tests/api_spec.rs`;
  `conformance/cases/{cli_scope,logical,coverage}/` and their runners;
  `conformance/api/surface.toml`.
- Acceptance: full gate; G1 0 differences outside named exceptions, time recorded.
  Depends on: 3c.2.

**3d.1 Chain and family tables.** Implements the chain rows of section 3.

- Changes: `succession_terminal` (registers, variables, classifications),
  `classification_chain` and `classification_family`; derive family module `chains.py`;
  next free minor. Succession is computed at the manifest's
  `classification_succession_as_of_year`, and `validate.py` keeps reading that year from
  the manifest; tests that need another policy year build artifacts with it.
  `same_as_resolution` was dropped (orchestrator, 2026-10-08): the reader's `same_as`
  BFS runs only when a direct lookup misses, and the writer requires both endpoints of
  every `variable_same_as` and `classification_same_as` edge to be live
  (`resolved_metadata.py` 743–757). On the v0.42.0 pin, 0 of 1,640 `variable_same_as`
  source keys are dead and `classification_same_as` is empty. The table would always be
  empty, so the unordered-BFS defect cannot be reached.
- `validate_built_db`: acyclic, consistent with `*_replaced_by` at the manifest year,
  equal to a recomputation.
- Paths: `reg_meta_build/src/reg_meta_build/{derive/,db,validate}.py`,
  `reg_meta/src/reg_meta/db.py` (version), the fixture scripts, tests and generations,
  `reg_meta_build/DESIGN.md`.
- Acceptance: full gate; byte-identical rebuild; a synthetic cycle in
  `variable_replaced_by` fails with a located message; G1 0 differences; times recorded.
  Depends on: 3b.1 (merge order of minors).

**3d.2 `graph` and `lineage`.** Implements both on the `graph` tool.

- Changes: one `graph` route for variable, classification and group refs; `lineage` with
  edges, warnings and per-register provenance. Search's classification arm reads
  `succession_terminal`; its request-time walk goes (the refs module reads it since
  3b.3). The reader does not port the `same_as` fallback (unreachable, 3d.1).
  `succession_terminal` stops at a split and applies the policy year to every kind
  (search's rule; ratified 2026-10-08). A retired ref whose walk stops at a split
  answers `ambiguous_ref` with the split's successors as `candidates`, never a bare
  `not_found`; a ref behind a future-dated edge is still live at the policy year and
  resolves to itself. 3b.3's refs apply this rule. The baseline's
  `resolve_terminal_successor` takes the first branch of a split and ignores the year
  for registers and variables, so both cases differ from the baseline's 301 target under
  a narrow `rust-only fix:` exception naming their `api` case. `succession_terminal` is
  unscoped: in holdings scope the refs module checks at read time that the terminal is
  held, as the frozen `resolve_terminal_successor` does. Search's terminal-centric
  `editions()` may be read from `classification_chain` (the anchor's rows up to its own
  position) only while no edition has two predecessors and a split's outbound edges
  share one year; 3d.2 verifies this against the reader rather than assuming it (result:
  both pins satisfy it, but the builder admits a merge and
  `api/search-classification-succession` has one, so `editions()` keeps its read-time
  walk). `show` reads `classification_family`: its `editions` join the
  `classification_family` kind, the classification root's `families` and a
  classification's `family` (3b.3 serves their key and label), and its membership
  replaces 3b.3's slug-prefix family lookup.
- Cases: twins of `http_catalog/{reference-edges,whole-variable-group-graph}`,
  `cli_scope/lineage-unheld-reference` and
  `logical/{edges-unheld-owner,unheld-terminal-*}`; a split successor; a retired ref
  through `show`, `graph` and `search` on one chain; a retired ref at a split
  (`ambiguous_ref` with its successors); MCP equivalence for both operations.
- G1 (run in C): webapp baseline for the three graph routes and `/lineage_warnings`; CLI
  baseline `get lineage`.
- Paths:
  `crates/reg-catalog/src/ops/{slice_3d.rs,graph.rs,lineage.rs,refs.rs,search/classification.rs,show.rs}`,
  `crates/reg-catalog/src/lib.rs` (`SCHEMA`), `conformance/cases/api/`,
  `conformance/cases/fixtures/show/`, `conformance/test_mcp.py`,
  `conformance/differential/`, the generated files.
- Acceptance: full gate; 3a's search cases unchanged. Depends on: 3b.3, 3b.4 (graph's
  variable nodes read its states leaf), 3d.1.

**C Catalog-page cutover (3b and 3d).** Implements §13 decision 15 for every
`/api/catalog*` and `/api/value-sets` route at once (D3).

- Changes: the vite proxy sends `/api/catalog`, `/api/states`, `/api/warnings`,
  `/api/values`, `/api/graph` and `/api/lineage` to Rust. `catalog.ts`, `CatalogRoot`,
  `CatalogNodeView`, `BindingLeafView`, `ConceptGroupView`, `ClassificationLeafView`,
  `ClassificationGroupView`, `ClassificationCodesPanel`, `DataWarnings` and
  `LineageDetails` read `show` plus facets; the binding page fetches states, warnings,
  lineage and graph separately. `artifact_requests.py` and
  `test_acceptance_agreement.py` move their `/api/catalog` steps to the Rust server.
- Deletes: `routes/catalog.py`, the node and facet models in `models.py`,
  `catalog_fqid.py`, `period_param.py` and `query_input.py` once their last user goes,
  their backend tests, `http_catalog/*` and the catalog steps of `http_scope/*`; the 3b
  and 3d route rows of `surface.toml`, and the stage-4 import rows whose last importer
  goes (appendix, a plan revision). The 3b and 3d command rows' `covered_by` points at
  the `api` twins. Done: `http_catalog/` and the catalog steps of `http_scope/` are
  deleted; their behaviors are `api` cases, and the three left without a twin
  (scope-not-leaking, `/variants` on a retired register, `same_as` under holdings) are
  covered elsewhere.
- Paths: `reg_webapp/frontend/`, `reg_webapp/backend/`,
  `reg_webapp/.claude/skills/run-reg-webapp/`,
  `conformance/{test_http,artifact_requests,test_acceptance_agreement}.py`,
  `conformance/cases/{http_catalog,http_scope}/`, `conformance/api/surface.toml`,
  `.github/workflows/container-build.yml`, the generated files, `reg_webapp/DESIGN.md`.
- Acceptance: full gate (`flows` included); G1 for 3b and 3d, 0 differences outside
  named exceptions; every catalog page in the dev setup rendered from Rust. Maintainer
  step: after the next release, the deployed root, a register, a binding, a concept
  group and a classification page render. Depends on: 3b.4, 3b.5, 3d.2.

**3e.1 Project types and structural validator in `reg-core`.** Implements §13 decision 3
(first half) and section 5 (project schema).

- Changes: `reg-core` gains the `project_data.json` types and the structural validator
  ported from `reg_schema/structural.py`: raw input kept, diagnostics accumulated,
  periods through 3a.3's grammar, messages from templates with codes and fields (section
  5). `serde_json` output is pinned to the two encodings its three consumers use: the
  order manifest and the validation result (sorted keys, `indent=2`, non-ASCII as is,
  trailing newline) and the project hash `order._project_hash` (sorted keys, compact,
  non-ASCII as is).
- Oracle: `reg_schema/test_corpus/` and the structural cases of
  `conformance/cases/validate/`, read as data by a Rust test. Those expected files stay
  frozen (frozen Python runs against them). An intentional Rust difference is pinned in
  the Rust or `api` corpus, with a reviewed exception in the Rust test naming the case;
  a change to a golden's meaning is an escalation.
- Paths: `crates/reg-core/`, `Cargo.lock`.
- Acceptance: full gate; every corpus case matches outside listed exceptions;
  byte-identical re-encoding of every committed manifest under
  `conformance/cases/validate/*` (today only `gap-clipped/order.json`), with
  `provenance.project_hash` equal to the Rust hash of the requesting body; the `order/*`
  manifests are committed by 3e.3, whose acceptance covers them. Depends on: none.

**3e.2 Interval algebra and `validate`.** Implements `validate` (POST, `order` tool).

- Changes: `reg-core` gains the interval algebra of `reg_meta/inventory.py` (unit tests
  and a seeded property loop, as 3a.3); `reg-catalog` validates semantically over refs,
  `expanded_state`, `resolver_column` and holdings. POST JSON bodies with the body
  limit; an invalid project is `200 {ok: false}`. The `order` tool starts with
  `validate`.
- Cases: `conformance/cases/validate/*` gain `api/` POST twins (shape conversion per the
  operation table); `http_scope/project-rejects`; MCP equivalence.
- G1 (run in 3e.4): CLI baseline `validate` over `cases.py`'s generated projects.
- Paths: `crates/`, `conformance/cases/{api,http_scope}/`, `conformance/test_mcp.py`,
  `conformance/differential/served/`, the generated files. Acceptance: full gate.
  Depends on: 3e.1, 3b.3.

**3e.3 `order` and the manifest download.** Implements `order` and its download route.

- Changes: `order` returns `{data: manifest, meta}`; `POST /api/project/order/manifest`
  serves the exact bytes as `attachment; filename="order.json"`; `project_invalid` and
  `order_blocked` carry structured findings. `conformance/cases/order/*` compare the
  committed `order.json` bytes over HTTP; `test_order.py` and `test_artifact.py` run
  against `--server-cmd`.
- Cases: MCP equivalence for `order`'s success and both errors.
- G1 (run in 3e.4): CLI baseline `order` bytes against the download bytes
  (`derived-generation-order` stays).
- Paths: as 3e.2 with `conformance/{test_order,test_artifact}.py` and
  `conformance/cases/order/`. Acceptance: full gate. Depends on: 3e.2.

**3e.4 Project cutover, closing 3e.** Implements §13 decision 15 for `/api/project/*`.

- Changes: the vite proxy sends `/api/project` to Rust; `api.ts`, `project_data.ts`,
  `ProjectEditor` and `ValidationPanel` read `{data, meta}`; `driver.mjs` `flows`
  intercepts the new shapes. The write rate limit moves to the server (the bucket `/mcp`
  uses). Adds project validate and order to the post-deploy smoke.
- Deletes: `routes/project.py`, `request_body.py`, `project_validation.py`, `limits.py`,
  the project models and tests, the `validate` surface of `test_http.py` and its proven
  twins, the 3e rows of `surface.toml` and the `reg_meta.order.OrderFinding` import row
  in `surface.toml`. The 3e command rows' `covered_by` points at the `api` twins.
- Paths: `reg_webapp/`, `conformance/`, `crates/reg-meta/` (the write rate limit),
  `.github/workflows/container-build.yml`, the generated files, `reg_webapp/DESIGN.md`.
- Acceptance: full gate; G1 for 3e, 0 differences outside named exceptions. Maintainer
  step: after the next release, a deployed project validates and orders. Depends on:
  3e.3.

**F The empty FastAPI app** (D4). Once C, 3b.6 and 3e.4 have merged FastAPI serves no
route. Deletes the app, middleware, `models.py`, the backend `openapi.json`,
`api-types.ts`, uvicorn in `dev.sh`, the vite fallback, the app-wiring import rows and
the transitional rows they end. Keeps what stage 4 still needs: the Dockerfile's
`regmeta-db` stage and the workspace members it copies. Paths: `reg_webapp/`,
`conformance/`, `scripts/gate.py` (`regen`), `pyproject.toml`, `uv.lock`, `ci.yml`.
Acceptance: full gate. Depends on: C, 3b.6, 3e.4.

**R Re-pin at checkpoint 3.** Implements section 4 (re-pin): the artifact pin moves to
the latest release; G1 runs on both pins and records each difference. `rust-only fix:`
exceptions carry over (D1). The baseline commit cannot simply follow the pin: cutovers
delete the webapp routes the served arm compares against (section 4, "Current pin"). R
either moves the served comparisons onto the CLI baseline (the CLI-baseline pattern
above) or keeps the webapp baseline commit separate from the artifact pin. The
`docs-fold-search*` exceptions match by term position (`3[47]`, `7[47]`, 3b.6): R
rechecks that those positions are still the terms `ß` and `ﬁlm`. Depends on: 3c.3, C, F
and the release. **Shipped 2026-10-09**: pin `reg_meta/v0.44.0`; the baseline stays at
553ea622, pinned apart from the release until stage 4; `docs-fold-raw-query` and
`docs-fold-raw-query-hint` record the maintainer's G2 ruling; positions 34/37 and 74/77
are still `ß` and `ﬁlm`.

#### Surface coverage (3b–3e rows)

Every `surface.toml` row owned by 3b, 3c, 3d or 3e (54: 27, 6, 9, 12), with the package
that ports it and the package whose deletion removes the row. Command rows stay until
stage 4 deletes the CLI; their slice points `covered_by` at the `api` twins.

  | Row                                                                                                                                    | Owner | Ported by                 | Row deleted by |
  | -------------------------------------------------------------------------------------------------------------------------------------- | ----- | ------------------------- | -------------- |
  | GET /api/catalog                                                                                                                       | 3b    | 3b.3                      | C              |
  | GET /api/catalog/group/class/{key}                                                                                                     | 3b    | 3b.3                      | C              |
  | GET /api/catalog/group/{provider}/{register}/{key}                                                                                     | 3b    | 3b.3                      | C              |
  | GET /api/catalog/{fqid}                                                                                                                | 3b    | 3b.3 (period subset 3b.4) | C              |
  | GET /api/catalog/{fqid}/data_warnings                                                                                                  | 3b    | 3b.4                      | C              |
  | GET /api/catalog/{fqid}/states                                                                                                         | 3b    | 3b.4                      | C              |
  | GET /api/catalog/{provider}/{register}/variants                                                                                        | 3b    | 3b.3                      | C              |
  | GET /api/value-sets/{value_set_id}/codes                                                                                               | 3b    | 3b.5                      | C              |
  | GET /api/docs/doc/{identifier}                                                                                                         | 3b    | 3b.2                      | 3b.6           |
  | GET /api/docs/file/{register}/{filename}                                                                                               | 3b    | 3b.2                      | 3b.6           |
  | GET /api/docs/related/{register}                                                                                                       | 3b    | 3b.2                      | 3b.6           |
  | GET /api/docs/for-variable                                                                                                             | 3b    | 3b.6                      | 3b.6           |
  | GET /api/docs/search                                                                                                                   | 3b    | 3b.6                      | 3b.6           |
  | command docs get                                                                                                                       | 3b    | 3b.2                      | stage 4        |
  | command docs list / docs search                                                                                                        | 3b    | 3b.6                      | stage 4        |
  | command get classification                                                                                                             | 3b    | 3b.3 (`--codes` 3b.5)     | stage 4        |
  | command get groups / get register / get varinfo                                                                                        | 3b    | 3b.3 (states 3b.4)        | stage 4        |
  | command get values                                                                                                                     | 3b    | 3b.5                      | stage 4        |
  | import reg_meta.doc_db.RelatedDocument                                                                                                 | 3b    | 3b.2                      | 3b.6           |
  | import reg_meta.doc_queries.{doc_get,doc_registers,doc_search,related_document_content,related_documents_for_register} (5)             | 3b    | 3b.2, 3b.6                | 3b.6           |
  | command get availability                                                                                                               | 3c    | 3c.3                      | stage 4        |
  | command get coded-variables                                                                                                            | 3c    | 3c.3                      | stage 4        |
  | command get datacolumns / get diff / get schema                                                                                        | 3c    | 3c.2                      | stage 4        |
  | command resolve                                                                                                                        | 3c    | 3c.3                      | stage 4        |
  | GET /api/catalog/group/class/{key}/graph                                                                                               | 3d    | 3d.2                      | C              |
  | GET /api/catalog/group/{provider}/{register}/{key}/graph                                                                               | 3d    | 3d.2                      | C              |
  | GET /api/catalog/{fqid}/graph                                                                                                          | 3d    | 3d.2                      | C              |
  | GET /api/catalog/{fqid}/lineage_warnings                                                                                               | 3d    | 3d.2                      | C              |
  | GET /api/catalog/{fqid}/{dimensions,lineage,predecessors,successors} (removed)                                                         | 3d    | none                      | C              |
  | command get lineage                                                                                                                    | 3d    | 3d.2                      | stage 4        |
  | POST /api/project/validate                                                                                                             | 3e    | 3e.2                      | 3e.4           |
  | POST /api/project/order                                                                                                                | 3e    | 3e.3                      | 3e.4           |
  | command validate / command order                                                                                                       | 3e    | 3e.2 / 3e.3               | stage 4        |
  | import reg_meta.errors.EXIT_SUCCESS                                                                                                    | 3e    | 3e.2                      | 3e.4           |
  | import reg_meta.order.{OrderManifest,blocked_message,parse_project} (3); {materialize_order,project_from_raw} (2, conformance imports) | 3e    | 3e.3                      | 3e.4; stage 4  |
  | import reg_meta.semantic.{validate_project,validation_json} (2)                                                                        | 3e    | 3e.2                      | 3e.4           |

Plan revision for stage-4 rows. `surface.toml` deliberately assigns shared-model and app
imports to stage 4. Under this plan their last importer outside `reg_meta` goes earlier,
so the deleting package removes the row mechanically (checkpoint 2):

- In C, imported only by `routes/catalog.py`, `models.py`, `catalog_fqid.py` or
  `period_param.py`: `reg_meta.ResolvedClassification`,
  `reg_meta.ClassificationDerivedFromRef`,
  `reg_meta.catalog.{BindingGroupRef,CatalogStorageId,ClassificationCode,ClassificationEdition,ClassificationExtensionMember,ConceptGroupMember,ConceptGroupSummary,GroupAxis,LineageEdge,LineageWarning,Period,RegisterCoverage,ResolvedProvider,ResolvedRegister,ResolvedVariable,TagMembership,ValueSetMember,VariableCoverage,VariableDelivery,VariableEdition,VariableRef,VariableState,VariantSummary}`,
  `reg_meta.graph.RelationshipGraph`, `reg_meta.queries.list_classifications`,
  `reg_meta.fqid.{CLASSIFICATION_PREFIX,DEFAULT_VARIANT_SLUG,RESERVED_*}`, and the
  `reg_webapp` entry of `used_by` for
  `reg_meta.fqid.{Fqid,FqidError,FqidKind,parse,validate_slug,is_period}` and
  `reg_meta.queries.fold_search`. Also in C, because their last `reg_webapp` importer
  went with the catalog routes (corrected in C; earlier text put them in F):
  `reg_meta.holdings.{ReadScope,resolve_scope}` and the `reg_webapp` entry of `used_by`
  for `reg_meta.errors.{EXIT_NOT_FOUND,EXIT_USAGE}`.
- In 3e.4: `reg_meta.order.OrderFinding` (`routes/project.py`, `OrderBlockedModel`).
- In F: none (corrected in F). `app.py`'s `reg_meta.db` was the last app-wiring import;
  `reg_meta.db` and `reg_meta.doc_db` keep their rows because the dev fixture builder
  (`reg_webapp/backend/scripts/fixture_db.py`) imports them, and `reg_meta` and the
  other `reg_meta.holdings.*` and `reg_meta.errors.*` rows no longer list `reg_webapp`.

#### Stage 3b–3e decisions (maintainer, 2026-10-08)

1. **G1 oracle.** The Python baseline stays through stage 3. A Rust-only fix gets a
   narrow named G1 exception linked to an `api` regression case, which guards the
   correction; a pinned-Rust baseline waits until stage 4.
2. **Compiling §3 rows.** Compile only work that would be repeated on every request
   (resolver, closure, aggregates); read a plain join directly only when it measures
   cheap.
3. **One joint catalog cutover (C)** for slices 3b and 3d.
4. **Package F** deletes the empty FastAPI app once C, 3b.6 and 3e.4 have merged,
   keeping what stage 4 still needs (decisions 2 and 15 amended).
5. **Batched releases**, one after C and one after 3e.4, each carrying every merged
   schema minor and the docs asset, followed by deployed smoke checks of the catalog,
   docs and project pages each cutover package lists.
6. **Rust-only fixes.** The Python runtime (the `reg_meta` reader and CLI, `reg_schema`)
   is frozen; defect fixes go in Rust only. The builder is not frozen: a derived table
   gets the correct logic in derive.
7. **G1 cadence.** G1 runs once per slice before its cutover, and on every PR that
   touches derive or the docs build; other PRs rely on the `api` corpus. The candidate
   Python CLI comparisons stay; served cases reuse baseline results only.
8. **Gate tooling.** `scripts/gate.py` holds the full gate (`all`: `g0`, `rust`,
   `release`, `flows`, `frontend`), `crates`, `regen` (every rebase-sensitive generated
   file), `g1` and `heavy -- CMD` (any other heavy command under the same lock). Heavy
   steps take one of three machine-wide slots, so at most three heavy jobs run at once;
   sccache shares compiled crates across worktrees.
9. **Sessions.** At most three concurrent slice sessions, each deleting its finished
   worktrees; the orchestrator is the only one who merges to main.
10. **Reviews.** The author applies the reviewer's report directly; the orchestrator
    settles only scope questions and disagreements.

Astra's review of the draft (2026-10-08) shaped 3b.2's split, MCP parity per operation,
the deploy-pause and release rules and the frozen-golden rule in 3e.1.

### Stage 4 packages

Each package follows the execution protocol (section 4) and the package and review
protocol of stages 3b–3e. Surveyed on `origin/main` at `45cc111e` (#1305, schema 9.7.0)
and written against `53764cfb`. The packages assume `reg_meta` 0.45.0 (schema 9.7) is
released before stage 4 starts; at writing the tag does not exist yet. The maintainer's
decisions are recorded after the packages ("Stage 4 decisions"); Dn below refers to
them.

What still depends on Python, by package:

- **The build** imports `reg_meta` in about 60 `reg_meta_build/src` modules and 85 of
  164 test files: the reader functions derive calls (4.2), the FQID and period grammar
  and `register_py_lower` (4.3), and the retained modules (`source_evidence`,
  `documentary`, `inventory`, `errors`, `cli_common`, the `db`/`doc_db` constants and
  admission, `queries.extract_year`, `catalog.DataWarning`) (4.4).
- **The fixture builder** `reg_meta/tests/reader_artifacts.py`, imported by conformance,
  builder tests and the webapp's dev fixture scripts (4.4).
- **Conformance runners** that call the Python reader or CLI (4.7), and Python G1 (4.1,
  4.9a).
- **Deploy and CI**: the Dockerfile's `reg-meta update` bake, the schema guard's Python
  axis, the Podman integration job, PyPI publishing (4.5).
- **The agent plugin** (4.6), `reg_meta/DESIGN.md` and `reg_schema/DESIGN.md` (4.8), and
  the release skill and agent docs (4.9b).

#### Packages

**4.1 G1 on a pinned Rust baseline.** Implements section 4 (G1) and stage 3b–3e decision 1.

- Changes: the baseline becomes the `reg-meta` binary built at `reg_meta/v0.45.0`,
  cached in the shared G1 cache, reading the release originals; the candidate stays the
  checkout's derive plus `reg-meta serve`. Responses compare as raw bytes: delete the
  `served/` mappings and the 553ea622 webapp baseline, keep `cases.py` and the exception
  machinery. The artifact pin moves to 0.45.0. Delete the `rust-only fix:` exceptions;
  keep `derived-generation-*` and `reader-version` (for `meta` and cursors when derive
  code differs). The Python CLI arms and the fold sweep run unchanged until 4.9a.
- Paths: `conformance/differential/`, `conformance/test_g1_cache.py`, `scripts/gate.py`
  (`g1`), section 4 ("Current pin", G1).
- Acceptance: full gate; G1 on the 9.7 pin shows 0 differences outside the named
  exceptions, within the 5-minute warm budget; cold time recorded.
- Escalate: any difference between the pinned binary and the checkout the PR does not
  explain. Depends on: checkpoint 3 and the 0.45.0 tag.

**4.2 Derive and the SWECOV generator stop calling the reader.** Implements section 4
("bootstrap").

- Changes: `derive/states.py` gets connection-level `_states_in_bounds`,
  `_variable_windows`, `_applicable_alias_windows` and `canonical_delivery_column` (with
  `_delivery_column_spellings` and `representative_columns`); `derive/schema.py` gets
  `get_coded_variables` with `scope_predicate`; `build_catalog.py`'s
  `_representative_spelling` reads `resolver_column` (as `holdings_compile` does)
  instead of `Catalog.delivery_columns`; `validate.py`'s recomputations use the moved
  code. Delete the matching `surface.toml` rows.
- Paths: `reg_meta_build/src/reg_meta_build/{derive/,validate}.py`,
  `reg_meta_build/input_data/swecov/build_catalog.py`, the tests they touch,
  `surface.toml`.
- Acceptance: full gate; derived copies of both pinned artifacts byte-identical to
  main's (SHA-256) and the SWECOV inventory output byte-identical; G1 (run in 4.2) 0
  differences.
- Escalate: any derived byte difference. Depends on: 4.1.

**4.3 FQID, period grammar and `fold_identity` through `reg-core-py`.** Implements
section 5.

- Changes: bind `Fqid` parse (kind and parts), `is_period`, period bounds, the slug
  check and `fold_identity`; move `derive_variable_slug`, `derive_period`, `try_emit`
  and `_YEAR` into `reg_meta_build`; `register_py_lower` uses the binding;
  `source_interpreter_commit` tracks the `reg-core-py` sources instead of
  `reg_meta.fqid` and `reg_meta.queries`.
- **Assumption:** reg-core's period bounds (Python ends every February on the 29th; G1
  exception `february-period-token`) and its slug error wording are accepted as a
  content change, reviewed in G2's dbdiff and in the diff of the curation
  located-failure goldens.
- Paths: `crates/reg-core-py/`, `crates/reg-core/src/grammar.rs` (visibility only), the
  FQID users in `reg_meta_build/src` and their tests, `surface.toml`, `Cargo.lock`.
- Acceptance: full gate; build cases and fixtures byte-identical except the named
  February-bound and error-wording changes; G1 (run in 4.3; it touches `derive/chains`).
- Escalate: any other byte change, or a character that differs between Unicode 16 and
  17. Depends on: 4.1; runs alongside 4.2.

**4.4 Move the build-input modules and the fixture builder (mechanical).**

- Coordination: before 4.2 and 4.3 start, the orchestrator agrees 4.4's merge window
  with the test-audit session; 4.4 merges in it with nothing else in flight. The sed
  script goes in the PR body so test-audit branches rebase by rerunning it.
- Changes: copy the retained modules into `reg_meta_build` (private-name imports become
  imports within one module; `reg_meta/` stays untouched until 4.9a); move
  `reader_artifacts.py` to `conformance/` with its own default output directory; rewrite
  every importer outside `reg_meta`.
- **Assumption:** `canonical_json` and `canonical_sha256` stay Python inside
  `reg_meta_build`: no Rust code recomputes a build hash, Python and `serde_json` format
  floats differently, and it is a hot path (section 11).
- Paths: `reg_meta_build/{src,tests,input_data/swecov}`, `conformance/`,
  `scripts/{suggest_default_slugs,parse_lisa_docs}.py`,
  `reg_webapp/backend/scripts/fixture_db.py`,
  `reg_webapp/.claude/skills/run-reg-webapp/{catalog_fixture_db.py,dev.sh}`, the inline
  import in `.github/workflows/integration.yml`, root `pyproject.toml` (`pythonpath`),
  `surface.toml`.
- Acceptance: full gate; fixtures byte-identical (the fixture cache rebuilds cold once);
  the private-boundary test in `scripts/tests` passes.
- Escalate: any edit that is not a path rewrite. Depends on: 4.2, 4.3 and the agreed
  window.

**4.5 Deploy and CI drop the CLI.** Implements sections 7 and 8.

- Changes: the Dockerfile's `regmeta-db` stage becomes Debian slim with curl and zstd;
  `container-build.yml` resolves the tag and asset digests (as `integration.yml` does)
  and passes them as build args; the stage downloads (tag `/` URL-encoded), checks the
  SHA-256 and unpacks. The schema guard reads
  `reg_meta_build/src/reg_meta_build/{db,doc_db}.py` at the tag, falling back to
  `reg_meta/src/reg_meta/doc_db.py` for the docs schema when the builder file lacks
  `DOC_SCHEMA_VERSION` (tags before 4.4, including 0.45.0, import it from
  `reg_meta.doc_db`). The fallback carries
  `simplify: fallback for tags before 4.4; delete it in the first release after 4.4`.
  The Python reader axis and the `reg_meta/**` path filter leave
  `schema_pending_bump.py`. `integration.yml` drops the Podman job; admission is
  `zstd -d` plus the server's own admission at boot. `publish_reg_meta.yml` drops the
  PyPI build, publish and CLI smoke, keeping the dispatches (D3).
- Paths: `reg_webapp/Dockerfile`,
  `.github/workflows/{container-build,integration,publish_reg_meta}.yml`,
  `scripts/schema_pending_bump.py` and its test, `reg_webapp/DESIGN.md`.
- Acceptance: full gate; `docker build` for `global` and `swecov` at `reg_meta/v0.45.0`
  passes the entrypoint smoke; actionlint clean. Maintainer step: the next push deploys.
- Depends on: 0.45.0 deployed and running cleanly in production.

**4.6 Agent plugin on MCP.** Implements section 8 and decision 12.

- Changes: both `plugin.json` files declare the hosted server at
  `https://catalog.swecov.se/mcp`; `SKILL.md` documents the tools (from
  `conformance/cases/mcp/tools-list.json`) and the `{data, meta}` and error envelopes;
  README, PRIVACY (queries now leave the machine) and marketplace text updated, plugin
  version bumped; the 7 `skill` rows leave `surface.toml`.
- Paths: `plugins/microdata-tools-se/**`, `.claude-plugin/marketplace.json`,
  `.agents/plugins/marketplace.json`, `surface.toml`.
- Acceptance: full gate; one recorded Claude Code session lists the tools and answers a
  `search`.
- Escalate: whether the Codex manifest supports MCP; the PRIVACY wording. Depends on:
  none.

**4.7 Conformance without the Python reader.** Implements section 9.

- Changes: delete `test_{cli_scope,logical,reader,folds,selection_update}.py`,
  `cases/{cli_scope,logical,reader,update}` and `test_validate.py`'s CLI arm, the PR
  listing each deleted case beside its `api` twin; selection behaviors with no twin
  become `api` startup-refusal cases. `artifact_requests.py`,
  `test_acceptance_agreement.py` and `test_artifact*.py` call only the Rust server;
  `conftest.py` and `http_cases.py` use the builder's `open_db`. Command rows'
  `covered_by` point at the `api` twins.
- Paths: `conformance/` (except `differential/`), `surface.toml`.
- Acceptance: full gate; no `reg_meta` import left outside `differential/`. Depends on:
  4.4.

**4.8 A home for the runtime design.**

- Changes: the still-true parts of `reg_meta/DESIGN.md` go to `crates/DESIGN.md`; the
  resolver, holdings and inventory-TOML parts to `reg_meta_build/DESIGN.md`;
  `reg_schema/DESIGN.md` to `crates/reg-core/DESIGN.md`; SPA comments pointing at the
  old files are updated.
- Paths: those files, comments in `reg_webapp/frontend/src`, `ARCHITECTURE.md`.
- Acceptance: panache format and lint; the PR lists every section as moved (with its
  target) or dropped (with the reason). Depends on: none.

**4.9a Delete `reg_meta/`, `reg_schema/`, the webapp's Python member and Python G1.**

- Deletes: the three packages (`run_search_eval.py` and `search_eval.toml` move to
  `scripts/`; `dev.sh` builds its fixture through `conformance/fixture_cache.py`);
  `crates/reg-core/tools/gen_fold_corpus.py`, `publish_reg_schema.yml`,
  `scripts/check_versions.sh`, `surface.toml` and `test_api_surface.py`; the `reg_meta`
  and `reg_schema` suites in `ci.yml`; root `pyproject.toml` members, `testpaths` and
  dependencies, `reg_meta_build`'s `reg-meta` dependency and their `uv.lock` entries; in
  `ci.yml` also the `check-versions` job, the `package-integration` job
  (`reg_meta/tests/test_integration.py`) and the `reg_webapp/backend/tests` matrix
  entry; the rest of Python G1 (`driver.py`'s CLI arms, the baseline environment,
  `folds.py`, the `reg_meta/src` cache key); the transitional-inventory rows this ends.
- Changes: `reg_schema/test_corpus` moves to `crates/reg-core/tests/project/corpus/`
  (only `hashes.json` keys change); the frontend consumers follow it
  (`project_data.test.ts`'s `reg_schema/pyproject.toml?raw` import,
  `validation.test.ts`'s corpus glob, the Vite `fs` allowlist), so the frontend gate
  stays green and the data oracles keep their cases; the Python regeneration comments in
  `crates/reg-core/tests/{interval,project}.rs` go.
- Acceptance: full gate; `git grep -nE '(from|import) (reg_meta|reg_schema)\b'` returns
  nothing; `uv sync --frozen` succeeds on a clean clone; G1 (run in 4.9a) 0 differences.
- Depends on: 4.1–4.8.

**4.9b Skills and agent docs.**

- Changes: one release skill, `.agents/skills/release/SKILL.md`, with
  `.claude/skills/release` a symlink to it; a release is a crate version bump, a tag,
  the DB assets and the binaries (4.12), with the PyPI and `reg_schema` steps gone.
  Package lists updated in `CLAUDE.md`, `AGENTS.md`, `ARCHITECTURE.md`, `README.md` and
  `CONTRIBUTING.md`; the `code-cleanup` and `test-audit` skills drop the Python
  packages.
- Acceptance: panache format and lint; skill discovery in `scripts/tests`.
- Depends on: 4.9a. Merges before the next release.

**4.10 Schema major 10.0.0 and the catalog rework** (D2). Sub-packages 4.10a–e open
their PRs against one integration branch, are reviewed like any package, and the
orchestrator merges each into that branch. When all have merged, the branch lands on
main as one merge; then the maintainer runs one G2 (a full 10.0 build compared against
the 0.45.0 binary on the 9.7 asset) and cuts one release, the 10.0.0. Production deploys
pause from that main merge until the release (about 2 h), because the schema guard
treats a major mismatch as a break. No G1 runs on 4.10: a 10.0 derive refuses a 9.7
base. The item letters below are #1296's; its numbers are not copied here. Size-only
items from #1296 item 3 are optional: each joins the sub-package that owns its table and
is listed in that PR. Depends on: 4.9a; 4.10b–e also wait for test-audit's
`reg_meta_build` test rewrite to finish (#1296). 4.1–4.9 do not wait for it.

- **4.10a Major bump and unread tables.** Builder `SCHEMA_VERSION` 10.0.0, Rust `SCHEMA`
  `(10, 0)`, `FIXTURE_SCHEMA_VERSION` follows. Drop artifact tables and views that the
  Rust reader, derive and `validate_built_db` all leave unread (known: the always-empty
  `classification_same_as`, 3d.1), each confirmed against the artifact DDL in
  `reg_meta_build/db.py`. Regenerate the version-carrying goldens (the set #1305
  touched). Paths: `reg_meta_build/src/reg_meta_build/{db,validate,derive/}`,
  `crates/reg-catalog/src/lib.rs`, the goldens. Acceptance: full gate; byte-identical
  rebuild. Escalate: dropping anything the PR does not list.
- **4.10b Dense `code_id`** (#1296 2a): renumbered densely in (label, code) order;
  reader tie-breaks move to (code, label). Paths: the builder's ID assignment,
  `crates/reg-catalog/src/ops/search/`, the goldens whose order changes. Acceptance:
  full gate; byte-identical rebuild; ordering changes reviewed in the golden diff.
- **4.10c Dense `variable_id` and clustering** (#1296 2b): `variable_id` renumbered in
  (register, slug) order; `variable_state`, `expanded_state` and `browse_delivery`
  clustered physically by variable; `state_id` stays hashed. Explicit sort keys replace
  the `expanded_state_id` sorts in `ops/schema.rs` and `ops/graph.rs`, and every reader
  tie-break on a storage ID is replaced by an explicit key. Paths: the builder's ID and
  DDL code, `derive/`, `crates/reg-catalog/src/ops/`. Acceptance: full gate;
  byte-identical rebuild; no `api` response changes except reviewed orderings.
- **4.10d `data_warning` split and clustering** (#1296 2c, maintainer decision there):
  build-only codes go to the build report only; user-facing rows are stored compactly
  and clustered by (register, variable). Paths: `reg_meta_build/src` (warnings writer,
  build report), `validate.py`, the warnings reader, the goldens. Acceptance: full gate;
  the changed warning-code list and counts reviewed in the diff.
- **4.10e State merging** (#1296 2d): adjacent explicit segments on one column with
  identical resolved state facts merge into one state; never across a gap. The merged
  state keeps the earliest segment's `state_id`. `api` cases pin a merged ID and an
  absorbed ID (`not_found`). Paths: the builder's state writer, `validate.py`,
  `conformance/cases/api/`, the goldens. Acceptance: full gate; byte-identical rebuild.

**4.11 Re-pin G1 to 10.0.0.** Implements the section 4 re-pin: the artifact pin and the
baseline binary both move to the 10.0.0 tag; the differences between the old and new
pins are recorded as the maintainer's G2 found them. Paths:
`conformance/differential/config.toml`, section 4. Acceptance: G1 0 differences. Depends
on: the 10.0.0 release.

**4.12 Release binaries** (D3). A matrix workflow on `reg_meta/v*` builds `reg-meta` for
macOS arm64 and Linux x86_64 and uploads them with SHA-256 checksums. The plugin README
documents `reg-meta mcp --db` with the curl and zstd recipe for a local catalog. Paths:
`.github/workflows/`, `plugins/microdata-tools-se/README.md`, the release skill.
Acceptance: full gate; actionlint clean; one workflow run on a test tag produces both
binaries and checksums. Depends on: 4.9b. Blocks nothing.

#### Order and parallelism

```
0.45.0 tag ── 4.1 ─┬─ 4.2 ─┐
                   └─ 4.3 ─┴─ 4.4* ─ 4.7 ─┐
0.45.0 deployed ── 4.5 ───────────────────┤
checkpoint 3 ───── 4.6 ───────────────────┼─ 4.9a ─┬─ 4.9b ─ 4.12
checkpoint 3 ───── 4.8 ───────────────────┘        └─ 4.10a–e** ─ [G2 + 10.0.0] ─ 4.11
*  4.4 merges in the window agreed with test-audit before 4.2 and 4.3 start
** on one integration branch; 4.10b–e also wait for test-audit's reg_meta_build rewrite
```

Waves, at most three packages in flight:

1. 4.1, 4.6, 4.8. 4.5 takes the first free slot once 0.45.0 has deployed cleanly.
2. 4.2 and 4.3, with 4.5, 4.6 or 4.8 in the third slot.
3. 4.4 alone, in the agreed window.
4. 4.7, alongside what is left of 4.5, 4.6 and 4.8.
5. 4.9a alone.
6. 4.9b, then 4.12; 4.10a–e on the integration branch, each sub-package counting as one
   package in flight.
7. 4.11 after the release.

#### When G1 retires

- **4.1:** the Python served baseline and its `served/` mappings go. G1 compares the
  pinned 0.45.0 Rust binary with the checkout's derive and server on schema 9.7; it
  checks 4.2 and 4.3.
- **4.9a:** the Python CLI arms, the baseline Python environment and the fold sweep go.
  No Python G1 is left.
- **4.10:** no G1, because derive refuses a base from an older major; the maintainer's
  G2 covers it. No derive change merges to main from the 4.10 merge to the 4.11 re-pin,
  the only window without a real-artifact differential. Coverage there is G0's `api`
  cases with MCP equivalence, `validate_built_db`, the release artifact-conformance job
  and G2.
- **4.11:** G1 runs again on the 10.0.0 pin. From then on it runs at releases only, not
  on every derive PR (D4), and is deleted if it stops catching anything.

#### Concurrency with test-audit

- 4.4 touches test-audit's stage-9 files (`_resolved_catalog_support.py`,
  `_source_formation_support.py`, `_source_intervals_support.py`,
  `test_catalog_lineage.py`, `test_resolved_writer_{data_warnings,publication}.py`,
  `test_resolved_metadata_contracts.py`, `test_source_formation_{coverage,facts}.py`,
  `test_source_interval{s,_populations}.py`) and the shared `_build_case_runner.py`,
  `_shared_fixtures.py` and `_slugged_db.py`. Its merge window is agreed with the
  test-audit session in advance, and its sed script is in the PR body.
- Smaller overlaps: 4.3 touches `test_fqid_slug*` and `test_curation_toml_*`; 4.2
  touches `test_derive_chains.py` and the validate cases. Open PRs touching builder
  tests at writing: #1306, #1274, #1307.
- 4.10b–e start only after test-audit's `reg_meta_build` test rewrite finishes (D2).

#### Stage 4 decisions (maintainer, 2026-10-09)

1. **Checkpoint 3: go.** The Python runtime is retired (section 4 checkpoint list).
2. **#1296's catalog rework lands inside schema 10.0.0**, not as a later track: dense
   `code_id` (2a), dense `variable_id` with clustering and explicit sort keys (2b), the
   `data_warning` split and clustering (2c), state merging that keeps the earliest
   segment's `state_id` (2d), and #1296 item 3's size-only items as optional. Package
   4.10 becomes 4.10a–e on one integration branch, merged to main together, then one G2
   and one release, keeping the production deploy pause to about 2 h. The rework waits
   for test-audit's `reg_meta_build` test rewrite; 4.1–4.9 do not.
3. **Distribution.** No `fetch` run mode (decision 11 amended) and no PyPI package
   (section 8 amended). Releases ship GitHub binaries for macOS arm64 and Linux x86_64
   with SHA-256 checksums (4.12); hosted MCP is the default.
4. **G1 successor:** the pinned-Rust-release baseline (4.1). After 10.0.0 ships and
   storage is stable, G1 runs only at releases, not on every derive PR; it can be
   deleted if it stops catching anything. 4.3's period-bound and slug-wording change and
   4.4's Python `canonical_json` stay assumptions.

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
   search-parity harness as the seed of the G1 harness.

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
- **Self-validating oracle.** If G1 compared against the checkout's own reader, a
  regression moved into derive would validate itself. Mitigated by the pinned,
  separately installed baseline reader and recorded semantic exceptions.
- **Fold parity.** The main source of silent drift. Mitigated by computing folded
  columns through the same code via bindings, and a property corpus.
- **Native extension in the build.** The build needs a compiled `reg-core-py`. That is
  only for the maintainer and CI, but it is a new prerequisite.
- **Derive speed.** Derive is fast only while it stays set-based or parallel. The G1
  budget is the early warning.
- **Cache keys in the incremental build.** A key that misses an input yields a stale,
  wrong artifact. Mitigated by the periodic uncached byte-compare.
- **Unported pages unavailable in production** from 3a.9 until their slice ships
  (accepted at checkpoint 2: no users). Kept short by the slice order.
- **Pinned artifacts drifting** from weekly curation releases. Mitigated by re-pin
  packages at every checkpoint.
- **Frontend churn during the slices and stage 5.** Type regeneration will move many
  generated names.

## 13. Decisions (2026-10-07)

  | #   | Question                               | Decision                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
  | --- | -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
  | 1   | Where are catalog facts resolved? (§3) | **Compiled in the build**, in the derive step (§4). Reverses "No state/window resolution is compiled" in `reg_meta/DESIGN.md`.                                                                                                                                                                                                                                                                                                                                               |
  | 2   | What serves the webapp API? (§6)       | **Rust server** (`reg-meta serve`). The FastAPI backend is deleted once it serves no route, by package F (amended 2026-10-08; was: in stage 4).                                                                                                                                                                                                                                                                                                                              |
  | 3   | `reg_schema`? (§5)                     | **Merged into `reg-core`.** The Python package is deleted in stage 4.                                                                                                                                                                                                                                                                                                                                                                                                        |
  | 4   | CLI v4 surface                         | **Superseded by decision 11.** Its settled rules carry over to the API (§7).                                                                                                                                                                                                                                                                                                                                                                                                 |
  | 5   | MCP server mode                        | **Yes. Now the primary agent interface** (decision 11), built with each operation slice in stage 3.                                                                                                                                                                                                                                                                                                                                                                          |
  | 6   | WASM in the SPA                        | **Yes, as the last stage** (stage 5).                                                                                                                                                                                                                                                                                                                                                                                                                                        |
  | 7   | Parallel per-register resolve (§11)    | **Yes**, after the family-scan fix (landed in #1181).                                                                                                                                                                                                                                                                                                                                                                                                                        |
  | 8   | Where this plan lives                  | **Its own root tracker**, with the governance rule amended to allow one tracker per concurrent refactor.                                                                                                                                                                                                                                                                                                                                                                     |
  | 9   | How to avoid full rebuilds per step    | **Base/derive split, pinned artifacts, three gates (G0–G2) with budgets, incremental base build** (§4, §11).                                                                                                                                                                                                                                                                                                                                                                 |
  | 10  | Who the runtime is designed for        | **Agents and the webapp only.** No human-oriented features (text output, prompts, progress, notebook import) while building; re-evaluated after stage 5 (§7).                                                                                                                                                                                                                                                                                                                |
  | 11  | Query CLI?                             | **None.** One operation set exposed over HTTP and MCP; the binary has run modes only (`serve`, `mcp`) (§6, §7) (amended 2026-10-09; was: also `fetch`).                                                                                                                                                                                                                                                                                                                      |
  | 12  | Where agents reach MCP                 | **Hosted and local.** Remote MCP endpoint on `serve` at catalog.swecov.se; `reg-meta mcp` over stdio for offline use and private steward catalogs (§7), from a GitHub release binary (§8; amended 2026-10-09: no PyPI package). The hosted endpoint is the default.                                                                                                                                                                                                          |
  | 13  | Tooling and versions                   | **Latest everywhere.** Newest stable versions of languages, crates, packages and SDKs, and modern methods; no compatibility work for older toolchains. Windows later, via hosted MCP unless a local binary is effortless (§8).                                                                                                                                                                                                                                               |
  | 14  | How agents execute it                  | **Execution protocol (§4).** Work packages in this file, written per stage; mechanical done (acceptance + gates + fresh-agent review); escalate-don't-decide list; small squash PRs to main; ≤3 in flight; four maintainer checkpoints. Stage 2 builds only the derive framework; slices own their tables.                                                                                                                                                                   |
  | 15  | How the SPA moves to the Rust server   | **Per slice.** Each slice ends by switching the SPA's calls for its routes to the Rust server and deleting the replaced FastAPI routes; slices 3b and 3d share one catalog-page cutover, C (amended 2026-10-08). From 3a.9 production runs the Rust server alone, and unported pages are unavailable until their slice ships (checkpoint 2: no users, so no proxy or edge routing). Package F deletes the empty app (amended 2026-10-08; was: stage 4 retires what remains). |
  | 16  | Full-text index folding                | **Pre-folded with `fold_search`** (checkpoint 1): one fold definition in `reg-core` for the build and the reader instead of `fold_search` plus SQLite's `unicode61` folding; `unicode61 remove_diacritics 0` stays as the tokenizer only. Applied in slice 3a to the catalog indexes; `doc_fts` with the docs slice (checkpoint 2), folded in the docs build (`doc_db.py`, package 3b.6).                                                                                    |
  | 17  | Search result shape                    | **One ranked list per call** (checkpoint 1); the SPA makes one call per typed group.                                                                                                                                                                                                                                                                                                                                                                                         |
