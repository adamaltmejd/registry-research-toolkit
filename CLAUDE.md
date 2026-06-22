# Registry Research Toolkit

Multi-package workspace for Swedish register research: catalog metadata, schema
validation, and MONA bundle / mock-data generation. See `ARCHITECTURE.md` for the
cross-package design and `REFACTOR_SPEC.md` for the remaining (post-A5) work.

## Packages

- `reg_meta` (CLI `reg-meta`) — search and query registry metadata.
- `reg_meta_build` (CLI `reg-meta-build`) — build the reg_meta SQLite DBs from SCB
  exports (maintainer-only).
- `reg_schema` (library) — `project_data.json` schema and structural validator.
- `reg_monabundle` (library) — MONA bundle build + runtime + PII scanner.
- `reg_webapp` — FastAPI backend + Svelte SPA: catalog browse + project authoring.
- `mock_data_wizard` (CLI `mock-data-wizard`) — local mock-data generation; pending
  rename to `reg_mockdata` + reg_meta-dep removal (see `REFACTOR_SPEC.md`).

## MONA constraint

[MONA](https://www.scb.se/mona) is Statistics Sweden's microdata platform. Agents are
not allowed on MONA. **PII must never leave MONA — only aggregate statistics are
exported.** `mock_data_wizard` (post-refactor: `reg_monabundle` + `reg_mockdata`)
bridges agentic local work to MONA projects.

# Governance

- `DESIGN.md` per package documents design rationale and constraints.
- No frozen specs or **permanent** implementation trackers — design decisions live in
  DESIGN.md, implementation history lives in git.
- **Exception**: a multi-PR refactor spanning weeks may keep a single root-level tracker
  (currently `REFACTOR_SPEC.md`, scoping the remaining post-A5 work; the earlier
  `MIGRATION_PLAN.md` was retired once A5 shipped) for cross-PR coordination. The
  tracker is **scoped and self-deleting**: it ships with an explicit completion gate
  (e.g. "deleted when stage X ships"), gets deleted at that gate, and never outlives the
  refactor it tracks. Per-package DESIGN.md notes for the same effort are still
  preferred where the scope is package-local.

# Maturity and compatibility

- Pre-v1, no external users — break things freely if it benefits the long-term design.
- Do not write migration code, shims, deprecation wrappers, or backwards-compatibility
  layers. If something needs to change, change it directly.
- Do not preserve old code "just in case." Dead code gets deleted.

# Coding principles

- Deterministic behavior with explicit seed/config.
- Fail fast with actionable errors and stable exit codes.
- Keep domain logic separate from IO/prompts/integrations.
- Validate JSON contracts at read/write boundaries.
- Avoid leaking sensitive row-level content.

## Reuse first, build last

Apply the global-CLAUDE.md ladder (no change → existing capability → stdlib/platform →
installed dep → minimal new code → new dep; optimize for risk and maintenance, not line
count) on every change. Two repo-specific notes:

- **The failure mode here is leaf-helper duplication, not new-module inflation.** A new
  adapter/route legitimately needs a new module, but a small format-agnostic leaf inside
  it (a validator, a write loop, a clamp gate) gets re-pasted instead of hoisted. Before
  writing a leaf, check whether an internal capability already does it
  (`reg_meta_build`'s `_curation.py` and `db.py`, `reg_webapp`'s `query_input.py` are
  typical homes). Extend it; don't re-type it.
- **The ladder cuts both ways.** This repo *under*-uses libraries as often as it
  over-builds (e.g. `reg_meta_build` hand-rolls TOML validators though it already ships
  Pydantic for its build-time IR — Stack makes no-Pydantic *hard* only for the
  amalgamated bundle, soft elsewhere). "Installed dep solves it → use it" binds as hard
  as "don't add a dep for what a few lines do" — add one only when it removes more
  complexity than it adds.

**Never simplify away** the load-bearing guards: PII/MONA confinement, k-anonymity /
disclosure control, determinism / byte-identity, JSON-contract validation, fail-fast. A
shorter diff that drops one of these isn't simpler, it's broken.

Mark a deliberate shortcut with a `simplify:` comment naming its ceiling and upgrade
trigger
(`# simplify: O(n^2) scan, index it if the candidate set passes a few thousand`), so a
simplification reads as intent and a deferral can't silently rot.

**Altitude split:** "does this need to exist / does an existing subsystem or library
subsume it?" is a plan-time call (the lead, or whoever scopes the work); the leaf-level
reuse and simplicity craft is the implementer's — see the pr-pipeline skill (the lead's
altitude duties) and the implementer role (the leaf craft).

# Python conventions

- Runtime deps live in each package's `pyproject.toml`; dev deps live only in the
  workspace-root `pyproject.toml`.
- Add with `uv add` (runtime) / `uv add --dev` (dev) — don't hand-edit `pyproject.toml`
  for new deps; uv writes to the right PEP 735 group. Hand-editing is fine for bumping
  an existing constraint (then `uv lock`).
- One-off tools (not project deps): `uvx --from <package> <tool>` — e.g.
  `uvx --from pre-commit==4.6.0 pre-commit run --all-files`.
- Refresh lockfile with `uv lock --upgrade`; CI uses `uv sync --frozen`.
- 7-day minimum release age is project policy: `exclude-newer = "7 days"` in the root
  `pyproject.toml` `[tool.uv]`, recorded in `uv.lock`'s `[options]` block. It applies on
  every checkout with no global uv config needed. Don't remove either side: dropping the
  pyproject setting makes plain `uv run` discard the committed lock on checkouts without
  a matching global config.
- The workspace floor is uniformly `>=3.14` (ruff `target-version = py314` to match),
  after a coordinated bump (#682, 2026-06-22). The MONA runner's actual ceiling is
  WinPython 3.13.7 — **deliberately not enforced right now**: the runner floor is
  deferred to REFACTOR_SPEC §10a, which rebuilds the runner standalone. As a consequence,
  the amalgamated runner slices (`reg_monabundle/runtime/classify.py` and `summarize.py`)
  now contain 3.14-only PEP 758 syntax (`except A, B:`) that would SyntaxError on MONA's
  3.13.7 — §10a must reconcile this. See `mock_data_wizard/DESIGN.md` "MONA Python
  runtime" for the probe details.

## Stack

Current state (the Model A refactor through A5 has shipped). See `ARCHITECTURE.md` for
the cross-package invariants and each `<package>/DESIGN.md` for the detail;
`REFACTOR_SPEC.md` tracks the remaining work.

- **Library packages** (`reg_meta`, `reg_monabundle`, `reg_mockdata`, `reg_meta_build`):
  - Modeling: `@dataclass`. The **hard** no-Pydantic + stdlib-module-level-imports rule
    binds **every slice amalgamated into the bundle** — the lightweight
    `constants`/`validate`/`scan` slices **plus** `reg_monabundle.runtime.*`, i.e.
    everything lifted into the uploaded `.py` (`validate` runs at bundle load on MONA,
    `scan` gates exports there), not the runtime alone — each must stay liftable into
    MONA's offline WinPython env. `reg_meta`'s no-Pydantic is a **soft** preference
    (import-ergonomics — importable from Jupyter/scripts without pulling pydantic-core —
    plus an aspirational query-layer port), **not** a MONA requirement: reg_meta is
    already absent from MONA-side code. Decided 2026-06-22 (#680); see
    `REFACTOR_SPEC.md` §10a.
  - Database: stdlib `sqlite3` with raw SQL; DDL string in `db.py`; `SCHEMA_VERSION`
    constant gates compatibility; regenerate-not-migrate. **No SQLAlchemy/Alembic** — DB
    is read-mostly, single-backend; an ORM would add overhead with no benefit.
  - Analytical queries: DuckDB where needed.
  - CLI: argparse. No click/typer.
- **`reg_schema`** (authoring/validation surface — exception to the no-Pydantic rule):
  Pydantic v2. Reasons: (1) it's the canonical structural validator for
  `project_data.json` — Pydantic's declarative field/model validators are the right
  tool; (2) FastAPI in `reg_webapp/backend/` consumes `reg_schema` models directly as
  response models, killing the 1:1 wrapper drift surface; (3) `model_json_schema()`
  gives the SPA's TypeScript codegen a free, always-correct schema source. Runtime
  escape valve: the MONA bundle does **not** ship Pydantic; bundle-build runs the
  Pydantic validator as the gate, then converts validated `Source` → dataclass
  `LoadedSpec` (`reg_monabundle.runtime.spec`) which the bundle amalgamates instead. See
  `reg_schema/DESIGN.md` and `reg_monabundle/DESIGN.md` for the boundary.
- **Web backend** (`reg_webapp/backend/`): FastAPI + Pydantic REST. `reg_schema`
  Pydantic models are response models directly (no wrapper layer). For `reg_meta`
  (dataclass-based) responses, the backend defines per-endpoint Pydantic response
  wrappers — the only place 1:1 wrappers remain.
- **Web frontend** (`reg_webapp/frontend/`): Svelte 5 + Vite + TypeScript, bun-managed.
  TS types codegen'd from FastAPI's `openapi.json`.
- **Tests**: pytest + pytest-xdist; `@pytest.mark.integration` opts into
  Docker-requiring tests. Build/parse coverage is fully synthetic (no gitignored real
  SCB/SOS data) and runs the full structural validator
  (`validate_built_db(corpus=False)` — every invariant except the real-corpus volume
  gate). Real-corpus drift is surfaced by a maintainer's actual `build-db`, which
  validates by default with `corpus=True` (opt out with `--no-validate`).
- **Type checking**: `uvx --from ty==0.0.44 ty check` (Astral, beta). Blocking in CI;
  pinned via `uvx` so CI, pre-commit, and cached Codex environments use the same
  checker. `ty` moves quickly, so bump this pin deliberately/frequently. Not a dev dep —
  keep `pyproject.toml` clean.
- **MONA bundle runtime deps are expensive**: `reg_monabundle.runtime.*` amalgamates
  into a single file uploaded to MONA. Each added runtime dep must already be in MONA's
  WinPython env (see `mock_data_wizard/DESIGN.md`). Prefer stdlib for runner-bound code.

`mock_data_wizard`'s old local authoring path (editor/server/`ui` subcommand and the
`web/` SPA) is fully deleted, superseded by `reg_webapp`; `classify.py` was **moved** to
`reg_monabundle/runtime/classify.py` (not deleted — it backs the bundle's runtime
classification). Don't revive that path — extend the new packages.

# Run (dev servers)

- `reg_webapp` local dev (FastAPI :8000 + Vite :5173): the `/run-reg-webapp` skill
  (`reg_webapp/.claude/skills/run-reg-webapp/`) has the verified launch steps + a
  Playwright driver for smoke/screenshots; `.claude/launch.json` registers both servers
  for `preview_start` (names `reg-webapp-backend` / `reg-webapp-frontend`).

# Lint and test

- `uv run ruff check` — python lint
- `uv run ruff format --check` — python format check
- `uvx --from panache-cli==2.51.0 panache format --check .` — markdown format check
  (config in `.panache.toml`; drop `--check` to fix)
- `uvx --from panache-cli==2.51.0 panache lint --check .` — markdown lint
- `uv run python -m pytest` — all tests (pytest discovers per-package via root pyproject
  `testpaths`)
- `uv run python -m pytest reg_meta/` — narrow to a single package
- `reg_meta_build/docs/lisa/*.md` are build artifacts — fix
  `scripts/parse_lisa_docs.py`, not the output

# Issue tracker

GitHub Issues is the coordination surface — keep it queryable and connected, not a pile
of prose. Before filing, **search open AND closed issues**
(`gh issue list --state all --search "<keywords>"`) for an existing match: extend or
comment on it rather than opening a duplicate, and read its relationships so you know
where the new work sits in the graph.

**Title** — mirror the commit convention: `<type>(<package>): <imperative summary>`
(e.g. `feat(reg_meta_build): …`, `fix(reg_webapp): …`). Append `(#<epic>)` when the
issue is part of a tracked epic.

**Labels — required on every issue:**

- exactly **one area label** — a package (`reg_meta`, `reg_meta_build`, `reg_schema`,
  `reg_monabundle`, `reg_webapp`, `mock_data_wizard`) or `cross-package`;
- a **type** — `enhancement`, `bug`, or `documentation`;
- `epic` on a tracking issue; `blocked` while an open dependency stalls it (remove once
  cleared).
- Lanes (the ad-hoc S/L/G/… streams) live in the epic/tracker prose, **not** as labels —
  they churn too fast to maintain as a taxonomy.

**Optional** — `priority:high` / `priority:low` (absence = normal; **at most one**). The
maintainer's "what's most important next" signal: `/plan-lanes` ranks by priority bucket
**first** (unblocking-power breaks ties within a bucket). Coarse and stable enough to be
a label — unlike the churny S/L/G lanes above — so it's machine-sortable, not buried in
prose. Hygiene flags more than one priority label.

**Body** — use the house skeleton, dropping any section that doesn't apply (don't pad):
`Problem`/`Context` · `Approach` · `Scope` (In / Out) · `Relationships` · `Touches` ·
`Open questions` · `Non-goals`. Worked examples and concrete file paths earn their
space.

**Relationships — make every cross-issue tie explicit** so the graph is reconstructable
from text, not implied by a title suffix. In a `Relationships` block, name each tie with
a keyword + number:

- `Part of #N (epic)` · `Depends on #N` / `Blocked by #N` · `Follow-up to #N` ·
  `Supersedes #N` · `Related to #N`.
- Wire epic ↔ child as **native sub-issues**, not only prose:
  `gh issue edit <child> --parent <epic>` (or
  `gh issue edit <epic> --add-sub-issue <child>`). Prefer this over a hand-maintained
  checklist-in-comments tracker — a latest-comment-is-truth surface goes stale and
  forces readers to re-read every comment.
- PR → issue closure uses closing keywords in the **PR** body (`Closes #N` /
  `Fixes #N`), never in the issue.

**Touches** — when the issue will change code, add a fenced `touches` block (info string
`touches`) listing the repo-relative paths or globs the work is expected to modify, one
per line (`#` line-comments allowed; paths that don't exist yet are fine). It lets the
sequencing pass compute which issues can run in parallel by set-intersection instead of
re-reading the tree. Omit it for discussion- or decision-only issues.

````
```touches
reg_meta_build/src/reg_meta_build/sources/sos.py
reg_meta_build/concept_groups.toml
```
````

**Epics** — a tracking issue labeled `epic` that owns its children as sub-issues; each
child carries its own scope. An epic's plan lives in its **body** — the generated
`<!-- plan-sequence -->` / `<!-- plan-lanes -->` blocks plus thin editorial (e.g.
Parked, Self-close); decisions live on the child issues. **Don't post
status-consolidation comments on an epic** — a recurring "current state" comment is the
retired prose-as-state anti-pattern (latest-comment-is-truth goes stale); comments are
for one-off notes, not the running plan.

**Sequencing is generated, not hand-written.** An epic's status — **ready / running /
blocked / parallel-safe / pending-release** — is rendered by `/plan-sequence`
(`scripts/plan_sequence.py`) into a `<!-- plan-sequence -->` block in the epic body. To
see what's ready to pick up and which issues are file-disjoint, **read that block** (or
re-run `/plan-sequence`); **don't hand-edit inside the markers** — it's overwritten. The
lane/decision narrative *around* the block is the editorial layer you do edit. The block
is **event-refreshed by CI** — `plan-sequence.yml` runs `--write` on every issue/PR
event (plus a daily cron safety-net), so it tracks reality without waiting for a human
or the loop. CI + cron are the **sole writers** of this block; `/issue-pulse` (run via
`/loop`) reads it read-only (`--tick`) and reports deltas, but no longer writes it. The
sequencing epic is #328.

**Lanes are ranked agentically.** `/plan-sequence` gives only the deterministic *floor*
— file-disjoint groups by `touches` set-intersection. **`/plan-lanes`** is the judgment
layer on top: it reads the issue bodies to fold in what set-intersection can't see
(semantic conflicts with no file overlap, implicit blockers, what coheres into one
PR-stream), then returns **ranked, parallel-safe candidate lanes** as markdown — ranked
by `priority:*` bucket first, then unblocking-power + size. It runs **forked** (its own
context), so callers get the ranked lanes back without the corpus-reading bloating
theirs: `/issue-pulse` re-ranks when the lanes go **stale**; `/pr-pipeline next`
consumes the ranking to pick a lane instead of composing one from raw candidates; you
can run it on demand. `/plan-lanes` is itself **read-only** — it ranks and returns,
never editing issues or opening PRs. `/issue-pulse` then **persists** the ranking into a
second generated block — `<!-- plan-lanes -->`, alongside `<!-- plan-sequence -->` — via
`plan_sequence.py --write-lanes` (single writer; `/pr-pipeline` only reads). Staleness
is deterministic and **three-way**: the lanes block stamps the ready/running sets it was
ranked against **plus a content signature over the lane-affecting projection** (the free
candidate set + each non-running issue's status, area, `touches`, `priority`, and full
`Relationships` graph), and `--lanes-stale` / `--tick` compares both to the live state —
necessary because once CI event-refreshes the projection, the projection delta no longer
signals that the work moved (the refresh absorbed it). The signature extends staleness
past membership: an area relabel, a `touches` edit, a `priority` change, or any
`Relationships` edit (including a blocked issue's `Blocked by` rewrite, which shifts
which ready issue has unblocking power) re-shapes the lane graph without moving a
section, yet still **re-ranks** (exit 1). But a `running` issue is in-flight, never a
lane member, so a delta confined to the running set (a PR merges, its issue closes, the
claim clears) can't change lane content — it routes to a cheap **re-stamp** (exit 2,
`--restamp-lanes`: rewrite the basis stamp, keep the ranked lanes) instead of paying for
a re-rank. The content signature excludes running issues' own projection precisely so
this common tick doesn't trip a re-rank; their one content effect — holding a ready
candidate — is folded in via the free set, so a merge that unholds a candidate still
re-ranks. Same edit rule as the projection: **don't hand-edit inside the markers** —
it's overwritten.

**Enforcement** — `scripts/check_issue_hygiene.py` (run by `.github/workflows/`
`issue-hygiene.yml`, **read-only** — `issues:read`) checks these rules: required labels,
at most one `priority:*` label, resolvable relationship targets, `blocked`-label /
sub-issue ↔ `Part of` agreement, `touches`-glob resolution, plus drift alerts (a merged
PR that left its issue open; `reg_meta_build` DB content changed since the last
`reg_meta_build/v*` tag, i.e. a release is pending). The write-capable refresh lives in
a **separate** workflow (`plan-sequence.yml`, `issues:write`) so the hygiene job's
read-only guarantee stays intact.

**Marking work in-flight** — when you start developing an issue — in `/pr-pipeline` **or
ad-hoc** — open a **draft PR** early whose body has `Closes #N`. That is the in-flight
claim: the sequencing view counts an open PR's `Closes #N` as work-in-progress, so
concurrent dispatches skip that issue and anything touching its files. The draft PR is
the marker (no `in-progress` label); merging or closing it clears the claim. For a known
multi-issue effort, open the drafts up front; open more as new work surfaces.

# Git

- Never run `git commit --no-verify`, `git commit -n`, or `git push --no-verify`. If a
  pre-commit hook fails, fix the underlying issue rather than bypassing.

## PR merge gate

Green CI alone is never sufficient to merge. Scale the rest to the PR's size and risk
(see `.claude/skills/pr-pipeline/SKILL.md` for the full pipeline version):

- **Independent review** — every PR gets at least one review independent of its author.
  For small, low-risk PRs the Codex/Copilot bot reviews can be enough; larger or riskier
  PRs additionally need an independent Claude review pass: `/code-review` (effort scaled
  to risk; this is what `/pr-pipeline` runs), or the lighter `reviewer` subagent for
  smaller/ad-hoc reviews. A subagent review reports its findings directly to the
  orchestrating session — not as PR comments. Address every finding: fix it, or dismiss
  it with a stated reason — findings can be wrong or immaterial, but none may go
  unanswered. Review is iterative: if fixes introduce substantial new changes, run
  another round on the new diff — repeat until a round produces nothing material.
- **Bot-review window** — after the PR is ready (and after each substantive push), give
  Codex/Copilot a bounded window. Poll for the **bot's own signal on the current HEAD**,
  NOT for CI finishing — CI is a separate gate that usually goes green far sooner, so a
  poller that exits on CI-done has not actually given the bot its window. The signal is
  one of: a submitted review/comment; Codex's clean verdict — a 👍 reaction on the PR
  body from `chatgpt-codex-connector[bot]` with no review submitted (invisible to
  `gh pr view`; check `gh api repos/<owner>/<repo>/issues/<pr>/reactions`); or a 👀
  reaction there meaning Codex is still reviewing — never conclude the window or merge
  while 👀 is present. Codex can also run **out of tokens**, in which case it posts an
  issue comment like "You have reached your Codex usage limits for code reviews" (in
  `gh api repos/<owner>/<repo>/issues/<pr>/comments`, not a review or reaction) — treat
  that as a definitive end-of-wait, not a blocker. Otherwise \~10 min with no signal is
  the ceiling — bots may skip a push entirely (Codex auto-reviews on open/ready only; a
  verdict on a new HEAD must be requested by commenting `@codex review`). Only trust a
  verdict timestamped after the latest push; absence at the ceiling is not a blocker.
- **Real-data validation** when build-pipeline or DB content changed: run a real-seed
  `reg-meta-build build-db` **on the PR head** (validation runs by default), not just
  fixture tests. The untracked seed lives only in the main checkout — from a worktree,
  point at it with an absolute `--input-dir <main-checkout>/reg_meta_build/input_data/`.
- **Visual verification** when the PR changes rendered output (`reg_webapp/frontend/**`,
  or any view / component / style the SPA renders). This is the UI analog of real-data
  validation — **required, not optional**, for rendered changes. Headless checks
  (`bun run lint/check/test/build`) never render a pixel, so green `bun` is not
  sufficient: run the app and *look*. Easiest is the **one-shot driver** —
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke` (or `dev.sh shot <route>` for
  the changed views): it picks free ports, renders from THIS checkout's `.venv`
  (worktree-correct), screenshots to `/tmp/reg-webapp-shots/`, and **tears the servers
  down on exit** — no port collisions, no leaked servers. For interactive poking use
  `preview_start` + `preview_snapshot` / `preview_click` / `preview_resize` (single-host
  fixed-port; in a worktree it serves *main's* code, so prefer `dev.sh` there). Attach a
  screenshot as the proof, the same way a build PR attaches its `build-db`. If the
  `/web-design-reviewer` skill is installed, also use it for a structured design-quality
  pass (and `/frontend-design` when authoring new UI).
- **Stale-head check**: before merging, confirm the PR's `headRefOid` equals the local
  branch tip; after merging, confirm the PR's changes are actually present on main — the
  GitHub API can capture a stale head and silently drop just-pushed commits. (Comparing
  the merge commit's tree to the branch tip works only when the base didn't advance in
  between.)

**Agent-driven PR work outside `/pr-pipeline`:** when you build a change end to end
without the user invoking the skill, run the same shape — plan → implement →
`/code-review` (effort scaled to risk) → docs — then **mark the PR ready for review**
(`gh pr ready <pr>`) and hand it back for this gate. Marking it ready is the step that
**starts the bot-review window** — Codex auto-reviews on the open/ready transition,
never on a draft, so a PR handed back as a draft stalls the gate. Leave a PR draft only
while it's genuinely still being built (the draft is also the in-flight claim). Once
ready, you may poll and report the bot-review window (above). But **don't merge on your
own initiative** — the *merge decision* is the human's (they invoke `/pr-pipeline`, or
tell you to merge). `/pr-pipeline` is the flow that carries a PR through to merge, and
it's user-invoked by design.

# Layout

For per-package design rationale, see `<package>/DESIGN.md` (the reg_meta object model
lives in `reg_meta/DESIGN.md`; per-provider source-delivery shapes in
`reg_meta_build/DESIGN.md`). For the cross-package design (topology, dependency graph,
repo-wide invariants), see `ARCHITECTURE.md`; for the remaining post-A5 work, see
`REFACTOR_SPEC.md`.
