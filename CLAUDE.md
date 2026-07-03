# Registry Research Toolkit

Multi-package workspace for Swedish register research: catalog metadata, schema
validation, and project authoring. See `ARCHITECTURE.md` for the cross-package design
and `REFACTOR_SPEC.md` for the remaining (post-A5) work.

## Packages

- `reg_meta` (CLI `reg-meta`) — search and query registry metadata.
- `reg_meta_build` (CLI `reg-meta-build`) — build the reg_meta SQLite DBs from SCB
  exports (maintainer-only).
- `reg_schema` (library) — `project_data.json` schema and structural validator.
- `reg_webapp` — FastAPI backend + Svelte SPA: catalog browse + project authoring.

The `reg_monabundle` (MONA bundle build + runtime + PII scanner) and `mock_data_wizard`
(local mock-data generation) packages have been archived to branch
`archive/mona-subsystem` (tag `mona-subsystem-pre-rebuild`) and removed from `main`,
pending a from-scratch MONA rebuild. See `REFACTOR_SPEC.md` and tracking issue #707.

## MONA constraint

[MONA](https://www.scb.se/mona) is Statistics Sweden's microdata platform. Agents are
not allowed on MONA. **PII must never leave MONA — only aggregate statistics are
exported.** This is a domain invariant that remains true regardless of the tooling
state.

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

Apply the global-CLAUDE.md/AGENTS.md ladder (no change → existing capability →
stdlib/platform → installed dep → minimal new code → new dep; optimize for risk and
maintenance, not line count) on every change. Two repo-specific notes:

- **The failure mode here is leaf-helper duplication, not new-module inflation.** A new
  adapter/route legitimately needs a new module, but a small format-agnostic leaf inside
  it (a validator, a write loop, a clamp gate) gets re-pasted instead of hoisted. Before
  writing a leaf, check whether an internal capability already does it
  (`reg_meta_build`'s `_curation.py` and `db.py`, `reg_webapp`'s `query_input.py` are
  typical homes). Extend it; don't re-type it.
- **The ladder cuts both ways.** This repo *under*-uses libraries as often as it
  over-builds (e.g. `reg_meta_build` hand-rolls TOML validators though it already ships
  Pydantic for its build-time IR). "Installed dep solves it → use it" binds as hard as
  "don't add a dep for what a few lines do" — add one only when it removes more
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
  after a coordinated bump (#682, 2026-06-22).
- A repo-root `.python-version` pins the interpreter to `3.14` so that **project-less
  `uv run --no-project` tooling** (the issue-hygiene / plan-sequence scripts in CI and
  the issue-tracker skills) resolves the `>=3.14` floor instead of the runner's ambient
  Python. `--no-project` skips workspace discovery, so without this pin the
  `requires-python` floor is bypassed for those runs and the 3.14-only PEP 758 syntax in
  `scripts/` would `SyntaxError` on a < 3.14 runner.

## Stack

Current state (the Model A refactor through A5 has shipped). See `ARCHITECTURE.md` for
the cross-package invariants and each `<package>/DESIGN.md` for the detail;
`REFACTOR_SPEC.md` tracks the remaining work.

- **Library packages** (`reg_meta`, `reg_meta_build`):
  - Modeling: `reg_meta` uses frozen Pydantic v2 (`_CatalogModel` base) so FastAPI can
    consume its catalog models directly (adopted #681, 2026-06-22); `reg_meta_build`
    uses Pydantic v2 `_IRBase` models for the build-time IR core and
    `@dataclass(frozen=True)` for local value types in feature modules.
  - Database: stdlib `sqlite3` with raw SQL; DDL string in `db.py`; `SCHEMA_VERSION`
    constant gates compatibility; regenerate-not-migrate. **No SQLAlchemy/Alembic** — DB
    is read-mostly, single-backend; an ORM would add overhead with no benefit.
  - Analytical queries: DuckDB where needed.
  - CLI: argparse. No click/typer.
- **`reg_schema`** (authoring/validation surface): Pydantic v2. Reasons: (1) it's the
  canonical structural validator for `project_data.json` — Pydantic's declarative
  field/model validators are the right tool; (2) FastAPI in `reg_webapp/backend/`
  consumes `reg_schema` models directly as response models, killing the 1:1 wrapper
  drift surface; (3) `model_json_schema()` gives the SPA's TypeScript codegen a free,
  always-correct schema source. See `reg_schema/DESIGN.md`.
- **Web backend** (`reg_webapp/backend/`): FastAPI + Pydantic REST. `reg_schema`
  Pydantic models are response models directly (no wrapper layer). `reg_meta`'s frozen
  Pydantic catalog models are consumed directly — the webapp's `kind`-discriminated node
  models embed them as field types (collapsed in #681). The only remaining 1:1 wrapper
  is reg_schema's `ValidationResult`/`ValidationIssue`.
- **Web frontend** (`reg_webapp/frontend/`): Svelte 5 + Vite + TypeScript, bun-managed.
  TS types codegen'd from FastAPI's `openapi.json`.
- **Tests**: pytest + pytest-xdist; `@pytest.mark.integration` opts into
  Docker-requiring tests. Build/parse coverage is fully synthetic (no gitignored real
  SCB/SOS data) and runs the full structural validator
  (`validate_built_db(corpus=False)` — every invariant except the real-corpus volume
  gate). Real-corpus drift is surfaced by a maintainer's actual `build-db`, which
  validates by default with `corpus=True` (opt out with `--no-validate`). Hypothesis
  (dev-only) is used for property-based tests on invariant-heavy surfaces
  (`test_*_properties.py` in `reg_meta` and `reg_meta_build`), additive to the
  example/snapshot suites.
- **Type checking**: `uvx --from ty==0.0.54 ty check` (Astral, beta). Blocking in CI;
  pinned via `uvx` so CI, pre-commit, and cached Codex environments use the same
  checker. `ty` moves quickly, so bump this pin deliberately/frequently. Not a dev dep —
  keep `pyproject.toml` clean.

# Run (dev servers)

- `reg_webapp` local dev (FastAPI + Vite with an `/api` proxy): the `/run-reg-webapp`
  skill (`reg_webapp/.claude/skills/run-reg-webapp/`) has the verified launch steps + a
  Playwright driver for smoke/screenshots. `.claude/launch.json` registers a single
  `reg-webapp` config for `preview_start` (its entry point is `dev.sh preview`, so
  `autoPort` makes parallel sessions collision-free and the proxy is auto-wired); for
  one-shot screenshots use `dev.sh smoke` / `dev.sh shot`.

# Lint and test

- `uv run ruff check` — python lint
- `uv run ruff format --check` — python format check
- `uvx --from panache-cli==2.59.0 panache format --check .` — markdown format check
  (config in `.panache.toml`; drop `--check` to fix)
- `uvx --from panache-cli==2.59.0 panache lint --check .` — markdown lint
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
  cleared); `parked` while maintainer-deferred work should stay out of `/plan-lanes`
  dispatch without inventing a blocker.
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
Self-close); decisions live on the child issues. **Don't post status-consolidation
comments on an epic** — a recurring "current state" comment is the retired
prose-as-state anti-pattern (latest-comment-is-truth goes stale); comments are for
one-off notes, not the running plan.

**Sequencing is generated, not hand-written.** An epic's status — **ready / running /
parked / blocked / parallel-safe / pending-release** — is rendered by `/plan-sequence`
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
section, yet still **re-ranks** (exit 1). A `parked` issue is signed as non-running
work, so adding/removing `parked` re-ranks and keeps it out of dispatch. But a `running`
issue is in-flight, never a lane member, so a delta confined to the running set (a PR
merges, its issue closes, the claim clears) can't change lane content — it routes to a
cheap **re-stamp** (exit 2, `--restamp-lanes`: rewrite the basis stamp, keep the ranked
lanes) instead of paying for a re-rank. The content signature excludes running issues'
own projection precisely so this common tick doesn't trip a re-rank; their one content
effect — holding a ready candidate — is folded in via the free set, so a merge that
unholds a candidate still re-ranks. Same edit rule as the projection: **don't hand-edit
inside the markers** — it's overwritten.

**Enforcement** — `scripts/check_issue_hygiene.py` (run by `.github/workflows/`
`issue-hygiene.yml`, **read-only** — `issues:read`) checks these rules: required labels,
at most one `priority:*` label, resolvable relationship targets, `blocked` / `parked`
status-label agreement, sub-issue ↔ `Part of` agreement, `touches`-glob resolution, plus
drift alerts (a merged PR that left its issue open; `reg_meta_build` DB content changed
since the last `reg_meta_build/v*` tag, i.e. a release is pending). The write-capable
refresh lives in a **separate** workflow (`plan-sequence.yml`, `issues:write`) so the
hygiene job's read-only guarantee stays intact.

**Ingestion trust gate** — this repo is public, so automation reads issue/PR content
**only** through `scripts/gh_issue.py`, a fail-closed maintainer-author trust gate:
issues/comments not authored by the maintainer, and fork-PR `Closes #N` claims, are
dropped rather than surfaced to a model or rendered into the epic body / candidate
floor. `plan_sequence.py`'s candidate floor and fork-PR closing-claim path route through
it, and the skills read issues via `gh_issue.py view`. Raw `gh issue view` /
`gh issue list` without `--search` (and `gh api .../issues`, `gh search issues`)
model-reads in skill files are forbidden, enforced by
`scripts/tests/test_skill_gh_reads.py`.

**Marking work in-flight** — when you start developing an issue — in `/pr-pipeline` **or
ad-hoc** — open a **draft PR** early whose body has `Closes #N`. That is the in-flight
claim: the sequencing view counts an open PR's `Closes #N` as work-in-progress, so
concurrent dispatches skip that issue and anything touching its files. The draft PR is
the marker (no `in-progress` label); merging or closing it clears the claim. For a known
multi-issue effort, open the drafts up front; open more as new work surfaces. A pipeline
lane additionally registers a **pipeline slot** —
`$XDG_STATE_HOME/registry-research-toolkit/pipeline-slots/<worktree-slug>.json`, the
machine-local concurrency ledger (max 3 parallel pipeline agents) the chief-of-staff's
watcher gates dispatch on; the chief-of-staff releases the slot when the lane's PRs are
all merged/closed. The slot file also carries **agent ownership** — `surface`
(`claude`/`codex`) and the owning `session` id — so the chief-of-staff routes a
follow-up straight to the owning session from the ledger instead of a fuzzy thread
search (see the pr-pipeline and chief-of-staff skills for the protocol).

**Chief-of-staff maintenance** — the recurring `chief-of-staff` tick owns routine issue
maintenance. It may automatically apply evidence-backed tracker fixes: required labels,
blocked/parked agreement, priority labels with explicit maintainer or dependency-graph
evidence, parent/sub-issue wiring already stated in issue text, clear
`Relationships`/`touches` repairs, and closing issues whose PR is merged and verified on
`main`. It should stop and ask only for material conflicts: product direction, issue
scope changes, new priorities without evidence, unparking deferred work without an
explicit resume signal, partial/disputed closure, new issue creation (except filing
follow-ups recorded in a pipeline merge-gate `followups.md` via `/file-issue`, which is
auto-allowed), deleting substantive prose, or contradictions between labels, body,
comments, and live PR state.
If maintenance changes lane-affecting state such as `priority:*`, `touches`,
`Relationships`, `blocked`, or `parked`, or if a merge changes the ready/running sets,
rerun the issue-pulse lane-staleness path before recommending work. It must run only
from the canonical main checkout `/Users/adam/Code/registry-research-toolkit`, never
from a worktree. Its startup gate is: verify the exact repo top-level, `test -d .git`,
and branch `main`; run `git pull --ff-only` as the first sync action; re-verify the main
checkout; and stop if `git status --short` is not empty. If any gate fails, report the
condition and ask the user to fix it before relaunching. Actual implementation work
always happens in separate worktrees; chief-of-staff coordinates issues/PRs and merges
ready gated PRs from the main checkout. When a merge creates a required build/release
boundary, such as DB content that dependent work needs published, chief-of-staff may
invoke `/release minor` or `/release patch` (`$release minor` / `$release patch` on
Codex surfaces) and then must follow the release workflow gates; a major release is not
autonomous. With `/chief-of-staff auto` (opt-in per session) it may also auto-dispatch
pr-pipeline lanes into free slots via `scripts/cos_dispatch.py`, gated by the
`<state-root>/auto-dispatch.off` kill switch (present ⇒ fall back to recommending), at
one of two launch tiers — `easy` (Sonnet 5 with an Opus advisor, for small low-risk
lanes) or `hard` (Codex gpt-5.5 xhigh, the default). Merges in the maintainer-approval
classes — a schema/DDL change, a build-affecting PR whose dbdiff delta exceeds what the
PR/issue states, a change to the COS/merge-gate machinery itself, or deploy/infra work
or a major release — always wait for the maintainer.

# Git

- Never run `git commit --no-verify`, `git commit -n`, or `git push --no-verify`. If a
  pre-commit hook fails, fix the underlying issue rather than bypassing.

## PR merge gate

Green CI alone is never sufficient to merge. Scale the rest to the PR's size and risk
(see the `pr-pipeline` skill for the full authoring/handoff version):

**Merge ownership** — `pr-pipeline` owns authoring, review, verification, and the
durable merge-gate handoff. It does **not** merge. The `chief-of-staff` skill owns
routine merge decisions and execution.

**Local merge-gate store** — the handoff signal lives on the maintainer's machine, not
in the PR: `$XDG_STATE_HOME/registry-research-toolkit/merge-gates/pr-<N>/` (default
`~/.local/state/registry-research-toolkit/merge-gates/pr-<N>/`), one directory per PR
holding `gate.json` plus the evidence files it references (design-reviewer report and
screenshots, `build-db` log, dbdiff output, and an optional `followups.md` recording the
lane's drafted follow-up issues for the chief-of-staff to file at merge via
`/file-issue`). All pipelines and the chief-of-staff run on
this machine, so a local file is durable across worktree deletion, `git clean`, and
reboots — which `/tmp` and worktree paths are not — and needs no GitHub attachment
gymnastics. Do NOT post evidence to the PR (no evidence branches, no committed
screenshots, no body blocks); the PR body carries only the description and closing
keywords (which stay authoritative for issue closure — gate.json does not duplicate
them). The `gate.json` contract: head-SHA-bound (`pr`, `head` full SHA, `status`
`ready-to-merge` \| `blocked`, `updated`, `blocker` naming the missing item when
blocked) plus a `gates` map with one line per repo gate; the expensive re-verifiable
gates (`build_db`, `visual`) each record the head SHA they were verified on inside their
line. Field-level worked example: the `pr-pipeline` skill. Write evidence files first
and `gate.json` last, atomically (temp file + rename) — the preflight probe polls it and
must never see a torn write; after repairing or adding evidence files, refresh
`gate.json` (bump `updated`) so the byte-change wakes the next tick. Readers treat an
entry whose `pr` field disagrees with its directory name as absent. A recurring
chief-of-staff tick may automatically squash-merge a PR only when its gate entry has
`status: ready-to-merge` with `head` matching the live `headRefOid` (and the
`build_db`/`visual` per-gate SHAs matching that head where those gates apply), all
required evidence files are present, and the chief-of-staff re-checks the live PR head,
CI, Codex bot signal, mergeability, and stack order immediately before merging.
Provenance is by construction: only local agents can write the store, so a fork PR can
never self-certify — but never automerge a PR whose head branch is not in this
repository, and treat a gate entry for such a PR as an error to surface. (Trust is
machine-level and accepted as such for this single-maintainer repo: any code executed
locally — a test run, a build — could write the store, exactly as it could previously
have edited a PR body with the maintainer's credentials; the gate defends against
process skew, not against malicious local code.) After a verified merge, the
chief-of-staff archives the PR's gate directory under `merge-gates/merged/` — the PR
carries no evidence, so the archive IS the audit trail for post-merge regressions; prune
entries whose PR closed without merging.

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
  Codex/Copilot a bounded window.
  **`uv run --no-project python scripts/pr_review_status.py <pr>`** computes the signal:
  JSON to stdout (`signal` ∈ `clean`/`findings`/`reviewing`/`exhausted`/`none`), exit
  **0** settled · **1** not-settled · **2** tool error. By default it **polls**
  (re-fetch every 30 s — there are no webhooks) to a \~15-min ceiling, so launch it
  **once per HEAD as a background task** (`run_in_background: true`) — the wait outlasts
  the 10-min foreground `Bash` cap; `--once` gives a single non-blocking snapshot
  instead. Prefer it over re-deriving the `gh api` calls by hand, which is where this
  gets shipped wrong: Codex submits reviews as login `chatgpt-codex-connector` but
  reacts as `chatgpt-codex-connector[bot]`, so a one-login poller misses half the
  signal. Poll for the **bot's own signal on the current HEAD**, NOT for CI finishing —
  CI is a separate gate that usually goes green far sooner, so a poller that exits on
  CI-done has not actually given the bot its window. The signals: a submitted Codex
  **review** = findings (its suggestions vehicle); Codex's "Codex Review: …" **comment**
  stamped `Reviewed commit: <sha>` for the head — or, when it posts no comment, a **👍
  reaction** (invisible to `gh pr view`) within the review window — = its clean verdict;
  a **👀 reaction** = still reviewing — never conclude or merge while it's the newest
  signal; an out-of-tokens comment ("reached your Codex usage limits") = a definitive
  end-of-wait, not a blocker. The poller scopes each signal to the current HEAD so a
  stale verdict can't read as fresh: the **review** by its `commit_id` and the
  SHA-stamped **clean comment** by its stamp (rebase-proof); the commit-unbound signals
  (👍, exhausted, 👀) by the **review window** = the later of the head commit time and
  the most recent `@codex review` request (GitHub exposes no reliable push time). The
  poller also returns the verdict bodies in `messages`, so you read them without a
  second `gh` call. \~15 min with no signal is the ceiling — bots may skip a push
  entirely (Codex auto-reviews on open/ready only; a verdict on a new HEAD must be
  requested by commenting `@codex review`). Absence at the ceiling is not a blocker for
  a human handoff, but it is not enough for an automatic `chief-of-staff` merge; leave
  the PR's gate entry below `status: ready-to-merge` until the signal is `clean` or an
  acceptable `exhausted` result with all other gates complete.
- **Real-data validation** when build-pipeline or DB content changed: run a real-seed
  `reg-meta-build build-db` **on the PR head** (validation runs by default), not just
  fixture tests. The untracked seed lives only in the main checkout. From a worktree,
  use an absolute input root; if the PR changes any tracked
  `reg_meta_build/input_data/**` file, make that root an overlay: main checkout
  untracked seed plus the PR-head tracked inputs copied on top, with PR deletions /
  renames mirrored. A direct `--input-dir <main-checkout>/reg_meta_build/input_data/`
  validates main's tracked inputs, not yours. Store the timestamped build log and any
  dbdiff output in the PR's merge-gate directory. When a PR is otherwise merge-ready but
  this gate's evidence is missing or stale (wrong head), the chief-of-staff runs the
  verification itself — a throwaway worktree at the PR head, the same build/dbdiff, the
  result written into the gate store — instead of routing a follow-up or asking the
  user.
- **Visual verification** when the PR changes rendered output (`reg_webapp/frontend/**`,
  or any view / component / style the SPA renders). This is the UI analog of real-data
  validation — **required, not optional**, for rendered changes. Headless checks
  (`bun run lint/check/test/build`) never render a pixel, so green `bun` is not
  sufficient. Run the structured repo design-review skill (`/reg-webapp-design-reviewer`
  on Claude Code; `reg-webapp-design-reviewer` on Codex) in a clean subagent/session for
  layout, responsive, accessibility, and consistency issues. Dispatch a fresh generic
  agent and instruct it to invoke that exact skill (not the Claude `reviewer` subagent —
  it has no Skill tool, so it cannot); do not run the formal reviewer pass in the lead
  session or substitute a generic web reviewer. That reviewer pass runs the app and
  *looks* with the **one-shot driver** —
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke` (or `dev.sh shot <route>` for
  the changed views): it picks free ports, renders from THIS checkout's `.venv`
  (worktree-correct), screenshots to `/tmp/reg-webapp-shots/`, and **tears the servers
  down on exit** — no port collisions, no leaked servers. For interactive poking use
  `preview_start` + `preview_snapshot` / `preview_click` / `preview_resize` (now
  `autoPort` + `dev.sh preview`-backed, so it's collision-free across sessions and
  serves the worktree's own code). Copy the `reg-webapp-design-reviewer` report and its
  screenshots into the PR's merge-gate directory as the durable proof, the same way a
  build PR stores its `build-db` log there — never attach them to the PR or commit them
  to the branch. Use the repo-local `reg-webapp-frontend-design` skill when authoring
  new UI.
- **Stale-head check**: before merging, confirm the PR's `headRefOid` equals the local
  branch tip and pass that SHA to `gh pr merge --match-head-commit`; after merging,
  fetch `origin main`, fast-forward the local main checkout with
  `git merge --ff-only origin/main`, and confirm the PR's changes are actually present
  on main — the GitHub API can capture a stale head and silently drop just-pushed
  commits. (Comparing the merge commit's tree to the branch tip works only when the base
  didn't advance in between.)
- **Stacked PR branch safety**: before merging a stack predecessor, inspect open
  successor PRs' `baseRefName` and `headRefName`. If a successor is based on the
  predecessor branch, do not delete the predecessor branch during merge; immediately
  retarget the successor to `main` after the predecessor merge, then verify it remains
  open on the intended head. After retargeting, require the successor branch to be
  rebased or otherwise updated onto the new base, then regenerate checks, Codex bot
  review, independent-review judgment, and the gate entry before automerging it. Never
  delete a branch that is the head branch of another open PR.

**Agent-driven PR work outside `/pr-pipeline`:** when you build a change end to end
without the user invoking the skill, run the same shape — plan → implement →
`/code-review` (effort scaled to risk) → docs — then **mark the PR ready for review**
(`gh pr ready <pr>`) and hand it back for this gate. Marking it ready is the step that
**starts the bot-review window** — Codex auto-reviews on the open/ready transition,
never on a draft, so a PR handed back as a draft stalls the gate. Leave a PR draft only
while it's genuinely still being built (the draft is also the in-flight claim). Once
ready, you may poll and report the bot-review window (above). Do **not** merge on your
own initiative. If you want the recurring staff loop to auto-merge it, write a
current-head `gate.json` with `status: ready-to-merge` into the local merge-gate store
and leave execution to `chief-of-staff`.

# Layout

For per-package design rationale, see `<package>/DESIGN.md` (the reg_meta object model
lives in `reg_meta/DESIGN.md`; per-provider source-delivery shapes in
`reg_meta_build/DESIGN.md`). For the cross-package design (topology, dependency graph,
repo-wide invariants), see `ARCHITECTURE.md`; for the remaining post-A5 work, see
`REFACTOR_SPEC.md`.
