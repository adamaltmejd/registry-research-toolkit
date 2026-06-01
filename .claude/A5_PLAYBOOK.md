# A5 Orchestration Playbook

**Scope & lifecycle.** Working playbook for an agent orchestrating **Stage A5
(Webapp + SPA)** of the Model A migration. Transient and **self-deleting** —
delete it in the A5.4 PR alongside `MIGRATION_PLAN.md` (A5.4 is the completion
gate for both). It explains *how* to run A5; *what* each step does lives in
`MIGRATION_PLAN.md` (the live tracker, "Stage A5") and `REFACTOR_SPEC.md` §9
(webapp/SPA design), §9.5 (URL routing), §16 (input-validation gates). The
companion `/goal` condition carries the compressed, must-not-forget rules
(re-injected every turn); this file is the elaboration — **re-read it at the
start of each step.**

## What's different from A4 (read first)

A4 mutated the reg_meta DB build and gated on **byte-identical output**. A5
builds a **new package from scratch** and gates on a **running app, security,
and codegen-drift** — not a DB diff. Specifically:

- **`reg_webapp/` does not exist yet.** A5.1 scaffolds it: `backend/`
  (FastAPI + Pydantic) and `frontend/` (Svelte 5 + Vite + TypeScript, bun).
  The old `mock_data_wizard/web/` SPA is **superseded** — do not revive or
  extend it; its deletion is §15 step 7 (separate, leave it alone unless I
  ask). Scaffolding also means: add `reg_webapp` to `[tool.uv.workspace]
  members` in the root `pyproject.toml`, wire its deps (`reg_meta`,
  `reg_schema`, `reg_monabundle`), and add CI + frontend-toolchain gates
  (below).
- **Reads reg_meta read-only.** The backend mmaps the real reg_meta SQLite
  (default path below) and builds an in-memory catalog index (§9.2). It never
  rebuilds the DB — so **no slug-dir footgun, no `build-db`, no dbdiff.**
- **Frontend toolchain is new to this repo's gates.** Pre-commit today covers
  Python + markdown only. A5.1 must wire TypeScript/Svelte gates (typecheck,
  build, format/lint) into CI — see the gate section.

## Role

You are the ORCHESTRATOR, running in the MAIN LOOP. Spawn opus subagents via
`Workflow()` for fan-out (planning, implementation, review panels). Keep in the
main loop — they cannot live inside a single `Workflow()` script — the
`/simplify` and `/review` skills, draft-PR creation, watching CI, waiting on
Codex, the running-app / visual checks, and merges.

## Sequencing

A5 is **not a strict chain.** A5.1 gates everything; after it merges,
**{A5.2, A5.3, A5.4}** open — but with two real couplings, so confirm the
order with me before fanning out:

- **A5.4** (IndexedDB v0.x hard-reject) is genuinely independent — a pure SPA
  load-guard. It can land any time after A5.1.
- **A5.3** (SPA TS regen + component updates + new-sub-endpoint integration)
  has a **soft dependency on A5.2**: it codegens TS against A5.2's OpenAPI and
  wires A5.2's endpoints into components. Default to **A5.2 → A5.3
  sequentially** unless the A5.3 planning agent finds a clean stub seam
  (codegen against the A5.1 baseline, re-codegen after A5.2). Surface that as
  a fork; I decide.

Parallelism otherwise applies **within** a step (e.g. A5.2's seven
sub-endpoints, A5.3's component set).

## Per-step pipeline

1. **Plan (Workflow, stage 1).** An opus planning agent reads the step's
   section of `MIGRATION_PLAN.md` + only the relevant `REFACTOR_SPEC.md`
   sections (§9 webapp design + §9.2 stack for A5.1; §9.5 URL routing + §16
   input-validation for A5.2; §9.5 variant browser + the SPA notes for A5.3;
   the `schema_version` / IndexedDB notes for A5.4 — don't read the whole
   4,700-line spec). It returns a concrete file-by-file plan, flags risks, and
   surfaces any maintainer-decision forks to me (the scaffold split and the
   A5.2/A5.3 ordering are the known ones).
2. **Implement (Workflow, stage 2).** An opus implementation agent implements
   per the plan. When the Workflow returns, open a **DRAFT** PR referencing the
   A5.x identifier.
3. **Simplify.** Run the `/simplify` skill on the diff (it fans out its own
   review agents). Apply the worthwhile fixes; ensure it's efficient and not
   over-engineered. Its edits weren't gate-checked — **re-run the gate after.**
4. **Gate.** Commit — pre-commit enforces the Python + doc gate (ruff-check,
   ruff-format, ty, pytest, markdownlint). Then run the **app + frontend +
   security gate** (below); pre-commit does not run those.
5. **Review.** Mark the PR ready. Watch CI; wait ~10 min for Codex + Copilot
   and address their inline P1/P2 (don't trigger `@codex` yourself). Also spawn
   an independent `/review` agent (or a Workflow review panel) — it RETURNS
   findings to me only and NEVER posts PR comments (posting under the
   maintainer identity trips the security classifier). Iterate; re-check the
   bots ~10 min after each fix push.
6. **Decide.** Resolve all relevant issues. Escalate genuine architecture/scope
   decisions to me instead of guessing; do NOT merge with an unresolved P1
   without my sign-off.
7. **Merge.** Squash-merge (merge-commit only to preserve distinct ride-along
   work). Flip the step's checkbox in `MIGRATION_PLAN.md` and update its status
   note as part of the PR.
8. **Follow-ups.** Triage what you found: critical → fold into the step's PR
   (or an add-on PR); useful-but-not-critical → file a "suggested task."

## The app + frontend + security gate (what makes A5 ≠ A3)

Fixture/unit tests alone aren't sufficient — A5 ships a running service and a
browser app. Pre-commit does not run any of this; do it manually, per step.

- **Real reg_meta DB (read-only).** The backend reads the fresh real DB at the
  default path `/Users/adam/.local/share/reg_meta/reg_meta.db` (SCHEMA_VERSION
  5.1.0, auto-discovered — no env var). Start the app against it; do **not**
  rebuild it. The backend should assert its expected `SCHEMA_VERSION` on
  startup and fail fast on drift (e.g. if A4 later bumps it) rather than
  serving wrong shapes silently.
- **Backend smoke (every step that touches the API).** Boot uvicorn against the
  real DB; `curl`/`httpx` the touched endpoints; assert 200 + the expected
  Pydantic shape on real FQIDs. Every route must declare `response_model=`
  (lint-enforced, §9.2).
- **OpenAPI + TS codegen drift (A5.1 onward).** `openapi.json` is committed and
  snapshot-tested in CI; TS types are codegen'd from it via
  `openapi-typescript` and committed. Add/keep a CI drift test: regenerating
  both must produce **no diff** — a dirty tree after regen fails the gate. Run
  the regen yourself before marking ready.
- **Frontend toolchain (A5.1 wires it; A5.3 leans on it).** Use bun/bunx, not
  npm. Gates to wire into CI (and run locally per step): `bunx tsc --noEmit`
  (or `bunx svelte-check`), `bun run build` (Vite production build must
  succeed), and one formatter/linter (pick Biome **or** prettier+eslint — one,
  not both). A5.1's PR must add the CI job; later steps just keep it green.
- **§16 input-validation security gate (A5.2 — hard requirement).** The
  `?period=` / `?variant=` parsers are **allow-lists**: malformed values
  (SQL-injection probes, path-traversal `..`, `%`-encoding, embedded NUL)
  return **422 with zero SQL executed** — verify via a SQLite trace hook
  asserting no statement ran. The per-segment FQID grammar check rejects
  non-slug input with 422 and no DB hit. Parametrized tests cover both. Also:
  the seven suffixed / sub-resource routes (`/states`, `/predecessors`,
  `/successors`, `/related`, `/lineage`, `/lineage_warnings`, the variant
  browser) must register **before** the `/api/catalog/{fqid:path}` catch-all —
  add the CI introspection test that enforces ordering. ETag cache key must
  include `?period` (and `?variant`).
- **Visual / running-app check (SPA steps, A5.3/A5.4).** Use the Claude Preview
  MCP tools in the main loop — `preview_start` the Vite dev server (or the
  built app served against the running backend), `preview_screenshot` the key
  views, `preview_click` through the variant browser / states picker, and
  `preview_console_logs` to confirm no console errors. Spawn a visual-review
  subagent if a second pass helps. There is **no** `web-design-reviewer`
  agent/skill in this registry — drive Preview directly.
- **Cloudflare edge-cache gate — NOT locally runnable.** A5.2's "small load
  test through Cloudflare" and the slash-bearing-FQID edge check need the
  deployed environment. Locally, validate the **ETag / `Cache-Control` header
  logic** via unit tests (correct per-URL ETag, period/variant in the key).
  The real Cloudflare round-trip is a **maintainer task** — escalate it; do not
  claim that gate passed from local runs.
- **Per-step gate order:** land → app+frontend+security gate → independent
  review + all bot reviews addressed → ~10-min window, re-check → merge.

## Conventions

- Follow the repo `CLAUDE.md` (uv not pip, **bun/bunx not npm**, rg/fd). Never
  bypass git hooks (`--no-verify`/`-n`); fix the cause.
- Package security: 7-day minimum release age on any new npm/PyPI dep.
- Don't begin a step on a dirty working tree — commit/stash first.
- A5 reads reg_meta read-only and ships no DB DDL — no `SCHEMA_VERSION` bump is
  expected from this stage.
