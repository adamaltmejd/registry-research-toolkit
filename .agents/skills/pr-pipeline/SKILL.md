---
name: pr-pipeline
description: >-
  Registry Research Toolkit PR development pipeline. Use when asked to run the PR
  pipeline workflow, including prompts like "$pr-pipeline issue 510"; develop issue(s),
  take a ranked lane through implementation, open draft PRs with closing keywords, run
  review/test/docs/visual gates, mark PRs ready, or continue toward merge when the user
  explicitly asks for merge.
---

# Registry PR Pipeline

## Scope

Turn an issue, a lane, or a feature request into one or more tightly scoped PRs.

Agent-surface notes:

- The lead agent implements directly by default, except for review: launch the review
  pass in a fresh subagent when the current environment allows it, so findings are
  independent of the authoring session. The review subagent reports findings back to the
  lead agent; the lead agent fixes or dismisses them.
- For review, prefer a callable built-in review capability when one is exposed.
  Slash-command reviews may be available to the top-level user/session without being
  invokable from inside this skill workflow. When no callable built-in review is
  exposed, run `registry-code-review` as the repo-scoped callable review workflow in a
  fresh subagent. If the review can only run in the authoring session, treat it as a
  diagnostic checklist, not as independent review evidence. The GitHub bot-review window
  described by the repository guidance still applies.
- Codex skills are invoked by their skill names, not by Claude slash-command syntax. For
  new UI authoring, use `frontend-design` before building when that skill is exposed in
  the active Codex setup. If it is not exposed, report that setup gap before authoring
  substantial new UI instead of silently substituting generic design prose.
- Do not merge unless the user explicitly asked for merge/full pipeline or confirms at
  the merge gate. Otherwise finish by marking the PR ready and reporting the gate state.

## Intake

1. If the target is `next`, first run `plan-lanes`, pick the top coherent lane unless
   there is a clear reason not to, and confirm non-trivial choices with the user.
2. Read issue bodies, comments, Relationships, the parent epic, blockers, linked PRs,
   repository guidance (`AGENTS.md`; `CLAUDE.md` is intentionally equivalent for agent
   surfaces that use it), relevant `<package>/DESIGN.md`, and affected code.
3. Shape the smallest coherent PR set — at altitude first: does the work need to exist
   at all, or does an existing subsystem or installed library already subsume it? Prefer
   extending existing architecture to adding a module. Sequence by dependency. For
   multi-PR or ambiguous work, show the breakdown before editing.
4. Decide whether behavior changed enough to need a dedicated test-gap pass and whether
   authored docs can drift.

## Claim

When building issue work, open draft PRs early so the sequencing projection marks issues
running.

For a known multi-issue or multi-PR effort, create all known draft PR claims before
implementation, not just the first branch. Each draft body must close the issue(s) that
PR is expected to resolve so the sequencing projection holds the whole planned lane.

```sh
git fetch origin main
git checkout -b s/<slug> origin/main
git commit --allow-empty -m "wip: <scope>"
git push -u origin s/<slug>
gh pr create --draft --title "wip: <scope>" --body-file <body-file>
```

The PR body must contain `Closes #<issue>` for each issue the PR resolves. Use
`--body-file`, not an inline heredoc. Supply `--title` (or `--fill` when appropriate) so
the draft claim works in noninteractive agent runs.

If the user asked only for local implementation and not PR creation, skip the draft
claim and say why.

## Build

Implement directly in the current checkout, keeping scope tight. Follow repo rules:
pre-v1 means no shims, compatibility layers, migrations, or dead-code retention;
validate JSON boundaries; keep domain logic separate from IO/prompts/integrations; use
`uv`, `bun`, `rg`, and `fd`.

Before edits, understand the relevant design docs. During edits, apply the AGENTS.md
reuse-first ladder: reuse an existing internal helper / stdlib / installed dep before
hand-rolling, no speculative abstractions, prefer deletion to addition. The common miss
is leaf-helper duplication — a validator / write-loop / clamp-gate re-pasted into a new
module instead of hoisted into `reg_meta_build`'s `_curation.py` / `db.py` or
`reg_webapp`'s `query_input.py`; a large hoist that grows scope is a call to confirm
with the user, not to do silently. Before review, re-read your own diff and cut what's
cuttable — but never simplify away PII/MONA confinement, k-anonymity, determinism,
JSON-contract validation, or anything requested.

Run focused verification as the work evolves:

- Python: `uv run ruff check`, `uv run ruff format --check`,
  `uvx --from ty==0.0.49 ty check`, and targeted `uv run python -m pytest <pkg>/`.
- Frontend: from `reg_webapp/frontend/`, use `bun run lint`, `bun run check`,
  `bun run test`, `bun run build`, and regenerate API types only after backend contract
  changes. Headless checks never render a pixel. If the change alters rendered output
  (`reg_webapp/frontend/**`, or any view / component / style the SPA renders), render it
  with `reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <changed-route>` or
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke`, inspect
  `/tmp/reg-webapp-shots/`, and keep the screenshot path for closeout / PR proof.
- Build-affecting DB changes: fast tests first; real `reg-meta-build build-db` is a
  final gate on the truly final head.

## Test And Review

1. Check test coverage pragmatically. Add regression tests for fixed bugs, new branches,
   contract boundaries, validation codes, exit codes, and deterministic ordering where
   they matter.
2. Commit and push the implementation before any GitHub-based PR review or bot-review
   window. The early draft PR may contain only the empty claim commit; do not count a
   review of that stale diff as the independent review for the actual patch. If running
   a callable built-in review locally before push, target the current local diff
   explicitly.
3. Run review on the actual implementation diff. Launch a fresh review subagent when
   available, and pass only the PR number or branch/range plus necessary issue context,
   not the author's intended fixes or conclusions. Prefer a callable built-in review
   capability; do not try to invoke slash commands that are only exposed to the
   top-level session. If no built-in review is callable from this workflow, have the
   subagent run `registry-code-review` on the PR number or branch/range. If subagents
   are unavailable, run `registry-code-review` in-session only as a diagnostic
   checklist, state that it does not satisfy the independent review gate, and stop
   before ready/merge until an external or subagent review signal is available. Fix or
   explicitly dismiss every material finding with a reason. Beyond correctness, weigh
   reuse/simplification/altitude cleanup — a one-caller abstraction, a module
   duplicating a subsystem elsewhere, a library that subsumes the approach — and route
   those cuts like any finding. (There is no `/simplify` on this surface; it is a Claude
   Code skill only.)
4. For rendered-output changes, run `web-design-reviewer` against the rendered app as
   the structured design-quality pass when the skill is exposed in the active Codex
   setup. If it is not exposed, report that setup gap and still complete the mandatory
   manual visual review with the `run-reg-webapp` render; do not treat headless `bun`
   checks as a substitute. Route design findings through the same fix / dismiss /
   re-review loop as code-review findings.
5. Re-review substantial fixes until the review converges.
6. Update authored docs only where the diff made them stale: package `DESIGN.md`,
   README/CLI examples, docstrings, `ARCHITECTURE.md`, repository guidance files,
   validation-code docs. Never edit generated `reg_meta_build/docs/lisa/*.md`.
7. Commit and push any review/doc fixes. Never use `--no-verify` or `-n`; fix hook
   failures.

## Ready Or Merge Gate

Mark the PR ready when the code is near-final:

```sh
gh pr ready <pr>
```

For merge, satisfy the repo gate:

- independent review converged;
- CI green;
- bot review window settled on the current HEAD — run
  `uv run --no-project python scripts/pr_review_status.py <pr>` to compute Codex's
  signal (`clean`/`findings`/`reviewing`/`exhausted`/`none`, scoped to the current HEAD;
  verdict bodies returned in `messages`, no second `gh` call) instead of re-deriving the
  login-sensitive `gh api` calls. It defaults to polling (re-fetch every 30 s — no
  webhooks — to a \~15-min ceiling), so launch it once per HEAD as a background task
  (the wait outlasts a 10-min foreground cap); `--once` is a single snapshot. Never
  conclude while it reports `reviewing`; after a new push, re-trigger with
  `@codex review` and launch a fresh background poll;
- real-data validation when build pipeline or DB content changed;
- visual verification when rendered output changed: run
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke` or
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <route>` on the assembled tree,
  inspect the screenshot, and run `web-design-reviewer` for the structured design pass
  when the active Codex setup exposes it;
- stale-head check before and after merge.

Run the real `build-db` last and once for build-affecting work, using the main
checkout's untracked seed if working from a worktree. Narrowing with `--providers` is
fine for a scoped dbdiff (e.g. `--providers scb,sos` for an SCB/SOS-only change is
faster than the full global build) — #563 gates the curated-override staleness check to
the built providers, so a thin provider's entity-key pins (#554) no longer crash a
restricted build. Pick the providers your PR affects, or omit `--providers` for the full
global set (release asset / cross-provider PRs). If the PR changes any tracked
`reg_meta_build/input_data/**` file (provider `*.toml`,
`classifications/`/`scb_canonical/` CSV, or an add/delete/rename), do not point
`--input-dir` directly at the main checkout: that validates main's tracked inputs, not
the PR head. Instead build an overlay input root that starts from the main checkout's
untracked seed and then copies the PR-head tracked `input_data` tree on top. Mirror any
PR deletion/rename in the overlay; never write back through a symlink into the main
checkout.

```sh
db_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-<slug>.XXXXXX")"
input_dir="<main-checkout>/reg_meta_build/input_data"
# If this PR changes tracked reg_meta_build/input_data/**, first build an
# overlay input root and set input_dir to that overlay.
uv run reg-meta-build --db "$db_dir" build-db \
  --input-dir "$input_dir"
```

Clean scratch outputs afterward:

```sh
git clean -fdX reg_meta_build/fqid_slugs/
rm -rf "$db_dir"
```

## Closeout

Report what changed, PR number/status, verification commands, review findings fixed or
dismissed, docs/test decisions, and any follow-up issues worth filing. Before proposing
a new issue, search open and closed issues with
`gh issue list --state all --search "<keywords>"`.
