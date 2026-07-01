---
name: pr-pipeline
description: >-
  Registry Research Toolkit PR development pipeline. Use when asked to run the PR
  pipeline workflow, including prompts like "$pr-pipeline issue 510"; develop issue(s),
  take a ranked lane through implementation, open draft PRs with closing keywords, run
  review/test/docs/visual gates, mark PRs ready, and record current-head merge-gate
  evidence for chief-of-staff automerge.
---

# Registry PR Pipeline

## Scope

Turn an issue, a lane, or a feature request into one or more tightly scoped PRs.

Agent-surface notes:

- The lead agent implements directly by default, except for review: first attempt to
  launch the review pass in a fresh subagent so findings are independent of the
  authoring session. The review subagent reports findings back to the lead agent; the
  lead agent fixes or dismisses them.
- For review, run `registry-code-review` as the repo-scoped callable review workflow in
  a fresh subagent. On Codex `multi_agent_v1`, omit `agent_type` (there is no
  review-specific role), do not fork the full history, and pass only the PR number or
  branch/range plus necessary issue context. In-session `registry-code-review` is
  diagnostic, not independent review evidence. The GitHub bot-review window described by
  the repository guidance still applies.
- For rendered-output PRs, run `web-design-reviewer` in a clean subagent session before
  the lead's own screenshot inspection. Pass the changed routes, PR/branch, and enough
  setup context for the reviewer to apply the skill's structured report; do not pass the
  author's visual conclusions as evidence. Manual screenshots alone are not reviewer
  evidence, and a PR that lacks this pass does not satisfy the visual gate.
- Codex skills are invoked by their skill names, not by Claude slash-command syntax. For
  new UI authoring, use the repo-local `frontend-design` skill before building.
- Named review/design skills may be provided by the agent environment rather than this
  repository's `.agents/skills/` tree. Do not downgrade a required gate because the
  skill is not repo-local.
- Do not merge. The `chief-of-staff` skill owns routine merge decisions and execution.
  Finish by marking PRs ready, recording current-head merge-gate evidence, and reporting
  the handoff state.

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
   authored docs can drift. "Authored docs" includes the design-spec files
   (`<package>/DESIGN.md`, `ARCHITECTURE.md`, `REFACTOR_SPEC.md`) and the factual
   references inside them — a token/symbol/flag/file name a section names drifts the
   moment the diff deletes or renames it, even in a historical "what shipped" note.

## Claim

When building issue work, open draft PRs early so the sequencing projection marks issues
running.

For a known multi-issue or multi-PR effort, create all known draft PR claims before
implementation, not just the first branch. Each draft body must close the issue(s) that
PR is expected to resolve so the sequencing projection holds the whole planned lane.

```sh
base_ref="main"  # use the predecessor branch name for a stacked successor
git fetch origin "$base_ref:refs/remotes/origin/$base_ref"
git checkout -b s/<slug> "origin/$base_ref"
git commit --allow-empty -m "wip: <scope>"
git push -u origin s/<slug>
gh pr create --draft --base "$base_ref" --title "wip: <scope>" --body-file <body-file>
```

The PR body must contain `Closes #<issue>` for each issue the PR resolves. For stacked
successors, set `base_ref` to the predecessor branch and pass `--base "$base_ref"`; do
not let `gh pr create` default the child PR back to `main`. Keep the closing keyword in
the body; `plan_sequence.py` parses PR bodies as a fallback because GitHub may not
populate `closingIssuesReferences` for non-default-base stacked PRs. Use `--body-file`,
not an inline heredoc. Supply `--title` (or `--fill` when appropriate) so the draft
claim works in noninteractive agent runs. For dependent successors, early draft creation
is only a claim; before implementing or testing the successor, first update the
predecessor branch with its real contract commits, then rebase or merge the successor
branch onto that finalized predecessor branch and push the new successor head.

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
  `uvx --from ty==0.0.51 ty check`, and targeted `uv run python -m pytest <pkg>/`.
- Frontend: from `reg_webapp/frontend/`, use `bun run lint`, `bun run check`,
  `bun run test`, `bun run build`, and regenerate API types only after backend contract
  changes. Headless checks never render a pixel. If the change alters rendered output
  (`reg_webapp/frontend/**`, or any view / component / style the SPA renders), render
  while iterating with
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <changed-route>` or
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke`. Iteration screenshots do not
  satisfy the formal visual gate; that gate runs later as `web-design-reviewer` in a
  clean subagent, followed by the lead's own screenshot inspection and durable PR proof.
- Build-affecting DB changes: fast tests first; real `reg-meta-build build-db` is a
  final gate on the truly final head.

## Test And Review

1. Check test coverage pragmatically. Add regression tests for fixed bugs, new branches,
   contract boundaries, validation codes, exit codes, and deterministic ordering where
   they matter.
2. Commit and push the implementation before any GitHub-based PR review or bot-review
   window. The early draft PR may contain only the empty claim commit; do not count a
   review of that stale diff as the independent review for the actual patch. If running
   `registry-code-review` locally before push, target the current local diff explicitly.
3. Run review on the actual implementation diff. First attempt to launch a fresh
   subagent running `registry-code-review`, and pass only the PR number or branch/range
   plus necessary issue context, not the author's intended fixes or conclusions. On
   Codex `multi_agent_v1`, omit `agent_type` (there is no review-specific role) and do
   not fork the full history. In-session `registry-code-review` is diagnostic and does
   not satisfy the independent review gate. Stop before ready/handoff until the subagent
   review has reported. Fix or explicitly dismiss every material finding with a reason.
   Beyond correctness, weigh reuse/simplification/altitude cleanup — a one-caller
   abstraction, a module duplicating a subsystem elsewhere, a library that subsumes the
   approach — and route those cuts like any finding. (There is no `/simplify` on this
   surface; it is a Claude Code skill only.)
4. For rendered-output changes, run the formal visual gate in this order:
   - First, launch a fresh subagent running `web-design-reviewer` against the rendered
     app or the changed route(s). The reviewer must apply the skill's structured report
     workflow for layout, responsive, accessibility, and consistency issues. It can use
     `run-reg-webapp` or an already-started preview URL, but its report must be separate
     from the author's manual inspection.
   - Route reviewer findings through the same fix / dismiss / re-review loop as
     code-review findings. Re-run the reviewer when fixes materially change the rendered
     surface.
   - Only after the reviewer pass converges, run the lead's own
     `reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke` or
     `reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <route>` screenshot pass,
     inspect the screenshots, and keep durable PR-visible proof. Do not set the
     merge-gate status to `ready-to-merge` until both the reviewer pass and the lead
     screenshot pass are complete. Headless `bun` checks or manual screenshots do not
     substitute for the reviewer pass.
5. Re-review substantial fixes until the review converges.
6. Update authored docs wherever the diff made them stale — including the design-spec
   prose and any token/symbol it names: package `DESIGN.md`, README/CLI examples,
   docstrings, `ARCHITECTURE.md`, repository guidance files, validation-code docs. Fix a
   one-line drift in place; don't defer a one-liner you already touched to a follow-up
   (and don't falsify a historical note — add a "superseded by …" pointer instead).
   Never edit generated `reg_meta_build/docs/lisa/*.md`.
7. Commit and push any review/doc fixes. Never use `--no-verify` or `-n`; fix hook
   failures.

## Ready And Merge-Gate Handoff

Mark the PR ready when the code is near-final — "ready" is what starts the Codex/Copilot
auto-review, and it fires ONCE on the open→ready transition, NOT on later pushes (a new
HEAD needs an explicit `@codex review`). The draft already holds the in-flight claim and
CI runs on drafts, so time "ready" so the bot reviews code you won't churn:

- trivial / mechanical / low-risk PR (you expect a clean review): mark ready early so
  the bot reviews in parallel with your review pass — if both stay clean and HEAD
  doesn't move, that one bot verdict also clears the gate;
- substantive PR (you expect review/doc fixes to push commits): stay draft through
  review
  + docs, then mark ready once on the converged HEAD — an early ready only strands the
    bot verdict on a stale HEAD (it won't re-review) and burns a Codex run.

```sh
gh pr ready <pr>
```

To mark a PR ready for `chief-of-staff` automerge, satisfy the repo gate and record
durable evidence in the PR body:

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
  `@codex review` and launch a fresh background poll. A `none` result can be handed to a
  human with explanation, but it is not enough for `status: ready-to-merge` automerge
  evidence;
- real-data validation when build pipeline or DB content changed;
- visual verification when rendered output changed: complete the clean-subagent
  `web-design-reviewer` pass first, then run
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke` or
  `reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <route>` on the assembled tree
  and inspect the screenshot;
- stale-head check before recording the handoff; `chief-of-staff` re-checks it
  immediately before and after merge.

After the gate is complete, update the PR body while preserving the closing keywords and
add or replace this block:

```md
<!-- pr-pipeline-merge-gate -->
**PR Pipeline Merge Gate**
- status: ready-to-merge
- head: <sha>
- closes: #<issue>[, #<issue>]
- independent-review: pass; <review source>; risk=<small|larger>; why sufficient; findings fixed/dismissed
- codex-bot: <clean|exhausted>; `scripts/pr_review_status.py <pr> --once`
- ci: pass; `gh pr checks <pr>`
- tests: <commands run>
- docs: <updated / not required>
- visual: <not required / pass; web-design-reviewer subagent result + durable PR-visible screenshot proof>
- build-db: <not required / pass with durable PR-visible proof or dbdiff summary>
- stack: <none / after #pr / before #pr>
<!-- /pr-pipeline-merge-gate -->
```

The current-head `status: ready-to-merge` block is the single chief-of-staff handoff
indicator. Do not write it if any gate is missing, pending, stale, or only reported in
the local chat transcript. Use `status: blocked` with the missing item, or leave the
block incomplete and report what chief-of-staff must wait for. A later push makes the
block stale; rerun the gate on the new head and refresh the block.

Proof must survive a later chief-of-staff tick. For rendered changes, attach or comment
both the `web-design-reviewer` result and screenshot evidence on the PR; a local
`/tmp/reg-webapp-shots/` path is useful in the authoring thread but is not durable merge
evidence. For build-db, record the timestamped log path only if it is accessible to the
future merge runner, otherwise summarize the completed command, validation result, and
dbdiff in the PR body or a PR comment.

Run the real `build-db` last and once for build-affecting work, using the `build-db`
skill / `scripts/build_db_watch.py` so the run has a timestamped log, sparse progress,
post-build SQLite checks, and long-session polling. Use the main checkout's untracked
seed if working from a worktree. Add `--dbdiff-against <baseline-reg_meta.db>` when the
PR is expected to be content-neutral or to have a small inspected DB delta. Narrowing
with `--providers` is fine for a scoped dbdiff (e.g. `--providers scb,sos` for an
SCB/SOS-only change is faster than the full global build); a thin / non-SCB subset
builds and validates green end-to-end (the staleness, corpus-volume, and seed-drift
gates are scoped to the built providers). Pick the providers your PR affects, or omit
`--providers` for the full global set (release asset / cross-provider PRs). If the PR
changes any tracked `reg_meta_build/input_data/**` file (provider `*.toml`,
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
uv run --no-project python scripts/build_db_watch.py \
  --slug "<slug>" \
  --db-dir "$db_dir" \
  --input-dir "$input_dir"
```

Clean scratch outputs afterward:

```sh
rm -rf "$db_dir"
```

## Closeout

Report what changed, PR number/status, verification commands, review findings fixed or
dismissed, docs/test decisions, merge-gate block status, and any follow-up issues worth
filing. For multi-PR pipelines, report the intended merge order, but leave execution to
`chief-of-staff`. Default to fixing doc drift inline — it's part of this PR; record a
follow-up only when the fix needs its own scoped change, never as an escape hatch for a
one-liner. Before proposing a new issue, search open and closed issues with
`gh issue list --state all --search "<keywords>"`.
