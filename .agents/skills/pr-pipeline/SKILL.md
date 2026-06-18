---
name: pr-pipeline
description: >-
  Registry Research Toolkit PR development pipeline. Use when asked to run the PR
  pipeline workflow, including prompts like "$pr-pipeline issue 510"; develop issue(s),
  take a ranked lane through implementation, open draft PRs with closing keywords, run
  review/test/docs gates, mark PRs ready, or continue toward merge when the user
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
- Do not merge unless the user explicitly asked for merge/full pipeline or confirms at
  the merge gate. Otherwise finish by marking the PR ready and reporting the gate state.

## Intake

1. If the target is `next`, first run `plan-lanes`, pick the top coherent lane unless
   there is a clear reason not to, and confirm non-trivial choices with the user.
2. Read issue bodies, comments, Relationships, the parent epic, blockers, linked PRs,
   repository guidance (`AGENTS.md`; `CLAUDE.md` is intentionally equivalent for agent
   surfaces that use it), relevant `<package>/DESIGN.md`, and affected code.
3. Shape the smallest coherent PR set. Sequence by dependency. For multi-PR or ambiguous
   work, show the breakdown before editing.
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

Before edits, understand the relevant design docs. During edits, prefer existing
patterns and delete obsolete code directly.

Run focused verification as the work evolves:

- Python: `uv run ruff check`, `uv run ruff format --check`,
  `uvx --from ty==0.0.44 ty check`, and targeted `uv run python -m pytest <pkg>/`.
- Frontend: from `reg_webapp/frontend/`, use `bun run lint`, `bun run check`,
  `bun run test`, `bun run build`, and regenerate API types only after backend contract
  changes.
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
   explicitly dismiss every material finding with a reason.
4. Re-review substantial fixes until the review converges.
5. Update authored docs only where the diff made them stale: package `DESIGN.md`,
   README/CLI examples, docstrings, `ARCHITECTURE.md`, repository guidance files,
   validation-code docs. Never edit generated `reg_meta_build/docs/lisa/*.md`.
6. Commit and push any review/doc fixes. Never use `--no-verify` or `-n`; fix hook
   failures.

## Ready Or Merge Gate

Mark the PR ready when the code is near-final:

```sh
gh pr ready <pr>
```

For merge, satisfy the repo gate:

- independent review converged;
- CI green;
- bot review window settled on the current HEAD, including review-bot reactions/comments
  if present;
- real-data validation when build pipeline or DB content changed;
- stale-head check before and after merge.

Run the real `build-db` last and once for build-affecting work, using the main
checkout's untracked seed if working from a worktree. Do **not** pass `--providers` —
build the full global set. A restricted build (e.g. `--providers scb,sos`) orphans the
global thin providers' mandatory entity-key pins (#554) and hard-fails
`slug_variable_override_stale` (#563 tracks restoring provider-scoped builds). If the PR
changes any tracked `reg_meta_build/input_data/**` file (a provider's `*.toml`, a
`classifications/` or `scb_canonical/` CSV, etc.), the absolute `--input-dir` below
reads the main checkout's copy, so your change isn't built and the gate can miss a
DB-content regression — overlay the PR-HEAD tracked files onto the main seed
(symlink-merge) and point `--input-dir` there:

```sh
db_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-<slug>.XXXXXX")"
uv run reg-meta-build --db "$db_dir" build-db \
  --input-dir <main-checkout>/reg_meta_build/input_data
```

Clean scratch outputs afterward: `rm -r "$db_dir"`.

## Closeout

Report what changed, PR number/status, verification commands, review findings fixed or
dismissed, docs/test decisions, and any follow-up issues worth filing. Before proposing
a new issue, search open and closed issues with
`gh issue list --state all --search "<keywords>"`.
