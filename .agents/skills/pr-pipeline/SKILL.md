---
name: pr-pipeline
description: Registry Research Toolkit PR development pipeline for
  /Users/adam/Code/registry-research-toolkit. Use when asked to run the PR pipeline
  workflow, including prompts like "$pr-pipeline issue 510"; develop issue(s), take a
  ranked lane through implementation, open draft PRs with closing keywords, run
  review/test/docs gates, mark PRs ready, or continue toward merge when the user
  explicitly asks for merge.
---

# Registry PR Pipeline

## Scope

Turn an issue, a lane, or a feature request into one or more tightly scoped PRs.

Agent-surface notes:

- The lead agent implements directly by default.
- Use subagents only when the user explicitly asked for delegation/parallel agent work
  and the current environment allows it.
- Use the built-in review capability for the independent review pass when available. In
  the Codex app/CLI this means `/review`; on GitHub PRs, use the configured PR review
  flow and the bot-review window described by the repository guidance. Use
  `registry-code-review` only as an explicit fallback checklist when built-in review is
  unavailable or the user asks for it.
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

```sh
git fetch origin main
git checkout -b s/<slug> origin/main
git commit --allow-empty -m "wip: <scope>"
git push -u origin s/<slug>
gh pr create --draft --body-file <body-file>
```

The PR body must contain `Closes #<issue>` for each issue the PR resolves. Use
`--body-file`, not an inline heredoc.

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
2. Run the built-in review capability on the PR diff. Fix or explicitly dismiss every
   material finding with a reason. If built-in review is unavailable, run
   `registry-code-review` as a fallback checklist.
3. Re-review substantial fixes until the review converges.
4. Update authored docs only where the diff made them stale: package `DESIGN.md`,
   README/CLI examples, docstrings, `ARCHITECTURE.md`, repository guidance files,
   validation-code docs. Never edit generated `reg_meta_build/docs/lisa/*.md`.
5. Commit and push. Never use `--no-verify` or `-n`; fix hook failures.

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
checkout's untracked seed if working from a worktree:

```sh
reg-meta-build --db /tmp/regmeta-<slug> build-db \
  --input-dir /Users/adam/Code/registry-research-toolkit/reg_meta_build/input_data \
  --providers scb,sos
```

Clean scratch outputs afterward.

## Closeout

Report what changed, PR number/status, verification commands, review findings fixed or
dismissed, docs/test decisions, and any follow-up issues worth filing. Before proposing
a new issue, search open and closed issues with
`gh issue list --state all --search "<keywords>"`.
