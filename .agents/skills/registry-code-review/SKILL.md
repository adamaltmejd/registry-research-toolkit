---
name: registry-code-review
description: >-
  Registry Research Toolkit callable code review workflow. Use when asked to review a
  Registry PR, branch, range, or working-tree diff; when pr-pipeline needs a callable
  review pass because slash-command review is unavailable; or when a repo-specific
  review checklist is needed. Review only; do not mutate files.
---

# Registry Code Review

## Scope

Review only. This is the repo-scoped callable review workflow for Registry PRs/diffs.
Use it when the top-level built-in review command is unavailable from the current
workflow or when explicitly asked. In `pr-pipeline`, run this skill in a fresh subagent
when available so the review is independent of the authoring session. The subagent
reports findings back to the lead agent; it does not mutate files, commit, push,
regenerate artifacts, apply fixes, or post GitHub comments unless explicitly instructed.
Findings lead the response, ordered by severity, with file and line references.

When this skill is run by the same session that authored the patch because subagents are
unavailable, state that review surface in the closeout. It is a diagnostic checklist,
not independent review evidence, and must not satisfy the `pr-pipeline` ready/merge gate
by itself.

## Inputs

Accept a PR number, branch/range, or current working-tree diff. For GitHub PRs, gather:

```sh
gh pr view <pr> --json number,title,body,headRefOid,baseRefName,headRefName,closingIssuesReferences
gh pr diff <pr>
gh pr view <pr> --comments
gh api "repos/<owner>/<repo>/pulls/<pr>/comments"
```

Read linked issues, comments, repository guidance (`AGENTS.md`; `CLAUDE.md` is
intentionally equivalent for agent surfaces that use it), relevant
`<package>/DESIGN.md`, `ARCHITECTURE.md` for cross-package work, and touched code.

## Review Method

1. Establish context before reading line-by-line:
   - identify the intended behavior from the issue/PR body and linked comments;
   - note PR size, touched packages, generated files, and CI/check status;
   - decide whether the diff needs package design docs, workflow graph inspection,
     frontend smoke testing, or real-data validation evidence.
2. Do a high-level pass:
   - compare the implementation shape to the issue scope and repo architecture;
   - inspect changed public contracts, schemas, CLIs, API responses, workflows, and
     persisted/generated artifacts;
   - check whether tests exercise the behavior that could actually regress.
3. Do a line-level pass:
   - trace changed control/data flow through call sites, not just the edited lines;
   - search for existing helpers or adjacent patterns before accepting new abstractions;
   - verify edge cases, failure paths, cleanup, determinism, and boundary validation.
4. Re-review only the current head. On follow-up passes, confirm which prior findings
   were fixed and report only remaining material issues.

## Review Lens

Look for material problems in the changed behavior:

- logic bugs, inverted conditions, off-by-one errors, missing `None`/empty handling,
  resource leaks;
- contract violations at JSON read/write boundaries, validation findings, exit codes,
  schema `extra=forbid` behavior;
- issue/spec mismatch and edge cases called out by the issue but not implemented;
- MONA/data-safety violations or accidental row-level/PII leakage;
- determinism and regeneration risks: ordering, seeds, DDL changes requiring schema
  version bumps, stale generated assets;
- tests that are tautological, too broad, or fail to assert the regression;
- repo convention drift: Pydantic only on `reg_schema` and FastAPI surfaces, stdlib
  sqlite for library DBs, argparse CLIs, no shims/migrations/dead code pre-v1.

Run tests only to confirm a concrete suspicion or verify risk; do not report
linter/type/format nits that existing checks already catch.

## Bar

Report only findings you are highly confident are real and material. Avoid pre-existing
issues outside the PR's changed lines unless they invalidate the PR. Do not pad with
style nits.

On re-review, focus on the new diff and whether prior findings were resolved. Say
`converged - no further findings` when nothing material remains.

## Output

Use this shape:

```md
Findings:
- blocking: path/to/file.py:123 - Concrete failure and why it matters.
- non-blocking: path/to/file.py:45 - Worth fixing, but not merge-blocking.
- question: path/to/file.py:67 - Ambiguity that needs author intent.

Open questions:
- ...

Test gaps / residual risk:
- ...

blocking findings remain
```

If there are no issues, say that clearly and include any tests you did or did not run.
