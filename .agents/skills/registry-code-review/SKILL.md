---
name: registry-code-review
description: >-
  Registry Research Toolkit fallback code review checklist. Use only when the built-in
  review capability is unavailable, when the user explicitly asks for this skill, or
  when a repo-specific checklist is needed in addition to built-in review; do not use
  as the default independent review pass.
---

# Registry Code Review

## Scope

Review only. Prefer the built-in review capability for normal independent review. Use
this skill as an explicit fallback or supplemental repository checklist. Do not mutate
files, commit, push, regenerate artifacts, or apply fixes. Findings lead the response,
ordered by severity, with file and line references.

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
