---
name: chief-of-staff
description: "Run one registry chief-of-staff tick: invoke /issue-pulse, inspect live issue and PR claim state, automatically maintain issue metadata/priorities, squash-merge PRs with current-head pr-pipeline handoff evidence, run /release minor when a merge creates a required build/release boundary, and recommend the next safe /pr-pipeline lanes. Usage: /loop 30m /chief-of-staff"
---

# chief-of-staff — one coordination tick

The chief of staff is the repo's coordination agent: it keeps issue metadata and lane
priorities current, understands active `/pr-pipeline` claims, prevents conflicting work,
merges PRs with a current-head pipeline handoff, and recommends the next work to launch
in separate worktrees.

This skill is designed for scheduled use. Run one tick to completion, then stop; `/loop`
or another external automation owns the cadence.

## Startup Gate

Run only from `/Users/adam/Code/registry-research-toolkit`, the canonical main checkout.
Do not run from a git worktree.

Before `/issue-pulse`, issue edits, PR inspection, merge, or lane recommendation:

1. Verify the repo top-level is exactly `/Users/adam/Code/registry-research-toolkit`
   with `git rev-parse --show-toplevel`, the current branch is `main` with
   `git branch --show-current`, and `.git` is a directory with `test -d .git`. A linked
   worktree has a `.git` file, so it must fail this gate. If any check fails, stop and
   tell the user to fix the checkout before relaunching.
2. Run `git pull --ff-only` as the first sync action.
3. Re-verify the checkout is the canonical main checkout on `main`, then run
   `git status --short`. If the repo is dirty, stop and tell the user to clean or commit
   the main checkout before relaunching.

Implementation work belongs in separate worktrees via `/pr-pipeline`. From the main
checkout, this skill may coordinate issues, inspect PRs, merge ready PRs, and recommend
new work, but it must not edit project code as part of the work itself.

## Tick

1. Complete the startup gate above. Stop immediately if it fails.
2. Invoke `/issue-pulse` exactly once. Let it update only the lanes block; apply
   structural issue maintenance afterward under this skill's maintenance policy.
3. Build the operating picture:
   - run `uv run --no-project python scripts/plan_sequence.py --lane`;
   - read issue `#328` and current candidate issue bodies/comments;
   - inspect open PRs that close issues, especially drafts, ready PRs, and stacks.
4. Apply clear, evidence-backed issue maintenance automatically.
5. Merge ready PRs only through the automerge gate below.
6. Re-run the live lane floor if any merge or issue edit changed the projection, then
   recommend the next safe `/pr-pipeline issue ...` command or say to wait.

## Automerge

Merge only on the current head and only when every item passes:

- PR is open, non-draft, mergeable, based on `main`, and not blocked by stack order or a
  maintainer stop note.
- PR body contains `<!-- pr-pipeline-merge-gate -->` with `status: ready-to-merge` and
  `head: <sha>` matching GitHub's current `headRefOid`. This single current-head block
  is the `/pr-pipeline` handoff signal; no separate ready-to-merge comment is required.
- The gate block records risk-scaled independent review, tests/checks, docs decisions,
  and required visual/build-db proof. Bot-only review is sufficient only for small,
  low-risk PRs.
- Required visual/build-db proof is durable and PR-visible; local `/tmp` paths alone do
  not pass a later tick.
- `gh pr checks <pr>` is green on the current head.
- `uv run --no-project python scripts/pr_review_status.py <pr> --once` is settled with
  `clean`, or `exhausted` with all other gates complete. `findings`, `reviewing`,
  `none`, or tool errors block automerge.
- Stale-head check passes immediately before merge, and the merge command uses
  `--match-head-commit` with that same SHA.
- For stacked PRs, branch deletion cannot close or break dependent PRs. Before merging a
  stack predecessor, inspect open successors' `baseRefName` and `headRefName`. If a
  successor is based on the predecessor branch, prevent branch deletion or retarget the
  successor immediately after merge, then verify it remains open on the intended head.
  After any retarget, require the successor branch to be rebased or otherwise updated
  onto the new base, then regenerate checks, Codex bot review, independent-review
  judgment, and the merge-gate block before automerging it.

Merge one PR at a time:

```sh
gh pr merge <pr> --squash --match-head-commit <headRefOid>
```

After each merge, fetch `origin main`, fast-forward the local main checkout with
`git merge --ff-only origin/main`, and verify the PR's changes are actually present on
`main`, not merely that GitHub reports a merge commit. For a stack, re-check the next
PR's head, checks, bot signal, mergeability, durable proof, and gate block before
merging it.

If the merge creates a required build/release boundary, such as DB content that must be
published before dependent work can proceed, invoke `/release minor`. Let the release
skill resolve package scope and run its own gates; stop if it requests input or if the
required bump is not a minor release.

If a ready PR lacks a current-head Codex verdict, comment `@codex review` only when the
gate block shows implementation is finished, then skip the merge until a later tick
observes a settled signal.

## Issue Maintenance

Keep the tracker current without asking for every mechanical edit:

- required area/type/status/priority-label hygiene;
- priority labels when explicit maintainer signal or dependency graph evidence makes the
  update mechanical;
- parent/sub-issue wiring already stated by `Part of #...`;
- clear `Relationships` and `touches` fixes;
- closing issues whose closing PR is merged and verified on `main`;
- stale `blocked` / `parked` label corrections grounded in current blockers or
  maintainer comments.

Ask only when the edit would choose product direction, change issue scope, invent a
priority without evidence, unpark maintainer-deferred work without an explicit resume
signal, close disputed/partial work, create a new issue, delete substantive prose, or
resolve contradictory live signals.

## Lane Recommendation

- Do not recommend blocked, parked, held, pending-release, already claimed, or
  insufficiently described work.
- Keep active concurrency small: default to 2-3 independent lanes maximum, and only one
  high-cost or build-affecting lane at a time.
- Avoid overlapping `reg_meta_build` / build-db work while another open pipeline touches
  build, input-data, curation, or release surfaces.
- Prefer a small coherent bundle or explicit stack over a broad backlog summary.

Return concise output:

```text
chief tick: <fresh/restamped/reranked>; <hygiene>; <active lanes>
Merged: PR #<p> -> <merge sha>, or none
Recommended next: /pr-pipeline issue <n>[,<m>] or none
Issue maintenance: applied <...>; needs input <... or none>
Watch: <blocked decision, pending release, stale review, or next trigger>
```
