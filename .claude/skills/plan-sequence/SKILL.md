---
name: plan-sequence
description: "Generate the issue-tracker sequencing projection — ready / running / blocked / parallel-safe + pending-release — from labels, Relationships, native sub-issues, `touches` globs, and open PRs, and splice it into the epic body (overwriting only the generated region). Use when asked to refresh the sequencing plan, update the epic, or see what's ready/blocked/parallel. Usage: /plan-sequence [epic-number]"
argument-hint: "[epic-number, default 328]"
disable-model-invocation: true
---

# plan-sequence

Turns the structured issue corpus into a **generated** sequencing view, so the plan is a
projection that's regenerated — never a hand-written narrative that rots. The render is
a marked block (`<!-- plan-sequence:start -->` … `<!-- plan-sequence:end -->`) spliced
into the epic body; everything **outside** the markers (lanes, decisions,
recommended-start narrative) is the human's and is preserved untouched.

The engine is `scripts/plan_sequence.py` (deterministic; pinned by
`scripts/tests/test_plan_sequence.py`). It reuses the hardened parsers/fetchers from the
hygiene validator. Read-only by default.

## What it computes

- **ready** — no open blocker, no open linked PR.
- **running** — an open PR closes it (`Closes #N` in the PR body →
  `closingIssuesReferences`).
- **blocked** — a `blocked` label, or an open `Depends on`/`Blocked by` target (issue
  *or* PR).
- **parallel-safe** — among ready issues, file-overlap of their `touches` globs is a
  set-intersection: disjoint sets run concurrently; overlapping sets are flagged to
  serialize. Issues with no `touches` are listed as "parallel-safety unknown".
- **pending release** — `reg_meta_build` DB content changed since the last
  `reg_meta_build/v*` tag (a rebuild+release is due).

Output is deterministic and sorted by issue number, with **no timestamp** — so
re-running with no state change leaves the body byte-identical (clean diffs).

## Steps

`--epic <N>` selects the target (default `328`); pass the skill's argument through.

1. **Preview (read-only):**

   ```sh
   uv run --no-project python scripts/plan_sequence.py --epic <N>
   ```

   Read the block. If you want to change the *narrative* (lane reasoning, decisions
   owed, recommended start), edit that in the epic body **outside** the markers — the
   script never touches it.

2. **Publish** — splice the generated block into the epic:

   ```sh
   uv run --no-project python scripts/plan_sequence.py --epic <N> --write
   ```

   This replaces only the marked region (or appends it once if the markers are absent),
   and prints the status delta vs the previous block. It is a mutating `gh issue edit` —
   show the user the rendered block (step 1) and get a go-ahead before `--write`.

## Carve a lane to dispatch

```sh
uv run --no-project python scripts/plan_sequence.py --lane
```

Read-only. Lists the **ready** issues that don't touch anything **in flight** (a running
issue's `touches`), grouped by area, with each issue's `touches` and a "must serialize"
hint for file-sharers. It deliberately does **not** pick the lane for you — *which*
issues go together and *how many* is a judgment call. Read the candidates, compose a
coherent lane, and dispatch it with `/pr-pipeline #… #…`.

**In-flight is tracked by the draft PR, not a label.** `/pr-pipeline` opens a draft PR
(`Closes #N`) for each issue up front, which makes those issues `running` — so the next
`--lane` automatically excludes them and anything touching their files. No claim step to
remember; merging the PR (or closing it) clears the marker.

## Notes

- **Backfill first for full value.** Parallel-safety needs `touches` blocks on issues,
  and lane grouping needs area labels. Until the corpus is backfilled (per the AGENTS.md
  "# Issue tracker" conventions), issues show `_no area_` and "parallel-safety unknown"
  — the projection still renders, just thinner.
- **Running detection is closing-keyword-based.** A child PR that doesn't use
  `Closes #N` won't show its issue as running. That's the trade-off for a deterministic,
  no-guess signal.
- This is the read/render half of the issue-tracking feature; the write-time enforcement
  half is `scripts/check_issue_hygiene.py` + `.github/workflows/issue-hygiene.yml`.
