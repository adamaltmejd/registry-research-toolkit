---
name: plan-sequence
description: >-
  Registry Research Toolkit issue sequencing workflow. Use when asked to run the
  plan-sequence workflow, preview or refresh epic #328 sequencing, inspect
  ready/running/parked/blocked/parallel-safe issues, update the generated plan-sequence
  block, or carve the deterministic ready lane floor with scripts/plan_sequence.py.
---

# Registry Plan Sequence

## Scope

Run this from the repository root of the current Registry Research Toolkit checkout.

The generated projection lives in an epic body between `<!-- plan-sequence:start -->`
and `<!-- plan-sequence:end -->`. Treat everything inside those markers as script-owned.
Do not hand-edit it.

## Preview

Default epic is `328`; pass another number only if the user asks.

```sh
git fetch --tags origin
uv run --no-project python scripts/plan_sequence.py --epic <N>
```

Report the generated block and any obvious issue hygiene problems. Keep the answer
focused on what is ready, running, parked, blocked, parallel-safe, and pending release.

## Publish

Writing mutates GitHub issue text through `gh issue edit`. Publish only when the user
explicitly asked to refresh/update the epic, or after showing the preview and getting a
go-ahead.

```sh
git fetch --tags origin
uv run --no-project python scripts/plan_sequence.py --epic <N> --write
```

The script replaces only the generated region or appends it once if markers are absent.
It should produce no timestamp-driven churn.

## Lane Floor

For a read-only list of ready issues not blocked by in-flight PR touches:

```sh
git fetch --tags origin
uv run --no-project python scripts/plan_sequence.py --lane
```

Use this as the deterministic floor for `plan-lanes`. It is not the final judgment
layer: it does file-set intersection from declared `touches`, but cannot infer semantic
conflicts.

## Interpretation

- Ready: no open blocker, no `parked` label, and no open linked PR.
- Running: an open PR closes the issue.
- Parked: `parked` label; excluded from the lane floor without requiring a synthetic
  blocker.
- Blocked: `blocked` label or open `Depends on` / `Blocked by` target.
- Parallel-safe: ready issues with disjoint declared `touches` sets.
- Pending release: `reg_meta_build` DB content changed since the last
  `reg_meta_build/v*` tag.

If output seems surprising, inspect the issue body, labels, native sub-issues,
relationships, and open PR body closing keywords before assuming the script is wrong.
