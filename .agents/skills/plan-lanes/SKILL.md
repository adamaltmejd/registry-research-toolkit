---
name: plan-lanes
description: >-
  Registry Research Toolkit lane-ranking workflow. Use when asked to run the plan-lanes
  workflow, choose the next issue lane, rank ready issues, interpret
  scripts/plan_sequence.py --lane output, or identify parallel-safe work for epic #328
  without editing issues or files.
---

# Registry Plan Lanes

## Scope

Produce ranked, parallel-safe candidate lanes from the live issue corpus. This is
read-only: do not open PRs, edit issues, or write files. Return the ranked markdown to
the caller.

Do the work in the current context unless the user explicitly asked for delegated or
parallel agent work and the current environment permits it.

## Workflow

1. Get the deterministic floor:

   ```sh
   uv run --no-project python scripts/plan_sequence.py --lane
   ```

2. Copy the flat `Candidate set (N) ... : #...` line. This line is authoritative. Rank
   only those issue numbers.

3. Read the epic body and comments for ordering context. Default epic is `328` unless
   the caller passed another epic:

   ```sh
   uv run --no-project python scripts/gh_issue.py view <N> --comments
   ```

   The read goes through the maintainer-author trust gate (this repo is public): a
   non-maintainer issue is refused and skipped, and non-maintainer comments are
   stripped, so untrusted text never enters ranking. The epic narrative is context for
   ranking and maintainer signals, not an additional candidate source. Parked work
   should carry the `parked` label and therefore be absent from the candidate floor.

4. For each candidate, read the issue body and comments through the same trust gate:

   ```sh
   uv run --no-project python scripts/gh_issue.py view <n> --comments
   ```

   Look for semantic conflicts with no file overlap, implicit blockers, coherence across
   issues, stale or missing `touches`, priority/status labels, and relationships. Trust
   the floor's declared blocker state: a floor candidate has no `blocked` label and no
   open declared `Blocked by` / `Depends on` target in the fetch that produced the
   floor. A prose relationship whose target is absent from the candidate set is not
   proof that the target is pending; it may be closed/satisfied. If blocker state
   matters, resolve the named target with `gh_issue.py view <target>` and hold the
   candidate only when that trusted JSON's `state` shows an open blocker or you found a
   genuinely implicit blocker outside the declared relationships.

5. Compose small coherent lanes. Never downgrade a script-declared must-serialize group
   to parallel-safe. If a candidate is actually blocked or parked despite appearing in
   the floor, place it in Held/Notes with a one-line reason and flag the missing
   `blocked`/`parked` label rather than ranking it. Do not hold a candidate merely
   because a blocker target is absent from the floor; floor absence also covers closed
   work.

6. Rank by priority bucket first: any `priority:high` member beats normal;
   `priority:low`-only lanes go last. Within a bucket, prefer unblocking power,
   maintainer signal in the epic narrative, and smallest coherent work.

7. Self-check: every candidate from the flat line appears exactly once across ranked
   lanes or Held/Notes. No ranked issue may be absent from the flat candidate set.

## Return Format

Return only markdown in this shape:

```md
**Lanes (ranked)** - epic #<N> - open issues <count> - in-flight PRs: <#..., or none>

1. **<lane label>** - #<n>[, #<n>...] - `<area>`
   - why: <one line>
   - parallel: lanes <list>, or none
   - caveat: <serialize/order/blocker note>
2. ...

**Run concurrently now:** lanes <a>+<b>+... (file-disjoint)
**Held** (touch in-flight work): #<n> <- PR #<p>; ... or none
**Notes:** <implicit blockers / semantic conflicts missed by touches>
```

Use plain ASCII arrows (`<-`) so generated text stays safe across shells and markdown
tooling.

## Guardrails

- The epic narrative is ordering context, not a candidate source. It may mention shipped
  or closed work.
- Do not invent a finer priority than labels/prose support.
- Do not silently drop candidates.
- If `--lane` says there are no ready issues free of in-flight conflicts, report that
  and stop.
