---
name: plan-lanes
description: >-
  Registry Research Toolkit lane-ranking workflow. Use when asked to run the
  plan-lanes workflow, choose the next issue lane, rank ready issues, interpret
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

3. For each candidate, read the issue body and comments:

   ```sh
   gh issue view <n> --comments
   ```

   Look for semantic conflicts with no file overlap, implicit blockers, coherence across
   issues, stale or missing `touches`, priority labels, and relationships.

4. Compose small coherent lanes. Never downgrade a script-declared must-serialize group
   to parallel-safe. If a candidate is actually blocked despite appearing in the floor,
   place it in Held/Notes with a one-line reason rather than ranking it.

5. Rank by priority bucket first: any `priority:high` member beats normal;
   `priority:low`-only lanes go last. Within a bucket, prefer unblocking power,
   maintainer signal in the epic narrative, and smallest coherent work.

6. Self-check: every candidate from the flat line appears exactly once across ranked
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
