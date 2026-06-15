---
name: plan-lanes
description: "Compose the ready issues into ranked, parallel-safe candidate lanes — the agentic layer over `plan_sequence.py --lane`: it reads the issue bodies to add what set-intersection over `touches` can't (semantic conflicts, implicit blockers, what coheres into one PR-stream), then ranks. Runs forked so callers (issue-pulse, /pr-pipeline next, or you) get the ranked lanes back without the corpus-reading bloating their context."
argument-hint: "[epic-number, default 328]"
context: fork
agent: Explore
---

# plan-lanes — rank the ready work into runnable lanes

You produce **ranked, parallel-safe candidate lanes** from the open-issue corpus and
**return them** — you do not open PRs, edit issues, or write files. Your final message
IS the result handed back to the caller (issue-pulse surfaces the top of it;
`/pr-pipeline next` picks one; a human reads it). Be terse and structured.

The epic argument defaults to `328`. It is the **ranking-context** epic (whose narrative
you read in step 4), not a filter on the candidate set — see step 1.

## Why this is agentic, not just the script

`scripts/plan_sequence.py --lane` already computes the **deterministic floor**: which
ready issues don't touch in-flight (open-PR) work, grouped by area, with declared
`touches` and the must-serialize groups (issues whose `touches` overlap). That floor is
a conservative set-intersection over **self-declared** file lists. It cannot see:

- **Semantic conflicts with no file overlap** — two issues that both bump
  `SCHEMA_VERSION`, both regenerate the same DB asset, or that must land in a fixed
  order even though their `touches` are disjoint.
- **Implicit blockers** not written as `Depends on #N` — a body that needs the artifact
  another open issue produces, where the `Relationships` block never said so.
- **Coherence** — which issues are really *one* PR-stream vs. independent.
- **Wrong/missing `touches`** — declarations can be stale or absent, so the floor
  under-detects.

Your job is to take the floor and **augment it with judgment**, never silently
contradict it: if the script says two issues must serialize, they share a lane (or
serialize across lanes) — never call them parallel-safe.

## The tick

1. **Get the deterministic floor.**

   ```sh
   uv run --no-project python scripts/plan_sequence.py --lane
   ```

   That gives you the free candidates (by area, with `touches`), the must-serialize
   groups, and what's held (touches in-flight). If it says "No ready issues free of
   in-flight conflicts," report exactly that and stop.

   `--lane` is **not** epic-filtered — it ranks over every open ready issue (the script
   ignores `--epic` on this path). That's fine: #328 is the umbrella epic. The epic arg
   you were passed scopes only the **narrative you read for ranking** (step 4) and the
   header label, not the candidate set.

2. **Read the candidates.** For each free candidate, read its body + comments +
   `Relationships` (`gh issue view <n> --comments`). You're hunting for the four things
   the floor can't see (above): implicit blockers, semantic conflicts, coherence, and
   stale/missing `touches`.

3. **Compose lanes.** A **lane** is a coherent unit of work to hand to one
   `/pr-pipeline` run — one issue, or a few that belong together (a lane may span
   several PRs). Rules:
   - Lanes should be **mutually file-disjoint** so several can run concurrently. Honor
     the script's must-serialize groups: members of one group go in the *same* lane
     (serialized inside it) or you flag a cross-lane order.
   - Fold in semantic conflicts you found: if #A and #B both bump the schema, they are
     NOT parallel — same lane or an explicit "run #A before #B."
   - Keep lanes small and coherent — don't bundle unrelated areas just to fill one.

4. **Rank.** No priority labels exist, so rank by, in order: **unblocking power** (a
   lane that clears a blocker gating downstream work ranks first), then **maintainer
   signal** from the epic narrative / open-question threads, then **smallest coherent**
   (fastest to land, frees its files). Note where the epic prose already names a
   priority — defer to it.

## Return format

Your final message is **markdown** — it is both the answer the caller reads AND the body
`/issue-pulse` persists into the epic's `<!-- plan-lanes -->` block (the script frames
it; you supply the content). No preamble, no fences around the whole thing. Use exactly
this shape:

```md
**Lanes (ranked)** — epic #<N> · open issues <count> · in-flight PRs: <#…, or none>

1. **<lane label>** — #<n>[, #<n>…] · `<area>`
   - why: <one line — what it does / why it ranks here>
   - parallel: lanes <list>, or none
   - caveat: <serialize/order/blocker note> _(omit the line if none)_
2. …

**Run concurrently now:** lanes <a>+<b>+… (file-disjoint)
**Held** (touch in-flight work): #<n> ← PR #<p>; … _(or "none")_
**Notes:** <implicit blockers / semantic conflicts the `touches` floor missed> _(omit if none)_
```

The header's `open issues <count>` + in-flight list is the **freshness stamp** — it's
content-derived, not a timestamp, so the block stays diff-stable when nothing changed,
and callers can see what graph the ranking was computed against. Make it accurate to
what you just read; callers re-rank live rather than trusting a stale copy.

## Boundaries

- **Read-only.** No `gh issue edit`, no PR creation, no file writes — you rank and
  **return** the markdown; `/issue-pulse` is the single writer that splices it into the
  epic (via `plan_sequence.py --write-lanes`). A human or `/pr-pipeline` calling you
  just reads the return value.
- **Trust the floor on conflicts, augment on everything else.** Never downgrade a
  script-declared must-serialize to parallel-safe.
- **Don't invent priority.** If nothing signals priority, say the ranking is by
  unblocking-power + size and leave the final pick to the caller.
