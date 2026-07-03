---
name: plan-lanes
description: >-
  Compose the ready issues into ranked, parallel-safe candidate lanes — the agentic
  layer over `plan_sequence.py --lane`: it reads the issue bodies to add what
  set-intersection over `touches` can't (semantic conflicts, implicit blockers, what
  coheres into one PR-stream), then ranks. Runs forked so callers (issue-pulse,
  /pr-pipeline next, or you) get the ranked lanes back without the corpus-reading
  bloating their context.
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

   Just below the intro paragraph the output has a
   `Candidate set (N) — rank ONLY these … : #… #…` line — the `N` issue numbers sit on
   **that same line**, flat and sorted. **That line is the authoritative floor** — copy
   it verbatim and rank only those `N` numbers. The per-area lists below it are the same
   set with detail; the flat line is your checklist.

   `--lane` is **not** epic-filtered — it ranks over every open ready issue (the script
   ignores `--epic` on this path). That's fine: #328 is the umbrella epic. The epic arg
   you were passed scopes only the **narrative you read for ranking** (step 4) and the
   header label, not the candidate set.

   **Hard guardrail — the floor IS the candidate set.** Your candidate set is *exactly*
   the free candidates the floor just listed. Never add an issue the floor didn't list,
   even if the epic narrative or comments mention it — those threads contain shipped and
   historical issues (closed work the narrative never pruned; e.g. #328's consolidated
   plan still names long-since-merged lanes). Steps 2 and 4 read issue bodies and the
   epic narrative **only to order and prioritize the step-1 candidates, never to expand
   the set.** If a number you're about to rank isn't in the step-1 floor, drop it — it's
   contamination, not a candidate.

2. **Read the candidates.** For each free candidate **from step 1 — and no others**,
   read its body + comments + `Relationships` (`gh issue view <n> --comments`). You're
   hunting for the four things the floor can't see (above): implicit blockers, semantic
   conflicts, coherence, and stale/missing `touches`. This read informs *ordering and
   grouping*; it never adds a number the floor didn't list.

3. **Compose lanes.** A **lane** is a coherent unit of work to hand to one
   `/pr-pipeline` run — one issue, or a few that belong together (a lane may span
   several PRs). Rules:
   - Lanes should be **mutually file-disjoint** so several can run concurrently. Honor
     the script's must-serialize groups: members of one group go in the *same* lane
     (serialized inside it) or you flag a cross-lane order.
   - Fold in semantic conflicts you found: if #A and #B both bump the schema, they are
     NOT parallel — same lane or an explicit "run #A before #B."
   - Keep lanes small and coherent — don't bundle unrelated areas just to fill one.

4. **Rank.** In order: **priority bucket** first — `--lane` annotates each candidate's
   `priority:*` label (`[high]`/`[low]`; unmarked = normal) and lists the buckets under
   "Priority (rank by this first)". A lane's bucket is its highest-priority member: any
   `[high]` member ranks the lane above all-normal lanes, `[low]`-only lanes rank last.
   Within a bucket, break ties by **unblocking power** (a lane that clears a blocker
   gating downstream work ranks first), then **maintainer signal** from the epic
   narrative / open-question threads, then **smallest coherent** (fastest to land, frees
   its files). Where the epic prose names a finer priority than the labels, defer to it.
   The narrative is an **ordering signal only** — it lists shipped/closed issues and
   stale lane names, so never let it introduce a candidate the step-1 floor didn't list
   (see the hard guardrail in step 1).

5. **Self-check against the `Candidate set` line (mandatory before returning).** Take
   the flat `Candidate set (N) — … : #… #…` line from step 1 as the source of truth.
   Confirm two things: (a) every issue number you **rank in a lane** is on that line —
   drop any that isn't (it came from the narrative: a shipped/closed/blocked issue); (b)
   every number on that line is **accounted for** — either ranked in a lane, or, if step
   2 revealed it's actually blocked/not-ready despite being on the floor (an implicit
   blocker the `touches` floor couldn't see), moved to the **Held/Notes** line with a
   one-line reason. Never silently drop a floor number, and never force a
   discovered-blocked one into a ranked lane — `/pr-pipeline next` would dispatch it as
   runnable. Each of the `N` candidate numbers must appear **exactly once**, across your
   ranked lanes plus the Held/Notes lines. The anti-contamination ban binds the **ranked
   lanes** — every number you *rank* must be a candidate. Other parts of the output may
   carry non-candidate numbers, but each must trace to the live floor, **never the epic
   narrative**: the header's own `epic #<N>`; the floor's `In-flight PRs:` PR numbers;
   and the floor's held issue numbers with their `← PR #<p>`, copied onto the `Held`
   line from the floor's `Held — …` line (those are excluded from the candidate set
   precisely because they touch in-flight work — keep them as blocker context, just
   never rank them). A PR number is never ranked; a floor-held issue is held, not
   ranked. Then set the header `open issues <count>` to `N` exactly; a mismatch means
   contamination or a dropped candidate — fix it and recount.

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

Fill the header's `in-flight PRs:` from the floor's `In-flight PRs:` line verbatim (or
`none` if the floor has no such line); fill each `Held` line's `← PR #<p>` from the
floor's `Held — … ← PR #…` line. Both come from the live `--lane` output — **never**
from the epic narrative or comments (the stale source this skill fences off).

The header's `open issues <count>` + in-flight list is the **freshness stamp** — it's
content-derived, not a timestamp, so the block stays diff-stable when nothing changed,
and callers can see what graph the ranking was computed against. `<count>` is exactly
`N` from the floor's `Candidate set (N) …` line — and every issue number you rank must
be one of those `N` (see the step-5 self-check). If the count doesn't match, you've
either pulled in narrative contamination or dropped a candidate — reconcile and recount.
Make it accurate to the floor you just read; callers re-rank live rather than trusting a
stale copy.

## Boundaries

- **Read-only.** No `gh issue edit`, no PR creation, no file writes — you rank and
  **return** the markdown; `/issue-pulse` is the single writer that splices it into the
  epic (via `plan_sequence.py --write-lanes`). A human or `/pr-pipeline` calling you
  just reads the return value.
- **Trust the floor on conflicts, augment on everything else.** Never downgrade a
  script-declared must-serialize to parallel-safe.
- **The floor is the whole candidate set.** Never rank an issue the step-1 floor didn't
  list — the epic narrative names shipped/closed work, and that is ordering context, not
  a candidate source (see step 1's hard guardrail).
- **Priority labels are the primary key; don't invent finer priority.** Rank by the
  `priority:*` buckets the floor reports, then unblocking-power + size. If no issue is
  labelled, every lane is normal — fall back to unblocking-power + size and leave the
  final pick to the caller. Don't manufacture a priority the labels/prose don't state.
