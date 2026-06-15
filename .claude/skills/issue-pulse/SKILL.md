---
name: issue-pulse
description: "One heartbeat of the issue-tracker — refresh the generated sequencing projection in the epic body, check for drift (stale labels, done-but-open, merged-but-unreleased build content), and surface what changed plus propose fixes. Built to run on a cadence via /loop. Usage: /loop [interval] /issue-pulse"
argument-hint: "[epic-number, default 328]"
---

# issue-pulse — one tick of the issue-tracking heartbeat

Run on a cadence with `/loop` (`/loop 30m /issue-pulse [epic-number]`, or bare
`/loop /issue-pulse` to self-pace the default epic). Each invocation does **one tick and
stops** — `/loop` handles re-invocation and sleeping.

This is the agentic layer over the deterministic scripts. The **projection** block is
kept fresh by CI on every issue/PR event (`plan-sequence.yml` runs `--write`), so the
heartbeat's real job is the part CI can't do: re-rank the **lanes** via `/plan-lanes`
(forked) when they go stale vs the live ready/running sets, surface what changed, and
propose structural fixes. CI is the reflex; this is the heartbeat and the push.

## The tick

`--epic <N>` defaults to `328`; pass the skill's argument through.

**Output budget — this runs every tick, so be terse.** A **quiet tick** (no status
delta; only benign new-file warnings) gets **one short line and nothing else**, e.g.
`✓ tick — no change · 0 errors · sleeping`. An **active tick** gets the deltas, the
**top 1–2 re-ranked lanes**, and real drift as a short bullet list — no preamble, no
restating the whole projection, no explaining the tooling. Don't narrate the steps; just
run them and report the result.

1. **Refresh the projection + capture the delta** — `--write` computes the status delta
   against the epic's *current* block **before** splicing, so this one call both updates
   the body and tells you what changed:

   ```sh
   uv run --no-project python scripts/plan_sequence.py --epic <N> --write
   ```

   - `Updated #<N>` + a delta (`newly ready: #…`, `left blocked: #…`) → it **changed**.
   - `already up to date` → nothing changed since the block was last written.

   CI (`plan-sequence.yml`) usually wrote it on the triggering event already, so this is
   mostly an idempotent safety net + the source of the human-facing delta. (The splice
   overwrites only the marked region — bounded and reversible.) **Don't** use this delta
   to decide whether to re-rank lanes — CI absorbs it; step 3 keys off the lanes' own
   basis instead.

2. **Drift + release check** — read-only:

   ```sh
   uv run --no-project python scripts/check_issue_hygiene.py --all
   ```

3. **Lanes stale? Re-rank + persist.** Ask the script whether the agentic lanes still
   match the live ready/running sets (this keys off the lanes block's own basis, **not**
   the step-1 delta — CI absorbs that):

   ```sh
   uv run --no-project python scripts/plan_sequence.py --lanes-stale --epic <N>
   ```

   - `fresh` (exit 0) → skip both; nothing to re-rank.

   - `stale` (exit 1) → the ready or in-flight set moved since the last ranking: invoke
     `/plan-lanes` via the `Skill` tool (it runs **forked**, so the corpus-reading stays
     out of this heartbeat's context, and returns the ranked lanes as markdown), then
     **persist** that markdown into the epic's `<!-- plan-lanes -->` block —
     `/plan-lanes` is read-only, so you are the writer — by piping its return to:

     ```sh
     uv run --no-project python scripts/plan_sequence.py --write-lanes --epic <N>
     ```

   Gating on staleness is the point: pay for lane-planning only when the ready work
   actually moved. (A `touches`/`Relationships` edit that moves no issue's section won't
   trip this — an acceptable miss for now.)

4. **Surface the DELTAS, not the whole state.** Report only:
   - the status delta from step 1 (newly ready / newly unblocked / newly running, or
     what left a section);
   - the **top 1–2 lanes** from step 3 if it ran (label + members + one-line why, and
     which are concurrently runnable) — omit on a quiet tick;
   - **pending release** (merged-but-unreleased `reg_meta_build`);
   - **decisions owed** (open questions gating downstream work);
   - **drift** from the hygiene check (missing labels, stale `blocked`, done-but-open,
     half-wired sub-issues).

   `touches … matches no files (ok if it's a new file)` warnings are **benign and
   recurring** — issues legitimately point `touches` at paths a future PR will create.
   Don't re-surface them every tick; only flag one if it looks like a typo (a real path
   gone wrong) or is new since the last tick.

   If step 1 said `already up to date` and the only warnings are those benign new-file
   ones, say so in one line and stop — no noise.

5. **Propose fixes, tiered by risk:**
   - **Auto (safe):** the projection refresh in step 1 is already applied.
   - **Propose, don't apply:** close a done-but-open issue (cite the merged PR), wire a
     missing sub-issue (`gh issue edit <child> --parent <epic>`), add a missing
     area/type label, draft a `Relationships`/`touches` block. List them; apply only
     what the user approves — or only the unambiguous, low-risk ones if they've said to
     act.

6. **Stop.** `/loop` re-invokes on its cadence. Do not sleep or poll inside the tick.

## Notes

- Self-paced `/loop` keys cadence to activity — wake often when PRs are in flight, sleep
  long when quiet. The projection's deterministic, timestamp-free output makes "did
  anything change?" the byte-diff in step 1's message.
- The write mandate is narrow: **auto-refresh the two generated blocks only** — the
  projection (step 1) and, on a material tick, the lanes (step 3). Everything structural
  (closing issues, labels, sub-issue wiring) stays propose-not-apply.
