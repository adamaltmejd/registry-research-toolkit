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
kept fresh by CI on every issue/PR event (`plan-sequence.yml` runs `--write`) plus a
daily cron, so the heartbeat's real job is the part CI can't do: re-rank the **lanes**
via `/plan-lanes` (forked) when they go stale vs the live state, surface what changed,
and propose structural fixes. CI is the reflex; this is the heartbeat and the push.

## The tick

`--epic <N>` defaults to `328`; pass the skill's argument through.

**Output budget — this runs every tick, so be terse.** A **quiet tick** (no status
delta; only benign new-file warnings) gets **one short line and nothing else**, e.g.
`✓ tick — no change · 0 errors · sleeping`. An **active tick** gets the deltas, the
**top 1–2 re-ranked lanes**, and real drift as a short bullet list — no preamble, no
restating the whole projection, no explaining the tooling. Don't narrate the steps; just
run them and report the result.

1. **One-fetch tick — projection delta + lanes staleness.** `--tick` builds the corpus
   **once** and emits both signals: the projection status delta (stderr) and the live
   lanes **basis** (stdout), exit-coding whether the lanes are stale. It is
   **read-only** — CI (`plan-sequence.yml`) on every event + the daily cron own the
   projection *write* now, so the loop no longer writes it. Capture the basis (you
   re-stamp it in step 3):

   ```sh
   basis="$(uv run --no-project python scripts/plan_sequence.py --tick --epic <N>)"
   ```

   - **stdout** (`$basis`) — the freshness basis to re-pass to `--write-lanes` if you
     re-rank.
   - **stderr** — the human-facing report: the projection delta (`newly ready: #…`,
     `left blocked: #…`, or `no status changes`) **and** the lanes verdict
     (`lanes: stale` / `lanes: fresh`).
   - **exit `0`** → lanes fresh, skip step 3. **exit `1`** → lanes stale, re-rank in
     step 3.

   The projection delta is for the human surface in step 4 — **not** the re-rank trigger
   (CI absorbs projection moves); the exit code is. (Need only one signal standalone?
   `--diff` prints just the delta; `--lanes-stale` just the basis + staleness.)

2. **Drift + release check** — read-only:

   ```sh
   uv run --no-project python scripts/check_issue_hygiene.py --all
   ```

3. **Lanes stale? Re-rank + persist.** Only if step 1 exited `1` (`lanes: stale`). The
   ready/running set — or one of those issues' `touches`/blockers/`priority` — moved
   since the last ranking. `$basis` now holds the state **as of step 1's check**. Invoke
   `/plan-lanes` via the `Skill` tool (it runs **forked**, so the corpus-reading stays
   out of this heartbeat's context, and returns the ranked lanes as markdown), then
   **persist** that markdown into the epic's `<!-- plan-lanes -->` block — `/plan-lanes`
   is read-only, so you are the writer — stamping the captured `$basis` (so the stamp
   matches the state the rank saw, not a post-rank recompute that could mark the new
   ranking fresh while it's already stale):

   ```sh
   printf '%s' "<the /plan-lanes markdown>" |
     uv run --no-project python scripts/plan_sequence.py --write-lanes --epic <N> --basis "$basis"
   ```

   Gating on staleness is the point: pay for lane-planning only when the work actually
   moved. The basis now stamps a signature over the ready/running issues'
   `touches`/blockers/`priority`, so a `touches`/`Relationships`/`priority` edit that
   moves no issue between sections trips it too (no longer the old acceptable miss).

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

   If step 1 reported `no status changes` + `lanes: fresh` and the only warnings are
   those benign new-file ones, say so in one line and stop — no noise.

5. **Propose fixes, tiered by risk:**
   - **Auto (safe):** the lanes re-rank in step 3, if it ran, is already persisted. (The
     projection is no longer written here — CI + cron own it.)
   - **Propose, don't apply:** close a done-but-open issue (cite the merged PR), wire a
     missing sub-issue (`gh issue edit <child> --parent <epic>`), add a missing
     area/type label, draft a `Relationships`/`touches` block. List them; apply only
     what the user approves — or only the unambiguous, low-risk ones if they've said to
     act.

6. **Stop.** `/loop` re-invokes on its cadence. Do not sleep or poll inside the tick.

## Notes

- Self-paced `/loop` keys cadence to activity — wake often when PRs are in flight, sleep
  long when quiet. CI + cron keep the projection fresh, so the heartbeat's "is there
  anything to do?" is the **`--tick` staleness exit** in step 1, not the projection
  delta (which CI usually already absorbed).
- The write mandate is narrow: **auto-refresh the lanes block only**, and only when it
  goes stale (step 3). The projection block is no longer written here — CI
  (`plan-sequence.yml`) + the daily cron own it. Everything structural (closing issues,
  labels, sub-issue wiring) stays propose-not-apply.
