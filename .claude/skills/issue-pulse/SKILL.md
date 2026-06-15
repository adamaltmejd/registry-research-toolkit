---
name: issue-pulse
description: "One heartbeat of the issue-tracker — refresh the generated sequencing projection in the epic body, check for drift (stale labels, done-but-open, merged-but-unreleased build content), and surface what changed plus propose fixes. Built to run on a cadence via /loop. Usage: /loop [interval] /issue-pulse"
argument-hint: "[epic-number, default 328]"
disable-model-invocation: true
---

# issue-pulse — one tick of the issue-tracking heartbeat

Run on a cadence with `/loop` (`/loop 30m /issue-pulse`, or bare `/loop /issue-pulse` to
self-pace). Each invocation does **one tick and stops** — `/loop` handles re-invocation
and sleeping.

This is the agentic layer over the deterministic scripts: it doesn't recompute anything,
it runs them and adds judgment (deltas + proposals). CI (`issue-hygiene.yml`) is the
reflex; this is the heartbeat and the push.

## The tick

1. **Refresh the projection** — bounded and reversible (the splice overwrites only the
   marked region):

   ```sh
   uv run --no-project python scripts/plan_sequence.py --write 328
   ```

   - `Spliced …` → the projection **changed** since the last tick.
   - `already up to date` → nothing changed; this tick is quiet.

2. **Drift + release check** — read-only:

   ```sh
   uv run --no-project python scripts/check_issue_hygiene.py --all
   ```

3. **Surface — emphasize DELTAS, not the whole state.** Report only:
   - if the projection changed: what is newly **ready** / newly **unblocked** / newly
     **running**;
   - **pending release** (merged-but-unreleased `reg_meta_build`);
   - **decisions owed** (open questions gating downstream work);
   - **drift** from the hygiene check (missing labels, stale `blocked`, done-but-open,
     half-wired sub-issues).

   If nothing changed and there is no drift, say so in one line and stop — no noise.

4. **Propose fixes, tiered by risk:**
   - **Auto (safe):** the projection refresh in step 1 is already applied.
   - **Propose, don't apply:** close a done-but-open issue (cite the merged PR), wire a
     missing sub-issue (`gh issue edit <child> --parent <epic>`), add a missing
     area/type label, draft a `Relationships`/`touches` block. List them; apply only
     what the user approves — or only the unambiguous, low-risk ones if they've said to
     act.

5. **Stop.** `/loop` re-invokes on its cadence. Do not sleep or poll inside the tick.

## Notes

- Self-paced `/loop` keys cadence to activity — wake often when PRs are in flight, sleep
  long when quiet. The projection's deterministic, timestamp-free output makes "did
  anything change?" the byte-diff in step 1's message.
- The write mandate is narrow: **auto-refresh the projection block only.** Everything
  structural (closing issues, labels, sub-issue wiring) stays propose-not-apply.
