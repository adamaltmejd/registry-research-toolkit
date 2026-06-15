---
name: issue-pulse
description: "One heartbeat of the issue-tracker — refresh the generated sequencing projection in the epic body, check for drift (stale labels, done-but-open, merged-but-unreleased build content), and surface what changed plus propose fixes. Built to run on a cadence via /loop. Usage: /loop [interval] /issue-pulse"
argument-hint: "[epic-number, default 328]"
---

# issue-pulse — one tick of the issue-tracking heartbeat

Run on a cadence with `/loop` (`/loop 30m /issue-pulse [epic-number]`, or bare
`/loop /issue-pulse` to self-pace the default epic). Each invocation does **one tick and
stops** — `/loop` handles re-invocation and sleeping.

This is the agentic layer over the deterministic scripts: it doesn't recompute anything,
it runs them and adds judgment (deltas + proposals). CI (`issue-hygiene.yml`) is the
reflex; this is the heartbeat and the push.

## The tick

`--epic <N>` defaults to `328`; pass the skill's argument through.

**Output budget — this runs every tick, so be terse.** A **quiet tick** (no status
delta; only benign new-file warnings) gets **one short line and nothing else**, e.g.
`✓ tick — no change · 0 errors · sleeping`. An **active tick** gets only the deltas +
real drift as a short bullet list — no preamble, no restating the whole projection, no
explaining the tooling. Don't narrate the steps; just run them and report the result.

1. **Refresh the projection + capture the delta** — `--write` computes the status delta
   against the epic's *current* block (i.e. the last tick) **before** splicing, so this
   one call both updates the body and tells you what changed:

   ```sh
   uv run --no-project python scripts/plan_sequence.py --epic <N> --write
   ```

   - `Updated #<N>` + a delta (`newly ready: #…`, `left blocked: #…`) → it **changed**.
   - `already up to date` → nothing changed; this tick is quiet.

   (The splice overwrites only the marked region — bounded and reversible.)

2. **Drift + release check** — read-only:

   ```sh
   uv run --no-project python scripts/check_issue_hygiene.py --all
   ```

3. **Surface the DELTAS, not the whole state.** Report only:
   - the status delta from step 1 (newly ready / newly unblocked / newly running, or
     what left a section);
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
