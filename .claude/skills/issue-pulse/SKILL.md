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
`✓ tick — no change · 0 errors · sleeping`. A **re-stamp tick** (a PR merged/opened, no
content move) is nearly as quiet — one line, e.g.
`✓ tick — PR #N merged → re-stamped lanes (no re-rank) · sleeping`. An **active tick**
(content moved) gets the deltas, the **top 1–2 re-ranked lanes**, and real drift as a
short bullet list — no preamble, no restating the whole projection, no explaining the
tooling. Don't narrate the steps; just run them and report the result.

1. **One-fetch tick — projection delta + lanes staleness.** `--tick` builds the corpus
   **once** and emits both signals: the projection status delta (stderr) and the live
   lanes **basis** (stdout), exit-coding the lanes freshness (fresh / re-rank /
   re-stamp). It is **read-only** — CI (`plan-sequence.yml`) on every event + the daily
   cron own the projection *write* now, so the loop no longer writes it. Capture the
   basis (you re-stamp it in step 3):

   ```sh
   basis="$(uv run --no-project python scripts/plan_sequence.py --tick --epic <N>)"
   ```

   - **stdout** (`$basis`) — the freshness basis to re-pass to `--write-lanes` (re-rank)
     or `--restamp-lanes` (re-stamp) in step 3.
   - **stderr** — the human-facing report: the projection delta (`newly ready: #…`,
     `left blocked: #…`, or `no status changes`) **and** the lanes verdict
     (`lanes: fresh` / `lanes: stale (re-rank)` / `lanes: stale (re-stamp …)`).
   - **exit `0`** → lanes fresh, skip step 3. **exit `1`** → lane *content* moved,
     **re-rank** in step 3. **exit `2`** → only the in-flight (running) set moved (a PR
     merged or opened); lane content is unchanged, so **re-stamp** in step 3 — no
     `/plan-lanes`.
   - A non-zero exit with **empty stdout** is a transient fetch failure (gh rate-limit /
     network), not a verdict — `$basis` is empty. Skip step 3 this tick (do **not**
     re-rank against an empty basis); the next tick retries.

   The projection delta is for the human surface in step 4 — **not** the re-rank trigger
   (CI absorbs projection moves); the exit code is. (Need only one signal standalone?
   `--diff` prints just the delta; `--lanes-stale` just the basis + the 0/1/2
   freshness.)

2. **Drift + release check** — read-only:

   ```sh
   uv run --no-project python scripts/check_issue_hygiene.py --all
   ```

3. **Lanes moved? Re-rank or re-stamp.** Skip if step 1 exited `0`. `$basis` holds the
   live state **as of step 1's check** — re-pass it verbatim either way (so the stamp
   matches what was checked, not a post-write recompute that could mark a stale ranking
   fresh).

   - **exit `2` (re-stamp only).** A running-set-only delta: a PR merged or opened, so
     an issue's in-flight claim cleared or appeared, but the ready candidates and their
     ranking inputs are unchanged. A `running` issue is never a lane member
     (`/plan-lanes` ranks only the ready set), so the existing ranking still holds —
     **do NOT invoke `/plan-lanes`.** Just refresh the basis stamp on the existing block
     (the human-visible in-flight line inside it may lag one tick — cosmetic, corrected
     on the next re-rank):

     ```sh
     uv run --no-project python scripts/plan_sequence.py --restamp-lanes --epic <N> --basis "$basis"
     ```

     If `--restamp-lanes` itself exits **1** (`no existing lanes content to re-stamp` —
     a stamped-but-empty block, so there's no ranking to keep), fall through to the
     re-rank path below instead.

   - **exit `1` (re-rank).** Lane content moved — the ready set, or a lane-affecting
     input of some work issue (its area, `touches`, `priority`, or `Relationships`).
     Invoke `/plan-lanes` via the `Skill` tool (it runs **forked**, so the
     corpus-reading stays out of this heartbeat's context, and returns the ranked lanes
     as markdown), then **persist** that markdown into the epic's `<!-- plan-lanes -->`
     block — `/plan-lanes` is read-only, so you are the writer:

     ```sh
     printf '%s' "<the /plan-lanes markdown>" |
       uv run --no-project python scripts/plan_sequence.py --write-lanes --epic <N> --basis "$basis"
     ```

     `--write-lanes` **refuses** (exit non-zero, no write) a body that drops a ready
     candidate — one absent from every ranked lane (checked when nothing is in flight,
     where every ready issue is a candidate). That means the `/plan-lanes` ranking was
     incomplete: re-run `/plan-lanes` (instructing it to account for every ready
     candidate) and persist the corrected markdown — don't retry the same body. (Empty
     stdout from step 1 is the different, transient case.)

   Gating on the three-way signal is the point: pay for the non-deterministic
   `/plan-lanes` re-rank only when lane content actually moved, not on every PR merge.
   The basis stamps a content signature over the lane-affecting projection (the free
   candidate set + each non-running issue's status, area, `touches`, `priority`,
   `Relationships`), so an edit to any of those that moves no issue between sections
   re-ranks too — while a running issue merely arriving or leaving re-stamps.

4. **Surface the DELTAS, not the whole state.** Report only:
   - the status delta from step 1 (newly ready / newly unblocked / newly running, or
     what left a section);
   - the **top 1–2 lanes** from step 3 if it **re-ranked** (label + members + one-line
     why, and which are concurrently runnable) — omit on a quiet or re-stamp tick (a
     re-stamp kept the existing ranking, so there's nothing new to surface);
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
   - **Auto (safe):** the lanes re-rank or re-stamp in step 3, if it ran, is already
     persisted. (The projection is no longer written here — CI + cron own it.)
   - **Propose, don't apply:** close a done-but-open issue (cite the merged PR), wire a
     missing sub-issue (`gh issue edit <child> --parent <epic>`), add a missing
     area/type label, draft a `Relationships`/`touches` block. List them; apply only
     what the user approves — or only the unambiguous, low-risk ones if they've said to
     act.

6. **Stop.** `/loop` re-invokes on its cadence. Do not sleep or poll inside the tick.

## Notes

- Self-paced `/loop` keys cadence to activity — wake often when PRs are in flight, sleep
  long when quiet. CI + cron keep the projection fresh, so the heartbeat's "is there
  anything to do?" is the **`--tick` freshness exit** in step 1, not the projection
  delta (which CI usually already absorbed).
- The write mandate is narrow: **auto-refresh the lanes block only**, and only when it
  moves (step 3) — a full re-rank when lane content changed, a cheap re-stamp when only
  an in-flight claim came or went. The projection block is no longer written here — CI
  (`plan-sequence.yml`) + the daily cron own it. Everything structural (closing issues,
  labels, sub-issue wiring) stays propose-not-apply.
