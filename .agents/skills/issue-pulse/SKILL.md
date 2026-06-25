---
name: issue-pulse
description: >-
  Registry Research Toolkit issue heartbeat. Use when asked to run the issue-pulse
  workflow, perform one issue-tracker tick, check plan-sequence drift, refresh or
  restamp plan-lanes, inspect issue hygiene, or report changes for epic #328.
---

# Registry Issue Pulse

## Scope

Run one heartbeat tick, report deltas, and stop. Do not sleep or poll; a loop runner
handles cadence if the user wants repeated ticks.

Default epic is `328`.

The projection block is refreshed by CI on issue/PR events plus cron. This heartbeat
owns the agentic layer CI cannot do: re-rank or restamp the generated lanes block when
the lane basis moves, surface material drift, and propose structural fixes.

## Tick

1. Run the read-only, one-fetch freshness check:

   ```sh
   git fetch --tags origin
   basis="$(uv run --no-project python scripts/plan_sequence.py --tick --epic <N>)"
   tick_status=$?
   ```

   Capture stdout as the lanes basis and stderr as the human-facing delta. Save
   `tick_status` immediately; later checks may overwrite `$?`.

   - stdout (`$basis`): the exact freshness basis to pass to `--write-lanes` or
     `--restamp-lanes`.
   - stderr: the projection delta plus the lanes verdict.

   Exit code meanings:

   - `0`: lanes fresh; skip lane write.
   - `1`: lane content moved; re-rank.
   - `2`: only running set moved; restamp existing lanes.

   If stdout is empty on a non-zero exit, treat it as a transient fetch failure. Do not
   re-rank or restamp with an empty basis.

   The projection delta is human-facing context, not the re-rank trigger. CI usually
   absorbs projection moves before the heartbeat runs; branch on `tick_status`.

2. Run hygiene:

   ```sh
   git fetch --tags origin
   uv run --no-project python scripts/check_issue_hygiene.py --all
   ```

   Hygiene is report-only and may overwrite `$?`; branch on the saved `tick_status` from
   step 1, not the latest command's exit status.

3. If `tick_status` was `2`, restamp the existing lanes block:

   ```sh
   uv run --no-project python scripts/plan_sequence.py --restamp-lanes --epic <N> --basis "$basis"
   ```

   A running-set-only delta means an in-flight claim appeared or cleared, but ready lane
   content and its ranking inputs did not move. Do not invoke the `plan-lanes`
   procedure. Refresh the basis stamp and keep the existing ranked lanes. The
   human-visible in-flight line in the block may lag until the next re-rank; that is
   cosmetic.

   If restamp exits `1` (no existing lanes content, or the preserved content is
   incomplete vs the live floor), fall through to re-rank.

4. If `tick_status` was `1`, or step 3's restamp exited `1`, use the `plan-lanes`
   procedure to produce ranked markdown, then persist it:

   ```sh
   printf '%s' "<ranked lanes markdown>" |
     uv run --no-project python scripts/plan_sequence.py --write-lanes --epic <N> --basis "$basis"
   ```

   `--write-lanes` refuses (non-zero exit, no write) a body that silently drops a free
   candidate — one from the floor `plan-lanes` was handed (the basis `free=` set) that
   doesn't appear anywhere in the body. It's a deliberately coarse silent-vanish
   backstop, not a placement checker (exact placement is `plan-lanes`' own self-check).
   On a refusal, the ranking was incomplete: re-run `plan-lanes` accounting for every
   candidate and persist the corrected markdown. Do not retry the same body.

   Stale-basis caveat: the refusal is judged against `$basis`'s `free=` set captured in
   step 1. If a candidate closed/became held before `plan-lanes` read the live floor,
   the correct new ranking omits it but the stale basis still demands it, and a re-rank
   can't converge. So if a refusal names a candidate no longer on the live `--lane`
   floor (or it persists across a re-rank), restart the tick (re-capture `$basis` from a
   fresh `--tick`) instead of re-ranking against the old basis.

   The basis stamps the lane-affecting projection: free candidates plus each non-running
   issue's status, area, `touches`, `priority`, and full Relationships graph. Changes
   there re-rank even when no issue moves between sections. A running issue merely
   appearing or disappearing only re-stamps.

## Output

Keep pulse output terse.

- Quiet tick: one short line and nothing else, e.g.
  `tick: no change; lanes fresh; no material hygiene drift`.
- Restamp tick: one short line naming the running-set change and restamp.
- Active tick: status delta, top 1-2 re-ranked lanes, pending release, decisions owed,
  and material hygiene drift. Omit top lanes on quiet/restamp ticks because the ranking
  did not change.

Do not resurface recurring `touches ... matches no files` warnings unless they look like
typos or are newly introduced.

## Mutation Policy

Auto-write only the lanes block through `--write-lanes` or `--restamp-lanes`; the
projection block is CI/cron-owned. Everything structural is propose-not-apply: closing a
done issue, editing labels, wiring sub-issues, or patching Relationships/touches blocks
requires user approval unless the user already asked you to apply that exact change.

## Stop

Stop after one tick. Do not sleep, poll, or keep watching from inside this workflow.
