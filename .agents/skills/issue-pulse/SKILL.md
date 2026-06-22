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

## Tick

1. Run the read-only freshness check:

   ```sh
   git fetch --tags origin
   basis="$(uv run --no-project python scripts/plan_sequence.py --tick --epic <N>)"
   tick_status=$?
   ```

   Capture stdout as the lanes basis and stderr as the human-facing delta. Exit code
   meanings:

   - `0`: lanes fresh; skip lane write.
   - `1`: lane content moved; re-rank.
   - `2`: only running set moved; restamp existing lanes.

   If stdout is empty on a non-zero exit, treat it as a transient fetch failure. Do not
   re-rank or restamp with an empty basis.

2. Run hygiene:

   ```sh
   git fetch --tags origin
   uv run --no-project python scripts/check_issue_hygiene.py --all
   ```

   Hygiene is report-only and may overwrite `$?`; branch on the saved `tick_status` from
   step 1, not the latest command's exit status.

3. If `tick_status` was `2`, restamp:

   ```sh
   uv run --no-project python scripts/plan_sequence.py --restamp-lanes --epic <N> --basis "$basis"
   ```

   If restamp exits `1` because no existing lanes content exists, fall through to
   re-rank.

4. If `tick_status` was `1`, use the `plan-lanes` procedure to produce ranked markdown,
   then persist it:

   ```sh
   printf '%s' "<ranked lanes markdown>" |
     uv run --no-project python scripts/plan_sequence.py --write-lanes --epic <N> --basis "$basis"
   ```

   `--write-lanes` refuses (non-zero exit, no write) a body that drops a candidate from
   the floor `plan-lanes` was handed (the basis `free=` set) — one absent from the
   accounting surfaces (a ranked lane, the Held line, or the Notes line; a lane's
   `- why:` rationale doesn't count). That means the ranking was incomplete: re-run
   `plan-lanes` accounting for every candidate and persist the corrected markdown; do
   not retry the same body.

## Output

Keep pulse output terse.

- Quiet tick: one short line, e.g.
  `tick: no change; lanes fresh; no material hygiene drift`.
- Restamp tick: one short line naming the running-set change and restamp.
- Active tick: status delta, top 1-2 re-ranked lanes, pending release, decisions owed,
  and material hygiene drift.

Do not resurface recurring `touches ... matches no files` warnings unless they look like
typos or are newly introduced.

## Mutation Policy

Auto-write only the lanes block through `--write-lanes` or `--restamp-lanes`. Do not
close issues, edit labels, wire sub-issues, or patch Relationships without user approval
unless the user has already told you to apply those exact changes.
