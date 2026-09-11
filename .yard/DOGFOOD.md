# Yard dogfood log

Open observations about how Yard behaves in practice on this repo: problems and
papercuts, filed upstream through the [report
form](https://github.com/adamaltmejd/switchyard/issues/new?template=report.yml) after
searching open and closed issues, with raw output redacted of tokens and private paths.
An entry is deleted once its upstream report is closed (fixed in the running Yard
version with the retest result on the issue, or declined), and an unfiled observation
once it is filed or judged not worth filing. The full text of everything deleted is in
git; the last unpruned revision is 4423fe7d (pruned 2026-09-12). Raw evidence stays
under the ignored `archive/reports/yard/`.

Running: yard 0.14.10 (b0c58aac) since 2026-09-11. Lane durations measured on 0.14.5
from the store: `ui` about 65 minutes median, `default` about 20, `light` 1–22.

## Open upstream reports

### 2026-09-11: Y-113 stopped by a network outage, then a host sleep spent its window

Driving the curation-consolidation chain (Y-113..Y-120, Claude Fable 5.1 operating).
Y-113/1's first worker generation died at 14:27Z on `provider-request-timeout` when the
laptop lost its network; the host then slept from 15:01Z to 21:41Z. The attempt's
360-minute total-work window is wall-clock by design (upstream #90's disposition) and
had expired at 20:10Z while the lane sat stopped.

- **Stale exit.** At 21:49Z `yard lane show Y-113/1` still printed A-295's `re-run` exit
  (`yard lane start Y-113/1/e1 --expect-generation 1`). Taking it was accepted
  ("re-running the implementation round; no new attempt") and cancelled within seconds
  as `total-work-timeout`, raising A-296 with the guarded nudge. The nudge (its help: "a
  fresh mandate renews the attempt's total-work window") started g3 on the retained
  session; `totalWorkStartedAt` moved to 21:50:41Z. Cost: one cancelled generation and
  one extra decision. Filed as
  [#99](https://github.com/adamaltmejd/switchyard/issues/99); searched #53, #89, #90
  first (adjacent, none covers an exit printed after the window expired). Raw wake
  lines, lane JSON and the submitted body are local-only under
  `archive/reports/yard/2026-09-11-y113-stale-rerun-exit/`.
- The 16m43s / $3.12 g1 spend had committed nothing (`head -`); whether the on-disk
  workspace state carried into g3 is only visible from the candidate.

## Observed, not yet filed

One line each, with the Yard version it was observed on. Retest on the running version,
then file it or delete it with a reason.

- (0.9.1) Gates are bound per workflow at filing time, never chosen from the candidate's
  changed paths; a docs-only candidate runs every gate its workflow names.
- (0.10.1) `build_artifacts` takes literal directories only; every package's
  `__pycache__` is declared by hand, and a new package re-hits the retarget exactness
  check.
- (0.11) No verb runs a gate against an arbitrary tree with the daemon's own image
  (`yard check run <gate> [--tree PATH]`); 0.14.10 preflight builds the image for the
  current tree only.
- (0.11) No "decision needed" proposal kind (question plus options); a worker's options
  memo arrives as `ticket.create` with a menu for a body.
- (0.11) The gate image build has its own 900 s limit
  (`docker build … [timed out after 900000ms]`) that no key in `config.toml` names.
- (0.11) A landed ticket left open (`blocksCompletion` on a `depends_on`) was
  re-admitted as a fresh attempt the moment its dependency landed (Y-25/2). Retest
  before filing.
- (0.11) Configured models are not validated against the vendored worker CLI at config
  load or preflight; a pin that refuses a model surfaces only as a worker failure.
- (0.13.3) A daemon restart to load config cancelled a running execution (Y-35/1/e1) and
  left a stopped attempt with no attention item and no printed exit. Not re-observed
  since; retest before filing.
- (0.14.5) A lane at `approval-needed` prints "admissions waiting …" and nothing is
  admitted into the free slot; undocumented whether deliberate. Confirm on 0.14.10.
- (0.14.5) Limit and transport stops want a daemon-taken "retry at <time>" rather than
  an operator exit read out of free text; #85 and #89 typed the exits, the wait is still
  manual, and the Y-113 outage above has the same shape.
- (0.14.5, 0.14.8) `yard lane replay` refuses with "lane capacity is full" instead of
  queueing (may already be inside #86's body).
- (0.14.8) Config edits (a new workflow, a retired key) need a lane: no
  `yard workflow add` or `yard config migrate`, and a new client refuses the old daemon,
  so the binary cannot be staged on PATH before the config lands.
- (0.14.8) No per-lane cost ceiling and no "cost so far" on the wake line; Y-92/2
  reached $17.87 for +233/−8 lines before anyone looked. Y-113/1 (0.14.10, opus xhigh)
  reached $51.95 across five generations, most of it a resumed session re-reading 37M
  cached input tokens; the approval item showed no cost either.
- (0.14.8) `yard proposal accept` has no `--title`; a convention retitle costs a
  `ticket edit --expect-revision` round-trip (check whether #91 covers it).
- (0.14.8) A gate repair whose worker reports "cannot repair: base-protected test" still
  queues the next repair round.
- (0.14.8) `yard proposal promote` takes one finding; an umbrella ticket for several
  same-surface advisories is one promote plus a body rewrite.
- (0.14.10) `daemon.log` has no timestamps and is never rotated: 315 stale
  `… is a conflict execution` lines and 4 `startup reconciliation failed` lines from
  earlier daemon lives. Observed on the running version; file it.
- (0.14.10) Yard has no notion of an open manual-integration or release window:
  `yard status` shows `relation equal`, and a second operator learns of the window only
  from this file.

## Host and repo-side notes

Not Yard defects. Each belongs in the skill or file that owns it; delete a line here
once it has moved.

- Gate image (`.yard/Dockerfile`): pre-warm uv/uvx/bun into the image,
  `UV_LINK_MODE=copy`, chmod in the warming layer, `env -i` floor, hatchling and
  editables as dev deps, two-step `uv sync --no-install-workspace` then
  `--no-build-isolation`. `reg_meta/pyproject.toml` is a COPY key of the warm layer;
  touching it rebuilds every later layer from the network. Playwright's Node needs
  `libatomic.so.1`.
- Gate commands: never pipe through `tail` (it masks exit codes); keep the PATH export
  (`python3` is absent from the container PATH and `catalog_fixture_db.py` needs it);
  container Chromium runs `no-sandbox`, the single-process fallback crashes on a second
  context. Never chain a spend command after `&&` on a pipeline.
- `docker system prune` strands every running attempt (executions pin exact image ids);
  prune only with the board empty. `~/.local/state/switchyard` grew to 15 GB before its
  first cleanup.
- Laptop clamshell sleep on battery spends wall-clock windows: the worker total-work
  window by design, review and gate deadlines until Y-660 (0.14.9). Keep the host awake
  during lanes.
- Upgrade recipe: `yard daemon stop`; back up `store.db` with wal/shm, the git mirror,
  `daemon.log`, the preflight report and the previous executable (not `.yard/local`);
  install; restart. Land any retired-config-key removal first, and restart the daemon
  between a config landing and the next approval.
- Capacity-contested recovery order: `yard pause`, park, abandon, `yard lane replay`.
- Claude sandbox: `yard sync` and daemon commands need `/bin/ps` process-inspection
  permission. Keep concurrency at 2 on this Mac.
- Review context: point the design seat at `reg_webapp/frontend/DESIGN.md`, never at
  `tokens.css` (autoreview refuses it as a sensitive filename).
- Workers have no Yard CLI: verify any Yard-documentation claim on the host. A repo
  skill the worker's `Skill` tool does not list is read by path.
- Host re-render of a candidate: worktree at base plus `git apply` of the lane diff,
  `reg-meta update` into a scratch dir (the XDG DB may be stale),
  `bunx playwright install chromium`, then `dev.sh shot --all <routes>`,
  `dev.sh flows <dir> <scenario>` or `--fixture-db`. The flow gates never render
  `/catalog/<p>/<r>` or `project-source-period`; a `period-flows` gate and a
  register-list scenario are project-side TODOs.
- Codex desktop app rewrites `~/.codex/config.toml` with a table older CLIs reject;
  `codex engine failed (1)` in review is fixed by `codex update`.
- Release skill: `UV_EXCLUDE_NEWER=<date>` for host `ty` pins younger than the global
  7-day `exclude-newer`; the SWECOV `flavor` plus `inventory` wave is a release
  prerequisite whenever a steward coordinate moves; reg_schema's version is the exact
  `schema_version`, never a routine patch; post-publish `integration` on the tag fails
  structurally when a steward coordinate moved, so rerun `integration.yml --ref main`
  and never re-release. Handoff step 3 is `git merge --ff-only origin/main`, then
  `yard sync --json` expecting `relation equal`; `yard pause`'s `changed: true` marks
  whose pause it is.
- Filing lessons: a fixture named under Proof is a behavior statement; a
  "byte-identical" clause names its paths; an accepted proposal body needs a Proof
  section before unpark; recheck old briefs before admitting a batch.
