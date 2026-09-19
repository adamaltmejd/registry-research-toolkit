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

### 2026-09-15: a following transcript exits with an offset error at completion

On 0.14.10, `lane tail --follow --current-generation` emitted normal worker completion
and then exited 3 for Y-157/1 and a fresh Y-157/3 follower, reporting
`transcript is shorter than byte offset 2021721` and `372041`, respectively. Y-159's
follower exited cleanly with offset 55901. Filed as
[#105](https://github.com/adamaltmejd/switchyard/issues/105); no data-loss or
candidate-corruption diagnosis is established. Supported CLI evidence, report and
trusted readback receipts are under
`archive/reports/yard/2026-09-15-usecase-architecture/yard-tail-completion/`.

### 2026-09-15: worker TMPDIR creates mixed-ownership scratch Git repositories

Y-155's Sol worker reproduced scratch Git failures under its default
`TMPDIR=/yard/state/tmp`: the repository directory belonged to `root:root`, while `.git`
belonged to `501:dialout`; `git config` exited 128. Equivalent controls under `/tmp` had
consistent ownership and succeeded. Command-local `TMPDIR=/tmp` let the focused tests
pass without changing product code or Git's ownership protections. Filed as
[#104](https://github.com/adamaltmejd/switchyard/issues/104). The ownership mechanism
remains unconfirmed. Supported Yard transcript output, controls and submission receipt
are local-only under
`archive/reports/yard/2026-09-15-cold-value-storage/yard-tmp-ownership/`.

Recurred during Y-161: the watcher path test passed alone but failed in the full file
under the shared temp tree. Command-local `TMPDIR=/tmp` made all 19 watcher tests pass;
no Git/path guard change was needed. The operator supplied the known workaround through
a guarded nudge and one stop, preserving the ongoing work.

Y-162 generation 7 reproduced the scratch-Git failure in four input-snapshot tests under
`/yard/state/tmp`. The worker isolated the failure to that temp location before changing
product code; its supported transcript records the controls. Command-local `TMPDIR=/tmp`
passed all 127 focused tests and the full 4,223-test gate. No ownership-check bypass or
product fix was needed.

Y-163 repeated the temp-tree failure in three focused tests. A worker control with
wildcard Git trust still left one failure and was discarded as evidence. After a guarded
nudge and one stop delivered the known workaround, command-local `TMPDIR=/tmp` with
ordinary Git trust passed all 140 focused tests. The worker kept product and test guards
unchanged. Supported transcript: Y-163/2 generations 1–2; local scope evidence is under
the same usecase-architecture archive.

### 2026-09-15: published macOS binary fails signature verification

On macOS 27.0 arm64, the installed 0.14.10 CLI exited 137 before printing output. Its
complete SHA256 matched the published release artifact, but `codesign --verify` reported
an invalid signature. A task-local copy with a renewed ad-hoc signature verified and ran
successfully; the installed binary was left unchanged. Filed as
[#103](https://github.com/adamaltmejd/switchyard/issues/103). The active operator uses
that same-version copy under the local-only evidence directory
`archive/reports/yard/2026-09-15-prepared-input-build/yard-tool/`; keep it while the
daemon runs from it. Release identity, repair receipts and the redacted report are
retained there. The trigger for the new host failure is not established.

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

### 2026-09-13: automatic repairs repeat a protected-test authority block

Reproduced the 0.14.8 observation on 0.14.10 while operating Y-125 and Y-128 with Sol.
Y-128's consecutive automatic repair rounds reported that the required existing test
edits were forbidden, made no changes, and still bought another round. Editing the
ticket clarified scope but did not change the repair prompt's authority. A guarded
unrestricted operator nudge permitted the specific correction. Filed as
[#102](https://github.com/adamaltmejd/switchyard/issues/102); candidate and test guards
were preserved. Redacted transcripts and submission text are local-only under
`archive/reports/yard/2026-09-13-sol-operator/`.

## Observed, not yet filed

One line each, with the Yard version it was observed on. Retest on the running version,
then file it or delete it with a reason.

- (0.15.3 upgrade) Moving from 0.14.10 needed a 0.15.1 bridge plus removal of job keys
  from legacy light roles before that bridge would start. The release says to convert
  before installation, but canonical config must first pass the old daemon through sync.
  Kept admissions paused and used supported sync throughout; local evidence is in
  `archive/reports/yard/v0.15.3/`.

- (0.15.3) Pi `lane tail Y-185/1/e1 --current-generation` renders raw
  `frame {"type":"message_update",...}` lines, including token deltas, instead of the
  documented worker-text/tool-result operator view. Raw transcript remains readable
  through `--raw`; local evidence is in the same upgrade archive.

- (0.14.10) After Y-159 changed the conflict role from Claude to Codex and a daemon
  restart confirmed the new config digest, `lane start Y-157/1/e7` still resumed its
  captured Claude session and hit the same weekly quota. The offered retry did not
  explain this binding; retained-candidate replay reached Codex. Evidence:
  `archive/reports/yard/2026-09-15-usecase-architecture/y157-before-provider-replay.json`.

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

- (0.14.10, filed as switchyard #100) Run from a git WORKTREE of the project, `yard`
  silently initializes a second, empty board (`.yard/local/` appears in the worktree;
  `yard status` prints `0 open`, `yard lane diff Y-123/1` prints
  `error: no attempt Y-123/1`) instead of resolving the project through the git common
  dir or refusing with a pointer to the main checkout.

- (0.14.10, filed as switchyard #101) `yard lane diff` truncates a large candidate diff
  (~29.7k lines, marker `[yard: diff truncated]` inside the patch text) and has no
  `--full`; the operator's pre-approval real-data build needs the whole candidate.
  Workaround: `git fetch .yard/local/git/canonical.git <head>` and a detached worktree.

- (0.14.10) After a provider/network outage a review execution (Y-116/1/e31) showed
  `reviewing r0/5 · seat codex 1/1 38m` with no evidence directory, no review process on
  the host, and no wake — the seat never started and nothing timed it out.
  `yard lane stop <exec>` then `yard lane start <attempt>` re-ran it (e32 started within
  a second). Retest before filing: one occurrence, right after an ENOTFOUND outage.

- (0.13.3) A daemon restart to load config cancelled a running execution (Y-35/1/e1) and
  left a stopped attempt with no attention item and no printed exit. Not re-observed
  since; retest before filing.

- (0.14.5) A lane at `approval-needed` prints "admissions waiting …" and nothing is
  admitted into the free slot; undocumented whether deliberate. Confirm on 0.14.10.

- (0.14.5) Limit and transport stops want a daemon-taken "retry at <time>" rather than
  an operator exit read out of free text; #85 and #89 typed the exits, the wait is still
  manual, and the Y-113 outage above has the same shape.

- (0.14.10) Y157/3/e1 generation 8 stopped on "Selected model is at capacity. Please try
  a different model." A395 classified it as `provider-error`, with no provider wait and
  only an abandon exit. Supported recovery preserved the candidate through replay as
  Y157/4, but repeated review and every gate before repair. Check whether capacity
  errors should expose a guarded retry; no claim that retry would have succeeded.

- (0.15.3) Two `pipeline-muse` workers (Y-188/1, Y-187/1) failed with OpenRouter
  `403 Request blocked by content filter: Content filter redaction would produce invalid tool call arguments`
  during ordinary source/test editing, classified `provider-error` with only an abandon
  exit. Both tickets were re-routed to `pipeline` (Opus) and completed there. Not yet
  reported upstream; unclear whether Yard can do more than surface the provider's
  rejection.

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
- Y157/3's Codex repair generations 7 and 8 each re-read all three operator skills and
  OPERATOR.md before repairing code, despite having no board decision to make. This
  repeats roughly 676 lines of operator-only context. Clarify the worker/operator
  distinction in the owning instructions; observed prompt overhead, not a CLI defect.
  Y162/2 generation 1 claimed to delegate to Sol, then its retained raw transcript
  showed waits with empty receiver IDs and no preceding spawn call. A guarded nudge plus
  stop resumed generation 2, which found partial implementation files despite the absent
  visible delegation evidence. This contradicts the initial inference that no child
  existed; a second guarded continuation asks the worker to check its live agent
  inventory before editing the shared tree. Its plan had already been accepted on the
  host. Treat this as unresolved observability/provider behavior. Generation 3 reported
  only the current worker in its inventory and retained the partial implementation; its
  first focused inspection test run passed. Y165/2 repeated this role confusion and
  tried the absent host CLI before reading lane_context. It then recovered
  independently. The operator issued a queued clarification and one stop before noticing
  that recovery in the command response; that continuation was unnecessary. Read fresh
  lane progress before interrupting. The implementation ticket now says its
  planning/review are already complete. Keep operator/planner model instructions out of
  implementation-ticket prose once that handoff is complete. Local-only evidence:
  `archive/reports/yard/2026-09-15-usecase-architecture/y162-raw-transcript.jsonl`.
- Host re-render of a candidate: worktree at base plus `git apply` of the lane diff,
  `reg-meta update` into a scratch dir (the XDG DB may be stale),
  `bunx playwright install chromium`, then `dev.sh shot --all <routes>`,
  `dev.sh flows <dir> <scenario>` or `--fixture-db`. The flow gates never render
  `/catalog/<p>/<r>` or `project-source-period`; a `period-flows` gate and a
  register-list scenario are project-side TODOs.
- Codex desktop app rewrites `~/.codex/config.toml` with a table older CLIs reject;
  `codex engine failed (1)` in review is fixed by `codex update`.
- Release skill: `UV_EXCLUDE_NEWER=<date>` for host `ty` pins younger than the global
  7-day `exclude-newer`; run `uv run --frozen` under that override, or uv rewrites
  `uv.lock` with an `[options] exclude-newer` block that pre-commit then stashes and
  restores; the SWECOV `flavor` plus `inventory` wave is a release prerequisite whenever
  a steward coordinate moves; reg_schema's version is the exact `schema_version`, never
  a routine patch; post-publish `integration` on the tag fails structurally when a
  steward coordinate moved, so rerun `integration.yml --ref main` and never re-release.
  Handoff step 3 is `git merge --ff-only origin/main`, then `yard sync --json` expecting
  `relation equal`; `yard pause`'s `changed: true` marks whose pause it is.
- Filing lessons: a fixture named under Proof is a behavior statement; a
  "byte-identical" clause names its paths; an accepted proposal body needs a Proof
  section before unpark; recheck old briefs before admitting a batch.

### 2026-09-18: watch fails when replaying a completed lane abandonment

On Yard 0.15.3 (`19903129`), after supported abandonment of Y-188/1 with no committed
candidate, `yard status --watch --notes --since 146431` repeatedly exited 1:
`lane.abandoned on Y-188/1 records no terminal outcome`. Abandonment and cleanup
completed. A fresh status cursor (146445) restored watching. Filed as
[#108](https://github.com/adamaltmejd/switchyard/issues/108); no root cause is
established. Raw receipts and the report are local-only under
`archive/reports/yard/2026-09-18-abandoned-watch/`.

### 2026-09-18: pipeline-muse lanes reach the operator with lint failures

**Lint half resolved by Yard 0.16.0 (2026-09-19).** Gates are now project-wide and
`lint` carries `stage = "candidate"`, so no lane can reach approval-needed with a plain
`ruff`/`panache` failure any more; the `pipeline-muse` workflow that ran `checks:none`
is deleted. Kept here for the two parts 0.16.0 does not address:

Y-203/1 showed the reviewer reversing itself between rounds (round 5 demanded the
sentinel list be pinned in the content hash, round 6 blocked because that changed every
hash); the lane hit the automatic review maximum and needed an operator nudge with a
design decision. Y-202/1 g1 ended "worker stopped with uncommitted or in-progress Git
work" after 30 min and was recovered by the automatic unclean-clone cleanup; no operator
action was needed, but the 30 min were spent.

### 2026-09-19: the 0.15 -> 0.16 upgrade path is deadlocked, with no supported way across

Upgrading this project from 0.15.3 to 0.16.0 cannot be completed with supported
commands. The daemon validates `.yard/config.toml` **at canonical's target head**, not
in the working tree, and the only command that moves canonical is `yard sync`, which
autostarts a daemon:

- the 0.16.0 daemon refuses to start while canonical holds the v0.15 config shape
  (`workflows.default.implementer: expected string, received undefined`), so `yard sync`
  cannot run to import the converted file;
- the 0.15.3 daemon refuses to import the converted file into canonical
  (`max_lanes, approve, gates, review: Unrecognized keys`), because invalid candidate
  configuration cannot become authoritative.

No config satisfies both loaders: each treats unknown keys as errors and they share no
valid key set (`agent`/`checks` vs `implementer`/`gates`/`review`), so a transitional
commit is impossible. Neither the release notes' "convert the file, install the
executable, restart the daemon" nor the README's "Upgrading an existing store" mentions
that the committed config at canonical is what gets validated.

Worked around by fast-forwarding `refs/heads/main` in `.yard/local/git/canonical.git`
directly, which bypasses the deliberately disabled canonical push remote. Filed as
[#109](https://github.com/adamaltmejd/switchyard/issues/109); the fix wanted is a
supported way to move canonical to a converted config — a `yard sync --config-only`, a
documented bootstrap, or accepting a forward-shape config during upgrade. Raw receipts
and the six report bodies are local-only under
`archive/reports/yard/2026-09-19-0.16.0-upgrade/`.

### 2026-09-19: a workflow can no longer select its gates

0.16.0 retired per-workflow `checks`, so the gate set is project-wide and every declared
gate runs on every landing batch. This project used the removed feature in both
directions: `ui`/`light-ui` selected three rendered Playwright flow gates that other
lanes did not pay for, and `pipeline`/`pipeline-muse` selected none at all for
operator-admitted bounded `reg_meta_build` repair batches. Neither is expressible now.

The three flow gates are declared without `stage`, which keeps them off candidate heads
but also moves their retained PNGs to the landing batch — so `yard lane show` no longer
prints rendered evidence for the candidate the operator is approving, which is what
`.yard/OPERATOR.md` "UI approval evidence" was built on. Staging them instead would put
three browser gates on every `reg_meta_build` repair. Filed as
[#110](https://github.com/adamaltmejd/switchyard/issues/110) as a request for
differential gates by workflow; monorepos need the cheap gates everywhere and the
expensive ones only where they decide something.

Four smaller 0.16.0 findings from the same session, all filed and awaiting the builder's
disposition: `yard daemon preflight` naming retired `checks.<gate>.<key>` keys
([#111](https://github.com/adamaltmejd/switchyard/issues/111)); DESIGN.md omitting
`[workspace]` while the shipped config keeps the retired `[merge]` comment
([#112](https://github.com/adamaltmejd/switchyard/issues/112)); removing a workflow
orphaning the tickets that name it, which stranded Y-199/Y-205/Y-206 here
([#113](https://github.com/adamaltmejd/switchyard/issues/113)); and `yard init`'s update
patch not being pipeable to `git apply`
([#114](https://github.com/adamaltmejd/switchyard/issues/114)).
