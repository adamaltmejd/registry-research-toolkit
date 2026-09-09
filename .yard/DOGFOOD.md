# Yard dogfooding log

Running observations from operating Yard on this repository. Newest entries at the
bottom of each section. Serious problems and papercuts alike; periodically summarized
into insights for the Yard builder agent, then pruned.

## 2026-08-29 — install, update 0.8.4 → 0.9.1, `yard init`, first config

### What worked well

- Install block in the README is copy-pasteable and the three-way verification
  (SHA256SUMS + release contract digest + `--version` reporting the contract's
  version/commit) all agreed on the first try.
- `yard daemon preflight` output is excellent: each line names what was found or exactly
  what has to become true. Docker-not-running was diagnosed with the precise socket path
  and requirement text. After `open -a Docker`, re-run went green with no stale state.
- `yard init` on a repo with an existing `.dockerignore` and `AGENTS.md` behaved
  respectfully: appended a marked block to the former, left the latter untouched and
  said so, with the exact remaining manual step named.

### Papercuts

- `yard daemon status` outside any project exits 1 with "no Yard project at or above … —
  run `yard init` first". Right after installing the binary, the natural first question
  is "is a daemon running anywhere / does the binary work"; being told to `yard init` in
  a scratch directory is misleading advice. A project-less `daemon status` could report
  "no project here; daemons run per-project" instead.
- `yard init` started a daemon immediately (reported pid in init output), before the
  config has been edited, committed, or synced. Surprising given the README's careful
  cost-boundary framing — nothing can run yet (no tickets), but an operator who reads
  "daemon pid 88726" mid-scaffold wonders what it is already doing and whether an
  un-edited scaffold config is now loaded (it is — and a later config commit then needs
  a restart, a step that would not exist if the daemon started on first use).
- The scaffolded `.yard/config.toml` is \~300 lines, most of it commentary. Great as
  reference, slow as a first read; a `--minimal` scaffold (or the commentary moved to
  `yard help config`) would make the edit-the-two-files step in Quickstart faster.
- Network-disabled container gates are the right default, but the scaffold gives no
  pattern for the (very common) lockfile-based toolchain: everything must be pre-warmed
  into the image at build time and every env var restated inline in gate commands
  (`env -i` floor). Getting uv + uvx tools + bun node_modules to resolve offline took
  most of the setup effort (cache paths, `UV_LINK_MODE=copy` because root-baked caches
  vs host-uid gate user, chmod in the warming layer). A worked example in the scaffold
  comments — "here is what an offline npm/uv/cargo gate looks like" — would have saved
  most of it.
- README `yard-operator` skill lives in `.claude/skills/` only; agents driven from
  AGENTS.md need the pointer added by hand (init says so — good), but in a repo where
  AGENTS.md is generated/mirrored from another file the "add the line yourself"
  instruction lands on a file the operator must not hand-edit. Low stakes, but the
  scaffold could ask or detect.
- The README top-level flow says "run the block above" for manual preflight, but the
  block mixes host checks with per-role checks (`operator.env` only matters for a Claude
  role, `codex login` only for a codex reviewer/worker) without saying which failures
  are ignorable for a given config. `yard daemon preflight` itself does this correctly
  (profile-driven), so the manual block is the weaker duplicate of it.

### `preflight --full` round (first project config)

- Good: `--full` caught a corrupted base-image digest (operator paste error) before any
  lane spent a token, quoting the exact Dockerfile line and Docker's own error. This is
  exactly the pay-nothing-first promise working.
- Papercut: gate-resolution probes the *first word* of each `command`, and a head of
  `export` or `cd` — normal openings for the multi-step gate any real CI mirror is —
  reports as undecided, with requirement text (`PATH=/its/bin:$PATH <command>`) whose
  worked example only covers a single-command gate with leading `VAR=` assignments. The
  workaround (wrap the whole gate in `sh -ec '…'`) is easy but undocumented, and it
  downgrades the probe's value to "sh exists". Either resolve past leading
  builtins/assignments, or document the `sh -c` wrapper as the intended shape for
  multi-step gates.
- Papercut: with the image build failing, the identical multi-line Docker error is
  repeated verbatim under every requirement that needed the image (worker-cli plus one
  per gate) — 4 copies in one report. One failure line + "same cause" references would
  read faster, especially for an agent operator paying tokens for the repetition.

### Config-completion round

- Product gap (from a real operator question): gates are selected per *workflow*, at
  filing time — a prediction about what the lane will touch. CI systems condition on the
  observed diff (paths-filter); a "non-UI" workflow that skips the frontend gate would
  silently skip the openapi-types drift tripwire whenever the prediction is wrong.
  Change-aware gates (skip/require by candidate paths, decided at gate time) would
  dissolve the whole UI-lane/core-lane workflow-taxonomy question.
- Asymmetry: the frozen planning prompt carries real anti-overengineering pressure
  (named consumer, minimum behavior, exclusions, delete-before-split), but the
  implementation role ships with no scope ceiling at all — `instructions` is empty in
  the scaffold and its one example is about code conventions. A default worker-side
  ceiling (the §7 rules of the operator skill, worker-facing) would protect projects
  that never hand-write role instructions.

### First lane (Y-1/1): gate-failure round trip

- What worked: the `gate-failed` attention carried ready-to-copy exits with guards
  (`yard lane start Y-1/1/e8 --expect-generation 1`); the worker's investigation was
  excellent — reproduced the failure on an untouched tree, proved it pre-existing,
  probed a fix, and filed a proposal instead of scope-creeping (the new role
  instructions visibly held); `yard lane show` exposes per-execution token usage and
  cost, which made the spend legible ($1.67 for a README lane, most of it spent
  diagnosing operator infrastructure).
- Product gap: the worker burned both `max_gate_repair_rounds` on a failure it could
  never fix — the gate image is built from `.yard/Dockerfile` in canonical, which the
  candidate cannot change. A gate that fails *identically on the base tree* is an
  environment failure, not a candidate failure; detecting that (run the failed gate once
  against base, or even just observe an unchanged-handoff round) and routing to the
  operator instead of the repair loop would save the rounds and the tokens.
- Product gap, larger: `preflight --full` advertises "can each gate's command start?"
  but resolves only the command's head word (`sh` after the wrapper workaround), so a
  whole class of image defects passed preflight and was discovered by paid lanes:
  missing `node` (CI green only because GitHub runners ship it), Debian node 20 vs
  undici incompatibility in vitest teardown, and uv's offline resolver refusing
  `build-system.requires` lookups entirely (editable workspace members cannot build in
  an isolated offline gate at all — fixed by locking hatchling+editables as dev deps and
  a two-step `uv sync --no-install-workspace` / `--no-build-isolation`). A `--full`
  variant that actually runs each gate command against the current tree would have
  caught all of it for container-minutes instead of worker-tokens.
- Operator note (own mistake, but instructive): verifying gate commands by piping
  through `tail` masks exit codes in sh (no pipefail); two rounds were lost to "SYNC_OK"
  lines printed after failed syncs.
- Learned the hard way (worth a README sentence in Yard): the *gate image* is built from
  the **candidate's own tree**, while config comes from canonical — so an infrastructure
  fix landed on canonical never reaches an in-flight candidate whose base predates it.
  The recovery is abandon + fresh attempt (full worker re-spend, here on an
  already-reviewed one-paragraph README diff). Also: an operator-triggered gate re-run
  (`yard lane start LANE/eN`) re-enters the automatic repair loop on failure — two more
  worker repair generations were spent on the same unfixable environment failure before
  the attention came back. An operator re-run might reasonably come straight back to the
  operator instead.
- Genuinely good strictness: `test_report = "junit"` treats any skip as missing
  evidence, and it immediately surfaced two fixture-conditional tests in
  `reg_meta/tests/test_doc_commands.py` that skip *unconditionally* everywhere (CI
  included) — dead coverage nobody had noticed. Gate temporarily downgraded to exit-code
  evidence (CI parity); ticket to fix the two tests and re-enable junit is the
  follow-up. Papercut in the same event: the attention's junit detail listed the skipped
  tests by name — excellent — but the "missing evidence" phrasing for "5 skipped"
  initially reads as "report unparsable", not "skips are disallowed".
- The candidate-pinned contract cuts deep: THREE attempts were spent discovering that
  neither the Dockerfile nor gate commands can be repaired under an in-flight attempt.
  Each iteration of gate config costs a full abandon + fresh worker implementation of an
  unchanged one-paragraph diff. An operator verb like "replay this candidate onto
  current canonical/contract" (rebase the candidate, keep the work, rerun verification)
  would have saved two of the three.

### End of first loop (Y-1 landed)

- Full loop proven: file parked, read, unpark, worker, review, gates, approval-needed,
  diff read, approve at exact head, landed, `yard sync`. The approve-to-landed-to-synced
  tail took seconds and the exits/guards were exactly right at every decision.
- Total cost of the one-paragraph README section: $3.20 across four attempts ($1.67 +
  $0.50 + $0.49 + $0.54). Attempt 1 was mostly the worker diagnosing the operator image;
  attempts 2 and 3 were burned solely because the contract is candidate-pinned and every
  gate-config fix needs a fresh attempt re-implementing an unchanged diff. Steady-state
  cost for such a ticket is the \~$0.50 of attempt 4; the 6x multiple is the price of
  setting up gates by live fire. All of it would have been avoided by a preflight mode
  that actually runs gate commands (logged above).

### Second lane (Y-2): config-transition candidate, single attempt

- Everything worked: one attempt, review pass, all gates green, landed. The worker
  rewrote both fixture-conditional tests deterministically and with *stronger*
  assertions than asked (asserting the truncation/continuation occurs), and restored the
  junit gate exactly to spec (quote-safe, deselection, aggregation).
- The self-proving config edit worked as hoped: the candidate's own test gate ran under
  its re-enabled junit contract (contract digest transition surfaced in the approval
  detail; report showed tests=4212 skipped=0). Correction to an earlier entry: a
  candidate CAN change gate config/Dockerfile and its own verification uses it —
  candidate-pinning cuts against *operator* fixes reaching in-flight attempts, not
  against a lane testing its own infra change.
- Noticed: the approval detail flagged `.yard/config.toml` under `protectedPaths`
  although `[merge].protected_paths` is empty — presumably a built-in always-protected
  set for auto-merge. Reasonable, but undocumented in the config comments.
- A landing that changes .yard/config.toml owes a daemon restart; `yard sync` printed
  the restart hint — good — though it is easy to miss below the fast-forward line.

## 2026-08-29 — update to 0.9.2

- Update flawless: verified install (SHA256SUMS + contract + --version agree), daemon
  restart, v0.9.1 store opened unchanged (schema 40 as the notes promised), full
  preflight green. The release notes' compatibility seam (`proposal show` vs an
  un-restarted daemon) was accurate.
- Resolved and verified: `yard proposal show` prints the full record — body, stamp,
  decision, reason — including for already-decided A-1. Fixes the truncated-board
  papercut from this morning.
- Retested unchanged: preflight gate probe (head word only), project-less
  `yard daemon status` error. High-value items (preflight gate-runs, candidate replay,
  environment-failure detection, junit root totals) not in this patch.

## 2026-08-29 — update to 0.10.1 and trial (Y-3)

- 0.10.1 answers the first report nearly point-for-point (Y-478..Y-484). Verified live:
  `preflight --gates` ran all three real gates to a passing verdict on the container
  floor for zero token spend (the exact mode the report asked for); project-less
  `yard daemon status` now explains instead of ordering `yard init`; Y-481's junit
  reader accepted stock pytest output in a real lane's test gate. Not yet exercised
  (need a natural occurrence): `yard lane replay`, `gate-fails-on-base`, the sh -n
  config lint, the new scaffold uv example.
- Y-3 (delete the now-obsolete junit aggregation workaround) trialed the `light`
  workflow: filed parked 17:50, landed 17:52 on first attempt, $0.36 — Sonnet, no review
  panel, all gates. Pure-deletion diff exactly to spec. The approval detail for a
  review-none lane correctly carries no review object; the operator read is visibly the
  only review, which is the right shape for this class.
- Board note: with the workaround gone, the gate config is back to what a fresh 0.10.1
  scaffold would suggest — the workaround lived exactly one release cycle, which is the
  dogfooding loop working as intended.

## 2026-08-29 evening — machinery retirement (Y-4..Y-9) on 0.10.1

Six lanes, \~2.5 hours wall-clock, $7.74 total: retired the pre-Yard coordination
machinery (9 scripts + 9 test files + 2 workflows + 7 skills + 4 role agents + both
mirror trees), trimmed CLAUDE.md/AGENTS.md by \~680 lines, then two worker-proposed
follow-up trims. Observations:

- **The proposal loop is the standout.** Three of six lanes came from worker proposals
  (A-11, A-14, A-15) — each flagged work outside its ticket fence instead of
  scope-creeping, each read whole via the new `proposal show`, each decided by the
  admission rule and accepted `--parked`, each landed clean. Workers also self-reported
  the judgment calls they were NOT making (trust-surface API, protected sections) — the
  frozen prompts plus role instructions are calibrating scope discipline extremely well.
- **Replay/retarget earned its keep**: Y-4's candidate was carried onto a moved main
  with review + gates redone and zero worker re-spend; two more lanes auto-retargeted
  silently. The one stop: retarget's exactness check tripped on `__pycache__` in the
  lane clone. The error's remedy text was perfect, but `build_artifacts` takes literal
  directories only, so Python's pycache spray must be enumerated dir by dir — a pattern
  (or built-in pycache handling) would remove a growing list.
- **Y-481's skip message verified live** ("1 skipped test is not gate evidence — fix or
  remove it: <name>") — and it caught a real flake: the same candidate's test gate
  skipped `test_pinned_providers_auto_toml_git_tracked[fqid_slugs]` ("slug_dir is not
  inside a git work tree") on one run and ran it green on the re-run. The gate view's
  git-work-tree availability appears non-deterministic across runs of the SAME candidate
  — worth a look at how the gate binds the candidate view.
- **`correctnessMismatch` did its job**: a reviewer verdicted pass while narrating
  "patch is incorrect" (claimed a deleted helper was still referenced); yard surfaced
  the tension as a flag, the operator read settled it (the reviewer was wrong — the
  helper had been inlined). Exactly the right division of labor.
- Papercut: a gate-failed attention after an operator-requested re-run still says reason
  "automatic-repair-rounds-exhausted" wording in some paths — minor, the exits were
  right.
- Costs: Y-4 $1.83 · Y-5 $0.83 (light) · Y-6 $1.15 · Y-7 $1.67 · Y-8 $0.66 (light) · Y-9
  $1.60. Concurrency 2 worked; the serialization point is the operator read, not the
  machinery.

## 2026-08-31 — v0.10.1 → v0.10.3 (skipped 0.10.2), codex fast tier on

- Update ritual is now routine and fast: release download + SHA256 verify + `install` +
  `yard daemon restart`. Store opened unchanged (schema 40), zero migration friction
  across two versions at once.
- **Y-488 (0.10.2) is our design-review thread answered point-for-point** — per-reviewer
  `instructions` on a profile entry, in the per-reviewer (not per-profile) form we asked
  for, with the 4 KiB cap settled at config load rather than lane time. Not yet
  exercised; will wire a `ui` profile + workflow when the first UI ticket lands.
- Cross-repo coupling seam handled well: the release notes named the exact agent-skills
  commit (fe73c38) the new report contract pairs with, and preflight's autoreview probe
  now demands `--codex-speed` exactly because our profile names it — the usage line
  visibly grew after the config landed. Requirement-follows-config is the right shape.
- **`yard sync` flagged the stale daemon config itself** ("configuration differs from
  canonical main; run `yard daemon restart`") with the restart recipe inline — caught me
  between commit and restart. Excellent.
- Papercut (mine, but the surface allows it): editing `.yard/config.toml` in the working
  tree and restarting does nothing — the daemon reads canonical. The restart output
  prints the config digest, but nothing says "your working tree differs from what I
  loaded" at restart time; only the later `yard sync` did. A one-line warning on
  `daemon restart` when the working config differs from the loaded one would close the
  gap sync happened to catch.
- `codex_speed = "fast"` on the default profile: config loaded (digest 0d1c863b3b1d),
  preflight all 9 proven. No review has run on the fast tier yet — latency/quality
  observations to follow with the next lane.

## 2026-08-31 — design review wired into yard (Y-488 exercised at config level)

- Built the layering settled in the design-review thread: `[checks.review.profiles.ui]` =
  default codex panel + an instructed claude reviewer pointing at
  `reg-webapp-design-reviewer`; `[workflows.ui]` selects it; the implementation role's
  instructions gained the in-lane self-check clause. The Y-488 table shape
  (`{ reviewer = "...", instructions = "..." }`) parsed and loaded first try; the
  per-reviewer form is exactly right for one instructed reviewer beside an uninstructed
  codex panel-mate.
- **Preflight earned its keep again — and found a real yard bug.** With the ui profile
  loaded, preflight demanded `reviewer:claude` and reported "no authenticated session"
  although `claude auth status --json` in my shell says loggedIn:true. Root cause,
  verified by env bisection + reading the source: the reviewer env is built from a
  positive allowlist (PATH, HOME, SHELL, USER, LOGNAME, TMPDIR, TERM, TZ, LANG, LC\_\*)
  over the **daemon's ambient env — and the daemon's own env floor is just HOME+PATH**
  (ps eww shows nothing else). On darwin, Claude Code resolves its keychain session only
  with USER present (`env -i HOME=… PATH=… claude auth status` → loggedIn:false; add
  USER → true). So the allowlist names USER but the daemon has none to pass, and every
  claude-engine reviewer run would fail auth the same way. Suggested fix: the daemon
  keeps (or derives via getpwuid) USER/LOGNAME in its own floor — the allowlist is
  right, the ambient it filters is too bare.
- The probe's failure text ("run `claude auth login` as this user") is actively
  misleading here — the user IS logged in; the session is invisible only to the daemon's
  env. A probe that knows it is on darwin and has no USER could say so.
- Not yet knowable: whether the unmet `reviewer:claude` requirement blocks admission
  globally or only ui-workflow tickets. Nothing is ready on the board; the next
  default-workflow ticket settles it (rollback if global: comment out the ui profile).

## 2026-08-31 — Y-11 trial: admission scoping settled, one watch papercut

- Filed Y-11 (light, doc-only) specifically to test whether the unmet `reviewer:claude`
  requirement blocks admission globally. **It does not**: the light-workflow ticket was
  admitted within seconds and ran to a clean landing while preflight still reports
  `not ready` on `reviewer:claude`. Per-profile requirement scoping is the right call —
  please pin it as contract, not accident.
- Worker behavior was exemplary: the ticket carried a conditional ("mirror the edit iff
  the `.agents` mirror exists"), the mirror does not exist, and the worker took the skip
  branch correctly instead of inventing the file. 23s of worker time, $0.33, gates 3/3,
  landed on first read.
- Papercut: the `yard status --watch --since <cursor>` armed before admission exited 0
  with an EMPTY output — no wake line, no quiet line — although a wake clearly happened
  (cursor 5519→5685, approval-needed raised). A watch that exits without writing the
  line it exited for leaves the operator to re-read the board cold; if it delivered the
  wake somewhere, it wasn't stdout.

## 2026-08-31 — Y-12: `yard init` scaffolds one agent path, once

- **Codex cannot discover the operator skill Yard scaffolds.** `yard init` (0.10.3)
  writes `.claude/skills/yard-operator/SKILL.md` and nothing under `.agents/skills/`, so
  a fresh Codex session — the other half of how this board is actually driven — has no
  operator routine in its catalog at all. Bridged here with a tracked relative symlink
  (`.agents/skills/yard-operator` → `../../.claude/skills/yard-operator`), which is the
  cheap fix; the durable one is for `init` to write the alias itself, or to ask which
  agent catalogs to scaffold. The root instructions have the same shape — this repo
  already keeps `AGENTS.md`/`CLAUDE.md` as byte-identical copies behind a pre-commit
  check, because static tools that do not follow links must see both.
- **The scaffolded routine is a copy, and an upgrade never revisits it.** The skill
  `init` wrote pre-dates `yard lane replay`, so after 0.10.3 its §6 still said "no
  command carries a candidate into a fresh lane, and none is needed" — the binary moved
  and the operator's own instructions were left asserting the opposite of current
  behavior. Nothing surfaced the drift; we caught it by reading. A version stamp in the
  scaffolded file, plus a `preflight`/`doctor` note when it trails the installed
  release, would close the gap. Overwriting on update is not the answer: the
  repository-specific appendix (admission rule, filing conventions) lives in that same
  file by Yard's own design.

## 2026-08-31 — `--watch --json` usage error exits 0

`yard status --watch --json` refuses with a usage error ("--watch writes JSON lines
already; --json says nothing more") but exits **0**. A script or operator loop that
armed the watch this way believes it is watching when it never armed — the exact failure
mode the wake-driven loop exists to prevent. Usage refusals should exit non-zero.
(Minor: the refusal is also printed twice, once plain and once as JSON.)

## 2026-08-31 — retarget/.hypothesis + restart timeout

- Y-13/1 retarget onto moved canonical blocked on the lane clone's untracked
  `.hypothesis/` (property-test example DB). The error's remediation was exact and
  worked (declare in build_artifacts, commit+sync, daemon restart, re-run e3) — good
  message. Papercut: every new ignored cache dir is discovered one failed retarget at a
  time; a preflight that lists ignored-dir candidates not in build_artifacts would have
  caught `.hypothesis` before it cost a blocked lane.
- `yard daemon restart` timed out ("acknowledged the stop but had not released the
  project within 60000ms") — yet immediately afterwards `yard daemon status` said no
  daemon was answering and a plain restart started cleanly. The timeout message
  suggested watching a quiesce that had apparently already finished.
- Follow-up: the very next retarget (e4) stopped on
  `reg_webapp/backend/src/reg_webapp/__pycache__` — the one-at-a-time discovery bit
  twice in ten minutes. Fixed by enumerating the whole per-package `__pycache__` spray
  from a main checkout in one commit. Also: the retried `yard lane start` right after
  `yard daemon restart` was refused with "startup reconciliation is still in progress" —
  fair, but the restart command returning before lane mutations are available makes the
  natural restart-then-act sequence race-y by default.
- Y-13/1 round 3: the reviewer flagged behavior ratified mid-lane by the operator (A-28:
  range editions = one lo..hi file), because the frozen ticket body still carried the
  pre-decision "per period unit" example — the "amendment does not travel" shape,
  mercifully advisory. A way to surface operator decisions recorded on a rejected
  proposal to the lane's reviewer (they ARE in the lane's history) would have avoided
  arguing §12 against the stale brief in a rejection note.
- Y-19/1 worker died on a transient provider error ("Connection lost mid-response"). The
  worker-failed attention item printed ONLY the abandon exit; the actual recovery —
  `yard lane start Y-19/1 --expect-generation 1` re-running the failed round with no new
  attempt — had to be found in `yard lane start --help`. For a provider-error reason
  specifically, the re-run exit seems like the primary answer and abandon the
  destructive one; offering only abandon invites exactly the
  healthy-attempt-stopped-on-a-misreading failure the skill warns about.
- 2026-09-02, investigating deterministic UI checks: needed to run the `frontend` gate
  against a working tree without admitting a lane. There is no `yard` verb for that, so
  the run was reconstructed by hand — `docker build -f .yard/Dockerfile`, then
  `docker run --network none` with the config.toml command and the documented env floor
  re-typed. Worked, but the image Yard actually uses is not discoverable
  (`docker images` lists `yard-gate-local`/`yard-gate-node24`, `docker image inspect` of
  those names errors — an uncertain mapping between Docker contexts), so a local gate
  run cannot claim to be the same image a lane sees. A
  `yard check run <gate> [--tree PATH]` that reuses the daemon's image and env would
  remove the guesswork.

## 2026-09-02 — decision memos arrive as `ticket.create` proposals

- Y-25's worker correctly stopped at a defect outside its scope and filed A-45 as a
  `blocksCompletion` proposal — but the only shape available was `ticket.create`, so the
  memo ("here are three options for the operator") became parked ticket Y-26 whose body
  was a menu, not a brief. A worker handed that body would have had to choose the design
  itself. The operator had to rewrite the whole body before unparking, which is fine
  once, but a "decision needed" proposal kind (question + options, resolved by the
  operator into zero or more tickets) would match what the worker actually produced.

## 2026-09-02 — Y-26/1: gate image build has its own hidden 15-minute clock

- The `frontend` gate on Y-26/1 failed with `docker build … [timed out after 900000ms]`.
  That 900 s is not `timeout_minutes` (20 on this gate) — it is an image-build limit
  nothing in `config.toml` names. The candidate touched `reg_meta/pyproject.toml` (a
  comment), which is a COPY key for the Python warm layer, so every layer after it
  rebuilt from the network. A manual
  `git archive HEAD | docker build -f .yard/Dockerfile -` of the same Dockerfile
  completed in about two minutes, so the gate hit a transient stall, not a slow build —
  but the attention item reads exactly like a candidate gate failure. Two asks: name the
  image-build timeout in config (or reuse the gate's), and in the gate-failed item say
  "image build" vs "gate command" up front so the operator knows the diff was never
  exercised.

## 2026-09-02 — a landed ticket left open re-admits itself the moment its blocker lands

- Y-25/1 landed on 2026-09-01, but the ticket stayed `open` with a `depends_on Y-26`
  (the worker's blocksCompletion proposal). When Y-26 landed, the scheduler saw a ready
  ticket and admitted Y-25/2 — a fresh Opus/xhigh attempt for work that was already on
  main. Caught within a minute (park → stop → abandon → done), 38 s of worker time. A
  ticket whose current attempt has landed should not be auto-admitted again just because
  a later-added dependency cleared; at minimum the operator should be asked, since
  "landed but still open" is precisely the blocksCompletion shape Yard itself creates.

## 2026-09-02 — vendored worker-CLI pin silently refuses a configured model; the only exit re-admits into the same failure

- `roles.planning.model = "claude-fable-5-1"` died at admission twice (Y-29/1, Y-29/2)
  with
  `provider-error: Claude Code 2.1.236 does not support this model; version 2.1.251 or newer is required. Run 'claude update'`.
  Three papercuts stacked:
  1. The error's remediation is wrong for Yard: the worker runs the **vendored** CLI
     (`claudePin("2.1.236")` baked into yard 0.11.0, published to `.yard/local/tools/`),
     so `claude update` on the host — already at 2.1.258 — changes nothing, and neither
     does a daemon restart. The operator only finds the real cause via
     `daemon preflight --full` (`worker-cli:claude ... 2.1.236`).
  2. The `worker-failed` item offers exactly one exit, `abandon` — and abandoning an
     unparked ticket re-admits a fresh attempt into the identical refusal within
     milliseconds. Nothing suggests parking first or flags that the failure is
     environmental (deterministic, config-caused) rather than transient.
  3. There is no config knob or env override for the pin, so the only recoveries are
     "change the role's model" or "release a new yard". Asks: bump the pin (2.1.258+),
     validate configured models against the vendored CLI's support at config load or
     preflight (fail at `yard daemon restart`, not per-attempt), and let a deterministic
     provider refusal park the ticket instead of looping.
- Operator workaround: planning role moved to `claude-opus-5` with a revert-trigger
  comment in config.toml.

## 2026-09-02 — chunked autoreview reports crash yard's parser: large diffs are unreviewable

- Y-29/4's candidate deletes the 1.1 MB `steward.project_data.json`, so autoreview
  (agent-skills @ fe73c380) chunked the review into 3 passes and wrote its report with a
  top-level `pass_reports` key. Yard 0.11.0's report schema is strict and rejects it
  (`unrecognized_keys: pass_reports`), so the execution errors with no verdict — twice,
  deterministically — even though the review itself completed and found two P1s (visible
  only in `review-tool.log`). Any candidate big enough to chunk can never record a
  review under this pairing; re-run loops forever. Asks: parse the chunked shape (or at
  least passthrough unknown keys and read the merged findings, which are present in the
  same report), and surface "report unparseable but review completed with N findings"
  instead of a bare error. Operator workaround: read the findings out of
  `review-tool.log` and hand them to the worker as a nudge — which works but records no
  review verdict, so the lane cannot reach approval-needed until yard or autoreview
  moves.

## 2026-09-02 — 0.12.0: conflict worker's handoff refused `lane_propose`; the rebase burns each retry

- Y-29/4's retarget conflict (3 files, vs the landed Y-30) went to the conflict role,
  which resolved it perfectly — clean rebase, empty porcelain, target an ancestor of
  HEAD — and then had its handoff refused:
  `worker capability does not grant method "lane_propose" (I-1)`. The daemon then
  reported `conflict-handoff-failed` with
  `evidence input is not a regular file: …/retarget.log` (only `transcript.jsonl`
  exists), which points at evidence collection rather than at the real refusal buried
  mid-transcript. Twice, deterministically, under the 0.12.0 daemon (the attempt itself
  predates the store migration — possibly the capability record migrated wrong). Each
  `yard lane start` retry starts from a FRESH clone (the prior session's rebased commits
  are gone), so every retry re-spends the whole ~$3.9 rebase before failing the same
  way. Asks: surface the capability refusal as the attention item's reason instead of
  the evidence-collection symptom; don't charge a full redo when the failure is the
  handoff, not the resolution. Operator recovery that worked: abandon (candidate
  retained) + `yard lane replay` as a fresh 0.12.0-native attempt.

- **Update (decisive)**: a fresh, fully 0.12.0-native attempt (`yard lane replay` →
  Y-29/5, admitted and executed entirely under the new daemon and store schema) hits the
  IDENTICAL `lane_propose` capability refusal on its conflict resolution. Not migration
  debris — a live 0.12.0 defect: the conflict worker's capability grant does not cover
  the method its own handoff calls (rename drift in the worker boundary?). Every
  conflicted retarget board-wide is currently unresolvable; each attempt burns the full
  rebase before failing. Y-29/5 is held at the stop (costs nothing) pending a switchyard
  fix.

- **0.12.2 follow-up**: the Y-538 fix has a second half that regressed. A fully
  0.12.2-native conflict execution (Y-29/6/e3, admitted post-upgrade) fails at MCP
  registration:
  `init frame omitted mcp__yard__lane_report_too_large, mcp__yard__lane_publish_plan`.
  The per-kind `tools/list` narrowing correctly withholds those from a conflict
  execution, but the registration validator still requires the full worker tool set — so
  no conflict execution can start at all now (cheaper than 0.12.0's fail-after-rebase,
  but absolute). The registration check needs the same per-kind expectation the
  advertisement got. Y-29/6 held at the stop.

## 2026-09-02 — 0.12.3: the grant fix works, but the conflict path's evidence writer never produces retarget.log

- Y-29/7/e3 under 0.12.3: the conflict worker resolved the retarget and its
  `lane_propose` was ACCEPTED ("Retarget resolved and committed. Branch yard/Y-29-7 is
  now d2c0c33 + the two candidate commits") — the Y-538/0.12.3 chain is proven good. The
  execution still ends `conflict-handoff-failed` with
  `evidence input is not a regular file: …/e3/retarget.log`: the daemon's finalization
  expects a retarget.log the conflict path never writes (the execution dir holds only
  transcript.jsonl), so a SUCCESSFUL resolution is discarded at evidence collection —
  twice (g1 ~$6.6, g2 re-ran the whole session). This was the masked second defect
  visible since 0.12.0 behind the capability refusal; the daemon log records nothing
  about it. Asks: write the log (or drop the requirement) on the conflict path, and
  surface the finalization error itself rather than the evidence-collection symptom.
  Lane held at the stop again; the resolution commits exist on the lane branch each time
  and are discarded each time.

## 2026-09-03–04 — Preflight repair and UI retry trial

- The verified 0.13.0 upgrade preserved the store, configuration, admission state and
  Y-29/7 stop. Preflight correctly rejected a skipped Python test: `_git_env()` in
  `test_slug_snapshot.py` removed Yard's scoped `safe.directory=/workspace` settings,
  then hid Git's ownership refusal as an unavailable checkout. The same gate environment
  existed in 0.12.3; this was a project helper defect exposed by verification. Y-32
  preserved trust while stripping routing variables and landed `68fe1a5`. Fresh
  preflight passed 14/14 requirements, including 3,712 Python tests with no skips.
- Y-21 reproduced the empty-draft failure against the real backend: `/order` returned
  `422 project_empty`, then Retry validation cleared the block without another order
  request. The repair keeps validation and order failures separate and adds Retry
  download for transport failures. A real browser recovered an `order.json` after the
  route was restored; structured 422 findings remained blocking.
- Independent review first stopped at A-71: Claude reported authentication failure
  despite a valid host login. Yard already had an identity floor; the decisive
  reproduction was Bun 1.4.0 returning username `unknown` from `os.userInfo()` when
  USER/LOGNAME were absent. `/usr/bin/id -un` restored the correct identity. An isolated
  patch and regression test were retained but never installed. The official 0.13.2
  daemon later authenticated its actual review child successfully, without credential
  rotation or a local patch.
- The original design-seat instructions required tools unavailable to autoreview's
  isolated source reviewers. The maintainer's revised contract resolved that mismatch:
  source-only review seats, deterministic Playwright gates, and operator visual
  judgment. This superseded the proposed visual autoreview mode. Y-33's host-check
  workaround was closed before admission because it would restore operator credentials
  across the host gate boundary and duplicate review orchestration.
- Autoreview also refused optional `tokens.css` context as a sensitive filename before
  invoking Claude. Removing that optional context resolved preparation; no filter was
  weakened. The generic missing-report summary required opening `review-tool.log` to
  find the cause. Codex's repeated claim that `canDownloadOrder` blocked the retry was
  rejected against the exact method and observed download; a later review was clean.
- Y-21 landed `6aea8f4` after real independent review and candidate gates: 1,285
  frontend and 3,712 Python tests, plus lint/type/build checks. Accepted the bounded
  limitation that finding-less deterministic order errors can offer an ineffective
  retry; speculative source omission was declined, and browser regression coverage went
  to Y-34. Final retry images were retained at 375/1280 only; earlier blocked/catalog
  images covered four widths. No final 768/1920 retry image payload was retained. Those
  were worker images, not evidence from the later browser gate.
- The visual re-review also noted that the retry removes its local banner while only the
  distant main CTA shows progress. That P3 was retained, not reported as repaired.
- Worker papercuts remain distinct from daemon failures: `Skill` returned unknown for
  present repo skills, recovered by reading canonical paths; `thinking_tokens` crowded
  useful calls out of `lane tail`; and an asynchronous visual subagent was followed by
  screenshot polling and an unconditional ten-minute sleep after it had finished. The
  first in-container render used a scratch launcher because bare `python3` was absent
  from PATH. It proved the synthetic fallback, not the default launcher.

## 2026-09-04–05 — Design guidance, browser evidence and memory diagnostics

- The SPA's `frontend/DESIGN.md` became the design-language authority, including the Ink
  palette and sentence case. Pointing review context at that roughly 15 KB source
  replaced the 178 KB engineering document; inline role names avoided the `tokens.css`
  refusal. `/simplify` lives in implementation-role instructions because Yard workflows
  have no separate pass stage. A clean subagent can run it, but the board has no
  distinct execution/evidence boundary; the light role must run it inline. Reconsider a
  first-class pass when a reviewer returns a candidate for work that pass should have
  removed.
- A config restart cancelled Y-35/1/e1 after eight minutes and left a stopped attempt
  without an attention item or visible status exit. Its own guarded
  `lane start Y-35/1 --expect-generation 1` resumed implementation without a new
  attempt. Remaining request: report cancelled executions and their resume exits, and
  make the resulting stops actionable. Later restart-protection checks do not establish
  this attention/discoverability issue is fixed.
- Verified 0.13.3 protected the existing daemon when sandboxed `/bin/ps` inspection was
  unavailable. Authorized restart succeeded. Disposable Y-39 proved preparation refused
  sensitive context before any reviewer/model invocation, but `lane show` omitted the
  explanation because no real invocation existed. The report requested bounded redacted
  preparation evidence. A suggested `grep -c` dry-run probe counted documentation twice;
  that was a verification-prompt error, not incompatible autoreview support.
- Y-34 waited for supported PNG retention, then added three project retry flows at four
  widths. Review caught two screenshots taken before busy markers cleared. Candidate and
  base frontend failures produced useful `gate-fails-on-base` evidence and guarded
  exits. Docker positively recorded an OOM for the base failure; the candidate timeout
  had no recorded OOM. Temporarily raising concurrency from two to three had reduced
  container memory from a computed 3,489 MiB to an observed 2,617 MiB without swap.
- Restored concurrency two in `d717f8f` and replayed the same attempt. Y-34 landed
  `d1de368` after 1,285 frontend tests, 3,712 Python tests, other gates and all 12
  browser cases. All 16 declared PNGs survived container teardown and were opened at
  widths 375/768/1280/1920. Final reviewers incorrectly requested a concurrency
  restoration already present on the new base; the approval disposition cited both base
  and candidate. No assertion or gate was weakened.
- Ordinary gate teardown initially lost OOM accounting that worker failures retained.
  Verified 0.13.5 added gate/base diagnostics and uncapped memory by default. This Mac's
  real containers showed Memory=0 and MemorySwap=0; no LXC setting was changed.
  Released-source checks passed 112 unit tests and three lifecycle regressions, covering
  cap settings, preparation redaction and positive/unknown OOM evidence without inducing
  host OOM. Live Y-40 reproduced the earlier preparation refusal with visible excerpts
  and zero reviewer usage, resolving Y-39's excerpt gap.
- Both diagnostic candidates were retired without landing. The temporary third slot was
  removed; final product/config content matched `d1de368`. Full 0.13.5 preflight passed
  17/17 with process-inspection access and all four gates, including 12 browser cases.
  Docker-off preflight correctly marked gates unrun. The tiny diagnostic worker still
  spent minutes on its configured simplify pass and looking for Yard source: worker
  orchestration overhead, not autoreview spend. Use a narrower supported diagnostic role
  or replay route if one becomes available.

## 2026-09-06 — UI skills integration and parked audit tickets

- Y-35's old-base checks missed a newer project-flow fixture consumer of the helper it
  removed. A narrow integration repair was required before approval. During that repair,
  0.13.5 accepted a conflict nudge, finished the worker at `439141a`, then repeatedly
  failed persistence with `is a conflict execution`. `beginQueuedTurn` excluded conflict
  although `requestNudge` accepted it. Saved the completed diff and used the guarded
  stop path to deliver the held message; no manual state edit or restart. Continuation
  creates a fresh staged clone, so the small resolution had to be repeated.
- Y-35/2 then landed `a8e1ea1`, with clean review, all three gates and 3,713 Python
  tests. Independent final-head host flows passed 12 cases/16 screenshots under normal
  Chromium permissions. The sandbox single-process fallback crashed opening its second
  context. This recovery concerns Y-35; Y-29/7's older `retarget.log` stop remained
  untouched. Later 0.14.0 source checks passed eight conflict continuation/nudge cases,
  including resolution preservation; they did not exercise a live Y-29 recovery.
- Y-36 landed `b276a77` with launcher isolation, four-width capture, smoke interactions
  and cleanup fixes. Independent final-head host checks covered concurrent captures,
  invocation from another cwd, relative output paths, owned-process cleanup and survival
  of an unrelated listener. Container checks used `no-sandbox`; host checks used the
  sandboxed stage. Opposite source-review advice about "default first" reflected an
  ambiguous brief; the operator disposed of the advisory using the explicit repair and
  observed behavior. It was not a zero-finding review or a new Yard lifecycle defect.
- Y-38 landed `345a63f` with canonical shared design skills, accurate consumer
  boundaries and candidate/coverage evidence. Walkthroughs removed a desktop-only
  exception, an overclaimed exhaustive role list and an overbroad fixture promise. Final
  advisories were disposed of against DESIGN.md. A-83's screenshot proposal was covered
  here, but its ancillary claim that the project-flow PATH export was redundant was
  wrong: that gate still calls `python3` for `catalog_fixture_db.py`. Preserve the PATH
  setup.
- Y-41 landed `967e848`, correcting workflow coverage, approval evidence and the
  obsolete claim that ticket edits never reach reviewers. The worker image lacks Yard
  CLI help; the operator verified those claims on the host. Documentation briefs should
  name that boundary. Final review was clean; 23 independent alias/ingestion checks
  passed.
- Y-37 landed `a701215` with Ink styling and design-parity checks after narrow repairs.
  The operator opened 32 final-head images across eight routes and four widths; parity
  checks and deliberate mutation checks established the guard's behavior. Synthetic
  coverage omits ID/Sensitive badges; pre-existing layout/contrast observations were not
  established as restyle regressions. This batch does not prove complete UI conformance
  or real-catalog behavior. Final source review passed the P1 threshold with advisories;
  accepted limits include literal-white hover debt before future theme work, incomplete
  error-border token coverage and pre-existing graph badge geometry. A-86's unrelated
  build/release proposal and A-91's broader layout/accessibility bundle were declined
  from scope with retained dispositions.
- Filed Y-42–Y-55 atomically parked: four `ui` workflows, three plan-first tickets and
  explicit dependencies. Readback verified every body/workflow/dependency; no lane
  started. Reused Y-29/Y-31 instead of duplicating inventory work. List/new JSON
  includes complete bodies, so project only needed fields in summaries and keep full
  receipts separately. Cold watch returning the existing Y-29 attention was expected
  catch-up, not a new stop.

## 2026-09-07 — Upgrade reports, generated guidance and 0.14.1 retests

- Verified 0.14.0's release, stopped-store backup and schema 42→43 migration. All 55
  ticket records and Y-29/7 remained unchanged. Warm preflight passed 17/17, all four
  gates and both actual reviewer seats' no-model preparation probes; all 14 newly filed
  tickets remained parked. Two findings survived investigation: the cold CLI lost its
  result at 300.158 seconds while daemon gates finished, and synthetic instant
  transitions exposed tied execution-end ordering already present in 0.13.5. No live
  ordering failure or new release regression was established.
- An initial local builder handoff was superseded by upstream issues after open/closed
  duplicate searches. All three are fixed in 0.14.1: [#48 preflight
  timeout](https://github.com/adamaltmejd/switchyard/issues/48), [#49 timestamp
  ordering](https://github.com/adamaltmejd/switchyard/issues/49) and [#50 proposal
  timing](https://github.com/adamaltmejd/switchyard/issues/50). The last report
  concerned advice to wait for an origin lane that a ticket-edit proposal itself stops;
  no live stuck lane was claimed.
- Submission used the live Report form's fields through authenticated GitHub CLI, with
  full bodies read back through the maintainer-author trust gate. The initially absent
  `report` label appeared on successful submission; that is not an outstanding defect.
  After browser login, the form and attribution were inspected without submitting again.
  At the maintainer's request, `filed-by:codex` was created/applied to all three issues;
  the author account and credentials stayed unchanged. Archived OPEN receipts describe
  submission-time status, not current issue disposition.
- The upgrade first retained a customized skill, then inspected `yard init`'s offered
  patch. The final approach replaces the selective fork with the exact generated
  template and moves project policy to `.yard/OPERATOR.md`. AGENTS.md/CLAUDE.md require
  both, with project policy taking precedence; the Codex alias remains one routine. Only
  generated skill paths are exempt from Panache. Preserve local admission/cleanup,
  disclosure, determinism, UI evidence and reporting rules, and review version-specific
  corrections at upgrades. Exact template matching restores `matchesTemplate=true` as a
  drift signal.
- Installed official 0.14.1 at `3798a98cf30979b2ed6e21e2041a5f5839fd195c`, verified
  against its checksum/contract/CI evidence. Schema 43 and configuration were unchanged.
  Live preflight passed 17/17: 3,718 Python tests, 1,287 frontend tests,
  lint/type/version checks and 12 browser-flow cases at four widths. Those preflight
  PNGs were not separately retained or reviewed. The pinned Claude executable passed
  checksum and offline `--version` checks in the exact image; no live model session
  tested provider acceptance.
- Independent #48 retest used the actual native CLI against a synthetic socket: valid
  JSON, exit 0 at 310.237 seconds, with a parallel timeout control. This is separate
  from the real-gate preflight. Pinned Bun 1.4.0 passed six client-boundary/cancellation
  tests, 21 check tests, three original frozen-clock reproductions and two ordering
  guards. Twenty fresh host Bun 1.4.2 processes passed 420/420 check cases. The host
  raw-fetch timeout control differed; it passed under release-pinned Bun. Initial long
  probes hit the scratch harness's 120-second limit and passed with its supported
  400-second limit. These setup observations are not native-release failures or new
  reports.
- Applied the 0.14.1 init patch: exact stamped template and alias verified, 23 existing
  skill alias/ingestion tests passed, policy format/lint passed, and mirrored agent
  files remained identical. Upstream now explicitly handles stopping ticket-edit
  proposals; removed only the redundant local timing correction. The earlier local fork
  is obsolete.
- During upgrade verification, all 55 ticket records and the complete Y-29/7 response
  stayed identical; Y-42–Y-55 remained parked and original admissions were restored. No
  paid model session, toolkit candidate change, deployment or MONA run was made. Y-29/7
  remains stopped at A-68, generation 2; newer conflict-source tests and Y-35's recovery
  do not establish its recovery. Its pre-0.14.0 store and old binary remain in
  `/Users/adam/.local/state/yard-upgrade-backups/registry-research-toolkit-20260907-pre-v0.14.0/`.

## 2026-09-07 — Y-29 guarded retry stops before conflict work

- Copied A-68's offered exit, `yard lane start Y-29/7/e3 --expect-generation 2`, after
  verifying the current 0.14.1 CLI, daemon and project policy. It reported
  `re-running conflict resolution; no new attempt`. The immediately re-armed watch
  produced A-95 at seq 26504: generation 3 failed with
  `invalid Git branch name: "yard/Y-29-7"`. Direct Git validation accepts that name. The
  candidate head and reported usage totals stayed unchanged. This existing attempt
  predates 0.14.x; the observation does not establish a new release regression or the
  underlying Git failure. Filed upstream
  [#51](https://github.com/adamaltmejd/switchyard/issues/51) with the exact output and a
  source lead that distinguishes the hidden Git cause from the reported branch error.
- A-95 offers only abandonment. Left the active attempt and retained candidate intact;
  no retry was invented and no daemon-owned state was edited. Y-31 remains blocked on
  Y-29, and all parked tickets remain parked. The final watch returned quiet at 26504.
- Diagnostic papercut: `yard lane tail Y-29/7/e3 --current-generation` returns exit 2
  from `lane.state`'s ID schema, although help accepts execution IDs and `lane show`
  resolves this one. The attempt-ID form returns exit 0. Reproduced twice, confirmed the
  CLI/method mismatch against the exact release source, and filed separate
  [#52](https://github.com/adamaltmejd/switchyard/issues/52). Both reports followed
  open/closed duplicate searches and use `report` plus `filed-by:codex`.
- Read-only Y-31 review identified scope clarifications for its eventual admission:
  remove the project-source sentinel exception from structural validation, preserve
  catalog full-history queries and generic order guards, and align project-writing
  frontend helpers and version declarations. No ticket edit or tests were run.
  Local-only receipts, report bodies and the scope note are in
  `archive/reports/yard/2026-09-07-y29-retry/`.

## 2026-09-07 — 0.14.2 restores diagnostics and reaches review

- Installed official 0.14.2 at `4371fb3e5474386f7ebfa7315db1a780ba219b5e` after matching
  its binary, checksum, release contract and successful CI run 34131493850. Restarted
  the project daemon through its supported command. Schema 43, configuration digest and
  every ticket record stayed unchanged. The current generated operator body still
  reports `matchesTemplate=true`; no content update was needed. Kept the old binary in
  the temporary upgrade directory. Admissions were held during the upgrade.
- Confirmed [#52](https://github.com/adamaltmejd/switchyard/issues/52) live: execution
  and attempt tail forms now return the same 993057-byte conflict transcript, and the
  same empty current-generation suffix at its correct boundary. Added verified retest
  comments to #52 and [#51](https://github.com/adamaltmejd/switchyard/issues/51).
- The g3→g4 retry now reported Git's actual `[cancelled]` failure. The six-hour
  total-work window still started on September 2; g4 failed during preparation after 26
  ms. Exact-source investigation found conflict timeouts take an abort-only path that
  loses the timeout reason and the corresponding nudge exit. The old attention still
  listed only abandonment, despite the supported start/nudge commands. Filed
  [#53](https://github.com/adamaltmejd/switchyard/issues/53), then used the guarded
  nudge to renew the window. g5 reached a real provider round and completed conflict
  resolution; e5 then retargeted onto current main, producing candidate `330d577b` on
  base `24a50dfc`. No attempt was abandoned and no daemon-owned state was edited.
- Review e6 reached three real passes over a 1424163-byte bundle, largely because the
  ticket deletes the obsolete steward JSON. Autoreview produced `pass_reports`, which
  Yard's strict schema rejects on the false assumption branch review cannot chunk. The
  lane stopped at A-97 with review error, not a passing review. Filed
  [#54](https://github.com/adamaltmejd/switchyard/issues/54) with the installed tool's
  clean commit and retained output. Did not strip evidence or rerun an identical review.
- The retained review also found a candidate defect. An independent synthetic audit
  against the exact index blob reproduced null-representation columns receiving sibling
  intervals and gaps, plus a falsely widened browse-year span. This does not establish
  over-ordering: the coordinate-period map is not the order materializer's input. Used
  A-97's guarded repair exit with narrow interval-association and regression-test
  guidance. Product repair remains separate from #54's upstream review integration.
- The repair produced `13eb35d322a973b028a023e199ed19412d911617`. All three independent
  synthetic cases now pass, including disjoint rename years and a real availability gap
  inside a continuous edition. The audited index and test blobs match the host diff; the
  full diff is capped inside the obsolete JSON deletion. The worker reports 3,714 tests
  passed, six skipped, and clean lint, format and type checks. That full suite was not
  independently repeated.
- Review e7 completed all three passes with zero findings and tool exit 0, but the same
  `pass_reports` rejection raised A-98 at seq 27470. Added and read back the clean
  reproduction on #54. Yard gates have not run; the candidate is unapproved. Preserve it
  for the offered `yard lane start Y-29/7/e7 --expect-generation 1` after the
  integration fix. Admissions are restored, all 20 parked tickets remain parked, Y-31
  remains blocked on Y-29, and canonical/local main remain `24a50dfc`. Watch cursor:
  `27471`.
- These were focused live retests, not a repeat of the full preflight suite. The
  worker's local checks, Yard's completed retarget, and the failed review are distinct
  evidence. Local-only release, lane, history, issue-readback and diagnostic receipts
  are in `archive/reports/yard/2026-09-07-v0.14.2-evidence/`.

## 2026-09-07 — 0.14.3 accepts chunked review and lands Y-29 and Y-31

- Installed official 0.14.3 at `5ab03815878bca835de6f218123b1247e7a194dd`, matching the
  binary checksum, release contract and successful CI run 34138868110. CLI and restarted
  daemon agree; schema 43, configuration, all 57 tickets and the complete retained lane
  view were unchanged across installation. Operator template still matches. The old
  0.14.2 executable is retained in the temporary upgrade directory.
- Retried A-98's guarded review exit. New review e8 accepted all three `pass_reports`
  with no parse error, retained its finding and overall incorrect verdict, and passed
  under the configured threshold with one P2 advisory. All candidate gates then passed:
  frontend (1,287 tests, build and generated contract), lint/types, and test (3,716
  tests, no failures or skips). This directly verifies
  [#54](https://github.com/adamaltmejd/switchyard/issues/54); the retest comment was
  read back through the trust gate.
- Advisory a1 claimed merging overlapping editions ending at year 9999 overflows.
  Independent validated construction rejects that year before index building: the
  authored grammar is 1900–2099, and null-representation states are clipped to those
  edition bounds. Maximum-valid 2099 overlaps pass for explicit and null mappings.
  Recorded the false-positive disposition in the approval, with widening the authored
  year grammar as the revisit condition. No speculative repair or follow-up was filed.
- Independent candidate checks passed 183 tests (one release test deselected), with the
  three earlier rename/gap regressions still passing. All changed source and test blobs
  were accounted for; the host diff cap cuts only the intentional 39,557-line legacy
  JSON deletion. Frontend changes are generated comments only. Approved exact head
  `13eb35d322a973b028a023e199ed19412d911617`; the terminal wake at 27676 reported
  landed, and `yard sync` plus local Git confirmed that head. The first sandboxed sync
  could not spawn `/bin/ps`; the same supported command succeeded with
  process-inspection permission. This is a host sandbox boundary, not a release
  regression.
- [#53](https://github.com/adamaltmejd/switchyard/issues/53) also has focused
  verification at the exact released source: five isolated tests, 27 assertions, cover
  expired planning/conflict timeout classification and printed exits, the existing
  implementation deadline, nudge renewal and retry anchor preservation. No live expired
  execution was fabricated or retried. Submitted that bounded result and read it back.
  No full preflight or live SWECOV/flavored-DB deployment validation was run.
- Y-31 was already selected and became ready after Y-29 landed. Corrected its brief from
  revision 4 to 5: remove only researcher-project sentinel handling, preserve
  full-history catalog queries and generic order guards, and align project-writing
  helpers and version declarations without absorbing Y-50. Selected the UI workflow for
  the project-writing control states, then restored admissions. All 20 previously parked
  tickets remain parked. Local-only receipts are in
  `archive/reports/yard/2026-09-07-v0.14.3-evidence/`.
- Operator visibility remains noisy on a long implementation. Y-31's first progress note
  arrived after 18 minutes and was truncated before its verification summary. A plain
  `lane show --json` produced roughly 62,000 tokens, including raw provider frames;
  selecting fields locally and reading transcript deltas made it usable. The transcript
  showed ongoing work, so this was not evidence of a stalled lane. A compact
  candidate/status response and shorter worker progress notes would reduce operator
  overhead without dropping retained evidence.
- Y-31 initially used 2.1.0 for a breaking change to a published schema. Guided it to
  3.0.0 with matching consumer floors, lockfile and new-project seed, preserving the
  existing version gate and leaving Y-50 parked. This was a candidate correction, not a
  Yard defect.
- The worker-completion boundary returned the same session to remove ignored
  `node_modules`, despite its build-artifact declaration. This matches the README's
  dependency-tree exception, but the recovery prompt says ignored build artifacts need
  no action and labels Yard's augmented listing as ordinary Git output. Filed
  [#55](https://github.com/adamaltmejd/switchyard/issues/55) for the prompt and comment
  inconsistency and read it back exactly. The worker removed its own copied tree; no
  candidate-cleanliness guard was weakened. The cleanup generation cost about nine
  minutes and $1.69, without evidence attributing all of that to the wording.
- Independent Y-31 audit at `46966f1` matched 272 finite-period semantic comparisons
  over ten synthetic catalogs. Root browser checks passed at 375, 768, 1280 and 1920
  widths: imported sentinel draft rejected, full-history catalog still usable, and an
  explicit finite study window produces a valid finite project add. Requested two small
  missing permanent regressions through a guarded nudge: no committed coverage from an
  imported sentinel, and finite staged add replacing that sentinel. An initial no-window
  browser alarm was corrected after source review: the unresolved add behavior predates
  this candidate and is outside Y-31.
- The final test-only amendment produced `87d4c8a`. Round-2 code review was clean, but
  the design seat invented stale `>=2.0.0` specifiers outside the lockfile diff and
  blocked the candidate. Direct `uv lock --check` and `uv sync --locked` both passed at
  the exact head; the claimed entries do not exist. The first design round had
  explicitly recognized the editable-member lock format. Its separate P3 invalid-input
  concern is already excluded by the documented structural gate and permanent
  route/shape tests. No code change was warranted for either finding.
- Automatic repair g5 then failed before starting: a 30-second Docker info probe timed
  out. The same daemon answered in 0.34 seconds afterward. A-100 printed only
  abandonment, although supported retry/nudge handlers and retained-review adjudication
  remained available. Source inspection established the standard `approve --residual`
  path for this terminal worker and exact review; A-101 records the false-positive
  evidence, resolves the stop, and queues every gate without approving landing. Filed
  and exactly read back [#56](https://github.com/adamaltmejd/switchyard/issues/56) for
  missing recovery exits. Retry/nudge were not exercised here; the reported recovery
  used review e6, with the full candidate-head guard. No workspace edits, abandoned
  attempt or weakened gate.
- Y-31 final gates passed at `87d4c8acfbfa3cd405b0c91c787401921712ceff`: frontend 1,289
  tests plus build/codegen; lint/types/versions; project-flows 12 cases; Python 3,713
  tests with no failures, errors or skips. Opened all 16 retained e9 PNGs and matched
  its logged full HEAD, plus 16 focused exact-head catalog/project captures, all at
  375/768/1280/1920 widths. Recorded the existing graph-label clipping as outside this
  candidate. Review e6 remains visibly failed with the evidence-backed A-101
  disposition; no fabricated clean verdict or waived gate.
- Approved the exact head after the host reads. Terminal wake 28326 confirmed landing;
  supported sync and local Git confirmed it reached main. Final quiet cursor 28337: no
  active lanes or attention, admissions enabled, 37 done and 20 parked/open. Park states
  and reasons are unchanged; only Y-29/Y-31 records changed from the upgrade baseline.
  Local-only final receipts are in the 0.14.3 evidence archive and verification report.
  No deployment, package publication, MONA execution or real-data processing.

Detailed reports, submission receipts and raw evidence are retained under the ignored,
host-only `archive/reports/yard/` directory. Key records are
`2026-09-04-v0.13.2-verification.md`, `2026-09-04-v0.13.3-verification.md`,
`2026-09-05-v0.13.5-verification.md`, `2026-09-06-ui-skills-completion.md`,
`2026-09-06-yard-conflict-nudge.md` and the `2026-09-07-v0.14.{0,1,2,3}-verification.md`
reports. They are local supporting evidence, not tracked deliverables; upstream issues
are the submitted handoff. The complete pre-condensation log is retained alongside them
at `cleanup-2026-09-07/original/.yard/DOGFOOD.md`.

## 2026-09-07 — parked-ticket reassessment and restart

Reassessed all 20 parked tickets at `87d4c8a` before admission. Nine briefs needed
current-contract or workflow corrections: Y-14–18, Y-22, Y-46, Y-48 and Y-50. Y-15's old
bug premise contradicted a later deliberate partial-picker guard; Y-17 and Y-22 partly
described work that had since landed. Guarded edits preserved parking and were read back
exactly. This supports rechecking old briefs rather than admitting a batch merely
because its dependencies are done.

Unparked Y-42 and Y-43 into the existing two worker slots, with a cursor watch after
each decision. Fresh synthetic/browser probes reconfirmed both failures. Y-50 is
prepared to follow, with its project-flow fixture aligned to the 3.0.0 contract.
Detailed decisions and verification receipts are local-only under
`archive/reports/yard/2026-09-07-ticket-reevaluation.md` and
`archive/reports/yard/2026-09-07-unpark-evidence/`.

Y-43 landed at `567767f` after two clean reviews and passing gates. A Markdown wrap miss
in the worker's first candidate triggered an automatic repair and another full
review/gate cycle; the repair changed no code or test bytes. The independent operator
audit transferred its results by verified tree identity rather than rerunning them.

Y-42's first committed candidate (`13fc749`) added eight catalog screenshots to an
existing 16-file gate. Candidate intake refused the resulting 24-file declaration at
`candidate-config` (A-107). Yard's 16-file bound is documented; the useful inspection
gap was that `lane show` exposed the rejected head but `lane diff` answered "has no
candidate." The guarded nudge exit resumed the same attempt for a harness/config-only
repair, preserving all 24 images in two checks. This incident required no manual lane
state or workspace edits. Detailed receipts remain in the local-only unpark evidence.

The config-stop inspection gap is filed as [Switchyard
#57](https://github.com/adamaltmejd/switchyard/issues/57), with the recorded base/head,
null candidate, and exact `lane diff` refusal. The artifact limit itself is documented
and was not reported as a defect. The split-gate repair produced candidate `8540851`,
but retargeting onto the landed Y43 head then stopped on the worker's ignored
`.vitest-attachments` output. Its printed exits offer retarget retry or abandonment; the
retry cannot remove the output. Read-only source investigation established the replay
recovery below without touching daemon-owned files. Separately, independent component
verification found that a pending catalog Add can mutate a deliberate New project after
the component unmounts. This required repair before Y42 approval; it is distinct from
the parked New/Open replacement-confirmation policy.

A-108 recovered through park, abandon, and explicit replay. Y-42/2 replayed `8540851`
onto the landed Y43 base and produced `4b2e2c1` for fresh review without an
implementation model turn. Source-checked `nudge`, `reject`, `stop`, and ticket-edit
guards offered no implementer return from this terminal retarget. This is a recovery
limitation, not the hidden-exit bug in #56: the guard correctly refused undeclared
ignored output and replay was supported. No new upstream issue was filed. The acceptance
clarification carries the independently reproduced pending-Apply race into the new
review; both New and successful Open reproduce in both catalog writers.

During Y-42/2's mandatory simplify review, the implementation worker continued applying
review findings and committed `98d3e50`. A reviewer assigned to read only saw that
shared tree change, attributed it to an unauthorized agent, and actively reset the
commit to `4b2e2c1`, reverted DESIGN.md and test refinements, and restored its earlier
six-file view. The transcript records those mutations explicitly. This is a
provider/subagent instruction violation in a shared worker checkout, not a demonstrated
Yard state-store defect. It exposes a practical hazard in concurrent implementation and
read-only review: a verbal read-only assignment did not constrain writes. The operator
queued a reconciliation nudge, then used the guarded stop on e1 generation 2; verified
cancellation resumed e1 generation 3 with that guidance. No operator touched lane files.
The worker reconciled the intended patch, restored its commit, reran focused checks and
negative regression checks, removed generated artifacts, and handed off the clean
candidate. An announced commit alone was not evidence of the final tree. Raw transcript
slices and public decision receipts are retained locally.

Y-42/2 landed at `98d3e50` after both review seats and all five gates passed: 1,298
frontend tests, 3,721 Python tests, lint/type/build checks, and 16 flow cases with 24
retained PNGs. The operator opened those images plus 40 independent leaf/group/project
and sibling images at all four widths. Independent tests awaited cancellation of all
four queued-Add New/Open races. Two speculative review advisories were dismissed after
reading the unchanged source: duplicate additions already merge and blocked IndexedDB
open already rejects. The clipped graph metadata also appeared in the baseline image;
Y-14's parked brief now carries the concrete reproduction, and A-105's broad cleanup
batch was rejected. A-106 was rejected pending an observed lost-window interaction.

Supported sync verified the same landed head in the working checkout. Its first attempt
hit the host sandbox's process-identity restriction; the authorized retry passed. The
idle unsupervised daemon was restarted to load the landed catalog-flow gate, then Y-50
was unparked at revision 2. Its attempt starts from `98d3e50` with both rendered gates;
the existing brief already names the stale 2.0.0 happy-path fixture to update.

A-104 was independently reproduced again on the landed app: a fresh no-query Kon pick
autosaves an empty period and type, while the `?period=2018` control validates. Current
validation reports two precise errors, correcting the proposal's earlier four-error
count. Accepted it parked as Y-58, then narrowed its brief and selected `ui` without
admission. The requested behavior uses the existing finite-period contract and requires
period selection when no valid context resolves it; it introduces no vintage default or
full-history policy. This remains separate from Y-42's completed persistence repair.

Y-50's progress note exposed an inferred compatibility window: same major, older minor,
patch ignored, justified by the separate SQLite DB rule. The project-file contract
delegates version acceptance to consumers but does not define that window. Narrowed the
ticket to exact equality with the canonical `reg_schema.__version__`, read revision 4
back, then queued the same guarded guidance for the worker's next round. Existing
ingress work remains useful, so this is an amendment within the attempt. This records a
scope clarification, not a Yard defect. Both decisions were immediately followed by
watches.

Y-50/1 landed at `98b51f6` on 2026-09-08 after both review seats and all five gates
passed (3,746 Python tests, 1,298 frontend tests, lint/type/build and sixteen flow
cases). Its supported flow fixture now uses 3.0.0. The root operator opened 24 retained
gate PNGs and sixteen independent schema-flow captures at all four widths. The separate
audit passed eighteen consumer cases, 295 focused tests and supported-output byte
identity against the baseline. A low-confidence review advisory guessed that an import
would violate isort; the actual lint gate and an independent targeted Ruff check both
disproved it. No repair or residual-risk acceptance was needed.

The independent browser harness completed three widths, then hit HTTP 429 on the fourth
because it exceeded the normal write quota. Resuming only that width after the window
passed; the limiter was not changed. This is harness pacing, not a Yard/product defect.
The raw schema-code heading remains minor copy polish under the existing additive-code
fallback. Full local-only receipts and rendered review are in the unpark evidence.
Terminal landing, supported sync and the working Git head all agree. The selected
Y-42/Y-43/Y-50 batch is complete; final watch cursor 30099 is quiet with 40 done and 18
parked tickets, no active lanes or attention items. Existing dirty DOGFOOD work remains
preserved; no second writer to main was introduced.

## 2026-09-08: Invoke the built-in simplify pass directly

The Y-42/2 transcript contains the actual Claude Code 2.1.263 `/simplify` prompt: launch
four review agents, wait for all four, then apply accepted quality fixes. Our
implementation-role instruction added an outer clean subagent; the worker assigned that
subagent findings-only work while continuing to edit the same checkout. This mixed the
built-in skill's editing role with a read-only assignment. The observed reset happened
inside the implementation turn, not during Yard's formal autoreview.

The maintainer selected direct invocation as the minimal trial. Y-59/1 changed only
`roles.implementation.instructions`: invoke `/simplify` in the implementation session,
do not delegate its invocation, let its own review and fix phases finish, then rerun
checks. The separate UI design reviewer, light role, autoreview, gates and workflows
remain unchanged. A custom skill or review harness was not introduced.

Y-59/1 landed at `1c9dcdc` after the light workflow's frontend, lint and test gates
passed, including 3,746 Python tests. No autoreview ran under that workflow. The
operator read the exact diff and verified that both TOML versions parse and every other
configuration value is identical. Supported sync initially hit the sandbox's `/bin/ps`
process-identity restriction; its authorized retry passed. Restarting the idle,
unsupervised daemon loaded the candidate configuration digest and cleared the restart
warning. This verifies the instruction change is loaded; behavior under the updated
implementation role remains to be observed on a subsequent lane.

## 2026-09-08: Upgrade to 0.14.4 and select the next batch

Installed official Yard 0.14.4 at `3c087bcc837a295f0b35492602349a71e2fe95f2` after
matching the executable digest to `SHA256SUMS`, its release contract and successful CI
run 34188453251. The CLI and restarted project daemon agree. All ticket records, schema
43 and the loaded configuration digest stayed unchanged; the existing operator routine
still reports `matchesTemplate=true`. The previous executable is retained in the
temporary upgrade directory. No full preflight or live failure was fabricated for this
patch release.

Six focused released-source regressions passed with 24 assertions: #55's cleanup
wording, #56's pre-start classification and guarded relaunch, and #57's unverified,
verified and missing-head diff cases. The CLI fixtures required process-inspection
permission after the sandbox blocked `/bin/ps`; the identical focused rerun passed.
These are synthetic source checks with stubbed integrations, not live incident
reproductions or a repeat of the full release suite.

Searched open and closed upstream reports and filed two remaining improvements:
[#58](https://github.com/adamaltmejd/switchyard/issues/58) requests earlier feedback for
undeclared ignored worker output, before the implementation cleanup continuation ends;
[#59](https://github.com/adamaltmejd/switchyard/issues/59) requests structured
individual lane detail without embedded provider frames. The first preserves the
existing artifact policy and retarget guard; the second preserves explicit transcript
access and reuses the existing detail surface. Both acknowledge existing workarounds and
distinguish the 0.14.3 observations from exact 0.14.4 source inspection. Submitted
bodies were read back through the maintainer-author trust gate and matched exactly.

Reassessed all 18 parked briefs. Selected Y-58, Y-52 and Y-51 for the next bounded
batch: unresolved catalog picks, atomic live-catalog publication, and Swedish
case-insensitive direct-name search. Source inspection reconfirmed the first two; an
in-memory query against both actual name-search functions reproduced `kön` matching and
`KÖN` missing the same synthetic `Kön` metadata. Their implementation files are
separate. Y-58 uses `ui`; Y-52 and Y-51 use `default`. Removed only Y-58's stale
parked-for-later sentence before admission. Y-15's deliberate partial-picker policy and
Y-18's unproven ranking premise remain outside this batch.

Release receipts, exact issue readbacks and bounded verification evidence are retained
locally under `archive/reports/yard/2026-09-08-v0.14.4-evidence/`. The accumulated log
is being preserved through Yard before the selected implementation tickets are admitted.

## 2026-09-08: Upgrade to 0.14.5 and adopt the generated skills

Installed official Yard 0.14.5 at `ad728ad7cd88cf2ebd3e198d7783595a8d2dbae8` after
matching the executable digest
`36c4a7d953f6b739d2fb34df0cf6bdfe9eabc7b1891576eb3e4859f3791e347f` to the GitHub asset
digests, `SHA256SUMS` and the release contract, and confirming release workflow
34210647957 passed on the exact tagged head (Linux, Darwin, Docker contract). The CLI
and restarted daemon agree; schema 43 and the loaded configuration digest
`3804f566d804f4888f301936fba1f9991072cd6b466d9ab4fbcaec3860b2714f` stayed unchanged, and
the board retained 45 done/16 open before the next batch was admitted.

`yard init` moved the generated Yard routine's canonical location: the scaffold now
stores `yard-operator`, `yard-file` and `yard-drive` as real files under
`.agents/skills/`, with `.claude/skills/<name>` as the relative alias — the reverse of
the prior direction. The skills were generated with the official binary in a disposable
empty project, `matchesTemplate=true` and `link.state="linked"` checked for all three,
and only that disposable daemon was stopped; this proves scaffold bytes, not a full
preflight. This repo adopted the three generated files unmodified, moved the
pre-existing real `.claude/skills/yard-operator` directory to its new canonical
`.agents` path, and updated the Claude/Codex aliases, `AGENTS.md`/`CLAUDE.md`,
`.yard/OPERATOR.md` and `.panache.toml`'s excludes to match.

Y-61 and Y-48 are now running; Y-53 was selected as the third product ticket, based on
fresh synthetic case-matching/bootstrap reproductions and current draft-lifecycle source
evidence. [#59](https://github.com/adamaltmejd/switchyard/issues/59) was also
live-retested while Y-61/1 ran: the lane detail JSON was 27638 bytes with 16 normalized
progress entries and no raw provider frames.
[#58](https://github.com/adamaltmejd/switchyard/issues/58) has not been live-reproduced.
Eight focused released-source tests passed with 90 assertions and both issues' bounded
v0.14.5 retest results were posted and read back exactly
([#58](https://github.com/adamaltmejd/switchyard/issues/58#issuecomment-5583171034),
[#59](https://github.com/adamaltmejd/switchyard/issues/59#issuecomment-5583173056)); #58
remains source/synthetic verification, #59 additionally has the live lane-detail sample
above, and no new real-provider cleanup incident was manufactured.

Y-61's first autoreview (candidate `5297ebcb3c1592d0340f5dce9d0a5bb77f6c9b29`) reported
a P1 SQL-precedence finding that turned out to be a false positive: the flagged function
already wraps every WHERE term through the existing join. Repairing it and rereviewing
cost an extra worker/review round for no actual scoping leak — recorded as a
review-quality papercut, not a Yard lifecycle regression. No upstream issue was filed
for it.

Local-only release/state receipts are retained under
`archive/reports/yard/2026-09-08-v0.14.5-evidence/`.

## 2026-09-08: Proposal acceptance wording follow-up

Filed [#60](https://github.com/adamaltmejd/switchyard/issues/60) for the remaining
proposal-acceptance wording in the generated `yard-file` skill. The routine correctly
starts with recorded-command execution, then categorically says acceptance creates
tickets and recommends `--parked` without the creation-only qualification. Its later
`ticket.edit` example and the CLI help establish the missing distinction: acceptance can
edit an existing ticket, and `--parked` refuses commands that create none.

Verified the installed v0.14.5 skill reports `matchesTemplate: true`; apart from its
generated version-stamp line, its contents match the exact tagged source. This is a
documentation inconsistency, not a reproduced runtime failure. The existing CLI guard is
correct. No proposal was accepted to manufacture a failure. The report references closed
#50, whose settle-first fix remains valid; the maintainer requested a separate report
for this remaining wording. Submitted with `report` and `filed-by:codex`, then read back
through the trust gate with an exact body match. Local-only report and readback receipts
are in `archive/reports/yard/2026-09-08-v0.14.5-evidence/`.

## 2026-09-08: Y-61/Y-62/Y-53/Y-48 batch complete

The batch selected alongside the 0.14.5 upgrade landed on main at
`a27af17a4e50d7ce78e8197d03b23ad66a5bc7c4`: Y-61 concept-group case matching at
`95bb969ec7fadb196beabe722edf9e9442eee836`, Y-62 generated-skill adoption at
`649206684095aa81e3bf6bf77f08c0ae82661036`, Y-53 bootstrap validation at
`b45c7e20149dc71d24c96669af75ca02b09c5e72`, and Y-48 safe draft replacement at
`a27af17a4e50d7ce78e8197d03b23ad66a5bc7c4`. Each was read via host candidate/diff,
approved at its exact full head, and verified after sync. The board settled quiet at
cursor 35677 with 49 done and 13 parked, no active or ready tickets. No push,
deployment, real corpus build or MONA run occurred. The user's local preview remains
running separately.

Y-48's final six gates passed, including 3,767 Python tests and 1,323 frontend tests.
Independent synthetic browser verification covered 44 cases plus four delayed-read
sequences, real local validation and IndexedDB reload, 16 actual downloads compared with
expected documents, and 61 opened screenshots; the operator also opened all 40 retained
gate PNGs. The final candidate preserved three explicitly accepted advisories: the
committed persistence test lacks a real reload (independent verification covered this
candidate instead), cancelling a valid Open can retain a dismissible old error banner,
and the confirmation copy mentions a last download even for a draft that was never
downloaded.

## 2026-09-08: `lane reject`'s refusal names approval; `lane nudge` is the workaround

While Y-16/1/e5 was running review of candidate
`0a9fdf9440f5feaca9d9e3a893a372ccebb49503`,
`yard lane reject Y-16/1 --expect-head 0a9fdf9440f5feaca9d9e3a893a372ccebb49503 -m <repair notes>`
refused with exit 1 and
`error: Y-16/1/e5 is running; approval requires a quiescent lane`. No rejection was
recorded. The quiescence guard worked correctly; the papercut is that the error named
approval after an attempted rejection, and installed `lane reject --help` omits the
running-execution prerequisite. These are separate facts: no lost decision, unexpected
work, corruption or guard failure was observed.

After reading `lane nudge --help`,
`yard lane nudge Y-16/1/e5 --expect-generation 1 -m <repair notes>` successfully queued
the same existing-scope repair guidance, preserved the active review and candidate, and
reported `waiting: "check"`. This is the supported way to request a repair round after
the current review completes; it does not interrupt that review, and it queues a later
implementation round that can spend for it — it is not a cost-free message to a running
model.

Open and closed upstream issue searches found no exact duplicate; related closed
[#52](https://github.com/adamaltmejd/switchyard/issues/52) and
[#57](https://github.com/adamaltmejd/switchyard/issues/57) cover other failures and were
not reused. Filed [#61 "lane reject names approval in its quiescence refusal and omits
the prerequisite from help"](https://github.com/adamaltmejd/switchyard/issues/61) for
this wording papercut. Raw evidence and the intake are local-only under
`archive/reports/yard/2026-09-08-continuation/`, notably
`Y-16-reject-quiescence-intake.md` and `Y-16-bounds-nudge.json`.

Upstream disposition (not locally retested): the maintainer closed #61, reporting it
landed as Y-627 at `08e2fd7`, first carried in v0.14.6.

## 2026-09-08: `lane tail`'s default operator view is dominated by thinking-token frames

On official Yard 0.14.5, the default operator view from
`yard lane tail Y-14/1 --current-generation` (neither `--raw` nor `--json`) produced 795
lines / 149,444 UTF-8 bytes: 453 `thinking_tokens` frames, 19 `rate_limit_event` frames,
and 6 each of `task_started` and `task_notification` frames. Thinking-token counters are
57% of the lines. Useful worker/tool output remains present; this is noise in default
presentation, not evidence of worker failure, lost output or incorrect lane state. The
request keeps raw access and useful status events. Closed
[#59](https://github.com/adamaltmejd/switchyard/issues/59) concerned `lane show` JSON
shaping and closed [#52](https://github.com/adamaltmejd/switchyard/issues/52) concerned
execution-ID selection, so neither is claimed regressed.

Filed [#62 "lane tail default operator view prints thinking-token bookkeeping
frames"](https://github.com/adamaltmejd/switchyard/issues/62). Raw evidence and
publication receipts are local-only under
`archive/reports/yard/2026-09-08-goal-completion/`, notably
`Y-14-tail-noise-publication.json`, `Y-14-tail-noise-observation.txt` and
`Y-14-tail-noise-observation.json`.

Upstream disposition (not locally retested): the maintainer closed #62, reporting it
landed as Y-628 at `2f4bd8f`, first carried in v0.14.6.

## 2026-09-08: screenshot `Read` exceeds the provider NDJSON limit and leaves only abandonment

Y-46/2/e1 generation 4 stopped with "Docker output consumer failed: provider NDJSON
record exceeded 1048576 bytes" immediately after a screenshot `Read`. The retained
provider-session result contains two identical 623,600-character base64 copies of a
467,700-byte PNG (1172 by 1999), exceeding the 1 MiB record bound; the rejected stdout
record itself was not retained, so the stored result corroborates the image-envelope
evidence rather than captures the failing stdout. Host attention offered only
abandonment. Read-only release-source and container-quiescence checks established a
supported same-session path: a guarded nudge with the existing repair brief plus bounded
image-preview guidance resumed the same attempt as generation 5, and fresh worker
activity, an unchanged retained head and no open attention were verified. This is a
recovery, not a completed repair. No attempt abandonment, lane-workspace editing or
guard removal occurred.

Filed [#63 "Screenshot Read exceeds the provider NDJSON limit and leaves only
abandonment"](https://github.com/adamaltmejd/switchyard/issues/63); now closed, with no
local retest claimed. Raw evidence and readback are local-only under
`archive/reports/yard/2026-09-08-goal-completion/Y-46-ndjson-failure/` and
`switchyard-63-1959-trusted.json`.

Upstream disposition (not locally retested): the maintainer reports Y-629 at `a2f32de`
raises the provider NDJSON record bound to 16 MiB and the frame queue to two records;
and Y-630 at `5a97f9f` classifies provider-record-oversized as a transient failure and
exposes the guarded same-session lane start exit beside abandonment. Both are first
carried in v0.14.6; the nudge recovery documented above remains valid regardless.

## 2026-09-08: Claude workers spend repeated tool calls on no-op waits for review agents

Two bounded top-level provider-frame samples on Yard 0.14.5 / Claude Code 2.1.263 /
claude-opus-5 xhigh show 54 `Bash echo waiting` calls in Y-17/1/e1 g1
(22:24:12.963-22:26:40.427 UTC, 147.464 seconds) and 65
`echo waiting`/`echo yield-1..64` calls in Y-22/1/e1 g1 (22:26:40.386-22:29:19.627 UTC,
159.241 seconds). These 119 calls are sample counts, not final totals. Y-22 had earlier
successfully used `TaskOutput(block=true, timeout=60000)`, returning `running`/timeout
after 60.093 seconds, so no missing wait capability is established. Both waits later
ended and useful work resumed; Y-22 declared candidate
`5715fb294ba696f31c54276914a2327f3d64dcb7` and entered formal review. This records extra
model/tool interactions and transcript noise; monetary cost, daemon deadlock and causal
attribution were not established. The foreground-only Yard finish instruction alongside
asynchronous provider Agent guidance is only a possible contributor. No worker
interruption or extra nudge round was requested: nudge delivers after the active round
and cannot change its current wait. Assistant-frame Monitor-like text is not attributed
to a fabricated or Yard-generated event.

Filed [#64 "Claude workers spend repeated tool calls on no-op waits for review
agents"](https://github.com/adamaltmejd/switchyard/issues/64); trust-gate-read as open,
with no upstream fix claimed. Local-only sanitized raw excerpts, the 119-call ledger,
the native wait receipt, source references and exact publication/readback are under
`archive/reports/yard/2026-09-08-goal-completion/Y-17-wait-loop-triage/`.
