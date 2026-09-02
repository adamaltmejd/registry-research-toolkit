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
