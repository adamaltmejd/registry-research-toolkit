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
