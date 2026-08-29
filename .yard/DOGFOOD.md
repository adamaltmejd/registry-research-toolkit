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
