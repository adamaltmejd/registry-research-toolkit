---
name: code-cleanup
description: >-
  Registry Research Toolkit whole-tree cleanup. Use when asked for a cleanup, YAGNI,
  simplification or consolidation review, and before a release: audit every package for
  dead code, duplication, wrong-altitude fixes and unmeasured cost, then land the
  deletions one commit per module, handing anything that touches a load-bearing guard or
  removes a feature to the maintainer.
---

# Code cleanup

CLAUDE.md says the tree stays lean ("Reuse first, build last", "Maturity and
compatibility"); this file is the recipe. Run it before a release and whenever the tree
feels fat. Preconditions: main is green and no other session is landing in the same
package. The pass reads the whole tree, not a diff.

## 0. Measure

Take the numbers before anything moves, and again at the end. `<tag>` is the release tag
prefix and `<dir>` the package directory: `reg_meta`, `reg_meta_build` and `reg_schema`
use their name for both; the web backend is `<dir>` = `reg_webapp/backend` with no
release tag (the fallback counts from the root).

```sh
last=$(git describe --tags --abbrev=0 --match '<tag>/v*' 2>/dev/null ||
  git rev-list --max-parents=0 HEAD)  # no reachable tag: count from the root
git log --oneline "$last"..HEAD -- <dir> | wc -l
git ls-files '<dir>/src/*.py' | xargs wc -l | tail -1
git ls-files '<dir>/tests/*.py' | xargs wc -l | tail -1
uv run python -m pytest <dir> -n auto -q --durations=20
```

The frontend:
`git ls-files 'reg_webapp/frontend/src/*.ts' 'reg_webapp/frontend/src/*.svelte' | xargs wc -l | tail -1`
and `bun run test` from `reg_webapp/frontend`. Compare each suite's wall time with its
budget in ARCHITECTURE.md → "Tiers". Rust: `git ls-files 'crates/*.rs' | xargs wc -l`
and `cargo test --workspace`; its dependency check is `cargo tree --workspace --depth 1`
against each crate's `Cargo.toml`.

## 1. Mechanical checks

These answer without judgment. Run them first, by one subagent on the tier below the
family's mid tier (in Claude, Sonnet); the work is lookup.

- **Dependencies.** Each package's `[project] dependencies` against what its `src/`
  imports and what runs it outside Python imports: entry points, Dockerfiles and
  entrypoint scripts, CI workflows (`uvicorn` is only invoked from
  `reg_webapp/docker-entrypoint.sh`). A dependency in one and not the other is a
  finding.
- **Dead code.** `uvx --from vulture==2.16 vulture <dir>/src --min-confidence 80`.
  Confirm each hit with `rg` across the workspace and the frontend before calling it
  dead.
- **Trace tables**, kept in the scratchpad: every argparse flag and subcommand, to its
  README or DESIGN.md line, to a test; every `openapi.json` path, to a conformance or
  backend case; every curation-TOML key the loader accepts, to a committed use. A gap is
  a finding: code without a stated behavior is YAGNI, and a stated behavior without a
  test is either a gap or a line to cut.
- **Deferrals.** `rg -n -i 'TODO|FIXME|XXX|revisit|simplify:'`. A `simplify:` whose
  named upgrade trigger has fired, or that names none, is a finding. So is a root
  tracker past its completion gate.

## 2. Read, one agent per module, in parallel

Do not read the modules yourself. Launch one subagent per module through the harness's
subagent tool (in Claude Code, the Agent tool with the `general-purpose` type; Explore
locates code and does not audit it), told to edit nothing. Launch as many at once as the
harness allows (Codex runs three children at a time) and the rest in waves. Where the
harness lets a call choose its model, set it on every call: the family's mid tier (in
Claude, Opus) for code, the tier below for docs. Modules: `reg_meta/src`,
`reg_meta_build/src` split as `sources/`, `ir/` and the top-level modules by size,
`reg_schema/src`, `reg_webapp/backend/src`, `reg_webapp/frontend/src/lib`, `scripts/`,
and `crates/` (Rust; while `RUST_RUNTIME_SPEC.md` is open, its findings go to that
refactor's owner, not to land here). The test trees are the `test-audit` skill's: run
its sweep in the same waves and merge its list into step 3.

Each prompt carries the module's paths, an instruction to read CLAUDE.md,
ARCHITECTURE.md and the package's DESIGN.md first and the module's files whole, the step
1 findings that touch it, and the rest of this section verbatim. The agent hunts what no
stated behavior asks for, what a library or the platform already does, and what is said
twice. Its best outcome is a shorter module.

One line per finding, no hedging:

`<file>:L<line>: <tag> <what to cut>. <replacement>. [design: <line or none>] [-<N>]`

- `delete:` dead code, an option nothing sets, a fallback for a state the design says
  cannot occur, a compatibility shim, a comment restating the code.
- `yagni:` a protocol with one implementation, a helper with one caller, a layer that
  only delegates. Inline it.
- `stdlib:` a hand-rolled thing the standard library ships. Name it.
- `dep:` a hand-rolled thing an installed dependency (Pydantic, DuckDB, FastAPI, Bits
  UI) already does. Name the feature.
- `dup:` the same leaf in two modules, the failure mode CLAUDE.md names. Name the home
  it belongs in (`_curation.py`, `db.py`, `query_input.py`, …).
- `shrink:` same logic, fewer lines, including a special case that a general fix to the
  mechanism removes. Show the form.
- `guard:` a cut that touches a load-bearing guard (PII/MONA confinement, disclosure
  control, determinism and byte-identity, JSON-contract validation, fail-fast), removes
  a feature, or changes a public boundary. Not the agent's call; it goes to the
  maintainer.

A speedup is a finding only with a before-number from step 0. Out of scope: correctness
bugs (reproduce at a boundary and file them), test assertions, and wording of errors and
logs. The agent ranks its list biggest cut first and ends `net: -<N> lines, -<M> deps.`
or `Lean already.`

In the same waves, one more agent reads what a user reads: the README files and each
CLI's `--help`. The bar: every sentence says how to use the current tooling, as briefly
as it can. It hunts history, ticket references, restated design, and drift between help,
README and behavior, with tags `cut:`, `shrink:` (quoting the new text) and `drift:`.

## 3. Judge

Merge the lists, dedup findings on the same mechanism, keep the ranking. Sort each into
one of three piles.

- **Land it.** One module, no change at a public boundary, no test assertion changes,
  and it names what it removes.
- **Maintainer's.** Every `guard:` finding, and anything mistagged that should have been
  one. List each with the DESIGN.md line it would change and the reason, and stop there
  until answered. A yes lands with the DESIGN.md edit in the same commit.
- **Rejected.** Everything else, each with the condition that would re-admit it, so the
  next pass does not raise it again.

## 4. Land

Edit directly, in an isolated worktree. One commit per module, the message naming what
was removed. Before each push: `uv run ruff check`, `uv run ruff format --check`,
`uvx --from ty==0.0.79 ty check`, and
`uv run python -m pytest <dir> conformance -n auto -q`; for `crates/`,
`cargo fmt --check`, `cargo clippy --workspace -- -D warnings` and
`cargo test --workspace`; for the frontend,
`bun run check && bun run lint && bun run test && bun run build` in
`reg_webapp/frontend`, as CI runs them. A change that redesigns a module rather than
deleting from it, or needs more than one sitting, becomes a GitHub issue instead (search
open and closed first).

## 5. Report

Repeat step 0. The PR body carries the numbers table (before, after), the possible net
from step 2 beside the landed net, each commit and what it removed, the maintainer's
list with the answers, and the rejected list with its conditions.
