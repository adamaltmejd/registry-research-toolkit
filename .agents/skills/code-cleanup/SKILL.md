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

AGENTS.md says the tree stays lean ("Reuse first, build last", "Maturity and
compatibility"); this file is the recipe. Run it before a release and whenever the tree
feels fat. Preconditions: main is green and no other session is landing in the same
package. The pass reads the whole tree, not a diff.

## 0. Measure

Take the numbers before anything moves, and again at the end. The Rust runtime and
builder release as `reg_meta` from `crates/`:

```sh
last=$(git describe --tags --abbrev=0 --match 'reg_meta/v*' 2>/dev/null ||
  git rev-list --max-parents=0 HEAD)  # no reachable tag: count from the root
git log --oneline "$last"..HEAD -- crates | wc -l
git ls-files 'crates/*.rs' | xargs wc -l | tail -1
git ls-files 'scripts/*.sh' 'reg_webapp/dev/*.sh' | xargs wc -l | tail -1
```

Compare each suite's wall time with its budget in ARCHITECTURE.md → "Tiers". Rust:
`cargo test --workspace`; its dependency check traces each direct dependency in a
crate's `Cargo.toml` to a use in that crate's sources, tests, examples or `build.rs`
(`rg`).

## 1. Mechanical checks

These answer without judgment. Run them first, by one subagent on the tier below the
family's mid tier (in Claude, Sonnet); the work is lookup.

- **Dependencies.** Rust: `cargo machete` (it also runs in `gate.sh crates`). The SPA:
  `reg_webapp/frontend/package.json` against its imports, scripts, build configuration
  and CI. A dependency declared and not used, or used and not declared, is a finding.
- **Dead code.** `cargo machete` and the compiler's warnings for Rust; `rg` each export
  of `reg_webapp/frontend/src` for a use. Confirm each hit with `rg` across the
  workspace before calling it dead.
- **Trace tables**, kept in the scratchpad: every CLI flag and subcommand (the Rust
  `parse_args` of `reg-build`, `reg-meta`, `reg-export-gate` and `xtask`; the steps of
  `scripts/gate.sh`), to its README or DESIGN.md line, to a test; every `openapi.json`
  path, to a conformance case; every curation-TOML key the loader accepts, to a
  committed use. A gap is a finding: code without a stated behavior is YAGNI, and a
  stated behavior without a test is either a gap or a line to cut.
- **Deferrals.** `rg -n -i 'TODO|FIXME|XXX|revisit|simplify:'`. A `simplify:` whose
  named upgrade trigger has fired, or that names none, is a finding. So is a root
  tracker past its completion gate.

## 2. Read, one agent per module, in parallel

Do not read the modules yourself. Launch one subagent per module through the harness's
subagent tool (in Claude Code, the Agent tool with the `general-purpose` type; Explore
locates code and does not audit it), told to edit nothing. Launch as many at once as the
harness allows and the rest in waves. Where the harness lets a call choose its model,
set it on every call: the family's mid tier (in Claude, Opus) for code, the tier below
for docs. Every tracked non-test source file (`git ls-files`, minus tests and committed
data) belongs to exactly one module; a file the split below misses joins its nearest
module. Today's split: `reg_webapp/edge` with the Dockerfile and deploy scripts;
`reg_webapp/frontend` (the SPA) with `reg_webapp/dev`; `scripts/`; the `conformance/`
harness; `plugins/microdata-tools-se/`; the R packages `reg_mona_runner` and
`reg_export_gate`; `curation/` (TOML, read for stale keys and entries); and `crates/`
(Rust). The test trees are the `test-audit` skill's: run its sweep in the same waves and
merge its list into step 3.

Each prompt carries the module's paths, an instruction to read AGENTS.md,
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
- `dep:` a hand-rolled thing an installed dependency already does. Name the feature.
- `dup:` the same leaf in two modules, the failure mode AGENTS.md names. Name the home
  it belongs in (`reg-core`, `reg-frame`, …).
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
  and it names what it removes. Test-audit deletions land through that skill's "Judge
  and land" step, not this criterion.
- **Maintainer's.** Every `guard:` finding, and anything mistagged that should have been
  one. List each with the DESIGN.md line it would change and the reason, and stop there
  until answered. A yes lands with the DESIGN.md edit in the same commit.
- **Rejected.** Everything else, each with the condition that would re-admit it, so the
  next pass does not raise it again.

## 4. Land

Edit directly, in an isolated worktree. One commit per module, the message naming what
was removed. Before each push, run the gate steps CI runs for the touched module
(`bash scripts/gate.sh <step>`): `lint` always, `g0` for `scripts/` and `crates/` (it
includes `crates`), `rust` for `conformance/` and the Rust server, and `frontend` plus
`flows` for `reg_webapp/frontend` and `reg_webapp/dev`. The R packages are not gated:
run their testthat suites by hand. For the edge worker, run
`bunx wrangler@<pin> deploy --dry-run --config reg_webapp/edge/wrangler.jsonc`, with the
wrangler pin `.github/workflows/container-build.yml` deploys with. A change that
redesigns a module rather than deleting from it, or needs more than one sitting, becomes
a GitHub issue instead (search open and closed first).

## 5. Report

Repeat step 0. The PR body carries the numbers table (before, after), the possible net
from step 2 beside the landed net, each commit and what it removed, the maintainer's
list with the answers, and the rejected list with its conditions.
