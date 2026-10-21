---
name: test-audit
description: >-
  Registry Research Toolkit test audit. Use to gate a change that adds or changes a
  test, and to sweep a package's test tree for assertion blocks that fail the Testing
  policy, deleting by default. Load when a change touches a test, or when asked to
  audit, prune or review tests.
---

# Test audit

AGENTS.md → "Testing policy" is the policy. ARCHITECTURE.md → "Testing strategy" lists
the boundaries ("Boundaries and oracles"), their oracles and each package's time budget
("Tiers"). This file is what to do with them at two moments. The unit is the assertion
block: each block is a claim about one boundary.

## At review: the change touches a test

Four answers, from the diff and the PR body, each checked against its policy rule. A
missing one sends the change back.

1. Which boundary (from the table) and which stated behavior.
2. Which change to the product makes it fail, named in the test's comment.
3. Why the boundary's existing cases do not already catch that failure, and whether this
   is the behavior's hardest case.
4. Where each expected value comes from.

Do not run the sabotage by hand. Review the named change as the failure proof.

## The sweep

Read-only. One subagent per test tree (each package's, plus `conformance/` and
`scripts/tests/`), through the harness's subagent tool (in Claude Code, the Agent tool
with the `general-purpose` type; Explore locates code and does not audit it), told to
edit nothing. Launch as many at once as the harness allows (Codex runs three children at
a time) and the rest in waves. Where the harness lets a call choose its model, use the
family's mid tier (in Claude, Opus). Each prompt carries AGENTS.md, the "Testing
strategy" section, the tree's conftest and helpers, a scratchpad file name unique to the
tree, and this section verbatim. The frontend gets two agents, browser tests and the
rest.

A tree too large to read whole gets file-level triage instead. Per file: `keep` (already
at a boundary), `delete` (name the boundary case that covers it), or `gap` (no corpus
exists yet; name the corpus to build).

One line per block that fails the bar, no hedging:

`<file>:L<a>-<b>: <tag> <what>. <disposition>. [boundary: <name>]`

- `taut:` expected value from the code under test's own output or a copy of its logic,
  including a golden regenerated without review.
- `easy:` the behavior's simplest case. Name the harder one.
- `unrelated:` a refusal that passes for another reason: a different error code, another
  guard, a missing fixture.
- `twin:` the same contract asserted in another block. Name the survivor, preferring the
  conformance case or golden.
- `detector:` wording, layout, CSS, incidental ordering or a count that is not the
  stated behavior.
- `promise:` the name or docstring claims more than the scenario exercises.
- `seam:` asserts the implementation's shape instead of a boundary's output: a private
  function, an object's internals, a call, a SQL string, a mock of an internal module, a
  component's props or state. Also a helper or product hook that only this block needs
  and that exposes internals; a readable fixture dedicated to one boundary case is not a
  seam.

Disposition is `delete`, or `rewrite` (name the corpus) when the block is the only proof
of a stated behavior. Never `delete` the only proof of a load-bearing guard (AGENTS.md →
"Reuse first, build last"); mark it `rewrite`. Report the tree's test count and slowest
files, then end with `<N> blocks, <M> to delete.` or `Lean already.`

## Judge and land

Deleting the only proof of a behavior is the maintainer's call: the behavior leaves the
package's DESIGN.md, or the block is rewritten into a corpus. Everything else: delete
the block and the helpers it orphans, one commit per test file or package. A kept block
that fails on main is a product bug: reproduce it at the boundary and file it (search
first; read through `scripts/gh_issue.py`), never delete it.

Before pushing, always `uv run python -m pytest <tree> -n auto -q`, where `<tree>` is
the touched Python package or `scripts/tests`, and conformance, which runs against the
Rust server (`uv run --no-project scripts/gate.py rust`), plus the tree's own gate step
(`uv run --no-project scripts/gate.py <step>`): `frontend` for the frontend, `crates`
for `crates/`. Then compare the wall time with the package budget. Report in the PR
body, or in the code-cleanup report when run inside that pass: each disposition with its
boundary, test lines removed beside product lines, suite time before and after.
