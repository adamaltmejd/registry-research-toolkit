---
name: curate
description: >-
  Registry Research Toolkit catalog curation workflow: curated variable and group
  descriptions and identity fixes (splits, merges, occurrence corrections, group
  membership) after new data, a new register or a curation sweep. Use for any pass that
  writes `[[statement]]` entries in `curation/registers/`, alone or as parallel lanes with
  automated review. Not for real-seed builds (build-db) or releases (release).
---

# Curate

Curation writes `[[statement]]` entries in `curation/registers/<provider>/<register>/curation.toml`
and refreshes `curation.lock`. The object model (statements, `set variable`, candidate
splits, `continues`, sets, the lock) is in `crates/DESIGN.md`; read its curation sections
before writing. This skill is the workflow and the pitfalls. Real-seed builds and
diffs are [build-db](../build-db/SKILL.md); releases are [release](../release/SKILL.md).

## Authority

Every ruling cited in a statement's evidence or a PR body must exist: a maintainer
ruling (date, issue or PR) or `crates/DESIGN.md`. A pass run without maintainer review
needs a written delegation, cited in every PR. Without one, stop at the review gate and
ask.

## Worklist and partition

1. Build the worklist from the catalog and the findings file
   (`reg-build build --findings FILE`, see build-db): variables whose used occurrences
   state several provider wordings (`description-default` leaves a partial fold or a
   real difference uncurated), plus F6 source conflicts and continuation proposals.
2. Exclude variables that already have a curated description, and group members when
   groups are curated separately (a group page would show a default on some members only).
3. Partition by register. One lane owns whole registers; no two lanes share one. Shared
   registers caused repeated rebases and relocks.

## Sources

- Read every raw documentation row of a column and year. The reader keeps only the
  first row per column-year (#2431), yet the provider's export can document one column
  several times a year (one row per form or scheme, differing in the operational
  definition). Catalog wordings can therefore be incomplete. Never split or open on
  first-row differences alone.
- Provider exports contain errors: a text recycled from another column's earlier year,
  paired columns swapped, a text copied between variables (a gain text on a loss
  column). Check the raw export first.
  - Export right, catalog wrong: a reader or placement bug. Stop, report it, fix it once
    in code.
  - Export wrong: correct at occurrence level (`set definition` or
    `operational_definition` on the occurrence) only when the column's own text exists
    elsewhere in the sources and matches. Otherwise leave the text out of the
    description and list it as a finding.
- Boilerplate shared by many columns (a definition reused by dozens, "uppgifterna kan
  vara imputerade") is left out, not generalised.
- A public source is allowed only when it is located and cited in the evidence.

## Descriptions

- One curated description per variable: what it means and how it is measured. Differing
  texts are drift to reconcile or an identity problem.
- Describe the variable, never its documentation: no "källan anger", "Se vidare",
  "Jämför", no narration of years. Give years only for a real change in measurement
  (question wording, population, classification version), stated as a fact with its years.
- The dominant defect is a fact true for some years, forms or members written as
  general. Never extend a fact beyond the years or members whose text states it.
- Rewording to fix errors, typos and awkward phrasing is allowed; say only what the
  provider texts or a located source support.
- Swedish, plain, in the style of the existing curated descriptions.
- Evidence form: the ruling line, a one-line reason, then one
  `catalog:variable_text:<variable>:<start>:<end>` line per run that carries a wording.
- A group (set) description says what one measure is and along which axes its members
  vary. It restates no member's text.

## Identity

- Split (`set variable` on the selected occurrences) only when the raw rows show
  materially different things: sector, scheme, unit, population, code meaning. Over
  time use `continues` with an evidenced `at`; parallel tables with different meanings
  are a candidate split without a succession edge.
- Do not split on a lone contradicting row (open it), on first-row differences, or on a
  historical recoding within one column.
- A group is one measure along axes. Unit, a component beside its sum, whose value
  (child, parent, spouse), sub-population, classification version and level of
  aggregation (person or employment, company, workplace, municipality, household) are
  slots of one group, not reasons to leave a variable out. A split by level leaves each
  level in the group.
- After any membership change check group sizes. A group left with one member fails the
  `--release` build (`set-too-small`); `reg-build lock` does not catch it.
- New ledger ids follow the conventions in the register's `ids.toml`.
- Before splitting, grep the stewards' `curation/` and `ids.toml` for the variable's
  pid. A steward that holds it re-locks at its bump; list it in the PR. Never write
  private steward specifics into the repository or its PRs.
- Earlier curation that is wrong (a bad merge, a wrong label, a stale description) is
  fixed with evidence and listed in the PR.
- Identity commits (`ids.toml`, `set variable`, occurrence corrections) are separate from
  description-only commits.
- Unsure after checking the sources: leave the item open with a located reason. Never guess.

## Review gate

Run all of it before the PR is ready.

1. Mechanical check, scripted and run on every batch: each subject is on the worklist or
   is a split half you created; not a group member; no earlier curated description;
   exactly one description statement; every cited `variable_text` id exists in the
   catalog; no documentation-narration phrases; Swedish.
2. Fresh-agent audit. A subagent that did not write the text gets only the provider
   wordings per year (every raw row) and the written description, not your reasons.
   Per item: which claims the wordings do not support; what important content is
   missing; whether the wordings describe one thing or several (a missed split).
   - Coverage: every identity change, occurrence correction and item that differed, and
     a random 20% of the rest.
   - If more than 20% of audited items have a defect in the first round, audit every
     item. Re-audit fixed items until a round finds nothing actionable. Report each
     round's rate and "audited N/N".
   - Convergence: if findings do not stop after two or three re-audits, make the item
     open instead of rewording again, or restrict later rounds to actionable defects
     (unsupported claim, wrong Swedish, missing main axis).
3. `codex -C <worktree> review --base origin/main`. It rarely finds anything in
   curation TOML; it checks the ledger and lock.
4. Lock. `reg-build lock <registers…> --inputs <inputs> --assets <assets> --curation curation`
   reads the whole corpus, so run one at a time (a mutex directory taken with `mkdir`
   and released by `trap`). Batch locks: one at the end of a batch. Commit the refreshed
   locks. Confirm no diagnostic in the new blocks and that no variable lacks a ledger id.
5. Before merge: a `--release` build (build-db), or at least a group-size check of
   every touched set. CI green and rebased on `origin/main`.

Never run a real-seed `build` as a check inside a lane, and never `pkill` or `killall`.

## Reporting

- PR body: scope; rulings cited; counts (written, split, identity fix, occurrence
  correction, open, group-skipped); identity-change table (register, change, new ids);
  open list with located reasons; operator calls (each judgment call with its reason);
  findings left for review (export errors not corrected); steward impact; checks run and
  not run; audit rates before fixes.
- Keep a running rulings log during the pass. A rule found mid-pass is written down at
  once in the pass's issue. A durable product rule about the catalog model goes into
  `crates/DESIGN.md` through a reviewed PR, not into the skill or the lane's TOML.
- Commits: `curation(reg_meta): …`, one PR per lane, labels `reg_meta` and `enhancement`.
  Keep private steward metadata out of anything on GitHub.

## Parallel lanes

- A brief gives one register set, the rulings, and this skill. Lanes do not share registers.
- Say in the brief: finish all steps without stopping after listing the remaining ones.
- Unattended lanes ask no questions. They decide by the rulings and DESIGN.md, record
  each call under "Operator calls", and when truly blocked post a `BLOCKED:` PR comment
  and stop.
- A lane ends with a PR comment starting `READY FOR OPERATOR MERGE`: head sha, counts,
  the first-round audit rate, Codex result and lock result. The Operator merges.
