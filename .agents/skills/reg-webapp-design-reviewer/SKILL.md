---
name: reg-webapp-design-reviewer
description: >-
  Registry Research Toolkit `reg_webapp` design review skill. Use to judge a frontend
  candidate against the design language in `reg_webapp/frontend/DESIGN.md`: a source
  review any reader of the diff can perform (token discipline, primitive reuse, ARIA and
  focus, copy, responsive rules, error/empty affordances) and, with a checkout, a
  four-width rendered review. This is the post-implementation counterpart to
  `reg-webapp-frontend-design`.
---

# Reg webapp design reviewer

Judge a `reg_webapp` frontend candidate against the committed design language. The skill
has two parts and they need different inputs:

- **Source review** — decidable from the diff plus `reg_webapp/frontend/DESIGN.md`.
- **Rendered review** — needs a checkout, a browser and screenshots.

Run the part your inputs support and say in the report which one you ran. Authoring
happens before this skill, under `reg-webapp-frontend-design`.

## Who runs this

An implementer self-checks in a clean subagent (prompt: goal, changed routes, diff, this
skill) that reports and does not rewrite; the implementer fixes or dismisses each
finding. A source-only reviewer reads only the diff, this skill and
`frontend/DESIGN.md`, runs the source review, and never claims or demands rendered
evidence. Neither substitutes for the maintainer rendering the changed surfaces before
approval.

## The contract

`reg_webapp/frontend/DESIGN.md` is normative for how the app looks: its front matter is
the token set, its prose says why the values exist and how to apply them. Judge the diff
against that file, not against the code around it: it outranks the older styling notes
in the sibling `reg_webapp/DESIGN.md`, and all source — new, reworked and untouched — is
held to it. A breach of its "Colors", "Typography", "Components" or "Do's and Don'ts"
rules is a finding; the checks below are where the yield is.

## Source review

### Token discipline

Components consume semantic roles only (the DESIGN.md front matter, implemented in
`src/tokens.css`). The `lint:tokens` script already fails a candidate on raw color
literals and font stacks inside a `<style>` block, so the yield here is what it cannot
see: a ramp stop read through `var()`, a one-off px spacing/radius/shadow value, and the
wrong role chosen for the job. The tree carries roles beyond the front matter (the label
roles `--micro-label-size` and `--micro-label-weight` among them), so an unfamiliar
`--role` name is not itself a finding; a literal or a ramp stop is.

The four color sub-systems are disjoint; borrowing across them is a finding. So is
accent used as status or emphasis, accent-colored *text* on a tint that takes `--accent`
instead of `--accent-ink`, status carried by hue alone, a `-bg` tint used as text, and
more than one `primary` button per view.

### Primitive reuse

Changed views compose the shared primitives (DESIGN.md → "Components"). A hand-rolled
near-duplicate of one — a bespoke table, button, tag, empty state or key-value list,
visible in the diff as new markup with its own scoped CSS — is at least a `P2`.

### Behavior layer

Accessibility-critical widgets (combobox, command/listbox, menu, dialog, popover,
tooltip, slider, tabs, accordion) come from Bits UI (`bits-ui`), styled with scoped CSS
reading roles. A hand-rolled widget Bits UI covers is at least a `P2`; so is a Bits UI
usage that ships one-off visual styling instead of roles.

### ARIA and focus semantics

- Focus follows DESIGN.md: `:focus-visible` in the component's **own** scoped CSS; no
  global stylesheet owns focus.
- Every interactive element is reachable and operable from the keyboard — a click-only
  handler on a non-interactive element is a finding.
- `DataTable`'s settled decisions are not re-litigated: explicit unconditional ARIA
  roles; ARIA-grid selection (per-row tab stops, not roving tabindex); `framed` tables
  never wrapped in a `Panel`; the first column primary by position; `mono`/`numeric`
  column flags for identifiers and measures.
- Screen-reader-only content uses the `.visually-hidden` utility, never `display: none`;
  `display: contents` strips roles from the accessibility tree.

### Copy and labels

Case, mono and naming follow DESIGN.md → "Typography" and "Do's and Don'ts". Check that
an action keeps its name through the whole flow ("Add to project", not "Submit").

### Responsive rules

The widths and breakpoint are in DESIGN.md → "Layout". Source-visible violations: a new
breakpoint; a fixed px width that cannot fit 375 px; a padded 100 %-width element
without `box-sizing: border-box`; an identifier column with no wrap or scroll rule; a
bespoke media-query stack where `DataTable`'s stacked-card behavior exists. Whether a
page actually overflows is a rendered question — the CSS rule is a source one.

### Error, empty and loading affordances

A fetching surface in the diff carries three states in source: `Skeleton` with
`aria-busy` while loading, `EmptyState` naming the next action when empty, and an error
banner (status tint fill, status foreground text, leading glyph). A new view that
renders only the happy path is at least a `P2`.

## Rendered review

Needs a checkout and a browser — the implementer's self-check and the maintainer. Skip
this part entirely if you have neither.

### Render

From the repo root. `dev.sh` picks free ports, runs the Playwright driver against them,
and tears both servers down on exit:

```sh
bash reg_webapp/dev/dev.sh --fixture-db shot --all <route>
REG_META_DB="$db_dir" bash reg_webapp/dev/dev.sh shot --all <route>
bash reg_webapp/dev/dev.sh --fixture-db smoke
```

`--all` captures the four DESIGN.md widths and it applies to every rendered change;
there is no desktop-only exception. `--fixture-db` builds a deterministic synthetic
catalog, so no released catalog DB is needed — `dev.sh` installs the venv and the
frontend dependencies itself; the remaining prerequisites (the wasm toolchain,
Playwright's Chromium) are in `reg_webapp/.claude/skills/run-reg-webapp/SKILL.md`. Pass
`REG_META_DB=<db_dir>` instead when the rendered behavior depends on specific catalog
content. `smoke` is a catalog-browsing flow at the default desktop viewport only — it
does not stand in for the four widths.

`dev.sh` prints the checkout it resolved, the full HEAD, and the unique output directory
of that invocation as its first line:

```
dev: repo /path/to/checkout HEAD 0123456789abcdef… shots /tmp/reg-webapp-shots.C8VNZN
```

Quote it. Every invocation gets its own directory and `/tmp` is purged, so a path
without a description of what the image showed is not evidence — and screenshots are
never committed to the branch.

If no route is given, derive the smallest stable route from the changed files and nearby
tests. Common routes: `/`, `/catalog`, `/catalog/<fqid>`,
`/catalog/group/<provider>/<register>/<key>`, `/catalog/group/class/<key>`,
`/search?q=...`, `/project`, `/doc/<identifier>`.

### Inspect

**Open the images.** Claim inspection only of images you actually opened, at the
viewport each was captured at. Read the DOM/accessibility snapshot and console output
alongside them where the tooling offers those.

- Render health: no blank screens, no stuck `aria-busy="true"`, no JS errors, no missing
  assets, no capture taken before content settled.
- Layout at each width: no horizontal page overflow, overlap, clipping, unstable
  wrapping or clipped table/card content.
- Density: the app reads as a dense research tool — no hero sections, decorative
  gradients or orbs, oversized cards, cards inside cards, or stock imagery.
- Accessibility visuals: visible focus rings, contrast, status meaning carried by glyph
  and text rather than hue.
- Cross-route coherence: render at least one untouched sibling route beside the changed
  one and compare type scale, spacing rhythm and table/card/tag treatment. Flag
  divergence from the surrounding app.

`bun run lint/check/build` render nothing. The browser component tests (`bun run test`)
do render in Chromium, but they exercise primitives in isolation: they are not app-level
route and state coverage, and they are not visual inspection.

### When rendering is unavailable

If the app will not boot, the driver fails, or the environment has no browser, the
outcome is **blocked** — not a product finding. Report the exact command, the observed
cause (quote the failing line; the driver names the Chromium launch rung it reached,
e.g. `driver: chromium launched (no-sandbox)`), and what stayed unverified. A blocked
rendered review also cannot support a clean visual approval. Raise a product `P1` only
where the evidence in hand establishes a candidate defect with that user impact.

### Author iteration vs. gate evidence

Fill every field of the rendered template below, and report the routes, states and
viewports you actually inspected — not the ones the command could have produced. Author
iteration captures and retained candidate gate evidence are different things: say which
you are reporting. The flow gates cover only their named scenarios — `project-flows` the
`/project` error and retry states, `catalog-flows` the one catalog leaf and `/project`
state of the catalog-authored draft flow — so neither is evidence for a search, a
different catalog view or a new one. Approval needs evidence for the surfaces the
candidate actually changes.

## Severity, findings and fixes

`P1` blocks, `P2` is a meaningful UX or contract defect, `P3` is polish. Report
findings; do not silently rewrite broad UI. If you are responsible for fixes — the
implementer acting on its subagent's report — make the smallest source change, follow
the existing primitives and roles, then re-render the affected route and update the
report.

Dismiss a finding only with a concrete reason: "existing behavior outside this
candidate", "intended clipping", "route not touched by this diff".

## Report format

Source-only — a source-only reviewer, or anyone without a checkout:

```markdown
# Reg webapp design review — source

- Candidate: <change, branch, or diff reviewed>
- Basis: diff + reg_webapp/frontend/DESIGN.md. Not rendered; no visual claim.
- Result: <no source findings / findings>

## Findings

### [P1|P2|P3] <title>

- File and line: <path:line>
- Rule: <the DESIGN.md rule or checklist item>
- Issue: <what the source establishes>
- Recommendation: <smallest correction>
```

Rendered — the implementer's self-check, the maintainer:

```markdown
# Reg webapp design review — rendered

- Candidate: <change, branch, full HEAD>
- Checkout: <path dev.sh resolved>
- Gate run: <gate.py step and result, or n/a>
- Command: <exact command>
- Routes and states inspected: <routes/states actually opened>
- Viewports inspected: <widths actually opened>
- Output directory: <dir dev.sh printed>; retained: <artifact paths, or none>
- Uncovered changed surfaces: <surfaces, or none>
- Result: <pass / findings fixed / findings dismissed / blocked: cause>

## Findings

### [P1|P2|P3] <title>

- Route and viewport: <route @ width>
- Element: <selector or description>
- Issue: <what is visibly wrong, described so it stands without the image>
- Recommendation: <smallest practical fix>
- Status: <fixed / dismissed with reason / needs owner>
```
