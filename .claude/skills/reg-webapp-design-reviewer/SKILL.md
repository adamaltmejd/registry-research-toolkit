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

Three consumers, one contract.

- **The implementation worker's in-lane self-check.** A worker whose ticket changes
  rendered UI runs both parts before declaring its candidate — in a clean subagent whose
  prompt is the ticket and attempt, the changed routes, the diff and this skill, so the
  judgment does not inherit the implementing session's rationalizations. The worker then
  fixes or explicitly dismisses every finding; the subagent reports, it does not
  rewrite.
- **The design review seat.** Its inputs are the candidate diff and two context files:
  this skill and `reg_webapp/frontend/DESIGN.md`. It has no checkout, no browser and no
  screenshots. It runs **the source review only** — it does not read further files, does
  not render, never claims rendered verification, and never raises a finding merely
  because rendered evidence is absent. Its report uses the source-only format below.
- **The operator.** Renders the surfaces the candidate changes and judges them before
  approval. Neither a worker self-check nor a source review substitutes for that.

## The contract

`reg_webapp/frontend/DESIGN.md` is normative for how the app looks: its front matter is
the token set, its prose says why the values exist and how to apply them. Judge the diff
against that file, not against the code around it. The app is mid-transition to the Ink
palette, so an untouched neighbour still carrying the old warm accent, `--rost-*` ramp
stops or tracked-uppercase eyebrows is pending the restyle ticket (Y-37), not a defect
this candidate introduced. New and reworked source is held to DESIGN.md.

## Source review

### Token discipline

Components consume **semantic roles only** — never primitive ramp stops (`--gray-*`,
`--rost-*`, a raw status or categorical hue) and never a literal, which can render
identically today and still break the role contract that makes a dark or per-provider
theme a pure remap. The `style_tokens` test already fails a candidate on raw color
literals and font stacks inside a `<style>` block, so the yield here is what it cannot
see: a ramp stop read through `var()`, a one-off px spacing/radius/shadow value, and the
wrong role chosen for the job.

The full role set, as the custom properties the source reads:

- Surfaces and ink — `--bg`, `--surface`, `--surface-raised`, `--surface-sunken`,
  `--surface-hover`, `--surface-selected`, `--text`, `--text-muted`, `--text-faint`,
  `--border`, `--border-strong`, `--scrim`, `--elevation-raised`, `--focus-ring`.
- Chrome — `--accent`, `--accent-fg`, `--accent-bg`, `--accent-ink`.
- Status — `--err`, `--warn`, `--info`, `--ok` and their `-bg` fill tints.
- Categorical node type — `--cat-reg|var|code|class|group` and their `-ink` label stops.
- Data encodings — `--facet-axis-0..5` (and `-ink`), `--viz-edge-succession`.
- Geometry and type — `--space-1..4`, `--radius-sm`, `--radius`, `--font-ui`,
  `--font-mono`, `--text-display|h1|h2|h3|body|sm|micro`, `--motion-fast`.

The four color sub-systems are disjoint; borrowing across them is a finding.

- The accent is ink and paints interactive chrome only — links, primary buttons,
  selection, focus ring, active nav. It is never a status or emphasis color, and
  accent-colored *text* on a tint takes `--accent-ink`, not `--accent`. At most one
  `primary` button per view.
- Status meaning always travels with a glyph **and** text; hue alone is never the
  carrier, and a `-bg` tint is never a text color.
- Categorical type tags what a node *is*; it never doubles as status or selection.
- Facet axes and viz edges are data — not chrome, not status.

### Primitive reuse

Changed views compose the shared primitives — `Panel`, `DataTable`, `Breadcrumbs`,
`Tag`, `Button`, `KeyValue`, `Skeleton`, `EmptyState`, plus `AppShell`. A hand-rolled
near-duplicate of one — a bespoke table, button, tag, empty state or key-value list,
visible in the diff as new markup with its own scoped CSS — is at least a `P2`.

### Behavior layer

Accessibility-critical widgets (combobox, command/listbox, menu, dialog, popover,
tooltip, slider, tabs, accordion) come from Bits UI (`bits-ui`), styled with scoped CSS
reading roles. A hand-rolled widget Bits UI covers is at least a `P2`; so is a Bits UI
usage that ships one-off visual styling instead of roles.

### ARIA and focus semantics

- Focus is visible via `:focus-visible { box-shadow: var(--focus-ring); }` in the
  component's **own** scoped CSS; no global stylesheet owns focus.
- Every interactive element is reachable and operable from the keyboard — a click-only
  handler on a non-interactive element is a finding.
- `DataTable`'s settled decisions are not re-litigated: explicit unconditional ARIA
  roles; ARIA-grid selection (per-row tab stops, not roving tabindex); `framed` tables
  never wrapped in a `Panel`; the first column primary by position; `mono`/`numeric`
  column flags for identifiers and measures.
- Screen-reader-only content uses the `.visually-hidden` utility, never `display: none`;
  `display: contents` strips roles from the accessibility tree.

### Copy and labels

Sentence case at normal tracking for labels, headings and buttons — no tracked-uppercase
eyebrows, numbered section markers, middle-dot meta strings or trailing arrows. Mono is
for machine identifiers only (FQIDs, slugs, value-set codes, years, counts, versions),
never for labels or body copy. Name things by what a researcher recognizes — registers,
variables, value sets, projects — never by internals, and keep an action's name the same
through its whole flow ("Add to project", not "Submit"). Errors say what went wrong and
how to fix it, without apologising.

### Responsive rules

The contract is 375, 768, 1280 and 1920 px, composed from the existing 48 rem
breakpoint. Source-visible violations: a new breakpoint; a fixed px width that cannot
fit 375 px; a padded 100 %-width element without `box-sizing: border-box`; an identifier
column with no wrap or scroll rule; a bespoke media-query stack where `DataTable`'s
stacked-card behavior exists. Whether a page actually overflows is a rendered question —
the CSS rule is a source one.

### Error, empty and loading affordances

A fetching surface in the diff carries three states in source: `Skeleton` with
`aria-busy` while loading, `EmptyState` naming the next action when empty, and an error
banner (status tint fill, status foreground text, leading glyph). A new view that
renders only the happy path is at least a `P2`.

## Rendered review

Needs a checkout and a browser — the worker's self-check and the operator. Skip this
part entirely if you have neither.

### Render

From the repo root. `dev.sh` picks free ports, runs the Playwright driver against them,
and tears both servers down on exit:

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db shot --all <route>
REG_META_DB="$db_dir" bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot --all <route>
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db smoke
```

`--all` is the four-width contract (375 / 768 / 1280 / 1920); use it unless the route is
demonstrably desktop-only. `--fixture-db` builds a deterministic synthetic catalog and
needs nothing installed; pass `REG_META_DB=<db_dir>` instead when the rendered behavior
depends on specific catalog content. `smoke` is a catalog-browsing flow at the default
desktop viewport only — it does not stand in for the four widths.

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
  divergence from the surrounding app, allowing for views the restyle ticket has not
  reached.

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
you are reporting. The `project-flows` gate covers its named `/project` error and retry
scenarios — it is not evidence for a catalog, search or new-view change. Operator
approval needs evidence for the surfaces the candidate actually changes.

## Severity, findings and fixes

`P1` blocks, `P2` is a meaningful UX or contract defect, `P3` is polish. Report
findings; do not silently rewrite broad UI. If you are responsible for fixes — the
implementing worker acting on its subagent's report — make the smallest source change,
follow the existing primitives and roles, then re-render the affected route and update
the report.

Dismiss a finding only with a concrete reason: "existing behavior outside this
candidate", "intended clipping", "route not touched by this diff", "pending the restyle
ticket".

## Report format

Source-only — the design seat, or anyone without a checkout:

```markdown
# Reg webapp design review — source

- Candidate: <ticket, branch, or diff reviewed>
- Basis: diff + reg_webapp/frontend/DESIGN.md. Not rendered; no visual claim.
- Result: <no source findings / findings>

## Findings

### [P1|P2|P3] <title>

- File and line: <path:line>
- Rule: <the DESIGN.md rule or checklist item>
- Issue: <what the source establishes>
- Recommendation: <smallest correction>
```

Rendered — the worker's self-check, the operator:

```markdown
# Reg webapp design review — rendered

- Candidate: <ticket/attempt, branch, full HEAD>
- Checkout: <path dev.sh resolved>
- Gate execution: <gate and execution, or n/a>
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
