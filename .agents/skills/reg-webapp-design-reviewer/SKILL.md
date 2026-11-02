---
name: reg-webapp-design-reviewer
description: >-
  Registry Research Toolkit `reg_webapp` design review skill. Use to judge a frontend
  candidate against the design language in `reg_webapp/frontend/DESIGN.md`: a source
  review any reader of the diff can perform (token discipline, primitive reuse, React
  and React Aria correctness, ARIA and focus, copy, responsive rules, error/empty
  affordances) and, with a checkout, a four-width rendered review. This is the
  post-implementation counterpart to `reg-webapp-frontend-design`.
---

# Reg webapp design reviewer

Judge a `reg_webapp` frontend candidate against the committed design language. The skill
has two parts and they need different inputs:

- **Source review** — decidable from the diff plus `reg_webapp/frontend/DESIGN.md` and
  the rule sections of `reg-webapp-frontend-design`.
- **Rendered review** — needs a checkout, a browser and screenshots.

Run the part your inputs support and say in the report which one you ran. Authoring
happens before this skill, under `reg-webapp-frontend-design`.

## Who runs this

An implementer self-checks in a clean subagent (prompt: goal, changed routes, diff, this
skill) that reports and does not rewrite; the implementer fixes or dismisses each
finding. A source-only reviewer reads only the diff, this skill, `frontend/DESIGN.md`
and `reg-webapp-frontend-design`, runs the source review, and never claims or demands
rendered evidence. Neither substitutes for the maintainer rendering the changed surfaces
before approval.

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
literals and font stacks in component styles, so the yield here is what it cannot see: a
literal in a `style={{}}` object it missed, a ramp stop read through `var()`, a one-off
px spacing/radius/shadow value, a CSS Module redeclaring a role, and the wrong role
chosen for the job. The tree carries roles beyond the front matter (the label roles
`--micro-label-size` and `--micro-label-weight` among them), so an unfamiliar `--role`
name is not itself a finding; a literal or a ramp stop is.

The four color sub-systems are disjoint; borrowing across them is a finding. So is
accent used as status or emphasis, accent-colored *text* on a tint that takes `--accent`
instead of `--accent-ink`, status carried by hue alone, a `-bg` tint used as text, and
more than one `primary` button per view.

### Primitive reuse

Changed views compose the shared primitives (DESIGN.md → "Components"). A hand-rolled
near-duplicate of one — a bespoke table, button, tag, empty state or key-value list,
visible in the diff as new markup with its own CSS Module — is at least a `P2`. A
verdict, failed load or warning drawn as a hand-styled coloured line instead of a
`ui/StatusRow`, or a classed anchor that does not take the shared `.link` class, is a
`P2`.

### Behavior layer and React correctness

Accessibility-critical widgets come from React Aria Components (RAC), styled by the
component's CSS Module reading roles. The rules are stated once, in
`reg-webapp-frontend-design` → "Behavior layer", "Swedish text", "Tree and sticky-list
gotchas" and "React 19 rules"; a breach visible in the diff is a finding:

- `P2`: a hand-rolled widget RAC covers; RAC styled with one-off values or a global
  `.react-aria-*` selector; any breach of the collection, `dependencies`,
  controlled-state, filtering, labelling, pinning, Swedish-text or Tree rules; an effect
  that derives state; an effect without cleanup or not idempotent under StrictMode; an
  ad-hoc fetch in an effect, or one not aborted on unmount or deps change, instead of
  the shared async-resource hook; a component defined inside another; a new state
  library. `P1` where it causes user-visible breakage.
- `P3`: a native `<button>`/`<a>` with `onClick` where RAC `Button`/`Link` fits (`P2` if
  keyboard operation breaks); `useMemo`/`useCallback` with no measured reason;
  `forwardRef` or `.Provider` in new code.

A new component test (a render of a component asserting its markup, classes, props or
internal state) is a `P2` defect: behavior is pinned only through the `flows` driver and
pure-TS unit tests. Rules of hooks, the compiler-backed rules and jsx-a11y are Oxlint
rules: don't re-report what lint fails.

### ARIA and focus semantics

- Focus follows DESIGN.md: `[data-focus-visible]` on RAC parts or `:focus-visible` on
  plain elements, with `var(--focus-ring)`, in the component's **own** CSS Module; no
  global stylesheet owns focus. Inside a scroll container with sticky rows the ring is
  inset.
- An `aria-label` on a part RAC already labels (the tree chevron) duplicates its name;
  `text-transform` on a labelling element changes the accessible name.
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

From the repo root; `dev.sh` picks free ports and tears everything down on exit:

```sh
bash reg_webapp/dev/dev.sh --fixture-db shot --all <route>
REG_META_DB="$db_dir" bash reg_webapp/dev/dev.sh shot --all <route>
bash reg_webapp/dev/dev.sh --fixture-db smoke
```

`--all` captures the four DESIGN.md widths for every rendered change; there is no
desktop-only exception, and `smoke` (desktop only) does not stand in for them. The
[run-reg-webapp skill](../run-reg-webapp/SKILL.md) owns the rest: prerequisites, what
each capture writes and prints, and driving a state no route reaches (an open dialog, a
filled form) in a named agent-browser session, captured at the four widths too.

`dev.sh`'s first line names the checkout, the full HEAD and the invocation's output
directory (`dev: repo … HEAD … shots /tmp/reg-webapp-shots.XXXXXX`). Quote it. `/tmp` is
purged, so a path without a description of what it showed is not evidence; never commit.

If no route is given, derive the smallest stable route from the changed files and nearby
tests. Common routes: `/`, `/catalog`, `/catalog/<fqid>`,
`/catalog/group/<provider>/<register>/<key>`, `/catalog/group/class/<key>`, `/project`,
`/doc/<identifier>`.

### Inspect

**Open the images.** Claim inspection only of images you actually opened, at the
viewport each was captured at. Read each capture's `.snapshot.txt`, `.a11y.txt` and the
printed console output alongside it.

- Render health: no blank screens, no stuck `aria-busy="true"`, no JS errors or
  React/RAC dev warnings, no missing assets, no capture taken before content settled.
- Layout at each width: no horizontal page overflow, overlap, clipping, unstable
  wrapping or clipped table/card content.
- Density: the app reads as a dense research tool — no hero sections, decorative
  gradients or orbs, oversized cards, cards inside cards, or stock imagery.
- Accessibility visuals: visible focus rings, contrast, status meaning carried by glyph
  and text rather than hue.
- Accessibility tree and audit: controls named and roled in `.snapshot.txt` (no
  unlabeled button, slider or input; landmarks and heading order intact); every axe
  violation in `.a11y.txt` on an element the candidate touches is a finding, and one
  present on the untouched sibling route as well is pre-existing (dismiss it as existing
  behavior outside this candidate). Treat `incomplete` entries as prompts to check by
  eye, not findings.
- Cross-route coherence: render at least one untouched sibling route beside the changed
  one and compare type scale, spacing rhythm and table/card/tag treatment. Flag
  divergence from the surrounding app.

Drive by hand where touched: a tree collapse with a child focused (Tab still enters the
tree), a filter then Escape (expansion restored), an overlay under reduced motion, `⌘K`.
The frontend gate renders nothing app-level, and jsx-a11y (Oxlint) is a floor, not the
axe audit.

### When rendering is unavailable

If the app will not boot or agent-browser cannot launch Chrome, the outcome is
**blocked**, not a product finding: report the exact command, the failing line (and
`$ab doctor`'s Chrome line, `$ab` as run-reg-webapp defines it) and what stayed
unverified. Blocked cannot support a clean visual approval; raise a product `P1` only
where the evidence in hand establishes a candidate defect with that impact.

### Author iteration vs. gate evidence

Fill every field of the rendered template and report the routes, states and viewports
you actually inspected. Say whether captures are author iteration or retained gate
evidence. `gate.py flows` (`dev.sh --fixture-db flows <out-dir>`) covers only its driver
scenarios (the `scenarios` table in `driver.mjs`); every case must pass, and a
`check(...)` that was weakened is a finding. Approval needs evidence for every surface
changed.

## Severity, findings and fixes

`P1` blocks, `P2` is a meaningful UX or contract defect, `P3` is polish. Report
findings; do not silently rewrite broad UI. If you are responsible for fixes — the
implementer acting on its subagent's report — make the smallest source change, follow
the existing primitives and roles, then re-render the affected route and update the
report.

Dismiss a finding only with a concrete reason: "existing behavior outside this
candidate", "intended clipping", "route not touched by this diff".

Optional cross-check, referenced and not vendored: Vercel's `web-interface-guidelines`
(no `transition: all`; animate transform and opacity only; `min-width: 0` on flex
children; links work with Cmd- and middle-click; the URL reflects state;
`translate="no"` on identifiers; `Intl` formats; destructive actions confirm; never
block paste; sticky headers never cover focus). Ignore its Title Case, curly quotes,
virtualize-over-50, identifier truncation, Tailwind, Next.js and hydration rules.

## Report format

Source-only — a source-only reviewer, or anyone without a checkout:

```markdown
# Reg webapp design review — source

- Candidate: <change, branch, or diff reviewed>
- Basis: diff + reg_webapp/frontend/DESIGN.md + reg-webapp-frontend-design. Not rendered.
- Result: <no source findings / findings>

## Findings

### [P1|P2|P3] <title>

- File and line: <path:line>
- Rule: <the DESIGN.md or skill rule>
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
