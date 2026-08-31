---
name: reg-webapp-design-reviewer
description: >-
  Registry Research Toolkit `reg_webapp` rendered-UI review skill. Use for repo-local
  rendered-output PR gates, visual verification, screenshot-based layout review,
  responsive checks, accessibility visual checks, and UI regression review after
  implementation. This is the post-implementation reviewer counterpart to
  `reg-webapp-frontend-design` and is deliberately named to avoid generic
  `web-design-reviewer` skills.
---

# Reg Webapp Design Reviewer

Review rendered `reg_webapp` changes against the Registry Research Toolkit visual and
accessibility contract. This is a post-implementation reviewer skill, not an authoring
or design-planning skill. Use `reg-webapp-frontend-design` before building new UI; use
this skill after implementation for the rendered visual gate.

## Who Runs This, And As What

Two consumers, one contract. Invoke this repo-local skill by its full name,
`reg-webapp-design-reviewer`; do not substitute a generic `web-design-reviewer` skill.

- **In-lane self-check (authoring layer).** A yard implementation worker whose ticket
  changes rendered UI runs this skill before declaring its candidate — in a clean
  subagent whose prompt is only the changed routes, the diff, and this skill, so the
  judgment does not inherit the implementing session's rationalizations. The worker
  fixes or explicitly dismisses every finding; the subagent reports, it does not
  rewrite.
- **Review seat (judgment layer).** Yard's `ui` review profile carries a claude reviewer
  instructed to run this skill in the operator environment. Its findings enter the
  review report with priorities; blocking findings drive repair rounds. This is the
  independent pass — the self-check above does not substitute for it.

In both seats the honesty rule is absolute: if the app fails to boot or screenshots
cannot be produced, that is a `P1` finding saying exactly that. Never report a clean
result without screenshot evidence. Manual spot screenshots, `bun` checks, or a
non-rendered code skim do not substitute for this pass.

Required output in either seat:

- changed route(s) or URL(s) reviewed;
- exact render command or preview URL used;
- viewports tested;
- screenshots or render proof inspected (local paths);
- findings grouped by severity;
- every finding fixed or dismissed with a reason.

## Inputs

Start from the PR/branch/diff, issue, implementer notes, or route list. If no route is
given, derive the smallest stable route from the changed files and nearby tests.

Common routes:

- `/`
- `/catalog`
- `/catalog/<fqid>`
- `/catalog/group/<provider>/<register>/<key>`
- `/catalog/group/class/<key>`
- `/search?q=...`
- `/project`
- `/doc/<identifier>`

If the rendered behavior depends on unreleased DB content, get the scratch DB directory
from the ticket or the build-db output and pass it as `REG_META_DB`; otherwise the
dev.sh helpers fetch the released DB themselves.

## Rendering

Prefer the one-shot helper from the repo root:

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot --all --viewport 1920x1080 <route>
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke
REG_META_DB="$db_dir" bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot --all --viewport 1920x1080 <route>
```

Use `shot --all --viewport 1920x1080` for responsive screenshots unless the route is
demonstrably desktop-only. Use `smoke` only as an additive broad-flow check for catalog
browsing or app-shell changes; smoke alone captures the default desktop viewport and is
not enough for the formal visual review. Screenshots land in `/tmp/reg-webapp-shots/`;
include these local paths and a concise proof payload in the report. `/tmp` is purged,
so the report's route/viewport/element descriptions must stand on their own — never
commit screenshots to the branch.

Browser automation should inspect screenshots and, when available, DOM/accessibility
snapshots and console output. Do not rely on `bun run lint/check/test/build`; those do
not render pixels.

## Review Checklist

Check the app as a dense registry research tool, not a marketing site.

- App shell: navigation, breadcrumbs, page titles, scroll containers, focus order.
- Catalog/search surfaces: long FQIDs, register names, Swedish labels, code values,
  filters, result density, empty/error/loading states.
- Project authoring: form layout, validation findings, dirty/disabled states, keyboard
  paths, popovers/dialogs, selected rows.
- Layout: no unintended horizontal page overflow, overlap, clipping, unstable wrapping,
  or clipped table/card content at mobile `375px`, tablet `768px`, desktop `1280px`, and
  wide `1920px`.
- Responsiveness: shared table-like results should keep headers accessible and become
  deliberate stacked/card rows on narrow screens.
- Accessibility visuals: visible focus rings, contrast, accessible names, label
  association, status meaning conveyed by text/glyphs rather than color alone.
- Design system: styling consistency and component reuse per the section below; no new
  one-off palettes, hero-style sections, decorative gradients/orbs, heavy shadows, or
  oversized cards.
- Render health: no blank screens, stuck `aria-busy="true"` loading states, obvious JS
  errors, missing critical assets, or screenshots captured before content settled.

## Styling Consistency And Component Reuse

The coherence baseline is the repo design system, not general taste. Before judging
consistency, read the "Visual language (design system)" section of
`reg_webapp/DESIGN.md`, the semantic tokens in `reg_webapp/frontend/src/tokens.css`, and
the shared primitives in `reg_webapp/frontend/src/lib/ui/` (Breadcrumbs, Button,
DataTable, EmptyState, KeyValue, Panel, Skeleton, Tag, plus `utilities.css`:
`.micro-label`, `.visually-hidden`).

Screenshots alone cannot catch token bypass, so also read the PR's frontend diff:

- Component reuse: changed views compose the existing `lib/ui` primitives. A hand-rolled
  near-duplicate of an existing primitive — a bespoke table, button, tag, empty state,
  or key-value list — is at least a `P2`.
- Behavior layer: a11y-critical widgets (comboboxes, menus, dialogs, popovers, sliders)
  come from Bits UI (`bits-ui`, the sanctioned headless-primitives dep — see
  `reg_webapp/DESIGN.md` § UI primitives), styled with scoped CSS reading semantic
  tokens. A hand-rolled widget Bits UI covers is at least a `P2`; so is a Bits UI usage
  that ships its own one-off visual styling instead of tokens.
- Token discipline: flag raw hex/rgb/oklch colors, one-off px spacing/radius/shadow
  values, or new font stacks where a semantic token exists (`--surface*`, `--border*`,
  `--accent*`, `--text-*`, `--space-*`, `--radius*`, `--font-*`, `--micro-label-*`,
  `--focus-ring`, `--elevation-*`). A hardcoded value can render identically today and
  still break the light-first, dark-ready token contract.
- Cross-route coherence: render at least one untouched sibling route alongside the
  changed route and compare type scale, spacing rhythm, and table/card/tag treatment;
  flag divergence from surrounding pages, not just defects within the changed route.

## Findings And Fixes

Mark blockers as `P1`, meaningful UX defects as `P2`, and minor polish as `P3`.

In the self-check subagent or the review seat, report findings; do not silently rewrite
broad UI. If explicitly responsible for fixes (the implementing worker acting on its
subagent's report), make the smallest source change, follow existing components/tokens,
then re-render the affected route and update the report.

Dismiss a finding only with a concrete reason, such as "existing behavior outside this
PR", "intended clipping", or "route not affected by this diff".

## Report Format

```markdown
# Reg Webapp Design Review Results

## Summary

- Ticket/branch: <ticket id, branch, or diff reviewed>
- Routes: <reviewed route(s)>
- Render command or URL: <command/URL>
- Viewports: <tested viewports>
- Local screenshots inspected: <paths under /tmp/reg-webapp-shots/ or other local paths>
- Result: <pass / findings fixed / findings dismissed / blocked>

## Findings

### [P1|P2|P3] <title>

- Page: <route>
- Viewport: <width>
- Element: <selector or description>
- Issue: <what is visibly wrong>
- Recommendation: <smallest practical fix>
- Status: <fixed / dismissed with reason / needs owner>

## Final Note

reg-webapp-design-reviewer: <pass / blocked>; routes=<routes>; viewports=<viewports>;
local_screenshots=<paths>; findings=<none / fixed / dismissed>.
```
