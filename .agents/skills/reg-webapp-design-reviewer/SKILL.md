---
name: reg-webapp-design-reviewer
description: >-
  Registry Research Toolkit `reg_webapp` rendered-UI review skill. Use for repo-local
  rendered-output PR gates, pr-pipeline visual verification, screenshot-based layout
  review, responsive checks, accessibility visual checks, and UI regression review after
  implementation. This is the post-implementation reviewer counterpart to
  `reg-webapp-frontend-design` and is deliberately named to avoid generic
  `web-design-reviewer` skills.
---

# Reg Webapp Design Reviewer

Review rendered `reg_webapp` changes against the Registry Research Toolkit visual and
accessibility contract. This is a post-implementation reviewer skill, not an authoring
or design-planning skill. Use `reg-webapp-frontend-design` before building new UI; use
this skill after implementation for the rendered visual gate.

## Registry PR Gate Contract

For rendered-output PRs, run this skill in a clean subagent/session before the lead
records the visual gate. The reviewer pass owns screenshot/render inspection and is the
required visual evidence. Invoke this repo-local skill by its full name,
`reg-webapp-design-reviewer`; do not substitute a generic `web-design-reviewer` skill.

Required output for the PR gate:

- changed route(s) or URL(s) reviewed;
- exact render command or preview URL used;
- viewports tested;
- screenshots or render proof inspected;
- findings grouped by severity;
- every finding fixed or dismissed with a reason;
- a final reviewer result suitable for the lead to copy, with the screenshots, into the
  PR's local merge-gate directory (`merge-gates/pr-<N>/` under the
  `$XDG_STATE_HOME/registry-research-toolkit` root, default
  `~/.local/state/registry-research-toolkit/`).

Manual screenshots, `bun` checks, or a lead-agent visual skim do not substitute for this
reviewer pass.

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

If the rendered behavior depends on unreleased DB content, ask the lead for the scratch
DB directory or use the PR's build-db output and pass it as `REG_META_DB`.

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
not enough for the formal visual gate. Screenshots land in `/tmp/reg-webapp-shots/`;
include these local paths and a concise proof payload in the reviewer report. The lead
must copy that reviewer result and its screenshots into the PR's merge-gate directory
before marking the merge gate ready — `/tmp/reg-webapp-shots/` is purged, so paths there
are not durable evidence. Never attach evidence to the PR or commit it to the branch.

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

If acting as the reviewer subagent, report findings to the lead; do not silently rewrite
broad UI. If explicitly responsible for fixes, make the smallest source change, follow
existing components/tokens, then re-render the affected route and update the report.

Dismiss a finding only with a concrete reason, such as "existing behavior outside this
PR", "intended clipping", or "route not affected by this diff".

## Report Format

```markdown
# Reg Webapp Design Review Results

## Summary

- PR/branch: <PR number or branch>
- Routes: <reviewed route(s)>
- Render command or URL: <command/URL>
- Viewports: <tested viewports>
- Local screenshots inspected: <paths under /tmp/reg-webapp-shots/ or other local paths>
- Merge-gate proof: <files copied to the PR's merge-gate directory, or pending lead copy>
- Result: <pass / findings fixed / findings dismissed / blocked>

## Findings

### [P1|P2|P3] <title>

- Page: <route>
- Viewport: <width>
- Element: <selector or description>
- Issue: <what is visibly wrong>
- Recommendation: <smallest practical fix>
- Status: <fixed / dismissed with reason / needs owner>

## Final Gate Note

reg-webapp-design-reviewer: <pass / blocked>; routes=<routes>; viewports=<viewports>;
local_screenshots=<paths>;
gate_proof=<files in the merge-gate directory or pending lead copy>;
findings=<none / fixed / dismissed>.
```
