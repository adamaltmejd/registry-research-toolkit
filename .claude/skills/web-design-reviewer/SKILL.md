---
name: web-design-reviewer
description: >-
  Registry Research Toolkit rendered-UI review skill. Use for `reg_webapp`
  rendered-output PR gates, pr-pipeline visual verification, screenshot-based layout
  review, responsive checks, accessibility visual checks, and UI regression review after
  implementation. This is the post-implementation reviewer counterpart to
  `frontend-design`.
---

# Web Design Reviewer

Review rendered `reg_webapp` changes against the Registry Research Toolkit visual and
accessibility contract. This is a post-implementation reviewer skill, not an authoring
or design-planning skill. Use `frontend-design` before building new UI; use this skill
after implementation for the rendered visual gate.

## Registry PR Gate Contract

For rendered-output PRs, run this skill in a clean subagent/session before the lead
records the visual gate. The reviewer pass owns screenshot/render inspection and is the
required visual evidence.

Required output for the PR gate:

- changed route(s) or URL(s) reviewed;
- exact render command or preview URL used;
- viewports tested;
- screenshots or render proof inspected;
- findings grouped by severity;
- every finding fixed or dismissed with a reason;
- a final reviewer result suitable for the lead to publish in the PR body or a PR
  comment.

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
must publish that reviewer result and screenshot/render evidence in a PR body or comment
before marking the merge gate ready.

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
- Design system: existing components and semantic tokens; no new one-off palettes,
  hero-style sections, decorative gradients/orbs, heavy shadows, or oversized cards.
- Render health: no blank screens, stuck `aria-busy="true"` loading states, obvious JS
  errors, missing critical assets, or screenshots captured before content settled.

## Findings And Fixes

Mark blockers as `P1`, meaningful UX defects as `P2`, and minor polish as `P3`.

If acting as the reviewer subagent, report findings to the lead; do not silently rewrite
broad UI. If explicitly responsible for fixes, make the smallest source change, follow
existing components/tokens, then re-render the affected route and update the report.

Dismiss a finding only with a concrete reason, such as "existing behavior outside this
PR", "intended clipping", or "route not affected by this diff".

## Report Format

```markdown
# Web Design Review Results

## Summary

- PR/branch: <PR number or branch>
- Routes: <reviewed route(s)>
- Render command or URL: <command/URL>
- Viewports: <tested viewports>
- Local screenshots inspected: <paths under /tmp/reg-webapp-shots/ or other local paths>
- PR-visible render proof: <published attachment/comment/artifact/link, or pending lead publication>
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

web-design-reviewer: <pass / blocked>; routes=<routes>; viewports=<viewports>;
local_screenshots=<paths>;
pr_visible_proof=<published attachment/comment/artifact/link or pending lead publication>;
findings=<none / fixed / dismissed>.
```
