---
name: web-design-reviewer
description: >-
  Structured visual review for rendered web UI. Use for Registry Research Toolkit
  `reg_webapp` rendered-output PRs, pr-pipeline visual gates, website design reviews,
  layout/overflow checks, responsive checks, accessibility visual checks, and UI polish
  reviews that require screenshots or browser inspection.
---

# Web Design Reviewer

Run a structured visual-quality review against a rendered web app. In this repository,
this skill is the required design-review pass for PRs that change rendered `reg_webapp`
output. It is separate from the author's own screenshot inspection.

## Registry PR Gate Contract

For rendered-output PRs, run this skill in a clean subagent/session before the lead
agent's final screenshot inspection.

Required output for the PR gate:

- changed route(s) or URL(s) reviewed;
- viewports tested;
- screenshots or render proof inspected;
- findings grouped by severity;
- every finding fixed or dismissed with a reason;
- final reviewer result recorded in the PR body or a PR comment.

Manual screenshots, `bun` checks, or a lead-agent visual skim do not substitute for this
reviewer pass.

## Prerequisites

The target app must be running or runnable. For `reg_webapp`, prefer the repo helper:

```sh
reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <route>
```

Use `dev.sh smoke` when the changed surface spans the catalog/project flow. The helper
picks free ports, renders from the current checkout, writes screenshots under
`/tmp/reg-webapp-shots/`, and tears down servers on exit.

Browser automation must be available for screenshots, page navigation, and viewport
resizing. DOM snapshots are recommended when the tool surface supports them.

## Review Workflow

1. Identify the changed rendered surface.
   - Read the PR diff, issue, or implementer notes.
   - Prefer exact routes named by the change.
   - For `reg_webapp`, common stable routes include `/`, `/catalog`, `/search?q=...`,
     `/project`, `/catalog/<fqid>`, and `/doc/<identifier>`.

2. Render and inspect the app.
   - Capture screenshots for the changed route(s).
   - Test at least mobile `375px`, tablet `768px`, desktop `1280px`, and wide `1920px`
     when the layout is responsive.
   - Retrieve a DOM/accessibility snapshot when available.

3. Check visual quality.
   - Layout: overflow, overlap, clipping, alignment, spacing, scroll behavior.
   - Responsive behavior: mobile usability, breakpoint transitions, touch targets.
   - Accessibility visuals: contrast, focus states, labels/alt text visible in the DOM
     or accessibility tree.
   - Consistency: typography, colors, density, component states, loading/empty/error
     states.
   - Console/render health: blank screens, visible JS errors, failed critical assets.

4. Report findings.
   - Mark blockers as `P1`, meaningful UX defects as `P2`, and minor polish as `P3`.
   - Include page/route, viewport, element, issue, and suggested source area.
   - If no issues are found, say so explicitly and name the routes/viewports reviewed.

5. Fix or route fixes.
   - If acting as the reviewer subagent, report findings to the lead; do not silently
     rewrite broad UI.
   - If also responsible for fixes, make the smallest source change, follow existing
     design/components, then re-render the affected route and update the report.

## Report Format

```markdown
# Web Design Review Results

## Summary

- Target: <URL or route(s)>
- Framework/styling: <detected stack>
- Viewports: <tested viewports>
- Screenshots/render proof: <paths or PR-visible links>
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

<One sentence suitable for PR body/comment, including whether the reviewer pass is
complete and where screenshot proof lives.>
```

## Fix Principles

- Prefer existing components, spacing tokens, color tokens, and layout patterns.
- Keep changes narrow to the affected route/component.
- Do not hide overflow as a substitute for making content fit unless clipping is the
  intended behavior.
- Preserve keyboard focus visibility and accessible names.
- Re-render after fixes; do not rely on headless checks alone.
