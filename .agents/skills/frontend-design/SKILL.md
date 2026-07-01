---
name: frontend-design
description: >-
  Registry Research Toolkit frontend design skill for reg_webapp UI authoring. Use
  before building new or substantially reworked Svelte views, components, app-shell
  surfaces, catalog/project authoring flows, responsive layouts, or visual systems under
  reg_webapp/frontend/. Also use when pr-pipeline needs the design-authoring pass that
  precedes implementation for rendered UI changes.
---

# Registry Frontend Design

## Scope

Shape new or substantially reworked `reg_webapp` UI before implementation. This is an
authoring skill, not a post-hoc visual review. It gives the implementer a concrete UI
direction that fits the existing registry webapp, then the implementer builds and
verifies it through the normal frontend and visual gates.

Use the app's current design system. Do not import generic landing-page aesthetics from
the upstream Claude frontend-design plugin.

Attribution: this repo-local skill is adapted from Anthropic's Apache-2.0
`frontend-design` plugin concept, but the rules below are specific to Registry Research
Toolkit.

## Read First

Before designing, read the current local sources that define the surface you are
changing:

1. `reg_webapp/DESIGN.md`, especially "Frontend toolchain", "UI primitives", and "Visual
   language (design system)".
2. `reg_webapp/frontend/src/tokens.css`.
3. The closest existing component(s) in `reg_webapp/frontend/src/lib/`.
4. Any relevant `*.browser.test.ts` for the component or flow.

If the request touches backend response shape, also inspect
`reg_webapp/backend/openapi.json`, the route/model code, and the generated
`frontend/src/lib/api-types.ts` contract before proposing UI.

## Design Direction

Default to the committed visual language:

- Modern data-tool/dashboard, not marketing site.
- Dense, calm, keyboard-first interfaces for academic researchers and data stewards.
- App-shell surfaces, panels, tables, filters, pickers, breadcrumbs, tags, validation
  findings, and project-authoring controls.
- Clarity and scan speed over editorial flourish.
- Light-first and dark-ready through semantic tokens.

Do not create hero-heavy pages, decorative split layouts, gradient/orb backgrounds,
stock imagery, oversized card compositions, or one-off palettes. The home route is an
entry screen for a tool, not a campaign landing page.

## Implementation Contract

Follow these rules when authoring the design plan and implementation:

- Svelte 5 + TypeScript + Vite; use runes and existing local patterns.
- Use Bun scripts from `reg_webapp/frontend/`: `bun run lint`, `bun run check`,
  `bun run test`, and `bun run build`.
- Use Biome; do not add Prettier, ESLint, Tailwind, shadcn, or a parallel token system.
- Use Bits UI for a11y-critical widgets it covers: comboboxes, command/listbox behavior,
  menus, dialogs. Style with scoped CSS and the repo tokens.
- Use existing shared UI primitives from `frontend/src/lib/ui/` before inventing another
  panel/table/tag/button/skeleton/empty-state shape.
- Component CSS must consume semantic roles from `tokens.css` only, not primitive ramps
  such as `--gray-*`, `--rost-*`, or raw hex colors.
- Brand `--accent` is interactive chrome only: links, primary buttons, selected/focus
  state, active nav. Use `--accent-ink` for accent text. Never use the brand accent as
  an error/warning/status color.
- Status meaning needs text/glyphs, not hue alone. Use `--err`, `--warn`, `--info`, and
  `--ok` roles with their background roles.
- Categorical type identity uses the category system (`reg`, `var`, `code`, `class`,
  `group`) and AA-cleared ink roles. Do not reuse brand or status colors for type tags.
- Keep motion small and functional: disclosure/popover/selection transitions around
  `--motion-fast`, not decorative animation.
- Keep radii within the existing token language (`--radius-sm`, `--radius`) and avoid
  heavy shadows.
- Preserve or improve keyboard navigation, focus-visible rings, accessible names, roles,
  and screen-reader semantics.
- Loading states must expose `aria-busy="true"` so `run-reg-webapp` waits for rendered
  content before screenshotting.

If a proposed UI needs a new dependency, stop and justify it against the repo ladder:
existing capability, platform, installed dependency, minimal code, then new dependency.

## Responsive Contract

Design every rendered change for at least:

- Mobile: 375px wide.
- Tablet: 768px wide.
- Desktop: 1280px wide.

Use the existing narrow breakpoint patterns, especially the 48rem breakpoint used by
`AppShell`, search, and `DataTable`. Long FQIDs, slugs, labels, codes, and Swedish text
must wrap or scroll deliberately without horizontal page overflow.

For tables or table-like results, prefer the shared `DataTable` behavior: explicit ARIA
roles, stacked card rows at narrow widths, visually hidden headers kept in the
accessibility tree, micro-labels for secondary cells, mono alignment for identifiers and
numeric values.

## Output To The Implementer

Before coding, write a short design brief in the working message or PR notes:

```md
Design brief:
- Surface: <route/component/flow>
- User task: <what the researcher is trying to do>
- Pattern: <existing component/primitives to extend>
- Layout: <desktop and mobile structure>
- States: <loading/empty/error/dirty/selected/disabled/focus>
- Verification routes: <dev.sh shot/smoke routes, include --all when responsive>
```

Then implement the smallest coherent change that satisfies the brief. If the brief
reveals that the requested UI duplicates an existing route, primitive, or data contract,
surface that as a design fork before coding.

## Verification

Headless checks are required but not sufficient for rendered UI.

From `reg_webapp/frontend/` run the relevant Bun checks. From the repo root, render the
changed view with the one-shot driver:

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot --all <route>
# or, for the catalog happy path:
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke
```

Inspect the screenshots in `/tmp/reg-webapp-shots/`. If the rendering depends on
unreleased DB content, use a scratch DB and pass `REG_META_DB="$db_dir"` to the same
driver command.

For `pr-pipeline`, this authoring pass does not replace `reg-webapp-design-reviewer`; it
precedes implementation. After implementation, the required visual gate is a clean
`reg-webapp-design-reviewer` pass against the rendered app with screenshot/render
evidence; manual visual inspection does not replace it.
