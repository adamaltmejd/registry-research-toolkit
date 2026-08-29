---
name: reg-webapp-frontend-design
description: >-
  Registry Research Toolkit `reg_webapp` UI-authoring design skill. Use before building
  new or substantially reworked Svelte views, components, app-shell surfaces,
  catalog/project-authoring flows, responsive layouts, or any rendered change under
  reg_webapp/frontend/. This is the pre-implementation authoring counterpart to
  `reg-webapp-design-reviewer`; it replaces the generic `frontend-design` plugin for
  this repo.
---

# Registry Webapp Frontend Design

Shape new or substantially reworked `reg_webapp` UI **before implementation**: produce a
design brief that composes the committed design system, then build to it. This is an
authoring skill — the post-implementation rendered gate is `reg-webapp-design-reviewer`.

## The premise: consistency IS the design

The design language is decided (#801, `reg_webapp/DESIGN.md` → "Visual language"): a
modern data-tool / dashboard aesthetic in the Linear/Observable lineage — app shell,
dense legible tables, keyboard-first interaction, subtle surfaces, one confident warm
accent — for academic researchers and data stewards scanning dense Swedish register
metadata. Light-first and dark-ready through semantic tokens. Clarity and scan speed
beat editorial flourish.

Generic frontend-design guidance optimizes for a distinctive look per page. Here that is
inverted: the app-level identity is already spent (Schibsted Grotesk + Rost +
micro-label eyebrows + panel grammar), and the win condition for a new view is that it
is **indistinguishable in language from the best existing view**. The novelty budget per
change is roughly zero. If the task genuinely needs a pattern the system lacks — a new
primitive, a new color sub-system, a new viz encoding — that is a **design fork**:
surface it with a recommendation before building; never invent a one-off silently.

Never import landing-page aesthetics: no hero sections, decorative split layouts,
gradient/orb backgrounds, stock imagery, oversized card compositions, or one-off
palettes. The home route is the entry screen of a tool, not a campaign page.

## Read first (in this order)

1. `reg_webapp/DESIGN.md` → "Visual language (design system)", "UI primitives — Bits UI +
   scoped CSS", "Frontend toolchain". **On any conflict, DESIGN.md wins over this
   skill** — this skill encodes the stable rules; DESIGN.md is the source of truth.
2. `reg_webapp/frontend/src/tokens.css` — the two-layer token set and the current role
   names (the file's comments carry the contrast rationale).
3. `reg_webapp/frontend/src/lib/ui/` — the primitive barrel (`index.ts`, `types.ts`)
   plus the one or two primitives closest to what you are building.
4. The closest existing view under `frontend/src/lib/` and its `*.browser.test.ts` —
   that is the pattern you extend.
5. If the change touches backend response shape: `reg_webapp/backend/openapi.json`, the
   route/model code, and the generated `frontend/src/lib/api-types.ts` contract before
   proposing UI.

## Composition ladder (reuse first)

Design by composing in this order, and name in the brief which rung you stopped at:

1. **An existing view pattern** — SubjectView sections, browse tables, picker columns,
   search results. Most "new" views are an existing pattern with different data.
2. **A shared primitive** from `frontend/src/lib/ui/` via the barrel: `Panel`,
   `DataTable`, `Breadcrumbs`, `Tag`, `Button`, `KeyValue`, `Skeleton`, `EmptyState`
   (plus `AppShell` in `lib/`). Do not invent another panel/table/tag/button/skeleton/
   empty-state shape.
3. **A Bits UI component** for interactive behavior the primitives don't cover (see next
   section).
4. **New scoped CSS on semantic roles** — smallest possible; a genuinely reusable new
   unit belongs in `lib/ui/` with a browser test and is a fork to surface first.

If a proposed UI needs a new dependency, stop and justify it against the repo ladder
(existing capability → platform → installed dep → minimal code → new dep).

## Behavior layer: Bits UI, always

`bits-ui` (the Svelte-5 runes major) is the sanctioned headless behavior + ARIA layer.
**Never hand-roll a widget Bits UI covers** — combobox, command/listbox, menu, dialog,
popover, tooltip, slider, tabs, accordion. A hand-extracted slider that predated this
decision (#632) is the canonical mistake; the #689 bake-off is the adoption rationale.

- Bits UI ships behavior and ARIA only, zero styles. All visuals come from scoped CSS
  reading semantic tokens. Do not add Tailwind, shadcn, or a parallel token system;
  Biome is the only formatter/linter (no Prettier/ESLint).
- Keep app logic the source of truth inside the primitive — e.g. `Command` with
  `shouldFilter={false}` where the app already ranks (`rankFilter`).
- Known caveat: a nested/expandable row inside a `Command` listbox cannot be a flat
  `role="option"` and splits the keyboard model (arrow-nav for leaves, Tab for
  expanders). If the design needs grouped rows inside a listbox, plan the keyboard model
  explicitly in the brief.

## Color: disjoint sub-systems, roles only

Components consume **semantic roles only** — never primitive ramps (`--gray-*`,
`--rost-*`, raw status/categorical hues) and never raw hex. That discipline is what
makes dark mode a pure role remap. Four disjoint color sub-systems; never borrow across
them:

- **Brand chrome** — `--accent` (Rost `#B8552A`) paints interactive chrome ONLY: links,
  primary buttons, selection, focus ring, active nav. Accent-colored **text** on a tint
  or surface uses `--accent-ink`, never `--accent` (it fails AA as text). One `primary`
  Button per view; the rest are `default`/`ghost`. The brand accent is never an
  error/warning/status color.
- **Status** — `--err`/`--warn`/`--info`/`--ok` are AA-cleared foregrounds; their `-bg`
  twins are fill tints. Status meaning always carries a glyph AND text, never hue alone
  (`Tag` enforces this: status tones require a `glyph`). Never use a `-bg` fill tint as
  a text color.
- **Categorical type identity** — the `reg`/`var`/`code`/`class`/`group` system tags
  what a node IS: raw `--cat-*` hue for fill/border, `--cat-*-ink` for label text
  (AA-cleared). Never reuse brand or status colors for type identity, or "this is a
  variable" collides with "this is selected / an error".
- **Data-viz edges** — graph relation marks read `--viz-edge-*` roles. An edge is data,
  not chrome and not status; it never borrows from the other three.

## Typography and geometry

- `--font-ui` (Schibsted Grotesk) for UI text; `--font-mono` (IBM Plex Mono) for **every
  machine identifier** — FQIDs, slugs, value-set codes, years, counts. Fonts are
  self-hosted woff2; never add a font or a CDN.
- Type scale via roles (`--text-display` … `--text-sm`, `--text-micro`). Hierarchy comes
  from the tracked-uppercase **`.micro-label`** eyebrow (global utility in
  `lib/ui/utilities.css`), not from heavy headings.
- Spacing on the `--space-*` rhythm; radii `--radius-sm` (controls) / `--radius`
  (panels) only; one soft `--elevation-raised` shadow, no heavy drop shadows; motion
  small and functional (\~`--motion-fast` disclosure/popover transitions), never
  decorative.

## Interaction and accessibility floor

- Keyboard-first: every interactive element reachable and operable. Focus is visible via
  `:focus-visible { box-shadow: var(--focus-ring); }` in the component's **own** scoped
  CSS — no global stylesheet owns focus.
- `DataTable`'s settled decisions are not re-litigated: explicit unconditional ARIA
  roles; ARIA-grid selection (per-row tab stops, not roving tabindex); `framed` tables
  are never wrapped in a `Panel` (duplicate heading rows); the FIRST column is the
  primary/title column by position; `mono`/`numeric` column flags for identifiers and
  measures.
- Screen-reader-only content uses the `.visually-hidden` utility, never `display: none`
  (which severs the a11y tree).
- Loading surfaces use `Skeleton` and expose `aria-busy="true"` — `run-reg-webapp` waits
  on it before screenshotting. Empty states use `EmptyState` and point to a next action.
  Errors say what went wrong and how to fix it, plainly.
- Copy is design material: sentence case, plain verbs, name things by what the
  researcher recognizes (registers, variables, value sets, projects), never by
  internals. An action keeps the same name through its whole flow.

## Responsive contract

Design every rendered change for **375 px, 768 px, and 1280 px**. Use the existing 48
rem breakpoint patterns (`AppShell`, search, `DataTable` stacking) rather than minting
new breakpoints. Long FQIDs, slugs, codes, and Swedish labels must wrap or scroll
deliberately — no horizontal page overflow. For table-like results prefer `DataTable`'s
stacked-card behavior (first column becomes the card title, non-primary cells get
micro-label prefixes, the header row stays in the a11y tree) over bespoke media queries.

Svelte traps that have bitten this codebase before:

- Scoped-CSS specificity: a scoped class selector beats a bare-element selector inside
  `@media` blocks — style via classes, not bare elements.
- 375 px horizontal overflow is usually a missing `box-sizing: border-box` on a padded
  100 %-width element.
- Do not fake subgrid with `display: contents` — it strips roles from the accessibility
  tree.

## Output: the design brief

Before coding, write a short brief in the working message or PR notes:

```md
Design brief:
- Surface: <route/component/flow>
- User task: <what the researcher/steward is trying to do>
- Pattern: <existing view + lib/ui primitives extended; Bits UI components used>
- Layout: <desktop and ≤48rem structure>
- States: <loading/empty/error/selected/focus/disabled>
- Forks: <none, or the new pattern/primitive/color the system lacks — needs a decision>
- Verification routes: <dev.sh shot/smoke routes, --all when responsive>
```

Then implement the smallest coherent change that satisfies the brief. If the brief
reveals that the requested UI duplicates an existing route, primitive, or data contract,
surface that as a design fork before coding.

## Verification

Headless checks are required but never render a pixel. From `reg_webapp/frontend/` run
`bun run lint`, `bun run check`, `bun run test`, `bun run build`; then render the
changed view from the repo root with the one-shot driver:

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot --all <route>
# or, for the catalog happy path:
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh smoke
```

**Look at** the screenshots in `/tmp/reg-webapp-shots/` and check them against this
skill's rules at all three widths. If the rendering depends on unreleased DB content,
pass `REG_META_DB=<db_dir>` to the same driver command.

This authoring pass precedes implementation and does not replace the visual gate: after
implementation, the required evidence is a clean `reg-webapp-design-reviewer` pass
against the rendered app with screenshot proof — author screenshots are iteration
evidence, not the gate.

Attribution: adapted from Anthropic's Apache-2.0 `frontend-design` plugin concept
(LICENSE.txt); the rules above are specific to the Registry Research Toolkit.
