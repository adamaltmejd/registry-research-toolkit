---
name: reg-webapp-frontend-design
description: >-
  Registry Research Toolkit `reg_webapp` UI-authoring design skill. Use before building
  new or substantially reworked Svelte views, components, app-shell surfaces,
  catalog/project-authoring flows, responsive layouts, or any rendered change under
  reg_webapp/frontend/. This is the pre-implementation counterpart to
  `reg-webapp-design-reviewer`; it replaces the generic `frontend-design` plugin for
  this repo.
---

# Registry webapp frontend design

Shape new or substantially reworked `reg_webapp` UI **before implementation**: write a
design brief that composes the committed design language, check it against the closest
existing view, then build to it. The post-implementation pass is
`reg-webapp-design-reviewer`.

## The premise: consistency IS the design

The design language is decided and committed in `reg_webapp/frontend/DESIGN.md`: **ink
on neutral paper**. A monochrome base — near-black text and controls on a light neutral
canvas, white working surfaces, hairline borders — where color is information and never
decoration, with mono reserved for machine identifiers and every label and heading in
sentence case. Light-first and dark-ready through semantic roles, for academic
researchers and data stewards scanning dense Swedish register metadata. Clarity and scan
speed beat editorial flourish.

Generic frontend-design guidance optimizes for a distinctive look per page. Here that is
inverted: the app-level identity is already spent on the typeface pairing and the shell,
and the win condition for a new view is that it is **indistinguishable in language from
the best existing view**. The novelty budget per change is roughly zero. If the task
genuinely needs a pattern the system lacks — a new primitive, a new color sub-system, a
new viz encoding — that is a **design fork**: surface it with a recommendation before
building; never invent a one-off silently.

Never import landing-page aesthetics: no hero sections, decorative split layouts,
gradient/orb backgrounds, stock imagery, oversized card compositions, cards inside
cards, or one-off palettes. The home route is the entry screen of a tool, not a campaign
page.

### Settled choices vs. the canonical tell list

Two settled choices that a generic `frontend-design` skill — and its canonical tell list
— will try to "fix". Both are deliberate: keep them, and do not reintroduce what they
replaced.

- **Mono for machine identifiers is the system.** FQIDs, slugs, value-set codes, years,
  counts and versions are set in `--font-mono` so a reader can separate the thing from
  its name in a catalog that is mostly identifiers. Mono is for identifiers only — never
  for labels, headings or body copy.
- **Tracked-uppercase eyebrows are retired.** Labels and headings are sentence case at
  normal tracking; hierarchy comes from size and weight, not decoration. The tree still
  carries the old `.micro-label` utility and the warm `--rost-*` ramp stops pending the
  restyle ticket (Y-37) — legacy, not a pattern to copy. Numbered section markers,
  middle-dot meta strings and trailing arrows stay out too.

## Read first (in this order)

1. `reg_webapp/frontend/DESIGN.md` — the normative design language: front matter is the
   token set, prose says why the values exist and how to apply them. **On any conflict
   it wins over this skill** — this skill encodes the working rules, not the values.
2. `reg_webapp/DESIGN.md` → "UI primitives — Bits UI + scoped CSS" for the bake-off
   rationale, "Frontend toolchain" for the lint/check/codegen setup, and "SPA routing +
   production fallback" if the change touches routing.
3. `reg_webapp/frontend/src/tokens.css` — the roles as implemented, and what still lags
   DESIGN.md pending the restyle.
4. `reg_webapp/frontend/src/lib/ui/` — the primitive barrel (`index.ts`, `types.ts`)
   plus the one or two primitives closest to what you are building.
5. The closest existing view under `frontend/src/lib/` and its `*.browser.test.ts` —
   that is the pattern you extend.
6. If the change touches backend response shape: `reg_webapp/backend/openapi.json`, the
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
  reading semantic roles. Do not add Tailwind, shadcn, or a parallel token system; Biome
  is the only formatter/linter (no Prettier/ESLint).
- Keep app logic the source of truth inside the primitive — e.g. `Command` with
  `shouldFilter={false}` where the app already ranks (`rankFilter`).
- Known caveat: a nested/expandable row inside a `Command` listbox cannot be a flat
  `role="option"` and splits the keyboard model (arrow-nav for leaves, Tab for
  expanders). If the design needs grouped rows inside a listbox, plan the keyboard model
  explicitly in the brief.

## Color: roles only, sub-systems disjoint

The values live in `reg_webapp/frontend/DESIGN.md`; these are the rules that use them.

- Components consume **semantic roles only** — never primitive ramp stops (`--gray-*`,
  `--rost-*`, a raw status or categorical hue) and never a literal. That discipline is
  what makes the deferred dark theme and the planned per-provider themes a pure role
  remap.
- **Chrome:** the accent is ink, and it paints interactive chrome ONLY — links, primary
  buttons, selection, focus ring, active nav. One `primary` Button per view; the rest
  are `default`/`ghost`. Accent-colored **text** on a tint or surface uses
  `--accent-ink`, never `--accent`. The accent is never a status color and never
  emphasis.
- **Status:** `--err`/`--warn`/`--info`/`--ok` are AA-cleared foregrounds; their `-bg`
  twins are fill tints, never text colors. Status meaning always carries a glyph AND
  text, never hue alone (`Tag` enforces this: status tones require a `glyph`).
- **Categorical type identity:** the `reg`/`var`/`code`/`class`/`group` system tags what
  a node IS — raw `--cat-*` hue for fill and border, `--cat-*-ink` for label text. Never
  reuse brand or status color for type identity, or "this is a variable" collides with
  "this is selected / an error".
- **Data encodings:** facet axes (`--facet-axis-*`) for concept-group dimensions, viz
  edges (`--viz-edge-*`) for graph relation marks. Data — not chrome, not status.

The four sub-systems are disjoint. Borrowing across them is a fork to surface, not a
shortcut to take.

## Typography and geometry

- `--font-ui` for UI text, `--font-mono` for every machine identifier. Both families are
  self-hosted woff2; never add a font or a CDN.
- Type scale via roles (`--text-display` … `--text-sm`, `--text-micro`); case and
  hierarchy are settled in the tell list above.
- Spacing on the `--space-*` rhythm, set by the container that owns the gap and never by
  margins on children. Two radii: `--radius-sm` on controls, `--radius` on panels and
  cards; nothing is a pill. One soft `--elevation-raised` shadow and nothing else casts
  one — no glows, gradients, blobs or glass.

## Interaction and accessibility floor

- Keyboard-first: every interactive element reachable and operable. Focus is visible via
  `:focus-visible { box-shadow: var(--focus-ring); }` in the component's **own** scoped
  CSS — no global stylesheet owns focus.
- Motion is short and functional (`--motion-fast` disclosure/popover transitions), never
  decorative; nothing animates on page load, and every transition respects
  `@media (prefers-reduced-motion: reduce)`.
- `DataTable`'s settled decisions are not re-litigated: explicit unconditional ARIA
  roles; ARIA-grid selection (per-row tab stops, not roving tabindex); `framed` tables
  are never wrapped in a `Panel` (duplicate heading rows); the FIRST column is the
  primary/title column by position; `mono`/`numeric` column flags for identifiers and
  measures.
- Screen-reader-only content uses the `.visually-hidden` utility, never `display: none`
  (which severs the a11y tree).
- Loading surfaces use `Skeleton` and expose `aria-busy="true"` — `run-reg-webapp` waits
  on it before screenshotting. Empty states use `EmptyState` and point to a next action.
  Errors say what went wrong and how to fix it, plainly and without apologising.
- Copy is design material: plain verbs, name things by what the researcher recognizes
  (registers, variables, value sets, projects), never by internals. An action keeps the
  same name through its whole flow.

## Responsive contract

Design every rendered change for **375, 768, 1280 and 1920 px**. Use the existing 48 rem
breakpoint patterns (`AppShell`, search, `DataTable` stacking) rather than minting new
breakpoints. Long FQIDs, slugs, codes, and Swedish labels must wrap or scroll
deliberately — no horizontal page overflow. For table-like results prefer `DataTable`'s
stacked-card behavior (first column becomes the card title, non-primary cells get label
prefixes, the header row stays in the a11y tree) over bespoke media queries.

Svelte traps that have bitten this codebase before:

- Scoped-CSS specificity: a scoped class selector beats a bare-element selector inside
  `@media` blocks — style via classes, not bare elements.
- 375 px horizontal overflow is usually a missing `box-sizing: border-box` on a padded
  100 %-width element.
- Do not fake subgrid with `display: contents` — it strips roles from the accessibility
  tree.

## Output: the design brief

Before coding, write a short brief in the working message or the ticket notes:

```md
Design brief:
- Surface: <route/component/flow>
- User task: <what the researcher/steward is trying to do>
- Pattern: <existing view + lib/ui primitives extended; Bits UI components used>
- Layout: <desktop and ≤48rem structure>
- States: <loading/empty/error/selected/focus/disabled>
- Forks: <none, or the new pattern/primitive/color the system lacks — needs a decision>
- Verification routes: <dev.sh shot/smoke routes, --all for the four widths>
```

Then check the brief against the closest existing view before writing code: same
pattern, same primitives, same states, same names for the same things? A divergence is a
**design fork** — surface it with a recommendation and get a decision rather than
shipping a second way to do the thing. If the brief reveals that the requested UI
duplicates an existing route, primitive or data contract, that is the same conversation.

Then implement the smallest coherent change that satisfies the brief.

## Verification

From `reg_webapp/frontend/`: `bun run lint`, `bun run check`, `bun run test`,
`bun run build`. The browser component tests do render in Chromium, but they exercise
primitives in isolation — they are not app-level route and state coverage, and they are
not visual inspection. Render the changed view from the repo root:

```sh
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db shot --all <route>
# or, for the catalog happy path:
bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh --fixture-db smoke
```

`--all` captures the four widths. `dev.sh` prints the unique output directory of that
invocation on its first line — **look at** the images there and check them against this
skill at every width. Pass `REG_META_DB=<db_dir>` instead of `--fixture-db` when the
rendering depends on specific catalog content.

Author screenshots are iteration evidence, not the candidate's design pass: that is a
`reg-webapp-design-reviewer` run in a clean subagent, whose findings you fix or
explicitly dismiss before declaring.

Attribution: adapted from Anthropic's Apache-2.0 `frontend-design` plugin concept
(LICENSE.txt); the rules above are specific to the Registry Research Toolkit.
