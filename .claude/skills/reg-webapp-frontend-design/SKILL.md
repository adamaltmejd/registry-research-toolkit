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

The design language is decided and committed in `reg_webapp/frontend/DESIGN.md`
("Overview" and "Do's and Don'ts"). Generic frontend-design guidance optimizes for a
distinctive look per page; here the win condition is the opposite, a view
indistinguishable in language from the best existing one. If the task genuinely needs a
pattern the system lacks — a new primitive, a new color sub-system, a new viz encoding —
that is a **design fork**: surface it with a recommendation before building; never
invent a one-off silently.

A generic `frontend-design` skill and its tell list will try to "fix" two settled
choices: mono for machine identifiers, and sentence-case labels at normal tracking
(DESIGN.md → "Typography"). Keep both. The `.micro-label` utility is the `label` level,
not an eyebrow to re-track.

## Read first (in this order)

1. `reg_webapp/frontend/DESIGN.md` — the normative design language: front matter is the
   token set, prose says why the values exist and how to apply them. **On any conflict
   it wins over this skill** — this skill encodes the working rules, not the values.
2. `reg_webapp/DESIGN.md` → "UI primitives — Bits UI + scoped CSS" for the bake-off
   rationale, "Frontend toolchain" for the lint/check/codegen setup, and "SPA routing +
   production fallback" if the change touches routing.
3. `reg_webapp/frontend/src/tokens.css` — the roles as implemented, which
   `bun run lint:tokens` holds to the DESIGN.md front matter.
4. `reg_webapp/frontend/src/lib/ui/` — the primitive barrel (`index.ts`, `types.ts`)
   plus the one or two primitives closest to what you are building.
5. The closest existing view under `frontend/src/lib/` and its `*.browser.test.ts` —
   that is the pattern you extend.
6. If the change touches server response shape: `crates/reg-meta/openapi.json`, the
   operation code, and the generated `frontend/src/lib/api-types-rust.ts` contract
   before proposing UI.

## Composition ladder (reuse first)

Design by composing in this order, and name in the brief which rung you stopped at:

1. **An existing view pattern** — SubjectView sections, browse tables, picker columns,
   search results. Most "new" views are an existing pattern with different data.
2. **A shared primitive** from `frontend/src/lib/ui/` via the barrel (listed in
   DESIGN.md → "Components"). Do not invent another shape for one of them.
3. **A Bits UI component** for interactive behavior the primitives don't cover (see next
   section).
4. **New scoped CSS on semantic roles** — smallest possible; a genuinely reusable new
   unit belongs in `lib/ui/` with a browser test and is a fork to surface first.

If a proposed UI needs a new dependency, stop and justify it against the repo ladder
(existing capability → platform → installed dep → minimal code → new dep).

## Behavior layer: Bits UI, always

`bits-ui` (the Svelte-5 runes major) is the sanctioned headless behavior + ARIA layer.
**Never hand-roll a widget Bits UI covers** — combobox, command/listbox, menu, dialog,
popover, tooltip, slider, tabs, accordion.

- Bits UI ships behavior and ARIA only, zero styles. Do not add Tailwind, shadcn, or a
  parallel token system; Biome is the only formatter/linter (no Prettier/ESLint).
- Keep app logic the source of truth inside the primitive — e.g. `Command` with
  `shouldFilter={false}` where the app already ranks (`rankFilter`).
- Known caveat: a nested/expandable row inside a `Command` listbox cannot be a flat
  `role="option"` and splits the keyboard model (arrow-nav for leaves, Tab for
  expanders). If the design needs grouped rows inside a listbox, plan the keyboard model
  explicitly in the brief.

## Visual and interaction rules

Color roles and their four disjoint sub-systems, type, spacing, radii, elevation, focus,
motion, copy and the four review widths (375, 768, 1280, 1920 px) are all in
`reg_webapp/frontend/DESIGN.md` ("Colors" through "Do's and Don'ts"). Read them there;
borrowing across color sub-systems or minting a breakpoint is a fork to surface. What
DESIGN.md does not say:

- Accent-colored **text** on a tint or surface uses `--accent-ink`, never `--accent`.
- `DataTable` selection is ARIA-grid with per-row tab stops, not roving tabindex. When
  it stacks below 48 rem, non-primary cells get label prefixes; prefer that over bespoke
  media queries for table-like results.
- Screen-reader-only content uses the `.visually-hidden` utility, never `display: none`
  (which severs the a11y tree).
- Scoped-CSS specificity: a scoped class selector beats a bare-element selector inside
  `@media` blocks — style via classes, not bare elements.
- 375 px horizontal overflow is usually a missing `box-sizing: border-box` on a padded
  100 %-width element.

## Output: the design brief

Before coding, write a short brief in the working message or the PR description:

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
bash reg_webapp/dev/dev.sh --fixture-db shot --all <route>
# or, for the catalog happy path:
bash reg_webapp/dev/dev.sh --fixture-db smoke
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
