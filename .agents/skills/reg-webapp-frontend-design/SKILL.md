---
name: reg-webapp-frontend-design
description: >-
  Registry Research Toolkit `reg_webapp` UI-authoring design skill. Use before building
  new or substantially reworked React views, components, app-shell surfaces,
  catalog/project-authoring flows, responsive layouts, or any rendered change under
  reg_webapp/frontend/. Load it together with `frontend-design:frontend-design`. This is
  the pre-implementation counterpart to `reg-webapp-design-reviewer`.
---

# Registry webapp frontend design

Shape new or substantially reworked `reg_webapp` UI **before implementation**: write a
design brief that composes the committed design language, check it against the closest
existing view, then build to it. The stack is React 19, React Aria Components (RAC) for
behavior, and CSS Modules reading the roles in `src/tokens.css`. The post-implementation
pass is `reg-webapp-design-reviewer`.

## Consistency is the design

The design language is decided and committed in `reg_webapp/frontend/DESIGN.md`. Here
the win condition is a view indistinguishable in language from the best existing one,
not a distinctive look per page. A pattern the system lacks (a new primitive, a new
colour sub-system, a new viz encoding) is a **design fork**: surface it with a
recommendation before building; never invent a one-off silently.

## Pair with `frontend-design:frontend-design`

Load the plugin skill alongside this one for process and craft: plan, check the plan
against the brief, build, critique from screenshots; "spend your boldness in one place"
and remove one accessory; and its whole "More on writing in design" section. DESIGN.md
sets every value. Where the plugin says otherwise, DESIGN.md wins:

- "The hero is the first thing": there is no hero. A detail pane opens on breadcrumb and
  `h1` (DESIGN.md → "Layout").
- "Choose your typefaces" and "a 4–6 hex palette": the pairing and tokens are fixed. The
  plan step is the design brief below, which composes existing roles.
- "A monospace face for small data labels" as a tell: mono for machine identifiers is
  settled. Mono for labels stays forbidden, so the two agree on labels.
- "Line lengths under 80 characters": prose in a pane stays under 60.

Where they agree, both apply: no all-caps tracked eyebrows, no middle-dot meta strings,
no "WORD — fragment" labels, no trailing arrows, no page-load motion, motion answers a
user action, no SaaS card kit, sentence case, errors say how to fix it.

## Read first (in this order)

1. `reg_webapp/frontend/DESIGN.md`: front matter is the token set, prose says why and
   how. **On any conflict it wins over this skill.**
2. `reg_webapp/DESIGN.md` → the UI primitives, frontend toolchain and SPA routing
   sections that the change touches.
3. `reg_webapp/frontend/src/tokens.css`: the roles as implemented, held to the front
   matter by `lint:tokens`.
4. `reg_webapp/frontend/src/lib/ui/`: the primitives (one file each) and the one or two
   closest to what you are building.
5. The closest existing view: the pattern you extend.
6. If the change touches a server response shape: `crates/reg-meta/openapi.json` and the
   generated `frontend/src/lib/api-types-rust.ts` before proposing UI.
7. The RAC page for each component you use: `https://react-aria.adobe.com/<Name>.md`
   (index at `https://react-aria.adobe.com/llms.txt`). Read the docs for the pinned
   version, not memory; the official RAC skill
   (`npx skills add https://react-aria.adobe.com`) is an optional local install, not
   vendored here.

## Composition ladder (reuse first)

Name in the brief which rung you stopped at:

1. **An existing view pattern.** Most "new" views are an existing pattern with different
   data.
2. **A shared primitive** from `src/lib/ui/`, imported by its file (DESIGN.md →
   "Components").
3. **A RAC component** for behavior no primitive covers.
4. **A new CSS Module on semantic roles**, as small as possible. A reusable new unit
   belongs in `lib/ui/` and is a fork to surface first.

A new dependency stops the work until it is justified against the repo ladder.

## Behavior layer: React Aria Components, always

**Never hand-roll a widget RAC covers:** Button, Link, Dialog/Modal, Popover, Tooltip,
Menu, ListBox, ComboBox, Autocomplete, Select, Slider, Tabs, Disclosure, Tree, GridList,
SearchField, Checkbox, ToggleButton. `DataTable` stays a plain `<table>` with explicit
roles (reg_webapp/DESIGN.md, "not to re-litigate"). No Tailwind, shadcn or a second
token system.

- **Props, not DOM habits.** `onPress`, not `onClick`; `isDisabled`, `isOpen`,
  `isSelected`, not `disabled`/`open`. A `Popover` or `Modal` holds a `<Dialog>`.
- **Collections.** Dynamic lists use `items` plus a render function. Every item has a
  stable domain `id` (FQID, slug), unique across the whole collection including
  sections, never an array index. With `array.map`, set both `key` and `id`. Never mix
  static children with `items`, and never wrap items in a fragment or `div`. Give
  `textValue` to any item whose children are not plain text.
- **`dependencies`.** An item render that reads state outside the item passes it in
  `dependencies={[...]}`, or RAC serves cached rows.
- **Controlled state.** `selectedKeys`/`expandedKeys` are Sets of ids owned by the app.
  Route every change, header buttons included, through one `onSelectionChange` or
  `onExpandedChange` handler.
- **Filter by changing `items`**, never by hiding rows with CSS.
- **Labels.** Don't add `aria-label` to a part RAC already labels (the tree chevron, the
  slider thumb). Don't hand-write `onKeyDown` on a RAC widget.
- **Customization.** Prefer slots and contexts to a wrapper. A wrapper composes
  `className`, `style` and `children` with `composeRenderProps`. A `render` prop renders
  the same element type, one root, and spreads the given DOM props.
- **Routing.** The app root wraps `RouterProvider` (`navigate`, `useHref`) over the
  vanilla router, so `href` on Link, MenuItem, ListBoxItem, TreeItem and GridListItem
  routes client-side. Import from the package root, never a private path.
- **Locale.** One `I18nProvider locale="en-US"` at the root: RAC's own strings follow
  the UI language. Swedish data follows the next section, not this locale.
- **Pinning.** RAC is pinned exactly. An alpha API (`TreeSection`) is used only under
  that pin and named in the brief.

## Required primitives and classes

- **A verdict or notice is a `ui/StatusRow`** (tone `error`, `warn`, `info`, `ok`;
  `live` for an alert or status). Never a hand-styled coloured `<p>` or a bare glyph; a
  data warning, a failed load and a refusal all render as one.
- **A classed anchor takes the shared `.link` class** (`utilities.css`), so it gets the
  ink-with-accent-underline look; a plain `<a>` with no class already has it. RAC `Link`
  inside a styled row keeps its own module class.

## Swedish text: filtering and sorting

- **Filtering** goes through the existing `rankFilter`/`foldSearch` in `src/lib/`
  (reg-core's fold over WASM), so a client filter matches exactly what the server search
  matches. That fold drops diacritics: "alderspension" finds "ålderspension". Never
  filter with RAC's `useFilter` or Autocomplete's default filter: they collate in the
  provider's en-US locale and disagree with the server.
- **Sorting** Swedish labels uses one shared
  `Intl.Collator('sv', { sensitivity: 'base' })`, never bare `localeCompare`, so å, ä
  and ö sort after z.

## Tree and sticky-list gotchas (RAC 1.22)

- The `Tree` DOM is flat: every row is a sibling. Style depth with `[data-level]` and
  `var(--tree-item-level)`, never `parent > child`.
- Flat sticky rows cover rather than push. Push-sticky group headers need the alpha
  `TreeSection` (see Pinning).
- `hasChildItems` is ignored on a parent with zero children: give it a
  `<TreeLoadMoreItem isLoading>` child while its children are unknown.
- RAC has no controlled focused key. Before a collapse that hides the focused row, call
  `state.selectionManager.setFocusedKey(parentKey)` (state read from the `className`
  render prop into a ref) inside the one `onExpandedChange`, or Tab skips the tree.
- Selection does not follow ←-to-parent, type-ahead or collapse. Selection drives the
  detail pane, so the brief states what collapse does to it.
- A filter keeps its own expansion set; Escape restores the user's.
- Inside a scroll container with sticky rows, focus rings are inset
  (`box-shadow: inset …`), or the sticky row covers them.
- `text-transform` on a labelling element leaks into the accessible name.

## React 19 rules

- **No effect for derived data.** Compute during render; reset on an identity change
  with `key`; event logic goes in the handler. An effect only syncs with an external
  system (DOM measurement, a listener, a fetch) and returns a cleanup.
- **StrictMode runs effects twice in dev.** A lifecycle starter is idempotent and
  returns a cleanup.
- **Fetches** go through the one shared async-resource hook (`useAsyncResource`): an
  `AbortController` per request, aborted on unmount or deps change, stale responses
  dropped. No ad-hoc fetch in an effect; never `setState` after an abort.
- **State** lives in plain-TS stores read with `useSyncExternalStore`; the URL owns
  route state. No new state library. Updates that read previous state use functional
  `setState`.
- **Components at module scope**, never defined inside another component. Hooks at top
  level only.
- **React 19 idioms:** `ref` is a plain prop (no `forwardRef`); `<Ctx value>` (no
  `.Provider`); ref callbacks may return a cleanup; `useEffectEvent` for non-reactive
  reads inside an effect.
- **No Suspense for data.** Loading is an explicit `Skeleton` with `aria-busy`.
- **Memoization.** The React Compiler is lint-only (no compiler transform). Memoize only
  a measured hot path, a value an effect depends on, or a value in RAC `dependencies`.
- **No virtualization by default.** Lists of a few thousand rows render whole
  (find-in-page works); measure before changing that.

Further reading, not vendored: Vercel's `react-best-practices`
(`vercel-labs/agent-skills`) for its client re-render rules only; skip its `server-*`,
hydration, `next/dynamic` and SWR rules, which do not apply to this SPA. Don't load
`webapp-testing`: `dev.sh` and agent-browser cover it.

## Visual rules DESIGN.md does not state

- Style in the component's CSS Module through module classes and RAC state attributes
  (`[data-selected]`, `[data-hovered]`, `[data-pressed]`, `[data-disabled]`,
  `[data-expanded]`). Passing `className` replaces RAC's default class; never target
  `.react-aria-*` globally.
- Focus: `[data-focus-visible]` on RAC parts, `:focus-visible` on plain elements, both
  with `var(--focus-ring)`.
- Overlay motion maps `[data-entering]`/`[data-exiting]` to the `motion.css` classes, so
  the reduced-motion collapse applies.
- Use RAC's CSS variables (`--trigger-width`, `--tree-item-level`) rather than measuring
  in JS.
- Accent-coloured **text** on a tint or surface uses `--accent-ink`, never `--accent`.
- `DataTable` selection is ARIA-grid with per-row tab stops. Below the narrow breakpoint
  it stacks with label prefixes; prefer that to a bespoke media query.
- Screen-reader-only content uses `.visually-hidden`, never `display: none`.
- 375 px horizontal overflow is usually a padded 100 %-width element missing
  `box-sizing: border-box`, or a flex child missing `min-width: 0`.

## Output: the design brief

Before coding, write a short brief in the working message or the PR description:

```md
Design brief:
- Surface: <route/component/flow>
- User task: <what the researcher/steward is trying to do>
- Pattern: <existing view + lib/ui primitives + RAC components; ladder rung>
- Layout: <desktop and below-breakpoint structure>
- States: <loading/empty/error/selected/focus/disabled>
- State owner: <store / URL / local>
- Keyboard model: <roving/grid/tree; where focus goes after collapse, close, delete>
- Forks: <none, or the pattern/primitive/colour the system lacks — needs a decision>
- Verification routes: <dev.sh shot routes and states>
```

Check the brief against the closest existing view: same pattern, primitives, states and
names for the same things? A divergence, or a duplicate of an existing route, primitive
or data contract, is a design fork to surface with a recommendation. Then implement the
smallest coherent change that satisfies the brief.

## Verification

Write no component tests. Behavior is pinned black-box by the `flows` driver and the
existing pure-TS unit tests (AGENTS.md → "Testing policy").

1. `uv run --no-project scripts/gate.py frontend`: types, lint (Biome, Oxlint with the
   React rules and jsx-a11y, `lint:tokens`), unit tests, build and codegen drift check;
   none of it is visual. Narrower loop: that step's package scripts.

2. Render the changed view at the four widths:

   ```sh
   bash reg_webapp/dev/dev.sh --fixture-db shot --all <route>
   ```

   `dev.sh` prints its output directory on the first line. **Open** each PNG and read
   its `.snapshot.txt` (accessibility tree) and `.a11y.txt` (axe). Fix console errors
   and React or RAC dev warnings. `smoke` is the desktop catalog flow only. Use
   `REG_META_DB=<db_dir>` instead of `--fixture-db` when content matters.

3. Run `dev.sh --fixture-db flows <out-dir>`; every case must pass, with no `check(...)`
   weakened. A new flow behavior gets a driver scenario, not a component test.

4. Run `reg-webapp-design-reviewer` in a clean subagent; fix or explicitly dismiss each
   finding before declaring.

Attribution: adapted from Anthropic's Apache-2.0 `frontend-design` plugin concept
(LICENSE.txt); the rules above are specific to the Registry Research Toolkit.
