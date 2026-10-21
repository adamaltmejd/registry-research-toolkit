---
version: alpha
name: Registry Research Toolkit
description: >-
  Design language for the reg_webapp catalog and project-authoring SPA. Ink on neutral
  paper, with one colour — pine — for what belongs to the open project; every other
  colour carries a meaning (status, node type, data). Schibsted Grotesk for UI text, IBM
  Plex Mono for every machine identifier. Dense, keyboard-first, light-first and
  dark-ready through semantic role tokens. Time is the axis every relation is drawn on.

colors:
# Primitive ramp — ink to paper. Components never read these; the semantic
# roles below reference them, so a re-tint or a provider theme is a role remap.
  gray-1: "#121412"
  gray-2: "#232527"
  gray-3: "#34373a"
  gray-4: "#474a4e"
  gray-5: "#5a605c"
  gray-6: "#6b706d"
  gray-7: "#8b918d"
  gray-8: "#b9bdba"
  gray-9: "#dadcd9"
  gray-10: "#e4e6e3"
  gray-11: "#f0f1ef"
  gray-12: "#fafaf8"
  white: "#ffffff"
  # Spec-required palette names, mapped onto the system.
  primary: "{colors.accent}"
  neutral: "{colors.gray-12}"
  # Semantic roles — each is the CSS custom property of the same name in
  # src/tokens.css (`colors.bg` → `--bg`).
  bg: "{colors.gray-12}"
  surface: "{colors.white}"
  surface-raised: "{colors.white}"
  surface-sunken: "{colors.gray-11}"
  surface-hover: "{colors.gray-11}"
  text: "{colors.gray-1}"
  text-muted: "{colors.gray-5}"
  text-faint: "{colors.gray-7}"
  border: "{colors.gray-9}"
  border-strong: "{colors.gray-1}"
  # Accent — pine, the ownership colour. One idea only: this is yours, or this
  # is where you are.
  accent: "#0f6e5c"
  accent-hover: "#0c5c4d"
  accent-fg: "{colors.white}"
  accent-bg: "#dcefe9"
  accent-soft: "#eaf5f1"
  accent-ink: "#0b5546"
  # Status — AA-cleared foreground stop + a separate fill tint each. Never a
  # text color from a `-bg` tint; never hue alone (a glyph and text always
  # carry the meaning).
  err: "#b3261e"
  err-bg: "#fbe9e7"
  err-border: "#f0a8a8"
  warn: "#7a5c00"
  warn-bg: "#f7f0d8"
  warn-hatch: "#c9a200"
  info: "#2f5f8f"
  info-bg: "#e6eef7"
  ok: "#1e7a3c"
  ok-bg: "#e3f3e8"
  # Categorical node type — fill/border hues for TypeTag in MIXED lists only;
  # label text uses the `-ink` stop (the hue 85% toward black).
  cat-reg: "#1f7a7a"
  cat-var: "#4b4b9e"
  cat-code: "#9a6b1a"
  cat-class: "#7a3f8c"
  cat-group: "#2f6b3f"
  cat-reg-ink: "#1a6868"
  cat-var-ink: "#404087"
  cat-code-ink: "#835b16"
  cat-class-ink: "#683677"
  cat-group-ink: "#285b36"
  # Facet-axis data palette — lane grouping and facet chips inside a LaneGraph.
  facet-axis-0: "#4f6f8f"
  facet-axis-1: "#6f7c3f"
  facet-axis-2: "#8a5f88"
  facet-axis-3: "#9a6a42"
  facet-axis-4: "#4f8074"
  facet-axis-5: "#6b6794"
  facet-axis-0-ink: "#3e5770"
  facet-axis-1-ink: "#576131"
  facet-axis-2-ink: "#6c4a6a"
  facet-axis-3-ink: "#785334"
  facet-axis-4-ink: "#3e645a"
  facet-axis-5-ink: "#535074"
  # Data-viz — marks of the time components (Timeline, VariableRow, LaneGraph).
  viz-span: "{colors.white}"
  viz-span-out: "{colors.border}"
  viz-edge-succession: "{colors.text}"
  viz-edge-rename: "{colors.text-muted}"
  viz-edition: "{colors.white}"

typography:
  display:
    fontFamily: Schibsted Grotesk
    fontSize: 2.75rem
    fontWeight: 700
    lineHeight: 1
    letterSpacing: -0.035em
  h1:
    fontFamily: Schibsted Grotesk
    fontSize: 1.625rem
    fontWeight: 700
    lineHeight: 1.1
    letterSpacing: -0.03em
  h2:
    fontFamily: Schibsted Grotesk
    fontSize: 1.125rem
    fontWeight: 600
    lineHeight: 1.25
    letterSpacing: -0.01em
  h3:
    fontFamily: Schibsted Grotesk
    fontSize: 0.9375rem
    fontWeight: 600
    lineHeight: 1.3
  body:
    fontFamily: Schibsted Grotesk
    fontSize: 0.875rem
    fontWeight: 400
    lineHeight: 1.5
  body-sm:
    fontFamily: Schibsted Grotesk
    fontSize: 0.8125rem
    fontWeight: 400
    lineHeight: 1.45
  label:
    fontFamily: Schibsted Grotesk
    fontSize: 0.6875rem
    fontWeight: 500
    lineHeight: 1.3
    letterSpacing: 0em
  mono:
    fontFamily: IBM Plex Mono
    fontSize: 0.92em
    fontWeight: 400
    lineHeight: 1.5
  column:
    fontFamily: IBM Plex Mono
    fontSize: 0.75rem
    fontWeight: 500
    lineHeight: 1.3
    letterSpacing: 0.01em
  axis:
    fontFamily: IBM Plex Mono
    fontSize: 0.625rem
    fontWeight: 400
    lineHeight: 1

rounded:
  sm: 3px
  md: 4px

spacing:
  1: 0.25rem
  2: 0.5rem
  3: 0.75rem
  4: 1rem
  5: 1.5rem
  6: 2rem
  rail: 16rem
  content-max: 80rem
  breakpoint-narrow: 48rem
  col-providers: 190px
  col-list: 230px
  lane-height: 58px

components:
  button-primary:
    backgroundColor: "{colors.accent}"
    textColor: "{colors.accent-fg}"
    rounded: "{rounded.sm}"
    typography: "{typography.body-sm}"
  button-default:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    typography: "{typography.body-sm}"
  button-ghost:
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    typography: "{typography.body-sm}"
  button-danger:
    backgroundColor: "{colors.err}"
    textColor: "{colors.white}"
    rounded: "{rounded.sm}"
    typography: "{typography.body-sm}"
  project-banner:
    backgroundColor: "{colors.accent-soft}"
    textColor: "{colors.text}"
    rounded: "{rounded.md}"
  column-chip:
    backgroundColor: "{colors.viz-span}"
    textColor: "{colors.text}"
    rounded: "{rounded.sm}"
    typography: "{typography.column}"
  column-chip-in-project:
    backgroundColor: "{colors.accent}"
    textColor: "{colors.accent-fg}"
    rounded: "{rounded.sm}"
    typography: "{typography.column}"
  panel:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
    rounded: "{rounded.md}"
  table-header:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text-muted}"
    typography: "{typography.label}"
  tag:
    rounded: "{rounded.sm}"
    typography: "{typography.body-sm}"
  banner-error:
    backgroundColor: "{colors.err-bg}"
    textColor: "{colors.err}"
    rounded: "{rounded.sm}"
---

# Registry Research Toolkit design language

This file follows the [DESIGN.md format](https://github.com/google-labs-code/design.md):
the YAML front matter is the normative token set, the prose says why the values exist
and how to apply them. It is the single design source of truth for
`reg_webapp/frontend/`. `src/tokens.css` implements it as CSS custom properties of the
same names, and `bun run lint:tokens` fails when the two disagree. Engineering rationale
for the SPA (routing, primitives' ARIA decisions, test setup) stays in
`reg_webapp/DESIGN.md`; on a conflict about *how something looks*, this file wins.

## Overview

The product is a catalog of Swedish register metadata and a workbench for assembling a
data order from it. The people using it are academic researchers and data stewards who
scan long lists of registers, variables, value-set codes and classifications, compare
periods and editions, and collect columns into a `project_data.json`. Their task is
reading, not being persuaded: they want density, exactness, stable positions, and
machine identifiers that look like machine identifiers.

**The project is the primary object.** Browsing exists to collect columns for a project,
so the interface tells you, everywhere, what you have already collected and whether a
column covers the study window you set.

The look is ink on neutral paper, with one colour for what is yours. The base is
monochrome: near-black text and rules on a near-white sheet. Colour appears only where
it carries meaning — pine for everything that belongs to the open project, the status
set for verdicts, the node-type hues where types actually vary, the facet and edge
colours inside graphs. It never decorates.

Personality: quiet, exact, archival. The nearest relatives are Linear and Observable,
not a marketing site and not a broadsheet. The app-level identity is spent on the
typeface pairing, the pine accent and the time components; the win condition for any new
view is that it is indistinguishable in language from the best existing view.

Planned axis: per-provider themes. A provider theme (SCB, Socialstyrelsen,
Försäkringskassan, …) is a remap of the `accent*` roles under a `[data-provider]`
attribute and nothing else. Do not pre-build it; design so that it stays that small.

## Colors

The neutral ramp `gray-1` … `gray-12` runs from ink to paper. Every role references a
ramp stop or a fixed hue; components read roles only, never ramp stops and never
literals. That discipline is what makes the dark theme (deferred) and provider themes
pure role remaps.

- **Sheet and surfaces:** `bg` is the canvas, `surface` where work happens (the detail
  pane, inputs, chips, dialogs), `surface-sunken` the selected item in a browse column
  and inset wells, `surface-hover` every hover.
- **Ink (`text`, `text-muted`, `text-faint`):** `text` for everything read; `text-muted`
  for labels, descriptions, the brand subtitle and unselected list items (6.4:1 on
  white); `text-faint` only for things that may recede — hints, out-of-window chips —
  never for text that must be read (3.2:1, below WCAG AA for text).
- **Rules:** `border` is the hairline between rows and columns. `border-strong` is an
  ink hairline and means structure: the rule above a section, a default button's border,
  a column chip's stroke, the axis of a graph.
- **Pine, the ownership colour (`accent` and its stops):** used for exactly one idea —
  this is yours, or this is where you are. Primary button fill (`accent-fg` text), the
  bar and count on a column item that has something in the project, the selected row's
  bar, the focus ring, the study-window bounds. `accent-bg` is the wash: selected
  variable rows, the study-window band, the active segment. `accent-soft` is the project
  banner's fill and the current lane in a graph. `accent-ink` is pine as text
  (in-project counts, window years on an axis). Never use pine for a verdict, a type or
  decoration; `ok` is a verdict, `accent` is ownership.
- **Status (`err`, `warn`, `info`, `ok` and their `-bg` tints):** the only other
  chromatic chrome, each paired with a glyph and text. `warn` also marks a definition
  change on a timeline; `warn-hatch` draws a delivery gap as a hatch. Never use a `-bg`
  tint as a text color, and never use a status hue for emphasis, selection, or brand.
- **Categorical node type (`cat-reg` teal, `cat-var` indigo, `cat-code` gold,
  `cat-class` plum, `cat-group` moss):** appears only in a MIXED list — search results,
  mentions, a project's sources — as a `TypeTag`. Fill and border take the raw hue at a
  10 % tint; label text takes the `-ink` stop so every type label clears AA. In a list
  where every item is the same type (registers under Registers, variables within a
  group) there is no type mark at all.
- **Facet axes (`facet-axis-0..5`):** group lanes and facet chips by dimension inside a
  `LaneGraph`. Data, not chrome, not status.
- **Graph marks:** `viz-edge-succession` (ink, solid) is a definition change;
  `viz-edge-rename` (muted, dashed) is a rename. Spans are `viz-span` with a
  `border-strong` stroke; outside the study window they take `viz-span-out` and
  `text-faint`. `viz-edition` cuts edition ticks into a coverage bar.

## Typography

Two families, both self-hosted woff2 under the SIL Open Font License, never a CDN:

- **Schibsted Grotesk** for all UI and display text. `display` and `h1` are weight 700
  with tight tracking (−0.035em / −0.03em) and are the only loud thing on a page; `h2`
  and `h3` at 600; `body` 400 at 14px; `body-sm` for dense UI; `label` for section
  labels and column headers in `text-muted`.
- **IBM Plex Mono** for every machine identifier: `mono` inline (FQIDs, slugs, years,
  counts, versions — relative, so it steps down inside any level), `column` for column
  names in chips and spans at weight 500, `axis` for axis years. Years and counts use
  tabular numerals. Mono is for identifiers only, never for labels or body copy.

Two kinds of name, never confused: a **variable name** is descriptive Swedish prose in
the text face ("Disponibel inkomst per konsumtionsenhet, familj"); a **column name** is
what the researcher types in code and is always a `ColumnChip` (`CDISPKE04H`): `column`
style, uppercase as delivered, in an outlined `sm`-radius chip, optionally with its
years inside. Never set a column name in the text face and never set a variable name in
mono or in a chip.

Hierarchy is size and weight. Labels and headings are **sentence case with normal
tracking**; nothing is underlined except a ghost button's label, which takes a 2 px
`accent` underline. Line length stays under 60 characters for prose in a pane; tables,
lanes and identifiers are exempt. Swedish text with å/ä/ö is the norm, so both families
ship the latin and latin-ext subsets.

## Layout

The target shell is a top bar and three browse columns, not a rail. The current
`AppShell` rail is a stepping stone (see Components) until it is replaced.

- **Top bar:** brand (name in `h3`, product line in `mono` `text-muted` under it), one
  search bar (the command bar, `Meta/Ctrl+K`, shown as `⌘K` on macOS), and the
  `ProjectBanner` at the right. There are no "Search" or "Project" nav items; the bar
  and the banner are those.
- **Browse columns (`ColumnList`):** providers at `col-providers`, then registers or a
  group's variables at `col-list`, then the detail pane capped at `content-max`.
  Selection moves with arrow keys across columns. Every column item carries its
  in-project mark on the right when it has one. Below the single narrow breakpoint
  (`breakpoint-narrow`) the columns stack, one at a time, with a back affordance. Do not
  mint new breakpoints.
- **Detail pane:** breadcrumb path in `mono`, `h1`, member-of line, facets as a labelled
  row, the actions row, then definitions, then the `Timeline`, then the list of
  variables or columns. Padding `spacing.5`.

Spacing is a 0.25 rem rhythm (`1`–`6`); gaps are set by the container that owns the gap,
never by margins on children. Density is tuned for scanning long lists: rows are 5–6 px
of vertical padding, panes 24 px, and there is no marketing whitespace. Every rendered
change is designed for 375, 768, 1280 and 1920 px wide; the page body never scrolls
horizontally.

**No counters.** A number appears only when it is actionable: how many of something are
in the project, how many results a filter leaves. Totals for their own sake ("6 367
variables") are left out.

## Time

Time is the axis every relation is drawn on.

- The **study window** comes from the project banner (which is the period picker) and
  shades every `Timeline` and `LaneGraph` as an `accent-bg` band with `accent` bounds
  and `accent-ink` years on the axis. With no project open there is no band.
- A **`Timeline`** draws coverage as an ink bar with `viz-edition` ticks per delivered
  edition, a `warn-hatch` hatch for a gap, and labelled spans for columns. A definition
  change under the same column name is two spans and a `warn` mark, never prose.
- A **`VariableRow`** repeats the same axis as a thin bar, so a variable that does not
  cover the window is visible before it is added.
- A **`LaneGraph`** draws a group or register as lanes (one per variable, ordered by
  facet), columns as spans, and relations as arrows: solid for succession, dashed for
  rename. Don't draw free-form node graphs; the time axis is the information.

## Elevation & Depth

Depth is tonal, not cast. Paper under white surfaces, sunken wells one stop darker,
hairline borders. Only floating layers — dialogs, popovers, the command palette, the
drawer — cast the one shadow (`--elevation-float`); panes and banners cast nothing and
are separated by hairlines. A 40 % ink scrim sits behind dialogs and the drawer. No
glows, gradients, blobs, textures or glass.

Stacking runs bottom-up: in-page positioned bits (1-4), the mobile drawer's scrim (55)
and the drawer itself (60), popovers and the palette (65), then a modal dialog's scrim (70)
and the dialog (71). A dialog is portalled to `<body>` and must name a layer above the
shell — a modal the app can paint through is not a modal.

## Shapes

Two radii: `sm` 3 px on controls (buttons, inputs, chips, column chips, lane spans) and
`md` 4 px on the banner, dialogs and status rows. Nothing is a pill, and radii are never
mixed on one element family. Selection and active marks are a straight bar on the
leading edge — 2 px `accent` on a column item, 3 px on a selected row — plus
`surface-sunken` or `accent-bg` behind it, never hue alone.

## Motion

Motion explains where things come from; state changes are near-instant.

- `--duration-quick` (120 ms, `--ease-standard`): hover fills, chip toggles, the focus
  ring.
- `--duration-base` (200 ms, `--ease-out` arriving / `--ease-in` leaving): popovers and
  the command palette (fade + 4 px rise from the trigger), disclosures (height), a
  status row appearing (fade + 4 px rise), the selection bar moving between column items
  (translate), skeleton-to-content (crossfade).
- `--duration-slow` (320 ms, `--ease-out` / `--ease-in`): the drawer (translate from the
  leading edge), a dialog (fade + scale from 98 %), a detail pane changing subject
  (crossfade).
- Nothing animates on page load. No springs, bounces, parallax or hover lifts. Under
  `prefers-reduced-motion: reduce` every transition becomes an opacity fade at
  `--duration-quick`, and the skeleton shimmer stops.
- The transitions are named once in `src/lib/ui/motion.css` (`motion-rise-in`,
  `motion-dialog-in`, `motion-fade-in`, `motion-slide-in`, `motion-disclose`,
  `motion-swap`, `motion-quick`, each with its `-out`); a component applies a class
  rather than writing its own keyframes, and inherits the reduced-motion collapse.

## Components

Behavior comes from Bits UI (the sanctioned headless primitives dependency); visuals
come from scoped CSS reading semantic roles. The target set, composed before anything
new is styled: `Button`, `ProjectBanner`, `Search`, `ColumnList`, `ColumnChip`,
`TypeTag`, `FilterChip`, `VariableRow`, `Timeline`, `LaneGraph`, `StatusRow`,
`Definitions`, `Dialog`, `Skeleton`, `EmptyState`. The current `src/lib/ui/` primitives
(`Panel`, `DataTable`, `Breadcrumbs`, `Tag`, `KeyValue`, `ConfirmDialog`, …) and
`AppShell` are the stepping stones: they keep reading the same roles until each is
replaced, and must not grow new visual language meanwhile.

- **Buttons:** `primary` is the pine fill with `accent-fg` text and there is at most one
  per pane; `default` is `surface` with a `border-strong` hairline; `ghost` has no box
  and a 2 px `accent` underline; `danger` is the `err` fill for destructive actions
  only. There is no success variant. A button says what happens: "Add column to
  project", never "Submit".
- **ProjectBanner:** the project's presence in the top bar — name, study window, source
  and column counts, warnings, save state — on `accent-soft` with a 3 px `accent`
  leading bar. The study window is the period picker. With no project open it is a
  single "Create a project" link and nothing in the catalog is tinted.
- **ColumnList:** a `label` header, then items, one selected (2 px `accent` bar on
  `surface-sunken` at weight 600). An item with something in the project carries a 6 px
  `accent` dot and the count in `accent-ink` `mono` — the only number in the list.
- **ColumnChip:** the one treatment for a column name (see Typography). Three states:
  outlined, in project (`accent` fill), outside the study window (`viz-span-out`,
  `text-faint`). The same shape as a lane span, so the two read as one object.
- **TypeTag:** the five `cat-*` hues, in mixed lists only.
- **FilterChip:** a real checkbox behind an `sm`-radius face, `accent-bg` with an
  `accent` border when on. A LENS: toggling narrows what is shown, never the project.
- **Timeline, VariableRow, LaneGraph:** see Time.
- **StatusRow:** the status tint as fill, the status foreground as text and border
  (`err-border` for error), a glyph first, plain sentence copy that says what went wrong
  and how to fix it.
- **Dialog:** Bits UI `AlertDialog`; `surface`, `md` radius, `--elevation-float`, over
  the scrim. A click on the scrim does not answer it.
- **Empty and loading states:** `EmptyState` names the next action; `Skeleton` exposes
  `aria-busy="true"` and respects reduced motion.

## Do's and Don'ts

- Do read roles only: `var(--text)`, never `var(--gray-1)`, never a literal. The
  `lint:tokens` script fails on literals and raw font stacks in any `<style>` block.
- Do keep pine for ownership and reserve every other chromatic color for a meaning
  (status, type, data). Do not tint headings or emphasis.
- Do pair every status with a glyph and text. Don't rely on hue.
- Do chip every column name and never a variable name. Do use mono for identifiers only.
  Don't set labels, headings or copy in mono.
- Do write labels and headings in sentence case at normal tracking. Don't use uppercase
  tracked eyebrows, numbered section markers, middle-dot meta strings, or trailing
  arrows on links and buttons.
- Do draw relations on the time axis. Don't draw a free-form node graph.
- Do compose `lib/ui` primitives and Bits UI behavior. Don't hand-roll a widget Bits UI
  covers (combobox, menu, dialog, popover, tooltip, slider, tabs, accordion) or a
  near-duplicate of a primitive.
- Do keep one `primary` button per pane. Don't add counters, hero sections, decorative
  gradients, oversized cards, cards inside cards, or stock imagery.
- Do design for 375, 768, 1280 and 1920 px and verify with `dev.sh shot --all`. Don't
  add breakpoints or `display: contents` (it strips accessibility roles).
- Do keep focus visible via `:focus-visible { box-shadow: var(--focus-ring) }` in each
  component's own CSS, and respect `prefers-reduced-motion`. Motion answers a user
  action at the three durations above; nothing animates on page load.
- Do name things by what a researcher recognizes (registers, variables, columns, value
  sets, projects, study window) and keep an action's name the same through its whole
  flow. Don't apologize in errors or leave them vague.
