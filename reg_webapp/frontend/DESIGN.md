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
  danger-fg: "{colors.white}"
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
  cat-var-ink: "#404086"
  cat-code-ink: "#835b16"
  cat-class-ink: "#683677"
  cat-group-ink: "#285b36"
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
  content-max: 80rem
  breakpoint-narrow: 48rem
  col-browse: 260px
  col-browse-max: 340px
  col-list: 230px
  col-list-max: 300px
  shell-max: 120rem
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
    textColor: "{colors.danger-fg}"
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
  status-row-error:
    backgroundColor: "{colors.err-bg}"
    textColor: "{colors.err}"
    rounded: "{rounded.md}"
    typography: "{typography.body-sm}"
    padding: 6px 10px
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
set for verdicts, the node-type hues where types actually vary, the edge colours inside
graphs. It never decorates.

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

- **Sheet and surfaces:** `bg` is the canvas and the browse columns, `surface` where
  work happens (the detail pane, inputs, chips, dialogs), `surface-sunken` the selected
  item in a browse column and inset wells, `surface-hover` every hover.
- **Ink (`text`, `text-muted`, `text-faint`):** `text` for everything read; `text-muted`
  for labels, descriptions and unselected list items (6.4:1 on white) and out-of-window
  chips; `text-faint` only for things that may recede (hints), never for text that must
  be read (3.2:1, below WCAG AA for text).
- **Rules:** `border` is the hairline between rows and columns. `border-strong` is an
  ink hairline and means structure: the rule above a section, a default button's border,
  a column chip's stroke, the axis of a graph.
- **Pine, the ownership colour (`accent` and its stops):** used for exactly one idea —
  this is yours, or this is where you are. Primary button fill (`accent-fg` text), the
  bar and count on a column item or group that has something in the project, the
  selected row's bar, the focus ring, the study-window bounds. `accent-bg` is the wash:
  selected variable rows, the study-window band, the active segment. `accent-soft` is
  the project banner's fill and the current lane in a graph. `accent-ink` is pine as
  text (in-project counts, window years on an axis). Never use pine for a verdict, a
  type or decoration; `ok` is a verdict, `accent` is ownership.
- **Status (`err`, `warn`, `info`, `ok` and their `-bg` tints):** the only other
  chromatic chrome, each paired with a glyph and text. `warn` also marks a definition
  change on a timeline; `warn-hatch` draws a delivery gap as a hatch. Never use a `-bg`
  tint as a text color, and never use a status hue for emphasis, selection, or brand.
- **Categorical node type (`cat-reg` teal, `cat-var` indigo, `cat-code` gold,
  `cat-class` plum, `cat-group` moss):** appears only in a MIXED list — palette results,
  mentions, a project's sources — as a `TypeTag`. Fill and border take the raw hue at a
  10 % tint; label text takes the `-ink` stop so every type label clears AA. In a list
  where every item is the same type (registers under Registers, variables within a
  group) there is no type mark at all.
- **Graph marks:** `viz-edge-succession` (ink, solid) is a definition change;
  `viz-edge-rename` (muted, dashed) is a rename. Spans are `viz-span` with a
  `border-strong` stroke; outside the study window they take `viz-span-out` and
  `text-muted`. `viz-edition` cuts edition ticks into a coverage bar.

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
`accent` underline, and text a search filter matched (see Layout). **Links** are `text`,
never pine, with a 1.5 px `accent` bottom border (a bottom border, not an underline) and
`surface-hover` on hover; the member-of line sets them at weight 600. Pine text
(`accent-ink`) is for what is in the open project, not for links. Line length stays
under 60 characters for prose in a pane; tables, lanes and identifiers are exempt.
Swedish text with å/ä/ö is the norm, so both families ship the latin and latin-ext
subsets.

## Layout

The shell is a top bar and two browse columns beside a detail pane, not a rail.

- **Top bar:** brand (the deployment's long name in `h3`, one link home, unadorned on
  hover; without a deployment it is "Registry Research Toolkit"), one search bar (a
  filter over the lists; `Meta/Ctrl+K`, shown as `⌘K` on macOS, opens the palette), and
  the `ProjectBanner` at the right. The field reads "Search registers, variables,
  codes…". While it holds text, focused or not, it takes an `accent` border with an
  `accent-bg` halo, and its hint becomes the match count in `accent-ink` with an `Esc`
  key hint; the key hints are `text-muted` on `surface-sunken`. There are no "Search" or
  "Project" nav items; the bar and the banner are those. Its content shares the shell's
  `shell-max` and centring, so the banner sits over the detail pane rather than at the
  screen's edge. Where the row is tight the brand wraps and the search bar shrinks
  before the banner leaves the row. From `breakpoint-narrow` to 60rem the name would
  still push the banner off the row, so there the brand is its initials, set in its own
  type (the brand has no logo), with the full name as the link's accessible name (#1820,
  maintainer ruling 2026-10-23). This range is the top bar's own fit, not a layout
  breakpoint. It sticks to the top of the page above `breakpoint-narrow`; below it the
  brand, the bar and the banner take a row each and scroll away with the page.
- **Browse columns (`ColumnList`):** registers grouped under sticky provider headers,
  then the selected register's or group's variables, then the detail pane capped at
  `content-max`. The two columns grow with the viewport from `col-browse` and `col-list`
  to `col-browse-max` and `col-list-max`, and the whole shell is capped at `shell-max`
  and centred, so a wide screen gets wider lists, not a stranded pane. The variables
  column is always there: before a register is chosen it holds an empty state ("Choose a
  register to list its variables."). The two column headers share one height, so their
  rules line up. Every group is open by default; a header triangle collapses it, the
  column header offers one toggle that reads "Collapse all" while any group is open and
  "Expand all" once all are closed, and collapse state is remembered per user. A group's
  items are indented `spacing.3` from the group's label, not a full column (toggle and
  indent maintainer-decided 2026-10-22). A group header is the `label` level at weight
  500 in `text-muted` on `bg`, its disclosure a small `text-faint` triangle turned −90°
  when closed, with no rule under it. Rows are selectable options, not links. On a
  variable or group page its register's row stays selected, and pressing it opens the
  register. Selection moves with arrow keys across columns, and selecting a row opens
  its page; type-ahead moves focus only. Left on a group header collapses it, Right
  expands it; Right on a register row moves to the variables column, and Left there
  moves back. Collapsing a group never changes the selection: the detail pane keeps its
  page, and keyboard focus moves to the group row. For a register the second column is
  flat, a concept group folded to one item (#303), its identifier and panel-key
  variables first and the rest in Swedish collation; inside a group (its page or a
  member's) it lists the group's members under `mono` facet headers, one per value of
  the group's first axis. A browse column fills the viewport below the top bar and
  scrolls its own list. Every item and group carries its in-project mark on the right
  when it has one. Below the single narrow breakpoint (`breakpoint-narrow`) the columns
  stack, one at a time; the way back is the breadcrumb's first crumb, "Browse
  registers", drawn as a `default` button on its own line above the trail so it reads as
  an action, not a path segment (#1824). Every page's trail leads with it below the
  breakpoint (and only there: beside the columns it would repeat them), and it opens the
  register list; while the search holds text it reads "Show matching registers", since
  the list the search filters is off screen. Do not mint new breakpoints.
- **Search filters in place.** Text in the search bar filters both browse columns rather
  than opening a results page, and only them: the open register, provider or variable
  page always shows its whole content (#1829). A provider with no matching register is
  hidden until the filter clears (#1830); with none left at all the column says so.
  Matched text is underlined in `accent` at weight 600, a register that matched only
  through its variables says so on its own line under it ("matched on 9 variables", the
  count in `accent-ink`), the column header states how many registers match, and the
  detail pane stays put. Escape clears the filter and restores the collapse state from
  anywhere on the page, unless a dialog, popover, the palette or another text field has
  focus (#1827). The command palette (RAC `Autocomplete`) opens from Enter in the field
  or `⌘K`, seeded with the field's text (maintainer-decided 2026-10-22), for codes,
  classifications and actions; its results are a mixed list and carry a `TypeTag`.
- **Home (`/`):** after the design system's Cover, with no hero, cards or counters: the
  product name in `display` type, the deployment's description (`steward.json`
  `description`: what the site is and, for a deployment like SWECOV, what holds the
  data) in `text-muted`. The browse columns are beside it and the project box is in the
  top bar, so there is no link into either.
- **Detail pane:** breadcrumb path ("Browse registers" as a button below the breakpoint,
  then `mono` slugs in `text-muted`), on a register's or a variable's page the kind mark
  (its `TypeTag` with the glyph), `h1`, member-of line, facets as a labelled row, the
  actions row, then definitions, then the `Timeline`, then the list of variables or
  columns. Padding `spacing.5`.

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

- The **study window** is the project's period, set in the project banner (which is the
  period picker). Most researchers pick one, from a start year to the latest data, and
  it is bounded by the viewed catalog's first and last year (`/api/context`
  `period_span`). It shades every `Timeline` and `LaneGraph` as an `accent-bg` band with
  `accent` bounds and `accent-ink` years on the axis. With no project open there is no
  band. That **window band** is the one drawing of a period control: where the period is
  set (the banner's study window, a variable page's source period) the band is dragged
  by slim `accent` thumbs on its bounds; both compose the same classes
  (`ui/WindowBand.module.css`), and a new period selector does too.
- A **source period** narrows the window for one variable. It stays allowed but is the
  exception: on a variable page the `Timeline`'s band is a selection, two thumbs bounded
  by the variable's availability. The thumbs move live while dragged and the period is
  applied when they come to rest (on release, or on each key press); there are no year
  fields and no Apply, and a "Clear period" ghost action drops it (maintainer ruling
  2026-10-22). The band always shows the active window: the source period when it
  differs, else the project window. A source period that differs from the project window
  within the variable's availability says so in a `warn` `StatusRow`, "▲ Differs from
  the project window", with a "Reset" ghost action; a period narrowed only by
  availability does not. A register or group pane draws the `Timeline` read-only.
- A variable whose **availability is narrower** than the window is information, not a
  warning: an `info` "i" note in the `Timeline` header names the uncovered years, and
  the uncovered part of the window is an `info-bg` band. `warn-hatch` stays for a
  delivery gap inside coverage.
- A **`Timeline`** is one axis from the catalog's first year to its last, under a
  `label` header that says what the band is and states the covered years (years only: no
  edition count). Coverage is an ink bar with `viz-edition` ticks per delivered year, a
  `warn-hatch` hatch for a gap, and a `label` legend of the marks it shows. Labelled
  spans for columns, with a definition change under the same column name as two spans
  and a `warn` mark, belong to it too (not drawn yet).
- A **`VariableRow`** repeats the same axis as a thin bar, so a variable that does not
  cover the window is visible before it is added. It is one row of a register's variable
  list, never a table with column headers: the in-project dot, the name in the text
  face, the bar, and under them each delivery column as a `ColumnChip` with its tick (a
  real checkbox; a column already in the project shows its `in` chip and no tick). Below
  `breakpoint-narrow` the row stacks into one column: the bar on top, then the name,
  then the chips (#1826). While anything is ticked, a sticky add bar reads "Add N
  columns to project" as the pane's one `primary` button; with nothing ticked there is
  no bar.
- A **`LaneGraph`** draws a group or register as lanes (one per variable, ordered by
  facet), columns as spans, and relations as arrows: solid for succession, dashed for
  rename. A lane's title joins its facet values with a comma, and its name opens the
  variable; a span is a checkbox for the add bar, and hovering it highlights its edges.
  Its axis is the catalog's first year to its last, like the `Timeline`'s. The band is
  the active window: the source period when one is set, else the study window. Don't
  draw free-form node graphs; the time axis is the information.
- **Terms** read as runs: consecutive half-years join across the year ("HT2012–VT2019"),
  and the same half-year in consecutive years joins on the year ("VT2013–2019").

## Elevation & Depth

Depth is tonal, not cast. Paper under white surfaces, sunken wells one stop darker,
hairline borders. Only floating layers — dialogs, popovers, the command palette, the
drawer — cast the one shadow (`--elevation-float`); panes and banners cast nothing and
are separated by hairlines. An ink scrim sits behind dialogs and the drawer at
`--scrim-opacity` (0.4). The scrim is the one pre-composited colour (`--scrim` already
carries the opacity), because its overlay contains the dialog and an `opacity` on it
would dim the dialog too. No glows, gradients, blobs, textures or glass.

Two opacities, both named in `tokens.css` and not part of the front matter: a disabled
control is `--opacity-disabled` (0.5), and a segment outside the study window is
`--opacity-out-of-window` (0.45). Every other opacity literal is a defect.

Stacking runs bottom-up: in-page positioned bits (1-4), the mobile drawer's scrim
(`--z-drawer-scrim`, 55) and the drawer itself (`--z-drawer`, 60), popovers and the
palette (`--z-popover`, 65), then a modal dialog's scrim (`--z-dialog-scrim`, 70) and
the dialog (`--z-dialog`, 71). A dialog is portalled to `<body>` and must name a layer
above the shell — a modal the app can paint through is not a modal. The sticky top bar (10)
is the one in-page layer above 1-4 that has no named token.

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

Behavior comes from React Aria Components (RAC, the sanctioned headless primitives
dependency); visuals come from each component's CSS Module reading semantic roles. The
target set, composed before anything new is styled: `Button`, `ProjectBanner`, `Search`,
`ColumnList`, `ColumnChip`, `TypeTag`, `FilterChip`, `VariableRow`, `Timeline`,
`LaneGraph`, `StatusRow`, `Definitions`, `Dialog`, `Skeleton`, `EmptyState`. The current
`src/lib/ui/` primitives (`Panel`, `DataTable`, `Breadcrumbs`, `Tag`, `KeyValue`, …) are
the stepping stones: they keep reading the same roles until each is replaced, and must
not grow new visual language meanwhile.

- **Buttons:** `primary` is the pine fill with `accent-fg` text and there is at most one
  per pane; `default` is `surface` with a `border-strong` hairline; `ghost` has no box,
  a 2 px `accent` underline, and `surface-hover` on hover; `danger` is the `err` fill
  with `danger-fg` text for destructive actions only, with no hover change. One size
  (`body-sm`, weight 500, `spacing.2` / `spacing.3` padding). A disabled button takes
  `--opacity-disabled`. There is no success variant. A button says what happens: "Add
  column to project", never "Submit".
- **ProjectBanner:** the project's presence in the top bar — name, study window, source
  and column counts, the verdict, save state — on `accent-soft` with a 1 px `border`
  hairline and a 3 px `accent` leading bar. The name is `h3`, the counts `mono`. The
  verdict leads with its glyph: "✓ Valid" in `ok`, "▲ Warnings" in `warn`, "✕ Errors" or
  "✕ Order blocked" in `err`; unchecked or in flight it is a plain `text-muted`
  "Checking" with no glyph. Its slot is as wide as the widest verdict, so the banner
  keeps one size while validation runs. A verdict with findings (Warnings, Errors, Order
  blocked) is a link, in its status hue with the link's `accent` rule, to /project,
  where the first non-empty findings group scrolls into view and takes focus; Valid and
  Checking stay plain text. Unsaved changes are a quiet `mono` `text-muted` "unsaved",
  never a status hue. The study window is the period picker: an inline two-thumb RAC
  `Slider` (no popover) bounded by the viewed catalog's first and last year, drawn as
  the Timeline's window band (see Time) on a `surface` strip, its years an `accent-ink`
  `mono` readout, with a "Clear" ghost action; the label, the readout and Clear keep one
  line. With no project open it is the same box, empty and untinted with a `border`
  leading bar, holding a `default` "Create a project" Button, which starts a project and
  opens it, and nothing in the catalog is tinted.
- **ColumnList:** a `label` header that names whose items they are ("Variables in
  Aktivitetsstöd", or the group's name on a group page; one line, truncated with an
  ellipsis, the whole of it as hover text) (#1825), then items on `bg`, one selected (2
  px `accent` bar on `surface-sunken` at weight 600; the column's one bar translates
  between items); in the first column, items grouped under sticky collapsible group
  headers. A row's focus ring is drawn inward (`--focus-ring-inset`, the same accent
  line and halo) so the scroll box and a sticky row never clip it. An item or group with
  something in the project carries a 6 px `accent` dot and the count in `accent-ink`
  `mono` — the only numbers in the list. While the search bar holds text, the list shows
  its filter state (see Layout).
- **ColumnChip:** the one treatment for a column name (see Typography). Three states:
  outlined, in project (`accent` fill), outside the study window (`viz-span-out`,
  `text-muted`: `text-faint` fails AA for chip text, so the faint stroke carries the
  signal). The same shape as a lane span, so the two read as one object. The project's
  column list chips every column, its superseded names too, beside the variable's name
  in the text face; the FQID stays out of that list (it is in the findings' "Fix in
  catalog" links and the downloaded files). Each row's remove action is a quiet ghost
  "Remove" that asks first in a `Dialog`, whose answer is the `danger` button.
- **TypeTag:** the five `cat-*` hues, in mixed lists, and as the kind mark over a
  register's or a variable's page title, where it leads with a glyph in its ink (a table
  for a register, one filled column of it for a variable). The label is fixed per type
  (Register, Variable, Code list, Classification, Group) and a caller passes only the
  type. 0.75 rem, line-height 1.4, padding 1 px 7 px, `sm` radius. A palette row sets
  the node's FQID in `mono` `text-muted` beside its name.
- **FilterChip:** a real checkbox behind an `sm`-radius face (padding 4 px 9 px),
  `accent-bg` with an `accent` border and `accent-ink` text at weight 600 when on. A
  LENS: toggling narrows what is shown, never the project. A strip is the facet's
  `label` inline, then its chips, in one row with no box around it.
- **Timeline, VariableRow, LaneGraph:** see Time.
- **StatusRow:** the status tint as fill, the status foreground as text and a 1 px
  border (`err-border` for error), `md` radius, `body-sm`, padding 6 px 10 px. The glyph
  comes first and is `aria-hidden` (✕ error, ▲ warn, i info, ✓ ok), then plain sentence
  copy that says what went wrong and how to fix it. It rises in with `motion-rise-in`
  when it appears after the reader has acted, and never on page load. Every failed load,
  refusal and verdict row is a StatusRow. With no tone it is a bare live region that
  holds no height, so a first message lands in a region the page already had.
- **Dialog:** RAC `Modal` + `Dialog` (`role="alertdialog"`); `surface`, `md` radius,
  `--elevation-float`, over the scrim, on `--z-dialog-scrim` and `--z-dialog`. The title
  is an `h2` (`h2` size, weight 600, −0.01em tracking) and the body is `body-sm` in
  `text-muted`. Actions are right-aligned, Cancel first and the destructive one last as
  a `danger` Button. A click on the scrim does not answer it.
- **Empty and loading states:** `EmptyState` is a centred, muted message: a title at the
  `h3` weight and size in `text`, a `body-sm` description, and an optional `default`
  Button that names the next action ("Browse registers"). The title states what is
  empty; the next step is the description and the button, never packed into the title.
  `Skeleton` is a `surface-sunken` line or a 3 rem block; content replaces it with a
  crossfade (`motion-swap`) at `--duration-base`. It exposes `aria-busy="true"`, and
  respects reduced motion. A loading region carries no "Loading…" line beside it.

## Do's and Don'ts

- Do read roles only: `var(--text)`, never `var(--gray-1)`, never a literal. The
  `lint:tokens` script fails on literals and raw font stacks in component styles.
- Do keep pine for ownership and reserve every other chromatic color for a meaning
  (status, type, data). Do not tint headings or emphasis.
- Do pair every status with a glyph and text. Don't rely on hue.
- Do chip every column name and never a variable name. Do use mono for identifiers only.
  Don't set labels, headings or copy in mono.
- Do write labels and headings in sentence case at normal tracking. Don't use uppercase
  tracked eyebrows, numbered section markers, middle-dot meta strings, or trailing
  arrows on links and buttons.
- Do draw relations on the time axis. Don't draw a free-form node graph.
- Do compose `lib/ui` primitives and RAC behavior. Don't hand-roll a widget RAC covers
  (combobox, menu, dialog, popover, tooltip, slider, tabs, accordion, tree, listbox) or
  a near-duplicate of a primitive.
- Do keep one `primary` button per pane. Don't add counters, hero sections, decorative
  gradients, oversized cards, cards inside cards, or stock imagery.
- Do design for 375, 768, 1280 and 1920 px and verify with `dev.sh shot --all`. Don't
  add breakpoints or `display: contents` (it strips accessibility roles).
- Do keep focus visible via `:focus-visible` (or `[data-focus-visible]` on RAC parts)
  `{ box-shadow: var(--focus-ring) }` in each component's own CSS, and respect
  `prefers-reduced-motion`. Motion answers a user action at the three durations above;
  nothing animates on page load.
- Do name things by what a researcher recognizes (registers, variables, columns, value
  sets, projects, study window) and keep an action's name the same through its whole
  flow. Don't apologize in errors or leave them vague.
