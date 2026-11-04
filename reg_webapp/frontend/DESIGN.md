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
  accent-band: "#bfe0d4"
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
  breakpoint-narrow: 60rem
  col-browse: 220px
  col-browse-max: 340px
  col-list: 200px
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
column covers the study years you set.

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

## Words

The site is a steward site (SWECOV now, IFAU next) and shows only what its steward
holds. Screens use the researcher's words, never the catalog's internal model
(maintainer 2026-11-04, #2066). The steward's own name, description and order
instructions come from `/api/context`, never from code. Some screens still use the old
words; the slices in `SPA-REDESIGN.md` bring them in line.

  | Say                                                              | Not                                    |
  | ---------------------------------------------------------------- | -------------------------------------- |
  | (nothing: everything shown is held)                              | held, holdings, held matches           |
  | (nothing: the timeline shows it)                                 | segments, census                       |
  | Column name                                                      | spelling                               |
  | Description                                                      | provider wording                       |
  | Years                                                            | coverage                               |
  | Years to add                                                     | add window                             |
  | Study years                                                      | study window                           |
  | Topics                                                           | tags                                   |
  | Group                                                            | groups, part of                        |
  | Notes on this data                                               | findings, data warnings                |
  | Row identifier                                                   | row keys                               |
  | Who is included                                                  | populations                            |
  | Table (in a project)                                             | source                                 |
  | Column, variable                                                 | binding                                |
  | Check                                                            | draft check, catalog check             |
  | Years not available                                              | period adjustments, clips              |
  | Ready to order, Has warnings, Has problems, Can't be ordered yet | valid, warnings, errors, order blocked |
  | Check did not run                                                | check failed, error, unchecked         |

**Type** is one list for every screen: Categorical, Numeric, Date, Date and time,
Identifier, Other (uncoded text is Other). It is shown as a pill with a glyph, with
years only when the type changes by period.

**Not shown at all:** role, coded, state, temporality, storage, unplaced releases,
source and provenance, timeless, implicit, cadence, edition, primary. A fact that is not
stated has no row. A yes/no fact (identifier, sensitive) shows only when yes.

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
  selected variable rows, the study-window band, the active segment. `accent-band` is
  one stop stronger and has one use: an add window that differs from the study window
  (see Time). `accent-soft` is the project banner's fill and the current lane in a
  graph. `accent-ink` is pine as text (in-project counts, window years on an axis).
  Never use pine for a verdict, a type or decoration; `ok` is a verdict, `accent` is
  ownership.
- **Status (`err`, `warn`, `info`, `ok` and their `-bg` tints):** the only other
  chromatic chrome, each paired with a glyph and text. `warn` also marks a definition
  change on a timeline. Never use a `-bg` tint as a text color, and never use a status
  hue for emphasis, selection, or brand.
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
  the `ProjectBanner` at the right. The field reads "Search registers, variables…".
  While it holds text, focused or not, it takes an `accent` border with an `accent-bg`
  halo, and its hint becomes the count of matching registers ("12 registers", naming its
  noun so it never reads as the palette's hits) in `accent-ink` with an `Esc` key hint;
  the key hints are `text-muted` on `surface-sunken`. There are no "Search" or "Project"
  nav items; the bar and the banner are those. The tab's title is the deployment's long
  name too. Its content shares the shell's `shell-max` and centring, so the banner sits
  over the detail pane rather than at the screen's edge. Where the row is tight the
  brand wraps and the search bar shrinks before the banner leaves the row. Below
  `breakpoint-narrow` the name would push the banner off the row, or take a row of its
  own, so there the brand is the steward's logo, with the full name as the link's
  accessible name (#1820, maintainer rulings 2026-10-23). Each steward supplies a logo
  (`stewards/<id>/logo.svg`), used in the compact header and as the favicon. The bar
  sticks to the top of the page above `breakpoint-narrow`; below it the logo and a
  "Search" button share a row, the banner takes the next, and both scroll away with the
  page. The button unfolds the field, which takes the row's whole width in place of the
  logo and the button, with focus in it; it stays open while it holds text, and folds
  back on a blur or an Escape while empty, focus returning to the button (maintainer
  ruling 2026-10-24). Below the breakpoint every text field is set at 1rem: iOS Safari
  zooms the page into a focused field under 16px and leaves it zoomed, cut off at the
  left.
- **Browse columns (`ColumnList`):** a steward deployment shows only what the steward
  holds, and never says so: everything shown is held, everything held is shown
  (maintainer 2026-11-04). The server does this, not the SPA: on a steward catalog it
  lists, searches and serves only what the steward holds, and every period it returns is
  the years held (crates/DESIGN.md). There is no scope control and no held or unheld
  wording anywhere in the SPA: it filters nothing by that, and reads no such field.
  Registers grouped under sticky provider headers, then the selected register's or
  group's variables, one name per row with no period under it (maintainer 2026-11-04),
  then the detail pane capped at `content-max`. The two columns grow with the viewport
  from `col-browse` and `col-list` to `col-browse-max` and `col-list-max`, and the whole
  shell is capped at `shell-max` and centred, so a wide screen gets wider lists, not a
  stranded pane. The variables column is always there: before a register is chosen it
  holds an empty state ("Choose a register to list its variables."). The two column
  headers share one height, so their rules line up. Every group is open by default; a
  header triangle collapses it, the column header offers one toggle that reads "Collapse
  all" while any group is open and "Expand all" once all are closed, and collapse state
  is remembered per user. A group's items are indented `spacing.3` from the group's
  label, not a full column (toggle and indent maintainer-decided 2026-10-22). A group
  header is the `label` level at weight 500 in `text-muted` on `bg`, its disclosure a
  small `text-faint` triangle turned −90° when closed, with no rule under it. Rows are
  selectable options, not links. On a variable or group page its register's row stays
  selected, and pressing it opens the register. Selection moves with arrow keys across
  columns, and selecting a row opens its page; type-ahead moves focus only. Left on a
  group header collapses it, Right expands it; Right on a register row moves to the
  variables column, and Left there moves back. Collapsing a group never changes the
  selection: the detail pane keeps its page, and keyboard focus moves to the group row.
  For a register the second column is flat, a concept group folded to one item (#303),
  its identifier and panel-key variables first and the rest in Swedish collation; on a
  group's page (or a member's, which redirects there) the same list stays and the
  group's row is selected. The members are lanes on that page, not rows in the column. A
  browse column fills the viewport below the top bar and scrolls its own list. Every
  item and group carries its in-project mark on the right when it has one. Below the
  single narrow breakpoint (`breakpoint-narrow`, 60rem: narrower, the two columns leave
  the detail pane under 400px, #2107) the columns stack, one at a time; the way back is
  the breadcrumb's first crumb, "Browse registers", drawn as a `default` button on its
  own line above the trail so it reads as an action, not a path segment (#1824). Every
  page's trail leads with it below the breakpoint (and only there: beside the columns it
  would repeat them), and it opens the register list; while the search holds text it
  reads "Show matching registers", since the list the search filters is off screen. Do
  not mint new breakpoints.
- **Search filters in place.** Text in the search bar filters both browse columns rather
  than opening a results page, and only them: the open register, provider or variable
  page always shows its whole content (#1829). A provider with no matching register is
  hidden until the filter clears (#1830); with none left at all the column says so.
  Matched text is underlined in `accent` at weight 600, a register that matched only
  through its variables says so on its own line under it ("matched on 9 variables", the
  count in `accent-ink`), the column's label becomes the count ("12 registers"), and the
  detail pane stays put. The variables column lists the open register's matches first,
  then every other matching register's below it in the register column's order (#1828):
  each under its name as a header, in the register column's group-row type with a
  `border` rule above, its first five matches (six, rather than a "1 more" row), then a
  "12 more in <register>" row that opens the register (where the whole list is). Those
  rows are variables, not folded into concept groups, and open in their own register on
  a click or Enter; the arrow keys pass them without opening, so walking the list does
  not move it to another register, and once one opens, focus returns to the column. When
  only other registers match, a line above them says the open one has none. Escape
  clears the filter and restores the collapse state from anywhere on the page, unless a
  dialog, popover, the palette or another text field has focus (#1827). The command
  palette (RAC `Autocomplete`) opens from Enter in the field or `⌘K`, for
  classifications and the app's places ("Go to the project", "Browse the data", "Go to
  the start page"); its results are a mixed list and carry a `TypeTag`. It is a list
  under the field with no field of its own: the top-bar field is its one input, so focus
  stays there, the arrow keys move through the rows, and typing goes on filtering the
  columns. A second field in the popover showed the same text twice (maintainer report
  2026-10-24). The 2026-10-22 ruling had the palette "seeded with the field's text"
  (maintainer-decided 2026-10-22). It still opens with that text, but the text no longer
  stays behind: choosing a row puts the browse filter back to its value from just before
  the palette opened, and folds the narrow field if that value is empty, so the brand
  comes back. This is a deliberate change from that ruling (Operator 2026-10-24). Focus
  leaving the field for anything outside the palette closes it. Below the breakpoint it
  is as wide as the unfolded field. Escape closes the palette first, then clears the
  text, then folds the narrow field; Tab or a press outside closes the palette.
- **Home (`/`):** after the design system's Cover, with no hero, cards or counters: the
  product name in `display` type, the deployment's description (`steward.json`
  `description`: what the site is and, for a deployment like SWECOV, what holds the
  data) in `text-muted`, with inline emphasis and `http(s)` links. The browse columns
  are beside it and the project box is in the top bar, so there is no link into either.
- **Detail pane:** breadcrumb path ("Browse registers" as a button below the breakpoint,
  then the ancestors' names in the UI face, `text-muted`: the provider, then the
  register, each a link, ending at the page's parent, since the `h1` names the page
  itself; maintainer Q5, 2026-11-04). The names come from reads the browse columns
  already make, so the trail fetches nothing of its own; an ancestor whose name has not
  arrived is left out, never shown as its slug. A group page's trail skips the group
  namespace, and a classification page has none. Then, on a register's or a variable's
  page the kind mark (its `TypeTag` with the glyph), `h1`, member-of line, facets as a
  labelled row, the actions row, then definitions, then the `Timeline`, then the list of
  variables or columns, "Notes on this data" (the findings, only when there are any;
  maintainer 2026-11-04), then source documents. A variable page has no source and
  provenance section; MCP keeps `evidence=true` (maintainer 2026-11-04). Padding
  `spacing.5`. A variable group page carries the Group kind mark and a member-of line
  naming its register, with constant facets shown once in the facets row. Members are
  read-only `LaneGraph` lanes on the page's axis, in API order: a lane is titled by its
  varying facets in curated axis order, with the member's name on the line under it, and
  by the name alone when no axis varies. A variable page's facts are Type,
  Classification, Sensitive (only when yes; an identifier is its Type pill), Topics and
  Group (maintainer 2026-11-04); role, coding, state, temporality, storage and period
  are not shown. Type is one `TypePill` per type its columns are written as, with the
  years inside only where it changes over time. The other facts are what its columns
  state: one value where every column states it alike, each value with its years where
  they differ, and no row where none states it, never "Not stated" down the list. The
  `Timeline` states the years and carries the columns, their tables and years, so there
  is no Years row, no tables list and no table of columns under it; value sets have
  their own panel, so the facts name only the classification. Where there is no curated
  description, the provider's wording is the Description: once when every period agrees,
  otherwise each wording under its years. It sits under the facts, above the `Timeline`.
  Timeline and facts read the one per-period list (`segments`): on a steward deployment
  its rows are the files and columns the steward has, over the years it has them. A
  table page leaves out a Row keys, Populations or Releases panel with nothing to list,
  and Timeless or Implicit when "No". Placement caveats are steward working notes, not
  shown until the steward curates them for readers. A register page has no tables table:
  its tables are FilterChips over the variable list, each with its period, a lens with
  OR within the selection, offered only when two or more tables deliver variables (the
  archived variant chips, maintainer 2026-11-03). A Tables definition row links every
  table's page by name. A register page has no "Unplaced releases" panel (maintainer
  2026-11-04). In steward mode a supplemental-only variable is selectable for a project
  like a reference variable: `order` and `validate` read the composed steward view
  (crates/DESIGN.md, "Three questions, three scopes").

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

- A **period as text** reads with an en dash for a range (`1990–2023`), its segments
  comma-spaced, and an open bound as "From 2019" or "Through 2023"; the `..` wire
  spelling (`1990..2023`) appears only where the page shows the project file's own value
  (maintainer 2026-11-03).

- The **study window** is the project's period, named "Study years" on screen
  (maintainer Q4, 2026-11-04), set in the project banner (which is the period picker).
  Most researchers pick one, from a start year to the latest data, and it is bounded by
  the viewed catalog's first and last year (`/api/context` `period_span`): the slider
  shows no data, so its track stays the catalog's span and early years stay reachable. A
  new project starts at 2015 to the catalog's last year (the whole span when the catalog
  ends before 2015); a window set while browsing carries over instead. It shades every
  `Timeline` and `LaneGraph` as an `accent-bg` band with `accent` bounds and
  `accent-ink` years on the axis, and it looks the same everywhere, the banner included:
  the bounds and the light shade are always drawn, also where an add window narrows
  inside it (#1910). With no project open there is no band. That **window band** is the
  one drawing of a period control: where the period is set (the banner's study window, a
  variable page's add window, a source's period on `/project`) the band is dragged by
  slim `accent` thumbs on its bounds, so it is clear what can be moved; all compose the
  same classes (`ui/WindowBand.module.css`), and a new period selector does too (#1823).
  On a coverage row (the add window, a source period) the two thumbs never sit closer
  than 16 px: a short window on a long axis pushes their faces apart to that distance,
  all of it inward at an end of the axis so both faces stay on the row, so a one-year
  window keeps both visible and grabbable, while the window's years and its band stay
  where they are (maintainer ruling 2026-10-23).

- The **add window** is a variable page's band, named "Years to add" on screen
  (maintainer 2026-11-04): the years the columns ticked there are added for (#1838). On
  a variable page the `Timeline`'s band is a selection, two thumbs on the coverage row
  bounded by the variable's availability. The thumbs move live while dragged (the band
  and the columns' in/out follow them) and the window is applied when they come to rest
  (on release, or on each key press); there are no year fields and no Apply (maintainer
  ruling 2026-10-22). Where the add window differs from the study window inside the
  variable's availability it is a second encoding over the study window's: the stronger
  `accent-band` between the thumbs, with no bounds of its own, running through every
  lane; the study window's shade and bounds stay drawn, so a narrower add window shows
  the study window is larger, and an add window that reaches past the study window
  extends the stronger band beyond its bounds. There is no legend. A "Clear period"
  ghost action above the drawing drops the period, and shows only while it differs;
  thumbs that come to rest on the study window (inside the availability) drop the period
  rather than write one equal to it. Only the drawn windows' years without availability
  are `info-bg` (below); the add window's fill never means a narrowing. While an add
  window is drawn the axis years are its bounds in `accent-ink`; the study window's
  bounds keep their rules, unlabelled. An add widens its source's period to the union
  and never narrows it, so the add window raises no warning; a column already in the
  project whose source years differ from the study window says so in its tooltip and
  accessible name, not in a line under the graph (#1833); one outside the window is
  still named there at `err`. A register or group pane draws the `Timeline` read-only.

- A **source period** is set on `/project`, on the source's card, where the source is
  drawn whole as the **source timeline**: a `LaneGraph` headed by the coverage row of
  the source's table, its period as the window band with the same thumbs, bounded by
  that table's coverage, and one lane per binding. Each delivered column is a span of
  the years that table delivers it. A column available for part of the period is a
  shorter span, not a finding. The card is headed by the table's register, by name, with
  the table's name as a link and "Years:" with the period's years under it; every label
  on the card names the table by its catalog name, as the page's findings do; the
  source's `name` (the file's join key) stands in only where the catalog has no name for
  the table. A stored period outside coverage widens the thumbs' bounds to their union;
  it is never clamped or rewritten on load. A comma-list period rests the thumbs on its
  outer years and a line under the drawing says a move makes it one span; a term period
  is shown as text with plain `LaneList` rows. A timeless source says "Unbounded", with
  no thumbs. A dated table with `_default` also says "Unbounded", but has thumbs seeded
  from table coverage so its missing period can be repaired; it writes only when a thumb
  rests. Every source card shares the page's one axis. The lanes are the card's columns:
  no list repeats them under the drawing (maintainer ruling 2026-10-23). Each lane's
  name carries a quiet ghost "Remove", named "Remove <column> from <table>", that asks
  first in a `Dialog` whose answer is the `danger` button and takes the column out of
  that one source. Removing the last binding asks to remove the source. Source removal
  also removes its panel memberships and any panels left without members; the dialog
  states this when affected. A ghost "Remove table" in the card header also asks first.
  Focus returns to the card heading after a column removal, or the Tables heading after
  a source removal.

  Each lane's below slot holds a labelled type `Select`, an optional classification pin,
  and its findings. Types use plain words: Identifier, Categorical, Numeric, Date, Date
  and time, Opaque. A type change drops subtype and format fields owned by other types,
  and retains the classification. The pin appears for a binding with a classification or
  a catalog ambiguity finding, using named versions from segments on this table
  overlapping the source period, plus "Not pinned" to clear it. A stored pin outside
  those candidates remains named and marked "not delivered in this period". Versions use
  a scrolling listbox, never chips. These controls also appear in plain rows for term
  periods.

- A variable whose **availability is narrower** than the window is information, not a
  warning: the uncovered part of the window is drawn `info-bg` on the coverage row. The
  picture says it, so there is no note or legend entry for it; the header names those
  years to a screen reader only (#1833).

- A **year without a delivery** is plain empty space on the `Timeline`, the `LaneGraph`
  and the `VariableRow`, for every register (#1832, maintainer ruling 2026-10-23): no
  hatch, no gap mark and no legend entry. Elections, censuses and other periodic
  deliveries read as delivered years with space between, which is all the catalog knows.

- The **axis** spans the data a page draws, not the catalog: from the first to the last
  year of that data, joined with the study window when one is set, rounded out to
  multiples of 5 (#1831). A register page's data is its variants' deliveries, a concept
  group's its members', a variable page's its columns and the lanes a succession
  reaches. Anything else drawn on the axis counts too: the active window, a source
  period as well as the study window. Data still delivered ends at the catalog's last
  year, and data whose start is unknown begins at its first, where that data is drawn; a
  span with no start at all (year-independent) begins at the axis start. A one-year axis
  widens by one step of 5, backward where forward would pass the catalog's last year. A
  page's drawings share its one axis, so a register's `VariableRow`s line up with its
  `Timeline`, and a page with several sources draws them on one axis.

- A **`Timeline`** is drawn on the page's axis, its coverage row captioned "Years" in
  the name column (dropped when the names stack, where the axis heads the row), with no
  header, no "Variable" column header and no legend (maintainer 2026-11-04); a screen
  reader hears the covered years and the years to add. Coverage is one ink box per
  delivered year, a `surface` hairline between boxes, empty where a year has no
  delivery. On a variable page it is one drawing with the columns (#1833, maintainer
  ruling 2026-10-23): it is drawn as a `LaneGraph` headed by the coverage row (axis,
  then coverage with the period thumbs, then lanes), so the band runs through every
  lane. The variable's columns are its lanes, one per table and titled by the table's
  name, labelled spans in the `ColumnChip` shape with a coding change as a `warn` mark.
  One span per column name on a table: a steward's yearly files of one table are the
  same column delivered in several files, one box over their years with a gap where a
  year is not delivered (maintainer 2026-11-04). A span's column name is never cut: it
  sits inside the widest delivered part when it fits there. After it, in muted mono,
  come the files the steward holds it in on a steward deployment, yearly files as year
  runs after their stem (`UtdAdr1 RTB_SaBo_Adress_2015–2020`, `RTB_2015–2017, 2019`),
  others as a list: cut short with an ellipsis where the span is narrow, and dropped
  where only a few characters would show. A span too narrow for the column name is a
  plain bar with the name beside it: to the right, to the left at the axis end, else in
  a gap between its deliveries, and only where no room is free over the bars at the
  track's end, never cut. Hovering or focusing a span shows its column, table and years
  at once in a RAC `Tooltip` with no delay. The variables a succession reaches are
  further lanes in time order, with their arrows. A line under the drawing says "Click a
  column to add it to your project."; with no project open it says "Create a project to
  add columns." beside a "Create a project" button, the top bar's action, since the top
  bar scrolls away below the breakpoint. There is no second graph on the page. While it
  loads the region is a `Skeleton`; a failed load is an error `StatusRow` in the region
  with Retry; a variable with no years says "No years recorded for this variable." in
  place of the lanes. Succession reaches one hop of read-only lanes, with arrows from
  predecessor to successor and each neighbour's name linking to its variable page.

- A **`VariableRow`** repeats its list's axis as a thin bar, so a variable that does not
  cover the window is visible before it is added. It is one read-only row of a
  register's variable list, never a table with column headers: the in-project dot and
  wash, the name in the text face as a link to the variable page, and the bar with the
  study window block. Assistive technology reads "Delivered <years>" and, when bound, ",
  in the project". Below `breakpoint-narrow` the bar sits on top, then the name with its
  dot (#1826). Adding starts from the variable page, where the coverage `LaneGraph`, add
  window and add bar show which tables and years are added.

- A **`LaneGraph`** draws a group or register as lanes (one per variable, in the group's
  member order), columns as spans, and relations as arrows: solid for succession, dashed
  for rename. A succession arrow between overlapping spans drops at the edge's effective
  year. A group lane's title joins its varying facet values with a comma, falling back
  to the variable's name, and its name opens the variable; its line under the name is
  plain words, never a slug: the register when it differs from the page's, the
  variable's name under a facet title, or what tells two lanes of one name apart (their
  column names); it also describes the link. A read-only neighbour lane still shows its
  columns' "in project" state. A span is a checkbox for the add bar, and hovering it
  highlights its edges. On `/project` a column's findings sit across its lane, under its
  spans. It shares the page's axis with the `Timeline`. It draws the study window's band
  and, over it, an add window that differs, as on the variable page, with the axis years
  on the add window's bounds when one is drawn. It has no legend: a span's tooltip names
  it, its accessible name carries its state, and each lane states its relations in words
  to a screen reader (maintainer 2026-11-04). Its add bar shows only while a change is
  staged. On a variable page the `LaneGraph` is the `Timeline` (above). Don't draw
  free-form node graphs; the time axis is the information.

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
  `motion-quick`, each with its `-out`); a component applies a class rather than writing
  its own keyframes, and inherits the reduced-motion collapse.

## Components

Behavior comes from React Aria Components (RAC, the sanctioned headless primitives
dependency); visuals come from each component's CSS Module reading semantic roles. The
target set, composed before anything new is styled: `Button`, `ProjectBanner`, `Search`,
`ColumnList`, `ColumnChip`, `TypeTag`, `TypePill`, `FilterChip`, `VariableRow`,
`Timeline`, `LaneGraph`, `StatusRow`, `Definitions`, `Dialog`, `Skeleton`, `EmptyState`.
The current `src/lib/ui/` primitives (`Panel`, `DataTable`, `Breadcrumbs`, …) are the
stepping stones: they keep reading the same roles until each is replaced, and must not
grow new visual language meanwhile.

- **Buttons:** `primary` is the pine fill with `accent-fg` text and there is at most one
  per pane; `default` is `surface` with a `border-strong` hairline; `ghost` has no box,
  a 2 px `accent` underline, and `surface-hover` on hover; `danger` is the `err` fill
  with `danger-fg` text for destructive actions only, with no hover change. One size
  (`body-sm`, weight 500, `spacing.2` / `spacing.3` padding). A disabled button takes
  `--opacity-disabled`. There is no success variant. A button says what happens: "Add
  column to project", never "Submit".
- **Select:** a labelled RAC `Select` with `Popover` and scrolling `ListBox`, on
  `surface` with a `border-strong` hairline and `sm` radius. Text uses `body-sm`; focus
  uses the shared ring. Options use plain names, and the overlay uses `--z-popover`.
  Type and classification controls compose this shared primitive.
- **AddBar:** the shared sticky bottom bar for staged columns on the variable page. Its
  one `primary` button reads "Add N columns to project". The caller omits it when
  nothing is selected.
- **ProjectBanner:** the project's presence in the top bar — name, study window, source
  and column counts, the verdict, save state — on `accent-soft` with a 1 px `border`
  hairline and a 3 px `accent` leading bar. The name is `h3`, the counts `mono` ("1
  table, 2 columns"). The verdict is the project page's, in the same words (see Project
  page), and leads with its glyph: "✓ Ready to order" in `ok`, "▲ Has warnings" in
  `warn`, "✕ Has problems" or "✕ Can't be ordered yet" in `err`; not yet checked or in
  flight it is a plain `text-muted` "Checking" with no glyph; a draft with no tables yet
  is the same plain "Empty", never an error, since nothing has gone wrong. A check that
  did not run (a rate limit, the network) is the same plain "Check did not run", never
  "Has problems": it says nothing about the project. Its slot is as wide as the widest
  verdict, so the banner keeps one size while the check runs. A verdict /project
  explains (Has warnings, Has problems, Can't be ordered yet with their findings, Check
  did not run with Retry) is a link, in its status hue with the link's `accent` rule, to
  the verdict on /project, which takes focus; Ready to order and Checking stay plain
  text. Unsaved changes are a quiet `mono` `text-muted` "unsaved", never a status hue.
  The study window, labelled "Study years", is the period picker: an inline two-thumb
  RAC `Slider` (no popover) bounded by the viewed catalog's first and last year, drawn
  as the Timeline's study-window band (see Time: `accent-bg` between `accent` bounds,
  the thumbs on those bounds) on a `surface` strip, its years an `accent-ink` `mono`
  readout, with a "Clear" ghost action; the label, the readout and Clear keep one line.
  With no project open it is the same box, empty and untinted with a `border` leading
  bar, holding a `default` "Create a project" Button, which starts a project and opens
  it, and nothing in the catalog is tinted.
- **Findings** (titled "Notes on this data" wherever they are listed, maintainer
  2026-11-04) are `StatusRow`s in their level's tone, set under what they concern: a
  column's under its lane, a source's at the top of its card. Each names its place in
  the catalog's words, never an FQID. Its title holds the stable code in a delayed
  `Tooltip` (`mono`, press to copy); no code chip on the page. Under the project's
  verdict, a finding no card holds (a file that fails the structure check, a finding
  about the whole project) is listed in full, and each warning or problem a card holds
  has a plain link that scrolls to it and focuses it; a card's notes (level info) change
  no verdict and get no link.
- **Project page** (`/project`, maintainer ruling Q1, 2026-11-04): the `h1` "Project"
  with no breadcrumb repeating it, the name field, then "Download project_data.json",
  "Open a project file…" and a ghost "Discard"; opening over a project and discarding
  ask first in a `Dialog`. Under them, the steward's `order_instructions` from
  `/api/context` as sent (its own line breaks, no markdown, never text from code), then
  the verdict, "Tables" (one card per table) and "What will be delivered". There is one
  check and no button for it: WASM on every edit, then `order` 350 ms after the last
  edit WASM accepted. The verdict is one `StatusRow`: "Ready to order", "Has warnings"
  with the count, "Can't be ordered yet" with the count of problems, or "Has problems"
  for a file that fails the structure check, whose problems are listed in full under it;
  a check that did not run is an `err` row with Retry. While a newer check runs, the
  verdict, its list and "What will be delivered" keep the previous check's answer (not a
  check that did not run) with `aria-busy`; a `Skeleton` shows only when there is none,
  or when a table or column was added, removed or moved since, so the earlier findings
  would sit on the wrong card. "What will be delivered" is the order the catalog would
  serve, shown only when the project can be ordered: per table its name and years, each
  column as a `ColumnChip` (with its years only where they differ from the table's)
  beside the variable's name, "Years not available" per column the period asks more of
  than the table has, and the "Row identifier" columns. There is no order-file download:
  the steward's instructions say how to order. With no project the page is one
  `EmptyState`, "No project yet", with "Create a project" and "Open a project file…".
  Below `breakpoint-narrow` a column's row on a card stacks: its name and Remove, its
  spans, then Type.
- **ColumnList:** a `label` header that names whose items they are ("Variables in
  Aktivitetsstöd", on a group page too, where the group's folded row is selected; one
  line, truncated with an ellipsis, the whole of it as hover text) (#1825), then items
  on `bg`, one selected (2 px `accent` bar on `surface-sunken` at weight 600; the
  column's one bar translates between items); in the first column, items grouped under
  sticky collapsible group headers. A row's focus ring is drawn inward
  (`--focus-ring-inset`, the same accent line and halo) so the scroll box and a sticky
  row never clip it. An item or group with something in the project carries a 6 px
  `accent` dot and the count in `accent-ink` `mono` — the only numbers in the list.
  While the search bar holds text, the list shows its filter state (see Layout).
- **ColumnChip:** the one treatment for a column name (see Typography). Three states:
  outlined, in project (`accent` fill), outside the study window (`viz-span-out`,
  `text-muted`: `text-faint` fails AA for chip text, so the faint stroke carries the
  signal). The same shape as a lane span, so the two read as one object.
- **TypePill:** a column's type: a 12 px glyph in `text-muted` and the word in a
  `border` hairline `sm` box on `surface`, 0.75 rem, line-height 1.4, padding 0 6 px,
  with its years in micro mono `text-muted` only where the type changes over time. The
  words are the binding types: Identifier, Categorical, Numeric, Date, Date and time,
  Other (uncoded text is Other; maintainer 2026-11-04). No hue: the type is a fact, not
  a node kind, so it never borrows a `TypeTag` colour.
- **TypeTag:** the five `cat-*` hues, in mixed lists, and as the kind mark over a
  register's or a variable's page title, where it leads with a glyph in its ink (a table
  for a register, one filled column of it for a variable). A group page's kind mark has
  no glyph. The label is fixed per type (Register, Variable, Code list, Classification,
  Group) and a caller passes only the type. 0.75 rem, line-height 1.4, padding 1 px 7
  px, `sm` radius. A palette row shows a name and its `TypeTag`, never an identifier
  (maintainer Q5, 2026-11-04).
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
- **Source documents:** omit the panel until rows arrive, or when documentation is
  unavailable. Group only parsed sections by source URL; a shared group starts
  collapsed, with its whole URL heading as the disclosure trigger, ellipsized with full
  text on hover. Filtering holds matching groups open. Show kind and licence once per
  group, without section sizes or counters. PDFs stay individual links to the inline
  file endpoint, with their size. A source link reads "Source", sized to its text with
  the URL on hover; a missing URL adds no link or placeholder. Parsed sections expand on
  demand as safe text with emphasis and paragraphs; preserve non-Markdown backslashes
  and keep pipe tables as text lines. Omit a first paragraph only when it exactly
  repeats the section title after trimming. Subjects use catalog names, skip value sets
  and the current page. Only register pages with parsed sections offer a title filter,
  with a count only while filtering. Documents never have hash routes or invented
  titles.
- **Empty and loading states:** `EmptyState` is a centred, muted message: a title at the
  `h3` weight and size in `text`, a `body-sm` description, and an optional `default`
  Button that names the next action ("Browse registers"). The title states what is
  empty; the next step is the description and the button, never packed into the title.
  `Skeleton` is a `surface-sunken` line or a 3 rem block; content replaces it in place.
  It exposes `aria-busy="true"`, and respects reduced motion. A loading region carries
  no "Loading…" line beside it. A source card's timeline region shows one info
  `StatusRow`, "Loading years…", in place of the graph while it loads.

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
  sets, projects, study years) and keep an action's name the same through its whole
  flow. Don't apologize in errors or leave them vague.
