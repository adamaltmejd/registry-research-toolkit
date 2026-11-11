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
  # Categorical — fill/border hues for the Pill: a node kind in MIXED lists and
  # on a page's pill row, plus an identifier and every other column type;
  # label text uses the `-ink` stop (the hue 85% toward black).
  cat-reg: "#1f7a7a"
  cat-var: "#4b4b9e"
  cat-table: "#2f6b3f"
  cat-id: "#8a4b2c"
  cat-type: "#4e6070"
  cat-reg-ink: "#1a6868"
  cat-var-ink: "#404086"
  cat-table-ink: "#285b36"
  cat-id-ink: "#754025"
  cat-type-ink: "#42525f"
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
  lane-heading-height: 28px
  lane-row-height: 36px

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
instructions come from `/api/context`, never from code.

  | Say                                                               | Not                                    |
  | ----------------------------------------------------------------- | -------------------------------------- |
  | (nothing: everything shown is held)                               | held, holdings, held matches           |
  | (nothing: the timeline shows it)                                  | segments, census                       |
  | Column name                                                       | spelling                               |
  | Description                                                       | provider wording                       |
  | Years                                                             | coverage                               |
  | Years to add                                                      | add window                             |
  | Study years                                                       | study window                           |
  | Topics                                                            | tags                                   |
  | Group                                                             | groups, part of                        |
  | Row identifier                                                    | row keys                               |
  | Classification, version                                           | class family, vintage                  |
  | Table (in a project)                                              | source                                 |
  | Column, variable                                                  | binding                                |
  | Check                                                             | draft check, catalog check             |
  | Years not available                                               | period adjustments, clips              |
  | Ready to download, Has warnings, Has problems, Can't download yet | valid, warnings, errors, order blocked |
  | Check did not run                                                 | check failed, error, unchecked         |
  | Database                                                          | linkage space, schema, category        |

**Type** is one list for every screen, the catalog's types in plain words: Whole number,
Decimal number, Text, Date, Date and time, Yes or no, Geometry, UUID, Binary, Identifier
(an identifier column is Identifier whatever it is stored as). It is shown as a pill
with a glyph, with years only when the type changes by period; a column whose type the
catalog does not state has no pill. The SPA never edits a project's type: a project
states one only to override the catalog, by editing the file (#2071, G13).

**Not shown at all:** role, coded, state, temporality, storage, unplaced releases,
source and provenance, timeless, implicit, cadence, edition, primary, populations,
releases, and a table's row keys (the catalog marks every identifier column as one;
Operator Q2, #2252). A fact that is not stated has no row. A yes/no fact (identifier,
sensitive) shows only when yes.

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
  ink hairline used only for a section rule inside the detail pane, a default button's
  or field's border, a column chip's stroke and the axis of a graph. A rule or divider
  in a list or browse column is always `border`, one shared rule (`.sectionRule`) in
  both columns (maintainer review of the classification pages, 2026-11-05, point 1).
- **Pine, the ownership colour (`accent` and its stops):** used for exactly one idea —
  this is yours, or this is where you are. Primary button fill (`accent-fg` text), the
  bar and count on a column item or group that has something in the project, the
  selected row's bar, the focus ring, the study-window bounds. `accent-bg` is the wash:
  selected variable rows, the study-window band, the active segment. `accent-band` is
  one stop stronger and has one use: an add window that differs from the study window
  (see Time). `accent-soft` is the project banner's fill and the picked version's lane
  on a classification's page; a variable's or group's lanes have no fill (maintainer,
  dogfood 2026-11-05, #2319). `accent-ink` is pine as text (in-project counts, window years on an axis).
  Never use pine for a verdict, a type or decoration; `ok` is a verdict, `accent` is
  ownership.
- **Status (`err`, `warn`, `info`, `ok` and their `-bg` tints):** the only other
  chromatic chrome, each paired with a glyph and text. A timeline marks no coding
  change ("Drop them", maintainer, dogfood 2026-11-05, #2319). Never use a `-bg` tint as a text color, and never use a status
  hue for emphasis, selection, or brand.
- **Categorical (`cat-reg` teal, `cat-var` indigo, `cat-code` gold, `cat-class` plum,
  `cat-group` moss; `cat-id` sienna, `cat-type` slate):** the `Pill` hues. A node kind's
  appears in a MIXED list — palette results, mentions, a project's sources — and as the
  kind pill on a page's pill row. `cat-id` is an identifier's Type pill and `cat-type`
  every other column type's: three categories, kind, identifier and type, one hue each
  (maintainer ruling 2026-11-10, quoted in PR #2391; the two hex values are proposed by
  the implementer). Fill and border take the raw hue at a 10 % tint; label text takes
  the `-ink` stop so every label clears AA. In a list where every item is the same type
  (registers under Registers, variables within a group) there is no type mark at all.
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
`accent` underline, and text a list filter matched (see Layout). **Links** are `text`,
never pine, with a 1.5 px `accent` bottom border (a bottom border, not an underline) and
`surface-hover` on hover. Pine text
(`accent-ink`) is for what is in the open project, not for links. Line length stays
under 60 characters for prose in a pane; tables, lanes and identifiers are exempt.
Swedish text with å/ä/ö is the norm, so both families ship the latin and latin-ext
subsets.

## Layout

The shell is a top bar and two browse columns beside a detail pane, not a rail.

- **Top bar:** brand (the deployment's long name in `h3`, one link home, unadorned on
  hover; without a deployment it is "Registry Research Toolkit"), one search bar (it searches and filters nothing; see Search), and
  the `ProjectBanner` at the right. The field reads "Search registers, variables…". Its
  hint is the `⌘K` key (`Meta/Ctrl+K`, shown as `⌘K` on macOS) while it is empty and
  the `Esc` key while it holds text, `text-muted` on `surface-sunken`; the field has no
  filtering state of its own, only the focus ring (#2398). There are no "Search" or "Project"
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
  Registers grouped under sticky provider headers, by name in Swedish collation, the
  order every list of a provider's registers uses (maintainer 2026-11-05), then the
  selected register's or group's variables, one name per row with no period under it
  (maintainer 2026-11-04), then the detail pane capped at `content-max`. Below the
  providers, under the column's section rule (`border`, the rule over each register's
  section of a provider's variables list), one "Classifications" row that never expands:
  selecting it opens the list of classifications, and the variables column then lists
  the classifications by name in Swedish collation, as it lists a register's variables,
  the open one selected (maintainer review of the classification pages, 2026-11-05,
  point 2). On a steward site the server lists only those the steward's variables use,
  and with none the row and its rule are not drawn. The column's filter box filters that list
  by name and slug (see Column filter). The two columns grow with the viewport from `col-browse`
  and `col-list` to `col-browse-max` and `col-list-max`, and the whole shell is capped
  at `shell-max` and centred, so a wide screen gets wider lists, not a stranded pane.
  The variables column is always there: before a register is chosen it holds an empty
  state ("Choose a register to list its variables."). The register column appears whole
  or not at all, never with a provider still loading (#2232). The two column headers
  share one height, so their rules line up. Every group is open by default; a header
  triangle collapses it, pressing a collapsed group's row opens it (and its page) but
  never closes it (maintainer 2026-11-05), the column header offers one toggle that
  reads "Collapse all" while any group is open and "Expand all" once all are closed, and
  collapse state is remembered per user. A group's items are indented `spacing.3` from
  the group's label, not a full column (toggle and indent maintainer-decided
  2026-10-22). A group header is the `label` level at weight 500 in `text-muted` on
  `bg`, its disclosure a small `text-faint` triangle turned −90° when closed, with no
  rule under it. Rows are selectable options, not links. On a variable or group page its
  register's row stays selected, and pressing it opens the register. Selection moves
  with arrow keys across columns, and selecting a row opens its page; type-ahead moves
  focus only, and focus arriving in a column from outside it (Tab, →) lands on a row but
  opens nothing. Left on a group header collapses it, Right expands it; Right on a
  register row, or on the open provider row of a provider's page, moves to the variables
  column, and Left there moves back. Collapsing a group never changes the selection: the
  detail pane keeps its page, and keyboard focus moves to the group row. On a provider's
  page the second column lists every variable of its registers, one section per register
  under the register's name, in that column's order (maintainer 2026-11-05); the filter
  box filters each section as it filters a register's list, name hits first, and drops
  a register with no match. Its rows open on a click, Enter or Space, not as the arrows pass (they
  re-root the list under the variable's register), and focus stays on the opened row.
  For a register the second column is flat, a concept group folded to one item (#303)
  that opens the group's page, sorted by name in Swedish collation with no special
  ordering, the group by its own name; while
  filtering, the group stands at its best-matching member's place, and a member matches
  on the group's name too. The row reads as the group's name, filtering or not, with no
  "4 matching": to a reader a group is a bigger variable (maintainer ruling 2026-10-22;
  #2398, maintainer 2026-11-11). A provider's list does not fold. On a group's page (or a
  member's, which shows that page) the same list stays and the group's row is selected.
  The members are lanes on that page, not rows in the column. A browse column fills the
  viewport below the top bar and scrolls its own list. Every item and group carries its
  in-project mark on the right when it has one. Below the single narrow breakpoint
  (`breakpoint-narrow`, 60rem: narrower, the two columns leave the detail pane under
  400px, #2107) the columns stack, one at a time; the way back is the breadcrumb's first
  crumb, "Browse registers", drawn as a `default` button on its own line above the trail
  so it reads as an action, not a path segment (#1824). Every page's trail leads with it
  below the breakpoint (and only there: beside the columns it would repeat them), and it
  opens the register list. Below the breakpoint the variables column is not drawn, and
  its filter box with it: a register's page lists its variables itself. The narrow mode
  that showed the column's matches under the trail while the top-bar text filtered
  (maintainer ruling 2026-11-05) went with that filter (#2398). Do not mint new
  breakpoints.
- **Search.** The top-bar field searches and filters nothing (#2398, maintainer
  2026-11-11): typing never changes the browse columns or the open page. Its results, the
  command palette (RAC `Autocomplete`), open under it as you type once the text reaches
  the search minimum (two characters, `SEARCH_MIN_QUERY_LENGTH`), and on Enter or `⌘K`
  (with an empty field, `⌘K` shows the places). This replaces the 2026-10-22 redesign's
  "Search filters in place" and its rules (#1827, #1828, #1830): the register column's
  filter and "matched on" lines, the other registers' matches under the open one, the
  count in the field and the page-wide Escape. The palette lists registers, variables
  (with their register) and the app's places ("Go to the project", "Browse the data", "Go
  to the start page"); classifications are browsed in the register column, not searched
  (Operator 2026-11-04, maintainer 2026-11-05). Each kind is one request for its first
  four hits, and the palette follows no cursor. On a steward site the answer holds two
  lists ranked apart, the catalog's `items` and the steward's own `steward.items`: the
  palette lists the catalog's hits, then the steward's, each in the server's order, a
  place listed once. The two ranks never merge; a list that pages follows each list's
  own cursor (`cursor`, `steward_cursor`) (#2110). Its results are a mixed list and
  carry a `TypeTag`. It is a list under the field with no field of its own: the top-bar
  field is its one input, so focus stays there and the arrow keys move through the rows.
  No row is active until they reach one, so Enter never opens a row whose hits are still
  loading (new rule, proposed by the implementer).
  A second field in the popover showed the same text twice (maintainer report
  2026-10-24). Choosing a row navigates and ends the search: the text clears and the
  narrow field folds, so the brand comes back (new rule, proposed by the implementer: with
  nothing filtered there is no state to put back, which the 2026-10-24 restore of the
  pre-palette filter did). Below the breakpoint it is as wide as the unfolded field.
  Escape closes the palette first, then clears the text, then folds the narrow field;
  Tab, a press outside or focus leaving the field closes it, and typing opens it again.
- **Column filter.** The variables column has a filter box of its own (#2398, maintainer
  2026-11-11), in a row under the column's header and outside its scroll box, so the two
  columns' headers keep one height: `FilterInput`, reading "Filter variables…" ("Filter
  classifications…" on the classification pages), with its "12 of 740" count of rows
  while it holds text. It filters that column only, never the open page, by name and slug
  with `rankFilter` (exact, then prefix, then other hits; a group's row counts once). A
  group stays when its own name or a member's matches; one kept only through a member
  reads as its name with nothing marked, so it comes after every row whose name matches
  (new rule, proposed by the implementer). The box is there for every list, however
  short, so the list does not shift as registers change (new rule, proposed by the
  implementer). It
  belongs to the list's root: opening another register, a provider or the
  classifications empties it, and opening a variable or group within the list keeps it.
  A provider's page and the classifications list get it too (new rule, proposed by the
  implementer: a provider lists every variable of its registers, 4,378 rows on
  /catalog/scb, and the palette does not search classifications); the register column
  gets none, since the search finds registers. Matched text is underlined in `accent` at
  weight 600. Escape in the box clears it, and on an empty box does nothing; no Escape
  elsewhere on the page touches it (new rule, proposed by the implementer: the
  page-wide Escape of #1827 cleared the top-bar filter, which is gone).
- **Home (`/`):** after the design system's Cover, with no hero, cards or counters: the
  product name in `display` type, the deployment's description (`steward.json`
  `description`: what the site is and, for a deployment like SWECOV, what holds the
  data) in `text-muted`, with inline emphasis and `https` links (Operator lane brief
  2026-11-10, PR #2391; any other scheme stays text). The browse columns are beside it
  and the project box is in the top bar, so there is no link into either.
- **Detail pane:** breadcrumb path ("Browse registers" as a button below the breakpoint,
  then the ancestors' names in the UI face, `text-muted`: the provider, then the
  register, each a link, ending at the page's parent, since the `h1` names the page
  itself; maintainer Q5, 2026-11-04). The names come from reads the browse columns
  already make, so the trail fetches nothing of its own; an ancestor whose name has not
  arrived is left out, never shown as its slug. A group page's trail skips the group
  namespace. A classification page's one ancestor is the list of classifications, so its
  trail is "Classifications", on a version address too: that address renders the family
  page, whose `h1` already names the family (slice 3, Q4). The list itself
  (`/catalog/class`) has no ancestors and lists every classification by name in Swedish
  collation, with its real name, its description (two lines, in `text-muted`) and its
  versions as a plain list of name and years, under part headings where it has parts
  (maintainer review 2026-11-05, point 3); a classification with one version shows only
  its years, since its version's name repeats its own, and a version named as its part
  links by its years alone under that heading (new rule, proposed by the implementer).
  There are no year bars: the classification's page draws them. Each version links to
  its address (new rule, proposed by the implementer). Then, on a register's,
  variable's, group's or classification's page one row of same-size pills under the
  breadcrumb: the kind ("Register", "Variable", "Variable group", each with its glyph),
  then on a variable's or group's page its Type pills; a classification's page has no kind pill
  (#2317). Then the `h1`, with no member-of line: the breadcrumb names the register
  (#2317). Then facets as a labelled row, the actions row, then definitions, then the
  `Timeline`, then the list of variables or columns, then source documents. A catalog
  page lists no build findings: the catalog does not carry them (maintainer 2026-11-06,
  #2335). A variable page has no source and provenance section; MCP keeps
  `evidence=true` (maintainer 2026-11-04). Padding `spacing.5`. A grouped variable has
  no page of its own (maintainer 2026-11-04): its address (from search, the palette, a
  project card or a note) renders its group's page, one route and one page, not a
  redirect, with that member's name focused and scrolled into view, unless focus is
  already elsewhere: opened from a browse column, focus stays on that column's row. A
  variable is in at most one group. A variable's page and a group's page are one page
  (maintainer, dogfood 2026-11-05, #2319): only the address (`/set/`) and the kind pill
  tell them apart. Both carry the pill row, the `h1`, a group's constant facets once in
  the facets row, the facts, the Description panel, the `Timeline`, the value sets and
  the source documents, every one over all the page's columns; a group's successions
  and documents are one read each at the group's ref, not one per member (#2232,
  2026-11-10). On a group page every
  member is a lane of the one `LaneGraph`, in API order: a lane is titled by its varying
  facets in curated axis order, with the member's name on the line under it, and by the
  name alone when no axis varies; under it only its description where it differs from
  the group's (two lines, "More" opens the rest). The lane grows to hold it, and its
  name and line wrap. It is a heading row with no spans: under it each table the member
  is delivered in is a row of its own, set in from the left, headed by the Table pill
  and the table's name as a variable page's table lanes are, also when the member has
  only one table ("on variable group pages where a lane can have many tables, the table
  name would go as a row heading in the leftmost column", and "Always one row per
  table", maintainer rulings, 2026-11-10, #2395). Its columns are spans to tick, with
  one coverage row, one "Years to add" and one add bar for the whole group. A succession
  neighbour outside the group is a read-only lane with its arrows; a relation between
  two members is an arrow between their table rows. The facts are Type, Classification,
  Sensitive (only when yes; an identifier is its Type pill) and Topics (maintainer
  2026-11-04); role, coding, state,
  temporality, storage and period are not shown. They sit in the page's head on both
  pages, never in a lane (#2319). Type is one `TypePill` per type the columns are
  written as, with the years inside only where it changes over time, in the pill row
  beside the kind pill rather than a definition row (#2317). The other facts are what
  the columns state: one value where every column states it alike, each value with its
  years where they differ, and no row where none states it, never "Not stated" down the
  list. Classification names the classification once, by its name, linking to its page,
  where the versions are; only a version the classifications list lacks is named by
  itself ("Family once", maintainer 2026-11-10, PR #2394 comment 6775423821: on the SWECOV Kommun group the members code
  28 versions of one classification). The `Timeline` states the years and carries the
  columns, their tables and years, so there is no Years row, no tables list and no table
  of columns under it; value sets have their own panel, so the facts name only the
  classification. The Description is the curated description; on a variable page
  without one it is the provider's wording: once when every period agrees, otherwise
  each wording under its years. It sits under the facts, above the `Timeline`, on both
  pages (new rule, proposed by the implementer: a group's description was prose under
  its `h1`). Timeline and
  facts read the one per-period list (`segments`): on a steward deployment its rows are
  the files and columns the steward has, over the years it has them. A table has no page
  of its own (maintainer, dogfood 2026-11-05, #2316): its address (a project card, the
  palette, a link) renders its register's page with that table's chip on, one route and
  one page, not a redirect; where no chip names it (it delivers no variable, or it is
  the register's only delivering table) the page shows the whole list. A
  classification's page shows its codes and its versions, and no variables (maintainer
  review 2026-11-05, point 5): the classification's name as `h1`, then Publisher and
  Description as definitions, only when stated. One unframed `LaneGraph` with no
  heading, as on the variable and group pages (maintainer ruling, 2026-11-05), draws the
  versions on the page's axis with one lane per part, named by the part in the group
  page's lane-name style, or one lane named by the classification where it has no parts
  (the approved plan). Each version is a span on its part's lane, with no name on it:
  its years sit on it where they fit, and where they do not its tooltip names the
  version and its years in the UI face (#2321). Succession reads as order along a lane,
  with no arrows, also where a version continues into another part (SUN 1996 into its
  three parts; maintainer, dogfood 2026-11-05, #2323). Lanes run in the order each part
  first starts. The page's axis reaches every version's start, beyond the catalog's
  years where needed (ICD-11), so every version has a span; an open-ended version's span
  still ends at the catalog's last year. The graph is the version picker at every width,
  with no `Select`, as a column is picked on the variable and group pages ("this should
  use the same component/design for classifications as for variables/columns on
  group/variable pages", and "Graph everywhere", maintainer ruling, 2026-11-05), under
  the hint "Click a version to see its codes." (new rule, proposed by the implementer,
  after the group page's hint). A panel titled "Codes in" and the picked version's name
  alone, without its years follows, so the version is named on screen and not only in a
  tooltip (new rule, proposed by the implementer): the picked version's list through
  `ValueSetCodes` (see value sets, under Time). There is no list of the codes changed
  since the version before (maintainer, dogfood 2026-11-05, #2323; a correspondence
  table is #795). The picked version is the one a version address names, else the
  latest version that has started by the catalog's last year (maintainer ruling,
  2026-11-05); it is local state, its part's lane on `accent-soft` and its span in the
  group page's selected-span style. Pressing a span picks its version (maintainer
  ruling, 2026-11-05), and a version address focuses that span (new rule, proposed by
  the implementer: picking does not change the URL, which would reload the page). Since
  one version is picked at a time, the spans are one radio group, not checkboxes: one
  tab stop, the arrow keys move between versions and pick them ("expose it as a radio
  group", maintainer comment on #2310, 2026-11-05). Placement caveats are steward
  working notes, not shown until the steward
  curates them for readers. A register page shows under its `h1` only its description,
  as prose: the timeline shows the years and the chips the tables, so there is no Years
  or Tables row (#2316). It has no tables table: its tables are FilterChips above the
  variable list, each with its period, a lens with OR within the selection, offered only
  when two or more tables deliver variables (the archived variant chips, maintainer
  2026-11-03). A register page has no "Unplaced releases" panel (maintainer 2026-11-04).
  In steward mode a supplemental-only variable is selectable for a project like a
  reference variable: `order` and `validate` read the composed steward view
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

  A period as text is years, never an ISO date or a period token (`LA1999`, `HT2019`):
  whole school years (1 July to 30 June) read as school years, `1999/2000` for one and
  `1984/1985–2023/2024` for a range (maintainer, #2315, dogfood 2026-11-05); anything
  else reads as the calendar years its first and last days fall in, so a 31 December
  snapshot is its year (`1968–2025`, not `1968-12-31–2025`; #2315, 2026-11-06). Stretches
  that meet at that grain read as one range (yearly snapshots `2010–2020`); stretches of
  different grains stay apart (`2018, 2019/2020`). A gap the
  server reports inside a year reads as that year too; a gap no whole year fills is the
  server's to stop reporting (maintainer, PR #2389, 2026-11-10). Text that states
  delivered years (a list row's, a lane's, a fact's) is read from the catalog periods,
  not from the drawing's year cells, so a school-year register reads `1972/1973–2023/2024`
  beside its bar (maintainer, PR #2389, 2026-11-10: fold). A source period on `/project`
  that the thumbs cannot write (a term, quarter, month or day, hand-written in the file)
  reads exactly, in words: `autumn 2019`, `spring 2020`, `2019 Q3`, `February 2020`,
  `15 October 2019` (maintainer, PR #2389, 2026-11-10). A period with no dated
  bound (a year-independent classification version, a timeless table or source) has no
  years text: a timeline draws it as an empty box and a list leaves its years out (#2323:
  "an unbounded version is an empty box in the timeline, with nothing on the list"); there
  is no "Unbounded", on `/project` either (maintainer, #2323, dogfood 2026-11-05: "Yes drop
  it there too").
  The label is display only: `renderIntervals` writes the project file.

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
  project whose source years differ from the study window says so in its accessible
  name, not in a tooltip (maintainer ruling, 2026-11-10, #2395) or a line under the
  graph (#1833); one outside the window is
  still named there at `err`. A register pane draws the `Timeline` read-only; a group
  pane draws it as the variable page does, a lane per member.

- A **source period** is set on `/project`, on the source's card, where the source is
  drawn whole as the **source timeline**: a `LaneGraph` headed by the coverage row of
  the source's table, its period as the window band with the same thumbs, bounded by
  that table's coverage, and one lane per binding. Each delivered column is a span of
  the years that table delivers it. A column available for part of the period is a
  shorter span, not a finding. The card is headed by the table's register, by name, with
  the table's name as a link and "Years:" with the period's years under it. Where two
  tables of one register would share that heading, the heading names its table on a
  muted line under the register: the table's own name, never an invented label
  (maintainer ruling, #2221). Every label on the card names the table by its catalog
  name, as the page's findings do; the source's `name` (the file's join key) stands in
  only where the catalog has no name for the table. A stored period outside coverage
  widens the thumbs' bounds to their union; it is never clamped or rewritten on load. A
  comma-list period rests the thumbs on its outer years and a line under the drawing
  says a move makes it one span; a term period is shown as text with plain `LaneList`
  rows. A timeless source has no "Years:" line and no thumbs. A dated table with
  `_default` has no "Years:" line either, but has thumbs seeded from table coverage so its
  missing period can be repaired; it writes only when a thumb rests. Every source card
  shares the page's one axis. The lanes are the card's columns: no list repeats them
  under the drawing (maintainer ruling 2026-10-23). Each lane's name carries a quiet
  ghost "Remove", named "Remove <variable> from <table>", that asks first in a `Dialog`
  whose answer is the `danger` button and takes the column out of that one source.
  Removing the last binding asks to remove the source. Source removal also removes its
  panel memberships and any panels left without members; the dialog states this when
  affected. A ghost "Remove table" in the card header also asks first. Focus returns to
  the card heading after a column removal, or the Tables heading after a source removal.

  Each lane's below slot holds an optional classification pin and its findings. There is
  no type control: an added column states no type, so the order reads the catalog's
  (#2071, G13). The pin appears for a binding with a classification or a catalog
  ambiguity finding, using named versions from segments on this table overlapping the
  source period, plus "Not pinned" to clear it. A stored pin outside those candidates
  remains named and marked "not delivered in this period". Versions use a scrolling
  listbox, never chips. These controls also appear in plain rows for term periods.

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
  year, and data whose start is unknown begins at its first, where that data is drawn. A
  year-independent span (no start and no end, a timeless table's) begins at the axis
  start on a lane (and ends at its end, never past it), so its column stays pickable; a
  `VariableRow` has no dated years for it, so it draws a bare track read as "No dated
  delivery." and does not count toward the axis. A one-year axis widens by one step of
  5, backward where forward would pass the catalog's last year. A page's drawings share
  its one year range, so they compare by year, not pixel for pixel: a `VariableRow`'s
  bar column and the `Timeline` each have their own gutters. A page
  with several sources draws them on one range. The axis of a `LaneGraph` (the
  `Timeline` and source timelines) and of a `VariableRow` list labels its
  first and last year, at every width (maintainer 2026-11-05), each on its own cell
  against the track's edge. On a `LaneGraph` a window bound whose label would touch an
  end's hangs away from that end, and where they still touch, the bound's label is
  dropped (the window's band still marks it); an end's label never moves or gives way.

- A **`Timeline`** is drawn on the page's axis, its coverage row captioned in the name
  column: by the variable's name on a variable page, which heads the drawing (#2319),
  and "Years" on a group page. Stacked, "Years" is dropped (the axis heads the row) and
  the variable's name sits over the drawing ("Caption + phone line", maintainer 2026-11-10, PR #2394 comment
  6775423821). No
  header, no "Variable" column header and no legend (maintainer 2026-11-04); a screen
  reader hears the covered years and the years to add. Coverage is one ink box per
  delivered year, a `surface` hairline between boxes, empty where a year has no
  delivery. On a variable page it is one drawing with the columns (#1833, maintainer
  ruling 2026-10-23): it is drawn as a `LaneGraph` headed by the coverage row (axis,
  then coverage with the period thumbs, then lanes), so the band runs through every
  lane. The variable's columns are its lanes, one per table, each titled by a "Table"
  kind pill (the `TypeTag` with its glyph) and the table's name, so a lane reads as one
  of the variable's tables, not as another variable (#2319); labelled spans in the
  `ColumnChip` shape. On a steward with several databases, the line under a table's
  name says which databases have it, by their `/api/context` names (#2343, #2344):
  "All databases" when every one has the same years, the names when those that have
  it share years, and each name with its years when they differ. The line wraps
  between databases, never inside a name or a range, and the table's name wraps
  with it. When the project's database lacks the table, the line opens "Not in
  <database>.", the visible reason its columns cannot be added. With one database
  there is no line. Once the
  project names a database, a span draws that database's years and adds only those;
  a column the database lacks keeps its own years drawn as outside the years to add
  (`viz-span-out`), cannot be picked, and its accessible name ends "Not in
  <database>." A column already in the project is blocked, as one outside the study
  years is, where the project's check finds it not in the database
  (`not_in_database`): that turns on its years, its other column names and its
  successor, so the page shows the check's verdict rather than its own (new rules, proposed by the implementer for
  #2343 and #2344).
  One span per column name on a table: a steward's yearly files of one table are the
  same column delivered in several files, one box over their years with a gap where a
  year is not delivered (maintainer 2026-11-04). A span's column name is never cut: it
  sits inside the widest delivered part when it fits there, with no list of the files
  it is delivered in: the lane already names the table (maintainer, dogfood 2026-11-05,
  #2321). A span too narrow for the column name is a plain bar with the name beside
  it: to the right, to the left at the axis end, else in a gap between its deliveries,
  and only where no room is free over the bars at the track's end, never cut; the bar
  carries the span's years where they fit. Stacked below the breakpoint, a lane's spans
  start just under its name. Hovering or focusing a span shows a RAC `Tooltip` with no
  delay only where the span's own text is cut off, which is its years (maintainer,
  dogfood 2026-11-05, #2321; the strict reading, maintainer ruling, 2026-11-10): no
  table name and no study-window note in a tooltip, and no table name on a span. The
  variables a succession reaches are further lanes in time order, with their arrows. A
  line under the drawing says "Click a column to add it to your project." while a
  project is open; with none there is no line and no button at any width, since the top
  bar has the action ("yes", maintainer, dogfood 2026-11-05, #2319). There is no second
  graph on the page. While it loads the region is a `Skeleton`; a
  failed load is an error `StatusRow` in the region with Retry; a variable with no years
  says "No years recorded for this variable." in place of the lanes. Succession reaches
  one hop of read-only lanes, with arrows from predecessor to successor and each
  neighbour's name linking to its variable page.

- A **`VariableRow`** repeats its list's axis as a thin bar (`ui/YearTrack`), so a
  variable that does not cover the window is visible before it is added. It is one
  read-only row of a register's variable list, or of a provider's register list (the
  register's years as one span), never a table row: the in-project dot and wash, the
  name in the text face as a link to its page, and the bar with the study window block.
  The list's one header is the bars' axis (`YearAxis`): its first year over the bars'
  start and its last over their end, in the `axis` type (maintainer 2026-11-05), with
  the list's name ("Variables", "Registers") on the same row over the names as the
  section's heading, so the panel has no title of its own (#2316; the provider page
  takes the same list, new rule, proposed by the implementer). Below the narrow
  breakpoint the name sits over the axis. The
  axis row shares the rows' grid, so below `breakpoint-narrow`, where the bar sits on
  top and then the name with its dot (#1826), the axis still sits over the bars.
  Assistive technology reads "Delivered <years>", an open end as "from 1952" or "through
  1990" as the period says it, never the axis bound ("No dated delivery." for a bare
  track) and, when bound, ", in the project"; the axis is decorative. Adding starts from
  the variable page, where the coverage `LaneGraph`, add window and add bar show which
  tables and years are added.

- A **`LaneGraph`** draws a group or register as lanes (one per variable, in the group's
  member order), columns as spans, and relations as arrows: solid for succession, dashed
  for rename. A succession arrow between overlapping spans drops at the edge's effective
  year. A group lane's title joins its varying facet values with a comma, falling back
  to the variable's name; it is no link, since the member's page is the group's. Its
  line under the name is plain words, never a slug: the variable's name under a facet
  title, or what tells two lanes of one name apart (their column names). A neighbour
  lane's name opens the variable, with the register on its line when it differs from the
  page's; that line also describes the link. A read-only neighbour lane still shows its
  columns' "in project" state. A span is a checkbox for the add bar, and hovering it
  highlights its edges. Spans stay pressable (#2310, after the maintainer ruling "Graph
  everywhere", 2026-11-05; the mechanics are new rules, proposed by the implementer):
  stacked below the breakpoint, and at any width where the primary pointer is a finger
  (`pointer: coarse`) and its spans would be smaller beside the names (it then stacks
  too), the track is widened until its shortest span is 24 px wide (WCAG 2.2 target
  size) and the graph scrolls sideways inside itself, never the page, with the lane
  names pinned at its left edge. A single-pick graph keeps its picked span in view, so a
  classification opens on its latest version, and its tooltip sits under the span,
  clear of the lane's name; any
  other graph that scrolls opens once on its latest span, so a lane with only a recent
  span does not read as empty. A ticked or picked span keeps its
  neighbours' size and 1 px stroke, changing only its colours, and its focus ring is
  drawn inside it, so pressing it shifts nothing (maintainer, dogfood 2026-11-05,
  #2321). On `/project` a column's
  findings sit across its lane, under its spans. It shares the page's axis with the
  `Timeline`. It draws the study window's band and, over it, an add window that differs,
  as on the variable page, with the axis years on the add window's bounds when one is
  drawn. It has no legend: a span names its column, a tooltip adds the years it cuts,
  its accessible name carries its state, and each lane states its relations in words to
  a screen reader (maintainer 2026-11-04). Its add bar shows only while a change is
  staged. On a variable page the `LaneGraph` is the `Timeline` (above). Don't draw
  free-form node graphs; the time axis is the information.

- A variable's or group's **value sets** are one panel, "Value sets", in one component
  on both pages (#2320, maintainer 2026-11-10; this overturns the 2026-11-04 group-page
  code timeline, PLAN §14): one entry per distinct code list, a disclosure titled by its
  first name ("and N more"), over the tables that use it, each table's name linking to
  its register page then the column's `ColumnChip` and its years, which wrap (the column tells
  a group's members apart where they share a table; new rule, proposed by the
  implementer). Opened, it lists the list's names, then its codes (`ValueSetCodes`). A
  list of 20 codes or fewer opens at once, with no search field; a larger one opens on
  demand, with "Search codes", and reads 50 codes at a time before "Show more"
  (maintainer 2026-11-10, #2320). A classification's page lists a version's codes the
  same way. A hierarchical list is browsed level by level.

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
`ColumnList`, `ColumnChip`, `Pill` (`TypeTag`, `TypePill`), `FilterChip`, `VariableRow`,
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
  page), and leads with its glyph: "✓ Ready to download" in `ok`, "▲ Has warnings" in
  `warn`, "✕ Has problems" or "✕ Can't download yet" in `err`; not yet checked or in
  flight it is a plain `text-muted` "Checking" with no glyph; a draft with no tables yet
  is the same plain "Empty", never an error, since nothing has gone wrong. A check that
  did not run (a rate limit, the network) is the same plain "Check did not run", never
  "Has problems": it says nothing about the project. A database not yet chosen, when
  nothing else is wrong, is the same plain "Choose a database", never an error
  (maintainer, #2345), and links to /project. Its slot is as wide as the widest verdict,
  so the banner keeps one size while the check runs. A verdict /project explains (Has
  warnings, Has problems, Can't download yet with their findings, Check did not run with
  Retry, Choose a database) is a link, in its status hue with the link's `accent` rule,
  to the verdict on /project, which takes focus; Ready to download and Checking stay
  plain text. Unsaved changes are a quiet `mono` `text-muted` "unsaved", never a status
  hue. The study window, labelled "Study years", is the period picker: an inline
  two-thumb RAC `Slider` (no popover) bounded by the viewed catalog's first and last
  year, drawn as the Timeline's study-window band (see Time: `accent-bg` between
  `accent` bounds, the thumbs on those bounds) on a `surface` strip, its years an
  `accent-ink` `mono` readout, with a "Clear" ghost action; the label, the readout and
  Clear keep one line. With no project open it is the same box, empty and untinted with
  a `border` leading bar, holding a `default` "Create a project" Button, which starts a
  project and opens it, and nothing in the catalog is tinted.
- **Findings** (the project check's) are `StatusRow`s in their level's tone, set under
  what they concern: a column's under its lane, a source's at the top of its card. Each
  names its place in the catalog's words, never an FQID. Its title holds the stable code
  in a delayed `Tooltip` (`mono`, press to copy); no code chip on the page. Under the
  project's verdict, a finding no card holds (a file that fails the structure check, a
  finding about the whole project) is listed in full, and each warning or problem a card
  holds has a plain link that scrolls to it and focuses it; a card's notes (level info)
  change no verdict and get no link.
- **Project page** (`/project`, maintainer ruling Q1, 2026-11-04): the `h1` "Project"
  with no breadcrumb repeating it, the name field, then "Download project_data.json",
  "Open a project file…" and a ghost "Discard"; opening over a project and discarding
  ask first in a `Dialog`. A steward with several databases, each with its own person
  key (maintainer ruling 2026-11-06: on this page only, and only then), adds a
  **Database** `Select` under the name field: an order draws from one database, so a
  project names one (`linkage.database`), and with one database nothing is shown. Until
  one is chosen the Select reads "Choose a database", an `info` `StatusRow` above the
  verdict reads "Choose the database your data comes from", and Download is inert and
  points at that row; the server's `database_required` and `database_unknown` are not
  listed again, and while nothing else is wrong that row is the only one: a choice not
  yet made is no error verdict (maintainer, #2345); while the check after a choice runs,
  the verdict reads Checking. A column the chosen database does not have is the server's
  `not_in_database` on its card, naming the databases that do; it is a finding, never a
  disabled option, and its lane draws the column blocked (the error ring and ✕), as
  the variable page draws a column outside the study years, never with the plain
  in-project fill (#2343). The name field is the shared form field the
  Database `Select` is: a `micro-label` over a `border-strong` hairline (maintainer
  ruling 2026-11-06, #2341: "components should match"). A variable the catalog does
  not name keeps its finding and has no link; the card's other columns keep their
  years. Under the buttons, the steward's `order_instructions` from
  `/api/context` as sent (its own line breaks, no markdown, never text from code), then
  the verdict, "Tables" (one card per table) and "What will be delivered". A card's
  column years load with its own table, not with other cards: a column the catalog does
  not know shows its own error and leaves the card's other columns their years, and
  adding or removing a column reloads no other (#2232). There is one check and no button
  for it: WASM on every edit, then `order` 350 ms after the last edit WASM accepted. The
  verdict is one `StatusRow`: "Ready to download", "Has warnings" with the count, "Can't
  download yet" with the count of problems, or "Has problems" for a file that fails the
  structure check, whose problems are listed in full under it; a check that did not run
  is an `err` row with Retry. While a newer check runs, the verdict, its list and "What
  will be delivered" keep the previous check's answer (not a check that did not run)
  with `aria-busy`; the banner's plain `text-muted` "Checking" shows in its place only
  when there is none, or when a table or column was added, removed or moved since, so
  the earlier findings would sit on the wrong card. A draft with no tables is "Empty"
  and is not checked. "What will be delivered" is the order the catalog would serve,
  shown only when the project can be ordered: per table its name and years, each column
  as a `ColumnChip` (with its years only where they differ from the table's) beside the
  variable's name, "Years not available" per column the period asks more of than the
  table has or the steward has data for (a year without data is not listed as
  delivered, #2220), and the "Row identifier" columns. There is no order-file download: the
  steward's instructions say how to order. With no project the page is one `EmptyState`,
  "No project yet", with "Create a project" and "Open a project file…". Below
  `breakpoint-narrow` a column's row on a card stacks: its name and Remove, its spans,
  then Type.
- **ColumnList:** a `label` header that names whose items they are ("Variables in
  Aktivitetsstöd", on a group page too, where the group's folded row is selected; one
  line, truncated with an ellipsis, the whole of it as hover text) (#1825). The header
  links to its page with the shared `.link` style: the subject's name to the register's
  or provider's page, "Classifications" to `/catalog/class` (#2324); the list's
  accessible name stays the plain text, then items
  on `bg`, one selected (2 px `accent` bar on `surface-sunken` at weight 600; the
  column's one bar translates between items); in the first column, items grouped under
  sticky collapsible group headers. A row's focus ring is drawn inward
  (`--focus-ring-inset`, the same accent line and halo) so the scroll box and a sticky
  row never clip it. An item or group with something in the project carries a 6 px
  `accent` dot and the count in `accent-ink` `mono` — the only numbers in the list.
  Its filter box is under Layout → Column filter. In the
  variables column a name, and a section's register name, wraps rather than truncates:
  many variable names share a long start and differ only at the end. A list of up to
  1,500 rows renders every row, so find-in-page works. A longer one (a provider's page:
  4,378 rows on /catalog/scb) renders only the rows in view, with RAC's `Virtualizer`
  and `ListLayout`: whole, it took about 3.2 s to paint and 820 ms an arrow key at 6×
  CPU throttling on a production build; virtualized, 1.0 s and 22 ms. There its register
  headers scroll with the rows (RAC 1.22.1's `ListLayout` cannot keep them sticky) and
  find-in-page sees only the rows in view; the filter box covers it. The global
  catalog's largest registers (ULF, FRIDA, HEK) pass the threshold too; in a virtualized
  list the selected row draws its own static bar, since the layout places rows where the
  moving bar cannot measure them. The unfiltered list's size decides, so a filter never
  switches a list between the two (that remounts it and drops focus).
- **ColumnChip:** the one treatment for a column name (see Typography). Three states:
  outlined, in project (`accent` fill), outside the study window (`viz-span-out`,
  `text-muted`: `text-faint` fails AA for chip text, so the faint stroke carries the
  signal). The same shape as a lane span, so the two read as one object.
- **Pill (`ui/Pill`):** the one pill, built once (#2317): an optional glyph in its ink
  (a 14 px box drawn at 12 px), the word, and years in micro mono `text-muted` where a
  caller has them, in a `sm`-radius box of its hue (the categorical rule in Colors):
  0.75 rem, line-height 1.4, padding 1 px 6 px. Every pill is that size, so a row of
  them reads as one. Two callers fix the word and glyph per type:
  - **TypeTag:** a node's kind. The label is fixed per type (Register, Table, Variable,
    Code list, Classification, Variable group) and a caller passes only the type. In a
    mixed list without a glyph; as the kind pill on a page's pill row with one: a table
    for a register, one filled column of it for a variable, two for a variable group. A
    table takes its register's hue: it is part of the register, and the word tells them
    apart. A palette row shows a name and its `TypeTag`, never an identifier (maintainer
    Q5, 2026-11-04).
  - **TypePill:** a column's type, its glyph and the word, with its years only where the
    type changes over time. The words are the catalog types in plain words ("Words";
    #2071). An identifier takes `cat-id`, every other type `cat-type` (maintainer
    ruling 2026-11-10, quoted in PR #2391).
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
  no "Loading…" line beside it. A name still loading inside a heading or link is a
  `NameSkeleton`: the inline line plus visually hidden text ("Loading table name"), so
  the heading keeps a name. Loading is not failure: only a settled miss shows the
  fallback (a table's join key, "Variable name unavailable"), and only after the names
  the project check gives: a variable this steward does not offer has no page to read a
  name from, so its finding and the order name it (#2220). A source card's timeline
  region shows one info `StatusRow`, "Loading years…", in place of the graph while it
  loads.

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
- Do design for 375, 768, 1280 and 1920 px and verify with `dev.sh shot --all`, on the
  fixture and on a real steward pair (`REG_META_DB` with `REG_META_CATALOG`), with a
  fresh-agent rendered review before merge. Don't add breakpoints or `display: contents`
  (it strips accessibility roles).
- Do keep focus visible via `:focus-visible` (or `[data-focus-visible]` on RAC parts)
  `{ box-shadow: var(--focus-ring) }` in each component's own CSS, and respect
  `prefers-reduced-motion`. Motion answers a user action at the three durations above;
  nothing animates on page load.
- Do name things by what a researcher recognizes (registers, variables, columns, value
  sets, projects, study years) and keep an action's name the same through its whole
  flow. Don't apologize in errors or leave them vague.
