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
    rounded: "{rounded.sm}"
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
  | Has warnings, Has problems, Can't download yet                    | warnings, errors, order blocked        |
  | (nothing: a ready project shows no verdict)                       | valid, ready                           |
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
  or a `Select`'s border, a column chip's stroke and the axis of a graph. A text field
  (the search field, every filter box) takes `border` (#2407, maintainer decisions
  2026-11-11). A rule or divider
  in a list or browse column is always `border`, one shared rule (`.sectionRule`) in
  both columns (maintainer review of the classification pages, 2026-11-05, point 1).
- **Pine, the ownership colour (`accent` and its stops):** used for exactly one idea —
  this is yours, or this is where you are. Primary button fill (`accent-fg` text), the
  bar and count on a column item or group that has something in the project, the
  selected row's bar, the focus ring, the banner's study-window bounds. `accent-bg` is
  the wash: selected variable rows, the banner's study-window band, the active segment,
  and on a `LaneGraph` an add window that differs from the study window (see Time).
  `accent-band` is one stop stronger: a ticked span. `accent-soft` is the project
  banner's fill and the study years' shade on a `LaneGraph`; no lane has a fill of its
  own, a classification's picked version's neither (maintainer, dogfood 2026-11-05,
  #2319; maintainer answer on #2399, 2026-11-11). `accent-ink` is pine as text (in-project counts, window years on an axis).
  Never use pine for a verdict, a type or decoration; `ok` is a verdict, `accent` is
  ownership.
- **Status (`err`, `warn`, `info`, `ok` and their `-bg` tints):** the only other
  chromatic chrome, each paired with a glyph and text. A timeline marks no coding
  change ("Drop them", maintainer, dogfood 2026-11-05, #2319). Never use a `-bg` tint as a text color, and never use a status
  hue for emphasis, selection, or brand.
- **Categorical (`cat-reg` teal, `cat-table` moss, `cat-var` indigo; `cat-id` sienna,
  `cat-type` slate):** the `Pill` hues. An entity's: a register teal, a table moss, a
  variable and a variable group one indigo, told apart only by the kind pill's word
  ("Variable", "Variable group"); a column has no hue, it is the white pill
  (maintainer, #2399, approved 2026-11-11). `cat-id` is an identifier's Type pill and `cat-type` every other column
  type's (maintainer ruling 2026-11-10, quoted in PR #2391). Fill and border take the
  raw hue (10 % and 35 %), the glyph its `-ink` stop; the text is always `text`, on
  every hued pill, and a pill link's underline is the text colour (maintainer, #2399, approved 2026-11-11).
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
what the researcher types in code and is always a column pill (`EntityPill`,
`CDISPKE04H`): mono, as delivered, in the white ink-outlined pill with the column glyph,
optionally with its years inside. Never set a column name in the text face and never
set a variable name in mono or in a column pill.

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
  sticks to the top of the page above `breakpoint-narrow`. Below it the bar has two rows
  that scroll away with the page (#2403, maintainer decisions 2026-11-11): the logo,
  then the search field filling the row, always shown; under them the menu button and
  the project banner, which fills the rest of the row. The logo and the button are
  both 32 px (`spacing.6`) and share one column, so the field and the banner share both
  edges (#2407, maintainer decisions 2026-11-11, variant B of the narrow top-bar
  mock-ups); the button sits at the top of its row, centred on the empty box and level
  with an open banner's first line. At every width the field and the banner are one
  box style: a 1 px `border`, `rounded.sm`, no leading bar; an open project keeps its
  `accent-soft` fill (#2407). The field and the empty box are one height, the column
  header's 2.75rem (new rule, proposed by the implementer). The button is icon-only (three
  lines), with "Browse registers" as its accessible name and tooltip, and
  `aria-expanded` (`aria-controls` names the register column); it opens the register
  browser and closes it again (see Browse columns). This replaces the "Search" button
  that unfolded the field (maintainer ruling 2026-10-24). Below the breakpoint every text field is set at 1rem: iOS Safari
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
  400px, #2107) there is one column (#2403, maintainer decisions 2026-11-11). The
  variables column is not drawn there: a register's variables are reached from its
  page, which has its own filter box (see Column filter). The register column is the
  register browser, shown in place of the page while open. The top bar's menu button
  opens it and closes it again; choosing an entry opens that provider's, register's or
  the classifications' page and closes it, and so does Escape, which returns focus to
  the button. The button replaces the "Browse registers" first crumb (#1824) as the way
  into the register list. Proposed by the implementer: opening it moves focus to the
  open page's row, or the first row; choosing a row is a press, Enter or Space; the
  arrows passing a row open its page behind it, as above the breakpoint, and leave it
  open; and any other navigation (a link, a search result, the history) closes it. The
  open browser has no side rule: there is no column beside it (#2407, maintainer
  decisions 2026-11-11). So the browse columns show only from 961 px, always beside a
  `Timeline` with its names in the gutter (see Time), and below the breakpoint the
  detail pane is one centred column, capped at the content width it has where the
  columns first show (493 px at 961 px, inside its padding), so a timeline keeps its
  width from 960 to 961 px and the content only moves right as the columns appear
  (#2407, maintainer decisions 2026-11-11). The pane keeps its 16 px narrow padding and
  the cap is measured inside it (new rule, proposed by the implementer: the mock-up's
  24 px padding from 481 px would mint a breakpoint). Do not mint new breakpoints.
- **Search.** The top-bar field searches and filters nothing (#2398, maintainer
  2026-11-11): typing never changes the browse columns or the open page. Its results, the
  command palette (RAC `Autocomplete`), open under it as you type once the text reaches
  the search minimum (two characters, `SEARCH_MIN_QUERY_LENGTH`), and on Enter or `⌘K`
  (with an empty field, `⌘K` shows the places). This replaces the 2026-10-22 redesign's
  "Search filters in place" and its rules (#1827, #1828, #1830): the register column's
  filter and "matched on" lines, the other registers' matches under the open one, the
  count in the field and the page-wide Escape. The palette lists registers, variables
  (with their register) and the app's places ("Go to the project", "Go to the start page"; there
  is no "Browse the data": the columns are always there on a wide screen, and the menu
  button opens them on a narrow one, maintainer 2026-11-11, #2403); classifications are browsed in the register column, not searched
  (Operator 2026-11-04, maintainer 2026-11-05). Each kind is one request for its first
  four hits, and the palette follows no cursor. On a steward site the answer holds two
  lists ranked apart, the catalog's `items` and the steward's own `steward.items`: the
  palette lists the catalog's hits, then the steward's, each in the server's order, a
  place listed once. The two ranks never merge; a list that pages follows each list's
  own cursor (`cursor`, `steward_cursor`) (#2110). Its results are a mixed list: each
  row's name is its entity pill (a register's, a variable's), its context beside it,
  with no separate kind pill (S1, maintainer, #2399, approved 2026-11-11). It is a list under the field with no field of its own: the top-bar
  field is its one input, so focus stays there and the arrow keys move through the rows.
  No row is active until they reach one, so Enter never opens a row whose hits are still
  loading (new rule, proposed by the implementer).
  A second field in the popover showed the same text twice (maintainer report
  2026-10-24). Choosing a row navigates and ends the search: the text clears (new rule,
  proposed by the implementer: with nothing filtered there is no state to put back, which
  the 2026-10-24 restore of the pre-palette filter did), and below the breakpoint the
  register browser closes. Below the breakpoint the palette is as wide as the field.
  Escape closes the palette first, then clears the text; Tab, a press outside or focus leaving the field closes it, and typing opens it again.
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
  page-wide Escape of #1827 cleared the top-bar filter, which is gone). Every filter box
  has a 1 px `border`, `rounded.sm` (#2407, maintainer decisions 2026-11-11). The
  register page has the same box first in its list's panel, then the table rows, then
  the variable list: the box filters the page's variable list, the rows the table
  filter leaves (#2403; order: #2110, maintainer decisions 2026-11-11, arrangement Y). A provider's page has the same box over its
  register list, reading "Filter registers…", named "Filter this provider's registers",
  matching by name and slug (#2407, maintainer decisions 2026-11-11; the name and its
  empty state "No registers match …" are proposed by the implementer). It shows at every width, so on
  wide screens the two boxes filter different lists (maintainer 2026-11-11, #2403). The same matching, marking and Escape; "Filter variables…" as its text and
  "Filter this register's variables" as its name, apart from the column's. It belongs to
  the page: it starts empty when another register opens. A filter that matches nothing
  says so in an `EmptyState` in the list's place (implementer proposals).
- **Home (`/`):** after the design system's Cover, with no hero, cards or counters: the
  product name in `display` type, the deployment's description (`steward.json`
  `description`: what the site is and, for a deployment like SWECOV, what holds the
  data) in `text-muted`, with inline emphasis and `https` links (Operator lane brief
  2026-11-10, PR #2391; any other scheme stays text). The browse columns are beside it
  and the project box is in the top bar, so there is no link into either. `/catalog`
  is no page: it redirects here in place (`history.replaceState`, no reload, its query
  and hash dropped), since the columns stand beside every page (maintainer 2026-11-11,
  #2403). A cold load reaches it through the edge's SPA fallback.
- **Detail pane:** breadcrumb path (the ancestors' names in the UI face, `text-muted`:
  the provider, then the register, each a link, ending at the page's parent, since the
  `h1` names the page itself; maintainer Q5, 2026-11-04), at every width (#2403). The
  names come from reads the browse columns already make, so the trail fetches nothing of
  its own; an ancestor whose name has not arrived is left out, never shown as its slug. A group page's trail skips the group
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
  #2335). The provider's series breaks are not build findings and are shown ("are important
  and must be shown by reg_meta to the user and the AI", maintainer ruling 2026-11-14,
  relayed in the Operator's brief): a "Series breaks" panel after the definitions on a register's page
  (the register's own, then each table's under "In <table>", only the selected tables'
  with a table filter on) and after the Description on a variable's or group's page (a
  group's under each member's lane title), each with its day or years muted over the
  provider's text, never a release name or provenance; it renders only when a break
  exists (placement: Operator, 2026-11-14; the rest is a new rule, proposed by the
  implementer). A variable page has no source and provenance section; MCP keeps
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
  each wording under its years. On a group page it is the group's curated description,
  else the one description every member states alike; with neither there is no
  Description panel. A member shows its own curated description under its lane name
  only where it differs from the group's; members' provider wordings are not on the
  group page (the Operator's proposal under the maintainer's delegation, 2026-11-11,
  #2322). It sits under the facts, above the `Timeline`, on both
  pages (new rule, proposed by the implementer: a group's description was prose under
  its `h1`). Timeline and
  facts read the one per-period list (`segments`): on a steward deployment its rows are
  the files and columns the steward has, over the years it has them. A table has no page
  of its own (maintainer, dogfood 2026-11-05, #2316): its address (a project card, the
  palette, a link) renders its register's page with that table's filter on, one route
  and one page, not a redirect; where no table row filters by it (it delivers no
  variable, or it is the register's only delivering table) the page shows the whole
  list. A
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
  2026-11-05); it is local state, its span in the group page's selected-span style; its
  lane has no fill (maintainer answer on #2399, 2026-11-11). Pressing a span picks its version (maintainer
  ruling, 2026-11-05), and a version address focuses that span (new rule, proposed by
  the implementer: picking does not change the URL, which would reload the page). Since
  one version is picked at a time, the spans are one radio group, not checkboxes: one
  tab stop, the arrow keys move between versions and pick them ("expose it as a radio
  group", maintainer comment on #2310, 2026-11-05). Placement caveats are steward
  working notes, not shown until the steward
  curates them for readers. A register page shows under its `h1` only its description,
  as prose: the timelines show the years and the tables, so there is no Years or
  Tables definition (#2316). Its tables are a `VariableRow` list of their own between
  the filter box and the variables, headed "Tables" with the muted note "Click to
  filter", on the variables' axis, each row's bar the table's years from the register's
  `coverage`, gaps kept (åk 9's 2020–2021), and its name its table pill as a
  `FilterChip`, so no table is listed twice (#2110, maintainer decisions 2026-11-11,
  arrangement Y, heading option 2). The rows are the tables that deliver a variable, by
  name; a lens with OR within the selection, offered when there are two things to
  filter by: two such tables, or one held in several databases, where only its database
  rows filter (the archived variant
  chips, maintainer 2026-11-03; the one-table rule, new rule, proposed by the
  implementer). On a steward with several databases a table held in one shows that
  database's pill after its own, plain; a table held in several has a row per database
  under its row, set in 16 px, the database's pill its filter (#2110). With a filter on,
  "N of M variables shown" sits after the Variables heading as a live region mounted
  empty, the Tables note stays, and each variable's bar draws only the selected tables'
  and databases' years; with none, a variable's bar is its years in every table, as
  before (#2110, maintainer decisions 2026-11-11). The browse column's list of the
  register's variables narrows with the page's filter while the page is open (#2110,
  maintainer 2026-11-11); leaving the page clears it (new rule, proposed by the
  implementer: a filter the column kept on another page would hide variables with no
  visible cause). The page reads a table's variables' years (`show` at the table, or for
  an implicit table, whose address is its register's, its `segments`) on its first press
  only, never on load (maintainer 2026-11-11: "lazy per table"); until they land the
  variable list is a skeleton, never rows the selection may not keep (new rule, proposed
  by the implementer). Table rows carry no in-project mark (maintainer decisions
  2026-11-11). A register page has no "Unplaced releases" panel (maintainer 2026-11-04).
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
  beside its bar (maintainer, PR #2389, 2026-11-10: fold). On `/project`, the years a
  table delivers (the delivery summary, a source card's lanes) read in the table's grain,
  which wins over the cut piece's own alignment: in a school-year table, a piece cut by
  the study years starts or ends at the school year the cut falls in, since that year's
  file is delivered whole, so study years 1981–2024 on Avgångsår read
  `avgar 1980/1981–1988/1989` and `AvgAr 1989/1990–2023/2024`, not `1981–1989`; a
  calendar-year table still reads in years (maintainer, #2416, 2026-11-11). The drawing
  keeps the cut. A source period on `/project`
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
  `Timeline` and `LaneGraph` (a variable's, a group's, a classification's, a source
  timeline) the same way: one continuous, lighter `accent-soft` shade from the coverage
  row through the last lane, 8 px past each bound so it never sits tight on the spans'
  ends, with no bound lines, and `accent-ink` years on the axis (maintainer, #2399,
  approved 2026-11-11; the classification page and `/project` too, maintainer answer on
  #2399, 2026-11-11). The shade stays drawn where an add window narrows inside it
  (#1910). The banner keeps its `accent-bg` band between `accent` bounds. With no
  project open there is no shade, no thumbs and no years to add: no column can be
  picked then (maintainer, #2399, 2026-11-11). That **window band** is the
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
  `accent-bg` between the thumbs, padded 8 px as the shade is, running through every
  lane; the study window's shade stays drawn, so a narrower add window shows the study
  window is larger, and an add window that reaches past the study window extends the
  stronger band beyond it (maintainer answer on #2399, 2026-11-11). There is no legend. A "Clear period"
  ghost action above the drawing drops the period, and shows only while it differs; its
  row is kept while it is hidden, so the drawing never moves when it shows (maintainer,
  #2408, 2026-11-11); thumbs that come to rest on the study window (inside the availability) drop the period
  rather than write one equal to it. Only the drawn windows' years without availability
  are `info-bg` (below); the add window's fill never means a narrowing. While an add
  window is drawn the axis years are its bounds in `accent-ink`; the study window's
  bounds are then unlabelled. **Adding is by variable** ("I don't think most users will
  understand the difference between columns and variables so adding should be by
  variable", maintainer, #2408, 2026-11-11): a project binds a variable once per table,
  so pressing any span of a variable's table lane stages that variable there, every
  column of it in that table inside the years to add marked "+" in the selection wash,
  and only those. Pressing a lane the project already binds stages its removal: the
  columns it takes are marked "−" and struck through (#2408, 2026-11-11). An add writes
  whole years: the years to add cut to the calendar years the variable is delivered in
  that table, gaps between deliveries kept, never a date end, so a school-year column
  added for 2005–2025 writes `2005..2024` (#2409, Operator proposal, not yet confirmed).
  An add widens its source's period to the union
  and never narrows it, so the add window raises no warning; a column already in the
  project whose source years differ from the study window says so in its accessible
  name, not in a tooltip (maintainer ruling, 2026-11-10, #2395) or a line under the
  graph (#1833); one outside the window is
  still named there at `err`. A register pane draws the `Timeline` read-only; a group
  pane draws it as the variable page does, a lane per member.

- A **source period** is set on `/project`, on the source's card, where the source is
  drawn whole as the **source timeline**: a `LaneGraph` headed by the coverage row of
  the source's table, its period as the window band with the same thumbs, bounded by
  that table's coverage, and one lane per binding. Its columns are drawn as on the
  variable page, one row per run of the variable's columns, each column over the
  days that table delivers it within the source's period (the dragged window's while
  a thumb moves), not the column's whole coverage (Operator proposal, #2409; rows,
  #2408). A column the period does not reach is not drawn. A column available for
  part of the period is a shorter span, not a finding. The card is headed by the table's register, by name, with
  the table's pill as a link and "Years:" with the period's years under it. Where two
  tables of one register would share that heading, the heading names its table with
  the table's pill under the register: the table's own name, never an invented label
  (maintainer ruling, #2221; the pill only where a table name already shows, Operator,
  #2399, 2026-11-11). Every label on the card names the table by its catalog
  name, as the page's findings do; the source's `name` (the file's join key) stands in
  only where the catalog has no name for the table. A stored period outside coverage
  widens the thumbs' bounds to their union; it is never clamped or rewritten on load. A
  comma-list period rests the thumbs on its outer years and a line under the drawing
  says a move makes it one span. Any other dated period (a date, a term, a month) rests
  the thumbs on the years its first and last days fall in, and the line says a move
  makes it whole years; the card never asks the researcher to edit the file (Operator
  proposal, #2409). The card's findings carry no "Years:" line of their own unless the
  finding has its own period: the card already states them. A timeless source has no "Years:" line and no thumbs. A dated table with
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

  Data the catalog does not document (a project's "manual" items, contract 6.2.0) is shown
  and removed, never edited, and nothing about it is read from the catalog. A source
  entered by hand is a plain `Panel` card headed "<provider>: <register>" as the
  researcher wrote them, with the line "Not in the catalog, entered by hand", its
  years as text (or "not tied to years"), and one row per column: the column name, its
  stated `TypePill`, and "N codes" when it states any. It has no lane, no coverage
  graph, no thumbs and no catalog link, and its own `manual_item` finding is left off the
  card, since the line says it. A column entered by hand inside a catalog source is the
  same row (a `LaneList`, its name's sub line carrying the label) under the lanes. Both
  take the lane's ghost "Remove", named "Remove <column> from <table>", with the same
  dialog. A catalog source keeps one catalog column at least, so removing its last
  catalog column removes the source, and its columns entered by hand with it; the dialog
  and the add bar say so. The delivery summary lists the order's manual items under
  their table, or under their own "<provider>: <register>" heading, each with its type
  and the same label (new rule, proposed by the implementer).

  Each lane's below slot holds an optional classification pin and its findings. There is
  no type control: an added column states no type, so the order reads the catalog's
  (#2071, G13). The pin appears for a binding with a classification or a catalog
  ambiguity finding, using named versions from segments on this table overlapping the
  source period, plus "Not pinned" to clear it. A stored pin outside those candidates
  remains named and marked "not delivered in this period". Versions use a scrolling
  listbox, never chips. These controls also appear in plain rows where the drawing is not shown (the table did not load, or the period is invalid).

- A variable whose **availability is narrower** than the window is information, not a
  warning: the uncovered part of the window is drawn `info-bg` on the coverage row. The
  picture says it, so there is no note or legend entry for it; the header names those
  years to a screen reader only (#1833).

- A **dated series break** is drawn on a variable's or group's `Timeline` at its day
  (maintainer request in the Operator's message of 2026-11-14, quoted verbatim:
  "markers at their day, with the text on hover/focus and accessible text. Undated
  breaks: no marker; the list covers them"): a small `warn` triangle under the axis in a
  24 px target and a dashed `warn` line down to the last lane. The marker is a tab stop
  whose name and tooltip are the break's place, day and text, one marker per day (new
  rule, proposed by the implementer).

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
  lane. The variable's columns are its lanes, one per table, a 36 px row headed by the
  table's pill (the name inside it, cut with an ellipsis), so a lane reads as one of the
  variable's tables, not as another variable (#2319); no divider runs between a
  variable's tables. Its spans are column pills (variant A, maintainer, #2399, approved
  2026-11-11). On a steward with several databases, the line under a table's
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
  One row per variable and table ("Maybe they should always share a row?", maintainer,
  #2408, 2026-11-11): a variable's columns in one table are successive spellings that
  never overlap in time (`avgar` to 1989, `AvgAr` from 1990), drawn as successive
  column pills on that row; only columns delivered side by side in one file (a slot's
  members, `ng2`–`ng5`) overlap, and each takes a row of its own; a later column joins
  a row only where exactly one row can take it, since sharing a slot member's row would
  read as its rename (new rule, proposed by the implementer). A steward's yearly
  files of one table are the same column delivered in several files, one box over their
  years with a gap where a year is not delivered (maintainer 2026-11-04). Spans are
  drawn at their real half-open dates, so school years abut instead of sharing a year
  cell; a one-day period (a 31 December snapshot) takes its calendar year, as its text
  does (#2408, 2026-11-11). Where a row joins several spellings, each name sits in its
  own widest part, or, too wide for it, beside the row (before it the names left of the
  first that fits, after it the rest), never cut; its tooltip lists each column with
  its years (new rule, proposed
  by the implementer). A span's column name is never cut: it
  sits inside the widest delivered part when it fits there, with no list of the files
  it is delivered in: the lane already names the table (maintainer, dogfood 2026-11-05,
  #2321). A span is the column pill stretched over its years: the pill's height,
  padding, font and column glyph, the column name only; the axis gives the years, which
  stay in its accessible name (maintainer, #2399, approved 2026-11-11). Its 24 px slot
  keeps the target size; the 21 px pill sits centred in it. A span too narrow for the
  column name is a bare bar with the name beside it: to the right, to the left at the
  axis end, else in a gap between its deliveries, and only where no room is free over
  the bars at the track's end, never cut. Beside the track the names take a compact
  gutter that narrows with the graph: 27 % of its width between 120 and 230 px, then a
  gap of 2 % between 8 and 16 px, so the 493 px graph of a pane at 961 px keeps a 350 px
  track (#2407, maintainer decisions 2026-11-11). Under a 320 px track, a graph under
  about 450 px, the names stack over their spans; the margin keeps a 961 px window
  with a classic scrollbar on the gutter view (new rule, proposed by the implementer).
  A graph whose lanes carry an action beside the name (the /project source card's
  Remove) keeps the full 230 + 16 px gutter and stacks under a 360 px track, so name
  and action still fit (new rule, proposed by the implementer: the compact gutter cut
  "Antal personer" to "Anta…" beside Remove).
  Stacked below the breakpoint (variant C of the
  Operator's inline-timeline mock-up, maintainer, #2408, 2026-11-11), the variable's
  heading is set in `h3` with a rule above it, and each table is a block with a moss
  guide line (`cat-table` at 35 %, the table pill's border) down its left: the table pill and its delivered years
  on one line, the years at the right and wrapping between runs, and the spans on a
  full-width track below, 16 px in, past the line; a track that scrolls passes under a
  `surface` strip of that inset, so no span runs past the line. A lane's spans start just under its
  name. A stacked graph that scrolls sideways with no span to tab to is itself a tab stop,
  a region named by the drawing, so a keyboard can scroll it (WCAG 2.1.1; Operator
  fold, #2408). Hovering or focusing a span
  shows a RAC `Tooltip` with no delay only where the span's own text is cut off, which
  is its years: always on a column span, on a classification version's (a span with no
  column name, which carries its years where they fit) only where they do not
  (maintainer, dogfood 2026-11-05, #2321; the strict reading,
  maintainer ruling, 2026-11-10): no table name and no study-window note in a tooltip,
  and no table name on a span. The
  variables a succession reaches are further lanes in time order, with their arrows. A
  line under the drawing says "Click a column to add or remove its variable." while a
  project is open; with none there is no line and no button at any width, since the top
  bar has the action ("yes", maintainer, dogfood 2026-11-05, #2319). There is no second
  graph on the page. While it loads the region is a `Skeleton`; a
  failed load is an error `StatusRow` in the region with Retry; a variable with no years
  says "No years recorded for this variable." in place of the lanes. Succession reaches
  one hop of read-only lanes, with arrows from predecessor to successor and each
  neighbour's name linking to its variable page.

- A **`VariableRow`** repeats its list's axis as a thin bar (`ui/YearTrack`), so a
  variable that does not cover the window is visible before it is added. It is one
  read-only row of a register's variable list, of its table list (a table's years, its
  filter pills for a name, no link and no project mark, #2110), or of a provider's
  register list (the register's years as one span), never an HTML table row: the
  in-project dot and wash, the name in the text face as a link to its page, and the bar
  with the study window block.
  The block is `accent-bg` between 1 px `accent` bounds; on an in-project row, whose
  wash is `accent-bg` too, it is `accent-band` between 1.5 px `accent` bounds (#2407,
  maintainer decisions 2026-11-11, variant C).
  The list's one header is the bars' axis (`YearAxis`): its first year over the bars'
  start and its last over their end, in the `axis` type (maintainer 2026-11-05), with
  the list's name ("Variables", "Registers") on the same row over the names as the
  section's heading, so the panel has no title of its own (#2316; the provider page
  takes the same list, new rule, proposed by the implementer). Below the narrow
  breakpoint the name sits over the axis. The
  axis row shares the rows' grid, so below `breakpoint-narrow`, where the bar sits on
  top and then the name with its dot (#1826), the axis still sits over the bars.
  A provider's register row also describes its register and counts it (#2404,
  maintainer 2026-11-11, variant A): the register's description under the name in
  muted `body-sm`, as long as it runs and cut only past about four lines (the full
  text is on the register page), and nothing where it has none; then a table count
  and a variable count as table and variable `EntityPill`s. On a list at least 40rem
  wide each count has its own column between the names and the bars, headed "Tables"
  and "Variables" on the axis row, the pill showing the number alone. A narrower list
  stacks as the narrow rows do, with the two pills under the description and their
  words ("2 tables", "258 variables"). The list's own width decides, not the viewport's
  (the approved mock-up's 640 px pane), so just above `breakpoint-narrow`, where the
  pane is narrow, the names keep their measure; below it the pane is always narrower
  than 40rem. The word stays in each pill's accessible name at every width, and the
  headings are decorative. The dot, pills and bar sit on the name's line. The table
  count is the tables the register page offers as its table filter, counted on one
  table too (implementer proposals: the headings' words, the four-line cut, the 40rem
  list width, the single-table count).
  Assistive technology reads "Delivered <years>", an open end as "from 1952" or "through
  1990" as the period says it, never the axis bound ("No dated delivery." for a bare
  track) and, when bound, ", in the project"; the axis is decorative. Adding starts from
  the variable page, where the coverage `LaneGraph`, add window and add bar show which
  tables and years are added.

- A **`LaneGraph`** draws a group or register as lanes (one per variable, in the group's
  member order), columns as spans, and relations as arrows: solid for succession, dashed
  for rename. A succession arrow between overlapping spans drops at the edge's effective
  year. On a group page each member is a 28 px heading row with no track, over its
  tables' 36 px rows, indented 16 px and headed by their table pills (as on the variable
  page); a divider runs between members only, never between a member's tables (variant
  A, maintainer, #2399, approved 2026-11-11). A plain lane (a succession neighbour, a
  classification's part, a `/project` column) keeps its 58 px and its rules above and
  below. A group lane's title joins its varying facet values with a comma, falling back
  to the variable's name; it is no link, since the member's page is the group's. Its
  line under the name is plain words, never a slug: the variable's name under a facet
  title, or what tells two lanes of one name apart (their column names). A neighbour
  lane's name opens the variable, with the register on its line when it differs from the
  page's; that line also describes the link. A read-only neighbour lane still shows its
  columns' "in project" state. A span is a checkbox for the add bar (adding or removing
  its lane's variable, above), and hovering it highlights its edges. Spans stay pressable (#2310, after the maintainer ruling "Graph
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
  `Timeline`. It draws the study years' shade and, over it, an add window that differs,
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
  its register page, as the linked table pill, then the column pill and its years,
  which wrap between runs, never inside one (the column tells
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
`ColumnList`, `Pill` (`EntityPill`, `TypeTag`, `TypePill`), `FilterChip`, `VariableRow`,
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
- **AddBar:** the shared sticky bottom bar for a staged change on the variable or group
  page. Its `primary` button reads "Add N variables to project", counting variables, not
  columns (Operator proposal, #2408, not yet confirmed); a staged removal adds the
  `danger` "Remove from project", and the press is the confirmation, with no dialog
  (Operator, #2408, 2026-11-11). Where a removal takes a table's last variable, a line
  beside the buttons says so before the press ("Also removes Slutbetyg from the
  project.", with "and from its panels" where the table is in one), and describes the
  button. The caller omits the bar when nothing is staged.
- **ProjectBanner:** the project's presence in the top bar — name, study window,
  source and column counts, the verdict, save state — on `accent-soft` with a 1 px
  `border` hairline and `rounded.sm`, the search field's box, with no leading bar
  (#2407, maintainer decisions 2026-11-11). The name is `h3`, the counts `mono` ("1
  table, 2 columns"). The verdict is the project page's, in the same words (see Project
  page), and leads with its glyph: "▲ Has warnings" in `warn`, "✕ Has problems" or "✕
  Can't download yet" in `err`; not yet checked or in flight it is a plain `text-muted`
  "Checking" with no glyph; a draft with no tables yet is the same plain "Empty", never
  an error, since nothing has gone wrong. A check that did not run (a rate limit, the
  network) is the same plain "Check did not run", never "Has problems": it says nothing
  about the project. A database not yet chosen, when nothing else is wrong, is the same
  plain "Choose a database", never an error (maintainer, #2345), and links to /project.
  Its slot is as wide as the widest verdict, so the banner keeps its width while the
  check runs. A verdict /project explains (Has warnings, Has problems, Can't download
  yet with their findings, Check did not run with Retry, Choose a database) is a link,
  in its status hue with the link's `accent` rule, to the verdict on /project, which
  takes focus; Checking and Empty stay plain text. A ready project shows no verdict in
  the box (maintainer, #2417, 2026-11-11: "Can we remove the big 'Ready to download'
  flag from the project box? … We can just not do that."). Its slot keeps its
  width but takes no height (maintainer, follow-up to #2417, 2026-11-11: "Good that the
  project box does not change size."), so where the verdict would wrap to a line of its
  own (a narrow top bar) there is no blank line, and there the box loses that line when
  a check ends. Unsaved changes are a quiet `mono` `text-muted` "unsaved", never a
  status hue. The study window, labelled "Study years", is the period picker: an
  inline two-thumb RAC `Slider` (no popover) bounded by the viewed catalog's first and
  last year, drawn as the Timeline's study-window band (see Time: `accent-bg` between
  `accent` bounds, the thumbs on those bounds) on a `surface` strip, its years an
  `accent-ink` `mono` readout, with a "Clear" ghost action; the label, the readout and
  Clear keep one line. With no project open it is the same box, empty and on `surface`,
  holding a `default` "Create a project" Button, which starts a project and opens it,
  and nothing in the catalog is tinted.
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
  verdict is one `StatusRow`: "Has warnings" with the count, "Can't download yet" with
  the count of problems, or "Has problems" for a file that fails the structure check,
  whose problems are listed in full under it. A ready project shows no verdict, as in
  the box (maintainer, follow-up to #2417, 2026-11-11: "Drop ready to download also on
  project page"). The row stays as an empty live region and the section takes no
  space, so the next verdict lands in a region the page already had (new rule, proposed
  by the implementer). A check that did not run is an `err`
  row with Retry. While a newer check runs, the verdict, its list and "What will be
  delivered" keep the previous check's answer (not a check that did not run)
  with `aria-busy`; the banner's plain `text-muted` "Checking" shows in its place only
  when there is none, or when a table or column was added, removed or moved since, so
  the earlier findings would sit on the wrong card. A draft with no tables is "Empty"
  and is not checked. "What will be delivered" is the order the catalog would serve,
  shown only when the project can be ordered: per table its name and years, each column
  as a column pill (with its years only where they differ from the table's) beside the
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
- **Pill (`ui/Pill`):** the one pill, built once (#2317, #2399): a 12 px glyph (a
  14 px box) in its hue's ink, the name, and years in micro mono `text-muted` where a
  caller has them, in an `sm`-radius box: 0.75 rem at weight 500, line-height 1.4,
  padding 1 px 6 px, 4 px from glyph to name, 20.8 px high. Every pill is that size: a
  table pill, a column pill, a span and a filter differ only in colour and glyph
  (maintainer, #2399, approved 2026-11-11). A hued pill's text is `text`, its hue on
  fill, border and glyph only (Colors); a pill that is a link underlines its name in
  the text colour. A long name ends in an ellipsis, the whole of it the accessible name
  and hover text only when cut; a pill in a list of tables alone keeps its glyph.
  Three callers fix the word and glyph:
  - **EntityPill:** an entity by name. A table moss with the grid glyph; a register
    teal with two stacked sheets; a variable and a variable group indigo with one
    column in a frame; a column the white pill, mono, ink-outlined, the same glyph in
    `text-muted`, in three states: outlined, in the project (`accent` fill), outside
    the years to add (`viz-span-out`, `text-muted`: `text-faint` fails AA). Where an
    entity is named: a table's row in a lane, a register's table filters, a project
    card, a value set's tables, a search row (a register's or variable's), a register's
    table and variable counts on its provider's page (#2404), and every column name,
    a lane span included. A steward's database is a cylinder on `surface` with a
    `border` stroke and its name in `text-muted` at weight 400, no entity hue (a
    database is where a table is held, not a catalog entity): on a register's table
    rows (#2110, maintainer 2026-11-11, the approved held-summary mock-up).
  - **TypeTag:** a page's kind pill, first in its pill row, the word at weight 400
    (Register, Variable, Variable group), in the entity's hue and glyph. A variable and
    a variable group share both; only the word tells them apart (maintainer, #2399,
    2026-11-11). Never in a list: a list row's name is its entity pill.
  - **TypePill:** a column's type, its glyph and the word at weight 400, with its years
    only where the type changes over time. The words are the catalog types in plain
    words ("Words"; #2071). An identifier takes `cat-id`, every other type `cat-type`
    (maintainer ruling 2026-11-10, quoted in PR #2391).
- **FilterChip:** a real checkbox whose face is the pill it filters by: a register's
  table or database pill at the pill's size, which it keeps when on (#2110), neutral at rest (`surface`, `border`, the hue on the
  glyph alone) and `accent-bg` with an `accent` border and `accent-ink` text when on, so
  the entity hues never compete with the pine "on" state (F1, maintainer, #2399,
  approved 2026-11-11). A LENS: toggling narrows what is shown, never the project. On a
  register page each chip is its table row's name (Time → VariableRow).
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
