---
version: alpha
name: Registry Research Toolkit
description: >-
  Design language for the reg_webapp catalog and project-authoring SPA. Ink on neutral
  paper: a monochrome base where color is reserved for meaning (status, node type, data
  edges), Schibsted Grotesk for UI text, IBM Plex Mono for every machine identifier.
  Dense, keyboard-first, light-first and dark-ready through semantic role tokens.

colors:
# Primitive ramp — neutral paper to ink. Components never read these; the
# semantic roles below reference them, so a re-tint or a provider theme is a
# role remap.
  gray-1: "#151618"
  gray-2: "#232527"
  gray-3: "#34373a"
  gray-4: "#474a4e"
  gray-5: "#5f6368"
  gray-6: "#6b6f74"
  gray-7: "#9a9ea3"
  gray-8: "#b9bbc0"
  gray-9: "#dcdde0"
  gray-10: "#e6e6e5"
  gray-11: "#efefee"
  gray-12: "#f7f7f6"
  white: "#ffffff"
  # Spec-required palette names, mapped onto the system.
  primary: "{colors.gray-1}"
  neutral: "{colors.gray-12}"
  # Semantic roles — each is the CSS custom property of the same name in
  # src/tokens.css (`colors.bg` → `--bg`).
  bg: "{colors.gray-12}"
  surface: "{colors.white}"
  surface-raised: "{colors.white}"
  surface-sunken: "{colors.gray-11}"
  surface-hover: "{colors.gray-11}"
  surface-selected: "{colors.gray-10}"
  text: "{colors.gray-1}"
  text-muted: "{colors.gray-5}"
  text-faint: "{colors.gray-6}"
  border: "{colors.gray-9}"
  border-strong: "{colors.gray-8}"
  accent: "{colors.gray-1}"
  accent-fg: "{colors.white}"
  accent-bg: "{colors.gray-10}"
  accent-ink: "{colors.gray-1}"
  # Status — AA-cleared foreground stop + a separate fill tint each. Never a
  # text color from a `-bg` tint; never hue alone (a glyph and text always
  # carry the meaning).
  err: "#b3261e"
  err-bg: "#fbe9e7"
  warn: "#7a5c00"
  warn-bg: "#f7f0d8"
  info: "#2f5f8f"
  info-bg: "#e6eef7"
  ok: "#1e7a3c"
  ok-bg: "#e3f3e8"
  # Categorical node type — fill/border hues; label text uses the `-ink` stop
  # (`color-mix(in srgb, <hue> 85%, black)` in tokens.css).
  cat-reg: "#1f7a7a"
  cat-var: "#4b4b9e"
  cat-code: "#9a6b1a"
  cat-class: "#7a3f8c"
  cat-group: "#2f6b3f"
  # Facet-axis data palette — concept-group dimensions only.
  facet-axis-0: "#4f6f8f"
  facet-axis-1: "#6f7c3f"
  facet-axis-2: "#8a5f88"
  facet-axis-3: "#9a6a42"
  facet-axis-4: "#4f8074"
  facet-axis-5: "#6b6794"
  # Data-viz edge — graph relation marks, not chrome.
  viz-edge-succession: "#4a6b86"

typography:
  display:
    fontFamily: Schibsted Grotesk
    fontSize: 2.25rem
    fontWeight: 600
    lineHeight: 1.15
    letterSpacing: -0.01em
  h1:
    fontFamily: Schibsted Grotesk
    fontSize: 1.75rem
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: -0.01em
  h2:
    fontFamily: Schibsted Grotesk
    fontSize: 1.375rem
    fontWeight: 600
    lineHeight: 1.25
  h3:
    fontFamily: Schibsted Grotesk
    fontSize: 1.125rem
    fontWeight: 600
    lineHeight: 1.3
  body:
    fontFamily: Schibsted Grotesk
    fontSize: 1rem
    fontWeight: 400
    lineHeight: 1.5
  body-sm:
    fontFamily: Schibsted Grotesk
    fontSize: 0.85rem
    fontWeight: 400
    lineHeight: 1.45
  label:
    fontFamily: Schibsted Grotesk
    fontSize: 0.75rem
    fontWeight: 500
    lineHeight: 1.3
    letterSpacing: 0em
  mono:
    fontFamily: IBM Plex Mono
    fontSize: 0.92em
    fontWeight: 400
    lineHeight: 1.5

rounded:
  sm: 5px
  md: 8px

spacing:
  1: 0.25rem
  2: 0.5rem
  3: 0.75rem
  4: 1rem
  rail: 16rem
  content-max: 80rem
  breakpoint-narrow: 48rem

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
    textColor: "{colors.accent-ink}"
    rounded: "{rounded.sm}"
    typography: "{typography.body-sm}"
  button-danger:
    backgroundColor: "{colors.err}"
    textColor: "{colors.white}"
    rounded: "{rounded.sm}"
    typography: "{typography.body-sm}"
  panel:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text}"
    rounded: "{rounded.md}"
  table-header:
    backgroundColor: "{colors.surface}"
    textColor: "{colors.text-muted}"
    typography: "{typography.label}"
  row-selected:
    backgroundColor: "{colors.surface-selected}"
    textColor: "{colors.text}"
  tag:
    rounded: "{rounded.sm}"
    typography: "{typography.mono}"
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
same names, and `bun run test` fails when the two disagree. Engineering rationale for
the SPA (routing, primitives' ARIA decisions, test setup) stays in
`reg_webapp/DESIGN.md`; on a conflict about *how something looks*, this file wins.

## Overview

The product is a catalog of Swedish register metadata and a workbench for assembling a
data order from it. The people using it are academic researchers and data stewards who
scan long lists of registers, variables, value-set codes and classifications, compare
periods and editions, and build a `project_data.json`. Their task is reading, not being
persuaded: they want density, exactness, stable positions, and machine identifiers that
look like machine identifiers.

The look is ink on neutral paper. The base is monochrome: near-black text and controls
on a very light neutral canvas, white working surfaces, hairline borders. Color is
information, never decoration: it appears only where it encodes something a reader must
distinguish at a glance (a validation state, a node type, a graph edge). This is a
deliberate departure from the earlier warm-cream-and-terracotta scheme, which had become
the default look of generated interfaces and therefore stopped reading as a choice.

Personality: quiet, exact, archival. The nearest relatives are Linear and Observable,
not a marketing site and not a broadsheet. The app-level identity is already spent on
the typeface pairing and the shell; the win condition for any new view is that it is
indistinguishable in language from the best existing view. The novelty budget per change
is roughly zero.

Planned axis: per-provider themes. Because the base is neutral, a provider theme (SCB,
Socialstyrelsen, Försäkringskassan, …) will be a remap of the `accent*` roles under a
`[data-provider]` attribute and nothing else. Do not pre-build it; design so that it
stays that small.

## Colors

The neutral ramp `gray-1` … `gray-12` runs from ink to paper. Every role references a
ramp stop or a fixed status/categorical hue; components read roles only, never ramp
stops and never literals. That discipline is what makes the dark theme (deferred) and
provider themes pure role remaps.

- **Paper (`bg` #f7f7f6) and surfaces (`surface` #ffffff, `surface-sunken` #efefee):**
  the canvas is a hair off white so white panels and tables read as the working surface.
  Sunken is the rail, hover rows, and inset wells. Selection is one ramp stop darker
  (`surface-selected` #e6e6e5) plus a 3 px ink bar on the leading edge, so a selected
  row never depends on tint alone.
- **Ink (`text` #151618, `text-muted` #5f6368, `text-faint` #6b6f74):** three text
  strengths, all AA on paper and on surfaces (16.9:1, 5.6:1, 4.7:1). Faint is for
  metadata that must stay legible, not for hiding things.
- **Accent (`accent` = ink):** interactive chrome is ink. Primary buttons are ink-filled
  with white text (18:1). Links are ink at weight 600 and underline on hover; inside
  row-navigation tables the row hover is the affordance and link underlines are
  suppressed. The focus ring is a 2 px `accent-bg` gap and a 3 px ink line, so it is
  visible on ink-filled controls too. `accent-ink` exists for callers that already
  distinguish fill from text; both resolve to ink in this theme.
- **Status (`err`, `warn`, `info`, `ok` and their `-bg` tints):** the only chromatic
  chrome. Foregrounds clear AA on paper and on their own tints (≥4.7:1). Status meaning
  always travels with a glyph and text; hue alone is never the carrier. Never use a
  `-bg` tint as a text color, and never use a status hue for emphasis, selection, or
  brand.
- **Categorical node type (`cat-reg` teal, `cat-var` indigo, `cat-code` gold,
  `cat-class` plum, `cat-group` moss):** tags what a node *is* in search results and
  listings. Fill and border take the raw hue at a 10 % tint; label text takes the `-ink`
  stop (85 % toward black) so every type label clears AA. A separate sub-system: never
  reuse it for status or selection, or "this is a variable" collides with "this is
  selected / an error".
- **Facet axes (`facet-axis-0..5`) and data-viz edges (`viz-edge-succession`):** data
  encodings for concept-group dimensions and graph relation marks. Data, not chrome, not
  status. They never borrow from the other sub-systems.

Named hues stay deliberately cooler or more saturated than the neutral base so that no
status or type color can be mistaken for chrome.

## Typography

Two families, both self-hosted woff2 under the SIL Open Font License, never a CDN:

- **Schibsted Grotesk** for all UI and display text. A Scandinavian media-house
  grotesque: domain-authentic and distinct from the system and Inter/Roboto defaults.
  Weights 400, 500, 600 (700 is loaded for legacy use; new work stays at 600 and below).
- **IBM Plex Mono** for every machine identifier: FQIDs, slugs, value-set codes, years,
  counts, versions. The catalog is full of identifiers; setting them in mono is what
  lets a reader separate the thing from its name. Mono is for identifiers only, never
  for labels or body copy.

The scale is `display`, `h1`–`h3`, `body`, `body-sm`, `label`, `mono` (front matter).
Hierarchy comes from size and weight, not from decoration: headings are sentence case at
600, body is 400 at 1rem with 1.5 line height, and the `label` level (0.75rem, 500,
muted) is the small label used for table headers, panel titles, rail section names and
key-value terms. Labels are **sentence case with normal tracking**. The earlier tracked
uppercase eyebrow is retired: it was the most recognizable generated-interface tell and
it carried no information the weight and color do not.

Line length stays under 80 characters for prose; tables and identifiers are exempt.
Swedish text with å/ä/ö is the norm, so both families ship the latin and latin-ext
subsets.

## Layout

An app shell, not a centered column: a persistent 16 rem left rail (brand, primary nav,
project card, study window, providers), a topbar with breadcrumb and the command bar
(`Meta/Ctrl+K`, shown as `⌘K` on macOS), and a wide content canvas capped at 80 rem.
Below the single narrow breakpoint (48 rem) the rail becomes a drawer and tables stack
into cards. Do not mint new breakpoints; compose with the existing 48 rem patterns.

Spacing is a 0.25 rem rhythm (`1`–`4` = 0.25–1 rem); larger gaps are multiples of it,
set by the container that owns the gap, never by margins on children. Density is tuned
for scanning long lists: rows are 9–10 px of vertical padding, panels 12 px, and there
is no marketing whitespace. Every rendered change is designed for 375, 768, 1280 and
1920 px wide; long FQIDs, slugs and Swedish labels wrap or scroll deliberately, and the
page body never scrolls horizontally.

Panels are the unit of grouping: a header (label-level title, optional meta) over a
body. Tables are the workhorse: label-level headers, right-aligned mono numerics,
hairline rows, no zebra, hover and keyboard-selected states. The first column is the
title column by position and becomes the card title when the table stacks.

## Elevation & Depth

Depth is tonal, not cast. Paper under white surfaces, sunken wells one stop darker,
hairline borders (`border` #dcdde0, `border-strong` #b9bbc0 for inputs and the drawer
edge). One soft shadow token (`elevation-raised`) for popovers, menus and the mobile
drawer; nothing else casts a shadow. A 40 % ink scrim sits behind dialogs and the
drawer. No glows, gradients, blobs, textures or glass.

## Shapes

Two radii: `sm` 5 px on controls (buttons, inputs, chips, tags) and `md` 8 px on panels
and cards. Nothing is a pill, and radii are never mixed on one element family. Selection
and active-nav marks are a straight 3 px bar on the leading edge, not a rounded
highlight.

## Components

Behavior comes from Bits UI (the sanctioned headless primitives dependency); visuals
come from scoped CSS reading semantic roles. The shared primitives live in `src/lib/ui/`
and are composed before anything new is styled: `Panel`, `DataTable`, `Breadcrumbs`,
`Tag`, `Button`, `KeyValue`, `Skeleton`, `EmptyState`, plus `AppShell` in `src/lib/`.

- **Buttons:** `primary` is ink-filled with white text and there is at most one per
  view; `default` is a white surface with a hairline border; `ghost` is borderless ink
  text; `danger` is the error fill for destructive actions only. Two sizes (`md`, `sm`),
  `sm` at label size. A button says what happens: "Add to project", never "Submit".
- **Tags:** mono, `sm` radius, a hairline border in the tone's hue. Chrome tones
  (`neutral`, `accent`), categorical type tones (`reg`/`var`/`code`/`class`/`group`),
  and status tones (`error`/`warn`/`info`/`ok`) that require a leading glyph.
- **Tables (`DataTable`):** explicit ARIA roles, label-level headers, mono/numeric
  column flags, `framed` when the table is the whole surface (never inside a `Panel`),
  stacked cards below 48 rem with the header row kept in the accessibility tree.
- **Panels:** white surface, `md` radius, hairline border, header with a label-level
  title.
- **Key-value lists:** label-level terms, mono values for identifiers.
- **Inputs and command palette:** white surface, `border-strong` hairline, focus ring on
  `:focus-visible`, placeholder in `text-muted`. Search and pickers use Bits UI
  `Command`/`Combobox` with app-side ranking.
- **Banners and status rows:** the status tint as fill, the status foreground as text, a
  glyph first, plain sentence copy that says what went wrong and how to fix it.
- **Empty and loading states:** `EmptyState` names the next action; `Skeleton` exposes
  `aria-busy="true"` (the screenshot driver waits on it) and respects reduced motion.

## Do's and Don'ts

- Do read roles only: `var(--text)`, never `var(--gray-1)`, never a literal. The
  `style_tokens` test fails on literals and raw font stacks in any `<style>` block.
- Do keep the accent monochrome and reserve every chromatic color for a meaning (status,
  type, data). Do not tint headings, counts or emphasis.
- Do pair every status with a glyph and text. Don't rely on hue.
- Do use mono for identifiers only. Don't set labels, headings or copy in mono.
- Do write labels and headings in sentence case at normal tracking. Don't use uppercase
  tracked eyebrows, numbered section markers, middle-dot meta strings, or trailing
  arrows on links and buttons.
- Do compose `lib/ui` primitives and Bits UI behavior. Don't hand-roll a widget Bits UI
  covers (combobox, menu, dialog, popover, tooltip, slider, tabs, accordion) or a
  near-duplicate of a primitive.
- Do keep one `primary` button per view and one memorable element per screen. Don't add
  hero sections, decorative gradients or orbs, oversized cards, cards inside cards, or
  stock imagery; the home route is the entry screen of a tool.
- Do design for 375, 768, 1280 and 1920 px and verify with `dev.sh shot --all`. Don't
  add breakpoints or `display: contents` (it strips accessibility roles).
- Do keep focus visible via `:focus-visible { box-shadow: var(--focus-ring) }` in each
  component's own CSS, and respect `prefers-reduced-motion`. Motion is short (140 ms)
  and answers a user action; nothing animates on page load.
- Do name things by what a researcher recognizes (registers, variables, value sets,
  projects) and keep an action's name the same through its whole flow. Don't apologize
  in errors or leave them vague.
