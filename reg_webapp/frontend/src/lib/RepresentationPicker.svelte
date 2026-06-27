<script lang="ts">
import {
  type BandIdentity,
  bandLabeling,
  leafSlug,
  type PickerRepresentation,
  pickerLabeling,
  representationInWindow,
  yearOf,
} from "./catalog";
import { router } from "./router.svelte";

// The direct COLUMN picker (#678 redesign): ONE compact, integrated list of a
// concept's delivery columns with a shared selection basket and a single "Add"
// footer. The binding leaf passes its single variable; the concept-group page passes
// one entry per member variable — and the two render essentially identically (the
// user is selecting a CONCEPT's columns, not reasoning about the underlying
// variables). Light hierarchy, no card chrome, no default collapse: every column is
// visible. A multi-column variable gets a thin subheading row (its distinguishing
// identity + a "select all" toggle) over its column rows; a single-column variable
// collapses to ONE selectable row. Thin + presentational: the parent owns the data
// (enumerates each variable's `rows`) and the store wiring (`onadd`); this owns the
// cross-variable selection + the layout.

/** One variable the picker lists — its identity + its delivery-column rows. `key` is
 * GLOBALLY unique (the member fqid for a group, the leaf fqid for the leaf) so it
 * namespaces the variable's column selection keys in the cross-variable set. (The
 * type name keeps "Band" for continuity with the consumers; the UI says "column".) */
export interface PickerBand {
  key: string;
  name: string;
  registerPrefix: string;
  facetLabel?: string | null;
  /** Per-DELIVERY-COLUMN human facet label (#678): a representation group can carry
   * several members on ONE variable, each a distinct `delivery_column` with its own
   * facet (e.g. CDISP "Inkl. kapitalvinst" vs CDISP5 "Exkl. kapitalvinst"). The band
   * is built per DISTINCT fqid, so without this the later members' facet labels never
   * reach their rows. Keyed by `delivery_column`, it lets the picker show the human
   * facet label per column rather than only the technical column name. The GROUP view
   * sets it; the binding LEAF leaves it undefined (its single member has no per-column
   * facet split). */
  facetByColumn?: Record<string, string>;
  isSensitive?: boolean;
  isIdentifier?: boolean;
  rows: PickerRepresentation[];
  /** The variable's leaf page href (the GROUP view sets it per member —
   * `catalogHref(member.fqid)` — so the picker can link to each member's own page).
   * Undefined for the binding LEAF (it's already that page — no self-link). When set,
   * the variable IDENTITY becomes a navigation link, kept DISTINCT from the selection
   * checkbox. */
  href?: string;
}

/** A committed selection — the variable it belongs to plus the picked column, so the
 * parent's `onadd` maps each to the right per-variable `addFromCatalog`. */
export interface PickerSelection {
  band: PickerBand;
  row: PickerRepresentation;
}

let {
  bands,
  window,
  canAdd,
  focusKey = null,
  onadd,
}: {
  /** The variables, in render order. One element for the leaf; one per member for
   * the group page. */
  bands: PickerBand[];
  /** The active period window as an inclusive year pair, or null (no narrowing → no
   * column dims). Columns whose span doesn't overlap render dimmed (still selectable). */
  window: [number, number] | null;
  /** Whether the Add action is permitted (the deployment seed is ready). When false
   * the button stays disabled regardless of selection. */
  canAdd: boolean;
  /** The `band.key` of a band to visually MARK as the deep-link focus (#678): the
   * group page passes the `?member=` hint's band so a `?member=<slug>` deep link
   * renders with that member highlighted, restoring the focus affordance. Null (the
   * leaf, or no hint) marks nothing. */
  focusKey?: string | null;
  /** Commit the selected columns across all variables. The parent maps each
   * `{ band, row }` to an `addFromCatalog` call and renders the confirmation. */
  onadd: (selected: PickerSelection[]) => void;
} = $props();

/** The cross-variable selection — namespaced keys `${band.key}::${row.key}` so two
 * variables sharing a `(variant, column)` row key never collide. Reassigned (not
 * mutated) so the `$state` Set is reactive. Reset when the variable set changes
 * underneath (a re-enumeration / a different group) so a stale key can never commit
 * a vanished column. */
let selectedKeys = $state(new Set<string>());
const bandsSignature = $derived(
  bands.map((b) => `${b.key}:${b.rows.map((r) => r.key).join(",")}`).join("|"),
);
$effect(() => {
  void bandsSignature;
  selectedKeys = new Set<string>();
});

/** The namespaced selection key for a variable's column row. */
function selKey(bandKey: string, rowKey: string): string {
  return `${bandKey}::${rowKey}`;
}

/** Toggle one column's selection. Reassigns the Set so `$state` stays reactive. */
function toggleRow(bandKey: string, rowKey: string): void {
  const sel = selKey(bandKey, rowKey);
  const next = new Set(selectedKeys);
  if (next.has(sel)) {
    next.delete(sel);
  } else {
    next.add(sel);
  }
  selectedKeys = next;
}

/** Whether EVERY column of a variable is selected — the variable-level "select all"
 * checked state (and the indeterminate complement: some-but-not-all). */
function allOfBandSelected(band: PickerBand): boolean {
  return (
    band.rows.length > 0 &&
    band.rows.every((r) => selectedKeys.has(selKey(band.key, r.key)))
  );
}
function someOfBandSelected(band: PickerBand): boolean {
  return band.rows.some((r) => selectedKeys.has(selKey(band.key, r.key)));
}

/** Select or clear every column of one variable in a single move (the per-variable
 * "select all columns of <identity>" affordance). */
function toggleBand(band: PickerBand): void {
  const next = new Set(selectedKeys);
  const select = !allOfBandSelected(band);
  for (const r of band.rows) {
    const sel = selKey(band.key, r.key);
    if (select) {
      next.add(sel);
    } else {
      next.delete(sel);
    }
  }
  selectedKeys = next;
}

/** The variable currently hovered at the SUBHEADING level — its column rows get the
 * row-hover highlight (signalling they move together). Set on subhead enter/leave;
 * the rows are sibling `<li>`s, so a JS `$state` flag scopes the highlight. */
let hoveredBandKey = $state<string | null>(null);

/** Every column key across all variables — the global select-all target. */
const allKeys = $derived(
  bands.flatMap((b) => b.rows.map((r) => selKey(b.key, r.key))),
);
const allSelected = $derived(
  allKeys.length > 0 && allKeys.every((k) => selectedKeys.has(k)),
);
const someSelected = $derived(allKeys.some((k) => selectedKeys.has(k)));

/** Select or clear every column across every variable in one move. */
function toggleAll(): void {
  selectedKeys = allSelected ? new Set() : new Set(allKeys);
}

/** The selected columns across all variables, in variable-then-column order — the
 * commit payload. A namespaced key that no longer resolves is skipped. */
const selected = $derived.by((): PickerSelection[] => {
  const out: PickerSelection[] = [];
  for (const band of bands) {
    for (const row of band.rows) {
      if (selectedKeys.has(selKey(band.key, row.key))) {
        out.push({ band, row });
      }
    }
  }
  return out;
});
const selectedCount = $derived(selected.length);

function commit(): void {
  if (selectedCount === 0 || !canAdd) {
    return;
  }
  onadd(selected);
}

/** The DISTINCT delivery columns a variable's rows address. A member whose rows all
 * deliver the SAME one column is a single-column member — its several rows are
 * POPULATIONS of that column, not distinct columns. */
function distinctColumns(band: PickerBand): string[] {
  return [...new Set(band.rows.map((r) => r.column).filter(Boolean))];
}

/** Whether the band is a SINGLE-COLUMN member — every row delivers one and the same
 * delivery column. Its identity is then that column (rendered as the chip-LINK); only
 * a genuinely multi-column member falls back to the leaf slug. */
function distinguisherIsColumn(band: PickerBand): boolean {
  return distinctColumns(band).length === 1;
}

/** A variable's distinguishing technical differentiator: its sole delivery column when
 * every row delivers the same one (a column-led group reads `SNI2002`/`SNI2007_Ag` —
 * the several rows are populations), else the member leaf slug — the fallback for a
 * genuinely multi-column variable. */
function distinguisherOf(band: PickerBand): string {
  const cols = distinctColumns(band);
  return cols.length === 1 ? cols[0] : leafSlug(band.key);
}

/** The adaptive variable-IDENTITY labeling across the members (#678): hoist constant
 * dimensions (the name → the page <h2>; the prefix → the breadcrumb) and lead each
 * variable with its first VARYING identity (name → facet → column/slug). A single
 * variable (the leaf) lands on the name fallback — so the leaf leads with its name. */
const identity = $derived(
  bandLabeling(
    bands.map(
      (b): BandIdentity => ({
        name: b.name,
        registerPrefix: b.registerPrefix,
        facetLabel: b.facetLabel ?? null,
        distinguisher: distinguisherOf(b),
        distinguisherIsColumn: distinguisherIsColumn(b),
      }),
    ),
  ),
);

/** Per-variable adaptive COLUMN labels (#678 1b) — show only what varies within the
 * variable, constants hoisted to a thin context line. Keyed by variable key. */
const labelingByBand = $derived(
  new Map(bands.map((b) => [b.key, pickerLabeling(b.rows)])),
);

/** Whether the active window starts BEFORE a row's data actually begins — the
 * "data starts late" warning trigger (#678). Only when a window is set, the row's
 * start year resolves (skip the open/unknown-start `0001-01-01` sentinel → `yearOf`
 * null or 1), and that start year is STRICTLY after the window's start. The data
 * start year, or null when no warning applies. */
function dataStartsLate(
  row: PickerRepresentation,
): { dataStart: number; windowStart: number } | null {
  if (!window) {
    return null;
  }
  const start = yearOf(row.from);
  // Skip unknown/open starts (the yearless sentinel reads as year 1): no real
  // "data begins later" claim there.
  if (start === null || row.from === "0001-01-01" || start <= window[0]) {
    return null;
  }
  return { dataStart: start, windowStart: window[0] };
}

/** The render model per variable: its leading identity, whether it is a single
 * column (→ one merged row, no subheading), the hoisted COLUMN chip + the quiet
 * value-set context, the adaptive per-row column labels, and whether EVERY one of its
 * rows is out of the active window (→ dim the subheading too). */
const view = $derived(
  bands.map((band, i) => {
    const id = identity.bands[i];
    const labeling = labelingByBand.get(band.key);
    const single = band.rows.length === 1;
    // The hoisted constant delivery column → a prominent chip in the context. But when
    // the variable's IDENTITY already IS that column (a single-column member — its
    // primary is the column chip-link, whether single-row or a multi-population
    // subheading), it's shown once as that identity, so suppress the duplicate context
    // chip here.
    const column = id.primaryIsColumn ? null : (labeling?.column ?? null);
    // ALL-OUT: every row out of the active window → the (multi-column) subheading
    // greys at the variable level too. A 0-row band is NOT all-out (nothing to scope).
    const allOut =
      band.rows.length > 0 &&
      band.rows.every((r) => !representationInWindow(r, window));
    return {
      band,
      primary: id.primary,
      primaryIsColumn: id.primaryIsColumn,
      single,
      column,
      allOut,
      // The deep-link `?member=` focus (#678): mark this band when its key matches.
      focused: focusKey != null && band.key === focusKey,
      context: labeling?.headerContext ?? [],
      rowLabels: new Map((labeling?.rows ?? []).map((r) => [r.key, r])),
    };
  }),
);

const footerLabel = $derived(
  `${selectedCount} ${selectedCount === 1 ? "column" : "columns"} selected`,
);

/** Navigate an in-picker identity link (the column chip / subhead title) through
 * the SPA ROUTER. The link sits inside a <label> wrapping the row's checkbox, so a
 * plain click would (a) toggle the checkbox and (b) — because we must keep it from
 * toggling — previously `stopPropagation`'d, which ALSO stopped the app-level
 * `use:link` delegated handler from intercepting the bubbled click → a full page
 * reload (losing app state, tripping the dirty-project beforeunload). Instead:
 * `preventDefault()` (kills the label toggle) + navigate via the router directly.
 * The bubbled click still reaches `use:link`'s `onNavClick`, but it early-returns on
 * `defaultPrevented`, so there's no double navigation. Modifier / non-primary
 * clicks (open-in-new-tab etc.) fall through to the browser, matching `onNavClick`. */
function navigateChip(event: MouseEvent, href: string): void {
  if (
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  ) {
    return;
  }
  event.preventDefault();
  router.navigate(href);
}
</script>

<!-- The delivery COLUMN chip (#678): the main selection signal, rendered as a small
     categorical pill (mono text + a subtle --cat-var tint, distinct from the rost
     selection accent) wherever a delivery column shows. When `href` is set (a single-
     column variable's IDENTITY chip in the group view), the chip is a NAVIGATION LINK
     to that variable's leaf page — clicking it navigates (via the SPA router, see
     `navigateChip`) and must NOT toggle the row selection. Otherwise a plain <code>. -->
{#snippet colChip(text: string, href?: string)}
  {#if href}
    <a
      class="col-chip link"
      {href}
      title={`Open ${text}`}
      onclick={(e) => navigateChip(e, href)}
      >{text}<span class="chip-arrow" aria-hidden="true">↗</span></a
    >
  {:else}
    <code class="col-chip" title={`Delivery column ${text}`}>{text}</code>
  {/if}
{/snippet}

<!-- The DATA-STARTS-LATE warning (#678): a quiet --warn marker shown immediately
     before the period when the active window starts BEFORE this column's data begins
     (the user asked from <windowStart> but data only starts <dataStart>). A text glyph
     (no icon webfont); kept outside the right-aligned year text so it never breaks the
     tabular-nums alignment. Only on IN-window rows — a fully-out row is already dimmed. -->
{#snippet lateWarn(row: PickerRepresentation)}
  {@const late = dataStartsLate(row)}
  {#if late}
    {@const msg = `Data starts ${late.dataStart} — your selected period begins ${late.windowStart}`}
    <span class="late-warn" title={msg} aria-label={msg}>⚠</span>
  {/if}
{/snippet}

<div class="rep-picker">
  {#if bands.length > 1 && allKeys.length > 1}
    <!-- Global select-all: grab every column of the concept (for the active period)
         in one move. A header strip, not card chrome. Omitted when there's only ONE
         variable — that variable's own select-all IS "select all columns", so a
         global one would just duplicate it. -->
    <div class="picker-head">
      <label class="select-all">
        <input
          type="checkbox"
          class="cbox"
          checked={allSelected}
          indeterminate={someSelected && !allSelected}
          aria-label="Select all columns"
          onchange={toggleAll}
        />
        <span>Select all columns</span>
      </label>
    </div>
  {/if}

  <ul class="col-list">
    {#each view as v (v.band.key)}
      {@const band = v.band}
      {#if v.single}
        {@const row = band.rows[0]}
        {@const checked = selectedKeys.has(selKey(band.key, row.key))}
        {@const inWindow = representationInWindow(row, window)}
        {@const facet = band.facetByColumn?.[row.column]}
        <!-- A single-column variable = ONE selectable row, led by the variable's
             distinguishing identity (the leaf ≈ one-variable group case). The row is a
             click-anywhere container (mouse toggles selection); a real checkbox owns
             keyboard. When the variable has an `href` (group view) the COLUMN CHIP is
             itself the navigation link to its leaf — clicking the chip navigates
             (via the SPA router, `navigateChip`), not toggles; there's no separate
             "View" link. -->
        <li class="col-row single" class:focused={v.focused}>
          <!-- The whole row is a <label> wrapping the checkbox: clicking ANYWHERE in
               it toggles selection natively (no JS, keyboard via the input). The chip-
               link inside `preventDefault`s + router-navigates so a nav click neither
               toggles the row NOR full-reloads the app. -->
          <label class="row-btn" class:selected={checked} class:dimmed={!inWindow}>
            <!-- No aria-label: the wrapping <label>'s text content (the column chip +
                 population + value set + period) names the checkbox for AT. -->
            <input
              type="checkbox"
              class="cbox"
              checked={checked}
              onchange={() => toggleRow(band.key, row.key)}
            />
            <span class="row-main">
              <span class="primary-line">
                {#if v.primary.mono}
                  <!-- The primary IS the delivery column → the prominent column chip,
                       a nav LINK when the variable has its own leaf page (group). -->
                  {@render colChip(v.primary.text, band.href)}
                {:else}
                  <span class="primary">{v.primary.text}</span>
                {/if}
                {#if v.column}
                  <!-- A constant delivery column hoisted alongside a non-column
                       primary (e.g. a name-led row) → the column chip (nav link). -->
                  {@render colChip(v.column, band.href)}
                {/if}
                {#if identity.showPrefix}
                  <code class="register-prefix">{band.registerPrefix}</code>
                {/if}
                {#if band.isIdentifier}
                  <span class="badge" title="Identifier">id</span>
                {/if}
                {#if band.isSensitive}
                  <span class="badge sensitive" title="Sensitive">sensitive</span
                  >
                {/if}
              </span>
              <!-- The column's human facet label (#678) leads the quiet context for a
                   single-column representation member (e.g. CDISP "Inkl. kapitalvinst")
                   so the row shows the human distinction, not only the column name. -->
              {#if facet || v.context.length > 0}
                <span class="sub"
                  >{[facet, ...v.context].filter(Boolean).join(" · ")}</span
                >
              {/if}
            </span>
            {#if row.codingsVary}
              <!-- A coding change over time on this ONE column (distinct value_set_id
                   across years). A quiet nudge to the value-set / States detail —
                   not a control. Placed BEFORE the period so the period stays the
                   last, right-aligned element on every row (aligned column). -->
              <span
                class="codings-vary"
                title="Coding changes over time — see the value sets"
                aria-label="Coding changes over time — see the value sets"
                >codings vary</span
              >
            {/if}
            {#if inWindow}
              {@render lateWarn(row)}
            {/if}
            {#if row.period}
              <span class="period">{row.period}</span>
            {/if}
          </label>
        </li>
      {:else}
        <!-- A multi-column variable: a thin, quiet subheading (its distinguishing
             identity + a "select all" toggle) over its column rows. No card chrome —
             a hairline separates the group from the rest of the list. HOVERING the
             subheading highlights ALL its column rows (they move together); CLICKING
             anywhere on it toggles ALL its columns (mirrors the select-all checkbox),
             except the title nav link (which `preventDefault`s + router-navigates) +
             the checkbox. -->
        {@const empty = band.rows.length === 0}
        <!-- Grey the whole subheading when EVERY column is out of the active window —
             the variable reads as out-of-scope at the variable level, not just per
             row (#678). A FULLY-selected variable carries the same rust left bar the
             selected rows do (only full selection — not partial — mirrors the fill). -->
        {@const fullySelected = allOfBandSelected(band)}
        <li
          class="subhead"
          class:empty
          class:dimmed={v.allOut}
          class:selected={fullySelected}
          class:focused={v.focused}
        >
          <!-- The identity chrome (primary + name/prefix/badges). When the variable
               has an `href` (group view) the title is a navigation LINK; otherwise
               plain text. The select-all checkbox is the control; the title link is
               separate, so navigation and selection never share a target. -->
          <!-- The leading identity. A SINGLE-COLUMN member (`primaryIsColumn`) leads
               with its delivery column as the prominent chip-LINK (the chip itself
               navigates to the member's leaf — no outer link/↗, like the single-row
               identity); a multi-column member leads with its mono slug (wrapped in
               the subhead-title nav link below). -->
          {#snippet identityPrimary()}
            {#if v.primaryIsColumn}
              {@render colChip(v.primary.text, band.href)}
            {:else if v.primary.mono}
              <code class="primary mono">{v.primary.text}</code>
            {:else}
              <span class="primary">{v.primary.text}</span>
            {/if}
          {/snippet}
          {#snippet identityMeta()}
            {#if identity.showName && band.name !== v.primary.text}
              <span class="var-name">{band.name}</span>
            {/if}
            {#if identity.showPrefix}
              <code class="register-prefix">{band.registerPrefix}</code>
            {/if}
            {#if band.isIdentifier}
              <span class="badge" title="Identifier">id</span>
            {/if}
            {#if band.isSensitive}
              <span class="badge sensitive" title="Sensitive">sensitive</span>
            {/if}
            {#if empty}
              <span class="empty-note">No columns</span>
            {/if}
          {/snippet}
          {#snippet identityInner()}
            {@render identityPrimary()}
            {@render identityMeta()}
          {/snippet}
          {#snippet subheadContext()}
            {#if v.column || v.context.length > 0}
              <span class="subhead-context">
                {#if v.column}
                  <!-- The constant delivery column (when it doesn't vary across this
                       variable's rows) → the prominent column chip (NOT a nav link for
                       a multi-column member, so it is part of the select-all surface). -->
                  {@render colChip(v.column)}
                {/if}
                {#if v.context.length > 0}
                  <span class="ctx-text">{v.context.join(" · ")}</span>
                {/if}
              </span>
            {/if}
          {/snippet}
          {#if empty}
            <div class="subhead-row">
              <span class="subhead-title">{@render identityInner()}</span>
            </div>
            {@render subheadContext()}
          {:else}
            <!-- The WHOLE subheading is one <label> wrapping the select-all checkbox:
                 a click ANYWHERE on it — the title, the column chip, OR the description
                 line — toggles all columns natively (the title nav link inside
                 `preventDefault`s + router-navigates so a nav click never toggles).
                 Hovering anywhere sets the
                 band-hover key → all this variable's column rows highlight together. -->
            <label
              class="subhead-label"
              onmouseenter={() => (hoveredBandKey = band.key)}
              onmouseleave={() => (hoveredBandKey = null)}
            >
              <input
                type="checkbox"
                class="cbox"
                checked={allOfBandSelected(band)}
                indeterminate={someOfBandSelected(band) &&
                  !allOfBandSelected(band)}
                aria-label={`Select all columns of ${v.primary.text}`}
                onchange={() => toggleBand(band)}
              />
              <!-- The title + description share ONE wrapping line: when they fit they
                   sit on one row (a dot separates them); when they don't, the
                   description wraps WHOLE to its own row so the heading stays intact
                   (the description is a single flex item — it never breaks mid-line
                   beside the heading). Keeps the table compact. -->
              <span class="subhead-body">
                {#if v.primaryIsColumn}
                  <!-- Single-column member: the column chip-LINK IS the identity (the
                       chip navigates; its color-deepen hover is the affordance). No
                       outer nav link / ↗ — the chip itself is the link, mirroring the
                       single-row identity. -->
                  <span class="subhead-title">{@render identityInner()}</span>
                {:else if band.href}
                  {@const href = band.href}
                  <a
                    class="subhead-title link"
                    {href}
                    title={`Open ${v.primary.text}`}
                    onclick={(e) => navigateChip(e, href)}
                  >
                    {@render identityInner()}
                    <span class="open-marker" aria-hidden="true">↗</span>
                  </a>
                {:else}
                  <span class="subhead-title">{@render identityInner()}</span>
                {/if}
                {@render subheadContext()}
              </span>
            </label>
          {/if}
        </li>
        {#each band.rows as row (row.key)}
          {@const checked = selectedKeys.has(selKey(band.key, row.key))}
          {@const inWindow = representationInWindow(row, window)}
          {@const label = v.rowLabels.get(row.key)}
          {@const facet = band.facetByColumn?.[row.column]}
          <!-- A nested column row: the SAME <label>-wraps-checkbox click-anywhere
               pattern as the single row, minus the nav link (a nested column is not
               its own variable). Gets the band-hover highlight when its subheading is
               hovered. -->
          <li class="col-row nested">
            <label
              class="row-btn"
              class:selected={checked}
              class:dimmed={!inWindow}
              class:band-hover={hoveredBandKey === band.key}
            >
              <!-- No aria-label: the <label> text (column chip + value-set + period)
                   names the checkbox for AT. -->
              <input
                type="checkbox"
                class="cbox"
                checked={checked}
                onchange={() => toggleRow(band.key, row.key)}
              />
              <span class="row-main">
                {#if label?.primary.mono}
                  <!-- A mono primary here is the varying DELIVERY COLUMN → chip (NOT a
                       link — these columns aren't separate variables). -->
                  {@render colChip(label.primary.text)}
                {:else}
                  <span class="primary">{label?.primary.text}</span>
                {/if}
                <!-- The human FACET label for this column (#678): a representation
                     group with several members on one variable distinguishes its
                     columns by facet ("Inkl./Exkl. kapitalvinst"), not just the
                     technical column name. Shown as the leading qualifier so the row
                     reads as the human distinction, not only `CDISP`/`CDISP5`. -->
                {#if facet || (label && label.qualifiers.length > 0)}
                  <span class="sub"
                    >{[facet, ...(label?.qualifiers ?? [])]
                      .filter(Boolean)
                      .join(" · ")}</span
                  >
                {/if}
              </span>
              <!-- The codings-vary nudge sits BEFORE the period so the period is the
                   last, right-aligned element on every row (a clean aligned column
                   whether or not a row carries the nudge — #678 fix). -->
              {#if row.codingsVary}
                <span
                  class="codings-vary"
                  title="Coding changes over time — see the value sets"
                  aria-label="Coding changes over time — see the value sets"
                  >codings vary</span
                >
              {/if}
              {#if inWindow}
                {@render lateWarn(row)}
              {/if}
              <!-- Every row shows its own period on the right (the period is never in
                   the hoisted context now — #678 fix 5). Use the raw `row.period` so
                   a constant-period band still shows each row's span. -->
              {#if row.period}
                <span class="period">{row.period}</span>
              {/if}
            </label>
          </li>
        {/each}
      {/if}
    {/each}
  </ul>

  <div class="picker-footer">
    <span class="count" role="status">{footerLabel}</span>
    <button
      type="button"
      class="add-to-project"
      disabled={selectedCount === 0 || !canAdd}
      onclick={commit}
    >
      Add to project
    </button>
  </div>
</div>

<style>
  .rep-picker {
    display: flex;
    flex-direction: column;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--surface);
    overflow: hidden;
  }

  /* The global select-all strip — a quiet header, not a card. */
  .picker-head {
    padding: 0.4rem 0.75rem;
    border-bottom: 1px solid var(--border);
  }
  .select-all {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.8rem;
    color: var(--text-muted);
    cursor: pointer;
  }

  /* ONE dense list — hairlines, no per-variable boxes. */
  .col-list {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .col-list > li + li {
    border-top: 1px solid var(--border);
  }

  /* A thin, quiet variable subheading — the distinguishing identity + select-all.
     NOT a card and NO fill: the `.col-list` hairline top divider alone separates it,
     so several stacked subheadings read flat and integrated. Click-anywhere toggles
     all its columns (cursor: pointer); its own hover tint reinforces the band-hover
     highlight on the rows below. */
  .subhead {
    padding: 0.4rem 0.75rem 0.3rem;
    /* The same 3px transparent left bar the rows carry — lines up with `.row-btn`'s
       border-left so a fully-selected variable's rust bar is continuous down the
       variable. Turns rust only on FULL selection (below), mirroring the fill rule. */
    border-left: 3px solid transparent;
    cursor: pointer;
  }
  .subhead.empty {
    cursor: default;
  }
  /* Fully-selected variable → the rust left bar + accent fill, matching the selected
     rows below (`.row-btn.selected`) so the variable's own row reads as selected too. A
     partial (indeterminate) selection deliberately does NOT get the bar. The left
     BORDER is the selected distinguisher; `.focused` below shares the same fill but is
     marked by an inset box-shadow instead, so the two stay distinct even when combined. */
  .subhead.selected {
    background: var(--accent-bg);
    border-left-color: var(--accent);
  }
  /* All columns out of the active window → the subheading greys at the variable level
     (same muted treatment as a dimmed row; un-dims a touch on hover). */
  .subhead.dimmed {
    opacity: 0.45;
  }
  .subhead.dimmed:hover {
    opacity: 0.7;
  }
  .subhead:not(.empty):hover {
    background: var(--accent-bg);
  }
  /* The whole non-empty subheading is one <label> (the hover-all + click-all surface):
     the checkbox beside a wrapping body holding the title + description. */
  .subhead-label {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    cursor: pointer;
    /* Fill the whole subheading: pull the label out to the `.subhead` content edges
       (cancel its padding) then re-pad inside, so the hover/click surface covers the
       entire item — no dead padding ring at the top/sides that highlights the subhead
       but not its rows. */
    box-sizing: border-box;
    margin: -0.4rem -0.75rem -0.3rem;
    padding: 0.4rem 0.75rem 0.3rem;
  }
  /* The checkbox stays vertically centered against the (possibly wrapping) body. */
  .subhead-label > .cbox {
    align-self: center;
  }
  /* The title + description flow on ONE wrapping line: both fit → one row (a dot
     separates); the description (a single flex item) drops WHOLE to its own row when
     it can't fit beside the heading, so the heading never shares a line with a
     fragment of the description. */
  .subhead-body {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.1rem 0.5rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  /* The empty-band header keeps the simple one-row layout (no description to wrap). */
  .subhead-row {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }
  .subhead-title {
    display: inline-flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.4rem 0.6rem;
  }
  .subhead-title .primary {
    font-weight: 600;
  }
  /* The identity-as-navigation link (group view): inherits the text color so it
     reads as the heading, shifting to the accent color on hover (no underline —
     matching the app's other links) — the `↗` marks it as a link. Distinct from
     the select-all checkbox beside it. */
  .subhead-title.link {
    text-decoration: none;
    color: inherit;
  }
  .subhead-title.link:hover {
    color: var(--accent);
  }
  .subhead-title.link:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
    border-radius: var(--radius-sm);
  }
  .open-marker {
    font-size: 0.8em;
    color: var(--text-muted);
  }
  /* The description rides inline after the title (one flex item in `.subhead-body`),
     so it sits on the same row when it fits and wraps WHOLE to its own row otherwise.
     Its OWN content flows as inline text (not an inner flex) so the leading dot glues
     to the text and never strands on a line by itself; `min-width: 0` lets the text
     wrap once the description is on its own row. */
  .subhead-context {
    min-width: 0;
    font-size: 0.78rem;
    color: var(--text-muted);
  }
  /* The dot that separates the heading from the description when they share a row.
     (It leads the description's own row after a wrap — a quiet continuation cue,
     glued to the first word so it can't sit alone.) */
  .subhead-context::before {
    content: "·";
    margin-right: 0.3rem;
  }
  /* A multi-column member's context column chip sits before the text. */
  .subhead-context .col-chip {
    margin-right: 0.3rem;
  }
  .subhead-context .ctx-text {
    overflow-wrap: anywhere;
  }
  /* A 0-column variable: a plain subheading with a quiet "No columns" marker. */
  .empty-note {
    font-size: 0.8rem;
    font-style: italic;
    color: var(--text-muted);
  }

  /* The deep-link `?member=` FOCUS marker (#678): the band the link named is marked
     with a subtle accent tint + a left accent rule so a `?member=<slug>` deep link
     lands with that member visibly highlighted. Distinct from `.selected` (a rust
     fill on the rows) — focus is a softer attention cue on the band itself, and the
     `box-shadow` inset rule reads even alongside the selected left border. */
  .subhead.focused,
  .col-row.single.focused {
    background: var(--accent-bg);
    box-shadow: inset 3px 0 0 0 var(--accent);
  }

  /* A column row: a click-anywhere checkbox. The whole row toggles (real <button> +
     role=checkbox for keyboard/AT). Nested rows indent under their subheading. */
  /* A column row: a click-anywhere container (a <div> — the real checkbox inside owns
     keyboard). Hovering OR band-hovering it highlights; clicking it toggles. */
  .row-btn {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    width: 100%;
    /* A <label> defaults to content-box (unlike the <button> this replaced), so
       width:100% + padding would overflow the row to the right — clip the years.
       Border-box folds the padding back in. */
    box-sizing: border-box;
    padding: 0.4rem 0.75rem;
    font: inherit;
    text-align: left;
    background: transparent;
    border: none;
    border-left: 3px solid transparent;
    cursor: pointer;
  }
  .col-row.nested .row-btn {
    padding-left: 1.6rem;
  }
  /* Hover (the row itself) AND band-hover (its subheading is hovered → all rows
     highlight together) share one highlight. */
  .row-btn:hover,
  .row-btn.band-hover {
    background: var(--accent-bg);
  }
  .row-btn.selected {
    background: var(--accent-bg);
    border-left-color: var(--accent);
  }
  .row-btn.dimmed {
    opacity: 0.45;
  }
  .row-btn.dimmed:hover {
    opacity: 0.7;
  }

  /* The shared checkbox visual — every box is now a real native <input> (the row's
     keyboard control AND the select-all), styled identically: same size / border /
     radius. OS chrome is stripped so the shared box + pseudo-element show through. The
     check itself is a single CENTERED pseudo-element (a rotated stub with a right +
     bottom border), never the old crossing-gradient X. */
  .cbox {
    position: relative;
    flex: 0 0 auto;
    width: 1rem;
    height: 1rem;
    margin: 0;
    border: 1px solid var(--border);
    border-radius: 3px;
    background: var(--surface);
    appearance: none;
    -webkit-appearance: none;
    cursor: pointer;
  }
  .cbox:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  /* FULL selection → the accent fill + the centered check. Indeterminate is NOT here:
     a partial box keeps the default surface bg + border, with only a visible dash. */
  input.cbox:checked {
    border-color: var(--accent);
    background: var(--accent);
  }
  /* The CENTERED checkmark: a short rotated stub (border-right + border-bottom)
     positioned at the box centre and nudged so the corner sits centred. Drawn only on
     a :checked input (full selection). */
  input.cbox:checked::after {
    content: "";
    position: absolute;
    left: 50%;
    top: 48%;
    width: 0.25rem;
    height: 0.5rem;
    border: solid var(--accent-fg);
    border-width: 0 2px 2px 0;
    transform: translate(-50%, -55%) rotate(45deg);
  }
  /* The indeterminate (partial-selection) visual: NO accent fill — the box keeps its
     surface bg + border — with a clearly visible centred --accent dash drawn ON that
     unfilled box. :indeterminate beats :checked so a partial box never draws a check. */
  input.cbox:indeterminate::after {
    content: "";
    position: absolute;
    left: 50%;
    top: 50%;
    width: 0.55rem;
    height: 2px;
    border: none;
    background: var(--accent);
    transform: translate(-50%, -50%);
  }

  .row-main {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  .primary-line {
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 0.4rem 0.6rem;
  }
  /* A single-column row leads with the variable identity (prominent); a nested
     column row leads with its adaptive varying dimension (regular). */
  .col-row.single .primary {
    font-weight: 600;
  }
  .primary {
    font-size: 0.9rem;
    overflow-wrap: anywhere;
  }
  .primary.mono {
    font-family: var(--font-mono);
  }
  /* The DELIVERY COLUMN chip (#678): the main selection signal, a small categorical
     pill in the --cat-var hue (the "variable"/column dimension tint), tuned like the
     Tag primitive — 10% fill + 35% border + the AA-cleared --cat-var-ink text. A
     DISTINCT, recognizable "column" mark, deliberately NOT the rost --accent/-bg
     (which mean "selected"). Light/dark-safe via color-mix. */
  .col-chip {
    font-family: var(--font-mono);
    font-size: 0.82rem;
    font-weight: 600;
    line-height: 1.3;
    padding: 0.05rem 0.4rem;
    border-radius: var(--radius-sm);
    color: var(--cat-var-ink);
    border: 1px solid color-mix(in srgb, var(--cat-var) 35%, transparent);
    background: color-mix(in srgb, var(--cat-var) 10%, var(--surface));
    overflow-wrap: anywhere;
    /* Hug the column text — never stretch to fill the row's flex column. */
    align-self: flex-start;
    width: fit-content;
    max-width: 100%;
  }
  /* The navigable column chip (single-column identity in the group view): a real <a>
     to the variable's leaf. Reads as the column chip, gaining a stronger border +
     underline on hover/focus so it's discoverable as a link, distinct from selection. */
  a.col-chip.link {
    text-decoration: none;
    cursor: pointer;
  }
  /* The link-out affordance: a ↗ inside the chip pill (the whole chip is the link).
     Sized up from the mono text (the glyph reads small at text size) and kept a touch
     lighter so the column name still leads. */
  .col-chip.link .chip-arrow {
    margin-left: 0.2rem;
    font-size: 1.15em;
    line-height: 1;
    opacity: 0.85;
  }
  a.col-chip.link:hover,
  a.col-chip.link:focus-visible {
    /* Color change on hover — deepen the chip's own hue, NOT an underline —
       matching the variable-name link's color-shift hover affordance. */
    border-color: color-mix(in srgb, var(--cat-var) 55%, transparent);
    background: color-mix(in srgb, var(--cat-var) 22%, var(--surface));
  }
  a.col-chip.link:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  .var-name {
    font-size: 0.85rem;
    color: var(--text-muted);
  }
  .register-prefix {
    font-family: var(--font-mono);
    font-size: 0.8rem;
    color: var(--text-muted);
  }
  .badge {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 0.1rem 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--text-muted);
  }
  .badge.sensitive {
    border-color: var(--accent);
    color: var(--accent);
  }
  .sub {
    font-size: 0.8rem;
    color: var(--text-muted);
    overflow-wrap: anywhere;
  }
  .period {
    flex: 0 0 auto;
    font-size: 0.8rem;
    color: var(--text-muted);
    text-align: right;
    white-space: nowrap;
    /* Tabular numerals so every same-format year string is identical width and the
       right-aligned year column lines up cleanly (#678 — the proportional font's
       digits otherwise differ in width). */
    font-variant-numeric: tabular-nums;
  }
  /* A quiet nudge for a column whose CODING changed over time (#678): a tiny muted
     pill after the period, pointing the eye to the value-set / States detail. A hint,
     not a control — token-styled, must not dominate the row. */
  .codings-vary {
    flex: 0 0 auto;
    font-size: 0.7rem;
    letter-spacing: 0.02em;
    padding: 0.05rem 0.4rem;
    border: 1px solid var(--border);
    border-radius: 999px;
    color: var(--text-muted);
    white-space: nowrap;
  }
  /* The data-starts-late warning marker (#678): a quiet --warn glyph just before the
     period. Sized so it sits in the row gap and never shifts the right-aligned period
     column. May appear on many rows, so it stays small + low-key (no fill). */
  .late-warn {
    flex: 0 0 auto;
    font-size: 0.75rem;
    line-height: 1;
    color: var(--warn);
    cursor: help;
  }

  /* ONE footer spanning the whole list: the selected count + the single Add. */
  .picker-footer {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
    padding: 0.5rem 0.75rem;
    border-top: 1px solid var(--border);
  }
  .count {
    font-size: 0.85rem;
    color: var(--text-muted);
  }
  .add-to-project {
    font: inherit;
    font-size: 0.9rem;
    padding: 0.35rem 0.9rem;
    border: 1px solid var(--accent);
    border-radius: 4px;
    background: var(--accent-bg);
    color: var(--accent-ink);
    cursor: pointer;
  }
  .add-to-project:hover:enabled {
    background: var(--surface);
  }
  .add-to-project:disabled {
    opacity: 0.5;
    cursor: default;
  }
</style>
