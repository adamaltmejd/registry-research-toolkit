<script lang="ts">
import {
  type BandIdentity,
  bandLabeling,
  leafSlug,
  type PickerRepresentation,
  pickerLabeling,
  representationInWindow,
} from "./catalog";

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
  isSensitive?: boolean;
  isIdentifier?: boolean;
  rows: PickerRepresentation[];
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

/** A variable's distinguishing technical differentiator: a single-column variable's
 * delivery column (so a column-led group reads `Ng0`/`Ng1`/`Sni`), else the member
 * leaf slug — the fallback for a multi-column variable. */
function distinguisherOf(band: PickerBand): string {
  if (band.rows.length === 1 && band.rows[0].column) {
    return band.rows[0].column;
  }
  return leafSlug(band.key);
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
      }),
    ),
  ),
);

/** Per-variable adaptive COLUMN labels (#678 1b) — show only what varies within the
 * variable, constants hoisted to a thin context line. Keyed by variable key. */
const labelingByBand = $derived(
  new Map(bands.map((b) => [b.key, pickerLabeling(b.rows)])),
);

/** The render model per variable: its leading identity, whether it is a single
 * column (→ one merged row, no subheading), the adaptive column labels, and the
 * hoisted row-context line (the column dropped when it's the single-column primary). */
const view = $derived(
  bands.map((band, i) => {
    const id = identity.bands[i];
    const labeling = labelingByBand.get(band.key);
    const single = band.rows.length === 1;
    // A single-column variable merges its identity into the one row: the row's
    // primary IS the variable identity. Drop the redundant "column …" context entry.
    const context =
      single && id.primaryIsColumn
        ? (labeling?.headerContext ?? []).filter(
            (c) => !c.startsWith("column "),
          )
        : (labeling?.headerContext ?? []);
    return {
      band,
      primary: id.primary,
      single,
      context,
      rowLabels: new Map((labeling?.rows ?? []).map((r) => [r.key, r])),
    };
  }),
);

const footerLabel = $derived(
  `${selectedCount} ${selectedCount === 1 ? "column" : "columns"} selected`,
);
</script>

<div class="rep-picker">
  {#if allKeys.length > 1}
    <!-- Global select-all: grab every column of the concept (for the active period)
         in one move. A header strip, not card chrome. -->
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
        <!-- A single-column variable = ONE selectable row, led by the variable's
             distinguishing identity (the leaf ≈ one-variable group case). -->
        <li class="col-row single">
          <button
            type="button"
            class="row-btn"
            role="checkbox"
            aria-checked={checked}
            class:selected={checked}
            class:dimmed={!inWindow}
            onclick={() => toggleRow(band.key, row.key)}
          >
            <span class="check cbox" aria-hidden="true"></span>
            <span class="row-main">
              <span class="primary-line">
                {#if v.primary.mono}
                  <code class="primary mono">{v.primary.text}</code>
                {:else}
                  <span class="primary">{v.primary.text}</span>
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
              {#if v.context.length > 0}
                <span class="sub">{v.context.join(" · ")}</span>
              {/if}
            </span>
            {#if row.period}
              <span class="period">{row.period}</span>
            {/if}
          </button>
        </li>
      {:else}
        <!-- A multi-column variable: a thin, quiet subheading (its distinguishing
             identity + a "select all" toggle) over its column rows. No card chrome —
             a hairline separates the group from the rest of the list. -->
        {@const empty = band.rows.length === 0}
        <li class="subhead" class:empty>
          <!-- The select-all toggle only exists when there ARE columns; a 0-column
               variable (absent from the graph union) renders a plain subheading. -->
          {#snippet subheadTitle()}
            <span class="subhead-title">
              {#if v.primary.mono}
                <code class="primary mono">{v.primary.text}</code>
              {:else}
                <span class="primary">{v.primary.text}</span>
              {/if}
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
            </span>
          {/snippet}
          {#if empty}
            {@render subheadTitle()}
          {:else}
            <label class="subhead-select">
              <input
                type="checkbox"
                class="cbox"
                checked={allOfBandSelected(band)}
                indeterminate={someOfBandSelected(band) &&
                  !allOfBandSelected(band)}
                aria-label={`Select all columns of ${v.primary.text}`}
                onchange={() => toggleBand(band)}
              />
              {@render subheadTitle()}
            </label>
          {/if}
          {#if v.context.length > 0}
            <span class="subhead-context">{v.context.join(" · ")}</span>
          {/if}
        </li>
        {#each band.rows as row (row.key)}
          {@const checked = selectedKeys.has(selKey(band.key, row.key))}
          {@const inWindow = representationInWindow(row, window)}
          {@const label = v.rowLabels.get(row.key)}
          <li class="col-row nested">
            <button
              type="button"
              class="row-btn"
              role="checkbox"
              aria-checked={checked}
              class:selected={checked}
              class:dimmed={!inWindow}
              onclick={() => toggleRow(band.key, row.key)}
            >
              <span class="check cbox" aria-hidden="true"></span>
              <span class="row-main">
                {#if label?.primary.mono}
                  <code class="primary mono">{label.primary.text}</code>
                {:else}
                  <span class="primary">{label?.primary.text}</span>
                {/if}
                {#if label && label.qualifiers.length > 0}
                  <span class="sub">{label.qualifiers.join(" · ")}</span>
                {/if}
              </span>
              <!-- Every row shows its own period on the right (the period is never in
                   the hoisted context now — #678 fix 5). Use the raw `row.period` so
                   a constant-period band still shows each row's span. -->
              {#if row.period}
                <span class="period">{row.period}</span>
              {/if}
            </button>
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
     so several stacked subheadings read flat and integrated. */
  .subhead {
    padding: 0.4rem 0.75rem 0.3rem;
  }
  .subhead-select {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
    cursor: pointer;
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
  .subhead-context {
    display: block;
    margin-top: 0.15rem;
    padding-left: 1.5rem;
    font-size: 0.78rem;
    color: var(--text-muted);
  }
  /* A 0-column variable: a plain subheading with a quiet "No columns" marker. */
  .empty-note {
    font-size: 0.8rem;
    font-style: italic;
    color: var(--text-muted);
  }

  /* A column row: a click-anywhere checkbox. The whole row toggles (real <button> +
     role=checkbox for keyboard/AT). Nested rows indent under their subheading. */
  .row-btn {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    width: 100%;
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
  .row-btn:hover {
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
  .row-btn:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }

  /* The shared checkbox visual — one box used by BOTH the row check (a <span>,
     driven by `.row-btn.selected`) and the select-all native <input> (driven by its
     :checked / :indeterminate). Same size / border / radius / accent fill so they're
     visually identical. The check itself is a single CENTERED pseudo-element (a
     rotated stub with a right + bottom border), never the old crossing-gradient X. */
  .cbox {
    position: relative;
    flex: 0 0 auto;
    width: 1rem;
    height: 1rem;
    margin: 0;
    border: 1px solid var(--border);
    border-radius: 3px;
    background: var(--surface);
  }
  /* The native select-all input needs its OS chrome stripped so the shared box +
     pseudo-element show through. */
  input.cbox {
    appearance: none;
    -webkit-appearance: none;
    cursor: pointer;
  }
  /* Filled (accent) when checked — the row's `.selected` parent or the input's own
     :checked. */
  .row-btn.selected .check.cbox,
  input.cbox:checked,
  input.cbox:indeterminate {
    border-color: var(--accent);
    background: var(--accent);
  }
  /* The CENTERED checkmark: a short rotated stub (border-right + border-bottom)
     positioned at the box centre and nudged so the corner sits centred. Drawn for a
     selected row check and a :checked input. */
  .row-btn.selected .check.cbox::after,
  input.cbox:checked::after {
    content: "";
    position: absolute;
    left: 50%;
    top: 48%;
    width: 0.25rem;
    height: 0.5rem;
    border: solid var(--accent-ink);
    border-width: 0 2px 2px 0;
    transform: translate(-50%, -55%) rotate(45deg);
  }
  /* The indeterminate (partial-selection) visual: a centred dash, not a check. Only
     the native input carries an indeterminate state; :indeterminate beats :checked
     so a partial box never also draws the check. */
  input.cbox:indeterminate::after {
    content: "";
    position: absolute;
    left: 50%;
    top: 50%;
    width: 0.5rem;
    height: 2px;
    /* Reset the check rule's border (an input can't be both here given the bound
       props, but keep the dash unambiguous if it ever is). */
    border: none;
    background: var(--accent-ink);
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
