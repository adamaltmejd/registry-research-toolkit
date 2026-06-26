<script lang="ts" generics="Row extends Record<string, unknown>">
import type { Snippet } from "svelte";
import type { Column } from "./types";

// The workhorse table (#804 / DESIGN.md → DataTable): uppercase micro-label
// headers, right-aligned mono numerics, zebra-free hairline rows, hover state.
//
// Column-definition API: `columns` describes each column (key/label/align/mono/
// numeric/width); `rows` are the row objects. A `numeric` column right-aligns +
// mono-faces a measure; `mono` mono-faces an identifier; `align` overrides. The
// `cell` snippet is the escape hatch for custom cell content — default renders
// `row[column.key]`.
//
// OPTIONAL selection: pass `getRowId` + `selectedId` (+ `onselect`) and rows
// become keyboard-focusable button-rows (tabindex, Enter/Space activate),
// carrying `aria-selected` + the selected style + the focus ring. Omit them and
// it's a plain static table (no row tabindex, no button semantics). List
// keyboard NAV is owned by Bits UI `Command` elsewhere — this is selectable
// rows + visual states only, NOT a roving-tabindex grid.

interface Props {
  columns: Column<Row>[];
  rows: Row[];
  /** Custom cell renderer; default renders `row[column.key]`. */
  cell?: Snippet<[Row, Column<Row>]>;
  /** Stable row id — enables selection when paired with `selectedId`. */
  getRowId?: (row: Row) => string;
  selectedId?: string;
  onselect?: (row: Row) => void;
}

let { columns, rows, cell, getRowId, selectedId, onselect }: Props = $props();

const selectable = $derived(getRowId !== undefined && onselect !== undefined);

function alignOf(col: Column<Row>): "start" | "end" {
  return col.align ?? (col.numeric ? "end" : "start");
}

function onkeydown(event: KeyboardEvent, row: Row): void {
  // Enter / Space activate the row (the row is a button via role=button); other
  // keys fall through (no roving tabindex — see the component note).
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    onselect?.(row);
  }
}
</script>

<table class="data-table">
  <thead>
    <tr>
      {#each columns as col (col.key)}
        <th
          scope="col"
          class="align-{alignOf(col)}"
          style={col.width ? `width: ${col.width}` : undefined}
        >
          {col.label}
        </th>
      {/each}
    </tr>
  </thead>
  <tbody>
    {#each rows as row, i (getRowId ? getRowId(row) : i)}
      {@const id = getRowId?.(row)}
      {@const isSelected = selectable && id === selectedId}
      <tr
        class:selectable
        class:selected={isSelected}
        role={selectable ? "button" : undefined}
        tabindex={selectable ? 0 : undefined}
        aria-selected={selectable ? isSelected : undefined}
        onclick={selectable ? () => onselect?.(row) : undefined}
        onkeydown={selectable ? (e) => onkeydown(e, row) : undefined}
      >
        {#each columns as col (col.key)}
          <td class="align-{alignOf(col)}" class:mono={col.mono || col.numeric}>
            {#if cell}{@render cell(row, col)}{:else}{row[col.key]}{/if}
          </td>
        {/each}
      </tr>
    {/each}
  </tbody>
</table>

<style>
  .data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-sm);
  }
  th {
    text-align: left;
    font-size: var(--micro-label-size);
    letter-spacing: var(--micro-label-tracking);
    text-transform: uppercase;
    font-weight: 600;
    color: var(--text-muted);
    padding: var(--space-2) var(--space-3);
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  td {
    padding: var(--space-2) var(--space-3);
    border-bottom: 1px solid var(--border);
    color: var(--text);
    vertical-align: baseline;
  }
  .align-end {
    text-align: right;
  }
  .align-start {
    text-align: left;
  }
  td.mono {
    font-family: var(--font-mono);
  }
  tbody tr.selectable {
    cursor: pointer;
  }
  tbody tr:hover {
    background: var(--surface-hover);
  }
  tbody tr.selected {
    background: var(--surface-selected);
  }
  tbody tr.selectable:focus-visible {
    outline: none;
    box-shadow: var(--focus-ring);
  }
</style>
