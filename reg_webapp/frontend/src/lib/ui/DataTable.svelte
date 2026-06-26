<script lang="ts" generics="Row extends object">
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
// OPTIONAL selection: pass `getRowId` + `selectedId` (+ `onselect`) and the
// table adopts ARIA grid semantics (role=grid) with keyboard-focusable selectable
// rows (tabindex, Enter/Space activate), carrying `aria-selected` + the selected
// style + the focus ring. Omit them and it's a plain static table (no grid role,
// no row tabindex). List keyboard NAV is owned by Bits UI `Command` elsewhere —
// this is selectable rows + visual states only, NOT a roving-tabindex grid.

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

// A `cell` snippet can render its own link/button. A click (or Enter/Space) on
// that nested control bubbles to the row, so bail when the event originated from
// an interactive descendant rather than the row itself — otherwise selecting the
// row would hijack the control's own activation.
const INTERACTIVE =
  'a[href], button, input, select, textarea, label, [role="button"], [tabindex]';

function fromInteractiveChild(
  event: Event,
  rowEl: EventTarget | null,
): boolean {
  const target = event.target;
  if (!(target instanceof Element)) return false;
  const hit = target.closest(INTERACTIVE);
  // The row itself is a tabindex element; only a DESCENDANT control should bail.
  return hit !== null && hit !== rowEl;
}

function onrowclick(event: MouseEvent, row: Row): void {
  if (fromInteractiveChild(event, event.currentTarget)) return;
  onselect?.(row);
}

function onkeydown(event: KeyboardEvent, row: Row): void {
  // Enter / Space activate the selected-row; other keys fall through (no roving
  // tabindex — see the component note).
  if (event.key !== "Enter" && event.key !== " ") return;
  // Don't hijack a nested control's own keyboard activation (e.g. focus on a cell
  // <button>); only activate when the row element itself is the event source.
  if (fromInteractiveChild(event, event.currentTarget)) return;
  event.preventDefault();
  onselect?.(row);
}
</script>

<!-- simplify: selection uses ARIA grid semantics (role=grid on the table; native
     <th>/<td> remap to columnheader/gridcell via HTML-AAM) so each row is a valid
     selectable `aria-selected` row. Every selectable row is its OWN tab stop
     (tabindex=0), NOT a single-tab-stop roving-tabindex grid: list keyboard NAV is
     owned by Bits UI `Command` elsewhere — this primitive provides selectable rows
     + visual states only. -->
<table class="data-table" role={selectable ? "grid" : undefined}>
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
        tabindex={selectable ? 0 : undefined}
        aria-selected={selectable ? isSelected : undefined}
        onclick={selectable ? (e) => onrowclick(e, row) : undefined}
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
