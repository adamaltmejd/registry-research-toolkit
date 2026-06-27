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
// style + the focus ring. Omit them and it's a plain static table (role=table, no
// row tabindex). List keyboard NAV is owned by Bits UI `Command` elsewhere —
// this is selectable rows + visual states only, NOT a roving-tabindex grid.
//
// RESPONSIVE: at <=48rem the table stacks (each <tr> becomes a card, cells stack
// with their column micro-label as a `::before` prefix). Because CSS `display`
// changes strip native table roles in Firefox/Safari, the ARIA roles are set
// EXPLICITLY and unconditionally (table/grid, rowgroup, row, columnheader/cell/
// gridcell) so the stacked form keeps valid table semantics. The first column is
// the primary title (no micro-label prefix); the `data-label` on each <td> feeds
// the decorative prefix for the rest.

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

<!-- Selection uses ARIA grid semantics (role=grid on the table) so each row is a
     valid selectable `aria-selected` row. Roles are set EXPLICITLY on every
     element (not left to native HTML-AAM remapping) because the responsive stacked
     form changes `display` to block, which strips native table roles in Firefox/
     Safari — explicit roles keep the table/grid semantics across that change.
     Every selectable row is its OWN tab stop (tabindex=0), NOT a single-tab-stop
     roving-tabindex grid: list keyboard NAV is owned by Bits UI `Command`
     elsewhere — this primitive provides selectable rows + visual states only. -->
<table class="data-table" role={selectable ? "grid" : "table"}>
  <!-- svelte-ignore a11y_no_redundant_roles -->
  <thead role="rowgroup">
    <!-- svelte-ignore a11y_no_redundant_roles -->
    <!-- These roles ARE redundant on a native table — deliberately so: the
         responsive stack switches `display` to block, stripping native table
         roles in Firefox/Safari, so every role is restated explicitly to keep
         the semantics across that change. -->
    <tr role="row">
      {#each columns as col, i (col.key)}
        <th
          scope="col"
          role="columnheader"
          class="micro-label align-{alignOf(col)}"
          class:first={i === 0}
          style={col.width ? `width: ${col.width}` : undefined}
        >
          {col.label}
        </th>
      {/each}
    </tr>
  </thead>
  <!-- svelte-ignore a11y_no_redundant_roles -->
  <tbody role="rowgroup">
    {#each rows as row, i (getRowId ? getRowId(row) : i)}
      {@const id = getRowId?.(row)}
      {@const isSelected = selectable && id === selectedId}
      <!-- svelte-ignore a11y_no_redundant_roles -->
      <tr
        role="row"
        class:selectable
        class:selected={isSelected}
        tabindex={selectable ? 0 : undefined}
        aria-selected={selectable ? isSelected : undefined}
        onclick={selectable ? (e) => onrowclick(e, row) : undefined}
        onkeydown={selectable ? (e) => onkeydown(e, row) : undefined}
      >
        {#each columns as col, colIndex (col.key)}
          <!-- `data-label` feeds the stacked-card micro-label prefix (<=48rem);
               `.first` marks the primary title cell (no prefix). The prefix is
               decorative — screen readers still get the column name from the
               (visually-hidden but a11y-tree-present) <th role="columnheader">. -->
          <td
            role={selectable ? "gridcell" : "cell"}
            class="align-{alignOf(col)}"
            class:first={colIndex === 0}
            class:mono={col.mono || col.numeric}
            data-label={col.label}
          >
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
    padding: var(--space-2) var(--space-3);
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  td {
    padding: var(--space-2) var(--space-3);
    border-bottom: 1px solid var(--border);
    color: var(--text);
    vertical-align: baseline;
    /* Graceful break for long text cells: `overflow-wrap` is the GUARANTEED break
       (and inherits into the cell's link/span), so a long Swedish compound name
       can't force a min-content width past a narrow canvas. `hyphens:auto` is a
       progressive enhancement — it only hyphenates when a hyphenation language is
       in scope; the document is lang="en" while the content is Swedish, so it's
       mostly inert today, but harmless. Excluded for mono/numeric cells below
       (codes/measures must not break or hyphenate). */
    overflow-wrap: anywhere;
    hyphens: auto;
  }
  /* Give the primary (first) column a readable floor so the name column isn't
     starved to min-content by a long Description. Applied on the <th> only: under
     table-layout:auto the header cell sizes the whole column track, so this floors
     the column without touching the body cells. An explicit `Column.width`
     (rendered as an inline `style="width: …"` on the <th>) wins via the
     attribute-absence guard, keeping the per-consumer override intact. */
  th.first:not([style*="width"]) {
    min-width: 12rem;
  }
  .align-end {
    text-align: right;
  }
  .align-start {
    text-align: left;
  }
  td.mono {
    font-family: var(--font-mono);
    /* Codes/FQIDs/measures are atomic — never break or hyphenate them. */
    overflow-wrap: normal;
    hyphens: none;
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

  /* Stacked cards on narrow canvases (#832). The same 48rem breakpoint the rest
     of the SPA uses (AppShell/SearchView). Native table layout collapses into a
     vertical stack: each <tr> is a bordered card, each <td> stacks with its
     column micro-label as a decorative `::before` prefix. The explicit ARIA roles
     in the markup keep table semantics intact across this `display` change (which
     would otherwise strip native roles in Firefox/Safari). */
  @media (max-width: 48rem) {
    .data-table,
    .data-table thead,
    .data-table tbody,
    .data-table tr,
    .data-table th,
    .data-table td {
      display: block;
    }
    /* Keep the headers in the a11y tree (columnheader semantics survive for
       screen readers) but visually hidden — NOT display:none, which would drop
       the roles. This is the MEDIA-CONDITIONAL sibling of the `.visually-hidden`
       utility in lib/ui/utilities.css: it can't use that class because the hiding
       is media-query-scoped (the thead is a VISIBLE header at desktop widths, an
       unconditional markup class can't express "hidden only when narrow"), so the
       recipe is kept inline — held textually IDENTICAL to `.visually-hidden` so the
       two can't drift (the extra props are harmless on a clipped, absolutely-
       positioned thead). Keep it in sync with `.visually-hidden`. (Mirrors the
       `td::before` micro-label exception below — same can't-take-a-class pattern.) */
    thead {
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip-path: inset(50%);
      white-space: nowrap;
      border: 0;
    }
    /* Each row becomes a card: the card border replaces the per-cell rules. */
    tbody tr {
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      padding: var(--space-2) var(--space-3);
    }
    tbody tr + tr {
      margin-top: var(--space-2);
    }
    td {
      padding: var(--space-1) 0;
      border-bottom: none;
      /* Alignment is meaningless once stacked — measures read left like the rest.
         `.align-end` (numeric/end columns) is reset explicitly because its
         non-media rule out-specifies a bare `td`, so the general rule alone
         can't override it. */
      text-align: left;
    }
    td.align-end {
      text-align: left;
    }
    /* The primary cell stays the prominent title (its link/weight is unchanged);
       it carries no micro-label prefix. */
    td.first {
      min-width: 0;
    }
    /* Non-primary cells show their column micro-label, styled like the <th> the
       card hides. Decorative (aria-hidden via being CSS-generated content): the
       columnheader still reaches screen readers from the visually-hidden thead.
       The eyebrow props are duplicated from the `.micro-label` utility (#836)
       rather than shared: a CSS-generated `::before` pseudo-element can't take a
       class, and plain CSS has no mixin — so this is the one eyebrow that keeps
       its own copy. Keep it in sync with `.micro-label` in lib/ui/utilities.css. */
    td:not(.first)::before {
      content: attr(data-label);
      display: block;
      font-size: var(--micro-label-size);
      letter-spacing: var(--micro-label-tracking);
      text-transform: uppercase;
      font-weight: 600;
      color: var(--text-muted);
    }
    /* An empty cell (e.g. a register with no Description/purpose — the consumer's
       `cell` snippet renders nothing) must not show a dangling micro-label. CSS
       `:empty` ignores comment nodes, so Svelte's {#if} anchor comments inside an
       otherwise-empty <td> don't defeat the match. */
    td:empty::before {
      content: none;
    }
    /* The card keeps ONE focus/hover/selected target (the selectable <tr>). */
    tbody tr.selectable:focus-visible {
      outline: none;
      box-shadow: var(--focus-ring);
    }
  }
</style>
