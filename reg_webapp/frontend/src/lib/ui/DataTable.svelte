<script lang="ts" generics="Row extends object">
import type { Snippet } from "svelte";
import type { Column } from "./types";

// The workhorse table (#804 / DESIGN.md → DataTable): label-level headers,
// right-aligned mono numerics, zebra-free hairline rows, hover state.
//
// Column-definition API: `columns` describes each column (key/label/align/mono/
// numeric/width); `rows` are the row objects. A `numeric` column right-aligns +
// mono-faces a measure; `mono` mono-faces an identifier; `align` overrides. The
// `cell` snippet is the escape hatch for custom cell content — default renders
// `row[column.key]`.
//
// Row navigation (optional): pass `rowNavigation` for link-list tables whose
// row/card surface should delegate a plain click to the row's primary `a[href]`.
// The anchor remains the only tab stop and accessible link; rows stay role=table
// rows with no tabindex. Omit it and it's a plain static table. `getRowId` keys
// the rows (stable identity across re-renders).
//
// RESPONSIVE: at <=48rem the default table stacks (each <tr> becomes a card,
// cells stack with their column micro-label as a `::before` prefix). The framed
// variant keeps a visible header row and renders flat grid rows instead, so the
// table surface does not contain card rows. Framed two-column tables collapse to
// one visual column: the first column is the primary heading and the second cell
// reads as secondary text underneath. Because CSS `display` changes strip native
// table roles in Firefox/Safari, the ARIA roles are set EXPLICITLY and
// unconditionally (table, rowgroup, row, columnheader/cell) so the
// narrow forms keep valid table semantics. The first column is the primary title
// (no micro-label prefix); the `data-label` on each <td> feeds the decorative
// prefix for the rest.

interface Props {
  columns: Column<Row>[];
  rows: Row[];
  /** Custom cell renderer; default renders `row[column.key]`. */
  cell?: Snippet<[Row, Column<Row>]>;
  /** Stable row id, used as the row key. */
  getRowId?: (row: Row) => string;
  /** Link-list variant: plain row/card clicks delegate to the row's primary anchor. */
  rowNavigation?: boolean;
  /** Give the table the raised panel surface; the column headers become the title row. */
  framed?: boolean;
}

let {
  columns,
  rows,
  cell,
  getRowId,
  rowNavigation = false,
  framed = false,
}: Props = $props();

type MouseStart = { row: EventTarget | null; x: number; y: number };
let mouseStart: MouseStart | null = null;

function alignOf(col: Column<Row>): "start" | "end" {
  return col.align ?? (col.numeric ? "end" : "start");
}

// A `cell` snippet can render its own link/button. A click on that nested
// control bubbles to the row, so bail when the event originated from an
// interactive descendant rather than the row itself — otherwise the row would
// hijack the control's own activation.
const INTERACTIVE =
  'a[href], button, input, select, textarea, label, [role="button"], [tabindex]';

function fromInteractiveChild(
  event: Event,
  rowEl: EventTarget | null,
): boolean {
  const target = event.target;
  if (!(target instanceof Element)) return false;
  const hit = target.closest(INTERACTIVE);
  // Only a DESCENDANT control should bail, never the row itself.
  return hit !== null && hit !== rowEl;
}

function rowSelectionText(rowEl: HTMLElement): boolean {
  const selection = window.getSelection();
  if (
    selection === null ||
    selection.isCollapsed ||
    selection.toString().trim() === ""
  ) {
    return false;
  }
  const { anchorNode, focusNode } = selection;
  return (
    (anchorNode !== null && rowEl.contains(anchorNode)) ||
    (focusNode !== null && rowEl.contains(focusNode))
  );
}

function movedSinceMouseDown(event: MouseEvent, rowEl: HTMLElement): boolean {
  if (mouseStart === null || mouseStart.row !== rowEl) return false;
  const dx = event.clientX - mouseStart.x;
  const dy = event.clientY - mouseStart.y;
  return Math.hypot(dx, dy) > 4;
}

function primaryRowLink(rowEl: HTMLElement): HTMLAnchorElement | null {
  const link = rowEl.querySelector("a[href]");
  return link instanceof HTMLAnchorElement ? link : null;
}

function onrowmousedown(event: MouseEvent): void {
  mouseStart =
    event.button === 0
      ? { row: event.currentTarget, x: event.clientX, y: event.clientY }
      : null;
}

function onrownavigationclick(event: MouseEvent): void {
  const rowEl = event.currentTarget;
  if (!(rowEl instanceof HTMLElement)) {
    mouseStart = null;
    return;
  }
  const dragged = movedSinceMouseDown(event, rowEl);
  mouseStart = null;
  if (
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey ||
    fromInteractiveChild(event, event.currentTarget)
  ) {
    return;
  }
  if (rowSelectionText(rowEl) || dragged) return;
  primaryRowLink(rowEl)?.click();
}
</script>

<!-- Roles are set EXPLICITLY on every element (not left to native HTML-AAM
     remapping) because the responsive stacked form changes `display` to block,
     which strips native table roles in Firefox/Safari — explicit roles keep the
     table semantics across that change. -->
<!-- Y-97: `.table-scroll`'s horizontal-overflow rationale is in its own CSS rule below. -->
<div class="table-scroll">
  <!-- svelte-ignore a11y_no_redundant_roles -->
  <table
    class="data-table"
    class:framed
    class:narrow-stack={framed && columns.length === 2}
    role="table"
    style={`--data-table-columns: ${columns.length}`}
  >
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
        <!-- svelte-ignore a11y_no_redundant_roles -->
        <tr
          role="row"
          class:navigable={rowNavigation}
          onmousedown={rowNavigation ? onrowmousedown : undefined}
          onclick={rowNavigation ? onrownavigationclick : undefined}
        >
          {#each columns as col, colIndex (col.key)}
            <!-- `data-label` feeds the stacked-card micro-label prefix (<=48rem);
                 `.first` marks the primary title cell (no prefix). The prefix is
                 decorative — screen readers still get the column name from the
                 (visually-hidden but a11y-tree-present) <th role="columnheader">. -->
            <td
              role="cell"
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
</div>

<style>
  /* Y-97: the horizontal-overflow scrollport lives here, on the wrapper of the
     element that is actually wide, rather than on a routed-region ancestor —
     which would make every route's scrollport horizontal-only and break
     `position: sticky` descendants elsewhere on the page (they'd pin to a box
     that never scrolls vertically instead of to the viewport). `max-inline-size:
     100%` keeps the wrapper itself from ever widening the page; the table inside
     it scrolls on its own. */
  .table-scroll {
    overflow-x: auto;
    max-inline-size: 100%;
  }
  .data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--text-sm);
  }
  .data-table.framed {
    border-collapse: separate;
    border-spacing: 0;
    background: var(--surface-raised);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--elevation-raised);
    overflow: hidden;
  }
  th {
    text-align: left;
    padding: var(--space-2) var(--space-3);
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  .framed th {
    padding: var(--space-3) var(--space-4);
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
  tbody tr.navigable {
    cursor: pointer;
  }
  tbody tr:hover {
    background: var(--surface-hover);
  }
  .framed tbody tr:last-child td {
    border-bottom: none;
  }
  tbody tr.navigable :global(a[href]:hover) {
    text-decoration: none;
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
    /* Non-primary cells show their column label, styled like the <th> the card
       hides. Decorative (aria-hidden via being CSS-generated content): the
       columnheader still reaches screen readers from the visually-hidden thead.
       The label props are duplicated from the `.micro-label` utility (#836)
       rather than shared: a CSS-generated `::before` pseudo-element can't take a
       class, and plain CSS has no mixin — so this is the one label that keeps its
       own copy. Keep it in sync with `.micro-label` in lib/ui/utilities.css. */
    td:not(.first)::before {
      content: attr(data-label);
      display: block;
      font-size: var(--micro-label-size);
      font-weight: var(--micro-label-weight);
      color: var(--text-muted);
    }
    /* An empty cell (e.g. a register with no Description/purpose — the consumer's
       `cell` snippet renders nothing) must not show a dangling micro-label. CSS
       `:empty` ignores comment nodes, so Svelte's {#if} anchor comments inside an
       otherwise-empty <td> don't defeat the match. */
    td:empty::before {
      content: none;
    }
    .framed thead {
      position: static;
      width: auto;
      height: auto;
      padding: 0;
      margin: 0;
      overflow: visible;
      clip-path: none;
      white-space: normal;
      border: 0;
    }
    .framed thead tr {
      display: grid;
      grid-template-columns: repeat(var(--data-table-columns), minmax(0, 1fr));
    }
    .framed thead th {
      display: block;
      padding: var(--space-3);
    }
    .framed tbody tr {
      display: grid;
      grid-template-columns: repeat(var(--data-table-columns), minmax(0, 1fr));
      border: 0;
      border-radius: 0;
      padding: 0;
    }
    .framed tbody tr + tr {
      margin-top: 0;
    }
    .framed td {
      display: block;
      padding: var(--space-3);
      border-bottom: 1px solid var(--border);
    }
    .framed tbody tr:last-child td {
      border-bottom: none;
    }
    .framed.narrow-stack thead tr,
    .framed.narrow-stack tbody tr {
      grid-template-columns: minmax(0, 1fr);
    }
    .framed.narrow-stack thead th:not(.first) {
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
    .framed.narrow-stack tbody tr {
      border-bottom: 1px solid var(--border);
    }
    .framed.narrow-stack tbody tr:last-child {
      border-bottom: 0;
    }
    .framed.narrow-stack td {
      border-bottom: 0;
    }
    .framed.narrow-stack td.first {
      padding-bottom: var(--space-1);
    }
    .framed.narrow-stack td:not(.first) {
      padding-top: 0;
      color: var(--text-muted);
    }
    .framed td:not(.first)::before {
      content: none;
    }
  }
</style>
