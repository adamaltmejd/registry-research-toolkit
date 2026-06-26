import type { Component, Snippet } from "svelte";
import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import DataTable from "./DataTable.svelte";
import DataTableEmptyCellHarness from "./DataTableEmptyCellHarness.svelte";
import DataTableInterfaceRowHarness from "./DataTableInterfaceRowHarness.svelte";
import type { Column } from "./types";

// DataTable: the load-bearing hooks are (1) micro-label scope="col" headers,
// (2) right-aligned + mono numeric/mono cells, (3) the custom-cell escape hatch,
// (4) OPTIONAL selection — ARIA grid semantics (role=grid) + keyboard-focusable
// selectable rows with aria-selected + the selected style — vs a plain static
// table (role=table, no row tabindex) when selection props are absent,
// (5) responsive stacking hooks — explicit ARIA roles (kept across the CSS
// display change) + per-cell `data-label` + the `.first` primary-column marker.

// Row fixtures are typed concretely. vitest-browser-svelte's `render(Component,
// props)` can't infer the `Row` generic from the props (unlike `<DataTable .. />`
// in a .svelte file, where Svelte infers it) — `render` fixes `Row` to the
// component's DEFAULT instantiation (`object`, whose `keyof & string` is `never`),
// so typed props can't satisfy it. `renderTable` localizes the one unavoidable
// component cast to instantiate `Row` per call; the `props` argument stays fully
// typed against the concrete row. The `<DataTable .. />`-shape callsites the
// downstream children use ARE inferred + type-checked — the proof is
// DataTableInterfaceRowHarness.svelte, a real interface-row callsite (Fix 1).
type Row = { code: string; label: string; count?: number };

interface TableProps<R extends object> {
  columns: Column<R>[];
  rows: R[];
  cell?: Snippet<[R, Column<R>]>;
  getRowId?: (row: R) => string;
  selectedId?: string;
  onselect?: (row: R) => void;
}

function renderTable<R extends object>(props: TableProps<R>) {
  return render(DataTable as unknown as Component<TableProps<R>>, props);
}

const columns: Column<Row>[] = [
  { key: "code", label: "Code", mono: true },
  { key: "label", label: "Label" },
  { key: "count", label: "Count", numeric: true },
];

const rows: Row[] = [
  { code: "1", label: "Man", count: 120 },
  { code: "2", label: "Woman", count: 98 },
];

describe("DataTable", () => {
  it("renders micro-label column headers with scope", async () => {
    const { container } = renderTable({ columns, rows });
    const headers = container.querySelectorAll("thead th");
    expect(headers).toHaveLength(3);
    for (const th of headers) {
      expect(th).toHaveAttribute("scope", "col");
    }
    await expect
      .element(page.getByRole("columnheader", { name: "Code" }))
      .toBeVisible();
  });

  it("right-aligns + mono-faces a numeric column", async () => {
    const { container } = renderTable({ columns, rows });
    // The numeric "Count" cell (3rd col) of the first data row.
    const firstRowCells = container.querySelectorAll("tbody tr:first-child td");
    const countCell = firstRowCells[2];
    expect(countCell).toHaveClass("align-end");
    expect(countCell).toHaveClass("mono");
  });

  it("renders cell values by column key by default", async () => {
    await renderTable({ columns, rows });
    // exact: "Man" is a substring of "Woman".
    await expect.element(page.getByText("Man", { exact: true })).toBeVisible();
    await expect.element(page.getByText("120")).toBeVisible();
  });

  it("uses the custom cell snippet when provided", async () => {
    const cell = createRawSnippet(() => ({
      render: () => "<span>custom</span>",
    }));
    const { container } = renderTable({ columns, rows, cell });
    // Every cell routes through the snippet → no raw value text.
    expect(container.querySelectorAll("tbody td").length).toBeGreaterThan(0);
    await expect.element(page.getByText("custom").first()).toBeVisible();
  });

  it("renders a plain static table with explicit (non-grid) ARIA roles", async () => {
    // Explicit roles are set UNCONDITIONALLY so the stacked responsive form (a CSS
    // `display:block` change that strips native table roles in Firefox/Safari)
    // keeps valid table semantics. Without selection props the table is a plain
    // `role="table"` (not grid); rows are `role="row"`; cells are `role="cell"`
    // (not gridcell); and rows are NOT a tab stop / not aria-selected.
    const { container } = renderTable({ columns, rows });
    const table = container.querySelector("table");
    expect(table).toHaveAttribute("role", "table");
    const tr = container.querySelector("tbody tr");
    expect(tr).toHaveAttribute("role", "row");
    expect(tr).not.toHaveAttribute("tabindex");
    expect(tr).not.toHaveAttribute("aria-selected");
    const td = container.querySelector("tbody td");
    expect(td).toHaveAttribute("role", "cell");
    // thead/tbody are rowgroups; the header row + cells carry their roles too.
    expect(container.querySelector("thead")).toHaveAttribute(
      "role",
      "rowgroup",
    );
    expect(container.querySelector("tbody")).toHaveAttribute(
      "role",
      "rowgroup",
    );
    expect(container.querySelector("thead th")).toHaveAttribute(
      "role",
      "columnheader",
    );
  });

  it("labels each cell with its column for the stacked-card prefix", async () => {
    // The responsive stacked form renders each non-primary cell's column label as
    // a `::before` prefix sourced from `data-label`; the primary (first) column is
    // the card title, marked `.first` with no prefix. Assert the DOM hooks the CSS
    // relies on (the `@media` rendering itself is environment-CSS, not asserted).
    const { container } = renderTable({ columns, rows });
    const firstRowCells = container.querySelectorAll("tbody tr:first-child td");
    expect(firstRowCells[0]).toHaveAttribute("data-label", "Code");
    expect(firstRowCells[0]).toHaveClass("first");
    expect(firstRowCells[1]).toHaveAttribute("data-label", "Label");
    expect(firstRowCells[1]).not.toHaveClass("first");
    expect(firstRowCells[2]).toHaveAttribute("data-label", "Count");
    // The first header cell is also marked primary (drives the wide-screen
    // min-width floor).
    expect(container.querySelector("thead th")).toHaveClass("first");
  });

  it("renders an explicit Column.width as an inline style on the header cell", async () => {
    // The wide-screen 12rem min-width floor (`th.first:not([style*="width"])`)
    // backs off ONLY when the consumer pins an explicit width — which the CSS
    // detects via the inline `style="width: …"` attribute. So the override hinges
    // on presence (width set) vs absence (no width) of that attribute; lock both.
    const widthColumns: Column<Row>[] = [
      { key: "code", label: "Code", mono: true, width: "8rem" },
      { key: "label", label: "Label" },
      { key: "count", label: "Count", numeric: true },
    ];
    const withWidth = renderTable({ columns: widthColumns, rows });
    expect(withWidth.container.querySelector("thead th")).toHaveAttribute(
      "style",
      expect.stringContaining("width: 8rem"),
    );
    // The standard fixture pins no width, so the floor stays in force. The CSS
    // guard keys off `width` appearing in the style attribute, not the attribute
    // being absent — so assert the style carries no `width`, not that it's empty.
    const noWidth = renderTable({ columns, rows });
    const noWidthStyle = noWidth.container
      .querySelector("thead th")
      ?.getAttribute("style");
    expect(noWidthStyle == null || !noWidthStyle.includes("width")).toBe(true);
  });

  it("makes rows selectable grid-rows when selection props are passed", async () => {
    let selected = "";
    const { container } = renderTable({
      columns,
      rows,
      getRowId: (r: Row) => String(r.code),
      selectedId: "1",
      onselect: (r: Row) => {
        selected = String(r.code);
      },
    });
    // The table adopts ARIA grid semantics so the rows can carry aria-selected.
    expect(container.querySelector("table")).toHaveAttribute("role", "grid");
    const trs = container.querySelectorAll("tbody tr");
    // Selected row carries the class + aria-selected; rows are tabbable (each its
    // own tab stop) with NO button role overriding the implicit grid row role.
    expect(trs[0]).toHaveClass("selected");
    expect(trs[0]).toHaveAttribute("aria-selected", "true");
    expect(trs[0]).not.toHaveAttribute("role", "button");
    expect(trs[0]).toHaveAttribute("tabindex", "0");
    expect(trs[1]).toHaveAttribute("aria-selected", "false");
    // Cells are gridcells under selection (vs `cell` in the plain table above).
    expect(trs[0].querySelector("td")).toHaveAttribute("role", "gridcell");

    // Click activates onselect.
    (trs[1] as HTMLElement).click();
    expect(selected).toBe("2");
  });

  it("leaves a snippet-empty cell matching :empty so the stacked label is suppressed (#832)", async () => {
    // The stacked-card form prefixes each non-primary cell with its column
    // micro-label via `td:not(.first)::before { content: attr(data-label) }`. A
    // register with no `purpose` makes the consumer's `cell` snippet render
    // NOTHING into the Description cell, which would otherwise show a dangling
    // "DESCRIPTION" label over empty space. The fix suppresses it with
    // `td:empty::before { content: none }` — which only works if Svelte's {#if}
    // anchor comments inside the empty <td> don't defeat CSS `:empty`. This
    // renders the real CatalogNodeView snippet shape through the Svelte compiler
    // and asserts the empty cell genuinely matches `:empty` (the crux), while a
    // populated cell does not.
    const { container } = render(DataTableEmptyCellHarness, {});
    const noPurposeRow = container.querySelectorAll(
      "tbody tr",
    )[1] as HTMLElement;
    const descCell = noPurposeRow.querySelectorAll("td")[1] as HTMLElement;
    // The non-primary Description cell carries the data-label that drives the
    // ::before prefix — confirm we're testing the right cell.
    expect(descCell).toHaveAttribute("data-label", "Description");
    expect(descCell).not.toHaveClass("first");
    // Crux: Svelte's {#if} anchor comments do NOT defeat `:empty`, so the empty
    // cell matches and `td:empty::before { content: none }` fires.
    expect(descCell.matches(":empty")).toBe(true);
    // A populated Description cell must NOT match :empty (label still shows).
    const withPurposeRow = container.querySelectorAll(
      "tbody tr",
    )[0] as HTMLElement;
    const populatedCell = withPurposeRow.querySelectorAll(
      "td",
    )[1] as HTMLElement;
    expect(populatedCell.matches(":empty")).toBe(false);
  });

  it("compiles + renders interface-typed rows (Fix 1: Row extends object)", async () => {
    // The harness is a real `<DataTable .. />` callsite whose `Row` is a named
    // `interface` (no implicit string index signature). svelte-check enforces the
    // component's `Row extends object` constraint on that callsite — which an
    // interface satisfies but `Row extends Record<string, unknown>` did NOT, so
    // this whole suite would fail `bun run check` if Fix 1 regressed. (renderTable
    // can't carry that proof: it casts the component, bypassing the constraint.)
    render(DataTableInterfaceRowHarness, {});
    await expect.element(page.getByText("Stockholm")).toBeVisible();
    await expect.element(page.getByText("01")).toBeVisible();
  });

  it("does not hijack selection from an interactive cell control (Fix 2)", async () => {
    // A `cell` snippet that renders a <button>: a click on the button must NOT
    // bubble into the row's onselect — the nested control owns its own activation.
    const cell = createRawSnippet<[Row, Column<Row>]>((_getRow, getCol) => ({
      render: () => {
        const col = getCol();
        // Render a real <button> in the label column, plain text elsewhere.
        return col.key === "label"
          ? '<button type="button">open</button>'
          : "<span>plain</span>";
      },
    }));
    let selected = "";
    const { container } = renderTable({
      columns,
      rows,
      cell,
      getRowId: (r: Row) => String(r.code),
      selectedId: "1",
      onselect: (r: Row) => {
        selected = String(r.code);
      },
    });
    const firstRow = container.querySelector("tbody tr") as HTMLElement;
    // Click the nested button → no row selection.
    const btn = firstRow.querySelector("button") as HTMLButtonElement;
    btn.click();
    expect(selected).toBe("");
    // Click a plain (non-interactive) cell → row selection still fires.
    const plainCell = firstRow.querySelector("td:last-child") as HTMLElement;
    plainCell.click();
    expect(selected).toBe("1");
  });
});
