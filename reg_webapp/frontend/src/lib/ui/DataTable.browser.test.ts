import type { Component, Snippet } from "svelte";
import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import DataTable from "./DataTable.svelte";
import type { Column } from "./types";

// DataTable: the load-bearing behaviour is (1) scope="col" column headers,
// (2) explicit table ARIA roles kept across the responsive stack's CSS display
// change, (3) the row-navigation variant delegating a plain row click to the
// row's primary link, and (4) the narrow layouts and their overflow guard.

// Row fixtures are typed concretely. vitest-browser-svelte's `await render(Component,
// props)` can't infer the `Row` generic from the props (unlike `<DataTable .. />`
// in a .svelte file, where Svelte infers it) — `render` fixes `Row` to the
// component's DEFAULT instantiation (`object`, whose `keyof & string` is `never`),
// so typed props can't satisfy it. `renderTable` localizes the one unavoidable
// component cast to instantiate `Row` per call; the `props` argument stays fully
// typed against the concrete row. The `<DataTable .. />`-shape callsites the
// downstream children use ARE inferred + type-checked by svelte-check (e.g.
// VariantsSummary.svelte's interface-typed rows).
type Row = { code: string; label: string; count?: number };

interface TableProps<R extends object> {
  columns: Column<R>[];
  rows: R[];
  cell?: Snippet<[R, Column<R>]>;
  getRowId?: (row: R) => string;
  rowNavigation?: boolean;
  framed?: boolean;
}

async function renderTable<R extends object>(props: TableProps<R>) {
  return await render(DataTable as unknown as Component<TableProps<R>>, props);
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
    const { container } = await renderTable({ columns, rows });
    const headers = container.querySelectorAll("thead th");
    expect(headers).toHaveLength(3);
    for (const th of headers) {
      expect(th).toHaveAttribute("scope", "col");
    }
    await expect
      .element(page.getByRole("columnheader", { name: "Code" }))
      .toBeVisible();
  });

  it("renders a plain static table with explicit (non-grid) ARIA roles", async () => {
    // Explicit roles are set UNCONDITIONALLY so the stacked responsive form (a CSS
    // `display:block` change that strips native table roles in Firefox/Safari)
    // keeps valid table semantics: the table is `role="table"`, rows are
    // `role="row"`, cells are `role="cell"`, and rows are NOT a tab stop.
    const { container } = await renderTable({ columns, rows });
    const table = container.querySelector("table");
    expect(table).toHaveAttribute("role", "table");
    const tr = container.querySelector("tbody tr");
    expect(tr).toHaveAttribute("role", "row");
    expect(tr).not.toHaveAttribute("tabindex");
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

  it("delegates row-navigation clicks to the row's primary link without grid semantics", async () => {
    const cell = createRawSnippet<[Row, Column<Row>]>((_getRow, getCol) => ({
      render: () =>
        getCol().key === "label"
          ? '<a href="/catalog/scb/lisa">open LISA</a>'
          : "<span>plain</span>",
    }));
    const { container } = await renderTable({
      columns,
      rows,
      cell,
      rowNavigation: true,
    });
    const table = container.querySelector("table");
    const firstRow = container.querySelector("tbody tr") as HTMLElement;
    const firstCell = firstRow.querySelector("td:first-child") as HTMLElement;
    const link = firstRow.querySelector("a") as HTMLAnchorElement;
    let clicks = 0;
    link.addEventListener("click", (event) => {
      event.preventDefault();
      clicks += 1;
    });

    expect(table).toHaveAttribute("role", "table");
    expect(firstRow).not.toHaveAttribute("tabindex");
    expect(firstRow.querySelector("td")).toHaveAttribute("role", "cell");

    firstCell.click();
    expect(clicks).toBe(1);

    link.click();
    expect(clicks).toBe(2);
  });

  it("leaves modified row-navigation clicks to browser/link semantics", async () => {
    const cell = createRawSnippet<[Row, Column<Row>]>((_getRow, getCol) => ({
      render: () =>
        getCol().key === "label"
          ? '<a href="/catalog/scb/lisa">open LISA</a>'
          : "<span>plain</span>",
    }));
    const { container } = await renderTable({
      columns,
      rows,
      cell,
      rowNavigation: true,
    });
    const firstRow = container.querySelector("tbody tr") as HTMLElement;
    const firstCell = firstRow.querySelector("td:first-child") as HTMLElement;
    const link = firstRow.querySelector("a") as HTMLAnchorElement;
    let clicks = 0;
    link.addEventListener("click", (event) => {
      event.preventDefault();
      clicks += 1;
    });

    firstCell.dispatchEvent(
      new MouseEvent("click", { bubbles: true, button: 0, metaKey: true }),
    );
    firstCell.dispatchEvent(
      new MouseEvent("click", { bubbles: true, button: 1 }),
    );
    expect(clicks).toBe(0);
  });

  it("does not navigate at the tail of a row text selection", async () => {
    const cell = createRawSnippet<[Row, Column<Row>]>((_getRow, getCol) => ({
      render: () =>
        getCol().key === "label"
          ? '<a href="/catalog/scb/lisa">open LISA</a>'
          : "<span>selectable text</span>",
    }));
    const { container } = await renderTable({
      columns,
      rows,
      cell,
      rowNavigation: true,
    });
    const firstRow = container.querySelector("tbody tr") as HTMLElement;
    const firstCell = firstRow.querySelector("td:first-child") as HTMLElement;
    const link = firstRow.querySelector("a") as HTMLAnchorElement;
    let clicks = 0;
    link.addEventListener("click", (event) => {
      event.preventDefault();
      clicks += 1;
    });

    const range = document.createRange();
    range.selectNodeContents(firstCell);
    const selection = window.getSelection();
    selection?.removeAllRanges();
    selection?.addRange(range);
    try {
      firstCell.click();
      expect(clicks).toBe(0);
    } finally {
      selection?.removeAllRanges();
    }
  });

  it("does not navigate after a row-navigation drag", async () => {
    const cell = createRawSnippet<[Row, Column<Row>]>((_getRow, getCol) => ({
      render: () =>
        getCol().key === "label"
          ? '<a href="/catalog/scb/lisa">open LISA</a>'
          : "<span>plain</span>",
    }));
    const { container } = await renderTable({
      columns,
      rows,
      cell,
      rowNavigation: true,
    });
    const firstRow = container.querySelector("tbody tr") as HTMLElement;
    const firstCell = firstRow.querySelector("td:first-child") as HTMLElement;
    const link = firstRow.querySelector("a") as HTMLAnchorElement;
    let clicks = 0;
    link.addEventListener("click", (event) => {
      event.preventDefault();
      clicks += 1;
    });

    firstCell.dispatchEvent(
      new MouseEvent("mousedown", {
        bubbles: true,
        button: 0,
        clientX: 0,
        clientY: 0,
      }),
    );
    firstCell.dispatchEvent(
      new MouseEvent("click", {
        bubbles: true,
        button: 0,
        clientX: 12,
        clientY: 0,
      }),
    );

    expect(clicks).toBe(0);
  });
});
