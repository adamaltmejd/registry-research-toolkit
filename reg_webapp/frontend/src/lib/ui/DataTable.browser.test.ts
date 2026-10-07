import type { Component, Snippet } from "svelte";
import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import DataTable from "./DataTable.svelte";
import DataTableEmptyCellHarness from "./DataTableEmptyCellHarness.svelte";
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

const twoColumnColumns: Column<Row>[] = [
  { key: "code", label: "Register" },
  { key: "label", label: "Description" },
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

  it("left-aligns a numeric cell in the stacked (narrow) layout", async () => {
    // Regression guard for the stacked-card alignment reset. The non-media
    // `.align-end { text-align: right }` rule out-specifies a bare `td`, so the
    // `@media (max-width: 48rem)` block's general `td { text-align: left }` can't
    // override a numeric column on its own — it needs the explicit
    // `td.align-end { text-align: left }`. Once stacked, every cell reads left.
    //
    // The suite otherwise avoids asserting `@media` CSS, but real Chromium lets us
    // drive the viewport under the 48rem (768px @16px root) breakpoint and read
    // the COMPUTED alignment, which exercises exactly the reset rule. Restore the
    // viewport afterward so the narrow size doesn't leak into sibling tests.
    await page.viewport(600, 800);
    try {
      const { container } = await renderTable({ columns, rows });
      const countCell = container.querySelector<HTMLElement>(
        "tbody tr:first-child td.align-end",
      );
      if (!countCell) throw new Error("numeric (.align-end) cell not found");
      // jsdom-free: a real engine resolves `start`/`left` per the LTR doc dir.
      const align = getComputedStyle(countCell).textAlign;
      expect(["left", "start"]).toContain(align);
    } finally {
      await page.viewport(1280, 800);
    }
  });

  it("renders framed two-column narrow rows as flat one-column rows", async () => {
    await page.viewport(600, 800);
    try {
      const { container } = await renderTable({
        columns: twoColumnColumns,
        rows,
        framed: true,
      });
      const firstRow = container.querySelector(
        "tbody tr:first-child",
      ) as HTMLElement;
      const secondRow = container.querySelector(
        "tbody tr:nth-child(2)",
      ) as HTMLElement;
      const firstCell = container.querySelector(
        "tbody tr:first-child td:first-child",
      ) as HTMLElement;
      const secondCell = container.querySelector(
        "tbody tr:first-child td:nth-child(2)",
      ) as HTMLElement;
      const hiddenHeader = container.querySelector(
        "thead th:nth-child(2)",
      ) as HTMLElement;

      expect(getComputedStyle(firstRow).display).toBe("grid");
      expect(
        getComputedStyle(firstRow).gridTemplateColumns.split(" "),
      ).toHaveLength(1);
      expect(getComputedStyle(firstRow).borderRadius).toBe("0px");
      expect(getComputedStyle(secondRow).marginTop).toBe("0px");
      expect(getComputedStyle(firstRow).borderBottomStyle).toBe("solid");
      expect(getComputedStyle(firstCell).borderBottomStyle).toBe("none");
      expect(getComputedStyle(secondCell, "::before").content).toBe("none");
      expect(getComputedStyle(hiddenHeader).position).toBe("absolute");
    } finally {
      await page.viewport(1280, 800);
    }
  });

  it("scrolls a wide table inside its own wrapper instead of widening the page (Y-97)", async () => {
    // Atomic (mono) cell content can't break/wrap, so the browser can't shrink
    // these columns to fit — a real overflow regardless of the table's `auto`
    // layout algorithm, unlike breakable text which could get squeezed and mask
    // the bug this proof guards against.
    const wideColumns: Column<Record<string, string>>[] = Array.from(
      { length: 8 },
      (_, i) => ({ key: `c${i}`, label: `Column ${i}`, mono: true }),
    );
    const wideRow: Record<string, string> = {};
    for (let i = 0; i < wideColumns.length; i++) {
      wideRow[`c${i}`] = "X".repeat(60);
    }
    const { container } = await renderTable<Record<string, string>>({
      columns: wideColumns,
      rows: [wideRow],
    });

    // The table's own scroll wrapper absorbs the overflow…
    const wrapper = container.querySelector<HTMLElement>(".table-scroll");
    expect(wrapper).not.toBeNull();
    expect(wrapper?.scrollWidth ?? 0).toBeGreaterThan(
      wrapper?.clientWidth ?? 0,
    );
    // …so the document itself never grows a horizontal scrollbar.
    expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(
      document.documentElement.clientWidth + 1,
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
    const { container } = await render(DataTableEmptyCellHarness, {});
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
});
