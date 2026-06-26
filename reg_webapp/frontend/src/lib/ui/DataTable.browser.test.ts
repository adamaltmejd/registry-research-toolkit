import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import DataTable from "./DataTable.svelte";
import type { Column } from "./types";

// DataTable: the load-bearing hooks are (1) micro-label scope="col" headers,
// (2) right-aligned + mono numeric/mono cells, (3) the custom-cell escape hatch,
// (4) OPTIONAL selection — keyboard-focusable button-rows with aria-selected +
// the selected style — vs a plain static table when selection props are absent.

// Fixtures are typed against the component's DEFAULT generic instantiation
// (`Record<string, unknown>`): vitest-browser-svelte's `render(Component, props)`
// can't infer the `Row` generic from the props (unlike `<DataTable .. />` in a
// .svelte file, where Svelte infers it), so the props must match the constraint
// default exactly. A concrete row interface would force unsound variance casts
// on the `cell`/`getRowId`/`onselect` positions; the base type assigns cleanly.
type Row = Record<string, unknown>;

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
    const { container } = render(DataTable, { columns, rows });
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
    const { container } = render(DataTable, { columns, rows });
    // The numeric "Count" cell (3rd col) of the first data row.
    const firstRowCells = container.querySelectorAll("tbody tr:first-child td");
    const countCell = firstRowCells[2];
    expect(countCell).toHaveClass("align-end");
    expect(countCell).toHaveClass("mono");
  });

  it("renders cell values by column key by default", async () => {
    await render(DataTable, { columns, rows });
    // exact: "Man" is a substring of "Woman".
    await expect.element(page.getByText("Man", { exact: true })).toBeVisible();
    await expect.element(page.getByText("120")).toBeVisible();
  });

  it("uses the custom cell snippet when provided", async () => {
    const cell = createRawSnippet(() => ({
      render: () => "<span>custom</span>",
    }));
    const { container } = render(DataTable, { columns, rows, cell });
    // Every cell routes through the snippet → no raw value text.
    expect(container.querySelectorAll("tbody td").length).toBeGreaterThan(0);
    await expect.element(page.getByText("custom").first()).toBeVisible();
  });

  it("renders a plain static table without selection props", async () => {
    const { container } = render(DataTable, { columns, rows });
    const tr = container.querySelector("tbody tr");
    expect(tr).not.toHaveAttribute("tabindex");
    expect(tr).not.toHaveAttribute("role", "button");
  });

  it("makes rows selectable button-rows when selection props are passed", async () => {
    let selected = "";
    const { container } = render(DataTable, {
      columns,
      rows,
      getRowId: (r: Row) => String(r.code),
      selectedId: "1",
      onselect: (r: Row) => {
        selected = String(r.code);
      },
    });
    const trs = container.querySelectorAll("tbody tr");
    // Selected row carries the class + aria-selected; rows are button + tabbable.
    expect(trs[0]).toHaveClass("selected");
    expect(trs[0]).toHaveAttribute("aria-selected", "true");
    expect(trs[0]).toHaveAttribute("role", "button");
    expect(trs[0]).toHaveAttribute("tabindex", "0");
    expect(trs[1]).toHaveAttribute("aria-selected", "false");

    // Click activates onselect.
    (trs[1] as HTMLElement).click();
    expect(selected).toBe("2");
  });
});
