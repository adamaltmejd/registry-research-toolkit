import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import RepresentationPicker, {
  type PickerBand,
} from "./RepresentationPicker.svelte";
import {
  clickFilter,
  edge,
  graph,
  graphNode,
  graphState,
  PROPS,
  row,
  visibleColumns,
} from "./representation-picker-test-helpers";

// Split from RepresentationPicker.browser.test.ts by contract surface: when graph mode falls back to the compact list.
// Siblings: RepresentationPicker.{graph,graph-fallback,graph-history,labels,filters,staging,row-identity}.browser.test.ts.

describe("RepresentationPicker graph mode (#904)", () => {
  it("falls back when one same_as graph node matches multiple picker bands", async () => {
    const aliasFqid = "scb/lisa/alias";
    const canonicalFqid = "scb/lisa/canonical";
    const successorFqid = "scb/lisa/successor";
    await render(RepresentationPicker, {
      bands: [
        {
          key: canonicalFqid,
          name: "Canonical",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "AliasCol" })],
        } satisfies PickerBand,
        {
          key: aliasFqid,
          name: "Alias leaf",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "AliasCol" })],
        } satisfies PickerBand,
        {
          key: successorFqid,
          name: "Successor",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "NextCol" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(canonicalFqid, {
            label: "Canonical",
            states: [graphState({ delivery_column_name: "AliasCol" })],
            same_as: [{ fqid: aliasFqid, register: "lisa_old" }],
          }),
          graphNode(successorFqid, {
            label: "Successor",
            states: [
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "NextCol",
                valid_from: "2011-01-01",
                valid_to: "9999-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(canonicalFqid, successorFqid)],
        focus_id: canonicalFqid,
      }),
      ...PROPS,
      focusKey: aliasFqid,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns()).toEqual(["AliasCol", "AliasCol", "NextCol"]);
  });

  it("keeps empty group bands visible when no graph can render", async () => {
    const aFqid = "scb/lisa/empty-a";
    const bFqid = "scb/lisa/empty-b";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Empty A",
          registerPrefix: "scb/lisa",
          rows: [],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "Empty B",
          registerPrefix: "scb/lisa",
          rows: [],
        } satisfies PickerBand,
      ],
      graphMemberHrefs: {
        [aFqid]: "/catalog/scb/lisa/empty-a",
        [bFqid]: "/catalog/scb/lisa/empty-b",
      },
      graph: graph({ nodes: [], edges: [], focus_id: null }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("empty group list not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(document.querySelectorAll(".empty-note")).toHaveLength(2);
    expect(document.body.textContent).toContain("No columns");
  });

  it("falls back when a graph run also carries a non-member column", async () => {
    const onapply = vi.fn();
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "MEMBER" })],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "NEXT" })],
        } satisfies PickerBand,
      ],
      graphMemberHrefs: {
        [aFqid]: "/catalog/scb/lisa/a",
        [bFqid]: "/catalog/scb/lisa/b",
      },
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: "1",
                period_scope: "intervals",
                representation_run_id: 1,
                delivery_column_name: "HIDDEN",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 1,
                delivery_column_name: "MEMBER",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "NEXT" })],
          }),
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: null,
      }),
      ...PROPS,
      onapply,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns()).toEqual(["MEMBER", "NEXT"]);
    expect(document.body.textContent).not.toContain("HIDDEN");

    // Two distinct, never-repeated names → no cluster headings, so each list row is
    // led by its variable NAME ahead of the column chip (Y-78).
    await page.getByRole("checkbox", { name: /^A MEMBER\b/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("MEMBER");
  });

  it("falls back when a group graph includes a non-member variable node", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    const outsideFqid = "scb/lisa/outside";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "Acol" })],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "Bcol" })],
        } satisfies PickerBand,
      ],
      graphMemberHrefs: {
        [aFqid]: "/catalog/scb/lisa/a",
        [bFqid]: "/catalog/scb/lisa/b",
      },
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [graphState({ delivery_column_name: "Acol" })],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "Bcol" })],
          }),
          graphNode(outsideFqid, {
            states: [graphState({ delivery_column_name: "Hidden" })],
          }),
        ],
        edges: [edge(aFqid, outsideFqid)],
        focus_id: null,
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns()).toEqual(["Acol", "Bcol"]);
    expect(document.body.textContent).not.toContain("Hidden");
  });

  it("uses the visible graph projection for size limits after filtering", async () => {
    const nodes = Array.from({ length: 19 }, (_, i) => {
      const fqid = `scb/lisa/v${i}`;
      return graphNode(fqid, {
        states: [
          graphState({
            state_id: String(i + 1),
            period_scope: "intervals",
            representation_run_id: i + 1,
            delivery_column_name: `C${i}`,
            valid_from: `${2000 + i}-01-01`,
            valid_to: `${2000 + i}-12-31`,
          }),
        ],
      });
    });
    const bands = nodes.map(
      (node, i) =>
        ({
          key: node.fqid as string,
          name: node.label,
          registerPrefix: "scb/lisa",
          href: `/catalog/${node.fqid as string}`,
          rows: [row({ column: `C${i}` })],
          facetsByColumn: {
            [`C${i}`]: [
              {
                axis: "era",
                value: i === 0 ? "old" : "new",
                label: i === 0 ? "Old" : "New",
              },
            ],
          },
        }) satisfies PickerBand,
    );

    await render(RepresentationPicker, {
      bands,
      axes: [{ name: "era", label: "Era" }],
      graphMemberHrefs: Object.fromEntries(
        nodes.map((node) => [
          node.fqid as string,
          `/catalog/${(node.fqid as string).replaceAll("/", "/")}`,
        ]),
      ),
      graph: graph({
        nodes,
        edges: [edge(nodes[0].id, nodes[1].id)],
        focus_id: nodes[1].id,
      }),
      ...PROPS,
      focusKey: nodes[1].fqid ?? null,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("initial large-graph list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();

    clickFilter("Old");
    await vi.waitFor(() => {
      const graphText =
        document.querySelector(".graph-picker")?.textContent ?? "";
      if (!graphText.includes("C0") || graphText.includes("C1")) {
        throw new Error(`filtered projected graph not ready: ${graphText}`);
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    await expect
      .element(page.getByText("Showing 1 of 19 columns"))
      .toBeVisible();
  });

  it("uses the visible cell projection for size limits after filtering", async () => {
    const aFqid = "scb/lisa/many-columns";
    const bFqid = "scb/lisa/successor";
    const columns = Array.from({ length: 50 }, (_, i) => `C${i}`);
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Many columns",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/many-columns",
          rows: columns.map((column, i) =>
            row({
              column,
              from: `${2000 + i}-01-01`,
              to: `${2000 + i}-12-31`,
              windows: [{ from: `${2000 + i}-01-01`, to: `${2000 + i}-12-31` }],
              period: `${2000 + i}`,
            }),
          ),
          facetsByColumn: Object.fromEntries(
            columns.map((column, i) => [
              column,
              [
                {
                  axis: "era",
                  value: i === 0 ? "old" : "new",
                  label: i === 0 ? "Old" : "New",
                },
              ],
            ]),
          ),
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "Successor",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/successor",
          rows: [row({ column: "NEXT" })],
          facetsByColumn: {
            NEXT: [{ axis: "era", value: "new", label: "New" }],
          },
        } satisfies PickerBand,
      ],
      axes: [{ name: "era", label: "Era" }],
      graphMemberHrefs: {
        [aFqid]: "/catalog/scb/lisa/many-columns",
        [bFqid]: "/catalog/scb/lisa/successor",
      },
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: columns.map((column, i) =>
              graphState({
                state_id: String(i + 1),
                period_scope: "intervals",
                representation_run_id: i + 1,
                delivery_column_name: column,
                valid_from: `${2000 + i}-01-01`,
                valid_to: `${2000 + i}-12-31`,
              }),
            ),
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: "100",
                period_scope: "intervals",
                representation_run_id: 100,
                delivery_column_name: "NEXT",
                valid_from: "2050-01-01",
                valid_to: "2050-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: null,
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("initial many-cell list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();

    clickFilter("Old");
    await vi.waitFor(() => {
      const graphText =
        document.querySelector(".graph-picker")?.textContent ?? "";
      if (!graphText.includes("C0") || graphText.includes("C1")) {
        throw new Error(`filtered cell projection not ready: ${graphText}`);
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    await expect
      .element(page.getByText("Showing 1 of 51 columns"))
      .toBeVisible();
  });

  it("falls back when a picker row has no graph cell coverage", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [
            row({ column: "MEMBER" }),
            row({
              column: "OMITTED",
              selectable: false,
              period: "not delivered",
              wirePeriod: null,
            }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "NEXT" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [graphState({ delivery_column_name: "MEMBER" })],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "NEXT" })],
          }),
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: null,
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns()).toEqual(["MEMBER", "OMITTED", "NEXT"]);
    await expect
      .element(page.getByRole("checkbox", { name: /OMITTED/ }))
      .toBeDisabled();
    await expect
      .element(page.getByText("not delivered", { exact: true }))
      .toBeVisible();
  });

  it("falls back when narrow selectable graph cells carry inline context", async () => {
    const aFqid = "scb/forskola/sun2000inr-prio";
    const bFqid = "scb/forskola/sun2000inr-next";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Priority coding",
          registerPrefix: "scb/forskola",
          rows: [
            row({
              column: "SUNPRIO",
              valueSetLabel: "SUN 2000",
              codingsVary: true,
              from: "2000-01-01",
              to: "2000-12-31",
              windows: [{ from: "2000-01-01", to: "2000-12-31" }],
              period: "2000",
            }),
          ],
          facetsByColumn: {
            SUNPRIO: [{ axis: "priority", value: "old", label: "Old" }],
          },
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "Next coding",
          registerPrefix: "scb/forskola",
          rows: [
            row({
              column: "SUNNEXT",
              valueSetLabel: "SUN 2020",
              from: "2020-01-01",
              to: "2020-12-31",
              windows: [{ from: "2020-01-01", to: "2020-12-31" }],
              period: "2020",
            }),
          ],
          facetsByColumn: {
            SUNNEXT: [{ axis: "priority", value: "new", label: "New" }],
          },
        } satisfies PickerBand,
      ],
      axes: [{ name: "priority", label: "Priority" }],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                delivery_column_name: "SUNPRIO",
                value_set_version_label: "SUN 2000",
                valid_from: "2000-01-01",
                valid_to: "2000-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "SUNNEXT",
                value_set_version_label: "SUN 2020",
                valid_from: "2020-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: aFqid,
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    await expect
      .element(page.getByRole("group", { name: /Filter columns/ }))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["SUNPRIO", "SUNNEXT"]);

    clickFilter("Old");
    await vi.waitFor(() => {
      expect(visibleColumns()).toEqual(["SUNPRIO"]);
    });
  });

  it("falls back to the compact list when one graph run has several selectable columns", async () => {
    const aFqid = "scb/lisa/monthly";
    const bFqid = "scb/lisa/successor";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Monthly family",
          registerPrefix: "scb/lisa",
          rows: [
            row({ column: "JAN", period: "2000 – 2010" }),
            row({ column: "FEB", period: "2000 – 2010" }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "Successor",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "NEXT" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: "1",
                period_scope: "intervals",
                representation_run_id: 1,
                delivery_column_name: "JAN",
              }),
              graphState({
                state_id: "1",
                period_scope: "intervals",
                representation_run_id: 1,
                delivery_column_name: "FEB",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: "NEXT",
                valid_from: "2011-01-01",
                valid_to: "9999-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: aFqid,
      }),
      ...PROPS,
    });

    await expect
      .element(page.getByRole("checkbox", { name: /FEB/ }))
      .toBeVisible();
    expect(
      page.getByRole("group", { name: "Graph column picker" }).elements(),
    ).toEqual([]);
  });
});
