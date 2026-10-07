import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { VariableGraphNode } from "./api";
import RepresentationPicker, {
  type PickerBand,
} from "./RepresentationPicker.svelte";
import {
  AXES,
  clickFilter,
  edge,
  graph,
  graphNode,
  graphState,
  PROPS,
  row,
} from "./representation-picker-test-helpers";

// Split from RepresentationPicker.browser.test.ts by contract surface: graph band matching, context cells and representation edges.
// Siblings: RepresentationPicker.{graph,graph-fallback,graph-history,labels,filters,staging,row-identity}.browser.test.ts.

describe("RepresentationPicker graph mode (#904)", () => {
  function smallSuccessionFixture() {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    const aBand = {
      key: aFqid,
      name: "A",
      registerPrefix: "scb/lisa",
      rows: [row({ column: "Acol" })],
    } satisfies PickerBand;
    const bBand = {
      key: bFqid,
      name: "B",
      registerPrefix: "scb/lisa",
      rows: [row({ column: "Bcol" })],
    } satisfies PickerBand;
    const nodes = [
      graphNode(aFqid, {
        label: "A",
        states: [graphState({ delivery_column_name: "Acol" })],
      }),
      graphNode(bFqid, {
        label: "B",
        states: [
          graphState({
            state_id: "2",
            period_scope: "intervals",
            representation_run_id: 2,
            delivery_column_name: "Bcol",
            valid_from: "2011-01-01",
            valid_to: "9999-12-31",
          }),
        ],
      }),
    ];
    return {
      bands: [aBand, bBand],
      graph: graph({
        nodes,
        edges: [edge(aFqid, bFqid)],
        focus_id: aFqid,
      }),
    };
  }

  /** A one-column variable whose delivery column was RENAMED mid-life (OLD → NEW):
   * TWO representation runs, and so graph context on an edge-less graph. `…Band` is
   * the picker row it offers; `…Node` the matching graph node. */
  function renamedRunBand(fqid: string, name: string): PickerBand {
    return {
      key: fqid,
      name,
      registerPrefix: "scb/lisa",
      rows: [
        row({
          column: "NEW",
          representation: null,
          renamedColumns: ["OLD"],
          from: "2000-01-01",
          to: "2020-12-31",
          windows: [
            { from: "2000-01-01", to: "2009-12-31" },
            { from: "2010-01-01", to: "2020-12-31" },
          ],
          period: "2000 – 2020",
        }),
      ],
    };
  }

  function renamedRunNode(fqid: string): VariableGraphNode {
    return graphNode(fqid, {
      states: [
        graphState({
          delivery_column_name: "OLD",
          valid_from: "2000-01-01",
          valid_to: "2009-12-31",
        }),
        graphState({
          state_id: "2",
          period_scope: "intervals",
          representation_run_id: 2,
          delivery_column_name: "NEW",
          valid_from: "2010-01-01",
          valid_to: "2020-12-31",
        }),
      ],
    });
  }

  it("matches graph nodes to picker bands through same_as aliases", async () => {
    const onapply = vi.fn();
    const aliasFqid = "scb/lisa/alias";
    const canonicalFqid = "scb/lisa/canonical";
    const successorFqid = "scb/lisa/successor";
    await render(RepresentationPicker, {
      bands: [
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
      onapply,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph picker not rendered");
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    expect(document.querySelector(".graph-lane.focused")).not.toBeNull();

    await page.getByRole("checkbox", { name: /AliasCol/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].band.key).toBe(aliasFqid);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("AliasCol");
  });

  it("renders leaf sibling graph context as unavailable cells outside the picker band", async () => {
    const fixture = smallSuccessionFixture();
    await render(RepresentationPicker, {
      bands: [fixture.bands[0]],
      graph: fixture.graph,
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph picker not rendered");
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    await expect
      .element(page.getByRole("checkbox", { name: /Acol/ }))
      .toBeVisible();
    const unavailable = [
      ...document.querySelectorAll<HTMLElement>(".graph-cell.unavailable"),
    ].map((el) => el.textContent ?? "");
    expect(unavailable.some((text) => text.includes("Bcol"))).toBe(true);
  });

  it("renders graph context for a picker band with zero selectable rows", async () => {
    const aFqid = "scb/lisa/no-column";
    const bFqid = "scb/lisa/no-column-next";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "No-column variable",
          registerPrefix: "scb/lisa",
          rows: [],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            label: "No-column variable",
            states: [
              graphState({
                delivery_column_name: null,
                value_set_version_label: "uncolumned coding",
              }),
            ],
          }),
          graphNode(bFqid, {
            label: "No-column successor",
            states: [
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: null,
                value_set_version_label: "successor coding",
                valid_from: "2011-01-01",
                valid_to: null,
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
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph picker not rendered");
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    const graphText =
      document.querySelector(".graph-picker")?.textContent ?? "";
    expect(graphText).toContain("uncolumned coding");
    expect(graphText).toContain("successor coding");
    expect(
      document.querySelector(".graph-picker input[type='checkbox']"),
    ).toBeNull();
  });

  it("renders uncolumned leaf graph cells beside selectable cells", async () => {
    const aFqid = "scb/lisa/mixed";
    const bFqid = "scb/lisa/mixed-next";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Mixed leaf",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              column: "COL",
              from: "2000-01-01",
              to: "2009-12-31",
              windows: [{ from: "2000-01-01", to: "2009-12-31" }],
              period: "2000 – 2009",
            }),
          ],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                delivery_column_name: "COL",
                valid_from: "2000-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: "2",
                period_scope: "intervals",
                representation_run_id: 2,
                delivery_column_name: null,
                value_set_version_label: "uncolumned coding",
                valid_from: "2010-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: "3",
                period_scope: "intervals",
                representation_run_id: 3,
                delivery_column_name: null,
                value_set_version_label: "successor context",
                valid_from: "2021-01-01",
                valid_to: null,
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
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph picker not rendered");
      }
    });
    await expect
      .element(page.getByRole("checkbox", { name: /COL/ }))
      .toBeVisible();
    const unavailable = [
      ...document.querySelectorAll<HTMLElement>(".graph-cell.unavailable"),
    ].map((el) => el.textContent ?? "");
    expect(unavailable.some((text) => text.includes("uncolumned coding"))).toBe(
      true,
    );
  });

  it("renders an edge-less GROUP graph the same way (Y-78)", async () => {
    // The `person-orgnr` group: several members, no succession edge between them YET.
    // The era context a member's own leaf page draws — its runs laid out in time — is
    // the same context on the group page, so the group draws it too rather than
    // dropping to the list only because the graph carries no edge.
    const renamedFqid = "scb/lisa/person-orgnr";
    const otherFqid = "scb/lisa/person-orgnr-2";
    await render(RepresentationPicker, {
      bands: [
        {
          ...renamedRunBand(renamedFqid, "Person-orgnr"),
          href: "/catalog/scb/lisa/person-orgnr",
        },
        {
          key: otherFqid,
          name: "Person-orgnr 2",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/person-orgnr-2",
          rows: [row({ column: "ORG" })],
        } satisfies PickerBand,
      ],
      graphMemberHrefs: {
        [renamedFqid]: "/catalog/scb/lisa/person-orgnr",
        [otherFqid]: "/catalog/scb/lisa/person-orgnr-2",
      },
      graph: graph({
        nodes: [
          renamedRunNode(renamedFqid),
          graphNode(otherFqid, {
            states: [graphState({ delivery_column_name: "ORG" })],
          }),
        ],
        edges: [],
        focus_id: null,
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      const graphText =
        document.querySelector(".graph-picker")?.textContent ?? "";
      if (!graphText.includes("OLD") || !graphText.includes("ORG")) {
        throw new Error(`edge-less group graph not rendered: ${graphText}`);
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    // Still selectable, and still the group's own members only.
    await expect
      .element(page.getByRole("checkbox", { name: /^OLD\b/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /^ORG\b/ }))
      .toBeVisible();
  });

  it("draws same-variable representation edges between graph cells", async () => {
    const aFqid = "scb/lisa/renamed";
    await render(RepresentationPicker, {
      bands: [renamedRunBand(aFqid, "Renamed leaf")],
      graph: graph({
        nodes: [renamedRunNode(aFqid)],
        edges: [
          {
            id: "repr-old-new",
            kind: "succession",
            source: aFqid,
            target: aFqid,
            label: "identifier rename",
            effective_year: 2010,
            source_column: "OLD",
            target_column: "NEW",
            variant: null,
          },
        ],
        focus_id: aFqid,
      }),
      ...PROPS,
    });

    const line = await vi.waitFor(() => {
      const edge = document.querySelector<SVGLineElement>(
        ".graph-edge.representation",
      );
      if (!edge) {
        throw new Error("representation edge not rendered");
      }
      return edge;
    });
    const x1 = Number(line.getAttribute("x1"));
    const x2 = Number(line.getAttribute("x2"));
    const y1 = Number(line.getAttribute("y1"));
    const y2 = Number(line.getAttribute("y2"));
    expect(Math.abs(x2 - x1)).toBeGreaterThan(8);
    expect(Math.abs(y2 - y1)).toBeLessThan(1);
    expect(document.querySelector(".graph-reason")?.textContent).toContain(
      "identifier rename · 2010",
    );
    expect(document.querySelector(".graph-picker")?.textContent).not.toContain(
      "OLD → NEW · 2010",
    );
  });

  it("hides representation edges when filters hide an endpoint cell", async () => {
    const aFqid = "scb/iot/filter-edge";
    await render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Filtered edge",
          registerPrefix: "scb/iot",
          rows: [
            row({
              column: "OLD",
              valueSetLabel: "kr",
              from: "2000-01-01",
              to: "2009-12-31",
              windows: [{ from: "2000-01-01", to: "2009-12-31" }],
              period: "2000 – 2009",
            }),
            row({
              column: "NEW",
              valueSetLabel: "kr",
              from: "2010-01-01",
              to: "2020-12-31",
              windows: [{ from: "2010-01-01", to: "2020-12-31" }],
              period: "2010 – 2020",
            }),
          ],
          facetsByColumn: {
            OLD: [
              { axis: "enhet", value: "ind", label: "Individ" },
              { axis: "hush", value: "h1", label: "Hushall" },
            ],
            NEW: [
              { axis: "enhet", value: "ind", label: "Individ" },
              { axis: "hush", value: "h2", label: "Familj" },
            ],
          },
        } satisfies PickerBand,
      ],
      axes: AXES,
      graph: graph({
        nodes: [renamedRunNode(aFqid)],
        edges: [
          {
            id: "repr-old-new",
            kind: "succession",
            source: aFqid,
            target: aFqid,
            label: "identifier rename",
            effective_year: 2010,
            source_column: "OLD",
            target_column: "NEW",
            variant: null,
          },
        ],
        focus_id: aFqid,
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".graph-edge.representation")) {
        throw new Error("representation edge not initially rendered");
      }
    });

    clickFilter("Familj");

    await expect
      .element(page.getByText("Showing 1 of 2 columns"))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /^NEW\b/ }))
      .toBeVisible();
    await vi.waitFor(() => {
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph mode should remain active");
      }
      if (document.querySelector(".graph-edge")) {
        throw new Error("filtered representation edge still rendered");
      }
      if (document.querySelector(".graph-reason")) {
        throw new Error("filtered representation edge label still rendered");
      }
      if (document.querySelector(".graph-fallback li")) {
        throw new Error("filtered representation edge fallback still rendered");
      }
    });
  });
});
