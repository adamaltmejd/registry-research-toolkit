import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  GraphEdge,
  GraphState,
  GroupAxisModel,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import type { PickerRepresentation } from "./catalog";
import RepresentationPicker, {
  type PickerBand,
} from "./RepresentationPicker.svelte";
import { type PickerCommittedRow, pickerRowKey } from "./staged_picker";

// RepresentationPicker drives the concept-group column picker. #908 adds
// dimension-type marking (per-row axis markers) + per-dimension filter controls
// (facet axis / population / coding). Render the component directly with `bands` +
// `axes` props — no API mocks needed; the picker is purely presentational.

function row(over: Partial<PickerRepresentation>): PickerRepresentation {
  return {
    key: `${over.variant ?? "v"}::${over.column ?? "Col"}`,
    variant: "v",
    variantLabel: over.variant ?? "v",
    column: over.column ?? "Col",
    representation: over.column ?? "Col",
    from: "2000-01-01",
    to: "2010-12-31",
    windows: [{ from: "2000-01-01", to: "2010-12-31" }],
    period: "2000 – 2010",
    wirePeriod: "2000..2010",
    valueSetLabel: "",
    codingsVary: false,
    renamedColumns: [],
    ...over,
  };
}

function graphState(over: Partial<GraphState> = {}): GraphState {
  return {
    state_id: 1,
    variant: "v",
    variant_label: null,
    representation_run_id: 1,
    valid_from: "2000-01-01",
    valid_to: "2010-12-31",
    value_set_id: null,
    value_set_version_label: "",
    classification_slug: null,
    delivery_column_name: "Col",
    ...over,
  };
}

function graphNode(
  fqid: string,
  over: Partial<VariableGraphNode> = {},
): VariableGraphNode {
  return {
    kind: "variable",
    id: fqid,
    fqid,
    label: fqid.split("/").at(-1) ?? fqid,
    group_key: "group",
    group_label: "Concept group",
    definition: null,
    description: null,
    operational_definition: null,
    facets: [],
    states: [graphState({ delivery_column_name: "Col" })],
    same_as: [],
    ...over,
  };
}

function graph(over: Partial<RelationshipGraph> = {}): RelationshipGraph {
  return { nodes: [], edges: [], focus_id: null, ...over };
}

function edge(source: string, target: string): GraphEdge {
  return {
    id: `${source}->${target}`,
    kind: "succession",
    source,
    target,
    label: null,
  };
}

/** A multi-axis representation group: one band, three delivery-column rows, each a
 * member with a distinct (enhet, hush) facet pair — the shape the #819 families
 * have and that #908's filters narrow. */
function multiAxisBand(): PickerBand {
  return {
    key: "scb/iot/dispink",
    name: "Disponibel inkomst",
    registerPrefix: "scb/iot",
    rows: [
      row({ column: "DIN1", valueSetLabel: "kr" }),
      row({ column: "DIN2", valueSetLabel: "kr" }),
      row({ column: "DIN3", valueSetLabel: "kr" }),
    ],
    facetsByColumn: {
      DIN1: [
        { axis: "enhet", value: "ind", label: "Individ" },
        { axis: "hush", value: "h1", label: "Hushall" },
      ],
      DIN2: [
        { axis: "enhet", value: "ind", label: "Individ" },
        { axis: "hush", value: "h2", label: "Familj" },
      ],
      DIN3: [
        { axis: "enhet", value: "fam", label: "Konsumtionsenhet" },
        { axis: "hush", value: "h1", label: "Hushall" },
      ],
    },
  };
}

const AXES: GroupAxisModel[] = [
  { name: "enhet", label: "Enhet" },
  { name: "hush", label: "Hushallsbegrepp" },
];

const PROPS = {
  window: null,
  canAdd: true,
  onapply: vi.fn(),
} as const;

function committedRowsFor(
  band: PickerBand,
  row: PickerRepresentation,
): Map<string, PickerCommittedRow> {
  const key = pickerRowKey(band, row);
  return new Map([
    [
      key,
      {
        key,
        registerVariant: `${band.registerPrefix}/${row.variant}`,
        variable: band.key,
        representation: row.representation,
        sourceName: "Source",
        sourcePeriod: 2000,
      },
    ],
  ]);
}

/** The delivery-column chips of the currently-visible column ROWS (not the filter
 * fieldsets). */
function visibleColumns(): (string | undefined)[] {
  return [...document.querySelectorAll(".col-list .col-row .col-chip")].map(
    (c) => c.textContent?.replace("↗", "").trim(),
  );
}

/** Click a filter PILL (a labelled checkbox inside a `.dim-filter` fieldset) by its
 * value text — scoped to `.dim-filters` so it never hits a row checkbox. */
function clickFilter(value: string): void {
  const pill = [...document.querySelectorAll(".dim-filters .filter-pill")].find(
    (p) => p.textContent?.trim() === value,
  ) as HTMLElement | undefined;
  if (!pill) {
    throw new Error(`filter pill not found: ${value}`);
  }
  pill.click();
}

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
            state_id: 2,
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

  it("uses the graph/time-band picker for a small edge-bearing variable graph", async () => {
    const onapply = vi.fn();
    const fixture = smallSuccessionFixture();
    render(RepresentationPicker, {
      bands: fixture.bands,
      graph: fixture.graph,
      ...PROPS,
      onapply,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph picker not rendered");
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    expect(document.querySelector(".graph-edge")).not.toBeNull();

    await page.getByRole("checkbox", { name: /Acol/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("Acol");
  });

  it("matches graph nodes to picker bands through same_as aliases", async () => {
    const onapply = vi.fn();
    const aliasFqid = "scb/lisa/alias";
    const canonicalFqid = "scb/lisa/canonical";
    const successorFqid = "scb/lisa/successor";
    render(RepresentationPicker, {
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
                state_id: 2,
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

  it("falls back when one same_as graph node matches multiple picker bands", async () => {
    const aliasFqid = "scb/lisa/alias";
    const canonicalFqid = "scb/lisa/canonical";
    const successorFqid = "scb/lisa/successor";
    render(RepresentationPicker, {
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
                state_id: 2,
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

  it("falls back when a leaf graph includes sibling cells outside the picker band", async () => {
    const fixture = smallSuccessionFixture();
    render(RepresentationPicker, {
      bands: [fixture.bands[0]],
      graph: fixture.graph,
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns()).toEqual(["Acol"]);
    expect(document.body.textContent).not.toContain("Bcol");
  });

  it("dims folded graph cells by the cell's era, not the folded row span", async () => {
    const onapply = vi.fn();
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              column: "NEW",
              representation: null,
              renamedColumns: ["OLD"],
              from: "1990-01-01",
              to: "2020-12-31",
              windows: [
                { from: "1990-01-01", to: "1999-12-31" },
                { from: "2000-01-01", to: "2020-12-31" },
              ],
              period: "1990 – 2020",
            }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "OTHER" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: 1,
                representation_run_id: 1,
                delivery_column_name: "OLD",
                valid_from: "1990-01-01",
                valid_to: "1999-12-31",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "NEW",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "OTHER" })],
          }),
        ],
        edges: [edge(bFqid, aFqid)],
        focus_id: aFqid,
      }),
      ...PROPS,
      window: [2010, 2010],
      onapply,
    });

    const oldCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLElement>(".graph-cell"),
      ].find((el) => el.textContent?.includes("OLD"));
      if (!cell) {
        throw new Error("OLD cell not rendered");
      }
      return cell;
    });
    const newCell = [
      ...document.querySelectorAll<HTMLElement>(".graph-cell"),
    ].find((el) => el.textContent?.includes("NEW"));
    expect(oldCell.classList.contains("dimmed")).toBe(true);
    expect(newCell?.classList.contains("dimmed")).toBe(false);

    await page.getByRole("checkbox", { name: /^OLD\b/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("NEW");
  });

  it("keeps an open-start graph cell in-window before the finite graph floor", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              column: "OPEN",
              from: "0001-01-01",
              to: "1999-12-31",
              windows: [{ from: "0001-01-01", to: "1999-12-31" }],
              period: "until 1999",
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
            states: [
              graphState({
                delivery_column_name: "OPEN",
                valid_from: null,
                valid_to: "1999-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "NEXT",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: aFqid,
      }),
      ...PROPS,
      window: [1980, 1980],
    });

    const openCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLElement>(".graph-cell"),
      ].find((el) => el.textContent?.includes("OPEN"));
      if (!cell) {
        throw new Error("OPEN cell not rendered");
      }
      return cell;
    });
    expect(openCell.classList.contains("dimmed")).toBe(false);
  });

  it("shows each rename era's delivered column while toggling the folded row", async () => {
    const onapply = vi.fn();
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              column: "NEW",
              representation: null,
              renamedColumns: ["OLD"],
              from: "1990-01-01",
              to: "2020-12-31",
              period: "1990 – 2020",
            }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "OTHER" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: 1,
                representation_run_id: 1,
                delivery_column_name: "OLD",
                valid_from: "1990-01-01",
                valid_to: "1999-12-31",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "NEW",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "OTHER" })],
          }),
        ],
        edges: [edge(bFqid, aFqid)],
        focus_id: aFqid,
      }),
      ...PROPS,
      onapply,
    });

    await page.getByRole("checkbox", { name: /^OLD\b/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("NEW");
  });

  it("falls back when a graph run also carries a non-member column", async () => {
    const onapply = vi.fn();
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
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
                state_id: 1,
                representation_run_id: 1,
                delivery_column_name: "HIDDEN",
              }),
              graphState({
                state_id: 2,
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

    await page.getByRole("checkbox", { name: /^MEMBER\b/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("MEMBER");
  });

  it("renders each graph cell's own era coding label", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              column: "NEW",
              representation: null,
              renamedColumns: ["OLD"],
              valueSetLabel: "New coding",
              from: "1990-01-01",
              to: "2020-12-31",
              period: "1990 – 2020",
            }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "OTHER" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: 1,
                representation_run_id: 1,
                delivery_column_name: "OLD",
                value_set_version_label: "Old coding",
                valid_from: "1990-01-01",
                valid_to: "1999-12-31",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "NEW",
                value_set_version_label: "New coding",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "OTHER" })],
          }),
        ],
        edges: [edge(bFqid, aFqid)],
        focus_id: null,
      }),
      ...PROPS,
    });

    const oldCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLElement>(".graph-cell"),
      ].find((el) => el.textContent?.includes("OLD"));
      if (!cell) {
        throw new Error("OLD cell not rendered");
      }
      return cell;
    });
    expect(oldCell.textContent).toContain("Old coding");
    expect(oldCell.textContent).not.toContain("New coding");
  });

  it("links a folded graph cell's codings-vary nudge to that cell's era column", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/a",
          rows: [
            row({
              variant: "v",
              column: "NEW",
              representation: null,
              renamedColumns: ["OLD"],
              codingsVary: true,
              from: "1990-01-01",
              to: "2020-12-31",
              period: "1990 – 2020",
            }),
          ],
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "OTHER" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: 1,
                representation_run_id: 1,
                delivery_column_name: "OLD",
                value_set_version_label: "Old coding",
                valid_from: "1990-01-01",
                valid_to: "1999-12-31",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "NEW",
                value_set_version_label: "New coding",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [graphState({ delivery_column_name: "OTHER" })],
          }),
        ],
        edges: [edge(bFqid, aFqid)],
        focus_id: null,
      }),
      ...PROPS,
    });

    const oldCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLElement>(".graph-cell"),
      ].find((el) => el.textContent?.includes("OLD"));
      if (!cell) {
        throw new Error("OLD cell not rendered");
      }
      return cell;
    });
    const nudge = oldCell.querySelector<HTMLAnchorElement>(".codings-vary");
    expect(nudge?.getAttribute("href")).toBe(
      "/catalog/scb/lisa/a?codes=v%3A%3AOLD#states-heading",
    );
  });

  it("falls back when a graph cell has no unambiguous picker member row", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "MEMBER" })],
        } satisfies PickerBand,
      ],
      graphMemberHrefs: { [aFqid]: "/catalog/scb/lisa/a" },
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                state_id: 1,
                representation_run_id: 1,
                delivery_column_name: "MEMBER",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "HIDDEN",
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
    });

    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(document.querySelector(".col-list")).not.toBeNull();
    expect(visibleColumns()).toEqual(["MEMBER"]);
    expect(document.body.textContent).not.toContain("HIDDEN");
  });

  it("falls back when a picker row has no graph cell coverage", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
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

  it("falls back when #908 dimensions would be hidden by graph mode", async () => {
    const band = multiAxisBand();
    render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      graph: graph({
        nodes: [
          graphNode(band.key, {
            states: [
              graphState({ delivery_column_name: "DIN1" }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "DIN2",
              }),
              graphState({
                state_id: 3,
                representation_run_id: 3,
                delivery_column_name: "DIN3",
              }),
            ],
          }),
          graphNode("scb/iot/next", {
            states: [
              graphState({
                state_id: 4,
                representation_run_id: 4,
                delivery_column_name: "NEXT",
              }),
            ],
          }),
        ],
        edges: [edge(band.key, "scb/iot/next")],
        focus_id: null,
      }),
      ...PROPS,
    });

    await expect
      .element(page.getByRole("group", { name: /Filter columns/ }))
      .toBeVisible();
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2", "DIN3"]);
  });

  it("falls back when a declared #908 facet axis has only one value", async () => {
    const aFqid = "scb/iot/dispink";
    const bFqid = "scb/iot/next";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Disponibel inkomst",
          registerPrefix: "scb/iot",
          rows: [row({ column: "DIN1", valueSetLabel: "kr" })],
          facetsByColumn: {
            DIN1: [{ axis: "enhet", value: "ind", label: "Individ" }],
          },
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "Next",
          registerPrefix: "scb/iot",
          rows: [row({ column: "NEXT" })],
        } satisfies PickerBand,
      ],
      axes: [{ name: "enhet", label: "Enhet" }],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [graphState({ delivery_column_name: "DIN1" })],
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "NEXT",
                valid_from: "2011-01-01",
                valid_to: "9999-12-31",
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
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns()).toEqual(["DIN1", "NEXT"]);
    expect(document.body.textContent).toContain("Enhet");
    expect(document.body.textContent).toContain("Individ");
  });

  it("falls back to the compact list when one graph run has several selectable columns", async () => {
    const onapply = vi.fn();
    const aFqid = "scb/lisa/monthly";
    const bFqid = "scb/lisa/successor";
    render(RepresentationPicker, {
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
                state_id: 1,
                representation_run_id: 1,
                delivery_column_name: "JAN",
              }),
              graphState({
                state_id: 1,
                representation_run_id: 1,
                delivery_column_name: "FEB",
              }),
            ],
          }),
          graphNode(bFqid, {
            states: [
              graphState({
                state_id: 2,
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
      onapply,
    });

    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(document.querySelector(".col-list")).not.toBeNull();
    await page.getByRole("checkbox", { name: /FEB/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("FEB");
  });

  it("falls back to the compact list when the graph is too large to draw cleanly", async () => {
    const nodes = Array.from({ length: 19 }, (_, i) => {
      const fqid = `scb/lisa/v${i}`;
      return graphNode(fqid, {
        states: [graphState({ delivery_column_name: `C${i}` })],
      });
    });
    const bands = nodes.map(
      (node, i) =>
        ({
          key: node.fqid as string,
          name: node.label,
          registerPrefix: "scb/lisa",
          rows: [row({ column: `C${i}` })],
        }) satisfies PickerBand,
    );
    render(RepresentationPicker, {
      bands,
      graph: graph({
        nodes,
        edges: [edge(nodes[0].id, nodes[1].id)],
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns().slice(0, 2)).toEqual(["C0", "C1"]);
  });
});

describe("RepresentationPicker dimension marking + filters (#908)", () => {
  it("renders a filter fieldset per discriminating dimension, naming its kind", async () => {
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });
    // Both facet axes discriminate (enhet: ind/fam; hush: h1/h2) → two fieldsets.
    // Coding is constant ("kr") and variant constant ("v") → no control for those.
    await expect
      .element(page.getByRole("group", { name: /Filter columns/ }))
      .toBeVisible();
    const legends = [...document.querySelectorAll(".dim-filter legend")].map(
      (l) => l.textContent?.trim(),
    );
    expect(legends).toEqual(["Enhet", "Hushallsbegrepp"]);
    // Each row is marked with its axis dimension markers (axis label + value).
    const markers = document.querySelector(".col-row .facet-markers");
    expect(markers?.textContent).toContain("Individ");
  });

  it("renders global select-all as an integrated row with selected and indeterminate states", async () => {
    render(RepresentationPicker, {
      bands: [
        multiAxisBand(),
        {
          key: "scb/iot/other",
          name: "Other income",
          registerPrefix: "scb/iot",
          rows: [row({ column: "DIN4", valueSetLabel: "kr" })],
        } satisfies PickerBand,
      ],
      axes: [],
      ...PROPS,
    });
    const selectAllRow = await vi.waitFor(() => {
      const row = document.querySelector<HTMLLabelElement>(
        ".col-list > .select-all-row > label.select-all",
      );
      if (!row) {
        throw new Error("global select-all row not rendered");
      }
      return row;
    });
    expect(selectAllRow.classList.contains("integrated-list-row")).toBe(true);
    expect(selectAllRow.classList.contains("row-btn")).toBe(true);

    const selectAllBox =
      selectAllRow.querySelector<HTMLInputElement>("input.cbox");
    expect(selectAllBox).not.toBeNull();
    expect(selectAllBox?.checked).toBe(false);
    expect(selectAllBox?.indeterminate).toBe(false);

    // Click the row label, not the checkbox itself: the full integrated row toggles.
    selectAllRow.click();
    await expect.element(page.getByText("+4 columns")).toBeVisible();
    expect(selectAllBox?.checked).toBe(true);
    expect(selectAllBox?.indeterminate).toBe(false);
    expect(selectAllRow.classList.contains("selected")).toBe(true);
    const rowBoxes = [
      ...document.querySelectorAll<HTMLInputElement>(
        ".col-list .col-row .row-btn input.cbox",
      ),
    ];
    expect(rowBoxes).toHaveLength(4);
    expect(rowBoxes.every((box) => box.checked)).toBe(true);

    rowBoxes[0].click();
    await expect.element(page.getByText("+3 columns")).toBeVisible();
    expect(selectAllBox?.checked).toBe(false);
    expect(selectAllBox?.indeterminate).toBe(true);
    expect(selectAllRow.classList.contains("selected")).toBe(false);
  });

  it("a single-value group surfaces NO filter controls", async () => {
    render(RepresentationPicker, {
      bands: [
        {
          key: "scb/x/y",
          name: "Y",
          registerPrefix: "scb/x",
          rows: [row({ column: "Cee", valueSetLabel: "one" })],
        } satisfies PickerBand,
      ],
      axes: [],
      ...PROPS,
    });
    await vi.waitFor(() => {
      if (!document.querySelector(".col-row .col-chip")) {
        throw new Error("row not rendered yet");
      }
    });
    expect(visibleColumns()).toEqual(["Cee"]);
    expect(document.querySelector(".dim-filters")).toBeNull();
  });

  it("selecting a facet value narrows the visible rows; clearing restores them", async () => {
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });
    await expect
      .element(page.getByText("Showing 3 of 3 columns"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2", "DIN3"]);

    // Filter Hushallsbegrepp → "Familj" (h2): only DIN2 carries it.
    clickFilter("Familj");
    await expect
      .element(page.getByText("Showing 1 of 3 columns"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN2"]);

    // Clear → all rows back.
    await page.getByRole("button", { name: "Clear filters" }).click();
    await expect
      .element(page.getByText("Showing 3 of 3 columns"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2", "DIN3"]);
  });

  it("filtering is presentation-only: a hidden selected column still commits, flagged in the footer", async () => {
    const onapply = vi.fn();
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onapply,
    });
    // Select DIN3 (carries enhet=fam, hush=h1) via its row checkbox.
    const din3 = await vi.waitFor(() => {
      const cb = [
        ...document.querySelectorAll<HTMLInputElement>(
          ".col-list .row-btn input.cbox",
        ),
      ][2];
      if (!cb) {
        throw new Error("DIN3 row checkbox not yet rendered");
      }
      return cb;
    });
    din3.click();
    await expect.element(page.getByText("+1 column")).toBeVisible();

    // Now filter Enhet → "Individ" (ind): DIN3 (fam) is hidden.
    clickFilter("Individ");
    // The selection persists and the footer signals the hidden selection.
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    // Committing still includes the hidden-but-selected DIN3.
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    const committed = onapply.mock.calls[0][0].adds as {
      row: PickerRepresentation;
    }[];
    expect(committed.map((s) => s.row.column)).toEqual(["DIN3"]);
  });

  it("keeps staged rows when the parent rejects an async apply as stale", async () => {
    const onapply = vi.fn().mockResolvedValue(false);
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onapply,
    });

    await page.getByRole("checkbox", { name: /DIN1/ }).click();
    await expect.element(page.getByText("Will be added")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    expect(onapply).toHaveBeenCalledTimes(1);
    await expect.element(page.getByText("Will be added")).toBeVisible();
    await expect.element(page.getByText("+1 column")).toBeVisible();
  });

  it("freezes staging controls while Apply is pending", async () => {
    let finishApply: () => void = () => {
      throw new Error("apply did not start");
    };
    const applyStarted = new Promise<void>((started) => {
      const onapply = vi.fn().mockImplementation(
        () =>
          new Promise<void>((resolve) => {
            finishApply = resolve;
            started();
          }),
      );
      render(RepresentationPicker, {
        bands: [multiAxisBand()],
        axes: AXES,
        ...PROPS,
        onapply,
      });
    });

    await page.getByRole("checkbox", { name: /DIN1/ }).click();
    await expect.element(page.getByText("Will be added")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    await applyStarted;

    await expect
      .element(page.getByRole("checkbox", { name: /DIN2/ }))
      .toBeDisabled();
    await expect
      .element(page.getByRole("checkbox", { name: /Select all columns of/ }))
      .toBeDisabled();
    await expect
      .element(page.getByRole("button", { name: "Reset" }))
      .toBeDisabled();

    finishApply();
    await expect
      .element(page.getByText("No staged changes"))
      .not.toBeInTheDocument();
    await expect
      .element(
        page.getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        }),
      )
      .not.toBeInTheDocument();
  });

  it("does not stage period-only source changes from a partial picker", async () => {
    const onapply = vi.fn();
    const band = multiAxisBand();
    const committedRows = committedRowsFor(band, band.rows[0]);
    render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      activePeriod: "2001",
      committedRows,
      onapply,
    });

    await expect
      .element(page.getByText("No staged changes"))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByRole("button", { name: "Reset" }))
      .not.toBeInTheDocument();
    await expect
      .element(
        page.getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        }),
      )
      .not.toBeInTheDocument();

    expect(onapply).not.toHaveBeenCalled();
  });

  it("allows remove-only applies before add seed context is ready", async () => {
    const onapply = vi.fn();
    const band = multiAxisBand();
    const committedRows = committedRowsFor(band, band.rows[0]);
    render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      canAdd: false,
      committedRows,
      onapply,
    });

    const rowCheckbox = page.getByRole("checkbox", { name: /DIN1/ });
    await expect.element(rowCheckbox).toBeChecked();
    await rowCheckbox.click();
    await expect.element(page.getByText("Will be removed")).toBeVisible();
    const apply = page.getByRole("button", { name: "Remove from project" });
    await expect.element(apply).toBeEnabled();
    await apply.click();

    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].removes).toHaveLength(1);
  });

  it("labels staged footer actions by diff shape", async () => {
    const band = multiAxisBand();
    const committedRows = committedRowsFor(band, band.rows[0]);
    render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      committedRows,
    });

    await expect
      .element(
        page.getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        }),
      )
      .not.toBeInTheDocument();

    await page.getByRole("checkbox", { name: /DIN2/ }).click();
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .toBeVisible();

    await page.getByRole("checkbox", { name: /DIN1/ }).click();
    await expect
      .element(page.getByRole("button", { name: "Apply changes" }))
      .toBeVisible();
  });

  it("allows committed nonselectable rows to be removed without allowing new adds", async () => {
    const onapply = vi.fn();
    const base = multiAxisBand();
    const nonselectableCommitted = {
      ...base.rows[0],
      selectable: false,
    };
    const nonselectableUncommitted = {
      ...base.rows[1],
      selectable: false,
    };
    const band = {
      ...base,
      rows: [nonselectableCommitted, nonselectableUncommitted, base.rows[2]],
    };
    const committedRows = committedRowsFor(band, nonselectableCommitted);
    render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      committedRows,
      onapply,
    });

    const committedCheckbox = page.getByRole("checkbox", { name: /DIN1/ });
    await expect.element(committedCheckbox).toBeChecked();
    await expect.element(committedCheckbox).toBeEnabled();
    await expect
      .element(page.getByRole("checkbox", { name: /DIN2/ }))
      .toBeDisabled();

    await committedCheckbox.click();
    await expect.element(page.getByText("Will be removed")).toBeVisible();
    const apply = page.getByRole("button", {
      name: /Add to project|Remove from project|Apply changes/,
    });
    await expect.element(apply).toBeEnabled();
    await apply.click();

    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].removes).toHaveLength(1);
    expect(onapply.mock.calls[0][0].adds).toHaveLength(0);
  });

  it("stages all rows backed by the same null binding when one is removed", async () => {
    const onapply = vi.fn();
    const band = multiAxisBand();
    const committedRows = new Map<string, PickerCommittedRow>(
      band.rows.slice(0, 2).map((r) => {
        const key = pickerRowKey(band, r);
        return [
          key,
          {
            key,
            registerVariant: `${band.registerPrefix}/${r.variant}`,
            variable: band.key,
            representation: null,
            sourceName: "Source",
            sourcePeriod: "_default",
          },
        ];
      }),
    );
    render(RepresentationPicker, {
      bands: [band],
      axes: AXES,
      ...PROPS,
      committedRows,
      onapply,
    });

    await page.getByRole("checkbox", { name: /DIN1/ }).click();

    await expect.element(page.getByText("-2 columns")).toBeVisible();
    expect(
      document.querySelectorAll(".col-list .row-btn.staged-remove"),
    ).toHaveLength(2);
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].removes).toHaveLength(2);
  });

  it("toggle-all acts on visible rows only: a hidden-but-selected row survives select-all then deselect-all", async () => {
    const onapply = vi.fn();
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
      onapply,
    });
    // Select DIN3 (enhet=fam, hush=h1) via its row checkbox.
    const din3 = await vi.waitFor(() => {
      const cb = [
        ...document.querySelectorAll<HTMLInputElement>(
          ".col-list .row-btn input.cbox",
        ),
      ][2];
      if (!cb) {
        throw new Error("DIN3 row checkbox not yet rendered");
      }
      return cb;
    });
    din3.click();
    await expect.element(page.getByText("+1 column")).toBeVisible();

    // Filter Enhet → "Individ" (ind): DIN3 (fam) is now hidden but still selected.
    clickFilter("Individ");
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    const selectAll = page.getByRole("checkbox", {
      name: "Select all columns",
    });
    // Select all → adds the 2 visible rows; the hidden DIN3 stays selected (3 total).
    await selectAll.click();
    await expect
      .element(page.getByText("+3 columns (1 hidden by filters)"))
      .toBeVisible();
    // Deselect all → clears the 2 visible rows only; the hidden DIN3 survives.
    await selectAll.click();
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["DIN1", "DIN2"]);

    // The surviving hidden selection still commits.
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    const committed = onapply.mock.calls[0][0].adds as {
      row: PickerRepresentation;
    }[];
    expect(committed.map((s) => s.row.column)).toEqual(["DIN3"]);
  });

  it("'No columns match' shows when a filter empties the list", async () => {
    render(RepresentationPicker, {
      bands: [multiAxisBand()],
      axes: AXES,
      ...PROPS,
    });
    await expect
      .element(page.getByText("Showing 3 of 3 columns"))
      .toBeVisible();
    // enhet=fam (DIN3) AND hush=h2 (DIN2) is an empty intersection.
    clickFilter("Konsumtionsenhet");
    clickFilter("Familj");
    await expect
      .element(page.getByText("No columns match the active filters."))
      .toBeVisible();
  });

  // C1: a whole-variable faceted member has a null delivery_column, so its facets
  // arrive band-level (the GROUP view sets `band.facets`), NOT keyed by column. The
  // common shape is a month-faceted group: one variable per month, each band carrying
  // its own `month`-axis facet on the whole variable. #908 must still surface the
  // facet filter + per-row markers for these.
  const MONTH_AXES: GroupAxisModel[] = [{ name: "month", label: "Month" }];
  function monthBand(slug: string, col: string, value: string, label: string) {
    return {
      key: `scb/x/${slug}`,
      name: label,
      registerPrefix: "scb/x",
      rows: [row({ column: col, valueSetLabel: "kr" })],
      // Band-level facets — no `facetsByColumn` (the whole-variable shape).
      facets: [{ axis: "month", value, label }],
    } satisfies PickerBand;
  }

  it("band-level facets (whole-variable members) render the facet filter + per-row markers (C1)", async () => {
    render(RepresentationPicker, {
      bands: [
        monthBand("jan", "JAN", "01", "January"),
        monthBand("feb", "FEB", "02", "February"),
      ],
      axes: MONTH_AXES,
      ...PROPS,
    });
    // The month axis discriminates (01/02) → one filter fieldset named "Month".
    await expect
      .element(page.getByRole("group", { name: /Filter columns/ }))
      .toBeVisible();
    const legends = [...document.querySelectorAll(".dim-filter legend")].map(
      (l) => l.textContent?.trim(),
    );
    expect(legends).toEqual(["Month"]);
    // Each row shows its band-level facet as a per-row marker (the fallback path).
    const markers = [
      ...document.querySelectorAll(".col-row .facet-markers"),
    ].map((m) => m.textContent);
    expect(markers.join(" ")).toContain("January");
    expect(markers.join(" ")).toContain("February");

    // Filtering by the band-level facet narrows the list.
    expect(visibleColumns()).toEqual(["JAN", "FEB"]);
    clickFilter("February");
    await expect
      .element(page.getByText("Showing 1 of 2 columns"))
      .toBeVisible();
    expect(visibleColumns()).toEqual(["FEB"]);
  });

  it("suppresses repeated operational definitions and constant coding context when facets distinguish rows (#959)", async () => {
    const axes: GroupAxisModel[] = [{ name: "rank", label: "Rank" }];
    render(RepresentationPicker, {
      bands: [
        {
          key: "scb/lisa/agi1faman",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens största förvärvskälla under året.",
          rows: [row({ column: "AGI1FAMAN", valueSetLabel: "Förekomst" })],
          facets: [{ axis: "rank", value: "1", label: "Största" }],
        },
        {
          key: "scb/lisa/agi2faman",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens näst största förvärvskälla under året.",
          rows: [row({ column: "AGI2FAMAN", valueSetLabel: "Förekomst" })],
          facets: [{ axis: "rank", value: "2", label: "Näst största" }],
        },
        {
          key: "scb/lisa/agi3faman",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens tredje största förvärvskälla under året.",
          rows: [row({ column: "AGI3FAMAN", valueSetLabel: "Förekomst" })],
          facets: [{ axis: "rank", value: "3", label: "Tredje största" }],
        },
      ],
      axes,
      ...PROPS,
    });

    await vi.waitFor(() => {
      const text = document.body.textContent ?? "";
      expect(text).toContain("Största");
      expect(text).toContain("Näst största");
      expect(text).toContain("Tredje största");
    });
    expect(document.body.textContent).not.toContain("Variabeln anger");
    expect(document.body.textContent).not.toContain("op def");
    expect(document.body.textContent).not.toContain("Förekomst");
  });

  it("keeps a unique operational definition when no facet axis carries the distinction (#959)", async () => {
    render(RepresentationPicker, {
      bands: [
        {
          key: "scb/x/owner",
          name: "Näringsgren",
          registerPrefix: "scb/x",
          operationalDefinition: "Owner industry at the end of the year.",
          rows: [row({ column: "SNI_OWNER" })],
        },
        {
          key: "scb/x/previous-owner",
          name: "Näringsgren",
          registerPrefix: "scb/x",
          operationalDefinition:
            "Previous owner industry at the end of the year.",
          rows: [row({ column: "SNI_PREV" })],
        },
      ],
      axes: [],
      ...PROPS,
    });

    await vi.waitFor(() => {
      const lines = [
        ...document.querySelectorAll<HTMLElement>(".op-def-text"),
      ].map((el) => el.textContent);
      expect(lines).toEqual([
        "Owner industry at the end of the year.",
        "Previous owner industry at the end of the year.",
      ]);
    });
    expect(document.body.textContent).not.toContain("op def");
  });
});

describe("codingsVaryNudge deep link (#905)", () => {
  /** Render one band whose single coding-varying row carries the nudge, then return
   * the nudge anchor's `href`. */
  async function nudgeHref(band: PickerBand): Promise<string> {
    render(RepresentationPicker, { bands: [band], axes: [], ...PROPS });
    const nudge = await vi.waitFor(() => {
      const a = document.querySelector(".rep-picker .codings-vary");
      if (!a) {
        throw new Error("codings-vary nudge not yet rendered");
      }
      return a;
    });
    expect(nudge.tagName).toBe("A");
    return nudge.getAttribute("href") ?? "";
  }

  // The leaf branch (band.href undefined → current path) is covered in
  // BindingLeafView.browser.test.ts. This covers the GROUP branch: when a band
  // carries `href` (the member's own leaf), the nudge links to that leaf with the
  // `codes` param carrying the ROW's `(variant, column)` identity — `variant::column`
  // (#905), merged into the href's existing query.
  it("a coding-varying row links to the member leaf with a (variant, column) ?codes deep link", async () => {
    const href = await nudgeHref({
      key: "scb/lisa/yrkesreg",
      name: "Yrkesregistret",
      registerPrefix: "scb/lisa",
      href: "/catalog/scb/lisa/yrkesreg",
      rows: [row({ variant: "individer", column: "Yrke", codingsVary: true })],
    } satisfies PickerBand);
    // The member leaf + `codes=<variant>::<column>` + states hash — clean query, the
    // row's variant carried so a shared column isolates THIS variant's coding.
    expect(href).toBe(
      "/catalog/scb/lisa/yrkesreg?codes=individer%3A%3AYrke#states-heading",
    );
  });

  // The nudge means "this column's coding changed OVER TIME — see the value sets",
  // which is inherently a FULL-HISTORY inspection (#905, Codex P2). So when `band.href`
  // carries an active `?period`, the nudge DROPS it (taking only the member path) and
  // emits a CLEAN `?codes=…` query — a period-narrowed leaf, focused via `?codes`, must
  // show the column's full coding history, not the period-scoped subset.
  it("drops an inherited ?period and emits a clean ?codes-only deep link", async () => {
    const href = await nudgeHref({
      key: "scb/lisa/yrkesreg",
      name: "Yrkesregistret",
      registerPrefix: "scb/lisa",
      href: "/catalog/scb/lisa/yrkesreg?period=2020",
      rows: [row({ variant: "v", column: "Yrke", codingsVary: true })],
    } satisfies PickerBand);
    // No `period` survives; the only param is the (variant, column) composite.
    expect(href).toBe(
      "/catalog/scb/lisa/yrkesreg?codes=v%3A%3AYrke#states-heading",
    );
    expect(href).not.toContain("period");
    // Exactly ONE `?` (no double query separator).
    expect(href.match(/\?/g)).toHaveLength(1);
  });
});

describe("RepresentationPicker sequential-rename hint (#902)", () => {
  // The picker collapses a variable's sequential column RENAME (non-overlapping eras,
  // distinct names) into ONE row led by the latest column, surfacing the earlier name(s)
  // as a quiet inline ".rename-hint" ("was OldCol"). This is normally produced by
  // pickerRepresentations; here we feed the collapsed `renamedColumns` directly to test
  // the render path (the `{@render renameHint(...)}` snippet) in isolation.
  it("renders a 'was <old>' hint for a collapsed rename, and none when empty", async () => {
    render(RepresentationPicker, {
      bands: [
        {
          key: "scb/x/renamed",
          name: "Renamed",
          registerPrefix: "scb/x",
          rows: [row({ column: "NewCol", renamedColumns: ["OldCol"] })],
        } satisfies PickerBand,
        {
          key: "scb/x/plain",
          name: "Plain",
          registerPrefix: "scb/x",
          rows: [row({ column: "PlainCol", renamedColumns: [] })],
        } satisfies PickerBand,
      ],
      axes: [],
      ...PROPS,
    });
    await vi.waitFor(() => {
      if (document.querySelectorAll(".col-row .col-chip").length < 2) {
        throw new Error("rows not rendered yet");
      }
    });

    // The renamed band shows exactly one ".rename-hint" naming the earlier column.
    const hints = [...document.querySelectorAll(".rename-hint")];
    expect(hints).toHaveLength(1);
    expect(hints[0].textContent?.trim()).toBe("was OldCol");

    // The plain band's row carries NO ".rename-hint".
    const plainRow = [...document.querySelectorAll(".col-row")].find((r) =>
      r.textContent?.includes("PlainCol"),
    );
    expect(plainRow?.querySelector(".rename-hint")).toBeNull();
  });
});
