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
import {
  type PickerRepresentation,
  type PickerStateInput,
  pickerRepresentations,
} from "./catalog";
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

type LisaIndividerVariant = "individer-16plus" | "individer-15plus";

function lisaIndividerState(variant: LisaIndividerVariant): PickerStateInput {
  const predecessor = variant === "individer-16plus";
  return {
    state_id: predecessor ? 1 : 2,
    variant,
    variant_label: predecessor ? "Individer, 16 plus" : "Individer, 15 plus",
    variant_family: "individer-15plus",
    variant_family_label: "Individer",
    delivery_column_name: "Kon",
    value_set_version_label: "",
    value_set_id: null,
    valid_from: predecessor ? "1990-01-01" : "2010-01-01",
    valid_to: predecessor ? "2009-12-31" : "2023-12-31",
  };
}

function lisaNarrowedBand(variant: LisaIndividerVariant): PickerBand {
  return {
    key: "scb/lisa/kon",
    name: "Kon",
    registerPrefix: "scb/lisa",
    rows: pickerRepresentations([lisaIndividerState(variant)]),
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

  it("renders leaf sibling graph context as unavailable cells outside the picker band", async () => {
    const fixture = smallSuccessionFixture();
    render(RepresentationPicker, {
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
    render(RepresentationPicker, {
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
                state_id: 2,
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

  it("keeps empty group bands visible when no graph can render", async () => {
    const aFqid = "scb/lisa/empty-a";
    const bFqid = "scb/lisa/empty-b";
    render(RepresentationPicker, {
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

  it("renders uncolumned leaf graph cells beside selectable cells", async () => {
    const aFqid = "scb/lisa/mixed";
    const bFqid = "scb/lisa/mixed-next";
    render(RepresentationPicker, {
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
                state_id: 2,
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
                state_id: 3,
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

  it("renders edge-less selectable multi-run leaf graphs", async () => {
    const aFqid = "scb/lisa/renamed";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Renamed leaf",
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
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                delivery_column_name: "OLD",
                valid_from: "2000-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "NEW",
                valid_from: "2010-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
        ],
        edges: [],
        focus_id: aFqid,
      }),
      ...PROPS,
    });

    await vi.waitFor(() => {
      const graphText =
        document.querySelector(".graph-picker")?.textContent ?? "";
      if (!graphText.includes("OLD") || !graphText.includes("NEW")) {
        throw new Error(
          `edge-less selectable graph not rendered: ${graphText}`,
        );
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    await expect
      .element(page.getByRole("checkbox", { name: /^OLD\b/ }))
      .toBeVisible();
  });

  it("draws same-variable representation edges between graph cells", async () => {
    const aFqid = "scb/lisa/renamed";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Renamed leaf",
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
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                delivery_column_name: "OLD",
                valid_from: "2000-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "NEW",
                valid_from: "2010-01-01",
                valid_to: "2020-12-31",
              }),
            ],
          }),
        ],
        edges: [
          {
            id: "repr-old-new",
            kind: "succession",
            source: aFqid,
            target: aFqid,
            label: null,
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
      "OLD → NEW · 2010",
    );
  });

  it("draws round-trip representation edges to the resumed later cell", async () => {
    const aFqid = "scb/lisa/roundtrip";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "Roundtrip leaf",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              column: "BorgNr",
              representation: null,
              renamedColumns: ["PersOrgNr"],
              from: "2007-01-01",
              to: "2023-12-31",
              windows: [
                { from: "2007-01-01", to: "2013-12-31" },
                { from: "2014-01-01", to: "2017-12-31" },
                { from: "2018-01-01", to: "2023-12-31" },
              ],
              period: "2007 – 2023",
            }),
          ],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(aFqid, {
            states: [
              graphState({
                delivery_column_name: "BorgNr",
                valid_from: "2007-01-01",
                valid_to: "2013-12-31",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "PersOrgNr",
                valid_from: "2014-01-01",
                valid_to: "2017-12-31",
              }),
              graphState({
                state_id: 3,
                representation_run_id: 3,
                delivery_column_name: "BorgNr",
                valid_from: "2018-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
        ],
        edges: [
          {
            id: "repr-borgnr-persorgnr",
            kind: "succession",
            source: aFqid,
            target: aFqid,
            label: null,
            effective_year: 2014,
            source_column: "BorgNr",
            target_column: "PersOrgNr",
            variant: null,
          },
          {
            id: "repr-persorgnr-borgnr",
            kind: "succession",
            source: aFqid,
            target: aFqid,
            label: null,
            effective_year: 2018,
            source_column: "PersOrgNr",
            target_column: "BorgNr",
            variant: null,
          },
        ],
        focus_id: aFqid,
      }),
      ...PROPS,
    });

    const line = await vi.waitFor(() => {
      const edge = document.querySelector<SVGLineElement>(
        '.graph-edge.representation[data-edge-id="repr-persorgnr-borgnr"]',
      );
      if (!edge) {
        throw new Error("round-trip representation edge not rendered");
      }
      return edge;
    });
    const x1 = Number(line.getAttribute("x1"));
    const x2 = Number(line.getAttribute("x2"));
    expect(x2).toBeGreaterThan(x1);
    expect(document.querySelector(".graph-picker")?.textContent).toContain(
      "PersOrgNr → BorgNr · 2018",
    );
    const labelPositions = await vi.waitFor(() => {
      const labels = [
        ...document.querySelectorAll<HTMLElement>(".graph-reason"),
      ].filter(
        (label) =>
          label.textContent?.includes("BorgNr → PersOrgNr · 2014") ||
          label.textContent?.includes("PersOrgNr → BorgNr · 2018"),
      );
      if (labels.length !== 2) {
        throw new Error("round-trip labels not rendered");
      }
      return labels.map((label) => `${label.style.left}:${label.style.top}`);
    });
    expect(new Set(labelPositions).size).toBe(2);
  });

  it("marks dead renamed predecessor lanes with the leaf slug and renamed hint", async () => {
    const liveFqid = "scb/lisa/sni2007";
    const deadFqid = "scb/lisa/sni92";
    render(RepresentationPicker, {
      bands: [
        {
          key: liveFqid,
          name: "Näringsgren (SNI 2007)",
          registerPrefix: "scb/lisa",
          rows: [row({ column: "SNI2007" })],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(liveFqid, {
            id: "live",
            fqid: liveFqid,
            label: "Näringsgren (SNI 2007)",
            group_key: null,
            group_label: null,
            states: [graphState({ delivery_column_name: "SNI2007" })],
          }),
          graphNode(deadFqid, {
            id: "dead",
            fqid: deadFqid,
            label: "Näringsgren (SNI 92)",
            group_key: null,
            group_label: null,
            states: [],
          }),
        ],
        edges: [
          {
            id: "dead->live",
            kind: "succession",
            source: "dead",
            target: "live",
            label: null,
          },
        ],
        focus_id: "live",
      }),
      ...PROPS,
    });

    const renamedLink = await vi.waitFor(() => {
      const el = [...document.querySelectorAll(".graph-name")].find(
        (node) =>
          (node as HTMLAnchorElement).getAttribute("href") ===
          "/catalog/scb/lisa/sni92",
      );
      if (!el) {
        throw new Error("renamed predecessor link not rendered");
      }
      return el as HTMLAnchorElement;
    });
    expect(renamedLink.textContent?.replace(/\s+/g, " ").trim()).toContain(
      "sni92 (renamed)",
    );
    expect(
      renamedLink.closest(".graph-lane")?.classList.contains("muted"),
    ).toBe(true);
    expect(document.body.textContent ?? "").toContain(
      "renamed predecessor with no live states",
    );
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

  it("labels a graph-matched folded variant-family row with its full family period", async () => {
    const predecessorFqid = "scb/lisa/kon-old";
    const currentFqid = "scb/lisa/kon";
    render(RepresentationPicker, {
      bands: [
        {
          key: currentFqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              key: "individer-15plus::Kon",
              variant: "individer-15plus",
              variantLabel: "Individer, 15 år och äldre",
              variantFamily: "individer-15plus",
              variantFamilyLabel: "Individer",
              column: "Kon",
              from: "1990-01-01",
              to: "2023-12-31",
              windows: [
                { from: "1990-01-01", to: "2009-12-31" },
                { from: "2010-01-01", to: "2023-12-31" },
              ],
              variantSegments: [
                {
                  variant: "individer-16plus",
                  variantLabel: "Individer, 16 år och äldre",
                  windows: [{ from: "1990-01-01", to: "2009-12-31" }],
                },
                {
                  variant: "individer-15plus",
                  variantLabel: "Individer, 15 år och äldre",
                  windows: [{ from: "2010-01-01", to: "2023-12-31" }],
                },
              ],
              period: "1990 – 2023",
              wirePeriod: "1990..2009,2010..2023",
            }),
          ],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(predecessorFqid, {
            label: "Kön old",
            states: [
              graphState({
                variant: "individer-16plus",
                delivery_column_name: "Kon",
                valid_from: "1990-01-01",
                valid_to: "2009-12-31",
              }),
            ],
          }),
          graphNode(currentFqid, {
            label: "Kön",
            states: [
              graphState({
                variant: "individer-15plus",
                delivery_column_name: "Kon",
                valid_from: "2010-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(predecessorFqid, currentFqid)],
        focus_id: currentFqid,
      }),
      ...PROPS,
    });

    await expect
      .element(page.getByRole("checkbox", { name: /Kon.*1990.*2023/ }))
      .toBeVisible();
    const matchedCell = await vi.waitFor(() => {
      const cell = [
        ...document.querySelectorAll<HTMLLabelElement>("label.graph-cell"),
      ].find((el) => el.textContent?.includes("Kon"));
      if (!cell) {
        throw new Error("matched graph cell not rendered");
      }
      return cell;
    });
    expect(matchedCell.textContent).toContain("1990 – 2023");
    expect(matchedCell.textContent).not.toContain("2010 – 2023");
  });

  it("matches every concrete variant graph cell for a folded family row", async () => {
    const onapply = vi.fn();
    const fqid = "scb/lisa/kon";
    render(RepresentationPicker, {
      bands: [
        {
          key: fqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          rows: [
            row({
              key: "individer-15plus::Kon",
              variant: "individer-15plus",
              variantLabel: "Individer, 15 år och äldre",
              variantFamily: "individer-15plus",
              variantFamilyLabel: "Individer",
              column: "Kon",
              from: "1990-01-01",
              to: "2023-12-31",
              windows: [
                { from: "1990-01-01", to: "2009-12-31" },
                { from: "2010-01-01", to: "2023-12-31" },
              ],
              variantSegments: [
                {
                  variant: "individer-16plus",
                  variantLabel: "Individer, 16 år och äldre",
                  windows: [{ from: "1990-01-01", to: "2009-12-31" }],
                },
                {
                  variant: "individer-15plus",
                  variantLabel: "Individer, 15 år och äldre",
                  windows: [{ from: "2010-01-01", to: "2023-12-31" }],
                },
              ],
              period: "1990 – 2023",
              wirePeriod: "1990..2009,2010..2023",
            }),
          ],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(fqid, {
            states: [
              graphState({
                variant: "individer-16plus",
                representation_run_id: 1,
                delivery_column_name: "Kon",
                valid_from: "1990-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: 2,
                variant: "individer-15plus",
                representation_run_id: 2,
                delivery_column_name: "Kon",
                valid_from: "2010-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
        ],
        edges: [],
        focus_id: fqid,
      }),
      ...PROPS,
      onapply,
    });

    const matchedCells = await vi.waitFor(() => {
      const cells = [
        ...document.querySelectorAll<HTMLLabelElement>("label.graph-cell"),
      ].filter((el) => el.textContent?.includes("1990 – 2023"));
      if (cells.length !== 2) {
        throw new Error(
          `expected two matched graph cells, got ${cells.length}`,
        );
      }
      return cells;
    });
    for (const cell of matchedCells) {
      expect(cell.textContent).toContain("1990 – 2023");
    }

    matchedCells[0].querySelector("input")?.click();

    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    expect(onapply).toHaveBeenCalledTimes(1);
    expect(onapply.mock.calls[0][0].adds[0].row.column).toBe("Kon");
  });

  it("keeps predecessor variant cells in a folded family group graph projection", async () => {
    const familyFqid = "scb/lisa/kon";
    const successorFqid = "scb/lisa/kon-next";
    render(RepresentationPicker, {
      bands: [
        {
          key: familyFqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/kon",
          rows: [
            row({
              key: "individer-15plus::Kon",
              variant: "individer-15plus",
              variantLabel: "Individer, 15 år och äldre",
              variantFamily: "individer-15plus",
              variantFamilyLabel: "Individer",
              column: "Kon",
              from: "1990-01-01",
              to: "2023-12-31",
              windows: [
                { from: "1990-01-01", to: "2009-12-31" },
                { from: "2010-01-01", to: "2023-12-31" },
              ],
              variantSegments: [
                {
                  variant: "individer-16plus",
                  variantLabel: "Individer, 16 år och äldre",
                  windows: [{ from: "1990-01-01", to: "2009-12-31" }],
                },
                {
                  variant: "individer-15plus",
                  variantLabel: "Individer, 15 år och äldre",
                  windows: [{ from: "2010-01-01", to: "2023-12-31" }],
                },
              ],
              period: "1990 – 2023",
              wirePeriod: "1990..2009,2010..2023",
            }),
          ],
        } satisfies PickerBand,
        {
          key: successorFqid,
          name: "Successor",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/kon-next",
          rows: [row({ column: "Kon2" })],
        } satisfies PickerBand,
      ],
      graphMemberHrefs: {
        [familyFqid]: "/catalog/scb/lisa/kon",
        [successorFqid]: "/catalog/scb/lisa/kon-next",
      },
      graph: graph({
        nodes: [
          graphNode(familyFqid, {
            states: [
              graphState({
                variant: "individer-16plus",
                representation_run_id: 1,
                delivery_column_name: "Kon",
                valid_from: "1990-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: 2,
                variant: "individer-15plus",
                representation_run_id: 2,
                delivery_column_name: "Kon",
                valid_from: "2010-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
          graphNode(successorFqid, {
            states: [
              graphState({
                state_id: 3,
                representation_run_id: 3,
                delivery_column_name: "Kon2",
                valid_from: "2024-01-01",
                valid_to: "9999-12-31",
              }),
            ],
          }),
        ],
        edges: [edge(familyFqid, successorFqid)],
        focus_id: familyFqid,
      }),
      ...PROPS,
    });

    const matchedCells = await vi.waitFor(() => {
      const cells = [
        ...document.querySelectorAll<HTMLLabelElement>("label.graph-cell"),
      ].filter((el) => el.textContent?.includes("1990 – 2023"));
      if (cells.length !== 2) {
        throw new Error(
          `expected two folded-family graph cells, got ${cells.length}`,
        );
      }
      return cells;
    });
    for (const cell of matchedCells) {
      expect(cell.textContent).toContain("1990 – 2023");
    }
    expect(document.querySelectorAll(".graph-edge")).toHaveLength(1);
    expect(document.querySelector(".col-list")).toBeNull();
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

  it("falls back when a graph node carries a separate non-member column run", async () => {
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
                valid_from: "1990-01-01",
                valid_to: "1999-12-31",
              }),
              graphState({
                state_id: 2,
                representation_run_id: 2,
                delivery_column_name: "MEMBER",
                valid_from: "2000-01-01",
                valid_to: "2020-12-31",
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

    await vi.waitFor(() => {
      if (!document.querySelector(".col-list")) {
        throw new Error("list fallback not rendered");
      }
    });
    expect(document.querySelector(".graph-picker")).toBeNull();
    expect(visibleColumns()).toEqual(["MEMBER", "NEXT"]);
    expect(document.body.textContent).not.toContain("HIDDEN");
  });

  it("falls back when a group graph includes a non-member variable node", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    const outsideFqid = "scb/lisa/outside";
    render(RepresentationPicker, {
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

  it("keeps graph mode while hiding graph members removed by filters", async () => {
    const aFqid = "scb/lisa/a";
    const bFqid = "scb/lisa/b";
    render(RepresentationPicker, {
      bands: [
        {
          key: aFqid,
          name: "A",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/a",
          rows: [row({ column: "Acol" })],
          facetsByColumn: {
            Acol: [{ axis: "era", value: "old", label: "Old" }],
          },
        } satisfies PickerBand,
        {
          key: bFqid,
          name: "B",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/b",
          rows: [row({ column: "Bcol" })],
          facetsByColumn: {
            Bcol: [{ axis: "era", value: "new", label: "New" }],
          },
        } satisfies PickerBand,
      ],
      axes: [{ name: "era", label: "Era" }],
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
        ],
        edges: [edge(aFqid, bFqid)],
        focus_id: null,
      }),
      ...PROPS,
      focusKey: bFqid,
    });

    await vi.waitFor(() => {
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph picker not rendered");
      }
    });

    clickFilter("Old");
    await vi.waitFor(() => {
      const graphText =
        document.querySelector(".graph-picker")?.textContent ?? "";
      if (!graphText.includes("Acol") || graphText.includes("Bcol")) {
        throw new Error(`filtered graph not ready: ${graphText}`);
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    expect(document.querySelector(".graph-picker")).not.toBeNull();
    await expect
      .element(page.getByText("Showing 1 of 2 columns"))
      .toBeVisible();
  });

  it("uses the visible graph projection for size limits after filtering", async () => {
    const nodes = Array.from({ length: 19 }, (_, i) => {
      const fqid = `scb/lisa/v${i}`;
      return graphNode(fqid, {
        states: [
          graphState({
            state_id: i + 1,
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

    render(RepresentationPicker, {
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
    render(RepresentationPicker, {
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
                state_id: i + 1,
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
                state_id: 100,
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

  it("attributes a folded FAMILY cell's codings-vary link to its concrete era variant", async () => {
    // #376 regression: a folded variant-family row folds TWO concrete variants
    // (individer-16plus predecessor era + individer-15plus successor era) delivering
    // one column `Kon`. Each era renders its own graph cell (carrying its concrete
    // `cell.variant`). The codings-vary deep link must target the cell's CONCRETE
    // variant, NOT the head `row.variant` — else the predecessor cell would link to a
    // `(individer-15plus, Kon)` coding the successor never delivered in that era.
    const fqid = "scb/lisa/kon";
    render(RepresentationPicker, {
      bands: [
        {
          key: fqid,
          name: "Kön",
          registerPrefix: "scb/lisa",
          href: "/catalog/scb/lisa/kon",
          rows: [
            row({
              key: "individer-15plus{individer-16plus,individer-15plus}::Kon",
              variant: "individer-15plus",
              variantLabel: "Individer, 15 år och äldre",
              variantFamily: "individer-15plus",
              variantFamilyLabel: "Individer",
              column: "Kon",
              codingsVary: true,
              from: "1990-01-01",
              to: "2023-12-31",
              windows: [
                { from: "1990-01-01", to: "2009-12-31" },
                { from: "2010-01-01", to: "2023-12-31" },
              ],
              variantSegments: [
                {
                  variant: "individer-16plus",
                  variantLabel: "Individer, 16 år och äldre",
                  windows: [{ from: "1990-01-01", to: "2009-12-31" }],
                },
                {
                  variant: "individer-15plus",
                  variantLabel: "Individer, 15 år och äldre",
                  windows: [{ from: "2010-01-01", to: "2023-12-31" }],
                },
              ],
              period: "1990 – 2023",
              wirePeriod: "1990..2009,2010..2023",
            }),
          ],
        } satisfies PickerBand,
      ],
      graph: graph({
        nodes: [
          graphNode(fqid, {
            states: [
              graphState({
                state_id: 1,
                variant: "individer-16plus",
                representation_run_id: 1,
                delivery_column_name: "Kon",
                value_set_id: 10,
                value_set_version_label: "16+ coding",
                valid_from: "1990-01-01",
                valid_to: "2009-12-31",
              }),
              graphState({
                state_id: 2,
                variant: "individer-15plus",
                representation_run_id: 2,
                delivery_column_name: "Kon",
                value_set_id: 20,
                value_set_version_label: "15+ coding",
                valid_from: "2010-01-01",
                valid_to: "2023-12-31",
              }),
            ],
          }),
        ],
        edges: [],
        focus_id: fqid,
      }),
      ...PROPS,
    });

    const hrefs = await vi.waitFor(() => {
      const found = [
        ...document.querySelectorAll<HTMLLabelElement>("label.graph-cell"),
      ]
        .map((c) =>
          c
            .querySelector<HTMLAnchorElement>(".codings-vary")
            ?.getAttribute("href"),
        )
        .filter((h): h is string => h != null);
      if (found.length < 2) {
        throw new Error("both era coding links not yet rendered");
      }
      return found;
    });
    // Each era's link carries ITS OWN concrete variant, never both collapsed to the head.
    expect(hrefs).toContain(
      "/catalog/scb/lisa/kon?codes=individer-16plus%3A%3AKon#states-heading",
    );
    expect(hrefs).toContain(
      "/catalog/scb/lisa/kon?codes=individer-15plus%3A%3AKon#states-heading",
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

  it("keeps #908 dimension filters above graph mode", async () => {
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
    expect(document.querySelector(".graph-picker")).not.toBeNull();
    expect(document.querySelector(".col-list")).toBeNull();
    await expect
      .element(page.getByText("Showing 3 of 3 columns"))
      .toBeVisible();

    clickFilter("Familj");
    await expect
      .element(page.getByText("Showing 1 of 3 columns"))
      .toBeVisible();
    expect(document.querySelector(".graph-picker")).not.toBeNull();
    const graphText =
      document.querySelector(".graph-picker")?.textContent ?? "";
    expect(graphText).toContain("DIN2");
    expect(graphText).not.toContain("DIN1");
    expect(graphText).not.toContain("DIN3");
  });

  it("renders graph mode when a declared #908 facet axis has only one value", async () => {
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
      if (!document.querySelector(".graph-picker")) {
        throw new Error("graph picker not rendered");
      }
    });
    expect(document.querySelector(".col-list")).toBeNull();
    expect(document.body.textContent).toContain("Enhet");
    expect(document.body.textContent).toContain("Individ");
  });

  it("falls back when narrow selectable graph cells carry inline context", async () => {
    const aFqid = "scb/forskola/sun2000inr-prio";
    const bFqid = "scb/forskola/sun2000inr-next";
    render(RepresentationPicker, {
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
                state_id: 2,
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

  it("stages selectable superseded predecessor rows from the history disclosure (#926)", async () => {
    const onapply = vi.fn();
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst familj",
      registerPrefix: "scb/iot",
      rows: [
        row({
          column: "DINFold",
          from: "1999-01-01",
          to: "2004-12-31",
          windows: [{ from: "1999-01-01", to: "2004-12-31" }],
          period: "1999 – 2004",
          wirePeriod: "1999..2004",
        }),
      ],
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst familj 2004",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew" })],
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;

    render(RepresentationPicker, {
      bands: [successor],
      ...PROPS,
      onapply,
    });

    await page.getByText("supersedes 1 edition").click();
    await page.getByRole("checkbox", { name: /DINFold/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(onapply).toHaveBeenCalledTimes(1);
    const payload = onapply.mock.calls[0][0];
    expect(payload.adds).toHaveLength(1);
    expect(payload.adds[0].band.key).toBe("scb/iot/dispink-old");
    expect(payload.adds[0].row.column).toBe("DINFold");
  });

  it("deduplicates one folded predecessor shared by split successors (#926)", async () => {
    const onapply = vi.fn();
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
    } satisfies PickerBand;
    const successors = ["new-a", "new-b"].map(
      (slug) =>
        ({
          key: `scb/iot/dispink-${slug}`,
          name: `Disponibel inkomst ${slug}`,
          registerPrefix: "scb/iot",
          rows: [row({ column: `DINF${slug}` })],
          supersedes: [
            {
              name: predecessor.name,
              href: "/catalog/scb/iot/dispink-old",
              effectiveYear: 2005,
              band: predecessor,
            },
          ],
        }) satisfies PickerBand,
    );

    render(RepresentationPicker, {
      bands: successors,
      ...PROPS,
      onapply,
    });

    const summary = document.querySelector<HTMLElement>(
      "details.history summary",
    );
    if (!summary) {
      throw new Error("history disclosure not rendered");
    }
    summary.click();
    await page.getByRole("checkbox", { name: /DINFold/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page.getByRole("button", { name: "Add to project" }).click();

    expect(onapply).toHaveBeenCalledTimes(1);
    const payload = onapply.mock.calls[0][0];
    expect(payload.adds).toHaveLength(1);
    expect(payload.adds[0].band.key).toBe("scb/iot/dispink-old");
    expect(payload.adds[0].row.column).toBe("DINFold");
  });

  it("applies filters and hidden-counts to folded history rows (#926)", async () => {
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
      facetsByColumn: {
        DINFold: [{ axis: "era", value: "old", label: "Old level" }],
      },
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst new",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew" })],
      facetsByColumn: {
        DINFnew: [{ axis: "era", value: "new", label: "New level" }],
      },
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;

    render(RepresentationPicker, {
      bands: [successor],
      axes: [{ name: "era", label: "Era" }],
      ...PROPS,
    });

    await page.getByText("supersedes 1 edition").click();
    await page.getByRole("checkbox", { name: /DINFold/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();

    clickFilter("Old level");
    await expect
      .element(page.getByText("Showing 1 of 2 columns"))
      .toBeVisible();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .not.toBeInTheDocument();
    const details =
      document.querySelector<HTMLDetailsElement>("details.history");
    if (!details) {
      throw new Error("history disclosure not rendered");
    }
    details.open = true;
    await expect
      .element(page.getByRole("checkbox", { name: /DINFold/ }))
      .toBeVisible();

    await page.getByRole("button", { name: "Clear filters" }).click();
    clickFilter("New level");
    await expect
      .element(page.getByText("+1 column (1 hidden by filters)"))
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: /DINFold/ }))
      .not.toBeInTheDocument();
  });

  it("includes visible folded history rows in global select-all (#926)", async () => {
    const onapply = vi.fn();
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst new",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew" })],
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;
    const sibling = {
      key: "scb/iot/dispink-other",
      name: "Disponibel inkomst other",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFother" })],
    } satisfies PickerBand;

    render(RepresentationPicker, {
      bands: [successor, sibling],
      ...PROPS,
      onapply,
    });

    await page.getByRole("checkbox", { name: "Select all columns" }).click();
    await expect.element(page.getByText("+3 columns")).toBeVisible();

    const details =
      document.querySelector<HTMLDetailsElement>("details.history");
    if (!details) {
      throw new Error("history disclosure not rendered");
    }
    details.open = true;
    await expect
      .element(page.getByRole("checkbox", { name: /DINFold/ }))
      .toBeChecked();

    await page.getByRole("button", { name: "Add to project" }).click();
    expect(onapply).toHaveBeenCalledTimes(1);
    const addedColumns = onapply.mock.calls[0][0].adds.map(
      (selection: { row: PickerRepresentation }) => selection.row.column,
    );
    expect(addedColumns.sort()).toEqual(["DINFnew", "DINFold", "DINFother"]);
  });

  it("shows global select-all for one successor plus one folded predecessor (#926)", async () => {
    const onapply = vi.fn();
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst new",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew" })],
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;

    render(RepresentationPicker, {
      bands: [successor],
      ...PROPS,
      onapply,
    });

    await page.getByRole("checkbox", { name: "Select all columns" }).click();
    await expect.element(page.getByText("+2 columns")).toBeVisible();

    const details =
      document.querySelector<HTMLDetailsElement>("details.history");
    if (!details) {
      throw new Error("history disclosure not rendered");
    }
    details.open = true;
    await expect
      .element(page.getByRole("checkbox", { name: /DINFold/ }))
      .toBeChecked();

    await page.getByRole("button", { name: "Add to project" }).click();
    expect(onapply).toHaveBeenCalledTimes(1);
    const addedColumns = onapply.mock.calls[0][0].adds.map(
      (selection: { row: PickerRepresentation }) => selection.row.column,
    );
    expect(addedColumns.sort()).toEqual(["DINFnew", "DINFold"]);
  });

  it("hides global select-all when filters leave one folded family band visible (#926)", async () => {
    const predecessor = {
      key: "scb/iot/dispink-old",
      name: "Disponibel inkomst old",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFold" })],
      facetsByColumn: {
        DINFold: [{ axis: "era", value: "old", label: "Old level" }],
      },
    } satisfies PickerBand;
    const successor = {
      key: "scb/iot/dispink-new",
      name: "Disponibel inkomst new",
      registerPrefix: "scb/iot",
      rows: [row({ column: "DINFnew1" }), row({ column: "DINFnew2" })],
      facetsByColumn: {
        DINFnew1: [{ axis: "era", value: "new", label: "New level" }],
        DINFnew2: [{ axis: "era", value: "new", label: "New level" }],
      },
      supersedes: [
        {
          name: predecessor.name,
          href: "/catalog/scb/iot/dispink-old",
          effectiveYear: 2005,
          band: predecessor,
        },
      ],
    } satisfies PickerBand;

    render(RepresentationPicker, {
      bands: [successor],
      axes: [{ name: "era", label: "Era" }],
      ...PROPS,
    });

    await vi.waitFor(() => {
      if (
        !document.querySelector(
          '.select-all-row input[aria-label="Select all columns"]',
        )
      ) {
        throw new Error("global select-all not rendered");
      }
    });
    clickFilter("New level");
    await expect
      .element(page.getByText("Showing 2 of 3 columns"))
      .toBeVisible();
    expect(
      document.querySelector(
        '.select-all-row input[aria-label="Select all columns"]',
      ),
    ).toBeNull();
  });

  it("clears staged adds when a narrowed folded variant changes concrete segment", async () => {
    const onapply = vi.fn();
    const { rerender } = render(RepresentationPicker, {
      bands: [lisaNarrowedBand("individer-16plus")],
      ...PROPS,
      onapply,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();

    await rerender({
      bands: [lisaNarrowedBand("individer-15plus")],
      ...PROPS,
      onapply,
    });

    await expect.element(page.getByText("+1 column")).not.toBeInTheDocument();
    await expect
      .element(
        page.getByRole("button", {
          name: /Add to project|Remove from project|Apply changes/,
        }),
      )
      .not.toBeInTheDocument();
    expect(onapply).not.toHaveBeenCalled();
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

  it("keeps operational definitions when only a majority share an axis-carried stem (#959)", async () => {
    const axes: GroupAxisModel[] = [{ name: "rank", label: "Rank" }];
    render(RepresentationPicker, {
      bands: [
        {
          key: "scb/lisa/first",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens första förvärvskälla under året.",
          rows: [row({ column: "FIRST" })],
          facets: [{ axis: "rank", value: "1", label: "Första" }],
        },
        {
          key: "scb/lisa/second",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Variabeln anger familjens andra förvärvskälla under året.",
          rows: [row({ column: "SECOND" })],
          facets: [{ axis: "rank", value: "2", label: "Andra" }],
        },
        {
          key: "scb/lisa/manual",
          name: "Förvärvskälla",
          registerPrefix: "scb/lisa",
          operationalDefinition:
            "Manually curated source classification for special cases.",
          rows: [row({ column: "MANUAL" })],
          facets: [{ axis: "rank", value: "x", label: "Special" }],
        },
      ],
      axes,
      ...PROPS,
    });

    await vi.waitFor(() => {
      const text = document.body.textContent ?? "";
      expect(text).toContain(
        "Variabeln anger familjens första förvärvskälla under året.",
      );
      expect(text).toContain(
        "Variabeln anger familjens andra förvärvskälla under året.",
      );
      expect(text).toContain(
        "Manually curated source classification for special cases.",
      );
    });
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
