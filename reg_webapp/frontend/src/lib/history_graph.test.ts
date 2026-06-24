import { describe, expect, it } from "vitest";
import type {
  BindingNodeData,
  ClassificationNodeData,
  ConceptGroupNodeData,
  VariableStateModel,
} from "./api";
import {
  historyGraphFromBinding,
  historyGraphFromClassification,
  historyGraphFromGroup,
} from "./history_graph";

function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "_default",
    register_variant_id: 1,
    valid_from: "2000-01-01",
    valid_to: "9999-12-31",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
    ...over,
  };
}

function binding(over: Partial<BindingNodeData>): BindingNodeData {
  return {
    kind: "binding",
    fqid: "scb/lisa/agi1lonfink",
    name: "Income by month",
    definition: null,
    description: null,
    measurement_unit: null,
    is_identifier: false,
    is_sensitive: false,
    register_id: 1,
    source_register_id: null,
    source_register_text: null,
    variable_id: 1,
    states: [],
    same_as: [],
    related_to: [],
    lineage: [],
    succession_chain: [],
    ...over,
  };
}

describe("history graph prototype model", () => {
  it("keeps a monthly family as one variable node with column slices", () => {
    const states = Array.from({ length: 12 }, (_, i) =>
      state({
        state_id: i + 1,
        delivery_column_name: `AGI${String(i + 1).padStart(2, "0")}`,
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
    );

    const graph = historyGraphFromBinding(binding({ states }));

    expect(graph.nodes).toHaveLength(1);
    expect(graph.nodes[0].columns.map((c) => c.label)).toHaveLength(12);
    expect(graph.nodes[0].kind).toBe("variable");
    expect(graph.nodeGrain).toBe("entity-with-column-slices");
    expect(graph.warnings.join("\n")).toContain("column slices");
  });

  it("renders split siblings and lineage while flagging missing client-stitch windows", () => {
    const graph = historyGraphFromBinding(
      binding({
        fqid: "scb/lisa/current",
        states: [
          state({
            state_id: 10,
            delivery_column_name: "CURR",
            valid_from: "2010-01-01",
          }),
        ],
        succession_chain: [
          {
            provider: "scb",
            register: "lisa",
            variable: "old",
            fqid: "scb/lisa/old",
            name: "Old",
            effective_year: 2010,
            reason: "renamed",
            is_self: false,
            is_current: false,
          },
          {
            provider: "scb",
            register: "lisa",
            variable: "current",
            fqid: "scb/lisa/current",
            name: "Current",
            effective_year: null,
            reason: null,
            is_self: true,
            is_current: true,
          },
        ],
        related_to: [
          {
            provider: "scb",
            register: "lisa",
            variable: "sibling",
            fqid: "scb/lisa/sibling",
            relation_kind: "related_to",
          },
        ],
        lineage: [
          {
            source_state_id: 1,
            consumer_state_id: 10,
            source_fqid: "scb/rtb/source",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          },
        ],
      }),
    );

    expect(graph.edges.map((edge) => edge.kind)).toEqual([
      "succession",
      "related",
      "lineage",
    ]);
    expect(graph.nodes.some((node) => node.id === "scb/lisa/sibling")).toBe(
      true,
    );
    expect(graph.nodes.some((node) => node.id === "scb/rtb/source")).toBe(true);
    expect(graph.warnings.join("\n")).toContain(
      "predecessor/successor column windows",
    );
    expect(graph.warnings.join("\n")).toContain(
      "related_to has no validity window",
    );
  });

  it("shows group members and records why group mode wants a backend graph payload", () => {
    const graph = historyGraphFromGroup({
      kind: "concept-group",
      key: "agi-months",
      label: "Monthly income",
      provider: "scb",
      register: "lisa",
      source: "curated",
      axes: ["month"],
      member: null,
      members: [
        {
          fqid: "scb/lisa/agi01",
          name: "Income",
          facets: [{ axis: "month", value: "01", label: "January" }],
          coverage: {
            coverage_from: "2019-01-01",
            coverage_to: "9999-12-31",
            open_ended: true,
            state_count: 1,
          },
        },
        {
          fqid: "scb/lisa/agi02",
          name: "Income",
          facets: [{ axis: "month", value: "02", label: "February" }],
          coverage: {
            coverage_from: "2019-01-01",
            coverage_to: "9999-12-31",
            open_ended: true,
            state_count: 1,
          },
        },
      ],
    } as ConceptGroupNodeData);

    expect(graph.nodes.map((node) => node.kind)).toEqual([
      "group",
      "group-member",
      "group-member",
    ]);
    expect(graph.edges.every((edge) => edge.kind === "member")).toBe(true);
    expect(graph.warnings.join("\n")).toContain("N+1 leaf fetches");
  });

  it("turns classification editions into succession nodes", () => {
    const graph = historyGraphFromClassification({
      kind: "classification",
      fqid: "class/sun2020",
      name: "SUN 2020",
      short_name: "SUN",
      codes: [],
      dimensions: [],
      edition_chain: [
        {
          slug: "sun2000",
          fqid: "class/sun2000",
          name: "SUN 2000",
          effective_year: 2020,
          is_self: false,
          is_current: false,
        },
        {
          slug: "sun2020",
          fqid: "class/sun2020",
          name: "SUN 2020",
          effective_year: null,
          is_self: true,
          is_current: true,
        },
      ],
    } as ClassificationNodeData);

    expect(graph.nodes.map((node) => node.id)).toEqual([
      "class/sun2000",
      "class/sun2020",
    ]);
    expect(graph.nodes.map((node) => [node.id, node.from, node.to])).toEqual([
      ["class/sun2000", 2000, 2000],
      ["class/sun2020", 2020, 2020],
    ]);
    expect(graph.edges).toMatchObject([
      {
        kind: "succession",
        from: "class/sun2000",
        to: "class/sun2020",
        fromYear: 2020,
      },
    ]);
  });

  it("does not invent succession edges for classification fan-out closures", () => {
    const graph = historyGraphFromClassification({
      kind: "classification",
      fqid: "class/sun1996",
      name: "SUN 1996",
      short_name: "SUN",
      codes: [],
      dimensions: [],
      edition_chain: [
        {
          slug: "sun1996",
          fqid: "class/sun1996",
          name: "Svensk utbildningsnomenklatur 1996",
          effective_year: 2000,
          is_self: true,
          is_current: false,
        },
        {
          slug: "sun-grupp2000",
          fqid: "class/sun-grupp2000",
          name: "Svensk utbildningsnomenklatur 2000 — Utbildningsgrupper",
          effective_year: 2020,
          is_self: false,
          is_current: false,
        },
        {
          slug: "sun-grupp2020",
          fqid: "class/sun-grupp2020",
          name: "Svensk utbildningsnomenklatur 2020 — Utbildningsgrupper",
          effective_year: null,
          is_self: false,
          is_current: true,
        },
        {
          slug: "sun-inriktning2000",
          fqid: "class/sun-inriktning2000",
          name: "Svensk utbildningsnomenklatur 2000 — Utbildningsinriktning",
          effective_year: 2020,
          is_self: false,
          is_current: false,
        },
        {
          slug: "sun-inriktning2020",
          fqid: "class/sun-inriktning2020",
          name: "Svensk utbildningsnomenklatur 2020 — Utbildningsinriktning",
          effective_year: null,
          is_self: false,
          is_current: true,
        },
        {
          slug: "sun-niva2000",
          fqid: "class/sun-niva2000",
          name: "Svensk utbildningsnomenklatur 2000 — Utbildningsnivå",
          effective_year: 2020,
          is_self: false,
          is_current: false,
        },
        {
          slug: "sun-niva2020",
          fqid: "class/sun-niva2020",
          name: "Svensk utbildningsnomenklatur 2020 — Utbildningsnivå",
          effective_year: null,
          is_self: false,
          is_current: true,
        },
      ],
    } as ClassificationNodeData);

    expect(graph.nodes.map((node) => node.id)).toEqual([
      "class/sun1996",
      "class/sun-grupp2000",
      "class/sun-grupp2020",
      "class/sun-inriktning2000",
      "class/sun-inriktning2020",
      "class/sun-niva2000",
      "class/sun-niva2020",
    ]);
    expect(graph.nodes.map((node) => node.label)).toEqual([
      "sun1996",
      "sun-grupp2000",
      "sun-grupp2020",
      "sun-inriktning2000",
      "sun-inriktning2020",
      "sun-niva2000",
      "sun-niva2020",
    ]);
    expect(
      graph.nodes.map((node) => ({
        id: node.id,
        from: node.from,
        to: node.to,
      })),
    ).toEqual([
      { id: "class/sun1996", from: 1996, to: 1996 },
      { id: "class/sun-grupp2000", from: 2000, to: 2000 },
      { id: "class/sun-grupp2020", from: 2020, to: 2020 },
      { id: "class/sun-inriktning2000", from: 2000, to: 2000 },
      { id: "class/sun-inriktning2020", from: 2020, to: 2020 },
      { id: "class/sun-niva2000", from: 2000, to: 2000 },
      { id: "class/sun-niva2020", from: 2020, to: 2020 },
    ]);
    expect(graph.edges).toEqual([]);
    expect(graph.warnings.join("\n")).toContain(
      "true branch edges need a backend graph payload",
    );
  });
});
