import { describe, expect, it } from "vitest";
import type {
  BindingNodeData,
  ClassificationNodeData,
  ConceptGroupNodeData,
  VariableStateModel,
} from "./api";
import {
  hasRenderableHistoryGraph,
  historyGraphFromBinding,
  historyGraphFromClassification,
  historyGraphFromClassificationGroup,
  historyGraphFromGroup,
  historyGraphYears,
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
    expect(graph.nodes[0].label).toBe("agi1lonfink");
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
    expect(graph.nodes.map((node) => [node.id, node.label])).toEqual([
      ["scb/lisa/old", "old"],
      ["scb/lisa/current", "current"],
      ["scb/lisa/sibling", "sibling"],
      ["scb/rtb/source", "source"],
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

  it("uses the resolved same_as target as the current graph id", () => {
    const graph = historyGraphFromBinding(
      binding({
        fqid: "scb/lisa/income-alias",
        via_same_as: ["scb/rams/income"],
        states: [
          state({
            state_id: 10,
            delivery_column_name: "INK",
            valid_from: "2010-01-01",
          }),
        ],
        succession_chain: [
          {
            provider: "scb",
            register: "rams",
            variable: "old-income",
            fqid: "scb/rams/old-income",
            name: "Old income",
            effective_year: 2010,
            reason: "renamed",
            is_self: false,
            is_current: false,
          },
          {
            provider: "scb",
            register: "rams",
            variable: "income",
            fqid: "scb/rams/income",
            name: "Income",
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
            valid_to: "9999-12-31",
          },
        ],
      }),
    );

    expect(graph.nodes.map((node) => node.id)).not.toContain(
      "scb/lisa/income-alias",
    );
    expect(graph.nodes.find((node) => node.self)?.id).toBe("scb/rams/income");
    expect(graph.edges).toContainEqual(
      expect.objectContaining({
        kind: "related",
        from: "scb/rams/income",
        to: "scb/lisa/sibling",
      }),
    );
    expect(graph.edges).toContainEqual(
      expect.objectContaining({
        kind: "lineage",
        from: "scb/rtb/source",
        to: "scb/rams/income",
      }),
    );
  });

  it("preserves unknown starts when mixed with finite state windows", () => {
    const graph = historyGraphFromBinding(
      binding({
        states: [
          state({
            state_id: 1,
            delivery_column_name: "VALUE",
            valid_from: "0001-01-01",
            valid_to: "2010-12-31",
          }),
          state({
            state_id: 2,
            delivery_column_name: "VALUE",
            valid_from: "2011-01-01",
            valid_to: "9999-12-31",
          }),
        ],
      }),
    );

    expect(graph.nodes[0]).toMatchObject({ from: null, to: null });
    expect(graph.nodes[0].columns).toMatchObject([
      { label: "VALUE", from: null, to: null, stateIds: [1, 2] },
    ]);
  });

  it("preserves gaps between reused delivery-column windows", () => {
    const graph = historyGraphFromBinding(
      binding({
        states: [
          state({
            state_id: 1,
            delivery_column_name: "VALUE",
            valid_from: "2000-01-01",
            valid_to: "2005-12-31",
          }),
          state({
            state_id: 2,
            delivery_column_name: "VALUE",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
          }),
        ],
      }),
    );

    expect(graph.nodes[0].columns).toMatchObject([
      { label: "VALUE", from: 2000, to: 2005, stateIds: [1] },
      { label: "VALUE", from: 2010, to: 2015, stateIds: [2] },
    ]);
  });

  it("does not treat type-only state splits as renderable graph history", () => {
    const graph = historyGraphFromBinding(
      binding({
        fqid: "scb/lisa/akters",
        states: [
          state({
            state_id: 54412,
            delivery_column_name: "AktErs",
            valid_from: "2010-01-01",
            valid_to: "2015-12-31",
            data_type: "int",
          }),
          state({
            state_id: 54413,
            delivery_column_name: "AktErs",
            valid_from: "2016-01-01",
            valid_to: "2023-12-31",
            data_type: "bigint",
          }),
        ],
      }),
    );

    expect(graph.nodes[0].columns).toMatchObject([
      { label: "AktErs", from: 2010, to: 2023, stateIds: [54412, 54413] },
    ]);
    expect(hasRenderableHistoryGraph(graph)).toBe(false);
  });

  it("treats sequential column renames as renderable single-variable history", () => {
    const graph = historyGraphFromBinding(
      binding({
        states: [
          state({
            state_id: 1,
            delivery_column_name: "OLD_VALUE",
            valid_from: "2000-01-01",
            valid_to: "2010-12-31",
          }),
          state({
            state_id: 2,
            delivery_column_name: "NEW_VALUE",
            valid_from: "2011-01-01",
            valid_to: "9999-12-31",
          }),
        ],
      }),
    );

    expect(graph.nodes[0].columns.map((column) => column.label)).toEqual([
      "OLD_VALUE",
      "NEW_VALUE",
    ]);
    expect(hasRenderableHistoryGraph(graph)).toBe(true);
  });

  it("treats same-column value-set changes as renderable single-variable history", () => {
    const graph = historyGraphFromBinding(
      binding({
        states: [
          state({
            state_id: 1,
            delivery_column_name: "KOD",
            valid_from: "2000-01-01",
            valid_to: "2010-12-31",
            value_set_id: 10,
            value_set_version_label: "Old codes",
          }),
          state({
            state_id: 2,
            delivery_column_name: "KOD",
            valid_from: "2011-01-01",
            valid_to: "9999-12-31",
            value_set_id: 20,
            value_set_version_label: "New codes",
          }),
        ],
      }),
    );

    expect(
      graph.nodes[0].columns.map((column) => [
        column.label,
        column.valueSetKey,
        column.valueSetLabel,
      ]),
    ).toEqual([
      ["KOD", "id/10", "Old codes"],
      ["KOD", "id/20", "New codes"],
    ]);
    expect(hasRenderableHistoryGraph(graph)).toBe(true);
  });

  it("extends open-ended variable domains through the current year", () => {
    const graph = historyGraphFromBinding(
      binding({
        states: [
          state({
            state_id: 1,
            valid_from: "2019-01-01",
            valid_to: "9999-12-31",
          }),
        ],
      }),
    );

    const domain = historyGraphYears(graph);

    expect(domain.min).toBe(2019);
    expect(domain.max).toBeGreaterThanOrEqual(new Date().getFullYear());
  });

  it("caps open-ended variable domains at the catalog vintage when provided", () => {
    const graph = historyGraphFromBinding(
      binding({
        states: [
          state({
            state_id: 1,
            valid_from: "2019-01-01",
            valid_to: "9999-12-31",
          }),
        ],
      }),
    );

    const domain = historyGraphYears(graph, 2024);

    expect(domain).toEqual({ min: 2019, max: 2024 });
  });

  it("shows group members without inventing a group entity node", () => {
    const graph = historyGraphFromGroup(
      {
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
      } as ConceptGroupNodeData,
      "scb/lisa/agi02",
    );

    expect(graph.nodes.map((node) => node.kind)).toEqual([
      "group-member",
      "group-member",
    ]);
    expect(graph.title).toBe("Variable relationships");
    expect(
      graph.nodes.map((node) => [node.id, node.label, node.detail]),
    ).toEqual([
      ["scb/lisa/agi01", "January", undefined],
      ["scb/lisa/agi02", "February", undefined],
    ]);
    expect(graph.nodes.map((node) => [node.id, node.self])).toEqual([
      ["scb/lisa/agi01", false],
      ["scb/lisa/agi02", true],
    ]);
    expect(graph.nodes.map((node) => node.id)).not.toContain(
      "group:agi-months",
    );
    expect(graph.edges).toEqual([]);
    expect(graph.warnings.join("\n")).toContain("member leaves");
  });

  it("omits the synthetic group node from shallow classification group graphs", () => {
    const graph = historyGraphFromGroup({
      kind: "concept-group",
      key: "sun",
      label: "Svensk utbildningsnomenklatur (SUN)",
      provider: "class",
      register: null,
      source: "curated",
      axes: ["dimension"],
      member: null,
      members: [
        {
          fqid: "class/sun2020-inriktning",
          name: "Utbildningsinriktning",
          facets: [
            {
              axis: "dimension",
              value: "inriktning",
              label: "Inriktning",
            },
          ],
          coverage: null,
        },
        {
          fqid: "class/niva-grovv1",
          name: "Utbildningsnivå, grov",
          facets: [
            {
              axis: "dimension",
              value: "niva-grov",
              label: "Aggregat",
            },
          ],
          coverage: null,
        },
      ],
    } as ConceptGroupNodeData);

    expect(graph.mode).toBe("classification");
    expect(graph.title).toBe("Classification relationships");
    expect(graph.nodes.map((node) => [node.id, node.kind, node.label])).toEqual(
      [
        ["class/sun2020-inriktning", "classification", "sun2020-inriktning"],
        ["class/niva-grovv1", "classification", "niva-grovv1"],
      ],
    );
    expect(graph.nodes.map((node) => node.id)).not.toContain("group:sun");
    expect(graph.edges).toEqual([]);
  });

  it("merges classification group member histories and succession edges", () => {
    const group = {
      kind: "concept-group",
      key: "sun",
      label: "Svensk utbildningsnomenklatur (SUN)",
      provider: "class",
      register: null,
      source: "curated",
      axes: ["dimension"],
      member: null,
      members: [
        {
          fqid: "class/sun2020-inriktning",
          name: "Utbildningsinriktning",
          facets: [
            {
              axis: "dimension",
              value: "inriktning",
              label: "Inriktning",
            },
          ],
          coverage: null,
        },
        {
          fqid: "class/sun2020-niva",
          name: "Utbildningsnivå",
          facets: [
            {
              axis: "dimension",
              value: "niva",
              label: "Nivå",
            },
          ],
          coverage: null,
        },
        {
          fqid: "class/niva-grovv1",
          name: "Utbildningsnivå, grov",
          facets: [
            {
              axis: "dimension",
              value: "niva-grov",
              label: "Aggregat",
            },
          ],
          coverage: null,
        },
      ],
    } as ConceptGroupNodeData;
    const graph = historyGraphFromClassificationGroup(group, [
      {
        kind: "classification",
        fqid: "class/sun2020-inriktning",
        name: "SUN 2020 — inriktning",
        short_name: "SUN2020-INRIKTNING",
        codes: [],
        dimensions: [],
        edition_chain: [
          {
            slug: "sun1996",
            fqid: "class/sun1996",
            name: "SUN 1996",
            short_name: "SUN1996",
            effective_year: 2000,
            is_self: false,
            is_current: false,
          },
          {
            slug: "sun2000-inriktning",
            fqid: "class/sun2000-inriktning",
            name: "SUN 2000 — inriktning",
            short_name: "SUN2000-INRIKTNING",
            effective_year: 2020,
            is_self: false,
            is_current: false,
          },
          {
            slug: "sun2020-inriktning",
            fqid: "class/sun2020-inriktning",
            name: "SUN 2020 — inriktning",
            short_name: "SUN2020-INRIKTNING",
            effective_year: null,
            is_self: true,
            is_current: true,
          },
        ],
        edition_edges: [
          {
            predecessor_slug: "sun1996",
            predecessor_fqid: "class/sun1996",
            successor_slug: "sun2000-inriktning",
            successor_fqid: "class/sun2000-inriktning",
            effective_year: 2000,
            note: null,
          },
          {
            predecessor_slug: "sun2000-inriktning",
            predecessor_fqid: "class/sun2000-inriktning",
            successor_slug: "sun2020-inriktning",
            successor_fqid: "class/sun2020-inriktning",
            effective_year: 2020,
            note: null,
          },
        ],
      },
      {
        kind: "classification",
        fqid: "class/sun2020-niva",
        name: "SUN 2020 — nivå",
        short_name: "SUN2020-NIVA",
        codes: [],
        dimensions: [],
        edition_chain: [
          {
            slug: "sun1996",
            fqid: "class/sun1996",
            name: "SUN 1996",
            short_name: "SUN1996",
            effective_year: 2000,
            is_self: false,
            is_current: false,
          },
          {
            slug: "sun2000-niva",
            fqid: "class/sun2000-niva",
            name: "SUN 2000 — nivå",
            short_name: "SUN2000-NIVA",
            effective_year: 2020,
            is_self: false,
            is_current: false,
          },
          {
            slug: "sun2020-niva",
            fqid: "class/sun2020-niva",
            name: "SUN 2020 — nivå",
            short_name: "SUN2020-NIVA",
            effective_year: null,
            is_self: true,
            is_current: true,
          },
        ],
        edition_edges: [
          {
            predecessor_slug: "sun1996",
            predecessor_fqid: "class/sun1996",
            successor_slug: "sun2000-niva",
            successor_fqid: "class/sun2000-niva",
            effective_year: 2000,
            note: null,
          },
          {
            predecessor_slug: "sun2000-niva",
            predecessor_fqid: "class/sun2000-niva",
            successor_slug: "sun2020-niva",
            successor_fqid: "class/sun2020-niva",
            effective_year: 2020,
            note: null,
          },
        ],
      },
      {
        kind: "classification",
        fqid: "class/niva-grovv1",
        name: "Utbildningsnivå, grov",
        short_name: "NIVA-GROV",
        codes: [],
        dimensions: [],
        edition_chain: [],
        edition_edges: [],
      },
    ] as ClassificationNodeData[]);

    expect(graph.nodes.map((node) => node.id)).toEqual([
      "class/sun1996",
      "class/sun2000-inriktning",
      "class/sun2020-inriktning",
      "class/sun2000-niva",
      "class/sun2020-niva",
      "class/niva-grovv1",
    ]);
    expect(graph.nodes.map((node) => node.label)).toEqual([
      "SUN1996",
      "SUN2000-INRIKTNING",
      "SUN2020-INRIKTNING",
      "SUN2000-NIVA",
      "SUN2020-NIVA",
      "NIVA-GROV",
    ]);
    expect(graph.nodes.map((node) => node.id)).not.toContain("group:sun");
    expect(graph.edges.map((edge) => [edge.from, edge.to, edge.kind])).toEqual([
      ["class/sun1996", "class/sun2000-inriktning", "succession"],
      ["class/sun2000-inriktning", "class/sun2020-inriktning", "succession"],
      ["class/sun1996", "class/sun2000-niva", "succession"],
      ["class/sun2000-niva", "class/sun2020-niva", "succession"],
    ]);
    expect(graph.nodes.filter((node) => node.self)).toEqual([]);
    expect(
      historyGraphFromClassificationGroup(
        group,
        graph.nodes.map((graphNode) => ({
          kind: "classification",
          fqid: graphNode.id,
          name: graphNode.label,
          short_name: graphNode.label.toUpperCase(),
          codes: [],
          dimensions: [],
          edition_chain: [
            {
              slug: graphNode.label,
              fqid: graphNode.id,
              name: graphNode.label,
              short_name: graphNode.label,
              effective_year: graphNode.from,
              is_self: true,
              is_current: graphNode.current ?? false,
            },
          ],
          edition_edges: [],
        })) as ClassificationNodeData[],
        "class/sun2020-niva",
      )
        .nodes.filter((node) => node.self)
        .map((node) => node.id),
    ).toEqual(["class/sun2020-niva"]);
    expect(graph.warnings).toEqual([]);
  });

  it("can focus a historical classification node inside the shared group graph", () => {
    const group = {
      kind: "concept-group",
      key: "sun",
      label: "Svensk utbildningsnomenklatur",
      provider: "class",
      register: null,
      source: "curated",
      axes: ["dimension"],
      member: null,
      members: [
        {
          fqid: "class/sun-inriktning2020",
          name: "Utbildningsinriktning",
          facets: [
            { axis: "dimension", value: "inriktning", label: "Inriktning" },
          ],
          coverage: null,
        },
      ],
    } as ConceptGroupNodeData;
    const graph = historyGraphFromClassificationGroup(
      group,
      [
        {
          kind: "classification",
          fqid: "class/sun-inriktning2020",
          name: "SUN 2020 — inriktning",
          short_name: "SUN2020-INRIKTNING",
          codes: [],
          dimensions: [],
          edition_chain: [
            {
              slug: "sun-inriktning2000",
              fqid: "class/sun-inriktning2000",
              name: "SUN 2000 — inriktning",
              short_name: "SUN2000-INRIKTNING",
              effective_year: 2020,
              is_self: false,
              is_current: false,
            },
            {
              slug: "sun-inriktning2020",
              fqid: "class/sun-inriktning2020",
              name: "SUN 2020 — inriktning",
              short_name: "SUN2020-INRIKTNING",
              effective_year: null,
              is_self: true,
              is_current: true,
            },
          ],
          edition_edges: [
            {
              predecessor_slug: "sun-inriktning2000",
              predecessor_fqid: "class/sun-inriktning2000",
              successor_slug: "sun-inriktning2020",
              successor_fqid: "class/sun-inriktning2020",
              effective_year: 2020,
              note: null,
            },
          ],
        } as ClassificationNodeData,
      ],
      "class/sun-inriktning2000",
    );

    expect(
      graph.nodes.filter((node) => node.self).map((node) => node.id),
    ).toEqual(["class/sun-inriktning2000"]);
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
          short_name: "SUN2000",
          effective_year: 2020,
          is_self: false,
          is_current: false,
        },
        {
          slug: "sun2020",
          fqid: "class/sun2020",
          name: "SUN 2020",
          short_name: "SUN2020",
          effective_year: null,
          is_self: true,
          is_current: true,
        },
      ],
      edition_edges: [
        {
          predecessor_slug: "sun2000",
          predecessor_fqid: "class/sun2000",
          successor_slug: "sun2020",
          successor_fqid: "class/sun2020",
          effective_year: 2020,
          note: null,
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

  it("keeps plain classification leaf graphs to the leaf's own history", () => {
    const graph = historyGraphFromClassification({
      kind: "classification",
      fqid: "class/niva-grovv1",
      name: "Utbildningsnivå, grov",
      short_name: "NIVA-GROV",
      codes: [],
      dimensions: [
        {
          key: "sun",
          label: "Svensk utbildningsnomenklatur (SUN)",
          source: "curated",
          axes: ["dimension"],
          members: [
            {
              fqid: "class/sun2020-inriktning",
              name: "Utbildningsinriktning",
              facets: [
                {
                  axis: "dimension",
                  value: "inriktning",
                  label: "Inriktning",
                },
              ],
            },
            {
              fqid: "class/niva-grovv1",
              name: "Utbildningsnivå, grov",
              facets: [
                {
                  axis: "dimension",
                  value: "niva-grov",
                  label: "Aggregat",
                },
              ],
            },
          ],
        },
      ],
      edition_chain: [],
      edition_edges: [],
    } as ClassificationNodeData);

    expect(graph.nodes.map((node) => [node.id, node.kind, node.label])).toEqual(
      [["class/niva-grovv1", "classification", "NIVA-GROV"]],
    );
    expect(
      graph.nodes.find((node) => node.id === "class/niva-grovv1"),
    ).toMatchObject({
      self: true,
    });
    expect(graph.edges).toEqual([]);
  });

  it("connects classification fan-out branches without treating them as a timeline", () => {
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
          short_name: "SUN1996",
          effective_year: 2000,
          is_self: true,
          is_current: false,
        },
        {
          slug: "sun-grupp2000",
          fqid: "class/sun-grupp2000",
          name: "Svensk utbildningsnomenklatur 2000 — Utbildningsgrupper",
          short_name: "SUN2000-GRUPP",
          effective_year: 2020,
          is_self: false,
          is_current: false,
        },
        {
          slug: "sun-grupp2020",
          fqid: "class/sun-grupp2020",
          name: "Svensk utbildningsnomenklatur 2020 — Utbildningsgrupper",
          short_name: "SUN2020-GRUPP",
          effective_year: null,
          is_self: false,
          is_current: true,
        },
        {
          slug: "sun-inriktning2000",
          fqid: "class/sun-inriktning2000",
          name: "Svensk utbildningsnomenklatur 2000 — Utbildningsinriktning",
          short_name: "SUN2000-INRIKTNING",
          effective_year: 2020,
          is_self: false,
          is_current: false,
        },
        {
          slug: "sun-inriktning2020",
          fqid: "class/sun-inriktning2020",
          name: "Svensk utbildningsnomenklatur 2020 — Utbildningsinriktning",
          short_name: "SUN2020-INRIKTNING",
          effective_year: null,
          is_self: false,
          is_current: true,
        },
        {
          slug: "sun-niva2000",
          fqid: "class/sun-niva2000",
          name: "Svensk utbildningsnomenklatur 2000 — Utbildningsnivå",
          short_name: "SUN2000-NIVA",
          effective_year: 2020,
          is_self: false,
          is_current: false,
        },
        {
          slug: "sun-niva2020",
          fqid: "class/sun-niva2020",
          name: "Svensk utbildningsnomenklatur 2020 — Utbildningsnivå",
          short_name: "SUN2020-NIVA",
          effective_year: null,
          is_self: false,
          is_current: true,
        },
      ],
      edition_edges: [
        {
          predecessor_slug: "sun1996",
          predecessor_fqid: "class/sun1996",
          successor_slug: "sun2000-grupp",
          successor_fqid: "class/sun2000-grupp",
          effective_year: 2000,
          note: null,
        },
        {
          predecessor_slug: "sun2000-grupp",
          predecessor_fqid: "class/sun2000-grupp",
          successor_slug: "sun2020-grupp",
          successor_fqid: "class/sun2020-grupp",
          effective_year: 2020,
          note: null,
        },
        {
          predecessor_slug: "sun1996",
          predecessor_fqid: "class/sun1996",
          successor_slug: "sun2000-inriktning",
          successor_fqid: "class/sun2000-inriktning",
          effective_year: 2000,
          note: null,
        },
        {
          predecessor_slug: "sun2000-inriktning",
          predecessor_fqid: "class/sun2000-inriktning",
          successor_slug: "sun2020-inriktning",
          successor_fqid: "class/sun2020-inriktning",
          effective_year: 2020,
          note: null,
        },
        {
          predecessor_slug: "sun1996",
          predecessor_fqid: "class/sun1996",
          successor_slug: "sun2000-niva",
          successor_fqid: "class/sun2000-niva",
          effective_year: 2000,
          note: null,
        },
        {
          predecessor_slug: "sun2000-niva",
          predecessor_fqid: "class/sun2000-niva",
          successor_slug: "sun2020-niva",
          successor_fqid: "class/sun2020-niva",
          effective_year: 2020,
          note: null,
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
      "SUN1996",
      "SUN2000-GRUPP",
      "SUN2020-GRUPP",
      "SUN2000-INRIKTNING",
      "SUN2020-INRIKTNING",
      "SUN2000-NIVA",
      "SUN2020-NIVA",
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
    expect(graph.edges.map((edge) => [edge.from, edge.to, edge.kind])).toEqual([
      ["class/sun1996", "class/sun2000-grupp", "succession"],
      ["class/sun2000-grupp", "class/sun2020-grupp", "succession"],
      ["class/sun1996", "class/sun2000-inriktning", "succession"],
      ["class/sun2000-inriktning", "class/sun2020-inriktning", "succession"],
      ["class/sun1996", "class/sun2000-niva", "succession"],
      ["class/sun2000-niva", "class/sun2020-niva", "succession"],
    ]);
    expect(graph.warnings).toEqual([]);
  });
});
