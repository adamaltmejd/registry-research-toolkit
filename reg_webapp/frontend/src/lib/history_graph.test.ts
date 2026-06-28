import { describe, expect, it } from "vitest";
import type {
  ClassificationGraphNode,
  GraphState,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import {
  axisTicks,
  cellsOf,
  clampCellsToScale,
  clustersOf,
  resolveEdges,
  type YearScale,
  yearScaleOf,
} from "./history_graph";

function state(over: Partial<GraphState> = {}): GraphState {
  return {
    state_id: 1,
    variant: "v",
    variant_label: null,
    representation_run_id: 1,
    valid_from: "2010-01-01",
    valid_to: "2010-12-31",
    value_set_id: null,
    value_set_version_label: "",
    classification_slug: null,
    delivery_column_name: "Col",
    ...over,
  };
}

function variableNode(
  over: Partial<VariableGraphNode> = {},
): VariableGraphNode {
  return {
    kind: "variable",
    id: "v1",
    fqid: "scb/lisa/kon",
    label: "Kön",
    group_key: null,
    group_label: null,
    definition: null,
    description: null,
    facets: [],
    states: [],
    same_as: [],
    ...over,
  };
}

function classificationNode(
  over: Partial<ClassificationGraphNode> = {},
): ClassificationGraphNode {
  return {
    kind: "classification",
    id: "c1",
    fqid: "class/sun2020",
    label: "SUN 2020",
    group_key: "sun",
    version_year: 2020,
    is_current: true,
    ...over,
  };
}

describe("cellsOf — representation-run grouping", () => {
  it("fuses consecutive states sharing a run into ONE cell, spanning their window", () => {
    const node = variableNode({
      states: [
        state({
          state_id: 1,
          representation_run_id: 1,
          value_set_version_label: "1-siffrig",
          valid_from: "2010-01-01",
          valid_to: "2010-12-31",
        }),
        state({
          state_id: 2,
          representation_run_id: 1,
          value_set_version_label: "1-siffrig",
          valid_from: "2011-01-01",
          valid_to: "2011-12-31",
        }),
      ],
    });
    const cells = cellsOf(node);
    expect(cells).toHaveLength(1);
    expect(cells[0].label).toBe("1-siffrig");
    expect(cells[0].window).toBe("2010 – 2011");
  });

  it("opens a NEW cell at each representation_run_id change (2 runs → 2 cells)", () => {
    const node = variableNode({
      states: [
        state({ representation_run_id: 1, value_set_version_label: "a" }),
        state({ representation_run_id: 2, value_set_version_label: "b" }),
      ],
    });
    expect(cellsOf(node).map((c) => c.runId)).toEqual([1, 2]);
  });

  it("labels by classification slug, then delivery column, when no version label", () => {
    const slug = cellsOf(
      variableNode({
        states: [
          state({
            value_set_version_label: "",
            classification_slug: "sun2020",
          }),
        ],
      }),
    );
    expect(slug[0].label).toBe("sun2020");

    const col = cellsOf(
      variableNode({
        states: [
          state({
            value_set_version_label: "",
            classification_slug: null,
            delivery_column_name: "Kon",
          }),
        ],
      }),
    );
    expect(col[0].label).toBe("Kon");
  });

  it("normalizes null bounds to the open-ended / yearless sentinels", () => {
    // null valid_to → open-ended ("since"), null valid_from → yearless ("until").
    const open = cellsOf(
      variableNode({
        states: [state({ valid_from: "2012-01-01", valid_to: null })],
      }),
    );
    expect(open[0].window).toBe("since 2012");
    const yearless = cellsOf(
      variableNode({
        states: [state({ valid_from: null, valid_to: "2008-12-31" })],
      }),
    );
    expect(yearless[0].window).toBe("until 2008");
  });

  it("carries numeric year bounds + open flags for the shared axis", () => {
    const cells = cellsOf(
      variableNode({
        states: [
          state({
            representation_run_id: 1,
            valid_from: "1995-01-01",
            valid_to: "2004-12-31",
          }),
          state({
            representation_run_id: 2,
            valid_from: "2005-01-01",
            valid_to: null,
          }),
        ],
      }),
    );
    // A closed run → finite from/to years, neither side open.
    expect(cells[0]).toMatchObject({
      fromYear: 1995,
      toYear: 2004,
      openStart: false,
      openEnd: false,
      row: 0,
    });
    // An open-ended run → openEnd set, toYear NaN (resolved later by the scale).
    expect(cells[1].openEnd).toBe(true);
    expect(Number.isNaN(cells[1].toYear)).toBe(true);
  });
});

describe("yearScaleOf — shared time axis (#678 rework)", () => {
  it("spans the min/max finite year over cells AND classification version_years", () => {
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: "1998-01-01", valid_to: "2004-12-31" })],
    });
    const c = classificationNode({ id: "c", version_year: 2020 });
    const scale = yearScaleOf({ nodes: [v, c], edges: [], focus_id: null });
    expect(scale).toEqual({
      minYear: 1998,
      maxYear: 2020,
      ceilingFromVintage: false,
    });
  });

  it("extends the ceiling to the catalog vintage for an open-ended cell only", () => {
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: "2010-01-01", valid_to: null })], // open-ended
    });
    const scale = yearScaleOf({ nodes: [v], edges: [], focus_id: null }, 2024);
    // Open-ended → vintage extends the max; the finite start anchors the min.
    expect(scale).toMatchObject({ minYear: 2010, maxYear: 2024 });
    expect(scale?.ceilingFromVintage).toBe(true);
  });

  it("does NOT let the vintage shrink a finite max, and ignores it when no cell is open", () => {
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: "2000-01-01", valid_to: "2018-12-31" })],
    });
    // A vintage past the finite max but no open-ended cell → max stays finite.
    const scale = yearScaleOf({ nodes: [v], edges: [], focus_id: null }, 2024);
    expect(scale).toMatchObject({ maxYear: 2018, ceilingFromVintage: false });
  });

  it("widens a single-year graph so the scale has non-zero width", () => {
    const c = classificationNode({ id: "c", version_year: 2000 });
    const scale = yearScaleOf({ nodes: [c], edges: [], focus_id: null });
    expect(scale).toMatchObject({ minYear: 2000, maxYear: 2001 });
  });

  it("returns null when NO node is datable (every bound open/unknown, no year)", () => {
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: null, valid_to: null })],
    });
    const undated = classificationNode({ id: "c", version_year: null });
    expect(
      yearScaleOf({ nodes: [v, undated], edges: [], focus_id: null }),
    ).toBeNull();
  });

  it("renders an axis for a one-sided finite window: unknown start, finite end", () => {
    // Every cell is `valid_from: null` (unknown start) but ends at a known year. A
    // single finite bound seeds BOTH ends of `noteYear`, so the graph is datable
    // (NOT the axis-less fallback): the degenerate widening gives it scale width
    // and the finite year is anchored. (Regression guard for #794 P2's concern —
    // the one-sided finite case already yields a usable axis.)
    const v = variableNode({
      id: "v",
      states: [state({ valid_from: null, valid_to: "2008-12-31" })],
    });
    const scale = yearScaleOf({ nodes: [v], edges: [], focus_id: null });
    expect(scale).not.toBeNull();
    expect(scale?.ceilingFromVintage).toBe(false);
    expect(Number.isFinite(scale?.minYear)).toBe(true);
    expect(Number.isFinite(scale?.maxYear)).toBe(true);
    // The finite endpoint (2008) is on the axis, and there ARE year ticks.
    const ticks = axisTicks(scale as YearScale);
    expect(ticks.length).toBeGreaterThan(0);
    expect(ticks.some((t) => t.year === 2008)).toBe(true);
  });

  it("renders an axis for a multi-cell one-sided finite window (all unknown starts)", () => {
    // Two cells, both unknown-start but finite-end (2005, 2008). The finite ends
    // span the scale — still datable, still an axis.
    const v = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 0,
          valid_from: null,
          valid_to: "2005-12-31",
        }),
        state({
          representation_run_id: 1,
          value_set_version_label: "next",
          valid_from: null,
          valid_to: "2008-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [v], edges: [], focus_id: null });
    expect(scale).toMatchObject({
      minYear: 2005,
      maxYear: 2008,
      ceilingFromVintage: false,
    });
  });
});

describe("clampCellsToScale", () => {
  it("resolves open/unknown bounds to the scale ends, leaving finite bounds", () => {
    const cells = cellsOf(
      variableNode({
        states: [state({ valid_from: null, valid_to: null })], // both open
      }),
    );
    const clamped = clampCellsToScale(cells, {
      minYear: 1990,
      maxYear: 2024,
      ceilingFromVintage: true,
    });
    expect(clamped[0]).toMatchObject({ fromYear: 1990, toYear: 2024 });
    // The open flags survive (the renderer fades those edges).
    expect(clamped[0].openStart).toBe(true);
    expect(clamped[0].openEnd).toBe(true);
  });
});

describe("axisTicks", () => {
  it("emits round, anchored year ticks across the scale", () => {
    const ticks = axisTicks({
      minYear: 1996,
      maxYear: 2020,
      ceilingFromVintage: false,
    });
    const years = ticks.map((t) => t.year);
    // Domain ends are anchored; an interior decade tick is present; sorted.
    expect(years[0]).toBe(1996);
    expect(years.at(-1)).toBe(2020);
    expect(years).toContain(2000);
    expect([...years].sort((a, b) => a - b)).toEqual(years);
  });
});

describe("clustersOf — sub-row packing on the shared axis", () => {
  it("packs time-overlapping cells onto distinct sub-rows (rowCount > 1)", () => {
    // Two runs whose windows OVERLAP (co-existing variants) must not collide.
    const node = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 1,
          variant: "a",
          valid_from: "2000-01-01",
          valid_to: "2010-12-31",
        }),
        state({
          representation_run_id: 2,
          variant: "b",
          valid_from: "2005-01-01",
          valid_to: "2015-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [node], edges: [], focus_id: null });
    const clusters = clustersOf(
      { nodes: [node], edges: [], focus_id: null },
      scale,
    );
    const lane = clusters[0].nodes[0];
    expect(lane.kind).toBe("variable");
    if (lane.kind === "variable") {
      expect(lane.rowCount).toBe(2);
      expect(lane.cells.map((c) => c.row)).toEqual([0, 1]);
    }
  });

  it("packs cells whose RENDERED widths overlap to separate rows (min-width footprint)", () => {
    // #794 P3: two SHORT runs only ~1 raw year apart (one ending 2010, the next
    // starting 2011) don't overlap in raw years — but the renderer floors each cell
    // to CELL_MIN_W (~3.4 years wide at PX_PER_YEAR), so they paint overlapping.
    // Packing must respect the rendered footprint and push them to distinct rows.
    const node = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 1,
          value_set_version_label: "a",
          valid_from: "2009-01-01",
          valid_to: "2010-12-31",
        }),
        state({
          representation_run_id: 2,
          value_set_version_label: "b",
          valid_from: "2011-01-01",
          valid_to: "2012-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [node], edges: [], focus_id: null });
    const lane = clustersOf(
      { nodes: [node], edges: [], focus_id: null },
      scale,
    )[0].nodes[0];
    expect(lane.kind).toBe("variable");
    if (lane.kind === "variable") {
      // The first cell's padded footprint (2009 + ~3.4 ≈ 2012.4) covers 2011, so the
      // second can't share row 0.
      expect(lane.rowCount).toBe(2);
      expect(lane.cells.map((c) => c.row)).toEqual([0, 1]);
    }
  });

  it("does NOT over-split a short cell whose footprint clears the next cell's start", () => {
    // Guard against the min-width fix splitting genuinely non-overlapping cells: a
    // single-year run at 2000 (footprint ends ~2003.4) and the next starting 2005
    // clear each other → one row. (The first cell ends 2000 but its ~3.4-year
    // footprint still ends before 2005.)
    const node = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 1,
          value_set_version_label: "a",
          valid_from: "2000-01-01",
          valid_to: "2000-12-31",
        }),
        state({
          representation_run_id: 2,
          value_set_version_label: "b",
          valid_from: "2005-01-01",
          valid_to: "2005-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [node], edges: [], focus_id: null });
    const lane = clustersOf(
      { nodes: [node], edges: [], focus_id: null },
      scale,
    )[0].nodes[0];
    if (lane.kind === "variable") {
      expect(lane.rowCount).toBe(1);
      expect(lane.cells.every((c) => c.row === 0)).toBe(true);
    }
  });

  it("keeps non-overlapping cells on one row (rowCount === 1)", () => {
    const node = variableNode({
      id: "v",
      states: [
        state({
          representation_run_id: 1,
          valid_from: "2000-01-01",
          valid_to: "2004-12-31",
        }),
        state({
          representation_run_id: 2,
          valid_from: "2010-01-01",
          valid_to: "2014-12-31",
        }),
      ],
    });
    const scale = yearScaleOf({ nodes: [node], edges: [], focus_id: null });
    const lane = clustersOf(
      { nodes: [node], edges: [], focus_id: null },
      scale,
    )[0].nodes[0];
    if (lane.kind === "variable") {
      expect(lane.rowCount).toBe(1);
      expect(lane.cells.every((c) => c.row === 0)).toBe(true);
    }
  });
});

describe("clustersOf — group_key clustering (Fork B)", () => {
  it("clusters nodes sharing a group_key under one cluster with the group_label", () => {
    const a = variableNode({
      id: "a",
      group_key: "g",
      group_label: "Group G",
    });
    const b = variableNode({ id: "b", group_key: "g", group_label: "Group G" });
    const clusters = clustersOf({ nodes: [a, b], edges: [], focus_id: null });
    expect(clusters).toHaveLength(1);
    expect(clusters[0].label).toBe("Group G");
    expect(clusters[0].nodes.map((n) => n.node.id)).toEqual(["a", "b"]);
  });

  it("titles a classification umbrella cluster by its group_label heading (#794 P3)", () => {
    // Classification umbrella members now carry `group_label` (the curated group's
    // display label) — the cluster heading must use it (not stay null), so a
    // SUN/related-granularities umbrella reads under a real title.
    const a = classificationNode({
      id: "c1",
      group_key: "class/sun",
      group_label: "SUN — Svensk utbildningsnomenklatur",
      version_year: 2000,
    });
    const b = classificationNode({
      id: "c2",
      group_key: "class/sun",
      group_label: "SUN — Svensk utbildningsnomenklatur",
      version_year: 2020,
    });
    const clusters = clustersOf({ nodes: [a, b], edges: [], focus_id: null });
    expect(clusters).toHaveLength(1);
    expect(clusters[0].label).toBe("SUN — Svensk utbildningsnomenklatur");
  });

  it("leaves a headless classification cluster (member carries no group_label) null", () => {
    // A non-member spine edition pulled in by the chain walk carries no group_label
    // → the cluster heading stays null (no spurious title).
    const a = classificationNode({
      id: "c1",
      group_key: null,
      group_label: null,
      version_year: 2000,
    });
    const clusters = clustersOf({ nodes: [a], edges: [], focus_id: null });
    expect(clusters[0].label).toBeNull();
  });

  it("gives each null-group_key node its OWN singleton cluster (no heading)", () => {
    const a = variableNode({ id: "a", group_key: null });
    const b = variableNode({ id: "b", group_key: null });
    const clusters = clustersOf({ nodes: [a, b], edges: [], focus_id: null });
    expect(clusters).toHaveLength(2);
    expect(clusters.every((c) => c.label === null)).toBe(true);
  });

  it("orders a cluster's classification members by version_year, oldest first", () => {
    const newer = classificationNode({ id: "c2", version_year: 2020 });
    const older = classificationNode({ id: "c1", version_year: 2000 });
    // Pass newest-first; expect ordered oldest-first.
    const clusters = clustersOf({
      nodes: [newer, older],
      edges: [],
      focus_id: null,
    });
    // Both share group_key "sun" → one cluster, ordered by year.
    expect(clusters[0].nodes.map((n) => n.node.id)).toEqual(["c1", "c2"]);
  });
});

describe("resolveEdges", () => {
  it("resolves edge ids to nodes and drops edges with a missing endpoint", () => {
    const a = variableNode({ id: "a" });
    const b = variableNode({ id: "b" });
    const graph: RelationshipGraph = {
      nodes: [a, b],
      edges: [
        { id: "e1", kind: "succession", source: "a", target: "b", label: null },
        // dangling: "z" isn't a node → dropped.
        { id: "e2", kind: "succession", source: "a", target: "z", label: null },
      ],
      focus_id: "a",
    };
    const resolved = resolveEdges(graph);
    expect(resolved).toHaveLength(1);
    expect(resolved[0].source.id).toBe("a");
    expect(resolved[0].target.id).toBe("b");
  });
});
