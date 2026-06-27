import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ClassificationGraphNode,
  GraphEdge,
  GraphState,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import HistoryGraph from "./HistoryGraph.svelte";

// HistoryGraph (#678) renders the relationship-graph contract (#761/#792) as SVG +
// a structured screen-reader fallback. These cover the contract behaviours: the
// empty "don't render" signal, representation-run cells per variable, classification
// version ordering + is_current, succession/related edges with labels, focus
// highlight, and the same_as "also delivered in" affordance. We assert against the
// VISUALLY-HIDDEN structured fallback (real text the SVG mirrors), which the a11y
// queries reach regardless of the SVG geometry.

function state(over: Partial<GraphState> = {}): GraphState {
  return {
    state_id: 1,
    variant: "v",
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
    facets: [],
    states: [state()],
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

function graph(over: Partial<RelationshipGraph> = {}): RelationshipGraph {
  return { nodes: [], edges: [], focus_id: null, ...over };
}

describe("HistoryGraph (#678)", () => {
  it("renders NOTHING for an empty graph (the 'don't render' signal)", async () => {
    render(HistoryGraph, { graph: graph({ nodes: [] }) });
    await expect
      .element(page.getByRole("heading", { name: "History" }))
      .not.toBeInTheDocument();
  });

  it("draws a shared TIME AXIS with year ticks spanning the graph (#678 rework)", async () => {
    // Two variables spanning 1990–2009 and 2010–2023 → one axis covering both,
    // with year ticks (the headline of the rework: cells lay on a shared scale).
    const a = variableNode({
      id: "a",
      fqid: "scb/lisa/a",
      label: "A",
      states: [state({ valid_from: "1990-01-01", valid_to: "2009-12-31" })],
    });
    const b = variableNode({
      id: "b",
      fqid: "scb/lisa/b",
      label: "B",
      states: [
        state({
          representation_run_id: 2,
          valid_from: "2010-01-01",
          valid_to: "2023-12-31",
        }),
      ],
    });
    render(HistoryGraph, { graph: graph({ nodes: [a, b], focus_id: "a" }) });

    // The axis is decorative to AT (aria-hidden), so query the rendered DOM. It
    // emits labelled year ticks with the domain ends (1990, 2023) anchored and an
    // interior decade tick (2000) present.
    const tickYears = await vi.waitFor(() => {
      const ticks = [...document.querySelectorAll(".tick")];
      if (ticks.length === 0) {
        throw new Error("axis ticks not yet rendered");
      }
      return ticks.map((t) => t.textContent?.trim() ?? "");
    });
    expect(tickYears).toContain("1990");
    expect(tickYears).toContain("2000");
    expect(tickYears).toContain("2023");
    // Cells are absolutely positioned on the track (a left offset in px), proving
    // they're laid on the scale rather than stacked at a fixed origin.
    const cells = [...document.querySelectorAll(".cell")];
    expect(cells.length).toBe(2);
    expect(
      cells.every((c) => /left:\s*\d/.test((c as HTMLElement).style.cssText)),
    ).toBe(true);
  });

  it("renders one cell per representation_run_id (2 runs → 2 cells) with each window", async () => {
    // Two consecutive states share run 1 (one cell, fused window); a third opens
    // run 2 (a second cell). The fallback list mirrors one <li> per cell.
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
        state({
          state_id: 3,
          representation_run_id: 2,
          value_set_version_label: "2-siffrig",
          valid_from: "2012-01-01",
          valid_to: "9999-12-31",
        }),
      ],
    });
    render(HistoryGraph, { graph: graph({ nodes: [node], focus_id: "v1" }) });

    // Run 1's label + fused window (2010–2011) and run 2's open-ended window.
    await expect.element(page.getByText("1-siffrig").first()).toBeVisible();
    await expect.element(page.getByText("2-siffrig").first()).toBeVisible();
    await expect.element(page.getByText("2010 – 2011").first()).toBeVisible();
    await expect.element(page.getByText("since 2012").first()).toBeVisible();
  });

  it("labels a cell by classification slug / delivery column when no version label", async () => {
    const node = variableNode({
      states: [
        state({
          representation_run_id: 1,
          value_set_version_label: "",
          classification_slug: "sun2020",
        }),
      ],
    });
    render(HistoryGraph, { graph: graph({ nodes: [node], focus_id: "v1" }) });
    await expect.element(page.getByText("sun2020").first()).toBeVisible();
  });

  it("renders classification nodes in version_year order with is_current marked", async () => {
    const older = classificationNode({
      id: "c1",
      fqid: "class/sun2000",
      label: "SUN 2000",
      version_year: 2000,
      is_current: false,
    });
    const newer = classificationNode({
      id: "c2",
      fqid: "class/sun2020",
      label: "SUN 2020",
      version_year: 2020,
      is_current: true,
    });
    // Pass newest-first to prove the renderer ORDERS by version_year.
    render(HistoryGraph, {
      graph: graph({ nodes: [newer, older], focus_id: "c2" }),
    });

    const fallback = document.querySelector(".graph-fallback");
    const text = fallback?.textContent ?? "";
    // The older edition appears before the newer in the ordered fallback.
    expect(text.indexOf("SUN 2000")).toBeLessThan(text.indexOf("SUN 2020"));
    // The current/head edition is marked.
    await expect.element(page.getByText("current edition")).toBeVisible();
  });

  it("renders succession (directed) and related (undirected) edges with labels", async () => {
    const a = variableNode({ id: "a", fqid: "scb/lisa/a", label: "A" });
    const b = variableNode({ id: "b", fqid: "scb/lisa/b", label: "B" });
    const c = variableNode({ id: "c", fqid: "scb/lisa/c", label: "C" });
    const edges: GraphEdge[] = [
      {
        id: "e1",
        kind: "succession",
        source: "a",
        target: "b",
        label: "Definition change",
      },
      {
        id: "e2",
        kind: "related",
        source: "b",
        target: "c",
        label: "code_vs_label_pair",
      },
    ];
    render(HistoryGraph, {
      graph: graph({ nodes: [a, b, c], edges, focus_id: "a" }),
    });

    // The fallback lists each edge with its label + the directional glyph. The
    // edge text spans several text nodes (whitespace-separated), so assert against
    // each `.fb-edge`'s normalized textContent rather than a single text node.
    const edgeTexts = await vi.waitFor(() => {
      const els = [...document.querySelectorAll(".fb-edge")];
      if (els.length < 2) {
        throw new Error("edges not yet rendered");
      }
      return els.map((el) => el.textContent?.replace(/\s+/g, " ").trim() ?? "");
    });
    // Succession is directed (→) with its reason label; related is undirected (↔)
    // with its relation_kind label.
    expect(edgeTexts.some((t) => /▸ A → B \(Definition change\)/.test(t))).toBe(
      true,
    );
    expect(
      edgeTexts.some((t) => /↔ B ↔ C \(code_vs_label_pair\)/.test(t)),
    ).toBe(true);
  });

  it("shows a related edge's relation_kind in a VISIBLE chip, not only the a11y fallback (#678 P2)", async () => {
    // The retired Related section showed `relation_kind` to sighted users; the new
    // dashed bow must not bury it in the sr-only fallback. A visible `.reason.related`
    // chip (outside the visually-hidden `.graph-fallback`) carries it.
    const a = variableNode({ id: "a", fqid: "scb/lisa/a", label: "A" });
    const b = variableNode({ id: "b", fqid: "scb/lisa/b", label: "B" });
    render(HistoryGraph, {
      graph: graph({
        nodes: [a, b],
        edges: [
          {
            id: "e1",
            kind: "related",
            source: "a",
            target: "b",
            label: "code_vs_label_pair",
          },
        ],
        focus_id: "a",
      }),
    });
    const chip = await vi.waitFor(() => {
      const el = document.querySelector(".reason.related");
      if (!el) {
        throw new Error("related reason chip not yet rendered");
      }
      return el;
    });
    // The chip carries the relation_kind text and is NOT inside the sr-only fallback.
    expect(chip.textContent?.replace(/\s+/g, " ").trim()).toContain(
      "code_vs_label_pair",
    );
    expect(chip.closest(".graph-fallback")).toBeNull();
  });

  it("highlights the focus node ('this variable')", async () => {
    const focus = variableNode({
      id: "v1",
      fqid: "scb/lisa/kon",
      label: "Kön",
    });
    const other = variableNode({
      id: "v2",
      fqid: "scb/lisa/alder",
      label: "Ålder",
    });
    render(HistoryGraph, {
      graph: graph({ nodes: [focus, other], focus_id: "v1" }),
    });
    await expect.element(page.getByText("this variable")).toBeVisible();
  });

  it("marks the focus classification edition ('this edition')", async () => {
    const node = classificationNode({ id: "c1", is_current: false });
    render(HistoryGraph, { graph: graph({ nodes: [node], focus_id: "c1" }) });
    await expect.element(page.getByText("this edition")).toBeVisible();
  });

  it("surfaces same_as as an 'also delivered in {register}' affordance (NOT an edge)", async () => {
    const node = variableNode({
      id: "v1",
      same_as: [
        { fqid: "scb/rams/kon", register: "rams" },
        { fqid: "scb/par/kon", register: "par" },
      ],
    });
    render(HistoryGraph, { graph: graph({ nodes: [node], focus_id: "v1" }) });

    // The sr-only fallback carries the full "also delivered in" prose; the gutter
    // shows a compact "also in" chip set. Assert the VISIBLE chip affordance — a
    // link labelled by the register, NOT an edge (the timeline draws no same_as
    // connector). Scope to the gutter `.sa-chip` so it doesn't collide with the
    // fallback's plain alias link (the register name appears in both surfaces).
    await expect.element(page.getByText("also in")).toBeVisible();
    const ramsChip = page.getByRole("link", { name: "rams" }).first();
    await expect.element(ramsChip).toHaveClass(/sa-chip/);
    await expect
      .element(ramsChip)
      .toHaveAttribute("href", "/catalog/scb/rams/kon");
  });

  it("clusters group members under their group_label heading (Fork B), labelled by facets", async () => {
    const a = variableNode({
      id: "a",
      fqid: "scb/lisa/agi1astsni2007g",
      label: "Näringsgren",
      group_key: "naringsgren",
      group_label: "Näringsgren, största förvärvskälla",
      facets: [{ axis: "source", value: "agi", label: "AGI" }],
    });
    const b = variableNode({
      id: "b",
      fqid: "scb/lisa/ku1astsni2002g",
      label: "Näringsgren",
      group_key: "naringsgren",
      group_label: "Näringsgren, största förvärvskälla",
      facets: [{ axis: "source", value: "ku", label: "KU" }],
    });
    render(HistoryGraph, {
      graph: graph({ nodes: [a, b], focus_id: "a" }),
    });

    // The cluster heading is the group_label (no `group:<key>` node).
    await expect
      .element(
        page.getByRole("heading", {
          name: "Näringsgren, största förvärvskälla",
          level: 4,
        }),
      )
      .toBeVisible();
    // Members are labelled by their facets within the cluster.
    await expect.element(page.getByText("AGI").first()).toBeVisible();
    await expect.element(page.getByText("KU").first()).toBeVisible();
  });

  it("labels facet-less edge-group members by their LEAF SLUG (members share label)", async () => {
    // An edge-style concept group: members share ONE group_label AND one concept
    // `label`, with NO facets. Labelling by `label` would make every member lane
    // read identically (the visual regression), so the lane must disambiguate by
    // leaf slug.
    const a = variableNode({
      id: "a",
      fqid: "scb/lisa/agi1astsni2007",
      label: "Näringsgren",
      group_key: "naringsgren",
      group_label: "Näringsgren, största förvärvskälla",
      facets: [],
    });
    const b = variableNode({
      id: "b",
      fqid: "scb/lisa/ku1astsni2002",
      label: "Näringsgren",
      group_key: "naringsgren",
      group_label: "Näringsgren, största förvärvskälla",
      facets: [],
    });
    render(HistoryGraph, {
      graph: graph({ nodes: [a, b], focus_id: "a" }),
    });

    // Each member lane reads by its distinct leaf slug, not the shared `label`.
    await expect
      .element(page.getByRole("link", { name: "agi1astsni2007" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "ku1astsni2002" }))
      .toBeVisible();
  });

  it("suppresses the internal curation tag on classification succession edges", async () => {
    // A classification-edition succession carries an internal provenance tag (the
    // predecessor `note`) as its `label`; the retired panels showed NO edition
    // succession reason, so the renderer must not leak it. Variable succession
    // labels still render (covered above).
    const older = classificationNode({
      id: "c1",
      fqid: "class/icd9",
      label: "ICD-9",
      version_year: 1987,
      is_current: false,
    });
    const newer = classificationNode({
      id: "c2",
      fqid: "class/icd10",
      label: "ICD-10",
      version_year: 1997,
      is_current: true,
    });
    const edges: GraphEdge[] = [
      {
        id: "e1",
        kind: "succession",
        source: "c1",
        target: "c2",
        label: "curated:slug_toml",
      },
    ];
    render(HistoryGraph, {
      graph: graph({ nodes: [older, newer], edges, focus_id: "c2" }),
    });

    // The edge renders (fallback lists it) but its internal tag is NOT shown.
    const edgeText = await vi.waitFor(() => {
      const el = document.querySelector(".fb-edge");
      if (!el) {
        throw new Error("edge not yet rendered");
      }
      return el.textContent?.replace(/\s+/g, " ").trim() ?? "";
    });
    expect(edgeText).toContain("ICD-9");
    expect(edgeText).toContain("ICD-10");
    expect(edgeText).not.toContain("curated:slug_toml");
    // And the tag never leaks into the drawn SVG labels either.
    expect(document.body.textContent ?? "").not.toContain("curated:slug_toml");
  });

  it("marks a dead/renamed predecessor (a variable with no states) muted with a '(renamed)' hint", async () => {
    // A thin node: a valid fqid but no live row (no states). It renders muted,
    // labelled by its leaf slug (not the full fqid), with a "(renamed)" hint; its
    // fqid link stays present (it 301-redirects).
    const live = variableNode({
      id: "live",
      fqid: "scb/lisa/sni2007",
      label: "Näringsgren (SNI 2007)",
      states: [state({ representation_run_id: 1 })],
    });
    const dead = variableNode({
      id: "dead",
      fqid: "scb/lisa/sni92",
      label: "Näringsgren (SNI 92)",
      states: [],
    });
    const edges: GraphEdge[] = [
      {
        id: "e1",
        kind: "succession",
        source: "dead",
        target: "live",
        label: null,
      },
    ];
    render(HistoryGraph, {
      graph: graph({ nodes: [live, dead], edges, focus_id: "live" }),
    });

    // The renamed hint is present.
    await expect.element(page.getByText("(renamed)").first()).toBeVisible();
    // The dead node is labelled by its leaf slug and still linked (301-redirect).
    const link = page.getByRole("link", { name: "sni92" });
    await expect.element(link).toBeVisible();
    await expect
      .element(link)
      .toHaveAttribute("href", "/catalog/scb/lisa/sni92");
  });

  it("renders fine with a null focus_id (group payload — no highlight)", async () => {
    const a = variableNode({ id: "a", fqid: "scb/lisa/a", label: "A" });
    render(HistoryGraph, { graph: graph({ nodes: [a], focus_id: null }) });
    // It renders (the History heading is present) and no node is the focus.
    await expect
      .element(page.getByRole("heading", { name: "History" }))
      .toBeVisible();
    await expect
      .element(page.getByText("this variable"))
      .not.toBeInTheDocument();
  });
});
