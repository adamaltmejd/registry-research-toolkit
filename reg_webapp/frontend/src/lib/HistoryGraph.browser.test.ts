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
// version ordering + is_current, succession edges with labels, focus
// highlight, and the same_as "also delivered in" affordance. We assert against the
// VISUALLY-HIDDEN structured fallback (real text the SVG mirrors), which the a11y
// queries reach regardless of the SVG geometry.

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
    operational_definition: null,
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

  it("renders a succession (directed) edge with its label", async () => {
    const a = variableNode({ id: "a", fqid: "scb/lisa/a", label: "A" });
    const b = variableNode({ id: "b", fqid: "scb/lisa/b", label: "B" });
    const edges: GraphEdge[] = [
      {
        id: "e1",
        kind: "succession",
        source: "a",
        target: "b",
        label: "Definition change",
      },
    ];
    render(HistoryGraph, {
      graph: graph({ nodes: [a, b], edges, focus_id: "a" }),
    });

    // The fallback lists the edge with its label + the directional glyph. The
    // edge text spans several text nodes (whitespace-separated), so assert against
    // the `.fb-edge`'s normalized textContent rather than a single text node.
    const edgeTexts = await vi.waitFor(() => {
      const els = [...document.querySelectorAll(".fb-edge")];
      if (els.length < 1) {
        throw new Error("edges not yet rendered");
      }
      return els.map((el) => el.textContent?.replace(/\s+/g, " ").trim() ?? "");
    });
    // Succession is directed (→) with its reason label.
    expect(edgeTexts.some((t) => /▸ A → B \(Definition change\)/.test(t))).toBe(
      true,
    );
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

    // The sr-only fallback carries the full "also delivered in" prose as plain text
    // (register names, no links — #794 P1); the gutter shows a compact "also in"
    // chip set whose chips ARE the links. Assert the VISIBLE chip affordance — a
    // link labelled by the register, NOT an edge (the timeline draws no same_as
    // connector). The gutter `.sa-chip` is the only `rams` LINK.
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
    // `a` is the focus node (plain label in the gutter, not a link) — its slug
    // shows as gutter TEXT; `b` is navigable, so its VISIBLE gutter name-link
    // carries the slug. The sr-only fallback carries the labels as plain text only
    // (no <a>) — #794 P1 removed the duplicate fallback links — so the ONLY link
    // here is `b`'s gutter name-link (the focus self-link is gone).
    const aGutter = await vi.waitFor(() => {
      const el = [...document.querySelectorAll(".gutter .name")].find((n) =>
        n.textContent?.trim().startsWith("agi1astsni2007"),
      );
      if (!el) {
        throw new Error("focus gutter name not yet rendered");
      }
      return el as HTMLElement;
    });
    // The focus node's slug is plain text, NOT a link.
    expect(aGutter.tagName).toBe("SPAN");
    const bGutter = await vi.waitFor(() => {
      const el = [...document.querySelectorAll("a.name-link")].find(
        (a) => a.textContent?.trim() === "ku1astsni2002",
      );
      if (!el) {
        throw new Error("member gutter link not yet rendered");
      }
      return el as HTMLAnchorElement;
    });
    expect(bGutter.getAttribute("href")).toBe(
      "/catalog/scb/lisa/ku1astsni2002",
    );
    // The non-focus member's slug is reachable as a link; the focus self-link is
    // gone, so exactly one slug link exists.
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
    // The dead node is labelled by its leaf slug and still linked (301-redirect) —
    // now from the VISIBLE gutter too (#794 P2), not only the sr-only fallback.
    const gutterLink = await vi.waitFor(() => {
      const el = [...document.querySelectorAll("a.name-link")].find(
        (a) =>
          (a as HTMLAnchorElement).getAttribute("href") ===
          "/catalog/scb/lisa/sni92",
      );
      if (!el) {
        throw new Error("dead-node gutter link not yet rendered");
      }
      return el as HTMLAnchorElement;
    });
    expect(gutterLink.closest(".graph-fallback")).toBeNull();
    const linkText = gutterLink.textContent?.replace(/\s+/g, " ").trim() ?? "";
    expect(linkText).toContain("sni92");
    expect(linkText).toContain("(renamed)");
  });

  it("does NOT mark a LIVE stateless variable as renamed (#794 P1: states:[] is not the renamed signal)", async () => {
    // A live stateless variable (a supported catalog/group case — e.g. a member with
    // no coverage row) also has `states: []`, but it is NOT a succession-chain
    // placeholder. The view must keep its real concept label, never relabel it to a
    // leaf slug + "(renamed)". Two LIVE group members, one stateless, NO succession.
    const withStates = variableNode({
      id: "a",
      fqid: "scb/lisa/kon",
      label: "Kön",
      group_key: "g",
      group_label: "Demografi",
      states: [state({ representation_run_id: 1 })],
    });
    const stateless = variableNode({
      id: "b",
      fqid: "scb/lisa/alder",
      label: "Ålder",
      group_key: "g",
      group_label: "Demografi",
      states: [], // live but stateless — NOT a renamed placeholder
    });
    render(HistoryGraph, {
      graph: graph({
        nodes: [withStates, stateless],
        edges: [],
        focus_id: "a",
      }),
    });

    // No "(renamed)" hint anywhere (the stateless live member is not a placeholder).
    await expect.element(page.getByText("(renamed)")).not.toBeInTheDocument();
    // The stateless member keeps its real concept label (its own slug, since the
    // group members share a label) — it renders as a gutter name, not muted.
    const statelessName = await vi.waitFor(() => {
      const el = [...document.querySelectorAll(".gutter .name")].find((n) =>
        n.textContent?.includes("alder"),
      );
      if (!el) {
        throw new Error("stateless member gutter name not yet rendered");
      }
      return el as HTMLElement;
    });
    // Its lane is NOT marked .renamed (no muted/dashed treatment).
    expect(statelessName.closest(".lane.renamed")).toBeNull();
  });

  it("fades BOTH ends of a both-ends-unbounded cell (#794 P3: combined open-start/open-end mask)", async () => {
    // A cell open on BOTH sides gets both `.open-start` and `.open-end`. The
    // single-axis rules each set one `mask-image`; without a combined rule the later
    // one wins and the right edge is a hard wall. A node with one both-open cell PLUS
    // a finite cell so the graph is datable (an axis exists; the open cell clamps to
    // the scale ends while keeping both open flags).
    const node = variableNode({
      id: "v1",
      fqid: "scb/lisa/kon",
      label: "Kön",
      states: [
        state({
          representation_run_id: 1,
          value_set_version_label: "a",
          valid_from: "2000-01-01",
          valid_to: "2004-12-31",
        }),
        state({
          representation_run_id: 2,
          value_set_version_label: "b",
          valid_from: null, // open start
          valid_to: null, // open end
        }),
      ],
    });
    render(HistoryGraph, { graph: graph({ nodes: [node], focus_id: "v1" }) });

    const bothOpen = await vi.waitFor(() => {
      const el = document.querySelector(".cell.open-start.open-end");
      if (!el) {
        throw new Error("both-ends-open cell not yet rendered");
      }
      return el as HTMLElement;
    });
    // Both open classes present (the renderer applies both).
    expect(bothOpen.classList.contains("open-start")).toBe(true);
    expect(bothOpen.classList.contains("open-end")).toBe(true);
    // The computed mask composes TWO gradients (one per side) — the combined rule
    // intersects them, so neither single-axis rule overwrites the other. (A single
    // gradient = only one side faded = the bug.)
    const mask =
      getComputedStyle(bothOpen).maskImage ||
      getComputedStyle(bothOpen).webkitMaskImage ||
      "";
    expect(mask.match(/gradient/g)?.length ?? 0).toBeGreaterThanOrEqual(2);
  });

  it("makes a navigable node's VISIBLE gutter name a catalog link; the focus node stays a plain label (#794 P2)", async () => {
    // A non-focus node with an fqid must be openable from the visible gutter (the
    // retired panels had visible links). The focus node (current page) is NOT a
    // self link. Scope to the gutter `.name-link` so this is the VISIBLE surface,
    // not the sr-only fallback's own `<a>`.
    const focus = variableNode({
      id: "v1",
      fqid: "scb/lisa/kon",
      label: "Kön",
    });
    const succ = variableNode({
      id: "v2",
      fqid: "scb/lisa/civilstand",
      label: "Civilstånd",
    });
    const edges: GraphEdge[] = [
      { id: "e1", kind: "succession", source: "v1", target: "v2", label: null },
    ];
    render(HistoryGraph, {
      graph: graph({ nodes: [focus, succ], edges, focus_id: "v1" }),
    });

    // The successor's gutter name is a VISIBLE link to its fqid.
    const link = await vi.waitFor(() => {
      const el = document.querySelector("a.name-link");
      if (!el) {
        throw new Error("gutter name link not yet rendered");
      }
      return el as HTMLAnchorElement;
    });
    expect(link.closest(".graph-fallback")).toBeNull(); // visible, not sr-only
    expect(link.getAttribute("href")).toBe("/catalog/scb/lisa/civilstand");
    expect(link.textContent?.trim()).toBe("Civilstånd");
    // The focus node's gutter name is NOT a link (no self-link).
    const focusLinks = [...document.querySelectorAll("a.name-link")].filter(
      (a) =>
        (a as HTMLAnchorElement).getAttribute("href") ===
        "/catalog/scb/lisa/kon",
    );
    expect(focusLinks).toHaveLength(0);
  });

  it("makes a non-focus classification edition's gutter name a catalog link (#794 P2)", async () => {
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
    render(HistoryGraph, {
      graph: graph({ nodes: [older, newer], focus_id: "c2" }),
    });
    // The non-focus older edition's gutter name links to its fqid.
    const link = await vi.waitFor(() => {
      const el = [...document.querySelectorAll("a.name-link")].find(
        (a) =>
          (a as HTMLAnchorElement).getAttribute("href") ===
          "/catalog/class/sun2000",
      );
      if (!el) {
        throw new Error("classification gutter link not yet rendered");
      }
      return el as HTMLAnchorElement;
    });
    expect(link.closest(".graph-fallback")).toBeNull();
    expect(link.textContent?.trim()).toContain("SUN 2000");
  });

  it("surfaces a succession edge's effective_year on the VISIBLE annotation, even with no reason (#794 P2)", async () => {
    // A replacement edge with an effective year but no human reason previously
    // rendered as an unlabelled arrow. The transition year must be visible again
    // ("→ 2009"); a reason-with-year reads "renamed → 2009".
    const a = variableNode({ id: "a", fqid: "scb/lisa/a", label: "A" });
    const b = variableNode({ id: "b", fqid: "scb/lisa/b", label: "B" });
    const c = variableNode({ id: "c", fqid: "scb/lisa/c", label: "C" });
    const edges: GraphEdge[] = [
      // Year only, no reason.
      {
        id: "e1",
        kind: "succession",
        source: "a",
        target: "b",
        label: null,
        effective_year: 2009,
      },
      // Reason + year.
      {
        id: "e2",
        kind: "succession",
        source: "b",
        target: "c",
        label: "renamed",
        effective_year: 2015,
      },
    ];
    render(HistoryGraph, {
      graph: graph({ nodes: [a, b, c], edges, focus_id: "a" }),
    });

    // VISIBLE reason chips (outside the sr-only fallback) carry the year.
    const chips = await vi.waitFor(() => {
      const els = [...document.querySelectorAll(".reason")].filter(
        (el) => el.closest(".graph-fallback") === null,
      );
      if (els.length < 2) {
        throw new Error("succession reason chips not yet rendered");
      }
      return els.map((el) => el.textContent?.replace(/\s+/g, " ").trim() ?? "");
    });
    expect(chips).toContain("→ 2009");
    expect(chips).toContain("renamed → 2015");
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

  it("exposes ONE coherent set of nav links: visible gutter links in the a11y tree, NO duplicate hidden fallback links (#794 P1)", async () => {
    // The a11y restructure: the timeline is a labelled GROUP (not role="img", which
    // would hide the descendant gutter links); the visible gutter <a>s are the real
    // navigation; the visually-hidden fallback is NON-interactive text (no <a>), so
    // keyboard users get exactly one set of focusable links (no invisible duplicate
    // tab stops, no focus self-link).
    const focus = variableNode({
      id: "v1",
      fqid: "scb/lisa/kon",
      label: "Kön",
    });
    const succ = variableNode({
      id: "v2",
      fqid: "scb/lisa/civilstand",
      label: "Civilstånd",
    });
    const edges: GraphEdge[] = [
      { id: "e1", kind: "succession", source: "v1", target: "v2", label: null },
    ];
    render(HistoryGraph, {
      graph: graph({ nodes: [focus, succ], edges, focus_id: "v1" }),
    });
    await expect
      .element(page.getByRole("heading", { name: "History" }))
      .toBeVisible();

    // The timeline container is a labelled GROUP, NOT role="img".
    const timeline = await vi.waitFor(() => {
      const el = document.querySelector(".timeline");
      if (!el) {
        throw new Error("timeline not yet rendered");
      }
      return el as HTMLElement;
    });
    expect(timeline.getAttribute("role")).toBe("group");
    expect(timeline.getAttribute("aria-label")).toMatch(/History timeline/);

    // The visually-hidden fallback exists (the SR description) but holds NO links.
    const fallback = document.querySelector(".graph-fallback");
    expect(fallback).not.toBeNull();
    expect(fallback?.querySelectorAll("a").length).toBe(0);

    // EVERY link in the component lives in the visible gutter (none clipped in the
    // fallback) — exactly one nav surface. The focus self-link is gone, so the only
    // link is the navigable successor.
    const links = [...document.querySelectorAll("a")];
    expect(links.length).toBeGreaterThan(0);
    expect(links.every((a) => a.closest(".graph-fallback") === null)).toBe(
      true,
    );
    const hrefs = links.map((a) => a.getAttribute("href"));
    expect(hrefs).toContain("/catalog/scb/lisa/civilstand"); // the successor link
    expect(hrefs).not.toContain("/catalog/scb/lisa/kon"); // no focus self-link
  });

  it("hides the decorative track/connectors from AT while keeping the gutter exposed (#794 P1)", async () => {
    // The drawn pixel layout (cell/point bars, connectors, axis) is aria-hidden so
    // AT isn't read the geometry; the fallback list mirrors it as text. The gutter
    // (names + nav links) stays in the a11y tree.
    const node = variableNode({
      id: "v1",
      fqid: "scb/lisa/kon",
      label: "Kön",
      states: [
        state({ representation_run_id: 1, value_set_version_label: "a" }),
        state({ representation_run_id: 2, value_set_version_label: "b" }),
      ],
    });
    render(HistoryGraph, { graph: graph({ nodes: [node], focus_id: "v1" }) });

    const track = await vi.waitFor(() => {
      const el = document.querySelector(".track");
      if (!el) {
        throw new Error("track not yet rendered");
      }
      return el as HTMLElement;
    });
    expect(track.getAttribute("aria-hidden")).toBe("true");
    // The connectors SVG is decorative too.
    const svg = document.querySelector("svg.connectors");
    expect(svg?.getAttribute("aria-hidden")).toBe("true");
    // The gutter is NOT inside an aria-hidden subtree (its name stays exposed).
    const gutterName = document.querySelector(".gutter .name");
    expect(gutterName?.closest("[aria-hidden='true']")).toBeNull();
  });
});
