/**
 * Pure projection of the relationship-graph contract (#761/#792) into the shapes
 * the `RepresentationPicker.svelte` graph mode (#904) draws — no runes,
 * unit-tested in `picker_graph.test.ts`. The renderer stays presentational over
 * these.
 *
 * Two node kinds, two substrates:
 *  - a VARIABLE node → a time axis of CELLS, one per `representation_run_id`
 *    (consecutive states sharing it = one cell; the wire is pre-ordered by
 *    `(variant, valid_from)`). A cell spans its states' `[min valid_from,
 *    max valid_to]` and is labelled by its representation identity.
 *  - a CLASSIFICATION node → a POINT at `version_year` (an edition is not "dead"
 *    after its successor), ordered into an edition sequence.
 *
 * Clusters group nodes sharing `group_key` under a `group_label` heading (Fork B);
 * both node kinds carry a `group_label` (a variable's concept-group label, a
 * classification umbrella member's curated label — #794 P3).
 */
import type {
  ClassificationGraphNode,
  GraphEdge,
  GraphNode,
  GraphState,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import {
  formatWindow,
  OPEN_ENDED_VALID_TO,
  YEARLESS_VALID_FROM,
  yearOf,
} from "./catalog";

// ── Track geometry (shared with the renderer) ───────────────────────────────
// The single source for the year→px scale density and a cell's minimum rendered
// width. Both the pure layout (sub-row packing must respect the rendered width,
// not raw year span — #794 P3) and `RepresentationPicker.svelte` (positioning cells)
// read these, so they live here and the component imports them.

/** Horizontal density of the year scale (px per year). */
export const PX_PER_YEAR = 19;
/** A cell's minimum rendered width (px) — a sub-year run still has to fit the
 * checkbox, delivery-column chip, and year text, so it occupies this even when its
 * year span is ~0. */
export const CELL_MIN_W = 128;
/** A cell's minimum footprint expressed in YEARS (its px floor ÷ the year scale).
 * Two cells closer than this in year-space render overlapping even when their raw
 * year windows don't, so packing treats this as the effective span. */
const CELL_MIN_YEARS = CELL_MIN_W / PX_PER_YEAR;

/** One rendered cell of a variable node — a representation run collapsed across
 * its consecutive same-`representation_run_id` states. `window` is the
 * display window over `[min valid_from, max valid_to]` (open start/end already
 * normalized by `formatWindow`), or null for wholly unknown windows; `label` is
 * the representation identity
 * (value-set version label / classification slug / delivery column); `variant`
 * is the run's variant (shown when the node spans >1 variant).
 *
 * `fromYear`/`toYear` are the cell's numeric year bounds on the SHARED time axis
 * (see `yearScaleOf`), already clamped: an unknown start (the yearless floor)
 * reads as the scale's `minYear` with `openStart` set; an open-ended end reads as
 * the scale's `maxYear` ceiling with `openEnd` set. The renderer maps these
 * through the linear x-scale to place the span — it never re-parses the ISO
 * window. */
export interface RunCell {
  runId: number;
  label: string;
  variant: string;
  /** Delivery columns covered by this cell's run. A sequential rename cell can cover
   * an older column while the picker row that toggles it is the folded latest-era
   * row, so graph mode uses this to map cells back to picker rows. */
  columns: string[];
  window: string | null;
  fromYear: number;
  toYear: number;
  openStart: boolean;
  openEnd: boolean;
  /** The sub-row this cell occupies within its lane (0-based). Cells whose time
   * windows overlap (co-existing variants / parallel deliveries) are packed onto
   * distinct rows so they never silently overlap; non-overlapping cells share
   * row 0. Set by `packCells` (after scale clamping). */
  row: number;
}

/** A variable node projected for rendering: its cells (one per representation
 * run, earliest→latest as the wire orders) plus whether it spans >1 variant
 * (so the renderer shows the per-cell variant). */
export interface VariableLane {
  kind: "variable";
  node: VariableGraphNode;
  cells: RunCell[];
  multiVariant: boolean;
  /** How many sub-rows the lane's cells pack into (≥1) — the renderer sizes the
   * lane's track height by this so overlapping (co-existing) cells never collide. */
  rowCount: number;
}

/** A classification node projected for rendering — a single point edition. The
 * cluster orders these by `version_year`. */
export interface ClassificationPoint {
  kind: "classification";
  node: ClassificationGraphNode;
}

export type RenderNode = VariableLane | ClassificationPoint;

/** A cluster of nodes sharing one `group_key` (Fork B) — `null` group_key nodes
 * each get their own singleton cluster. `label` is the `group_label` (when any
 * member carries one — a variable's concept-group label OR a classification
 * umbrella member's curated label, #794 P3), else null (no heading). */
export interface NodeCluster {
  groupKey: string | null;
  label: string | null;
  nodes: RenderNode[];
}

/** The representation identity of a run cell — the value-set version label when
 * non-empty, else the classification slug, else the delivery column name, else a
 * generic fallback. The first state of the run is the representative (the run is
 * homogeneous in representation by construction). */
function cellLabel(rep: GraphState): string {
  if (rep.value_set_version_label !== "") {
    return rep.value_set_version_label;
  }
  if (rep.classification_slug) {
    return rep.classification_slug;
  }
  if (rep.delivery_column_name) {
    return rep.delivery_column_name;
  }
  return "(unlabelled)";
}

/** Group a variable node's pre-ordered `states` into cells by
 * `representation_run_id`: consecutive states sharing the id form ONE cell. The
 * cell's window spans `[min valid_from, max valid_to]` over its states (a null
 * `valid_from` is the yearless-floor sentinel for `formatWindow`; a null
 * `valid_to` is the open-ended sentinel).
 *
 * `fromYear`/`toYear` carry the RAW numeric year bounds for the shared axis: an
 * unknown start (yearless floor) is `openStart` with a NaN `fromYear`, an
 * open-ended end is `openEnd` with a NaN `toYear` — `clampCellsToScale` resolves
 * those NaNs against the computed scale. (`cellsOf` is unit-tested for the
 * grouping + `window`; scale clamping is applied by `renderNodeOf` once the
 * graph-wide scale is known.) */
export function cellsOf(node: VariableGraphNode): RunCell[] {
  const cells: RunCell[] = [];
  let open: {
    rep: GraphState;
    from: string;
    to: string;
    columns: Set<string>;
  } | null = null;
  const flush = () => {
    if (open) {
      const openStart = open.from === YEARLESS_VALID_FROM;
      const openEnd = open.to === OPEN_ENDED_VALID_TO;
      cells.push({
        runId: open.rep.representation_run_id,
        label: cellLabel(open.rep),
        variant: open.rep.variant,
        columns: [...open.columns],
        window: formatWindow(open.from, open.to),
        // A bound's year, or NaN for the open/unknown sentinels (resolved against
        // the scale later). yearOf returns null for an edge/blank bound — treat
        // that as open on its side too rather than NaN-poisoning the scale.
        fromYear: openStart ? Number.NaN : (yearOf(open.from) ?? Number.NaN),
        toYear: openEnd ? Number.NaN : (yearOf(open.to) ?? Number.NaN),
        openStart,
        openEnd,
        row: 0,
      });
    }
  };
  for (const s of node.states) {
    // A null wire bound maps to the display sentinel: null start = yearless
    // floor ("until …"), null end = open-ended ("since …").
    const from = s.valid_from ?? YEARLESS_VALID_FROM;
    const to = s.valid_to ?? OPEN_ENDED_VALID_TO;
    if (open && open.rep.representation_run_id === s.representation_run_id) {
      // Extend the open run's window: min start, max end (ISO strings compare
      // chronologically; the open-ended sentinel sorts last).
      if (from < open.from) {
        open.from = from;
      }
      if (to > open.to) {
        open.to = to;
      }
      if (s.delivery_column_name) {
        open.columns.add(s.delivery_column_name);
      }
    } else {
      flush();
      open = {
        rep: s,
        from,
        to,
        columns: new Set(
          s.delivery_column_name ? [s.delivery_column_name] : [],
        ),
      };
    }
  }
  flush();
  return cells;
}

/** Whether a variable node spans more than one variant across its states. */
function spansMultipleVariants(node: VariableGraphNode): boolean {
  const first = node.states[0]?.variant;
  return node.states.some((s) => s.variant !== first);
}

// ── Shared time axis (#678 rework) ───────────────────────────────────────────
// Every node in the graph lays out against ONE horizontal year scale, so a cell
// at 2010–2018 and an edition point at 2020 align in time across lanes. The scale
// spans the finite years the graph actually exhibits; open-ended cells extend to
// a ceiling, unknown-start cells clamp to the floor.

/** The graph-wide year axis: `[minYear, maxYear]` inclusive. `ceilingFromVintage`
 * records whether `maxYear` came from the catalog vintage (an open-ended cell's
 * "still delivered" ceiling) rather than a finite bound — the renderer fades the
 * open-ended edge toward it rather than drawing a hard wall. */
export interface YearScale {
  minYear: number;
  maxYear: number;
  ceilingFromVintage: boolean;
}

/** Compute the shared year scale across the WHOLE graph: the min/max over every
 * cell's finite `valid_from`/`valid_to` year and every classification's
 * `version_year`. Open-ended cells (`valid_to` null) don't bound the max by
 * themselves — they extend it to `vintageYear` (the catalog vintage ceiling) when
 * that exceeds the finite max, so the track reaches "now" without a finite span
 * ballooning it. A single-year graph (min === max) is widened to a one-year span
 * so the axis still has width to draw. Returns null for a graph with NO datable
 * node (every cell open/unknown on both sides and no classification year) — the
 * renderer then falls back to the axis-less stacked layout. */
export function yearScaleOf(
  graph: RelationshipGraph,
  vintageYear?: number,
): YearScale | null {
  let minYear = Number.POSITIVE_INFINITY;
  let maxYear = Number.NEGATIVE_INFINITY;
  let hasOpenEnded = false;
  const noteYear = (y: number) => {
    if (y < minYear) {
      minYear = y;
    }
    if (y > maxYear) {
      maxYear = y;
    }
  };
  for (const node of graph.nodes) {
    if (node.kind === "classification") {
      if (node.version_year != null) {
        noteYear(node.version_year);
      }
      continue;
    }
    for (const cell of cellsOf(node)) {
      if (!cell.openStart && Number.isFinite(cell.fromYear)) {
        noteYear(cell.fromYear);
      }
      if (!cell.openEnd && Number.isFinite(cell.toYear)) {
        noteYear(cell.toYear);
      } else if (cell.openEnd) {
        hasOpenEnded = true;
      }
    }
  }
  if (!Number.isFinite(minYear) || !Number.isFinite(maxYear)) {
    return null; // no datable node — no axis to draw
  }
  // An open-ended cell reaches the catalog vintage; let it extend the ceiling
  // (but never SHRINK a finite max). The vintage also caps where the open-edge
  // fade lands.
  let ceilingFromVintage = false;
  if (hasOpenEnded && vintageYear != null && vintageYear > maxYear) {
    maxYear = vintageYear;
    ceilingFromVintage = true;
  }
  // Degenerate single-year graph: widen by one year so the linear scale has a
  // non-zero domain (else every x collapses onto one point).
  if (minYear === maxYear) {
    maxYear = minYear + 1;
  }
  return { minYear, maxYear, ceilingFromVintage };
}

/** Resolve a cell's NaN open/unknown bounds against the scale: an unknown start
 * clamps to `minYear`, an open-ended end clamps to `maxYear`. A finite bound is
 * left as-is. Pure, returns a new cell list (the scale isn't known when `cellsOf`
 * runs). */
export function clampCellsToScale(
  cells: RunCell[],
  scale: YearScale,
): RunCell[] {
  return cells.map((c) => ({
    ...c,
    fromYear:
      c.openStart || !Number.isFinite(c.fromYear) ? scale.minYear : c.fromYear,
    toYear: c.openEnd || !Number.isFinite(c.toYear) ? scale.maxYear : c.toYear,
  }));
}

/** A cell's RENDERED end on the year axis: the later of its `toYear` and its start
 * plus the minimum-width footprint (`CELL_MIN_YEARS`). The renderer floors every
 * cell to `CELL_MIN_W` px, so a short run (e.g. one year) actually paints ~6.7
 * years wide — packing must reserve that footprint or two visually-overlapping
 * runs (one ending 2010, the next starting 2011) wrongly share a row (#794 P3). */
function renderedEnd(cell: RunCell): number {
  return Math.max(cell.toYear, cell.fromYear + CELL_MIN_YEARS);
}

/** Pack a lane's cells into sub-rows so time-overlapping cells (co-existing
 * variants / parallel deliveries — the multi-variant case) never render on top of
 * one another. Greedy interval packing: cells in wire order, each placed on the
 * first row whose last-placed cell's RENDERED end is strictly before this cell
 * begins; a new row opens when none fits. Mutates `row` on each cell and returns
 * the row count (≥1). Non-overlapping cells all land on row 0 (`rowCount === 1`),
 * the common single-track lane. Packing uses the rendered footprint (not the raw
 * year span) so the min-width floor can't make two adjacent short runs abut. */
function packCells(cells: RunCell[]): number {
  const rowEnds: number[] = []; // the max RENDERED end placed on each row so far
  for (const cell of cells) {
    let placed = false;
    for (let r = 0; r < rowEnds.length; r++) {
      // Strictly-after the row's rendered end so two adjacent cells whose padded
      // widths would visually overlap go to separate rows.
      if (cell.fromYear > rowEnds[r]) {
        cell.row = r;
        rowEnds[r] = renderedEnd(cell);
        placed = true;
        break;
      }
    }
    if (!placed) {
      cell.row = rowEnds.length;
      rowEnds.push(renderedEnd(cell));
    }
  }
  return Math.max(1, rowEnds.length);
}

/** Labelled year ticks across the scale — a SMALL set of round years (4–6),
 * snapped to a "nice" step (1/2/5/10/20/25/50…) so the axis reads as decades, not
 * arbitrary fractions. Always includes a tick at or before `minYear` and at or
 * after `maxYear`'s nearest step so the ends are anchored. Each tick carries the
 * year (for the x-scale) and its label. */
export function axisTicks(scale: YearScale): { year: number; label: string }[] {
  const span = scale.maxYear - scale.minYear;
  // Aim for ~5 intervals; snap the raw step up to the nearest "nice" value.
  const rawStep = span / 5;
  const niceSteps = [1, 2, 5, 10, 20, 25, 50, 100];
  const step = niceSteps.find((s) => s >= rawStep) ?? 100;
  const first = Math.ceil(scale.minYear / step) * step;
  const ticks: { year: number; label: string }[] = [];
  for (let y = first; y <= scale.maxYear; y += step) {
    ticks.push({ year: y, label: String(y) });
  }
  // Guarantee the domain ends are represented even when the nice step skips them
  // (a 1996–2020 span on step 10 → 2000,2010,2020 misses 1996): prepend/append
  // the raw bounds so the track is anchored. Drop the FIRST/LAST nice tick when it
  // sits within ~half a step of the anchored end — otherwise the two labels (e.g.
  // 1969 anchor + 1970 step tick) overlap on the axis. Keep at least the anchors.
  const tooClose = step * 0.5;
  if (ticks[0] && ticks[0].year - scale.minYear < tooClose) {
    ticks.shift();
  }
  if (ticks.at(-1) && scale.maxYear - ticks[ticks.length - 1].year < tooClose) {
    ticks.pop();
  }
  if (ticks[0]?.year !== scale.minYear) {
    ticks.unshift({ year: scale.minYear, label: String(scale.minYear) });
  }
  if (ticks.at(-1)?.year !== scale.maxYear) {
    ticks.push({ year: scale.maxYear, label: String(scale.maxYear) });
  }
  return ticks;
}

/** Project one graph node into its render shape (variable → lane with cells,
 * classification → point). When a `scale` is given, the variable's cells are
 * clamped to it (open/unknown bounds resolved to the scale ends); without one
 * (the axis-less fallback) cells keep their raw bounds. */
function renderNodeOf(node: GraphNode, scale: YearScale | null): RenderNode {
  if (node.kind === "variable") {
    const cells = scale
      ? clampCellsToScale(cellsOf(node), scale)
      : cellsOf(node);
    // Pack into sub-rows only on the time axis (rows are defined by year overlap);
    // the axis-less fallback stacks cells left→right and keeps one row.
    const rowCount = scale ? packCells(cells) : 1;
    return {
      kind: "variable",
      node,
      cells,
      multiVariant: spansMultipleVariants(node),
      rowCount,
    };
  }
  return { kind: "classification", node };
}

/** The sort key for ordering nodes within a cluster: classifications by
 * `version_year` (a null year sorts last); variables by their earliest finite
 * cell window — both reduce to "earliest first". A classification's point year
 * and a variable's first cell don't share a scale, but a cluster is homogeneous
 * in kind in practice (group members are one grain), so the cross-kind tiebreak
 * never bites; the fallback keeps the order stable. */
function orderKey(rn: RenderNode): number {
  if (rn.kind === "classification") {
    return rn.node.version_year ?? Number.POSITIVE_INFINITY;
  }
  const first = rn.node.states[0]?.valid_from;
  if (first == null) {
    return Number.NEGATIVE_INFINITY; // unknown start = earliest
  }
  // A non-leading-4-digit bound (blank/edge) sorts last, as before.
  return yearOf(first) ?? Number.POSITIVE_INFINITY;
}

/** Resolve a union-find root for `id`, path-compressing on the way. */
function findRoot(parent: Map<string, string>, id: string): string {
  let root = id;
  while (parent.get(root) !== root) {
    root = parent.get(root) as string;
  }
  let cur = id;
  while (parent.get(cur) !== root) {
    const next = parent.get(cur) as string;
    parent.set(cur, root);
    cur = next;
  }
  return root;
}

function variableNodeById(
  graph: RelationshipGraph,
): Map<string, VariableGraphNode> {
  return new Map(
    graph.nodes
      .filter((node): node is VariableGraphNode => node.kind === "variable")
      .map((node) => [node.id, node]),
  );
}

function nodeHasRepresentationEndpoint(
  node: VariableGraphNode | undefined,
  column: string,
  variant: string | null | undefined,
): boolean {
  const foldedColumn = column.toLocaleLowerCase("sv-SE");
  return (
    node?.states.some(
      (state) =>
        state.delivery_column_name?.toLocaleLowerCase("sv-SE") ===
          foldedColumn &&
        (variant == null || state.variant === variant),
    ) ?? false
  );
}

/** Whether an edge applies to the graph currently being rendered. Variable-grain
 * succession edges apply whenever their nodes are present. Representation-grain
 * edges additionally require the source/target columns to be present on the
 * endpoint nodes, and a non-null `variant` confines the match to that concrete
 * register variant (#846/#888). */
export function graphEdgeVisibleInGraph(
  edge: GraphEdge,
  graph: RelationshipGraph,
): boolean {
  if (edge.source_column == null || edge.target_column == null) {
    return true;
  }
  const byId = variableNodeById(graph);
  return (
    nodeHasRepresentationEndpoint(
      byId.get(edge.source),
      edge.source_column,
      edge.variant,
    ) &&
    nodeHasRepresentationEndpoint(
      byId.get(edge.target),
      edge.target_column,
      edge.variant,
    )
  );
}

/** Cluster the graph's nodes into connected subjects, preserving first-seen
 * cluster order and ordering each cluster's members earliest-first. Two nodes
 * share a cluster when they share a `group_key` (Fork B) OR are joined by an edge
 * — so a succession chain of ungrouped variables renders as ONE rail (like the
 * retired panel), not several disconnected singletons whose edges couldn't draw.
 * A node with no `group_key` and no edge gets its own singleton cluster (no
 * heading). The cluster `label` is any member's `group_label`. */
export function clustersOf(
  graph: RelationshipGraph,
  scale: YearScale | null = null,
): NodeCluster[] {
  // Union-find over node ids.
  const parent = new Map<string, string>();
  for (const node of graph.nodes) {
    parent.set(node.id, node.id);
  }
  const union = (a: string, b: string) => {
    if (!parent.has(a) || !parent.has(b)) {
      return; // a dangling edge endpoint — ignore (resolveEdges drops it too)
    }
    parent.set(findRoot(parent, a), findRoot(parent, b));
  };
  // Union all members sharing a group_key (each key's first member anchors it).
  const keyAnchor = new Map<string, string>();
  for (const node of graph.nodes) {
    if (node.group_key != null) {
      const anchor = keyAnchor.get(node.group_key);
      if (anchor === undefined) {
        keyAnchor.set(node.group_key, node.id);
      } else {
        union(anchor, node.id);
      }
    }
  }
  // Union edge-connected nodes (a succession chain is one subject).
  for (const edge of graph.edges) {
    if (graphEdgeVisibleInGraph(edge, graph)) {
      union(edge.source, edge.target);
    }
  }

  // Build clusters in first-seen node order, keyed on the union root.
  const order: string[] = [];
  const byRoot = new Map<string, NodeCluster>();
  for (const node of graph.nodes) {
    const root = findRoot(parent, node.id);
    let cluster = byRoot.get(root);
    if (!cluster) {
      cluster = { groupKey: node.group_key, label: null, nodes: [] };
      byRoot.set(root, cluster);
      order.push(root);
    } else if (cluster.groupKey == null && node.group_key != null) {
      cluster.groupKey = node.group_key;
    }
    // The cluster heading is any member's group_label — BOTH kinds carry one now:
    // a variable from its canonical concept group, a classification umbrella member
    // from its curated group's display label (#794 P3). A non-member spine edition
    // (or an ungrouped node) carries none and never sets the heading.
    if (cluster.label == null && node.group_label != null) {
      cluster.label = node.group_label;
    }
    cluster.nodes.push(renderNodeOf(node, scale));
  }
  const clusters = order.map((r) => byRoot.get(r) as NodeCluster);
  for (const cluster of clusters) {
    cluster.nodes.sort((a, b) => orderKey(a) - orderKey(b));
  }
  return clusters;
}

/** An edge resolved to its endpoint nodes for rendering — dropped when either
 * endpoint id isn't a rendered node (defensive; the contract pairs edges with
 * nodes, but a stale-skew payload shouldn't crash the render). */
export interface ResolvedEdge {
  edge: GraphEdge;
  source: GraphNode;
  target: GraphNode;
}

/** Resolve each edge's `source`/`target` ids to nodes, dropping any edge whose
 * endpoint isn't present. The edge set is succession-only (directed); the
 * renderer draws each as a chronological arrow. */
export function resolveEdges(graph: RelationshipGraph): ResolvedEdge[] {
  const byId = new Map<string, GraphNode>(graph.nodes.map((n) => [n.id, n]));
  const resolved: ResolvedEdge[] = [];
  for (const edge of graph.edges) {
    if (!graphEdgeVisibleInGraph(edge, graph)) {
      continue;
    }
    const source = byId.get(edge.source);
    const target = byId.get(edge.target);
    if (source && target) {
      resolved.push({ edge, source, target });
    }
  }
  return resolved;
}

export interface ClassificationDagNode {
  point: ClassificationPoint;
  column: number;
  row: number;
  order: number;
}

export interface ClassificationDagEdge {
  edge: GraphEdge;
  source: ClassificationDagNode;
  target: ClassificationDagNode;
}

export interface ClassificationDagLayout {
  nodes: ClassificationDagNode[];
  edges: ClassificationDagEdge[];
  columns: number;
  rows: number;
}

/** Compact non-timeline layout for classification edition succession (#906).
 * Columns are topological ranks (predecessor → successor), not absolute years;
 * rows only open when a rank branches. The input points already carry the stable
 * edition ordering from `clustersOf`, so ties remain deterministic. */
export function classificationDagLayout(
  points: ClassificationPoint[],
  edges: ResolvedEdge[],
): ClassificationDagLayout {
  const ordered = points.map((point, order) => ({
    point,
    order,
    rank: 0,
  }));
  const byId = new Map(ordered.map((node) => [node.point.node.id, node]));
  const outgoing = new Map<string, string[]>();
  const indegree = new Map<string, number>(
    ordered.map((node) => [node.point.node.id, 0]),
  );
  const dagEdges = edges.filter(
    (edge) =>
      edge.source.kind === "classification" &&
      edge.target.kind === "classification" &&
      byId.has(edge.source.id) &&
      byId.has(edge.target.id),
  );

  for (const edge of dagEdges) {
    const targets = outgoing.get(edge.source.id) ?? [];
    targets.push(edge.target.id);
    outgoing.set(edge.source.id, targets);
    indegree.set(edge.target.id, (indegree.get(edge.target.id) ?? 0) + 1);
  }
  for (const targets of outgoing.values()) {
    targets.sort((a, b) => {
      const left = byId.get(a)?.order ?? 0;
      const right = byId.get(b)?.order ?? 0;
      return left - right;
    });
  }

  const queue = ordered
    .filter((node) => indegree.get(node.point.node.id) === 0)
    .sort((a, b) => a.order - b.order);
  const visited = new Set<string>();
  while (queue.length > 0) {
    const node = queue.shift() as (typeof ordered)[number];
    visited.add(node.point.node.id);
    for (const targetId of outgoing.get(node.point.node.id) ?? []) {
      const target = byId.get(targetId);
      if (!target) {
        continue;
      }
      target.rank = Math.max(target.rank, node.rank + 1);
      const nextIndegree = (indegree.get(targetId) ?? 0) - 1;
      indegree.set(targetId, nextIndegree);
      if (nextIndegree === 0) {
        queue.push(target);
        queue.sort((a, b) => a.order - b.order);
      }
    }
  }

  // Defensive cycle/skew fallback: keep every node visible in deterministic order.
  let fallbackRank = ordered.reduce((max, node) => Math.max(max, node.rank), 0);
  for (const node of ordered) {
    if (!visited.has(node.point.node.id)) {
      fallbackRank += 1;
      node.rank = fallbackRank;
    }
  }

  const byRank = new Map<number, typeof ordered>();
  for (const node of ordered) {
    const column = byRank.get(node.rank) ?? [];
    column.push(node);
    byRank.set(node.rank, column);
  }

  const layoutNodes: ClassificationDagNode[] = [];
  for (const [rank, column] of byRank) {
    column.sort((a, b) => a.order - b.order);
    column.forEach((node, row) => {
      layoutNodes.push({
        point: node.point,
        column: rank,
        row,
        order: node.order,
      });
    });
  }
  layoutNodes.sort((a, b) => a.order - b.order);

  const layoutById = new Map(
    layoutNodes.map((node) => [node.point.node.id, node]),
  );
  return {
    nodes: layoutNodes,
    edges: dagEdges
      .map((edge) => {
        const source = layoutById.get(edge.source.id);
        const target = layoutById.get(edge.target.id);
        return source && target ? { edge: edge.edge, source, target } : null;
      })
      .filter((edge): edge is ClassificationDagEdge => edge != null),
    columns:
      layoutNodes.reduce((max, node) => Math.max(max, node.column), -1) + 1,
    rows: layoutNodes.reduce((max, node) => Math.max(max, node.row), -1) + 1,
  };
}
