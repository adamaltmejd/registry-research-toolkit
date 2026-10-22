/**
 * Pure projection of the relationship-graph contract (#761/#792) into the shapes
 * the classification edition graph (`ClassificationEditionGraph.tsx`) draws —
 * unit-tested in `picker_graph.test.ts`. The renderer stays presentational over
 * these. A CLASSIFICATION node is a POINT at `version_year` (an edition is not
 * "dead" after its successor), ordered into an edition sequence. Variable nodes
 * are drawn by the LaneGraph (`picker_lanes.ts`); here they only join clusters
 * and gate representation-grain edges.
 *
 * Clusters group nodes sharing `group_key` under a `group_label` heading (Fork B);
 * both node kinds carry a `group_label` (a variable's concept-group label, a
 * classification umbrella member's curated label — #794 P3).
 */
import type {
  ClassificationGraphNode,
  GraphEdge,
  GraphNode,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";

/** A classification node projected for rendering — a single point edition. The
 * cluster orders these by `version_year`. */
export interface ClassificationPoint {
  kind: "classification";
  node: ClassificationGraphNode;
}

/** A cluster of nodes sharing one `group_key` (Fork B) — `null` group_key nodes
 * each get their own singleton cluster. `label` is the `group_label` (when any
 * member carries one — a variable's concept-group label OR a classification
 * umbrella member's curated label, #794 P3), else null (no heading). */
export interface NodeCluster {
  groupKey: string | null;
  label: string | null;
  nodes: ClassificationPoint[];
}

/** The sort key for ordering a cluster's editions: `version_year`, earliest
 * first; a null year sorts last. */
function orderKey(point: ClassificationPoint): number {
  return point.node.version_year ?? Number.POSITIVE_INFINITY;
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

function graphColumnMatches(left: string, right: string): boolean {
  return left.toLocaleLowerCase("sv-SE") === right.toLocaleLowerCase("sv-SE");
}

function nodeHasRepresentationEndpoint(
  node: VariableGraphNode | undefined,
  column: string,
  variant: string | null | undefined,
): boolean {
  return (
    node?.states.some(
      (state) =>
        state.delivery_column_name != null &&
        graphColumnMatches(state.delivery_column_name, column) &&
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
  if (edge.source_column == null && edge.target_column == null) {
    return true;
  }
  if (edge.source_column == null || edge.target_column == null) {
    return false;
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
export function clustersOf(graph: RelationshipGraph): NodeCluster[] {
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
      cluster = { groupKey: node.group_key ?? null, label: null, nodes: [] };
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
    if (node.kind === "classification") {
      cluster.nodes.push({ kind: "classification", node });
    }
  }
  // A cluster of variables alone has no edition to draw.
  const clusters = order
    .map((r) => byRoot.get(r) as NodeCluster)
    .filter((cluster) => cluster.nodes.length > 0);
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

/** Whether the classification-edition renderer has a succession component to
 * draw for the current focus. A group graph can contain edges for sibling
 * editions while its focused member is isolated, so a graph-wide edge check is
 * not a renderability check. */
export function classificationGraphHasRenderableSuccession(
  graph: RelationshipGraph,
): boolean {
  const successionEdges = resolveEdges(graph).filter(
    (edge) =>
      edge.source.kind === "classification" &&
      edge.target.kind === "classification",
  );
  if (graph.focus_id == null) {
    return successionEdges.length > 0;
  }
  return successionEdges.some(
    (edge) =>
      edge.source.id === graph.focus_id || edge.target.id === graph.focus_id,
  );
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
