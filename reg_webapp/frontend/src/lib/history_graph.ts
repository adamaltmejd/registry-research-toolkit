import type {
  GraphEdge as ApiGraphEdge,
  GraphNode as ApiGraphNode,
  GraphState,
  RelationshipGraph,
} from "./api";

export type HistoryGraphMode = "variable" | "classification";
export type HistoryGraphNodeKind = "variable" | "classification";
export type HistoryGraphEdgeKind = "succession" | "related";

export interface HistoryColumnSlice {
  id: string;
  label: string;
  columnLabels: string[];
  valueSetLabel?: string | null;
  variant: string;
  from: number | null;
  to: number | null;
  stateIds: number[];
}

export interface HistoryGraphNode {
  id: string;
  kind: HistoryGraphNodeKind;
  label: string;
  fqid: string | null;
  from: number | null;
  to: number | null;
  current?: boolean;
  self?: boolean;
  columns: HistoryColumnSlice[];
}

export interface HistoryGraphEdge {
  id: string;
  kind: HistoryGraphEdgeKind;
  from: string;
  to: string;
  fromYear: number | null;
  toYear: number | null;
  label: string | null;
}

export interface HistoryGraph {
  mode: HistoryGraphMode;
  title: string;
  nodes: HistoryGraphNode[];
  edges: HistoryGraphEdge[];
  timeDomain: HistoryGraphTimeDomain | null;
  warnings: string[];
  nodeGrain: "entity-with-column-slices";
  dataContract: "reg-meta-relationship-graph";
}

interface HistoryGraphTimeDomain {
  from: number | null;
  to: number | null;
  openEnded: boolean;
}

const OPEN_END_YEAR = 9999;
const YEARLESS_START = 1;

export function historyGraphFromRelationshipGraph(
  graph: RelationshipGraph,
): HistoryGraph {
  const nodes = graph.nodes.map((node) => renderNode(node, graph.focus_id));
  const nodeById = new Map(nodes.map((node) => [node.id, node]));
  const hasVariable = nodes.some((node) => node.kind === "variable");
  return {
    mode: hasVariable ? "variable" : "classification",
    title: "Relations",
    nodes,
    edges: graph.edges.map((edge) => renderEdge(edge, nodeById)),
    timeDomain: renderTimeDomain(graph),
    warnings: [],
    nodeGrain: "entity-with-column-slices",
    dataContract: "reg-meta-relationship-graph",
  };
}

export function hasRenderableHistoryGraph(graph: HistoryGraph): boolean {
  return graph.nodes.length > 0;
}

export function historyGraphYears(
  graph: HistoryGraph,
  vintageYear?: number,
): { min: number; max: number } {
  const nodeYears = historyGraphNodeYears(graph, vintageYear);
  if (graph.timeDomain !== null) {
    const min = graph.timeDomain.from ?? nodeYears.min;
    const max =
      graph.timeDomain.to ??
      (graph.timeDomain.openEnded
        ? (vintageYear ?? nodeYears.max)
        : nodeYears.max);
    return { min, max: Math.max(min, max) };
  }
  return nodeYears;
}

function historyGraphNodeYears(
  graph: HistoryGraph,
  vintageYear?: number,
): { min: number; max: number } {
  const years: number[] = [];
  for (const node of graph.nodes) {
    if (node.from !== null) {
      years.push(node.from);
    }
    if (node.to !== null) {
      years.push(node.to);
    }
    for (const column of node.columns) {
      if (column.from !== null) {
        years.push(column.from);
      }
      if (column.to !== null) {
        years.push(column.to);
      }
    }
  }
  if (years.length === 0) {
    const fallback = vintageYear ?? new Date().getFullYear();
    return { min: fallback, max: fallback };
  }
  const max = Math.max(...years, vintageYear ?? -Infinity);
  return { min: Math.min(...years), max };
}

function renderTimeDomain(
  graph: RelationshipGraph,
): HistoryGraphTimeDomain | null {
  const domain = graph.time_domain;
  if (!domain) {
    return null;
  }
  const from = wireYear(domain.coverage_from);
  const to = wireYear(domain.coverage_to);
  if (from === null && to === null && !domain.open_ended) {
    return null;
  }
  return { from, to, openEnded: domain.open_ended };
}

function renderNode(
  node: ApiGraphNode,
  focusId: string | null,
): HistoryGraphNode {
  if (node.kind === "classification") {
    return {
      id: node.id,
      kind: "classification",
      label: node.label,
      fqid: node.fqid,
      from: node.version_year,
      to: node.version_year,
      current: node.is_current,
      self: node.id === focusId,
      columns: [],
    };
  }
  const columns = runSlices(node.id, node.states);
  const coverage = coverageFromStates(node.states);
  return {
    id: node.id,
    kind: "variable",
    label: node.label,
    fqid: node.fqid,
    from: coverage.from,
    to: coverage.to,
    self: node.id === focusId,
    columns,
  };
}

function renderEdge(
  edge: ApiGraphEdge,
  nodes: Map<string, HistoryGraphNode>,
): HistoryGraphEdge {
  const source = nodes.get(edge.source);
  const target = nodes.get(edge.target);
  const edgeYear =
    edge.kind === "succession"
      ? (source?.to ?? target?.from ?? source?.from ?? target?.to ?? null)
      : null;
  return {
    id: edge.id,
    kind: edge.kind,
    from: edge.source,
    to: edge.target,
    fromYear: edgeYear,
    toYear: edgeYear,
    label: edge.label,
  };
}

function runSlices(nodeId: string, states: GraphState[]): HistoryColumnSlice[] {
  const runs = new Map<number, GraphState[]>();
  for (const state of states) {
    const bucket = runs.get(state.representation_run_id) ?? [];
    bucket.push(state);
    runs.set(state.representation_run_id, bucket);
  }
  return [...runs.entries()]
    .map(([runId, runStates]) => {
      const sorted = [...runStates].sort(
        (a, b) =>
          compareYear(wireYear(a.valid_from), wireYear(b.valid_from)) ||
          a.state_id - b.state_id,
      );
      const froms = sorted.map((state) => wireYear(state.valid_from));
      const tos = sorted.map((state) => wireYear(state.valid_to));
      const labels = distinctInOrder(
        sorted.map((state) => state.delivery_column_name).filter(isPresent),
      );
      const variants = distinct(sorted.map((state) => state.variant));
      const valueLabels = distinct(sorted.map(valueSetLabel).filter(isPresent));
      return {
        id: `${nodeId}:run:${runId}`,
        label: compactColumnLabel(labels) ?? `run ${runId + 1}`,
        columnLabels: labels,
        valueSetLabel: valueLabels.join(" / ") || null,
        variant: variants.join(" / "),
        from: minKnown(froms),
        to: maxKnown(tos),
        stateIds: sorted.map((state) => state.state_id),
      };
    })
    .sort(
      (a, b) =>
        compareYear(a.from, b.from) ||
        compareYear(a.to, b.to) ||
        a.label.localeCompare(b.label, "sv"),
    );
}

function coverageFromStates(states: GraphState[]): {
  from: number | null;
  to: number | null;
} {
  return {
    from: minKnown(states.map((state) => wireYear(state.valid_from))),
    to: maxKnown(states.map((state) => wireYear(state.valid_to))),
  };
}

function valueSetLabel(state: GraphState): string | null {
  return (
    state.classification_slug ?? (state.value_set_version_label.trim() || null)
  );
}

function wireYear(value: string | null | undefined): number | null {
  if (!value) {
    return null;
  }
  const year = Number(value.slice(0, 4));
  if (
    !Number.isInteger(year) ||
    year === YEARLESS_START ||
    year >= OPEN_END_YEAR
  ) {
    return null;
  }
  return year;
}

function minKnown(values: (number | null)[]): number | null {
  const known = values.filter(isPresent);
  return known.length > 0 ? Math.min(...known) : null;
}

function maxKnown(values: (number | null)[]): number | null {
  const known = values.filter(isPresent);
  return known.length > 0 ? Math.max(...known) : null;
}

function compareYear(a: number | null, b: number | null): number {
  if (a === b) {
    return 0;
  }
  if (a === null) {
    return -1;
  }
  if (b === null) {
    return 1;
  }
  return a - b;
}

function distinct(values: string[]): string[] {
  return [...new Set(values)].sort((a, b) => a.localeCompare(b, "sv"));
}

function distinctInOrder(values: string[]): string[] {
  return [...new Set(values)];
}

function compactColumnLabel(labels: string[]): string | null {
  if (labels.length === 0) {
    return null;
  }
  if (labels.length === 1) {
    return labels[0];
  }
  if (labels.length <= 3) {
    return labels.join(" · ");
  }
  return `${labels[0]} +${labels.length - 1}`;
}

function isPresent<T>(value: T | null | undefined): value is T {
  return value !== null && value !== undefined;
}
