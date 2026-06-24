import type {
  BindingNodeData,
  ClassificationNodeData,
  ConceptGroup,
  ConceptGroupNodeData,
  RelatedRefModel,
  VariableStateModel,
} from "./api";

export type HistoryGraphMode = "variable" | "group" | "classification";

export type HistoryGraphNodeKind =
  | "variable"
  | "group"
  | "group-member"
  | "classification";

export type HistoryGraphEdgeKind =
  | "succession"
  | "related"
  | "lineage"
  | "member";

export interface HistoryColumnSlice {
  id: string;
  label: string;
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
  detail?: string;
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
  warnings: string[];
  nodeGrain: "entity-with-column-slices";
  dataContract: "client-stitch-prototype";
}

const OPEN_END_YEAR = 9999;
const YEARLESS_START = 1;

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

function fqidFromRelated(ref: RelatedRefModel): string | null {
  return ref.fqid ?? `${ref.provider}/${ref.register}/${ref.variable}`;
}

function leafSlug(fqid: string | null): string {
  return fqid?.split("/").at(-1) ?? "";
}

function columnLabel(state: VariableStateModel): string {
  return state.delivery_column_name ?? `state ${state.state_id}`;
}

function columnSlices(states: VariableStateModel[]): HistoryColumnSlice[] {
  const byColumn = new Map<string, HistoryColumnSlice>();
  for (const state of states) {
    const label = columnLabel(state);
    const key = `${state.variant}:${label}`;
    const existing = byColumn.get(key);
    const from = wireYear(state.valid_from);
    const to = wireYear(state.valid_to);
    if (existing) {
      existing.from =
        existing.from === null || from === null
          ? (existing.from ?? from)
          : Math.min(existing.from, from);
      existing.to =
        existing.to === null || to === null ? null : Math.max(existing.to, to);
      existing.stateIds.push(state.state_id);
    } else {
      byColumn.set(key, {
        id: key,
        label,
        variant: state.variant,
        from,
        to,
        stateIds: [state.state_id],
      });
    }
  }
  return [...byColumn.values()].sort((a, b) =>
    a.label.localeCompare(b.label, "sv"),
  );
}

function coverageFromStates(states: VariableStateModel[]): {
  from: number | null;
  to: number | null;
} {
  let from: number | null = null;
  let to: number | null = null;
  for (const state of states) {
    const stateFrom = wireYear(state.valid_from);
    const stateTo = wireYear(state.valid_to);
    if (stateFrom !== null) {
      from = from === null ? stateFrom : Math.min(from, stateFrom);
    }
    if (stateTo === null) {
      to = null;
    } else if (to !== null) {
      to = Math.max(to, stateTo);
    } else if (!states.some((s) => wireYear(s.valid_to) === null)) {
      to = stateTo;
    }
  }
  return { from, to };
}

function dimensionHint(dimensions: ConceptGroup[]): string | undefined {
  const axes = new Set<string>();
  for (const group of dimensions) {
    for (const member of group.members) {
      for (const facet of member.facets) {
        axes.add(facet.axis);
      }
    }
  }
  return axes.size > 0 ? `facets: ${[...axes].sort().join(", ")}` : undefined;
}

function classificationLabel(edition: {
  name?: string | null;
  slug: string;
}): string {
  const name = edition.name ?? edition.slug;
  const suffix = name.split(" — ").at(-1);
  const year = classificationSlugYear(edition.slug)?.toString();
  if (suffix && suffix !== name && year) {
    return `${year} · ${suffix}`;
  }
  return edition.slug
    .replaceAll("-", " ")
    .replace(/([A-Za-zÅÄÖåäö]+)(\d{4})$/, "$1 $2")
    .replace(/^([a-zåäö]+)(?= )/, (prefix) => prefix.toUpperCase());
}

function classificationSlugYear(slug: string): number | null {
  const year = Number(slug.match(/(\d{4})$/)?.[1]);
  return Number.isInteger(year) ? year : null;
}

export function historyGraphFromBinding(
  node: BindingNodeData,
  dimensions: ConceptGroup[] = [],
): HistoryGraph {
  const nodes = new Map<string, HistoryGraphNode>();
  const edges: HistoryGraphEdge[] = [];
  const warnings: string[] = [];
  const currentId = node.fqid;
  const currentCoverage = coverageFromStates(node.states);
  const currentColumns = columnSlices(node.states);
  const chain = node.succession_chain ?? [];
  const dimensionDetail = dimensionHint(dimensions);

  if (chain.length > 0) {
    for (let i = 0; i < chain.length; i += 1) {
      const edition = chain[i];
      const id =
        edition.fqid ??
        `${edition.provider}/${edition.register}/${edition.variable}`;
      const isViewed = edition.fqid === currentId || edition.is_self;
      nodes.set(id, {
        id,
        kind: "variable",
        label: edition.name ?? edition.variable,
        fqid: edition.fqid,
        from: isViewed ? currentCoverage.from : null,
        to: isViewed ? currentCoverage.to : edition.effective_year,
        current: edition.is_current,
        self: edition.is_self,
        columns: isViewed ? currentColumns : [],
        detail: isViewed ? dimensionDetail : undefined,
      });
      if (i > 0) {
        const previous = chain[i - 1];
        const previousId =
          previous.fqid ??
          `${previous.provider}/${previous.register}/${previous.variable}`;
        edges.push({
          id: `succession:${previousId}->${id}`,
          kind: "succession",
          from: previousId,
          to: id,
          fromYear: previous.effective_year,
          toYear: previous.effective_year,
          label: previous.reason,
        });
      }
    }
  } else {
    nodes.set(currentId, {
      id: currentId,
      kind: "variable",
      label: node.name ?? leafSlug(node.fqid),
      fqid: node.fqid,
      from: currentCoverage.from,
      to: currentCoverage.to,
      current: true,
      self: true,
      columns: currentColumns,
      detail: dimensionDetail,
    });
  }

  if (!nodes.has(currentId)) {
    nodes.set(currentId, {
      id: currentId,
      kind: "variable",
      label: node.name ?? leafSlug(node.fqid),
      fqid: node.fqid,
      from: currentCoverage.from,
      to: currentCoverage.to,
      current: true,
      self: true,
      columns: currentColumns,
      detail: dimensionDetail,
    });
  }

  for (const related of node.related_to) {
    const relatedId = fqidFromRelated(related);
    if (relatedId === null) {
      continue;
    }
    if (!nodes.has(relatedId)) {
      nodes.set(relatedId, {
        id: relatedId,
        kind: "variable",
        label: related.variable,
        fqid: related.fqid,
        from: null,
        to: null,
        columns: [],
      });
    }
    edges.push({
      id: `related:${currentId}->${relatedId}:${related.relation_kind}`,
      kind: "related",
      from: currentId,
      to: relatedId,
      fromYear: null,
      toYear: null,
      label: related.relation_kind,
    });
  }

  for (const edge of node.lineage) {
    const sourceId = edge.source_fqid ?? `source-state:${edge.source_state_id}`;
    if (!nodes.has(sourceId)) {
      nodes.set(sourceId, {
        id: sourceId,
        kind: "variable",
        label: edge.source_fqid ? leafSlug(edge.source_fqid) : sourceId,
        fqid: edge.source_fqid ?? null,
        from: null,
        to: null,
        columns: [],
      });
    }
    edges.push({
      id: `lineage:${edge.source_state_id}->${edge.consumer_state_id}`,
      kind: "lineage",
      from: sourceId,
      to: currentId,
      fromYear: wireYear(edge.valid_from),
      toYear: wireYear(edge.valid_to),
      label: "source",
    });
  }

  if (currentColumns.length > 1) {
    warnings.push(
      "Monthly and multi-column variables need column slices inside the variable node; column-as-node would explode dense families.",
    );
  }
  if (chain.some((edition) => edition.fqid !== currentId)) {
    warnings.push(
      "Client stitching only has delivery states for the viewed binding; predecessor/successor column windows need a graph payload.",
    );
  }
  if (node.related_to.length > 0) {
    warnings.push(
      "related_to has no validity window in the current client payload, so related edges are unbounded.",
    );
  }
  if (node.lineage.length > 0) {
    warnings.push(
      "Lineage edges identify source states but not their delivery columns; backend graph can attach source-state shape once.",
    );
  }

  return {
    mode: "variable",
    title: "History graph prototype",
    nodes: [...nodes.values()],
    edges,
    warnings,
    nodeGrain: "entity-with-column-slices",
    dataContract: "client-stitch-prototype",
  };
}

export function historyGraphFromGroup(
  node: ConceptGroupNodeData,
): HistoryGraph {
  const groupId = `group:${node.key}`;
  const nodes: HistoryGraphNode[] = [
    {
      id: groupId,
      kind: "group",
      label: node.label,
      fqid: null,
      from: null,
      to: null,
      columns: [],
      detail:
        node.axes.length > 0 ? `facets: ${node.axes.join(", ")}` : undefined,
    },
  ];
  const edges: HistoryGraphEdge[] = [];

  for (const member of node.members) {
    const coverage = member.coverage;
    const detail =
      member.facets.length > 0
        ? member.facets.map((f) => `${f.axis}: ${f.label}`).join(" · ")
        : undefined;
    nodes.push({
      id: member.fqid,
      kind: "group-member",
      label: member.name ?? leafSlug(member.fqid),
      fqid: member.fqid,
      from: wireYear(coverage?.coverage_from),
      to: coverage?.open_ended ? null : wireYear(coverage?.coverage_to),
      columns: [],
      detail,
    });
    edges.push({
      id: `member:${groupId}->${member.fqid}`,
      kind: "member",
      from: groupId,
      to: member.fqid,
      fromYear: wireYear(coverage?.coverage_from),
      toYear: coverage?.open_ended ? null : wireYear(coverage?.coverage_to),
      label: "member",
    });
  }

  return {
    mode: "group",
    title: "History graph prototype",
    nodes,
    edges,
    warnings: [
      "Group pages only carry members and coverage today; all-member succession, related, and lineage edges would require N+1 leaf fetches without a backend graph payload.",
    ],
    nodeGrain: "entity-with-column-slices",
    dataContract: "client-stitch-prototype",
  };
}

export function historyGraphFromClassification(
  node: ClassificationNodeData,
): HistoryGraph {
  const chain = node.edition_chain ?? [];
  const editions =
    chain.length > 0
      ? chain
      : [
          {
            slug: leafSlug(node.fqid),
            fqid: node.fqid,
            name: node.name,
            effective_year: null,
            is_current: true,
            is_self: true,
          },
        ];
  const fanOut = editions.filter((edition) => edition.is_current).length > 1;
  const fanOutKnownYears = editions
    .map((edition) => edition.effective_year)
    .filter((year): year is number => year !== null);
  const fanOutCurrentYear =
    fanOutKnownYears.length > 0 ? Math.max(...fanOutKnownYears) : null;
  const nodes: HistoryGraphNode[] = editions.map((edition, i) => {
    const previousCut = i > 0 ? editions[i - 1].effective_year : null;
    const fallbackYear =
      classificationSlugYear(edition.slug) ??
      edition.effective_year ??
      previousCut ??
      fanOutCurrentYear;
    return {
      id: edition.fqid ?? `class/${edition.slug}`,
      kind: "classification",
      label: edition.slug,
      fqid: edition.fqid,
      from: fallbackYear,
      to: fallbackYear,
      current: edition.is_current,
      self: edition.is_self,
      columns: [],
      detail: edition.name ?? classificationLabel(edition),
    };
  });
  const edges: HistoryGraphEdge[] = [];
  if (!fanOut) {
    for (let i = 1; i < editions.length; i += 1) {
      const previous = editions[i - 1];
      const current = editions[i];
      edges.push({
        id: `classification:${previous.slug}->${current.slug}`,
        kind: "succession",
        from: previous.fqid ?? `class/${previous.slug}`,
        to: current.fqid ?? `class/${current.slug}`,
        fromYear: previous.effective_year,
        toYear: previous.effective_year,
        label: null,
      });
    }
  }

  return {
    mode: "classification",
    title: "Classification editions",
    nodes,
    edges,
    warnings: fanOut
      ? [
          "Flattened client chain; true branch edges need a backend graph payload.",
        ]
      : [],
    nodeGrain: "entity-with-column-slices",
    dataContract: "client-stitch-prototype",
  };
}

export function historyGraphYears(graph: HistoryGraph): {
  min: number;
  max: number;
} {
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
  for (const edge of graph.edges) {
    if (edge.fromYear !== null) {
      years.push(edge.fromYear);
    }
    if (edge.toYear !== null) {
      years.push(edge.toYear);
    }
  }
  if (years.length === 0) {
    const year = new Date().getFullYear();
    return { min: year - 2, max: year + 2 };
  }
  const min = Math.min(...years);
  const max = Math.max(...years);
  return min === max ? { min: min - 1, max: max + 1 } : { min, max };
}
