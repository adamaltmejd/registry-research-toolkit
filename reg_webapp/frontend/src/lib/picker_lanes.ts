/**
 * The representation picker's bands, the project and the staged diff, read as
 * a LaneGraph: one lane per variable, one span per picker row. Pure, so the
 * picker component holds only state and controls.
 *
 * Relations: a `succession` edge of the relationship graph between two lanes is
 * a solid arrow. The wire carries succession only; a rename is the picker's
 * own #902 fold (one row whose columns follow each other), drawn as that row's
 * named eras joined by a dashed hop.
 */
import type {
  GroupFacetModel,
  RelationshipGraph,
  VariableGraphNode,
} from "./api";
import {
  catalogHref,
  leafSlug,
  OPEN_ENDED_VALID_TO,
  type PickerRepresentation,
  pickerRepresentations,
  representationInWindow,
  rowCoversColumn,
  YEARLESS_VALID_FROM,
  yearOf,
} from "./catalog";
import type { LaneEdgeModel, LaneModel, LaneSpanModel } from "./LaneGraph";
import { dataAxis, yearWindowLabel } from "./period";
import type { ProjectStudyWindow } from "./project_data";
import {
  committedMarker,
  type PickerCommittedRow,
  pickerRowKey,
  type StagedPickerBand,
} from "./staged_picker";

/** One variable the picker lists: its identity and its delivery-column rows.
 * `key` is the variable's FQID, unique across the picker. */
export interface PickerBand extends StagedPickerBand {
  name: string;
  /** The member's facets in its group, joined ("Familj, Inkl."). */
  facetLabel?: string | null;
  /** Per delivery column, the facets of the group member that column is. */
  facetsByColumn?: Record<string, GroupFacetModel[]>;
  /** The facets of a whole-variable member: they apply to every row. */
  facets?: GroupFacetModel[];
  /** What tells parallel members apart where their names coincide. */
  operationalDefinition?: string | null;
  /** The member's own page; absent on the page that is this variable. */
  href?: string;
  /** The lane's id where one variable gives several lanes (a group's
   * members by column); defaults to `key`. */
  laneKey?: string;
}

export interface PickerLanes {
  lanes: LaneModel[];
  spans: LaneSpanModel[];
  edges: LaneEdgeModel[];
  axis: [number, number];
  /** Span id → the band and row it toggles. */
  rows: Map<string, { band: PickerBand; row: PickerRepresentation }>;
}

export interface PickerLaneInput {
  /** In member order, each with the rows the filters leave. */
  bands: readonly PickerBand[];
  axes: readonly { name: string }[];
  graph: RelationshipGraph | null;
  /** Draw the graph's other variables that a relation reaches as lanes to
   * read, not pick (the binding leaf: its predecessor and successor). */
  neighbours: boolean;
  focusKey: string | null;
  window: [number, number] | null;
  studyWindow: ProjectStudyWindow | null;
  vintageYear: number;
  /** Years the page draws above the lanes (the coverage row, a group's
   * Timeline), so the lanes share their axis. */
  drawnYears: readonly number[];
  /** The catalog's first and last year: the axis when nothing is dated. */
  catalogSpan: readonly [number, number];
  committed: ReadonlyMap<string, PickerCommittedRow>;
  stagedAdds: ReadonlySet<string>;
  stagedRemoves: ReadonlySet<string>;
}

function bandFacets(band: PickerBand): GroupFacetModel[] {
  return band.facets ?? Object.values(band.facetsByColumn ?? {})[0] ?? [];
}

/** Lanes ordered by facet (DESIGN.md → Time): by each axis in turn, values in
 * the order the members first carry them; a stable sort, so member order breaks
 * ties. */
function orderByFacet(
  bands: readonly PickerBand[],
  axes: readonly { name: string }[],
): PickerBand[] {
  if (axes.length === 0) return [...bands];
  const rank = new Map<string, number>();
  for (const band of bands) {
    for (const facet of bandFacets(band)) {
      const key = `${facet.axis}\u0000${facet.value}`;
      if (!rank.has(key)) rank.set(key, rank.size);
    }
  }
  const tuple = (band: PickerBand) =>
    axes.map((axis) => {
      const facet = bandFacets(band).find((f) => f.axis === axis.name);
      return facet
        ? (rank.get(`${facet.axis}\u0000${facet.value}`) ?? 0)
        : Number.MAX_SAFE_INTEGER;
    });
  return [...bands].sort((a, b) => {
    const ta = tuple(a);
    const tb = tuple(b);
    for (let i = 0; i < ta.length; i += 1) {
      if (ta[i] !== tb[i]) return ta[i] - tb[i];
    }
    return 0;
  });
}

interface Years {
  from: number | null;
  to: number | null;
  openEnd: boolean;
}

function rowYears(row: PickerRepresentation): Years | null {
  if (row.period_scope === "year_independent") {
    return { from: null, to: null, openEnd: true };
  }
  if (row.from === null || row.to === null || row.from > row.to) return null;
  return {
    from: row.from === YEARLESS_VALID_FROM ? null : yearOf(row.from),
    to: row.to === OPEN_ENDED_VALID_TO ? null : yearOf(row.to),
    openEnd: row.to === OPEN_ENDED_VALID_TO,
  };
}

function yearsLabel(row: PickerRepresentation, years: Years): string {
  if (row.period_scope === "year_independent") return "year-independent";
  if (years.from === null && years.to === null) return "undated";
  if (years.from === null) return `–${years.to}`;
  if (years.to === null) return `${years.from}–`;
  return yearWindowLabel({ from: years.from, to: years.to });
}

/** The years a column's coding changed under the same name: a new value set
 * between two consecutive states. */
function codingChanges(
  node: VariableGraphNode | undefined,
  row: PickerRepresentation,
): number[] {
  if (!node || !row.codingsVary) return [];
  const years = new Set<number>();
  for (const column of [row.column, ...row.renamedColumns]) {
    const states = node.states
      .filter((s) => s.delivery_column_name === column && s.valid_from)
      .sort((a, b) => (a.valid_from ?? "").localeCompare(b.valid_from ?? ""));
    for (let i = 1; i < states.length; i += 1) {
      if (states[i].value_set_id !== states[i - 1].value_set_id) {
        const year = yearOf(states[i].valid_from);
        if (year !== null) years.add(year);
      }
    }
  }
  return [...years].sort((a, b) => a - b);
}

function graphNodes(
  graph: RelationshipGraph | null,
): Map<string, VariableGraphNode> {
  const byFqid = new Map<string, VariableGraphNode>();
  for (const node of graph?.nodes ?? []) {
    if (node.kind !== "variable") continue;
    byFqid.set(node.fqid, node);
    for (const alias of node.same_as) byFqid.set(alias.fqid, node);
  }
  return byFqid;
}

export function pickerLanes(input: PickerLaneInput): PickerLanes {
  const nodes = graphNodes(input.graph);
  const ordered = orderByFacet(input.bands, input.axes);
  const bandKeys = new Set(ordered.map((band) => band.key));

  // The variables a relation reaches from a band, drawn as lanes to read.
  const neighbours: VariableGraphNode[] = [];
  if (input.neighbours) {
    const seen = new Set<string>();
    for (const edge of input.graph?.edges ?? []) {
      if (edge.kind !== "succession") continue;
      const [near, far] = bandKeys.has(edge.source)
        ? [edge.source, edge.target]
        : [edge.target, edge.source];
      if (!bandKeys.has(near) || bandKeys.has(far) || seen.has(far)) continue;
      const node = nodes.get(far);
      if (node && node.states.length > 0) {
        seen.add(far);
        neighbours.push(node);
      }
    }
  }

  const lanes: LaneModel[] = [];
  const spans: LaneSpanModel[] = [];
  const rows = new Map<
    string,
    { band: PickerBand; row: PickerRepresentation }
  >();
  // Every lane of a variable: a group gives one per member column.
  const lanesOfFqid = new Map<string, string[]>();
  const placedYears: Years[] = [];
  const draft: {
    lane: LaneModel;
    band: PickerBand;
    rows: PickerRepresentation[];
    selectable: boolean;
  }[] = [];

  for (const band of ordered) {
    const lane: LaneModel = {
      id: band.laneKey ?? band.key,
      name: band.facetLabel || band.name,
      sub: band.operationalDefinition
        ? { text: band.operationalDefinition }
        : band.facetLabel && band.facetLabel !== band.name
          ? { text: band.name }
          : null,
      href: band.href ?? null,
      current: input.focusKey === band.key,
      relations: [],
    };
    draft.push({ lane, band, rows: band.rows, selectable: true });
    lanesOfFqid.set(band.key, [...(lanesOfFqid.get(band.key) ?? []), lane.id]);
  }
  for (const node of neighbours) {
    const band: PickerBand = {
      key: node.fqid,
      name: node.label,
      registerPrefix: "",
      rows: pickerRepresentations(node.states),
    };
    const lane: LaneModel = {
      id: node.fqid,
      name: node.label,
      sub: { text: leafSlug(node.fqid), mono: true },
      href: catalogHref(node.fqid),
      relations: [],
    };
    draft.push({ lane, band, rows: band.rows, selectable: false });
    lanesOfFqid.set(node.fqid, [lane.id]);
  }

  // Two lanes under one name say which variable each is.
  const names = new Map<string, number>();
  for (const { lane } of draft) {
    const key = `${lane.name}\u0000${lane.sub?.text ?? ""}`;
    names.set(key, (names.get(key) ?? 0) + 1);
  }
  for (const { lane } of draft) {
    if ((names.get(`${lane.name}\u0000${lane.sub?.text ?? ""}`) ?? 0) > 1) {
      lane.sub = { text: leafSlug(lane.id), mono: true };
    }
  }

  // The bands first, directly under the Timeline's coverage row they share a
  // variable with; then the neighbours in time, by their first year.
  const firstYear = (rowsOf: PickerRepresentation[]) =>
    Math.min(
      ...rowsOf.map((row) => rowYears(row)?.from ?? Number.MAX_SAFE_INTEGER),
    );
  const stack = [
    ...draft.filter((d) => d.selectable),
    ...draft
      .filter((d) => !d.selectable)
      .sort((a, b) => firstYear(a.rows) - firstYear(b.rows)),
  ];

  const pending: {
    span: LaneSpanModel;
    years: Years;
    /** A rename's named eras, or an interrupted series' deliveries. */
    parts: { name: string | null; from: string; to: string }[] | undefined;
  }[] = [];
  for (const { lane, band, rows: laneRows, selectable } of stack) {
    lanes.push(lane);
    const undelivered: string[] = [];
    for (const row of laneRows) {
      const years = rowYears(row);
      if (years === null) {
        undelivered.push(row.column);
        continue;
      }
      placedYears.push(years);
      const key = selectable
        ? pickerRowKey(band, row)
        : `${band.laneKey ?? band.key}::${row.key}`;
      const committed = selectable ? input.committed.get(key) : undefined;
      const staged = input.stagedAdds.has(key)
        ? ("add" as const)
        : input.stagedRemoves.has(key)
          ? ("remove" as const)
          : null;
      const inWindow = representationInWindow(row, input.window);
      const marker = committed
        ? committedMarker(committed.period, input.studyWindow)
        : null;
      const label = yearsLabel(row, years);
      const changes = codingChanges(nodes.get(band.key), row);
      // A folded rename reads as drawn: each name over its own years.
      const eraWords = row.renameEras?.map((era) => {
        const eraYears = rowYears({ ...row, from: era.from, to: era.to });
        return `${era.column} ${eraYears ? yearsLabel(row, eraYears) : ""}`.trim();
      });
      const words = [
        eraWords ? eraWords.join(", then ") : `${row.column} ${label}`,
        lane.name,
        changes.length > 0 ? `coding changed in ${changes.join(", ")}` : null,
        staged === "add"
          ? "will be added"
          : staged === "remove"
            ? "will be removed"
            : marker
              ? `${marker.label}${marker.detail ? ` ${marker.detail}` : ""}`
              : null,
        inWindow ? null : "outside the window",
      ];
      const canToggle = selectable && (row.selectable !== false || !!committed);
      if (canToggle) rows.set(key, { band, row });
      pending.push({
        years,
        parts:
          row.renameEras?.map((era) => ({ ...era, name: era.column })) ??
          (row.windows.length > 1
            ? row.windows.map((w) => ({ ...w, name: null }))
            : undefined),
        span: {
          id: key,
          lane: lane.id,
          name: row.column,
          from: 0,
          to: 0,
          years: label,
          state:
            committed && staged !== "remove"
              ? "in"
              : inWindow
                ? "default"
                : "out",
          staged,
          blocked: marker?.tone === "error",
          note:
            marker?.detail && staged !== "remove"
              ? {
                  text: `${marker.label} ${marker.detail}`,
                  tone: marker.tone === "error" ? "error" : "info",
                }
              : null,
          changes,
          selectable: canToggle,
          isSelected: committed ? staged !== "remove" : staged === "add",
          accessibleName: words.filter(Boolean).join(", "),
        },
      });
    }
    if (undelivered.length > 0) lane.undelivered = undelivered;
  }

  // The axis (DESIGN.md → Time): the years drawn here and above the lanes,
  // with the study window, rounded out. A still-delivered column ends at the
  // catalog vintage, never at the rounded axis end.
  const dated = [
    ...input.drawnYears,
    ...placedYears.flatMap((y) =>
      [y.from, y.to].filter((v): v is number => v !== null),
    ),
  ];
  if (placedYears.some((y) => y.openEnd)) dated.push(input.vintageYear);
  const axis = dataAxis(dated, input.studyWindow, input.catalogSpan);

  for (const { span, years, parts } of pending) {
    span.from = years.from ?? axis[0];
    span.to =
      years.to ??
      (years.openEnd ? Math.max(span.from, input.vintageYear) : axis[1]);
    span.segments = parts?.map((era) => ({
      name: era.name,
      from:
        era.from === YEARLESS_VALID_FROM
          ? span.from
          : (yearOf(era.from) ?? span.from),
      to:
        era.to === OPEN_ENDED_VALID_TO ? span.to : (yearOf(era.to) ?? span.to),
    }));
    spans.push(span);
  }

  // Succession edges between two drawn lanes, span end to span start.
  const edges: LaneEdgeModel[] = [];
  const laneNames = new Map(lanes.map((lane) => [lane.id, lane.name]));
  for (const edge of input.graph?.edges ?? []) {
    if (edge.kind !== "succession") continue;
    const fromLanes = lanesOfFqid.get(edge.source);
    const toLanes = lanesOfFqid.get(edge.target);
    if (!fromLanes || !toLanes) continue;
    // The edge's column picks the span, and so the lane, across every lane
    // of the variable.
    const pick = (
      laneIds: string[],
      column: string | null | undefined,
      last: boolean,
    ) => {
      const candidates = spans.filter((s) => laneIds.includes(s.lane));
      const named = column
        ? candidates.filter((s) => {
            const entry = rows.get(s.id);
            return entry
              ? rowCoversColumn(entry.row, column)
              : s.name === column;
          })
        : [];
      const pool = named.length > 0 ? named : candidates;
      return [...pool].sort((a, b) =>
        last ? b.to - a.to : a.from - b.from,
      )[0];
    };
    const from = pick(fromLanes, edge.source_column, true);
    const to = pick(toLanes, edge.target_column, false);
    if (!from || !to || from.id === to.id) continue;
    const fromLane = from.lane;
    const toLane = to.lane;
    edges.push({ id: edge.id, from: from.id, to: to.id, kind: "succession" });
    const year = edge.effective_year ? ` in ${edge.effective_year}` : "";
    if (fromLane !== toLane) {
      lanes
        .find((l) => l.id === fromLane)
        ?.relations?.push(`Succeeded by ${laneNames.get(toLane)}${year}`);
      lanes
        .find((l) => l.id === toLane)
        ?.relations?.push(`Succeeds ${laneNames.get(fromLane)}${year}`);
    } else {
      lanes
        .find((l) => l.id === fromLane)
        ?.relations?.push(`${from.name} succeeded by ${to.name}${year}`);
    }
  }

  return { lanes, spans, edges, axis, rows };
}
