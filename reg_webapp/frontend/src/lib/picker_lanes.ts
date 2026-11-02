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
  VariantModel,
} from "./api";
import {
  catalogHref,
  facetLabelJoin,
  OPEN_ENDED_VALID_TO,
  type PickerRepresentation,
  pickerRepresentations,
  registerPrefixOf,
  representationInWindow,
  rowCoversColumn,
  valueSetKey,
  YEARLESS_VALID_FROM,
  yearOf,
} from "./catalog";
import type { LaneEdgeModel, LaneModel, LaneSpanModel } from "./LaneGraph";
import { dataAxis, yearWindowLabel } from "./period";
import type { ProjectStudyWindow, RawDraft } from "./project_data";
import {
  committedMarker,
  committedPickerRows,
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
   * read, not pick (a variable's predecessor and successor): from the
   * `focusKey` band when one is set, else from every band. */
  neighbours: boolean;
  focusKey: string | null;
  window: [number, number] | null;
  studyWindow: ProjectStudyWindow | null;
  vintageYear: number;
  /** Years the page draws above the lanes (the coverage row, a group's
   * Timeline), so the lanes share their axis. */
  drawnYears: readonly number[];
  /** The catalog's first and last year: where an unknown start is drawn, and
   * the axis when nothing is dated. */
  catalogSpan: readonly [number, number];
  committed: ReadonlyMap<string, PickerCommittedRow>;
  /** The project, for the neighbour lanes' own "in project" state: a
   * neighbour is read, not picked, but a column of it already in the project
   * says so (#1901). */
  draft: RawDraft | null;
  /** The variants of the neighbours' registers (`variantsByRegister`): the
   * family members a neighbour's column may be in the project under. */
  variantsByRegister: Readonly<Record<string, readonly VariantModel[]>>;
  /** The catalog's names for the lanes' registers (`catalogNames`): what tells
   * a neighbour in another register, or two lanes under one name, apart. */
  names: Readonly<Record<string, string>>;
  stagedAdds: ReadonlySet<string>;
  stagedRemoves: ReadonlySet<string>;
}

/** A band's facets: its whole-variable member's, else its first column's. */
export function bandFacets(band: PickerBand): GroupFacetModel[] {
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

/** Where a span is drawn (DESIGN.md → Time): an unknown start at the
 * catalog's first year, as a still-delivered end at the catalog vintage. A
 * span undated at both ends (year-independent) has no start of its own: null,
 * drawn from the axis start. */
function spanEnds(
  years: Years,
  catalogFirst: number,
  vintageYear: number,
): { from: number | null; to: number | null } {
  const from = years.from ?? (years.to !== null ? catalogFirst : null);
  const to =
    years.to ?? (years.openEnd ? Math.max(from ?? 0, vintageYear) : null);
  return { from, to };
}

/** The years the bands' columns draw on the axis (`spanEnds`), so a page that
 * draws a Timeline over the lanes computes their one axis. */
export function bandAxisYears(
  bands: readonly PickerBand[],
  catalogFirst: number,
  vintageYear: number,
): number[] {
  return bands.flatMap((band) =>
    band.rows.flatMap((row) => {
      const years = rowYears(row);
      if (years === null) return [];
      const { from, to } = spanEnds(years, catalogFirst, vintageYear);
      return [from, to].filter((v): v is number => v !== null);
    }),
  );
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
      if (valueSetKey(states[i]) !== valueSetKey(states[i - 1])) {
        const year = yearOf(states[i].valid_from);
        if (year !== null) years.add(year);
      }
    }
  }
  return [...years].sort((a, b) => a - b);
}

/** A lane's columns as words: the last thing that tells two variables under one
 * name apart, and one a researcher reads in the delivered file. */
function columnsTeller(rows: readonly PickerRepresentation[]): string | null {
  const columns = [...new Set(rows.map((row) => row.column))];
  if (columns.length === 0) return null;
  return `${columns.length === 1 ? "Column" : "Columns"} ${columns.join(", ")}`;
}

function textSub(text: string | null): LaneModel["sub"] {
  return text ? { text } : null;
}

/** The concrete variants a row is delivered under: what a variant-scoped
 * succession edge is matched against. */
function rowVariants(row: PickerRepresentation): string[] {
  return row.variantSegments && row.variantSegments.length > 0
    ? row.variantSegments.map((segment) => segment.variant)
    : [row.variant];
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
  // A group page focused on a member draws that member's neighbours only, as
  // its own variable page did (#1909).
  const nearKeys =
    input.focusKey !== null && bandKeys.has(input.focusKey)
      ? new Set([input.focusKey])
      : bandKeys;
  const neighbours: VariableGraphNode[] = [];
  if (input.neighbours) {
    const seen = new Set<string>();
    for (const edge of input.graph?.edges ?? []) {
      if (edge.kind !== "succession") continue;
      const [near, far] = nearKeys.has(edge.source)
        ? [edge.source, edge.target]
        : [edge.target, edge.source];
      if (!nearKeys.has(near) || bandKeys.has(far) || seen.has(far)) continue;
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
  // Span id → the row it draws, for every lane (`rows` holds only the ones a
  // tick reaches).
  const spanRows = new Map<string, PickerRepresentation>();
  const placedYears: Years[] = [];
  const draft: {
    lane: LaneModel;
    band: PickerBand;
    rows: PickerRepresentation[];
    selectable: boolean;
    /** What could tell this lane from another under the same name, best
     * first: never its slug, which names neither a column nor a variable. */
    tellers: (string | null | undefined)[];
  }[] = [];
  const registerName = (fqid: string) => {
    const register = registerPrefixOf(fqid);
    return input.names[register] ?? null;
  };

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
    draft.push({
      lane,
      band,
      rows: band.rows,
      selectable: true,
      tellers: [
        band.operationalDefinition,
        band.facetLabel,
        registerName(band.key),
        columnsTeller(band.rows),
      ],
    });
    lanesOfFqid.set(band.key, [...(lanesOfFqid.get(band.key) ?? []), lane.id]);
  }
  const bandRegisters = new Set(
    ordered.map((band) => registerPrefixOf(band.key)),
  );
  const neighbourBands: PickerBand[] = [];
  for (const node of neighbours) {
    const band: PickerBand = {
      key: node.fqid,
      name: node.label,
      registerPrefix: registerPrefixOf(node.fqid),
      rows: pickerRepresentations(node.states),
    };
    neighbourBands.push(band);
    const lane: LaneModel = {
      id: node.fqid,
      name: node.label,
      // A neighbour in another register says which; in the page's own register
      // it needs no line.
      sub: bandRegisters.has(band.registerPrefix)
        ? null
        : textSub(registerName(node.fqid)),
      href: catalogHref(node.fqid),
      relations: [],
    };
    draft.push({
      lane,
      band,
      rows: band.rows,
      selectable: false,
      tellers: [
        node.operational_definition,
        node.facets.length > 0 ? facetLabelJoin(node.facets) : null,
        registerName(node.fqid),
        columnsTeller(band.rows),
      ],
    });
    lanesOfFqid.set(node.fqid, [lane.id]);
  }
  // A neighbour is in the project the way a band is, through a source of its
  // family, whose members its register's variants name; it is still only read
  // here, never ticked.
  const neighbourCommitted = new Map<string, PickerCommittedRow>();
  for (const register of new Set(neighbourBands.map((b) => b.registerPrefix))) {
    const rows = committedPickerRows(
      input.draft,
      neighbourBands.filter((b) => b.registerPrefix === register),
      input.variantsByRegister[register] ?? [],
    );
    for (const [key, row] of rows) neighbourCommitted.set(key, row);
  }
  const committedOf = (key: string) =>
    input.committed.get(key) ?? neighbourCommitted.get(key);

  // Two lanes under one name say which variable each is, by the first thing
  // that tells every one of them apart.
  const shown = (lane: LaneModel) =>
    `${lane.name}\u0000${lane.sub?.text ?? ""}`;
  const alike = new Map<string, typeof draft>();
  for (const entry of draft) {
    const key = shown(entry.lane);
    alike.set(key, [...(alike.get(key) ?? []), entry]);
  }
  for (const group of alike.values()) {
    if (group.length < 2) continue;
    const slots = group[0].tellers.length;
    for (let i = 0; i < slots; i += 1) {
      const texts = group.map((entry) => entry.tellers[i]?.trim() || "");
      if (texts.every(Boolean) && new Set(texts).size === texts.length) {
        group.forEach((entry, j) => {
          entry.lane.sub = { text: texts[j] };
        });
        break;
      }
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
      // Membership is looked up for every lane; whether a lane can be ticked
      // is a separate matter (#1901).
      const committed = committedOf(pickerRowKey(band, row));
      spanRows.set(key, row);
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
  // with the active window and the study window, rounded out. A
  // still-delivered column ends at the catalog vintage, never at the rounded
  // axis end.
  const [catalogFirst] = input.catalogSpan;
  const dated = [
    ...input.drawnYears,
    ...placedYears.flatMap((y) => {
      const { from, to } = spanEnds(y, catalogFirst, input.vintageYear);
      return [from, to].filter((v): v is number => v !== null);
    }),
  ];
  if (input.window) dated.push(...input.window);
  const axis = dataAxis(dated, input.studyWindow, input.catalogSpan);

  for (const { span, years, parts } of pending) {
    const at = spanEnds(years, catalogFirst, input.vintageYear);
    span.from = at.from ?? axis[0];
    span.to = at.to ?? axis[1];
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
      // A variant-scoped edge holds only where that variant's columns are
      // drawn: it reaches no span delivered under another variant.
      const candidates = spans.filter((s) => {
        if (!laneIds.includes(s.lane)) return false;
        const row = spanRows.get(s.id);
        return (
          !edge.variant || (!!row && rowVariants(row).includes(edge.variant))
        );
      });
      const named = column
        ? candidates.filter((s) => {
            const row = spanRows.get(s.id);
            return row ? rowCoversColumn(row, column) : s.name === column;
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
    edges.push({
      id: edge.id,
      from: from.id,
      to: to.id,
      kind: "succession",
      year: edge.effective_year ?? null,
    });
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
