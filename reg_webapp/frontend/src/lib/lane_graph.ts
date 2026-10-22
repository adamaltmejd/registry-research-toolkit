/**
 * The geometry of a `LaneGraph` (DESIGN.md → Time): lanes stacked down the
 * page, spans placed on one shared year axis, edges between span ends. Pure:
 * the component measures its width and draws what this returns, so the same
 * input at the same width always lays out the same way.
 *
 * A span keeps its true years. When its name does not fit inside, the name is
 * set beside it, and packing reserves that room, so a label never covers
 * another span. Spans of one lane that would collide move to a further row of
 * that lane.
 */

export interface LaneSegment {
  /** The column name this part was delivered under; null repeats the span's. */
  name: string | null;
  from: number;
  /** Inclusive. */
  to: number;
}

export interface LaneSpanGeometryInput {
  id: string;
  lane: string;
  /** The column name, the text set on the span. */
  name: string;
  /** Inclusive years on the axis. */
  from: number;
  to: number;
  /** Disjoint parts of the span: the eras of a rename, or the deliveries of an
   * interrupted series. One part spanning `from`..`to` when absent. */
  segments?: LaneSegment[];
}

export type LaneEdgeKind = "succession" | "rename";

export interface LaneEdgeInput {
  id: string;
  from: string;
  to: string;
  kind: LaneEdgeKind;
}

export interface LaneGeometryInput {
  lanes: readonly { id: string }[];
  spans: readonly LaneSpanGeometryInput[];
  edges: readonly LaneEdgeInput[];
  /** Inclusive first and last year of the axis. */
  axis: [number, number];
  window: [number, number] | null;
  /** The graph's measured width in px. */
  width: number;
}

export interface PlacedSegment {
  name: string | null;
  /** Relative to the span's own left edge. */
  x: number;
  width: number;
}

export interface PlacedSpan {
  id: string;
  x: number;
  y: number;
  width: number;
  segments: PlacedSegment[];
  /** Where the name sits: inside the span, or beside it. */
  label: "inside" | "right" | "left";
}

export interface PlacedLane {
  id: string;
  y: number;
  height: number;
}

export interface LaneGeometry {
  /** True when the lane names sit above their spans instead of beside them. */
  stacked: boolean;
  /** Where the track begins and how wide it is. */
  trackX: number;
  trackWidth: number;
  height: number;
  axisY: number;
  lanes: PlacedLane[];
  spans: Map<string, PlacedSpan>;
  /** `label` and `anchor` are set on the one label that stands for both
   * window bounds when they are too close to label apart. */
  ticks: {
    year: number;
    x: number;
    inWindow: boolean;
    label?: string;
    anchor?: "start" | "end";
  }[];
  window: { x: number; width: number } | null;
  /** Drawn from a span's end to the next one's start. A span folding a
   * rename also gets one dashed edge per change of name inside it. */
  edges: {
    id: string;
    from: string;
    to: string;
    kind: LaneEdgeKind;
    d: string;
  }[];
}

// The lane metrics of the mockup: a 58 px lane (`--lane-height`) holding one
// 24 px span, and a further 32 px for each further row.
export const LANE_HEIGHT = 58;
export const SPAN_HEIGHT = 24;
const ROW_STEP = 32;
const AXIS_HEIGHT = 28;
// Above this width the names take the `--col-list` column beside the track.
const NAME_WIDTH = 230;
const NAME_GAP = 16;
const MIN_TRACK = 360;
const STACKED_NAME_HEIGHT = 38;
// IBM Plex Mono advances 0.6 em; the `column` type is 12 px. Set in mono, a
// name's width follows from its length without measuring the DOM.
const COLUMN_CHAR = 7.2;
const LABEL_PAD = 14;
const LABEL_GAP = 4;
const PACK_GAP = 6;
const MIN_SPAN = 6;

export function labelWidth(name: string): number {
  return name.length * COLUMN_CHAR + LABEL_PAD;
}

/** Tick years at a step that keeps labels about 48 px apart. */
function tickYears(axis: [number, number], pxPerYear: number): number[] {
  const step = [1, 2, 5, 10, 20, 50].find((s) => s * pxPerYear >= 48) ?? 100;
  const years: number[] = [];
  for (let y = Math.ceil(axis[0] / step) * step; y <= axis[1]; y += step) {
    years.push(y);
  }
  return years;
}

export function laneGeometry(input: LaneGeometryInput): LaneGeometry {
  const stacked = input.width < NAME_WIDTH + NAME_GAP + MIN_TRACK;
  const trackX = stacked ? 0 : NAME_WIDTH + NAME_GAP;
  const trackWidth = Math.max(input.width - trackX, 1);
  const [first, last] = input.axis;
  const pxPerYear = trackWidth / Math.max(last + 1 - first, 1);
  const x = (year: number) => trackX + (year - first) * pxPerYear;

  // Place each span on the track, then pack each lane's spans into rows by the
  // room their labels need.
  const placed = new Map<string, PlacedSpan>();
  const rowsOf = new Map<string, number>();
  const rowOf = new Map<string, number>();
  const trackEnd = trackX + trackWidth;
  for (const lane of input.lanes) {
    const spans = input.spans
      .filter((s) => s.lane === lane.id)
      .sort((a, b) => a.from - b.from || a.to - b.to);
    const rowEnds: number[] = [];
    for (const span of spans) {
      const left = x(span.from);
      const width = Math.max(x(span.to + 1) - left, MIN_SPAN);
      const need = labelWidth(span.name);
      let label: PlacedSpan["label"] = "inside";
      let start = left;
      let end = left + width;
      if (need > width) {
        if (end + LABEL_GAP + need <= trackEnd) {
          label = "right";
          end += LABEL_GAP + need;
        } else if (start - LABEL_GAP - need >= trackX) {
          label = "left";
          start -= LABEL_GAP + need;
        }
      }
      let row = rowEnds.findIndex((rowEnd) => rowEnd + PACK_GAP <= start);
      if (row < 0) {
        row = rowEnds.length;
        rowEnds.push(end);
      } else {
        rowEnds[row] = end;
      }
      rowOf.set(span.id, row);
      const segments = (
        span.segments ?? [{ name: null, from: span.from, to: span.to }]
      ).map((segment) => {
        const segLeft = x(segment.from) - left;
        return {
          name: segment.name,
          x: segLeft,
          width: Math.max(x(segment.to + 1) - left - segLeft, MIN_SPAN),
        };
      });
      placed.set(span.id, {
        id: span.id,
        x: left,
        y: 0,
        width,
        segments,
        label,
      });
    }
    rowsOf.set(lane.id, Math.max(rowEnds.length, 1));
  }

  // Stack the lanes and give each span its row's height.
  const lanes: PlacedLane[] = [];
  const laneTop = new Map<string, number>();
  let y = AXIS_HEIGHT;
  const spanTop = stacked
    ? STACKED_NAME_HEIGHT
    : (LANE_HEIGHT - SPAN_HEIGHT) / 2;
  for (const lane of input.lanes) {
    const rows = rowsOf.get(lane.id) ?? 1;
    const height = stacked
      ? STACKED_NAME_HEIGHT + rows * ROW_STEP + 2
      : LANE_HEIGHT + (rows - 1) * ROW_STEP;
    lanes.push({ id: lane.id, y, height });
    laneTop.set(lane.id, y);
    y += height;
  }
  for (const span of input.spans) {
    const at = placed.get(span.id);
    if (!at) continue;
    at.y =
      (laneTop.get(span.lane) ?? 0) +
      spanTop +
      (rowOf.get(span.id) ?? 0) * ROW_STEP;
  }

  const window = input.window
    ? {
        x: x(Math.max(input.window[0], first)),
        width:
          x(Math.min(input.window[1], last) + 1) -
          x(Math.max(input.window[0], first)),
      }
    : null;
  const inWindow = (year: number) =>
    input.window !== null &&
    (year === input.window[0] || year === input.window[1]);
  // The window's own bounds are always labelled; a regular tick that would
  // crowd one gives way.
  const bounds = input.window
    ? [input.window[0], input.window[1]].filter(
        (year) => year >= first && year <= last,
      )
    : [];
  // Bounds too close to label apart read as one span hung from its inner
  // edge, so the two years never run together ("20182020").
  const [lo, hi] = bounds;
  const joined =
    lo !== undefined && hi !== undefined && lo !== hi && x(hi) - x(lo) < 36;
  const late = joined && x(lo) + x(hi) > x(first) + x(last);
  const boundTicks: LaneGeometry["ticks"] = joined
    ? [
        {
          year: late ? hi : lo,
          x: late ? x(hi) : x(lo),
          inWindow: true,
          label: `${lo}–${hi}`,
          anchor: late ? "end" : "start",
        },
      ]
    : [...new Set(bounds)].map((year) => ({
        year,
        x: x(year),
        inWindow: inWindow(year),
      }));
  const ticks = [
    ...tickYears(input.axis, pxPerYear)
      .filter((year) =>
        bounds.every((bound) => Math.abs(x(year) - x(bound)) >= 36),
      )
      .map((year) => ({ year, x: x(year), inWindow: false })),
    ...boundTicks,
  ].sort((a, b) => a.x - b.x);

  const curve = (x1: number, y1: number, x2: number, y2: number) => {
    const bend = Math.max(Math.abs(x2 - x1) / 2, 24);
    return `M${x1},${y1} C${x1 + bend},${y1} ${x2 - bend},${y2} ${x2},${y2}`;
  };
  const edges: LaneGeometry["edges"] = input.edges.flatMap((edge) => {
    const from = placed.get(edge.from);
    const to = placed.get(edge.to);
    if (!from || !to) return [];
    const fromEnd = from.x + from.width;
    if (to.x < fromEnd - 1) {
      // The successor starts before its predecessor ends (overlapping
      // deliveries): a forward curve would loop back, so drop straight from
      // the predecessor to the successor where the successor begins.
      const lo = Math.max(from.x, to.x);
      const hi = Math.min(fromEnd, to.x + to.width);
      const x = Math.min(lo + 12, (lo + hi) / 2);
      const down = to.y > from.y;
      const y1 = down ? from.y + SPAN_HEIGHT : from.y;
      const y2 = down ? to.y : to.y + SPAN_HEIGHT;
      return [{ ...edge, d: `M${x},${y1} L${x},${y2}` }];
    }
    return [
      {
        ...edge,
        d: curve(
          from.x + from.width,
          from.y + SPAN_HEIGHT / 2,
          to.x,
          to.y + SPAN_HEIGHT / 2,
        ),
      },
    ];
  });
  for (const span of placed.values()) {
    const named = span.segments.filter((segment) => segment.name !== null);
    for (let i = 1; i < named.length; i += 1) {
      const before = named[i - 1];
      const after = named[i];
      if (before.name === after.name) continue;
      const mid = span.y + SPAN_HEIGHT / 2;
      // A short dashed hop under the join, so it reads even where the two
      // eras touch.
      const x1 = span.x + before.x + before.width - 16;
      const x2 = span.x + after.x + 16;
      edges.push({
        id: `${span.id}#rename${i}`,
        from: span.id,
        to: span.id,
        kind: "rename",
        d: `M${x1},${mid + SPAN_HEIGHT / 2 + 3} L${Math.max(x2, x1 + 10)},${mid + SPAN_HEIGHT / 2 + 3}`,
      });
    }
  }

  return {
    stacked,
    trackX,
    trackWidth,
    height: y,
    axisY: AXIS_HEIGHT,
    lanes,
    spans: placed,
    ticks,
    window,
    edges,
  };
}
