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
  /** Where the part starts and ends on the axis, in years, half-open: a
   * calendar year Y is `[Y, Y + 1)`, a school year from 1 July about
   * `[Y + 0.5, Y + 1.5)` (#2408). */
  from: number;
  to: number;
}

export interface LaneSpanGeometryInput {
  id: string;
  lane: string;
  /** The column name, the text set on the span. */
  name: string;
  /** Half-open axis positions, as a segment's. */
  from: number;
  to: number;
  /** Disjoint parts of the span: the columns of one variable in one table,
   * each named, or the deliveries of an interrupted series. One part spanning
   * `from`..`to` when absent. */
  segments?: LaneSegment[];
}

export type LaneEdgeKind = "succession" | "rename";

interface LaneEdgeInput {
  id: string;
  from: string;
  to: string;
  kind: LaneEdgeKind;
  /** The year the succession takes effect: where the arrow is drawn when the
   * two spans overlap. Unknown, the arrow drops where they start to overlap. */
  year?: number | null;
}

/** A lane's row: the default lane, a group member's heading row with no
 * track, or a table's row headed by its table pill (#2399). */
export type LaneRow = "lane" | "heading" | "table";

interface LaneGeometryInput {
  lanes: readonly { id: string; row?: LaneRow }[];
  spans: readonly LaneSpanGeometryInput[];
  edges: readonly LaneEdgeInput[];
  /** Inclusive first and last year of the axis. */
  axis: [number, number];
  window: [number, number] | null;
  /** The graph's measured width in px. */
  width: number;
  /** Reserve the Timeline's coverage row between the axis and the lanes (the
   * variable page). */
  coverageRow?: boolean;
  /** Measured px under a lane's spans for what the page sets there (a
   * column's findings on /project, #1906), by lane id. */
  below?: ReadonlyMap<string, number>;
  /** Measured px of a lane's name block, by lane id: where it holds more
   * than a name and its line (a group member's facts and description) the
   * lane grows to hold it, and stacked, the spans start under it. */
  nameHeights?: ReadonlyMap<string, number>;
  /** The primary pointer is a finger: a graph whose spans would be under 24 px
   * beside the names stacks at any width, to take the 24 px floor. */
  touch?: boolean;
  /** A lane carries an action beside its name (the /project source card's
   * Remove): the gutter keeps its full width, so name and action fit. */
  actions?: boolean;
}

interface PlacedSegment {
  name: string | null;
  /** The name set inside this part, or "" for none: each column name of a
   * span sits once, in its widest part. */
  label: string;
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
  /** Where a name set beside the span starts, from the span's left edge. */
  labelX: number;
  /** A span over several column names: those too wide for their parts, set
   * beside it, each `x` from the span's left edge. */
  beside: { text: string; x: number }[];
}

interface PlacedLane {
  id: string;
  y: number;
  height: number;
}

export interface LaneGeometry {
  /** True when the lane names sit above their spans instead of beside them. */
  stacked: boolean;
  /** The names' gutter beside the track; 0 when stacked. */
  nameWidth: number;
  /** Where the track begins and how wide it is. */
  trackX: number;
  trackWidth: number;
  height: number;
  axisY: number;
  /** The coverage row's top, when one is reserved. */
  coverageY: number | null;
  lanes: PlacedLane[];
  spans: Map<string, PlacedSpan>;
  /** Where the widest drawing ends: past the graph's width when a column name
   * is wider than the whole track or a stacked track grows to its spans'
   * least width, and the graph then scrolls itself. */
  contentWidth: number;
  /** `label` and `anchor` are set on the one label that stands for both
   * window bounds when they are too close to label apart. */
  ticks: {
    year: number;
    x: number;
    inWindow: boolean;
    label?: string;
    anchor?: "start" | "end";
  }[];
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

// The lane metrics: a 58 px lane (`--lane-height`) holding one 24 px span, and
// a further 32 px for each further row. A variable or group page's rows are
// shorter (#2399): a 28 px heading row with no track
// (`--lane-heading-height`) and a 36 px table row (`--lane-row-height`). The
// span's slot keeps the 24 px target; the pill inside it is 21 px.
const ROW_BASE: Record<LaneRow, number> = { lane: 58, heading: 28, table: 36 };
// Stacked, the track starts this far in, past a table row's guide line, and
// a table row keeps this gap under its spans (variant C, #2408).
const STACKED_INSET = 16;
const STACKED_TABLE_GAP = 8;
export const SPAN_HEIGHT = 24;
const ROW_STEP = 32;
const AXIS_HEIGHT = 28;
// The Timeline's plot: a 12 px coverage bar centred in a 40 px row.
export const COVERAGE_HEIGHT = 40;
// Beside the track the names take a gutter that narrows with the graph, so the
// 493 px timeline of a pane at the breakpoint keeps a 350 px track (#2407,
// maintainer decisions 2026-11-11): 27 % of the width between 120 and 230 px,
// then a 2 % gap between 8 and 16 px.
const NAME_WIDTH = [120, 0.27, 230] as const;
const NAME_GAP = [8, 0.02, 16] as const;
// Under this track the names stack over their spans: a graph under about
// 450 px. Margin below the 478 px a 961 px window with a classic scrollbar
// leaves, so the browse columns never stand beside a stacked graph.
const MIN_TRACK = 320;
// A gutter that holds a lane's name and its action (Remove) does not narrow,
// and stacks under the track it always had.
const ACTION_NAME_WIDTH = [230, 0, 230] as const;
const ACTION_NAME_GAP = [16, 0, 16] as const;
const ACTION_MIN_TRACK = 360;
// Stacked, the room a two-line lane name keeps until it is measured.
const STACKED_NAME_HEIGHT = 54;
// Stacked, the least room between a measured name and its spans.
const NAME_SPAN_GAP = 4;
// IBM Plex Mono advances 0.6 em; the `column` type is 12 px. Set in mono, a
// name's width follows from its length without measuring the DOM.
const COLUMN_CHAR = 7.2;
// The column pill around the name: 6 px padding and a 1 px border each side,
// then its 12 px glyph and the 4 px gap after it.
const LABEL_PAD = 14 + 12 + 4;
const LABEL_GAP = 4;
const PACK_GAP = 6;
const MIN_SPAN = 6;
// Stacked (phones, or any graph under a finger whose spans would be smaller),
// the shortest span is at least WCAG 2.2's 24 px target size, and the track
// grows past the graph's width to make room (#2310).
const MIN_TARGET = 24;

/** The room a span's text needs: a column name with its glyph, or a
 * nameless span's years. */
export function labelWidth(text: string): number {
  return text.length * COLUMN_CHAR + LABEL_PAD;
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
  const [first, last] = input.axis;
  const axisYears = Math.max(last + 1 - first, 1);
  // Keyed on the dates alone, never on which spans are pickable, so the track
  // keeps its scale while an add window moves. Spans are drawn at their dates
  // (#2408), so a term is half a year. simplify: floored at a month, which
  // caps the grown track at 288 px a year; lift it if a shorter pickable
  // span turns up on a lane.
  const shortest = Math.min(
    ...input.spans.map((span) =>
      Math.max(
        Math.min(span.to, last + 1) - Math.max(span.from, first),
        1 / 12,
      ),
    ),
  );
  const fluid = ([min, share, max]: readonly [number, number, number]) =>
    Math.min(Math.max(input.width * share, min), max);
  const [nameSpec, gapSpec, minTrack] = input.actions
    ? [ACTION_NAME_WIDTH, ACTION_NAME_GAP, ACTION_MIN_TRACK]
    : [NAME_WIDTH, NAME_GAP, MIN_TRACK];
  const nameGap = fluid(gapSpec);
  const besideTrack = input.width - fluid(nameSpec) - nameGap;
  // Under a finger, a graph whose shortest span would be under 24 px beside
  // the names stacks, as on a phone, and grows its track there.
  const stacked =
    besideTrack < minTrack ||
    (!!input.touch && (besideTrack / axisYears) * shortest < MIN_TARGET);
  const nameWidth = stacked ? 0 : fluid(nameSpec);
  const trackX = stacked ? STACKED_INSET : nameWidth + nameGap;
  const pxPerYear = Math.max(
    Math.max(input.width - trackX, 1) / axisYears,
    stacked && Number.isFinite(shortest) ? MIN_TARGET / shortest : 0,
  );
  const trackWidth = pxPerYear * axisYears;
  // The drawing's width: the graph's, or the grown track's.
  const drawWidth = Math.max(input.width, trackX + trackWidth);
  const x = (year: number) => trackX + (year - first) * pxPerYear;

  // Place each span on the track, then pack each lane's spans into rows by the
  // room their labels need.
  const placed = new Map<string, PlacedSpan>();
  const rowsOf = new Map<string, number>();
  const rowOf = new Map<string, number>();
  const trackEnd = trackX + trackWidth;
  // Where each span's drawing ends, its name included when set to the right.
  const ends = new Map<string, number>();
  for (const lane of input.lanes) {
    const spans = input.spans
      .filter((s) => s.lane === lane.id)
      .sort((a, b) => a.from - b.from || a.to - b.to);
    const rowEnds: number[] = [];
    for (const span of spans) {
      const left = x(span.from);
      const width = Math.max(x(span.to) - left, MIN_SPAN);
      // The column name is never cut (maintainer 2026-11-04): it sits inside
      // when it fits, with no file list (#2321). A span too narrow for the name is
      // a plain bar with the name beside it, right, or left at the axis end.
      // The name is drawn in one delivered segment, the widest, so it must fit
      // that segment, not the envelope across gaps.
      const segments: PlacedSegment[] = (
        span.segments ?? [{ name: null, from: span.from, to: span.to }]
      ).map((segment) => {
        const segLeft = x(segment.from) - left;
        return {
          name: segment.name,
          label: "",
          x: segLeft,
          width: Math.max(x(segment.to) - left - segLeft, MIN_SPAN),
        };
      });
      // Each name's widest part: where it is set when it fits.
      const widest = new Map<string, PlacedSegment>();
      for (const segment of segments) {
        const name = segment.name ?? span.name;
        const best = widest.get(name);
        if (!best || segment.width > best.width) widest.set(name, segment);
      }
      // Several column names (one variable's spellings in one table, #2408):
      // each sits inside its widest part; one too wide for its parts is set
      // beside the span, never cut (maintainer 2026-11-04): before it the names
      // left of the first that fits, after it the rest, each side moving to
      // the other where the track has no room.
      const several = widest.size > 1;
      const beside: PlacedSpan["beside"] = [];
      if (several) {
        const fitting = [...widest].filter(
          ([name, segment]) => labelWidth(name) <= segment.width,
        );
        const firstFit = Math.min(...fitting.map(([, segment]) => segment.x));
        let before: string[] = [];
        let after: string[] = [];
        for (const [name, segment] of widest) {
          if (labelWidth(name) <= segment.width) continue;
          (segment.x < firstFit ? before : after).push(name);
        }
        const text = (names: string[]) => names.join(", ");
        if (
          before.length &&
          left - LABEL_GAP - labelWidth(text(before)) < trackX
        ) {
          after = [...before, ...after];
          before = [];
        }
        if (
          after.length &&
          left + width + LABEL_GAP + labelWidth(text(after)) > trackEnd &&
          left - LABEL_GAP - labelWidth(text([...before, ...after])) >= trackX
        ) {
          before = [...before, ...after];
          after = [];
        }
        if (before.length)
          beside.push({
            text: text(before),
            x: -LABEL_GAP - labelWidth(text(before)),
          });
        if (after.length)
          beside.push({ text: text(after), x: width + LABEL_GAP });
      }
      const room = Math.max(...segments.map((segment) => segment.width));
      // A span with no name (a classification version, named in its tooltip)
      // needs no label room and may touch its neighbour: a yearly series
      // stays on one row.
      const need = span.name && !several ? labelWidth(span.name) : 0;
      const packGap = span.name ? PACK_GAP : 0;
      // Beside the span first, then in a gap between its deliveries; where no
      // room is free the name sits over the bars at the track's end, uncut.
      const gaps = segments.slice(0, -1).map((segment, i) => ({
        from: segment.x + segment.width,
        to: segments[i + 1].x,
      }));
      const gap = gaps.find((g) => g.to - g.from >= 2 * LABEL_GAP + need);
      const [label, labelX]: [PlacedSpan["label"], number] =
        several || need <= room
          ? ["inside", 0]
          : left + width + LABEL_GAP + need <= trackEnd
            ? ["right", width + LABEL_GAP]
            : left - LABEL_GAP - need >= trackX
              ? ["left", -LABEL_GAP - need]
              : gap
                ? ["right", gap.from + LABEL_GAP]
                : ["right", Math.max(trackX, trackEnd - need) - left];
      const start = Math.min(
        left,
        left + labelX,
        ...beside.map((name) => left + name.x),
      );
      const end = Math.max(
        left + width,
        label === "inside" ? left : left + labelX + need,
        ...beside.map((name) => left + name.x + labelWidth(name.text)),
      );
      let row = rowEnds.findIndex((rowEnd) => rowEnd + packGap <= start);
      if (row < 0) {
        row = rowEnds.length;
        rowEnds.push(end);
      } else {
        rowEnds[row] = end;
      }
      if (label === "inside")
        for (const [name, segment] of widest)
          if (!several || labelWidth(name) <= segment.width)
            segment.label = name;
      rowOf.set(span.id, row);
      ends.set(span.id, end);
      placed.set(span.id, {
        id: span.id,
        x: left,
        y: 0,
        width,
        segments,
        label,
        labelX,
        beside,
      });
    }
    // A lane with no spans (a group member's heading row) keeps no span row.
    rowsOf.set(lane.id, rowEnds.length);
  }

  // Stack the lanes and give each span its row's height.
  const lanes: PlacedLane[] = [];
  const laneTop = new Map<string, number>();
  const coverageY = input.coverageRow ? AXIS_HEIGHT : null;
  let y = AXIS_HEIGHT + (input.coverageRow ? COVERAGE_HEIGHT : 0);
  // Stacked, the spans start under the lane's name block; beside it, they
  // keep the lane's top and the lane grows to the taller of the two.
  const spanTop = new Map<string, number>();
  for (const lane of input.lanes) {
    const rows = rowsOf.get(lane.id) ?? 0;
    const name = input.nameHeights?.get(lane.id) ?? 0;
    const base = ROW_BASE[lane.row ?? "lane"];
    // Stacked, the spans start right under the measured name.
    const top = stacked
      ? name > 0
        ? name + NAME_SPAN_GAP
        : STACKED_NAME_HEIGHT
      : (base - SPAN_HEIGHT) / 2;
    const height =
      (stacked
        ? top + rows * ROW_STEP + (lane.row === "table" ? STACKED_TABLE_GAP : 2)
        : Math.max(base + (Math.max(rows, 1) - 1) * ROW_STEP, name)) +
      (input.below?.get(lane.id) ?? 0);
    lanes.push({ id: lane.id, y, height });
    laneTop.set(lane.id, y);
    spanTop.set(lane.id, top);
    y += height;
  }
  for (const span of input.spans) {
    const at = placed.get(span.id);
    if (!at) continue;
    at.y =
      (laneTop.get(span.lane) ?? 0) +
      (spanTop.get(span.lane) ?? 0) +
      (rowOf.get(span.id) ?? 0) * ROW_STEP;
  }

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
  // The axis's first and last year are always labelled, on their cells
  // (maintainer 2026-11-05). A window bound whose year would touch one hangs
  // away from that end; where they still touch, the bound's label goes (the
  // window's band still marks it), never the end's.
  type Tick = LaneGeometry["ticks"][number];
  // A label's run, anchored as LaneGraph draws it (about 7 px per mono
  // character): a joined label's whole run, not only the bound it hangs from.
  const run = (tick: Tick): [number, number] => {
    const length = (tick.label ?? String(tick.year)).length * 7;
    const anchor =
      tick.anchor ??
      (tick.x - trackX < 16
        ? "start"
        : drawWidth - tick.x < 16
          ? "end"
          : "middle");
    return anchor === "start"
      ? [tick.x, tick.x + length]
      : anchor === "end"
        ? [tick.x - length, tick.x]
        : [tick.x - length / 2, tick.x + length / 2];
  };
  const touch = (one: Tick, other: Tick, gap = 4) => {
    const [a, b] = run(one);
    const [c, d] = run(other);
    return b + gap > c && d + gap > a;
  };
  const axisEnds = [first, last].filter((year) => !bounds.includes(year));
  // Each end's label sits on its own cell, against the track's edge: a tick's
  // x is its cell's start, so the last year hangs back from the cell's end,
  // not from its start, where it would read as the year before.
  const endTicks: Tick[] = axisEnds.map((year) =>
    year === first
      ? { year, x: x(year), inWindow: false, anchor: "start" }
      : { year, x: x(year + 1), inWindow: false, anchor: "end" },
  );
  for (const tick of boundTicks) {
    if (tick.anchor !== undefined) continue;
    for (const end of endTicks) {
      if (touch(end, tick)) tick.anchor = end.year === first ? "start" : "end";
    }
  }
  const shownBounds = boundTicks.filter((bound) =>
    endTicks.every((end) => !touch(end, bound)),
  );
  // A regular year gives way to every labelled bound and end.
  const fixed = [...endTicks, ...shownBounds];
  const ticks = [
    ...tickYears(input.axis, pxPerYear)
      .filter((year) => year !== first && year !== last)
      .map((year) => ({ year, x: x(year), inWindow: false }))
      .filter((tick) => fixed.every((other) => !touch(tick, other, 8))),
    ...fixed,
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
      // the predecessor to the successor, at the year the succession takes
      // effect (#1901), else where the overlap begins.
      const lo = Math.max(from.x, to.x);
      const hi = Math.min(fromEnd, to.x + to.width);
      const at =
        edge.year != null ? x(edge.year) : Math.min(lo + 12, (lo + hi) / 2);
      // Kept on both spans, so the line never hangs in empty track.
      const drop =
        hi - lo > 4 ? Math.min(Math.max(at, lo + 2), hi - 2) : (lo + hi) / 2;
      const down = to.y > from.y;
      const y1 = down ? from.y + SPAN_HEIGHT : from.y;
      const y2 = down ? to.y : to.y + SPAN_HEIGHT;
      return [{ ...edge, d: `M${drop},${y1} L${drop},${y2}` }];
    }
    return [
      {
        ...edge,
        // A name set right of the bar: the arrow leaves after it, not
        // through it, when there is room before the successor.
        d: curve(
          Math.max(
            fromEnd,
            Math.min(ends.get(edge.from) ?? fromEnd, to.x - LABEL_GAP),
          ),
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
    nameWidth,
    trackX,
    trackWidth,
    height: y,
    axisY: AXIS_HEIGHT,
    coverageY,
    lanes,
    spans: placed,
    contentWidth: Math.max(drawWidth, ...ends.values()),
    ticks,
    edges,
  };
}
