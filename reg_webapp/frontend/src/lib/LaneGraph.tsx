import {
  type ReactNode,
  useCallback,
  useEffect,
  useId,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import {
  Checkbox,
  Focusable,
  Radio,
  RadioGroup,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import findingStyles from "./Finding.module.css";
import styles from "./LaneGraph.module.css";
import {
  COVERAGE_HEIGHT,
  type LaneEdgeKind,
  type LaneGeometry,
  type LaneRow,
  type LaneSpanGeometryInput,
  laneGeometry,
  type PlacedSpan,
  SPAN_HEIGHT,
} from "./lane_graph";
import { coverageMarks } from "./period";
import type { ProjectStudyWindow } from "./project_data";
import { EntityPill } from "./ui/EntityPill";
import type { ColumnState } from "./ui/Pill";

export interface LaneModel {
  id: string;
  name: string;
  /** The line under the name: what tells this lane from its neighbours. */
  sub?: { text: string; mono?: boolean } | null;
  /** The variable's own page; the name links there. */
  href?: string | null;
  /** A group member's heading row (no track), or a table's row, its name
   * drawn as the table pill (#2399); a plain lane when absent. */
  row?: LaneRow;
  /** A row under another lane's heading row: a group member's table. */
  nested?: boolean;
  /** More about the lane under its name and line, in the name column: a
   * group member's type, facts and description. The lane grows to hold it. */
  detail?: ReactNode;
  /** The name and its line wrap and the lane grows to hold them, as with a
   * `detail`: a long name rather than an ellipsis (a classification's
   * lanes, named by its parts or its own long name). */
  wrap?: boolean;
  /** Its relations as sentences, for a screen reader: the edges are drawn
   * only. */
  relations?: string[];
  /** Columns the catalog knows but that were never delivered: no years to
   * place, so they sit under the name. */
  undelivered?: string[];
  /** A control beside the name that acts on this lane alone (the /project
   * source timeline's per-column remove). */
  action?: ReactNode;
  /** Set directly under the lane's spans, across the whole lane: a /project
   * column's findings (#1906). The lane grows to hold it. */
  below?: ReactNode;
}

export interface LaneSpanModel extends LaneSpanGeometryInput {
  /** "2015–2019", "2020–", or "year-independent". */
  years: string;
  state: ColumnState;
  /** A pending change to the project, shown until committed. */
  staged?: "add" | "remove" | null;
  /** In the project through a source the study window has moved off. */
  blocked?: boolean;
  /** A checkbox when true; a mark to read otherwise. */
  selectable: boolean;
  isSelected?: boolean;
  /** The table's fqid: which span of a variable a relation anchors on. */
  tableRef?: string;
  /** Starts with the column name, then the variable, the years and the
   * state. */
  accessibleName: string;
  /** What a span with no column name stands for (a classification
   * version), its tooltip in the UI face in place of its years: the caller
   * adds the years where the label lacks them. */
  label?: string;
}

export interface LaneEdgeModel {
  id: string;
  from: string;
  to: string;
  kind: LaneEdgeKind;
  /** The year the succession takes effect: where an overlap's arrow drops. */
  year?: number | null;
}

/** The variable page's coverage row (DESIGN.md → Time → Timeline), drawn
 * between the axis and the lanes. */
interface LaneCoverage {
  /** The coverage edges, open ends resolved; null when unknown. */
  covered: ProjectStudyWindow | null;
  /** The delivered years, or null to draw coverage as one bar. */
  years: readonly number[] | null;
  /** The band's years outside coverage, in `info-bg` on this row. */
  outside: readonly ProjectStudyWindow[];
  /** What the row is the coverage of, in place of "Years": the variable on
   * its own page, which heads the drawing. Stacked, it sits over the drawing. */
  caption?: string;
}

interface Props {
  label: string;
  lanes: LaneModel[];
  spans: LaneSpanModel[];
  edges: LaneEdgeModel[];
  /** Inclusive first and last year. */
  axis: [number, number];
  /** The window drawn as the band (its bounds and the light shade), the
   * study window on a variable or group page; null draws none. */
  window: [number, number] | null;
  /** An add window that differs from `window`, drawn as the stronger band
   * between the thumbs (#1910); it may reach past `window`. */
  addWindow?: [number, number] | null;
  coverage?: LaneCoverage;
  /** Laid over the coverage row's track (the period slider), on `axis`. */
  overlay?: ReactNode;
  isDisabled?: boolean;
  onToggle?: (spanId: string) => void;
  /** The lane whose name takes focus, scrolled into view, once it is first
   * drawn: a group page opened at one member's address, when nothing else
   * holds focus. */
  focus?: string | null;
  /** The selectable span that takes focus the same way: a classification
   * opened at one version's address. */
  focusSpan?: string | null;
  /** Under the graph while any span is selectable; null for none. */
  hint?: string | null;
  /** One span is picked at a time (a classification's version): the spans are
   * one radio group, a single tab stop with arrow keys between them (#2310). */
  single?: boolean;
}

/** Measured heights by lane id, and the callback a measured block reports
 * to; zero gives the room back. */
function useHeights() {
  const [heights, setHeights] = useState<ReadonlyMap<string, number>>(
    () => new Map(),
  );
  const onHeight = useCallback((id: string, height: number) => {
    setHeights((current) => {
      if (current.get(id) === height) return current;
      const next = new Map(current);
      if (height > 0) next.set(id, height);
      else next.delete(id);
      return next;
    });
  }, []);
  return [heights, onHeight] as const;
}

/**
 * Lanes over time (DESIGN.md → Time → LaneGraph): one lane per variable, each
 * column a span, the column pill stretched over its years, on one shared
 * axis, relations as arrows, solid for succession and dashed for a rename,
 * the study window as a light `accent-soft` shade 8 px past its bounds and an
 * add window as a stronger `accent-bg` over it (#2399). The marks are one decorative SVG; the lanes and spans
 * are HTML over it, so every span is a real checkbox in reading order, named with
 * its column, variable and years. Tab walks the spans lane by lane and Space
 * toggles one. On a variable page a coverage row heads the lanes, so the
 * Timeline and the columns are one drawing on one axis (#1833). There is no
 * legend: the span names its column, a tooltip adds only what the span cuts
 * (#2321), and the relations are sentences in each lane (maintainer
 * 2026-11-04).
 */
export function LaneGraph({
  label,
  lanes,
  spans,
  edges,
  axis,
  window,
  addWindow = null,
  coverage,
  overlay,
  isDisabled = false,
  onToggle,
  focus = null,
  focusSpan = null,
  hint = "Click a column to add it to your project.",
  single = false,
}: Props) {
  const [width, setWidth] = useState(0);
  const [hovered, setHovered] = useState<string | null>(null);
  // The measured height of each lane's `below` block and of each name block
  // with a `detail`, so the lanes under them move down instead of being
  // covered.
  const [below, onBelowHeight] = useHeights();
  const [nameHeights, onNameHeight] = useHeights();
  const markerId = useId();
  // The one DOM measurement: the graph's width, which the layout needs to
  // place labels and pack rows.
  const graphRef = useRef<HTMLDivElement | null>(null);
  const measure = useCallback((node: HTMLDivElement | null) => {
    graphRef.current = node;
    if (!node) return;
    const observer = new ResizeObserver(([entry]) => {
      setWidth(Math.floor(entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const touch = useCoarsePointer();
  const geometry =
    width > 0
      ? laneGeometry({
          lanes,
          spans,
          edges,
          axis,
          // The axis labels the add window's bounds when one is drawn; the
          // study window's keep their rules. Labelling both needs
          // `laneGeometry` to take both windows.
          window: addWindow ?? window,
          width,
          coverageRow: coverage !== undefined,
          below,
          nameHeights,
          touch,
        })
      : null;
  // A single-pick graph that scrolls keeps its picked span in view (a classification's
  // version, which opens on the latest), by its own scroll, never the page's.
  const picked = spans.filter((span) => span.isSelected);
  const keep =
    single && picked.length === 1
      ? geometry?.spans.get(picked[0].id)
      : undefined;
  const keepFrom = keep?.x;
  const keepTo = keep ? keep.x + keep.width : undefined;
  useEffect(() => {
    const node = graphRef.current;
    if (!node || keepFrom === undefined || keepTo === undefined) return;
    const margin = 16;
    // The measured width, so a resize that keeps the scale still re-runs this.
    const view = width;
    // A span wider than the view keeps its leading edge, where its name sits.
    const fits = keepTo - keepFrom + 2 * margin <= view;
    if (
      keepFrom < node.scrollLeft ||
      (!fits && keepFrom > node.scrollLeft + view - margin)
    )
      node.scrollLeft = Math.max(keepFrom - margin, 0);
    else if (fits && keepTo > node.scrollLeft + view)
      node.scrollLeft = keepTo - view + margin;
  }, [keepFrom, keepTo, width]);
  // Unless it keeps a picked span in view, a graph that scrolls opens once on
  // its latest span, so a lane whose only span is recent does not read as
  // empty; the span, not the axis end, which study years may stretch past.
  const latest =
    geometry && geometry.contentWidth > width && keep === undefined
      ? Math.max(...[...geometry.spans.values()].map((s) => s.x + s.width))
      : undefined;
  const opened = useRef(false);
  useEffect(() => {
    const node = graphRef.current;
    if (!node || latest === undefined || opened.current) return;
    opened.current = true;
    node.scrollLeft = Math.max(latest - node.clientWidth + 16, 0);
  }, [latest]);
  const spansByLane = new Map<string, LaneSpanModel[]>();
  for (const span of spans) {
    spansByLane.set(span.lane, [...(spansByLane.get(span.lane) ?? []), span]);
  }

  return (
    <div className={styles.frame}>
      {geometry?.stacked && coverage?.caption && (
        <p className={styles.stackedCaption} aria-hidden="true">
          {coverage.caption}
        </p>
      )}
      <div
        ref={measure}
        className={styles.graph}
        data-stacked={geometry?.stacked || undefined}
        // A name wider than the track, or a stacked track grown to its spans'
        // least width, scrolls inside the graph, never the page.
        data-scroll={(geometry && geometry.contentWidth > width) || undefined}
      >
        {geometry && (
          <>
            {/* Sizes the scroll box from inside, so a classic horizontal
                scrollbar adds to the graph's height instead of covering the
                last lane. */}
            <div
              aria-hidden="true"
              style={{ width: geometry.contentWidth, height: geometry.height }}
            />
            <Marks
              geometry={geometry}
              lanes={lanes}
              axis={axis}
              window={window}
              addWindow={addWindow}
              coverage={coverage}
              width={geometry.contentWidth}
              hovered={hovered}
              markerId={markerId}
            />
            {overlay && geometry.coverageY !== null && (
              <div
                className={styles.overlay}
                style={{
                  left: geometry.trackX,
                  top: geometry.coverageY,
                  width: geometry.trackWidth,
                  height: COVERAGE_HEIGHT,
                }}
              >
                {overlay}
              </div>
            )}
            {!geometry.stacked && geometry.coverageY !== null && (
              <span
                className={styles.coverageCaption}
                style={{
                  top: geometry.coverageY,
                  height: COVERAGE_HEIGHT,
                  width: geometry.trackX - 16,
                }}
                aria-hidden="true"
              >
                {coverage?.caption ? (
                  <span className={styles.named} title={coverage.caption}>
                    {coverage.caption}
                  </span>
                ) : (
                  "Years"
                )}
              </span>
            )}
            <Picker
              single={single}
              label={label}
              value={picked[0]?.id ?? null}
              isDisabled={isDisabled}
              onChange={onToggle}
            >
              <ol
                className={styles.lanes}
                aria-label={single ? undefined : label}
              >
                {geometry.lanes.map((placed) => {
                  const lane = lanes.find((l) => l.id === placed.id);
                  if (!lane) return null;
                  return (
                    <li
                      key={lane.id}
                      className={styles.lane}
                      style={{
                        top: placed.y,
                        height: placed.height,
                        width: geometry.contentWidth,
                      }}
                    >
                      <LaneName
                        lane={lane}
                        width={geometry.stacked ? width : geometry.trackX - 16}
                        // Stacked, every lane's spans start under its measured
                        // name; beside the track only a detail grows the lane.
                        onHeight={
                          lane.detail || lane.wrap || geometry.stacked
                            ? onNameHeight
                            : undefined
                        }
                        focus={lane.id === focus}
                        top={placed.y}
                      />
                      <ul className={styles.spans}>
                        {(spansByLane.get(lane.id) ?? []).map((span) => {
                          const at = geometry.spans.get(span.id);
                          return at ? (
                            <li
                              key={span.id}
                              className={styles.slot}
                              style={{
                                left: at.x,
                                top: at.y - placed.y,
                                width: at.width,
                                height: SPAN_HEIGHT,
                              }}
                            >
                              <Span
                                span={span}
                                at={at}
                                isDisabled={isDisabled}
                                onToggle={onToggle}
                                onActive={setHovered}
                                focus={span.id === focusSpan}
                                single={single}
                              />
                            </li>
                          ) : null;
                        })}
                      </ul>
                      {lane.below && (
                        <LaneBelow
                          id={lane.id}
                          width={width}
                          top={placed.height - (below.get(lane.id) ?? 0)}
                          onHeight={onBelowHeight}
                        >
                          {lane.below}
                        </LaneBelow>
                      )}
                    </li>
                  );
                })}
              </ol>
            </Picker>
          </>
        )}
      </div>
      {hint && spans.some((s) => s.selectable) && (
        <p className={styles.hint}>{hint}</p>
      )}
    </div>
  );
}

function Marks({
  geometry,
  lanes,
  axis,
  window,
  addWindow,
  coverage,
  width,
  hovered,
  markerId,
}: {
  geometry: LaneGeometry;
  lanes: LaneModel[];
  axis: [number, number];
  window: [number, number] | null;
  addWindow: [number, number] | null;
  coverage: LaneCoverage | undefined;
  width: number;
  hovered: string | null;
  markerId: string;
}) {
  const marker = `${markerId}-arrow`;
  const bottom = geometry.height;
  const x = (year: number) =>
    geometry.trackX +
    ((year - axis[0]) / (axis[1] + 1 - axis[0])) * geometry.trackWidth;
  const run = (r: ProjectStudyWindow) => ({
    x: x(r.from),
    width: x(r.to + 1) - x(r.from),
  });
  // A window as drawn: its part on the axis, or null when wholly off it.
  const onAxis = (w: [number, number] | null) =>
    w && w[1] >= axis[0] && w[0] <= axis[1]
      ? run({ from: Math.max(w[0], axis[0]), to: Math.min(w[1], axis[1]) })
      : null;
  // A window's shade runs 8 px past its bounds, so it never sits tight on the
  // spans' ends (maintainer, #2399, 2026-11-11), and stays on the drawing.
  const shade = (w: [number, number] | null) => {
    const r = onAxis(w);
    if (!r) return null;
    const from = r.x - SHADE_PAD;
    return {
      x: from,
      width: Math.min(r.x + r.width + SHADE_PAD, width) - from,
    };
  };
  const study = shade(window);
  const add = shade(addWindow);
  const rowOf = (id: string) => lanes.find((l) => l.id === id)?.row ?? "lane";
  const cy = geometry.coverageY;
  const marks =
    coverage && cy !== null
      ? coverageMarks(coverage.covered, coverage.years)
      : null;
  return (
    <svg
      className={styles.marks}
      width={width}
      height={bottom}
      aria-hidden="true"
    >
      <defs>
        <marker
          id={marker}
          viewBox="0 0 8 8"
          refX="7"
          refY="4"
          markerWidth="6"
          markerHeight="6"
          orient="auto"
        >
          <path d="M0,0 L8,4 L0,8 z" className={styles.arrowhead} />
        </marker>
      </defs>
      {/* The study window's shade and the add window's stronger band over it,
          so the study window reads wherever an add window narrows or
          overreaches it (#1910). The thumbs mark the bounds; there are no
          bound lines (maintainer, 2026-11-11). */}
      {study && (
        <rect
          className={styles.band}
          x={study.x}
          y={geometry.axisY}
          width={study.width}
          height={bottom - geometry.axisY}
        />
      )}
      {add && (
        <rect
          className={styles.addBand}
          x={add.x}
          y={geometry.axisY}
          width={add.width}
          height={bottom - geometry.axisY}
        />
      )}
      {coverage && cy !== null && marks && (
        <g>
          {/* Where the band leaves coverage, its wash gives way to info-bg on
              this row; the lanes below keep the band whole. */}
          {coverage.outside.map((r) => (
            <rect
              key={`u${r.from}`}
              className={styles.uncovered}
              y={cy}
              height={COVERAGE_HEIGHT}
              {...run(r)}
            />
          ))}
          {marks.delivered.map((r) => (
            <rect
              key={`d${r.from}`}
              className={styles.coverage}
              y={cy + 14}
              height={12}
              rx={1}
              x={run(r).x}
              width={Math.max(run(r).width, 2)}
            />
          ))}
          {marks.ticks.map((year) => (
            <line
              key={`t${year}`}
              className={styles.edition}
              x1={x(year)}
              x2={x(year)}
              y1={cy + 14}
              y2={cy + 26}
            />
          ))}
          <line
            className={styles.rule}
            x1={0}
            x2={width}
            y1={cy + COVERAGE_HEIGHT}
            y2={cy + COVERAGE_HEIGHT}
          />
        </g>
      )}
      {/* A divider between variables only, never between a variable's tables
          (#2399): a heading row's above it, a plain lane's above and below. */}
      {geometry.lanes.flatMap((lane) => {
        const row = rowOf(lane.id);
        const ys =
          row === "table"
            ? []
            : row === "heading"
              ? [lane.y]
              : [lane.y, lane.y + lane.height];
        return ys.map((y) => (
          <line
            key={`${lane.id}@${y}`}
            className={styles.rule}
            x1={0}
            x2={width}
            y1={y}
            y2={y}
          />
        ));
      })}
      <line
        className={styles.axis}
        x1={0}
        x2={width}
        y1={geometry.axisY}
        y2={geometry.axisY}
      />
      {geometry.ticks.map((tick) => (
        <text
          key={`${tick.year}:${tick.inWindow}`}
          className={tick.inWindow ? styles.tickWindow : styles.tick}
          x={tick.x}
          y={geometry.axisY - 10}
          // A year at either end of the track is anchored inward, not clipped.
          textAnchor={
            tick.anchor ??
            (tick.x - geometry.trackX < 16
              ? "start"
              : width - tick.x < 16
                ? "end"
                : "middle")
          }
        >
          {tick.label ?? tick.year}
        </text>
      ))}
      {geometry.edges.map((edge) => (
        <path
          key={edge.id}
          className={`${styles.edge} ${styles[edge.kind]} ${
            hovered !== null && (edge.from === hovered || edge.to === hovered)
              ? styles.edgeActive
              : ""
          }`}
          d={edge.d}
          markerEnd={`url(#${marker})`}
        />
      ))}
    </svg>
  );
}

/** A lane's `below` block, reporting its height so the lane grows to hold it.
 * Unmounting reports zero, which gives the room back. */
function LaneBelow({
  id,
  top,
  width,
  onHeight,
  children,
}: {
  id: string;
  top: number;
  /** The graph's own width: the block stays in view while the lane scrolls. */
  width: number;
  onHeight: (id: string, height: number) => void;
  children: ReactNode;
}) {
  const measure = useMeasuredHeight(id, onHeight);
  return (
    <div ref={measure} className={styles.below} style={{ top }}>
      <div className={styles.pinned} style={{ width }}>
        {children}
      </div>
    </div>
  );
}

/**
 * The lanes' names and actions without the drawing, one plain row each: for a
 * /project source whose period draws no lanes (a token period), so its columns
 * keep their per-column action.
 */
export function LaneList({
  label,
  lanes,
}: {
  label: string;
  lanes: LaneModel[];
}) {
  return (
    <ol className={styles.laneList} aria-label={label}>
      {lanes.map((lane) => (
        <li key={lane.id}>
          <LaneName lane={lane} />
          {lane.below && <div className={styles.listBelow}>{lane.below}</div>}
        </li>
      ))}
    </ol>
  );
}

function LaneName({
  lane,
  width,
  onHeight,
  focus = false,
  top,
}: {
  lane: LaneModel;
  width?: number;
  onHeight?: (id: string, height: number) => void;
  focus?: boolean;
  top?: number;
}) {
  const measure = useMeasuredHeight(lane.id, onHeight);
  // The action is a sibling of the text block, not inside it: the block's
  // `overflow: hidden` (the ellipsis) would clip the action's focus ring.
  if (lane.action) {
    return (
      <div
        ref={measure}
        className={styles.nameRow}
        style={width ? { width } : undefined}
      >
        <LaneText lane={lane} />
        <span className={styles.action}>{lane.action}</span>
      </div>
    );
  }
  return (
    <LaneText
      lane={lane}
      width={width}
      measure={measure}
      focus={focus}
      top={top}
    />
  );
}

/** A ref that reports its node's border-box height under `id`, and zero
 * when the node goes, so the room is given back. */
function useMeasuredHeight(
  id: string,
  onHeight?: (id: string, height: number) => void,
) {
  return useCallback(
    (node: HTMLDivElement | null) => {
      if (!node || !onHeight) return;
      const observer = new ResizeObserver(([entry]) => {
        onHeight(id, Math.ceil(entry.borderBoxSize[0]?.blockSize ?? 0));
      });
      observer.observe(node);
      return () => {
        observer.disconnect();
        onHeight(id, 0);
      };
    },
    [id, onHeight],
  );
}

function LaneText({
  lane,
  width,
  measure,
  focus = false,
  top,
}: {
  lane: LaneModel;
  width?: number;
  measure?: (node: HTMLDivElement | null) => void;
  focus?: boolean;
  /** The lane's place in the drawing, which moves while the lanes above
   * measure their names. */
  top?: number;
}) {
  const subId = useId();
  const focusName = useFocusOnce(focus, top);
  const name =
    lane.row === "table" ? (
      // The table's name inside its pill, which cuts it with an ellipsis and
      // the whole name as hover text (#2399).
      <EntityPill entity="table" name={lane.name} />
    ) : lane.href ? (
      <a
        className={`link ${styles.nameText}`}
        href={lane.href}
        // Tab reads links only: the line under the name tells lanes of one
        // name apart, so it describes the link.
        aria-describedby={lane.sub ? subId : undefined}
        // The name and its line truncate; two lanes can then read alike
        // (SyssStat11, SyssStat19), so the whole of each is the hover text.
        title={lane.name}
      >
        {lane.name}
      </a>
    ) : (
      <span
        ref={focusName}
        className={styles.nameText}
        title={lane.name}
        // The focus target of a member's address, said in words.
        tabIndex={focus ? -1 : undefined}
        aria-current={focus ? "true" : undefined}
      >
        {lane.name}
      </span>
    );
  return (
    <div
      ref={measure}
      className={`${styles.name} ${lane.detail || lane.wrap ? styles.detailed : ""} ${lane.nested ? styles.nested : ""}`}
      data-row={lane.row}
      style={width ? { width } : undefined}
    >
      {name}
      {lane.sub && (
        <span
          id={subId}
          className={`${styles.sub} ${lane.sub.mono ? styles.mono : ""}`}
          title={lane.sub.text}
        >
          {lane.sub.text}
        </span>
      )}
      {lane.undelivered && lane.undelivered.length > 0 && (
        <span className={styles.undelivered}>
          {lane.undelivered.map((column) => (
            <EntityPill
              key={column}
              entity="column"
              name={column}
              state="out"
            />
          ))}
          <span className={styles.sub}>never delivered</span>
        </span>
      )}
      {lane.detail}
      {lane.relations && lane.relations.length > 0 && (
        <span className="visually-hidden">{lane.relations.join(". ")}.</span>
      )}
    </div>
  );
}

/** A ref that focuses its node once, when it is first drawn (the lanes mount
 * only after the graph is measured), and keeps it in view while the lanes
 * above grow to their measured names, until focus moves on. Only when nothing
 * holds focus (a fresh load or a deep link): a member opened from a browse
 * column keeps focus on that column's row, which owns it. `top` is the node's
 * place in the drawing, which moves while the lanes above measure. */
function useFocusOnce(focus: boolean, top: number | undefined) {
  const focused = useRef<HTMLElement | null>(null);
  const focusNode = useCallback(
    (node: HTMLElement | null) => {
      if (!node || !focus) return;
      const active = document.activeElement;
      if (active && active !== document.body) return;
      focused.current = node;
      node.focus({ preventScroll: true });
    },
    [focus],
  );
  useEffect(() => {
    const node = focused.current;
    if (top === undefined || !node || document.activeElement !== node) return;
    // Only a lane out of view scrolls: centring one already in view pushed the
    // page's head (kind mark, title) half under the sticky top bar.
    const bar =
      Number.parseFloat(
        getComputedStyle(document.documentElement).getPropertyValue(
          "--top-bar-height",
        ),
      ) || 0;
    const { top: y, bottom } = node.getBoundingClientRect();
    if (y < bar || bottom > window.innerHeight)
      node.scrollIntoView({ block: "center" });
  }, [top]);
  // The reader's own scroll or key ends that: a later resize must not pull
  // the page back.
  useEffect(() => {
    if (!focus) return;
    const release = () => {
      focused.current = null;
    };
    const events = ["wheel", "touchmove", "keydown", "pointerdown"] as const;
    for (const event of events)
      window.addEventListener(event, release, { once: true, passive: true });
    return () => {
      for (const event of events) window.removeEventListener(event, release);
    };
  }, [focus]);
  return focusNode;
}

function Span({
  span,
  at,
  isDisabled,
  onToggle,
  onActive,
  focus,
  single,
}: {
  span: LaneSpanModel;
  at: PlacedSpan;
  isDisabled: boolean;
  onToggle?: (id: string) => void;
  onActive: (id: string | null) => void;
  focus: boolean;
  single: boolean;
}) {
  const face = useRef<HTMLLabelElement & HTMLSpanElement>(null);
  const focusInput = useFocusOnce(focus, at.y);
  // Set while the page moves focus here itself (a version address): that
  // focus opens no tooltip, which would cover the lane's name.
  const quiet = useRef(false);
  // The checkbox's or radio's root is its label; focus lands on its input.
  const pickRef = useCallback(
    (node: HTMLLabelElement | null) => {
      face.current = node;
      quiet.current = true;
      focusInput(node?.querySelector("input") ?? null);
      quiet.current = false;
    },
    [focusInput],
  );
  const [open, setOpen] = useState(false);
  // An unnamed series (an interrupted delivery) carries its name once, on its
  // widest part, so a one-year first delivery does not clip it.
  const widest = at.segments.reduce(
    (best, segment, i) => (segment.width > at.segments[best].width ? i : best),
    0,
  );
  const faces = at.segments.map((segment, i) => {
    const name =
      at.label !== "inside"
        ? ""
        : (segment.name ?? (i === widest ? span.name : ""));
    return (
      // The column pill stretched over its years: the name only, since the
      // axis gives the years (maintainer, #2399, 2026-11-11).
      <EntityPill
        // Positional: the segments are rebuilt whole from the span.
        // oxlint-disable-next-line react/no-array-index-key
        key={i}
        entity="column"
        name={name}
        state={span.state}
        className={styles.face}
      />
    );
  });
  const body = (
    <>
      {at.segments.map((segment, i) => (
        <span
          // oxlint-disable-next-line react/no-array-index-key
          key={i}
          className={styles.segment}
          style={{ left: segment.x, width: segment.width }}
        >
          {faces[i]}
        </span>
      ))}
      {span.staged && (
        <span className={styles.glyph} aria-hidden="true">
          {span.staged === "add" ? "✓" : "−"}
        </span>
      )}
      {span.blocked && !span.staged && (
        <span className={styles.glyph} aria-hidden="true">
          ✕
        </span>
      )}
      {at.label !== "inside" && (
        <span className={styles.outside} style={{ left: at.labelX }}>
          {span.name}
        </span>
      )}
    </>
  );
  const attributes = {
    "data-state": span.state,
    "data-staged": span.staged ?? undefined,
    "data-blocked": span.blocked || undefined,
  };
  // The tooltip opens only where the span's own text is cut off (maintainer,
  // 2026-11-10, #2321): its years, which a span no longer shows (#2399). The
  // column name is never cut.
  const tipped = !!span.years;
  const tip = tipped && (
    <Tooltip
      triggerRef={face}
      className={({ isEntering, isExiting }) =>
        `${findingStyles.tooltip} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
      }
      offset={6}
      // A single-pick graph's arrow keys open it on each step: under the span,
      // it leaves the lane's name readable.
      placement={single ? "bottom" : "top"}
      style={{ zIndex: "var(--z-popover)" }}
    >
      {/* A span without a column name (a classification version) says its
          label, or else its years, in the UI face. */}
      <span>
        {span.name ? (
          <>
            <code>{span.name}</code>
            {span.years && `, ${span.years}`}
          </>
        ) : (
          (span.label ?? span.years)
        )}
      </span>
    </Tooltip>
  );
  const show = (on: boolean) => {
    if (on && quiet.current) return;
    setOpen(on);
    onActive(on ? span.id : null);
  };
  if (!span.selectable || !onToggle) {
    return (
      // A read-only span is no tab stop: Focusable hands it the trigger's
      // hover, focus and Escape handling, and its role and name carry the
      // column, so hovering opens the tooltip and Escape closes it.
      <TooltipTrigger
        isOpen={open}
        onOpenChange={setOpen}
        delay={0}
        closeDelay={0}
      >
        <Focusable ref={face} excludeFromTabOrder>
          <span
            // Focusable needs a named role; an <img> cannot hold the drawing.
            // oxlint-disable-next-line jsx-a11y/prefer-tag-over-role
            role="img"
            aria-label={span.accessibleName}
            className={styles.span}
            {...attributes}
          >
            {body}
          </span>
        </Focusable>
        {tip}
      </TooltipTrigger>
    );
  }
  return (
    // The trigger's focus props land on the checkbox's input, so focus opens
    // the tooltip through onOpenChange; hover is the face's own, since the
    // input is visually hidden. The tooltip is placed on the visible face.
    <TooltipTrigger isOpen={open} onOpenChange={show} delay={0} closeDelay={0}>
      {single ? (
        // The group owns the value and the disabled state.
        <Radio
          ref={pickRef}
          value={span.id}
          className={`${styles.span} ${styles.pick}`}
          style={{ height: SPAN_HEIGHT }}
          aria-label={span.accessibleName}
          onHoverStart={() => show(true)}
          onHoverEnd={() => show(false)}
          onFocus={() => show(true)}
          onBlur={() => show(false)}
          {...attributes}
        >
          {body}
        </Radio>
      ) : (
        <Checkbox
          ref={pickRef}
          className={`${styles.span} ${styles.pick}`}
          style={{ height: SPAN_HEIGHT }}
          aria-label={span.accessibleName}
          isSelected={span.isSelected ?? false}
          isDisabled={isDisabled}
          onChange={() => onToggle(span.id)}
          onHoverStart={() => show(true)}
          onHoverEnd={() => show(false)}
          onFocus={() => show(true)}
          onBlur={() => show(false)}
          {...attributes}
        >
          {body}
        </Checkbox>
      )}
      {tip}
    </TooltipTrigger>
  );
}

/** The lanes as one radio group where one span is picked at a time; as
 * they are otherwise, each span its own checkbox. */
function Picker({
  single,
  label,
  value,
  isDisabled,
  onChange,
  children,
}: {
  single: boolean;
  label: string;
  value: string | null;
  isDisabled: boolean;
  onChange?: (id: string) => void;
  children: ReactNode;
}) {
  if (!single) return children;
  return (
    <RadioGroup
      aria-label={label}
      value={value}
      isDisabled={isDisabled}
      onChange={onChange}
    >
      {children}
    </RadioGroup>
  );
}

const SHADE_PAD = 8;

const COARSE = "(pointer: coarse)";

/** Whether the primary pointer is a finger: a touch screen of any width gets
 * the spans' 24 px least width (#2310). */
function useCoarsePointer(): boolean {
  return useSyncExternalStore(
    (onChange) => {
      const query = window.matchMedia(COARSE);
      query.addEventListener("change", onChange);
      return () => query.removeEventListener("change", onChange);
    },
    () => window.matchMedia(COARSE).matches,
  );
}
