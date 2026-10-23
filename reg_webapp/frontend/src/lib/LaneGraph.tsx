import { type ReactNode, useCallback, useId, useState } from "react";
import styles from "./LaneGraph.module.css";
import {
  COVERAGE_HEIGHT,
  type LaneEdgeKind,
  type LaneGeometry,
  type LaneSpanGeometryInput,
  labelWidth,
  laneGeometry,
  type PlacedSpan,
  SPAN_HEIGHT,
} from "./lane_graph";
import { coverageMarks } from "./period";
import type { ProjectStudyWindow } from "./project_data";
import { ColumnChip, type ColumnChipState } from "./ui/ColumnChip";

export interface LaneModel {
  id: string;
  name: string;
  /** The line under the name: what tells this lane from its neighbours. */
  sub?: { text: string; mono?: boolean } | null;
  /** The variable's own page; the name links there. */
  href?: string | null;
  /** The variable this page is about, on `accent-soft`. */
  current?: boolean;
  /** Its relations as sentences, for a screen reader: the edges are drawn
   * only. */
  relations?: string[];
  /** Columns the catalog knows but that were never delivered: no years to
   * place, so they sit under the name. */
  undelivered?: string[];
}

export interface LaneSpanModel extends LaneSpanGeometryInput {
  /** "2015–2019", "2020–", or "year-independent". */
  years: string;
  state: ColumnChipState;
  /** A pending change to the project, shown until Apply. */
  staged?: "add" | "remove" | null;
  /** In the project through a source the study window has moved off. */
  blocked?: boolean;
  /** How the committed column relates to the study window, where it is more
   * than "in project": shown as the span's tooltip and listed under the
   * graph by the consumer. */
  note?: { text: string; tone: "info" | "error" } | null;
  /** Years the column's coding changed under the same name. */
  changes?: number[];
  /** A checkbox when true; a mark to read otherwise. */
  selectable: boolean;
  isSelected?: boolean;
  /** Starts with the column name, then the variable, the years and the
   * state. */
  accessibleName: string;
}

export interface LaneEdgeModel {
  id: string;
  from: string;
  to: string;
  kind: LaneEdgeKind;
}

/** The variable page's coverage row (DESIGN.md → Time → Timeline), drawn
 * between the axis and the lanes. */
export interface LaneCoverage {
  /** The coverage edges, open ends resolved; null when unknown. */
  covered: ProjectStudyWindow | null;
  /** The delivered years, or null to draw coverage as one bar. */
  years: readonly number[] | null;
  /** The band's years outside coverage, in `info-bg` on this row. */
  outside: readonly ProjectStudyWindow[];
  /** What the band is, for the legend. */
  bandName: string | null;
}

interface Props {
  label: string;
  lanes: LaneModel[];
  spans: LaneSpanModel[];
  edges: LaneEdgeModel[];
  /** Inclusive first and last year. */
  axis: [number, number];
  /** The active window, shaded as a band; null draws none. */
  window: [number, number] | null;
  coverage?: LaneCoverage;
  /** Laid over the coverage row's track (the period slider), on `axis`. */
  overlay?: (axis: [number, number]) => ReactNode;
  isDisabled?: boolean;
  onToggle: (spanId: string) => void;
}

/**
 * Lanes over time (DESIGN.md → Time → LaneGraph): one lane per variable, each
 * column a span in the ColumnChip shape on one shared axis, relations as
 * arrows, solid for succession and dashed for a rename, and the window as an
 * `accent-bg` band. The marks are one decorative SVG; the lanes and spans are
 * HTML over it, so every span is a real checkbox in reading order, named with
 * its column, variable and years. Tab walks the spans lane by lane and Space
 * toggles one. On a variable page a coverage row heads the lanes, so the
 * Timeline and the columns are one drawing on one axis (#1833).
 */
export function LaneGraph({
  label,
  lanes,
  spans,
  edges,
  axis,
  window,
  coverage,
  overlay,
  isDisabled = false,
  onToggle,
}: Props) {
  const [width, setWidth] = useState(0);
  const [hovered, setHovered] = useState<string | null>(null);
  const markerId = useId();
  // The one DOM measurement: the graph's width, which the layout needs to
  // place labels and pack rows.
  const measure = useCallback((node: HTMLDivElement | null) => {
    if (!node) return;
    const observer = new ResizeObserver(([entry]) => {
      setWidth(Math.floor(entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const geometry =
    width > 0
      ? laneGeometry({
          lanes,
          spans,
          edges,
          axis,
          window,
          width,
          coverageRow: coverage !== undefined,
        })
      : null;
  const spansByLane = new Map<string, LaneSpanModel[]>();
  for (const span of spans) {
    spansByLane.set(span.lane, [...(spansByLane.get(span.lane) ?? []), span]);
  }
  const kinds = new Set(geometry?.edges.map((e) => e.kind));

  return (
    <div className={styles.frame}>
      <div
        ref={measure}
        className={styles.graph}
        data-stacked={geometry?.stacked || undefined}
        style={{ height: geometry?.height ?? 0 }}
      >
        {geometry && (
          <>
            <Marks
              geometry={geometry}
              lanes={lanes}
              axis={axis}
              coverage={coverage}
              width={width}
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
                {overlay(axis)}
              </div>
            )}
            {!geometry.stacked && (
              <span
                className={styles.axisCaption}
                style={{ top: geometry.axisY - 18 }}
                aria-hidden="true"
              >
                Variable
              </span>
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
                Coverage
              </span>
            )}
            <ol className={styles.lanes} aria-label={label}>
              {geometry.lanes.map((placed) => {
                const lane = lanes.find((l) => l.id === placed.id);
                if (!lane) return null;
                return (
                  <li
                    key={lane.id}
                    className={styles.lane}
                    style={{ top: placed.y, height: placed.height }}
                  >
                    <LaneName
                      lane={lane}
                      width={geometry.stacked ? width : geometry.trackX - 16}
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
                            />
                          </li>
                        ) : null;
                      })}
                    </ul>
                  </li>
                );
              })}
            </ol>
          </>
        )}
      </div>
      {spans.some((s) => s.selectable) && (
        <p className={styles.hint}>
          Click a column to tick it for the project.
        </p>
      )}
      <Legend
        coverage={coverage}
        succession={kinds.has("succession")}
        rename={kinds.has("rename")}
        inProject={spans.some((s) => s.state === "in")}
        blocked={spans.some((s) => s.blocked)}
        window={window !== null}
      />
    </div>
  );
}

function Marks({
  geometry,
  lanes,
  axis,
  coverage,
  width,
  hovered,
  markerId,
}: {
  geometry: LaneGeometry;
  lanes: LaneModel[];
  axis: [number, number];
  coverage: LaneCoverage | undefined;
  width: number;
  hovered: string | null;
  markerId: string;
}) {
  const marker = `${markerId}-arrow`;
  const hatch = `${markerId}-hatch`;
  const bottom = geometry.height;
  const x = (year: number) =>
    geometry.trackX +
    ((year - axis[0]) / (axis[1] + 1 - axis[0])) * geometry.trackWidth;
  const run = (r: ProjectStudyWindow) => ({
    x: x(r.from),
    width: x(r.to + 1) - x(r.from),
  });
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
        <pattern
          id={hatch}
          width="4"
          height="4"
          patternUnits="userSpaceOnUse"
          patternTransform="rotate(45)"
        >
          <rect width="2" height="4" className={styles.hatch} />
        </pattern>
      </defs>
      {geometry.lanes.map((lane) =>
        lanes.find((l) => l.id === lane.id)?.current ? (
          <rect
            key={lane.id}
            className={styles.current}
            x={0}
            y={lane.y}
            width={width}
            height={lane.height}
          />
        ) : null,
      )}
      {geometry.window && (
        <>
          <rect
            className={styles.band}
            x={geometry.window.x}
            y={geometry.axisY}
            width={geometry.window.width}
            height={bottom - geometry.axisY}
          />
          {[geometry.window.x, geometry.window.x + geometry.window.width].map(
            (x) => (
              <line
                key={x}
                className={styles.bound}
                x1={x}
                x2={x}
                y1={geometry.axisY}
                y2={bottom}
              />
            ),
          )}
        </>
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
          {marks.gaps.map((r) => (
            <rect
              key={`g${r.from}`}
              fill={`url(#${hatch})`}
              y={cy + 14}
              height={12}
              {...run(r)}
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
      {geometry.lanes.map((lane) => (
        <line
          key={lane.id}
          className={styles.rule}
          x1={0}
          x2={width}
          y1={lane.y + lane.height}
          y2={lane.y + lane.height}
        />
      ))}
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

function LaneName({ lane, width }: { lane: LaneModel; width: number }) {
  return (
    <div className={styles.name} style={{ width }}>
      {lane.href ? (
        <a className={`link ${styles.nameText}`} href={lane.href}>
          {lane.name}
        </a>
      ) : (
        <span className={styles.nameText}>{lane.name}</span>
      )}
      {lane.sub && (
        <span className={`${styles.sub} ${lane.sub.mono ? styles.mono : ""}`}>
          {lane.sub.text}
        </span>
      )}
      {lane.undelivered && lane.undelivered.length > 0 && (
        <span className={styles.undelivered}>
          {lane.undelivered.map((column) => (
            <ColumnChip key={column} name={column} state="out" />
          ))}
          <span className={styles.sub}>never delivered</span>
        </span>
      )}
      {lane.relations && lane.relations.length > 0 && (
        <span className="visually-hidden">{lane.relations.join(". ")}.</span>
      )}
    </div>
  );
}

function Span({
  span,
  at,
  isDisabled,
  onToggle,
  onActive,
}: {
  span: LaneSpanModel;
  at: PlacedSpan;
  isDisabled: boolean;
  onToggle: (id: string) => void;
  onActive: (id: string | null) => void;
}) {
  const showYears =
    at.label === "inside" &&
    at.segments.length === 1 &&
    labelWidth(`${span.name} ${span.years}`) <= at.width;
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
      <ColumnChip
        // Positional: the segments are rebuilt whole from the span.
        // oxlint-disable-next-line react/no-array-index-key
        key={i}
        name={name}
        years={showYears ? span.years : undefined}
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
          {span.staged === "add" ? "+" : "−"}
        </span>
      )}
      {span.blocked && !span.staged && (
        <span className={styles.glyph} aria-hidden="true">
          ✕
        </span>
      )}
      {at.label !== "inside" && (
        <span className={styles.outside} data-side={at.label}>
          {span.name}
        </span>
      )}
      {(span.changes ?? []).map((year) => (
        <span
          key={year}
          className={styles.change}
          style={{
            left: Math.max(
              0,
              ((year - span.from) / (span.to + 1 - span.from)) * at.width,
            ),
          }}
          aria-hidden="true"
        >
          ▲ {year}
        </span>
      ))}
    </>
  );
  const attributes = {
    "data-state": span.state,
    "data-staged": span.staged ?? undefined,
    "data-blocked": span.blocked || undefined,
  };
  if (!span.selectable) {
    return (
      <span className={styles.span} title={span.note?.text} {...attributes}>
        {body}
        <span className="visually-hidden">{span.accessibleName}</span>
      </span>
    );
  }
  return (
    <label
      className={`${styles.span} ${styles.pick}`}
      style={{ height: SPAN_HEIGHT }}
      title={span.note?.text}
      data-disabled={isDisabled || undefined}
      {...attributes}
    >
      {body}
      <input
        type="checkbox"
        className={styles.hit}
        aria-label={span.accessibleName}
        checked={span.isSelected ?? false}
        disabled={isDisabled}
        onChange={() => onToggle(span.id)}
        onMouseEnter={() => onActive(span.id)}
        onMouseLeave={() => onActive(null)}
        onFocus={() => onActive(span.id)}
        onBlur={() => onActive(null)}
      />
    </label>
  );
}

function Legend({
  coverage,
  succession,
  rename,
  inProject,
  blocked,
  window,
}: {
  succession: boolean;
  rename: boolean;
  inProject: boolean;
  blocked: boolean;
  window: boolean;
  coverage: LaneCoverage | undefined;
}) {
  return (
    <ul className={styles.legend} aria-hidden="true">
      {coverage?.bandName && (
        <li>
          <i className={styles.keyBand} />
          {coverage.bandName}
        </li>
      )}
      {coverage && (
        <li>
          <i className={styles.keyCoverage} />
          Delivered years
        </li>
      )}
      {coverage &&
        coverageMarks(coverage.covered, coverage.years).gaps.length > 0 && (
          <li>
            <i className={styles.keyGap} />
            Gap inside coverage
          </li>
        )}
      {succession && (
        <li>
          <i className={styles.keySuccession} />
          succeeded by (definition change)
        </li>
      )}
      {rename && (
        <li>
          <i className={styles.keyRename} />
          renamed, same definition
        </li>
      )}
      {inProject && (
        <li>
          <ColumnChip name="COL" state="in" /> in project
        </li>
      )}
      {blocked && (
        <li>
          <span className={styles.keyBlocked}>✕</span> in project, outside the
          study window
        </li>
      )}
      {window && (
        <li>
          <ColumnChip name="COL" state="out" /> outside the{" "}
          {coverage?.bandName?.toLowerCase() ?? "window"}
        </li>
      )}
    </ul>
  );
}
