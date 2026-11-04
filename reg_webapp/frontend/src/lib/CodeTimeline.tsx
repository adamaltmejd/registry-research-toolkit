import { useCallback, useId, useState } from "react";
import {
  type CatalogSegment,
  getValues,
  type ValueItem,
  type ValueSetRef,
} from "./api";
import styles from "./CodeTimeline.module.css";
import { laneGeometry } from "./lane_graph";
import { yearWindowLabel } from "./period";
import type { ProjectStudyWindow } from "./project_data";
import { columnYearRuns } from "./study_window";
import { Button } from "./ui/Button";
import { ColumnChip } from "./ui/ColumnChip";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { ValueSetCodes } from "./ValueSetCodes";

/** A code list shown whole, without asking: at most this many codes. */
const SHOWN = 10;
/** Two code lists sharing this share of their code and label pairs are one
 * block (PLAN §14, round 2). */
const RELATED = 0.3;

// Codes are often numbers: "10" sorts after "9".
const codeOrder = new Intl.Collator("sv", {
  sensitivity: "base",
  numeric: true,
});

interface Use {
  column: string;
  years: ProjectStudyWindow[];
}

interface CodeList {
  valueSet: ValueSetRef;
  uses: Use[];
}

/** One code list's codes, every page of them. */
async function allValues(hash: string, signal: AbortSignal) {
  const items: ValueItem[] = [];
  let cursor: string | undefined;
  do {
    const page = await getValues(
      { value_set: hash },
      { limit: 200, cursor },
      { signal },
    );
    items.push(...page.items);
    cursor = page.next_cursor ?? undefined;
  } while (cursor);
  return items;
}

const pair = (item: ValueItem) => `${item.code}\u0000${item.label ?? ""}`;

/** Lists that belong together: one column codes both, or they share
 * `RELATED` of their code and label pairs. A list too long to fetch unasked
 * joins only by column. */
function blocks(
  lists: readonly CodeList[],
  codes: ReadonlyMap<string, readonly ValueItem[]>,
): CodeList[][] {
  const parent = lists.map((_, i) => i);
  const root = (i: number): number =>
    parent[i] === i ? i : (parent[i] = root(parent[i]));
  const pairs = lists.map((list) => {
    const items = codes.get(list.valueSet.hash);
    return items ? new Set(items.map(pair)) : null;
  });
  // simplify: pairwise over a group's code lists, a few dozen at most.
  for (let i = 0; i < lists.length; i += 1) {
    for (let j = i + 1; j < lists.length; j += 1) {
      const columns = new Set(lists[i].uses.map((use) => use.column));
      const sameColumn = lists[j].uses.some((use) => columns.has(use.column));
      const a = pairs[i];
      const b = pairs[j];
      const shared = a && b ? [...a].filter((key) => b.has(key)).length : 0;
      const related = a && b && shared / new Set([...a, ...b]).size >= RELATED;
      if (sameColumn || related) parent[root(i)] = root(j);
    }
  }
  const grouped = new Map<number, CodeList[]>();
  lists.forEach((list, i) => {
    const key = root(i);
    grouped.set(key, [...(grouped.get(key) ?? []), list]);
  });
  return [...grouped.values()];
}

/** The year runs of a set of years. */
function runsOf(years: Iterable<number>): ProjectStudyWindow[] {
  const sorted = [...new Set(years)].sort((a, b) => a - b);
  const runs: ProjectStudyWindow[] = [];
  for (const year of sorted) {
    const last = runs.at(-1);
    if (last && year === last.to + 1) last.to = year;
    else runs.push({ from: year, to: year });
  }
  return runs;
}

const yearsIn = (runs: readonly ProjectStudyWindow[]) =>
  runs.flatMap((run) =>
    Array.from({ length: run.to - run.from + 1 }, (_, i) => run.from + i),
  );

const label = (runs: readonly ProjectStudyWindow[]) =>
  runs.map(yearWindowLabel).join(", ");

/**
 * A group's codes on the page's axis (DESIGN.md → Codes): one row per code and
 * label, a box over the years a column uses it, a light track where the
 * block's columns have codes. Lists one column codes, or that share most of
 * their codes, are one block; with several blocks each is headed by its
 * columns. A block with a long list waits for "Show codes".
 */
export function CodeTimeline({
  segments,
  axis,
  bounds,
}: {
  /** Every member's per-period rows. */
  segments: readonly CatalogSegment[];
  axis: [number, number];
  bounds: readonly [number, number];
}) {
  const byHash = new Map<string, CodeList>();
  for (const segment of segments) {
    if (!segment.value_set) continue;
    const list = byHash.get(segment.value_set.hash) ?? {
      valueSet: segment.value_set,
      uses: [],
    };
    list.uses.push({
      column: segment.column,
      years: columnYearRuns([segment], bounds, axis[0]),
    });
    byHash.set(segment.value_set.hash, list);
  }
  const lists = [...byHash.values()];
  // A hierarchy is browsed level by level, not drawn: its own block.
  const flat = lists.filter((list) => !list.valueSet.hierarchical);
  const trees = lists.filter((list) => list.valueSet.hierarchical);
  const small = flat
    .filter((list) => list.valueSet.n_items <= SHOWN)
    .map((list) => list.valueSet.hash);
  const smallKey = small.join("\n");
  const eager = useAsyncResource(smallKey || null, async (signal) => {
    const fetched = await Promise.all(
      small.map((hash) => allValues(hash, signal)),
    );
    return new Map(small.map((hash, i) => [hash, fetched[i]]));
  });
  const headingId = useId();
  const [width, setWidth] = useState(0);
  const measure = useCallback((node: HTMLDivElement | null) => {
    if (!node) return;
    const observer = new ResizeObserver(([entry]) => {
      setWidth(Math.floor(entry.contentRect.width));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);
  if (lists.length === 0) return null;
  const known = eager.data ?? new Map<string, ValueItem[]>();
  const all = [
    ...(eager.loading ? [] : blocks(flat, known)),
    ...trees.map((list) => [list]),
  ];
  // The lanes' track: the same rule on the same width puts each year where
  // the timeline above has it.
  const geometry =
    width > 0
      ? laneGeometry({
          lanes: [],
          spans: [],
          edges: [],
          axis,
          window: null,
          width,
        })
      : null;
  return (
    <section className={styles.section} aria-labelledby={headingId}>
      <h2 id={headingId} className={`micro-label ${styles.heading}`}>
        Codes
      </h2>
      <div ref={measure} className={styles.frame}>
        {eager.loading ? (
          <div aria-busy="true">
            <Skeleton count={2} />
          </div>
        ) : eager.error ? (
          <StatusRow tone="error" live="alert">
            The codes did not load ({eager.error}). Reload the page to try
            again.
          </StatusRow>
        ) : (
          geometry && (
            <>
              {all.map((block) => (
                <Block
                  key={block.map((list) => list.valueSet.hash).join(" ")}
                  lists={block}
                  codes={known}
                  axis={axis}
                  geometry={geometry}
                  headed={all.length > 1}
                />
              ))}
            </>
          )
        )}
      </div>
    </section>
  );
}

type Geometry = ReturnType<typeof laneGeometry>;

function Axis({ geometry }: { geometry: Geometry }) {
  return (
    <div className={styles.axis} aria-hidden="true">
      {geometry.ticks.map((tick) => (
        <span key={tick.year} className={styles.tick} style={{ left: tick.x }}>
          {tick.year}
        </span>
      ))}
    </div>
  );
}

function Block({
  lists,
  codes,
  axis,
  geometry,
  headed,
}: {
  lists: readonly CodeList[];
  codes: ReadonlyMap<string, readonly ValueItem[]>;
  axis: [number, number];
  geometry: Geometry;
  headed: boolean;
}) {
  const [asked, setAsked] = useState(false);
  const columns = [
    ...new Set(lists.flatMap((list) => list.uses.map((use) => use.column))),
  ];
  const head = headed && (
    <p className={styles.columns}>
      {columns.map((column) => (
        <ColumnChip key={column} name={column} />
      ))}
    </p>
  );
  if (lists[0].valueSet.hierarchical)
    return (
      <div className={styles.block}>
        {head}
        <ValueSetCodes
          subject={{ value_set: lists[0].valueSet.hash }}
          valueSet={lists[0].valueSet}
        />
      </div>
    );
  const missing = lists.filter((list) => !codes.has(list.valueSet.hash));
  if (missing.length && !asked)
    return (
      <div className={styles.block}>
        {head}
        <p className={styles.ask}>
          <Button onPress={() => setAsked(true)}>Show codes</Button>
          <span className={styles.hint}>
            Each column has up to{" "}
            {Math.max(...lists.map((list) => list.valueSet.n_items))} codes.
          </span>
        </p>
      </div>
    );
  return (
    <div className={styles.block}>
      {head}
      {missing.length ? (
        <Fetched lists={lists} known={codes} axis={axis} geometry={geometry} />
      ) : (
        <Rows lists={lists} codes={codes} axis={axis} geometry={geometry} />
      )}
    </div>
  );
}

function Fetched({
  lists,
  known,
  axis,
  geometry,
}: {
  lists: readonly CodeList[];
  known: ReadonlyMap<string, readonly ValueItem[]>;
  axis: [number, number];
  geometry: Geometry;
}) {
  const hashes = lists
    .map((list) => list.valueSet.hash)
    .filter((hash) => !known.has(hash));
  const resource = useAsyncResource(hashes.join("\n"), async (signal) => {
    const fetched = await Promise.all(
      hashes.map((hash) => allValues(hash, signal)),
    );
    return new Map([
      ...known,
      ...hashes.map((hash, i) => [hash, fetched[i]] as const),
    ]);
  });
  if (resource.loading)
    return (
      <div aria-busy="true">
        <Skeleton count={3} />
      </div>
    );
  if (resource.error || !resource.data)
    return (
      <StatusRow tone="error" live="alert">
        The codes did not load ({resource.error}). Reload the page to try again.
      </StatusRow>
    );
  return (
    <Rows lists={lists} codes={resource.data} axis={axis} geometry={geometry} />
  );
}

interface Row {
  code: string;
  label: string;
  /** The columns using it, by year. */
  years: Map<number, Set<string>>;
}

function Rows({
  lists,
  codes,
  axis,
  geometry,
}: {
  lists: readonly CodeList[];
  codes: ReadonlyMap<string, readonly ValueItem[]>;
  axis: [number, number];
  geometry: Geometry;
}) {
  // The columns with codes in this block, by year.
  const coded = new Map<number, Set<string>>();
  const rows = new Map<string, Row>();
  for (const list of lists) {
    for (const use of list.uses) {
      for (const year of yearsIn(use.years))
        coded.set(year, (coded.get(year) ?? new Set()).add(use.column));
    }
    for (const item of codes.get(list.valueSet.hash) ?? []) {
      const key = pair(item);
      const row = rows.get(key) ?? {
        code: item.code,
        label: item.label ?? "",
        years: new Map(),
      };
      for (const use of list.uses) {
        for (const year of yearsIn(use.years))
          row.years.set(
            year,
            (row.years.get(year) ?? new Set()).add(use.column),
          );
      }
      rows.set(key, row);
    }
  }
  const sorted = [...rows.values()].sort(
    (a, b) =>
      codeOrder.compare(a.code, b.code) ||
      Math.min(...a.years.keys()) - Math.min(...b.years.keys()),
  );
  const track = runsOf(coded.keys());
  const columnYears = new Map<string, Set<number>>();
  for (const [year, columns] of coded) {
    for (const column of columns)
      columnYears.set(column, (columnYears.get(column) ?? new Set()).add(year));
  }
  // The name column of the timeline above, then its track.
  const columnsStyle = geometry.stacked
    ? "minmax(0, 1fr)"
    : `${geometry.trackX - 16}px minmax(0, 1fr)`;
  const x = (year: number) =>
    ((year - axis[0]) / (axis[1] + 1 - axis[0])) * geometry.trackWidth;
  const bar = (run: ProjectStudyWindow) => ({
    left: x(run.from),
    width: Math.max(x(run.to + 1) - x(run.from), 2),
  });
  return (
    <>
      {/* The axis heads each drawn block, never a block still to be asked. */}
      <Axis geometry={geometry} />
      <ol className={styles.rows} data-stacked={geometry.stacked || undefined}>
        {sorted.map((row, index) => {
          const used = runsOf(row.years.keys());
          // Columns that have codes in a year this code is used, but not this
          // one: named under the label, with years only where that is part of
          // the column's years here.
          const lacking = new Map<string, number[]>();
          for (const [year, columns] of coded) {
            const using = row.years.get(year);
            if (!using) continue;
            for (const column of columns)
              if (!using.has(column))
                lacking.set(column, [...(lacking.get(column) ?? []), year]);
          }
          const users = [
            ...new Set([...row.years.values()].flatMap((set) => [...set])),
          ];
          const note =
            lacking.size === 0 ? null : users.length < lacking.size ? (
              <>
                Only in{" "}
                {users.map((column) => (
                  <ColumnChip key={column} name={column} />
                ))}
              </>
            ) : (
              <>
                Not in{" "}
                {[...lacking].map(([column, years]) => {
                  const own = [...(columnYears.get(column) ?? [])].filter(
                    (year) => row.years.has(year),
                  );
                  const whole = label(runsOf(years)) === label(runsOf(own));
                  return (
                    <span key={column} className={styles.lack}>
                      <ColumnChip name={column} />
                      {!whole && ` ${label(runsOf(years))}`}
                    </span>
                  );
                })}
              </>
            );
          const continued = index > 0 && sorted[index - 1].code === row.code;
          return (
            <li
              key={`${row.code}\u0000${row.label}`}
              className={styles.row}
              data-continued={continued || undefined}
              style={{ gridTemplateColumns: columnsStyle }}
            >
              <span className={styles.text}>
                <code className={styles.code}>{continued ? "" : row.code}</code>
                <span className={styles.label}>
                  {row.label}
                  <span className="visually-hidden">, used {label(used)}</span>
                  {note && <span className={styles.note}>{note}</span>}
                </span>
              </span>
              <span className={styles.track} aria-hidden="true">
                {track.map((run) => (
                  <span
                    key={run.from}
                    className={styles.base}
                    style={bar(run)}
                  />
                ))}
                {used.map((run) => (
                  <span
                    key={run.from}
                    className={styles.box}
                    style={bar(run)}
                  />
                ))}
              </span>
            </li>
          );
        })}
      </ol>
    </>
  );
}
