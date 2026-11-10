import type { CatalogPeriod } from "./api";
import { yearWindowLabel } from "./period";
import { sourcePeriodIntervals } from "./reg_core";

/** Catalog periods are half-open; reg-core renders inclusive date intervals. */
export function catalogLastDate(period: CatalogPeriod): string | null {
  if (period.end === null) return null;
  const last = new Date(`${period.end}T00:00:00Z`);
  last.setUTCDate(last.getUTCDate() - 1);
  return last.toISOString().slice(0, 10);
}

/** Inclusive ISO days, null for an open end. */
type Days = readonly [string | null, string | null];

/** Days as a reader sees them (DESIGN.md → Time): years, never a date or a
 * period token. Whole school years (1 July to 30 June) read as school years
 * (`1999/2000`, `1984/1985–2023/2024`); anything else as the calendar years
 * its first and last days fall in, so a 31 December snapshot is its year
 * (`1968–2025`). Spans join where they meet at that grain. Null when a span
 * has no dated bound: there are no years to state. This is display only;
 * `renderIntervals` writes the project file. */
function daysLabel(intervals: readonly Days[]): string | null {
  if (
    intervals.length === 0 ||
    intervals.some(([first, last]) => first === null && last === null)
  )
    return null;
  const year = (day: string | null) =>
    day === null ? null : Number(day.slice(0, 4));
  const school = intervals.every(
    ([first, last]) => first?.endsWith("-07-01") && last?.endsWith("-06-30"),
  );
  // A school year is numbered by the year it starts in.
  const spans = intervals
    .map(([first, last]) => ({
      from: year(first),
      to: school ? (year(last) as number) - 1 : year(last),
    }))
    .sort((a, b) => (a.from ?? -Infinity) - (b.from ?? -Infinity));
  const runs: { from: number | null; to: number | null }[] = [];
  for (const span of spans) {
    const run = runs.at(-1);
    if (
      run &&
      (run.to === null || span.from === null || span.from <= run.to + 1)
    )
      run.to =
        run.to === null || span.to === null ? null : Math.max(run.to, span.to);
    else runs.push({ ...span });
  }
  const name = (y: number) => (school ? `${y}/${y + 1}` : String(y));
  return runs
    .map(({ from, to }) =>
      from === null
        ? `Through ${name(to as number)}`
        : to === null
          ? `From ${name(from)}`
          : school
            ? from === to
              ? name(from)
              : `${name(from)}–${name(to)}`
            : yearWindowLabel({ from, to }),
    )
    .join(", ");
}

const catalogDays = (period: CatalogPeriod): Days => [
  period.start,
  catalogLastDate(period),
];

/** A catalog period's years, or null for one with no dated bound. */
export function catalogPeriodLabel(period: CatalogPeriod): string | null {
  return daysLabel([catalogDays(period)]);
}

/** A project source period's years, or null when it states none (unset,
 * malformed, or the year-independent `_default`). */
export function sourcePeriodLabel(period: unknown): string | null {
  const parsed = sourcePeriodIntervals(period);
  return "error" in parsed || parsed.intervals === null
    ? null
    : daysLabel(parsed.intervals);
}

/** The years of several catalog periods, joined where they meet. */
export function catalogPeriodRuns(
  periods: readonly CatalogPeriod[],
): string | null {
  return daysLabel(periods.map(catalogDays));
}

/** `requested` less `ordered`: the years a clip cannot deliver. Half-open ISO
 * days compare as strings; a null end is open. */
export function periodsLess(
  requested: readonly CatalogPeriod[],
  ordered: readonly CatalogPeriod[],
): CatalogPeriod[] {
  let rest = requested.map((period) => ({ ...period }));
  for (const cut of ordered) {
    rest = rest.flatMap((period) => {
      const overlaps =
        (cut.end === null || period.start === null || period.start < cut.end) &&
        (cut.start === null || period.end === null || cut.start < period.end);
      if (!overlaps) return [period];
      const pieces: CatalogPeriod[] = [];
      if (
        cut.start !== null &&
        (period.start === null || period.start < cut.start)
      )
        pieces.push({ start: period.start, end: cut.start });
      if (cut.end !== null && (period.end === null || period.end > cut.end))
        pieces.push({ start: cut.end, end: period.end });
      return pieces;
    });
  }
  return rest;
}
