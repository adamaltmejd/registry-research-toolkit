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
 * (`1968–2025`). Spans join where they meet at that grain. Null when they join into one
 * with no dated bound: there are no years to state. This is display only;
 * `renderIntervals` writes the project file. */
function daysLabel(intervals: readonly Days[]): string | null {
  if (intervals.length === 0) return null;
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
  if (runs.some(({ from, to }) => from === null && to === null)) return null;
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

const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

/** One day in words: `15 October 2019`. */
function dayText(day: string): string {
  const [year, month, date] = day.split("-").map(Number);
  return `${date} ${MONTHS[month - 1]} ${year}`;
}

/** Inclusive days in words, at the coarsest grain they fill exactly: a year,
 * a school year, a term (`autumn 2019`; a half year is the same days), a quarter
 * (`2019 Q3`), a month (`February 2020`) or a day; else a range of years
 * where an end falls on a year's edge and of days where it does not. */
function exactText([first, last]: readonly [string, string]): string {
  const year = first.slice(0, 4);
  const from = first.slice(5);
  const to = last.slice(5);
  if (year === last.slice(0, 4)) {
    const whole: Record<string, string> = {
      "01-01 12-31": year,
      "01-01 06-30": `spring ${year}`,
      "07-01 12-31": `autumn ${year}`,
      "01-01 03-31": `${year} Q1`,
      "04-01 06-30": `${year} Q2`,
      "07-01 09-30": `${year} Q3`,
      "10-01 12-31": `${year} Q4`,
    };
    const token = whole[`${from} ${to}`];
    if (token) return token;
    const month = Number(from.slice(0, 2));
    if (from.endsWith("-01") && from.slice(0, 2) === to.slice(0, 2)) {
      const next = new Date(Date.UTC(Number(year), month, 1));
      next.setUTCDate(0);
      if (next.toISOString().slice(0, 10) === last)
        return `${MONTHS[month - 1]} ${year}`;
    }
    if (first === last) return dayText(first);
  }
  if (from === "07-01" && to === "06-30")
    return daysLabel([[first, last]]) as string;
  const start = from === "01-01" ? year : dayText(first);
  const end = to === "12-31" ? last.slice(0, 4) : dayText(last);
  return start === end ? start : `${start}–${end}`;
}

/** A project source period as a reader sees it, or null when it states no
 * years (unset, malformed, or the year-independent `_default`). Whole years
 * and school years read as catalog periods do; a period the year thumbs
 * cannot write (a term, quarter, month or day, hand-written in the file)
 * reads exactly, in words (maintainer 2026-11-10, PR #2389). */
export function sourcePeriodLabel(period: unknown): string | null {
  const parsed = sourcePeriodIntervals(period);
  if ("error" in parsed || parsed.intervals === null) return null;
  const { intervals } = parsed;
  const whole = intervals.every(
    ([first, last]) =>
      (first.endsWith("-01-01") && last.endsWith("-12-31")) ||
      (first.endsWith("-07-01") && last.endsWith("-06-30")),
  );
  return whole ? daysLabel(intervals) : intervals.map(exactText).join(", ");
}

/** The years of several catalog periods, joined where they meet. Segments
 * split at a metadata boundary are coalesced first, so a school year cut in
 * January still reads as a school year. */
export function catalogPeriodRuns(
  periods: readonly CatalogPeriod[],
): string | null {
  const sorted = [...periods].sort((a, b) =>
    a.start === b.start
      ? 0
      : a.start === null
        ? -1
        : b.start === null
          ? 1
          : a.start < b.start
            ? -1
            : 1,
  );
  const runs: CatalogPeriod[] = [];
  for (const period of sorted) {
    const last = runs.at(-1);
    if (
      last &&
      (last.end === null || period.start === null || period.start <= last.end)
    )
      last.end =
        last.end === null || period.end === null
          ? null
          : last.end > period.end
            ? last.end
            : period.end;
    else runs.push({ ...period });
  }
  return daysLabel(runs.map(catalogDays));
}

/** The years of segments (or any items with a catalog period), joined: what a
 * drawing's year cells cannot say, such as school years. Empty when there are
 * no dated years. */
export function segmentYears(
  segments: readonly { period: CatalogPeriod }[],
): string {
  return catalogPeriodRuns(segments.map((segment) => segment.period)) ?? "";
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
