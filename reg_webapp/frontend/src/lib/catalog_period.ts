import type { CatalogPeriod } from "./api";
import { renderIntervals, sourcePeriodIntervals } from "./reg_core";

/** Catalog periods are half-open; reg-core renders inclusive date intervals. */
export function catalogLastDate(period: CatalogPeriod): string | null {
  if (period.end === null) return null;
  const last = new Date(`${period.end}T00:00:00Z`);
  last.setUTCDate(last.getUTCDate() - 1);
  return last.toISOString().slice(0, 10);
}

/** A reg-core period spelling for reading: each range with an en dash, the
 * segments comma-spaced (`2005–2008, 2012`). Presentation only; the wire with
 * `..` stays reg-core's. */
export function periodText(wire: string): string {
  return wire
    .split(",")
    .map((segment) => segment.replace("..", "–"))
    .join(", ");
}

export function catalogPeriodLabel(period: CatalogPeriod): string {
  if (period.start === null && period.end === null) return "Unbounded";
  const end = catalogLastDate(period);
  if (end === null)
    return `From ${period.start?.endsWith("-01-01") ? period.start.slice(0, 4) : period.start}`;
  if (period.start === null)
    return `Through ${end.endsWith("-12-31") ? end.slice(0, 4) : end}`;
  return periodText(renderIntervals([[period.start, end]]));
}

/** Project periods use the same inclusive renderer as catalog periods. */
export function sourcePeriodLabel(period: unknown): string | null {
  const parsed = sourcePeriodIntervals(period);
  if ("error" in parsed) return null;
  return parsed.intervals === null
    ? "Unbounded"
    : periodText(renderIntervals(parsed.intervals));
}

/** Coalesce half-open segment periods before displaying coverage runs. */
export function catalogPeriodRuns(periods: readonly CatalogPeriod[]): string {
  const sorted = periods
    .map((period) => ({ ...period }))
    .sort((a, b) =>
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
    ) {
      last.end =
        last.end === null || period.end === null
          ? null
          : last.end > period.end
            ? last.end
            : period.end;
    } else runs.push(period);
  }
  return runs.map(catalogPeriodLabel).join(", ");
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
