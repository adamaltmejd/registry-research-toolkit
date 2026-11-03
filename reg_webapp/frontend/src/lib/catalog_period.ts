import type { CatalogPeriod } from "./api";
import { renderIntervals, sourcePeriodIntervals } from "./reg_core";

/** Catalog periods are half-open; reg-core renders inclusive date intervals. */
export function catalogLastDate(period: CatalogPeriod): string | null {
  if (period.end === null) return null;
  const last = new Date(`${period.end}T00:00:00Z`);
  last.setUTCDate(last.getUTCDate() - 1);
  return last.toISOString().slice(0, 10);
}

export function catalogPeriodLabel(period: CatalogPeriod): string {
  if (period.start === null && period.end === null) return "Unbounded";
  const end = catalogLastDate(period);
  if (end === null)
    return `From ${period.start?.endsWith("-01-01") ? period.start.slice(0, 4) : period.start}`;
  if (period.start === null)
    return `Through ${end.endsWith("-12-31") ? end.slice(0, 4) : end}`;
  return renderIntervals([[period.start, end]]);
}

/** Project periods use the same inclusive renderer as catalog periods. */
export function sourcePeriodLabel(period: unknown): string | null {
  const parsed = sourcePeriodIntervals(period);
  if ("error" in parsed) return null;
  return parsed.intervals === null
    ? "Unbounded"
    : renderIntervals(parsed.intervals);
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
