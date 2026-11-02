import type { CatalogPeriod } from "./api";
import { renderIntervals } from "./reg_core";

/** Catalog periods are half-open; reg-core renders inclusive date intervals. */
export function catalogPeriodLabel(period: CatalogPeriod): string {
  if (period.start === null && period.end === null) return "Unbounded";
  if (period.end === null) return `From ${period.start}`;
  const last = new Date(`${period.end}T00:00:00Z`);
  last.setUTCDate(last.getUTCDate() - 1);
  const end = last.toISOString().slice(0, 10);
  if (period.start === null) return `Through ${end}`;
  return renderIntervals([[period.start, end]]);
}
