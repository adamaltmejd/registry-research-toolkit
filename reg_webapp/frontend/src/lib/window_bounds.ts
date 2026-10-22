import type { Context } from "./api";

// The study window's bounds (#611 → Period model). A steward deployment can expose
// a best-effort catalog-wide year span; use it so filtered catalogs do not show
// decades of empty track. Global/unparseable deployments fall back to the
// historical 1960 → catalog-vintage range.
const FALLBACK_WINDOW_FLOOR_YEAR = 1960;

export interface WindowBounds {
  min: number;
  max: number;
  /** A steward's own span, which a stale window is clamped into. */
  enforced: boolean;
  /** The catalog vintage: the ceiling an open-ended coverage projects to.
   * Wall-clock is the pre-context fallback only. */
  vintageYear: number;
}

export function windowBounds(context: Context | null): WindowBounds {
  const vintageYear =
    Number(context?.import_date.slice(0, 4)) || new Date().getFullYear();
  const span = context?.period_span ?? null;
  return {
    min: span?.from ?? FALLBACK_WINDOW_FLOOR_YEAR,
    max: span?.to ?? vintageYear,
    enforced: span !== null,
    vintageYear,
  };
}
