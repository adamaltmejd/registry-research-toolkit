import type { Context } from "./api";
import type { ProjectStudyWindow } from "./project_data";

// The study window's bounds (#611 → Period model): the viewed catalog's first and
// last year (`/api/context` `period_span`: a steward's held periods, the global
// catalog's delivered windows), so the slider never shows decades of empty track.
// A catalog with no dated delivery falls back to the 1960 → catalog-vintage range.
const FALLBACK_WINDOW_FLOOR_YEAR = 1960;

export interface WindowBounds {
  min: number;
  max: number;
  /** The catalog's own span, which a stale window is clamped into. */
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

/** The first year of a new project's study window (DESIGN.md → Time). */
const DEFAULT_WINDOW_FROM_YEAR = 2015;

/** A new project's study window: 2015 to the catalog's last year, or the
 * whole span when the catalog ends before 2015. */
export function defaultStudyWindow(bounds: WindowBounds): ProjectStudyWindow {
  return bounds.max < DEFAULT_WINDOW_FROM_YEAR
    ? { from: bounds.min, to: bounds.max }
    : { from: Math.max(bounds.min, DEFAULT_WINDOW_FROM_YEAR), to: bounds.max };
}
