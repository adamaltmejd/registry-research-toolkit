/**
 * `reg-core` in the browser: the ONLY importer of the `reg-core-wasm` module
 * (`crates/reg-core-wasm`, built into `./reg-core-wasm/` by `bun run gen:wasm`).
 *
 * The module loads once, before the app mounts (`main.ts` awaits `initRegCore`;
 * the test setups initialise it too). No top-level await: the wrappers below are
 * synchronous and throw when called before `initRegCore` settles, so a missed
 * initialisation fails loudly instead of skipping a check. JSON text crosses the
 * boundary; every export is total, so no input can abort the module.
 */

import type { ValidationResultModel } from "./api";
import type { ProjectSourcePeriod } from "./project_data";
import init, {
  check_project,
  initSync,
  merge,
  overlap,
  parse_period,
  period_token_for_bounds,
  project_schema_version,
  render,
  source_period_from_wire,
  source_period_intervals,
  source_period_to_wire,
  source_period_years,
} from "./reg-core-wasm/reg_core_wasm";

let ready = false;

/** Fetch and instantiate the module (the browser path). Idempotent. */
export async function initRegCore(): Promise<void> {
  if (!ready) {
    await init();
    ready = true;
  }
}

/** Instantiate the module from its bytes (the jsdom test setup, which has no
 * `fetch` of a file URL). Idempotent. */
export function initRegCoreSync(bytes: BufferSource): void {
  if (!ready) {
    initSync({ module: bytes });
    ready = true;
  }
}

function requireReady(): void {
  if (!ready) {
    throw new Error("reg-core is not initialised: await initRegCore() first");
  }
}

/** The `project_data.json` `schema_version` this build reads; a new draft takes it. */
export function projectSchemaVersion(): string {
  requireReady();
  return project_schema_version();
}

/** The server's structural door (`reg_core::project::check`) over `json`: the one
 * `unsupported_schema_version` issue, every structural issue, or `ok` with none.
 * Any JSON value is checked; text serde_json cannot read (not JSON, or nested
 * past its 128-level limit) is one `invalid_json` issue. */
export function checkProject(json: string): ValidationResultModel {
  requireReady();
  return JSON.parse(check_project(json)) as ValidationResultModel;
}

// ── Periods ──────────────────────────────────────────────────────────────────
// The SPA never parses period grammar or computes a date bound: every answer
// below is reg-core's (`crates/reg-core/src/grammar.rs`, `interval.rs`,
// `project.rs`). The oracles are conformance/cases/grammar/period.jsonl and
// source_period.jsonl, which reg_core.test.ts runs through this module.

/** An inclusive `[lo, hi]` interval of ISO dates. */
export type Interval = [string, string];

/** A `period.jsonl` kind. */
export type PeriodKind =
  | "year"
  | "month"
  | "day"
  | "term"
  | "school_year"
  | "quarter"
  | "half"
  | "range";

/** One period token or `from..to` range through the grammar: its canonical
 * spelling, kind, first and last day, and the calendar years it touches; or the
 * error catalog code that refuses it. The text is not trimmed. */
export type ParsedPeriod =
  | {
      canonical: string;
      kind: PeriodKind;
      bounds: Interval;
      years: [number, number];
    }
  | { error: string };

export function parsePeriod(text: string): ParsedPeriod {
  requireReady();
  return JSON.parse(parse_period(text)) as ParsedPeriod;
}

/** The coarsest period token whose days are exactly `lo..hi`, else the explicit
 * `lo..hi` (a term wins over the half-year it equals). */
export function periodTokenForBounds(lo: string, hi: string): string {
  requireReady();
  return period_token_for_bounds(lo, hi);
}

/** The `Source.period` a `?period` wire spells, shaped and not validated: a
 * comma list becomes segments, `from..to` a range object, a grammar year an int;
 * other text stays a trimmed string for the validator to report. A null wire is
 * the unset period `""`. */
export function sourcePeriodFromWire(wire: string | null): ProjectSourcePeriod {
  requireReady();
  return JSON.parse(source_period_from_wire(wire ?? "")) as ProjectSourcePeriod;
}

/** The `?period` wire of a `Source.period` (any draft value), or null when it
 * has none: blank, an empty list, or not a period's shape. Not validated. */
export function sourcePeriodToWire(period: unknown): string | null {
  requireReady();
  return JSON.parse(source_period_to_wire(JSON.stringify(period ?? null))) as
    | string
    | null;
}

/** The days a `Source.period` (any draft value) requests, merged: `intervals`
 * is null for the year-independent `"_default"`; `error` when the period's
 * structural check or its range order refuses it. */
export type SourcePeriodIntervals =
  | { intervals: Interval[] | null }
  | { error: string; message: string };

export function sourcePeriodIntervals(period: unknown): SourcePeriodIntervals {
  requireReady();
  return JSON.parse(
    source_period_intervals(JSON.stringify(period ?? null)),
  ) as SourcePeriodIntervals;
}

/** The calendar-year spans of a `Source.period` (any draft value), merged
 * (`2015..2017` and `2018` are one span), or null unless the period is valid and
 * every endpoint is a year. */
export function sourcePeriodYears(period: unknown): [number, number][] | null {
  requireReady();
  return JSON.parse(source_period_years(JSON.stringify(period ?? null))) as
    | [number, number][]
    | null;
}

/** `intervals` sorted and coalesced: overlapping and day-adjacent intervals join. */
export function mergeIntervals(intervals: readonly Interval[]): Interval[] {
  requireReady();
  return JSON.parse(merge(JSON.stringify(intervals))) as Interval[];
}

/** The intervals two ascending, disjoint lists share. */
export function overlapIntervals(
  a: readonly Interval[],
  b: readonly Interval[],
): Interval[] {
  requireReady();
  return JSON.parse(
    overlap(JSON.stringify(a), JSON.stringify(b)),
  ) as Interval[];
}

/** The period spelling of ascending, disjoint intervals: each its coarsest
 * token, else a range with year endpoints where they fall on a year's first or
 * last day (`2019..2020-06-30`); comma-joined. */
export function renderIntervals(intervals: readonly Interval[]): string {
  requireReady();
  return JSON.parse(render(JSON.stringify(intervals))) as string;
}
