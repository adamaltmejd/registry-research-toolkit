/**
 * Pure period/query helpers for the binding-leaf resolution state (no runes —
 * unit-testable in isolation; `period.test.ts`). The resolution state lives in
 * the URL query (`?period`/`?variant`/`?value_set_version`; see
 * reg_webapp/DESIGN.md → Catalog router structure); these helpers shape the wire
 * period for UI/project behavior and build the query string the router navigates to.
 *
 * The SPA never parses period grammar or computes a date bound: every period
 * answer here comes from reg-core through `reg_core.ts` (WASM) — token bounds,
 * wire shaping, the days a source period requests, merging and rendering. What
 * stays here is view state and year-integer arithmetic over reg-core's answers.
 */

import type {
  ProjectPeriodSegment,
  ProjectSourcePeriod,
  ProjectStudyWindow,
} from "./project_data";
import {
  type Interval,
  mergeIntervals,
  overlapIntervals,
  parsePeriod,
  renderIntervals,
  sourcePeriodFromWire,
  sourcePeriodIntervals,
  sourcePeriodToWire,
  sourcePeriodYears,
} from "./reg_core";

/** The narrowing modifiers carried in the URL query alongside `?period`. */
export interface ResolutionParams {
  period?: string;
  variant?: string;
  value_set_version?: string;
}

/** Sentinel `?value_set_version` selecting the empty/default label (a state with
 * `value_set_version_label === ""`). The empty string can't ride in the query
 * (≡ absent), so the picker sends this for the unlabeled-version option; the Rust
 * `states` operation reads it as `""`. MUST match `NO_VERSION` in
 * `crates/reg-catalog/src/ops/states.rs`. */
export const VALUE_SET_VERSION_NONE = "_none";

// ── Period answers from reg-core ─────────────────────────────────────────────

/** The days a `Source.period` (any draft value) requests, merged, or null when
 * it requests none the SPA can place: the year-independent `_default`, an unset
 * period, or one reg-core's structural check refuses (all-or-nothing: one bad
 * list member refuses the whole period). */
export function datedIntervals(period: unknown): Interval[] | null {
  const days = sourcePeriodIntervals(period);
  return "intervals" in days ? days.intervals : null;
}

/** Whether a `?period` wire is a period a source may hold: the year-independent
 * `_default`, or a dated period reg-core accepts (an unsorted or overlapping list
 * and an inverted range are refused). Used where a wire is written straight into
 * project_data. */
export function periodWireValid(wire: string): boolean {
  return !("error" in sourcePeriodIntervals(sourcePeriodFromWire(wire)));
}

/** The text as a bare grammar year (19xx/20xx), or null. Stricter than "any
 * integer" on purpose: a typo like "202" is not a year the wire accepts. */
function wireYear(raw: string): number | null {
  const parsed = parsePeriod(raw.trim());
  return "kind" in parsed && parsed.kind === "year" ? parsed.years[0] : null;
}

/** The period that requests exactly `days`: merged, rendered, shaped back from
 * the rendered wire (each interval its coarsest token, else a range with year
 * endpoints where they fall on a year's first or last day). */
function coveragePeriod(days: readonly Interval[]): ProjectSourcePeriod {
  return sourcePeriodFromWire(renderIntervals(mergeIntervals(days)));
}

function sameIntervals(a: readonly Interval[], b: readonly Interval[]) {
  return JSON.stringify(a) === JSON.stringify(b);
}

/** Whether a period is the UNSET/empty value — the fresh-source no-period marker
 * (`""`, what `sourcePeriodFromWire(null)` yields) or an empty segment list
 * (`[]`). Blank `{from,to}` endpoints ride the string/number arms, so this only
 * tests the two true "no period" shapes. */
function isEmptyPeriod(period: ProjectSourcePeriod): boolean {
  if (Array.isArray(period)) {
    return period.length === 0;
  }
  return typeof period === "string" && period.trim() === "";
}

/** Two of a Y-101 period-row list's segments that SHARE a year, by their original
 * row index (ascending) — what `normalizePeriodRows` returns instead of a
 * `Period` when the rows can't be coalesced without silently discarding which
 * row the researcher meant. */
export interface PeriodRowOverlap {
  a: number;
  b: number;
}

/**
 * Normalise a Y-101 source-period ROW LIST — one already year-valid window per
 * row (see `resolveYearEntry`), in the order the researcher entered them — into
 * the `Period` that requests the same years: sorted ascending, touching rows
 * merged into one span, a lone survivor a scalar rather than a one-element list.
 * Two windows that actually SHARE a year return their row indices instead: two
 * rows the researcher is authoring side by side are a replace, and merging across
 * a genuine overlap would erase which row was which — so this refuses rather than
 * coalescing silently. Pure — unit-tested in `period.test.ts`.
 */
export function normalizePeriodRows(
  windows: ProjectStudyWindow[],
): { period: ProjectSourcePeriod } | PeriodRowOverlap {
  for (let a = 0; a < windows.length; a++) {
    for (let b = a + 1; b < windows.length; b++) {
      if (
        windows[a].from <= windows[b].to &&
        windows[b].from <= windows[a].to
      ) {
        return { a, b };
      }
    }
  }
  if (windows.length === 0) {
    return { period: [] };
  }
  return {
    period: coveragePeriod(windows.flatMap((w) => datedIntervals(w) ?? [])),
  };
}

/** Extend a source's period to cover an incoming one as well (#992: a source is
 * keyed by `register_variant` alone, so a second add of the same variant extends
 * its period). The union of the days both request, through reg-core: merged
 * (overlapping and day-adjacent days join). When both periods are years only the
 * union is rendered whole (`2005..2010,2015..2020`, a string year becoming an
 * int). Otherwise a segment whose days survive the merge unchanged keeps the
 * spelling it was written in (`2020-H1` plus `2022` is `2020-H1,2022`), and only
 * an interval the merge created or changed is rendered (`2019-Q1` and `2019-Q2`
 * become `VT2019`). An unset side yields the other; when either side has no days
 * the SPA can place (`_default`, or a period reg-core refuses) the incoming
 * period wins — the user's most recent explicit choice. */
export function periodCoverageUnion(
  existing: ProjectSourcePeriod,
  incoming: ProjectSourcePeriod,
): ProjectSourcePeriod {
  if (isEmptyPeriod(incoming)) {
    return existing;
  }
  if (isEmptyPeriod(existing)) {
    return incoming;
  }
  const have = datedIntervals(existing);
  const add = datedIntervals(incoming);
  if (have === null || add === null) {
    return incoming;
  }
  const merged = mergeIntervals([...have, ...add]);
  if (
    sourcePeriodYears(existing) !== null &&
    sourcePeriodYears(incoming) !== null
  ) {
    return coveragePeriod(merged);
  }
  // Each segment of an accepted period is itself a period with one interval.
  const written = [existing, incoming].flatMap((period) =>
    (Array.isArray(period) ? period : [period]).map((segment) => ({
      segment,
      days: datedIntervals(segment),
    })),
  );
  const segments = merged.map(
    (interval) =>
      written.find(
        ({ days }) => days !== null && sameIntervals(days, [interval]),
      )?.segment ?? coveragePeriod([interval]),
  ) as ProjectPeriodSegment[];
  return segments.length === 1 ? segments[0] : segments;
}

// ── Query-string builder ─────────────────────────────────────────────────────

/** Build a `?query` string from the resolution params, omitting undefined /
 * empty values and emitting a SINGLE value per param (NOT FastAPI deepObject —
 * the backend reads one `?period=`/`?variant=`/`?value_set_version=` each).
 * Returns the query WITHOUT a leading `?` (the empty string when no params), so
 * callers do `pathname + (q ? "?" + q : "")`. */
export function queryFromParams(params: ResolutionParams): string {
  const qs = new URLSearchParams();
  if (params.period) {
    qs.set("period", params.period);
  }
  if (params.variant) {
    qs.set("variant", params.variant);
  }
  if (params.value_set_version) {
    qs.set("value_set_version", params.value_set_version);
  }
  return qs.toString();
}

/** Merge a partial resolution change against the CURRENT params and produce the
 * next `?query` string (no leading `?`). The narrowing rule: `?variant` and
 * `?value_set_version` are MODIFIERS of a `?period` resolve (the server 422s them
 * without one), so clearing the period (`next.period` empty/null) DROPS them —
 * the result is the empty query (full history). A field left `undefined` in
 * `next` inherits from `current`; an explicit empty string clears that field.
 * Pure (no runes / no `window`) so it's unit-tested directly. */
export function nextResolutionQuery(
  current: ResolutionParams,
  next: {
    period?: string | null;
    variant?: string | null;
    value_set_version?: string | null;
  },
): string {
  const period =
    next.period === undefined ? current.period : (next.period ?? undefined);
  if (!period) {
    return "";
  }
  const variant =
    next.variant === undefined ? current.variant : (next.variant ?? undefined);
  const value_set_version =
    next.value_set_version === undefined
      ? current.value_set_version
      : (next.value_set_version ?? undefined);
  return queryFromParams({ period, variant, value_set_version });
}

// ── Year-window slider (#615 availability-aware local period) ────────────────
// The default subject-page period control is a year-grain dual-thumb slider over
// the project WINDOW + the subject's data COVERAGE (#611 → Period model). It is
// year-granular by design (mirrors the header's `ProjectStudyWindow`); the rich
// sub-annual grammar (term/quarter/month/day, segment lists, `_default`, text)
// stays behind the picker's "more" expander. These pure helpers do the
// year-int ↔ wire shaping so the slider can be a presentation-only component
// (props in / `onchange` out, unit-testable) and the picker stays the wire seam.

/** The `?period` WIRE for a year-grain window: a bare year when `from === to`
 * (`2018`), else the inclusive `from..to` range (`2018..2020`) — exactly the
 * forms the existing range path already round-trips. */
export function yearWindowToWire(window: ProjectStudyWindow): string {
  return window.from === window.to
    ? String(window.from)
    : `${window.from}..${window.to}`;
}

/** Seed a year-grain `{from, to}` from a wire `?period`, or null when the value
 * isn't a pure YEAR token / uniform year range (a sub-annual token, `_default`,
 * a segment list, or junk — those belong to the "more" expander, never silently
 * snapped onto the year slider). Endpoints must be bare grammar years. */
export function yearWindowFromWire(
  wire: string | null | undefined,
): ProjectStudyWindow | null {
  const windows = yearSegmentsFromWire(wire);
  return windows?.length === 1 ? windows[0] : null;
}

/** Every segment of a wire `?period` as a year window, in STORED order — the
 * Y-101 list editor's one-row-per-segment seed, generalising `yearWindowFromWire`
 * to the #307 comma list (a single segment is the one-row case). `null` when the
 * wire is blank or ANY segment is not a bare year / ordered year range — a token
 * segment (`HT2018`) makes the WHOLE period unrepresentable by year rows. Each
 * segment is judged on its own (reg-core's year spans of that segment), so the
 * rows keep the order and grouping they were written in. */
export function yearSegmentsFromWire(
  wire: string | null | undefined,
): ProjectStudyWindow[] | null {
  const period = sourcePeriodFromWire(wire ?? null);
  const windows: ProjectStudyWindow[] = [];
  for (const segment of Array.isArray(period) ? period : [period]) {
    const years = sourcePeriodYears(segment);
    if (years?.length !== 1) {
      return null;
    }
    windows.push({ from: years[0][0], to: years[0][1] });
  }
  return windows;
}

/** Whether a wire `?period` is a pure year window the year slider can hold (a
 * bare year or a uniform-year range) — the slider's representability gate. A
 * sub-annual token / `_default` / segment list / junk remains valid URL state but
 * is displayed as read-only active text instead of being authored by the slider. */
export function yearWindowRepresentable(
  wire: string | null | undefined,
): boolean {
  return yearWindowFromWire(wire) !== null;
}

/** Clamp a year window to `[min, max]` (and keep `from <= to`) — the slider's
 * bounds guard for a seed that falls outside the rendered track (an older
 * `?period` predating the current bounds). */
export function clampYearWindow(
  window: ProjectStudyWindow,
  min: number,
  max: number,
): ProjectStudyWindow {
  const from = Math.min(Math.max(window.from, min), max);
  const to = Math.min(Math.max(window.to, min), max);
  return { from: Math.min(from, to), to: Math.max(from, to) };
}

/** Clamp only a year-grain `?period` wire to `[min, max]`. Non-year-grain
 * values stay verbatim because the year slider renders them read-only instead of
 * silently rewriting richer period grammar. */
export function clampYearPeriodWire(
  wire: string | null | undefined,
  min: number,
  max: number,
): string | null {
  const value = wire ?? null;
  const window = yearWindowFromWire(value);
  return window === null
    ? value
    : yearWindowToWire(clampYearWindow(window, min, max));
}

/** The subject's data-availability span with INDEPENDENTLY-bounded sides (#615):
 * a `null` side is UNBOUNDED (the start/end is unknown — the `0001`/`9999`
 * sentinels of `coverageFromStates`). Distinct from `ProjectStudyWindow` (a hard int
 * pair — the wire shape for the window/selection), since a coverage span can be
 * open on one side while finite on the other (`0001..2008` → `{from:null,
 * to:2008}`); a gap fires only against a FINITE side. */
export interface Coverage {
  from: number | null;
  to: number | null;
}

/** The COVERAGE BAND of an availability track: the inclusive year span the
 * subject actually delivers, resolved against the track edges (an open START
 * runs to `min`) and the catalog vintage (an open END stops at `vintageYear` —
 * the catalog only knows delivery up to its own vintage, #631; falls back to
 * `max` for callers that don't cap). `null` when there is no coverage, or when
 * the resolved band INVERTS (`from > to` — e.g. a register first delivered 2025
 * on a 2024-vintage catalog): an inverted band is no band at all, so the track
 * draws nothing and clamps nothing rather than emitting a negative-width cell.
 *
 * This span is the SELECTABLE year range of the period control: the slider
 * passes it to DualThumbTrack's `selectableMin/Max` (#671 hard clamp) and the
 * picker validates its exact year entry against it, so a typed year and a
 * dragged thumb reach exactly the same years. */
export function coverageBandEdges(
  coverage: Coverage | null,
  min: number,
  max: number,
  vintageYear?: number,
): ProjectStudyWindow | null {
  if (coverage === null) {
    return null;
  }
  const from = coverage.from ?? min;
  const to = coverage.to ?? vintageYear ?? max;
  return from > to ? null : { from, to };
}

/** The seed window for the availability slider when there is no explicit
 * `?period` to honour (#671): the part of the data COVERAGE the project window
 * actually frames, so a variable's true coverage shows UP FRONT rather than the
 * full 1960–vintage span reading as available. Pure (no runes) — unit-tested in
 * `period.test.ts`.
 *
 * The coverage side is `coverageBandEdges` above, resolved against the supplied
 * fallbacks — which the caller passes as the EFFECTIVE coverage edges (open
 * coverage end → the vintage; open coverage start → the slider floor), so an
 * open-sided coverage still yields a finite seed. Then:
 *   - WITH a `window`: the intersection `[max(covStart, window.from),
 *     min(covEnd, window.to)]` — the window narrowed to where data exists. When
 *     the window lies WHOLLY outside coverage the intersection inverts; we clamp
 *     it to the nearest coverage edge (an empty single-year seed there, never an
 *     inverted span) so the thumbs still land on a covered year, and the
 *     window-vs-data deviation hint speaks for the mismatch.
 *   - WITHOUT a window (`null`): the effective coverage span itself.
 *   - NO coverage (`null`): a SET window still seeds at the window (a stateless
 *     variable with a project window honours it, never widening to full history —
 *     that would regress the seed and Apply to `fallbackMin..vintage`); only
 *     no-window + no-coverage falls to the full `[fallbackMin, fallbackMax]`
 *     bounds (nothing to narrow to).
 *   - INVERTED effective coverage (e.g. open-ended coverage `{from:2025,
 *     to:null}` on a 2024-vintage catalog → 2025 > 2024): `coverageBandEdges`
 *     nulls it (Fix D — no band, no clamp), so the seed is the window (else the
 *     full bounds) rather than a manufactured span over no-data years. Reading
 *     the same function is what keeps the seed and the slider agreed on "no
 *     selectable no-data span". */
export function intersectCoverageWindow(
  coverage: Coverage | null,
  window: ProjectStudyWindow | null,
  fallbackMin: number,
  fallbackMax: number,
): ProjectStudyWindow {
  // No coverage — or an INVERTED one, which is no band at all — seeds from the
  // window, else the full bounds: never a manufactured span over no-data years.
  const band = coverageBandEdges(coverage, fallbackMin, fallbackMax);
  if (band === null) {
    return window ?? { from: fallbackMin, to: fallbackMax };
  }
  if (window === null) {
    return band;
  }
  const from = Math.max(band.from, window.from);
  const to = Math.min(band.to, window.to);
  // A window wholly outside coverage inverts (from > to): snap to the nearest
  // coverage edge so the seed is a covered year, never an inverted/empty span.
  if (from > to) {
    const edge = window.to < band.from ? band.from : band.to;
    return { from: edge, to: edge };
  }
  return { from, to };
}

/** The not-delivered gaps of a `selection` window against the subject's
 * `coverage` — the sub-spans inside the selection but OUTSIDE coverage (#615
 * availability deviation). Returns the leading gap (selection starts before a
 * FINITE coverage start) and/or trailing gap (selection ends after a FINITE
 * coverage end), each a year-int `{from, to}`; an empty array when coverage
 * fully covers the selection, the relevant side is unbounded (null = no gap
 * there), or there is no coverage to compare against. Inclusive bounds. */
export function notDeliveredGaps(
  selection: ProjectStudyWindow,
  coverage: Coverage | null,
): ProjectStudyWindow[] {
  if (coverage === null) {
    return [];
  }
  const gaps: ProjectStudyWindow[] = [];
  if (coverage.from !== null && selection.from < coverage.from) {
    gaps.push({
      from: selection.from,
      to: Math.min(selection.to, coverage.from - 1),
    });
  }
  if (coverage.to !== null && selection.to > coverage.to) {
    gaps.push({
      from: Math.max(selection.from, coverage.to + 1),
      to: selection.to,
    });
  }
  return gaps.filter((g) => g.from <= g.to);
}

/** Whether two year windows describe the SAME span (the user-deviation test:
 * `?period` ≠ the project window). Null-safe — two nulls are equal. */
export function sameYearWindow(
  a: ProjectStudyWindow | null,
  b: ProjectStudyWindow | null,
): boolean {
  if (a === null || b === null) {
    return a === b;
  }
  return a.from === b.from && a.to === b.to;
}

/** A year window as a reader sees it: a bare year when it is one year wide, else
 * the en-dash range (`2015–2020`). */
export function yearWindowLabel(window: ProjectStudyWindow): string {
  return window.from === window.to
    ? String(window.from)
    : `${window.from}–${window.to}`;
}

/** A `Source.period` as a reader sees it: each segment of the wire with its range
 * separator as an en dash, the segments comma-separated (`2005–2008, 2012–2015`).
 * Null when the period has no wire (unset / malformed). Presentation only: the
 * wire is reg-core's. */
export function periodLabel(period: unknown): string | null {
  const wire = sourcePeriodToWire(period);
  return wire === null
    ? null
    : wire
        .split(",")
        .map((segment) => segment.replace("..", "–"))
        .join(", ");
}

/** How a source period sits against the project's common study window:
 *   - `same`     — it covers exactly the window's years, one unbroken span;
 *   - `differs`  — it overlaps the window but covers more, less, or a holed part
 *                  of it (a token period that overlaps always differs: its grain is
 *                  not the window's);
 *   - `disjoint` — not one day of it falls inside the window.
 * Null when there is nothing to compare: no window, or a period with no dated
 * bounds (unset, malformed, or the year-independent `_default`, which no window
 * applies to, or one reg-core refuses). Disjointness is judged on the days
 * (reg-core's intervals and overlap), so a sub-annual period is placed at its real
 * grain; sameness on merged year spans, so `2010..2014,2015..2020` is the same as
 * `2010..2020`. */
export type WindowRelation = "same" | "differs" | "disjoint";

export function periodWindowRelation(
  period: unknown,
  window: ProjectStudyWindow | null,
): WindowRelation | null {
  if (period == null || window === null) {
    return null;
  }
  const days = datedIntervals(period);
  // The window is a `{from, to}` of years: a period reg-core places as well.
  const windowDays = datedIntervals({ from: window.from, to: window.to });
  if (days === null || windowDays === null) {
    return null;
  }
  if (overlapIntervals(days, windowDays).length === 0) {
    return "disjoint";
  }
  const years = sourcePeriodYears(period);
  return years?.length === 1 &&
    sameYearWindow({ from: years[0][0], to: years[0][1] }, window)
    ? "same"
    : "differs";
}

// ── Exact-year entry (Y-16 PeriodPicker + Y-81 SourceEditor, hoisted Y-100) ──
// Both the catalog's period card and the /project cart's per-source period
// editor carry two typed year fields and resolve them the same way; this is
// the "is this a usable year pair" rule they both call, once.

/** Any four-digit run → its int, else null. WIDER than `wireYear` (19xx/20xx)
 * so a caller with its own selectable band (`YearEntryOptions.selectableYears`)
 * can tell "not a year" from "not a year we hold" — an out-of-band four-digit
 * year (`2100`) is out of RANGE, not badly typed. */
function fourDigitYear(raw: string): number | null {
  const trimmed = raw.trim();
  return /^\d{4}$/.test(trimmed) ? Number.parseInt(trimmed, 10) : null;
}

/** The clause a plain grammar-year check refuses by, when the caller supplies
 * no band-specific wording (`YearEntryOptions.yearRule`'s default) — the wire
 * grammar's own century range. */
const DEFAULT_YEAR_RULE = "four-digit year, 1900 to 2099.";

export interface YearEntryOptions {
  /** The selectable year band an entry must ALSO fall within — the picker's
   * slider-clamped years (coverage / project window / steward bounds). Omitted
   * for a plain wire-grammar check: `SourceEditor` writes the period itself, so
   * the wire's own 19xx/20xx rule (`wireYear`) IS the only band it has. */
  selectableYears?: ProjectStudyWindow;
  /** The clause naming the year rule in the "must be a ___" refusal, e.g.
   * `"four-digit year, like 2015."` (the picker, off the band's first year).
   * Defaults to `DEFAULT_YEAR_RULE` — the wording a plain grammar-year check
   * (no `selectableYears`) refuses by. */
  yearRule?: string;
}

/**
 * The exact-year entry both `PeriodPicker` and `SourceEditor` resolve their two
 * typed year fields against: the year window the pair names, or why it names
 * none — with the field(s) that refusal is about, so the caller's hairline
 * marks the year at fault rather than both. `from`/`to` are the raw field text,
 * or `null` while the fields still mirror the seeded value (nothing typed
 * yet) — the caller's own "has this been touched" signal, folded in here
 * rather than gated a second time at each call site.
 *
 * Without `selectableYears`, the strict wire grammar (`wireYear`) is both
 * the "is this a year" test and the only band. With it, a WIDER "is this even
 * a year" test (`fourDigitYear`) runs first, so a four-digit year outside the
 * band reports as out of range rather than badly typed, then a second check
 * against the band itself. Pure — unit-tested in `period.test.ts`.
 */
export function resolveYearEntry(
  from: string | null,
  to: string | null,
  opts: YearEntryOptions = {},
):
  | { years: ProjectStudyWindow }
  | { problem: string; at: { from: boolean; to: boolean } }
  | null {
  if (from === null || to === null) {
    return null;
  }
  const { selectableYears, yearRule = DEFAULT_YEAR_RULE } = opts;
  const parseYear = selectableYears ? fourDigitYear : wireYear;
  const fromYear = parseYear(from);
  const toYear = parseYear(to);
  if (fromYear === null || toYear === null) {
    const at = { from: fromYear === null, to: toYear === null };
    if (at.from && at.to) {
      return { problem: `From and To must each be a ${yearRule}`, at };
    }
    return { problem: `${at.from ? "From" : "To"} must be a ${yearRule}`, at };
  }
  if (selectableYears) {
    const inBand = (raw: string, year: number) =>
      wireYear(raw) !== null &&
      year >= selectableYears.from &&
      year <= selectableYears.to;
    const at = { from: !inBand(from, fromYear), to: !inBand(to, toYear) };
    if (at.from || at.to) {
      const band = `${selectableYears.from}–${selectableYears.to}`;
      if (at.from && at.to) {
        return {
          problem: `${fromYear} and ${toYear} are outside ${band} — pick years in that range.`,
          at,
        };
      }
      return {
        problem: `${at.from ? fromYear : toYear} is outside ${band} — pick a year in that range.`,
        at,
      };
    }
  }
  if (fromYear > toYear) {
    // The pair, not either year on its own.
    return {
      problem: `From ${fromYear} is after To ${toYear} — enter From at or before To.`,
      at: { from: true, to: true },
    };
  }
  return { years: { from: fromYear, to: toYear } };
}
