/**
 * Pure period/query helpers for the binding-leaf resolution state (no runes —
 * unit-testable in isolation; `period.test.ts`). The resolution state lives in
 * the URL query (`?period`/`?variant`/`?value_set_version`; see
 * reg_webapp/DESIGN.md → Catalog router structure); these
 * (de)serialize the period between the query and the picker field and build the
 * query string the router navigates to.
 *
 * The SPA only mirrors the wire grammar (see reg_webapp/DESIGN.md → Pydantic
 * boundary) — the server (`reg_meta.fqid`) is the CANONICAL validator. `looksLikePeriod` is a LIGHT, ADVISORY client hint
 * only (it must never block submit; a "looks wrong" value is still sent so the
 * server's 422 detail is the authority).
 */

import type { Period, PeriodSegment, StudyWindow } from "./project_data";

/** The narrowing modifiers carried in the URL query alongside `?period`. */
export interface ResolutionParams {
  period?: string;
  variant?: string;
  value_set_version?: string;
}

/** Sentinel `?value_set_version` selecting the empty/default label (a state with
 * `value_set_version_label === ""`). The empty string can't ride in the query
 * (≡ absent), so the picker sends this for the "(no version)" option; the backend
 * maps it back to `""` before `resolve_at`. MUST match `period_param.py`'s
 * `VALUE_SET_VERSION_NONE`. */
export const VALUE_SET_VERSION_NONE = "_none";

// ── Period field ↔ query (de)serialize ──────────────────────────────────────
// The picker is a single free-text field; the URL query carries the same wire
// string. Round-tripping is the identity on a trimmed string — the field's raw
// text IS the wire `?period` value (year / token / range / `_default`). These
// thin wrappers name the boundary (and normalize whitespace) so the call sites
// read intentionally and a future format tweak has one home.

/** The picker field text for a `?period` query value (or `null`/absent → ""). */
export function periodFieldFromQuery(
  period: string | null | undefined,
): string {
  return period ?? "";
}

/** The `?period` wire value for the picker field text — trimmed; an empty/blank
 * field yields `null` (no `?period`, i.e. full history). */
export function periodQueryFromField(raw: string): string | null {
  const trimmed = raw.trim();
  return trimmed === "" ? null : trimmed;
}

// ── Advisory grammar hint (wire tokens) ──────────────────────────────────────
// Mirrors `reg_meta.fqid._PERIOD_PATTERNS` (anchored, `\Z`-equivalent — JS `$`
// already does NOT match before a trailing `\n` the way Python's does, so the
// trailing-newline footgun the backend guards doesn't exist here; still, the
// server is the canonical gate). A range is `<endpoint>..<endpoint>`; each
// endpoint is a single token. The author-supplied day of a `YYYY-MM-DD` token is
// ALSO calendar-checked (regex can't do leap years, so `2019-02-29` matches the
// pattern but is rejected by `isRealCalendarDay` — mirrors the reg_meta/reg_schema
// side). ADVISORY ONLY — never gates submit.

const YEAR = "(?:19|20)\\d{2}";
const MONTH = "(?:0[1-9]|1[0-2])";
const DAY = "(?:0[1-9]|[12]\\d|3[01])";

const TOKEN_RE = new RegExp(
  `^(?:${YEAR}|${YEAR}-${MONTH}|${YEAR}-${MONTH}-${DAY}|[HV]T${YEAR}|${YEAR}-Q[1-4]|${YEAR}-H[12])$`,
);

const FULL_DATE_RE = new RegExp(`^${YEAR}-${MONTH}-${DAY}$`);

/** Is a `YYYY-MM-DD` string a REAL calendar date? The grammar regex only bounds
 * the day 01-31; this rejects calendar-impossible days (`2019-02-29` in a
 * non-leap year, `2018-02-30`). Builds the date in UTC (avoids TZ drift) and
 * checks each component round-trips — `Date` silently rolls a bad day over
 * (Feb 30 → Mar 2), so a mismatch means the day was impossible. */
function isRealCalendarDay(value: string): boolean {
  const [y, m, d] = value.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return (
    dt.getUTCFullYear() === y &&
    dt.getUTCMonth() === m - 1 &&
    dt.getUTCDate() === d
  );
}

/** One period TOKEN (no range, no `_default`): the single-token forms —
 * `YYYY`, `YYYY-MM`, `YYYY-MM-DD`, `HTYYYY`/`VTYYYY`, `YYYY-Q[1-4]`,
 * `YYYY-H[12]`. A `YYYY-MM-DD` is additionally calendar-validated. */
function isPeriodToken(value: string): boolean {
  if (!TOKEN_RE.test(value)) {
    return false;
  }
  return FULL_DATE_RE.test(value) ? isRealCalendarDay(value) : true;
}

const RANGE_SEP = "..";
const LIST_SEP = ",";
const DEFAULT_SENTINEL = "_default";

/** One period SEGMENT looks valid: a single token or a `<token>..<token>`
 * range. `_default` is NOT a segment (it is the whole-history sentinel, only
 * legal at the top level — mirrors the backend's list-member rule). */
function looksLikeSegment(value: string): boolean {
  if (value.includes(RANGE_SEP)) {
    const parts = value.split(RANGE_SEP);
    // Exactly one separator → two endpoints; each must be a single token
    // (a range of `_default` / nested ranges is not grammar).
    return parts.length === 2 && parts.every((p) => isPeriodToken(p));
  }
  return isPeriodToken(value);
}

/** ADVISORY: does `raw` look like a period (a single token, a
 * `<token>..<token>` range, the `_default` snapshot sentinel, or a
 * comma-joined LIST of segments — the #307 interrupted-series wire form,
 * e.g. `2005..2010,2015..2020`)? Leading/trailing whitespace is tolerated
 * (the picker trims before sending). Returns `false` for junk so the picker
 * can show an inline "doesn't look like a period" hint — but the caller MUST
 * still allow submit (the server is canonical; it also owns the
 * sorted/non-overlap list rules this hint doesn't check). */
export function looksLikePeriod(raw: string): boolean {
  const value = raw.trim();
  if (value === "") {
    return false;
  }
  if (value === DEFAULT_SENTINEL) {
    return true;
  }
  if (value.includes(LIST_SEP)) {
    return value
      .split(LIST_SEP)
      .every((member) => looksLikeSegment(member.trim()));
  }
  return looksLikeSegment(value);
}

// ── Token bounds (advisory mirror of `reg_meta.fqid` interval semantics) ────
// The #306 one-click add needs CLIENT-side window math (clip register-variant
// validity windows to the user's range to tell succession from co-existence).
// ADVISORY like `looksLikePeriod`: an unparseable token simply degrades to the
// explicit variant prompt — the server stays the canonical period authority.

/** Inclusive ISO date bounds of one period TOKEN. */
export interface PeriodBounds {
  from: string;
  to: string;
}

const TERM_BOUNDS: Record<string, [string, string]> = {
  VT: ["01-01", "06-30"], // spring term
  HT: ["07-01", "12-31"], // autumn term
  H1: ["01-01", "06-30"],
  H2: ["07-01", "12-31"],
  Q1: ["01-01", "03-31"],
  Q2: ["04-01", "06-30"],
  Q3: ["07-01", "09-30"],
  Q4: ["10-01", "12-31"],
};

/** Last day of a month, as the 2-digit day string (UTC; day 0 of month+1). */
function lastDayOfMonth(year: number, month: number): string {
  return String(new Date(Date.UTC(year, month, 0)).getUTCDate()).padStart(
    2,
    "0",
  );
}

/** The inclusive ISO date window a single period token denotes (`2020` →
 * 2020-01-01..2020-12-31, `VT2009` → 2009-01-01..2009-06-30, `2020-03` →
 * 2020-03-01..2020-03-31, a day → itself). `null` for anything that isn't a
 * single grammar token (`_default`, ranges, junk). */
export function periodTokenBounds(token: string): PeriodBounds | null {
  const value = token.trim();
  if (!isPeriodToken(value)) {
    return null;
  }
  const term = /^([HV]T)((?:19|20)\d{2})$/.exec(value);
  if (term) {
    const [mmddFrom, mmddTo] = TERM_BOUNDS[term[1]];
    return { from: `${term[2]}-${mmddFrom}`, to: `${term[2]}-${mmddTo}` };
  }
  const quarterHalf = /^((?:19|20)\d{2})-([QH][1-4])$/.exec(value);
  if (quarterHalf) {
    const [mmddFrom, mmddTo] = TERM_BOUNDS[quarterHalf[2]];
    return {
      from: `${quarterHalf[1]}-${mmddFrom}`,
      to: `${quarterHalf[1]}-${mmddTo}`,
    };
  }
  const month = /^((?:19|20)\d{2})-(\d{2})$/.exec(value);
  if (month) {
    const y = Number(month[1]);
    const m = Number(month[2]);
    return {
      from: `${month[1]}-${month[2]}-01`,
      to: `${month[1]}-${month[2]}-${lastDayOfMonth(y, m)}`,
    };
  }
  if (FULL_DATE_RE.test(value)) {
    return { from: value, to: value };
  }
  // A bare year (the only remaining single-token form).
  return { from: `${value}-01-01`, to: `${value}-12-31` };
}

/** Split a wire period into its two RANGE endpoints (`"2010..2020"` →
 * `["2010", "2020"]`), or `null` when it isn't a 2-endpoint range. */
export function periodRangeEndpoints(wire: string): [string, string] | null {
  if (!wire.includes(RANGE_SEP)) {
    return null;
  }
  const parts = wire.split(RANGE_SEP);
  return parts.length === 2 ? [parts[0].trim(), parts[1].trim()] : null;
}

// ── Grain model (#308 range-first picker) ────────────────────────────────────
// The picker UI works in GRAINS (year/term/quarter/month/day) with from/to
// controls per grain; the wire grammar stays the serialization. The `-H1`/`-H2`
// half forms map onto the term grain (same bounds; reg_meta accepts them on
// input but never emits them — the term spelling is canonical).

export type PeriodGrain = "year" | "term" | "quarter" | "month" | "day";

/** Coarse → fine — the grain `<select>` order. */
export const PERIOD_GRAINS: PeriodGrain[] = [
  "year",
  "term",
  "quarter",
  "month",
  "day",
];

/** Whether a wire period is representable by the RANGE UI (#308): a single
 * grammar token, or a range whose endpoints share ONE grain. `_default`,
 * mixed-grain ranges (the #306 succession clips), #307 segment LISTS, and junk
 * need the text / token escape hatch — they must stay visible and editable,
 * never silently blanked into empty range controls. */
export function rangeRepresentable(
  wire: string,
  grains: PeriodGrain[] = PERIOD_GRAINS,
): boolean {
  const endpoints = periodRangeEndpoints(wire) ?? [wire, wire];
  const gFrom = grainOfToken(endpoints[0]);
  return (
    gFrom !== null &&
    gFrom === grainOfToken(endpoints[1]) &&
    // A surface offering a NARROWED grain set (the catalog picker) can't
    // render a value at an excluded grain either — it must fall back to the
    // text mode, not show blank controls over a hidden value (Codex P2).
    grains.includes(gFrom)
  );
}

/** The grain of one single wire token, or null for a non-token (`_default`,
 * ranges, lists, junk). `YYYY-H1`/`-H2` report `term` (their VT/HT bounds
 * twins). */
export function grainOfToken(token: string): PeriodGrain | null {
  const value = token.trim();
  if (!isPeriodToken(value)) {
    return null;
  }
  if (/^[HV]T/.test(value)) {
    return "term";
  }
  if (/-Q[1-4]$/.test(value)) {
    return "quarter";
  }
  if (/-H[12]$/.test(value)) {
    return "term";
  }
  if (/^\d{4}$/.test(value)) {
    return "year";
  }
  return /^\d{4}-\d{2}$/.test(value) ? "month" : "day";
}

/** Convert a structured `Source.period` (int | token-string | {from,to} |
 * "_default" | segment list) into the wire `?period` string (a bare year, a
 * `from..to` range, a token, `_default`, or — for the #307 list form — the
 * comma-joined member wires, `2005..2010,2015..2020`). Returns `null` when the
 * period can't form a resolvable query (blank / malformed / a list with a
 * malformed member) — the picker then can't derive-on-pick and shows its "set
 * the period" hint. The ONE wire for display, round-trip, AND resolve: the
 * catalog `?period=` accepts the comma form since #340 (per-segment resolve,
 * state_id-deduped union). ADVISORY shaping only; the backend is the
 * canonical period validator. */
export function periodToWire(period: Period): string | null {
  if (Array.isArray(period)) {
    if (period.length === 0) {
      return null;
    }
    const members = period.map((segment) => periodToWire(segment));
    return members.some((m) => m === null || m.includes(LIST_SEP))
      ? null
      : members.join(LIST_SEP);
  }
  if (typeof period === "number") {
    return String(period);
  }
  if (typeof period === "string") {
    const trimmed = period.trim();
    return trimmed === "" ? null : trimmed;
  }
  if (
    period != null &&
    typeof period === "object" &&
    "from" in period &&
    "to" in period
  ) {
    const from = String(period.from).trim();
    const to = String(period.to).trim();
    return from === "" || to === "" ? null : `${from}${RANGE_SEP}${to}`;
  }
  return null;
}

/** The INVERSE of `periodToWire`: shape a wire `?period` string (as the catalog
 * page's PeriodPicker holds it) into a structured `Source.period` the editor's
 * PeriodEditor models (C1 — catalog→project handoff period prefill). The mapping
 * mirrors PeriodEditor's mode inference so a prefilled source opens in the right
 * editor mode:
 *   - a bare integer year (`"2018"`) → the `number` arm (years mode, from=to);
 *   - ANY 2-endpoint `from..to` range → the `{from, to}` object, each endpoint
 *     an integer year when it parses as one, else the token string verbatim
 *     (`"VT1992..2009"` → `{from: "VT1992", to: 2009}`). The OBJECT form is the
 *     only range shape `Source.period` accepts — reg_schema's string arm is
 *     single-token-only, so a raw `"a..b"` string period would fail
 *     `invalid_period` (bit the #306 succession auto-split, whose clipped
 *     segments routinely carry date/token endpoints);
 *   - a comma-joined LIST wire (`"2005..2010,2015..2020"`, the #307
 *     interrupted-series form) → an array of segments, each member shaped by
 *     the same scalar rules (a blank member rides through as the raw string);
 *   - anything else — a non-year token (`"HT2018"`, `"2019-03"`), a `_default`
 *     sentinel, or a malformed multi-`..` string — rides through as the raw
 *     string (token mode), which is exactly how PeriodEditor would model it.
 * A null/blank wire string yields `""` (the fresh-source unset period: PR B's
 * unresolved marker + amber hint then guide the user). ADVISORY shaping only — the
 * backend is the canonical period validator. */
export function periodFromWire(wire: string | null): Period {
  const value = (wire ?? "").trim();
  if (value === "") {
    return "";
  }
  if (value.includes(LIST_SEP)) {
    // #307 list wire (`2005..2010,2015..2020`) → a segment array, each member
    // shaped like a scalar wire. A blank member means malformed list text —
    // ride through as the raw string (the server's invalid_period is the
    // authority, exactly like any other junk token).
    const members = value.split(LIST_SEP).map((m) => m.trim());
    if (members.some((m) => m === "")) {
      return value;
    }
    return members.map((m) => segmentFromWire(m));
  }
  return segmentFromWire(value);
}

/** One scalar wire member → its structured segment shape (the pre-#307
 * `periodFromWire` body). */
function segmentFromWire(value: string): PeriodSegment {
  if (value.includes(RANGE_SEP)) {
    const parts = value.split(RANGE_SEP);
    if (parts.length === 2) {
      // ALWAYS the {from, to} object for a 2-endpoint range: int-year endpoints
      // where they parse, token strings otherwise. Never the raw "a..b" string —
      // reg_schema's string arm is single-token-only, so a raw range string
      // (scalar OR #307 list member) would fail `invalid_period`.
      return {
        from: yearInt(parts[0]) ?? parts[0].trim(),
        to: yearInt(parts[1]) ?? parts[1].trim(),
      };
    }
    return value;
  }
  const year = yearInt(value);
  // A bare integer year → the single-year `number` arm (from=to in the editor).
  return year !== null ? year : value;
}

/** Parse a string as a bare GRAMMAR year (19xx/20xx), else null. Stricter than
 * "any integer" on purpose: an int Source.period passes reg_schema's int-literal
 * arm unchecked, so coercing a typo like "202" to int 202 would slip a nonsense
 * year past the structural gate — left as a string, the grammar check flags it
 * (review on #308). */
function yearInt(raw: string): number | null {
  const trimmed = raw.trim();
  return /^(?:19|20)\d{2}$/.test(trimmed) ? Number.parseInt(trimmed, 10) : null;
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
// year-granular by design (mirrors the header's `StudyWindow`); the rich
// sub-annual grammar (term/quarter/month/day, segment lists, `_default`, text)
// stays behind the picker's "more" expander. These pure helpers do the
// year-int ↔ wire shaping so the slider can be a presentation-only component
// (props in / `onchange` out, unit-testable) and the picker stays the wire seam.

/** The `?period` WIRE for a year-grain window: a bare year when `from === to`
 * (`2018`), else the inclusive `from..to` range (`2018..2020`) — exactly the
 * forms the existing range path already round-trips. */
export function yearWindowToWire(window: StudyWindow): string {
  return window.from === window.to
    ? String(window.from)
    : `${window.from}..${window.to}`;
}

/** Seed a year-grain `{from, to}` from a wire `?period`, or null when the value
 * isn't a pure YEAR token / uniform year range (a sub-annual token, `_default`,
 * a segment list, or junk — those belong to the "more" expander, never silently
 * snapped onto the year slider). Endpoints must parse as bare grammar years. */
export function yearWindowFromWire(
  wire: string | null | undefined,
): StudyWindow | null {
  const value = (wire ?? "").trim();
  if (value === "") {
    return null;
  }
  const endpoints = periodRangeEndpoints(value) ?? [value, value];
  const from = grammarYear(endpoints[0]);
  const to = grammarYear(endpoints[1]);
  if (from === null || to === null || to < from) {
    return null;
  }
  return { from, to };
}

/** Parse a string as a bare grammar year (19xx/20xx) → its int, else null. */
function grammarYear(raw: string): number | null {
  const trimmed = raw.trim();
  return /^(?:19|20)\d{2}$/.test(trimmed) ? Number.parseInt(trimmed, 10) : null;
}

/** Whether a wire `?period` is a pure year window the year slider can hold (a
 * bare year or a uniform-year range) — the slider's representability gate
 * (analogous to `rangeRepresentable` for the fine-grain range UI). A sub-annual
 * token / `_default` / segment list / junk routes to the "more" expander. */
export function yearWindowRepresentable(
  wire: string | null | undefined,
): boolean {
  return yearWindowFromWire(wire) !== null;
}

/** Clamp a year window to `[min, max]` (and keep `from <= to`) — the slider's
 * bounds guard for a seed that falls outside the rendered track (an older
 * `?period` predating the current bounds). */
export function clampYearWindow(
  window: StudyWindow,
  min: number,
  max: number,
): StudyWindow {
  const from = Math.min(Math.max(window.from, min), max);
  const to = Math.min(Math.max(window.to, min), max);
  return { from: Math.min(from, to), to: Math.max(from, to) };
}

/** The subject's data-availability span with INDEPENDENTLY-bounded sides (#615):
 * a `null` side is UNBOUNDED (the start/end is unknown — the `0001`/`9999`
 * sentinels of `coverageFromStates`). Distinct from `StudyWindow` (a hard int
 * pair — the wire shape for the window/selection), since a coverage span can be
 * open on one side while finite on the other (`0001..2008` → `{from:null,
 * to:2008}`); a gap fires only against a FINITE side. */
export interface Coverage {
  from: number | null;
  to: number | null;
}

/** The not-delivered gaps of a `selection` window against the subject's
 * `coverage` — the sub-spans inside the selection but OUTSIDE coverage (#615
 * availability deviation). Returns the leading gap (selection starts before a
 * FINITE coverage start) and/or trailing gap (selection ends after a FINITE
 * coverage end), each a year-int `{from, to}`; an empty array when coverage
 * fully covers the selection, the relevant side is unbounded (null = no gap
 * there), or there is no coverage to compare against. Inclusive bounds. */
export function notDeliveredGaps(
  selection: StudyWindow,
  coverage: Coverage | null,
): StudyWindow[] {
  if (coverage === null) {
    return [];
  }
  const gaps: StudyWindow[] = [];
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
  a: StudyWindow | null,
  b: StudyWindow | null,
): boolean {
  if (a === null || b === null) {
    return a === b;
  }
  return a.from === b.from && a.to === b.to;
}
