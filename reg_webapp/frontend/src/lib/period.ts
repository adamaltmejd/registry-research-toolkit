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

import type { Period } from "./project_data";

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
const DEFAULT_SENTINEL = "_default";

/** ADVISORY: does `raw` look like a period (a single token, a
 * `<token>..<token>` range, or the `_default` snapshot sentinel)? Leading/
 * trailing whitespace is tolerated (the picker trims before sending). Returns
 * `false` for junk so the picker can show an inline "doesn't look like a period"
 * hint — but the caller MUST still allow submit (the server is canonical). */
export function looksLikePeriod(raw: string): boolean {
  const value = raw.trim();
  if (value === "") {
    return false;
  }
  if (value === DEFAULT_SENTINEL) {
    return true;
  }
  if (value.includes(RANGE_SEP)) {
    const parts = value.split(RANGE_SEP);
    // Exactly one separator → two endpoints; each must be a single token
    // (a range of `_default` / nested ranges is not grammar).
    return parts.length === 2 && parts.every((p) => isPeriodToken(p));
  }
  return isPeriodToken(value);
}

/** Convert a structured `Source.period` (int | token-string | {from,to} |
 * "_default") into the wire `?period` string the catalog resolve takes (a bare
 * year, a `from..to` range, a token, or `_default`). Returns `null` when the
 * period can't form a resolvable query (blank / malformed) — the picker then
 * can't derive-on-pick and shows its "set the period" hint. ADVISORY shaping
 * only; the backend is the canonical period validator. */
export function periodToWire(period: Period): string | null {
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
