/**
 * Pure period/query helpers for the binding-leaf resolution state (no runes —
 * unit-testable in isolation; `period.test.ts`). The resolution state lives in
 * the URL query (`?period`/`?variant`/`?value_set_version`, §9.5); these
 * (de)serialize the period between the query and the picker field and build the
 * query string the router navigates to.
 *
 * §9.6: the SPA only mirrors the wire grammar — the server (`reg_meta.fqid`) is
 * the CANONICAL validator. `looksLikePeriod` is a LIGHT, ADVISORY client hint
 * only (it must never block submit; a "looks wrong" value is still sent so the
 * server's 422 detail is the authority).
 */

/** The §9.5 narrowing modifiers carried in the URL query alongside `?period`. */
export interface ResolutionParams {
  period?: string;
  variant?: string;
  value_set_version?: string;
}

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

// ── Advisory grammar hint (§9.5 tokens) ──────────────────────────────────────
// Mirrors `reg_meta.fqid._PERIOD_PATTERNS` (anchored, `\Z`-equivalent — JS `$`
// already does NOT match before a trailing `\n` the way Python's does, so the
// trailing-newline footgun the backend guards doesn't exist here; still, the
// server is the canonical gate). A range is `<endpoint>..<endpoint>`; each
// endpoint is a single token. ADVISORY ONLY — never gates submit.

const YEAR = "(?:19|20)\\d{2}";
const MONTH = "(?:0[1-9]|1[0-2])";
const DAY = "(?:0[1-9]|[12]\\d|3[01])";

/** One period TOKEN (no range, no `_default`): the §9.5 single-token forms —
 * `YYYY`, `YYYY-MM`, `YYYY-MM-DD`, `HTYYYY`/`VTYYYY`, `YYYY-Q[1-4]`,
 * `YYYY-H[12]`. */
const TOKEN_RE = new RegExp(
  `^(?:${YEAR}|${YEAR}-${MONTH}|${YEAR}-${MONTH}-${DAY}|[HV]T${YEAR}|${YEAR}-Q[1-4]|${YEAR}-H[12])$`,
);

const RANGE_SEP = "..";
const DEFAULT_SENTINEL = "_default";

/** ADVISORY: does `raw` look like a §9.5 period (a single token, a
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
    return parts.length === 2 && parts.every((p) => TOKEN_RE.test(p));
  }
  return TOKEN_RE.test(value);
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
 * next `?query` string (no leading `?`). The §9.5 narrowing rule: `?variant` and
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
