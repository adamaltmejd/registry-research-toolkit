/**
 * Pure validation-presentation helpers (NO runes — unit-tested + corpus-pinned;
 * `validation.test.ts`). The backend is the CANONICAL validator (see
 * reg_webapp/DESIGN.md → Pydantic boundary): the SPA
 * never re-implements the server's validation rules — it only PARSES the issue list the server
 * returns (`ValidationResultModel`) and maps each `code` to a UI label/severity
 * hint for presentation.
 *
 * Two responsibilities:
 * 1. RFC-6901 JSON-pointer decoding (`parseJsonPointer`) so an issue `path`
 *    (`/sources/0/bindings/0/typ`) can be matched against a draft location — the
 *    A5.3c-ii inline field-highlighting seam (c-i only groups by level).
 * 2. The `KNOWN_CODES` registry: every stable validation code → a friendly label +
 *    the level it's typically raised at. Hand-maintained from the validator
 *    sources: structural (`reg_schema/structural.py`; see reg_schema/DESIGN.md →
 *    Structural rules and issue codes) and semantic (see reg_webapp/DESIGN.md →
 *    Semantic validation (semantic.py)). An UNKNOWN code
 *    degrades gracefully (the issue is still shown with its raw code + level).
 */

import type { components } from "./api-types";
import { catalogHref, registerPrefixOf } from "./catalog";
import { periodYearIntervals } from "./period";
import {
  type Period,
  type PeriodSegment,
  type SafeSource,
  type StudyWindow,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
} from "./project_data";

export type ValidationIssue = components["schemas"]["ValidationIssueModel"];
export type ValidationResult = components["schemas"]["ValidationResultModel"];

/**
 * Decode an RFC-6901 JSON pointer into its reference tokens. The empty string
 * `""` is the WHOLE document → `[]`. Each token unescapes `~1`→`/` THEN `~0`→`~`
 * (order matters: doing `~0` first would turn an encoded `~01` into `/` instead
 * of `~1`). A pointer must start with `/` (or be empty); a malformed pointer
 * (non-empty, no leading `/`) returns `null` so callers can fall back to a
 * whole-document treatment rather than mis-split it.
 */
export function parseJsonPointer(ptr: string): string[] | null {
  if (ptr === "") {
    return [];
  }
  if (!ptr.startsWith("/")) {
    return null;
  }
  return ptr
    .slice(1)
    .split("/")
    .map((token) => token.replace(/~1/g, "/").replace(/~0/g, "~"));
}

/**
 * The INVERSE of `parseJsonPointer`: encode reference tokens into an RFC-6901
 * pointer. Each token escapes `~`→`~0` THEN `/`→`~1` (the RFC-6901 escape order):
 * `~` MUST be escaped first, otherwise a literal `/`→`~1` would emit a `~` that the
 * later `~`→`~0` pass re-escapes into `~01`. This is the exact inverse of
 * `parseJsonPointer`'s `~1`→`/` then `~0`→`~` decode, so the two round-trip.
 * Numeric tokens (array indices) stringify. An EMPTY token array is the whole
 * document → `""`. Callers build a location's pointer with this and match it
 * against the server's issue list (e.g. `issuesUnderPointer`). */
export function jsonPointer(tokens: (string | number)[]): string {
  if (tokens.length === 0) {
    return "";
  }
  return `/${tokens
    .map((token) => String(token).replace(/~/g, "~0").replace(/\//g, "~1"))
    .join("/")}`;
}

/**
 * Every issue AT `prefix` OR BELOW it — the ROLL-UP lookup so a source/binding
 * header can badge all errors under its subtree (`/sources/{i}` rolls up
 * `/sources/{i}/bindings/0/type`, `/sources/{i}/name`, …). A descendant is matched
 * by `path === prefix` (exact) OR `path.startsWith(prefix + "/")` — the trailing
 * `/` guard is load-bearing: it stops `/sources/1` from false-matching
 * `/sources/10` (a bare `startsWith(prefix)` would). An empty `prefix` (whole
 * document) rolls up everything. */
export function issuesUnderPointer(
  issues: ValidationIssue[],
  prefix: string,
): ValidationIssue[] {
  if (prefix === "") {
    return issues;
  }
  const descendantPrefix = `${prefix}/`;
  return issues.filter(
    (issue) => issue.path === prefix || issue.path.startsWith(descendantPrefix),
  );
}

/** A hand-maintained registry entry for a stable validation code. `label` is the
 * human phrasing for the UI; `hint` is the level the rule is TYPICALLY raised at
 * (advisory — the actual issue's `level` is authoritative when rendering). */
export interface CodeInfo {
  label: string;
  hint: "error" | "warning" | "info";
}

/**
 * The stable validation-code registry. Hand-maintained from the validator sources —
 * new codes are ADDITIVE, so a code missing here is not a bug, it just
 * renders with its raw code (see `codeLabel`). Sourced from:
 * - structural (`reg_schema/structural.py`; see reg_schema/DESIGN.md → Structural rules and issue codes)
 * - semantic (`reg_webapp.semantic`; see reg_webapp/DESIGN.md → Semantic validation (semantic.py))
 */
export const KNOWN_CODES: Record<string, CodeInfo> = {
  // ── structural ────────────────────────────────────────────────────────────
  invalid_root: { label: "Root must be a JSON object", hint: "error" },
  missing_required_field: { label: "Missing required field", hint: "error" },
  invalid_field_type: { label: "Wrong field type", hint: "error" },
  invalid_enum_value: { label: "Value outside the allowed set", hint: "error" },
  unexpected_field: { label: "Unexpected field", hint: "error" },
  invalid_fqid: { label: "Malformed FQID", hint: "error" },
  fqid_register_variant_mismatch: {
    label: "Binding FQID prefix does not match the source's register_variant",
    hint: "error",
  },
  invalid_period: { label: "Invalid period", hint: "error" },
  subtype_on_wrong_type: {
    label: "Subtype/format field set on a binding type that doesn't own it",
    hint: "error",
  },
  empty_bindings: { label: "Source has no bindings", hint: "error" },
  duplicate_source_name: { label: "Duplicate source name", hint: "error" },
  display_name_collision: {
    label: "Two bindings on a source share a display_name",
    hint: "error",
  },
  duplicate_panel_id: { label: "Duplicate panel_id", hint: "error" },
  empty_members: { label: "Panel has no members", hint: "error" },
  literal_period_invalid: {
    label: "Malformed literal-period / range time_key",
    hint: "error",
  },
  composite_time_key_mixed_kinds: {
    label: "Composite time_key mixes column refs and literals",
    hint: "error",
  },
  composite_key_inconsistent: {
    label: "Composite keys across panel members are not identically ordered",
    hint: "error",
  },
  time_key_member_kind_mismatch: {
    label: "Member time_key kind differs from the panel-level composite",
    hint: "error",
  },
  literal_time_key_duplicate: {
    label: "Two panel members resolve to the same literal time_key",
    hint: "error",
  },
  entity_key_unknown_column: {
    label: "entity_key ref matches no display_name on the source",
    hint: "error",
  },
  time_key_unknown_column: {
    label: "time_key ref matches no display_name on the source",
    hint: "error",
  },
  source_referenced_by_multiple_panels: {
    label: "A source appears in more than one panel",
    hint: "error",
  },
  panel_member_unknown_source: {
    label: "Panel member references an unknown source",
    hint: "error",
  },
  // Thin defensive code: a residual ProjectData model-construction failure
  // structural didn't replicate (routes/project.py `_model_issue`).
  invalid_field: { label: "Invalid field", hint: "error" },

  // ── semantic (reg_meta-backed) ────────────────────────────────────────────
  fqid_unresolved: {
    label: "FQID does not resolve against this reg_meta build",
    hint: "error",
  },
  value_set_missing: {
    label: "Referenced value set is missing",
    hint: "error",
  },
  period_outside_state_validity: {
    label: "No variable state covers the binding's (variant, period)",
    hint: "error",
  },
  binding_value_set_version_ambiguous: {
    label:
      "The concept has several representations (delivery columns) at this period — pick one via representation",
    hint: "error",
  },
  binding_representation_unknown: {
    label: "The chosen representation is not a delivery column at this period",
    hint: "error",
  },
  fqid_outside_steward_catalog: {
    label:
      "This column is outside the steward's catalog (not supplied by this deployment)",
    hint: "warning",
  },
  representation_outside_steward_catalog: {
    label:
      "The steward holds this variable, but not the chosen representation (see the message for what it does supply)",
    hint: "warning",
  },
  binding_state_drifts_within_period: {
    label: "The period crosses a state transition (per-state subsets returned)",
    hint: "info",
  },
  range_period_partially_covered: {
    label: "The requested range has a sub-range no variable state covers",
    hint: "info",
  },
  deprecated_traversal: {
    label: "The binding resolves through deprecated catalog metadata",
    hint: "info",
  },
  variable_replaced: {
    label: "The binding has a replacement edge at/before this period",
    hint: "info",
  },
};

/** The friendly label for a code, or the raw code when it isn't registered (an
 * unknown/new code degrades gracefully — codes are additive). */
export function codeLabel(code: string): string {
  return KNOWN_CODES[code]?.label ?? code;
}

/** The DOM-anchor id of a source card. Mirrored by `SourceEditor.svelte`'s root
 * `id` so the findings panel's click-to-locate can `scrollIntoView`. */
export function sourceAnchorId(sourceIndex: number): string {
  return `loc-source-${sourceIndex}`;
}

/** The DOM-anchor id of a binding card. Mirrored by `BindingEditor.svelte`. */
export function bindingAnchorId(
  sourceIndex: number,
  bindingIndex: number,
): string {
  return `loc-source-${sourceIndex}-binding-${bindingIndex}`;
}

/** A user-facing LOCATION for a finding, derived from its RFC-6901 `path` plus the
 * draft's sources — the summary list speaks "Source 'lisa_main' → binding
 * scb/lisa/adeldag" instead of leaking the raw pointer, carries the DOM-anchor id so
 * a click scrolls to the relevant card, AND — since the cart is read-only and fixes
 * happen in the catalog browser (#991) — an outbound `catalogHref` to the subject
 * page where the offending binding/source can actually be re-picked.
 *
 * `anchorId` is the deepest card the path reaches (binding card if the path dives
 * into `/bindings/{j}`, else the source card). `label` names the source by its
 * authored `name` (falling back to the index when unnamed / out of range) and, for
 * a binding path, appends the binding's `variable` FQID. `catalogHref`/`catalogLabel`
 * point at the catalog subject page: the binding's `variable` FQID for a binding
 * path, the source's `register_variant` for a source path — both omitted (undefined)
 * when the coordinate is missing/blank (a fresh unpicked row) so the panel just
 * skips the link. A whole-document path (`""`) or a non-`/sources/...` path (e.g. a
 * top-level `/name`) has no card to locate → `null`, and the panel falls back to
 * showing the raw pointer. */
export interface FindingLocation {
  label: string;
  anchorId: string;
  /** The catalog subject page for the located binding/source, when its
   * variable/register_variant coordinate is resolvable — the read-only cart's
   * "fix it in the browser" outbound link (#991). Omitted otherwise. */
  catalogHref?: string;
  catalogLabel?: string;
}

export interface WindowCoverageHint {
  label: string;
  message: string;
  catalogHref?: string;
  catalogLabel?: string;
}

function comparablePeriodSegment(segment: unknown): PeriodSegment | null {
  if (typeof segment === "number" || typeof segment === "string") {
    return segment;
  }
  if (
    segment != null &&
    typeof segment === "object" &&
    "from" in segment &&
    "to" in segment
  ) {
    const { from, to } = segment as { from?: unknown; to?: unknown };
    if (
      (typeof from === "number" || typeof from === "string") &&
      (typeof to === "number" || typeof to === "string")
    ) {
      return { from, to };
    }
  }
  return null;
}

function comparablePeriod(period: unknown): Period | null {
  if (Array.isArray(period)) {
    const segments: PeriodSegment[] = [];
    for (const segment of period) {
      const comparable = comparablePeriodSegment(segment);
      if (comparable == null) {
        return null;
      }
      segments.push(comparable);
    }
    return segments;
  }
  return comparablePeriodSegment(period);
}

function windowCoverageMessage(
  label: string,
  gaps: readonly StudyWindow[],
  window: StudyWindow,
): string | null {
  if (gaps.length === 0) {
    return null;
  }
  const missing = gaps.map(formatYearRange).join(", ");
  return `${label} does not cover ${missing} within your study window ${window.from}..${window.to}.`;
}

function formatYearRange(window: StudyWindow): string {
  return window.from === window.to
    ? String(window.from)
    : `${window.from}..${window.to}`;
}

function uncoveredStudyWindowIntervals(
  intervals: readonly StudyWindow[],
  window: StudyWindow,
): StudyWindow[] {
  let cursor = window.from;
  const gaps: StudyWindow[] = [];
  for (const interval of intervals) {
    if (interval.to < cursor) {
      continue;
    }
    if (interval.from > window.to) {
      break;
    }
    if (interval.from > cursor) {
      gaps.push({ from: cursor, to: Math.min(interval.from - 1, window.to) });
    }
    cursor = Math.max(cursor, interval.to + 1);
    if (cursor > window.to) {
      break;
    }
  }
  if (cursor <= window.to) {
    gaps.push({ from: cursor, to: window.to });
  }
  return gaps;
}

/** Client-side project-window hint for the read-only cart (#994). This is NOT a
 * synthetic ValidationIssue: it compares two values already in the draft and
 * gently points the user back to the register page to stage an extension. Token
 * periods are skipped because mixed-grain coverage is not safely comparable. */
export function windowCoverageHints(
  window: StudyWindow | null | undefined,
  sources: readonly SafeSource[],
): WindowCoverageHint[] {
  if (
    window == null ||
    !Number.isInteger(window.from) ||
    !Number.isInteger(window.to)
  ) {
    return [];
  }
  const hints: WindowCoverageHint[] = [];
  for (const [index, source] of sources.entries()) {
    const period = comparablePeriod(safeSourcePeriod(source));
    if (period == null) {
      continue;
    }
    const intervals = periodYearIntervals(period);
    if (intervals == null) {
      continue;
    }
    const rawName = safeSourceName(source);
    const label =
      rawName.length > 0 ? `Source '${rawName}'` : `Source ${index + 1}`;
    const rv = safeSourceRegisterVariant(source);
    const registerPrefix = rv.length > 0 ? registerPrefixOf(rv) : "";
    const gaps = uncoveredStudyWindowIntervals(intervals, window);
    const message = windowCoverageMessage(label, gaps, window);
    if (message == null) {
      continue;
    }
    const catalog =
      registerPrefix.length > 0
        ? {
            catalogHref: catalogHref(registerPrefix),
            catalogLabel: registerPrefix,
          }
        : {};
    hints.push({
      label,
      message,
      ...catalog,
    });
  }
  return hints;
}

export function findingLocation(
  path: string,
  sources: readonly SafeSource[],
): FindingLocation | null {
  const tokens = parseJsonPointer(path);
  // Only /sources/<i>[/bindings/<j>/...] paths name a locatable card.
  if (tokens == null || tokens[0] !== "sources" || tokens.length < 2) {
    return null;
  }
  const sIdx = Number(tokens[1]);
  if (!Number.isInteger(sIdx) || sIdx < 0) {
    return null;
  }
  const source = sources[sIdx] ?? null;
  const rawName = safeSourceName(source);
  const sourceLabel =
    rawName.length > 0 ? `Source '${rawName}'` : `Source ${sIdx + 1}`;

  if (tokens[2] === "bindings" && tokens.length >= 4) {
    const bIdx = Number(tokens[3]);
    if (Number.isInteger(bIdx) && bIdx >= 0) {
      const bindings = safeSourceBindings(source);
      const variable = (bindings[bIdx] as { variable?: unknown } | undefined)
        ?.variable;
      const bindingLabel =
        typeof variable === "string" && variable.length > 0
          ? `binding ${variable}`
          : `binding ${bIdx + 1}`;
      // The binding's own variable FQID is its catalog subject page; a fresh row
      // with no variable yet has no target → omit the link.
      const catalog =
        typeof variable === "string" && variable.length > 0
          ? { catalogHref: catalogHref(variable), catalogLabel: variable }
          : {};
      return {
        label: `${sourceLabel} → ${bindingLabel}`,
        anchorId: bindingAnchorId(sIdx, bIdx),
        ...catalog,
      };
    }
  }
  // A source-level issue points at the source's REGISTER page — the 2-seg
  // `provider/register` prefix of the `register_variant`, NOT the 3-seg coordinate.
  // A variant slug is a query axis, not a browsable catalog node, so a 3-seg
  // `/catalog/<provider>/<register>/<variant>` link would be dead (#993). When the
  // register_variant has fewer than 2 segments (`registerPrefixOf` → "") there is no
  // valid register prefix, so the link is omitted (the no-catalog fallback).
  const rv = safeSourceRegisterVariant(source);
  const registerPrefix = rv.length > 0 ? registerPrefixOf(rv) : "";
  const catalog =
    registerPrefix.length > 0
      ? {
          catalogHref: catalogHref(registerPrefix),
          catalogLabel: registerPrefix,
        }
      : {};
  return {
    label: sourceLabel,
    anchorId: sourceAnchorId(sIdx),
    ...catalog,
  };
}
