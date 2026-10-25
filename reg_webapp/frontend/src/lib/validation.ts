/**
 * Pure validation-presentation helpers (unit-tested + corpus-pinned;
 * `validation.test.ts`). The backend is the CANONICAL validator (see
 * reg_webapp/DESIGN.md → Pydantic boundary): the SPA
 * never re-implements the server's validation rules — it only PARSES the issue list the server
 * returns (`ValidationResultModel`) and maps each `code` to a UI label/severity
 * hint for presentation.
 *
 * Three responsibilities:
 * 1. RFC-6901 JSON-pointer decoding (`parseJsonPointer`) so an issue `path`
 *    (`/sources/0/bindings/0/typ`) locates the source card and column lane it
 *    concerns (`findingLocation`), where the finding renders (#1906).
 * 2. One finding shape for every check (`projectFindings`): a validation issue, a
 *    blocked order's finding and the study window's disjoint source.
 * 3. The `KNOWN_CODES` registry: every stable validation code → a friendly label +
 *    the level it's typically raised at. Hand-maintained from the validator
 *    sources: structural (`crates/reg-core/src/structural.rs`; see crates/reg-core/DESIGN.md →
 *    Structural rules and issue codes), semantic (see crates/DESIGN.md →
 *    Project semantic validation) and the ORDER MATERIALIZER's findings
 *    (`crates/reg-catalog/src/ops/order.rs`), which the panel renders through the same path. An
 *    UNKNOWN code degrades gracefully (the issue is still shown with its raw
 *    code + level).
 */

import type { OrderBlocking } from "./api";
import type { components as RustComponents } from "./api-types-rust";
import { registerPrefixOf } from "./catalog";
import { periodLabel, periodWindowRelation, yearWindowLabel } from "./period";
import {
  type ProjectStudyWindow,
  type SafeSource,
  safeSourceBindings,
  safeSourceName,
  safeSourcePeriod,
  safeSourceRegisterVariant,
} from "./project_data";

export type ValidationIssue = RustComponents["schemas"]["ValidationIssue"];
export type ValidationResult = RustComponents["schemas"]["Validation"];

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
 * document → `""`. Callers build a location's pointer with this, so an SPA
 * finding is located by the same `findingLocation` as a server one. */
export function jsonPointer(tokens: (string | number)[]): string {
  if (tokens.length === 0) {
    return "";
  }
  return `/${tokens
    .map((token) => String(token).replace(/~/g, "~0").replace(/\//g, "~1"))
    .join("/")}`;
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
 * - structural (`crates/reg-core/src/structural.rs`; see crates/reg-core/DESIGN.md → Structural rules and issue codes)
 * - semantic (`crates/reg-catalog/src/ops/validate.rs`; see crates/DESIGN.md → Project semantic validation)
 * - the order materializer's findings (`crates/reg-catalog/src/ops/order.rs`; see crates/DESIGN.md → Order manifest)
 */
export const KNOWN_CODES: Record<string, CodeInfo> = {
  // ── the version decision (`reg_core::project::check`) ─────────────────────
  unsupported_schema_version: {
    label: "Project written for another schema version",
    hint: "error",
  },
  // reg-core-wasm: JSON the browser parsed but reg-core cannot read (nested
  // deeper than its 128-level limit).
  invalid_json: { label: "Project file could not be read", hint: "error" },
  // ── structural ────────────────────────────────────────────────────────────
  invalid_root: { label: "Root must be a JSON object", hint: "error" },
  missing_required_field: { label: "Missing required field", hint: "error" },
  invalid_field_type: { label: "Wrong field type", hint: "error" },
  invalid_enum_value: { label: "Value outside the allowed set", hint: "error" },
  unexpected_field: { label: "Unexpected field", hint: "error" },
  invalid_fqid: { label: "Malformed FQID", hint: "error" },
  invalid_slug: { label: "Malformed steward id", hint: "error" },
  fqid_register_variant_mismatch: {
    label: "Column FQID prefix does not match the source's register_variant",
    hint: "error",
  },
  invalid_period: { label: "Invalid period", hint: "error" },
  invalid_window: {
    label: "Study window ends before it starts",
    hint: "error",
  },
  subtype_on_wrong_type: {
    label: "Subtype/format field set on a column type that doesn't own it",
    hint: "error",
  },
  empty_bindings: { label: "Source has no columns", hint: "error" },
  duplicate_source_name: { label: "Duplicate source name", hint: "error" },
  display_name_collision: {
    label: "Two columns on a source share a display_name",
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
  // structural didn't replicate (the server's `validate`).
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
    label: "No variable state covers the column's (variant, period)",
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
  variable_sensitive: {
    label: "The column is a sensitive variable",
    hint: "warning",
  },
  binding_state_drifts_within_period: {
    label: "The period crosses a state transition (per-state subsets returned)",
    hint: "info",
  },
  source_period_narrower_than_available: {
    // The one period finding (#1836): availability narrower than a period is the
    // order's clip, not a finding; this is the reverse.
    label:
      "The source's period is narrower than what its columns deliver in the study window",
    hint: "warning",
  },
  deprecated_traversal: {
    label: "The column resolves through deprecated catalog metadata",
    hint: "info",
  },
  variable_replaced: {
    label: "The column has a replacement edge at/before this period",
    hint: "info",
  },
  // ── order materializer ────────────────────────────────────────────────────
  // Every one BLOCKS the order: the materializer is fail-closed and emits a
  // manifest or findings, never a partial order. Most reach the panel from
  // `/order`'s 422 — but `/validate` runs the materializer's own availability
  // pass, so `period_not_orderable` and `representation_unresolved` can arrive
  // from either endpoint under these same names.
  // Short CLASSIFIERS, not sentences: an order finding's own message already
  // states the case WITH its coordinates in it, so a title paraphrasing it costs
  // a line and adds nothing (the structural/semantic labels above lead rows
  // whose messages are far terser).
  steward_mismatch: { label: "Steward mismatch", hint: "error" },
  project_unnamed: { label: "Unnamed project", hint: "error" },
  project_empty: { label: "Empty project", hint: "error" },
  period_not_orderable: { label: "Period not orderable", hint: "error" },
  variable_unresolved: { label: "Unresolved variable", hint: "error" },
  binding_unavailable: { label: "Variable unavailable", hint: "error" },
  representation_unknown: { label: "Unknown representation", hint: "error" },
  representation_unresolved: {
    label: "Unresolved representation",
    hint: "error",
  },
  representation_ambiguous: {
    label: "Ambiguous representation",
    hint: "error",
  },
  mapping_missing: { label: "Unmapped variable", hint: "error" },
  coverage_gap: { label: "Coverage gap", hint: "error" },
};

/** The friendly label for a code, or the raw code when it isn't registered (an
 * unknown/new code degrades gracefully — codes are additive). */
export function codeLabel(code: string): string {
  return KNOWN_CODES[code]?.label ?? code;
}

/** The DOM-anchor id of a source card: where focus goes after a removal, and
 * the card a finding of that source renders in. */
export function sourceAnchorId(sourceIndex: number): string {
  return `loc-source-${sourceIndex}`;
}

/** Where a finding sits in the draft, read off its RFC-6901 `path`: the source
 * card and, for a `/bindings/{j}` path, the column lane inside it. A finding
 * renders there, under what it concerns (#1906); the summary panel only links to
 * it. `catalogFqid` is the catalog page where it is fixed, since the cart is
 * read-only (#991): the binding's variable, else the source's REGISTER (a variant
 * slug is a query axis, not a browsable node, so a 3-segment link would be dead,
 * #993). Null when that coordinate is missing or blank (a fresh unpicked row).
 *
 * Naming the place is the view's job: the words come from catalog reads
 * (`catalogNames`), which a pure locator cannot make. */
export interface FindingLocation {
  sourceIndex: number;
  /** The column lane, or null for a finding about the source itself. */
  bindingIndex: number | null;
  catalogFqid: string | null;
}

/** A whole-document path (`""`) or a top-level one (`/name`, `/window`) has no
 * card: null, and the finding stays in the summary panel. */
export function findingLocation(
  path: string,
  sources: readonly SafeSource[],
): FindingLocation | null {
  const tokens = parseJsonPointer(path);
  if (tokens == null || tokens[0] !== "sources" || tokens.length < 2) {
    return null;
  }
  const sIdx = Number(tokens[1]);
  if (!Number.isInteger(sIdx) || sIdx < 0) {
    return null;
  }
  const source = sources[sIdx] ?? null;
  if (tokens[2] === "bindings" && tokens.length >= 4) {
    const bIdx = Number(tokens[3]);
    if (Number.isInteger(bIdx) && bIdx >= 0) {
      const variable = bindingVariableAt(source, bIdx);
      return {
        sourceIndex: sIdx,
        bindingIndex: bIdx,
        catalogFqid: variable || null,
      };
    }
  }
  const rv = safeSourceRegisterVariant(source);
  return {
    sourceIndex: sIdx,
    bindingIndex: null,
    catalogFqid: (rv.length > 0 && registerPrefixOf(rv)) || null,
  };
}

/** The variable FQID of a source's `index`th binding, or "" when it has none. */
function bindingVariableAt(source: SafeSource, index: number): string {
  const variable = (
    safeSourceBindings(source)[index] as { variable?: unknown } | undefined
  )?.variable;
  return typeof variable === "string" ? variable : "";
}

/** One source whose period has NO years inside the study window: the project's
 * blocking authoring finding (reg_webapp/DESIGN.md → "Common study window"). */
export interface WindowDisjointFinding {
  message: string;
  location: FindingLocation | null;
}

/** Every source left disjoint from the common study window — what blocks the
 * order download while the source keeps its explicit period. An SPA authoring
 * rule, not a re-implementation of a server one: the backend reads `window` only
 * for `source_period_narrower_than_available`, so this is the one place a disjoint
 * period is enforced. Located like any finding, so it renders on its card. */
export function windowDisjointFindings(
  window: ProjectStudyWindow | null,
  sources: readonly SafeSource[],
): WindowDisjointFinding[] {
  if (window === null) {
    return [];
  }
  const findings: WindowDisjointFinding[] = [];
  for (const [index, source] of sources.entries()) {
    const period = safeSourcePeriod(source);
    if (periodWindowRelation(period, window) !== "disjoint") {
      continue;
    }
    const years = periodLabel(period) ?? "";
    findings.push({
      message: `Its period ${years} has no years inside the study window ${yearWindowLabel(window)}. Change the period to overlap the window, or remove the source.`,
      location: findingLocation(jsonPointer(["sources", index]), sources),
    });
  }
  return findings;
}

export type FindingLevel = "error" | "warning" | "info";
export const FINDING_LEVELS: readonly FindingLevel[] = [
  "error",
  "warning",
  "info",
];

/** Every finding on the project in one shape, whichever check raised it: a
 * `/validate` issue, a blocked order's finding (`/order`'s 422) or the study
 * window's disjoint source. They differ in how they name their place, not in how
 * they are shown. */
export interface ProjectFinding {
  /** Unique on the page: the DOM id the summary panel links to. */
  id: string;
  check: "validation" | "order" | "window";
  level: FindingLevel;
  title: string;
  /** The stable code people cite; null for the SPA's own window finding. */
  code: string | null;
  message: string;
  /** A blocked order's offending subperiod: the one edit that fixes it. */
  period: string | null;
  location: FindingLocation | null;
  /** The catalog coordinates the message may print, so the view can say them in
   * words (`plainMessage`): the located binding's variable, the source's
   * register variant, a replacement's successor, an order finding's variable. */
  fqids: string[];
}

/** The coordinates of a located finding's card: its source's register variant
 * and, for a binding, its variable. */
function locatedFqids(
  location: FindingLocation | null,
  sources: readonly SafeSource[],
): string[] {
  if (location === null) return [];
  const source = sources[location.sourceIndex] ?? null;
  const registerVariant = safeSourceRegisterVariant(source);
  return [
    registerVariant,
    // An unknown variant's message names the register it was looked up in.
    registerPrefixOf(registerVariant),
    location.bindingIndex === null
      ? ""
      : bindingVariableAt(source, location.bindingIndex),
  ].filter((fqid) => fqid.length > 0);
}

/** A DOM id from a finding's content, with an occurrence suffix where two
 * findings share one (two identical findings). Kept to a plain alphabet: a JSON
 * pointer's `/` would need escaping in a selector. */
function findingIds(bases: readonly string[]): string[] {
  const seen = new Map<string, number>();
  return bases.map((raw) => {
    const base = `finding-${raw.replace(/[^A-Za-z0-9_-]/g, "_")}`;
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    return n === 0 ? base : `${base}-${n}`;
  });
}

export function projectFindings({
  issues,
  orderFindings,
  windowFindings,
  sources,
}: {
  issues: readonly ValidationIssue[];
  orderFindings: readonly OrderBlocking[];
  windowFindings: readonly WindowDisjointFinding[];
  sources: readonly SafeSource[];
}): ProjectFinding[] {
  const order = orderFindings.map((f): Omit<ProjectFinding, "id"> => {
    const location = findingLocation(orderFindingPointer(f, sources), sources);
    return {
      check: "order",
      level: "error",
      title: codeLabel(f.code),
      code: f.code,
      message: f.message,
      period: f.period ?? null,
      location,
      fqids: [
        ...locatedFqids(location, sources),
        ...(f.variable ? [f.variable] : []),
      ],
    };
  });
  const window = windowFindings.map(
    (f): Omit<ProjectFinding, "id"> => ({
      check: "window",
      level: "error",
      title: "No years inside the study window",
      code: null,
      message: f.message,
      period: null,
      location: f.location,
      fqids: [],
    }),
  );
  const validation = issues.map((issue): Omit<ProjectFinding, "id"> => {
    const location = findingLocation(issue.path, sources);
    return {
      check: "validation",
      // An unknown level (forward-compat) reads as info, so it still shows.
      level: FINDING_LEVELS.includes(issue.level as FindingLevel)
        ? (issue.level as FindingLevel)
        : "info",
      title: codeLabel(issue.code),
      code: issue.code,
      message: issue.message,
      period: null,
      location,
      fqids: [
        ...locatedFqids(location, sources),
        ...(issue.successor_fqid ? [issue.successor_fqid] : []),
      ],
    };
  });
  const all = [...order, ...window, ...validation];
  const ids = findingIds(
    all.map((f) =>
      [
        f.check,
        f.code ?? "",
        f.location?.sourceIndex ?? "",
        f.location?.bindingIndex ?? "",
      ].join("-"),
    ),
  );
  return all.map((f, i) => ({ ...f, id: ids[i] }));
}

/** A finding's message with each catalog coordinate it names said in the
 * catalog's words: "column 'scb/lisa/cfar-nummer' spans …" reads "column
 * 'Arbetsställets CFAR-nummer' spans …". Only the coordinates the finding is
 * known to carry are replaced, never a pattern hunted in the prose, and only as
 * whole coordinates, so `scb/lisa/kon` leaves `scb/lisa/kon-2` alone. One the
 * catalog cannot name (an unresolvable FQID) stays: the honest thing to print. */
export function plainMessage(
  message: string,
  fqids: readonly string[],
  nameOf: (fqid: string) => string | null,
): string {
  let out = message;
  for (const fqid of new Set(fqids)) {
    const name = nameOf(fqid);
    if (!name) continue;
    const escaped = fqid.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    out = out.replace(
      new RegExp(`(?<![\\w/-])${escaped}(?![\\w/-])`, "g"),
      () => name,
    );
  }
  return out;
}

/** The RFC-6901 pointer an ORDER finding names, so a blocked order renders
 * through the SAME `findingLocation` path as a validation issue.
 *
 * The two validators address the draft differently: the validator speaks in
 * POSITIONS (it walked the document), the order materializer speaks in VALUES —
 * the source's `name` and the binding's `variable` FQID, because it walked the
 * MODEL. This resolves those values back to positions against the current draft.
 *
 * `""` (the whole document) when nothing matches: a project-level finding
 * (`steward_mismatch`, `project_unnamed`, `project_empty`) names no coordinate,
 * and a source or binding edited away since the request has no card left to
 * locate. The panel's existing no-location fallback then applies.
 */
export function orderFindingPointer(
  finding: { source?: string | null; variable?: string | null },
  sources: readonly SafeSource[],
): string {
  if (!finding.source) {
    return "";
  }
  const sIdx = sources.findIndex((s) => safeSourceName(s) === finding.source);
  if (sIdx < 0) {
    return "";
  }
  if (!finding.variable) {
    return jsonPointer(["sources", sIdx]);
  }
  const bIdx = safeSourceBindings(sources[sIdx]).findIndex(
    (b) => (b as { variable?: unknown }).variable === finding.variable,
  );
  return bIdx < 0
    ? jsonPointer(["sources", sIdx])
    : jsonPointer(["sources", sIdx, "bindings", bIdx]);
}

/** The loaded project's `h1`: where focus goes when the last source card it
 * could land on is removed. */
export const PROJECT_HEADING_ID = "project-heading";
