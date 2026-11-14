/** Presentation only. WASM and the catalog own validation and issue levels. */
import type { components } from "./api-types-rust";
import { catalogPeriodLabel } from "./catalog_period";

/** A finding, with the server's names for what it is about when the order check
 * gave them (`NamedIssue`); a structural issue from WASM has none. */
export type ValidationIssue = components["schemas"]["Issue"] &
  Partial<
    Pick<
      components["schemas"]["NamedIssue"],
      "variable_name" | "table_name" | "successor_name"
    >
  >;

/** The server's names for a finding's ids. They stand in where the site has no
 * page to read a name from: a variable this steward does not offer (#2220). */
export function issueNames(issue: ValidationIssue): Map<string, string> {
  const names = new Map<string, string>();
  for (const [ref, name] of [
    [issue.variable, issue.variable_name],
    [issue.table, issue.table_name],
    [issue.successor_fqid, issue.successor_name],
  ] as const)
    if (ref && name) names.set(ref, name);
  return names;
}

// Current reg-core structural codes and catalog order/check findings. Their
// messages remain the detail; titles name the problem without machine grammar.
const TITLES: Readonly<Record<string, string>> = {
  unsupported_schema_version: "Project uses another schema version",
  invalid_json: "Project file could not be read",
  invalid_root: "Project must be a JSON object",
  missing_required_field: "Required field is missing",
  invalid_field_type: "Field has the wrong type",
  invalid_enum_value: "Choose a supported field value",
  invalid_fqid: "Catalog reference is invalid",
  fqid_table_mismatch: "Variable and table belong to different registers",
  unexpected_field: "Field is not supported",
  invalid_period: "Years are invalid",
  invalid_window: "Study years are invalid",
  empty_bindings: "Table has no columns",
  duplicate_source_name: "Table names must be unique",
  display_name_collision: "Two columns of one table are given the same name",
  duplicate_panel_id: "Panel names must be unique",
  empty_members: "Panel has no tables",
  panel_member_unknown_source: "Panel names a table that is not in the project",
  source_referenced_by_multiple_panels: "Table belongs to more than one panel",
  fqid_unresolved: "Catalog item was not found",
  retired_ref: "Catalog item has been retired",
  table_is_register: "Choose a table within the register",
  variable_not_in_table: "Variable is not available in this table",
  outside_availability: "Variable is not available for the study years",
  edition_disagreement: "Editions disagree about this variable",
  classification_incompatible: "Classification does not match the study years",
  classification_version_ambiguous: "Choose one classification version",
  ambiguous_window: "The variable has more than one form in these years",
  coverage_end_unstated: "The end of coverage has not been confirmed",
  form_change: "The delivered column changes during the study years",
  series_break: "Series break during the study years",
  binding_type_differs: "The type in the project differs from the catalog",
  period_clipped: "Years not available",
  preliminary_data: "The requested data are preliminary",
  variable_replaced: "Variable has a successor",
  sensitive_variables: "Some variables may contain sensitive personal data",
  source_outside_window: "Table is outside the study years",
  table_succession_undated: "The table succession has no stated date",
  period_default_on_dated_table: "Choose years for this dated table",
  period_on_timeless_table: "This table has no years",
  period_not_orderable: "These years cannot be read",
  project_empty: "Project has no variables",
  // The steward's tables (a steward server's `order` runs this stage too).
  // `mapping_caveat` is not shown (api.ts `forReaders`).
  mapping_missing: "This column is not available for these years",
  coverage_gap: "This column is not available for some years",
  column_window_unavailable: "This column leaves out some requested years",
  held_key_missing: "The row identifier is not available",
  held_key_ambiguous: "The row identifier is in more than one column",
  file_name_unsafe: "This table's file name cannot be used",
  physical_schema_missing: "This table has no single location",
  rows_unfiltered: "Some rows from other years are included",
  row_filter_widened: "Some rows around these years are included",
  row_filter_union: "Tables sharing a file are delivered over all their years",
  // A steward with several databases: an order draws from one.
  database_required: "Choose a database",
  database_unknown: "This database does not exist",
  not_in_database: "This column is not in the chosen database",
  // Data the project states by hand, which the catalog does not describe.
  manual_item: "Not checked",
};

export function issueTitle(issue: ValidationIssue): string {
  return TITLES[issue.code] ?? issue.message;
}

function parseJsonPointer(path: string): string[] {
  return path.startsWith("/")
    ? path
        .slice(1)
        .split("/")
        .map((part) => part.replaceAll("~1", "/").replaceAll("~0", "~"))
    : [];
}

function objectOf(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function panelPlace(panels: unknown, path: readonly string[]): string | null {
  if (path[0] !== "panels") return null;
  if (path.length === 1) return "Panels";
  if (!/^\d+$/.test(path[1])) return null;
  const index = Number(path[1]);
  if (!Number.isSafeInteger(index)) return null;
  const id = Array.isArray(panels) ? objectOf(panels[index])?.panel_id : null;
  return `Panel ${typeof id === "string" && id.trim() ? id : index + 1}`;
}

/** Read only the named slots of a rejected document; never coerce it into a draft. */
export function issuePlace(
  issue: ValidationIssue,
  raw: string | null,
): {
  source: string | null;
  panel: string | null;
  variable: string | null;
  table: string | null;
  classification: string | null;
  period: unknown;
} {
  let root: unknown = null;
  try {
    root = raw === null ? null : JSON.parse(raw);
  } catch {
    /* The WASM issue explains unreadable JSON. */
  }
  const path = parseJsonPointer(issue.path);
  const sources = objectOf(root)?.sources;
  const source = Array.isArray(sources)
    ? path[0] === "sources"
      ? sources[Number(path[1])]
      : sources.find((source) => objectOf(source)?.name === issue.source)
    : null;
  const object = objectOf(source);
  const bindings = object?.bindings;
  const binding = Array.isArray(bindings)
    ? path[2] === "bindings"
      ? bindings[Number(path[3])]
      : bindings.find(
          (binding) => objectOf(binding)?.variable === issue.variable,
        )
    : null;
  const bindingObject = objectOf(binding);
  return {
    panel: panelPlace(objectOf(root)?.panels, path),
    source:
      issue.source ?? (typeof object?.name === "string" ? object.name : null),
    variable:
      issue.variable ??
      (typeof bindingObject?.variable === "string"
        ? bindingObject.variable
        : null),
    table:
      issue.table ?? (typeof object?.table === "string" ? object.table : null),
    classification:
      typeof bindingObject?.classification === "string"
        ? bindingObject.classification
        : null,
    period: object?.period,
  };
}

export function issueRefs(
  issue: ValidationIssue,
  raw: string | null,
): string[] {
  const place = issuePlace(issue, raw);
  return [
    ...new Set(
      [
        place.variable,
        place.table,
        place.classification,
        issue.variable,
        issue.table,
        issue.successor_fqid,
      ].filter((ref): ref is string => Boolean(ref)),
    ),
  ];
}

export function issueId(issue: ValidationIssue): string {
  return `issue-${encodeURIComponent(JSON.stringify([issue.code, issue.path, issue.message]))}`;
}

export function plainIssueText(
  text: string,
  refs: readonly (string | null)[],
  names: ReadonlyMap<string, string>,
  raw: string | null = null,
): string {
  if (raw && (text.includes("/panels") || text.includes("/sources"))) {
    try {
      const panels = objectOf(JSON.parse(raw))?.panels;
      text = text
        .replace(
          /(?<![\p{L}\p{N}_/-])\/panels(?:\/\d+)?(?![\p{L}\p{N}_/-])/gu,
          (pointer) => panelPlace(panels, parseJsonPointer(pointer)) ?? pointer,
        )
        .replace(
          /(?<![\p{L}\p{N}_/-])\/sources(?![\p{L}\p{N}_/-])/gu,
          "project tables",
        );
    } catch {
      // An unreadable draft has no panel identities to resolve.
    }
  }
  // A register-level refusal can list its tables in the message without naming
  // them in Issue.table. Resolve those names from the already-read register too.
  const message = [
    ...new Set(
      [...refs, ...names.keys()].filter((ref): ref is string => Boolean(ref)),
    ),
  ]
    .sort((a, b) => b.length - a.length)
    .reduce(
      (message, ref) =>
        message.replaceAll(ref, names.get(ref) ?? "this catalog item"),
      text,
    );
  // The server writes a period as `[start, end)` with `open` for a missing bound
  // (`span` in reg-catalog's order ops) and a study window as `2019..2020`.
  // Adapt only those exact forms, in prose with an en dash; malformed intervals
  // and other authored prose stay intact.
  return message
    .replace(
      /\[(\d{4}-\d{2}-\d{2}|open),\s*(\d{4}-\d{2}-\d{2}|open)\)/g,
      (interval, start: string, end: string) => {
        const bound = (date: string) => (date === "open" ? null : date);
        if (
          [start, end].some((date) => {
            if (date === "open") return false;
            const parsed = new Date(`${date}T00:00:00Z`);
            return (
              !Number.isFinite(parsed.getTime()) ||
              parsed.toISOString().slice(0, 10) !== date
            );
          }) ||
          (start !== "open" && end !== "open" && start >= end)
        )
          return interval;
        return (
          // Both bounds open: a year-independent span, which has every year.
          catalogPeriodLabel({ start: bound(start), end: bound(end) }) ??
          "all years"
        );
      },
    )
    .replace(/(?<![\d.-])(\d{4})\.\.(\d{4})(?![\d.-])/g, "$1–$2");
}
