/** Presentation only. WASM and the catalog own validation and issue levels. */
import type { components } from "./api-types-rust";

export type ValidationIssue = components["schemas"]["Issue"];

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
  invalid_period: "Period is invalid",
  invalid_window: "Study window is invalid",
  subtype_on_wrong_type: "Format or subtype does not match the variable type",
  empty_bindings: "Source has no variables",
  duplicate_source_name: "Source names must be unique",
  display_name_collision: "Output names must be unique within a source",
  duplicate_panel_id: "Panel names must be unique",
  empty_members: "Panel has no sources",
  panel_member_unknown_source:
    "Panel names a source that is not in the project",
  source_referenced_by_multiple_panels: "Source belongs to more than one panel",
  panel_row_keys_differ: "Panel sources use different row identifiers",
  fqid_unresolved: "Catalog item was not found",
  retired_ref: "Catalog item has been retired",
  table_is_register: "Choose a table within the register",
  variable_not_in_table: "Variable is not available in this table",
  outside_availability: "Variable is unavailable for the requested period",
  edition_disagreement: "Editions disagree about this variable",
  classification_incompatible:
    "Classification does not match the requested period",
  classification_version_ambiguous: "Choose one classification version",
  ambiguous_window: "The requested period needs curation",
  coverage_end_unstated: "The end of coverage has not been confirmed",
  form_change: "The delivered column changes during the requested period",
  period_clipped: "Only part of the requested period is available",
  preliminary_data: "The requested data are preliminary",
  variable_replaced: "Variable has a successor",
  variable_sensitive: "Variable contains sensitive information",
  source_outside_window: "Source is outside the study window",
  table_succession_undated: "The table succession has no stated date",
  period_default_on_dated_table: "Choose a period for this dated table",
  period_on_timeless_table: "This table does not use dated periods",
  period_not_orderable: "The requested period cannot be ordered",
  project_empty: "Project has no variables to order",
};

export function issueTitle(issue: ValidationIssue): string {
  return TITLES[issue.code] ?? issue.message;
}

export function parseJsonPointer(path: string): string[] {
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

/** Read only the named slots of a rejected document; never coerce it into a draft. */
export function issuePlace(
  issue: ValidationIssue,
  raw: string | null,
): {
  source: string | null;
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
): string {
  // A register-level refusal can list its tables in the message without naming
  // them in Issue.table. Resolve those names from the already-read register too.
  return [
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
}
