/** Presentation only. WASM and the catalog own validation and issue levels. */
import type { components } from "./api-types-rust";

export type ValidationIssue = components["schemas"]["Issue"];

// Only current reg-core codes whose generic server messages benefit from a title.
const TITLES: Readonly<Record<string, string>> = {
  unsupported_schema_version: "Project uses another schema version",
  invalid_json: "Project file could not be read",
  invalid_root: "Project must be a JSON object",
  missing_required_field: "Required field is missing",
  invalid_field_type: "Field has the wrong type",
  unexpected_field: "Field is not supported",
  invalid_period: "Period is invalid",
  invalid_window: "Study window is invalid",
  empty_bindings: "Source has no variables",
  duplicate_source_name: "Source names must be unique",
  display_name_collision: "Output names must be unique within a source",
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

/** Read only the named slots of a rejected document; never coerce it into a draft. */
export function issuePlace(
  issue: ValidationIssue,
  raw: string | null,
): {
  source: string | null;
  variable: string | null;
  period: unknown;
} {
  let root: unknown = null;
  try {
    root = raw === null ? null : JSON.parse(raw);
  } catch {
    /* The WASM issue explains unreadable JSON. */
  }
  const path = parseJsonPointer(issue.path);
  const sources =
    root && typeof root === "object" && "sources" in root ? root.sources : null;
  const source: unknown =
    path[0] === "sources" && Array.isArray(sources)
      ? sources[Number(path[1])]
      : null;
  const object =
    source && typeof source === "object"
      ? (source as Record<string, unknown>)
      : null;
  const bindings = object?.bindings;
  const binding: unknown =
    path[2] === "bindings" && Array.isArray(bindings)
      ? bindings[Number(path[3])]
      : null;
  const variable =
    binding && typeof binding === "object" && "variable" in binding
      ? binding.variable
      : null;
  return {
    source:
      issue.source ?? (typeof object?.name === "string" ? object.name : null),
    variable:
      issue.variable ?? (typeof variable === "string" ? variable : null),
    period: object?.period,
  };
}

export function issueId(issue: ValidationIssue): string {
  return `issue-${encodeURIComponent(JSON.stringify([issue.code, issue.path, issue.message]))}`;
}

export function plainIssueText(
  text: string,
  refs: readonly (string | null)[],
  names: ReadonlyMap<string, string>,
): string {
  return [...new Set(refs.filter((ref): ref is string => Boolean(ref)))]
    .sort((a, b) => b.length - a.length)
    .reduce(
      (message, ref) =>
        message.replaceAll(ref, names.get(ref) ?? "this catalog item"),
      text,
    );
}
