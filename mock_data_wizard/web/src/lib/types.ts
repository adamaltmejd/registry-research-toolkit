/**
 * Wire types mirroring `mock_data_wizard._serialize.state_snapshot_to_dict`.
 *
 * Drift between this file and the Python serialiser is detected by the
 * golden-fixture contract test (`types.test.ts` ↔
 * `mock_data_wizard/tests/data/state_snapshot.golden.json`). When the
 * Python side renames or adds a field, regenerate the golden via
 * `pytest --update-golden`, then update these interfaces and the type
 * guards below. The contract test fails until both sides agree.
 */

export type ColumnType = "id" | "categorical" | "numeric" | "opaque" | "date";
export type IdSubtype = "integer" | "string";
export type NumericSubtype = "integer" | "double";
export type Confidence = "high" | "partial" | "none";
export type Provenance = "manual" | "auto";

export interface ColumnTypeOverride {
  type: ColumnType;
  /** Present only when type === "id". */
  id_subtype?: IdSubtype;
  /** Present only when type === "numeric". */
  numeric_subtype?: NumericSubtype;
  /** Present only when type === "date". */
  date_format?: string;
}

export interface PanelMember {
  source: string;
  /** Set for file-period members; mutually exclusive with `time_key`. */
  period?: number;
  /** Set for column-driven members; mutually exclusive with `period`. */
  time_key?: string;
}

export interface Panel {
  panel_id: string;
  panel_key: string;
  members: PanelMember[];
}

export interface RegmetaSignal {
  /** "numeric" | "date" | null — see classify.RegmetaSignal. */
  datatyp_kind: string | null;
  classification_short_name: string | null;
  has_value_codes: boolean;
}

export interface ColumnInfo {
  name: string;
  sql_type: string | null;
  current_type: ColumnType;
  /** Inline subtype/format hint projected from the active override. */
  hint: Record<string, unknown> | null;
  provenance: Provenance;
  regmeta_signal: RegmetaSignal | null;
  regmeta_implied_type: ColumnType | null;
}

export interface PanelCandidateMember {
  source: string;
  period?: number;
  time_key?: string;
}

export interface PanelCandidate {
  members: PanelCandidateMember[];
  suggested_panel_id: string | null;
  suggested_panel_key: string | null;
}

export interface RegisterGroupView {
  group_id: string;
  register_id: number | null;
  register_name: string | null;
  confidence: Confidence;
  sources: string[];
  columns_by_source: Record<string, ColumnInfo[]>;
  schema_variants: number;
  panel_candidate: PanelCandidate | null;
}

export interface MDWConfig {
  contract_version: string;
  discover_hash: string | null;
  column_types: Record<string, Record<string, ColumnTypeOverride>>;
  /** Per-source × per-column option dict. Validated server-side; the UI
   * treats values as opaque keyed scalars. */
  column_options: Record<string, Record<string, Record<string, unknown>>>;
  sources: Record<string, { year?: number | null; register?: string | null }>;
  panels: Panel[];
  manual_columns: [string, string][];
}

export interface EditorWarning {
  code: string;
  message: string;
  context: Record<string, unknown>;
}

export interface StateSnapshot {
  config: MDWConfig;
  groups: RegisterGroupView[];
  /** The discover payload as-loaded; opaque to the UI other than for
   * surfacing source-level metadata. Null when no discover was found. */
  discover: Record<string, unknown> | null;
  warnings: EditorWarning[];
  snapshot_version: string;
}

export interface RegisterEntry {
  id: number;
  name: string;
}

export interface RegistersResponse {
  registers: RegisterEntry[];
}

/**
 * Error envelope returned by the Python server. `context.fresh_state`
 * is populated on `stale_state` (409) when the server could fetch a
 * fresh snapshot.
 */
export interface ApiErrorEnvelope {
  error: {
    code: string;
    message: string;
    context?: Record<string, unknown>;
  };
}

// -- Type guards ---------------------------------------------------------

const COLUMN_TYPES = new Set<ColumnType>([
  "id",
  "categorical",
  "numeric",
  "opaque",
  "date",
]);

function isObject(x: unknown): x is Record<string, unknown> {
  return typeof x === "object" && x !== null && !Array.isArray(x);
}

function isStringOrNull(x: unknown): x is string | null {
  return x === null || typeof x === "string";
}

function isNumberOrNull(x: unknown): x is number | null {
  return x === null || typeof x === "number";
}

function isColumnType(x: unknown): x is ColumnType {
  return typeof x === "string" && COLUMN_TYPES.has(x as ColumnType);
}

function isPanelMember(x: unknown): x is PanelMember {
  if (!isObject(x)) return false;
  if (typeof x.source !== "string") return false;
  const hasPeriod = "period" in x;
  const hasTimeKey = "time_key" in x;
  if (hasPeriod === hasTimeKey) return false;
  if (hasPeriod && typeof x.period !== "number") return false;
  if (hasTimeKey && typeof x.time_key !== "string") return false;
  return true;
}

function isRegmetaSignal(x: unknown): x is RegmetaSignal {
  if (!isObject(x)) return false;
  return (
    isStringOrNull(x.datatyp_kind) &&
    isStringOrNull(x.classification_short_name) &&
    typeof x.has_value_codes === "boolean"
  );
}

function isColumnInfo(x: unknown): x is ColumnInfo {
  if (!isObject(x)) return false;
  return (
    typeof x.name === "string" &&
    isStringOrNull(x.sql_type) &&
    isColumnType(x.current_type) &&
    (x.hint === null || isObject(x.hint)) &&
    (x.provenance === "manual" || x.provenance === "auto") &&
    (x.regmeta_signal === null || isRegmetaSignal(x.regmeta_signal)) &&
    (x.regmeta_implied_type === null || isColumnType(x.regmeta_implied_type))
  );
}

function isPanelCandidate(x: unknown): x is PanelCandidate {
  if (!isObject(x)) return false;
  if (!Array.isArray(x.members) || !x.members.every(isPanelMember)) return false;
  return (
    isStringOrNull(x.suggested_panel_id) && isStringOrNull(x.suggested_panel_key)
  );
}

function isRegisterGroupView(x: unknown): x is RegisterGroupView {
  if (!isObject(x)) return false;
  if (typeof x.group_id !== "string") return false;
  if (!isNumberOrNull(x.register_id)) return false;
  if (!isStringOrNull(x.register_name)) return false;
  if (x.confidence !== "high" && x.confidence !== "partial" && x.confidence !== "none")
    return false;
  if (!Array.isArray(x.sources) || !x.sources.every((s) => typeof s === "string"))
    return false;
  if (!isObject(x.columns_by_source)) return false;
  for (const cols of Object.values(x.columns_by_source)) {
    if (!Array.isArray(cols) || !cols.every(isColumnInfo)) return false;
  }
  if (typeof x.schema_variants !== "number") return false;
  if (x.panel_candidate !== null && !isPanelCandidate(x.panel_candidate))
    return false;
  return true;
}

function isMDWConfig(x: unknown): x is MDWConfig {
  if (!isObject(x)) return false;
  return (
    typeof x.contract_version === "string" &&
    isStringOrNull(x.discover_hash) &&
    isObject(x.column_types) &&
    isObject(x.column_options) &&
    isObject(x.sources) &&
    Array.isArray(x.panels) &&
    Array.isArray(x.manual_columns)
  );
}

function isEditorWarning(x: unknown): x is EditorWarning {
  if (!isObject(x)) return false;
  return (
    typeof x.code === "string" &&
    typeof x.message === "string" &&
    isObject(x.context)
  );
}

/**
 * Structural validation of a `StateSnapshot` parsed from JSON. Used by
 * the golden-fixture contract test; runtime callers should trust the
 * server's typed shape and skip this check.
 */
export function isStateSnapshot(x: unknown): x is StateSnapshot {
  if (!isObject(x)) return false;
  return (
    isMDWConfig(x.config) &&
    Array.isArray(x.groups) &&
    x.groups.every(isRegisterGroupView) &&
    (x.discover === null || isObject(x.discover)) &&
    Array.isArray(x.warnings) &&
    x.warnings.every(isEditorWarning) &&
    typeof x.snapshot_version === "string"
  );
}
