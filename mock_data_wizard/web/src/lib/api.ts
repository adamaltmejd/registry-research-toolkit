/**
 * Typed fetch wrappers for the Python editor API.
 *
 * The server returns either a JSON envelope on error or a typed payload
 * on success. The `stale_state` (409) envelope carries the fresh state
 * inside `error.context.fresh_state`, so the client can re-apply
 * without an extra GET — `mutate()` surfaces it via `ApiStaleState`.
 */

import type {
  ApiErrorEnvelope,
  PanelMember,
  RegistersResponse,
  StateSnapshot,
} from "./types";

export class ApiError extends Error {
  readonly code: string;
  readonly status: number;
  readonly context: Record<string, unknown> | undefined;

  constructor(
    code: string,
    message: string,
    status: number,
    context?: Record<string, unknown>,
  ) {
    super(message);
    this.code = code;
    this.status = status;
    this.context = context;
  }
}

export class ApiStaleState extends ApiError {
  readonly freshState: StateSnapshot | undefined;

  constructor(message: string, status: number, freshState?: StateSnapshot) {
    super("stale_state", message, status, { fresh_state: freshState });
    this.freshState = freshState;
  }
}

async function parseEnvelope(response: Response): Promise<ApiErrorEnvelope> {
  // Server always returns JSON for our endpoints; anything else is a bug
  // worth surfacing rather than smoothing over.
  return (await response.json()) as ApiErrorEnvelope;
}

async function send<T>(
  url: string,
  init: RequestInit,
): Promise<T> {
  const response = await fetch(url, init);
  if (response.ok) {
    return (await response.json()) as T;
  }
  const envelope = await parseEnvelope(response);
  const { code, message, context } = envelope.error;
  if (response.status === 409 && code === "stale_state") {
    const fresh = (context?.fresh_state as StateSnapshot | undefined) ?? undefined;
    throw new ApiStaleState(message, response.status, fresh);
  }
  throw new ApiError(code, message, response.status, context);
}

export function getState(): Promise<StateSnapshot> {
  return send<StateSnapshot>("/api/state", { method: "GET" });
}

export function initProject(): Promise<StateSnapshot> {
  return send<StateSnapshot>("/api/init", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
}

export function listRegisters(): Promise<RegistersResponse> {
  return send<RegistersResponse>("/api/registers", { method: "GET" });
}

export interface SetColumnTypeArgs {
  /** Non-empty list of sources to update. Server validates every
   *  (source, column) pair before writing; a single bad pair aborts
   *  the whole call with no on-disk changes. */
  sources: string[];
  column: string;
  type: string;
  expected_version: string;
  /** Omit to leave hint unchanged; `null` clears it; an object sets it. */
  hint?: Record<string, unknown> | null;
}

export function setColumnType(args: SetColumnTypeArgs): Promise<StateSnapshot> {
  return send<StateSnapshot>("/api/column-type", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(args),
  });
}

export interface UnsetColumnManualArgs {
  /** Non-empty list of sources whose manual marker on `column` should be
   *  cleared. Pairs that aren't currently manual are silently skipped. */
  sources: string[];
  column: string;
  expected_version: string;
}

export function unsetColumnManual(
  args: UnsetColumnManualArgs,
): Promise<StateSnapshot> {
  return send<StateSnapshot>("/api/unset-column-manual", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(args),
  });
}

export interface SetGroupRegisterArgs {
  group_id: string;
  register: string | null;
  expected_version: string;
  reclassify_manual?: boolean;
}

export function setGroupRegister(
  args: SetGroupRegisterArgs,
): Promise<StateSnapshot> {
  return send<StateSnapshot>("/api/group-register", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(args),
  });
}

export interface SetSourceRegistersArgs {
  /** source_name → register name (or null to clear). Server validates
   *  every source exists, resolves every non-null name, and runs
   *  reclassification only on sources whose register actually changed
   *  (auto-only unless `reclassify_manual` forces it). */
  assignments: Record<string, string | null>;
  expected_version: string;
  reclassify_manual?: boolean;
}

export function setSourceRegisters(
  args: SetSourceRegistersArgs,
): Promise<StateSnapshot> {
  return send<StateSnapshot>("/api/source-registers", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(args),
  });
}

export interface PutPanelArgs {
  panel_id: string;
  entity_key: string;
  members: PanelMember[];
  expected_version: string;
  /** Renamed-from id when editing. The server drops the old entry in
   *  the same lock so rename doesn't collide with the new entry on
   *  member-source overlap. Omit when creating a new panel. */
  previous_panel_id?: string;
}

export function putPanel(args: PutPanelArgs): Promise<StateSnapshot> {
  return send<StateSnapshot>("/api/panel", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(args),
  });
}

export interface RemovePanelArgs {
  panel_id: string;
  expected_version: string;
}

export function removePanel(args: RemovePanelArgs): Promise<StateSnapshot> {
  return send<StateSnapshot>("/api/remove-panel", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(args),
  });
}

export interface ColumnValueCode {
  code: string;
  label: string | null;
}

export type VarianceTier = "1" | "2" | "3a" | "3b";

export interface ValueSetGroup {
  value_set_id: number;
  year_min: number | null;
  year_max: number | null;
}

export interface ClassificationGroup {
  short_name: string;
  year_min: number | null;
  year_max: number | null;
}

export interface ColumnValuesResponse {
  kind: "classification" | "values" | "none";
  title: string;
  description: string | null;
  codes: ColumnValueCode[];
  tier: VarianceTier | null;
  note: string | null;
  classifications: ClassificationGroup[];
  picked_classification: string | null;
  value_sets: ValueSetGroup[];
  picked_value_set: number | null;
}

export interface GetColumnValuesArgs {
  register: string | null;
  column: string;
  picked_classification?: string | null;
  picked_value_set?: number | null;
  /** Project source years (non-null). Server drops value-set groups
   * whose year window doesn't overlap any of these. */
  relevant_years?: number[];
}

export function getColumnValues(
  args: GetColumnValuesArgs,
): Promise<ColumnValuesResponse> {
  return send<ColumnValuesResponse>("/api/column-values", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(args),
  });
}

export interface VarinfoDescription {
  variabelnamn: string | null;
  variabeldefinition: string | null;
  variabelbeskrivning: string | null;
  variabeloperationell_definition: string | null;
  variabelreferenstid: string | null;
  variabelhamtadfran: string | null;
  variabelregister_kalla: string | null;
  mattenhet: string | null;
  var_id: number;
  register_name: string | null;
}

export interface VarinfoAlternative {
  description: VarinfoDescription;
  instances: number;
}

export type ColumnVarinfoResponse =
  | { kind: "none" }
  | {
      kind: "single";
      primary: VarinfoDescription;
      primary_share: { instances: number; total: number };
    }
  | {
      kind: "divergent";
      primary: VarinfoDescription;
      primary_share: { instances: number; total: number };
      alternatives: VarinfoAlternative[];
    };

export interface GetColumnVarinfoArgs {
  register: string | null;
  column: string;
}

export function getColumnVarinfo(
  args: GetColumnVarinfoArgs,
): Promise<ColumnVarinfoResponse> {
  return send<ColumnVarinfoResponse>("/api/column-varinfo", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(args),
  });
}
