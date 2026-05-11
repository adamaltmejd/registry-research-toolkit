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

/** Year-variance tier surfaced inside the value-codes popup (issue #64).
 * 1 = uniform across years; 2 = code set widens/narrows but every code
 * has one label; 3a = same code has different labels across years
 * (municipal reorgs); 3b = column maps to different classifications
 * across years. Null only when kind === "none". */
export type VarianceTier = "1" | "2" | "3a" | "3b";

/** One distinct value-set group attached to a column×register (issue
 * #64 follow-up). Surfaced as a picker option on the values path so the
 * user can drill from "union of all years" into one year-correct group.
 *
 * cvids are intentionally not on the wire — a single popular column
 * (e.g. RTB×Kon) covers thousands of cvids per group, and "applies to"
 * is scoped to the project's sources via year intersection rather than
 * to the regmeta cvid set.
 */
export interface ValueSetGroup {
  value_set_id: number;
  /** Year window from register_version names; null when none of the
   * cvids' regver names parse to a year. */
  year_min: number | null;
  year_max: number | null;
}

export interface ColumnValuesResponse {
  /** "classification" — canonical SCB classification code list.
   *  "values" — per-instance value codes aggregated for this register.
   *  "none" — regmeta has no codes (unmatched, no register, etc.). */
  kind: "classification" | "values" | "none";
  title: string;
  description: string | null;
  codes: ColumnValueCode[];
  tier: VarianceTier | null;
  /** Human-readable variance note. Null for tier 1 / kind="none". */
  note: string | null;
  /** Distinct classification short_names attached to this column under
   * this register. Populated when > 1 across years (drives the picker). */
  classifications: string[];
  /** Which classification's codes are rendered. Only meaningful when
   * `kind === "classification"` and the picker is shown. */
  picked_classification: string | null;
  /** Distinct value-set groups attached to this column under this
   * register. Populated when > 1 across years (drives the tier 2 / 3a
   * picker). Ordered chronologically by year_min. */
  value_sets: ValueSetGroup[];
  /** value_set_id actually rendered. null = union (tier 2 default).
   * For tier 3a the server fills this with the most-common group. */
  picked_value_set: number | null;
}

export interface GetColumnValuesArgs {
  /** Register name or numeric id; null when the column's group has no
   *  register assigned (server returns kind="none"). */
  register: string | null;
  column: string;
  /** Opt into a non-default classification when the column maps to
   * multiple across years. Ignored when not a candidate. */
  picked_classification?: string | null;
  /** Opt into a specific value-set on the per-instance values path.
   * null = let the server pick the tier default (union for tier 2,
   * most-common for tier 3a). Ignored when not a candidate. */
  picked_value_set?: number | null;
  /** Project source years (deduped, non-null). Server drops value-set
   * groups that don't overlap any of these so the picker stops
   * surfacing year windows the project doesn't touch. Falls back to
   * "show all + note" when no group overlaps. Omit / empty = no
   * filter. */
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
