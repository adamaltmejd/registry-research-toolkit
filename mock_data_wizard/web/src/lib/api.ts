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
  source: string;
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
