/**
 * Tiny, dependency-free typed fetch wrapper for the reg_webapp backend (§9.6:
 * the SPA only talks HTTP/JSON to the backend — no domain coupling).
 *
 * Every response type is the codegen'd `components["schemas"][...]` from
 * `./api-types` (generated from the backend's committed `openapi.json`), so the
 * client carries the exact API contract with no hand-maintained mirror.
 */
import type { components } from "./api-types";
import { queryFromParams, type ResolutionParams } from "./period";

type Schemas = components["schemas"];

/** The `/api` base. Same-origin in production (Cloudflare fronts both the SPA
 * and the API); the Vite dev server proxies `/api` to the backend on :8000. */
const API_BASE = "/api";

/**
 * A non-2xx response, thrown by the GET helpers. `status` is the HTTP status;
 * `body` is the parsed JSON error body when present (the backend returns
 * `{detail: ...}` on 4xx from FastAPI's `HTTPException`, and the §6.8.0 shapes
 * elsewhere) or `null` when the body wasn't JSON; `message` is a human-readable
 * summary suitable for an error banner.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(status: number, body: unknown, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

/** Pull a human-readable message out of a parsed error body. FastAPI's 4xx
 * `HTTPException` serializes as `{detail: string}`; `detail` can also be a list
 * of validation errors (422). Falls back to the status line. */
function messageFromBody(status: number, body: unknown): string {
  if (body && typeof body === "object" && "detail" in body) {
    const detail = (body as { detail: unknown }).detail;
    if (typeof detail === "string") {
      return detail;
    }
    if (Array.isArray(detail)) {
      // FastAPI 422 validation-error list: surface the first message.
      const first = detail[0];
      if (first && typeof first === "object" && "msg" in first) {
        return String((first as { msg: unknown }).msg);
      }
    }
  }
  return `Request failed (HTTP ${status})`;
}

/**
 * GET `path` (relative to `/api`) and return the parsed JSON typed as `T`.
 * Throws `ApiError` on any non-2xx response, parsing a JSON error body when it
 * can. `path` must already be URL-safe (segments are slug-validated server-side;
 * callers building catalog paths encode each segment).
 */
export async function apiGet<T>(path: string): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    headers: { Accept: "application/json" },
  });
  if (!resp.ok) {
    let body: unknown = null;
    try {
      body = await resp.json();
    } catch {
      // Non-JSON error body (e.g. a proxy 502) — leave `body` null.
    }
    throw new ApiError(resp.status, body, messageFromBody(resp.status, body));
  }
  return (await resp.json()) as T;
}

// ── Typed endpoint helpers ─────────────────────────────────────────────────
// Thin wrappers naming each browse endpoint's response type. The catch-all
// (`getCatalogNode`) returns the discriminated `CatalogNode` union; A5.3a never
// passes `?period`, so the `StatesResponse` arm (no `kind`) is out of scope here
// — callers narrow on `kind` (see `lib/catalog.ts`).

export type Context = Schemas["ContextResponse"];
export type RootResponse = Schemas["RootResponse"];
export type VariantsResponse = Schemas["VariantsResponse"];

/** The discriminated browse union the catch-all returns WITHOUT `?period`
 * (A5.3a). Each arm carries a `kind` literal. */
export type CatalogNode =
  | Schemas["ProviderResponse"]
  | Schemas["RegisterResponse"]
  | Schemas["BindingNode"]
  | Schemas["ClassificationRootResponse"]
  | Schemas["ClassificationNode"];

/** The binding-leaf node (3-seg) the catch-all returns WITHOUT a query — the
 * variable's full embedded longitudinal record (states + edges, §9.5). */
export type BindingNodeData = Schemas["BindingNode"];
export type VariableStateModel = Schemas["VariableStateModel"];
export type VariableRefModel = Schemas["VariableRefModel"];
export type RelatedRefModel = Schemas["RelatedRefModel"];
export type LineageEdgeModel = Schemas["LineageEdgeModel"];
export type LineageWarningModel = Schemas["LineageWarningModel"];

// The catch-all returns a `StatesResponse` (NOT a `kind`-tagged node) when a
// binding leaf is queried with `?period` (the resolve_at subset, §9.5). A5.3b
// narrows on `isStatesResponse` at the fetch boundary.
export type StatesResponse = Schemas["StatesResponse"];
export type PredecessorsResponse = Schemas["PredecessorsResponse"];
export type SuccessorsResponse = Schemas["SuccessorsResponse"];
export type RelatedResponse = Schemas["RelatedResponse"];
export type LineageResponse = Schemas["LineageResponse"];
export type LineageWarningsResponse = Schemas["LineageWarningsResponse"];

/** Narrow the catch-all's `CatalogNode | StatesResponse` result. A
 * `StatesResponse` has NO `kind` discriminator (it carries `binding` + `states`);
 * every `CatalogNode` arm carries a `kind`. Checked structurally (absence of
 * `kind`) so a binding-leaf `?period` resolve is distinguished from the full
 * `BindingNode`. */
export function isStatesResponse(
  x: CatalogNode | StatesResponse,
): x is StatesResponse {
  return !("kind" in x);
}

/** Percent-encode each FQID segment for use in a URL path (the server
 * re-validates the slug grammar per segment, §16). Split/join on `/` so the
 * path separators survive while reserved chars inside a segment are escaped.
 * Shared with `catalog.catalogHref` so the SPA's link hrefs and the API paths
 * encode identically. (A no-op for today's ASCII slugs, but correct for any
 * input.) */
export function encodeFqid(fqidPath: string): string {
  return fqidPath.split("/").map(encodeURIComponent).join("/");
}

export function getContext(): Promise<Context> {
  return apiGet<Context>("/context");
}

export function getCatalogRoot(): Promise<RootResponse> {
  return apiGet<RootResponse>("/catalog");
}

/** Resolve a catalog node by its FQID path (e.g. `scb/lisa/kon`). With `params`
 * (`?period` + the `?variant`/`?value_set_version` modifiers, §9.5) a binding
 * leaf resolves to the `resolve_at` subset — a `StatesResponse` (no `kind`),
 * narrowed by `isStatesResponse`. A malformed period/variant is the server's
 * 422 (surfaced as an `ApiError`). */
export function getCatalogNode(
  fqidPath: string,
  params?: ResolutionParams,
): Promise<CatalogNode | StatesResponse> {
  const query = params ? queryFromParams(params) : "";
  const path = `/catalog/${encodeFqid(fqidPath)}${query ? `?${query}` : ""}`;
  return apiGet<CatalogNode | StatesResponse>(path);
}

/** List a register's variants (the `?variant=` browse axis, §9.5). `register`
 * is the 2-seg register FQID `provider/register`. */
export function getRegisterVariants(
  register: string,
): Promise<VariantsResponse> {
  return apiGet<VariantsResponse>(`/catalog/${encodeFqid(register)}/variants`);
}

// ── Binding sub-endpoints (§9.5) ────────────────────────────────────────────
// The leaf already EMBEDS same_as / replaced_by (outbound) / related_to /
// lineage, so A5.3b fetches only the two NOT embedded: `/predecessors` (inbound
// succession) and `/lineage_warnings`. The other four helpers are provided for
// completeness/symmetry (and `/states` round-trips the embedded states subset).
// Each GETs `/catalog/{encodeFqid}/{suffix}`; the suffix is greedy-matched ABOVE
// the catch-all server-side.

export function getBindingStates(fqidPath: string): Promise<StatesResponse> {
  return apiGet<StatesResponse>(`/catalog/${encodeFqid(fqidPath)}/states`);
}

export function getBindingPredecessors(
  fqidPath: string,
): Promise<PredecessorsResponse> {
  return apiGet<PredecessorsResponse>(
    `/catalog/${encodeFqid(fqidPath)}/predecessors`,
  );
}

export function getBindingSuccessors(
  fqidPath: string,
): Promise<SuccessorsResponse> {
  return apiGet<SuccessorsResponse>(
    `/catalog/${encodeFqid(fqidPath)}/successors`,
  );
}

export function getBindingRelated(fqidPath: string): Promise<RelatedResponse> {
  return apiGet<RelatedResponse>(`/catalog/${encodeFqid(fqidPath)}/related`);
}

export function getBindingLineage(fqidPath: string): Promise<LineageResponse> {
  return apiGet<LineageResponse>(`/catalog/${encodeFqid(fqidPath)}/lineage`);
}

export function getBindingLineageWarnings(
  fqidPath: string,
): Promise<LineageWarningsResponse> {
  return apiGet<LineageWarningsResponse>(
    `/catalog/${encodeFqid(fqidPath)}/lineage_warnings`,
  );
}
