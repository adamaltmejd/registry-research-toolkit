/**
 * Tiny, dependency-free typed fetch wrapper for the reg_webapp backend (§9.6:
 * the SPA only talks HTTP/JSON to the backend — no domain coupling).
 *
 * Every response type is the codegen'd `components["schemas"][...]` from
 * `./api-types` (generated from the backend's committed `openapi.json`), so the
 * client carries the exact API contract with no hand-maintained mirror.
 */
import type { components } from "./api-types";

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

/** Resolve a catalog node by its FQID path (e.g. `scb/lisa/kon`). */
export function getCatalogNode(fqidPath: string): Promise<CatalogNode> {
  return apiGet<CatalogNode>(`/catalog/${encodeFqid(fqidPath)}`);
}

/** List a register's variants (the `?variant=` browse axis, §9.5). `register`
 * is the 2-seg register FQID `provider/register`. */
export function getRegisterVariants(
  register: string,
): Promise<VariantsResponse> {
  return apiGet<VariantsResponse>(`/catalog/${encodeFqid(register)}/variants`);
}
