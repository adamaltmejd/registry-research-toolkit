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

/** Normalize a caught `unknown` into a banner-ready message: an `Error`'s
 * `message` (covers `ApiError`, whose `message` is already human-readable), else
 * `String(e)`. Shared by the catch arms in the store + App shell. */
export function errMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
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

/**
 * POST `body` as JSON to `path` (relative to `/api`) and return the parsed JSON
 * typed as `T`. Throws `ApiError` on any NON-2xx response (parsing a JSON error
 * body when present). Used for `/project/validate`, where a non-2xx is a malformed
 * REQUEST (a 4xx from `read_raw_json_object` / the body cap) — NOT a validation
 * failure: a validation failure is a 200 with `ok:false`, which this RETURNS
 * (see `validateProject`).
 */
export async function apiPostJson<T>(path: string, body: unknown): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    let errBody: unknown = null;
    try {
      errBody = await resp.json();
    } catch {
      // Non-JSON error body — leave null.
    }
    throw new ApiError(
      resp.status,
      errBody,
      messageFromBody(resp.status, errBody),
    );
  }
  return (await resp.json()) as T;
}

/**
 * POST `body` as JSON to `path` and trigger a browser file download of the 2xx
 * response blob. The filename is taken from the response's `Content-Disposition`
 * (`attachment; filename="..."`), falling back to `fallbackFilename`. A non-2xx is
 * an `ApiError` (the backend's 400/422 — a malformed request or an invalid spec
 * the download endpoints reject, unlike `/validate`'s 200 diagnosis). Used for the
 * CSV order export + the MONA bundle (`/project/order`, `/bundle`).
 *
 * The download is wired with a transient `<a download>` + `createObjectURL`,
 * revoked after the click — the standard no-dep blob-download pattern.
 */
export async function apiPostForBlob(
  path: string,
  body: unknown,
  fallbackFilename: string,
): Promise<void> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    let errBody: unknown = null;
    try {
      errBody = await resp.json();
    } catch {
      // Non-JSON error body — leave null.
    }
    throw new ApiError(
      resp.status,
      errBody,
      messageFromBody(resp.status, errBody),
    );
  }
  const blob = await resp.blob();
  const filename =
    filenameFromContentDisposition(resp.headers.get("content-disposition")) ??
    fallbackFilename;
  triggerDownload(blob, filename);
}

/** Parse the `filename="..."` out of a `Content-Disposition` header, or `null`
 * when absent/unparseable. Only the simple quoted form the backend emits
 * (`attachment; filename="order.csv"`) is handled — that's all the contract
 * produces. */
function filenameFromContentDisposition(header: string | null): string | null {
  if (!header) {
    return null;
  }
  const match = /filename="?([^"]+)"?/.exec(header);
  return match ? match[1] : null;
}

/** Save `blob` to the user's filesystem under `filename` via a transient
 * `<a download>` + an object URL (revoked after the click). Shared by
 * `apiPostForBlob` (server blobs) and the store's local project_data.json
 * download. */
export function triggerDownload(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
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

/** A binding child under a register node — a thin (fqid, name) entry, NOT the
 * embedded longitudinal record (that lives on the binding LEAF, `BindingNode`). */
export type BindingChild = Schemas["BindingChild"];

/** The binding-leaf node (3-seg) the catch-all returns WITHOUT a query — the
 * variable's full embedded longitudinal record (states + edges, §9.5). */
export type BindingNodeData = Schemas["BindingNode"];
export type VariableStateModel = Schemas["VariableStateModel"];
export type VariableRefModel = Schemas["VariableRefModel"];
export type RelatedRefModel = Schemas["RelatedRefModel"];
export type LineageEdgeModel = Schemas["LineageEdgeModel"];
export type LineageWarningModel = Schemas["LineageWarningModel"];

// The catch-all returns a `StatesResponse` (NOT a `kind`-tagged node) when a
// binding leaf is queried with `?period` (the resolve_at subset, §9.5), and a
// SUB-ENDPOINT path returns other no-`kind` envelopes — both are distinguished
// from a browsable node by `isCatalogNode` at the fetch boundary.
export type StatesResponse = Schemas["StatesResponse"];
export type PredecessorsResponse = Schemas["PredecessorsResponse"];
export type LineageWarningsResponse = Schemas["LineageWarningsResponse"];

/** A browsable catalog node — every `CatalogNode` arm carries a `kind` literal;
 * the catch-all's other payloads (a `?period` `StatesResponse`, or a SUB-ENDPOINT
 * `VariantsResponse`/… on a `.../states` path) do NOT. Positive `"kind" in x`
 * check (negate it for "the non-node response"). Phrased as `isCatalogNode`
 * rather than `isStatesResponse` because a no-`kind` payload is NOT necessarily a
 * `StatesResponse` — only the binding-leaf `?period` resolve is. */
export function isCatalogNode(
  x: CatalogNode | StatesResponse,
): x is CatalogNode {
  return "kind" in x;
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
 * distinguished from a browsable node by `isCatalogNode`. A malformed
 * period/variant is the server's 422 (surfaced as an `ApiError`). */
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
// The leaf already EMBEDS states / same_as / replaced_by (outbound) /
// related_to / lineage, so A5.3b fetches only the two it does NOT embed:
// `/predecessors` (inbound succession) and `/lineage_warnings`. Each GETs
// `/catalog/{encodeFqid}/{suffix}`; the suffix is greedy-matched ABOVE the
// catch-all server-side. (The other four suffixed endpoints exist server-side
// but aren't fetched by the SPA — A5.3c adds helpers if/when it needs them.)

export function getBindingPredecessors(
  fqidPath: string,
): Promise<PredecessorsResponse> {
  return apiGet<PredecessorsResponse>(
    `/catalog/${encodeFqid(fqidPath)}/predecessors`,
  );
}

export function getBindingLineageWarnings(
  fqidPath: string,
): Promise<LineageWarningsResponse> {
  return apiGet<LineageWarningsResponse>(
    `/catalog/${encodeFqid(fqidPath)}/lineage_warnings`,
  );
}

// ── Project write surface (§9.5, A5.2b-ii) ──────────────────────────────────
// The three POST endpoints the authoring SPA drives. Each takes the WHOLE
// serialized draft as an open object (the requestBodies are
// `additionalProperties: true`), so steward-namespaced blocks ride along — the
// backend embeds the raw dict (routes/project.py, routes/bundle.py).

export type ValidationResultModel = Schemas["ValidationResultModel"];

/** A serialized project_data.json draft posted to the write endpoints — an open
 * object (the backend reads it raw, preserving namespaced blocks). */
export type ProjectDataBody = Record<string, unknown>;

/**
 * POST a draft to `/api/project/validate` and RETURN the 200
 * `ValidationResultModel` (`{ok, issues}`). A validation FAILURE is a 200 with
 * `ok:false` — this NEVER throws on `ok:false`; the caller renders the issues.
 * Only a true 4xx (a malformed REQUEST — bad JSON / oversized body, from the
 * backend's `read_raw_json_object` / body cap) throws an `ApiError` (shown as a
 * banner, distinct from the issue list). §9.6: no client-side structural
 * validator — the backend is canonical; the SPA mirrors codes for presentation.
 */
export function validateProject(
  draft: ProjectDataBody,
): Promise<ValidationResultModel> {
  return apiPostJson<ValidationResultModel>("/project/validate", draft);
}

/** POST a draft to `/api/project/order` and download the rendered order-export
 * CSV. A structurally invalid spec is the backend's 422 (an `ApiError`) — unlike
 * `/validate`, the order endpoint cannot render from an invalid spec. */
export function downloadOrderCsv(draft: ProjectDataBody): Promise<void> {
  return apiPostForBlob("/project/order", draft, "order.csv");
}

/** POST a draft to `/api/bundle` and download the single-file MONA `.py` bundle.
 * A build-gate failure (bad input) is the backend's 422 (an `ApiError`). */
export function downloadBundle(draft: ProjectDataBody): Promise<void> {
  return apiPostForBlob("/bundle", draft, "mona_bundle.py");
}
