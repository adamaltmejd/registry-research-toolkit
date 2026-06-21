/**
 * Tiny, dependency-free typed fetch wrapper for the reg_webapp backend (see
 * reg_webapp/DESIGN.md → Pydantic boundary: the SPA only talks HTTP/JSON to the
 * backend — no domain coupling).
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
 * `{detail: ...}` on 4xx from FastAPI's `HTTPException`, and the validation
 * issue shapes elsewhere) or `null` when the body wasn't JSON; `message` is a human-readable
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
 * callers building catalog paths encode each segment). An optional `signal`
 * cancels the in-flight request (the omnibox passes the asyncResource teardown
 * signal so a superseded query aborts server-side); a `fetch` abort throws an
 * `AbortError`/`TimeoutError`, NOT an `ApiError` — callers map it as they see fit.
 */
export async function apiGet<T>(
  path: string,
  options?: { signal?: AbortSignal },
): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    signal: options?.signal,
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
 * (A5.3a). Each arm carries a `kind` literal. `ConceptGroupNode` (#617) is NOT
 * an arm: the group SUBJECT is served ONLY by the FIXED `/catalog/group/...`
 * route (its own response type, `ConceptGroupNodeData`), never by the catch-all,
 * so this union advertises exactly the kinds the catch-all can return. */
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
 * variable's full embedded longitudinal record (states + edges; see
 * reg_webapp/DESIGN.md → Catalog router structure). */
export type BindingNodeData = Schemas["BindingNode"];
export type VariableStateModel = Schemas["VariableStateModel"];
export type VariableRefModel = Schemas["VariableRefModel"];
/** One edition in a variable's embedded FULL succession timeline (#582) — the
 * variable-grain dual of `ClassificationChainEdition`. The chain arrives
 * oldest-first, terminal last, with `is_self`/`is_current` flags and per-edition
 * `reason`/`effective_year`. A chain edition CAN be a dead/renamed predecessor
 * (#355/#411): a valid `fqid` (301-redirects to the current edition) but a null
 * `name` (no live row). `fqid` is null only on a malformed triple. The browse
 * panel renders the whole chain synchronously from these. */
export type VariableEditionModel = Schemas["VariableEditionModel"];
export type RelatedRefModel = Schemas["RelatedRefModel"];
export type LineageEdgeModel = Schemas["LineageEdgeModel"];
export type LineageWarningModel = Schemas["LineageWarningModel"];

/** A derived concept group (#303) on a register / classification-root node —
 * a PRESENTATION-ONLY fold of near-identical rows. Members carry the real leaf
 * FQIDs (the same entries also appear in `children`); the browse collapses the
 * member rows under the group and expands to a facet picker. */
export type ConceptGroup = Schemas["ConceptGroupModel"];
export type ConceptGroupMember = Schemas["ConceptGroupMemberModel"];

/** The concept group as a browsable SUBJECT (#617), returned by the fixed
 * `/catalog/group/<provider>/<register>/<key>` route — group identity +
 * members WITH per-member coverage + the echoed `?member=` focus hint. This is
 * the group route's OWN response type, NOT an arm of the catch-all `CatalogNode`
 * union. Distinct from the presentation-only `ConceptGroup` folded into a
 * register listing. */
export type ConceptGroupNodeData = Schemas["ConceptGroupNode"];
/** A member on the group SUBJECT node — the browse member plus its per-variable
 * study-window `coverage` (null for a stateless member). */
export type ConceptGroupNodeMember = Schemas["ConceptGroupNodeMember"];
/** The concept group a binding belongs to (#616), as `(provider, register,
 * key)` — carried on `BindingNode.group` (null when ungrouped) so a member page
 * links to its group subject without a second fetch. */
export type BindingGroupRef = Schemas["BindingGroupRefModel"];

// The catch-all returns a `StatesResponse` (NOT a `kind`-tagged node) when a
// binding leaf is queried with `?period` (the resolve_at subset), and a
// SUB-ENDPOINT path returns other no-`kind` envelopes — both are distinguished
// from a browsable node by `isCatalogNode` at the fetch boundary.
export type StatesResponse = Schemas["StatesResponse"];
export type DimensionsResponse = Schemas["DimensionsResponse"];
export type LineageWarningsResponse = Schemas["LineageWarningsResponse"];

/** One edition in a classification's embedded succession timeline (#571) — the
 * chain arrives oldest-first, terminal last, with `is_self`/`is_current` flags.
 * Every edition is a live classification row (the build validator guarantees
 * succession editions are live), so `fqid` is null only when the slug is
 * missing/unresolvable, in which case it renders as plain text. The browse panel
 * renders the whole chain synchronously from these. */
export type ClassificationChainEdition = Schemas["ClassificationChainEdition"];
/** One code/label entry in a classification edition's value set (#609), embedded
 * on `ClassificationNodeData.codes`. `is_valid` is canonical (true) / observed-only
 * (false) / unknown (null — no canonical CSV for the edition). */
export type ClassificationCodeModel = Schemas["ClassificationCodeModel"];
/** The resolved classification leaf the catch-all returns — carries its embedded
 * FULL succession chain (`edition_chain`, oldest first / terminal last), the
 * resolved edition's value-set `codes` (#609), and the curated umbrella
 * `dimensions` it belongs to (#609 niva ↔ aggregate cross-reference). */
export type ClassificationNodeData = Schemas["ClassificationNode"];

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
 * re-validates the slug grammar per segment — see reg_webapp/DESIGN.md → FQID
 * path guard (catalog_fqid.py)). Split/join on `/` so the
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
 * (`?period` + the `?variant`/`?value_set_version` modifiers) a binding
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

/** List a register's variants (the `?variant=` browse axis). `register`
 * is the 2-seg register FQID `provider/register`. */
export function getRegisterVariants(
  register: string,
): Promise<VariantsResponse> {
  return apiGet<VariantsResponse>(`/catalog/${encodeFqid(register)}/variants`);
}

/** Resolve a concept group SUBJECT by `(provider, register, key)` (#617) — the
 * group with all members + per-member coverage. Each path segment is
 * percent-encoded (the server re-validates provider/register as slugs; the key
 * is a derivation key). `member` is the optional focus hint (a member leaf slug
 * to highlight), echoed back on the node only when it names a real member. A
 * 404 (unknown key / register) surfaces as an `ApiError`. */
export function getConceptGroup(
  provider: string,
  register: string,
  key: string,
  member?: string,
): Promise<ConceptGroupNodeData> {
  const path = `/catalog/group/${encodeFqid(`${provider}/${register}/${key}`)}`;
  const query = member ? `?member=${encodeURIComponent(member)}` : "";
  return apiGet<ConceptGroupNodeData>(`${path}${query}`);
}

// ── Binding sub-endpoints ───────────────────────────────────────────────────
// The leaf now EMBEDS states / same_as / succession_chain (the FULL chain, #582) /
// related_to / lineage, so the SPA fetches only the one it does NOT embed:
// `/lineage_warnings`. It GETs `/catalog/{encodeFqid}/lineage_warnings`; the suffix
// is greedy-matched ABOVE the catch-all server-side. (The `/predecessors` route
// still exists server-side — the #411 inbound-succession surface — but the SPA no
// longer fetches it: the embedded `succession_chain` already carries predecessors.)

export function getBindingLineageWarnings(
  fqidPath: string,
): Promise<LineageWarningsResponse> {
  return apiGet<LineageWarningsResponse>(
    `/catalog/${encodeFqid(fqidPath)}/lineage_warnings`,
  );
}

/** The concept-group dimension memberships for this binding's variable (#489) —
 * the "pick your variant" facet groups (level / population / rank / …) that
 * contain it. Empty `dimensions` when the variable is in no group. */
export function getBindingDimensions(
  fqidPath: string,
): Promise<DimensionsResponse> {
  return apiGet<DimensionsResponse>(
    `/catalog/${encodeFqid(fqidPath)}/dimensions`,
  );
}

// ── Project write surface (A5.2b-ii) ────────────────────────────────────────
// The three POST endpoints the authoring SPA drives (see reg_webapp/DESIGN.md →
// Project-write surface (routes/project.py + routes/bundle.py)). Each takes the WHOLE
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
 * banner, distinct from the issue list). No client-side structural
 * validator — the backend is canonical (see reg_webapp/DESIGN.md → Pydantic
 * boundary); the SPA mirrors codes for presentation.
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

// ── Search surface (#379) ───────────────────────────────────────────────────
// `GET /api/search?q=` returns four ORDERED, typed result groups over the shipped
// FTS indexes (registers / variables / classifications / codes). Each group's
// `total_count` is the full match count BEFORE the per-request `limit`, so the SPA
// renders "showing N of M". A folded concept-group row (`type:"group"`) is NOT
// itself FQID-addressable — its `members` carry the real leaf FQIDs. A `fqid` can
// be `null` on any leaf (a hit with no resolvable catalog node).

export type SearchResponse = Schemas["SearchResponse"];
export type RegisterSearchGroup = Schemas["RegisterSearchGroup"];
export type VariableSearchGroup = Schemas["VariableSearchGroup"];
export type ClassificationSearchGroup = Schemas["ClassificationSearchGroup"];
export type CodeSearchGroup = Schemas["CodeSearchGroup"];
export type RegisterSearchResult = Schemas["RegisterSearchResult"];
export type VariableSearchResult = Schemas["VariableSearchResult"];
export type ClassificationSearchResult = Schemas["ClassificationSearchResult"];
/** A folded classification-succession row (#571): a query hit ≥2 editions of one
 * chain, collapsed onto the TERMINAL (current) edition. `editions` is the full
 * chain (terminal-first, descending year); the terminal `fqid` is the navigable
 * target (NOT a concept group). */
export type ClassificationSuccessionSearchResult =
  Schemas["ClassificationSuccessionSearchResult"];
export type ClassificationEditionModel = Schemas["ClassificationEditionModel"];
export type ConceptGroupSearchResult = Schemas["ConceptGroupSearchResult"];
export type CodeSearchResult = Schemas["CodeSearchResult"];
export type CodeOwnerVariable = Schemas["CodeOwnerVariable"];
export type CodeOwnerClassification = Schemas["CodeOwnerClassification"];

/** The omnibox's client-side timeout. The codes/value sub-query can be slow
 * server-side (a separate backend index fix is in flight); past this the SPA
 * aborts the request and shows a friendly "timed out" message rather than an
 * infinite spinner. A timeout-abort throws a `TimeoutError` (distinct `name` from
 * the supersede-abort's `AbortError`), which SearchView maps to the timeout copy. */
const SEARCH_TIMEOUT_MS = 12_000;

/** The scoped-search toggle values (#393 item 1). `all` (the default) returns the
 * four typed groups; any single value scopes the search to that one group. Mirrors
 * reg_meta's `SEARCH_TYPES` (the backend 422s an unknown value). */
export type SearchType =
  | "all"
  | "register"
  | "variable"
  | "classification"
  | "value";

/** GET a search endpoint (`path` relative to `/api`) with the shared query +
 * abort plumbing every search surface uses: `q` is encoded, an explicit `limit`
 * appended (server default otherwise), an explicit non-`all` `type` appended
 * (#393 item 1 — `all` is the server default, so it's OMITTED to keep the URL +
 * ETag stable), an optional `register` filter appended (the for-variable hook
 * scopes by register; `search`/`docSearch` pass none), and the request aborts on
 * EITHER the caller's `signal` (a supersede/unmount teardown, which stays silent)
 * OR a ~12s timeout (surfaced as a `TimeoutError`) — `AbortSignal.any` fires on
 * whichever wins. */
function searchGet<T>(
  path: string,
  q: string,
  options?: {
    signal?: AbortSignal;
    limit?: number;
    type?: SearchType;
    register?: string;
  },
): Promise<T> {
  const params = new URLSearchParams({ q });
  if (options?.limit !== undefined) {
    params.set("limit", String(options.limit));
  }
  if (options?.type !== undefined && options.type !== "all") {
    params.set("type", options.type);
  }
  if (options?.register !== undefined) {
    params.set("register", options.register);
  }
  const signals = [AbortSignal.timeout(SEARCH_TIMEOUT_MS)];
  if (options?.signal) {
    signals.push(options.signal);
  }
  return apiGet<T>(`${path}?${params}`, {
    signal: AbortSignal.any(signals),
  });
}

/** Search the catalog. `q` is the raw user query (encoded); `limit` is the
 * per-group result cap — omit it to use the server default (20, clamped ≤50);
 * `type` scopes the search to one group (#393 item 1) — omit it (or pass `all`)
 * for the four-group default. A blank/punctuation-only query returns the selected
 * group(s) empty (`total_count: 0`), not an error. */
export function search(
  q: string,
  options?: { signal?: AbortSignal; limit?: number; type?: SearchType },
): Promise<SearchResponse> {
  return searchGet<SearchResponse>("/search", q, options);
}

// ── Docs surface (#354/#394/#402) ───────────────────────────────────────────
// `GET /api/docs/search?q=` returns a single documentation result group over the
// docs FTS index; `GET /api/docs/doc/{identifier}` returns one doc's metadata +
// source pointer + a BOUNDED excerpt (never the full body); `GET
// /api/docs/for-variable?q=&register=` is the binding-leaf "mentioned in
// documentation" hook (#402) — FUZZY name/provider_key text matches for one
// variable, scoped to its register. When the deployment ships no docs index,
// search returns `ingested:false` with empty results (NOT a 500), and the doc
// endpoint returns 404 — the SPA degrades silently on search (failure isolation
// in SearchView + DocMentionsPanel) and shows a clear note on the doc viewer.
// Snippets/excerpts are EXCERPTS, rendered as TEXT only (Svelte auto-escapes
// `{value}`) — never `{@html}` (they may carry FTS highlight markers; the full
// document lives at the SCB source, not here).

export type DocSearchResponse = Schemas["DocSearchResponse"];
export type DocResult = Schemas["DocResult"];
export type DocDetail = Schemas["DocDetail"];
export type DocVariableMentions = Schemas["DocVariableMentions"];

/** Search documentation. Shares `search`'s query + abort/timeout plumbing (a ~12s
 * client `TimeoutError` layered with the caller's teardown `signal`); `limit` is
 * the per-request result cap (server default otherwise). An absent docs index
 * returns `ingested:false`, not an error. */
export function docSearch(
  q: string,
  options?: { signal?: AbortSignal; limit?: number },
): Promise<DocSearchResponse> {
  return searchGet<DocSearchResponse>("/docs/search", q, options);
}

/** Resolve one doc by its `identifier` (a filename — a single path segment, so
 * `encodeURIComponent` the whole thing). 404 when the index is absent OR the doc
 * isn't found (the `detail` distinguishes them — surfaced via the resource error). */
export function getDoc(identifier: string): Promise<DocDetail> {
  return apiGet<DocDetail>(`/docs/doc/${encodeURIComponent(identifier)}`);
}

/** The binding-leaf "mentioned in documentation" hook (#402): FUZZY
 * name/provider_key text matches for `q`, scoped to the bare `register` slug
 * (e.g. `"lisa"`, matched verbatim). Shares the search query + abort/timeout
 * plumbing (a ~12s client `TimeoutError` layered with the caller's teardown
 * `signal`); `limit` caps the results. An absent docs index returns
 * `ingested:false`, and a register with no ingested docs `register_ingested:false`
 * — neither is an error; the panel distinguishes both from "no mentions found"
 * (every `DocResult` is `fuzzy:true` — a heuristic match, NOT an authoritative
 * variable→doc link). */
export function getDocsForVariable(
  q: string,
  options?: { register?: string; limit?: number; signal?: AbortSignal },
): Promise<DocVariableMentions> {
  return searchGet<DocVariableMentions>("/docs/for-variable", q, options);
}
