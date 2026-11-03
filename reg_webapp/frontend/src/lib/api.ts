/**
 * Tiny, dependency-free typed fetch wrapper for the Rust server (`reg-meta
 * serve`): the SPA only talks HTTP/JSON to it — no domain coupling.
 *
 * Every response type is the codegen'd `components["schemas"][...]` from
 * `./api-types-rust` (generated from `crates/reg-meta/openapi.json`), so the
 * client carries the exact API contract with no hand-maintained mirror.
 */
import type {
  components as RustComponents,
  operations as RustOperations,
} from "./api-types-rust";

type RustSchemas = RustComponents["schemas"];

/** The `/api` base. Same-origin in production (Cloudflare fronts both the SPA
 * and the API); the Vite dev server proxies it to the Rust server
 * (`vite.config.ts`). */
const API_BASE = "/api";

/**
 * A non-2xx response, thrown by the GET helpers. `status` is the HTTP status;
 * `body` is the parsed JSON error body when present (the Rust server's
 * `{error, meta}`) or `null` when the body wasn't JSON; `message` is a
 * human-readable summary suitable for an error banner.
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

/** Pull a human-readable message out of a parsed error body: the Rust server's
 * error document is `{error: {message, ...}, meta}`. Falls back to the status
 * line. */
function messageFromBody(status: number, body: unknown): string {
  if (body && typeof body === "object" && "error" in body) {
    const error = (body as { error: unknown }).error;
    if (error && typeof error === "object" && "message" in error) {
      return String((error as { message: unknown }).message);
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
 * body when present). Used for `/project/validate`, where a non-2xx is a refused
 * REQUEST (`malformed_request`, `payload_too_large`, `rate_limited`) — NOT a
 * validation failure: a validation failure is a 200 with `ok:false`, which this
 * RETURNS (see `validateProject`).
 */
export async function apiPostJson<T>(
  path: string,
  body: unknown,
  options?: { signal?: AbortSignal },
): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    signal: options?.signal,
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

/** Save `blob` to the user's filesystem under `filename` via a transient
 * `<a download>` + an object URL (revoked after the click). */
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

// ── Catalog surface (the Rust server) ──────────────────────────────────────────
// Every catalog read answers `{data, meta}`; the helpers below return `data`.
// `show` summarizes any ref (no ref is the catalog root). Entity facets return
// with their routed pages in W2–W5.

/** The Rust server's `context`: artifact identity and catalog counts. */
export type Context = RustSchemas["Context"];

/** The headline catalog-size counts (#675) the landing page renders — slugged
 * (browse-addressable) providers, registers and variables in the read scope. */
export type CatalogSizes = RustSchemas["Sizes"];

/** `show`'s `kind`-tagged summary of a ref. */
export type ShowNode = RustSchemas["Show"];

export type RootShow = Extract<ShowNode, { kind: "root" }>;

export type ProviderShow = Extract<ShowNode, { kind: "provider" }>;

export type RegisterShow = Extract<ShowNode, { kind: "register" }>;

/** A variable's shared metadata. */
export type VariableShow = Extract<ShowNode, { kind: "variable" }>;

/** Percent-encode each FQID segment for use in a URL path. Split/join on `/` so
 * the path separators survive while reserved chars inside a segment are escaped.
 * Shared with `catalog.catalogHref` so the SPA's link hrefs and the API paths
 * encode identically. */
export function encodeFqid(fqidPath: string): string {
  return fqidPath.split("/").map(encodeURIComponent).join("/");
}

/** The concept-group ref, `group/<provider>/<register>/<key>`. */
export function conceptGroupRef(
  provider: string,
  register: string,
  key: string,
): string {
  return `group/${provider}/${register}/${key}`;
}

/** The classification-group (or family) ref, `group/class/<key>`. */
export function classificationGroupRef(key: string): string {
  return `group/class/${key}`;
}

/** The concept-group subject route, shared by SPA links and API fetches. */
export function conceptGroupPath(
  provider: string,
  register: string,
  key: string,
): string {
  return `/catalog/${encodeFqid(conceptGroupRef(provider, register, key))}`;
}

/** The classification-group subject route (#756), shared by SPA links and API
 * fetches — the classification sibling of `conceptGroupPath`. */
export function classificationGroupPath(key: string): string {
  return `/catalog/${encodeFqid(classificationGroupRef(key))}`;
}

/** GET a Rust-server read and return its `data`. */
async function rustGet<T>(
  path: string,
  query?: URLSearchParams,
  options?: { signal?: AbortSignal },
): Promise<T> {
  const suffix = query && query.size > 0 ? `?${query}` : "";
  return (await apiGet<{ data: T }>(`${path}${suffix}`, options)).data;
}

/** The Rust server answers with `{data, meta}`; the SPA reads `data`. */
export function getContext(): Promise<Context> {
  return rustGet<Context>("/context");
}

/** `show` for `ref` (a FQID or group ref); no ref is the catalog root. */
export function getShow(
  ref?: string,
  options?: { signal?: AbortSignal },
): Promise<ShowNode> {
  return rustGet<ShowNode>(
    ref ? `/catalog/${encodeFqid(ref)}` : "/catalog",
    undefined,
    options,
  );
}

// ── Project surface (the Rust server) ──────────────────────────────────────────
// `validate` and `order` take the WHOLE serialized draft as the POST body, a raw
// object: malformed uploads (including unknown root keys) must reach the server
// unchanged so `validate` can diagnose them.

/** `validate`'s result: `ok` when no issue is an error, and every issue in
 * emission order. */
export type ValidationResultModel = RustSchemas["Validation"];

export type LogicalOrder = RustSchemas["LogicalOrder"];

/** The logical order, checked against the loaded catalog. */
export async function getProjectOrder(
  draft: RustSchemas["ProjectData"],
  options?: { signal?: AbortSignal },
): Promise<LogicalOrder> {
  return (
    await apiPostJson<{ data: LogicalOrder }>("/project/order", draft, options)
  ).data;
}

/** Read only the value-set identity and names; its item page is not displayed. */
export async function getValueSet(
  hash: string,
  options?: { signal?: AbortSignal },
): Promise<RustSchemas["ValueSetRef"]> {
  return (
    await rustGet<RustSchemas["ValueItemsPage"]>(
      "/values",
      new URLSearchParams({ value_set: hash, limit: "1" }),
      options,
    )
  ).value_set;
}

/**
 * POST a draft to `/api/project/validate` and RETURN its `data`
 * (`ValidationResultModel`, `{ok, issues}`). A validation FAILURE is a 200 with
 * `ok:false` — this NEVER throws on `ok:false`; the caller renders the issues.
 * Only a refused REQUEST (`malformed_request`, `payload_too_large`,
 * `rate_limited`) throws an `ApiError` (shown as a banner, distinct from the
 * issue list). The store POSTs only drafts reg-core's structural door accepted in
 * the browser (`lib/reg_core.ts`); the semantic layer is the server's.
 */
export async function validateProject(
  draft: Record<string, unknown>,
  options?: { signal?: AbortSignal },
): Promise<ValidationResultModel> {
  return (
    await apiPostJson<{ data: ValidationResultModel }>(
      "/project/validate",
      draft,
      options,
    )
  ).data;
}

// ── Search surface (#379) ───────────────────────────────────────────────────
// The Rust server's `search` (`GET /api/search?q=`): one ranked list
// per call, `{items, next_cursor}` inside `{data, meta}`. With `type` the list is
// one arm's hits (its pins first); without it, every arm's hits ranked together
// (grouped variable members hidden). A concept-group hit (`type:"group"`) is not
// an FQID, but it can be linked to its fixed group route when the scope is
// derivable from members; its `members` carry the real leaf FQIDs for fallback
// links. A `fqid` can be `null` on any leaf (a hit with no resolvable catalog
// node).

export type SearchPage = RustSchemas["SearchPage"];

export type SearchHit = RustSchemas["SearchHit"];

export type RegisterHit = Extract<SearchHit, { type: "register" }>;

export type VariableHit = Extract<SearchHit, { type: "variable" }>;

export type ClassificationHit = Extract<SearchHit, { type: "classification" }>;

export type CodeHit = Extract<SearchHit, { type: "code" }>;

/** The arm a search keeps (`?type=`); omitted, the search ranks every arm. */
export type SearchType = NonNullable<
  RustOperations["search"]["parameters"]["query"]["type"]
>;

/** The search client-side timeout. The codes/value sub-query can be slow
 * server-side; past this the SPA aborts the request and the command palette
 * reports a failed search rather than an endless "Searching…". A timeout-abort
 * throws a `TimeoutError` (distinct `name` from the supersede-abort's
 * `AbortError`). */
const SEARCH_TIMEOUT_MS = 12_000;

/** GET a search endpoint (`path` relative to `/api`) with the shared query +
 * abort plumbing every search surface uses: `q` is encoded, an explicit `limit`
 * appended (server default otherwise), an optional `type` and `register` filter
 * appended (the for-variable hook scopes by register; `search` passes none), and
 * the request aborts on EITHER the caller's `signal` (a supersede/unmount
 * teardown, which stays silent) OR a ~12s timeout (surfaced as a `TimeoutError`) — `AbortSignal.any` fires on
 * whichever wins. */
function searchGet<T>(
  path: string,
  q: string,
  options?: {
    signal?: AbortSignal;
    limit?: number;
    type?: SearchType;
    register?: string;
    cursor?: string;
  },
): Promise<T> {
  const params = new URLSearchParams({ q });
  if (options?.limit !== undefined) {
    params.set("limit", String(options.limit));
  }
  if (options?.type !== undefined) {
    params.set("type", options.type);
  }
  if (options?.register !== undefined) {
    params.set("register", options.register);
  }
  if (options?.cursor !== undefined) {
    params.set("cursor", options.cursor);
  }
  const signals = [AbortSignal.timeout(SEARCH_TIMEOUT_MS)];
  if (options?.signal) {
    signals.push(options.signal);
  }
  return apiGet<T>(`${path}?${params}`, {
    signal: AbortSignal.any(signals),
  });
}

/** The minimum query length that's worth a GET /api/search round-trip: a single
 * char is the most expensive query server-side (the codes/value sub-query) and
 * the least useful. The single source of truth for this threshold — the
 * command palette gates its fetch on it. */
export const SEARCH_MIN_QUERY_LENGTH = 2;

/** Search the catalog: one page of the ranked list. `q` is the raw user query
 * (encoded); `limit` is the page size (the server's default is 50); `type` keeps
 * one arm (omit it for the ranked list across arms); `cursor` is a previous page's
 * `next_cursor`. A query with no letter or digit returns no items, not an error. */
export async function search(
  q: string,
  options?: {
    signal?: AbortSignal;
    limit?: number;
    type?: SearchType;
    cursor?: string;
  },
): Promise<SearchPage> {
  return (await searchGet<{ data: SearchPage }>("/search", q, options)).data;
}

export type RegisterFacet = RustSchemas["RegisterFacet"];

/** Per register, how many of its variables match `q` (#1785): every match, no
 * paging, before concept groups fold. A register without a match is absent.
 * With `register`, only that register, and its entry lists every matched
 * variable (#1788); without it, `limit` lists each register's first `limit`
 * and names the register (#1828). */
export async function searchFacets(
  q: string,
  options?: { signal?: AbortSignal; register?: string; limit?: number },
): Promise<RegisterFacet[]> {
  return (
    await searchGet<{ data: RustSchemas["RegisterFacets"] }>(
      "/search/facets",
      q,
      options,
    )
  ).data.registers;
}

/** Provider and register entries of the current catalog summary. */
export type RootChild = RootShow["providers"][number];
export type RegisterChild = ProviderShow["registers"][number];

export type TableShow = Extract<ShowNode, { kind: "table" }>;
export type CatalogPeriod = RustSchemas["Period"];
export type CatalogFinding = RustSchemas["Finding"];
export type CatalogSegment = RustSchemas["Segment"];
export type VariableEvidence = RustSchemas["VariableEvidence"];
export type Graph = RustSchemas["Graph"];

export async function getGraph(
  ref: string,
  options: { kind: "continues"; signal?: AbortSignal },
): Promise<Graph> {
  // StrictMode tears down its first effect immediately; do not send that read.
  await Promise.resolve();
  options.signal?.throwIfAborted();
  return rustGet<Graph>(
    `/graph/${encodeFqid(ref)}`,
    new URLSearchParams({ kind: options.kind }),
    options,
  );
}

/** All structural segments, following the server's generation-bound cursor. */
export async function getSegments(
  ref: string,
  options?: { signal?: AbortSignal },
): Promise<CatalogSegment[]> {
  const items: CatalogSegment[] = [];
  let cursor: string | null = null;
  do {
    const query = new URLSearchParams({ limit: "200" });
    if (cursor !== null) query.set("cursor", cursor);
    const page = await rustGet<RustSchemas["SegmentPage"]>(
      `/segments/${encodeFqid(ref)}`,
      query,
      options,
    );
    items.push(...page.items);
    cursor = page.next_cursor;
  } while (cursor !== null);
  return items;
}

/** Requested only when the variable's provenance disclosure is first opened. */
export async function getVariableEvidence(
  ref: string,
  options?: { signal?: AbortSignal },
): Promise<VariableEvidence> {
  const node = await rustGet<ShowNode>(
    `/catalog/${encodeFqid(ref)}`,
    new URLSearchParams({ evidence: "true" }),
    options,
  );
  if (node.kind !== "variable" || !node.evidence) {
    throw new Error("The catalog returned no variable evidence.");
  }
  return node.evidence;
}

/** Review items and notices for a catalog subject, including its descendants. */
export function getFindings(
  ref: string,
  options?: { signal?: AbortSignal },
): Promise<CatalogFinding[]> {
  return rustGet<CatalogFinding[]>(
    `/findings/${encodeFqid(ref)}`,
    undefined,
    options,
  );
}

export type ValueSetRef = RustSchemas["ValueSetRef"];
export type ValueItem = RustSchemas["ValueItem"];
export type ValueItemsPage = RustSchemas["ValueItemsPage"];
export type ValuesSubject = { value_set: string } | { classification: string };

/** One page of a set or classification version; lookup by code is a separate surface. */
export function getValues(
  subject: ValuesSubject,
  filters: {
    parent?: string;
    level?: number;
    q?: string;
    cursor?: string;
    limit?: number;
  } = {},
  options?: { signal?: AbortSignal },
): Promise<ValueItemsPage> {
  const query = new URLSearchParams(subject);
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined) query.set(key, String(value));
  }
  return rustGet<ValueItemsPage>("/values", query, options);
}

// ── Linked source documents ──────────────────────────────────────────────
export type RelatedDocument = RustSchemas["RelatedDocument"];
export type DocDetail = RustSchemas["DocDetail"];

export async function getRelatedDocuments(
  ref: string,
  options?: { signal?: AbortSignal },
): Promise<RelatedDocument[]> {
  try {
    return await rustGet<RelatedDocument[]>(
      `/docs/related/${encodeFqid(ref)}`,
      undefined,
      options,
    );
  } catch (error) {
    if (error instanceof ApiError) {
      const body = error.body as { error?: { code?: unknown } } | null;
      if (body?.error?.code === "docs_unavailable") return [];
    }
    throw error;
  }
}

export function getDoc(
  doc: string,
  options?: { signal?: AbortSignal },
): Promise<DocDetail> {
  return rustGet<DocDetail>(
    `/docs/doc/${encodeURIComponent(doc)}`,
    undefined,
    options,
  );
}

export function docFileHref(doc: string): string {
  return `${API_BASE}/docs/file/${encodeURIComponent(doc)}`;
}
