/**
 * Tiny, dependency-free typed fetch wrapper for the reg_webapp backend (see
 * reg_webapp/DESIGN.md → Pydantic boundary: the SPA only talks HTTP/JSON to the
 * backend — no domain coupling).
 *
 * Every response type is the codegen'd `components["schemas"][...]` from
 * `./api-types` (generated from the backend's committed `openapi.json`) or, for
 * the routes the Rust server answers, from `./api-types-rust` (generated from
 * `crates/reg-meta/openapi.json`), so the client carries the exact API contract
 * with no hand-maintained mirror.
 */
import type { components } from "./api-types";
import type {
  components as RustComponents,
  operations as RustOperations,
} from "./api-types-rust";
import { queryFromParams, type ResolutionParams } from "./period";

type Schemas = components["schemas"];
type RustSchemas = RustComponents["schemas"];

/** The `/api` base. Same-origin in production (Cloudflare fronts both the SPA
 * and the API); the Vite dev server proxies the routes ported to the Rust server
 * there and the rest of `/api` to the FastAPI backend (`vite.config.ts`). */
const API_BASE = "/api";

/**
 * A non-2xx response, thrown by the GET helpers. `status` is the HTTP status;
 * `body` is the parsed JSON error body when present (the Rust server's
 * `{error, meta}`, FastAPI's `{detail: ...}` from `HTTPException`, and the
 * validation issue shapes elsewhere) or `null` when the body wasn't JSON; `message` is a human-readable
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

/** Pull a human-readable message out of a parsed error body. The Rust server's
 * error document is `{error: {message, ...}, meta}`; FastAPI's 4xx
 * `HTTPException` serializes as `{detail: string}`, and `detail` can also be a
 * list of validation errors (422). Falls back to the status line. */
function messageFromBody(status: number, body: unknown): string {
  if (body && typeof body === "object" && "error" in body) {
    const error = (body as { error: unknown }).error;
    if (error && typeof error === "object" && "message" in error) {
      return String((error as { message: unknown }).message);
    }
  }
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
 * the download endpoint rejects, unlike `/validate`'s 200 diagnosis). Used for the
 * order manifest (`/project/order`).
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
 * (`attachment; filename="order.json"`) is handled — that's all the contract
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

// ── Catalog surface (the Rust server, RUST_RUNTIME_SPEC.md package C) ──────────
// Every catalog read answers `{data, meta}`; the helpers below return `data`.
// `show` summarizes any ref (no ref is the catalog root); the heavy parts are
// facets, each its own request: a variable's `states`, `warnings`, `lineage` and
// relationship `graph`, and the `values` of a classification or of one state. A
// retired FQID resolves to its terminal successor (the result carries the
// canonical `fqid`, no redirect); a ref that a split makes ambiguous is a 409
// `ambiguous_ref` whose message names the candidates.

/** The Rust server's `context`: branding, artifact identity and headline counts. */
export type Context = RustSchemas["Context"];
/** Deployment identity + branding (`id` / `name` / `long_name`) — the `steward`
 * block of `/api/context`, threaded into the Home landing page (#675). */
export type Steward = RustSchemas["Steward"];
/** The headline catalog-size counts (#675) the landing page renders — slugged
 * (browse-addressable) providers, registers and variables in the read scope. */
export type CatalogSizes = RustSchemas["Sizes"];

/** `show`'s `kind`-tagged summary of a ref. */
export type ShowNode = RustSchemas["Show"];
export type RootShow = Extract<ShowNode, { kind: "root" }>;
export type ProviderShow = Extract<ShowNode, { kind: "provider" }>;
export type RegisterShow = Extract<ShowNode, { kind: "register" }>;
/** A variable's shared metadata. Its states, warnings, lineage and succession
 * chain are the `states`, `warnings`, `lineage` and `graph` facets. */
export type VariableShow = Extract<ShowNode, { kind: "variable" }>;
export type ClassificationRootShow = Extract<
  ShowNode,
  { kind: "classification_root" }
>;
/** A classification edition. Its codes are `values`, its edition chain `graph`
 * (or its family's `editions`). */
export type ClassificationShow = Extract<ShowNode, { kind: "classification" }>;
/** A register's concept group as a browsable subject (#617): its members in scope,
 * each with its own coverage. */
export type ConceptGroupShow = Extract<ShowNode, { kind: "concept_group" }>;
/** A curated classification umbrella (#756): catalog-global, members without
 * coverage. */
export type ClassificationGroupShow = Extract<
  ShowNode,
  { kind: "classification_group" }
>;
/** A derived one-dimensional classification succession family (#771). */
export type ClassificationFamilyShow = Extract<
  ShowNode,
  { kind: "classification_family" }
>;
export type ClassificationSubjectShow =
  | ClassificationGroupShow
  | ClassificationFamilyShow;

export type RootChild = RustSchemas["RootChild"];
/** A provider's register, with its coverage in the read scope. */
export type RegisterChild = RustSchemas["RegisterChild"];
/** A register's variable, with every `(variant, column)` it is delivered under. */
export type VariableChild = RustSchemas["VariableChild"];
/** One `(variant, delivery column)` a register child is delivered under (Y-82),
 * with that pair's own windows. */
export type VariableDeliveryModel = RustSchemas["Delivery"];
export type CoverageModel = RustSchemas["Coverage"];
export type RegisterCoverageModel = RustSchemas["RegisterCoverage"];
/** A register variant with its versions' prose (the `?variant=` browse axis). */
export type VariantModel = RustSchemas["Variant"];
export type TagModel = RustSchemas["Tag"];
/** A concept or classification group as listed by its register, the
 * classification root or a member classification — a presentation fold of
 * near-identical rows whose members carry the real leaf FQIDs. */
export type ConceptGroup = RustSchemas["Group"];
/** A group member; two members of one variable differ by `delivery_column`.
 * `coverage` is present on a group's own page only. */
export type ConceptGroupMember = RustSchemas["Member"];
/** One facet assignment on a group member (`axis`/`value`/`label`). */
export type GroupFacetModel = RustSchemas["Facet"];
/** One declared facet axis of a group (#819): the stable `name` (equal to a
 * member's facet `axis`) and its display `label`. */
export type GroupAxisModel = RustSchemas["Axis"];
/** One edition of a classification succession family, in chain order, with
 * `is_self`/`is_current` flags. */
export type FamilyEdition = RustSchemas["FamilyEdition"];
export type OwningVariable = RustSchemas["OwningVariable"];
export type Derivation = RustSchemas["Derivation"];

/** One state of a variable (`states`), in the catalog page's light form:
 * `value_set` is null and `value_set_summary` carries the counts; the codes are
 * the `values` facet. `state_id` is a decimal-string storage id.
 *
 * `Required` because the server serializes every field of a state, an absent
 * value as `null`, while the OpenAPI document marks each nullable field optional;
 * the state readers compare bounds and columns against `null`. The same holds for
 * `GraphState`. */
export type VariableStateModel = Required<RustSchemas["State"]>;
export type ValueSetSummaryModel = RustSchemas["ValueSetSummary"];
export type DenseIntegerRangeModel = RustSchemas["IntegerRange"];
export type DataWarningModel = RustSchemas["DataWarning"];

/** One page of `values`, with `total` matches of `q` in the whole set. */
export type ValuesPage = RustSchemas["ValuesPage"];
export type ValueRow = RustSchemas["ValueRow"];
export type ValueSetMemberModel = RustSchemas["ValueSetMember"];
export type ClassificationExtensionMemberModel =
  RustSchemas["ClassificationExtensionMember"];
/** One code of a classification edition; `is_valid` is canonical (true) or
 * unknown (null: no canonical list for the edition). */
export type ClassificationCodeModel = RustSchemas["ClassificationCode"];

/** A variable's lineage: its state-lineage `edges`, lineage `warnings`, and the
 * same-named variables' `registers` provenance rows. */
export type LineageModel = RustSchemas["Lineage"];
export type LineageEdgeModel = RustSchemas["LineageEdge"];
export type LineageWarningModel = RustSchemas["LineageWarning"];

/** The catalog relationship-graph contract (#761): one node per variable (states
 * as sub-structure, grouped into representation-run cells) or classification
 * edition, plus `succession` edges. An empty graph (`nodes: []`) is the "don't
 * render" signal. `focus_id` is the requested node, null for a group ref. */
export type RelationshipGraph = Omit<RustSchemas["Graph"], "nodes"> & {
  nodes: GraphNode[];
};
export type VariableGraphNode = Omit<
  Extract<RustSchemas["Node"], { kind: "variable" }>,
  "states"
> & { states: GraphState[] };
/** A classification-edition node: a point at `version_year`; `is_current` marks
 * the head edition. */
export type ClassificationGraphNode = Extract<
  RustSchemas["Node"],
  { kind: "classification" }
>;
export type GraphNode = VariableGraphNode | ClassificationGraphNode;
export type GraphEdge = RustSchemas["Edge"];
export type GraphState = Required<RustSchemas["GraphState"]>;

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
export function getShow(ref?: string): Promise<ShowNode> {
  return rustGet<ShowNode>(ref ? `/catalog/${encodeFqid(ref)}` : "/catalog");
}

/** The page size the SPA walks `states` with: the operation's maximum.
 * simplify: sequential page walk; the largest history on the v0.43.0 pin
 * (`scb/rtb/kon`, 521 states) is 3 requests. Fetch pages concurrently, or let the
 * binding page render the first page while the rest load, if a variable passes
 * about 1,000 states (5 pages) or the leaf's load time becomes noticeable. */
const STATES_PAGE = 200;

/** A variable's states: its whole history, or with `period` (and the
 * `variant`/`value_set_version` modifiers) the resolved subset. Walks every page;
 * the server stops paging at depth 1000, so a longer history ends there. A
 * malformed modifier is the server's 422 (an `ApiError`). */
export async function getStates(
  ref: string,
  params: ResolutionParams = {},
): Promise<VariableStateModel[]> {
  const items: VariableStateModel[] = [];
  let cursor: string | null | undefined;
  do {
    const query = new URLSearchParams(queryFromParams(params));
    query.set("limit", String(STATES_PAGE));
    if (cursor) {
      query.set("cursor", cursor);
    }
    const page = await rustGet<{
      items: VariableStateModel[];
      next_cursor?: string | null;
    }>(`/states/${encodeFqid(ref)}`, query);
    items.push(...page.items);
    cursor = page.next_cursor;
  } while (cursor);
  return items;
}

/** A register's or variable's data warnings. `unassigned_only` keeps a
 * register's warnings that name no variable. */
export function getWarnings(
  ref: string,
  params: {
    unassigned_only?: boolean;
    period?: string | null;
    variant?: string | null;
    representation?: string | null;
  } = {},
): Promise<DataWarningModel[]> {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value) query.set(key, String(value));
  }
  return rustGet<DataWarningModel[]>(`/warnings/${encodeFqid(ref)}`, query);
}

/** One page of `values`: a classification's codes (`ref` a classification), or
 * the value set of one `state` of the variable `ref`. With `classification`,
 * `partition` picks that declared book's part of the state's codes; `column` with
 * `alias_window_from` selects a coded alias window instead. `q` filters the whole
 * set before the page and `total` counts the matches; `cursor` continues. */
export function getValues(
  ref: string,
  params: {
    state?: string | null;
    partition?: "canonical" | "source_extensions" | "nonstandard" | "sentinels";
    classification?: string;
    column?: string;
    alias_window_from?: string;
    q?: string;
    cursor?: string | null;
    limit?: number;
  } = {},
  options?: { signal?: AbortSignal },
): Promise<ValuesPage> {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value != null && value !== "") query.set(key, String(value));
  }
  return rustGet<ValuesPage>(`/values/${encodeFqid(ref)}`, query, options);
}

/** The relationship graph of a variable, classification, concept group or
 * classification group ref. */
export function getGraph(ref: string): Promise<RelationshipGraph> {
  return rustGet<RelationshipGraph>(`/graph/${encodeFqid(ref)}`);
}

/** A variable's lineage: edges, lineage warnings and per-register provenance. */
export function getLineage(ref: string): Promise<LineageModel> {
  return rustGet<LineageModel>(`/lineage/${encodeFqid(ref)}`);
}

// ── Project write surface (A5.2b-ii) ────────────────────────────────────────
// The POST endpoints the authoring SPA drives (see reg_webapp/DESIGN.md →
// Project-write surface (routes/project.py)). Each takes the WHOLE serialized
// draft as a raw object. The OpenAPI request schema documents the canonical
// CLOSED ProjectData contract, while this transport type stays deliberately raw:
// malformed uploads (including unknown root keys) must reach the backend
// unchanged so `/validate` can diagnose them.

export type ValidationResultModel = Schemas["ValidationResultModel"];

/** A serialized project_data.json draft posted to the write endpoints. This is a
 * raw diagnostic transport shape, not an extension surface: unknown keys are
 * invalid but must survive until the backend reports them. */
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

/** One blocking reason an order could not be materialized — reg_meta's own
 * `OrderFinding`, straight off the 422 body: the stable `code`, the message, and
 * the optional `source` / `variable` / `period` coordinates that say WHERE. */
export type OrderFinding = Schemas["OrderFinding"];

/** The `/project/order` 422 body: the flattened `detail` line plus the findings
 * as DATA (`OrderBlockedModel`). */
export type OrderBlocked = Schemas["OrderBlockedModel"];

/** POST a draft to `/api/project/order` and download the materialized JSON order
 * manifest (`OrderManifest`, served verbatim so the SPA and the `reg-meta order`
 * CLI hand the steward byte-identical files). Anything that is NOT an order — an
 * invalid spec, or an order the materializer fail-closed on — is the backend's
 * 422 (an `ApiError` whose `body` is `OrderBlocked`), never a partial download. */
export function downloadOrderManifest(draft: ProjectDataBody): Promise<void> {
  return apiPostForBlob("/project/order", draft, "order.json");
}

/** The typed findings carried by a caught `/project/order` failure, or `[]` for
 * anything else (a network error, the gate's finding-less 422, a non-JSON body).
 *
 * The narrowing is structural on purpose: this reads an `unknown` catch value at
 * the HTTP boundary, so it trusts only the shape it verifies — a malformed entry
 * drops rather than reaching the renderer with `undefined` fields.
 */
export function orderFindingsFromError(e: unknown): OrderFinding[] {
  if (!(e instanceof ApiError)) {
    return [];
  }
  const body = e.body;
  if (!body || typeof body !== "object" || !("findings" in body)) {
    return [];
  }
  const findings = (body as { findings: unknown }).findings;
  if (!Array.isArray(findings)) {
    return [];
  }
  return findings.filter(
    (f): f is OrderFinding =>
      f != null &&
      typeof f === "object" &&
      typeof (f as OrderFinding).code === "string" &&
      typeof (f as OrderFinding).message === "string",
  );
}

// ── Search surface (#379) ───────────────────────────────────────────────────
// The Rust server's `search` (`GET /api/search?q=`, decision 17): one ranked list
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
/** A folded classification-succession row (#571): a query hit ≥2 editions of one
 * chain, collapsed onto the TERMINAL (current) edition. `editions` is the full
 * chain (terminal-first, descending year); the terminal `fqid` is the navigable
 * target (NOT a concept group). */
export type ClassificationSuccessionHit = Extract<
  SearchHit,
  { type: "classification_succession" }
>;
export type ConceptGroupHit = Extract<SearchHit, { type: "group" }>;
export type CodeHit = Extract<SearchHit, { type: "code" }>;
/** The arm a search keeps (`?type=`); omitted, the search ranks every arm. */
export type SearchType = NonNullable<
  RustOperations["search"]["parameters"]["query"]["type"]
>;

/** The omnibox's client-side timeout. The codes/value sub-query can be slow
 * server-side (a separate backend index fix is in flight); past this the SPA
 * aborts the request and shows a friendly "timed out" message rather than an
 * infinite spinner. A timeout-abort throws a `TimeoutError` (distinct `name` from
 * the supersede-abort's `AbortError`), which SearchView maps to the timeout copy. */
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
 * the least useful. The single source of truth for this threshold — SearchView
 * (the results page) gates its fetch on it. */
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

// ── Docs surface (#354/#394/#402/#742) ─────────────────────────────────────
// The Rust server's docs operations, each `{data, meta}`: `docs_search`
// (`GET /api/docs/search?q=&register=`, the binding-leaf "mentioned in
// documentation" hook #402), `docs_get` (`GET /api/docs/doc/{identifier}`: one
// doc's metadata, source pointer and a BOUNDED `excerpt`; the SPA never renders
// its `body`) and `docs_related` (`GET /api/docs/related/{register FQID}`, with
// the bytes at `/api/docs/file/{register FQID}/{filename}`, #742). A deployment
// without a docs database answers `docs_unavailable` (404): the search and
// related helpers return `null` for it, so the panels degrade silently, and the
// doc viewer shows the error.
// Snippets/excerpts are EXCERPTS, rendered as TEXT only (Svelte auto-escapes
// `{value}`) — never `{@html}` (they carry `**` highlight markers; the full
// converted FTS document lives at the SCB source, not here).

export type DocPage = RustSchemas["DocPage"];
export type DocResult = RustSchemas["DocResult"];
export type DocDetail = RustSchemas["DocDetail"];
export type RelatedDocument = RustSchemas["RelatedDocument"];

/** A docs read's `data`, or `null` when the deployment ships no docs database. */
async function docsData<T>(request: Promise<{ data: T }>): Promise<T | null> {
  try {
    return (await request).data;
  } catch (e) {
    if (
      e instanceof ApiError &&
      (e.body as { error?: { code?: unknown } } | null)?.error?.code ===
        "docs_unavailable"
    ) {
      return null;
    }
    throw e;
  }
}

/** Resolve one doc by its `identifier` (a variable name or a filename — a single
 * path segment, so `encodeURIComponent` the whole thing). A 404 (`not_found` or
 * `docs_unavailable`) surfaces as an `ApiError` whose message says which. */
export async function getDoc(identifier: string): Promise<DocDetail> {
  return (
    await apiGet<{ data: DocDetail }>(
      `/docs/doc/${encodeURIComponent(identifier)}`,
    )
  ).data;
}

/** The binding-leaf "mentioned in documentation" hook (#402): text matches for
 * `q` in the docs of the `register` FQID (e.g. `"scb/lisa"`). Shares the search
 * query + abort/timeout plumbing (a ~12s client `TimeoutError` layered with the
 * caller's teardown `signal`); `limit` caps the results. `null` when the
 * deployment has no docs database; `register_ingested:false` when the register has
 * no docs — the panel distinguishes both from "no mentions found". */
export function getDocsForVariable(
  q: string,
  options: { register: string; limit?: number; signal?: AbortSignal },
): Promise<DocPage | null> {
  return docsData(searchGet<{ data: DocPage }>("/docs/search", q, options));
}

/** The rehosted register-version PDFs of a register FQID; `null` when the
 * deployment has no docs database. */
export function getRelatedDocuments(
  register: string,
  options?: { signal?: AbortSignal },
): Promise<RelatedDocument[] | null> {
  return docsData(
    apiGet<{ data: RelatedDocument[] }>(
      `/docs/related/${encodeFqid(register)}`,
      { signal: options?.signal },
    ),
  );
}

/** Same-origin PDF URL for one related document of a register FQID. This is a
 * browser `href`, not a JSON fetch helper, so it includes the `/api` base. */
export function relatedDocumentFileHref(
  register: string,
  filename: string,
): string {
  return `${API_BASE}/docs/file/${encodeFqid(register)}/${encodeURIComponent(filename)}`;
}
