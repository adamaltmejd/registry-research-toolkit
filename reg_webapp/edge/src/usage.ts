// Usage statistics (#1323, reg_webapp/DESIGN.md → "Usage statistics"): one Workers
// Analytics Engine data point per /api and /mcp request, from the origin's
// `x-reg-usage` label, which this module strips before the client sees it. No IP
// address, user id, cookie or raw User-Agent is written: the client is a family name
// and the place a country code.

// The slice of Cloudflare's AnalyticsEngineDataset binding used here.
export interface UsageDataset {
  writeDataPoint(point: { indexes?: string[]; blobs?: string[]; doubles?: number[] }): void;
}

const LABEL_HEADER = "x-reg-usage";
// An SPA pick beacon carries the FQIDs one add picked: the grammar's characters,
// bounded, at most MAX_EVENT_PICKS (under the 250 data points a worker may write).
const FQID = /^[a-z0-9][a-z0-9\-_/]{0,199}$/;
const MAX_EVENT_PICKS = 100;
const MAX_EVENT_BYTES = 24 * 1024;
// The origin's scrub for ids (crates/reg-meta/src/usage.rs `scrub`): a run of six or
// more digits, `-` or `+` allowed between, is a person number or the like. FQID admits
// only ASCII digits, so this is the same rule.
const DIGIT_RUN = /[0-9](?:[-+]?[0-9]){5,}/g;

// User-Agent substrings to a client family, first match wins. Agents before
// browsers: an agent's UA can name a browser engine too.
const CLIENT_FAMILIES: [string, string][] = [
  ["reg-meta-ci-smoke", "ci"],
  ["claude", "claude"],
  ["cursor", "cursor"],
  ["codex", "codex"],
  ["vscode", "vscode"],
  ["curl/", "curl"],
  ["python", "python"],
  ["node", "node"],
  ["edg/", "edge"],
  ["firefox/", "firefox"],
  ["chrome/", "chrome"],
  ["safari/", "safari"],
];

// Cache statuses whose response came from the origin on this request, so its
// Server-Timing is this request's (on a HIT it is the request that filled the cache).
const FROM_ORIGIN = new Set(["", "MISS", "EXPIRED", "BYPASS", "DYNAMIC"]);

interface Label {
  op?: string;
  fqids?: string[];
  q?: string;
  n?: number;
  err?: string;
}

export function clientFamily(userAgent: string | null): string {
  const ua = (userAgent ?? "").toLowerCase();
  return CLIENT_FAMILIES.find(([needle]) => ua.includes(needle))?.[1] ?? "other";
}

function surface(request: Request, url: URL): string {
  if (url.pathname === "/mcp") return "mcp";
  return request.headers.get("sec-fetch-site") === "same-origin" ? "spa" : "rest";
}

function country(request: Request): string {
  const cf = (request as { cf?: { country?: unknown } }).cf;
  return typeof cf?.country === "string" ? cf.country : "";
}

function parseLabel(value: string | null): Label | null {
  if (!value) return null;
  try {
    const label: unknown = JSON.parse(value);
    return label && typeof label === "object" ? (label as Label) : null;
  } catch {
    return null;
  }
}

function appMs(timing: string | null): number {
  const dur = /(?:^|,)\s*app;dur=([0-9.]+)/.exec(timing ?? "")?.[1];
  return dur === undefined ? -1 : Number(dur);
}

// One data point. Blobs: steward, surface, kind, op, fqid, q, err, cache status,
// client family, country, HTTP status. Doubles: edge ms, app ms (-1 when not from
// origin), response bytes, hits (-1 when not a search). Index: steward:op.
function write(
  dataset: UsageDataset,
  steward: string,
  blobs: {
    surface: string;
    kind: string;
    op: string;
    fqid?: string;
    q?: string;
    err?: string;
  },
  request: Request,
  response: { cache: string; status: number },
  doubles: [number, number, number, number],
): void {
  // The index is at most 96 bytes: `op` can be a client's JSON-RPC method, so it is
  // made ASCII first. A failed write never decides the response.
  const index = `${steward}:${blobs.op}`.replace(/[^\x20-\x7e]/g, "?").slice(0, 96);
  try {
    dataset.writeDataPoint({
      indexes: [index],
      blobs: [
        steward,
        blobs.surface,
        blobs.kind,
        blobs.op,
        blobs.fqid ?? "",
        blobs.q ?? "",
        blobs.err ?? "",
        response.cache,
        clientFamily(request.headers.get("user-agent")),
        country(request),
        String(response.status),
      ],
      doubles,
    });
  } catch {
    // Recording is best-effort.
  }
}

// Record one forwarded request and return its response without the label. `started`
// is Date.now() before the origin fetch.
export function recordRequest(
  dataset: UsageDataset | undefined,
  steward: string,
  request: Request,
  url: URL,
  origin: Response,
  started: number,
): Response {
  const label = parseLabel(origin.headers.get(LABEL_HEADER));
  const response = new Response(origin.body, origin);
  response.headers.delete(LABEL_HEADER);
  if (!dataset) return response;
  const cache = origin.headers.get("cf-cache-status") ?? "";
  // A 304 answered before the route runs carries no label: a revalidation, counted
  // without attributing it.
  const op = label?.op ?? (origin.status === 304 ? "revalidated" : "unlabelled");
  const where = surface(request, url);
  const status = { cache, status: origin.status };
  write(
    dataset,
    steward,
    {
      surface: where,
      kind: "req",
      op,
      fqid: label?.fqids?.join(" "),
      q: label?.q,
      err: label?.err,
    },
    request,
    status,
    [
      Date.now() - started,
      FROM_ORIGIN.has(cache) ? appMs(origin.headers.get("server-timing")) : -1,
      // -1 when the size is unknown (a chunked or compressed answer).
      Number(origin.headers.get("content-length") ?? -1),
      typeof label?.n === "number" ? label.n : -1,
    ],
  );
  // An MCP order's bound variables are an agent's picks (the origin lists them only
  // over MCP); at most 100, under the 250 data points a worker may write.
  if (where === "mcp" && op === "order") {
    for (const fqid of (label?.fqids ?? []).slice(0, 100)) {
      write(
        dataset,
        steward,
        { surface: where, kind: "pick", op, fqid },
        request,
        status,
        [0, -1, -1, -1],
      );
    }
  }
  return response;
}

// The SPA's pick beacon, `POST /api/event` with `{"fqids": ["...", ...]}`, one per add:
// answered here, never forwarded or cached. The origin's per-client limit never sees
// it; the site's Cloudflare rule on `/api/` counts it, one request per add, beside the
// reads. Only the SPA's own page may send it (`Sec-Fetch-Site: same-origin`), with a
// declared length, so another site cannot inflate picks and no body is read unbounded.
export async function recordPick(
  dataset: UsageDataset | undefined,
  steward: string,
  request: Request,
): Promise<Response> {
  const answer = (status: number) =>
    new Response(null, { status, headers: { "cache-control": "no-store" } });
  if (request.method !== "POST") return answer(405);
  if (request.headers.get("sec-fetch-site") !== "same-origin") return answer(403);
  const length = request.headers.get("content-length");
  if (length === null) return answer(411);
  if (!(Number(length) <= MAX_EVENT_BYTES)) return answer(413);
  const text = await request.text();
  if (text.length > MAX_EVENT_BYTES) return answer(413);
  let fqids: unknown;
  try {
    const event: unknown = JSON.parse(text);
    fqids = event && typeof event === "object" ? (event as { fqids?: unknown }).fqids : undefined;
  } catch {
    return answer(400);
  }
  if (
    !Array.isArray(fqids) ||
    fqids.length === 0 ||
    fqids.length > MAX_EVENT_PICKS ||
    !fqids.every((fqid) => typeof fqid === "string" && FQID.test(fqid))
  )
    return answer(400);
  if (dataset) {
    for (const fqid of fqids as string[]) {
      write(
        dataset,
        steward,
        { surface: "spa", kind: "pick", op: "event", fqid: fqid.replace(DIGIT_RUN, "<redacted>") },
        request,
        { cache: "", status: 204 },
        [0, -1, -1, -1],
      );
    }
  }
  return answer(204);
}
