import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  apiGet,
  type BindingNodeData,
  docSearch,
  getBindingLineageWarnings,
  getBindingPredecessors,
  getCatalogNode,
  getClassificationPredecessors,
  getDoc,
  getDocsForVariable,
  isCatalogNode,
  type StatesResponse,
  search,
  validateProject,
} from "./api";

// Stub the global fetch per test. jsdom provides `Response`, but we hand-build
// the minimal shape `apiGet` reads (ok / status / json) so the tests don't
// depend on a real network or a full Response.
function stubFetch(impl: (url: string) => Promise<unknown>): void {
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string) => impl(url)),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("apiGet", () => {
  it("returns the parsed JSON on a 2xx response", async () => {
    stubFetch(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ kind: "root", children: [] }),
    }));
    const body = await apiGet<{ kind: string }>("/catalog");
    expect(body.kind).toBe("root");
  });

  it("throws ApiError carrying status + parsed {detail} body on a 404", async () => {
    stubFetch(async () => ({
      ok: false,
      status: 404,
      json: async () => ({ detail: "fqid not found" }),
    }));
    await expect(apiGet("/catalog/scb/nope")).rejects.toMatchObject({
      name: "ApiError",
      status: 404,
      message: "fqid not found",
    });
  });

  it("surfaces the first msg of a FastAPI 422 validation-error list", async () => {
    stubFetch(async () => ({
      ok: false,
      status: 422,
      json: async () => ({
        detail: [{ loc: ["query", "period"], msg: "bad period" }],
      }),
    }));
    const err = await apiGet("/catalog/scb/lisa/kon").catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(422);
    expect((err as ApiError).message).toBe("bad period");
  });

  it("falls back to a status-line message when the error body is not JSON", async () => {
    stubFetch(async () => ({
      ok: false,
      status: 502,
      json: async () => {
        throw new Error("not json");
      },
    }));
    const err = (await apiGet("/context").catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(502);
    expect(err.body).toBeNull();
    expect(err.message).toContain("502");
  });
});

describe("getCatalogNode", () => {
  // Capture the requested URL for a single stubbed call.
  async function urlFor(
    fqidPath: string,
    params?: Parameters<typeof getCatalogNode>[1],
  ): Promise<string> {
    let seen = "";
    stubFetch(async (url) => {
      seen = url;
      return { ok: true, status: 200, json: async () => ({ kind: "binding" }) };
    });
    await getCatalogNode(fqidPath, params);
    return seen;
  }

  it("URL-encodes each FQID segment but keeps the slash separators", async () => {
    // Segments encoded individually; the path separators survive.
    expect(await urlFor("scb/lisa/kön")).toBe("/api/catalog/scb/lisa/k%C3%B6n");
  });

  it("omits the query entirely when no params are passed", async () => {
    expect(await urlFor("scb/lisa/kon")).toBe("/api/catalog/scb/lisa/kon");
  });

  it("appends a single value per param (period, variant, value_set_version)", async () => {
    const url = await urlFor("scb/lisa/kon", {
      period: "2020",
      variant: "x",
      value_set_version: "y",
    });
    expect(url).toBe(
      "/api/catalog/scb/lisa/kon?period=2020&variant=x&value_set_version=y",
    );
  });

  it("omits undefined/empty params but keeps the present ones", async () => {
    const url = await urlFor("scb/lisa/kon", {
      period: "2018..2020",
      variant: undefined,
      value_set_version: "",
    });
    // The `..` range and reserved chars are percent-encoded by URLSearchParams.
    expect(url).toBe("/api/catalog/scb/lisa/kon?period=2018..2020");
  });

  it("percent-encodes a param value with reserved characters", async () => {
    const url = await urlFor("scb/lisa/kon", { period: "2020 Q3&x" });
    expect(url).toContain("?period=2020+Q3%26x");
  });
});

describe("binding sub-endpoint helpers", () => {
  // Each GETs `/catalog/{encodeFqid}/{suffix}` with no query.
  const cases: [(fqidPath: string) => Promise<unknown>, string, string][] = [
    [
      getBindingPredecessors,
      "predecessors",
      "/api/catalog/scb/lisa/kon/predecessors",
    ],
    [
      getBindingLineageWarnings,
      "lineage_warnings",
      "/api/catalog/scb/lisa/kon/lineage_warnings",
    ],
  ];

  for (const [fn, name, expected] of cases) {
    it(`GETs the ${name} sub-endpoint URL`, async () => {
      let seen = "";
      stubFetch(async (url) => {
        seen = url;
        return { ok: true, status: 200, json: async () => ({}) };
      });
      await fn("scb/lisa/kon");
      expect(seen).toBe(expected);
    });
  }

  it("encodes the FQID segments in a sub-endpoint URL", async () => {
    let seen = "";
    stubFetch(async (url) => {
      seen = url;
      return { ok: true, status: 200, json: async () => ({}) };
    });
    await getBindingPredecessors("scb/lisa/kön");
    expect(seen).toBe("/api/catalog/scb/lisa/k%C3%B6n/predecessors");
  });
});

describe("classification succession sub-endpoint helper (#571)", () => {
  // GETs `/catalog/{encodeFqid}/classification_predecessors` on a 2-seg
  // classification FQID. (Only predecessors is fetched — the outbound arm rides
  // on the embedded `replaced_by`, so there's no successors helper.)
  it("GETs the classification_predecessors sub-endpoint URL", async () => {
    let seen = "";
    stubFetch(async (url) => {
      seen = url;
      return { ok: true, status: 200, json: async () => ({}) };
    });
    await getClassificationPredecessors("class/sun2020");
    expect(seen).toBe("/api/catalog/class/sun2020/classification_predecessors");
  });
});

describe("validateProject", () => {
  it("RETURNS the 200 ok:false body WITHOUT throwing (a validation failure is not a 4xx)", async () => {
    const body = {
      ok: false,
      issues: [
        {
          level: "error",
          code: "unexpected_field",
          path: "/sources/0/bindings/0/typ",
          message: "unexpected key 'typ' on binding",
        },
      ],
    };
    stubFetch(async () => ({ ok: true, status: 200, json: async () => body }));
    const result = await validateProject({ schema_version: "2.0.0" });
    expect(result.ok).toBe(false);
    expect(result.issues).toHaveLength(1);
    expect(result.issues[0].code).toBe("unexpected_field");
  });

  it("RETURNS the 200 ok:true body for a clean spec", async () => {
    stubFetch(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ ok: true, issues: [] }),
    }));
    const result = await validateProject({ schema_version: "2.0.0" });
    expect(result.ok).toBe(true);
    expect(result.issues).toEqual([]);
  });

  it("POSTs the whole draft as JSON to /api/project/validate", async () => {
    let seenUrl = "";
    let seenInit: RequestInit | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((url: string, init?: RequestInit) => {
        seenUrl = url;
        seenInit = init;
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({ ok: true, issues: [] }),
        });
      }),
    );
    const draft = { schema_version: "2.0.0", reg_monabundle: { x: 1 } };
    await validateProject(draft);
    expect(seenUrl).toBe("/api/project/validate");
    expect(seenInit?.method).toBe("POST");
    // The namespaced block rides along in the posted body (the raw-dict embed).
    expect(JSON.parse(seenInit?.body as string)).toEqual(draft);
  });

  it("throws ApiError on a true 4xx (a malformed request)", async () => {
    stubFetch(async () => ({
      ok: false,
      status: 400,
      json: async () => ({ detail: "request body is not a JSON object" }),
    }));
    const err = (await validateProject({}).catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(400);
    expect(err.message).toBe("request body is not a JSON object");
  });
});

describe("search", () => {
  // Capture the URL + the fetch init for a single stubbed call.
  async function callFor(
    q: string,
    options?: Parameters<typeof search>[1],
  ): Promise<{ url: string; init: RequestInit | undefined }> {
    let url = "";
    let init: RequestInit | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((u: string, i?: RequestInit) => {
        url = u;
        init = i;
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({ kind: "search", query: q, groups: [] }),
        });
      }),
    );
    await search(q, options);
    return { url, init };
  }

  it("encodes the query and omits limit by default", async () => {
    const { url } = await callFor("kö n");
    expect(url).toBe("/api/search?q=k%C3%B6+n");
  });

  it("appends an explicit limit", async () => {
    const { url } = await callFor("kon", { limit: 5 });
    expect(url).toBe("/api/search?q=kon&limit=5");
  });

  it("appends an explicit non-'all' type (#393 item 1)", async () => {
    const { url } = await callFor("kon", { type: "value" });
    expect(url).toBe("/api/search?q=kon&type=value");
  });

  it("OMITS type=all (the server default → clean canonical URL)", async () => {
    const { url } = await callFor("kon", { type: "all" });
    expect(url).toBe("/api/search?q=kon");
  });

  it("omits type when not provided", async () => {
    const { url } = await callFor("kon");
    expect(url).toBe("/api/search?q=kon");
  });

  it("always passes an AbortSignal to fetch (the ~12s timeout floor)", async () => {
    // Even with no caller signal, `search` layers AbortSignal.timeout so a hung
    // request can't spin forever — fetch must always receive a signal.
    const { init } = await callFor("kon");
    expect(init?.signal).toBeInstanceOf(AbortSignal);
    expect(init?.signal?.aborted).toBe(false);
  });

  it("aborts fetch when the caller's signal aborts (supersede)", async () => {
    // The combined signal (caller ∪ timeout) must abort when the CALLER aborts —
    // this is the supersede path the omnibox relies on.
    const controller = new AbortController();
    const { init } = await callFor("kon", { signal: controller.signal });
    expect(init?.signal?.aborted).toBe(false);
    controller.abort();
    expect(init?.signal?.aborted).toBe(true);
  });
});

describe("docSearch (#394)", () => {
  // Mirror the `search` suite's harness: capture the URL + fetch init for one call.
  // docSearch shares search's `searchGet` plumbing, so the encoding/abort assertions
  // are identical against the `/docs/search` path.
  async function callFor(
    q: string,
    options?: Parameters<typeof docSearch>[1],
  ): Promise<{ url: string; init: RequestInit | undefined }> {
    let url = "";
    let init: RequestInit | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((u: string, i?: RequestInit) => {
        url = u;
        init = i;
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({
            kind: "doc-search",
            query: q,
            ingested: true,
            total_count: 0,
            results: [],
          }),
        });
      }),
    );
    await docSearch(q, options);
    return { url, init };
  }

  it("encodes the query and omits limit by default (same encoding as search)", async () => {
    const { url } = await callFor("kö n");
    expect(url).toBe("/api/docs/search?q=k%C3%B6+n");
  });

  it("appends an explicit limit", async () => {
    const { url } = await callFor("kon", { limit: 5 });
    expect(url).toBe("/api/docs/search?q=kon&limit=5");
  });

  it("always passes an AbortSignal to fetch (the ~12s timeout floor)", async () => {
    const { init } = await callFor("kon");
    expect(init?.signal).toBeInstanceOf(AbortSignal);
    expect(init?.signal?.aborted).toBe(false);
  });
});

describe("getDoc (#394)", () => {
  it("GETs the doc endpoint with the WHOLE identifier as one encoded segment", async () => {
    // A space AND a reserved char (`&`) prove encodeURIComponent runs over the
    // entire identifier (one path segment / filename), not split on anything.
    let seen = "";
    stubFetch(async (url) => {
      seen = url;
      return {
        ok: true,
        status: 200,
        json: async () => ({ kind: "doc", filename: "lisa kon.md", tags: [] }),
      };
    });
    await getDoc("lisa kon&x.md");
    expect(seen).toBe("/api/docs/doc/lisa%20kon%26x.md");
  });

  it("throws ApiError carrying status 404 + backend detail when the doc is absent", async () => {
    stubFetch(async () => ({
      ok: false,
      status: 404,
      json: async () => ({ detail: "no documentation for 'x'" }),
    }));
    const err = (await getDoc("x").catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(404);
    expect(err.message).toBe("no documentation for 'x'");
  });
});

describe("getDocsForVariable (#402)", () => {
  // Mirror the `docSearch`/`search` harness: capture the URL + fetch init for one
  // call. `getDocsForVariable` shares the same `searchGet` plumbing against the
  // `/docs/for-variable` path, plus the `register` filter the for-variable hook
  // appends — so the encoding/abort assertions are the same shape.
  async function callFor(
    q: string,
    options?: Parameters<typeof getDocsForVariable>[1],
  ): Promise<{ url: string; init: RequestInit | undefined }> {
    let url = "";
    let init: RequestInit | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((u: string, i?: RequestInit) => {
        url = u;
        init = i;
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({
            ingested: true,
            kind: "doc-mentions",
            register: "lisa",
            register_ingested: true,
            total_count: 0,
            results: [],
          }),
        });
      }),
    );
    await getDocsForVariable(q, options);
    return { url, init };
  }

  it("encodes q + register + limit (the register filter is appended)", async () => {
    // `searchGet` appends params in q → limit → register order; `kö n` exercises
    // the URLSearchParams encoding (space → `+`, ö → `%C3%B6`).
    const { url } = await callFor("kö n", { register: "lisa", limit: 5 });
    expect(url).toBe(
      "/api/docs/for-variable?q=k%C3%B6+n&limit=5&register=lisa",
    );
  });

  it("omits register from the URL when not passed", async () => {
    const { url } = await callFor("kon");
    expect(url).toBe("/api/docs/for-variable?q=kon");
  });

  it("always passes an AbortSignal to fetch (the ~12s timeout floor)", async () => {
    // Even with no caller signal, the shared `searchGet` layers AbortSignal.timeout
    // so a hung request can't spin forever — fetch must always receive a signal.
    const { init } = await callFor("kon");
    expect(init?.signal).toBeInstanceOf(AbortSignal);
    expect(init?.signal?.aborted).toBe(false);
  });

  it("aborts fetch when the caller's signal aborts (supersede)", async () => {
    // The combined signal (caller ∪ timeout) must abort when the CALLER aborts —
    // the supersede path the panel's asyncResource teardown relies on.
    const controller = new AbortController();
    const { init } = await callFor("kon", { signal: controller.signal });
    expect(init?.signal?.aborted).toBe(false);
    controller.abort();
    expect(init?.signal?.aborted).toBe(true);
  });
});

describe("isCatalogNode", () => {
  it("is true for a kind-tagged node", () => {
    const node = { kind: "binding" } as unknown as BindingNodeData;
    expect(isCatalogNode(node)).toBe(true);
  });

  it("is false for a no-`kind` response (a StatesResponse subset)", () => {
    // The only field `isCatalogNode` reads is `kind`; a StatesResponse (or any
    // sub-endpoint payload) has none, so it's distinguished from a browsable node.
    const states: StatesResponse = { binding: "scb/lisa/kon", states: [] };
    expect(isCatalogNode(states)).toBe(false);
  });
});
