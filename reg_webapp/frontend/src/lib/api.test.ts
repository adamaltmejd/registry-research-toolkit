import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  apiGet,
  type BindingNodeData,
  getBindingLineage,
  getBindingLineageWarnings,
  getBindingPredecessors,
  getBindingRelated,
  getBindingStates,
  getBindingSuccessors,
  getCatalogNode,
  isStatesResponse,
  type StatesResponse,
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
    [getBindingStates, "states", "/api/catalog/scb/lisa/kon/states"],
    [
      getBindingPredecessors,
      "predecessors",
      "/api/catalog/scb/lisa/kon/predecessors",
    ],
    [
      getBindingSuccessors,
      "successors",
      "/api/catalog/scb/lisa/kon/successors",
    ],
    [getBindingRelated, "related", "/api/catalog/scb/lisa/kon/related"],
    [getBindingLineage, "lineage", "/api/catalog/scb/lisa/kon/lineage"],
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

describe("isStatesResponse", () => {
  it("is true for a StatesResponse (no `kind`, has binding+states)", () => {
    const states: StatesResponse = { binding: "scb/lisa/kon", states: [] };
    expect(isStatesResponse(states)).toBe(true);
  });

  it("is false for a kind-tagged BindingNode", () => {
    // A minimal BindingNode-shaped object: the only field `isStatesResponse`
    // reads is `kind`, so the structural check distinguishes the two.
    const node = { kind: "binding" } as unknown as BindingNodeData;
    expect(isStatesResponse(node)).toBe(false);
  });
});
