import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, apiGet, getCatalogNode } from "./api";

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
  it("URL-encodes each FQID segment but keeps the slash separators", async () => {
    let seen = "";
    stubFetch(async (url) => {
      seen = url;
      return { ok: true, status: 200, json: async () => ({ kind: "binding" }) };
    });
    await getCatalogNode("scb/lisa/kön");
    // Segments encoded individually; the path separators survive.
    expect(seen).toBe("/api/catalog/scb/lisa/k%C3%B6n");
  });
});
