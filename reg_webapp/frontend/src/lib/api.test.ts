import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  apiGet,
  classificationGroupPath,
  getBindingLineageWarnings,
  getCatalogNode,
  getConceptGroup,
  getContext,
  getDoc,
  getDocsForVariable,
  getRelatedDocuments,
  getValueSetCodes,
  relatedDocumentFileHref,
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

  it("surfaces the message of the Rust server's {error, meta} document", async () => {
    // Fails if messageFromBody stops reading `error.message` (the banner would
    // show the bare status line).
    stubFetch(async () => ({
      ok: false,
      status: 422,
      json: async () => ({
        error: {
          code: "scope_unavailable",
          class: "usage",
          message: "This catalog has no holdings scope.",
          remediation: "Drop `scope`.",
          fields: ["holdings"],
        },
        meta: {
          contract_version: "4.0.0",
          generation: "0",
          scope: "reference",
        },
      }),
    }));
    const err = (await getContext().catch((e) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(422);
    expect(err.message).toBe("This catalog has no holdings scope.");
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

describe("getContext", () => {
  it("returns the `data` of the Rust server's {data, meta} envelope", async () => {
    // Fails if getContext returns the envelope instead of its `data`.
    const data = { schema_version: "9.3.0" };
    stubFetch(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ data, meta: { scope: "reference" } }),
    }));
    expect(await getContext()).toEqual(data);
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

  it("omits undefined/empty params but keeps the present ones", async () => {
    const url = await urlFor("scb/lisa/kon", {
      period: "2018..2020",
      variant: undefined,
      value_set_version: "",
    });
    // The `..` range and reserved chars are percent-encoded by URLSearchParams.
    expect(url).toBe("/api/catalog/scb/lisa/kon?period=2018..2020");
  });
});

describe("getConceptGroup", () => {
  it("encodes provider/register/key as fixed route segments", async () => {
    let seen = "";
    stubFetch(async (url) => {
      seen = url;
      return { ok: true, status: 200, json: async () => ({}) };
    });
    await getConceptGroup("scb", "lsön", "a/b", "member x/y");
    expect(seen).toBe(
      "/api/catalog/group/scb/ls%C3%B6n/a%2Fb?member=member%20x%2Fy",
    );
  });
});

describe("classificationGroupPath / getClassificationGroup (#756)", () => {
  it("builds the fixed `class` route with the key encoded", () => {
    // The path is `/api`-less (like conceptGroupPath) — `apiGet` prepends `/api`.
    expect(classificationGroupPath("sun")).toBe("/catalog/group/class/sun");
    expect(classificationGroupPath("a/b")).toBe("/catalog/group/class/a%2Fb");
  });
});

describe("binding sub-endpoint helpers", () => {
  // The SPA now fetches only `/lineage_warnings` (succession rides the embedded
  // `succession_chain`, #582). GETs `/catalog/{encodeFqid}/lineage_warnings`.
  it("encodes the FQID segments in a sub-endpoint URL", async () => {
    let seen = "";
    stubFetch(async (url) => {
      seen = url;
      return { ok: true, status: 200, json: async () => ({}) };
    });
    await getBindingLineageWarnings("scb/lisa/kön");
    expect(seen).toBe("/api/catalog/scb/lisa/k%C3%B6n/lineage_warnings");
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
    const draft = { schema_version: "2.0.0", typo_root: { x: 1 } };
    await validateProject(draft);
    expect(seenUrl).toBe("/api/project/validate");
    expect(seenInit?.method).toBe("POST");
    // The invalid key survives transport so the backend can diagnose it.
    expect(JSON.parse(seenInit?.body as string)).toEqual(draft);
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
          json: async () => ({
            data: { items: [], next_cursor: null },
            meta: {},
          }),
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

  it("sends the page's type, limit and cursor", async () => {
    // Fails if a continuation drops `limit` (the Rust default is 50, not the
    // SPA's page size) or the arm it continues.
    const { url } = await callFor("kon", {
      type: "register_value",
      limit: 3,
      cursor: "ab",
    });
    expect(url).toBe("/api/search?q=kon&limit=3&type=register_value&cursor=ab");
  });

  it("returns the page inside the Rust server's {data, meta}", async () => {
    // Fails if `search` hands callers the envelope instead of `data`.
    vi.stubGlobal(
      "fetch",
      vi.fn(() =>
        Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({
            data: { items: [], next_cursor: "c1" },
            meta: { scope: "reference" },
          }),
        }),
      ),
    );
    expect(await search("kon")).toEqual({ items: [], next_cursor: "c1" });
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

describe("getDoc (#394)", () => {
  it("GETs the WHOLE identifier as one encoded segment and returns `data`", async () => {
    // A space AND a reserved char (`&`) prove encodeURIComponent runs over the
    // entire identifier (one path segment / filename), not split on anything.
    // Fails if the identifier is split, or if getDoc returns the envelope.
    let seen = "";
    const data = { filename: "lisa kon&x.md", display_name: "LISA", tags: [] };
    stubFetch(async (url) => {
      seen = url;
      return {
        ok: true,
        status: 200,
        json: async () => ({ data, meta: {} }),
      };
    });
    expect(await getDoc("lisa kon&x.md")).toEqual(data);
    expect(seen).toBe("/api/docs/doc/lisa%20kon%26x.md");
  });
});

// The Rust server's error document for a docs read.
function docsError(code: string): () => Promise<unknown> {
  return async () => ({
    ok: false,
    status: 404,
    json: async () => ({ error: { code, message: code }, meta: {} }),
  });
}

describe("getDocsForVariable (#402)", () => {
  it("GETs /docs/search with q, limit and the register FQID, and returns `data`", async () => {
    // `kö n` exercises the URLSearchParams encoding (space → `+`, ö → `%C3%B6`).
    // Fails if the path, the register parameter or the `{data, meta}` unwrap
    // changes. The abort/timeout plumbing is `searchGet`'s, asserted on `search`.
    let seen = "";
    const data = { items: [], total: 0, register_ingested: true };
    stubFetch(async (url) => {
      seen = url;
      return {
        ok: true,
        status: 200,
        json: async () => ({ data, meta: {} }),
      };
    });
    expect(
      await getDocsForVariable("kö n", { register: "scb/lisa", limit: 5 }),
    ).toEqual(data);
    expect(seen).toBe(
      "/api/docs/search?q=k%C3%B6+n&limit=5&register=scb%2Flisa",
    );
  });

  it("returns null for `docs_unavailable` and throws any other error", async () => {
    // A deployment without a docs database is not an error to the panel. Fails if
    // `docs_unavailable` throws, or if another 404 code is swallowed as "no docs".
    stubFetch(docsError("docs_unavailable"));
    expect(
      await getDocsForVariable("kon", { register: "scb/lisa" }),
    ).toBeNull();

    stubFetch(docsError("not_found"));
    await expect(
      getDocsForVariable("kon", { register: "scb/nope" }),
    ).rejects.toMatchObject({ name: "ApiError", status: 404 });
  });
});

describe("getRelatedDocuments / relatedDocumentFileHref (#742)", () => {
  it("GETs the register FQID path, returns `data`, and maps `docs_unavailable` to null", async () => {
    // Fails if the FQID is encoded as one segment (`scb%2Flisa`), the envelope is
    // returned, or a docs-less deployment throws.
    const docs = [{ filename: "lisa.pdf" }];
    let seen = "";
    stubFetch(async (url) => {
      seen = url;
      return {
        ok: true,
        status: 200,
        json: async () => ({ data: docs, meta: {} }),
      };
    });
    expect(await getRelatedDocuments("scb/lisa")).toEqual(docs);
    expect(seen).toBe("/api/docs/related/scb/lisa");

    stubFetch(docsError("docs_unavailable"));
    expect(await getRelatedDocuments("scb/lisa")).toBeNull();
  });

  it("builds the file href from the register FQID and an encoded filename", () => {
    // Fails if the FQID's `/` is encoded or the filename is not.
    expect(relatedDocumentFileHref("scb/lisa", "lisa manual.pdf")).toBe(
      "/api/docs/file/scb/lisa/lisa%20manual.pdf",
    );
  });
});

it("round-trips adjacent catalog IDs above JavaScript’s safe integer range without collision", async () => {
  const paths: string[] = [];
  stubFetch(async (url) => {
    paths.push(url);
    const parsed = new URL(url, "https://catalog.test");
    return {
      ok: true,
      status: 200,
      json: async () => ({
        value_set_id: parsed.pathname.split("/")[3],
        state_id: parsed.searchParams.get("state"),
        codes: [],
        total: 0,
        offset: 0,
        limit: 200,
        q: "",
      }),
    };
  });
  const ids = ["9007199254740992", "9007199254740993"];
  for (const id of ids) {
    const response = await getValueSetCodes(id, {
      state: id,
      classification: "sni2007",
      partition: "sentinels",
      column: "NgS1",
      alias_window_from: "2013-01-01",
    });
    expect(response.value_set_id).toBe(id);
    expect(response.state_id).toBe(id);
  }
  expect(new Set(paths).size).toBe(2);
  expect(paths[1]).toContain("/9007199254740993/codes?");
  expect(paths[1]).toContain("state=9007199254740993");
  expect(paths[1]).toContain("classification=sni2007");
  expect(paths[1]).toContain("alias_window_from=2013-01-01");
});
