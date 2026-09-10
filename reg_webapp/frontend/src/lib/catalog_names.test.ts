import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CatalogNode, RootResponse, VariantsResponse } from "./api";
import { getCatalogNode, getCatalogRoot, getRegisterVariants } from "./api";
import {
  columnNames,
  resetCatalogNames,
  sourceNames,
  UNASKED,
} from "./catalog_names.svelte";

// Y-80: the cart holds coordinates, so the words come from the catalog. These
// cases pin `sourceNames`' ALL-OR-NOTHING contract — a card says the coordinate it
// already holds until EVERY read behind its name has landed with an answer, so it
// can never settle in two steps or say half a name that reads like another source.
// The rendered side of the same contract is SourceEditor.browser.test.ts.

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
    getCatalogRoot: vi.fn(),
    getRegisterVariants: vi.fn(),
  };
});

const COORDINATE = "scb/lisa/individer-16plus";

/** The catalog root as a deployment serving exactly these providers. */
function rootResponse(
  ...providers: { fqid: string; name: string }[]
): RootResponse {
  return {
    kind: "root",
    children: providers.map(({ fqid, name }) => ({
      kind: "provider",
      fqid,
      name,
    })),
  } as unknown as RootResponse;
}

/** The `scb` provider node, which names every register it owns. */
function scbNode(): CatalogNode {
  return {
    kind: "provider",
    fqid: "scb",
    name: "scb",
    children: [
      { kind: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
    ],
  } as unknown as CatalogNode;
}

beforeEach(() => {
  // The cache is a session singleton: start each case from an empty one.
  resetCatalogNames();
  vi.mocked(getCatalogRoot).mockReset();
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getRegisterVariants).mockReset();
  vi.mocked(getCatalogRoot).mockResolvedValue(
    rootResponse({ fqid: "scb", name: "Statistiska Centralbyrån" }),
  );
  vi.mocked(getCatalogNode).mockResolvedValue(scbNode());
  vi.mocked(getRegisterVariants).mockResolvedValue({
    variants: [
      { slug: "individer-16plus", name: "Individer, 16 år och äldre" },
    ],
  } as unknown as VariantsResponse);
});

/** Ask for the names (which STARTS the reads), then wait out those reads. Every
 * call re-reads the cache, so the returned value is the settled one. */
async function settledNames() {
  sourceNames(COORDINATE);
  await vi.waitFor(() => expect(sourceNames(COORDINATE).loading).toBe(false));
  return sourceNames(COORDINATE);
}

describe("sourceNames (the words behind a source's coordinate)", () => {
  it("names the register and the variant once every read has landed", async () => {
    const names = await settledNames();

    expect(names.register).toBe("LISA");
    expect(names.variant).toBe("Individer, 16 år och äldre");
    // One provider: the name is there, and the caller (App's `providerQualified`)
    // is what decides that this deployment does not need it.
    expect(names.provider).toBe("Statistiska Centralbyrån");
  });

  it("names nothing when the catalog ROOT read failed, though the rest landed", async () => {
    // The root is what says whether a bare register name is ambiguous here, so it
    // is part of the name: with it missing, a card on a multi-provider deployment
    // would title itself unqualified and read as a register nobody has to
    // disambiguate. The coordinate is the honest answer instead.
    vi.mocked(getCatalogRoot).mockRejectedValue(new Error("offline"));

    const names = await settledNames();

    expect(names.register).toBeNull();
    expect(names.variant).toBeNull();
    expect(names.provider).toBeNull();
  });

  it("names nothing when a multi-provider root does not list this provider", async () => {
    vi.mocked(getCatalogRoot).mockResolvedValue(
      rootResponse(
        { fqid: "sos", name: "Socialstyrelsen" },
        { fqid: "fk", name: "Försäkringskassan" },
      ),
    );

    const names = await settledNames();

    expect(names.provider).toBeNull();
    expect(names.register).toBeNull();
    expect(names.variant).toBeNull();
  });
});

describe("columnNames (the delivery column a binding's variable resolves to)", () => {
  it("asks nothing for a coordinate carrying no variant", () => {
    // A `register_variant` of fewer than 3 segments — only a malformed draft has
    // one — leaves the card with an empty variant slug. Resolving the variable
    // WITHOUT it answers over every variant of the register, so the row would
    // lead with a delivery column this source does not deliver.
    expect(columnNames("scb/lisa/kon", "2020", "")).toBe(UNASKED);
    expect(vi.mocked(getCatalogNode).mock.calls).toHaveLength(0);
  });

  it("resolves at the source's own (period, variant)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: [],
    } as unknown as CatalogNode);

    columnNames("scb/lisa/kon", "1990..2020", "arbetsstallen");

    await vi.waitFor(() =>
      expect(
        columnNames("scb/lisa/kon", "1990..2020", "arbetsstallen").loading,
      ).toBe(false),
    );
    expect(vi.mocked(getCatalogNode).mock.calls).toEqual([
      ["scb/lisa/kon", { period: "1990..2020", variant: "arbetsstallen" }],
    ]);
  });
});

describe("resetCatalogNames", () => {
  it("keeps a read that was in flight when the cache was reset out of it", async () => {
    // The browser suites reset in `beforeEach`; a read the previous case started
    // must not land in the next case's cache and answer it with the previous
    // case's stub.
    let landStale!: (response: VariantsResponse) => void;
    vi.mocked(getRegisterVariants).mockReturnValue(
      new Promise<VariantsResponse>((resolve) => {
        landStale = resolve;
      }),
    );
    sourceNames(COORDINATE);
    expect(sourceNames(COORDINATE).loading).toBe(true);

    resetCatalogNames();
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [{ slug: "individer-16plus", name: "Individer, 16+" }],
    } as unknown as VariantsResponse);
    expect((await settledNames()).variant).toBe("Individer, 16+");

    // The abandoned read lands only now, on a cache that has already answered
    // from its own request. A macrotask hop drains the microtasks its settle
    // rides on, so this asserts AFTER the abandoned read had its chance at the
    // cache rather than merely before it took one.
    landStale({
      variants: [{ slug: "individer-16plus", name: "Stale, from before" }],
    } as unknown as VariantsResponse);
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(sourceNames(COORDINATE).variant).toBe("Individer, 16+");
  });
});
