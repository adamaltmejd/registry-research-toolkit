import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CatalogNode, RootResponse, VariantsResponse } from "./api";
import { getCatalogNode, getCatalogRoot, getRegisterVariants } from "./api";
import { resetCatalogNames, sourceNames } from "./catalog_names.svelte";

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
