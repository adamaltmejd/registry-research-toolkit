import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ShowNode } from "./api";
import { rootShow, stubApi } from "./api-test-helpers";
import { resetCatalogNames, sourceNames } from "./catalog_names";

// Y-80: the cart holds coordinates, so the words come from the catalog. These
// cases pin `sourceNames`' ALL-OR-NOTHING contract — a card says the coordinate it
// already holds until EVERY read behind its name has landed with an answer, so it
// can never settle in two steps or say half a name that reads like another source.

const COORDINATE = "scb/lisa/individer-16plus";

/** Serve `show` for the root, the `scb` provider (which names every register it
 * owns) and the `scb/lisa` register (which carries its variants). */
function stubCatalog(root: () => ShowNode) {
  stubApi((url): ShowNode | undefined => {
    if (url.pathname === "/api/catalog") return root();
    if (url.pathname === "/api/catalog/scb") {
      return {
        kind: "provider",
        fqid: "scb",
        name: "scb",
        children: [{ fqid: "scb/lisa", name: "LISA", purpose: null }],
      } as unknown as ShowNode;
    }
    if (url.pathname !== "/api/catalog/scb/lisa") return undefined;
    return {
      kind: "register",
      fqid: "scb/lisa",
      name: "LISA",
      children: [],
      variants: [
        { slug: "individer-16plus", name: "Individer, 16 år och äldre" },
      ],
    } as unknown as ShowNode;
  });
}

beforeEach(() => {
  // The cache is a session singleton: start each case from an empty one.
  resetCatalogNames();
  stubCatalog(() =>
    rootShow({ fqid: "scb", name: "Statistiska Centralbyrån" }),
  );
});

/** Ask for the names (which STARTS the reads), then wait out those reads. Every
 * call re-reads the cache, so the returned value is the settled one. */
async function settledNames() {
  sourceNames(COORDINATE);
  await vi.waitFor(() => expect(sourceNames(COORDINATE).loading).toBe(false));
  return sourceNames(COORDINATE);
}

describe("sourceNames (the words behind a source's coordinate)", () => {
  it("names nothing when the catalog ROOT read failed, though the rest landed", async () => {
    // The root is what says whether a bare register name is ambiguous here, so it
    // is part of the name: with it missing, a card on a multi-provider deployment
    // would title itself unqualified and read as a register nobody has to
    // disambiguate. The coordinate is the honest answer instead.
    // Fails if the names stop waiting on the root `show` read (package C moved
    // every name read to `show`).
    stubCatalog(() => {
      throw new Error("offline");
    });

    const names = await settledNames();

    expect(names.register).toBeNull();
    expect(names.variant).toBeNull();
    expect(names.provider).toBeNull();
  });

  it("names nothing when a multi-provider root does not list this provider", async () => {
    // Fails if a provider missing from the root `show` still yields a name.
    stubCatalog(() =>
      rootShow(
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
