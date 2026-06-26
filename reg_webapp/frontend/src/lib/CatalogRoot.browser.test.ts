import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { RootResponse } from "./api";
import { getCatalogRoot } from "./api";
import CatalogRoot from "./CatalogRoot.svelte";

// CatalogRoot fetches the catalog root via `getCatalogRoot()` and lists every
// provider. Mock that single GET (mirrors CatalogNodeView's api-mock style); keep
// the rest of api.ts real (the type exports + path helpers `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogRoot: vi.fn(),
  };
});

// The catalog root with two provider children (`scb` / `sos`). Shaped like
// RootResponse → ProviderNode children — the #806 root renders these as DataTable
// links (name → catalog link) with the FQID code element dropped.
function catalogRoot(): RootResponse {
  return {
    kind: "root",
    children: [
      { kind: "provider", fqid: "scb", name: "SCB" },
      { kind: "provider", fqid: "sos", name: "SoS" },
    ],
  } as unknown as RootResponse;
}

beforeEach(() => {
  vi.mocked(getCatalogRoot).mockReset();
});

describe("CatalogRoot", () => {
  it("renders each provider as a link by name with no FQID code element", async () => {
    vi.mocked(getCatalogRoot).mockResolvedValue(catalogRoot());

    const { container } = await render(CatalogRoot, {});

    // #806: each provider is a name link to its catalog page…
    await expect
      .element(page.getByRole("link", { name: "SCB" }))
      .toHaveAttribute("href", "/catalog/scb");

    // …with the raw FQID <code> element dropped — the link's name is identity.
    expect(container.querySelector("code")).toBeNull();
  });

  it("shows EmptyState when the filter matches nothing", async () => {
    vi.mocked(getCatalogRoot).mockResolvedValue(catalogRoot());

    await render(CatalogRoot, {});

    const filterBox = page.getByRole("textbox", { name: /Filter providers/i });
    await filterBox.fill("zzz");

    await expect.element(page.getByText(/No providers match/)).toBeVisible();
  });
});
