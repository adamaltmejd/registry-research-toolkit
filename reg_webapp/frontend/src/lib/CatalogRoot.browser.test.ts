import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { RootResponse } from "./api";
import { getCatalogRoot } from "./api";
import CatalogRoot from "./CatalogRoot.svelte";

// CatalogRoot fetches the catalog root via `getCatalogRoot()` and lists every
// top-level catalog section. Mock that single GET (mirrors CatalogNodeView's
// api-mock style); keep the rest of api.ts real (the type exports + path helpers
// `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogRoot: vi.fn(),
  };
});

// The catalog root with two provider children (`scb` / `sos`) plus the
// classification-root sentinel. Shaped like RootResponse children — the root
// renders these as a plain navigation table.
function catalogRoot(): RootResponse {
  return {
    kind: "root",
    children: [
      { kind: "provider", fqid: "scb", name: "SCB" },
      { kind: "provider", fqid: "sos", name: "SoS" },
      { kind: "classification-root", fqid: "class", name: "Classifications" },
    ],
  } as unknown as RootResponse;
}

beforeEach(() => {
  vi.mocked(getCatalogRoot).mockReset();
});

describe("CatalogRoot", () => {
  it("renders top-level catalog sections as a single-column navigation table", async () => {
    vi.mocked(getCatalogRoot).mockResolvedValue(catalogRoot());

    const { container } = await render(CatalogRoot, {});

    await expect
      .element(page.getByRole("columnheader", { name: "Name" }))
      .toBeVisible();

    // #806/#976: each section is a name link to its catalog page…
    await expect
      .element(page.getByRole("link", { name: "SCB" }))
      .toHaveAttribute("href", "/catalog/scb");
    await expect
      .element(page.getByRole("link", { name: "Classifications" }))
      .toHaveAttribute("href", "/catalog/class");

    const table = container.querySelector("table.data-table");
    expect(table?.closest(".panel")).toBeNull();
    expect(table?.querySelectorAll("thead tr")).toHaveLength(1);
    expect(
      [...(table?.querySelectorAll("thead th") ?? [])].map((th) =>
        th.textContent?.trim(),
      ),
    ).toEqual(["Name"]);
    expect(container.querySelector("thead")?.textContent).not.toContain("Type");
    expect(container.querySelector("thead")?.textContent).not.toContain(
      "Scope",
    );
    expect(container.querySelector(".tag")).toBeNull();

    // …with the raw FQID <code> element dropped — the link's name is identity.
    expect(container.querySelector("code")).toBeNull();
  });

  it("shows EmptyState when the filter matches nothing", async () => {
    vi.mocked(getCatalogRoot).mockResolvedValue(catalogRoot());

    await render(CatalogRoot, {});

    const filterBox = page.getByRole("textbox", {
      name: /Filter catalog sections/i,
    });
    await filterBox.fill("zzz");

    await expect
      .element(page.getByText(/No catalog sections match/))
      .toBeVisible();
  });
});
