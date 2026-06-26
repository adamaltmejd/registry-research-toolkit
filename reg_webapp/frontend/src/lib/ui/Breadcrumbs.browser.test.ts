import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import Breadcrumbs from "./Breadcrumbs.svelte";

// Breadcrumbs: the contract is (1) a labelled nav landmark, (2) intermediate
// items are links, (3) the LAST item is the current page — plain text with
// aria-current="page", never a link.
describe("Breadcrumbs", () => {
  const items = [
    { label: "Catalog", href: "/catalog" },
    { label: "SCB", href: "/catalog/scb" },
    { label: "LISA", href: "/catalog/scb/lisa" },
  ];

  it("exposes a Breadcrumb nav landmark", async () => {
    await render(Breadcrumbs, { items });
    await expect
      .element(page.getByRole("navigation", { name: "Breadcrumb" }))
      .toBeVisible();
  });

  it("links every item except the last", async () => {
    await render(Breadcrumbs, { items });
    await expect
      .element(page.getByRole("link", { name: "Catalog" }))
      .toBeVisible();
    await expect.element(page.getByRole("link", { name: "SCB" })).toBeVisible();
    // The current page is not a link.
    expect(page.getByRole("link", { name: "LISA" }).query()).toBeNull();
  });

  it("marks the last item as the current page", async () => {
    const { container } = render(Breadcrumbs, { items });
    const current = container.querySelector('[aria-current="page"]');
    expect(current).not.toBeNull();
    expect(current?.textContent).toBe("LISA");
  });
});
