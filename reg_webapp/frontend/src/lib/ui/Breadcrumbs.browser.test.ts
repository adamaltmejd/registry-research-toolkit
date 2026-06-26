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

  it("treats only the last item as current — a plain non-final item is not (Fix 6)", async () => {
    // A middle item without href is an intentionally-plain span; it must NOT also
    // expose aria-current="page", or two "current page" nodes leak into a11y.
    const withPlainMiddle = [
      { label: "Catalog", href: "/catalog" },
      { label: "SCB" }, // no href, non-final → plain span, no aria-current
      { label: "LISA", href: "/catalog/scb/lisa" },
    ];
    const { container } = render(Breadcrumbs, { items: withPlainMiddle });
    // Exactly one current node, and it's the last item.
    const currents = container.querySelectorAll('[aria-current="page"]');
    expect(currents).toHaveLength(1);
    expect(currents[0]?.textContent?.trim()).toBe("LISA");
    // The non-final no-href item rendered as a plain span without aria-current.
    const spans = container.querySelectorAll("li span");
    const scb = Array.from(spans).find((s) => s.textContent?.trim() === "SCB");
    expect(scb).not.toBeUndefined();
    expect(scb).not.toHaveAttribute("aria-current");
  });
});
