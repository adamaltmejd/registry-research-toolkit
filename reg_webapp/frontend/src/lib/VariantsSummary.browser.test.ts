import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import VariantsSummary from "./VariantsSummary.svelte";
import {
  datedVersions,
  variant,
  variantsResponse,
} from "./variants-test-helpers";

// Presentational since Y-82 — `CatalogNodeView` fetches the list once for the page
// (its variant chips name their variants out of it) and hands it down, so these
// render with the response as a prop instead of mocking the GET.

/** The slug lines of every row, in render order. */
function slugsOf(container: HTMLElement): string[] {
  return [...container.querySelectorAll("tbody code.slug")].map(
    (el) => el.textContent ?? "",
  );
}

describe("VariantsSummary — one row per variant family (Y-79)", () => {
  it("folds a family into one row with its slugs, span and a link to the page", async () => {
    const variants = variantsResponse(
      // Slug order out of the catalog puts the successor first.
      variant("individer-15plus", {
        name: "Individer, 15 år och äldre",
        variant_family: "individer-15plus",
        variant_family_label: "Individer",
        versions: datedVersions(2010, 2023),
      }),
      variant("individer-16plus", {
        name: "Individer, 16 år och äldre",
        variant_family: "individer-15plus",
        variant_family_label: "Individer",
        versions: datedVersions(1990, 2009),
      }),
      variant("arbetsstallen", {
        name: "Arbetsställen",
        versions: datedVersions(2005, 2023),
      }),
    );

    const { container } = await render(VariantsSummary, {
      registerFqid: "scb/lisa",
      variants,
    });

    await expect
      .element(page.getByRole("heading", { name: "Variants" }))
      .toBeVisible();
    // Two rows for three variants: the family is one row, oldest segment first.
    expect(container.querySelectorAll("tbody tr")).toHaveLength(2);
    expect(slugsOf(container)).toEqual([
      "individer-16plus",
      "individer-15plus",
      "arbetsstallen",
    ]);
    await expect.element(page.getByText("1990–2023")).toBeVisible();
    await expect.element(page.getByText("2005–2023")).toBeVisible();
    // No version wall on the register page — the prose lives on the page below.
    expect(container.querySelectorAll("section.version-meta")).toHaveLength(0);
    await expect
      .element(page.getByRole("link", { name: "All variant details" }))
      .toHaveAttribute("href", "/catalog/scb/lisa/variants");
  });

  it("leaves the year cell empty for a variant with no dated versions", async () => {
    const { container } = await render(VariantsSummary, {
      registerFqid: "scb/lisa",
      variants: variantsResponse(
        variant("combined", { name: "Combined register" }),
      ),
    });

    await expect
      .element(page.getByText("Combined register", { exact: true }))
      .toBeVisible();
    const cells = container.querySelectorAll("tbody td");
    expect(cells[1]?.textContent?.trim()).toBe("");
  });

  it("surfaces a failed load as an alert", async () => {
    await render(VariantsSummary, {
      registerFqid: "scb/lisa",
      variants: null,
      error: "Error: boom",
    });

    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("Failed to load variants: Error: boom");
  });
});

describe("VariantsSummary — hide the section without a real variant (#673/M4)", () => {
  it("renders NOTHING when the only variant is the synthesized/stored _default", async () => {
    // `_default` is NOT a user-facing variant (it's a stored variant for some
    // registers and the synthesized default for others). A register whose only
    // "variant" is _default has no real variant axis → no section, no heading,
    // no link to a page with nothing on it.
    const { container } = await render(VariantsSummary, {
      registerFqid: "scb/sol",
      variants: variantsResponse(variant("_default")),
    });

    expect(
      page.getByRole("heading", { name: "Variants" }).elements(),
    ).toHaveLength(0);
    expect(container.querySelector("section.variants")).toBeNull();
    expect(
      page.getByRole("link", { name: "All variant details" }).elements(),
    ).toHaveLength(0);
  });

  it("renders NOTHING for an empty variant list", async () => {
    const { container } = await render(VariantsSummary, {
      registerFqid: "scb/empty",
      variants: variantsResponse(),
    });

    expect(
      page.getByRole("heading", { name: "Variants" }).elements(),
    ).toHaveLength(0);
    expect(container.querySelector("section.variants")).toBeNull();
    // The dropped fallback text must not appear either.
    expect(page.getByText("No variants.").elements()).toHaveLength(0);
  });

  it("renders the section for a real variant mixed with _default (no _default filtering of the list)", async () => {
    // ≥1 real variant → render the FULL list unchanged; _default is NOT filtered
    // out of a mixed list (out of scope), so it still appears alongside the real
    // one — only the all-_default/empty cases suppress the section.
    const { container } = await render(VariantsSummary, {
      registerFqid: "scb/lisa",
      variants: variantsResponse(
        variant("individer", { name: "Individer" }),
        variant("_default"),
      ),
    });

    await expect
      .element(page.getByRole("heading", { name: "Variants" }))
      .toBeVisible();
    expect(container.querySelectorAll("tbody tr")).toHaveLength(2);
    expect(slugsOf(container)).toEqual(["individer", "_default"]);
  });
});
