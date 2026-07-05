import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { VariantsResponse } from "./api";
import { getRegisterVariants } from "./api";
import VariantBrowser from "./VariantBrowser.svelte";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getRegisterVariants: vi.fn() };
});

beforeEach(() => {
  vi.mocked(getRegisterVariants).mockReset();
});

describe("VariantBrowser — name/display_group dedupe (D1.4)", () => {
  it("omits display_group when it just repeats the name (the common case)", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [
        {
          slug: "arbetsstallen",
          name: "Arbetsställen",
          display_group: "Arbetsställen",
        },
      ],
    } as unknown as VariantsResponse);

    await render(VariantBrowser, { registerFqid: "scb/lisa" });

    // The name renders exactly once — not "Arbetsställen Arbetsställen".
    await expect
      .element(page.getByText("Arbetsställen", { exact: true }))
      .toBeVisible();
    const matches = page.getByText("Arbetsställen", { exact: true });
    expect(await matches.all()).toHaveLength(1);
  });

  it("treats a trailing-whitespace-only difference as a duplicate (SCB noise)", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [
        {
          slug: "individer-sociala-agi-ku",
          // name carries a trailing space the display_group lacks.
          name: "Individer - sociala AGI/KU ",
          display_group: "Individer - sociala AGI/KU",
        },
      ],
    } as unknown as VariantsResponse);

    await render(VariantBrowser, { registerFqid: "scb/lsum" });

    // Only the `name` span renders; the trimmed-equal display_group is omitted,
    // so the trimmed label appears exactly once (no right-aligned repeat).
    const matches = page.getByText("Individer - sociala AGI/KU", {
      exact: false,
    });
    expect(await matches.all()).toHaveLength(1);
  });

  it("shows display_group when it genuinely differs from the name", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [
        {
          slug: "foretag-kuagg",
          name: "Företag - Uppgifter",
          display_group: "KUAGG aggregat",
        },
      ],
    } as unknown as VariantsResponse);

    await render(VariantBrowser, { registerFqid: "scb/lsum" });

    await expect
      .element(page.getByText("Företag - Uppgifter", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("KUAGG aggregat", { exact: true }))
      .toBeVisible();
  });

  it("renders register-version population and object-type metadata", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [
        {
          slug: "standard",
          name: "Standard",
          display_group: null,
          versions: [
            {
              name: "2019",
              description: "RAMS 2019 description",
              measurement_information: "RAMS measurement information",
              populations: [
                {
                  name: "Employees",
                  definition: "People with employment income",
                  comment: "Fixture population note",
                  date_range: "2019",
                },
                {
                  name: "Employees",
                  definition: "People with employment income, second frame",
                  comment: "Repeated name should not duplicate a Svelte key",
                  date_range: "2020",
                },
              ],
              object_types: [
                { name: "Person", definition: "Individual worker" },
                {
                  name: "Person",
                  definition: "Individual worker, second frame",
                },
              ],
            },
          ],
        },
      ],
    } as unknown as VariantsResponse);

    await render(VariantBrowser, { registerFqid: "scb/rams" });

    await expect.element(page.getByText("Version")).toBeVisible();
    await expect.element(page.getByText("RAMS 2019 description")).toBeVisible();
    await expect
      .element(page.getByText("RAMS measurement information"))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Population" }))
      .toBeVisible();
    expect(await page.getByText("Employees").all()).toHaveLength(2);
    await expect
      .element(page.getByText("People with employment income, second frame"))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Object type" }))
      .toBeVisible();
    expect(await page.getByText("Person").all()).toHaveLength(2);
    await expect
      .element(page.getByText("Individual worker, second frame"))
      .toBeVisible();
  });
});

describe("VariantBrowser — hide the section without a real variant (#673/M4)", () => {
  it("renders NOTHING when the only variant is the synthesized/stored _default", async () => {
    // `_default` is NOT a user-facing variant (it's a stored variant for some
    // registers and the synthesized default for others). A register whose only
    // "variant" is _default has no real variant axis → no section, no heading,
    // no "No variants." text (the M4 complaint: a useless "Variants" heading).
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [{ slug: "_default", name: null, display_group: null }],
    } as unknown as VariantsResponse);

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/sol",
    });

    // No "Variants" heading, no section.
    expect(
      page.getByRole("heading", { name: "Variants" }).elements(),
    ).toHaveLength(0);
    expect(container.querySelector("section.variants")).toBeNull();
  });

  it("renders NOTHING for an empty variant list", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [],
    } as unknown as VariantsResponse);

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/empty",
    });

    expect(
      page.getByRole("heading", { name: "Variants" }).elements(),
    ).toHaveLength(0);
    expect(container.querySelector("section.variants")).toBeNull();
    // The dropped fallback text must not appear either.
    expect(page.getByText("No variants.").elements()).toHaveLength(0);
  });

  it("renders the section for a single real variant", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [{ slug: "individer", name: "Individer", display_group: null }],
    } as unknown as VariantsResponse);

    await render(VariantBrowser, { registerFqid: "scb/lisa" });

    await expect
      .element(page.getByRole("heading", { name: "Variants" }))
      .toBeVisible();
    // The slug span (exact, to avoid also matching the "Individer" name span).
    await expect
      .element(page.getByText("individer", { exact: true }))
      .toBeVisible();
  });

  it("renders the section for a real variant mixed with _default (no _default filtering of the list)", async () => {
    // ≥1 real variant → render the FULL list unchanged; _default is NOT filtered
    // out of a mixed list (out of scope), so it still appears alongside the real
    // one — only the all-_default/empty cases suppress the section.
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [
        { slug: "individer", name: "Individer", display_group: null },
        { slug: "_default", name: null, display_group: null },
      ],
    } as unknown as VariantsResponse);

    await render(VariantBrowser, { registerFqid: "scb/lisa" });

    await expect
      .element(page.getByRole("heading", { name: "Variants" }))
      .toBeVisible();
    // Both slug spans render — the real variant AND _default (mixed lists keep
    // _default; only all-_default/empty lists suppress the section).
    await expect
      .element(page.getByText("individer", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("_default", { exact: true }))
      .toBeVisible();
  });
});

describe("VariantBrowser — variant family folds (#376)", () => {
  it("renders one browse item for concrete variants in the same family", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [
        {
          slug: "individer-16plus",
          name: "Individer, 16 år och äldre",
          display_group: "Individer, 16 år och äldre",
          variant_family: "individer-15plus",
          variant_family_label: "Individer",
        },
        {
          slug: "individer-15plus",
          name: "Individer, 15 år och äldre",
          display_group: "Individer, 15 år och äldre",
          variant_family: "individer-15plus",
          variant_family_label: "Individer",
        },
      ],
    } as unknown as VariantsResponse);

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/lisa",
    });

    await expect
      .element(page.getByText("Individer", { exact: true }))
      .toBeVisible();
    expect(container.querySelectorAll(".variant-list > li")).toHaveLength(1);
    await expect
      .element(page.getByText("individer-16plus", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("individer-15plus", { exact: true }))
      .toBeVisible();
  });
});
