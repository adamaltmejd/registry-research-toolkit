import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { getRegisterVariants } from "./api";
import VariantBrowser from "./VariantBrowser.svelte";
import {
  datedVersions,
  lisaVersion,
  variant,
  variantsResponse,
} from "./variants-test-helpers";

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getRegisterVariants: vi.fn() };
});

beforeEach(() => {
  vi.mocked(getRegisterVariants).mockReset();
});

// The fold and grouping RULES are unit-tested in `variants.test.ts`; these
// assert what the page does with them.

describe("VariantBrowser — folded versions (Y-79)", () => {
  it("renders an unchanged run as ONE block, naming the delivery it prints", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("individer-16plus", {
          name: "Individer, 16 år och äldre",
          versions: [
            lisaVersion(2007, "16 år och äldre"),
            lisaVersion(2008, "16 år och äldre"),
            lisaVersion(2009, "16 år och äldre"),
          ],
        }),
      ),
    );

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/lisa",
    });

    await expect
      .element(page.getByRole("heading", { name: "Versions 2007–2009" }))
      .toBeVisible();
    expect(container.querySelectorAll("section.version-meta")).toHaveLength(1);
    // The block keeps the first delivery's prose — printed once, not three
    // times — and says so, since that prose names 2007 under a 2007–2009 head.
    expect(
      await page
        .getByText("LISA 2007 innehåller uppgifter om individer.")
        .all(),
    ).toHaveLength(1);
    await expect
      .element(page.getByText("Wording as delivered for 2007."))
      .toBeVisible();
  });

  it("opens a new block on a genuinely changed population, keeping both frames", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("individer", {
          name: "Individer",
          versions: [
            lisaVersion(2008, "16 år och äldre"),
            lisaVersion(2009, "16 år och äldre"),
            // The 2010 population change — the one thing worth reading.
            lisaVersion(2010, "15 år och äldre"),
            lisaVersion(2011, "15 år och äldre"),
          ],
        }),
      ),
    );

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/lisa",
    });

    await expect
      .element(page.getByRole("heading", { name: "Versions 2008–2009" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Versions 2010–2011" }))
      .toBeVisible();
    expect(container.querySelectorAll("section.version-meta")).toHaveLength(2);
    await expect
      .element(
        page.getByText(
          "Samtliga individer 16 år och äldre folkbokförda 2008-12-31.",
        ),
      )
      .toBeVisible();
    await expect
      .element(
        page.getByText(
          "Samtliga individer 15 år och äldre folkbokförda 2010-12-31.",
        ),
      )
      .toBeVisible();
  });

  it("labels a lone delivery with its own year and prints it unqualified", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("standard", {
          name: "Standard",
          versions: [lisaVersion(2019, "16 år och äldre")],
        }),
      ),
    );

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/rams",
    });

    await expect
      .element(page.getByRole("heading", { name: "Version 2019" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Population" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Object type" }))
      .toBeVisible();
    // Nothing was folded, so there is no "which delivery is this" caveat.
    expect(container.querySelectorAll("p.as-delivered")).toHaveLength(0);
  });

  it("renders every population and object type of a version, repeated names included", async () => {
    // One delivery can carry two frames, and reg_meta may deliver both under
    // the SAME name — the metadata loops are unkeyed precisely because a name
    // is not an identity here, so neither row may collapse into the other.
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("standard", {
          name: "Standard",
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
        }),
      ),
    );

    await render(VariantBrowser, { registerFqid: "scb/rams" });

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

  it("keeps a text-free delivery visible instead of folding its neighbours across it", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("standard", {
          name: "Standard",
          versions: [
            lisaVersion(2018, "16 år och äldre"),
            // Delivered, named, but carrying no wording of its own.
            ...datedVersions(2019),
            lisaVersion(2020, "16 år och äldre"),
          ],
        }),
      ),
    );

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/lisa",
    });

    // 2019 is still there to be found…
    for (const year of ["2018", "2019", "2020"]) {
      await expect
        .element(page.getByRole("heading", { name: `Version ${year}` }))
        .toBeVisible();
    }
    // …and its neighbours did not fold across it into one 2018–2020 block that
    // would put words in its mouth.
    expect(container.querySelectorAll("section.version-meta")).toHaveLength(3);
  });
});

describe("VariantBrowser — variant family segments (#376/Y-79)", () => {
  it("renders a family as one entry with its segments and each segment's population", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        // The catalog orders variants by SLUG, so the successor arrives FIRST —
        // the segments must still read oldest delivery first.
        variant("individer-15plus", {
          name: "Individer, 15 år och äldre",
          display_group: "Individer, 15 år och äldre",
          variant_family: "individer-15plus",
          variant_family_label: "Individer",
          versions: [
            lisaVersion(2010, "15 år och äldre"),
            lisaVersion(2011, "15 år och äldre"),
          ],
        }),
        variant("individer-16plus", {
          name: "Individer, 16 år och äldre",
          display_group: "Individer, 16 år och äldre",
          variant_family: "individer-15plus",
          variant_family_label: "Individer",
          versions: [
            lisaVersion(1990, "16 år och äldre"),
            lisaVersion(1991, "16 år och äldre"),
          ],
        }),
      ),
    );

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/lisa",
    });

    // ONE entry, headed by the family label.
    await expect
      .element(page.getByRole("heading", { name: "Individer", exact: true }))
      .toBeVisible();
    expect(container.querySelectorAll("li.variant-entry")).toHaveLength(1);
    // Its segments, oldest first, each on its OWN line with the years it was
    // delivered — the boundary is markup, not a separator glyph.
    const segments = [...container.querySelectorAll("ul.entry-segments > li")];
    expect(
      segments.map((li) => li.textContent?.replace(/\s+/g, " ").trim()),
    ).toEqual(["16 år och äldre 1990–1991", "15 år och äldre 2010–2011"]);
    expect(
      segments.map((li) => li.querySelector("span.years")?.textContent),
    ).toEqual(["1990–1991", "2010–2011"]);
    // Both concrete slugs stay visible — a project source extracts one of them.
    await expect
      .element(page.getByText("individer-16plus", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("individer-15plus", { exact: true }))
      .toBeVisible();
    // And each segment carries its own population definition, so the changed
    // frame reads as one variant with two frames.
    await expect
      .element(
        page.getByText(
          "Samtliga individer 16 år och äldre folkbokförda 1990-12-31.",
        ),
      )
      .toBeVisible();
    await expect
      .element(
        page.getByText(
          "Samtliga individer 15 år och äldre folkbokförda 2010-12-31.",
        ),
      )
      .toBeVisible();
  });

  it("omits display_group when it just repeats the name (the common case)", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("arbetsstallen", {
          name: "Arbetsställen",
          display_group: "Arbetsställen",
          versions: datedVersions(2004),
        }),
      ),
    );

    await render(VariantBrowser, { registerFqid: "scb/lisa" });

    // The name renders exactly once — not "Arbetsställen Arbetsställen".
    const matches = page.getByText("Arbetsställen", { exact: true });
    await expect.element(matches).toBeVisible();
    expect(await matches.all()).toHaveLength(1);
  });

  it("heads a variant that has no name with its display_group, printed once", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("kuagg", {
          // No `name`, so the entry heading itself falls back to the group —
          // which must not then print a second time as the entry's meta line.
          display_group: "KUAGG aggregat",
          versions: datedVersions(2004),
        }),
      ),
    );

    const { container } = await render(VariantBrowser, {
      registerFqid: "scb/lsum",
    });

    await expect
      .element(page.getByRole("heading", { name: "KUAGG aggregat" }))
      .toBeVisible();
    expect(
      await page.getByText("KUAGG aggregat", { exact: true }).all(),
    ).toHaveLength(1);
    expect(container.querySelectorAll("p.entry-meta")).toHaveLength(0);
  });

  it("shows display_group when it genuinely differs from the name", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse(
        variant("foretag-kuagg", {
          name: "Företag - Uppgifter",
          display_group: "KUAGG aggregat",
        }),
      ),
    );

    await render(VariantBrowser, { registerFqid: "scb/lsum" });

    await expect
      .element(page.getByText("Företag - Uppgifter", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("KUAGG aggregat", { exact: true }))
      .toBeVisible();
  });
});

describe("VariantBrowser — page states (Y-79)", () => {
  it("says a register has no variants and points back at it", async () => {
    // The page is deep-linkable, so an empty list is a real state here — unlike
    // the register page's summary, which suppresses itself entirely.
    vi.mocked(getRegisterVariants).mockResolvedValue(variantsResponse());

    await render(VariantBrowser, { registerFqid: "scb/empty" });

    await expect
      .element(page.getByText("No variants for this register."))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "Back to the register" }))
      .toHaveAttribute("href", "/catalog/scb/empty");
  });

  it("surfaces a failed load as an alert", async () => {
    vi.mocked(getRegisterVariants).mockRejectedValue(new Error("boom"));

    await render(VariantBrowser, { registerFqid: "scb/lisa" });

    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("Failed to load variants: Error: boom");
  });
});
