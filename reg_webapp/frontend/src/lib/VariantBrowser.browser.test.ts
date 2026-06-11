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
});
