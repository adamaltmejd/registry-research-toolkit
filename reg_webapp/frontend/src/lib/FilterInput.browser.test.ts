import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import FilterInput from "./FilterInput.svelte";

describe("FilterInput", () => {
  it("shows the result count only while filtering", async () => {
    // Empty value → plain full list, no count.
    const { rerender } = render(FilterInput, {
      value: "",
      total: 740,
      shown: 740,
      label: "Filter variables",
    });
    await expect.element(page.getByText("of 740")).not.toBeInTheDocument();

    // A non-empty value → the "12 of 740" count surfaces.
    await rerender({ value: "lon", total: 740, shown: 12 });
    await expect.element(page.getByText("12 of 740")).toBeVisible();
  });

  it("autofocuses the input when asked", async () => {
    render(FilterInput, {
      value: "",
      total: 1,
      shown: 1,
      autofocus: true,
      label: "Filter variables",
    });
    const input = page.getByRole("textbox", { name: "Filter variables" });
    await expect.element(input).toBeVisible();
    // The {@attach} focus hook ran on mount.
    await vi.waitFor(() =>
      expect(input.element()).toBe(document.activeElement),
    );
  });

  it("propagates typed text via the bound value", async () => {
    render(FilterInput, { value: "", total: 1, shown: 1, label: "Filter" });
    const input = page.getByRole("textbox", { name: "Filter" });
    await input.fill("kon");
    await expect.element(input).toHaveValue("kon");
  });
});
