import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ConceptGroup } from "./api";
import DimensionsPanel from "./DimensionsPanel.svelte";

// PRESENTATIONAL since #670: the `/dimensions` fetch moved up to BindingLeafView
// (which shares the resource with the header qualifier + group link), so the
// panel now takes resolved `groups` + `loading` + `error` props — no api mock.

const inkomst: ConceptGroup = {
  key: "ink",
  label: "Inkomst",
  source: "token",
  axes: [{ name: "month", label: "month" }],
  members: [
    {
      fqid: "scb/rams/inkjan",
      name: "Inkomst",
      facets: [{ axis: "month", value: "01", label: "januari" }],
    },
    {
      fqid: "scb/rams/inkfeb",
      name: "Inkomst",
      facets: [{ axis: "month", value: "02", label: "februari" }],
    },
  ],
} as unknown as ConceptGroup;

describe("DimensionsPanel (#489 / #670 presentational)", () => {
  it("renders a passed concept group via ConceptGroupRow", async () => {
    await render(DimensionsPanel, {
      groups: [inkomst],
      loading: false,
      error: null,
    });

    await expect
      .element(page.getByRole("heading", { name: "Variants / dimensions" }))
      .toBeVisible();
    // The group's label + count come from ConceptGroupRow's <summary>.
    await expect.element(page.getByText("Inkomst")).toBeVisible();
    await expect.element(page.getByText("2 variables")).toBeVisible();
  });

  it("omits the whole section when the variable is in no group", async () => {
    await render(DimensionsPanel, { groups: [], loading: false, error: null });

    await expect
      .element(page.getByRole("heading", { name: "Variants / dimensions" }))
      .not.toBeInTheDocument();
  });

  it("shows the aria-busy loading line while the parent's fetch is pending", async () => {
    await render(DimensionsPanel, { groups: [], loading: true, error: null });

    await expect
      .element(page.getByRole("heading", { name: "Variants / dimensions" }))
      .toBeVisible();
    await expect.element(page.getByText("Loading…")).toBeVisible();
    expect(document.querySelector('[aria-busy="true"]')).not.toBeNull();
  });

  it("keeps the section visible with an inline error when the fetch failed", async () => {
    await render(DimensionsPanel, {
      groups: [],
      loading: false,
      error: "backend down",
    });

    // A fetch failure must NOT silently collapse the section (that would read as
    // "in no group"); it stays visible with the inline error.
    await expect
      .element(page.getByRole("heading", { name: "Variants / dimensions" }))
      .toBeVisible();
    await expect
      .element(page.getByText(/Failed to load dimensions/))
      .toBeVisible();
  });
});
