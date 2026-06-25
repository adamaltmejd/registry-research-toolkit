import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationNodeData, ConceptGroup } from "./api";
import ClassificationDimensionsPanel from "./ClassificationDimensionsPanel.svelte";

// The panel renders the EMBEDDED `dimensions` synchronously — no fetch, so no
// mocking. The cross-reference is the curated umbrella group (e.g. group:sun) the
// edition belongs to, rendered through the shared ConceptGroupRow.

// A `group:sun`-shaped umbrella: AXIS-LESS (`axes: []`, #516), three granularity
// members each carrying a curated `{axis: null, label}` facet (the short label).
// ConceptGroupRow must surface those curated labels even with no group axis.
const SUN_GROUP: ConceptGroup = {
  key: "sun",
  label: "Utbildningsnivå",
  source: "curated",
  axes: [],
  members: [
    {
      fqid: "class/sun-niva2020",
      name: "Nivå",
      facets: [{ axis: null, value: "niva", label: "Nivå" }],
    },
    {
      fqid: "class/niva-oldv1",
      name: "Nivå (7 nivåer)",
      facets: [{ axis: null, value: "niva-old", label: "7-nivå" }],
    },
    {
      fqid: "class/niva-grovv1",
      name: "Nivå (5 nivåer)",
      facets: [{ axis: null, value: "niva-grov", label: "5-nivå" }],
    },
  ],
} as unknown as ConceptGroup;

function node(dimensions: ConceptGroup[]): ClassificationNodeData {
  return {
    kind: "classification",
    fqid: "class/sun-niva2020",
    name: "Utbildningsnivå",
    short_name: "SUN-NIVA2020",
    edition_chain: [],
    codes: [],
    dimensions,
  } as unknown as ClassificationNodeData;
}

describe("ClassificationDimensionsPanel — niva ↔ aggregate cross-reference (#609)", () => {
  it("omits the whole section when the classification is in no umbrella group", async () => {
    await render(ClassificationDimensionsPanel, { node: node([]) });
    await expect
      .element(page.getByRole("heading", { name: "Related granularities" }))
      .not.toBeInTheDocument();
  });

  it("renders the umbrella group with its sibling granularities", async () => {
    await render(ClassificationDimensionsPanel, { node: node([SUN_GROUP]) });

    await expect
      .element(page.getByRole("heading", { name: "Related granularities" }))
      .toBeVisible();
    // ConceptGroupRow's summary surfaces the group label + member count noun.
    await expect.element(page.getByText("Utbildningsnivå")).toBeVisible();
    await expect.element(page.getByText("3 granularities")).toBeVisible();
    // The members fold into a closed <details> (ConceptGroupRow's idiom) — expand
    // it, then the curated facet-label chips link to each sibling. This is the
    // regression: with `axes: []` the curated labels (not bare slugs) must show.
    await page.getByText("3 granularities").click();
    await expect
      .element(page.getByRole("link", { name: "7-nivå" }))
      .toHaveAttribute("href", "/catalog/class/niva-oldv1");
  });
});
