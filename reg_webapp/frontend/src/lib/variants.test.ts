import { describe, expect, it } from "vitest";
import { foldVersions, groupVariants, showsDistinctGroup } from "./variants";
import { datedVersions, lisaVersion, variant } from "./variants-test-helpers";

describe("foldVersions", () => {
  it("re-opens a block when a changed text later reverts", () => {
    // Runs are CONSECUTIVE: an identical text on both sides of a change stays
    // two blocks, so the delivered order survives the fold.
    const blocks = foldVersions([
      lisaVersion(2008, "16 år och äldre"),
      lisaVersion(2009, "15 år och äldre"),
      lisaVersion(2010, "16 år och äldre"),
    ]);

    expect(blocks.map((block) => block.label)).toEqual([
      "2008",
      "2009",
      "2010",
    ]);
  });

  it("keeps two deliveries apart when their names differ by more than a year", () => {
    // Identical prose, distinct deliveries. Only a YEAR difference folds:
    // collapsing these would print one name for both.
    const blocks = foldVersions([
      { ...lisaVersion(2007, "16 år och äldre"), name: "Preliminär 2007" },
      { ...lisaVersion(2007, "16 år och äldre"), name: "Slutlig 2007" },
    ]);

    expect(blocks.map((block) => block.version.name)).toEqual([
      "Preliminär 2007",
      "Slutlig 2007",
    ]);
  });

  it("does not mistake a longer digit run for a year", () => {
    // Mirrors reg_meta's `extract_year`: `20190101` and `20200101` carry no
    // year, so these two are NOT year-equivalent — two blocks, each labelled by
    // the name its delivery came under.
    const blocks = foldVersions([
      { ...lisaVersion(2007, "16 år och äldre"), name: "20190101" },
      { ...lisaVersion(2007, "16 år och äldre"), name: "20200101" },
    ]);

    expect(blocks.map((block) => block.label)).toEqual([
      "20190101",
      "20200101",
    ]);
  });
});

describe("groupVariants", () => {
  it("keeps a segment's full name when it doesn't extend the family label", () => {
    // A non-uniform family takes the first label as its label (reg_meta's
    // `_variant_family_label` fallback), so a member may share no stem with it.
    const groups = groupVariants([
      variant("foretag", {
        name: "Företag",
        variant_family: "foretag",
        variant_family_label: "Företag",
        versions: datedVersions(2001),
      }),
      variant("arbetsstallen", {
        name: "Arbetsställen",
        variant_family: "foretag",
        variant_family_label: "Företag",
        versions: datedVersions(2002),
      }),
    ]);

    expect(groups[0].segments.map((segment) => segment.name)).toEqual([
      "Företag",
      "Arbetsställen",
    ]);
  });

  it("sorts a variant whose versions name no year after every dated sibling", () => {
    const groups = groupVariants([
      variant("undated", {
        name: "Familj, odaterad",
        variant_family: "dated",
        variant_family_label: "Familj",
        versions: [],
      }),
      variant("dated", {
        name: "Familj, daterad",
        variant_family: "dated",
        variant_family_label: "Familj",
        versions: datedVersions(1998),
      }),
    ]);

    expect(groups[0].segments.map((segment) => segment.variant.slug)).toEqual([
      "dated",
      "undated",
    ]);
    expect(groups[0].segments[1].span).toBe("");
    expect(groups[0].span).toBe("1998");
  });
});

describe("showsDistinctGroup", () => {
  it("hides a display_group that just repeats the name, whitespace and all", () => {
    expect(showsDistinctGroup("Arbetsställen", "Arbetsställen")).toBe(false);
    // SCB delivers trailing-whitespace noise on one side of the pair.
    expect(
      showsDistinctGroup(
        "Individer - sociala AGI/KU ",
        "Individer - sociala AGI/KU",
      ),
    ).toBe(false);
    expect(showsDistinctGroup("Företag - Uppgifter", "KUAGG aggregat")).toBe(
      true,
    );
    expect(showsDistinctGroup("Arbetsställen", null)).toBe(false);
  });
});
