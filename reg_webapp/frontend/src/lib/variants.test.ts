import { describe, expect, it } from "vitest";
import { foldVersions, groupVariants, showsDistinctGroup } from "./variants";
import { datedVersions, lisaVersion, variant } from "./variants-test-helpers";

describe("foldVersions", () => {
  it("folds consecutive deliveries that differ only in the year they name", () => {
    const blocks = foldVersions([
      lisaVersion(2007, "16 år och äldre"),
      lisaVersion(2008, "16 år och äldre"),
      lisaVersion(2009, "16 år och äldre"),
    ]);

    expect(blocks).toHaveLength(1);
    expect(blocks[0].label).toBe("2007–2009");
    expect(blocks[0].count).toBe(3);
    // The block carries the FIRST delivery verbatim — nothing is rewritten.
    expect(blocks[0].version.description).toBe(
      "LISA 2007 innehåller uppgifter om individer.",
    );
  });

  it("opens a new block on a genuinely changed population", () => {
    const blocks = foldVersions([
      lisaVersion(2008, "16 år och äldre"),
      lisaVersion(2009, "16 år och äldre"),
      // The 2010 population change — the one thing worth reading.
      lisaVersion(2010, "15 år och äldre"),
      lisaVersion(2011, "15 år och äldre"),
    ]);

    expect(blocks.map((block) => block.label)).toEqual([
      "2008–2009",
      "2010–2011",
    ]);
  });

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

  it("passes over a delivery that carries no text, so it can't split a run", () => {
    const blocks = foldVersions([
      lisaVersion(2018, "16 år och äldre"),
      ...datedVersions(2019),
      lisaVersion(2020, "16 år och äldre"),
    ]);

    expect(blocks).toHaveLength(1);
    expect(blocks[0].label).toBe("2018–2020");
    // The skipped delivery is not counted as a member of the run.
    expect(blocks[0].count).toBe(2);
  });

  it("labels a lone delivery with its own year, and an undated one by name", () => {
    expect(foldVersions([lisaVersion(2019, "16 år och äldre")])[0].label).toBe(
      "2019",
    );
    expect(
      foldVersions([
        { ...lisaVersion(2019, "16 år och äldre"), name: "Senaste versionen" },
      ])[0].label,
    ).toBe("Senaste versionen");
  });

  it("does not mistake a longer digit run for a year", () => {
    // Mirrors reg_meta's `extract_year`: `12019` and `20190101` are not years,
    // so two deliveries naming them are NOT year-equivalent and stay apart.
    const blocks = foldVersions([
      { ...lisaVersion(2007, "16 år och äldre"), name: "20190101" },
      { ...lisaVersion(2007, "16 år och äldre"), name: "20200101" },
    ]);

    expect(blocks).toHaveLength(1);
    expect(blocks[0].label).toBe("20190101");
  });
});

describe("groupVariants", () => {
  it("keeps an ordinary variant as its own single-segment entry", () => {
    const groups = groupVariants([
      variant("arbetsstallen", {
        name: "Arbetsställen",
        versions: datedVersions(2004, 2005, 2006),
      }),
    ]);

    expect(groups).toHaveLength(1);
    expect(groups[0]).toMatchObject({
      key: "arbetsstallen",
      label: "Arbetsställen",
      span: "2004–2006",
      isFamily: false,
    });
    expect(groups[0].segments).toHaveLength(1);
    expect(groups[0].segments[0].span).toBe("2004–2006");
  });

  it("collects a succession family into ONE entry, oldest delivery first", () => {
    // The catalog orders variants by SLUG, so the successor arrives FIRST.
    const groups = groupVariants([
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
    ]);

    expect(groups).toHaveLength(1);
    expect(groups[0]).toMatchObject({
      label: "Individer",
      span: "1990–2023",
      isFamily: true,
    });
    // Segments read oldest first, each naming only what the family label doesn't.
    expect(
      groups[0].segments.map((segment) => `${segment.name} ${segment.span}`),
    ).toEqual(["16 år och äldre 1990–2009", "15 år och äldre 2010–2023"]);
  });

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

  it("falls back through name → display_group → slug for an entry label", () => {
    expect(groupVariants([variant("kuagg")])[0].label).toBe("kuagg");
    expect(
      groupVariants([variant("kuagg", { display_group: "KUAGG aggregat" })])[0]
        .label,
    ).toBe("KUAGG aggregat");
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
