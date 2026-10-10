import { describe, expect, it } from "vitest";
import {
  clampYearWindow,
  coverageBandEdges,
  intersectCoverageWindow,
  nextResolutionQuery,
  normalizePeriodRows,
  notDeliveredGaps,
  periodCoverageUnion,
  resolveYearEntry,
  yearSegmentsFromWire,
  yearWindowFromWire,
} from "./period";

// The period grammar, wire shaping, days and render are reg-core's: their cases
// live in conformance/cases/grammar/ (period.jsonl, source_period.jsonl), which
// reg_core.test.ts runs through the WASM module. This file pins the SPA's own
// composition of those answers.

describe("nextResolutionQuery (resolution-merge rule)", () => {
  it("clearing the period DROPS the variant/value_set_version modifiers", () => {
    // ?variant / ?value_set_version are inert without ?period (the server
    // 422s them), so clearing the period yields the empty query (full history).
    const current = {
      period: "2020",
      variant: "x",
      value_set_version: "y",
    };
    expect(nextResolutionQuery(current, { period: null })).toBe("");
    expect(nextResolutionQuery(current, { period: "" })).toBe("");
  });

  it("an undefined field inherits; an explicit empty string clears that field", () => {
    const current = { period: "2020", variant: "x" };
    // variant undefined → inherited
    expect(nextResolutionQuery(current, { period: "2021" })).toBe(
      "period=2021&variant=x",
    );
    // variant "" → cleared (but the period survives)
    expect(nextResolutionQuery(current, { variant: "" })).toBe("period=2020");
  });
});

// ── #615 year-window slider helpers ──────────────────────────────────────────

describe("yearWindowFromWire (?period wire → year window | null)", () => {
  it("a sub-annual token / _default / list / junk → null (belongs to the expander)", () => {
    expect(yearWindowFromWire("HT2020")).toBeNull();
    expect(yearWindowFromWire("2020-Q3")).toBeNull();
    expect(yearWindowFromWire("2020-08")).toBeNull();
    expect(yearWindowFromWire("_default")).toBeNull();
    expect(yearWindowFromWire("2005..2010,2015..2020")).toBeNull();
    expect(yearWindowFromWire("nonsense")).toBeNull();
  });

  it("a mixed range with a sub-annual endpoint → null", () => {
    expect(yearWindowFromWire("VT2010..2020")).toBeNull();
    expect(yearWindowFromWire("2010..2020-08")).toBeNull();
  });

  it("an inverted year range (to < from) → null", () => {
    expect(yearWindowFromWire("2020..2010")).toBeNull();
  });
});

describe("yearSegmentsFromWire (Y-101 list-row seed)", () => {
  it("a token ANYWHERE in the list makes the whole period unrepresentable", () => {
    expect(yearSegmentsFromWire("HT2018")).toBeNull();
    expect(yearSegmentsFromWire("2015..2017,HT2018")).toBeNull();
  });
});

describe("clampYearWindow", () => {
  it("keeps from <= to after clamping (a fully-out-of-range window collapses)", () => {
    const w = clampYearWindow({ from: 2030, to: 2040 }, 1960, 2026);
    expect(w.from).toBeLessThanOrEqual(w.to);
    expect(w).toEqual({ from: 2026, to: 2026 });
  });
});

describe("intersectCoverageWindow (#671 coverage-aware seed)", () => {
  it("partial overlap → the overlapping span", () => {
    expect(
      intersectCoverageWindow(
        { from: 1995, to: 2015 },
        { from: 1990, to: 2005 },
        1960,
        2026,
      ),
    ).toEqual({ from: 1995, to: 2005 });
  });

  it("no coverage but a SET window → the window (a stateless variable honours its window, Fix A)", () => {
    // FIX A: coverage null + a window must seed at the window, NOT widen to full
    // history — else a stateless variable's Apply would submit 1960..vintage.
    expect(
      intersectCoverageWindow(null, { from: 2000, to: 2010 }, 1960, 2026),
    ).toEqual({ from: 2000, to: 2010 });
  });

  it("window wholly BEFORE coverage → snaps to the coverage start", () => {
    const seed = intersectCoverageWindow(
      { from: 2000, to: 2010 },
      { from: 1980, to: 1990 },
      1960,
      2026,
    );
    expect(seed.from).toBeLessThanOrEqual(seed.to);
    expect(seed).toEqual({ from: 2000, to: 2000 });
  });

  it("INVERTED effective coverage WITH a window → the window (inverted coverage is no coverage; mirrors slider Fix D)", () => {
    // Same inverted case but a window is set: treated as no coverage, so the seed
    // is the bare window (the stateless-variable rule), never the manufactured span.
    expect(
      intersectCoverageWindow(
        { from: 2025, to: null },
        { from: 2000, to: 2010 },
        1960,
        2024,
      ),
    ).toEqual({ from: 2000, to: 2010 });
  });
});

describe("notDeliveredGaps (selection minus coverage)", () => {
  it("both leading and trailing gaps", () => {
    expect(
      notDeliveredGaps({ from: 1990, to: 2020 }, { from: 2000, to: 2010 }),
    ).toEqual([
      { from: 1990, to: 1999 },
      { from: 2011, to: 2020 },
    ]);
  });

  it("a selection entirely outside (before) coverage is one gap", () => {
    expect(
      notDeliveredGaps({ from: 1980, to: 1990 }, { from: 2000, to: 2010 }),
    ).toEqual([{ from: 1980, to: 1990 }]);
  });
});

describe("periodCoverageUnion (#992 find-or-create period extension)", () => {
  it("coalesces year periods into a sorted list, adjacent years joined", () => {
    expect(
      periodCoverageUnion({ from: 2015, to: 2020 }, { from: 2005, to: 2010 }),
    ).toEqual([
      { from: 2005, to: 2010 },
      { from: 2015, to: 2020 },
    ]);
    expect(
      periodCoverageUnion(
        [
          { from: 2005, to: 2010 },
          { from: 2015, to: 2020 },
        ],
        { from: 2011, to: 2014 },
      ),
    ).toEqual({ from: 2005, to: 2020 });
    // A numeric-STRING year coalesces like the int form (Fix 1).
    expect(periodCoverageUnion("2020", 2022)).toEqual([2020, 2022]);
  });

  it("joins day-adjacent sub-annual periods and renders the union's days", () => {
    expect(periodCoverageUnion("2019-Q1", "2019-Q2")).toBe("VT2019");
    expect(
      periodCoverageUnion("2019", { from: "2020-01", to: "2020-06" }),
    ).toEqual({ from: 2019, to: "2020-06-30" });
  });

  it("an add the existing token period already covers leaves it as written", () => {
    expect(periodCoverageUnion({ from: "LA2004", to: "LA2005" }, 2005)).toEqual(
      {
        from: "LA2004",
        to: "LA2005",
      },
    );
  });

  it("an UNSET side yields the other; an undated side lets the incoming win (Fix 2)", () => {
    expect(periodCoverageUnion(2020, "")).toBe(2020);
    expect(periodCoverageUnion([], 2020)).toBe(2020);
    expect(periodCoverageUnion("", { from: 2010, to: 2015 })).toEqual({
      from: 2010,
      to: 2015,
    });
    expect(periodCoverageUnion(2018, "_default")).toBe("_default");
  });
});

describe("normalizePeriodRows (Y-101 period-row list → Period)", () => {
  it("disjoint rows sort ascending, out of the order they were entered", () => {
    expect(
      normalizePeriodRows([
        { from: 2019, to: 2020 },
        { from: 2015, to: 2017 },
      ]),
    ).toEqual({
      period: [
        { from: 2015, to: 2017 },
        { from: 2019, to: 2020 },
      ],
    });
  });

  it("adjacency-merges TOUCHING rows into one span, same as periodCoverageUnion", () => {
    expect(
      normalizePeriodRows([
        { from: 2010, to: 2011 },
        { from: 2012, to: 2013 },
      ]),
    ).toEqual({ period: { from: 2010, to: 2013 } });
  });

  it("reports the first overlapping pair by ORIGINAL row index, not sorted position", () => {
    expect(
      normalizePeriodRows([
        { from: 2019, to: 2021 },
        { from: 2015, to: 2016 },
        { from: 2020, to: 2022 },
      ]),
    ).toEqual({ a: 0, b: 2 });
  });
});

describe("coverageBandEdges (#671 selectable band / #631 vintage cap)", () => {
  it("an open START runs to the track floor", () => {
    expect(
      coverageBandEdges({ from: null, to: 2008 }, 1960, 2026, 2024),
    ).toEqual({ from: 1960, to: 2008 });
  });

  it("without a vintage an open END falls back to the track edge", () => {
    expect(coverageBandEdges({ from: 1995, to: null }, 1960, 2026)).toEqual({
      from: 1995,
      to: 2026,
    });
  });
});

describe("resolveYearEntry (Y-16/Y-81 exact-year entry, hoisted Y-100)", () => {
  it("without a band: badly-typed text refuses with the default century wording", () => {
    expect(resolveYearEntry("20x", "2020")).toEqual({
      problem: "From must be a four-digit year, 1900 to 2099.",
      at: { from: true, to: false },
    });
    expect(resolveYearEntry("", "")).toEqual({
      problem: "From and To must each be a four-digit year, 1900 to 2099.",
      at: { from: true, to: true },
    });
  });

  it("without a band: a year outside 1900-2099 refuses the same as badly-typed (no band to be 'outside')", () => {
    expect(resolveYearEntry("2100", "2100")).toEqual({
      problem: "From and To must each be a four-digit year, 1900 to 2099.",
      at: { from: true, to: true },
    });
  });
});
