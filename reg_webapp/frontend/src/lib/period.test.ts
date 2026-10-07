import { describe, expect, it } from "vitest";
import {
  clampYearWindow,
  coverageBandEdges,
  intersectCoverageWindow,
  isStructurallyValidPeriodWire,
  looksLikePeriod,
  mergePeriods,
  nextResolutionQuery,
  normalizePeriodRows,
  notDeliveredGaps,
  periodFromWire,
  periodTokenBounds,
  periodTokenForBounds,
  periodToWire,
  periodWireBounds,
  periodYearIntervals,
  resolveYearEntry,
  yearSegmentsFromWire,
  yearWindowFromWire,
} from "./period";

describe("periodToWire (Source.period → ?period wire string)", () => {
  it("a token string → trimmed; blank → null", () => {
    expect(periodToWire("2020-Q1")).toBe("2020-Q1");
    expect(periodToWire("  2019  ")).toBe("2019");
    expect(periodToWire("")).toBeNull();
    expect(periodToWire("   ")).toBeNull();
  });

  it("a {from,to} range → from..to; a blank endpoint → null", () => {
    expect(periodToWire({ from: 2018, to: 2020 })).toBe("2018..2020");
    expect(periodToWire({ from: "2018", to: "2020" })).toBe("2018..2020");
    expect(periodToWire({ from: "", to: 2020 })).toBeNull();
  });

  it("a list with a malformed/blank member (or empty list) → null", () => {
    expect(periodToWire([])).toBeNull();
    expect(periodToWire([2018, ""])).toBeNull();
    expect(periodToWire([{ from: "", to: 2020 }])).toBeNull();
  });
});

describe("periodFromWire (?period wire string → Source.period, C1 prefill)", () => {
  it("a token-endpoint range becomes the {from,to} object (the only valid range shape for Source.period)", () => {
    expect(periodFromWire("HT2018..VT2019")).toEqual({
      from: "HT2018",
      to: "VT2019",
    });
    expect(periodFromWire("2019-03..2019-06")).toEqual({
      from: "2019-03",
      to: "2019-06",
    });
    // The #306 succession auto-split's mixed-grain clips.
    expect(periodFromWire("1992..2009-06-30")).toEqual({
      from: 1992,
      to: "2009-06-30",
    });
    expect(periodFromWire("VT1992..2009")).toEqual({
      from: "VT1992",
      to: 2009,
    });
  });

  it("a malformed multi-separator string stays the raw string (the backend flags it)", () => {
    expect(periodFromWire("2018..2019..2020")).toBe("2018..2019..2020");
  });

  it("a non-grammar 'year' is NOT coerced to int — it rides as a string the validator flags", () => {
    // int Source.period passes reg_schema's int-literal arm unchecked, so a
    // typo like "202" must stay a string for the grammar check to catch.
    expect(periodFromWire("202")).toBe("202");
    expect(periodFromWire("202..2009")).toEqual({ from: "202", to: 2009 });
    expect(periodFromWire("3000")).toBe("3000");
  });

  it("round-trips a single year and ranges (int + token endpoints) through periodToWire", () => {
    expect(periodToWire(periodFromWire("2018"))).toBe("2018");
    expect(periodToWire(periodFromWire("2010..2020"))).toBe("2010..2020");
    expect(periodToWire(periodFromWire("VT1992..2009"))).toBe("VT1992..2009");
    expect(periodToWire(periodFromWire("1992..2009-06-30"))).toBe(
      "1992..2009-06-30",
    );
  });

  it("a malformed comma wire (blank member) stays the raw string", () => {
    expect(periodFromWire("2018,")).toBe("2018,");
    expect(periodFromWire(",2018")).toBe(",2018");
  });
});

// periodFromTokenText was retired in the #308 merge: the editor's token-mode
// emission threads through periodFromWire (which carries the #307 comma-list
// arm AND the schema-valid scalar shaping), so the wire mapper below is the
// single emit path. List/blank-member coverage lives on periodFromWire.
describe("periodFromWire #307 list arm", () => {
  it("comma text becomes the segment list (members shaped like scalars)", () => {
    expect(periodFromWire("2005..2010, 2015..2020")).toEqual([
      { from: 2005, to: 2010 },
      { from: 2015, to: 2020 },
    ]);
  });
});

describe("looksLikePeriod (advisory period grammar)", () => {
  const accepted = [
    "2020",
    "1999",
    "2020-01",
    "2020-12",
    "2020-12-31",
    "HT2020",
    "VT2020",
    "LA2004",
    "2020-Q1",
    "2020-Q4",
    "2020-H1",
    "2020-H2",
    "2020-02-29", // 2020 IS a leap year — a real Feb 29
    "2018..2020",
    "2020-Q1..2020-Q4",
    "_default",
    "  2020  ", // tolerates surrounding whitespace
    "2005..2010,2015..2020", // #307 interrupted-series list wire
    "2018, HT2020", // list members tolerate surrounding whitespace
  ];
  for (const value of accepted) {
    it(`accepts ${JSON.stringify(value)}`, () => {
      expect(looksLikePeriod(value)).toBe(true);
    });
  }

  const rejected = [
    "", // empty
    "  ", // blank
    "abc", // junk
    "20", // too-short year
    "20200", // too-long
    "2020-13", // bad month
    "2020-Q5", // bad quarter
    "2020-H3", // bad half
    "XT2020", // bad term prefix
    "LA", // school-year prefix without its starting year
    "2020-2021", // a dash range is NOT the `..` range grammar
    "2018..2019..2020", // two separators
    "2018..", // missing endpoint
    "_default..2020", // `_default` is not a range endpoint
    "2020; DROP TABLE", // SQLi probe shape
    "../etc/passwd", // traversal probe
    "2019-02-29", // calendar-impossible: 2019 is NOT a leap year
    "2018-02-30", // February never has 30 days
    "2021-04-31", // April has 30 days
    "2018,", // list with a blank member
    "2018,_default", // `_default` is not a list segment
    "2018,abc", // list with a junk member
  ];
  for (const value of rejected) {
    it(`rejects ${JSON.stringify(value)}`, () => {
      expect(looksLikePeriod(value)).toBe(false);
    });
  }
});

describe("isStructurallyValidPeriodWire", () => {
  it("accepts sorted non-overlapping period wires", () => {
    expect(isStructurallyValidPeriodWire("2020")).toBe(true);
    expect(isStructurallyValidPeriodWire("2019-03..2019-06")).toBe(true);
    expect(isStructurallyValidPeriodWire("LA2004..LA2005")).toBe(true);
    expect(isStructurallyValidPeriodWire("2005..2010,2015..2020")).toBe(true);
  });

  it("accepts an explicit year-independent period without treating it as dates", () => {
    expect(looksLikePeriod("_default")).toBe(true);
    expect(isStructurallyValidPeriodWire("_default")).toBe(true);
  });

  it("rejects grammar-looking lists that are unsorted or overlapping", () => {
    expect(isStructurallyValidPeriodWire("2020,2019")).toBe(false);
    expect(isStructurallyValidPeriodWire("2010..2020,2020..2021")).toBe(false);
    expect(isStructurallyValidPeriodWire("2004,LA2004")).toBe(false);
    expect(isStructurallyValidPeriodWire("2020..2019")).toBe(false);
  });
});

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

describe("periodTokenBounds (#306 advisory window math)", () => {
  it("maps a year to its calendar window", () => {
    expect(periodTokenBounds("2020")).toEqual({
      from: "2020-01-01",
      to: "2020-12-31",
    });
  });

  it("maps terms, halves, and quarters", () => {
    expect(periodTokenBounds("VT2009")).toEqual({
      from: "2009-01-01",
      to: "2009-06-30",
    });
    expect(periodTokenBounds("HT2009")).toEqual({
      from: "2009-07-01",
      to: "2009-12-31",
    });
    expect(periodTokenBounds("2020-H2")).toEqual({
      from: "2020-07-01",
      to: "2020-12-31",
    });
    expect(periodTokenBounds("2020-Q3")).toEqual({
      from: "2020-07-01",
      to: "2020-09-30",
    });
  });

  it("maps a school year across its two calendar years", () => {
    expect(periodTokenBounds("LA2004")).toEqual({
      from: "2004-07-01",
      to: "2005-06-30",
    });
  });

  it("maps months (leap-aware) and days", () => {
    expect(periodTokenBounds("2020-02")).toEqual({
      from: "2020-02-01",
      to: "2020-02-29",
    });
    expect(periodTokenBounds("2019-02")).toEqual({
      from: "2019-02-01",
      to: "2019-02-28",
    });
    expect(periodTokenBounds("2020-08-15")).toEqual({
      from: "2020-08-15",
      to: "2020-08-15",
    });
  });

  it("rejects non-tokens (ranges, _default, junk, impossible days)", () => {
    expect(periodTokenBounds("2018..2020")).toBeNull();
    expect(periodTokenBounds("_default")).toBeNull();
    expect(periodTokenBounds("banana")).toBeNull();
    expect(periodTokenBounds("2019-02-29")).toBeNull();
  });
});

describe("periodTokenForBounds (#271 inverse — coarsest exact token)", () => {
  it("term-spelling wins the H1/H2 tie-break (VT/HT, never -H)", () => {
    expect(periodTokenForBounds("2009-01-01", "2009-06-30")).toBe("VT2009");
    expect(periodTokenForBounds("2009-07-01", "2009-12-31")).toBe("HT2009");
  });

  it("a window no token covers → the explicit ISO range (NEVER year-rounded)", () => {
    // Feb–Jun has no single token; the range preserves the exact span rather than
    // collapsing to a containing year (which would re-introduce the ambiguity the
    // interval resolver removes).
    expect(periodTokenForBounds("2020-02-01", "2020-06-30")).toBe(
      "2020-02-01..2020-06-30",
    );
    expect(periodTokenForBounds("2010-01-01", "2020-12-31")).toBe(
      "2010-01-01..2020-12-31",
    );
  });

  it("round-trips through periodTokenBounds for every emitted token", () => {
    for (const [lo, hi] of [
      ["2020-01-01", "2020-12-31"],
      ["2020-03-01", "2020-03-31"],
      ["2020-07-01", "2020-09-30"],
      ["2009-01-01", "2009-06-30"],
      ["2004-07-01", "2005-06-30"],
      ["2020-08-15", "2020-08-15"],
    ] as const) {
      const token = periodTokenForBounds(lo, hi);
      expect(periodTokenBounds(token)).toEqual({ from: lo, to: hi });
    }
  });
});

describe("periodWireBounds (#678: exact ISO bounds of a whole ?period)", () => {
  it("a range resolves to its outer endpoints' bounds", () => {
    expect(periodWireBounds("2010..2015")).toEqual({
      from: "2010-01-01",
      to: "2015-12-31",
    });
    // A sub-annual endpoint keeps its grain on the outer side.
    expect(periodWireBounds("2010-Q2..2015-03")).toEqual({
      from: "2010-04-01",
      to: "2015-03-31",
    });
    expect(periodWireBounds("LA2004..LA2005")).toEqual({
      from: "2004-07-01",
      to: "2006-06-30",
    });
  });

  it("a comma list unions every part's bounds (outer min start, max end)", () => {
    expect(periodWireBounds("2005..2010,2015..2020")).toEqual({
      from: "2005-01-01",
      to: "2020-12-31",
    });
  });

  it("null when ANY segment is invalid (no partial clamp from the valid fragments)", () => {
    // A mixed valid+invalid wire must NOT yield the valid pieces' bounds — that
    // would silently turn an invalid user period into a DIFFERENT valid source
    // period on Add. The whole wire is refused; the caller falls back safely.
    expect(periodWireBounds("2018,junk")).toBeNull();
    expect(periodWireBounds("junk,2018")).toBeNull();
    expect(periodWireBounds("2010..junk,2015..2020")).toBeNull();
    expect(periodWireBounds("2010..2020,nope")).toBeNull();
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

describe("periodYearIntervals", () => {
  it("returns coalesced year-shaped intervals without hiding real gaps", () => {
    expect(
      periodYearIntervals([
        { from: 2015, to: 2020 },
        { from: 2000, to: 2005 },
        { from: 2006, to: 2008 },
      ]),
    ).toEqual([
      { from: 2000, to: 2008 },
      { from: 2015, to: 2020 },
    ]);
  });
});

describe("mergePeriods (#992 find-or-create period extension)", () => {
  it("coalesces disjoint year RANGES into a sorted, non-overlapping list", () => {
    expect(
      mergePeriods({ from: 2015, to: 2020 }, { from: 2005, to: 2010 }),
    ).toEqual([
      { from: 2005, to: 2010 },
      { from: 2015, to: 2020 },
    ]);
  });

  it("merges OVERLAPPING year ranges into one span", () => {
    expect(
      mergePeriods({ from: 2005, to: 2012 }, { from: 2010, to: 2020 }),
    ).toEqual({ from: 2005, to: 2020 });
  });

  it("adjacency-merges TOUCHING year ranges (a 0-year gap) into one span", () => {
    // 2010..2011 then 2012..2013 fuse (gap of 0 years).
    expect(
      mergePeriods({ from: 2010, to: 2011 }, { from: 2012, to: 2013 }),
    ).toEqual({ from: 2010, to: 2013 });
  });

  it("collapses a lone merged interval to a scalar year (not a 1-element list)", () => {
    // A point year absorbed into an adjacent range → a single {from,to}; a lone
    // point stays a bare number.
    expect(mergePeriods(2010, 2010)).toBe(2010);
    expect(mergePeriods(2010, { from: 2011, to: 2013 })).toEqual({
      from: 2010,
      to: 2013,
    });
  });

  it("coalesces an existing LIST with an incoming window", () => {
    expect(
      mergePeriods(
        [
          { from: 2005, to: 2010 },
          { from: 2015, to: 2020 },
        ],
        { from: 2011, to: 2014 },
      ),
    ).toEqual({ from: 2005, to: 2020 });
  });

  it("REPLACES with incoming when EITHER side uses token grammar", () => {
    // A token (`HT2020`) can't be coalesced with a year — a mixed-grain sort is
    // undefined, so the incoming window wins wholesale.
    expect(mergePeriods("HT2020", 2018)).toBe(2018);
    expect(mergePeriods(2018, "HT2020")).toBe("HT2020");
    expect(mergePeriods("VT2020", "HT2021")).toBe("HT2021");
  });

  it("REPLACES when a range endpoint is a non-year token (mixed grammar)", () => {
    // A `{from: "VT1992", to: 2009}` range has a token endpoint → not pure year
    // grammar → replace with incoming.
    expect(mergePeriods({ from: "VT1992", to: 2009 } as never, 2020)).toBe(
      2020,
    );
  });

  it("COALESCES a numeric-STRING year against an incoming year (Fix 1)", () => {
    // `Source.period` validly carries year tokens as strings ("2020"); they must
    // coalesce like the int form, NOT fall to REPLACE and drop the existing
    // coverage. 2020 + adjacent 2021 fuse to one range (proves it did NOT replace).
    expect(mergePeriods("2020", 2021)).toEqual({ from: 2020, to: 2021 });
    // A DISJOINT string year + incoming → the #307 list form (coalesced, not
    // replaced): opening a file with period "2020" and adding 2022 keeps both.
    expect(mergePeriods("2020", 2022)).toEqual([2020, 2022]);
    // A `{from,to}` with numeric-string endpoints parses both endpoints.
    expect(mergePeriods({ from: "2018", to: "2020" }, 2019)).toEqual({
      from: 2018,
      to: 2020,
    });
  });

  it("an UNSET incoming period does NOT wipe a valid existing period (Fix 2)", () => {
    // `periodFromWire(null)` yields "" — a catalog row with no finite period must
    // not blank the existing period (that would invalidate every binding).
    expect(mergePeriods(2020, "")).toBe(2020);
    expect(mergePeriods([], 2020)).toBe(2020); // empty-list existing is unset too
  });

  it("a blank EXISTING period adopts a set incoming period (Fix 2)", () => {
    // A fresh/blank source takes the add's window.
    expect(mergePeriods("", 2020)).toBe(2020);
    expect(mergePeriods("", { from: 2010, to: 2015 })).toEqual({
      from: 2010,
      to: 2015,
    });
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

  it("adjacency-merges TOUCHING rows into one span, same as mergePeriods", () => {
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
