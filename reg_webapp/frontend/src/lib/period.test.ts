import { describe, expect, it } from "vitest";
import {
  clampYearWindow,
  grainOfToken,
  intersectCoverageWindow,
  looksLikePeriod,
  nextResolutionQuery,
  notDeliveredGaps,
  periodFieldFromQuery,
  periodFromWire,
  periodQueryFromField,
  periodRangeEndpoints,
  periodTokenBounds,
  periodTokenForBounds,
  periodToWire,
  queryFromParams,
  rangeRepresentable,
  sameYearWindow,
  VALUE_SET_VERSION_NONE,
  yearWindowFromWire,
  yearWindowRepresentable,
  yearWindowToWire,
} from "./period";

describe("periodToWire (Source.period → ?period wire string)", () => {
  it("a bare year int → the year string", () => {
    expect(periodToWire(2020)).toBe("2020");
  });

  it("a token string → trimmed; blank → null", () => {
    expect(periodToWire("2020-Q1")).toBe("2020-Q1");
    expect(periodToWire("  2019  ")).toBe("2019");
    expect(periodToWire("_default")).toBe("_default");
    expect(periodToWire("")).toBeNull();
    expect(periodToWire("   ")).toBeNull();
  });

  it("a {from,to} range → from..to; a blank endpoint → null", () => {
    expect(periodToWire({ from: 2018, to: 2020 })).toBe("2018..2020");
    expect(periodToWire({ from: "2018", to: "2020" })).toBe("2018..2020");
    expect(periodToWire({ from: "", to: 2020 })).toBeNull();
  });

  it("null / a partial {from-only} object → null (defensive fallthrough)", () => {
    expect(periodToWire(null as never)).toBeNull();
    expect(periodToWire({ from: 2018 } as never)).toBeNull();
  });

  it("a #307 segment list → comma-joined member wires", () => {
    expect(
      periodToWire([
        { from: 2005, to: 2010 },
        { from: 2015, to: 2020 },
      ]),
    ).toBe("2005..2010,2015..2020");
    expect(periodToWire([2018, "HT2020"])).toBe("2018,HT2020");
  });

  it("a list with a malformed/blank member (or empty list) → null", () => {
    expect(periodToWire([])).toBeNull();
    expect(periodToWire([2018, ""])).toBeNull();
    expect(periodToWire([{ from: "", to: 2020 }])).toBeNull();
  });
});

describe("periodFromWire (?period wire string → Source.period, C1 prefill)", () => {
  it("a bare integer year → the number arm (single year, from=to in the editor)", () => {
    expect(periodFromWire("2018")).toBe(2018);
    expect(periodFromWire("  2020  ")).toBe(2020);
  });

  it("an integer-year range → the {from,to} numbers (years range mode)", () => {
    expect(periodFromWire("2010..2020")).toEqual({ from: 2010, to: 2020 });
  });

  it("a non-year token rides through as the raw string (token mode)", () => {
    expect(periodFromWire("HT2018")).toBe("HT2018");
    expect(periodFromWire("2019-03")).toBe("2019-03");
    expect(periodFromWire("_default")).toBe("_default");
  });

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

  it("null / blank → the unset empty-string period", () => {
    expect(periodFromWire(null)).toBe("");
    expect(periodFromWire("")).toBe("");
    expect(periodFromWire("   ")).toBe("");
  });

  it("round-trips a single year and ranges (int + token endpoints) through periodToWire", () => {
    expect(periodToWire(periodFromWire("2018"))).toBe("2018");
    expect(periodToWire(periodFromWire("2010..2020"))).toBe("2010..2020");
    expect(periodToWire(periodFromWire("VT1992..2009"))).toBe("VT1992..2009");
    expect(periodToWire(periodFromWire("1992..2009-06-30"))).toBe(
      "1992..2009-06-30",
    );
  });

  it("a comma wire → the #307 segment list, members shaped like scalars", () => {
    expect(periodFromWire("2005..2010,2015..2020")).toEqual([
      { from: 2005, to: 2010 },
      { from: 2015, to: 2020 },
    ]);
    expect(periodFromWire("2018,HT2020")).toEqual([2018, "HT2020"]);
  });

  it("a malformed comma wire (blank member) stays the raw string", () => {
    expect(periodFromWire("2018,")).toBe("2018,");
    expect(periodFromWire(",2018")).toBe(",2018");
  });

  it("round-trips a list wire through periodToWire", () => {
    expect(periodToWire(periodFromWire("2005..2010,2015..2020"))).toBe(
      "2005..2010,2015..2020",
    );
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

  it("malformed comma text (blank member) rides through as the raw string", () => {
    expect(periodFromWire("2018,")).toBe("2018,");
  });
});

describe("VALUE_SET_VERSION_NONE sentinel", () => {
  it("matches the backend period_param.VALUE_SET_VERSION_NONE", () => {
    // The picker's "(no version)" chip sends this; the backend maps it to "".
    // MUST stay in lockstep with reg_webapp/backend/.../period_param.py.
    expect(VALUE_SET_VERSION_NONE).toBe("_none");
  });

  it("rides in the query like any value", () => {
    expect(
      queryFromParams({
        period: "2020",
        value_set_version: VALUE_SET_VERSION_NONE,
      }),
    ).toBe("period=2020&value_set_version=_none");
  });
});

describe("period field ↔ query round-trip", () => {
  // The field text IS the wire value (identity on a trimmed string), for every
  // wire form.
  const wireForms = [
    "2020", // year
    "HT2020", // term token
    "VT2020",
    "2020-Q3", // quarter
    "2020-H1", // half
    "2020-08", // month
    "2020-12-31", // day
    "2018..2020", // range
    "_default", // snapshot sentinel
  ];

  for (const wire of wireForms) {
    it(`round-trips ${wire}`, () => {
      const field = periodFieldFromQuery(wire);
      expect(field).toBe(wire);
      expect(periodQueryFromField(field)).toBe(wire);
    });
  }

  it("maps null/absent query to an empty field", () => {
    expect(periodFieldFromQuery(null)).toBe("");
    expect(periodFieldFromQuery(undefined)).toBe("");
  });

  it("maps a blank/whitespace field to null (full history)", () => {
    expect(periodQueryFromField("")).toBeNull();
    expect(periodQueryFromField("   ")).toBeNull();
  });

  it("trims surrounding whitespace off the field", () => {
    expect(periodQueryFromField("  2020  ")).toBe("2020");
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

  it("is advisory only — a rejected value is still a string the caller may send", () => {
    // Guard the contract: the helper returns a boolean (never throws), so the
    // picker can show a hint without blocking submit.
    expect(typeof looksLikePeriod("definitely not a period")).toBe("boolean");
  });
});

describe("queryFromParams", () => {
  it("omits undefined/empty params and single-values the rest", () => {
    expect(queryFromParams({ period: "2020" })).toBe("period=2020");
    expect(queryFromParams({})).toBe("");
    expect(queryFromParams({ period: "2020", variant: undefined })).toBe(
      "period=2020",
    );
    expect(queryFromParams({ period: "", variant: "x" })).toBe("variant=x");
  });

  it("emits all three params in a single-valued query (no leading ?)", () => {
    expect(
      queryFromParams({
        period: "2020",
        variant: "x",
        value_set_version: "y",
      }),
    ).toBe("period=2020&variant=x&value_set_version=y");
  });

  it("percent-encodes reserved characters in a value", () => {
    expect(queryFromParams({ period: "2020 Q3&z" })).toBe("period=2020+Q3%26z");
  });

  it("encodes a free-text value_set_version LABEL (the picker sends the label)", () => {
    // value_set_version is the human label (spaces/commas/case), NOT a slug — the
    // backend input-validation gate accepts it (it's a Python-filter match, not SQL). The query
    // builder must URL-encode it so it round-trips.
    expect(
      queryFromParams({
        period: "2020",
        value_set_version: "SUN 1996, 5 positioner, brutto",
      }),
    ).toBe("period=2020&value_set_version=SUN+1996%2C+5+positioner%2C+brutto");
  });
});

describe("nextResolutionQuery (resolution-merge rule)", () => {
  it("sets a period from scratch", () => {
    expect(nextResolutionQuery({}, { period: "2020" })).toBe("period=2020");
  });

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

  it("picking a variant inherits the current period + value_set_version", () => {
    const current = { period: "2020", value_set_version: "y" };
    expect(nextResolutionQuery(current, { variant: "x" })).toBe(
      "period=2020&variant=x&value_set_version=y",
    );
  });

  it("picking a value_set_version inherits the current period + variant", () => {
    const current = { period: "2020", variant: "x" };
    expect(nextResolutionQuery(current, { value_set_version: "y" })).toBe(
      "period=2020&variant=x&value_set_version=y",
    );
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

  it("a new period without a modifier keeps the existing modifiers", () => {
    const current = { period: "2020", variant: "x" };
    expect(nextResolutionQuery(current, { period: "2019" })).toBe(
      "period=2019&variant=x",
    );
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
  it("a full-year window → the bare year", () => {
    expect(periodTokenForBounds("2020-01-01", "2020-12-31")).toBe("2020");
  });

  it("a single month → the month token (not the year)", () => {
    expect(periodTokenForBounds("2020-01-01", "2020-01-31")).toBe("2020-01");
    expect(periodTokenForBounds("2020-02-01", "2020-02-29")).toBe("2020-02");
  });

  it("the four quarters → their tokens", () => {
    expect(periodTokenForBounds("2020-01-01", "2020-03-31")).toBe("2020-Q1");
    expect(periodTokenForBounds("2020-04-01", "2020-06-30")).toBe("2020-Q2");
    expect(periodTokenForBounds("2020-07-01", "2020-09-30")).toBe("2020-Q3");
    expect(periodTokenForBounds("2020-10-01", "2020-12-31")).toBe("2020-Q4");
  });

  it("term-spelling wins the H1/H2 tie-break (VT/HT, never -H)", () => {
    expect(periodTokenForBounds("2009-01-01", "2009-06-30")).toBe("VT2009");
    expect(periodTokenForBounds("2009-07-01", "2009-12-31")).toBe("HT2009");
  });

  it("a single day → the bare day token", () => {
    expect(periodTokenForBounds("2020-08-15", "2020-08-15")).toBe("2020-08-15");
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
      ["2020-08-15", "2020-08-15"],
    ] as const) {
      const token = periodTokenForBounds(lo, hi);
      expect(periodTokenBounds(token)).toEqual({ from: lo, to: hi });
    }
  });
});

describe("periodRangeEndpoints", () => {
  it("splits a 2-endpoint range", () => {
    expect(periodRangeEndpoints("2018..2020")).toEqual(["2018", "2020"]);
    expect(periodRangeEndpoints("VT2018..HT2020")).toEqual([
      "VT2018",
      "HT2020",
    ]);
  });

  it("returns null for non-ranges and malformed ranges", () => {
    expect(periodRangeEndpoints("2020")).toBeNull();
    expect(periodRangeEndpoints("2018..2019..2020")).toBeNull();
  });
});

describe("grainOfToken / rangeRepresentable (#308)", () => {
  it("classifies every token form (H1/H2 map to term)", () => {
    expect(grainOfToken("2020")).toBe("year");
    expect(grainOfToken("VT2009")).toBe("term");
    expect(grainOfToken("HT2009")).toBe("term");
    expect(grainOfToken("2020-H1")).toBe("term");
    expect(grainOfToken("2020-Q3")).toBe("quarter");
    expect(grainOfToken("2020-08")).toBe("month");
    expect(grainOfToken("2020-08-15")).toBe("day");
    expect(grainOfToken("_default")).toBeNull();
    expect(grainOfToken("2018..2020")).toBeNull();
    expect(grainOfToken("junk")).toBeNull();
  });

  it("rangeRepresentable accepts single tokens and uniform-grain ranges only", () => {
    expect(rangeRepresentable("2020")).toBe(true);
    expect(rangeRepresentable("HT2018")).toBe(true);
    expect(rangeRepresentable("2010..2020")).toBe(true);
    expect(rangeRepresentable("VT2018..HT2019")).toBe(true);
    // Mixed grains (the #306 succession clips) need the text/token escape.
    expect(rangeRepresentable("1992..2009-06-30")).toBe(false);
    expect(rangeRepresentable("_default")).toBe(false);
    expect(rangeRepresentable("")).toBe(false);
  });

  it("rangeRepresentable is grains-aware: a value at an excluded grain needs the text mode", () => {
    expect(rangeRepresentable("2020-08", ["year"])).toBe(false);
    expect(rangeRepresentable("2020-08", ["year", "month"])).toBe(true);
    expect(rangeRepresentable("2020", ["year"])).toBe(true);
  });
});

// ── #615 year-window slider helpers ──────────────────────────────────────────

describe("yearWindowToWire (year window → ?period wire)", () => {
  it("a single year (from === to) → the bare year", () => {
    expect(yearWindowToWire({ from: 2018, to: 2018 })).toBe("2018");
  });

  it("a multi-year window → the from..to range", () => {
    expect(yearWindowToWire({ from: 2010, to: 2020 })).toBe("2010..2020");
  });
});

describe("yearWindowFromWire (?period wire → year window | null)", () => {
  it("a bare year → from === to", () => {
    expect(yearWindowFromWire("2018")).toEqual({ from: 2018, to: 2018 });
  });

  it("a uniform-year range → {from, to}", () => {
    expect(yearWindowFromWire("2010..2020")).toEqual({ from: 2010, to: 2020 });
  });

  it("blank / null → null", () => {
    expect(yearWindowFromWire(null)).toBeNull();
    expect(yearWindowFromWire("")).toBeNull();
    expect(yearWindowFromWire("   ")).toBeNull();
  });

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

describe("yearWindowRepresentable", () => {
  it("true for pure year windows, false otherwise (mirrors yearWindowFromWire)", () => {
    expect(yearWindowRepresentable("2018")).toBe(true);
    expect(yearWindowRepresentable("2010..2020")).toBe(true);
    expect(yearWindowRepresentable("HT2020")).toBe(false);
    expect(yearWindowRepresentable("_default")).toBe(false);
    expect(yearWindowRepresentable(null)).toBe(false);
  });
});

describe("clampYearWindow", () => {
  it("clamps both endpoints into [min, max]", () => {
    expect(clampYearWindow({ from: 1950, to: 2050 }, 1960, 2026)).toEqual({
      from: 1960,
      to: 2026,
    });
  });

  it("a window inside the bounds is unchanged", () => {
    expect(clampYearWindow({ from: 2000, to: 2010 }, 1960, 2026)).toEqual({
      from: 2000,
      to: 2010,
    });
  });

  it("keeps from <= to after clamping (a fully-out-of-range window collapses)", () => {
    const w = clampYearWindow({ from: 2030, to: 2040 }, 1960, 2026);
    expect(w.from).toBeLessThanOrEqual(w.to);
    expect(w).toEqual({ from: 2026, to: 2026 });
  });
});

describe("intersectCoverageWindow (#671 coverage-aware seed)", () => {
  it("window inside coverage → the window itself", () => {
    expect(
      intersectCoverageWindow(
        { from: 1995, to: 2015 },
        { from: 2000, to: 2010 },
        1960,
        2026,
      ),
    ).toEqual({ from: 2000, to: 2010 });
  });

  it("window wider than coverage → narrowed to the coverage span (the intersection)", () => {
    expect(
      intersectCoverageWindow(
        { from: 1995, to: 2008 },
        { from: 1990, to: 2020 },
        1960,
        2026,
      ),
    ).toEqual({ from: 1995, to: 2008 });
  });

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

  it("open coverage START resolves to fallbackMin", () => {
    expect(
      intersectCoverageWindow(
        { from: null, to: 2008 },
        { from: 1950, to: 2005 },
        1960,
        2026,
      ),
    ).toEqual({ from: 1960, to: 2005 });
  });

  it("open coverage START + a pre-1960 window: the WINDOW-AWARE fallbackMin keeps the covered pre-1960 years (Fix 5)", () => {
    // Fix 5: a project window may legitimately start before 1960. The picker passes
    // a window-aware lower fallback (`sliderBounds.min` = 1950 here, not the fixed
    // 1960 floor), so an OPEN-start coverage extends to the rendered track start —
    // window 1950–2005 ∩ coverage {null..2008} → 1950–2005, NOT 1960–2005 (which
    // silently dropped the covered 1950–1959 years).
    expect(
      intersectCoverageWindow(
        { from: null, to: 2008 },
        { from: 1950, to: 2005 },
        1950,
        2026,
      ),
    ).toEqual({ from: 1950, to: 2005 });
  });

  it("open coverage END resolves to fallbackMax (the vintage ceiling)", () => {
    expect(
      intersectCoverageWindow(
        { from: 1995, to: null },
        { from: 2000, to: 2030 },
        1960,
        2026,
      ),
    ).toEqual({ from: 2000, to: 2026 });
  });

  it("no window → the (effective) coverage span", () => {
    expect(
      intersectCoverageWindow({ from: 1995, to: 2008 }, null, 1960, 2026),
    ).toEqual({ from: 1995, to: 2008 });
  });

  it("no window, fully-open coverage → the full fallback bounds", () => {
    expect(
      intersectCoverageWindow({ from: null, to: null }, null, 1960, 2026),
    ).toEqual({ from: 1960, to: 2026 });
  });

  it("no coverage but a SET window → the window (a stateless variable honours its window, Fix A)", () => {
    // FIX A: coverage null + a window must seed at the window, NOT widen to full
    // history — else a stateless variable's Apply would submit 1960..vintage.
    expect(
      intersectCoverageWindow(null, { from: 2000, to: 2010 }, 1960, 2026),
    ).toEqual({ from: 2000, to: 2010 });
  });

  it("no coverage AND no window → the full fallback bounds (nothing to narrow to, Fix A)", () => {
    expect(intersectCoverageWindow(null, null, 1960, 2026)).toEqual({
      from: 1960,
      to: 2026,
    });
  });

  it("window wholly AFTER coverage → snaps to the coverage end (a covered year, not inverted)", () => {
    const seed = intersectCoverageWindow(
      { from: 1995, to: 2008 },
      { from: 2012, to: 2018 },
      1960,
      2026,
    );
    expect(seed.from).toBeLessThanOrEqual(seed.to);
    expect(seed).toEqual({ from: 2008, to: 2008 });
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

  it("INVERTED effective coverage (open end past the vintage), no window → the full bounds, NOT a manufactured no-data span (mirrors slider Fix D)", () => {
    // Open-ended coverage from 2025 on a 2024-vintage catalog → covFrom 2025 >
    // covTo 2024 (inverted). The old Math.min/Math.max manufactured {2024, 2025}
    // (a selectable no-data span); inverted coverage is now treated as NO coverage,
    // so the seed is the full fallback bounds — agreeing with PeriodWindowSlider's
    // `bandEdges` (Fix D), which nulls the band (no clamp).
    expect(
      intersectCoverageWindow({ from: 2025, to: null }, null, 1960, 2024),
    ).toEqual({ from: 1960, to: 2024 });
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
  it("no coverage → no gaps (nothing to compare against)", () => {
    expect(notDeliveredGaps({ from: 2000, to: 2010 }, null)).toEqual([]);
  });

  it("coverage fully covers the selection → no gaps", () => {
    expect(
      notDeliveredGaps({ from: 2000, to: 2010 }, { from: 1995, to: 2015 }),
    ).toEqual([]);
  });

  it("a leading gap (selection starts before coverage)", () => {
    expect(
      notDeliveredGaps({ from: 1990, to: 2010 }, { from: 2000, to: 2015 }),
    ).toEqual([{ from: 1990, to: 1999 }]);
  });

  it("a trailing gap (selection ends after coverage)", () => {
    expect(
      notDeliveredGaps({ from: 2000, to: 2020 }, { from: 1995, to: 2015 }),
    ).toEqual([{ from: 2016, to: 2020 }]);
  });

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

  it("an UNBOUNDED start (from: null) preserves the finite-end gap, no leading gap", () => {
    // Fix A: a `0001..2008` coverage → `{from: null, to: 2008}`. A 2010–2015
    // selection STILL flags "after 2008" (trailing gap), and the open start
    // never fires a spurious leading gap.
    // The trailing gap is clamped to the selection start (2010), not 2009.
    expect(
      notDeliveredGaps({ from: 2010, to: 2015 }, { from: null, to: 2008 }),
    ).toEqual([{ from: 2010, to: 2015 }]);
    // A selection spanning the open start gaps only on the finite end.
    expect(
      notDeliveredGaps({ from: 1990, to: 2010 }, { from: null, to: 2008 }),
    ).toEqual([{ from: 2009, to: 2010 }]);
    // Entirely within the covered (finite-end) span → no gap.
    expect(
      notDeliveredGaps({ from: 1990, to: 2005 }, { from: null, to: 2008 }),
    ).toEqual([]);
  });

  it("an UNBOUNDED end (to: null) preserves the finite-start gap, no trailing gap", () => {
    // Fix A mirror: a `1990..9999` coverage → `{from: 1990, to: null}`. A
    // selection before 1990 gaps on the start; a selection after the start never
    // fires an "after" gap (still delivered).
    expect(
      notDeliveredGaps({ from: 1985, to: 2020 }, { from: 1990, to: null }),
    ).toEqual([{ from: 1985, to: 1989 }]);
    expect(
      notDeliveredGaps({ from: 2000, to: 2030 }, { from: 1990, to: null }),
    ).toEqual([]);
  });
});

describe("sameYearWindow", () => {
  it("two equal windows are the same", () => {
    expect(
      sameYearWindow({ from: 2000, to: 2010 }, { from: 2000, to: 2010 }),
    ).toBe(true);
  });

  it("differing windows are not", () => {
    expect(
      sameYearWindow({ from: 2000, to: 2010 }, { from: 2000, to: 2011 }),
    ).toBe(false);
  });

  it("null-safe: two nulls equal, one null not", () => {
    expect(sameYearWindow(null, null)).toBe(true);
    expect(sameYearWindow({ from: 2000, to: 2010 }, null)).toBe(false);
    expect(sameYearWindow(null, { from: 2000, to: 2010 })).toBe(false);
  });
});
