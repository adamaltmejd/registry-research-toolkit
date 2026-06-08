import { describe, expect, it } from "vitest";
import {
  looksLikePeriod,
  nextResolutionQuery,
  periodFieldFromQuery,
  periodQueryFromField,
  periodToWire,
  queryFromParams,
  VALUE_SET_VERSION_NONE,
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
