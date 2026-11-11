import { describe, expect, it } from "vitest";
import {
  catalogPeriodLabel,
  catalogPeriodRuns,
  inTableGrain,
  sourcePeriodLabel,
} from "./catalog_period";

describe("catalog period labels", () => {
  // DESIGN.md → Time; cases from #2315 and #2323. The API's end is exclusive:
  // reading it as inclusive adds the next year. A school year that kept its
  // dates or its `LA` token, or a 31 December start beside a year, fails here.
  it.each([
    [{ start: "1984-07-01", end: "2024-07-01" }, "1984/1985–2023/2024"],
    [{ start: "1999-07-01", end: "2000-07-01" }, "1999/2000"],
    [{ start: "1968-12-31", end: "2026-01-01" }, "1968–2025"],
    [{ start: "2019-01-01", end: "2020-07-01" }, "2019–2020"],
    [{ start: "2020-02-01", end: "2020-03-01" }, "2020"],
    [{ start: "2020-01-01", end: null }, "From 2020"],
    [{ start: null, end: "2021-01-01" }, "Through 2020"],
    [{ start: null, end: null }, null],
  ])("renders %j as %s", (period, expected) => {
    expect(catalogPeriodLabel(period)).toBe(expected);
  });

  // Runs join at the label's grain: yearly October snapshots are one range,
  // not a list of years, and a gap year still splits them.
  it("joins snapshot years into runs", () => {
    expect(
      catalogPeriodRuns([
        { start: "2012-10-15", end: "2012-10-16" },
        { start: "2010-10-15", end: "2010-10-16" },
        { start: "2011-10-15", end: "2011-10-16" },
        { start: "2014-10-15", end: "2014-10-16" },
      ]),
    ).toBe("2010–2012, 2014");
  });
});

describe("delivered years in the table's grain", () => {
  // #2416 (maintainer 2026-11-11): study years from 1981 cut SWECOV's
  // Slutbetyg (school years 1972/1973–2023/2024) at 1 January 1981; the
  // 1980/1981 file is delivered whole, so the piece reads from it. A
  // calendar-year table cut the same way still reads in years.
  it.each([
    [
      { start: "1972-07-01", end: "2024-07-01" },
      { start: "1981-01-01", end: "1989-07-01" },
      "1980/1981–1988/1989",
    ],
    [
      { start: "1990-01-01", end: "2025-01-01" },
      { start: "1991-01-01", end: "2024-01-01" },
      "1991–2023",
    ],
  ])("in a table of %j reads %j as %s", (table, piece, expected) => {
    expect(catalogPeriodRuns(inTableGrain([piece], table))).toBe(expected);
  });
});

describe("source period labels", () => {
  // A period the year thumbs cannot write reads exactly, in words
  // (maintainer 2026-11-10, PR #2389): coarsening HT2019 to "2019" claims the
  // spring term too; echoing the token shows the file's grammar.
  it.each([
    ["HT2019", "autumn 2019"],
    ["VT2020", "spring 2020"],
    ["2019-Q3", "2019 Q3"],
    ["2020-02", "February 2020"],
    ["2019-10-15", "15 October 2019"],
    ["LA1999", "1999/2000"],
    [["2018", "LA2019"], "2018, 2019/2020"],
    [{ from: "2019", to: "2020-06-30" }, "2019–30 June 2020"],
    ["_default", null],
  ])("renders %s as %s", (period, expected) => {
    expect(sourcePeriodLabel(period)).toBe(expected);
  });
});
