import { describe, expect, it } from "vitest";
import { catalogPeriodLabel, catalogPeriodRuns } from "./catalog_period";

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
