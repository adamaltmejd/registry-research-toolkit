import { describe, expect, it } from "vitest";
import { catalogPeriodLabel } from "./catalog_period";

describe("catalog period labels", () => {
  // The API's end is exclusive. Treating it as inclusive adds a day (or the
  // next year), and local-time subtraction can lose leap day across time zones.
  // Open year-aligned bounds must coarsen like closed year ranges.
  it.each([
    [{ start: "2020-01-01", end: null }, "From 2020"],
    [{ start: null, end: "2021-01-01" }, "Through 2020"],
    [{ start: "2020-02-01", end: "2020-03-01" }, "2020-02"],
    [{ start: "2019-01-01", end: "2021-01-01" }, "2019..2020"],
    [{ start: null, end: "2020-03-01" }, "Through 2020-02-29"],
    [{ start: "2020-02-29", end: null }, "From 2020-02-29"],
    [{ start: null, end: null }, "Unbounded"],
  ])("renders %j as %s", (period, expected) => {
    expect(catalogPeriodLabel(period)).toBe(expected);
  });
});
