import { describe, expect, it } from "vitest";
import { distinctValueSets } from "./catalog";
import { state } from "./catalog-test-helpers";

// Split from catalog.test.ts by contract surface: the value-set fold.
// Siblings: catalog.{browse,picker,picker-filters,value-sets}.test.ts.

describe("distinctValueSets (#668 — value-set-centric fold)", () => {
  it("lists which variants use a value set (the cross-variant case)", () => {
    const states = [
      state({ value_set_id: "1", variant: "doda", valid_from: "1983-01-01" }),
      state({ value_set_id: "1", variant: "fodda", valid_from: "1983-01-01" }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages.map((u) => u.variant).sort()).toEqual([
      "doda",
      "fodda",
    ]);
  });

  it.each([
    ["continuing column first", 2, 3, false],
    ["added alias first", 3, 2, true],
  ])(
    "does not report a replacement when a successor adds an alias (%s)",
    (_label, continuingStateId, aliasStateId, reverseSuccessors) => {
      const predecessor = state({
        state_id: "1",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2017-01-01",
        valid_to: "2017-12-31",
        delivery_column_name: "A",
      });
      const continuing = state({
        state_id: String(continuingStateId),
        value_set_id: "1",
        variant: "individer",
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
        delivery_column_name: "A",
      });
      const alias = state({
        state_id: String(aliasStateId),
        value_set_id: "1",
        variant: "individer",
        valid_from: "2018-01-01",
        valid_to: "2018-12-31",
        delivery_column_name: "B",
      });
      const successors = reverseSuccessors
        ? [alias, continuing]
        : [continuing, alias];

      const usage = distinctValueSets([predecessor, ...successors])[0]
        .usages[0];
      expect(
        usage.states
          .filter((s) => s.valid_from === "2018-01-01")
          .map((s) => s.delivery_column_name),
      ).toEqual(["A", "B"]);
      expect(usage.spans).toEqual([
        { from: "2017-01-01", to: "2018-12-31", pooled: false },
      ]);
    },
  );

  it("does not report technical changes for same-state monthly windows", () => {
    const states = [
      state({
        state_id: "10",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-01-31",
        delivery_column_name: "LonFinkJan",
      }),
      state({
        state_id: "10",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-02-01",
        valid_to: "2020-02-29",
        delivery_column_name: "LonFinkFeb",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2020-02-29", pooled: false },
    ]);
  });

  it("does not report technical changes for overlapping alternatives", () => {
    const states = [
      state({
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
        delivery_column_name: "A",
      }),
      state({
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-06-01",
        valid_to: "2021-12-31",
        delivery_column_name: "B",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2021-12-31", pooled: false },
    ]);
  });

  it("keeps the span-end predecessor after a contained overlap", () => {
    const states = [
      state({
        state_id: "1",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2021-12-31",
        data_type: "int",
        delivery_column_name: "A",
      }),
      state({
        state_id: "2",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2021-01-01",
        valid_to: "2021-06-30",
        data_type: "char",
        delivery_column_name: "B",
      }),
      state({
        state_id: "3",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2022-01-01",
        valid_to: "2022-12-31",
        data_type: "bigint",
        delivery_column_name: "C",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      {
        from: "2020-01-01",
        to: "2022-12-31",
        pooled: false,
        changes: [
          {
            at: "2022-01-01",
            notes: ["type int -> bigint", "column A -> C"],
          },
        ],
      },
    ]);
  });

  it("does not pick an arbitrary transition after equal-end overlapping alternatives", () => {
    const states = [
      state({
        state_id: "1",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-01-01",
        valid_to: "2020-12-31",
        delivery_column_name: "A",
      }),
      state({
        state_id: "2",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2020-06-01",
        valid_to: "2020-12-31",
        delivery_column_name: "B",
      }),
      state({
        state_id: "3",
        value_set_id: "1",
        variant: "individer",
        valid_from: "2021-01-01",
        valid_to: "2021-12-31",
        delivery_column_name: "C",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2020-01-01", to: "2021-12-31", pooled: false },
    ]);
  });

  it("does not merge across different variants (spans are per-variant)", () => {
    const states = [
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2000-12-31",
      }),
      state({
        value_set_id: "1",
        variant: "fodda",
        valid_from: "2001-01-01",
        valid_to: "2001-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    const doda = vs[0].usages.find((u) => u.variant === "doda");
    const fodda = vs[0].usages.find((u) => u.variant === "fodda");
    expect(doda?.spans).toEqual([
      { from: "2000-01-01", to: "2000-12-31", pooled: false },
    ]);
    expect(fodda?.spans).toEqual([
      { from: "2001-01-01", to: "2001-12-31", pooled: false },
    ]);
  });

  it("collapses contiguous years across the UNION of ids in one classification edition (M20)", () => {
    // Two distinct value_set_ids share `lkf1980` (the M13 collapse) and deliver
    // adjacent years (1980, 1981) under the SAME variant. The per-variant M20
    // collapse runs over the UNION of those ids' states, so they fuse into ONE
    // span — not one per id (which would leave two adjacent rows).
    const states = [
      state({
        value_set_id: "100",
        classifications: [
          {
            slug: "lkf1980",
            short_name: "lkf1980",
            name: "lkf1980",
            conformance: null,
          },
        ],
        variant: "doda",
        valid_from: "1980-01-01",
        valid_to: "1980-12-31",
      }),
      state({
        value_set_id: "101", // distinct id, SAME edition + variant + adjacent year
        classifications: [
          {
            slug: "lkf1980",
            short_name: "lkf1980",
            name: "lkf1980",
            conformance: null,
          },
        ],
        variant: "doda",
        valid_from: "1981-01-01",
        valid_to: "1981-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs).toHaveLength(2);
    expect(vs[0].usages).toHaveLength(1);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "1980-01-01", to: "1980-12-31", pooled: false },
    ]);
  });

  it("collapseSpans: overlapping windows extend into one span", () => {
    // Two states whose windows OVERLAP (not merely back-to-back) fuse into a
    // single span spanning the outer bounds.
    const states = [
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2003-12-31",
      }),
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2002-01-01", // starts INSIDE the first window
        valid_to: "2005-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2000-01-01", to: "2005-12-31", pooled: false },
    ]);
  });

  it("collapseSpans: a real >1-day gap splits into two spans", () => {
    // A multi-day gap between windows (not a same-day continuation) starts a new
    // span — the day-after adjacency test must NOT fuse across it.
    const states = [
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2000-01-01",
        valid_to: "2000-06-30",
      }),
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2000-08-01", // a one-month gap after 2000-06-30
        valid_to: "2000-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2000-01-01", to: "2000-06-30", pooled: false },
      { from: "2000-08-01", to: "2000-12-31", pooled: false },
    ]);
  });

  it("collapseSpans: two open-ended states under one (value set, variant) → ONE span (FIX A)", () => {
    // Regression for the `dayAfter("9999-12-31")` year-10000 overflow: two
    // still-delivered states (both `valid_to: 9999-12-31`) for one (value set,
    // variant) MUST collapse to a single open-ended span. Before the fix the
    // overflowed day-after sorted BELOW any real `valid_from`, so the second
    // open-ended state wrongly opened a spurious "since 2020" span beside the
    // "since 2016" one.
    const states = [
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2016-01-01",
        valid_to: "9999-12-31",
      }),
      state({
        value_set_id: "1",
        variant: "doda",
        valid_from: "2020-01-01",
        valid_to: "9999-12-31",
      }),
    ];
    const vs = distinctValueSets(states);
    expect(vs[0].usages[0].spans).toEqual([
      { from: "2016-01-01", to: "9999-12-31", pooled: false },
    ]);
  });
});
