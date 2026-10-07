import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { getValueSetCodes } from "./api";
import ValueSetView from "./ValueSetView.svelte";
import {
  CODES,
  classState,
  plainState,
  state,
} from "./value-set-view-test-helpers";

// Split from ValueSetView.browser.test.ts by contract surface:
// per-row Isolate and the focusColumn deep link.
// Siblings: ValueSetView.browser.test.ts, ValueSetView.{conformance,isolate,period}.browser.test.ts.

// Y-46: the states carry each coding's IDENTITY and a cardinality-independent
// summary, never its members — so the code tables here are BOUNDED READS. The
// stub below serves them out of a per-test registry, filtering and windowing in
// the same order the route does (filter the whole set, then page it).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getValueSetCodes: vi.fn() };
});

beforeEach(() => {
  vi.mocked(getValueSetCodes).mockReset();
  vi.mocked(getValueSetCodes).mockImplementation(
    async (
      valueSetId,
      {
        state = null,
        partition = "source_extensions",
        q = "",
        offset = 0,
        limit = 200,
      },
    ) => {
      const all = CODES.get(`${valueSetId}:${state ?? ""}:${partition}`) ?? [];
      const needle = q.trim().toLowerCase();
      const matched = all.filter(
        (c) =>
          c.code.toLowerCase().includes(needle) ||
          c.label.toLowerCase().includes(needle),
      );
      return {
        value_set_id: String(valueSetId),
        state_id: state,
        period_scope: "intervals",
        q,
        total: matched.length,
        offset,
        limit,
        codes: matched.slice(offset, offset + limit),
      };
    },
  );
});

describe("ValueSetView — value-set-centric multi-state view (#668/#905)", () => {
  it("per-row Isolate focuses one value set; '← All value sets' returns to the union", async () => {
    await render(ValueSetView, {
      states: [classState, plainState],
      narrowed: false,
    });
    // Union by default: two list rows, each with an Isolate button.
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
    const isolateButtons = page.getByRole("button", { name: "Isolate" });
    // Isolate the FIRST row (the classification one).
    await isolateButtons.first().click();
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(0);
    await expect.element(page.getByText("Used by")).toBeVisible();
    // The reset returns to the union.
    await page.getByRole("button", { name: "← All value sets" }).click();
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
  });

  it("Isolate after filtering isolates the FILTERED value set (stable key, not list index)", async () => {
    // `plainState` is the SECOND value set in the unfiltered list. Filtering to it
    // leaves a single row whose Isolate must focus IT — not the first of the
    // unfiltered list. If isolation keyed on a list INDEX, the filtered row's
    // index 0 would wrongly isolate `classState` (= LKF 2007); keying on the
    // stable `vs.key` isolates the right one.
    await render(ValueSetView, {
      states: [classState, plainState],
      narrowed: false,
    });
    const filter = page.getByRole("textbox", { name: "Filter value sets" });
    await filter.fill("historisk");
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    await page.getByRole("button", { name: "Isolate" }).click();
    // The isolated detail shows the plain value set, NOT the classification one:
    // its heading reads "Kommun historisk" and there is no LKF classification link.
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("Kommun historisk");
    expect(
      document.querySelector('a[href="/catalog/class/lkf2007"]'),
    ).toBeNull();
  });

  // ── focusColumn deep-link (#905) ────────────────────────────────────────────
  it("re-seeds the isolation when the states change underneath (sibling navigation)", async () => {
    // The reset `$effect` (keyed on `states`) must re-run when navigation swaps the
    // states for a sibling column: a stale isolated detail can't survive into the new
    // view. Render with COL → "First coding" focused, then rerender with a DIFFERENT
    // `states` set where COL now delivers "Second coding"; the isolation must FOLLOW
    // to the new value set, not strand the old one.
    const first = state({
      state_id: "1",
      value_set_id: "401",
      value_set_version_label: "First coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2010-01-01",
      valid_to: "2012-12-31",
    });
    const firstOther = state({
      state_id: "2",
      value_set_id: "402",
      value_set_version_label: "First other",
      variant: "v",
      delivery_column_name: "OTHER",
      valid_from: "2010-01-01",
      valid_to: "2012-12-31",
    });
    const { rerender } = await render(ValueSetView, {
      states: [first, firstOther],
      narrowed: false,
      focusColumn: "COL",
    });
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("First coding");

    // Navigate to a sibling: a NEW states set where COL delivers a different coding.
    const second = state({
      state_id: "3",
      value_set_id: "501",
      value_set_version_label: "Second coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2013-01-01",
      valid_to: "2015-12-31",
    });
    const secondOther = state({
      state_id: "4",
      value_set_id: "502",
      value_set_version_label: "Second other",
      variant: "v",
      delivery_column_name: "OTHER",
      valid_from: "2013-01-01",
      valid_to: "2015-12-31",
    });
    await rerender({
      states: [second, secondOther],
      narrowed: false,
      focusColumn: "COL",
    });
    // The isolation re-seeded onto the NEW value set; the stale one is gone.
    const heading = document.querySelector(
      ".vs-detail .vs-heading",
    )?.textContent;
    expect(heading).toContain("Second coding");
    expect(heading).not.toContain("First coding");
  });

  it("isolates a focusColumn value set even when it is OUT of period (isolate beats period-collapse)", async () => {
    // A `?period` collapses out-of-period value sets behind a disclosure (#744), but
    // a `?codes=<column>` deep link must still land on its target even when that
    // column's value set falls OUTSIDE the period. The isolate path (keyed on the
    // value set regardless of period) takes precedence over the period-collapse: the
    // focused detail renders fully, not buried under "… outside this period".
    const inPeriodCol = state({
      state_id: "1",
      value_set_id: "600",
      value_set_version_label: "In-period coding",
      variant: "v",
      delivery_column_name: "INCOL",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const outOfPeriodCol = state({
      state_id: "2",
      value_set_id: "601",
      value_set_version_label: "Out-of-period coding",
      variant: "v",
      delivery_column_name: "OUTCOL",
      valid_from: "1990-01-01",
      valid_to: "1995-12-31",
    });
    await render(ValueSetView, {
      states: [inPeriodCol, outOfPeriodCol],
      // scopeStates covers ONLY the in-period value set.
      scopeStates: [inPeriodCol],
      narrowed: true,
      // …but the deep link focuses the OUT-of-period column.
      focusColumn: "OUTCOL",
    });
    // The isolated detail is fully visible (not collapsed): its heading shows the
    // out-of-period coding and "Used by" is present, with no period disclosure in
    // the way.
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("Out-of-period coding");
    // The period-collapse disclosure does not gate the focused detail.
    expect(document.querySelector(".out-of-period")).toBeNull();
  });

  it("focusColumn degrades to the default union when no state delivers it", async () => {
    // A stale / unknown `?codes=` matches nothing → the viewer shows its default
    // union list, not a blank isolated detail.
    const classCol = state({ ...classState, delivery_column_name: "CLASSCOL" });
    const plainCol = state({ ...plainState, delivery_column_name: "PLAINCOL" });
    await render(ValueSetView, {
      states: [classCol, plainCol],
      narrowed: false,
      focusColumn: "NOPE",
    });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
    expect(document.querySelector(".vs-detail")).toBeNull();
  });
});
