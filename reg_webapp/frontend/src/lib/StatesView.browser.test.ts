import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { VariableStateModel } from "./api";
import StatesView from "./StatesView.svelte";

// The value-set-centric multi-state view (#668 — dogfooding M13/M18/M20). The
// kommun shape blows up by VINTAGE (415 states); the view dedups at TWO levels —
// classification editions by `classification_slug`, others by `value_set_id` —
// and shows DISTINCT value sets, classification ones linking out (no code dump).
// A FilterInput + per-row Isolate replace the old all-chips strip. Single-state
// detail + the empty mode are unchanged.

// Minimal VariableStateModel — only the fields StatesView reads.
function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "v",
    register_variant_id: 1,
    valid_from: "2000-01-01",
    valid_to: "2000-12-31",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
    period_token: null,
    ...over,
  };
}

// Required callbacks — no-ops; the view is presentational, the URL writes are
// BindingLeafView's. Local isolation is independent of these.
const noopCallbacks = {
  onpickVariant: () => {},
  onpickValueSetVersion: () => {},
};

// A two-value-set fixture mirroring kommun: one classification value set (links
// out, no codes), one plain value set (expandable codes).
const classState = state({
  state_id: 1,
  value_set_id: 100,
  classification_slug: "lkf2007",
  value_set_version_label: "LKF",
  variant: "doda",
  valid_from: "2007-01-01",
  valid_to: "2010-12-31",
});
const plainState = state({
  state_id: 2,
  value_set_id: 200,
  classification_slug: null,
  value_set_version_label: "Kommun historisk",
  variant: "fodda",
  valid_from: "1961-01-01",
  valid_to: "1967-12-31",
  value_set: [
    { code: "0114", label: "Upplands Väsby" },
    { code: "0115", label: "Vallentuna" },
  ],
});

describe("StatesView — value-set-centric multi-state view (#668)", () => {
  it("renders DISTINCT value sets, not raw states (the dedup)", async () => {
    // Four states, two value sets → two rows in the union list.
    const states = [
      classState,
      state({ ...classState, state_id: 3, valid_from: "2011-01-01" }),
      plainState,
      state({ ...plainState, state_id: 4, valid_from: "1968-01-01" }),
    ];
    render(StatesView, { states, narrowed: false, ...noopCallbacks });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
  });

  it("a classification value set shows the '= LKF ⟨vintage⟩' link, NOT a code dump", async () => {
    render(StatesView, {
      states: [classState, plainState],
      narrowed: false,
      ...noopCallbacks,
    });
    // The classification row links out to the classification.
    const link = page.getByRole("link", { name: "= LKF 2007" });
    await expect.element(link).toBeVisible();
    expect(link.element().getAttribute("href")).toBe("/catalog/class/lkf2007");
  });

  it("a plain value set exposes its codes inline (expandable), not the classification link", async () => {
    // ≥2 states → the multi-state union (one state alone is single-state DETAIL).
    render(StatesView, {
      states: [plainState, classState],
      narrowed: false,
      ...noopCallbacks,
    });
    // The "Values (2)" disclosure is present (the plain value set); expanding
    // reveals the code rows.
    const summary = page.getByText("Values (2)");
    await expect.element(summary).toBeVisible();
    await summary.click();
    await expect.element(page.getByText("Upplands Väsby")).toBeVisible();
  });

  it("collapses several value_set_ids that share one classification_slug into ONE row (M13)", async () => {
    // The duplicate-LKF-row bug: SCB ships ≥2 distinct value_set_ids per LKF
    // edition. Two such states for lkf2007 must render ONE "= LKF 2007" row, not
    // two — plus the one plain value set → two rows total.
    const states = [
      classState, // lkf2007, value_set_id 100
      state({ ...classState, state_id: 9, value_set_id: 101 }), // SAME edition, distinct id
      plainState,
    ];
    render(StatesView, { states, narrowed: false, ...noopCallbacks });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
    // Exactly one "= LKF 2007" link (no duplicate row).
    expect(
      document.querySelectorAll('a[href="/catalog/class/lkf2007"]'),
    ).toHaveLength(1);
  });

  it("disambiguates non-classification rows that share a version label by span", async () => {
    // Two plain value sets both labelled "Kommun historisk" (kommun's ×22 case):
    // the bare label can't tell them apart, so each row appends its overall span.
    const a = state({
      state_id: 1,
      value_set_id: 10,
      value_set_version_label: "Kommun historisk",
      variant: "a",
      valid_from: "1968-01-01",
      valid_to: "1970-12-31",
    });
    const b = state({
      state_id: 2,
      value_set_id: 11,
      value_set_version_label: "Kommun historisk",
      variant: "a",
      valid_from: "1971-01-01",
      valid_to: "1973-12-31",
    });
    render(StatesView, { states: [a, b], narrowed: false, ...noopCallbacks });
    const labels = [...document.querySelectorAll(".vs-label")].map(
      (el) => el.textContent,
    );
    expect(labels).toEqual([
      "Kommun historisk · 1968 – 1970",
      "Kommun historisk · 1971 – 1973",
    ]);
  });

  it("per-row Isolate focuses one value set; '← All value sets' returns to the union", async () => {
    render(StatesView, {
      states: [classState, plainState],
      narrowed: false,
      ...noopCallbacks,
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

  it("the FilterInput narrows the union list (by label / variant slug)", async () => {
    render(StatesView, {
      states: [classState, plainState],
      narrowed: false,
      ...noopCallbacks,
    });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
    // Filter to the plain value set by its label substring.
    const filter = page.getByRole("textbox", { name: "Filter value sets" });
    await filter.fill("historisk");
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    await expect
      .element(page.getByText("Kommun historisk", { exact: true }))
      .toBeVisible();
  });

  it("Isolate after filtering isolates the FILTERED value set (stable key, not list index)", async () => {
    // `plainState` is the SECOND value set in the unfiltered list. Filtering to it
    // leaves a single row whose Isolate must focus IT — not the first of the
    // unfiltered list. If isolation keyed on a list INDEX, the filtered row's
    // index 0 would wrongly isolate `classState` (= LKF 2007); keying on the
    // stable `vs.key` isolates the right one.
    render(StatesView, {
      states: [classState, plainState],
      narrowed: false,
      ...noopCallbacks,
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

  it("a plain value set with no inline value_set renders 'No value set.' when isolated", async () => {
    // A plain (non-classification) value set whose `value_set` is null/empty: the
    // isolated body has no codes to dump and no classification to link, so it
    // reads the explicit "No value set." rather than rendering nothing.
    const codeless = state({
      state_id: 1,
      value_set_id: 300,
      value_set_version_label: "Codeless",
      variant: "a",
      value_set: null,
    });
    const other = state({
      state_id: 2,
      value_set_id: 301,
      value_set_version_label: "Other",
      variant: "a",
    });
    render(StatesView, {
      states: [codeless, other],
      narrowed: false,
      ...noopCallbacks,
    });
    await page.getByRole("button", { name: "Isolate" }).first().click();
    await expect.element(page.getByText("No value set.")).toBeVisible();
  });

  it("narrowed=false with multiple variants does NOT render the narrowing picker", async () => {
    // The narrowing picker (`.picker`) writes `?variant`/`?value_set_version`,
    // which the server only honors WITH a `?period` — so it must NOT appear in the
    // full-history (`narrowed: false`) view, even when several variants exist.
    render(StatesView, {
      states: [classState, plainState], // distinct variants doda / fodda
      narrowed: false,
      ...noopCallbacks,
    });
    expect(document.querySelector(".picker")).toBeNull();
  });

  it("greys an out-of-scope value set when a variant is active", async () => {
    // `lkf2007` is used by `doda`, `Kommun historisk` by `fodda`. Pinning variant
    // `doda` greys the `fodda`-only value set (greyed, NOT removed).
    render(StatesView, {
      states: [classState, plainState],
      narrowed: true,
      activeVariant: "doda",
      ...noopCallbacks,
    });
    const rows = document.querySelectorAll<HTMLLIElement>(".vs-list > li");
    expect(rows).toHaveLength(2);
    const greyed = [...rows].filter((li) =>
      li.classList.contains("out-of-scope"),
    );
    // Exactly the fodda-only value set is greyed.
    expect(greyed).toHaveLength(1);
    expect(greyed[0].textContent).toContain("Kommun historisk");
  });

  it("renders technical-change hints inside a folded value-set usage (#743)", async () => {
    const states = [
      state({
        state_id: 1,
        value_set_id: 300,
        value_set_version_label: "Kommun historisk",
        variant: "doda",
        valid_from: "2010-01-01",
        valid_to: "2010-12-31",
        data_type: "int",
        delivery_column_name: "KOMMUN",
      }),
      state({
        state_id: 2,
        value_set_id: 300,
        value_set_version_label: "Kommun historisk",
        variant: "doda",
        valid_from: "2011-01-01",
        valid_to: "2011-12-31",
        data_type: "bigint",
        delivery_column_name: "KOMMUN_ID",
      }),
    ];
    render(StatesView, { states, narrowed: false, ...noopCallbacks });
    await expect
      .element(
        page.getByText(
          "changed 2011: type int -> bigint; column KOMMUN -> KOMMUN_ID",
        ),
      )
      .toBeVisible();
  });

  it("collapses period-out-of-scope value sets behind a disclosure (#744)", async () => {
    const inScopePlain = state({
      state_id: 3,
      value_set_id: 201,
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    render(StatesView, {
      states: [classState, inScopePlain, plainState],
      scopeStates: [classState, inScopePlain],
      narrowed: true,
      ...noopCallbacks,
    });
    const inlineRows = document.querySelectorAll(
      "ul.vs-list:not(.out-of-period-list) > li",
    );
    expect(inlineRows).toHaveLength(2);
    expect(inlineRows[0].textContent).toContain("LKF 2007");
    expect(
      [...inlineRows].map((row) => row.textContent).join(" "),
    ).not.toContain("Kommun historisk");

    const disclosure = page.getByText("1 value set outside this period");
    await expect.element(disclosure).toBeVisible();
    await expect
      .element(page.getByText("Kommun historisk", { exact: true }))
      .not.toBeVisible();
    await disclosure.click();
    await expect
      .element(page.getByText("Kommun historisk", { exact: true }))
      .toBeVisible();
  });

  it("keeps same-period other-variant value sets inline and greyed (#744 review)", async () => {
    const samePeriodOtherVariant = state({
      state_id: 3,
      value_set_id: 201,
      value_set_version_label: "Same-period other variant",
      variant: "fodda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    render(StatesView, {
      states: [classState, samePeriodOtherVariant, plainState],
      scopeStates: [classState, samePeriodOtherVariant],
      narrowed: true,
      activeVariant: "doda",
      ...noopCallbacks,
    });
    const inlineRows = [
      ...document.querySelectorAll<HTMLLIElement>(
        "ul.vs-list:not(.out-of-period-list) > li",
      ),
    ];
    expect(inlineRows).toHaveLength(2);
    const otherVariantRow = inlineRows.find((row) =>
      row.textContent?.includes("Same-period other variant"),
    );
    expect(otherVariantRow).toBeDefined();
    expect(otherVariantRow?.classList.contains("out-of-scope")).toBe(true);
    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
  });

  it("keeps single-state detail resolution when full history is available (#744)", async () => {
    render(StatesView, {
      states: [classState, plainState],
      scopeStates: [classState],
      narrowed: true,
      ...noopCallbacks,
    });
    await expect.element(page.getByText("Variant")).toBeVisible();
    expect(document.querySelector(".vs-list")).toBeNull();
  });

  it("single-state DETAIL mode is unchanged (Variant / Valid / value set)", async () => {
    render(StatesView, {
      states: [
        state({
          variant: "doda",
          value_set_version_label: "Kommun historisk",
          value_set: [{ code: "0114", label: "Upplands Väsby" }],
        }),
      ],
      narrowed: false,
      ...noopCallbacks,
    });
    // The single-state detail renders its own dl.meta + the value-set heading —
    // NOT the multi-state value-set list UI (`.vs-list`, which only the >1-state
    // view emits), so this really guards the single/multi boundary.
    await expect.element(page.getByText("Variant")).toBeVisible();
    expect(document.querySelector(".vs-list")).toBeNull();
    await expect.element(page.getByText("Upplands Väsby")).toBeVisible();
  });

  it("empty mode is unchanged (clean no-state message, not an error)", async () => {
    render(StatesView, { states: [], narrowed: true, ...noopCallbacks });
    await expect
      .element(page.getByText("No state delivered for this period."))
      .toBeVisible();
  });
});
