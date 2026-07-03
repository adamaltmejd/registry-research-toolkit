import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { VariableStateModel } from "./api";
import ValueSetView from "./ValueSetView.svelte";

// The pure value-set / coding viewer (#905 — extracted from the retired StatesView,
// #668 dogfooding M13/M18/M20). The kommun shape blows up by VINTAGE (415 states);
// the view dedups at TWO levels — classification editions by `classification_slug`,
// others by `value_set_id` — and shows DISTINCT value sets, classification ones
// linking out (no code dump). A FilterInput + per-row Isolate replace the old
// all-chips strip. A `?codes=<column>` deep link (the picker's "codings vary"
// nudge) seeds the isolation via `focusColumn`. Single-state detail + the empty
// mode are unchanged. NO resolution (variant / value-set version) plumbing — the
// picker owns that now.

// Minimal VariableStateModel — only the fields ValueSetView reads.
function state(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    state_id: 1,
    variant: "v",
    variant_label: null,
    register_variant_id: 1,
    valid_from: "2000-01-01",
    valid_to: "2000-12-31",
    data_type: null,
    data_length: null,
    delivery_column_name: null,
    source_register_text: null,
    value_set_version_label: "",
    value_set_id: null,
    value_set: null,
    is_identifier: false,
    classification_slug: null,
    classification_conformance: null,
    period_token: null,
    ...over,
  };
}

function normalizedText(selector: string): string {
  return (
    document
      .querySelector(selector)
      ?.textContent?.replace(/\s+/g, " ")
      .trim() ?? ""
  );
}

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

describe("ValueSetView — value-set-centric multi-state view (#668/#905)", () => {
  it("renders DISTINCT value sets, not raw states (the dedup)", async () => {
    // Four states, two value sets → two rows in the union list.
    const states = [
      classState,
      state({ ...classState, state_id: 3, valid_from: "2011-01-01" }),
      plainState,
      state({ ...plainState, state_id: 4, valid_from: "1968-01-01" }),
    ];
    render(ValueSetView, { states, narrowed: false });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
  });

  it("shows state operational definitions when parallel columns share one value set (#736)", async () => {
    const states = [
      state({
        state_id: 10,
        value_set_id: 500,
        value_set_version_label: "vald/inte vald",
        delivery_column_name: "fedunsatreason_1",
        operational_definition: "Education was not relevant to work",
        value_set: [
          { code: "0", label: "Inte vald" },
          { code: "1", label: "Vald" },
        ],
      }),
      state({
        state_id: 11,
        value_set_id: 500,
        value_set_version_label: "vald/inte vald",
        delivery_column_name: "fedunsatreason_2",
        operational_definition: "Education was too theoretical",
        value_set: [
          { code: "0", label: "Inte vald" },
          { code: "1", label: "Vald" },
        ],
      }),
    ];

    render(ValueSetView, { states, narrowed: false });

    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    await expect.element(page.getByText("fedunsatreason_1")).toBeVisible();
    await expect
      .element(page.getByText("Education was not relevant to work"))
      .toBeVisible();
    await expect.element(page.getByText("fedunsatreason_2")).toBeVisible();
    await expect
      .element(page.getByText("Education was too theoretical"))
      .toBeVisible();
  });

  it("renders expanded state definitions with duplicate source state ids (#736)", async () => {
    const states = [
      state({
        state_id: 20,
        value_set_id: 600,
        value_set_version_label: "expanded",
        delivery_column_name: "month_jan",
        operational_definition: "January expanded state",
        valid_from: "2020-01-01",
        valid_to: "2020-01-31",
        value_set: [
          { code: "0", label: "No" },
          { code: "1", label: "Yes" },
        ],
      }),
      state({
        state_id: 20,
        value_set_id: 600,
        value_set_version_label: "expanded",
        delivery_column_name: "month_feb",
        operational_definition: "February expanded state",
        valid_from: "2020-02-01",
        valid_to: "2020-02-29",
        value_set: [
          { code: "0", label: "No" },
          { code: "1", label: "Yes" },
        ],
      }),
    ];

    render(ValueSetView, { states, narrowed: false });

    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(1);
    await expect.element(page.getByText("month_jan")).toBeVisible();
    await expect
      .element(page.getByText("January expanded state"))
      .toBeVisible();
    await expect.element(page.getByText("month_feb")).toBeVisible();
    await expect
      .element(page.getByText("February expanded state"))
      .toBeVisible();
  });

  it("disambiguates repeated definition column labels by state window (#736)", async () => {
    const states = [
      state({
        state_id: 30,
        value_set_id: 700,
        value_set_version_label: "stable",
        delivery_column_name: "reason",
        operational_definition: "Early definition",
        valid_from: "2010-01-01",
        valid_to: "2010-12-31",
        value_set: [
          { code: "0", label: "No" },
          { code: "1", label: "Yes" },
        ],
      }),
      state({
        state_id: 31,
        value_set_id: 700,
        value_set_version_label: "stable",
        delivery_column_name: "reason",
        operational_definition: "Later definition",
        valid_from: "2011-01-01",
        valid_to: "2011-12-31",
        value_set: [
          { code: "0", label: "No" },
          { code: "1", label: "Yes" },
        ],
      }),
    ];

    render(ValueSetView, { states, narrowed: false });

    await expect.element(page.getByText("reason (2010)")).toBeVisible();
    await expect.element(page.getByText("Early definition")).toBeVisible();
    await expect.element(page.getByText("reason (2011)")).toBeVisible();
    await expect.element(page.getByText("Later definition")).toBeVisible();
  });

  it("a classification value set shows the '= LKF ⟨vintage⟩' link, NOT a code dump", async () => {
    render(ValueSetView, {
      states: [classState, plainState],
      narrowed: false,
    });
    // The classification row links out to the classification.
    const link = page.getByRole("link", { name: "= LKF 2007" });
    await expect.element(link).toBeVisible();
    expect(link.element().getAttribute("href")).toBe("/catalog/class/lkf2007");
  });

  it("warns on nonconforming codes while keeping a classification link", async () => {
    render(ValueSetView, {
      states: [
        state({
          ...classState,
          classification_conformance: {
            declared_classification_slug: "lkf2007",
            declared_classification_short_name: "LKF2007",
            declared_classification_name: "Kommun historisk",
            status: "kept",
            checked_code_count: 3,
            matched_code_count: 2,
            nonconforming_code_count: 1,
            overlap: 2 / 3,
            nonconforming_codes: [{ code: "X", label: "Extra code" }],
          },
        }),
        plainState,
      ],
      narrowed: false,
    });
    expect(normalizedText(".conformance-notice")).toContain(
      "kept, but 1 code is not part",
    );
    const summary = page.getByText("Nonconforming codes (1)");
    await summary.click();
    await expect.element(page.getByText("Extra code")).toBeVisible();
  });

  it("shows severed classification evidence on the plain value-set row", async () => {
    render(ValueSetView, {
      states: [
        state({
          value_set_id: 300,
          value_set_version_label: "ISCED F 2013",
          value_set: [
            { code: "13", label: "Datavetenskap" },
            { code: "1a", label: "Pedagogik" },
          ],
          classification_conformance: {
            declared_classification_slug: "isced-f2013",
            declared_classification_short_name: "ISCED-F 2013",
            declared_classification_name: "ISCED-F 2013",
            status: "severed",
            checked_code_count: 25,
            matched_code_count: 1,
            nonconforming_code_count: 24,
            overlap: 0.04,
            nonconforming_codes: [{ code: "13", label: "Datavetenskap" }],
          },
        }),
        plainState,
      ],
      narrowed: false,
    });
    expect(normalizedText(".conformance-notice")).toContain(
      "severed: 4% of checked codes match",
    );
    await expect
      .element(page.getByRole("link", { name: "= ISCED-F 2013" }))
      .not.toBeInTheDocument();
    expect(
      [...document.querySelectorAll("summary")].filter(
        (el) => el.textContent?.trim() === "Values (2)",
      ).length,
    ).toBeGreaterThanOrEqual(1);
  });

  it("a plain value set exposes its codes inline (expandable), not the classification link", async () => {
    // ≥2 states → the multi-state union (one state alone is single-state DETAIL).
    render(ValueSetView, {
      states: [plainState, classState],
      narrowed: false,
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
    render(ValueSetView, { states, narrowed: false });
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
    render(ValueSetView, { states: [a, b], narrowed: false });
    const labels = [...document.querySelectorAll(".vs-label")].map(
      (el) => el.textContent,
    );
    expect(labels).toEqual([
      "Kommun historisk · 1968 – 1970",
      "Kommun historisk · 1971 – 1973",
    ]);
  });

  it("per-row Isolate focuses one value set; '← All value sets' returns to the union", async () => {
    render(ValueSetView, {
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

  it("the FilterInput narrows the union list (by label / variant slug)", async () => {
    render(ValueSetView, {
      states: [classState, plainState],
      narrowed: false,
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
    render(ValueSetView, {
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
    render(ValueSetView, {
      states: [codeless, other],
      narrowed: false,
    });
    await page.getByRole("button", { name: "Isolate" }).first().click();
    await expect.element(page.getByText("No value set.")).toBeVisible();
  });

  it("does NOT render any resolution-narrowing picker (the picker owns that now)", async () => {
    // #905: the old variant / value-set-version chips moved to RepresentationPicker.
    // The viewer is pure display — no `.picker` fieldset, regardless of variant
    // multiplicity.
    render(ValueSetView, {
      states: [classState, plainState], // distinct variants doda / fodda
      narrowed: false,
    });
    expect(document.querySelector(".picker")).toBeNull();
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
    render(ValueSetView, { states, narrowed: false });
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
    render(ValueSetView, {
      states: [classState, inScopePlain, plainState],
      scopeStates: [classState, inScopePlain],
      narrowed: true,
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

  it("counts filtered matches inside the outside-period disclosure (#744 review)", async () => {
    const inScopePlain = state({
      state_id: 3,
      value_set_id: 201,
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    render(ValueSetView, {
      states: [classState, inScopePlain, plainState],
      scopeStates: [classState, inScopePlain],
      narrowed: true,
    });
    const filter = page.getByRole("textbox", { name: "Filter value sets" });
    await filter.fill("historisk");
    await expect.element(page.getByText("1 of 3")).toBeVisible();
    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
  });

  it("single-state DETAIL mode is unchanged (Variant / Valid / value set)", async () => {
    render(ValueSetView, {
      states: [
        state({
          variant: "doda",
          value_set_version_label: "Kommun historisk",
          value_set: [{ code: "0114", label: "Upplands Väsby" }],
        }),
      ],
      narrowed: false,
    });
    // The single-state detail renders its own dl.meta + the value-set heading —
    // NOT the multi-state value-set list UI (`.vs-list`, which only the >1-state
    // view emits), so this really guards the single/multi boundary.
    await expect.element(page.getByText("Variant")).toBeVisible();
    expect(document.querySelector(".vs-list")).toBeNull();
    await expect.element(page.getByText("Upplands Väsby")).toBeVisible();
  });

  it("empty mode is unchanged (clean no-state message, not an error)", async () => {
    render(ValueSetView, { states: [], narrowed: true });
    await expect
      .element(page.getByText("No state delivered for this period."))
      .toBeVisible();
  });

  it("single-state detail is PERIOD-AWARE: a 1-state variable viewed OUTSIDE its window shows the no-state message, NOT the detail (Fix C)", async () => {
    // #905, Codex P2: a variable with exactly ONE historical state, viewed at a
    // `?period` OUTSIDE that state. The leaf passes the full history (1 state) but an
    // EMPTY period scope. `single` must key off the SCOPE (zero in-period → no single
    // detail) and fall through to the "No state delivered" path — not render the lone
    // state's detail as if it were in-period.
    const lone = state({
      variant: "doda",
      value_set_version_label: "Kommun historisk",
      value_set: [{ code: "0114", label: "Upplands Väsby" }],
      valid_from: "2007-01-01",
      valid_to: "2010-12-31",
    });
    render(ValueSetView, {
      states: [lone],
      scopeStates: [], // the period delivered ZERO of this variable's states
      narrowed: true,
    });
    // Full history (1 state) is present, so this lands in the multi-state branch's
    // empty-period hint (NOT the bare empty branch) — but it still tells the user no
    // state was delivered for the period, and the historical state is collapsed.
    await expect
      .element(
        page.getByText(/No state delivered for this period\./, {
          exact: false,
        }),
      )
      .toBeVisible();
    // The single-state DETAIL block must NOT render (the lone state is collapsed as a
    // historical value set, not surfaced as the in-period detail).
    expect(document.querySelector(".state-detail")).toBeNull();
  });

  it("single-state detail is PERIOD-AWARE: a 1-state variable viewed IN its window still shows the detail (Fix C)", async () => {
    // The control: the SAME lone state, with a period scope that DID deliver it →
    // exactly one in-period state → the single-state detail renders.
    const lone = state({
      variant: "doda",
      value_set_version_label: "Kommun historisk",
      value_set: [{ code: "0114", label: "Upplands Väsby" }],
      valid_from: "2007-01-01",
      valid_to: "2010-12-31",
    });
    render(ValueSetView, {
      states: [lone],
      scopeStates: [lone],
      narrowed: true,
    });
    await expect.element(page.getByText("Variant")).toBeVisible();
    expect(document.querySelector(".vs-list")).toBeNull();
    await expect.element(page.getByText("Upplands Väsby")).toBeVisible();
  });

  it("a filter that hides the in-period rows does NOT mis-report 'No state delivered for this period' (Codex P3)", async () => {
    // The period DID deliver in-period value sets (classState + inScopePlain), but a
    // text filter matches only the OUT-of-period row's variant ("fodda"). The empty
    // hint must key off the UNFILTERED period scope (which is non-empty), so it must
    // NOT appear — the union branch's own "no matches" describes the filtered-out
    // state instead.
    const inScopePlain = state({
      state_id: 3,
      value_set_id: 201,
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    render(ValueSetView, {
      states: [classState, inScopePlain, plainState],
      scopeStates: [classState, inScopePlain],
      narrowed: true,
    });
    const filter = page.getByRole("textbox", { name: "Filter value sets" });
    // "fodda" is only the out-of-period plainState's variant → in-period shown rows
    // become empty, but the out-of-period row still matches and stays collapsed.
    await filter.fill("fodda");
    // The out-of-period row stays collapsed (the filter matched it), which renders the
    // union branch — proving we did NOT fall into the "No state delivered" branch.
    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    // The mis-report must be entirely absent from the DOM (the in-period scope is
    // non-empty), not merely hidden.
    expect(document.body.textContent).not.toContain(
      "No state delivered for this period.",
    );
  });

  // ── focusColumn deep-link (#905) ────────────────────────────────────────────
  it("focusColumn auto-isolates the distinct value set its column delivers", async () => {
    // `plainState` is delivered via column PLAINCOL; the `?codes=PLAINCOL` deep link
    // (focusColumn) seeds the isolation onto its value set, NOT the classification
    // one — the union list is hidden and the isolated detail shows it.
    const classCol = state({ ...classState, delivery_column_name: "CLASSCOL" });
    const plainCol = state({ ...plainState, delivery_column_name: "PLAINCOL" });
    render(ValueSetView, {
      states: [classCol, plainCol],
      narrowed: false,
      focusColumn: "PLAINCOL",
    });
    // Isolated → no union rows, the detail's "Used by" + the plain value set's
    // heading are visible.
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(0);
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("Kommun historisk");
  });

  it("focusColumn on a coding-VARYING column isolates the LATEST-era value set", async () => {
    // One column delivered two distinct value sets over time (a coding change):
    // the deep link isolates the LATEST-era one (max valid_to) — the picker row's
    // representative coding. The earlier coding stays one "← All value sets" away.
    const early = state({
      state_id: 1,
      value_set_id: 303,
      value_set_version_label: "Old coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2015-01-01",
      valid_to: "2018-12-31",
    });
    const latest = state({
      state_id: 2,
      value_set_id: 249,
      value_set_version_label: "New coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2019-01-01",
      valid_to: "2022-12-31",
    });
    render(ValueSetView, {
      states: [early, latest],
      narrowed: false,
      focusColumn: "COL",
    });
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("New coding");
    // The reset returns to the union showing BOTH codings.
    await page.getByRole("button", { name: "← All value sets" }).click();
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
  });

  it("re-seeds the isolation when the states change underneath (sibling navigation)", async () => {
    // The reset `$effect` (keyed on `states`) must re-run when navigation swaps the
    // states for a sibling column: a stale isolated detail can't survive into the new
    // view. Render with COL → "First coding" focused, then rerender with a DIFFERENT
    // `states` set where COL now delivers "Second coding"; the isolation must FOLLOW
    // to the new value set, not strand the old one.
    const first = state({
      state_id: 1,
      value_set_id: 401,
      value_set_version_label: "First coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2010-01-01",
      valid_to: "2012-12-31",
    });
    const firstOther = state({
      state_id: 2,
      value_set_id: 402,
      value_set_version_label: "First other",
      variant: "v",
      delivery_column_name: "OTHER",
      valid_from: "2010-01-01",
      valid_to: "2012-12-31",
    });
    const { rerender } = render(ValueSetView, {
      states: [first, firstOther],
      narrowed: false,
      focusColumn: "COL",
    });
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("First coding");

    // Navigate to a sibling: a NEW states set where COL delivers a different coding.
    const second = state({
      state_id: 3,
      value_set_id: 501,
      value_set_version_label: "Second coding",
      variant: "v",
      delivery_column_name: "COL",
      valid_from: "2013-01-01",
      valid_to: "2015-12-31",
    });
    const secondOther = state({
      state_id: 4,
      value_set_id: 502,
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
      state_id: 1,
      value_set_id: 600,
      value_set_version_label: "In-period coding",
      variant: "v",
      delivery_column_name: "INCOL",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const outOfPeriodCol = state({
      state_id: 2,
      value_set_id: 601,
      value_set_version_label: "Out-of-period coding",
      variant: "v",
      delivery_column_name: "OUTCOL",
      valid_from: "1990-01-01",
      valid_to: "1995-12-31",
    });
    render(ValueSetView, {
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
    render(ValueSetView, {
      states: [classCol, plainCol],
      narrowed: false,
      focusColumn: "NOPE",
    });
    expect(document.querySelectorAll(".vs-list > li")).toHaveLength(2);
    expect(document.querySelector(".vs-detail")).toBeNull();
  });

  it("focusVariant isolates the clicked variant's coding when a column is shared across variants (#905)", async () => {
    // One delivery column COL delivered by TWO variants with DISTINCT codings —
    // picker rows are keyed `(variant, column)`, so the deep link carries the variant.
    // `focusVariant: "a"` must isolate variant a's coding, NOT variant b's latest-era
    // one (the unscoped column lookup would pick b).
    const a = state({
      state_id: 1,
      value_set_id: 100,
      value_set_version_label: "Coding A",
      variant: "a",
      delivery_column_name: "COL",
      valid_from: "2015-01-01",
      valid_to: "2018-12-31",
    });
    const b = state({
      state_id: 2,
      value_set_id: 200,
      value_set_version_label: "Coding B",
      variant: "b",
      delivery_column_name: "COL",
      valid_from: "2019-01-01",
      valid_to: "2022-12-31",
    });
    const { rerender } = render(ValueSetView, {
      states: [a, b],
      narrowed: false,
      focusColumn: "COL",
      focusVariant: "a",
    });
    await expect.element(page.getByText("Used by")).toBeVisible();
    const headingA = document.querySelector(
      ".vs-detail .vs-heading",
    )?.textContent;
    expect(headingA).toContain("Coding A");
    expect(headingA).not.toContain("Coding B");
    // Re-render with the OTHER variant: same column, the other row's coding. The
    // reset $effect re-seeds the isolation onto b's value set.
    await rerender({
      states: [a, b],
      narrowed: false,
      focusColumn: "COL",
      focusVariant: "b",
    });
    const headingB = document.querySelector(
      ".vs-detail .vs-heading",
    )?.textContent;
    expect(headingB).toContain("Coding B");
    expect(headingB).not.toContain("Coding A");
  });
});
