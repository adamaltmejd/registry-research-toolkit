import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { getValues } from "./api";
import ValueSetView from "./ValueSetView.svelte";
import {
  classState,
  coding,
  FQID,
  plainState,
  serveValues,
  state,
} from "./value-set-view-test-helpers";

// Split from ValueSetView.browser.test.ts by contract surface:
// period scope (out-of-period collapse, empty and period-aware single-state modes).
// Siblings: ValueSetView.browser.test.ts, ValueSetView.{conformance,isolate,period}.browser.test.ts.

// Y-46: the states carry each coding's IDENTITY and a cardinality-independent
// summary, never its members — so the code tables here are BOUNDED READS. The
// stub below serves them out of a per-test registry, filtering and windowing in
// the same order the route does (filter the whole set, then page it).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getValues: vi.fn() };
});

beforeEach(() => {
  vi.mocked(getValues).mockReset();
  vi.mocked(getValues).mockImplementation(serveValues);
});

describe("ValueSetView — value-set-centric multi-state view (#668/#905)", () => {
  it("collapses period-out-of-scope value sets behind a disclosure (#744)", async () => {
    const inScopePlain = state({
      state_id: "3",
      value_set_id: "201",
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    await render(ValueSetView, {
      fqid: FQID,
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
      state_id: "3",
      value_set_id: "201",
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    await render(ValueSetView, {
      fqid: FQID,
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

  it("empty mode is unchanged (clean no-state message, not an error)", async () => {
    await render(ValueSetView, { fqid: FQID, states: [], narrowed: true });
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
      value_set_id: "900",
      value_set_summary: coding(900, [
        { code: "0114", label: "Upplands Väsby" },
      ]),
      valid_from: "2007-01-01",
      valid_to: "2010-12-31",
    });
    await render(ValueSetView, {
      fqid: FQID,
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
      value_set_id: "900",
      value_set_summary: coding(900, [
        { code: "0114", label: "Upplands Väsby" },
      ]),
      valid_from: "2007-01-01",
      valid_to: "2010-12-31",
    });
    await render(ValueSetView, {
      fqid: FQID,
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
      state_id: "3",
      value_set_id: "201",
      value_set_version_label: "In-period plain",
      variant: "doda",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    await render(ValueSetView, {
      fqid: FQID,
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
});
