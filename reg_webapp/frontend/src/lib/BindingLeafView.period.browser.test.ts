import { beforeEach, describe, expect, it, vi } from "vitest";
import { page, userEvent } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { StatesResponse } from "./api";
import {
  getBindingGraph,
  getBindingLineageWarnings,
  getCatalogNode,
  getDocsForVariable,
  getValueSetCodes,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import {
  node,
  pickerStates,
  SEED,
  state,
  statesResponse,
} from "./binding-leaf-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Split from BindingLeafView.browser.test.ts by contract surface: period-scoped history, pending resolve, study window.
// Siblings: BindingLeafView{,.graph,.rows,.technical-details,.provenance,.period}.browser.test.ts.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getDataWarnings: vi.fn().mockResolvedValue([]),
    getCatalogNode: vi.fn(),
    getBindingGraph: vi.fn(),
    getBindingLineageWarnings: vi.fn(),
    getDocsForVariable: vi.fn(),
    getValueSetCodes: vi.fn(),
  };
});

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
    const variant =
      typeof params?.variant === "string" ? params.variant : undefined;
    return statesResponse(
      pickerStates.filter(
        (s) => variant === undefined || s.variant === variant,
      ),
    );
  });
  // The graph fetch: an EMPTY graph by default (no nodes) → the picker uses the list
  // itself and the header derives no qualifier. Member-identity cases override it.
  vi.mocked(getBindingGraph).mockReset();
  vi.mocked(getBindingGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  } as never);
  vi.mocked(getBindingLineageWarnings).mockReset();
  vi.mocked(getBindingLineageWarnings).mockResolvedValue({
    binding: "scb/lisa/kon",
    lineage_warnings: [],
  } as never);
  vi.mocked(getValueSetCodes).mockReset();
  vi.mocked(getValueSetCodes).mockImplementation(
    async (valueSetId, { state = null, q = "", offset = 0, limit = 200 }) => {
      const codes =
        valueSetId === "814"
          ? [
              { code: "0", label: "Nej" },
              { code: "1", label: "Ja" },
            ]
          : [];
      return {
        value_set_id: String(valueSetId),
        state_id: state,
        period_scope: "intervals",
        q,
        total: codes.length,
        offset,
        limit,
        codes: codes.slice(offset, offset + limit),
      };
    },
  );
  vi.mocked(getDocsForVariable).mockReset();
  vi.mocked(getDocsForVariable).mockResolvedValue({
    results: [],
    total_count: 0,
  } as never);
  // No `?period` — the embedded states drive the plan.
  window.history.pushState({}, "", "/__reset__");
  router.navigate("/catalog/scb/lisa/kon");
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("BindingLeafView period-scoped value-set history (#744)", () => {
  it("narrows the value-set list to the active variant modifier, keeping a period-only outside-period scope (Codex P2)", async () => {
    // #905, Codex P2: with `?variant` active the page shows a "Narrowed by" chip and
    // the picker is scoped via `narrowStatesByModifier`. The value-set list MUST match
    // that narrowing — a DIFFERENT variant's same-period coding must NOT appear as an
    // in-scope row. The period-only outside-period collapse (#744) still works, scoped
    // to the narrowed variant.
    const inA = state({
      state_id: "20",
      variant: "individer",
      value_set_id: "20",
      value_set_version_label: "In-period A",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const inB = state({
      state_id: "21",
      variant: "individer",
      value_set_id: "21",
      value_set_version_label: "In-period B",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    const samePeriodOtherVariant = state({
      state_id: "22",
      variant: "other-population",
      value_set_id: "22",
      value_set_version_label: "Same-period other variant",
      valid_from: "2008-01-01",
      valid_to: "2008-12-31",
    });
    // An individer coding OUTSIDE the period — survives the modifier narrowing and
    // demonstrates the outside-period disclosure still functions for the narrowed
    // variant.
    const outsideIndivider = state({
      state_id: "23",
      variant: "individer",
      value_set_id: "23",
      value_set_version_label: "Outside period",
      valid_from: "1990-01-01",
      valid_to: "1990-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(
      async (_fqid, params) =>
        ({
          // The period-only scope (no variant param) returns the same-period rows of
          // ALL variants; the variant-scoped resolve returns only individer's.
          states: params?.variant
            ? [inA, inB]
            : [inA, inB, samePeriodOtherVariant],
        }) as never,
    );
    router.navigate(
      "/catalog/scb/lisa/kon?period=2007..2008&variant=individer",
    );

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([inA, inB, samePeriodOtherVariant, outsideIndivider]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The narrowed value-set list shows individer's in-period codings…
    await expect.element(page.getByText("In-period A")).toBeVisible();
    await expect.element(page.getByText("In-period B")).toBeVisible();
    // …the period-only outside-period collapse still works (individer's out-of-period
    // coding)…
    await expect
      .element(page.getByText("1 value set outside this period"))
      .toBeVisible();
    // …and the OTHER variant's same-period coding is absent (Fix 3): the list reflects
    // the active narrowing, not the full history.
    expect(
      [...document.querySelectorAll(".vs-label")].some(
        (el) => el.textContent === "Same-period other variant",
      ),
    ).toBe(false);
  });

  it("keeps modifier-resolved single-state detail with a broader period-only scope", async () => {
    const picked = state({
      state_id: "30",
      variant: "individer",
      value_set_id: "30",
      value_set_version_label: "Picked",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const samePeriodOtherVariant = state({
      state_id: "31",
      variant: "other-population",
      value_set_id: "31",
      value_set_version_label: "Same-period other variant",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(
      async (_fqid, params) =>
        ({
          states: params?.variant ? [picked] : [picked, samePeriodOtherVariant],
        }) as never,
    );
    router.navigate("/catalog/scb/lisa/kon?period=2007&variant=individer");

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([picked, samePeriodOtherVariant]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("Value-set version", { exact: true }))
      .toBeVisible();
    expect(document.querySelector(".vs-list")).toBeNull();
  });

  it("keeps Add scoped to the primary resolve when the period-scope fetch fails", async () => {
    const picked = state({
      state_id: "40",
      variant: "individer",
      value_set_id: "40",
      value_set_version_label: "Picked",
      delivery_column_name: "Kon",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const otherVariant = state({
      state_id: "41",
      variant: "other-population",
      value_set_id: "41",
      value_set_version_label: "Other",
      delivery_column_name: "Sni",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
      if (params?.variant) {
        return { states: [picked] } as never;
      }
      throw new Error("scope failed");
    });
    router.navigate("/catalog/scb/lisa/kon?period=2007&variant=individer");

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([picked, otherVariant]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText(/Could not load full period value-set context/))
      .toBeVisible();
    // The picker is independent of the scope-fetch failure: its rows come from
    // `node.states`, and the Add commits the primary resolve's variant.
    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    await expect.element(page.getByText(/Applied \+1 column/)).toBeVisible();
    expect(projectStore.draft?.sources).toEqual([
      expect.objectContaining({
        register_variant: "scb/lisa/individer",
        bindings: [expect.objectContaining({ variable: "scb/lisa/kon" })],
      }),
    ]);
  });

  it("shows FULL history in the value-set list when a `?period` resolve fails with a stale `?variant` (Fix B)", async () => {
    // #905, Codex P2: when the PRIMARY `?period` resolve fails (a stale/typo `?variant`,
    // a 5xx, a network drop), `states` falls back to `node.states` (full history). The
    // modifier narrowing must NOT then apply — a stale `?variant` would narrow that
    // fallback to empty, defeating the full-history fallback. `valueSetStates`/
    // `valueSetScope` are gated on `!narrowedError`, so the value-set list shows the
    // full history (every variant), not a modifier-narrowed (possibly empty) subset.
    const individerCoding = state({
      state_id: "50",
      variant: "individer",
      value_set_id: "50",
      value_set_version_label: "Individer coding",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    const otherCoding = state({
      state_id: "51",
      variant: "other-population",
      value_set_id: "51",
      value_set_version_label: "Other coding",
      valid_from: "2007-01-01",
      valid_to: "2007-12-31",
    });
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
      // The variant-scoped PRIMARY resolve fails (a stale `?variant=typo`); the
      // period-only scope would succeed but is moot once the primary errors.
      if (params?.variant) {
        throw new Error("422 bad variant");
      }
      return { states: [individerCoding, otherCoding] } as never;
    });
    router.navigate("/catalog/scb/lisa/kon?period=2007&variant=typo");

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node([individerCoding, otherCoding]),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The full-history value-set list shows BOTH variants' codings — the stale variant
    // did NOT narrow the error-fallback to empty.
    await expect.element(page.getByText("Individer coding")).toBeVisible();
    await expect.element(page.getByText("Other coding")).toBeVisible();
  });
});

// The leaf half of Y-65: applying a period no longer remounts the catalog subtree
// around the researcher, so the field that applied it keeps focus — and the route's
// own announced loading branch (CatalogNodeView's `aria-live` placeholder) no longer
// runs. The pending resolve has to announce itself, without disturbing that focus.
describe("BindingLeafView — the applied period's pending resolve (Y-65)", () => {
  it("announces the in-flight states load while the field that applied keeps focus", async () => {
    router.navigate("/catalog/scb/lisa/kon?period=2019..2020");
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });
    // By ROLE: what has to hold is the computed live region, not one attribute
    // spelling of it. Filtered because the picker footer is a status line too.
    const loading = page
      .getByRole("status")
      .filter({ hasText: "Loading states…" });
    const from = page.getByRole("textbox", { name: "From" });
    await expect.element(from).toHaveValue("2019");
    // The ARRIVAL's own resolve has to land first, or the in-flight line asserted
    // below could be that one rather than the Apply's.
    await expect.element(loading).not.toBeInTheDocument();

    // Hold the resolve the Apply triggers, so its in-flight moment is observable.
    const { promise: resolving, resolve: finishResolve } =
      Promise.withResolvers<StatesResponse>();
    vi.mocked(getCatalogNode).mockReturnValue(resolving as never);

    await from.fill("2018");
    const field = from.element() as HTMLInputElement;
    field.focus();
    // Enter in a year field is the card's implicit submit — the keyboard Apply.
    await userEvent.keyboard("{Enter}");

    // In flight: the pending period speaks…
    await expect.element(loading).toBeVisible();
    expect(loading.element().getAttribute("aria-busy")).toBe("true");
    // …and the field that applied it still holds focus, and its typed value: the
    // card was never remounted around the researcher.
    expect(document.activeElement).toBe(field);
    expect(field.value).toBe("2018");
    expect(window.location.search).toBe("?period=2018..2020");

    finishResolve(statesResponse(pickerStates.slice(1)));
    await expect.element(loading).not.toBeInTheDocument();
    expect(document.activeElement).toBe(field);
  });
});

it("omits annual availability controls for year-independent delivery and preserves the study window", async () => {
  const studyWindow = { from: 2018, to: 2020 };
  windowStore.set(studyWindow);
  const independent = state({
    period_scope: "year_independent",
    valid_from: null,
    valid_to: null,
    delivery_column_name: "KON",
  });
  const { rerender } = await render(BindingLeafView, {
    ...SEED,
    vintageYear: 2026,
    fqidPath: "scb/lisa/kon",
    node: node([independent]),
  });
  await expect
    .element(
      page.getByText(
        "Year-independent delivery. An annual availability period does not apply.",
      ),
    )
    .toBeVisible();
  await expect
    .element(page.getByRole("button", { name: "Apply period" }))
    .not.toBeInTheDocument();
  expect(windowStore.value).toEqual(studyWindow);
  await rerender({
    ...SEED,
    vintageYear: 2026,
    fqidPath: "scb/lisa/kon",
    node: node([
      independent,
      state({
        state_id: "2",
        delivery_column_name: "YEARLY",
        valid_from: "2018-01-01",
        valid_to: "2020-12-31",
      }),
    ]),
  });
  await expect
    .element(page.getByRole("button", { name: "Apply period" }))
    .toBeVisible();
  expect(windowStore.value).toEqual(studyWindow);
});
