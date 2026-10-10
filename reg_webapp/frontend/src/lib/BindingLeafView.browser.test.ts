import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import {
  getDocsForVariable,
  getGraph,
  getLineage,
  getStates,
  getValues,
  getWarnings,
} from "./api";
import BindingLeafView from "./BindingLeafView.svelte";
import {
  leaf,
  pickerStates,
  SEED,
  state,
} from "./binding-leaf-view-test-helpers";
import { expectStagedAddColumnVisible } from "./picker-test-helpers";
import { projectStore } from "./project_store.svelte";
import { storedProject } from "./project-store-test-helpers";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Two surfaces under test:
//   1. the direct representation picker (#678) — the variable's representations
//      list as selectable rows; selecting rows + Add commits the right
//      `applyStagedDiff` payloads; out-of-window rows dim; empty selection / no
//      seed disables Add.
//   2. the #670 member identity, now derived from the relationship-graph FOCUS node
//      (#678/#904) — the leaf's single `/graph` fetch feeds both the picker graph
//      renderer AND the header qualifier + "member of ⟨group⟩" link.
//
// The leaf + sibling-panel GETs (graph / lineage warnings / parsed docs / the
// ?period resolve) are stubbed so nothing hits a real fetch;
// the panels are independent failure domains, so an empty/rejecting stub never
// blanks the picker under test.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getWarnings: vi.fn().mockResolvedValue([]),
    getStates: vi.fn(),
    getGraph: vi.fn(),
    getLineage: vi.fn(),
    getDocsForVariable: vi.fn(),
    getValues: vi.fn(),
  };
});

const foldedVariantStates = [
  state({
    state_id: "10",
    variant: "individer-16plus",
    variant_label: "Individer, 16 år och äldre",
    variant_family: "individer-15plus",
    variant_family_label: "Individer",
    delivery_column_name: "Kon",
    valid_from: "1990-01-01",
    valid_to: "2009-12-31",
    data_type: "int",
  }),
  state({
    state_id: "11",
    variant: "individer-15plus",
    variant_label: "Individer, 15 år och äldre",
    variant_family: "individer-15plus",
    variant_family_label: "Individer",
    delivery_column_name: "Kon",
    valid_from: "2010-01-01",
    valid_to: "2023-12-31",
    data_type: "int",
  }),
];

/** The fixture catalog's own `scb/lisa/kon`: ONE delivery column, delivered
 * OPEN-ENDED from 2018. With no `?period` and no project window there is nothing
 * finite to clip it to, so the pick resolves no period at all (Y-58). */
const openEndedStates = [
  state({
    state_id: "1",
    variant: "individer-15plus",
    delivery_column_name: "Kon",
    valid_from: "2018-01-01",
    valid_to: "9999-12-31",
    value_set_id: "7",
  }),
];

beforeEach(() => {
  vi.mocked(getStates).mockReset();
  vi.mocked(getStates).mockImplementation(async (_fqid, params) => {
    const variant =
      typeof params?.variant === "string" ? params.variant : undefined;
    return pickerStates.filter(
      (s) => variant === undefined || s.variant === variant,
    );
  });
  // The graph fetch: an EMPTY graph by default (no nodes) → the picker uses the list
  // itself and the header derives no qualifier. Member-identity cases override it.
  vi.mocked(getGraph).mockReset();
  vi.mocked(getGraph).mockResolvedValue({
    nodes: [],
    edges: [],
    focus_id: null,
  } as never);
  vi.mocked(getLineage).mockReset();
  vi.mocked(getLineage).mockResolvedValue({
    edges: [],
    warnings: [],
    registers: [],
  });
  vi.mocked(getValues).mockReset();
  vi.mocked(getValues).mockResolvedValue({
    items: [],
    next_cursor: null,
    total: 0,
  });
  vi.mocked(getDocsForVariable).mockReset();
  vi.mocked(getDocsForVariable).mockResolvedValue({
    items: [],
    next_cursor: null,
    total: 0,
    register_ingested: true,
  });
  // No `?period` — the embedded states drive the plan.
  window.history.pushState({}, "", "/__reset__");
  router.navigate("/catalog/scb/lisa/kon");
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("BindingLeafView representation picker (#678)", () => {
  it("selecting rows + Apply commits the right staged diff", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page.getByRole("checkbox", { name: /Sni/ }).click();
    await expect.element(page.getByText("+2 columns")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+2 columns/)).toBeVisible();
    expect(storedProject()?.sources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          register_variant: "scb/lisa/individer",
          period: { from: 2010, to: 2015 },
          // Exact binding objects, not `objectContaining`: a pick writes the
          // resolved type + representation and no `display_name` (Y-76).
          bindings: [
            { variable: "scb/lisa/kon", type: "opaque", representation: null },
          ],
        }),
        expect.objectContaining({
          register_variant: "scb/lisa/arbetsstallen",
          period: { from: 2018, to: 2020 },
          bindings: [
            { variable: "scb/lisa/kon", type: "opaque", representation: null },
          ],
        }),
      ]),
    );
  });

  it("discards a queued Add when the researcher navigates away meanwhile", async () => {
    // Same wait, the other way out of it: the leaf is gone before the gate
    // settles, so the late continuation must not mutate the draft from a page the
    // researcher has left.
    const { promise: gate, resolve: release } = Promise.withResolvers<void>();
    const restoring = vi.spyOn(projectStore, "restored", "get");
    restoring.mockReturnValue(gate);
    try {
      const view = await render(BindingLeafView, {
        fqidPath: "scb/lisa/kon",
        ...leaf(pickerStates),
        regMetaVersion: SEED.regMetaVersion,
        steward: SEED.steward,
        windowMinYear: SEED.windowMinYear,
        vintageYear: 2024,
      });

      await page.getByRole("checkbox", { name: /Kon/ }).click();
      await page.getByRole("button", { name: "Add to project" }).click();
      await expect
        .element(page.getByRole("button", { name: "Applying..." }))
        .toBeVisible();

      view.unmount();
      release();
      // A fixed wait, not `vi.waitFor`: the assertion is that NOTHING happens, and
      // an unguarded Add needs several microtask hops (its add resolutions)
      // to reach the mutation — a poll would pass on the first tick, before the
      // continuation it is meant to catch has run.
      await new Promise((r) => setTimeout(r, 100));
      expect(storedProject()?.sources).toHaveLength(0);
    } finally {
      restoring.mockRestore();
    }
  });

  it("applying one folded variant-family row adds each concrete era source", async () => {
    vi.mocked(getStates).mockImplementation(async (_fqid, params) => {
      const variant =
        typeof params?.variant === "string" ? params.variant : undefined;
      return foldedVariantStates.filter(
        (s) => variant === undefined || s.variant === variant,
      );
    });

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(foldedVariantStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    const sourcesByVariant = new Map(
      storedProject()?.sources?.map((source) => [
        source.register_variant,
        source,
      ]),
    );
    expect([...sourcesByVariant.keys()].sort()).toEqual([
      "scb/lisa/individer-15plus",
      "scb/lisa/individer-16plus",
    ]);
    // `objectContaining` (not `toMatchObject`) so the bindings compare exactly: a
    // pick writes no `display_name` (Y-76).
    expect(sourcesByVariant.get("scb/lisa/individer-16plus")).toEqual(
      expect.objectContaining({
        period: { from: 1990, to: 2009 },
        bindings: [
          { variable: "scb/lisa/kon", type: "numeric", representation: null },
        ],
      }),
    );
    expect(sourcesByVariant.get("scb/lisa/individer-15plus")).toEqual(
      expect.objectContaining({
        period: { from: 2010, to: 2023 },
        bindings: [
          { variable: "scb/lisa/kon", type: "numeric", representation: null },
        ],
      }),
    );
  });

  it("resolves a staged add against the final merged source period", async () => {
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/individer",
          period: 2000,
          binding: {
            variable: "scb/lisa/other",
            type: "opaque",
          },
        },
      ],
    });
    vi.mocked(getStates).mockResolvedValue([
      state({
        variant: "individer",
        delivery_column_name: "Kon",
        data_type: "int",
      }),
    ]);

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(storedProject()?.sources[0]).toEqual(
      expect.objectContaining({
        period: [2000, { from: 2010, to: 2015 }],
        bindings: expect.arrayContaining([
          expect.objectContaining({ variable: "scb/lisa/other" }),
          expect.objectContaining({
            variable: "scb/lisa/kon",
            type: "numeric",
          }),
        ]),
      }),
    );
  });

  it("clears the previous applied confirmation when a new staged diff starts", async () => {
    vi.mocked(getStates).mockResolvedValue([
      state({
        variant: "individer",
        delivery_column_name: "Kon",
        data_type: "int",
      }),
    ]);
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    const applied = page.getByText(/Applied \+1 column/);
    await expect.element(applied).toBeVisible();

    await page.getByRole("checkbox", { name: /Kon/ }).click();

    await expect.element(page.getByText("-1 column")).toBeVisible();
    await expect.element(applied).not.toBeInTheDocument();
  });

  it("keeps staged keys visible when a draft edit makes async apply stale", async () => {
    let resolveFetch: () => void = () => {
      throw new Error("resolve fetch not started");
    };
    const resolveStarted = new Promise<void>((resolve) => {
      vi.mocked(getStates).mockImplementation(async (_fqid, params) => {
        if (params?.period === "2010..2015") {
          resolve();
          await new Promise<void>((done) => {
            resolveFetch = done;
          });
          return [
            state({
              variant: "individer",
              delivery_column_name: "Kon",
              data_type: "int",
            }),
          ];
        }
        return pickerStates;
      });
    });

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Kon/ }).click();
    await expectStagedAddColumnVisible();
    const apply = page.getByRole("button", {
      name: /Add to project|Remove from project|Apply changes/,
    });
    await apply.click();
    await resolveStarted;

    projectStore.updateField("name", "edited during apply");
    resolveFetch();

    await expectStagedAddColumnVisible();
    await expect.element(page.getByText("+1 column")).toBeVisible();
    expect(storedProject()?.sources).toHaveLength(0);
    expect(projectStore.draft?.name).toBe("edited during apply");
  });

  it("renders committed rows and applies a staged remove only on Apply", async () => {
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/individer",
          period: { from: 2010, to: 2015 },
          binding: {
            variable: "scb/lisa/kon",
            type: "opaque",
            display_name: "Kon",
            representation: "Kon",
          },
        },
      ],
    });
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const kon = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(kon).toBeChecked();
    await expect.element(page.getByText("In project")).toBeVisible();

    await kon.click();
    await expect.element(kon).not.toBeChecked();
    await expect.element(page.getByText("Will be removed")).toBeVisible();
    await expect.element(page.getByText("-1 column")).toBeVisible();
    expect(storedProject()?.sources).toHaveLength(1);

    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();
    await expect.element(page.getByText(/-1 column/)).toBeVisible();
    expect(storedProject()?.sources).toHaveLength(0);
  });

  it("does not clamp staged adds with a structurally invalid ?period", async () => {
    router.navigate("/catalog/scb/lisa/kon?period=2020,2019");

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await page.getByRole("checkbox", { name: /Sni/ }).click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText("+1 column")).toBeVisible();
    expect(storedProject()?.sources[0]?.period).toEqual({
      from: 2018,
      to: 2020,
    });
  });

  // Y-58: ordinary browsing reaches the leaf with NO query string, so nothing has
  // narrowed the open-ended row to a finite period. The Add must refuse instead of
  // authoring `period: ""` plus the `type: ""` the resolve cannot derive — a source
  // the API rejects, autosaved before /project is ever opened.
  it("refuses a pick that resolves no finite period, leaving the draft unchanged", async () => {
    vi.mocked(getStates).mockResolvedValue(openEndedStates);

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(openEndedStates),
      ...SEED,
      vintageYear: 2024,
    });

    const kon = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(kon).toBeVisible();
    await kon.click();
    await page.getByRole("button", { name: "Add to project" }).click();

    // Y-77: the refusal names BOTH ways out — the rail's study window and this
    // page's Apply — not just the one the old copy pointed to.
    await expect
      .element(
        page.getByText(
          "Apply a period before adding — set the study window in the rail, or press Apply under Period above, then select and add again.",
        ),
      )
      .toBeVisible();
    expect(storedProject()?.sources).toHaveLength(0);
    await expect.element(page.getByText(/^Applied/)).not.toBeInTheDocument();

    // Recoverable: resolving the leaf retires the notice, so the researcher is not
    // left staring at a warning the page has already answered.
    router.navigate("/catalog/scb/lisa/kon?period=2018");
    await expect
      .element(page.getByText(/Apply a period before adding/))
      .not.toBeInTheDocument();
  });

  it("retires the refusal when the staging behind it is cleared", async () => {
    // Unchecking the column (like the picker's Reset) empties the staged diff, so
    // the refusal has nothing left to describe — it must go with it rather than sit
    // on the page as a warning about a pick that no longer exists.
    vi.mocked(getStates).mockResolvedValue(openEndedStates);

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(openEndedStates),
      ...SEED,
      vintageYear: 2024,
    });

    const kon = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(kon).toBeVisible();
    await kon.click();
    await page.getByRole("button", { name: "Add to project" }).click();
    await expect
      .element(page.getByText(/Apply a period before adding/))
      .toBeVisible();

    await kon.click();
    await expect
      .element(page.getByText(/Apply a period before adding/))
      .not.toBeInTheDocument();
  });

  it("clamps a stale project window to steward bounds before staged add (#1037)", async () => {
    const longSpan = [
      state({
        state_id: "1",
        variant: "individer",
        delivery_column_name: "Kon",
        valid_from: "1996-01-01",
        valid_to: "2026-12-31",
      }),
    ];
    vi.mocked(getStates).mockResolvedValue(longSpan);
    windowStore.set({ from: 1960, to: 2026 });

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(longSpan),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: 2000,
      windowMaxYear: 2010,
      vintageYear: 2024,
      enforcePeriodBounds: true,
    });

    const kon = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(kon).toBeVisible();
    await kon.click();
    await page
      .getByRole("button", {
        name: /Add to project|Remove from project|Apply changes/,
      })
      .click();

    await expect.element(page.getByText(/\+1 column/)).toBeVisible();
    expect(storedProject()?.sources[0]).toEqual(
      expect.objectContaining({
        period: { from: 2000, to: 2010 },
      }),
    );
  });

  it("uses the steward-bounded period in the States narrowed note (#1037)", async () => {
    const longSpan = [
      state({
        state_id: "1",
        variant: "individer",
        delivery_column_name: "Kon",
        valid_from: "1996-01-01",
        valid_to: "2026-12-31",
      }),
    ];
    vi.mocked(getStates).mockResolvedValue(longSpan);
    router.navigate("/catalog/scb/lisa/kon?period=1960..2026");

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(longSpan),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: 2000,
      windowMaxYear: 2010,
      vintageYear: 2024,
      enforcePeriodBounds: true,
    });

    await expect
      .element(page.getByText(/narrowed to 2000\.\.2010/))
      .toBeVisible();
    expect(document.body.textContent).not.toContain("narrowed to 1960..2026");
  });

  it("Apply stays seed-gated (disabled) even when a row is staged, until the seed is present", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(pickerStates),
      regMetaVersion: "",
      steward: "",
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });
    await expect
      .element(page.getByRole("button", { name: "Add to project" }))
      .toBeDisabled();
    // Selecting a row must NOT enable Apply while the seed is absent.
    await page.getByRole("checkbox", { name: /Kon/ }).click();
    const add = page.getByRole("button", { name: "Add to project" });
    await expect.element(add).toBeDisabled();
  });
});

describe("BindingLeafView data warnings", () => {
  it("renders the variable's own warnings from its warnings read", async () => {
    // Fails if the leaf stops reading the variable's data warnings (they are no
    // longer embedded in the variable's `show`).
    vi.mocked(getWarnings).mockImplementation(async (ref) =>
      ref === "scb/lisa/kon"
        ? [
            {
              warning_id: "b".repeat(64),
              register_fqid: "scb/lisa",
              variable_fqid: "scb/lisa/kon",
              code: "missing_coding_period",
              severity: "warning",
              summary: "Response codes unavailable.",
              detail: "No source dictionary supplied.",
              diagnostic_detail_sha256: "d".repeat(64),
              fields: [],
              refs: [],
              withheld_output: [],
            },
          ]
        : [],
    );

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      ...leaf(pickerStates),
      ...SEED,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByText("Response codes unavailable."))
      .toBeVisible();
  });
});
