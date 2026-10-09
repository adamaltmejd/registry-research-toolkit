import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
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
  single,
  state,
  statesResponse,
} from "./binding-leaf-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { router } from "./router.svelte";
import { windowStore } from "./window.svelte";

// Split from BindingLeafView.browser.test.ts by contract surface: picker row rendering and ?codes deep links.
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
  it("dims rows whose span does not overlap the active period window", async () => {
    // Narrow to 2018..2020 — the Sni row (2018–2020) overlaps, the Kon row
    // (2010–2015) does not, so Kon's row is dimmed (but still selectable).
    vi.mocked(getCatalogNode).mockResolvedValue({
      states: pickerStates,
    } as never);
    router.navigate("/catalog/scb/lisa/kon?period=2018..2020");

    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(pickerStates),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    const konRow = page.getByRole("checkbox", { name: /Kon/ });
    await expect.element(konRow).toBeVisible();
    // The `dimmed` class is on the row container (.row-btn label), not the checkbox.
    await vi.waitFor(() => {
      const rowBtn = konRow.element().closest(".row-btn");
      if (!rowBtn?.classList.contains("dimmed")) {
        throw new Error("Kon row not yet dimmed");
      }
    });
    // The in-window Sni row is NOT dimmed.
    const sniRow = page
      .getByRole("checkbox", { name: /Sni/ })
      .element()
      .closest(".row-btn");
    expect(sniRow?.classList.contains("dimmed")).toBe(false);
    // A dimmed row stays selectable.
    await konRow.click();
    await expect.element(konRow).toBeChecked();
  });

  it("hoists a constant column to the band context and shows the varying population per row (fordonsreg shape)", async () => {
    // Every representation delivers the CONSTANT column "Sni2002" over the SAME
    // span; only the population (lastbilar/bussar) varies — so the column is
    // hoisted once and the rows show the population.
    const fordonsreg = [
      state({
        state_id: "1",
        variant: "lastbilar",
        delivery_column_name: "Sni2002",
        value_set_version_label: "SNI 2002",
        valid_from: "2003-01-01",
        valid_to: "2015-12-31",
      }),
      state({
        state_id: "2",
        variant: "bussar",
        delivery_column_name: "Sni2002",
        value_set_version_label: "SNI 2002",
        valid_from: "2003-01-01",
        valid_to: "2015-12-31",
      }),
    ];
    await render(BindingLeafView, {
      fqidPath: "scb/fordonsreg/naringsgren",
      node: node(fordonsreg, { fqid: "scb/fordonsreg/naringsgren" }),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The constant column is hoisted once as the subheading (its select-all), and
    // each row is named by its varying POPULATION and period only: neither the
    // column nor the constant value-set label is repeated per row.
    await expect
      .element(
        page.getByRole("checkbox", { name: "Select all columns of Sni2002" }),
      )
      .toBeVisible();
    for (const population of ["lastbilar", "bussar"]) {
      await expect
        .element(
          page.getByRole("checkbox", {
            name: `${population} 2003 – 2015`,
            exact: true,
          }),
        )
        .toBeVisible();
    }
    // The two populations discriminate, so the leaf surfaces the #908 Variant filter.
    await expect
      .element(page.getByRole("group", { name: "Variant" }))
      .toBeVisible();
  });

  it("leads each column-varies row with its delivery column, then its value-set qualifier (#678)", async () => {
    // Two CO-EXISTING (overlapping) columns on one variable (column VARIES) → each
    // nested row leads with its column rendered as a .col-chip (the selection signal);
    // the value-set qualifier (which varies too) stays plain text, NOT a chip. The
    // windows OVERLAP (the SSYK 3-digit / 5-digit parallel-coding case): a non-
    // overlapping pair would collapse to one rename row (#902).
    const colVaries = [
      state({
        state_id: "1",
        variant: "individer",
        delivery_column_name: "Ssyk3",
        value_set_version_label: "SSYK 3-siffrig",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        state_id: "2",
        variant: "individer",
        delivery_column_name: "Ssyk5",
        value_set_version_label: "SSYK 5-siffrig",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
    ];
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/yrke",
      node: node(colVaries, { fqid: "scb/lisa/yrke", name: "Yrke" }),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // Each row is named by its column first, then the varying value-set qualifier.
    for (const name of [
      "Ssyk3 SSYK 3-siffrig 2010 – 2020",
      "Ssyk5 SSYK 5-siffrig 2010 – 2020",
    ]) {
      await expect
        .element(page.getByRole("checkbox", { name, exact: true }))
        .toBeVisible();
    }
  });

  it("hoists a common value-set STEM to the context and shows per-row suffixes (sni92 shape, #678)", async () => {
    // sni92: the long "Svensk standard för näringsgrensindelning," stem repeats; the
    // picker hoists it once to the subhead context and each row shows only its suffix.
    // Two CO-EXISTING (overlapping-window) columns so they stay separate nested rows
    // (a non-overlapping pair would collapse to one rename row, #902).
    const sni92 = [
      state({
        state_id: "1",
        variant: "individer",
        delivery_column_name: "Sni92A",
        value_set_version_label:
          "Svensk standard för näringsgrensindelning, Aktiviteter",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        state_id: "2",
        variant: "individer",
        delivery_column_name: "Sni92B",
        value_set_version_label:
          "Svensk standard för näringsgrensindelning, Branscher",
        valid_from: "2010-01-01",
        valid_to: "2020-12-31",
      }),
    ];
    await render(BindingLeafView, {
      fqidPath: "scb/fordonsreg/sni92",
      node: node(sni92, { fqid: "scb/fordonsreg/sni92", name: "Näringsgren" }),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // The shared stem shows once beside the subheading; each row is named by its
    // column and only its suffix.
    await expect
      .element(
        page.getByText("Svensk standard för näringsgrensindelning,", {
          exact: true,
        }),
      )
      .toBeVisible();
    for (const name of [
      "Sni92A Aktiviteter 2010 – 2020",
      "Sni92B Branscher 2010 – 2020",
    ]) {
      await expect
        .element(page.getByRole("checkbox", { name, exact: true }))
        .toBeVisible();
    }
  });

  it("renders a 'codings vary' nudge only on a column whose value_set_id changed over time (#678)", async () => {
    // ColA carried value-set 303 then 249 (a coding change → the nudge). ColB keeps
    // id 249 under two drifting LABELS (the SUN case): one coding, no nudge. ColC
    // gains a coding (null → 42), which counts as a change. Keyed on the reliable
    // value_set_id, NOT the label.
    const states = [
      state({
        state_id: "1",
        variant: "v",
        delivery_column_name: "ColA",
        value_set_id: "303",
        value_set_version_label: "MiS 1996:1",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
      state({
        state_id: "2",
        variant: "v",
        delivery_column_name: "ColA",
        value_set_id: "249",
        value_set_version_label: "SUN",
        valid_from: "2020-01-01",
        valid_to: "2022-12-31",
      }),
      state({
        state_id: "3",
        variant: "v",
        delivery_column_name: "ColB",
        value_set_id: "249",
        value_set_version_label: "SUN 2020 NivaOld",
        valid_from: "2019-01-01",
        valid_to: "2020-12-31",
      }),
      state({
        state_id: "4",
        variant: "v",
        delivery_column_name: "ColB",
        value_set_id: "249",
        value_set_version_label: "SUN 2000 NivaOld",
        valid_from: "2021-01-01",
        valid_to: "2022-12-31",
      }),
      state({
        state_id: "5",
        variant: "v",
        delivery_column_name: "ColC",
        value_set_id: null,
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
      state({
        state_id: "6",
        variant: "v",
        delivery_column_name: "ColC",
        value_set_id: "42",
        valid_from: "2020-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(states),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // #905: each nudge is a deep link to the value-set viewer focused on its ROW —
    // the current leaf path + `?codes=<variant>::<column>#states-heading`.
    const nudges = page.getByRole("link", {
      name: "Coding changes over time — see the value sets",
    });
    await expect.element(nudges.first()).toBeVisible();
    expect(nudges.elements().map((a) => a.getAttribute("href"))).toEqual([
      "/catalog/scb/lisa/kon?codes=v%3A%3AColA#states-heading",
      "/catalog/scb/lisa/kon?codes=v%3A%3AColC#states-heading",
    ]);
  });

  it("a ?codes=<column> deep link focuses the value-set viewer on that column's latest coding (#905)", async () => {
    // The other side of the deep link: with `?codes=ColA` in the URL the leaf passes
    // it as `focusColumn`, and the value-set viewer auto-isolates ColA's latest-era
    // coding (value-set 249 / "SUN", not the earlier 303 / "MiS 1996:1").
    const states = [
      state({
        state_id: "1",
        variant: "v",
        delivery_column_name: "ColA",
        value_set_id: "303",
        value_set_version_label: "MiS 1996:1",
        valid_from: "2019-01-01",
        valid_to: "2019-12-31",
      }),
      state({
        state_id: "2",
        variant: "v",
        delivery_column_name: "ColA",
        value_set_id: "249",
        value_set_version_label: "SUN",
        valid_from: "2020-01-01",
        valid_to: "2022-12-31",
      }),
      state({
        state_id: "3",
        variant: "v",
        delivery_column_name: "ColB",
        value_set_id: "100",
        value_set_version_label: "Stable",
        valid_from: "2019-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    router.navigate("/catalog/scb/lisa/kon?codes=ColA");
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(states),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });
    // The viewer is isolated on ColA's latest coding: the "Used by" detail + the
    // "SUN" heading show, and the union list is hidden.
    await expect.element(page.getByText("Used by")).toBeVisible();
    expect(
      document.querySelector(".vs-detail .vs-heading")?.textContent,
    ).toContain("SUN");
    expect(document.querySelector(".vs-list > li")).toBeNull();
  });

  it("a ?codes=<variant>::<column> deep link isolates the CLICKED variant's coding when a column is shared across variants (#905)", async () => {
    // One delivery column COL delivered by TWO variants with DISTINCT codings:
    // variant "a" → "Coding A" (older), variant "b" → "Coding B" (latest era). The
    // unscoped column lookup would pick B (the latest), but the deep link carries the
    // ROW's variant, so `?codes=a::COL` must isolate A's coding — the deep-link bug.
    const states = [
      state({
        state_id: "1",
        variant: "a",
        delivery_column_name: "COL",
        value_set_id: "100",
        value_set_version_label: "Coding A",
        valid_from: "2015-01-01",
        valid_to: "2018-12-31",
      }),
      state({
        state_id: "2",
        variant: "b",
        delivery_column_name: "COL",
        value_set_id: "200",
        value_set_version_label: "Coding B",
        valid_from: "2019-01-01",
        valid_to: "2022-12-31",
      }),
    ];
    router.navigate("/catalog/scb/lisa/kon?codes=a%3A%3ACOL");
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(states),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });
    // Isolated on variant "a"'s coding ("Coding A"), NOT "b"'s latest-era "Coding B".
    await expect.element(page.getByText("Used by")).toBeVisible();
    const heading = document.querySelector(
      ".vs-detail .vs-heading",
    )?.textContent;
    expect(heading).toContain("Coding A");
    expect(heading).not.toContain("Coding B");
  });

  it("the single-COLUMN leaf renders ONE compact row led by its COLUMN (#678)", async () => {
    // A single-column leaf has nothing varying, so it merges to ONE compact row. The
    // variable name is already the page <h2>, so the row leads with just its COLUMN
    // chip ("Kon"), NOT a repeated "Kön". The constant register prefix is hoisted off.
    const oneColumn = [
      state({
        state_id: "1",
        variant: "individer",
        delivery_column_name: "Kon",
        valid_from: "2010-01-01",
        valid_to: "2015-12-31",
        value_set_version_label: "1-siffrig",
      }),
    ];
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(oneColumn),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    // ONE row named by its column ("Kon") and period: no repeated variable name
    // ("Kön") and no subheading select-all.
    await expect
      .element(
        page.getByRole("checkbox", { name: "Kon 2010 – 2015", exact: true }),
      )
      .toBeVisible();
    expect(page.getByRole("checkbox").elements()).toHaveLength(1);
    // The LEAF passes no `href` → the column is not a navigation link (the leaf is
    // already its own page; nav is a group-view affordance only).
    expect(page.getByRole("link", { name: /Kon/ }).elements()).toEqual([]);
  });

  it("renders no picker when no state carries a delivery column", async () => {
    await render(BindingLeafView, {
      fqidPath: "scb/lisa/kon",
      node: node(single),
      regMetaVersion: SEED.regMetaVersion,
      steward: SEED.steward,
      windowMinYear: SEED.windowMinYear,
      vintageYear: 2024,
    });

    await expect
      .element(page.getByRole("heading", { name: "Kön", level: 2 }))
      .toBeVisible();
    expect(page.getByRole("checkbox").elements()).toEqual([]);
    expect(
      page.getByRole("button", { name: /Add to project/ }).elements(),
    ).toEqual([]);
  });
});
