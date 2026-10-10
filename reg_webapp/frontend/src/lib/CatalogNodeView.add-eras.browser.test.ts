// Split from CatalogNodeView.browser.test.ts by contract surface: adding columns
// across eras, gaps and renames (Y-104/Y-110). Siblings: CatalogNodeView.browser
// / .register / .add-columns.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import type { RegisterShow } from "./api";
import { getRelatedDocuments, getShow, getStates } from "./api";
import {
  columnedRegisterNode,
  delivery,
  mockRegisterAndResolve,
  registerShow,
  renderRegister,
  tickColumn,
  withLisaVariants,
} from "./catalog-node-view-test-helpers";
import { projectStore } from "./project_store.svelte";
import { storedProject } from "./project-store-test-helpers";
import { windowStore } from "./window.svelte";

// CatalogNodeView reads one node via `getShow(fqidPath)` and switches on
// `kind`. Mock that GET (mirrors ConceptGroupView's api-mock style); keep
// the rest of api.ts real (the type exports + path helpers `catalog.ts` uses).
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getWarnings: vi.fn().mockResolvedValue([]),
    getShow: vi.fn(),
    getStates: vi.fn(),
    getRelatedDocuments: vi.fn(),
  };
});

// A #319 monthly family with a month missing: ONE column delivered in two
// DISJOINT SUB-YEAR eras inside a single year. The cell prints years, so both eras
// read "2018" — a label the list must say ONCE, and can never key itself by.
function subYearErasRegisterNode(): RegisterShow {
  return registerShow({
    fqid: "scb/lonestrukturstatistik",
    name: "Lönestrukturstatistik",
    children: [
      {
        fqid: "scb/lonestrukturstatistik/lon",
        name: "Lön",
        deliveries: [
          delivery(
            "individer",
            "LonFink",
            ["2018-01-01", "2018-01-31"],
            ["2018-03-01", "2018-03-31"],
          ),
        ],
      },
    ],
  });
}

// The shape a name-grain gate alone gets wrong (Y-104). `individer-15plus`
// renamed `CDISP` to `CDISP5` in 2010 — #902 folds its two deliveries into ONE
// picker row spanning both — while `individer-16plus` still delivers `CDISP`. Pool
// the NAME's eras across the two and `CDISP` reads as delivered to this day, which
// is true of 16plus and false of 15plus, whose row reaches a late window only
// through the successor.
function renamedByOneVariantRegisterNode(): RegisterShow {
  return registerShow({
    children: [
      {
        fqid: "scb/lisa/disp",
        name: "Disponibel inkomst",
        deliveries: [
          delivery("individer-15plus", "CDISP", ["2000-01-01", "2009-12-31"]),
          delivery("individer-15plus", "CDISP5", ["2010-01-01", null]),
          delivery("individer-16plus", "CDISP", ["2000-01-01", null]),
        ],
      },
    ],
  });
}

// The Y-104 shape: ONE column, delivered in DISJOINT eras. `Lan` on
// civilståndsändringar was delivered in 1968, again in 1995–1996, and continuously
// from 1998 — a history the MIN/MAX span "1968–" cannot tell from an unbroken one.
// The wire carries the eras themselves now, so the list can show and match them.
function interruptedColumnRegisterNode(): RegisterShow {
  return registerShow({
    fqid: "scb/civilstandsandringar",
    name: "Civilståndsändringar",
    children: [
      {
        fqid: "scb/civilstandsandringar/lan",
        name: "Län",
        deliveries: [
          delivery(
            "individer",
            "Lan",
            ["1968-01-01", "1968-12-31"],
            ["1995-01-01", "1996-12-31"],
            ["1998-01-01", null],
          ),
        ],
      },
    ],
  });
}

// The Y-110 tail: FOUR disjoint eras, one more than `ERA_LABEL_LIMIT`. `Lan` is
// delivered in 1968, again in 1972, again in 1995–1996, and continuously from
// 1998 — enough eras that printing all four would push the row's NAME onto its
// own line (the defect Y-110 fixes).
function fourEraColumnRegisterNode(): RegisterShow {
  return registerShow({
    fqid: "scb/civilstandsandringar",
    name: "Civilståndsändringar",
    children: [
      {
        fqid: "scb/civilstandsandringar/lan",
        name: "Län",
        deliveries: [
          delivery(
            "individer",
            "Lan",
            ["1968-01-01", "1968-12-31"],
            ["1972-01-01", "1972-12-31"],
            ["1995-01-01", "1996-12-31"],
            ["1998-01-01", null],
          ),
        ],
      },
    ],
  });
}

/** Two columns the years cannot fully date. `Fodelsear` was delivered from before
 * the record starts until 1968 and again from 1995 (the `0001-01-01` start
 * sentinel), and `LanAlias` is delivered in NO era at all — the boundless delivery
 * `VariableDelivery` documents: an alias spelling on a variant with no states of
 * its own, carrying an empty `windows` and a null coverage. */
function undatedColumnRegisterNode(): RegisterShow {
  return registerShow({
    fqid: "scb/civilstandsandringar",
    name: "Civilståndsändringar",
    children: [
      {
        fqid: "scb/civilstandsandringar/fodelsear",
        name: "Födelseår",
        deliveries: [
          delivery(
            "individer",
            "Fodelsear",
            ["0001-01-01", "1968-12-31"],
            ["1995-01-01", null],
          ),
        ],
      },
      {
        fqid: "scb/civilstandsandringar/lan",
        name: "Län",
        deliveries: [delivery("individer", "LanAlias")],
      },
    ],
  });
}

beforeEach(() => {
  vi.mocked(getShow).mockReset();
  vi.mocked(getStates).mockReset();
  vi.mocked(getRelatedDocuments).mockReset();
  vi.mocked(getRelatedDocuments).mockResolvedValue([]);
  // Both stores are module singletons: clear the browse-time window fallback, then
  // open a fresh empty draft — the state a catalog page authors into. A fresh draft
  // seeds its window from that (now empty) fallback, so each case starts windowless.
  windowStore.set(null);
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("CatalogNodeView register arm: add columns (Y-83)", () => {
  it("folds a long era list to its first, last and a +N affordance (Y-110)", async () => {
    mockRegisterAndResolve(fourEraColumnRegisterNode());
    windowStore.set({ from: 1960, to: 2024 });

    await renderRegister("scb/civilstandsandringar");
    await expect.element(page.getByText("Län")).toBeVisible();

    // One era past ERA_LABEL_LIMIT: the label prints only the first and last
    // era — the boundaries of the whole history — plus a +N affordance carrying
    // the folded eras (1972 and 1995–1996) in its hover title…
    await expect
      .element(page.getByText("+2", { exact: true }))
      .toHaveAttribute("title", "1968, 1972, 1995–1996, 1998–");

    // …and in the tick's own accessible name, which a screen-reader user hears.
    await expect
      .element(
        page.getByRole("checkbox", {
          name: /Lan 1968 … 1998–.*1972, 1995–1996/,
        }),
      )
      .toBeVisible();
  });

  it("commits an interrupted column as its real eras, never across the gap (Y-104)", async () => {
    mockRegisterAndResolve(interruptedColumnRegisterNode());
    windowStore.set({ from: 1995, to: 2015 });

    await renderRegister("scb/civilstandsandringar");
    await expect.element(page.getByText("Län")).toBeVisible();
    await tickColumn("Lan 1968, 1995–1996, 1998–");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    // The 1997 gap is carved out as the #307 list form — what the variable's own
    // page commits, and NOT the single 1995–2015 span the span would give.
    expect(storedProject()?.sources[0]?.period).toEqual([
      { from: 1995, to: 1996 },
      { from: 1998, to: 2015 },
    ]);
    // …and the row reads as in the project off those same eras: the marker matches
    // the committed source era by era, with no second read of the states. The 1997
    // hole is a divergence from the study window, and the marker says so.
    await expect
      .element(
        page.getByRole("checkbox", {
          name: "Lan 1968, 1995–1996, 1998– In project, years differ (source period 1995–1996, 1998–2015; study window 1995–2015)",
          exact: true,
        }),
      )
      .not.toBeChecked();
  });

  it("offers no tick for a column whose gap swallows the study window (Y-104)", async () => {
    mockRegisterAndResolve(interruptedColumnRegisterNode());
    // 1970–1990 falls between `Lan`'s 1968 era and its 1995 one. The span "1968–"
    // covers it; the eras do not, and the eras are what the list matches on.
    windowStore.set({ from: 1970, to: 1990 });

    await renderRegister("scb/civilstandsandringar");
    await expect.element(page.getByText("Län")).toBeVisible();

    const lan = page.getByRole("checkbox", {
      name: "Lan 1968, 1995–1996, 1998– Not delivered in 1970–1990",
      exact: true,
    });
    await expect.element(lan).toBeDisabled();
    await expect.element(lan).not.toBeChecked();
    expect(storedProject()?.sources).toEqual([]);
  });

  it("commits a sequentially renamed column as the ONE row its variable's page commits", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    windowStore.set({ from: 2015, to: 2022 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // `disp` is delivered as CDISP through 2019 and as CDISP5 from 2020 — a
    // sequential RENAME, which the list still lists under both names (Y-82: the name
    // is what a researcher hunts for) where the variable's own page shows the ONE
    // folded row (#902).
    await tickColumn("CDISP 1968–2019");
    await tickColumn("CDISP5 2020–");
    await expect.element(page.getByText("2 columns selected")).toBeVisible();

    await page
      .getByRole("button", { name: "Add 2 columns to project" })
      .click();

    // Both ticked columns are confirmed — that is what the researcher ticked — and
    // they commit as ONE binding over the window-clipped union, exactly as the leaf
    // page commits its folded row. Never one pinned binding per column name: pinning
    // either would break the other's era.
    await expect.element(page.getByText("Applied +2 columns")).toBeVisible();
    expect(storedProject()?.sources).toEqual([
      expect.objectContaining({
        register_variant: "scb/lisa/individer-15plus",
        period: { from: 2015, to: 2022 },
        bindings: [
          expect.objectContaining({
            variable: "scb/lisa/disp",
            representation: null,
          }),
        ],
      }),
    ]);
  });

  it("commits the whole rename when only one of its names is ticked", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    windowStore.set({ from: 2015, to: 2022 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // One tick of the RETIRED name is still a pick of the variable's one folded row:
    // it commits the same 2015–2022 the variable's own page commits, where per-period
    // resolution reads CDISP through 2019 and CDISP5 after it. A period stopping at
    // 2019 would be a row this variable has not had since #902 folded it.
    await tickColumn("CDISP 1968–2019");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(storedProject()?.sources[0]?.period).toEqual({
      from: 2015,
      to: 2022,
    });
    // The list's other name for it reads as in the project too — one representation,
    // both of its names.
    await expect
      .element(
        page.getByRole("checkbox", {
          name: "CDISP5 2020– In project",
          exact: true,
        }),
      )
      .not.toBeChecked();
  });

  it("refuses a column the study window has moved off, on its own row", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    // The list shows every column the register ever delivered. `ForvErs` ended in
    // 2021, so under a 2022–2024 window it has no era to commit and no period to
    // commit it under — the study window is this page's only period control.
    windowStore.set({ from: 2022, to: 2024 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // The reason rides inside the tick's own label, so it is in its accessible
    // name — and the tick itself is not offered.
    const forvErs = page.getByRole("checkbox", {
      name: "ForvErs Not delivered in 2022–2024",
      exact: true,
    });
    await expect.element(forvErs).toBeDisabled();
    await expect.element(forvErs).not.toBeChecked();
    // Nothing is staged, so nothing can be reported as applied and no project is
    // minted for an empty diff.
    await expect
      .element(page.getByRole("button", { name: "Add columns to project" }))
      .toBeDisabled();
    expect(storedProject()?.sources).toEqual([]);
  });

  it("dates an era that began before the record does (Y-104)", async () => {
    mockRegisterAndResolve(undatedColumnRegisterNode());
    windowStore.set({ from: 1960, to: 2024 });

    await renderRegister("scb/civilstandsandringar");
    await expect.element(page.getByText("Födelseår")).toBeVisible();

    // The first era's start is the `0001-01-01` sentinel — unknown, not absent.
    // Dropped from a LIST it would read as a gap before 1995, beside a tick that
    // commits those years all the same; the undated side takes the same bare dash
    // the open-ended side does.
    await expect
      .element(
        page.getByRole("checkbox", {
          name: "Fodelsear –1968, 1995–",
          exact: true,
        }),
      )
      .toBeVisible();
  });

  it("says one year once for two sub-year eras inside it (Y-104)", async () => {
    mockRegisterAndResolve(subYearErasRegisterNode());
    windowStore.set({ from: 2018, to: 2018 });

    await renderRegister("scb/lonestrukturstatistik");
    // By role: the register's own heading contains the variable's name.
    await expect.element(page.getByRole("link", { name: "Lön" })).toBeVisible();

    // Two eras, one label. The cell is year-grain, so a second "2018" says nothing
    // a reader can use — and a list keyed by its rendered label would collide on
    // the two outright, taking the whole row down.
    await expect
      .element(
        page.getByRole("checkbox", { name: "LonFink 2018", exact: true }),
      )
      .toBeVisible();
  });

  it("offers no tick for a column delivered in no era at all (Y-104)", async () => {
    mockRegisterAndResolve(undatedColumnRegisterNode());
    // No study window, where every other column IS tickable: this one has no era
    // to commit under any window, so the tick is refused on its own terms and the
    // reason names no window — none would lift it. A checkbox that ticks and
    // stages nothing would be a control that lies.

    await renderRegister("scb/civilstandsandringar");
    await expect.element(page.getByText("Län")).toBeVisible();

    const alias = page.getByRole("checkbox", {
      name: "LanAlias Not delivered",
      exact: true,
    });
    await expect.element(alias).toBeDisabled();
    await expect.element(alias).not.toBeChecked();
  });

  it("marks only the rename's names the committed years reach (Y-104)", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    // 2015–2019 is inside `CDISP`'s era and wholly before `CDISP5`'s, so the ONE
    // folded row they share commits years only `CDISP` was delivered in.
    windowStore.set({ from: 2015, to: 2019 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("CDISP 1968–2019");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();

    // The ticked name reads as in the project…
    await expect
      .element(
        page.getByRole("checkbox", {
          name: "CDISP 1968–2019 In project",
          exact: true,
        }),
      )
      .toBeVisible();
    // …and its successor does NOT: the row is committed, but none of the years
    // `CDISP5` was delivered in are. Marked, it would send a researcher away from
    // a column they still have to add — beside a tick refusing it in the same
    // breath.
    await expect
      .element(
        page.getByRole("checkbox", {
          name: "CDISP5 2020– Not delivered in 2015–2019",
          exact: true,
        }),
      )
      .toBeVisible();
  });

  it("offers no tick for a RETIRED name its successor still delivers (Y-104)", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    // `disp` is delivered as CDISP through 2019 and as CDISP5 from 2020, and #902
    // folds the two into ONE picker row spanning both — so the row reaches a
    // 2022–2024 window and the retired NAME does not. The gate is the name's own
    // eras, the same ones its label prints beside it: a tick offered off the folded
    // row would invite a pick of a column the register stopped delivering three
    // years before the window opens, under a label that says so.
    windowStore.set({ from: 2022, to: 2024 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    const retired = page.getByRole("checkbox", {
      name: "CDISP 1968–2019 Not delivered in 2022–2024",
      exact: true,
    });
    await expect.element(retired).toBeDisabled();
    // The name that IS delivered then stays tickable — one row, two names, one
    // offered.
    await expect
      .element(
        page.getByRole("checkbox", { name: "CDISP5 2020–", exact: true }),
      )
      .not.toBeDisabled();
  });

  it("stages a retired name only under the variant still delivering it (Y-104)", async () => {
    mockRegisterAndResolve(withLisaVariants(renamedByOneVariantRegisterNode()));
    // Inside 16plus's `CDISP`, and years after 15plus renamed it.
    windowStore.set({ from: 2015, to: 2020 });

    await renderRegister();
    await expect.element(page.getByText("Disponibel inkomst")).toBeVisible();

    // The tick is offered — 16plus really does deliver `CDISP` in 2015–2020, and the
    // label pools both variants to say so — but it stages that variant ALONE. 15plus's
    // folded row reaches the window only through `CDISP5`, the name this tick is not.
    await tickColumn("CDISP 2000–");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      storedProject()?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-16plus"]);

    // Commit 15plus's row too, under the successor's name — the years it really was
    // delivered under `CDISP5`.
    await tickColumn("CDISP5 2010–");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      storedProject()?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-16plus", "scb/lisa/individer-15plus"]);

    // `CDISP5` reads as in the project. `CDISP` does NOT, though BOTH its rows are
    // now committed: 15plus's was committed for years it delivered `CDISP5` in, and
    // pooling 16plus's still-current eras over that row would claim a name 15plus
    // never added — the marker asks each row's own variant.
    await expect
      .element(
        page.getByRole("checkbox", {
          name: "CDISP5 2010– In project",
          exact: true,
        }),
      )
      .toBeVisible();
    await expect
      .element(page.getByRole("checkbox", { name: "CDISP 2000–", exact: true }))
      .toBeVisible();
  });
});
