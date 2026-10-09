// Split from CatalogNodeView.browser.test.ts by contract surface: adding ticked
// columns (Y-83), lens staging, in-flight abandonment, focus, sticky bar (Y-97).
// Siblings: CatalogNodeView.browser / .register / .add-eras.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import type { RegisterShow } from "./api";
import { getRelatedDocuments, getShow, getStates } from "./api";
import {
  clickVariantChip,
  columnedRegisterNode,
  delivery,
  mockRegisterAndResolve,
  registerShow,
  renderRegister,
  splitColumnRegisterNode,
  tickColumn,
  withLisaVariants,
} from "./catalog-node-view-test-helpers";
import { projectStore } from "./project_store.svelte";
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

// ONE variable delivered under TWO column names by BOTH variants — the shape a pooled
// variant set cross-authors. `Kon` ticked under one lens and `Alder` under the other
// both exist in both variants, so matching a row by name alone would stage each under
// the other's variant as well. The eras differ, so the committed source periods say
// which column went where.
function crossedColumnRegisterNode(): RegisterShow {
  return registerShow({
    children: [
      {
        fqid: "scb/lisa/kon",
        name: "Kön",
        deliveries: [
          delivery("individer-15plus", "Kon", ["2010-01-01", null]),
          delivery("individer-16plus", "Kon", ["2010-01-01", null]),
          delivery("individer-15plus", "Alder", ["2000-01-01", "2015-12-31"]),
          delivery("individer-16plus", "Alder", ["2000-01-01", "2015-12-31"]),
        ],
      },
    ],
  });
}

// A register node with MANY ungrouped leaves — enough rows to overflow the test
// viewport vertically (Y-97), the way LISA's ~740-variable list does for a real
// researcher. Empty `deliveries`: the sticky-bar proof doesn't tick anything,
// only scrolls past it.
function manyLeavesRegisterNode(count: number): RegisterShow {
  return registerShow({
    children: Array.from({ length: count }, (_, i) => ({
      fqid: `scb/lisa/v${i}`,
      name: `Variable ${i}`,
      deliveries: [],
    })),
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

/** Hold the app-owned draft restore open, and return its release. This is the gate
 * `applyStagedPicks` awaits between the press and the commit — and, since the list
 * reads nothing of its own at Add time (Y-104), the place a case lands something
 * (a New, a window drag, a navigation) while an Add is in flight. The same spy the
 * leaf and the concept group hold their Adds at. */
function holdRestore(): () => void {
  const { promise, resolve } = Promise.withResolvers<void>();
  const restoring = vi.spyOn(projectStore, "restored", "get");
  restoring.mockReturnValue(promise);
  return () => {
    restoring.mockRestore();
    resolve();
  };
}

describe("CatalogNodeView register arm: add columns (Y-83)", () => {
  it("adds every ticked column to the draft in one action", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // Two columns of two DIFFERENT variables, ticked from the list itself — the
    // researcher never opens either variable's page.
    await tickColumn("Kon");
    await tickColumn("ForvErs");
    await expect.element(page.getByText("2 columns selected")).toBeVisible();

    await page
      .getByRole("button", { name: "Add 2 columns to project" })
      .click();

    await expect.element(page.getByText("Applied +2 columns")).toBeVisible();
    // ONE source: both adds land on the same register variant, its period the
    // union of the two window-clipped spans (`Kon` 2018–2023, `ForvErs` 2018–2021).
    expect(projectStore.draft?.sources).toEqual([
      expect.objectContaining({
        register_variant: "scb/lisa/individer-15plus",
        period: { from: 2018, to: 2023 },
        bindings: [
          expect.objectContaining({
            variable: "scb/lisa/kon",
            type: "categorical",
          }),
          expect.objectContaining({
            variable: "scb/lisa/forvink-ers",
            type: "categorical",
          }),
        ],
      }),
    ]);
  });

  it("shows an added column as in the project, unticked, and re-ticking it changes nothing", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    // The tick is consumed and the column now reads as part of the project — the
    // state rides inside the tick's own label, so it is in its accessible name.
    await expect
      .element(
        page.getByRole("checkbox", { name: "Kon In project", exact: true }),
      )
      .not.toBeChecked();
    const committed = JSON.stringify(projectStore.draft);

    // Ticking it again is a no-op: the same add folds into the source it is
    // already in (`applyStagedDiff`'s duplicate-binding guard).
    await tickColumn("Kon In project");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(JSON.stringify(projectStore.draft)).toBe(committed);
  });

  it("refuses an open-ended column with no study window, and says which control fixes it", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // `Kon` is delivered open-ended (2018–). With no study window there is no
    // finite period to commit it under, so the batch is refused whole — and the
    // nudge names the rail's window, the only period control this page has.
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect
      .element(page.getByText(/Apply a period before adding/))
      .toBeVisible();
    await expect
      .element(page.getByText(/set the study window in the rail/))
      .toBeVisible();
    expect(projectStore.draft?.sources).toEqual([]);
    // A refusal the researcher's own next move retires, so it is announced
    // politely through `StagedAddStatus`'s status row (Y-106), never an alert.
    const refusal = page.getByText(/Apply a period before adding/).element();
    expect(refusal.closest("[role]")?.getAttribute("role")).toBe("status");
  });

  it("retires the study-window nudge once the window is set, keeping the ticks", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect
      .element(page.getByText(/Apply a period before adding/))
      .toBeVisible();

    // Doing what the nudge asked retires it, so the refusal never outlives the
    // pick it refused — and the tick survives, so "add again" is one press.
    windowStore.set({ from: 2018, to: 2023 });
    await expect
      .element(page.getByText(/Apply a period before adding/))
      .not.toBeInTheDocument();
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
  });

  it("clears a stale confirmation when a later Add is refused (Y-106)", async () => {
    // A SECOND register_variant (`arbetsstallen`), carrying no source yet: a
    // pick on `Kon`'s own `individer-15plus` would inherit that source's period
    // instead of refusing, which would test nothing.
    mockRegisterAndResolve(withLisaVariants(columnedRegisterNode(2)));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();

    // Unsetting the window after a successful Add, then ticking an open-ended
    // column on the OTHER register_variant (`ArbstNr`, delivered 2005–, no
    // existing source) and pressing Add again: that batch has no finite period
    // to commit under and is refused whole. The earlier confirmation must not go
    // on reading as current beside a refusal for a DIFFERENT Add.
    windowStore.set(null);
    await tickColumn("ArbstNr");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect
      .element(page.getByText(/Apply a period before adding/))
      .toBeVisible();
    await expect.element(page.getByText(/Applied/)).not.toBeInTheDocument();
  });

  it("stages what the variant lens shows, one add per delivering variant", async () => {
    mockRegisterAndResolve(withLisaVariants(splitColumnRegisterNode()));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // `Kon` is delivered by BOTH variants under one name: one tick, one add per
    // concrete register variant (#376), so both sources are authored — and the
    // confirmation still counts the ONE column the researcher ticked.
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-15plus", "scb/lisa/individer-16plus"]);
  });

  it("stages only the lensed variant of a column two variants deliver", async () => {
    mockRegisterAndResolve(withLisaVariants(splitColumnRegisterNode()));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // The chip narrows the LIST, and a tick stages exactly what the row now
    // shows: the same `Kon` under ONE variant authors that variant's source
    // alone, where the unlensed tick above authored both.
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-15plus"]);
  });

  it("drops a tick from the batch while the lens shows only a variant it excluded", async () => {
    mockRegisterAndResolve(withLisaVariants(splitColumnRegisterNode()));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon");

    // Moving the lens to the OTHER variant leaves the tick nothing it covers: the
    // `Kon` on screen is now the 16+ delivery, which this tick never stood for. It
    // reads as unticked and there is nothing to add — never a silent switch.
    await clickVariantChip("Individer, 16 år och äldre");
    await clickVariantChip("Individer, 15 år och äldre");
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .not.toBeChecked();
    await expect
      .element(page.getByRole("button", { name: "Add columns to project" }))
      .toBeDisabled();

    // The tick was not thrown away, only held out of the batch: the lens that made
    // it brings it back, still scoped to the one variant it was made under.
    await clickVariantChip("Individer, 16 år och äldre");
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .toBeChecked();
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-15plus"]);
  });

  it("re-ticking a column under a moved lens captures the variant now on screen", async () => {
    mockRegisterAndResolve(withLisaVariants(splitColumnRegisterNode()));
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon");
    await clickVariantChip("Individer, 16 år och äldre");
    await clickVariantChip("Individer, 15 år och äldre");

    // The box reads unticked under this lens, so clicking it must TICK it — and the
    // tick it makes is a tick on what the row shows now, the 16+ delivery.
    await tickColumn("Kon");
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .toBeChecked();
    await page.getByRole("button", { name: "Add 1 column to project" }).click();

    await expect.element(page.getByText("Applied +1 column")).toBeVisible();
    expect(
      projectStore.draft?.sources.map((source) => source.register_variant),
    ).toEqual(["scb/lisa/individer-16plus"]);
  });

  it("stages each column under the variants ITS OWN tick was made under", async () => {
    mockRegisterAndResolve(withLisaVariants(crossedColumnRegisterNode()));
    windowStore.set({ from: 1990, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();

    // Two columns of ONE variable, ticked under DIFFERENT lenses. Both names exist in
    // both variants, so a batch that pooled the two ticks' variants would stage each
    // column under the other's variant too — four adds where the researcher made two.
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Kon 2010–");
    await clickVariantChip("Individer, 16 år och äldre");
    await clickVariantChip("Individer, 15 år och äldre");
    await tickColumn("Alder 2000–2015");
    // Only `Alder` is in the batch here — `Kon`'s tick covers no variant this lens
    // shows — which is what makes the two ticks' variant sets genuinely different.
    await expect.element(page.getByText("1 column selected")).toBeVisible();
    await page.getByRole("button", { name: "Clear variant filter" }).click();

    await expect.element(page.getByText("2 columns selected")).toBeVisible();
    await page
      .getByRole("button", { name: "Add 2 columns to project" })
      .click();
    await expect.element(page.getByText("Applied +2 columns")).toBeVisible();

    // `Kon` under 15+ alone and `Alder` under 16+ alone: one source each, over that
    // column's own era. Pooled, both sources would span BOTH eras instead.
    expect(
      Object.fromEntries(
        (projectStore.draft?.sources ?? []).map((source) => [
          source.register_variant,
          source.period,
        ]),
      ),
    ).toEqual({
      "scb/lisa/individer-15plus": { from: 2010, to: 2023 },
      "scb/lisa/individer-16plus": { from: 2000, to: 2015 },
    });
  });

  it("abandons a batch whose study window moves while the Add is queued", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    const release = holdRestore();
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .toBeVisible();

    // The rail's window is not disabled by an Add, and it IS this page's period:
    // moving it mid-Add makes the pending batch a pick under years the researcher
    // has already left, so it is abandoned rather than committed as 2018–2023.
    windowStore.set({ from: 2019, to: 2023 });
    release();

    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .not.toBeInTheDocument();
    expect(projectStore.draft?.sources).toEqual([]);
    // Abandoned, not refused — and the tick survives, so the Add the new window
    // asks for is one press.
    await expect.element(page.getByText(/Applied/)).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .toBeChecked();
  });

  it("abandons a batch whose project is replaced while the Add is queued", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    const release = holdRestore();
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .toBeVisible();

    // A New replaces the project the pick was staged against — appending to the
    // replacement would corrupt a document the researcher never picked from.
    projectStore.newProject({
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
    });
    release();

    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .not.toBeInTheDocument();
    expect(projectStore.draft?.sources).toEqual([]);
    // Abandoned, not refused: nothing was authored and nothing is claimed either
    // way, and the tick survives for an Add against the project now open.
    await expect.element(page.getByText(/Applied/)).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("checkbox", { name: "Kon", exact: true }))
      .toBeChecked();
  });

  it("abandons a batch whose page changes while the Add is queued (Y-106)", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    const release = holdRestore();
    windowStore.set({ from: 2018, to: 2023 });

    const screen = await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await page.getByRole("button", { name: "Add 1 column to project" }).click();
    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .toBeVisible();

    // This component is REUSED register → register (the route-reset `$effect`),
    // so `unmounted` alone would miss a page change mid-Add: the batch must bind to
    // the route too, or the queued commit would land on the register the researcher
    // has since opened.
    await screen.rerender({ fqidPath: "scb/rams" });
    release();

    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .not.toBeInTheDocument();
    expect(projectStore.draft?.sources).toEqual([]);
    // Nothing is authored and nothing is reported — not a refusal either — on the
    // page the researcher has since opened. `StagedAddStatus` says both through a
    // `status` row, so its absence is the whole claim.
    await expect.element(page.getByText(/Applied/)).not.toBeInTheDocument();
    expect(document.querySelector(".page-add")).toBeNull();
  });

  it("keeps keyboard focus on the Add button through an add (Y-106)", async () => {
    mockRegisterAndResolve(columnedRegisterNode(1));
    const release = holdRestore();
    windowStore.set({ from: 2018, to: 2023 });

    await renderRegister();
    await expect.element(page.getByText("Kön")).toBeVisible();
    await tickColumn("Kon");
    await tickColumn("ForvErs");

    const addButton = page.getByRole("button", {
      name: "Add 2 columns to project",
    });
    const addButtonEl = addButton.element();
    await addButton.click();
    await expect
      .element(page.getByRole("button", { name: "Adding…" }))
      .toBeVisible();

    // `aria-disabled` freezes the ACTION while the add is in flight; the element
    // itself stays in the tab order, so the press that started it keeps focus
    // instead of dropping to <body> — a natively `disabled` button's fate.
    expect(document.activeElement).toBe(addButtonEl);

    release();
    await expect.element(page.getByText("Applied +2 columns")).toBeVisible();
    expect(document.activeElement).toBe(addButtonEl);
  });
});

describe("CatalogNodeView register arm: sticky add bar (Y-97)", () => {
  it("keeps the add bar pinned to the viewport bottom after scrolling through a long list", async () => {
    await page.viewport(1280, 800);
    vi.mocked(getShow).mockResolvedValue(manyLeavesRegisterNode(60));

    await renderRegister();
    await expect.element(page.getByText("Variable 0")).toBeVisible();

    // Scroll well past the bar's unscrolled position under the table — the
    // "two thirds down a long list" the ticket's researcher is in.
    window.scrollTo(0, document.documentElement.scrollHeight / 2);

    const bar = document.querySelector<HTMLElement>(".add-bar");
    expect(bar).not.toBeNull();
    const rect = bar?.getBoundingClientRect();
    const viewportBottom = window.innerHeight;
    // Pinned flush to the viewport's bottom edge (within 1px)…
    const bottomGap = Math.abs((rect?.bottom ?? 0) - viewportBottom);
    expect(bottomGap).toBeLessThanOrEqual(1);
    // …and its top is on screen, not clipped above the fold.
    expect(rect?.top ?? -1).toBeGreaterThanOrEqual(0);
    expect(rect?.top ?? viewportBottom).toBeLessThan(viewportBottom);

    // The page itself never grows a horizontal scrollbar — DataTable's own
    // wrapper owns the (unused, here narrow) horizontal overflow, not `.routed`.
    expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(
      document.documentElement.clientWidth + 1,
    );
  });
});
