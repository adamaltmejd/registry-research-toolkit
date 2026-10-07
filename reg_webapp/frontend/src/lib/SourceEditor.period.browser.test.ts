import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import type { StatesResponse } from "./api";
import { getCatalogNode, getCatalogRoot, getRegisterVariants } from "./api";
import { resetCatalogNames } from "./catalog_names.svelte";
import type { Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import {
  providerNode,
  registerNode,
  renderCard,
  seedSource,
  stubCatalog,
} from "./source-editor-test-helpers";

// Split from SourceEditor.browser.test.ts by contract surface: the source period
// (Y-81) and its list segments (Y-101). Sibling: SourceEditor.browser.test.ts.

// Stub the three catalog GETs the card's names come from; keep the rest of api.ts
// real (the types + path helpers `catalog.ts` uses) — the partial-mock pattern
// `VariantBrowser` / `CatalogNodeView` use for the same reads.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getDataWarnings: vi.fn().mockResolvedValue([]),
    getCatalogNode: vi.fn(),
    getCatalogRoot: vi.fn(),
    getRegisterVariants: vi.fn(),
  };
});

beforeEach(() => {
  // projectStore is a module singleton; start each test from a fresh draft so the
  // stable-id mirror (sourceId/bindingId) resolves for index 0.
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
  // The name cache is a session singleton too — a case stubbing a different
  // catalog for the same coordinate must not read the previous case's answer.
  resetCatalogNames();
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getCatalogRoot).mockReset();
  vi.mocked(getRegisterVariants).mockReset();
  stubCatalog();
});

// ── Y-81: the source's period, edited on the card ────────────────────────────
//
// A researcher whose study window is 2005–2020 wants ONE source to reach back to
// 1990. The period is a field of the SOURCE, not of any single pick, and this card
// is the only surface that shows a source whole — its coordinate and every column
// it carries — so it is the one field the cart authors. The rewrite goes through
// the store's guarded, period-only path (`project_store.staged.svelte.test.ts` owns
// its semantics); what is pinned here is the card's half: the entry, the deviation
// marker, and the refusal when the source moved underneath.
describe("SourceEditor source period (Y-81)", () => {
  it("rewrites this source's period and nothing else", async () => {
    const source = seedSource(2020);
    await renderCard(source);

    await page.getByRole("textbox", { name: "From" }).fill("1990");
    await page.getByRole("button", { name: /Apply period/ }).click();

    // The write says so: a form that changed nothing visible and reported nothing
    // leaves the researcher guessing whether Apply did anything.
    await expect
      .element(page.getByText("Period set to 1990–2020."))
      .toBeVisible();
    // The span moved; the column list and the source itself did not — a period-only
    // diff, never unioned with anything staged.
    expect(projectStore.draft?.sources?.[0]?.period).toEqual({
      from: 1990,
      to: 2020,
    });
    expect(projectStore.draft?.sources?.[0]?.bindings).toHaveLength(1);
    expect(projectStore.draft?.sources).toHaveLength(1);
  });

  it("refuses years that name no range, saying which field is at fault", async () => {
    const source = seedSource(2020);
    await renderCard(source);

    await page.getByRole("textbox", { name: "From" }).fill("2030");
    await page.getByRole("button", { name: /Apply period/ }).click();

    await expect
      .element(page.getByText(/From 2030 is after To 2020/))
      .toBeVisible();
    // The refusal wrote nothing, and the years stay as typed so the researcher can
    // see what was refused.
    expect(projectStore.draft?.sources?.[0]?.period).toBe(2020);
    await expect
      .element(page.getByRole("textbox", { name: "From" }))
      .toHaveValue("2030");
  });

  // The window is an authoring SEED, not an inheritance: a source may deliberately
  // cover more or less, so a divergence is MARKED, never warned about.
  it("marks nothing when the period matches the study window", async () => {
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/arbetsstallen",
      period: { from: 2005, to: 2020 },
      bindings: [],
    } as unknown as Source;
    await renderCard(source, { studyWindow: { from: 2005, to: 2020 } });

    await expect
      .element(page.getByRole("textbox", { name: "From" }))
      .toHaveValue("2005");
    expect(page.getByText(/Differs from study window/).query()).toBeNull();
  });

  // The marker compares SPANS, not spellings: a hand-authored single-year source
  // writes `{from, to}` where a pick writes the bare year, and the two cover the
  // same one year. Marking that as a divergence would send a researcher looking
  // for a difference that isn't there.
  it("marks nothing for a one-year period spelled as a range", async () => {
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/arbetsstallen",
      period: { from: 2020, to: 2020 },
      bindings: [],
    } as unknown as Source;
    await renderCard(source, { studyWindow: { from: 2020, to: 2020 } });

    await expect
      .element(page.getByRole("textbox", { name: "To" }))
      .toHaveValue("2020");
    expect(page.getByText(/Differs from study window/).query()).toBeNull();
  });

  it("refuses an Apply whose source moved under the edit", async () => {
    const source = seedSource(2020);
    await renderCard(source);

    await page.getByRole("textbox", { name: "From" }).fill("1990");
    // …and the draft moves underneath: another column joins this very source, so
    // the source the researcher was looking at is not the one on the draft now.
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/arbetsstallen",
          period: 2020,
          binding: { variable: "scb/lisa/alder", type: "opaque" },
        },
      ],
    });
    await page.getByRole("button", { name: /Apply period/ }).click();

    await expect
      .element(page.getByRole("alert"))
      .toMatchTextContent(/This source changed while you were editing/);
    // The competing write stands; the refused edit wrote nothing over it.
    expect(projectStore.draft?.sources?.[0]?.period).toBe(2020);
    expect(projectStore.draft?.sources?.[0]?.bindings).toHaveLength(2);
  });

  // The draft lifecycle is APPLICATION-owned and its restore is ASYNCHRONOUS, so
  // the card waits on the same gate the catalog's Add waits on: the store checks
  // an edit against the draft it FINDS, and a write issued while the restore is in
  // flight is checked against a draft the lifecycle has not finished loading.
  it("holds the write until the app-owned draft restore has settled, freezing the fields meanwhile", async () => {
    const source = seedSource(2020);
    const { promise: gate, resolve: release } = Promise.withResolvers<void>();
    const restoring = vi.spyOn(projectStore, "restored", "get");
    restoring.mockReturnValue(gate);
    try {
      await renderCard(source);

      const from = page.getByRole("textbox", { name: "From" });
      const apply = page.getByRole("button", { name: /Apply period/ });
      await from.fill("1990");
      await apply.click();

      // Still restoring: the wait says so (a4) rather than nothing at all, and the
      // fields freeze on what THIS press captured (a1/a3) — the value it typed
      // before the press is what the eventual write uses, not a `2020` a second
      // press could otherwise queue behind it.
      await expect
        .element(page.getByText("Waiting for the project to load…"))
        .toBeVisible();
      await expect.element(from).toHaveAttribute("readonly");
      await expect.element(apply).toHaveAttribute("aria-disabled", "true");
      expect(projectStore.draft?.sources?.[0]?.period).toBe(2020);
      expect(page.getByText(/Period set to/).query()).toBeNull();

      // …and once the gate opens it lands, checked against the settled draft, with
      // the fields live again.
      release();
      await expect
        .element(page.getByText("Period set to 1990–2020."))
        .toBeVisible();
      expect(projectStore.draft?.sources?.[0]?.period).toEqual({
        from: 1990,
        to: 2020,
      });
      await expect.element(from).not.toHaveAttribute("readonly");
      await expect.element(apply).toHaveAttribute("aria-disabled", "false");
    } finally {
      restoring.mockRestore();
    }
  });

  // a1/a3: the fields staying editable through the wait let a second Apply queue
  // behind the first on the SAME gate — and once the first's write landed, the
  // second would find the draft it captured already moved, raising a staleness
  // refusal for a write that did succeed. Freezing the controls for the whole span
  // makes a second press a no-op instead.
  it("ignores a second Apply held on the same restore gate, so a landed write is never re-checked as stale", async () => {
    const source = seedSource(2020);
    const { promise: gate, resolve: release } = Promise.withResolvers<void>();
    const restoring = vi.spyOn(projectStore, "restored", "get");
    restoring.mockReturnValue(gate);
    try {
      await renderCard(source);

      await page.getByRole("textbox", { name: "From" }).fill("1990");
      const apply = page.getByRole("button", { name: /Apply period/ });
      await apply.click();
      await expect
        .element(page.getByText("Waiting for the project to load…"))
        .toBeVisible();
      // A second press while the first still holds the gate: nothing new to
      // contribute, and it must not queue a write of its own. `force` bypasses
      // Playwright's own actionability wait (which already refuses to click an
      // `aria-disabled` element) — the guard under test is `applyPeriod`'s own,
      // for whatever got a click event through regardless.
      await apply.click({ force: true });

      release();
      await expect
        .element(page.getByText("Period set to 1990–2020."))
        .toBeVisible();
      expect(projectStore.draft?.sources?.[0]?.period).toEqual({
        from: 1990,
        to: 2020,
      });
      // A second queued write would have found the draft already moved by the
      // first and raised the staleness alert instead.
      expect(page.getByRole("alert").query()).toBeNull();
    } finally {
      restoring.mockRestore();
    }
  });

  it("retires a stale confirmation once a later Apply finds nothing to change", async () => {
    // A real write, then an untouched second press: the line must switch to the
    // unchanged notice rather than leaving the earlier "Period set to" standing —
    // it is no longer what this press did.
    const source = seedSource(2020);
    await renderCard(source);

    await page.getByRole("textbox", { name: "From" }).fill("1990");
    await page.getByRole("button", { name: /Apply period/ }).click();
    await expect
      .element(page.getByText("Period set to 1990–2020."))
      .toBeVisible();

    await page.getByRole("button", { name: /Apply period/ }).click();
    await expect.element(page.getByText("Period unchanged.")).toBeVisible();
    expect(page.getByText(/Period set to/).query()).toBeNull();
  });

  // a2: two sources an imported spec put on the SAME register_variant share a
  // heading and a variant — the one thing still unique between them is the
  // source's own generated name, so it has to lead every per-source control's
  // accessible name or a screen-reader controls list can't tell them apart.
  it("names two sources on the SAME register variant by their own source name", async () => {
    const lisaCore = {
      name: "lisa-core",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [],
    } as unknown as Source;
    const lisaLonfink = {
      name: "lisa-lonfink",
      register_variant: "scb/lisa/v1",
      period: 2018,
      bindings: [],
    } as unknown as Source;
    await renderCard(lisaCore, { sourceIndex: 0 });
    await renderCard(lisaLonfink, { sourceIndex: 1 });

    await expect
      .element(
        page.getByRole("button", {
          name: "Apply period for lisa-core (LISA, Individer 15+)",
          exact: true,
        }),
      )
      .toBeVisible();
    await expect
      .element(
        page.getByRole("button", {
          name: "Apply period for lisa-lonfink (LISA, Individer 15+)",
          exact: true,
        }),
      )
      .toBeVisible();
    await expect
      .element(
        page.getByRole("group", {
          name: "Period for lisa-core (LISA, Individer 15+)",
          exact: true,
        }),
      )
      .toBeVisible();
    await expect
      .element(
        page.getByRole("group", {
          name: "Period for lisa-lonfink (LISA, Individer 15+)",
          exact: true,
        }),
      )
      .toBeVisible();
  });

  it("drops a held write whose card left the page meanwhile", async () => {
    // Same wait, the other way out of it: the gate's wait is UNBOUNDED, so a card
    // gone before it settles — the researcher left /project — must not have its
    // late continuation write into the draft behind them.
    const source = seedSource(2020);
    const { promise: gate, resolve: release } = Promise.withResolvers<void>();
    const restoring = vi.spyOn(projectStore, "restored", "get");
    restoring.mockReturnValue(gate);
    try {
      const view = await renderCard(source);

      await page.getByRole("textbox", { name: "From" }).fill("1990");
      await page.getByRole("button", { name: /Apply period/ }).click();
      view.unmount();
      release();

      await new Promise((r) => setTimeout(r, 100));
      expect(projectStore.draft?.sources?.[0]?.period).toBe(2020);
    } finally {
      restoring.mockRestore();
    }
  });

  // A TOKEN period — a different vocabulary nothing in the SPA authors (Y-101).
  // Collapsing it into a span would order years nobody asked for, so the card
  // shows it as it stands.
  it("leaves a token period alone, read-only", async () => {
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/arbetsstallen",
      period: "HT2018",
      bindings: [],
    } as unknown as Source;
    await renderCard(source);

    await expect.element(page.getByText("HT2018")).toBeVisible();
    await expect
      .element(page.getByText(/can't express this period/))
      .toBeVisible();
    expect(page.getByRole("textbox").elements()).toHaveLength(0);
  });

  // Y-80 rider: the column rows resolve their default name at THIS source's own
  // (variant, period) — the card threads both into BindingEditor, and they have to
  // reach the catalog GET as the wire `?period=` / `?variant=` a catalog URL spells.
  it("resolves its columns at its own variant and period, in wire form", async () => {
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/arbetsstallen",
      period: { from: 1990, to: 2020 },
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    // Only the resolve at the card's own variant and period, in wire form, names
    // the column; any other coordinate resolves to nothing, so the row would keep
    // its bare FQID.
    vi.mocked(getCatalogNode).mockImplementation(async (fqid, params) => {
      if (fqid === "scb") {
        return providerNode("scb", registerNode("scb/lisa", "LISA"));
      }
      const own =
        fqid === "scb/lisa/kon" &&
        params?.period === "1990..2020" &&
        params?.variant === "arbetsstallen";
      return {
        states: own
          ? [
              {
                warning_ids: [],
                state_id: "1",
                period_scope: "intervals",
                variant: "arbetsstallen",
                variant_label: null,
                register_variant_id: "1",
                valid_from: "1990-01-01",
                valid_to: "9999-12-31",
                data_type: "int",
                data_length: null,
                delivery_column_name: "KonArb",
                source_register_text: null,
                provenance: null,
                pooled: false,
                value_set_version_label: "",
                value_set_id: null,
                value_set: null,
                value_set_summary: null,
                is_identifier: false,
                classifications: [],
              },
            ]
          : [],
      } as unknown as StatesResponse;
    });
    await renderCard(source);

    await expect.element(page.getByText("KonArb")).toBeVisible();
  });
});

// ── Y-101: a multi-segment source period, one From/To row per segment ───────
//
// A source whose period is a #307 comma list (`2015..2017,2019..2020` —
// `mergePeriods`' own output for two disjoint picks on one register variant) used
// to render read-only. Every segment here is a plain year or a uniform-year
// range, so it is authored the same way a single range is: one row per segment,
// "Add years", a per-row "Remove", ONE Apply.
describe("SourceEditor source period list segments (Y-101)", () => {
  it("renders one row per segment, in stored order, and applies an edit as a list", async () => {
    const source = seedSource([
      { from: 2015, to: 2017 },
      { from: 2019, to: 2020 },
    ]);
    await renderCard(source);

    const froms = page.getByRole("textbox", { name: "From" });
    const tos = page.getByRole("textbox", { name: "To" });
    expect(froms.elements()).toHaveLength(2);
    await expect.element(froms.nth(0)).toHaveValue("2015");
    await expect.element(tos.nth(0)).toHaveValue("2017");
    await expect.element(froms.nth(1)).toHaveValue("2019");
    await expect.element(tos.nth(1)).toHaveValue("2020");

    // Extend the second segment — well clear of the first, so the two stay two
    // segments rather than fusing (a touching pair adjacency-merges, its own case
    // below).
    await tos.nth(1).fill("2022");
    await page.getByRole("button", { name: /Apply period/ }).click();

    await expect
      .element(page.getByText("Period set to 2015–2017, 2019–2022."))
      .toBeVisible();
    expect(projectStore.draft?.sources?.[0]?.period).toEqual([
      { from: 2015, to: 2017 },
      { from: 2019, to: 2022 },
    ]);
  });

  it("adds a segment row and applies it as an extra list member", async () => {
    const source = seedSource(2020);
    await renderCard(source);

    // A single-range period has no Remove — one row is the floor.
    expect(
      page.getByRole("button", { name: "Remove", exact: true }).query(),
    ).toBeNull();

    await page.getByRole("button", { name: "Add years" }).click();
    const froms = page.getByRole("textbox", { name: "From" });
    expect(froms.elements()).toHaveLength(2);

    await froms.nth(1).fill("2022");
    await page.getByRole("textbox", { name: "To" }).nth(1).fill("2023");
    await page.getByRole("button", { name: /Apply period/ }).click();

    await expect.element(page.getByText(/Period set to/)).toBeVisible();
    expect(projectStore.draft?.sources?.[0]?.period).toEqual([
      2020,
      { from: 2022, to: 2023 },
    ]);
  });

  it("reduces a two-segment period to one row and writes the single-range wire, not a one-element list", async () => {
    const source = seedSource([
      { from: 2015, to: 2017 },
      { from: 2019, to: 2020 },
    ]);
    await renderCard(source);

    await page
      .getByRole("button", { name: "Remove", exact: true })
      .first()
      .click();
    expect(page.getByRole("textbox", { name: "From" }).elements()).toHaveLength(
      1,
    );
    // The remaining row is the row that WASN'T removed (2019–2020), not a reset.
    await expect
      .element(page.getByRole("textbox", { name: "From" }))
      .toHaveValue("2019");
    await page.getByRole("button", { name: /Apply period/ }).click();

    expect(projectStore.draft?.sources?.[0]?.period).toEqual({
      from: 2019,
      to: 2020,
    });
  });

  it("refuses an overlapping pair of segments, naming both spans", async () => {
    const source = seedSource([
      { from: 2015, to: 2018 },
      { from: 2019, to: 2020 },
    ]);
    await renderCard(source);

    await page.getByRole("textbox", { name: "From" }).nth(1).fill("2017");
    await page.getByRole("button", { name: /Apply period/ }).click();

    await expect
      .element(page.getByText(/2015–2018 and 2017–2020 overlap/))
      .toBeVisible();
    // Refused: the stored list is untouched.
    expect(projectStore.draft?.sources?.[0]?.period).toEqual([
      { from: 2015, to: 2018 },
      { from: 2019, to: 2020 },
    ]);
  });

  it("marks a list period against the study window by the years it covers", async () => {
    // Touching segments cover the window's years exactly: no mark, however the wire
    // spells them.
    const touching = await renderCard(
      {
        name: "LISA",
        register_variant: "scb/lisa/arbetsstallen",
        period: [
          { from: 2015, to: 2017 },
          { from: 2018, to: 2020 },
        ],
        bindings: [],
      } as unknown as Source,
      { studyWindow: { from: 2015, to: 2020 } },
    );
    expect(page.getByText(/Differs from study window/).query()).toBeNull();
    touching.unmount();

    // A hole (2018) inside the window is a divergence too — the source does not
    // cover every year of it — so it is marked, like a narrower or wider span.
    const spanning = {
      name: "LISA",
      register_variant: "scb/lisa/arbetsstallen",
      period: [
        { from: 2015, to: 2017 },
        { from: 2019, to: 2020 },
      ],
      bindings: [],
    } as unknown as Source;
    const holed = await renderCard(spanning, {
      studyWindow: { from: 2015, to: 2020 },
    });
    await expect
      .element(page.getByText("Differs from study window 2015–2020"))
      .toBeVisible();
    holed.unmount();

    await renderCard(spanning, { studyWindow: { from: 2010, to: 2020 } });
    await expect
      .element(page.getByText("Differs from study window 2010–2020"))
      .toBeVisible();
  });
});
