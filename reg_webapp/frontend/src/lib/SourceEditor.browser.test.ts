import type { ComponentProps } from "svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  CatalogNode,
  RootResponse,
  StatesResponse,
  VariantsResponse,
} from "./api";
import { getCatalogNode, getCatalogRoot, getRegisterVariants } from "./api";
import { resetCatalogNames } from "./catalog_names.svelte";
import type { Period, Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";

// #991/#993: SourceEditor is the cart source card — it DISPLAYS the register it
// delivers from, its coordinate/name and its columns, and offers delete only. No
// name / register_variant inputs, no variant picker. Y-81: its one EDITABLE field is
// the source's PERIOD, authored as a year range. Y-75: the card is titled by its
// REGISTER (the thing the researcher picked), the columns are called columns, and
// dropping a whole source asks first.
// Y-80: the register and the variant are AS THE CATALOG NAMES THEM, read from the
// catalog (the draft holds only the coordinate) and never a slug rule — and they are
// composed as separate elements, not strung into one dot-joined heading.

// Stub the three catalog GETs the card's names come from; keep the rest of api.ts
// real (the types + path helpers `catalog.ts` uses) — the partial-mock pattern
// `VariantBrowser` / `CatalogNodeView` use for the same reads.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
    getCatalogRoot: vi.fn(),
    getRegisterVariants: vi.fn(),
  };
});

/** The catalog root as a deployment serving `providers` (the shell's facet list,
 * and what decides whether a card's title carries its provider). */
function rootResponse(
  ...providers: { fqid: string; name: string }[]
): RootResponse {
  return {
    kind: "root",
    children: providers.map(({ fqid, name }) => ({
      kind: "provider",
      fqid,
      name,
    })),
  } as unknown as RootResponse;
}

/** A register entry carrying its curated display name — "LISA", "MiDAS": the word
 * the register's own catalog page is headed with. */
function registerNode(fqid: string, name: string | null) {
  return { kind: "register", fqid, name, purpose: null, coverage: null };
}

/** A provider node listing the registers it owns — the read the card's register
 * word comes from (one light payload per provider, not one register node each). */
function providerNode(
  fqid: string,
  ...registers: ReturnType<typeof registerNode>[]
): CatalogNode {
  return {
    kind: "provider",
    fqid,
    name: fqid,
    children: registers,
  } as unknown as CatalogNode;
}

/** A register's variant list, as `GET /{provider}/{register}/variants` returns it. */
function variantsResponse(
  ...variants: { slug: string; name?: string | null }[]
): VariantsResponse {
  return { variants } as unknown as VariantsResponse;
}

/** The LISA fixture every case starts from: a single-provider deployment whose
 * `scb/lisa` register delivers the two individual-frame variants of one succession
 * family plus the workplace frame. A case that needs another register/deployment
 * overrides the mock it cares about. */
function stubCatalog(): void {
  vi.mocked(getCatalogRoot).mockResolvedValue(
    rootResponse({ fqid: "scb", name: "Statistiska Centralbyrån" }),
  );
  vi.mocked(getCatalogNode).mockImplementation(async (fqid) =>
    fqid === "scb"
      ? providerNode("scb", registerNode("scb/lisa", "LISA"))
      : // The columns' own leaf resolve (BindingEditor.browser.test.ts owns it):
        // nothing covering, so the rows show their FQID alone.
        ({ states: [] } as unknown as StatesResponse),
  );
  vi.mocked(getRegisterVariants).mockResolvedValue(
    variantsResponse(
      { slug: "v1", name: "Individer 15+" },
      { slug: "individer-15plus", name: "Individer, 15 år och äldre" },
      { slug: "individer-16plus", name: "Individer, 16 år och äldre" },
      { slug: "arbetsstallen", name: "Arbetsställen" },
    ),
  );
}

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

/** The card under test, always at index 0 of the fresh draft above. Every case
 * renders it with the same three constants, so a case that overrides one — the
 * multi-provider deployment, a standing validation error — says so and nothing
 * else. */
function renderCard(
  source: Source,
  overrides: Partial<ComponentProps<typeof SourceEditor>> = {},
) {
  return render(SourceEditor, {
    sourceIndex: 0,
    source,
    issues: [],
    providerQualified: false,
    studyWindow: null,
    ...overrides,
  });
}

/** A one-column LISA Arbetsställen source IN THE DRAFT — the store's staleness
 * guard re-reads the draft, so a card edited against a detached object would be
 * refused every time. Returns the slot to render. */
function seedSource(period: Period): Source {
  projectStore.applyStagedDiff({
    adds: [
      {
        registerVariant: "scb/lisa/arbetsstallen",
        period,
        binding: { variable: "scb/lisa/kon", type: "categorical" },
      },
    ],
  });
  return projectStore.draft?.sources?.[0] as Source;
}

describe("SourceEditor cart card", () => {
  it("displays the register_variant read-only, with the period as its one field", async () => {
    const source = {
      name: "lisa_main",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    await renderCard(source);

    // The coordinate is shown read-only…
    await expect.element(page.getByText("scb/lisa/v1")).toBeVisible();
    // …the period as the two year fields that author it (Y-81)…
    await expect
      .element(page.getByRole("textbox", { name: "From" }))
      .toHaveValue("2020");
    await expect
      .element(page.getByRole("textbox", { name: "To" }))
      .toHaveValue("2020");
    // …the column's variable is shown…
    await expect.element(page.getByText("scb/lisa/kon")).toBeVisible();

    // …and those two years are the WHOLE of the card's authoring: no name field, no
    // "Pick variant", no way to add a column from the cart.
    expect(page.getByRole("textbox").elements()).toHaveLength(2);
    expect(
      page.getByRole("button", { name: /Pick variant/ }).query(),
    ).toBeNull();
    expect(
      page.getByRole("button", { name: "Add binding" }).query(),
    ).toBeNull();
  });

  // Y-75: `name` is a generated join key — three variants of one register mint
  // `LISA`, `LISA_2`, `LISA_3`, which name nothing a researcher picked. The card is
  // titled by the REGISTER; the name stays visible as a detail because panels and
  // `OrderEntry.source` join on it. Y-80: by the register's CATALOG name.
  it("heads the card with its register, qualified by its variant, keeping the generated name as a detail", async () => {
    const source = {
      name: "LISA_2",
      register_variant: "scb/lisa/individer-16plus",
      period: 2020,
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    await renderCard(source);

    await expect
      .element(page.getByRole("heading", { name: "LISA", exact: true }))
      .toBeVisible();
    // The variant that names the population qualifies the heading as its OWN
    // element under it — nothing on the card strings the two into one line.
    await expect
      .element(page.getByText("Individer, 16 år och äldre", { exact: true }))
      .toBeVisible();
    expect(document.body.textContent).not.toContain("·");
    // One provider in this deployment: the register name is unambiguous, so the
    // card does not spend a row on the word every card would carry.
    expect(page.getByText("Provider", { exact: true }).query()).toBeNull();
    // The delete button says the same thing as a sentence, led by the source's own
    // NAME (Y-98) — the one thing that still differs between two sources an
    // imported spec puts on the same register_variant — with the heading and
    // variant alongside for a reader who does not recognise it on its own.
    await expect
      .element(
        page.getByRole("button", {
          name: "Remove source LISA_2 (LISA, Individer, 16 år och äldre)",
          exact: true,
        }),
      )
      .toBeVisible();
    // The concrete variant the source extracts stays on the card, in mono.
    await expect.element(page.getByText("Register variant")).toBeVisible();
    await expect
      .element(page.getByText("scb/lisa/individer-16plus", { exact: true }))
      .toBeVisible();
    // The generated name is not the title, and it has not left the card either.
    await expect.element(page.getByText("Source name")).toBeVisible();
    await expect
      .element(page.getByText("LISA_2", { exact: true }))
      .toBeVisible();
  });

  it("names a variant with no family too, from the same catalog read", async () => {
    // Y-80's complaint in one case: before, only LISA's individual frame had a
    // label at all and every other variant read as a bare coordinate.
    const source = {
      name: "LISA_3",
      register_variant: "scb/lisa/arbetsstallen",
      period: 2020,
      bindings: [],
    } as unknown as Source;
    await renderCard(source);

    await expect
      .element(page.getByRole("heading", { name: "LISA", exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("Arbetsställen", { exact: true }))
      .toBeVisible();
  });

  it("reads two sources of one variant family as one family with a changed frame", async () => {
    // reg_meta derives a family label as the common leading stem of its members'
    // names, so each member's own name already leads with the family word: the two
    // cards line up on "Individer" and differ in the frame.
    const older = {
      name: "LISA",
      register_variant: "scb/lisa/individer-16plus",
      period: { from: 1990, to: 2009 },
      bindings: [],
    } as unknown as Source;
    const newer = {
      name: "LISA_2",
      register_variant: "scb/lisa/individer-15plus",
      period: { from: 2010, to: 2020 },
      bindings: [],
    } as unknown as Source;
    const view = await renderCard(older);
    await expect
      .element(page.getByRole("heading", { name: "LISA", exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("Individer, 16 år och äldre", { exact: true }))
      .toBeVisible();
    view.unmount();

    await renderCard(newer);
    await expect
      .element(page.getByRole("heading", { name: "LISA", exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("Individer, 15 år och äldre", { exact: true }))
      .toBeVisible();
    // ONE variants read for the register both cards sit on: the name cache is keyed
    // per register, so the second card — and every re-render of either — asks
    // nothing. A cart of a hundred columns on one register makes this one request.
    expect(vi.mocked(getRegisterVariants).mock.calls).toHaveLength(1);
  });

  it("titles a `_default` variant with the register alone", async () => {
    // `_default` is the whole-register default, not a population anyone picked.
    vi.mocked(getCatalogNode).mockResolvedValue(
      providerNode("fk", registerNode("fk/midas", "MiDAS")),
    );
    vi.mocked(getRegisterVariants).mockResolvedValue(
      variantsResponse({ slug: "_default", name: null }),
    );
    const source = {
      name: "MIDAS",
      register_variant: "fk/midas/_default",
      period: 2020,
      bindings: [],
    } as unknown as Source;
    await renderCard(source);

    // The catalog's spelling, not the slug uppercased: "MiDAS", never "MIDAS".
    await expect
      .element(page.getByRole("heading", { name: "MiDAS", exact: true }))
      .toBeVisible();
    // Naming no population, it adds no qualifying line — and none to the button.
    expect(document.querySelector(".source-variant")).toBeNull();
    await expect
      .element(
        page.getByRole("button", {
          name: "Remove source MIDAS (MiDAS)",
          exact: true,
        }),
      )
      .toBeVisible();
  });

  it("names the provider that owns the register where the deployment has more than one", async () => {
    vi.mocked(getCatalogRoot).mockResolvedValue(
      rootResponse(
        { fqid: "scb", name: "Statistiska Centralbyrån" },
        { fqid: "fk", name: "Försäkringskassan" },
      ),
    );
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/individer-15plus",
      period: 2020,
      bindings: [],
    } as unknown as Source;
    await renderCard(source, { providerQualified: true });

    // The register still HEADS the card, qualified by the variant that names the
    // population. The provider that OWNS it is an attribute of the register rather
    // than part of the pick, so it joins the card's other named attributes as a
    // LABELLED row — nothing on the card is a second unlabelled qualifier line.
    await expect
      .element(page.getByRole("heading", { name: "LISA", exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("Provider", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("Statistiska Centralbyrån", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("Individer, 15 år och äldre", { exact: true }))
      .toBeVisible();
    // The rail's own spelling — never `slug.toUpperCase()` — and never dot-strung.
    expect(document.body.textContent).not.toContain("SCB LISA");
    expect(document.body.textContent).not.toContain("·");
  });

  it("shows the coordinate ALONE, exactly once, while the names are unavailable", async () => {
    // Offline, or a coordinate outside this steward's catalog: the card must stay
    // readable and must not invent a word. The title falls back to the coordinate,
    // and the detail row does not then repeat it.
    vi.mocked(getCatalogNode).mockRejectedValue(new Error("offline"));
    vi.mocked(getRegisterVariants).mockRejectedValue(new Error("offline"));
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/individer-15plus",
      period: 2020,
      bindings: [],
    } as unknown as Source;
    await renderCard(source);

    await expect
      .element(
        page.getByRole("heading", {
          name: "scb/lisa/individer-15plus",
          exact: true,
        }),
      )
      .toBeVisible();
    expect(
      await page.getByText("scb/lisa/individer-15plus", { exact: true }).all(),
    ).toHaveLength(1);
    expect(page.getByText("Register variant").query()).toBeNull();
    // A coordinate standing in for a name is still a machine identifier: mono, like
    // every other FQID the app shows, and said ONCE in the button's name too — the
    // source's own name still leads it.
    const heading = document.querySelector<HTMLElement>(".source-head h3");
    expect(getComputedStyle(heading as HTMLElement).fontFamily).toContain(
      "mono",
    );
    await expect
      .element(
        page.getByRole("button", {
          name: "Remove source LISA (scb/lisa/individer-15plus)",
          exact: true,
        }),
      )
      .toBeVisible();
  });

  it("falls back to the coordinate when the register is named but its variants are not", async () => {
    // Half a name is worse than none: "LISA" alone is how a `_default` source reads,
    // and two variants of one register would title identically. The card says the
    // coordinate until it can say the whole name.
    vi.mocked(getRegisterVariants).mockRejectedValue(new Error("offline"));
    const source = {
      // Not named "LISA": the register's word must be absent from the whole card.
      name: "s1",
      register_variant: "scb/lisa/individer-15plus",
      period: 2020,
      bindings: [],
    } as unknown as Source;
    await renderCard(source);

    await expect
      .element(
        page.getByRole("heading", {
          name: "scb/lisa/individer-15plus",
          exact: true,
        }),
      )
      .toBeVisible();
    expect(page.getByText("LISA", { exact: true }).query()).toBeNull();
  });

  it("arms the year fields empty for a source carrying no period yet", async () => {
    const source = {
      name: "s",
      register_variant: "scb/lisa/v1",
      period: null,
      bindings: [],
    } as unknown as Source;
    await renderCard(source);

    // A missing period is not another grammar — it is what this source lacks, so the
    // same two fields author it, empty. Apply stays live — a dead button explains
    // nothing — and writes nothing until the fields name a range, saying so (a4)
    // rather than leaving a blank press looking like it did something.
    await expect
      .element(page.getByRole("textbox", { name: "From" }))
      .toHaveValue("");
    await expect
      .element(page.getByRole("textbox", { name: "To" }))
      .toHaveValue("");
    await page.getByRole("button", { name: /Apply period/ }).click();
    await expect
      .element(page.getByRole("textbox", { name: "From" }))
      .toHaveValue("");
    expect(page.getByText(/must be a four-digit year/).elements()).toHaveLength(
      0,
    );
    await expect.element(page.getByText("Period unchanged.")).toBeVisible();
  });

  // Y-75: a whole source is the register plus every column taken from it, and
  // nothing in the read-only cart puts them back — so the removal asks first, and
  // the question names both.
  it("asks before removing a source, naming the register and its column count", async () => {
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/v1",
          period: 2020,
          binding: { variable: "scb/lisa/kon", type: "categorical" },
        },
        {
          registerVariant: "scb/lisa/v1",
          period: 2020,
          binding: { variable: "scb/lisa/adeldag", type: "opaque" },
        },
        {
          registerVariant: "scb/rtb/v1",
          period: 2019,
          binding: { variable: "scb/rtb/x", type: "opaque" },
        },
      ],
    });
    const source = projectStore.draft?.sources?.[0] as Source;
    await renderCard(source);

    await page.getByRole("button", { name: "Remove source" }).click();

    const dialog = page.getByRole("alertdialog");
    await expect
      .element(dialog)
      .toMatchTextContent(
        /Remove the source LISA \(LISA, Individer 15\+\) and its 2 columns\?/,
      );
    // Nothing has gone yet — the question is the whole effect of the first click.
    expect(projectStore.draft?.sources).toHaveLength(2);

    await dialog.getByRole("button", { name: "Cancel" }).click();
    expect(projectStore.draft?.sources).toHaveLength(2);
  });

  it("removes the source through the store once the removal is confirmed", async () => {
    // Seed a two-source draft so we can observe the removal against the store.
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/v1",
          period: 2020,
          binding: { variable: "scb/lisa/kon", type: "categorical" },
        },
        {
          registerVariant: "scb/rtb/v1",
          period: 2019,
          binding: { variable: "scb/rtb/x", type: "opaque" },
        },
      ],
    });
    const source = projectStore.draft?.sources?.[0] as Source;
    await renderCard(source);

    await page.getByRole("button", { name: "Remove source" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect.element(dialog).toMatchTextContent(/and its 1 column\?/);
    await dialog.getByRole("button", { name: "Remove source" }).click();

    // The store dropped source 0 (scb/lisa/v1); scb/rtb/v1 survives.
    expect(projectStore.draft?.sources?.map((s) => s.register_variant)).toEqual(
      ["scb/rtb/v1"],
    );
  });

  it("renders an alert (not a crash) when bindings is a non-array", async () => {
    const source = {
      name: "bad",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: "oops",
    } as unknown as Source;
    await renderCard(source);

    const alert = page.getByRole("alert");
    await expect.element(alert).toBeVisible();
    await expect.element(alert).toMatchTextContent(/bindings\s+are malformed/);
  });

  it("renders an alert (not a crash) when the source slot is null", async () => {
    // A `sources: [null, …]` slot: SourceEditor must degrade to a malformed card
    // rather than deref `source.<field>` and throw (defense in depth for the render
    // boundary — ProjectEditor passes the raw slot straight in).
    await renderCard(null as unknown as Source);

    const alert = page.getByRole("alert");
    await expect.element(alert).toBeVisible();
    await expect.element(alert).toMatchTextContent(/source entry is malformed/);
    // Still removable — the degraded card keeps its Remove affordance.
    await expect
      .element(page.getByRole("button", { name: /Remove source/ }))
      .toBeVisible();
    // A malformed slot names no coordinate, so it asks the catalog nothing.
    expect(vi.mocked(getCatalogNode).mock.calls).toHaveLength(0);
  });

  it("counts and empties in COLUMNS, the word the rest of the app uses", async () => {
    const source = {
      name: "ok",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [],
    } as Source;
    const view = await renderCard(source);

    await expect
      .element(page.getByRole("heading", { name: "Columns (0)" }))
      .toBeVisible();
    await expect
      .element(
        page.getByText(
          "No columns yet. Browse the catalog to add columns from this register.",
        ),
      )
      .toBeVisible();
    expect(page.getByRole("alert").query()).toBeNull();
    // Nothing on this page calls a column a binding any more.
    expect(document.body.textContent).not.toContain("Bindings");
    view.unmount();

    const filled = {
      name: "ok",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [
        { variable: "scb/lisa/kon", type: "categorical" },
        { variable: "scb/lisa/adeldag", type: "opaque" },
      ],
    } as Source;
    await renderCard(filled);

    await expect
      .element(page.getByRole("heading", { name: "Columns (2)" }))
      .toBeVisible();
  });

  it("wraps a long register title, source name and column FQID without horizontal overflow on mobile (#1110)", async () => {
    // Regression for PR #1109's visual-gate finding: at 375px a long unbroken run
    // (`.source-head h3`, a mono KeyValue value, the column FQID in `.binding-body`)
    // formerly refused to shrink (flex/grid items default to `min-width: auto`) and
    // clipped/overflowed the card. `min-width: 0` + `overflow-wrap: anywhere` at
    // those boundaries must let each wrap in-card. Y-75 moved the source NAME out of
    // the heading and into the card's mono detail rows, so the long name is asserted
    // there now. Y-80 puts the catalog's own register name in the heading — a
    // curated name can be long and unbroken too.
    const longRegisterName =
      "Longitudinell_integrationsdatabas_for_sjukforsakrings_och_arbetsmarknadsstudier";
    vi.mocked(getCatalogNode).mockResolvedValue(
      providerNode("scb", registerNode("scb/lisa", longRegisterName)),
    );
    const source = {
      name: "a_very_long_source_name_that_would_not_normally_wrap_on_its_own",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [
        {
          variable:
            "scb/lisa/a_very_long_binding_identifier_that_would_not_wrap_either",
          type: "categorical",
        },
      ],
    } as Source;
    const view = await renderCard(source);

    // The mobile breakpoint must be active for the mobile-target regression to be
    // meaningful — pin the precondition so a viewport-config change can't silently
    // no-op this test (mirrors the SearchView #808/#806 wrap regression).
    expect(window.matchMedia("(max-width: 48rem)").matches).toBe(true);

    // Pin the card to the 375px canvas (narrowest mobile target); border-box keeps
    // 375 inclusive of the card's padding so content resolves against the real width.
    const root = document.querySelector<HTMLElement>(".source");
    expect(root).not.toBeNull();
    if (root) {
      root.style.boxSizing = "border-box";
      root.style.width = "375px";
    }

    // Assert on the CONSTRAINED containers, not the leaf text nodes: the `h3` /
    // `.variable-value` are flex items that (pre-fix) keep `min-width: auto` and take
    // their full content width, so their OWN scrollWidth == clientWidth even while
    // overflowing the card — the overflow shows up on the bounded parent. This mirrors
    // the SearchView #808/#806 regression, which checks the `.cols-1` grid container.
    await expect
      .element(
        page.getByRole("heading", { name: new RegExp(longRegisterName) }),
      )
      .toBeVisible();
    const sourceHead = document.querySelector<HTMLElement>(".source-head");
    expect(sourceHead?.scrollWidth ?? 0).toBeLessThanOrEqual(
      (sourceHead?.clientWidth ?? 0) + 1,
    );

    // The generated name now rides in the mono detail rows, which must break it too.
    const nameRow = [...document.querySelectorAll<HTMLElement>("dd.mono")].find(
      (dd) => dd.textContent?.includes("a_very_long_source_name"),
    );
    expect(nameRow).not.toBeUndefined();

    const variableValue = document.querySelector<HTMLElement>(
      ".binding .variable-value",
    );
    expect(variableValue?.textContent).toContain(
      "a_very_long_binding_identifier_that_would_not_wrap_either",
    );
    const binding = document.querySelector<HTMLElement>(".binding");
    expect(binding?.scrollWidth ?? 0).toBeLessThanOrEqual(
      (binding?.clientWidth ?? 0) + 1,
    );

    // Belt-and-suspenders: the whole card must not overflow its 375px box either.
    expect(root?.scrollWidth ?? 0).toBeLessThanOrEqual(
      (root?.clientWidth ?? 0) + 1,
    );

    view.unmount();
  });

  it("rolls up errors under the source into a header badge", async () => {
    const source = {
      name: "s",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    await renderCard(source, {
      issues: [
        {
          level: "error" as const,
          code: "fqid_unresolved",
          path: "/sources/0/bindings/0/variable",
          message: "nope",
        },
      ],
    });

    await expect.element(page.getByText("1 error")).toBeVisible();
  });
});

// ── Y-81: the source's period, edited on the card ────────────────────────────
//
// A researcher whose study window is 2005–2020 wants ONE source to reach back to
// 1990. The period is a field of the SOURCE, not of any single pick, and this card
// is the only surface that shows a source whole — its coordinate and every column
// it carries — so it is the one field the cart authors. The rewrite goes through
// the store's guarded, period-only path (`project_store.svelte.test.ts` owns its
// semantics); what is pinned here is the card's half: the entry, the deviation
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
  it("marks a period that differs from the study window", async () => {
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/arbetsstallen",
      period: { from: 1990, to: 2020 },
      bindings: [],
    } as unknown as Source;
    await renderCard(source, { studyWindow: { from: 2005, to: 2020 } });

    await expect
      .element(page.getByText("Differs from study window 2005–2020"))
      .toBeVisible();
  });

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

  // a4: an Apply pressed with nothing to change used to return silently, leaving
  // the researcher to guess whether it did anything.
  it("announces when an Apply has nothing to change", async () => {
    const source = seedSource(2020);
    await renderCard(source);

    // Pressed with the fields still showing exactly the stored period.
    await page.getByRole("button", { name: /Apply period/ }).click();
    await expect.element(page.getByText("Period unchanged.")).toBeVisible();
    expect(projectStore.draft?.sources?.[0]?.period).toBe(2020);

    // Typed back to the same value and applied again: still nothing to write.
    await page.getByRole("textbox", { name: "From" }).fill("2020");
    await page.getByRole("button", { name: /Apply period/ }).click();
    await expect.element(page.getByText("Period unchanged.")).toBeVisible();
    expect(projectStore.draft?.sources?.[0]?.period).toBe(2020);
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
    await renderCard(source);

    await expect.element(page.getByText("scb/lisa/kon")).toBeVisible();
    await vi.waitFor(() => {
      expect(vi.mocked(getCatalogNode).mock.calls).toContainEqual([
        "scb/lisa/kon",
        { period: "1990..2020", variant: "arbetsstallen" },
      ]);
    });
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

  it("marks a list period's overall span — first From to last To — against the study window", async () => {
    const spanning = {
      name: "LISA",
      register_variant: "scb/lisa/arbetsstallen",
      period: [
        { from: 2015, to: 2017 },
        { from: 2019, to: 2020 },
      ],
      bindings: [],
    } as unknown as Source;
    const matching = await renderCard(spanning, {
      studyWindow: { from: 2015, to: 2020 },
    });
    // The internal gap (2018) doesn't matter — only the outer span does, same as
    // a single range.
    expect(page.getByText(/Differs from study window/).query()).toBeNull();
    matching.unmount();

    await renderCard(spanning, { studyWindow: { from: 2010, to: 2020 } });
    await expect
      .element(page.getByText("Differs from study window 2010–2020"))
      .toBeVisible();
  });
});
