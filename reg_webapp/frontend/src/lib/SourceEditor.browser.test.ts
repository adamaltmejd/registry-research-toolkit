import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { getShow, getStates } from "./api";
import { resetCatalogNames } from "./catalog_names.svelte";
import type { ProjectSource } from "./project_data";
import { projectStore } from "./project_store.svelte";
import { storedProject } from "./project-store-test-helpers";
import {
  providerShow,
  registerChild,
  registerShow,
  renderCard,
  rootShow,
  stubCatalog,
} from "./source-editor-test-helpers";

// #991/#993: SourceEditor is the cart source card — it DISPLAYS the register it
// delivers from, its coordinate/name and its columns, and offers delete only. No
// name / register_variant inputs, no variant picker. Y-81: its one EDITABLE field is
// the source's PERIOD, authored as a year range. Y-75: the card is titled by its
// REGISTER (the thing the researcher picked), the columns are called columns, and
// dropping a whole source asks first.
// Y-80: the register and the variant are AS THE CATALOG NAMES THEM, read from the
// catalog (the draft holds only the coordinate) and never a slug rule — and they are
// composed as separate elements, not strung into one dot-joined heading.

// Stub the catalog reads the card's names and columns come from (`show`,
// `states`); keep the rest of api.ts real (the types + path helpers `catalog.ts`
// uses) — the partial-mock pattern `VariantBrowser` / `CatalogNodeView` use for
// the same reads.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getWarnings: vi.fn().mockResolvedValue([]),
    getShow: vi.fn(),
    getStates: vi.fn(),
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
  vi.mocked(getShow).mockReset();
  vi.mocked(getStates).mockReset();
  stubCatalog();
});
describe("SourceEditor cart card", () => {
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
    } as ProjectSource;
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

  it("reads two sources of one variant family as one family with a changed frame", async () => {
    // reg_meta derives a family label as the common leading stem of its members'
    // names, so each member's own name already leads with the family word: the two
    // cards line up on "Individer" and differ in the frame.
    const older = {
      name: "LISA",
      register_variant: "scb/lisa/individer-16plus",
      period: { from: 1990, to: 2009 },
      bindings: [],
    } as unknown as ProjectSource;
    const newer = {
      name: "LISA_2",
      register_variant: "scb/lisa/individer-15plus",
      period: { from: 2010, to: 2020 },
      bindings: [],
    } as unknown as ProjectSource;
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
    // ONE register read for the register both cards sit on: the name cache is
    // keyed per register, so the second card — and every re-render of either —
    // asks nothing. A cart of a hundred columns on one register makes this one
    // request. Fails if the variants are read per card (they ride the register's
    // `show` since package C).
    expect(
      vi.mocked(getShow).mock.calls.filter(([ref]) => ref === "scb/lisa"),
    ).toHaveLength(1);
  });

  it("titles a `_default` variant with the register alone", async () => {
    // `_default` is the whole-register default, not a population anyone picked.
    stubCatalog({
      fk: providerShow("fk", registerChild("fk/midas", "MiDAS")),
      "fk/midas": registerShow("fk/midas", { slug: "_default", name: null }),
    });
    const source = {
      name: "MIDAS",
      register_variant: "fk/midas/_default",
      period: 2020,
      bindings: [],
    } as unknown as ProjectSource;
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
    stubCatalog({
      "": rootShow(
        { fqid: "scb", name: "Statistiska Centralbyrån" },
        { fqid: "fk", name: "Försäkringskassan" },
      ),
    });
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/individer-15plus",
      period: 2020,
      bindings: [],
    } as unknown as ProjectSource;
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
    stubCatalog({
      scb: new Error("offline"),
      "scb/lisa": new Error("offline"),
    });
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/individer-15plus",
      period: 2020,
      bindings: [],
    } as unknown as ProjectSource;
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
    stubCatalog({ "scb/lisa": new Error("offline") });
    const source = {
      // Not named "LISA": the register's word must be absent from the whole card.
      name: "s1",
      register_variant: "scb/lisa/individer-15plus",
      period: 2020,
      bindings: [],
    } as unknown as ProjectSource;
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
    } as unknown as ProjectSource;
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
    const source = storedProject()?.sources?.[0] as ProjectSource;
    await renderCard(source);

    await page.getByRole("button", { name: "Remove source" }).click();

    const dialog = page.getByRole("alertdialog");
    await expect
      .element(dialog)
      .toMatchTextContent(
        /Remove the source LISA \(LISA, Individer 15\+\) and its 2 columns\?/,
      );
    // Nothing has gone yet — the question is the whole effect of the first click.
    expect(storedProject()?.sources).toHaveLength(2);

    await dialog.getByRole("button", { name: "Cancel" }).click();
    expect(storedProject()?.sources).toHaveLength(2);
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
    const source = storedProject()?.sources?.[0] as ProjectSource;
    await renderCard(source);

    await page.getByRole("button", { name: "Remove source" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect.element(dialog).toMatchTextContent(/and its 1 column\?/);
    await dialog.getByRole("button", { name: "Remove source" }).click();

    // The store dropped source 0 (scb/lisa/v1); scb/rtb/v1 survives.
    expect(storedProject()?.sources?.map((s) => s.register_variant)).toEqual([
      "scb/rtb/v1",
    ]);
  });

  it("renders an alert (not a crash) when bindings is a non-array", async () => {
    const source = {
      name: "bad",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: "oops",
    } as unknown as ProjectSource;
    await renderCard(source);

    const alert = page.getByRole("alert");
    await expect.element(alert).toBeVisible();
    await expect.element(alert).toMatchTextContent(/bindings\s+are malformed/);
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
    stubCatalog({
      scb: providerShow("scb", registerChild("scb/lisa", longRegisterName)),
    });
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
    } as ProjectSource;
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
    } as ProjectSource;
    await renderCard(source, {
      issues: [
        {
          level: "error" as const,
          code: "fqid_unresolved",
          path: "/sources/0/bindings/0/variable",
          message: "nope",
          successor_fqid: null,
        },
      ],
    });

    await expect.element(page.getByText("1 error")).toBeVisible();
  });
});
