import type { ComponentProps } from "svelte";
import { beforeEach, describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";

// #991/#993: SourceEditor is the READ-ONLY cart source card — it DISPLAYS the
// register it delivers from, its coordinate/period/name and its columns, and offers
// delete only. No name / register_variant inputs, no variant picker, no
// PeriodEditor. Y-75: the card is titled by its REGISTER (the thing the researcher
// picked), the columns are called columns, and dropping a whole source asks first.

beforeEach(() => {
  // projectStore is a module singleton; start each test from a fresh draft so the
  // stable-id mirror (sourceId/bindingId) resolves for index 0.
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
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
    ...overrides,
  });
}

describe("SourceEditor read-only cart card", () => {
  it("displays the register_variant + period read-only, with no inputs or pickers", async () => {
    const source = {
      name: "lisa_main",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    await renderCard(source);

    // The coordinate + period are shown…
    await expect.element(page.getByText("scb/lisa/v1")).toBeVisible();
    await expect.element(page.getByText("2020")).toBeVisible();
    // …the column's variable is shown…
    await expect.element(page.getByText("scb/lisa/kon")).toBeVisible();

    // …and there are NO editing affordances: no textboxes, no "Pick variant".
    expect(page.getByRole("textbox").query()).toBeNull();
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
  // `OrderEntry.source` join on it.
  it("titles the card with its register, keeping the generated name as a detail", async () => {
    const source = {
      name: "LISA_2",
      register_variant: "scb/lisa/individer-15plus",
      period: 2020,
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    await renderCard(source);

    await expect
      .element(page.getByRole("heading", { name: "LISA", exact: true }))
      .toBeVisible();
    // The generated name is not the title, and it has not left the card either.
    await expect.element(page.getByText("Source name")).toBeVisible();
    await expect
      .element(page.getByText("LISA_2", { exact: true }))
      .toBeVisible();
  });

  it("qualifies the register title with its provider where the deployment has more than one", async () => {
    const source = {
      name: "LISA",
      register_variant: "scb/lisa/individer-15plus",
      period: 2020,
      bindings: [],
    } as unknown as Source;
    await renderCard(source, { providerQualified: true });

    await expect
      .element(page.getByRole("heading", { name: "SCB LISA", exact: true }))
      .toBeVisible();
  });

  it("shows the LISA family label without hiding the concrete source variant", async () => {
    const source = {
      name: "lisa_old",
      register_variant: "scb/lisa/individer-16plus",
      period: { from: 1990, to: 2009 },
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    await renderCard(source);

    await expect
      .element(page.getByText("Individer (scb/lisa/individer-16plus)"))
      .toBeVisible();
  });

  it("shows the '(no period)' fallback for a null period", async () => {
    const source = {
      name: "s",
      register_variant: "scb/lisa/v1",
      period: null,
      bindings: [],
    } as unknown as Source;
    await renderCard(source);

    await expect.element(page.getByText("(no period)")).toBeVisible();
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
      .toMatchTextContent(/Remove LISA and its 2 columns\?/);
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
    await expect
      .element(dialog)
      .toMatchTextContent(/Remove LISA and its 1 column\?/);
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
    // there now.
    const source = {
      name: "a_very_long_source_name_that_would_not_normally_wrap_on_its_own",
      register_variant:
        "scb/a_very_long_register_slug_that_would_not_wrap_either/v1",
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
    const heading = document.querySelector<HTMLElement>(".source-head h3");
    expect(heading?.textContent).toContain(
      "A_VERY_LONG_REGISTER_SLUG_THAT_WOULD_NOT_WRAP_EITHER",
    );
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
