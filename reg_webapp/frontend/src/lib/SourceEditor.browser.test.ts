import { beforeEach, describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { Source } from "./project_data";
import { projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";

// #991/#993: SourceEditor is the READ-ONLY cart source card — it DISPLAYS the
// source coordinate/period + its bindings, and offers delete only. No name /
// register_variant inputs, no variant picker, no PeriodEditor.

beforeEach(() => {
  // projectStore is a module singleton; start each test from a fresh draft so the
  // stable-id mirror (sourceId/bindingId) resolves for index 0.
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

describe("SourceEditor read-only cart card", () => {
  it("displays the register_variant + period read-only, with no inputs or pickers", async () => {
    const source = {
      name: "lisa_main",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    await render(SourceEditor, { sourceIndex: 0, source, issues: [] });

    // The coordinate + period are shown…
    await expect.element(page.getByText("scb/lisa/v1")).toBeVisible();
    await expect.element(page.getByText("2020")).toBeVisible();
    // …the binding's variable is shown…
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

  it("shows the LISA family label without hiding the concrete source variant", async () => {
    const source = {
      name: "lisa_old",
      register_variant: "scb/lisa/individer-16plus",
      period: { from: 1990, to: 2009 },
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    } as Source;
    await render(SourceEditor, { sourceIndex: 0, source, issues: [] });

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
    await render(SourceEditor, { sourceIndex: 0, source, issues: [] });

    await expect.element(page.getByText("(no period)")).toBeVisible();
  });

  it("removes the source through the store when 'Remove source' is clicked", async () => {
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
    await render(SourceEditor, { sourceIndex: 0, source, issues: [] });

    await page.getByRole("button", { name: "Remove source" }).click();

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
    await render(SourceEditor, { sourceIndex: 0, source, issues: [] });

    const alert = page.getByRole("alert");
    await expect.element(alert).toBeVisible();
    await expect.element(alert).toHaveTextContent(/bindings\s+are malformed/);
  });

  it("renders an alert (not a crash) when the source slot is null", async () => {
    // A `sources: [null, …]` slot: SourceEditor must degrade to a malformed card
    // rather than deref `source.<field>` and throw (defense in depth for the render
    // boundary — ProjectEditor passes the raw slot straight in).
    await render(SourceEditor, {
      sourceIndex: 0,
      source: null as unknown as Source,
      issues: [],
    });

    const alert = page.getByRole("alert");
    await expect.element(alert).toBeVisible();
    await expect.element(alert).toHaveTextContent(/source entry is malformed/);
    // Still removable — the degraded card keeps its Remove affordance.
    await expect
      .element(page.getByRole("button", { name: /Remove source/ }))
      .toBeVisible();
  });

  it("renders the empty state (no alert) for a well-formed source with no bindings", async () => {
    const source = {
      name: "ok",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [],
    } as Source;
    await render(SourceEditor, { sourceIndex: 0, source, issues: [] });

    await expect.element(page.getByText("No bindings yet.")).toBeVisible();
    expect(page.getByRole("alert").query()).toBeNull();
  });

  it("wraps a long source name and binding FQID without horizontal overflow on mobile (#1110)", async () => {
    // Regression for PR #1109's visual-gate finding: at 375px a long unbroken source
    // name (`.source-head h3`) and a long unbroken binding FQID (`.variable-value`,
    // a flex item of `.binding-body`) formerly refused to shrink (flex items default
    // to `min-width: auto`) and clipped/overflowed the card. `min-width: 0` +
    // `overflow-wrap: anywhere` at those flex boundaries must let both wrap in-card.
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
    const view = await render(SourceEditor, {
      sourceIndex: 0,
      source,
      issues: [],
    });

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
    //
    // The heading text lives on `h3`; assert it is present, then check the heading row
    // (`.source-head`) and the whole card (`.source`) do not overflow.
    const heading = document.querySelector<HTMLElement>(".source-head h3");
    expect(heading?.textContent).toContain(
      "a_very_long_source_name_that_would_not_normally_wrap_on_its_own",
    );
    const sourceHead = document.querySelector<HTMLElement>(".source-head");
    expect(sourceHead?.scrollWidth ?? 0).toBeLessThanOrEqual(
      (sourceHead?.clientWidth ?? 0) + 1,
    );

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
    await render(SourceEditor, {
      sourceIndex: 0,
      source,
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
