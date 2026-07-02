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
