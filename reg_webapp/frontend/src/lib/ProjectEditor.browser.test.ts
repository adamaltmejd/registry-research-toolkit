import { beforeEach, describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import ProjectEditor from "./ProjectEditor.svelte";
import { projectStore } from "./project_store.svelte";

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global" as const,
};

// A Model-A-versioned but structurally malformed spec (non-array `sources`). The
// version gate accepts it (schema_version 2.x + reg_meta/v1.x), so it loads — and
// the editor must render rather than crash (the backend diagnoses the structure).
const MALFORMED = JSON.stringify({
  schema_version: "2.0.0",
  steward: "global",
  reg_meta_version: "reg_meta/v1.0.0",
  name: "Malformed",
  sources: "not-an-array",
});

beforeEach(() => {
  // Reset the singleton home/new state before each test.
  projectStore.newProject({
    reg_meta_version: "reg_meta/v1.0.0",
    steward: "global",
  });
});

// Scenario 4 (#201): the top-level guard coerces a non-array `sources` to [] for
// rendering while keeping the draft verbatim.
describe("ProjectEditor malformed-sources guard", () => {
  it("coerces a non-array sources to empty and renders without crashing", async () => {
    await projectStore.openFromFile(new File([MALFORMED], "project_data.json"));
    await render(ProjectEditor, { regMetaVersion: "1.0.0", steward: "global" });

    // The loaded draft renders (name heading) — no crash on the non-array.
    await expect
      .element(page.getByRole("heading", { name: /Malformed/ }))
      .toBeVisible();
    // Coerced to [] for the summary → (0) + the empty state.
    await expect
      .element(page.getByRole("heading", { name: "Sources (0)" }))
      .toBeVisible();
    await expect.element(page.getByText(/No sources yet/)).toBeVisible();

    // The malformed value is preserved verbatim on the draft (serialize/validate
    // still see it — the SPA is not the structural validator).
    expect(projectStore.draft?.sources as unknown).toBe("not-an-array");
  });
});

// Issue #200: the each-blocks key on a store-owned STABLE id, so removing a MIDDLE
// source/binding remounts the correct component instances instead of rebinding a
// survivor's per-instance UI state to a shifted item (the #188 snap-back).
describe("ProjectEditor stable keys (middle-remove keeps survivor state on the right item)", () => {
  /** Seed a draft of `n` sources, each with a distinct single-year period. */
  function seedSources(years: number[]): void {
    projectStore.newProject(SEED);
    years.forEach((y, i) => {
      projectStore.addSource();
      projectStore.updateSource(i, { name: `src-${y}`, period: y });
    });
  }

  it("removing the MIDDLE source leaves survivors showing their OWN period", async () => {
    // Three sources with distinct year periods → three PeriodEditors, each seeded
    // (one-time at mount) to its own year. An index key would rebind the surviving
    // instances on a middle remove and snap the wrong year in.
    seedSources([2001, 2002, 2003]);
    await render(ProjectEditor, { regMetaVersion: "1.0.0", steward: "global" });

    // Sanity: the three From spinners show 2001 / 2002 / 2003.
    const fromBefore = page
      .getByRole("spinbutton", { name: "From" })
      .elements();
    expect(fromBefore.map((el) => (el as HTMLInputElement).value)).toEqual([
      "2001",
      "2002",
      "2003",
    ]);

    // Remove the MIDDLE source (2002) via its own "Remove source" button.
    await page.getByRole("button", { name: "Remove source" }).nth(1).click();

    // The two survivors must still show THEIR OWN years (2001, 2003) — NOT 2002
    // snapped onto a rebound instance (the #188 symptom). The store also confirms
    // the draft itself dropped 2002.
    expect(projectStore.draft?.sources?.map((s) => s.period)).toEqual([
      2001, 2003,
    ]);
    await expect
      .element(page.getByRole("spinbutton", { name: "From" }).nth(0))
      .toHaveValue(2001);
    await expect
      .element(page.getByRole("spinbutton", { name: "From" }).nth(1))
      .toHaveValue(2003);
  });

  it("an unrelated edit after switching a survivor's period mode does NOT snap it back (#188 regression)", async () => {
    // The #188 snap-back: switch a period to Token, then edit something else; the
    // removed re-seed workaround used to reset the mode back to Years. With the
    // one-time mount seed + stable keys, the mode must persist.
    seedSources([2001, 2002]);
    await render(ProjectEditor, { regMetaVersion: "1.0.0", steward: "global" });

    // Switch the SECOND source's period to Token mode (local UI state only).
    await page.getByRole("radio", { name: "Token" }).nth(1).click();
    await expect
      .element(page.getByRole("radio", { name: "Token" }).nth(1))
      .toBeChecked();

    // An unrelated edit (rename the project) re-renders the whole list; the second
    // SourceEditor survives on its stable key, so its PeriodEditor must NOT re-seed.
    projectStore.updateField("name", "renamed-while-token");
    await expect
      .element(page.getByRole("radio", { name: "Token" }).nth(1))
      .toBeChecked();
  });
});
