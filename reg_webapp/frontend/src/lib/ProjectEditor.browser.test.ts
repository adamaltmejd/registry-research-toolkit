import { beforeEach, describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import ProjectEditor from "./ProjectEditor.svelte";
import { projectStore } from "./project_store.svelte";

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
