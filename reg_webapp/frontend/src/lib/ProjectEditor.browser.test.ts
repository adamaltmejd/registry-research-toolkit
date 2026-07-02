import { beforeEach, describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import ProjectEditor from "./ProjectEditor.svelte";
import { projectStore } from "./project_store.svelte";

// #991/#993: /project is a READ-ONLY data-order CART. The page shows the picked
// sources/bindings read-only, supports delete + project-name edit + Open/Download +
// Validate, and links out to the catalog for fixes. There is NO "Add source" /
// "Add binding" / field-editing affordance — adding data happens in the catalog
// browser.

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

/** Seed a draft of `n` sources by register variant, each a single categorical
 * binding — the cart's read-only content (adds funnel through the staged-diff
 * commit path, since the editor no longer mutates directly). */
function seedSources(registerVariants: string[]): void {
  projectStore.newProject(SEED);
  projectStore.applyStagedDiff({
    adds: registerVariants.map((rv, i) => ({
      registerVariant: rv,
      period: 2000 + i,
      binding: { variable: `${rv}/var`, type: "categorical" },
    })),
  });
}

beforeEach(() => {
  // Reset the singleton home/new state before each test.
  projectStore.newProject(SEED);
});

describe("ProjectEditor cart — read-only, no add affordances", () => {
  it("renders picked sources read-only with no Add source / Add binding buttons", async () => {
    seedSources(["scb/lisa/v1", "scb/rtb/v1"]);
    await render(ProjectEditor, { regMetaVersion: "1.0.0", steward: "global" });

    // The two sources' coordinates show read-only.
    await expect
      .element(page.getByRole("heading", { name: "Sources (2)" }))
      .toBeVisible();
    // Exact — the register_variant coordinate is a substring of the binding's
    // variable FQID (`scb/lisa/v1/var`), so a loose match would double-hit.
    await expect
      .element(page.getByText("scb/lisa/v1", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("scb/rtb/v1", { exact: true }))
      .toBeVisible();

    // No "Add source" / "Add binding" — data is added in the catalog browser.
    expect(page.getByRole("button", { name: "Add source" }).query()).toBeNull();
    expect(
      page.getByRole("button", { name: "Add binding" }).query(),
    ).toBeNull();
  });

  it("shows the browse-to-add empty state when there are no sources", async () => {
    projectStore.newProject(SEED);
    await render(ProjectEditor, { regMetaVersion: "1.0.0", steward: "global" });

    await expect
      .element(page.getByText(/Browse the catalog to add data/))
      .toBeVisible();
  });

  it("keeps the manual Validate + name edit + Download toolbar", async () => {
    seedSources(["scb/lisa/v1"]);
    await render(ProjectEditor, { regMetaVersion: "1.0.0", steward: "global" });

    // The manual Validate button (#994 auto-validate is out of scope) and the
    // downloads are present.
    await expect
      .element(page.getByRole("button", { name: "Validate" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("button", { name: "Download project_data.json" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("button", { name: "Download order CSV" }))
      .toBeVisible();

    // The one editable field — the project name — writes through updateField.
    const nameInput = page.getByRole("textbox", { name: "Name" });
    await nameInput.fill("My study");
    expect(projectStore.draft?.name).toBe("My study");
  });

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
    await expect
      .element(page.getByText(/Browse the catalog to add data/))
      .toBeVisible();

    // The malformed value is preserved verbatim on the draft (serialize/validate
    // still see it — the SPA is not the structural validator).
    expect(projectStore.draft?.sources as unknown).toBe("not-an-array");
  });
});

// Issue #200: the each-blocks key on a store-owned STABLE id, so removing a MIDDLE
// source remounts the correct component instances instead of rebinding a survivor's
// per-instance UI state to a shifted item. In the read-only cart there is no
// per-instance edit state left to snap, but the keying invariant still governs which
// survivor each card shows — so a middle-remove leaves the survivors' OWN content.
describe("ProjectEditor stable keys (middle-remove keeps the right survivors)", () => {
  it("removing the MIDDLE source leaves the outer survivors showing their OWN coordinate", async () => {
    seedSources(["scb/lisa/v1", "scb/rtb/v1", "scb/uht/v1"]);
    await render(ProjectEditor, { regMetaVersion: "1.0.0", steward: "global" });

    // Sanity: three source cards.
    expect(
      page.getByRole("button", { name: "Remove source" }).elements(),
    ).toHaveLength(3);

    // Remove the MIDDLE source (scb/rtb/v1) via its own "Remove source" button.
    await page.getByRole("button", { name: "Remove source" }).nth(1).click();

    // The store dropped the middle source; the two survivors keep their coordinates.
    expect(projectStore.draft?.sources?.map((s) => s.register_variant)).toEqual(
      ["scb/lisa/v1", "scb/uht/v1"],
    );
    await expect
      .element(page.getByText("scb/lisa/v1", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("scb/uht/v1", { exact: true }))
      .toBeVisible();
    expect(page.getByText("scb/rtb/v1", { exact: true }).query()).toBeNull();
  });
});

describe("ProjectEditor renders the ValidationPanel", () => {
  it("shows the not-yet-validated hint (the panel is present)", async () => {
    seedSources(["scb/lisa/v1"]);
    await render(ProjectEditor, { regMetaVersion: "1.0.0", steward: "global" });

    await expect.element(page.getByText(/Not yet validated/)).toBeVisible();
  });
});
