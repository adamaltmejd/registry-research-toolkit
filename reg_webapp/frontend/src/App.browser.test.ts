import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import App from "./App.svelte";
import type { CatalogStats, Context, RootResponse } from "./lib/api";
import {
  getCatalogRoot,
  getContext,
  getStats,
  validateProject,
} from "./lib/api";
import type { ProjectData } from "./lib/project_data";
import { projectStore, setPersistence } from "./lib/project_store.svelte";
import { router } from "./lib/router.svelte";

vi.mock("./lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./lib/api")>();
  return {
    ...actual,
    getCatalogRoot: vi.fn(),
    getContext: vi.fn(),
    getStats: vi.fn(),
    validateProject: vi.fn(),
  };
});

const context: Context = {
  catalog_drift_warnings: [],
  reg_meta: {
    import_date: "2026-07-14T00:00:00Z",
    schema_version: "5.2.0",
  },
  steward: {
    id: "global",
    name: "Global",
    long_name: "Register Research Catalog",
    catalog_period_span: null,
  },
  webapp: { reg_meta_version: "1.0.0", version: "1.0.0" },
};

beforeEach(() => {
  router.navigate("/");
  vi.mocked(getContext).mockReset();
  vi.mocked(getCatalogRoot).mockReset();
  vi.mocked(getStats).mockReset();
  vi.mocked(getContext).mockResolvedValue(context);
  vi.mocked(getCatalogRoot).mockResolvedValue({
    kind: "root",
    children: [],
  } as unknown as RootResponse);
  vi.mocked(getStats).mockResolvedValue({
    providers: 1,
    registers: 2,
    variables: 3,
  } satisfies CatalogStats);
  vi.mocked(validateProject).mockReset();
  vi.mocked(validateProject).mockResolvedValue({ ok: true, issues: [] });
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

describe("App viewport geometry", () => {
  it("shares a short canvas between routed content and the citation footer without document overflow", async () => {
    const { container } = await render(App);

    await expect
      .element(page.getByText(/as of reg_meta v1\.0\.0/))
      .toBeVisible();
    const shell = container.querySelector<HTMLElement>(".shell");
    const routed = container.querySelector<HTMLElement>(".routed");
    const footer = container.querySelector<HTMLElement>(".vintage");
    expect(shell).not.toBeNull();
    expect(routed).not.toBeNull();
    expect(footer).not.toBeNull();
    expect(routed?.getBoundingClientRect().bottom).toBeLessThanOrEqual(
      footer?.getBoundingClientRect().top ?? 0,
    );
    expect(shell?.getBoundingClientRect().height).toBeCloseTo(
      document.documentElement.clientHeight,
      0,
    );
    expect(document.documentElement.scrollHeight).toBeLessThanOrEqual(
      document.documentElement.clientHeight,
    );
  });
});

describe("App owns the draft lifecycle", () => {
  it("autosaves and validates a draft authored away from /project, once", async () => {
    // The lifecycle belongs to the app root, not to /project: a draft authored
    // from a catalog page is restored, autosaved and validated on every route —
    // and visiting /project afterwards must not start a SECOND instance that
    // saves and validates the same edit twice.
    const saves: string[] = [];
    setPersistence({
      save: (_key: string, draft: ProjectData) => {
        saves.push(String(draft.name));
        return Promise.resolve();
      },
      load: () => Promise.resolve(null),
    });

    router.navigate("/catalog");
    await render(App);
    await projectStore.restored;

    projectStore.newProject({
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
    });
    projectStore.updateField("name", "picked-from-the-catalog");

    await vi.waitFor(
      () => {
        expect(saves).toEqual(["picked-from-the-catalog"]);
      },
      { timeout: 3000 },
    );
    expect(validateProject).toHaveBeenCalledTimes(1);

    router.navigate("/project");
    await expect
      .element(page.getByRole("heading", { name: /picked-from-the-catalog/ }))
      .toBeVisible();

    projectStore.updateField("name", "renamed-on-project");
    await vi.waitFor(
      () => {
        expect(saves).toEqual([
          "picked-from-the-catalog",
          "renamed-on-project",
        ]);
      },
      { timeout: 3000 },
    );
    expect(validateProject).toHaveBeenCalledTimes(2);
  });
});
