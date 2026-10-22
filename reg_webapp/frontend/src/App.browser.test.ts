import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { render } from "vitest-browser-svelte";
import App from "./App.svelte";
import type { Context } from "./lib/api";
import { getContext, getShow, validateProject } from "./lib/api";
import { registerShow } from "./lib/api-test-helpers";
import { delivery, variableChild } from "./lib/catalog-node-view-test-helpers";
import type { RawDraft } from "./lib/project_data";
import { projectStore, setPersistence } from "./lib/project_store.svelte";
import { router } from "./lib/router.svelte";

vi.mock("./lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./lib/api")>();
  return {
    ...actual,
    getShow: vi.fn(),
    getContext: vi.fn(),
    validateProject: vi.fn(),
    // The register page's side reads, answered empty.
    getWarnings: vi.fn().mockResolvedValue([]),
    getRelatedDocuments: vi.fn().mockResolvedValue([]),
  };
});

const context: Context = {
  import_date: "2026-07-14T00:00:00Z",
  schema_version: "9.3.0",
  period_span: null,
  reg_meta_version: "1.0.0",
  sizes: { providers: 1, registers: 2, variables: 3 },
  steward: {
    id: "global",
    name: "Global",
    long_name: "Register Research Catalog",
  },
};

beforeEach(() => {
  router.navigate("/");
  vi.mocked(getContext).mockReset();
  vi.mocked(getShow).mockReset();
  vi.mocked(getContext).mockResolvedValue(context);
  vi.mocked(getShow).mockResolvedValue({
    kind: "root",
    catalog: {
      id: "global",
      reg_meta_version: "0.0.0",
      project_schema_version: "3.0.0",
    },
    children: [],
  });
  vi.mocked(validateProject).mockReset();
  vi.mocked(validateProject).mockResolvedValue({ ok: true, issues: [] });
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

describe("App renders the landing page from one context read", () => {
  it("shows Home's catalog sizes and the footer from the context", async () => {
    // Fails if Home stops reading the sizes App threads from `context`, or the
    // footer reads a field the Rust `context` shape does not carry.
    await render(App);

    await expect
      .element(page.getByText("1 providers · 2 registers · 3 variables"))
      .toBeVisible();
    await expect
      .element(
        page.getByText(
          "as of reg_meta v1.0.0 · schema 9.3.0 · built 2026-07-14",
        ),
      )
      .toBeVisible();
    expect(getContext).toHaveBeenCalledTimes(1);
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
      save: (_key: string, draft: RawDraft) => {
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

describe("App keeps the routed page across a query-only navigation", () => {
  it("leaves focus where it was when only `?period` changes (Y-65)", async () => {
    // Fails if a same-path, new-query navigation hands the page a new route
    // (or refetches the query-independent node): the article around the
    // control remounts and keyboard focus drops to the document body mid-Apply.
    vi.mocked(getShow).mockResolvedValue(
      registerShow({
        children: [
          variableChild("scb/lisa/kon", "Kon", [
            delivery("_default", "Kon", ["1990-01-01", null]),
          ]),
        ],
      }),
    );
    router.navigate("/catalog/scb/lisa");
    await render(App);
    const link = page.getByRole("link", { name: "Kon" }).first();
    await expect.element(link).toBeVisible();
    const focused = link.element() as HTMLElement;
    focused.focus();

    router.navigate("/catalog/scb/lisa?period=2019");
    await expect.poll(() => router.getQueryParam("period")).toBe("2019");
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(focused.isConnected).toBe(true);
    expect(document.activeElement).toBe(focused);
    expect(getShow).toHaveBeenCalledTimes(1);
  });
});
