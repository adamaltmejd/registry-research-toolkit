import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import App from "./App.svelte";
import type { CatalogStats, Context, RootResponse } from "./lib/api";
import { getCatalogRoot, getContext, getStats } from "./lib/api";
import { router } from "./lib/router.svelte";

vi.mock("./lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./lib/api")>();
  return {
    ...actual,
    getCatalogRoot: vi.fn(),
    getContext: vi.fn(),
    getStats: vi.fn(),
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
