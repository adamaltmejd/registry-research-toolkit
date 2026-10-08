import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { SearchPage } from "./api";
import { search } from "./api";
import SearchView from "./SearchView.svelte";
import { mockSearch, pillLabel, setQuery } from "./search-view-test-helpers";

// Split from SearchView.browser.test.ts by contract surface:
// code rows and code-system groups. Siblings: SearchView.*.browser.test.ts.

// Stub the search GET the view drives; keep the rest of api.ts real (the type
// exports). SearchView reads `?q=` off the `router` singleton, so each case sets
// the URL (and re-syncs the singleton's reactive `search`) before rendering.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    search: vi.fn(),
  };
});

beforeEach(() => {
  vi.mocked(search).mockReset();
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

function nextFrame(): Promise<void> {
  return new Promise((resolve) => requestAnimationFrame(() => resolve()));
}

describe("SearchView — typed result groups (#379)", () => {
  it("expands a multi-variable code to the full variable-owner link list (#808 round 5)", async () => {
    mockSearch({
      register_value: [
        {
          type: "code",
          code: "1",
          label: "Man",
          variables: [
            { fqid: "scb/lisa/kon", name: "Kön", register_name: "LISA" },
            {
              fqid: "scb/lisa/civil",
              name: "Civilstånd",
              register_name: "LISA",
            },
            { fqid: "scb/rams/age", name: "Ålder", register_name: "RAMS" },
          ],
          variable_count: 3,
          classifications: [],
          classification_count: 0,
        },
      ],
    });
    // ≥ 2 chars so the min-length guard fetches.
    setQuery("11");
    await render(SearchView);

    // The collapsed row shows the MUTED variable-count summary…
    await expect.element(page.getByText("3 variables")).toBeVisible();
    // …and the owner sub-rows are NOT rendered until expanded (the disclosure is
    // collapsed by default, so the owner link is absent).
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .not.toBeInTheDocument();

    // Expand the disclosure (the <summary> carries the code/label).
    await page.getByText("Man").click();

    // The owning variable is now a navigable link (its register shown muted).
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    await expect
      .element(page.getByRole("link", { name: /Civilstånd/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/civil");
    await expect
      .element(page.getByRole("link", { name: /Ålder/ }))
      .toHaveAttribute("href", "/catalog/scb/rams/age");
    await expect.element(page.getByText(/\+\d+ more/)).not.toBeInTheDocument();
  });

  it("renders an OWNERLESS code as a plain Code · Label row — no count, no disclosure (#808 round 5)", async () => {
    // The common classification value-set code (e.g. an ATC code) has NO owner
    // variables AND no owner classifications, so it shows NO usage count and is
    // NOT a disclosure — just a clean Code · Label row.
    mockSearch({
      classification_code: [
        {
          type: "code",
          code: "1",
          label: "Primary education",
          variables: [],
          variable_count: 0,
          classifications: [],
          classification_count: 0,
          code_system: "SUN2020",
        },
      ],
    });
    setQuery("sun");
    await render(SearchView);

    // The bucket heading names the classification (SUN2020) the codes come from.
    await expect
      .element(page.getByRole("heading", { name: "SUN2020" }))
      .toBeVisible();
    // The compact Code · Label row renders…
    await expect.element(page.getByText("Primary education")).toBeVisible();
    // …with no count (zero owners) and no disclosure (<details>/<summary>).
    await expect
      .element(page.getByText(/variable|classification/))
      .not.toBeInTheDocument();
    expect(document.querySelector(".search-view .usage-count")).toBeNull();
    expect(
      document.querySelector(".search-view details.code-disclosure"),
    ).toBeNull();
  });

  it("links a classification-backed code-system heading and keeps classification owners out of row matches (#808 round 5)", async () => {
    // A code carrying BOTH variable + classification owners: the bucket heading is
    // the classification link, while the row summarizes and expands variable
    // matches only.
    mockSearch({
      classification_code: [
        {
          type: "code",
          code: "1",
          label: "Man",
          variables: [
            { fqid: "scb/lisa/kon", name: "Kön", register_name: "LISA" },
            { fqid: "scb/rams/kon", name: "Kön RAMS", register_name: "RAMS" },
          ],
          variable_count: 2,
          classifications: [
            { fqid: "class/sun2020", short_name: "SUN2020", name: null },
          ],
          classification_count: 1,
          code_system: "SUN2020",
        },
      ],
    });
    // ≥ 2 chars so the min-length guard fetches.
    setQuery("11");
    await render(SearchView);

    await expect
      .element(page.getByRole("link", { name: "SUN2020" }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    // The collapsed row summarizes variable owners only; no owner is rendered yet.
    await expect.element(page.getByText("2 variables")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "Kön", exact: true }))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByText("1 classification"))
      .not.toBeInTheDocument();

    // Expand: variable owners become navigable sub-rows; the classification owner
    // is not repeated inside the row because the heading is already linked.
    await page.getByText("Man").click();
    await expect
      .element(page.getByRole("link", { name: /Kön.*SCB: LISA/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    await expect
      .element(page.getByRole("link", { name: /Kön RAMS.*SCB: RAMS/ }))
      .toHaveAttribute("href", "/catalog/scb/rams/kon");
    expect(
      document.querySelector(
        ".search-view .owner-row[href='/catalog/class/sun2020']",
      ),
    ).toBeNull();
    expect(document.querySelector(".search-view .owner-row .tag")).toBeNull();
  });

  it("keeps secondary classification owners visible for reused code rows", async () => {
    mockSearch({
      classification_code: [
        {
          type: "code",
          code: "1",
          label: "Shared code",
          variables: [
            { fqid: "scb/lisa/kon", name: "Kön", register_name: "LISA" },
            { fqid: "scb/rams/kon", name: "Kön RAMS", register_name: "RAMS" },
          ],
          variable_count: 2,
          classifications: [
            { fqid: "class/sun2020", short_name: "SUN2020", name: null },
            { fqid: "class/sun2000", short_name: "SUN2000", name: null },
          ],
          classification_count: 2,
          code_system: "SUN2020",
        },
      ],
    });
    setQuery("11");
    await render(SearchView);

    await expect
      .element(page.getByText("2 variables | 2 classifications"))
      .toBeVisible();
    await page.getByText("Shared code").click();
    await expect
      .element(page.getByRole("link", { name: "SUN2000" }))
      .toHaveAttribute("href", "/catalog/class/sun2000");
    expect(
      document.querySelector(
        ".search-view .owner-row[href='/catalog/class/sun2020']",
      ),
    ).toBeNull();
  });
});

describe("SearchView — codes grouped by code system (#393 item 3)", () => {
  it("renders classification code-system subsections and local value sets as separate groups", async () => {
    // Two SUN2020 codes and one register-local value. The server now splits
    // classification-owned codes from register-local value sets into separate
    // top-level groups, so the view only nests classification code-system buckets.
    mockSearch({
      classification_code: [
        {
          type: "code",
          code: "1",
          label: "Man",
          variables: [],
          variable_count: 0,
          classifications: [
            { fqid: "class/sun2020", short_name: "SUN2020", name: null },
          ],
          classification_count: 1,
          code_system: "SUN2020",
        },
        {
          type: "code",
          code: "2",
          label: "Woman",
          variables: [],
          variable_count: 0,
          classifications: [
            { fqid: "class/sun2020", short_name: "SUN2020", name: null },
          ],
          classification_count: 1,
          code_system: "SUN2020",
        },
      ],
      register_value: [
        {
          type: "code",
          code: "9",
          label: "Local",
          variables: [
            { fqid: "scb/lisa/kon", name: "Kön", register_name: "LISA" },
          ],
          variable_count: 1,
          classifications: [],
          classification_count: 0,
          code_system: null,
        },
      ],
    });
    setQuery("code");
    await render(SearchView);

    // The classification group gets a linked code-system subsection heading.
    await expect
      .element(page.getByRole("heading", { name: /SUN2020/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /SUN2020/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    const systemPill = document.querySelector<HTMLAnchorElement>(
      ".code-system-heading .code-system-chip[href='/catalog/class/sun2020']",
    );
    expect(systemPill).not.toBeNull();
    expect(pillLabel(systemPill)).toBe("SUN2020");
    // Register-local values render in their own bubble, not as a nested
    // "Register-local" subsection under classification codes.
    await expect
      .element(page.getByRole("heading", { name: "Register-local value sets" }))
      .toBeVisible();
    expect(
      Array.from(document.querySelectorAll(".code-label")).some(
        (label) => label.textContent?.trim() === "Local",
      ),
    ).toBe(true);
    const headings = Array.from(
      document.querySelectorAll<HTMLElement>(
        ".code-system-heading .code-system-chip",
      ),
    ).map((h) => pillLabel(h));
    expect(headings).toEqual(["SUN2020"]);
  });

  it("communicates bounded continuation and appends the arm's next page", async () => {
    // Fails if a continuation loses its arm, its cursor or its page size (the
    // Rust default is 50), or replaces the first page instead of appending.
    vi.mocked(search).mockImplementation(async (_q, options) => {
      if (options?.type !== "classification_code") {
        return { items: [], next_cursor: null };
      }
      return options.cursor === "opaque-page-2"
        ? {
            items: [
              {
                type: "code",
                code: "2",
                label: "Kvinna",
                variables: [],
                variable_count: 0,
                classifications: [],
                classification_count: 0,
                code_system: null,
              },
            ],
            next_cursor: null,
          }
        : {
            items: [
              {
                type: "code",
                code: "1",
                label: "Man",
                variables: [],
                variable_count: 0,
                classifications: [
                  { fqid: "class/sun2020", short_name: "SUN2020", name: null },
                ],
                classification_count: 1,
                code_system: "SUN2020",
              },
            ],
            next_cursor: "opaque-page-2",
          };
    });
    setQuery("code");
    await render(SearchView);

    await expect.element(page.getByText("1+ results")).toBeVisible();
    await page.getByRole("button", { name: "Load more" }).click();
    await expect.element(page.getByText("2 results")).toBeVisible();
    await expect.element(page.getByText("Kvinna")).toBeVisible();
    expect(search).toHaveBeenLastCalledWith("code", {
      cursor: "opaque-page-2",
      limit: 3,
      type: "classification_code",
    });
  });

  it("ignores a continuation response after the search context changes", async () => {
    let resolveContinuation!: (page: SearchPage) => void;
    const continuation = new Promise<SearchPage>((resolve) => {
      resolveContinuation = resolve;
    });
    vi.mocked(search).mockImplementation(async (q, options) => {
      if (options?.type !== "register") {
        return { items: [], next_cursor: null };
      }
      if (options.cursor !== undefined) {
        return continuation;
      }
      return q === "old"
        ? {
            items: [
              {
                type: "register",
                fqid: "scb/old",
                name: "Old first",
                purpose: null,
              },
            ],
            next_cursor: "old-page-2",
          }
        : {
            items: [
              {
                type: "register",
                fqid: "scb/new",
                name: "New result",
                purpose: null,
              },
            ],
            next_cursor: null,
          };
    });
    setQuery("old");
    await render(SearchView);
    await page.getByRole("button", { name: "Load more" }).click();

    setQuery("new");
    await expect.element(page.getByText("New result")).toBeVisible();
    resolveContinuation({
      items: [
        {
          type: "register",
          fqid: "scb/stale",
          name: "Stale second",
          purpose: null,
        },
      ],
      next_cursor: null,
    });
    await nextFrame();

    await expect.element(page.getByText("New result")).toBeVisible();
    expect(page.getByText("Stale second").query()).toBeNull();
  });
});
