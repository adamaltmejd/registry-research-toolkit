import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { SearchResponse } from "./api";
import { search } from "./api";
import { router } from "./router.svelte";
import SearchView from "./SearchView.svelte";
import { setQuery } from "./search-view-test-helpers";

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

describe("SearchView — typed result groups (#379)", () => {
  it("renders the four groups in order, each with its hits", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [
        {
          group: "registers",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        {
          group: "variables",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "variable",
              fqid: "scb/lisa/kon",
              name: "Kön",
              register: "LISA",
              definition: null,
              delivery_column_names: ["kon"],
            },
          ],
        },
        {
          group: "classifications",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "classification",
              fqid: "class/sun2020",
              short_name: "SUN",
              name: "Svensk utbildningsnomenklatur",
            },
          ],
        },
        {
          group: "register_value_sets",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              // A DISTINCT owner name from the variable leaf above so the
              // link-by-name assertions below stay unambiguous.
              variables: [
                { fqid: "scb/saga/sex", name: "Sex", register: "SAGA" },
              ],
              variable_count: 1,
              classifications: [],
              classification_count: 0,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("kon");
    await render(SearchView);

    for (const heading of [
      "Registers",
      "Variables",
      "Classifications",
      "Register-local value sets",
    ]) {
      await expect
        .element(page.getByRole("heading", { name: heading }))
        .toBeVisible();
    }
    // A register/variable/classification leaf links to its catalog node. The
    // whole-row link accessible names include muted context, so match the
    // distinctive visible names plus context where needed.
    await expect
      .element(page.getByRole("link", { name: /LISA.*SCB/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
  });

  it("renders the top-results group before the typed groups", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [
        {
          group: "top_results",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "variable",
              fqid: "scb/lisa/kon",
              name: "Kön",
              register: "LISA",
              definition: null,
              delivery_column_names: ["Kon"],
            },
            {
              type: "register",
              fqid: "scb/lisa",
              name: "LISA",
              purpose: null,
            },
          ],
        },
        {
          group: "registers",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("kon");
    await render(SearchView);

    // Poll until the async search results have rendered before the sync queries.
    await expect
      .element(page.getByRole("heading", { name: "Top results" }))
      .toBeVisible();
    const headings = Array.from(document.querySelectorAll("h2")).map((h) =>
      h.textContent?.trim(),
    );
    expect(headings.filter(Boolean).slice(-2)).toEqual([
      "Top results",
      "Registers",
    ]);
    await expect
      .element(page.getByRole("heading", { name: "Top results" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /Kön/ }).first())
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
  });

  it("shows code-system context on code hits in top results", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "C12",
      groups: [
        {
          group: "top_results",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "code",
              code: "C12",
              label: "Malign tumör i tungbas",
              variables: [
                {
                  fqid: "scb/ulf/ha0611m",
                  name: "Sjukdomsdiagnos 1, ICD-10",
                  register: "ULF",
                },
                {
                  fqid: "scb/ulf/ha0612m",
                  name: "Sjukdomsdiagnos 2, ICD-10",
                  register: "ULF",
                },
              ],
              variable_count: 2,
              classifications: [
                {
                  fqid: "class/icd-10-se",
                  short_name: "ICD-10-SE",
                  name: "Internationell statistisk klassifikation av sjukdomar och relaterade hälsoproblem, svensk version",
                },
              ],
              classification_count: 1,
              code_system: "ICD-10-SE",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("C12");
    await render(SearchView);

    await expect.element(page.getByText("C12")).toBeVisible();
    await expect.element(page.getByText("Code system")).not.toBeInTheDocument();
    await expect
      .element(page.getByText("showing 1 of 25"))
      .not.toBeInTheDocument();
    // The code system is a link to its classification.
    await expect
      .element(page.getByRole("link", { name: /^ICD-10-SE/ }))
      .toHaveAttribute("href", "/catalog/class/icd-10-se");
    // Expanding the code lists its owners: the variables and the classification.
    await page.getByText("Malign tumör i tungbas").click();
    await expect
      .element(page.getByRole("link", { name: /Sjukdomsdiagnos 1/ }))
      .toHaveAttribute("href", "/catalog/scb/ulf/ha0611m");
    expect(
      page
        .getByRole("link")
        .elements()
        .filter((a) => a.getAttribute("href") === "/catalog/class/icd-10-se"),
    ).toHaveLength(2);
  });

  it("links single-owner code hits in top results", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "man",
      groups: [
        {
          group: "top_results",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
              ],
              variable_count: 1,
              classifications: [
                { fqid: "class/sun2020", short_name: "SUN2020", name: null },
              ],
              classification_count: 1,
              code_system: "SUN2020",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("man");
    await render(SearchView);

    // Poll until the async search results have rendered before the sync queries.
    await expect
      .element(page.getByRole("heading", { name: "Top results" }))
      .toBeVisible();
    await expect.element(page.getByText("1 = Man")).toBeVisible();
    await expect.element(page.getByText("Code system")).not.toBeInTheDocument();
    expect(
      page
        .getByRole("link")
        .elements()
        .map((a) => a.getAttribute("href")),
    ).toEqual(
      expect.arrayContaining([
        "/catalog/scb/lisa/kon",
        "/catalog/class/sun2020",
      ]),
    );
    await expect
      .element(page.getByRole("link", { name: /^SUN2020/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("shows delivery column names and operational definitions on variable hits", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "fedunsatreason",
      groups: [
        {
          group: "variables",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "variable",
              fqid: "scb/aes/formal-utbildning",
              name: "Orsak till missnöje, formell utbildning",
              register: "AES",
              definition: "Orsak till missnöje",
              operational_definition: "Formal education dissatisfaction reason",
              delivery_column_names: ["fedunsatreason_1", "fedunsatreason_2"],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("fedunsatreason");
    await render(SearchView);

    await expect.element(page.getByText("AES")).toBeVisible();
    await expect.element(page.getByText("fedunsatreason_1")).toBeVisible();
    await expect.element(page.getByText("fedunsatreason_2")).toBeVisible();
    await expect
      .element(page.getByText("Formal education dissatisfaction reason"))
      .toBeVisible();
  });

  it("folds duplicate variable hits into one row with merged delivery column chips", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "fedunsatreason",
      groups: [
        {
          group: "variables",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "variable",
              fqid: "scb/aes/formal-utbildning",
              name: "Orsak till missnöje, formell utbildning",
              register: "AES",
              definition: null,
              operational_definition: null,
              delivery_column_names: ["fedunsatreason_1"],
            },
            {
              type: "variable",
              fqid: "scb/aes/formal-utbildning",
              name: "Orsak till missnöje, formell utbildning",
              register: "AES",
              definition: null,
              operational_definition: null,
              delivery_column_names: ["fedunsatreason_2"],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("fedunsatreason");
    await render(SearchView);

    // Poll until the async search results have rendered before the sync queries.
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    const links = document.querySelectorAll<HTMLAnchorElement>(
      ".search-view a.row-link[href='/catalog/scb/aes/formal-utbildning']",
    );
    expect(links).toHaveLength(1);
    const columnPills = Array.from(
      document.querySelectorAll<HTMLElement>(".search-view .col-chip"),
    ).map((pill) => pill.textContent?.trim());
    expect(columnPills).toEqual(["fedunsatreason_1", "fedunsatreason_2"]);
  });

  it("keeps the matched delivery column visible before the +N overflow", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "target variable",
      groups: [
        {
          group: "variables",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "variable",
              fqid: "scb/aes/formal-utbildning",
              name: "Orsak till missnöje, formell utbildning",
              register: "AES",
              definition: null,
              delivery_column_names: [
                "alpha_1",
                "bravo_1",
                "charlie_1",
                "target_1",
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("target variable");
    await render(SearchView);

    await expect.element(page.getByText("target_1")).toBeVisible();
    await expect.element(page.getByText("+1")).toBeVisible();
    await expect.element(page.getByText("charlie_1")).not.toBeInTheDocument();
  });

  it("closes back to the route that entered search using replaceState", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [],
    } as unknown as SearchResponse);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/catalog/scb/lisa");
    router.navigate("/search?q=kon");
    await render(SearchView);

    const historyLength = window.history.length;
    await page.getByRole("button", { name: "Close search" }).click();

    await expect.poll(() => router.route.name).toBe("catalog-node");
    await expect.poll(() => window.location.pathname).toBe("/catalog/scb/lisa");
    expect(window.history.length).toBe(historyLength);
  });

  it("omits a group whose results are empty (no empty header)", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [
        {
          group: "registers",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        { group: "variables", has_more: false, next_cursor: null, results: [] },
        {
          group: "classifications",
          has_more: false,
          next_cursor: null,
          results: [],
        },
        {
          group: "classification_codes",
          has_more: false,
          next_cursor: null,
          results: [],
        },
        {
          group: "register_value_sets",
          has_more: false,
          next_cursor: null,
          results: [],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .not.toBeInTheDocument();
  });

  it("uses an exact rendered count when the bounded page is complete", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "lisa",
      groups: [
        {
          group: "registers",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("lisa");
    await render(SearchView);

    // The group + its hit render…
    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect.element(page.getByText("1 result")).toBeVisible();
  });

  it("deduplicates variable leaf hits that are already represented by a group-page result", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "disp",
      groups: [
        {
          group: "variables",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "group",
              group_key: "dispink",
              group_label: "Disponibel inkomst",
              kind: "variable",
              label_matched: false,
              matched_count: 1,
              member_count: 1,
              register: "LISA",
              source: "token",
              members: [
                {
                  fqid: "scb/lisa/dispink-2019",
                  name: "Disp 2019",
                  facets: [],
                },
              ],
            },
            {
              type: "variable",
              fqid: "scb/lisa/dispink-2019",
              name: "Disp 2019",
              register: "LISA",
              definition: null,
              delivery_column_names: ["dispink"],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("disp");
    await render(SearchView);

    // Poll until the async search results have rendered before the sync queries.
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    expect(
      document.querySelector(
        ".search-view .group-result-row a.row-link[href='/catalog/group/scb/lisa/dispink']",
      ),
    ).not.toBeNull();
    await expect
      .element(page.getByRole("link", { name: /Disp 2019/ }))
      .not.toBeInTheDocument();
    expect(
      document.querySelector(
        ".search-view a.leaf-row[href='/catalog/scb/lisa/dispink-2019']",
      ),
    ).toBeNull();
  });
});
