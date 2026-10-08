import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { search } from "./api";
import { router } from "./router.svelte";
import SearchView from "./SearchView.svelte";
import { mockSearch, setQuery } from "./search-view-test-helpers";

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
    mockSearch({
      register: [
        { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
      ],
      variable: [
        {
          type: "variable",
          fqid: "scb/lisa/kon",
          name: "Kön",
          register_name: "LISA",
          definition: null,
          delivery_column_names: ["kon"],
        },
      ],
      classification: [
        {
          type: "classification",
          fqid: "class/sun2020",
          short_name: "SUN",
          name: "Svensk utbildningsnomenklatur",
        },
      ],
      register_value: [
        {
          type: "code",
          code: "1",
          label: "Man",
          // A DISTINCT owner name from the variable leaf above so the
          // link-by-name assertions below stay unambiguous.
          variables: [
            { fqid: "scb/saga/sex", name: "Sex", register_name: "SAGA" },
          ],
          variable_count: 1,
          classifications: [],
          classification_count: 0,
        },
      ],
    });
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
    mockSearch({
      top: [
        {
          type: "variable",
          fqid: "scb/lisa/kon",
          name: "Kön",
          register_name: "LISA",
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
      register: [
        { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
      ],
    });
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
      .element(page.getByRole("link", { name: /Kön/ }).first())
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
  });

  it("shows code-system context on code hits in top results", async () => {
    mockSearch({
      top: [
        // A second hit: the strip renders only when it ranks more than one.
        { type: "register", fqid: "scb/other", name: "Other", purpose: null },
        {
          type: "code",
          code: "C12",
          label: "Malign tumör i tungbas",
          variables: [
            {
              fqid: "scb/ulf/ha0611m",
              name: "Sjukdomsdiagnos 1, ICD-10",
              register_name: "ULF",
            },
            {
              fqid: "scb/ulf/ha0612m",
              name: "Sjukdomsdiagnos 2, ICD-10",
              register_name: "ULF",
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
    });
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
    mockSearch({
      top: [
        // A second hit: the strip renders only when it ranks more than one.
        { type: "register", fqid: "scb/other", name: "Other", purpose: null },
        {
          type: "code",
          code: "1",
          label: "Man",
          variables: [
            { fqid: "scb/lisa/kon", name: "Kön", register_name: "LISA" },
          ],
          variable_count: 1,
          classifications: [
            { fqid: "class/sun2020", short_name: "SUN2020", name: null },
          ],
          classification_count: 1,
          code_system: "SUN2020",
        },
      ],
    });
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
    mockSearch({
      variable: [
        {
          type: "variable",
          fqid: "scb/aes/formal-utbildning",
          name: "Orsak till missnöje, formell utbildning",
          register_name: "AES",
          definition: "Orsak till missnöje",
          operational_definition: "Formal education dissatisfaction reason",
          delivery_column_names: ["fedunsatreason_1", "fedunsatreason_2"],
        },
      ],
    });
    setQuery("fedunsatreason");
    await render(SearchView);

    await expect.element(page.getByText("AES")).toBeVisible();
    await expect.element(page.getByText("fedunsatreason_1")).toBeVisible();
    await expect.element(page.getByText("fedunsatreason_2")).toBeVisible();
    await expect
      .element(page.getByText("Formal education dissatisfaction reason"))
      .toBeVisible();
  });

  it("keeps the matched delivery column visible before the +N overflow", async () => {
    mockSearch({
      variable: [
        {
          type: "variable",
          fqid: "scb/aes/formal-utbildning",
          name: "Orsak till missnöje, formell utbildning",
          register_name: "AES",
          definition: null,
          delivery_column_names: [
            "alpha_1",
            "bravo_1",
            "charlie_1",
            "target_1",
          ],
        },
      ],
    });
    setQuery("target variable");
    await render(SearchView);

    await expect.element(page.getByText("target_1")).toBeVisible();
    await expect.element(page.getByText("+1")).toBeVisible();
    await expect.element(page.getByText("charlie_1")).not.toBeInTheDocument();
  });

  it("closes back to the route that entered search using replaceState", async () => {
    mockSearch({});
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

  it("omits an empty section, and the top-results strip with a lone hit", async () => {
    // Fails if an empty arm renders a header, or the strip repeats the only hit
    // its arm section already shows.
    mockSearch({
      top: [
        { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
      ],
      register: [
        { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
      ],
      variable: [],
      classification: [],
      classification_code: [],
      register_value: [],
    });
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByRole("heading", { name: "Top results" }))
      .not.toBeInTheDocument();
  });

  it("uses an exact rendered count when the bounded page is complete", async () => {
    mockSearch({
      register: [
        { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
      ],
    });
    setQuery("lisa");
    await render(SearchView);

    // The group + its hit render…
    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect.element(page.getByText("1 result")).toBeVisible();
  });
});
