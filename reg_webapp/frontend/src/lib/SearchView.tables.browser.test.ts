import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { search } from "./api";
import SearchView from "./SearchView.svelte";
import { mockSearch, type Pages, setQuery } from "./search-view-test-helpers";

// Split from SearchView.browser.test.ts by contract surface:
// compact per-type tables. Siblings: SearchView.*.browser.test.ts.

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

describe("SearchView — compact per-type tables (#808)", () => {
  // The #808 round-3 redesign: registers / variables / classifications render ONE
  // CSS-grid `.children.table` over their results IN RANK ORDER — leaves and
  // concept groups are whole-row subgrid <a>s, while classification succession is
  // the only inline disclosure. Headings stay plain text; the raw FQID is hidden.
  // Codes render a compact, code-FIRST grid table per code-system bucket.
  const FOUR_GROUPS: Pages = {
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
        variables: [
          { fqid: "scb/saga/sex", name: "Sex", register_name: "SAGA" },
        ],
        variable_count: 1,
        classifications: [],
        classification_count: 0,
      },
    ],
  };

  it("omits the variable definition when it exactly repeats the variable name", async () => {
    mockSearch({
      variable: [
        {
          type: "variable",
          fqid: "scb/lisa/raks-andelutbbidrink",
          name: "Andel av den totala inkomsten som är föranledd av arbetsmarknadspolitiska åtgärder",
          register_name: "LISA",
          definition:
            "Andel av den totala inkomsten som är föranledd av arbetsmarknadspolitiska åtgärder",
          operational_definition: null,
          delivery_column_names: ["Raks_AndelUtbBidrInk"],
        },
      ],
    });
    setQuery("raks");
    await render(SearchView);

    // Poll until the async search results have rendered before the sync queries.
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    expect(
      page
        .getByText(
          "Andel av den totala inkomsten som är föranledd av arbetsmarknadspolitiska åtgärder",
        )
        .elements(),
    ).toHaveLength(1);
  });

  it("makes a variable leaf primary link focusable and register context separately navigable (#808 a11y)", async () => {
    // Linked metadata pills mean the row can no longer be one nested anchor. Assert
    // the primary variable name is still a real keyboard-focusable catalog link and
    // the register context is its own link.
    mockSearch(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // Poll until the async search results have rendered before the sync queries.
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    const link = page.getByRole("link", { name: /^Kön/ });
    await expect.element(link).toHaveAttribute("href", "/catalog/scb/lisa/kon");
    (link.element() as HTMLElement).focus();
    expect(document.activeElement).toBe(link.element());
    await expect
      .element(page.getByRole("link", { name: /^SCB: LISA/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
  });

  it("renders a null-fqid variable leaf as a non-link row (plain text, no navigation target)", async () => {
    // A null-fqid leaf can't navigate: it renders as a non-link <div.leaf-row>
    // (no <a>), its name is plain text. The row still renders (no crash).
    mockSearch({
      variable: [
        {
          type: "variable",
          fqid: null,
          name: "Orphan",
          register_name: "LISA",
          definition: null,
          delivery_column_names: [],
        },
      ],
    });
    setQuery("orphan");
    await render(SearchView);

    await expect.element(page.getByText("Orphan")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /Orphan/ }))
      .not.toBeInTheDocument();
    // The row is a non-link <div>, not an <a> (no navigation target on a null fqid).
    expect(
      document.querySelector(".search-view .cols-1 a.leaf-row"),
    ).toBeNull();
    expect(
      document.querySelector(".search-view .cols-1 div.leaf-row"),
    ).not.toBeNull();
  });

  it("interleaves a concept-group link inline in rank order (no 'Grouped families' block)", async () => {
    // #808 round 3: a group result sits inline at its rank position among the leaf
    // rows — it is NOT pulled out into a separate "Grouped families" sub-block and
    // it is NOT a disclosure. Assert a leaf row, a group link, and another leaf row
    // all render in the SAME grid table, and that the old label is gone.
    mockSearch({
      variable: [
        {
          type: "variable",
          fqid: "scb/lisa/before",
          name: "Before fold",
          register_name: "LISA",
          definition: null,
          delivery_column_names: [],
        },
        {
          type: "group",
          key: "dispink",
          label: "Disponibel inkomst",
          kind: "variable",
          matched_count: 2,
          register_name: "LISA",
          members: [
            {
              fqid: "scb/lisa/dispink-2019",
              name: "Disp 2019",
            },
          ],
        },
        {
          type: "variable",
          fqid: "scb/lisa/after",
          name: "After fold",
          register_name: "LISA",
          definition: null,
          delivery_column_names: [],
        },
      ],
    });
    setQuery("ink");
    await render(SearchView);

    // Both leaf rows AND the group row render.
    await expect
      .element(page.getByRole("link", { name: /Before fold/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/before");
    await expect
      .element(page.getByRole("link", { name: /After fold/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/after");
    expect(
      document.querySelector(
        ".search-view .group-result-row a.row-link[href='/catalog/group/scb/lisa/dispink']",
      ),
    ).not.toBeNull();
    const grid = document.querySelector(".search-view .cols-1");
    const groupLink = grid?.querySelector(
      ".group-result-row a.row-link[href='/catalog/group/scb/lisa/dispink']",
    );
    expect(
      groupLink,
      "group link renders inline in the variables grid",
    ).not.toBeNull();
    expect(
      grid?.querySelector("details.concept-group"),
      "variable concept groups do not render as details",
    ).toBeNull();
    await expect
      .element(page.getByText(/variables matched/))
      .not.toBeInTheDocument();
    // The old "Grouped families" pulled-out block is GONE.
    expect(document.querySelector(".search-view .folds-label")).toBeNull();
    await expect
      .element(page.getByText("Grouped families"))
      .not.toBeInTheDocument();
  });

  it("falls back to direct member links when a group page is not derivable", async () => {
    mockSearch({
      variable: [
        {
          type: "group",
          key: "mixed-income",
          label: "Disponibel inkomst",
          kind: "variable",
          matched_count: 2,
          register_name: "LISA",
          members: [
            {
              fqid: "scb/lisa/dispink-2019",
              name: "Disp LISA",
            },
            {
              fqid: "scb/iot/dispink-2019",
              name: "Disp IoT",
            },
          ],
        },
      ],
    });
    setQuery("ink");
    await render(SearchView);

    // No single register-scoped group page can be derived, so members are emitted
    // as direct leaf links. They are visible without expanding anything.
    await expect
      .element(page.getByRole("link", { name: /Disp LISA/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/dispink-2019");
    await expect
      .element(page.getByRole("link", { name: /Disp IoT/ }))
      .toHaveAttribute("href", "/catalog/scb/iot/dispink-2019");
    expect(
      document.querySelector(".search-view details.concept-group"),
    ).toBeNull();
  });

  it("renders a single-owner code as a compact whole-row link to the variable (#808 round 5)", async () => {
    // #808 round 5: each code-system bucket is a compact code-FIRST table — one row
    // per code, with code and label paired as `code = label`. A single variable
    // owner makes the whole code row link to that variable and renders the owner as
    // muted inline context, not as an expandable row. The classification owner is
    // represented by the linked bucket heading.
    mockSearch({
      classification_code: [
        {
          type: "code",
          code: "A10",
          label: "Diabetes drugs",
          variables: [
            { fqid: "scb/lmed/atc", name: "ATC-kod", register_name: "LMED" },
          ],
          variable_count: 1,
          classifications: [
            { fqid: "class/atc", short_name: "ATC code list", name: null },
          ],
          classification_count: 1,
          code_system: "ATC",
        },
      ],
    });
    setQuery("man");
    await render(SearchView);

    // The bucket heading names and links the classification/value-set the codes
    // come from.
    await expect
      .element(page.getByRole("link", { name: "ATC", exact: true }))
      .toHaveAttribute("href", "/catalog/class/atc");
    // One code row, with the code and label rendered as one paired expression.
    const codeCell = document.querySelector(".search-view .code-cell");
    expect(codeCell?.textContent?.trim()).toBe("A10");
    expect(
      document
        .querySelector(".search-view .code-expression")
        ?.textContent?.trim(),
    ).toBe("A10 = Diabetes drugs");
    await expect
      .element(page.getByText("1 classification"))
      .not.toBeInTheDocument();
    const row = document.querySelector<HTMLAnchorElement>(
      ".search-view a.single-code-row[href='/catalog/scb/lmed/atc']",
    );
    expect(row).not.toBeNull();
    expect(row?.getAttribute("href")).toBe("/catalog/scb/lmed/atc");
    expect(row?.querySelector(".code-owner-single")?.textContent).toContain(
      "ATC-kod",
    );
    expect(row?.querySelector(".code-owner-single")?.textContent).toContain(
      "LMED",
    );
    await expect
      .element(page.getByRole("link", { name: /ATC-kod/ }))
      .toHaveAttribute("href", "/catalog/scb/lmed/atc");
    expect(document.querySelector(".search-view details.code-row")).toBeNull();
  });

  // Fix A keys the disclosure each-blocks by CONTENT identity + index (`code|i` for
  // codes; `key|i` / `fqid|i` for the variables-/classifications-grid folds),
  // not the bare index. A bare-index key makes Svelte REUSE a <details> element for
  // whatever NEW row lands at that position on a reactive list update, carrying its
  // `open` state over (a freshly-fetched row renders expanded though the user never
  // opened it). NOTE: the end-to-end "expand then refine the query" leak is NOT
  // observable through this view, because `asyncResource` flips `loading=true` +
  // `data=null` on every refetch, so the whole results `{#each}` is torn down and
  // rebuilt between queries (fresh closed <details> regardless of key) — verified by
  // a negative-control probe. So these guard the OBSERVABLE half of the fix: the
  // index component keeps the key UNIQUE under content collisions (duplicate `code`
  // in a bucket; the same register-scoped `key` recurring across registers),
  // which a content-ONLY key (`code` / `key` alone) would crash on with
  // Svelte's each_key_duplicate — the same lesson the leaf groups already encode.

  it("renders DUPLICATE codes in one bucket without an each_key_duplicate crash (Fix A keeps the index in the key)", async () => {
    // The same `code` value recurs within one code-system bucket (distinct labels /
    // owners), so a `code`-ONLY key would collide and crash the whole render. The
    // `code|index` key tolerates it — both disclosure rows must render.
    mockSearch({
      classification_code: [
        {
          type: "code",
          code: "1",
          label: "First meaning",
          variables: [{ fqid: "scb/a/x", name: "X", register_name: "A" }],
          variable_count: 1,
          classifications: [],
          classification_count: 0,
          code_system: "SUN2020",
        },
        {
          type: "code",
          code: "1",
          label: "Second meaning",
          variables: [{ fqid: "scb/b/y", name: "Y", register_name: "B" }],
          variable_count: 1,
          classifications: [],
          classification_count: 0,
          code_system: "SUN2020",
        },
      ],
    });
    setQuery("dup");
    await render(SearchView);

    // Both duplicate-code rows render (no crash / stuck "Searching…").
    await expect.element(page.getByText("First meaning")).toBeVisible();
    await expect.element(page.getByText("Second meaning")).toBeVisible();
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
    expect(document.querySelectorAll(".search-view .code-row").length).toBe(2);
  });
});
