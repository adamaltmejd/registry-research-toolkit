import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { DocSearchResponse, SearchResponse } from "./api";
import { docSearch, search } from "./api";
import { catalogHref } from "./catalog";
import { router } from "./router.svelte";
import SearchView from "./SearchView.svelte";

// Stub the two GETs the view drives (`search` + the independent `docSearch`, #394);
// keep the rest of api.ts real (the type exports). SearchView reads `?q=` off the
// `router` singleton, so each case sets the URL (and re-syncs the singleton's
// reactive `search`) before rendering. `docSearch` is mocked in EVERY test so it
// never hits a real fetch — defaulted to a no-hit response in `beforeEach`.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    search: vi.fn(),
    docSearch: vi.fn(),
  };
});

// A docs response with no hits — the default for the #379 suite, so its existing
// assertions are unaffected by the additive docs group.
const NO_DOC_HITS: DocSearchResponse = {
  kind: "doc-search",
  query: "",
  ingested: true,
  total_count: 0,
  results: [],
};

function setQuery(q: string): void {
  // Reset to a SENTINEL URL distinct from the target first so `navigate` isn't a
  // no-op (its guard compares the full URL), then route to the target — this
  // re-syncs the singleton's reactive route/search regardless of where a prior
  // test left it. (A bare `pushState` to the same path would no-op the navigate
  // and leak the prior test's `?q=`.)
  window.history.pushState({}, "", "/__reset__");
  router.navigate(`/search?q=${encodeURIComponent(q)}`);
}

beforeEach(() => {
  vi.mocked(search).mockReset();
  // Default the docs resource to a no-hit response so the docs group is omitted
  // and the #379 suite's assertions stand unchanged. Tests that exercise the docs
  // group override this.
  vi.mocked(docSearch).mockReset();
  vi.mocked(docSearch).mockResolvedValue(NO_DOC_HITS);
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
          total_count: 1,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        {
          group: "variables",
          total_count: 1,
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
          total_count: 1,
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
          group: "codes",
          total_count: 1,
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
      "Codes / values",
    ]) {
      await expect
        .element(page.getByRole("heading", { name: heading }))
        .toBeVisible();
    }
    // A register/variable/classification leaf links to its catalog node. The
    // register leaf is the registers whole-row name link (exact "LISA"); the
    // variable leaf is the whole-row grid link whose accessible name folds in the
    // muted register/detail line and column chips, so match it by its /Kön/ name.
    await expect
      .element(page.getByRole("link", { name: "LISA", exact: true }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
  });

  it("shows delivery column names and operational definitions on variable hits", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "fedunsatreason",
      groups: [
        {
          group: "variables",
          total_count: 1,
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

    expect(document.querySelector(".search-view .register-pill")).toBeNull();
    await expect.element(page.getByText("AES")).toBeVisible();
    const columnPills = Array.from(
      document.querySelectorAll<HTMLElement>(".search-view .col-chip"),
    ).map((pill) => pill.textContent?.trim());
    expect(columnPills).toEqual(["fedunsatreason_1", "fedunsatreason_2"]);
    await expect
      .element(
        page.getByText("Operational: Formal education dissatisfaction reason"),
      )
      .toBeVisible();
  });

  it("keeps the matched delivery column visible before the +N overflow", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "target variable",
      groups: [
        {
          group: "variables",
          total_count: 1,
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

    await page.getByRole("button", { name: "Close search" }).click();

    await expect.poll(() => router.route.name).toBe("catalog-node");
    await expect.poll(() => window.location.pathname).toBe("/catalog/scb/lisa");
  });

  it("omits a group whose results are empty (no empty header)", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [
        {
          group: "registers",
          total_count: 1,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        { group: "variables", total_count: 0, results: [] },
        { group: "classifications", total_count: 0, results: [] },
        { group: "codes", total_count: 0, results: [] },
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

  it("shows 'showing N of M' only when the slice is truncated", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ab",
      groups: [
        {
          group: "registers",
          total_count: 42,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
      ],
    } as unknown as SearchResponse);
    // ≥ 2 chars so the min-length guard fetches (a 1-char query short-circuits to
    // the keep-typing hint).
    setQuery("ab");
    await render(SearchView);

    await expect.element(page.getByText("showing 1 of 42")).toBeVisible();
  });

  it("omits the 'showing N of M' caption when the slice is complete", async () => {
    // Guards the strict `shown < total` boundary in `showingOf` — a `<=`
    // regression would print "showing 1 of 1" on every complete group.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "lisa",
      groups: [
        {
          group: "registers",
          total_count: 1,
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
    // …but no truncation caption (shown === total).
    await expect.element(page.getByText(/showing/)).not.toBeInTheDocument();
  });

  it("expands a multi-variable code to the full variable-owner link list (#808 round 5)", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "11",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
                {
                  fqid: "scb/lisa/civil",
                  name: "Civilstånd",
                  register: "LISA",
                },
                { fqid: "scb/rams/age", name: "Ålder", register: "RAMS" },
              ],
              variable_count: 3,
              classifications: [],
              classification_count: 0,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
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

  it("makes an expanded owner-row link keyboard-focusable (#808 a11y)", async () => {
    // Fix 2 (#808 a11y): the owner sub-rows are whole-row FLEX `<a>`s (NOT
    // display:contents), so — unlike the leaf rows — they ARE in the keyboard tab
    // order and a `:focus-visible` ring draws on the anchor's own box. The visible
    // ring is a pure CSS hook (verified via screenshot in the dev-shot tool); here we
    // guard the prerequisite: an expanded owner row is a real, focusable <a>.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "11",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
                { fqid: "scb/rams/kon", name: "Kön RAMS", register: "RAMS" },
              ],
              variable_count: 2,
              classifications: [],
              classification_count: 0,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("11");
    await render(SearchView);

    // Expand the disclosure to reveal the owner sub-rows.
    await page.getByText("Man").click();
    const owner = document.querySelector<HTMLAnchorElement>(
      ".search-view a.owner-row",
    );
    expect(owner).not.toBeNull();
    owner?.focus();
    expect(document.activeElement).toBe(owner);
  });

  it("renders an OWNERLESS code as a plain Code · Label row — no count, no disclosure (#808 round 5)", async () => {
    // The common classification value-set code (e.g. an ATC code) has NO owner
    // variables AND no owner classifications, so it shows NO usage count and is
    // NOT a disclosure — just a clean Code · Label row.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
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
        },
      ],
    } as unknown as SearchResponse);
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
    expect(
      document.querySelector(".search-view .usage-count")?.textContent?.trim(),
    ).toBe("");
    expect(
      document.querySelector(".search-view details.code-disclosure"),
    ).toBeNull();
  });

  it("links a classification-backed code-system heading and keeps classification owners out of row matches (#808 round 5)", async () => {
    // A code carrying BOTH variable + classification owners: the bucket heading is
    // the classification link, while the row summarizes and expands variable
    // matches only.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "11",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
                { fqid: "scb/rams/kon", name: "Kön RAMS", register: "RAMS" },
              ],
              variable_count: 2,
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
      .element(page.getByRole("link", { name: /Kön LISA/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    await expect
      .element(page.getByRole("link", { name: /Kön RAMS/ }))
      .toHaveAttribute("href", "/catalog/scb/rams/kon");
    expect(
      document.querySelector(
        ".search-view .owner-row[href='/catalog/class/sun2020']",
      ),
    ).toBeNull();
    expect(document.querySelector(".search-view .owner-row .tag")).toBeNull();
  });

  it("keeps secondary classification owners visible for reused code rows", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "11",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Shared code",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
                { fqid: "scb/rams/kon", name: "Kön RAMS", register: "RAMS" },
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
        },
      ],
    } as unknown as SearchResponse);
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

  it("links a concept-group result to its group page", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ink",
      groups: [
        {
          group: "variables",
          total_count: 1,
          results: [
            {
              type: "group",
              group_key: "dispink",
              group_label: "Disponibel inkomst",
              kind: "variable",
              label_matched: false,
              matched_count: 2,
              member_count: 3,
              register: "LISA",
              source: "token",
              members: [
                {
                  fqid: "scb/lisa/dispink-2019",
                  name: "Disp 2019",
                  facets: [],
                },
                {
                  fqid: "scb/lisa/dispink-2020",
                  name: "Disp 2020",
                  facets: [],
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("ink");
    await render(SearchView);

    // The group itself is a normal row link to the first-class group page; no
    // inline disclosure is reintroduced in search results.
    await expect
      .element(page.getByRole("link", { name: /Group: Disponibel inkomst/ }))
      .toHaveAttribute("href", "/catalog/group/scb/lisa/dispink");
    await expect.element(page.getByText("LISA")).toBeVisible();
    await expect
      .element(page.getByText(/variables matched/))
      .not.toBeInTheDocument();
    expect(
      document.querySelector(".search-view details.concept-group"),
    ).toBeNull();
  });

  it("deduplicates variable leaf hits that are already represented by a group-page result", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "disp",
      groups: [
        {
          group: "variables",
          total_count: 2,
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

    await expect
      .element(page.getByRole("link", { name: /Group: Disponibel inkomst/ }))
      .toHaveAttribute("href", "/catalog/group/scb/lisa/dispink");
    await expect
      .element(page.getByRole("link", { name: /Disp 2019/ }))
      .not.toBeInTheDocument();
    expect(
      document.querySelector(
        ".search-view a.leaf-row[href='/catalog/scb/lisa/dispink-2019']",
      ),
    ).toBeNull();
  });

  it("renders a classification_succession result: terminal edition links + flat edition rows (#571)", async () => {
    // A folded succession row in the classifications group: the TERMINAL edition is
    // the navigable header link; the editions fold under a <details> disclosure
    // (collapsed) with the "matched M of N editions" hint, each edition linkable.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification_succession",
              fqid: "class/sun2020",
              short_name: "SUN",
              name: "SUN 2020",
              matched_count: 2,
              editions: [
                {
                  slug: "sun2020",
                  fqid: "class/sun2020",
                  name: "SUN 2020",
                  effective_year: 2020,
                },
                {
                  slug: "sun2000",
                  fqid: "class/sun2000",
                  name: "SUN 2000",
                  effective_year: 2000,
                },
                {
                  slug: "sun1996",
                  fqid: "class/sun1996",
                  name: "SUN 1996",
                  effective_year: 1996,
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun");
    await render(SearchView);

    // The terminal edition is the always-visible header link.
    await expect
      .element(page.getByRole("link", { name: "SUN matched 2 of 3 editions" }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    // The family hint reads "matched M of N editions"; the row is NOT a concept group.
    await expect
      .element(page.getByText("matched 2 of 3 editions"))
      .toBeVisible();
    // Older editions are normal visible rows, not hidden behind a disclosure.
    await expect
      .element(page.getByRole("link", { name: /SUN 1996/ }))
      .toHaveAttribute("href", "/catalog/class/sun1996");
  });

  it("routes a classification_succession with a null terminal fqid to plain text (no header link)", async () => {
    // A dead chain end (null terminal fqid): the succession guard still routes it
    // to the succession snippet (NOT the leaf), and the header is plain text.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification_succession",
              fqid: null,
              short_name: "SUN",
              name: "SUN (dead)",
              matched_count: 1,
              editions: [
                {
                  slug: "sun1996",
                  fqid: "class/sun1996",
                  name: "SUN 1996",
                  effective_year: 1996,
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun");
    await render(SearchView);

    // The succession snippet renders (its editions hint), so the guard routed it.
    await expect
      .element(page.getByText("matched 1 of 1 editions"))
      .toBeVisible();
    // The null-terminal header is plain text, not a link.
    await expect
      .element(page.getByRole("link", { name: "SUN (dead)" }))
      .not.toBeInTheDocument();
  });

  it("renders a 'current edition' link on a lone old-edition classification leaf (#571)", async () => {
    // A lone non-terminal edition hit (the chain didn't fold — only one edition
    // matched): the leaf carries `terminal_fqid`, so the snippet renders a compact
    // link to the current/terminal edition so the user can jump forward.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun1996",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification",
              fqid: "class/sun1996",
              short_name: "SUN1996",
              name: "Svensk utbildningsnomenklatur",
              terminal_fqid: "class/sun2020",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun1996");
    await render(SearchView);

    // The leaf itself links to its own (old) edition…
    await expect
      .element(page.getByRole("link", { name: /SUN1996/ }))
      .toHaveAttribute("href", "/catalog/class/sun1996");
    // …and the current-edition affordance links to the terminal edition.
    await expect
      .element(page.getByRole("link", { name: /current edition/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("keeps the 'current edition' link on a null-fqid classification leaf with a terminal_fqid (#808)", async () => {
    // Regression: the backend allows a malformed/unresolvable vintage (fqid: null)
    // that still carries a valid terminal_fqid. The terminal "→ current edition"
    // link is the row's ONLY navigable target, so it must still render; the name
    // degrades to plain text (no broken self-link).
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun1996",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification",
              fqid: null,
              short_name: "SUN1996",
              name: "Svensk utbildningsnomenklatur",
              terminal_fqid: "class/sun2020",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun1996");
    await render(SearchView);

    // The current-edition affordance still links to the terminal edition…
    await expect
      .element(page.getByRole("link", { name: /current edition/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    // …and the name is plain text, NOT a (broken) link.
    await expect.element(page.getByText("SUN1996")).toBeInTheDocument();
    await expect
      .element(page.getByRole("link", { name: /SUN1996/ }))
      .not.toBeInTheDocument();
  });

  it("omits the 'current edition' link on a classification leaf with no terminal_fqid (#571)", async () => {
    // A current edition (or a non-edition classification) carries no terminal_fqid,
    // so the forward-link affordance must NOT render.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun2020",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification",
              fqid: "class/sun2020",
              short_name: "SUN2020",
              name: "Svensk utbildningsnomenklatur",
              terminal_fqid: null,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun2020");
    await render(SearchView);

    // The leaf renders…
    await expect
      .element(page.getByRole("link", { name: /SUN2020/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    // …but no current-edition affordance.
    await expect
      .element(page.getByText(/current edition/))
      .not.toBeInTheDocument();
  });

  it("renders two concept groups sharing a group_key across registers (no each_key_duplicate crash)", async () => {
    // The `inkomst` production crash (#379 omnibox-dup-key): the variables group
    // returns TWO type:"group" results with the SAME group_key ("tfoab") from
    // DIFFERENT registers (IoT vs LINDA). Concept-group keys are register-scoped
    // unique (#322), so the same key legitimately recurs across registers. Keying
    // the each by group_key throws Svelte's each_key_duplicate at render time,
    // crashing the WHOLE results render — the omnibox stays on "Searching…"
    // forever. Index keys can't collide; this asserts both rows render.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "inkomst",
      groups: [
        {
          group: "variables",
          total_count: 2,
          results: [
            {
              type: "group",
              group_key: "tfoab",
              group_label: "Inkomst IoT",
              kind: "variable",
              label_matched: false,
              matched_count: 1,
              member_count: 1,
              register: "IoT",
              source: "token",
              members: [
                { fqid: "scb/iot/tfoab-2019", name: "IoT 2019", facets: [] },
              ],
            },
            {
              type: "group",
              group_key: "tfoab",
              group_label: "Inkomst LINDA",
              kind: "variable",
              label_matched: false,
              matched_count: 1,
              member_count: 1,
              register: "LINDA",
              source: "token",
              members: [
                {
                  fqid: "scb/linda/tfoab-2019",
                  name: "LINDA 2019",
                  facets: [],
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("inkomst");
    await render(SearchView);

    // The view RENDERS (didn't crash / stay on "Searching…"): the group label
    // and both register-distinct concept-group rows are visible.
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    await expect.element(page.getByText("Inkomst IoT")).toBeVisible();
    await expect.element(page.getByText("Inkomst LINDA")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /Inkomst IoT/ }))
      .toHaveAttribute("href", "/catalog/group/scb/iot/tfoab");
    await expect
      .element(page.getByRole("link", { name: /Inkomst LINDA/ }))
      .toHaveAttribute("href", "/catalog/group/scb/linda/tfoab");
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
  });

  it("renders two leaf hits sharing a null fqid and the same name (no each_key_duplicate crash)", async () => {
    // The leaf-collision twin of the concept-group case: a null `fqid` plus an
    // identical `name` made the old `(result.fqid ?? result.name)` key collide,
    // crashing the render. Index keys tolerate it — both rows must render.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "orphan",
      groups: [
        {
          group: "registers",
          total_count: 2,
          results: [
            { type: "register", fqid: null, name: "Orphan", purpose: "first" },
            { type: "register", fqid: null, name: "Orphan", purpose: "second" },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("orphan");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect.element(page.getByText("first")).toBeVisible();
    await expect.element(page.getByText("second")).toBeVisible();
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
  });

  it("renders a null-fqid hit as plain text (not a link)", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "orphan",
      groups: [
        {
          group: "registers",
          total_count: 1,
          results: [
            { type: "register", fqid: null, name: "Orphan", purpose: null },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("orphan");
    await render(SearchView);

    await expect.element(page.getByText("Orphan")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "Orphan" }))
      .not.toBeInTheDocument();
  });

  it("tolerates an UNKNOWN/future group by SKIPPING it (no crash) while rendering known groups", async () => {
    // The backend documents `group` as an extension point and requires the SPA to
    // tolerate unknown/future `group` values by SKIPPING them. An unknown group has
    // no GROUP_HEADINGS entry, so the heading lookup is `undefined`; dereferencing
    // `heading.tone` on it would throw and crash the WHOLE search page. The view must
    // render the known groups and silently omit the unknown one instead.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [
        {
          group: "registers",
          total_count: 1,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        {
          // A group literal the SPA has never heard of, carrying NON-EMPTY results.
          group: "future_widgets",
          total_count: 1,
          results: [
            { type: "future_widget", fqid: "scb/lisa/x", name: "Widget" },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("kon");
    await render(SearchView);

    // The known group renders (the page did NOT crash on the unknown group)…
    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "LISA", exact: true }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    // …and the unknown group is silently omitted (no heading, no row).
    await expect.element(page.getByText("Widget")).not.toBeInTheDocument();
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
  });

  it("shows the empty-query hint and never fetches", async () => {
    setQuery("");
    await render(SearchView);

    await expect
      .element(
        page.getByText(
          "Start typing to search registers, variables, codes, classifications.",
        ),
      )
      .toBeVisible();
    expect(search).not.toHaveBeenCalled();
  });

  it("shows a no-matches line when every group is empty for a non-empty query", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "zzz",
      groups: [
        { group: "registers", total_count: 0, results: [] },
        { group: "variables", total_count: 0, results: [] },
        { group: "classifications", total_count: 0, results: [] },
        { group: "codes", total_count: 0, results: [] },
      ],
    } as unknown as SearchResponse);
    setQuery("zzz");
    await render(SearchView);

    await expect.element(page.getByText("No matches for “zzz”.")).toBeVisible();
  });

  it("shows 'No matches' when the ONLY group is a non-empty UNKNOWN group (no blank body)", async () => {
    // A response carrying ONLY an unknown/future group with non-empty results: the
    // render loop SKIPS it (no GROUP_HEADINGS entry), so nothing renders — but
    // `noMatches` must still fire so the body isn't blank. The skipped group's
    // non-empty `results` must NOT count as a match.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "zzz",
      groups: [
        {
          group: "future_widgets",
          total_count: 1,
          results: [
            { type: "future_widget", fqid: "scb/lisa/x", name: "Widget" },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("zzz");
    await render(SearchView);

    // The unknown group renders nothing…
    await expect.element(page.getByText("Widget")).not.toBeInTheDocument();
    // …and the "No matches" message shows instead of a blank body.
    await expect.element(page.getByText("No matches for “zzz”.")).toBeVisible();
  });

  it("does NOT show 'No matches' when a known group renders a folded succession (guard for the skip exclusion)", async () => {
    // Guards Fix B's caveat: successions ride INSIDE the classifications group (a
    // `classification_succession` row, NOT a top-level group), so the classifications
    // group IS in GROUP_HEADINGS and is NOT skipped. The skip-exclusion in `noMatches`
    // must not wrongly mark a rendered group empty — "No matches" must stay hidden
    // while a succession is on screen.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification_succession",
              fqid: "class/sun2020",
              short_name: "SUN",
              name: "SUN 2020",
              matched_count: 2,
              editions: [
                {
                  slug: "sun2020",
                  fqid: "class/sun2020",
                  name: "SUN 2020",
                  effective_year: 2020,
                },
                {
                  slug: "sun2000",
                  fqid: "class/sun2000",
                  name: "SUN 2000",
                  effective_year: 2000,
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun");
    await render(SearchView);

    // The succession renders…
    await expect
      .element(page.getByText("matched 2 of 2 editions"))
      .toBeVisible();
    // …and "No matches" stays hidden (the rendered group is not wrongly excluded).
    await expect.element(page.getByText(/No matches/)).not.toBeInTheDocument();
  });

  it("surfaces a fetch error as an alert", async () => {
    vi.mocked(search).mockRejectedValue(new Error("backend down"));
    setQuery("kon");
    await render(SearchView);

    // asyncResource stringifies a non-ApiError via `String(e)` → "Error: …".
    await expect
      .element(page.getByText(/Search failed:.*backend down/))
      .toBeVisible();
    // A generic error must NOT trip the timeout branch — pins the `startsWith`
    // discriminator against accidental broadening (e.g. `includes`).
    await expect.element(page.getByText(/timed out/)).not.toBeInTheDocument();
  });
});

describe("SearchView — compact per-type tables (#808)", () => {
  // The #808 round-3 redesign: registers / variables / classifications render ONE
  // CSS-grid `.children.table` over their results IN RANK ORDER — leaves and
  // concept groups are whole-row subgrid <a>s, while classification succession is
  // the only inline disclosure. Headings stay plain text; the raw FQID is hidden.
  // Codes render a compact, code-FIRST grid table per code-system bucket.
  const FOUR_GROUPS = {
    kind: "search",
    query: "kon",
    groups: [
      {
        group: "registers",
        total_count: 1,
        results: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
      },
      {
        group: "variables",
        total_count: 1,
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
        total_count: 1,
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
        group: "codes",
        total_count: 1,
        results: [
          {
            type: "code",
            code: "1",
            label: "Man",
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
  } as unknown as SearchResponse;

  it("renders all four leaf groups as grid tables (no DataTable / no role=grid) (#808 a11y)", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    // L319 / a11y: registers no longer use a DataTable (selection-as-navigation made
    // a null-fqid row an interactive dead row). ALL four result types now render the
    // accessible `.children.table` subgrid pattern, so NO role=grid table remains and
    // there are no selectable <tr> tab stops.
    expect(
      document.querySelectorAll(".search-view table[role='grid']").length,
    ).toBe(0);
    expect(document.querySelectorAll(".search-view tr.selectable").length).toBe(
      0,
    );
    // Registers + variables + classifications + each codes bucket render a
    // `.children.table` grid.
    const gridTables = document.querySelectorAll(
      ".search-view .children.table",
    );
    expect(gridTables.length).toBeGreaterThanOrEqual(3);
  });

  it("makes the register's whole-row link target its catalog node (href, not DataTable selection)", async () => {
    // L319 / a11y: the register row is a real whole-row <a> (subgrid), so navigation
    // is the anchor's OWN href (the shell's `use:link` intercepts it at runtime) —
    // no DataTable selection-as-navigation. Assert the row link's href, the same
    // open-in-new-tab-safe contract the variable/classification leaves use.
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    const row = document.querySelector<HTMLAnchorElement>(
      ".search-view .group a.leaf-row[href='/catalog/scb/lisa']",
    );
    expect(row).not.toBeNull();
    expect(row?.getAttribute("href")).toBe(catalogHref("scb/lisa"));
  });

  it("hides the raw FQID and shows the variable's delivery column as a heading chip", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The variable heading shows the delivery-column name ("kon"), never the full
    // FQID path ("scb/lisa/kon").
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    // No code-like token renders the full FQID with slashes (delivery-column
    // chips are column names, not FQIDs).
    for (const code of document.querySelectorAll(".search-view code")) {
      expect(code.textContent ?? "").not.toContain("/");
    }
    const columnChips = Array.from(
      document.querySelectorAll<HTMLElement>(".search-view .col-chip"),
    ).map((c) => c.textContent?.trim());
    expect(columnChips).toContain("kon");
  });

  it("renders Variables as a normal heading with column chips and muted register context", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    await expect.element(page.getByText("Column")).not.toBeInTheDocument();
    const variablesHeading = page.getByRole("heading", { name: "Variables" });
    await expect.element(variablesHeading).toBeVisible();
    expect(document.querySelector(".search-view .heading-tag")).toBeNull();

    const row = document.querySelector(
      ".search-view a.leaf-row[href='/catalog/scb/lisa/kon']",
    );
    const variablePanel = row?.closest(".panel");
    expect(variablePanel).not.toBeNull();
    expect(row?.classList.contains("integrated-list-row")).toBe(true);
    expect(row ? getComputedStyle(row).borderLeftWidth : null).toBe("3px");
    const panelBody = row?.closest(".panel-body");
    expect(panelBody ? getComputedStyle(panelBody).paddingTop : null).toBe(
      "0px",
    );
    const rowCell = row?.querySelector<HTMLElement>(".name-cell");
    expect(rowCell ? getComputedStyle(rowCell).paddingLeft : null).toBe("12px");
    expect(document.querySelector(".search-view .head-row")).toBeNull();
    expect(row?.querySelector(".register-pill")).toBeNull();
    expect(row?.querySelector(".col-chip")?.textContent?.trim()).toBe("kon");
    expect(row?.querySelector(".result-detail")?.textContent).toContain("LISA");
  });

  it("makes each leaf a real catalog link (open-in-new-tab safe)", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // Each leaf is a real <a href> to its catalog node (so middle-click /
    // open-in-new-tab / screen readers get a link). The register leaf is the
    // whole-row name link (exact "LISA"); the variable + classification leaves are
    // whole-row grid links matched by their /Kön/ + /SUN/ names.
    await expect
      .element(page.getByRole("link", { name: "LISA", exact: true }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("renders result group headings as plain text, not heading badges", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Classifications" }))
      .toBeVisible();
    expect(document.querySelector(".search-view h3 .heading-tag")).toBeNull();
    expect(document.querySelectorAll(".search-view h3 .tag").length).toBe(0);
  });

  it("makes a variable leaf row a whole-row subgrid link carrying column chips and muted register context (#808 a11y)", async () => {
    // The headline redesign + a11y fix: a variable leaf row is ONE real link — a
    // SUBGRID box (`display:grid` + `grid-template-columns: subgrid`, NOT
    // `display:contents`) so its child cells align to the parent grid's track while
    // the <a> stays a real, keyboard-focusable element. Clicking anywhere on the row
    // = clicking the link. Assert the <a> targets the catalog node, carries
    // delivery-column chips plus muted register context, resolves to `subgrid`,
    // AND is keyboard-focusable (the display:contents version was dropped from the
    // tab order).
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The variable leaf row is an <a.leaf-row> to the variable's catalog node.
    const row = document.querySelector<HTMLAnchorElement>(
      ".search-view a.leaf-row[href='/catalog/scb/lisa/kon']",
    );
    expect(row).not.toBeNull();
    expect(row?.getAttribute("href")).toBe("/catalog/scb/lisa/kon");
    // It is a subgrid GRID box (the whole row is the link, no role=grid): its cells
    // align to the parent grid's tracks via subgrid.
    const style = getComputedStyle(row as Element);
    expect(style.display).toBe("grid");
    expect(style.gridTemplateColumns).toContain("subgrid");
    // KEYBOARD FOCUSABLE: focusing the anchor moves activeElement to it — the load-
    // bearing proof (a `display:contents` <a> fails this, dropped from the tab order).
    row?.focus();
    expect(document.activeElement).toBe(row);
    // The row carries delivery-column chips in the heading; register context is a
    // muted detail, not a green pill.
    expect(row?.querySelector(".register-pill")).toBeNull();
    expect(row?.querySelector(".col-chip")?.textContent?.trim()).toBe("kon");
    expect(row?.querySelector(".result-detail")?.textContent).toContain("LISA");
  });

  it("makes a classification leaf row a keyboard-focusable whole-row link (#808 a11y)", async () => {
    // The classification leaf (no terminal link) is also a subgrid whole-row <a>;
    // assert it is keyboard-focusable — the display:contents version was not.
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // Scope by the classification href; several groups share the same row class.
    const row = document.querySelector<HTMLAnchorElement>(
      ".search-view a.leaf-row[href='/catalog/class/sun2020']",
    );
    expect(row).not.toBeNull();
    expect(row?.getAttribute("href")).toBe("/catalog/class/sun2020");
    const style = getComputedStyle(row as Element);
    expect(style.display).toBe("grid");
    expect(style.gridTemplateColumns).toContain("subgrid");
    row?.focus();
    expect(document.activeElement).toBe(row);
  });

  it("makes a register leaf row a keyboard-focusable whole-row subgrid link (#808 a11y, replaces DataTable)", async () => {
    // L319 / a11y: registers no longer use DataTable selection-as-navigation (which
    // made a null-fqid row an interactive dead row). They render the SAME subgrid
    // whole-row-link pattern as variables/classifications — a real, keyboard-
    // focusable <a> per FQID-addressable register.
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The registers group renders the same one-column grid table as variables,
    // NOT a DataTable and not a split name/description table.
    const grid = document.querySelector(".search-view .children.table.cols-1");
    expect(grid).not.toBeNull();
    const row = document.querySelector<HTMLAnchorElement>(
      ".search-view .group a.leaf-row[href='/catalog/scb/lisa']",
    );
    expect(row).not.toBeNull();
    const style = getComputedStyle(row as Element);
    expect(style.display).toBe("grid");
    expect(style.gridTemplateColumns).toContain("subgrid");
    expect(row?.querySelector(".result-title")?.textContent).toContain("LISA");
    expect(row?.querySelector(".result-detail")).toBeNull();
    row?.focus();
    expect(document.activeElement).toBe(row);
  });

  it("renders a null-fqid variable leaf as a non-link row (plain text, no navigation target)", async () => {
    // A null-fqid leaf can't navigate: it renders as a non-link <div.leaf-row>
    // (no <a>), its name is plain text. The row still renders (no crash).
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "orphan",
      groups: [
        {
          group: "variables",
          total_count: 1,
          results: [
            {
              type: "variable",
              fqid: null,
              name: "Orphan",
              register: "LISA",
              definition: null,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
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

  it("wraps a long delivery-column chip without creating horizontal overflow on mobile (#808/#806)", async () => {
    // Regression for the former variables-grid third-column blowout at the 375px
    // canvas. Variable metadata now rides inside the full-width heading, so a long
    // unbroken delivery-column chip must wrap within the single row instead of
    // claiming a separate max-content grid track.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "for",
      groups: [
        {
          group: "variables",
          total_count: 1,
          results: [
            {
              type: "variable",
              // A long unbroken delivery column — the shape that formerly drove
              // the separate column track to its full intrinsic width.
              fqid: "scb/lisa/foervaervsarbetandebefolkningstatus",
              name: "Förvärvsarbetande befolkningsstatus",
              register: "LISA",
              definition: null,
              delivery_column_names: ["foervaervsarbetandebefolkningstatus"],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("for");
    const view = await render(SearchView);

    // The mobile breakpoint must be active for the bounded track to apply — pin the
    // precondition so a viewport-config change can't silently no-op this regression.
    expect(window.matchMedia("(max-width: 48rem)").matches).toBe(true);

    // Pin the rendered panel to the 375px canvas (the narrowest mobile target) so the
    // grid resolves its tracks against the real constraint. border-box keeps the 375
    // inclusive of padding.
    const root = document.querySelector<HTMLElement>(".search-view");
    expect(root).not.toBeNull();
    if (root) {
      root.style.boxSizing = "border-box";
      root.style.width = "375px";
    }

    const grid = document.querySelector<HTMLElement>(".search-view .cols-1");
    expect(grid).not.toBeNull();
    const columnChip = grid?.querySelector<HTMLElement>(".col-chip");
    expect(columnChip?.textContent?.trim()).toBe(
      "foervaervsarbetandebefolkningstatus",
    );

    const cols = grid ? getComputedStyle(grid).gridTemplateColumns : "";
    const trackPx = cols.split(/\s+/).map((t) => Number.parseFloat(t));
    expect(trackPx).toHaveLength(1);
    expect(trackPx[0]).toBeGreaterThan(300);

    expect(grid?.scrollWidth ?? 0).toBeLessThanOrEqual(
      (grid?.clientWidth ?? 0) + 1,
    );

    view.unmount();
  });

  it("interleaves a concept-group link inline in rank order (no 'Grouped families' block)", async () => {
    // #808 round 3: a group result sits inline at its rank position among the leaf
    // rows — it is NOT pulled out into a separate "Grouped families" sub-block and
    // it is NOT a disclosure. Assert a leaf row, a group link, and another leaf row
    // all render in the SAME grid table, and that the old label is gone.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ink",
      groups: [
        {
          group: "variables",
          total_count: 3,
          results: [
            {
              type: "variable",
              fqid: "scb/lisa/before",
              name: "Before fold",
              register: "LISA",
              definition: null,
            },
            {
              type: "group",
              group_key: "dispink",
              group_label: "Disponibel inkomst",
              kind: "variable",
              label_matched: false,
              matched_count: 2,
              member_count: 3,
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
              fqid: "scb/lisa/after",
              name: "After fold",
              register: "LISA",
              definition: null,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("ink");
    await render(SearchView);

    // Both leaf rows AND the group row render.
    await expect
      .element(page.getByRole("link", { name: /Before fold/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/before");
    await expect
      .element(page.getByRole("link", { name: /After fold/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/after");
    await expect
      .element(page.getByRole("link", { name: /Group: Disponibel inkomst/ }))
      .toHaveAttribute("href", "/catalog/group/scb/lisa/dispink");
    const grid = document.querySelector(".search-view .cols-1");
    const groupLink = grid?.querySelector(
      "a.group-result-row[href='/catalog/group/scb/lisa/dispink']",
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
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ink",
      groups: [
        {
          group: "variables",
          total_count: 1,
          results: [
            {
              type: "group",
              group_key: "mixed-income",
              group_label: "Disponibel inkomst",
              kind: "variable",
              label_matched: false,
              matched_count: 2,
              member_count: 3,
              register: "LISA",
              source: "token",
              members: [
                {
                  fqid: "scb/lisa/dispink-2019",
                  name: "Disp LISA",
                  facets: [],
                },
                {
                  fqid: "scb/iot/dispink-2019",
                  name: "Disp IoT",
                  facets: [],
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
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

  it("renders a single-owner code as a code-first row with a muted variable detail link (#808 round 5)", async () => {
    // #808 round 5: each code-system bucket is a compact code-FIRST table — one row
    // per code, the CODE the highlighted primary cell, then the Label. A single
    // variable owner is shown as muted detail context, not as an expandable row.
    // The classification owner is represented by the linked bucket heading.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "man",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "A10",
              label: "Diabetes drugs",
              variables: [
                { fqid: "scb/lmed/atc", name: "ATC-kod", register: "LMED" },
              ],
              variable_count: 1,
              classifications: [
                { fqid: "class/atc", short_name: "ATC code list", name: null },
              ],
              classification_count: 1,
              code_system: "ATC",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("man");
    await render(SearchView);

    // The bucket heading names and links the classification/value-set the codes
    // come from.
    await expect
      .element(page.getByRole("link", { name: "ATC", exact: true }))
      .toHaveAttribute("href", "/catalog/class/atc");
    // One code row, with the highlighted primary CODE cell.
    const codeCell = document.querySelector(".search-view .code-cell");
    expect(codeCell?.textContent?.trim()).toBe("A10");
    await expect
      .element(page.getByText("1 classification"))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByRole("link", { name: /ATC-kod/ }))
      .toHaveAttribute("href", "/catalog/scb/lmed/atc");
    expect(document.querySelector(".search-view details.code-row")).toBeNull();
  });

  // Fix A keys the disclosure each-blocks by CONTENT identity + index (`code|i` for
  // codes; `group_key|i` / `fqid|i` for the variables-/classifications-grid folds),
  // not the bare index. A bare-index key makes Svelte REUSE a <details> element for
  // whatever NEW row lands at that position on a reactive list update, carrying its
  // `open` state over (a freshly-fetched row renders expanded though the user never
  // opened it). NOTE: the end-to-end "expand then refine the query" leak is NOT
  // observable through this view, because `asyncResource` flips `loading=true` +
  // `data=null` on every refetch, so the whole results `{#each}` is torn down and
  // rebuilt between queries (fresh closed <details> regardless of key) — verified by
  // a negative-control probe. So these guard the OBSERVABLE half of the fix: the
  // index component keeps the key UNIQUE under content collisions (duplicate `code`
  // in a bucket; the same register-scoped `group_key` recurring across registers),
  // which a content-ONLY key (`code` / `group_key` alone) would crash on with
  // Svelte's each_key_duplicate — the same lesson the leaf groups already encode.

  it("renders DUPLICATE codes in one bucket without an each_key_duplicate crash (Fix A keeps the index in the key)", async () => {
    // The same `code` value recurs within one code-system bucket (distinct labels /
    // owners), so a `code`-ONLY key would collide and crash the whole render. The
    // `code|index` key tolerates it — both disclosure rows must render.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "dup",
      groups: [
        {
          group: "codes",
          total_count: 2,
          results: [
            {
              type: "code",
              code: "1",
              label: "First meaning",
              variables: [{ fqid: "scb/a/x", name: "X", register: "A" }],
              variable_count: 1,
              classifications: [],
              classification_count: 0,
              code_system: "SUN2020",
            },
            {
              type: "code",
              code: "1",
              label: "Second meaning",
              variables: [{ fqid: "scb/b/y", name: "Y", register: "B" }],
              variable_count: 1,
              classifications: [],
              classification_count: 0,
              code_system: "SUN2020",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("dup");
    await render(SearchView);

    // Both duplicate-code rows render (no crash / stuck "Searching…").
    await expect.element(page.getByText("First meaning")).toBeVisible();
    await expect.element(page.getByText("Second meaning")).toBeVisible();
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
    expect(document.querySelectorAll(".search-view .code-row").length).toBe(2);
  });

  it("renders two group links sharing a group_key across registers without an each_key_duplicate crash (Fix A keeps the index in the key)", async () => {
    // A concept_group's `group_key` is only register-scoped-unique (#322), so the
    // same key legitimately recurs across registers in one variables group. A
    // `group_key`-ONLY each key would crash; `group_key|index` tolerates it — both
    // group links must render.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ink",
      groups: [
        {
          group: "variables",
          total_count: 2,
          results: [
            {
              type: "group",
              group_key: "tfoab",
              group_label: "Inkomst IoT",
              kind: "variable",
              label_matched: false,
              matched_count: 1,
              member_count: 1,
              register: "IoT",
              source: "token",
              members: [
                { fqid: "scb/iot/tfoab-2019", name: "IoT 2019", facets: [] },
              ],
            },
            {
              type: "group",
              group_key: "tfoab",
              group_label: "Inkomst LINDA",
              kind: "variable",
              label_matched: false,
              matched_count: 1,
              member_count: 1,
              register: "LINDA",
              source: "token",
              members: [
                {
                  fqid: "scb/linda/tfoab-2019",
                  name: "LINDA 2019",
                  facets: [],
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("ink");
    await render(SearchView);

    await expect.element(page.getByText("Inkomst IoT")).toBeVisible();
    await expect.element(page.getByText("Inkomst LINDA")).toBeVisible();
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
    expect(
      document.querySelectorAll(".search-view a.group-result-row").length,
    ).toBe(2);
    expect(
      document.querySelectorAll(".search-view details.concept-group").length,
    ).toBe(0);
  });
});

describe("SearchView — docs group (#394)", () => {
  // A one-register main-search response so the main groups have a visible heading
  // (lets the failure-isolation cases assert the main groups are unaffected).
  const ONE_REGISTER: SearchResponse = {
    kind: "search",
    query: "kon",
    groups: [
      {
        group: "registers",
        total_count: 1,
        results: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
      },
    ],
  } as unknown as SearchResponse;

  // A docs response with N hits (total defaults to results.length; override for the
  // truncation-caption case).
  function docHits(
    results: DocSearchResponse["results"],
    total = results.length,
  ): DocSearchResponse {
    return {
      kind: "doc-search",
      query: "kon",
      ingested: true,
      total_count: total,
      results,
    };
  }

  it("(A) keeps the main groups and omits Documentation when docSearch REJECTS", async () => {
    // Failure isolation: a docs fetch error must NOT error or blank the main
    // groups, and the docs group is silently omitted.
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockRejectedValue(new Error("docs index down"));
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .not.toBeInTheDocument();
  });

  it("(B) omits Documentation when the docs index is absent (ingested:false)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue({
      kind: "doc-search",
      query: "kon",
      ingested: false,
      total_count: 0,
      results: [],
    });
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .not.toBeInTheDocument();
  });

  it("(C) omits Documentation when the index is present but has zero hits", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(docHits([]));
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .not.toBeInTheDocument();
  });

  it("(D) renders Documentation even when the MAIN search errors (sibling/independent)", async () => {
    // The highest-value invariant: the docs group is a SIBLING of the main `{#if}`,
    // so a main-search failure shows its error AND the resolved docs group renders.
    vi.mocked(search).mockRejectedValue(new Error("backend down"));
    vi.mocked(docSearch).mockResolvedValue(
      docHits([
        {
          filename: "lisa_kon.md",
          display_name: "LISA — Kön",
          fuzzy: false,
          register: "LISA",
          snippet: null,
          source: null,
          source_url: null,
          tags: [],
          variable: null,
        },
      ]),
    );
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByText(/Search failed:.*backend down/))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .toBeVisible();
    await expect.element(page.getByText("LISA — Kön")).toBeVisible();
  });

  it("(E) renders docs hits with display_name, /doc link, and the truncation caption", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(
      // total_count 9 > 1 rendered → the "showing 1 of 9" caption shows; the
      // filename carries a space to assert the href is encoded.
      docHits(
        [
          {
            filename: "lisa kon.md",
            display_name: "LISA — Kön",
            fuzzy: false,
            register: "LISA",
            snippet: null,
            source: null,
            source_url: null,
            tags: [],
            variable: null,
          },
        ],
        9,
      ),
    );
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /LISA — Kön/ }))
      .toHaveAttribute("href", "/doc/lisa%20kon.md");
    await expect.element(page.getByText("showing 1 of 9")).toBeVisible();
  });

  it("renders Documentation under the default (all) scope but NOT under a non-all scope (#393)", async () => {
    // The #393 toggle has no Docs option, so a scoped search means "only that one
    // group" — the additive docs section must be skipped (no fetch) and hidden
    // whenever ?type= is anything but the default `all`.
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(
      docHits([
        {
          filename: "lisa_kon.md",
          display_name: "LISA — Kön",
          fuzzy: false,
          register: "LISA",
          snippet: null,
          source: null,
          source_url: null,
          tags: [],
          variable: null,
        },
      ]),
    );

    // Default (all) scope → Documentation renders.
    setQuery("kon");
    const allView = await render(SearchView);
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .toBeVisible();
    allView.unmount();
    vi.mocked(docSearch).mockClear();

    // A scoped (?type=value) search → docs is short-circuited (no fetch) and hidden.
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=value");
    await render(SearchView);
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .not.toBeInTheDocument();
    expect(docSearch).not.toHaveBeenCalled();
  });

  it("renders a docs snippet as escaped text, never parsed HTML (republication guard)", async () => {
    // The snippet may carry HTML-looking text; the inline renderer still
    // interpolates DATA segments through Svelte, so a `<b>` in the snippet must
    // surface as literal characters, not as a parsed element.
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(
      docHits([
        {
          filename: "lisa_kon.md",
          display_name: "LISA — Kön",
          fuzzy: false,
          register: "LISA",
          snippet: "foo <b>bar</b> baz",
          source: null,
          source_url: null,
          tags: [],
          variable: null,
        },
      ]),
    );
    setQuery("kon");
    await render(SearchView);

    await expect.element(page.getByText("foo <b>bar</b> baz")).toBeVisible();
    expect(document.querySelector(".search-view b")).toBeNull();
  });

  it("renders docs FTS highlight markers with safe inline markdown (#745)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(
      docHits([
        {
          filename: "lisa_kon.md",
          display_name: "LISA — Kön",
          fuzzy: false,
          register: "LISA",
          snippet: "…the **kön** variable and _below_ <b>tag</b>…",
          source: null,
          source_url: null,
          tags: [],
          variable: null,
        },
      ]),
    );
    setQuery("kon");
    await render(SearchView);

    const detail = document.querySelector(".search-view .hit-detail");
    expect(detail?.textContent).toBe("…the kön variable and below <b>tag</b>…");
    expect(detail?.querySelector("mark")?.textContent).toBe("kön");
    expect(detail?.querySelector("em")?.textContent).toBe("below");
    expect(detail?.querySelector("b")).toBeNull();
  });
});

describe("SearchView — scoped-search ?type= toggle (#393 item 1)", () => {
  const ONE_REGISTER: SearchResponse = {
    kind: "search",
    query: "kon",
    groups: [
      {
        group: "registers",
        total_count: 1,
        results: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
      },
    ],
  } as unknown as SearchResponse;

  it("renders the toggle whenever there's a query and marks 'All' active by default", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("group", { name: "Search scope" }))
      .toBeVisible();
    // No ?type= → "All" is the active (aria-pressed) button.
    await expect
      .element(page.getByRole("button", { name: "All" }))
      .toHaveAttribute("aria-pressed", "true");
    await expect
      .element(page.getByRole("button", { name: "Registers" }))
      .toHaveAttribute("aria-pressed", "false");
  });

  it("passes the URL's ?type= to search() so the scoped result set fetches", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    // Deep-link straight to a scoped URL.
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=register");
    await render(SearchView);

    // The fetcher reads searchType and forwards it.
    await expect
      .poll(() =>
        vi
          .mocked(search)
          .mock.calls.some(([, opts]) => opts?.type === "register"),
      )
      .toBe(true);
    // The scoped button is the active one.
    await expect
      .element(page.getByRole("button", { name: "Registers" }))
      .toHaveAttribute("aria-pressed", "true");
  });

  it("clicking a scope button routes ?type= and refetches scoped", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    setQuery("kon");
    await render(SearchView);

    await page.getByRole("button", { name: "Codes" }).click();

    // The URL gains ?type=value (the "Codes" toggle value).
    await expect.poll(() => router.getQueryParam("type")).toBe("value");
    // …and search refetches with that scope.
    await expect
      .poll(() =>
        vi.mocked(search).mock.calls.some(([, opts]) => opts?.type === "value"),
      )
      .toBe(true);
  });

  it("clicking 'All' OMITS ?type= from the URL (clean canonical URL)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=register");
    await render(SearchView);
    await expect.poll(() => router.getQueryParam("type")).toBe("register");

    await page.getByRole("button", { name: "All" }).click();

    // Back to the default scope: no ?type= in the URL.
    await expect.poll(() => router.getQueryParam("type")).toBeNull();
  });

  it("degrades an unknown ?type= to 'all' (no crash, All active)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=bogus");
    await render(SearchView);

    await expect
      .element(page.getByRole("button", { name: "All" }))
      .toHaveAttribute("aria-pressed", "true");
    // An unknown scope is sent to the server as "all" (omitted by the api layer).
    await expect
      .poll(() =>
        vi.mocked(search).mock.calls.some(([, opts]) => opts?.type === "all"),
      )
      .toBe(true);
  });

  it("renders the toggle while loading (so the user can switch scope mid-search)", async () => {
    // A never-resolving search keeps the view in the loading state.
    vi.mocked(search).mockReturnValue(new Promise<SearchResponse>(() => {}));
    setQuery("kon");
    await render(SearchView);

    await expect.element(page.getByText("Searching…")).toBeVisible();
    await expect
      .element(page.getByRole("group", { name: "Search scope" }))
      .toBeVisible();
  });
});

describe("SearchView — codes grouped by code system (#393 item 3)", () => {
  it("renders per-code-system subsections, curated first, Register-local trailing", async () => {
    // Two SUN2020 codes and one register-local (null code_system). The codes are
    // already item-2-ordered upstream; the view groups by code_system preserving
    // first-appearance, so SUN2020 leads and Register-local trails.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "code",
      groups: [
        {
          group: "codes",
          total_count: 3,
          results: [
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
            {
              type: "code",
              code: "9",
              label: "Local",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
              ],
              variable_count: 1,
              classifications: [],
              classification_count: 0,
              code_system: null,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("code");
    await render(SearchView);

    // Both subsection headings render.
    await expect
      .element(page.getByRole("heading", { name: "SUN2020" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "SUN2020" }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    await expect
      .element(page.getByRole("heading", { name: "Register-local" }))
      .toBeVisible();
    // The SUN2020 subsection appears BEFORE Register-local in the DOM (curated
    // first; null/empty folds into the trailing bucket).
    const headings = Array.from(
      document.querySelectorAll<HTMLElement>(".code-system-heading"),
    ).map((h) => h.textContent?.trim());
    expect(headings).toEqual(["SUN2020", "Register-local"]);
  });

  it("keeps the 'showing N of M' caption on the codes group header", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "code",
      groups: [
        {
          group: "codes",
          total_count: 9,
          results: [
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
        },
      ],
    } as unknown as SearchResponse);
    setQuery("code");
    await render(SearchView);

    await expect.element(page.getByText("showing 1 of 9")).toBeVisible();
  });
});
