import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { DocSearchResponse, SearchResponse } from "./api";
import { docSearch, search } from "./api";
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
    // A register/variable/classification leaf links to its catalog node.
    await expect
      .element(page.getByRole("link", { name: /LISA/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
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

  it("links code-hit owners and shows a muted '+N more' for the slice cap", async () => {
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
              ],
              variable_count: 6,
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

    // The owning variable is the link target (not the bare code).
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    // The slice cap surfaces as a muted, non-interactive "+5 more".
    await expect.element(page.getByText("+5 more")).toBeVisible();
  });

  it("omits '+N more' for a code hit's classification owners when the slice is complete", async () => {
    // Symmetric to the variable-owner "+N more" test, but for the classification
    // branch: classifications.length === classification_count → no overflow line.
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
              classifications: [
                { fqid: "class/sun2020", short_name: "SUN", name: null },
              ],
              classification_count: 1,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun");
    await render(SearchView);

    // The owning classification is the link target…
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    // …and no overflow line, since the slice is complete.
    await expect.element(page.getByText(/more/)).not.toBeInTheDocument();
  });

  it("renders BOTH owner lists for a code hit carrying variables and classifications", async () => {
    // The two `<ul class="owners">` lists are independently gated — a regression
    // coupling the classification list to the variable list being empty (or vice
    // versa) would still pass the single-owner tests above, so exercise both at
    // once and assert each link renders.
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
              ],
              variable_count: 1,
              classifications: [
                { fqid: "class/sun2020", short_name: "SUN", name: null },
              ],
              classification_count: 1,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    // ≥ 2 chars so the min-length guard fetches.
    setQuery("11");
    await render(SearchView);

    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("expands a folded concept-group result to its member links", async () => {
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
              group_key: "scb/lisa/dispink",
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

    // The family hint reads "matched M of N"; the group is NOT itself a link.
    await expect.element(page.getByText("matched 2 of 3")).toBeVisible();
    // Expand the <details> (collapsed by default) to reveal the member links.
    await page.getByText("Disponibel inkomst").click();
    // Members are real leaf links.
    await expect
      .element(page.getByRole("link", { name: /Disp 2019/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/dispink-2019");
  });

  it("renders a classification_succession result: terminal edition links + a folded edition history (#571)", async () => {
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
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    // The family hint reads "matched M of N editions"; the row is NOT a concept group.
    await expect
      .element(page.getByText("matched 2 of 3 editions"))
      .toBeVisible();
    // Expand the <details> to reveal the per-edition links.
    await page.getByText("matched 2 of 3 editions").click();
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

  it("renders two folded concept groups sharing a group_key across registers (no each_key_duplicate crash)", async () => {
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
  // The #808 round-2 redesign: each leaf group renders a compact DataTable (the
  // #806 pattern) — categorical type identity moves to the GROUP HEADING (a single
  // Tag), the raw FQID is hidden everywhere (the leaf SLUG is the only identifier
  // shown), the Register column is prominent for variables, and the whole row is
  // navigable to the hit. Folded families stay <details>; codes stay a list.
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

  it("renders the leaf groups as DataTables (grid roles + rows)", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The three leaf groups (registers / variables / classifications) each render
    // a selectable DataTable — a role=grid with selectable rows (gridcell remap).
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    const grids = document.querySelectorAll(".search-view table[role='grid']");
    expect(grids.length).toBe(3);
    // The rows are keyboard-activatable (DataTable selection: tabindex + grid row).
    const selectableRows = document.querySelectorAll(
      ".search-view tr.selectable[tabindex='0']",
    );
    expect(selectableRows.length).toBeGreaterThanOrEqual(3);
  });

  it("hides the raw FQID and shows the leaf SLUG as the variable's Column", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The variable's Column cell shows the LEAF SLUG ("kon"), never the full
    // FQID path ("scb/lisa/kon").
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    // No leaf row renders the full FQID with slashes (the old <code class=
    // "hit-fqid">scb/lisa/kon</code> is gone). The member-slug <code> in folded
    // families never carries slashes either.
    for (const code of document.querySelectorAll(".search-view code")) {
      expect(code.textContent ?? "").not.toContain("/");
    }
    // The mono Column cell carries the bare leaf slug.
    const monoCells = Array.from(
      document.querySelectorAll<HTMLElement>(".search-view td.mono"),
    ).map((c) => c.textContent?.trim());
    expect(monoCells).toContain("kon");
  });

  it("renders a prominent Register column for variable hits", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The variables table carries a "Column" header (the leaf-slug column) that
    // disambiguates it from the registers table — assert its presence so we know
    // the variable DataTable rendered with its three columns.
    await expect
      .element(page.getByRole("columnheader", { name: "Column" }))
      .toBeVisible();
    // The variable's Register renders in its OWN prominent column (the `.register`
    // span), not as a muted trailing label.
    const register = document.querySelector(".search-view .register");
    expect(register?.textContent?.trim()).toBe("LISA");
  });

  it("makes the name cell a real catalog link (open-in-new-tab safe)", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // Each leaf's name cell is a real <a href> to its catalog node (so middle-
    // click / open-in-new-tab / screen readers get a link), independent of the
    // whole-row navigation.
    await expect
      .element(page.getByRole("link", { name: /LISA/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("marks the group identity via a heading Tag, not per-row badges", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    // The categorical tone lives on the heading Tag (one per leaf group), NOT a
    // per-row badge. The codes heading carries no single tone.
    for (const tone of ["reg", "var", "class"] as const) {
      const tag = document.querySelector(
        `.search-view h3 .heading-tag .tag.tone-${tone}`,
      );
      expect(tag, `expected a heading Tag on tone ${tone}`).not.toBeNull();
    }
    // Exactly the three leaf-group headings carry a categorical Tag (no per-row
    // badge proliferation).
    const headingTags = document.querySelectorAll(".search-view h3 .tag");
    expect(headingTags.length).toBe(3);
  });

  it("navigates to the hit when the whole leaf row is activated (not the name link)", async () => {
    // The headline redesign: a leaf row is itself navigable (DataTable
    // selection-as-navigation). SearchView wires `onselect={(r) =>
    // navigateTo(r.fqid)}`, and `navigateTo` routes to `catalogHref(fqid)`.
    // Click the ROW at a NON-link point (the plain `.register` cell, not the
    // inner name <a>) so this fails if the `onselect` wiring is dropped — the
    // name-link href is asserted separately by the "open-in-new-tab safe" test.
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The variable leaf row carries a plain Register cell ("LISA") with no
    // interactive descendant; clicking it bubbles to the selectable row and
    // fires onselect (mirrors DataTable.browser.test.ts's plain-cell click).
    const register = document.querySelector<HTMLElement>(
      ".search-view .register",
    );
    expect(register?.textContent?.trim()).toBe("LISA");
    register?.click();

    // The row click navigated to the variable's catalog node (slashes preserved
    // by catalogHref), and the query URL is gone — so this can't pass on the
    // no-op path the null-fqid sibling test guards.
    await expect
      .poll(() => window.location.pathname)
      .toBe("/catalog/scb/lisa/kon");
    // The router parsed the new path into the catalog-node route (proves a real
    // navigation, not just a URL-string mutation).
    expect(router.route).toEqual({
      name: "catalog-node",
      fqidPath: "scb/lisa/kon",
    });
  });

  it("renders a null-fqid leaf as plain text and never navigates on its row", async () => {
    // A null-fqid leaf can't navigate: its name is plain text (no link), and the
    // row's onselect no-ops (navigateTo bails on a falsy fqid). The row still
    // renders (no each_key_duplicate crash on the synthetic rowId key).
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
    // Clicking the row doesn't crash / navigate away (the URL keeps its query).
    await page.getByText("Orphan").click();
    await expect.poll(() => router.getQueryParam("q")).toBe("orphan");
  });

  it("keeps folded families as <details> with leaf-slug members (no full FQID)", async () => {
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
              group_key: "scb/lisa/dispink",
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
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("ink");
    await render(SearchView);

    // The fold stays a <details> disclosure (no DataTable for it).
    await expect.element(page.getByText("matched 2 of 3")).toBeVisible();
    await page.getByText("Disponibel inkomst").click();
    // Members are real leaf links…
    await expect
      .element(page.getByRole("link", { name: /Disp 2019/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/dispink-2019");
    // …and the member identifier is the leaf SLUG, never the full FQID path.
    const memberSlug = document.querySelector(".search-view .member-slug");
    expect(memberSlug?.textContent?.trim()).toBe("dispink-2019");
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
