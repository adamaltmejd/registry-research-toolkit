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

  it("renders a docs snippet as LITERAL TEXT, never parsed HTML (republication guard)", async () => {
    // The snippet may carry FTS markers; `{value}` auto-escapes. A `<b>` in the
    // snippet must surface as literal characters, not a parsed element — so no
    // `<b>` exists inside the docs section.
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
