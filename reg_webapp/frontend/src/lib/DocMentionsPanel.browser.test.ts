import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { DocPage, VariableShow } from "./api";
import { apiError, stubApi } from "./api-test-helpers";
import DocMentionsPanel from "./DocMentionsPanel.svelte";

/** Answer the panel's one request, `GET /api/docs/search`: a page, `null` for a
 * deployment shipped without docs (`docs_unavailable`), an `Error` for a request
 * that fails, or a promise that never settles. Returns the spy whose calls are
 * the requests the panel sent. */
function serveDocs(answer: DocPage | null | Error | Promise<never>) {
  return stubApi((url) => {
    if (url.pathname !== "/api/docs/search") {
      return undefined;
    }
    if (answer instanceof Error) {
      throw answer;
    }
    return answer === null
      ? apiError(404, "docs_unavailable", "no docs database")
      : answer;
  });
}

/** The query of the first request the panel sent. */
function sentQuery(spy: ReturnType<typeof serveDocs>): URLSearchParams {
  return new URL(String(spy.mock.calls[0]?.[0]), "http://localhost")
    .searchParams;
}

// A binding leaf node. The panel only reads `node.fqid` and `node.name`, so the
// other fields are empty/zero (mirrors how LineagePanels' test builds its node).
function node(over: Partial<VariableShow> = {}): VariableShow {
  return {
    kind: "variable",
    fqid: "scb/lisa/kon",
    name: "Kön",
    deprecated: false,
    is_identifier: false,
    is_sensitive: false,
    same_as: [],
    tags: [],
    ...over,
  };
}

// A docs page; cases override the fields under test.
function mentions(overrides: Partial<DocPage> = {}): DocPage {
  return {
    register_ingested: true,
    total: 0,
    items: [],
    ...overrides,
  };
}

describe("DocMentionsPanel (#402)", () => {
  it("shows the aria-busy loading line while the fetch is pending", async () => {
    // A never-resolving promise pins the loading branch.
    serveDocs(new Promise<never>(() => {}));
    await render(DocMentionsPanel, { node: node() });

    await expect.element(page.getByText("Loading…")).toBeVisible();
    expect(document.querySelector('[aria-busy="true"]')).not.toBeNull();
  });

  it("surfaces a fetch error inline without blanking the heading (failure isolation)", async () => {
    // The docs section is an independent failure domain — an error stays inline as
    // an alert and the panel heading must survive (the leaf never blanks).
    serveDocs(new Error("network down"));
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(page.getByRole("alert"))
      .toMatchTextContent("Failed to load parsed documentation");
    await expect
      .element(page.getByRole("heading", { name: "Parsed documentation" }))
      .toBeVisible();
  });

  it("omits the whole section when the deployment has no docs database", async () => {
    // `null` = `docs_unavailable`: fails if the panel shows an empty section or an
    // error for a deployment shipped without docs.
    serveDocs(null);
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(page.getByRole("heading", { name: "Parsed documentation" }))
      .not.toBeInTheDocument();
  });

  it("omits the whole section when this register has no ingested docs", async () => {
    // The index EXISTS but THIS register has no docs — a resolved-empty state, so
    // the whole section is omitted (no "no docs for this register" wall).
    serveDocs(
      mentions({ register_ingested: false }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(page.getByRole("heading", { name: "Parsed documentation" }))
      .not.toBeInTheDocument();
  });

  it("omits the whole section when the index is present but has zero hits", async () => {
    serveDocs(
      mentions({ register_ingested: true, total: 0 }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(page.getByRole("heading", { name: "Parsed documentation" }))
      .not.toBeInTheDocument();
  });

  it("renders a hit with encoded /doc and source-PDF hrefs plus a literal snippet (republication guard)", async () => {
    // The snippet may carry FTS markers; `{value}` auto-escapes. A `<b>` in the
    // snippet must surface as literal characters, not a parsed element — so no `<b>`
    // exists inside the mentions list. The filename carries a space to assert the
    // href is encoded.
    serveDocs(
      mentions({
        register_ingested: true,
        total: 1,
        items: [
          {
            register: "lisa",
            filename: "lisa kon.md",
            display_name: "LISA — Kön",
            snippet: "foo <b>bar</b>",
            source_url: "https://www.scb.se/lisa-source.pdf",
            source_title: "Full LISA PDF",
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(page.getByRole("link", { name: /LISA — Kön/ }))
      .toHaveAttribute("href", "/doc/lisa%20kon.md");
    await expect
      .element(page.getByRole("link", { name: "Source PDF: Full LISA PDF" }))
      .toHaveAttribute("href", "https://www.scb.se/lisa-source.pdf");
    await expect.element(page.getByText("foo <b>bar</b>")).toBeVisible();
    expect(document.querySelector(".mentions b")).toBeNull();
  });

  it("renders the FTS `**…**` highlight as a <mark>, not literal markers (#672)", async () => {
    // The `**` markers are the FTS highlight delimiter; they must render as a
    // <mark> matched-term span, NOT surface as literal `**` characters. Rendering
    // still goes through auto-escaped interpolation (no {@html}).
    serveDocs(
      mentions({
        register_ingested: true,
        total: 1,
        items: [
          {
            register: "lisa",
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: "…the **kön** variable…",
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    // Poll until the async mentions fetch has rendered before the sync queries.
    await expect.element(page.getByText("LISA — Kön")).toBeVisible();
    const highlight = document.querySelector(".mentions mark");
    expect(highlight?.textContent).toBe("kön");
    // The literal delimiter never reaches the DOM text.
    await expect
      .element(page.getByText("**kön**", { exact: false }))
      .not.toBeInTheDocument();
  });

  it("renders an `_em_` snippet as an <em>, not a <mark> (em template branch, #672)", async () => {
    // `_…_` (literal markdown emphasis from the source body, not an FTS highlight)
    // takes the `em` template branch → <em>, distinct from the `strong` → <mark>
    // matched-term branch. Exercises the previously-untested em arm.
    serveDocs(
      mentions({
        register_ingested: true,
        total: 1,
        items: [
          {
            register: "lisa",
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: "see _below_ for detail",
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    // Poll until the async mentions fetch has rendered before the sync queries.
    await expect.element(page.getByText("LISA — Kön")).toBeVisible();
    expect(document.querySelector(".mentions em")?.textContent).toBe("below");
    expect(document.querySelector(".mentions mark")).toBeNull();
  });

  it("shows 'showing N of M' only when the slice is truncated", async () => {
    // (a) truncated → caption shows.
    serveDocs(
      mentions({
        register_ingested: true,
        total: 7,
        items: [
          {
            register: "lisa",
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: null,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect.element(page.getByText("showing 1 of 7")).toBeVisible();
  });

  it("omits the 'showing N of M' caption when the slice is complete", async () => {
    // (b) complete (shown === total) → no caption; guards the strict `>` boundary.
    serveDocs(
      mentions({
        register_ingested: true,
        total: 1,
        items: [
          {
            register: "lisa",
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: null,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect.element(page.getByText(/showing/i)).not.toBeInTheDocument();
  });

  it("queries by the display name, scoped to the register FQID (first two FQID segments)", async () => {
    // Fails if the panel sends the bare register slug (`rams`), which the Rust
    // `docs_search` does not match, or queries by the slug although the variable
    // has a display name.
    const spy = serveDocs(mentions());
    await render(DocMentionsPanel, {
      node: node({ fqid: "scb/rams/yrke", name: "Yrke" }),
    });

    await expect.poll(() => spy.mock.calls.length).toBe(1);
    expect(sentQuery(spy).get("register")).toBe("scb/rams");
    expect(sentQuery(spy).get("q")).toBe("Yrke");
  });

  it("falls back to the slug (3rd segment) as the query when name is null", async () => {
    const spy = serveDocs(mentions());
    await render(DocMentionsPanel, {
      node: node({ fqid: "scb/lisa/kon", name: null }),
    });

    await expect.poll(() => spy.mock.calls.length).toBe(1);
    expect(sentQuery(spy).get("q")).toBe("kon");
  });

  it("captions a grouped member's hits as concept-grain (#670)", async () => {
    // A grouped member (`node.group` set) shares its concept name with its
    // siblings, so the FTS hits are concept-grain (common across the group) — the
    // panel adds a clarifying caption above the fuzzy note. Not suppression: the
    // hit list still renders.
    serveDocs(
      mentions({
        register_ingested: true,
        total: 1,
        items: [
          {
            register: "lisa",
            filename: "lisa_naringsgren.md",
            display_name: "LISA — Näringsgren",
            snippet: null,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, {
      node: node({
        fqid: "scb/lisa/naringsgren-storsta-agi-sni2007g",
        name: "Näringsgren, största förvärvskälla",
        group: "group/scb/lisa/naringsgren",
      }),
    });

    await expect.element(page.getByText(/shared concept name/i)).toBeVisible();
    // Still shows the hit (caption clarifies grain, does not suppress).
    await expect
      .element(page.getByRole("link", { name: /LISA — Näringsgren/ }))
      .toBeVisible();
  });

  it("does NOT caption an ungrouped variable's hits (no node.group)", async () => {
    serveDocs(
      mentions({
        register_ingested: true,
        total: 1,
        items: [
          {
            register: "lisa",
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: null,
            tags: [],
          },
        ],
      }),
    );
    // The default node() fixture has no `group`.
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(page.getByText(/shared concept name/i))
      .not.toBeInTheDocument();
    // The ordinary fuzzy note still renders.
    await expect
      .element(page.getByText(/Heuristic name matches/))
      .toBeVisible();
  });
});
