import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { BindingNodeData, DocVariableMentions } from "./api";
import { getDocsForVariable } from "./api";
import DocMentionsPanel from "./DocMentionsPanel.svelte";

// Mock the single GET the panel drives (mirrors LineagePanels' api-mock style);
// keep the rest of api.ts real (the type exports). Each case stubs
// `getDocsForVariable` and renders `<DocMentionsPanel node=… />`.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getDocsForVariable: vi.fn(),
  };
});

// A binding leaf node. The panel only reads `node.fqid` and `node.name`, so the
// other fields are empty/zero (mirrors how LineagePanels' test builds its node).
function node(over: Partial<BindingNodeData> = {}): BindingNodeData {
  return {
    kind: "binding",
    fqid: "scb/lisa/kon",
    name: "Kön",
    succession_chain: [],
    lineage: [],
    same_as: [],
    states: [],
    ...over,
  } as unknown as BindingNodeData;
}

// A docs-mentions envelope; cases override the fields under test.
function mentions(
  overrides: Partial<DocVariableMentions> = {},
): DocVariableMentions {
  return {
    kind: "doc-mentions",
    ingested: true,
    register: "lisa",
    register_ingested: true,
    total_count: 0,
    results: [],
    ...overrides,
  };
}

beforeEach(() => {
  vi.mocked(getDocsForVariable).mockReset();
});

describe("DocMentionsPanel (#402)", () => {
  it("shows the aria-busy loading line while the fetch is pending", async () => {
    // A never-resolving promise pins the loading branch.
    vi.mocked(getDocsForVariable).mockReturnValue(new Promise(() => {}));
    await render(DocMentionsPanel, { node: node() });

    await expect.element(page.getByText("Loading…")).toBeVisible();
    expect(document.querySelector('[aria-busy="true"]')).not.toBeNull();
  });

  it("surfaces a fetch error inline without blanking the heading (failure isolation)", async () => {
    // The docs section is an independent failure domain — an error stays inline as
    // an alert and the panel heading must survive (the leaf never blanks).
    vi.mocked(getDocsForVariable).mockRejectedValue(new Error("network down"));
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(page.getByRole("alert"))
      .toHaveTextContent("Failed to load documentation mentions");
    await expect
      .element(
        page.getByRole("heading", { name: "Mentioned in documentation" }),
      )
      .toBeVisible();
  });

  it("omits the whole section when no docs index is ingested in this deployment", async () => {
    // Resolved-empty (ingested:false) is noise on the subject page — the entire
    // section is omitted, NOT rendered as an empty-state note.
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({ ingested: false, register_ingested: false }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(
        page.getByRole("heading", { name: "Mentioned in documentation" }),
      )
      .not.toBeInTheDocument();
  });

  it("omits the whole section when this register has no ingested docs", async () => {
    // The index EXISTS but THIS register has no docs — a resolved-empty state, so
    // the whole section is omitted (no "no docs for this register" wall).
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({ ingested: true, register_ingested: false }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(
        page.getByRole("heading", { name: "Mentioned in documentation" }),
      )
      .not.toBeInTheDocument();
  });

  it("omits the whole section when the index is present but has zero hits", async () => {
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({ ingested: true, register_ingested: true, total_count: 0 }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(
        page.getByRole("heading", { name: "Mentioned in documentation" }),
      )
      .not.toBeInTheDocument();
  });

  it("renders a hit with an encoded /doc href and a snippet as LITERAL TEXT (republication guard)", async () => {
    // The snippet may carry FTS markers; `{value}` auto-escapes. A `<b>` in the
    // snippet must surface as literal characters, not a parsed element — so no `<b>`
    // exists inside the mentions list. The filename carries a space to assert the
    // href is encoded.
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 1,
        results: [
          {
            filename: "lisa kon.md",
            display_name: "LISA — Kön",
            snippet: "foo <b>bar</b>",
            fuzzy: true,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(page.getByRole("link", { name: /LISA — Kön/ }))
      .toHaveAttribute("href", "/doc/lisa%20kon.md");
    await expect.element(page.getByText("foo <b>bar</b>")).toBeVisible();
    expect(document.querySelector(".mentions b")).toBeNull();
  });

  it("renders the FTS `**…**` highlight as a <mark>, not literal markers (#672)", async () => {
    // The `**` markers are the FTS highlight delimiter; they must render as a
    // <mark> matched-term span, NOT surface as literal `**` characters. Rendering
    // still goes through auto-escaped interpolation (no {@html}).
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 1,
        results: [
          {
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: "…the **kön** variable…",
            fuzzy: true,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

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
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 1,
        results: [
          {
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: "see _below_ for detail",
            fuzzy: true,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    expect(document.querySelector(".mentions em")?.textContent).toBe("below");
    expect(document.querySelector(".mentions mark")).toBeNull();
  });

  it("renders a markerless snippet verbatim with no <mark> or <em> (plain branch)", async () => {
    // No emphasis markers → one plain segment → the else branch; the snippet text
    // surfaces verbatim and neither emphasis element is created.
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 1,
        results: [
          {
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: "some plain context",
            fuzzy: true,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    expect(document.querySelector(".mentions mark")).toBeNull();
    expect(document.querySelector(".mentions em")).toBeNull();
    expect(document.querySelector(".hit-detail")?.textContent).toBe(
      "some plain context",
    );
  });

  it("renders one <mark> per `**…**` highlight in a multi-match snippet (#672)", async () => {
    // A realistic FTS snippet wraps every matched term; each must become its own
    // <mark>, in order, with the delimiters dropped.
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 1,
        results: [
          {
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: "…**kön** och **ålder**…",
            fuzzy: true,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    const marks = document.querySelectorAll(".mentions mark");
    expect(marks.length).toBe(2);
    expect(marks[0]?.textContent).toBe("kön");
    expect(marks[1]?.textContent).toBe("ålder");
  });

  it("shows 'showing N of M' only when the slice is truncated", async () => {
    // (a) truncated → caption shows.
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 7,
        results: [
          {
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: null,
            fuzzy: true,
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
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 1,
        results: [
          {
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: null,
            fuzzy: true,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect.element(page.getByText(/showing/i)).not.toBeInTheDocument();
  });

  it("scopes the fetch to the register = 2nd FQID segment", async () => {
    vi.mocked(getDocsForVariable).mockResolvedValue(mentions());
    await render(DocMentionsPanel, {
      node: node({ fqid: "scb/rams/yrke", name: "Yrke" }),
    });

    const call = vi.mocked(getDocsForVariable).mock.calls[0];
    expect(call?.[1]?.register).toBe("rams");
  });

  it("queries by the variable's display name when present", async () => {
    vi.mocked(getDocsForVariable).mockResolvedValue(mentions());
    await render(DocMentionsPanel, {
      node: node({ fqid: "scb/lisa/kon", name: "Kön" }),
    });

    const call = vi.mocked(getDocsForVariable).mock.calls[0];
    expect(call?.[0]).toBe("Kön");
  });

  it("falls back to the slug (3rd segment) as the query when name is null", async () => {
    vi.mocked(getDocsForVariable).mockResolvedValue(mentions());
    await render(DocMentionsPanel, {
      node: node({ fqid: "scb/lisa/kon", name: null }),
    });

    const call = vi.mocked(getDocsForVariable).mock.calls[0];
    expect(call?.[0]).toBe("kon");
  });

  it("captions a grouped member's hits as concept-grain (#670)", async () => {
    // A grouped member (`node.group` set) shares its concept name with its
    // siblings, so the FTS hits are concept-grain (common across the group) — the
    // panel adds a clarifying caption above the fuzzy note. Not suppression: the
    // hit list still renders.
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 1,
        results: [
          {
            filename: "lisa_naringsgren.md",
            display_name: "LISA — Näringsgren",
            snippet: null,
            fuzzy: true,
            tags: [],
          },
        ],
      }),
    );
    await render(DocMentionsPanel, {
      node: node({
        fqid: "scb/lisa/naringsgren-storsta-agi-sni2007g",
        name: "Näringsgren, största förvärvskälla",
        group: {
          provider: "scb",
          register: "lisa",
          key: "naringsgren-storsta-agi-sni2007",
        },
      }),
    });

    await expect.element(page.getByText(/shared concept name/i)).toBeVisible();
    // Still shows the hit (caption clarifies grain, does not suppress).
    await expect
      .element(page.getByRole("link", { name: /LISA — Näringsgren/ }))
      .toBeVisible();
  });

  it("does NOT show the concept-grain caption for a grouped member with zero hits (#670)", async () => {
    // The concept-grain caption lives INSIDE the resolved-data branch, which only
    // renders when there are usable hits (`results.length > 0`). A grouped member
    // with zero hits omits the WHOLE section (omit-when-empty), so the caption
    // never appears — pin that it doesn't leak out of the results branch.
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({ ingested: true, register_ingested: true, total_count: 0 }),
    );
    await render(DocMentionsPanel, {
      node: node({
        fqid: "scb/lisa/naringsgren-storsta-agi-sni2007g",
        name: "Näringsgren, största förvärvskälla",
        group: {
          provider: "scb",
          register: "lisa",
          key: "naringsgren-storsta-agi-sni2007",
        },
      }),
    });

    // The whole section is omitted (no heading), so the caption is absent too.
    await expect
      .element(
        page.getByRole("heading", { name: "Mentioned in documentation" }),
      )
      .not.toBeInTheDocument();
    await expect
      .element(page.getByText(/shared concept name/i))
      .not.toBeInTheDocument();
  });

  it("does NOT caption an ungrouped variable's hits (no node.group)", async () => {
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({
        register_ingested: true,
        total_count: 1,
        results: [
          {
            filename: "lisa_kon.md",
            display_name: "LISA — Kön",
            snippet: null,
            fuzzy: true,
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
