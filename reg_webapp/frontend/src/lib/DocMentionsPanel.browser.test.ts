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
    replaced_by: [],
    related_to: [],
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

  it("shows the deployment-absent note (ingested:false) and never says 'undocumented'", async () => {
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({ ingested: false, register_ingested: false }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(
        page.getByText("Documentation index not available in this deployment."),
      )
      .toBeVisible();
    // The absent-index note must NOT read as "this variable is undocumented".
    await expect
      .element(page.getByText(/undocumented/i))
      .not.toBeInTheDocument();
  });

  it("shows the register-not-ingested note and avoids the semantic-trap copy", async () => {
    // The index EXISTS but THIS register has no docs — "no docs for this register"
    // (LISA-only coverage), NOT "undocumented" and NOT "not available".
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({ ingested: true, register_ingested: false }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(
        page.getByText(
          "No documentation ingested for this register — coverage is LISA-only today.",
        ),
      )
      .toBeVisible();
    await expect
      .element(page.getByText(/undocumented/i))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByText(/not available/i))
      .not.toBeInTheDocument();
  });

  it("shows the no-mentions line when the index is present but has zero hits", async () => {
    vi.mocked(getDocsForVariable).mockResolvedValue(
      mentions({ ingested: true, register_ingested: true, total_count: 0 }),
    );
    await render(DocMentionsPanel, { node: node() });

    await expect
      .element(
        page.getByText("No documentation mentions found for this variable."),
      )
      .toBeVisible();
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
});
