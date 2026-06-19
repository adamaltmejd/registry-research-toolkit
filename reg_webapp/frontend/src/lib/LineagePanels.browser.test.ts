import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  BindingNodeData,
  LineageWarningsResponse,
  VariableEditionModel,
} from "./api";
import { getBindingLineageWarnings } from "./api";
import LineagePanels from "./LineagePanels.svelte";

// Stub the one FETCHED lineage arm (warnings); the embedded arms (succession_chain
// / related_to / lineage) ride on the `node` prop and need no mock.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getBindingLineageWarnings: vi.fn(),
  };
});

// A binding node with NO embedded lineage by default; override per case.
function node(over: Partial<BindingNodeData> = {}): BindingNodeData {
  return {
    kind: "binding",
    fqid: "scb/lisa/kon",
    name: "Kön",
    succession_chain: [],
    related_to: [],
    lineage: [],
    same_as: [],
    states: [],
    ...over,
  } as unknown as BindingNodeData;
}

// A succession-chain edition with sensible defaults; override per case.
function edition(
  over: Partial<VariableEditionModel> = {},
): VariableEditionModel {
  return {
    provider: "scb",
    register: "lisa",
    variable: "var",
    fqid: "scb/lisa/var",
    name: "Var",
    effective_year: null,
    reason: null,
    is_current: false,
    is_self: false,
    ...over,
  } as unknown as VariableEditionModel;
}

beforeEach(() => {
  vi.mocked(getBindingLineageWarnings).mockReset();
  // Default: the fetched warnings arm resolves EMPTY.
  vi.mocked(getBindingLineageWarnings).mockResolvedValue({
    lineage_warnings: [],
  } as unknown as LineageWarningsResponse);
});

describe("LineagePanels — empty-section collapse (D1.2)", () => {
  it("omits every empty section and shows one compact line instead of None walls", async () => {
    await render(LineagePanels, { fqidPath: "scb/lisa/kon", node: node() });

    // The compact fallback replaces the four "None." sections.
    await expect
      .element(page.getByText("No succession or lineage links."))
      .toBeVisible();

    // None of the section headings render when their data is empty.
    for (const heading of [
      "Succession",
      "Related (split siblings)",
      "Lineage",
      "Lineage warnings",
    ]) {
      await expect
        .element(page.getByRole("heading", { name: heading }))
        .not.toBeInTheDocument();
    }
    // And no stray "None." wall survives.
    await expect
      .element(page.getByText("None.", { exact: true }))
      .not.toBeInTheDocument();
  });

  it("renders no Succession panel for a 1-edition chain (no real succession)", async () => {
    // A standalone variable resolves to a single edition carrying both flags —
    // the panel renders nothing extra.
    await render(LineagePanels, {
      fqidPath: "scb/lisa/kon",
      node: node({
        succession_chain: [
          edition({
            variable: "kon",
            fqid: "scb/lisa/kon",
            name: "Kön",
            is_current: true,
            is_self: true,
          }),
        ] as unknown as BindingNodeData["succession_chain"],
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Succession" }))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByText("No succession or lineage links."))
      .toBeVisible();
  });

  it("renders the embedded chain: predecessors → this var → current, marked + reason shown", async () => {
    // A 3-edition chain, oldest→current: anninkf04 (2004) → anninkf (this, 2010,
    // 'Definition change') → anninkf18 (current, 2018). All visible (none collapse
    // — earlier is a single edition shown only when before self; here self is the
    // MIDDLE one, so the predecessor is the "earlier" arm of size 1).
    await render(LineagePanels, {
      fqidPath: "scb/lisa/anninkf",
      node: node({
        fqid: "scb/lisa/anninkf",
        name: "Annan inkomst",
        succession_chain: [
          edition({
            variable: "anninkf04",
            fqid: "scb/lisa/anninkf04",
            name: "Annan inkomst (2004)",
            effective_year: 2004,
          }),
          edition({
            variable: "anninkf",
            fqid: "scb/lisa/anninkf",
            name: "Annan inkomst",
            effective_year: 2010,
            reason: "Definition change",
            is_self: true,
          }),
          edition({
            variable: "anninkf18",
            fqid: "scb/lisa/anninkf18",
            name: "Annan inkomst (2018)",
            effective_year: 2018,
            reason: "Renamed",
            is_current: true,
          }),
        ] as unknown as BindingNodeData["succession_chain"],
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Succession" }))
      .toBeVisible();

    // The viewed variable is marked in place (a non-link "this variable" node).
    await expect.element(page.getByText("(this variable)")).toBeVisible();
    // The terminal edition is labeled current.
    await expect.element(page.getByText("(current edition)")).toBeVisible();
    // A transition reason renders on the annotated edition.
    await expect.element(page.getByText(/Definition change/)).toBeVisible();

    // The predecessor (size-1 "earlier" arm) is collapsed behind a disclosure;
    // expand it, then assert the link.
    await page.getByText("1 earlier edition").click();
    await expect
      .element(page.getByRole("link", { name: "Annan inkomst (2004)" }))
      .toBeVisible();
    // The current edition links too (always visible, not collapsed).
    await expect
      .element(page.getByRole("link", { name: "Annan inkomst (2018)" }))
      .toBeVisible();

    // The current/this-var node is non-link: the <li> holding "(this variable)"
    // has no <a> descendant.
    const items = page.getByRole("listitem").elements();
    const currentLi = items.find((li) =>
      li.textContent?.includes("(this variable)"),
    );
    expect(currentLi, "no listitem held the this-variable marker").toBeTruthy();
    expect(currentLi?.querySelector("a")).toBeNull();

    // The compact fallback is gone once a section has content.
    await expect
      .element(page.getByText("No succession or lineage links."))
      .not.toBeInTheDocument();
  });

  it("links a dead/renamed predecessor (name=null) using its slug + a (renamed) hint", async () => {
    // #355/#411: a dead predecessor has a VALID fqid (301-redirects to current)
    // but null name. It must render as a LINK (NOT plain text) using the variable
    // slug, with a muted "(renamed)" hint.
    await render(LineagePanels, {
      fqidPath: "scb/lisa/anninkf18",
      node: node({
        fqid: "scb/lisa/anninkf18",
        name: "Annan inkomst",
        succession_chain: [
          edition({
            variable: "anninkf04",
            fqid: "scb/lisa/anninkf04",
            name: null, // dead/renamed predecessor — no live row
            effective_year: 2004,
          }),
          edition({
            variable: "anninkf18",
            fqid: "scb/lisa/anninkf18",
            name: "Annan inkomst",
            effective_year: 2018,
            is_current: true,
            is_self: true,
          }),
        ] as unknown as BindingNodeData["succession_chain"],
      }),
    });

    // Expand the collapsed "earlier" arm (the dead predecessor), then assert it's
    // a redirecting link keyed on the slug.
    await page.getByText("1 earlier edition").click();
    const deadLink = page.getByRole("link", { name: "anninkf04" });
    await expect.element(deadLink).toBeVisible();
    // The link points at the (redirecting) fqid catalog href.
    expect(deadLink.element().getAttribute("href")).toContain(
      "scb/lisa/anninkf04",
    );
    // …and reads as historical.
    await expect.element(page.getByText("(renamed)")).toBeVisible();
  });

  it("collapses the bulk of a long chain into earlier/later disclosures", async () => {
    // A 6-edition chain viewed at index 2; current is the last (index 5). The
    // earlier arm (2 editions) and the later arm (between self and current, 2
    // editions) collapse; self + current stay visible.
    const chain = [
      edition({ variable: "v0", fqid: "scb/lisa/v0", name: "V0" }),
      edition({ variable: "v1", fqid: "scb/lisa/v1", name: "V1" }),
      edition({
        variable: "v2",
        fqid: "scb/lisa/v2",
        name: "V2 viewed",
        is_self: true,
      }),
      edition({ variable: "v3", fqid: "scb/lisa/v3", name: "V3" }),
      edition({ variable: "v4", fqid: "scb/lisa/v4", name: "V4" }),
      edition({
        variable: "v5",
        fqid: "scb/lisa/v5",
        name: "V5 current",
        is_current: true,
      }),
    ];

    await render(LineagePanels, {
      fqidPath: "scb/lisa/v2",
      node: node({
        fqid: "scb/lisa/v2",
        name: "V2 viewed",
        succession_chain:
          chain as unknown as BindingNodeData["succession_chain"],
      }),
    });

    // Both collapse disclosures render with their counts.
    await expect.element(page.getByText("2 earlier editions")).toBeVisible();
    await expect.element(page.getByText("2 later editions")).toBeVisible();

    // Self + current stay visible without expanding anything.
    await expect.element(page.getByText("(this variable)")).toBeVisible();
    await expect.element(page.getByText("(current edition)")).toBeVisible();

    // The bulk is hidden behind the closed disclosures — a collapsed <details>
    // drops its content from the accessibility tree, so the role-query finds NO
    // link for V0/V4 until expanded.
    expect(page.getByRole("link", { name: "V0" }).elements()).toHaveLength(0);
    expect(page.getByRole("link", { name: "V4" }).elements()).toHaveLength(0);

    // Expand the "earlier" disclosure → its member (V0) becomes a visible link,
    // proving the bulk was folded behind it.
    await page.getByText("2 earlier editions").click();
    await expect.element(page.getByRole("link", { name: "V0" })).toBeVisible();
  });

  it("renders the Related section when split-sibling edges exist", async () => {
    await render(LineagePanels, {
      fqidPath: "scb/aes/birthfather",
      node: node({
        fqid: "scb/aes/birthfather",
        related_to: [
          {
            provider: "scb",
            register: "aes",
            variable: "birthmother",
            fqid: "scb/aes/birthmother",
            relation_kind: "code_vs_label_pair",
          },
        ] as unknown as BindingNodeData["related_to"],
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Related (split siblings)" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "birthmother" }))
      .toBeVisible();
  });

  it("renders the Lineage warnings section when the fetched arm returns warnings", async () => {
    vi.mocked(getBindingLineageWarnings).mockResolvedValue({
      lineage_warnings: [
        {
          consumer_state_id: 1,
          warning_kind: "source_gap",
          message: "No source state covers 2015.",
        },
      ],
    } as unknown as LineageWarningsResponse);

    await render(LineagePanels, { fqidPath: "scb/lisa/kon", node: node() });

    await expect
      .element(page.getByRole("heading", { name: "Lineage warnings" }))
      .toBeVisible();
    await expect
      .element(page.getByText("No source state covers 2015."))
      .toBeVisible();
  });

  // Regression guard for the dangerous false negative: an ERRORED fetched arm
  // must keep its section visible (with the error) — never collapse into
  // "No succession or lineage links.", which would read as a confirmed absence.
  it("keeps the Lineage warnings section visible (no compact line) when the fetch errors", async () => {
    vi.mocked(getBindingLineageWarnings).mockRejectedValue(
      new Error("backend down"),
    );
    await render(LineagePanels, { fqidPath: "scb/lisa/kon", node: node() });

    await expect
      .element(page.getByText(/Failed to load lineage warnings/))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Lineage warnings" }))
      .toBeVisible();
    await expect
      .element(page.getByText("No succession or lineage links."))
      .not.toBeInTheDocument();
  });
});
