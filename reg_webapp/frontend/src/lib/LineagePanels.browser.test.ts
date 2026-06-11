import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  BindingNodeData,
  LineageWarningsResponse,
  PredecessorsResponse,
} from "./api";
import { getBindingLineageWarnings, getBindingPredecessors } from "./api";
import LineagePanels from "./LineagePanels.svelte";

// Stub the two FETCHED lineage arms (predecessors / warnings); the embedded arms
// (replaced_by / related_to / lineage) ride on the `node` prop and need no mock.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getBindingPredecessors: vi.fn(),
    getBindingLineageWarnings: vi.fn(),
  };
});

// A binding node with NO embedded lineage by default; override per case.
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

beforeEach(() => {
  vi.mocked(getBindingPredecessors).mockReset();
  vi.mocked(getBindingLineageWarnings).mockReset();
  // Default: both fetched arms resolve EMPTY.
  vi.mocked(getBindingPredecessors).mockResolvedValue({
    predecessors: [],
  } as unknown as PredecessorsResponse);
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

  it("renders the Succession section (and only the populated direction) when replaced_by exists", async () => {
    await render(LineagePanels, {
      fqidPath: "scb/slk/utbildningsinriktning",
      node: node({
        fqid: "scb/slk/utbildningsinriktning",
        replaced_by: [
          {
            provider: "scb",
            register: "slk",
            variable: "utbildningens-inriktning-enligt-sun-2000",
            fqid: "scb/slk/utbildningens-inriktning-enligt-sun-2000",
            relation_kind: null,
            reason: null,
            effective_year: null,
          },
        ] as unknown as BindingNodeData["replaced_by"],
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Succession" }))
      .toBeVisible();
    // The populated direction shows…
    await expect
      .element(page.getByRole("heading", { name: "Replaced by" }))
      .toBeVisible();
    // …the empty inbound direction is omitted (no empty "predecessors" sub-block).
    await expect
      .element(page.getByRole("heading", { name: "Replaces / predecessors" }))
      .not.toBeInTheDocument();
    // The compact fallback is gone once a section has content.
    await expect
      .element(page.getByText("No succession or lineage links."))
      .not.toBeInTheDocument();
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
            relation_kind: "same_definition_different_column",
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
  it("keeps the Succession section visible (no compact line) when the predecessors fetch errors", async () => {
    vi.mocked(getBindingPredecessors).mockRejectedValue(
      new Error("backend down"),
    );
    await render(LineagePanels, { fqidPath: "scb/lisa/kon", node: node() });

    await expect
      .element(page.getByText(/Failed to load predecessors/))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Succession" }))
      .toBeVisible();
    await expect
      .element(page.getByText("No succession or lineage links."))
      .not.toBeInTheDocument();
  });
});
