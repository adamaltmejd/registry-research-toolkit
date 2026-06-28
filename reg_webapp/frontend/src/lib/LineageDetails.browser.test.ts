import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { BindingNodeData, LineageWarningsResponse } from "./api";
import { getBindingLineageWarnings } from "./api";
import LineageDetails from "./LineageDetails.svelte";

// LineageDetails (#678) re-homes the two NON-graph affordances off the retired
// LineagePanels: PROVENANCE (the embedded `lineage[]` edges + the variable's
// source register) and the FETCHED lineage warnings. Succession is NOT
// here — it's a graph edge now (HistoryGraph). These port the relevant cases:
// omit-when-empty, the provenance list, the source-register line, the warnings
// loading/error/empty/data states, and failure isolation.

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getBindingLineageWarnings: vi.fn() };
});

function node(over: Partial<BindingNodeData> = {}): BindingNodeData {
  return {
    kind: "binding",
    fqid: "scb/lisa/kon",
    name: "Kön",
    lineage: [],
    source_register_id: null,
    source_register_text: null,
    states: [],
    succession_chain: [],
    same_as: [],
    ...over,
  } as unknown as BindingNodeData;
}

beforeEach(() => {
  vi.mocked(getBindingLineageWarnings).mockReset();
  // Default: the fetched warnings arm resolves EMPTY.
  vi.mocked(getBindingLineageWarnings).mockResolvedValue({
    lineage_warnings: [],
  } as unknown as LineageWarningsResponse);
});

describe("LineageDetails — omit-when-empty (#678)", () => {
  it("shows one compact line (no headed walls) when provenance + warnings are empty", async () => {
    await render(LineageDetails, { fqidPath: "scb/lisa/kon", node: node() });

    await expect
      .element(page.getByText("No provenance or lineage warnings."))
      .toBeVisible();
    for (const heading of ["Provenance", "Lineage warnings"]) {
      await expect
        .element(page.getByRole("heading", { name: heading }))
        .not.toBeInTheDocument();
    }
  });
});

describe("LineageDetails — provenance", () => {
  it("renders the consumer/source lineage edges with a window + source link", async () => {
    await render(LineageDetails, {
      fqidPath: "scb/lisa/kon",
      node: node({
        lineage: [
          {
            consumer_state_id: 1,
            source_state_id: 2,
            valid_from: "2005-01-01",
            valid_to: "2010-12-31",
            source_fqid: "scb/rtb/kon",
          },
        ] as unknown as BindingNodeData["lineage"],
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Provenance" }))
      .toBeVisible();
    await expect.element(page.getByText("2005 – 2010")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "scb/rtb/kon" }))
      .toBeVisible();
  });

  it("falls back to 'source state #N' when a lineage edge has no source_fqid", async () => {
    await render(LineageDetails, {
      fqidPath: "scb/lisa/kon",
      node: node({
        lineage: [
          {
            consumer_state_id: 1,
            source_state_id: 7,
            valid_from: "2005-01-01",
            valid_to: "2010-12-31",
            source_fqid: null,
          },
        ] as unknown as BindingNodeData["lineage"],
      }),
    });
    await expect.element(page.getByText("source state #7")).toBeVisible();
  });

  it("surfaces the variable's source register as a compact line", async () => {
    await render(LineageDetails, {
      fqidPath: "scb/lisa/kon",
      node: node({ source_register_text: "Registret över totalbefolkningen" }),
    });
    await expect
      .element(page.getByRole("heading", { name: "Provenance" }))
      .toBeVisible();
    await expect.element(page.getByText(/Source register:/)).toBeVisible();
    await expect
      .element(page.getByText("Registret över totalbefolkningen"))
      .toBeVisible();
  });
});

describe("LineageDetails — warnings (own failure domain)", () => {
  it("renders the warnings section when the fetched arm returns warnings", async () => {
    vi.mocked(getBindingLineageWarnings).mockResolvedValue({
      lineage_warnings: [
        {
          consumer_state_id: 1,
          warning_kind: "source_gap",
          message: "No source state covers 2015.",
        },
      ],
    } as unknown as LineageWarningsResponse);

    await render(LineageDetails, { fqidPath: "scb/lisa/kon", node: node() });

    await expect
      .element(page.getByRole("heading", { name: "Lineage warnings" }))
      .toBeVisible();
    await expect
      .element(page.getByText("No source state covers 2015."))
      .toBeVisible();
  });

  it("keeps the warnings section visible (no compact line) when the fetch errors", async () => {
    // The dangerous false negative: an ERRORED fetched arm must keep its section
    // visible (with the error) — never collapse into the compact "no links" line,
    // which would read as a confirmed absence.
    vi.mocked(getBindingLineageWarnings).mockRejectedValue(
      new Error("backend down"),
    );
    await render(LineageDetails, { fqidPath: "scb/lisa/kon", node: node() });

    await expect
      .element(page.getByText(/Failed to load lineage warnings/))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Lineage warnings" }))
      .toBeVisible();
    await expect
      .element(page.getByText("No provenance or lineage warnings."))
      .not.toBeInTheDocument();
  });
});
