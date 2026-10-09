import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { LineageModel, VariableShow } from "./api";
import { getLineage } from "./api";
import LineageDetails from "./LineageDetails.svelte";

// LineageDetails (#678) re-homes the two NON-graph affordances off the retired
// LineagePanels: PROVENANCE (the `lineage` read's edges + the variable's source
// register) and the lineage warnings from the same read. Succession is NOT here —
// it's a graph edge now (the picker graph mode). These cover: omit-when-empty,
// the provenance list, the source-register line, the warnings section, and the
// read's one failure domain.

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getLineage: vi.fn() };
});

function node(over: Partial<VariableShow> = {}): VariableShow {
  return {
    kind: "variable",
    fqid: "scb/lisa/kon",
    name: "Kön",
    deprecated: false,
    is_identifier: false,
    is_sensitive: false,
    source_register_text: null,
    same_as: [],
    tags: [],
    ...over,
  };
}

function lineage(over: Partial<LineageModel> = {}): LineageModel {
  return { edges: [], warnings: [], registers: [], ...over };
}

beforeEach(() => {
  vi.mocked(getLineage).mockReset();
  // Default: the lineage read resolves EMPTY.
  vi.mocked(getLineage).mockResolvedValue(lineage());
});

describe("LineageDetails — omit-when-empty (#678)", () => {
  it("renders nothing when provenance + warnings are empty", async () => {
    const screen = await render(LineageDetails, { node: node() });

    for (const heading of ["Provenance", "Lineage warnings"]) {
      await expect
        .element(page.getByRole("heading", { name: heading }))
        .not.toBeInTheDocument();
    }
    expect(screen.container.querySelector(".lineage-details")).toBeNull();
  });
});

describe("LineageDetails — provenance", () => {
  it("renders the read's consumer/source edges with a window + source link", async () => {
    // Fails if the edges stop coming from the variable's `lineage` read.
    vi.mocked(getLineage).mockResolvedValue(
      lineage({
        edges: [
          {
            consumer_state_id: "1",
            source_state_id: "2",
            valid_from: "2005-01-01",
            valid_to: "2010-12-31",
            source_fqid: "scb/rtb/kon",
          },
        ],
      }),
    );
    await render(LineageDetails, { node: node() });

    expect(getLineage).toHaveBeenCalledWith("scb/lisa/kon");
    await expect
      .element(page.getByRole("heading", { name: "Provenance" }))
      .toBeVisible();
    await expect.element(page.getByText("2005 – 2010")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "scb/rtb/kon" }))
      .toBeVisible();
  });

  it("falls back to 'source state #N' when a lineage edge has no source_fqid", async () => {
    vi.mocked(getLineage).mockResolvedValue(
      lineage({
        edges: [
          {
            consumer_state_id: "1",
            source_state_id: "7",
            valid_from: "2005-01-01",
            valid_to: "2010-12-31",
            source_fqid: null,
          },
        ],
      }),
    );
    await render(LineageDetails, { node: node() });
    await expect.element(page.getByText("source state #7")).toBeVisible();
  });

  it("surfaces the variable's source register as a compact line", async () => {
    await render(LineageDetails, {
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

describe("LineageDetails — warnings (the read's failure domain)", () => {
  it("renders the warnings section when the read returns warnings", async () => {
    vi.mocked(getLineage).mockResolvedValue(
      lineage({
        warnings: [
          {
            consumer_state_id: "1",
            warning_kind: "no_source_state",
            message: "No source state covers 2015.",
          },
        ],
      }),
    );

    await render(LineageDetails, { node: node() });

    await expect
      .element(page.getByRole("heading", { name: "Lineage warnings" }))
      .toBeVisible();
    await expect
      .element(page.getByText("No source state covers 2015."))
      .toBeVisible();
  });

  it("keeps the warnings section visible with the error when the read fails", async () => {
    // The dangerous false negative: an ERRORED read must keep its section visible
    // (with the error) — never collapse to nothing, which would read as a
    // confirmed absence. The source-register line from `node` still renders.
    vi.mocked(getLineage).mockRejectedValue(new Error("backend down"));
    await render(LineageDetails, {
      node: node({ source_register_text: "Registret över totalbefolkningen" }),
    });

    await expect
      .element(page.getByText(/Failed to load lineage:.*backend down/))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Lineage warnings" }))
      .toBeVisible();
    await expect
      .element(page.getByText("Registret över totalbefolkningen"))
      .toBeVisible();
  });
});
