import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ClassificationNodeData,
  ClassificationPredecessorsResponse,
} from "./api";
import { getClassificationPredecessors } from "./api";
import ClassificationLineagePanels from "./ClassificationLineagePanels.svelte";

// Stub the FETCHED inbound arm (`/classification_predecessors`); the outbound arm
// (`replaced_by`) rides on the `node` prop and needs no mock. Mirrors the variable
// LineagePanels suite.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getClassificationPredecessors: vi.fn(),
  };
});

// A classification node with NO succession by default; override per case.
function node(
  over: Partial<ClassificationNodeData> = {},
): ClassificationNodeData {
  return {
    kind: "classification",
    fqid: "class/sun2020",
    name: "SUN 2020",
    short_name: "SUN",
    replaced_by: [],
    ...over,
  } as unknown as ClassificationNodeData;
}

beforeEach(() => {
  vi.mocked(getClassificationPredecessors).mockReset();
  // Default: the inbound arm resolves EMPTY.
  vi.mocked(getClassificationPredecessors).mockResolvedValue({
    classification: "class/sun2020",
    predecessors: [],
  } as unknown as ClassificationPredecessorsResponse);
});

describe("ClassificationLineagePanels — edition chain (#571)", () => {
  it("omits the panel entirely for a standalone classification (no succession either side)", async () => {
    await render(ClassificationLineagePanels, {
      fqidPath: "class/sun2020",
      node: node(),
    });

    // No Editions heading, no current-edition marker — the panel renders nothing.
    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .not.toBeInTheDocument();
    await expect
      .element(page.getByText("(current edition)"))
      .not.toBeInTheDocument();
  });

  it("renders the panel (current edition marked in place) when replaced_by exists", async () => {
    await render(ClassificationLineagePanels, {
      fqidPath: "class/sun2000",
      node: node({
        fqid: "class/sun2000",
        name: "SUN 2000",
        replaced_by: [
          {
            slug: "sun2020",
            fqid: "class/sun2020",
            effective_year: 2020,
            note: "derived:vintage_chain",
          },
        ] as unknown as ClassificationNodeData["replaced_by"],
      }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    // The current edition is marked in place (a non-link node).
    await expect.element(page.getByText("(current edition)")).toBeVisible();
    // …and the successor edition links to its class/<slug>.
    await expect
      .element(page.getByRole("link", { name: "sun2020" }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("composes ONE chain ordered by effective_year: predecessors → this edition → successors", async () => {
    // Two predecessors (1996 + an undated one, nulls last); one successor (2020).
    // The chain must read top-to-bottom in period order, THIS edition between the
    // arms. The predecessors live in a collapsed <details> — open it to read them.
    vi.mocked(getClassificationPredecessors).mockResolvedValue({
      classification: "class/sun2000",
      predecessors: [
        {
          slug: "sun-undated",
          fqid: "class/sun-undated",
          effective_year: null,
        },
        { slug: "sun1996", fqid: "class/sun1996", effective_year: 1996 },
      ],
    } as unknown as ClassificationPredecessorsResponse);

    await render(ClassificationLineagePanels, {
      fqidPath: "class/sun2000",
      node: node({
        fqid: "class/sun2000",
        name: "SUN 2000",
        replaced_by: [
          { slug: "sun2020", fqid: "class/sun2020", effective_year: 2020 },
        ] as unknown as ClassificationNodeData["replaced_by"],
      }),
    });

    // The "earlier editions" disclosure surfaces the predecessor count, collapsed.
    await expect.element(page.getByText("2 earlier editions")).toBeVisible();
    await page.getByText("2 earlier editions").click();

    // Read the chain's LEAF rows top-to-bottom (the `.chain-node` <li>s — NOT the
    // `.chain-history` disclosure wrapper, whose own descendant anchors would
    // double-count the predecessor links). Each leaf yields its link href, or its
    // <code> fqid for the non-link current edition.
    const nodes = Array.from(
      document.querySelectorAll<HTMLElement>(".chain-node"),
    );
    const order = nodes.map((li) => {
      const a = li.querySelector("a");
      return a
        ? (a.getAttribute("href") ?? "")
        : (li.querySelector("code")?.textContent ?? "");
    });
    // Predecessors first (1996 before the undated null, nulls last), then THIS
    // edition (its bare fqid, non-link), then the 2020 successor.
    expect(order).toEqual([
      "/catalog/class/sun1996",
      "/catalog/class/sun-undated",
      "class/sun2000",
      "/catalog/class/sun2020",
    ]);

    // The current edition is a non-link node (it carries the marker text).
    const currentLi = nodes.find((li) =>
      li.textContent?.includes("(current edition)"),
    );
    expect(
      currentLi,
      "no chain-node held the current-edition marker",
    ).toBeTruthy();
    expect(currentLi?.querySelector("a")).toBeNull();
  });

  it("renders the panel (with the loading note) while predecessors are still fetching, even with empty replaced_by", async () => {
    // A terminal edition (empty replaced_by) whose inbound fetch is in flight must
    // still SHOW the panel — hiding a panel whose inbound state is unknown would
    // read as a confirmed standalone. A never-settling fetch keeps it loading.
    vi.mocked(getClassificationPredecessors).mockReturnValue(
      new Promise<ClassificationPredecessorsResponse>(() => {}),
    );

    await render(ClassificationLineagePanels, {
      fqidPath: "class/sun2020",
      node: node({ replaced_by: [] }),
    });

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    await expect
      .element(page.getByText("Loading earlier editions…"))
      .toBeVisible();
  });

  it("keeps the panel visible (with the error) when the predecessors fetch errors", async () => {
    // Regression guard for the dangerous false negative: an errored inbound arm
    // must keep the panel visible (with the error) — never collapse into nothing,
    // which would read as a confirmed standalone classification.
    vi.mocked(getClassificationPredecessors).mockRejectedValue(
      new Error("backend down"),
    );
    await render(ClassificationLineagePanels, {
      fqidPath: "class/sun2020",
      node: node({ replaced_by: [] }),
    });

    await expect
      .element(page.getByText(/Failed to load earlier editions/))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
  });

  it("renders a dead predecessor edition (null fqid) as plain text, not a link", async () => {
    // Succession tolerates dead editions (null fqid): the slug still shows, but it
    // must NOT be a link.
    vi.mocked(getClassificationPredecessors).mockResolvedValue({
      classification: "class/sun2000",
      predecessors: [{ slug: "sun-dead", fqid: null, effective_year: 1990 }],
    } as unknown as ClassificationPredecessorsResponse);

    await render(ClassificationLineagePanels, {
      fqidPath: "class/sun2000",
      node: node({
        fqid: "class/sun2000",
        replaced_by: [
          { slug: "sun2020", fqid: "class/sun2020", effective_year: 2020 },
        ] as unknown as ClassificationNodeData["replaced_by"],
      }),
    });

    await page.getByText("1 earlier edition").click();
    await expect.element(page.getByText("sun-dead")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "sun-dead" }))
      .not.toBeInTheDocument();
  });
});
