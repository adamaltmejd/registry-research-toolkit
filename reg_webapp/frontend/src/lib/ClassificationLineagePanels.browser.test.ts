import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationChainEdition, ClassificationNodeData } from "./api";
import ClassificationLineagePanels from "./ClassificationLineagePanels.svelte";

// The panel renders the EMBEDDED `edition_chain` synchronously — no fetch, so no
// mocking. The chain arrives oldest→current; each edition carries `is_self` (the
// viewed edition) and `is_current` (the terminal/latest edition).

// `fqid`/`name` default OFF the slug so editions in one chain are internally
// consistent (distinct slugs → distinct fqids → distinct `#each` keys); override
// `fqid: null` to exercise the component's null-guard (missing/unresolvable fqid).
function edition(
  over: Partial<ClassificationChainEdition> & { slug: string },
): ClassificationChainEdition {
  return {
    fqid: `class/${over.slug}`,
    name: over.slug.toUpperCase(),
    effective_year: 2000,
    version_year: 2000,
    is_current: false,
    is_self: false,
    ...over,
  };
}

// A classification node whose `edition_chain` is the given chain (oldest first).
function node(chain: ClassificationChainEdition[]): ClassificationNodeData {
  return {
    kind: "classification",
    fqid: "class/sun2000",
    name: "SUN 2000",
    short_name: "SUN",
    edition_chain: chain,
  } as unknown as ClassificationNodeData;
}

describe("ClassificationLineagePanels — embedded edition chain (#571)", () => {
  it("omits the panel entirely for a standalone classification (a 1-element chain)", async () => {
    // A standalone classification is its own self + current — a single edition.
    await render(ClassificationLineagePanels, {
      node: node([
        edition({ slug: "sun2000", is_self: true, is_current: true }),
      ]),
    });

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .not.toBeInTheDocument();
  });

  it("renders the chain with the viewed edition marked and the latest labeled current", async () => {
    // Viewing sun2000 with a single successor sun2020 (the current edition).
    await render(ClassificationLineagePanels, {
      node: node([
        edition({
          slug: "sun2000",
          fqid: "class/sun2000",
          name: "SUN 2000",
          effective_year: 2000,
          is_self: true,
        }),
        edition({
          slug: "sun2020",
          fqid: "class/sun2020",
          name: "SUN 2020",
          effective_year: 2020,
          is_current: true,
        }),
      ]),
    });

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    // The viewed edition is marked "you are here".
    await expect.element(page.getByText("you are here")).toBeVisible();
    // The terminal edition is labeled "current edition" and links to its node.
    await expect.element(page.getByText("current edition")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "SUN 2020" }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("marks one node both 'you are here' and 'current' when viewing the latest edition", async () => {
    // Viewing the terminal edition: a single node carries BOTH flags.
    await render(ClassificationLineagePanels, {
      node: node([
        edition({ slug: "sun2000", name: "SUN 2000", effective_year: 2000 }),
        edition({
          slug: "sun2020",
          name: "SUN 2020",
          effective_year: 2020,
          is_self: true,
          is_current: true,
        }),
      ]),
    });

    const current = document.querySelector<HTMLElement>(".chain-node.marked");
    expect(current?.textContent).toContain("you are here");
    expect(current?.textContent).toContain("current edition");
    // Only ONE marked node (the single self+current edition); the older edition
    // is the always-visible non-marked head of the chain.
    expect(document.querySelectorAll(".chain-node.marked")).toHaveLength(1);
  });

  it("renders an edition with a missing/unresolvable fqid (null) as plain text, not a link", async () => {
    // The build validator guarantees succession editions are live rows, so this
    // null-fqid shape won't occur in a validated DB — it exercises the component's
    // generic null-guard on the optional wire field.
    await render(ClassificationLineagePanels, {
      node: node([
        edition({
          slug: "sun-nolink",
          fqid: null,
          name: null,
          effective_year: 1990,
          is_self: true,
        }),
        edition({
          slug: "sun2020",
          fqid: "class/sun2020",
          name: "SUN 2020",
          effective_year: 2020,
          is_current: true,
        }),
      ]),
    });

    // The edition's slug shows (it has no name) but is NOT a link.
    await expect.element(page.getByText("sun-nolink")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "sun-nolink" }))
      .not.toBeInTheDocument();
  });

  it("collapses a long chain: the viewed + current editions stay visible, the rest hide until expanded", async () => {
    // A 5-edition chain viewed at the 3rd: editions 1–2 collapse into "2 earlier
    // editions", edition 4 into "1 later edition", and the viewed (3rd) + the
    // current (5th) stay always-visible.
    await render(ClassificationLineagePanels, {
      node: node([
        edition({ slug: "lkf1980", name: "LKF 1980", effective_year: 1980 }),
        edition({ slug: "lkf1990", name: "LKF 1990", effective_year: 1990 }),
        edition({
          slug: "lkf2000",
          name: "LKF 2000",
          effective_year: 2000,
          is_self: true,
        }),
        edition({ slug: "lkf2010", name: "LKF 2010", effective_year: 2010 }),
        edition({
          slug: "lkf2020",
          name: "LKF 2020",
          effective_year: 2020,
          is_current: true,
        }),
      ]),
    });

    // The viewed + current editions render WITHOUT expanding.
    await expect
      .element(page.getByRole("link", { name: "LKF 2000" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "LKF 2020" }))
      .toBeVisible();

    // The collapsed runs surface their counts; their editions are hidden in a
    // closed <details>.
    await expect.element(page.getByText("2 earlier editions")).toBeVisible();
    await expect.element(page.getByText("1 later edition")).toBeVisible();
    expect(
      document.querySelector<HTMLDetailsElement>("details")?.open,
    ).toBeFalsy();

    // Expanding the "earlier" disclosure reveals the older editions.
    await page.getByText("2 earlier editions").click();
    await expect
      .element(page.getByRole("link", { name: "LKF 1980" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "LKF 1990" }))
      .toBeVisible();
  });

  it("renders a SPLIT root flat with every branch tip marked current (#605)", async () => {
    // #605/#579: browsing the split ROOT (sun1996) fans out into three branches
    // (niva/inriktning/grupp), each ending in a 2020 tip — MULTIPLE is_current
    // editions. The linear collapse can't represent a fan-out, so the panel renders
    // the whole closure FLAT: every branch edition is visible WITHOUT expanding any
    // disclosure, and each terminal is tagged "current edition".
    await render(ClassificationLineagePanels, {
      node: node([
        edition({
          slug: "sun1996",
          name: "SUN 1996",
          effective_year: 2000,
          is_self: true,
        }),
        edition({ slug: "sun-grupp2000", name: "SUN grupp 2000" }),
        edition({
          slug: "sun-grupp2020",
          name: "SUN grupp 2020",
          effective_year: 2020,
          is_current: true,
        }),
        edition({ slug: "sun-inriktning2000", name: "SUN inriktning 2000" }),
        edition({
          slug: "sun-inriktning2020",
          name: "SUN inriktning 2020",
          effective_year: 2020,
          is_current: true,
        }),
        edition({ slug: "sun-niva2000", name: "SUN niva 2000" }),
        edition({
          slug: "sun-niva2020",
          name: "SUN niva 2020",
          effective_year: 2020,
          is_current: true,
        }),
      ]),
    });

    await expect
      .element(page.getByRole("heading", { name: "Editions" }))
      .toBeVisible();
    // The fan-out renders FLAT — no <details> collapse, so EVERY branch tip is
    // visible without expanding.
    expect(document.querySelector("details")).toBeNull();
    for (const tip of [
      "SUN grupp 2020",
      "SUN inriktning 2020",
      "SUN niva 2020",
    ]) {
      await expect.element(page.getByRole("link", { name: tip })).toBeVisible();
    }
    // The viewed root + all three terminal tips are marked (filled marker): 4 nodes.
    expect(document.querySelectorAll(".chain-node.marked")).toHaveLength(4);
    // All three tips carry a "current edition" tag; the viewed root "you are here".
    const tags = [...document.querySelectorAll(".tag")].map(
      (t) => t.textContent,
    );
    expect(tags.filter((t) => t === "current edition")).toHaveLength(3);
    await expect.element(page.getByText("you are here")).toBeVisible();
  });
});
