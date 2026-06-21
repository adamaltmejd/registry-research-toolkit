import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import SubjectView from "./SubjectView.svelte";

// The unified catalog-SUBJECT shell (#638 PR1): a thin presentational layout. The
// contract this guards is (1) the title/fqid header, (2) the FIXED section order
// (description → picker → value set → relationships → docs), and (3) omit-when-
// absent (a section with no snippet renders nothing — no empty wrapper). Sections
// are passed as raw snippets, each a uniquely-marked sentinel so DOM order asserts
// the canonical order.

/** A snippet rendering one marked sentinel <p> so we can assert presence + order. */
function marker(id: string) {
  return createRawSnippet(() => ({
    render: () => `<p data-testid="${id}">${id}</p>`,
  }));
}

describe("SubjectView (#638 shell)", () => {
  it("renders the title and fqid header", async () => {
    await render(SubjectView, {
      title: "Kön",
      fqid: "scb/lisa/kon",
    });
    await expect
      .element(page.getByRole("heading", { name: "Kön", level: 2 }))
      .toBeVisible();
    await expect.element(page.getByText("scb/lisa/kon")).toBeVisible();
  });

  it("omits the fqid line entirely when no fqid is given", async () => {
    const screen = await render(SubjectView, { title: "Inkomst" });
    await expect
      .element(page.getByRole("heading", { name: "Inkomst", level: 2 }))
      .toBeVisible();
    // No fqid prop → no .fqid paragraph at all (a concept group has no single fqid).
    expect(screen.container.querySelector(".fqid")).toBeNull();
  });

  it("renders the sections in the canonical order when all are provided", async () => {
    const screen = await render(SubjectView, {
      title: "Kön",
      fqid: "scb/lisa/kon",
      // Pass DELIBERATELY out of declaration order — the shell must still render
      // them in the canonical order, not in prop order.
      docs: marker("docs"),
      relationships: marker("relationships"),
      valueSet: marker("value-set"),
      picker: marker("picker"),
      description: marker("description"),
    });

    const order = [...screen.container.querySelectorAll("[data-testid]")].map(
      (el) => el.getAttribute("data-testid"),
    );
    expect(order).toEqual([
      "description",
      "picker",
      "value-set",
      "relationships",
      "docs",
    ]);
  });

  it("renders nothing for an omitted section (no empty wrapper)", async () => {
    const screen = await render(SubjectView, {
      title: "Kön",
      fqid: "scb/lisa/kon",
      // Only the first and last sections — the three middle ones are omitted.
      description: marker("description"),
      docs: marker("docs"),
    });

    const order = [...screen.container.querySelectorAll("[data-testid]")].map(
      (el) => el.getAttribute("data-testid"),
    );
    // Exactly the two provided sections, in canonical order — nothing rendered for
    // picker / value set / relationships.
    expect(order).toEqual(["description", "docs"]);
    expect(screen.container.querySelector('[data-testid="picker"]')).toBeNull();
    expect(
      screen.container.querySelector('[data-testid="value-set"]'),
    ).toBeNull();
    expect(
      screen.container.querySelector('[data-testid="relationships"]'),
    ).toBeNull();
  });
});
