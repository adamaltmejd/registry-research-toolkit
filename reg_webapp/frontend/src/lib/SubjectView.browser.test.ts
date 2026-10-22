import { describe, expect, it } from "vite-plus/test";
import { page } from "vite-plus/test/browser";
import { render } from "vitest-browser-svelte";
import SubjectView from "./SubjectView.svelte";

// The unified catalog-SUBJECT shell (#638 PR1): a thin presentational layout. Its
// title and fqid header are read through its consumers (ClassificationLeafView,
// ConceptGroupView); this file keeps the binding leaf's fqid opt-out.

describe("SubjectView (#638 shell)", () => {
  it("omits the fqid line when showFqid=false even with an fqid (#670 binding leaf opt-out)", async () => {
    // The binding leaf passes showFqid=false — its breadcrumb already ends in the
    // slug, so the under-header fqid line is redundant (M12). The fqid prop is
    // still present (used for the docs/dimensions wiring), but the line is dropped.
    const screen = await render(SubjectView, {
      title: "Näringsgren, största förvärvskälla",
      fqid: "scb/lisa/naringsgren-storsta-agi-sni2007g",
      showFqid: false,
    });
    await expect
      .element(
        page.getByRole("heading", {
          name: "Näringsgren, största förvärvskälla",
          level: 2,
        }),
      )
      .toBeVisible();
    expect(screen.container.querySelector(".fqid")).toBeNull();
  });
});
