import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import Panel from "./Panel.svelte";

// Panel: the load-bearing hooks are the micro-label header (a heading carrying
// the title), the optional meta slot, and the body. A string OR snippet title.
function body(text: string) {
  return createRawSnippet(() => ({ render: () => `<p>${text}</p>` }));
}

describe("Panel", () => {
  it("renders a string title as the header and the body", async () => {
    await render(Panel, { title: "Value set", children: body("contents") });
    await expect
      .element(page.getByRole("heading", { name: "Value set" }))
      .toBeVisible();
    await expect.element(page.getByText("contents")).toBeVisible();
  });

  it("renders a snippet title", async () => {
    const title = createRawSnippet(() => ({
      render: () => "<span>scb/lisa/kon</span>",
    }));
    await render(Panel, { title, children: body("x") });
    await expect.element(page.getByText("scb/lisa/kon")).toBeVisible();
  });

  it("renders the meta slot when provided", async () => {
    const meta = createRawSnippet(() => ({
      render: () => "<span>12 codes</span>",
    }));
    const { container } = render(Panel, {
      title: "Codes",
      meta,
      children: body("x"),
    });
    expect(container.querySelector(".panel-meta")).not.toBeNull();
    await expect.element(page.getByText("12 codes")).toBeVisible();
  });

  it("omits the meta wrapper when absent", async () => {
    const { container } = render(Panel, {
      title: "Codes",
      children: body("x"),
    });
    expect(container.querySelector(".panel-meta")).toBeNull();
  });

  it("can render a flush body for integrated lists", () => {
    const { container } = render(Panel, {
      title: "Variables",
      flush: true,
      children: body("x"),
    });
    const panelBody = container.querySelector<HTMLElement>(".panel-body");
    expect(panelBody?.classList.contains("flush")).toBe(true);
    expect(panelBody ? getComputedStyle(panelBody).paddingTop : null).toBe(
      "0px",
    );
  });
});
