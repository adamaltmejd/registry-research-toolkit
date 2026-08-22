import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import Button from "./Button.svelte";

// Button: the contract is (1) renders a real <button>, (2) switches to an <a>
// when href is set (Bits UI polymorphism), (3) the variant/size class hooks,
// (4) onclick fires, (5) disabled blocks activation.
function label(text: string) {
  return createRawSnippet(() => ({ render: () => `<span>${text}</span>` }));
}

describe("Button", () => {
  it("renders a button with the variant + size class", async () => {
    const { container } = await render(Button, {
      variant: "primary",
      size: "sm",
      children: label("Add"),
    });
    const btn = container.querySelector("button");
    expect(btn).not.toBeNull();
    expect(btn).toHaveClass("variant-primary");
    expect(btn).toHaveClass("size-sm");
  });

  it("renders an anchor when href is set", async () => {
    const { container } = await render(Button, {
      href: "/catalog",
      children: label("Browse"),
    });
    const anchor = container.querySelector("a");
    expect(anchor).not.toBeNull();
    expect(anchor).toHaveAttribute("href", "/catalog");
    expect(container.querySelector("button")).toBeNull();
  });

  it("fires onclick", async () => {
    let clicked = 0;
    await render(Button, {
      onclick: () => {
        clicked += 1;
      },
      children: label("Go"),
    });
    await page.getByRole("button", { name: "Go" }).click();
    expect(clicked).toBe(1);
  });

  it("does not fire onclick when disabled", async () => {
    let clicked = 0;
    const { container } = await render(Button, {
      disabled: true,
      onclick: () => {
        clicked += 1;
      },
      children: label("Go"),
    });
    const btn = container.querySelector("button");
    expect(btn).toBeDisabled();
    // A disabled button swallows the click — assert no handler ran.
    btn?.click();
    expect(clicked).toBe(0);
  });

  it("forwards native attributes onto the element (Fix 3)", async () => {
    // Migrated callers need aria-label (icon buttons), title, etc. — `...rest`
    // spreads them onto Bits UI's Button.Root.
    const { container } = await render(Button, {
      "aria-label": "Close panel",
      title: "Close",
      children: label("×"),
    });
    const btn = container.querySelector("button");
    expect(btn).toHaveAttribute("aria-label", "Close panel");
    expect(btn).toHaveAttribute("title", "Close");
  });

  it("merges a caller class with the variant/size hook (Fix 3)", async () => {
    const { container } = await render(Button, {
      variant: "ghost",
      class: "my-layout",
      children: label("Go"),
    });
    const btn = container.querySelector("button");
    // Both the caller class AND the foundational ui-btn hook survive.
    expect(btn).toHaveClass("my-layout");
    expect(btn).toHaveClass("ui-btn");
    expect(btn).toHaveClass("variant-ghost");
  });
});
