import { createRawSnippet } from "svelte";
import { describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import FilterChip from "./FilterChip.svelte";

// FilterChip: the load-bearing parts are the native checkbox behind the chip
// face (keyboard + a11y, kept in the DOM by the `.visually-hidden` utility, never
// `display: none`), the `.on` selection hook, and the toggle callback the host
// filter set listens to.
describe("FilterChip", () => {
  const label = createRawSnippet(() => ({
    render: () => "<span>januari</span>",
  }));

  it("renders its content over a real checkbox", async () => {
    const { container } = await render(FilterChip, {
      onToggle: () => {},
      children: label,
    });
    await expect.element(page.getByText("januari")).toBeVisible();
    const input = container.querySelector<HTMLInputElement>(
      'input[type="checkbox"]',
    );
    expect(input).not.toBeNull();
    // Hidden visually, present to assistive tech + the keyboard.
    expect(input).toHaveClass("visually-hidden");
    expect(input?.checked).toBe(false);
  });

  it("marks the selected chip and checks its box", async () => {
    const { container } = await render(FilterChip, {
      selected: true,
      onToggle: () => {},
      children: label,
    });
    expect(container.querySelector(".ui-chip")).toHaveClass("on");
    expect(
      container.querySelector<HTMLInputElement>('input[type="checkbox"]')
        ?.checked,
    ).toBe(true);
  });

  it("toggles through the checkbox the label owns", async () => {
    const onToggle = vi.fn();
    const { container } = await render(FilterChip, {
      onToggle,
      children: label,
    });
    container.querySelector<HTMLLabelElement>(".ui-chip")?.click();
    expect(onToggle).toHaveBeenCalledTimes(1);
  });
});
