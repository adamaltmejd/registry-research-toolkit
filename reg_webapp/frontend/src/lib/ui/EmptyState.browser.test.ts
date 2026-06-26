import { createRawSnippet } from "svelte";
import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import EmptyState from "./EmptyState.svelte";

// EmptyState: the contract is (1) the title + optional description render,
// (2) the optional action snippet renders, (3) absent optionals leave no
// wrapper.
describe("EmptyState", () => {
  it("renders the title and description", async () => {
    await render(EmptyState, {
      title: "No results",
      description: "Try a broader query.",
    });
    await expect.element(page.getByText("No results")).toBeVisible();
    await expect.element(page.getByText("Try a broader query.")).toBeVisible();
  });

  it("omits the description when absent", async () => {
    const { container } = render(EmptyState, { title: "Empty" });
    expect(container.querySelector(".description")).toBeNull();
  });

  it("renders the action snippet", async () => {
    const action = createRawSnippet(() => ({
      render: () => "<button>Reset</button>",
    }));
    const { container } = render(EmptyState, { title: "Empty", action });
    expect(container.querySelector(".action")).not.toBeNull();
    await expect
      .element(page.getByRole("button", { name: "Reset" }))
      .toBeVisible();
  });
});
