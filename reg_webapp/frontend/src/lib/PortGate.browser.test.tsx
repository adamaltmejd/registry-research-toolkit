import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-react/pure";
import { PortGate } from "./PortGate";

// Fails if the browser project stops compiling .tsx through plugin-react, or if
// React state updates do not reach the DOM in the iframe the Svelte tests share.
describe("PortGate", () => {
  it("renders a React toggle button that updates on click", async () => {
    await render(<PortGate label="Gate" />);
    const button = page.getByRole("button", { name: "Gate" });
    await expect.element(button).toHaveAttribute("aria-pressed", "false");
    await button.click();
    await expect.element(button).toHaveAttribute("aria-pressed", "true");
  });
});
