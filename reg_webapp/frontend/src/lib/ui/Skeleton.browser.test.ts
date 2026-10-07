import { describe, expect, it } from "vitest";
import { render } from "vitest-browser-svelte";
import Skeleton from "./Skeleton.svelte";

// Skeleton: the visual is aria-hidden (loading semantics belong to the
// container).
describe("Skeleton", () => {
  it("hides the placeholder from the a11y tree", async () => {
    const { container } = await render(Skeleton, {});
    expect(container.querySelector(".skeleton-stack")).toHaveAttribute(
      "aria-hidden",
      "true",
    );
  });
});
