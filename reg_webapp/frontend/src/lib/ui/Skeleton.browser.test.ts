import { describe, expect, it } from "vitest";
import { render } from "vitest-browser-svelte";
import Skeleton from "./Skeleton.svelte";

// Skeleton: the contract is (1) the visual is aria-hidden (loading semantics
// belong to the container), (2) `count` repeats the placeholder, (3) the
// variant drives the shape class.
describe("Skeleton", () => {
  it("hides the placeholder from the a11y tree", async () => {
    const { container } = render(Skeleton, {});
    expect(container.querySelector(".skeleton-stack")).toHaveAttribute(
      "aria-hidden",
      "true",
    );
  });

  it("repeats the placeholder `count` times", async () => {
    const { container } = render(Skeleton, { count: 3 });
    expect(container.querySelectorAll(".skeleton")).toHaveLength(3);
  });

  it("applies the variant class", async () => {
    const { container } = render(Skeleton, { variant: "block" });
    expect(container.querySelector(".skeleton")).toHaveClass("block");
  });

  it("applies an explicit width", async () => {
    const { container } = render(Skeleton, { width: "12rem" });
    const el = container.querySelector(".skeleton") as HTMLElement;
    expect(el.style.width).toBe("12rem");
  });
});
