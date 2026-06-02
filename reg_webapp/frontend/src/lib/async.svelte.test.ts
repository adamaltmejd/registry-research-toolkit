import { flushSync } from "svelte";
import { describe, expect, it, vi } from "vitest";
import { ApiError } from "./api";
import { asyncResource } from "./async.svelte";

// `asyncResource` registers an `$effect`, so drive it inside an `$effect.root`
// scope (the Svelte 5 way to run effects outside a component) and `flushSync()`
// to fire the effect. `vi.waitFor` polls the getter until the promise settles —
// robust against the microtask timing rather than a brittle fixed tick.

describe("asyncResource", () => {
  it("starts loading, then exposes the resolved data", async () => {
    let res!: ReturnType<typeof asyncResource<number>>;
    const stop = $effect.root(() => {
      res = asyncResource(() => Promise.resolve(42));
    });
    flushSync();
    expect(res.loading).toBe(true);
    await vi.waitFor(() => expect(res.loading).toBe(false));
    expect(res.data).toBe(42);
    expect(res.error).toBeNull();
    expect(res.status).toBeNull();
    stop();
  });

  it("maps an ApiError to its message and HTTP status", async () => {
    let res!: ReturnType<typeof asyncResource<number>>;
    const stop = $effect.root(() => {
      res = asyncResource(() =>
        Promise.reject(new ApiError(404, null, "not found")),
      );
    });
    flushSync();
    await vi.waitFor(() => expect(res.loading).toBe(false));
    expect(res.data).toBeNull();
    expect(res.error).toBe("not found");
    expect(res.status).toBe(404);
    stop();
  });

  it("stringifies a non-ApiError rejection", async () => {
    let res!: ReturnType<typeof asyncResource<number>>;
    const stop = $effect.root(() => {
      res = asyncResource(() => Promise.reject(new Error("boom")));
    });
    flushSync();
    await vi.waitFor(() => expect(res.loading).toBe(false));
    expect(res.error).toContain("boom");
    expect(res.status).toBeNull();
    stop();
  });
});
