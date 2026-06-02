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

  it("ignores a stale response after its tracked input changed", async () => {
    // The load-bearing guarantee for A5.3b's refetch-on-period: when the input
    // changes mid-flight, the first (now-stale) fetch's late resolution must NOT
    // clobber the fresh state. Drive two deferred fetches via a reactive `key`.
    function deferred() {
      let resolve!: (v: string) => void;
      const promise = new Promise<string>((r) => {
        resolve = r;
      });
      return { promise, resolve };
    }
    const first = deferred();
    const second = deferred();
    let key = $state(1);
    let res!: ReturnType<typeof asyncResource<string>>;
    const stop = $effect.root(() => {
      res = asyncResource(() => (key === 1 ? first.promise : second.promise));
    });
    flushSync(); // effect runs → fn() === first.promise, loading=true

    key = 2; // input changes → effect re-runs (cancels run 1), fn() === second
    flushSync();

    first.resolve("STALE"); // the cancelled run resolves — must be ignored
    await Promise.resolve();
    flushSync();
    expect(res.data).not.toBe("STALE");
    expect(res.loading).toBe(true); // still awaiting the fresh fetch

    second.resolve("FRESH");
    await vi.waitFor(() => expect(res.loading).toBe(false));
    expect(res.data).toBe("FRESH");
    stop();
  });
});
