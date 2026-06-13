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

  it("refetches (re-invokes fn) when a tracked reactive input changes", async () => {
    // Pins the dependency-tracking contract: `fn` must be called SYNCHRONOUSLY
    // in the effect so the reactive values it reads are tracked. If `fn` were
    // deferred to a microtask, the effect would track nothing and never refetch
    // on input change (the A5.3b period-refetch break). Count the invocations.
    let key = $state(1);
    let calls = 0;
    let res!: ReturnType<typeof asyncResource<number>>;
    const stop = $effect.root(() => {
      res = asyncResource(() => {
        calls++;
        return Promise.resolve(key);
      });
    });
    flushSync();
    await vi.waitFor(() => expect(res.data).toBe(1));
    expect(calls).toBe(1);

    key = 2;
    flushSync();
    await vi.waitFor(() => expect(res.data).toBe(2)); // refetched with the new input
    expect(calls).toBe(2);
    stop();
  });

  it("aborts the signal on teardown (input change), and a fn that ignores it still works (backward compat)", async () => {
    // The two halves of the abort contract: (1) the signal handed to `fn` is
    // aborted when the effect re-runs / unmounts, so a signal-aware fetch is
    // cancelled in flight; (2) a `fn` that IGNORES the signal (every browse
    // caller) is unaffected — it still resolves and exposes data.
    let key = $state(1);
    const seenSignals: AbortSignal[] = [];
    let res!: ReturnType<typeof asyncResource<number>>;
    const stop = $effect.root(() => {
      res = asyncResource((signal) => {
        seenSignals.push(signal);
        return Promise.resolve(key); // ignores the signal — backward compat
      });
    });
    flushSync();
    await vi.waitFor(() => expect(res.data).toBe(1));
    expect(seenSignals[0].aborted).toBe(false);

    key = 2; // input change → effect re-runs → run 1's signal must abort
    flushSync();
    // The teardown aborts run 1's signal; poll rather than asserting synchronously
    // (the re-run + teardown settle across the flush, not strictly inline).
    await vi.waitFor(() => expect(seenSignals[0].aborted).toBe(true));
    expect(seenSignals[1].aborted).toBe(false); // run 2's fresh signal stays open
    await vi.waitFor(() => expect(res.data).toBe(2)); // ignore-signal fn still resolves
    expect(res.error).toBeNull(); // a teardown-abort never surfaces as an error
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
