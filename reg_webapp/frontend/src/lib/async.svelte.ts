/**
 * Reactive async resource — the SPA's fetch-on-input pattern in one tested place.
 *
 * Wraps the `{loading, error, data}` triple plus the `$effect` lifecycle that the
 * browse components (and A5.3b's states/lineage views) all need. `fn` is
 * re-invoked whenever a reactive value it READS changes (Svelte 5 effect
 * dependency tracking), so `asyncResource(() => getNode(fqidPath))` refetches
 * when `fqidPath` changes. The in-flight fetch is cancelled on input change /
 * unmount (the `$effect` teardown flips `cancelled`), so a response that arrives
 * after its inputs moved on never clobbers fresher state.
 *
 * Must be called at component init (it registers an `$effect`). `.svelte.ts` so
 * the Svelte compiler processes the runes.
 */
import { ApiError } from "./api";

export interface AsyncResource<T> {
  /** The resolved value, or `null` while loading / on error. */
  readonly data: T | null;
  /** A human-readable error message, or `null`. */
  readonly error: string | null;
  /** The HTTP status when the error was an `ApiError` (e.g. 404), else `null`. */
  readonly status: number | null;
  readonly loading: boolean;
}

export function asyncResource<T>(fn: () => Promise<T>): AsyncResource<T> {
  let data = $state<T | null>(null);
  let error = $state<string | null>(null);
  let status = $state<number | null>(null);
  let loading = $state(true);

  $effect(() => {
    let cancelled = false;
    loading = true;
    error = null;
    status = null;
    data = null;
    const setError = (e: unknown) => {
      if (cancelled) return;
      if (e instanceof ApiError) {
        error = e.message;
        status = e.status;
      } else {
        error = String(e);
      }
      loading = false;
    };
    // Call `fn()` SYNCHRONOUSLY here (not in a `.then`) so the reactive values it
    // reads are tracked as this effect's dependencies — Svelte only tracks reads
    // during synchronous effect execution. Deferring `fn` to a microtask would
    // silently break refetch-on-input (e.g. A5.3b's period change). The try/catch
    // funnels a SYNCHRONOUS throw in `fn` into the same error path (so it can't
    // escape the effect and wedge `loading` at true).
    try {
      fn()
        .then((resp) => {
          if (!cancelled) data = resp;
        })
        .catch(setError)
        .finally(() => {
          if (!cancelled) loading = false;
        });
    } catch (e) {
      setError(e);
    }
    return () => {
      cancelled = true;
    };
  });

  return {
    get data() {
      return data;
    },
    get error() {
      return error;
    },
    get status() {
      return status;
    },
    get loading() {
      return loading;
    },
  };
}
