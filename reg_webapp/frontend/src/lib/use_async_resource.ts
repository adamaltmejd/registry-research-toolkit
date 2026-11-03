import { useEffect, useEffectEvent, useState } from "react";
import { ApiError, errMessage } from "./api";

/** One request's outcome. `loading` is true from the moment `key` names a request
 * until that request settles; `data`, `error` and `status` belong to the request
 * `key` names now, never to an earlier one. */
export interface AsyncResource<T> {
  readonly data: T | null;
  /** A human-readable error message, or `null`. */
  readonly error: string | null;
  /** The HTTP status when the error was an `ApiError` (e.g. 404), else `null`. */
  readonly status: number | null;
  readonly loading: boolean;
}

interface Settled<T> {
  key: string;
  data: T | null;
  error: string | null;
  status: number | null;
}

/**
 * The SPA's fetch-on-input hook. `key` names the request (an FQID, a query):
 * `load` runs when the component mounts and again whenever `key` changes, never on
 * a mere re-render. Each run gets its own `AbortController`, aborted when `key`
 * changes or the component unmounts, and a response that arrives after its abort
 * is dropped. `load` may ignore the signal; then only the dropping applies. A
 * `null` key asks for nothing: idle, not loading.
 */
export function useAsyncResource<T>(
  key: string | null,
  load: (signal: AbortSignal) => Promise<T>,
): AsyncResource<T> {
  const [settled, setSettled] = useState<Settled<T> | null>(null);
  // The latest `load`, read when the effect runs: a new closure each render
  // must not refetch, only a new key.
  const run = useEffectEvent(load);

  useEffect(() => {
    if (key === null) {
      return;
    }
    const controller = new AbortController();
    const settle = (outcome: Omit<Settled<T>, "key">) => {
      if (!controller.signal.aborted) {
        setSettled({ key, ...outcome });
      }
    };
    run(controller.signal).then(
      (data) => settle({ data, error: null, status: null }),
      (e: unknown) =>
        settle({
          data: null,
          error: errMessage(e),
          status: e instanceof ApiError ? e.status : null,
        }),
    );
    return () => controller.abort();
  }, [key]);

  const current = settled?.key === key ? settled : null;
  return {
    data: current?.data ?? null,
    error: current?.error ?? null,
    status: current?.status ?? null,
    loading: key !== null && current === null,
  };
}
