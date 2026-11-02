import {
  getShow,
  type RegisterShow,
  type RootChild,
  type ShowNode,
} from "./api";

/** One cached catalog read. `value` is null until it lands and STAYS null when it
 * failed; `loading` tells those two apart (the caller's `aria-busy`, and a settled
 * null is a read that had nothing to give). Each entry is REPLACED, never mutated,
 * so a caller holding one can trust its identity: a memo over an unchanged
 * read stops there instead of re-running everything downstream of it. */
export interface Read<T> {
  readonly value: T | null;
  readonly loading: boolean;
}

/** Every read this session, by key, held in ONE object.
 *
 * The two mutations differ: ADDING an in-flight entry writes into the current
 * object and notifies nobody, which is what makes starting a read from a render
 * safe; a read that SETTLES replaces the object and wakes every subscriber —
 * cheap, they are all lookups. */
let reads: Record<string, Read<unknown>> = {};

const listeners = new Set<() => void>();

/** Call `listener` whenever a read settles or the cache is reset. */
export function subscribeCatalogNames(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function notify(): void {
  for (const listener of listeners) listener();
}

/** The shared in-flight entry: a read that has not landed says the same thing
 * whatever it is reading. */
const PENDING: Read<never> = { value: null, loading: true };

/** The shared never-started entry — what a caller with nothing to ask about gets,
 * so it too holds a stable identity. */
export const UNASKED: Read<never> = { value: null, loading: false };

/** Which CACHE the in-flight reads belong to — bumped by every reset, so a read
 * started before one cannot write into the cache that replaced it. */
let generation = 0;

/** The cached read for `key`, STARTING it on first ask. Repeat asks (a re-render,
 * a second card on the same register) get the same entry and issue no request. */
function read<T>(key: string, fetcher: () => Promise<T>): Read<T> {
  const hit = reads[key] as Read<T> | undefined;
  if (hit) {
    return hit;
  }
  reads[key] = PENDING;
  const startedIn = generation;
  const settle = (value: T | null) => {
    // A read the reset abandoned settles into nothing: repopulating the fresh
    // cache with the previous one's answer would hand a case the response the
    // case before it stubbed.
    if (startedIn === generation) {
      reads = { ...reads, [key]: { value, loading: false } };
      notify();
    }
  };
  // A failed name read is not an error surface — see the module comment.
  fetcher().then(settle, () => settle(null));
  return PENDING;
}

/** Drop every cached read, in-flight ones included. The test seam: the cache is a
 * session singleton, so a case that stubs a DIFFERENT catalog response for the
 * same coordinate must start from an empty one. */
export function resetCatalogNames(): void {
  generation += 1;
  reads = {};
  showsInFlight.clear();
  notify();
}

/** `getShow` reads in flight, by ref. The detail page and the browse column of
 * the register it shows ask for the same node at the same moment; this makes that
 * one request. An entry lives only until its read settles, so a failure is not
 * remembered and nothing here outlives the request: what lasts is each reader's
 * own result (the detail page's resource, the `register:` read below).
 *
 * A reader that passes a signal subscribes: its abort rejects its own promise,
 * and once the last subscriber has left, the request itself is aborted, so
 * navigating away cancels a page read nobody is still waiting on. A reader
 * without a signal holds the read until it settles. */
interface InFlight {
  readonly promise: Promise<ShowNode>;
  readonly controller: AbortController;
  /** Readers still waiting; at zero the request is aborted. */
  subscribers: number;
}

const showsInFlight = new Map<string, InFlight>();

function abortError(): DOMException {
  return new DOMException("The catalog read was aborted.", "AbortError");
}

/** Drop `entry` from the map unless a reset has already put a newer read there. */
function forget(ref: string, entry: InFlight): void {
  if (showsInFlight.get(ref) === entry) {
    showsInFlight.delete(ref);
  }
}

export function sharedShow(
  ref: string,
  signal?: AbortSignal,
): Promise<ShowNode> {
  if (signal?.aborted) {
    return Promise.reject(abortError());
  }
  let entry = showsInFlight.get(ref);
  if (!entry) {
    const controller = new AbortController();
    const created: InFlight = {
      controller,
      subscribers: 0,
      promise: getShow(ref, { signal: controller.signal }).finally(() =>
        forget(ref, created),
      ),
    };
    showsInFlight.set(ref, created);
    entry = created;
  }
  const held = entry;
  held.subscribers += 1;
  if (!signal) {
    return held.promise;
  }
  return new Promise<ShowNode>((resolve, reject) => {
    const leave = () => {
      held.subscribers -= 1;
      if (held.subscribers === 0) {
        held.controller.abort();
        forget(ref, held);
      }
      reject(abortError());
    };
    signal.addEventListener("abort", leave, { once: true });
    held.promise.then(
      (node) => {
        signal.removeEventListener("abort", leave);
        resolve(node);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", leave);
        reject(error);
      },
    );
  });
}

/** The catalog root's children — the deployment's providers (plus the
 * classification root), for the shell's facet rail and for `sourceNames`'
 * provider word. */
export function catalogRootChildren(): Read<RootChild[]> {
  return read("root", async () => {
    const root = await getShow();
    return root.kind === "root" ? root.providers : [];
  });
}

/** A register's display names, shared by table headers and located findings. */
export function registerRead(ref: string): Read<RegisterShow> {
  return read(`register:${ref}`, async () => {
    const node = await sharedShow(ref);
    if (node.kind !== "register") throw new Error(`${ref} is not a register`);
    return node;
  });
}

/** Display names shared by register descendants and their located findings. */
export function registerSubjectNames(
  register?: RegisterShow | null,
): ReadonlyMap<string, string> {
  const names = new Map<string, string>();
  if (register) {
    names.set(register.fqid, register.name);
    for (const subject of [...register.tables, ...register.variables]) {
      names.set(subject.fqid, subject.name);
    }
  }
  return names;
}

/** A named catalog subject outside a register (for example a successor class). */
export function showRead(ref: string): Read<ShowNode> {
  return read(`show:${ref}`, () => sharedShow(ref));
}

/** Resolve display names through the same register read used by the browser. */
export function catalogNames(
  refs: readonly string[],
): ReadonlyMap<string, string> {
  const names = new Map<string, string>();
  const registers = new Set<string>();
  for (const ref of refs) {
    if (ref.startsWith("class/")) {
      const node = showRead(ref).value;
      if (node && "name" in node) names.set(ref, node.name);
      continue;
    }
    registers.add(ref.split("/").slice(0, 2).join("/"));
  }
  for (const ref of registers) {
    const register = registerRead(ref).value;
    if (!register) continue;
    for (const [fqid, name] of registerSubjectNames(register)) {
      names.set(fqid, name);
    }
  }
  return names;
}
