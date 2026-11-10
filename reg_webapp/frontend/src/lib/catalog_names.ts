import {
  ApiError,
  type CatalogSegment,
  type ClassFamilyRef,
  type Graph,
  getGraph,
  getSegments,
  getShow,
  type RegisterShow,
  type RootChild,
  type ShowNode,
} from "./api";
import { registerOfPath } from "./catalog";

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

/** Call `listener` whenever a read settles. */
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
const UNASKED: Read<never> = { value: null, loading: false };

function settleRead(key: string, value: unknown): void {
  settleReads([[key, value]]);
}

/** Settle several reads that one request answered, with one notification. */
function settleReads(settled: readonly (readonly [string, unknown])[]): void {
  const next: Record<string, Read<unknown>> = {};
  for (const [key, value] of settled) {
    const current = reads[key];
    // An older failure cannot replace a success.
    if (value === null && current?.value) continue;
    if (current && !current.loading && current.value === value) continue;
    next[key] = { value, loading: false };
  }
  if (Object.keys(next).length === 0) return;
  reads = { ...reads, ...next };
  notify();
}

/** The cached read for `key`, STARTING it on first ask. Repeat asks (a re-render,
 * a second card on the same register) get the same entry and issue no request. */
function read<T>(key: string, fetcher: () => Promise<T>): Read<T> {
  const hit = reads[key] as Read<T> | undefined;
  if (hit) {
    return hit;
  }
  reads[key] = PENDING;
  const settle = (value: T | null) => settleRead(key, value);
  // A failed name read is not an error surface — see the module comment.
  fetcher().then(settle, () => settle(null));
  return PENDING;
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

/** Drop `entry` from the map unless a newer read has already taken its place. */
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

/** The catalog root's children — the deployment's providers, each with its
 * registers: the whole browse column in one read (#2232), and the breadcrumb's
 * provider names. */
export function catalogRootChildren(): Read<RootChild[]> {
  return read("root", async () => {
    const root = await getShow();
    return root.kind === "root" ? root.providers : [];
  });
}

/** The classification list's address (`/catalog/class`) and its name: the
 * list is a page of the browse column, not an entity with a name of its own. */
export const CLASSIFICATIONS = "class";
export const CLASSIFICATIONS_LABEL = "Classifications";
const CLASSIFICATIONS_NAME: Read<string> = {
  value: CLASSIFICATIONS_LABEL,
  loading: false,
};

/** The classifications, for the browse column's list of them and its page:
 * on a steward site only those its variables use (the server scopes it). */
export function classFamiliesRead(): Read<ClassFamilyRef[]> {
  return read("class", async () => {
    const node = await getShow("class");
    return node.kind === "class_families" ? node.families : [];
  });
}

/** A register's display names, shared by table headers and related documents. */
export function registerRead(ref: string): Read<RegisterShow> {
  return read(`register:${ref}`, () => loadRegister(ref));
}

/** A variable's whole per-period list. */
export function segmentsRead(ref: string): Read<CatalogSegment[]> {
  return read(`segments:${ref}`, () => getSegments(ref));
}

/** The variables a table read names at once. Requests and server work pull apart: the
 * edge counts requests (at least rows / 200, the page size), while the server rebuilds
 * a read's whole row set for each page it serves. On a 107-column LISA individer-16plus
 * draft (1,543 rows) 10 gives 11 single-page reads, 25 gives 9 reads building 3,010
 * rows, 100 gives 9 reads building 11,812 (measured for #2232).
 * simplify: tuned on that one table; a read form without the page cap would serve each
 * batch once, so revisit if a page load of a larger draft passes the edge's budget. */
const TABLE_READ_VARIABLES = 25;

/** The named variable a refused table read blames (`fields.ref`, set by not_found,
 * ambiguous_ref and invalid_ref), if it is one of `batch`. */
function refusedVariable(
  error: unknown,
  batch: readonly string[],
): string | null {
  if (!(error instanceof ApiError) || error.status >= 500) return null;
  const body = error.body as { error?: { fields?: { ref?: unknown } } } | null;
  const ref = body?.error?.fields?.ref;
  return typeof ref === "string" && batch.includes(ref) ? ref : null;
}

/** Each of `variables`' rows in `table`, by variable. The columns not read yet are
 * read together in parallel batches of `TABLE_READ_VARIABLES` rather than one read
 * per column (#2232), and each keeps its own entry: a column added later reads only
 * itself, and source edits reload no other card. A batch settles on its own. A
 * variable the server refuses fails alone and the rest of its batch is read again;
 * any other failure fails that batch's columns, as a column's own read did. */
export function tableSegmentsReads(
  table: string,
  variables: readonly string[],
): Record<string, Read<CatalogSegment[]>> {
  const key = (variable: string) =>
    `table-segments:${JSON.stringify([table, variable])}`;
  const readBatch = (batch: readonly string[]) => {
    getSegments(table, { variables: batch }).then(
      (rows) =>
        settleReads(
          batch.map((variable) => [
            key(variable),
            rows.filter((row) => row.variable === variable),
          ]),
        ),
      (error: unknown) => {
        const refused = refusedVariable(error, batch);
        // A failed read is not an error surface — see the module comment. A
        // variable the server refuses by name has no rows here, which is an
        // answer, not a failure: the project's check names why, and the
        // card's other columns keep their years.
        settleReads(
          refused === null
            ? batch.map((variable) => [key(variable), null])
            : [[key(refused), []]],
        );
        // Each retry drops one variable, so this ends within the batch.
        const rest = batch.filter((variable) => variable !== refused);
        if (refused !== null && rest.length > 0) readBatch(rest);
      },
    );
  };
  const missing = variables.filter((variable) => !reads[key(variable)]);
  for (const variable of missing) reads[key(variable)] = PENDING;
  for (let i = 0; i < missing.length; i += TABLE_READ_VARIABLES)
    readBatch(missing.slice(i, i + TABLE_READ_VARIABLES));
  return Object.fromEntries(
    variables.map((variable) => [
      variable,
      reads[key(variable)] as Read<CatalogSegment[]>,
    ]),
  );
}

const graphKey = (ref: string) => `graph:${ref}`;

/** A variable's one-hop succession, shared by its variable or group page. */
export function graphRead(ref: string): Read<Graph> {
  return read(graphKey(ref), () => getGraph(ref, { kind: "continues" }));
}

/** `member`'s own graph within a group's: the edges that touch it and their ends,
 * which is what the member's own read answers. */
function memberGraph(graph: Graph, member: string): Graph {
  const edges = graph.edges.filter(
    (edge) => edge.from === member || edge.to === member,
  );
  const ends = new Set([member, ...edges.flatMap((e) => [e.from, e.to])]);
  return {
    focus: member,
    nodes: graph.nodes.filter((node) => ends.has(node.fqid)),
    edges,
  };
}

/** Each of a group's `members`' one-hop successions from ONE read: the server
 * answers a group ref with the union of its members' graphs (#2232), split here
 * into each member's entry under `graphRead`'s key, so a member's own page reads
 * nothing again. Only members not read yet are asked for; a failure fails them. */
export function groupGraphReads(
  group: string,
  members: readonly string[],
): Record<string, Read<Graph>> {
  const missing = members.filter((member) => !reads[graphKey(member)]);
  if (missing.length > 0) {
    for (const member of missing) reads[graphKey(member)] = PENDING;
    const settle = (graph: Graph | null) =>
      settleReads(
        missing.map((member) => [
          graphKey(member),
          graph && memberGraph(graph, member),
        ]),
      );
    // A failed read is not an error surface — see the module comment.
    getGraph(group, { kind: "continues" }).then(settle, () => settle(null));
  }
  return Object.fromEntries(
    members.map((member) => [member, reads[graphKey(member)] as Read<Graph>]),
  );
}

/** A page owns its pending/error state; only a successful cached read is reusable. */
export async function loadRegister(
  ref: string,
  signal?: AbortSignal,
): Promise<RegisterShow> {
  const key = `register:${ref}`;
  const cached = reads[key] as Read<RegisterShow> | undefined;
  if (cached?.value) return cached.value;
  // Evict a settled failure on Retry or revisit, without notifying subscribers
  // into an automatic retry loop. sharedShow only retains in-flight requests.
  if (cached && !cached.loading) delete reads[key];
  const node = await sharedShow(ref, signal);
  if (node.kind !== "register") throw new Error(`${ref} is not a register`);
  settleRead(key, node);
  return node;
}

const subjectNames = new WeakMap<RegisterShow, ReadonlyMap<string, string>>();
const NO_NAMES: ReadonlyMap<string, string> = new Map();

/** Display names shared by register descendants and the panels that cite them. */
export function registerSubjectNames(
  register?: RegisterShow | null,
): ReadonlyMap<string, string> {
  if (!register) return NO_NAMES;
  const cached = subjectNames.get(register);
  if (cached) return cached;
  const names = new Map<string, string>([[register.fqid, register.name]]);
  for (const subject of [...register.tables, ...register.variables]) {
    names.set(subject.fqid, subject.name);
  }
  subjectNames.set(register, names);
  return names;
}

/** Resolve display names through the same register read used by the browser. */
export function catalogNames(
  refs: readonly string[],
): ReadonlyMap<string, string> {
  const names = new Map<string, string>();
  const registers = new Set<string>();
  for (const ref of refs) {
    if (ref.startsWith("class/")) {
      names.set(ref, classificationLabel(ref));
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

/** A single name and its read state, so pending names never look like failures. */
export function catalogNameRead(ref: string | null): Read<string> {
  if (ref === null) return UNASKED;
  if (ref.startsWith("class/")) {
    return { value: classificationLabel(ref), loading: false };
  }
  const state = registerRead(ref.split("/").slice(0, 2).join("/"));
  return {
    value: registerSubjectNames(state.value).get(ref) ?? null,
    loading: state.loading,
  };
}

/** A breadcrumb ancestor's name: a provider's from the root read the browse
 * column makes, a register's from the register read its variable list shares,
 * so a trail issues no request of its own. */
export function ancestorNameRead(ref: string | null): Read<string> {
  if (ref === CLASSIFICATIONS) return CLASSIFICATIONS_NAME;
  if (ref === null || ref.includes("/")) return catalogNameRead(ref);
  const root = catalogRootChildren();
  return {
    value: root.value?.find((provider) => provider.fqid === ref)?.name ?? null,
    loading: root.loading,
  };
}

const classificationNames = new WeakMap<object, ReadonlyMap<string, string>>();

/** Every classification name the per-period rows read so far carry
 * (`classification_name`), by version ref. A name read never asks `show` for a
 * classification: one binding can have dozens of candidate versions, and
 * fetching each in full only for its name cost a request apiece. Cached per
 * `reads` object, which a settled read replaces. */
function loadedClassificationNames(): ReadonlyMap<string, string> {
  const cached = classificationNames.get(reads);
  if (cached) return cached;
  const names = new Map<string, string>();
  for (const [key, read] of Object.entries(reads)) {
    if (!key.startsWith("segments:") && !key.startsWith("table-segments:"))
      continue;
    for (const row of (read.value as CatalogSegment[] | null) ?? [])
      if (row.classification && row.classification_name)
        names.set(row.classification, row.classification_name);
  }
  classificationNames.set(reads, names);
  return names;
}

/** A classification version as text: its name from a row already read, or,
 * where no loaded row carries it (a pinned version not delivered on the
 * project's tables), its own id, the ref's last part, shown as an id rather
 * than a name made up from it. */
function classificationLabel(ref: string): string {
  return (
    loadedClassificationNames().get(ref) ?? (ref.split("/").at(-1) as string)
  );
}

/** Whether a catalog path is a table's address, from the register read the
 * trail already makes: such an address renders its register's page (#2316). */
export function isTablePathRead(path: string | null): boolean {
  const register = path === null ? null : registerOfPath(path);
  if (register === null || register === path) return false;
  return (
    registerRead(register).value?.tables.some((table) => table.fqid === path) ??
    false
  );
}
