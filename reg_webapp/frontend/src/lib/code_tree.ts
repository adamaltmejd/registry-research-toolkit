import { useEffect, useRef, useState } from "react";
import type { Key } from "react-aria-components";
import { type ClassificationCodeModel, errMessage, getValues } from "./api";

/** One level of the tree, by its parent code ("" for the roots): loading, its
 * codes, or why the read failed. A level never asked for is absent. */
export type Level =
  | { status: "loading" }
  | { status: "ok"; rows: ClassificationCodeModel[] }
  | { status: "error"; error: string };

/** A panel's code tree: the levels read so far and the open nodes. Owned by the
 * panel (`useCodeTree`), not the tree, so a filter round-trip, which unmounts
 * the tree for the flat matches, reopens it as it was without a read. */
export interface CodeTreeModel {
  levels: ReadonlyMap<string, Level>;
  expanded: ReadonlySet<Key>;
  setExpanded: (keys: ReadonlySet<Key>) => void;
  /** Read `parent`'s level unless it is loaded or in flight; after a failure,
   * read it again (Retry). */
  load: (parent: string) => void;
}

// The server's ceiling, as the flat panel reads.
const PAGE_SIZE = 200;

async function readLevel(
  fqid: string,
  parent: string,
): Promise<ClassificationCodeModel[]> {
  const rows: ClassificationCodeModel[] = [];
  let cursor: string | null = null;
  do {
    const page = await getValues(fqid, { parent, cursor, limit: PAGE_SIZE });
    rows.push(...(page.items as ClassificationCodeModel[]));
    cursor = page.next_cursor ?? null;
  } while (cursor !== null);
  return rows;
}

/**
 * The state behind a classification's code tree (#1297). Each level is read
 * once, when its node first opens, page by page within the node; a failed read
 * is kept as its error until Retry reads it again.
 *
 * Not `useAsyncResource`: that hook names one request by one key, and a tree
 * holds one read per open node, started from the expand handler. The reads
 * carry no abort signal; an answer after unmount is dropped instead.
 */
export function useCodeTree(fqid: string): CodeTreeModel {
  const [levels, setLevels] = useState<ReadonlyMap<string, Level>>(
    () => new Map(),
  );
  const [expanded, setExpanded] = useState<ReadonlySet<Key>>(() => new Set());
  const inFlight = useRef(new Set<string>());
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  function settle(parent: string, level: Level): void {
    inFlight.current.delete(parent);
    if (mounted.current) {
      setLevels((current) => new Map(current).set(parent, level));
    }
  }

  function load(parent: string): void {
    const level = levels.get(parent);
    if (inFlight.current.has(parent) || level?.status === "ok") return;
    inFlight.current.add(parent);
    setLevels((current) => new Map(current).set(parent, { status: "loading" }));
    readLevel(fqid, parent).then(
      (rows) => settle(parent, { status: "ok", rows }),
      (e: unknown) => settle(parent, { status: "error", error: errMessage(e) }),
    );
  }

  return { levels, expanded, setExpanded, load };
}
