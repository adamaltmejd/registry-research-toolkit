import { useState } from "react";
import { type ClassificationCodeModel, getValues } from "./api";
import styles from "./CodeList.module.css";
import { Button } from "./ui/Button";
import { Skeleton } from "./ui/Skeleton";
import { useAsyncResource } from "./use_async_resource";

/** Each level's read, shared by every level of one panel. */
export type Levels = Map<string, Promise<ClassificationCodeModel[]>>;

/** The open nodes of one panel, by fqid and code. */
export interface OpenNodes {
  has: (key: string) => boolean;
  toggle: (key: string, open: boolean) => void;
}

interface Props {
  /** The classification FQID. */
  fqid: string;
  /** "" for the roots, else the code whose children this level lists. */
  parent: string;
  /** How many rows to reserve while loading: the parent's `child_count`. */
  expected: number | null;
  levels: Levels;
  opened: OpenNodes;
}

// The server's ceiling, as the flat panel reads.
const PAGE_SIZE = 200;
// Past this many skeleton rows the 18rem scroll is full; more reserve nothing.
const MAX_RESERVED = 20;

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
 * One level of a classification's code tree (#1297): the roots (`parent` "")
 * or one code's children. A row with children is a native disclosure whose
 * subtree mounts only while open, so a closed node issues no request. A level
 * is read once per panel (`levels`), so reopening a node or clearing a filter
 * never reads it again; a longer level is read page by page within its node.
 * While loading, the node reserves one skeleton row per child it announced, so
 * the list below does not jump (#1139).
 */
export function CodeTreeLevel({
  fqid,
  parent,
  expected,
  levels,
  opened,
}: Props) {
  // Bumped by Retry: part of the key, so a failed level reads again.
  const [attempt, setAttempt] = useState(0);
  const resource = useAsyncResource(
    `${fqid}\u0000${parent}\u0000${attempt}`,
    () => {
      const key = `${fqid}\u0000${parent}`;
      let read = levels.get(key);
      if (!read) {
        read = readLevel(fqid, parent);
        // A failed read is not cached: Retry, or reopening the node, reads again.
        read.catch(() => levels.delete(key));
        levels.set(key, read);
      }
      return read;
    },
  );
  const nested = parent !== "" ? styles.nested : "";
  const nodeKey = (code: string) => `${fqid}\u0000${code}`;

  if (resource.data) {
    return (
      <>
        <ul className={`${styles.treeLevel} ${nested}`}>
          {resource.data.map((code) => (
            <li key={`${code.code}\u0000${code.label}`}>
              {code.child_count ? (
                <details
                  open={opened.has(nodeKey(code.code))}
                  onToggle={(event) =>
                    opened.toggle(nodeKey(code.code), event.currentTarget.open)
                  }
                >
                  <summary className={styles.treeRow}>
                    <code className={styles.key}>{code.code}</code>
                    <span className={styles.label}>
                      {code.label}{" "}
                      <span className={styles.childCount}>
                        ({code.child_count}
                        <span className="visually-hidden"> codes</span>)
                      </span>
                    </span>
                  </summary>
                  {opened.has(nodeKey(code.code)) && (
                    <CodeTreeLevel
                      fqid={fqid}
                      parent={code.code}
                      expected={code.child_count}
                      levels={levels}
                      opened={opened}
                    />
                  )}
                </details>
              ) : (
                <div className={styles.treeRow}>
                  <code className={styles.key}>{code.code}</code>
                  <span className={styles.label}>{code.label}</span>
                </div>
              )}
            </li>
          ))}
        </ul>
        {resource.data.length === 0 && (
          <p className={`${styles.empty} ${nested}`}>
            No codes under {parent || "this classification"}.
          </p>
        )}
      </>
    );
  }
  if (resource.error !== null) {
    return (
      <div className={`${styles.status} ${nested}`} role="alert">
        <p className="error">
          <span aria-hidden="true">✕</span> Could not load codes:{" "}
          {resource.error}
        </p>
        <Button size="sm" onPress={() => setAttempt((n) => n + 1)}>
          Retry
        </Button>
      </div>
    );
  }
  const reserved = Array.from(
    { length: Math.min(expected ?? 3, MAX_RESERVED) },
    (_, i) => `reserve-${i}`,
  );
  return (
    <div className={`${styles.treeLoading} ${nested}`} aria-busy="true">
      <span className="visually-hidden">Loading codes…</span>
      {reserved.map((id) => (
        <div key={id} className={styles.treeRow}>
          <span className={styles.reserve}>
            <Skeleton width="60%" />
          </span>
        </div>
      ))}
    </div>
  );
}
