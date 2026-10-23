import type { ReactNode } from "react";
import type { Steward } from "./api";
import styles from "./Home.module.css";

/** `[text](https://…)`: the one markup a steward description takes
 * (reg_webapp/DESIGN.md → steward.json). https only, so a description can
 * never carry a `javascript:` or relative link. */
const LINK = /\[([^\]]+)\]\((https:\/\/[^)\s]+)\)/g;

/** The description as text with its links: React children, never HTML. */
function withLinks(text: string): ReactNode[] {
  const out: ReactNode[] = [];
  let at = 0;
  for (const match of text.matchAll(LINK)) {
    out.push(text.slice(at, match.index));
    out.push(
      <a key={match.index} href={match[2]}>
        {match[1]}
      </a>,
    );
    at = match.index + match[0].length;
  }
  out.push(text.slice(at));
  return out;
}

/**
 * The landing page at `/` (#675), after the design system's Cover: the
 * deployment's name in display type and its description (`steward.json`). The
 * register list beside it is the way into the catalog, and the top bar holds
 * search and the project box (with "Create a project" until one is open), so
 * none of them is repeated here. No hero, cards or
 * counters (DESIGN.md → No counters). `steward` is threaded from App's
 * `/api/context`; until it resolves the product name stands in.
 */
export function Home({ steward }: { steward: Steward | null }) {
  return (
    <article className={styles.home}>
      <h1 className={styles.title}>
        {steward?.long_name ?? "Registry Research Toolkit"}
      </h1>
      {steward?.description && (
        <p className={styles.tagline}>{withLinks(steward.description)}</p>
      )}
    </article>
  );
}
