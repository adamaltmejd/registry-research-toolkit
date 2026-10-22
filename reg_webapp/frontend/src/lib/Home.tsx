import type { Steward } from "./api";
import styles from "./Home.module.css";

/**
 * The landing page at `/` (#675), after the design system's Cover: the
 * deployment's name in display type, one line on what the catalog is for, and
 * the ways in as plain list items. Search is the top bar, not an entry here. No
 * hero, cards or counters (DESIGN.md → No counters). `steward` is threaded from
 * App's `/api/context`; until it resolves the product name stands in.
 */
export function Home({ steward }: { steward: Steward | null }) {
  return (
    <article className={styles.home}>
      <h1 className={styles.title}>
        {steward?.long_name ?? "Registry Research Toolkit"}
      </h1>
      <p className={styles.tagline}>
        Swedish register metadata, collected into a project.
      </p>
      <ul className={styles.entries}>
        <li>
          <a href="/catalog">Browse registers</a>
        </li>
        <li>
          <a href="/project">Create a project</a>
        </li>
      </ul>
    </article>
  );
}
