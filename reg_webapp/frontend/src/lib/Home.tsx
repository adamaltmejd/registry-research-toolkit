import type { Steward } from "./api";
import styles from "./Home.module.css";

/**
 * The landing page at `/` (#675), after the design system's Cover: the
 * deployment's name in display type, its description (`steward.json`), and the
 * way into a project. The register list beside it is the way into the catalog,
 * and search is the top bar, so neither is an entry here. No hero, cards or
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
        <p className={styles.tagline}>{steward.description}</p>
      )}
      <p className={styles.entry}>
        <a href="/project">Create a project</a>
      </p>
    </article>
  );
}
