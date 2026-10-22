import type { Steward } from "./api";
import styles from "./Home.module.css";

const ENTRIES = [
  {
    href: "/catalog",
    title: "Browse the data",
    description: "Providers, registers and their variables, with value sets.",
  },
  {
    href: "/project",
    title: "Create a project",
    description:
      "Collect columns into a data order and check it before you send it.",
  },
] as const;

/**
 * The landing page at `/` (#675): the deployment's name, what the catalog is
 * for, and the two ways in. Search is the top bar, not an entry here. No hero,
 * cards or catalog totals (DESIGN.md → No counters). `steward` is threaded
 * from App's `/api/context`; until it resolves the generic name stands in.
 */
export function Home({ steward }: { steward: Steward | null }) {
  return (
    <article className={styles.home}>
      <h1 className={styles.title}>
        {steward?.long_name ?? "Register Research Catalog"}
      </h1>
      <p className={styles.lead}>
        Swedish register metadata, and a workbench for collecting it into a data
        order.
      </p>
      <ul className={styles.entries}>
        {ENTRIES.map((entry) => (
          <li key={entry.href}>
            <a className={styles.link} href={entry.href}>
              {entry.title}
            </a>
            <p className={styles.description}>{entry.description}</p>
          </li>
        ))}
      </ul>
    </article>
  );
}
