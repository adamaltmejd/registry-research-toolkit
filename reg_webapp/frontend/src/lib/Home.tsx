import type { StewardConfig } from "./api";
import styles from "./Home.module.css";
import { InlineMarkdown } from "./InlineMarkdown";

export function Home({
  config,
  brand,
}: {
  config: StewardConfig | null;
  /** `null` while the deployment details are missing: the page then has no name. */
  brand: string | null;
}) {
  const presentation = config?.presentation;
  return (
    <article className={styles.home}>
      {brand === null ? (
        <h1 className="visually-hidden">Home</h1>
      ) : (
        <h1 className={styles.title}>{brand}</h1>
      )}
      {presentation?.description && (
        <p className={styles.tagline}>
          <InlineMarkdown text={presentation.description} />
        </p>
      )}
    </article>
  );
}
