import type { StewardConfig } from "./api";
import styles from "./Home.module.css";
import { InlineMarkdown } from "./InlineMarkdown";

export function Home({ config }: { config: StewardConfig | null }) {
  const presentation = config?.presentation;
  return (
    <article className={styles.home}>
      <h1 className={styles.title}>
        {presentation?.long_name ??
          presentation?.name ??
          "Registry Research Toolkit"}
      </h1>
      {presentation?.description && (
        <p className={styles.tagline}>
          <InlineMarkdown text={presentation.description} />
        </p>
      )}
    </article>
  );
}
