import type { StewardConfig } from "./api";
import styles from "./Home.module.css";
import { parseInlineMarkdown } from "./inline_markdown";

/* oxlint-disable react/no-array-index-key -- Immutable description segments have no independent ids. */
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
          {parseInlineMarkdown(presentation.description).map(
            (segment, index) =>
              segment.href ? (
                <a key={index} href={segment.href}>
                  {segment.text}
                </a>
              ) : segment.emphasis === "strong" ? (
                <strong key={index}>{segment.text}</strong>
              ) : segment.emphasis === "em" ? (
                <em key={index}>{segment.text}</em>
              ) : (
                segment.text
              ),
          )}
        </p>
      )}
    </article>
  );
}
