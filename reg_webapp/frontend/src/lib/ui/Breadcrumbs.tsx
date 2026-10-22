import {
  Breadcrumbs as AriaBreadcrumbs,
  Breadcrumb,
  Link,
} from "react-aria-components";
import styles from "./Breadcrumbs.module.css";
import type { BreadcrumbItem } from "./types";

/**
 * The detail pane's path (DESIGN.md → Detail pane): `mono`, over the `h1`. The
 * last item is the current page (RAC marks it `aria-current="page"` and makes
 * it inert); an earlier item without an `href` is plain text. The separator is
 * generated content, so a screen reader reads only the labels.
 */
export function Breadcrumbs({
  items,
  className,
}: {
  items: readonly BreadcrumbItem[];
  className?: string;
}) {
  return (
    // RAC always names the list (its fallback is "Breadcrumbs"), so the name
    // lives there alone; a second one on the nav was announced twice.
    <nav
      className={`${className ?? ""} ${items.every((item) => item.narrowOnly) ? styles.narrowOnly : ""}`}
    >
      <AriaBreadcrumbs aria-label="Breadcrumb" className={styles.list}>
        {items.map((item, i) => (
          // A label can repeat along a path; its position cannot.
          <Breadcrumb
            // oxlint-disable-next-line react/no-array-index-key
            key={i}
            id={i}
            className={`${styles.crumb} ${item.word ? styles.word : ""} ${item.narrowOnly ? styles.narrowOnly : ""}`}
          >
            {item.href || i === items.length - 1 ? (
              <Link href={item.href} className={styles.link}>
                {item.label}
              </Link>
            ) : (
              <span className={styles.link}>{item.label}</span>
            )}
          </Breadcrumb>
        ))}
      </AriaBreadcrumbs>
    </nav>
  );
}
