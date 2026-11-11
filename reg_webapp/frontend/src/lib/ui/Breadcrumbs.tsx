import { Link } from "react-aria-components";
import styles from "./Breadcrumbs.module.css";
import type { BreadcrumbItem } from "./types";

/**
 * The detail pane's path (DESIGN.md → Detail pane): the ancestors' names over
 * the `h1`, ending at the page's parent. Not RAC's `Breadcrumbs`: it makes the
 * last item the current page, inert and `aria-current`, and this trail's last
 * item is a link to the parent (the `h1` names the page itself). The separator
 * is generated content, so a screen reader reads only the names. `busy` while
 * an ancestor's name is still loading.
 */
export function Breadcrumbs({
  items,
  busy = false,
  className,
}: {
  items: readonly BreadcrumbItem[];
  busy?: boolean;
  className?: string;
}) {
  if (items.length === 0) return null;
  return (
    <nav
      aria-label="Breadcrumb"
      aria-busy={busy || undefined}
      className={className}
    >
      <ol className={styles.list}>
        {items.map((item) => (
          <li key={item.href} className={styles.crumb}>
            <Link href={item.href} className={styles.link}>
              {item.label}
            </Link>
          </li>
        ))}
      </ol>
    </nav>
  );
}
