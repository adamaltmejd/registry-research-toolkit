import { useEffect, useRef } from "react";
import { Input, SearchField } from "react-aria-components";
import type { Steward } from "./api";
import { browseFilter, useBrowseFilter } from "./browse_filter";
import { ProjectBanner } from "./ProjectBanner";
import { commandShortcutHint, isMacPlatform } from "./platform";
import { router } from "./router";
import styles from "./TopBar.module.css";
import type { WindowBounds } from "./window_bounds";

interface Props {
  /** The deployment's branding, or null until /api/context resolves. */
  steward: Steward | null;
  bounds: WindowBounds;
}

const mac = isMacPlatform();

/** The top bar (DESIGN.md → Layout): the brand, the one search bar and the
 * project banner. There are no "Search" or "Project" nav items; the bar and the
 * banner are those. It sticks to the top of the page, and publishes its height
 * as `--top-bar-height` so the browse columns can fill the viewport below it (the
 * bar wraps, so its height is measured, not fixed). */
export function TopBar({ steward, bounds }: Props) {
  const ref = useRef<HTMLElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const filter = useBrowseFilter();

  useEffect(() => {
    const header = ref.current;
    if (!header) return;
    const root = document.documentElement;
    const observer = new ResizeObserver(() => {
      root.style.setProperty(
        "--top-bar-height",
        `${header.getBoundingClientRect().height}px`,
      );
    });
    observer.observe(header);
    return () => {
      observer.disconnect();
      root.style.removeProperty("--top-bar-height");
    };
  }, []);

  // ⌘K on macOS, Ctrl+K elsewhere, focuses the bar from anywhere; Shift+⌘K is
  // a different chord. The palette it opens (DESIGN.md) arrives in port PR 8.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== "k" || event.altKey || event.shiftKey) {
        return;
      }
      if ((event.metaKey && mac) || (event.ctrlKey && !mac)) {
        event.preventDefault();
        input.current?.focus();
        input.current?.select();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  return (
    <header ref={ref} className={styles.topBar}>
      <a href="/" className={styles.brand}>
        <span className={styles.brandName}>
          {steward?.long_name ?? "Register Research Catalog"}
        </span>
        {steward && <span className={styles.brandId}>{steward.id}</span>}
      </a>
      {/* Text here filters both browse columns in place; Escape clears it and
          gives back the collapse state. Enter opens the catalog-wide results
          page, the way to codes and classifications until the palette replaces
          that page (port PR 8). */}
      <SearchField
        aria-label="Filter registers and variables"
        className={styles.search}
        value={filter}
        onChange={browseFilter.set}
        onSubmit={(text) => {
          const q = text.trim();
          if (q) router.navigate(`/search?q=${encodeURIComponent(q)}`);
        }}
      >
        <Input
          ref={input}
          className={styles.searchInput}
          placeholder="Filter registers and variables…"
          autoComplete="off"
        />
        {filter === "" && (
          <kbd className={styles.shortcut} aria-hidden="true">
            {commandShortcutHint(mac)}
          </kbd>
        )}
      </SearchField>
      <ProjectBanner bounds={bounds} />
    </header>
  );
}
