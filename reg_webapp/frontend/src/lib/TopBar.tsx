import { useEffect, useRef } from "react";
import type { Steward } from "./api";
import { ProjectBanner } from "./ProjectBanner";
import SearchOmnibox from "./SearchOmnibox.svelte";
import { SvelteIsland } from "./SvelteIsland";
import styles from "./TopBar.module.css";
import type { WindowBounds } from "./window_bounds";

interface Props {
  /** The deployment's branding, or null until /api/context resolves. */
  steward: Steward | null;
  bounds: WindowBounds;
}

const NO_PROPS = {};

/** The top bar (DESIGN.md → Layout): the brand, the one search bar and the
 * project banner. There are no "Search" or "Project" nav items; the bar and the
 * banner are those. It sticks to the top of the page, and publishes its height
 * as `--top-bar-height` so the browse column can fill the viewport below it (the
 * bar wraps, so its height is measured, not fixed). */
export function TopBar({ steward, bounds }: Props) {
  const ref = useRef<HTMLElement>(null);
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

  return (
    <header ref={ref} className={styles.topBar}>
      <a href="/" className={styles.brand}>
        <span className={styles.brandName}>
          {steward?.long_name ?? "Register Research Catalog"}
        </span>
        {steward && <span className={styles.brandId}>{steward.id}</span>}
      </a>
      {/* The search bar stays the Svelte omnibox until the palette replaces it
          (port PR 8). */}
      <SvelteIsland
        component={SearchOmnibox}
        props={NO_PROPS}
        className={styles.search}
      />
      <ProjectBanner bounds={bounds} />
    </header>
  );
}
