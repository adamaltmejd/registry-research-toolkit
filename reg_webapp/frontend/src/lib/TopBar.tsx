import { useEffect, useRef, useState } from "react";
import { Input, SearchField } from "react-aria-components";
import type { Steward } from "./api";
import {
  browseFilter,
  useBrowseFilter,
  useBrowseMatches,
} from "./browse_filter";
import { CommandPalette } from "./CommandPalette";
import { ProjectBanner } from "./ProjectBanner";
import { commandShortcutHint, isMacPlatform } from "./platform";
import styles from "./TopBar.module.css";
import type { WindowBounds } from "./window_bounds";

interface Props {
  /** The deployment's branding, or null until /api/context resolves. */
  steward: Steward | null;
  /** The server's version, the seed of a new project; empty until
   * /api/context resolves. */
  regMetaVersion: string;
  bounds: WindowBounds;
}

const mac = isMacPlatform();

/** The top bar (DESIGN.md → Layout): the brand (the deployment's name), the
 * one search bar and the project banner. There are no "Search" or "Project" nav items; the bar and the
 * banner are those. It sticks to the top of the page, and publishes its height
 * as `--top-bar-height` so the browse columns can fill the viewport below it (the
 * bar wraps, so its height is measured, not fixed). */
export function TopBar({ steward, regMetaVersion, bounds }: Props) {
  const ref = useRef<HTMLElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const field = useRef<HTMLDivElement>(null);
  const filter = useBrowseFilter();
  const matches = useBrowseMatches();
  // The palette opens with the field's text at that moment; it never writes
  // back to the filter.
  const [palette, setPalette] = useState<{ seed: string } | null>(null);

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

  // ⌘K on macOS, Ctrl+K elsewhere, opens the palette from anywhere, seeded with
  // the bar's text; Shift+⌘K is a different chord. Focus moves to the bar first,
  // so closing the palette returns it there.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== "k" || event.altKey || event.shiftKey) {
        return;
      }
      if ((event.metaKey && mac) || (event.ctrlKey && !mac)) {
        event.preventDefault();
        input.current?.focus();
        setPalette({ seed: input.current?.value ?? "" });
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  return (
    <header ref={ref} className={styles.topBar}>
      {/* The row is the shell's width, so on a wide screen the banner stays
          over the detail pane's edge instead of the screen's. */}
      <div className={styles.inner}>
      <a href="/" className={styles.brand}>
        {steward?.long_name ?? "Registry Research Toolkit"}
      </a>
      {/* Text here filters both browse columns in place; Escape clears it and
          gives back the collapse state. Enter opens the palette with the text,
          the way to codes, classifications and the rest of the catalog. */}
      <SearchField
        ref={field}
        aria-label="Search registers, variables, codes"
        className={styles.search}
        value={filter}
        onChange={browseFilter.set}
        onSubmit={(text) => setPalette({ seed: text })}
      >
        <Input
          ref={input}
          className={styles.searchInput}
          placeholder="Search registers, variables, codes…"
          autoComplete="off"
        />
        {filter === "" ? (
          <kbd className={styles.hint} aria-hidden="true">
            {commandShortcutHint(mac)}
          </kbd>
        ) : (
          <span className={styles.hint}>
            {/* The count is a live region that exists with the text, so a
                change in it is announced. */}
            <span className={styles.count} aria-live="polite">
              {matches === null
                ? ""
                : matches === 1
                  ? "1 match"
                  : `${matches} matches`}
            </span>
            <kbd className={styles.key} aria-hidden="true">
              Esc
            </kbd>
          </span>
        )}
      </SearchField>
      <CommandPalette
        isOpen={palette !== null}
        onOpenChange={(open) => {
          if (!open) setPalette(null);
        }}
        triggerRef={field}
        seed={palette?.seed ?? ""}
      />
      <ProjectBanner
        bounds={bounds}
        seed={{ regMetaVersion, steward: steward?.id ?? "" }}
      />
      </div>
    </header>
  );
}
