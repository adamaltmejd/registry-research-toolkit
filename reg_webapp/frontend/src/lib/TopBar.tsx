import { useEffect, useRef, useState } from "react";
import { Input, SearchField } from "react-aria-components";
import type { Steward } from "./api";
import { browseFilter, useBrowseFilter } from "./browse_filter";
import { CommandPalette } from "./CommandPalette";
import { ProjectBanner } from "./ProjectBanner";
import { commandShortcutHint, isMacPlatform } from "./platform";
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
  const field = useRef<HTMLDivElement>(null);
  const filter = useBrowseFilter();
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
      <a href="/" className={styles.brand}>
        <span className={styles.brandName}>
          {steward?.long_name ?? "Register Research Catalog"}
        </span>
        {steward && <span className={styles.brandId}>{steward.id}</span>}
      </a>
      {/* Text here filters both browse columns in place; Escape clears it and
          gives back the collapse state. Enter opens the palette with the text,
          the way to codes, classifications and the rest of the catalog. */}
      <SearchField
        ref={field}
        aria-label="Filter registers and variables"
        className={styles.search}
        value={filter}
        onChange={browseFilter.set}
        onSubmit={(text) => setPalette({ seed: text })}
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
      <CommandPalette
        isOpen={palette !== null}
        onOpenChange={(open) => {
          if (!open) setPalette(null);
        }}
        triggerRef={field}
        seed={palette?.seed ?? ""}
      />
      <ProjectBanner bounds={bounds} />
    </header>
  );
}
