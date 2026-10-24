import { useEffect, useRef, useState } from "react";
import { Autocomplete, Input, SearchField } from "react-aria-components";
import { flushSync } from "react-dom";
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
import { Button } from "./ui/Button";
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

/** Input types that take no typed text: Escape on them is not about them. */
const NON_TEXT_INPUTS = new Set([
  "checkbox",
  "radio",
  "range",
  "button",
  "submit",
  "reset",
]);

/** A text-entry element other than the search field itself. */
function isOtherTextEntry(
  target: EventTarget | null,
  search: HTMLInputElement | null,
): boolean {
  if (!(target instanceof HTMLElement) || target === search) return false;
  if (target instanceof HTMLInputElement) {
    return !NON_TEXT_INPUTS.has(target.type);
  }
  return target instanceof HTMLTextAreaElement || target.isContentEditable;
}

/** The top bar (DESIGN.md → Layout): the brand (the deployment's name, or its
 * logo where the name does not fit), the one search bar and the project
 * banner. There are no "Search" or "Project" nav items; the bar and the banner
 * are those. It sticks to the top of the page, and publishes its height
 * as `--top-bar-height` so the browse columns can fill the viewport below it (the
 * bar wraps, so its height is measured, not fixed). */
export function TopBar({ steward, regMetaVersion, bounds }: Props) {
  const brand = steward?.long_name ?? "Registry Research Toolkit";
  const ref = useRef<HTMLElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const field = useRef<HTMLDivElement>(null);
  const toggle = useRef<HTMLButtonElement>(null);
  const filter = useBrowseFilter();
  const matches = useBrowseMatches();
  const [palette, setPalette] = useState(false);
  // Below the narrow breakpoint the field is folded behind the "Search" button
  // until it is opened; it stays open while it holds text, so an active filter
  // is never hidden. Above the breakpoint CSS ignores this.
  const [opened, setOpened] = useState(false);
  const searchOpen = opened || filter !== "";

  /** Unfold the field and move focus into it, in the user's own act. */
  const openSearch = () => {
    flushSync(() => setOpened(true));
    input.current?.focus();
  };

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
        openSearch();
        setPalette(true);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Escape clears the filter from anywhere on the page (#1827), not only in the
  // field. Whatever takes Escape itself handles it first and stops it or marks
  // it handled: the field's own clear, a dialog, a popover, the palette. Escape
  // typed in another text field (a period year, the project name) leaves the
  // search alone: it is about that field, not the browse columns.
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (
        event.key !== "Escape" ||
        isOtherTextEntry(event.target, input.current) ||
        event.defaultPrevented ||
        event.isComposing ||
        event.altKey ||
        event.ctrlKey ||
        event.metaKey ||
        event.shiftKey ||
        browseFilter.value === ""
      ) {
        return;
      }
      browseFilter.set("");
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  return (
    <header ref={ref} className={styles.topBar}>
      {/* The row is the shell's width, so on a wide screen the banner stays
          over the detail pane's edge instead of the screen's. */}
      <div className={styles.inner} data-search-open={searchOpen || undefined}>
        <a href="/" className={styles.brand}>
          <span className={styles.brandName}>{brand}</span>
          {/* The steward's logo (`stewards/<catalog>/logo.svg`, also the
              favicon): shown only where the full name would push the banner
              off the row. Sized here so the bar's measured height does not
              jump when it loads. */}
          <img
            className={styles.brandMark}
            src="/api/steward/logo.svg"
            alt=""
            width={28}
            height={28}
          />
        </a>
        {/* Text here filters both browse columns in place, never the open
          page. Enter opens the palette under it, the way to codes,
          classifications and the rest of the catalog; the field stays the
          palette's one input, so typing there goes on filtering the columns
          too. Escape closes the palette first, then clears the text (here or
          anywhere on the page, giving back the collapse state), then, below
          the breakpoint, folds the field back behind the "Search" button. */}
        <Autocomplete inputValue={filter} onInputChange={browseFilter.set}>
          <SearchField
            ref={field}
            aria-label="Search registers, variables, codes"
            className={styles.search}
            onSubmit={() => {
              if (!palette) setPalette(true);
            }}
            onBlur={() => {
              if (filter === "" && !palette) setOpened(false);
            }}
          >
            <Input
              ref={input}
              className={styles.searchInput}
              placeholder="Search registers, variables, codes…"
              autoComplete="off"
              // Capture: ahead of the field's own Escape, which clears the
              // text, and of the Autocomplete's, which hands it to the list.
              onKeyDownCapture={(event) => {
                if (event.key !== "Escape" || event.nativeEvent.isComposing) {
                  return;
                }
                if (palette) {
                  // Marked handled too: the page-wide Escape skips it.
                  event.preventDefault();
                  event.stopPropagation();
                  setPalette(false);
                } else if (filter === "" && opened) {
                  event.preventDefault();
                  event.stopPropagation();
                  flushSync(() => setOpened(false));
                  toggle.current?.focus();
                }
              }}
            />
            {filter === "" ? (
              <kbd className={styles.hint} aria-hidden="true">
                {commandShortcutHint(mac)}
              </kbd>
            ) : (
              <span className={styles.hint}>
                {/* How many registers the browse columns keep, never the
                  palette's hits, so it names its noun. A live region that
                  exists with the text, so a change in it is announced. */}
                <span className={styles.count} aria-live="polite">
                  {matches === null
                    ? ""
                    : matches === 1
                      ? "1 register"
                      : `${matches} registers`}
                </span>
                <kbd className={styles.key} aria-hidden="true">
                  Esc
                </kbd>
              </span>
            )}
          </SearchField>
          <CommandPalette
            isOpen={palette}
            onOpenChange={setPalette}
            triggerRef={field}
            text={filter}
          />
        </Autocomplete>
        {/* Below the breakpoint only (CSS): the folded field's way in. */}
        <span className={styles.searchToggle}>
          <Button
            ref={toggle}
            aria-label="Search registers, variables, codes"
            onPress={openSearch}
          >
            Search
          </Button>
        </span>
        <ProjectBanner
          bounds={bounds}
          seed={{ regMetaVersion, steward: steward?.id ?? "" }}
        />
      </div>
    </header>
  );
}
