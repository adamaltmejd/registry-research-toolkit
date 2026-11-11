import { useEffect, useEffectEvent, useRef, useState } from "react";
import { Autocomplete, Input, SearchField } from "react-aria-components";
import { flushSync } from "react-dom";
import {
  brandName,
  type Context,
  SEARCH_MIN_QUERY_LENGTH,
  stewardConfig,
} from "./api";
import { CommandPalette } from "./CommandPalette";
import { ProjectBanner } from "./ProjectBanner";
import { commandShortcutHint, isMacPlatform } from "./platform";
import styles from "./TopBar.module.css";
import { Button } from "./ui/Button";
import type { ProjectCheck } from "./use_project";

const mac = isMacPlatform();

/** The top bar (DESIGN.md → Layout): the brand (the deployment's name, or its
 * logo where the name does not fit), the one search bar and the project
 * banner. There are no "Search" or "Project" nav items; the bar and the banner
 * are those. It sticks to the top of the page, and publishes its height
 * as `--top-bar-height` so the browse columns can fill the viewport below it (the
 * bar wraps, so its height is measured, not fixed). */
export function TopBar({
  context,
  validation,
}: {
  context: Context | null;
  validation: ProjectCheck;
}) {
  const config = stewardConfig(context);
  const brand = brandName(context);
  const ref = useRef<HTMLElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const field = useRef<HTMLDivElement>(null);
  const toggle = useRef<HTMLButtonElement>(null);
  const popover = useRef<HTMLElement>(null);
  // The field's text: it searches, and filters nothing (#2398).
  const [text, setText] = useState("");
  const [palette, setPalette] = useState(false);
  // Below the narrow breakpoint the field is folded behind the "Search" button
  // until it is opened; it stays open while it holds text. Above the
  // breakpoint CSS ignores this.
  const [opened, setOpened] = useState(false);
  const searchOpen = opened || text !== "";
  // Typing opens the results once the text is long enough to search (#2398);
  // a keystroke after Escape closed them opens them again.
  const onInputChange = (next: string) => {
    setText(next);
    if (next.trim().length >= SEARCH_MIN_QUERY_LENGTH) setPalette(true);
  };

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
  const onCommandK = useEffectEvent(() => {
    openSearch();
    setPalette(true);
  });
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== "k" || event.altKey || event.shiftKey) {
        return;
      }
      if ((event.metaKey && mac) || (event.ctrlKey && !mac)) {
        event.preventDefault();
        onCommandK();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  return (
    <header ref={ref} className={styles.topBar}>
      {/* The row is the shell's width, so on a wide screen the banner stays
          over the detail pane's edge instead of the screen's. */}
      <div className={styles.inner} data-search-open={searchOpen || undefined}>
        {brand !== null && (
          <a href="/" className={styles.brand}>
            <span className={styles.brandName}>{brand}</span>
            {config && (
              <img
                className={styles.brandLogo}
                alt={brand}
                src={`data:image/svg+xml,${encodeURIComponent(config.logo)}`}
              />
            )}
          </a>
        )}
        {/* The field searches and filters nothing (#2398): its results, the
          palette, open under it as the text reaches the search minimum, and
          on Enter or ⌘K, for registers, variables and the app's places; the
          field is the palette's one input. Escape closes the palette first,
          then clears the text, then, below the breakpoint, folds the field
          back behind the "Search" button. */}
        <Autocomplete
          inputValue={text}
          onInputChange={onInputChange}
          // No row is active until the arrow keys reach one: the results open
          // as you type, and Enter on an auto-focused first row opened a
          // place ranked before the catalog's hits had landed.
          disableAutoFocusFirst
        >
          <SearchField
            ref={field}
            aria-label="Search registers, variables"
            className={styles.search}
            onSubmit={() => setPalette(true)}
            onBlur={(event) => {
              // simplify: focus inside the popover is not handled, because the
              // RAC Autocomplete menu uses virtual focus and is never tabbable;
              // revisit if the menu ever becomes tabbable.
              // The rows take virtual focus, so focus landing in the popover is
              // still the palette's. A blur to nothing (a press on the page) is
              // left to the palette's outside-press handler.
              const next = event.relatedTarget;
              if (next instanceof Node && popover.current?.contains(next)) {
                return;
              }
              const away = next !== null;
              if (palette && away) setPalette(false);
              if (text === "" && (!palette || away)) setOpened(false);
            }}
          >
            <Input
              ref={input}
              className={styles.searchInput}
              placeholder="Search registers, variables…"
              autoComplete="off"
              // No aria-expanded: ARIA does not allow it on a searchbox (axe
              // aria-allowed-attr); it would take role="combobox".
              aria-haspopup="menu"
              // RAC's Autocomplete sets "list" whether or not the palette is
              // mounted, and an undefined here does not override it; "none"
              // says there is no list while it is closed.
              aria-autocomplete={palette ? "list" : "none"}
              // Capture: ahead of the field's own Escape, which clears the
              // text, and of the Autocomplete's, which hands it to the list.
              onKeyDownCapture={(event) => {
                if (event.key !== "Escape" || event.nativeEvent.isComposing) {
                  return;
                }
                if (palette) {
                  event.preventDefault();
                  event.stopPropagation();
                  setPalette(false);
                } else if (text === "" && opened) {
                  event.preventDefault();
                  event.stopPropagation();
                  flushSync(() => setOpened(false));
                  toggle.current?.focus();
                }
              }}
            />
            {/* A touch device has neither key: CSS hides it there. */}
            <kbd className={styles.hint} aria-hidden="true">
              {text === "" ? commandShortcutHint(mac) : "Esc"}
            </kbd>
          </SearchField>
          <CommandPalette
            isOpen={palette}
            onOpenChange={(open) => {
              setPalette(open);
              // Closed from outside the field (a press elsewhere, a row's
              // action, a scroll): an empty field folds with it.
              if (!open && text === "") setOpened(false);
            }}
            triggerRef={field}
            popoverRef={popover}
            text={text}
            onChoose={() => {
              // A row navigates and ends the search: the text clears and the
              // narrow field folds away, so the brand and the page return.
              setPalette(false);
              setText("");
              setOpened(false);
            }}
          />
        </Autocomplete>
        {/* Below the breakpoint only (CSS): the folded field's way in. */}
        <span className={styles.searchToggle}>
          <Button
            ref={toggle}
            aria-label="Search registers, variables"
            onPress={openSearch}
          >
            Search
          </Button>
        </span>
        <ProjectBanner context={context} validation={validation} />
      </div>
    </header>
  );
}
