import { type Ref, useEffect, useEffectEvent, useRef, useState } from "react";
import { Autocomplete, Input, SearchField } from "react-aria-components";
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
 * logo where the name does not fit), the one search bar, the project banner
 * and, below the narrow breakpoint, the button that shows the register
 * browser (#2403). There are no "Search" or "Project" nav items; the bar and the banner
 * are those. It sticks to the top of the page, and publishes its height
 * as `--top-bar-height` so the browse columns can fill the viewport below it (the
 * bar wraps, so its height is measured, not fixed). */
export function TopBar({
  context,
  validation,
  browser,
  browserButtonRef,
}: {
  context: Context | null;
  validation: ProjectCheck;
  browser: RegisterBrowserButton;
  browserButtonRef: Ref<HTMLButtonElement>;
}) {
  const config = stewardConfig(context);
  const brand = brandName(context);
  const ref = useRef<HTMLElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const field = useRef<HTMLDivElement>(null);
  const popover = useRef<HTMLElement>(null);
  // The field's text: it searches, and filters nothing (#2398).
  const [text, setText] = useState("");
  const [palette, setPalette] = useState(false);
  // Typing opens the results once the text is long enough to search (#2398);
  // a keystroke after Escape closed them opens them again.
  const onInputChange = (next: string) => {
    setText(next);
    if (next.trim().length >= SEARCH_MIN_QUERY_LENGTH) setPalette(true);
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
    input.current?.focus();
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
      <div className={styles.inner}>
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
          then clears the text. It is shown at every width (#2403). */}
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
              if (palette && next !== null) setPalette(false);
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
                if (
                  event.key === "Escape" &&
                  !event.nativeEvent.isComposing &&
                  palette
                ) {
                  event.preventDefault();
                  event.stopPropagation();
                  setPalette(false);
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
            onOpenChange={setPalette}
            triggerRef={field}
            popoverRef={popover}
            text={text}
            onChoose={() => {
              // A row navigates and ends the search: the text clears, and the
              // narrow register browser closes, also on a result for the page
              // already open (#2403).
              setPalette(false);
              setText("");
              browser.onSearchChoose();
            }}
          />
        </Autocomplete>
        {/* Below the breakpoint only (CSS): it shows the register column in
          place of the page, and hides it again (#2403). */}
        <span className={styles.menu}>
          <Button
            ref={browserButtonRef}
            aria-label="Browse registers"
            aria-expanded={browser.open}
            isInert={browser.inert}
            aria-controls={browser.controls}
            onPress={browser.onToggle}
          >
            <MenuIcon />
          </Button>
        </span>
        <ProjectBanner context={context} validation={validation} />
      </div>
    </header>
  );
}

/** The narrow register browser's button, wired by the shell that owns the
 * browser (App.tsx). */
export interface RegisterBrowserButton {
  open: boolean;
  /** The register column's id. */
  controls: string;
  /** Shown for good: the catalog root's page is the register list. */
  inert: boolean;
  onToggle: () => void;
  /** A search result was chosen: the browser closes; focus stays in the
   * field, as the palette leaves it. */
  onSearchChoose: () => void;
}

/** Three lines: the register browser's button. */
function MenuIcon() {
  return (
    <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true">
      <path
        d="M2 4h12M2 8h12M2 12h12"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
      />
    </svg>
  );
}
