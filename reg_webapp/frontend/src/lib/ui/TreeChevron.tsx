/** A tree row's disclosure glyph, inside RAC's `slot="chevron"` button (which
 * supplies the localized label). The row's CSS turns it on `[data-expanded]`. */
export function TreeChevron() {
  return (
    <svg viewBox="0 0 16 16" width="12" height="12" aria-hidden="true">
      <path
        d="M6 3l5 5-5 5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.6"
      />
    </svg>
  );
}
