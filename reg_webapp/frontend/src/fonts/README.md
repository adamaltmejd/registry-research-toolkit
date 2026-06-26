# Self-hosted webfonts

This directory holds the two webfaces the SPA self-hosts (no runtime CDN). Both are
`woff2` **subsets** — latin + latin-ext only — derived from the upstream families:

- **Schibsted Grotesk** (UI) — © The Schibsted Grotesk Project Authors.
- **IBM Plex Mono** (mono) — © IBM Corp.

Both families are licensed under the **SIL Open Font License 1.1 (OFL)**. The full
license text plus the copyright notices and Reserved Font Names accompany the fonts
here:

- `schibsted-grotesk-LICENSE.txt`
- `ibm-plex-mono-LICENSE.txt`

Upstream source: the [fontsource](https://fontsource.org) packages
(`@fontsource/schibsted-grotesk`, `@fontsource/ibm-plex-mono`), themselves repackaging
the original projects ([Schibsted
Grotesk](https://github.com/schibsted/schibsted-grotesk), [IBM
Plex](https://github.com/IBM/plex)).

The `.woff2` files here are subsetted derivatives of those originals — modification and
redistribution are permitted under OFL 1.1, and the copyright + license text travel with
the fonts in this source tree (OFL §2). `@font-face` declarations live in
`../tokens.css`.
