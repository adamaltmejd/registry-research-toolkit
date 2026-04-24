"""Provider-specific parsers that feed the regmeta DB.

regmeta is intentionally data-provider-agnostic: one metadata DB, one docs
DB, one query surface. Each upstream provider has its own parser module
here that reads the provider's native delivery format and yields a
structured representation consumed by `build-db`.

Current providers:

- `sos` — Socialstyrelsen metadata Excel workbooks.
  (`scb` import logic currently lives in `regmeta.db`; migrating it here
  is tracked for later.)
"""
