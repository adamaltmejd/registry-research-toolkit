-- The catalog file, schema v4 (crates/DESIGN.md, "The catalog builder").
PRAGMA page_size = 4096;
PRAGMA application_id = 1380271939;      -- 'REGC'
-- user_version: emit sets it from reg_core::CATALOG_SCHEMA_VERSION (= meta schema_version).
PRAGMA encoding = 'UTF-8';

-- Scalar facts. Required keys: schema_version, generation_id, catalog_version,
-- builder_version, sqlite_version, open_start, open_end, curation_digest,
-- period_grammar, delivery_name_grammar.
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) STRICT, WITHOUT ROWID;

-- Pinned inputs. Every locator names one; generation_id hashes these rows.
CREATE TABLE input (
  id         INTEGER PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,        -- 'demo'
  repo       TEXT NOT NULL,
  commit_sha TEXT NOT NULL,
  path       TEXT NOT NULL,
  sha256     TEXT NOT NULL,
  bytes      INTEGER NOT NULL
) STRICT;

-- Interned text: names, labels, definitions, codes, code labels, evidence.
-- Deduplicated on (lang, body) by the build; no UNIQUE index (it would store every
-- body twice).
CREATE TABLE text (
  id   INTEGER PRIMARY KEY,
  lang TEXT NOT NULL DEFAULT 'sv',
  body TEXT NOT NULL
) STRICT;

-- Folded column names (reg-core fold_identity; NFC is open, crates/DESIGN.md).
-- Identity and header lookup use these.
CREATE TABLE name_fold (
  id     INTEGER PRIMARY KEY,
  folded TEXT NOT NULL UNIQUE
) STRICT;

-- Exact delivered spellings, evidence and display.
CREATE TABLE spelling (
  id      INTEGER PRIMARY KEY,
  exact   TEXT NOT NULL UNIQUE,
  fold_id INTEGER NOT NULL REFERENCES name_fold(id)
) STRICT;
CREATE INDEX spelling_fold ON spelling(fold_id);

-- Curation statements that won at least one field, link, assignment or ack.
CREATE TABLE statement (
  id            INTEGER PRIMARY KEY,
  hash          TEXT NOT NULL UNIQUE,     -- hash of normalized content
  kind          TEXT NOT NULL CHECK (kind IN ('bind','assert','assign','set','link','ack')),
  file          TEXT NOT NULL,            -- curation path, repo-relative
  line          INTEGER NOT NULL,
  evidence_text INTEGER REFERENCES text(id),  -- NULL only for an ack of a notice
  caveat_text   INTEGER REFERENCES text(id)
) STRICT;

CREATE TABLE unit_type (
  id         INTEGER PRIMARY KEY,
  slug       TEXT NOT NULL UNIQUE,        -- 'person', 'foretag', 'vardkontakt'
  label_text INTEGER NOT NULL REFERENCES text(id)
) STRICT;

CREATE TABLE id_kind (
  id            INTEGER PRIMARY KEY,
  slug          TEXT NOT NULL UNIQUE,     -- 'person', 'person-lopnr', 'firm'
  label_text    INTEGER NOT NULL REFERENCES text(id),
  base_id       INTEGER REFERENCES id_kind(id),  -- pseudonymised form -> its base
  pseudonymised INTEGER NOT NULL CHECK (pseudonymised IN (0,1))
) STRICT;

CREATE TABLE provider (
  id        INTEGER PRIMARY KEY,
  pid       TEXT NOT NULL UNIQUE,         -- 'demo'
  name_text INTEGER NOT NULL REFERENCES text(id),
  url       TEXT
) STRICT;

CREATE TABLE register (
  id               INTEGER PRIMARY KEY,
  provider_id      INTEGER NOT NULL REFERENCES provider(id),
  pid              TEXT NOT NULL UNIQUE,  -- 'demo/hush'
  name_text        INTEGER NOT NULL REFERENCES text(id),
  description_text INTEGER REFERENCES text(id),
  start            INTEGER NOT NULL,      -- hull of its tables' coverage
  end              INTEGER NOT NULL,
  CHECK (start < end)
) STRICT;
CREATE INDEX register_provider ON register(provider_id);

-- Tables. A timeless table (lookup) has grain 'none' and one span [open_start, open_end).
CREATE TABLE tbl (
  id               INTEGER PRIMARY KEY,
  register_id      INTEGER NOT NULL REFERENCES register(id),
  pid              TEXT NOT NULL UNIQUE,  -- 'demo/hush/individ'; implicit = register pid
  name_text        INTEGER NOT NULL REFERENCES text(id),
  description_text INTEGER REFERENCES text(id),
  role             TEXT NOT NULL CHECK (role IN ('data','codelist')),
  implicit         INTEGER NOT NULL CHECK (implicit IN (0,1)),
  timeless         INTEGER NOT NULL CHECK (timeless IN (0,1)),
  unit_type_id     INTEGER REFERENCES unit_type(id),
  cadence          TEXT CHECK (cadence IN ('year','half','quarter','month','day','irregular')),
  start            INTEGER NOT NULL,      -- coverage hull
  end              INTEGER NOT NULL,
  CHECK (start < end),
  CHECK (timeless = 0 OR cadence IS NULL)
) STRICT;
CREATE INDEX tbl_register ON tbl(register_id);

CREATE TABLE tbl_span (
  table_id INTEGER NOT NULL REFERENCES tbl(id),
  start    INTEGER NOT NULL,
  end      INTEGER NOT NULL,
  PRIMARY KEY (table_id, start),
  CHECK (start < end)
) STRICT, WITHOUT ROWID;

-- Stated deliveries. Never synthesized. An unparsed release has no period and no
-- primary flag. Among parsed editions of one (table, start, end, qualifier), one is
-- primary.
CREATE TABLE release (
  id               INTEGER PRIMARY KEY,
  table_id         INTEGER NOT NULL REFERENCES tbl(id),
  parse_state      TEXT NOT NULL CHECK (parse_state IN ('parsed','unparsed','curated')),
  start            INTEGER,
  end              INTEGER,
  grain            TEXT CHECK (grain IN ('year','half','quarter','month','day','span')),
  edition          TEXT NOT NULL CHECK (edition IN ('final','unstated','preliminary')),
  qualifier        TEXT NOT NULL DEFAULT '',  -- free text from the name; '' = none
  is_primary       INTEGER CHECK (is_primary IN (0,1)),
  name_text        INTEGER NOT NULL REFERENCES text(id),  -- literal delivery name
  description_text INTEGER REFERENCES text(id),
  ref_day          INTEGER,               -- stated reference date, may be absent
  source_key       TEXT NOT NULL,         -- opaque: 'r18774', or 'stmt:<hash>' if asserted
  input_id         INTEGER REFERENCES input(id),  -- NULL only when asserted
  locator          TEXT,
  UNIQUE (table_id, source_key),
  CHECK ((input_id IS NULL) = (locator IS NULL)),
  CHECK ((parse_state = 'unparsed') = (start IS NULL)),
  CHECK ((start IS NULL) = (end IS NULL)),
  CHECK ((start IS NULL) = (grain IS NULL)),
  CHECK ((start IS NULL) = (is_primary IS NULL)),
  CHECK (start IS NULL OR start < end)
) STRICT;
CREATE INDEX release_table ON release(table_id, start);

CREATE TABLE release_population (
  release_id      INTEGER NOT NULL REFERENCES release(id),
  ord             INTEGER NOT NULL,
  name_text       INTEGER NOT NULL REFERENCES text(id),
  definition_text INTEGER REFERENCES text(id),
  ref_day         INTEGER,
  PRIMARY KEY (release_id, ord)
) STRICT, WITHOUT ROWID;

-- Variables: register-scoped. label/definition are the latest primary run, for lists.
CREATE TABLE variable (
  id              INTEGER PRIMARY KEY,
  register_id     INTEGER NOT NULL REFERENCES register(id),
  pid             TEXT NOT NULL UNIQUE,   -- 'demo/hush/alder'
  label_text      INTEGER NOT NULL REFERENCES text(id),
  definition_text INTEGER REFERENCES text(id),
  start           INTEGER NOT NULL,       -- hull of its segments
  end             INTEGER NOT NULL,
  CHECK (start < end)
) STRICT;
CREATE INDEX variable_register ON variable(register_id, pid);

CREATE TABLE variable_unit_type (
  variable_id  INTEGER NOT NULL REFERENCES variable(id),
  unit_type_id INTEGER NOT NULL REFERENCES unit_type(id),
  PRIMARY KEY (variable_id, unit_type_id)
) STRICT, WITHOUT ROWID;

-- Definition runs: descriptive text over time.
CREATE TABLE variable_text (
  variable_id     INTEGER NOT NULL REFERENCES variable(id),
  start           INTEGER NOT NULL,
  end             INTEGER NOT NULL,
  label_text      INTEGER NOT NULL REFERENCES text(id),
  definition_text INTEGER REFERENCES text(id),
  opdef_text      INTEGER REFERENCES text(id),   -- operational definition
  unit_text       INTEGER REFERENCES text(id),
  PRIMARY KEY (variable_id, start),
  CHECK (start < end)
) STRICT, WITHOUT ROWID;

-- Content-addressed code lists. A classification version's content is one of these.
CREATE TABLE valueset (
  id               INTEGER PRIMARY KEY,
  hash             TEXT NOT NULL UNIQUE,  -- sha256 of the canonical item list
  n_items          INTEGER NOT NULL,
  hierarchical     INTEGER NOT NULL CHECK (hierarchical IN (0,1)),
  class_version_id INTEGER REFERENCES class_version(id),  -- curated version; V1 only proposes (D29)
  class_match      TEXT CHECK (class_match IN ('equal','subset')),
  CHECK ((class_version_id IS NULL) = (class_match IS NULL))
) STRICT;

-- The names sources give a value set (a code-list title).
CREATE TABLE valueset_name (
  valueset_id INTEGER NOT NULL REFERENCES valueset(id),
  input_id    INTEGER NOT NULL REFERENCES input(id),
  name_text   INTEGER NOT NULL REFERENCES text(id),
  PRIMARY KEY (valueset_id, input_id, name_text)
) STRICT, WITHOUT ROWID;

CREATE TABLE vs_item (
  valueset_id INTEGER NOT NULL REFERENCES valueset(id),
  ord         INTEGER NOT NULL,           -- 0..n_items-1, canonical order
  code_text   INTEGER NOT NULL REFERENCES text(id),
  label_text  INTEGER REFERENCES text(id),
  level       INTEGER NOT NULL DEFAULT 1,
  parent_ord  INTEGER,
  sentinel    INTEGER NOT NULL DEFAULT 0 CHECK (sentinel IN (0,1)),  -- "unknown" etc.
  PRIMARY KEY (valueset_id, ord)
) STRICT, WITHOUT ROWID;

-- Per-code validity dates are applied at read time and not stored (design D30).

CREATE TABLE class_family (
  id               INTEGER PRIMARY KEY,
  pid              TEXT NOT NULL UNIQUE,  -- 'class/sun'
  kind             TEXT NOT NULL CHECK (kind IN ('classification','codelist')),
  name_text        INTEGER NOT NULL REFERENCES text(id),
  publisher_text   INTEGER REFERENCES text(id),
  description_text INTEGER REFERENCES text(id)
) STRICT;

CREATE TABLE class_version (
  id          INTEGER PRIMARY KEY,
  family_id   INTEGER NOT NULL REFERENCES class_family(id),
  pid         TEXT NOT NULL UNIQUE,       -- 'class/sun/2020'
  label_text  INTEGER NOT NULL REFERENCES text(id),
  start       INTEGER NOT NULL,
  end         INTEGER NOT NULL,
  valueset_id INTEGER NOT NULL REFERENCES valueset(id),
  CHECK (start < end)
) STRICT;
CREATE INDEX class_version_family ON class_version(family_id, start);

CREATE TABLE class_level (
  version_id INTEGER NOT NULL REFERENCES class_version(id),
  level      INTEGER NOT NULL,
  name_text  INTEGER NOT NULL REFERENCES text(id),
  PRIMARY KEY (version_id, level)
) STRICT, WITHOUT ROWID;

CREATE TABLE correspondence (
  id                INTEGER PRIMARY KEY,
  source_version_id INTEGER NOT NULL REFERENCES class_version(id),
  target_version_id INTEGER NOT NULL REFERENCES class_version(id),
  source_level      INTEGER NOT NULL,
  target_level      INTEGER NOT NULL,
  description_text  INTEGER REFERENCES text(id),
  UNIQUE (source_version_id, target_version_id, source_level, target_level)
) STRICT;
CREATE INDEX corr_target ON correspondence(target_version_id);

CREATE TABLE corr_map (
  correspondence_id INTEGER NOT NULL REFERENCES correspondence(id),
  source_code_text  INTEGER NOT NULL REFERENCES text(id),
  target_code_text  INTEGER NOT NULL REFERENCES text(id),
  PRIMARY KEY (correspondence_id, source_code_text, target_code_text)
) STRICT, WITHOUT ROWID;

-- Segments: maximal runs of one form of a variable in one table. One folded
-- spelling per (variable, table) at a time. spelling_id = exact spelling of the latest
-- primary occurrence.
-- Form fields are NULL where the state is 'ambiguous' and the claimants disagree.
CREATE TABLE segment (
  id               INTEGER PRIMARY KEY,
  variable_id      INTEGER NOT NULL REFERENCES variable(id),
  table_id         INTEGER NOT NULL REFERENCES tbl(id),
  fold_id          INTEGER NOT NULL REFERENCES name_fold(id),
  start            INTEGER NOT NULL,
  end              INTEGER NOT NULL,
  spelling_id      INTEGER NOT NULL REFERENCES spelling(id),
  dtype            TEXT,                  -- normalized: 'int','decimal','char','date',...
  length           INTEGER,
  format           TEXT,                  -- 'yyyymmdd', 'yyyy', ...
  valueset_id      INTEGER REFERENCES valueset(id),
  class_version_id INTEGER REFERENCES class_version(id),
  coded            TEXT NOT NULL CHECK (coded IN ('yes','no','unknown')),
  sensitive        INTEGER CHECK (sensitive IN (0,1)),
  identifier       INTEGER CHECK (identifier IN (0,1)),
  id_kind_id       INTEGER REFERENCES id_kind(id),
  role             TEXT NOT NULL CHECK (role IN ('row_key','time_start','time_stop','time_point','measure','code','label')),
  temporality      TEXT NOT NULL CHECK (temporality IN ('fixed','status','accumulated','event','unknown')),
  state            TEXT NOT NULL CHECK (state IN ('clear','ambiguous','preliminary')),
  CHECK (start < end)
) STRICT;
CREATE UNIQUE INDEX segment_variable ON segment(variable_id, table_id, start);
CREATE INDEX segment_table    ON segment(table_id, start);
CREATE INDEX segment_fold     ON segment(fold_id);
CREATE INDEX segment_valueset ON segment(valueset_id, variable_id) WHERE valueset_id IS NOT NULL;
CREATE INDEX segment_class    ON segment(class_version_id, variable_id) WHERE class_version_id IS NOT NULL;

-- Occurrences: one column as documented in one period. The atom; evidence.
-- override_mask bits: 0 spelling, 1 dtype, 2 length, 3 format, 4 valueset,
-- 5 class_version, 6 coded, 7 sensitive, 8 identifier, 9 id_kind, 10 role,
-- 11 temporality, 12 label, 13 definition, 14 opdef, 15 unit, 16 period, 17 table,
-- 18 variable. Bit set: see override; no override row = the field's default rule.
CREATE TABLE occurrence (
  id               INTEGER PRIMARY KEY,
  table_id         INTEGER NOT NULL REFERENCES tbl(id),
  release_id       INTEGER REFERENCES release(id),
  start            INTEGER,                           -- NULL only when unplaced
  end              INTEGER,
  spelling_id      INTEGER REFERENCES spelling(id),   -- NULL = documented without column
  source_key       TEXT NOT NULL,                     -- opaque: 'v2267', 'n:alder'
  label_text       INTEGER REFERENCES text(id),
  definition_text  INTEGER REFERENCES text(id),
  opdef_text       INTEGER REFERENCES text(id),
  unit_text        INTEGER REFERENCES text(id),
  dtype            TEXT,
  length           INTEGER,
  format           TEXT,
  valueset_id      INTEGER REFERENCES valueset(id),
  class_version_id INTEGER REFERENCES class_version(id),
  coded            TEXT NOT NULL CHECK (coded IN ('yes','no','unknown')),
  sensitive        INTEGER CHECK (sensitive IN (0,1)),
  identifier       INTEGER CHECK (identifier IN (0,1)),
  id_kind_id       INTEGER REFERENCES id_kind(id),
  role             TEXT NOT NULL CHECK (role IN ('row_key','time_start','time_stop','time_point','measure','code','label')),
  temporality      TEXT NOT NULL CHECK (temporality IN ('fixed','status','accumulated','event','unknown')),
  variable_id      INTEGER REFERENCES variable(id),   -- NULL when withheld, unplaced, or a
                                                      -- marker of a key that never has a column
  segment_id       INTEGER REFERENCES segment(id),    -- NULL unless status = 'used'
  status           TEXT NOT NULL CHECK (status IN ('used','withheld','shadowed','gap_marker','rejected','unplaced')),
  override_mask    INTEGER NOT NULL DEFAULT 0,
  CHECK ((start IS NULL) = (end IS NULL)),
  CHECK ((status = 'unplaced') = (start IS NULL)),
  CHECK (start IS NULL OR start < end),
  CHECK ((status = 'used') = (segment_id IS NOT NULL)),
  CHECK (status NOT IN ('withheld','unplaced') OR variable_id IS NULL),
  CHECK (status IN ('withheld','unplaced','gap_marker') OR variable_id IS NOT NULL),
  CHECK (status = 'unplaced' OR (status = 'gap_marker') = (spelling_id IS NULL))
) STRICT;
CREATE INDEX occurrence_segment  ON occurrence(segment_id) WHERE segment_id IS NOT NULL;
CREATE INDEX occurrence_variable ON occurrence(variable_id, start);
CREATE INDEX occurrence_release  ON occurrence(release_id, variable_id) WHERE release_id IS NOT NULL;
CREATE INDEX occurrence_table    ON occurrence(table_id, start);
CREATE INDEX occurrence_key      ON occurrence(table_id, source_key);  -- build and debug only

-- Value sets a source states per population when they differ within one occurrence
-- (design D31). The occurrence's own valueset_id stays NULL until curation sets one, and
-- F6 is raised once per key. Empty in milestone A.
CREATE TABLE occurrence_population_valueset (
  occurrence_id  INTEGER NOT NULL REFERENCES occurrence(id),
  population_ord INTEGER NOT NULL,        -- release_population.ord of the occurrence's release
  valueset_id    INTEGER NOT NULL REFERENCES valueset(id),
  -- several rows for one population are alternatives its source rows disagree on (F6)
  PRIMARY KEY (occurrence_id, population_ord, valueset_id)
) STRICT, WITHOUT ROWID;

-- Which input rows stated an occurrence (merged population rows give several).
CREATE TABLE occurrence_locator (
  occurrence_id INTEGER NOT NULL REFERENCES occurrence(id),
  input_id      INTEGER NOT NULL REFERENCES input(id),
  locator       TEXT NOT NULL,            -- opaque: '<file>:row=12'
  PRIMARY KEY (occurrence_id, input_id, locator)
) STRICT, WITHOUT ROWID;

-- Non-source winners. subject_kind names the table subject_id points into.
CREATE TABLE override (
  subject_kind TEXT NOT NULL CHECK (subject_kind IN ('occurrence','table','release','variable','register','valueset','class_version')),
  subject_id   INTEGER NOT NULL,
  field        TEXT NOT NULL,             -- the closed field schema of `set`
  prov         TEXT NOT NULL CHECK (prov IN ('curated','rule')),
  statement_id INTEGER REFERENCES statement(id),
  rule_code    TEXT,
  value        TEXT,                      -- winning value, rendered
  source_value TEXT,                      -- losing source value; NULL = source silent
  PRIMARY KEY (subject_kind, subject_id, field),
  CHECK ((prov = 'curated') = (statement_id IS NOT NULL)),
  CHECK ((prov = 'rule') = (rule_code IS NOT NULL))
) STRICT, WITHOUT ROWID;

-- Gaps inside a coverage hull, with a reason (design T5).
CREATE TABLE gap (
  subject_kind TEXT NOT NULL CHECK (subject_kind IN ('table','variable')),
  subject_id   INTEGER NOT NULL,
  table_id     INTEGER NOT NULL REFERENCES tbl(id),
  start        INTEGER NOT NULL,
  end          INTEGER NOT NULL,
  reason       TEXT NOT NULL CHECK (reason IN ('no_release','release_without_column','documented_without_column','ambiguous','withheld')),
  PRIMARY KEY (subject_kind, subject_id, table_id, start),
  CHECK (start < end)
) STRICT, WITHOUT ROWID;

-- Typed edges. Table succession is kind 'continues' between tables, at_start = the
-- day the successor takes over.
CREATE TABLE relation (
  id           INTEGER PRIMARY KEY,
  kind         TEXT NOT NULL CHECK (kind IN ('continues','equivalent','derived_from','label_of')),
  subject_kind TEXT NOT NULL CHECK (subject_kind IN ('variable','table','register','class_version')),
  subject_id   INTEGER NOT NULL,
  object_kind  TEXT NOT NULL CHECK (object_kind IN ('variable','table','register','class_version')),
  object_id    INTEGER NOT NULL,
  at_start     INTEGER,
  at_end       INTEGER,
  prov         TEXT NOT NULL CHECK (prov IN ('source','curated','rule')),
  statement_id INTEGER REFERENCES statement(id),
  caveat_text  INTEGER REFERENCES text(id),
  UNIQUE (kind, subject_kind, subject_id, object_kind, object_id, at_start),
  CHECK (kind <> 'continues' OR subject_kind = object_kind)
) STRICT;
CREATE INDEX relation_subject ON relation(subject_kind, subject_id, kind);
CREATE INDEX relation_object  ON relation(object_kind, object_id, kind);

-- Curated sets (within one register, the picking unit) and tags (cross-register,
-- search only). Authored with `set` on variables (fields `sets`, `tags`).
CREATE TABLE var_set (
  id               INTEGER PRIMARY KEY,
  kind             TEXT NOT NULL CHECK (kind IN ('set','tag')),
  pid              TEXT NOT NULL UNIQUE,  -- 'demo/hush/<set>' or 'tag/<slug>'
  register_id      INTEGER REFERENCES register(id),
  name_text        INTEGER NOT NULL REFERENCES text(id),
  description_text INTEGER REFERENCES text(id),
  CHECK ((kind = 'set') = (register_id IS NOT NULL))
) STRICT;
CREATE INDEX var_set_register ON var_set(register_id) WHERE register_id IS NOT NULL;

CREATE TABLE var_set_member (
  set_id      INTEGER NOT NULL REFERENCES var_set(id),
  variable_id INTEGER NOT NULL REFERENCES variable(id),
  ord         INTEGER NOT NULL,
  PRIMARY KEY (set_id, variable_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX var_set_member_variable ON var_set_member(variable_id);

-- Findings shown to readers: open review items and notices (including acked caveats).
-- code is a rule code plus its sub-kind: 'F1:type-change',
-- 'F2:ambiguous', 'F7:edition-disagreement', 'F9:unparsed-delivery', 'T1:clamp'.
CREATE TABLE finding (
  id               INTEGER PRIMARY KEY,
  code             TEXT NOT NULL,
  severity         TEXT NOT NULL CHECK (severity IN ('review','notice')),
  subject_kind     TEXT NOT NULL CHECK (subject_kind IN ('register','table','release','variable','segment','occurrence','valueset')),
  subject_id       INTEGER NOT NULL,
  register_id      INTEGER REFERENCES register(id),
  start            INTEGER,
  end              INTEGER,
  message_text     INTEGER NOT NULL REFERENCES text(id),
  caveat_text      INTEGER REFERENCES text(id),
  ack_statement_id INTEGER REFERENCES statement(id),
  CHECK ((start IS NULL) = (end IS NULL))
) STRICT;
CREATE INDEX finding_subject  ON finding(subject_kind, subject_id);
CREATE INDEX finding_register ON finding(register_id, code);

CREATE TABLE doc (
  id         INTEGER PRIMARY KEY,
  hash       TEXT NOT NULL UNIQUE,        -- sha256 of the original bytes
  kind       TEXT NOT NULL CHECK (kind IN ('parsed','pdf')),
  title_text INTEGER NOT NULL REFERENCES text(id),
  licence    TEXT NOT NULL,
  bytes      INTEGER NOT NULL,
  url        TEXT,
  input_id   INTEGER REFERENCES input(id)
) STRICT;

CREATE TABLE doc_section (
  id      INTEGER PRIMARY KEY,            -- rowid for the FTS external content
  doc_id  INTEGER NOT NULL REFERENCES doc(id),
  ord     INTEGER NOT NULL,
  heading TEXT,
  page    INTEGER,
  body    TEXT NOT NULL,
  UNIQUE (doc_id, ord)
) STRICT;

CREATE VIRTUAL TABLE doc_fts USING fts5(
  heading, body,
  content = 'doc_section', content_rowid = 'id',
  tokenize = 'unicode61 remove_diacritics 0'
);

CREATE TABLE doc_link (
  doc_id       INTEGER NOT NULL REFERENCES doc(id),
  subject_kind TEXT NOT NULL CHECK (subject_kind IN ('register','table','variable','class_family','valueset')),
  subject_id   INTEGER NOT NULL,
  page         INTEGER NOT NULL DEFAULT 0,  -- 0 = whole document
  prov         TEXT NOT NULL CHECK (prov IN ('source','curated','rule')),
  PRIMARY KEY (doc_id, subject_kind, subject_id, page)
) STRICT, WITHOUT ROWID;
CREATE INDEX doc_link_subject ON doc_link(subject_kind, subject_id);

-- One public namespace across kinds; bare-name lookup. The only pid -> row id path.
CREATE TABLE ident (
  pid         TEXT PRIMARY KEY,
  kind        TEXT NOT NULL CHECK (kind IN ('provider','register','table','variable','set','tag','class_family','class_version')),
  entity_id   INTEGER NOT NULL,
  register_id INTEGER REFERENCES register(id),
  name_fold   TEXT NOT NULL               -- folded last path segment
) STRICT, WITHOUT ROWID;
CREATE INDEX ident_name ON ident(name_fold, kind);
CREATE UNIQUE INDEX ident_entity ON ident(kind, entity_id);

-- Retired ids: published, never reused, never redirected.
CREATE TABLE ident_retired (
  pid        TEXT PRIMARY KEY,
  kind       TEXT NOT NULL,
  retired_in TEXT NOT NULL,               -- catalog_version that retired it
  reason     TEXT NOT NULL CHECK (reason IN ('merged','split','withdrawn'))
) STRICT, WITHOUT ROWID;

-- Every folded name a variable was ever documented under (any status but withheld),
-- plus curated other names. Serves header lookup.
CREATE TABLE alias (
  fold_id     INTEGER NOT NULL REFERENCES name_fold(id),
  variable_id INTEGER NOT NULL REFERENCES variable(id),
  kind        TEXT NOT NULL CHECK (kind IN ('delivered','curated')),
  PRIMARY KEY (fold_id, variable_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX alias_variable ON alias(variable_id);

-- Search: one row per searchable entity; rowid shared with the FTS index.
-- A 'code' row names a value set (a classification version's or a provider's) and a
-- code; sort_key breaks bm25 ties deterministically (pid, or 'vs:<hash>#code').
CREATE TABLE search_item (
  id          INTEGER PRIMARY KEY,
  kind        TEXT NOT NULL CHECK (kind IN ('register','table','variable','set','classification','code','document')),
  entity_id   INTEGER,                    -- NULL for 'code'
  valueset_id INTEGER REFERENCES valueset(id),  -- 'code' only
  code_text   INTEGER REFERENCES text(id),      -- 'code' only
  register_id INTEGER REFERENCES register(id),
  start       INTEGER,                    -- coverage hull, for the period filter
  end         INTEGER,
  sort_key    TEXT NOT NULL UNIQUE,
  CHECK ((kind = 'code') = (valueset_id IS NOT NULL)),
  CHECK ((kind = 'code') = (code_text IS NOT NULL)),
  CHECK ((kind = 'code') = (entity_id IS NULL))
) STRICT;

CREATE VIRTUAL TABLE search USING fts5(
  name, label, body,                      -- pid + spellings; labels + synonyms; definitions
  content = '',
  tokenize = 'unicode61 remove_diacritics 0',
  prefix = '2 3'
);
