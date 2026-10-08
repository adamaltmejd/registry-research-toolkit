-- `tag` has no INTEGER PRIMARY KEY, so its rows are read back in insertion
-- order; import_manifest's TEXT key is not a rowid alias either.
CREATE TABLE tag (label TEXT, n INTEGER);
INSERT INTO tag VALUES ('x', 1), ('y', 2), ('x', 1), ('z', 3);
CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO import_manifest VALUES
  ('schema_version', '5.1.0'),
  ('import_date', '2026-06-01T06:24:08Z'),
  ('input_dir', '/some/path');
