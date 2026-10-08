-- a.sql with a later import_date.
CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO import_manifest VALUES
  ('schema_version', '5.1.0'),
  ('import_date', '2026-07-15T12:00:00Z'),
  ('input_dir', '/some/path');
