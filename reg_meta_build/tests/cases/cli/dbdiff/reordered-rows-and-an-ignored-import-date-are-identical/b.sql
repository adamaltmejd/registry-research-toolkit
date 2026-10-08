-- a.sql's rows, inserted in another order.
CREATE TABLE tag (label TEXT, n INTEGER);
INSERT INTO tag VALUES ('z', 3), ('x', 1), ('y', 2), ('x', 1);
CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO import_manifest VALUES
  ('input_dir', '/some/path'),
  ('import_date', '2026-06-01T06:24:08Z'),
  ('schema_version', '5.1.0');
