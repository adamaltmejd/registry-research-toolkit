-- a.sql with widget 1 renamed 'ALPHA'.
CREATE TABLE widget (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  payload BLOB,
  source_id INTEGER
);
INSERT INTO widget VALUES
  (1, 'ALPHA', X'000102', 100),
  (2, 'beta', X'FFFE', NULL),
  (3, 'gamma', NULL, 300);
CREATE TABLE tag (label TEXT, n INTEGER);
INSERT INTO tag VALUES ('x', 1), ('y', 2), ('x', 1);
