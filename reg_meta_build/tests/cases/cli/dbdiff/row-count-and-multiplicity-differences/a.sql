CREATE TABLE widget (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  payload BLOB,
  source_id INTEGER
);
INSERT INTO widget VALUES
  (1, 'alpha', X'000102', 100),
  (2, 'beta', X'FFFE', NULL),
  (3, 'gamma', NULL, 300);
CREATE TABLE tag (label TEXT, n INTEGER);
INSERT INTO tag VALUES ('x', 1), ('x', 1), ('x', 1), ('y', 2);
