-- a.sql with widget 1's last payload byte changed, and pair's bytes
-- moved across the column boundary.
CREATE TABLE widget (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  payload BLOB,
  source_id INTEGER
);
INSERT INTO widget VALUES
  (1, 'alpha', X'000103', 100),
  (2, 'beta', X'FFFE', NULL),
  (3, 'gamma', NULL, 300);
CREATE TABLE pair (x TEXT, y TEXT);
INSERT INTO pair VALUES ('a' || char(3) || 'b', '');
